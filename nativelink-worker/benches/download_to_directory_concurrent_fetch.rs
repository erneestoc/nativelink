// Copyright 2026 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0 Future License (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    See LICENSE file for details
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Criterion benchmark comparing two implementations of `download_to_directory`:
//!
//! - **baseline**: per-file `populate_fast_store` + hardlink chained via
//!   `buffer_unordered(64)` (mirror of `origin/main` shape).
//! - **new**: blob-fetch deduplication + fetcher/hardlinker pipeline (the
//!   implementation now living in `running_actions_manager.rs`).
//!
//! The two implementations share the same `FastSlowStore`+`FilesystemStore`
//! fixture so any wall-time delta is attributable to scheduling/concurrency
//! shape, not store setup.

#![allow(clippy::missing_docs_in_private_items)]
#![allow(clippy::too_many_lines)]

use core::pin::Pin;
use core::sync::atomic::{AtomicUsize, Ordering};
use core::time::Duration;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use filetime::{FileTime, set_file_mtime};
use futures::future::TryFutureExt;
use futures::stream::{StreamExt, TryStreamExt};
use nativelink_config::stores::{
    FastSlowSpec, FilesystemSpec, MemorySpec, StoreDirection, StoreSpec,
};
use nativelink_error::{Code, Error, ResultExt};
use nativelink_metric::{
    MetricFieldData, MetricKind, MetricPublishKnownKindData, MetricsComponent,
};
use nativelink_proto::build::bazel::remote::execution::v2::{
    Directory as ProtoDirectory, FileNode,
};
use nativelink_store::cas_utils::is_zero_digest;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::filesystem_store::{FileEntry, FilesystemStore};
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use nativelink_util::common::{DigestInfo, fs};
use nativelink_util::digest_hasher::{DigestHasher, DigestHasherFunc};
use nativelink_util::health_utils::{
    HealthRegistryBuilder, HealthStatusIndicator, default_health_status_indicator,
};
use nativelink_util::spawn_blocking;
use nativelink_util::store_trait::{
    RemoveItemCallback, Store, StoreDriver, StoreKey, StoreLike, StoreOptimizations, UploadSizeInfo,
};
use nativelink_worker::running_actions_manager::download_to_directory as new_download_to_directory;
use prost::Message;
use tempfile::TempDir;
use tokio::runtime::Runtime;

// ---------------------------------------------------------------------------
// Baseline implementation (mirror of origin/main shape).
//
// The baseline issues per-file populate_fast_store + hardlink chains, capped
// at 64 via `buffer_unordered`. There is no inter-file dedup and no
// fetch/hardlink pipelining: each file's hardlink waits on its own fetch.
// ---------------------------------------------------------------------------
const BASELINE_CONCURRENCY: usize = 64;

#[allow(clippy::cognitive_complexity)]
fn baseline_download_to_directory<'a>(
    cas_store: &'a FastSlowStore,
    filesystem_store: Pin<&'a FilesystemStore>,
    digest: &'a DigestInfo,
    current_directory: &'a str,
) -> futures::future::BoxFuture<'a, Result<(), Error>> {
    use futures::FutureExt;
    use nativelink_store::ac_utils::get_and_decode_digest;
    #[cfg(target_family = "unix")]
    use std::fs::Permissions;
    #[cfg(target_family = "unix")]
    use std::os::unix::fs::PermissionsExt;
    use tokio::io::AsyncWriteExt;

    async move {
        let directory = get_and_decode_digest::<ProtoDirectory>(cas_store, digest.into())
            .await
            .err_tip(|| "Converting digest to Directory")?;
        let mut futures: Vec<futures::future::BoxFuture<'a, Result<(), Error>>> = Vec::new();

        for file in directory.files {
            let digest: DigestInfo = file
                .digest
                .err_tip(|| "Expected Digest to exist in Directory::file::digest")?
                .try_into()
                .err_tip(|| "In Directory::file::digest")?;
            let dest = format!("{}/{}", current_directory, file.name);
            let (mtime, mut unix_mode) = match file.node_properties {
                Some(properties) => (properties.mtime, properties.unix_mode),
                None => (None, None),
            };
            #[cfg_attr(target_family = "windows", allow(unused_assignments))]
            if file.is_executable {
                unix_mode = Some(unix_mode.unwrap_or(0o444) | 0o111);
            }
            futures.push(
                cas_store
                    .populate_fast_store(digest.into())
                    .and_then(move |()| async move {
                        if is_zero_digest(digest) {
                            let mut file_slot = fs::create_file(&dest).await?;
                            file_slot.write_all(&[]).await?;
                        } else {
                            let file_entry = filesystem_store
                                .get_file_entry_for_digest(&digest)
                                .await
                                .err_tip(|| "During hard link")?;
                            let src_path = file_entry
                                .get_file_path_locked(|src| async move { Ok(PathBuf::from(src)) })
                                .await?;
                            fs::hard_link(&src_path, &dest).await.map_err(|e| {
                                if e.code == Code::NotFound {
                                    e.append(format!("hardlink {dest}"))
                                } else {
                                    e.append(format!("hardlink {dest}"))
                                }
                            })?;
                        }
                        #[cfg(target_family = "unix")]
                        if let Some(unix_mode) = unix_mode {
                            fs::set_permissions(&dest, Permissions::from_mode(unix_mode))
                                .await
                                .err_tip(|| "set_permissions")?;
                        }
                        if let Some(mtime) = mtime {
                            spawn_blocking!("baseline_set_mtime", move || {
                                set_file_mtime(
                                    &dest,
                                    FileTime::from_unix_time(mtime.seconds, mtime.nanos as u32),
                                )
                                .err_tip(|| "set_mtime")
                            })
                            .await
                            .err_tip(|| "spawn_blocking")??;
                        }
                        Ok(())
                    })
                    .map_err(move |e| e.append(format!("for digest {digest}")))
                    .boxed(),
            );
        }
        // Symlinks/subdirectories: bench fixtures are flat, so skipped here.
        futures::stream::iter(futures)
            .buffer_unordered(BASELINE_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?;
        Ok(())
    }
    .boxed()
}

// ---------------------------------------------------------------------------
// Per-fetch latency-injecting slow store. Mirrors the test-only DelayedStore.
// ---------------------------------------------------------------------------
#[derive(Debug)]
struct DelayedStore {
    inner: Arc<MemoryStore>,
    get_part_delay: Duration,
    in_flight: AtomicUsize,
    max_in_flight: AtomicUsize,
}

impl DelayedStore {
    fn new(inner: Arc<MemoryStore>, get_part_delay: Duration) -> Arc<Self> {
        Arc::new(Self {
            inner,
            get_part_delay,
            in_flight: AtomicUsize::new(0),
            max_in_flight: AtomicUsize::new(0),
        })
    }
}

impl MetricsComponent for DelayedStore {
    fn publish(
        &self,
        _: MetricKind,
        _: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        Ok(MetricPublishKnownKindData::Component)
    }
}

default_health_status_indicator!(DelayedStore);

#[async_trait]
impl StoreDriver for DelayedStore {
    async fn has_with_results(
        self: Pin<&Self>,
        keys: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        Pin::new(self.inner.as_ref())
            .has_with_results(keys, results)
            .await
    }

    async fn update(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        reader: DropCloserReadHalf,
        size_info: UploadSizeInfo,
    ) -> Result<(), Error> {
        Pin::new(self.inner.as_ref())
            .update(key, reader, size_info)
            .await
    }

    fn optimized_for(&self, _: StoreOptimizations) -> bool {
        false
    }

    async fn get_part(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        let cur = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        let mut m = self.max_in_flight.load(Ordering::SeqCst);
        while cur > m {
            match self
                .max_in_flight
                .compare_exchange(m, cur, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => break,
                Err(a) => m = a,
            }
        }
        tokio::time::sleep(self.get_part_delay).await;
        let r = Pin::new(self.inner.as_ref())
            .get_part(key, writer, offset, length)
            .await;
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        r
    }

    fn inner_store(&self, _: Option<StoreKey>) -> &dyn StoreDriver {
        self
    }
    fn as_any<'a>(&'a self) -> &'a (dyn core::any::Any + Sync + Send + 'static) {
        self
    }
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn core::any::Any + Sync + Send + 'static> {
        self
    }
    fn register_health(self: Arc<Self>, r: &mut HealthRegistryBuilder) {
        r.register_indicator(self);
    }
    fn register_remove_callback(
        self: Arc<Self>,
        _: Arc<dyn RemoveItemCallback>,
    ) -> Result<(), Error> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Workload definition & fixture builder.
// ---------------------------------------------------------------------------
struct Workload {
    name: &'static str,
    num_files: usize,
    num_unique_digests: usize,
    file_size_bytes: usize,
    fetch_latency: Option<Duration>,
}

/// Long-lived per-workload state. Holds the slow store (with blobs and the
/// directory proto already populated) so we can rebuild a fresh fast store
/// per iteration to measure the cold-cache path that matches production
/// (every Bazel action arrives with a unique input set; the directory cache
/// short-circuits warm-cache reuse, so by the time `download_to_directory`
/// is called the fast store has none of the action's blobs).
struct BlobBank {
    inner_slow: Arc<MemoryStore>,
    root_digest: DigestInfo,
    fetch_latency: Option<Duration>,
}

/// Per-iteration fixture: a freshly constructed FastSlowStore wrapping a
/// freshly constructed FilesystemStore. The slow store is the shared
/// `BlobBank` so we don't pay re-population cost on every iteration.
struct Fixture {
    _tempdir: TempDir,
    cas_store: Arc<FastSlowStore>,
    fast_store: Arc<FilesystemStore>,
    root_digest: DigestInfo,
}

async fn build_blob_bank(w: &Workload) -> Result<BlobBank, Error> {
    let inner_slow = MemoryStore::new(&MemorySpec::default());

    // Build unique blobs in inner_slow.
    let mut unique_digests: Vec<DigestInfo> = Vec::with_capacity(w.num_unique_digests);
    for i in 0..w.num_unique_digests {
        let mut body = vec![0u8; w.file_size_bytes];
        let id = (i as u64).to_le_bytes();
        let prefix_len = 8.min(body.len());
        body[..prefix_len].copy_from_slice(&id[..prefix_len]);
        let mut hasher = DigestHasherFunc::Blake3.hasher();
        hasher.update(&body);
        let digest = hasher.finalize_digest();
        inner_slow.update_oneshot(digest, body.into()).await?;
        unique_digests.push(digest);
    }

    // Build root Directory referencing N files, each pointing at one of the
    // unique digests (round-robin).
    let mut files = Vec::with_capacity(w.num_files);
    for i in 0..w.num_files {
        let digest = unique_digests[i % w.num_unique_digests];
        files.push(FileNode {
            name: format!("f_{i:06}.dat"),
            digest: Some(digest.into()),
            is_executable: false,
            node_properties: None,
        });
    }
    let dir = ProtoDirectory {
        files,
        ..Default::default()
    };
    let dir_bytes = dir.encode_to_vec();
    let mut hasher = DigestHasherFunc::Blake3.hasher();
    hasher.update(&dir_bytes);
    let root_digest = hasher.finalize_digest();
    inner_slow
        .update_oneshot(root_digest, dir_bytes.into())
        .await?;

    Ok(BlobBank {
        inner_slow,
        root_digest,
        fetch_latency: w.fetch_latency,
    })
}

async fn build_fixture(bank: &BlobBank, idx: usize) -> Result<Fixture, Error> {
    let tempdir = tempfile::tempdir()
        .map_err(|e| nativelink_error::make_err!(Code::Internal, "tempdir: {e}"))?;
    let content_path = tempdir.path().join(format!("content_{idx}"));
    let temp_path = tempdir.path().join(format!("temp_{idx}"));
    let fast_config = FilesystemSpec {
        content_path: content_path.to_string_lossy().into_owned(),
        temp_path: temp_path.to_string_lossy().into_owned(),
        eviction_policy: None,
        ..Default::default()
    };
    let fast_store = FilesystemStore::new(&fast_config).await?;
    let slow_store: Store = if let Some(d) = bank.fetch_latency {
        let delayed = DelayedStore::new(bank.inner_slow.clone(), d);
        Store::new(delayed)
    } else {
        Store::new(bank.inner_slow.clone())
    };
    let cas_store = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Filesystem(fast_config.clone()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
        },
        Store::new(fast_store.clone()),
        slow_store,
    );
    Ok(Fixture {
        _tempdir: tempdir,
        cas_store,
        fast_store,
        root_digest: bank.root_digest,
    })
}

fn workloads() -> Vec<Workload> {
    vec![
        Workload {
            name: "small_unique",
            num_files: 64,
            num_unique_digests: 64,
            file_size_bytes: 1024,
            fetch_latency: None,
        },
        Workload {
            name: "medium_unique",
            num_files: 635,
            num_unique_digests: 635,
            file_size_bytes: 290 * 1024,
            fetch_latency: None,
        },
        Workload {
            name: "large_unique",
            num_files: 1978,
            num_unique_digests: 1978,
            file_size_bytes: 240 * 1024,
            fetch_latency: None,
        },
        Workload {
            name: "small_50pct_dedup",
            num_files: 64,
            num_unique_digests: 32,
            file_size_bytes: 1024,
            fetch_latency: None,
        },
        Workload {
            name: "large_90pct_dedup",
            num_files: 1978,
            num_unique_digests: 198,
            file_size_bytes: 240 * 1024,
            fetch_latency: None,
        },
        Workload {
            name: "slow_fetch_unique",
            num_files: 256,
            num_unique_digests: 256,
            file_size_bytes: 1024,
            fetch_latency: Some(Duration::from_millis(5)),
        },
    ]
}

// Reduced (--quick-friendly) workloads to keep wall time bounded; we keep
// the same shapes but cap the heaviest ones via env override.
fn maybe_scaled(w: &Workload) -> Workload {
    let scale: f64 = std::env::var("BENCH_SCALE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1.0);
    if (scale - 1.0).abs() < f64::EPSILON {
        return Workload {
            name: w.name,
            num_files: w.num_files,
            num_unique_digests: w.num_unique_digests,
            file_size_bytes: w.file_size_bytes,
            fetch_latency: w.fetch_latency,
        };
    }
    let nf = ((w.num_files as f64) * scale).max(1.0) as usize;
    let nu = ((w.num_unique_digests as f64) * scale).max(1.0) as usize;
    Workload {
        name: w.name,
        num_files: nf,
        num_unique_digests: nu.min(nf),
        file_size_bytes: w.file_size_bytes,
        fetch_latency: w.fetch_latency,
    }
}

async fn run_once(is_baseline: bool, fx: &Fixture, dest_root: &str) -> Result<(), Error> {
    fs::create_dir_all(dest_root)
        .await
        .err_tip(|| "create_dir_all")?;
    if is_baseline {
        baseline_download_to_directory(
            fx.cas_store.as_ref(),
            Pin::new(fx.fast_store.as_ref()),
            &fx.root_digest,
            dest_root,
        )
        .await
    } else {
        new_download_to_directory(
            fx.cas_store.as_ref(),
            Pin::new(fx.fast_store.as_ref()),
            &fx.root_digest,
            dest_root,
        )
        .await
    }
}

fn bench_download(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");
    let mut group = c.benchmark_group("download_to_directory");
    group.sample_size(10);

    for w_template in workloads() {
        let w = maybe_scaled(&w_template);
        // Total payload bytes across the workload (unique blobs only).
        let throughput_bytes = (w.num_unique_digests as u64) * (w.file_size_bytes as u64);
        group.throughput(Throughput::Bytes(throughput_bytes));

        // The blob bank (slow store + digests + dir proto) is shared across
        // all iterations of this workload. The FastSlowStore + FilesystemStore
        // are rebuilt per iteration so each iter exercises the cold-cache
        // path - matching production where every Bazel action arrives with
        // a unique input set.
        let bank = rt.block_on(build_blob_bank(&w)).expect("build_blob_bank");

        for (id_name, is_baseline) in [("baseline", true), ("new", false)] {
            let bench_id = BenchmarkId::new(id_name, w.name);
            group.bench_with_input(bench_id, &w.name, |b, _| {
                let iter_idx = std::cell::Cell::new(0u64);
                b.to_async(&rt).iter_custom(|iters| {
                    let bank = &bank;
                    let iter_idx = &iter_idx;
                    async move {
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            iter_idx.set(iter_idx.get() + 1);
                            // Fresh fast store + cas store per iter => cold
                            // cache for this iteration's run.
                            let fx = build_fixture(bank, iter_idx.get() as usize)
                                .await
                                .expect("build_fixture");
                            let dest = tempfile::tempdir().expect("tempdir");
                            let dest_root = dest
                                .path()
                                .join(format!("d_{}", iter_idx.get()))
                                .to_string_lossy()
                                .into_owned();
                            let t0 = std::time::Instant::now();
                            run_once(is_baseline, &fx, &dest_root)
                                .await
                                .expect("run_once");
                            total += t0.elapsed();
                            criterion::black_box(&dest_root);
                            drop(dest);
                            drop(fx);
                        }
                        total
                    }
                });
            });
        }
    }
    group.finish();
}

criterion_group!(benches, bench_download);
criterion_main!(benches);
