// Copyright 2024 The NativeLink Authors. All rights reserved.
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

//! Head-to-head benchmarks: `EvictingMap` vs `MokaEvictingMap`.
//!
//! Workloads mimic `NativeLink` shapes (`SwiftCompile` mean 183 KB / p95
//! 466 KB input blobs, mixed read/write under sustained pressure). Run with
//! `cargo bench --bench evicting_map_comparison -p nativelink-util`.
//!
//! The bench uses `tokio::spawn` directly. The `nativelink-util::task`
//! wrappers add per-spawn span+context machinery that adds overhead
//! orthogonal to what we're measuring. Justified.

#![allow(
    clippy::disallowed_methods,
    reason = "criterion benches need raw tokio::spawn for measurement parity"
)]
#![allow(clippy::missing_docs_in_private_items, reason = "bench-local helpers")]
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    reason = "bench-local numeric conversions on known-small bench parameters"
)]
#![allow(
    clippy::missing_const_for_fn,
    reason = "bench helpers are simple and not perf-sensitive"
)]

use std::sync::{Arc, OnceLock};
use std::time::SystemTime;

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use nativelink_config::stores::EvictionPolicy;
use nativelink_util::evicting_map::{EvictingMap, LenEntry};
use nativelink_util::moka_evicting_map::MokaEvictingMap;
use rand::SeedableRng;
use rand::distr::{Distribution, Uniform};
use rand::rngs::StdRng;
use tokio::runtime::Runtime;

#[derive(Clone, Debug)]
struct BytesWrapper(Bytes);

impl LenEntry for BytesWrapper {
    fn len(&self) -> u64 {
        self.0.len() as u64
    }
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// Realistic blob size distribution mixing `SwiftCompile` mean (183 KB) and
/// p95 (466 KB). 90% mean-shaped, 10% p95-shaped.
fn blob_size(rng: &mut StdRng) -> usize {
    let dice: f64 = rand::Rng::random_range(rng, 0.0..1.0);
    let jitter: f64 = rand::Rng::random_range(rng, 0.75..1.25);
    let base_kb = if dice < 0.9 { 183.0 } else { 466.0 };
    (base_kb * 1024.0 * jitter) as usize
}

fn make_blob(rng: &mut StdRng) -> Bytes {
    Bytes::from(vec![0u8; blob_size(rng)])
}

/// Zipfian-1 sampler over `[0, n)`. Good-enough approximation: pick
/// uniformly from a power-law CDF table built once.
struct ZipfSampler {
    cdf: Vec<f64>,
    n: usize,
}

impl ZipfSampler {
    fn new(n: usize, s: f64) -> Self {
        let mut weights: Vec<f64> = (1..=n).map(|i| 1.0 / (i as f64).powf(s)).collect();
        let total: f64 = weights.iter().sum();
        for w in &mut weights {
            *w /= total;
        }
        let mut cdf = Vec::with_capacity(n);
        let mut acc = 0.0;
        for w in &weights {
            acc += w;
            cdf.push(acc);
        }
        Self { cdf, n }
    }
    fn sample(&self, rng: &mut StdRng) -> usize {
        let u: f64 = rand::Rng::random_range(rng, 0.0..1.0);
        // binary search
        match self.cdf.binary_search_by(|a| a.partial_cmp(&u).unwrap()) {
            Ok(i) | Err(i) => i.min(self.n - 1),
        }
    }
}

// ---------- Setup helpers ----------

const PREPOP_KEYS: usize = 10_000;
const THREAD_COUNT: usize = 16;
// Smaller per-thread op counts to keep the bench under 10-15 minutes.
const OPS_PER_THREAD_READ: u64 = 2_000;
const OPS_PER_THREAD_WRITE: u64 = 500;

fn cap_bytes() -> u64 {
    // ~500 MB working set headroom — big enough that pure-read benches
    // don't evict, small enough that eviction-pressure benches actually
    // evict.
    500 * 1024 * 1024
}

fn build_eviction_policy_full() -> EvictionPolicy {
    EvictionPolicy {
        max_bytes: usize::try_from(cap_bytes()).unwrap(),
        evict_bytes: usize::try_from(cap_bytes() / 10).unwrap(),
        max_seconds: 0,
        max_count: 0,
    }
}

fn build_eviction_policy_tight() -> EvictionPolicy {
    // 80% of working set — forces continuous eviction.
    let cap = (PREPOP_KEYS as u64) * 200 * 1024 * 8 / 10;
    EvictionPolicy {
        max_bytes: usize::try_from(cap).unwrap(),
        evict_bytes: usize::try_from(cap / 10).unwrap(),
        max_seconds: 0,
        max_count: 0,
    }
}

/// Global multi-threaded tokio runtime shared across all bench iterations.
/// Built once to avoid repeated worker-thread spin-up cost per iter.
fn rt() -> &'static Runtime {
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(num_worker_threads())
            .build()
            .unwrap()
    })
}

fn num_worker_threads() -> usize {
    std::thread::available_parallelism()
        .map(core::num::NonZeroUsize::get)
        .unwrap_or(4)
        .min(8)
}

fn build_old(policy: &EvictionPolicy) -> Arc<EvictingMap<u64, u64, BytesWrapper, SystemTime>> {
    Arc::new(EvictingMap::new(policy, SystemTime::now()))
}

fn build_new(policy: &EvictionPolicy) -> Arc<MokaEvictingMap<u64, u64, BytesWrapper, SystemTime>> {
    let m = Arc::new(MokaEvictingMap::with_anchor(policy, SystemTime::now()));
    let _enter = rt().enter();
    m.start_background_eviction();
    m
}

fn prepopulate_old(map: &EvictingMap<u64, u64, BytesWrapper, SystemTime>, n: usize) {
    let mut rng = StdRng::seed_from_u64(0xCAFE);
    rt().block_on(async {
        for i in 0..n {
            map.insert(i as u64, BytesWrapper(make_blob(&mut rng)))
                .await;
        }
    });
}

fn prepopulate_new(map: &MokaEvictingMap<u64, u64, BytesWrapper, SystemTime>, n: usize) {
    let mut rng = StdRng::seed_from_u64(0xCAFE);
    for i in 0..n {
        map.insert_startup(i as u64, BytesWrapper(make_blob(&mut rng)));
    }
    map.run_pending_tasks();
}

// ---------- Workload bodies ----------

fn read_heavy_uniform(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_heavy_uniform");
    group.throughput(Throughput::Elements(
        (THREAD_COUNT as u64) * OPS_PER_THREAD_READ,
    ));
    let policy = build_eviction_policy_full();

    // ---- old ----
    let old = build_old(&policy);
    prepopulate_old(&old, PREPOP_KEYS);
    group.bench_function(BenchmarkId::new("evicting_map", "uniform"), |b| {
        b.iter(|| {
            rt().block_on(async {
                let mut handles = Vec::new();
                for _t in 0..THREAD_COUNT {
                    let m = Arc::clone(&old);
                    handles.push(tokio::spawn(async move {
                        let mut rng = StdRng::seed_from_u64(0xDEAD);
                        let u = Uniform::new(0u64, PREPOP_KEYS as u64).unwrap();
                        for _ in 0..OPS_PER_THREAD_READ {
                            let k = u.sample(&mut rng);
                            criterion::black_box(m.get(&k).await);
                        }
                    }));
                }
                for h in handles {
                    h.await.unwrap();
                }
            });
        });
    });

    // ---- new ----
    let new = build_new(&policy);
    prepopulate_new(&new, PREPOP_KEYS);
    group.bench_function(BenchmarkId::new("moka_evicting_map", "uniform"), |b| {
        b.iter(|| {
            rt().block_on(async {
                let mut handles = Vec::new();
                for _t in 0..THREAD_COUNT {
                    let m = Arc::clone(&new);
                    handles.push(tokio::spawn(async move {
                        let mut rng = StdRng::seed_from_u64(0xDEAD);
                        let u = Uniform::new(0u64, PREPOP_KEYS as u64).unwrap();
                        for _ in 0..OPS_PER_THREAD_READ {
                            let k = u.sample(&mut rng);
                            criterion::black_box(m.get(&k));
                        }
                    }));
                }
                for h in handles {
                    h.await.unwrap();
                }
            });
        });
    });
    group.finish();
}

fn read_heavy_zipfian(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_heavy_zipfian");
    group.throughput(Throughput::Elements(
        (THREAD_COUNT as u64) * OPS_PER_THREAD_READ,
    ));
    let policy = build_eviction_policy_full();
    let sampler = Arc::new(ZipfSampler::new(PREPOP_KEYS, 1.0));

    let old = build_old(&policy);
    prepopulate_old(&old, PREPOP_KEYS);
    group.bench_function(BenchmarkId::new("evicting_map", "zipfian"), |b| {
        b.iter(|| {
            let sampler = Arc::clone(&sampler);
            rt().block_on(async {
                let mut handles = Vec::new();
                for _t in 0..THREAD_COUNT {
                    let m = Arc::clone(&old);
                    let s = Arc::clone(&sampler);
                    handles.push(tokio::spawn(async move {
                        let mut rng = StdRng::seed_from_u64(0xFEED);
                        for _ in 0..OPS_PER_THREAD_READ {
                            let k = s.sample(&mut rng) as u64;
                            criterion::black_box(m.get(&k).await);
                        }
                    }));
                }
                for h in handles {
                    h.await.unwrap();
                }
            });
        });
    });

    let new = build_new(&policy);
    prepopulate_new(&new, PREPOP_KEYS);
    group.bench_function(BenchmarkId::new("moka_evicting_map", "zipfian"), |b| {
        b.iter(|| {
            let sampler = Arc::clone(&sampler);
            rt().block_on(async {
                let mut handles = Vec::new();
                for _t in 0..THREAD_COUNT {
                    let m = Arc::clone(&new);
                    let s = Arc::clone(&sampler);
                    handles.push(tokio::spawn(async move {
                        let mut rng = StdRng::seed_from_u64(0xFEED);
                        for _ in 0..OPS_PER_THREAD_READ {
                            let k = s.sample(&mut rng) as u64;
                            criterion::black_box(m.get(&k));
                        }
                    }));
                }
                for h in handles {
                    h.await.unwrap();
                }
            });
        });
    });
    group.finish();
}

fn write_heavy_unique_keys(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_heavy_unique_keys");
    group.throughput(Throughput::Elements(
        (THREAD_COUNT as u64) * OPS_PER_THREAD_WRITE,
    ));
    let policy = build_eviction_policy_full();

    group.bench_function(BenchmarkId::new("evicting_map", "unique_keys"), |b| {
        b.iter(|| {
            let old = build_old(&policy);
            rt().block_on(async {
                let mut handles = Vec::new();
                for t in 0..THREAD_COUNT {
                    let m = Arc::clone(&old);
                    handles.push(tokio::spawn(async move {
                        let mut rng = StdRng::seed_from_u64(0x1234 + t as u64);
                        for i in 0..OPS_PER_THREAD_WRITE {
                            let k = (t as u64) * OPS_PER_THREAD_WRITE * 10 + i;
                            m.insert(k, BytesWrapper(make_blob(&mut rng))).await;
                        }
                    }));
                }
                for h in handles {
                    h.await.unwrap();
                }
            });
        });
    });

    group.bench_function(BenchmarkId::new("moka_evicting_map", "unique_keys"), |b| {
        b.iter(|| {
            let new = build_new(&policy);
            rt().block_on(async {
                let mut handles = Vec::new();
                for t in 0..THREAD_COUNT {
                    let m = Arc::clone(&new);
                    handles.push(tokio::spawn(async move {
                        let mut rng = StdRng::seed_from_u64(0x1234 + t as u64);
                        for i in 0..OPS_PER_THREAD_WRITE {
                            let k = (t as u64) * OPS_PER_THREAD_WRITE * 10 + i;
                            m.insert(k, BytesWrapper(make_blob(&mut rng)));
                        }
                    }));
                }
                for h in handles {
                    h.await.unwrap();
                }
            });
        });
    });
    group.finish();
}

fn mixed_80r_20w(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_80r_20w");
    group.throughput(Throughput::Elements(
        (THREAD_COUNT as u64) * OPS_PER_THREAD_READ,
    ));
    let policy = build_eviction_policy_full();
    let sampler = Arc::new(ZipfSampler::new(PREPOP_KEYS, 1.0));

    let old = build_old(&policy);
    prepopulate_old(&old, PREPOP_KEYS);
    group.bench_function(BenchmarkId::new("evicting_map", "80r_20w"), |b| {
        b.iter(|| {
            let sampler = Arc::clone(&sampler);
            rt().block_on(async {
                let mut handles = Vec::new();
                for t in 0..THREAD_COUNT {
                    let m = Arc::clone(&old);
                    let s = Arc::clone(&sampler);
                    handles.push(tokio::spawn(async move {
                        let mut rng = StdRng::seed_from_u64(0xACE + t as u64);
                        let mut write_key = (t as u64) * 1_000_000;
                        for i in 0..OPS_PER_THREAD_READ {
                            if i % 5 == 4 {
                                m.insert(write_key, BytesWrapper(make_blob(&mut rng))).await;
                                write_key += 1;
                            } else {
                                let k = s.sample(&mut rng) as u64;
                                criterion::black_box(m.get(&k).await);
                            }
                        }
                    }));
                }
                for h in handles {
                    h.await.unwrap();
                }
            });
        });
    });

    let new = build_new(&policy);
    prepopulate_new(&new, PREPOP_KEYS);
    group.bench_function(BenchmarkId::new("moka_evicting_map", "80r_20w"), |b| {
        b.iter(|| {
            let sampler = Arc::clone(&sampler);
            rt().block_on(async {
                let mut handles = Vec::new();
                for t in 0..THREAD_COUNT {
                    let m = Arc::clone(&new);
                    let s = Arc::clone(&sampler);
                    handles.push(tokio::spawn(async move {
                        let mut rng = StdRng::seed_from_u64(0xACE + t as u64);
                        let mut write_key = (t as u64) * 1_000_000;
                        for i in 0..OPS_PER_THREAD_READ {
                            if i % 5 == 4 {
                                m.insert(write_key, BytesWrapper(make_blob(&mut rng)));
                                write_key += 1;
                            } else {
                                let k = s.sample(&mut rng) as u64;
                                criterion::black_box(m.get(&k));
                            }
                        }
                    }));
                }
                for h in handles {
                    h.await.unwrap();
                }
            });
        });
    });
    group.finish();
}

fn concurrent_writes_same_key(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_writes_same_key");
    group.throughput(Throughput::Elements(
        (THREAD_COUNT as u64) * OPS_PER_THREAD_WRITE,
    ));
    let policy = build_eviction_policy_full();
    let shared_keys: Vec<u64> = (0..10u64).collect();

    let old = build_old(&policy);
    let shared_keys_arc = Arc::new(shared_keys.clone());
    group.bench_function(BenchmarkId::new("evicting_map", "10keys"), |b| {
        b.iter(|| {
            let keys = Arc::clone(&shared_keys_arc);
            rt().block_on(async {
                let mut handles = Vec::new();
                for t in 0..THREAD_COUNT {
                    let m = Arc::clone(&old);
                    let keys = Arc::clone(&keys);
                    handles.push(tokio::spawn(async move {
                        let mut rng = StdRng::seed_from_u64(0x5EED + t as u64);
                        for i in 0..OPS_PER_THREAD_WRITE {
                            let k = keys[usize::try_from(i).unwrap() % keys.len()];
                            m.insert(k, BytesWrapper(make_blob(&mut rng))).await;
                        }
                    }));
                }
                for h in handles {
                    h.await.unwrap();
                }
            });
        });
    });

    let new = build_new(&policy);
    let shared_keys_arc = Arc::new(shared_keys);
    group.bench_function(BenchmarkId::new("moka_evicting_map", "10keys"), |b| {
        b.iter(|| {
            let keys = Arc::clone(&shared_keys_arc);
            rt().block_on(async {
                let mut handles = Vec::new();
                for t in 0..THREAD_COUNT {
                    let m = Arc::clone(&new);
                    let keys = Arc::clone(&keys);
                    handles.push(tokio::spawn(async move {
                        let mut rng = StdRng::seed_from_u64(0x5EED + t as u64);
                        for i in 0..OPS_PER_THREAD_WRITE {
                            let k = keys[usize::try_from(i).unwrap() % keys.len()];
                            m.insert(k, BytesWrapper(make_blob(&mut rng)));
                        }
                    }));
                }
                for h in handles {
                    h.await.unwrap();
                }
            });
        });
    });
    group.finish();
}

fn eviction_pressure_filesystem_shape(c: &mut Criterion) {
    let mut group = c.benchmark_group("eviction_pressure_filesystem_shape");
    group.throughput(Throughput::Elements(
        (THREAD_COUNT as u64) * OPS_PER_THREAD_WRITE,
    ));
    let policy = build_eviction_policy_tight();

    group.bench_function(BenchmarkId::new("evicting_map", "tight"), |b| {
        b.iter(|| {
            let old = build_old(&policy);
            rt().block_on(async {
                let mut handles = Vec::new();
                for t in 0..THREAD_COUNT {
                    let m = Arc::clone(&old);
                    handles.push(tokio::spawn(async move {
                        let mut rng = StdRng::seed_from_u64(0xBEEF + t as u64);
                        for i in 0..OPS_PER_THREAD_WRITE {
                            let k = (t as u64) * OPS_PER_THREAD_WRITE + i;
                            m.insert(k, BytesWrapper(make_blob(&mut rng))).await;
                        }
                    }));
                }
                for h in handles {
                    h.await.unwrap();
                }
            });
        });
    });

    group.bench_function(BenchmarkId::new("moka_evicting_map", "tight"), |b| {
        b.iter(|| {
            let new = build_new(&policy);
            rt().block_on(async {
                let mut handles = Vec::new();
                for t in 0..THREAD_COUNT {
                    let m = Arc::clone(&new);
                    handles.push(tokio::spawn(async move {
                        let mut rng = StdRng::seed_from_u64(0xBEEF + t as u64);
                        for i in 0..OPS_PER_THREAD_WRITE {
                            let k = (t as u64) * OPS_PER_THREAD_WRITE + i;
                            m.insert(k, BytesWrapper(make_blob(&mut rng)));
                        }
                    }));
                }
                for h in handles {
                    h.await.unwrap();
                }
            });
        });
    });
    group.finish();
}

fn startup_insertion_atime_ordered(c: &mut Criterion) {
    let mut group = c.benchmark_group("startup_insertion_atime_ordered");
    group.throughput(Throughput::Elements(PREPOP_KEYS as u64));
    let policy = build_eviction_policy_full();

    group.bench_function(BenchmarkId::new("evicting_map", "startup"), |b| {
        b.iter(|| {
            let old = build_old(&policy);
            rt().block_on(async {
                let mut rng = StdRng::seed_from_u64(0x0001);
                for i in 0..PREPOP_KEYS {
                    let atime = i32::try_from(PREPOP_KEYS - i).unwrap_or(i32::MAX);
                    old.insert_with_time(i as u64, BytesWrapper(make_blob(&mut rng)), atime)
                        .await;
                }
            });
        });
    });

    group.bench_function(BenchmarkId::new("moka_evicting_map", "startup"), |b| {
        b.iter(|| {
            let new = build_new(&policy);
            let mut rng = StdRng::seed_from_u64(0x0001);
            for i in 0..PREPOP_KEYS {
                let atime = i32::try_from(PREPOP_KEYS - i).unwrap_or(i32::MAX);
                new.insert_with_time(i as u64, BytesWrapper(make_blob(&mut rng)), atime);
            }
            new.run_pending_tasks();
        });
    });
    group.finish();
}

fn configure_bench() -> Criterion {
    Criterion::default()
        .sample_size(10) // keep total run-time low
        .warm_up_time(core::time::Duration::from_secs(1))
        .measurement_time(core::time::Duration::from_secs(3))
}

criterion_group! {
    name = benches;
    config = configure_bench();
    targets =
        read_heavy_uniform,
        read_heavy_zipfian,
        write_heavy_unique_keys,
        mixed_80r_20w,
        concurrent_writes_same_key,
        eviction_pressure_filesystem_shape,
        startup_insertion_atime_ordered,
}
criterion_main!(benches);
