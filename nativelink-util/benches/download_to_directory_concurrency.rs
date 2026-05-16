// Copyright 2024 The NativeLink Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Microbenchmark that isolates the C3 change — bounded vs unbounded
//! concurrent `hard_link(2)` calls — without spinning up a full Store
//! backend. Replicates the exact pattern in
//! `running_actions_manager::download_to_directory`:
//!
//! - **unbounded** = pre-C3 (`FuturesUnordered` polled to completion)
//! - **buffered_64** = post-C3 (`stream::buffer_unordered(64)`)
//!
//! Hypothesis from `HANDOFF-nativelink-macos-clonefile-optimizations.md`:
//! on macOS APFS, thousands of parallel `hardlink(2)` syscalls fight the
//! per-volume metadata lock, so the unbounded path is *equal-or-slower*
//! than the 64-cap. The exec-log shape is ~4 ms per input file at scale
//! — the bench should reproduce something in that ballpark and show the
//! cap matches or beats the unbounded path on every input count.
//!
//! Acceptance:
//! - macOS arm64, 1978 files: buffered_64 ≤ unbounded (≤ 1.0× ratio).
//!   A win > 1.2× confirms the APFS metadata-lock theory; ≈ 1.0× still
//!   validates "the cap is not a regression."
//! - Linux: ratio within ±5%.

#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::cargo,
    clippy::restriction,
    clippy::expect_used,
    clippy::unwrap_used,
    missing_docs
)]

use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::Duration;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use futures::stream::{self, FuturesUnordered, StreamExt, TryStreamExt};
use tempfile::TempDir;
use tokio::fs;
use tokio::runtime::Runtime;

fn runtime() -> &'static Runtime {
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(
                std::thread::available_parallelism()
                    .map(std::num::NonZeroUsize::get)
                    .unwrap_or(8),
            )
            .enable_all()
            .build()
            .expect("tokio runtime")
    })
}

/// Mirrors the per-file futures in `download_to_directory`: a vector of
/// (src, dst) hardlink jobs over a flat directory of small files. Returns
/// the source paths.
fn build_source_files(root: &Path, n: usize) -> Vec<PathBuf> {
    use std::fs::File;
    use std::io::Write;

    let src_dir = root.join("src");
    std::fs::create_dir_all(&src_dir).expect("create src");

    let payload = vec![0u8; 1024]; // 1 KB — input files are small in practice.
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let p = src_dir.join(format!("f{i:05}.bin"));
        let mut f = File::create(&p).expect("create");
        f.write_all(&payload).expect("write");
        out.push(p);
    }
    out
}

/// Pre-C3 behavior: push every `hard_link` future into an unbounded
/// `FuturesUnordered`, then drain.
async fn hardlink_all_unbounded(src: &[PathBuf], dst_dir: &Path) {
    let mut futs: FuturesUnordered<_> = src
        .iter()
        .enumerate()
        .map(|(i, s)| {
            let d = dst_dir.join(format!("f{i:05}.bin"));
            let s = s.clone();
            async move { fs::hard_link(&s, &d).await }
        })
        .collect();
    while let Some(r) = futs.next().await {
        r.expect("hardlink");
    }
}

/// Post-C3 behavior: drive the same set of `hard_link` jobs through
/// `stream::buffer_unordered(CAP)` so at most CAP are in flight.
async fn hardlink_all_buffered<const CAP: usize>(src: &[PathBuf], dst_dir: &Path) {
    let jobs = src.iter().enumerate().map(|(i, s)| {
        let d = dst_dir.join(format!("f{i:05}.bin"));
        let s = s.clone();
        async move { fs::hard_link(&s, &d).await }
    });
    stream::iter(jobs)
        .buffer_unordered(CAP)
        .try_collect::<Vec<_>>()
        .await
        .expect("hardlink");
}

const INPUT_COUNTS: &[usize] = &[64, 256, 635, 1978];

fn bench_concurrency(c: &mut Criterion) {
    let rt = runtime();
    let src_holder = TempDir::new().expect("src tempdir");
    // Build the largest fixture once; smaller benches reuse a prefix.
    let max_n = *INPUT_COUNTS.iter().max().unwrap();
    let all_src = build_source_files(src_holder.path(), max_n);

    for &n in INPUT_COUNTS {
        let mut group = c.benchmark_group(format!("download_to_directory_concurrency/n={n}"));
        group.throughput(Throughput::Elements(n as u64));
        // hard_link is metadata only; scale samples down for large n so the
        // disk's inode pressure stays bounded.
        let samples = if n <= 256 { 60 } else { 30 };
        group.sample_size(samples);
        group.warm_up_time(Duration::from_secs(1));
        group.measurement_time(Duration::from_secs(if n <= 256 { 4 } else { 8 }));

        let src_slice = all_src[..n].to_vec();

        group.bench_with_input(
            BenchmarkId::new("unbounded", n),
            &src_slice,
            |b, src_slice| {
                b.to_async(rt).iter_batched(
                    || {
                        let dst_holder = TempDir::new().expect("dst tempdir");
                        let dst = dst_holder.path().join("dst");
                        std::fs::create_dir_all(&dst).expect("mk dst");
                        (dst_holder, dst, src_slice.clone())
                    },
                    |(dst_holder, dst, src_slice)| async move {
                        hardlink_all_unbounded(&src_slice, &dst).await;
                        drop(dst_holder);
                    },
                    BatchSize::PerIteration,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("buffered_64", n),
            &src_slice,
            |b, src_slice| {
                b.to_async(rt).iter_batched(
                    || {
                        let dst_holder = TempDir::new().expect("dst tempdir");
                        let dst = dst_holder.path().join("dst");
                        std::fs::create_dir_all(&dst).expect("mk dst");
                        (dst_holder, dst, src_slice.clone())
                    },
                    |(dst_holder, dst, src_slice)| async move {
                        hardlink_all_buffered::<64>(&src_slice, &dst).await;
                        drop(dst_holder);
                    },
                    BatchSize::PerIteration,
                );
            },
        );

        group.finish();
    }
}

criterion_group!(benches, bench_concurrency);
criterion_main!(benches);
