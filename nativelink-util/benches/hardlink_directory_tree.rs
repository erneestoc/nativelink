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

//! Layer-1 microbenchmark for `hardlink_directory_tree`.
//!
//! Compares the public `hardlink_directory_tree` (clonefile fast path on
//! macOS, per-file `hard_link` on Linux/Windows) against the per-file
//! hardlink path (`hardlink_directory_tree_perfile`, exposed for
//! benchmarking only) across tree shapes that mirror real Bazel
//! SwiftCompile action input sets observed in
//! `~/Downloads/bazel-exec-log-this.zst`:
//!
//! | shape         | files | size   | mirrors                       |
//! |---------------|-------|--------|-------------------------------|
//! | small_flat    |    64 |  64 KB | small SwiftCompile            |
//! | medium_flat   |   635 | 180 MB | mean SwiftCompile             |
//! | large_flat    |  1978 | 466 MB | p95 SwiftCompile              |
//! | deep_nested   |   200 |  50 MB | recursion + per-level cap     |
//! | pcm_cluster   |   219 |  40 MB | SwiftPrecompileCModule output |
//!
//! Acceptance (from `HANDOFF-nativelink-macos-clonefile-optimizations.md`):
//! - macOS arm64: treatment ≥ 10× faster on shapes ≥ 200 files.
//! - Linux:       treatment within ±5% of baseline (same code path).

#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::cargo,
    clippy::restriction,
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::print_stdout,
    missing_docs
)]

use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use nativelink_util::fs_util::{hardlink_directory_tree, hardlink_directory_tree_perfile};
use rand::RngCore;
use rand::rngs::SmallRng;
use rand::SeedableRng;
use tempfile::TempDir;
use tokio::runtime::Runtime;

/// One persistent runtime + tempdir holder so source trees aren't rebuilt
/// across the two functions in the comparison. `OnceLock` is enough: the
/// runtime is `Send`/`Sync` and we never need to mutate it after init.
fn runtime() -> &'static Runtime {
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(num_cpus_or_8())
            .enable_all()
            .build()
            .expect("tokio runtime")
    })
}

fn num_cpus_or_8() -> usize {
    std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(8)
}

/// Shape descriptor — file count, average file size, and tree depth. Depth
/// 1 means all files in one directory; depth N produces a chain of N nested
/// dirs each holding `files/N` files.
#[derive(Clone, Copy)]
struct Shape {
    name: &'static str,
    files: usize,
    bytes_per_file: usize,
    depth: usize,
}

const SHAPES: &[Shape] = &[
    Shape {
        name: "small_flat",
        files: 64,
        bytes_per_file: 1024, // 1 KB
        depth: 1,
    },
    Shape {
        name: "pcm_cluster",
        files: 219,
        bytes_per_file: 190 * 1024, // ~40 MB total
        depth: 1,
    },
    Shape {
        name: "deep_nested",
        files: 200,
        bytes_per_file: 256 * 1024, // ~50 MB total
        depth: 5,
    },
    Shape {
        name: "medium_flat",
        files: 635,
        bytes_per_file: 290 * 1024, // ~180 MB total
        depth: 1,
    },
    Shape {
        name: "large_flat",
        files: 1978,
        bytes_per_file: 245 * 1024, // ~466 MB total
        depth: 1,
    },
];

/// Build the source tree for a shape in `root/src`. Synchronous, blocking
/// — runs once per shape outside the bench loop. Returns the src path.
fn build_source_tree(root: &Path, shape: Shape) -> PathBuf {
    use std::fs;
    use std::io::Write;

    let src = root.join("src");
    fs::create_dir_all(&src).expect("create src");

    // File payload — random bytes so APFS can't trivially dedup at the
    // block layer and skew measurements. Seeded for reproducibility.
    let mut rng = SmallRng::seed_from_u64(0xC10E_F11E);
    let mut payload = vec![0u8; shape.bytes_per_file];
    rng.fill_bytes(&mut payload);

    // Build per-depth-level directory chain: src/d0/d1/.../d{depth-1}/.
    // Distribute files round-robin across the leaf dirs (depth=1 → one leaf).
    let leaf_dirs: Vec<PathBuf> = if shape.depth == 1 {
        vec![src.clone()]
    } else {
        let mut leaves = Vec::with_capacity(shape.depth);
        let mut cur = src.clone();
        for level in 0..shape.depth {
            cur = cur.join(format!("d{level}"));
            fs::create_dir_all(&cur).expect("create level");
            leaves.push(cur.clone());
        }
        leaves
    };

    for i in 0..shape.files {
        let leaf = &leaf_dirs[i % leaf_dirs.len()];
        let path = leaf.join(format!("f{i:05}.bin"));
        let mut f = fs::File::create(&path).expect("create file");
        f.write_all(&payload).expect("write file");
    }

    src
}

fn bench_shape(c: &mut Criterion, shape: Shape) {
    // Build source once outside the bench loop. The TempDir lives until the
    // closure that owns it returns at end of `bench_shape`, so the src tree
    // persists across all criterion samples. Wrap in `Arc` so each batched
    // setup can clone it cheaply into the async closure.
    let src_holder = TempDir::new().expect("src tempdir");
    let src = Arc::new(build_source_tree(src_holder.path(), shape));

    let rt = runtime();

    let mut group = c.benchmark_group(format!("hardlink_directory_tree/{}", shape.name));
    let total_bytes = (shape.files * shape.bytes_per_file) as u64;
    group.throughput(Throughput::Bytes(total_bytes));
    // Larger trees are slow and disk-heavy; cap sample size and warmup.
    let (samples, warmup_secs, measurement_secs) = match shape.files {
        n if n <= 100 => (50_usize, 1u64, 4u64),
        n if n <= 300 => (30_usize, 1u64, 5u64),
        n if n <= 800 => (20_usize, 2u64, 8u64),
        _ => (10_usize, 2u64, 12u64),
    };
    group.sample_size(samples);
    group.warm_up_time(Duration::from_secs(warmup_secs));
    group.measurement_time(Duration::from_secs(measurement_secs));

    // Treatment — the public API. On macOS hits clonefile(2); on Linux
    // falls through to the per-file path (identical to baseline).
    let src_t = Arc::clone(&src);
    group.bench_with_input(
        BenchmarkId::new("treatment", shape.name),
        &shape,
        move |b, _| {
            let src_t = Arc::clone(&src_t);
            b.to_async(rt).iter_batched(
                || {
                    let dst_holder = TempDir::new().expect("dst tempdir");
                    let dst = dst_holder.path().join("dst");
                    (dst_holder, dst, Arc::clone(&src_t))
                },
                |(dst_holder, dst, src_t)| async move {
                    hardlink_directory_tree(&src_t, &dst)
                        .await
                        .expect("hardlink_directory_tree");
                    drop(dst_holder);
                },
                BatchSize::PerIteration,
            );
        },
    );

    // Baseline — per-file hardlink walk regardless of platform. This is
    // what `hardlink_directory_tree` does today on Linux and what it did
    // on macOS prior to commit b3b0cd3f (the clonefile fast path).
    let src_b = Arc::clone(&src);
    group.bench_with_input(
        BenchmarkId::new("baseline_perfile", shape.name),
        &shape,
        move |b, _| {
            let src_b = Arc::clone(&src_b);
            b.to_async(rt).iter_batched(
                || {
                    let dst_holder = TempDir::new().expect("dst tempdir");
                    let dst = dst_holder.path().join("dst");
                    (dst_holder, dst, Arc::clone(&src_b))
                },
                |(dst_holder, dst, src_b)| async move {
                    hardlink_directory_tree_perfile(&src_b, &dst)
                        .await
                        .expect("hardlink_directory_tree_perfile");
                    drop(dst_holder);
                },
                BatchSize::PerIteration,
            );
        },
    );

    group.finish();
}

fn bench_all_shapes(c: &mut Criterion) {
    for shape in SHAPES {
        bench_shape(c, *shape);
    }
}

criterion_group!(benches, bench_all_shapes);
criterion_main!(benches);
