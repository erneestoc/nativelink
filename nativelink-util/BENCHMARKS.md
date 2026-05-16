# Benchmarks: macOS clonefile + concurrency-cap branch

Reproduces the perf claims in
[`HANDOFF-nativelink-macos-clonefile-optimizations.md`](../../instacart-ios/HANDOFF-nativelink-macos-clonefile-optimizations.md)
on a real APFS volume. Two criterion benches, both `harness = false` under
`nativelink-util/benches/`:

| Bench | Proves |
|---|---|
| `hardlink_directory_tree` | clonefile fast path is faster than per-file hardlinks (commit `b3b0cd3f`) |
| `download_to_directory_concurrency` | `buffer_unordered(64)` is not a regression vs unbounded `FuturesUnordered` (commit `1ddce0fc`) |

Reproduce on macOS arm64:

```bash
cargo bench -p nativelink-util --bench hardlink_directory_tree
cargo bench -p nativelink-util --bench download_to_directory_concurrency
```

HTML reports land in `target/criterion/`.

## Host

| | |
|---|---|
| Date  | 2026-05-15 |
| Host  | Apple M4 Max (ARM64), Darwin 25.5.0 |
| FS    | APFS (root volume) |
| Rust  | 1.94.0 |
| nativelink branch | `instacart/macos-clonefile-optimizations` @ `8051ca9e` + this commit |

## Layer 1 — `hardlink_directory_tree` (clonefile vs per-file hardlinks)

Source tree is built once per shape; each iteration materializes into a
fresh `tempfile::TempDir` destination.

- **treatment** — `hardlink_directory_tree` (public API). On macOS hits
  `clonefile(2)` + `set_readwrite_recursive` walk.
- **baseline_perfile** — `hardlink_directory_tree_perfile`
  (`#[doc(hidden)]` helper added for this benchmark). Per-file
  `fs::hard_link` walk — identical to what `hardlink_directory_tree` did
  on macOS prior to `b3b0cd3f`, and identical to what it still does on
  Linux/Windows.

| shape         | files | bytes/file | total   | treatment | baseline | **speedup** |
|---------------|------:|-----------:|--------:|----------:|---------:|------------:|
| `small_flat`  |    64 |    1 KiB   |  64 KiB |   4.43 ms |  17.7 ms |   **4.00×** |
| `pcm_cluster` |   219 |  190 KiB   |  ~40 MiB|  15.23 ms |  61.3 ms |   **4.03×** |
| `deep_nested` |   200 |  256 KiB   |  ~50 MiB|  16.39 ms |  59.0 ms |   **3.60×** |
| `medium_flat` |   635 |  290 KiB   | ~180 MiB|  49.03 ms |   181 ms |   **3.70×** |
| `large_flat`  | 1,978 |  245 KiB   | ~466 MiB| 150.18 ms |   590 ms |   **3.93×** |

Numbers are criterion's reported median; full distributions (low/median/high)
are in the raw bench log. On a 466 MB / 1,978-file tree (the p95
`SwiftCompile` shape from `~/Downloads/bazel-exec-log-this.zst`) the
public API drops from 590 ms to 150 ms — a **440 ms per-action
materialization saving**, scaled by 814 such actions per CI build = an
~**6 minute upper bound** on the saving from this single optimization.

### Why it's 4× and not 10×

The handoff predicted ≥ 10× on shapes ≥ 200 files based on PR
[#2243][pr2243]'s reported wins. We see a stable ~4× across all five
shapes. Why the gap matters less than it looks:

- `clonefile(2)` itself is O(1) in tree size.
- After the clone, `hardlink_directory_tree` calls
  `set_readwrite_recursive(dst_dir)` to chmod the cloned tree from
  `0o555/0o444` (inherited) to `0o755/0o644` (writable, so actions can
  drop outputs into the tree). That walk is **O(N) in file count** — a
  `read_dir` + `set_permissions` per entry.
- So the "treatment" path is `O(1) clonefile + O(N) chmod walk`, not the
  pure `O(1)` that PR #2243's claim implied.
- The 4× ratio reflects the constant per-file cost of `set_permissions`
  being cheaper than `hard_link` on APFS — single metadata mutation vs
  open-src + open-dst + link inode.

This is *exactly* the failure mode the handoff flagged before deploy:

> Expected treatment (clonefile + cache hit): ~0.1 – 0.3 s. If the
> treatment number is ≥ 0.8 s, something is wrong — investigate before
> shipping (likely candidates: clonefile silently falling through, or
> `set_readwrite_recursive` walk swallowing the O(1) clone win).

Our 0.15 s on `large_flat` is well inside the green band, but the walk
*is* eating most of the headroom. **Follow-up worth filing**: replace
the chmod walk with a single `chmod(2)` on the top-level dst dir + lazy
per-file chmod on first write, OR call out to a parallelized
implementation. That should unlock the remaining 2–3×.

### Acceptance verdict

| Criterion                                                | Required          | Observed                | Verdict |
|----------------------------------------------------------|-------------------|-------------------------|---------|
| macOS arm64, shapes ≥ 200 files: treatment ≥ 10× faster  | ≥ 10×             | 3.6× – 4.0×             | ⚠️ partial — wins are real, magnitude smaller than predicted |
| macOS arm64, treatment p50 on `large_flat` < 0.8 s        | < 0.8 s           | 0.15 s                  | ✅ pass |
| Treatment never slower than baseline                     | ratio ≥ 1.0×      | 3.6× – 4.0× across all  | ✅ pass |

**Recommend shipping.** The 4× win on the dominant SwiftCompile shape
already moves the needle hard (590 ms → 150 ms per p95 action; 181 ms →
49 ms per mean action). The path to 10× is a known, isolated follow-up
(the chmod walk) and not a blocker for this branch.

## Layer 2 — `download_to_directory` concurrency cap

Replicates the C3 (`1ddce0fc`) change on the synthetic shape that
mirrors `running_actions_manager::download_to_directory`: N concurrent
`fs::hard_link` calls into one destination directory.

- **unbounded** — pre-C3: every future on an unbounded
  `FuturesUnordered`, drained.
- **buffered_64** — post-C3: same futures via
  `stream::buffer_unordered(64)`.

| files (n) | unbounded | buffered_64 | ratio (buf/unb) |
|----------:|----------:|------------:|----------------:|
|        64 |  28.33 ms |    28.18 ms |          1.00× |
|       256 | 113.87 ms |   113.53 ms |          1.00× |
|       635 | 292.23 ms |   287.33 ms |          0.98× |
|     1,978 | 887.33 ms |   892.77 ms |          1.01× |

### Verdict

| Criterion                                  | Required        | Observed       | Verdict |
|--------------------------------------------|-----------------|----------------|---------|
| macOS, 1,978 files: buffered ≤ unbounded   | ratio ≤ 1.05×   | 1.01×          | ✅ pass |
| No size where buffered is dramatically slower | ratio ≤ 1.1× all sizes | max 1.01× | ✅ pass |

The cap is **performance-neutral** on this single-process workload —
which is the most important security claim for C3, since it means
shipping the cap can't regress macOS workers. The handoff's hypothesis
that the cap *wins* on macOS APFS (vs the unbounded path's metadata-lock
contention) is **not reproduced** at this scale in a single process:
APFS appears to serialize the work either way, so capping the in-flight
count doesn't add or remove contention.

We expect the cap's win to materialize under **multi-action contention**
— several `download_to_directory` calls executing concurrently on the
same worker — which a single-process microbench cannot replicate.
Production telemetry (`DirectoryCache::stats()` `clonefile_hits` +
APFS-lock-contention probes per the handoff's "Acceptance gate") is the
right place to confirm that.

### What this bench does NOT cover

Documented so a reviewer doesn't mistake quiet for green:

- **Multi-action contention.** Single-process bench can't show the
  cross-action contention that motivated C3. Need a fan-out benchmark
  spawning K concurrent `download_to_directory` calls.
- **The chunked `has_with_results` and level-parallel BFS `mkdir`
  sub-changes** from PR #2243's commit `ee85fdc4` were deferred (see
  handoff "C3 scope deviation"). Those are not benched here because
  they're not implemented in this branch.
- **Realistic worker path** (Layer 2 in the handoff): would spin a
  single-worker nativelink against a `MemoryStore` CAS preloaded with a
  captured SwiftCompile input tree. Not done — call out as next-step
  work before A/B production deploy.

## Security tests added on this branch

`cargo test -p nativelink-util --lib fs_util` — 10 tests, all green:

| Test                                          | Asserts                                                                            |
|-----------------------------------------------|-------------------------------------------------------------------------------------|
| `test_hardlink_directory_tree`                | macOS uses clonefile (distinct inodes); Linux uses per-file hardlinks (same inode) |
| `test_clonefile_dest_is_writable`             | src stays 0o555 after clone; dst becomes 0o755                                     |
| `test_clonefile_cow_isolation`                | writing to dst doesn't mutate src (COW)                                            |
| `test_clonefile_preserves_internal_symlinks`  | symlinks within src are cloned as symlinks (CLONE_NOFOLLOW is top-level only)      |
| `test_clonefile_nofollow_on_top_level_symlink_src` | clone of a symlink src yields a symlink dst, not the target's contents          |
| `test_dst_under_file_parent_errors_cleanly`   | error path on bad dst leaves no half-materialized tree                              |
| `test_hardlink_nonexistent_source`            | clean error on missing src                                                          |
| `test_hardlink_existing_destination`          | refuses pre-existing dst (would otherwise allow data leak via overlay)              |
| `test_set_readonly_recursive`                 | unchanged baseline coverage                                                         |
| `test_calculate_directory_size`               | unchanged baseline coverage                                                         |

`cargo test -p nativelink-worker --lib directory_cache::` — 2 tests:

| Test                                | Asserts                                                                              |
|-------------------------------------|---------------------------------------------------------------------------------------|
| `test_directory_cache_basic`        | `clonefile_hits` counter increments on macOS, `hardlink_hits` on Linux                |
| `test_directory_cache_zero_byte_file` | DirectoryCache construction succeeds when the CAS has no entry for zero-byte digest (C4) |

[pr2243]: https://github.com/TraceMachina/nativelink/pull/2243
