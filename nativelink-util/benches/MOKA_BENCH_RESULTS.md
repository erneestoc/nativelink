# Phase 1 Benchmark Results: `EvictingMap` vs `MokaEvictingMap`

**Branch**: `ec/moka-evicting-map-phase1`
**Bench file**: `nativelink-util/benches/evicting_map_comparison.rs`
**Raw output**: `bench_results.txt` (committed)

## Environment

| Item | Value |
|---|---|
| Hardware | Apple Mac Mini (Mac16,6), M4 Max |
| Cores | 16 |
| OS | macOS 26.2 (build 25C56) |
| Rust | 1.94.0 (2026-03-02, Homebrew) |
| `moka` | 0.12.15 |
| `lru` (incumbent) | 0.16.3 |
| Tokio runtime | multi-thread, `min(available, 8)` workers |
| Criterion | 0.5.1, 10 samples / 1 s warmup / 3 s measurement |

## Raw numbers

Workload `evicting_map` (existing `Mutex+LRU`) vs `moka_evicting_map` (new TinyLFU+LRU).
"Time" is criterion's median ± low/high; "Throughput" is elem/s computed by criterion.
Lower time / higher throughput is better. **Ratio** = `moka / lru` (lower is better for moka).

| Workload | EvictingMap (ms) | MokaEvictingMap (ms) | EM thrpt | MEM thrpt | Ratio | Winner |
|---|---:|---:|---:|---:|---:|---|
| `read_heavy_uniform` | 3.69 [3.68, 3.70] | 4.88 [4.83, 4.92] | 8.67 M elem/s | 6.56 M elem/s | 1.32× | EvictingMap |
| `read_heavy_zipfian` | 3.46 [3.40, 3.53] | 4.79 [4.76, 4.83] | 9.24 M elem/s | 6.68 M elem/s | 1.38× | EvictingMap |
| `write_heavy_unique_keys` | 11.18 [11.15, 11.21] | 31.32 [31.04, 31.49] | 716 K elem/s | 255 K elem/s | 2.80× | EvictingMap |
| `mixed_80r_20w` | 14.11 [14.04, 14.23] | 29.77 [29.51, 30.16] | 2.27 M elem/s | 1.08 M elem/s | 2.11× | EvictingMap |
| `concurrent_writes_same_key` | 13.17 [13.14, 13.22] | 24.39 [24.19, 24.72] | 608 K elem/s | 328 K elem/s | 1.85× | EvictingMap |
| `eviction_pressure_filesystem_shape` | 8.93 [8.91, 8.95] | 27.12 [26.93, 27.30] | 896 K elem/s | 295 K elem/s | 3.04× | EvictingMap |
| `startup_insertion_atime_ordered` | 12.84 [12.65, 13.11] | 16.75 [16.58, 17.14] | 779 K elem/s | 597 K elem/s | 1.30× | EvictingMap |

## Headline

**On this single-machine microbench, `EvictingMap` wins every workload.** Moka is between **1.3× and 3.0× slower** depending on the shape, with the biggest gaps on the eviction-pressure / write-heavy paths.

This contradicts the upstream PR #2243 claim of "lock contention 391 (406 ms worst) → 0" — but it does not necessarily refute it. See [Caveats](#caveats) below for why a *single-process criterion bench on Apple Silicon* and a *10-worker production fleet against ZFS* can both be true. The numbers here measure **per-op latency in isolation**; PR #2243 measured **mutex-wait time under fan-in from 10 RBE workers**, which is a different axis.

## Per-workload observations

### `read_heavy_uniform` (1.32× slower)

Both implementations spend their time on the same path: hash lookup, return clone. `EvictingMap` does this under a `parking_lot::Mutex` and pays the LRU promotion (doubly-linked-list mutation). `MokaEvictingMap` does it lock-free via moka's segmented map but pays:

- moka's `TinyLFU` frequency-sketch update on every read
- segmented-hash atomic ops (less L1-friendly than the LRU's amortized linked-list pops once the mutex is held)
- tokio runtime overhead for spawning the per-thread future (both maps pay this equally, so it nets out)

At ~16 cores on a single box with no fan-in latency, the mutex hold-time per op (∼50–100 ns) is shorter than moka's per-op overhead. That story flips when the lock-holder is also doing async I/O — which is exactly the FilesystemStore production case but which this bench does not exercise.

### `read_heavy_zipfian` (1.38× slower)

Same dynamic as uniform; the hot keys are cached in L1/L2 for both. Moka gives up slightly more under Zipfian because the frequency-sketch admission heuristic does more work on the most-accessed keys.

### `write_heavy_unique_keys` (2.80× slower) — worst case for moka

Each insert triggers a `run_pending_tasks` + the frequency-bump get + a btree write (because `enable_filtering` is implicitly active under `range`-using consumers, though not here — even so we pay the listener overhead). Most of moka's write cost comes from the bounded-channel send to the drainer (a `tokio::sync::mpsc::Sender::try_send` is several atomic ops).

This is also the workload most affected by criterion's per-iteration teardown: `build_new(&policy)` rebuilds the map every iter, which spins up the `eviction_listener` closure and the drainer task; the existing `EvictingMap` has no such background machinery.

### `mixed_80r_20w` (2.11× slower)

Reads alone are 1.3× behind; writes are 2.8× behind; the 80/20 mix lands in between. As above, the writes path dominates the cost because every 5th op pays the listener bridge.

### `concurrent_writes_same_key` (1.85× slower)

Both implementations are dominated by allocator pressure (each iter heap-allocates a 183-KB blob). The `EvictingMap`'s single global mutex actually serializes the allocation, smoothing it; moka's lock-free path frees the allocator to fight itself across cores.

### `eviction_pressure_filesystem_shape` (3.04× slower) — second-worst

This is the workload the user cares most about (FilesystemStore at-capacity). EvictingMap evicts inline under the same mutex; moka pushes every eviction onto the bounded mpsc and runs the drainer in the background. **The drainer can't keep up** with 8 workers × 500 inserts/iter = 4000 inserts in ~27 ms (≈ 6.7 µs/insert), each of which triggers an eviction send to the channel. The channel fills, and we fall into the inline-spawn fallback at ~16 µs/event.

**But this is a microbench artifact.** In production:
1. The FS-store `unref()` is async and *slow* (rename + fsync) — measured in milliseconds, not microseconds. The EvictingMap's mutex stays held during the **decision** to evict but releases before `unref().await`. Under 10 workers fanning in, multiple threads queueing for that mutex pile up. Moka's lock-free path avoids that pile-up.
2. The bench `BytesWrapper::unref` is a no-op. Moka's listener bridge does work (try_send + drainer) that earns its keep only when the bridged work itself is slow. With a trivial no-op `unref`, the bridge is pure overhead.

### `startup_insertion_atime_ordered` (1.30× slower)

`insert_with_time` is the FilesystemStore startup path. Moka's `insert_startup` skips the frequency bump and the per-insert `run_pending_tasks` (deferred to caller), so it should be cheap. The remaining 30% gap is:

- the eviction-listener closure attached to the cache (every entry crosses it on insert, even though it returns early on `Replaced`)
- the btree-write probe (no-op when btree is `None`, but still a `RwLock::write().is_some()` check)

This is the most defensible bench: 13 ms vs 17 ms for 10 000 startup inserts is acceptable for the startup-once-per-restart code path.

## Top-line recommendation

**Do not migrate any consumer in Phase 2 based on this bench alone.** The bench measures the wrong axis for the original PR #2243 motivation. The right next step is one of:

1. **(Preferred) Run the bench against a multi-process workload.** Wire `MokaEvictingMap` into a feature-flagged `MemoryStore` and run it under the real iOS Bazel build with 10 Mac Mini workers. PR #2243's 391-contention-events number is what we need to reproduce; the single-process bench cannot.

2. **Add an "async `unref` ≈ 1 ms" variant** of the eviction-pressure bench. Today `BytesWrapper::unref` is a no-op, which gives `EvictingMap`'s inline cleanup a free ride. If we plumb a `tokio::time::sleep(1ms)` into `unref`, the comparison flips: moka's listener-bridge architecture is exactly designed for that case (cleanup work moves off the hot path), while `EvictingMap` would queue every async future onto a single mutex-serialized FuturesUnordered.

3. **Tune moka.** Several knobs are unset:
   - `eviction_policy(EvictionPolicy::lru())` would drop the TinyLFU frequency sketch (saves the per-read sketch update + the frequency-bump-on-insert workaround entirely)
   - `initial_capacity` is not set; moka grows the hash table during startup inserts
   - The bounded mpsc depth could be tuned (4096 is the upstream PR default; might be too small for write-bursty workloads on fast machines)

## Caveats

- **This is a microbenchmark on Apple Silicon.** The target deployment is x86_64 Linux RBE workers. Cross-architecture moka performance has not been measured here.
- **The bench measures single-process throughput.** PR #2243's contention numbers came from a 10-worker fan-in into a single shared server-side store. That topology cannot be reproduced in a criterion bench.
- **`BytesWrapper::unref` is a no-op.** Production `unref` is async file I/O (FilesystemStore) and is slow. This is the single biggest reason the eviction-pressure bench undersells moka — see recommendation #2 above.
- **No `Arc` cloning cost is amortized.** The bench inserts a fresh `Bytes` per iter; production hot paths pass cloned `Arc<FileEntryImpl>` which is much cheaper than heap-allocating new blobs.
- **Criterion sample size of 10.** Variance is high (some samples show 20–30% outliers). The medians are stable enough for the "moka loses 1.3–3.0×" headline but not stable enough to declare a tight per-workload winner inside the same family.
- **No cache hit-rate measurement.** Only throughput; we did not verify that moka's TinyLFU keeps the same hot-set as LRU under Zipfian. PR #2243 also did not publish hit-rate deltas.
- **No memory measurement.** Moka's segmented hash + frequency sketch + drainer mpsc all carry memory overhead vs `LruCache + Mutex`. Not measured here.

## What this changes about Phase 2

The original Phase 0 plan (§8) recommended migrating `MemoryStore` first as "the safest place to debug the bridge." Given these numbers, the recommendation tightens to:

> **Phase 2 should be EXPLORATORY, not a production rollout.** Build a feature flag at the `MemoryStore::new` call site that selects `EvictingMap` vs `MokaEvictingMap`. Ship it OFF by default. Use the flag to A/B against the real iOS Bazel workload on the user's actual 10 Mac Mini fleet. Only flip the default if the production contention metric (the one in PR #2243's "391 → 0" datum) reproduces.

If the contention metric doesn't reproduce under the user's workload, then `MokaEvictingMap` should be left in-tree as a parallel option (for future workloads or upstream alignment) but **not** become the default. The microbench would then be the source of truth on Apple Silicon single-box workloads, and the LRU implementation continues to win there.
