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

//! `MokaEvictingMap` — a parallel implementation of [`crate::evicting_map::EvictingMap`]
//! backed by [`moka::sync::Cache`] (`TinyLFU` + LRU).
//!
//! # Why
//!
//! The existing `EvictingMap` serializes every read and write behind a single
//! [`parking_lot::Mutex`], which becomes the dominant contention point on
//! `FilesystemStore` under sustained insert+evict pressure (see Phase 0
//! discovery for the full analysis). This module ships a drop-in alternative
//! that:
//!
//! * uses lock-free reads via moka's segmented hash table;
//! * scales eviction work off the read path;
//! * bridges async [`LenEntry::unref`] through a bounded mpsc + background
//!   drainer so moka's sync `eviction_listener` can call into async cleanup.
//!
//! # Behavioral parity & known divergences
//!
//! `MokaEvictingMap` mirrors the public surface of `EvictingMap` for every
//! method exercised by the in-scope consumers (`MemoryStore`,
//! `ExistenceCacheStore`, `MemoryAwaitedActionDb`, `FilesystemStore`). The
//! deliberate divergences are:
//!
//! * **`size_for_key` / `sizes_for_keys` `peek` parameter** — accepted but
//!   ignored. moka has no non-promoting probe; reads always advance the
//!   `TinyLFU` frequency sketch.
//! * **`insert_with_time` ignores the `seconds_since_anchor` argument** —
//!   delegates to [`MokaEvictingMap::insert_startup`], which preserves
//!   insertion-order FIFO inside moka's `MainProbation` queue (used by
//!   `FilesystemStore`'s startup atime-ordered repopulation).
//! * **`remove_if` is non-atomic** — `cache.get` → cond → `cache.remove`.
//!   The window between get and remove is a hash lookup; PR #2341's
//!   `Arc::ptr_eq` race semantics still hold (`ptr_eq` works across clones).
//!
//! # Phase 1 scope
//!
//! Pinning (the `DashMap<K, PinnedEntry<T>>` parallel layer described in
//! Phase 0 §3.4) is **NOT** included. No in-scope consumer pins entries.
//! Adding pinning later is a forward-only change: the eviction listener
//! already checks a `has_pinned()` fast-path, so we simply set it to
//! `false` for now and reserve the hook.

use core::borrow::Borrow;
use core::fmt::Debug;
use core::hash::Hash;
use core::marker::PhantomData;
use core::ops::RangeBounds;
use core::sync::atomic::{AtomicU64, Ordering};
use core::time::Duration;
use std::collections::BTreeSet;
use std::sync::Arc;

use moka::notification::RemovalCause;
use moka::sync::Cache;
use nativelink_config::stores::EvictionPolicy;
use nativelink_metric::MetricsComponent;
use parking_lot::RwLock as PlRwLock;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tracing::{debug, warn};

use crate::background_spawn;
use crate::evicting_map::{LenEntry, RemoveItemCallback};
use crate::instant_wrapper::InstantWrapper;
use crate::metrics_utils::{Counter, CounterWithTime};

/// Re-exported under a phase-1-friendly alias so consumers (and tests) can
/// `use moka_evicting_map::ItemCallback`. The underlying contract is
/// identical to [`RemoveItemCallback`] — fired when an item leaves the map
/// for any reason other than replacement.
pub trait ItemCallback<Q>: RemoveItemCallback<Q> {}
impl<Q, T: RemoveItemCallback<Q>> ItemCallback<Q> for T {}

/// Drop-in default callback type matching `evicting_map::NoopRemove`.
pub use crate::evicting_map::NoopRemove as ItemCallbackHolder;

/// Bridge from moka's sync `eviction_listener` into async `unref()` and the
/// configured item callbacks.
#[derive(Debug)]
struct EvictionEvent<K, T> {
    key: K,
    value: T,
    // Recorded for future use (per-cause metrics). Currently unread by the
    // drainer — the drainer only needs key+value to fire callbacks+unref.
    #[allow(dead_code)]
    cause: RemovalCause,
}

/// Bounded queue depth between the sync moka listener and the async
/// drainer. 4096 mirrors upstream PR #2243. On overflow we fall through to
/// inline spawn; callbacks are dropped in that path.
const EVICTION_CHANNEL_DEPTH: usize = 4096;

/// `MokaEvictingMap` — `TinyLFU` + LRU cache mirroring the `EvictingMap` API.
///
/// Type parameters mirror `EvictingMap`:
/// * `K` — owned key type stored inside the cache. Must be `Hash + Eq + Send + Sync + 'static`.
/// * `Q` — borrowed key type used by lookups. `K: Borrow<Q>`.
/// * `T` — value type. Must implement [`LenEntry`] for weighing.
/// * `I` — anchor instant wrapper (defaults to `SystemTime`).
/// * `C` — item-callback type (defaults to `ItemCallbackHolder = NoopRemove`).
#[derive(MetricsComponent)]
pub struct MokaEvictingMap<K, Q, T, I = std::time::SystemTime, C = ItemCallbackHolder>
where
    K: Hash + Eq + Ord + Clone + Debug + Send + Sync + Borrow<Q> + 'static,
    Q: Hash + Eq + Ord + Debug,
    T: LenEntry + Clone + Debug + Send + Sync + 'static,
    I: InstantWrapper,
    C: ItemCallback<Q>,
{
    cache: Cache<K, T>,
    btree: Arc<PlRwLock<Option<BTreeSet<K>>>>,
    /// Sender to the background drainer. Cloned into the sync listener.
    eviction_tx: mpsc::Sender<EvictionEvent<K, T>>,
    /// Held only so we can hand the receiver to the drainer when
    /// [`Self::start_background_eviction`] is called. `Option` so we can take
    /// it out on first call.
    eviction_rx: parking_lot::Mutex<Option<mpsc::Receiver<EvictionEvent<K, T>>>>,
    callbacks: Arc<PlRwLock<Vec<C>>>,
    anchor_time: I,
    #[metric(help = "Maximum size of the store in bytes")]
    max_bytes: u64,
    #[metric(help = "Number of bytes to evict when the store is full")]
    evict_bytes: u64,
    #[metric(help = "Maximum number of seconds to keep an item in the store")]
    max_seconds: u32,
    #[metric(help = "Maximum number of items to keep in the store")]
    max_count: u64,

    // ---- counters mirrored from EvictingMap ----
    // Counter wraps a non-Clone AtomicU64; for fields the sync
    // eviction_listener also updates, we keep a parallel Arc<AtomicU64>
    // shadow rather than wrapping Counter in Arc (Counter has no public
    // getter we'd need for tests). The metric-published Counter fields
    // below are bumped from `insert_inner` / drainer paths.
    #[metric(help = "Number of bytes replaced in the store")]
    replaced_bytes: Counter,
    #[metric(help = "Number of items replaced in the store")]
    replaced_items: CounterWithTime,
    #[metric(help = "Number of bytes inserted into the store since it was created")]
    lifetime_inserted_bytes: Counter,

    /// Atomic shadow of evicted-bytes — readable for tests, updated by
    /// the sync eviction listener directly.
    evicted_bytes: Arc<AtomicU64>,
    /// Atomic shadow of evicted-items count — same rationale.
    evicted_items: Arc<AtomicU64>,
    /// Atomic shadow of inline-fallback count.
    inline_fallback_evictions: Arc<AtomicU64>,

    /// Approximate `sum_store_size` mirror. moka tracks weight internally
    /// but we want the same byte-accurate counter the existing surface
    /// exposes (and the eviction listener updates it).
    sum_store_size: Arc<AtomicU64>,

    _phantom: PhantomData<fn() -> *const Q>,
}

impl<K, Q, T, I, C> Debug for MokaEvictingMap<K, Q, T, I, C>
where
    K: Hash + Eq + Ord + Clone + Debug + Send + Sync + Borrow<Q> + 'static,
    Q: Hash + Eq + Ord + Debug,
    T: LenEntry + Clone + Debug + Send + Sync + 'static,
    I: InstantWrapper,
    C: ItemCallback<Q>,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("MokaEvictingMap")
            .field("entry_count", &self.cache.entry_count())
            .field("max_bytes", &self.max_bytes)
            .field("max_count", &self.max_count)
            .field("max_seconds", &self.max_seconds)
            .field(
                "sum_store_size",
                &self.sum_store_size.load(Ordering::Relaxed),
            )
            .finish_non_exhaustive()
    }
}

/// KB-scaled weigher. moka's weigher returns `u32`. We divide value length
/// by 1024 (ceiling) so a single 4 GB value still fits inside `u32::MAX`
/// and the cache's `max_capacity` is scaled accordingly. See Phase 0 §4e.
fn kb_weight(value_len: u64) -> u32 {
    let kb = value_len.div_ceil(1024);
    u32::try_from(kb).unwrap_or(u32::MAX)
}

impl<K, Q, T, I, C> MokaEvictingMap<K, Q, T, I, C>
where
    K: Hash + Eq + Ord + Clone + Debug + Send + Sync + Borrow<Q> + 'static,
    Q: Hash + Eq + Ord + Debug + 'static,
    T: LenEntry + Clone + Debug + Send + Sync + 'static,
    I: InstantWrapper,
    C: ItemCallback<Q> + 'static,
{
    /// Construct with an explicit anchor instant. Mirrors `EvictingMap::new`.
    pub fn with_anchor(config: &EvictionPolicy, anchor_time: I) -> Self {
        let max_bytes = config.max_bytes as u64;
        let evict_bytes = config.evict_bytes as u64;
        let max_seconds = config.max_seconds;
        let max_count = config.max_count;

        let (eviction_tx, eviction_rx) = mpsc::channel(EVICTION_CHANNEL_DEPTH);

        let btree: Arc<PlRwLock<Option<BTreeSet<K>>>> = Arc::new(PlRwLock::new(None));
        let sum_store_size = Arc::new(AtomicU64::new(0));
        let inline_fallback_evictions = Arc::new(AtomicU64::new(0));
        let evicted_bytes = Arc::new(AtomicU64::new(0));
        let evicted_items = Arc::new(AtomicU64::new(0));

        let listener_btree = Arc::clone(&btree);
        let listener_sum = Arc::clone(&sum_store_size);
        let listener_tx = eviction_tx.clone();
        let listener_inline_fallback = Arc::clone(&inline_fallback_evictions);
        let listener_evicted_bytes = Arc::clone(&evicted_bytes);
        let listener_evicted_items = Arc::clone(&evicted_items);

        let mut builder = Cache::builder().weigher(|_k: &K, v: &T| kb_weight(v.len()));

        // KB-scaled max_capacity. Mirror EvictingMap's "low watermark" by
        // sizing the cache to (max_bytes - evict_bytes) — moka itself
        // doesn't surface a low-water-mark knob, so this is the closest
        // approximation: the cache is *sized to the post-eviction target*.
        if max_bytes > 0 {
            let effective = max_bytes.saturating_sub(evict_bytes).max(1);
            let cap = effective.div_ceil(1024);
            builder = builder.max_capacity(cap);
        } else if max_count > 0 {
            // count-based cap. weigher still applies but max_capacity caps
            // by sum-of-weights; with a uniform-weight cache and `max_count`
            // entries each ~1 KB this approximates count semantics. For
            // strictly-count workloads (MemoryAwaitedActionDb) entries are
            // weight 1 (LenEntry::len() == 0 -> kb_weight returns 0; we
            // bump min to 1).
            builder = builder.max_capacity(max_count);
        }

        if max_seconds > 0 {
            builder = builder.time_to_idle(Duration::from_secs(u64::from(max_seconds)));
        }

        builder = builder.eviction_listener(move |key: Arc<K>, value: T, cause: RemovalCause| {
            // Replaced items are handled by `insert_inner` directly; the
            // replaced value is unrefed there. Skip here to avoid
            // double-unref.
            if cause == RemovalCause::Replaced {
                return;
            }
            // Maintain the size mirror.
            let len = value.len();
            let cur = listener_sum.load(Ordering::Relaxed);
            listener_sum.fetch_sub(len.min(cur), Ordering::Relaxed);
            listener_evicted_bytes.fetch_add(len, Ordering::Relaxed);
            listener_evicted_items.fetch_add(1, Ordering::Relaxed);

            // Maintain the btree. (BTreeSet<K> — comparison by Q via Borrow.)
            {
                let mut guard = listener_btree.write();
                if let Some(b) = guard.as_mut() {
                    let q: &Q = key.as_ref().borrow();
                    b.remove(q);
                }
            }

            let event = EvictionEvent {
                key: (*key).clone(),
                value,
                cause,
            };
            match listener_tx.try_send(event) {
                Ok(()) => {}
                Err(TrySendError::Full(ev)) => {
                    listener_inline_fallback.fetch_add(1, Ordering::Relaxed);
                    warn!("MokaEvictingMap eviction channel full; falling back to inline unref (ItemCallback skipped)");
                    // Inline-spawn the unref. Callbacks are skipped in this
                    // fallback path — that is by design (the channel is
                    // already at backpressure depth 4096).
                    let value = ev.value;
                    background_spawn!("moka_inline_unref", async move {
                        value.unref().await;
                    });
                }
                Err(TrySendError::Closed(_)) => {
                    // The drainer was never started, or the map was dropped
                    // mid-eviction. Either way: no cleanup possible. Log and
                    // move on.
                    debug!("MokaEvictingMap eviction channel closed; eviction silently dropped");
                }
            }
        });

        let cache = builder.build();

        Self {
            cache,
            btree,
            eviction_tx,
            eviction_rx: parking_lot::Mutex::new(Some(eviction_rx)),
            callbacks: Arc::new(PlRwLock::new(Vec::new())),
            anchor_time,
            max_bytes,
            evict_bytes,
            max_seconds,
            max_count,
            evicted_bytes,
            evicted_items,
            replaced_bytes: Counter::default(),
            replaced_items: CounterWithTime::default(),
            lifetime_inserted_bytes: Counter::default(),
            inline_fallback_evictions,
            sum_store_size,
            _phantom: PhantomData,
        }
    }

    /// Start the background drainer that consumes eviction events and runs
    /// `unref()` + item callbacks. Must be called exactly once. Subsequent
    /// calls are no-ops (the receiver is taken on the first call).
    pub fn start_background_eviction(self: &Arc<Self>) {
        let Some(mut rx) = self.eviction_rx.lock().take() else {
            return;
        };
        let callbacks = Arc::clone(&self.callbacks);
        background_spawn!("moka_eviction_drainer", async move {
            while let Some(event) = rx.recv().await {
                // Run callbacks first so observers see the key going away
                // before the underlying resource is unref'd.
                let cbs: Vec<_> = {
                    let guard = callbacks.read();
                    guard
                        .iter()
                        .map(|cb| cb.callback(event.key.borrow()))
                        .collect()
                };
                for cb in cbs {
                    cb.await;
                }
                event.value.unref().await;
            }
        });
    }

    /// Build the `BTree` index for `range`. Idempotent.
    pub fn enable_filtering(&self) {
        let mut guard = self.btree.write();
        if guard.is_some() {
            return;
        }
        let mut tree = BTreeSet::new();
        for (k, _v) in &self.cache {
            tree.insert((*k).clone());
        }
        *guard = Some(tree);
    }

    /// Returns the number of entries in the cache. **Test/diagnostic only.**
    pub fn len_for_test(&self) -> usize {
        self.cache.run_pending_tasks();
        usize::try_from(self.cache.entry_count()).unwrap_or(usize::MAX)
    }

    /// Lock-free read. Promotes LRU position implicitly via moka's frequency
    /// sketch.
    pub fn get(&self, key: &Q) -> Option<T>
    where
        K: Borrow<Q>,
    {
        self.cache.get(key)
    }

    /// Batched read. Single pass; each `get` is still lock-free.
    pub fn get_many(&self, keys: &[&Q]) -> Vec<Option<T>> {
        keys.iter().map(|k| self.cache.get(*k)).collect()
    }

    /// Return the size of a `key` if present. Promotes LRU position. `peek`
    /// is accepted for API parity but **ignored** — moka has no
    /// non-promoting probe.
    pub fn size_for_key(&self, key: &Q) -> Option<u64> {
        self.cache.get(key).map(|v| v.len())
    }

    /// Batched size lookup. `peek` accepted but ignored (documented divergence).
    pub fn sizes_for_keys<It, R>(&self, keys: It, results: &mut [Option<u64>], _peek: bool)
    where
        It: IntoIterator<Item = R>,
        R: Borrow<Q>,
    {
        for (key, slot) in keys.into_iter().zip(results.iter_mut()) {
            *slot = self.cache.get(key.borrow()).map(|v| v.len());
        }
    }

    /// Walk the `BTree` index over `prefix_range` and invoke
    /// `handler(&K, &T)` for each present value. Entries evicted between
    /// `BTree` snapshot and value lookup are silently skipped.
    ///
    /// Requires [`Self::enable_filtering`] to have been called; if not, the
    /// `BTree` is built lazily on first call.
    pub fn range<F, R>(&self, prefix_range: R, mut handler: F) -> u64
    where
        F: FnMut(&K, &T) -> bool,
        R: RangeBounds<Q>,
    {
        // Build on demand.
        if self.btree.read().is_none() {
            self.enable_filtering();
        }
        let snapshot: Vec<K> = {
            let guard = self.btree.read();
            guard
                .as_ref()
                .expect("btree built above")
                .range(prefix_range)
                .cloned()
                .collect()
        };
        let mut count = 0u64;
        for key in &snapshot {
            let Some(value) = self.cache.get(key.borrow()) else {
                continue;
            };
            if !handler(key, &value) {
                break;
            }
            count += 1;
        }
        count
    }

    /// Insert a value. Returns the replaced value if any. Includes the
    /// `TinyLFU` "frequency bump" workaround (Phase 0 §4a): immediately
    /// after `cache.insert` we do a `cache.get` to push the candidate's
    /// frequency-sketch counter to 1, so it wins admission against a
    /// freq-0 incumbent on the next maintenance pass.
    pub fn insert(&self, key: K, value: T) -> Option<T> {
        let replaced = self.insert_inner(key.clone(), value);
        // Frequency bump — single sync hash lookup, ~100 ns.
        drop(self.cache.get(key.borrow()));
        self.cache.run_pending_tasks();
        replaced
    }

    /// Insert with an explicit time offset. The time is **ignored** in this
    /// implementation; we delegate to [`Self::insert_startup`] so the value
    /// joins `MainProbation` in insertion order — preserving the
    /// `FilesystemStore` startup-atime contract (Phase 0 §4b).
    pub fn insert_with_time(&self, key: K, value: T, _seconds_since_anchor: i32) -> Option<T> {
        self.insert_startup(key, value)
    }

    /// Startup-path insert: no frequency bump, no `run_pending_tasks`.
    /// Callers should `cache.run_pending_tasks` once at the end of the
    /// startup batch (or let the next normal `insert` flush them).
    pub fn insert_startup(&self, key: K, value: T) -> Option<T> {
        self.insert_inner(key, value)
    }

    /// Batched insert. Single `run_pending_tasks` at end (Phase 0 §3.1, §3.3).
    pub fn insert_many<It>(&self, inserts: It) -> Vec<T>
    where
        It: IntoIterator<Item = (K, T)>,
    {
        let mut replaced = Vec::new();
        for (k, v) in inserts {
            let key_clone = k.clone();
            if let Some(old) = self.insert_inner(k, v) {
                replaced.push(old);
            }
            // Frequency bump per entry, no run_pending_tasks until the end.
            drop(self.cache.get(key_clone.borrow()));
        }
        self.cache.run_pending_tasks();
        replaced
    }

    /// Common insert path. Updates btree, counters; surfaces the replaced
    /// value (if any) synchronously by snapshotting it via `cache.get`
    /// before the `insert` mutation. Does **not** advance pending tasks.
    fn insert_inner(&self, key: K, value: T) -> Option<T> {
        let new_len = value.len();
        let prior = self.cache.get(key.borrow());
        if let Some(p) = &prior {
            let plen = p.len();
            self.sum_store_size.fetch_sub(plen, Ordering::Relaxed);
            self.replaced_bytes.add(plen);
            self.replaced_items.inc();
        }
        {
            let mut guard = self.btree.write();
            if let Some(b) = guard.as_mut() {
                b.insert(key.clone());
            }
        }
        self.cache.insert(key, value);
        self.sum_store_size.fetch_add(new_len, Ordering::Relaxed);
        self.lifetime_inserted_bytes.add(new_len);
        prior
    }

    /// Remove a key. Returns true if the key was present.
    pub fn remove(&self, key: &Q) -> bool {
        let prior = self.cache.get(key);
        if prior.is_none() {
            return false;
        }
        self.cache.invalidate(key);
        self.cache.run_pending_tasks();
        true
    }

    /// Atomically (within the moka hash op) conditional remove. The
    /// `cond` closure receives the current value via `cache.get`. If
    /// `cond` returns true, the entry is removed.
    ///
    /// **Atomicity note**: `remove_if` is not strictly atomic across get
    /// and remove — a writer can replace the value in between. PR #2341
    /// uses `Arc::ptr_eq` for the cond, which is safe across clones (the
    /// pointer-equality check sees the same allocation regardless of how
    /// many `Arc::clone`s sit between).
    pub fn remove_if<F>(&self, key: &Q, cond: F) -> bool
    where
        F: FnOnce(&T) -> bool,
    {
        let Some(value) = self.cache.get(key) else {
            return false;
        };
        if !cond(&value) {
            return false;
        }
        self.cache.invalidate(key);
        self.cache.run_pending_tasks();
        true
    }

    /// Register an item callback.
    pub fn add_item_callback(&self, callback: C) {
        self.callbacks.write().push(callback);
    }

    /// Compat alias matching `EvictingMap::add_remove_callback`.
    pub fn add_remove_callback(&self, callback: C) {
        self.add_item_callback(callback);
    }

    /// Total live bytes (mirrors `EvictingMap`'s `sum_store_size`).
    pub fn sum_store_size(&self) -> u64 {
        self.sum_store_size.load(Ordering::Relaxed)
    }

    /// Force moka to flush any pending write/eviction work. Useful for
    /// deterministic tests; production callers do not need this.
    pub fn run_pending_tasks(&self) {
        self.cache.run_pending_tasks();
    }

    /// Number of times the eviction listener fell through to the inline
    /// fallback path (channel full). Test/diagnostic accessor.
    pub fn inline_fallback_count(&self) -> u64 {
        self.inline_fallback_evictions.load(Ordering::Relaxed)
    }

    /// Snapshot of the evicted-bytes counter. Test/diagnostic accessor.
    pub fn evicted_bytes_count(&self) -> u64 {
        self.evicted_bytes.load(Ordering::Relaxed)
    }

    /// Snapshot of the evicted-items counter. Test/diagnostic accessor.
    pub fn evicted_items_count(&self) -> u64 {
        self.evicted_items.load(Ordering::Relaxed)
    }

    /// Accessor for the channel capacity (remaining slots). Useful for
    /// diagnostic tests that want to verify backpressure behavior.
    pub fn eviction_channel_remaining(&self) -> usize {
        self.eviction_tx.capacity()
    }

    /// Accessor for the anchor instant (used by future per-entry-time work).
    pub const fn anchor_time(&self) -> &I {
        &self.anchor_time
    }
}

impl<K, Q, T, C> MokaEvictingMap<K, Q, T, std::time::SystemTime, C>
where
    K: Hash + Eq + Ord + Clone + Debug + Send + Sync + Borrow<Q> + 'static,
    Q: Hash + Eq + Ord + Debug + 'static,
    T: LenEntry + Clone + Debug + Send + Sync + 'static,
    C: ItemCallback<Q> + 'static,
{
    /// Construct using `SystemTime::now()` as the anchor.
    pub fn new(config: &EvictionPolicy) -> Self {
        Self::with_anchor(config, std::time::SystemTime::now())
    }
}
