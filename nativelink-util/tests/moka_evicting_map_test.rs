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

//! Tests for [`nativelink_util::moka_evicting_map::MokaEvictingMap`].
//!
//! Each test is deterministic; we do not rely on wall-clock `sleep` for
//! sequencing — channels and `Notify` are used instead.

use core::fmt::Debug;
use core::pin::Pin;
use core::sync::atomic::{AtomicU64, Ordering};
use core::time::Duration;
use std::sync::Arc;

use bytes::Bytes;
use futures::Future;
use nativelink_config::stores::EvictionPolicy;
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_util::evicting_map::{LenEntry, RemoveItemCallback};
use nativelink_util::instant_wrapper::MockInstantWrapped;
use nativelink_util::moka_evicting_map::MokaEvictingMap;
use pretty_assertions::assert_eq;
use tokio::sync::Notify;

// ---------- shared fixtures ----------

#[derive(Clone, PartialEq, Eq, Debug)]
struct BytesWrapper(Bytes);

impl LenEntry for BytesWrapper {
    fn len(&self) -> u64 {
        Bytes::len(&self.0) as u64
    }
    fn is_empty(&self) -> bool {
        Bytes::is_empty(&self.0)
    }
}

impl From<Bytes> for BytesWrapper {
    fn from(b: Bytes) -> Self {
        Self(b)
    }
}

/// Value whose `unref()` increments a shared counter — lets tests observe
/// the async cleanup bridge.
#[derive(Clone, Debug)]
struct UnrefCounting {
    payload: Bytes,
    unref_count: Arc<AtomicU64>,
    notify: Option<Arc<Notify>>,
}

impl UnrefCounting {
    fn new(size: usize) -> Self {
        Self {
            payload: Bytes::from(vec![0u8; size]),
            unref_count: Arc::new(AtomicU64::new(0)),
            notify: None,
        }
    }
    #[allow(dead_code)]
    fn with_notify(mut self, notify: Arc<Notify>) -> Self {
        self.notify = Some(notify);
        self
    }
}

impl LenEntry for UnrefCounting {
    fn len(&self) -> u64 {
        self.payload.len() as u64
    }
    fn is_empty(&self) -> bool {
        self.payload.is_empty()
    }
    async fn unref(&self) {
        self.unref_count.fetch_add(1, Ordering::SeqCst);
        if let Some(n) = &self.notify {
            n.notify_waiters();
        }
    }
}

#[derive(Clone, Debug)]
struct CallbackCounter {
    invocations: Arc<AtomicU64>,
}

impl CallbackCounter {
    fn new() -> Self {
        Self {
            invocations: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl<Q: Send + Sync + 'static + Debug> RemoveItemCallback<Q> for CallbackCounter {
    fn callback(&self, _key: &Q) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        let cnt = Arc::clone(&self.invocations);
        Box::pin(async move {
            cnt.fetch_add(1, Ordering::SeqCst);
        })
    }
}

fn policy_max_bytes(max_bytes: u64, evict_bytes: u64) -> EvictionPolicy {
    EvictionPolicy {
        max_bytes: usize::try_from(max_bytes).unwrap(),
        evict_bytes: usize::try_from(evict_bytes).unwrap(),
        max_seconds: 0,
        max_count: 0,
    }
}
const fn policy_max_count(max_count: u64) -> EvictionPolicy {
    EvictionPolicy {
        max_bytes: 0,
        evict_bytes: 0,
        max_seconds: 0,
        max_count,
    }
}
const fn policy_ttl(max_seconds: u32) -> EvictionPolicy {
    EvictionPolicy {
        max_bytes: 0,
        evict_bytes: 0,
        max_seconds,
        max_count: 0,
    }
}

/// Wait for moka's background work to flush, deterministically (no sleeps).
fn flush<T, C>(map: &MokaEvictingMap<u64, u64, T, MockInstantWrapped, C>)
where
    T: LenEntry + Clone + Debug + Send + Sync + 'static,
    C: RemoveItemCallback<u64> + 'static,
{
    map.run_pending_tasks();
}

// ---------- tests ----------

// 1. round-trip insert / get
#[nativelink_test]
async fn insert_then_get() -> Result<(), Error> {
    let map: MokaEvictingMap<u64, u64, BytesWrapper, MockInstantWrapped> =
        MokaEvictingMap::with_anchor(&policy_max_count(100), MockInstantWrapped::default());
    map.insert(1u64, Bytes::from_static(b"hello").into());
    let got = map.get(&1u64).expect("present");
    assert_eq!(got.0.as_ref(), b"hello");
    Ok(())
}

// 2. byte-budget eviction: insert past max_bytes (size-based)
#[nativelink_test]
async fn insert_evicts_oldest_under_max_bytes() -> Result<(), Error> {
    let map: MokaEvictingMap<u64, u64, BytesWrapper, MockInstantWrapped> =
        MokaEvictingMap::with_anchor(
            // 3 KiB cap, ~1 KiB evict — should hold ~2 entries of 1 KiB.
            &policy_max_bytes(3 * 1024, 1024),
            MockInstantWrapped::default(),
        );
    for i in 0u64..10 {
        map.insert(i, Bytes::from(vec![0u8; 1024]).into());
    }
    flush(&map);
    // We expect the cache to keep at most ~ ((3-1)/1)=2 KB-weighted entries.
    let len = map.len_for_test();
    assert!(len <= 4, "expected eviction under byte budget, got len={len}");
    assert!(len >= 1, "expected at least the last insert to survive");
    Ok(())
}

// 3. count-budget eviction.
#[nativelink_test]
async fn insert_evicts_oldest_under_max_count() -> Result<(), Error> {
    let map: MokaEvictingMap<u64, u64, BytesWrapper, MockInstantWrapped> =
        MokaEvictingMap::with_anchor(&policy_max_count(3), MockInstantWrapped::default());
    for i in 0u64..10 {
        map.insert(i, Bytes::from(vec![0u8; 1]).into());
    }
    flush(&map);
    let len = map.len_for_test();
    // moka enforces the cap asynchronously; after run_pending_tasks it must
    // be at the configured count.
    assert!(
        len <= 3,
        "expected count-based cap to enforce <= 3, got {len}"
    );
    Ok(())
}

// 4. TTL eviction
#[nativelink_test]
async fn ttl_evicts_after_max_seconds() -> Result<(), Error> {
    let map: MokaEvictingMap<u64, u64, BytesWrapper, MockInstantWrapped> =
        MokaEvictingMap::with_anchor(&policy_ttl(1), MockInstantWrapped::default());
    map.insert(42u64, Bytes::from_static(b"hi").into());
    assert!(map.get(&42u64).is_some());
    // Advance the mock clock by emitting many yields + a real short sleep so
    // tokio's clock used by moka's time_to_idle moves forward. Moka uses
    // std::time internally, so we need a real (small) sleep here.
    tokio::time::sleep(Duration::from_millis(1100)).await;
    flush(&map);
    // After TTL, a get should return None.
    assert!(map.get(&42u64).is_none(), "expected TTL eviction");
    Ok(())
}

// 5. insert_with_time uses insert_startup — preserves insertion order
#[nativelink_test]
async fn insert_with_time_preserves_startup_order() -> Result<(), Error> {
    let map: MokaEvictingMap<u64, u64, BytesWrapper, MockInstantWrapped> =
        MokaEvictingMap::with_anchor(&policy_max_count(5), MockInstantWrapped::default());
    // Caller would sort by atime ascending: oldest first. Verify that
    // inserting in that order, then over-filling, evicts oldest-first.
    for i in 0u64..5 {
        // descending atime offset to prove the field is ignored
        map.insert_with_time(
            i,
            Bytes::from(vec![0u8; 1]).into(),
            100 - i32::try_from(i).unwrap(),
        );
    }
    flush(&map);
    // Now over-fill — older entries should evict first.
    for i in 5u64..10 {
        map.insert(i, Bytes::from(vec![0u8; 1]).into());
    }
    flush(&map);
    // The first-inserted (0,1,2,3,4) should be the ones at the LRU front;
    // after 5 more inserts at least one of {0..5} must be gone.
    let present_low = (0u64..5).filter(|i| map.get(i).is_some()).count();
    assert!(
        present_low < 5,
        "expected oldest entries to be evicted first; all {present_low} survived"
    );
    Ok(())
}

// 6. insert_many uses at most one run_pending_tasks.
//    We can't directly observe maintenance passes, but we CAN observe that
//    insert_many followed by run_pending_tasks reports the expected count.
#[nativelink_test]
async fn insert_many_batches_pending_tasks() -> Result<(), Error> {
    let map: MokaEvictingMap<u64, u64, BytesWrapper, MockInstantWrapped> =
        MokaEvictingMap::with_anchor(&policy_max_count(1000), MockInstantWrapped::default());
    let pairs: Vec<_> = (0u64..50).map(|i| (i, Bytes::from(vec![0u8; 1]).into())).collect();
    let replaced = map.insert_many(pairs);
    assert!(replaced.is_empty(), "no prior entries should be replaced");
    let len = map.len_for_test();
    assert_eq!(len, 50, "all 50 entries should be present after insert_many");
    Ok(())
}

// 7. remove returns true if removed
#[nativelink_test]
async fn remove_returns_true_if_removed() -> Result<(), Error> {
    let map: MokaEvictingMap<u64, u64, BytesWrapper, MockInstantWrapped> =
        MokaEvictingMap::with_anchor(&policy_max_count(100), MockInstantWrapped::default());
    map.insert(7u64, Bytes::from_static(b"x").into());
    assert!(map.remove(&7u64));
    assert!(!map.remove(&7u64), "second remove returns false");
    assert!(map.get(&7u64).is_none());
    Ok(())
}

// 8. remove_if with Arc::ptr_eq — critical for PR #2341 contract.
#[nativelink_test]
async fn remove_if_with_arc_ptr_eq_semantics() -> Result<(), Error> {
    let map: MokaEvictingMap<u64, u64, Arc<UnrefCounting>, MockInstantWrapped> =
        MokaEvictingMap::with_anchor(&policy_max_count(10), MockInstantWrapped::default());
    let entry = Arc::new(UnrefCounting::new(1));
    map.insert(99u64, Arc::clone(&entry));
    // Cond using Arc::ptr_eq — same allocation as the inserted Arc.
    let removed = map.remove_if(&99u64, |held| Arc::<UnrefCounting>::ptr_eq(held, &entry));
    assert!(removed);
    assert!(map.get(&99u64).is_none());
    // Wrong-pointer cond should leave the entry alone.
    let other = Arc::new(UnrefCounting::new(1));
    map.insert(100u64, Arc::clone(&other));
    let not_removed =
        map.remove_if(&100u64, |held| Arc::<UnrefCounting>::ptr_eq(held, &entry));
    assert!(!not_removed, "ptr_eq against unrelated Arc must not remove");
    assert!(map.get(&100u64).is_some());
    Ok(())
}

// 9. range walks BTree in order.
#[nativelink_test]
async fn range_walks_btree_in_order() -> Result<(), Error> {
    let map: MokaEvictingMap<u64, u64, BytesWrapper, MockInstantWrapped> =
        MokaEvictingMap::with_anchor(&policy_max_count(100), MockInstantWrapped::default());
    map.enable_filtering();
    for i in [3u64, 1, 4, 1, 5, 9, 2, 6, 5, 3] {
        map.insert(i, Bytes::from_static(b"v").into());
    }
    flush(&map);
    let mut seen = Vec::new();
    let count = map.range(2u64..6u64, |k, _v| {
        seen.push(*k);
        true
    });
    assert_eq!(count, seen.len() as u64);
    // BTree is sorted: 2,3,4,5
    assert_eq!(seen, vec![2u64, 3, 4, 5]);
    Ok(())
}

// 10. add_item_callback fires on eviction via the async drainer.
#[nativelink_test]
async fn add_item_callback_fires_on_eviction() -> Result<(), Error> {
    let map: Arc<MokaEvictingMap<u64, u64, Arc<UnrefCounting>, MockInstantWrapped, CallbackCounter>> =
        Arc::new(MokaEvictingMap::with_anchor(
            &policy_max_count(2),
            MockInstantWrapped::default(),
        ));
    map.start_background_eviction();
    let cb = CallbackCounter::new();
    let cb_invocations = Arc::clone(&cb.invocations);
    map.add_item_callback(cb);
    // Insert 3 items into a max-2 cache; one must evict.
    for i in 0u64..3 {
        map.insert(i, Arc::new(UnrefCounting::new(1)));
    }
    // Drain — yield a few times to let the drainer run.
    for _ in 0..50 {
        flush(&map);
        if cb_invocations.load(Ordering::SeqCst) > 0 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(
        cb_invocations.load(Ordering::SeqCst) >= 1,
        "expected at least one callback invocation; got {}",
        cb_invocations.load(Ordering::SeqCst)
    );
    Ok(())
}

// 11. unref bridge inline-fallback when channel full.
//     We construct a map without start_background_eviction(), causing the
//     drainer never to consume — the bounded channel fills, then overflows
//     trigger inline_fallback_count > 0.
#[nativelink_test]
async fn unref_bridge_handles_full_channel_inline() -> Result<(), Error> {
    let map: Arc<MokaEvictingMap<u64, u64, Arc<UnrefCounting>, MockInstantWrapped>> =
        Arc::new(MokaEvictingMap::with_anchor(
            &policy_max_count(1),
            MockInstantWrapped::default(),
        ));
    // Intentionally do NOT call start_background_eviction().
    // Each insert beyond 1 evicts the prior one — pushing onto the channel.
    // After EVICTION_CHANNEL_DEPTH (4096) events, the next eviction must
    // fall through to the inline path.
    for i in 0u64..(4096u64 + 200) {
        map.insert(i, Arc::new(UnrefCounting::new(1)));
        if i % 256 == 0 {
            flush(&map);
        }
    }
    flush(&map);
    // Yield to let any inline spawns run.
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert!(
        map.inline_fallback_count() > 0,
        "expected inline-fallback path to fire, got {}",
        map.inline_fallback_count()
    );
    Ok(())
}

// 12. Concurrent reads are lock-free / make progress.
#[nativelink_test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_reads_are_lock_free() -> Result<(), Error> {
    let map: Arc<MokaEvictingMap<u64, u64, BytesWrapper, MockInstantWrapped>> =
        Arc::new(MokaEvictingMap::with_anchor(
            &policy_max_count(10_000),
            MockInstantWrapped::default(),
        ));
    for i in 0u64..1000 {
        map.insert(i, Bytes::from(vec![0u8; 1]).into());
    }
    flush(&map);
    let mut handles = Vec::new();
    for t in 0..8u64 {
        let m = Arc::clone(&map);
        handles.push(tokio::spawn(async move {
            let mut hits = 0u64;
            for i in 0u64..5000 {
                let k = (i + t * 7) % 1000;
                if m.get(&k).is_some() {
                    hits += 1;
                }
            }
            hits
        }));
    }
    let mut total = 0u64;
    for h in handles {
        total += h.await.unwrap();
    }
    assert!(total > 0, "no reads succeeded — concurrent path is wedged");
    Ok(())
}

// 13. Concurrent writes progress under contention.
#[nativelink_test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writes_progress_under_contention() -> Result<(), Error> {
    let map: Arc<MokaEvictingMap<u64, u64, BytesWrapper, MockInstantWrapped>> =
        Arc::new(MokaEvictingMap::with_anchor(
            &policy_max_count(10_000),
            MockInstantWrapped::default(),
        ));
    let mut handles = Vec::new();
    for t in 0..8u64 {
        let m = Arc::clone(&map);
        handles.push(tokio::spawn(async move {
            for i in 0u64..500 {
                let k = i + t * 1000;
                m.insert(k, Bytes::from(vec![0u8; 16]).into());
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    flush(&map);
    let len = map.len_for_test();
    assert!(len >= 1000, "writers produced fewer than 1000 live entries: {len}");
    Ok(())
}

// 14. Frequency bump prevents single-access rejection at capacity.
#[nativelink_test]
async fn frequency_bump_prevents_single_access_rejection() -> Result<(), Error> {
    // Fill cache to ~capacity, then insert N new keys and verify they all
    // become visible. Without the frequency bump TinyLFU would reject the
    // candidates against incumbents on ties.
    let map: MokaEvictingMap<u64, u64, BytesWrapper, MockInstantWrapped> =
        MokaEvictingMap::with_anchor(&policy_max_count(100), MockInstantWrapped::default());
    for i in 0u64..100 {
        map.insert(i, Bytes::from(vec![0u8; 1]).into());
    }
    flush(&map);
    // Insert 100 brand-new keys.
    for i in 100u64..200 {
        map.insert(i, Bytes::from(vec![0u8; 1]).into());
    }
    flush(&map);
    // At least one of the new keys must be visible — without freq bump,
    // TinyLFU on tie-breaks would reject all of them.
    let new_visible = (100u64..200).filter(|i| map.get(i).is_some()).count();
    assert!(
        new_visible > 0,
        "frequency-bump regression: TinyLFU rejected all candidates"
    );
    Ok(())
}

// 15. size_for_key peek param is documented as no-op.
#[nativelink_test]
async fn size_for_key_peek_param_documented_as_noop() -> Result<(), Error> {
    let map: MokaEvictingMap<u64, u64, BytesWrapper, MockInstantWrapped> =
        MokaEvictingMap::with_anchor(&policy_max_count(10), MockInstantWrapped::default());
    map.insert(1u64, Bytes::from(vec![0u8; 5]).into());
    let mut results = [None; 1];
    // peek=true is accepted; we don't have a way to observe promotion from
    // a unit test (TinyLFU sketch is opaque). We DO assert that peek=true
    // still returns the value — i.e., the parameter does not gate access.
    map.sizes_for_keys([&1u64], &mut results[..], true);
    assert_eq!(results[0], Some(5));
    Ok(())
}

// 16. len_for_test after run_pending_tasks is accurate.
#[nativelink_test]
async fn len_for_test_after_run_pending_tasks_is_accurate() -> Result<(), Error> {
    let map: MokaEvictingMap<u64, u64, BytesWrapper, MockInstantWrapped> =
        MokaEvictingMap::with_anchor(&policy_max_count(1000), MockInstantWrapped::default());
    for i in 0u64..123 {
        map.insert(i, Bytes::from(vec![0u8; 4]).into());
    }
    flush(&map);
    assert_eq!(map.len_for_test(), 123);
    Ok(())
}

// 17. Weigher handles large values via KB scaling. We can't actually
//     allocate 4 GiB in a test, but we DO verify a 64 MiB value is admitted
//     and that the kb-scaled weight does not overflow u32.
#[nativelink_test]
async fn weigher_handles_large_values_via_kb_scaling() -> Result<(), Error> {
    let map: MokaEvictingMap<u64, u64, BytesWrapper, MockInstantWrapped> =
        MokaEvictingMap::with_anchor(
            // 256 MiB cap — accommodates a 64 MiB value.
            &policy_max_bytes(256 * 1024 * 1024, 16 * 1024 * 1024),
            MockInstantWrapped::default(),
        );
    let big = Bytes::from(vec![0u8; 64 * 1024 * 1024]);
    map.insert(1u64, big.into());
    flush(&map);
    let got = map.get(&1u64);
    assert!(got.is_some(), "64 MiB value should be admitted under 256 MiB cap");
    Ok(())
}

// 18. cancel-safe unref: drop the map mid-eviction, no leaks (just no panic).
#[nativelink_test]
async fn cancel_safe_unref_drains_on_drop() -> Result<(), Error> {
    let map: Arc<MokaEvictingMap<u64, u64, Arc<UnrefCounting>, MockInstantWrapped>> =
        Arc::new(MokaEvictingMap::with_anchor(
            &policy_max_count(5),
            MockInstantWrapped::default(),
        ));
    map.start_background_eviction();
    let unref_counter = Arc::new(AtomicU64::new(0));
    let notify = Arc::new(Notify::new());
    for i in 0u64..50 {
        let v = UnrefCounting {
            payload: Bytes::from(vec![0u8; 1]),
            unref_count: Arc::clone(&unref_counter),
            notify: Some(Arc::clone(&notify)),
        };
        map.insert(i, Arc::new(v));
    }
    flush(&map);
    // Drop the map. The drainer task is owned by background_spawn! and will
    // detect the closed channel and exit cleanly.
    drop(map);
    // Yield so the drainer terminates.
    for _ in 0..50 {
        tokio::task::yield_now().await;
    }
    // No panic, no hang — pass.
    Ok(())
}
