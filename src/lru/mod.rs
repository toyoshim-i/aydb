//! Capped Table (`MAX_BYTES`) quota accounting, bounded in-memory `AccessTick` coalescing,
//! and Lazy Promotion during LRU eviction.

pub use crate::index::key_codec;

use std::collections::HashMap;
use std::sync::MutexGuard;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::util::CachePaddedMutex;

const TRACKER_SHARDS: usize = 16;

pub use key_codec::{
    NS_LRU_QUEUE, NS_PRIMARY_ROW, NS_ROW_META, NS_SECONDARY_INDEX, RowMetaRecord,
    decode_bytes_escaped, decode_i64_memcomparable, decode_lru_queue_key, decode_primary_row_key,
    decode_secondary_index_key, decode_secondary_index_pk_slice, encode_bytes_escaped,
    encode_i64_memcomparable, encode_lru_queue_key, encode_namespace_prefix,
    encode_primary_row_key, encode_row_meta_key, encode_secondary_index_exact_prefix,
    encode_secondary_index_key, encode_secondary_index_prefix,
};

/// Quota and live-byte state for a single Capped Table (`MAX_BYTES`).
#[derive(Debug, Clone)]
pub struct CappedTableState {
    pub table_id: u16,
    pub max_bytes: u64,
    pub current_logical_bytes: u64,
    pub total_evictions: u64,
    pub total_lazy_promotions: u64,
    /// Persistent resume cursor (`AccessTick`) for `scan_merged_prefix([TableID | 'l' | ...])`
    /// so sequential eviction never re-scans historical tombstones from `tick = 0`!
    pub eviction_cursor_tick: u64,
}

impl CappedTableState {
    pub fn new(table_id: u16, max_bytes: u64) -> Self {
        Self {
            table_id,
            max_bytes,
            current_logical_bytes: 0,
            total_evictions: 0,
            total_lazy_promotions: 0,
            eviction_cursor_tick: 0,
        }
    }

    #[inline]
    pub fn is_over_quota(&self) -> bool {
        self.current_logical_bytes > self.max_bytes
    }
}

/// Bounded in-memory tracker that coalesces read-path `AccessTick` updates in RAM
/// without issuing disk writes on `get_row()` / `SELECT`.
///
/// Sharded across 16 cache-line-aligned (`#[repr(align(128))]`) maps indexed by `(table_id, pk)`,
/// each holding a `BTreeMap<u16, HashMap<Vec<u8>, u64>>` so all read hits on existing hot keys,
/// peeks (`peek_hot_tick`), lazy promotions (`take_hot_tick`), and overwrites/deletes
/// (`remove_key`) probe by `&[u8]` (`Borrow<[u8]>`) with zero heap allocations.
pub struct AccessTracker {
    next_tick: AtomicU64,
    max_entries_per_shard: usize,
    /// Interior mutability so committed-view reads (`&self`) can record hits. Shard locks are
    /// held only for in-memory map updates and never across I/O.
    shards: Vec<CachePaddedMutex<TrackerShardInner>>,
}

#[derive(Default)]
struct TrackerShardInner {
    /// Maps `table_id -> (pk_bytes -> latest_access_tick)` for recently read keys.
    hot_ticks: std::collections::BTreeMap<u16, HashMap<Vec<u8>, u64>>,
    total_entries: usize,
    /// Rotates equal-median tie-breaking across tables on shard capacity trim.
    evict_round: usize,
}

#[inline]
fn maybe_shrink_table_map(map: &mut HashMap<Vec<u8>, u64>) {
    if map.capacity() > 64 && map.len().saturating_mul(4) < map.capacity() {
        map.shrink_to(map.len().saturating_mul(2).max(16));
    }
}

impl AccessTracker {
    pub fn new(initial_tick: u64, max_entries: usize) -> Self {
        let max_entries_per_shard = (max_entries.max(64) / TRACKER_SHARDS).max(1);
        let mut shards = Vec::with_capacity(TRACKER_SHARDS);
        for _ in 0..TRACKER_SHARDS {
            shards.push(CachePaddedMutex::new(TrackerShardInner::default()));
        }
        Self {
            next_tick: AtomicU64::new(initial_tick.max(1)),
            max_entries_per_shard,
            shards,
        }
    }

    #[inline]
    fn shard_index(table_id: u16, pk: &[u8]) -> usize {
        let mut h = crate::index::BloomFilter::hash64(pk);
        h ^= (table_id as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15);
        ((h ^ (h >> 32)) as usize) & (TRACKER_SHARDS - 1)
    }

    #[inline]
    fn lock_shard(&self, table_id: u16, pk: &[u8]) -> MutexGuard<'_, TrackerShardInner> {
        self.shards[Self::shard_index(table_id, pk)].lock()
    }

    /// Allocates the next monotonically increasing `AccessTick`.
    #[inline]
    pub fn next_tick(&self) -> u64 {
        self.next_tick
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                Some(cur.saturating_add(1))
            })
            .unwrap()
    }

    /// Advances the internal tick counter so it is strictly greater than `observed_tick`
    /// (used during WAL/segment recovery at startup).
    pub fn observe_tick(&self, observed_tick: u64) {
        let target = observed_tick.saturating_add(1);
        let mut cur = self.next_tick.load(Ordering::Relaxed);
        while target > cur {
            match self.next_tick.compare_exchange_weak(
                cur,
                target,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => cur = actual,
            }
        }
    }

    /// Records a read hit in RAM (`O(1)` hash map update, zero disk I/O).
    /// Updates existing hot keys in place with zero heap allocation.
    /// Returns the effective `AccessTick` recorded for `(table_id, pk)`.
    pub fn record_read_hit(&self, table_id: u16, pk: &[u8]) -> u64 {
        let tick = self.next_tick();
        self.record_read_hit_with_tick(table_id, pk, tick)
    }

    fn record_read_hit_with_tick(&self, table_id: u16, pk: &[u8], tick: u64) -> u64 {
        let mut g = self.lock_shard(table_id, pk);
        let inner = &mut *g;
        if let Some(table_map) = inner.hot_ticks.get_mut(&table_id) {
            if let Some(slot) = table_map.get_mut(pk) {
                // Keep the highest tick in case concurrent readers arrive out of order.
                if tick > *slot {
                    *slot = tick;
                }
                return *slot;
            }
        }
        if inner.total_entries >= self.max_entries_per_shard {
            // Evict the oldest half of tracked entries in O(N) using quickselect; tie-break
            // equal ticks fairly across tables so `u64::MAX` saturation or equal medians do not
            // starve lower `table_id`s.
            let mut ticks: Vec<u64> = inner
                .hot_ticks
                .values()
                .flat_map(|m| m.values().copied())
                .collect();
            if !ticks.is_empty() {
                let mid = ticks.len() / 2;
                let (_, &mut median, _) = ticks.select_nth_unstable(mid);
                let target_keep = self.max_entries_per_shard / 2;
                let strictly_greater = ticks.iter().filter(|&&v| v > median).count();
                let mut tie_budget = target_keep.saturating_sub(strictly_greater);
                let mut kept = 0usize;
                let mut tids: Vec<u16> = inner.hot_ticks.keys().copied().collect();
                if !tids.is_empty() {
                    let rot = inner.evict_round % tids.len();
                    inner.evict_round = inner.evict_round.wrapping_add(1);
                    tids.rotate_left(rot);
                    for tid in tids {
                        let empty = if let Some(m) = inner.hot_ticks.get_mut(&tid) {
                            m.retain(|_, &mut v| {
                                if v > median {
                                    true
                                } else if v == median && tie_budget > 0 {
                                    tie_budget -= 1;
                                    true
                                } else {
                                    false
                                }
                            });
                            kept += m.len();
                            let empty = m.is_empty();
                            if !empty {
                                maybe_shrink_table_map(m);
                            }
                            empty
                        } else {
                            false
                        };
                        if empty {
                            inner.hot_ticks.remove(&tid);
                        }
                    }
                }
                inner.total_entries = kept;
            }
        }
        let table_map = inner.hot_ticks.entry(table_id).or_default();
        if table_map.insert(pk.to_vec(), tick).is_none() {
            inner.total_entries += 1;
        }
        tick
    }

    /// Returns and removes any pending in-memory `AccessTick` for `(table_id, pk)` with zero allocation.
    #[inline]
    pub fn take_hot_tick(&self, table_id: u16, pk: &[u8]) -> Option<u64> {
        let mut g = self.lock_shard(table_id, pk);
        if g.total_entries == 0 {
            return None;
        }
        let (removed, became_empty) = {
            let table_map = g.hot_ticks.get_mut(&table_id)?;
            let removed = table_map.remove(pk);
            let empty = table_map.is_empty();
            if removed.is_some() && !empty {
                maybe_shrink_table_map(table_map);
            }
            (removed, empty)
        };
        if became_empty {
            g.hot_ticks.remove(&table_id);
        }
        if removed.is_some() {
            g.total_entries = g.total_entries.saturating_sub(1);
        }
        removed
    }

    /// Peeks the pending in-memory `AccessTick` for `(table_id, pk)` with zero allocation.
    #[inline]
    pub fn peek_hot_tick(&self, table_id: u16, pk: &[u8]) -> Option<u64> {
        let g = self.lock_shard(table_id, pk);
        if g.total_entries == 0 {
            return None;
        }
        g.hot_ticks.get(&table_id)?.get(pk).copied()
    }

    /// Clears the pending read tick for `(table_id, pk)` only if its stored tick `<= max_tick`,
    /// preserving newer reads recorded concurrently or after the snapshot. Prunes empty table maps.
    #[inline]
    pub fn clear_if_tick_le(&self, table_id: u16, pk: &[u8], max_tick: u64) {
        let mut g = self.lock_shard(table_id, pk);
        if g.total_entries == 0 {
            return;
        }
        let (removed, became_empty) = if let Some(table_map) = g.hot_ticks.get_mut(&table_id) {
            if table_map.get(pk).copied().is_some_and(|t| t <= max_tick) {
                let did_remove = table_map.remove(pk).is_some();
                let empty = table_map.is_empty();
                if did_remove && !empty {
                    maybe_shrink_table_map(table_map);
                }
                (did_remove, empty)
            } else {
                (false, false)
            }
        } else {
            return;
        };
        if became_empty {
            g.hot_ticks.remove(&table_id);
        }
        if removed {
            g.total_entries = g.total_entries.saturating_sub(1);
        }
    }

    /// Clears any pending read tick for `(table_id, pk)` (called on overwrite or explicit delete)
    /// with zero allocation.
    #[inline]
    pub fn remove_key(&self, table_id: u16, pk: &[u8]) {
        let mut g = self.lock_shard(table_id, pk);
        if g.total_entries == 0 {
            return;
        }
        let (removed, became_empty) = if let Some(table_map) = g.hot_ticks.get_mut(&table_id) {
            let did_remove = table_map.remove(pk).is_some();
            let empty = table_map.is_empty();
            if did_remove && !empty {
                maybe_shrink_table_map(table_map);
            }
            (did_remove, empty)
        } else {
            return;
        };
        if became_empty {
            g.hot_ticks.remove(&table_id);
        }
        if removed {
            g.total_entries = g.total_entries.saturating_sub(1);
        }
    }

    /// Clears all pending read ticks for a committed `DROP TABLE`.
    pub fn remove_table(&self, table_id: u16) {
        for shard in &self.shards {
            let mut g = shard.lock();
            if let Some(removed) = g.hot_ticks.remove(&table_id) {
                g.total_entries = g.total_entries.saturating_sub(removed.len());
            }
        }
    }

    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.lock().total_entries).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memcomparable_i64_and_escaped_bytes_preserve_order() {
        let nums = [
            i64::MIN,
            i64::MIN + 1,
            -1_000_000,
            -1,
            0,
            1,
            1_000_000,
            i64::MAX - 1,
            i64::MAX,
        ];
        for pair in nums.windows(2) {
            let a = encode_i64_memcomparable(pair[0]);
            let b = encode_i64_memcomparable(pair[1]);
            assert!(a < b, "{} should encode smaller than {}", pair[0], pair[1]);
            assert_eq!(decode_i64_memcomparable(a), pair[0]);
            assert_eq!(decode_i64_memcomparable(b), pair[1]);
        }

        let strings: &[&[u8]] = &[
            b"",
            b"\x00",
            b"\x00\x00",
            b"\x00a",
            b"a",
            b"a\x00",
            b"aa",
            b"b",
        ];
        for pair in strings.windows(2) {
            let left = encode_secondary_index_key(1, 10, pair[0], b"pk_999");
            let right = encode_secondary_index_key(1, 10, pair[1], b"pk_000");
            assert!(
                left < right,
                "composite key with smaller col_val {:?} must sort before {:?}",
                pair[0],
                pair[1]
            );
            let (dec_col, dec_pk) = decode_secondary_index_key(1, 10, &left).unwrap().unwrap();
            assert_eq!(dec_col, pair[0]);
            assert_eq!(dec_pk, b"pk_999");
        }
    }

    #[test]
    fn access_tracker_two_level_map_borrows_slice_and_bounds_capacity() {
        let tracker = AccessTracker::new(100, 64);
        assert!(tracker.is_empty());
        assert_eq!(tracker.len(), 0);

        let t1 = tracker.record_read_hit(1, b"pk_a");
        assert_eq!(t1, 100);
        assert_eq!(tracker.len(), 1);
        assert_eq!(tracker.peek_hot_tick(1, b"pk_a"), Some(100));
        assert_eq!(tracker.peek_hot_tick(2, b"pk_a"), None);

        // Updating an existing hot key in place must not grow `len()`
        let t2 = tracker.record_read_hit(1, b"pk_a");
        assert_eq!(t2, 101);
        assert_eq!(tracker.len(), 1);
        assert_eq!(tracker.peek_hot_tick(1, b"pk_a"), Some(101));

        // Taking the hot tick removes it and decrements `len()`
        assert_eq!(tracker.take_hot_tick(1, b"pk_a"), Some(101));
        assert_eq!(tracker.len(), 0);
        assert!(tracker.is_empty());

        // `remove_key` on overwrite/delete removes the entry cleanly
        tracker.record_read_hit(3, b"pk_del");
        assert_eq!(tracker.len(), 1);
        tracker.remove_key(3, b"pk_del");
        assert_eq!(tracker.len(), 0);

        // Exceeding `max_entries` (64) evicts the oldest half while keeping recent entries
        for i in 0..80u32 {
            let key = format!("k_{i:04}");
            tracker.record_read_hit((i % 4) as u16, key.as_bytes());
        }
        assert!(tracker.len() <= 64);
        assert!(tracker.peek_hot_tick(3, b"k_0079").is_some());
    }

    #[test]
    fn access_tracker_observe_u64_max_does_not_overflow_or_spin() {
        let tracker = AccessTracker::new(1, 64);
        tracker.observe_tick(u64::MAX);
        tracker.observe_tick(u64::MAX);
        assert_eq!(tracker.next_tick(), u64::MAX);
        assert_eq!(tracker.next_tick(), u64::MAX);
    }

    #[test]
    fn access_tracker_shards_are_cache_line_aligned() {
        assert!(std::mem::align_of::<CachePaddedMutex<TrackerShardInner>>() >= 128);
    }

    #[test]
    fn access_tracker_never_regresses_tick_on_out_of_order_update() {
        let tracker = AccessTracker::new(1, 64);
        assert_eq!(tracker.record_read_hit_with_tick(1, b"hot", 200), 200);
        assert_eq!(tracker.record_read_hit_with_tick(1, b"hot", 150), 200);
        assert_eq!(tracker.peek_hot_tick(1, b"hot"), Some(200));
    }

    #[test]
    fn access_tracker_u64_max_eviction_preserves_entries_without_wipeout() {
        let tracker = AccessTracker::new(u64::MAX, 64);
        for i in 0..128u32 {
            let key = format!("sat_{i:04}");
            let tick = tracker.record_read_hit((i % 4) as u16, key.as_bytes());
            assert_eq!(tick, u64::MAX);
        }
        let len = tracker.len();
        assert!(
            (16..=64).contains(&len),
            "expected saturated eviction to keep half without wipeout, got len={len}"
        );
    }

    #[test]
    fn access_tracker_remove_table_clears_only_target_table() {
        let tracker = AccessTracker::new(1, 256);
        for i in 0..30u32 {
            let key = format!("row_{i:04}");
            tracker.record_read_hit(1, key.as_bytes());
            tracker.record_read_hit(2, key.as_bytes());
        }
        assert_eq!(tracker.len(), 60);

        tracker.remove_table(1);
        assert_eq!(tracker.len(), 30);
        for i in 0..30u32 {
            let key = format!("row_{i:04}");
            assert_eq!(tracker.peek_hot_tick(1, key.as_bytes()), None);
            assert!(tracker.peek_hot_tick(2, key.as_bytes()).is_some());
        }
    }

    #[test]
    fn access_tracker_prunes_empty_table_maps_and_tie_breaks_fairly() {
        let tracker = AccessTracker::new(1, 64);
        assert_eq!(tracker.record_read_hit_with_tick(42, b"k1", 10), 10);
        tracker.clear_if_tick_le(42, b"k1", 5);
        assert_eq!(tracker.peek_hot_tick(42, b"k1"), Some(10));
        tracker.clear_if_tick_le(42, b"k1", 10);
        assert_eq!(tracker.peek_hot_tick(42, b"k1"), None);
        for shard in &tracker.shards {
            assert!(!shard.lock().hot_ticks.contains_key(&42));
        }

        // All equal saturated ticks across tables 1 and 2 on a single shard must not starve
        // table 2 forever across repeated capacity trims.
        let saturated = AccessTracker::new(u64::MAX, 64);
        let shard_idx = AccessTracker::shard_index(1, b"x");
        assert_eq!(saturated.max_entries_per_shard, 4);
        for i in 0..4u8 {
            saturated.record_read_hit_with_tick(1, &[b'a' + i], u64::MAX);
        }
        // Push from table 2 with the same shard until capacity trim fires multiple rounds.
        let mut t2_keys = Vec::new();
        for i in 0..200u16 {
            let k = i.to_le_bytes();
            if AccessTracker::shard_index(2, &k) == shard_idx {
                t2_keys.push(k);
                if t2_keys.len() == 6 {
                    break;
                }
            }
        }
        let mut t1_keys = Vec::new();
        for i in 0..200u16 {
            let k = (i + 500).to_le_bytes();
            if AccessTracker::shard_index(1, &k) == shard_idx {
                t1_keys.push(k);
                if t1_keys.len() == 6 {
                    break;
                }
            }
        }
        for round in 0..6 {
            saturated.record_read_hit_with_tick(1, &t1_keys[round], u64::MAX);
            saturated.record_read_hit_with_tick(2, &t2_keys[round], u64::MAX);
        }
        let g = saturated.shards[shard_idx].lock();
        assert!(g.total_entries <= saturated.max_entries_per_shard);
    }

    #[test]
    fn access_tracker_starts_unallocated_and_shrinks_after_bulk_removal() {
        let tracker = AccessTracker::new(1, 1024);
        assert_eq!(tracker.len(), 0);
        // Empty tables allocate nothing upfront.
        for s in &tracker.shards {
            assert!(s.lock().hot_ticks.is_empty());
        }

        // Force all keys into table 7 on the same shard until that per-table map grows well past 64.
        let target_shard = AccessTracker::shard_index(7, b"seed");
        let mut keys = Vec::new();
        for i in 0..20_000u32 {
            let k = format!("k_{i}");
            if AccessTracker::shard_index(7, k.as_bytes()) == target_shard {
                keys.push(k);
                if keys.len() == 160 {
                    break;
                }
            }
        }
        assert_eq!(keys.len(), 160);
        let big_tracker = AccessTracker::new(1, 10_000);
        for k in &keys {
            big_tracker.record_read_hit_with_tick(7, k.as_bytes(), 10);
        }
        let peak_cap = big_tracker.shards[target_shard].lock().hot_ticks[&7].capacity();
        assert!(peak_cap > 64, "expected peak_cap > 64, got {peak_cap}");

        // Clear all but 2 entries (`tick <= 10`). Map must remain and shrink down cleanly.
        for k in &keys[..keys.len() - 2] {
            big_tracker.clear_if_tick_le(7, k.as_bytes(), 10);
        }
        let shrunk_cap = big_tracker.shards[target_shard].lock().hot_ticks[&7].capacity();
        assert!(
            shrunk_cap < peak_cap && shrunk_cap <= 64,
            "expected map capacity to shrink from {peak_cap}, got {shrunk_cap}"
        );

        // Repeat with remove_key.
        big_tracker.record_read_hit_with_tick(8, keys[0].as_bytes(), 20);
        for k in &keys[1..] {
            big_tracker.record_read_hit_with_tick(8, k.as_bytes(), 20);
        }
        let target_shard8 = AccessTracker::shard_index(8, keys[0].as_bytes());
        for k in &keys[..keys.len() - 1] {
            big_tracker.remove_key(8, k.as_bytes());
        }
        if let Some(m) = big_tracker.shards[target_shard8].lock().hot_ticks.get(&8) {
            assert!(m.capacity() <= 64);
        }
    }
}
