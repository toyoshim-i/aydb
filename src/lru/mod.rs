//! Capped Table (`MAX_BYTES`) quota accounting, bounded in-memory `AccessTick` coalescing,
//! and Lazy Promotion during LRU eviction.

pub mod key_codec;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

pub use key_codec::{
    NS_LRU_QUEUE, NS_PRIMARY_ROW, NS_ROW_META, NS_SECONDARY_INDEX, RowMetaRecord,
    decode_bytes_escaped, decode_i64_memcomparable, decode_lru_queue_key, decode_primary_row_key,
    decode_secondary_index_key, encode_bytes_escaped, encode_i64_memcomparable,
    encode_lru_queue_key, encode_namespace_prefix, encode_primary_row_key, encode_row_meta_key,
    encode_secondary_index_exact_prefix, encode_secondary_index_key,
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
/// Uses a two-level `BTreeMap<u16, HashMap<Vec<u8>, u64>>` so all read hits on existing
/// hot keys, peeks (`peek_hot_tick`), lazy promotions (`take_hot_tick`), and overwrites/deletes
/// (`remove_key`) probe by `&[u8]` (`Borrow<[u8]>`) with zero heap allocations (`P-04`).
pub struct AccessTracker {
    next_tick: AtomicU64,
    /// Maps `table_id -> (pk_bytes -> latest_access_tick)` for recently read keys (`P-04`).
    hot_ticks: std::collections::BTreeMap<u16, HashMap<Vec<u8>, u64>>,
    total_entries: usize,
    max_entries: usize,
}

impl AccessTracker {
    pub fn new(initial_tick: u64, max_entries: usize) -> Self {
        Self {
            next_tick: AtomicU64::new(initial_tick.max(1)),
            hot_ticks: std::collections::BTreeMap::new(),
            total_entries: 0,
            max_entries: max_entries.max(64),
        }
    }

    /// Allocates the next monotonically increasing `AccessTick`.
    #[inline]
    pub fn next_tick(&self) -> u64 {
        self.next_tick.fetch_add(1, Ordering::Relaxed)
    }

    /// Advances the internal tick counter so it is strictly greater than `observed_tick`
    /// (used during WAL/segment recovery at startup).
    pub fn observe_tick(&self, observed_tick: u64) {
        let mut cur = self.next_tick.load(Ordering::Relaxed);
        while observed_tick >= cur {
            match self.next_tick.compare_exchange_weak(
                cur,
                observed_tick + 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => cur = actual,
            }
        }
    }

    /// Records a read hit in RAM (`O(1)` hash map update, zero disk I/O).
    /// Updates existing hot keys in place with zero heap allocation (`P-04`).
    /// Returns the newly assigned `AccessTick`.
    pub fn record_read_hit(&mut self, table_id: u16, pk: &[u8]) -> u64 {
        let tick = self.next_tick();
        if let Some(table_map) = self.hot_ticks.get_mut(&table_id) {
            if let Some(slot) = table_map.get_mut(pk) {
                *slot = tick;
                return tick;
            }
        }
        if self.total_entries >= self.max_entries {
            // Evict the oldest half of tracked in-memory ticks if bounded capacity is reached
            let mut ticks: Vec<u64> = self
                .hot_ticks
                .values()
                .flat_map(|m| m.values().copied())
                .collect();
            if !ticks.is_empty() {
                ticks.sort_unstable();
                let median = ticks[ticks.len() / 2];
                let mut kept = 0usize;
                for m in self.hot_ticks.values_mut() {
                    m.retain(|_, &mut v| v > median);
                    kept += m.len();
                }
                self.total_entries = kept;
            }
        }
        let table_map = self
            .hot_ticks
            .entry(table_id)
            .or_insert_with(|| HashMap::with_capacity(self.max_entries.min(4096)));
        if table_map.insert(pk.to_vec(), tick).is_none() {
            self.total_entries += 1;
        }
        tick
    }

    /// Returns and removes any pending in-memory `AccessTick` for `(table_id, pk)` with zero allocation (`P-04`).
    #[inline]
    pub fn take_hot_tick(&mut self, table_id: u16, pk: &[u8]) -> Option<u64> {
        if self.total_entries == 0 {
            return None;
        }
        let removed = self.hot_ticks.get_mut(&table_id)?.remove(pk);
        if removed.is_some() {
            self.total_entries = self.total_entries.saturating_sub(1);
        }
        removed
    }

    /// Peeks the pending in-memory `AccessTick` for `(table_id, pk)` with zero allocation (`P-04`).
    #[inline]
    pub fn peek_hot_tick(&self, table_id: u16, pk: &[u8]) -> Option<u64> {
        if self.total_entries == 0 {
            return None;
        }
        self.hot_ticks.get(&table_id)?.get(pk).copied()
    }

    /// Clears any pending read tick for `(table_id, pk)` (called on overwrite or explicit delete)
    /// with zero allocation (`P-04`).
    #[inline]
    pub fn remove_key(&mut self, table_id: u16, pk: &[u8]) {
        if self.total_entries == 0 {
            return;
        }
        if let Some(table_map) = self.hot_ticks.get_mut(&table_id) {
            if table_map.remove(pk).is_some() {
                self.total_entries = self.total_entries.saturating_sub(1);
            }
        }
    }

    pub fn len(&self) -> usize {
        self.total_entries
    }

    pub fn is_empty(&self) -> bool {
        self.total_entries == 0
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
    fn p04_access_tracker_two_level_map_borrows_slice_and_bounds_capacity() {
        let mut tracker = AccessTracker::new(100, 64);
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
}
