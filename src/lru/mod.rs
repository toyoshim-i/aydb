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
pub struct AccessTracker {
    next_tick: AtomicU64,
    /// Maps `(table_id, pk_bytes)` -> `latest_access_tick` for recently read keys.
    hot_ticks: HashMap<(u16, Vec<u8>), u64>,
    max_entries: usize,
}

impl AccessTracker {
    pub fn new(initial_tick: u64, max_entries: usize) -> Self {
        Self {
            next_tick: AtomicU64::new(initial_tick.max(1)),
            hot_ticks: HashMap::with_capacity(max_entries.min(4096)),
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
    /// Returns the newly assigned `AccessTick`.
    pub fn record_read_hit(&mut self, table_id: u16, pk: &[u8]) -> u64 {
        let tick = self.next_tick();
        if self.hot_ticks.len() >= self.max_entries
            && !self.hot_ticks.contains_key(&(table_id, pk.to_vec()))
        {
            // Evict the oldest half of tracked in-memory ticks if bounded capacity is reached
            let mut ticks: Vec<u64> = self.hot_ticks.values().copied().collect();
            ticks.sort_unstable();
            let median = ticks[ticks.len() / 2];
            self.hot_ticks.retain(|_, &mut v| v > median);
        }
        self.hot_ticks.insert((table_id, pk.to_vec()), tick);
        tick
    }

    /// Returns and removes any pending in-memory `AccessTick` for `(table_id, pk)`.
    pub fn take_hot_tick(&mut self, table_id: u16, pk: &[u8]) -> Option<u64> {
        self.hot_ticks.remove(&(table_id, pk.to_vec()))
    }

    /// Peeks the pending in-memory `AccessTick` for `(table_id, pk)`.
    pub fn peek_hot_tick(&self, table_id: u16, pk: &[u8]) -> Option<u64> {
        self.hot_ticks.get(&(table_id, pk.to_vec())).copied()
    }

    /// Clears any pending read tick for `(table_id, pk)` (called on overwrite or explicit delete).
    pub fn remove_key(&mut self, table_id: u16, pk: &[u8]) {
        self.hot_ticks.remove(&(table_id, pk.to_vec()));
    }

    pub fn len(&self) -> usize {
        self.hot_ticks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.hot_ticks.is_empty()
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
}
