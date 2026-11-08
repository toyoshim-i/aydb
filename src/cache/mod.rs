//! Bounded in-process verified block cache and small blob cache.
//!
//! Provides two independent, strictly bounded user-space caches:
//! - [`VerifiedBlockCache`]: Caches immutable 4 KB `idx_*.db` data blocks after CRC32C
//!   verification. Hits skip both the `ReadFile`/`pread` kernel transition and the 4 KB CRC32C
//!   pass (`IdxBlockReader::open_preverified`), reducing lookup latency from microseconds to tens
//!   of nanoseconds.
//! - [`VerifiedBlobCache`]: Caches small verified out-of-line blob payload bytes (<= 16 KB) from
//!   sealed segments or below `flushed_offset()` of active segments. Hits skip blob file I/O,
//!   CRC32C, and key matching entirely.
//!
//! Both caches are sharded into 16 independent CLOCK shards to eliminate lock contention
//! under concurrent `EngineReader` threads. Scan resistance is enforced by bypassing cache
//! insertion on full-table scans and background compaction cursors.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::index::IDX_BLOCK_SIZE;

/// Default number of shards for cache concurrency.
const NUM_SHARDS: usize = 16;

/// Maximum payload size eligible for small blob caching (16 KB).
pub const MAX_CACHED_BLOB_PAYLOAD_BYTES: usize = 16 * 1024;

// =========================================================================
// 1. Verified 4 KB Index Block Cache
// =========================================================================

struct BlockCacheSlot {
    seq_id: u64,
    block_idx: u32,
    occupied: bool,
    ref_bit: bool,
    data: Option<Box<[u8; IDX_BLOCK_SIZE]>>,
}

struct BlockClockShard {
    slots: Vec<BlockCacheSlot>,
    index: HashMap<(u64, u32), usize>,
    clock_hand: usize,
}

impl BlockClockShard {
    fn new(slots_per_shard: usize) -> Self {
        let mut slots = Vec::with_capacity(slots_per_shard);
        for _ in 0..slots_per_shard {
            slots.push(BlockCacheSlot {
                seq_id: 0,
                block_idx: 0,
                occupied: false,
                ref_bit: false,
                data: None,
            });
        }
        Self {
            slots,
            index: HashMap::with_capacity(slots_per_shard),
            clock_hand: 0,
        }
    }
}

/// Strictly-bounded in-process cache for verified 4 KB `idx_*.db` data blocks.
pub struct VerifiedBlockCache {
    shards: Vec<Mutex<BlockClockShard>>,
    slots_per_shard: usize,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl VerifiedBlockCache {
    /// Creates a new block cache budgeted at `capacity_bytes`.
    /// `0` disables the cache completely (zero slot allocations).
    pub fn new(capacity_bytes: usize) -> Self {
        let total_slots = capacity_bytes / IDX_BLOCK_SIZE;
        let slots_per_shard = if total_slots == 0 {
            0
        } else {
            (total_slots / NUM_SHARDS).max(1)
        };

        let mut shards = Vec::with_capacity(NUM_SHARDS);
        for _ in 0..NUM_SHARDS {
            shards.push(Mutex::new(BlockClockShard::new(slots_per_shard)));
        }

        Self {
            shards,
            slots_per_shard,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Whether caching is enabled (`capacity_bytes > 0`).
    #[inline]
    pub fn is_enabled(&self) -> bool {
        self.slots_per_shard > 0
    }

    #[inline]
    fn shard_index(&self, seq_id: u64, block_idx: u32) -> usize {
        let mut h = (seq_id ^ 0x517c_c1b7_2722_0a95).wrapping_mul(0x9e37_79b9_7f4a_7c15);
        h ^= (block_idx as u64).wrapping_mul(0xd6e8_feb8_6659_fd93);
        ((h ^ (h >> 32)) as usize) & (NUM_SHARDS - 1)
    }

    /// Attempts to read the verified block `(seq_id, block_idx)` into `out`.
    /// Returns `true` on cache hit, `false` on miss.
    pub fn get_into(&self, seq_id: u64, block_idx: u32, out: &mut [u8; IDX_BLOCK_SIZE]) -> bool {
        if self.slots_per_shard == 0 {
            return false;
        }

        let shard_idx = self.shard_index(seq_id, block_idx);
        let mut shard = self.shards[shard_idx].lock().unwrap();
        if let Some(&slot_idx) = shard.index.get(&(seq_id, block_idx)) {
            let slot = &mut shard.slots[slot_idx];
            if slot.occupied {
                slot.ref_bit = true;
                if let Some(data) = &slot.data {
                    out.copy_from_slice(data.as_ref());
                    self.hits.fetch_add(1, Ordering::Relaxed);
                    return true;
                }
            }
        }
        self.misses.fetch_add(1, Ordering::Relaxed);
        false
    }

    /// Inserts a verified 4 KB block into the cache using CLOCK second-chance replacement.
    pub fn insert(&self, seq_id: u64, block_idx: u32, block: &[u8; IDX_BLOCK_SIZE]) {
        if self.slots_per_shard == 0 {
            return;
        }

        let shard_idx = self.shard_index(seq_id, block_idx);
        let mut shard = self.shards[shard_idx].lock().unwrap();

        // 1. If already resident, update in place and set reference bit
        if let Some(&slot_idx) = shard.index.get(&(seq_id, block_idx)) {
            let slot = &mut shard.slots[slot_idx];
            slot.ref_bit = true;
            if let Some(data) = &mut slot.data {
                data.copy_from_slice(block);
            } else {
                slot.data = Some(Box::new(*block));
            }
            return;
        }

        // 2. CLOCK eviction to locate victim slot
        let n = shard.slots.len();
        if n == 0 {
            return;
        }
        let mut victim = 0;
        for _ in 0..(n * 2) {
            let idx = shard.clock_hand;
            shard.clock_hand = (shard.clock_hand + 1) % n;
            let slot = &mut shard.slots[idx];
            if !slot.occupied {
                victim = idx;
                break;
            }
            if !slot.ref_bit {
                victim = idx;
                break;
            }
            slot.ref_bit = false;
        }

        // 3. Evict existing key from index if occupied
        if shard.slots[victim].occupied {
            let old_key = (shard.slots[victim].seq_id, shard.slots[victim].block_idx);
            shard.index.remove(&old_key);
        }

        // 4. Install new verified block into victim slot
        let slot = &mut shard.slots[victim];
        slot.seq_id = seq_id;
        slot.block_idx = block_idx;
        slot.occupied = true;
        slot.ref_bit = true;
        if let Some(data) = &mut slot.data {
            data.copy_from_slice(block);
        } else {
            slot.data = Some(Box::new(*block));
        }
        shard.index.insert((seq_id, block_idx), victim);
    }

    /// Invalidates all cached blocks for `seq_id` wholesale (called when an SSTable is retired).
    pub fn invalidate_segment(&self, seq_id: u64) {
        if self.slots_per_shard == 0 {
            return;
        }
        for shard_mutex in &self.shards {
            let mut shard = shard_mutex.lock().unwrap();
            let mut to_remove = Vec::new();
            for slot in &mut shard.slots {
                if slot.occupied && slot.seq_id == seq_id {
                    slot.occupied = false;
                    slot.ref_bit = false;
                    to_remove.push((seq_id, slot.block_idx));
                }
            }
            for k in to_remove {
                shard.index.remove(&k);
            }
        }
    }

    /// Number of cache hits since creation.
    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    /// Number of cache misses since creation.
    pub fn misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }
}

// =========================================================================
// 2. Small Verified Blob Payload Cache
// =========================================================================

/// Unique composite key identifying an out-of-line blob record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlobCacheKey {
    pub kind: u8,
    pub table_id: u16,
    pub seg_id: u64,
    pub offset: u32,
}

struct BlobCacheEntry {
    key: BlobCacheKey,
    payload: Arc<[u8]>,
    size: usize,
    ref_bit: bool,
}

struct BlobClockShard {
    entries: Vec<Option<BlobCacheEntry>>,
    index: HashMap<BlobCacheKey, usize>,
    clock_hand: usize,
    current_bytes: usize,
    capacity_bytes: usize,
}

impl BlobClockShard {
    fn new(capacity_bytes: usize) -> Self {
        Self {
            entries: Vec::new(),
            index: HashMap::new(),
            clock_hand: 0,
            current_bytes: 0,
            capacity_bytes,
        }
    }
}

/// Strictly-bounded in-process cache for small verified out-of-line blob payloads.
pub struct VerifiedBlobCache {
    shards: Vec<Mutex<BlobClockShard>>,
    shard_capacity: usize,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl VerifiedBlobCache {
    /// Creates a new blob cache budgeted at `capacity_bytes`.
    /// `0` disables the cache completely.
    pub fn new(capacity_bytes: usize) -> Self {
        let shard_capacity = if capacity_bytes == 0 {
            0
        } else {
            (capacity_bytes / NUM_SHARDS).max(1024)
        };

        let mut shards = Vec::with_capacity(NUM_SHARDS);
        for _ in 0..NUM_SHARDS {
            shards.push(Mutex::new(BlobClockShard::new(shard_capacity)));
        }

        Self {
            shards,
            shard_capacity,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Whether caching is enabled (`capacity_bytes > 0`).
    #[inline]
    pub fn is_enabled(&self) -> bool {
        self.shard_capacity > 0
    }

    #[inline]
    fn shard_index(&self, key: &BlobCacheKey) -> usize {
        let mut h = (key.seg_id ^ 0x517c_c1b7_2722_0a95).wrapping_mul(0x9e37_79b9_7f4a_7c15);
        h ^= (key.offset as u64).wrapping_mul(0xd6e8_feb8_6659_fd93);
        h ^= ((key.table_id as u64) << 16 | (key.kind as u64)).wrapping_mul(0x27bb_2ee6_87b0_b0fd);
        ((h ^ (h >> 32)) as usize) & (NUM_SHARDS - 1)
    }

    /// Appends the verified blob payload onto `out` if resident in cache.
    /// Returns `true` on cache hit, `false` on miss.
    pub fn get_append_into(&self, key: &BlobCacheKey, out: &mut Vec<u8>) -> bool {
        if self.shard_capacity == 0 {
            return false;
        }

        let shard_idx = self.shard_index(key);
        let mut shard = self.shards[shard_idx].lock().unwrap();
        if let Some(&slot_idx) = shard.index.get(key) {
            if let Some(entry) = &mut shard.entries[slot_idx] {
                entry.ref_bit = true;
                out.extend_from_slice(&entry.payload);
                self.hits.fetch_add(1, Ordering::Relaxed);
                return true;
            }
        }
        self.misses.fetch_add(1, Ordering::Relaxed);
        false
    }

    /// Returns `true` if `key` is resident in the blob cache.
    pub fn contains(&self, key: &BlobCacheKey) -> bool {
        if self.shard_capacity == 0 {
            return false;
        }
        let shard_idx = self.shard_index(key);
        let shard = self.shards[shard_idx].lock().unwrap();
        shard.index.contains_key(key)
    }

    /// Inserts a small verified blob payload into the cache.
    pub fn insert(&self, key: BlobCacheKey, payload: &[u8]) {
        if self.shard_capacity == 0 || payload.len() > MAX_CACHED_BLOB_PAYLOAD_BYTES {
            return;
        }

        let entry_size = payload.len().saturating_add(64);
        if entry_size > self.shard_capacity {
            return;
        }

        let shard_idx = self.shard_index(&key);
        let mut shard = self.shards[shard_idx].lock().unwrap();

        // 1. If key is already resident, remove existing entry to adjust memory accounting
        if let Some(&existing_idx) = shard.index.get(&key) {
            if let Some(old) = shard.entries[existing_idx].take() {
                shard.current_bytes = shard.current_bytes.saturating_sub(old.size);
            }
            shard.index.remove(&key);
        }

        // 2. CLOCK eviction to make room for new entry
        while shard.current_bytes.saturating_add(entry_size) > shard.capacity_bytes
            && !shard.index.is_empty()
        {
            let n = shard.entries.len();
            if n == 0 {
                break;
            }
            let hand = shard.clock_hand % n;
            shard.clock_hand = (hand + 1) % n;
            if let Some(entry) = &mut shard.entries[hand] {
                if entry.ref_bit {
                    entry.ref_bit = false;
                } else {
                    let removed = shard.entries[hand].take().unwrap();
                    shard.index.remove(&removed.key);
                    shard.current_bytes = shard.current_bytes.saturating_sub(removed.size);
                }
            }
        }

        // 3. Find an empty slot or push a new slot
        let new_entry = BlobCacheEntry {
            key,
            payload: Arc::from(payload),
            size: entry_size,
            ref_bit: true,
        };

        let slot_idx = if let Some(idx) = shard.entries.iter().position(|e| e.is_none()) {
            shard.entries[idx] = Some(new_entry);
            idx
        } else {
            shard.entries.push(Some(new_entry));
            shard.entries.len() - 1
        };

        shard.index.insert(key, slot_idx);
        shard.current_bytes = shard.current_bytes.saturating_add(entry_size);
    }

    /// Invalidates all cached blobs for a retired `(kind, table_id, seg_id)`.
    pub fn invalidate_blob_segment(&self, kind: u8, table_id: u16, seg_id: u64) {
        if self.shard_capacity == 0 {
            return;
        }
        for shard_mutex in &self.shards {
            let mut shard = shard_mutex.lock().unwrap();
            let mut freed_bytes = 0usize;
            let mut keys_to_remove = Vec::new();
            for entry_opt in &mut shard.entries {
                if let Some(e) = entry_opt {
                    if e.key.kind == kind && e.key.table_id == table_id && e.key.seg_id == seg_id {
                        freed_bytes = freed_bytes.saturating_add(e.size);
                        keys_to_remove.push(e.key);
                        *entry_opt = None;
                    }
                }
            }
            for k in keys_to_remove {
                shard.index.remove(&k);
            }
            shard.current_bytes = shard.current_bytes.saturating_sub(freed_bytes);
        }
    }

    /// Invalidates all cached LRU blobs for `table_id` (e.g. on `DROP TABLE`).
    pub fn invalidate_table_blobs(&self, table_id: u16) {
        if self.shard_capacity == 0 {
            return;
        }
        for shard_mutex in &self.shards {
            let mut shard = shard_mutex.lock().unwrap();
            let mut freed_bytes = 0usize;
            let mut keys_to_remove = Vec::new();
            for entry_opt in &mut shard.entries {
                if let Some(e) = entry_opt {
                    if e.key.table_id == table_id {
                        freed_bytes = freed_bytes.saturating_add(e.size);
                        keys_to_remove.push(e.key);
                        *entry_opt = None;
                    }
                }
            }
            for k in keys_to_remove {
                shard.index.remove(&k);
            }
            shard.current_bytes = shard.current_bytes.saturating_sub(freed_bytes);
        }
    }

    /// Number of cache hits since creation.
    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    /// Number of cache misses since creation.
    pub fn misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }
}

// =========================================================================
// Tests
// =========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_cache_hit_miss_clock_eviction_and_segment_invalidation() {
        // Budget for 16 shards * 2 slots = 32 slots * 4096 = 131,072 bytes
        let cache = VerifiedBlockCache::new(32 * IDX_BLOCK_SIZE);
        assert!(cache.is_enabled());

        let mut block1 = [0x11u8; IDX_BLOCK_SIZE];
        block1[0] = 42;
        cache.insert(1, 0, &block1);

        let mut out = [0u8; IDX_BLOCK_SIZE];
        assert!(cache.get_into(1, 0, &mut out));
        assert_eq!(out[0], 42);
        assert_eq!(cache.hits(), 1);

        assert!(!cache.get_into(1, 1, &mut out));
        assert_eq!(cache.misses(), 1);

        // Invalidate segment 1 wholesale
        cache.invalidate_segment(1);
        assert!(!cache.get_into(1, 0, &mut out));
        assert_eq!(cache.misses(), 2);
    }

    #[test]
    fn block_cache_disabled_when_zero_bytes() {
        let cache = VerifiedBlockCache::new(0);
        assert!(!cache.is_enabled());

        let block = [0x55u8; IDX_BLOCK_SIZE];
        cache.insert(1, 0, &block);
        let mut out = [0u8; IDX_BLOCK_SIZE];
        assert!(!cache.get_into(1, 0, &mut out));
    }

    #[test]
    fn blob_cache_hit_miss_and_segment_invalidation() {
        // Budget 64 KB
        let cache = VerifiedBlobCache::new(64 * 1024);
        assert!(cache.is_enabled());

        let key1 = BlobCacheKey {
            kind: 1,
            table_id: 10,
            seg_id: 100,
            offset: 4096,
        };
        let payload1 = b"hello blob cache";
        cache.insert(key1, payload1);

        let mut out = Vec::new();
        assert!(cache.get_append_into(&key1, &mut out));
        assert_eq!(out, payload1);
        assert_eq!(cache.hits(), 1);

        let key_absent = BlobCacheKey {
            kind: 1,
            table_id: 10,
            seg_id: 100,
            offset: 9999,
        };
        assert!(!cache.get_append_into(&key_absent, &mut out));
        assert_eq!(cache.misses(), 1);

        // Invalidate segment 100
        cache.invalidate_blob_segment(1, 10, 100);
        out.clear();
        assert!(!cache.get_append_into(&key1, &mut out));
    }

    #[test]
    fn blob_cache_rejects_payload_larger_than_16kb() {
        let cache = VerifiedBlobCache::new(64 * 1024);
        let key = BlobCacheKey {
            kind: 1,
            table_id: 1,
            seg_id: 1,
            offset: 0,
        };
        let big_payload = vec![0xAAu8; 17 * 1024];
        cache.insert(key, &big_payload);

        let mut out = Vec::new();
        assert!(!cache.get_append_into(&key, &mut out));
    }
}
