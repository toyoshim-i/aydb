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
use std::sync::{Arc, Mutex, MutexGuard};

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
    /// Shared so that a hit can copy or search the block after the shard lock is released. A
    /// slot's block is never mutated in place: replacing the entry installs a new `Arc`, so a
    /// reader that still holds the old one keeps seeing the block it was given.
    data: Option<Arc<[u8; IDX_BLOCK_SIZE]>>,
}

struct BlockClockShard {
    slots: Vec<BlockCacheSlot>,
    index: HashMap<(u64, u32), usize>,
    clock_hand: usize,
    hits: u64,
    misses: u64,
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
            hits: 0,
            misses: 0,
        }
    }
}

#[repr(align(128))]
struct PaddedBlockShard(Mutex<BlockClockShard>);

impl PaddedBlockShard {
    #[inline]
    fn lock(&self) -> MutexGuard<'_, BlockClockShard> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// Strictly-bounded in-process cache for verified 4 KB `idx_*.db` data blocks.
pub struct VerifiedBlockCache {
    shards: Vec<PaddedBlockShard>,
    total_slots: usize,
}

impl VerifiedBlockCache {
    /// Creates a new block cache budgeted at `capacity_bytes`.
    /// Fewer than `IDX_BLOCK_SIZE` bytes (including `0`) disables the cache (zero slot allocations).
    pub fn new(capacity_bytes: usize) -> Self {
        let total_slots = capacity_bytes / IDX_BLOCK_SIZE;
        let base = total_slots / NUM_SHARDS;
        let rem = total_slots % NUM_SHARDS;

        let mut shards = Vec::with_capacity(NUM_SHARDS);
        for i in 0..NUM_SHARDS {
            let shard_slots = base + usize::from(i < rem);
            shards.push(PaddedBlockShard(Mutex::new(BlockClockShard::new(
                shard_slots,
            ))));
        }

        Self {
            shards,
            total_slots,
        }
    }

    /// Whether caching is enabled (at least one 4 KB slot allocated).
    #[inline]
    pub fn is_enabled(&self) -> bool {
        self.total_slots > 0
    }

    #[inline]
    fn shard_index(&self, seq_id: u64, block_idx: u32) -> usize {
        let mut h = (seq_id ^ 0x517c_c1b7_2722_0a95).wrapping_mul(0x9e37_79b9_7f4a_7c15);
        h ^= (block_idx as u64).wrapping_mul(0xd6e8_feb8_6659_fd93);
        ((h ^ (h >> 32)) as usize) & (NUM_SHARDS - 1)
    }

    /// Returns a shared handle to the verified block `(seq_id, block_idx)` on a hit.
    /// The handle is cloned under the shard lock; the caller reads the block after it is released.
    pub fn get_arc(&self, seq_id: u64, block_idx: u32) -> Option<Arc<[u8; IDX_BLOCK_SIZE]>> {
        if !self.is_enabled() {
            return None;
        }

        let shard_idx = self.shard_index(seq_id, block_idx);
        let mut shard = self.shards[shard_idx].lock();
        let found = match shard.index.get(&(seq_id, block_idx)).copied() {
            Some(slot_idx) => {
                let slot = &mut shard.slots[slot_idx];
                if slot.occupied {
                    slot.ref_bit = true;
                    slot.data.clone()
                } else {
                    None
                }
            }
            None => None,
        };
        if found.is_some() {
            shard.hits = shard.hits.wrapping_add(1);
        } else {
            shard.misses = shard.misses.wrapping_add(1);
        }
        found
    }

    /// Attempts to read the verified block `(seq_id, block_idx)` into `out`.
    /// Returns `true` on cache hit, `false` on miss.
    pub fn get_into(&self, seq_id: u64, block_idx: u32, out: &mut [u8; IDX_BLOCK_SIZE]) -> bool {
        match self.get_arc(seq_id, block_idx) {
            Some(block) => {
                out.copy_from_slice(block.as_ref());
                true
            }
            None => false,
        }
    }

    /// Inserts a verified 4 KB block into the cache using CLOCK second-chance replacement.
    pub fn insert(&self, seq_id: u64, block_idx: u32, block: &[u8; IDX_BLOCK_SIZE]) {
        if !self.is_enabled() {
            return;
        }

        // Allocated and filled before taking the lock. A replaced slot gets a fresh allocation
        // because readers may still hold the previous `Arc`.
        let new_block = Arc::new(*block);
        let shard_idx = self.shard_index(seq_id, block_idx);
        let evicted: Option<Arc<[u8; IDX_BLOCK_SIZE]>>;
        {
            let mut shard = self.shards[shard_idx].lock();

            // 1. If already resident, replace the block and set the reference bit
            if let Some(&slot_idx) = shard.index.get(&(seq_id, block_idx)) {
                let slot = &mut shard.slots[slot_idx];
                slot.ref_bit = true;
                evicted = slot.data.replace(new_block);
                drop(shard);
                drop(evicted);
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
                if !slot.occupied || !slot.ref_bit {
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
            evicted = slot.data.replace(new_block);
            shard.index.insert((seq_id, block_idx), victim);
        }
        drop(evicted);
    }

    /// Invalidates all cached blocks for `seq_id` wholesale (called when an SSTable is retired).
    pub fn invalidate_segment(&self, seq_id: u64) {
        if !self.is_enabled() {
            return;
        }
        let mut released = Vec::new();
        for shard_wrapper in &self.shards {
            {
                let mut shard_guard = shard_wrapper.lock();
                let shard = &mut *shard_guard;
                for slot in &mut shard.slots {
                    if slot.occupied && slot.seq_id == seq_id {
                        slot.occupied = false;
                        slot.ref_bit = false;
                        if let Some(arc) = slot.data.take() {
                            released.push(arc);
                        }
                        shard.index.remove(&(seq_id, slot.block_idx));
                    }
                }
            }
            released.clear();
        }
    }

    /// Number of cache hits since creation.
    pub fn hits(&self) -> u64 {
        if !self.is_enabled() {
            return 0;
        }
        self.shards
            .iter()
            .fold(0u64, |acc, s| acc.wrapping_add(s.lock().hits))
    }

    /// Number of cache misses since creation.
    pub fn misses(&self) -> u64 {
        if !self.is_enabled() {
            return 0;
        }
        self.shards
            .iter()
            .fold(0u64, |acc, s| acc.wrapping_add(s.lock().misses))
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
    pub len: u32,
    pub crc32c: u32,
    pub key_hash: u64,
}

impl BlobCacheKey {
    #[inline]
    pub fn from_ptr(ptr: &crate::wal::BlobPtr, expected_key: &[u8]) -> Self {
        Self {
            kind: ptr.kind as u8,
            table_id: ptr.table_id,
            seg_id: ptr.seg_id,
            offset: ptr.offset,
            len: ptr.len,
            crc32c: ptr.crc32c,
            key_hash: crate::index::BloomFilter::hash64(expected_key),
        }
    }
}

struct BlobCacheEntry {
    key: BlobCacheKey,
    payload: Arc<[u8]>,
    size: usize,
    ref_bit: bool,
}

struct BlobClockShard {
    entries: Vec<Option<BlobCacheEntry>>,
    free_slots: Vec<usize>,
    index: HashMap<BlobCacheKey, usize>,
    clock_hand: usize,
    current_bytes: usize,
    capacity_bytes: usize,
    hits: u64,
    misses: u64,
}

impl BlobClockShard {
    fn new(capacity_bytes: usize) -> Self {
        Self {
            entries: Vec::new(),
            free_slots: Vec::new(),
            index: HashMap::new(),
            clock_hand: 0,
            current_bytes: 0,
            capacity_bytes,
            hits: 0,
            misses: 0,
        }
    }

    fn maybe_compact(&mut self) {
        if self.index.is_empty() {
            self.entries.clear();
            self.free_slots.clear();
            self.clock_hand = 0;
            return;
        }
        if self.entries.len() > 64 && self.free_slots.len() > self.entries.len() / 2 {
            let mut write = 0usize;
            for read in 0..self.entries.len() {
                if let Some(entry) = self.entries[read].take() {
                    self.index.insert(entry.key, write);
                    self.entries[write] = Some(entry);
                    write += 1;
                }
            }
            self.entries.truncate(write);
            self.free_slots.clear();
            self.clock_hand %= write.max(1);
        }
    }
}

#[repr(align(128))]
struct PaddedBlobShard(Mutex<BlobClockShard>);

impl PaddedBlobShard {
    #[inline]
    fn lock(&self) -> MutexGuard<'_, BlobClockShard> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }
}

const BLOB_CACHE_ENTRY_OVERHEAD: usize = 64;

/// Strictly-bounded in-process cache for small verified out-of-line blob payloads.
pub struct VerifiedBlobCache {
    shards: Vec<PaddedBlobShard>,
    max_shard_capacity: usize,
}

impl VerifiedBlobCache {
    /// Creates a new blob cache budgeted strictly within `capacity_bytes`.
    /// Fewer than `64` bytes (the per-entry metadata overhead, including `0`) disables the cache.
    pub fn new(capacity_bytes: usize) -> Self {
        let mut shard_caps = [0usize; NUM_SHARDS];
        if capacity_bytes >= BLOB_CACHE_ENTRY_OVERHEAD {
            let active_shards = (capacity_bytes / BLOB_CACHE_ENTRY_OVERHEAD).min(NUM_SHARDS);
            let base = capacity_bytes / active_shards;
            let rem = capacity_bytes % active_shards;
            for (i, cap) in shard_caps.iter_mut().take(active_shards).enumerate() {
                *cap = base + usize::from(i < rem);
            }
        }
        let max_shard_capacity = shard_caps.iter().copied().max().unwrap_or(0);

        let mut shards = Vec::with_capacity(NUM_SHARDS);
        for cap in shard_caps {
            shards.push(PaddedBlobShard(Mutex::new(BlobClockShard::new(cap))));
        }

        Self {
            shards,
            max_shard_capacity,
        }
    }

    /// Whether caching is enabled (at least one entry fits within `capacity_bytes`).
    #[inline]
    pub fn is_enabled(&self) -> bool {
        self.max_shard_capacity > 0
    }

    #[inline]
    fn shard_index(&self, key: &BlobCacheKey) -> usize {
        let mut h = (key.seg_id ^ 0x517c_c1b7_2722_0a95).wrapping_mul(0x9e37_79b9_7f4a_7c15);
        h ^= (key.offset as u64).wrapping_mul(0xd6e8_feb8_6659_fd93);
        h ^= ((key.len as u64) << 32 | (key.crc32c as u64)).wrapping_mul(0x94d0_49bb_1331_11eb);
        h ^= key.key_hash.wrapping_mul(0xbf58_476d_1ce4_e5b9);
        h ^= ((key.table_id as u64) << 16 | (key.kind as u64)).wrapping_mul(0x27bb_2ee6_87b0_b0fd);
        ((h ^ (h >> 32)) as usize) & (NUM_SHARDS - 1)
    }

    /// Appends the verified blob payload onto `out` if resident in cache.
    /// Returns `true` on cache hit, `false` on miss.
    pub fn get_append_into(&self, key: &BlobCacheKey, out: &mut Vec<u8>) -> bool {
        if !self.is_enabled() {
            return false;
        }

        let shard_idx = self.shard_index(key);
        let payload = {
            let mut shard = self.shards[shard_idx].lock();
            if let Some(&slot_idx) = shard.index.get(key) {
                if let Some(entry) = &mut shard.entries[slot_idx] {
                    entry.ref_bit = true;
                    let arc = Arc::clone(&entry.payload);
                    shard.hits = shard.hits.wrapping_add(1);
                    Some(arc)
                } else {
                    shard.misses = shard.misses.wrapping_add(1);
                    None
                }
            } else {
                shard.misses = shard.misses.wrapping_add(1);
                None
            }
        };
        if let Some(payload) = payload {
            out.extend_from_slice(&payload);
            true
        } else {
            false
        }
    }

    /// Returns `true` if `key` is resident in the blob cache.
    pub fn contains(&self, key: &BlobCacheKey) -> bool {
        if !self.is_enabled() {
            return false;
        }
        let shard_idx = self.shard_index(key);
        let shard = self.shards[shard_idx].lock();
        shard.index.contains_key(key)
    }

    /// Inserts a small verified blob payload into the cache.
    pub fn insert(&self, key: BlobCacheKey, payload: &[u8]) {
        if !self.is_enabled() || payload.len() > MAX_CACHED_BLOB_PAYLOAD_BYTES {
            return;
        }

        let entry_size = payload.len().saturating_add(BLOB_CACHE_ENTRY_OVERHEAD);
        if entry_size > self.max_shard_capacity {
            return;
        }

        // Allocate outside the shard lock so lock hold time is purely pointer/map bookkeeping.
        let shard_idx = self.shard_index(&key);
        if entry_size > self.shards[shard_idx].lock().capacity_bytes {
            return;
        }
        let new_entry = BlobCacheEntry {
            key,
            payload: Arc::from(payload),
            size: entry_size,
            ref_bit: true,
        };
        let mut evicted: Vec<Arc<[u8]>> = Vec::new();
        {
            let mut shard = self.shards[shard_idx].lock();
            if entry_size > shard.capacity_bytes {
                return;
            }

            // 1. If key is already resident, remove existing entry to adjust memory accounting
            if let Some(&existing_idx) = shard.index.get(&key) {
                if let Some(old) = shard.entries[existing_idx].take() {
                    shard.current_bytes = shard.current_bytes.saturating_sub(old.size);
                    evicted.push(old.payload);
                }
                shard.index.remove(&key);
                shard.free_slots.push(existing_idx);
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
                        shard.free_slots.push(hand);
                        evicted.push(removed.payload);
                    }
                }
            }

            shard.maybe_compact();

            // 3. Install into an O(1) free slot or push a new slot
            let slot_idx = if let Some(idx) = shard.free_slots.pop() {
                shard.entries[idx] = Some(new_entry);
                idx
            } else {
                shard.entries.push(Some(new_entry));
                shard.entries.len() - 1
            };

            shard.index.insert(key, slot_idx);
            shard.current_bytes = shard.current_bytes.saturating_add(entry_size);
        }
        drop(evicted);
    }

    fn invalidate_matching<F>(&self, mut pred: F)
    where
        F: FnMut(&BlobCacheKey) -> bool,
    {
        if !self.is_enabled() {
            return;
        }
        let mut evicted: Vec<Arc<[u8]>> = Vec::new();
        for shard_wrapper in &self.shards {
            {
                let mut shard = shard_wrapper.lock();
                let mut freed_bytes = 0usize;
                for idx in 0..shard.entries.len() {
                    let matches = shard.entries[idx].as_ref().is_some_and(|e| pred(&e.key));
                    if matches {
                        if let Some(removed) = shard.entries[idx].take() {
                            freed_bytes = freed_bytes.saturating_add(removed.size);
                            shard.index.remove(&removed.key);
                            shard.free_slots.push(idx);
                            evicted.push(removed.payload);
                        }
                    }
                }
                shard.current_bytes = shard.current_bytes.saturating_sub(freed_bytes);
                shard.maybe_compact();
            }
            evicted.clear();
        }
    }

    /// Invalidates all cached blobs for a retired `(kind, table_id, seg_id)`.
    pub fn invalidate_blob_segment(&self, kind: u8, table_id: u16, seg_id: u64) {
        self.invalidate_matching(|k| {
            k.kind == kind && k.table_id == table_id && k.seg_id == seg_id
        });
    }

    /// Invalidates all cached LRU blobs for `table_id` (e.g. on `DROP TABLE`).
    pub fn invalidate_table_blobs(&self, table_id: u16) {
        let lru_kind = crate::wal::BlobKind::Lru as u8;
        self.invalidate_matching(|k| k.kind == lru_kind && k.table_id == table_id);
    }

    /// Number of cache hits since creation.
    pub fn hits(&self) -> u64 {
        if !self.is_enabled() {
            return 0;
        }
        self.shards
            .iter()
            .fold(0u64, |acc, s| acc.wrapping_add(s.lock().hits))
    }

    /// Number of cache misses since creation.
    pub fn misses(&self) -> u64 {
        if !self.is_enabled() {
            return 0;
        }
        self.shards
            .iter()
            .fold(0u64, |acc, s| acc.wrapping_add(s.lock().misses))
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
    fn block_cache_get_arc_counts_hits_and_keeps_held_blocks_stable() {
        // One slot per shard, so a second block hashed to the same shard replaces the first.
        let cache = VerifiedBlockCache::new(NUM_SHARDS * IDX_BLOCK_SIZE);
        assert!(cache.get_arc(1, 0).is_none());
        assert_eq!((cache.hits(), cache.misses()), (0, 1));

        cache.insert(1, 0, &[0x11u8; IDX_BLOCK_SIZE]);
        let held = cache.get_arc(1, 0).expect("resident block");
        assert_eq!(held[0], 0x11);
        assert_eq!((cache.hits(), cache.misses()), (1, 1));

        // Re-inserting the same key and evicting it by other keys must not change the block that
        // a reader already holds.
        cache.insert(1, 0, &[0x22u8; IDX_BLOCK_SIZE]);
        assert_eq!(held[0], 0x11);
        assert_eq!(cache.get_arc(1, 0).unwrap()[0], 0x22);
        for blk in 1..(NUM_SHARDS as u32 * 8) {
            cache.insert(1, blk, &[0x33u8; IDX_BLOCK_SIZE]);
        }
        assert!(cache.get_arc(1, 0).is_none());
        assert_eq!(held[0], 0x11);
        assert!(held.iter().all(|&b| b == 0x11));

        let mut out = [0u8; IDX_BLOCK_SIZE];
        let hits = cache.hits();
        assert!(!cache.get_into(1, 0, &mut out));
        assert_eq!(cache.hits(), hits);
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
            kind: crate::wal::BlobKind::Lru as u8,
            table_id: 10,
            seg_id: 100,
            offset: 4096,
            len: 32,
            crc32c: 0x1111_2222,
            key_hash: 0xAAAA_BBBB,
        };
        let payload1 = b"hello blob cache";
        cache.insert(key1, payload1);

        let mut out = Vec::new();
        assert!(cache.get_append_into(&key1, &mut out));
        assert_eq!(out, payload1);
        assert_eq!(cache.hits(), 1);

        let key_absent = BlobCacheKey {
            kind: crate::wal::BlobKind::Lru as u8,
            table_id: 10,
            seg_id: 100,
            offset: 9999,
            len: 32,
            crc32c: 0x1111_2222,
            key_hash: 0xAAAA_BBBB,
        };
        assert!(!cache.get_append_into(&key_absent, &mut out));
        assert_eq!(cache.misses(), 1);

        // Invalidate segment 100
        cache.invalidate_blob_segment(crate::wal::BlobKind::Lru as u8, 10, 100);
        out.clear();
        assert!(!cache.get_append_into(&key1, &mut out));
    }

    #[test]
    fn blob_cache_rejects_payload_larger_than_16kb() {
        let cache = VerifiedBlobCache::new(64 * 1024);
        let key = BlobCacheKey {
            kind: crate::wal::BlobKind::Lru as u8,
            table_id: 1,
            seg_id: 1,
            offset: 0,
            len: (17 * 1024) as u32,
            crc32c: 0x5555_6666,
            key_hash: 0xCCCC_DDDD,
        };
        let big_payload = vec![0xAAu8; 17 * 1024];
        cache.insert(key, &big_payload);

        let mut out = Vec::new();
        assert!(!cache.get_append_into(&key, &mut out));
    }

    #[test]
    fn block_cache_invalidate_releases_arc_and_blob_cache_key_distinguishes_crc_len_key() {
        let block_cache = VerifiedBlockCache::new(1024 * 1024);
        let block = [0x3Cu8; IDX_BLOCK_SIZE];
        block_cache.insert(7, 0, &block);
        let held = block_cache.get_arc(7, 0).unwrap();
        // `held` + cache slot hold two strong references.
        assert_eq!(Arc::strong_count(&held), 2);
        block_cache.invalidate_segment(7);
        assert_eq!(Arc::strong_count(&held), 1);

        let blob_cache = VerifiedBlobCache::new(64 * 1024);
        let base_ptr = crate::wal::BlobPtr {
            kind: crate::wal::BlobKind::Standard,
            table_id: 0,
            seg_id: 5,
            offset: 128,
            len: 64,
            crc32c: 0x1234_5678,
        };
        let std_key = BlobCacheKey::from_ptr(&base_ptr, b"row_a");
        blob_cache.insert(std_key, b"payload_a");

        // Standard blobs (`table_id == 0`) must not be wiped by `invalidate_table_blobs(0)`.
        blob_cache.invalidate_table_blobs(0);
        let mut out = Vec::new();
        assert!(blob_cache.get_append_into(&std_key, &mut out));
        assert_eq!(out, b"payload_a");

        // Same `(kind, table_id, seg_id, offset)` with different len / crc32c / key must miss.
        let mut diff_len = base_ptr;
        diff_len.len = 65;
        assert!(!blob_cache.contains(&BlobCacheKey::from_ptr(&diff_len, b"row_a")));

        let mut diff_crc = base_ptr;
        diff_crc.crc32c ^= 1;
        assert!(!blob_cache.contains(&BlobCacheKey::from_ptr(&diff_crc, b"row_a")));

        assert!(!blob_cache.contains(&BlobCacheKey::from_ptr(&base_ptr, b"row_b")));
    }

    #[test]
    fn cache_shards_are_cache_line_aligned() {
        assert!(std::mem::align_of::<PaddedBlockShard>() >= 128);
        assert!(std::mem::align_of::<PaddedBlobShard>() >= 128);
    }

    #[test]
    fn blob_cache_reuses_free_slots_and_compacts_after_shrink_and_invalidation() {
        let cache = VerifiedBlobCache::new(256 * 1024);
        let lru_kind = crate::wal::BlobKind::Lru as u8;

        let small_payload = vec![0x42u8; 64];
        for i in 0..3200u32 {
            let key = BlobCacheKey {
                kind: lru_kind,
                table_id: 1,
                seg_id: 1,
                offset: i * 128,
                len: 64,
                crc32c: i,
                key_hash: i as u64 + 1,
            };
            cache.insert(key, &small_payload);
        }

        let mut large_entries = Vec::new();
        for i in 0..160u32 {
            let key = BlobCacheKey {
                kind: lru_kind,
                table_id: 1,
                seg_id: 1,
                offset: 1_000_000 + i * 4096,
                len: 2048,
                crc32c: 10_000 + i,
                key_hash: 100_000 + i as u64,
            };
            let payload = vec![(i & 0xFF) as u8; 2048];
            cache.insert(key, &payload);
            large_entries.push((key, payload));
        }

        let mut buf = Vec::new();
        for (key, expected) in &large_entries {
            if cache.contains(key) {
                buf.clear();
                assert!(cache.get_append_into(key, &mut buf));
                assert_eq!(&buf, expected);
            }
        }

        for shard_wrapper in &cache.shards {
            let shard = shard_wrapper.lock();
            assert_eq!(shard.entries.iter().flatten().count(), shard.index.len());
            assert!(
                shard.entries.len() <= 64.max(shard.index.len().saturating_mul(2)),
                "shard accumulated too many holes: entries={} live={}",
                shard.entries.len(),
                shard.index.len()
            );
        }

        cache.invalidate_blob_segment(lru_kind, 1, 1);
        for shard_wrapper in &cache.shards {
            let shard = shard_wrapper.lock();
            assert!(shard.entries.is_empty());
            assert!(shard.free_slots.is_empty());
            assert!(shard.index.is_empty());
            assert_eq!(shard.current_bytes, 0);
        }
    }

    #[test]
    fn caches_track_hits_and_misses_accurately_under_concurrent_access() {
        let block_cache = Arc::new(VerifiedBlockCache::new(32 * IDX_BLOCK_SIZE));
        let blob_cache = Arc::new(VerifiedBlobCache::new(64 * 1024));
        let lru_kind = crate::wal::BlobKind::Lru as u8;

        std::thread::scope(|s| {
            for t in 0..4u32 {
                let bc = Arc::clone(&block_cache);
                let blc = Arc::clone(&blob_cache);
                s.spawn(move || {
                    let mut out_blk = [0u8; IDX_BLOCK_SIZE];
                    let mut out_blob = Vec::new();
                    for i in 0..500u32 {
                        let blk_idx = (i + t * 7) % 48;
                        if !bc.get_into(1, blk_idx, &mut out_blk) {
                            let fill = [((blk_idx ^ t) & 0xFF) as u8; IDX_BLOCK_SIZE];
                            bc.insert(1, blk_idx, &fill);
                        }

                        let blob_id = (i + t * 5) % 64;
                        let key = BlobCacheKey {
                            kind: lru_kind,
                            table_id: 2,
                            seg_id: 3,
                            offset: blob_id * 512,
                            len: 128,
                            crc32c: blob_id,
                            key_hash: blob_id as u64 + 99,
                        };
                        out_blob.clear();
                        if !blc.get_append_into(&key, &mut out_blob) {
                            let payload = vec![(blob_id & 0xFF) as u8; 128];
                            blc.insert(key, &payload);
                        }
                    }
                });
            }
        });

        assert_eq!(block_cache.hits() + block_cache.misses(), 2000);
        assert_eq!(blob_cache.hits() + blob_cache.misses(), 2000);
    }

    #[test]
    fn tiny_non_zero_cache_budgets_never_exceed_configured_capacity() {
        let disabled_blk = VerifiedBlockCache::new(IDX_BLOCK_SIZE - 1);
        assert!(!disabled_blk.is_enabled());
        assert_eq!(
            disabled_blk
                .shards
                .iter()
                .map(|s| s.lock().slots.len())
                .sum::<usize>(),
            0
        );

        let one_blk = VerifiedBlockCache::new(IDX_BLOCK_SIZE);
        assert!(one_blk.is_enabled());
        assert_eq!(
            one_blk
                .shards
                .iter()
                .map(|s| s.lock().slots.len())
                .sum::<usize>(),
            1
        );
        one_blk.insert(1, 0, &[0xAB; IDX_BLOCK_SIZE]);

        let tiny_blob = VerifiedBlobCache::new(63);
        assert!(!tiny_blob.is_enabled());
        assert_eq!(
            tiny_blob
                .shards
                .iter()
                .map(|s| s.lock().capacity_bytes)
                .sum::<usize>(),
            0
        );

        let exact_blob = VerifiedBlobCache::new(100);
        assert!(exact_blob.is_enabled());
        assert_eq!(
            exact_blob
                .shards
                .iter()
                .map(|s| s.lock().capacity_bytes)
                .sum::<usize>(),
            100
        );
        let k = BlobCacheKey {
            kind: crate::wal::BlobKind::Standard as u8,
            table_id: 1,
            seg_id: 1,
            offset: 0,
            len: 50,
            crc32c: 1,
            key_hash: 1,
        };
        // Overhead (64) + payload (50) = 114 > 100 must be rejected.
        exact_blob.insert(k, &[0x11; 50]);
        assert!(!exact_blob.contains(&k));
        // Overhead (64) + payload (32) = 96 <= 100 may admit if hashed to active shard.
        let k2 = BlobCacheKey { len: 32, ..k };
        exact_blob.insert(k2, &[0x22; 32]);
        for shard in &exact_blob.shards {
            let g = shard.lock();
            assert!(g.current_bytes <= g.capacity_bytes);
        }
    }
}
