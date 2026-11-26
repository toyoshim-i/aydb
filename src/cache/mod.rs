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
use std::sync::Arc;

use crate::index::IDX_BLOCK_SIZE;
use crate::util::CachePaddedMutex;

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

/// Strictly-bounded in-process cache for verified 4 KB `idx_*.db` data blocks.
pub struct VerifiedBlockCache {
    shards: Vec<CachePaddedMutex<BlockClockShard>>,
    total_slots: usize,
}

impl VerifiedBlockCache {
    /// Creates a new block cache budgeted at `capacity_bytes`.
    /// Fewer than `IDX_BLOCK_SIZE` bytes (including `0`) disables the cache (zero slot allocations).
    pub fn new(capacity_bytes: usize) -> Self {
        let total_slots = capacity_bytes / IDX_BLOCK_SIZE;
        if total_slots == 0 {
            return Self {
                shards: Vec::new(),
                total_slots: 0,
            };
        }

        let num_shards = total_slots.min(NUM_SHARDS);
        let base = total_slots / num_shards;
        let rem = total_slots % num_shards;

        let mut shards = Vec::with_capacity(num_shards);
        for i in 0..num_shards {
            let shard_slots = base + usize::from(i < rem);
            shards.push(CachePaddedMutex::new(BlockClockShard::new(shard_slots)));
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
        let mixed = (h ^ (h >> 32)) as usize;
        let len = self.shards.len();
        if len == NUM_SHARDS {
            mixed & (NUM_SHARDS - 1)
        } else {
            mixed % len
        }
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
            if self.entries.capacity() > 64 {
                self.entries.shrink_to(0);
            }
            if self.free_slots.capacity() > 64 {
                self.free_slots.shrink_to(0);
            }
            if self.index.capacity() > 64 {
                self.index.shrink_to(0);
            }
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
            let target_entries = (write * 2).max(64);
            if self.entries.capacity() > target_entries.saturating_mul(2) {
                self.entries.shrink_to(target_entries);
            }
            if self.free_slots.capacity() > 64 {
                self.free_slots.shrink_to(0);
            }
            let target_index = (self.index.len() * 2).max(64);
            if self.index.capacity() > target_index.saturating_mul(2) {
                self.index.shrink_to(target_index);
            }
        }
    }
}

const BLOB_CACHE_ENTRY_OVERHEAD: usize = 64;

/// Strictly-bounded in-process cache for small verified out-of-line blob payloads.
pub struct VerifiedBlobCache {
    shards: Vec<CachePaddedMutex<BlobClockShard>>,
    shard_caps: Vec<usize>,
    max_shard_capacity: usize,
}

impl VerifiedBlobCache {
    /// Creates a new blob cache budgeted strictly within `capacity_bytes`.
    /// Fewer than `64` bytes (the per-entry metadata overhead, including `0`) disables the cache.
    pub fn new(capacity_bytes: usize) -> Self {
        if capacity_bytes < BLOB_CACHE_ENTRY_OVERHEAD {
            return Self {
                shards: Vec::new(),
                shard_caps: Vec::new(),
                max_shard_capacity: 0,
            };
        }

        const MIN_SHARD_BUDGET: usize = MAX_CACHED_BLOB_PAYLOAD_BYTES + BLOB_CACHE_ENTRY_OVERHEAD;
        let num_shards = (capacity_bytes / MIN_SHARD_BUDGET).clamp(1, NUM_SHARDS);
        let base = capacity_bytes / num_shards;
        let rem = capacity_bytes % num_shards;

        let mut shard_caps = Vec::with_capacity(num_shards);
        let mut shards = Vec::with_capacity(num_shards);
        for i in 0..num_shards {
            let cap = base + usize::from(i < rem);
            shard_caps.push(cap);
            shards.push(CachePaddedMutex::new(BlobClockShard::new(cap)));
        }
        let max_shard_capacity = shard_caps.iter().copied().max().unwrap_or(0);

        Self {
            shards,
            shard_caps,
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
        let mixed = (h ^ (h >> 32)) as usize;
        let len = self.shards.len();
        if len == NUM_SHARDS {
            mixed & (NUM_SHARDS - 1)
        } else {
            mixed % len
        }
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

        // Check per-shard cap and allocate outside the shard lock so lock hold time is purely
        // pointer/map bookkeeping (single lock acquisition).
        let shard_idx = self.shard_index(&key);
        if entry_size > self.shard_caps[shard_idx] {
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
mod tests;
