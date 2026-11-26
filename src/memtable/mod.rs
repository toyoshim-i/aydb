//! Layer 4 (Part 1): Bounded In-Memory `MemTable`.
//!
//! Absorbs the latest mutations (`InlineValue`, `BlobPtr`, or `Tombstone`) in sorted key order
//! while enforcing a strict heap memory budget (e.g., 4 MB). Because large payloads (`>= 1 KB`)
//! are separated into `blob_*.db` files before entering the `MemTable`, each large blob consumes
//! only 23 bytes inside the `MemTable`.

use std::collections::BTreeMap;
use std::collections::btree_map;
use std::io;
use std::ops::Bound;
use std::sync::Arc;

use crate::index::{IdxSegmentBuilder, IdxSegmentReader, IndexEntry, IndexValue};
use crate::pal::{PlatformFile, PlatformFs};
use crate::wal::{BlobPtr, WalBatch, WalRecord};

/// Default maximum heap budget for the active `MemTable` before triggering a flush (4 MB).
pub const DEFAULT_MEMTABLE_CAPACITY_BYTES: usize = 4 * 1024 * 1024;

const BTREE_ENTRY_OVERHEAD_BYTES: usize = 48;

/// A chunk that grows past this many entries is split in two. Chunks only grow (deletes are
/// tombstones), so every chunk holds between `MAX_CHUNK_ENTRIES / 2` and `MAX_CHUNK_ENTRIES`
/// entries, except possibly the first one.
const MAX_CHUNK_ENTRIES: usize = 512;

type EntryMap = BTreeMap<Vec<u8>, MemEntry>;

/// One copy-on-write piece of the key space: all keys `k` with `min <= k < next.min`.
#[derive(Debug, Clone)]
struct Chunk {
    /// Routing lower bound. Fixed at creation; the first chunk uses the empty key so that
    /// every key has exactly one chunk.
    min: Arc<[u8]>,
    map: Arc<EntryMap>,
}

fn empty_chunks() -> Arc<Vec<Chunk>> {
    Arc::new(vec![Chunk {
        min: Arc::from(&[][..]),
        map: Arc::new(BTreeMap::new()),
    }])
}

/// Index of the chunk responsible for `key`.
#[inline]
fn chunk_index(chunks: &[Chunk], key: &[u8]) -> usize {
    // `chunks[0].min` is empty and therefore `<= key`, so the partition point is at least 1.
    chunks.partition_point(|c| *c.min <= *key) - 1
}

/// Splits `chunks[i]` (which the caller has just made unique) until no piece exceeds
/// `MAX_CHUNK_ENTRIES`. Only positions after `i` change.
fn split_if_large(chunks: &mut Vec<Chunk>, i: usize) {
    let len = chunks[i].map.len();
    if len <= MAX_CHUNK_ENTRIES {
        return;
    }
    if len <= MAX_CHUNK_ENTRIES * 2 {
        let Some(mid) = chunks[i].map.keys().nth(len / 2).cloned() else {
            return;
        };
        let right = Arc::make_mut(&mut chunks[i].map).split_off(mid.as_slice());
        chunks.insert(
            i + 1,
            Chunk {
                min: Arc::from(mid),
                map: Arc::new(right),
            },
        );
        return;
    }

    let pieces = len.div_ceil(MAX_CHUNK_ENTRIES);
    let base = len / pieces;
    let rem = len % pieces;
    let map = std::mem::take(Arc::make_mut(&mut chunks[i].map));
    let mut iter = map.into_iter();

    let first_count = base + usize::from(0 < rem);
    let mut first_map = BTreeMap::new();
    for _ in 0..first_count {
        if let Some((k, v)) = iter.next() {
            first_map.insert(k, v);
        }
    }
    chunks[i].map = Arc::new(first_map);

    let mut extra = Vec::with_capacity(pieces - 1);
    for p in 1..pieces {
        let count = base + usize::from(p < rem);
        let mut piece_map = BTreeMap::new();
        let mut piece_min: Option<Arc<[u8]>> = None;
        for _ in 0..count {
            let Some((k, v)) = iter.next() else {
                break;
            };
            if piece_min.is_none() {
                piece_min = Some(Arc::from(k.as_slice()));
            }
            piece_map.insert(k, v);
        }
        if let Some(min) = piece_min {
            extra.push(Chunk {
                min,
                map: Arc::new(piece_map),
            });
        }
    }
    chunks.splice(i + 1..i + 1, extra);
}

/// Value and mutation sequence number stored for a key inside [`MemTable`].
#[derive(Debug, PartialEq, Eq)]
#[cfg_attr(not(test), derive(Clone))]
pub struct MemEntry {
    pub lsn: u64,
    pub value: IndexValue,
}

#[cfg(test)]
thread_local! {
    /// Number of `MemEntry` deep copies made on this thread; lets tests prove that a write
    /// after a snapshot does not copy the whole table.
    static MEM_ENTRY_CLONES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
impl Clone for MemEntry {
    fn clone(&self) -> Self {
        MEM_ENTRY_CLONES.with(|c| c.set(c.get() + 1));
        Self {
            lsn: self.lsn,
            value: self.value.clone(),
        }
    }
}

/// Returns the number of `MemEntry` clones performed on the current thread so far.
#[cfg(test)]
pub(crate) fn mem_entry_clones() -> usize {
    MEM_ENTRY_CLONES.with(|c| c.get())
}

impl MemEntry {
    #[inline]
    fn approx_heap_bytes(&self, key_len: usize) -> usize {
        BTREE_ENTRY_OVERHEAD_BYTES + key_len + self.value.encoded_payload_len()
    }
}

/// Ordered in-memory mutation table with strict byte-size tracking.
///
/// Entries live in a sorted directory of chunks of at most `MAX_CHUNK_ENTRIES` entries. A
/// clone (read snapshot) shares the directory and, through it, every chunk. The first write
/// after a clone copies the directory (one pair of reference-count increments per chunk) and
/// then only the single chunk it touches, so a write never copies more than
/// `O(table / MAX_CHUNK_ENTRIES + MAX_CHUNK_ENTRIES)` and a snapshot retains at most one stale
/// chunk per chunk written since it was taken.
#[derive(Debug, Clone)]
pub struct MemTable {
    chunks: Arc<Vec<Chunk>>,
    /// Number of distinct keys. Entries are never removed (deletes are tombstones).
    len: usize,
    approximate_bytes: usize,
    max_capacity_bytes: usize,
    highest_lsn: u64,
    /// Bumped on every insertion of committed data; deliberately not by `clear`, which only
    /// moves the same logical content into a segment. Clones keep the value they were taken at.
    generation: u64,
}

impl MemTable {
    pub fn new(max_capacity_bytes: usize) -> Self {
        Self {
            chunks: empty_chunks(),
            len: 0,
            approximate_bytes: 0,
            max_capacity_bytes,
            highest_lsn: 0,
            generation: 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn approximate_bytes(&self) -> usize {
        self.approximate_bytes
    }

    pub fn should_flush(&self) -> bool {
        !self.is_empty() && self.approximate_bytes >= self.max_capacity_bytes
    }

    pub fn highest_lsn(&self) -> u64 {
        self.highest_lsn
    }

    /// Monotonic counter of committed-data publications into this table (every insert,
    /// delete, WAL apply and overlay absorb). Unlike LSNs it cannot be hidden by a flush.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Moves every entry of a committed transaction overlay into this `MemTable` without
    /// cloning keys or values. Each entry keeps the LSN it was assigned at mutation time and
    /// replaces whatever the `MemTable` held for that key.
    pub fn absorb(&mut self, mut entries: BTreeMap<Vec<u8>, MemEntry>) {
        if entries.is_empty() {
            return;
        }
        self.generation += 1;
        // Bulk path: when the overlay is not tiny relative to the table, hand each chunk its
        // slice of the overlay and move entries directly (empty chunks take ownership with zero
        // lookups; populated chunks update via a single per-key insert) instead of double scans.
        if entries.len().saturating_mul(8) >= self.len {
            let chunks = Arc::make_mut(&mut self.chunks);
            // Walk the chunks from the back so that splits never shift a pending index.
            for i in (0..chunks.len()).rev() {
                if entries.is_empty() {
                    break;
                }
                let part = if i == 0 {
                    std::mem::take(&mut entries)
                } else {
                    entries.split_off(&*chunks[i].min)
                };
                if part.is_empty() {
                    continue;
                }
                if chunks[i].map.is_empty() {
                    for (k, e) in &part {
                        self.approximate_bytes = self
                            .approximate_bytes
                            .saturating_add(e.approx_heap_bytes(k.len()));
                        if e.lsn > self.highest_lsn {
                            self.highest_lsn = e.lsn;
                        }
                    }
                    self.len += part.len();
                    chunks[i].map = Arc::new(part);
                } else {
                    let dst = Arc::make_mut(&mut chunks[i].map);
                    for (k, e) in part {
                        let k_len = k.len();
                        let new_bytes = e.approx_heap_bytes(k_len);
                        if e.lsn > self.highest_lsn {
                            self.highest_lsn = e.lsn;
                        }
                        match dst.insert(k, e) {
                            Some(old) => {
                                self.approximate_bytes = self
                                    .approximate_bytes
                                    .saturating_sub(old.approx_heap_bytes(k_len))
                                    .saturating_add(new_bytes);
                            }
                            None => {
                                self.len += 1;
                                self.approximate_bytes =
                                    self.approximate_bytes.saturating_add(new_bytes);
                            }
                        }
                    }
                }
                split_if_large(chunks, i);
            }
            return;
        }
        for (key, entry) in entries {
            self.upsert(key, entry.value, entry.lsn);
        }
    }

    /// Inserts or updates a key with an inline value (`< INLINE_THRESHOLD`), returning the displaced [`MemEntry`].
    pub fn put_inline(&mut self, key: Vec<u8>, value: Vec<u8>, lsn: u64) -> Option<MemEntry> {
        self.upsert(key, IndexValue::Inline(value), lsn)
    }

    /// Inserts or updates a key with a 23-byte out-of-line [`BlobPtr`], returning the displaced [`MemEntry`].
    pub fn put_blob_ptr(&mut self, key: Vec<u8>, blob_ptr: BlobPtr, lsn: u64) -> Option<MemEntry> {
        self.upsert(key, IndexValue::Blob(blob_ptr), lsn)
    }

    /// Marks `key` as deleted with a tombstone at `lsn`, returning the displaced [`MemEntry`].
    pub fn delete(&mut self, key: Vec<u8>, lsn: u64) -> Option<MemEntry> {
        self.upsert(key, IndexValue::Tombstone, lsn)
    }

    /// Applies all records in a [`WalBatch`] into this `MemTable`.
    pub fn apply_wal_batch(&mut self, batch: &WalBatch) {
        for rec in &batch.records {
            match rec {
                WalRecord::PutInline { key, value } => {
                    self.put_inline(key.clone(), value.clone(), batch.lsn);
                }
                WalRecord::PutBlobPtr { key, blob_ptr } => {
                    self.put_blob_ptr(key.clone(), *blob_ptr, batch.lsn);
                }
                WalRecord::Delete { key } => {
                    self.delete(key.clone(), batch.lsn);
                }
                WalRecord::TxBegin | WalRecord::TxCommit | WalRecord::TxContinue => {}
            }
        }
    }

    /// Consumes a [`WalBatch`] and moves its `key` and `value` vectors directly into this
    /// `MemTable` without cloning!
    pub fn apply_wal_batch_owned(&mut self, batch: WalBatch) {
        let lsn = batch.lsn;
        for rec in batch.records {
            let _ = self.apply_single_wal_record(lsn, rec);
        }
    }

    /// Applies a single [`WalRecord`] at `lsn` into this `MemTable` without cloning,
    /// returning the displaced previous [`MemEntry`] if any.
    #[inline]
    pub fn apply_single_wal_record(&mut self, lsn: u64, rec: WalRecord) -> Option<MemEntry> {
        match rec {
            WalRecord::PutInline { key, value } => self.put_inline(key, value, lsn),
            WalRecord::PutBlobPtr { key, blob_ptr } => self.put_blob_ptr(key, blob_ptr, lsn),
            WalRecord::Delete { key } => self.delete(key, lsn),
            WalRecord::TxBegin | WalRecord::TxCommit | WalRecord::TxContinue => None,
        }
    }

    /// Point lookup in the `MemTable`.
    pub fn get(&self, key: &[u8]) -> Option<&MemEntry> {
        self.chunks[chunk_index(&self.chunks, key)].map.get(key)
    }

    /// Scans all entries in the `MemTable` whose key starts with `prefix`.
    pub fn scan_prefix(&self, prefix: &[u8]) -> Vec<IndexEntry> {
        self.scan_prefix_from(prefix, prefix, usize::MAX)
    }

    /// Scans up to `limit` entries in the `MemTable` starting at `seek_key` whose key starts with `prefix`.
    pub fn scan_prefix_from(
        &self,
        prefix: &[u8],
        seek_key: &[u8],
        limit: usize,
    ) -> Vec<IndexEntry> {
        self.scan_prefix_range(prefix, seek_key, None, limit)
    }

    /// Scans up to `limit` entries in the `MemTable` starting at `seek_key`, stopping at `upper_key_bound`.
    pub fn scan_prefix_range(
        &self,
        prefix: &[u8],
        seek_key: &[u8],
        upper_key_bound: Option<(&[u8], bool)>,
        limit: usize,
    ) -> Vec<IndexEntry> {
        let mut out = Vec::new();
        if limit == 0 {
            return out;
        }
        let start = if seek_key > prefix { seek_key } else { prefix };
        if let Some((ub, inclusive)) = upper_key_bound {
            if start > ub || (!inclusive && start == ub) {
                return out;
            }
        }
        let upper = match upper_key_bound {
            Some((ub, true)) => Bound::Included(ub),
            Some((ub, false)) => Bound::Excluded(ub),
            None => Bound::Unbounded,
        };
        for (k, entry) in self.iter_range(Bound::Included(start), upper) {
            if !k.starts_with(prefix) {
                break;
            }
            out.push(IndexEntry {
                key: k.clone(),
                lsn: entry.lsn,
                value: entry.value.clone(),
            });
            if out.len() >= limit {
                break;
            }
        }
        out
    }

    /// Zero-allocation range iterator over `(key, MemEntry)` in the `MemTable`.
    #[inline]
    pub fn iter_range<'a>(
        &'a self,
        lower: Bound<&'a [u8]>,
        upper: Bound<&'a [u8]>,
    ) -> MemTableRange<'a> {
        let empty = match (lower, upper) {
            (Bound::Included(lo), Bound::Included(hi)) => lo > hi,
            (Bound::Included(lo), Bound::Excluded(hi))
            | (Bound::Excluded(lo), Bound::Included(hi))
            | (Bound::Excluded(lo), Bound::Excluded(hi)) => lo >= hi,
            _ => false,
        };
        let chunks = if empty {
            &[]
        } else {
            let first = match lower {
                Bound::Included(k) | Bound::Excluded(k) => chunk_index(&self.chunks, k),
                Bound::Unbounded => 0,
            };
            let last = match upper {
                Bound::Included(k) | Bound::Excluded(k) => chunk_index(&self.chunks, k),
                Bound::Unbounded => self.chunks.len() - 1,
            };
            if first <= last {
                &self.chunks[first..=last]
            } else {
                &[]
            }
        };
        MemTableRange {
            chunks,
            lower,
            upper,
            front: None,
            back: None,
            next_front: 0,
            next_back: chunks.len(),
        }
    }

    /// Writes the sorted contents of this `MemTable` to a new immutable `idx_<seq_id>.db`
    /// segment on disk **without** clearing `self.map` or `self.approximate_bytes`.
    pub fn write_to_segment<F: PlatformFile>(
        &self,
        fs: &impl PlatformFs<File = F>,
        seq_id: u64,
        bloom_bits_per_key: usize,
    ) -> io::Result<IdxSegmentReader<F>> {
        let mut builder = IdxSegmentBuilder::create(fs, seq_id, 0, bloom_bits_per_key)?;
        builder.set_cap_bloom_to_meta_budget(true);
        for (k, entry) in self.chunks.iter().flat_map(|c| c.map.iter()) {
            match &entry.value {
                IndexValue::Inline(val) => {
                    builder.add_raw_entry(k, entry.lsn, 1, val)?;
                }
                IndexValue::Blob(ptr) => {
                    let blob_buf = ptr.to_bytes();
                    builder.add_raw_entry(k, entry.lsn, 2, &blob_buf)?;
                }
                IndexValue::Tombstone => {
                    builder.add_raw_entry(k, entry.lsn, 3, &[])?;
                }
            }
        }
        builder.finish()
    }

    /// Clears all entries from the `MemTable` after its segment has been durably committed to `boot_meta.db`.
    pub fn clear(&mut self) {
        self.chunks = empty_chunks();
        self.len = 0;
        self.approximate_bytes = 0;
    }

    /// Flushes the sorted contents of this `MemTable` to a new immutable `idx_<seq_id>.db`
    /// segment on disk and clears the `MemTable`.
    pub fn flush_to_segment<F: PlatformFile>(
        &mut self,
        fs: &impl PlatformFs<File = F>,
        seq_id: u64,
        bloom_bits_per_key: usize,
    ) -> io::Result<IdxSegmentReader<F>> {
        let reader = self.write_to_segment(fs, seq_id, bloom_bits_per_key)?;
        self.clear();
        Ok(reader)
    }

    fn upsert(&mut self, key: Vec<u8>, value: IndexValue, lsn: u64) -> Option<MemEntry> {
        self.generation += 1;
        if lsn > self.highest_lsn {
            self.highest_lsn = lsn;
        }
        let klen = key.len();
        let new_entry = MemEntry { lsn, value };
        let new_cost = new_entry.approx_heap_bytes(klen);

        let chunks = Arc::make_mut(&mut self.chunks);
        let i = chunk_index(chunks, &key);
        let prev = Arc::make_mut(&mut chunks[i].map).insert(key, new_entry);
        if prev.is_none() {
            self.len += 1;
            split_if_large(chunks, i);
        }
        if let Some(ref old_entry) = prev {
            let old_cost = old_entry.approx_heap_bytes(klen);
            self.approximate_bytes = self
                .approximate_bytes
                .saturating_sub(old_cost)
                .saturating_add(new_cost);
        } else {
            self.approximate_bytes = self.approximate_bytes.saturating_add(new_cost);
        }
        prev
    }
}

/// Double-ended iterator over a key range of a [`MemTable`], in key order.
pub struct MemTableRange<'a> {
    /// Chunks that may hold keys of the range, in key order.
    chunks: &'a [Chunk],
    lower: Bound<&'a [u8]>,
    upper: Bound<&'a [u8]>,
    front: Option<btree_map::Range<'a, Vec<u8>, MemEntry>>,
    back: Option<btree_map::Range<'a, Vec<u8>, MemEntry>>,
    /// Chunks `[next_front, next_back)` have not been opened yet.
    next_front: usize,
    next_back: usize,
}

impl<'a> MemTableRange<'a> {
    #[inline]
    fn open(&self, chunk: usize) -> btree_map::Range<'a, Vec<u8>, MemEntry> {
        self.chunks[chunk]
            .map
            .range::<[u8], _>((self.lower, self.upper))
    }
}

impl<'a> Iterator for MemTableRange<'a> {
    type Item = (&'a Vec<u8>, &'a MemEntry);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(range) = self.front.as_mut() {
                if let Some(item) = range.next() {
                    return Some(item);
                }
                self.front = None;
            }
            if self.next_front < self.next_back {
                self.front = Some(self.open(self.next_front));
                self.next_front += 1;
            } else {
                return self.back.as_mut()?.next();
            }
        }
    }
}

impl DoubleEndedIterator for MemTableRange<'_> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(range) = self.back.as_mut() {
                if let Some(item) = range.next_back() {
                    return Some(item);
                }
                self.back = None;
            }
            if self.next_front < self.next_back {
                self.next_back -= 1;
                self.back = Some(self.open(self.next_back));
            } else {
                return self.front.as_mut()?.next_back();
            }
        }
    }
}

#[cfg(test)]
mod tests;
