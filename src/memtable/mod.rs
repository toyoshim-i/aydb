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
    split_if_large(chunks, i + 1);
    split_if_large(chunks, i);
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

impl MemEntry {
    #[inline]
    fn approx_heap_bytes(&self, key_len: usize) -> usize {
        BTREE_ENTRY_OVERHEAD_BYTES + key_len + self.value.encoded_payload_len()
    }
}

/// Ordered in-memory mutation table with strict byte-size tracking.
///
/// Entries live in a sorted directory of chunks of at most [`MAX_CHUNK_ENTRIES`] entries. A
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
        self.approximate_bytes >= self.max_capacity_bytes
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
        // slice of the overlay and merge with `BTreeMap::append` (overlay wins on equal keys)
        // instead of m separate O(log n) inserts. Only the touched chunks are made unique.
        if entries.len().saturating_mul(8) >= self.len {
            let chunks = Arc::make_mut(&mut self.chunks);
            // Walk the chunks from the back so that splits never shift a pending index.
            for i in (0..chunks.len()).rev() {
                if entries.is_empty() {
                    break;
                }
                let mut part = if i == 0 {
                    std::mem::take(&mut entries)
                } else {
                    entries.split_off(&*chunks[i].min)
                };
                if part.is_empty() {
                    continue;
                }
                let mut replaced = 0usize;
                for (k, e) in &part {
                    if let Some(old) = chunks[i].map.get(k) {
                        replaced += 1;
                        self.approximate_bytes = self
                            .approximate_bytes
                            .saturating_sub(old.approx_heap_bytes(k.len()));
                    }
                    self.approximate_bytes = self
                        .approximate_bytes
                        .saturating_add(e.approx_heap_bytes(k.len()));
                    if e.lsn > self.highest_lsn {
                        self.highest_lsn = e.lsn;
                    }
                }
                self.len += part.len() - replaced;
                Arc::make_mut(&mut chunks[i].map).append(&mut part);
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
                WalRecord::TxBegin | WalRecord::TxCommit => {}
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
            WalRecord::TxBegin | WalRecord::TxCommit => None,
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
        let first = match lower {
            Bound::Included(k) | Bound::Excluded(k) => chunk_index(&self.chunks, k),
            Bound::Unbounded => 0,
        };
        let last = match upper {
            Bound::Included(k) | Bound::Excluded(k) => chunk_index(&self.chunks, k),
            Bound::Unbounded => self.chunks.len() - 1,
        };
        let chunks = if first <= last {
            &self.chunks[first..=last]
        } else {
            &[]
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
        for (k, entry) in self.chunks.iter().flat_map(|c| c.map.iter()) {
            match &entry.value {
                IndexValue::Inline(val) => {
                    builder.add_raw_entry(k, entry.lsn, 1, val)?;
                }
                IndexValue::Blob(ptr) => {
                    let mut blob_buf = [0u8; crate::index::BLOB_PTR_ENCODED_LEN];
                    blob_buf[0] = ptr.kind as u8;
                    blob_buf[1..3].copy_from_slice(&ptr.table_id.to_le_bytes());
                    blob_buf[3..11].copy_from_slice(&ptr.seg_id.to_le_bytes());
                    blob_buf[11..15].copy_from_slice(&ptr.offset.to_le_bytes());
                    blob_buf[15..19].copy_from_slice(&ptr.len.to_le_bytes());
                    blob_buf[19..23].copy_from_slice(&ptr.crc32c.to_le_bytes());
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
mod tests {
    use super::*;
    use crate::pal::OsFs;
    use crate::util::test_util::TempTestDir;

    #[test]
    fn cloned_memtables_preserve_entries_and_accounting_after_mutations() {
        let mut original = MemTable::new(4096);
        original.put_inline(b"a".to_vec(), b"original".to_vec(), 1);
        original.put_inline(b"b".to_vec(), b"retained".to_vec(), 2);
        let snapshot = original.clone();
        assert!(Arc::ptr_eq(&original.chunks, &snapshot.chunks));
        let snapshot_bytes = snapshot.approximate_bytes();

        original.put_inline(b"a".to_vec(), b"changed".to_vec(), 3);
        original.delete(b"b".to_vec(), 4);
        let after_upsert = original.clone();
        let after_upsert_bytes = original.approximate_bytes();
        original.absorb(BTreeMap::from([
            (
                b"a".to_vec(),
                MemEntry {
                    lsn: 5,
                    value: IndexValue::Inline(b"merged".to_vec()),
                },
            ),
            (
                b"c".to_vec(),
                MemEntry {
                    lsn: 6,
                    value: IndexValue::Inline(b"added".to_vec()),
                },
            ),
        ]));
        let before_clear = original.clone();
        original.clear();

        assert!(original.is_empty());
        assert_eq!(original.approximate_bytes(), 0);
        assert_eq!(
            snapshot.get(b"a").unwrap().value,
            IndexValue::Inline(b"original".to_vec())
        );
        assert_eq!(
            snapshot.get(b"b").unwrap().value,
            IndexValue::Inline(b"retained".to_vec())
        );
        assert_eq!(snapshot.approximate_bytes(), snapshot_bytes);
        assert_eq!(snapshot.highest_lsn(), 2);
        assert_eq!(
            after_upsert.get(b"a").unwrap().value,
            IndexValue::Inline(b"changed".to_vec())
        );
        assert_eq!(after_upsert.get(b"b").unwrap().value, IndexValue::Tombstone);
        assert_eq!(after_upsert.approximate_bytes(), after_upsert_bytes);
        assert_eq!(
            before_clear.get(b"a").unwrap().value,
            IndexValue::Inline(b"merged".to_vec())
        );
        assert_eq!(before_clear.len(), 3);
        assert_eq!(before_clear.highest_lsn(), 6);
    }

    struct TestRng(u64);

    impl TestRng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }

        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    /// Plain-`BTreeMap` model of a `MemTable`.
    #[derive(Clone, Default)]
    struct Model {
        map: BTreeMap<Vec<u8>, MemEntry>,
        bytes: usize,
        highest_lsn: u64,
        generation: u64,
    }

    impl Model {
        fn put(&mut self, key: Vec<u8>, entry: MemEntry) -> Option<MemEntry> {
            self.generation += 1;
            self.highest_lsn = self.highest_lsn.max(entry.lsn);
            let new_cost = entry.approx_heap_bytes(key.len());
            let prev = self.map.insert(key.clone(), entry);
            if let Some(old) = &prev {
                self.bytes -= old.approx_heap_bytes(key.len());
            }
            self.bytes += new_cost;
            prev
        }
    }

    fn model_key(rng: &mut TestRng) -> Vec<u8> {
        match rng.below(40) {
            0 => Vec::new(),
            1 => vec![0xFF, 0xFF],
            2..=5 => format!("k{:03}", rng.below(300)).into_bytes(),
            _ => format!("k{:05}", rng.below(5000)).into_bytes(),
        }
    }

    fn model_entry(rng: &mut TestRng, lsn: u64) -> MemEntry {
        let value = match rng.below(4) {
            0 => IndexValue::Tombstone,
            1 => IndexValue::Blob(BlobPtr {
                kind: crate::wal::BlobKind::Standard,
                table_id: 0,
                seg_id: rng.below(100),
                offset: 0,
                len: 5000,
                crc32c: 0,
            }),
            _ => IndexValue::Inline(vec![b'v'; rng.below(40) as usize]),
        };
        MemEntry { lsn, value }
    }

    fn assert_matches_model(mt: &MemTable, model: &Model, rng: &mut TestRng) {
        assert_eq!(mt.len(), model.map.len());
        assert_eq!(mt.is_empty(), model.map.is_empty());
        assert_eq!(mt.approximate_bytes(), model.bytes);
        assert_eq!(mt.highest_lsn(), model.highest_lsn);
        assert_eq!(mt.generation(), model.generation);

        let all = Bound::<&[u8]>::Unbounded;
        let forward: Vec<_> = mt.iter_range(all, all).collect();
        assert!(forward.iter().map(|(k, e)| (*k, *e)).eq(model.map.iter()));
        let backward: Vec<_> = mt.iter_range(all, all).rev().collect();
        assert!(
            backward
                .iter()
                .map(|(k, e)| (*k, *e))
                .eq(model.map.iter().rev())
        );

        // Point reads, hits and misses.
        for _ in 0..40 {
            let key = model_key(rng);
            assert_eq!(mt.get(&key), model.map.get(&key));
        }
        for (k, e) in model.map.iter().step_by(97) {
            assert_eq!(mt.get(k), Some(e));
        }

        // Bounded ranges, consumed from both ends in a random order.
        for _ in 0..30 {
            let (a, b) = (model_key(rng), model_key(rng));
            let (lo_key, hi_key) = if a <= b { (a, b) } else { (b, a) };
            let lo = match rng.below(3) {
                0 => Bound::Unbounded,
                1 => Bound::Included(lo_key.as_slice()),
                _ => Bound::Excluded(lo_key.as_slice()),
            };
            let hi = match rng.below(3) {
                0 => Bound::Unbounded,
                1 => Bound::Included(hi_key.as_slice()),
                _ if lo_key == hi_key && matches!(lo, Bound::Excluded(_)) => Bound::Unbounded,
                _ => Bound::Excluded(hi_key.as_slice()),
            };
            if lo_key == hi_key && matches!((lo, hi), (Bound::Excluded(_), Bound::Excluded(_))) {
                continue;
            }
            let mut got = mt.iter_range(lo, hi);
            let mut want = model.map.range::<[u8], _>((lo, hi));
            loop {
                let (g, w) = if rng.below(2) == 0 {
                    (got.next(), want.next())
                } else {
                    (got.next_back(), want.next_back())
                };
                assert_eq!(g, w);
                if w.is_none() {
                    break;
                }
            }
        }

        // Prefix scans go through the same range machinery.
        let prefix = b"k00";
        let want: Vec<_> = model
            .map
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, e)| (k.clone(), e.lsn))
            .collect();
        let got: Vec<_> = mt
            .scan_prefix(prefix)
            .into_iter()
            .map(|e| (e.key, e.lsn))
            .collect();
        assert_eq!(got, want);
    }

    #[test]
    fn matches_btreemap_model_across_random_operations() {
        for seed in 1..=4u64 {
            let mut rng = TestRng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
            let mut mt = MemTable::new(usize::MAX);
            let mut model = Model::default();
            let mut snapshots: Vec<(MemTable, Model)> = Vec::new();
            let mut lsn = 0u64;
            for step in 0..6000 {
                match rng.below(100) {
                    0..=59 => {
                        lsn += 1;
                        let key = model_key(&mut rng);
                        let entry = model_entry(&mut rng, lsn);
                        let prev_model = model.put(key.clone(), entry.clone());
                        let prev = match entry.value {
                            IndexValue::Inline(v) => mt.put_inline(key, v, lsn),
                            IndexValue::Blob(p) => mt.put_blob_ptr(key, p, lsn),
                            IndexValue::Tombstone => mt.delete(key, lsn),
                        };
                        assert_eq!(prev, prev_model);
                    }
                    60..=79 => {
                        // Alternate between tiny overlays (per-key path) and large ones
                        // (bulk path, possibly spanning and splitting many chunks).
                        let count = if rng.below(4) == 0 {
                            rng.below(1500)
                        } else {
                            rng.below(4)
                        };
                        let mut overlay = BTreeMap::new();
                        for _ in 0..count {
                            lsn += 1;
                            overlay.insert(model_key(&mut rng), model_entry(&mut rng, lsn));
                        }
                        let generation_before = mt.generation();
                        let published = !overlay.is_empty();
                        for (k, e) in &overlay {
                            model.put(k.clone(), e.clone());
                        }
                        mt.absorb(overlay);
                        // Every non-empty publication bumps the generation (the per-key path
                        // may bump it more than once); an empty one must not.
                        assert_eq!(mt.generation() > generation_before, published);
                        model.generation = mt.generation();
                    }
                    80..=87 => {
                        if snapshots.len() < 8 {
                            snapshots.push((mt.clone(), model.clone()));
                        } else {
                            snapshots.remove(rng.below(8) as usize);
                        }
                    }
                    88..=89 => {
                        // Clearing keeps the generation, like a flush that moved the content.
                        mt.clear();
                        model.map.clear();
                        model.bytes = 0;
                    }
                    _ => {}
                }
                if step % 500 == 499 {
                    assert_matches_model(&mt, &model, &mut rng);
                    for (snap, snap_model) in &snapshots {
                        assert_matches_model(snap, snap_model, &mut rng);
                    }
                }
            }
            assert_matches_model(&mt, &model, &mut rng);
            for (snap, snap_model) in &snapshots {
                assert_matches_model(snap, snap_model, &mut rng);
            }
        }
    }

    #[test]
    fn chunks_stay_bounded_and_ordered_for_sequential_and_bulk_loads() {
        let key = |i: usize| format!("key{i:08}").into_bytes();
        let entry = |i: usize| MemEntry {
            lsn: i as u64 + 1,
            value: IndexValue::Inline(vec![1; 8]),
        };
        let mut incremental = MemTable::new(usize::MAX);
        let mut bulk = MemTable::new(usize::MAX);
        let total = 10_000;
        for i in 0..total {
            incremental.put_inline(key(i), vec![1; 8], i as u64 + 1);
        }
        bulk.absorb((0..total).map(|i| (key(i), entry(i))).collect());
        for mt in [&incremental, &bulk] {
            assert_eq!(mt.len(), total);
            assert!(mt.chunks.len() >= total / MAX_CHUNK_ENTRIES);
            assert!(mt.chunks.iter().all(|c| c.map.len() <= MAX_CHUNK_ENTRIES));
            assert!(mt.chunks.windows(2).all(|w| w[0].min < w[1].min));
            assert!(mt.chunks[0].min.is_empty());
            for c in mt.chunks.iter() {
                assert!(c.map.keys().all(|k| k.as_slice() >= &*c.min));
            }
            assert_eq!(mt.get(&key(4321)), Some(&entry(4321)));
        }
    }

    fn entry_clones() -> usize {
        MEM_ENTRY_CLONES.with(|c| c.get())
    }

    #[test]
    fn write_after_snapshot_does_not_copy_the_whole_table() {
        let total = 20_000usize;
        let mut mt = MemTable::new(usize::MAX);
        for i in 0..total {
            mt.put_inline(
                format!("key{i:08}").into_bytes(),
                vec![7u8; 16],
                i as u64 + 1,
            );
        }
        let snapshot = mt.clone();

        // Updates, inserts, deletes and a small absorb while the snapshot is alive.
        let before = entry_clones();
        mt.put_inline(b"key00010000".to_vec(), b"changed".to_vec(), 50_000);
        let first_write = entry_clones() - before;
        mt.put_inline(b"zzz-new".to_vec(), b"new".to_vec(), 50_001);
        mt.delete(b"key00000007".to_vec(), 50_002);
        mt.absorb(BTreeMap::from([(
            b"key00015000".to_vec(),
            MemEntry {
                lsn: 50_003,
                value: IndexValue::Inline(b"abs".to_vec()),
            },
        )]));
        let copied = entry_clones() - before;

        assert!(
            first_write <= 1024,
            "first write after a snapshot copied {first_write} of {total} entries"
        );
        assert!(
            copied <= 4 * 1024,
            "writes after a snapshot copied {copied} of {total} entries"
        );
        // The snapshot is untouched.
        assert_eq!(
            snapshot.get(b"key00010000").unwrap().value,
            IndexValue::Inline(vec![7u8; 16])
        );
        assert!(snapshot.get(b"zzz-new").is_none());
        assert_eq!(snapshot.len(), total);
        assert_eq!(mt.len(), total + 1);
    }

    #[test]
    fn single_pass_upsert_zero_copy_cursors_and_256kb_coalesced_segment_builder() {
        let tmp = TempTestDir::new("memtable_idx");
        let fs = OsFs::open(&tmp.path, true).expect("open OsFs");

        // Verify single-pass `put_inline`, `delete`, and `apply_single_wal_record`
        // return the displaced `Option<MemEntry>` directly without a separate lookup.
        let mut mt = MemTable::new(16 * 1024 * 1024);
        assert_eq!(mt.put_inline(b"k1".to_vec(), b"v1".to_vec(), 10), None);
        assert_eq!(
            mt.put_inline(b"k1".to_vec(), b"v2".to_vec(), 11),
            Some(MemEntry {
                lsn: 10,
                value: IndexValue::Inline(b"v1".to_vec()),
            })
        );
        assert_eq!(
            mt.delete(b"k1".to_vec(), 12),
            Some(MemEntry {
                lsn: 11,
                value: IndexValue::Inline(b"v2".to_vec()),
            })
        );
        assert_eq!(
            mt.apply_single_wal_record(
                13,
                WalRecord::PutInline {
                    key: b"k1".to_vec(),
                    value: b"v3".to_vec(),
                },
            ),
            Some(MemEntry {
                lsn: 12,
                value: IndexValue::Tombstone,
            })
        );

        // Populate > 80 blocks (> 320 KB, crossing the 256 KB VACB write coalescing boundary)
        // and flush to an `idx_<SeqID>.db` segment.
        let mut big_mt = MemTable::new(16 * 1024 * 1024);
        let payload = vec![0x5Au8; 256];
        let total_keys = 1200u32; // 1200 * ~280 B > 336 KB (> 82 blocks of 4 KB)
        for i in 0..total_keys {
            let key = format!("coalesce_key_{i:06}").into_bytes();
            big_mt.put_inline(key, payload.clone(), (i + 1) as u64);
        }
        let seg = big_mt
            .flush_to_segment(&fs, 10, 10)
            .expect("flush > 256 KB segment");
        let num_blocks = seg.num_blocks();
        assert!(
            num_blocks > 64,
            "segment must exceed 64 blocks (256 KB) to exercise multi-chunk write coalescing"
        );
        assert_eq!(seg.record_count(), total_keys);
        drop(seg);

        // Reopen from disk to verify all coalesced 256 KB + tail blocks and metadata trailer
        let reopened = IdxSegmentReader::open(&fs, 10).expect("reopen coalesced segment");
        assert_eq!(reopened.num_blocks(), num_blocks);
        assert_eq!(reopened.record_count(), total_keys);

        // Verify zero-copy forward and reverse cursors (`current_key()` and `peek_raw()`)
        let mut fwd = reopened.cursor_all().expect("forward cursor");
        let mut count_fwd = 0u32;
        while let Some(raw) = fwd.peek_raw() {
            let expected_key = format!("coalesce_key_{count_fwd:06}");
            assert_eq!(raw.key, expected_key.as_bytes());
            assert_eq!(fwd.current_key(), Some(expected_key.as_bytes()));
            assert_eq!(raw.lsn, (count_fwd + 1) as u64);
            assert_eq!(raw.val_type, 1);
            assert_eq!(raw.val_bytes, payload.as_slice());
            count_fwd += 1;
            fwd.advance().unwrap();
        }
        assert_eq!(count_fwd, total_keys);

        let mut rev = reopened.cursor_reverse_from(None).expect("reverse cursor");
        let mut count_rev = total_keys;
        while let Some(raw) = rev.peek_raw() {
            count_rev -= 1;
            let expected_key = format!("coalesce_key_{count_rev:06}");
            assert_eq!(raw.key, expected_key.as_bytes());
            assert_eq!(rev.current_key(), Some(expected_key.as_bytes()));
            assert_eq!(raw.lsn, (count_rev + 1) as u64);
            assert_eq!(raw.val_type, 1);
            assert_eq!(raw.val_bytes, payload.as_slice());
            rev.advance().unwrap();
        }
        assert_eq!(count_rev, 0);
    }
}
