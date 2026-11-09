//! Layer 4 (Part 1): Bounded In-Memory `MemTable`.
//!
//! Absorbs the latest mutations (`InlineValue`, `BlobPtr`, or `Tombstone`) in sorted key order
//! while enforcing a strict heap memory budget (e.g., 4 MB). Because large payloads (`>= 1 KB`)
//! are separated into `blob_*.db` files before entering the `MemTable`, each large blob consumes
//! only 23 bytes inside the `MemTable`.

use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;

use crate::index::{IdxSegmentBuilder, IdxSegmentReader, IndexEntry, IndexValue};
use crate::pal::{PlatformFile, PlatformFs};
use crate::wal::{BlobPtr, WalBatch, WalRecord};

/// Default maximum heap budget for the active `MemTable` before triggering a flush (4 MB).
pub const DEFAULT_MEMTABLE_CAPACITY_BYTES: usize = 4 * 1024 * 1024;

const BTREE_ENTRY_OVERHEAD_BYTES: usize = 48;

/// Value and mutation sequence number stored for a key inside [`MemTable`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemEntry {
    pub lsn: u64,
    pub value: IndexValue,
}

impl MemEntry {
    #[inline]
    fn approx_heap_bytes(&self, key_len: usize) -> usize {
        BTREE_ENTRY_OVERHEAD_BYTES + key_len + self.value.encoded_payload_len()
    }
}

/// Ordered in-memory mutation table with strict byte-size tracking.
/// Clones share immutable entries until the next mutation, allowing bounded read snapshots.
#[derive(Debug, Clone)]
pub struct MemTable {
    map: Arc<BTreeMap<Vec<u8>, MemEntry>>,
    approximate_bytes: usize,
    max_capacity_bytes: usize,
    highest_lsn: u64,
}

impl MemTable {
    pub fn new(max_capacity_bytes: usize) -> Self {
        Self {
            map: Arc::new(BTreeMap::new()),
            approximate_bytes: 0,
            max_capacity_bytes,
            highest_lsn: 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn len(&self) -> usize {
        self.map.len()
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

    /// Moves every entry of a committed transaction overlay into this `MemTable` without
    /// cloning keys or values. Each entry keeps the LSN it was assigned at mutation time and
    /// replaces whatever the `MemTable` held for that key.
    pub fn absorb(&mut self, mut entries: BTreeMap<Vec<u8>, MemEntry>) {
        if entries.is_empty() {
            return;
        }
        // Bulk path: when the overlay is not tiny relative to the table, rebuild with
        // `BTreeMap::append` (O(n + m), overlay wins on equal keys) instead of n separate
        // O(log m) inserts.
        if entries.len().saturating_mul(8) >= self.map.len() {
            for (k, e) in &entries {
                if let Some(old) = self.map.get(k) {
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
            if self.map.is_empty() {
                self.map = Arc::new(entries);
            } else {
                Arc::make_mut(&mut self.map).append(&mut entries);
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
        self.map.get(key)
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
        use std::ops::Bound;
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
        for (k, entry) in self.map.range::<[u8], _>((Bound::Included(start), upper)) {
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
        lower: std::ops::Bound<&'a [u8]>,
        upper: std::ops::Bound<&'a [u8]>,
    ) -> std::collections::btree_map::Range<'a, Vec<u8>, MemEntry> {
        self.map.range::<[u8], _>((lower, upper))
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
        for (k, entry) in self.map.iter() {
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
        if let Some(map) = Arc::get_mut(&mut self.map) {
            map.clear();
        } else {
            self.map = Arc::new(BTreeMap::new());
        }
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
        if lsn > self.highest_lsn {
            self.highest_lsn = lsn;
        }
        let klen = key.len();
        let new_entry = MemEntry { lsn, value };
        let new_cost = new_entry.approx_heap_bytes(klen);

        let prev = Arc::make_mut(&mut self.map).insert(key, new_entry);
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
        assert!(Arc::ptr_eq(&original.map, &snapshot.map));
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
