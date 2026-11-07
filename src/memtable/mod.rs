//! Layer 4 (Part 1): Bounded In-Memory `MemTable`.
//!
//! Absorbs the latest mutations (`InlineValue`, `BlobPtr`, or `Tombstone`) in sorted key order
//! while enforcing a strict heap memory budget (e.g., 4 MB). Because large payloads (`>= 1 KB`)
//! are separated into `blob_*.db` files before entering the `MemTable`, each large blob consumes
//! only 23 bytes inside the `MemTable`.

use std::collections::BTreeMap;
use std::io;

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
#[derive(Debug, Clone)]
pub struct MemTable {
    map: BTreeMap<Vec<u8>, MemEntry>,
    approximate_bytes: usize,
    max_capacity_bytes: usize,
    highest_lsn: u64,
}

impl MemTable {
    pub fn new(max_capacity_bytes: usize) -> Self {
        Self {
            map: BTreeMap::new(),
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

    pub fn set_highest_lsn(&mut self, lsn: u64) {
        self.highest_lsn = lsn;
    }

    /// Restores `key` to its pre-mutation state (`prev`) during transaction/savepoint rollback
    /// in $O(\log N)$ time while maintaining exact `approximate_bytes` accounting (`PERF-03`).
    pub fn restore_entry(&mut self, key: Vec<u8>, prev: Option<MemEntry>) {
        let klen = key.len();
        match prev {
            Some(prev_entry) => {
                let prev_cost = prev_entry.approx_heap_bytes(klen);
                if let Some(cur_entry) = self.map.insert(key, prev_entry) {
                    let cur_cost = cur_entry.approx_heap_bytes(klen);
                    self.approximate_bytes = self
                        .approximate_bytes
                        .saturating_sub(cur_cost)
                        .saturating_add(prev_cost);
                } else {
                    self.approximate_bytes = self.approximate_bytes.saturating_add(prev_cost);
                }
            }
            None => {
                if let Some(cur_entry) = self.map.remove(&key) {
                    let cur_cost = cur_entry.approx_heap_bytes(klen);
                    self.approximate_bytes = self.approximate_bytes.saturating_sub(cur_cost);
                }
            }
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

    /// Applies a single [`WalRecord`] at `lsn` into this `MemTable` without cloning (`R-02`, `P5`),
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
        for (k, entry) in &self.map {
            builder.add_entry(&IndexEntry {
                key: k.clone(),
                lsn: entry.lsn,
                value: entry.value.clone(),
            })?;
        }
        builder.finish()
    }

    /// Clears all entries from the `MemTable` after its segment has been durably committed to `boot_meta.db`.
    pub fn clear(&mut self) {
        self.map.clear();
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

        let prev = self.map.insert(key, new_entry);
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
    use crate::blob::{BlobSegmentReader, BlobSegmentWriter};
    use crate::index::compact_idx_segments;
    use crate::pal::OsFs;
    use crate::wal::BlobKind;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static WISCKEY_TEST_COUNTER: AtomicU64 = AtomicU64::new(1);

    struct TempTestDir {
        path: PathBuf,
    }

    impl TempTestDir {
        fn new(prefix: &str) -> Self {
            let id = WISCKEY_TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
            let pid = std::process::id();
            let dir = std::env::temp_dir().join(format!("aydb_wisckey_test_{prefix}_{pid}_{id}"));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).expect("create temp test dir");
            Self { path: dir }
        }
    }

    impl Drop for TempTestDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    #[test]
    fn blob_write_verify_and_unflushed_truncation_safety() {
        let tmp = TempTestDir::new("blob");
        let fs = OsFs::open(&tmp.path, true).expect("open OsFs");

        let mut writer =
            BlobSegmentWriter::open(&fs, BlobKind::Standard, 0, 1).expect("open blob writer");
        let payload = vec![0xABu8; 8192];
        let ptr1 = writer
            .append_blob(b"http://example.com/a.png", &payload)
            .expect("append blob 1");
        writer.sync_durable().unwrap();
        drop(writer);

        let reader =
            BlobSegmentReader::open(&fs, BlobKind::Standard, 0, 1).expect("open blob reader");
        let fetched = reader
            .read_blob(&ptr1, Some(b"http://example.com/a.png"))
            .expect("read blob")
            .expect("blob must exist");
        assert_eq!(fetched, payload);

        // Wrong key check must return None
        assert!(
            reader
                .read_blob(&ptr1, Some(b"http://example.com/wrong.png"))
                .unwrap()
                .is_none()
        );

        // PERF-06: Verify `read_blob_into` reuses the caller's `out: &mut Vec<u8>` buffer
        // capacity across calls and validates CRC32C + `expected_key`!
        let mut reusable_buf = Vec::with_capacity(16384);
        let orig_cap = reusable_buf.capacity();
        assert!(
            reader
                .read_blob_into(&ptr1, Some(b"http://example.com/a.png"), &mut reusable_buf)
                .unwrap()
        );
        assert_eq!(reusable_buf, payload);
        assert_eq!(reusable_buf.capacity(), orig_cap);

        assert!(
            !reader
                .read_blob_into(
                    &ptr1,
                    Some(b"http://example.com/wrong.png"),
                    &mut reusable_buf
                )
                .unwrap()
        );
        assert!(reusable_buf.is_empty());

        let mut bad_crc_ptr = ptr1;
        bad_crc_ptr.crc32c ^= 0xDEAD_BEEF;
        assert!(
            !reader
                .read_blob_into(
                    &bad_crc_ptr,
                    Some(b"http://example.com/a.png"),
                    &mut reusable_buf
                )
                .unwrap()
        );
        assert!(reusable_buf.is_empty());

        // Unflushed / beyond-EOF pointer (simulating power loss before blob reached disk) must return Ok(None)
        let dangling_ptr = BlobPtr {
            kind: BlobKind::Standard,
            table_id: 0,
            seg_id: 1,
            offset: ptr1.len,
            len: 4096,
            crc32c: 0x1122_3344,
        };
        assert!(reader.read_blob(&dangling_ptr, None).unwrap().is_none());
        assert!(!reader.validate_blob_ptr(&dangling_ptr));
    }

    #[test]
    fn idx_segment_fence_pointers_bloom_and_compaction() {
        let tmp = TempTestDir::new("idx_compact");
        let fs = OsFs::open(&tmp.path, true).expect("open OsFs");

        // Segment 1 (Older, LSN 1..=200)
        let mut mt1 = MemTable::new(DEFAULT_MEMTABLE_CAPACITY_BYTES);
        for i in 0..200u32 {
            let key = format!("user_key_{i:05}").into_bytes();
            if i == 50 {
                // Key 50 holds a BlobPtr in Segment 1 that will be overwritten in Segment 2
                mt1.put_blob_ptr(
                    key,
                    BlobPtr {
                        kind: BlobKind::Standard,
                        table_id: 0,
                        seg_id: 10,
                        offset: 0,
                        len: 16384,
                        crc32c: 0x9999,
                    },
                    (i + 1) as u64,
                );
            } else {
                mt1.put_inline(key, format!("val_v1_{i}").into_bytes(), (i + 1) as u64);
            }
        }
        let seg1 = mt1.flush_to_segment(&fs, 1, 10).expect("flush seg 1");
        assert!(seg1.num_blocks() >= 2);

        // Reopen Segment 1 from disk to test Footer + Bloom + FencePointer deserialization
        drop(seg1);
        let seg1 = IdxSegmentReader::open(&fs, 1).expect("reopen seg 1");

        // PERF-02: Verify `[min_key, max_key]` key-range metadata & pruning on `seg1`
        assert_eq!(seg1.min_key(), b"user_key_00000");
        assert_eq!(seg1.max_key(), b"user_key_00199");
        assert!(!seg1.may_contain_key(b"aaa_before_min"));
        assert!(!seg1.may_contain_key(b"zzz_after_max"));
        assert!(seg1.may_contain_key(b"user_key_00042"));
        assert!(!seg1.overlaps_range(b"zzz_0", Some(b"zzz_9")));
        assert!(!seg1.overlaps_range(b"aaa_0", Some(b"aaa_9")));
        assert!(seg1.overlaps_range(b"user_key_00100", Some(b"user_key_00150")));

        // Point lookups on Segment 1
        assert_eq!(
            seg1.get(b"user_key_00042").unwrap(),
            Some((43, IndexValue::Inline(b"val_v1_42".to_vec())))
        );
        assert_eq!(seg1.get(b"nonexistent_key").unwrap(), None);

        // Segment 2 (Newer, LSN 500..): Overwrites key 50 and deletes key 100
        let mut mt2 = MemTable::new(DEFAULT_MEMTABLE_CAPACITY_BYTES);
        mt2.put_inline(
            b"user_key_00050".to_vec(),
            b"val_v2_50_inline".to_vec(),
            500,
        );
        mt2.delete(b"user_key_00100".to_vec(), 501);
        let seg2 = mt2.flush_to_segment(&fs, 2, 10).expect("flush seg 2");

        // Compact Segment 2 and Segment 1 into Segment 3 with drop_tombstones = true
        let comp = compact_idx_segments(&fs, &[&seg2, &seg1], 3, 1, 10, true).expect("compact");
        assert_eq!(comp.new_segment.record_count(), 199); // 200 - 1 deleted key (key 100)
        assert_eq!(
            comp.new_segment.get(b"user_key_00050").unwrap(),
            Some((500, IndexValue::Inline(b"val_v2_50_inline".to_vec())))
        );
        assert_eq!(comp.new_segment.get(b"user_key_00100").unwrap(), None);

        // Verify that overwriting key 50's BlobPtr reported 16384 newly dead blob bytes on seg_id 10!
        assert_eq!(
            comp.newly_dead_blob_bytes,
            vec![(BlobKind::Standard, 0, 10, 16384)]
        );

        // S01: Verify that a crafted index segment with an inflated `fp_count` (even with a
        // valid recomputed footer_crc32c) is rejected before allocating `fence_pointers`.
        drop(comp);
        let seg_fname = crate::meta::format_idx_file_name(3);
        let seg_file = fs
            .open_file(&seg_fname, false, true, crate::pal::OpenHint::ImmutableRandom)
            .unwrap();
        let flen = seg_file.file_len().unwrap() as usize;
        let mut raw_seg = vec![0u8; flen];
        seg_file.read_exact_at(&mut raw_seg, 0).unwrap();

        let footer_start = flen - 48;
        let meta_offset =
            u64::from_le_bytes(raw_seg[footer_start + 24..footer_start + 32].try_into().unwrap())
                as usize;
        // `meta_buf` starts with `[num_probes: u8 | reserved: [u8; 3] | bit_bytes_len: u32 LE | bits...]`,
        // followed by `fp_count: u32`.
        let bloom_byte_len =
            u32::from_le_bytes(raw_seg[meta_offset + 4..meta_offset + 8].try_into().unwrap())
                as usize;
        let fp_count_pos = meta_offset + 8 + bloom_byte_len;
        // Overwrite `fp_count` with `u32::MAX` and recompute both `meta_crc` and `footer_crc32c`
        raw_seg[fp_count_pos..fp_count_pos + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        let new_meta_crc = crate::util::crc32c::crc32c(&raw_seg[meta_offset..footer_start]);
        raw_seg[footer_start + 36..footer_start + 40].copy_from_slice(&new_meta_crc.to_le_bytes());
        let new_footer_crc =
            crate::util::crc32c::crc32c(&raw_seg[footer_start..footer_start + 44]);
        raw_seg[footer_start + 44..footer_start + 48]
            .copy_from_slice(&new_footer_crc.to_le_bytes());
        seg_file.write_all_at(&raw_seg, 0).unwrap();
        drop(seg_file);

        let err = match IdxSegmentReader::open(&fs, 3) {
            Ok(_) => panic!("S01: crafted huge fp_count with valid CRC must be rejected"),
            Err(e) => e,
        };
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn p2_p3_p5_single_pass_upsert_zero_copy_cursors_and_256kb_coalesced_segment_builder() {
        let tmp = TempTestDir::new("p2_p3_p5_idx");
        let fs = OsFs::open(&tmp.path, true).expect("open OsFs");

        // P5 Part A: Verify single-pass `put_inline`, `delete`, and `apply_single_wal_record`
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

        // P3: Populate > 80 blocks (> 320 KB, crossing the 256 KB VACB write coalescing boundary)
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

        // P2: Verify zero-copy forward and reverse cursors (`current_key()` and `peek_raw()`)
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

        let mut rev = reopened
            .cursor_reverse_from(None)
            .expect("reverse cursor");
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
