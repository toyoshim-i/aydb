use super::*;
use crate::memtable::{DEFAULT_MEMTABLE_CAPACITY_BYTES, MemTable};
use crate::pal::OsFs;
use crate::util::test_util::TempTestDir;

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

    // Verify `[min_key, max_key]` key-range metadata & pruning on `seg1`
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

    // Verify that a crafted index segment with an inflated `fp_count` (even with a
    // valid recomputed footer_crc32c) is rejected before allocating `fence_pointers`.
    drop(comp);
    let seg_fname = crate::meta::format_idx_file_name(3);
    let seg_file = fs
        .open_file(
            &seg_fname,
            false,
            true,
            crate::pal::OpenHint::ImmutableRandom,
        )
        .unwrap();
    let flen = seg_file.file_len().unwrap() as usize;
    let mut raw_seg = vec![0u8; flen];
    seg_file.read_exact_at(&mut raw_seg, 0).unwrap();

    let footer_start = flen - 48;
    let meta_offset = u64::from_le_bytes(
        raw_seg[footer_start + 24..footer_start + 32]
            .try_into()
            .unwrap(),
    ) as usize;
    // `meta_buf` starts with `[num_probes: u8 | reserved: [u8; 3] | bit_bytes_len: u32 LE | bits...]`,
    // followed by `fp_count: u32`.
    let bloom_byte_len = u32::from_le_bytes(
        raw_seg[meta_offset + 4..meta_offset + 8]
            .try_into()
            .unwrap(),
    ) as usize;
    let fp_count_pos = meta_offset + 8 + bloom_byte_len;
    // Overwrite `fp_count` with `u32::MAX` and recompute both `meta_crc` and `footer_crc32c`
    raw_seg[fp_count_pos..fp_count_pos + 4].copy_from_slice(&u32::MAX.to_le_bytes());
    let new_meta_crc = crate::util::crc32c::crc32c(&raw_seg[meta_offset..footer_start]);
    raw_seg[footer_start + 36..footer_start + 40].copy_from_slice(&new_meta_crc.to_le_bytes());
    let new_footer_crc = crate::util::crc32c::crc32c(&raw_seg[footer_start..footer_start + 44]);
    raw_seg[footer_start + 44..footer_start + 48].copy_from_slice(&new_footer_crc.to_le_bytes());
    seg_file.write_all_at(&raw_seg, 0).unwrap();
    drop(seg_file);

    let err = match IdxSegmentReader::open(&fs, 3) {
        Ok(_) => panic!("crafted huge fp_count with valid CRC must be rejected"),
        Err(e) => e,
    };
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
}

#[test]
fn point_lookup_uses_cached_block_and_counts_hits_and_misses() {
    let tmp = TempTestDir::new("idx_get_cache");
    let fs = OsFs::open(&tmp.path, true).expect("open OsFs");
    let mut mt = MemTable::new(DEFAULT_MEMTABLE_CAPACITY_BYTES);
    for i in 0..200u32 {
        mt.put_inline(
            format!("key_{i:05}").into_bytes(),
            format!("val_{i}").into_bytes(),
            (i + 1) as u64,
        );
    }
    let mut seg = mt.flush_to_segment(&fs, 1, 10).expect("flush");
    let cache = Arc::new(VerifiedBlockCache::new(1 << 20));
    seg.set_block_cache(Some(Arc::clone(&cache)));

    // The first lookup reads the block from disk and populates the cache; later lookups of
    // keys in the same block are served from it. Each lookup counts exactly one hit or miss.
    let expect = |i: u32| {
        Some((
            (i + 1) as u64,
            IndexValue::Inline(format!("val_{i}").into_bytes()),
        ))
    };
    assert_eq!(seg.get(b"key_00007").unwrap(), expect(7));
    assert_eq!((cache.hits(), cache.misses()), (0, 1));
    assert_eq!(seg.get(b"key_00007").unwrap(), expect(7));
    assert_eq!((cache.hits(), cache.misses()), (1, 1));
    for i in 0..200u32 {
        assert_eq!(
            seg.get(format!("key_{i:05}").as_bytes()).unwrap(),
            expect(i)
        );
    }
    assert_eq!(cache.hits() + cache.misses(), 202);
    assert!(cache.hits() > 190);
    assert_eq!(seg.get(b"key_99999").unwrap(), None);
}

#[test]
fn hashed_lookup_and_visitor_match_get_for_every_value_type() {
    let tmp = TempTestDir::new("idx_visit_hashed");
    let fs = OsFs::open(&tmp.path, true).expect("open OsFs");
    let ptr = BlobPtr {
        kind: BlobKind::Standard,
        table_id: 3,
        seg_id: 9,
        offset: 128,
        len: 4096,
        crc32c: 0xABCD,
    };
    let mut mt = MemTable::new(DEFAULT_MEMTABLE_CAPACITY_BYTES);
    mt.put_inline(b"inline".to_vec(), b"payload".to_vec(), 1);
    mt.put_blob_ptr(b"blob".to_vec(), ptr, 2);
    mt.delete(b"gone".to_vec(), 3);
    let mut seg = mt.flush_to_segment(&fs, 1, 10).expect("flush");
    seg.set_block_cache(Some(Arc::new(VerifiedBlockCache::new(1 << 20))));

    // Run twice so that both the disk path and the cached-block path are exercised.
    for _ in 0..2 {
        for key in [&b"inline"[..], b"blob", b"gone", b"absent"] {
            let hash = BloomFilter::hash_pair(key);
            let owned = seg.get(key).unwrap();
            assert_eq!(seg.get_hashed(key, hash).unwrap(), owned);
            let visited = seg
                .visit_hashed(key, hash, |raw| {
                    Ok((raw.lsn, raw.val_type, raw.val_bytes.to_vec()))
                })
                .unwrap();
            match (&owned, &visited) {
                (None, None) => {}
                (Some((lsn, IndexValue::Inline(v))), Some((l, 1, b))) => {
                    assert_eq!((lsn, v), (l, b));
                }
                (Some((lsn, IndexValue::Blob(p))), Some((l, 2, b))) => {
                    assert_eq!(lsn, l);
                    assert_eq!(*p, RawIdxEntry::decode_blob_ptr(b).unwrap());
                }
                (Some((lsn, IndexValue::Tombstone)), Some((l, 3, _))) => assert_eq!(lsn, l),
                other => panic!("visitor and owned lookup disagree: {other:?}"),
            }
        }
    }
}

#[test]
fn idx_builder_finish_rejects_metadata_exceeding_max_index_meta_bytes() {
    let tmp = TempTestDir::new("idx_meta_cap");
    let fs = OsFs::open(&tmp.path, true).expect("open OsFs");
    let bits_per_key = (MAX_INDEX_META_BYTES + 1024) * 8;
    let mut builder = IdxSegmentBuilder::create(&fs, 1, 0, bits_per_key).unwrap();
    builder.add_raw_entry(b"k1", 1, 1, b"v1").unwrap();
    let err = builder
        .finish()
        .err()
        .expect("oversized idx metadata must be rejected before writing footer");
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
}

#[test]
fn idx_open_rejects_mismatched_unsorted_or_missing_max_key_metadata() {
    let tmp = TempTestDir::new("idx_fence_validate");
    let fs = OsFs::open(&tmp.path, true).expect("open OsFs");

    let mut mt = MemTable::new(DEFAULT_MEMTABLE_CAPACITY_BYTES);
    let payload = vec![0x42u8; 256];
    for i in 0..80u32 {
        mt.put_inline(
            format!("k_{i:04}").into_bytes(),
            payload.clone(),
            (i + 1) as u64,
        );
    }
    let seg = mt.flush_to_segment(&fs, 1, 10).unwrap();
    assert!(seg.num_blocks() >= 3);
    let expected_max_key = seg.max_key().to_vec();
    drop(seg);

    let fname = format_idx_file_name(1);
    let orig_bytes = std::fs::read(tmp.path.join(&fname)).unwrap();
    let footer_start = orig_bytes.len() - IDX_FOOTER_SIZE;
    let footer: [u8; IDX_FOOTER_SIZE] = orig_bytes[footer_start..].try_into().unwrap();
    let meta_offset = u64::from_le_bytes(footer[24..32].try_into().unwrap()) as usize;
    let num_blocks = u32::from_le_bytes(footer[16..20].try_into().unwrap());
    let orig_meta = &orig_bytes[meta_offset..footer_start];

    // Parse original Bloom + fence pointers to build targeted crafted trailers.
    let mut s = orig_meta;
    let bloom = BloomFilter::decode(&mut s).unwrap();
    let mut bloom_bytes = Vec::new();
    bloom.encode(&mut bloom_bytes);
    let fp_count = u32::from_le_bytes(s[0..4].try_into().unwrap()) as usize;
    assert_eq!(fp_count, num_blocks as usize);
    s = &s[4..];
    let mut fps = Vec::with_capacity(fp_count);
    for _ in 0..fp_count {
        let klen = u16::from_le_bytes([s[0], s[1]]) as usize;
        let bidx = u32::from_le_bytes([s[2], s[3], s[4], s[5]]);
        let fk = s[6..6 + klen].to_vec();
        s = &s[6 + klen..];
        fps.push((fk, bidx));
    }

    let write_crafted = |data: &[u8], new_meta: &[u8]| {
        let mut raw = data.to_vec();
        raw.truncate(meta_offset);
        raw.extend_from_slice(new_meta);
        let mut crafted_footer = footer;
        crafted_footer[32..36].copy_from_slice(&(new_meta.len() as u32).to_le_bytes());
        crafted_footer[36..40].copy_from_slice(&crc32c(new_meta).to_le_bytes());
        let fcrc = crc32c(&crafted_footer[0..44]);
        crafted_footer[44..48].copy_from_slice(&fcrc.to_le_bytes());
        raw.extend_from_slice(&crafted_footer);
        std::fs::write(tmp.path.join(&fname), &raw).unwrap();
    };

    let encode_fps = |entries: &[(Vec<u8>, u32)], max_tail: Option<&[u8]>| -> Vec<u8> {
        let mut m = bloom_bytes.clone();
        m.extend_from_slice(&(entries.len() as u32).to_le_bytes());
        for (fk, bidx) in entries {
            m.extend_from_slice(&(fk.len() as u16).to_le_bytes());
            m.extend_from_slice(&bidx.to_le_bytes());
            m.extend_from_slice(fk);
        }
        if let Some(tail) = max_tail {
            m.extend_from_slice(tail);
        }
        m
    };

    // 1. Mismatched `fp_count < num_blocks` must fail with InvalidData.
    let short_meta = encode_fps(&fps[..1], Some(&[2, 0, b'z', b'z']));
    write_crafted(&orig_bytes, &short_meta);
    assert_eq!(
        IdxSegmentReader::open(&fs, 1).err().unwrap().kind(),
        io::ErrorKind::InvalidData
    );

    // 2. Unsorted fence pointer keys must fail with InvalidData.
    let mut swapped = fps.clone();
    let (k0, k1) = (swapped[0].0.clone(), swapped[1].0.clone());
    swapped[0].0 = k1;
    swapped[1].0 = k0;
    let mut normal_tail = Vec::new();
    normal_tail.extend_from_slice(&(expected_max_key.len() as u16).to_le_bytes());
    normal_tail.extend_from_slice(&expected_max_key);
    write_crafted(&orig_bytes, &encode_fps(&swapped, Some(&normal_tail)));
    assert_eq!(
        IdxSegmentReader::open(&fs, 1).err().unwrap().kind(),
        io::ErrorKind::InvalidData
    );

    // 3. Trailing garbage bytes after `max_key` must fail with InvalidData.
    let mut garbage_tail = normal_tail.clone();
    garbage_tail.push(0xAA);
    write_crafted(&orig_bytes, &encode_fps(&fps, Some(&garbage_tail)));
    assert_eq!(
        IdxSegmentReader::open(&fs, 1).err().unwrap().kind(),
        io::ErrorKind::InvalidData
    );

    // 4. Single truncated length byte in `max_key` tail must fail with InvalidData.
    write_crafted(&orig_bytes, &encode_fps(&fps, Some(&[1u8])));
    assert_eq!(
        IdxSegmentReader::open(&fs, 1).err().unwrap().kind(),
        io::ErrorKind::InvalidData
    );

    // 5. Segment omitting trailing `max_key` (pre-release legacy shape) must fail with
    //    InvalidData even when the last data block is intact.
    let legacy_meta = encode_fps(&fps, None);
    write_crafted(&orig_bytes, &legacy_meta);
    assert_eq!(
        IdxSegmentReader::open(&fs, 1).err().unwrap().kind(),
        io::ErrorKind::InvalidData
    );
}

#[test]
fn idx_segment_cursor_borrows_cached_block_zero_copy_and_shrinks_struct() {
    assert!(std::mem::size_of::<IdxSegmentCursor<'_, crate::pal::OsFile>>() < 512);
    assert!(std::mem::size_of::<IdxSegmentRevCursor<'_, crate::pal::OsFile>>() < 512);

    let tmp = TempTestDir::new("cursor_zero_copy");
    let fs = OsFs::open(&tmp.path, true).expect("open OsFs");
    let cache = Arc::new(VerifiedBlockCache::new(1024 * 1024));

    let mut builder = IdxSegmentBuilder::create(&fs, 1, 0, 10).unwrap();
    let val = vec![0x55u8; 64];
    for i in 0..200u32 {
        let k = format!("zc_key_{i:05}").into_bytes();
        builder.add_raw_entry(&k, i as u64 + 1, 1, &val).unwrap();
    }
    let mut reader = builder.finish().unwrap();
    assert!(reader.num_blocks() >= 3);
    reader.set_block_cache(Some(Arc::clone(&cache)));

    // Warm all blocks in VerifiedBlockCache.
    let mut warm = reader.cursor_from(b"zc_key_00000", true).unwrap();
    while warm.next_entry().unwrap().is_some() {}

    let fwd = reader.cursor_from(b"zc_key_00000", true).unwrap();
    assert!(fwd.block.scratch.is_none());
    assert!(matches!(fwd.block.active, ActiveBlock::Cached(_)));
    assert_eq!(fwd.current_key(), Some(b"zc_key_00000".as_slice()));

    let rev = reader.cursor_reverse_from(None).unwrap();
    assert!(rev.block.scratch.is_none());
    assert!(matches!(rev.block.active, ActiveBlock::Cached(_)));
    assert_eq!(rev.current_key(), Some(b"zc_key_00199".as_slice()));
}

#[test]
fn compact_idx_segments_uses_batched_reads_instead_of_per_block_syscalls() {
    use std::path::Path;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    struct CountingReadFile {
        inner: Arc<<OsFs as PlatformFs>::File>,
        is_idx: bool,
        read_exact_calls: Arc<AtomicUsize>,
    }

    impl PlatformFile for CountingReadFile {
        fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
            self.inner.read_at(buf, offset)
        }
        fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
            if self.is_idx {
                self.read_exact_calls.fetch_add(1, AtomicOrdering::Relaxed);
            }
            self.inner.read_exact_at(buf, offset)
        }
        fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
            self.inner.write_all_at(buf, offset)
        }
        fn file_len(&self) -> io::Result<u64> {
            self.inner.file_len()
        }
        fn preallocate(
            &self,
            target_len: u64,
            strategy: crate::pal::PreallocStrategy,
        ) -> io::Result<()> {
            self.inner.preallocate(target_len, strategy)
        }
        fn sync_barrier(&self) -> io::Result<()> {
            self.inner.sync_barrier()
        }
        fn sync_durable(&self) -> io::Result<()> {
            self.inner.sync_durable()
        }
        fn evict_os_cache(&self, offset: u64, len: u64) -> io::Result<()> {
            self.inner.evict_os_cache(offset, len)
        }
    }

    struct CountingReadFs {
        inner: OsFs,
        read_exact_calls: Arc<AtomicUsize>,
    }

    impl PlatformFs for CountingReadFs {
        type File = CountingReadFile;

        fn open_file(
            &self,
            rel_name: &str,
            create: bool,
            writable: bool,
            hint: OpenHint,
        ) -> io::Result<Arc<Self::File>> {
            Ok(Arc::new(CountingReadFile {
                inner: self.inner.open_file(rel_name, create, writable, hint)?,
                is_idx: rel_name.starts_with("idx_"),
                read_exact_calls: Arc::clone(&self.read_exact_calls),
            }))
        }
        fn remove_file(&self, rel_name: &str) -> io::Result<()> {
            self.inner.remove_file(rel_name)
        }
        fn list_files(&self) -> io::Result<Vec<String>> {
            self.inner.list_files()
        }
        fn root_path(&self) -> &Path {
            self.inner.root_path()
        }
    }

    let tmp = TempTestDir::new("batched_compaction_reads");
    let calls = Arc::new(AtomicUsize::new(0));
    let fs = CountingReadFs {
        inner: OsFs::open(&tmp.path, true).expect("open OsFs"),
        read_exact_calls: Arc::clone(&calls),
    };

    let val = vec![0x3Cu8; 64];
    for (seq_id, start) in [(1u64, 0u32), (2u64, 1u32)] {
        let mut b = IdxSegmentBuilder::create(&fs, seq_id, 0, 10).unwrap();
        for i in (start..2400u32).step_by(2) {
            let k = format!("batch_key_{i:05}").into_bytes();
            b.add_raw_entry(&k, i as u64 + 1, 1, &val).unwrap();
        }
        let r = b.finish().unwrap();
        assert!(r.num_blocks() >= 16);
    }

    let s1 = IdxSegmentReader::open(&fs, 1).unwrap();
    let s2 = IdxSegmentReader::open(&fs, 2).unwrap();
    calls.store(0, AtomicOrdering::Relaxed);

    let res = compact_idx_segments(&fs, &[&s1, &s2], 3, 1, 10, true).unwrap();
    let syscalls = calls.load(AtomicOrdering::Relaxed);
    assert!(
        syscalls <= 4,
        "expected batched compaction reads (<= 4 syscalls across >= 32 blocks), got {syscalls}"
    );
    let merged = res.new_segment.scan_all().unwrap();
    assert_eq!(merged.len(), 2400);
    for (idx, entry) in merged.iter().enumerate() {
        assert_eq!(entry.key, format!("batch_key_{idx:05}").into_bytes());
    }
}

#[test]
fn idx_block_and_segment_builders_reject_malformed_and_unsorted_entries_without_corrupting_state() {
    let mut blk = IdxBlockBuilder::new();
    blk.add_raw_entry(b"k20", 1, 1, b"val20").unwrap();
    assert_eq!(blk.num_entries(), 1);
    assert_eq!(blk.last_key(), Some(b"k20".as_slice()));

    // Invalid type tag, wrong BlobPtr length, non-empty tombstone, duplicate key, and unsorted key
    // must all fail cleanly BEFORE mutating `offsets` or `write_pos`.
    assert_eq!(
        blk.add_raw_entry(b"k30", 2, 99, b"").unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        blk.add_raw_entry(b"k30", 2, 2, &[0u8; 12])
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        blk.add_raw_entry(b"k30", 2, 3, b"x").unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        blk.add_raw_entry(b"k20", 2, 1, b"dup").unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        blk.add_raw_entry(b"k10", 2, 1, b"back").unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(blk.num_entries(), 1);

    blk.add_raw_entry(b"k30", 3, 1, b"val30").unwrap();
    let raw_block = blk.finish_and_reset();
    let reader = IdxBlockReader::open_verified(&raw_block).unwrap();
    assert_eq!(reader.num_entries, 2);
    assert_eq!(
        reader.lookup_binary_search(b"k20").unwrap(),
        Some((1, IndexValue::Inline(b"val20".to_vec())))
    );
    assert_eq!(
        reader.lookup_binary_search(b"k30").unwrap(),
        Some((3, IndexValue::Inline(b"val30".to_vec())))
    );

    // IdxSegmentBuilder must also reject out-of-order / duplicate keys across block boundaries
    // and oversized single entries without corrupting previously buffered entries.
    let tmp = TempTestDir::new("builder_validation");
    let fs = OsFs::open(&tmp.path, true).expect("open OsFs");
    let mut seg = IdxSegmentBuilder::create(&fs, 7, 0, 10).unwrap();
    let pad_val = vec![0x55u8; 512];
    for i in 0..16u32 {
        let k = format!("k_{i:04}").into_bytes();
        seg.add_raw_entry(&k, i as u64 + 1, 1, &pad_val).unwrap();
    }
    assert!(seg.fence_pointers.len() >= 2);
    assert_eq!(
        seg.add_raw_entry(b"k_0005", 100, 1, b"v")
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        seg.add_raw_entry(b"k_0015", 100, 1, b"v")
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        seg.add_entry(&IndexEntry {
            key: b"z_huge".to_vec(),
            lsn: 101,
            value: IndexValue::Inline(vec![0u8; IDX_BLOCK_SIZE]),
        })
        .unwrap_err()
        .kind(),
        io::ErrorKind::InvalidInput
    );
    let seg_reader = seg.finish().unwrap();
    assert_eq!(seg_reader.scan_all().unwrap().len(), 16);
}

#[test]
fn fence_pointer_seek_exact_block_boundary_skips_prior_block() {
    use std::path::Path;
    use std::sync::Mutex;

    struct SpyOffsetFile {
        inner: Arc<<OsFs as PlatformFs>::File>,
        is_idx: bool,
        offsets: Arc<Mutex<Vec<u64>>>,
    }

    impl PlatformFile for SpyOffsetFile {
        fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
            self.inner.read_at(buf, offset)
        }
        fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
            if self.is_idx {
                self.offsets.lock().unwrap().push(offset);
            }
            self.inner.read_exact_at(buf, offset)
        }
        fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
            self.inner.write_all_at(buf, offset)
        }
        fn file_len(&self) -> io::Result<u64> {
            self.inner.file_len()
        }
        fn preallocate(
            &self,
            target_len: u64,
            strategy: crate::pal::PreallocStrategy,
        ) -> io::Result<()> {
            self.inner.preallocate(target_len, strategy)
        }
        fn sync_barrier(&self) -> io::Result<()> {
            self.inner.sync_barrier()
        }
        fn sync_durable(&self) -> io::Result<()> {
            self.inner.sync_durable()
        }
        fn evict_os_cache(&self, offset: u64, len: u64) -> io::Result<()> {
            self.inner.evict_os_cache(offset, len)
        }
    }

    struct SpyOffsetFs {
        inner: OsFs,
        offsets: Arc<Mutex<Vec<u64>>>,
    }

    impl PlatformFs for SpyOffsetFs {
        type File = SpyOffsetFile;

        fn open_file(
            &self,
            rel_name: &str,
            create: bool,
            writable: bool,
            hint: OpenHint,
        ) -> io::Result<Arc<Self::File>> {
            Ok(Arc::new(SpyOffsetFile {
                inner: self.inner.open_file(rel_name, create, writable, hint)?,
                is_idx: rel_name.starts_with("idx_"),
                offsets: Arc::clone(&self.offsets),
            }))
        }
        fn remove_file(&self, rel_name: &str) -> io::Result<()> {
            self.inner.remove_file(rel_name)
        }
        fn list_files(&self) -> io::Result<Vec<String>> {
            self.inner.list_files()
        }
        fn root_path(&self) -> &Path {
            self.inner.root_path()
        }
    }

    let tmp = TempTestDir::new("fence_exact_boundary");
    let offsets = Arc::new(Mutex::new(Vec::new()));
    let fs = SpyOffsetFs {
        inner: OsFs::open(&tmp.path, true).expect("open OsFs"),
        offsets: Arc::clone(&offsets),
    };

    let val = vec![0x7Au8; 512];
    let mut seg = IdxSegmentBuilder::create(&fs, 1, 0, 10).unwrap();
    for i in 0..32u32 {
        let k = format!("fp_{i:04}").into_bytes();
        seg.add_raw_entry(&k, i as u64 + 1, 1, &val).unwrap();
    }
    let seg_reader = seg.finish().unwrap();
    assert!(seg_reader.fence_pointers.len() >= 4);
    let target_key = seg_reader.fence_pointers[2].first_key.to_vec();
    let expected_offset = 2 * IDX_BLOCK_SIZE as u64;

    // 1. cursor_from (inclusive) seeking to block 2's exact first_key must read block 2 directly.
    offsets.lock().unwrap().clear();
    let cur = seg_reader.cursor_from(&target_key, true).unwrap();
    assert_eq!(cur.current_key(), Some(target_key.as_slice()));
    assert_eq!(*offsets.lock().unwrap(), vec![expected_offset]);

    // 2. scan_prefix_range seeking to block 2's exact first_key must also skip block 1.
    offsets.lock().unwrap().clear();
    let rows = seg_reader
        .scan_prefix_range(b"fp_", &target_key, None, 1)
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].key, target_key);
    assert_eq!(*offsets.lock().unwrap(), vec![expected_offset]);
}

#[test]
fn idx_block_builder_finish_into_zeroes_padding_across_reused_blocks() {
    let mut builder = IdxBlockBuilder::new();
    let noisy = vec![0xFFu8; 512];
    for i in 0..6u32 {
        let k = format!("noisy_{i:02}").into_bytes();
        builder.add_raw_entry(&k, i as u64 + 1, 1, &noisy).unwrap();
    }
    let mut out = Vec::new();
    builder.finish_into(&mut out);
    assert_eq!(out.len(), IDX_BLOCK_SIZE);
    let b1: &[u8; IDX_BLOCK_SIZE] = (&out[..IDX_BLOCK_SIZE]).try_into().unwrap();
    assert!(IdxBlockReader::open_verified(b1).is_ok());

    // Second block contains only one short entry, leaving > 4000 bytes of padding that used
    // to hold 0xFF from the first block.
    out.clear();
    builder.add_raw_entry(b"tiny", 10, 1, b"ok").unwrap();
    builder.finish_into(&mut out);
    assert_eq!(out.len(), IDX_BLOCK_SIZE);
    let b2: &[u8; IDX_BLOCK_SIZE] = (&out[..IDX_BLOCK_SIZE]).try_into().unwrap();
    let reader2 = IdxBlockReader::open_verified(b2).unwrap();
    assert_eq!(reader2.num_entries(), 1);
    assert_eq!(
        reader2.lookup_binary_search(b"tiny").unwrap(),
        Some((10, IndexValue::Inline(b"ok".to_vec())))
    );

    let entry_end = 11 + 4 + 2 + 2;
    let dir_start = IDX_BLOCK_USABLE_SIZE - 2;
    assert!(
        b2[entry_end..dir_start].iter().all(|&b| b == 0),
        "unused gap in reused IdxBlockBuilder must be zeroed"
    );
}

#[test]
fn empty_cursors_and_exclusive_min_upper_bound_return_none_without_panicking() {
    let tmp = TempTestDir::new("empty_cursor_no_panic");
    let fs = OsFs::open(&tmp.path, true).expect("open OsFs");

    // 1. Non-empty segment: forward seek past max_key and reverse seek with exclusive min_key.
    let mut builder = IdxSegmentBuilder::create(&fs, 1, 0, 10).unwrap();
    for i in 1..=10u32 {
        let k = format!("k_{i:04}").into_bytes();
        builder.add_raw_entry(&k, i as u64, 1, b"val").unwrap();
    }
    let seg = builder.finish().unwrap();

    let mut fwd_past = seg.cursor_from(b"k_0010", false).unwrap();
    assert_eq!(fwd_past.current_key(), None);
    assert_eq!(fwd_past.peek_raw(), None);
    assert_eq!(fwd_past.next_entry().unwrap(), None);
    fwd_past.advance().unwrap();

    let mut rev_below_min = seg
        .cursor_reverse_from(Some((seg.min_key(), false)))
        .unwrap();
    assert_eq!(rev_below_min.current_key(), None);
    assert_eq!(rev_below_min.peek_raw(), None);
    assert_eq!(rev_below_min.next_entry().unwrap(), None);
    rev_below_min.advance().unwrap();

    // 2. 0-block segment (e.g. produced after full compaction drops all tombstones).
    let empty_builder = IdxSegmentBuilder::create(&fs, 2, 1, 10).unwrap();
    let empty_seg = empty_builder.finish().unwrap();
    assert_eq!(empty_seg.num_blocks(), 0);

    for mut cur in [
        empty_seg.cursor_all().unwrap(),
        empty_seg.cursor_all_batched(4).unwrap(),
        empty_seg.cursor_from(b"k_0001", true).unwrap(),
    ] {
        assert_eq!(cur.current_key(), None);
        assert_eq!(cur.peek_raw(), None);
        assert_eq!(cur.next_entry().unwrap(), None);
        cur.advance().unwrap();
    }
    let mut rev_empty = empty_seg.cursor_reverse_from(None).unwrap();
    assert_eq!(rev_empty.current_key(), None);
    assert_eq!(rev_empty.peek_raw(), None);
    assert_eq!(rev_empty.next_entry().unwrap(), None);
    rev_empty.advance().unwrap();
}

#[test]
fn cursor_load_error_leaves_cursor_empty_without_panicking() {
    let tmp = TempTestDir::new("cursor_load_error_state");
    let fs = OsFs::open(&tmp.path, true).expect("open OsFs");

    let mut builder = IdxSegmentBuilder::create(&fs, 1, 0, 10).unwrap();
    let val = vec![0x42u8; 512];
    for i in 0..24u32 {
        let k = format!("key_{i:04}").into_bytes();
        builder.add_raw_entry(&k, i as u64 + 1, 1, &val).unwrap();
    }
    let seg = builder.finish().unwrap();
    assert!(seg.num_blocks() >= 2);
    drop(seg);

    // Corrupt block 1's CRC while leaving block 0 and footer valid.
    let fname = crate::meta::format_idx_file_name(1);
    let file = fs
        .open_file(&fname, false, true, OpenHint::ImmutableRandom)
        .unwrap();
    let mut b1 = [0u8; IDX_BLOCK_SIZE];
    file.read_exact_at(&mut b1, IDX_BLOCK_SIZE as u64).unwrap();
    b1[0] ^= 0xFF;
    file.write_all_at(&b1, IDX_BLOCK_SIZE as u64).unwrap();
    file.sync_barrier().unwrap();
    drop(file);

    // Single-block forward cursor: block 0 works, crossing into block 1 fails cleanly.
    let seg = IdxSegmentReader::open(&fs, 1).unwrap();
    let mut cur = seg.cursor_all().unwrap();
    loop {
        match cur.advance() {
            Ok(()) if cur.current_key().is_none() => panic!("expected block 1 corruption"),
            Ok(()) => {}
            Err(e) => {
                assert_eq!(e.kind(), io::ErrorKind::InvalidData);
                break;
            }
        }
    }
    assert_eq!(cur.current_key(), None);
    assert_eq!(cur.peek_raw(), None);

    // Batched read-ahead cursor: failing to load block 1 resets current_span and active block.
    let mut batched = seg.cursor_all_batched(1).unwrap();
    assert!(batched.current_key().is_some());
    loop {
        match batched.advance() {
            Ok(()) if batched.current_key().is_none() => {
                panic!("expected block 1 corruption on batched cursor")
            }
            Ok(()) => {}
            Err(e) => {
                assert_eq!(e.kind(), io::ErrorKind::InvalidData);
                break;
            }
        }
    }
    assert_eq!(batched.current_key(), None);
    assert_eq!(batched.peek_raw(), None);

    // Reverse cursor: starting on last block then walking backward into corrupted block 1.
    let mut rev = seg.cursor_reverse_from(None).unwrap();
    assert!(rev.current_key().is_some());
    loop {
        match rev.advance() {
            Ok(()) if rev.current_key().is_none() => {
                panic!("expected block 1 corruption on reverse cursor")
            }
            Ok(()) => {}
            Err(e) => {
                assert_eq!(e.kind(), io::ErrorKind::InvalidData);
                break;
            }
        }
    }
    assert_eq!(rev.current_key(), None);
    assert_eq!(rev.peek_raw(), None);
}

#[test]
fn index_public_bounds_checks_reject_invalid_blob_kind_and_oob_indices() {
    let mut bb = IdxBlockBuilder::new();
    let mut bad_ptr = [0u8; BLOB_PTR_ENCODED_LEN];
    bad_ptr[0] = 99;
    let err = bb.add_raw_entry(b"k1", 1, 2, &bad_ptr).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);

    bb.add_raw_entry(b"k1", 1, 1, b"v1").unwrap();
    let mut raw_vec = Vec::new();
    bb.finish_into(&mut raw_vec);
    let raw_blk: [u8; IDX_BLOCK_SIZE] = raw_vec.try_into().unwrap();
    let reader = IdxBlockReader::open_verified(&raw_blk).unwrap();
    assert_eq!(
        reader.raw_entry_at(1).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        reader.decode_entry_at(5).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );

    let tmp = TempTestDir::new("idx_oob_read_block");
    let fs = OsFs::open(&tmp.path, true).unwrap();
    let mut sb = IdxSegmentBuilder::create(&fs, 2, 0, 10).unwrap();
    sb.add_raw_entry(b"k1", 1, 1, b"v1").unwrap();
    let seg = sb.finish().unwrap();
    let mut scratch = [0u8; IDX_BLOCK_SIZE];
    let oob_idx = seg.num_blocks();
    assert_eq!(
        seg.read_block(oob_idx, &mut scratch, false)
            .err()
            .unwrap()
            .kind(),
        io::ErrorKind::InvalidInput
    );

    let mut capped_sb =
        IdxSegmentBuilder::create(&fs, 3, 0, (MAX_INDEX_META_BYTES + 1024) * 8).unwrap();
    capped_sb.set_cap_bloom_to_meta_budget(true);
    capped_sb.add_raw_entry(b"k1", 1, 1, b"v1").unwrap();
    let capped_seg = capped_sb.finish().unwrap();
    assert_eq!(
        capped_seg.get(b"k1").unwrap(),
        Some((1, IndexValue::Inline(b"v1".to_vec())))
    );
}

#[test]
fn idx_block_and_segment_structural_checks_reject_crafted_corruption() {
    // 1. `open_verified` rejects overlapping directory offsets and > MAX_IDX_BLOCK_ENTRIES.
    let mut bb = IdxBlockBuilder::new();
    bb.add_raw_entry(b"k1", 1, 1, b"v1").unwrap();
    bb.add_raw_entry(b"k2", 2, 1, b"v2").unwrap();
    let mut raw_vec = Vec::new();
    bb.finish_into(&mut raw_vec);
    let base_blk: [u8; IDX_BLOCK_SIZE] = raw_vec.try_into().unwrap();

    let mut overlap_blk = base_blk;
    let dir_start = IDX_BLOCK_USABLE_SIZE - 4;
    // Force second entry offset to overlap first entry (offset 2 < prev_end).
    overlap_blk[dir_start + 2..dir_start + 4].copy_from_slice(&2u16.to_le_bytes());
    let crc = crc32c(&overlap_blk[0..4092]);
    overlap_blk[4092..4096].copy_from_slice(&crc.to_le_bytes());
    assert_eq!(
        IdxBlockReader::open_verified(&overlap_blk)
            .err()
            .unwrap()
            .kind(),
        io::ErrorKind::InvalidData
    );

    let mut too_many_blk = base_blk;
    let bad_count = (MAX_IDX_BLOCK_ENTRIES + 1) as u16;
    too_many_blk[4088..4090].copy_from_slice(&bad_count.to_le_bytes());
    let crc = crc32c(&too_many_blk[0..4092]);
    too_many_blk[4092..4096].copy_from_slice(&crc.to_le_bytes());
    assert_eq!(
        IdxBlockReader::open_verified(&too_many_blk)
            .err()
            .unwrap()
            .kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(
        IdxBlockReader::open_preverified(&too_many_blk)
            .err()
            .unwrap()
            .kind(),
        io::ErrorKind::InvalidData
    );

    // 2. `IdxSegmentBuilder::add_raw_entry` rejects invalid BlobKind BEFORE flushing.
    let tmp = TempTestDir::new("idx_raw_blobkind_and_empty");
    let fs = OsFs::open(&tmp.path, true).unwrap();
    let mut sb = IdxSegmentBuilder::create(&fs, 10, 0, 10).unwrap();
    sb.add_raw_entry(b"a", 1, 1, b"x").unwrap();
    let mut bad_ptr = [0u8; BLOB_PTR_ENCODED_LEN];
    bad_ptr[0] = 77;
    assert_eq!(
        sb.add_raw_entry(b"b", 2, 2, &bad_ptr).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(sb.num_blocks, 0);
    let valid_seg = sb.finish().unwrap();
    assert_eq!(valid_seg.record_count(), 1);
    drop(valid_seg);

    // 3. `IdxSegmentReader::open` rejects num_blocks==0 with non-zero record_count or max_key.
    let write_crafted_empty_seg = |seq_id: u64, record_count: u32, max_key: &[u8]| {
        let mut meta_buf = Vec::new();
        BloomFilter::new(0, 0).encode(&mut meta_buf);
        meta_buf.extend_from_slice(&0u32.to_le_bytes());
        meta_buf.extend_from_slice(&(max_key.len() as u16).to_le_bytes());
        meta_buf.extend_from_slice(max_key);

        let mut footer = [0u8; IDX_FOOTER_SIZE];
        footer[0..4].copy_from_slice(IDX_FOOTER_MAGIC);
        footer[4..6].copy_from_slice(&IDX_FORMAT_VERSION.to_le_bytes());
        footer[8..16].copy_from_slice(&seq_id.to_le_bytes());
        footer[16..20].copy_from_slice(&0u32.to_le_bytes()); // num_blocks == 0
        footer[20..24].copy_from_slice(&record_count.to_le_bytes());
        footer[24..32].copy_from_slice(&0u64.to_le_bytes()); // meta_offset == 0
        footer[32..36].copy_from_slice(&(meta_buf.len() as u32).to_le_bytes());
        footer[36..40].copy_from_slice(&crc32c(&meta_buf).to_le_bytes());
        let fcrc = crc32c(&footer[0..44]);
        footer[44..48].copy_from_slice(&fcrc.to_le_bytes());

        let mut bytes = Vec::with_capacity(meta_buf.len() + IDX_FOOTER_SIZE);
        bytes.extend_from_slice(&meta_buf);
        bytes.extend_from_slice(&footer);
        let f = fs
            .open_file(
                &format_idx_file_name(seq_id),
                true,
                true,
                OpenHint::ImmutableRandom,
            )
            .unwrap();
        f.write_all_at(&bytes, 0).unwrap();
    };

    write_crafted_empty_seg(11, 5, b"");
    assert_eq!(
        IdxSegmentReader::open(&fs, 11).err().unwrap().kind(),
        io::ErrorKind::InvalidData
    );

    write_crafted_empty_seg(12, 0, b"nonempty");
    assert_eq!(
        IdxSegmentReader::open(&fs, 12).err().unwrap().kind(),
        io::ErrorKind::InvalidData
    );

    // 4. `IdxBlockReader::open_verified` rejects unsorted and duplicate keys even with valid CRC.
    let craft_two_key_block = |k_first: &[u8], k_second: &[u8]| -> [u8; IDX_BLOCK_SIZE] {
        let mut blk = [0u8; IDX_BLOCK_SIZE];
        let mut write_entry = |pos: usize, key: &[u8], lsn: u64| -> usize {
            blk[pos..pos + 2].copy_from_slice(&(key.len() as u16).to_le_bytes());
            blk[pos + 2] = 1; // Inline
            blk[pos + 3..pos + 11].copy_from_slice(&lsn.to_le_bytes());
            let ks = pos + 11;
            blk[ks..ks + key.len()].copy_from_slice(key);
            let vs = ks + key.len();
            blk[vs..vs + 2].copy_from_slice(&1u16.to_le_bytes());
            blk[vs + 2] = b'v';
            vs + 3
        };
        let off0 = 0usize;
        let off1 = write_entry(off0, k_first, 1);
        let _end = write_entry(off1, k_second, 2);
        let dir_start = IDX_BLOCK_USABLE_SIZE - 4;
        blk[dir_start..dir_start + 2].copy_from_slice(&(off0 as u16).to_le_bytes());
        blk[dir_start + 2..dir_start + 4].copy_from_slice(&(off1 as u16).to_le_bytes());
        blk[4088..4090].copy_from_slice(&2u16.to_le_bytes());
        let crc = crc32c(&blk[0..4092]);
        blk[4092..4096].copy_from_slice(&crc.to_le_bytes());
        blk
    };

    let descending_blk = craft_two_key_block(b"k2", b"k1");
    assert_eq!(
        IdxBlockReader::open_verified(&descending_blk)
            .err()
            .unwrap()
            .kind(),
        io::ErrorKind::InvalidData
    );
    let duplicate_blk = craft_two_key_block(b"k1", b"k1");
    assert_eq!(
        IdxBlockReader::open_verified(&duplicate_blk)
            .err()
            .unwrap()
            .kind(),
        io::ErrorKind::InvalidData
    );

    // 5. `IdxSegmentBuilder::finish` with bounded bloom & multi-block segment completes with exact footer reservation.
    let mut sb2 = IdxSegmentBuilder::create(&fs, 13, 0, 10).unwrap();
    for i in 0..200u32 {
        let k = format!("key_{i:04}");
        let v = vec![0x33u8; 64];
        sb2.add_entry(&IndexEntry {
            key: k.into_bytes(),
            lsn: i as u64 + 1,
            value: IndexValue::Inline(v),
        })
        .unwrap();
    }
    let multi = sb2.finish().unwrap();
    assert_eq!(multi.record_count(), 200);
    assert!(multi.num_blocks() > 1);
}
