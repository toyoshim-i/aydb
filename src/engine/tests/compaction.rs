use super::*;

#[test]
fn inverted_and_empty_range_queries_do_not_panic() {
    let tmp = TempTestDir::new("inverted_range");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut engine = StorageEngine::open(fs, EngineConfig::default()).unwrap();

    for i in 1..=10u8 {
        engine
            .put_row(1, &[i], &[i * 10], &[], WriteDurability::BufferedFast)
            .unwrap();
    }

    // 1. Test MemTable-only path (`idx_segments.is_empty()`)
    assert!(
        engine
            .scan_table_range(1, Some((&[8], true)), Some((&[3], true)), 50)
            .unwrap()
            .is_empty()
    );
    assert!(
        engine
            .scan_table_range(1, Some((&[5], false)), Some((&[5], false)), 50)
            .unwrap()
            .is_empty()
    );
    assert!(
        engine
            .scan_table_range(1, Some((&[5], true)), Some((&[5], false)), 50)
            .unwrap()
            .is_empty()
    );

    let mut slab = Vec::new();
    let mut spans = Vec::new();
    engine
        .scan_table_values_into(
            1,
            Some((&[8], true)),
            Some((&[3], true)),
            50,
            &mut slab,
            &mut spans,
        )
        .unwrap();
    assert!(spans.is_empty());

    // 2. Flush to SSTable and test multi-segment path
    engine.flush_memtable().unwrap();
    assert!(
        engine
            .scan_table_range(1, Some((&[8], true)), Some((&[3], true)), 50)
            .unwrap()
            .is_empty()
    );
    assert!(
        engine
            .scan_table_range(1, Some((&[5], false)), Some((&[5], false)), 50)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn flush_memtable_metadata_fault_preserves_entries_until_reopen() {
    use crate::pal::test_fs::{FsHooks, HookedFs};
    use std::sync::atomic::AtomicBool;

    struct FaultHooks {
        fail_boot_meta_sync: Arc<AtomicBool>,
    }

    impl FsHooks for FaultHooks {
        fn write_all_at(
            &self,
            name: &str,
            inner: &dyn PlatformFile,
            buf: &[u8],
            offset: u64,
        ) -> io::Result<()> {
            if name == "boot_meta.db" && self.fail_boot_meta_sync.load(Ordering::SeqCst) {
                return Err(io::Error::new(
                    io::ErrorKind::StorageFull,
                    "simulated boot_meta write failure",
                ));
            }
            inner.write_all_at(buf, offset)
        }
    }

    let tmp = TempTestDir::new("flush_fault_retry");
    let fail_flag = Arc::new(AtomicBool::new(false));
    let fs = Arc::new(HookedFs::new(
        OsFs::open(&tmp.path, true).unwrap(),
        FaultHooks {
            fail_boot_meta_sync: Arc::clone(&fail_flag),
        },
    ));

    let mut engine = StorageEngine::open(Arc::clone(&fs), EngineConfig::default()).unwrap();
    for i in 0..20u32 {
        let k = format!("k_{i:02}").into_bytes();
        let v = format!("v_{i:02}").into_bytes();
        engine
            .put_row(1, &k, &v, &[], WriteDurability::BufferedFast)
            .unwrap();
    }

    // Inject fault on boot_meta.db write during flush_memtable()
    fail_flag.store(true, Ordering::SeqCst);
    assert!(engine.flush_memtable().is_err());

    // The slot write may have reached disk, so reads remain available but mutations require reopen.
    fail_flag.store(false, Ordering::SeqCst);
    for i in 0..20u32 {
        let k = format!("k_{i:02}").into_bytes();
        let expected = format!("v_{i:02}").into_bytes();
        assert_eq!(engine.get_row(1, &k).unwrap(), Some(expected));
    }

    assert!(engine.is_poisoned());
    assert!(engine.flush_memtable().is_err());
    drop(engine);

    let mut reopened = StorageEngine::open(Arc::clone(&fs), EngineConfig::default()).unwrap();
    for i in 0..20u32 {
        let k = format!("k_{i:02}").into_bytes();
        let expected = format!("v_{i:02}").into_bytes();
        assert_eq!(reopened.get_row(1, &k).unwrap(), Some(expected));
    }
    drop(reopened);

    // A flush triggered by a write that is already committed must not fail that write.
    let mut cfg = EngineConfig::default();
    cfg.memtable_capacity_bytes = 256;
    let mut engine = StorageEngine::open(fs, cfg).unwrap();
    assert_eq!(engine.maintenance_failures(), 0);
    fail_flag.store(true, Ordering::SeqCst);
    engine
        .put_row(
            1,
            b"post",
            &[0x11u8; 512],
            &[],
            WriteDurability::BufferedFast,
        )
        .expect("a committed write must not report a post-commit flush failure");
    assert_eq!(engine.maintenance_failures(), 1);
    assert!(
        engine
            .last_maintenance_error()
            .unwrap()
            .contains("simulated boot_meta write failure")
    );
    assert!(engine.is_poisoned());
    assert_eq!(engine.get_row(1, b"post").unwrap(), Some(vec![0x11u8; 512]));
    fail_flag.store(false, Ordering::SeqCst);
    assert!(engine.flush_memtable().is_err());
    let fs = Arc::clone(&engine.fs);
    drop(engine);
    let mut reopened = StorageEngine::open(fs, EngineConfig::default()).unwrap();
    assert_eq!(
        reopened.get_row(1, b"post").unwrap(),
        Some(vec![0x11u8; 512])
    );
}

#[test]
fn segment_range_pruning_and_auto_compaction() {
    let tmp = TempTestDir::new("auto_compact");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut cfg = EngineConfig::default();
    cfg.auto_compact_segment_threshold = 4;

    let mut engine = StorageEngine::open(Arc::clone(&fs), cfg).unwrap();

    // Flush 3 segments (< threshold 4): segment count grows to 3
    for seg_idx in 0..3u32 {
        for k_idx in 0..20u32 {
            let k = format!("s{seg_idx:02}_k{k_idx:03}").into_bytes();
            let v = format!("v_{seg_idx}_{k_idx}").into_bytes();
            engine
                .put_row(1, &k, &v, &[], WriteDurability::BufferedFast)
                .unwrap();
        }
        engine.flush_memtable().unwrap();
        assert_eq!(engine.active_idx_segment_count(), (seg_idx + 1) as usize);
    }

    // 4th flush reaches `auto_compact_segment_threshold = 4` -> automatically compacts into 1 segment!
    for k_idx in 0..20u32 {
        let k = format!("s03_k{k_idx:03}").into_bytes();
        let v = format!("v_3_{k_idx}").into_bytes();
        engine
            .put_row(1, &k, &v, &[], WriteDurability::BufferedFast)
            .unwrap();
    }
    engine.flush_memtable().unwrap();
    assert_eq!(
        engine.active_idx_segment_count(),
        1,
        "reaching auto_compact_segment_threshold must automatically compact SSTables into 1 segment"
    );

    // Verify all 80 keys across the 4 flushed batches remain intact
    for seg_idx in 0..4u32 {
        for k_idx in 0..20u32 {
            let k = format!("s{seg_idx:02}_k{k_idx:03}").into_bytes();
            let expected = format!("v_{seg_idx}_{k_idx}").into_bytes();
            assert_eq!(engine.get_row(1, &k).unwrap(), Some(expected));
        }
    }
}

#[test]
fn size_tiered_auto_compaction_scan_keys_and_tx_budget() {
    let tmp = TempTestDir::new("tiered_scan_budget");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut cfg = EngineConfig::default();
    cfg.auto_compact_segment_threshold = 4;
    cfg.max_transaction_bytes = 8 * 1024; // 8 KB transaction budget for testing
    let mut engine = StorageEngine::open(fs.clone(), cfg).unwrap();

    // 1. Build a large base segment (`idx_segments[0]`, ~64 KB across multiple 4 KB blocks)
    let base_payload = vec![0x42u8; 256];
    for i in 0..180u32 {
        let pk = format!("base_key_{i:04}");
        engine
            .put_row(
                1,
                pk.as_bytes(),
                &base_payload,
                &[],
                WriteDurability::BufferedFast,
            )
            .unwrap();
    }
    engine.flush_memtable_no_auto_compact().unwrap();
    assert_eq!(engine.active_idx_segment_count(), 1);
    let base_seq_id = engine.idx_segments[0].seq_id();
    assert!(
        engine.idx_segments[0].file_len() >= 16 * 1024,
        "base segment should span multiple 4 KB blocks"
    );

    // Now perform 3 small flushes (including a tombstone for `base_key_0010` in the base segment).
    // When the 4th segment is flushed (`len() == 4 == auto_compact_segment_threshold`),
    // `compact_auto_tiered()` must merge ONLY the 3 small suffix segments (`1..4`) with
    // `drop_tombstones = false`, leaving the large `base_seq_id` segment at index 0 untouched!
    engine
        .delete_row(1, b"base_key_0010", WriteDurability::BufferedFast)
        .unwrap();
    engine.flush_memtable().unwrap();
    engine
        .put_row(1, b"new_key_1", b"v1", &[], WriteDurability::BufferedFast)
        .unwrap();
    engine.flush_memtable().unwrap();
    assert_eq!(engine.active_idx_segment_count(), 3);

    engine
        .put_row(1, b"new_key_2", b"v2", &[], WriteDurability::BufferedFast)
        .unwrap();
    engine.flush_memtable().unwrap();

    // The 3 small segments should have been merged into 1 suffix segment, leaving 2 segments total,
    // with `idx_segments[0]` still having `base_seq_id`!
    assert_eq!(
        engine.active_idx_segment_count(),
        2,
        "size-tiered auto-compaction must merge the 3 small suffix segments into 1"
    );
    assert_eq!(
        engine.idx_segments[0].seq_id(),
        base_seq_id,
        "large base segment must NOT be rewritten during small suffix auto-compaction"
    );
    // And the tombstone for `base_key_0010` in the merged suffix segment MUST have been preserved!
    assert_eq!(
        engine.get_row(1, b"base_key_0010").unwrap(),
        None,
        "suffix compaction with start_idx > 0 must preserve tombstones shadowing base segment"
    );
    assert_eq!(
        engine.get_row(1, b"new_key_1").unwrap(),
        Some(b"v1".to_vec())
    );
    assert_eq!(
        engine.get_row(1, b"new_key_2").unwrap(),
        Some(b"v2".to_vec())
    );

    // Explicit full `compact()` merges down to `start_idx == 0` (`1` segment) and drops tombstones
    engine.compact().unwrap();
    assert_eq!(engine.active_idx_segment_count(), 1);
    assert_eq!(engine.get_row(1, b"base_key_0010").unwrap(), None);

    // 2. `scan_keys_range` & `scan_range_into_slab`
    let pks = engine
        .scan_keys_range(1, Some(b"base_key_0008"), Some(b"base_key_0012"), 10)
        .unwrap();
    assert_eq!(
        pks,
        vec![
            b"base_key_0008".to_vec(),
            b"base_key_0009".to_vec(),
            // `base_key_0010` was deleted!
            b"base_key_0011".to_vec(),
            b"base_key_0012".to_vec(),
        ]
    );

    let mut rev_pks = Vec::new();
    let mut rev_slab = Vec::new();
    let mut rev_spans = Vec::new();
    engine
        .scan_range_into_slab(
            1,
            RangeScanOptions {
                lower: Some((b"base_key_0008", true)),
                upper: Some((b"base_key_0012", true)),
                reverse: true,
                limit: 2,
                keys_only: false,
            },
            SlabScanOutput {
                pks: &mut rev_pks,
                slab: &mut rev_slab,
                spans: &mut rev_spans,
                lazy_blobs: None,
            },
        )
        .unwrap();
    assert_eq!(rev_spans.len(), 2);
    assert_eq!(
        &rev_slab[rev_spans[0].0..rev_spans[0].1],
        base_payload.as_slice()
    );

    // 3. `max_transaction_bytes` budget enforcement
    engine.begin_transaction();
    engine
        .put_row(
            1,
            b"tx_ok",
            &vec![0x11u8; 2048],
            &[],
            WriteDurability::BufferedFast,
        )
        .unwrap();
    let err = engine
        .put_row(
            1,
            b"tx_too_big",
            &vec![0x22u8; 8192],
            &[],
            WriteDurability::BufferedFast,
        )
        .expect_err("exceeding max_transaction_bytes must return Err(InvalidInput)");
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    engine.rollback_transaction().unwrap();
    assert_eq!(engine.get_row(1, b"tx_ok").unwrap(), None);
    assert_eq!(engine.get_row(1, b"tx_too_big").unwrap(), None);
}

#[test]
fn reverse_streaming_scan_row_meta_skip_and_tx_blob_read() {
    let dir = TempTestDir::new("reverse_scan_row_meta_tx_blob");
    let fs = Arc::new(OsFs::open(&dir.path, true).unwrap());

    {
        let mut engine = StorageEngine::open(fs.clone(), EngineConfig::default()).unwrap();

        // 1. Uncapped table without secondary indexes (`table_id = 10`) must cache
        //    `may_have_row_meta(10) == false` on the first update/delete!
        engine
            .put_row(10, b"k1", b"v1", &[], WriteDurability::BufferedFast)
            .unwrap();
        engine.flush_memtable().unwrap();
        engine
            .put_row(10, b"k1", b"v2", &[], WriteDurability::BufferedFast)
            .unwrap();
        assert_eq!(
            engine.table_has_row_meta.get(&10),
            Some(&false),
            "uncapped table without secondary indexes must cache table_has_row_meta = false"
        );

        // Table with secondary indexes (`table_id = 11`) must record `table_has_row_meta = true`
        engine
            .put_row(
                11,
                b"sk1",
                b"sv1",
                &[(1, b"idx_a".to_vec())],
                WriteDurability::BufferedFast,
            )
            .unwrap();
        assert_eq!(engine.table_has_row_meta.get(&11), Some(&true));
        engine.flush_memtable().unwrap();

        // 2. Reading an uncommitted >1KB blob inside `tx_depth > 0` before WAL flush
        //    must transparently flush `BlobSegmentWriter`'s write buffer so `get_row` succeeds!
        engine.begin_transaction();
        let blob_payload = vec![0x7Bu8; 2048];
        engine
            .put_row(
                10,
                b"tx_blob",
                &blob_payload,
                &[],
                WriteDurability::BufferedFast,
            )
            .unwrap();
        assert_eq!(engine.get_row(10, b"tx_blob").unwrap(), Some(blob_payload));
        engine.commit_transaction().unwrap();

        // 3. Populate `table_id = 12` across multiple SSTable segments + MemTable
        //    and verify reverse streaming merge (`scan_range_into_slab(..., reverse = true, ...)`).
        for i in 0..30i32 {
            let pk = (i as u32).to_be_bytes();
            let val = format!("seg1_{i:02}").into_bytes();
            engine
                .put_row(12, &pk, &val, &[], WriteDurability::BufferedFast)
                .unwrap();
        }
        engine.flush_memtable().unwrap();
        for i in 15..45i32 {
            let pk = (i as u32).to_be_bytes();
            let val = format!("seg2_{i:02}").into_bytes();
            engine
                .put_row(12, &pk, &val, &[], WriteDurability::BufferedFast)
                .unwrap();
        }
        engine.flush_memtable().unwrap();
        // Overwrite a few and delete #44 in MemTable
        engine
            .put_row(
                12,
                &40u32.to_be_bytes(),
                b"mem_40",
                &[],
                WriteDurability::BufferedFast,
            )
            .unwrap();
        engine
            .delete_row(12, &44u32.to_be_bytes(), WriteDurability::BufferedFast)
            .unwrap();

        let mut pks = Vec::new();
        let mut slab = Vec::new();
        let mut spans = Vec::new();
        engine
            .scan_range_into_slab(
                12,
                RangeScanOptions {
                    lower: Some((&10u32.to_be_bytes(), true)),
                    upper: Some((&44u32.to_be_bytes(), true)),
                    reverse: true,
                    limit: 5,
                    keys_only: false,
                },
                SlabScanOutput {
                    pks: &mut pks,
                    slab: &mut slab,
                    spans: &mut spans,
                    lazy_blobs: None,
                },
            )
            .unwrap();
        assert_eq!(spans.len(), 5);
        // Expected top 5 keys in `[10, 44]` after deleting 44: 43, 42, 41, 40 (mem_40), 39
        let got_vals: Vec<String> = spans
            .iter()
            .map(|&(s, e)| String::from_utf8(slab[s..e].to_vec()).unwrap())
            .collect();
        assert_eq!(
            got_vals,
            vec!["seg2_43", "seg2_42", "seg2_41", "mem_40", "seg2_39"]
        );
        assert_eq!(
            pks.last().map(|v| v.as_slice()),
            Some(39u32.to_be_bytes().as_slice())
        );
    }

    // Reopen from disk and verify `may_have_row_meta` lazily detects `'t'` on table 11
    // and absence of `'t'` on table 10!
    {
        let mut engine = StorageEngine::open(fs, EngineConfig::default()).unwrap();
        assert!(!engine.may_have_row_meta(10).unwrap());
        assert!(engine.may_have_row_meta(11).unwrap());
        // Updating `sk1` on table 11 must clean up old secondary key `idx_a`
        engine
            .put_row(
                11,
                b"sk1",
                b"sv2",
                &[(1, b"idx_b".to_vec())],
                WriteDurability::BufferedFast,
            )
            .unwrap();
        assert!(
            engine
                .lookup_by_secondary_index(11, 1, b"idx_a")
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            engine.lookup_by_secondary_index(11, 1, b"idx_b").unwrap(),
            vec![b"sk1".to_vec()]
        );
    }
}

#[test]
fn secondary_index_streaming_known_absent_hint_and_batched_blob_relocation() {
    let dir = TempTestDir::new("sec_streaming_batched_blob_gc");
    let fs = Arc::new(OsFs::open(&dir.path, true).unwrap());

    let mut cfg = EngineConfig::default();
    cfg.inline_threshold_bytes = 128;
    cfg.max_blob_segment_bytes = 2048;

    let mut engine = StorageEngine::open(fs.clone(), cfg.clone()).unwrap();

    // 1. `put_row_owned_with_hint(..., known_absent: true)` skips redundant SSTable/MemTable
    //    probing on fresh inserts while maintaining secondary indexes properly.
    for i in 0..40u32 {
        let pk = format!("pk_{i:03}").into_bytes();
        let val = if i % 4 == 0 {
            vec![b'B'; 300] // Out-of-line blob (> 128 B)
        } else {
            format!("inline_val_{i:03}").into_bytes()
        };
        let sec_bucket = format!("bucket_{}", i % 3).into_bytes();
        engine
            .put_row_owned_with_hint(
                1,
                &pk,
                val,
                &[(1, sec_bucket)],
                WriteDurability::BufferedFast,
                true,
            )
            .unwrap();
    }
    engine.flush_memtable().unwrap();

    // Add more rows in MemTable so queries span both SSTable segment and MemTable
    for i in 40..60u32 {
        let pk = format!("pk_{i:03}").into_bytes();
        let val = format!("inline_val_{i:03}").into_bytes();
        let sec_bucket = format!("bucket_{}", i % 3).into_bytes();
        engine
            .put_row_owned_with_hint(
                1,
                &pk,
                val,
                &[(1, sec_bucket)],
                WriteDurability::BufferedFast,
                true,
            )
            .unwrap();
    }

    // 2. Chunked `scan_secondary_index_into_slab` (both `keys_only = true` and `keys_only = false`)
    let prefix = crate::lru::encode_secondary_index_exact_prefix(1, 1, b"bucket_0");
    let mut all_pks_keys_only = Vec::new();
    let mut cur_lo: Vec<u8> = prefix.clone();
    let mut cur_inc = true;
    loop {
        let mut chunk_pks = Vec::new();
        let mut slab = Vec::new();
        let mut spans = Vec::new();
        let next_opt = engine
            .scan_secondary_index_into_slab(
                1,
                1,
                SecondaryScanOptions {
                    prefix: &prefix,
                    lower_sec_key: (&cur_lo, cur_inc),
                    upper_sec_key: None,
                    limit: 7,
                    keys_only: true,
                },
                SlabScanOutput {
                    pks: &mut chunk_pks,
                    slab: &mut slab,
                    spans: &mut spans,
                    lazy_blobs: None,
                },
            )
            .unwrap();
        assert!(slab.is_empty() && spans.is_empty());
        all_pks_keys_only.extend(chunk_pks);
        match next_opt {
            Some(last_sec) => {
                cur_lo = last_sec;
                cur_inc = false;
            }
            None => break,
        }
    }
    assert_eq!(all_pks_keys_only.len(), 20); // 60 / 3 = 20 rows in bucket_0

    let mut all_rows_slab = Vec::new();
    let mut cur_lo2: Vec<u8> = prefix.clone();
    let mut cur_inc2 = true;
    loop {
        let mut pks = Vec::new();
        let mut slab = Vec::new();
        let mut spans = Vec::new();
        let next_opt = engine
            .scan_secondary_index_into_slab(
                1,
                1,
                SecondaryScanOptions {
                    prefix: &prefix,
                    lower_sec_key: (&cur_lo2, cur_inc2),
                    upper_sec_key: None,
                    limit: 6,
                    keys_only: false,
                },
                SlabScanOutput {
                    pks: &mut pks,
                    slab: &mut slab,
                    spans: &mut spans,
                    lazy_blobs: None,
                },
            )
            .unwrap();
        for (s, e) in spans {
            all_rows_slab.push(slab[s..e].to_vec());
        }
        match next_opt {
            Some(last_sec) => {
                cur_lo2 = last_sec;
                cur_inc2 = false;
            }
            None => break,
        }
    }
    assert_eq!(all_rows_slab.len(), 20);

    // 3. Verify batched WAL commit in `relocate_fragmented_std_blobs`.
    //    Create a sealed `blob_std` segment with 6 blobs, delete 4 of them (> 50% dead bytes),
    //    and run `relocate_fragmented_std_blobs`. All 2 surviving blobs in that segment must be
    //    relocated in a SINGLE batched WAL commit (`lsn` advances by 1 for the batch of relocated pointers!).
    for i in 0..6u32 {
        let pk = format!("frag_blob_{i}").into_bytes();
        engine
            .put_row(
                2,
                &pk,
                &vec![0xABu8; 300],
                &[],
                WriteDurability::BufferedFast,
            )
            .unwrap();
    }
    // Roll the active std writer by inserting one more blob into a new segment
    engine
        .put_row(
            2,
            b"frag_blob_roll",
            &vec![0xCDu8; 2000],
            &[],
            WriteDurability::BufferedFast,
        )
        .unwrap();
    engine.flush_memtable().unwrap();

    // Delete 4 of the 6 blobs in the sealed segment so dead_bytes * 2 >= file_len
    for i in 0..4u32 {
        let pk = format!("frag_blob_{i}").into_bytes();
        engine
            .delete_row(2, &pk, WriteDurability::BufferedFast)
            .unwrap();
    }
    engine.flush_memtable().unwrap();

    let lsn_before_reloc = engine.next_lsn();
    let relocated = engine
        .relocate_fragmented_std_blobs(0.5)
        .expect("relocate fragmented blobs");
    assert!(relocated >= 2);
    let lsn_after_reloc = engine.next_lsn();
    // Even though >= 2 live blobs were relocated across segments, `relocate_fragmented_std_blobs`
    // batches their `WalRecord::PutBlobPtr` updates into a single WAL batch per 512-key window!
    assert!(
        lsn_after_reloc - lsn_before_reloc <= 1,
        "relocating multiple blobs in a 512-key window must use at most 1 WAL batch commit"
    );
    assert_eq!(
        engine.get_row(2, b"frag_blob_4").unwrap().unwrap(),
        vec![0xABu8; 300]
    );
    assert_eq!(
        engine.get_row(2, b"frag_blob_5").unwrap().unwrap(),
        vec![0xABu8; 300]
    );
}

#[test]
fn background_mode_flush_only_marks_pending_and_install_after_new_flushes() {
    let tmp = TempTestDir::new("bg_install");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut engine = StorageEngine::open(fs.clone(), bg_cfg()).unwrap();

    for r in 0..4 {
        bg_put_round(&mut engine, r, 5, 64);
    }
    assert_eq!(
        engine.active_idx_segment_count(),
        4,
        "no inline compaction in background mode"
    );
    assert!(engine.compaction_pending());

    let job = engine.prepare_compaction().unwrap().expect("job");
    let merged = job.input_count();
    assert!(merged >= 2);

    // Newer segments + overwrites + a delete land while the job is "running".
    bg_put_round(&mut engine, 4, 5, 64);
    engine
        .delete_row(1, b"r000_000", WriteDurability::BufferedFast)
        .unwrap();
    engine
        .put_row(
            1,
            b"r001_000",
            b"overwritten",
            &[],
            WriteDurability::BufferedFast,
        )
        .unwrap();
    engine.flush_memtable().unwrap();
    let before = engine.active_idx_segment_count();
    assert_eq!(before, 6);

    let out = job.run().unwrap();
    assert!(engine.install_compaction(out).unwrap());
    assert_eq!(engine.active_idx_segment_count(), before - merged + 1);

    let check = |e: &mut StorageEngine<OsFs>| {
        assert!(e.get_row(1, b"r000_000").unwrap().is_none());
        assert_eq!(e.get_row(1, b"r001_000").unwrap().unwrap(), b"overwritten");
        for r in 0..5usize {
            for i in 0..5usize {
                if (r, i) == (0, 0) || (r, i) == (1, 0) {
                    continue;
                }
                let key = format!("r{r:03}_{i:03}");
                assert_eq!(
                    e.get_row(1, key.as_bytes()).unwrap().unwrap(),
                    vec![r as u8; 64]
                );
            }
        }
    };
    check(&mut engine);
    drop(engine);
    let mut reopened = StorageEngine::open(fs, bg_cfg()).unwrap();
    check(&mut reopened);
}

#[test]
fn overlapping_jobs_second_install_aborts_and_removes_output() {
    let tmp = TempTestDir::new("bg_abort");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut engine = StorageEngine::open(fs.clone(), bg_cfg()).unwrap();
    for r in 0..5 {
        bg_put_round(&mut engine, r, 4, 64);
    }
    let job1 = engine.prepare_compaction().unwrap().unwrap();
    let job2 = engine.prepare_compaction().unwrap().unwrap();
    assert_ne!(job1.output_seq_id(), job2.output_seq_id());
    let out1 = job1.run().unwrap();
    let out2 = job2.run().unwrap();
    let id2_name = crate::meta::format_idx_file_name(out2.output_seq_id());
    assert!(idx_file_names(&fs).contains(&id2_name));

    assert!(engine.install_compaction(out1).unwrap());
    assert!(
        !engine.install_compaction(out2).unwrap(),
        "inputs already replaced"
    );
    assert!(
        !idx_file_names(&fs).contains(&id2_name),
        "aborted output must be deleted"
    );

    // The aborted job's handles pinned the superseded inputs (undeletable on Windows) when
    // `install(out1)` queued them; the retry on the next meta commit unlinks them.
    bg_put_round(&mut engine, 5, 1, 64);
    let live = idx_file_names(&fs);
    assert_eq!(live.len(), engine.active_idx_segment_count());
    drop(engine);
    let mut reopened = StorageEngine::open(fs.clone(), bg_cfg()).unwrap();
    assert_eq!(idx_file_names(&fs), live, "reopen sweeps nothing live");
    for r in 0..5usize {
        for i in 0..4usize {
            let key = format!("r{r:03}_{i:03}");
            assert_eq!(
                reopened.get_row(1, key.as_bytes()).unwrap().unwrap(),
                vec![r as u8; 64]
            );
        }
    }
}

#[test]
fn suffix_job_keeps_tombstones_shadowing_older_segments() {
    let tmp = TempTestDir::new("bg_tombstone");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut engine = StorageEngine::open(fs.clone(), bg_cfg()).unwrap();
    // Large base segment, then tiny ones, the first of which deletes a base key.
    bg_put_round(&mut engine, 0, 600, 100);
    engine
        .delete_row(1, b"r000_007", WriteDurability::BufferedFast)
        .unwrap();
    engine.flush_memtable().unwrap();
    bg_put_round(&mut engine, 1, 1, 8);
    bg_put_round(&mut engine, 2, 1, 8);
    assert!(engine.compaction_pending());

    let job = engine.prepare_compaction().unwrap().unwrap();
    assert!(
        job.input_count() < engine.active_idx_segment_count(),
        "suffix only"
    );
    assert!(!job.drops_tombstones(), "suffix job must keep tombstones");
    let out = job.run().unwrap();
    assert!(engine.install_compaction(out).unwrap());

    assert!(
        engine.get_row(1, b"r000_007").unwrap().is_none(),
        "tombstone must still shadow base"
    );
    assert!(engine.get_row(1, b"r000_008").unwrap().is_some());
    drop(engine);
    let mut reopened = StorageEngine::open(fs, bg_cfg()).unwrap();
    assert!(reopened.get_row(1, b"r000_007").unwrap().is_none());
    assert!(reopened.get_row(1, b"r000_008").unwrap().is_some());
}

#[test]
fn uninstalled_compaction_output_is_swept_on_reopen() {
    let tmp = TempTestDir::new("bg_crash");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut engine = StorageEngine::open(fs.clone(), bg_cfg()).unwrap();
    for r in 0..4 {
        bg_put_round(&mut engine, r, 4, 64);
    }
    let live = idx_file_names(&fs);
    let job = engine.prepare_compaction().unwrap().unwrap();
    let out = job.run().unwrap();
    let orphan = crate::meta::format_idx_file_name(out.output_seq_id());
    assert!(idx_file_names(&fs).contains(&orphan));
    // "Crash": the output is never installed.
    drop(out);
    drop(engine);
    assert!(idx_file_names(&fs).contains(&orphan));

    let mut reopened = StorageEngine::open(fs.clone(), bg_cfg()).unwrap();
    assert_eq!(
        idx_file_names(&fs),
        live,
        "orphan swept, live segments untouched"
    );
    // A fresh allocation never reuses the orphan's id.
    bg_put_round(&mut reopened, 9, 2, 64);
    assert!(!idx_file_names(&fs).iter().any(|n| n == &orphan));
    assert!(reopened.get_row(1, b"r002_001").unwrap().is_some());
}

#[test]
fn backpressure_falls_back_to_inline_compaction() {
    let tmp = TempTestDir::new("bg_backpressure");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut cfg = bg_cfg();
    cfg.max_segments_before_inline_compaction = 6;
    let mut engine = StorageEngine::open(fs, cfg).unwrap();
    for r in 0..12 {
        bg_put_round(&mut engine, r, 3, 64);
        assert!(engine.active_idx_segment_count() <= 6);
    }
}

#[test]
fn sync_compact_still_works_with_shared_segments() {
    let tmp = TempTestDir::new("bg_sync");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut cfg = EngineConfig::default();
    cfg.auto_compact_segment_threshold = 4;
    let mut engine = StorageEngine::open(fs, cfg).unwrap();
    for r in 0..9 {
        bg_put_round(&mut engine, r, 3, 64);
    }
    assert!(engine.active_idx_segment_count() < 5);
    engine.compact().unwrap();
    assert_eq!(engine.active_idx_segment_count(), 1);
}

#[test]
fn full_scan_bypasses_block_cache_insertion() {
    let tmp = TempTestDir::new("scan_resistance_test");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut cfg = EngineConfig::default();
    cfg.block_cache_bytes = 64 * 1024;
    let mut engine = StorageEngine::open(fs, cfg).unwrap();

    for i in 0..50u32 {
        let k = format!("s_{i:04}").into_bytes();
        engine
            .put_row(1, &k, b"v", &[], WriteDurability::BufferedFast)
            .unwrap();
    }
    engine.flush_memtable().unwrap();
    let seg_id = engine.idx_segments[0].seq_id();

    // Verify cache has no blocks for this segment yet
    let mut block_buf = [0u8; 4096];
    assert!(!engine.block_cache().get_into(seg_id, 0, &mut block_buf));

    // Full unbounded table scan (lower: None, upper: None, limit: usize::MAX, keys_only: true)
    let mut pks = Vec::new();
    let mut slab = Vec::new();
    let mut spans = Vec::new();
    engine
        .scan_range_into_slab(
            1,
            RangeScanOptions {
                lower: None,
                upper: None,
                reverse: false,
                limit: usize::MAX,
                keys_only: true,
            },
            SlabScanOutput {
                pks: &mut pks,
                slab: &mut slab,
                spans: &mut spans,
                lazy_blobs: None,
            },
        )
        .unwrap();
    assert_eq!(pks.len(), 50);

    // Full table scan must NOT have inserted the blocks into VerifiedBlockCache!
    assert!(
        !engine.block_cache().get_into(seg_id, 0, &mut block_buf),
        "Full unbounded scan must bypass cache insertion to avoid cache pollution"
    );
}

#[test]
fn chunked_full_scan_bypasses_block_cache_while_secondary_lookup_populates_it() {
    let tmp = TempTestDir::new("chunked_scan_cache_policy");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut cfg = EngineConfig::default();
    cfg.block_cache_bytes = 64 * 1024;
    let mut engine = StorageEngine::open(fs, cfg).unwrap();

    for i in 0..600u32 {
        let k = format!("s_{i:04}").into_bytes();
        let tag = format!("tag_{i:04}").into_bytes();
        engine
            .put_row(
                1,
                &k,
                b"val_payload_padding",
                &[(1, tag)],
                WriteDurability::BufferedFast,
            )
            .unwrap();
    }
    engine.flush_memtable().unwrap();
    let seg_id = engine.idx_segments[0].seq_id();

    let mut block_buf = [0u8; 4096];
    assert!(!engine.block_cache().get_into(seg_id, 0, &mut block_buf));

    // Stream the entire table in 256-row chunks (mirroring vtab_filter / vtab_next).
    let mut total_pks = 0usize;
    let mut lower_pk: Option<Vec<u8>> = None;
    let (mut pks, mut slab, mut spans) = (Vec::new(), Vec::new(), Vec::new());
    loop {
        pks.clear();
        slab.clear();
        spans.clear();
        engine
            .scan_range_into_slab(
                1,
                RangeScanOptions {
                    lower: lower_pk.as_deref().map(|pk| (pk, false)),
                    upper: None,
                    reverse: false,
                    limit: 256,
                    keys_only: true,
                },
                SlabScanOutput {
                    pks: &mut pks,
                    slab: &mut slab,
                    spans: &mut spans,
                    lazy_blobs: None,
                },
            )
            .unwrap();
        total_pks += pks.len();
        if pks.len() < 256 {
            break;
        }
        lower_pk = pks.last().cloned();
    }
    assert_eq!(total_pks, 600);
    assert!(
        !engine.block_cache().get_into(seg_id, 0, &mut block_buf),
        "Chunked full-table scan must bypass block cache population"
    );

    // Exact secondary index lookup must populate the block cache and serve hits on repeat calls.
    let first = engine.lookup_by_secondary_index(1, 1, b"tag_0001").unwrap();
    assert_eq!(first, vec![b"s_0001".to_vec()]);
    let hits_after_1 = engine.block_cache().hits();
    let misses_after_1 = engine.block_cache().misses();

    let second = engine.lookup_by_secondary_index(1, 1, b"tag_0001").unwrap();
    assert_eq!(second, vec![b"s_0001".to_vec()]);
    assert!(
        engine.block_cache().hits() > hits_after_1,
        "Repeat secondary index lookup must hit VerifiedBlockCache"
    );
    assert_eq!(engine.block_cache().misses(), misses_after_1);
}

#[test]
fn background_install_with_dirty_memtable_does_not_persist_blob_accounting() {
    let tmp = TempTestDir::new("install_dirty_memtable");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut cfg = EngineConfig::default();
    cfg.max_blob_segment_bytes = 40 * 1024;
    let mut engine = StorageEngine::open(fs.clone(), cfg.clone()).unwrap();
    let v = vec![0xABu8; 16 * 1024];
    let put = |e: &mut StorageEngine<OsFs>, k: &[u8], v: &[u8]| {
        e.put_row(1, k, v, &[], WriteDurability::BufferedFast)
            .unwrap();
    };
    put(&mut engine, b"k1", &v);
    put(&mut engine, b"k2", &v);
    engine.flush_memtable().unwrap();
    put(&mut engine, b"k3", &v);
    engine.flush_memtable().unwrap();
    // The overwrite kills k1's blob, but only in the WAL; it is not checkpointed.
    put(&mut engine, b"k1", b"small");
    let job = engine.prepare_compaction().unwrap().unwrap();
    let out = job.run().unwrap();
    assert!(engine.install_compaction(out).unwrap());
    drop(engine);

    let mut engine = StorageEngine::open(fs, cfg).unwrap();
    assert_eq!(engine.get_row(1, b"k2").unwrap(), Some(v));
    assert_eq!(engine.get_row(1, b"k1").unwrap(), Some(b"small".to_vec()));
}

#[test]
fn orphan_segment_file_name_is_not_reused_after_crashed_flush() {
    let tmp = TempTestDir::new("orphan_idx_reuse");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let cfg = EngineConfig::default();
    let orphan_seq = {
        let mut engine = StorageEngine::open(fs.clone(), cfg.clone()).unwrap();
        engine
            .put_row(1, b"a", b"1", &[], WriteDurability::BufferedFast)
            .unwrap();
        engine.flush_memtable().unwrap();
        engine
            .put_row(1, b"b", b"2", &[], WriteDurability::BufferedFast)
            .unwrap();
        engine.meta_mgr().state().next_seq_id
    };
    // A crashed flush left an uncataloged segment, longer than any segment recovery will write.
    std::fs::write(
        tmp.path.join(crate::meta::format_idx_file_name(orphan_seq)),
        vec![0x5Au8; 64 * 1024],
    )
    .unwrap();

    drop(StorageEngine::open(fs.clone(), cfg.clone()).unwrap());

    let mut engine = StorageEngine::open(fs, cfg).unwrap();
    assert_eq!(engine.get_row(1, b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(engine.get_row(1, b"b").unwrap(), Some(b"2".to_vec()));
}

#[test]
fn checkpoint_with_empty_memtable_persists_blob_segment_lengths() {
    let tmp = TempTestDir::new("checkpoint_blob_len");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut cfg = EngineConfig::default();
    cfg.inline_threshold_bytes = 128;
    let wal = WriteDurability::BufferedFast;

    let mut engine = StorageEngine::open(fs.clone(), cfg.clone()).unwrap();
    engine.put_row(1, b"a", &vec![1u8; 4000], &[], wal).unwrap();
    engine.flush_memtable().unwrap();
    // A rolled-back blob write leaves bytes in the segment beyond the committed length while
    // the MemTable stays empty.
    engine.begin_transaction();
    engine.put_row(1, b"b", &vec![2u8; 4000], &[], wal).unwrap();
    engine.rollback_transaction().unwrap();
    assert!(engine.memtable.is_empty());
    let committed = engine.meta_mgr().state().std_blob_segments[0].file_len;
    let in_memory = engine.std_blobs.values().next().unwrap().file_len;
    assert!(in_memory > committed);

    engine.checkpoint().unwrap();
    let st = engine.meta_mgr().state();
    assert_eq!(st.std_blob_segments.len(), 1);
    assert_eq!(st.std_blob_segments[0].file_len, in_memory);
    let dead = st.std_blob_segments[0].dead_bytes;
    assert!(dead > 0);
    drop(engine);

    let mut engine = StorageEngine::open(fs, cfg).unwrap();
    let seg = &engine.meta_mgr().state().std_blob_segments[0];
    assert_eq!(seg.file_len, in_memory);
    assert_eq!(seg.dead_bytes, dead);
    assert_eq!(engine.get_row(1, b"a").unwrap(), Some(vec![1u8; 4000]));
    assert_eq!(engine.get_row(1, b"b").unwrap(), None);
}

#[test]
fn checkpoint_flushes_pending_memtable() {
    let tmp = TempTestDir::new("checkpoint_flush");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let cfg = EngineConfig::default();
    let mut engine = StorageEngine::open(fs.clone(), cfg.clone()).unwrap();
    engine
        .put_row(1, b"k", b"v", &[], WriteDurability::BufferedFast)
        .unwrap();
    engine.checkpoint().unwrap();
    assert!(engine.memtable.is_empty());
    assert_eq!(engine.meta_mgr().state().idx_segments.len(), 1);
    drop(engine);
    let mut engine = StorageEngine::open(fs, cfg).unwrap();
    assert!(engine.memtable.is_empty());
    assert_eq!(engine.get_row(1, b"k").unwrap(), Some(b"v".to_vec()));
}

#[test]
fn point_lookups_resolve_newest_segment_for_inline_blob_and_deleted_rows() {
    let tmp = TempTestDir::new("point_lookup_segments");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut cfg = EngineConfig::default();
    cfg.inline_threshold_bytes = 128;
    cfg.auto_compact_segment_threshold = 100;
    let wal = WriteDurability::BufferedFast;
    let big = |b: u8| vec![b; 1000];

    let mut engine = StorageEngine::open(fs, cfg).unwrap();
    engine.put_row(1, b"inline", b"v1", &[], wal).unwrap();
    engine.put_row(1, b"blob", &big(1), &[], wal).unwrap();
    engine.put_row(1, b"gone", b"v1", &[], wal).unwrap();
    engine.put_row(1, b"flip", b"small", &[], wal).unwrap();
    engine.flush_memtable().unwrap();
    engine.put_row(1, b"inline", b"v2", &[], wal).unwrap();
    engine.put_row(1, b"blob", &big(2), &[], wal).unwrap();
    engine.delete_row(1, b"gone", wal).unwrap();
    engine.put_row(1, b"flip", &big(3), &[], wal).unwrap();
    engine.flush_memtable().unwrap();
    engine.put_row(1, b"later", b"v1", &[], wal).unwrap();
    engine.flush_memtable().unwrap();
    assert!(engine.memtable.is_empty());
    assert!(engine.idx_segments.len() >= 3);

    let expected: [(&[u8], Option<Vec<u8>>); 6] = [
        (b"inline", Some(b"v2".to_vec())),
        (b"blob", Some(big(2))),
        (b"gone", None),
        (b"flip", Some(big(3))),
        (b"later", Some(b"v1".to_vec())),
        (b"absent", None),
    ];
    for (pk, want) in &expected {
        let mut slab = Vec::new();
        let mut spans = Vec::new();
        let hit = engine.get_row_into(1, pk, &mut slab, &mut spans).unwrap();
        assert_eq!(hit, want.is_some(), "{pk:?}");
        if let Some(want) = want {
            let (s, e) = spans[0];
            assert_eq!(&slab[s..e], want.as_slice(), "{pk:?}");
        }

        let mut slab = Vec::new();
        let mut spans = Vec::new();
        let mut lazy = Vec::new();
        let hit = engine
            .get_row_into_lazy(1, pk, &mut slab, &mut spans, &mut lazy)
            .unwrap();
        assert_eq!(hit, want.is_some(), "{pk:?}");
        if let Some(want) = want {
            match &lazy[0] {
                None => {
                    let (s, e) = spans[0];
                    assert_eq!(&slab[s..e], want.as_slice(), "{pk:?}");
                }
                Some(lb) => assert_eq!(lb.ptr.len as usize, 16 + 3 + pk.len() + want.len()),
            }
        }
        assert_eq!(engine.get_row(1, pk).unwrap(), *want, "{pk:?}");
    }
}

#[test]
fn ring_full_flushes_still_trigger_inline_compaction_outside_transactions() {
    ring_only_workload(false, false);
}

#[test]
fn ring_full_flushes_still_trigger_inline_compaction_for_transactions() {
    ring_only_workload(false, true);
}

#[test]
fn ring_full_flushes_still_wake_background_compaction_outside_transactions() {
    let (_e, hooks) = ring_only_workload(true, false);
    assert!(hooks > 0, "background compaction hook never fired");
}

#[test]
fn ring_full_flushes_still_wake_background_compaction_for_transactions() {
    let (_e, hooks) = ring_only_workload(true, true);
    assert!(hooks > 0, "background compaction hook never fired");
}

/// Auto compaction relocates live blobs out of fragmented segments by writing to the WAL. A
/// full-WAL rotation must not run it while its own batch holds an unappended LSN: the relocation
/// batch would take that LSN and the checkpoint would cover the batch, losing it on recovery.
#[test]
fn ring_full_rotation_does_not_relocate_blobs_under_the_held_lsn() {
    let fs = Arc::new(PowerLossFs::new());
    let mut cfg = ring_full_config();
    cfg.auto_compact_segment_threshold = 1000;
    cfg.max_blob_segment_bytes = 4096;
    let mut e = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
    let sync = WriteDurability::SyncTransactional;
    // Two blobs share the first blob segment, the third seals it; overwriting one leaves it
    // half dead, so a full compaction would relocate the other.
    e.put_row(1, b"a", &[1u8; 1500], &[], sync).unwrap();
    e.put_row(1, b"b", &[2u8; 1500], &[], sync).unwrap();
    e.put_row(1, b"c", &[3u8; 1500], &[], sync).unwrap();
    e.put_row(1, b"a", b"x", &[], sync).unwrap();
    e.flush_memtable().unwrap();
    let epoch = e.sync_wal().active_epoch();
    e.config.auto_compact_segment_threshold = 2;
    let mut i = 0;
    while e.sync_wal().current_sector_idx() + 2 < e.sync_wal().total_sectors() {
        let k = format!("fill{i:04}");
        e.put_row(2, k.as_bytes(), b"v", &[], sync).unwrap();
        i += 1;
    }
    assert_eq!(e.sync_wal().active_epoch(), epoch);
    // Keep writing until one write has to rotate the ring.
    let mut last = Vec::new();
    while e.sync_wal().active_epoch() == epoch {
        let k = format!("last{}", last.len());
        e.put_row(3, k.as_bytes(), b"v", &[], sync).unwrap();
        last.push(k);
        assert!(last.len() < 8, "the ring never rotated");
    }
    fs.power_loss();
    std::mem::forget(e);
    let mut e = StorageEngine::open(fs, cfg).unwrap();
    for k in &last {
        assert_eq!(e.get_row(3, k.as_bytes()).unwrap(), Some(b"v".to_vec()));
    }
    assert_eq!(e.get_row(1, b"b").unwrap(), Some(vec![2u8; 1500]));
}

/// A commit parked in the group commit is logged but not yet durable: a compaction install must
/// neither persist its dead bytes nor reclaim the blob segment they would empty.
#[test]
fn compaction_install_ignores_dead_bytes_of_commits_waiting_for_their_group_flush() {
    let tmp = TempTestDir::new("group_pending_blob_reclaim");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut cfg = EngineConfig::default();
    cfg.max_blob_segment_bytes = 2048;
    let mut e = StorageEngine::open(Arc::clone(&fs), cfg).unwrap();
    e.put_row(1, b"r1", &OLD_BLOB, &[], GROUP_SYNC).unwrap();
    e.put_row(1, b"r2", &OLD_BLOB, &[], GROUP_SYNC).unwrap();
    e.flush_memtable().unwrap();
    e.put_row(2, b"filler", b"v", &[], GROUP_SYNC).unwrap();
    e.flush_memtable().unwrap();
    let blobs = |fs: &OsFs| {
        fs.list_files()
            .unwrap()
            .into_iter()
            .filter(|f| f.starts_with("blob_"))
            .count()
    };
    assert_eq!(blobs(&fs), 2);

    e.begin_transaction_for_conn(1).unwrap();
    e.put_row(1, b"r1", b"new-inline", &[], GROUP_SYNC).unwrap();
    e.prepare_commit_transaction(GROUP_SYNC).unwrap();
    let CommitStep::AwaitingGroup { lsn, file, gate } = e.begin_commit_for_conn(1).unwrap() else {
        panic!("a plain synchronous transaction must join the group commit");
    };
    let ticket: GroupTicket = (lsn, file, gate);
    assert!(!e.pending_group.is_empty());

    let job = e.prepare_compaction().unwrap().expect("two idx segments");
    let out = job.run().unwrap();
    assert!(e.install_compaction(out).unwrap());
    assert_eq!(
        blobs(&fs),
        2,
        "the unflushed commit must not reclaim a blob"
    );

    flush_group(&mut e, &ticket);
    e.flush_memtable().unwrap();
    assert_eq!(
        blobs(&fs),
        1,
        "the segment is reclaimed once the commit is durable"
    );
    assert_eq!(e.get_row(1, b"r1").unwrap(), Some(b"new-inline".to_vec()));
}

#[test]
fn memtable_table_scan_batch_copies_only_the_batch() {
    let tmp = TempTestDir::new("scan_batch_copies");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut engine = StorageEngine::open(fs, EngineConfig::default()).unwrap();
    for i in 0..5000u32 {
        engine
            .put_row(
                1,
                &i.to_be_bytes(),
                b"v",
                &[],
                WriteDurability::BufferedFast,
            )
            .unwrap();
    }
    assert!(engine.idx_segments.is_empty());

    super::scan::TABLE_SCAN_ENTRIES_COPIED.with(|c| c.set(0));
    let rows = engine.scan_table_range(1, None, None, 128).unwrap();
    assert_eq!(rows.len(), 128);
    let copied = super::scan::TABLE_SCAN_ENTRIES_COPIED.with(|c| c.get());
    assert!(
        copied <= 128,
        "a 128-row batch copied {copied} of 5000 MemTable entries"
    );
}

#[test]
fn table_scan_batches_cover_the_table_despite_corrupt_rows_and_tombstones() {
    for flush in [false, true] {
        let tmp = TempTestDir::new("scan_batch_corrupt");
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
        let mut engine = StorageEngine::open(fs, EngineConfig::default()).unwrap();
        engine.set_table_corrupt_policy(1, CorruptPolicy::Evict);
        let big = vec![7u8; 4096];
        for i in 0..300u32 {
            engine
                .put_row(
                    1,
                    &i.to_be_bytes(),
                    &big,
                    &[],
                    WriteDurability::BufferedFast,
                )
                .unwrap();
        }
        for i in (0..300u32).step_by(7) {
            engine
                .delete_row(1, &i.to_be_bytes(), WriteDurability::BufferedFast)
                .unwrap();
        }
        if flush {
            engine.flush_memtable().unwrap();
        }
        // Rows 10..40 (not deleted ones) point to a blob segment that does not exist.
        let dangling = crate::wal::BlobPtr {
            kind: crate::wal::BlobKind::Standard,
            table_id: 0,
            seg_id: u64::MAX,
            offset: 0,
            len: 4096,
            crc32c: 0,
        };
        let mut expected = Vec::new();
        for i in 0..300u32 {
            if i % 7 == 0 {
                continue;
            }
            if (10..40).contains(&i) {
                let key = encode_primary_row_key(1, &i.to_be_bytes());
                engine
                    .memtable
                    .put_blob_ptr(key, dangling, 1_000_000 + i as u64);
            } else {
                expected.push(i.to_be_bytes().to_vec());
            }
        }
        for limit in [1, 7, 31, 32, 128, 1000] {
            assert_eq!(
                walk_table_in_batches(&mut engine, limit),
                expected,
                "flush={flush} limit={limit}"
            );
        }
        assert_eq!(
            engine.scan_table_range(1, None, None, 100).unwrap().len(),
            100,
            "limit counts readable rows"
        );
    }
}

#[test]
fn backfill_adds_entries_and_meta_to_a_row_without_either() {
    let (_tmp, _fs, _cfg, mut engine) = backfill_engine("backfill_fresh");
    engine
        .put_row(1, b"k", b"value", &[], WriteDurability::BufferedFast)
        .unwrap();
    assert!(row_meta(&mut engine, 1, b"k").is_none());
    let wrote = engine
        .backfill_secondary_keys(
            1,
            b"k",
            &[(UNIQ, b"u1".to_vec())],
            WriteDurability::BufferedFast,
        )
        .unwrap();
    assert!(wrote);
    let meta = row_meta(&mut engine, 1, b"k").unwrap();
    assert_eq!(meta.recorded_tick, 0);
    assert_eq!(meta.logical_size, 1 + 5);
    assert_eq!(meta.secondary_keys, vec![(UNIQ, b"u1".to_vec())]);
    assert_eq!(
        engine.lookup_by_secondary_index(1, UNIQ, b"u1").unwrap(),
        vec![b"k".to_vec()]
    );
    assert_eq!(engine.get_row(1, b"k").unwrap().unwrap(), b"value");
}

#[test]
fn backfill_is_idempotent_and_a_repeat_writes_nothing() {
    let (_tmp, _fs, _cfg, mut engine) = backfill_engine("backfill_repeat");
    engine
        .put_row(1, b"k", b"value", &[], WriteDurability::BufferedFast)
        .unwrap();
    let keys = [(UNIQ, b"u1".to_vec()), (UNIQ, b"u1".to_vec())];
    assert!(
        engine
            .backfill_secondary_keys(1, b"k", &keys, WriteDurability::BufferedFast)
            .unwrap()
    );
    let lsn = engine.next_lsn;
    assert!(
        !engine
            .backfill_secondary_keys(1, b"k", &keys, WriteDurability::BufferedFast)
            .unwrap()
    );
    assert!(
        !engine
            .backfill_secondary_keys(1, b"k", &[], WriteDurability::BufferedFast)
            .unwrap()
    );
    assert_eq!(engine.next_lsn, lsn, "a repeat must not log a batch");
    let meta = row_meta(&mut engine, 1, b"k").unwrap();
    assert_eq!(meta.secondary_keys, vec![(UNIQ, b"u1".to_vec())]);
    assert_eq!(live_keys(&engine, 1, NS_SECONDARY_INDEX).len(), 1);
}

#[test]
fn backfill_keeps_declared_index_entries_and_adds_only_the_missing_ones() {
    let (_tmp, _fs, _cfg, mut engine) = backfill_engine("backfill_declared");
    engine
        .put_row(
            1,
            b"k",
            b"value",
            &[(1, b"decl".to_vec())],
            WriteDurability::BufferedFast,
        )
        .unwrap();
    let wrote = engine
        .backfill_secondary_keys(
            1,
            b"k",
            &[(1, b"decl".to_vec()), (UNIQ, b"u".to_vec())],
            WriteDurability::BufferedFast,
        )
        .unwrap();
    assert!(wrote);
    let meta = row_meta(&mut engine, 1, b"k").unwrap();
    assert_eq!(
        meta.secondary_keys,
        vec![(1, b"decl".to_vec()), (UNIQ, b"u".to_vec())]
    );
    assert_eq!(live_keys(&engine, 1, NS_SECONDARY_INDEX).len(), 2);
    // An overwrite replaces both kinds of entry through the recorded meta.
    engine
        .put_row(
            1,
            b"k",
            b"value2",
            &[(1, b"decl2".to_vec())],
            WriteDurability::BufferedFast,
        )
        .unwrap();
    assert_eq!(
        live_keys(&engine, 1, NS_SECONDARY_INDEX),
        vec![encode_secondary_index_key(1, 1, b"decl2", b"k")]
    );
}

#[test]
fn backfill_on_absent_row_is_a_noop_and_corrupt_meta_is_an_error() {
    let (_tmp, _fs, _cfg, mut engine) = backfill_engine("backfill_absent_corrupt");
    assert!(
        !engine
            .backfill_secondary_keys(
                1,
                b"nope",
                &[(UNIQ, b"u".to_vec())],
                WriteDurability::BufferedFast
            )
            .unwrap()
    );
    assert!(live_keys(&engine, 1, NS_SECONDARY_INDEX).is_empty());

    engine
        .put_row(
            1,
            b"k",
            b"v",
            &[(1, b"a".to_vec())],
            WriteDurability::BufferedFast,
        )
        .unwrap();
    engine
        .commit_wal_and_memtable(
            vec![WalRecord::PutInline {
                key: encode_row_meta_key(1, b"k"),
                value: vec![1, 2, 3],
            }],
            WriteDurability::BufferedFast,
        )
        .unwrap();
    let lsn = engine.next_lsn;
    let err = engine
        .backfill_secondary_keys(
            1,
            b"k",
            &[(UNIQ, b"u".to_vec())],
            WriteDurability::BufferedFast,
        )
        .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    assert_eq!(engine.next_lsn, lsn);
    assert!(
        engine
            .lookup_by_secondary_index(1, UNIQ, b"u")
            .unwrap()
            .is_empty()
    );
}

#[test]
fn backfill_keeps_row_pointer_value_and_capped_tick() {
    let tmp = TempTestDir::new("backfill_capped");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut cfg = EngineConfig::default();
    cfg.inline_threshold_bytes = 256;
    let mut engine = StorageEngine::open(fs, cfg).unwrap();
    engine.register_capped_table(1, 4_000).unwrap();
    let big = vec![0x5Au8; 1000];
    engine
        .put_row(1, b"a", &big, &[], WriteDurability::BufferedFast)
        .unwrap();
    engine
        .put_row(1, b"b", b"small", &[], WriteDurability::BufferedFast)
        .unwrap();
    let row_entries = |engine: &StorageEngine<OsFs>| {
        [b"a", b"b"]
            .iter()
            .map(|pk| {
                engine
                    .lookup_raw_entry(&encode_primary_row_key(1, *pk))
                    .unwrap()
            })
            .collect::<Vec<_>>()
    };
    let rows_before = row_entries(&engine);
    let meta_before = row_meta(&mut engine, 1, b"a").unwrap();
    assert!(meta_before.recorded_tick > 0);
    let lru_before = live_keys(&engine, 1, NS_LRU_QUEUE);
    let state_before = engine.capped_table_state(1).unwrap();
    let (bytes_before, evict_before) = (
        state_before.current_logical_bytes,
        state_before.total_evictions,
    );

    for pk in [b"a", b"b"] {
        assert!(
            engine
                .backfill_secondary_keys(
                    1,
                    pk,
                    &[(UNIQ, pk.to_vec())],
                    WriteDurability::BufferedFast
                )
                .unwrap()
        );
    }
    assert_eq!(
        rows_before,
        row_entries(&engine),
        "row records (and blob pointers) are untouched"
    );
    let meta_after = row_meta(&mut engine, 1, b"a").unwrap();
    assert_eq!(meta_after.recorded_tick, meta_before.recorded_tick);
    assert_eq!(meta_after.logical_size, meta_before.logical_size);
    assert_eq!(live_keys(&engine, 1, NS_LRU_QUEUE), lru_before);
    let state_after = engine.capped_table_state(1).unwrap();
    assert_eq!(state_after.current_logical_bytes, bytes_before);
    assert_eq!(state_after.total_evictions, evict_before);

    // The tick is still what eviction orders by: "a" is the coldest and goes first, taking its
    // backfilled entry with it.
    for i in 0..4u8 {
        engine
            .put_row(1, &[b'z', i], &big, &[], WriteDurability::BufferedFast)
            .unwrap();
    }
    assert!(engine.get_row(1, b"a").unwrap().is_none());
    assert!(
        engine
            .lookup_by_secondary_index(1, UNIQ, b"a")
            .unwrap()
            .is_empty()
    );
}

#[test]
fn backfill_entries_go_with_delete_drop_and_rollback() {
    let (_tmp, _fs, _cfg, mut engine) = backfill_engine("backfill_removal");
    for pk in [b"a", b"b"] {
        engine
            .put_row(
                1,
                pk,
                b"v",
                &[(1, b"d".to_vec())],
                WriteDurability::BufferedFast,
            )
            .unwrap();
    }
    let u = |pk: &[u8]| vec![(UNIQ, pk.to_vec())];

    // Rollback removes entries and restores the meta.
    engine.begin_transaction();
    assert!(
        engine
            .backfill_secondary_keys(1, b"a", &u(b"a"), WriteDurability::BufferedFast)
            .unwrap()
    );
    assert_eq!(
        engine.lookup_by_secondary_index(1, UNIQ, b"a").unwrap(),
        vec![b"a".to_vec()]
    );
    engine.rollback_transaction().unwrap();
    assert!(
        engine
            .lookup_by_secondary_index(1, UNIQ, b"a")
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        row_meta(&mut engine, 1, b"a").unwrap().secondary_keys,
        vec![(1, b"d".to_vec())]
    );

    // A committed transaction keeps them.
    engine.begin_transaction();
    assert!(
        engine
            .backfill_secondary_keys(1, b"a", &u(b"a"), WriteDurability::BufferedFast)
            .unwrap()
    );
    engine.commit_transaction().unwrap();
    assert_eq!(
        engine.lookup_by_secondary_index(1, UNIQ, b"a").unwrap(),
        vec![b"a".to_vec()]
    );

    // Delete removes every entry of the row.
    assert!(
        engine
            .delete_row(1, b"a", WriteDurability::BufferedFast)
            .unwrap()
    );
    assert!(
        engine
            .lookup_by_secondary_index(1, UNIQ, b"a")
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        engine.lookup_by_secondary_index(1, 1, b"d").unwrap(),
        vec![b"b".to_vec()]
    );

    // Drop removes everything that is left.
    engine
        .backfill_secondary_keys(1, b"b", &u(b"b"), WriteDurability::BufferedFast)
        .unwrap();
    engine.drop_table(1, None).unwrap();
    for ns in [NS_PRIMARY_ROW, NS_SECONDARY_INDEX, NS_ROW_META] {
        assert!(live_keys(&engine, 1, ns).is_empty());
    }
}

#[test]
fn backfill_survives_flush_and_reopen_with_a_spilled_meta() {
    let (_tmp, fs, cfg, mut engine) = backfill_engine("backfill_reopen");
    engine
        .put_row(1, b"k", b"v", &[], WriteDurability::BufferedFast)
        .unwrap();
    engine
        .put_row(1, b"s", b"v", &[], WriteDurability::BufferedFast)
        .unwrap();
    engine
        .backfill_secondary_keys(
            1,
            b"k",
            &[(UNIQ, b"u".to_vec())],
            WriteDurability::SyncTransactional,
        )
        .unwrap();
    // Enough pairs to push the meta record out of line.
    let spill: Vec<(u16, Vec<u8>)> = (0..60u8).map(|i| (UNIQ + i as u16, vec![i; 90])).collect();
    assert!(
        engine
            .backfill_secondary_keys(1, b"s", &spill, WriteDurability::BufferedFast)
            .unwrap()
    );
    engine.flush_memtable().unwrap();
    drop(engine);

    let mut reopened = StorageEngine::open(fs, cfg).unwrap();
    assert_eq!(
        reopened.lookup_by_secondary_index(1, UNIQ, b"u").unwrap(),
        vec![b"k".to_vec()]
    );
    assert_eq!(
        row_meta(&mut reopened, 1, b"s").unwrap().secondary_keys,
        spill
    );
    assert_eq!(live_keys(&reopened, 1, NS_SECONDARY_INDEX).len(), 61);
    reopened
        .delete_row(1, b"s", WriteDurability::BufferedFast)
        .unwrap();
    assert_eq!(live_keys(&reopened, 1, NS_SECONDARY_INDEX).len(), 1);
}

#[test]
fn secondary_lookup_sees_the_owners_uncommitted_entries_and_tombstones() {
    for flushed in [false, true] {
        let tmp = TempTestDir::new("lookup_owner_view");
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
        let mut engine = StorageEngine::open(fs, EngineConfig::default()).unwrap();
        let sync = WriteDurability::SyncTransactional;
        engine
            .put_row(1, b"a", b"v", &[(0x8001, b"x".to_vec())], sync)
            .unwrap();
        if flushed {
            engine.flush_memtable().unwrap();
        }
        let look =
            |e: &StorageEngine<OsFs>, v: &[u8]| e.lookup_by_secondary_index(1, 0x8001, v).unwrap();
        engine.begin_transaction();
        // An entry deleted earlier in the same transaction is gone, a new one is visible.
        engine.delete_row(1, b"a", sync).unwrap();
        assert!(look(&engine, b"x").is_empty(), "flushed={flushed}");
        engine
            .put_row(1, b"b", b"v", &[(0x8001, b"x".to_vec())], sync)
            .unwrap();
        assert_eq!(look(&engine, b"x"), vec![b"b".to_vec()]);
        engine.savepoint(7);
        engine.delete_row(1, b"b", sync).unwrap();
        assert!(look(&engine, b"x").is_empty());
        engine.rollback_to_savepoint(7).unwrap();
        assert_eq!(look(&engine, b"x"), vec![b"b".to_vec()]);
        engine.rollback_transaction().unwrap();
        assert_eq!(look(&engine, b"x"), vec![b"a".to_vec()]);
    }
}

#[test]
fn install_compaction_discards_output_file_when_blob_sync_fails() {
    let fs = Arc::new(PowerLossFs::new());
    let mut cfg = EngineConfig::default();
    cfg.inline_threshold_bytes = 64;
    cfg.auto_compact_segment_threshold = 0;
    cfg.sync_wal_size_bytes = 64 * 4096;
    cfg.buf_wal_size_bytes = 64 * 4096;
    let mut engine = StorageEngine::open(Arc::clone(&fs), cfg).unwrap();

    engine
        .put_row(1, b"k1", &[0xAAu8; 200], &[], WriteDurability::BufferedFast)
        .unwrap();
    engine.flush_memtable().unwrap();
    engine
        .put_row(1, b"k2", &[0xBBu8; 200], &[], WriteDurability::BufferedFast)
        .unwrap();
    engine.flush_memtable().unwrap();

    // Leave unsynced bytes in `active_std_writer` with an empty MemTable via a rolled-back write.
    engine.begin_transaction();
    engine
        .put_row(
            1,
            b"rolled_back",
            &[0xCCu8; 200],
            &[],
            WriteDurability::BufferedFast,
        )
        .unwrap();
    engine.rollback_transaction().unwrap();

    let job = engine.prepare_compaction_at(0).unwrap().unwrap();
    let out_name = crate::meta::format_idx_file_name(job.output_seq_id());
    let out = job.run().unwrap();
    assert!(fs.list_files().unwrap().contains(&out_name));

    fs.arm_blob_sync_failure();
    let err = engine.install_compaction(out).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::Other);
    assert!(
        !fs.list_files().unwrap().contains(&out_name),
        "failed install_compaction must unlink {out_name} instead of leaking it"
    );
}

#[test]
fn prepare_compaction_and_wal_rotation_avoid_redundant_fsyncs() {
    let tmp = TempTestDir::new("pm1_avoid_redundant_fsyncs");
    let (fs, sync_counts, _) = open_counted(&tmp.path);

    let mut cfg = EngineConfig::default();
    cfg.auto_compact_segment_threshold = 2;
    cfg.background_compaction = true;
    cfg.inline_threshold_bytes = 64;
    cfg.max_blob_segment_bytes = 300;
    let mut engine = StorageEngine::open(Arc::clone(&fs), cfg).unwrap();

    // 1. Two idx segments -> prepare_compaction must not fsync boot_meta.db.
    engine
        .put_row(1, b"k1", b"v1", &[], WriteDurability::BufferedFast)
        .unwrap();
    engine.flush_memtable().unwrap();
    engine
        .put_row(1, b"k2", b"v2", &[], WriteDurability::BufferedFast)
        .unwrap();
    engine.flush_memtable().unwrap();
    sync_counts.lock().unwrap().clear();

    let job1 = engine.prepare_compaction().unwrap().unwrap();
    let job2 = engine.prepare_compaction().unwrap().unwrap();
    assert_ne!(job1.output_seq_id(), job2.output_seq_id());
    assert_eq!(
        sync_counts
            .lock()
            .unwrap()
            .get("boot_meta.db")
            .copied()
            .unwrap_or(0),
        0,
        "prepare_compaction must reserve SeqID in memory without committing boot_meta.db"
    );
    let out1 = job1.run().unwrap();
    assert!(engine.install_compaction(out1).unwrap());
    drop(job2);

    // 2. Buffered writes followed by flush_memtable advance `last_buf_synced_lsn`, so a later
    //    `SyncTransactional` commit does not redundantly fsync `wal_buf.db`.
    engine
        .put_row(1, b"buf_k", b"v_buf", &[], WriteDurability::BufferedFast)
        .unwrap();
    engine.flush_memtable().unwrap();
    sync_counts.lock().unwrap().clear();
    engine
        .put_row(
            1,
            b"sync_k",
            b"v_sync",
            &[],
            WriteDurability::SyncTransactional,
        )
        .unwrap();
    assert_eq!(
        sync_counts
            .lock()
            .unwrap()
            .get("wal_buf.db")
            .copied()
            .unwrap_or(0),
        0,
        "WAL rotation must advance last_buf_synced_lsn so SyncTransactional avoids wal_buf fsync"
    );

    // 3. Reclaiming a dead LRU segment after synchronous deletes skips `wal_buf.db` fsync.
    engine.register_capped_table(10, 1024 * 1024).unwrap();
    engine
        .put_row(
            10,
            b"l1",
            &[0x11u8; 200],
            &[],
            WriteDurability::SyncTransactional,
        )
        .unwrap();
    engine
        .put_row(
            10,
            b"l2",
            &[0x22u8; 200],
            &[],
            WriteDurability::SyncTransactional,
        )
        .unwrap();
    engine
        .delete_row(10, b"l1", WriteDurability::SyncTransactional)
        .unwrap();
    assert_eq!(engine.active_lru_blob_segment_count(10), 2);
    sync_counts.lock().unwrap().clear();

    engine.reclaim_fully_dead_lru_blob_segments(10).unwrap();
    assert_eq!(
        sync_counts
            .lock()
            .unwrap()
            .get("wal_buf.db")
            .copied()
            .unwrap_or(0),
        0,
        "reclaim_fully_dead_lru_blob_segments must not fsync wal_buf.db when already synced"
    );
    assert_eq!(engine.active_lru_blob_segment_count(10), 1);
}

#[test]
fn mem_only_secondary_scan_clamps_seek_below_prefix_like_merged_scan() {
    use crate::lru::{encode_secondary_index_key, encode_secondary_index_prefix};

    let mut cfg = EngineConfig::default();
    cfg.sync_wal_size_bytes = 64 * 4096;
    cfg.buf_wal_size_bytes = 64 * 4096;

    let run = |flush_to_idx: bool| {
        let dir = TempTestDir::new("scan_boundary_parity");
        let fs = Arc::new(OsFs::open(&dir.path, true).unwrap());
        let mut engine = StorageEngine::open(fs, cfg.clone()).unwrap();

        // Populate table 1 (lower table_id < table 2) AND two secondary index entries on table 2.
        engine
            .put_row(
                1,
                b"k_t1",
                b"v1",
                &[(1, b"a1".to_vec())],
                WriteDurability::BufferedFast,
            )
            .unwrap();
        engine
            .put_row(
                2,
                b"pk1",
                b"r1",
                &[(1, b"alpha".to_vec())],
                WriteDurability::BufferedFast,
            )
            .unwrap();
        engine
            .put_row(
                2,
                b"pk2",
                b"r2",
                &[(1, b"beta".to_vec())],
                WriteDurability::BufferedFast,
            )
            .unwrap();
        // Also ensure secondary key prefix on table 2 idx 2 exists (sorts after idx 1).
        engine
            .put_row(
                2,
                b"pk3",
                b"r3",
                &[(2, b"gamma".to_vec())],
                WriteDurability::BufferedFast,
            )
            .unwrap();

        if flush_to_idx {
            engine.flush_memtable().unwrap();
        }

        // Deliberately pass `seek_key` strictly smaller than `prefix` (pointing into table 1),
        // exercising `PreparedPrefixRange` clamping to `prefix` in both keys_only and full-row modes.
        let prefix = encode_secondary_index_prefix(2, 1);
        let below_prefix_seek = encode_secondary_index_key(1, 1, b"a", b"k");
        assert!(below_prefix_seek.as_slice() < prefix.as_slice());

        let mut pks_only = Vec::new();
        let mut slab_only = Vec::new();
        let mut spans_only = Vec::new();
        engine
            .scan_secondary_index_into_slab(
                2,
                1,
                SecondaryScanOptions {
                    prefix: &prefix,
                    lower_sec_key: (&below_prefix_seek, true),
                    upper_sec_key: None,
                    limit: 16,
                    keys_only: true,
                },
                SlabScanOutput {
                    pks: &mut pks_only,
                    slab: &mut slab_only,
                    spans: &mut spans_only,
                    lazy_blobs: None,
                },
            )
            .unwrap();

        let mut pks_rows = Vec::new();
        let mut slab_rows = Vec::new();
        let mut spans_rows = Vec::new();
        engine
            .scan_secondary_index_into_slab(
                2,
                1,
                SecondaryScanOptions {
                    prefix: &prefix,
                    lower_sec_key: (&below_prefix_seek, false),
                    upper_sec_key: None,
                    limit: 16,
                    keys_only: false,
                },
                SlabScanOutput {
                    pks: &mut pks_rows,
                    slab: &mut slab_rows,
                    spans: &mut spans_rows,
                    lazy_blobs: None,
                },
            )
            .unwrap();
        let rows: Vec<Vec<u8>> = spans_rows
            .into_iter()
            .map(|(s, e)| slab_rows[s..e].to_vec())
            .collect();

        // Check `lookup_by_secondary_index` parity too.
        let exact = engine.lookup_by_secondary_index(2, 1, b"beta").unwrap();
        (pks_only, rows, exact)
    };

    let mem_only = run(false);
    let idx_backed = run(true);
    assert_eq!(mem_only, idx_backed);
    assert_eq!(mem_only.0, vec![b"pk1".to_vec(), b"pk2".to_vec()]);
    assert_eq!(mem_only.1, vec![b"r1".to_vec(), b"r2".to_vec()]);
    assert_eq!(mem_only.2, vec![b"pk2".to_vec()]);
}

#[test]
fn empty_pk_resume_key_and_mem_only_prepopulated_limit_behave_consistently() {
    let tmp = TempTestDir::new("scan_empty_pk_and_limit");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut engine = StorageEngine::open(fs, EngineConfig::default()).unwrap();

    for (pk, val) in [
        (b"" as &[u8], b"v_empty" as &[u8]),
        (b"\x00", b"v_zero"),
        (b"a", b"v_a"),
    ] {
        engine
            .put_row(1, pk, val, &[], WriteDurability::BufferedFast)
            .unwrap();
    }

    // Check both Mem-only path and segment-merged path.
    for flushed in [false, true] {
        if flushed {
            engine.flush_memtable().unwrap();
        }
        let mut pks = Vec::new();
        let mut slab = Vec::new();
        let mut spans = Vec::new();
        engine
            .scan_range_into_slab(
                1,
                RangeScanOptions {
                    lower: None,
                    upper: None,
                    reverse: false,
                    limit: 1,
                    keys_only: false,
                },
                SlabScanOutput {
                    pks: &mut pks,
                    slab: &mut slab,
                    spans: &mut spans,
                    lazy_blobs: None,
                },
            )
            .unwrap();
        assert_eq!(spans.len(), 1, "flushed={flushed}");
        assert_eq!(&slab[spans[0].0..spans[0].1], b"v_empty");
        assert_eq!(
            pks,
            vec![Vec::<u8>::new()],
            "empty-PK resume key must be preserved (flushed={flushed})"
        );

        // Resume exclusively past empty PK and append into pre-populated slab/spans/pks.
        let last_pk = pks.pop().unwrap();
        engine
            .scan_range_into_slab(
                1,
                RangeScanOptions {
                    lower: Some((&last_pk, false)),
                    upper: None,
                    reverse: false,
                    limit: 1,
                    keys_only: false,
                },
                SlabScanOutput {
                    pks: &mut pks,
                    slab: &mut slab,
                    spans: &mut spans,
                    lazy_blobs: None,
                },
            )
            .unwrap();
        assert_eq!(
            spans.len(),
            2,
            "limit=1 on pre-populated output must append 1 new row (flushed={flushed})"
        );
        assert_eq!(&slab[spans[1].0..spans[1].1], b"v_zero");
        assert_eq!(pks, vec![b"\x00".to_vec()]);
    }
}

#[test]
fn reconcile_unique_keys_batch_respects_single_sector_wal_limit() {
    // R3-M5: on a 1-sector (4096 B) WAL, cumulative batch bytes between 4046 and 4096 must split
    // before overflowing the 4077 B fragment payload cap (`WriteZero`).
    let tmp = TempTestDir::new("r3_m5_unique_single_sector");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut cfg = EngineConfig::default();
    cfg.sync_wal_size_bytes = 4096;
    cfg.buf_wal_size_bytes = 4096;
    let mut engine = StorageEngine::open(fs, cfg).unwrap();

    // Build rows with ~300 B primary keys + 40 B unique component; ~370 B estimated per row so
    // accumulating hits 4046..=4096 before exceeding 4096 if clamped naively to 4096.
    let mut items = Vec::new();
    for i in 0..25u32 {
        let pk = format!("pk_{i:04}_{}", "k".repeat(290)).into_bytes();
        let comp = format!("uniq_{i:04}_{}", "u".repeat(30)).into_bytes();
        engine
            .put_row(1, &pk, b"row_val", &[], WriteDurability::BufferedFast)
            .unwrap();
        items.push((pk, b"row_val".len(), vec![(0x8001, comp)]));
    }

    let count = engine
        .reconcile_unique_keys_batch(1, &items, WriteDurability::SyncTransactional)
        .unwrap();
    assert_eq!(count, 25);
}
