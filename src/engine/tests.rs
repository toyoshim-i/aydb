use super::*;
use crate::pal::OsFs;
use crate::util::test_util::TempTestDir;
use std::path::PathBuf;
use std::sync::atomic::Ordering;

#[test]
fn capped_table_lazy_promotion_and_cascading_eviction_with_copyless_blob_gc() {
    let tmp = TempTestDir::new("capped_lru");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());

    let mut cfg = EngineConfig::default();
    cfg.inline_threshold_bytes = 256;
    cfg.max_blob_segment_bytes = 2048; // Force blob segment rolling every ~2 blobs

    let mut engine = StorageEngine::open(fs.clone(), cfg.clone()).unwrap();
    // Register Capped Table 1 with MAX_BYTES = 3,200 (fits at most 3 rows of ~1,000 B each)
    engine.register_capped_table(1, 3_200).unwrap();

    let payload = vec![0x42u8; 1000];
    // Insert k1, k2, k3 (each ~1,002 B -> total ~3,006 B <= 3,200 B)
    engine
        .put_row(
            1,
            b"k1",
            &payload,
            &[(10, b"tag_a".to_vec())],
            WriteDurability::BufferedFast,
        )
        .unwrap();
    engine
        .put_row(
            1,
            b"k2",
            &payload,
            &[(10, b"tag_b".to_vec())],
            WriteDurability::BufferedFast,
        )
        .unwrap();
    engine
        .put_row(
            1,
            b"k3",
            &payload,
            &[(10, b"tag_a".to_vec())],
            WriteDurability::BufferedFast,
        )
        .unwrap();

    // Read "k1" so it receives a newer in-memory AccessTick via CoalescedLazyPromotion!
    assert_eq!(engine.get_row(1, b"k1").unwrap().unwrap(), payload);

    // Now insert "k4" (~1,002 B) -> total would be ~4,008 B > 3,200 B!
    // Eviction scanner checks "k1" first, sees hot_tick > old_tick -> LAZY PROMOTES "k1"
    // and evicts "k2" instead!
    engine
        .put_row(
            1,
            b"k4",
            &payload,
            &[(10, b"tag_c".to_vec())],
            WriteDurability::BufferedFast,
        )
        .unwrap();

    assert!(
        engine.get_row(1, b"k1").unwrap().is_some(),
        "k1 was read recently and must survive via Lazy Promotion"
    );
    assert!(
        engine.get_row(1, b"k2").unwrap().is_none(),
        "k2 was the coldest un-accessed key and must be evicted"
    );
    assert!(
        engine
            .lookup_by_secondary_index(1, 10, b"tag_b")
            .unwrap()
            .is_empty(),
        "secondary index for evicted k2 must be cascade-deleted"
    );
    assert_eq!(
        engine.lookup_by_secondary_index(1, 10, b"tag_a").unwrap(),
        vec![b"k1".to_vec(), b"k3".to_vec()]
    );

    let st = engine.capped_table_state(1).unwrap();
    assert!(st.current_logical_bytes <= 3_200);
    assert_eq!(st.total_lazy_promotions, 1);
    assert_eq!(st.total_evictions, 1);

    // Insert k5, k6, k7 to push out k3, k1, k4 and verify older blob_lru segments are unlinked
    for k in [b"k5", b"k6", b"k7"] {
        engine
            .put_row(1, k, &payload, &[], WriteDurability::BufferedFast)
            .unwrap();
    }
    assert!(engine.get_row(1, b"k1").unwrap().is_none());
    assert!(engine.get_row(1, b"k3").unwrap().is_none());
    assert!(engine.get_row(1, b"k4").unwrap().is_none());
    assert!(engine.get_row(1, b"k7").unwrap().is_some());

    // Flush, compact, and reopen to verify full persistence, O(1) blob GC, & recovery
    engine.flush_memtable().unwrap();
    // Sealed blob segments whose entries were all evicted must have been reclaimed on flush
    assert!(
        engine.active_lru_blob_segment_count(1) <= 2,
        "sealed 100%-dead blob_lru segments must be unlinked automatically"
    );
    engine.compact().unwrap();
    drop(engine);

    let mut reopened = StorageEngine::open(fs, cfg).unwrap();
    assert!(reopened.get_row(1, b"k2").unwrap().is_none());
    assert_eq!(reopened.get_row(1, b"k7").unwrap().unwrap(), payload);
    assert!(
        reopened
            .capped_table_state(1)
            .unwrap()
            .current_logical_bytes
            <= 3_200
    );
}

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
fn multi_segment_range_scan_and_lru_eviction_skip_tombstone_windows() {
    let tmp = TempTestDir::new("tombstone_window");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut engine = StorageEngine::open(fs, EngineConfig::default()).unwrap();

    // Segment 1: 120 live rows k_000..k_119
    for i in 0..120u32 {
        let k = format!("k_{i:03}").into_bytes();
        let v = format!("val_{i:03}").into_bytes();
        engine
            .put_row(1, &k, &v, &[], WriteDurability::BufferedFast)
            .unwrap();
    }
    engine.flush_memtable().unwrap();

    // Segment 2: 100 tombstones shadowing k_000..k_099
    for i in 0..100u32 {
        let k = format!("k_{i:03}").into_bytes();
        engine
            .delete_row(1, &k, WriteDurability::BufferedFast)
            .unwrap();
    }
    engine.flush_memtable().unwrap();

    // Scan with limit = 5 must skip all 100 tombstones and return k_100..k_104
    let rows = engine.scan_table_range(1, None, None, 5).unwrap();
    assert_eq!(rows.len(), 5);
    for (idx, (pk, val)) in rows.iter().enumerate() {
        let expected_i = 100 + idx;
        assert_eq!(pk, &format!("k_{expected_i:03}").into_bytes());
        assert_eq!(val, &format!("val_{expected_i:03}").into_bytes());
    }

    let mut slab = Vec::new();
    let mut spans = Vec::new();
    engine
        .scan_table_values_into(1, None, None, 5, &mut slab, &mut spans)
        .unwrap();
    assert_eq!(spans.len(), 5);

    // Capped table 2: verify eviction succeeds even when >64 tombstones precede live LRU entries
    engine.register_capped_table(2, 100_000).unwrap();
    for i in 0..100u32 {
        let k = format!("lru_{i:03}").into_bytes();
        engine
            .put_row(2, &k, &[0xAB; 200], &[], WriteDurability::BufferedFast)
            .unwrap();
    }
    engine.flush_memtable().unwrap();

    // Delete first 80 keys (> EVICT_SCAN_WINDOW = 64) and flush so 80 tombstones sit at the head of 'l'
    for i in 0..80u32 {
        let k = format!("lru_{i:03}").into_bytes();
        engine
            .delete_row(2, &k, WriteDurability::BufferedFast)
            .unwrap();
    }
    engine.flush_memtable().unwrap();

    // Lower quota to 1,000 bytes (fits at most 4 rows of ~207 B) and verify eviction reclaims past the 80 tombstones
    engine.register_capped_table(2, 1_000).unwrap();
    let st = engine.capped_table_state(2).unwrap();
    assert!(
        st.current_logical_bytes <= 1_000,
        "eviction must advance past 80 tombstones and bring table within quota (got {})",
        st.current_logical_bytes
    );
}

#[test]
fn blob_segments_and_coalesced_wal_survive_reopen_before_flush_memtable() {
    let tmp = TempTestDir::new("blob_and_coalesced_reopen");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());

    let mut cfg = EngineConfig::default();
    cfg.buf_wal_mode = WalStreamMode::BufferedUserSpaceCoalesced;
    cfg.inline_threshold_bytes = 512;

    let large_blob = vec![0x5Au8; 2048];
    {
        let mut engine = StorageEngine::open(fs.clone(), cfg.clone()).unwrap();
        engine
            .put_row(1, b"blob_k1", &large_blob, &[], WriteDurability::BufferedFast)
            .unwrap();
        engine
            .put_row(
                1,
                b"inline_k2",
                b"small_tail_val",
                &[],
                WriteDurability::BufferedFast,
            )
            .unwrap();
        // Drop without calling flush_memtable()!
    }

    let mut reopened = StorageEngine::open(fs, cfg).unwrap();
    assert_eq!(reopened.get_row(1, b"blob_k1").unwrap(), Some(large_blob));
    assert_eq!(
        reopened.get_row(1, b"inline_k2").unwrap(),
        Some(b"small_tail_val".to_vec())
    );
}

#[test]
fn flush_memtable_io_fault_preserves_memtable_entries_across_retry() {
    use crate::pal::PreallocStrategy;
    use std::path::Path;
    use std::sync::atomic::AtomicBool;

    struct FaultFile<F> {
        inner: Arc<F>,
        name: String,
        fail_boot_meta_sync: Arc<AtomicBool>,
    }

    impl<F: PlatformFile> PlatformFile for FaultFile<F> {
        fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
            self.inner.read_at(buf, offset)
        }
        fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
            self.inner.read_exact_at(buf, offset)
        }
        fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
            if self.name == "boot_meta.db" && self.fail_boot_meta_sync.load(Ordering::SeqCst) {
                return Err(io::Error::new(
                    io::ErrorKind::StorageFull,
                    "simulated boot_meta write failure",
                ));
            }
            self.inner.write_all_at(buf, offset)
        }
        fn file_len(&self) -> io::Result<u64> {
            self.inner.file_len()
        }
        fn preallocate(&self, target_len: u64, strategy: PreallocStrategy) -> io::Result<()> {
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

    struct FaultFs {
        inner: OsFs,
        fail_boot_meta_sync: Arc<AtomicBool>,
    }

    impl PlatformFs for FaultFs {
        type File = FaultFile<<OsFs as PlatformFs>::File>;

        fn open_file(
            &self,
            rel_name: &str,
            create: bool,
            writable: bool,
            hint: OpenHint,
        ) -> io::Result<Arc<Self::File>> {
            let inner = self.inner.open_file(rel_name, create, writable, hint)?;
            Ok(Arc::new(FaultFile {
                inner,
                name: rel_name.to_string(),
                fail_boot_meta_sync: Arc::clone(&self.fail_boot_meta_sync),
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

    let tmp = TempTestDir::new("flush_fault_retry");
    let fail_flag = Arc::new(AtomicBool::new(false));
    let fs = Arc::new(FaultFs {
        inner: OsFs::open(&tmp.path, true).unwrap(),
        fail_boot_meta_sync: Arc::clone(&fail_flag),
    });

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

    // MemTable entries must remain intact in memory and survive subsequent retry
    fail_flag.store(false, Ordering::SeqCst);
    for i in 0..20u32 {
        let k = format!("k_{i:02}").into_bytes();
        let expected = format!("v_{i:02}").into_bytes();
        assert_eq!(engine.get_row(1, &k).unwrap(), Some(expected));
    }

    engine.flush_memtable().unwrap();
    drop(engine);

    let mut reopened = StorageEngine::open(fs, EngineConfig::default()).unwrap();
    for i in 0..20u32 {
        let k = format!("k_{i:02}").into_bytes();
        let expected = format!("v_{i:02}").into_bytes();
        assert_eq!(reopened.get_row(1, &k).unwrap(), Some(expected));
    }
}

#[test]
fn large_transaction_over_4mb_rollback_leaves_zero_leaked_rows() {
    let tmp = TempTestDir::new("large_tx_rollback");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut engine = StorageEngine::open(fs, EngineConfig::default()).unwrap();

    engine
        .put_row(1, b"pre_tx", b"baseline", &[], WriteDurability::BufferedFast)
        .unwrap();

    engine.begin_transaction();
    let payload = vec![0xAAu8; 1000];
    for i in 0..5_000u32 {
        let k = format!("tx_key_{i:05}").into_bytes();
        engine
            .put_row(1, &k, &payload, &[], WriteDurability::BufferedFast)
            .unwrap();
    }
    engine.rollback_transaction().unwrap();

    assert_eq!(
        engine.get_row(1, b"pre_tx").unwrap(),
        Some(b"baseline".to_vec())
    );
    for i in 0..5_000u32 {
        let k = format!("tx_key_{i:05}").into_bytes();
        assert!(engine.get_row(1, &k).unwrap().is_none());
    }

    // Also verify a large > 4 MB transaction COMMIT succeeds via 512 KB WAL chunking
    engine.begin_transaction();
    for i in 0..5_000u32 {
        let k = format!("commit_key_{i:05}").into_bytes();
        engine
            .put_row(1, &k, &payload, &[], WriteDurability::SyncTransactional)
            .unwrap();
    }
    engine.commit_transaction().unwrap();
    assert_eq!(
        engine.get_row(1, b"commit_key_00000").unwrap().as_ref(),
        Some(&payload)
    );
    assert_eq!(
        engine.get_row(1, b"commit_key_04999").unwrap().as_ref(),
        Some(&payload)
    );
}

#[test]
fn sealed_blob_overwrite_and_rollback_preserves_original_blob() {
    let tmp = TempTestDir::new("sealed_blob_rollback");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut cfg = EngineConfig::default();
    cfg.inline_threshold_bytes = 128;
    cfg.max_blob_segment_bytes = 4096;
    let mut engine = StorageEngine::open(fs, cfg).unwrap();

    let orig_blob = vec![0x11u8; 4096];
    let second_blob = vec![0x22u8; 4096];
    engine
        .put_row(1, b"k1", &orig_blob, &[], WriteDurability::BufferedFast)
        .unwrap();
    engine
        .put_row(1, b"k2", &second_blob, &[], WriteDurability::BufferedFast)
        .unwrap();

    engine.begin_transaction();
    engine
        .put_row(
            1,
            b"k1",
            &vec![0x99u8; 4096],
            &[],
            WriteDurability::BufferedFast,
        )
        .unwrap();
    engine.rollback_transaction().unwrap();

    assert_eq!(engine.get_row(1, b"k1").unwrap(), Some(orig_blob.clone()));
    assert_eq!(engine.get_row(1, b"k2").unwrap(), Some(second_blob));

    // Even after a subsequent flush_memtable(), k1's sealed blob segment must remain intact!
    engine
        .put_row(1, b"k3", b"inline", &[], WriteDurability::BufferedFast)
        .unwrap();
    engine.flush_memtable().unwrap();
    assert_eq!(engine.get_row(1, b"k1").unwrap(), Some(orig_blob));
}

#[test]
fn post_crash_sealed_blob_gc_and_partial_std_blob_relocation() {
    let tmp = TempTestDir::new("post_crash_blob_gc");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut cfg = EngineConfig::default();
    cfg.inline_threshold_bytes = 128;
    cfg.max_blob_segment_bytes = 16 * 1024;

    let blob = vec![0x77u8; 4000];
    {
        let mut engine = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
        for i in 0..8u32 {
            let k = format!("blob_key_{i:03}").into_bytes();
            engine
                .put_row(1, &k, &blob, &[], WriteDurability::BufferedFast)
                .unwrap();
        }
        engine.flush_memtable().unwrap();
    }

    // Delete all 4 blobs in Segment 1, then drop without flush_memtable (simulated crash before checkpoint)
    {
        let mut engine = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
        for i in 0..4u32 {
            let k = format!("blob_key_{i:03}").into_bytes();
            engine
                .delete_row(1, &k, WriteDurability::BufferedFast)
                .unwrap();
        }
    }

    // Reopen must succeed cleanly (Segment 1 was not prematurely unlinked before checkpoint),
    // and flushing now reclaims Segment 1!
    {
        let mut engine = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
        for i in 0..4u32 {
            let k = format!("blob_key_{i:03}").into_bytes();
            assert!(engine.get_row(1, &k).unwrap().is_none());
        }
        for i in 4..8u32 {
            let k = format!("blob_key_{i:03}").into_bytes();
            assert_eq!(engine.get_row(1, &k).unwrap(), Some(blob.clone()));
        }

        // Delete 3 of the 4 remaining blobs in Segment 2 (75% dead >= 50% threshold)
        for i in 4..7u32 {
            let k = format!("blob_key_{i:03}").into_bytes();
            engine
                .delete_row(1, &k, WriteDurability::BufferedFast)
                .unwrap();
        }
    }

    // Reopen after partial deletion (dead_bytes recovered from WAL replay), then compact()
    // to trigger `relocate_fragmented_std_blobs(0.5)` and reclaim the fragmented sealed segment!
    {
        let mut engine = StorageEngine::open(Arc::clone(&fs), cfg).unwrap();
        engine.flush_memtable().unwrap();
        engine.compact().unwrap();
        assert_eq!(engine.get_row(1, b"blob_key_007").unwrap(), Some(blob));
        let std_files: Vec<String> = fs
            .list_files()
            .unwrap()
            .into_iter()
            .filter(|f| f.starts_with("blob_std_"))
            .collect();
        assert_eq!(
            std_files.len(),
            1,
            "all old full/fragmented sealed blob_std segments must be reclaimed after relocation + compact"
        );
    }
}

#[test]
fn capped_table_logical_bytes_preserved_across_flush_reopen_and_rollback() {
    let tmp = TempTestDir::new("capped_reconcile");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let cfg = EngineConfig::default();

    {
        let mut engine = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
        // Insert rows before registering table as capped (tests reconciliation of SSTable rows lacking 't')
        engine
            .put_row(1, b"k1", &[0x11; 100], &[], WriteDurability::BufferedFast)
            .unwrap();
        engine.flush_memtable().unwrap();

        engine.register_capped_table(1, 1_000).unwrap();
        assert_eq!(
            engine.capped_table_state(1).unwrap().current_logical_bytes,
            102
        );

        engine
            .put_row(1, b"k2", &[0x22; 200], &[], WriteDurability::BufferedFast)
            .unwrap();
        assert_eq!(
            engine.capped_table_state(1).unwrap().current_logical_bytes,
            304
        );

        // Mutate inside a transaction and rollback -> logical bytes must revert to 304
        engine.begin_transaction();
        engine
            .put_row(1, b"k3", &[0x33; 300], &[], WriteDurability::BufferedFast)
            .unwrap();
        assert_eq!(
            engine.capped_table_state(1).unwrap().current_logical_bytes,
            606
        );
        engine.rollback_transaction().unwrap();
        assert_eq!(
            engine.capped_table_state(1).unwrap().current_logical_bytes,
            304
        );
    }

    // Reopen without an explicit flush_memtable() after k2 insert and verify quota & logical bytes
    let reopened = StorageEngine::open(fs, cfg).unwrap();
    let st = reopened.capped_table_state(1).unwrap();
    assert_eq!(st.max_bytes, 1_000);
    assert_eq!(st.current_logical_bytes, 304);
}

#[test]
fn writes_after_crash_recovery_survive_next_restart() {
    let tmp = TempTestDir::new("post_recovery_writes");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut cfg = EngineConfig::default();
    cfg.buf_wal_mode = WalStreamMode::BufferedUserSpaceCoalesced;

    let row_900 = vec![0x61u8; 900];
    // 1. Write 9 rows of 900 B in BufferedUserSpaceCoalesced mode:
    //    First 4 rows + First fragment of 5th row fill Sector 0 (flushed to OS),
    //    while the Last fragment of 5th row and rows 6..9 stay in user-space sector_buf!
    let mut engine1 = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
    for i in 0..9u32 {
        let k = format!("k_{i:02}").into_bytes();
        engine1
            .put_row(1, &k, &row_900, &[], WriteDurability::BufferedFast)
            .unwrap();
    }
    // Simulate sudden process crash (`kill -9`) leaving a dangling `First` fragment at the end of Sector 0
    engine1.buf_wal.discard_user_space_buffer_for_test();
    drop(engine1);

    // 2. Reopen (recovers first 4 rows and rotates WAL epochs), write 3 new rows, and close cleanly
    {
        let mut engine2 = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
        for i in 100..103u32 {
            let k = format!("post_rec_{i}").into_bytes();
            engine2
                .put_row(
                    1,
                    &k,
                    b"survived_post_recovery",
                    &[],
                    WriteDurability::BufferedFast,
                )
                .unwrap();
        }
    }

    // 3. Reopen again and verify all 3 post-recovery rows survived!
    let mut engine3 = StorageEngine::open(fs, cfg).unwrap();
    for i in 100..103u32 {
        let k = format!("post_rec_{i}").into_bytes();
        assert_eq!(
            engine3.get_row(1, &k).unwrap(),
            Some(b"survived_post_recovery".to_vec()),
            "post-recovery write {i} must survive subsequent restart"
        );
    }
}

#[test]
fn oversized_keys_rejected_and_large_inline_values_or_meta_routed_to_blob() {
    let tmp = TempTestDir::new("block_bounds");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut cfg = EngineConfig::default();
    cfg.inline_threshold_bytes = 8192; // Configured above 4 KB block capacity!
    let mut engine = StorageEngine::open(fs, cfg).unwrap();

    // 1. 5,000-byte primary key must be rejected with InvalidInput before polluting MemTable
    let err = engine
        .put_row(1, &[b'x'; 5000], b"v", &[], WriteDurability::BufferedFast)
        .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);

    // 2. 5,000-byte secondary index key must also be rejected with InvalidInput
    let err_sec = engine
        .put_row(
            1,
            b"ok_pk",
            b"v",
            &[(1, vec![b's'; 5000])],
            WriteDurability::BufferedFast,
        )
        .unwrap_err();
    assert_eq!(err_sec.kind(), io::ErrorKind::InvalidInput);

    // 3. 5,000-byte row value with `inline_threshold_bytes = 8192` must automatically route
    //    to a blob segment so `flush_memtable()` succeeds cleanly!
    let val_5000 = vec![0x55u8; 5000];
    engine
        .put_row(
            1,
            b"large_val_pk",
            &val_5000,
            &[],
            WriteDurability::BufferedFast,
        )
        .unwrap();

    // 4. Row with many secondary keys whose combined `RowMetaRecord` exceeds 4 KB must route `'t'` to blob
    let many_sec: Vec<(u16, Vec<u8>)> = (0..10u16)
        .map(|idx| (idx + 1, vec![b'a' + (idx as u8); 600]))
        .collect();
    engine
        .put_row(
            1,
            b"many_sec_pk",
            b"small_v",
            &many_sec,
            WriteDurability::BufferedFast,
        )
        .unwrap();

    engine
        .flush_memtable()
        .expect("flush_memtable must succeed without wedging");
    assert_eq!(engine.get_row(1, b"large_val_pk").unwrap(), Some(val_5000));
    assert_eq!(
        engine
            .lookup_by_secondary_index(1, 1, &vec![b'a'; 600])
            .unwrap(),
        vec![b"many_sec_pk".to_vec()]
    );
}

#[test]
fn multi_chunk_transaction_without_commit_marker_discards_all_chunks() {
    let tmp = TempTestDir::new("tx_framing");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());

    {
        let mut engine = StorageEngine::open(Arc::clone(&fs), EngineConfig::default()).unwrap();
        engine
            .put_row(
                1,
                b"baseline",
                b"keep_me",
                &[],
                WriteDurability::SyncTransactional,
            )
            .unwrap();

        // Write a transaction with `TxBegin` in chunk 1 directly to `wal_sync` without a `TxCommit` chunk
        let chunk1 = WalBatch::new(
            engine.next_lsn(),
            vec![
                WalRecord::TxBegin,
                WalRecord::PutInline {
                    key: encode_primary_row_key(1, b"uncommitted_chunk_key"),
                    value: vec![0xBB; 1024],
                },
            ],
        );
        engine.sync_wal.append_batch(&chunk1).unwrap();
    }

    let mut reopened = StorageEngine::open(fs, EngineConfig::default()).unwrap();
    assert_eq!(
        reopened.get_row(1, b"baseline").unwrap(),
        Some(b"keep_me".to_vec())
    );
    assert!(
        reopened
            .get_row(1, b"uncommitted_chunk_key")
            .unwrap()
            .is_none(),
        "incomplete multi-chunk transaction lacking TxCommit must be completely discarded on recovery"
    );
}

#[test]
fn crash_consistency_and_failed_write_rollback() {
    use crate::pal::PreallocStrategy;
    use std::path::Path;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;

    struct SimDiskFile {
        durable: Mutex<Vec<u8>>,
        cache: Mutex<Vec<u8>>,
        fail_wal_writes: Arc<AtomicBool>,
        is_wal: bool,
        is_sync_wal: bool,
    }

    impl PlatformFile for SimDiskFile {
        fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
            let c = self.cache.lock().unwrap();
            let off = offset as usize;
            if off >= c.len() {
                return Ok(0);
            }
            let n = (c.len() - off).min(buf.len());
            buf[..n].copy_from_slice(&c[off..off + n]);
            Ok(n)
        }
        fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
            let n = self.read_at(buf, offset)?;
            if n < buf.len() {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "eof"));
            }
            Ok(())
        }
        fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
            if self.is_wal && self.fail_wal_writes.load(Ordering::SeqCst) {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "simulated WAL write I/O failure",
                ));
            }
            let mut c = self.cache.lock().unwrap();
            let end = offset as usize + buf.len();
            if end > c.len() {
                c.resize(end, 0);
            }
            c[offset as usize..end].copy_from_slice(buf);
            if self.is_sync_wal {
                let mut d = self.durable.lock().unwrap();
                if end > d.len() {
                    d.resize(end, 0);
                }
                d[offset as usize..end].copy_from_slice(buf);
            }
            Ok(())
        }
        fn file_len(&self) -> io::Result<u64> {
            Ok(self.cache.lock().unwrap().len() as u64)
        }
        fn preallocate(&self, target_len: u64, _strategy: PreallocStrategy) -> io::Result<()> {
            let mut c = self.cache.lock().unwrap();
            let mut d = self.durable.lock().unwrap();
            let len = target_len as usize;
            if len > c.len() {
                c.resize(len, 0);
            }
            if len > d.len() {
                d.resize(len, 0);
            }
            Ok(())
        }
        fn sync_barrier(&self) -> io::Result<()> {
            self.sync_durable()
        }
        fn sync_durable(&self) -> io::Result<()> {
            let c = self.cache.lock().unwrap();
            *self.durable.lock().unwrap() = c.clone();
            Ok(())
        }
        fn evict_os_cache(&self, _offset: u64, _len: u64) -> io::Result<()> {
            Ok(())
        }
    }

    struct SimDiskFs {
        files: Mutex<HashMap<String, Arc<SimDiskFile>>>,
        fail_wal_writes: Arc<AtomicBool>,
        root: PathBuf,
    }

    impl SimDiskFs {
        fn new() -> Self {
            Self {
                files: Mutex::new(HashMap::new()),
                fail_wal_writes: Arc::new(AtomicBool::new(false)),
                root: PathBuf::from("/sim/crash_test"),
            }
        }
        fn simulate_power_loss(&self) {
            let files = self.files.lock().unwrap();
            for f in files.values() {
                let d = f.durable.lock().unwrap().clone();
                *f.cache.lock().unwrap() = d;
            }
        }
    }

    impl PlatformFs for SimDiskFs {
        type File = SimDiskFile;
        fn open_file(
            &self,
            rel_name: &str,
            create: bool,
            _writable: bool,
            hint: OpenHint,
        ) -> io::Result<Arc<Self::File>> {
            let mut files = self.files.lock().unwrap();
            if let Some(f) = files.get(rel_name) {
                return Ok(Arc::clone(f));
            }
            if !create {
                return Err(io::Error::new(io::ErrorKind::NotFound, "not found"));
            }
            let f = Arc::new(SimDiskFile {
                durable: Mutex::new(Vec::new()),
                cache: Mutex::new(Vec::new()),
                fail_wal_writes: Arc::clone(&self.fail_wal_writes),
                is_wal: rel_name.starts_with("wal_"),
                is_sync_wal: hint == OpenHint::MutableSyncLog,
            });
            files.insert(rel_name.to_string(), Arc::clone(&f));
            Ok(f)
        }
        fn remove_file(&self, rel_name: &str) -> io::Result<()> {
            self.files
                .lock()
                .unwrap()
                .remove(rel_name)
                .map(|_| ())
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "not found"))
        }
        fn list_files(&self) -> io::Result<Vec<String>> {
            Ok(self.files.lock().unwrap().keys().cloned().collect())
        }
        fn root_path(&self) -> &Path {
            &self.root
        }
    }

    let fs = Arc::new(SimDiskFs::new());
    let mut cfg = EngineConfig::default();
    cfg.inline_threshold_bytes = 128;
    cfg.max_blob_segment_bytes = 2048;

    let mut engine = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
    engine.register_capped_table(2, 4_000).unwrap();

    // Write blob_1 and blob_2 to Table 1 so blob_std_1 is sealed and checkpointed
    let orig_blob = vec![0x33u8; 2000];
    engine
        .put_row(1, b"r1", &orig_blob, &[], WriteDurability::SyncTransactional)
        .unwrap();
    engine
        .put_row(1, b"r2", &orig_blob, &[], WriteDurability::SyncTransactional)
        .unwrap();
    engine.flush_memtable().unwrap();

    // Failing a WAL write must NOT mutate `MemTable`!
    fs.fail_wal_writes.store(true, Ordering::SeqCst);
    assert!(
        engine
            .put_row(
                1,
                b"r1",
                b"failed_update",
                &[],
                WriteDurability::BufferedFast
            )
            .is_err()
    );
    assert_eq!(engine.get_row(1, b"r1").unwrap(), Some(orig_blob.clone()));

    // Failing commit_transaction must roll back MemTable!
    fs.fail_wal_writes.store(false, Ordering::SeqCst);
    engine.begin_transaction();
    engine
        .put_row(1, b"tx_fail_k", b"tx_val", &[], WriteDurability::BufferedFast)
        .unwrap();
    fs.fail_wal_writes.store(true, Ordering::SeqCst);
    assert!(engine.commit_transaction().is_err());
    fs.fail_wal_writes.store(false, Ordering::SeqCst);
    assert!(
        engine.get_row(1, b"tx_fail_k").unwrap().is_none(),
        "failed commit_transaction must roll back unlogged MemTable mutations"
    );

    // Overwrite `r1` with `BufferedFast` (making `blob_std_1` 100% dead in RAM),
    // then trigger `evict_until_within_quota` on capped Table 2, then simulate power loss!
    engine
        .put_row(
            1,
            b"r1",
            &vec![0x44u8; 2000],
            &[],
            WriteDurability::BufferedFast,
        )
        .unwrap();
    engine
        .put_row(2, b"cap_k", b"small", &[], WriteDurability::BufferedFast)
        .unwrap();

    // Write `idx_row` with a large out-of-line blob (> 128 B) using `BufferedFast` (`tag_buf`),
    // then overwrite `idx_row` with an inline value using `SyncTransactional` (`tag_sync`),
    // then simulate power loss!
    // Because active blob writers are synced before syncing `wal_buf.db` on the later
    // `SyncTransactional` inline write, the earlier buffered blob batch is NOT rejected on
    // recovery, preventing `tag_buf` from remaining as a stale secondary index entry!
    engine
        .put_row(
            1,
            b"idx_row",
            &vec![0x77u8; 512],
            &[(1, b"tag_buf".to_vec())],
            WriteDurability::BufferedFast,
        )
        .unwrap();
    engine
        .put_row(
            1,
            b"idx_row",
            b"v_sync_inline",
            &[(1, b"tag_sync".to_vec())],
            WriteDurability::SyncTransactional,
        )
        .unwrap();

    // Now write one more un-synced BufferedFast overwrite on `r3` and cut power!
    engine
        .put_row(
            1,
            b"r3_unsynced",
            b"lost",
            &[],
            WriteDurability::BufferedFast,
        )
        .unwrap();
    std::mem::forget(engine);
    fs.simulate_power_loss();

    let mut recovered = StorageEngine::open(Arc::clone(&fs), cfg).unwrap();
    // `tag_buf` must NOT remain as a dangling secondary index entry!
    assert!(
        recovered
            .lookup_by_secondary_index(1, 1, b"tag_buf")
            .unwrap()
            .is_empty(),
        "prefix durability on sync commit must prevent dangling secondary index from earlier buffered write"
    );
    assert_eq!(
        recovered
            .lookup_by_secondary_index(1, 1, b"tag_sync")
            .unwrap(),
        vec![b"idx_row".to_vec()]
    );
    // `r1` must still be readable (either new version if synced by prefix durability or original)!
    assert!(recovered.get_row(1, b"r1").unwrap().is_some());
}

#[test]
fn bounded_scan_quota_reject_and_stale_meta_cleanup() {
    let tmp = TempTestDir::new("bounded_scan_quota_meta");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut engine = StorageEngine::open(fs, EngineConfig::default()).unwrap();

    // Row larger than capped table `max_bytes` is rejected with `InvalidInput`
    // without evicting existing rows!
    engine.register_capped_table(1, 500).unwrap();
    engine
        .put_row(1, b"keep", &[0x11; 200], &[], WriteDurability::BufferedFast)
        .unwrap();
    let err = engine
        .put_row(
            1,
            b"too_big",
            &[0x22; 600],
            &[],
            WriteDurability::BufferedFast,
        )
        .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(
        engine.get_row(1, b"keep").unwrap(),
        Some(vec![0x11; 200]),
        "existing capped table rows must not be evicted when an oversized row is rejected"
    );

    // Overwriting an indexed row with a row that has no secondary keys on a non-capped
    // table deletes the stale `'t'` metadata record!
    engine
        .put_row(
            2,
            b"u7_pk",
            b"v1",
            &[(1, b"sec_old".to_vec())],
            WriteDurability::BufferedFast,
        )
        .unwrap();
    assert!(
        engine
            .get_row_meta_with_ptr(&encode_row_meta_key(2, b"u7_pk"))
            .unwrap()
            .is_some()
    );
    engine
        .put_row(
            2,
            b"u7_pk",
            b"v2_no_sec",
            &[],
            WriteDurability::BufferedFast,
        )
        .unwrap();
    assert!(
        engine
            .get_row_meta_with_ptr(&encode_row_meta_key(2, b"u7_pk"))
            .unwrap()
            .is_none(),
        "stale 't' metadata record must be deleted when overwritten without secondary keys"
    );

    // With 1 flushed segment on table 2 and 25,000 unrelated keys on table 3 in MemTable,
    // 200 secondary index lookups on table 2 must finish in < 250 ms (O(log N), not O(N))!
    engine
        .put_row(
            2,
            b"indexed_pk",
            b"val",
            &[(5, b"target_sec".to_vec())],
            WriteDurability::BufferedFast,
        )
        .unwrap();
    engine.flush_memtable().unwrap();

    for i in 0..25_000u32 {
        let k = format!("unrelated_{i:05}").into_bytes();
        engine
            .put_row(3, &k, b"v", &[], WriteDurability::BufferedFast)
            .unwrap();
    }

    let t0 = std::time::Instant::now();
    for _ in 0..200 {
        let pks = engine
            .lookup_by_secondary_index(2, 5, b"target_sec")
            .unwrap();
        assert_eq!(pks, vec![b"indexed_pk".to_vec()]);
    }
    assert!(
        t0.elapsed().as_millis() < 250,
        "200 secondary-index lookups must not drain 25,000 unrelated MemTable entries (took {:?})",
        t0.elapsed()
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
fn fast_undo_rollback_savepoints_and_zero_copy_blob_into() {
    let tmp = TempTestDir::new("undo_and_blob_into");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut cfg = EngineConfig::default();
    cfg.inline_threshold_bytes = 256;
    let mut engine = StorageEngine::open(fs, cfg).unwrap();

    // Populate 10,000 baseline rows in MemTable
    for i in 0..10_000u32 {
        let k = format!("base_{i:05}").into_bytes();
        engine
            .put_row(1, &k, b"orig_val", &[], WriteDurability::BufferedFast)
            .unwrap();
    }

    // 100 single-row transactions + rollbacks and nested savepoint rollbacks on top
    // of a 10,000-row MemTable must execute in O(tx mutations) (< 250 ms total), not O(WAL + MemTable)!
    let t0 = std::time::Instant::now();
    for i in 0..100u32 {
        let mod_key = format!("base_{i:05}").into_bytes();
        let ins_key = format!("temp_ins_{i:05}").into_bytes();
        let del_key = format!("base_{:05}", i + 100).into_bytes();

        engine.begin_transaction();
        engine
            .put_row(
                1,
                &mod_key,
                b"mutated_v1",
                &[],
                WriteDurability::BufferedFast,
            )
            .unwrap();
        engine
            .put_row(1, &ins_key, b"temp_val", &[], WriteDurability::BufferedFast)
            .unwrap();
        engine
            .delete_row(1, &del_key, WriteDurability::BufferedFast)
            .unwrap();

        engine.savepoint(1);
        engine
            .put_row(
                1,
                &mod_key,
                b"mutated_v2",
                &[],
                WriteDurability::BufferedFast,
            )
            .unwrap();
        engine.rollback_to_savepoint(1).unwrap();
        assert_eq!(
            engine.get_row(1, &mod_key).unwrap(),
            Some(b"mutated_v1".to_vec())
        );

        engine.rollback_transaction().unwrap();
        assert_eq!(
            engine.get_row(1, &mod_key).unwrap(),
            Some(b"orig_val".to_vec())
        );
        assert!(engine.get_row(1, &ins_key).unwrap().is_none());
        assert_eq!(
            engine.get_row(1, &del_key).unwrap(),
            Some(b"orig_val".to_vec())
        );
    }
    assert!(
        t0.elapsed().as_millis() < 250,
        "100 transaction & savepoint rollbacks must execute in O(tx mutations) via undo log (took {:?})",
        t0.elapsed()
    );

    // Verify `get_row_into` appends out-of-line blobs directly into `slab`
    let blob_a = vec![0xA1u8; 1024];
    let blob_b = vec![0xB2u8; 1536];
    engine
        .put_row(2, b"bk1", &blob_a, &[], WriteDurability::BufferedFast)
        .unwrap();
    engine
        .put_row(2, b"bk2", &blob_b, &[], WriteDurability::BufferedFast)
        .unwrap();

    let mut slab = Vec::with_capacity(4096);
    let mut spans = Vec::new();
    assert!(engine.get_row_into(2, b"bk1", &mut slab, &mut spans).unwrap());
    assert!(engine.get_row_into(2, b"bk2", &mut slab, &mut spans).unwrap());
    assert_eq!(spans.len(), 2);
    assert_eq!(&slab[spans[0].0..spans[0].1], blob_a.as_slice());
    assert_eq!(&slab[spans[1].0..spans[1].1], blob_b.as_slice());
}

#[test]
fn deferred_tx_checkpoint_blob_prefix_durability_and_prepare_commit() {
    let tmp = TempTestDir::new("deferred_tx_checkpoint_prepare");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut engine = StorageEngine::open(fs.clone(), EngineConfig::default()).unwrap();

    // Calling `flush_memtable()` and `compact()` inside an active transaction must NOT
    // publish uncommitted rows to SSTables, so `rollback_transaction()` completely removes them
    // both in memory and across reopen!
    engine.begin_transaction();
    engine
        .put_row(1, b"aborted", b"value", &[], WriteDurability::BufferedFast)
        .unwrap();
    engine.flush_memtable().unwrap();
    engine.compact().unwrap();
    engine.rollback_transaction().unwrap();
    assert_eq!(
        engine.get_row(1, b"aborted").unwrap(),
        None,
        "rolled-back row must not remain visible after flush_memtable() during transaction"
    );
    drop(engine);

    let mut engine = StorageEngine::open(fs.clone(), EngineConfig::default()).unwrap();
    assert_eq!(
        engine.get_row(1, b"aborted").unwrap(),
        None,
        "rolled-back row must not survive reopen after flush_memtable() during transaction"
    );

    // Verify that if the transaction DOES commit after `flush_memtable()` was requested inside
    // the transaction, the deferred flush executes upon commit and creates an SSTable segment!
    assert_eq!(engine.active_idx_segment_count(), 0);
    engine.begin_transaction();
    engine
        .put_row(
            1,
            b"committed_flushed",
            b"ok",
            &[],
            WriteDurability::BufferedFast,
        )
        .unwrap();
    engine.flush_memtable().unwrap();
    assert_eq!(
        engine.active_idx_segment_count(),
        0,
        "flush must be deferred while tx_depth > 0"
    );
    engine.commit_transaction().unwrap();
    assert_eq!(
        engine.active_idx_segment_count(),
        1,
        "deferred flush must execute upon commit_transaction()"
    );
    assert_eq!(
        engine.get_row(1, b"committed_flushed").unwrap(),
        Some(b"ok".to_vec())
    );

    // `prepare_commit_transaction` runs capped-table quota eviction while still inside
    // the undoable transaction, so `rollback_transaction()` after `prepare_commit_transaction`
    // restores the evicted row!
    engine.register_capped_table(5, 300).unwrap();
    engine
        .put_row(
            5,
            b"row1",
            &vec![0x11u8; 150],
            &[],
            WriteDurability::BufferedFast,
        )
        .unwrap();
    engine.begin_transaction();
    engine
        .put_row(
            5,
            b"row2",
            &vec![0x22u8; 200],
            &[],
            WriteDurability::BufferedFast,
        )
        .unwrap();
    // Prepare phase evicts `row1` to bring table 5 within its 300-byte quota
    engine
        .prepare_commit_transaction(WriteDurability::BufferedFast)
        .unwrap();
    assert!(engine.get_row(5, b"row1").unwrap().is_none());
    // If another 2PC participant fails and SQLite triggers rollback after xSync:
    engine.rollback_transaction().unwrap();
    assert_eq!(
        engine.get_row(5, b"row1").unwrap(),
        Some(vec![0x11u8; 150]),
        "rolling back after prepare_commit_transaction must restore quota-evicted rows"
    );
    assert!(engine.get_row(5, b"row2").unwrap().is_none());
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
            Some((b"base_key_0008", true)),
            Some((b"base_key_0012", true)),
            true,
            2,
            false,
            &mut rev_pks,
            &mut rev_slab,
            &mut rev_spans,
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
fn uncataloged_blob_recovery_inline_bounds_and_committed_read_isolation() {
    let tmp = TempTestDir::new("blob_recovery_and_isolation");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());

    // ---------------------------------------------------------------------
    // 1. Uncataloged blob segment created before a crash (or when
    //    `boot_meta.db` falls back to a prior slot) must NOT be deleted by
    //    premature orphan sweeping before WAL recovery validates its BlobPtr!
    //    Meanwhile, a truly unreferenced orphan blob segment MUST be swept
    //    after WAL recovery completes!
    // ---------------------------------------------------------------------
    let blob_payload = vec![0xA5u8; 2048];
    let unreferenced_orphan_blob = crate::meta::format_std_blob_file_name(999);
    {
        let mut engine = StorageEngine::open(fs.clone(), EngineConfig::default()).unwrap();
        engine
            .put_row(
                1,
                b"r01_blob_key",
                &blob_payload,
                &[],
                WriteDurability::SyncTransactional,
            )
            .unwrap();
        // Simulate a crash where `boot_meta.db` newest slot (which cataloged `blob_std_0001.db`)
        // suffered a sub-sector torn write, forcing `boot_meta.db` to fall back to the prior
        // slot where `std_blob_segments` was still empty, while `wal_sync.db` and `blob_std_0001.db`
        // are intact on disk!
        let active_slot = engine.meta_mgr.active_slot();
        engine
            .meta_mgr
            .file()
            .write_all_at(&[0xFF], active_slot.byte_offset() + 4095)
            .unwrap();
        // Also plant an unreferenced orphan blob file on disk to verify post-recovery sweep
        let _orphan = fs
            .open_file(
                &unreferenced_orphan_blob,
                true,
                true,
                OpenHint::ImmutableSequential,
            )
            .unwrap();
    }

    {
        let mut engine = StorageEngine::open(fs.clone(), EngineConfig::default()).unwrap();
        assert_eq!(
            engine.get_row(1, b"r01_blob_key").unwrap(),
            Some(blob_payload.clone()),
            "committed blob row in uncataloged blob segment must survive recovery"
        );
        assert!(
            !fs.list_files().unwrap().contains(&unreferenced_orphan_blob),
            "truly unreferenced orphan blob segment must be swept after WAL recovery"
        );

        // -----------------------------------------------------------------
        // 2. Multiple updates/deletes to the same key within a single
        //    explicit transaction (`WalBatch`) must NOT double-count the
        //    pre-transaction SSTable blob's `dead_bytes` during WAL recovery,
        //    which previously caused `dead_bytes >= file_len` to unlink a
        //    blob segment that still held other live blobs!
        // -----------------------------------------------------------------
        engine
            .put_row(
                1,
                b"r02_live_neighbor",
                &vec![0x11u8; 2048],
                &[],
                WriteDurability::SyncTransactional,
            )
            .unwrap();
        engine
            .put_row(
                1,
                b"r02_mutated_twice",
                &vec![0x22u8; 2048],
                &[],
                WriteDurability::SyncTransactional,
            )
            .unwrap();
        engine.flush_memtable().unwrap();

        // Now in a single transaction (single WalBatch), update `r02_mutated_twice`
        // to inline and then delete it (2 mutations to the same key in 1 WalBatch!).
        engine.begin_transaction();
        engine
            .put_row(
                1,
                b"r02_mutated_twice",
                b"inline_mid",
                &[],
                WriteDurability::SyncTransactional,
            )
            .unwrap();
        engine
            .delete_row(1, b"r02_mutated_twice", WriteDurability::SyncTransactional)
            .unwrap();
        engine.commit_transaction().unwrap();
        // Drop without flushing so WAL recovery replays the multi-op WalBatch!
    }

    {
        let mut engine = StorageEngine::open(fs.clone(), EngineConfig::default()).unwrap();
        // Delete `r01_blob_key` as well so if `r02_mutated_twice` was double-counted,
        // `dead_bytes` would reach `>= file_len` even though `r02_live_neighbor` is still live!
        engine
            .delete_row(1, b"r01_blob_key", WriteDurability::SyncTransactional)
            .unwrap();
        engine.flush_memtable().unwrap();
        engine.compact().unwrap();
        assert_eq!(
            engine.get_row(1, b"r02_live_neighbor").unwrap(),
            Some(vec![0x11u8; 2048]),
            "live neighbor blob must not be unlinked due to double-counted dead_bytes"
        );
        assert_eq!(engine.get_row(1, b"r02_mutated_twice").unwrap(), None);

        // -----------------------------------------------------------------
        // 3. Inline block footprint boundary test!
        //    When `row_key.len() + value.len() == 4074` (`13 + 4074 == 4087 > 4086`),
        //    `IndexEntry::inline_fits_in_block` is `false`, so with a large
        //    `inline_threshold_bytes` the engine MUST route it to a blob segment
        //    instead of wedging `flush_memtable()`!
        // -----------------------------------------------------------------
        engine.config.inline_threshold_bytes = 8192;
        let pk_r03 = vec![b'k'; 100]; // `row_key.len() == 103`
        // `4086 - 13 - 103 = 3970` fits inline; `3971` (`13 + 103 + 3971 = 4087`) must spill to blob!
        let val_3970 = vec![0x30u8; 3970];
        let val_3971 = vec![0x31u8; 3971];
        engine
            .put_row(1, &pk_r03, &val_3970, &[], WriteDurability::BufferedFast)
            .unwrap();
        engine
            .flush_memtable()
            .expect("3970B value at exact 4086B limit must flush inline");
        assert_eq!(engine.get_row(1, &pk_r03).unwrap(), Some(val_3970));

        engine
            .put_row(1, &pk_r03, &val_3971, &[], WriteDurability::BufferedFast)
            .unwrap();
        engine
            .flush_memtable()
            .expect("3971B value (4087B inline footprint) must spill to blob and not wedge flush_memtable");
        assert_eq!(engine.get_row(1, &pk_r03).unwrap(), Some(val_3971));
        engine.config.inline_threshold_bytes = DEFAULT_INLINE_THRESHOLD;

        // -----------------------------------------------------------------
        // 4. Unreferenced uncommitted tail bytes in a blob segment
        //    after a crash between blob append and WAL write must be credited
        //    to `dead_bytes` on recovery so $O(1)$ segment reclamation works!
        // -----------------------------------------------------------------
        engine.register_capped_table(4, 10_000).unwrap();
        engine
            .put_row(
                4,
                b"lru_k1",
                &vec![0x44u8; 2048],
                &[],
                WriteDurability::BufferedFast,
            )
            .unwrap();
        engine.flush_memtable().unwrap();
        let (&(_, lru_seg_id), _) = engine
            .lru_blobs
            .iter()
            .find(|&(&(tid, _), _)| tid == 4)
            .unwrap();
        let lru_fname = crate::meta::format_lru_blob_file_name(4, lru_seg_id);
        drop(engine);
        let f = fs
            .open_file(&lru_fname, false, true, OpenHint::ImmutableSequential)
            .unwrap();
        let orig_len = f.file_len().unwrap();
        // Append 4096 unreferenced tail bytes (simulating a crash after blob write before WAL write)
        f.write_all_at(&vec![0xEEu8; 4096], orig_len).unwrap();
        f.sync_durable().unwrap();
    }

    {
        let mut engine = StorageEngine::open(fs.clone(), EngineConfig::default()).unwrap();
        // Now delete `lru_k1`; because recovery accounted for the 4096 unreferenced tail bytes,
        // `dead_bytes >= file_len` is satisfied and `reclaim_fully_dead_lru_blob_segments`
        // reclaims the segment in O(1)!
        engine
            .delete_row(4, b"lru_k1", WriteDurability::BufferedFast)
            .unwrap();
        engine.reclaim_fully_dead_lru_blob_segments(4).unwrap();
        assert_eq!(
            engine.active_lru_blob_segment_count(4),
            0,
            "blob segment with uncommitted tail bytes must still be reclaimed in O(1) once live blobs are deleted"
        );

        // -----------------------------------------------------------------
        // 5. `register_or_resolve_vtab` inside an active transaction
        //    must route through transactional buffering so `rollback_transaction`
        //    cleanly undoes the catalog entry and rewinds `next_lsn` without
        //    leaving duplicate LSNs in the WAL!
        // -----------------------------------------------------------------
        let lsn_before = engine.next_lsn();
        engine.begin_transaction();
        let tid_aborted = engine
            .register_or_resolve_vtab("aborted_vtab", Some(77))
            .unwrap();
        assert_eq!(tid_aborted, 77);
        engine.rollback_transaction().unwrap();
        assert_eq!(
            engine.next_lsn(),
            lsn_before,
            "rolling back transaction after register_or_resolve_vtab must restore next_lsn"
        );
        assert!(
            !engine.active_vtab_ids.contains_key(&77),
            "rolled-back vtab registration must not remain in active_vtab_ids"
        );
        // Subsequent write at `lsn_before` must succeed and survive recovery cleanly
        engine
            .put_row(
                1,
                b"after_r05",
                b"ok",
                &[],
                WriteDurability::SyncTransactional,
            )
            .unwrap();

        // -----------------------------------------------------------------
        // 6. `drop_table` inside an explicit transaction (`BEGIN ... COMMIT`)
        //    must reclaim 100%-dead LRU blob segments and remove the table quota
        //    from `boot_meta.db` upon `commit_transaction()`!
        // -----------------------------------------------------------------
        engine.register_capped_table(6, 50_000).unwrap();
        engine
            .put_row(
                6,
                b"cap6_k1",
                &vec![0x66u8; 2048],
                &[],
                WriteDurability::BufferedFast,
            )
            .unwrap();
        assert_eq!(engine.active_lru_blob_segment_count(6), 1);

        engine.begin_transaction();
        engine.drop_table(6, Some("cap6_table")).unwrap();
        engine.commit_transaction().unwrap();
        assert_eq!(
            engine.active_lru_blob_segment_count(6),
            0,
            "committing DROP TABLE on a capped table must reclaim its 100%-dead LRU blob segments"
        );
        assert!(
            engine
                .meta_mgr
                .state()
                .table_quotas
                .iter()
                .all(|q| q.table_id != 6),
            "committing DROP TABLE on a capped table must remove its quota from boot_meta.db"
        );

        // -----------------------------------------------------------------
        // 7. Concurrent read via `with_committed_read_for_conn` when
        //    `LruReadTrackingMode::EagerWriteOnRead` is enabled while another
        //    connection holds an active transaction must NOT corrupt the
        //    active transaction's undo log or MemTable!
        // -----------------------------------------------------------------
        engine.config.lru_read_tracking = LruReadTrackingMode::EagerWriteOnRead;
        engine.register_capped_table(7, 50_000).unwrap();
        engine
            .put_row(
                7,
                b"shared_k",
                b"committed_v1",
                &[],
                WriteDurability::BufferedFast,
            )
            .unwrap();

        // Connection 1 begins a transaction and overwrites `shared_k` -> `uncommitted_v2`
        engine.begin_transaction_for_conn(101).unwrap();
        engine
            .put_row(
                7,
                b"shared_k",
                b"uncommitted_v2",
                &[],
                WriteDurability::BufferedFast,
            )
            .unwrap();
        let undo_len_before = engine.tx_memtable_undo.len();

        // Connection 2 reads `shared_k` via `with_committed_read_for_conn(202, ...)`
        let conn2_val = engine
            .with_committed_read_for_conn(202, |eng| eng.get_row(7, b"shared_k"))
            .unwrap();
        assert_eq!(conn2_val, Some(b"committed_v1".to_vec()));
        assert_eq!(
            engine.tx_memtable_undo.len(),
            undo_len_before,
            "concurrent committed read under EagerWriteOnRead must not append to Connection 1's tx_memtable_undo"
        );

        // Connection 1 must still see its uncommitted write `uncommitted_v2`, and rolling back
        // Connection 1 must cleanly restore `committed_v1`!
        assert_eq!(
            engine.get_row(7, b"shared_k").unwrap(),
            Some(b"uncommitted_v2".to_vec())
        );
        engine.rollback_transaction_for_conn(101).unwrap();
        assert_eq!(
            engine.get_row(7, b"shared_k").unwrap(),
            Some(b"committed_v1".to_vec()),
            "rolling back Connection 1 after Connection 2's EagerWriteOnRead read must restore committed_v1"
        );
    }
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
                Some((&10u32.to_be_bytes(), true)),
                Some((&44u32.to_be_bytes(), true)),
                true,
                5,
                false,
                &mut pks,
                &mut slab,
                &mut spans,
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
fn tx_capped_quota_persistence_and_sync_catalog_durability() {
    let dir = TempTestDir::new("tx_capped_quota_sync_cat");
    let fs = Arc::new(OsFs::open(&dir.path, true).unwrap());

    let assigned_id = {
        let mut engine = StorageEngine::open(fs.clone(), EngineConfig::default()).unwrap();

        // 1. Register a capped table inside an explicit transaction (`tx_depth > 0`),
        //    verify that rolling back a savepoint undoes a tentative registration, and
        //    committing persists `table_quotas` to `boot_meta.db` even without a MemTable flush!
        engine.begin_transaction();
        engine.register_capped_table(42, 128_000).unwrap();
        engine.savepoint(1);
        engine.register_capped_table(99, 64_000).unwrap();
        engine.rollback_to_savepoint(1).unwrap();
        engine.release_savepoint(1).unwrap();
        engine
            .put_row(42, b"k1", b"val1", &[], WriteDurability::BufferedFast)
            .unwrap();
        engine.commit_transaction().unwrap();
        assert_eq!(
            engine.active_idx_segment_count(),
            0,
            "precondition: no MemTable flush has occurred yet"
        );

        // 2. `register_or_resolve_vtab_with_durability(..., SyncTransactional)` must
        //    commit the catalog record to `sync_wal` (`current_sector_idx() > 0`)!
        let sync_sectors_before = engine.sync_wal.current_sector_idx();
        let id = engine
            .register_or_resolve_vtab_with_durability(
                "sync_cat_tbl",
                None,
                WriteDurability::SyncTransactional,
            )
            .unwrap();
        assert!(
            engine.sync_wal.current_sector_idx() > sync_sectors_before,
            "SyncTransactional catalog registration must append to sync_wal"
        );
        id
    };

    // Reopen from disk and verify:
    // - Table 42's `max_bytes = 128_000` quota is present in `boot_meta.db`,
    // - Rolled-back table 99 is NOT present,
    // - `"sync_cat_tbl"` resolves to the same `assigned_id`.
    {
        let mut engine = StorageEngine::open(fs, EngineConfig::default()).unwrap();
        let st42 = engine
            .capped_table_state(42)
            .expect("capped table 42 quota must survive reopen after tx commit");
        assert_eq!(st42.max_bytes, 128_000);
        assert!(
            engine.capped_table_state(99).is_none(),
            "rolled-back capped table 99 must not be persisted"
        );
        let resolved_again = engine
            .register_or_resolve_vtab_with_durability(
                "sync_cat_tbl",
                None,
                WriteDurability::SyncTransactional,
            )
            .unwrap();
        assert_eq!(resolved_again, assigned_id);
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
                &prefix,
                (&cur_lo, cur_inc),
                None,
                7,
                true,
                &mut chunk_pks,
                &mut slab,
                &mut spans,
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
                &prefix,
                (&cur_lo2, cur_inc2),
                None,
                6,
                false,
                &mut pks,
                &mut slab,
                &mut spans,
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
