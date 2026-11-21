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
            .put_row(
                1,
                b"blob_k1",
                &large_blob,
                &[],
                WriteDurability::BufferedFast,
            )
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
fn flush_memtable_metadata_fault_preserves_entries_until_reopen() {
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
fn large_transaction_over_4mb_rollback_leaves_zero_leaked_rows() {
    let tmp = TempTestDir::new("large_tx_rollback");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut engine = StorageEngine::open(fs, EngineConfig::default()).unwrap();

    engine
        .put_row(
            1,
            b"pre_tx",
            b"baseline",
            &[],
            WriteDurability::BufferedFast,
        )
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
                return Err(io::Error::other("simulated WAL write I/O failure"));
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
        .put_row(
            1,
            b"r1",
            &orig_blob,
            &[],
            WriteDurability::SyncTransactional,
        )
        .unwrap();
    engine
        .put_row(
            1,
            b"r2",
            &orig_blob,
            &[],
            WriteDurability::SyncTransactional,
        )
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
    // A failed WAL write poisons the engine; reopen (without power loss) to continue.
    assert!(engine.is_poisoned());
    fs.fail_wal_writes.store(false, Ordering::SeqCst);
    drop(engine);
    let mut engine = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
    assert!(!engine.is_poisoned());
    assert_eq!(engine.get_row(1, b"r1").unwrap(), Some(orig_blob.clone()));

    // Failing commit_transaction must roll back MemTable!
    engine.begin_transaction();
    engine
        .put_row(
            1,
            b"tx_fail_k",
            b"tx_val",
            &[],
            WriteDurability::BufferedFast,
        )
        .unwrap();
    fs.fail_wal_writes.store(true, Ordering::SeqCst);
    assert!(engine.commit_transaction().is_err());
    fs.fail_wal_writes.store(false, Ordering::SeqCst);
    assert!(engine.is_poisoned());
    assert!(
        engine.get_row(1, b"tx_fail_k").unwrap().is_none(),
        "failed commit_transaction must roll back unlogged MemTable mutations"
    );
    drop(engine);
    let mut engine = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
    assert!(engine.get_row(1, b"tx_fail_k").unwrap().is_none());

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
    drop(engine);
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
    assert!(
        engine
            .get_row_into(2, b"bk1", &mut slab, &mut spans)
            .unwrap()
    );
    assert!(
        engine
            .get_row_into(2, b"bk2", &mut slab, &mut spans)
            .unwrap()
    );
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
            &[0x11u8; 150],
            &[],
            WriteDurability::BufferedFast,
        )
        .unwrap();
    engine.begin_transaction();
    engine
        .put_row(
            5,
            b"row2",
            &[0x22u8; 200],
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
            None,
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
        engine.flush_memtable().expect(
            "3971B value (4087B inline footprint) must spill to blob and not wedge flush_memtable",
        );
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
        let undo_len_before = engine.tx_overlay.undo.len();

        // Connection 2 reads `shared_k` via `with_committed_read_for_conn(202, ...)`
        let conn2_val = engine
            .with_committed_read_for_conn(202, |eng| eng.get_row(7, b"shared_k"))
            .unwrap();
        assert_eq!(conn2_val, Some(b"committed_v1".to_vec()));
        assert_eq!(
            engine.tx_overlay.undo.len(),
            undo_len_before,
            "concurrent committed read under EagerWriteOnRead must not append to Connection 1's tx_overlay undo log"
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
                None,
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
                None,
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
                None,
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
fn wal_write_failure_poisons_engine_until_reopen() {
    use crate::pal::PreallocStrategy;
    use std::path::Path;
    use std::sync::atomic::AtomicBool;

    struct WalFaultFile<F> {
        inner: Arc<F>,
        is_wal: bool,
        fail_next_wal_write: Arc<AtomicBool>,
    }

    impl<F: PlatformFile> PlatformFile for WalFaultFile<F> {
        fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
            self.inner.read_at(buf, offset)
        }
        fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
            self.inner.read_exact_at(buf, offset)
        }
        fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
            if self.is_wal && self.fail_next_wal_write.swap(false, Ordering::SeqCst) {
                return Err(io::Error::other("simulated WAL write failure"));
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

    struct WalFaultFs {
        inner: OsFs,
        fail_next_wal_write: Arc<AtomicBool>,
    }

    impl PlatformFs for WalFaultFs {
        type File = WalFaultFile<<OsFs as PlatformFs>::File>;

        fn open_file(
            &self,
            rel_name: &str,
            create: bool,
            writable: bool,
            hint: OpenHint,
        ) -> io::Result<Arc<Self::File>> {
            let inner = self.inner.open_file(rel_name, create, writable, hint)?;
            Ok(Arc::new(WalFaultFile {
                inner,
                is_wal: rel_name == "wal_sync.db" || rel_name == "wal_buf.db",
                fail_next_wal_write: Arc::clone(&self.fail_next_wal_write),
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

    for durability in [
        WriteDurability::SyncTransactional,
        WriteDurability::BufferedFast,
    ] {
        let tmp = TempTestDir::new("wal_poison");
        let flag = Arc::new(AtomicBool::new(false));
        let fs = Arc::new(WalFaultFs {
            inner: OsFs::open(&tmp.path, true).unwrap(),
            fail_next_wal_write: Arc::clone(&flag),
        });

        let mut engine = StorageEngine::open(Arc::clone(&fs), EngineConfig::default()).unwrap();
        engine
            .put_row(1, b"before", b"v0", &[], durability)
            .unwrap();
        assert!(!engine.is_poisoned());

        flag.store(true, Ordering::SeqCst);
        let err = engine
            .put_row(1, b"failed", b"v1", &[], durability)
            .unwrap_err();
        assert!(err.to_string().contains("simulated WAL write failure"));
        assert!(engine.is_poisoned());

        let err2 = engine
            .put_row(1, b"after", b"v2", &[], durability)
            .unwrap_err();
        assert!(err2.to_string().contains("poisoned"));
        assert!(err2.to_string().contains("simulated WAL write failure"));
        assert!(engine.delete_row(1, b"before", durability).is_err());
        assert!(engine.flush_memtable().is_err());
        assert!(engine.compact().is_err());

        // Reads keep working and never see the failed or rejected writes.
        assert_eq!(engine.get_row(1, b"before").unwrap(), Some(b"v0".to_vec()));
        assert_eq!(engine.get_row(1, b"failed").unwrap(), None);
        assert_eq!(engine.get_row(1, b"after").unwrap(), None);
        drop(engine);

        let mut reopened = StorageEngine::open(fs, EngineConfig::default()).unwrap();
        assert!(!reopened.is_poisoned());
        assert_eq!(
            reopened.get_row(1, b"before").unwrap(),
            Some(b"v0".to_vec())
        );
        reopened
            .put_row(1, b"after", b"v2", &[], durability)
            .unwrap();
        assert_eq!(reopened.get_row(1, b"after").unwrap(), Some(b"v2".to_vec()));
    }
}

#[test]
fn lazy_blob_survives_segment_reclaim_via_pinned_handle() {
    let tmp = TempTestDir::new("lazy_blob_pinned");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut cfg = EngineConfig::default();
    cfg.max_blob_segment_bytes = 4096; // roll after ~2 blobs so early segments become sealed
    let mut engine = StorageEngine::open(fs, cfg).unwrap();

    let val = |i: u8, gen_: u8| {
        let mut v = vec![gen_; 2048];
        v[0] = i;
        v
    };
    for i in 0..8u8 {
        let k = [b'k', i];
        engine
            .put_row(1, &k, &val(i, 0xA1), &[], WriteDurability::BufferedFast)
            .unwrap();
    }
    engine.flush_memtable().unwrap();

    // Capture lazy pointers for the first rows (living in the oldest sealed segments).
    let mut slab = Vec::new();
    let mut spans = Vec::new();
    let mut lazy = Vec::new();
    for i in 0..2u8 {
        assert!(
            engine
                .get_row_into_lazy(1, &[b'k', i], &mut slab, &mut spans, &mut lazy)
                .unwrap()
        );
    }
    let first_seg = lazy[0].as_ref().expect("blob row is lazy").ptr.seg_id;
    assert!(engine.std_blobs.contains_key(&first_seg));

    // Overwrite every row (original blobs become dead), then compact to reclaim segments.
    for i in 0..8u8 {
        let k = [b'k', i];
        engine
            .put_row(1, &k, &val(i, 0xB2), &[], WriteDurability::BufferedFast)
            .unwrap();
    }
    engine.flush_memtable().unwrap();
    engine.compact().unwrap();
    engine.flush_memtable().unwrap();
    engine.compact().unwrap();
    assert!(
        !engine.std_blobs.contains_key(&first_seg),
        "original blob segment should have been reclaimed"
    );

    // Dereference through pinned handles must still return the original bytes.
    for (i, lb) in lazy.iter().enumerate() {
        let lb = lb.as_ref().unwrap();
        let mut out = Vec::new();
        let (s, e) = engine
            .dereference_lazy_blob_into(1, &[b'k', i as u8], lb, &mut out)
            .unwrap()
            .expect("pinned blob must still be readable");
        assert_eq!(&out[s..e], &val(i as u8, 0xA1)[..]);
    }
    // And the live rows read the new generation.
    assert_eq!(engine.get_row(1, b"k\x00").unwrap(), Some(val(0, 0xB2)));
}

#[test]
fn sync_point_seals_buf_wal_sector_so_it_is_never_rewritten() {
    for mode in [
        WalStreamMode::BufferedImmediateOsCache,
        WalStreamMode::BufferedUserSpaceCoalesced,
    ] {
        let tmp = TempTestDir::new("seal_buf_sector");
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
        let mut cfg = EngineConfig::default();
        cfg.buf_wal_mode = mode;

        let mut engine = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
        engine
            .put_row(1, b"k1", b"v1", &[], WriteDurability::BufferedFast)
            .unwrap();
        let before_sync = engine.buf_wal.current_sector_idx();
        engine
            .put_row(1, b"k2", b"v2", &[], WriteDurability::SyncTransactional)
            .unwrap();
        // The sector holding k1 was synced and sealed: the cursor moved past it.
        assert!(engine.buf_wal.current_sector_idx() > before_sync);
        let after_sync = engine.buf_wal.current_sector_idx();
        engine
            .put_row(1, b"k3", b"v3", &[], WriteDurability::BufferedFast)
            .unwrap();
        assert!(engine.buf_wal.current_sector_idx() >= after_sync);
        drop(engine);

        let mut engine = StorageEngine::open(fs, cfg).unwrap();
        for (k, v) in [(&b"k1"[..], &b"v1"[..]), (b"k2", b"v2"), (b"k3", b"v3")] {
            assert_eq!(engine.get_row(1, k).unwrap(), Some(v.to_vec()));
        }
    }
}

#[test]
fn stale_same_epoch_wal_sectors_are_not_replayed_after_halt_at_sector_zero() {
    let tmp = TempTestDir::new("stale_wal_sectors");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());

    let mut cfg = EngineConfig::default();
    cfg.inline_threshold_bytes = 8192;

    let val = vec![0x77u8; 1500];
    {
        let mut engine = StorageEngine::open(fs.clone(), cfg.clone()).unwrap();
        // Each SyncTransactional write seals the open wal_buf sector first, so every buffered
        // batch ends at a sector boundary and the next sector starts with a Full fragment.
        for i in 0..6u8 {
            engine
                .put_row(1, &[b'b', i], &val, &[], WriteDurability::BufferedFast)
                .unwrap();
            engine
                .put_row(
                    1,
                    &[b'y', i],
                    b"sync",
                    &[],
                    WriteDurability::SyncTransactional,
                )
                .unwrap();
        }
        // Dropped without flush_memtable(): simulates a crash with several wal_buf sectors.
    }

    // Tear sector 0 of both WALs so recovery halts there while later sectors of the same epoch
    // stay on disk.
    for name in ["wal_buf.db", "wal_sync.db"] {
        let wal_path = tmp.path.join(name);
        let mut bytes = std::fs::read(&wal_path).unwrap();
        assert!(bytes.len() > 100);
        bytes[100] ^= 0xFF;
        std::fs::write(&wal_path, &bytes).unwrap();
    }

    {
        let mut engine = StorageEngine::open(fs.clone(), cfg.clone()).unwrap();
        engine
            .put_row(
                1,
                b"new_key",
                b"new_val",
                &[],
                WriteDurability::BufferedFast,
            )
            .unwrap();
    }

    let mut engine = StorageEngine::open(fs, cfg).unwrap();
    assert_eq!(
        engine.get_row(1, b"new_key").unwrap(),
        Some(b"new_val".to_vec())
    );
    for i in 0..6u8 {
        for prefix in *b"by" {
            assert_eq!(
                engine.get_row(1, &[prefix, i]).unwrap(),
                None,
                "stale pre-crash key {}{i} was resurrected",
                prefix as char
            );
        }
    }
}

#[test]
fn open_time_catalog_commit_does_not_persist_wal_replay_dead_bytes() {
    use crate::pal::PreallocStrategy;
    use std::path::Path;
    use std::sync::atomic::AtomicI64;

    // `mode`: 0 = normal, 1 = silently drop boot_meta writes (lost-write crash simulation),
    // 2 = allow `budget` more boot_meta writes, then fail them.
    struct Ctl {
        mode: AtomicI64,
        budget: AtomicI64,
    }
    struct FaultFile<F> {
        inner: Arc<F>,
        name: String,
        ctl: Arc<Ctl>,
    }
    impl<F: PlatformFile> PlatformFile for FaultFile<F> {
        fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
            self.inner.read_at(buf, offset)
        }
        fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
            self.inner.read_exact_at(buf, offset)
        }
        fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
            if self.name == "boot_meta.db" {
                match self.ctl.mode.load(Ordering::SeqCst) {
                    1 => return Ok(()),
                    2 if self.ctl.budget.fetch_sub(1, Ordering::SeqCst) <= 0 => {
                        return Err(io::Error::other("simulated crash before checkpoint"));
                    }
                    _ => {}
                }
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
        ctl: Arc<Ctl>,
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
                ctl: Arc::clone(&self.ctl),
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

    let tmp = TempTestDir::new("open_commit_dead_bytes");
    let ctl = Arc::new(Ctl {
        mode: AtomicI64::new(0),
        budget: AtomicI64::new(0),
    });
    let fs = Arc::new(FaultFs {
        inner: OsFs::open(&tmp.path, true).unwrap(),
        ctl: Arc::clone(&ctl),
    });
    let mut cfg = EngineConfig::default();
    cfg.inline_threshold_bytes = 128;
    cfg.max_blob_segment_bytes = 16 * 1024;
    let key = |i: u32| format!("blob_key_{i:03}").into_bytes();
    let blob = |i: u32| vec![i as u8 + 1; 3000];

    // Segment A: four live blobs, checkpointed.
    {
        let mut engine = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
        for i in 0..4u32 {
            engine
                .put_row(1, &key(i), &blob(i), &[], WriteDurability::BufferedFast)
                .unwrap();
        }
        engine.flush_memtable().unwrap();
    }
    let seg_a = {
        let engine = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
        let segs = &engine.meta_mgr.state().std_blob_segments;
        assert_eq!(segs.len(), 1);
        assert_eq!(segs[0].dead_bytes, 0);
        segs[0].seq_id
    };

    // Overwrite 3 of the 4 blobs; the new segment B's catalog commit is lost (crash), so B is an
    // uncataloged live segment referenced only by the WAL. Dead bytes of A exist only in the WAL.
    {
        let mut engine = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
        ctl.mode.store(1, Ordering::SeqCst);
        for i in 0..3u32 {
            engine
                .put_row(
                    1,
                    &key(i),
                    &blob(i + 10),
                    &[],
                    WriteDurability::BufferedFast,
                )
                .unwrap();
        }
    }
    ctl.mode.store(0, Ordering::SeqCst);

    // Open: the uncataloged-blob catalog commit succeeds, then the post-recovery checkpoint
    // fails (crash before the checkpoint_lsn advances).
    ctl.budget.store(1, Ordering::SeqCst);
    ctl.mode.store(2, Ordering::SeqCst);
    assert!(StorageEngine::open(Arc::clone(&fs), cfg.clone()).is_err());
    ctl.mode.store(0, Ordering::SeqCst);

    // The persisted dead_bytes must not include the (not yet checkpointed) replay increments.
    {
        let mut engine = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
        let a = engine
            .meta_mgr
            .state()
            .std_blob_segments
            .iter()
            .find(|s| s.seq_id == seg_a)
            .cloned()
            .expect("segment A with a live blob must not be reclaimed");
        // Exactly 3 of 4 equal blobs are dead; a double count would be ~6/4 of the file.
        assert!(
            a.dead_bytes > 0 && a.dead_bytes * 4 <= a.file_len * 3,
            "{a:?}"
        );
        assert!(
            fs.list_files()
                .unwrap()
                .contains(&format_std_blob_file_name(seg_a))
        );
        for i in 0..3u32 {
            assert_eq!(engine.get_row(1, &key(i)).unwrap(), Some(blob(i + 10)));
        }
        assert_eq!(engine.get_row(1, &key(3)).unwrap(), Some(blob(3)));
    }
}

// ---------------------------------------------------------------------------
// Blob corruption policy (`CorruptPolicy`, M4)
// ---------------------------------------------------------------------------

/// Locates the payload's last byte of the blob record referenced by the record at `key` (must be
/// a blob pointer). Call after `flush_memtable` and BEFORE dropping the engine; the corruption
/// itself is applied by `apply_blob_corruption` once the engine (exclusive handles on Windows)
/// is gone.
fn blob_corruption_target(engine: &StorageEngine<OsFs>, key: &[u8]) -> (String, u64) {
    let Some((_, IndexValue::Blob(ptr))) = engine.lookup_raw_entry(key).unwrap() else {
        panic!("key is not stored as a blob");
    };
    let name = match ptr.kind {
        BlobKind::Standard => format_std_blob_file_name(ptr.seg_id),
        BlobKind::Lru => format_lru_blob_file_name(ptr.table_id, ptr.seg_id),
    };
    (name, ptr.offset as u64 + ptr.len as u64 - 1)
}

fn apply_blob_corruption(dir: &std::path::Path, target: &(String, u64)) {
    let path = dir.join(&target.0);
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[target.1 as usize] ^= 0xFF;
    std::fs::write(&path, &bytes).unwrap();
}

fn live_lru_pks(engine: &StorageEngine<OsFs>, table_id: u16) -> Vec<Vec<u8>> {
    engine
        .scan_merged_prefix(&encode_namespace_prefix(table_id, NS_LRU_QUEUE))
        .unwrap()
        .iter()
        .filter(|e| !matches!(e.value, IndexValue::Tombstone))
        .map(|e| {
            crate::index::decode_lru_queue_key(table_id, &e.key)
                .unwrap()
                .1
                .to_vec()
        })
        .collect()
}

#[test]
fn corrupt_blob_uncapped_table_errors_on_read_and_delete_cleans_up() {
    let tmp = TempTestDir::new("corrupt_uncapped");
    let mut cfg = EngineConfig::default();
    cfg.inline_threshold_bytes = 256;
    let payload = vec![0x42u8; 1000];
    let target;
    {
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
        let mut engine = StorageEngine::open(fs, cfg.clone()).unwrap();
        for (pk, tag) in [
            (&b"k1"[..], &b"tag_a"[..]),
            (b"k2", b"tag_b"),
            (b"k3", b"tag_c"),
        ] {
            engine
                .put_row(
                    1,
                    pk,
                    &payload,
                    &[(10, tag.to_vec())],
                    WriteDurability::BufferedFast,
                )
                .unwrap();
        }
        engine.flush_memtable().unwrap();
        target = blob_corruption_target(&engine, &encode_primary_row_key(1, b"k2"));
    }
    apply_blob_corruption(&tmp.path, &target);

    let fs = Arc::new(OsFs::open(&tmp.path, false).unwrap());
    let mut engine = StorageEngine::open(fs, cfg).unwrap();
    assert_eq!(engine.corrupt_policy(1), CorruptPolicy::Error);
    assert_eq!(engine.corrupt_blob_detections(), 0);

    let err = engine.get_row(1, b"k2").unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    let msg = err.to_string();
    assert!(msg.contains("table 1") && msg.contains("0x6b32"), "{msg}");
    assert!(engine.corrupt_blob_detections() > 0);
    let mut slab = Vec::new();
    let mut spans = Vec::new();
    assert_eq!(
        engine
            .get_row_into(1, b"k2", &mut slab, &mut spans)
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(
        engine.scan_table(1).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    // Other rows are unaffected, and nothing was written by the failed reads.
    assert_eq!(engine.get_row(1, b"k1").unwrap().unwrap(), payload);
    assert_eq!(engine.get_row(1, b"k3").unwrap().unwrap(), payload);
    assert_eq!(
        engine.lookup_by_secondary_index(1, 10, b"tag_b").unwrap(),
        vec![b"k2".to_vec()]
    );

    assert!(
        engine
            .delete_row(1, b"k2", WriteDurability::BufferedFast)
            .unwrap()
    );
    assert!(
        engine
            .lookup_by_secondary_index(1, 10, b"tag_b")
            .unwrap()
            .is_empty()
    );
    assert_eq!(engine.get_row(1, b"k2").unwrap(), None);
    assert_eq!(engine.scan_table(1).unwrap().len(), 2);
}

#[test]
fn corrupt_blob_capped_table_evicts_row_at_next_write() {
    let tmp = TempTestDir::new("corrupt_capped");
    let mut cfg = EngineConfig::default();
    cfg.inline_threshold_bytes = 256;
    let payload = vec![0x42u8; 1000];
    let targets;
    {
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
        let mut engine = StorageEngine::open(fs, cfg.clone()).unwrap();
        engine.register_capped_table(1, 10_000).unwrap();
        for (pk, tag) in [(&b"k1"[..], &b"a"[..]), (b"k2", b"b"), (b"k3", b"c")] {
            engine
                .put_row(
                    1,
                    pk,
                    &payload,
                    &[(10, tag.to_vec())],
                    WriteDurability::BufferedFast,
                )
                .unwrap();
        }
        engine.flush_memtable().unwrap();
        targets = [
            blob_corruption_target(&engine, &encode_primary_row_key(1, b"k2")),
            blob_corruption_target(&engine, &encode_primary_row_key(1, b"k3")),
        ];
    }
    for t in &targets {
        apply_blob_corruption(&tmp.path, t);
    }

    let fs = Arc::new(OsFs::open(&tmp.path, false).unwrap());
    let mut engine = StorageEngine::open(fs, cfg).unwrap();
    assert_eq!(engine.corrupt_policy(1), CorruptPolicy::Evict);
    assert_eq!(
        engine.capped_table_state(1).unwrap().current_logical_bytes,
        3 * 1002
    );

    assert_eq!(engine.get_row(1, b"k2").unwrap(), None);
    assert_eq!(engine.get_row(1, b"k3").unwrap(), None);
    assert!(engine.corrupt_blob_detections() >= 2);
    // Reads never write: rows are still physically present until the next mutation.
    assert!(engine.row_exists(1, b"k2").unwrap());
    assert!(engine.get_row(1, b"k1").unwrap().is_some());

    // Overwrite k3 inside a transaction (the queue is not drained while a tx is active).
    engine.begin_transaction();
    engine
        .put_row(
            1,
            b"k3",
            b"fresh",
            &[(10, b"c".to_vec())],
            WriteDurability::BufferedFast,
        )
        .unwrap();
    assert!(engine.row_exists(1, b"k2").unwrap());
    // Committing the outermost tx drains the queue: k2 is physically removed, the overwritten
    // k3 (pointer changed since detection) is kept.
    engine.commit_transaction().unwrap();
    assert!(!engine.row_exists(1, b"k2").unwrap());
    assert!(engine.row_exists(1, b"k3").unwrap());
    engine
        .put_row(1, b"k4", b"v4", &[], WriteDurability::BufferedFast)
        .unwrap();
    assert!(!engine.row_exists(1, b"k2").unwrap());
    assert!(
        engine
            .lookup_raw_entry(&encode_row_meta_key(1, b"k2"))
            .unwrap()
            .is_none_or(|(_, v)| matches!(v, IndexValue::Tombstone))
    );
    let lru = live_lru_pks(&engine, 1);
    assert!(!lru.contains(&b"k2".to_vec()));
    assert!(lru.contains(&b"k3".to_vec()));
    assert!(
        engine
            .lookup_by_secondary_index(1, 10, b"b")
            .unwrap()
            .is_empty()
    );
    assert_eq!(engine.get_row(1, b"k3").unwrap().unwrap(), b"fresh");
    // k1 (1002) + k3 (2 + 5) + k4 (2 + 2)
    assert_eq!(
        engine.capped_table_state(1).unwrap().current_logical_bytes,
        1002 + 7 + 4
    );
}

#[test]
fn corrupt_spilled_row_meta_delete_still_cleans_secondary_and_lru() {
    let tmp = TempTestDir::new("corrupt_meta");
    let cfg = EngineConfig::default();
    // 8 large secondary keys push the encoded `RowMetaRecord` past the inline block fit.
    let sec: Vec<(u16, Vec<u8>)> = (0..8u16)
        .map(|i| (20 + i, vec![b'a' + i as u8; 1500]))
        .collect();
    let targets;
    {
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
        let mut engine = StorageEngine::open(fs, cfg.clone()).unwrap();
        engine.register_capped_table(2, 1_000_000).unwrap();
        for table_id in [1u16, 2] {
            engine
                .put_row(table_id, b"pk", b"val", &sec, WriteDurability::BufferedFast)
                .unwrap();
            engine
                .put_row(
                    table_id,
                    b"other",
                    b"val",
                    &sec[..1],
                    WriteDurability::BufferedFast,
                )
                .unwrap();
        }
        engine.flush_memtable().unwrap();
        targets =
            [1u16, 2].map(|t| blob_corruption_target(&engine, &encode_row_meta_key(t, b"pk")));
    }
    for t in &targets {
        apply_blob_corruption(&tmp.path, t);
    }

    let fs = Arc::new(OsFs::open(&tmp.path, false).unwrap());
    let mut engine = StorageEngine::open(fs, cfg).unwrap();
    assert_eq!(engine.capped_table_state(2).unwrap().max_bytes, 1_000_000);
    for table_id in [1u16, 2] {
        // Delete must succeed in `error` mode (table 1) and in `evict` mode (table 2).
        // (Table 2's corrupt row is already queued by reconciliation and removed by the
        // eviction drain at the start of `delete_row`, so its own result may be `false`.)
        let deleted = engine
            .delete_row(table_id, b"pk", WriteDurability::BufferedFast)
            .unwrap();
        assert!(deleted || table_id == 2);
        for (idx, col) in &sec {
            let pks = engine
                .lookup_by_secondary_index(table_id, *idx, col)
                .unwrap();
            if *idx == 20 {
                assert_eq!(pks, vec![b"other".to_vec()], "table {table_id}");
            } else {
                assert!(pks.is_empty(), "table {table_id} idx {idx} leaked");
            }
        }
        assert_eq!(engine.get_row(table_id, b"pk").unwrap(), None);
        assert_eq!(engine.get_row(table_id, b"other").unwrap().unwrap(), b"val");
    }
    assert_eq!(live_lru_pks(&engine, 2), vec![b"other".to_vec()]);
    assert_eq!(
        engine.capped_table_state(2).unwrap().current_logical_bytes,
        (5 + 3) as u64
    );
    assert!(engine.corrupt_blob_detections() > 0);
}

#[test]
fn corrupt_policy_override_and_defaults() {
    let tmp = TempTestDir::new("corrupt_policy_defaults");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut engine = StorageEngine::open(fs, EngineConfig::default()).unwrap();
    engine.register_capped_table(5, 10_000).unwrap();
    assert_eq!(engine.corrupt_policy(1), CorruptPolicy::Error);
    assert_eq!(engine.corrupt_policy(5), CorruptPolicy::Evict);
    engine.set_table_corrupt_policy(5, CorruptPolicy::Error);
    engine.set_table_corrupt_policy(1, CorruptPolicy::Evict);
    assert_eq!(engine.corrupt_policy(5), CorruptPolicy::Error);
    assert_eq!(engine.corrupt_policy(1), CorruptPolicy::Evict);
    engine.reset_table_corrupt_policy(5);
    assert_eq!(engine.corrupt_policy(5), CorruptPolicy::Evict);
}

// ---------------------------------------------------------------------------------------------
// Background (3-phase) compaction
// ---------------------------------------------------------------------------------------------

fn bg_cfg() -> EngineConfig {
    let mut cfg = EngineConfig::default();
    cfg.auto_compact_segment_threshold = 4;
    cfg.background_compaction = true;
    cfg
}

fn bg_put_round(engine: &mut StorageEngine<OsFs>, round: usize, rows: usize, len: usize) {
    for i in 0..rows {
        let key = format!("r{round:03}_{i:03}");
        engine
            .put_row(
                1,
                key.as_bytes(),
                &vec![round as u8; len],
                &[],
                WriteDurability::BufferedFast,
            )
            .unwrap();
    }
    engine.flush_memtable().unwrap();
}

fn idx_file_names(fs: &OsFs) -> Vec<String> {
    let mut v: Vec<String> = fs
        .list_files()
        .unwrap()
        .into_iter()
        .filter(|n| n.starts_with("idx_"))
        .collect();
    v.sort();
    v
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

fn put_t(engine: &mut StorageEngine<OsFs>, k: &str, v: &str) {
    engine
        .put_row(
            1,
            k.as_bytes(),
            v.as_bytes(),
            &[],
            WriteDurability::BufferedFast,
        )
        .unwrap();
}

fn keys_of_r(r: &crate::engine::EngineReader<'_, OsFs>, reverse: bool) -> Vec<String> {
    let mut pks = Vec::new();
    let mut slab = Vec::new();
    let mut spans = Vec::new();
    r.scan_range_into_slab(
        1,
        None,
        None,
        reverse,
        usize::MAX,
        true,
        &mut pks,
        &mut slab,
        &mut spans,
        None,
    )
    .unwrap();
    pks.into_iter()
        .map(|k| String::from_utf8(k).unwrap())
        .collect()
}

fn keys_of(engine: &mut StorageEngine<OsFs>, reverse: bool) -> Vec<String> {
    let mut pks = Vec::new();
    let mut slab = Vec::new();
    let mut spans = Vec::new();
    engine
        .scan_range_into_slab(
            1,
            None,
            None,
            reverse,
            usize::MAX,
            true,
            &mut pks,
            &mut slab,
            &mut spans,
            None,
        )
        .unwrap();
    pks.into_iter()
        .map(|k| String::from_utf8(k).unwrap())
        .collect()
}

#[test]
fn non_owner_read_with_large_uncommitted_tx_sees_nothing_and_does_not_touch_engine_state() {
    let tmp = TempTestDir::new("overlay_non_owner");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut engine = StorageEngine::open(fs, EngineConfig::default()).unwrap();
    put_t(&mut engine, "committed", "c1");
    let mem_len = engine.memtable.len();
    let mem_bytes = engine.memtable.approximate_bytes();
    let mem_hi = engine.memtable.highest_lsn();

    engine.begin_transaction_for_conn(1).unwrap();
    put_t(&mut engine, "committed", "uncommitted_overwrite");
    for i in 0..10_000u32 {
        put_t(&mut engine, &format!("u{i:06}"), "x");
    }
    assert_eq!(
        engine.memtable.len(),
        mem_len,
        "tx writes must stay in the overlay"
    );
    assert_eq!(engine.memtable.approximate_bytes(), mem_bytes);
    assert_eq!(engine.memtable.highest_lsn(), mem_hi);

    let (v, miss, scanned) = engine
        .with_committed_read_for_conn(2, |e| {
            Ok((
                e.get_row(1, b"committed")?,
                e.get_row(1, b"u000123")?,
                keys_of_r(e, false),
            ))
        })
        .unwrap();
    assert_eq!(v, Some(b"c1".to_vec()));
    assert_eq!(miss, None);
    assert_eq!(scanned, vec!["committed".to_string()]);
    assert_eq!(engine.memtable.len(), mem_len);
    assert_eq!(engine.memtable.approximate_bytes(), mem_bytes);
    assert_eq!(engine.memtable.highest_lsn(), mem_hi);
    assert_eq!(engine.read_view_for_conn(1), ReadView::Owner);
    assert_eq!(engine.read_view_for_conn(2), ReadView::Committed);

    // The owner still sees everything.
    assert_eq!(
        engine.get_row(1, b"committed").unwrap(),
        Some(b"uncommitted_overwrite".to_vec())
    );
    assert_eq!(keys_of(&mut engine, false).len(), 10_001);

    engine.commit_transaction_for_conn(1).unwrap();
    assert_eq!(engine.memtable.len(), mem_len + 10_000);
    assert!(engine.tx_overlay.map.is_empty());
    assert_eq!(
        engine
            .with_committed_read_for_conn(2, |e| e.get_row(1, b"u000123"))
            .unwrap(),
        Some(b"x".to_vec())
    );
}

#[test]
fn owner_scan_merges_overlay_with_tombstones_and_overwrites_forward_and_reverse() {
    for flush_first in [false, true] {
        let tmp = TempTestDir::new("overlay_scan_merge");
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
        let mut engine = StorageEngine::open(fs, EngineConfig::default()).unwrap();
        for k in ["a", "b", "c", "d", "e"] {
            put_t(&mut engine, k, "old");
        }
        if flush_first {
            engine.flush_memtable().unwrap();
            // Newer committed layer on top of the segment.
            put_t(&mut engine, "c", "mem_c");
            put_t(&mut engine, "z", "mem_z");
        }

        engine.begin_transaction_for_conn(1).unwrap();
        engine
            .delete_row(1, b"b", WriteDurability::BufferedFast)
            .unwrap();
        put_t(&mut engine, "c", "tx_c");
        put_t(&mut engine, "bb", "tx_bb");
        put_t(&mut engine, "0", "tx_0");
        engine
            .delete_row(1, b"e", WriteDurability::BufferedFast)
            .unwrap();

        let mut expect: Vec<&str> = vec!["0", "a", "bb", "c", "d"];
        if flush_first {
            expect.push("z");
        }
        let fwd = keys_of(&mut engine, false);
        assert_eq!(fwd, expect);
        let mut rev_expect = expect.clone();
        rev_expect.reverse();
        assert_eq!(keys_of(&mut engine, true), rev_expect);
        assert_eq!(engine.get_row(1, b"c").unwrap(), Some(b"tx_c".to_vec()));
        assert_eq!(engine.get_row(1, b"b").unwrap(), None);
        let rows = engine.scan_table(1).unwrap();
        assert_eq!(rows.iter().find(|(k, _)| k == b"c").unwrap().1, b"tx_c");

        // Non-owner sees only committed data.
        let committed: Vec<String> = ["a", "b", "c", "d", "e"]
            .iter()
            .map(|s| s.to_string())
            .chain(flush_first.then(|| "z".to_string()))
            .collect();
        assert_eq!(
            engine
                .with_committed_read_for_conn(2, |e| Ok(keys_of_r(e, false)))
                .unwrap(),
            committed
        );
        let non_owner_c = engine
            .with_committed_read_for_conn(2, |e| e.get_row(1, b"c"))
            .unwrap();
        assert_eq!(
            non_owner_c,
            Some(if flush_first {
                b"mem_c".to_vec()
            } else {
                b"old".to_vec()
            })
        );

        engine.commit_transaction_for_conn(1).unwrap();
        assert_eq!(keys_of(&mut engine, false), expect);
    }
}

#[test]
fn savepoint_rollback_restores_overlay_precisely() {
    let tmp = TempTestDir::new("overlay_savepoint");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut engine = StorageEngine::open(fs, EngineConfig::default()).unwrap();
    put_t(&mut engine, "base", "b0");
    put_t(&mut engine, "gone", "g0");

    engine.begin_transaction_for_conn(1).unwrap();
    put_t(&mut engine, "pre", "p1"); // before any savepoint: not undo-logged
    engine.savepoint_for_conn(1, 1).unwrap();
    put_t(&mut engine, "pre", "p2"); // overwrite of an overlay key
    put_t(&mut engine, "base", "b1"); // overwrite of a committed key
    put_t(&mut engine, "new_after_sp", "n"); // key first written after the savepoint
    engine
        .delete_row(1, b"gone", WriteDurability::BufferedFast)
        .unwrap();
    engine.savepoint_for_conn(1, 2).unwrap();
    put_t(&mut engine, "deeper", "d");
    put_t(&mut engine, "new_after_sp", "n2");

    engine.rollback_to_savepoint_for_conn(1, 2).unwrap();
    assert_eq!(engine.get_row(1, b"deeper").unwrap(), None);
    assert_eq!(
        engine.get_row(1, b"new_after_sp").unwrap(),
        Some(b"n".to_vec())
    );

    engine.rollback_to_savepoint_for_conn(1, 1).unwrap();
    assert_eq!(engine.get_row(1, b"pre").unwrap(), Some(b"p1".to_vec()));
    assert_eq!(engine.get_row(1, b"base").unwrap(), Some(b"b0".to_vec()));
    assert_eq!(engine.get_row(1, b"gone").unwrap(), Some(b"g0".to_vec()));
    assert_eq!(engine.get_row(1, b"new_after_sp").unwrap(), None);
    assert_eq!(keys_of(&mut engine, false), vec!["base", "gone", "pre"]);

    // Writes after the rollback still work and commit cleanly.
    put_t(&mut engine, "after", "a");
    engine.release_savepoint_for_conn(1, 1).unwrap();
    engine.commit_transaction_for_conn(1).unwrap();
    assert_eq!(
        keys_of(&mut engine, false),
        vec!["after", "base", "gone", "pre"]
    );
}

#[test]
fn commit_wal_failure_leaves_memtable_without_tx_data_and_poisons_engine() {
    use crate::pal::PreallocStrategy;
    use std::path::Path;
    use std::sync::atomic::AtomicBool;

    struct FaultFile<F> {
        inner: Arc<F>,
        name: String,
        fail_wal: Arc<AtomicBool>,
    }
    impl<F: PlatformFile> PlatformFile for FaultFile<F> {
        fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
            self.inner.read_at(buf, offset)
        }
        fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
            self.inner.read_exact_at(buf, offset)
        }
        fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
            if self.name.starts_with("wal_") && self.fail_wal.load(Ordering::SeqCst) {
                return Err(io::Error::other("simulated WAL write failure"));
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
        fail_wal: Arc<AtomicBool>,
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
                fail_wal: Arc::clone(&self.fail_wal),
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

    let tmp = TempTestDir::new("overlay_commit_fault");
    let fail = Arc::new(AtomicBool::new(false));
    let fs = Arc::new(FaultFs {
        inner: OsFs::open(&tmp.path, true).unwrap(),
        fail_wal: Arc::clone(&fail),
    });
    let mut engine = StorageEngine::open(fs, EngineConfig::default()).unwrap();
    engine
        .put_row(1, b"committed", b"c", &[], WriteDurability::BufferedFast)
        .unwrap();
    let mem_len = engine.memtable.len();

    engine.begin_transaction_for_conn(1).unwrap();
    for i in 0..50u32 {
        let k = format!("tx{i:03}");
        engine
            .put_row(1, k.as_bytes(), b"v", &[], WriteDurability::BufferedFast)
            .unwrap();
    }
    fail.store(true, Ordering::SeqCst);
    assert!(engine.commit_transaction_for_conn(1).is_err());
    fail.store(false, Ordering::SeqCst);

    assert!(engine.is_poisoned());
    assert_eq!(
        engine.memtable.len(),
        mem_len,
        "MemTable must hold no tx data"
    );
    assert!(engine.tx_overlay.map.is_empty());
    assert!(!engine.in_active_transaction());
    assert_eq!(engine.get_row(1, b"tx000").unwrap(), None);
    assert_eq!(
        engine.get_row(1, b"committed").unwrap(),
        Some(b"c".to_vec())
    );
    assert!(
        engine
            .put_row(1, b"more", b"v", &[], WriteDurability::BufferedFast)
            .is_err()
    );
}

/// Runs `f` on a helper thread and fails the test if it does not finish in `secs` (deadlock guard).
fn run_with_timeout(secs: u64, f: impl FnOnce() + Send + 'static) {
    let (tx, rx) = std::sync::mpsc::channel();
    let h = std::thread::spawn(move || {
        f();
        let _ = tx.send(());
    });
    match rx.recv_timeout(std::time::Duration::from_secs(secs)) {
        Ok(()) => h.join().unwrap(),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            panic!("test body did not finish within {secs}s (deadlock?)")
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => h.join().unwrap(),
    }
}

fn pair_value(n: u32) -> Vec<u8> {
    // Alternate inline (50 B) and out-of-line blob (3000 B) payloads.
    let len = if n.is_multiple_of(2) { 50 } else { 3000 };
    format!("{n:08}").bytes().cycle().take(len).collect()
}

#[test]
fn read_view_and_exclusive_rules() {
    let tmp = TempTestDir::new("read_rules");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut eng = StorageEngine::open(fs, EngineConfig::default()).unwrap();
    put_t(&mut eng, "k", "v");
    assert_eq!(eng.read_view_for_conn(1), ReadView::Owner);
    assert!(!eng.read_needs_exclusive(1, 1));
    assert!(!eng.read_needs_exclusive(2, 1));

    eng.begin_transaction_for_conn(1).unwrap();
    assert_eq!(eng.read_view_for_conn(1), ReadView::Owner);
    assert_eq!(eng.read_view_for_conn(2), ReadView::Committed);
    assert!(eng.read_needs_exclusive(1, 1));
    assert!(!eng.read_needs_exclusive(2, 1));

    // The owner's uncommitted blob lives in the writer buffer: a shared-access reader refuses
    // to read it (and says so), the exclusive path flushes and succeeds.
    eng.put_row(
        1,
        b"big",
        &pair_value(1),
        &[],
        WriteDurability::BufferedFast,
    )
    .unwrap();
    {
        let r = eng.reader_for_conn(1);
        assert!(r.get_row(1, b"big").is_err());
        assert!(r.needs_exclusive_retry());
    }
    assert_eq!(
        eng.with_committed_read_for_conn(1, |r| r.get_row(1, b"big"))
            .unwrap(),
        Some(pair_value(1))
    );
    assert_eq!(
        eng.reader_for_conn(2).get_row(1, b"big").unwrap(),
        None,
        "uncommitted blob invisible to other connections"
    );
    eng.commit_transaction_for_conn(1).unwrap();
    assert!(!eng.read_needs_exclusive(1, 1));
    assert_eq!(
        eng.reader_for_conn(2).get_row(1, b"big").unwrap(),
        Some(pair_value(1))
    );
}

#[test]
fn concurrent_shared_readers_never_see_torn_or_uncommitted_pairs() {
    use std::sync::RwLock;
    use std::sync::atomic::AtomicBool;
    run_with_timeout(120, || {
        let tmp = TempTestDir::new("rw_readers");
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
        let engine = Arc::new(RwLock::new(
            StorageEngine::open(fs, EngineConfig::default()).unwrap(),
        ));
        {
            let mut g = engine.write().unwrap();
            for k in [&b"a"[..], b"b"] {
                g.put_row(1, k, &pair_value(0), &[], WriteDurability::BufferedFast)
                    .unwrap();
            }
        }
        let stop = Arc::new(AtomicBool::new(false));
        let readers: Vec<_> = (0..6usize)
            .map(|t| {
                let (engine, stop) = (engine.clone(), stop.clone());
                std::thread::spawn(move || {
                    let conn = 100 + t;
                    let mut reads = 0u64;
                    let (mut slab, mut spans) = (Vec::new(), Vec::new());
                    while !stop.load(Ordering::Relaxed) {
                        let g = engine.read().unwrap();
                        assert!(!g.read_needs_exclusive(conn, 1));
                        let r = g.reader_for_conn(conn);
                        let a = r.get_row(1, b"a").unwrap().unwrap();
                        let b = r.get_row(1, b"b").unwrap().unwrap();
                        assert_eq!(a, b, "torn pair");
                        slab.clear();
                        spans.clear();
                        assert!(r.get_row_into(1, b"a", &mut slab, &mut spans).unwrap());
                        assert_eq!(&slab[..], &a[..]);
                        reads += 1;
                    }
                    reads
                })
            })
            .collect();
        for n in 1..=300u32 {
            engine
                .write()
                .unwrap()
                .begin_transaction_for_conn(1)
                .unwrap();
            for k in [&b"a"[..], b"b"] {
                let mut g = engine.write().unwrap();
                g.put_row(1, k, &pair_value(n), &[], WriteDurability::BufferedFast)
                    .unwrap();
                // Owner reads its own uncommitted (possibly buffered blob) value: exclusive path.
                assert!(g.read_needs_exclusive(1, 1));
                let own = g
                    .with_committed_read_for_conn(1, |r| r.get_row(1, k))
                    .unwrap();
                assert_eq!(own, Some(pair_value(n)));
            }
            engine
                .write()
                .unwrap()
                .commit_transaction_for_conn(1)
                .unwrap();
        }
        stop.store(true, Ordering::Relaxed);
        let total: u64 = readers.into_iter().map(|h| h.join().unwrap()).sum();
        assert!(total > 0);
    });
}

#[test]
fn table_inline_threshold_override_and_effective_default_for_capped_tables() {
    let tmp = TempTestDir::new("inline_threshold_test");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut cfg = EngineConfig::default();
    cfg.inline_threshold_bytes = 1024;
    let mut engine = StorageEngine::open(fs, cfg).unwrap();

    // 1. Table 1 (uncapped): default effective threshold = 1024
    assert_eq!(engine.effective_inline_threshold(1), 1024);
    let val_2048 = vec![0x44u8; 2048];
    engine
        .put_row(1, b"key1", &val_2048, &[], WriteDurability::BufferedFast)
        .unwrap();
    // Default spills to standard blob segment
    assert_eq!(engine.std_blobs.len(), 1);

    // Override inline threshold for Table 1 to 2500 bytes
    engine.set_table_inline_threshold(1, 2500);
    assert_eq!(engine.effective_inline_threshold(1), 2500);
    let initial_blob_len = engine.active_std_writer.as_ref().unwrap().current_len();
    engine
        .put_row(1, b"key2", &val_2048, &[], WriteDurability::BufferedFast)
        .unwrap();
    // Inlined into MemTable! No bytes appended to blob segment!
    assert_eq!(
        engine.active_std_writer.as_ref().unwrap().current_len(),
        initial_blob_len
    );
    assert_eq!(engine.get_row(1, b"key2").unwrap(), Some(val_2048.clone()));

    // 2. Table 2: Capped LRU Table
    engine.register_capped_table(2, 50_000).unwrap();
    // Globally raise inline_threshold_bytes to 3584
    engine.config.inline_threshold_bytes = 3584;
    // Uncapped table 1 (with reset override) would get 3584:
    engine.reset_table_inline_threshold(1);
    assert_eq!(engine.effective_inline_threshold(1), 3584);
    // BUT capped table 2 must remain at 1024 by default to preserve O(1) LRU blob segment eviction!
    assert_eq!(engine.effective_inline_threshold(2), 1024);

    let val_1500 = vec![0x66u8; 1500];
    engine
        .put_row(2, b"cap1", &val_1500, &[], WriteDurability::BufferedFast)
        .unwrap();
    // Spilled to LRU blob segment because 1500 >= 1024
    assert!(engine.active_lru_writers.contains_key(&2));

    // Explicit override on capped table is honored:
    engine.set_table_inline_threshold(2, 2048);
    assert_eq!(engine.effective_inline_threshold(2), 2048);

    // 3. Table 3: Low override spills small rows
    engine.set_table_inline_threshold(3, 256);
    assert_eq!(engine.effective_inline_threshold(3), 256);
    let val_500 = vec![0x77u8; 500];
    let prev_blob_len = engine.active_std_writer.as_ref().unwrap().current_len();
    engine
        .put_row(3, b"k3", &val_500, &[], WriteDurability::BufferedFast)
        .unwrap();
    assert!(engine.active_std_writer.as_ref().unwrap().current_len() > prev_blob_len);

    // 4. Dropping table cleans up threshold
    engine.drop_table(3, None).unwrap();
    assert_eq!(engine.effective_inline_threshold(3), 3584);
}

#[test]
fn block_cache_and_blob_cache_hits_and_segment_invalidation_lifecycle() {
    let tmp = TempTestDir::new("cache_lifecycle_test");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut cfg = EngineConfig::default();
    cfg.block_cache_bytes = 64 * 1024;
    cfg.blob_cache_bytes = 64 * 1024;
    cfg.inline_threshold_bytes = 1024;
    let mut engine = StorageEngine::open(fs, cfg).unwrap();

    assert!(engine.block_cache().is_enabled());
    assert!(engine.blob_cache().is_enabled());

    // 1. Populate 100 inline rows
    for i in 0..100u32 {
        let k = format!("k_{i:04}").into_bytes();
        let v = format!("val_{i:04}").into_bytes();
        engine
            .put_row(1, &k, &v, &[], WriteDurability::BufferedFast)
            .unwrap();
    }
    // Also write a 2048 B blob
    let blob_val = vec![0xBBu8; 2048];
    engine
        .put_row(
            1,
            b"blob_row",
            &blob_val,
            &[],
            WriteDurability::BufferedFast,
        )
        .unwrap();

    // Flush to create segment 1 and sealed blob segment
    engine.flush_memtable().unwrap();
    assert_eq!(engine.idx_segments.len(), 1);

    let _initial_block_hits = engine.block_cache().hits();
    let _initial_blob_hits = engine.blob_cache().hits();

    // Pass 1: read all rows -> cache misses on initial block load, populated into cache
    for i in 0..100u32 {
        let k = format!("k_{i:04}").into_bytes();
        assert!(engine.get_row(1, &k).unwrap().is_some());
    }
    assert_eq!(
        engine.get_row(1, b"blob_row").unwrap(),
        Some(blob_val.clone())
    );

    // Pass 2: read all rows again -> must HIT verified block cache and blob cache!
    let block_hits_after_p1 = engine.block_cache().hits();
    for i in 0..100u32 {
        let k = format!("k_{i:04}").into_bytes();
        assert!(engine.get_row(1, &k).unwrap().is_some());
    }
    assert!(
        engine.block_cache().hits() > block_hits_after_p1,
        "Pass 2 point reads must hit VerifiedBlockCache"
    );

    let blob_hits_after_p1 = engine.blob_cache().hits();
    assert_eq!(engine.get_row(1, b"blob_row").unwrap(), Some(blob_val));
    assert!(
        engine.blob_cache().hits() > blob_hits_after_p1,
        "Pass 2 blob read must hit VerifiedBlobCache"
    );

    // Compacting segments invalidates retired segment 1
    // Write another batch and flush so there are 2 segments for compact() to merge!
    engine
        .put_row(
            1,
            b"another_row",
            b"val",
            &[],
            WriteDurability::BufferedFast,
        )
        .unwrap();
    engine.flush_memtable().unwrap();
    assert_eq!(engine.idx_segments.len(), 2);

    let seg1_id = engine.idx_segments[0].seq_id();
    // Verify block was resident in cache before compaction
    let mut out_block = [0u8; 4096];
    assert!(engine.block_cache().get_into(seg1_id, 0, &mut out_block));

    engine.compact().unwrap();
    // Segment 1 blocks are purged from cache upon retirement!
    assert!(!engine.block_cache().get_into(seg1_id, 0, &mut out_block));
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
            None,
            None,
            false,
            usize::MAX,
            true,
            &mut pks,
            &mut slab,
            &mut spans,
            None,
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
                lower_pk.as_deref().map(|pk| (pk, false)),
                None,
                false,
                256,
                true,
                &mut pks,
                &mut slab,
                &mut spans,
                None,
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
fn rollback_without_active_transaction_is_a_noop() {
    let tmp = TempTestDir::new("rollback_noop");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut engine = StorageEngine::open(fs, EngineConfig::default()).unwrap();
    engine
        .put_row(1, b"k", &[0x33u8; 8192], &[], WriteDurability::BufferedFast)
        .unwrap();
    engine.flush_memtable().unwrap();
    let lsn = engine.next_lsn();

    engine.rollback_transaction().unwrap();
    engine.rollback_transaction().unwrap();

    assert_eq!(engine.next_lsn(), lsn);
    engine.flush_memtable().unwrap();
    assert_eq!(engine.get_row(1, b"k").unwrap(), Some(vec![0x33u8; 8192]));
}

#[test]
fn sync_commit_hitting_wal_ring_full_survives_crash() {
    let tmp = TempTestDir::new("tx_ring_full");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    // Each single-statement sync transaction occupies one sector of the 8-sector sync WAL.
    let mut cfg = EngineConfig::default();
    cfg.sync_wal_size_bytes = 8 * 4096;
    let mut engine = StorageEngine::open(fs.clone(), cfg.clone()).unwrap();
    for i in 0..9u32 {
        engine.begin_transaction();
        engine
            .put_row(
                1,
                &i.to_be_bytes(),
                b"value",
                &[],
                WriteDurability::SyncTransactional,
            )
            .unwrap();
        engine.commit_transaction().unwrap();
    }
    drop(engine);

    let mut engine = StorageEngine::open(fs, cfg).unwrap();
    for i in 0..9u32 {
        assert!(
            engine.get_row(1, &i.to_be_bytes()).unwrap().is_some(),
            "committed row {i} lost after crash"
        );
    }
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
fn transient_blob_read_error_during_recovery_fails_open_without_dropping_commit() {
    use crate::pal::PreallocStrategy;
    use std::path::Path;
    use std::sync::atomic::AtomicBool;

    struct BlobReadFaultFile {
        inner: Arc<<OsFs as PlatformFs>::File>,
        is_blob: bool,
        fail_blob_reads: Arc<AtomicBool>,
    }

    impl PlatformFile for BlobReadFaultFile {
        fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
            self.inner.read_at(buf, offset)
        }
        fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
            if self.is_blob && self.fail_blob_reads.load(Ordering::SeqCst) {
                return Err(io::Error::other("simulated transient read error"));
            }
            self.inner.read_exact_at(buf, offset)
        }
        fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
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

    struct BlobReadFaultFs {
        inner: OsFs,
        fail_blob_reads: Arc<AtomicBool>,
    }

    impl PlatformFs for BlobReadFaultFs {
        type File = BlobReadFaultFile;

        fn open_file(
            &self,
            rel_name: &str,
            create: bool,
            writable: bool,
            hint: OpenHint,
        ) -> io::Result<Arc<Self::File>> {
            Ok(Arc::new(BlobReadFaultFile {
                inner: self.inner.open_file(rel_name, create, writable, hint)?,
                is_blob: rel_name.starts_with("blob_"),
                fail_blob_reads: Arc::clone(&self.fail_blob_reads),
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

    let tmp = TempTestDir::new("recovery_transient_blob_error");
    let fail_flag = Arc::new(AtomicBool::new(false));
    let make_fs = || {
        Arc::new(BlobReadFaultFs {
            inner: OsFs::open(&tmp.path, true).unwrap(),
            fail_blob_reads: Arc::clone(&fail_flag),
        })
    };
    let value = vec![0x11u8; 8192];
    {
        let mut engine = StorageEngine::open(make_fs(), EngineConfig::default()).unwrap();
        engine
            .put_row(1, b"k", &value, &[], WriteDurability::SyncTransactional)
            .unwrap();
    } // crash: the row exists only in the WAL and an uncataloged blob segment

    fail_flag.store(true, Ordering::SeqCst);
    assert!(
        StorageEngine::open(make_fs(), EngineConfig::default()).is_err(),
        "open must fail when blob validation hits an I/O error"
    );

    fail_flag.store(false, Ordering::SeqCst);
    let mut engine = StorageEngine::open(make_fs(), EngineConfig::default()).unwrap();
    assert_eq!(engine.get_row(1, b"k").unwrap(), Some(value));
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
        engine.meta_mgr.state().next_seq_id
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
fn drop_table_with_tombstones_larger_than_wal_is_atomic_and_durable() {
    let tmp = TempTestDir::new("drop_table_larger_than_wal");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut cfg = EngineConfig::default();
    cfg.buf_wal_size_bytes = 64 * 1024;
    let mut engine = StorageEngine::open(fs.clone(), cfg.clone()).unwrap();
    for i in 0..20000u32 {
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
    engine
        .put_row(2, b"keep", b"v", &[], WriteDurability::BufferedFast)
        .unwrap();
    engine.flush_memtable().unwrap();

    engine.drop_table(1, None).unwrap();
    assert!(engine.get_row(1, &0u32.to_be_bytes()).unwrap().is_none());
    assert!(
        engine
            .get_row(1, &19999u32.to_be_bytes())
            .unwrap()
            .is_none()
    );
    drop(engine);

    let mut engine = StorageEngine::open(fs, cfg).unwrap();
    for i in [0u32, 9999, 19999] {
        assert!(engine.get_row(1, &i.to_be_bytes()).unwrap().is_none());
    }
    assert_eq!(engine.get_row(2, b"keep").unwrap(), Some(b"v".to_vec()));
}

#[test]
fn explicit_table_id_cannot_alias_or_remap_a_persisted_table() {
    let tmp = TempTestDir::new("explicit_table_id_alias");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut engine = StorageEngine::open(fs, EngineConfig::default()).unwrap();
    let plan = engine.plan_vtab_registration("alpha", Some(5)).unwrap();
    let (id, token) = engine
        .register_vtab_plan(plan, WriteDurability::BufferedFast, None)
        .unwrap();
    assert_eq!(id, 5);
    engine
        .put_row(id, b"k", b"alpha-data", &[], WriteDurability::BufferedFast)
        .unwrap();
    engine.unregister_vtab_token(id, token);

    // Another name must not take over the persisted id.
    let err = engine
        .register_or_resolve_vtab("beta", Some(5))
        .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    // The persisted name must not be re-bound to a different id.
    let err = engine
        .register_or_resolve_vtab("alpha", Some(6))
        .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);

    // The original binding still resolves.
    assert_eq!(
        engine.register_or_resolve_vtab("alpha", Some(5)).unwrap(),
        5
    );
    assert_eq!(engine.register_or_resolve_vtab("alpha", None).unwrap(), 5);
}

#[test]
fn buffered_recovery_stops_at_first_lost_blob() {
    let tmp = TempTestDir::new("buf_prefix_recovery");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut cfg = EngineConfig::default();
    cfg.inline_threshold_bytes = 256;
    cfg.max_blob_segment_bytes = 2048; // One 1,500 B blob per segment.

    let payload = vec![0x5Au8; 1500];
    {
        let mut engine = StorageEngine::open(fs.clone(), cfg.clone()).unwrap();
        for k in [b"k1", b"k2", b"k3"] {
            engine
                .put_row(1, k, &payload, &[], WriteDurability::BufferedFast)
                .unwrap();
        }
        // Dropped without flushing: simulates a crash.
    }

    // Cut the blob segment holding k2 so it fails validation while k3's segment survives.
    let mut segs: Vec<PathBuf> = std::fs::read_dir(&tmp.path)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("blob_"))
        })
        .collect();
    segs.sort();
    assert!(segs.len() >= 3, "expected one blob segment per row");
    let f = std::fs::OpenOptions::new()
        .write(true)
        .open(&segs[1])
        .unwrap();
    f.set_len(0).unwrap();
    drop(f);

    let mut engine = StorageEngine::open(fs, cfg).unwrap();
    assert_eq!(engine.get_row(1, b"k1").unwrap().unwrap(), payload);
    assert!(engine.get_row(1, b"k2").unwrap().is_none());
    assert!(
        engine.get_row(1, b"k3").unwrap().is_none(),
        "a buffered commit after a rolled-back one must not be replayed"
    );
}

#[test]
fn open_reconciles_capped_rows_into_the_rotated_wal_epoch() {
    let tmp = TempTestDir::new("reconcile_after_rotation");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let cfg = EngineConfig::default();

    {
        let mut engine = StorageEngine::open(fs.clone(), cfg.clone()).unwrap();
        for k in [b"a", b"b", b"c"] {
            engine
                .put_row(1, k, b"value", &[], WriteDurability::BufferedFast)
                .unwrap();
        }
        engine.flush_memtable().unwrap();
        // Persist a quota for table 1 although its rows carry no row-meta or LRU entries.
        engine
            .meta_mgr
            .commit_update(|st| {
                st.table_quotas = vec![crate::meta::TableQuotaMeta {
                    table_id: 1,
                    eviction_policy: EvictionPolicy::Lru,
                    max_bytes: 1 << 20,
                    current_logical_bytes: 0,
                }];
            })
            .unwrap();
        // A WAL record that is not checkpointed makes the next open replay, which is what
        // forces the full reconciliation; without it the persisted totals are trusted.
        engine
            .put_row(1, b"d", b"value", &[], WriteDurability::BufferedFast)
            .unwrap();
    }

    let epochs = {
        let engine = StorageEngine::open(fs.clone(), cfg.clone()).unwrap();
        // Reconciliation appended its records after the epoch rotation, so they are still
        // pending in the MemTable and backed only by the fresh WAL epoch.
        assert!(!engine.memtable.is_empty());
        assert_eq!(
            engine.sync_wal.active_epoch(),
            engine.meta_mgr.state().sync_wal_epoch
        );
        assert_eq!(
            engine.buf_wal.active_epoch(),
            engine.meta_mgr.state().buf_wal_epoch
        );
        assert!(engine.capped_table_state(1).unwrap().current_logical_bytes > 0);
        (
            engine.sync_wal.active_epoch(),
            engine.buf_wal.active_epoch(),
        )
        // Dropped without flushing: simulates a crash.
    };

    let mut engine = StorageEngine::open(fs, cfg).unwrap();
    assert!(engine.sync_wal.active_epoch() > epochs.0);
    assert!(engine.buf_wal.active_epoch() > epochs.1);
    for k in [b"a", b"b", b"c"] {
        assert_eq!(engine.get_row(1, k).unwrap(), Some(b"value".to_vec()));
    }
    assert!(engine.capped_table_state(1).unwrap().current_logical_bytes > 0);
}

/// Counts `sync_durable` calls on blob segment files and records the blob sync count observed
/// at the time of each WAL sync.
mod blob_sync_counting {
    use super::*;
    use crate::pal::PreallocStrategy;
    use std::path::Path;
    use std::sync::atomic::AtomicU64;

    #[derive(Default)]
    pub struct Ctl {
        pub blob_syncs: AtomicU64,
        pub blob_syncs_at_wal_sync: AtomicU64,
        pub fail_blob_sync: std::sync::atomic::AtomicBool,
    }
    pub struct CountFile<F> {
        inner: Arc<F>,
        name: String,
        ctl: Arc<Ctl>,
    }
    impl<F> CountFile<F> {
        fn record_wal_sync(&self) {
            if self.name.starts_with("wal_sync") {
                self.ctl
                    .blob_syncs_at_wal_sync
                    .store(self.ctl.blob_syncs.load(Ordering::SeqCst), Ordering::SeqCst);
            }
        }
    }
    impl<F: PlatformFile> PlatformFile for CountFile<F> {
        fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
            self.inner.read_at(buf, offset)
        }
        fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
            self.inner.read_exact_at(buf, offset)
        }
        fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
            self.inner.write_all_at(buf, offset)
        }
        fn file_len(&self) -> io::Result<u64> {
            self.inner.file_len()
        }
        fn preallocate(&self, target_len: u64, strategy: PreallocStrategy) -> io::Result<()> {
            self.inner.preallocate(target_len, strategy)
        }
        fn sync_barrier(&self) -> io::Result<()> {
            self.record_wal_sync();
            self.inner.sync_barrier()
        }
        fn sync_durable(&self) -> io::Result<()> {
            self.record_wal_sync();
            if self.name.starts_with("blob_") {
                if self.ctl.fail_blob_sync.load(Ordering::SeqCst) {
                    return Err(io::Error::other("simulated blob sync failure"));
                }
                self.ctl.blob_syncs.fetch_add(1, Ordering::SeqCst);
            }
            self.inner.sync_durable()
        }
        fn evict_os_cache(&self, offset: u64, len: u64) -> io::Result<()> {
            self.inner.evict_os_cache(offset, len)
        }
    }
    pub struct CountFs {
        pub inner: OsFs,
        pub ctl: Arc<Ctl>,
    }
    impl PlatformFs for CountFs {
        type File = CountFile<<OsFs as PlatformFs>::File>;
        fn open_file(
            &self,
            rel_name: &str,
            create: bool,
            writable: bool,
            hint: OpenHint,
        ) -> io::Result<Arc<Self::File>> {
            let inner = self.inner.open_file(rel_name, create, writable, hint)?;
            Ok(Arc::new(CountFile {
                inner,
                name: rel_name.to_string(),
                ctl: Arc::clone(&self.ctl),
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
}

#[test]
fn inline_sync_commit_does_not_fsync_idle_blob_segments() {
    use blob_sync_counting::{CountFs, Ctl};

    let tmp = TempTestDir::new("idle_blob_sync");
    let ctl = Arc::new(Ctl::default());
    let fs = Arc::new(CountFs {
        inner: OsFs::open(&tmp.path, true).unwrap(),
        ctl: Arc::clone(&ctl),
    });
    let mut engine = StorageEngine::open(fs, EngineConfig::default()).unwrap();
    let sync = WriteDurability::SyncTransactional;

    // A blob commit must sync the blob segment before its WAL record is made durable.
    engine.begin_transaction();
    engine
        .put_row(1, b"blob", &vec![1u8; 8192], &[], sync)
        .unwrap();
    engine.commit_transaction().unwrap();
    let after_blob = ctl.blob_syncs.load(Ordering::SeqCst);
    assert!(after_blob >= 1);
    assert_eq!(
        ctl.blob_syncs_at_wal_sync.load(Ordering::SeqCst),
        after_blob
    );

    for i in 0..100u32 {
        engine.begin_transaction();
        engine
            .put_row(1, &i.to_be_bytes(), b"inline", &[], sync)
            .unwrap();
        engine.prepare_commit_transaction(sync).unwrap();
        engine.commit_transaction().unwrap();
    }
    assert_eq!(ctl.blob_syncs.load(Ordering::SeqCst), after_blob);

    // A later blob write is synced again.
    engine.begin_transaction();
    engine
        .put_row(1, b"blob2", &vec![2u8; 8192], &[], sync)
        .unwrap();
    engine.commit_transaction().unwrap();
    assert!(ctl.blob_syncs.load(Ordering::SeqCst) > after_blob);
}

#[test]
fn failed_blob_sync_poisons_instead_of_being_retried() {
    use blob_sync_counting::{CountFs, Ctl};

    let tmp = TempTestDir::new("blob_sync_poison");
    let ctl = Arc::new(Ctl::default());
    let fs = Arc::new(CountFs {
        inner: OsFs::open(&tmp.path, true).unwrap(),
        ctl: Arc::clone(&ctl),
    });
    let mut engine = StorageEngine::open(fs, EngineConfig::default()).unwrap();
    let sync = WriteDurability::SyncTransactional;
    engine.begin_transaction();
    engine
        .put_row(1, b"blob", &vec![3u8; 8192], &[], sync)
        .unwrap();

    ctl.fail_blob_sync.store(true, Ordering::SeqCst);
    assert!(engine.prepare_commit_transaction(sync).is_err());
    ctl.fail_blob_sync.store(false, Ordering::SeqCst);
    // A retried sync could report success for data the kernel already dropped, so the failure
    // poisons the engine and the commit cannot be retried.
    assert!(engine.is_poisoned());
    let before = ctl.blob_syncs.load(Ordering::SeqCst);
    assert!(engine.prepare_commit_transaction(sync).is_err());
    assert!(engine.commit_transaction().is_err());
    assert_eq!(ctl.blob_syncs.load(Ordering::SeqCst), before);
}

#[test]
fn checkpoint_is_rejected_during_a_transaction() {
    let tmp = TempTestDir::new("checkpoint_in_tx");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut engine = StorageEngine::open(fs, EngineConfig::default()).unwrap();
    engine.begin_transaction();
    let err = engine.checkpoint().unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    engine.rollback_transaction().unwrap();
    engine.checkpoint().unwrap();
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
    let committed = engine.meta_mgr.state().std_blob_segments[0].file_len;
    let in_memory = engine.std_blobs.values().next().unwrap().file_len;
    assert!(in_memory > committed);

    engine.checkpoint().unwrap();
    let st = engine.meta_mgr.state();
    assert_eq!(st.std_blob_segments.len(), 1);
    assert_eq!(st.std_blob_segments[0].file_len, in_memory);
    let dead = st.std_blob_segments[0].dead_bytes;
    assert!(dead > 0);
    drop(engine);

    let mut engine = StorageEngine::open(fs, cfg).unwrap();
    let seg = &engine.meta_mgr.state().std_blob_segments[0];
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
    assert_eq!(engine.meta_mgr.state().idx_segments.len(), 1);
    drop(engine);
    let mut engine = StorageEngine::open(fs, cfg).unwrap();
    assert!(engine.memtable.is_empty());
    assert_eq!(engine.get_row(1, b"k").unwrap(), Some(b"v".to_vec()));
}

#[test]
fn clean_reopen_skips_capped_reconcile_and_keeps_tick_order() {
    let tmp = TempTestDir::new("capped_fast_reopen");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut cfg = EngineConfig::default();
    cfg.inline_threshold_bytes = 4096;
    let wal = WriteDurability::BufferedFast;
    let payload = vec![0x42u8; 1000];

    let mut engine = StorageEngine::open(fs.clone(), cfg.clone()).unwrap();
    engine.register_capped_table(1, 3_200).unwrap();
    for k in [b"k1", b"k2", b"k3"] {
        engine.put_row(1, k, &payload, &[], wal).unwrap();
    }
    let bytes = engine.capped_table_state(1).unwrap().current_logical_bytes;
    assert!(bytes > 0);
    engine.checkpoint().unwrap();
    drop(engine);

    // Reopen after the checkpoint: byte totals are taken from boot_meta and the new row gets a
    // tick above every existing one, so the oldest row is the one evicted.
    let mut engine = StorageEngine::open(fs.clone(), cfg.clone()).unwrap();
    assert_eq!(
        engine.capped_table_state(1).unwrap().current_logical_bytes,
        bytes
    );
    assert!(engine.access_tracker.next_tick() > 3);
    engine.put_row(1, b"k4", &payload, &[], wal).unwrap();
    assert_eq!(engine.get_row(1, b"k1").unwrap(), None);
    for k in [b"k2", b"k3", b"k4"] {
        assert_eq!(
            engine.get_row(1, k).unwrap(),
            Some(payload.clone()),
            "{k:?}"
        );
    }
    engine.checkpoint().unwrap();
    drop(engine);

    // The persisted total is authoritative when nothing was replayed: a deliberately altered
    // value survives the open, which a full row scan would have corrected.
    {
        let mut engine = StorageEngine::open(fs.clone(), cfg.clone()).unwrap();
        engine
            .meta_mgr
            .commit_update(|st| st.table_quotas[0].current_logical_bytes = 12_345)
            .unwrap();
    }
    let engine = StorageEngine::open(fs, cfg).unwrap();
    assert_eq!(
        engine.capped_table_state(1).unwrap().current_logical_bytes,
        12_345
    );
}

#[test]
fn crash_with_wal_contents_runs_full_capped_reconcile() {
    let tmp = TempTestDir::new("capped_full_reopen");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut cfg = EngineConfig::default();
    cfg.inline_threshold_bytes = 4096;
    let wal = WriteDurability::BufferedFast;
    let payload = vec![0x42u8; 1000];

    let mut engine = StorageEngine::open(fs.clone(), cfg.clone()).unwrap();
    engine.register_capped_table(1, 100_000).unwrap();
    engine.put_row(1, b"k1", &payload, &[], wal).unwrap();
    engine.checkpoint().unwrap();
    let flushed_bytes = engine.capped_table_state(1).unwrap().current_logical_bytes;
    engine.put_row(1, b"k2", &payload, &[], wal).unwrap();
    engine.put_row(1, b"k3", &payload, &[], wal).unwrap();
    let expected = engine.capped_table_state(1).unwrap().current_logical_bytes;
    assert!(expected > flushed_bytes);
    drop(engine); // crash: k2 and k3 exist only in the WAL

    let mut engine = StorageEngine::open(fs, cfg).unwrap();
    assert_eq!(
        engine.capped_table_state(1).unwrap().current_logical_bytes,
        expected
    );
    for k in [b"k1", b"k2", b"k3"] {
        assert_eq!(
            engine.get_row(1, k).unwrap(),
            Some(payload.clone()),
            "{k:?}"
        );
    }
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

const GROUP_SYNC: WriteDurability = WriteDurability::SyncTransactional;

fn open_group_engine(tmp: &TempTestDir) -> StorageEngine<OsFs> {
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    StorageEngine::open(fs, EngineConfig::default()).unwrap()
}

fn row_visible_to(engine: &StorageEngine<OsFs>, conn: usize, key: &[u8]) -> Option<Vec<u8>> {
    let (mut slab, mut spans) = (Vec::new(), Vec::new());
    engine
        .reader_for_conn(conn)
        .get_row_into(1, key, &mut slab, &mut spans)
        .unwrap()
        .then_some(slab)
}

type GroupTicket = (u64, Arc<<OsFs as PlatformFs>::File>, Arc<tx::CommitGate>);

fn commit_into_group(
    engine: &mut StorageEngine<OsFs>,
    conn: usize,
    key: &[u8],
    value: &[u8],
) -> GroupTicket {
    engine.begin_transaction_for_conn(conn).unwrap();
    engine.put_row(1, key, value, &[], GROUP_SYNC).unwrap();
    engine.prepare_commit_transaction(GROUP_SYNC).unwrap();
    match engine.begin_commit_for_conn(conn).unwrap() {
        CommitStep::AwaitingGroup { lsn, file, gate } => (lsn, file, gate),
        _ => panic!("a plain synchronous transaction must join the group commit"),
    }
}

fn flush_group(engine: &mut StorageEngine<OsFs>, ticket: &GroupTicket) -> u64 {
    let GroupRole::Lead(target) = ticket.2.join_group(ticket.0).unwrap() else {
        panic!("no flush has run, so this commit must lead");
    };
    engine
        .finish_group_flush(target, ticket.1.sync_barrier())
        .unwrap();
    target
}

#[test]
fn group_commit_is_visible_to_the_next_writer_but_not_to_readers_until_durable() {
    let tmp = TempTestDir::new("group_visibility");
    let mut engine = open_group_engine(&tmp);
    let first = commit_into_group(&mut engine, 1, b"a", b"1");

    assert!(
        !engine.in_active_transaction(),
        "the writer slot must be free"
    );
    assert_eq!(row_visible_to(&engine, 2, b"a"), None);

    engine.begin_transaction_for_conn(2).unwrap();
    assert_eq!(engine.get_row(1, b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(
        row_visible_to(&engine, 3, b"a"),
        None,
        "readers must wait for durability"
    );
    engine.rollback_transaction().unwrap();

    flush_group(&mut engine, &first);
    assert!(matches!(
        first.2.join_group(first.0).unwrap(),
        GroupRole::Durable
    ));
    assert_eq!(row_visible_to(&engine, 3, b"a"), Some(b"1".to_vec()));
}

#[test]
fn one_flush_makes_every_appended_commit_durable_in_order() {
    let tmp = TempTestDir::new("group_order");
    let mut engine = open_group_engine(&tmp);
    let first = commit_into_group(&mut engine, 1, b"k", b"old");
    let second = commit_into_group(&mut engine, 2, b"k", b"new");
    assert_eq!(
        engine.pending_group[1].1.len(),
        1,
        "a batch must hold only its own writes"
    );

    assert_eq!(flush_group(&mut engine, &first), second.0);
    assert!(matches!(
        second.2.join_group(second.0).unwrap(),
        GroupRole::Durable
    ));
    assert!(engine.pending_group.is_empty());
    assert_eq!(row_visible_to(&engine, 9, b"k"), Some(b"new".to_vec()));
}

#[test]
fn rolling_back_a_later_writer_keeps_the_earlier_group_commit() {
    let tmp = TempTestDir::new("group_rollback");
    let mut engine = open_group_engine(&tmp);
    let first = commit_into_group(&mut engine, 1, b"a", b"1");
    engine.begin_transaction_for_conn(2).unwrap();
    engine.put_row(1, b"a", b"2", &[], GROUP_SYNC).unwrap();
    engine.rollback_transaction_for_conn(2).unwrap();

    flush_group(&mut engine, &first);
    assert_eq!(row_visible_to(&engine, 9, b"a"), Some(b"1".to_vec()));
}

#[test]
fn failed_group_flush_poisons_the_engine_and_fails_every_waiter() {
    let tmp = TempTestDir::new("group_failure");
    let mut engine = open_group_engine(&tmp);
    let first = commit_into_group(&mut engine, 1, b"a", b"1");
    let second = commit_into_group(&mut engine, 2, b"b", b"2");
    let GroupRole::Lead(target) = first.2.join_group(first.0).unwrap() else {
        panic!("this commit must lead");
    };
    let flushed = Err(io::Error::other("simulated flush failure"));
    assert!(engine.finish_group_flush(target, flushed).is_err());

    assert!(engine.is_poisoned());
    assert!(second.2.join_group(second.0).is_err());
    assert_eq!(row_visible_to(&engine, 9, b"a"), None);
}

#[test]
fn poisoned_engine_refuses_pending_group_drain_and_finish_group_flush() {
    let tmp = TempTestDir::new("poisoned_group_bypass");
    let mut engine = open_group_engine(&tmp);
    let ticket = commit_into_group(&mut engine, 1, b"a", b"1");
    let GroupRole::Lead(target) = ticket.2.join_group(ticket.0).unwrap() else {
        panic!("this commit must lead");
    };

    // Simulate an unrelated write failure poisoning the engine while the leader synced.
    let _ = engine.poison(io::Error::other("simulated prior wal failure"));

    assert!(engine.finish_group_flush(target, Ok(())).is_err());
    assert!(engine.drain_group_for_autocommit().is_err());
    assert!(
        engine
            .reconcile_unique_keys(1, b"a", &[], WriteDurability::BufferedFast)
            .is_err()
    );
    assert!(
        engine
            .plan_assign_rowid(1, b"a", 1, |_, _| Ok(false))
            .is_err()
    );
    assert!(engine.plan_set_rowid(1, b"a", 1).is_err());

    assert!(!engine.pending_group.is_empty());
    assert_eq!(row_visible_to(&engine, 9, b"a"), None);
    assert!(ticket.2.join_group(ticket.0).is_err());
}

#[test]
fn group_commit_leader_guard_wakes_followers_on_unwind() {
    let tmp = TempTestDir::new("group_leader_unwind");
    let mut engine = open_group_engine(&tmp);
    let first = commit_into_group(&mut engine, 1, b"a", b"1");
    let second = commit_into_group(&mut engine, 2, b"b", b"2");
    let GroupRole::Lead(_) = first.2.join_group(first.0).unwrap() else {
        panic!("expected leader role");
    };

    let follower_gate = Arc::clone(&second.2);
    let follower_lsn = second.0;
    let handle = std::thread::spawn(move || follower_gate.join_group(follower_lsn));

    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = first.2.leader_guard();
        panic!("simulated leader panic during sync_barrier");
    }));
    assert!(res.is_err());

    let follower_res = handle.join().unwrap();
    assert!(
        follower_res.is_err(),
        "abandoned leader must fail followers instead of hanging"
    );
}

#[test]
fn memtable_flush_includes_commits_still_waiting_for_their_group_flush() {
    let tmp = TempTestDir::new("group_flush_memtable");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut engine = StorageEngine::open(Arc::clone(&fs), EngineConfig::default()).unwrap();
    let ticket = commit_into_group(&mut engine, 1, b"a", b"1");

    engine.flush_memtable().unwrap();
    assert!(engine.pending_group.is_empty());
    assert!(matches!(
        ticket.2.join_group(ticket.0).unwrap(),
        GroupRole::Durable
    ));

    drop(ticket);
    drop(engine);
    let mut reopened = StorageEngine::open(fs, EngineConfig::default()).unwrap();
    assert_eq!(reopened.get_row(1, b"a").unwrap(), Some(b"1".to_vec()));
}

#[test]
fn buffered_commit_first_makes_earlier_group_commits_durable() {
    let tmp = TempTestDir::new("group_then_buffered");
    let mut engine = open_group_engine(&tmp);
    let ticket = commit_into_group(&mut engine, 1, b"a", b"1");
    engine.begin_transaction_for_conn(2).unwrap();
    engine
        .put_row(1, b"b", b"2", &[], WriteDurability::BufferedFast)
        .unwrap();
    engine
        .prepare_commit_transaction(WriteDurability::BufferedFast)
        .unwrap();
    assert!(matches!(
        engine.begin_commit_for_conn(2).unwrap(),
        CommitStep::Done
    ));

    assert!(matches!(
        ticket.2.join_group(ticket.0).unwrap(),
        GroupRole::Durable
    ));
    assert_eq!(row_visible_to(&engine, 9, b"a"), Some(b"1".to_vec()));
    assert_eq!(row_visible_to(&engine, 9, b"b"), Some(b"2".to_vec()));
}

#[test]
fn publish_committed_transaction_does_not_reabsorb_flushed_pending_group_entries() {
    let seeded_row_key = encode_primary_row_key(1, b"seeded_k");

    // 1. Direct commit path (`BufferedFast` WAL rotation flushes `pending_group` entries into an `idx` segment).
    {
        let tmp = TempTestDir::new("pending_group_no_reabsorb_buf");
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
        let mut cfg = EngineConfig::default();
        cfg.inline_threshold_bytes = 8192;
        cfg.sync_wal_size_bytes = 8 * 4096;
        cfg.buf_wal_size_bytes = 8 * 4096;
        let mut engine = StorageEngine::open(fs, cfg).unwrap();

        for i in 0..9u32 {
            engine
                .put_row(
                    1,
                    format!("pre_{i}").as_bytes(),
                    &[b'p'; 2500],
                    &[],
                    WriteDurability::BufferedFast,
                )
                .unwrap();
        }
        let ticket = commit_into_group(&mut engine, 1, b"seeded_k", b"v1");
        engine.begin_transaction_for_conn(2).unwrap();
        assert!(
            !engine.tx_overlay.map.contains_key(&seeded_row_key),
            "pending_group entries must be shared directly without cloning into tx_overlay"
        );
        assert_eq!(
            engine.get_row(1, b"seeded_k").unwrap(),
            Some(b"v1".to_vec())
        );
        for i in 0..4u32 {
            let k = format!("tx2_{i:02}");
            engine
                .put_row(
                    1,
                    k.as_bytes(),
                    &[b'x'; 2500],
                    &[],
                    WriteDurability::BufferedFast,
                )
                .unwrap();
        }
        engine.commit_transaction_for_conn(2).unwrap();

        assert!(
            !engine.idx_segments.is_empty(),
            "RingFull during buffered WAL append must flush MemTable to an idx segment"
        );
        assert!(
            engine.memtable.get(&seeded_row_key).is_none(),
            "seeded entry already flushed to idx must not be re-absorbed into MemTable"
        );
        assert_eq!(
            engine.get_row(1, b"seeded_k").unwrap(),
            Some(b"v1".to_vec())
        );
        assert!(engine.get_row(1, b"tx2_00").unwrap().is_some());
        assert!(matches!(
            ticket.2.join_group(ticket.0).unwrap(),
            GroupRole::Durable
        ));
    }

    // 2. Two-phase non-groupable `AwaitingSync` + `finish_commit_for_conn` path.
    {
        let tmp = TempTestDir::new("pending_group_no_reabsorb_sync");
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
        let mut cfg = EngineConfig::default();
        cfg.inline_threshold_bytes = 8192;
        cfg.sync_wal_size_bytes = 8 * 4096;
        cfg.buf_wal_size_bytes = 8 * 4096;
        let mut engine = StorageEngine::open(fs, cfg).unwrap();

        for i in 0..6u32 {
            engine
                .put_row(
                    1,
                    format!("pre_{i}").as_bytes(),
                    &[b'p'; 2500],
                    &[],
                    GROUP_SYNC,
                )
                .unwrap();
        }
        let ticket = commit_into_group(&mut engine, 1, b"seeded_k", b"v1");
        engine.begin_transaction_for_conn(2).unwrap();
        engine.register_capped_table(9, 100_000).unwrap();
        for i in 0..3u32 {
            let k = format!("tx2_{i:02}");
            engine
                .put_row(1, k.as_bytes(), &[b'y'; 2500], &[], GROUP_SYNC)
                .unwrap();
        }
        engine.prepare_commit_transaction(GROUP_SYNC).unwrap();
        let CommitStep::AwaitingSync(file) = engine.begin_commit_for_conn(2).unwrap() else {
            panic!("tx with non-empty capped_tables must take AwaitingSync path");
        };
        engine.finish_commit_for_conn(file.sync_barrier()).unwrap();

        assert!(!engine.idx_segments.is_empty());
        assert!(
            engine.memtable.get(&seeded_row_key).is_none(),
            "finish_commit_for_conn must not re-absorb flushed pending_group entries"
        );
        assert_eq!(
            engine.get_row(1, b"seeded_k").unwrap(),
            Some(b"v1".to_vec())
        );
        assert!(engine.get_row(1, b"tx2_00").unwrap().is_some());
        assert!(matches!(
            ticket.2.join_group(ticket.0).unwrap(),
            GroupRole::Durable
        ));
    }
}

#[test]
fn transaction_without_writes_does_not_publish_undurable_commits() {
    let tmp = TempTestDir::new("group_read_only");
    let mut engine = open_group_engine(&tmp);
    let _ticket = commit_into_group(&mut engine, 1, b"a", b"1");
    engine.begin_transaction_for_conn(2).unwrap();
    engine.commit_transaction_for_conn(2).unwrap();
    assert_eq!(row_visible_to(&engine, 9, b"a"), None);
}

#[test]
fn commit_needing_maintenance_flushes_with_the_transaction_still_active() {
    let tmp = TempTestDir::new("deferred_sync_maintenance");
    let mut engine = open_group_engine(&tmp);
    engine.begin_transaction_for_conn(1).unwrap();
    engine.put_row(1, b"k", b"v", &[], GROUP_SYNC).unwrap();
    engine.prepare_commit_transaction(GROUP_SYNC).unwrap();
    engine.deferred_flush_on_commit = true;

    let CommitStep::AwaitingSync(file) = engine.begin_commit_for_conn(1).unwrap() else {
        panic!("a commit with deferred maintenance cannot join the group");
    };
    assert!(engine.in_active_transaction());
    assert_eq!(row_visible_to(&engine, 2, b"k"), None);
    let err = engine.check_write_allowed_for_conn(2).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::WouldBlock);

    engine.finish_commit_for_conn(file.sync_barrier()).unwrap();
    assert!(!engine.in_active_transaction());
    assert_eq!(engine.get_row(1, b"k").unwrap(), Some(b"v".to_vec()));
}

#[test]
fn failed_deferred_flush_poisons_the_engine_and_discards_the_transaction() {
    let tmp = TempTestDir::new("deferred_sync_failure");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut engine = StorageEngine::open(fs, EngineConfig::default()).unwrap();
    engine.begin_transaction_for_conn(1).unwrap();
    engine.put_row(1, b"k", b"v", &[], GROUP_SYNC).unwrap();
    engine.prepare_commit_transaction(GROUP_SYNC).unwrap();
    engine.deferred_flush_on_commit = true;
    assert!(matches!(
        engine.begin_commit_for_conn(1).unwrap(),
        CommitStep::AwaitingSync(_)
    ));

    let flushed = Err(io::Error::other("simulated flush failure"));
    assert!(engine.finish_commit_for_conn(flushed).is_err());
    assert!(engine.is_poisoned());
    assert!(!engine.in_active_transaction());
    assert_eq!(engine.get_row(1, b"k").unwrap(), None);
}

#[test]
fn buffered_commit_is_not_deferred() {
    let tmp = TempTestDir::new("deferred_sync_buffered");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut engine = StorageEngine::open(fs, EngineConfig::default()).unwrap();
    engine.begin_transaction_for_conn(1).unwrap();
    engine
        .put_row(1, b"k", b"v", &[], WriteDurability::BufferedFast)
        .unwrap();
    engine
        .prepare_commit_transaction(WriteDurability::BufferedFast)
        .unwrap();
    assert!(matches!(
        engine.begin_commit_for_conn(1).unwrap(),
        CommitStep::Done
    ));
    assert_eq!(engine.get_row(1, b"k").unwrap(), Some(b"v".to_vec()));
}

/// In-memory filesystem whose power loss reverts every file to its last `sync_durable` /
/// `sync_barrier` image, except `MutableSyncLog` files, which are write-through.
struct PowerLossFs {
    files: std::sync::Mutex<HashMap<String, Arc<PowerLossFile>>>,
    root: PathBuf,
    /// Fault mode shared with every `blob_*` file, see `BLOB_SYNC_FAIL_ONCE`.
    blob_sync_fault: Arc<std::sync::atomic::AtomicU8>,
}

/// Blob sync fault modes of [`PowerLossFs`]. `BLOB_SYNC_FAIL_ONCE` models Linux "fsyncgate":
/// the next `sync_durable` on a blob file fails, the dirty pages are dropped and marked clean,
/// so every later sync reports success without persisting anything (`BLOB_SYNC_LIE`).
const BLOB_SYNC_OK: u8 = 0;
const BLOB_SYNC_FAIL_ONCE: u8 = 1;
const BLOB_SYNC_LIE: u8 = 2;
/// Every `write_all_at` on a blob file fails with `StorageFull`; syncs are unaffected.
const BLOB_WRITE_FAIL: u8 = 3;

struct PowerLossFile {
    durable: std::sync::Mutex<Vec<u8>>,
    cache: std::sync::Mutex<Vec<u8>>,
    write_through: bool,
    /// Set for `blob_*` files: the shared fault mode of the filesystem.
    blob_sync_fault: Option<Arc<std::sync::atomic::AtomicU8>>,
}

impl PowerLossFs {
    fn new() -> Self {
        Self {
            files: std::sync::Mutex::new(HashMap::new()),
            root: PathBuf::from("/sim/power_loss"),
            blob_sync_fault: Arc::new(std::sync::atomic::AtomicU8::new(BLOB_SYNC_OK)),
        }
    }

    fn set_blob_write_failure(&self, on: bool) {
        let mode = if on { BLOB_WRITE_FAIL } else { BLOB_SYNC_OK };
        self.blob_sync_fault.store(mode, Ordering::SeqCst);
    }

    fn arm_blob_sync_failure(&self) {
        self.blob_sync_fault
            .store(BLOB_SYNC_FAIL_ONCE, Ordering::SeqCst);
    }

    fn power_loss(&self) {
        for f in self.files.lock().unwrap().values() {
            let d = f.durable.lock().unwrap().clone();
            *f.cache.lock().unwrap() = d;
        }
    }
}

impl PlatformFile for PowerLossFile {
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
        if self.read_at(buf, offset)? < buf.len() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "eof"));
        }
        Ok(())
    }
    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        if let Some(fault) = &self.blob_sync_fault {
            if fault.load(Ordering::SeqCst) == BLOB_WRITE_FAIL {
                return Err(io::Error::from(io::ErrorKind::StorageFull));
            }
        }
        let end = offset as usize + buf.len();
        let mut images = vec![self.cache.lock().unwrap()];
        if self.write_through {
            images.push(self.durable.lock().unwrap());
        }
        for img in &mut images {
            if end > img.len() {
                img.resize(end, 0);
            }
            img[offset as usize..end].copy_from_slice(buf);
        }
        Ok(())
    }
    fn file_len(&self) -> io::Result<u64> {
        Ok(self.cache.lock().unwrap().len() as u64)
    }
    fn preallocate(
        &self,
        target_len: u64,
        _strategy: crate::pal::PreallocStrategy,
    ) -> io::Result<()> {
        let len = target_len as usize;
        for img in [&self.cache, &self.durable] {
            let mut img = img.lock().unwrap();
            if len > img.len() {
                img.resize(len, 0);
            }
        }
        Ok(())
    }
    fn sync_barrier(&self) -> io::Result<()> {
        self.sync_durable()
    }
    fn sync_durable(&self) -> io::Result<()> {
        if let Some(fault) = &self.blob_sync_fault {
            match fault.load(Ordering::SeqCst) {
                BLOB_SYNC_FAIL_ONCE => {
                    fault.store(BLOB_SYNC_LIE, Ordering::SeqCst);
                    return Err(io::Error::other("injected blob sync failure"));
                }
                BLOB_SYNC_LIE => return Ok(()),
                _ => {}
            }
        }
        let c = self.cache.lock().unwrap().clone();
        *self.durable.lock().unwrap() = c;
        Ok(())
    }
    fn evict_os_cache(&self, _offset: u64, _len: u64) -> io::Result<()> {
        Ok(())
    }
}

impl PlatformFs for PowerLossFs {
    type File = PowerLossFile;
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
        let f = Arc::new(PowerLossFile {
            durable: std::sync::Mutex::new(Vec::new()),
            cache: std::sync::Mutex::new(Vec::new()),
            write_through: hint == OpenHint::MutableSyncLog,
            blob_sync_fault: rel_name
                .starts_with("blob_")
                .then(|| Arc::clone(&self.blob_sync_fault)),
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
    fn root_path(&self) -> &std::path::Path {
        &self.root
    }
}

fn ring_full_config() -> EngineConfig {
    let mut c = EngineConfig::default();
    c.sync_wal_size_bytes = 16 * 4096;
    c.buf_wal_size_bytes = 16 * 4096;
    c.max_blob_segment_bytes = 2048;
    c.blob_cache_bytes = 0;
    c.block_cache_bytes = 0;
    c
}

/// Opens an engine whose table 1 evicts corrupt rows, flushes one blob row ("victim") into a
/// blob segment, optionally corrupts that blob on disk, and fills the sync WAL to the brim so the
/// next sizeable batch of the given durability hits `WalAppendOutcome::RingFull`.
fn ring_full_setup(
    corrupt: bool,
    durability: WriteDurability,
) -> (Arc<PowerLossFs>, StorageEngine<PowerLossFs>) {
    let fs = Arc::new(PowerLossFs::new());
    let mut e = StorageEngine::open(Arc::clone(&fs), ring_full_config()).unwrap();
    e.set_table_corrupt_policy(1, CorruptPolicy::Evict);
    e.put_row(
        1,
        b"victim",
        &[1u8; 1500],
        &[],
        WriteDurability::SyncTransactional,
    )
    .unwrap();
    e.flush_memtable().unwrap();
    if corrupt {
        let name = fs
            .list_files()
            .unwrap()
            .into_iter()
            .find(|f| f.starts_with("blob_std_"))
            .unwrap();
        let f = fs
            .open_file(&name, false, true, OpenHint::ImmutableSequential)
            .unwrap();
        f.write_all_at(&[0xEE; 32], 64).unwrap();
        f.sync_durable().unwrap();
    }
    fill_wal_to_last_sector(&mut e, durability);
    (fs, e)
}

/// Fills the WAL of the given durability until only its last sector is left.
fn fill_wal_to_last_sector(e: &mut StorageEngine<PowerLossFs>, durability: WriteDurability) {
    let mut i = 0;
    loop {
        let (idx, total) = match durability {
            WriteDurability::SyncTransactional => {
                (e.sync_wal.current_sector_idx(), e.sync_wal.total_sectors())
            }
            WriteDurability::BufferedFast => {
                (e.buf_wal.current_sector_idx(), e.buf_wal.total_sectors())
            }
        };
        if idx + 2 >= total {
            break;
        }
        let k = format!("fill{i:04}");
        e.put_row(2, k.as_bytes(), &[7u8; 1000], &[], durability)
            .unwrap();
        i += 1;
    }
}

fn wal_epoch(e: &StorageEngine<PowerLossFs>, durability: WriteDurability) -> u64 {
    match durability {
        WriteDurability::SyncTransactional => e.sync_wal.active_epoch(),
        WriteDurability::BufferedFast => e.buf_wal.active_epoch(),
    }
}

fn count_tx_rows(e: &mut StorageEngine<PowerLossFs>) -> usize {
    (0..12)
        .filter(|i| {
            e.get_row(3, format!("tx{i:04}").as_bytes())
                .unwrap()
                .is_some()
        })
        .count()
}

/// Commits a 12-row transaction that rotates the full WAL, after the transaction itself read
/// the corrupt row (queueing its eviction). Returns the number of rows that survive a power loss.
fn ring_full_commit_survivors(corrupt: bool, durability: WriteDurability) -> usize {
    let (fs, mut e) = ring_full_setup(corrupt, durability);
    let epoch = wal_epoch(&e, durability);
    e.begin_transaction();
    let seen = e.get_row(1, b"victim").unwrap();
    assert_eq!(seen.is_none(), corrupt);
    assert_eq!(e.has_pending_corrupt_evictions(), corrupt);
    for i in 0..12 {
        let k = format!("tx{i:04}");
        e.put_row(3, k.as_bytes(), &[9u8; 900], &[], durability)
            .unwrap();
    }
    e.commit_transaction().unwrap();
    assert!(
        wal_epoch(&e, durability) > epoch,
        "the commit must have rotated the full WAL"
    );
    // The queued eviction is not lost: it is carried out after the commit.
    if corrupt {
        assert_eq!(e.get_row(1, b"victim").unwrap(), None);
        assert!(!e.has_pending_corrupt_evictions());
    }
    // A buffered commit only becomes durable with the next synchronous commit (prefix durability).
    e.put_row(2, b"marker", b"v", &[], WriteDurability::SyncTransactional)
        .unwrap();
    fs.power_loss();
    std::mem::forget(e);
    let mut e = StorageEngine::open(fs, ring_full_config()).unwrap();
    count_tx_rows(&mut e)
}

#[test]
fn ring_full_sync_commit_survives_power_loss_without_corruption() {
    assert_eq!(
        ring_full_commit_survivors(false, WriteDurability::SyncTransactional),
        12
    );
}

#[test]
fn ring_full_sync_commit_with_queued_eviction_survives_power_loss() {
    assert_eq!(
        ring_full_commit_survivors(true, WriteDurability::SyncTransactional),
        12
    );
}

#[test]
fn ring_full_buffered_commit_with_queued_eviction_survives_power_loss() {
    assert_eq!(
        ring_full_commit_survivors(true, WriteDurability::BufferedFast),
        12
    );
}

#[test]
fn ring_full_non_transaction_write_with_queued_eviction_survives_power_loss() {
    for durability in [
        WriteDurability::SyncTransactional,
        WriteDurability::BufferedFast,
    ] {
        let (fs, mut e) = ring_full_setup(true, durability);
        let epoch = wal_epoch(&e, durability);
        // Reading the corrupt row queues its eviction; the next write drains it first, so the
        // full-WAL rotation then sees an empty queue. The writes must survive either way.
        assert_eq!(e.get_row(1, b"victim").unwrap(), None);
        assert!(e.has_pending_corrupt_evictions());
        for i in 0..12 {
            let k = format!("tx{i:04}");
            e.put_row(3, k.as_bytes(), &[9u8; 900], &[], durability)
                .unwrap();
        }
        assert!(wal_epoch(&e, durability) > epoch);
        assert_eq!(e.get_row(1, b"victim").unwrap(), None);
        assert!(!e.has_pending_corrupt_evictions());
        e.put_row(2, b"marker", b"v", &[], WriteDurability::SyncTransactional)
            .unwrap();
        fs.power_loss();
        std::mem::forget(e);
        let mut e = StorageEngine::open(fs, ring_full_config()).unwrap();
        assert_eq!(count_tx_rows(&mut e), 12, "{durability:?}");
    }
}

/// WAL small enough that every flush is forced by a full ring, with a MemTable that never
/// reaches `should_flush()` on its own.
fn ring_only_flush_config(background: bool) -> EngineConfig {
    let mut c = ring_full_config();
    c.auto_compact_segment_threshold = 4;
    c.background_compaction = background;
    c
}

const RING_ONLY_SEGMENT_BOUND: usize = 8;

fn ring_only_workload(background: bool, in_tx: bool) -> (StorageEngine<PowerLossFs>, usize) {
    let fs = Arc::new(PowerLossFs::new());
    let mut e = StorageEngine::open(fs, ring_only_flush_config(background)).unwrap();
    let hook_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let hc = Arc::clone(&hook_calls);
    e.set_compaction_hook(Some(Arc::new(move || {
        hc.fetch_add(1, Ordering::SeqCst);
    })));
    for i in 0..300 {
        let k = format!("k{i:05}");
        if in_tx {
            e.begin_transaction();
        }
        e.put_row(
            1,
            k.as_bytes(),
            b"v",
            &[],
            WriteDurability::SyncTransactional,
        )
        .unwrap();
        if in_tx {
            e.commit_transaction().unwrap();
        }
        assert!(
            e.active_idx_segment_count() <= RING_ONLY_SEGMENT_BOUND,
            "segments grew without bound at write {i}: {}",
            e.active_idx_segment_count()
        );
    }
    assert!(!e.memtable.should_flush());
    (e, hook_calls.load(Ordering::SeqCst))
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
    let epoch = e.sync_wal.active_epoch();
    e.config.auto_compact_segment_threshold = 2;
    let mut i = 0;
    while e.sync_wal.current_sector_idx() + 2 < e.sync_wal.total_sectors() {
        let k = format!("fill{i:04}");
        e.put_row(2, k.as_bytes(), b"v", &[], sync).unwrap();
        i += 1;
    }
    assert_eq!(e.sync_wal.active_epoch(), epoch);
    // Keep writing until one write has to rotate the ring.
    let mut last = Vec::new();
    while e.sync_wal.active_epoch() == epoch {
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

// ---------------------------------------------------------------------------------------------
// Blob reclaim and persisted dead bytes must ignore transactions that are not yet published.
// ---------------------------------------------------------------------------------------------

const OLD_BLOB: [u8; 1500] = [0x33; 1500];

fn blob_file_count(fs: &PowerLossFs) -> usize {
    fs.list_files()
        .unwrap()
        .into_iter()
        .filter(|f| f.starts_with("blob_"))
        .count()
}

/// Opens an engine whose table 1 holds `r1` and `r2` as 1500-byte blobs in two different, durable
/// blob segments (so the first, sealed segment holds only `r1`). With `fill` the WAL of the given
/// durability is then filled up to its last sector, so the next sizeable batch rotates it. With
/// `capped` table 1 is a capped table, which puts its blobs into `blob_lru` segments.
fn dead_blob_setup(
    durability: WriteDurability,
    capped: bool,
    fill: bool,
) -> (Arc<PowerLossFs>, StorageEngine<PowerLossFs>) {
    let fs = Arc::new(PowerLossFs::new());
    let mut e = StorageEngine::open(Arc::clone(&fs), ring_full_config()).unwrap();
    if capped {
        e.register_capped_table(1, 1_000_000).unwrap();
    }
    let sync = WriteDurability::SyncTransactional;
    e.put_row(1, b"r1", &OLD_BLOB, &[], sync).unwrap();
    e.put_row(1, b"r2", &OLD_BLOB, &[], sync).unwrap();
    e.flush_memtable().unwrap();
    assert_eq!(blob_file_count(&fs), 2);
    if fill {
        fill_wal_to_last_sector(&mut e, durability);
    }
    (fs, e)
}

/// Overwrites `r1` inside a transaction that also writes 12 sizeable rows. Returns whether the
/// commit rotated the WAL.
fn commit_overwrite_of_r1(e: &mut StorageEngine<PowerLossFs>, durability: WriteDurability) -> bool {
    let epoch = wal_epoch(e, durability);
    e.begin_transaction();
    e.put_row(1, b"r1", b"new-inline", &[], durability).unwrap();
    for i in 0..12 {
        let k = format!("tx{i:04}");
        e.put_row(3, k.as_bytes(), &[9u8; 900], &[], durability)
            .unwrap();
    }
    e.commit_transaction().unwrap();
    wal_epoch(e, durability) > epoch
}

fn persisted_dead_bytes(e: &StorageEngine<PowerLossFs>) -> u64 {
    let st = e.meta_mgr.state();
    st.std_blob_segments
        .iter()
        .map(|s| s.dead_bytes)
        .chain(st.lru_blob_segments.iter().map(|s| s.dead_bytes))
        .sum()
}

#[test]
fn ring_full_commit_keeps_blob_segment_of_a_transaction_lost_by_power_loss() {
    let durability = WriteDurability::BufferedFast;
    let (fs, mut e) = dead_blob_setup(durability, false, true);
    assert!(commit_overwrite_of_r1(&mut e, durability));
    assert_eq!(blob_file_count(&fs), 2);
    // The rotation checkpointed the log without the transaction's dead bytes.
    assert_eq!(persisted_dead_bytes(&e), 0);
    fs.power_loss();
    std::mem::forget(e);
    let mut e = StorageEngine::open(fs, ring_full_config()).unwrap();
    assert_eq!(e.get_row(1, b"r1").unwrap(), Some(OLD_BLOB.to_vec()));
    assert_eq!(e.get_row(1, b"r2").unwrap(), Some(OLD_BLOB.to_vec()));
}

/// A capped table reclaims its `blob_lru` segments right after the commit, which first makes the
/// buffered log durable. So the commit survives and the old segment has left the catalog, but its
/// file stays until the next WAL rotation, and the rotation inside the commit must not have
/// emptied the segment of a row that is not yet replaced.
#[test]
fn ring_full_commit_on_capped_table_never_loses_the_row_it_overwrote() {
    let durability = WriteDurability::BufferedFast;
    let (fs, mut e) = dead_blob_setup(durability, true, true);
    assert!(commit_overwrite_of_r1(&mut e, durability));
    assert_eq!(blob_file_count(&fs), 2);
    fs.power_loss();
    std::mem::forget(e);
    let mut e = StorageEngine::open(fs, ring_full_config()).unwrap();
    assert_eq!(e.get_row(1, b"r1").unwrap(), Some(b"new-inline".to_vec()));
    assert_eq!(e.get_row(1, b"r2").unwrap(), Some(OLD_BLOB.to_vec()));
}

#[test]
fn commit_without_full_wal_keeps_blob_segment_of_a_transaction_lost_by_power_loss() {
    // Control: the same workload with room left in the WAL never rotates.
    let durability = WriteDurability::BufferedFast;
    let (fs, mut e) = dead_blob_setup(durability, false, false);
    assert!(!commit_overwrite_of_r1(&mut e, durability));
    assert_eq!(blob_file_count(&fs), 2);
    fs.power_loss();
    std::mem::forget(e);
    let mut e = StorageEngine::open(fs, ring_full_config()).unwrap();
    assert_eq!(e.get_row(1, b"r1").unwrap(), Some(OLD_BLOB.to_vec()));
}

#[test]
fn dead_bytes_of_a_lost_ring_full_transaction_never_reclaim_live_blobs_after_reopen() {
    let durability = WriteDurability::BufferedFast;
    let (fs, mut e) = dead_blob_setup(durability, false, true);
    assert!(commit_overwrite_of_r1(&mut e, durability));
    fs.power_loss();
    std::mem::forget(e);
    let mut e = StorageEngine::open(Arc::clone(&fs), ring_full_config()).unwrap();
    // Later activity: flushes, a checkpoint and compaction would all reclaim a segment whose
    // persisted dead bytes covered the lost transaction.
    for round in 0..3 {
        for i in 0..4 {
            let k = format!("more{round}{i}");
            e.put_row(
                2,
                k.as_bytes(),
                &[5u8; 200],
                &[],
                WriteDurability::SyncTransactional,
            )
            .unwrap();
        }
        e.flush_memtable().unwrap();
    }
    e.checkpoint().unwrap();
    e.compact().unwrap();
    assert_eq!(e.get_row(1, b"r1").unwrap(), Some(OLD_BLOB.to_vec()));
    assert_eq!(blob_file_count(&fs), 2);
    fs.power_loss();
    std::mem::forget(e);
    let mut e = StorageEngine::open(fs, ring_full_config()).unwrap();
    assert_eq!(e.get_row(1, b"r1").unwrap(), Some(OLD_BLOB.to_vec()));
}

#[test]
fn ring_full_commit_still_reclaims_the_overwritten_blob_segment_at_a_later_checkpoint() {
    for capped in [false, true] {
        for durability in [
            WriteDurability::SyncTransactional,
            WriteDurability::BufferedFast,
        ] {
            let (fs, mut e) = dead_blob_setup(durability, capped, true);
            assert!(commit_overwrite_of_r1(&mut e, durability));
            // Not reclaimed by the rotation inside the commit. A capped table drops the segment
            // from the catalog right after the commit, but its file lingers until the WAL is
            // rotated again, because a logged batch may still point into it ...
            assert_eq!(blob_file_count(&fs), 2, "{durability:?} capped={capped}");
            // ... but by the next flush, which persists the dead bytes and unlinks the segment.
            e.flush_memtable().unwrap();
            assert_eq!(blob_file_count(&fs), 1, "{durability:?} capped={capped}");
            assert_eq!(
                e.get_row(1, b"r1").unwrap(),
                Some(b"new-inline".to_vec()),
                "{durability:?} capped={capped}"
            );
            e.put_row(2, b"marker", b"v", &[], WriteDurability::SyncTransactional)
                .unwrap();
            fs.power_loss();
            std::mem::forget(e);
            let mut e = StorageEngine::open(fs, ring_full_config()).unwrap();
            assert_eq!(e.get_row(1, b"r1").unwrap(), Some(b"new-inline".to_vec()));
            assert_eq!(e.get_row(1, b"r2").unwrap(), Some(OLD_BLOB.to_vec()));
        }
    }
}

#[test]
fn rolled_back_transaction_leaves_dead_bytes_and_snapshot_consistent() {
    let (fs, mut e) = dead_blob_setup(WriteDurability::BufferedFast, false, false);
    let dead_before: Vec<u64> = e.std_blobs.values().map(|b| b.dead_bytes).collect();
    e.begin_transaction();
    e.put_row(1, b"r1", b"new-inline", &[], WriteDurability::BufferedFast)
        .unwrap();
    assert!(e.tx_start_meta.active);
    e.rollback_transaction().unwrap();
    assert!(!e.tx_start_meta.active);
    let dead_after: Vec<u64> = e.std_blobs.values().map(|b| b.dead_bytes).collect();
    assert_eq!(dead_before, dead_after);
    e.put_row(2, b"k", b"v", &[], WriteDurability::BufferedFast)
        .unwrap();
    e.flush_memtable().unwrap();
    assert_eq!(blob_file_count(&fs), 2);
    assert_eq!(e.get_row(1, b"r1").unwrap(), Some(OLD_BLOB.to_vec()));
}

/// Non-transaction writes count the dead bytes of the row they replace only after their batch is
/// in the log, so a full WAL rotating for that very write cannot reclaim the old blob.
#[test]
fn ring_full_non_transaction_overwrite_and_delete_keep_the_old_blob_until_logged() {
    const VICTIMS: usize = 400;
    for delete in [false, true] {
        for durability in [
            WriteDurability::SyncTransactional,
            WriteDurability::BufferedFast,
        ] {
            let fs = Arc::new(PowerLossFs::new());
            let mut e = StorageEngine::open(Arc::clone(&fs), ring_full_config()).unwrap();
            for j in 0..VICTIMS {
                let k = format!("v{j:04}");
                e.put_row(
                    1,
                    k.as_bytes(),
                    &OLD_BLOB,
                    &[],
                    WriteDurability::BufferedFast,
                )
                .unwrap();
            }
            e.flush_memtable().unwrap();
            fill_wal_to_last_sector(&mut e, durability);
            let epoch = wal_epoch(&e, durability);
            let mut hit = None;
            for j in 0..VICTIMS {
                let k = format!("v{j:04}");
                if delete {
                    assert!(e.delete_row(1, k.as_bytes(), durability).unwrap());
                } else {
                    e.put_row(1, k.as_bytes(), &[4u8; 300], &[], durability)
                        .unwrap();
                }
                if wal_epoch(&e, durability) > epoch {
                    hit = Some(j);
                    break;
                }
            }
            let hit = hit.unwrap_or_else(|| {
                panic!("no write hit a full WAL: delete={delete} {durability:?}")
            });
            fs.power_loss();
            std::mem::forget(e);
            let mut e = StorageEngine::open(fs, ring_full_config()).unwrap();
            for j in 0..VICTIMS {
                let k = format!("v{j:04}");
                let got = e.get_row(1, k.as_bytes()).unwrap();
                // Writes before the rotation were flushed durably; the one that rotated is only
                // durable when it was synchronous; later ones were never made.
                let changed =
                    j < hit || (j == hit && durability == WriteDurability::SyncTransactional);
                let expected = match (changed, delete) {
                    (false, _) => Some(OLD_BLOB.to_vec()),
                    (true, true) => None,
                    (true, false) => Some(vec![4u8; 300]),
                };
                assert_eq!(got, expected, "delete={delete} {durability:?} row {j}");
            }
        }
    }
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

// ---------------------------------------------------------------------------------------------
// Blob segment sync failures (Linux "fsyncgate"): a failed blob sync leaves the durability of
// the blob bytes unknown, and a retried sync may report success without persisting them. The
// engine must therefore be poisoned, so that no checkpoint, flush or WAL record can ever cover
// a blob that is not durable.
// ---------------------------------------------------------------------------------------------

const FAULT_BLOB_LEN: usize = 1500;

type AckedRows = Vec<(Vec<u8>, Vec<u8>)>;

fn blob_fault_config() -> EngineConfig {
    let mut c = EngineConfig::default();
    c.blob_cache_bytes = 0;
    c.block_cache_bytes = 0;
    c
}

fn blob_fault_value(tag: u8) -> Vec<u8> {
    vec![tag; FAULT_BLOB_LEN]
}

fn blob_fault_setup(
    config: &EngineConfig,
) -> (Arc<PowerLossFs>, StorageEngine<PowerLossFs>, AckedRows) {
    let fs = Arc::new(PowerLossFs::new());
    let mut e = StorageEngine::open(Arc::clone(&fs), config.clone()).unwrap();
    e.register_capped_table(2, 1_000_000).unwrap();
    e.put_row(
        1,
        b"before",
        &blob_fault_value(1),
        &[],
        WriteDurability::SyncTransactional,
    )
    .unwrap();
    let acked = vec![(b"before".to_vec(), blob_fault_value(1))];
    (fs, e, acked)
}

/// Every mutation and every checkpoint-like operation must fail after the poisoning failure.
fn assert_blob_sync_fail_stop(e: &mut StorageEngine<PowerLossFs>) {
    assert!(
        e.is_poisoned(),
        "a blob sync failure must poison the engine"
    );
    assert!(e.checkpoint().is_err(), "checkpoint must not succeed");
    assert!(e.flush_memtable().is_err(), "flush must not succeed");
    assert!(e.compact().is_err());
    for durability in [
        WriteDurability::SyncTransactional,
        WriteDurability::BufferedFast,
    ] {
        assert!(
            e.put_row(1, b"later", &blob_fault_value(9), &[], durability)
                .is_err()
        );
        assert!(e.put_row(1, b"inline", b"v", &[], durability).is_err());
    }
    // Reads keep working, as in the other poison cases.
    assert_eq!(
        e.get_row(1, b"before").unwrap(),
        Some(blob_fault_value(1)),
        "reads of durable rows keep working"
    );
}

/// Simulates a power loss, reopens, and checks that every acknowledged row is readable.
fn assert_acked_rows_survive(
    fs: Arc<PowerLossFs>,
    e: StorageEngine<PowerLossFs>,
    config: &EngineConfig,
    acked: &AckedRows,
) {
    fs.power_loss();
    std::mem::forget(e);
    let mut e = StorageEngine::open(fs, config.clone()).unwrap();
    assert!(!e.is_poisoned());
    for (key, value) in acked {
        let table = if key.starts_with(b"lru") { 2 } else { 1 };
        assert_eq!(
            e.get_row(table, key).unwrap().as_deref(),
            Some(value.as_slice()),
            "acknowledged row {:?} is lost or unreadable after reopen",
            String::from_utf8_lossy(key)
        );
    }
}

#[test]
fn blob_sync_failure_on_sync_commit_poisons_and_never_acknowledges_lost_blob() {
    let config = blob_fault_config();
    let (fs, mut e, mut acked) = blob_fault_setup(&config);
    fs.arm_blob_sync_failure();
    let r = e.put_row(
        1,
        b"failed",
        &blob_fault_value(2),
        &[],
        WriteDurability::SyncTransactional,
    );
    assert!(r.is_err(), "the failed operation must return an error");
    // Retrying is what turns the lost sync into an acknowledged-but-lost row: the second sync of
    // the same file reports success without persisting the bytes.
    let retry = e.put_row(
        1,
        b"failed",
        &blob_fault_value(2),
        &[],
        WriteDurability::SyncTransactional,
    );
    if retry.is_ok() {
        acked.push((b"failed".to_vec(), blob_fault_value(2)));
    }
    assert_blob_sync_fail_stop(&mut e);
    assert_acked_rows_survive(fs, e, &config, &acked);
}

#[test]
fn blob_sync_failure_on_lru_sync_commit_poisons() {
    let config = blob_fault_config();
    let (fs, mut e, mut acked) = blob_fault_setup(&config);
    e.put_row(
        2,
        b"lru-before",
        &blob_fault_value(3),
        &[],
        WriteDurability::SyncTransactional,
    )
    .unwrap();
    acked.push((b"lru-before".to_vec(), blob_fault_value(3)));
    fs.arm_blob_sync_failure();
    let first = e.put_row(
        2,
        b"lru-failed",
        &blob_fault_value(4),
        &[],
        WriteDurability::SyncTransactional,
    );
    assert!(first.is_err());
    let retry = e.put_row(
        2,
        b"lru-failed",
        &blob_fault_value(4),
        &[],
        WriteDurability::SyncTransactional,
    );
    if retry.is_ok() {
        acked.push((b"lru-failed".to_vec(), blob_fault_value(4)));
    }
    assert_blob_sync_fail_stop(&mut e);
    assert_acked_rows_survive(fs, e, &config, &acked);
}

#[test]
fn blob_sync_failure_in_flush_memtable_poisons_and_checkpoint_stays_failed() {
    let config = blob_fault_config();
    let (fs, mut e, acked) = blob_fault_setup(&config);
    e.put_row(
        1,
        b"buffered",
        &blob_fault_value(5),
        &[],
        WriteDurability::BufferedFast,
    )
    .unwrap();
    fs.arm_blob_sync_failure();
    assert!(e.flush_memtable().is_err());
    assert_blob_sync_fail_stop(&mut e);
    assert_acked_rows_survive(fs, e, &config, &acked);
}

#[test]
fn blob_sync_failure_in_checkpoint_with_empty_memtable_poisons() {
    let config = blob_fault_config();
    let (fs, mut e, acked) = blob_fault_setup(&config);
    e.flush_memtable().unwrap();
    // A rolled back transaction leaves unsynced blob bytes behind an empty MemTable.
    e.begin_transaction();
    e.put_row(
        1,
        b"rolled-back",
        &blob_fault_value(6),
        &[],
        WriteDurability::BufferedFast,
    )
    .unwrap();
    e.rollback_transaction().unwrap();
    assert!(e.memtable.is_empty());
    fs.arm_blob_sync_failure();
    assert!(e.checkpoint().is_err());
    assert_blob_sync_fail_stop(&mut e);
    assert_acked_rows_survive(fs, e, &config, &acked);
}

#[test]
fn blob_sync_failure_when_sealing_rolled_segment_poisons() {
    let mut config = blob_fault_config();
    config.max_blob_segment_bytes = 2048;
    let (fs, mut e, acked) = blob_fault_setup(&config);
    // The buffered blob rolls into a fresh segment and stays unsynced there.
    e.put_row(
        1,
        b"unsynced",
        &blob_fault_value(5),
        &[],
        WriteDurability::BufferedFast,
    )
    .unwrap();
    fs.arm_blob_sync_failure();
    // This append rolls the segment again and must seal the unsynced one first.
    let r = e.put_row(
        1,
        b"rolls",
        &blob_fault_value(7),
        &[],
        WriteDurability::BufferedFast,
    );
    assert!(r.is_err());
    assert_blob_sync_fail_stop(&mut e);
    assert_acked_rows_survive(fs, e, &config, &acked);
}

#[test]
fn blob_sync_failure_in_transaction_commit_poisons() {
    let config = blob_fault_config();
    let (fs, mut e, acked) = blob_fault_setup(&config);
    e.begin_transaction();
    e.put_row(
        1,
        b"in-tx",
        &blob_fault_value(8),
        &[],
        WriteDurability::SyncTransactional,
    )
    .unwrap();
    fs.arm_blob_sync_failure();
    assert!(
        e.prepare_commit_transaction(WriteDurability::SyncTransactional)
            .is_err()
    );
    assert!(e.commit_transaction().is_err());
    let _ = e.rollback_transaction();
    assert_blob_sync_fail_stop(&mut e);
    assert_acked_rows_survive(fs, e, &config, &acked);
}

#[test]
fn blob_sync_control_without_fault_keeps_every_row() {
    let config = blob_fault_config();
    let (fs, mut e, mut acked) = blob_fault_setup(&config);
    for (i, durability) in [
        WriteDurability::SyncTransactional,
        WriteDurability::BufferedFast,
    ]
    .into_iter()
    .enumerate()
    {
        let key = format!("ok{i}").into_bytes();
        e.put_row(1, &key, &blob_fault_value(10 + i as u8), &[], durability)
            .unwrap();
        e.flush_memtable().unwrap();
        e.checkpoint().unwrap();
        acked.push((key, blob_fault_value(10 + i as u8)));
    }
    assert!(!e.is_poisoned());
    assert_acked_rows_survive(fs, e, &config, &acked);
}

#[test]
fn blob_write_failure_before_sync_is_retryable_and_does_not_poison() {
    let config = blob_fault_config();

    // Through a synchronous commit.
    let (fs, mut e, mut acked) = blob_fault_setup(&config);
    fs.set_blob_write_failure(true);
    let r = e.put_row(
        1,
        b"full",
        &blob_fault_value(2),
        &[],
        WriteDurability::SyncTransactional,
    );
    assert!(r.is_err(), "the failed write must be reported");
    assert!(!e.is_poisoned(), "a write that never landed is retryable");
    fs.set_blob_write_failure(false);
    e.put_row(
        1,
        b"full",
        &blob_fault_value(2),
        &[],
        WriteDurability::SyncTransactional,
    )
    .unwrap();
    acked.push((b"full".to_vec(), blob_fault_value(2)));

    // Through `sync_active_blob_writers`: inside a transaction the blob stays staged in the
    // writer buffer until it is flushed.
    e.begin_transaction();
    e.put_row(
        1,
        b"staged",
        &blob_fault_value(3),
        &[],
        WriteDurability::SyncTransactional,
    )
    .unwrap();
    fs.set_blob_write_failure(true);
    assert!(e.sync_active_blob_writers().is_err());
    assert!(
        e.prepare_commit_transaction(WriteDurability::SyncTransactional)
            .is_err()
    );
    assert!(!e.is_poisoned());
    fs.set_blob_write_failure(false);
    e.prepare_commit_transaction(WriteDurability::SyncTransactional)
        .unwrap();
    e.commit_transaction().unwrap();
    acked.push((b"staged".to_vec(), blob_fault_value(3)));
    e.checkpoint().unwrap();

    assert_acked_rows_survive(fs, e, &config, &acked);
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

/// Walks the table with `scan_table_batch` the way the UNIQUE check does.
fn walk_table_in_batches(engine: &mut StorageEngine<OsFs>, limit: usize) -> Vec<Vec<u8>> {
    let mut seen = Vec::new();
    let mut cursor: Option<Vec<u8>> = None;
    loop {
        let (rows, next) = engine
            .scan_table_batch(1, cursor.as_deref().map(|pk| (pk, false)), None, limit)
            .unwrap();
        seen.extend(rows.into_iter().map(|(pk, _)| pk));
        match next {
            Some(pk) => cursor = Some(pk),
            None => break,
        }
    }
    seen
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

// ---------------------------------------------------------------------------------------------
// backfill_secondary_keys
// ---------------------------------------------------------------------------------------------

fn live_keys(engine: &StorageEngine<OsFs>, table_id: u16, ns: u8) -> Vec<Vec<u8>> {
    engine
        .scan_merged_prefix(&encode_namespace_prefix(table_id, ns))
        .unwrap()
        .into_iter()
        .filter(|e| !matches!(e.value, IndexValue::Tombstone))
        .map(|e| e.key)
        .collect()
}

fn row_meta(engine: &mut StorageEngine<OsFs>, table_id: u16, pk: &[u8]) -> Option<RowMetaRecord> {
    engine
        .get_row_meta_with_ptr(&encode_row_meta_key(table_id, pk))
        .unwrap()
        .map(|(m, _)| m)
}

fn backfill_engine(name: &str) -> (TempTestDir, Arc<OsFs>, EngineConfig, StorageEngine<OsFs>) {
    let tmp = TempTestDir::new(name);
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let cfg = EngineConfig::default();
    let engine = StorageEngine::open(fs.clone(), cfg.clone()).unwrap();
    (tmp, fs, cfg, engine)
}

const UNIQ: u16 = 0x8001;

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

// Test-only readers used by the virtual table tests, which cannot reach the engine's private scans.
impl<FS: PlatformFs> StorageEngine<FS> {
    /// Live keys of namespace `ns` of `table_id`, in key order, as the owner sees them.
    pub(crate) fn test_live_keys(&self, table_id: u16, ns: u8) -> io::Result<Vec<Vec<u8>>> {
        Ok(self
            .scan_merged_prefix(&encode_namespace_prefix(table_id, ns))?
            .into_iter()
            .filter(|e| !matches!(e.value, IndexValue::Tombstone))
            .map(|e| e.key)
            .collect())
    }

    /// The `'t'` record of a row, if it has a readable one.
    pub(crate) fn test_row_meta(
        &mut self,
        table_id: u16,
        pk: &[u8],
    ) -> io::Result<Option<RowMetaRecord>> {
        Ok(self
            .get_row_meta_with_ptr(&encode_row_meta_key(table_id, pk))?
            .map(|(m, _)| m))
    }
}

impl<FS: PlatformFs> StorageEngine<FS> {
    /// Live secondary index entries of `table_id`, without the UNIQUE index marker.
    pub(crate) fn test_live_entry_keys(&self, table_id: u16) -> io::Result<Vec<Vec<u8>>> {
        let marker = crate::index::encode_secondary_index_prefix(
            table_id,
            unique_index::UNIQUE_MARKER_INDEX_ID,
        );
        Ok(self
            .test_live_keys(table_id, NS_SECONDARY_INDEX)?
            .into_iter()
            .filter(|k| k.as_slice() != marker)
            .collect())
    }
}

impl<FS: PlatformFs> StorageEngine<FS> {
    /// Writes an empty-valued key straight into the log, for tests that fabricate entries which
    /// no row owns (as a half-finished operation of an older version could have left).
    pub(crate) fn test_put_raw_entry(&mut self, key: Vec<u8>) -> io::Result<()> {
        self.commit_wal_and_memtable(
            vec![WalRecord::PutInline {
                key,
                value: Vec::new(),
            }],
            WriteDurability::BufferedFast,
        )?;
        Ok(())
    }
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
fn crash_recovery_persists_reconciled_capped_bytes_across_second_crash() {
    let tmp = TempTestDir::new("capped_double_crash");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut cfg = EngineConfig::default();
    cfg.inline_threshold_bytes = 4096;
    let wal = WriteDurability::BufferedFast;
    let payload = vec![0x42u8; 1000];

    let mut engine = StorageEngine::open(fs.clone(), cfg.clone()).unwrap();
    engine.register_capped_table(1, 100_000).unwrap();
    engine.put_row(1, b"k1", &payload, &[], wal).unwrap();
    engine.checkpoint().unwrap();
    let flushed_bytes = engine.capped_table_state(1).unwrap().current_logical_bytes;
    engine.put_row(1, b"k2", &payload, &[], wal).unwrap();
    engine.put_row(1, b"k3", &payload, &[], wal).unwrap();
    let expected = engine.capped_table_state(1).unwrap().current_logical_bytes;
    assert!(expected > flushed_bytes);
    drop(engine); // Crash #1: k2 and k3 exist only in the WAL

    // Recovery #1 replays WAL, reconciles `current_logical_bytes`, and persists it during open.
    let engine = StorageEngine::open(fs.clone(), cfg.clone()).unwrap();
    assert_eq!(
        engine.capped_table_state(1).unwrap().current_logical_bytes,
        expected
    );
    drop(engine); // Crash #2 immediately after recovery without calling `checkpoint()`

    // Recovery #2 sees an empty WAL (`replayed_wal == false`) and reads persisted `table_quotas`.
    let mut engine = StorageEngine::open(fs, cfg).unwrap();
    assert_eq!(
        engine.capped_table_state(1).unwrap().current_logical_bytes,
        expected
    );
    for k in [b"k1", b"k2", b"k3"] {
        assert_eq!(engine.get_row(1, k).unwrap(), Some(payload.clone()));
    }
}

#[test]
fn register_capped_table_reconcile_failure_does_not_leave_stale_zero_state() {
    use crate::lru::RowMetaRecord;
    use crate::wal::{BlobKind, WalRecord};

    let fs = Arc::new(PowerLossFs::new());
    let mut cfg = EngineConfig::default();
    cfg.inline_threshold_bytes = 64;
    let mut engine = StorageEngine::open(Arc::clone(&fs), cfg).unwrap();

    // Seed table 9 with an existing row and an un-ticked (`recorded_tick = 0`) RowMetaRecord
    // stored in the buffered standard blob writer so initial reconciliation must flush & read it.
    engine
        .put_row(9, b"pk1", b"val1", &[], WriteDurability::BufferedFast)
        .unwrap();
    let meta_key = crate::lru::encode_row_meta_key(9, b"pk1");
    let big_meta = RowMetaRecord::encode_slice(0, 7, &[(1, vec![b'x'; 4096])]).unwrap();
    let meta_ptr = engine
        .append_to_blob_segment(BlobKind::Standard, 0, &meta_key, &big_meta)
        .unwrap();
    engine
        .commit_wal_and_memtable(
            vec![WalRecord::PutBlobPtr {
                key: meta_key,
                blob_ptr: meta_ptr,
            }],
            WriteDurability::BufferedFast,
        )
        .unwrap();

    // Fail blob I/O during initial `register_capped_table` reconciliation.
    fs.set_blob_write_failure(true);
    let err = engine.register_capped_table(9, 10_000).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::StorageFull);
    assert!(
        engine.capped_table_state(9).is_none(),
        "failed register_capped_table must roll back empty capped_tables entry"
    );

    // Clear the transient I/O failure and retry; reconciliation must run and count `pk1`.
    fs.set_blob_write_failure(false);
    engine.register_capped_table(9, 10_000).unwrap();
    let ct = engine.capped_table_state(9).unwrap();
    assert_eq!(ct.current_logical_bytes, 7);
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
fn multiple_capped_tables_simultaneous_lru_blob_rollover_preserves_durability_and_quotas() {
    let tmp = TempTestDir::new("multi_capped_rollover");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut cfg = EngineConfig::default();
    cfg.inline_threshold_bytes = 64;
    cfg.max_blob_segment_bytes = 4 * 1024;
    cfg.sync_wal_size_bytes = 64 * 4096;
    cfg.buf_wal_size_bytes = 64 * 4096;
    cfg.memtable_capacity_bytes = 16 * 1024;

    let tables = [(10u16, 8 * 1024u64), (11, 12 * 1024), (12, 6 * 1024)];
    let mut all_keys: HashMap<u16, Vec<Vec<u8>>> = HashMap::new();
    let mut expected: HashMap<(u16, Vec<u8>), Vec<u8>> = HashMap::new();

    {
        let mut engine = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
        for &(tid, max_bytes) in &tables {
            engine.register_capped_table(tid, max_bytes).unwrap();
        }

        for round in 0..60usize {
            for &(tid, _) in &tables {
                let pk = format!("k_{tid}_{round:03}").into_bytes();
                let len = match tid {
                    10 => 300 + (round % 31),
                    11 => 360 + (round % 31),
                    _ => 420 + (round % 31),
                };
                let fill = (tid as u8).wrapping_mul(17).wrapping_add(round as u8);
                let mut val = vec![fill; len];
                let tag = format!("t{tid}_r{round:03}_");
                val[..tag.len()].copy_from_slice(tag.as_bytes());
                let durability = if (round + tid as usize).is_multiple_of(2) {
                    WriteDurability::BufferedFast
                } else {
                    WriteDurability::SyncTransactional
                };
                engine.put_row(tid, &pk, &val, &[], durability).unwrap();
                all_keys.entry(tid).or_default().push(pk.clone());
                expected.insert((tid, pk), val);
            }
            if round % 15 == 14 {
                engine.flush_memtable().unwrap();
            }
            if round == 30 {
                engine.compact().unwrap();
            }
            if round == 45 {
                engine.checkpoint().unwrap();
            }
        }

        let mut survivors_before: HashMap<(u16, Vec<u8>), Vec<u8>> = HashMap::new();
        for &(tid, max_bytes) in &tables {
            let ct = engine.capped_table_state(tid).unwrap();
            assert!(
                ct.current_logical_bytes <= max_bytes,
                "table {tid} exceeded quota: {} > {max_bytes}",
                ct.current_logical_bytes
            );
            let mut surviving_count = 0usize;
            for pk in &all_keys[&tid] {
                if let Some(got) = engine.get_row(tid, pk).unwrap() {
                    let exp = expected.get(&(tid, pk.clone())).unwrap();
                    assert_eq!(&got, exp, "corrupted payload for table {tid}");
                    survivors_before.insert((tid, pk.clone()), got);
                    surviving_count += 1;
                }
            }
            assert!(
                (1..60).contains(&surviving_count),
                "expected partial eviction on table {tid}, got {surviving_count}/60"
            );
        }
        drop(engine);

        let mut reopened = StorageEngine::open(Arc::clone(&fs), cfg).unwrap();
        for &(tid, max_bytes) in &tables {
            reopened.register_capped_table(tid, max_bytes).unwrap();
            let ct = reopened.capped_table_state(tid).unwrap();
            assert!(
                ct.current_logical_bytes <= max_bytes,
                "reopened table {tid} exceeded quota: {} > {max_bytes}",
                ct.current_logical_bytes
            );
        }

        let mut survivors_after: HashMap<(u16, Vec<u8>), Vec<u8>> = HashMap::new();
        for &(tid, _) in &tables {
            for pk in &all_keys[&tid] {
                if let Some(got) = reopened.get_row(tid, pk).unwrap() {
                    survivors_after.insert((tid, pk.clone()), got);
                }
            }
        }
        assert_eq!(survivors_after, survivors_before);
    }
}

#[test]
fn reopen_skips_blob_fsync_when_replayed_pointers_are_within_cataloged_len() {
    use std::path::Path;

    struct CountingSyncFile {
        rel_name: String,
        inner: Arc<<OsFs as PlatformFs>::File>,
        sync_counts: Arc<std::sync::Mutex<HashMap<String, usize>>>,
    }

    impl PlatformFile for CountingSyncFile {
        fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
            self.inner.read_at(buf, offset)
        }
        fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
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
        fn sync_durable(&self) -> io::Result<()> {
            *self
                .sync_counts
                .lock()
                .unwrap()
                .entry(self.rel_name.clone())
                .or_insert(0) += 1;
            self.inner.sync_durable()
        }
        fn sync_barrier(&self) -> io::Result<()> {
            self.inner.sync_barrier()
        }
        fn evict_os_cache(&self, offset: u64, len: u64) -> io::Result<()> {
            self.inner.evict_os_cache(offset, len)
        }
    }

    struct CountingSyncFs {
        inner: OsFs,
        sync_counts: Arc<std::sync::Mutex<HashMap<String, usize>>>,
    }

    impl PlatformFs for CountingSyncFs {
        type File = CountingSyncFile;

        fn open_file(
            &self,
            rel_name: &str,
            create: bool,
            writable: bool,
            hint: OpenHint,
        ) -> io::Result<Arc<Self::File>> {
            Ok(Arc::new(CountingSyncFile {
                rel_name: rel_name.to_string(),
                inner: self.inner.open_file(rel_name, create, writable, hint)?,
                sync_counts: Arc::clone(&self.sync_counts),
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

    let tmp = TempTestDir::new("skip_cataloged_blob_sync");
    let sync_counts = Arc::new(std::sync::Mutex::new(HashMap::new()));
    let fs = Arc::new(CountingSyncFs {
        inner: OsFs::open(&tmp.path, true).unwrap(),
        sync_counts: Arc::clone(&sync_counts),
    });
    let mut cfg = EngineConfig::default();
    cfg.inline_threshold_bytes = 64;
    cfg.max_blob_segment_bytes = 300;
    cfg.sync_wal_size_bytes = 64 * 4096;
    cfg.buf_wal_size_bytes = 64 * 4096;

    let (seg1, seg2) = {
        let mut engine = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
        engine
            .put_row(1, b"k1", &[0xAAu8; 200], &[], WriteDurability::BufferedFast)
            .unwrap();
        // Rolling over the active blob segment syncs `seg1` and catalogs its `file_len`
        // in `boot_meta`, while opening `seg2` with cataloged `file_len == 0`. Both rows'
        // `PutBlobPtr` records remain in `wal_buf.db` for replay on reopen.
        engine
            .put_row(1, b"k2", &[0xBBu8; 200], &[], WriteDurability::BufferedFast)
            .unwrap();
        engine.flush_active_blob_buffers().unwrap();
        let seg1 = match engine
            .memtable
            .get(&crate::lru::encode_primary_row_key(1, b"k1"))
            .unwrap()
            .value
        {
            IndexValue::Blob(p) => p.seg_id,
            ref other => panic!("expected Blob pointer for k1, got {other:?}"),
        };
        let seg2 = match engine
            .memtable
            .get(&crate::lru::encode_primary_row_key(1, b"k2"))
            .unwrap()
            .value
        {
            IndexValue::Blob(p) => p.seg_id,
            ref other => panic!("expected Blob pointer for k2, got {other:?}"),
        };
        assert_ne!(seg1, seg2);
        // Simulate a crash after recovery cataloged `seg1`'s already-synced length in `boot_meta`
        // (while `seg2` still has cataloged `file_len == 0`) with both WAL records still subject
        // to replay on reopen.
        let seg1_len = engine.std_blobs[&seg1].file_len;
        engine
            .meta_mgr
            .commit_update(|st| {
                if let Some(m) = st.std_blob_segments.iter_mut().find(|m| m.seq_id == seg1) {
                    m.file_len = seg1_len;
                }
            })
            .unwrap();
        (seg1, seg2)
    };

    sync_counts.lock().unwrap().clear();
    let mut reopened = StorageEngine::open(Arc::clone(&fs), cfg).unwrap();
    assert_eq!(
        reopened.get_row(1, b"k1").unwrap().unwrap(),
        vec![0xAAu8; 200]
    );
    assert_eq!(
        reopened.get_row(1, b"k2").unwrap().unwrap(),
        vec![0xBBu8; 200]
    );
    let counts = sync_counts.lock().unwrap();
    let seg1_syncs = counts
        .get(&format_std_blob_file_name(seg1))
        .copied()
        .unwrap_or(0);
    let seg2_syncs = counts
        .get(&format_std_blob_file_name(seg2))
        .copied()
        .unwrap_or(0);
    assert_eq!(
        seg1_syncs, 0,
        "already-cataloged sealed blob within bmeta.file_len should not be fsynced at open: {counts:?}"
    );
    assert!(
        seg2_syncs >= 1,
        "replayed blob extending past cataloged file_len must still be fsynced at open: {counts:?}"
    );
}

#[test]
fn drop_table_inside_rolled_back_tx_preserves_lru_writer_policies_and_holders() {
    let tmp = TempTestDir::new("drop_table_rollback_state");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut cfg = EngineConfig::default();
    cfg.inline_threshold_bytes = 64;
    let mut engine = StorageEngine::open(fs, cfg).unwrap();

    let plan = engine.plan_vtab_registration("t", None).unwrap();
    let (tid, token) = engine
        .register_vtab_plan(plan, WriteDurability::BufferedFast, None)
        .unwrap();
    engine.register_capped_table(tid, 1 << 20).unwrap();
    engine.set_table_corrupt_policy(tid, CorruptPolicy::Error);
    engine.set_table_inline_threshold(tid, 96);
    engine
        .put_row(
            tid,
            b"k1",
            &[0x5Au8; 200],
            &[],
            WriteDurability::BufferedFast,
        )
        .unwrap();

    assert!(engine.active_lru_writers.contains_key(&tid));
    assert_eq!(engine.corrupt_policy(tid), CorruptPolicy::Error);
    assert_eq!(engine.effective_inline_threshold(tid), 96);
    assert_eq!(
        engine
            .active_vtab_holders
            .get(&tid)
            .map(Vec::as_slice)
            .unwrap_or(&[]),
        &[token]
    );

    engine.begin_transaction();
    engine.drop_table(tid, Some("t")).unwrap();
    engine.rollback_transaction().unwrap();

    assert!(
        engine.active_lru_writers.contains_key(&tid),
        "active_lru_writers entry must survive rollback of drop_table"
    );
    assert_eq!(engine.corrupt_policy(tid), CorruptPolicy::Error);
    assert_eq!(engine.effective_inline_threshold(tid), 96);
    assert_eq!(
        engine
            .active_vtab_holders
            .get(&tid)
            .map(Vec::as_slice)
            .unwrap_or(&[]),
        &[token]
    );
    assert_eq!(
        engine.active_vtab_ids.get(&tid).map(|s| s.as_ref()),
        Some("t")
    );
    assert!(engine.capped_tables.contains_key(&tid));
    assert!(
        engine
            .lru_blobs
            .iter()
            .filter(|(k, _)| k.0 == tid)
            .all(|(_, b)| b.dead_bytes < b.file_len),
        "lru_blobs dead_bytes must not remain full segment length after rollback"
    );
    assert_eq!(engine.get_row(tid, b"k1").unwrap(), Some(vec![0x5Au8; 200]));
}

#[test]
fn drop_table_clears_access_tracker_on_commit_but_not_on_rollback() {
    let tmp = TempTestDir::new("drop_table_access_tracker");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut engine = StorageEngine::open(fs, EngineConfig::default()).unwrap();

    engine.register_capped_table(1, 1024 * 1024).unwrap();
    engine
        .put_row(1, b"k1", b"v1", &[], WriteDurability::BufferedFast)
        .unwrap();
    assert_eq!(engine.get_row(1, b"k1").unwrap(), Some(b"v1".to_vec()));
    assert!(engine.access_tracker.peek_hot_tick(1, b"k1").is_some());

    engine.begin_transaction();
    engine.drop_table(1, None).unwrap();
    engine.rollback_transaction().unwrap();
    assert!(
        engine.access_tracker.peek_hot_tick(1, b"k1").is_some(),
        "rolled back drop_table must keep AccessTracker entries intact"
    );

    engine.drop_table(1, None).unwrap();
    assert!(engine.access_tracker.peek_hot_tick(1, b"k1").is_none());
    assert!(engine.access_tracker.is_empty());
}

#[test]
fn relocate_fragmented_std_blobs_preserves_data_generation_and_does_not_recurse() {
    let tmp = TempTestDir::new("reloc_gen_no_recurse");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut cfg = EngineConfig::default();
    cfg.memtable_capacity_bytes = 1024;
    cfg.inline_threshold_bytes = 64;
    cfg.max_blob_segment_bytes = 4096;
    cfg.auto_compact_segment_threshold = 8;
    let mut engine = StorageEngine::open(Arc::clone(&fs), cfg).unwrap();

    let payload = vec![0x7Eu8; 96];
    for i in 0..40u32 {
        let pk = format!("rk_{i:04}").into_bytes();
        engine
            .put_row(1, &pk, &payload, &[], WriteDurability::BufferedFast)
            .unwrap();
    }
    engine.compact().unwrap();

    let mut initial_std_files: Vec<String> = fs
        .list_files()
        .unwrap()
        .into_iter()
        .filter(|f| f.starts_with("blob_std_"))
        .collect();
    initial_std_files.sort();
    assert_eq!(
        initial_std_files.len(),
        2,
        "expected 2 blob_std files: {initial_std_files:?}"
    );
    let fragmented_file = initial_std_files[0].clone();

    // Fragment sealed segment 1 (>= 50% dead) while keeping enough live blobs to force a
    // mid-relocation MemTable flush under `memtable_capacity_bytes = 1024`.
    for i in 0..20u32 {
        let pk = format!("rk_{i:04}").into_bytes();
        engine
            .delete_row(1, &pk, WriteDurability::BufferedFast)
            .unwrap();
    }
    engine.flush_memtable().unwrap();
    assert!(engine.has_fragmented_std_blobs(0.5));

    let snap = engine.capture_read_snapshot();
    let relocated = engine.relocate_fragmented_std_blobs(0.5).unwrap();
    assert!(
        relocated >= 10,
        "expected >= 10 relocated blobs, got {relocated}"
    );
    assert!(
        engine.snapshot_is_current(&snap),
        "physical blob relocation must not invalidate user-data ReadSnapshot"
    );
    drop(snap);

    engine.flush_memtable().unwrap();
    assert!(!engine.has_fragmented_std_blobs(0.5));
    let std_files: Vec<String> = fs
        .list_files()
        .unwrap()
        .into_iter()
        .filter(|f| f.starts_with("blob_std_"))
        .collect();
    assert!(
        !std_files.contains(&fragmented_file),
        "expected fragmented {fragmented_file} reclaimed, remaining: {std_files:?}"
    );

    for i in 20..40u32 {
        let pk = format!("rk_{i:04}").into_bytes();
        assert_eq!(engine.get_row(1, &pk).unwrap(), Some(payload.clone()));
    }
}

type SyncCountMap = Arc<std::sync::Mutex<HashMap<String, usize>>>;

struct CountedFile {
    rel_name: String,
    inner: Arc<<OsFs as PlatformFs>::File>,
    sync_counts: SyncCountMap,
    barrier_counts: SyncCountMap,
}

impl PlatformFile for CountedFile {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        self.inner.read_at(buf, offset)
    }
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
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
    fn sync_durable(&self) -> io::Result<()> {
        *self
            .sync_counts
            .lock()
            .unwrap()
            .entry(self.rel_name.clone())
            .or_insert(0) += 1;
        self.inner.sync_durable()
    }
    fn sync_barrier(&self) -> io::Result<()> {
        *self
            .barrier_counts
            .lock()
            .unwrap()
            .entry(self.rel_name.clone())
            .or_insert(0) += 1;
        self.inner.sync_barrier()
    }
    fn evict_os_cache(&self, offset: u64, len: u64) -> io::Result<()> {
        self.inner.evict_os_cache(offset, len)
    }
}

struct CountedFs {
    inner: OsFs,
    sync_counts: SyncCountMap,
    barrier_counts: SyncCountMap,
}

impl CountedFs {
    fn open_counted(dir: &std::path::Path) -> (Arc<Self>, SyncCountMap, SyncCountMap) {
        let sync_counts = Arc::new(std::sync::Mutex::new(HashMap::new()));
        let barrier_counts = Arc::new(std::sync::Mutex::new(HashMap::new()));
        let fs = Arc::new(Self {
            inner: OsFs::open(dir, true).unwrap(),
            sync_counts: Arc::clone(&sync_counts),
            barrier_counts: Arc::clone(&barrier_counts),
        });
        (fs, sync_counts, barrier_counts)
    }
}

impl PlatformFs for CountedFs {
    type File = CountedFile;

    fn open_file(
        &self,
        rel_name: &str,
        create: bool,
        writable: bool,
        hint: crate::pal::OpenHint,
    ) -> io::Result<Arc<Self::File>> {
        Ok(Arc::new(CountedFile {
            rel_name: rel_name.to_string(),
            inner: self.inner.open_file(rel_name, create, writable, hint)?,
            sync_counts: Arc::clone(&self.sync_counts),
            barrier_counts: Arc::clone(&self.barrier_counts),
        }))
    }
    fn remove_file(&self, rel_name: &str) -> io::Result<()> {
        self.inner.remove_file(rel_name)
    }
    fn list_files(&self) -> io::Result<Vec<String>> {
        self.inner.list_files()
    }
    fn root_path(&self) -> &std::path::Path {
        self.inner.root_path()
    }
}

#[test]
fn prepare_compaction_and_wal_rotation_avoid_redundant_fsyncs() {
    let tmp = TempTestDir::new("pm1_avoid_redundant_fsyncs");
    let (fs, sync_counts, _) = CountedFs::open_counted(&tmp.path);

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
fn evict_until_within_quota_batches_window_evictions_and_promotions_into_single_wal_sync() {
    let tmp = TempTestDir::new("ph4_batched_eviction");
    let (fs, _, barrier_counts) = CountedFs::open_counted(&tmp.path);

    let mut cfg = EngineConfig::default();
    cfg.inline_threshold_bytes = 256;
    cfg.max_blob_segment_bytes = 16 * 1024;
    cfg.memtable_capacity_bytes = 4 * 1024 * 1024;

    {
        let mut engine = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
        engine.register_capped_table(7, 20_000).unwrap();

        let payload = vec![0x5Au8; 1_000];
        for i in 0..18u32 {
            let pk = format!("k_{i:02}");
            let tag = format!("tag_{i:02}");
            engine
                .put_row(
                    7,
                    pk.as_bytes(),
                    &payload,
                    &[(1, tag.into_bytes())],
                    WriteDurability::SyncTransactional,
                )
                .unwrap();
        }

        // Warm the 3 oldest keys in `AccessTracker` so eviction triggers 3 lazy promotions.
        for i in 0..3u32 {
            let pk = format!("k_{i:02}");
            assert!(engine.get_row(7, pk.as_bytes()).unwrap().is_some());
        }

        barrier_counts.lock().unwrap().clear();

        // Inserting one 12 KB row pushes total logical bytes (~30 KB) well above 20 KB, forcing
        // 3 lazy promotions + 11 cascading evictions inside a single 64-entry scan window.
        let big_payload = vec![0x7Bu8; 12_000];
        engine
            .put_row(
                7,
                b"k_big",
                &big_payload,
                &[(1, b"tag_big".to_vec())],
                WriteDurability::SyncTransactional,
            )
            .unwrap();

        let wal_sync_barriers = barrier_counts
            .lock()
            .unwrap()
            .get(crate::wal::WAL_SYNC_FILE_NAME)
            .copied()
            .unwrap_or(0);
        assert_eq!(
            wal_sync_barriers, 2,
            "put_row + entire batched eviction window must issue only 2 wal_sync barriers total"
        );

        let ct = engine.capped_table_state(7).unwrap();
        assert_eq!(ct.total_lazy_promotions, 3);
        assert_eq!(ct.total_evictions, 11);
        assert!(!ct.is_over_quota());

        for i in 0..3u32 {
            let pk = format!("k_{i:02}");
            let tag = format!("tag_{i:02}");
            assert!(
                engine.get_row(7, pk.as_bytes()).unwrap().is_some(),
                "lazily promoted row {pk} must survive eviction"
            );
            let idx_hits = engine
                .lookup_by_secondary_index(7, 1, tag.as_bytes())
                .unwrap();
            assert_eq!(idx_hits.len(), 1);
            assert_eq!(idx_hits[0], pk.as_bytes());
        }
        for i in 3..14u32 {
            let pk = format!("k_{i:02}");
            let tag = format!("tag_{i:02}");
            assert_eq!(engine.get_row(7, pk.as_bytes()).unwrap(), None);
            assert!(
                engine
                    .lookup_by_secondary_index(7, 1, tag.as_bytes())
                    .unwrap()
                    .is_empty(),
                "secondary index entry for evicted row {pk} must be removed"
            );
        }
        for i in 14..18u32 {
            let pk = format!("k_{i:02}");
            assert!(engine.get_row(7, pk.as_bytes()).unwrap().is_some());
        }
        assert_eq!(
            engine.get_row(7, b"k_big").unwrap().as_deref(),
            Some(big_payload.as_slice())
        );
    }

    // Reopen and verify WAL replay + reconciliation reproduce the exact live keys and quota state.
    let mut reopened = StorageEngine::open(fs, cfg).unwrap();
    let live_keys = reopened.scan_keys_range(7, None, None, 100).unwrap();
    assert_eq!(live_keys.len(), 8);
    let ct = reopened.capped_table_state(7).unwrap();
    assert!(!ct.is_over_quota());
}

#[test]
fn multi_chunk_sync_transaction_uses_single_wal_barrier_and_recovers_cleanly() {
    let tmp = TempTestDir::new("ph5_multi_chunk_single_barrier");
    let (fs, _, barrier_counts) = CountedFs::open_counted(&tmp.path);

    let mut cfg = EngineConfig::default();
    cfg.inline_threshold_bytes = 1024 * 1024;
    cfg.max_transaction_bytes = 16 * 1024 * 1024;
    cfg.memtable_capacity_bytes = 16 * 1024 * 1024;

    // 1. Multi-chunk transaction (~1.65 MB of inline rows -> 4 chunks) must issue only ONE `wal_sync` barrier.
    {
        let mut engine = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
        engine.begin_transaction();
        for i in 0..550u32 {
            let pk = format!("chunk_{i:03}");
            let val = vec![(i as u8).wrapping_add(1); 3_000];
            engine
                .put_row(
                    1,
                    pk.as_bytes(),
                    &val,
                    &[],
                    WriteDurability::SyncTransactional,
                )
                .unwrap();
        }
        engine
            .prepare_commit_transaction(WriteDurability::SyncTransactional)
            .unwrap();
        let lsn_before_flush = engine.next_lsn;
        barrier_counts.lock().unwrap().clear();
        engine.commit_transaction().unwrap();

        let wal_sync_barriers = barrier_counts
            .lock()
            .unwrap()
            .get(crate::wal::WAL_SYNC_FILE_NAME)
            .copied()
            .unwrap_or(0);
        assert_eq!(
            wal_sync_barriers, 1,
            "multi-chunk SyncTransactional commit must issue only one wal_sync barrier on the final chunk"
        );
        assert!(
            engine.next_lsn - lsn_before_flush >= 3,
            "multi-chunk commit must emit multiple chunk batches (TxBegin + middle + TxCommit), got {}",
            engine.next_lsn - lsn_before_flush
        );
        assert!(engine.idx_segments.is_empty());
    }

    // Reopen and verify every row from all chunks recovered cleanly from `wal_sync.db`.
    {
        let mut reopened = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
        for i in 0..550u32 {
            let pk = format!("chunk_{i:03}");
            let expected = vec![(i as u8).wrapping_add(1); 3_000];
            assert_eq!(
                reopened.get_row(1, pk.as_bytes()).unwrap(),
                Some(expected),
                "reopen must replay multi-chunk row {pk}"
            );
        }

        // 2. Exact 2-chunk boundary (~525 KB > 512 KB): only `TxBegin` + `TxCommit`, still 1 barrier.
        reopened.begin_transaction();
        for i in 0..176u32 {
            let pk = format!("two_{i:03}");
            let val = vec![(i as u8).wrapping_add(11); 3_000];
            reopened
                .put_row(
                    1,
                    pk.as_bytes(),
                    &val,
                    &[],
                    WriteDurability::SyncTransactional,
                )
                .unwrap();
        }
        reopened
            .prepare_commit_transaction(WriteDurability::SyncTransactional)
            .unwrap();
        let lsn_before_flush = reopened.next_lsn;
        barrier_counts.lock().unwrap().clear();
        reopened.commit_transaction().unwrap();

        let wal_sync_barriers = barrier_counts
            .lock()
            .unwrap()
            .get(crate::wal::WAL_SYNC_FILE_NAME)
            .copied()
            .unwrap_or(0);
        assert_eq!(wal_sync_barriers, 1);
        assert_eq!(reopened.next_lsn - lsn_before_flush, 2);
    }

    let mut reopened2 = StorageEngine::open(fs, cfg).unwrap();
    for i in 0..176u32 {
        let pk = format!("two_{i:03}");
        let expected = vec![(i as u8).wrapping_add(11); 3_000];
        assert_eq!(reopened2.get_row(1, pk.as_bytes()).unwrap(), Some(expected));
    }
}

#[test]
fn pending_group_visible_to_next_writer_without_cloning_into_overlay() {
    let tmp = TempTestDir::new("ph6_pending_group_layer");
    let mut engine = open_group_engine(&tmp);

    // tx1 writes k1=a, k2=b into pending_group.
    engine.begin_transaction_for_conn(1).unwrap();
    engine.put_row(1, b"k1", b"a", &[], GROUP_SYNC).unwrap();
    engine.put_row(1, b"k2", b"b", &[], GROUP_SYNC).unwrap();
    engine.prepare_commit_transaction(GROUP_SYNC).unwrap();
    let CommitStep::AwaitingGroup {
        lsn: lsn1,
        file: file1,
        ..
    } = engine.begin_commit_for_conn(1).unwrap()
    else {
        panic!("expected group commit for tx1");
    };

    // tx2 overwrites k2=c, adds k3=d, and deletes k1 into pending_group.
    engine.begin_transaction_for_conn(2).unwrap();
    engine.put_row(1, b"k2", b"c", &[], GROUP_SYNC).unwrap();
    engine.put_row(1, b"k3", b"d", &[], GROUP_SYNC).unwrap();
    engine.delete_row(1, b"k1", GROUP_SYNC).unwrap();
    engine.prepare_commit_transaction(GROUP_SYNC).unwrap();
    let CommitStep::AwaitingGroup { lsn: lsn2, .. } = engine.begin_commit_for_conn(2).unwrap()
    else {
        panic!("expected group commit for tx2");
    };

    // Autocommit / non-owner readers must see nothing until durable flush.
    assert_eq!(row_visible_to(&engine, 9, b"k2"), None);

    // tx3 begins: overlay must stay empty (zero clones from pending_group).
    engine.begin_transaction_for_conn(3).unwrap();
    assert!(engine.tx_overlay.map.is_empty());
    assert_eq!(engine.get_row(1, b"k1").unwrap(), None);
    assert_eq!(engine.get_row(1, b"k2").unwrap(), Some(b"c".to_vec()));
    assert_eq!(engine.get_row(1, b"k3").unwrap(), Some(b"d".to_vec()));

    let fwd = engine.scan_table_range(1, None, None, 100).unwrap();
    assert_eq!(
        fwd,
        vec![
            (b"k2".to_vec(), b"c".to_vec()),
            (b"k3".to_vec(), b"d".to_vec()),
        ]
    );
    let mut pks = Vec::new();
    let mut slab = Vec::new();
    let mut spans = Vec::new();
    let mut lazy = Vec::new();
    engine
        .scan_range_into_slab(
            1,
            None,
            None,
            true,
            100,
            false,
            &mut pks,
            &mut slab,
            &mut spans,
            Some(&mut lazy),
        )
        .unwrap();
    assert_eq!(pks, vec![b"k3".to_vec(), b"k2".to_vec()]);
    assert_eq!(
        spans.iter().map(|&(s, e)| &slab[s..e]).collect::<Vec<_>>(),
        vec![b"d", b"c"]
    );

    // Inside a savepoint in tx3, overwrite k2 then roll back: pending_group k2=c still wins.
    engine.savepoint_for_conn(3, 1).unwrap();
    engine.put_row(1, b"k2", b"e", &[], GROUP_SYNC).unwrap();
    assert_eq!(engine.get_row(1, b"k2").unwrap(), Some(b"e".to_vec()));
    engine.rollback_to_savepoint_for_conn(3, 1).unwrap();
    assert_eq!(engine.get_row(1, b"k2").unwrap(), Some(b"c".to_vec()));
    engine.rollback_transaction_for_conn(3).unwrap();

    // Flush tx1 only while tx2 remains in pending_group, then flush tx2.
    engine
        .finish_group_flush(lsn1, file1.sync_barrier())
        .unwrap();
    assert_eq!(row_visible_to(&engine, 9, b"k2"), Some(b"b".to_vec()));
    engine.begin_transaction_for_conn(4).unwrap();
    assert_eq!(engine.get_row(1, b"k1").unwrap(), None);
    assert_eq!(engine.get_row(1, b"k2").unwrap(), Some(b"c".to_vec()));
    engine.rollback_transaction_for_conn(4).unwrap();

    engine.finish_group_flush(lsn2, Ok(())).unwrap();
    assert_eq!(row_visible_to(&engine, 9, b"k1"), None);
    assert_eq!(row_visible_to(&engine, 9, b"k2"), Some(b"c".to_vec()));
    assert_eq!(row_visible_to(&engine, 9, b"k3"), Some(b"d".to_vec()));
}

#[test]
fn tx_overlay_rollback_preserves_filter_and_bounds_without_false_negatives() {
    let mut overlay = tx::TxOverlay::default();
    let rec = |v: &[u8]| crate::wal::WalRecord::PutInline {
        key: v.to_vec(),
        value: v.to_vec(),
    };
    overlay.apply(1, rec(b"b"), true);
    overlay.apply(2, rec(b"d"), true);
    let outer_undo = overlay.undo.len();

    overlay.apply(3, rec(b"a"), true);
    overlay.apply(4, rec(b"c"), true);
    overlay.apply(5, rec(b"z"), true);
    assert_eq!(overlay.min_key.as_deref(), Some(b"a".as_slice()));
    assert_eq!(overlay.max_key.as_deref(), Some(b"z".as_slice()));

    // Roll back inner mutations: surviving keys keep no false negatives and shrink min/max bounds.
    overlay.rollback_to(outer_undo);
    assert!(overlay.may_contain_key(b"b"));
    assert!(overlay.may_contain_key(b"d"));
    assert_eq!(overlay.min_key.as_deref(), Some(b"b".as_slice()));
    assert_eq!(overlay.max_key.as_deref(), Some(b"d".as_slice()));

    // Emptying the overlay clears min/max bounds and resets the filter.
    overlay.rollback_to(0);
    assert!(overlay.map.is_empty());
    assert_eq!(overlay.min_key, None);
    assert_eq!(overlay.max_key, None);
    assert_eq!(overlay.filter, [0; 16]);
}

#[test]
fn statement_savepoint_rollback_restores_blobs_capped_bytes_and_row_meta_without_full_snapshot() {
    let tmp = TempTestDir::new("ph6_stmt_savepoint");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut cfg = EngineConfig::default();
    cfg.inline_threshold_bytes = 64;
    let mut engine = StorageEngine::open(fs, cfg).unwrap();

    engine.register_capped_table(7, 100_000).unwrap();
    let payload1 = vec![0x11u8; 256];
    engine
        .put_row(
            7,
            b"k1",
            &payload1,
            &[(1, b"tag1".to_vec())],
            WriteDurability::BufferedFast,
        )
        .unwrap();

    engine.begin_transaction_for_conn(1).unwrap();
    let initial_dead: u64 = engine
        .lru_blobs
        .iter()
        .filter(|&(&(tid, _), _)| tid == 7)
        .map(|(_, b)| b.dead_bytes)
        .sum();
    let initial_len: u64 = engine
        .lru_blobs
        .iter()
        .filter(|&(&(tid, _), _)| tid == 7)
        .map(|(_, b)| b.file_len)
        .sum();
    let initial_logical_bytes = engine.capped_table_state(7).unwrap().current_logical_bytes;
    let initial_row_meta = engine.table_has_row_meta.clone();

    engine.statement_savepoint_for_conn(1, 7).unwrap();
    assert!(engine.statement_savepoint_active);
    assert!(matches!(
        engine.savepoints.last().unwrap().meta,
        tx::SavepointMeta::Statement(_)
    ));

    // Delete k1 (marks k1 blob dead) and insert k2 (appends new blob).
    assert!(
        engine
            .delete_row(7, b"k1", WriteDurability::BufferedFast)
            .unwrap()
    );
    let payload2 = vec![0x22u8; 256];
    engine
        .put_row(
            7,
            b"k2",
            &payload2,
            &[(1, b"tag2".to_vec())],
            WriteDurability::BufferedFast,
        )
        .unwrap();
    let after_len: u64 = engine
        .lru_blobs
        .iter()
        .filter(|&(&(tid, _), _)| tid == 7)
        .map(|(_, b)| b.file_len)
        .sum();
    let aborted_len = after_len - initial_len;
    assert!(aborted_len > 0);

    engine
        .rollback_to_savepoint_for_conn(1, STATEMENT_SAVEPOINT)
        .unwrap();
    engine
        .release_savepoint_for_conn(1, STATEMENT_SAVEPOINT)
        .unwrap();

    let final_dead: u64 = engine
        .lru_blobs
        .iter()
        .filter(|&(&(tid, _), _)| tid == 7)
        .map(|(_, b)| b.dead_bytes)
        .sum();
    assert_eq!(
        final_dead,
        initial_dead + aborted_len,
        "rollback must unmark deleted k1 blob and mark only aborted k2 blob bytes as dead"
    );
    assert_eq!(
        engine.capped_table_state(7).unwrap().current_logical_bytes,
        initial_logical_bytes
    );
    assert_eq!(engine.table_has_row_meta, initial_row_meta);
    assert_eq!(engine.get_row(7, b"k1").unwrap(), Some(payload1));
    assert_eq!(engine.get_row(7, b"k2").unwrap(), None);
    assert!(!engine.statement_savepoint_active);
    assert!(engine.statement_blob_undo.is_empty());

    engine.rollback_transaction_for_conn(1).unwrap();
}

#[test]
fn table_id_allocation_returns_error_when_all_ids_exhausted() {
    assert_eq!(find_free_table_id(|_| false).unwrap(), 1);
    assert_eq!(find_free_table_id(|id| id < u16::MAX).unwrap(), u16::MAX);
    let err = find_free_table_id(|_| true).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::Other);

    let tmp = TempTestDir::new("table_id_exhaustion");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut engine = StorageEngine::open(fs, EngineConfig::default()).unwrap();
    for id in 1..u16::MAX {
        engine
            .active_vtab_ids
            .insert(id, Arc::from(format!("t_{id}")));
    }
    let tail_plan = engine.plan_vtab_registration("tail", None).unwrap();
    assert_eq!(tail_plan.table_id, u16::MAX);
    assert!(!tail_plan.persisted);

    engine.active_vtab_ids.insert(u16::MAX, Arc::from("tail"));
    let Err(err) = engine.plan_vtab_registration("overflow", None) else {
        panic!("expected error when all 65535 table_ids are exhausted");
    };
    assert_eq!(err.kind(), io::ErrorKind::Other);
}

#[test]
fn open_inner_wal_replay_skips_idx_reads_for_non_blob_namespaces() {
    use crate::pal::PreallocStrategy;
    use std::path::Path;
    use std::sync::atomic::AtomicUsize;

    struct IdxBlockSpyFile {
        inner: Arc<<OsFs as PlatformFs>::File>,
        is_idx: bool,
        block_reads: Arc<AtomicUsize>,
    }

    impl PlatformFile for IdxBlockSpyFile {
        fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
            self.inner.read_at(buf, offset)
        }
        fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
            if self.is_idx && buf.len() == crate::index::IDX_BLOCK_SIZE {
                self.block_reads.fetch_add(1, Ordering::Relaxed);
            }
            self.inner.read_exact_at(buf, offset)
        }
        fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
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

    struct IdxBlockSpyFs {
        inner: OsFs,
        block_reads: Arc<AtomicUsize>,
    }

    impl PlatformFs for IdxBlockSpyFs {
        type File = IdxBlockSpyFile;

        fn open_file(
            &self,
            rel_name: &str,
            create: bool,
            writable: bool,
            hint: OpenHint,
        ) -> io::Result<Arc<Self::File>> {
            Ok(Arc::new(IdxBlockSpyFile {
                inner: self.inner.open_file(rel_name, create, writable, hint)?,
                is_idx: rel_name.starts_with("idx_"),
                block_reads: Arc::clone(&self.block_reads),
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

    let tmp = TempTestDir::new("wal_replay_skip_idx");
    let reads = Arc::new(AtomicUsize::new(0));
    let mut cfg = EngineConfig::default();
    cfg.bloom_bits_per_key = 0; // Disable bloom so any idx.get() in range reads a 4 KB block.
    cfg.inline_threshold_bytes = 256;

    {
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
        let mut engine = StorageEngine::open(fs, cfg.clone()).unwrap();
        // Flush an idx segment containing a standard blob and secondary index keys bracketing idx_id 1..10.
        engine
            .put_row(
                1,
                b"pk_blob",
                &vec![0xAB; 2048],
                &[(1, b"s1".to_vec()), (10, b"s9".to_vec())],
                WriteDurability::SyncTransactional,
            )
            .unwrap();
        engine.flush_memtable().unwrap();

        // Append non-blob-bearing namespace WAL records strictly inside [idx_id 1 .. idx_id 10].
        let wal_recs = vec![
            WalRecord::PutInline {
                key: crate::lru::encode_secondary_index_key(1, 5, b"mid", b"pk_x"),
                value: Vec::new(),
            },
            WalRecord::Delete {
                key: crate::lru::encode_secondary_index_key(1, 6, b"mid2", b"pk_y"),
            },
            WalRecord::PutInline {
                key: crate::lru::encode_lru_queue_key(1, 50, b"pk_x"),
                value: Vec::new(),
            },
        ];
        engine
            .commit_wal_and_memtable(wal_recs, WriteDurability::SyncTransactional)
            .unwrap();
        // Drop without flushing MemTable so next open replays the WAL records above.
    }

    let spy_fs = Arc::new(IdxBlockSpyFs {
        inner: OsFs::open(&tmp.path, true).unwrap(),
        block_reads: Arc::clone(&reads),
    });
    reads.store(0, Ordering::Relaxed);
    let mut reopened = StorageEngine::open(spy_fs, cfg).unwrap();
    assert_eq!(
        reads.load(Ordering::Relaxed),
        0,
        "replaying non-'r'/'t' WAL records must not read 4 KB idx blocks"
    );
    assert_eq!(
        reopened.get_row(1, b"pk_blob").unwrap().unwrap().len(),
        2048
    );
    assert!(
        reopened
            .lookup_raw_entry(&crate::lru::encode_secondary_index_key(
                1, 5, b"mid", b"pk_x"
            ))
            .unwrap()
            .is_some()
    );
}

#[test]
fn prefetch_lazy_blobs_coalesces_reads_and_isolates_bad_pointer() {
    use crate::pal::PreallocStrategy;
    use std::path::Path;
    use std::sync::atomic::AtomicUsize;

    struct BlobReadSpyFile {
        inner: Arc<<OsFs as PlatformFs>::File>,
        is_blob: bool,
        reads: Arc<AtomicUsize>,
    }

    impl PlatformFile for BlobReadSpyFile {
        fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
            self.inner.read_at(buf, offset)
        }
        fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
            if self.is_blob {
                self.reads.fetch_add(1, Ordering::Relaxed);
            }
            self.inner.read_exact_at(buf, offset)
        }
        fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
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

    struct BlobReadSpyFs {
        inner: OsFs,
        reads: Arc<AtomicUsize>,
    }

    impl PlatformFs for BlobReadSpyFs {
        type File = BlobReadSpyFile;

        fn open_file(
            &self,
            rel_name: &str,
            create: bool,
            writable: bool,
            hint: OpenHint,
        ) -> io::Result<Arc<Self::File>> {
            Ok(Arc::new(BlobReadSpyFile {
                inner: self.inner.open_file(rel_name, create, writable, hint)?,
                is_blob: rel_name.starts_with("blob_std_"),
                reads: Arc::clone(&self.reads),
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

    let tmp = TempTestDir::new("prefetch_coalesce_isolate");
    let reads = Arc::new(AtomicUsize::new(0));
    let fs = Arc::new(BlobReadSpyFs {
        inner: OsFs::open(&tmp.path, true).unwrap(),
        reads: Arc::clone(&reads),
    });

    let mut cfg = EngineConfig::default();
    cfg.inline_threshold_bytes = 256;
    cfg.blob_cache_bytes = 4 * 1024 * 1024;
    let mut engine = StorageEngine::open(fs, cfg).unwrap();

    let mut expected_vals = Vec::new();
    for i in 0..6u8 {
        let pk = format!("pk_{i}").into_bytes();
        let val = vec![0x30 + i; 2048];
        engine
            .put_row(1, &pk, &val, &[], WriteDurability::BufferedFast)
            .unwrap();
        expected_vals.push((pk, val));
    }

    let mut pks = Vec::new();
    let mut slab = Vec::new();
    let mut spans = Vec::new();
    let mut lazy = Vec::new();
    engine
        .scan_range_into_slab(
            1,
            None,
            None,
            false,
            16,
            false,
            &mut pks,
            &mut slab,
            &mut spans,
            Some(&mut lazy),
        )
        .unwrap();
    assert_eq!(pks.len(), 6);
    assert!(lazy.iter().all(Option::is_some));

    let seg_id = lazy[0].as_ref().unwrap().ptr.seg_id;
    let cands: Vec<(&[u8], &LazyBlob<_>)> = pks
        .iter()
        .zip(lazy.iter())
        .map(|(pk, lb)| (pk.as_slice(), lb.as_ref().unwrap()))
        .collect();

    // 1. Coalesces all 6 adjacent 2 KB blobs into at most 2 contiguous reads, and caches them.
    reads.store(0, Ordering::Relaxed);
    engine.prefetch_lazy_blobs(1, &cands).unwrap();
    let coalesced_reads = reads.load(Ordering::Relaxed);
    assert!(
        (1..=2).contains(&coalesced_reads),
        "expected <= 2 coalesced reads for 6 blobs, got {coalesced_reads}"
    );

    reads.store(0, Ordering::Relaxed);
    for (idx, (pk, val)) in expected_vals.iter().enumerate() {
        let mut out = Vec::new();
        let (s, e) = engine
            .dereference_lazy_blob_into(1, pk, lazy[idx].as_ref().unwrap(), &mut out)
            .unwrap()
            .unwrap();
        assert_eq!(&out[s..e], val.as_slice());
    }
    assert_eq!(
        reads.load(Ordering::Relaxed),
        0,
        "cached blobs must not issue extra disk reads"
    );

    // 2. Clear blob cache and add an out-of-bounds pointer inside the <= 16 KB gap so the
    //    coalesced cluster read fails; fallback must still verify and cache all 6 valid siblings.
    engine
        .blob_cache
        .invalidate_blob_segment(crate::wal::BlobKind::Standard as u8, 0, seg_id);
    let last_ptr = lazy.last().unwrap().as_ref().unwrap().ptr;
    let file_len = engine.std_blobs[&seg_id].file_len;
    let bad_lazy = LazyBlob {
        ptr: crate::wal::BlobPtr {
            offset: (file_len + 1024) as u32,
            len: 2048,
            crc32c: 0x1234_5678,
            ..last_ptr
        },
        reader: lazy.last().unwrap().as_ref().unwrap().reader.clone(),
    };
    let bad_pk = b"pk_bad".to_vec();
    let mut mixed_cands = cands;
    mixed_cands.push((bad_pk.as_slice(), &bad_lazy));

    engine.prefetch_lazy_blobs(1, &mixed_cands).unwrap();

    reads.store(0, Ordering::Relaxed);
    for (idx, (pk, val)) in expected_vals.iter().enumerate() {
        let mut out = Vec::new();
        let (s, e) = engine
            .dereference_lazy_blob_into(1, pk, lazy[idx].as_ref().unwrap(), &mut out)
            .unwrap()
            .unwrap();
        assert_eq!(&out[s..e], val.as_slice());
    }
    assert_eq!(
        reads.load(Ordering::Relaxed),
        0,
        "valid sibling blobs must still be cached despite bad peer in the same cluster"
    );
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
                &prefix,
                (&below_prefix_seek, true),
                None,
                16,
                true,
                &mut pks_only,
                &mut slab_only,
                &mut spans_only,
                None,
            )
            .unwrap();

        let mut pks_rows = Vec::new();
        let mut slab_rows = Vec::new();
        let mut spans_rows = Vec::new();
        engine
            .scan_secondary_index_into_slab(
                2,
                1,
                &prefix,
                (&below_prefix_seek, false),
                None,
                16,
                false,
                &mut pks_rows,
                &mut slab_rows,
                &mut spans_rows,
                None,
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
