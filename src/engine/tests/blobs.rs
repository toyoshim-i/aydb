use super::*;

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
        let active_slot = engine.meta_mgr().active_slot();
        engine
            .meta_mgr()
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
                .meta_mgr()
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
        let undo_len_before = engine.tx_state.active_tx().unwrap().overlay.undo.len();

        // Connection 2 reads `shared_k` via `with_committed_read_for_conn(202, ...)`
        let conn2_val = engine
            .with_committed_read_for_conn(202, |eng| eng.get_row(7, b"shared_k"))
            .unwrap();
        assert_eq!(conn2_val, Some(b"committed_v1".to_vec()));
        assert_eq!(
            engine.tx_state.active_tx().unwrap().overlay.undo.len(),
            undo_len_before,
            "concurrent committed read under EagerWriteOnRead must not append to Connection 1's overlay undo log"
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
fn open_time_catalog_commit_does_not_persist_wal_replay_dead_bytes() {
    use crate::pal::test_fs::{FsHooks, HookedFs};
    use std::sync::atomic::AtomicI64;

    // `mode`: 0 = normal, 1 = silently drop boot_meta writes (lost-write crash simulation),
    // 2 = allow `budget` more boot_meta writes, then fail them.
    struct Ctl {
        mode: AtomicI64,
        budget: AtomicI64,
    }
    struct FaultHooks {
        ctl: Arc<Ctl>,
    }
    impl FsHooks for FaultHooks {
        fn write_all_at(
            &self,
            name: &str,
            inner: &dyn PlatformFile,
            buf: &[u8],
            offset: u64,
        ) -> io::Result<()> {
            if name == "boot_meta.db" {
                match self.ctl.mode.load(Ordering::SeqCst) {
                    1 => return Ok(()),
                    2 if self.ctl.budget.fetch_sub(1, Ordering::SeqCst) <= 0 => {
                        return Err(io::Error::other("simulated crash before checkpoint"));
                    }
                    _ => {}
                }
            }
            inner.write_all_at(buf, offset)
        }
    }

    let tmp = TempTestDir::new("open_commit_dead_bytes");
    let ctl = Arc::new(Ctl {
        mode: AtomicI64::new(0),
        budget: AtomicI64::new(0),
    });
    let fs = Arc::new(HookedFs::new(
        OsFs::open(&tmp.path, true).unwrap(),
        FaultHooks {
            ctl: Arc::clone(&ctl),
        },
    ));
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
        let segs = &engine.meta_mgr().state().std_blob_segments;
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
            .meta_mgr()
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
    let initial_blob_len = engine.storage.active_std_len().unwrap();
    engine
        .put_row(1, b"key2", &val_2048, &[], WriteDurability::BufferedFast)
        .unwrap();
    // Inlined into MemTable! No bytes appended to blob segment!
    assert_eq!(engine.storage.active_std_len().unwrap(), initial_blob_len);
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
    assert!(engine.active_lru_seq_id(2).is_some());

    // Explicit override on capped table is honored:
    engine.set_table_inline_threshold(2, 2048);
    assert_eq!(engine.effective_inline_threshold(2), 2048);

    // 3. Table 3: Low override spills small rows
    engine.set_table_inline_threshold(3, 256);
    assert_eq!(engine.effective_inline_threshold(3), 256);
    let val_500 = vec![0x77u8; 500];
    let prev_blob_len = engine.storage.active_std_len().unwrap();
    engine
        .put_row(3, b"k3", &val_500, &[], WriteDurability::BufferedFast)
        .unwrap();
    assert!(engine.storage.active_std_len().unwrap() > prev_blob_len);

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
fn transient_blob_read_error_during_recovery_fails_open_without_dropping_commit() {
    use crate::pal::test_fs::{FsHooks, HookedFs};
    use std::sync::atomic::AtomicBool;

    struct BlobReadFaultHooks {
        fail_blob_reads: Arc<AtomicBool>,
    }

    impl FsHooks for BlobReadFaultHooks {
        fn read_exact_at(
            &self,
            name: &str,
            inner: &dyn PlatformFile,
            buf: &mut [u8],
            offset: u64,
        ) -> io::Result<()> {
            if name.starts_with("blob_") && self.fail_blob_reads.load(Ordering::SeqCst) {
                return Err(io::Error::other("simulated transient read error"));
            }
            inner.read_exact_at(buf, offset)
        }
    }

    let tmp = TempTestDir::new("recovery_transient_blob_error");
    let fail_flag = Arc::new(AtomicBool::new(false));
    let make_fs = || {
        Arc::new(HookedFs::new(
            OsFs::open(&tmp.path, true).unwrap(),
            BlobReadFaultHooks {
                fail_blob_reads: Arc::clone(&fail_flag),
            },
        ))
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
fn inline_sync_commit_does_not_fsync_idle_blob_segments() {
    use blob_sync_counting::{Ctl, open_fs};

    let tmp = TempTestDir::new("idle_blob_sync");
    let ctl = Arc::new(Ctl::default());
    let fs = open_fs(&tmp.path, &ctl);
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
    use blob_sync_counting::{Ctl, open_fs};

    let tmp = TempTestDir::new("blob_sync_poison");
    let ctl = Arc::new(Ctl::default());
    let fs = open_fs(&tmp.path, &ctl);
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
fn reopen_skips_blob_fsync_when_replayed_pointers_are_within_cataloged_len() {
    use crate::pal::test_fs::{FsHooks, HookedFs};

    struct CountingSyncHooks {
        sync_counts: Arc<std::sync::Mutex<HashMap<String, usize>>>,
    }

    impl FsHooks for CountingSyncHooks {
        fn sync_durable(&self, name: &str, inner: &dyn PlatformFile) -> io::Result<()> {
            *self
                .sync_counts
                .lock()
                .unwrap()
                .entry(name.to_owned())
                .or_insert(0) += 1;
            inner.sync_durable()
        }
    }

    let tmp = TempTestDir::new("skip_cataloged_blob_sync");
    let sync_counts = Arc::new(std::sync::Mutex::new(HashMap::new()));
    let fs = Arc::new(HookedFs::new(
        OsFs::open(&tmp.path, true).unwrap(),
        CountingSyncHooks {
            sync_counts: Arc::clone(&sync_counts),
        },
    ));
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
            .healthy()
            .unwrap()
            .commit_meta_update(|st| {
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

#[test]
fn chunked_std_blob_relocation_steps_interleave_and_reclaim_segments() {
    let tmp = TempTestDir::new("chunked_std_reloc");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut cfg = EngineConfig::default();
    cfg.memtable_capacity_bytes = 1024;
    cfg.inline_threshold_bytes = 64;
    cfg.max_blob_segment_bytes = 4096;
    cfg.auto_compact_segment_threshold = 8;
    let mut engine = StorageEngine::open(Arc::clone(&fs), cfg).unwrap();

    let payload = vec![0x6Du8; 96];
    for i in 0..40u32 {
        let pk = format!("cr_{i:04}").into_bytes();
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
    assert!(initial_std_files.len() >= 2);
    let fragmented_file = initial_std_files[0].clone();

    for i in 0..20u32 {
        let pk = format!("cr_{i:04}").into_bytes();
        engine
            .delete_row(1, &pk, WriteDurability::BufferedFast)
            .unwrap();
    }
    engine.flush_memtable().unwrap();
    assert!(engine.has_fragmented_std_blobs(0.5));

    let mut session = engine
        .start_std_blob_relocation(0.5)
        .unwrap()
        .expect("expected fragmented relocation session");

    let first_step = engine
        .step_std_blob_relocation(&mut session, 8, 200)
        .unwrap();
    assert_eq!(first_step, crate::engine::StdBlobRelocStep::Continue);
    assert!(
        !engine.is_relocating_std_blobs(),
        "relocating_std_blobs guard must clear between steps"
    );

    // An active foreground transaction must defer stepping without poisoning or leaking the guard.
    engine.begin_transaction();
    engine
        .put_row(
            1,
            b"cr_fg",
            &[0x99u8; 128],
            &[],
            WriteDurability::BufferedFast,
        )
        .unwrap();
    assert_eq!(
        engine
            .step_std_blob_relocation(&mut session, 8, 200)
            .unwrap(),
        crate::engine::StdBlobRelocStep::Deferred
    );
    assert!(!engine.is_relocating_std_blobs());
    engine.commit_transaction().unwrap();

    let mut steps = 1usize;
    let relocated = loop {
        steps += 1;
        match engine
            .step_std_blob_relocation(&mut session, 8, 200)
            .unwrap()
        {
            crate::engine::StdBlobRelocStep::Continue => {
                assert!(!engine.is_relocating_std_blobs());
            }
            crate::engine::StdBlobRelocStep::Finished { relocated } => break relocated,
            crate::engine::StdBlobRelocStep::Deferred => {
                panic!("unexpected deferral outside transaction");
            }
        }
    };
    assert!(steps >= 2, "expected multi-chunk stepping, got {steps}");
    assert!(
        relocated >= 10,
        "expected >= 10 relocated blobs, got {relocated}"
    );
    engine.finish_std_blob_relocation(relocated).unwrap();
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
        let pk = format!("cr_{i:04}").into_bytes();
        assert_eq!(engine.get_row(1, &pk).unwrap(), Some(payload.clone()));
    }
    assert_eq!(
        engine.get_row(1, b"cr_fg").unwrap(),
        Some(vec![0x99u8; 128])
    );
}

#[test]
fn prefetch_lazy_blobs_coalesces_reads_and_isolates_bad_pointer() {
    use crate::pal::test_fs::{FsHooks, HookedFs};
    use std::sync::atomic::AtomicUsize;

    struct BlobReadSpyHooks {
        reads: Arc<AtomicUsize>,
    }

    impl FsHooks for BlobReadSpyHooks {
        fn read_exact_at(
            &self,
            name: &str,
            inner: &dyn PlatformFile,
            buf: &mut [u8],
            offset: u64,
        ) -> io::Result<()> {
            if name.starts_with("blob_std_") {
                self.reads.fetch_add(1, Ordering::Relaxed);
            }
            inner.read_exact_at(buf, offset)
        }
    }

    let tmp = TempTestDir::new("prefetch_coalesce_isolate");
    let reads = Arc::new(AtomicUsize::new(0));
    let fs = Arc::new(HookedFs::new(
        OsFs::open(&tmp.path, true).unwrap(),
        BlobReadSpyHooks {
            reads: Arc::clone(&reads),
        },
    ));

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
            RangeScanOptions {
                lower: None,
                upper: None,
                reverse: false,
                limit: 16,
                keys_only: false,
            },
            SlabScanOutput {
                pks: &mut pks,
                slab: &mut slab,
                spans: &mut spans,
                lazy_blobs: Some(&mut lazy),
            },
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
fn autocommit_ring_full_power_loss_reclaims_uncataloged_std_and_lru_blobs() {
    for is_capped in [false, true] {
        let fs = Arc::new(PowerLossFs::new());
        let mut cfg = EngineConfig::default();
        cfg.inline_threshold_bytes = 64;
        cfg.buf_wal_size_bytes = 3 * 4096;
        cfg.sync_wal_size_bytes = 4 * 4096;
        let fast = WriteDurability::BufferedFast;
        let sync = WriteDurability::SyncTransactional;

        let mut engine = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
        let tid = if is_capped {
            engine.register_capped_table(7, 1 << 20).unwrap();
            7
        } else {
            1
        };

        // Create and flush a baseline segment holding `b"k1"`.
        engine
            .put_row(tid, b"k1", &[0x11u8; 300], &[], fast)
            .unwrap();
        engine.flush_memtable().unwrap();
        let (seg_id, len_after_k1) = if is_capped {
            let m = &engine.meta_mgr().state().lru_blob_segments[0];
            (m.seq_id, m.file_len)
        } else {
            let m = &engine.meta_mgr().state().std_blob_segments[0];
            (m.seq_id, m.file_len)
        };
        assert!(len_after_k1 > 0);

        // Fill sectors 0..=1 of the 3-sector buffered WAL with inline rows so we sit inside sector
        // 2 (final sector) without appending to any blob segment.
        let mut i = 0usize;
        while engine.buf_wal().current_sector_idx() < 2 {
            let k = format!("pad_{i:04}");
            engine
                .put_row(tid, k.as_bytes(), &[0x33u8; 32], &[], fast)
                .unwrap();
            i += 1;
        }
        let epoch_before = engine.buf_wal().active_epoch();

        // Append an out-of-line blob whose WAL record (~9 KB) cannot fit in the partially used
        // final sector (<4 KB remaining) yet fits in the fresh 3-sector WAL after `RingFull`
        // rotation (unsynced).
        let big_pk = vec![b'z'; 2000];
        let sec = (1u16, vec![b'y'; 1500]);
        engine
            .put_row(tid, &big_pk, &[0x22u8; 300], &[sec], fast)
            .unwrap();
        assert_eq!(engine.buf_wal().active_epoch(), epoch_before + 1);
        assert!(engine.in_flight_autocommit_blob_starts.is_empty());

        let cataloged_at_ring_full = if is_capped {
            engine
                .meta_mgr()
                .state()
                .lru_blob_segments
                .iter()
                .find(|m| m.table_id == tid && m.seq_id == seg_id)
                .unwrap()
                .file_len
        } else {
            engine
                .meta_mgr()
                .state()
                .std_blob_segments
                .iter()
                .find(|m| m.seq_id == seg_id)
                .unwrap()
                .file_len
        };
        assert_eq!(
            cataloged_at_ring_full, len_after_k1,
            "RingFull rotation checkpoint must not catalog uncommitted blob bytes"
        );
        let blob2_tail = if is_capped {
            engine.lru_blobs.get(&(tid, seg_id)).unwrap().file_len - len_after_k1
        } else {
            engine.std_blobs.get(&seg_id).unwrap().file_len - len_after_k1
        };
        assert!(blob2_tail > 0);

        // Power loss drops the retried buffered WAL batch while keeping the synced blob file and
        // the `boot_meta.db` checkpoint written by the `RingFull` rotation.
        drop(engine);
        fs.power_loss();

        let mut reopened = StorageEngine::open(Arc::clone(&fs), cfg).unwrap();
        assert_eq!(reopened.get_row(tid, &big_pk).unwrap(), None);
        assert_eq!(
            reopened.get_row(tid, b"k1").unwrap(),
            Some(vec![0x11u8; 300])
        );

        if is_capped {
            let b = reopened.lru_blobs.get(&(tid, seg_id)).unwrap();
            assert_eq!(b.dead_bytes, blob2_tail);
        } else {
            let b = reopened.std_blobs.get(&seg_id).unwrap();
            assert_eq!(b.dead_bytes, blob2_tail);
        }

        // Deleting `k1` must now make the entire segment dead and reclaim it cleanly.
        reopened.delete_row(tid, b"k1", sync).unwrap();
        reopened.flush_memtable().unwrap();
        if is_capped {
            assert!(!reopened.lru_blobs.contains_key(&(tid, seg_id)));
        } else {
            assert!(!reopened.std_blobs.contains_key(&seg_id));
        }
    }
}

#[test]
fn empty_sealed_blob_segments_are_reclaimed_and_corrupt_std_blobs_not_rescanned() {
    let tmp = TempTestDir::new("empty_and_unreloc_blobs");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut cfg = EngineConfig::default();
    cfg.inline_threshold_bytes = 64;
    cfg.max_blob_segment_bytes = 4096;
    cfg.buf_wal_size_bytes = 8 * 4096;
    let mut engine = StorageEngine::open(Arc::clone(&fs), cfg).unwrap();

    // Register an empty sealed `blob_std` segment directly in state + meta, then reclaim.
    let seq_empty = engine.storage.debug_meta_mgr_mut().reserve_seq_id();
    let empty_writer = BlobSegmentWriter::open(&*fs, BlobKind::Standard, 0, seq_empty).unwrap();
    let empty_reader = empty_writer.reader();
    drop(empty_writer);
    engine.std_blobs.insert(
        seq_empty,
        TrackedStdBlob {
            reader: empty_reader,
            file_len: 0,
            dead_bytes: 0,
        },
    );
    engine.reclaim_fully_dead_blob_segments().unwrap();
    assert!(!engine.std_blobs.contains_key(&seq_empty));

    // Create a fragmented std segment whose remaining live blob is corrupt under `Evict`.
    engine.set_table_corrupt_policy(1, CorruptPolicy::Evict);
    let payload = vec![0x6B; 512];
    for i in 0..4u32 {
        engine
            .put_row(
                1,
                format!("k{i:02}").as_bytes(),
                &payload,
                &[],
                WriteDurability::BufferedFast,
            )
            .unwrap();
    }
    engine.sync_active_std_writer().unwrap();
    engine.storage.debug_clear_active_std_writer();
    for i in 0..3u32 {
        engine
            .delete_row(
                1,
                format!("k{i:02}").as_bytes(),
                WriteDurability::BufferedFast,
            )
            .unwrap();
    }
    assert!(engine.has_fragmented_std_blobs(0.5));
    let seg_id = *engine.std_blobs.keys().next().unwrap();
    let seg_name = crate::meta::format_std_blob_file_name(seg_id);
    engine.flush_memtable().unwrap();
    drop(engine);

    // Corrupt the surviving last blob record while the engine handle is closed.
    let f = fs
        .open_file(
            &seg_name,
            false,
            true,
            crate::pal::OpenHint::ImmutableRandom,
        )
        .unwrap();
    let flen = f.file_len().unwrap();
    f.write_all_at(&[0xFF; 16], flen - 32).unwrap();
    f.sync_durable().unwrap();
    drop(f);

    let mut cfg2 = EngineConfig::default();
    cfg2.inline_threshold_bytes = 64;
    cfg2.max_blob_segment_bytes = 4096;
    let mut engine = StorageEngine::open(Arc::clone(&fs), cfg2).unwrap();
    engine.set_table_corrupt_policy(1, CorruptPolicy::Evict);
    assert!(engine.has_fragmented_std_blobs(0.5));

    assert_eq!(engine.relocate_fragmented_std_blobs(0.5).unwrap(), 0);
    assert!(
        !engine.has_fragmented_std_blobs(0.5),
        "unrelocatable corrupt blob segment must not trigger endless re-scans"
    );
}

#[test]
fn reclaim_dead_blobs_defers_unlink_until_wal_rotation_and_finish_reloc_drains_group() {
    // R2-C2 & R2-M9a:
    // 1. reclaim_fully_dead_blob_segments must defer unlinking while MemTable/WAL is non-empty,
    //    and remove reclaimed std seqs from `unrelocatable_std_blobs`.
    let tmp = TempTestDir::new("r2_c2_reclaim_defers_unlink");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut cfg = EngineConfig::default();
    cfg.inline_threshold_bytes = 64;
    cfg.max_blob_segment_bytes = 1024;
    let mut engine = StorageEngine::open(Arc::clone(&fs), cfg).unwrap();

    engine
        .put_row(1, b"k1", &[0x11u8; 600], &[], WriteDurability::BufferedFast)
        .unwrap();
    let seg1 = *engine.std_blobs.keys().next().unwrap();
    engine
        .put_row(1, b"k2", &[0x22u8; 600], &[], WriteDurability::BufferedFast)
        .unwrap();
    engine.unrelocatable_std_blobs.insert(seg1);
    // Delete k1 in MemTable without flushing; seg1 is dead in MemTable/WAL.
    engine
        .delete_row(1, b"k1", WriteDurability::BufferedFast)
        .unwrap();
    engine.reclaim_fully_dead_blob_segments().unwrap();

    let seg1_name = crate::meta::format_std_blob_file_name(seg1);
    assert!(!engine.std_blobs.contains_key(&seg1));
    assert!(!engine.unrelocatable_std_blobs.contains(&seg1));
    assert_eq!(engine.unlink_after_wal_rotation, vec![seg1_name.clone()]);
    assert!(
        fs.list_files().unwrap().contains(&seg1_name),
        "dead segment referenced by uncheckpointed WAL must stay on disk until WAL rotation"
    );

    // Flush MemTable rotates WAL and deletes `seg1_name`.
    engine.flush_memtable().unwrap();
    assert!(engine.unlink_after_wal_rotation.is_empty());
    assert!(!fs.list_files().unwrap().contains(&seg1_name));

    // 2. finish_std_blob_relocation drains `pending_group` before checking memtable emptiness, and
    //    sets `deferred_flush_on_commit` when called inside an active transaction.
    engine.flush_memtable().unwrap();
    let _ticket = commit_into_group(&mut engine, 1, b"grp_k", b"grp_v");
    assert!(engine.memtable.is_empty());
    assert!(!engine.pending_group.is_empty());
    engine.finish_std_blob_relocation(1).unwrap();
    assert!(engine.pending_group.is_empty());
    assert!(engine.memtable.is_empty());

    engine.begin_transaction_for_conn(9).unwrap();
    engine.finish_std_blob_relocation(1).unwrap();
    assert!(engine.tx_state.active_tx().unwrap().deferred_flush);
    engine.rollback_transaction().unwrap();
}

#[test]
fn open_ignores_non_canonical_hex_blob_filenames() {
    let tmp = TempTestDir::new("non_canon_hex_blobs");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let eng = StorageEngine::open(Arc::clone(&fs), EngineConfig::default()).unwrap();
    drop(eng);

    let decoy_files = [
        "blob_std_1.db",
        "blob_std_000000000000000A.db",
        "blob_std_+000000000000001.db",
        "blob_lru_1_1.db",
        "blob_lru_0001_000000000000000A.db",
    ];
    for name in decoy_files {
        std::fs::write(tmp.path.join(name), b"not_a_canonical_blob").unwrap();
    }

    let reopened = StorageEngine::open(Arc::clone(&fs), EngineConfig::default()).unwrap();
    assert!(reopened.std_blobs.is_empty());
    assert!(reopened.lru_blobs.is_empty());
    drop(reopened);
    // Decoys must remain untouched because open_inner neither adopts nor sweeps non-canonical names.
    for name in decoy_files {
        assert!(tmp.path.join(name).exists(), "missing decoy {name}");
    }
}

#[test]
fn empty_memtable_flush_reclaims_dead_std_blobs_poison_blocks_append_and_delete_clears_unrelocatable()
 {
    // Covers R3-M9 (empty-memtable flush reclaims 100%-dead std blobs), R3-L4 (record_dead_blob_bytes
    // clears unrelocatable_std_blobs), and R3-L3 (append_to_blob_segment rejects poisoned engine).
    let tmp = TempTestDir::new("r3_commit4_blob_invariants");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut cfg = EngineConfig::default();
    cfg.inline_threshold_bytes = 64;
    cfg.max_blob_segment_bytes = 1024;
    let mut engine = StorageEngine::open(Arc::clone(&fs), cfg).unwrap();

    engine
        .put_row(1, b"b1", &[0x11u8; 600], &[], WriteDurability::BufferedFast)
        .unwrap();
    let seg1 = *engine.std_blobs.keys().next().unwrap();
    engine
        .put_row(1, b"b2", &[0x22u8; 600], &[], WriteDurability::BufferedFast)
        .unwrap();
    engine.flush_memtable().unwrap();

    // Mark seg1 unrelocatable, then delete b1 (calls record_dead_blob_bytes).
    engine.unrelocatable_std_blobs.insert(seg1);
    engine
        .delete_row(1, b"b1", WriteDurability::BufferedFast)
        .unwrap();
    assert!(
        !engine.unrelocatable_std_blobs.contains(&seg1),
        "R3-L4: record_dead_blob_bytes must clear seg_id from unrelocatable_std_blobs"
    );

    // Flush the tombstone to idx and WAL, then simulate a 100%-dead sealed segment observed while
    // `memtable` is empty and run `flush_memtable_no_auto_compact`.
    engine.flush_memtable().unwrap();
    engine
        .put_row(1, b"b3", &[0x33u8; 600], &[], WriteDurability::BufferedFast)
        .unwrap();
    let seg_new = *engine
        .std_blobs
        .keys()
        .find(|&&s| s > seg1)
        .expect("new blob segment");
    // Flush b3 into idx, then mark the sealed intermediate segment dead while memtable is empty.
    engine
        .put_row(1, b"b4", &[0x44u8; 600], &[], WriteDurability::BufferedFast)
        .unwrap();
    engine.flush_memtable().unwrap();
    assert!(engine.memtable.is_empty());
    let flen = engine.std_blobs.get(&seg_new).unwrap().file_len;
    engine.std_blobs.get_mut(&seg_new).unwrap().dead_bytes = flen;
    assert!(engine.has_fully_dead_std_blobs());
    engine.flush_memtable_no_auto_compact().unwrap();
    assert!(!engine.has_fully_dead_std_blobs());

    // R3-L3: poisoned engine must refuse `append_to_blob_segment` before writing to disk.
    let segs_before = engine.std_blobs.len();
    engine.debug_poison("simulated poison before blob append");
    let err = engine
        .append_to_blob_segment(crate::wal::BlobKind::Standard, 0, b"k_p", &[0x55u8; 200])
        .unwrap_err();
    assert!(!err.to_string().is_empty());
    assert_eq!(engine.std_blobs.len(), segs_before);
}
