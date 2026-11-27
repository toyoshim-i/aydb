use super::*;

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
        let sync_sectors_before = engine.sync_wal().current_sector_idx();
        let id = engine
            .register_or_resolve_vtab_with_durability(
                "sync_cat_tbl",
                None,
                WriteDurability::SyncTransactional,
            )
            .unwrap();
        assert!(
            engine.sync_wal().current_sector_idx() > sync_sectors_before,
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
    assert!(!engine.tx_state.is_in_tx());
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
    use crate::pal::test_fs::{FsHooks, HookedFs};
    use std::sync::atomic::AtomicBool;

    struct FaultHooks {
        fail_wal: Arc<AtomicBool>,
    }
    impl FsHooks for FaultHooks {
        fn write_all_at(
            &self,
            name: &str,
            inner: &dyn PlatformFile,
            buf: &[u8],
            offset: u64,
        ) -> io::Result<()> {
            if name.starts_with("wal_") && self.fail_wal.load(Ordering::SeqCst) {
                return Err(io::Error::other("simulated WAL write failure"));
            }
            inner.write_all_at(buf, offset)
        }
    }

    let tmp = TempTestDir::new("overlay_commit_fault");
    let fail = Arc::new(AtomicBool::new(false));
    let fs = Arc::new(HookedFs::new(
        OsFs::open(&tmp.path, true).unwrap(),
        FaultHooks {
            fail_wal: Arc::clone(&fail),
        },
    ));
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
    assert!(!engine.tx_state.is_in_tx());
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
fn drop_table_with_tombstones_larger_than_wal_is_atomic_and_durable() {
    let tmp = TempTestDir::new("drop_table_larger_than_wal");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut cfg = EngineConfig::default();
    cfg.buf_wal_size_bytes = 64 * 1024;
    cfg.sync_wal_size_bytes = 64 * 1024;
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
            .reconcile_unique_keys_batch(
                1,
                &[(b"a".to_vec(), 1, Vec::new())],
                WriteDurability::BufferedFast
            )
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
            !engine
                .tx_state
                .active_tx()
                .unwrap()
                .overlay
                .map
                .contains_key(&seeded_row_key),
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
    engine.tx_state.active_tx_mut().unwrap().deferred_flush = true;

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
    engine.tx_state.active_tx_mut().unwrap().deferred_flush = true;
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
    assert!(engine.tx_state.active_tx().unwrap().overlay.map.is_empty());
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
            RangeScanOptions {
                lower: None,
                upper: None,
                reverse: true,
                limit: 100,
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
    let mut overlay = crate::engine::tx::TxOverlay::default();
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
    {
        let tx = engine.tx_state.active_tx().unwrap();
        assert!(engine.tx_state.statement_savepoint_active());
        assert!(matches!(
            tx.savepoint_payloads.last().unwrap().meta,
            crate::engine::tx::SavepointMeta::Statement(_)
        ));
    }

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
    {
        let tx = engine.tx_state.active_tx().unwrap();
        assert!(!engine.tx_state.statement_savepoint_active());
        assert!(tx.statement_blob_undo.is_empty());
    }

    engine.rollback_transaction_for_conn(1).unwrap();
}

#[test]
fn commit_gate_join_group_returns_durable_before_later_failure() {
    let gate = crate::engine::tx::CommitGate::default();
    gate.mark_durable(5, true);
    gate.fail(&io::Error::other("later failure"));

    assert!(matches!(gate.join_group(5).unwrap(), GroupRole::Durable));
    assert!(gate.join_group(6).is_err());
}

#[test]
fn finish_group_flush_handles_already_durable_drain_race_and_late_failure() {
    let tmp1 = TempTestDir::new("group_drain_race_ok");
    let mut engine1 = open_group_engine(&tmp1);
    let ticket1 = commit_into_group(&mut engine1, 1, b"a", b"1");
    let GroupRole::Lead(target1) = ticket1.2.join_group(ticket1.0).unwrap() else {
        panic!("expected leader role");
    };
    engine1.drain_pending_durable().unwrap();
    assert_eq!(row_visible_to(&engine1, 9, b"a"), Some(b"1".to_vec()));
    engine1.finish_group_flush(target1, Ok(())).unwrap();
    assert!(!engine1.is_poisoned());
    assert!(matches!(
        ticket1.2.join_group(ticket1.0 + 1).unwrap(),
        GroupRole::Lead(_)
    ));

    let tmp2 = TempTestDir::new("group_drain_race_err");
    let mut engine2 = open_group_engine(&tmp2);
    let ticket2 = commit_into_group(&mut engine2, 1, b"a", b"1");
    let GroupRole::Lead(target2) = ticket2.2.join_group(ticket2.0).unwrap() else {
        panic!("expected leader role");
    };
    engine2.drain_pending_durable().unwrap();
    assert_eq!(row_visible_to(&engine2, 9, b"a"), Some(b"1".to_vec()));
    // The batch at `target2` was already synced & absorbed; late leader failure still completes
    // `target2`, releases leadership, and poisons the engine for subsequent writes.
    engine2
        .finish_group_flush(target2, Err(io::Error::other("late sync err")))
        .unwrap();
    assert!(engine2.is_poisoned());
    assert!(ticket2.2.is_durable_through(target2));
    assert!(matches!(
        ticket2.2.join_group(target2).unwrap(),
        GroupRole::Durable
    ));
    assert!(matches!(
        ticket2.2.join_group(target2 + 1).unwrap(),
        GroupRole::Lead(_)
    ));
}

#[test]
fn poisoned_engine_hides_pending_group_from_open_writer_reads() {
    let tmp1 = TempTestDir::new("poisoned_pending_writer_err");
    let mut engine1 = open_group_engine(&tmp1);
    let ticket = commit_into_group(&mut engine1, 1, b"k1", b"v1");
    engine1.begin_transaction_for_conn(2).unwrap();
    assert_eq!(engine1.get_row(1, b"k1").unwrap(), Some(b"v1".to_vec()));
    let GroupRole::Lead(target) = ticket.2.join_group(ticket.0).unwrap() else {
        panic!("expected leader role");
    };
    assert!(
        engine1
            .finish_group_flush(target, Err(io::Error::other("flush failed")))
            .is_err()
    );
    assert!(!engine1.pending_group.is_empty());
    assert_eq!(engine1.get_row(1, b"k1").unwrap(), None);
    assert!(
        engine1
            .scan_table_range(1, None, None, 100)
            .unwrap()
            .is_empty()
    );

    let tmp2 = TempTestDir::new("poisoned_pending_writer_prepoison");
    let mut engine2 = open_group_engine(&tmp2);
    let _ticket2 = commit_into_group(&mut engine2, 1, b"k1", b"v1");
    engine2.begin_transaction_for_conn(2).unwrap();
    assert_eq!(engine2.get_row(1, b"k1").unwrap(), Some(b"v1".to_vec()));
    let _ = engine2.poison(io::Error::other("prior failure"));
    assert!(!engine2.pending_group.is_empty());
    assert_eq!(engine2.get_row(1, b"k1").unwrap(), None);
    assert!(
        engine2
            .scan_table_range(1, None, None, 100)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn table_has_synthetic_rowids_snapshot_and_restore_across_tx_and_savepoint() {
    let tmp = TempTestDir::new("synthetic_rowid_tx_snap");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut engine = StorageEngine::open(fs, EngineConfig::default()).unwrap();
    let sync = WriteDurability::SyncTransactional;

    // Seed table 1 with a synthetic rowid mapping and populate `table_has_synthetic_rowids`.
    engine.put_row(1, b"pk1", b"val1", &[], sync).unwrap();
    let rid = engine
        .assign_rowid(1, b"pk1", 10, sync, |_, _| Ok(false))
        .unwrap();
    assert!(engine.may_have_synthetic_rowids(1).unwrap());
    assert_eq!(engine.test_table_has_synthetic_rowids(1), Some(true));

    // Seed table 2 with no synthetic rowids and cache `false`.
    assert!(!engine.may_have_synthetic_rowids(2).unwrap());
    assert_eq!(engine.test_table_has_synthetic_rowids(2), Some(false));

    engine.begin_transaction_for_conn(1).unwrap();
    engine.savepoint_for_conn(1, 10).unwrap();

    // Dropping table 1 in-tx clears the owner cache, and probing or reusing `table_id = 1` sets
    // the owner cache to `false`; concurrent `ReadView::Committed` must keep seeing committed
    // synthetic rowids.
    engine.drop_table(1, None).unwrap();
    assert_eq!(engine.test_table_has_synthetic_rowids(1), None);
    assert!(!engine.may_have_synthetic_rowids(1).unwrap());
    assert_eq!(engine.test_table_has_synthetic_rowids(1), Some(false));
    assert_eq!(
        engine
            .rowid_for_pk_view(ReadView::Owner, 1, b"pk1")
            .unwrap(),
        None
    );
    assert_eq!(
        engine.pk_for_rowid_view(ReadView::Owner, 1, rid).unwrap(),
        None
    );
    assert_eq!(
        engine
            .rowid_for_pk_view(ReadView::Committed, 1, b"pk1")
            .unwrap(),
        Some(rid)
    );
    assert_eq!(
        engine
            .pk_for_rowid_view(ReadView::Committed, 1, rid)
            .unwrap(),
        Some(b"pk1".to_vec())
    );

    // Rolling back to the savepoint restores the `true` cache flag.
    engine.rollback_to_savepoint_for_conn(1, 10).unwrap();
    assert_eq!(engine.test_table_has_synthetic_rowids(1), Some(true));
    assert_eq!(
        engine
            .rowid_for_pk_view(ReadView::Owner, 1, b"pk1")
            .unwrap(),
        Some(rid)
    );

    // Drop table 1 again (re-caching `Some(false)` in owner state) and also add a synthetic rowid
    // on table 2 in-tx.
    engine.drop_table(1, None).unwrap();
    assert!(!engine.may_have_synthetic_rowids(1).unwrap());
    assert_eq!(engine.test_table_has_synthetic_rowids(1), Some(false));
    engine.put_row(2, b"p2", b"v2", &[], sync).unwrap();
    let rid2 = engine
        .assign_rowid(2, b"p2", 1, sync, |_, _| Ok(false))
        .unwrap();
    assert_eq!(
        engine
            .rowid_for_pk_view(ReadView::Committed, 2, b"p2")
            .unwrap(),
        None
    );
    assert_eq!(
        engine.rowid_for_pk_view(ReadView::Owner, 2, b"p2").unwrap(),
        Some(rid2)
    );

    // Full rollback restores both tables' synthetic-rowid cache flags.
    engine.rollback_transaction_for_conn(1).unwrap();
    assert_eq!(engine.test_table_has_synthetic_rowids(1), Some(true));
    assert_eq!(engine.test_table_has_synthetic_rowids(2), Some(false));
    assert_eq!(
        engine
            .rowid_for_pk_view(ReadView::Owner, 1, b"pk1")
            .unwrap(),
        Some(rid)
    );
    assert_eq!(
        engine
            .pk_for_rowid_view(ReadView::Committed, 1, rid)
            .unwrap(),
        Some(b"pk1".to_vec())
    );

    // Deleting the restored row removes its synthetic `'u'`/`'v'` entries instead of skipping them.
    assert!(engine.delete_row(1, b"pk1", sync).unwrap());
    assert_eq!(
        engine
            .rowid_for_pk_view(ReadView::Committed, 1, b"pk1")
            .unwrap(),
        None
    );
    assert_eq!(
        engine
            .pk_for_rowid_view(ReadView::Committed, 1, rid)
            .unwrap(),
        None
    );
}

#[test]
fn group_gate_failure_and_awaiting_sync_abort_poison_and_block_compaction() {
    // 1. commit_gate failure alone makes is_poisoned() true and suppresses run_due_auto_compaction,
    //    while check_not_poisoned preserves explicit `self.poisoned` messages.
    {
        let tmp = TempTestDir::new("r2_h3_gate_poison");
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
        let mut engine = StorageEngine::open(fs, EngineConfig::default()).unwrap();
        engine.auto_compaction_due = true;
        engine
            .commit_gate
            .fail(&io::Error::other("simulated leader EIO"));
        assert!(engine.is_poisoned());
        engine.run_due_auto_compaction();
        assert!(
            engine.auto_compaction_due,
            "run_due_auto_compaction must not run when commit_gate has failed"
        );
        let msg = engine.check_not_poisoned().unwrap_err().to_string();
        assert!(
            msg.contains("simulated leader EIO"),
            "unexpected msg: {msg}"
        );

        let _ = engine.poison(io::Error::other("primary disk fault"));
        let msg2 = engine.check_not_poisoned().unwrap_err().to_string();
        assert!(
            msg2.contains("primary disk fault"),
            "explicit poison message must take precedence: {msg2}"
        );
    }

    // 2. Rollback after CommitStep::AwaitingSync (before finish_commit_for_conn) must poison.
    {
        let tmp = TempTestDir::new("r2_m7_awaiting_sync_abort");
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
        let mut engine = StorageEngine::open(fs, EngineConfig::default()).unwrap();
        engine.register_capped_table(1, 1024 * 1024).unwrap(); // Non-groupable -> AwaitingSync
        engine.begin_transaction_for_conn(1).unwrap();
        engine
            .put_row(
                1,
                b"k1",
                &[0x11u8; 64],
                &[],
                WriteDurability::SyncTransactional,
            )
            .unwrap();
        let lsn_before = engine.next_lsn;
        engine
            .prepare_commit_transaction(WriteDurability::SyncTransactional)
            .unwrap();
        let step = engine.begin_commit_for_conn(1).unwrap();
        assert!(matches!(
            step,
            crate::engine::tx::CommitStep::AwaitingSync(_)
        ));
        assert!(engine.next_lsn > lsn_before);
        // Simulate abort before finish_commit_for_conn runs.
        engine.rollback_transaction().unwrap();
        assert!(engine.is_poisoned());
        assert!(
            engine
                .put_row(1, b"k2", b"v2", &[], WriteDurability::SyncTransactional)
                .is_err()
        );
    }
}

#[test]
fn commit_at_zero_depth_preserves_state_and_one_depth_rolls_back_on_poison() {
    // R2-H2: commit_transaction when TxState::Idle on a poisoned engine is a no-op that never
    // modifies committed state; at depth == 1 it rolls back the active transaction overlay and
    // fails cleanly.
    let tmp = TempTestDir::new("r2_h2_commit_poison_depth");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut engine = StorageEngine::open(fs, EngineConfig::default()).unwrap();
    let plan = engine.plan_vtab_registration("t", None).unwrap();
    let (tid, _) = engine
        .register_vtab_plan(plan, WriteDurability::BufferedFast, None)
        .unwrap();
    engine.register_capped_table(tid, 64 * 1024).unwrap();
    engine
        .put_row(
            tid,
            b"k1",
            &[0x22u8; 2048],
            &[],
            WriteDurability::BufferedFast,
        )
        .unwrap();

    let next_lsn_before = engine.next_lsn();
    let std_blob_cnt = engine.std_blobs.len();
    let lru_blob_cnt = engine.lru_blobs.len();
    let capped_cnt = engine.capped_tables.len();
    let active_cnt = engine.active_vtab_ids.len();

    engine.begin_transaction_for_conn(1).unwrap();
    engine
        .put_row(
            tid,
            b"k2",
            b"uncommitted",
            &[],
            WriteDurability::BufferedFast,
        )
        .unwrap();
    engine.debug_poison("simulated poison at depth 1");
    assert!(engine.commit_transaction().is_err());
    assert!(!engine.tx_state.is_in_tx());
    assert!(engine.get_row(tid, b"k2").unwrap().is_none());

    // Calling commit_transaction again while TxState::Idle must NOT zero out state.
    engine.commit_transaction().unwrap();
    assert_eq!(engine.next_lsn(), next_lsn_before);
    assert_eq!(engine.std_blobs.len(), std_blob_cnt);
    assert_eq!(engine.lru_blobs.len(), lru_blob_cnt);
    assert_eq!(engine.capped_tables.len(), capped_cnt);
    assert_eq!(engine.active_vtab_ids.len(), active_cnt);
}

#[test]
fn quota_only_tx_drains_pending_group_before_persisting_boot_meta() {
    // R2-H5: a zero-record tx that modifies table_quotas must drain pending_group before
    // persisting `table_quotas` to `boot_meta.db`.
    let tmp = TempTestDir::new("r2_h5_quota_drains_group");
    let mut engine = open_group_engine(&tmp);
    let ticket = commit_into_group(&mut engine, 1, b"seed", &[0x44u8; 64]);
    assert!(!engine.pending_group.is_empty());

    engine.begin_transaction_for_conn(2).unwrap();
    engine.register_capped_table(2, 4096).unwrap();
    assert_eq!(engine.tx_state.pending_record_count(), 0);
    assert!(engine.tx_state.active_tx().unwrap().quotas_modified);

    engine.commit_transaction_for_conn(2).unwrap();
    assert!(engine.pending_group.is_empty());
    assert!(matches!(
        ticket.2.join_group(ticket.0).unwrap(),
        GroupRole::Durable
    ));
    let stored_q = engine
        .meta_mgr()
        .state()
        .table_quotas
        .iter()
        .find(|q| q.table_id == 2)
        .expect("table quota persisted");
    assert_eq!(stored_q.max_bytes, 4096);
}

#[test]
fn mixed_durability_sync_vtab_before_buffered_vtab_keeps_sync_upgrade() {
    // R2-M6: if a sync vtab executes `xSync` before a buffered vtab in a multi-vtab tx where no
    // sync table writes occurred (or after tx_prepared), `prepare_commit_transaction` must still
    // see `need_sync_upgrade` and upgrade `pending_tx_sync` / WAL prefix durability.
    let tmp = TempTestDir::new("r2_m6_mixed_vtab_sync_order");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut engine = StorageEngine::open(fs, EngineConfig::default()).unwrap();

    // Seed an unsynced buffered commit first so a sync upgrade must make it durable.
    engine
        .put_row(1, b"buf_pre", b"v0", &[], WriteDurability::BufferedFast)
        .unwrap();
    assert!(engine.last_buf_append_lsn > engine.last_buf_synced_lsn);

    engine.begin_vtab_transaction(7, 1001).unwrap();
    engine.begin_vtab_transaction(7, 1002).unwrap();
    // Write only to the buffered table (1002).
    engine
        .put_row(2, b"k_buf", b"v1", &[], WriteDurability::BufferedFast)
        .unwrap();
    // Prepare earlier once under buffered durability (e.g. a retried COMMIT).
    engine
        .prepare_commit_transaction(WriteDurability::BufferedFast)
        .unwrap();
    assert!(engine.tx_state.is_prepared());
    assert!(!engine.tx_state.pending_sync());

    // Now vtab 1001 (SyncTransactional) enters xSync first, followed by vtab 1002 (BufferedFast).
    assert!(!engine.mark_vtab_synced_ready_to_commit(1001, WriteDurability::SyncTransactional));
    assert!(
        !engine.tx_state.pending_sync(),
        "mark_vtab_synced_ready_to_commit must not prematurely flip pending_sync"
    );
    assert!(engine.mark_vtab_synced_ready_to_commit(1002, WriteDurability::BufferedFast));

    // vtab 1002 calls prepare_commit_transaction with its own BufferedFast durability:
    engine
        .prepare_commit_transaction(WriteDurability::BufferedFast)
        .unwrap();
    assert!(engine.tx_state.pending_sync());
    assert_eq!(engine.last_buf_synced_lsn, engine.last_buf_append_lsn);

    let step = engine.begin_commit_for_conn(7).unwrap();
    assert!(
        matches!(step, CommitStep::AwaitingGroup { .. }),
        "upgraded mixed tx must go through synchronous WAL group commit"
    );
}

#[test]
fn empty_sync_tx_does_not_leak_pending_tx_sync_to_subsequent_buffered_tx() {
    // R3-M6: an empty SyncTransactional prepare/commit must clear `pending_tx_sync` so subsequent
    // BufferedFast transactions stay on `wal_buf.db` without hitting `wal_sync.db`.
    let tmp = TempTestDir::new("r3_m6_empty_sync_tx_leak");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut engine = StorageEngine::open(fs, EngineConfig::default()).unwrap();

    let sync_sector_before = engine.sync_wal().current_sector_idx();
    engine.begin_transaction();
    engine
        .prepare_commit_transaction(WriteDurability::SyncTransactional)
        .unwrap();
    engine.commit_transaction().unwrap();
    assert!(!engine.tx_state.pending_sync());
    assert_eq!(engine.sync_wal().current_sector_idx(), sync_sector_before);

    engine.begin_transaction();
    engine
        .put_row(1, b"k_buf", b"v_buf", &[], WriteDurability::BufferedFast)
        .unwrap();
    engine
        .prepare_commit_transaction(WriteDurability::BufferedFast)
        .unwrap();
    assert!(!engine.tx_state.pending_sync());
    engine.commit_transaction().unwrap();
    assert_eq!(
        engine.sync_wal().current_sector_idx(),
        sync_sector_before,
        "BufferedFast tx must not spill to wal_sync.db after an empty sync tx"
    );
}

#[test]
fn tx_state_idle_clears_all_state_across_commit_group_and_rollback() {
    // INV-TX-1: Moving back to TxState::Idle on regular commit, group-commit handoff, or rollback
    // drops ActiveTx atomically and leaves zero residual flags for subsequent transactions.
    let tmp = TempTestDir::new("m1_tx_state_idle_clean");
    let mut engine = open_group_engine(&tmp);

    let assert_idle = |eng: &StorageEngine<OsFs>| {
        assert!(!eng.in_active_transaction());
        assert!(!eng.tx_state.is_in_tx());
        assert!(eng.tx_state.phase().is_none());
        assert!(eng.tx_state.start_meta().is_none());
        assert_eq!(eng.tx_state.pending_record_count(), 0);
        assert!(!eng.tx_state.pending_sync());
    };

    assert_idle(&engine);

    // 1. Synchronous commit with savepoint, vtab attachment, and deferred flag.
    engine.begin_vtab_transaction(1, 1001).unwrap();
    engine.savepoint_for_conn(1, 1).unwrap();
    engine
        .put_row(1, b"k1", b"v1", &[], WriteDurability::SyncTransactional)
        .unwrap();
    engine.tx_state.active_tx_mut().unwrap().deferred_flush = true;
    engine.prepare_commit_transaction(GROUP_SYNC).unwrap();
    engine.commit_transaction_for_conn(1).unwrap();
    assert_idle(&engine);

    // 2. Subsequent buffered transaction must not inherit prior sync state.
    let sync_sector_before = engine.sync_wal().current_sector_idx();
    engine.begin_transaction_for_conn(1).unwrap();
    engine
        .put_row(1, b"k2", b"v2", &[], WriteDurability::BufferedFast)
        .unwrap();
    assert!(!engine.tx_state.pending_sync());
    engine.commit_transaction_for_conn(1).unwrap();
    assert_idle(&engine);
    assert_eq!(engine.sync_wal().current_sector_idx(), sync_sector_before);

    // 3. Group-commit handoff moves `tx_state` to `Idle` while awaiting barrier completion.
    let ticket = commit_into_group(&mut engine, 2, b"k_grp", b"v_grp");
    assert_idle(&engine);
    flush_group(&mut engine, &ticket);
    assert_idle(&engine);
    assert_eq!(
        engine.get_row(1, b"k_grp").unwrap(),
        Some(b"v_grp".to_vec())
    );

    // 4. Explicit rollback also transitions cleanly to `Idle`.
    engine.begin_vtab_transaction(3, 2001).unwrap();
    engine.savepoint_for_conn(3, 10).unwrap();
    engine
        .put_row(1, b"k_abort", b"v_abort", &[], GROUP_SYNC)
        .unwrap();
    engine.rollback_transaction_for_conn(3).unwrap();
    assert_idle(&engine);
    assert_eq!(engine.get_row(1, b"k_abort").unwrap(), None);
}

#[test]
fn write_scope_internal_bypass_preserves_prepare_while_normal_invalidates() {
    // INV-TX-2 & INV-TX-3: WriteScope::InternalBypass bypasses `max_transaction_bytes` and keeps
    // `TxPhase::Prepared` + `sync_durability_upgrade` + `synced_vtabs`, while `WriteScope::Normal`
    // enforces the transaction byte budget and invalidates back to `TxPhase::Active`.
    let tmp = TempTestDir::new("m1_write_scope_prepare");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut cfg = EngineConfig::default();
    cfg.max_transaction_bytes = 256;
    let mut engine = StorageEngine::open(fs, cfg).unwrap();

    engine.begin_vtab_transaction(1, 1001).unwrap();
    engine.begin_vtab_transaction(1, 1002).unwrap();
    engine
        .put_row(1, b"k_init", b"v1", &[], WriteDurability::BufferedFast)
        .unwrap();
    engine
        .prepare_commit_transaction(WriteDurability::BufferedFast)
        .unwrap();
    assert_eq!(
        engine.tx_state.phase(),
        Some(crate::engine::tx::TxPhase::Prepared)
    );

    // Simulate table 1001 (SyncTransactional) reaching `xSync` before table 1002.
    assert!(!engine.mark_vtab_synced_ready_to_commit(1001, WriteDurability::SyncTransactional));
    {
        let core = engine.tx_state.core().unwrap();
        assert!(core.sync_durability_upgrade());
        assert_eq!(core.synced_vtabs(), &[1001]);
    }

    // InternalBypass succeeds past the 256-byte limit and leaves preparation/durability intact.
    engine
        .commit_wal_and_memtable_core(
            vec![crate::wal::WalRecord::PutInline {
                key: crate::lru::encode_primary_row_key(1, b"k_bypass"),
                value: vec![b'b'; 400],
            }],
            WriteDurability::BufferedFast,
            true,
            false,
            crate::engine::tx::WriteScope::InternalBypass,
        )
        .unwrap();
    assert_eq!(
        engine.tx_state.phase(),
        Some(crate::engine::tx::TxPhase::Prepared)
    );
    {
        let core = engine.tx_state.core().unwrap();
        assert!(core.sync_durability_upgrade());
        assert_eq!(core.synced_vtabs(), &[1001]);
    }

    // Normal scope rejects writes exceeding `max_transaction_bytes` while leaving `Prepared` intact.
    let err = engine
        .commit_wal_and_memtable_core(
            vec![crate::wal::WalRecord::PutInline {
                key: crate::lru::encode_primary_row_key(1, b"k_too_big"),
                value: vec![b'x'; 64],
            }],
            WriteDurability::BufferedFast,
            true,
            false,
            crate::engine::tx::WriteScope::Normal,
        )
        .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(
        engine.tx_state.phase(),
        Some(crate::engine::tx::TxPhase::Prepared)
    );

    // Lower budget check with Normal scope via resetting config budget above `pending_bytes`:
    // a small Normal write invalidates `Prepared` -> `Active` and clears `synced_vtabs` + upgrade.
    engine.config.max_transaction_bytes = 4096;
    engine
        .put_row(1, b"k_normal", b"ok", &[], WriteDurability::BufferedFast)
        .unwrap();
    assert_eq!(
        engine.tx_state.phase(),
        Some(crate::engine::tx::TxPhase::Active)
    );
    {
        let core = engine.tx_state.core().unwrap();
        assert!(!core.sync_durability_upgrade());
        assert!(core.synced_vtabs().is_empty());
    }
    engine.rollback_transaction_for_conn(1).unwrap();
}
