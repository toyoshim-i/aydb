use super::*;

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
            .healthy()
            .unwrap()
            .commit_meta_update(|st| {
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
            engine.sync_wal().active_epoch(),
            engine.meta_mgr().state().sync_wal_epoch
        );
        assert_eq!(
            engine.buf_wal().active_epoch(),
            engine.meta_mgr().state().buf_wal_epoch
        );
        assert!(engine.capped_table_state(1).unwrap().current_logical_bytes > 0);
        (
            engine.sync_wal().active_epoch(),
            engine.buf_wal().active_epoch(),
        )
        // Dropped without flushing: simulates a crash.
    };

    let mut engine = StorageEngine::open(fs, cfg).unwrap();
    assert!(engine.sync_wal().active_epoch() > epochs.0);
    assert!(engine.buf_wal().active_epoch() > epochs.1);
    for k in [b"a", b"b", b"c"] {
        assert_eq!(engine.get_row(1, k).unwrap(), Some(b"value".to_vec()));
    }
    assert!(engine.capped_table_state(1).unwrap().current_logical_bytes > 0);
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
            .healthy()
            .unwrap()
            .commit_meta_update(|st| st.table_quotas[0].current_logical_bytes = 12_345)
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

    assert!(engine.active_lru_seq_id(tid).is_some());
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
        engine.active_lru_seq_id(tid).is_some(),
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
fn rolled_back_tx_with_capped_eviction_preserves_hot_access_ticks() {
    let tmp = TempTestDir::new("tx_evict_preserves_hot_ticks");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut engine = StorageEngine::open(fs, EngineConfig::default()).unwrap();

    // Small cap so a second row triggers eviction + lazy promotion.
    engine.register_capped_table(1, 200).unwrap();
    engine
        .put_row(1, b"k1", &[0x11; 150], &[], WriteDurability::BufferedFast)
        .unwrap();
    engine
        .put_row(1, b"k2", &[0x22; 40], &[], WriteDurability::BufferedFast)
        .unwrap();
    assert_eq!(engine.get_row(1, b"k1").unwrap(), Some(vec![0x11; 150]));
    let hot_before = engine.access_tracker.peek_hot_tick(1, b"k1");
    assert!(hot_before.is_some());

    engine.begin_transaction();
    engine
        .put_row(1, b"k3", &[0x33; 150], &[], WriteDurability::BufferedFast)
        .unwrap();
    engine
        .prepare_commit_transaction(WriteDurability::BufferedFast)
        .unwrap();
    engine.rollback_transaction().unwrap();
    assert_eq!(engine.access_tracker.peek_hot_tick(1, b"k1"), hot_before);
}

#[test]
fn evict_until_within_quota_batches_window_evictions_and_promotions_into_single_wal_sync() {
    let tmp = TempTestDir::new("ph4_batched_eviction");
    let (fs, _, barrier_counts) = open_counted(&tmp.path);

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
fn dropped_capped_table_lru_sync_failure_poisons_before_advancing_wal_durable_lsn() {
    let fs = Arc::new(PowerLossFs::new());
    let mut cfg = EngineConfig::default();
    cfg.inline_threshold_bytes = 256;
    let mut engine = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();

    engine.register_capped_table(1, 1024 * 1024).unwrap();
    let payload = vec![0xABu8; 2048];
    engine
        .put_row(1, b"k1", &payload, &[], WriteDurability::BufferedFast)
        .unwrap();
    let durable_before = engine.wal_links.stamp().1;
    assert!(engine.last_buf_append_lsn > engine.last_buf_synced_lsn);

    // Next sync on any `blob_*` file fails once (fsyncgate), dropping dirty pages from cache.
    fs.arm_blob_sync_failure();
    let _ = engine.drop_table_with_durability(1, None, WriteDurability::BufferedFast);

    assert!(
        engine.is_poisoned(),
        "failed LRU blob sync on dropped table must poison the engine"
    );
    assert!(
        engine.maintenance_failures() >= 1,
        "failed LRU blob sync must be counted as a maintenance failure"
    );
    assert_eq!(
        engine.wal_links.stamp().1,
        durable_before,
        "durable watermark must not advance over a pre-drop WAL batch whose LRU blob failed to sync"
    );

    // Subsequent synchronous writes on an unrelated table must be refused rather than acknowledged
    // above an unrecoverable pre-drop `PutBlobPtr`.
    assert!(
        engine
            .put_row(2, b"k2", b"val", &[], WriteDurability::SyncTransactional)
            .is_err()
    );
}

#[test]
fn poisoned_engine_rejects_lru_reclaim_and_release_unlinkable() {
    // R2-H4
    let tmp = TempTestDir::new("r2_h4_poison_reclaim_guards");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut engine = StorageEngine::open(fs, EngineConfig::default()).unwrap();
    engine.register_capped_table(1, 64 * 1024).unwrap();
    engine
        .unlink_after_wal_rotation
        .push("blob_lru_0001_0000000000000001.db".to_string());
    engine.debug_poison("simulated WAL poison");

    assert!(engine.reclaim_fully_dead_lru_blob_segments(1).is_err());
    assert!(engine.release_unlinkable_blob_files().is_err());
    assert_eq!(engine.unlink_after_wal_rotation.len(), 1);
}

#[test]
fn single_sector_wal_sub_batches_eviction_and_relocation_and_stops_on_flush_err() {
    // R2-M4 & R2-M5
    assert_eq!(
        crate::engine::eviction::max_wal_sub_batch_bytes(1, 64 * 1024),
        4045
    );

    let tmp = TempTestDir::new("r2_m4_m5_one_sector_wal");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut cfg = EngineConfig::default();
    cfg.buf_wal_size_bytes = 4096; // 1 sector
    cfg.sync_wal_size_bytes = 4096; // 1 sector
    cfg.inline_threshold_bytes = 64;
    cfg.max_blob_segment_bytes = 4096;
    let mut engine = StorageEngine::open(Arc::clone(&fs), cfg).unwrap();

    // Insert 150 small inline rows into capped table 2 under a large quota, then shrink quota
    // to force multi-sub-batch eviction on a 1-sector WAL.
    engine.register_capped_table(2, 1_000_000).unwrap();
    for i in 0..150u32 {
        let pk = format!("ev_{i:04}");
        engine
            .put_row(
                2,
                pk.as_bytes(),
                &[0x55u8; 40],
                &[],
                WriteDurability::BufferedFast,
            )
            .unwrap();
    }
    engine.register_capped_table(2, 512).unwrap();
    assert!(
        engine.capped_table_state(2).unwrap().current_logical_bytes <= 512,
        "1-sector WAL eviction must split sub-batches <= 4045 B without WriteZero"
    );

    // Populate fragmented std blobs across many keys so relocation spans multiple WAL sub-batches
    // on the 1-sector WAL.
    for i in 0..220u32 {
        let pk = format!("rel_{i:04}_padded_pk_to_fill_wal_batch");
        engine
            .put_row(
                1,
                pk.as_bytes(),
                &[0x77u8; 128],
                &[],
                WriteDurability::BufferedFast,
            )
            .unwrap();
    }
    engine.compact().unwrap();
    for i in 0..220u32 {
        if i % 3 < 2 {
            let pk = format!("rel_{i:04}_padded_pk_to_fill_wal_batch");
            engine
                .delete_row(1, pk.as_bytes(), WriteDurability::BufferedFast)
                .unwrap();
        }
    }
    engine.flush_memtable().unwrap();

    // Test R2-M4: mid-chunk flush failure must not advance `session.seek_key` past un-flushed keys.
    if let Some(mut session) = engine.start_std_blob_relocation(0.2).unwrap() {
        let initial_seek = session.seek_key.clone();
        engine.debug_fail_next_wal_append();
        let err = engine.step_std_blob_relocation(&mut session, 200, usize::MAX);
        assert!(err.is_err());
        assert!(engine.is_poisoned());
        assert_eq!(
            session.seek_key, initial_seek,
            "failed sub-batch flush must not advance seek_key beyond last committed sub-batch"
        );
    }

    // Reopen clean engine and confirm 1-sector WAL relocation succeeds cleanly end-to-end.
    drop(engine);
    let mut cfg2 = EngineConfig::default();
    cfg2.buf_wal_size_bytes = 4096;
    cfg2.sync_wal_size_bytes = 4096;
    cfg2.inline_threshold_bytes = 64;
    cfg2.max_blob_segment_bytes = 4096;
    let mut engine2 = StorageEngine::open(Arc::clone(&fs), cfg2).unwrap();
    let relocated = engine2.relocate_fragmented_std_blobs(0.2).unwrap();
    assert!(relocated > 0);
}

#[test]
fn tiny_memtable_autocommit_updates_persist_exact_quotas_and_dead_bytes() {
    // R2-H1: with memtable_capacity_bytes = 1, every autocommit batch triggers an auto-flush.
    // Quota totals and dead blob bytes must be updated before that auto-flush persists boot_meta.
    let tmp = TempTestDir::new("r2_h1_tiny_memtable_quota_dead_bytes");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut cfg = EngineConfig::default();
    cfg.memtable_capacity_bytes = 1;
    cfg.inline_threshold_bytes = 64;
    cfg.max_blob_segment_bytes = 4096;

    {
        let mut engine = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
        engine.register_capped_table(1, 100_000).unwrap();

        engine
            .put_row(1, b"k1", &[0x11u8; 600], &[], WriteDurability::BufferedFast)
            .unwrap();
        let stored1 = engine
            .meta_mgr()
            .state()
            .table_quotas
            .iter()
            .find(|q| q.table_id == 1)
            .unwrap()
            .current_logical_bytes;
        assert_eq!(stored1, 602);

        // Overwrite with a 200B blob -> auto-flushes in put_row_with_extra_records.
        engine
            .put_row(1, b"k1", &[0x22u8; 200], &[], WriteDurability::BufferedFast)
            .unwrap();
        let stored2 = engine
            .meta_mgr()
            .state()
            .table_quotas
            .iter()
            .find(|q| q.table_id == 1)
            .unwrap()
            .current_logical_bytes;
        assert_eq!(stored2, 202);
        let stored_dead: u64 = engine
            .meta_mgr()
            .state()
            .lru_blob_segments
            .iter()
            .filter(|b| b.table_id == 1)
            .map(|b| b.dead_bytes)
            .sum();
        assert!(
            stored_dead > 600,
            "old 600B blob dead bytes must be persisted by auto-flush (got {stored_dead})"
        );

        // Delete -> auto-flushes in delete_row_with_rowid_hint.
        assert!(
            engine
                .delete_row(1, b"k1", WriteDurability::BufferedFast)
                .unwrap()
        );
        let stored3 = engine
            .meta_mgr()
            .state()
            .table_quotas
            .iter()
            .find(|q| q.table_id == 1)
            .unwrap()
            .current_logical_bytes;
        assert_eq!(stored3, 0);
        engine.checkpoint().unwrap();
    }

    let reopened = StorageEngine::open(fs, cfg).unwrap();
    assert_eq!(
        reopened
            .capped_table_state(1)
            .unwrap()
            .current_logical_bytes,
        0
    );
}

#[test]
fn reconcile_capped_table_tiny_memtable_with_corrupt_meta_does_not_double_subtract() {
    // R2-H1: corrupt rows evicted during post-reconcile auto-flush must not be overwritten or
    // double-subtracted by `ct.current_logical_bytes = total_bytes`.
    let tmp = TempTestDir::new("r2_h1_reconcile_corrupt_evict_bytes");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut cfg = EngineConfig::default();
    cfg.memtable_capacity_bytes = 1;
    let mut engine = StorageEngine::open(fs, cfg).unwrap();
    engine.set_table_corrupt_policy(1, CorruptPolicy::Evict);

    engine
        .put_row(1, b"c_bad", b"vvvvvv", &[], WriteDurability::BufferedFast)
        .unwrap();
    engine
        .put_row(1, b"c_ok", b"good_val", &[], WriteDurability::BufferedFast)
        .unwrap();
    // Corrupt c_bad's row metadata directly.
    let bad_meta_key = crate::lru::encode_row_meta_key(1, b"c_bad");
    engine
        .commit_wal_and_memtable(
            vec![crate::wal::WalRecord::PutInline {
                key: bad_meta_key,
                value: vec![0xFF, 0xFE, 0xFD],
            }],
            WriteDurability::BufferedFast,
        )
        .unwrap();
    assert!(engine.memtable.is_empty());

    engine.register_capped_table(1, 100_000).unwrap();
    assert!(engine.get_row(1, b"c_bad").unwrap().is_none());
    let expected_ok_bytes = (b"c_ok".len() + b"good_val".len()) as u64;
    assert_eq!(
        engine.capped_table_state(1).unwrap().current_logical_bytes,
        expected_ok_bytes
    );
}

#[test]
fn evict_until_within_quota_removes_orphan_lru_key_without_lazy_promotion() {
    // R2-L6: orphan `'l'` entry with an in-memory hot tick must be deleted, not promoted.
    let tmp = TempTestDir::new("r2_l6_orphan_lru_no_promote");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut engine = StorageEngine::open(fs, EngineConfig::default()).unwrap();
    engine.register_capped_table(1, 100_000).unwrap();
    engine
        .put_row(1, b"live", b"payload", &[], WriteDurability::BufferedFast)
        .unwrap();

    let orphan_tick = 1u64;
    let orphan_key = crate::lru::encode_lru_queue_key(1, orphan_tick, b"ghost");
    engine
        .commit_wal_and_memtable(
            vec![crate::wal::WalRecord::PutInline {
                key: orphan_key.clone(),
                value: Vec::new(),
            }],
            WriteDurability::BufferedFast,
        )
        .unwrap();
    engine.access_tracker.record_read_hit(1, b"ghost");
    assert!(
        engine
            .access_tracker
            .peek_hot_tick(1, b"ghost")
            .is_some_and(|t| t > orphan_tick)
    );

    // Trigger quota eviction: `"ghost"`'s `'l'` key sits at tick 1 (coldest), must be cleaned up
    // without lazy promotion (`total_lazy_promotions == 0`).
    engine.register_capped_table(1, 1).unwrap();
    let st = engine.capped_table_state(1).unwrap();
    assert_eq!(st.total_lazy_promotions, 0);
    assert_eq!(st.current_logical_bytes, 0);
    assert!(engine.access_tracker.peek_hot_tick(1, b"ghost").is_none());
    let lru_prefix = crate::lru::encode_namespace_prefix(1, crate::lru::NS_LRU_QUEUE);
    assert!(engine.scan_merged_prefix(&lru_prefix).unwrap().is_empty());
}

#[test]
fn register_capped_table_mid_reconcile_ring_full_persists_exact_logical_bytes() {
    // R3-H6: a RingFull flush mid-reconcile must not persist `current_logical_bytes = 0` and
    // cause `register_capped_table()` to skip syncing buffered repairs and committing real totals.
    let fs = Arc::new(PowerLossFs::new());
    let mut cfg = EngineConfig::default();
    cfg.buf_wal_size_bytes = 4096; // 1 sector: forces RingFull flushes during reconciliation.
    cfg.sync_wal_size_bytes = 4096;
    cfg.memtable_capacity_bytes = 64 << 20;
    cfg.auto_compact_segment_threshold = 0;

    let mut engine = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
    let val = vec![b'v'; 50];
    let mut expected_bytes = 0u64;
    for i in 0..40u32 {
        let pk = format!("capped_init_{i:03}_{}", "p".repeat(80)).into_bytes();
        expected_bytes += (pk.len() + val.len()) as u64;
        engine
            .put_row(1, &pk, &val, &[], WriteDurability::BufferedFast)
            .unwrap();
    }

    engine.register_capped_table(1, 1_000_000).unwrap();
    assert_eq!(
        engine.capped_table_state(1).unwrap().current_logical_bytes,
        expected_bytes
    );

    // Simulate power loss immediately after `register_capped_table` returns.
    std::mem::forget(engine);
    fs.power_loss();

    let reopened = StorageEngine::open(Arc::clone(&fs), cfg).unwrap();
    assert_eq!(
        reopened
            .capped_table_state(1)
            .unwrap()
            .current_logical_bytes,
        expected_bytes
    );
    let lru_prefix = crate::lru::encode_namespace_prefix(1, crate::lru::NS_LRU_QUEUE);
    assert_eq!(reopened.scan_merged_prefix(&lru_prefix).unwrap().len(), 40);
}
