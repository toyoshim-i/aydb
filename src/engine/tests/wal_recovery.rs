use super::*;

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
    engine1.storage.debug_discard_buf_wal_user_space();
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
        engine.storage.debug_append_sync_batch(&chunk1);
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
fn wal_write_failure_poisons_engine_until_reopen() {
    use std::sync::atomic::AtomicBool;

    struct WalFaultHooks {
        fail_next_wal_write: Arc<AtomicBool>,
    }

    impl crate::pal::test_fs::FsHooks for WalFaultHooks {
        fn write_all_at(
            &self,
            name: &str,
            inner: &dyn PlatformFile,
            buf: &[u8],
            offset: u64,
        ) -> io::Result<()> {
            if (name == "wal_sync.db" || name == "wal_buf.db")
                && self.fail_next_wal_write.swap(false, Ordering::SeqCst)
            {
                return Err(io::Error::other("simulated WAL write failure"));
            }
            inner.write_all_at(buf, offset)
        }
    }

    for durability in [
        WriteDurability::SyncTransactional,
        WriteDurability::BufferedFast,
    ] {
        let tmp = TempTestDir::new("wal_poison");
        let flag = Arc::new(AtomicBool::new(false));
        let fs = Arc::new(crate::pal::test_fs::HookedFs::new(
            OsFs::open(&tmp.path, true).unwrap(),
            WalFaultHooks {
                fail_next_wal_write: Arc::clone(&flag),
            },
        ));

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
        let before_sync = engine.buf_wal().current_sector_idx();
        engine
            .put_row(1, b"k2", b"v2", &[], WriteDurability::SyncTransactional)
            .unwrap();
        // The sector holding k1 was synced and sealed: the cursor moved past it.
        assert!(engine.buf_wal().current_sector_idx() > before_sync);
        let after_sync = engine.buf_wal().current_sector_idx();
        engine
            .put_row(1, b"k3", b"v3", &[], WriteDurability::BufferedFast)
            .unwrap();
        assert!(engine.buf_wal().current_sector_idx() >= after_sync);
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
    assert!(e.tx_state.start_meta().is_some());
    e.rollback_transaction().unwrap();
    assert!(e.tx_state.start_meta().is_none());
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
fn multi_chunk_sync_transaction_uses_single_wal_barrier_and_recovers_cleanly() {
    let tmp = TempTestDir::new("ph5_multi_chunk_single_barrier");
    let (fs, _, barrier_counts) = open_counted(&tmp.path);

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
fn open_inner_wal_replay_skips_idx_reads_for_non_blob_namespaces() {
    use crate::pal::test_fs::{FsHooks, HookedFs};
    use std::sync::atomic::AtomicUsize;

    struct IdxBlockSpyHooks {
        block_reads: Arc<AtomicUsize>,
    }

    impl FsHooks for IdxBlockSpyHooks {
        fn read_exact_at(
            &self,
            name: &str,
            inner: &dyn PlatformFile,
            buf: &mut [u8],
            offset: u64,
        ) -> io::Result<()> {
            if name.starts_with("idx_") && buf.len() == crate::index::IDX_BLOCK_SIZE {
                self.block_reads.fetch_add(1, Ordering::Relaxed);
            }
            inner.read_exact_at(buf, offset)
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

    let spy_fs = Arc::new(HookedFs::new(
        OsFs::open(&tmp.path, true).unwrap(),
        IdxBlockSpyHooks {
            block_reads: Arc::clone(&reads),
        },
    ));
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
fn flushing_wal_phase_keeps_start_meta_settled_during_ring_full_rotation() {
    // 1. RingFull WAL rotation inside `TxPhase::FlushingWal` keeps `start_meta` authoritative so
    //    the checkpoint does not persist uncommitted `dead_bytes`, then finishes cleanly in `Idle`.
    let durability = WriteDurability::BufferedFast;
    let (fs, mut e) = dead_blob_setup(durability, false, true);
    let snap_dead: u64 = {
        e.begin_transaction();
        e.put_row(1, b"r1", b"new-inline", &[], durability).unwrap();
        for i in 0..12 {
            let k = format!("tx{i:04}");
            e.put_row(3, k.as_bytes(), &[9u8; 900], &[], durability)
                .unwrap();
        }
        assert_eq!(e.tx_state.phase(), Some(crate::engine::tx::TxPhase::Active));
        e.tx_state
            .start_meta()
            .expect("start_meta present")
            .std_blobs
            .values()
            .map(|&(_, dead)| dead)
            .sum()
    };
    assert_eq!(snap_dead, 0);

    let epoch_before = wal_epoch(&e, durability);
    e.commit_transaction().unwrap();
    assert!(wal_epoch(&e, durability) > epoch_before);
    assert!(!e.tx_state.is_in_tx());
    assert!(e.tx_state.start_meta().is_none());
    assert_eq!(persisted_dead_bytes(&e), 0);

    fs.power_loss();
    std::mem::forget(e);
    let mut reopened = StorageEngine::open(fs, ring_full_config()).unwrap();
    assert_eq!(reopened.get_row(1, b"r1").unwrap(), Some(OLD_BLOB.to_vec()));
    drop(reopened);

    // 2. A WAL write failure during `TxPhase::FlushingWal` cleanly rolls back to `TxState::Idle`,
    //    restoring `start_meta` (`next_lsn` and `dead_bytes`) without SyncInFlight false poison.
    let tmp = TempTestDir::new("m1_flushing_wal_fail_rollback");
    let os_fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut cfg = EngineConfig::default();
    cfg.inline_threshold_bytes = 64;
    let mut eng = StorageEngine::open(os_fs, cfg).unwrap();
    eng.put_row(1, b"base", &OLD_BLOB, &[], WriteDurability::BufferedFast)
        .unwrap();
    let base_lsn = eng.next_lsn();
    let base_dead: u64 = eng.std_blobs.values().map(|b| b.dead_bytes).sum();

    eng.begin_transaction_for_conn(1).unwrap();
    eng.put_row(
        1,
        b"base",
        b"overwritten",
        &[],
        WriteDurability::BufferedFast,
    )
    .unwrap();
    eng.debug_fail_next_wal_append();
    let err = eng.commit_transaction_for_conn(1).unwrap_err();
    assert!(err.to_string().contains("injected WAL append failure"));
    assert!(eng.is_poisoned());
    assert!(!eng.tx_state.is_in_tx());
    assert!(eng.tx_state.start_meta().is_none());
    assert_eq!(eng.next_lsn(), base_lsn);
    let restored_dead: u64 = eng.std_blobs.values().map(|b| b.dead_bytes).sum();
    assert_eq!(restored_dead, base_dead);
    assert_eq!(eng.get_row(1, b"base").unwrap(), Some(OLD_BLOB.to_vec()));
}
