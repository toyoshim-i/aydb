use super::*;
use crate::pal::OsFs;
use crate::util::test_util::TempTestDir;

#[test]
fn reserve_seq_ids_skips_every_existing_managed_file_name() {
    let tmp = TempTestDir::new("reserve_seq");
    let fs = Arc::new(OsFs::open(&tmp.path, true).expect("open OsFs"));
    let mut mgr = BootMetaManager::open_or_recover(Arc::clone(&fs)).expect("open fresh");
    assert_eq!(mgr.state().next_seq_id, 1);

    for (name, seq) in [
        (format_idx_file_name(4), 4),
        (format_std_blob_file_name(9), 9),
        (format_lru_blob_file_name(3, 17), 17),
        (format_manifest_file_name(12), 12),
    ] {
        std::fs::write(tmp.path.join(&name), b"orphan").unwrap();
        mgr.reserve_seq_ids_above_existing_files().unwrap();
        assert!(mgr.state().next_seq_id > seq, "{name}");
    }
    assert_eq!(mgr.state().next_seq_id, 18);

    // The reservation is durable and idempotent.
    let generation = mgr.state().generation;
    mgr.reserve_seq_ids_above_existing_files().unwrap();
    assert_eq!(mgr.state().generation, generation);
    drop(mgr);
    let reopened = BootMetaManager::open_or_recover_deferred_sweep(fs).unwrap();
    assert_eq!(reopened.state().next_seq_id, 18);
}

#[test]
fn fresh_init_and_ping_pong_commits() {
    let tmp = TempTestDir::new("ping_pong");
    let fs = Arc::new(OsFs::open(&tmp.path, true).expect("open OsFs"));

    {
        let mut mgr = BootMetaManager::open_or_recover(Arc::clone(&fs)).expect("open fresh");
        assert_eq!(mgr.active_slot(), SlotIndex::SlotA);
        assert_eq!(mgr.state().generation, 1);

        // Commit Gen 2 -> lands in Slot B
        mgr.commit_update(|s| {
            let id = s.allocate_seq_id();
            s.idx_segments.push(IdxSegmentMeta {
                seq_id: id,
                file_len: 16384,
                level: 0,
                record_count: 42,
            });
            s.checkpoint_lsn = 100;
        })
        .expect("commit gen 2");
        assert_eq!(mgr.active_slot(), SlotIndex::SlotB);
        assert_eq!(mgr.state().generation, 2);

        // Create the referenced idx file so orphan sweep on reopen sees it as live
        let idx_name = format_idx_file_name(1);
        let _f = fs
            .open_file(&idx_name, true, true, OpenHint::ImmutableRandom)
            .expect("create idx_1");

        // Commit Gen 3 -> lands in Slot A
        mgr.commit_update(|s| {
            s.table_quotas.push(TableQuotaMeta {
                table_id: 7,
                eviction_policy: EvictionPolicy::Lru,
                max_bytes: 100 * 1024 * 1024,
                current_logical_bytes: 4096,
            });
            s.checkpoint_lsn = 250;
        })
        .expect("commit gen 3");
        assert_eq!(mgr.active_slot(), SlotIndex::SlotA);
        assert_eq!(mgr.state().generation, 3);
    }

    // Reopen and verify Generation 3 is selected from Slot A
    let mgr = BootMetaManager::open_or_recover(Arc::clone(&fs)).expect("reopen");
    assert_eq!(mgr.active_slot(), SlotIndex::SlotA);
    assert_eq!(mgr.state().generation, 3);
    assert_eq!(mgr.state().checkpoint_lsn, 250);
    assert_eq!(mgr.state().idx_segments.len(), 1);
    assert_eq!(mgr.state().table_quotas.len(), 1);
    assert_eq!(mgr.state().table_quotas[0].table_id, 7);
}

#[test]
fn subsector_torn_write_and_dual_corruption_detection() {
    let tmp = TempTestDir::new("torn_write");
    let fs = Arc::new(OsFs::open(&tmp.path, true).expect("open OsFs"));

    {
        let mut mgr = BootMetaManager::open_or_recover(Arc::clone(&fs)).expect("open fresh");
        // Gen 2 in Slot B
        mgr.commit_update(|s| {
            s.checkpoint_lsn = 500;
        })
        .expect("commit gen 2");
        // Gen 3 in Slot A
        mgr.commit_update(|s| {
            s.checkpoint_lsn = 900;
        })
        .expect("commit gen 3");
        assert_eq!(mgr.active_slot(), SlotIndex::SlotA);
    }

    // Simulate a 512-byte sub-sector torn write at the very end of Slot A (byte 4095,
    // corrupting SlotTrailerCrc32c while leaving HeaderCrc32c intact!).
    {
        let f = fs
            .open_file(BOOT_META_FILE_NAME, false, true, OpenHint::MutableSyncLog)
            .expect("open boot_meta raw");
        f.write_all_at(&[0xFF], 4095).expect("corrupt trailer byte");
    }

    // Recovery must detect the torn trailer on Slot A (Gen 3) and fall back cleanly to Slot B (Gen 2)!
    {
        let mgr = BootMetaManager::open_or_recover(Arc::clone(&fs))
            .expect("recover from Slot B after Slot A torn write");
        assert_eq!(mgr.active_slot(), SlotIndex::SlotB);
        assert_eq!(mgr.state().generation, 2);
        assert_eq!(mgr.state().checkpoint_lsn, 500);
    }

    // Now corrupt Slot B as well; recovery must refuse to open and return InvalidData!
    {
        let f = fs
            .open_file(BOOT_META_FILE_NAME, false, true, OpenHint::MutableSyncLog)
            .expect("open boot_meta raw");
        f.write_all_at(&[0xEE], 4096 + 10)
            .expect("corrupt Slot B header");
    }

    let res = BootMetaManager::open_or_recover(Arc::clone(&fs));
    assert!(res.is_err());
    assert_eq!(res.err().unwrap().kind(), io::ErrorKind::InvalidData);
}

#[test]
fn external_manifest_overflow_and_gc() {
    let tmp = TempTestDir::new("ext_manifest");
    let fs = Arc::new(OsFs::open(&tmp.path, true).expect("open OsFs"));

    let first_mf_id;
    let second_mf_id;
    {
        let mut mgr = BootMetaManager::open_or_recover(Arc::clone(&fs)).expect("open fresh");

        // Add 200 index segments (200 * 24B = 4800B > 4028B inline limit)
        mgr.commit_update(|s| {
            for i in 0..200 {
                let seq_id = s.allocate_seq_id();
                let fname = format_idx_file_name(seq_id);
                let _ = fs
                    .open_file(&fname, true, true, OpenHint::ImmutableRandom)
                    .unwrap();
                s.idx_segments.push(IdxSegmentMeta {
                    seq_id,
                    file_len: 4096 * (i + 1),
                    level: 1,
                    record_count: 100,
                });
            }
        })
        .expect("commit overflow gen 2");

        first_mf_id = mgr
            .state()
            .active_manifest_seq_id
            .expect("must overflow to external manifest");

        // Commit another overflow update; the first manifest must be garbage collected
        mgr.commit_update(|s| {
            s.checkpoint_lsn = 7777;
        })
        .expect("commit overflow gen 3");

        second_mf_id = mgr
            .state()
            .active_manifest_seq_id
            .expect("must overflow to second external manifest");
        assert_ne!(first_mf_id, second_mf_id);

        // The first manifest is still referenced by the standby slot and must be retained.
        let files = fs.list_files().unwrap();
        assert!(files.contains(&format_manifest_file_name(first_mf_id)));
        assert!(files.contains(&format_manifest_file_name(second_mf_id)));

        // Once a third generation is committed the first manifest is referenced by neither slot.
        mgr.commit_update(|s| {
            s.checkpoint_lsn = 8888;
        })
        .expect("commit overflow gen 4");
        let files = fs.list_files().unwrap();
        assert!(!files.contains(&format_manifest_file_name(first_mf_id)));
        assert!(files.contains(&format_manifest_file_name(second_mf_id)));
    }

    // Reopen and verify all 200 segments are recovered from the external manifest
    let mgr = BootMetaManager::open_or_recover(Arc::clone(&fs)).expect("reopen overflow");
    assert_eq!(mgr.state().generation, 4);
    assert_eq!(mgr.state().checkpoint_lsn, 8888);
    assert_eq!(mgr.state().idx_segments.len(), 200);
    assert!(mgr.state().active_manifest_seq_id.is_some());
}

#[test]
fn standby_manifest_survives_commits_and_restart_sweep_and_serves_fallback() {
    let tmp = TempTestDir::new("standby_manifest");
    let fs = Arc::new(OsFs::open(&tmp.path, true).expect("open OsFs"));
    let manifests = |fs: &Arc<OsFs>| {
        let mut v: Vec<String> = fs
            .list_files()
            .unwrap()
            .into_iter()
            .filter(|f| f.starts_with("manifest_"))
            .collect();
        v.sort();
        v
    };

    let (active_mf, standby_mf, active_slot);
    {
        let mut mgr = BootMetaManager::open_or_recover(Arc::clone(&fs)).expect("open fresh");
        mgr.commit_update(|s| {
            for i in 0..200 {
                let seq_id = s.allocate_seq_id();
                let fname = format_idx_file_name(seq_id);
                let _ = fs
                    .open_file(&fname, true, true, OpenHint::ImmutableRandom)
                    .unwrap();
                s.idx_segments.push(IdxSegmentMeta {
                    seq_id,
                    file_len: 4096 * (i + 1),
                    level: 1,
                    record_count: 100,
                });
            }
        })
        .unwrap();
        for lsn in 1..=4u64 {
            let prev = mgr.state().active_manifest_seq_id.unwrap();
            mgr.commit_update(|s| s.checkpoint_lsn = lsn).unwrap();
            let cur = mgr.state().active_manifest_seq_id.unwrap();
            assert_ne!(prev, cur);
            // Exactly the active and the standby manifests exist after every commit.
            assert_eq!(manifests(&fs), {
                let mut v = vec![
                    format_manifest_file_name(prev),
                    format_manifest_file_name(cur),
                ];
                v.sort();
                v
            });
        }
        active_mf = mgr.state().active_manifest_seq_id.unwrap();
        standby_mf = mgr.standby_manifest_id.unwrap();
        active_slot = mgr.active_slot();
    }

    // A restart sweep keeps both manifests.
    drop(BootMetaManager::open_or_recover(Arc::clone(&fs)).expect("reopen with sweep"));
    assert!(manifests(&fs).contains(&format_manifest_file_name(active_mf)));
    assert!(manifests(&fs).contains(&format_manifest_file_name(standby_mf)));

    // Corrupt the active slot; the open must fall back to the standby generation.
    let f = fs
        .open_file(BOOT_META_FILE_NAME, false, true, OpenHint::MutableSyncLog)
        .unwrap();
    let off = match active_slot {
        SlotIndex::SlotA => 0,
        SlotIndex::SlotB => SLOT_SIZE as u64,
    };
    f.write_all_at(&[0xFF], off + SLOT_SIZE as u64 - 1).unwrap();
    drop(f);

    let mgr = BootMetaManager::open_or_recover(Arc::clone(&fs)).expect("fallback open");
    assert_eq!(mgr.state().active_manifest_seq_id, Some(standby_mf));
    assert_eq!(mgr.state().idx_segments.len(), 200);
    assert_eq!(mgr.state().checkpoint_lsn, 3);
}

/// Test-only `PlatformFs` that fails manifest opens with a non-corruption error on demand.
struct FlakyManifestFs {
    inner: OsFs,
    fail_manifest_open: std::sync::atomic::AtomicBool,
    fail_target: Option<String>,
    blocked_open_attempts: std::sync::atomic::AtomicUsize,
}

impl PlatformFs for FlakyManifestFs {
    type File = <OsFs as PlatformFs>::File;

    fn open_file(
        &self,
        rel_name: &str,
        create: bool,
        writable: bool,
        hint: OpenHint,
    ) -> io::Result<Arc<Self::File>> {
        let matches_target = match &self.fail_target {
            Some(name) => rel_name == name,
            None => rel_name.starts_with("manifest_"),
        };
        if matches_target
            && !create
            && self
                .fail_manifest_open
                .load(std::sync::atomic::Ordering::SeqCst)
        {
            self.blocked_open_attempts
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            return Err(io::Error::from(io::ErrorKind::PermissionDenied));
        }
        self.inner.open_file(rel_name, create, writable, hint)
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
fn transient_manifest_io_error_does_not_roll_back_or_sweep() {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    let tmp = TempTestDir::new("flaky_manifest");
    let fs = Arc::new(FlakyManifestFs {
        inner: OsFs::open(&tmp.path, true).expect("open OsFs"),
        fail_manifest_open: AtomicBool::new(false),
        fail_target: None,
        blocked_open_attempts: AtomicUsize::new(0),
    });

    {
        let mut mgr = BootMetaManager::open_or_recover(Arc::clone(&fs)).expect("open fresh");
        mgr.commit_update(|s| {
            for i in 0..200 {
                let seq_id = s.allocate_seq_id();
                let fname = format_idx_file_name(seq_id);
                let _ = fs
                    .open_file(&fname, true, true, OpenHint::ImmutableRandom)
                    .unwrap();
                s.idx_segments.push(IdxSegmentMeta {
                    seq_id,
                    file_len: 4096 * (i + 1),
                    level: 1,
                    record_count: 100,
                });
            }
        })
        .expect("commit gen 2");
        mgr.commit_update(|s| s.checkpoint_lsn = 4242)
            .expect("commit gen 3");
    }

    let files_before = fs.list_files().unwrap();

    fs.fail_manifest_open.store(true, Ordering::SeqCst);
    let res = BootMetaManager::open_or_recover(Arc::clone(&fs));
    assert_eq!(
        res.err().expect("transient error must fail open").kind(),
        io::ErrorKind::PermissionDenied
    );
    let res = BootMetaManager::open_or_recover_deferred_sweep(Arc::clone(&fs));
    assert_eq!(
        res.err().expect("transient error must fail open").kind(),
        io::ErrorKind::PermissionDenied
    );
    assert_eq!(
        fs.list_files().unwrap(),
        files_before,
        "nothing may be swept"
    );

    fs.fail_manifest_open.store(false, Ordering::SeqCst);
    let mgr = BootMetaManager::open_or_recover(Arc::clone(&fs)).expect("reopen after clear");
    assert_eq!(mgr.state().generation, 3);
    assert_eq!(mgr.state().checkpoint_lsn, 4242);
    assert_eq!(mgr.state().idx_segments.len(), 200);
}

#[test]
fn healthy_active_manifest_opens_without_reading_corrupt_or_failing_standby_manifest() {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    let tmp = TempTestDir::new("lazy_standby_mf");
    let standby_mf_id;
    let active_mf_id;
    {
        let fs = Arc::new(OsFs::open(&tmp.path, true).expect("open OsFs"));
        let mut mgr = BootMetaManager::open_or_recover(Arc::clone(&fs)).expect("open fresh");
        mgr.commit_update(|s| {
            for i in 0..200 {
                let seq_id = s.allocate_seq_id();
                let fname = format_idx_file_name(seq_id);
                let _ = fs
                    .open_file(&fname, true, true, OpenHint::ImmutableRandom)
                    .unwrap();
                s.idx_segments.push(IdxSegmentMeta {
                    seq_id,
                    file_len: 4096 * (i + 1),
                    level: 1,
                    record_count: 100,
                });
            }
            s.checkpoint_lsn = 111;
        })
        .expect("commit gen 2");
        standby_mf_id = mgr.state().active_manifest_seq_id.expect("gen 2 manifest");

        mgr.commit_update(|s| {
            s.checkpoint_lsn = 222;
        })
        .expect("commit gen 3");
        active_mf_id = mgr.state().active_manifest_seq_id.expect("gen 3 manifest");
        assert_ne!(standby_mf_id, active_mf_id);
    }

    // Corrupt the standby manifest on disk AND block any attempt to open it.
    let standby_name = format_manifest_file_name(standby_mf_id);
    std::fs::write(tmp.path.join(&standby_name), b"corrupt standby manifest").unwrap();
    let fs = Arc::new(FlakyManifestFs {
        inner: OsFs::open(&tmp.path, true).expect("open OsFs"),
        fail_manifest_open: AtomicBool::new(true),
        fail_target: Some(standby_name.clone()),
        blocked_open_attempts: AtomicUsize::new(0),
    });

    let mut mgr = BootMetaManager::open_or_recover(Arc::clone(&fs))
        .expect("healthy active manifest must open without touching standby manifest");
    assert_eq!(fs.blocked_open_attempts.load(Ordering::SeqCst), 0);
    assert_eq!(mgr.state().generation, 3);
    assert_eq!(mgr.state().checkpoint_lsn, 222);
    assert_eq!(mgr.state().active_manifest_seq_id, Some(active_mf_id));
    // Standby manifest must still be retained until the standby slot is overwritten.
    assert!(fs.list_files().unwrap().contains(&standby_name));

    // Advancing one more generation overwrites the old standby slot and sweeps standby_name.
    mgr.commit_update(|s| {
        s.checkpoint_lsn = 333;
    })
    .expect("commit gen 4");
    assert!(!fs.list_files().unwrap().contains(&standby_name));
}

#[test]
fn deferred_gc_on_sharing_violation_and_orphan_sweep() {
    let tmp = TempTestDir::new("deferred_gc");
    let fs = Arc::new(OsFs::open(&tmp.path, true).expect("open OsFs"));

    let locked_idx_name = format_idx_file_name(10);
    let orphan_idx_name = format_idx_file_name(99);

    {
        let mut mgr = BootMetaManager::open_or_recover(Arc::clone(&fs)).expect("open");
        // Commit Gen 2 referencing `locked_idx_name`
        mgr.commit_update(|s| {
            s.idx_segments.push(IdxSegmentMeta {
                seq_id: 10,
                file_len: 4096,
                level: 0,
                record_count: 1,
            });
        })
        .expect("commit gen 2");
    }

    // Hold an open handle to `idx_000000000000000a.db` to simulate an antivirus filter
    // blocking DeleteFile while the segment is retired.
    let held_handle = fs
        .open_file(&locked_idx_name, true, true, OpenHint::ImmutableRandom)
        .expect("create and hold idx 10");

    // Also create an unreferenced orphan segment file (simulating a crash mid-flush)
    let _orphan = fs
        .open_file(&orphan_idx_name, true, true, OpenHint::ImmutableRandom)
        .expect("create orphan idx 99");
    drop(_orphan);

    {
        let mut mgr =
            BootMetaManager::open_or_recover(Arc::clone(&fs)).expect("reopen with orphan");
        // Orphan `idx_99` should have been swept immediately on startup!
        assert!(!fs.list_files().unwrap().contains(&orphan_idx_name));

        // Retire `locked_idx_name` and queue it for deletion while `held_handle` is open
        mgr.commit_update(|s| {
            s.idx_segments.clear();
            s.queue_pending_delete(locked_idx_name.clone());
        })
        .expect("retire idx 10");

        // On Windows, `locked_idx_name` could not be deleted because `held_handle` is open;
        // verify it remains queued in `pending_delete_files` and persisted on the next commit.
        #[cfg(windows)]
        {
            assert!(mgr.state().pending_delete_files.contains(&locked_idx_name));
            mgr.commit_update(|s| {
                s.checkpoint_lsn = 42;
            })
            .expect("commit with pending delete");
            assert!(mgr.state().pending_delete_files.contains(&locked_idx_name));
        }
    }

    // Now drop the held handle (antivirus scan finished) and reopen `BootMetaManager`;
    // startup GC sweep must delete `locked_idx_name` cleanly!
    drop(held_handle);

    let mgr = BootMetaManager::open_or_recover(Arc::clone(&fs)).expect("reopen after unlock");
    assert!(mgr.state().pending_delete_files.is_empty());
    assert!(!fs.list_files().unwrap().contains(&locked_idx_name));
}

#[test]
fn missing_or_zeroed_boot_meta_refuses_to_destroy_existing_segments() {
    let tmp = TempTestDir::new("missing_boot_meta");
    let fs = Arc::new(OsFs::open(&tmp.path, true).expect("open OsFs"));

    let idx_name = format_idx_file_name(1);
    {
        let mut mgr = BootMetaManager::open_or_recover(Arc::clone(&fs)).expect("open fresh");
        mgr.commit_update(|s| {
            let id = s.allocate_seq_id();
            s.idx_segments.push(IdxSegmentMeta {
                seq_id: id,
                file_len: 4096,
                level: 0,
                record_count: 10,
            });
        })
        .expect("commit idx_1");
        let _f = fs
            .open_file(&idx_name, true, true, OpenHint::ImmutableRandom)
            .expect("create idx_1");
    }

    // Delete `boot_meta.db` while `idx_0000000000000001.db` exists on disk
    fs.remove_file(BOOT_META_FILE_NAME)
        .expect("remove boot_meta.db");

    let err = match BootMetaManager::open_or_recover(Arc::clone(&fs)) {
        Ok(_) => panic!("open_or_recover must fail when boot_meta.db is missing"),
        Err(e) => e,
    };
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    assert!(
        fs.list_files().unwrap().contains(&idx_name),
        "existing segment file must NOT be deleted when boot_meta.db is missing or zeroed"
    );
}

#[test]
fn corrupted_catalog_counts_with_valid_crc_rejected() {
    let tmp = TempTestDir::new("corrupted_catalog_counts");
    let fs = Arc::new(OsFs::open(&tmp.path, true).expect("open OsFs"));
    {
        let _mgr = BootMetaManager::open_or_recover(Arc::clone(&fs)).expect("open fresh");
    }

    // Craft a slot payload claiming `num_deletes = u32::MAX` while recomputing a valid slot CRC32C
    let file = fs
        .open_file(BOOT_META_FILE_NAME, false, true, OpenHint::ImmutableRandom)
        .unwrap();
    let mut buf = vec![0u8; BOOT_META_FILE_SIZE as usize];
    file.read_exact_at(&mut buf, 0).unwrap();

    // Slot A starts at 0; payload starts at 64 (`num_idx=0`, `num_std=0`, `num_lru=0`, `num_quotas=0`, `num_deletes=u32::MAX`)
    buf[64..68].copy_from_slice(&0u32.to_le_bytes());
    buf[68..72].copy_from_slice(&0u32.to_le_bytes());
    buf[72..76].copy_from_slice(&0u32.to_le_bytes());
    buf[76..80].copy_from_slice(&0u32.to_le_bytes());
    buf[80..84].copy_from_slice(&u32::MAX.to_le_bytes());
    // Bump Slot A generation to 99 so it wins over Slot B, and recompute Slot A CRC32C
    buf[8..16].copy_from_slice(&99u64.to_le_bytes());
    let crc_a = crc32c(&buf[0..4092]);
    buf[4092..4096].copy_from_slice(&crc_a.to_le_bytes());
    // Corrupt Slot B so Slot A is the sole valid-CRC candidate
    buf[4096..4100].copy_from_slice(b"XXXX");
    file.write_all_at(&buf, 0).unwrap();
    file.sync_durable().unwrap();
    drop(file);

    let err = match BootMetaManager::open_or_recover(Arc::clone(&fs)) {
        Ok(_) => panic!("crafted huge num_deletes with valid CRC must be rejected"),
        Err(e) => e,
    };
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
}

/// Event recorded by [`LoggingFs`] for `boot_meta.db`.
#[derive(Debug, Clone, PartialEq, Eq)]
enum BootMetaEvent {
    Write(u64),
    Preallocate(u64),
    Sync,
}

type EventLog = Arc<std::sync::Mutex<Vec<BootMetaEvent>>>;

/// Test-only `PlatformFs` that records the writes and syncs issued on `boot_meta.db`.
struct LoggingFs {
    inner: OsFs,
    log: EventLog,
}

struct LoggingFile {
    inner: Arc<<OsFs as PlatformFs>::File>,
    log: Option<EventLog>,
}

impl LoggingFile {
    fn record(&self, event: BootMetaEvent) {
        if let Some(log) = &self.log {
            log.lock().unwrap().push(event);
        }
    }
}

impl PlatformFile for LoggingFile {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        self.inner.read_at(buf, offset)
    }
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        self.inner.read_exact_at(buf, offset)
    }
    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        self.record(BootMetaEvent::Write(offset));
        self.inner.write_all_at(buf, offset)
    }
    fn file_len(&self) -> io::Result<u64> {
        self.inner.file_len()
    }
    fn preallocate(&self, target_len: u64, strategy: PreallocStrategy) -> io::Result<()> {
        self.record(BootMetaEvent::Preallocate(target_len));
        self.inner.preallocate(target_len, strategy)
    }
    fn sync_barrier(&self) -> io::Result<()> {
        self.inner.sync_barrier()
    }
    fn sync_durable(&self) -> io::Result<()> {
        self.record(BootMetaEvent::Sync);
        self.inner.sync_durable()
    }
    fn evict_os_cache(&self, offset: u64, len: u64) -> io::Result<()> {
        self.inner.evict_os_cache(offset, len)
    }
}

impl PlatformFs for LoggingFs {
    type File = LoggingFile;

    fn open_file(
        &self,
        rel_name: &str,
        create: bool,
        writable: bool,
        hint: OpenHint,
    ) -> io::Result<Arc<Self::File>> {
        Ok(Arc::new(LoggingFile {
            inner: self.inner.open_file(rel_name, create, writable, hint)?,
            log: (rel_name == BOOT_META_FILE_NAME).then(|| Arc::clone(&self.log)),
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
fn first_slot_write_after_open_is_preceded_by_a_sync_of_boot_meta() {
    let tmp = TempTestDir::new("sync_before_first_slot_write");
    {
        // Two commits make both slots valid, so the next open picks a winner.
        let fs = Arc::new(OsFs::open(&tmp.path, true).expect("open OsFs"));
        let mut mgr = BootMetaManager::open_or_recover(fs).unwrap();
        mgr.commit_update(|st| st.checkpoint_lsn = 1).unwrap();
        mgr.commit_update(|st| st.checkpoint_lsn = 2).unwrap();
    }
    let log: EventLog = Arc::default();
    let fs = Arc::new(LoggingFs {
        inner: OsFs::open(&tmp.path, true).expect("reopen OsFs"),
        log: Arc::clone(&log),
    });
    let mut mgr = BootMetaManager::open_or_recover(fs).unwrap();
    mgr.commit_update(|st| st.checkpoint_lsn = 3).unwrap();
    mgr.commit_update(|st| st.checkpoint_lsn = 4).unwrap();

    // The winning slot may exist only in the OS cache after a process crash. It must be
    // durable before the commit overwrites the opposite slot, the only known-durable one.
    let events = log.lock().unwrap().clone();
    let first_write = events
        .iter()
        .position(|e| matches!(e, BootMetaEvent::Write(_)))
        .expect("a slot write");
    assert!(
        events[..first_write].contains(&BootMetaEvent::Sync),
        "no boot_meta.db sync before the first slot write: {events:?}"
    );
    // Only the first commit pays for it: each commit adds exactly write + sync.
    let syncs = events.iter().filter(|e| **e == BootMetaEvent::Sync).count();
    assert_eq!(syncs, 3, "{events:?}");
}

/// Flag bits other than the external-manifest flag are refused, including `0x0002`, which an
/// earlier format used for the UNIQUE index guard.
#[test]
fn unknown_flag_bits_are_rejected() {
    for bit in [0x0002u16, 0x0004] {
        let tmp = TempTestDir::new("unknown_flag");
        let fs = Arc::new(OsFs::open(&tmp.path, true).expect("open OsFs"));
        {
            let mut mgr = BootMetaManager::open_or_recover(Arc::clone(&fs)).unwrap();
            mgr.commit_update(|s| s.checkpoint_lsn = 1).unwrap();
            mgr.commit_update(|s| s.checkpoint_lsn = 2).unwrap();
        }
        let path = tmp.path.join(BOOT_META_FILE_NAME);
        let mut bytes = std::fs::read(&path).unwrap();
        for slot in 0..2 {
            let base = slot * SLOT_SIZE;
            let flags = u16::from_le_bytes([bytes[base + 6], bytes[base + 7]]) | bit;
            bytes[base + 6..base + 8].copy_from_slice(&flags.to_le_bytes());
            let header_crc = crc32c(&bytes[base..base + 60]);
            bytes[base + 60..base + 64].copy_from_slice(&header_crc.to_le_bytes());
            let trailer = crc32c(&bytes[base..base + 4092]);
            bytes[base + 4092..base + 4096].copy_from_slice(&trailer.to_le_bytes());
        }
        std::fs::write(&path, &bytes).unwrap();
        let err = BootMetaManager::open_or_recover(fs)
            .err()
            .expect("must refuse");
        assert!(
            err.to_string().contains("unknown boot_meta flags"),
            "{bit:#x}: {err}"
        );
    }
}

#[test]
fn single_incompatible_slot_aborts_open_without_falling_back_to_older_slot() {
    for case in ["version", "flags", "reserved"] {
        let tmp = TempTestDir::new("single_incompat_slot");
        let fs = Arc::new(OsFs::open(&tmp.path, true).expect("open OsFs"));
        let idx1 = format_idx_file_name(1);
        let idx2 = format_idx_file_name(2);
        {
            let mut mgr = BootMetaManager::open_or_recover(Arc::clone(&fs)).unwrap();
            // Gen 2 in Slot B references idx_1.
            mgr.commit_update(|s| {
                let id = s.allocate_seq_id();
                s.idx_segments.push(IdxSegmentMeta {
                    seq_id: id,
                    file_len: 4096,
                    level: 0,
                    record_count: 1,
                });
            })
            .unwrap();
            let _ = fs
                .open_file(&idx1, true, true, OpenHint::ImmutableRandom)
                .unwrap();
            assert_eq!(mgr.active_slot(), SlotIndex::SlotB);

            // Gen 3 in Slot A references idx_1 and idx_2.
            mgr.commit_update(|s| {
                let id = s.allocate_seq_id();
                s.idx_segments.push(IdxSegmentMeta {
                    seq_id: id,
                    file_len: 4096,
                    level: 0,
                    record_count: 1,
                });
            })
            .unwrap();
            let _ = fs
                .open_file(&idx2, true, true, OpenHint::ImmutableRandom)
                .unwrap();
            assert_eq!(mgr.active_slot(), SlotIndex::SlotA);
        }

        // Modify ONLY Slot A (leaving Slot B intact valid v1) with recomputed slot CRCs.
        let path = tmp.path.join(BOOT_META_FILE_NAME);
        let mut bytes = std::fs::read(&path).unwrap();
        match case {
            "version" => {
                bytes[4..6].copy_from_slice(&(FORMAT_VERSION + 1).to_le_bytes());
            }
            "flags" => {
                let flags = u16::from_le_bytes([bytes[6], bytes[7]]) | 0x0004;
                bytes[6..8].copy_from_slice(&flags.to_le_bytes());
            }
            "reserved" => {
                bytes[56..60].copy_from_slice(&1u32.to_le_bytes());
            }
            _ => unreachable!(),
        }
        let header_crc = crc32c(&bytes[0..60]);
        bytes[60..64].copy_from_slice(&header_crc.to_le_bytes());
        let trailer_crc = crc32c(&bytes[0..4092]);
        bytes[4092..4096].copy_from_slice(&trailer_crc.to_le_bytes());
        std::fs::write(&path, &bytes).unwrap();

        let err = BootMetaManager::open_or_recover(Arc::clone(&fs))
            .err()
            .expect("CRC-valid incompatible Slot A must abort open");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{case}: {err}");
        assert!(
            fs.list_files().unwrap().contains(&idx2),
            "{case}: newer-generation segment {idx2} was swept on fallback"
        );
    }
}

#[test]
fn single_incompatible_external_manifest_aborts_open_without_fallback() {
    for case in ["version", "reserved"] {
        // Part 1: Unchecksummed header bit-flips must be caught as Corrupt (CRC mismatch) and
        // fall back cleanly to the standby Slot B generation.
        {
            let tmp = TempTestDir::new("bitflip_incompat_mf");
            let fs = Arc::new(OsFs::open(&tmp.path, true).expect("open OsFs"));
            let mf_seq;
            {
                let mut mgr = BootMetaManager::open_or_recover(Arc::clone(&fs)).unwrap();
                mgr.commit_update(|s| {
                    s.checkpoint_lsn = 10;
                })
                .unwrap();
                assert_eq!(mgr.active_slot(), SlotIndex::SlotB);

                mgr.commit_update(|s| {
                    for i in 0..200 {
                        let seq_id = s.allocate_seq_id();
                        let fname = format_idx_file_name(seq_id);
                        let _ = fs
                            .open_file(&fname, true, true, OpenHint::ImmutableRandom)
                            .unwrap();
                        s.idx_segments.push(IdxSegmentMeta {
                            seq_id,
                            file_len: 4096 * (i + 1),
                            level: 1,
                            record_count: 100,
                        });
                    }
                })
                .unwrap();
                assert_eq!(mgr.active_slot(), SlotIndex::SlotA);
                mf_seq = mgr
                    .state()
                    .active_manifest_seq_id
                    .expect("gen 3 must use external manifest");
            }

            let mf_path = tmp.path.join(format_manifest_file_name(mf_seq));
            let mut mf_bytes = std::fs::read(&mf_path).unwrap();
            match case {
                "version" => {
                    mf_bytes[4..6].copy_from_slice(&(FORMAT_VERSION + 1).to_le_bytes());
                }
                "reserved" => {
                    mf_bytes[6..8].copy_from_slice(&1u16.to_le_bytes());
                }
                _ => unreachable!(),
            }
            std::fs::write(&mf_path, &mf_bytes).unwrap();

            let mgr = BootMetaManager::open_or_recover(Arc::clone(&fs))
                .expect("unchecksummed manifest header corruption must fall back to standby slot");
            assert_eq!(mgr.active_slot(), SlotIndex::SlotB, "{case}");
            assert_eq!(mgr.state().checkpoint_lsn, 10, "{case}");
        }

        // Part 2: CRC-valid incompatible header fields (both manifest CRC and slot pointer CRC
        // updated) must abort open without falling back or sweeping Gen 3 files.
        let tmp = TempTestDir::new("single_incompat_mf");
        let fs = Arc::new(OsFs::open(&tmp.path, true).expect("open OsFs"));
        let extra_idx_seq;
        let mf_seq;
        {
            let mut mgr = BootMetaManager::open_or_recover(Arc::clone(&fs)).unwrap();
            // Gen 2 in Slot B is inline (empty catalog).
            mgr.commit_update(|s| {
                s.checkpoint_lsn = 10;
            })
            .unwrap();
            assert_eq!(mgr.active_slot(), SlotIndex::SlotB);

            // Gen 3 in Slot A overflows into an external manifest referencing new idx files.
            mgr.commit_update(|s| {
                for i in 0..200 {
                    let seq_id = s.allocate_seq_id();
                    let fname = format_idx_file_name(seq_id);
                    let _ = fs
                        .open_file(&fname, true, true, OpenHint::ImmutableRandom)
                        .unwrap();
                    s.idx_segments.push(IdxSegmentMeta {
                        seq_id,
                        file_len: 4096 * (i + 1),
                        level: 1,
                        record_count: 100,
                    });
                }
            })
            .unwrap();
            assert_eq!(mgr.active_slot(), SlotIndex::SlotA);
            extra_idx_seq = mgr.state().idx_segments[0].seq_id;
            mf_seq = mgr
                .state()
                .active_manifest_seq_id
                .expect("gen 3 must use external manifest");
        }

        let mf_name = format_manifest_file_name(mf_seq);
        let mf_path = tmp.path.join(&mf_name);
        let mut mf_bytes = std::fs::read(&mf_path).unwrap();
        match case {
            "version" => {
                mf_bytes[4..6].copy_from_slice(&(FORMAT_VERSION + 1).to_le_bytes());
            }
            "reserved" => {
                mf_bytes[6..8].copy_from_slice(&1u16.to_le_bytes());
            }
            _ => unreachable!(),
        }
        let updated_crc = compute_manifest_crc(&mf_bytes[0..12], &mf_bytes[MANIFEST_HEADER_LEN..]);
        mf_bytes[12..16].copy_from_slice(&updated_crc.to_le_bytes());
        std::fs::write(&mf_path, &mf_bytes).unwrap();

        let meta_path = tmp.path.join(BOOT_META_FILE_NAME);
        let mut raw = std::fs::read(&meta_path).unwrap();
        raw[80..84].copy_from_slice(&updated_crc.to_le_bytes());
        let ptr_crc = crc32c(&raw[64..64 + EXTERNAL_POINTER_LEN]);
        raw[52..56].copy_from_slice(&ptr_crc.to_le_bytes());
        let h_crc = crc32c(&raw[0..60]);
        raw[60..64].copy_from_slice(&h_crc.to_le_bytes());
        let t_crc = crc32c(&raw[0..4092]);
        raw[4092..4096].copy_from_slice(&t_crc.to_le_bytes());
        std::fs::write(&meta_path, &raw).unwrap();

        let err = BootMetaManager::open_or_recover(Arc::clone(&fs))
            .err()
            .expect("CRC-valid incompatible external manifest must abort open");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{case}: {err}");
        let files = fs.list_files().unwrap();
        assert!(files.contains(&mf_name), "{case}: manifest was deleted");
        assert!(
            files.contains(&format_idx_file_name(extra_idx_seq)),
            "{case}: newer-generation idx file was swept on fallback"
        );
    }
}

#[test]
fn open_deferred_sweep_defers_preallocate_and_sync_until_mutation() {
    let tmp = TempTestDir::new("defer_boot_meta");
    let path = tmp.path.join(BOOT_META_FILE_NAME);
    {
        let fs = Arc::new(OsFs::open(&tmp.path, true).expect("open OsFs"));
        let _mgr = BootMetaManager::open_or_recover(fs).unwrap();
    }
    // Truncate after Slot A (keeping valid Slot A in the first 4 KB).
    std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(SLOT_SIZE as u64)
        .unwrap();

    let log: EventLog = Arc::default();
    let fs = Arc::new(LoggingFs {
        inner: OsFs::open(&tmp.path, true).expect("reopen OsFs"),
        log: Arc::clone(&log),
    });
    let mut mgr = BootMetaManager::open_or_recover_deferred_sweep(fs).unwrap();
    assert!(
        log.lock().unwrap().is_empty(),
        "deferred open must neither preallocate nor sync boot_meta.db: {:?}",
        *log.lock().unwrap()
    );
    assert_eq!(std::fs::metadata(&path).unwrap().len(), SLOT_SIZE as u64);

    mgr.commit_update(|s| s.checkpoint_lsn = 7).unwrap();
    let events = log.lock().unwrap().clone();
    assert_eq!(
        events,
        vec![
            BootMetaEvent::Preallocate(BOOT_META_FILE_SIZE),
            BootMetaEvent::Sync,
            BootMetaEvent::Write(SLOT_SIZE as u64),
            BootMetaEvent::Sync,
        ]
    );
    assert_eq!(std::fs::metadata(&path).unwrap().len(), BOOT_META_FILE_SIZE);
}

#[test]
fn missing_boot_meta_propagates_wal_io_error_without_creating_boot_meta() {
    struct InjectWalIoErrorFs {
        inner: OsFs,
        fail_open: bool,
        fail_len: bool,
    }
    struct InjectWalLenFile {
        inner: Arc<<OsFs as PlatformFs>::File>,
        fail_len: bool,
    }
    impl PlatformFile for InjectWalLenFile {
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
            if self.fail_len {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "injected wal_sync file_len failure",
                ));
            }
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
    impl PlatformFs for InjectWalIoErrorFs {
        type File = InjectWalLenFile;
        fn open_file(
            &self,
            rel_name: &str,
            create: bool,
            writable: bool,
            hint: OpenHint,
        ) -> io::Result<Arc<Self::File>> {
            if self.fail_open && rel_name == crate::wal::WAL_SYNC_FILE_NAME {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "injected wal_sync open failure",
                ));
            }
            Ok(Arc::new(InjectWalLenFile {
                inner: self.inner.open_file(rel_name, create, writable, hint)?,
                fail_len: self.fail_len && rel_name == crate::wal::WAL_SYNC_FILE_NAME,
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

    for (fail_open, fail_len) in [(true, false), (false, true)] {
        let tmp = TempTestDir::new("missing_boot_meta_wal_err");
        std::fs::write(tmp.path.join(crate::wal::WAL_SYNC_FILE_NAME), b"wal").unwrap();
        let fs = Arc::new(InjectWalIoErrorFs {
            inner: OsFs::open(&tmp.path, true).expect("open OsFs"),
            fail_open,
            fail_len,
        });
        let err = BootMetaManager::open_or_recover(fs)
            .err()
            .expect("WAL I/O error must propagate instead of creating blank boot_meta");
        assert_eq!(err.kind(), io::ErrorKind::PermissionDenied);
        assert!(
            !tmp.path.join(BOOT_META_FILE_NAME).exists(),
            "boot_meta.db must not be created when missing-meta guard fails on WAL I/O"
        );
    }
}

fn craft_raw_catalog_with_pending_delete(name: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&0u32.to_le_bytes()); // idx_segments
    buf.extend_from_slice(&0u32.to_le_bytes()); // std_blob_segments
    buf.extend_from_slice(&0u32.to_le_bytes()); // lru_blob_segments
    buf.extend_from_slice(&0u32.to_le_bytes()); // table_quotas
    buf.extend_from_slice(&1u32.to_le_bytes()); // pending_delete_files = 1
    buf.extend_from_slice(&(name.len() as u16).to_le_bytes());
    buf.extend_from_slice(name.as_bytes());
    buf
}

#[test]
fn decode_rejects_unmanaged_and_referenced_pending_deletes() {
    for bad_name in [
        "boot_meta.db",
        "wal_sync.db",
        "wal_buf.db",
        "db.lock",
        "../escape.db",
        "idx_bad.db",
        "IDX_0000000000000001.db",
        "",
    ] {
        let mut state = BootMetaState::default();
        state.queue_pending_delete(bad_name.to_string());
        assert!(
            state.pending_delete_files.is_empty(),
            "queue_pending_delete must ignore {bad_name:?}"
        );
        let raw = craft_raw_catalog_with_pending_delete(bad_name);
        let err = state
            .decode_catalog_payload(&raw)
            .expect_err("decode must reject non-managed pending_delete filename");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{bad_name:?}");
    }

    // Duplicate segment / quota IDs must also be rejected cleanly.
    let mut dup_idx_payload = Vec::new();
    dup_idx_payload.extend_from_slice(&2u32.to_le_bytes());
    for _ in 0..2 {
        dup_idx_payload.extend_from_slice(&1u64.to_le_bytes());
        dup_idx_payload.extend_from_slice(&4096u64.to_le_bytes());
        dup_idx_payload.extend_from_slice(&0u16.to_le_bytes());
        dup_idx_payload.extend_from_slice(&0u16.to_le_bytes());
        dup_idx_payload.extend_from_slice(&1u32.to_le_bytes());
    }
    dup_idx_payload.extend_from_slice(&[0u8; 16]);
    let mut st = BootMetaState::default();
    assert_eq!(
        st.decode_catalog_payload(&dup_idx_payload)
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );

    // Active referenced segment listed in pending_delete_files must demote the slot, and
    // runtime `try_process_pending_deletes` must refuse to unlink non-managed/referenced files.
    let tmp = TempTestDir::new("pending_delete_guard");
    let fs = Arc::new(OsFs::open(&tmp.path, true).expect("open OsFs"));
    let active_idx = format_idx_file_name(1);
    let mut mgr = BootMetaManager::open_or_recover(Arc::clone(&fs)).unwrap();
    mgr.commit_update(|s| {
        s.idx_segments.push(IdxSegmentMeta {
            seq_id: 1,
            file_len: 4096,
            level: 0,
            record_count: 1,
        });
    })
    .unwrap();
    let _active_f = fs
        .open_file(&active_idx, true, true, OpenHint::ImmutableRandom)
        .unwrap();
    drop(_active_f);
    // Inject an unmanaged file + the live segment directly into in-memory state and run GC.
    std::fs::write(tmp.path.join("user_keep.txt"), b"keep").unwrap();
    mgr.state.pending_delete_files.push("boot_meta.db".into());
    mgr.state.pending_delete_files.push("user_keep.txt".into());
    mgr.state.pending_delete_files.push(active_idx.clone());
    mgr.try_process_pending_deletes();
    assert!(mgr.state().pending_delete_files.is_empty());
    assert!(tmp.path.join("boot_meta.db").exists());
    assert!(tmp.path.join("user_keep.txt").exists());
    assert!(tmp.path.join(&active_idx).exists());

    // Now craft Slot A (Gen 99) claiming `idx_segments=[1, 2]` and `pending_delete=[idx_1]` on
    // disk while `idx_2` exists only in Gen 99; reopen must abort with `InvalidData` rather
    // than falling back to Gen 2 Slot B and sweeping `idx_2`.
    drop(mgr);
    let newer_idx = format_idx_file_name(2);
    let _newer_f = fs
        .open_file(&newer_idx, true, true, OpenHint::ImmutableRandom)
        .unwrap();
    drop(_newer_f);
    let mut state = BootMetaState {
        idx_segments: vec![
            IdxSegmentMeta {
                seq_id: 1,
                file_len: 4096,
                level: 0,
                record_count: 1,
            },
            IdxSegmentMeta {
                seq_id: 2,
                file_len: 4096,
                level: 0,
                record_count: 1,
            },
        ],
        pending_delete_files: vec![active_idx.clone()],
        ..BootMetaState::default()
    };
    let payload = state.encode_catalog_payload().unwrap();
    state.pending_delete_files.clear();
    let meta_path = tmp.path.join(BOOT_META_FILE_NAME);
    let mut raw_slots = std::fs::read(&meta_path).unwrap();
    // Target Slot A (Gen 1) -> bump to Gen 99 with conflicting active + pending delete.
    raw_slots[8..16].copy_from_slice(&99u64.to_le_bytes());
    raw_slots[48..52].copy_from_slice(&(payload.len() as u32).to_le_bytes());
    raw_slots[52..56].copy_from_slice(&crc32c(&payload).to_le_bytes());
    let h_crc = crc32c(&raw_slots[0..60]);
    raw_slots[60..64].copy_from_slice(&h_crc.to_le_bytes());
    raw_slots[64..64 + payload.len()].copy_from_slice(&payload);
    let t_crc = crc32c(&raw_slots[0..4092]);
    raw_slots[4092..4096].copy_from_slice(&t_crc.to_le_bytes());
    std::fs::write(&meta_path, &raw_slots).unwrap();

    let err = match BootMetaManager::open_or_recover(Arc::clone(&fs)) {
        Ok(_) => panic!("CRC-valid self-contradictory slot must abort open without fallback"),
        Err(e) => e,
    };
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    assert!(tmp.path.join(&active_idx).exists());
    assert!(tmp.path.join(&newer_idx).exists());
}

#[test]
fn corrupt_slot_with_u64_max_or_zero_epoch_is_rejected_without_panic() {
    let cases: [(usize, u64); 8] = [
        (8, 0),
        (8, u64::MAX),
        (16, 0),
        (16, u64::MAX),
        (24, u64::MAX),
        (32, 0),
        (32, u64::MAX),
        (40, u64::MAX),
    ];
    for (offset, val) in cases {
        let tmp = TempTestDir::new("u64_max_slot");
        let fs = Arc::new(OsFs::open(&tmp.path, true).expect("open OsFs"));
        {
            let mut mgr = BootMetaManager::open_or_recover(Arc::clone(&fs)).unwrap();
            mgr.commit_update(|s| s.checkpoint_lsn = 42).unwrap();
            assert_eq!(mgr.active_slot(), SlotIndex::SlotB);
        }
        let extra_idx = format_idx_file_name(99);
        std::fs::write(tmp.path.join(&extra_idx), b"gen99").unwrap();
        let meta_path = tmp.path.join(BOOT_META_FILE_NAME);
        let mut raw = std::fs::read(&meta_path).unwrap();
        // Rewrite Slot A (generation=99) with the extreme header value and valid CRCs.
        raw[8..16].copy_from_slice(&99u64.to_le_bytes());
        raw[offset..offset + 8].copy_from_slice(&val.to_le_bytes());
        let h_crc = crc32c(&raw[0..60]);
        raw[60..64].copy_from_slice(&h_crc.to_le_bytes());
        let t_crc = crc32c(&raw[0..4092]);
        raw[4092..4096].copy_from_slice(&t_crc.to_le_bytes());
        std::fs::write(&meta_path, &raw).unwrap();

        let err = match BootMetaManager::open_or_recover(Arc::clone(&fs)) {
            Ok(_) => panic!("offset={offset} val={val:#x}: CRC-valid slot must abort open"),
            Err(e) => e,
        };
        assert_eq!(
            err.kind(),
            io::ErrorKind::InvalidData,
            "offset={offset} val={val:#x}: {err}"
        );
        assert!(
            tmp.path.join(&extra_idx).exists(),
            "offset={offset} val={val:#x}: fallback must not sweep {extra_idx}"
        );
    }

    // An orphan file at SeqID u64::MAX must fail cleanly instead of panicking on next allocate.
    let tmp = TempTestDir::new("seq_u64_max");
    let fs = Arc::new(OsFs::open(&tmp.path, true).expect("open OsFs"));
    let mut mgr = BootMetaManager::open_or_recover(Arc::clone(&fs)).unwrap();
    std::fs::write(tmp.path.join(format_idx_file_name(u64::MAX)), b"alien").unwrap();
    let err = mgr.reserve_seq_ids_above_existing_files().unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
}

#[test]
fn write_rejects_oversized_manifest_and_cleans_up_failed_manifest_file() {
    use std::sync::atomic::{AtomicBool, Ordering};

    // Part 1: catalog exceeding MAX_EXTERNAL_MANIFEST_BYTES (16 MB) fails before allocation or poisoning.
    let tmp = TempTestDir::new("oversized_manifest");
    let fs = Arc::new(OsFs::open(&tmp.path, true).expect("open OsFs"));
    let mut mgr = BootMetaManager::open_or_recover(Arc::clone(&fs)).unwrap();
    let err = mgr
        .commit_update(|s| {
            for seq in 1..=700_000u64 {
                s.idx_segments.push(IdxSegmentMeta {
                    seq_id: seq,
                    file_len: 4096,
                    level: 0,
                    record_count: 1,
                });
            }
        })
        .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    assert!(!mgr.is_poisoned());
    assert_eq!(mgr.state().next_seq_id, 1);
    assert!(
        !fs.list_files()
            .unwrap()
            .iter()
            .any(|f| f.starts_with("manifest_"))
    );
    mgr.commit_update(|s| s.checkpoint_lsn = 55).unwrap();
    assert_eq!(mgr.state().checkpoint_lsn, 55);

    // Part 2: failure writing/syncing an external manifest cleans up the file while consuming SeqID.
    struct FailManifestSyncFs {
        inner: OsFs,
        fail_sync: Arc<AtomicBool>,
    }
    struct FailManifestSyncFile {
        inner: Arc<<OsFs as PlatformFs>::File>,
        is_manifest: bool,
        fail_sync: Arc<AtomicBool>,
    }
    impl PlatformFile for FailManifestSyncFile {
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
            self.inner.sync_barrier()
        }
        fn sync_durable(&self) -> io::Result<()> {
            if self.is_manifest && self.fail_sync.swap(false, Ordering::SeqCst) {
                return Err(io::Error::other("simulated manifest sync failure"));
            }
            self.inner.sync_durable()
        }
        fn evict_os_cache(&self, offset: u64, len: u64) -> io::Result<()> {
            self.inner.evict_os_cache(offset, len)
        }
    }
    impl PlatformFs for FailManifestSyncFs {
        type File = FailManifestSyncFile;
        fn open_file(
            &self,
            rel_name: &str,
            create: bool,
            writable: bool,
            hint: OpenHint,
        ) -> io::Result<Arc<Self::File>> {
            Ok(Arc::new(FailManifestSyncFile {
                inner: self.inner.open_file(rel_name, create, writable, hint)?,
                is_manifest: rel_name.starts_with("manifest_"),
                fail_sync: Arc::clone(&self.fail_sync),
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

    let tmp2 = TempTestDir::new("manifest_cleanup");
    let fail_sync = Arc::new(AtomicBool::new(true));
    let fs2 = Arc::new(FailManifestSyncFs {
        inner: OsFs::open(&tmp2.path, true).unwrap(),
        fail_sync: Arc::clone(&fail_sync),
    });
    let mut mgr2 = BootMetaManager::open_or_recover(Arc::clone(&fs2)).unwrap();
    let err = mgr2
        .commit_update(|s| {
            for i in 1..=200u64 {
                s.idx_segments.push(IdxSegmentMeta {
                    seq_id: i,
                    file_len: 4096,
                    level: 0,
                    record_count: 1,
                });
            }
            s.next_seq_id = 201;
        })
        .unwrap_err();
    assert!(err.to_string().contains("simulated manifest sync failure"));
    assert!(!mgr2.is_poisoned());
    // Failed SeqID 201 stays consumed so retry gets 202, and orphan `manifest_*` is gone!
    assert_eq!(mgr2.state().next_seq_id, 202);
    assert!(
        !fs2.list_files()
            .unwrap()
            .iter()
            .any(|f| f.starts_with("manifest_"))
    );
}

#[test]
fn crc_valid_duplicate_seq_id_slot_and_manifest_abort_without_fallback() {
    // Part 1: inline Slot A (Gen 99) with valid CRCs and duplicate idx seq_id must abort
    // open without falling back to Slot B (Gen 2) or sweeping newer-generation segments.
    let tmp1 = TempTestDir::new("dup_seq_inline");
    let fs1 = Arc::new(OsFs::open(&tmp1.path, true).expect("open OsFs"));
    {
        let mut mgr = BootMetaManager::open_or_recover(Arc::clone(&fs1)).unwrap();
        mgr.commit_update(|s| s.checkpoint_lsn = 10).unwrap();
        assert_eq!(mgr.active_slot(), SlotIndex::SlotB);
    }
    let newer_idx1 = format_idx_file_name(50);
    std::fs::write(tmp1.path.join(&newer_idx1), b"gen99").unwrap();

    let mut dup_payload = Vec::new();
    dup_payload.extend_from_slice(&2u32.to_le_bytes());
    for _ in 0..2 {
        dup_payload.extend_from_slice(&50u64.to_le_bytes());
        dup_payload.extend_from_slice(&4096u64.to_le_bytes());
        dup_payload.extend_from_slice(&0u16.to_le_bytes());
        dup_payload.extend_from_slice(&0u16.to_le_bytes());
        dup_payload.extend_from_slice(&1u32.to_le_bytes());
    }
    dup_payload.extend_from_slice(&[0u8; 16]);

    let meta_path1 = tmp1.path.join(BOOT_META_FILE_NAME);
    let mut raw1 = std::fs::read(&meta_path1).unwrap();
    raw1[6..8].copy_from_slice(&0u16.to_le_bytes());
    raw1[8..16].copy_from_slice(&99u64.to_le_bytes());
    raw1[48..52].copy_from_slice(&(dup_payload.len() as u32).to_le_bytes());
    raw1[52..56].copy_from_slice(&crc32c(&dup_payload).to_le_bytes());
    let h_crc1 = crc32c(&raw1[0..60]);
    raw1[60..64].copy_from_slice(&h_crc1.to_le_bytes());
    raw1[64..64 + dup_payload.len()].copy_from_slice(&dup_payload);
    let t_crc1 = crc32c(&raw1[0..4092]);
    raw1[4092..4096].copy_from_slice(&t_crc1.to_le_bytes());
    std::fs::write(&meta_path1, &raw1).unwrap();

    let err1 = match BootMetaManager::open_or_recover(Arc::clone(&fs1)) {
        Ok(_) => panic!("CRC-valid duplicate seq_id inline slot must abort open"),
        Err(e) => e,
    };
    assert_eq!(err1.kind(), io::ErrorKind::InvalidData);
    assert!(tmp1.path.join(&newer_idx1).exists());

    // Part 2: external manifest (Gen 99) with valid CRCs and duplicate idx seq_id must also
    // abort open without falling back to Slot B or sweeping newer-generation segments.
    let tmp2 = TempTestDir::new("dup_seq_manifest");
    let fs2 = Arc::new(OsFs::open(&tmp2.path, true).expect("open OsFs"));
    {
        let mut mgr = BootMetaManager::open_or_recover(Arc::clone(&fs2)).unwrap();
        mgr.commit_update(|s| s.checkpoint_lsn = 10).unwrap();
        assert_eq!(mgr.active_slot(), SlotIndex::SlotB);
    }
    let newer_idx2 = format_idx_file_name(60);
    std::fs::write(tmp2.path.join(&newer_idx2), b"gen99").unwrap();
    let mf_seq = 61u64;
    let mf_name = format_manifest_file_name(mf_seq);
    let mut mf_cat = Vec::new();
    mf_cat.extend_from_slice(&2u32.to_le_bytes());
    for _ in 0..2 {
        mf_cat.extend_from_slice(&60u64.to_le_bytes());
        mf_cat.extend_from_slice(&4096u64.to_le_bytes());
        mf_cat.extend_from_slice(&0u16.to_le_bytes());
        mf_cat.extend_from_slice(&0u16.to_le_bytes());
        mf_cat.extend_from_slice(&1u32.to_le_bytes());
    }
    mf_cat.extend_from_slice(&[0u8; 16]);
    let mut mf_bytes = Vec::with_capacity(MANIFEST_HEADER_LEN + mf_cat.len());
    mf_bytes.extend_from_slice(MANIFEST_MAGIC);
    mf_bytes.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    mf_bytes.extend_from_slice(&0u16.to_le_bytes());
    mf_bytes.extend_from_slice(&(mf_cat.len() as u32).to_le_bytes());
    let mf_crc = compute_manifest_crc(&mf_bytes[0..12], &mf_cat);
    mf_bytes.extend_from_slice(&mf_crc.to_le_bytes());
    mf_bytes.extend_from_slice(&mf_cat);
    std::fs::write(tmp2.path.join(&mf_name), &mf_bytes).unwrap();

    let mut ptr_payload = [0u8; EXTERNAL_POINTER_LEN];
    ptr_payload[0..8].copy_from_slice(&mf_seq.to_le_bytes());
    ptr_payload[8..16].copy_from_slice(&(mf_bytes.len() as u64).to_le_bytes());
    ptr_payload[16..20].copy_from_slice(&mf_crc.to_le_bytes());

    let meta_path2 = tmp2.path.join(BOOT_META_FILE_NAME);
    let mut raw2 = std::fs::read(&meta_path2).unwrap();
    raw2[6..8].copy_from_slice(&FLAG_EXTERNAL_MANIFEST.to_le_bytes());
    raw2[8..16].copy_from_slice(&99u64.to_le_bytes());
    raw2[48..52].copy_from_slice(&(EXTERNAL_POINTER_LEN as u32).to_le_bytes());
    raw2[52..56].copy_from_slice(&crc32c(&ptr_payload).to_le_bytes());
    let h_crc2 = crc32c(&raw2[0..60]);
    raw2[60..64].copy_from_slice(&h_crc2.to_le_bytes());
    raw2[64..64 + EXTERNAL_POINTER_LEN].copy_from_slice(&ptr_payload);
    let t_crc2 = crc32c(&raw2[0..4092]);
    raw2[4092..4096].copy_from_slice(&t_crc2.to_le_bytes());
    std::fs::write(&meta_path2, &raw2).unwrap();

    let err2 = match BootMetaManager::open_or_recover(Arc::clone(&fs2)) {
        Ok(_) => panic!("CRC-valid duplicate seq_id external manifest must abort open"),
        Err(e) => e,
    };
    assert_eq!(err2.kind(), io::ErrorKind::InvalidData);
    assert!(tmp2.path.join(&mf_name).exists());
    assert!(tmp2.path.join(&newer_idx2).exists());
}

#[test]
fn try_process_pending_deletes_batches_into_single_sync_dir() {
    use std::path::Path;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct BatchDeleteCountFs {
        inner: OsFs,
        unsynced_deletes: AtomicUsize,
        sync_dir_calls: AtomicUsize,
        fail_sync: std::sync::atomic::AtomicBool,
    }

    impl PlatformFs for BatchDeleteCountFs {
        type File = crate::pal::OsFile;

        fn open_file(
            &self,
            rel_name: &str,
            create: bool,
            writable: bool,
            hint: crate::pal::OpenHint,
        ) -> io::Result<Arc<Self::File>> {
            self.inner.open_file(rel_name, create, writable, hint)
        }

        fn remove_file(&self, rel_name: &str) -> io::Result<()> {
            self.inner.remove_file(rel_name)
        }

        fn remove_file_unsynced(&self, rel_name: &str) -> io::Result<()> {
            self.unsynced_deletes.fetch_add(1, Ordering::Relaxed);
            self.inner.remove_file_unsynced(rel_name)
        }

        fn sync_dir(&self) -> io::Result<()> {
            self.sync_dir_calls.fetch_add(1, Ordering::Relaxed);
            if self.fail_sync.swap(false, Ordering::Relaxed) {
                return Err(io::Error::other("injected directory flush failure"));
            }
            self.inner.sync_dir()
        }

        fn list_files(&self) -> io::Result<Vec<String>> {
            self.inner.list_files()
        }

        fn root_path(&self) -> &Path {
            self.inner.root_path()
        }
    }

    let tmp = TempTestDir::new("meta_batch_delete_sync_dir");
    let fs = Arc::new(BatchDeleteCountFs {
        inner: OsFs::open(&tmp.path, true).unwrap(),
        unsynced_deletes: AtomicUsize::new(0),
        sync_dir_calls: AtomicUsize::new(0),
        fail_sync: std::sync::atomic::AtomicBool::new(false),
    });
    let mut mgr = BootMetaManager::open_or_recover(Arc::clone(&fs)).unwrap();

    let mut names = Vec::new();
    for i in 0..5u64 {
        let name = format_idx_file_name(100 + i);
        std::fs::write(tmp.path.join(&name), b"orphan_idx").unwrap();
        names.push(name);
    }

    fs.unsynced_deletes.store(0, Ordering::Relaxed);
    fs.sync_dir_calls.store(0, Ordering::Relaxed);
    fs.fail_sync.store(true, Ordering::Relaxed);
    mgr.commit_update(|s| {
        for name in &names {
            s.queue_pending_delete(name.clone());
        }
    })
    .unwrap();

    assert_eq!(fs.unsynced_deletes.load(Ordering::Relaxed), 5);
    assert_eq!(fs.sync_dir_calls.load(Ordering::Relaxed), 1);
    assert_eq!(mgr.state().pending_delete_files, names);
    mgr.try_process_pending_deletes();
    assert_eq!(fs.sync_dir_calls.load(Ordering::Relaxed), 2);
    assert!(mgr.state().pending_delete_files.is_empty());
    for name in &names {
        assert!(!tmp.path.join(name).exists());
    }
}

#[test]
fn parse_managed_seq_id_rejects_non_canonical_names() {
    assert_eq!(parse_managed_seq_id("idx_000000000000002a.db"), Some(0x2a));
    assert_eq!(
        parse_managed_seq_id("blob_lru_0007_fffffffffffffffe.db"),
        Some(u64::MAX - 1)
    );
    for non_canonical in [
        "idx_+ffffffffffffffff.db",
        "idx_ffffffffffffffff.db".to_ascii_uppercase().as_str(),
        "idx_1.db",
        "blob_lru_backup_ffffffffffffffff.db",
        "../idx_0000000000000001.db",
    ] {
        assert_eq!(
            parse_managed_seq_id(non_canonical),
            None,
            "must reject {non_canonical}"
        );
    }

    let tmp = TempTestDir::new("non_canon_seq");
    let fs = Arc::new(OsFs::open(&tmp.path, true).expect("open OsFs"));
    let mut mgr = BootMetaManager::open_or_recover(Arc::clone(&fs)).unwrap();
    std::fs::write(tmp.path.join("idx_+ffffffffffffffff.db"), b"alien").unwrap();
    mgr.reserve_seq_ids_above_existing_files().unwrap();
    assert!(tmp.path.join("idx_+ffffffffffffffff.db").exists());

    // `u64::MAX - 1` would overflow `next_seq_id` to unreadable `u64::MAX` on +1.
    std::fs::write(tmp.path.join(format_idx_file_name(u64::MAX - 1)), b"edge").unwrap();
    let err = mgr.reserve_seq_ids_above_existing_files().unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
}

#[test]
fn write_rejects_unreadable_u64_max_or_zero_header_bounds() {
    let tmp = TempTestDir::new("write_bounds");
    let fs = Arc::new(OsFs::open(&tmp.path, true).expect("open OsFs"));
    let mut mgr = BootMetaManager::open_or_recover(Arc::clone(&fs)).unwrap();

    assert_eq!(
        mgr.commit_update(|s| s.next_seq_id = u64::MAX)
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(
        mgr.commit_update(|s| s.checkpoint_lsn = u64::MAX)
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(
        mgr.commit_update(|s| s.sync_wal_epoch = 0)
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(
        mgr.commit_update(|s| s.buf_wal_epoch = u64::MAX)
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );
    // State near generation = u64::MAX - 1 must refuse advancing to u64::MAX.
    mgr.state.generation = u64::MAX - 1;
    assert_eq!(
        mgr.commit_update(|_| {}).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
}

#[test]
fn decode_rejects_inflated_and_duplicate_pending_deletes() {
    let mut inflated = Vec::new();
    inflated.extend_from_slice(&0u32.to_le_bytes());
    inflated.extend_from_slice(&0u32.to_le_bytes());
    inflated.extend_from_slice(&0u32.to_le_bytes());
    inflated.extend_from_slice(&0u32.to_le_bytes());
    inflated.extend_from_slice(&10u32.to_le_bytes());
    let name = format_idx_file_name(1);
    inflated.extend_from_slice(&(name.len() as u16).to_le_bytes());
    inflated.extend_from_slice(name.as_bytes());
    let mut st = BootMetaState::default();
    assert_eq!(
        st.decode_catalog_payload(&inflated).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );

    let mut dup = Vec::new();
    dup.extend_from_slice(&0u32.to_le_bytes());
    dup.extend_from_slice(&0u32.to_le_bytes());
    dup.extend_from_slice(&0u32.to_le_bytes());
    dup.extend_from_slice(&0u32.to_le_bytes());
    dup.extend_from_slice(&2u32.to_le_bytes());
    for _ in 0..2 {
        dup.extend_from_slice(&(name.len() as u16).to_le_bytes());
        dup.extend_from_slice(name.as_bytes());
    }
    assert_eq!(
        st.decode_catalog_payload(&dup).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
}

#[test]
fn try_seq_id_allocation_fails_cleanly_before_u64_max() {
    let tmp = TempTestDir::new("meta_seq_exhausted");
    let fs = Arc::new(OsFs::open(&tmp.path, true).expect("open OsFs"));
    let mut mgr = BootMetaManager::open_or_recover(fs).unwrap();

    mgr.state.next_seq_id = u64::MAX - 2;
    assert_eq!(mgr.try_reserve_seq_id().unwrap(), u64::MAX - 2);
    assert_eq!(mgr.state().next_seq_id, u64::MAX - 1);

    // `u64::MAX - 1` must return Err without advancing `next_seq_id` to invalid `u64::MAX`.
    let err = mgr.try_reserve_seq_id().unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    assert_eq!(mgr.state().next_seq_id, u64::MAX - 1);

    let err2 = mgr.state.try_allocate_seq_id().unwrap_err();
    assert_eq!(err2.kind(), io::ErrorKind::InvalidData);
    assert_eq!(mgr.state().next_seq_id, u64::MAX - 1);
}

#[test]
fn open_recovers_concurrently_populated_boot_meta_after_initial_not_found() {
    use std::path::Path;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct RacingOpenFs {
        inner: OsFs,
        inject_not_found: AtomicBool,
    }

    impl PlatformFs for RacingOpenFs {
        type File = crate::pal::OsFile;

        fn open_file(
            &self,
            rel_name: &str,
            create: bool,
            writable: bool,
            hint: OpenHint,
        ) -> io::Result<Arc<Self::File>> {
            if rel_name == BOOT_META_FILE_NAME
                && !create
                && self.inject_not_found.swap(false, Ordering::AcqRel)
            {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "simulated concurrent creation race",
                ));
            }
            self.inner.open_file(rel_name, create, writable, hint)
        }

        fn remove_file(&self, rel_name: &str) -> io::Result<()> {
            self.inner.remove_file(rel_name)
        }

        fn remove_file_unsynced(&self, rel_name: &str) -> io::Result<()> {
            self.inner.remove_file_unsynced(rel_name)
        }

        fn sync_dir(&self) -> io::Result<()> {
            self.inner.sync_dir()
        }

        fn list_files(&self) -> io::Result<Vec<String>> {
            self.inner.list_files()
        }

        fn root_path(&self) -> &Path {
            self.inner.root_path()
        }
    }

    let tmp = TempTestDir::new("meta_concurrent_create_race");
    let os_fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    {
        let mut p1 = BootMetaManager::open_or_recover(Arc::clone(&os_fs)).unwrap();
        p1.commit_update(|s| {
            s.idx_segments.push(IdxSegmentMeta {
                seq_id: 7,
                file_len: 4096,
                level: 0,
                record_count: 3,
            });
            s.next_seq_id = 8;
            s.checkpoint_lsn = 42;
        })
        .unwrap();
    }

    let racing_fs = Arc::new(RacingOpenFs {
        inner: OsFs::open(&tmp.path, true).unwrap(),
        inject_not_found: AtomicBool::new(true),
    });
    let p2 = BootMetaManager::open_or_recover_deferred_sweep(racing_fs).unwrap();
    assert!(p2.state().generation > 1);
    assert_eq!(p2.state().checkpoint_lsn, 42);
    assert_eq!(p2.state().next_seq_id, 8);
    assert_eq!(p2.state().idx_segments.len(), 1);
    assert_eq!(p2.state().idx_segments[0].seq_id, 7);
}

#[test]
fn all_zero_boot_meta_initializes_without_dropping_handle() {
    use std::path::Path;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct OpenCountFs {
        inner: OsFs,
        boot_meta_opens: AtomicUsize,
        sync_dir_calls: AtomicUsize,
    }

    impl PlatformFs for OpenCountFs {
        type File = crate::pal::OsFile;

        fn open_file(
            &self,
            rel_name: &str,
            create: bool,
            writable: bool,
            hint: OpenHint,
        ) -> io::Result<Arc<Self::File>> {
            if rel_name == BOOT_META_FILE_NAME {
                self.boot_meta_opens.fetch_add(1, Ordering::Relaxed);
            }
            self.inner.open_file(rel_name, create, writable, hint)
        }

        fn remove_file(&self, rel_name: &str) -> io::Result<()> {
            self.inner.remove_file(rel_name)
        }

        fn remove_file_unsynced(&self, rel_name: &str) -> io::Result<()> {
            self.inner.remove_file_unsynced(rel_name)
        }

        fn sync_dir(&self) -> io::Result<()> {
            self.sync_dir_calls.fetch_add(1, Ordering::Relaxed);
            self.inner.sync_dir()
        }

        fn list_files(&self) -> io::Result<Vec<String>> {
            self.inner.list_files()
        }

        fn root_path(&self) -> &Path {
            self.inner.root_path()
        }
    }

    let tmp = TempTestDir::new("meta_zero_no_drop");
    std::fs::write(
        tmp.path.join(BOOT_META_FILE_NAME),
        vec![0u8; BOOT_META_FILE_SIZE as usize],
    )
    .unwrap();
    let fs = Arc::new(OpenCountFs {
        inner: OsFs::open(&tmp.path, true).unwrap(),
        boot_meta_opens: AtomicUsize::new(0),
        sync_dir_calls: AtomicUsize::new(0),
    });
    let mgr = BootMetaManager::open_or_recover_deferred_sweep(Arc::clone(&fs)).unwrap();
    assert_eq!(mgr.state().generation, 1);
    assert_eq!(fs.boot_meta_opens.load(Ordering::Relaxed), 1);
    assert_eq!(fs.sync_dir_calls.load(Ordering::Relaxed), 1);
}
