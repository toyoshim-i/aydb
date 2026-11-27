use super::*;
use crate::meta::format_idx_file_name;
use crate::pal::test_fs::{FsHooks, HookedFs};
use crate::pal::{OsFs, PlatformFile};
use crate::util::test_util::TempTestDir;
use std::sync::atomic::AtomicU8;

struct MetadataFaultHooks {
    fault: Arc<AtomicU8>,
}

impl FsHooks for MetadataFaultHooks {
    fn write_all_at(
        &self,
        name: &str,
        inner: &dyn PlatformFile,
        buf: &[u8],
        offset: u64,
    ) -> io::Result<()> {
        if name == "boot_meta.db" && self.fault.load(Ordering::SeqCst) == 1 {
            return Err(io::Error::other("injected metadata write failure"));
        }
        inner.write_all_at(buf, offset)
    }

    fn sync_durable(&self, name: &str, inner: &dyn PlatformFile) -> io::Result<()> {
        inner.sync_durable()?;
        if name == "boot_meta.db" && self.fault.load(Ordering::SeqCst) == 2 {
            return Err(io::Error::other(
                "injected metadata error after durable write",
            ));
        }
        if name.starts_with("manifest_") && self.fault.load(Ordering::SeqCst) == 3 {
            return Err(io::Error::other("injected manifest sync failure"));
        }
        Ok(())
    }
}

type MetadataFaultFs = HookedFs<OsFs, MetadataFaultHooks>;

fn assert_fail_stop(engine: &mut StorageEngine<MetadataFaultFs>) {
    assert!(engine.is_poisoned());
    assert!(engine.flush_memtable().is_err());
    assert!(engine.checkpoint().is_err());
    assert!(engine.prepare_compaction().is_err());
    assert!(engine.register_capped_table(2, 100).is_err());
    assert!(
        engine
            .put_row(
                1,
                b"rejected",
                b"value",
                &[],
                WriteDurability::SyncTransactional
            )
            .is_err()
    );
    assert!(engine.healthy().is_err());
    assert!(engine.storage.debug_meta_mgr_mut().sweep_orphans().is_err());
    assert!(
        engine
            .storage
            .debug_meta_mgr_mut()
            .commit_update(|_| {})
            .is_err()
    );
    engine
        .storage
        .debug_meta_mgr_mut()
        .try_process_pending_deletes();
    assert_eq!(engine.get_row(1, b"key").unwrap(), Some(b"value".to_vec()));
}

#[test]
fn uncertain_metadata_flush_keeps_output_until_reopen() {
    // Both outcomes are possible after an I/O error: the old slot can remain authoritative,
    // or the new slot can be fully durable even though its flush reported failure.
    for fault_mode in [1, 2] {
        let tmp = TempTestDir::new("metadata_flush_uncertain");
        let fault = Arc::new(AtomicU8::new(0));
        let fs = Arc::new(HookedFs::new(
            OsFs::open(&tmp.path, true).unwrap(),
            MetadataFaultHooks {
                fault: Arc::clone(&fault),
            },
        ));
        let mut engine = StorageEngine::open(Arc::clone(&fs), EngineConfig::default()).unwrap();
        engine
            .put_row(1, b"key", b"value", &[], WriteDurability::SyncTransactional)
            .unwrap();
        let output = format_idx_file_name(engine.meta_mgr().state().next_seq_id);
        fault.store(fault_mode, Ordering::SeqCst);
        assert!(engine.flush_memtable().is_err());
        fault.store(0, Ordering::SeqCst);
        assert_fail_stop(&mut engine);
        assert!(fs.list_files().unwrap().contains(&output));
        drop(engine);

        let mut reopened = StorageEngine::open(fs, EngineConfig::default()).unwrap();
        assert!(!reopened.is_poisoned());
        assert_eq!(
            reopened.get_row(1, b"key").unwrap(),
            Some(b"value".to_vec())
        );
        reopened
            .put_row(
                1,
                b"after",
                b"reopen",
                &[],
                WriteDurability::SyncTransactional,
            )
            .unwrap();
    }
}

#[test]
fn uncertain_metadata_compaction_keeps_inputs_and_output_until_reopen() {
    for fault_mode in [1, 2] {
        let tmp = TempTestDir::new("metadata_compaction_uncertain");
        let fault = Arc::new(AtomicU8::new(0));
        let fs = Arc::new(HookedFs::new(
            OsFs::open(&tmp.path, true).unwrap(),
            MetadataFaultHooks {
                fault: Arc::clone(&fault),
            },
        ));
        let mut config = EngineConfig::default();
        config.auto_compact_segment_threshold = 0;
        let mut engine = StorageEngine::open(Arc::clone(&fs), config.clone()).unwrap();
        for value in [b"old".as_slice(), b"value".as_slice()] {
            engine
                .put_row(1, b"key", value, &[], WriteDurability::SyncTransactional)
                .unwrap();
            engine.flush_memtable().unwrap();
        }
        let inputs: Vec<_> = engine
            .idx_segments
            .iter()
            .map(|r| format_idx_file_name(r.seq_id()))
            .collect();
        let job = engine.prepare_compaction().unwrap().unwrap();
        let output = format_idx_file_name(job.output_seq_id());
        let result = job.run().unwrap();
        fault.store(fault_mode, Ordering::SeqCst);
        assert!(engine.install_compaction(result).is_err());
        fault.store(0, Ordering::SeqCst);
        assert_fail_stop(&mut engine);
        let remaining = fs.list_files().unwrap();
        assert!(remaining.contains(&output));
        assert!(inputs.iter().all(|name| remaining.contains(name)));
        drop(engine);

        let mut reopened = StorageEngine::open(fs, config).unwrap();
        assert!(!reopened.is_poisoned());
        assert_eq!(
            reopened.get_row(1, b"key").unwrap(),
            Some(b"value".to_vec())
        );
    }
}

const SYNC: WriteDurability = WriteDurability::SyncTransactional;

fn open_with_large_catalog(
    name: &str,
) -> (
    TempTestDir,
    Arc<MetadataFaultFs>,
    Arc<AtomicU8>,
    EngineConfig,
    StorageEngine<MetadataFaultFs>,
) {
    let tmp = TempTestDir::new(name);
    let fault = Arc::new(AtomicU8::new(0));
    let fs = Arc::new(HookedFs::new(
        OsFs::open(&tmp.path, true).unwrap(),
        MetadataFaultHooks {
            fault: Arc::clone(&fault),
        },
    ));
    let mut config = EngineConfig::default();
    config.auto_compact_segment_threshold = 0;
    let mut engine = StorageEngine::open(Arc::clone(&fs), config.clone()).unwrap();
    // Enough tables that the catalog payload overflows into an external manifest file.
    for i in 0..300u16 {
        engine.register_capped_table(1000 + i, 1 << 40).unwrap();
    }
    assert!(engine.meta_mgr().state().active_manifest_seq_id.is_some());
    (tmp, fs, fault, config, engine)
}

#[test]
fn manifest_retry_after_pre_slot_failure_keeps_acknowledged_writes() {
    let (_tmp, fs, fault, config, mut engine) = open_with_large_catalog("manifest_retry_shrink");
    engine
        .put_row(1, b"pre", b"acked-before-flush", &[], SYNC)
        .unwrap();

    // The manifest is written and synced, then the failure is reported before any boot_meta
    // slot is touched, so the commit is retryable and leaves a full-size manifest file behind.
    fault.store(3, Ordering::SeqCst);
    assert!(engine.flush_memtable().is_err());
    fault.store(0, Ordering::SeqCst);
    assert!(!engine.is_poisoned());

    // The catalog shrinks without its own metadata commit (the quota commit is skipped because
    // a flush follows), so the retried manifest is shorter than the failed attempt's.
    engine.begin_transaction();
    engine.drop_table(1000, None).unwrap();
    engine.flush_memtable().unwrap();
    engine.commit_transaction().unwrap();
    assert_eq!(engine.maintenance_failures(), 0);
    engine
        .put_row(1, b"post", b"acked-after-flush", &[], SYNC)
        .unwrap();
    drop(engine);

    let mut reopened = StorageEngine::open(fs, config).unwrap();
    assert_eq!(
        reopened.get_row(1, b"pre").unwrap(),
        Some(b"acked-before-flush".to_vec())
    );
    assert_eq!(
        reopened.get_row(1, b"post").unwrap(),
        Some(b"acked-after-flush".to_vec())
    );
}

#[test]
fn manifest_retry_after_pre_slot_failure_without_shrink_keeps_acknowledged_writes() {
    // Control: the same failure and retry with an unchanged catalog size.
    let (_tmp, fs, fault, config, mut engine) = open_with_large_catalog("manifest_retry_same");
    engine
        .put_row(1, b"pre", b"acked-before-flush", &[], SYNC)
        .unwrap();
    fault.store(3, Ordering::SeqCst);
    assert!(engine.flush_memtable().is_err());
    fault.store(0, Ordering::SeqCst);
    engine.begin_transaction();
    engine.flush_memtable().unwrap();
    engine.commit_transaction().unwrap();
    engine
        .put_row(1, b"post", b"acked-after-flush", &[], SYNC)
        .unwrap();
    drop(engine);

    let mut reopened = StorageEngine::open(fs, config).unwrap();
    assert_eq!(
        reopened.get_row(1, b"pre").unwrap(),
        Some(b"acked-before-flush".to_vec())
    );
    assert_eq!(
        reopened.get_row(1, b"post").unwrap(),
        Some(b"acked-after-flush".to_vec())
    );
}
