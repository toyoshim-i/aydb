use super::*;
use crate::meta::format_idx_file_name;
use crate::pal::{OsFile, OsFs, PreallocStrategy};
use crate::util::test_util::TempTestDir;
use std::path::Path;
use std::sync::atomic::AtomicU8;

struct MetadataFaultFile {
    inner: Arc<OsFile>,
    is_metadata: bool,
    fault: Arc<AtomicU8>,
}

impl PlatformFile for MetadataFaultFile {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        self.inner.read_at(buf, offset)
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        self.inner.read_exact_at(buf, offset)
    }

    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        if self.is_metadata && self.fault.load(Ordering::SeqCst) == 1 {
            return Err(io::Error::other("injected metadata write failure"));
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
        self.inner.sync_durable()?;
        if self.is_metadata && self.fault.load(Ordering::SeqCst) == 2 {
            return Err(io::Error::other(
                "injected metadata error after durable write",
            ));
        }
        Ok(())
    }

    fn evict_os_cache(&self, offset: u64, len: u64) -> io::Result<()> {
        self.inner.evict_os_cache(offset, len)
    }
}

struct MetadataFaultFs {
    inner: OsFs,
    fault: Arc<AtomicU8>,
}

impl PlatformFs for MetadataFaultFs {
    type File = MetadataFaultFile;

    fn open_file(
        &self,
        name: &str,
        create: bool,
        writable: bool,
        hint: OpenHint,
    ) -> io::Result<Arc<Self::File>> {
        Ok(Arc::new(MetadataFaultFile {
            inner: self.inner.open_file(name, create, writable, hint)?,
            is_metadata: name == "boot_meta.db",
            fault: Arc::clone(&self.fault),
        }))
    }

    fn remove_file(&self, name: &str) -> io::Result<()> {
        self.inner.remove_file(name)
    }

    fn list_files(&self) -> io::Result<Vec<String>> {
        self.inner.list_files()
    }

    fn root_path(&self) -> &Path {
        self.inner.root_path()
    }
}

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
    assert!(engine.meta_mgr.sweep_orphans().is_err());
    assert!(engine.meta_mgr.commit_update(|_| {}).is_err());
    engine.meta_mgr.try_process_pending_deletes();
    assert_eq!(engine.get_row(1, b"key").unwrap(), Some(b"value".to_vec()));
}

#[test]
fn uncertain_metadata_flush_keeps_output_until_reopen() {
    // Both outcomes are possible after an I/O error: the old slot can remain authoritative,
    // or the new slot can be fully durable even though its flush reported failure.
    for fault_mode in [1, 2] {
        let tmp = TempTestDir::new("metadata_flush_uncertain");
        let fault = Arc::new(AtomicU8::new(0));
        let fs = Arc::new(MetadataFaultFs {
            inner: OsFs::open(&tmp.path, true).unwrap(),
            fault: Arc::clone(&fault),
        });
        let mut engine = StorageEngine::open(Arc::clone(&fs), EngineConfig::default()).unwrap();
        engine
            .put_row(1, b"key", b"value", &[], WriteDurability::SyncTransactional)
            .unwrap();
        let output = format_idx_file_name(engine.meta_mgr.state().next_seq_id);
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
        let fs = Arc::new(MetadataFaultFs {
            inner: OsFs::open(&tmp.path, true).unwrap(),
            fault: Arc::clone(&fault),
        });
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
