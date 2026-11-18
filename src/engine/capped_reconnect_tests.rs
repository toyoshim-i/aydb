//! Reconnecting a capped table (`register_capped_table` on every full-path connect) must only
//! commit metadata when something changed: the registration, the quota, or the live byte count
//! that the eviction at connect changed.

use super::*;
use crate::pal::{OpenHint, OsFs, PlatformFile, PlatformFs, PreallocStrategy};
use crate::util::test_util::TempTestDir;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};

struct CountFile<F> {
    inner: Arc<F>,
    syncs: Arc<AtomicUsize>,
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
        self.syncs.fetch_add(1, Ordering::SeqCst);
        self.inner.sync_barrier()
    }
    fn sync_durable(&self) -> io::Result<()> {
        self.syncs.fetch_add(1, Ordering::SeqCst);
        self.inner.sync_durable()
    }
    fn evict_os_cache(&self, offset: u64, len: u64) -> io::Result<()> {
        self.inner.evict_os_cache(offset, len)
    }
}

struct CountFs {
    inner: OsFs,
    syncs: Arc<AtomicUsize>,
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
        Ok(Arc::new(CountFile {
            inner: self.inner.open_file(rel_name, create, writable, hint)?,
            syncs: Arc::clone(&self.syncs),
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

const TABLE: u16 = 7;
const QUOTA: u64 = 20_000;

struct Lab {
    fs: Arc<CountFs>,
    syncs: Arc<AtomicUsize>,
    _dir: TempTestDir,
}

impl Lab {
    fn new(name: &str) -> Self {
        let dir = TempTestDir::new(name);
        let syncs = Arc::new(AtomicUsize::new(0));
        let fs = Arc::new(CountFs {
            inner: OsFs::open(&dir.path, true).unwrap(),
            syncs: Arc::clone(&syncs),
        });
        Self {
            fs,
            syncs,
            _dir: dir,
        }
    }

    fn open(&self) -> StorageEngine<CountFs> {
        StorageEngine::open(Arc::clone(&self.fs), EngineConfig::default()).unwrap()
    }

    /// A capped table of five rows, closed.
    fn seeded(name: &str) -> Self {
        let lab = Self::new(name);
        let mut engine = lab.open();
        engine.register_capped_table(TABLE, QUOTA).unwrap();
        for k in 0..5u8 {
            engine
                .put_row(
                    TABLE,
                    &[b'k', k],
                    &[b'v'; 1000],
                    &[],
                    WriteDurability::SyncTransactional,
                )
                .unwrap();
        }
        lab
    }

    /// Metadata commits and syncs that `register_capped_table(TABLE, max_bytes)` caused.
    fn reconnect(&self, engine: &mut StorageEngine<CountFs>, max_bytes: u64) -> (u64, usize) {
        let generation = engine.meta_mgr.state().generation;
        let syncs = self.syncs.load(Ordering::SeqCst);
        engine.register_capped_table(TABLE, max_bytes).unwrap();
        (
            engine.meta_mgr.state().generation - generation,
            self.syncs.load(Ordering::SeqCst) - syncs,
        )
    }
}

fn row_count(engine: &mut StorageEngine<CountFs>) -> usize {
    engine
        .scan_keys_range(TABLE, None, None, 1000)
        .unwrap()
        .len()
}

#[test]
fn an_unchanged_reconnect_commits_no_metadata() {
    let lab = Lab::seeded("capped_reconnect_same");
    let mut engine = lab.open();
    for _ in 0..3 {
        assert_eq!(lab.reconnect(&mut engine, QUOTA), (0, 0));
    }
    assert_eq!(row_count(&mut engine), 5);
    // Also on the engine that registered the table, without a reopen.
    drop(engine);
    let mut engine = lab.open();
    assert_eq!(lab.reconnect(&mut engine, QUOTA), (0, 0));
}

#[test]
fn a_first_registration_commits_the_quota() {
    let lab = Lab::new("capped_reconnect_first");
    let mut engine = lab.open();
    let (commits, syncs) = lab.reconnect(&mut engine, QUOTA);
    assert_eq!(commits, 1);
    assert!(syncs >= 1);
    drop(engine);
    let engine = lab.open();
    assert_eq!(engine.capped_table_state(TABLE).unwrap().max_bytes, QUOTA);
}

#[test]
fn a_changed_quota_commits_and_persists() {
    let lab = Lab::seeded("capped_reconnect_changed");
    let mut engine = lab.open();
    let (commits, syncs) = lab.reconnect(&mut engine, QUOTA * 2);
    assert_eq!(commits, 1);
    assert!(syncs >= 1);
    assert_eq!(lab.reconnect(&mut engine, QUOTA * 2), (0, 0));
    drop(engine);
    let engine = lab.open();
    assert_eq!(
        engine.capped_table_state(TABLE).unwrap().max_bytes,
        QUOTA * 2
    );
}

#[test]
fn a_reconnect_that_evicts_commits_the_new_byte_count() {
    let lab = Lab::seeded("capped_reconnect_evicts");
    let mut engine = lab.open();
    // 5 rows of about 1000 bytes do not fit 2500 bytes.
    let (commits, syncs) = lab.reconnect(&mut engine, 2_500);
    assert_eq!(commits, 1);
    assert!(syncs >= 1);
    let kept = row_count(&mut engine);
    assert!(kept > 0 && kept < 5, "{kept}");
    drop(engine);
    let mut engine = lab.open();
    assert_eq!(row_count(&mut engine), kept);
    assert_eq!(engine.capped_table_state(TABLE).unwrap().max_bytes, 2_500);
    assert_eq!(lab.reconnect(&mut engine, 2_500), (0, 0));
}

/// A quota change whose commit was lost (the process died before it) is picked up by the next
/// connect: the stored quota differs from the declared one, so the connect commits it.
#[test]
fn a_lost_quota_change_converges_on_the_next_connect() {
    let lab = Lab::seeded("capped_reconnect_lost");
    {
        let mut engine = lab.open();
        // Simulate the lost commit: the in-memory quota moves on, the stored one does not.
        engine.capped_tables.get_mut(&TABLE).unwrap().max_bytes = QUOTA * 3;
        assert_eq!(engine.meta_mgr.state().table_quotas[0].max_bytes, QUOTA);
    }
    let mut engine = lab.open();
    assert_eq!(engine.capped_table_state(TABLE).unwrap().max_bytes, QUOTA);
    let (commits, _) = lab.reconnect(&mut engine, QUOTA * 3);
    assert_eq!(commits, 1);
    drop(engine);
    let engine = lab.open();
    assert_eq!(
        engine.capped_table_state(TABLE).unwrap().max_bytes,
        QUOTA * 3
    );
}
