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
fn reconciliation_preserves_unreadable_metadata_for_index_cleanup() {
    for spilled in [false, true] {
        for policy in [CorruptPolicy::Error, CorruptPolicy::Evict] {
            let lab = Lab::new("capped_malformed_metadata");
            let mut engine = lab.open();
            let durability = WriteDurability::SyncTransactional;
            engine.register_capped_table(TABLE, QUOTA).unwrap();
            engine
                .put_row(TABLE, b"k", b"v", &[(1, b"a".to_vec())], durability)
                .unwrap();
            let key = encode_row_meta_key(TABLE, b"k");
            let record = if spilled {
                let blob_ptr = engine
                    .append_to_blob_segment(BlobKind::Standard, 0, &key, &[0])
                    .unwrap();
                engine.sync_active_std_writer().unwrap();
                WalRecord::PutBlobPtr { key, blob_ptr }
            } else {
                WalRecord::PutInline {
                    key,
                    value: vec![0],
                }
            };
            engine
                .commit_wal_and_memtable(vec![record], durability)
                .unwrap();
            engine.set_table_corrupt_policy(TABLE, policy);
            assert!(engine.reconcile_capped_tables_readonly().unwrap());
            assert!(!engine.has_pending_corrupt_evictions());
            engine.reconcile_single_capped_table(TABLE).unwrap();
            assert_eq!(
                engine.has_pending_corrupt_evictions(),
                policy == CorruptPolicy::Evict
            );
            assert_eq!(
                engine
                    .capped_table_state(TABLE)
                    .unwrap()
                    .current_logical_bytes,
                2
            );
            engine.drain_corrupt_evictions().unwrap();
            assert_eq!(
                engine.row_exists(TABLE, b"k").unwrap(),
                policy == CorruptPolicy::Error
            );
            engine.delete_row(TABLE, b"k", durability).unwrap();
            assert!(
                engine
                    .lookup_by_secondary_index(TABLE, 1, b"a")
                    .unwrap()
                    .is_empty()
            );
            assert_eq!(
                engine
                    .capped_table_state(TABLE)
                    .unwrap()
                    .current_logical_bytes,
                0
            );
        }
    }
}

#[test]
fn eviction_reports_a_malformed_queue_key() {
    let lab = Lab::new("capped_malformed_queue");
    let mut engine = lab.open();
    engine.register_capped_table(TABLE, QUOTA).unwrap();
    let mut key = crate::lru::encode_namespace_prefix(TABLE, crate::lru::NS_LRU_QUEUE).to_vec();
    key.push(1);
    engine
        .commit_wal_and_memtable(
            vec![WalRecord::PutInline { key, value: vec![] }],
            WriteDurability::BufferedFast,
        )
        .unwrap();
    engine
        .capped_tables
        .get_mut(&TABLE)
        .unwrap()
        .current_logical_bytes = QUOTA + 1;
    let err = engine
        .evict_until_within_quota(TABLE, WriteDurability::BufferedFast)
        .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
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
