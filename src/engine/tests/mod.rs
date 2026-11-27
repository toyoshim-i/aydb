use super::*;
use crate::pal::OsFs;
use crate::util::test_util::TempTestDir;
use std::path::PathBuf;
use std::sync::atomic::Ordering;

mod blobs;
mod compaction;
mod eviction;
mod tx;
mod wal_recovery;
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

// ---------------------------------------------------------------------------
// Blob corruption policy (`CorruptPolicy`, M4)
// ---------------------------------------------------------------------------

/// Locates the payload's last byte of the blob record referenced by the record at `key` (must be
/// a blob pointer). Call after `flush_memtable` and BEFORE dropping the engine; the corruption
/// itself is applied by `apply_blob_corruption` once the engine (exclusive handles on Windows)
/// is gone.
pub(super) fn blob_corruption_target(engine: &StorageEngine<OsFs>, key: &[u8]) -> (String, u64) {
    let Some((_, IndexValue::Blob(ptr))) = engine.lookup_raw_entry(key).unwrap() else {
        panic!("key is not stored as a blob");
    };
    let name = match ptr.kind {
        BlobKind::Standard => format_std_blob_file_name(ptr.seg_id),
        BlobKind::Lru => format_lru_blob_file_name(ptr.table_id, ptr.seg_id),
    };
    (name, ptr.offset as u64 + ptr.len as u64 - 1)
}

pub(super) fn apply_blob_corruption(dir: &std::path::Path, target: &(String, u64)) {
    let path = dir.join(&target.0);
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[target.1 as usize] ^= 0xFF;
    std::fs::write(&path, &bytes).unwrap();
}

pub(super) fn live_lru_pks(engine: &StorageEngine<OsFs>, table_id: u16) -> Vec<Vec<u8>> {
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

// ---------------------------------------------------------------------------------------------
// Background (3-phase) compaction
// ---------------------------------------------------------------------------------------------

pub(super) fn bg_cfg() -> EngineConfig {
    let mut cfg = EngineConfig::default();
    cfg.auto_compact_segment_threshold = 4;
    cfg.background_compaction = true;
    cfg
}

pub(super) fn bg_put_round(
    engine: &mut StorageEngine<OsFs>,
    round: usize,
    rows: usize,
    len: usize,
) {
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

pub(super) fn idx_file_names(fs: &OsFs) -> Vec<String> {
    let mut v: Vec<String> = fs
        .list_files()
        .unwrap()
        .into_iter()
        .filter(|n| n.starts_with("idx_"))
        .collect();
    v.sort();
    v
}

pub(super) fn put_t(engine: &mut StorageEngine<OsFs>, k: &str, v: &str) {
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

pub(super) fn keys_of_r(r: &crate::engine::EngineReader<'_, OsFs>, reverse: bool) -> Vec<String> {
    let mut pks = Vec::new();
    let mut slab = Vec::new();
    let mut spans = Vec::new();
    r.scan_range_into_slab(
        1,
        RangeScanOptions {
            lower: None,
            upper: None,
            reverse,
            limit: usize::MAX,
            keys_only: true,
        },
        SlabScanOutput {
            pks: &mut pks,
            slab: &mut slab,
            spans: &mut spans,
            lazy_blobs: None,
        },
    )
    .unwrap();
    pks.into_iter()
        .map(|k| String::from_utf8(k).unwrap())
        .collect()
}

pub(super) fn keys_of(engine: &mut StorageEngine<OsFs>, reverse: bool) -> Vec<String> {
    let mut pks = Vec::new();
    let mut slab = Vec::new();
    let mut spans = Vec::new();
    engine
        .scan_range_into_slab(
            1,
            RangeScanOptions {
                lower: None,
                upper: None,
                reverse,
                limit: usize::MAX,
                keys_only: true,
            },
            SlabScanOutput {
                pks: &mut pks,
                slab: &mut slab,
                spans: &mut spans,
                lazy_blobs: None,
            },
        )
        .unwrap();
    pks.into_iter()
        .map(|k| String::from_utf8(k).unwrap())
        .collect()
}

/// Runs `f` on a helper thread and fails the test if it does not finish in `secs` (deadlock guard).
pub(super) fn run_with_timeout(secs: u64, f: impl FnOnce() + Send + 'static) {
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

pub(super) fn pair_value(n: u32) -> Vec<u8> {
    // Alternate inline (50 B) and out-of-line blob (3000 B) payloads.
    let len = if n.is_multiple_of(2) { 50 } else { 3000 };
    format!("{n:08}").bytes().cycle().take(len).collect()
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

/// Counts `sync_durable` calls on blob segment files and records the blob sync count observed
/// at the time of each WAL sync.
pub(super) mod blob_sync_counting {
    use super::*;
    use crate::pal::test_fs::{FsHooks, HookedFs};
    use std::sync::atomic::AtomicU64;

    #[derive(Default)]
    pub struct Ctl {
        pub blob_syncs: AtomicU64,
        pub blob_syncs_at_wal_sync: AtomicU64,
        pub fail_blob_sync: std::sync::atomic::AtomicBool,
    }
    pub struct CountHooks {
        pub ctl: Arc<Ctl>,
    }
    impl CountHooks {
        fn record_wal_sync(&self, name: &str) {
            if name.starts_with("wal_sync") {
                self.ctl
                    .blob_syncs_at_wal_sync
                    .store(self.ctl.blob_syncs.load(Ordering::SeqCst), Ordering::SeqCst);
            }
        }
    }
    impl FsHooks for CountHooks {
        fn sync_barrier(&self, name: &str, inner: &dyn PlatformFile) -> io::Result<()> {
            self.record_wal_sync(name);
            inner.sync_barrier()
        }
        fn sync_durable(&self, name: &str, inner: &dyn PlatformFile) -> io::Result<()> {
            self.record_wal_sync(name);
            if name.starts_with("blob_") {
                if self.ctl.fail_blob_sync.load(Ordering::SeqCst) {
                    return Err(io::Error::other("simulated blob sync failure"));
                }
                self.ctl.blob_syncs.fetch_add(1, Ordering::SeqCst);
            }
            inner.sync_durable()
        }
    }
    pub fn open_fs(dir: &std::path::Path, ctl: &Arc<Ctl>) -> Arc<HookedFs<OsFs, CountHooks>> {
        Arc::new(HookedFs::new(
            OsFs::open(dir, true).unwrap(),
            CountHooks {
                ctl: Arc::clone(ctl),
            },
        ))
    }
}

pub(super) const GROUP_SYNC: WriteDurability = WriteDurability::SyncTransactional;

pub(super) fn open_group_engine(tmp: &TempTestDir) -> StorageEngine<OsFs> {
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    StorageEngine::open(fs, EngineConfig::default()).unwrap()
}

pub(super) fn row_visible_to(
    engine: &StorageEngine<OsFs>,
    conn: usize,
    key: &[u8],
) -> Option<Vec<u8>> {
    let (mut slab, mut spans) = (Vec::new(), Vec::new());
    engine
        .reader_for_conn(conn)
        .get_row_into(1, key, &mut slab, &mut spans)
        .unwrap()
        .then_some(slab)
}

pub(super) type GroupTicket = (
    u64,
    Arc<<OsFs as PlatformFs>::File>,
    Arc<crate::engine::tx::CommitGate>,
);

pub(super) fn commit_into_group(
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

pub(super) fn flush_group(engine: &mut StorageEngine<OsFs>, ticket: &GroupTicket) -> u64 {
    let GroupRole::Lead(target) = ticket.2.join_group(ticket.0).unwrap() else {
        panic!("no flush has run, so this commit must lead");
    };
    engine
        .finish_group_flush(target, ticket.1.sync_barrier())
        .unwrap();
    target
}

/// In-memory filesystem whose power loss reverts every file to its last `sync_durable` /
/// `sync_barrier` image, except `MutableSyncLog` files, which are write-through.
pub(super) struct PowerLossFs {
    files: std::sync::Mutex<HashMap<String, Arc<PowerLossFile>>>,
    root: PathBuf,
    /// Fault mode shared with every `blob_*` file, see `BLOB_SYNC_FAIL_ONCE`.
    blob_sync_fault: Arc<std::sync::atomic::AtomicU8>,
}

/// Blob sync fault modes of [`PowerLossFs`]. `BLOB_SYNC_FAIL_ONCE` models Linux "fsyncgate":
/// the next `sync_durable` on a blob file fails, the dirty pages are dropped and marked clean,
/// so every later sync reports success without persisting anything (`BLOB_SYNC_LIE`).
pub(super) const BLOB_SYNC_OK: u8 = 0;
pub(super) const BLOB_SYNC_FAIL_ONCE: u8 = 1;
pub(super) const BLOB_SYNC_LIE: u8 = 2;
/// Every `write_all_at` on a blob file fails with `StorageFull`; syncs are unaffected.
pub(super) const BLOB_WRITE_FAIL: u8 = 3;

pub(super) struct PowerLossFile {
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

pub(super) fn ring_full_config() -> EngineConfig {
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
pub(super) fn ring_full_setup(
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
pub(super) fn fill_wal_to_last_sector(
    e: &mut StorageEngine<PowerLossFs>,
    durability: WriteDurability,
) {
    let mut i = 0;
    loop {
        let (idx, total) = match durability {
            WriteDurability::SyncTransactional => (
                e.sync_wal().current_sector_idx(),
                e.sync_wal().total_sectors(),
            ),
            WriteDurability::BufferedFast => (
                e.buf_wal().current_sector_idx(),
                e.buf_wal().total_sectors(),
            ),
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

pub(super) fn wal_epoch(e: &StorageEngine<PowerLossFs>, durability: WriteDurability) -> u64 {
    match durability {
        WriteDurability::SyncTransactional => e.sync_wal().active_epoch(),
        WriteDurability::BufferedFast => e.buf_wal().active_epoch(),
    }
}

pub(super) fn count_tx_rows(e: &mut StorageEngine<PowerLossFs>) -> usize {
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
pub(super) fn ring_full_commit_survivors(corrupt: bool, durability: WriteDurability) -> usize {
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

/// WAL small enough that every flush is forced by a full ring, with a MemTable that never
/// reaches `should_flush()` on its own.
pub(super) fn ring_only_flush_config(background: bool) -> EngineConfig {
    let mut c = ring_full_config();
    c.auto_compact_segment_threshold = 4;
    c.background_compaction = background;
    c
}

pub(super) const RING_ONLY_SEGMENT_BOUND: usize = 8;

pub(super) fn ring_only_workload(
    background: bool,
    in_tx: bool,
) -> (StorageEngine<PowerLossFs>, usize) {
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

// ---------------------------------------------------------------------------------------------
// Blob reclaim and persisted dead bytes must ignore transactions that are not yet published.
// ---------------------------------------------------------------------------------------------

pub(super) const OLD_BLOB: [u8; 1500] = [0x33; 1500];

pub(super) fn blob_file_count(fs: &PowerLossFs) -> usize {
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
pub(super) fn dead_blob_setup(
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
pub(super) fn commit_overwrite_of_r1(
    e: &mut StorageEngine<PowerLossFs>,
    durability: WriteDurability,
) -> bool {
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

pub(super) fn persisted_dead_bytes(e: &StorageEngine<PowerLossFs>) -> u64 {
    let st = e.meta_mgr().state();
    st.std_blob_segments
        .iter()
        .map(|s| s.dead_bytes)
        .chain(st.lru_blob_segments.iter().map(|s| s.dead_bytes))
        .sum()
}

// ---------------------------------------------------------------------------------------------
// Blob segment sync failures (Linux "fsyncgate"): a failed blob sync leaves the durability of
// the blob bytes unknown, and a retried sync may report success without persisting them. The
// engine must therefore be poisoned, so that no checkpoint, flush or WAL record can ever cover
// a blob that is not durable.
// ---------------------------------------------------------------------------------------------

pub(super) const FAULT_BLOB_LEN: usize = 1500;

pub(super) type AckedRows = Vec<(Vec<u8>, Vec<u8>)>;

pub(super) fn blob_fault_config() -> EngineConfig {
    let mut c = EngineConfig::default();
    c.blob_cache_bytes = 0;
    c.block_cache_bytes = 0;
    c
}

pub(super) fn blob_fault_value(tag: u8) -> Vec<u8> {
    vec![tag; FAULT_BLOB_LEN]
}

pub(super) fn blob_fault_setup(
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
pub(super) fn assert_blob_sync_fail_stop(e: &mut StorageEngine<PowerLossFs>) {
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
pub(super) fn assert_acked_rows_survive(
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

/// Walks the table with `scan_table_batch` the way the UNIQUE check does.
pub(super) fn walk_table_in_batches(
    engine: &mut StorageEngine<OsFs>,
    limit: usize,
) -> Vec<Vec<u8>> {
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

// ---------------------------------------------------------------------------------------------
// backfill_secondary_keys
// ---------------------------------------------------------------------------------------------

pub(super) fn live_keys(engine: &StorageEngine<OsFs>, table_id: u16, ns: u8) -> Vec<Vec<u8>> {
    engine
        .scan_merged_prefix(&encode_namespace_prefix(table_id, ns))
        .unwrap()
        .into_iter()
        .filter(|e| !matches!(e.value, IndexValue::Tombstone))
        .map(|e| e.key)
        .collect()
}

pub(super) fn row_meta(
    engine: &mut StorageEngine<OsFs>,
    table_id: u16,
    pk: &[u8],
) -> Option<RowMetaRecord> {
    engine
        .get_row_meta_with_ptr(&encode_row_meta_key(table_id, pk))
        .unwrap()
        .map(|(m, _)| m)
}

pub(super) fn backfill_engine(
    name: &str,
) -> (TempTestDir, Arc<OsFs>, EngineConfig, StorageEngine<OsFs>) {
    let tmp = TempTestDir::new(name);
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let cfg = EngineConfig::default();
    let engine = StorageEngine::open(fs.clone(), cfg.clone()).unwrap();
    (tmp, fs, cfg, engine)
}

pub(super) const UNIQ: u16 = 0x8001;

#[test]
fn lsn_exhaustion_rejects_writes_and_preserves_the_last_commit() {
    let (_tmp, fs, cfg, mut engine) = backfill_engine("lsn_exhaustion");
    let sync = WriteDurability::SyncTransactional;
    engine.next_lsn = u64::MAX - 1;
    engine.begin_transaction();
    engine
        .put_row(1, b"rolled-back", b"value", &[], sync)
        .unwrap();
    assert!(engine.commit_transaction().is_err());
    assert_eq!(engine.next_lsn(), u64::MAX - 1);
    engine.put_row(1, b"last", b"value", &[], sync).unwrap();
    assert!(engine.put_row(1, b"later", b"value", &[], sync).is_err());
    drop(engine);
    let mut reopened = StorageEngine::open(fs, cfg).unwrap();
    assert_eq!(reopened.next_lsn(), u64::MAX);
    assert_eq!(
        reopened.get_row(1, b"last").unwrap().as_deref(),
        Some(&b"value"[..])
    );
    assert!(reopened.get_row(1, b"rolled-back").unwrap().is_none());
    assert!(reopened.get_row(1, b"later").unwrap().is_none());
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

pub(super) type SyncCountMap = Arc<std::sync::Mutex<HashMap<String, usize>>>;

pub(super) struct CountedHooks {
    sync_counts: SyncCountMap,
    barrier_counts: SyncCountMap,
}

impl crate::pal::test_fs::FsHooks for CountedHooks {
    fn sync_durable(&self, name: &str, inner: &dyn PlatformFile) -> io::Result<()> {
        *self
            .sync_counts
            .lock()
            .unwrap()
            .entry(name.to_owned())
            .or_insert(0) += 1;
        inner.sync_durable()
    }

    fn sync_barrier(&self, name: &str, inner: &dyn PlatformFile) -> io::Result<()> {
        *self
            .barrier_counts
            .lock()
            .unwrap()
            .entry(name.to_owned())
            .or_insert(0) += 1;
        inner.sync_barrier()
    }
}

pub(super) type CountedFs = crate::pal::test_fs::HookedFs<OsFs, CountedHooks>;

pub(super) fn open_counted(dir: &std::path::Path) -> (Arc<CountedFs>, SyncCountMap, SyncCountMap) {
    let sync_counts = Arc::new(std::sync::Mutex::new(HashMap::new()));
    let barrier_counts = Arc::new(std::sync::Mutex::new(HashMap::new()));
    let fs = Arc::new(crate::pal::test_fs::HookedFs::new(
        OsFs::open(dir, true).unwrap(),
        CountedHooks {
            sync_counts: Arc::clone(&sync_counts),
            barrier_counts: Arc::clone(&barrier_counts),
        },
    ));
    (fs, sync_counts, barrier_counts)
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
fn poison_guards_block_zero_record_prepared_tx_savepoints_and_dropped_leader() {
    let tmp = TempTestDir::new("poison_guards_tx_gate");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut engine = StorageEngine::open(fs, EngineConfig::default()).unwrap();

    // Zero-record prepared tx and savepoints must reject after commit leader guard drops.
    engine.begin_transaction_for_conn(1).unwrap();
    engine
        .prepare_commit_transaction(WriteDurability::BufferedFast)
        .unwrap();
    {
        let _guard = engine.commit_gate.leader_guard();
        // Dropped without `disarm()`.
    }
    assert!(engine.commit_transaction().is_err());
    assert!(engine.savepoint_for_conn(1, 1).is_err());
    assert!(engine.statement_savepoint_for_conn(1, 1).is_err());
}

#[test]
fn scoped_raii_guards_clear_on_early_return_and_unwind() {
    // INV-TX-6: Both `ReconcilingCappedGuard` and `RelocatingBlobsGuard` restore their latches on
    // early `?` error returns as well as during panic unwinds.
    let tmp = TempTestDir::new("m1_scoped_raii_guards");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut engine = StorageEngine::open(fs, EngineConfig::default()).unwrap();

    assert_eq!(engine.reconciling_capped_table(), None);
    assert!(!engine.is_relocating_std_blobs());

    fn enter_reconcile_then_err(engine: &mut StorageEngine<OsFs>) -> io::Result<()> {
        let guard = scoped_guards::ReconcilingCappedGuard::enter(engine, 42);
        assert_eq!(guard.reconciling_capped_table(), Some(42));
        if guard.reconciling_capped_table() == Some(42) {
            return Err(io::Error::other("early return"));
        }
        Ok(())
    }

    fn enter_reloc_then_err(engine: &mut StorageEngine<OsFs>) -> io::Result<()> {
        let guard = scoped_guards::RelocatingBlobsGuard::enter(engine);
        assert!(guard.is_relocating_std_blobs());
        if guard.is_relocating_std_blobs() {
            return Err(io::Error::other("early return"));
        }
        Ok(())
    }

    let early_reconcile = enter_reconcile_then_err(&mut engine);
    assert!(early_reconcile.is_err());
    assert_eq!(engine.reconciling_capped_table(), None);

    let early_reloc = enter_reloc_then_err(&mut engine);
    assert!(early_reloc.is_err());
    assert!(!engine.is_relocating_std_blobs());

    let panic_reconcile = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let guard = scoped_guards::ReconcilingCappedGuard::enter(&mut engine, 7);
        assert_eq!(guard.reconciling_capped_table(), Some(7));
        panic!("simulated unwind during reconcile");
    }));
    assert!(panic_reconcile.is_err());
    assert_eq!(engine.reconciling_capped_table(), None);

    let panic_reloc = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let guard = scoped_guards::RelocatingBlobsGuard::enter(&mut engine);
        assert!(guard.is_relocating_std_blobs());
        panic!("simulated unwind during relocation");
    }));
    assert!(panic_reloc.is_err());
    assert!(!engine.is_relocating_std_blobs());
}

#[test]
fn healthy_capability_rejects_all_three_poison_sources_and_preserves_reads() {
    use std::sync::atomic::AtomicBool;

    let assert_rejected_and_readable = |eng: &mut StorageEngine<OsFs>| {
        assert!(eng.is_poisoned());
        assert!(eng.healthy().is_err());
        let lsn_before = eng.next_lsn();
        let gen_before = eng.meta_mgr().state().generation;

        assert!(
            eng.put_row(1, b"blocked", b"v", &[], WriteDurability::SyncTransactional)
                .is_err()
        );
        assert!(
            eng.delete_row(1, b"k1", WriteDurability::SyncTransactional)
                .is_err()
        );
        assert!(eng.flush_memtable().is_err());
        assert!(eng.checkpoint().is_err());
        assert!(eng.prepare_compaction().is_err());
        assert!(eng.register_capped_table(2, 4096).is_err());
        assert!(
            eng.append_to_blob_segment(BlobKind::Standard, 0, b"kb", b"vb")
                .is_err()
        );
        assert_eq!(eng.next_lsn(), lsn_before);
        assert_eq!(eng.meta_mgr().state().generation, gen_before);
        assert_eq!(eng.get_row(1, b"k1").unwrap(), Some(b"v1".to_vec()));
        assert_eq!(
            eng.scan_table(1).unwrap(),
            vec![(b"k1".to_vec(), b"v1".to_vec())]
        );
    };

    // 1. EngineHealth::Poisoned
    {
        let tmp = TempTestDir::new("m2_poison_engine_health");
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
        let mut engine = StorageEngine::open(fs, EngineConfig::default()).unwrap();
        engine
            .put_row(1, b"k1", b"v1", &[], WriteDurability::SyncTransactional)
            .unwrap();
        engine.debug_poison("wal disk fault");
        let err = engine.healthy().err().unwrap().to_string();
        assert!(err.contains("wal disk fault"), "unexpected err: {err}");
        assert_rejected_and_readable(&mut engine);
    }

    // 2. CommitGate failure alone
    {
        let tmp = TempTestDir::new("m2_poison_commit_gate");
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
        let mut engine = StorageEngine::open(fs, EngineConfig::default()).unwrap();
        engine
            .put_row(1, b"k1", b"v1", &[], WriteDurability::SyncTransactional)
            .unwrap();
        engine
            .commit_gate
            .fail(&io::Error::other("group leader EIO"));
        let err = engine.healthy().err().unwrap().to_string();
        assert!(err.contains("group leader EIO"), "unexpected err: {err}");
        assert_rejected_and_readable(&mut engine);
    }

    // 3. BootMetaManager poison alone
    {
        use crate::pal::test_fs::{FsHooks, HookedFs};

        struct MetaWriteFaultHooks {
            fail_meta: Arc<AtomicBool>,
        }
        impl FsHooks for MetaWriteFaultHooks {
            fn write_all_at(
                &self,
                name: &str,
                inner: &dyn PlatformFile,
                buf: &[u8],
                offset: u64,
            ) -> io::Result<()> {
                if name == "boot_meta.db" && self.fail_meta.load(Ordering::SeqCst) {
                    return Err(io::Error::other("injected boot_meta write failure"));
                }
                inner.write_all_at(buf, offset)
            }
        }

        let tmp = TempTestDir::new("m2_poison_meta_mgr");
        let fail_meta = Arc::new(AtomicBool::new(false));
        let fs = Arc::new(HookedFs::new(
            OsFs::open(&tmp.path, true).unwrap(),
            MetaWriteFaultHooks {
                fail_meta: Arc::clone(&fail_meta),
            },
        ));
        let mut engine = StorageEngine::open(fs, EngineConfig::default()).unwrap();
        engine
            .put_row(1, b"k1", b"v1", &[], WriteDurability::SyncTransactional)
            .unwrap();
        fail_meta.store(true, Ordering::SeqCst);
        assert!(engine.flush_memtable().is_err());
        assert!(engine.meta_mgr().is_poisoned());
        assert!(engine.is_poisoned());
        let lsn_before = engine.next_lsn();
        assert!(engine.healthy().is_err());
        assert!(
            engine
                .put_row(1, b"k2", b"v2", &[], WriteDurability::SyncTransactional)
                .is_err()
        );
        assert_eq!(engine.next_lsn(), lsn_before);
        assert_eq!(engine.get_row(1, b"k1").unwrap(), Some(b"v1".to_vec()));
    }
}

#[test]
fn healthy_capability_auto_poisons_on_wal_append_and_sync_errors_while_defer_barrier_unwinds() {
    // 1. WAL append failure via `begin_commit_for_conn` poisons `EngineHealth` while still
    //    restoring `sync_wal.barrier_deferred()` to false.
    {
        let tmp = TempTestDir::new("m2_auto_poison_wal_defer");
        let mut engine = open_group_engine(&tmp);
        engine.begin_transaction_for_conn(1).unwrap();
        engine.put_row(1, b"k1", b"v1", &[], GROUP_SYNC).unwrap();
        engine.prepare_commit_transaction(GROUP_SYNC).unwrap();
        engine.tx_state.active_tx_mut().unwrap().deferred_flush = true;
        engine.debug_fail_next_wal_append();
        assert!(engine.begin_commit_for_conn(1).is_err());
        assert!(engine.is_poisoned());
        assert!(!engine.sync_wal().barrier_deferred());
        assert!(engine.healthy().is_err());
    }

    // 2. Blob sync failure auto-poisons `EngineHealth` inside `HealthyEngine::sync_active_std_writer`.
    {
        use blob_sync_counting::{Ctl, open_fs};

        let tmp = TempTestDir::new("m2_auto_poison_blob_sync");
        let ctl = Arc::new(Ctl::default());
        let fs = open_fs(&tmp.path, &ctl);
        let mut cfg = EngineConfig::default();
        cfg.inline_threshold_bytes = 64;
        let mut engine = StorageEngine::open(fs, cfg).unwrap();
        engine
            .put_row(
                1,
                b"blob_k",
                &[0x44u8; 256],
                &[],
                WriteDurability::BufferedFast,
            )
            .unwrap();
        ctl.fail_blob_sync.store(true, Ordering::SeqCst);
        let err = engine.sync_active_std_writer().unwrap_err();
        assert!(err.to_string().contains("simulated blob sync failure"));
        assert!(engine.is_poisoned());
        assert!(engine.healthy().is_err());
    }
}

#[test]
fn healthy_capability_allows_retryable_buf_flush_without_poisoning() {
    use crate::pal::test_fs::{FsHooks, HookedFs};
    use std::sync::atomic::AtomicBool;

    struct RetryableFlushHooks {
        fail_next_buf_wal: Arc<AtomicBool>,
        fail_next_std_blob: Arc<AtomicBool>,
    }
    impl FsHooks for RetryableFlushHooks {
        fn write_all_at(
            &self,
            name: &str,
            inner: &dyn PlatformFile,
            buf: &[u8],
            offset: u64,
        ) -> io::Result<()> {
            if name == "wal_buf.db" && self.fail_next_buf_wal.swap(false, Ordering::SeqCst) {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "transient buf wal write failure",
                ));
            }
            if name.starts_with("blob_std_")
                && self.fail_next_std_blob.swap(false, Ordering::SeqCst)
            {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "transient blob buffer write failure",
                ));
            }
            inner.write_all_at(buf, offset)
        }
    }

    let tmp = TempTestDir::new("m2_retryable_buf_flush");
    let fail_buf = Arc::new(AtomicBool::new(false));
    let fail_blob = Arc::new(AtomicBool::new(false));
    let fs = Arc::new(HookedFs::new(
        OsFs::open(&tmp.path, true).unwrap(),
        RetryableFlushHooks {
            fail_next_buf_wal: Arc::clone(&fail_buf),
            fail_next_std_blob: Arc::clone(&fail_blob),
        },
    ));
    let mut cfg = EngineConfig::default();
    cfg.inline_threshold_bytes = 64;
    cfg.buf_wal_mode = WalStreamMode::BufferedUserSpaceCoalesced;
    let mut engine = StorageEngine::open(fs, cfg).unwrap();

    // Put a small buffered row so `wal_buf.db` holds user-space buffered data.
    engine
        .put_row(1, b"buf_k", b"v", &[], WriteDurability::BufferedFast)
        .unwrap();
    fail_buf.store(true, Ordering::SeqCst);
    let err = engine.healthy().unwrap().flush_buf_wal_to_os().unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::PermissionDenied);
    assert!(
        !engine.is_poisoned(),
        "failed buf flush must stay retryable"
    );
    assert!(engine.healthy().is_ok());
    engine.healthy().unwrap().flush_buf_wal_to_os().unwrap();

    // Put a buffered blob inside an open transaction so `active_std_writer` holds user-space
    // buffered data; failing its buffer flush before `sync_durable` must remain retryable
    // without poisoning.
    engine.begin_transaction();
    engine
        .put_row(
            1,
            b"blob_k",
            &[0x33u8; 256],
            &[],
            WriteDurability::BufferedFast,
        )
        .unwrap();
    fail_blob.store(true, Ordering::SeqCst);
    let blob_err = engine.sync_active_std_writer().unwrap_err();
    assert_eq!(blob_err.kind(), io::ErrorKind::PermissionDenied);
    assert!(
        !engine.is_poisoned(),
        "failed blob buffer flush before sync must stay retryable"
    );
    assert!(engine.healthy().is_ok());
    engine.sync_active_std_writer().unwrap();
    engine.commit_transaction().unwrap();
}

pub(super) fn run_live_2pc_poison_trace(prefix: &str) -> tx_fsm::TxTraceRecorder {
    let tmp = TempTestDir::new(prefix);
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut engine = StorageEngine::open(fs, EngineConfig::default()).unwrap();
    // Register a capped table before tracing so `begin_commit_for_conn` takes the non-groupable
    // `AwaitingSync` -> `finish_commit_for_conn` two-phase commit path.
    engine.register_capped_table(99, 65_536).unwrap();

    engine.enable_tx_trace();

    // 1. Multi-vtab 2PC transaction with savepoint, rollback-to, release, InternalBypass write,
    //    SyncTransactional upgrade, and clean `AwaitingSync` completion.
    let conn1 = 0x7000_0001_usize;
    let vtab1 = 0x8000_0001_usize;
    let vtab2 = 0x8000_0002_usize;
    engine.begin_vtab_transaction(conn1, vtab1).unwrap();
    engine.begin_vtab_transaction(conn1, vtab2).unwrap();
    engine.savepoint_for_conn(conn1, 0).unwrap();
    engine
        .put_row(1, b"rolled", b"v_drop", &[], WriteDurability::BufferedFast)
        .unwrap();
    engine.rollback_to_savepoint_for_conn(conn1, 0).unwrap();
    engine
        .put_row(1, b"kept", b"v_ok", &[], WriteDurability::SyncTransactional)
        .unwrap();
    engine.release_savepoint_for_conn(conn1, 0).unwrap();

    assert!(!engine.mark_vtab_synced_ready_to_commit(vtab1, WriteDurability::SyncTransactional));
    assert!(engine.mark_vtab_synced_ready_to_commit(vtab2, WriteDurability::BufferedFast));
    engine
        .prepare_commit_transaction(WriteDurability::BufferedFast)
        .unwrap();
    engine
        .commit_wal_and_memtable_core(
            vec![crate::wal::WalRecord::PutInline {
                key: crate::lru::encode_primary_row_key(1, b"bypass"),
                value: b"internal".to_vec(),
            }],
            WriteDurability::BufferedFast,
            true,
            false,
            crate::engine::tx::WriteScope::InternalBypass,
        )
        .unwrap();
    let crate::engine::tx::CommitStep::AwaitingSync(file) =
        engine.begin_commit_for_conn(conn1).unwrap()
    else {
        panic!("expected non-groupable AwaitingSync commit step");
    };
    engine.finish_commit_for_conn(file.sync_barrier()).unwrap();
    assert_eq!(engine.get_row(1, b"rolled").unwrap(), None);
    assert_eq!(engine.get_row(1, b"kept").unwrap(), Some(b"v_ok".to_vec()));
    assert_eq!(
        engine.get_row(1, b"bypass").unwrap(),
        Some(b"internal".to_vec())
    );

    // 2. Second transaction failing `finish_commit_for_conn` poisons the engine, then a foreign
    //    connection attempt is rejected with Poisoned.
    let conn2 = 0x7000_0002_usize;
    engine.begin_transaction_for_conn(conn2).unwrap();
    engine
        .put_row(
            1,
            b"doomed",
            b"v_fail",
            &[],
            WriteDurability::SyncTransactional,
        )
        .unwrap();
    engine
        .prepare_commit_transaction(WriteDurability::SyncTransactional)
        .unwrap();
    let crate::engine::tx::CommitStep::AwaitingSync(_) =
        engine.begin_commit_for_conn(conn2).unwrap()
    else {
        panic!("expected non-groupable AwaitingSync commit step");
    };
    assert!(
        engine
            .finish_commit_for_conn(Err(io::Error::other("simulated barrier failure")))
            .is_err()
    );
    assert!(engine.is_poisoned());
    assert!(engine.begin_transaction_for_conn(0x7000_0003).is_err());

    assert!(!engine.tx_trace().unwrap().is_empty());
    assert!(engine.tx_trace().unwrap().len() >= 18);
    let trace = engine.take_tx_trace().unwrap();
    assert!(engine.tx_trace().is_none());
    trace
}

#[test]
fn tx_fsm_trace_replay_matches_live_engine_2pc_savepoints_and_poison() {
    let trace = run_live_2pc_poison_trace("m3_tx_fsm_live_trace");
    trace
        .replay_verify()
        .expect("live engine trace must replay");

    let normalized = trace.normalize_ids();
    normalized
        .replay_verify()
        .expect("normalized trace must replay");
    let ndjson = normalized.to_ndjson();
    let lines: Vec<&str> = ndjson.lines().collect();
    assert_eq!(
        lines[0],
        "{\"kind\":\"header\",\"schema\":1,\"fsm\":\"tx_fsm\"}"
    );
    assert!(ndjson.contains("\"tag\":\"BeginVtab\",\"conn_id\":1,\"vtab_id\":1"));
    assert!(ndjson.contains("\"tag\":\"BeginVtab\",\"conn_id\":1,\"vtab_id\":2"));
    assert!(ndjson.contains(
        "\"tag\":\"Savepoint\",\"owner_conn\":1,\"sp_id\":0,\"is_statement\":false,\"check_poison\":true"
    ));
    assert!(ndjson.contains("\"tag\":\"RollbackToSavepoint\",\"conn_id\":1,\"sp_id\":0"));
    assert!(ndjson.contains("\"tag\":\"ReleaseSavepoint\",\"conn_id\":1,\"sp_id\":0"));
    assert!(
        ndjson.contains("\"tag\":\"VtabSync\",\"vtab_id\":1,\"durability\":\"SyncTransactional\"")
    );
    assert!(ndjson.contains("\"tag\":\"BeginPrepare\",\"durability\":\"BufferedFast\""));
    assert!(ndjson.contains("\"tag\":\"FinishPrepare\",\"ok\":true"));
    assert!(ndjson.contains("\"scope\":\"InternalBypass\""));
    assert!(ndjson.contains("\"tag\":\"EnterWalFlush\",\"defer_sync_barrier\":true"));
    assert!(ndjson.contains("\"tag\":\"CompleteWalFlush\",\"mode\":\"AwaitSync\""));
    assert!(ndjson.contains("\"tag\":\"FinishSyncCommit\",\"ok\":true"));
    assert!(ndjson.contains("\"tag\":\"FinishSyncCommit\",\"ok\":false"));
    assert!(ndjson.contains(
        "\"event\":{\"tag\":\"BeginForConn\",\"conn_id\":3},\"post\":{\"health\":\"Poisoned\",\"state\":{\"tag\":\"Idle\"}},\"outcome\":{\"status\":\"Err\",\"error\":{\"tag\":\"Poisoned\"}}"
    ));
}

pub(super) fn maybe_write_formal_trace(filename: &str, ndjson: &str) {
    let Some(dir) = std::env::var_os("AYDB_EXPORT_TRACES_DIR") else {
        return;
    };
    if dir.is_empty() {
        return;
    }
    let dir_path = std::path::Path::new(&dir);
    std::fs::create_dir_all(dir_path).expect("create AYDB_EXPORT_TRACES_DIR");
    std::fs::write(dir_path.join(filename), ndjson).expect("write formal trace fixture");
}

#[test]
fn export_formal_traces_tx_fsm_scenarios() {
    let trace1 = run_live_2pc_poison_trace("m4_trace_2pc_poison");
    trace1.replay_verify().unwrap();
    let norm1 = trace1.normalize_ids();
    norm1.replay_verify().unwrap();
    let ndjson1 = norm1.to_ndjson();
    maybe_write_formal_trace("tx_fsm_engine_2pc_poison.ndjson", &ndjson1);

    // Scenario 2: Group commit handoff, TxBudgetExceeded, Busy foreign conn, UnknownSavepoint,
    // interrupted SyncInFlight rollback poisoning, and post-poison absorption.
    let tmp = TempTestDir::new("m4_trace_group_errors");
    let mut engine = open_group_engine(&tmp);
    engine.config.max_transaction_bytes = 512;
    engine.enable_tx_trace();

    let conn1 = 0x7100_0001_usize;
    let conn2 = 0x7100_0002_usize;

    // 2a. Clean group commit handoff (`HandOffGroup`).
    let ticket = commit_into_group(&mut engine, conn1, b"g1", b"val1");
    let _target = flush_group(&mut engine, &ticket);

    // 2b. Budget reject (`TxBudgetExceeded`), foreign `Busy`, `UnknownSavepoint(99)`.
    engine.begin_transaction_for_conn(conn1).unwrap();
    engine.savepoint_for_conn(conn1, 0).unwrap();
    engine
        .put_row(1, b"ok1", b"small", &[], GROUP_SYNC)
        .unwrap();
    let budget_err = engine
        .commit_wal_and_memtable_core(
            vec![crate::wal::WalRecord::PutInline {
                key: crate::lru::encode_primary_row_key(1, b"too_big"),
                value: vec![0x41u8; 600],
            }],
            GROUP_SYNC,
            true,
            false,
            crate::engine::tx::WriteScope::Normal,
        )
        .unwrap_err();
    assert_eq!(budget_err.kind(), io::ErrorKind::InvalidInput);
    let busy_err = engine.begin_transaction_for_conn(conn2).unwrap_err();
    assert_eq!(busy_err.kind(), io::ErrorKind::WouldBlock);
    let unknown_sp_err = engine
        .rollback_to_savepoint_for_conn(conn1, 99)
        .unwrap_err();
    assert_eq!(unknown_sp_err.kind(), io::ErrorKind::InvalidInput);
    engine.release_savepoint_for_conn(conn1, 0).unwrap();
    engine.rollback_transaction_for_conn(conn1).unwrap();

    // 2c. Interrupted `SyncInFlight` rollback triggers `PoisonEngineInterruptedSync`, followed by
    //     post-poison rejection.
    engine.register_capped_table(99, 65_536).unwrap();
    engine.begin_transaction_for_conn(conn1).unwrap();
    engine
        .put_row(
            1,
            b"sync_k",
            &[0x22u8; 64],
            &[],
            WriteDurability::SyncTransactional,
        )
        .unwrap();
    engine
        .prepare_commit_transaction(WriteDurability::SyncTransactional)
        .unwrap();
    let step = engine.begin_commit_for_conn(conn1).unwrap();
    assert!(matches!(step, CommitStep::AwaitingSync(_)));
    engine.rollback_transaction().unwrap();
    assert!(engine.is_poisoned());
    assert!(engine.begin_transaction_for_conn(conn2).is_err());

    let trace2 = engine.take_tx_trace().unwrap();
    trace2.replay_verify().unwrap();
    let norm2 = trace2.normalize_ids();
    norm2.replay_verify().unwrap();
    let ndjson2 = norm2.to_ndjson();
    assert!(ndjson2.contains("\"mode\":\"HandOffGroup\""));
    assert!(ndjson2.contains("\"error\":{\"tag\":\"TxBudgetExceeded\"}"));
    assert!(ndjson2.contains("\"error\":{\"tag\":\"Busy\"}"));
    assert!(ndjson2.contains("\"error\":{\"tag\":\"UnknownSavepoint\",\"sp_id\":99}"));
    assert!(ndjson2.contains("\"tag\":\"PoisonEngineInterruptedSync\""));
    assert!(ndjson2.contains("\"error\":{\"tag\":\"Poisoned\"}"));
    maybe_write_formal_trace("tx_fsm_engine_group_and_errors.ndjson", &ndjson2);
}
