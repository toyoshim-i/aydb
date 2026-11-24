//! Reads must keep working on a poisoned engine, also those that would write on their own:
//! with `LruReadTrackingMode::EagerWriteOnRead` a read of a capped table queues an LRU
//! promotion, which is a WAL write.

use super::*;
use crate::pal::{OpenHint, OsFs, PlatformFile, PlatformFs, PreallocStrategy};
use crate::util::test_util::TempTestDir;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

struct FaultFile<F> {
    inner: Arc<F>,
    is_wal: bool,
    is_blob: bool,
    fail_next_wal_write: Arc<AtomicBool>,
    fail_next_blob_write: Arc<AtomicBool>,
}

impl<F: PlatformFile> PlatformFile for FaultFile<F> {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        self.inner.read_at(buf, offset)
    }
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        self.inner.read_exact_at(buf, offset)
    }
    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        if self.is_wal && self.fail_next_wal_write.swap(false, Ordering::SeqCst) {
            return Err(io::Error::other("simulated WAL write failure"));
        }
        if self.is_blob && self.fail_next_blob_write.swap(false, Ordering::SeqCst) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "simulated blob flush failure",
            ));
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
        self.inner.sync_durable()
    }
    fn evict_os_cache(&self, offset: u64, len: u64) -> io::Result<()> {
        self.inner.evict_os_cache(offset, len)
    }
}

struct FaultFs {
    inner: OsFs,
    fail_next_wal_write: Arc<AtomicBool>,
    fail_next_blob_write: Arc<AtomicBool>,
}

impl PlatformFs for FaultFs {
    type File = FaultFile<<OsFs as PlatformFs>::File>;

    fn open_file(
        &self,
        rel_name: &str,
        create: bool,
        writable: bool,
        hint: OpenHint,
    ) -> io::Result<Arc<Self::File>> {
        let inner = self.inner.open_file(rel_name, create, writable, hint)?;
        Ok(Arc::new(FaultFile {
            inner,
            is_wal: rel_name == "wal_sync.db" || rel_name == "wal_buf.db",
            is_blob: rel_name.starts_with("blob_"),
            fail_next_wal_write: Arc::clone(&self.fail_next_wal_write),
            fail_next_blob_write: Arc::clone(&self.fail_next_blob_write),
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

const UNCAPPED: u16 = 1;
const CAPPED: u16 = 7;
const CAPPED_BYTES: u64 = 20_000;

fn config() -> EngineConfig {
    EngineConfig {
        lru_read_tracking: LruReadTrackingMode::EagerWriteOnRead,
        ..EngineConfig::default()
    }
}

fn open_with_blob_flag(
    dir: &TempTestDir,
    wal_flag: &Arc<AtomicBool>,
    blob_flag: &Arc<AtomicBool>,
) -> (Arc<FaultFs>, StorageEngine<FaultFs>) {
    let fs = Arc::new(FaultFs {
        inner: OsFs::open(&dir.path, true).unwrap(),
        fail_next_wal_write: Arc::clone(wal_flag),
        fail_next_blob_write: Arc::clone(blob_flag),
    });
    let engine = StorageEngine::open(Arc::clone(&fs), config()).unwrap();
    (fs, engine)
}

fn open(dir: &TempTestDir, flag: &Arc<AtomicBool>) -> (Arc<FaultFs>, StorageEngine<FaultFs>) {
    open_with_blob_flag(dir, flag, &Arc::new(AtomicBool::new(false)))
}

fn seed(engine: &mut StorageEngine<FaultFs>) {
    engine.register_capped_table(CAPPED, CAPPED_BYTES).unwrap();
    for table in [UNCAPPED, CAPPED] {
        for k in 0..5u8 {
            engine
                .put_row(
                    table,
                    &[b'k', k],
                    &[b'v'; 100],
                    &[],
                    WriteDurability::BufferedFast,
                )
                .unwrap();
        }
    }
}

/// Every read API that applies queued promotions afterwards.
fn read_everything(engine: &mut StorageEngine<FaultFs>, table: u16) {
    for k in 0..5u8 {
        let key = [b'k', k];
        assert_eq!(
            engine.get_row(table, &key).unwrap(),
            Some(vec![b'v'; 100]),
            "get_row {table} {k}"
        );
        let mut slab = Vec::new();
        let mut spans = Vec::new();
        assert!(
            engine
                .get_row_into(table, &key, &mut slab, &mut spans)
                .unwrap()
        );
        assert_eq!(slab, vec![b'v'; 100]);
    }
    assert_eq!(
        engine
            .scan_keys_range(table, None, None, 100)
            .unwrap()
            .len(),
        5
    );
}

#[test]
fn reads_with_eager_promotion_keep_working_on_a_poisoned_engine() {
    let dir = TempTestDir::new("read_poison_api");
    let flag = Arc::new(AtomicBool::new(false));
    let (fs, mut engine) = open(&dir, &flag);
    seed(&mut engine);
    // A failed write poisons the engine.
    flag.store(true, Ordering::SeqCst);
    assert!(
        engine
            .put_row(UNCAPPED, b"lost", b"x", &[], WriteDurability::BufferedFast)
            .is_err()
    );
    assert!(engine.is_poisoned());
    for table in [UNCAPPED, CAPPED] {
        read_everything(&mut engine, table);
    }
    assert!(engine.get_row(UNCAPPED, b"lost").unwrap().is_none());
    drop(engine);

    // Nothing the reads skipped is missing after a reopen: every acknowledged row is there, the
    // LRU state is consistent and the capped table still evicts and accepts writes.
    let mut engine = StorageEngine::open(fs, config()).unwrap();
    assert!(!engine.is_poisoned());
    for table in [UNCAPPED, CAPPED] {
        read_everything(&mut engine, table);
    }
    for k in 5..200u8 {
        engine
            .put_row(
                CAPPED,
                &[b'k', k],
                &[b'w'; 200],
                &[],
                WriteDurability::BufferedFast,
            )
            .unwrap();
    }
    let kept = engine
        .scan_keys_range(CAPPED, None, None, 1000)
        .unwrap()
        .len();
    assert!(kept > 0 && kept < 195, "{kept}");
}

#[test]
fn a_read_whose_promotion_write_fails_still_returns_its_row() {
    let dir = TempTestDir::new("read_poison_promotion");
    let flag = Arc::new(AtomicBool::new(false));
    let (fs, mut engine) = open(&dir, &flag);
    seed(&mut engine);
    // The promotion queued by this read is the next WAL write, and it fails.
    flag.store(true, Ordering::SeqCst);
    assert_eq!(
        engine.get_row(CAPPED, &[b'k', 2]).unwrap(),
        Some(vec![b'v'; 100])
    );
    // The failed write poisoned the engine, as any failed WAL write does; reads go on, and
    // the non-poisoned promotion failure was recorded once.
    assert!(engine.is_poisoned());
    assert_eq!(engine.maintenance_failures(), 1);
    assert!(engine.last_maintenance_error().is_some());
    read_everything(&mut engine, CAPPED);
    assert_eq!(engine.maintenance_failures(), 1);
    drop(engine);
    let mut engine = StorageEngine::open(fs, config()).unwrap();
    assert!(!engine.is_poisoned());
    read_everything(&mut engine, CAPPED);
}

#[test]
fn eager_promotion_does_not_mutate_active_transaction_or_budget() {
    let dir = TempTestDir::new("read_eager_active_tx");
    let flag = Arc::new(AtomicBool::new(false));
    let fs = Arc::new(FaultFs {
        inner: OsFs::open(&dir.path, true).unwrap(),
        fail_next_wal_write: Arc::clone(&flag),
        fail_next_blob_write: Arc::new(AtomicBool::new(false)),
    });
    let mut cfg = config();
    cfg.max_transaction_bytes = 512;
    let mut engine = StorageEngine::open(fs, cfg).unwrap();
    seed(&mut engine);

    engine.begin_transaction_for_conn(1).unwrap();
    engine.savepoint_for_conn(1, 1).unwrap();
    engine
        .put_row(
            UNCAPPED,
            b"tx_k",
            b"tx_v",
            &[],
            WriteDurability::BufferedFast,
        )
        .unwrap();

    let (rec_count, tx_bytes, overlay_len, undo_len) = {
        let tx = engine.tx_state.active_tx().expect("active tx");
        (
            tx.pending_record_count,
            tx.pending_bytes,
            tx.overlay.map.len(),
            tx.overlay.undo.len(),
        )
    };
    let epoch = engine.mutation_epoch;

    for _ in 0..20 {
        for k in 0..5u8 {
            assert_eq!(
                engine.get_row(CAPPED, &[b'k', k]).unwrap(),
                Some(vec![b'v'; 100])
            );
        }
    }
    let tx = engine.tx_state.active_tx().expect("active tx");
    assert_eq!(tx.pending_record_count, rec_count);
    assert_eq!(tx.pending_bytes, tx_bytes);
    assert_eq!(engine.mutation_epoch, epoch);
    assert_eq!(tx.overlay.map.len(), overlay_len);
    assert_eq!(tx.overlay.undo.len(), undo_len);

    engine.rollback_to_savepoint_for_conn(1, 1).unwrap();
    assert_eq!(engine.get_row(UNCAPPED, b"tx_k").unwrap(), None);
    engine.commit_transaction_for_conn(1).unwrap();
}

#[test]
fn shared_reader_scan_and_poisoned_scan_succeed_after_rolled_back_blob_write() {
    let dir = TempTestDir::new("read_unflushed_rolled_back_blob");
    let flag = Arc::new(AtomicBool::new(false));
    let fs = Arc::new(FaultFs {
        inner: OsFs::open(&dir.path, true).unwrap(),
        fail_next_wal_write: Arc::clone(&flag),
        fail_next_blob_write: Arc::new(AtomicBool::new(false)),
    });
    let mut cfg = config();
    cfg.inline_threshold_bytes = 64;
    let mut engine = StorageEngine::open(fs, cfg).unwrap();

    seed(&mut engine);
    engine
        .put_row(
            UNCAPPED,
            b"committed_blob",
            &[0x33u8; 256],
            &[],
            WriteDurability::SyncTransactional,
        )
        .unwrap();

    // Appending a large blob inside an uncommitted transaction leaves buffered bytes inside the
    // active blob writer even after rollback_transaction() unwinds the overlay.
    engine.begin_transaction();
    engine
        .put_row(
            UNCAPPED,
            b"aborted_blob",
            &[0x77u8; 256],
            &[],
            WriteDurability::BufferedFast,
        )
        .unwrap();
    assert!(engine.any_blob_unflushed());
    engine.rollback_transaction().unwrap();
    assert!(!engine.in_active_transaction());
    assert!(!engine.tx_state.is_in_tx());
    assert!(engine.any_blob_unflushed());

    let verify_scans = |eng: &mut StorageEngine<FaultFs>| {
        let mut pks = Vec::new();
        let mut slab = Vec::new();
        let mut spans = Vec::new();
        let mut lazy = Vec::new();
        eng.reader_for_conn(1)
            .scan_range_into_slab(
                UNCAPPED,
                None,
                None,
                false,
                100,
                false,
                &mut pks,
                &mut slab,
                &mut spans,
                Some(&mut lazy),
            )
            .expect("shared &self owner-view scan must not fail with NEED_FLUSH after rollback");
        assert!(pks.iter().any(|k| k.as_slice() == b"committed_blob"));
        assert!(!pks.iter().any(|k| k.as_slice() == b"aborted_blob"));
        assert_eq!(pks.len(), 6);
        assert_eq!(spans.len(), 6);
        assert_eq!(lazy.len(), 6);

        // Also exercise the eager-value slab path (`lazy_blobs_out = None`).
        let mut eager_pks = Vec::new();
        let mut eager_slab = Vec::new();
        let mut eager_spans = Vec::new();
        eng.reader_for_conn(1)
            .scan_range_into_slab(
                UNCAPPED,
                None,
                None,
                false,
                100,
                false,
                &mut eager_pks,
                &mut eager_slab,
                &mut eager_spans,
                None,
            )
            .expect("shared &self eager-value scan must not fail with NEED_FLUSH after rollback");
        assert_eq!(eager_spans.len(), 6);
        assert!(
            eager_spans
                .iter()
                .any(|&(s, e)| eager_slab[s..e] == [0x33u8; 256])
        );

        let rows = eng.scan_table(UNCAPPED).unwrap();
        assert_eq!(rows.len(), 6);
        assert!(
            rows.iter()
                .any(|(k, v)| k.as_slice() == b"committed_blob" && v == &[0x33u8; 256])
        );
    };

    verify_scans(&mut engine);

    // Poisoning the engine must not break scans either, even while the rolled-back bytes remain
    // unflushed in the active blob writer buffer.
    flag.store(true, Ordering::SeqCst);
    assert!(
        engine
            .put_row(
                UNCAPPED,
                b"fail",
                b"x",
                &[],
                WriteDurability::SyncTransactional
            )
            .is_err()
    );
    assert!(engine.is_poisoned());
    verify_scans(&mut engine);
}

#[test]
fn with_committed_read_for_conn_propagates_blob_flush_error() {
    let dir = TempTestDir::new("with_committed_read_flush_err");
    let wal_flag = Arc::new(AtomicBool::new(false));
    let blob_flag = Arc::new(AtomicBool::new(false));
    let (_fs, mut engine) = open_with_blob_flag(&dir, &wal_flag, &blob_flag);

    engine.begin_transaction_for_conn(1).unwrap();
    engine
        .put_row(
            UNCAPPED,
            b"buffered_owner_blob",
            &vec![0x55u8; 2048],
            &[],
            WriteDurability::BufferedFast,
        )
        .unwrap();
    assert!(engine.any_blob_unflushed());

    blob_flag.store(true, Ordering::SeqCst);
    let err = engine
        .with_committed_read_for_conn(1, |r| r.get_row(UNCAPPED, b"buffered_owner_blob"))
        .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::PermissionDenied);
    assert!(
        err.to_string().contains("simulated blob flush failure"),
        "expected injected flush error, got: {err}"
    );
    assert!(!engine.is_poisoned());

    // `try_with_committed_read_for_conn` likewise propagates flush errors and recovers once I/O clears.
    blob_flag.store(true, Ordering::SeqCst);
    let err2 = engine
        .try_with_committed_read_for_conn(1, |r| r.get_row(UNCAPPED, b"buffered_owner_blob"))
        .unwrap_err();
    assert_eq!(err2.kind(), io::ErrorKind::PermissionDenied);

    let got = engine
        .try_with_committed_read_for_conn(1, |r| r.get_row(UNCAPPED, b"buffered_owner_blob"))
        .unwrap()
        .unwrap();
    assert_eq!(got, Some(vec![0x55u8; 2048]));
}
