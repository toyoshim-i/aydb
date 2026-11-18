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
    fail_next_wal_write: Arc<AtomicBool>,
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
            fail_next_wal_write: Arc::clone(&self.fail_next_wal_write),
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

fn open(dir: &TempTestDir, flag: &Arc<AtomicBool>) -> (Arc<FaultFs>, StorageEngine<FaultFs>) {
    let fs = Arc::new(FaultFs {
        inner: OsFs::open(&dir.path, true).unwrap(),
        fail_next_wal_write: Arc::clone(flag),
    });
    let engine = StorageEngine::open(Arc::clone(&fs), config()).unwrap();
    (fs, engine)
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
    // The failed write poisoned the engine, as any failed WAL write does; reads go on.
    assert!(engine.is_poisoned());
    read_everything(&mut engine, CAPPED);
    drop(engine);
    let mut engine = StorageEngine::open(fs, config()).unwrap();
    assert!(!engine.is_poisoned());
    read_everything(&mut engine, CAPPED);
}
