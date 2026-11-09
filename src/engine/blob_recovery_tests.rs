//! Blob segment creation that fails after it took effect.
//!
//! A creation can report an error although the file exists (the directory entry is written but
//! not yet flushed). The retry must not reuse that name (ARCHITECTURE.md section 2.3): opening
//! an existing file never makes its directory entry durable, so a power loss would drop the
//! file although a later, acknowledged WAL batch references it.

use super::*;
use crate::pal::{OsFile, OsFs};
use crate::util::test_util::TempTestDir;
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;

/// `OsFs` with two additions: the next creation of a file whose name starts with `fail_prefix`
/// creates it and then reports an error, and every file created that way is remembered as one
/// whose directory entry is not durable (`volatile`) until it is removed.
struct VolatileEntryFs {
    inner: OsFs,
    fail_next_create: AtomicBool,
    fail_prefix: &'static str,
    volatile: Mutex<BTreeSet<String>>,
}

impl PlatformFs for VolatileEntryFs {
    type File = OsFile;

    fn open_file(
        &self,
        name: &str,
        create: bool,
        writable: bool,
        hint: OpenHint,
    ) -> io::Result<Arc<Self::File>> {
        let exists = self.inner.open_file(name, false, false, hint).is_ok();
        if create
            && !exists
            && name.starts_with(self.fail_prefix)
            && self.fail_next_create.swap(false, Ordering::SeqCst)
        {
            self.inner.open_file(name, true, writable, hint)?;
            self.volatile.lock().unwrap().insert(name.to_string());
            return Err(io::Error::other("injected failure after file creation"));
        }
        self.inner.open_file(name, create, writable, hint)
    }

    fn remove_file(&self, name: &str) -> io::Result<()> {
        self.volatile.lock().unwrap().remove(name);
        self.inner.remove_file(name)
    }

    fn list_files(&self) -> io::Result<Vec<String>> {
        self.inner.list_files()
    }

    fn root_path(&self) -> &Path {
        self.inner.root_path()
    }
}

fn open_fs(dir: &Path, fail_prefix: &'static str) -> Arc<VolatileEntryFs> {
    Arc::new(VolatileEntryFs {
        inner: OsFs::open(dir, true).unwrap(),
        fail_next_create: AtomicBool::new(false),
        fail_prefix,
        volatile: Mutex::new(BTreeSet::new()),
    })
}

/// A power loss as far as directory entries go: files created by an operation that reported
/// failure lose their entry, everything else stays.
fn lose_volatile_entries(fs: &VolatileEntryFs) {
    for name in fs.volatile.lock().unwrap().iter() {
        let _ = std::fs::remove_file(fs.root_path().join(name));
    }
}

fn failed_create_then_retry_keeps_acknowledged_row(fail_prefix: &'static str, table_id: u16) {
    let tmp = TempTestDir::new("blob_failed_create");
    let fs = open_fs(&tmp.path, fail_prefix);
    let config = EngineConfig::default();
    let mut engine = StorageEngine::open(Arc::clone(&fs), config.clone()).unwrap();
    if table_id != 0 {
        engine.register_capped_table(table_id, 1 << 20).unwrap();
    }
    let table = if table_id == 0 { 1 } else { table_id };
    let value = vec![0x5Au8; 8000];
    let sync = WriteDurability::SyncTransactional;

    fs.fail_next_create.store(true, Ordering::SeqCst);
    assert!(engine.put_row(table, b"lost", &value, &[], sync).is_err());
    assert!(!engine.is_poisoned());
    assert_eq!(fs.volatile.lock().unwrap().len(), 1);

    engine.put_row(table, b"acked", &value, &[], sync).unwrap();
    drop(engine);

    lose_volatile_entries(&fs);
    let mut reopened = StorageEngine::open(open_fs(&tmp.path, fail_prefix), config).unwrap();
    assert_eq!(reopened.get_row(table, b"acked").unwrap(), Some(value));
    assert_eq!(reopened.get_row(table, b"lost").unwrap(), None);
}

#[test]
fn failed_standard_blob_create_does_not_reuse_the_name() {
    failed_create_then_retry_keeps_acknowledged_row("blob_std_", 0);
}

#[test]
fn failed_lru_blob_create_does_not_reuse_the_name() {
    failed_create_then_retry_keeps_acknowledged_row("blob_lru_", 7);
}

// ---------------------------------------------------------------------------------------------
// Dead-byte accounting across recovery
// ---------------------------------------------------------------------------------------------

fn lru_segment_file(dir: &Path) -> Option<std::path::PathBuf> {
    let mut found: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("blob_lru_"))
        })
        .collect();
    found.sort();
    found.into_iter().next()
}

fn open_plain(dir: &Path, config: &EngineConfig) -> StorageEngine<OsFs> {
    StorageEngine::open(Arc::new(OsFs::open(dir, true).unwrap()), config.clone()).unwrap()
}

/// A segment whose catalog entry already counts its last blob as dead, while the file lost
/// that blob (its bytes were never synced), must not count those bytes dead a second time at
/// each reopen: that made a segment with a live blob look fully dead and deleted it.
#[test]
fn reopen_does_not_count_a_cataloged_but_missing_tail_as_dead_again() {
    let tmp = TempTestDir::new("blob_missing_tail");
    let config = EngineConfig::default();
    let sync = WriteDurability::SyncTransactional;
    let mut engine = open_plain(&tmp.path, &config);
    engine.register_capped_table(7, 1 << 20).unwrap();
    // One segment: A (dead after the overwrite), B (live), C (dead after the delete). C is the
    // biggest, so counting it twice would make the dead total reach the file length.
    engine.put_row(7, b"a", &[1u8; 3000], &[], sync).unwrap();
    engine.put_row(7, b"b", &[2u8; 3000], &[], sync).unwrap();
    engine.put_row(7, b"c", &[3u8; 4000], &[], sync).unwrap();
    engine.put_row(7, b"a", b"inline", &[], sync).unwrap();
    engine.delete_row(7, b"c", sync).unwrap();
    engine.flush_memtable().unwrap();
    drop(engine);

    // Power loss: the tail holding C never reached the device although the catalog lists it.
    let segment = lru_segment_file(&tmp.path).unwrap();
    let len = std::fs::metadata(&segment).unwrap().len();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&segment)
        .unwrap()
        .set_len(len - 4000)
        .unwrap();

    // Several recoveries without a checkpoint in between must give the same result.
    for _ in 0..3 {
        drop(open_plain(&tmp.path, &config));
    }
    let mut engine = open_plain(&tmp.path, &config);
    engine.put_row(7, b"d", b"inline", &[], sync).unwrap();
    engine.flush_memtable().unwrap();
    engine.checkpoint().unwrap();
    assert!(segment.exists(), "a segment with a live blob was reclaimed");
    drop(engine);

    let mut engine = open_plain(&tmp.path, &config);
    assert_eq!(engine.get_row(7, b"b").unwrap(), Some(vec![2u8; 3000]));
}

/// Bytes beyond the cataloged length that no replayed batch references are garbage and still
/// count as dead, so a segment of dead blobs is reclaimed.
#[test]
fn a_fully_dead_segment_with_a_garbage_tail_is_still_reclaimed() {
    let tmp = TempTestDir::new("blob_dead_segment");
    let mut config = EngineConfig::default();
    config.max_blob_segment_bytes = 4000;
    let sync = WriteDurability::SyncTransactional;
    let mut engine = open_plain(&tmp.path, &config);
    engine.register_capped_table(7, 1 << 20).unwrap();
    engine.put_row(7, b"x", &[1u8; 3000], &[], sync).unwrap();
    // Rolls to a second segment, sealing the first.
    engine.put_row(7, b"y", &[2u8; 3000], &[], sync).unwrap();
    engine.flush_memtable().unwrap();
    drop(engine);

    let first = lru_segment_file(&tmp.path).unwrap();
    {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&first)
            .unwrap();
        f.write_all(&[0xEE; 500]).unwrap();
    }

    let mut engine = open_plain(&tmp.path, &config);
    engine.delete_row(7, b"x", sync).unwrap();
    for i in 0..4u8 {
        engine.put_row(7, &[b'k', i], b"inline", &[], sync).unwrap();
        engine.flush_memtable().unwrap();
    }
    engine.checkpoint().unwrap();
    assert!(
        !first.exists(),
        "the fully dead segment was never reclaimed"
    );
    assert_eq!(engine.get_row(7, b"y").unwrap(), Some(vec![2u8; 3000]));
}
