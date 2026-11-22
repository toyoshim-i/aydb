//! Segment file creation that fails after it took effect, and blob accounting at recovery.
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

#[test]
fn recovery_rejects_an_intact_blob_bound_to_another_row() {
    let tmp = TempTestDir::new("recovery_blob_wrong_key");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let cfg = EngineConfig::default();
    let mut engine = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
    let first_key = encode_primary_row_key(1, b"first");
    let ptr = engine
        .append_to_blob_segment(BlobKind::Standard, 0, &first_key, b"value")
        .unwrap();
    engine.sync_active_std_writer().unwrap();
    let sync = WriteDurability::SyncTransactional;
    engine
        .commit_wal_and_memtable(
            vec![WalRecord::PutBlobPtr {
                key: first_key,
                blob_ptr: ptr,
            }],
            sync,
        )
        .unwrap();
    engine
        .commit_wal_and_memtable(
            vec![WalRecord::PutBlobPtr {
                key: encode_primary_row_key(1, b"wrong"),
                blob_ptr: ptr,
            }],
            sync,
        )
        .unwrap();
    engine.put_row(1, b"later", b"v", &[], sync).unwrap();
    drop(engine);
    let mut recovered = StorageEngine::open(fs, cfg).unwrap();
    assert_eq!(
        recovered
            .last_wal_recovery()
            .damage
            .as_ref()
            .unwrap()
            .reason,
        crate::wal::WalDamageReason::UnverifiableBlob
    );
    assert_eq!(
        recovered.get_row(1, b"first").unwrap().as_deref(),
        Some(&b"value"[..])
    );
    assert!(!recovered.row_exists(1, b"wrong").unwrap());
    assert!(!recovered.row_exists(1, b"later").unwrap());
}

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

// ---------------------------------------------------------------------------------------------
// Idx segment creation that fails and cannot be cleaned up
// ---------------------------------------------------------------------------------------------

/// Shared switches of [`FlakyFs`].
#[derive(Default)]
struct FlakyCtl {
    /// The next `sync_durable` of a file whose name starts with `idx_` fails.
    fail_idx_sync_once: AtomicBool,
    /// Every `remove_file` of a name starting with `idx_` fails (an antivirus lock).
    fail_idx_remove: AtomicBool,
}

struct FlakyFile {
    inner: Arc<OsFile>,
    is_idx: bool,
    ctl: Arc<FlakyCtl>,
}

impl PlatformFile for FlakyFile {
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
    fn preallocate(
        &self,
        target_len: u64,
        strategy: crate::pal::PreallocStrategy,
    ) -> io::Result<()> {
        self.inner.preallocate(target_len, strategy)
    }
    fn sync_barrier(&self) -> io::Result<()> {
        self.inner.sync_barrier()
    }
    fn sync_durable(&self) -> io::Result<()> {
        if self.is_idx && self.ctl.fail_idx_sync_once.swap(false, Ordering::SeqCst) {
            return Err(io::Error::other("injected idx sync failure"));
        }
        self.inner.sync_durable()
    }
    fn evict_os_cache(&self, offset: u64, len: u64) -> io::Result<()> {
        self.inner.evict_os_cache(offset, len)
    }
}

struct FlakyFs {
    inner: OsFs,
    ctl: Arc<FlakyCtl>,
}

impl PlatformFs for FlakyFs {
    type File = FlakyFile;

    fn open_file(
        &self,
        name: &str,
        create: bool,
        writable: bool,
        hint: OpenHint,
    ) -> io::Result<Arc<Self::File>> {
        Ok(Arc::new(FlakyFile {
            inner: self.inner.open_file(name, create, writable, hint)?,
            is_idx: name.starts_with("idx_"),
            ctl: Arc::clone(&self.ctl),
        }))
    }

    fn remove_file(&self, name: &str) -> io::Result<()> {
        if name.starts_with("idx_") && self.ctl.fail_idx_remove.load(Ordering::SeqCst) {
            return Err(io::Error::other("injected idx remove failure"));
        }
        self.inner.remove_file(name)
    }

    fn list_files(&self) -> io::Result<Vec<String>> {
        self.inner.list_files()
    }

    fn root_path(&self) -> &Path {
        self.inner.root_path()
    }
}

/// A flush whose segment sync failed and whose cleanup could not remove the file leaves a long
/// `idx_<SeqID>.db` behind. The next flush must not write into that name: the builder does not
/// shorten an existing file, so the old tail and footer would survive and, after a reopen, hide
/// the acknowledged overwrite behind the old value.
#[test]
fn failed_idx_flush_with_failed_cleanup_does_not_reuse_the_name() {
    let tmp = TempTestDir::new("idx_failed_flush");
    let ctl = Arc::new(FlakyCtl::default());
    let open = |ctl: &Arc<FlakyCtl>| {
        Arc::new(FlakyFs {
            inner: OsFs::open(&tmp.path, true).unwrap(),
            ctl: Arc::clone(ctl),
        })
    };
    let mut config = EngineConfig::default();
    config.auto_compact_segment_threshold = 0;
    config.inline_threshold_bytes = 4000;
    let sync = WriteDurability::SyncTransactional;

    let mut engine = StorageEngine::open(open(&ctl), config.clone()).unwrap();
    let big = vec![b'A'; 3500];
    for i in 0..20u32 {
        engine
            .put_row(1, format!("k{i:03}").as_bytes(), &big, &[], sync)
            .unwrap();
    }

    ctl.fail_idx_sync_once.store(true, Ordering::SeqCst);
    ctl.fail_idx_remove.store(true, Ordering::SeqCst);
    assert!(engine.flush_memtable().is_err());
    ctl.fail_idx_remove.store(false, Ordering::SeqCst);

    for i in 0..20u32 {
        engine
            .put_row(1, format!("k{i:03}").as_bytes(), b"x", &[], sync)
            .unwrap();
    }
    engine.flush_memtable().unwrap();
    assert_eq!(engine.get_row(1, b"k005").unwrap(), Some(b"x".to_vec()));
    drop(engine);

    let mut reopened = StorageEngine::open(open(&ctl), config).unwrap();
    for i in 0..20u32 {
        assert_eq!(
            reopened.get_row(1, format!("k{i:03}").as_bytes()).unwrap(),
            Some(b"x".to_vec()),
            "k{i:03} lost its acknowledged overwrite after reopen"
        );
    }
}

#[test]
fn reopen_accounts_interior_aborted_blob_gap_and_reclaims_segment() {
    let tmp = TempTestDir::new("blob_interior_aborted_gap");
    let mut config = EngineConfig::default();
    config.inline_threshold_bytes = 64;
    config.max_blob_segment_bytes = 600;
    let sync = WriteDurability::SyncTransactional;
    let fast = WriteDurability::BufferedFast;

    let seg1;
    let seg2;
    {
        let mut engine = open_plain(&tmp.path, &config);
        engine
            .put_row(1, b"live0", &[0x11u8; 120], &[], sync)
            .unwrap();
        seg1 = engine.active_std_writer.as_ref().unwrap().seq_id();
        engine.flush_memtable().unwrap();

        // Append inside an aborted transaction, leaving unreferenced bytes immediately above the
        // cataloged length, then commit a subsequent blob into the same segment before rolling over.
        engine.begin_transaction();
        engine
            .put_row(1, b"aborted", &[0x22u8; 150], &[], fast)
            .unwrap();
        assert_eq!(engine.active_std_writer.as_ref().unwrap().seq_id(), seg1);
        engine.rollback_transaction().unwrap();

        engine
            .put_row(1, b"live1", &[0x33u8; 150], &[], fast)
            .unwrap();
        assert_eq!(engine.active_std_writer.as_ref().unwrap().seq_id(), seg1);

        // Rolling to seg2 seals seg1 with [live0 (cataloged) | aborted gap | live1 (WAL)].
        engine
            .put_row(1, b"live2", &[0x44u8; 300], &[], fast)
            .unwrap();
        seg2 = engine.active_std_writer.as_ref().unwrap().seq_id();
        assert_ne!(seg1, seg2);
        engine.flush_active_blob_buffers().unwrap();
    }

    let seg1_path = tmp.path.join(format_std_blob_file_name(seg1));
    assert!(seg1_path.exists());

    let mut reopened = open_plain(&tmp.path, &config);
    assert_eq!(reopened.get_row(1, b"aborted").unwrap(), None);
    assert_eq!(
        reopened.get_row(1, b"live0").unwrap(),
        Some(vec![0x11u8; 120])
    );
    assert_eq!(
        reopened.get_row(1, b"live1").unwrap(),
        Some(vec![0x33u8; 150])
    );
    assert_eq!(
        reopened.get_row(1, b"live2").unwrap(),
        Some(vec![0x44u8; 300])
    );
    assert!(
        reopened.std_blobs[&seg1].dead_bytes >= 150,
        "interior aborted gap must be accounted on recovery: dead_bytes={} file_len={}",
        reopened.std_blobs[&seg1].dead_bytes,
        reopened.std_blobs[&seg1].file_len
    );

    reopened.delete_row(1, b"live0", sync).unwrap();
    reopened.delete_row(1, b"live1", sync).unwrap();
    reopened.flush_memtable().unwrap();
    reopened.checkpoint().unwrap();

    assert!(
        !seg1_path.exists(),
        "segment with interior aborted gap was never reclaimed after deleting live rows"
    );
    assert_eq!(
        reopened.get_row(1, b"live2").unwrap(),
        Some(vec![0x44u8; 300])
    );
}

#[test]
fn blob_rollover_during_uncommitted_tx_does_not_leak_segment_after_rollback_and_crash() {
    let tmp = TempTestDir::new("blob_uncommitted_rollover");
    let mut config = EngineConfig::default();
    config.inline_threshold_bytes = 64;
    config.max_blob_segment_bytes = 300;
    let fast = WriteDurability::BufferedFast;

    let seg1;
    let seg2;
    {
        let mut engine = open_plain(&tmp.path, &config);
        engine.begin_transaction();
        engine.put_row(1, b"t1", &[0xAAu8; 200], &[], fast).unwrap();
        seg1 = engine.active_std_writer.as_ref().unwrap().seq_id();
        engine.put_row(1, b"t2", &[0xBBu8; 200], &[], fast).unwrap();
        seg2 = engine.active_std_writer.as_ref().unwrap().seq_id();
        assert_ne!(seg1, seg2);
        engine.rollback_transaction().unwrap();
        // Drop without checkpoint to simulate crash after rollback.
    }

    let seg1_path = tmp.path.join(format_std_blob_file_name(seg1));
    let reopened = open_plain(&tmp.path, &config);
    assert!(
        !seg1_path.exists(),
        "uncommitted rolled-over blob segment leaked on disk across rollback + crash"
    );
    assert!(
        !reopened
            .meta_mgr
            .state()
            .std_blob_segments
            .iter()
            .any(|m| m.seq_id == seg1),
        "reclaimed uncommitted segment must not remain in boot_meta catalog"
    );
}
