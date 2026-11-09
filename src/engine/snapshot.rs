//! Read snapshots pin the committed in-memory state and immutable segment handles.

use std::collections::BTreeMap;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard, Weak};

use crate::blob::BlobSegmentReader;
use crate::index::IdxSegmentReader;
use crate::memtable::MemTable;
use crate::pal::{PlatformFile, PlatformFs};
use crate::wal::{BlobKind, BlobPtr};

use super::{CxRef, EngineReader, ReadCtx, ReadView, StorageEngine};

/// Keeps a captured committed view alive across statements or streaming cursor batches.
#[derive(Clone)]
pub(crate) struct ReadSnapshotToken {
    id: u64,
    keep_alive: Arc<()>,
}

pub(super) struct ReadSnapshot<F: PlatformFile> {
    token: Weak<()>,
    memtable: MemTable,
    idx_segments: Vec<Arc<IdxSegmentReader<F>>>,
    std_blobs: BTreeMap<u64, BlobSegmentReader<F>>,
    lru_blobs: BTreeMap<(u16, u64), BlobSegmentReader<F>>,
}

/// Registry of live read snapshots, mutable through a shared engine reference.
///
/// Captures and releases only touch this registry and clone immutable sources, so they run under
/// the shared engine lock and concurrent readers do not serialize on the exclusive lock. The
/// engine publishes a commit under the exclusive lock, so a capture under the shared lock sees
/// either all of a commit or none of it.
pub(super) struct SnapshotRegistry<F: PlatformFile> {
    snapshots: RwLock<BTreeMap<u64, Arc<ReadSnapshot<F>>>>,
    next_id: AtomicU64,
}

impl<F: PlatformFile> SnapshotRegistry<F> {
    pub(super) fn new() -> Self {
        Self {
            snapshots: RwLock::new(BTreeMap::new()),
            next_id: AtomicU64::new(1),
        }
    }

    fn read(&self) -> RwLockReadGuard<'_, BTreeMap<u64, Arc<ReadSnapshot<F>>>> {
        self.snapshots
            .read()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn write(&self) -> RwLockWriteGuard<'_, BTreeMap<u64, Arc<ReadSnapshot<F>>>> {
        self.snapshots
            .write()
            .unwrap_or_else(PoisonError::into_inner)
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.read().len()
    }

    #[cfg(test)]
    pub(super) fn is_empty(&self) -> bool {
        self.read().is_empty()
    }

    fn prune(&self) {
        // Skip the exclusive registry lock when nothing is dead; the common release path of a
        // statement that shares a still-live snapshot then only takes the shared lock.
        if self
            .read()
            .values()
            .all(|snapshot| snapshot.token.strong_count() > 0)
        {
            return;
        }
        self.write()
            .retain(|_, snapshot| snapshot.token.strong_count() > 0);
    }
}

impl<FS: PlatformFs> StorageEngine<FS> {
    /// Captures the committed sources. Runs under the shared engine lock: every source is
    /// mutated only through `&mut self`, so the captured memtable, segment list and blob maps
    /// belong to one consistent published state.
    pub(crate) fn capture_read_snapshot(&self) -> ReadSnapshotToken {
        let registry = &self.read_snapshots;
        let id = registry.next_id.fetch_add(1, Ordering::Relaxed);
        assert!(id != u64::MAX, "read snapshot identifier overflow");
        let token = ReadSnapshotToken {
            id,
            keep_alive: Arc::new(()),
        };
        let snapshot = Arc::new(ReadSnapshot {
            token: Arc::downgrade(&token.keep_alive),
            memtable: self.memtable.clone(),
            idx_segments: self.idx_segments.clone(),
            std_blobs: self
                .std_blobs
                .iter()
                .map(|(&id, b)| (id, b.reader.clone()))
                .collect(),
            lru_blobs: self
                .lru_blobs
                .iter()
                .map(|(&id, b)| (id, b.reader.clone()))
                .collect(),
        });
        registry.prune();
        registry.write().insert(id, snapshot);
        token
    }

    pub(crate) fn release_read_snapshot(&self, token: ReadSnapshotToken) {
        drop(token);
        self.cleanup_read_snapshots();
    }

    #[cfg(test)]
    pub(crate) fn live_read_snapshots(&self) -> usize {
        self.read_snapshots.len()
    }

    pub(crate) fn cleanup_read_snapshots(&self) {
        self.read_snapshots.prune();
    }

    /// The reader borrows `token`, which keeps the snapshot registered while it is in use.
    pub(crate) fn reader_for_snapshot<'a>(
        &'a self,
        token: &'a ReadSnapshotToken,
    ) -> EngineReader<'a, FS> {
        EngineReader {
            eng: self,
            cx: CxRef::Owned(ReadCtx::new(ReadView::Snapshot(token.id), false)),
        }
    }

    /// A snapshot is current while no committed data was published since capture. This compares
    /// the memtable publication generation, not LSNs: a flush inside a commit can make the
    /// checkpoint LSN equal an in-flight transaction's LSN and hide its later publication.
    pub(crate) fn snapshot_is_current(&self, token: &ReadSnapshotToken) -> bool {
        self.pending_group.is_empty()
            && self
                .read_snapshots
                .read()
                .get(&token.id)
                .is_some_and(|snapshot| {
                    snapshot.memtable.generation() == self.memtable.generation()
                })
    }

    pub(crate) fn check_snapshot_write_upgrade(
        &self,
        conn_id: usize,
        token: &ReadSnapshotToken,
    ) -> io::Result<()> {
        self.check_write_allowed_for_conn(conn_id)?;
        if !self.snapshot_is_current(token) {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "read snapshot changed before write upgrade; restart the transaction",
            ));
        }
        Ok(())
    }

    fn snapshot_for_view(&self, view: ReadView) -> Option<&ReadSnapshot<FS::File>> {
        let ReadView::Snapshot(id) = view else {
            return None;
        };
        let snapshot = Arc::as_ptr(
            self.read_snapshots
                .read()
                .get(&id)
                .expect("live read snapshot token must retain its sources"),
        );
        // SAFETY: the registry drops an entry only after the last `ReadSnapshotToken` of the
        // snapshot is gone, and a `ReadView::Snapshot` is only handed out by
        // `reader_for_snapshot`, whose reader borrows a token. The `Arc` allocation therefore
        // outlives every reference returned here, even though the registry lock is released.
        Some(unsafe { &*snapshot })
    }

    pub(super) fn memtable_for_view(&self, view: ReadView) -> &MemTable {
        self.snapshot_for_view(view)
            .map_or(&self.memtable, |s| &s.memtable)
    }

    pub(super) fn idx_segments_for_view(
        &self,
        view: ReadView,
    ) -> &[Arc<IdxSegmentReader<FS::File>>] {
        self.snapshot_for_view(view)
            .map_or(&self.idx_segments, |s| &s.idx_segments)
    }

    pub(super) fn blob_reader_for_view(
        &self,
        view: ReadView,
        ptr: &BlobPtr,
    ) -> Option<&BlobSegmentReader<FS::File>> {
        if let Some(snapshot) = self.snapshot_for_view(view) {
            return match ptr.kind {
                BlobKind::Standard => snapshot.std_blobs.get(&ptr.seg_id),
                BlobKind::Lru => snapshot.lru_blobs.get(&(ptr.table_id, ptr.seg_id)),
            };
        }
        match ptr.kind {
            BlobKind::Standard => self.std_blobs.get(&ptr.seg_id).map(|b| &b.reader),
            BlobKind::Lru => self
                .lru_blobs
                .get(&(ptr.table_id, ptr.seg_id))
                .map(|b| &b.reader),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{CommitStep, EngineConfig, WriteDurability};
    use crate::lru::encode_secondary_index_exact_prefix;
    use crate::pal::OsFs;
    use crate::util::test_util::TempTestDir;

    #[test]
    fn snapshot_keeps_memtable_segments_and_blobs_after_compaction() {
        let tmp = TempTestDir::new("read_snapshot_sources");
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
        let mut config = EngineConfig::default();
        config.auto_compact_segment_threshold = 0;
        config.max_blob_segment_bytes = 3000;
        let mut engine = StorageEngine::open(fs, config).unwrap();
        let blob = vec![7u8; 2048];
        let sync = WriteDurability::SyncTransactional;
        engine
            .put_row(1, b"a", &blob, &[(1, b"old".to_vec())], sync)
            .unwrap();
        engine
            .put_row(1, b"b", b"before", &[(1, b"old".to_vec())], sync)
            .unwrap();
        engine.flush_memtable().unwrap();
        engine
            .put_row(1, b"c", b"memory", &[(1, b"old".to_vec())], sync)
            .unwrap();
        let snapshot = engine.capture_read_snapshot();
        assert!(engine.snapshot_is_current(&snapshot));
        engine.check_snapshot_write_upgrade(1, &snapshot).unwrap();

        engine
            .put_row(1, b"a", &vec![8u8; 2048], &[(1, b"new".to_vec())], sync)
            .unwrap();
        engine.delete_row(1, b"b", sync).unwrap();
        engine
            .put_row(1, b"c", b"changed", &[(1, b"new".to_vec())], sync)
            .unwrap();
        engine.put_row(1, b"d", b"added", &[], sync).unwrap();
        engine.compact().unwrap();
        assert!(!engine.snapshot_is_current(&snapshot));
        assert!(engine.check_snapshot_write_upgrade(1, &snapshot).is_err());

        let reader = engine.reader_for_snapshot(&snapshot);
        assert_eq!(reader.get_row(1, b"a").unwrap(), Some(blob.clone()));
        assert_eq!(reader.get_row(1, b"b").unwrap(), Some(b"before".to_vec()));
        assert_eq!(reader.get_row(1, b"c").unwrap(), Some(b"memory".to_vec()));
        assert!(!reader.row_exists(1, b"d").unwrap());
        assert_eq!(
            reader.scan_keys_range(1, None, None, 100).unwrap(),
            [b"a", b"b", b"c"]
        );
        for reverse in [false, true] {
            let mut pks = Vec::new();
            let mut slab = Vec::new();
            let mut spans = Vec::new();
            reader
                .scan_range_into_slab(
                    1, None, None, reverse, 100, false, &mut pks, &mut slab, &mut spans, None,
                )
                .unwrap();
            // Eager payload scans return only the resume key alongside all row spans.
            assert_eq!(pks, if reverse { vec![b"a"] } else { vec![b"c"] });
            assert_eq!(spans.len(), 3);
            let blob_pos = if reverse { 2 } else { 0 };
            assert_eq!(&slab[spans[blob_pos].0..spans[blob_pos].1], blob);
        }
        let prefix = encode_secondary_index_exact_prefix(1, 1, b"old");
        let mut pks = Vec::new();
        let mut slab = Vec::new();
        let mut spans = Vec::new();
        reader
            .scan_secondary_index_into_slab(
                1,
                1,
                &prefix,
                (&prefix, true),
                None,
                100,
                true,
                &mut pks,
                &mut slab,
                &mut spans,
                None,
            )
            .unwrap();
        assert_eq!(pks, [b"a", b"b", b"c"]);

        let mut lazy = Vec::new();
        let mut slab = Vec::new();
        let mut spans = Vec::new();
        assert!(
            reader
                .get_row_into_lazy(1, b"a", &mut slab, &mut spans, &mut lazy)
                .unwrap()
        );
        let span = reader
            .dereference_lazy_blob_into(1, b"a", lazy[0].as_ref().unwrap(), &mut slab)
            .unwrap()
            .unwrap();
        assert_eq!(&slab[span.0..span.1], blob);
        engine.release_read_snapshot(snapshot);
        assert!(engine.read_snapshots.is_empty());
    }

    #[test]
    fn snapshot_excludes_private_overlay_and_requires_last_token_release() {
        let tmp = TempTestDir::new("read_snapshot_overlay");
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
        let mut engine = StorageEngine::open(fs, EngineConfig::default()).unwrap();
        engine
            .put_row(1, b"key", b"committed", &[], WriteDurability::BufferedFast)
            .unwrap();
        engine.begin_transaction_for_conn(1).unwrap();
        engine
            .put_row(1, b"key", b"private", &[], WriteDurability::BufferedFast)
            .unwrap();
        let token = engine.capture_read_snapshot();
        assert_eq!(
            engine
                .reader_for_snapshot(&token)
                .get_row(1, b"key")
                .unwrap(),
            Some(b"committed".to_vec())
        );
        let cursor_token = token.clone();
        engine.release_read_snapshot(token);
        assert_eq!(engine.read_snapshots.len(), 1);
        engine.rollback_transaction().unwrap();
        engine.release_read_snapshot(cursor_token);
        assert!(engine.read_snapshots.is_empty());
    }

    #[test]
    fn snapshot_upgrade_detects_group_commit_published_after_capture() {
        let tmp = TempTestDir::new("read_snapshot_group_commit");
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
        let mut engine = StorageEngine::open(fs, EngineConfig::default()).unwrap();
        engine.begin_transaction_for_conn(1).unwrap();
        engine
            .put_row(
                1,
                b"key",
                b"pending",
                &[],
                WriteDurability::SyncTransactional,
            )
            .unwrap();
        engine
            .prepare_commit_transaction(WriteDurability::SyncTransactional)
            .unwrap();
        let CommitStep::AwaitingGroup { lsn, file, .. } = engine.begin_commit_for_conn(1).unwrap()
        else {
            panic!("plain synchronous write must use group commit");
        };
        let token = engine.capture_read_snapshot();
        assert!(!engine.snapshot_is_current(&token));
        assert_eq!(
            engine
                .reader_for_snapshot(&token)
                .get_row(1, b"key")
                .unwrap(),
            None
        );
        engine.finish_group_flush(lsn, file.sync_barrier()).unwrap();
        assert!(!engine.snapshot_is_current(&token));
        assert_eq!(
            engine
                .reader_for_snapshot(&token)
                .get_row(1, b"key")
                .unwrap(),
            None
        );
        assert_eq!(
            engine.get_row(1, b"key").unwrap(),
            Some(b"pending".to_vec())
        );
        engine.release_read_snapshot(token);
    }

    #[test]
    fn snapshot_taken_during_wal_rotation_inside_commit_is_stale_after_publish() {
        let tmp = TempTestDir::new("read_snapshot_rotation_in_commit");
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
        let mut config = EngineConfig::default();
        config.sync_wal_size_bytes = 8 * 4096;
        let mut engine = StorageEngine::open(fs, config).unwrap();
        let sync = WriteDurability::SyncTransactional;
        let mut violations = Vec::new();
        for i in 0..200u32 {
            let value = i.to_le_bytes().to_vec();
            engine.begin_transaction_for_conn(1).unwrap();
            engine.put_row(1, b"key", &value, &[], sync).unwrap();
            engine.prepare_commit_transaction(sync).unwrap();
            let CommitStep::AwaitingGroup { lsn, file, .. } =
                engine.begin_commit_for_conn(1).unwrap()
            else {
                panic!("expected group commit");
            };
            // A reader starts while the commit is still waiting for its flush.
            let token = engine.capture_read_snapshot();
            engine.finish_group_flush(lsn, file.sync_barrier()).unwrap();
            let seen = engine
                .reader_for_snapshot(&token)
                .get_row(1, b"key")
                .unwrap();
            if engine.snapshot_is_current(&token) {
                assert_ne!(seen.as_deref(), Some(value.as_slice()));
                violations.push(i);
            }
            engine.release_read_snapshot(token);
        }
        assert!(
            violations.is_empty(),
            "stale snapshots reported current at commits: {violations:?}"
        );
    }

    #[test]
    fn snapshot_stays_current_across_flush_without_data_change() {
        let tmp = TempTestDir::new("read_snapshot_flush_no_change");
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
        let mut engine = StorageEngine::open(fs, EngineConfig::default()).unwrap();
        let sync = WriteDurability::SyncTransactional;
        engine.put_row(1, b"key", b"a", &[], sync).unwrap();
        let token = engine.capture_read_snapshot();
        engine.flush_memtable().unwrap();
        assert!(engine.snapshot_is_current(&token));
        engine.check_snapshot_write_upgrade(1, &token).unwrap();
        engine.put_row(1, b"key", b"b", &[], sync).unwrap();
        assert!(!engine.snapshot_is_current(&token));
        engine.release_read_snapshot(token);
    }

    #[test]
    fn writers_prune_snapshot_releases_deferred_by_a_busy_engine_lock() {
        let tmp = TempTestDir::new("read_snapshot_deferred_release");
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
        let mut engine = StorageEngine::open(fs, EngineConfig::default()).unwrap();
        let token = engine.capture_read_snapshot();
        // A trace callback may have to drop its token while another writer owns the engine.
        drop(token);
        assert_eq!(engine.read_snapshots.len(), 1);
        engine.begin_transaction_for_conn(1).unwrap();
        assert!(engine.read_snapshots.is_empty());
        engine.rollback_transaction().unwrap();

        let token = engine.capture_read_snapshot();
        drop(token);
        engine
            .put_row(1, b"key", b"value", &[], WriteDurability::BufferedFast)
            .unwrap();
        assert!(engine.read_snapshots.is_empty());
    }

    #[test]
    fn capture_and_release_need_only_a_shared_engine_reference() {
        fn capture_cycle(engine: &StorageEngine<OsFs>) -> usize {
            let token = engine.capture_read_snapshot();
            let live = engine.read_snapshots.len();
            engine.release_read_snapshot(token);
            live
        }

        let tmp = TempTestDir::new("read_snapshot_shared_lock");
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
        let engine = Arc::new(std::sync::RwLock::new(
            StorageEngine::open(fs, EngineConfig::default()).unwrap(),
        ));
        // Captures from several threads must proceed while every one of them holds the shared
        // lock; with an exclusive lock the barrier below could never be passed.
        let barrier = Arc::new(std::sync::Barrier::new(4));
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let engine = Arc::clone(&engine);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    let guard = engine.read().unwrap();
                    let token = guard.capture_read_snapshot();
                    barrier.wait();
                    assert!(guard.snapshot_is_current(&token));
                    guard.release_read_snapshot(token);
                    for _ in 0..100 {
                        capture_cycle(&guard);
                    }
                })
            })
            .collect();
        for handle in handles {
            handle.join().unwrap();
        }
        let guard = engine.read().unwrap();
        guard.cleanup_read_snapshots();
        assert!(guard.read_snapshots.is_empty());
    }

    #[test]
    fn snapshot_captured_concurrently_with_commits_is_never_torn() {
        let tmp = TempTestDir::new("read_snapshot_not_torn");
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
        let mut config = EngineConfig::default();
        config.inline_threshold_bytes = 64;
        config.max_blob_segment_bytes = 16 * 1024;
        let engine = Arc::new(std::sync::RwLock::new(
            StorageEngine::open(fs, config).unwrap(),
        ));
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let readers: Vec<_> = (0..3)
            .map(|_| {
                let (engine, stop) = (Arc::clone(&engine), Arc::clone(&stop));
                std::thread::spawn(move || {
                    let mut checked = 0u32;
                    while !stop.load(Ordering::Relaxed) {
                        let guard = engine.read().unwrap();
                        let token = guard.capture_read_snapshot();
                        let reader = guard.reader_for_snapshot(&token);
                        // Every commit writes the same generation to an inline row, a blob row
                        // and a row that moves between memtable and segments.
                        let a = reader.get_row(1, b"a").unwrap();
                        let b = reader.get_row(1, b"b").unwrap();
                        let c = reader.get_row(1, b"c").unwrap();
                        match (a, b, c) {
                            (None, None, None) => {}
                            (Some(a), Some(b), Some(c)) => {
                                assert_eq!(a[..4], b[..4], "torn snapshot");
                                assert_eq!(a[..4], c[..4], "torn snapshot");
                            }
                            other => panic!("torn snapshot: {other:?}"),
                        }
                        checked += 1;
                        guard.release_read_snapshot(token);
                    }
                    checked
                })
            })
            .collect();
        let sync = WriteDurability::BufferedFast;
        for i in 0..300u32 {
            let tag = i.to_le_bytes();
            let mut big = tag.to_vec();
            big.resize(1024, 0xAB);
            let mut mid = tag.to_vec();
            mid.resize(16, 0xCD);
            let mut small = tag.to_vec();
            small.resize(8, 0xEF);
            let mut guard = engine.write().unwrap();
            guard.begin_transaction();
            guard.put_row(1, b"a", &small, &[], sync).unwrap();
            guard.put_row(1, b"b", &big, &[], sync).unwrap();
            guard.put_row(1, b"c", &mid, &[], sync).unwrap();
            guard.commit_transaction().unwrap();
            if i % 25 == 0 {
                guard.flush_memtable().unwrap();
            }
            if i % 100 == 99 {
                guard.compact().unwrap();
            }
            drop(guard);
            std::thread::yield_now();
        }
        stop.store(true, Ordering::Relaxed);
        let checked: u32 = readers.into_iter().map(|h| h.join().unwrap()).sum();
        assert!(checked > 0);
        let guard = engine.read().unwrap();
        guard.cleanup_read_snapshots();
        assert!(guard.read_snapshots.is_empty());
    }
}
