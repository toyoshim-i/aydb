//! Read snapshots pin the committed in-memory state and immutable segment handles.

use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::blob::BlobSegmentReader;
use crate::index::IdxSegmentReader;
use crate::memtable::MemTable;
use crate::pal::{OsFile, PlatformFile, PlatformFs};
use crate::wal::{BlobKind, BlobPtr};

use super::reader::ReadSource;
use super::{CxRef, EngineReader, ReadCtx, StorageEngine};

static NEXT_ENGINE_ID: AtomicU64 = AtomicU64::new(1);

/// Allocates a process-wide unique identifier for a [`StorageEngine`] instance.
pub(super) fn alloc_engine_id() -> u64 {
    let id = NEXT_ENGINE_ID.fetch_add(1, Ordering::Relaxed);
    assert!(id != u64::MAX, "storage engine identifier overflow");
    id
}

type CachedSegments<F> = Arc<Vec<Arc<IdxSegmentReader<F>>>>;
type CachedStdBlobs<F> = Arc<BTreeMap<u64, BlobSegmentReader<F>>>;
type CachedLruBlobs<F> = Arc<BTreeMap<(u16, u64), BlobSegmentReader<F>>>;

pub(super) struct ReadSnapshot<F: PlatformFile> {
    engine_id: u64,
    data_generation: u64,
    memtable: MemTable,
    idx_segments: CachedSegments<F>,
    std_blobs: CachedStdBlobs<F>,
    lru_blobs: CachedLruBlobs<F>,
}

/// Keeps a captured committed view alive across statements or streaming cursor batches.
pub(crate) struct ReadSnapshotToken<F: PlatformFile = OsFile> {
    snapshot: Arc<ReadSnapshot<F>>,
}

impl<F: PlatformFile> Clone for ReadSnapshotToken<F> {
    fn clone(&self) -> Self {
        Self {
            snapshot: Arc::clone(&self.snapshot),
        }
    }
}

#[cfg(test)]
impl<F: PlatformFile> ReadSnapshotToken<F> {
    pub(crate) fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.snapshot, &other.snapshot)
    }

    pub(crate) fn strong_count(&self) -> usize {
        Arc::strong_count(&self.snapshot)
    }
}

/// Engine-side cache of immutable snapshot components and the latest [`ReadSnapshot`].
///
/// Repeated captures at the same committed state reuse both the top-level `Arc<ReadSnapshot>` and
/// the inner `Arc` collections without cloning `BTreeMap`s or bumping `MemTable` CoW refcounts
/// once writers invalidate stale slots before mutating engine state.
pub(super) struct SnapshotCache<F: PlatformFile> {
    snapshot: Option<Arc<ReadSnapshot<F>>>,
    idx_segments: Option<CachedSegments<F>>,
    std_blobs: Option<CachedStdBlobs<F>>,
    lru_blobs: Option<CachedLruBlobs<F>>,
}

impl<F: PlatformFile> SnapshotCache<F> {
    pub(super) fn new() -> Self {
        Self {
            snapshot: None,
            idx_segments: None,
            std_blobs: None,
            lru_blobs: None,
        }
    }
}

impl<FS: PlatformFs> StorageEngine<FS> {
    fn snapshot_cache_mut(&mut self) -> &mut SnapshotCache<FS::File> {
        self.snapshot_cache
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Clears the cached composite snapshot before mutating `self.memtable`.
    #[inline]
    pub(super) fn invalidate_snapshot_memtable(&mut self) {
        self.snapshot_cache_mut().snapshot = None;
    }

    /// Clears cached snapshot and segment-list `Arc`s before replacing/dropping `idx_segments`.
    #[inline]
    pub(super) fn invalidate_snapshot_idx_segments(&mut self) {
        let cache = self.snapshot_cache_mut();
        cache.snapshot = None;
        cache.idx_segments = None;
    }

    /// Clears cached snapshot and standard-blob map `Arc`s before mutating `std_blobs`.
    #[inline]
    pub(super) fn invalidate_snapshot_std_blobs(&mut self) {
        let cache = self.snapshot_cache_mut();
        cache.snapshot = None;
        cache.std_blobs = None;
    }

    /// Clears cached snapshot and LRU-blob map `Arc`s before mutating `lru_blobs`.
    #[inline]
    pub(super) fn invalidate_snapshot_lru_blobs(&mut self) {
        let cache = self.snapshot_cache_mut();
        cache.snapshot = None;
        cache.lru_blobs = None;
    }

    /// Captures the committed sources. Runs under the shared engine lock: every source is
    /// mutated only through `&mut self`, so the captured memtable, segment list and blob maps
    /// belong to one consistent published state.
    pub(crate) fn capture_read_snapshot(&self) -> ReadSnapshotToken<FS::File> {
        {
            let cache = self
                .snapshot_cache
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(snapshot) = &cache.snapshot {
                return ReadSnapshotToken {
                    snapshot: Arc::clone(snapshot),
                };
            }
        }

        let mut cache = self
            .snapshot_cache
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(snapshot) = &cache.snapshot {
            return ReadSnapshotToken {
                snapshot: Arc::clone(snapshot),
            };
        }

        let idx_segments = Arc::clone(
            cache
                .idx_segments
                .get_or_insert_with(|| Arc::new(self.idx_segments.clone())),
        );
        let std_blobs = Arc::clone(cache.std_blobs.get_or_insert_with(|| {
            Arc::new(
                self.std_blobs
                    .iter()
                    .map(|(&id, b)| (id, b.reader.clone()))
                    .collect(),
            )
        }));
        let lru_blobs = Arc::clone(cache.lru_blobs.get_or_insert_with(|| {
            Arc::new(
                self.lru_blobs
                    .iter()
                    .map(|(&id, b)| (id, b.reader.clone()))
                    .collect(),
            )
        }));

        let snapshot = Arc::new(ReadSnapshot {
            engine_id: self.engine_id,
            data_generation: self.data_generation,
            memtable: self.memtable.clone(),
            idx_segments,
            std_blobs,
            lru_blobs,
        });
        cache.snapshot = Some(Arc::clone(&snapshot));
        ReadSnapshotToken { snapshot }
    }

    #[cfg(test)]
    fn cached_snapshot_strong_count(&self) -> Option<usize> {
        self.snapshot_cache
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .snapshot
            .as_ref()
            .map(Arc::strong_count)
    }

    /// The reader borrows `token`, keeping its committed sources alive while in use.
    pub(crate) fn reader_for_snapshot<'a>(
        &'a self,
        token: &'a ReadSnapshotToken<FS::File>,
    ) -> EngineReader<'a, FS> {
        assert_eq!(
            token.snapshot.engine_id, self.engine_id,
            "read snapshot token belongs to a different StorageEngine"
        );
        EngineReader {
            eng: self,
            cx: CxRef::Owned(ReadCtx::new(ReadSource::Snapshot(&token.snapshot), false)),
        }
    }

    /// Executes a read closure against a pinned `ReadSnapshotToken` with exclusive engine access
    /// so `EagerWriteOnRead` promotions can be persisted outside active transactions, propagating
    /// any blob buffer flush error.
    pub(crate) fn with_snapshot_read<R>(
        &mut self,
        token: &ReadSnapshotToken<FS::File>,
        mut f: impl FnMut(&EngineReader<'_, FS>) -> R,
    ) -> io::Result<R> {
        assert_eq!(
            token.snapshot.engine_id, self.engine_id,
            "read snapshot token belongs to a different StorageEngine"
        );
        let (r, extra) = self.run_read_src(ReadSource::Snapshot(&token.snapshot), |eng, cx| {
            f(&EngineReader {
                eng,
                cx: CxRef::Borrowed(cx),
            })
        });
        extra.map_or(Ok(r), Err)
    }

    /// A snapshot is current while it belongs to this engine and no committed data was published
    /// since capture. This compares the data publication generation (ignoring internal
    /// `EagerWriteOnRead` LRU promotions), rather than LSNs or `MemTable::generation`: a flush
    /// inside a commit can make the checkpoint LSN equal an in-flight transaction's LSN, and an
    /// eager LRU promotion bumps the `MemTable` generation without changing user data.
    pub(super) fn snapshot_is_current(&self, token: &ReadSnapshotToken<FS::File>) -> bool {
        token.snapshot.engine_id == self.engine_id
            && self.pending_group.is_empty()
            && token.snapshot.data_generation == self.data_generation
    }

    pub(crate) fn check_snapshot_current_for_upgrade(
        &self,
        token: &ReadSnapshotToken<FS::File>,
    ) -> io::Result<()> {
        if token.snapshot.engine_id != self.engine_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "read snapshot token belongs to a different StorageEngine",
            ));
        }
        if !self.snapshot_is_current(token) {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "read snapshot changed before write upgrade; restart the transaction",
            ));
        }
        Ok(())
    }

    #[cfg(test)]
    fn check_snapshot_write_upgrade(
        &self,
        conn_id: usize,
        token: &ReadSnapshotToken<FS::File>,
    ) -> io::Result<()> {
        if token.snapshot.engine_id != self.engine_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "read snapshot token belongs to a different StorageEngine",
            ));
        }
        self.check_write_allowed_for_conn(conn_id)?;
        self.check_snapshot_current_for_upgrade(token)
    }

    #[inline]
    pub(super) fn memtable_for<'a>(&'a self, src: ReadSource<'a, FS::File>) -> &'a MemTable {
        match src {
            ReadSource::Snapshot(s) => &s.memtable,
            ReadSource::Owner | ReadSource::Committed => &self.memtable,
        }
    }

    #[inline]
    pub(super) fn idx_segments_for<'a>(
        &'a self,
        src: ReadSource<'a, FS::File>,
    ) -> &'a [Arc<IdxSegmentReader<FS::File>>] {
        match src {
            ReadSource::Snapshot(s) => &s.idx_segments,
            ReadSource::Owner | ReadSource::Committed => &self.idx_segments,
        }
    }

    #[inline]
    pub(super) fn blob_reader_for<'a>(
        &'a self,
        src: ReadSource<'a, FS::File>,
        ptr: &BlobPtr,
    ) -> Option<&'a BlobSegmentReader<FS::File>> {
        if let ReadSource::Snapshot(s) = src {
            return match ptr.kind {
                BlobKind::Standard => s.std_blobs.get(&ptr.seg_id),
                BlobKind::Lru => s.lru_blobs.get(&(ptr.table_id, ptr.seg_id)),
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
                    1,
                    crate::engine::RangeScanOptions {
                        lower: None,
                        upper: None,
                        reverse,
                        limit: 100,
                        keys_only: false,
                    },
                    crate::engine::SlabScanOutput {
                        pks: &mut pks,
                        slab: &mut slab,
                        spans: &mut spans,
                        lazy_blobs: None,
                    },
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
                crate::engine::SecondaryScanOptions {
                    prefix: &prefix,
                    lower_sec_key: (&prefix, true),
                    upper_sec_key: None,
                    limit: 100,
                    keys_only: true,
                },
                crate::engine::SlabScanOutput {
                    pks: &mut pks,
                    slab: &mut slab,
                    spans: &mut spans,
                    lazy_blobs: None,
                },
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
        assert_eq!(snapshot.strong_count(), 1);
        drop(snapshot);
        assert_eq!(engine.cached_snapshot_strong_count(), None);
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
        drop(token);
        assert!(cursor_token.strong_count() >= 2);
        assert_eq!(
            engine
                .reader_for_snapshot(&cursor_token)
                .get_row(1, b"key")
                .unwrap(),
            Some(b"committed".to_vec())
        );
        engine.rollback_transaction().unwrap();
        assert_eq!(cursor_token.strong_count(), 2);
        drop(cursor_token);
        assert_eq!(engine.cached_snapshot_strong_count(), Some(1));
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
        drop(token);
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
            drop(token);
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
        drop(token);
    }

    #[test]
    fn writers_invalidate_stale_cached_snapshot_without_external_registry() {
        let tmp = TempTestDir::new("read_snapshot_cache_invalidation");
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
        let mut engine = StorageEngine::open(fs, EngineConfig::default()).unwrap();
        let token = engine.capture_read_snapshot();
        assert_eq!(token.strong_count(), 2);
        drop(token);
        assert_eq!(engine.cached_snapshot_strong_count(), Some(1));
        engine
            .put_row(1, b"key", b"value", &[], WriteDurability::BufferedFast)
            .unwrap();
        assert_eq!(engine.cached_snapshot_strong_count(), None);
    }

    #[test]
    fn capture_and_release_need_only_a_shared_engine_reference() {
        fn capture_cycle(engine: &StorageEngine<OsFs>) -> usize {
            let token = engine.capture_read_snapshot();
            let count = token.strong_count();
            drop(token);
            count
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
                    drop(token);
                    for _ in 0..100 {
                        assert!(capture_cycle(&guard) >= 2);
                    }
                })
            })
            .collect();
        for handle in handles {
            handle.join().unwrap();
        }
        let guard = engine.read().unwrap();
        assert_eq!(guard.cached_snapshot_strong_count(), Some(1));
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
        let _guard = engine.read().unwrap();
    }

    #[test]
    fn cross_engine_snapshot_token_is_rejected_without_ub() {
        let tmp_a = TempTestDir::new("cross_eng_snap_a");
        let tmp_b = TempTestDir::new("cross_eng_snap_b");
        let fs_a = Arc::new(OsFs::open(&tmp_a.path, true).unwrap());
        let fs_b = Arc::new(OsFs::open(&tmp_b.path, true).unwrap());
        let mut engine_a = StorageEngine::open(fs_a, EngineConfig::default()).unwrap();
        let engine_b = StorageEngine::open(fs_b, EngineConfig::default()).unwrap();
        engine_a
            .put_row(1, b"k", b"v", &[], WriteDurability::BufferedFast)
            .unwrap();
        let tok_a = engine_a.capture_read_snapshot();
        assert!(!engine_b.snapshot_is_current(&tok_a));
        let err = engine_b
            .check_snapshot_write_upgrade(1, &tok_a)
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = engine_b.reader_for_snapshot(&tok_a);
        }));
        assert!(res.is_err());
    }

    #[test]
    fn cached_snapshot_reuses_arc_and_avoids_cow_copy_after_release() {
        let tmp = TempTestDir::new("snap_cache_cow");
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
        let mut engine = StorageEngine::open(fs, EngineConfig::default()).unwrap();
        for i in 0..50u32 {
            let key = format!("k{i:04}");
            engine
                .put_row(
                    1,
                    key.as_bytes(),
                    b"val",
                    &[],
                    WriteDurability::BufferedFast,
                )
                .unwrap();
        }
        let tok1 = engine.capture_read_snapshot();
        let tok2 = engine.capture_read_snapshot();
        assert!(tok1.ptr_eq(&tok2));
        assert_eq!(tok1.strong_count(), 3);
        drop(tok1);
        drop(tok2);
        assert_eq!(engine.cached_snapshot_strong_count(), Some(1));

        let clones_before = crate::memtable::mem_entry_clones();
        engine
            .put_row(1, b"k0000", b"updated", &[], WriteDurability::BufferedFast)
            .unwrap();
        let clones_after = crate::memtable::mem_entry_clones();
        assert_eq!(clones_after - clones_before, 0);
        assert_eq!(engine.cached_snapshot_strong_count(), None);
    }

    #[test]
    fn compaction_drops_cached_snapshot_handles_before_unlinking() {
        let tmp = TempTestDir::new("snap_cache_compaction_unlink");
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
        let mut config = EngineConfig::default();
        config.auto_compact_segment_threshold = 0;
        let mut engine = StorageEngine::open(fs, config).unwrap();
        let sync = WriteDurability::BufferedFast;
        engine.put_row(1, b"a", b"v1", &[], sync).unwrap();
        engine.flush_memtable().unwrap();
        engine.put_row(1, b"a", b"v2", &[], sync).unwrap();
        engine.flush_memtable().unwrap();

        // Populate `snapshot_cache` with a now-dropped snapshot right before compacting.
        let tok = engine.capture_read_snapshot();
        drop(tok);
        assert_eq!(engine.cached_snapshot_strong_count(), Some(1));

        engine.compact().unwrap();
        assert_eq!(engine.cached_snapshot_strong_count(), None);
        assert!(
            engine.meta_mgr().state().pending_delete_files.is_empty(),
            "cached snapshot must release old handles so dead segments unlink immediately"
        );
    }
}
