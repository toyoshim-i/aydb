//! Read snapshots pin the committed in-memory state and immutable segment handles.

use std::collections::BTreeMap;
use std::io;
use std::sync::{Arc, Weak};

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

impl<FS: PlatformFs> StorageEngine<FS> {
    pub(crate) fn capture_read_snapshot(&mut self) -> ReadSnapshotToken {
        self.cleanup_read_snapshots();
        let token = ReadSnapshotToken {
            id: self.next_read_snapshot_id,
            keep_alive: Arc::new(()),
        };
        self.next_read_snapshot_id = self
            .next_read_snapshot_id
            .checked_add(1)
            .expect("read snapshot identifier overflow");
        let snapshot = ReadSnapshot {
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
        };
        self.read_snapshots.insert(token.id, snapshot);
        token
    }

    pub(crate) fn release_read_snapshot(&mut self, token: ReadSnapshotToken) {
        drop(token);
        self.cleanup_read_snapshots();
    }

    pub(crate) fn cleanup_read_snapshots(&mut self) {
        self.read_snapshots
            .retain(|_, snapshot| snapshot.token.strong_count() > 0);
    }

    pub(crate) fn reader_for_snapshot(&self, token: &ReadSnapshotToken) -> EngineReader<'_, FS> {
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
            && self.read_snapshots.get(&token.id).is_some_and(|snapshot| {
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
        match view {
            ReadView::Snapshot(id) => Some(
                self.read_snapshots
                    .get(&id)
                    .expect("live read snapshot token must retain its sources"),
            ),
            _ => None,
        }
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
}
