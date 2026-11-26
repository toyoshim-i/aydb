//! Layer 4 (Part 2): Unified Phase 1 Storage Engine Coordinator (`StorageEngine`).
//!
//! Integrates `BootMetaManager` (`boot_meta.db`), two-stream Epoch WAL (`wal_sync.db` + `wal_buf.db`),
//! out-of-line blob segments (`blob_std` + per-table `blob_lru`), bounded `MemTable`, 4 KB block
//! `idx_<SeqID>.db` segments with Bloom filters and sparse fence pointers, multi-way merge
//! compaction, and Capped Table (`MAX_BYTES`) delayed LRU eviction.

mod blobs;
mod catalog;
#[cfg(test)]
use self::catalog::find_free_table_id;
pub(crate) use self::catalog::{VtabPlan, normalize_vtab_ident};
mod compaction;
pub(crate) use compaction::StdBlobRelocStep;
pub use compaction::{CompactionJob, CompactionOutput};
mod eviction;
mod open;
mod reader;
mod rowid;
mod salvage;
mod scan;
pub use scan::{RangeScanOptions, SecondaryScanOptions, SlabScanOutput};
mod snapshot;
mod tx;
mod tx_fsm;
mod unique_index;
mod wal_links;
#[cfg(test)]
pub(crate) use scan::TABLE_SCAN_ENTRIES_COPIED;
pub(crate) use snapshot::ReadSnapshotToken;

pub use salvage::{SalvagedOp, SalvagedTx};
pub(crate) use tx::{CommitStep, GroupRole, STATEMENT_SAVEPOINT};

#[cfg(test)]
mod blob_recovery_tests;
#[cfg(test)]
mod capped_reconnect_tests;
#[cfg(test)]
mod crash_tests;
#[cfg(test)]
mod metadata_failure_tests;
#[cfg(test)]
mod read_poison_tests;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod wal_link_tests;

use std::collections::{BTreeMap, HashMap};
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

#[cfg(test)]
use crate::blob::BlobSegmentWriter;
use crate::blob::{
    BLOB_RECORD_HEADER_LEN, BlobSegmentReader, DEFAULT_INLINE_THRESHOLD,
    DEFAULT_MAX_BLOB_SEGMENT_SIZE,
};
use crate::cache::{VerifiedBlobCache, VerifiedBlockCache};
use crate::index::{
    BloomFilter, DEFAULT_BLOOM_BITS_PER_KEY, IdxSegmentReader, IndexEntry, IndexValue, RawIdxEntry,
    decode_lru_queue_key, decode_secondary_index_pk_slice,
};
use crate::lru::{
    AccessTracker, CappedTableState, NS_LRU_QUEUE, NS_PRIMARY_ROW, NS_ROW_META, NS_SECONDARY_INDEX,
    RowMetaRecord, encode_lru_queue_key, encode_namespace_prefix, encode_primary_row_key,
    encode_row_meta_key, encode_secondary_index_key,
};
use crate::memtable::{DEFAULT_MEMTABLE_CAPACITY_BYTES, MemTable};
use crate::meta::BootMetaManager;
#[cfg(test)]
use crate::meta::{EvictionPolicy, format_lru_blob_file_name, format_std_blob_file_name};
#[cfg(test)]
use crate::pal::OpenHint;
use crate::pal::{PlatformFile, PlatformFs};
use crate::wal::{
    BlobKind, BlobPtr, DEFAULT_BUF_WAL_SIZE, DEFAULT_SYNC_WAL_SIZE, WalAppendOutcome, WalBatch,
    WalRecord, WalStreamMode, WalStreamWriter,
};
pub use crate::wal::{StreamCounts, WalDamage, WalDamageReason, WalRecoveryReport};

use self::blobs::PendingCorruptEviction;
use self::reader::{CxRef, NEED_FLUSH_MSG, ReadCtx, ReadSource};
pub use self::reader::{EngineReader, ReadView};

/// Optional parameters controlling row insertion in `StorageEngine::put_row_owned_with_hint_and_extra`.
pub(crate) struct PutRowOptions<'a> {
    pub(crate) secondary_keys: &'a [(u16, Vec<u8>)],
    pub(crate) extra_records: Vec<WalRecord>,
    pub(crate) durability: WriteDurability,
    pub(crate) known_absent: bool,
}

struct PreparedRowMutation {
    row_key: Vec<u8>,
    new_sec_keys: Vec<Vec<u8>>,
    is_capped: bool,
    logical_size: u32,
}

pub(super) struct OldMetaDeleteOpts<'a> {
    pub(super) require_capped_for_lru: bool,
    pub(super) delete_meta_key: bool,
    pub(super) match_lru_key: Option<&'a [u8]>,
}

pub(super) struct OldMetaDeleteResult {
    pub(super) had_old_meta: bool,
    pub(super) old_meta_blob_ptr: Option<BlobPtr>,
    pub(super) old_logical_size: u64,
    pub(super) matched_lru_key: bool,
}
use self::scoped_guards::{
    ReconcilingCappedGuard, RelocatingBlobsGuard, ScopedReconcilingTable, ScopedRelocatingFlag,
};
use self::tx::WriteScope;
use self::tx_fsm::TxEvent;
use self::wal_links::LogStream;

/// Type-enforced RAII guards for engine-scoped re-entrancy/phase latches.
mod scoped_guards {
    use std::ops::{Deref, DerefMut};

    use super::StorageEngine;
    use crate::pal::PlatformFs;

    #[derive(Default)]
    pub(super) struct ScopedRelocatingFlag {
        active: bool,
    }

    impl ScopedRelocatingFlag {
        #[inline]
        pub(super) fn is_active(&self) -> bool {
            self.active
        }
    }

    #[must_use = "RelocatingBlobsGuard clears relocating_std_blobs on drop"]
    pub(super) struct RelocatingBlobsGuard<'a, FS: PlatformFs> {
        engine: &'a mut StorageEngine<FS>,
    }

    impl<'a, FS: PlatformFs> RelocatingBlobsGuard<'a, FS> {
        #[inline]
        pub(super) fn enter(engine: &'a mut StorageEngine<FS>) -> Self {
            debug_assert!(!engine.relocating_std_blobs.active);
            engine.relocating_std_blobs.active = true;
            Self { engine }
        }
    }

    impl<FS: PlatformFs> Deref for RelocatingBlobsGuard<'_, FS> {
        type Target = StorageEngine<FS>;
        fn deref(&self) -> &Self::Target {
            self.engine
        }
    }

    impl<FS: PlatformFs> DerefMut for RelocatingBlobsGuard<'_, FS> {
        fn deref_mut(&mut self) -> &mut Self::Target {
            self.engine
        }
    }

    impl<FS: PlatformFs> Drop for RelocatingBlobsGuard<'_, FS> {
        fn drop(&mut self) {
            self.engine.relocating_std_blobs.active = false;
        }
    }

    #[derive(Default)]
    pub(super) struct ScopedReconcilingTable {
        table_id: Option<u16>,
    }

    impl ScopedReconcilingTable {
        #[inline]
        pub(super) fn get(&self) -> Option<u16> {
            self.table_id
        }
    }

    #[must_use = "ReconcilingCappedGuard clears reconciling_capped_table on drop"]
    pub(super) struct ReconcilingCappedGuard<'a, FS: PlatformFs> {
        engine: &'a mut StorageEngine<FS>,
    }

    impl<'a, FS: PlatformFs> ReconcilingCappedGuard<'a, FS> {
        #[inline]
        pub(super) fn enter(engine: &'a mut StorageEngine<FS>, table_id: u16) -> Self {
            debug_assert!(engine.reconciling_capped_table.table_id.is_none());
            engine.reconciling_capped_table.table_id = Some(table_id);
            Self { engine }
        }
    }

    impl<FS: PlatformFs> Deref for ReconcilingCappedGuard<'_, FS> {
        type Target = StorageEngine<FS>;
        fn deref(&self) -> &Self::Target {
            self.engine
        }
    }

    impl<FS: PlatformFs> DerefMut for ReconcilingCappedGuard<'_, FS> {
        fn deref_mut(&mut self) -> &mut Self::Target {
            self.engine
        }
    }

    impl<FS: PlatformFs> Drop for ReconcilingCappedGuard<'_, FS> {
        fn drop(&mut self) {
            self.engine.reconciling_capped_table.table_id = None;
        }
    }
}

/// Type-enforced poison capability gate owning all durable writers and `BootMetaManager`.
///
/// Keeping durable handles private inside this sibling module ensures no path in `src/engine/`
/// can issue metadata, WAL, or blob-segment mutations without first proving via
/// [`HealthGatedStorage::try_healthy`] that neither the engine, `BootMetaManager`, nor `CommitGate`
/// is poisoned, and consuming the resulting one-shot [`HealthyEngine`] token.
mod health {
    use std::collections::{BTreeMap, HashMap};
    use std::io;

    use super::tx::CommitGate;
    use super::{TrackedLruBlob, TrackedStdBlob, WriteDurability};
    use crate::blob::BlobSegmentWriter;
    use crate::meta::{BootMetaManager, BootMetaState};
    use crate::pal::PlatformFs;
    use crate::wal::{BlobKind, BlobPtr, WalAppendOutcome, WalStreamWriter};

    #[derive(Debug, Default)]
    pub(super) enum EngineHealth {
        #[default]
        Healthy,
        Poisoned(String),
    }

    pub(super) struct HealthGatedStorage<FS: PlatformFs> {
        health: EngineHealth,
        meta_mgr: BootMetaManager<FS>,
        sync_wal: WalStreamWriter<FS::File>,
        buf_wal: WalStreamWriter<FS::File>,
        active_std_writer: Option<BlobSegmentWriter<FS::File>>,
        active_lru_writers: HashMap<u16, BlobSegmentWriter<FS::File>>,
    }

    #[must_use = "HealthyEngine proves unpoisoned state for one durable operation"]
    pub(super) struct HealthyEngine<'a, FS: PlatformFs> {
        storage: &'a mut HealthGatedStorage<FS>,
    }

    impl<FS: PlatformFs> HealthGatedStorage<FS> {
        pub(super) fn new(
            meta_mgr: BootMetaManager<FS>,
            sync_wal: WalStreamWriter<FS::File>,
            buf_wal: WalStreamWriter<FS::File>,
        ) -> Self {
            Self {
                health: EngineHealth::Healthy,
                meta_mgr,
                sync_wal,
                buf_wal,
                active_std_writer: None,
                active_lru_writers: HashMap::new(),
            }
        }

        #[inline]
        pub(super) fn is_poisoned(&self, commit_gate: &CommitGate) -> bool {
            !matches!(self.health, EngineHealth::Healthy)
                || self.meta_mgr.is_poisoned()
                || commit_gate.is_failed()
        }

        pub(super) fn check_healthy(&self, commit_gate: &CommitGate) -> io::Result<()> {
            self.meta_mgr.check_healthy()?;
            if let EngineHealth::Poisoned(msg) = &self.health {
                return Err(io::Error::other(format!(
                    "engine is poisoned by an earlier WAL I/O failure and must be reopened: {msg}"
                )));
            }
            if let Some(err) = commit_gate.failed_error() {
                return Err(err);
            }
            if commit_gate.is_failed() {
                return Err(io::Error::other(
                    "engine is poisoned after a group-commit leader failure and must be reopened",
                ));
            }
            Ok(())
        }

        /// Records a WAL or blob-sync failure (first one wins) and returns the original error.
        pub(super) fn poison(&mut self, e: io::Error) -> io::Error {
            if matches!(self.health, EngineHealth::Healthy) {
                self.health = EngineHealth::Poisoned(e.to_string());
            }
            e
        }

        #[inline]
        pub(super) fn try_healthy(
            &mut self,
            commit_gate: &CommitGate,
        ) -> io::Result<HealthyEngine<'_, FS>> {
            self.check_healthy(commit_gate)?;
            Ok(HealthyEngine { storage: self })
        }

        #[inline]
        pub(super) fn meta_mgr(&self) -> &BootMetaManager<FS> {
            &self.meta_mgr
        }

        #[inline]
        pub(super) fn sync_wal(&self) -> &WalStreamWriter<FS::File> {
            &self.sync_wal
        }

        #[inline]
        pub(super) fn buf_wal(&self) -> &WalStreamWriter<FS::File> {
            &self.buf_wal
        }

        #[inline]
        pub(super) fn set_sync_defer_barrier(&mut self, defer: bool) {
            self.sync_wal.set_defer_barrier(defer);
        }

        #[inline]
        pub(super) fn active_std_seq_id(&self) -> Option<u64> {
            self.active_std_writer.as_ref().map(|w| w.seq_id())
        }

        #[cfg(test)]
        #[inline]
        pub(super) fn active_std_len(&self) -> Option<u64> {
            self.active_std_writer.as_ref().map(|w| w.current_len())
        }

        #[inline]
        pub(super) fn active_lru_seq_id(&self, table_id: u16) -> Option<u64> {
            self.active_lru_writers.get(&table_id).map(|w| w.seq_id())
        }

        #[inline]
        pub(super) fn active_lru_table_ids(&self) -> Vec<u16> {
            self.active_lru_writers.keys().copied().collect()
        }

        #[inline]
        pub(super) fn blob_unflushed(&self, ptr: &BlobPtr) -> bool {
            let end = (ptr.offset as u64).saturating_add(ptr.len as u64);
            match ptr.kind {
                BlobKind::Standard => self
                    .active_std_writer
                    .as_ref()
                    .is_some_and(|w| w.seq_id() == ptr.seg_id && end > w.flushed_offset()),
                BlobKind::Lru => self
                    .active_lru_writers
                    .get(&ptr.table_id)
                    .is_some_and(|w| w.seq_id() == ptr.seg_id && end > w.flushed_offset()),
            }
        }

        pub(super) fn any_blob_unflushed(&self) -> bool {
            self.active_std_writer
                .as_ref()
                .is_some_and(|w| w.current_len() > w.flushed_offset())
                || self
                    .active_lru_writers
                    .values()
                    .any(|w| w.current_len() > w.flushed_offset())
        }

        #[inline]
        pub(super) fn is_blob_flushed_for_cache(&self, ptr: &BlobPtr) -> bool {
            !self.blob_unflushed(ptr)
        }

        #[inline]
        pub(super) fn active_std_needs_roll(&self, needed: u64, max_bytes: u64) -> bool {
            self.active_std_writer
                .as_ref()
                .map(|w| w.current_len() > 0 && w.current_len() + needed > max_bytes)
                .unwrap_or(true)
        }

        #[inline]
        pub(super) fn active_lru_needs_roll(
            &self,
            table_id: u16,
            needed: u64,
            max_bytes: u64,
        ) -> bool {
            self.active_lru_writers
                .get(&table_id)
                .map(|w| w.current_len() > 0 && w.current_len() + needed > max_bytes)
                .unwrap_or(true)
        }

        /// Flushes in-RAM blob write buffers to OS handles so read paths (which remain permitted
        /// on a poisoned engine) can read buffered owner blobs without altering durability.
        pub(super) fn flush_buffered_blobs_for_read(
            &mut self,
            std_blobs: &mut BTreeMap<u64, TrackedStdBlob<FS::File>>,
            lru_blobs: &mut BTreeMap<(u16, u64), TrackedLruBlob<FS::File>>,
        ) -> io::Result<()> {
            if let Some(w) = &mut self.active_std_writer {
                w.flush_buffer()?;
                if let Some(b) = std_blobs.get_mut(&w.seq_id()) {
                    b.file_len = w.current_len();
                }
            }
            for w in self.active_lru_writers.values_mut() {
                w.flush_buffer()?;
                if let Some(b) = lru_blobs.get_mut(&(w.table_id(), w.seq_id())) {
                    b.file_len = w.current_len();
                }
            }
            Ok(())
        }

        pub(super) fn ensure_blob_flushed_for_read(
            &mut self,
            ptr: &BlobPtr,
            std_blobs: &mut BTreeMap<u64, TrackedStdBlob<FS::File>>,
            lru_blobs: &mut BTreeMap<(u16, u64), TrackedLruBlob<FS::File>>,
        ) -> io::Result<()> {
            match ptr.kind {
                BlobKind::Standard => {
                    if let Some(w) = &mut self.active_std_writer {
                        if w.seq_id() == ptr.seg_id
                            && (ptr.offset as u64).saturating_add(ptr.len as u64)
                                > w.flushed_offset()
                        {
                            w.flush_buffer()?;
                            if let Some(b) = std_blobs.get_mut(&w.seq_id()) {
                                b.file_len = w.current_len();
                            }
                        }
                    }
                }
                BlobKind::Lru => {
                    if let Some(w) = self.active_lru_writers.get_mut(&ptr.table_id) {
                        if w.seq_id() == ptr.seg_id
                            && (ptr.offset as u64).saturating_add(ptr.len as u64)
                                > w.flushed_offset()
                        {
                            w.flush_buffer()?;
                            if let Some(b) = lru_blobs.get_mut(&(ptr.table_id, w.seq_id())) {
                                b.file_len = w.current_len();
                            }
                        }
                    }
                }
            }
            Ok(())
        }

        #[cfg(test)]
        pub(super) fn debug_meta_mgr_mut(&mut self) -> &mut BootMetaManager<FS> {
            &mut self.meta_mgr
        }

        #[cfg(test)]
        pub(super) fn debug_discard_buf_wal_user_space(&mut self) {
            self.buf_wal.discard_user_space_buffer_for_test();
        }

        #[cfg(test)]
        pub(super) fn debug_append_sync_batch(&mut self, batch: &crate::wal::WalBatch) {
            self.sync_wal.append_batch(batch).unwrap();
        }

        #[cfg(test)]
        pub(super) fn debug_clear_active_std_writer(&mut self) {
            self.active_std_writer = None;
        }
    }

    impl<FS: PlatformFs> HealthyEngine<'_, FS> {
        pub(super) fn commit_meta_update<M: FnOnce(&mut BootMetaState)>(
            self,
            mutate: M,
        ) -> io::Result<()> {
            self.storage.meta_mgr.commit_update(mutate)
        }

        pub(super) fn commit_wal_epoch_rotation<M: FnOnce(&mut BootMetaState)>(
            self,
            new_sync_epoch: u64,
            new_buf_epoch: u64,
            mutate: M,
        ) -> io::Result<()> {
            self.storage.meta_mgr.commit_update(mutate)?;
            self.storage.sync_wal.reset_for_new_epoch(new_sync_epoch)?;
            self.storage.buf_wal.reset_for_new_epoch(new_buf_epoch)?;
            Ok(())
        }

        pub(super) fn commit_and_install_std_writer<M: FnOnce(&mut BootMetaState)>(
            self,
            writer: BlobSegmentWriter<FS::File>,
            mutate: M,
        ) -> io::Result<()> {
            self.storage.meta_mgr.commit_update(mutate)?;
            self.storage.active_std_writer = Some(writer);
            Ok(())
        }

        pub(super) fn commit_and_install_lru_writer<M: FnOnce(&mut BootMetaState)>(
            self,
            table_id: u16,
            writer: BlobSegmentWriter<FS::File>,
            mutate: M,
        ) -> io::Result<()> {
            self.storage.meta_mgr.commit_update(mutate)?;
            self.storage.active_lru_writers.insert(table_id, writer);
            Ok(())
        }

        pub(super) fn reserve_seq_id(self) -> io::Result<u64> {
            self.storage.meta_mgr.try_reserve_seq_id()
        }

        pub(super) fn try_process_pending_deletes(self) {
            self.storage.meta_mgr.try_process_pending_deletes();
        }

        pub(super) fn sweep_orphans(self) -> io::Result<()> {
            self.storage.meta_mgr.sweep_orphans()
        }

        pub(super) fn append_wal_raw_batch(
            self,
            durability: WriteDurability,
            encoded: &[u8],
        ) -> io::Result<WalAppendOutcome> {
            let res = match durability {
                WriteDurability::SyncTransactional => {
                    self.storage.sync_wal.append_raw_batch(encoded)
                }
                WriteDurability::BufferedFast => self.storage.buf_wal.append_raw_batch(encoded),
            };
            res.map_err(|e| self.storage.poison(e))
        }

        pub(super) fn make_buf_wal_durable_and_seal(self, durable: bool) -> io::Result<()> {
            self.storage
                .buf_wal
                .make_durable_and_seal(durable)
                .map_err(|e| self.storage.poison(e))
        }

        pub(super) fn flush_buf_wal_to_os(self) -> io::Result<()> {
            self.storage.buf_wal.flush_buffered_to_os()
        }

        pub(super) fn ensure_wal_min_size(
            self,
            durability: WriteDurability,
            target_bytes: u64,
        ) -> io::Result<()> {
            match durability {
                WriteDurability::SyncTransactional => {
                    self.storage.sync_wal.ensure_min_size(target_bytes)
                }
                WriteDurability::BufferedFast => self.storage.buf_wal.ensure_min_size(target_bytes),
            }
        }

        pub(super) fn flush_active_blob_buffers(
            self,
            std_blobs: &mut BTreeMap<u64, TrackedStdBlob<FS::File>>,
            lru_blobs: &mut BTreeMap<(u16, u64), TrackedLruBlob<FS::File>>,
        ) -> io::Result<()> {
            self.storage
                .flush_buffered_blobs_for_read(std_blobs, lru_blobs)
        }

        pub(super) fn sync_active_std_writer(
            self,
            std_blobs: &mut BTreeMap<u64, TrackedStdBlob<FS::File>>,
            seal: bool,
        ) -> io::Result<()> {
            let Some(w) = &mut self.storage.active_std_writer else {
                return Ok(());
            };
            // A failed write never reached the page cache and its buffer is kept, so it is retryable.
            w.flush_buffer()?;
            let synced = w.sync_durable();
            let (seq_id, len) = (w.seq_id(), w.current_len());
            synced.map_err(|e| self.storage.poison(e))?;
            if let Some(b) = std_blobs.get_mut(&seq_id) {
                b.file_len = len;
            }
            if seal {
                self.storage.active_std_writer = None;
            }
            Ok(())
        }

        pub(super) fn sync_active_lru_writer(
            self,
            table_id: u16,
            lru_blobs: &mut BTreeMap<(u16, u64), TrackedLruBlob<FS::File>>,
            seal: bool,
        ) -> io::Result<()> {
            let Some(w) = self.storage.active_lru_writers.get_mut(&table_id) else {
                return Ok(());
            };
            // A failed write never reached the page cache and its buffer is kept, so it is retryable.
            w.flush_buffer()?;
            let synced = w.sync_durable();
            let (seq_id, len) = (w.seq_id(), w.current_len());
            synced.map_err(|e| self.storage.poison(e))?;
            if let Some(b) = lru_blobs.get_mut(&(table_id, seq_id)) {
                b.file_len = len;
            }
            if seal {
                self.storage.active_lru_writers.remove(&table_id);
            }
            Ok(())
        }

        pub(super) fn sync_and_remove_dropped_lru_writer(
            self,
            table_id: u16,
            lru_blobs: &mut BTreeMap<(u16, u64), TrackedLruBlob<FS::File>>,
        ) -> io::Result<()> {
            let Some(mut old_w) = self.storage.active_lru_writers.remove(&table_id) else {
                return Ok(());
            };
            if let Err(e) = old_w.sync_durable() {
                self.storage.active_lru_writers.insert(table_id, old_w);
                return Err(self.storage.poison(e));
            }
            if let Some(b) = lru_blobs.get_mut(&(table_id, old_w.seq_id())) {
                b.file_len = old_w.current_len();
                b.dead_bytes = b.file_len;
            }
            Ok(())
        }

        pub(super) fn append_std_blob(
            self,
            key: &[u8],
            value: &[u8],
        ) -> io::Result<(u64, BlobPtr, u64)> {
            let writer = self
                .storage
                .active_std_writer
                .as_mut()
                .expect("std blob writer must be installed before append");
            let seg_id = writer.seq_id();
            let ptr = writer.append_blob(key, value)?;
            let new_len = writer.current_len();
            Ok((seg_id, ptr, new_len))
        }

        pub(super) fn append_lru_blob(
            self,
            table_id: u16,
            key: &[u8],
            value: &[u8],
        ) -> io::Result<(u64, BlobPtr, u64)> {
            let writer = self
                .storage
                .active_lru_writers
                .get_mut(&table_id)
                .expect("lru blob writer must be installed before append");
            let seg_id = writer.seq_id();
            let ptr = writer.append_blob(key, value)?;
            let new_len = writer.current_len();
            Ok((seg_id, ptr, new_len))
        }
    }
}

/// Result of a policy-free `'t'` row-meta lookup.
pub(super) enum MetaLookup {
    Absent,
    Found(RowMetaRecord, Option<BlobPtr>),
    /// Record exists but cannot be read/decoded (carries the spilled blob pointer, if any).
    Corrupt(Option<BlobPtr>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ExistingRowStatus {
    NotFound,
    Inline,
    Blob(BlobPtr),
}

/// Outcome of probing one `idx` segment for a row key without materializing its value.
enum SegProbe {
    /// The inline value was appended to the caller's slab.
    Inline,
    Blob(BlobPtr),
    Tombstone,
}

/// Classifies a borrowed segment entry, appending an inline value to `slab`.
fn probe_raw_into_slab(raw: RawIdxEntry<'_>, slab: &mut Vec<u8>) -> io::Result<SegProbe> {
    match raw.val_type {
        1 => {
            slab.extend_from_slice(raw.val_bytes);
            Ok(SegProbe::Inline)
        }
        2 => Ok(SegProbe::Blob(RawIdxEntry::decode_blob_ptr(raw.val_bytes)?)),
        3 => Ok(SegProbe::Tombstone),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown IndexValue type_tag: {other}"),
        )),
    }
}

/// Durability guarantee requested for a write or delete operation.
///
/// If a WAL append or sync, or a blob segment sync, fails, the call returns an error and the
/// engine becomes *poisoned* (see [`StorageEngine::is_poisoned`]): the on-disk outcome of the failed write is unknown, so
/// after reopening the failed write may or may not be present (but never partially applied).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteDurability {
    /// Logs to `wal_sync.db` and flushes the device (`FlushFileBuffers` on Windows, `fdatasync`
    /// on Linux, `F_FULLFSYNC` on macOS), surviving immediate physical power loss upon return.
    SyncTransactional,
    /// Logs to `wal_buf.db` (OS Page Cache), surviving application crashes (`kill -9`)
    /// with microsecond latency.
    BufferedFast,
}

/// What a read does when an out-of-line blob fails verification (CRC mismatch, bad magic, key
/// mismatch, short read) or its segment is missing after the engine has opened.
///
/// After `StorageEngine::open`, every blob referenced by durable state has been validated
/// (WAL-replayed pointers) or synced before its index checkpoint, so a verification failure
/// means genuine corruption. Runtime-only: not persisted (the vtab re-applies it on connect).
/// Defaults: capped tables use [`CorruptPolicy::Evict`], all others [`CorruptPolicy::Error`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CorruptPolicy {
    /// Reads of the corrupt row fail with `io::ErrorKind::InvalidData` naming the table id and
    /// primary key. Explicit deletes/overwrites of that row still succeed and clean up fully.
    Error,
    /// Reads treat the row as absent and the row is queued for automatic deletion at the next
    /// safe mutating point (see `StorageEngine::drain_corrupt_evictions`).
    Evict,
}

/// What `StorageEngine::open` does when recovery finds that acknowledged commits were lost from
/// the WAL (media damage, never the unsynced tail of a power cut; see ARCHITECTURE.md 4.6).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum WalDamagePolicy {
    /// Replays the verified prefix of the log, discards the rest and opens. The loss is visible
    /// only in [`StorageEngine::last_wal_recovery`].
    #[default]
    Truncate,
    /// `open` fails with `io::ErrorKind::InvalidData` whose inner error is the [`WalDamage`]
    /// (reach it with `get_ref()` and `downcast_ref`). The decision comes before anything is
    /// written or deleted, so every file is left as it was.
    Fail,
}

/// Controls how the engine tracks read-path `AccessTick` updates on Capped Tables.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LruReadTrackingMode {
    /// Default architecture: Updates only the bounded in-memory [`AccessTracker`] on `get_row()`
    /// (`O(1)`, zero disk I/O) and lazily promotes hot entries only when scanned during eviction.
    CoalescedLazyPromotion,
    /// Baseline comparison mode (for benchmarking): Immediately logs and indexes a new
    /// `[TableID | 'l' | NewTick | PK]` mutation on every `get_row()` hit.
    ///
    /// Because persisting promotions requires mutable engine state, reads on capped tables
    /// (including SQL snapshot reads outside active transactions while the engine is healthy)
    /// take the exclusive engine lock and serialize concurrent readers on those tables.
    EagerWriteOnRead,
}

/// Default number of immutable `idx_<SeqID>.db` segments that triggers automatic compaction.
pub const DEFAULT_AUTO_COMPACT_SEGMENT_THRESHOLD: usize = 8;

/// Default maximum in-memory transaction footprint (`64 MB`). `0` disables the limit.
pub const DEFAULT_MAX_TRANSACTION_BYTES: usize = 64 * 1024 * 1024;

/// Default block cache capacity in bytes (`4 MB`). `0` disables the cache.
pub const DEFAULT_BLOCK_CACHE_BYTES: usize = 4 * 1024 * 1024;

/// Default small blob cache capacity in bytes (`4 MB`). `0` disables the cache.
pub const DEFAULT_BLOB_CACHE_BYTES: usize = 4 * 1024 * 1024;

/// Configuration parameters for [`StorageEngine`].
#[derive(Debug, Clone)]
pub struct EngineConfig {
    pub sync_wal_size_bytes: u64,
    pub buf_wal_size_bytes: u64,
    pub buf_wal_mode: WalStreamMode,
    pub memtable_capacity_bytes: usize,
    pub inline_threshold_bytes: usize,
    pub max_blob_segment_bytes: u64,
    pub bloom_bits_per_key: usize,
    pub lru_read_tracking: LruReadTrackingMode,
    pub max_in_memory_access_ticks: usize,
    /// Automatically triggers `compact_auto_tiered()` after `flush_memtable()` when the active
    /// segment count reaches this threshold (`0` disables automatic compaction).
    pub auto_compact_segment_threshold: usize,
    /// When `true`, `flush_memtable()` does NOT merge segments inline; the segment count merely
    /// becomes visible through [`StorageEngine::compaction_pending`] and an external worker is
    /// expected to run [`StorageEngine::prepare_compaction`] -> [`CompactionJob::run`] (without
    /// the engine) -> [`StorageEngine::install_compaction`]. Default `false` (inline compaction).
    pub background_compaction: bool,
    /// Backpressure bound for `background_compaction`: once the segment count reaches this value
    /// the flush falls back to inline compaction. `0` means `2 * auto_compact_segment_threshold`.
    pub max_segments_before_inline_compaction: usize,
    /// How long a SQL writer waits for another connection's transaction to end before failing
    /// with `SQLITE_BUSY` (`0` fails immediately).
    pub writer_wait_timeout_ms: u32,
    /// Maximum uncommitted transaction payload + WAL record budget in bytes (`0` = unlimited).
    pub max_transaction_bytes: usize,
    /// Total capacity for in-process verified 4 KB index block cache (`0` disables block cache).
    pub block_cache_bytes: usize,
    /// Total capacity for in-process verified small blob payload cache (`0` disables blob cache).
    pub blob_cache_bytes: usize,
    /// What `open` does when the WAL lost acknowledged commits (default: truncate and continue).
    pub wal_damage_policy: WalDamagePolicy,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            sync_wal_size_bytes: DEFAULT_SYNC_WAL_SIZE,
            buf_wal_size_bytes: DEFAULT_BUF_WAL_SIZE,
            buf_wal_mode: WalStreamMode::BufferedImmediateOsCache,
            memtable_capacity_bytes: DEFAULT_MEMTABLE_CAPACITY_BYTES,
            inline_threshold_bytes: DEFAULT_INLINE_THRESHOLD,
            max_blob_segment_bytes: DEFAULT_MAX_BLOB_SEGMENT_SIZE,
            bloom_bits_per_key: DEFAULT_BLOOM_BITS_PER_KEY,
            lru_read_tracking: LruReadTrackingMode::CoalescedLazyPromotion,
            max_in_memory_access_ticks: 65_536,
            auto_compact_segment_threshold: DEFAULT_AUTO_COMPACT_SEGMENT_THRESHOLD,
            background_compaction: false,
            max_segments_before_inline_compaction: 0,
            writer_wait_timeout_ms: 1_000,
            max_transaction_bytes: DEFAULT_MAX_TRANSACTION_BYTES,
            block_cache_bytes: DEFAULT_BLOCK_CACHE_BYTES,
            blob_cache_bytes: DEFAULT_BLOB_CACHE_BYTES,
            wal_damage_policy: WalDamagePolicy::default(),
        }
    }
}

/// A lazily-dereferenced out-of-line blob captured by a cursor.
///
/// Holds the blob pointer together with a clone of the owning segment's file handle (`reader`).
/// Pinning the handle keeps the bytes readable even if the segment is compacted/reclaimed and
/// removed from the engine maps before the cursor dereferences it:
/// - Unix: an unlinked file stays readable through an open descriptor.
/// - Windows: files are opened with share mode 0 so `DeleteFile` fails while the handle is open;
///   the file name stays queued in `pending_delete_files` (see `try_process_pending_deletes` in
///   `meta`, which keeps failed deletes queued and retries them on later commits/startup).
///
/// `reader` is `None` if the segment was already absent when the pointer was captured.
pub struct LazyBlob<F: PlatformFile> {
    pub ptr: BlobPtr,
    pub(in crate::engine) reader: Option<BlobSegmentReader<F>>,
}

impl<F: PlatformFile> Clone for LazyBlob<F> {
    fn clone(&self) -> Self {
        Self {
            ptr: self.ptr,
            reader: self.reader.clone(),
        }
    }
}

pub(super) struct TrackedStdBlob<F: PlatformFile> {
    pub(super) reader: BlobSegmentReader<F>,
    pub(super) file_len: u64,
    pub(super) dead_bytes: u64,
}

pub(super) struct TrackedLruBlob<F: PlatformFile> {
    pub(super) table_id: u16,
    pub(super) reader: BlobSegmentReader<F>,
    pub(super) file_len: u64,
    pub(super) dead_bytes: u64,
}

/// Unified Phase 1 Storage Engine.
pub struct StorageEngine<FS: PlatformFs> {
    pub(in crate::engine) engine_id: u64,
    pub(in crate::engine) snapshot_cache: RwLock<snapshot::SnapshotCache<FS::File>>,
    pub(super) fs: Arc<FS>,
    pub(super) config: EngineConfig,
    /// Poison-gated storage owner wrapping `BootMetaManager`, WAL writers, and active blob writers.
    pub(in crate::engine) storage: health::HealthGatedStorage<FS>,
    pub(super) next_lsn: u64,
    pub(super) memtable: MemTable,
    /// Active immutable index segments (`idx_<SeqID>.db`), ordered oldest to newest (`last()` is newest).
    pub(super) idx_segments: Vec<Arc<IdxSegmentReader<FS::File>>>,
    /// Active standard blob segments (`seq_id -> reader + stats`).
    pub(super) std_blobs: BTreeMap<u64, TrackedStdBlob<FS::File>>,
    /// Active per-table LRU blob segments (`(table_id, seq_id) -> reader + stats`).
    pub(super) lru_blobs: BTreeMap<(u16, u64), TrackedLruBlob<FS::File>>,
    /// Capped tables (`table_id -> CappedTableState`).
    pub(super) capped_tables: HashMap<u16, CappedTableState>,
    /// Bounded in-memory `AccessTick` tracker.
    pub(super) access_tracker: AccessTracker,
    /// Transaction state machine (`Idle` or `InTx { phase, tx }`). While active, serialized
    /// `WalRecord` bytes are accumulated inside `ActiveTx::pending_buf` and committed as a
    /// single atomic `WalBatch` on `commit_transaction`!
    pub(in crate::engine) tx_state: tx::TxState,
    /// Capacity-only reusable buffer recycled from completed/aborted transactions.
    pub(in crate::engine) scratch_tx_buf: Vec<u8>,
    /// Coordinates writers waiting for their turn and commits sharing a device flush.
    pub(super) commit_gate: Arc<tx::CommitGate>,
    /// Committed batches written to `wal_sync.db` whose device flush has not finished, in LSN
    /// order, as `(batch LSN, entries)`. The next writer's overlay starts from these entries;
    /// readers see them only after they move to the `MemTable`.
    pub(super) pending_group:
        std::collections::VecDeque<(u64, BTreeMap<Vec<u8>, crate::memtable::MemEntry>)>,
    /// Fully dead `blob_lru` segments that were dropped from the catalog but not yet queued for
    /// deletion, because a WAL batch of the current epoch may still carry a pointer into them.
    /// They are queued in the commit that rotates the WAL epochs (see
    /// [`Self::release_unlinkable_blob_files`]). A crash before that leaves them as uncataloged
    /// files, which recovery adopts if a batch references them and sweeps otherwise.
    pub(super) unlink_after_wal_rotation: Vec<String>,
    /// Set when a full-WAL rotation flushed the MemTable without auto compaction; consumed by
    /// `run_due_auto_compaction` at the next point where no batch holds an unappended LSN.
    pub(super) auto_compaction_due: bool,
    /// Re-entrancy guard preventing `relocate_fragmented_std_blobs` from triggering another
    /// relocation pass if its internal MemTable flush invokes auto-compaction. Mutated only via
    /// [`RelocatingBlobsGuard`].
    pub(in crate::engine) relocating_std_blobs: ScopedRelocatingFlag,
    /// Table currently executing initial `register_capped_table` reconciliation; excluded from
    /// checkpointed `table_quotas` until reconciliation completes. Mutated only via
    /// [`ReconcilingCappedGuard`].
    pub(in crate::engine) reconciling_capped_table: ScopedReconcilingTable,
    /// Sealed `blob_std` segment IDs containing an unreadable corrupt blob under lenient
    /// relocation, skipped on subsequent relocation passes until reclaimed by compaction/drop.
    pub(super) unrelocatable_std_blobs: std::collections::HashSet<u64>,
    /// Invoked (under the engine lock, so it must be cheap and non-blocking) when a flush leaves
    /// compaction pending in background mode.
    pub(super) compaction_hook: Option<Arc<dyn Fn() + Send + Sync>>,
    /// Generation id of the worker that owns [`Self::compaction_hook`] so an older worker's teardown
    /// cannot clear a newly registered worker's hook.
    pub(super) compaction_hook_id: u64,
    /// Initial `(kind, tid_key, seq_id, prev_file_len)` recorded for autocommit (`Idle` /
    /// non-tx) blob appends not yet appended to the WAL (`tid_key = 0` for
    /// `BlobKind::Standard`), so a `RingFull` WAL rotation checkpoint never catalogs uncommitted
    /// blob lengths ahead of the batch.
    pub(super) in_flight_autocommit_blob_starts: Vec<(BlobKind, u16, u64, u64)>,
    /// Registry of active virtual tables (`table_id -> table_name`) preventing duplicate `table_id` sharing.
    pub(super) active_vtab_ids: HashMap<u16, Arc<str>>,
    /// Monotonic generation bumped whenever any persisted UNIQUE completion marker changes.
    pub(super) unique_marker_gen: u64,
    /// Monotonic registration token counter for live virtual table instances.
    pub(in crate::engine) next_vtab_token: u64,
    /// Live virtual table holder tokens per `table_id` (reference-counted across connections).
    pub(super) active_vtab_holders: HashMap<u16, Vec<u64>>,
    /// Bumped by every change that can add or remove a row as some view sees it: each write,
    /// each commit, each rollback (also to a savepoint), each drop. A value derived from the
    /// rows (a cache) stays valid exactly as long as the epoch does not move.
    pub(super) mutation_epoch: u64,
    /// Per-table counterpart to [`Self::mutation_epoch`]: bumped only for tables whose primary rows
    /// or persistent rowid mappings changed, preventing writes on unrelated tables from
    /// invalidating another table's `auto_key_cache`.
    pub(super) table_mutation_epochs: HashMap<u16, u64>,
    /// Lazy per-table cache indicating whether `table_id` may contain synthetic rowid mappings in
    /// `NS_ROWID_BY_PK` (`'u'`) or non-marker `NS_PK_BY_ROWID` (`'v'`) entries.
    pub(super) table_has_synthetic_rowids: BTreeMap<u16, bool>,
    /// Monotonic counter of committed user-data publications, incremented when committed batches
    /// (other than internal `EagerWriteOnRead` LRU tick updates) enter the `MemTable`. Used by
    /// read snapshots to detect concurrent writes without false write-upgrade aborts.
    pub(super) data_generation: u64,
    /// Lazy per-table cache indicating whether `table_id` may contain `'t'` (`NS_ROW_META`) keys.
    /// Standard uncapped tables without secondary indexes resolve to `false` once and skip
    /// all `'t'` negative point lookups on `UPDATE` and `DELETE`!
    pub(super) table_has_row_meta: BTreeMap<u16, bool>,
    /// Highest LSN appended to `buf_wal`.
    pub(super) last_buf_append_lsn: u64,
    /// Highest LSN in `buf_wal` synced to durable storage.
    pub(super) last_buf_synced_lsn: u64,
    /// Predecessor link and durable watermark stamped into every WAL batch (section 4.6).
    pub(in crate::engine) wal_links: wal_links::WalLinks,
    /// What recovery found in the WAL during `open` (section 4.6.8).
    pub(in crate::engine) last_wal_recovery: WalRecoveryReport,
    /// Number of post-commit maintenance steps (flush, quota eviction, reclaim, compaction) that
    /// failed after their triggering write was already committed.
    pub(in crate::engine) maintenance_failures: AtomicU64,
    /// Text of the most recent post-commit maintenance failure.
    pub(in crate::engine) last_maintenance_error: Mutex<Option<String>>,
    /// Explicit per-table corruption policies (absent -> derived default, see
    /// [`Self::corrupt_policy`]).
    pub(super) corrupt_policies: HashMap<u16, CorruptPolicy>,
    /// Explicit per-table inline threshold overrides in bytes (see [`Self::set_table_inline_threshold`]).
    pub(super) table_inline_thresholds: BTreeMap<u16, usize>,
    /// Scratch buffer for encoded WAL batches on commit, avoiding per-commit heap allocation.
    pub(super) scratch_wal_encoded: Vec<u8>,
    /// In-process verified 4 KB index block cache.
    pub(in crate::engine) block_cache: Arc<VerifiedBlockCache>,
    /// In-process small verified blob payload cache.
    pub(in crate::engine) blob_cache: Arc<VerifiedBlobCache>,
    /// Engine-wide count of blob verification failures observed by reads/cleanup paths.
    pub(in crate::engine) corrupt_blob_detections: AtomicU64,
    /// Rows awaiting automatic deletion under [`CorruptPolicy::Evict`]. Filled from `&self` read
    /// paths (hence the `Mutex`); drained only by [`Self::drain_corrupt_evictions`].
    pub(in crate::engine) pending_corrupt_evictions:
        Mutex<HashMap<(u16, Vec<u8>), PendingCorruptEviction>>,
    /// Test seam: makes the next `append_raw_wal_batch_with_rotation` fail and poison the engine.
    #[cfg(test)]
    pub(super) debug_fail_next_wal_append: bool,
    /// Test seam: makes the next vtab commit `sync_barrier` fail as a device flush failure would.
    #[cfg(test)]
    pub(super) debug_fail_next_wal_sync: Arc<std::sync::atomic::AtomicBool>,
    /// Test-only Sans-I/O `tx_fsm` trace recorder attached to [`StorageEngine::apply_tx_event`].
    #[cfg(test)]
    pub(super) tx_trace: Option<tx_fsm::TxTraceRecorder>,
}

impl<FS: PlatformFs> StorageEngine<FS> {
    #[inline]
    pub(super) fn take_scratch_tx_buf(&mut self) -> Vec<u8> {
        let mut buf = core::mem::take(&mut self.scratch_tx_buf);
        buf.clear();
        buf
    }

    #[inline]
    pub(super) fn recycle_tx_buf(&mut self, mut buf: Vec<u8>) {
        buf.clear();
        if buf.capacity() > self.scratch_tx_buf.capacity() {
            self.scratch_tx_buf = buf;
        }
    }

    #[inline]
    pub(super) fn reconciling_capped_table(&self) -> Option<u16> {
        self.reconciling_capped_table.get()
    }

    #[inline]
    pub(crate) fn is_relocating_std_blobs(&self) -> bool {
        self.relocating_std_blobs.is_active()
    }

    #[inline]
    pub(in crate::engine) fn healthy(&mut self) -> io::Result<health::HealthyEngine<'_, FS>> {
        self.storage.try_healthy(&self.commit_gate)
    }

    #[inline]
    pub(in crate::engine) fn meta_mgr(&self) -> &BootMetaManager<FS> {
        self.storage.meta_mgr()
    }

    #[inline]
    pub(in crate::engine) fn sync_wal(&self) -> &WalStreamWriter<FS::File> {
        self.storage.sync_wal()
    }

    #[inline]
    pub(in crate::engine) fn buf_wal(&self) -> &WalStreamWriter<FS::File> {
        self.storage.buf_wal()
    }

    #[inline]
    pub(crate) fn active_std_seq_id(&self) -> Option<u64> {
        self.storage.active_std_seq_id()
    }

    #[inline]
    pub(in crate::engine) fn active_lru_seq_id(&self, table_id: u16) -> Option<u64> {
        self.storage.active_lru_seq_id(table_id)
    }

    /// Returns true if a WAL, metadata, or group-commit flush failure poisoned this engine. Reads keep
    /// working, but all mutations fail until the engine is reopened.
    pub fn is_poisoned(&self) -> bool {
        self.storage.is_poisoned(&self.commit_gate)
    }

    /// Test seam: poisons the engine as a failed WAL write would.
    #[cfg(test)]
    pub(crate) fn debug_poison(&mut self, reason: &str) {
        let _ = self.poison(io::Error::other(reason.to_string()));
    }

    /// Test seam: causes the next WAL batch append to fail and poison the engine.
    #[cfg(test)]
    pub(crate) fn debug_fail_next_wal_append(&mut self) {
        self.debug_fail_next_wal_append = true;
    }

    /// Test seam: causes the next vtab commit `sync_barrier` to fail as a device flush error.
    #[cfg(test)]
    pub(crate) fn debug_fail_next_wal_sync(&self) {
        self.debug_fail_next_wal_sync.store(true, Ordering::SeqCst);
    }

    /// Test seam: returns the shared sync-failure injection flag.
    #[cfg(test)]
    pub(crate) fn debug_fail_wal_sync_flag(&self) -> Arc<std::sync::atomic::AtomicBool> {
        Arc::clone(&self.debug_fail_next_wal_sync)
    }

    /// Returns how many post-commit maintenance steps have failed. The writes that triggered them
    /// were committed; the work is retried because `should_flush` stays true and an exceeded quota
    /// stays exceeded.
    pub fn maintenance_failures(&self) -> u64 {
        self.maintenance_failures.load(Ordering::Relaxed)
    }

    /// Returns the text of the most recent post-commit maintenance failure, if any.
    pub fn last_maintenance_error(&self) -> Option<String> {
        self.last_maintenance_error
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Records a failure of a maintenance step that ran after its write was committed, so the
    /// caller still sees the commit as successful. WAL I/O failures have already poisoned the
    /// engine at their source.
    pub(crate) fn record_maintenance_failure(&self, e: &io::Error) {
        self.maintenance_failures.fetch_add(1, Ordering::Relaxed);
        *self
            .last_maintenance_error
            .lock()
            .unwrap_or_else(|p| p.into_inner()) = Some(e.to_string());
    }

    /// Fails if a prior WAL, metadata, or group-commit leader failure left the on-disk state uncertain.
    pub(super) fn check_not_poisoned(&self) -> io::Result<()> {
        self.storage.check_healthy(&self.commit_gate)
    }

    /// Records a WAL I/O failure (first one wins) and returns the original error.
    pub(super) fn poison(&mut self, e: io::Error) -> io::Error {
        let e = self.storage.poison(e);
        let _ = self.apply_tx_event(TxEvent::MarkPoisoned);
        e
    }

    /// How long a SQL writer waits for its turn before failing with `SQLITE_BUSY`.
    pub(crate) fn writer_wait_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_millis(u64::from(self.config.writer_wait_timeout_ms))
    }

    pub fn next_lsn(&self) -> u64 {
        self.next_lsn
    }

    /// Every assigned LSN needs a representable successor, including the final commit chunk.
    fn lsn_after_batches(&self, count: usize) -> io::Result<u64> {
        self.next_lsn
            .checked_add(count as u64)
            .ok_or_else(|| io::Error::other("WAL LSN space exhausted"))
    }

    /// Enables or disables background compaction mode at runtime (see
    /// [`EngineConfig::background_compaction`]).
    pub fn set_background_compaction(&mut self, enabled: bool) {
        self.config.background_compaction = enabled;
    }

    /// Whether background compaction mode is currently enabled.
    pub fn background_compaction_enabled(&self) -> bool {
        self.config.background_compaction
    }

    /// Installs a hook called after a flush when background compaction is pending. It runs under
    /// the caller's `&mut self`, so it must only signal (e.g. a `Condvar`), never lock the engine.
    pub fn set_compaction_hook(&mut self, hook: Option<Arc<dyn Fn() + Send + Sync>>) {
        self.compaction_hook = hook;
    }

    pub(crate) fn install_compaction_hook(
        &mut self,
        worker_id: u64,
        hook: Arc<dyn Fn() + Send + Sync>,
    ) {
        if worker_id >= self.compaction_hook_id {
            self.compaction_hook_id = worker_id;
            self.compaction_hook = Some(hook);
            self.config.background_compaction = true;
        }
    }

    pub(crate) fn clear_compaction_hook(&mut self, worker_id: u64) {
        if self.compaction_hook_id == worker_id {
            self.compaction_hook = None;
            self.config.background_compaction = false;
        }
    }

    /// Returns the number of currently active `idx_<SeqID>.db` segments.
    pub fn active_idx_segment_count(&self) -> usize {
        self.idx_segments.len()
    }

    /// Returns the number of currently active `blob_lru_<TblID>_<SeqID>.db` segments for `table_id`.
    pub fn active_lru_blob_segment_count(&self, table_id: u16) -> usize {
        self.lru_blobs
            .keys()
            .filter(|&&(tid, _)| tid == table_id)
            .count()
    }

    /// Checks whether a row with primary key `pk` currently exists in `table_id`.
    pub fn row_exists(&self, table_id: u16, pk: &[u8]) -> io::Result<bool> {
        let row_key = encode_primary_row_key(table_id, pk);
        Ok(self.check_existing_row(&row_key)? != ExistingRowStatus::NotFound)
    }

    /// Inserts or overwrites a row (`pk` -> `value`) in `table_id`, along with any secondary index
    /// `(index_id, col_val)` pairs. Automatically routes values `>= inline_threshold_bytes` to
    /// `blob_std` or `blob_lru` (`Write-Blob-Before-WAL`) and enforces `MAX_BYTES` if `table_id` is capped.
    pub fn put_row(
        &mut self,
        table_id: u16,
        pk: &[u8],
        value: &[u8],
        secondary_keys: &[(u16, Vec<u8>)],
        durability: WriteDurability,
    ) -> io::Result<()> {
        self.put_row_owned(table_id, pk, value.to_vec(), secondary_keys, durability)
    }

    /// Owned-value variant of [`Self::put_row`] that moves `value: Vec<u8>` directly into the `MemTable`
    /// without cloning when stored inline.
    pub fn put_row_owned(
        &mut self,
        table_id: u16,
        pk: &[u8],
        value: Vec<u8>,
        secondary_keys: &[(u16, Vec<u8>)],
        durability: WriteDurability,
    ) -> io::Result<()> {
        self.put_row_owned_with_hint(table_id, pk, value, secondary_keys, durability, false)
    }

    /// Inserts or overwrites a row (`pk` -> `value`) in `table_id` with an optional `known_absent`
    /// hint. When `known_absent` is `true`, the caller has just verified under the same
    /// engine lock that `pk` does not exist in `table_id`, so redundant `check_existing_row` and
    /// `get_row_meta_with_ptr` lookups are skipped completely.
    pub fn put_row_owned_with_hint(
        &mut self,
        table_id: u16,
        pk: &[u8],
        value: Vec<u8>,
        secondary_keys: &[(u16, Vec<u8>)],
        durability: WriteDurability,
        known_absent: bool,
    ) -> io::Result<()> {
        self.put_row_owned_with_hint_and_extra(
            table_id,
            pk,
            value,
            PutRowOptions {
                secondary_keys,
                extra_records: Vec::new(),
                durability,
                known_absent,
            },
        )
    }

    fn prepare_row_mutation(
        &self,
        table_id: u16,
        pk: &[u8],
        value_len: usize,
        secondary_keys: &[(u16, Vec<u8>)],
    ) -> io::Result<PreparedRowMutation> {
        if pk.len() > 2048 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "primary key exceeds maximum length (2048 bytes)",
            ));
        }
        let row_key = encode_primary_row_key(table_id, pk);
        if !IndexEntry::blob_ptr_fits_in_block(row_key.len()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "primary key exceeds maximum length (2048 bytes)",
            ));
        }

        if secondary_keys.len() > u16::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "RowMetaRecord secondary key count exceeds u16::MAX",
            ));
        }

        let mut new_sec_keys = Vec::with_capacity(secondary_keys.len());
        for (idx_id, col_val) in secondary_keys {
            let sec_key = encode_secondary_index_key(table_id, *idx_id, col_val, pk);
            if !IndexEntry::inline_fits_in_block(sec_key.len(), 0) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "secondary index key exceeds 4 KB block capacity",
                ));
            }
            new_sec_keys.push(sec_key);
        }

        let is_capped = self.capped_tables.contains_key(&table_id);
        let logical_size = (pk.len() + value_len) as u32;
        if let Some(ct) = self.capped_tables.get(&table_id) {
            if (3 + pk.len() + value_len) as u64 > ct.max_bytes {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "row logical size exceeds capped table max_bytes quota",
                ));
            }
        }

        Ok(PreparedRowMutation {
            row_key,
            new_sec_keys,
            is_capped,
            logical_size,
        })
    }

    /// Collects cascade `Delete` records and old metadata pointers for an existing row's `'t'`
    /// metadata (LRU queue entry, secondary index keys, and optional `'t'` key itself), falling
    /// back to orphan scanning when `'t'` is corrupt.
    pub(super) fn collect_old_meta_deletes(
        &mut self,
        table_id: u16,
        pk: &[u8],
        row_key: &[u8],
        opts: OldMetaDeleteOpts<'_>,
        records: &mut Vec<WalRecord>,
    ) -> io::Result<OldMetaDeleteResult> {
        let mut res = OldMetaDeleteResult {
            had_old_meta: false,
            old_meta_blob_ptr: None,
            old_logical_size: 0,
            matched_lru_key: false,
        };
        let meta_key = encode_row_meta_key(table_id, pk);
        match self.get_row_meta_lookup(&meta_key)? {
            MetaLookup::Found(old_meta, m_ptr) => {
                res.had_old_meta = true;
                res.old_meta_blob_ptr = m_ptr;
                res.old_logical_size = old_meta.logical_size as u64;
                if (!opts.require_capped_for_lru || self.capped_tables.contains_key(&table_id))
                    && old_meta.recorded_tick > 0
                {
                    let lru_k = encode_lru_queue_key(table_id, old_meta.recorded_tick, pk);
                    if opts.match_lru_key == Some(lru_k.as_slice()) {
                        res.matched_lru_key = true;
                    }
                    records.push(WalRecord::Delete { key: lru_k });
                }
                for (old_idx_id, old_col_val) in &old_meta.secondary_keys {
                    records.push(WalRecord::Delete {
                        key: encode_secondary_index_key(table_id, *old_idx_id, old_col_val, pk),
                    });
                }
                if opts.delete_meta_key {
                    records.push(WalRecord::Delete { key: meta_key });
                }
            }
            MetaLookup::Corrupt(m_ptr) => {
                // Unreadable metadata: find old 'i'/'l' entries by scanning for `pk` and estimate
                // `old_logical_size` from the row record.
                res.had_old_meta = true;
                res.old_meta_blob_ptr = m_ptr;
                let (orphans, est) = self.collect_orphans_for_pk(table_id, pk, row_key)?;
                res.old_logical_size = est;
                for key in orphans {
                    if opts.match_lru_key == Some(key.as_slice()) {
                        res.matched_lru_key = true;
                    }
                    records.push(WalRecord::Delete { key });
                }
                if opts.delete_meta_key {
                    records.push(WalRecord::Delete { key: meta_key });
                }
            }
            MetaLookup::Absent => {}
        }
        Ok(res)
    }

    /// Atomically inserts or overwrites a row (`pk` -> `value`) together with additional derived
    /// `extra_records` (such as persistent SQLite rowid mappings) in a single WAL/MemTable batch.
    pub(crate) fn put_row_owned_with_hint_and_extra(
        &mut self,
        table_id: u16,
        pk: &[u8],
        value: Vec<u8>,
        opts: PutRowOptions<'_>,
    ) -> io::Result<()> {
        let PutRowOptions {
            secondary_keys,
            extra_records,
            durability,
            known_absent,
        } = opts;
        self.check_not_poisoned()?;
        self.drain_group_for_autocommit()?;
        self.drain_corrupt_evictions()?;
        let PreparedRowMutation {
            row_key,
            new_sec_keys,
            is_capped,
            logical_size,
        } = self.prepare_row_mutation(table_id, pk, value.len(), secondary_keys)?;

        // 1. Check if the row already exists so we can clean up old blob bytes, old secondary keys,
        //    and old LRU queue entries (`[TableID | 'l' | old_tick | pk]`).
        let mut records = Vec::with_capacity(3 + secondary_keys.len() * 2);
        let mut old_logical_size = 0u64;
        let mut old_blob_ptr = None;
        let mut old_meta_blob_ptr = None;
        let mut had_old_meta = false;

        let existing = if known_absent {
            ExistingRowStatus::NotFound
        } else {
            self.check_existing_row(&row_key)?
        };
        if let ExistingRowStatus::Blob(old_ptr) = existing {
            old_blob_ptr = Some(old_ptr);
        }

        if existing != ExistingRowStatus::NotFound && self.may_have_row_meta(table_id)? {
            let old_meta = self.collect_old_meta_deletes(
                table_id,
                pk,
                &row_key,
                OldMetaDeleteOpts {
                    require_capped_for_lru: is_capped,
                    delete_meta_key: false,
                    match_lru_key: None,
                },
                &mut records,
            )?;
            had_old_meta = old_meta.had_old_meta;
            old_meta_blob_ptr = old_meta.old_meta_blob_ptr;
            old_logical_size = old_meta.old_logical_size;
        }

        // 2. Write out-of-line blob FIRST if `value.len() >= effective_inline_threshold` or if inline entry exceeds 4 KB block
        let inline_thresh = self.effective_inline_threshold(table_id);
        let store_as_blob = value.len() >= inline_thresh
            || !IndexEntry::inline_fits_in_block(row_key.len(), value.len());
        let extra_bytes: usize = extra_records
            .iter()
            .map(WalRecord::est_tx_budget_bytes)
            .sum();
        if self.config.max_transaction_bytes > 0
            && self.tx_state.is_active()
            && !self.tx_state.is_preparing()
        {
            let est_row_bytes =
                ((if store_as_blob { 28 } else { 9 }) + row_key.len() + value.len())
                    .saturating_add(extra_bytes);
            if self.tx_state.pending_bytes().saturating_add(est_row_bytes)
                > self.config.max_transaction_bytes
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "transaction exceeds max_transaction_bytes budget",
                ));
            }
        }
        let mut new_row_blob_ptr = None;
        if store_as_blob {
            let blob_kind = if is_capped {
                BlobKind::Lru
            } else {
                BlobKind::Standard
            };
            let ptr = self.append_to_blob_segment(blob_kind, table_id, &row_key, &value)?;
            new_row_blob_ptr = Some(ptr);
            if durability == WriteDurability::SyncTransactional && !self.tx_state.is_active() {
                let sync_res = match blob_kind {
                    BlobKind::Standard => self.sync_active_std_writer(),
                    BlobKind::Lru => self.sync_active_lru_writer(table_id),
                };
                if let Err(e) = sync_res {
                    self.abort_uncommitted_blobs(new_row_blob_ptr.as_slice());
                    return Err(e);
                }
            }
            records.push(WalRecord::PutBlobPtr {
                key: row_key,
                blob_ptr: ptr,
            });
        } else {
            records.push(WalRecord::PutInline {
                key: row_key,
                value,
            });
        }

        // 3. Emit Secondary Index entries (`[TableID | 'i' | IndexID | Escaped(ColVal) | pk]`)
        for sec_key in new_sec_keys {
            records.push(WalRecord::PutInline {
                key: sec_key,
                value: Vec::new(),
            });
        }

        // 4. Emit RowMetaRecord (`'t'`) and LRU Queue entry (`'l'`) if Capped Table or has secondary keys
        let assigned_tick = if is_capped {
            let tick = self.access_tracker.next_tick();
            records.push(WalRecord::PutInline {
                key: encode_lru_queue_key(table_id, tick, pk),
                value: Vec::new(),
            });
            tick
        } else {
            0
        };

        let need_meta = is_capped || !secondary_keys.is_empty();
        let mut new_meta_blob_ptr = None;
        if need_meta {
            let meta_key = encode_row_meta_key(table_id, pk);
            let meta_bytes =
                match RowMetaRecord::encode_slice(assigned_tick, logical_size, secondary_keys) {
                    Ok(bytes) => bytes,
                    Err(e) => {
                        self.abort_uncommitted_blobs(new_row_blob_ptr.as_slice());
                        return Err(e);
                    }
                };
            new_meta_blob_ptr =
                match self.append_std_meta_record(meta_key, meta_bytes, durability, &mut records) {
                    Ok(ptr_opt) => ptr_opt,
                    Err(e) => {
                        self.abort_uncommitted_blobs(new_row_blob_ptr.as_slice());
                        return Err(e);
                    }
                };
        } else if had_old_meta {
            records.push(WalRecord::Delete {
                key: encode_row_meta_key(table_id, pk),
            });
        }

        records.extend(extra_records);

        // 5. Append atomic batch to WAL and apply to MemTable (deferring auto-flush until
        //    dead-blob bytes and quota totals are updated below).
        if let Err(e) =
            self.commit_wal_and_memtable_core(records, durability, true, false, WriteScope::Normal)
        {
            let mut aborted = Vec::new();
            if let Some(p) = new_row_blob_ptr {
                aborted.push(p);
            }
            if let Some(p) = new_meta_blob_ptr {
                aborted.push(p);
            }
            self.abort_uncommitted_blobs(&aborted);
            return Err(e);
        }
        if is_capped {
            let stale_max_tick = assigned_tick.saturating_sub(1);
            if let Some(tx) = self.tx_state.active_tx_mut() {
                tx.pending_lru_clears
                    .push((table_id, pk.to_vec(), stale_max_tick));
            } else {
                self.access_tracker
                    .clear_if_tick_le(table_id, pk, stale_max_tick);
            }
        }
        if need_meta {
            self.table_has_row_meta.insert(table_id, true);
        }

        // 6. Apply dead blob byte accounting and update Capped Table logical byte counter before
        //    any auto-flush so `boot_meta.db` never persists stale quotas or uncounted dead bytes.
        if let Some(old_ptr) = old_blob_ptr {
            self.record_dead_blob_bytes(&old_ptr);
        }
        if let Some(old_m_ptr) = old_meta_blob_ptr {
            self.record_dead_blob_bytes(&old_m_ptr);
        }
        if let Some(ct) = self.capped_tables.get_mut(&table_id) {
            ct.current_logical_bytes = ct
                .current_logical_bytes
                .saturating_sub(old_logical_size)
                .saturating_add(logical_size as u64);
        }
        self.maybe_auto_flush_and_compact();
        if is_capped && !self.tx_state.is_active() {
            // The row is committed; an exceeded quota is re-evaluated by the next write.
            if let Err(e) = self.evict_until_within_quota(table_id, durability) {
                self.record_maintenance_failure(&e);
            }
        }

        Ok(())
    }

    /// Adds secondary index entries to an existing row without rewriting the row.
    ///
    /// Each `(index_id, col_val)` pair of `extra_keys` that the row's `'t'` metadata does not
    /// record yet is written as an `'i'` entry, and the metadata is rewritten to hold the old
    /// and the new pairs, all in one WAL batch. The row value, blob pointer, `recorded_tick`
    /// and logical size are kept, and nothing counts as an overwrite: no dead row bytes, no
    /// quota or LRU change. A row without metadata (an uncapped table that never recorded any)
    /// gets a record with tick 0.
    ///
    /// Returns `Ok(true)` when a batch was written, `Ok(false)` when the row is absent or
    /// already records every pair (a repeated call is a no-op). Unreadable metadata is reported
    /// as `InvalidData` and nothing is written. The caller owns the meaning of the pairs: a
    /// pair with a changed `col_val` for an index the row already records is added next to the
    /// old one, not substituted for it.
    ///
    /// Like [`Self::put_row`], the batch joins the enclosing transaction if one is open and
    /// is rolled back with it.
    pub fn backfill_secondary_keys(
        &mut self,
        table_id: u16,
        pk: &[u8],
        extra_keys: &[(u16, Vec<u8>)],
        durability: WriteDurability,
    ) -> io::Result<bool> {
        self.check_not_poisoned()?;
        self.drain_group_for_autocommit()?;
        self.drain_corrupt_evictions()?;
        if pk.len() > 2048 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "primary key exceeds maximum length (2048 bytes)",
            ));
        }
        let row_key = encode_primary_row_key(table_id, pk);
        let existing = self.check_existing_row(&row_key)?;
        if existing == ExistingRowStatus::NotFound {
            return Ok(false);
        }

        let meta_key = encode_row_meta_key(table_id, pk);
        let lookup = if self.may_have_row_meta(table_id)? {
            self.get_row_meta_lookup(&meta_key)?
        } else {
            MetaLookup::Absent
        };
        let (old_meta, old_meta_blob_ptr) = match lookup {
            MetaLookup::Found(meta, ptr) => (Some(meta), ptr),
            MetaLookup::Absent => (None, None),
            MetaLookup::Corrupt(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "row metadata is unreadable; secondary keys cannot be added",
                ));
            }
        };
        if old_meta.is_none() && self.capped_tables.contains_key(&table_id) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "row of a capped table has no metadata; secondary keys cannot be added",
            ));
        }

        let (recorded_tick, logical_size, mut merged) = match old_meta {
            Some(m) => (m.recorded_tick, m.logical_size, m.secondary_keys),
            None => {
                let value_len = match self.lookup_raw_entry(&row_key)? {
                    Some((_, IndexValue::Inline(v))) => v.len(),
                    Some((_, IndexValue::Blob(p))) => {
                        (p.len as usize).saturating_sub(BLOB_RECORD_HEADER_LEN + row_key.len())
                    }
                    _ => 0,
                };
                (0, (pk.len() + value_len) as u32, Vec::new())
            }
        };

        let mut records = Vec::with_capacity(2 + extra_keys.len());
        let had_keys = merged.len();
        for (idx_id, col_val) in extra_keys {
            if merged.iter().any(|(i, v)| i == idx_id && v == col_val) {
                continue;
            }
            if col_val.len() > u16::MAX as usize {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "secondary index value exceeds 65535 bytes",
                ));
            }
            let sec_key = encode_secondary_index_key(table_id, *idx_id, col_val, pk);
            if !IndexEntry::inline_fits_in_block(sec_key.len(), 0) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "secondary index key exceeds 4 KB block capacity",
                ));
            }
            records.push(WalRecord::PutInline {
                key: sec_key,
                value: Vec::new(),
            });
            merged.push((*idx_id, col_val.clone()));
        }
        if merged.len() == had_keys {
            return Ok(false);
        }

        let meta_bytes = RowMetaRecord::encode_slice(recorded_tick, logical_size, &merged)?;
        let new_meta_blob_ptr =
            self.append_std_meta_record(meta_key, meta_bytes, durability, &mut records)?;
        if let Err(e) =
            self.commit_wal_and_memtable_core(records, durability, true, false, WriteScope::Normal)
        {
            self.abort_uncommitted_blobs(new_meta_blob_ptr.as_slice());
            return Err(e);
        }
        self.table_has_row_meta.insert(table_id, true);
        if let Some(old_m_ptr) = old_meta_blob_ptr {
            self.record_dead_blob_bytes(&old_m_ptr);
        }
        self.maybe_auto_flush_and_compact();
        Ok(true)
    }

    /// Point lookup of a row (`pk`) in `table_id` (owner view: sees this engine's own
    /// transaction overlay). If `table_id` is a Capped Table, updates its `AccessTick`
    /// according to `config.lru_read_tracking`.
    pub fn get_row(&mut self, table_id: u16, pk: &[u8]) -> io::Result<Option<Vec<u8>>> {
        self.owner_read(|eng, cx| eng.get_row_cx(cx, table_id, pk))
    }

    /// Point lookup of a row (`pk`) in `table_id` without recording any LRU read hit or queuing
    /// eager promotions. Used by internal constraint checks inside writes.
    pub(crate) fn get_row_untracked(
        &mut self,
        table_id: u16,
        pk: &[u8],
    ) -> io::Result<Option<Vec<u8>>> {
        self.owner_read_with_tracking(false, |eng, cx| eng.get_row_cx(cx, table_id, pk))
    }

    pub(super) fn get_row_cx(
        &self,
        cx: &ReadCtx<'_, FS::File>,
        table_id: u16,
        pk: &[u8],
    ) -> io::Result<Option<Vec<u8>>> {
        let row_key = encode_primary_row_key(table_id, pk);
        let Some((_lsn, idx_val)) = self.lookup_raw_entry_src(cx.src, &row_key)? else {
            return Ok(None);
        };

        let resolved = match idx_val {
            IndexValue::Inline(bytes) => Some(bytes),
            IndexValue::Blob(ptr) => {
                self.ensure_flushed_cx(cx, &ptr)?;
                match self.read_blob_ref_src(cx.src, &ptr, &row_key)? {
                    Some(v) => Some(v),
                    None => {
                        self.note_corrupt_blob(&row_key, &ptr)?;
                        None
                    }
                }
            }
            IndexValue::Tombstone => None,
        };

        if resolved.is_some() {
            self.track_read_hit(cx, table_id, pk);
        }

        Ok(resolved)
    }

    /// Records an LRU read hit for capped tables: queued for an eager WAL promotion only outside
    /// any active transaction when the reader has exclusive access and `EagerWriteOnRead` is
    /// configured; otherwise coalesced in RAM (interior-mutable tracker, safe under a shared
    /// lock and inside transactions).
    #[inline]
    pub(super) fn track_read_hit(&self, cx: &ReadCtx<'_, FS::File>, table_id: u16, pk: &[u8]) {
        if !cx.track_hits || !self.capped_tables.contains_key(&table_id) {
            return;
        }
        if cx.eager_ok
            && !self.tx_state.is_active()
            && self.config.lru_read_tracking == LruReadTrackingMode::EagerWriteOnRead
            && matches!(cx.src, ReadSource::Owner | ReadSource::Snapshot(_))
        {
            cx.eager_hits.borrow_mut().push((table_id, pk.to_vec()));
        } else {
            self.access_tracker.record_read_hit(table_id, pk);
        }
    }

    /// Whether `ptr` lives in an active writer's not-yet-flushed buffer.
    #[inline]
    pub(super) fn blob_unflushed(&self, ptr: &BlobPtr) -> bool {
        self.storage.blob_unflushed(ptr)
    }

    /// True if any active blob writer holds buffered (unflushed) bytes.
    pub(super) fn any_blob_unflushed(&self) -> bool {
        self.storage.any_blob_unflushed()
    }

    /// Read-path replacement for `ensure_blob_flushed` under `&self`. Committed blobs are always
    /// flushed to the OS handle before their WAL record is appended, so a committed-view read
    /// never finds an unflushed pointer (debug-asserted). The owner view may reference its own
    /// uncommitted blobs: the read is aborted with `cx.need_flush` set and the exclusive caller
    /// flushes and retries.
    #[inline]
    pub(super) fn ensure_flushed_cx(
        &self,
        cx: &ReadCtx<'_, FS::File>,
        ptr: &BlobPtr,
    ) -> io::Result<()> {
        if !self.blob_unflushed(ptr) {
            return Ok(());
        }
        debug_assert!(
            cx.src.is_owner(),
            "committed-view read hit an unflushed blob pointer"
        );
        cx.need_flush.set(true);
        Err(io::Error::other(NEED_FLUSH_MSG))
    }

    /// Owner-view scans that read blob payloads need every buffered byte on the OS handle.
    pub(super) fn ensure_all_flushed_cx(&self, cx: &ReadCtx<'_, FS::File>) -> io::Result<()> {
        if cx.src.is_owner() && self.tx_state.is_active() && self.any_blob_unflushed() {
            cx.need_flush.set(true);
            return Err(io::Error::other(NEED_FLUSH_MSG));
        }
        Ok(())
    }

    /// Resolves the read view of `conn_id`: the owner of the active transaction (or any
    /// connection while no transaction is active) sees the transaction overlay; every other
    /// connection sees committed data only.
    #[inline]
    pub fn read_view_for_conn(&self, conn_id: usize) -> ReadView {
        if self
            .tx_state
            .active_core()
            .is_some_and(|core| core.owner_conn() != Some(conn_id))
        {
            return ReadView::Committed;
        }
        ReadView::Owner
    }

    /// True if reads of `table_id` by `conn_id` need `&mut` access (exclusive lock): the
    /// connection owns the active transaction (its reads may touch its own unflushed blobs), or
    /// `EagerWriteOnRead` tracking applies to a capped table. Otherwise
    /// [`Self::reader_for_conn`] under a shared lock is sufficient.
    pub fn read_needs_exclusive(&self, conn_id: usize, table_id: u16) -> bool {
        if self.read_view_for_conn(conn_id) == ReadView::Committed {
            return false;
        }
        self.tx_state.is_active()
            || (self.config.lru_read_tracking == LruReadTrackingMode::EagerWriteOnRead
                && self.capped_tables.contains_key(&table_id))
    }

    /// True if a shared-lock read by `conn_id` on `table_id` must upgrade to an exclusive lock
    /// before attempting [`Self::reader_for_conn`]: either `EagerWriteOnRead` tracking applies to
    /// a capped table outside any transaction, or `conn_id` owns an active transaction that
    /// currently has buffered (unflushed) blob bytes.
    #[inline]
    pub fn vtab_read_needs_exclusive(&self, conn_id: usize, table_id: u16) -> bool {
        if self.read_view_for_conn(conn_id) == ReadView::Committed {
            return false;
        }
        let active = self.tx_state.is_active();
        (active && self.any_blob_unflushed())
            || (!active
                && !self.is_poisoned()
                && self.config.lru_read_tracking == LruReadTrackingMode::EagerWriteOnRead
                && self.capped_tables.contains_key(&table_id))
    }

    /// True if a snapshot read on `table_id` should upgrade to an exclusive lock so
    /// `EagerWriteOnRead` can persist LRU promotions outside active transactions.
    #[inline]
    pub(crate) fn snapshot_read_needs_exclusive(&self, table_id: u16) -> bool {
        !self.tx_state.is_active()
            && !self.is_poisoned()
            && self.config.lru_read_tracking == LruReadTrackingMode::EagerWriteOnRead
            && self.capped_tables.contains_key(&table_id)
    }

    /// Shared-access reader for `conn_id` (see [`EngineReader`]). Must only be used when
    /// [`Self::read_needs_exclusive`] is `false`; otherwise reads may fail with an
    /// "unflushed blob" error and `EagerWriteOnRead` is downgraded to coalesced tracking.
    pub fn reader_for_conn(&self, conn_id: usize) -> EngineReader<'_, FS> {
        let src = ReadSource::from_view(self.read_view_for_conn(conn_id));
        EngineReader {
            eng: self,
            cx: CxRef::Owned(ReadCtx::new(src, false)),
        }
    }

    /// Runs `f` with exclusive access in `src`: flushes buffered blobs and retries when the
    /// reader asks for it, then applies queued eager LRU promotions on a best-effort basis. The
    /// second tuple element carries a flush error (the read result itself is returned untouched).
    fn run_read_src<R>(
        &mut self,
        src: ReadSource<'_, FS::File>,
        f: impl for<'a> FnMut(&'a Self, &'a ReadCtx<'a, FS::File>) -> R,
    ) -> (R, Option<io::Error>) {
        self.run_read_with_tracking_src(src, true, f)
    }

    fn run_read_with_tracking_src<R>(
        &mut self,
        src: ReadSource<'_, FS::File>,
        track_hits: bool,
        mut f: impl for<'a> FnMut(&'a Self, &'a ReadCtx<'a, FS::File>) -> R,
    ) -> (R, Option<io::Error>) {
        let mut attempts = 0;
        loop {
            let cx = ReadCtx::new_with_tracking(src, track_hits, track_hits);
            let r = f(&*self, &cx);
            if cx.need_flush.get() && attempts < 2 {
                attempts += 1;
                if let Err(e) = self
                    .storage
                    .flush_buffered_blobs_for_read(&mut self.std_blobs, &mut self.lru_blobs)
                {
                    return (r, Some(e));
                }
                continue;
            }
            // The promotions are an optimisation of the read, never part of its result: a
            // poisoned engine cannot write them, and a write that fails must not fail the read,
            // though a failure on a non-poisoned engine is recorded via `record_maintenance_failure`.
            // Inside an active transaction (`tx_state.is_active()`) eager promotions must never mutate the
            // transaction overlay/budget. A hit that is not written is still noted in RAM, so the
            // LRU order learns it lazily as for any coalesced read.
            let hits = cx.eager_hits.take();
            let mut write_failed = self.is_poisoned() || self.tx_state.is_active();
            for (table_id, pk) in hits {
                if !write_failed {
                    if let Err(e) = self.eager_promote_lru_on_read(table_id, &pk) {
                        self.record_maintenance_failure(&e);
                        write_failed = true;
                    }
                }
                if write_failed && track_hits {
                    self.access_tracker.record_read_hit(table_id, &pk);
                }
            }
            return (r, None);
        }
    }

    pub(super) fn owner_read<R>(
        &mut self,
        f: impl for<'a> FnMut(&'a Self, &'a ReadCtx<'a, FS::File>) -> io::Result<R>,
    ) -> io::Result<R> {
        self.owner_read_with_tracking(true, f)
    }

    pub(super) fn combine_read_result<R>(res: (io::Result<R>, Option<io::Error>)) -> io::Result<R> {
        match res {
            (Ok(v), None) => Ok(v),
            (Ok(_), Some(e)) => Err(e),
            (Err(e), Some(flush_err)) if e.to_string() == NEED_FLUSH_MSG => Err(flush_err),
            (Err(e), _) => Err(e),
        }
    }

    pub(super) fn owner_read_with_tracking<R>(
        &mut self,
        track_hits: bool,
        f: impl for<'a> FnMut(&'a Self, &'a ReadCtx<'a, FS::File>) -> io::Result<R>,
    ) -> io::Result<R> {
        Self::combine_read_result(self.run_read_with_tracking_src(ReadSource::Owner, track_hits, f))
    }

    /// Deletes a row (`pk`) in `table_id`, cascading tombstones to its secondary indexes and LRU queue.
    pub fn delete_row(
        &mut self,
        table_id: u16,
        pk: &[u8],
        durability: WriteDurability,
    ) -> io::Result<bool> {
        self.delete_row_with_rowid_hint(table_id, pk, durability, true)
    }

    /// Deletes a row (`pk`) in `table_id`, checking persistent synthetic rowid mappings only when
    /// `check_synthetic_rowid` is `true` (callers on `INTEGER PRIMARY KEY` tables with an exact
    /// integer `pk` know no synthetic rowid entry can exist for `pk`).
    pub(crate) fn delete_row_with_rowid_hint(
        &mut self,
        table_id: u16,
        pk: &[u8],
        durability: WriteDurability,
        check_synthetic_rowid: bool,
    ) -> io::Result<bool> {
        self.check_not_poisoned()?;
        self.drain_group_for_autocommit()?;
        self.drain_corrupt_evictions()?;
        if pk.len() > 2048 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "primary key exceeds maximum length (2048 bytes)",
            ));
        }
        let row_key = encode_primary_row_key(table_id, pk);
        let existing = self.check_existing_row(&row_key)?;
        if existing == ExistingRowStatus::NotFound {
            return Ok(false);
        }
        let old_blob_ptr = match existing {
            ExistingRowStatus::Blob(old_ptr) => Some(old_ptr),
            _ => None,
        };

        let mut records = vec![WalRecord::Delete {
            key: row_key.clone(),
        }];
        if check_synthetic_rowid {
            self.append_rowid_deletes(table_id, pk, &mut records)?;
        }
        let mut old_logical_size = 0u64;
        let mut old_meta_blob_ptr = None;

        if self.may_have_row_meta(table_id)? {
            let old_meta = self.collect_old_meta_deletes(
                table_id,
                pk,
                &row_key,
                OldMetaDeleteOpts {
                    require_capped_for_lru: false,
                    delete_meta_key: true,
                    match_lru_key: None,
                },
                &mut records,
            )?;
            old_meta_blob_ptr = old_meta.old_meta_blob_ptr;
            old_logical_size = old_meta.old_logical_size;
        }

        let is_capped = self.capped_tables.contains_key(&table_id);
        self.commit_wal_and_memtable_core(records, durability, true, false, WriteScope::Normal)?;
        if is_capped {
            if let Some(tx) = self.tx_state.active_tx_mut() {
                tx.pending_lru_clears
                    .push((table_id, pk.to_vec(), u64::MAX));
            } else {
                self.access_tracker.remove_key(table_id, pk);
            }
        }

        if let Some(old_ptr) = old_blob_ptr {
            self.record_dead_blob_bytes(&old_ptr);
        }
        if let Some(old_m_ptr) = old_meta_blob_ptr {
            self.record_dead_blob_bytes(&old_m_ptr);
        }
        if let Some(ct) = self.capped_tables.get_mut(&table_id) {
            ct.current_logical_bytes = ct.current_logical_bytes.saturating_sub(old_logical_size);
        }
        self.maybe_auto_flush_and_compact();
        Ok(true)
    }

    /// Point lookup of a row (`pk`) in `table_id`, appending its payload directly into `slab`
    /// and recording its `(start, end)` offset in `spans` with zero intermediate `Vec<u8>` allocations!
    pub fn get_row_into(
        &mut self,
        table_id: u16,
        pk: &[u8],
        slab: &mut Vec<u8>,
        spans: &mut Vec<(usize, usize)>,
    ) -> io::Result<bool> {
        self.owner_read(|eng, cx| eng.get_row_into_cx(cx, table_id, pk, slab, spans))
    }

    pub(super) fn get_row_into_cx(
        &self,
        cx: &ReadCtx<'_, FS::File>,
        table_id: u16,
        pk: &[u8],
        slab: &mut Vec<u8>,
        spans: &mut Vec<(usize, usize)>,
    ) -> io::Result<bool> {
        let mut stack_buf = [0u8; 64];
        let heap_buf;
        let row_key: &[u8] = if 3 + pk.len() <= stack_buf.len() {
            let tb = table_id.to_be_bytes();
            stack_buf[0] = tb[0];
            stack_buf[1] = tb[1];
            stack_buf[2] = NS_PRIMARY_ROW;
            stack_buf[3..3 + pk.len()].copy_from_slice(pk);
            &stack_buf[..3 + pk.len()]
        } else {
            heap_buf = encode_primary_row_key(table_id, pk);
            &heap_buf
        };

        let mut hit = false;
        if let Some(mem_entry) = self.mem_get_src(cx.src, row_key) {
            match &mem_entry.value {
                IndexValue::Inline(bytes) => {
                    let start = slab.len();
                    slab.extend_from_slice(bytes);
                    spans.push((start, slab.len()));
                    hit = true;
                }
                IndexValue::Blob(ptr) => {
                    let ptr_copy = *ptr;
                    let start = slab.len();
                    if self.dereference_blob_append_into_cx(cx, &ptr_copy, row_key, slab)? {
                        spans.push((start, slab.len()));
                        hit = true;
                    }
                }
                IndexValue::Tombstone => {}
            }
        } else {
            let hash = BloomFilter::hash_pair(row_key);
            for seg in self.idx_segments_for(cx.src).iter().rev() {
                let start = slab.len();
                let probe =
                    seg.visit_hashed(row_key, hash, |raw| probe_raw_into_slab(raw, slab))?;
                if let Some(probe) = probe {
                    match probe {
                        SegProbe::Inline => {
                            spans.push((start, slab.len()));
                            hit = true;
                        }
                        SegProbe::Blob(ptr) => {
                            if self.dereference_blob_append_into_cx(cx, &ptr, row_key, slab)? {
                                spans.push((start, slab.len()));
                                hit = true;
                            }
                        }
                        SegProbe::Tombstone => {}
                    }
                    break;
                }
            }
        }

        if hit {
            self.track_read_hit(cx, table_id, pk);
        }

        Ok(hit)
    }

    /// Point lookup of a row (`pk`) in `table_id` with lazy blob dereferencing:
    /// if the row value is inline, appends it to `slab`, records `(start, end)` in `spans`, and pushes `None` to `lazy_blobs`.
    /// if the row value is an external blob, records placeholder `(0, 0)` in `spans` and pushes `Some(ptr)` to `lazy_blobs`.
    pub fn get_row_into_lazy(
        &mut self,
        table_id: u16,
        pk: &[u8],
        slab: &mut Vec<u8>,
        spans: &mut Vec<(usize, usize)>,
        lazy_blobs: &mut Vec<Option<LazyBlob<FS::File>>>,
    ) -> io::Result<bool> {
        self.owner_read(|eng, cx| {
            eng.get_row_into_lazy_cx(cx, table_id, pk, slab, spans, lazy_blobs)
        })
    }

    pub(super) fn get_row_into_lazy_cx(
        &self,
        cx: &ReadCtx<'_, FS::File>,
        table_id: u16,
        pk: &[u8],
        slab: &mut Vec<u8>,
        spans: &mut Vec<(usize, usize)>,
        lazy_blobs: &mut Vec<Option<LazyBlob<FS::File>>>,
    ) -> io::Result<bool> {
        let mut stack_buf = [0u8; 64];
        let heap_buf;
        let row_key: &[u8] = if 3 + pk.len() <= stack_buf.len() {
            let tb = table_id.to_be_bytes();
            stack_buf[0] = tb[0];
            stack_buf[1] = tb[1];
            stack_buf[2] = NS_PRIMARY_ROW;
            stack_buf[3..3 + pk.len()].copy_from_slice(pk);
            &stack_buf[..3 + pk.len()]
        } else {
            heap_buf = encode_primary_row_key(table_id, pk);
            &heap_buf
        };

        // Evict-policy tables verify blobs eagerly (a row cannot be skipped after emission).
        let lazy_ok = self.corrupt_policy(table_id) == CorruptPolicy::Error;
        if !lazy_ok {
            self.ensure_all_flushed_cx(cx)?;
        }
        let mut hit = false;
        if let Some(mem_entry) = self.mem_get_src(cx.src, row_key) {
            match &mem_entry.value {
                IndexValue::Inline(bytes) => {
                    let start = slab.len();
                    slab.extend_from_slice(bytes);
                    spans.push((start, slab.len()));
                    lazy_blobs.push(None);
                    hit = true;
                }
                IndexValue::Blob(ptr) => {
                    if lazy_ok {
                        spans.push((0, 0));
                        lazy_blobs.push(Some(self.pin_lazy_blob_src(cx.src, ptr)));
                        hit = true;
                    } else {
                        let start = slab.len();
                        if self.read_flushed_blob_append_into_src(cx.src, ptr, row_key, slab)? {
                            spans.push((start, slab.len()));
                            lazy_blobs.push(None);
                            hit = true;
                        }
                    }
                }
                IndexValue::Tombstone => {}
            }
        } else {
            let hash = BloomFilter::hash_pair(row_key);
            for seg in self.idx_segments_for(cx.src).iter().rev() {
                let start = slab.len();
                let probe =
                    seg.visit_hashed(row_key, hash, |raw| probe_raw_into_slab(raw, slab))?;
                if let Some(probe) = probe {
                    match probe {
                        SegProbe::Inline => {
                            spans.push((start, slab.len()));
                            lazy_blobs.push(None);
                            hit = true;
                        }
                        SegProbe::Blob(ptr) => {
                            if lazy_ok {
                                spans.push((0, 0));
                                lazy_blobs.push(Some(self.pin_lazy_blob_src(cx.src, &ptr)));
                                hit = true;
                            } else if self
                                .read_flushed_blob_append_into_src(cx.src, &ptr, row_key, slab)?
                            {
                                spans.push((start, slab.len()));
                                lazy_blobs.push(None);
                                hit = true;
                            }
                        }
                        SegProbe::Tombstone => {}
                    }
                    break;
                }
            }
        }

        if hit && matches!(lazy_blobs.last(), Some(None)) {
            self.track_read_hit(cx, table_id, pk);
        }

        Ok(hit)
    }

    /// Reference to the engine's in-process verified block cache.
    pub fn block_cache(&self) -> &Arc<VerifiedBlockCache> {
        &self.block_cache
    }

    /// Reference to the engine's in-process small blob cache.
    pub fn blob_cache(&self) -> &Arc<VerifiedBlobCache> {
        &self.blob_cache
    }

    /// What recovery found in the WAL during the `open` that created this engine. It is defined
    /// even when nothing happened (no damage, nothing replayed). The WAL is damaged only when
    /// acknowledged commits were lost; the unsynced tail of a power cut is not damage.
    pub fn last_wal_recovery(&self) -> &WalRecoveryReport {
        &self.last_wal_recovery
    }

    /// Outside a transaction, makes the group-committed batches that await their flush durable
    /// and publishes them to the `MemTable`, so that the caller reads the newest version of every
    /// row it maintains secondary entries, row metadata or rowid mappings for. Writers read the
    /// committed state, which does not hold a pending group commit yet, while a transaction
    /// starts from the pending entries (`seed_overlay_from_pending`).
    ///
    /// This costs nothing when no group commit is pending. Otherwise it runs the device flush that
    /// the write's own commit would run first anyway (`commit_wal_and_memtable` drains too), so
    /// the write waits for the group's flush exactly as it did, only before its reads.
    pub(crate) fn drain_group_for_autocommit(&mut self) -> io::Result<()> {
        if !self.tx_state.is_active() {
            self.drain_pending_durable()?;
        }
        Ok(())
    }

    // =========================================================================
    // Internal Helper Methods
    // =========================================================================

    pub(super) fn maybe_auto_flush_and_compact(&mut self) {
        if self.tx_state.is_active() || self.is_poisoned() {
            return;
        }
        // The batch is committed; a failed flush is retried because `should_flush` stays true.
        if self.memtable.should_flush() {
            if let Err(e) = self.flush_memtable() {
                self.record_maintenance_failure(&e);
            }
        }
        // Safe point: the batch is appended and `next_lsn` has advanced.
        self.run_due_auto_compaction();
    }

    pub(super) fn commit_wal_and_memtable(
        &mut self,
        records: Vec<WalRecord>,
        durability: WriteDurability,
    ) -> io::Result<u64> {
        self.commit_wal_and_memtable_scoped(records, durability, WriteScope::Normal)
    }

    pub(in crate::engine) fn commit_wal_and_memtable_scoped(
        &mut self,
        records: Vec<WalRecord>,
        durability: WriteDurability,
        scope: WriteScope,
    ) -> io::Result<u64> {
        self.commit_wal_and_memtable_core(records, durability, true, true, scope)
    }

    pub(in crate::engine) fn commit_wal_and_memtable_core(
        &mut self,
        records: Vec<WalRecord>,
        durability: WriteDurability,
        count_as_data_write: bool,
        allow_auto_flush: bool,
        scope: WriteScope,
    ) -> io::Result<u64> {
        if count_as_data_write || self.tx_state.is_active() {
            self.mutation_epoch = self.mutation_epoch.wrapping_add(1);
        }
        self.check_not_poisoned()?;
        let lsn = self.next_lsn;
        let successor = self.lsn_after_batches(1)?;

        if self.tx_state.is_active() {
            let batch_bytes: usize = records.iter().map(WalRecord::est_tx_budget_bytes).sum();
            let max_transaction_bytes = self.config.max_transaction_bytes;
            let poisoned = self.is_poisoned();
            let reset_buf = match self.tx_state.can_record_write(
                poisoned,
                scope,
                records.len(),
                batch_bytes,
                max_transaction_bytes,
            ) {
                Ok(reset) => reset,
                Err(err) => {
                    let io_err = self.tx_error_to_io(err);
                    let _ = self.apply_tx_event(TxEvent::RecordWrite {
                        scope,
                        durability,
                        record_count: records.len(),
                        batch_bytes,
                        max_transaction_bytes,
                    });
                    return Err(io_err);
                }
            };
            {
                let Some(tx) = self.tx_state.active_tx_mut() else {
                    unreachable!("checked self.tx_state.is_active()");
                };
                if reset_buf {
                    tx.pending_buf.clear();
                    tx.pending_buf
                        .extend_from_slice(&[0u8; crate::wal::WAL_BATCH_HEADER_SIZE]);
                }
                let prev_buf_len = tx.pending_buf.len();
                if let Err(e) = WalBatch::try_append_records_to_buf(&records, &mut tx.pending_buf) {
                    tx.pending_buf.truncate(prev_buf_len);
                    return Err(e);
                }
            }
            let (effects, _) = self
                .apply_tx_event(TxEvent::RecordWrite {
                    scope,
                    durability,
                    record_count: records.len(),
                    batch_bytes,
                    max_transaction_bytes,
                })
                .map_err(|e| self.tx_error_to_io(e))?;
            let record_undo = effects.iter().any(|e| match e {
                tx_fsm::TxEffect::AppendTxBatch { record_undo, .. } => record_undo,
                _ => false,
            });
            self.next_lsn = successor;
            self.note_records_table_mutations(&records);
            if let Some(tx) = self.tx_state.active_tx_mut() {
                for rec in records {
                    tx.overlay.apply(lsn, rec, record_undo);
                }
            }
            return Ok(lsn);
        }

        let has_records = !records.is_empty();
        let batch = match (|| -> io::Result<WalBatch> {
            self.drain_pending_durable()?;
            let mut batch = WalBatch::new(lsn, records);
            self.flush_active_blob_buffers()?;
            (batch.prev_lsn, batch.durable_lsn) = self.wal_links_for_append(durability)?;
            let mut raw = core::mem::take(&mut self.scratch_wal_encoded);
            let res = match batch.try_encode_into(&mut raw) {
                Ok(()) => self.append_raw_wal_batch_with_rotation(&raw, durability),
                Err(e) => Err(e),
            };
            raw.clear();
            self.scratch_wal_encoded = raw;
            res?;
            Ok(batch)
        })() {
            Ok(batch) => {
                self.in_flight_autocommit_blob_starts.clear();
                batch
            }
            Err(e) => {
                self.in_flight_autocommit_blob_starts.clear();
                return Err(e);
            }
        };
        self.next_lsn = self.next_lsn.max(successor);
        if count_as_data_write && has_records {
            self.note_records_table_mutations(&batch.records);
        }
        self.invalidate_snapshot_memtable();
        self.memtable.apply_wal_batch_owned(batch);
        if count_as_data_write && has_records {
            self.data_generation += 1;
        }
        if allow_auto_flush {
            self.maybe_auto_flush_and_compact();
        }
        Ok(lsn)
    }

    /// Makes everything appended to `wal_buf.db` so far durable and advances `last_buf_synced_lsn`.
    ///
    /// Write-Blob-Before-WAL: the active blob writers are flushed and synced first, because the
    /// buffered batches may reference out-of-line values in them. This is the only place that
    /// advances `last_buf_synced_lsn`, so no caller can report the buffered stream durable
    /// without its blobs. Then the log is flushed, its open sector sealed (so it is never
    /// rewritten in place after being synced) and synced. A blob sync failure or a WAL sync
    /// failure leaves the durability outcome unknown and poisons the engine; a failed buffer
    /// write stays retryable.
    pub(super) fn make_buf_wal_durable(&mut self, durable: bool) -> io::Result<()> {
        self.check_not_poisoned()?;
        self.sync_active_blob_writers()?;
        self.healthy()?.make_buf_wal_durable_and_seal(durable)?;
        self.last_buf_synced_lsn = self.last_buf_append_lsn;
        self.wal_links.buf_synced();
        Ok(())
    }

    /// Returns the `(prev_lsn, durable_lsn)` header fields for the next batch of `durability`.
    ///
    /// Prefix durability comes first: before a `SyncTransactional` batch at LSN `L` is appended,
    /// every earlier `BufferedFast` batch AND its out-of-line blobs are synced. That sync must
    /// precede the stamp, so that the watermark of the synchronous batch covers them. Callers
    /// stamp the header after this call and append the batch right away under the same lock.
    pub(super) fn wal_links_for_append(
        &mut self,
        durability: WriteDurability,
    ) -> io::Result<(u64, u64)> {
        if durability == WriteDurability::SyncTransactional
            && self.last_buf_append_lsn > self.last_buf_synced_lsn
        {
            self.make_buf_wal_durable(true)?;
        }
        Ok(self.wal_links.stamp())
    }

    pub(super) fn append_raw_wal_batch_with_rotation(
        &mut self,
        encoded: &[u8],
        durability: WriteDurability,
    ) -> io::Result<()> {
        self.check_not_poisoned()?;
        #[cfg(test)]
        if core::mem::replace(&mut self.debug_fail_next_wal_append, false) {
            return Err(self.poison(io::Error::other("injected WAL append failure")));
        }
        let batch_lsn = if encoded.len() >= 8 {
            u64::from_le_bytes(encoded[0..8].try_into().unwrap())
        } else {
            self.next_lsn
        };

        // Prefix durability across `wal_buf.db`, active blob segments, and `wal_sync.db` was
        // enforced by `wal_links_for_append`, which stamped this batch's header.
        // A failed append/sync leaves the on-disk outcome of this batch unknown (it may or may not
        // be replayed after restart), so the engine is poisoned and must be reopened.
        let outcome = self.healthy()?.append_wal_raw_batch(durability, encoded)?;

        if outcome == WalAppendOutcome::RingFull {
            // The batch already holds `batch_lsn` but is not in the log yet, and the checkpoint
            // LSN is derived from `next_lsn - 1`. The rotation must therefore not allocate LSNs
            // or write anything else: a delete (a drained corrupt-row eviction) or a blob
            // relocation (auto compaction) would take `batch_lsn`, the checkpoint would then
            // cover the batch, and recovery would discard it as already checkpointed. Hence no
            // `flush_memtable` here: queued evictions stay queued for the next drain point, and
            // auto compaction is only marked due (`auto_compaction_due`) and runs at the next
            // safe point, once this batch is appended and `next_lsn` has advanced.
            self.drain_pending_durable()?;
            if self.memtable.is_empty() {
                self.rotate_wal_epochs_only()?;
            } else {
                self.flush_memtable_no_auto_compact()?;
                self.auto_compaction_due = true;
            }
            debug_assert!(
                self.next_lsn <= batch_lsn && self.meta_mgr().state().checkpoint_lsn < batch_lsn,
                "WAL rotation must not consume the LSN of the batch being appended"
            );
            let retry = self.healthy()?.append_wal_raw_batch(durability, encoded)?;
            if retry == WalAppendOutcome::RingFull {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "single WAL batch exceeds entire pre-allocated WAL file capacity",
                ));
            }
        }

        // Only now that the append succeeded does the batch become the predecessor of the next.
        // A synchronous append that ran its own barrier also made every earlier write to
        // `wal_sync.db` durable, including group-committed batches that await their flush.
        let prev_lsn = match encoded.get(16..24) {
            Some(b) => u64::from_le_bytes(b.try_into().unwrap()),
            None => self.wal_links.stamp().0,
        };
        match durability {
            WriteDurability::BufferedFast => {
                self.last_buf_append_lsn = self.last_buf_append_lsn.max(batch_lsn);
                self.wal_links
                    .appended(LogStream::Buffered, batch_lsn, prev_lsn, false);
            }
            WriteDurability::SyncTransactional => {
                let synced = !self.sync_wal().barrier_deferred();
                self.wal_links
                    .appended(LogStream::Sync, batch_lsn, prev_lsn, synced);
            }
        }
        Ok(())
    }

    pub(super) fn check_existing_row(&self, key: &[u8]) -> io::Result<ExistingRowStatus> {
        self.check_existing_row_src(ReadSource::Owner, key)
    }

    fn check_existing_row_src(
        &self,
        src: ReadSource<'_, FS::File>,
        key: &[u8],
    ) -> io::Result<ExistingRowStatus> {
        if let Some(mem_entry) = self.mem_get_src(src, key) {
            return Ok(match &mem_entry.value {
                IndexValue::Tombstone => ExistingRowStatus::NotFound,
                IndexValue::Inline(_) => ExistingRowStatus::Inline,
                IndexValue::Blob(ptr) => ExistingRowStatus::Blob(*ptr),
            });
        }
        let hash = BloomFilter::hash_pair(key);
        for seg in self.idx_segments_for(src).iter().rev() {
            let status = seg.visit_hashed(key, hash, |raw| match raw.val_type {
                1 => Ok(ExistingRowStatus::Inline),
                2 => Ok(ExistingRowStatus::Blob(RawIdxEntry::decode_blob_ptr(
                    raw.val_bytes,
                )?)),
                3 => Ok(ExistingRowStatus::NotFound),
                other => Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unknown IndexValue type_tag: {other}"),
                )),
            })?;
            if let Some(status) = status {
                return Ok(status);
            }
        }
        Ok(ExistingRowStatus::NotFound)
    }

    /// Lazily determines whether `table_id` may contain any `'t'` (`NS_ROW_META`) records.
    ///
    /// On standard uncapped tables without secondary indexes, the first `UPDATE`/`DELETE` after
    /// `StorageEngine::open` runs a single 1-item prefix probe and caches `false` in
    /// `self.table_has_row_meta`, skipping all negative `'t'` lookups on subsequent mutations!
    pub(super) fn may_have_row_meta(&mut self, table_id: u16) -> io::Result<bool> {
        if self.capped_tables.contains_key(&table_id) {
            return Ok(true);
        }
        if let Some(&has) = self.table_has_row_meta.get(&table_id) {
            return Ok(has);
        }
        let prefix = encode_namespace_prefix(table_id, NS_ROW_META);
        let has = !self
            .scan_merged_prefix_range(&prefix, &prefix, true, None, 1, false)?
            .is_empty();
        self.table_has_row_meta.insert(table_id, has);
        Ok(has)
    }

    /// Policy-free row-meta lookup used by delete/overwrite/eviction paths. An unreadable
    /// `'t'` record (spilled blob failing verification, dangling pointer, or undecodable bytes)
    /// is reported as [`MetaLookup::Corrupt`] instead of an error so callers can clean up.
    pub(super) fn get_row_meta_lookup(&mut self, meta_key: &[u8]) -> io::Result<MetaLookup> {
        let Some((_lsn, val)) = self.lookup_raw_entry(meta_key)? else {
            return Ok(MetaLookup::Absent);
        };
        match val {
            IndexValue::Inline(bytes) => Ok(match RowMetaRecord::from_bytes(&bytes) {
                Ok(r) => MetaLookup::Found(r, None),
                Err(e) if e.kind() != io::ErrorKind::InvalidData => return Err(e),
                Err(_) => {
                    self.count_corrupt_detection();
                    MetaLookup::Corrupt(None)
                }
            }),
            IndexValue::Blob(ptr) => match self.dereference_blob_lenient(&ptr, meta_key)? {
                Some(bytes) => Ok(match RowMetaRecord::from_bytes(&bytes) {
                    Ok(r) => MetaLookup::Found(r, Some(ptr)),
                    Err(e) if e.kind() != io::ErrorKind::InvalidData => return Err(e),
                    Err(_) => {
                        self.count_corrupt_detection();
                        MetaLookup::Corrupt(Some(ptr))
                    }
                }),
                None => Ok(MetaLookup::Corrupt(Some(ptr))),
            },
            IndexValue::Tombstone => Ok(MetaLookup::Absent),
        }
    }

    /// Convenience wrapper: `None` for absent or corrupt metadata.
    pub(super) fn get_row_meta_with_ptr(
        &mut self,
        meta_key: &[u8],
    ) -> io::Result<Option<(RowMetaRecord, Option<BlobPtr>)>> {
        Ok(match self.get_row_meta_lookup(meta_key)? {
            MetaLookup::Found(r, p) => Some((r, p)),
            _ => None,
        })
    }

    /// Fallback cleanup for a row whose `'t'` metadata is unreadable: returns the keys of every
    /// secondary-index (`'i'`) and LRU-queue (`'l'`) entry of `table_id` whose trailing primary
    /// key equals `pk`, found by scanning both namespaces, plus the best-effort logical size
    /// estimate of the row (`pk.len()` + value length, derived from the row record itself).
    pub(super) fn collect_orphans_for_pk(
        &self,
        table_id: u16,
        pk: &[u8],
        row_key: &[u8],
    ) -> io::Result<(Vec<Vec<u8>>, u64)> {
        let mut keys = Vec::new();
        let sec_prefix = encode_namespace_prefix(table_id, NS_SECONDARY_INDEX);
        for e in self.scan_merged_prefix(&sec_prefix)? {
            if matches!(e.value, IndexValue::Tombstone) || e.key.len() < 5 {
                continue;
            }
            let index_id = u16::from_be_bytes([e.key[3], e.key[4]]);
            if decode_secondary_index_pk_slice(table_id, index_id, &e.key) == Some(pk) {
                keys.push(e.key);
            }
        }
        let lru_prefix = encode_namespace_prefix(table_id, NS_LRU_QUEUE);
        for e in self.scan_merged_prefix(&lru_prefix)? {
            if matches!(e.value, IndexValue::Tombstone) {
                continue;
            }
            if decode_lru_queue_key(table_id, &e.key).is_some_and(|(_, p)| p == pk) {
                keys.push(e.key);
            }
        }
        let value_len = match self.lookup_raw_entry(row_key)? {
            Some((_, IndexValue::Inline(v))) => v.len(),
            Some((_, IndexValue::Blob(p))) => {
                (p.len as usize).saturating_sub(BLOB_RECORD_HEADER_LEN + row_key.len())
            }
            _ => 0,
        };
        Ok((keys, (pk.len() + value_len) as u64))
    }

    /// Newest in-memory entry for `key` as seen by `src`: the transaction overlay, followed
    /// inside an active transaction by unflushed `pending_group` batches (hidden from autocommit
    /// and non-owner readers), over the committed `MemTable`.
    #[inline]
    fn mem_get_src<'a>(
        &'a self,
        src: ReadSource<'a, FS::File>,
        key: &[u8],
    ) -> Option<&'a crate::memtable::MemEntry> {
        if src.is_owner() {
            if let Some(tx) = self.tx_state.active_tx() {
                if tx.overlay.may_contain_key(key) {
                    if let Some(e) = tx.overlay.map.get(key) {
                        return Some(e);
                    }
                }
                if !self.pending_group.is_empty() && !self.is_poisoned() {
                    for (_, entries) in self.pending_group.iter().rev() {
                        if let Some(e) = entries.get(key) {
                            return Some(e);
                        }
                    }
                }
            }
        }
        self.memtable_for(src).get(key)
    }

    pub(super) fn lookup_raw_entry(&self, key: &[u8]) -> io::Result<Option<(u64, IndexValue)>> {
        self.lookup_raw_entry_src(ReadSource::Owner, key)
    }

    pub(super) fn lookup_raw_entry_view(
        &self,
        view: ReadView,
        key: &[u8],
    ) -> io::Result<Option<(u64, IndexValue)>> {
        self.lookup_raw_entry_src(ReadSource::from_view(view), key)
    }

    fn lookup_raw_entry_src(
        &self,
        src: ReadSource<'_, FS::File>,
        key: &[u8],
    ) -> io::Result<Option<(u64, IndexValue)>> {
        if let Some(mem_entry) = self.mem_get_src(src, key) {
            return Ok(Some((mem_entry.lsn, mem_entry.value.clone())));
        }
        let hash = BloomFilter::hash_pair(key);
        for seg in self.idx_segments_for(src).iter().rev() {
            if let Some((lsn, val)) = seg.get_hashed(key, hash)? {
                return Ok(Some((lsn, val)));
            }
        }
        Ok(None)
    }
}
