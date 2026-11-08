//! Layer 4 (Part 2): Unified Phase 1 Storage Engine Coordinator (`StorageEngine`).
//!
//! Integrates `BootMetaManager` (`boot_meta.db`), two-stream Epoch WAL (`wal_sync.db` + `wal_buf.db`),
//! out-of-line blob segments (`blob_std` + per-table `blob_lru`), bounded `MemTable`, 4 KB block
//! `idx_<SeqID>.db` segments with Bloom filters and sparse fence pointers, multi-way merge
//! compaction, and Capped Table (`MAX_BYTES`) delayed LRU eviction.

mod compaction;
mod eviction;
mod scan;
mod tx;

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, HashMap};
use std::io;
use std::sync::Arc;

use crate::blob::{
    BlobSegmentReader, BlobSegmentWriter, DEFAULT_INLINE_THRESHOLD, DEFAULT_MAX_BLOB_SEGMENT_SIZE,
};
use crate::index::{DEFAULT_BLOOM_BITS_PER_KEY, IdxSegmentReader, IndexEntry, IndexValue};
use crate::lru::{
    AccessTracker, CappedTableState, NS_LRU_QUEUE, NS_PRIMARY_ROW, NS_ROW_META,
    NS_SECONDARY_INDEX, RowMetaRecord, encode_lru_queue_key, encode_namespace_prefix,
    encode_primary_row_key, encode_row_meta_key, encode_secondary_index_key,
};
use crate::memtable::{DEFAULT_MEMTABLE_CAPACITY_BYTES, MemTable};
use crate::meta::{
    BootMetaManager, EvictionPolicy, LruBlobSegmentMeta, StdBlobSegmentMeta,
    format_lru_blob_file_name, format_std_blob_file_name,
};
use crate::pal::{OpenHint, PlatformFile, PlatformFs};
use crate::wal::{
    BlobKind, BlobPtr, DEFAULT_BUF_WAL_SIZE, DEFAULT_SYNC_WAL_SIZE, WAL_BUF_FILE_NAME,
    WAL_SYNC_FILE_NAME, WalAppendOutcome, WalBatch, WalRecord, WalStreamMode, WalStreamWriter,
    recover_two_streams,
};

use self::tx::{SavepointMarker, TxMetaSnapshot};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ExistingRowStatus {
    NotFound,
    Inline,
    Blob(BlobPtr),
}

/// Durability guarantee requested for a write or delete operation.
///
/// If a WAL append or sync fails, the call returns an error and the engine becomes *poisoned*
/// (see [`StorageEngine::is_poisoned`]): the on-disk outcome of the failed write is unknown, so
/// after reopening the failed write may or may not be present (but never partially applied).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteDurability {
    /// Logs to `wal_sync.db` (`FILE_FLAG_WRITE_THROUGH` / `O_DSYNC`), surviving immediate
    /// physical power loss upon return.
    SyncTransactional,
    /// Logs to `wal_buf.db` (OS Page Cache), surviving application crashes (`kill -9`)
    /// with microsecond latency.
    BufferedFast,
}

/// Controls how the engine tracks read-path `AccessTick` updates on Capped Tables.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LruReadTrackingMode {
    /// Default architecture: Updates only the bounded in-memory [`AccessTracker`] on `get_row()`
    /// (`O(1)`, zero disk I/O) and lazily promotes hot entries only when scanned during eviction.
    CoalescedLazyPromotion,
    /// Baseline comparison mode (for benchmarking): Immediately logs and indexes a new
    /// `[TableID | 'l' | NewTick | PK]` mutation on every `get_row()` hit.
    EagerWriteOnRead,
}

/// Default number of immutable `idx_<SeqID>.db` segments that triggers automatic compaction.
pub const DEFAULT_AUTO_COMPACT_SEGMENT_THRESHOLD: usize = 8;

/// Default maximum in-memory transaction footprint (`64 MB`). `0` disables the limit.
pub const DEFAULT_MAX_TRANSACTION_BYTES: usize = 64 * 1024 * 1024;

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
    /// Maximum uncommitted transaction payload + WAL record budget in bytes (`0` = unlimited).
    pub max_transaction_bytes: usize,
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
            max_transaction_bytes: DEFAULT_MAX_TRANSACTION_BYTES,
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
    reader: Option<BlobSegmentReader<F>>,
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
    pub(super) fs: Arc<FS>,
    pub(super) config: EngineConfig,
    pub(super) meta_mgr: BootMetaManager<FS>,
    pub(super) sync_wal: WalStreamWriter<FS::File>,
    pub(super) buf_wal: WalStreamWriter<FS::File>,
    pub(super) next_lsn: u64,
    pub(super) memtable: MemTable,
    /// Active immutable index segments (`idx_<SeqID>.db`), ordered oldest to newest (`last()` is newest).
    pub(super) idx_segments: Vec<IdxSegmentReader<FS::File>>,
    /// Active standard blob segments (`seq_id -> reader + stats`).
    pub(super) std_blobs: BTreeMap<u64, TrackedStdBlob<FS::File>>,
    pub(super) active_std_writer: Option<BlobSegmentWriter<FS::File>>,
    /// Active per-table LRU blob segments (`(table_id, seq_id) -> reader + stats`).
    pub(super) lru_blobs: BTreeMap<(u16, u64), TrackedLruBlob<FS::File>>,
    pub(super) active_lru_writers: HashMap<u16, BlobSegmentWriter<FS::File>>,
    /// Capped tables (`table_id -> CappedTableState`).
    pub(super) capped_tables: HashMap<u16, CappedTableState>,
    /// Bounded in-memory `AccessTick` tracker.
    pub(super) access_tracker: AccessTracker,
    /// Transaction nesting depth (`xBegin` / `xCommit`). While `tx_depth > 0`, serialized
    /// `WalRecord` bytes are accumulated directly into `pending_tx_buf` and committed as a
    /// single atomic `WalBatch` on `commit_transaction`!
    pub(super) tx_depth: usize,
    /// Optional connection identifier (`*mut sqlite3 as usize`) owning the active transaction.
    pub(super) tx_owner_conn: Option<usize>,
    /// True if `flush_memtable()` was requested during an active transaction.
    pub(super) deferred_flush_on_commit: bool,
    /// True if `compact()` was requested during an active transaction.
    pub(super) deferred_compact_on_commit: bool,
    /// True if `prepare_commit_transaction()` (`xSync`) has already run for the current commit.
    pub(super) tx_prepared: bool,
    /// True while `prepare_commit_transaction()` is running internal quota eviction.
    pub(super) tx_preparing: bool,
    pub(super) pending_tx_buf: Vec<u8>,
    pub(super) pending_tx_record_count: u32,
    pub(super) pending_tx_bytes: usize,
    pub(super) pending_tx_sync: bool,
    pub(super) tx_memtable_undo: Vec<(Vec<u8>, Option<crate::memtable::MemEntry>)>,
    pub(super) tx_start_meta: TxMetaSnapshot,
    pub(super) savepoints: Vec<SavepointMarker>,
    /// Tables dropped inside the currently active transaction (`(table_id, was_capped)`).
    pub(super) dropped_tables_in_tx: Vec<(u16, bool)>,
    /// True if any capped-table quota was registered or modified inside the active transaction.
    pub(super) quotas_modified_in_tx: bool,
    /// True while a non-owning connection is reading the temporary committed snapshot.
    pub(super) in_committed_read_view: bool,
    /// Registry of active virtual tables (`table_id -> table_name`) preventing duplicate `table_id` sharing.
    pub(super) active_vtab_ids: HashMap<u16, Arc<str>>,
    /// Lazy per-table cache indicating whether `table_id` may contain `'t'` (`NS_ROW_META`) keys.
    /// Standard uncapped tables without secondary indexes resolve to `false` once and skip
    /// all `'t'` negative point lookups on `UPDATE` and `DELETE`!
    pub(super) table_has_row_meta: BTreeMap<u16, bool>,
    /// Highest LSN appended to `buf_wal`.
    pub(super) last_buf_append_lsn: u64,
    /// Highest LSN in `buf_wal` synced to durable storage.
    pub(super) last_buf_synced_lsn: u64,
    /// Set after a WAL append/sync failure (original error text). While set, every mutating
    /// operation fails and the engine must be reopened (the on-disk state is unknown).
    pub(super) poisoned: Option<String>,
}

impl<FS: PlatformFs> StorageEngine<FS> {
    /// Opens or recovers a `StorageEngine` rooted at `fs`.
    pub fn open(fs: Arc<FS>, config: EngineConfig) -> io::Result<Self> {
        let mut meta_mgr = BootMetaManager::open_or_recover_deferred_sweep(fs.clone())?;
        let state_snapshot = meta_mgr.state().clone();

        // 1. Open active `idx_<SeqID>.db` segments
        let mut idx_segments = Vec::with_capacity(state_snapshot.idx_segments.len());
        for seg_meta in &state_snapshot.idx_segments {
            let reader = IdxSegmentReader::open(&*fs, seg_meta.seq_id)?;
            idx_segments.push(reader);
        }

        // 2. Open active `blob_std_<SeqID>.db` segments
        let mut std_blobs = BTreeMap::new();
        let mut std_valid_end = BTreeMap::new();
        for bmeta in &state_snapshot.std_blob_segments {
            let fname = format_std_blob_file_name(bmeta.seq_id);
            let file = fs.open_file(&fname, false, true, OpenHint::ImmutableSequential)?;
            let physical_len = file.file_len()?;
            let file_len = physical_len.max(bmeta.file_len);
            std_valid_end.insert(bmeta.seq_id, bmeta.file_len.min(physical_len));
            std_blobs.insert(
                bmeta.seq_id,
                TrackedStdBlob {
                    reader: BlobSegmentReader::new(file, bmeta.seq_id),
                    file_len,
                    dead_bytes: bmeta.dead_bytes,
                },
            );
        }

        // 3. Open active `blob_lru_<TblID>_<SeqID>.db` segments
        let mut lru_blobs = BTreeMap::new();
        let mut lru_valid_end = BTreeMap::new();
        for lmeta in &state_snapshot.lru_blob_segments {
            let fname = format_lru_blob_file_name(lmeta.table_id, lmeta.seq_id);
            let file = fs.open_file(&fname, false, true, OpenHint::ImmutableSequential)?;
            let physical_len = file.file_len()?;
            let file_len = physical_len.max(lmeta.file_len);
            lru_valid_end.insert(
                (lmeta.table_id, lmeta.seq_id),
                lmeta.file_len.min(physical_len),
            );
            lru_blobs.insert(
                (lmeta.table_id, lmeta.seq_id),
                TrackedLruBlob {
                    table_id: lmeta.table_id,
                    reader: BlobSegmentReader::new(file, lmeta.seq_id),
                    file_len,
                    dead_bytes: lmeta.dead_bytes,
                },
            );
        }

        // Also discover any uncataloged `blob_std_*` or `blob_lru_*` created during the active epoch
        // before a crash so `Write-Blob-Before-WAL` validation can verify their records!
        for fname in fs.list_files()? {
            if let Some(hex) = fname
                .strip_prefix("blob_std_")
                .and_then(|s| s.strip_suffix(".db"))
            {
                if let Ok(seq_id) = u64::from_str_radix(hex, 16) {
                    if !std_blobs.contains_key(&seq_id) {
                        if let Ok(file) =
                            fs.open_file(&fname, false, true, OpenHint::ImmutableSequential)
                        {
                            let flen = file.file_len().unwrap_or(0);
                            std_valid_end.insert(seq_id, 0);
                            std_blobs.insert(
                                seq_id,
                                TrackedStdBlob {
                                    reader: BlobSegmentReader::new(file, seq_id),
                                    file_len: flen,
                                    dead_bytes: 0,
                                },
                            );
                        }
                    }
                }
            } else if let Some(rest) = fname
                .strip_prefix("blob_lru_")
                .and_then(|s| s.strip_suffix(".db"))
            {
                if let Some((tbl_hex, seq_hex)) = rest.split_once('_') {
                    if let (Ok(tbl_id), Ok(seq_id)) = (
                        u16::from_str_radix(tbl_hex, 16),
                        u64::from_str_radix(seq_hex, 16),
                    ) {
                        if !lru_blobs.contains_key(&(tbl_id, seq_id)) {
                            if let Ok(file) =
                                fs.open_file(&fname, false, true, OpenHint::ImmutableSequential)
                            {
                                let flen = file.file_len().unwrap_or(0);
                                lru_valid_end.insert((tbl_id, seq_id), 0);
                                lru_blobs.insert(
                                    (tbl_id, seq_id),
                                    TrackedLruBlob {
                                        table_id: tbl_id,
                                        reader: BlobSegmentReader::new(file, seq_id),
                                        file_len: flen,
                                        dead_bytes: 0,
                                    },
                                );
                            }
                        }
                    }
                }
            }
        }

        // For any blob segment whose physical `file_len` exceeds its cataloged `bmeta.file_len`
        // (or is uncataloged), also scan `idx_segments` in case an `idx` segment already
        // references blobs in that segment (e.g. if `boot_meta.db` fell back to an older slot).
        let needs_idx_blob_scan = std_blobs
            .iter()
            .any(|(&seq, b)| b.file_len > std_valid_end.get(&seq).copied().unwrap_or(0))
            || lru_blobs
                .iter()
                .any(|(&k, b)| b.file_len > lru_valid_end.get(&k).copied().unwrap_or(0));
        if needs_idx_blob_scan {
            for seg in &idx_segments {
                let mut cur = seg.cursor_all()?;
                while let Some(entry) = cur.next_entry()? {
                    if let IndexValue::Blob(ptr) = entry.value {
                        let end = (ptr.offset as u64).saturating_add(ptr.len as u64);
                        match ptr.kind {
                            BlobKind::Standard => {
                                if let Some(ve) = std_valid_end.get_mut(&ptr.seg_id) {
                                    *ve = (*ve).max(end);
                                }
                            }
                            BlobKind::Lru => {
                                if let Some(ve) =
                                    lru_valid_end.get_mut(&(ptr.table_id, ptr.seg_id))
                                {
                                    *ve = (*ve).max(end);
                                }
                            }
                        }
                    }
                }
            }
        }

        // 4. Open `wal_sync.db` and `wal_buf.db` and run Two-Stream Recovery
        let mut sync_wal = WalStreamWriter::open(
            &*fs,
            WAL_SYNC_FILE_NAME,
            config.sync_wal_size_bytes,
            state_snapshot.sync_wal_epoch,
            WalStreamMode::SyncSectorIsolated,
        )?;
        let mut buf_wal = WalStreamWriter::open(
            &*fs,
            WAL_BUF_FILE_NAME,
            config.buf_wal_size_bytes,
            state_snapshot.buf_wal_epoch,
            config.buf_wal_mode,
        )?;

        let rec = recover_two_streams(
            &**sync_wal.file(),
            state_snapshot.sync_wal_epoch,
            &**buf_wal.file(),
            state_snapshot.buf_wal_epoch,
            state_snapshot.checkpoint_lsn,
            |ptr| match ptr.kind {
                BlobKind::Standard => std_blobs
                    .get(&ptr.seg_id)
                    .map(|b| b.reader.validate_blob_ptr(ptr))
                    .unwrap_or(false),
                BlobKind::Lru => lru_blobs
                    .get(&(ptr.table_id, ptr.seg_id))
                    .map(|b| b.reader.validate_blob_ptr(ptr))
                    .unwrap_or(false),
            },
        )?;

        sync_wal.set_cursor_after_recovery(rec.next_sync_sector_idx);
        buf_wal.set_cursor_after_recovery(rec.next_buf_sector_idx);

        let mut memtable = MemTable::new(config.memtable_capacity_bytes);
        for batch in rec.merged_batches {
            let batch_lsn = batch.lsn;
            for record in batch.records {
                if let WalRecord::PutBlobPtr { blob_ptr: ptr, .. } = &record {
                    let end = (ptr.offset as u64).saturating_add(ptr.len as u64);
                    match ptr.kind {
                        BlobKind::Standard => {
                            if let Some(ve) = std_valid_end.get_mut(&ptr.seg_id) {
                                *ve = (*ve).max(end);
                            }
                        }
                        BlobKind::Lru => {
                            if let Some(ve) = lru_valid_end.get_mut(&(ptr.table_id, ptr.seg_id)) {
                                *ve = (*ve).max(end);
                            }
                        }
                    }
                }
                let key_opt = match &record {
                    WalRecord::PutInline { key, .. }
                    | WalRecord::PutBlobPtr { key, .. }
                    | WalRecord::Delete { key } => Some(key.as_slice()),
                    WalRecord::TxBegin | WalRecord::TxCommit => None,
                };
                if let Some(key) = key_opt {
                    let old_ptr = if let Some(mem_entry) = memtable.get(key) {
                        match mem_entry.value {
                            IndexValue::Blob(ptr) => Some(ptr),
                            _ => None,
                        }
                    } else {
                        let mut found = None;
                        for seg in idx_segments.iter().rev() {
                            if let Some((_lsn, val)) = seg.get(key)? {
                                if let IndexValue::Blob(ptr) = val {
                                    found = Some(ptr);
                                }
                                break;
                            }
                        }
                        found
                    };
                    if let Some(ptr) = old_ptr {
                        match ptr.kind {
                            BlobKind::Standard => {
                                if let Some(b) = std_blobs.get_mut(&ptr.seg_id) {
                                    b.dead_bytes = b.dead_bytes.saturating_add(ptr.len as u64);
                                }
                            }
                            BlobKind::Lru => {
                                if let Some(b) = lru_blobs.get_mut(&(ptr.table_id, ptr.seg_id)) {
                                    b.dead_bytes = b.dead_bytes.saturating_add(ptr.len as u64);
                                }
                            }
                        }
                    }
                }
                // Apply each record immediately so subsequent mutations to the same key within
                // the same `WalBatch` observe the updated `MemTable` entry!
                memtable.apply_single_wal_record(batch_lsn, record);
            }
        }

        // Reconcile unreferenced uncommitted tail bytes and prune any uncataloged
        // blob segment that had zero valid references.
        let cataloged_std_seqs: std::collections::HashSet<u64> = state_snapshot
            .std_blob_segments
            .iter()
            .map(|s| s.seq_id)
            .collect();
        let cataloged_lru_keys: std::collections::HashSet<(u16, u64)> = state_snapshot
            .lru_blob_segments
            .iter()
            .map(|s| (s.table_id, s.seq_id))
            .collect();

        let mut had_uncataloged_live_blobs = false;
        std_blobs.retain(|&seq_id, b| {
            let valid_end = std_valid_end.get(&seq_id).copied().unwrap_or(0);
            if !cataloged_std_seqs.contains(&seq_id) && valid_end == 0 {
                return false;
            }
            if !cataloged_std_seqs.contains(&seq_id) {
                had_uncataloged_live_blobs = true;
            }
            if b.file_len > valid_end {
                let tail_garbage = b.file_len - valid_end;
                b.dead_bytes = b.dead_bytes.saturating_add(tail_garbage).min(b.file_len);
            }
            true
        });
        lru_blobs.retain(|&(tid, seq_id), b| {
            let valid_end = lru_valid_end.get(&(tid, seq_id)).copied().unwrap_or(0);
            if !cataloged_lru_keys.contains(&(tid, seq_id)) && valid_end == 0 {
                return false;
            }
            if !cataloged_lru_keys.contains(&(tid, seq_id)) {
                had_uncataloged_live_blobs = true;
            }
            if b.file_len > valid_end {
                let tail_garbage = b.file_len - valid_end;
                b.dead_bytes = b.dead_bytes.saturating_add(tail_garbage).min(b.file_len);
            }
            true
        });

        // Bump `next_seq_id` past any recovered blob segment SeqIDs if necessary
        let max_blob_seq = std_blobs
            .keys()
            .copied()
            .chain(lru_blobs.keys().map(|&(_, s)| s))
            .max()
            .unwrap_or(0);
        if max_blob_seq >= meta_mgr.state().next_seq_id || had_uncataloged_live_blobs {
            let std_metas: Vec<StdBlobSegmentMeta> = std_blobs
                .iter()
                .map(|(&seq_id, b)| StdBlobSegmentMeta {
                    seq_id,
                    file_len: b.file_len,
                    dead_bytes: b.dead_bytes,
                })
                .collect();
            let lru_metas: Vec<LruBlobSegmentMeta> = lru_blobs
                .iter()
                .map(|(&(_tid, seq_id), b)| LruBlobSegmentMeta {
                    seq_id,
                    file_len: b.file_len,
                    dead_bytes: b.dead_bytes,
                    table_id: b.table_id,
                })
                .collect();
            meta_mgr.commit_update(|st| {
                if max_blob_seq >= st.next_seq_id {
                    st.next_seq_id = max_blob_seq + 1;
                }
                st.std_blob_segments = std_metas;
                st.lru_blob_segments = lru_metas;
            })?;
        }

        // 5. Restore Capped Table quotas and scan `'t'` / `'l'` keys to sync logical bytes & ticks
        let mut capped_tables = HashMap::new();
        for q in &state_snapshot.table_quotas {
            if q.eviction_policy == EvictionPolicy::Lru {
                let mut ct = CappedTableState::new(q.table_id, q.max_bytes);
                ct.current_logical_bytes = q.current_logical_bytes;
                capped_tables.insert(q.table_id, ct);
            }
        }

        let mut engine = Self {
            fs,
            config,
            meta_mgr,
            sync_wal,
            buf_wal,
            next_lsn: rec.max_recovered_lsn + 1,
            memtable,
            idx_segments,
            std_blobs,
            active_std_writer: None,
            lru_blobs,
            active_lru_writers: HashMap::new(),
            capped_tables,
            access_tracker: AccessTracker::new(1, 65_536),
            tx_depth: 0,
            tx_owner_conn: None,
            deferred_flush_on_commit: false,
            deferred_compact_on_commit: false,
            tx_prepared: false,
            tx_preparing: false,
            pending_tx_buf: Vec::new(),
            pending_tx_record_count: 0,
            pending_tx_bytes: 0,
            pending_tx_sync: false,
            tx_memtable_undo: Vec::new(),
            tx_start_meta: TxMetaSnapshot::default(),
            savepoints: Vec::new(),
            dropped_tables_in_tx: Vec::new(),
            quotas_modified_in_tx: false,
            in_committed_read_view: false,
            active_vtab_ids: HashMap::new(),
            table_has_row_meta: BTreeMap::new(),
            last_buf_append_lsn: 0,
            last_buf_synced_lsn: 0,
            poisoned: None,
        };
        engine.access_tracker = AccessTracker::new(1, engine.config.max_in_memory_access_ticks);
        engine.reconcile_capped_tables_after_recovery()?;

        // Always rotate both WAL epochs after recovery, even when nothing was recovered.
        // Recovery halts at the first torn/old sector, but later sectors of the SAME epoch may
        // already be on disk (write-back reordering before a crash). If we kept the epoch, new
        // writes would restart at that halt point and a later recovery could run past the new
        // data into those stale same-epoch sectors, replaying batches that were never recovered
        // the first time (with colliding LSNs). A fresh epoch makes every pre-crash sector
        // unreadable as part of the new stream. Cost: one boot_meta commit per open.
        if !engine.memtable.is_empty() {
            engine.flush_memtable()?;
        } else {
            engine.rotate_wal_epochs_only()?;
        }

        // Now that WAL recovery and the post-recovery checkpoint are complete, sweep any truly
        // orphaned segment/manifest files and process pending deletes!
        engine.meta_mgr.sweep_orphans()?;

        Ok(engine)
    }

    /// Returns the next LSN that will be assigned to a WAL batch.
    /// Returns true if a WAL append/sync failure poisoned this engine. Reads keep working, but
    /// all mutations fail until the engine is reopened.
    pub fn is_poisoned(&self) -> bool {
        self.poisoned.is_some()
    }

    /// Fails with an `io::ErrorKind::Other` error if a prior WAL I/O failure poisoned the engine.
    pub(super) fn check_not_poisoned(&self) -> io::Result<()> {
        match &self.poisoned {
            None => Ok(()),
            Some(msg) => Err(io::Error::other(format!(
                "engine is poisoned by an earlier WAL I/O failure and must be reopened: {msg}"
            ))),
        }
    }

    /// Records a WAL I/O failure (first one wins) and returns the original error.
    pub(super) fn poison(&mut self, e: io::Error) -> io::Error {
        if self.poisoned.is_none() {
            self.poisoned = Some(e.to_string());
        }
        e
    }

    pub fn next_lsn(&self) -> u64 {
        self.next_lsn
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

    /// Registers or resolves a virtual table `(table_name, explicit_table_id)` in the engine's
    /// active registry and persistent catalog (`table_id = 0` under `'c'`) using
    /// [`WriteDurability::BufferedFast`].
    pub fn register_or_resolve_vtab(
        &mut self,
        table_name: &str,
        explicit_id: Option<u16>,
    ) -> io::Result<u16> {
        self.register_or_resolve_vtab_with_durability(
            table_name,
            explicit_id,
            WriteDurability::BufferedFast,
        )
    }

    /// Registers or resolves a virtual table `(table_name, explicit_table_id)` in the engine's
    /// active registry and persistent catalog (`table_id = 0` under `'c'`) with the specified
    /// [`WriteDurability`].
    pub fn register_or_resolve_vtab_with_durability(
        &mut self,
        table_name: &str,
        explicit_id: Option<u16>,
        durability: WriteDurability,
    ) -> io::Result<u16> {
        self.check_not_poisoned()?;
        let norm_name = table_name.trim().to_ascii_lowercase();
        let catalog_prefix = [0u8, 0u8, b'c'];
        let entries = self.scan_merged_prefix(&catalog_prefix)?;
        let mut persisted_name_to_id: HashMap<String, u16> = HashMap::new();
        let mut persisted_id_to_name: HashMap<u16, String> = HashMap::new();
        for entry in entries {
            if entry.key.len() > 3 {
                if let IndexValue::Inline(val) = entry.value {
                    if val.len() == 2 {
                        let name = String::from_utf8_lossy(&entry.key[3..]).into_owned();
                        let id = u16::from_be_bytes([val[0], val[1]]);
                        persisted_name_to_id.insert(name.clone(), id);
                        persisted_id_to_name.insert(id, name);
                    }
                }
            }
        }

        let chosen_id = match explicit_id {
            Some(id) => {
                if id == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "table_id 0 is reserved for engine system catalog",
                    ));
                }
                if let Some(existing_name) = self.active_vtab_ids.get(&id) {
                    if existing_name.as_ref() != norm_name.as_str() {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!(
                                "table_id {id} is already registered to live virtual table '{existing_name}'"
                            ),
                        ));
                    }
                }
                id
            }
            None => {
                if let Some(&id) = persisted_name_to_id.get(&norm_name) {
                    if let Some(existing_name) = self.active_vtab_ids.get(&id) {
                        if existing_name.as_ref() != norm_name.as_str() {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidInput,
                                format!(
                                    "table_id {id} is already registered to live virtual table '{existing_name}'"
                                ),
                            ));
                        }
                    }
                    id
                } else {
                    let mut candidate = 1u16;
                    while candidate < u16::MAX
                        && (self.active_vtab_ids.contains_key(&candidate)
                            || persisted_id_to_name.contains_key(&candidate))
                    {
                        candidate += 1;
                    }
                    candidate
                }
            }
        };

        self.active_vtab_ids
            .insert(chosen_id, Arc::from(norm_name.as_str()));
        if persisted_name_to_id.get(&norm_name).copied() != Some(chosen_id) {
            let mut cat_key = Vec::with_capacity(3 + norm_name.len());
            cat_key.extend_from_slice(&catalog_prefix);
            cat_key.extend_from_slice(norm_name.as_bytes());
            if !IndexEntry::inline_fits_in_block(cat_key.len(), 2) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "virtual table name exceeds maximum catalog key length",
                ));
            }
            self.commit_wal_and_memtable(
                vec![WalRecord::PutInline {
                    key: cat_key,
                    value: chosen_id.to_be_bytes().to_vec(),
                }],
                durability,
            )?;
        }
        Ok(chosen_id)
    }

    /// Unregisters a disconnected virtual table `(table_id, table_name)` from the live registry.
    pub fn unregister_vtab(&mut self, table_id: u16, table_name: &str) {
        let norm_name = table_name.trim().to_ascii_lowercase();
        if self.active_vtab_ids.get(&table_id).map(|s| s.as_ref()) == Some(norm_name.as_str()) {
            self.active_vtab_ids.remove(&table_id);
        }
    }

    /// Drops a table (`xDestroy` / `DROP TABLE`), deleting all primary rows (`'r'`), secondary
    /// index entries (`'i'`), row metadata (`'t'`), LRU queue entries (`'l'`), capped table
    /// quota registrations, and system catalog entries.
    pub fn drop_table(&mut self, table_id: u16, table_name: Option<&str>) -> io::Result<()> {
        self.check_not_poisoned()?;
        let mut records = Vec::new();
        let mut dead_ptrs = Vec::new();

        for ns in [NS_PRIMARY_ROW, NS_SECONDARY_INDEX, NS_ROW_META, NS_LRU_QUEUE] {
            let prefix = encode_namespace_prefix(table_id, ns);
            let entries = self.scan_merged_prefix(&prefix)?;
            for entry in entries {
                if let IndexValue::Blob(ptr) = entry.value {
                    dead_ptrs.push(ptr);
                }
                records.push(WalRecord::Delete { key: entry.key });
            }
        }

        let removed_name = self.active_vtab_ids.remove(&table_id).map(|s| s.to_string());
        let name_to_clean = table_name
            .map(|s| s.trim().to_ascii_lowercase())
            .or(removed_name);
        if let Some(norm_name) = name_to_clean {
            let mut cat_key = Vec::with_capacity(3 + norm_name.len());
            cat_key.extend_from_slice(&[0u8, 0u8, b'c']);
            cat_key.extend_from_slice(norm_name.as_bytes());
            if self.check_existing_row(&cat_key)? != ExistingRowStatus::NotFound {
                records.push(WalRecord::Delete { key: cat_key });
            }
        }

        if !records.is_empty() {
            self.commit_wal_and_memtable(records, WriteDurability::BufferedFast)?;
        }
        for ptr in dead_ptrs {
            self.record_dead_blob_bytes(&ptr);
        }

        let was_capped = self.capped_tables.remove(&table_id).is_some();
        self.table_has_row_meta.insert(table_id, false);
        if let Some(mut old_w) = self.active_lru_writers.remove(&table_id) {
            let _ = old_w.sync_durable();
            if let Some(b) = self.lru_blobs.get_mut(&(table_id, old_w.seq_id())) {
                b.file_len = old_w.current_len();
                b.dead_bytes = b.file_len;
            }
        }
        for (&(tid, _), b) in &mut self.lru_blobs {
            if tid == table_id {
                b.dead_bytes = b.file_len;
            }
        }

        if self.tx_depth == 0 {
            self.reclaim_fully_dead_lru_blob_segments(table_id)?;
            if was_capped {
                let quota_metas = self.build_quota_metas();
                self.meta_mgr.commit_update(|st| {
                    st.table_quotas = quota_metas;
                })?;
            }
        } else {
            self.dropped_tables_in_tx.push((table_id, was_capped));
        }
        Ok(())
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
        self.check_not_poisoned()?;
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
        let logical_size = (pk.len() + value.len()) as u32;
        if let Some(ct) = self.capped_tables.get(&table_id) {
            if (3 + pk.len() + value.len()) as u64 > ct.max_bytes {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "row logical size exceeds capped table max_bytes quota",
                ));
            }
        }

        // 1. Check if the row already exists so we can clean up old blob bytes, old secondary keys,
        //    and old LRU queue entries (`[TableID | 'l' | old_tick | pk]`).
        let mut records = Vec::with_capacity(3 + secondary_keys.len() * 2);
        let mut old_logical_size = 0u64;
        let mut old_blob_ptr = None;
        let mut old_meta_blob_ptr = None;
        let mut had_old_meta = false;
        let meta_key = encode_row_meta_key(table_id, pk);

        let existing = if known_absent {
            ExistingRowStatus::NotFound
        } else {
            self.check_existing_row(&row_key)?
        };
        if let ExistingRowStatus::Blob(old_ptr) = existing {
            old_blob_ptr = Some(old_ptr);
        }

        if existing != ExistingRowStatus::NotFound && self.may_have_row_meta(table_id)? {
            if let Some((old_meta, m_ptr)) = self.get_row_meta_with_ptr(&meta_key)? {
                had_old_meta = true;
                old_meta_blob_ptr = m_ptr;
                old_logical_size = old_meta.logical_size as u64;
                if is_capped && old_meta.recorded_tick > 0 {
                    records.push(WalRecord::Delete {
                        key: encode_lru_queue_key(table_id, old_meta.recorded_tick, pk),
                    });
                }
                for (old_idx_id, old_col_val) in &old_meta.secondary_keys {
                    records.push(WalRecord::Delete {
                        key: encode_secondary_index_key(table_id, *old_idx_id, old_col_val, pk),
                    });
                }
            }
        }

        // 2. Write out-of-line blob FIRST if `value.len() >= inline_threshold_bytes` or if inline entry exceeds 4 KB block
        let store_as_blob = value.len() >= self.config.inline_threshold_bytes
            || !IndexEntry::inline_fits_in_block(row_key.len(), value.len());
        if self.tx_depth > 0
            && !self.tx_preparing
            && self.config.max_transaction_bytes > 0
        {
            let est_row_bytes = (if store_as_blob { 28 } else { 9 })
                + row_key.len()
                + value.len();
            if self.pending_tx_bytes.saturating_add(est_row_bytes)
                > self.config.max_transaction_bytes
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "transaction exceeds max_transaction_bytes budget",
                ));
            }
        }
        if store_as_blob {
            let blob_kind = if is_capped {
                BlobKind::Lru
            } else {
                BlobKind::Standard
            };
            let ptr = self.append_to_blob_segment(blob_kind, table_id, &row_key, &value)?;
            if durability == WriteDurability::SyncTransactional && self.tx_depth == 0 {
                match blob_kind {
                    BlobKind::Standard => {
                        if let Some(w) = &mut self.active_std_writer {
                            w.sync_durable()?;
                            if let Some(b) = self.std_blobs.get_mut(&w.seq_id()) {
                                b.file_len = w.current_len();
                            }
                        }
                    }
                    BlobKind::Lru => {
                        if let Some(w) = self.active_lru_writers.get_mut(&table_id) {
                            w.sync_durable()?;
                            if let Some(b) = self.lru_blobs.get_mut(&(table_id, w.seq_id())) {
                                b.file_len = w.current_len();
                            }
                        }
                    }
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
            self.access_tracker.remove_key(table_id, pk);
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
        if need_meta {
            self.table_has_row_meta.insert(table_id, true);
            let meta_bytes =
                RowMetaRecord::encode_slice(assigned_tick, logical_size, secondary_keys);
            if !IndexEntry::inline_fits_in_block(meta_key.len(), meta_bytes.len()) {
                let ptr =
                    self.append_to_blob_segment(BlobKind::Standard, 0, &meta_key, &meta_bytes)?;
                if durability == WriteDurability::SyncTransactional && self.tx_depth == 0 {
                    if let Some(w) = &mut self.active_std_writer {
                        w.sync_durable()?;
                        if let Some(b) = self.std_blobs.get_mut(&w.seq_id()) {
                            b.file_len = w.current_len();
                        }
                    }
                }
                records.push(WalRecord::PutBlobPtr {
                    key: meta_key,
                    blob_ptr: ptr,
                });
            } else {
                records.push(WalRecord::PutInline {
                    key: meta_key,
                    value: meta_bytes,
                });
            }
        } else if had_old_meta {
            records.push(WalRecord::Delete { key: meta_key });
        }

        // 5. Append atomic batch to WAL and apply to MemTable
        self.commit_wal_and_memtable(records, durability)?;

        // 6. Apply dead blob byte accounting and update Capped Table logical byte counter after WAL succeeds
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
        if is_capped && self.tx_depth == 0 {
            self.evict_until_within_quota(table_id, durability)?;
        }

        Ok(())
    }

    /// Point lookup of a row (`pk`) in `table_id`.
    /// If `table_id` is a Capped Table, updates its `AccessTick` according to `config.lru_read_tracking`.
    pub fn get_row(&mut self, table_id: u16, pk: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let row_key = encode_primary_row_key(table_id, pk);
        let Some((_lsn, idx_val)) = self.lookup_raw_entry(&row_key)? else {
            return Ok(None);
        };

        let resolved = match idx_val {
            IndexValue::Inline(bytes) => Some(bytes),
            IndexValue::Blob(ptr) => self.dereference_blob(&ptr, &row_key)?,
            IndexValue::Tombstone => None,
        };

        if resolved.is_some() && self.capped_tables.contains_key(&table_id) {
            if self.in_committed_read_view {
                self.access_tracker.record_read_hit(table_id, pk);
            } else {
                match self.config.lru_read_tracking {
                    LruReadTrackingMode::CoalescedLazyPromotion => {
                        self.access_tracker.record_read_hit(table_id, pk);
                    }
                    LruReadTrackingMode::EagerWriteOnRead => {
                        self.eager_promote_lru_on_read(table_id, pk)?;
                    }
                }
            }
        }

        Ok(resolved)
    }

    /// Deletes a row (`pk`) in `table_id`, cascading tombstones to its secondary indexes and LRU queue.
    pub fn delete_row(
        &mut self,
        table_id: u16,
        pk: &[u8],
        durability: WriteDurability,
    ) -> io::Result<bool> {
        self.check_not_poisoned()?;
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

        let mut records = vec![WalRecord::Delete { key: row_key }];
        let mut old_logical_size = 0u64;
        let mut old_meta_blob_ptr = None;

        if self.may_have_row_meta(table_id)? {
            let meta_key = encode_row_meta_key(table_id, pk);
            if let Some((old_meta, m_ptr)) = self.get_row_meta_with_ptr(&meta_key)? {
                old_meta_blob_ptr = m_ptr;
                old_logical_size = old_meta.logical_size as u64;
                if old_meta.recorded_tick > 0 {
                    records.push(WalRecord::Delete {
                        key: encode_lru_queue_key(table_id, old_meta.recorded_tick, pk),
                    });
                }
                for (old_idx_id, old_col_val) in &old_meta.secondary_keys {
                    records.push(WalRecord::Delete {
                        key: encode_secondary_index_key(table_id, *old_idx_id, old_col_val, pk),
                    });
                }
                records.push(WalRecord::Delete { key: meta_key });
            }
        }

        self.access_tracker.remove_key(table_id, pk);
        self.commit_wal_and_memtable(records, durability)?;

        if let Some(old_ptr) = old_blob_ptr {
            self.record_dead_blob_bytes(&old_ptr);
        }
        if let Some(old_m_ptr) = old_meta_blob_ptr {
            self.record_dead_blob_bytes(&old_m_ptr);
        }
        if let Some(ct) = self.capped_tables.get_mut(&table_id) {
            ct.current_logical_bytes = ct.current_logical_bytes.saturating_sub(old_logical_size);
        }
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
        if let Some(mem_entry) = self.memtable.get(row_key) {
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
                    if self.dereference_blob_append_into(&ptr_copy, row_key, slab)? {
                        spans.push((start, slab.len()));
                        hit = true;
                    }
                }
                IndexValue::Tombstone => {}
            }
        } else {
            for seg in self.idx_segments.iter().rev() {
                if let Some((_lsn, val)) = seg.get(row_key)? {
                    match val {
                        IndexValue::Inline(bytes) => {
                            let start = slab.len();
                            slab.extend_from_slice(&bytes);
                            spans.push((start, slab.len()));
                            hit = true;
                        }
                        IndexValue::Blob(ptr) => {
                            let start = slab.len();
                            if self.dereference_blob_append_into(&ptr, row_key, slab)? {
                                spans.push((start, slab.len()));
                                hit = true;
                            }
                        }
                        IndexValue::Tombstone => {}
                    }
                    break;
                }
            }
        }

        if hit && self.capped_tables.contains_key(&table_id) {
            if self.in_committed_read_view {
                self.access_tracker.record_read_hit(table_id, pk);
            } else {
                match self.config.lru_read_tracking {
                    LruReadTrackingMode::CoalescedLazyPromotion => {
                        self.access_tracker.record_read_hit(table_id, pk);
                    }
                    LruReadTrackingMode::EagerWriteOnRead => {
                        self.eager_promote_lru_on_read(table_id, pk)?;
                    }
                }
            }
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
        if let Some(mem_entry) = self.memtable.get(row_key) {
            match &mem_entry.value {
                IndexValue::Inline(bytes) => {
                    let start = slab.len();
                    slab.extend_from_slice(bytes);
                    spans.push((start, slab.len()));
                    lazy_blobs.push(None);
                    hit = true;
                }
                IndexValue::Blob(ptr) => {
                    spans.push((0, 0));
                    lazy_blobs.push(Some(self.pin_lazy_blob(ptr)));
                    hit = true;
                }
                IndexValue::Tombstone => {}
            }
        } else {
            for seg in self.idx_segments.iter().rev() {
                if let Some((_lsn, val)) = seg.get(row_key)? {
                    match val {
                        IndexValue::Inline(bytes) => {
                            let start = slab.len();
                            slab.extend_from_slice(&bytes);
                            spans.push((start, slab.len()));
                            lazy_blobs.push(None);
                            hit = true;
                        }
                        IndexValue::Blob(ptr) => {
                            spans.push((0, 0));
                            lazy_blobs.push(Some(self.pin_lazy_blob(&ptr)));
                            hit = true;
                        }
                        IndexValue::Tombstone => {}
                    }
                    break;
                }
            }
        }

        if hit
            && matches!(lazy_blobs.last(), Some(None))
            && self.capped_tables.contains_key(&table_id)
        {
            if self.in_committed_read_view {
                self.access_tracker.record_read_hit(table_id, pk);
            } else {
                match self.config.lru_read_tracking {
                    LruReadTrackingMode::CoalescedLazyPromotion => {
                        self.access_tracker.record_read_hit(table_id, pk);
                    }
                    LruReadTrackingMode::EagerWriteOnRead => {
                        self.eager_promote_lru_on_read(table_id, pk)?;
                    }
                }
            }
        }

        Ok(hit)
    }

    /// Captures `ptr` together with a pinned handle to its segment file (see [`LazyBlob`]).
    pub(super) fn pin_lazy_blob(&self, ptr: &BlobPtr) -> LazyBlob<FS::File> {
        let reader = match ptr.kind {
            BlobKind::Standard => self.std_blobs.get(&ptr.seg_id).map(|b| b.reader.clone()),
            BlobKind::Lru => self
                .lru_blobs
                .get(&(ptr.table_id, ptr.seg_id))
                .map(|b| b.reader.clone()),
        };
        LazyBlob { ptr: *ptr, reader }
    }

    /// Dereferences an out-of-line blob pointer on demand for a cursor row, appending the verified
    /// blob payload to `slab` and returning its `(start_offset, end_offset)`.
    /// Also updates LRU read tracking if `table_id` is a capped table.
    pub fn dereference_lazy_blob_into(
        &mut self,
        table_id: u16,
        pk: &[u8],
        lazy: &LazyBlob<FS::File>,
        slab: &mut Vec<u8>,
    ) -> io::Result<Option<(usize, usize)>> {
        let ptr = &lazy.ptr;
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

        // Flush buffered writer bytes (if the segment is still the active one) so the pinned
        // handle sees them; harmless if the writer has since rolled to a new segment.
        self.ensure_blob_flushed(ptr)?;
        let Some(reader) = &lazy.reader else {
            return Ok(None);
        };
        let start = slab.len();
        if !reader.append_blob_into(ptr, Some(row_key), slab)? {
            return Ok(None);
        }
        let end = slab.len();

        if self.capped_tables.contains_key(&table_id) {
            if self.in_committed_read_view {
                self.access_tracker.record_read_hit(table_id, pk);
            } else {
                match self.config.lru_read_tracking {
                    LruReadTrackingMode::CoalescedLazyPromotion => {
                        self.access_tracker.record_read_hit(table_id, pk);
                    }
                    LruReadTrackingMode::EagerWriteOnRead => {
                        self.eager_promote_lru_on_read(table_id, pk)?;
                    }
                }
            }
        }

        Ok(Some((start, end)))
    }

    // =========================================================================
    // Internal Helper Methods
    // =========================================================================

    pub(super) fn flush_active_blob_buffers(&mut self) -> io::Result<()> {
        if let Some(w) = &mut self.active_std_writer {
            w.flush_buffer()?;
            if let Some(b) = self.std_blobs.get_mut(&w.seq_id()) {
                b.file_len = w.current_len();
            }
        }
        for w in self.active_lru_writers.values_mut() {
            w.flush_buffer()?;
            if let Some(b) = self.lru_blobs.get_mut(&(w.table_id(), w.seq_id())) {
                b.file_len = w.current_len();
            }
        }
        Ok(())
    }

    pub(super) fn sync_active_blob_writers(&mut self) -> io::Result<()> {
        if let Some(w) = &mut self.active_std_writer {
            w.sync_durable()?;
            if let Some(b) = self.std_blobs.get_mut(&w.seq_id()) {
                b.file_len = w.current_len();
            }
        }
        for w in self.active_lru_writers.values_mut() {
            w.sync_durable()?;
            if let Some(b) = self.lru_blobs.get_mut(&(w.table_id(), w.seq_id())) {
                b.file_len = w.current_len();
            }
        }
        Ok(())
    }

    pub(super) fn append_to_blob_segment(
        &mut self,
        kind: BlobKind,
        table_id: u16,
        key: &[u8],
        value: &[u8],
    ) -> io::Result<BlobPtr> {
        let needed = (16 + key.len() + value.len()) as u64;
        match kind {
            BlobKind::Standard => {
                let roll = self
                    .active_std_writer
                    .as_ref()
                    .map(|w| {
                        w.current_len() > 0
                            && w.current_len() + needed > self.config.max_blob_segment_bytes
                    })
                    .unwrap_or(true);
                if roll {
                    if let Some(mut old_w) = self.active_std_writer.take() {
                        old_w.sync_durable()?;
                        if let Some(b) = self.std_blobs.get_mut(&old_w.seq_id()) {
                            b.file_len = old_w.current_len();
                        }
                    }
                    let seq_id = self.meta_mgr.state().next_seq_id;
                    let writer =
                        BlobSegmentWriter::open(&*self.fs, BlobKind::Standard, 0, seq_id)?;
                    self.std_blobs.insert(
                        seq_id,
                        TrackedStdBlob {
                            reader: BlobSegmentReader::new(writer.file().clone(), seq_id),
                            file_len: 0,
                            dead_bytes: 0,
                        },
                    );
                    let std_metas = self.build_checkpointed_std_blob_metas();
                    self.meta_mgr.commit_update(|st| {
                        let allocated = st.allocate_seq_id();
                        debug_assert_eq!(allocated, seq_id);
                        st.std_blob_segments = std_metas;
                    })?;
                    self.active_std_writer = Some(writer);
                }
                let writer = self.active_std_writer.as_mut().unwrap();
                let ptr = writer.append_blob(key, value)?;
                if let Some(b) = self.std_blobs.get_mut(&writer.seq_id()) {
                    b.file_len = writer.current_len();
                }
                Ok(ptr)
            }
            BlobKind::Lru => {
                let roll = self
                    .active_lru_writers
                    .get(&table_id)
                    .map(|w| {
                        w.current_len() > 0
                            && w.current_len() + needed > self.config.max_blob_segment_bytes
                    })
                    .unwrap_or(true);
                if roll {
                    if let Some(mut old_w) = self.active_lru_writers.remove(&table_id) {
                        old_w.sync_durable()?;
                        if let Some(b) = self.lru_blobs.get_mut(&(table_id, old_w.seq_id())) {
                            b.file_len = old_w.current_len();
                        }
                    }
                    let seq_id = self.meta_mgr.state().next_seq_id;
                    let writer =
                        BlobSegmentWriter::open(&*self.fs, BlobKind::Lru, table_id, seq_id)?;
                    self.lru_blobs.insert(
                        (table_id, seq_id),
                        TrackedLruBlob {
                            table_id,
                            reader: BlobSegmentReader::new(writer.file().clone(), seq_id),
                            file_len: 0,
                            dead_bytes: 0,
                        },
                    );
                    let lru_metas = self.build_checkpointed_lru_blob_metas();
                    self.meta_mgr.commit_update(|st| {
                        let allocated = st.allocate_seq_id();
                        debug_assert_eq!(allocated, seq_id);
                        st.lru_blob_segments = lru_metas;
                    })?;
                    self.active_lru_writers.insert(table_id, writer);
                }
                let writer = self.active_lru_writers.get_mut(&table_id).unwrap();
                let ptr = writer.append_blob(key, value)?;
                if let Some(b) = self.lru_blobs.get_mut(&(table_id, writer.seq_id())) {
                    b.file_len = writer.current_len();
                }
                Ok(ptr)
            }
        }
    }

    pub(super) fn record_dead_blob_bytes(&mut self, ptr: &BlobPtr) {
        match ptr.kind {
            BlobKind::Standard => {
                if let Some(b) = self.std_blobs.get_mut(&ptr.seg_id) {
                    b.dead_bytes = b.dead_bytes.saturating_add(ptr.len as u64);
                }
            }
            BlobKind::Lru => {
                if let Some(b) = self.lru_blobs.get_mut(&(ptr.table_id, ptr.seg_id)) {
                    b.dead_bytes = b.dead_bytes.saturating_add(ptr.len as u64);
                }
            }
        }
    }

    pub(super) fn ensure_blob_flushed(&mut self, ptr: &BlobPtr) -> io::Result<()> {
        match ptr.kind {
            BlobKind::Standard => {
                if let Some(w) = &mut self.active_std_writer {
                    if w.seq_id() == ptr.seg_id
                        && (ptr.offset as u64).saturating_add(ptr.len as u64) > w.flushed_offset()
                    {
                        w.flush_buffer()?;
                        if let Some(b) = self.std_blobs.get_mut(&w.seq_id()) {
                            b.file_len = w.current_len();
                        }
                    }
                }
            }
            BlobKind::Lru => {
                if let Some(w) = self.active_lru_writers.get_mut(&ptr.table_id) {
                    if w.seq_id() == ptr.seg_id
                        && (ptr.offset as u64).saturating_add(ptr.len as u64) > w.flushed_offset()
                    {
                        w.flush_buffer()?;
                        if let Some(b) = self.lru_blobs.get_mut(&(ptr.table_id, w.seq_id())) {
                            b.file_len = w.current_len();
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub(super) fn dereference_blob(
        &mut self,
        ptr: &BlobPtr,
        expected_key: &[u8],
    ) -> io::Result<Option<Vec<u8>>> {
        self.ensure_blob_flushed(ptr)?;
        match ptr.kind {
            BlobKind::Standard => {
                let Some(b) = self.std_blobs.get(&ptr.seg_id) else {
                    return Ok(None);
                };
                b.reader.read_blob(ptr, Some(expected_key))
            }
            BlobKind::Lru => {
                let Some(b) = self.lru_blobs.get(&(ptr.table_id, ptr.seg_id)) else {
                    return Ok(None);
                };
                b.reader.read_blob(ptr, Some(expected_key))
            }
        }
    }

    pub(super) fn dereference_blob_append_into(
        &mut self,
        ptr: &BlobPtr,
        expected_key: &[u8],
        out: &mut Vec<u8>,
    ) -> io::Result<bool> {
        self.ensure_blob_flushed(ptr)?;
        self.read_flushed_blob_append_into(ptr, expected_key, out)
    }

    pub(super) fn read_flushed_blob_append_into(
        &self,
        ptr: &BlobPtr,
        expected_key: &[u8],
        out: &mut Vec<u8>,
    ) -> io::Result<bool> {
        let reader = match ptr.kind {
            BlobKind::Standard => {
                let Some(b) = self.std_blobs.get(&ptr.seg_id) else {
                    return Ok(false);
                };
                &b.reader
            }
            BlobKind::Lru => {
                let Some(b) = self.lru_blobs.get(&(ptr.table_id, ptr.seg_id)) else {
                    return Ok(false);
                };
                &b.reader
            }
        };
        if out.is_empty() {
            reader.read_blob_into(ptr, Some(expected_key), out)
        } else {
            reader.append_blob_into(ptr, Some(expected_key), out)
        }
    }

    pub(super) fn commit_wal_and_memtable(
        &mut self,
        records: Vec<WalRecord>,
        durability: WriteDurability,
    ) -> io::Result<u64> {
        self.check_not_poisoned()?;
        let lsn = self.next_lsn;

        if self.tx_depth > 0 {
            let batch_bytes: usize = records
                .iter()
                .map(|r| match r {
                    WalRecord::PutInline { key, value } => 9 + key.len() + value.len(),
                    WalRecord::PutBlobPtr { key, blob_ptr } => {
                        28 + key.len() + (blob_ptr.len as usize)
                    }
                    WalRecord::Delete { key } => 5 + key.len(),
                    WalRecord::TxBegin | WalRecord::TxCommit => 1,
                })
                .sum();
            let next_tx_bytes = self.pending_tx_bytes.saturating_add(batch_bytes);
            if !self.tx_preparing
                && self.config.max_transaction_bytes > 0
                && next_tx_bytes > self.config.max_transaction_bytes
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "transaction exceeds max_transaction_bytes budget",
                ));
            }
            self.pending_tx_bytes = next_tx_bytes;
            self.next_lsn += 1;
            if durability == WriteDurability::SyncTransactional {
                self.pending_tx_sync = true;
            }
            if self.pending_tx_record_count == 0 {
                self.pending_tx_buf.clear();
                self.pending_tx_buf.extend_from_slice(&[0u8; 16]);
            }
            WalBatch::append_records_to_buf(&records, &mut self.pending_tx_buf);
            self.pending_tx_record_count += records.len() as u32;
            for rec in records {
                let undo_key = match &rec {
                    WalRecord::PutInline { key, .. }
                    | WalRecord::PutBlobPtr { key, .. }
                    | WalRecord::Delete { key } => Some(key.clone()),
                    WalRecord::TxBegin | WalRecord::TxCommit => None,
                };
                let prev = self.memtable.apply_single_wal_record(lsn, rec);
                if let Some(k) = undo_key {
                    self.tx_memtable_undo.push((k, prev));
                }
            }
            return Ok(lsn);
        }

        let batch = WalBatch::new(lsn, records);
        let encoded = batch.encode();
        self.flush_active_blob_buffers()?;
        self.append_raw_wal_batch_with_rotation(&encoded, durability)?;
        self.next_lsn = self.next_lsn.max(lsn + 1);
        self.memtable.apply_wal_batch_owned(batch);
        if self.memtable.should_flush() {
            self.flush_memtable()?;
        }
        Ok(lsn)
    }

    /// Makes everything appended to `wal_buf.db` so far durable (flush, seal the open sector so it
    /// is never rewritten in place after being synced, then sync) and advances
    /// `last_buf_synced_lsn`. A failure leaves the durability outcome unknown and poisons the engine.
    pub(super) fn make_buf_wal_durable(&mut self, durable: bool) -> io::Result<()> {
        self.buf_wal
            .make_durable_and_seal(durable)
            .map_err(|e| self.poison(e))?;
        self.last_buf_synced_lsn = self.last_buf_append_lsn;
        Ok(())
    }

    pub(super) fn append_raw_wal_batch_with_rotation(
        &mut self,
        encoded: &[u8],
        durability: WriteDurability,
    ) -> io::Result<()> {
        self.check_not_poisoned()?;
        let batch_lsn = if encoded.len() >= 8 {
            u64::from_le_bytes(encoded[0..8].try_into().unwrap())
        } else {
            self.next_lsn
        };

        // Enforce prefix durability across `wal_buf.db`, active blob segments, and `wal_sync.db`:
        // before appending a `SyncTransactional` batch at LSN `L`, ensure all earlier `BufferedFast`
        // batches AND their out-of-line blobs (`last_buf_append_lsn > last_buf_synced_lsn`) are synced!
        if durability == WriteDurability::SyncTransactional
            && self.last_buf_append_lsn > self.last_buf_synced_lsn
        {
            self.sync_active_blob_writers()?;
            self.make_buf_wal_durable(false)?;
        }

        // A failed append/sync leaves the on-disk outcome of this batch unknown (it may or may not
        // be replayed after restart), so the engine is poisoned and must be reopened.
        let r = match durability {
            WriteDurability::SyncTransactional => self.sync_wal.append_raw_batch(encoded),
            WriteDurability::BufferedFast => self.buf_wal.append_raw_batch(encoded),
        };
        let outcome = r.map_err(|e| self.poison(e))?;

        if outcome == WalAppendOutcome::RingFull {
            if self.memtable.is_empty() {
                self.rotate_wal_epochs_only()?;
            } else {
                self.flush_memtable()?;
            }
            let r = match durability {
                WriteDurability::SyncTransactional => self.sync_wal.append_raw_batch(encoded),
                WriteDurability::BufferedFast => self.buf_wal.append_raw_batch(encoded),
            };
            let retry = r.map_err(|e| self.poison(e))?;
            if retry == WalAppendOutcome::RingFull {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "single WAL batch exceeds entire pre-allocated WAL file capacity",
                ));
            }
        }

        if durability == WriteDurability::BufferedFast {
            self.last_buf_append_lsn = self.last_buf_append_lsn.max(batch_lsn);
        }
        Ok(())
    }

    pub(super) fn check_existing_row(&self, key: &[u8]) -> io::Result<ExistingRowStatus> {
        if let Some(mem_entry) = self.memtable.get(key) {
            return Ok(match &mem_entry.value {
                IndexValue::Tombstone => ExistingRowStatus::NotFound,
                IndexValue::Inline(_) => ExistingRowStatus::Inline,
                IndexValue::Blob(ptr) => ExistingRowStatus::Blob(*ptr),
            });
        }
        for seg in self.idx_segments.iter().rev() {
            if let Some((_lsn, val)) = seg.get(key)? {
                return Ok(match val {
                    IndexValue::Tombstone => ExistingRowStatus::NotFound,
                    IndexValue::Inline(_) => ExistingRowStatus::Inline,
                    IndexValue::Blob(ptr) => ExistingRowStatus::Blob(ptr),
                });
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
            .scan_merged_prefix_range(&prefix, &prefix, true, None, 1)?
            .is_empty();
        self.table_has_row_meta.insert(table_id, has);
        Ok(has)
    }

    pub(super) fn get_row_meta_with_ptr(
        &mut self,
        meta_key: &[u8],
    ) -> io::Result<Option<(RowMetaRecord, Option<BlobPtr>)>> {
        let Some((_lsn, val)) = self.lookup_raw_entry(meta_key)? else {
            return Ok(None);
        };
        match val {
            IndexValue::Inline(bytes) => {
                Ok(RowMetaRecord::from_bytes(&bytes).ok().map(|r| (r, None)))
            }
            IndexValue::Blob(ptr) => {
                if let Some(bytes) = self.dereference_blob(&ptr, meta_key)? {
                    Ok(RowMetaRecord::from_bytes(&bytes)
                        .ok()
                        .map(|r| (r, Some(ptr))))
                } else {
                    Ok(None)
                }
            }
            IndexValue::Tombstone => Ok(None),
        }
    }

    pub(super) fn lookup_raw_entry(&self, key: &[u8]) -> io::Result<Option<(u64, IndexValue)>> {
        if let Some(mem_entry) = self.memtable.get(key) {
            return Ok(Some((mem_entry.lsn, mem_entry.value.clone())));
        }
        for seg in self.idx_segments.iter().rev() {
            if let Some((lsn, val)) = seg.get(key)? {
                return Ok(Some((lsn, val)));
            }
        }
        Ok(None)
    }
}
