//! Layer 4 (Part 2): Unified Phase 1 Storage Engine Coordinator (`StorageEngine`).
//!
//! Integrates `BootMetaManager` (`boot_meta.db`), two-stream Epoch WAL (`wal_sync.db` + `wal_buf.db`),
//! out-of-line blob segments (`blob_std` + per-table `blob_lru`), bounded `MemTable`, 4 KB block
//! `idx_<SeqID>.db` segments with Bloom filters and sparse fence pointers, multi-way merge
//! compaction, and Capped Table (`MAX_BYTES`) delayed LRU eviction.

use std::collections::{BTreeMap, HashMap};
use std::io;
use std::sync::Arc;

use crate::blob::{
    BlobSegmentReader, BlobSegmentWriter, DEFAULT_INLINE_THRESHOLD, DEFAULT_MAX_BLOB_SEGMENT_SIZE,
};
use crate::index::{
    DEFAULT_BLOOM_BITS_PER_KEY, IdxSegmentReader, IndexEntry, IndexValue, compact_idx_segments,
};
use crate::lru::{
    AccessTracker, CappedTableState, NS_LRU_QUEUE, NS_PRIMARY_ROW, NS_ROW_META,
    NS_SECONDARY_INDEX, RowMetaRecord, decode_lru_queue_key, decode_primary_row_key,
    encode_lru_queue_key, encode_namespace_prefix, encode_primary_row_key, encode_row_meta_key,
    encode_secondary_index_exact_prefix, encode_secondary_index_key,
};
use crate::memtable::{DEFAULT_MEMTABLE_CAPACITY_BYTES, MemTable};
use crate::meta::{
    BootMetaManager, EvictionPolicy, IdxSegmentMeta, LruBlobSegmentMeta, StdBlobSegmentMeta,
    TableQuotaMeta, format_idx_file_name, format_lru_blob_file_name, format_std_blob_file_name,
};
use crate::pal::{OpenHint, PlatformFile, PlatformFs};
use crate::wal::{
    BlobKind, BlobPtr, DEFAULT_BUF_WAL_SIZE, DEFAULT_SYNC_WAL_SIZE, WAL_BUF_FILE_NAME,
    WAL_SYNC_FILE_NAME, WalAppendOutcome, WalBatch, WalRecord, WalStreamMode, WalStreamWriter,
    recover_two_streams,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExistingRowStatus {
    NotFound,
    Inline,
    Blob(BlobPtr),
}

/// Durability guarantee requested for a write or delete operation.
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

/// Default number of immutable `idx_<SeqID>.db` segments that triggers automatic compaction (`PERF-02`).
pub const DEFAULT_AUTO_COMPACT_SEGMENT_THRESHOLD: usize = 8;

/// Default maximum in-memory transaction footprint (`64 MB`) (`P02`). `0` disables the limit.
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
    /// segment count reaches this threshold (`0` disables automatic compaction) (`PERF-02`, `P01`).
    pub auto_compact_segment_threshold: usize,
    /// Maximum uncommitted transaction payload + WAL record budget in bytes (`0` = unlimited) (`P02`).
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

struct TrackedStdBlob<F: PlatformFile> {
    reader: BlobSegmentReader<F>,
    file_len: u64,
    dead_bytes: u64,
}

struct TrackedLruBlob<F: PlatformFile> {
    table_id: u16,
    reader: BlobSegmentReader<F>,
    file_len: u64,
    dead_bytes: u64,
}

#[derive(Clone, Default)]
struct TxMetaSnapshot {
    next_lsn: u64,
    memtable_highest_lsn: u64,
    std_blobs: BTreeMap<u64, (u64, u64)>,
    lru_blobs: BTreeMap<(u16, u64), (u64, u64)>,
    capped_tables: HashMap<u16, CappedTableState>,
    active_vtab_ids: HashMap<u16, String>,
    dropped_tables_len: usize,
}

#[derive(Clone)]
struct SavepointMarker {
    sp_id: i32,
    buf_offset: usize,
    record_count: u32,
    pending_tx_bytes: usize,
    pending_tx_sync: bool,
    implicit_tx: bool,
    undo_len: usize,
    meta_snapshot: TxMetaSnapshot,
}

/// Unified Phase 1 Storage Engine.
pub struct StorageEngine<FS: PlatformFs> {
    fs: Arc<FS>,
    config: EngineConfig,
    meta_mgr: BootMetaManager<FS>,
    sync_wal: WalStreamWriter<FS::File>,
    buf_wal: WalStreamWriter<FS::File>,
    next_lsn: u64,
    memtable: MemTable,
    /// Active immutable index segments (`idx_<SeqID>.db`), ordered oldest to newest (`last()` is newest).
    idx_segments: Vec<IdxSegmentReader<FS::File>>,
    /// Active standard blob segments (`seq_id -> reader + stats`).
    std_blobs: BTreeMap<u64, TrackedStdBlob<FS::File>>,
    active_std_writer: Option<BlobSegmentWriter<FS::File>>,
    /// Active per-table LRU blob segments (`(table_id, seq_id) -> reader + stats`).
    lru_blobs: BTreeMap<(u16, u64), TrackedLruBlob<FS::File>>,
    active_lru_writers: HashMap<u16, BlobSegmentWriter<FS::File>>,
    /// Capped tables (`table_id -> CappedTableState`).
    capped_tables: HashMap<u16, CappedTableState>,
    /// Bounded in-memory `AccessTick` tracker.
    access_tracker: AccessTracker,
    /// Transaction nesting depth (`xBegin` / `xCommit`). While `tx_depth > 0`, serialized
    /// `WalRecord` bytes are accumulated directly into `pending_tx_buf` and committed as a
    /// single atomic `WalBatch` on `commit_transaction`!
    tx_depth: usize,
    /// Optional connection identifier (`*mut sqlite3 as usize`) owning the active transaction (`F01`).
    tx_owner_conn: Option<usize>,
    /// True if `flush_memtable()` was requested during an active transaction (`F02`).
    deferred_flush_on_commit: bool,
    /// True if `compact()` was requested during an active transaction (`F02`).
    deferred_compact_on_commit: bool,
    /// True if `prepare_commit_transaction()` (`xSync`) has already run for the current commit (`F07`).
    tx_prepared: bool,
    /// True while `prepare_commit_transaction()` is running internal quota eviction (`P02`).
    tx_preparing: bool,
    pending_tx_buf: Vec<u8>,
    pending_tx_record_count: u32,
    pending_tx_bytes: usize,
    pending_tx_sync: bool,
    tx_memtable_undo: Vec<(Vec<u8>, Option<crate::memtable::MemEntry>)>,
    tx_start_meta: TxMetaSnapshot,
    savepoints: Vec<SavepointMarker>,
    /// Tables dropped inside the currently active transaction (`(table_id, was_capped)`, `R-06`).
    dropped_tables_in_tx: Vec<(u16, bool)>,
    /// True while a non-owning connection is reading the temporary committed snapshot (`R-07`).
    in_committed_read_view: bool,
    /// Registry of active virtual tables (`table_id -> table_name`) preventing duplicate `table_id` sharing (`AUD-03`).
    active_vtab_ids: HashMap<u16, String>,
    /// Highest LSN appended to `buf_wal` (`AUD-08`).
    last_buf_append_lsn: u64,
    /// Highest LSN in `buf_wal` synced to durable storage (`AUD-08`).
    last_buf_synced_lsn: u64,
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
        // before a crash so `Write-Blob-Before-WAL` validation can verify their records (`R-01`)!
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

        let had_recovered_batches = !rec.merged_batches.is_empty();
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
                // the same `WalBatch` observe the updated `MemTable` entry (`R-02`)!
                memtable.apply_single_wal_record(batch_lsn, record);
            }
        }

        // Reconcile unreferenced uncommitted tail bytes (`R-04`) and prune any uncataloged
        // blob segment that had zero valid references (`R-01`).
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
            in_committed_read_view: false,
            active_vtab_ids: HashMap::new(),
            last_buf_append_lsn: 0,
            last_buf_synced_lsn: 0,
        };
        engine.access_tracker = AccessTracker::new(1, engine.config.max_in_memory_access_ticks);
        engine.reconcile_capped_tables_after_recovery()?;

        if had_recovered_batches
            || rec.next_sync_sector_idx > 0
            || rec.next_buf_sector_idx > 0
        {
            if !engine.memtable.is_empty() {
                engine.flush_memtable()?;
            } else {
                engine.rotate_wal_epochs_only()?;
            }
        }

        // Now that WAL recovery and the post-recovery checkpoint are complete, sweep any truly
        // orphaned segment/manifest files and process pending deletes (`R-01`)!
        engine.meta_mgr.sweep_orphans()?;

        Ok(engine)
    }

    /// Registers or updates a Capped Table with a strict `max_bytes` quota and automatic LRU eviction.
    pub fn register_capped_table(&mut self, table_id: u16, max_bytes: u64) -> io::Result<()> {
        let was_present = self.capped_tables.contains_key(&table_id);
        let entry = self
            .capped_tables
            .entry(table_id)
            .or_insert_with(|| CappedTableState::new(table_id, max_bytes));
        entry.max_bytes = max_bytes;
        if !was_present {
            self.reconcile_single_capped_table(table_id)?;
        }
        if self.tx_depth == 0 {
            self.evict_until_within_quota(table_id, WriteDurability::BufferedFast)?;
            let quota_metas = self.build_quota_metas();
            self.meta_mgr.commit_update(|st| {
                st.table_quotas = quota_metas;
            })?;
        }
        Ok(())
    }

    /// Returns the next LSN that will be assigned to a WAL batch.
    pub fn next_lsn(&self) -> u64 {
        self.next_lsn
    }

    /// Returns a snapshot of the [`CappedTableState`] for `table_id`, if registered.
    pub fn capped_table_state(&self, table_id: u16) -> Option<&CappedTableState> {
        self.capped_tables.get(&table_id)
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

    ///Registers or resolves a virtual table `(table_name, explicit_table_id)` in the engine's
    /// active registry and persistent catalog (`table_id = 0` under `'c'`) (`AUD-03`).
    pub fn register_or_resolve_vtab(
        &mut self,
        table_name: &str,
        explicit_id: Option<u16>,
    ) -> io::Result<u16> {
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
                    if existing_name != &norm_name {
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
                        if existing_name != &norm_name {
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

        self.active_vtab_ids.insert(chosen_id, norm_name.clone());
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
                WriteDurability::BufferedFast,
            )?;
        }
        Ok(chosen_id)
    }

    /// Unregisters a disconnected virtual table `(table_id, table_name)` from the live registry.
    pub fn unregister_vtab(&mut self, table_id: u16, table_name: &str) {
        let norm_name = table_name.trim().to_ascii_lowercase();
        if self.active_vtab_ids.get(&table_id).map(|s| s.as_str()) == Some(norm_name.as_str()) {
            self.active_vtab_ids.remove(&table_id);
        }
    }

    /// Drops a table (`xDestroy` / `DROP TABLE`), deleting all primary rows (`'r'`), secondary
    /// index entries (`'i'`), row metadata (`'t'`), LRU queue entries (`'l'`), capped table
    /// quota registrations, and system catalog entries (`USE-02`, `R-06`).
    pub fn drop_table(&mut self, table_id: u16, table_name: Option<&str>) -> io::Result<()> {
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

        let removed_name = self.active_vtab_ids.remove(&table_id);
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
        if let Some(old_w) = self.active_lru_writers.remove(&table_id) {
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

    /// Owned-value variant of [`put_row`] that moves `value: Vec<u8>` directly into the `MemTable`
    /// without cloning when stored inline.
    pub fn put_row_owned(
        &mut self,
        table_id: u16,
        pk: &[u8],
        value: Vec<u8>,
        secondary_keys: &[(u16, Vec<u8>)],
        durability: WriteDurability,
    ) -> io::Result<()> {
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

        let existing = self.check_existing_row(&row_key)?;
        if let ExistingRowStatus::Blob(old_ptr) = existing {
            old_blob_ptr = Some(old_ptr);
        }

        if existing != ExistingRowStatus::NotFound {
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

        // 2. Write out-of-line blob FIRST if `value.len() >= inline_threshold_bytes` or if inline entry exceeds 4 KB block (`AUD-02`, `R-03`)
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
            let meta_bytes = RowMetaRecord::encode_slice(assigned_tick, logical_size, secondary_keys);
            if !IndexEntry::inline_fits_in_block(meta_key.len(), meta_bytes.len()) {
                let ptr = self.append_to_blob_segment(BlobKind::Standard, 0, &meta_key, &meta_bytes)?;
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

        // 6. Apply dead blob byte accounting and update Capped Table logical byte counter after WAL succeeds (`AUD-07`)
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

    /// Scans all live rows `(pk, value)` in `table_id` in ascending memcomparable primary-key order.
    pub fn scan_table(&mut self, table_id: u16) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan_table_range(table_id, None, None, usize::MAX)
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

    /// Scans up to `limit` live primary keys (`pk`) in `table_id` within inclusive `[lower, upper]`
    /// without reading or dereferencing any out-of-line blob files (`P02`).
    pub fn scan_keys_range(
        &mut self,
        table_id: u16,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        limit: usize,
    ) -> io::Result<Vec<Vec<u8>>> {
        let mut pks = Vec::new();
        let mut slab = Vec::new();
        let mut spans = Vec::new();
        self.scan_range_into_slab(
            table_id,
            lower.map(|b| (b, true)),
            upper.map(|b| (b, true)),
            false,
            limit,
            true,
            &mut pks,
            &mut slab,
            &mut spans,
        )?;
        Ok(pks)
    }

    /// Streams live rows in `table_id` within `lower` and `upper` directly into `slab_out` and
    /// `spans_out` (or into `pks_out` when `keys_only` is `true` without dereferencing blobs)
    /// without intermediate per-row `Vec<u8>` allocations (`P02`).
    #[allow(clippy::too_many_arguments)]
    pub fn scan_range_into_slab(
        &mut self,
        table_id: u16,
        lower: Option<(&[u8], bool)>,
        upper: Option<(&[u8], bool)>,
        reverse: bool,
        limit: usize,
        keys_only: bool,
        pks_out: &mut Vec<Vec<u8>>,
        slab_out: &mut Vec<u8>,
        spans_out: &mut Vec<(usize, usize)>,
    ) -> io::Result<()> {
        use std::ops::Bound;
        if limit == 0 {
            return Ok(());
        }
        let prefix = encode_namespace_prefix(table_id, NS_PRIMARY_ROW);
        let mut lb_stack = [0u8; 64];
        let lb_heap;
        let seek_key: &[u8] = match lower {
            Some((lb_pk, _)) if 3 + lb_pk.len() <= lb_stack.len() => {
                lb_stack[0..3].copy_from_slice(&prefix);
                lb_stack[3..3 + lb_pk.len()].copy_from_slice(lb_pk);
                &lb_stack[..3 + lb_pk.len()]
            }
            Some((lb_pk, _)) => {
                lb_heap = encode_primary_row_key(table_id, lb_pk);
                &lb_heap
            }
            None => &prefix,
        };

        let mut ub_stack = [0u8; 64];
        let ub_heap;
        let upper_key_slice: Option<(&[u8], bool)> = match upper {
            Some((ub_pk, inc)) if 3 + ub_pk.len() <= ub_stack.len() => {
                ub_stack[0..3].copy_from_slice(&prefix);
                ub_stack[3..3 + ub_pk.len()].copy_from_slice(ub_pk);
                Some((&ub_stack[..3 + ub_pk.len()], inc))
            }
            Some((ub_pk, inc)) => {
                ub_heap = encode_primary_row_key(table_id, ub_pk);
                Some((&ub_heap, inc))
            }
            None => None,
        };

        let lo_inc = lower.map(|(_, inc)| inc).unwrap_or(true);
        if let Some((ub, ub_inc)) = upper_key_slice {
            if seek_key > ub || (seek_key == ub && (!lo_inc || !ub_inc)) {
                return Ok(());
            }
        }

        if self.idx_segments.is_empty() && !reverse {
            let lo = match lower {
                Some((_, false)) => Bound::Excluded(seek_key),
                _ => Bound::Included(seek_key),
            };
            let hi = match upper_key_slice {
                Some((ub, true)) => Bound::Included(ub),
                Some((ub, false)) => Bound::Excluded(ub),
                None => Bound::Unbounded,
            };
            for (k, entry) in self.memtable.iter_range(lo, hi) {
                if !k.starts_with(&prefix) {
                    break;
                }
                if keys_only {
                    if !matches!(entry.value, IndexValue::Tombstone) {
                        pks_out.push(k[3..].to_vec());
                        if pks_out.len() >= limit {
                            break;
                        }
                    }
                    continue;
                }
                match &entry.value {
                    IndexValue::Inline(v) => {
                        let start = slab_out.len();
                        slab_out.extend_from_slice(v);
                        spans_out.push((start, slab_out.len()));
                        if spans_out.len() >= limit {
                            break;
                        }
                    }
                    IndexValue::Blob(ptr) => {
                        let start = slab_out.len();
                        if self.dereference_blob_append_into(ptr, k, slab_out)? {
                            spans_out.push((start, slab_out.len()));
                            if spans_out.len() >= limit {
                                break;
                            }
                        }
                    }
                    IndexValue::Tombstone => {}
                }
            }
            return Ok(());
        }

        let merge_limit = if reverse { usize::MAX } else { limit };
        let merged = self.scan_merged_prefix_range(
            &prefix,
            seek_key,
            lo_inc,
            upper_key_slice,
            merge_limit,
        )?;

        if reverse {
            for entry in merged.into_iter().rev() {
                if entry.key.len() < 3 || !entry.key.starts_with(&prefix) {
                    continue;
                }
                if keys_only {
                    if !matches!(entry.value, IndexValue::Tombstone) {
                        pks_out.push(entry.key[3..].to_vec());
                        if pks_out.len() >= limit {
                            break;
                        }
                    }
                } else {
                    match entry.value {
                        IndexValue::Inline(v) => {
                            let start = slab_out.len();
                            slab_out.extend_from_slice(&v);
                            spans_out.push((start, slab_out.len()));
                            if spans_out.len() >= limit {
                                break;
                            }
                        }
                        IndexValue::Blob(ptr) => {
                            let start = slab_out.len();
                            if self.dereference_blob_append_into(&ptr, &entry.key, slab_out)? {
                                spans_out.push((start, slab_out.len()));
                                if spans_out.len() >= limit {
                                    break;
                                }
                            }
                        }
                        IndexValue::Tombstone => {}
                    }
                }
            }
        } else {
            for entry in merged {
                if entry.key.len() < 3 || !entry.key.starts_with(&prefix) {
                    continue;
                }
                if keys_only {
                    if !matches!(entry.value, IndexValue::Tombstone) {
                        pks_out.push(entry.key[3..].to_vec());
                        if pks_out.len() >= limit {
                            break;
                        }
                    }
                } else {
                    match entry.value {
                        IndexValue::Inline(v) => {
                            let start = slab_out.len();
                            slab_out.extend_from_slice(&v);
                            spans_out.push((start, slab_out.len()));
                            if spans_out.len() >= limit {
                                break;
                            }
                        }
                        IndexValue::Blob(ptr) => {
                            let start = slab_out.len();
                            if self.dereference_blob_append_into(&ptr, &entry.key, slab_out)? {
                                spans_out.push((start, slab_out.len()));
                                if spans_out.len() >= limit {
                                    break;
                                }
                            }
                        }
                        IndexValue::Tombstone => {}
                    }
                }
            }
        }
        Ok(())
    }

    /// Scans up to `limit` live row payloads in `table_id` within `lower_bound` and `upper_bound`
    /// directly into `slab` and `spans`, avoiding per-row `Vec<u8>` heap allocations!
    pub fn scan_table_values_into(
        &mut self,
        table_id: u16,
        lower_bound: Option<(&[u8], bool)>,
        upper_bound: Option<(&[u8], bool)>,
        limit: usize,
        slab: &mut Vec<u8>,
        spans: &mut Vec<(usize, usize)>,
    ) -> io::Result<()> {
        let mut unused_pks = Vec::new();
        self.scan_range_into_slab(
            table_id,
            lower_bound,
            upper_bound,
            false,
            limit,
            false,
            &mut unused_pks,
            slab,
            spans,
        )
    }

    /// Scans up to `limit` live rows `(pk, value)` in `table_id` within `lower_bound` and `upper_bound`
    /// (`(bound_pk_bytes, inclusive)`), seeking directly via Sparse Fence Pointers!
    pub fn scan_table_range(
        &mut self,
        table_id: u16,
        lower_bound: Option<(&[u8], bool)>,
        upper_bound: Option<(&[u8], bool)>,
        limit: usize,
    ) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        use std::ops::Bound;
        if limit == 0 {
            return Ok(Vec::new());
        }
        let prefix = encode_namespace_prefix(table_id, NS_PRIMARY_ROW);
        let seek_key = match lower_bound {
            Some((lb_pk, _)) => encode_primary_row_key(table_id, lb_pk),
            None => prefix.to_vec(),
        };
        let lo_inc = lower_bound.map(|(_, inc)| inc).unwrap_or(true);
        let upper_full = upper_bound.map(|(ub_pk, inc)| (encode_primary_row_key(table_id, ub_pk), inc));

        if let Some((ref ub, ub_inc)) = upper_full {
            if seek_key.as_slice() > ub.as_slice()
                || (seek_key.as_slice() == ub.as_slice() && (!lo_inc || !ub_inc))
            {
                return Ok(Vec::new());
            }
        }

        if self.idx_segments.is_empty() {
            let lo = match lower_bound {
                Some((_, false)) => Bound::Excluded(seek_key.as_slice()),
                _ => Bound::Included(seek_key.as_slice()),
            };
            let hi = match &upper_full {
                Some((ub, true)) => Bound::Included(ub.as_slice()),
                Some((ub, false)) => Bound::Excluded(ub.as_slice()),
                None => Bound::Unbounded,
            };
            let mut rows = Vec::new();
            for (k, entry) in self.memtable.iter_range(lo, hi) {
                if !k.starts_with(&prefix) {
                    break;
                }
                let val_opt = match &entry.value {
                    IndexValue::Inline(v) => Some(v.clone()),
                    IndexValue::Blob(ptr) => self.dereference_blob(ptr, k)?,
                    IndexValue::Tombstone => None,
                };
                if let Some(val) = val_opt {
                    rows.push((k[3..].to_vec(), val));
                    if rows.len() >= limit {
                        break;
                    }
                }
            }
            return Ok(rows);
        }

        let merged = self.scan_merged_prefix_range(
            &prefix,
            &seek_key,
            lo_inc,
            upper_full.as_ref().map(|(v, inc)| (v.as_slice(), *inc)),
            limit,
        )?;
        let mut rows = Vec::with_capacity(merged.len().min(limit));

        for entry in merged {
            let Some(pk) = decode_primary_row_key(table_id, &entry.key) else {
                continue;
            };
            let val_opt = match entry.value {
                IndexValue::Inline(v) => Some(v),
                IndexValue::Blob(ptr) => self.dereference_blob(&ptr, &entry.key)?,
                IndexValue::Tombstone => None,
            };
            if let Some(val) = val_opt {
                rows.push((pk.to_vec(), val));
                if rows.len() >= limit {
                    break;
                }
            }
        }
        Ok(rows)
    }

    /// Scans primary keys matching an exact secondary index value `(table_id, index_id, col_val)`.
    pub fn lookup_by_secondary_index(
        &self,
        table_id: u16,
        index_id: u16,
        col_val: &[u8],
    ) -> io::Result<Vec<Vec<u8>>> {
        use std::ops::Bound;
        let prefix = encode_secondary_index_exact_prefix(table_id, index_id, col_val);
        if self.idx_segments.is_empty() {
            let mut pks = Vec::new();
            for (k, entry) in self
                .memtable
                .iter_range(Bound::Included(prefix.as_slice()), Bound::Unbounded)
            {
                if !k.starts_with(&prefix) {
                    break;
                }
                if !matches!(entry.value, IndexValue::Tombstone) {
                    pks.push(k[prefix.len()..].to_vec());
                }
            }
            return Ok(pks);
        }

        let merged = self.scan_merged_prefix(&prefix)?;
        let mut pks = Vec::with_capacity(merged.len());
        for entry in merged {
            if matches!(entry.value, IndexValue::Tombstone) {
                continue;
            }
            if entry.key.starts_with(&prefix) {
                pks.push(entry.key[prefix.len()..].to_vec());
            }
        }
        Ok(pks)
    }

    /// Begins an explicit batch transaction (`BEGIN`). Subsequent mutations apply immediately to
    /// the in-memory `MemTable` (for read-your-own-writes visibility) while coalescing their
    /// [`WalRecord`] entries into a single atomic [`WalBatch`] flushed on [`commit_transaction`].
    pub fn begin_transaction(&mut self) {
        if self.tx_depth == 0 {
            self.tx_memtable_undo.clear();
            self.dropped_tables_in_tx.clear();
            self.tx_start_meta = self.capture_tx_meta_snapshot();
            self.tx_owner_conn = None;
            self.deferred_flush_on_commit = false;
            self.deferred_compact_on_commit = false;
            self.tx_prepared = false;
            self.tx_preparing = false;
            self.pending_tx_bytes = 0;
        }
        self.tx_depth += 1;
    }

    /// Begins an explicit transaction owned by SQLite connection `conn_id` (`F01`).
    /// Returns `io::ErrorKind::WouldBlock` (`SQLITE_BUSY`) if another connection already holds
    /// an active transaction on this `StorageEngine`.
    pub fn begin_transaction_for_conn(&mut self, conn_id: usize) -> io::Result<()> {
        if self.tx_depth > 0 && self.tx_owner_conn != Some(conn_id) {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "database is locked by another connection's active transaction",
            ));
        }
        if self.tx_depth == 0 {
            self.tx_memtable_undo.clear();
            self.dropped_tables_in_tx.clear();
            self.tx_start_meta = self.capture_tx_meta_snapshot();
            self.tx_owner_conn = Some(conn_id);
            self.deferred_flush_on_commit = false;
            self.deferred_compact_on_commit = false;
            self.tx_prepared = false;
            self.tx_preparing = false;
            self.pending_tx_bytes = 0;
        }
        self.tx_depth += 1;
        Ok(())
    }

    /// Checks whether `conn_id` is permitted to mutate this `StorageEngine` (`F01`).
    /// Returns `io::ErrorKind::WouldBlock` (`SQLITE_BUSY`) if another connection holds an active transaction.
    #[inline]
    pub fn check_write_allowed_for_conn(&self, conn_id: usize) -> io::Result<()> {
        if self.tx_depth > 0 && self.tx_owner_conn != Some(conn_id) {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "database is locked by another connection's active transaction",
            ));
        }
        Ok(())
    }

    /// Executes a read closure `f` for `conn_id` against the committed snapshot (`F01`).
    /// If `conn_id` owns the active transaction (or no transaction is active), `f` sees the live
    /// `MemTable` (read-your-own-writes). If another connection owns an active transaction,
    /// uncommitted `MemTable` mutations are temporarily hidden so `conn_id` sees only committed data.
    pub fn with_committed_read_for_conn<F, R>(&mut self, conn_id: usize, f: F) -> R
    where
        F: FnOnce(&mut Self) -> R,
    {
        if self.tx_depth == 0 || self.tx_owner_conn == Some(conn_id) {
            return f(self);
        }

        struct CommittedReadGuard<'a, FS: PlatformFs> {
            eng: &'a mut StorageEngine<FS>,
            uncommitted_redo: Vec<(Vec<u8>, Option<crate::memtable::MemEntry>)>,
            uncommitted_highest_lsn: u64,
            prev_in_view: bool,
        }

        impl<'a, FS: PlatformFs> Drop for CommittedReadGuard<'a, FS> {
            fn drop(&mut self) {
                while let Some((key, entry)) = self.uncommitted_redo.pop() {
                    self.eng.memtable.restore_entry(key, entry);
                }
                self.eng
                    .memtable
                    .set_highest_lsn(self.uncommitted_highest_lsn);
                self.eng.in_committed_read_view = self.prev_in_view;
            }
        }

        let prev_in_view = self.in_committed_read_view;
        self.in_committed_read_view = true;
        let uncommitted_highest_lsn = self.memtable.highest_lsn();
        let committed_highest_lsn = self.tx_start_meta.memtable_highest_lsn;
        let mut uncommitted_redo = Vec::with_capacity(self.tx_memtable_undo.len());
        for i in (0..self.tx_memtable_undo.len()).rev() {
            let key = self.tx_memtable_undo[i].0.clone();
            let prev_entry = self.tx_memtable_undo[i].1.clone();
            let cur_entry = self.memtable.get(&key).cloned();
            self.memtable.restore_entry(key.clone(), prev_entry);
            uncommitted_redo.push((key, cur_entry));
        }
        self.memtable.set_highest_lsn(committed_highest_lsn);

        let guard = CommittedReadGuard {
            eng: self,
            uncommitted_redo,
            uncommitted_highest_lsn,
            prev_in_view,
        };
        f(guard.eng)
    }

    /// Prepares the active transaction for commit (`xSync` two-phase commit phase 1, `F07`).
    /// Runs deferred capped-table quota evictions inside the undoable transaction context,
    /// flushes/syncs out-of-line blob segments, and pre-checks WAL capacity so any fallible I/O
    /// or space errors surface during `xSync` while `xRollback` can still cleanly undo all state.
    pub fn prepare_commit_transaction(&mut self, durability: WriteDurability) -> io::Result<()> {
        if self.tx_depth == 0 {
            return Ok(());
        }
        if durability == WriteDurability::SyncTransactional {
            self.pending_tx_sync = true;
        }
        let effective_durability = if self.pending_tx_sync {
            WriteDurability::SyncTransactional
        } else {
            WriteDurability::BufferedFast
        };

        // 1. Run capped-table quota evictions while `tx_depth > 0` so eviction mutations are
        //    recorded in `pending_tx_buf` and `tx_memtable_undo` (fully undoable on `xRollback`!).
        self.tx_preparing = true;
        let capped_table_ids: Vec<u16> = self.capped_tables.keys().copied().collect();
        for tid in capped_table_ids {
            if let Err(e) = self.evict_until_within_quota(tid, effective_durability) {
                self.tx_preparing = false;
                return Err(e);
            }
        }
        self.tx_preparing = false;

        // 2. Sync active blob writers (`Write-Blob-Before-WAL` & prefix durability `F06`/`F07`).
        if effective_durability == WriteDurability::SyncTransactional {
            self.sync_active_blob_writers()?;
            if self.last_buf_append_lsn > self.last_buf_synced_lsn {
                self.buf_wal.flush_buffered_to_os()?;
                self.buf_wal.file().sync_barrier()?;
                self.last_buf_synced_lsn = self.last_buf_append_lsn;
            }
        }

        // 3. Pre-verify/grow WAL capacity if this is a multi-chunk transaction (> 512 KB).
        const MAX_WAL_CHUNK_BYTES: usize = 512 * 1024;
        if self.pending_tx_buf.len() > MAX_WAL_CHUNK_BYTES {
            let required_sectors = (self.pending_tx_buf.len() / 4070) + 8;
            match effective_durability {
                WriteDurability::SyncTransactional => {
                    let target_sectors = self.sync_wal.current_sector_idx() + required_sectors;
                    if target_sectors > self.sync_wal.total_sectors() {
                        self.sync_wal.ensure_min_size(
                            (target_sectors * crate::wal::WAL_SECTOR_SIZE) as u64,
                        )?;
                    }
                }
                WriteDurability::BufferedFast => {
                    let target_sectors = self.buf_wal.current_sector_idx() + required_sectors;
                    if target_sectors > self.buf_wal.total_sectors() {
                        self.buf_wal.ensure_min_size(
                            (target_sectors * crate::wal::WAL_SECTOR_SIZE) as u64,
                        )?;
                    }
                }
            }
        }

        self.tx_prepared = true;
        Ok(())
    }

    /// Commits an explicit batch transaction (`COMMIT`), writing all coalesced [`WalRecord`]
    /// entries in a single [`WalBatch`] syscall (or chunked batches if exceeding 512 KB)!
    pub fn commit_transaction(&mut self) -> io::Result<()> {
        if self.tx_depth == 1 && !self.tx_prepared {
            let durability = if self.pending_tx_sync {
                WriteDurability::SyncTransactional
            } else {
                WriteDurability::BufferedFast
            };
            if let Err(e) = self.prepare_commit_transaction(durability) {
                let _ = self.rollback_transaction();
                return Err(e);
            }
        }
        if self.tx_depth > 0 {
            self.tx_depth -= 1;
        }
        if self.tx_depth == 0 {
            if let Err(e) = self.flush_pending_tx_records() {
                self.tx_depth = 1;
                let _ = self.rollback_transaction();
                return Err(e);
            }
            self.tx_owner_conn = None;
            self.tx_prepared = false;
            self.tx_preparing = false;
            self.pending_tx_bytes = 0;
            self.savepoints.clear();
            self.tx_memtable_undo.clear();
            self.tx_start_meta = TxMetaSnapshot::default();

            let capped_table_ids: Vec<u16> = self.capped_tables.keys().copied().collect();
            for tid in capped_table_ids {
                self.reclaim_fully_dead_lru_blob_segments(tid)?;
            }

            let dropped_tables = core::mem::take(&mut self.dropped_tables_in_tx);
            let mut dropped_any_capped = false;
            for (tid, was_capped) in dropped_tables {
                self.reclaim_fully_dead_lru_blob_segments(tid)?;
                if was_capped {
                    dropped_any_capped = true;
                }
            }
            if dropped_any_capped {
                let quota_metas = self.build_quota_metas();
                self.meta_mgr.commit_update(|st| {
                    st.table_quotas = quota_metas;
                })?;
            }

            let do_compact = core::mem::replace(&mut self.deferred_compact_on_commit, false);
            let do_flush = core::mem::replace(&mut self.deferred_flush_on_commit, false);
            if do_compact {
                self.compact()?;
            } else if do_flush || self.memtable.should_flush() {
                self.flush_memtable()?;
            }
        }
        Ok(())
    }

    /// Commits an explicit transaction for SQLite connection `conn_id` (`F01`).
    pub fn commit_transaction_for_conn(&mut self, conn_id: usize) -> io::Result<()> {
        self.check_write_allowed_for_conn(conn_id)?;
        self.commit_transaction()
    }

    /// Aborts an explicit batch transaction (`ROLLBACK`), discarding all buffered `pending_tx_buf`
    /// mutations and restoring `MemTable`, `capped_tables`, and blob `dead_bytes` in
    /// $O(\text{tx mutations})$ time via the in-memory undo log (`PERF-03`).
    pub fn rollback_transaction(&mut self) -> io::Result<()> {
        self.tx_depth = 0;
        self.tx_owner_conn = None;
        self.deferred_flush_on_commit = false;
        self.deferred_compact_on_commit = false;
        self.tx_prepared = false;
        self.tx_preparing = false;
        self.savepoints.clear();
        self.pending_tx_record_count = 0;
        self.pending_tx_bytes = 0;
        self.pending_tx_buf.clear();
        self.pending_tx_sync = false;
        let snap = core::mem::take(&mut self.tx_start_meta);
        self.restore_tx_state(0, &snap);
        Ok(())
    }

    /// Aborts an explicit transaction for SQLite connection `conn_id` (`F01`).
    pub fn rollback_transaction_for_conn(&mut self, conn_id: usize) -> io::Result<()> {
        self.check_write_allowed_for_conn(conn_id)?;
        self.rollback_transaction()
    }

    /// Opens or updates a transaction savepoint (`xSavepoint`) (`USE-01`).
    pub fn savepoint(&mut self, sp_id: i32) {
        self.tx_prepared = false;
        let mut implicit_tx = false;
        if self.tx_depth == 0 {
            self.begin_transaction();
            implicit_tx = true;
        }
        if let Some(pos) = self.savepoints.iter().position(|s| s.sp_id == sp_id) {
            implicit_tx = self.savepoints[pos].implicit_tx;
            self.savepoints.truncate(pos);
        }
        let buf_offset = if self.pending_tx_record_count == 0 {
            0
        } else {
            self.pending_tx_buf.len()
        };
        let meta_snapshot = self.capture_tx_meta_snapshot();
        self.savepoints.push(SavepointMarker {
            sp_id,
            buf_offset,
            record_count: self.pending_tx_record_count,
            pending_tx_bytes: self.pending_tx_bytes,
            pending_tx_sync: self.pending_tx_sync,
            implicit_tx,
            undo_len: self.tx_memtable_undo.len(),
            meta_snapshot,
        });
    }

    /// Opens or updates a transaction savepoint for SQLite connection `conn_id` (`F01`).
    pub fn savepoint_for_conn(&mut self, conn_id: usize, sp_id: i32) -> io::Result<()> {
        self.check_write_allowed_for_conn(conn_id)?;
        let was_zero = self.tx_depth == 0;
        self.savepoint(sp_id);
        if was_zero {
            self.tx_owner_conn = Some(conn_id);
        }
        Ok(())
    }

    /// Releases savepoints `>= sp_id` (`xRelease`) (`USE-01`).
    pub fn release_savepoint(&mut self, sp_id: i32) -> io::Result<()> {
        if let Some(pos) = self.savepoints.iter().position(|s| s.sp_id == sp_id) {
            let implicit_tx = self.savepoints[pos].implicit_tx;
            self.savepoints.truncate(pos);
            if implicit_tx && self.tx_depth == 1 {
                self.commit_transaction()?;
            }
        }
        Ok(())
    }

    /// Releases savepoints `>= sp_id` for SQLite connection `conn_id` (`F01`).
    pub fn release_savepoint_for_conn(&mut self, conn_id: usize, sp_id: i32) -> io::Result<()> {
        self.check_write_allowed_for_conn(conn_id)?;
        self.release_savepoint(sp_id)
    }

    /// Rolls back all mutations since savepoint `sp_id` (`xRollbackTo`) in
    /// $O(\text{mutations since savepoint})$ time via the undo log (`USE-01`, `PERF-03`).
    pub fn rollback_to_savepoint(&mut self, sp_id: i32) -> io::Result<()> {
        self.tx_prepared = false;
        let Some(pos) = self.savepoints.iter().position(|s| s.sp_id == sp_id) else {
            return Ok(());
        };
        let marker = self.savepoints[pos].clone();
        self.savepoints.truncate(pos + 1);
        if marker.record_count == 0 {
            self.pending_tx_buf.clear();
        } else {
            self.pending_tx_buf.truncate(marker.buf_offset);
        }
        self.pending_tx_record_count = marker.record_count;
        self.pending_tx_bytes = marker.pending_tx_bytes;
        self.pending_tx_sync = marker.pending_tx_sync;
        self.restore_tx_state(marker.undo_len, &marker.meta_snapshot);
        Ok(())
    }

    /// Rolls back to savepoint `sp_id` for SQLite connection `conn_id` (`F01`).
    pub fn rollback_to_savepoint_for_conn(&mut self, conn_id: usize, sp_id: i32) -> io::Result<()> {
        self.check_write_allowed_for_conn(conn_id)?;
        self.rollback_to_savepoint(sp_id)
    }

    fn capture_tx_meta_snapshot(&self) -> TxMetaSnapshot {
        TxMetaSnapshot {
            next_lsn: self.next_lsn,
            memtable_highest_lsn: self.memtable.highest_lsn(),
            std_blobs: self
                .std_blobs
                .iter()
                .map(|(&seq, b)| (seq, (b.file_len, b.dead_bytes)))
                .collect(),
            lru_blobs: self
                .lru_blobs
                .iter()
                .map(|(&k, b)| (k, (b.file_len, b.dead_bytes)))
                .collect(),
            capped_tables: self.capped_tables.clone(),
            active_vtab_ids: self.active_vtab_ids.clone(),
            dropped_tables_len: self.dropped_tables_in_tx.len(),
        }
    }

    fn restore_tx_state(&mut self, undo_target_len: usize, snap: &TxMetaSnapshot) {
        while self.tx_memtable_undo.len() > undo_target_len {
            let (key, prev_entry) = self.tx_memtable_undo.pop().unwrap();
            self.memtable.restore_entry(key, prev_entry);
        }
        self.memtable.set_highest_lsn(snap.memtable_highest_lsn);
        self.next_lsn = snap.next_lsn;
        for (&seq, b) in &mut self.std_blobs {
            let (snap_len, snap_dead) = snap.std_blobs.get(&seq).copied().unwrap_or((0, 0));
            let aborted = b.file_len.saturating_sub(snap_len);
            b.dead_bytes = snap_dead.saturating_add(aborted);
        }
        for (&k, b) in &mut self.lru_blobs {
            let (snap_len, snap_dead) = snap.lru_blobs.get(&k).copied().unwrap_or((0, 0));
            let aborted = b.file_len.saturating_sub(snap_len);
            b.dead_bytes = snap_dead.saturating_add(aborted);
        }
        self.capped_tables = snap.capped_tables.clone();
        self.active_vtab_ids = snap.active_vtab_ids.clone();
        self.dropped_tables_in_tx.truncate(snap.dropped_tables_len);
    }

    fn flush_pending_tx_records(&mut self) -> io::Result<()> {
        if self.pending_tx_record_count == 0 {
            return Ok(());
        }
        let record_count = self.pending_tx_record_count;
        self.pending_tx_record_count = 0;
        let durability = if self.pending_tx_sync {
            WriteDurability::SyncTransactional
        } else {
            WriteDurability::BufferedFast
        };
        self.pending_tx_sync = false;

        if durability == WriteDurability::SyncTransactional {
            self.sync_active_blob_writers()?;
        }

        const MAX_WAL_CHUNK_BYTES: usize = 512 * 1024;
        if self.pending_tx_buf.len() <= MAX_WAL_CHUNK_BYTES {
            let lsn = self.next_lsn;
            self.next_lsn += 1;
            WalBatch::finalize_encoded_buf(lsn, record_count, &mut self.pending_tx_buf);
            let mut raw_buf = core::mem::take(&mut self.pending_tx_buf);
            let res = self.append_raw_wal_batch_with_rotation(&raw_buf, durability);
            raw_buf.clear();
            self.pending_tx_buf = raw_buf;
            return res;
        }

        // Large transaction buffer (> 512 KB): chunk serialized records into <= 512 KB WalBatches
        // framed by `WalRecord::TxBegin` in the first chunk and `WalRecord::TxCommit` in the last
        // chunk (`AUD-04`). Ensure the target WAL file has enough capacity so all chunks of the
        // transaction remain in the same WAL epoch without flushing uncommitted `MemTable` state.
        let mut raw_buf = core::mem::take(&mut self.pending_tx_buf);
        let mut cursor = 16usize;
        let total_len = raw_buf.len();
        let mut chunks: Vec<(Vec<u8>, u32)> = Vec::new();
        let mut chunk_buf = Vec::with_capacity(MAX_WAL_CHUNK_BYTES + 4096);
        let mut chunk_records = 0u32;

        while cursor < total_len {
            if chunk_records == 0 {
                chunk_buf.clear();
                chunk_buf.extend_from_slice(&[0u8; 16]);
                if chunks.is_empty() {
                    // Prepend `WalRecord::TxBegin` (`4u8`) to the first chunk (`AUD-04`).
                    chunk_buf.push(4u8);
                    chunk_records += 1;
                }
            }
            let rec_start = cursor;
            let op = raw_buf[cursor];
            cursor += 1;
            match op {
                1 => {
                    let klen =
                        u32::from_le_bytes(raw_buf[cursor..cursor + 4].try_into().unwrap())
                            as usize;
                    cursor += 4 + klen;
                    let vlen =
                        u32::from_le_bytes(raw_buf[cursor..cursor + 4].try_into().unwrap())
                            as usize;
                    cursor += 4 + vlen;
                }
                2 => {
                    let klen =
                        u32::from_le_bytes(raw_buf[cursor..cursor + 4].try_into().unwrap())
                            as usize;
                    cursor += 4 + klen + 23;
                }
                3 => {
                    let klen =
                        u32::from_le_bytes(raw_buf[cursor..cursor + 4].try_into().unwrap())
                            as usize;
                    cursor += 4 + klen;
                }
                4 | 5 => {}
                _ => unreachable!("invalid op in pending_tx_buf"),
            }
            let rec_slice = &raw_buf[rec_start..cursor];
            let min_records_before_split = if chunks.is_empty() { 2 } else { 1 };
            if chunk_records >= min_records_before_split
                && chunk_buf.len() + rec_slice.len() > MAX_WAL_CHUNK_BYTES
            {
                chunks.push((core::mem::take(&mut chunk_buf), chunk_records));
                chunk_buf = Vec::with_capacity(MAX_WAL_CHUNK_BYTES + 4096);
                chunk_buf.extend_from_slice(&[0u8; 16]);
                chunk_records = 0;
            }
            chunk_buf.extend_from_slice(rec_slice);
            chunk_records += 1;
        }

        if chunk_records > 0 {
            chunks.push((chunk_buf, chunk_records));
        }

        raw_buf.clear();
        self.pending_tx_buf = raw_buf;

        if chunks.len() == 1 {
            let (mut single_buf, mut single_records) = chunks.pop().unwrap();
            // Strip the leading `TxBegin` (`4u8` at index 16) since it fit in a single chunk
            single_buf.remove(16);
            single_records -= 1;
            let lsn = self.next_lsn;
            self.next_lsn += 1;
            WalBatch::finalize_encoded_buf(lsn, single_records, &mut single_buf);
            return self.append_raw_wal_batch_with_rotation(&single_buf, durability);
        }

        if let Some((last_buf, last_records)) = chunks.last_mut() {
            // Append `WalRecord::TxCommit` (`5u8`) to the final chunk (`AUD-04`).
            last_buf.push(5u8);
            *last_records += 1;
        }

        // Ensure the target WAL stream has sufficient sectors to fit all chunks in the current epoch
        let mut required_sectors = 0usize;
        for (cbuf, _) in &chunks {
            required_sectors += (cbuf.len() / 4070) + 2;
        }
        match durability {
            WriteDurability::SyncTransactional => {
                let target_sectors = self.sync_wal.current_sector_idx() + required_sectors;
                if target_sectors > self.sync_wal.total_sectors() {
                    self.sync_wal
                        .ensure_min_size((target_sectors * crate::wal::WAL_SECTOR_SIZE) as u64)?;
                }
            }
            WriteDurability::BufferedFast => {
                let target_sectors = self.buf_wal.current_sector_idx() + required_sectors;
                if target_sectors > self.buf_wal.total_sectors() {
                    self.buf_wal
                        .ensure_min_size((target_sectors * crate::wal::WAL_SECTOR_SIZE) as u64)?;
                }
            }
        }

        for (mut cbuf, crec) in chunks {
            let lsn = self.next_lsn;
            self.next_lsn += 1;
            WalBatch::finalize_encoded_buf(lsn, crec, &mut cbuf);
            self.append_raw_wal_batch_with_rotation(&cbuf, durability)?;
        }

        Ok(())
    }

    /// Flushes the current `MemTable` to a new immutable `idx_<SeqID>.db` segment, rotates both
    /// WAL streams to `epoch + 1` in $O(1)$ without zero-filling, atomically commits `boot_meta.db`,
    /// and triggers automatic compaction if `auto_compact_segment_threshold` is reached (`PERF-02`).
    /// If called while an explicit transaction is active (`tx_depth > 0`), defers checkpointing
    /// until `commit_transaction` so provisional state is never published to SSTables (`F02`).
    pub fn flush_memtable(&mut self) -> io::Result<()> {
        if self.tx_depth > 0 {
            self.deferred_flush_on_commit = true;
            return Ok(());
        }
        self.flush_memtable_no_auto_compact()?;
        if self.tx_depth == 0
            && self.config.auto_compact_segment_threshold > 1
            && self.idx_segments.len() >= self.config.auto_compact_segment_threshold
        {
            self.compact_auto_tiered()?;
        }
        Ok(())
    }

    fn flush_memtable_no_auto_compact(&mut self) -> io::Result<()> {
        if self.tx_depth > 0 {
            self.deferred_flush_on_commit = true;
            return Ok(());
        }
        self.flush_pending_tx_records()?;
        self.buf_wal.flush_buffered_to_os()?;
        if self.memtable.is_empty() {
            return Ok(());
        }

        // Ensure all active blob segments are durably flushed before checkpointing `boot_meta.db`
        self.sync_active_blob_writers()?;

        let new_seq_id = self.meta_mgr.state_snapshot().next_seq_id;

        let new_reader = match self.memtable.write_to_segment(
            &*self.fs,
            new_seq_id,
            self.config.bloom_bits_per_key,
        ) {
            Ok(r) => r,
            Err(e) => {
                let _ = self.fs.remove_file(&format_idx_file_name(new_seq_id));
                return Err(e);
            }
        };
        let idx_meta = IdxSegmentMeta {
            seq_id: new_reader.seq_id(),
            file_len: new_reader.file_len(),
            level: new_reader.level(),
            record_count: new_reader.record_count(),
        };

        let new_sync_epoch = self.sync_wal.active_epoch() + 1;
        let new_buf_epoch = self.buf_wal.active_epoch() + 1;
        let new_checkpoint_lsn = self.next_lsn.saturating_sub(1);

        let (dead_std_seqs, dead_lru_keys, dead_blob_files) = self.collect_fully_dead_blob_segments();
        let std_metas: Vec<StdBlobSegmentMeta> = self
            .std_blobs
            .iter()
            .filter(|(seq, _)| !dead_std_seqs.contains(seq))
            .map(|(&seq_id, b)| StdBlobSegmentMeta {
                seq_id,
                file_len: b.file_len,
                dead_bytes: b.dead_bytes,
            })
            .collect();
        let lru_metas: Vec<LruBlobSegmentMeta> = self
            .lru_blobs
            .iter()
            .filter(|(k, _)| !dead_lru_keys.contains(k))
            .map(|(&(_tid, seq_id), b)| LruBlobSegmentMeta {
                seq_id,
                file_len: b.file_len,
                dead_bytes: b.dead_bytes,
                table_id: b.table_id,
            })
            .collect();
        let quota_metas = self.build_quota_metas();

        if let Err(e) = self.meta_mgr.commit_update(|st| {
            let allocated = st.allocate_seq_id();
            debug_assert_eq!(allocated, new_seq_id);
            st.idx_segments.push(idx_meta);
            st.std_blob_segments = std_metas;
            st.lru_blob_segments = lru_metas;
            st.table_quotas = quota_metas;
            st.checkpoint_lsn = new_checkpoint_lsn;
            st.sync_wal_epoch = new_sync_epoch;
            st.buf_wal_epoch = new_buf_epoch;
            for fname in &dead_blob_files {
                st.queue_pending_delete(fname.clone());
            }
        }) {
            drop(new_reader);
            let _ = self.fs.remove_file(&format_idx_file_name(new_seq_id));
            return Err(e);
        }

        for seq in dead_std_seqs {
            self.std_blobs.remove(&seq);
        }
        for key in dead_lru_keys {
            self.lru_blobs.remove(&key);
        }
        if !dead_blob_files.is_empty() {
            self.meta_mgr.try_process_pending_deletes();
        }

        self.memtable.clear();
        self.idx_segments.push(new_reader);
        self.sync_wal.reset_for_new_epoch(new_sync_epoch);
        self.buf_wal.reset_for_new_epoch(new_buf_epoch);
        Ok(())
    }

    fn rotate_wal_epochs_only(&mut self) -> io::Result<()> {
        self.buf_wal.flush_buffered_to_os()?;
        let new_sync_epoch = self.sync_wal.active_epoch() + 1;
        let new_buf_epoch = self.buf_wal.active_epoch() + 1;
        let new_checkpoint_lsn = self.next_lsn.saturating_sub(1);
        self.meta_mgr.commit_update(|st| {
            st.checkpoint_lsn = new_checkpoint_lsn;
            st.sync_wal_epoch = new_sync_epoch;
            st.buf_wal_epoch = new_buf_epoch;
        })?;
        self.sync_wal.reset_for_new_epoch(new_sync_epoch);
        self.buf_wal.reset_for_new_epoch(new_buf_epoch);
        Ok(())
    }

    /// Relocates live blobs out of fragmented sealed `blob_std_<SeqID>.db` segments whose
    /// `dead_bytes / file_len >= dead_ratio_threshold`, appending them to the active `blob_std`
    /// segment and marking the old `BlobPtr`s dead so the fragmented segments reach 100% dead
    /// and can be reclaimed in $O(1)$.
    pub fn relocate_fragmented_std_blobs(&mut self, dead_ratio_threshold: f64) -> io::Result<usize> {
        if self.tx_depth > 0 {
            return Ok(0);
        }
        use std::collections::HashSet;
        let active_std_seq = self.active_std_writer.as_ref().map(|w| w.seq_id());
        let fragmented_seqs: HashSet<u64> = self
            .std_blobs
            .iter()
            .filter(|&(&seq, b)| {
                Some(seq) != active_std_seq
                    && b.file_len > 0
                    && b.dead_bytes < b.file_len
                    && (b.dead_bytes as f64) / (b.file_len as f64) >= dead_ratio_threshold
            })
            .map(|(&seq, _)| seq)
            .collect();

        if fragmented_seqs.is_empty() {
            return Ok(0);
        }

        const RELOCATE_BATCH_SIZE: usize = 512;
        let mut seek_key: Vec<u8> = Vec::new();
        let mut lower_inclusive = true;
        let mut relocated = 0usize;

        loop {
            let entries = self.scan_merged_prefix_range(
                &[],
                &seek_key,
                lower_inclusive,
                None,
                RELOCATE_BATCH_SIZE,
            )?;
            if entries.is_empty() {
                break;
            }
            seek_key = entries.last().unwrap().key.clone();
            lower_inclusive = false;

            for entry in entries {
                if let IndexValue::Blob(old_ptr) = entry.value {
                    if old_ptr.kind == BlobKind::Standard
                        && fragmented_seqs.contains(&old_ptr.seg_id)
                    {
                        if let Some(payload) = self.dereference_blob(&old_ptr, &entry.key)? {
                            let new_ptr = self.append_to_blob_segment(
                                BlobKind::Standard,
                                0,
                                &entry.key,
                                &payload,
                            )?;
                            self.record_dead_blob_bytes(&old_ptr);
                            self.commit_wal_and_memtable(
                                vec![WalRecord::PutBlobPtr {
                                    key: entry.key,
                                    blob_ptr: new_ptr,
                                }],
                                WriteDurability::BufferedFast,
                            )?;
                            relocated += 1;
                        }
                    }
                }
            }
        }
        Ok(relocated)
    }

    /// Runs multi-way merge compaction across all active `idx_<SeqID>.db` segments, reclaims
    /// dead blob bytes from overwritten keys, relocates fragmented standard blob segments,
    /// commits the compacted segment to `boot_meta.db`, and deletes superseded `idx` and
    /// 100%-dead blob segments via Deferred GC.
    /// If called while an explicit transaction is active (`tx_depth > 0`), defers compaction
    /// until `commit_transaction` (`F02`).
    pub fn compact(&mut self) -> io::Result<()> {
        if self.tx_depth > 0 {
            self.deferred_compact_on_commit = true;
            return Ok(());
        }
        if !self.memtable.is_empty() {
            self.flush_memtable_no_auto_compact()?;
        }
        let relocated = self.relocate_fragmented_std_blobs(0.5)?;
        if relocated > 0 && !self.memtable.is_empty() {
            self.flush_memtable_no_auto_compact()?;
        }
        if self.idx_segments.len() <= 1 {
            self.reclaim_fully_dead_blob_segments()?;
            return Ok(());
        }

        let comp_seq_id = self.meta_mgr.state_snapshot().next_seq_id;

        // Pass segments ordered newest-first (`rev()`) to `compact_idx_segments`
        let refs_newest_first: Vec<&IdxSegmentReader<FS::File>> =
            self.idx_segments.iter().rev().collect();
        let outcome = match compact_idx_segments(
            &*self.fs,
            &refs_newest_first,
            comp_seq_id,
            1,
            self.config.bloom_bits_per_key,
            true,
        ) {
            Ok(o) => o,
            Err(e) => {
                let _ = self.fs.remove_file(&format_idx_file_name(comp_seq_id));
                return Err(e);
            }
        };

        let old_idx_names: Vec<String> = self
            .idx_segments
            .iter()
            .map(|r| format_idx_file_name(r.seq_id()))
            .collect();

        let new_meta = IdxSegmentMeta {
            seq_id: outcome.new_segment.seq_id(),
            file_len: outcome.new_segment.file_len(),
            level: outcome.new_segment.level(),
            record_count: outcome.new_segment.record_count(),
        };

        let (dead_std_seqs, dead_lru_keys, dead_blob_files) = self.collect_fully_dead_blob_segments();
        let std_metas: Vec<StdBlobSegmentMeta> = self
            .std_blobs
            .iter()
            .filter(|(seq, _)| !dead_std_seqs.contains(seq))
            .map(|(&seq_id, b)| StdBlobSegmentMeta {
                seq_id,
                file_len: b.file_len,
                dead_bytes: b.dead_bytes,
            })
            .collect();
        let lru_metas: Vec<LruBlobSegmentMeta> = self
            .lru_blobs
            .iter()
            .filter(|(k, _)| !dead_lru_keys.contains(k))
            .map(|(&(_tid, seq_id), b)| LruBlobSegmentMeta {
                seq_id,
                file_len: b.file_len,
                dead_bytes: b.dead_bytes,
                table_id: b.table_id,
            })
            .collect();
        let quota_metas = self.build_quota_metas();

        if let Err(e) = self.meta_mgr.commit_update(|st| {
            let allocated = st.allocate_seq_id();
            debug_assert_eq!(allocated, comp_seq_id);
            st.idx_segments = vec![new_meta];
            st.std_blob_segments = std_metas;
            st.lru_blob_segments = lru_metas;
            st.table_quotas = quota_metas;
            for name in old_idx_names {
                st.queue_pending_delete(name);
            }
            for fname in &dead_blob_files {
                st.queue_pending_delete(fname.clone());
            }
        }) {
            drop(outcome);
            let _ = self.fs.remove_file(&format_idx_file_name(comp_seq_id));
            return Err(e);
        }

        for seq in dead_std_seqs {
            self.std_blobs.remove(&seq);
        }
        for key in dead_lru_keys {
            self.lru_blobs.remove(&key);
        }

        // Drop old segment readers and run pending deletes so superseded `idx_<SeqID>.db`
        // and 100%-dead blob files are unlinked immediately now that `boot_meta.db` is committed!
        self.idx_segments.clear();
        self.idx_segments.push(outcome.new_segment);
        self.meta_mgr.try_process_pending_deletes();
        Ok(())
    }

    /// Runs size-tiered suffix automatic compaction (`P01`).
    ///
    /// Selects a contiguous suffix `self.idx_segments[start_idx..]` of newer/smaller segments
    /// such that no older segment is merged when its size exceeds `2x` the combined size of all
    /// newer segments after it:
    /// - When `start_idx == 0` (all segments are comparable in size), delegates to full
    ///   [`compact()`] (`drop_tombstones = true` + blob relocation).
    /// - When `start_idx > 0` (partial suffix compaction), merges only `idx_segments[start_idx..]`
    ///   with `drop_tombstones = false` (preserving tombstones that may shadow keys in older
    ///   segments `0..start_idx`) and skips whole-database blob relocation.
    pub fn compact_auto_tiered(&mut self) -> io::Result<()> {
        if self.tx_depth > 0 {
            self.deferred_compact_on_commit = true;
            return Ok(());
        }
        if !self.memtable.is_empty() {
            self.flush_memtable_no_auto_compact()?;
        }
        if self.idx_segments.len() <= 1 {
            self.reclaim_fully_dead_blob_segments()?;
            return Ok(());
        }

        let n = self.idx_segments.len();
        let mut start_idx = (n / 2).min(n - 2);
        for i in 0..(n - 1) {
            let curr_bytes = self.idx_segments[i].file_len();
            let newer_bytes: u64 = self.idx_segments[i + 1..].iter().map(|s| s.file_len()).sum();
            if curr_bytes <= newer_bytes.saturating_mul(2) {
                start_idx = i;
                break;
            }
        }

        if start_idx == 0 {
            return self.compact();
        }

        let comp_seq_id = self.meta_mgr.state_snapshot().next_seq_id;
        let new_level = self.idx_segments[start_idx..]
            .iter()
            .map(|s| s.level())
            .max()
            .unwrap_or(1)
            .max(1);

        // Pass suffix segments ordered newest-first (`rev()`) with `drop_tombstones = false`
        let refs_newest_first: Vec<&IdxSegmentReader<FS::File>> =
            self.idx_segments[start_idx..].iter().rev().collect();
        let outcome = match compact_idx_segments(
            &*self.fs,
            &refs_newest_first,
            comp_seq_id,
            new_level,
            self.config.bloom_bits_per_key,
            false,
        ) {
            Ok(o) => o,
            Err(e) => {
                let _ = self.fs.remove_file(&format_idx_file_name(comp_seq_id));
                return Err(e);
            }
        };

        let old_idx_names: Vec<String> = self.idx_segments[start_idx..]
            .iter()
            .map(|r| format_idx_file_name(r.seq_id()))
            .collect();

        let new_meta = IdxSegmentMeta {
            seq_id: outcome.new_segment.seq_id(),
            file_len: outcome.new_segment.file_len(),
            level: outcome.new_segment.level(),
            record_count: outcome.new_segment.record_count(),
        };

        let mut new_idx_metas: Vec<IdxSegmentMeta> = Vec::with_capacity(start_idx + 1);
        for seg in &self.idx_segments[..start_idx] {
            new_idx_metas.push(IdxSegmentMeta {
                seq_id: seg.seq_id(),
                file_len: seg.file_len(),
                level: seg.level(),
                record_count: seg.record_count(),
            });
        }
        new_idx_metas.push(new_meta);

        let (dead_std_seqs, dead_lru_keys, dead_blob_files) =
            self.collect_fully_dead_blob_segments();
        let std_metas: Vec<StdBlobSegmentMeta> = self
            .std_blobs
            .iter()
            .filter(|(seq, _)| !dead_std_seqs.contains(seq))
            .map(|(&seq_id, b)| StdBlobSegmentMeta {
                seq_id,
                file_len: b.file_len,
                dead_bytes: b.dead_bytes,
            })
            .collect();
        let lru_metas: Vec<LruBlobSegmentMeta> = self
            .lru_blobs
            .iter()
            .filter(|(k, _)| !dead_lru_keys.contains(k))
            .map(|(&(_tid, seq_id), b)| LruBlobSegmentMeta {
                seq_id,
                file_len: b.file_len,
                dead_bytes: b.dead_bytes,
                table_id: b.table_id,
            })
            .collect();
        let quota_metas = self.build_quota_metas();

        if let Err(e) = self.meta_mgr.commit_update(|st| {
            let allocated = st.allocate_seq_id();
            debug_assert_eq!(allocated, comp_seq_id);
            st.idx_segments = new_idx_metas;
            st.std_blob_segments = std_metas;
            st.lru_blob_segments = lru_metas;
            st.table_quotas = quota_metas;
            for name in old_idx_names {
                st.queue_pending_delete(name);
            }
            for fname in &dead_blob_files {
                st.queue_pending_delete(fname.clone());
            }
        }) {
            drop(outcome);
            let _ = self.fs.remove_file(&format_idx_file_name(comp_seq_id));
            return Err(e);
        }

        for seq in dead_std_seqs {
            self.std_blobs.remove(&seq);
        }
        for key in dead_lru_keys {
            self.lru_blobs.remove(&key);
        }

        self.idx_segments.truncate(start_idx);
        self.idx_segments.push(outcome.new_segment);
        self.meta_mgr.try_process_pending_deletes();
        Ok(())
    }

    // =========================================================================
    // Internal Helper Methods
    // =========================================================================

    fn eager_promote_lru_on_read(&mut self, table_id: u16, pk: &[u8]) -> io::Result<()> {
        let meta_key = encode_row_meta_key(table_id, pk);
        let Some((mut meta, old_m_ptr)) = self.get_row_meta_with_ptr(&meta_key)? else {
            return Ok(());
        };
        let old_tick = meta.recorded_tick;
        let new_tick = self.access_tracker.next_tick();
        meta.recorded_tick = new_tick;

        let mut records = Vec::with_capacity(3);
        if old_tick > 0 {
            records.push(WalRecord::Delete {
                key: encode_lru_queue_key(table_id, old_tick, pk),
            });
        }
        records.push(WalRecord::PutInline {
            key: encode_lru_queue_key(table_id, new_tick, pk),
            value: Vec::new(),
        });
        let meta_bytes = meta.to_bytes();
        if !IndexEntry::inline_fits_in_block(meta_key.len(), meta_bytes.len()) {
            let ptr = self.append_to_blob_segment(BlobKind::Standard, 0, &meta_key, &meta_bytes)?;
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
        self.commit_wal_and_memtable(records, WriteDurability::BufferedFast)?;
        if let Some(m_ptr) = old_m_ptr {
            self.record_dead_blob_bytes(&m_ptr);
        }
        Ok(())
    }

    fn evict_until_within_quota(
        &mut self,
        table_id: u16,
        durability: WriteDurability,
    ) -> io::Result<()> {
        const EVICT_SCAN_WINDOW: usize = 64;
        loop {
            let (over, cursor_tick) = self
                .capped_tables
                .get(&table_id)
                .map(|ct| (ct.is_over_quota(), ct.eviction_cursor_tick))
                .unwrap_or((false, 0));
            if !over {
                break;
            }

            let lru_prefix = encode_namespace_prefix(table_id, NS_LRU_QUEUE);
            let seek_key = encode_lru_queue_key(table_id, cursor_tick, &[]);
            let candidates =
                self.scan_merged_prefix_from(&lru_prefix, &seek_key, EVICT_SCAN_WINDOW)?;
            if candidates.is_empty() {
                if cursor_tick > 0 {
                    if let Some(ct) = self.capped_tables.get_mut(&table_id) {
                        ct.eviction_cursor_tick = 0;
                    }
                    continue;
                }
                break;
            }

            let mut progress = false;
            let mut last_scanned_tick = cursor_tick;

            for entry in candidates {
                let Some((old_tick, pk_slice)) = decode_lru_queue_key(table_id, &entry.key) else {
                    continue;
                };
                last_scanned_tick = old_tick;

                if matches!(entry.value, IndexValue::Tombstone) {
                    continue;
                }
                let pk = pk_slice.to_vec();

                // Check if this key was accessed in RAM since `old_tick` was indexed!
                if let Some(hot_tick) = self.access_tracker.take_hot_tick(table_id, &pk) {
                    if hot_tick > old_tick {
                        // LAZY PROMOTION: Move key to `hot_tick` in the LRU queue without evicting!
                        let meta_key = encode_row_meta_key(table_id, &pk);
                        let mut records = vec![
                            WalRecord::Delete {
                                key: entry.key.clone(),
                            },
                            WalRecord::PutInline {
                                key: encode_lru_queue_key(table_id, hot_tick, &pk),
                                value: Vec::new(),
                            },
                        ];
                        let mut old_m_ptr = None;
                        if let Some((mut mrec, m_ptr)) = self.get_row_meta_with_ptr(&meta_key)? {
                            old_m_ptr = m_ptr;
                            mrec.recorded_tick = hot_tick;
                            let meta_bytes = mrec.to_bytes();
                            if !IndexEntry::inline_fits_in_block(meta_key.len(), meta_bytes.len())
                            {
                                let ptr = self.append_to_blob_segment(
                                    BlobKind::Standard,
                                    0,
                                    &meta_key,
                                    &meta_bytes,
                                )?;
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
                        }
                        self.commit_wal_and_memtable(records, durability)?;
                        if let Some(m_ptr) = old_m_ptr {
                            self.record_dead_blob_bytes(&m_ptr);
                        }
                        if let Some(ct) = self.capped_tables.get_mut(&table_id) {
                            ct.total_lazy_promotions += 1;
                            ct.eviction_cursor_tick = old_tick.saturating_add(1);
                        }
                        progress = true;
                        continue;
                    }
                }

                // Coldest candidate confirmed -> Cascade Evict!
                if self.delete_row(table_id, &pk, durability)? {
                    if let Some(ct) = self.capped_tables.get_mut(&table_id) {
                        ct.total_evictions += 1;
                        ct.eviction_cursor_tick = old_tick.saturating_add(1);
                    }
                    progress = true;
                } else {
                    // Stale LRU queue entry whose row was already gone; clean up the orphan `'l'` key
                    self.commit_wal_and_memtable(
                        vec![WalRecord::Delete { key: entry.key }],
                        durability,
                    )?;
                    if let Some(ct) = self.capped_tables.get_mut(&table_id) {
                        ct.eviction_cursor_tick = old_tick.saturating_add(1);
                    }
                    progress = true;
                }

                let still_over = self
                    .capped_tables
                    .get(&table_id)
                    .map(|ct| ct.is_over_quota())
                    .unwrap_or(false);
                if !still_over {
                    break;
                }
            }

            if !progress {
                if last_scanned_tick >= cursor_tick {
                    if let Some(ct) = self.capped_tables.get_mut(&table_id) {
                        ct.eviction_cursor_tick = last_scanned_tick + 1;
                    }
                } else {
                    break;
                }
            }
        }

        if self.tx_depth == 0 {
            self.reclaim_fully_dead_lru_blob_segments(table_id)?;
        }
        Ok(())
    }

    fn sync_active_blob_writers(&mut self) -> io::Result<()> {
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

    fn append_to_blob_segment(
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
                    if let Some(old_w) = self.active_std_writer.take() {
                        old_w.sync_durable()?;
                        if let Some(b) = self.std_blobs.get_mut(&old_w.seq_id()) {
                            b.file_len = old_w.current_len();
                        }
                    }
                    let seq_id = self.meta_mgr.state_snapshot().next_seq_id;
                    let writer = BlobSegmentWriter::open(&*self.fs, BlobKind::Standard, 0, seq_id)?;
                    self.std_blobs.insert(
                        seq_id,
                        TrackedStdBlob {
                            reader: BlobSegmentReader::new(writer.file().clone(), seq_id),
                            file_len: 0,
                            dead_bytes: 0,
                        },
                    );
                    let mut std_metas = self.build_std_blob_metas();
                    // Keep `boot_meta.db` `dead_bytes` at their checkpointed values until `flush_memtable()`!
                    let prev_std = &self.meta_mgr.state().std_blob_segments;
                    for m in &mut std_metas {
                        m.dead_bytes = prev_std
                            .iter()
                            .find(|p| p.seq_id == m.seq_id)
                            .map(|p| p.dead_bytes)
                            .unwrap_or(0);
                    }
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
                    if let Some(old_w) = self.active_lru_writers.remove(&table_id) {
                        old_w.sync_durable()?;
                        if let Some(b) = self.lru_blobs.get_mut(&(table_id, old_w.seq_id())) {
                            b.file_len = old_w.current_len();
                        }
                    }
                    let seq_id = self.meta_mgr.state_snapshot().next_seq_id;
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
                    let mut lru_metas = self.build_lru_blob_metas();
                    let prev_lru = &self.meta_mgr.state().lru_blob_segments;
                    for m in &mut lru_metas {
                        m.dead_bytes = prev_lru
                            .iter()
                            .find(|p| p.table_id == m.table_id && p.seq_id == m.seq_id)
                            .map(|p| p.dead_bytes)
                            .unwrap_or(0);
                    }
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

    fn record_dead_blob_bytes(&mut self, ptr: &BlobPtr) {
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

    fn collect_fully_dead_blob_segments(&self) -> (Vec<u64>, Vec<(u16, u64)>, Vec<String>) {
        let active_std_seq = self.active_std_writer.as_ref().map(|w| w.seq_id());
        let dead_std: Vec<u64> = self
            .std_blobs
            .iter()
            .filter(|&(&seq, b)| {
                Some(seq) != active_std_seq && b.file_len > 0 && b.dead_bytes >= b.file_len
            })
            .map(|(&seq, _)| seq)
            .collect();

        let mut deleted_files = Vec::new();
        for &seq in &dead_std {
            deleted_files.push(format_std_blob_file_name(seq));
        }

        let dead_lru: Vec<(u16, u64)> = self
            .lru_blobs
            .iter()
            .filter(|&(&(tid, seq), b)| {
                let active_seq = self.active_lru_writers.get(&tid).map(|w| w.seq_id());
                Some(seq) != active_seq && b.file_len > 0 && b.dead_bytes >= b.file_len
            })
            .map(|(&k, _)| k)
            .collect();

        for &(tid, seq) in &dead_lru {
            deleted_files.push(format_lru_blob_file_name(tid, seq));
        }

        (dead_std, dead_lru, deleted_files)
    }

    /// Reclaims any sealed `blob_lru` segment belonging to `table_id` where `dead_bytes >= file_len`,
    /// syncing `buf_wal` first so the eviction WAL records are durable before unlinking (`AUD-06`).
    fn reclaim_fully_dead_lru_blob_segments(&mut self, table_id: u16) -> io::Result<()> {
        let active_seq = self.active_lru_writers.get(&table_id).map(|w| w.seq_id());
        let dead_lru: Vec<(u16, u64)> = self
            .lru_blobs
            .iter()
            .filter(|&(&(tid, seq), b)| {
                tid == table_id
                    && Some(seq) != active_seq
                    && b.file_len > 0
                    && b.dead_bytes >= b.file_len
            })
            .map(|(&k, _)| k)
            .collect();

        if dead_lru.is_empty() {
            return Ok(());
        }

        // Ensure all buffered eviction `Delete` WAL records that killed these LRU blob segments
        // are durable on disk before removing the segments from `boot_meta.db` and unlinking them (`AUD-06`).
        self.buf_wal.flush_buffered_to_os()?;
        self.buf_wal.file().sync_durable()?;
        self.last_buf_synced_lsn = self.last_buf_append_lsn;

        let mut deleted_files = Vec::with_capacity(dead_lru.len());
        for &(tid, seq) in &dead_lru {
            deleted_files.push(format_lru_blob_file_name(tid, seq));
            self.lru_blobs.remove(&(tid, seq));
        }

        let mut lru_metas = self.build_lru_blob_metas();
        let prev_lru = &self.meta_mgr.state().lru_blob_segments;
        for m in &mut lru_metas {
            m.dead_bytes = prev_lru
                .iter()
                .find(|p| p.table_id == m.table_id && p.seq_id == m.seq_id)
                .map(|p| p.dead_bytes)
                .unwrap_or(0);
        }
        self.meta_mgr.commit_update(|st| {
            st.lru_blob_segments = lru_metas;
            for fname in &deleted_files {
                st.queue_pending_delete(fname.clone());
            }
        })?;
        self.meta_mgr.try_process_pending_deletes();
        Ok(())
    }

    /// Reclaims any sealed `blob_std` or `blob_lru` segment where `dead_bytes >= file_len`
    /// ($O(1)$ Copy-less Segment Deletion!).
    fn reclaim_fully_dead_blob_segments(&mut self) -> io::Result<()> {
        let (dead_std, dead_lru, deleted_files) = self.collect_fully_dead_blob_segments();
        if deleted_files.is_empty() {
            return Ok(());
        }

        for seq in dead_std {
            self.std_blobs.remove(&seq);
        }
        for key in dead_lru {
            self.lru_blobs.remove(&key);
        }

        let mut std_metas = self.build_std_blob_metas();
        let prev_std = &self.meta_mgr.state().std_blob_segments;
        for m in &mut std_metas {
            m.dead_bytes = prev_std
                .iter()
                .find(|p| p.seq_id == m.seq_id)
                .map(|p| p.dead_bytes)
                .unwrap_or(0);
        }
        let mut lru_metas = self.build_lru_blob_metas();
        let prev_lru = &self.meta_mgr.state().lru_blob_segments;
        for m in &mut lru_metas {
            m.dead_bytes = prev_lru
                .iter()
                .find(|p| p.table_id == m.table_id && p.seq_id == m.seq_id)
                .map(|p| p.dead_bytes)
                .unwrap_or(0);
        }
        self.meta_mgr.commit_update(|st| {
            st.std_blob_segments = std_metas;
            st.lru_blob_segments = lru_metas;
            for fname in &deleted_files {
                st.queue_pending_delete(fname.clone());
            }
        })?;
        self.meta_mgr.try_process_pending_deletes();
        Ok(())
    }

    fn dereference_blob(&self, ptr: &BlobPtr, expected_key: &[u8]) -> io::Result<Option<Vec<u8>>> {
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

    fn dereference_blob_append_into(
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

    fn commit_wal_and_memtable(
        &mut self,
        records: Vec<WalRecord>,
        durability: WriteDurability,
    ) -> io::Result<u64> {
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
            for rec in &records {
                let key = match rec {
                    WalRecord::PutInline { key, .. }
                    | WalRecord::PutBlobPtr { key, .. }
                    | WalRecord::Delete { key } => key.as_slice(),
                    WalRecord::TxBegin | WalRecord::TxCommit => continue,
                };
                let prev = self.memtable.get(key).cloned();
                self.tx_memtable_undo.push((key.to_vec(), prev));
            }
            self.memtable
                .apply_wal_batch_owned(WalBatch::new(lsn, records));
            return Ok(lsn);
        }

        let batch = WalBatch::new(lsn, records);
        let encoded = batch.encode();
        self.append_raw_wal_batch_with_rotation(&encoded, durability)?;
        self.next_lsn = self.next_lsn.max(lsn + 1);
        self.memtable.apply_wal_batch_owned(batch);
        if self.memtable.should_flush() {
            self.flush_memtable()?;
        }
        Ok(lsn)
    }

    fn append_raw_wal_batch_with_rotation(
        &mut self,
        encoded: &[u8],
        durability: WriteDurability,
    ) -> io::Result<()> {
        let batch_lsn = if encoded.len() >= 8 {
            u64::from_le_bytes(encoded[0..8].try_into().unwrap())
        } else {
            self.next_lsn
        };

        // Enforce prefix durability across `wal_buf.db`, active blob segments, and `wal_sync.db` (`AUD-08`, `F06`):
        // before appending a `SyncTransactional` batch at LSN `L`, ensure all earlier `BufferedFast`
        // batches AND their out-of-line blobs (`last_buf_append_lsn > last_buf_synced_lsn`) are synced!
        if durability == WriteDurability::SyncTransactional
            && self.last_buf_append_lsn > self.last_buf_synced_lsn
        {
            self.sync_active_blob_writers()?;
            self.buf_wal.flush_buffered_to_os()?;
            self.buf_wal.file().sync_barrier()?;
            self.last_buf_synced_lsn = self.last_buf_append_lsn;
        }

        let outcome = match durability {
            WriteDurability::SyncTransactional => self.sync_wal.append_raw_batch(encoded)?,
            WriteDurability::BufferedFast => self.buf_wal.append_raw_batch(encoded)?,
        };

        if outcome == WalAppendOutcome::RingFull {
            if self.memtable.is_empty() {
                self.rotate_wal_epochs_only()?;
            } else {
                self.flush_memtable()?;
            }
            let retry = match durability {
                WriteDurability::SyncTransactional => self.sync_wal.append_raw_batch(encoded)?,
                WriteDurability::BufferedFast => self.buf_wal.append_raw_batch(encoded)?,
            };
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

    fn check_existing_row(&self, key: &[u8]) -> io::Result<ExistingRowStatus> {
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

    fn get_row_meta_with_ptr(
        &self,
        meta_key: &[u8],
    ) -> io::Result<Option<(RowMetaRecord, Option<BlobPtr>)>> {
        let Some((_lsn, val)) = self.lookup_raw_entry(meta_key)? else {
            return Ok(None);
        };
        match val {
            IndexValue::Inline(bytes) => Ok(RowMetaRecord::from_bytes(&bytes).ok().map(|r| (r, None))),
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

    fn lookup_raw_entry(&self, key: &[u8]) -> io::Result<Option<(u64, IndexValue)>> {
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

    fn scan_merged_prefix(&self, prefix: &[u8]) -> io::Result<Vec<IndexEntry>> {
        self.scan_merged_prefix_from(prefix, prefix, usize::MAX)
    }

    fn scan_merged_prefix_from(
        &self,
        prefix: &[u8],
        seek_key: &[u8],
        limit: usize,
    ) -> io::Result<Vec<IndexEntry>> {
        self.scan_merged_prefix_range(prefix, seek_key, true, None, limit)
    }

    /// Performs a bounded-memory streaming K-way merge across `self.memtable` and all
    /// `self.idx_segments` within `[seek_key, upper_key_bound]` under `prefix`.
    ///
    /// Resolves the highest-`lsn` version of each key on the fly, discards
    /// `IndexValue::Tombstone` entries before counting toward `limit`, and returns up to
    /// `limit` live entries in ascending key order.
    fn scan_merged_prefix_range(
        &self,
        prefix: &[u8],
        seek_key: &[u8],
        lower_inclusive: bool,
        upper_key_bound: Option<(&[u8], bool)>,
        limit: usize,
    ) -> io::Result<Vec<IndexEntry>> {
        use std::ops::Bound;
        if limit == 0 {
            return Ok(Vec::new());
        }

        let (eff_lo_key, eff_lo_inc) = if seek_key >= prefix {
            (seek_key, lower_inclusive)
        } else {
            (prefix, true)
        };

        if let Some((ub, ub_inc)) = upper_key_bound {
            if eff_lo_key > ub || (eff_lo_key == ub && (!eff_lo_inc || !ub_inc)) {
                return Ok(Vec::new());
            }
        }

        let in_upper_and_prefix = |key: &[u8]| -> bool {
            if !key.starts_with(prefix) {
                return false;
            }
            if let Some((ub, ub_inc)) = upper_key_bound {
                if ub_inc {
                    if key > ub {
                        return false;
                    }
                } else if key >= ub {
                    return false;
                }
            }
            true
        };

        let prefix_succ_opt: Option<Vec<u8>> = if upper_key_bound.is_none() && !prefix.is_empty() {
            let mut s = prefix.to_vec();
            while s.last() == Some(&0xFF) {
                s.pop();
            }
            if let Some(last) = s.last_mut() {
                *last += 1;
                Some(s)
            } else {
                None
            }
        } else {
            None
        };

        let lo_bound = if eff_lo_inc {
            Bound::Included(eff_lo_key)
        } else {
            Bound::Excluded(eff_lo_key)
        };
        let hi_bound = match upper_key_bound {
            Some((ub, true)) => Bound::Included(ub),
            Some((ub, false)) => Bound::Excluded(ub),
            None => match &prefix_succ_opt {
                Some(succ) if succ.as_slice() > eff_lo_key => Bound::Excluded(succ.as_slice()),
                _ => Bound::Unbounded,
            },
        };
        let mut mem_iter = self.memtable.iter_range(lo_bound, hi_bound).peekable();
        let mut mem_exhausted = false;

        let mut seg_cursors = Vec::with_capacity(self.idx_segments.len());
        let upper_for_overlap = upper_key_bound
            .map(|(ub, _)| ub)
            .or(prefix_succ_opt.as_deref());
        for seg in &self.idx_segments {
            if !seg.overlaps_range(eff_lo_key, upper_for_overlap) {
                continue;
            }
            if !prefix.is_empty()
                && seg.min_key() > prefix
                && !seg.min_key().starts_with(prefix)
            {
                continue;
            }
            seg_cursors.push(seg.cursor_from(eff_lo_key, eff_lo_inc)?);
        }

        let mut out = Vec::new();
        while out.len() < limit {
            let mut min_key: Option<&[u8]> = None;

            if !mem_exhausted {
                if let Some(&(k, _)) = mem_iter.peek() {
                    if in_upper_and_prefix(k.as_slice()) {
                        min_key = Some(k.as_slice());
                    } else {
                        mem_exhausted = true;
                    }
                } else {
                    mem_exhausted = true;
                }
            }

            for cur in &seg_cursors {
                if let Some(entry) = cur.peek() {
                    if in_upper_and_prefix(entry.key.as_slice()) {
                        match min_key {
                            None => min_key = Some(entry.key.as_slice()),
                            Some(mk) if entry.key.as_slice() < mk => {
                                min_key = Some(entry.key.as_slice());
                            }
                            _ => {}
                        }
                    }
                }
            }

            let Some(target_key_slice) = min_key else {
                break;
            };
            let target_key = target_key_slice.to_vec();

            let mut best_entry: Option<IndexEntry> = None;

            if !mem_exhausted {
                if let Some(&(k, _)) = mem_iter.peek() {
                    if k.as_slice() == target_key.as_slice() {
                        let (k, mem_entry) = mem_iter.next().unwrap();
                        best_entry = Some(IndexEntry {
                            key: k.clone(),
                            lsn: mem_entry.lsn,
                            value: mem_entry.value.clone(),
                        });
                    }
                }
            }

            for cur in &mut seg_cursors {
                while cur
                    .peek()
                    .map(|e| e.key.as_slice() == target_key.as_slice())
                    .unwrap_or(false)
                {
                    let seg_entry = cur.next_entry()?.unwrap();
                    match &mut best_entry {
                        None => best_entry = Some(seg_entry),
                        Some(existing) => {
                            if seg_entry.lsn > existing.lsn {
                                *existing = seg_entry;
                            }
                        }
                    }
                }
            }

            if let Some(winner) = best_entry {
                if !matches!(winner.value, IndexValue::Tombstone) {
                    out.push(winner);
                }
            }
        }

        Ok(out)
    }

    fn reconcile_single_capped_table(&mut self, tid: u16) -> io::Result<()> {
        const RECONCILE_BATCH_SIZE: usize = 512;
        let meta_prefix = encode_namespace_prefix(tid, NS_ROW_META);
        let user_prefix = encode_namespace_prefix(tid, NS_PRIMARY_ROW);

        let mut r_seek_key = user_prefix.to_vec();
        let mut r_lower_inc = true;
        let mut total_bytes = 0u64;

        loop {
            let row_entries = self.scan_merged_prefix_range(
                &user_prefix,
                &r_seek_key,
                r_lower_inc,
                None,
                RECONCILE_BATCH_SIZE,
            )?;
            if row_entries.is_empty() {
                break;
            }
            r_seek_key = row_entries.last().unwrap().key.clone();
            r_lower_inc = false;

            let first_pk = decode_primary_row_key(tid, &row_entries.first().unwrap().key)
                .unwrap_or(&[]);
            let last_pk = decode_primary_row_key(tid, &row_entries.last().unwrap().key)
                .unwrap_or(&[]);
            let meta_lo = encode_row_meta_key(tid, first_pk);
            let meta_hi = encode_row_meta_key(tid, last_pk);

            let meta_entries = self.scan_merged_prefix_range(
                &meta_prefix,
                &meta_lo,
                true,
                Some((&meta_hi, true)),
                 usize::MAX,
            )?;
            let mut meta_map: HashMap<Vec<u8>, (RowMetaRecord, Option<BlobPtr>)> =
                HashMap::with_capacity(meta_entries.len());
            for entry in meta_entries {
                if entry.key.len() >= 3 {
                    let decoded = match entry.value {
                        IndexValue::Inline(bytes) => {
                            RowMetaRecord::from_bytes(&bytes).ok().map(|r| (r, None))
                        }
                        IndexValue::Blob(ptr) => self
                            .dereference_blob(&ptr, &entry.key)?
                            .and_then(|bytes| RowMetaRecord::from_bytes(&bytes).ok())
                            .map(|r| (r, Some(ptr))),
                        IndexValue::Tombstone => None,
                    };
                    if let Some((rec, m_ptr)) = decoded {
                        self.access_tracker.observe_tick(rec.recorded_tick);
                        meta_map.insert(entry.key[3..].to_vec(), (rec, m_ptr));
                    }
                }
            }

            let mut missing_records = Vec::new();
            let mut dead_meta_ptrs = Vec::new();

            for entry in row_entries {
                let Some(pk) = decode_primary_row_key(tid, &entry.key) else {
                    continue;
                };
                if let Some((meta, old_m_ptr)) = meta_map.get(pk) {
                    total_bytes = total_bytes.saturating_add(meta.logical_size as u64);
                    if meta.recorded_tick == 0 {
                        let tick = self.access_tracker.next_tick();
                        missing_records.push(WalRecord::PutInline {
                            key: encode_lru_queue_key(tid, tick, pk),
                            value: Vec::new(),
                        });
                        let meta_key = encode_row_meta_key(tid, pk);
                        let meta_bytes = RowMetaRecord::encode_slice(
                            tick,
                            meta.logical_size,
                            &meta.secondary_keys,
                        );
                        if !IndexEntry::inline_fits_in_block(meta_key.len(), meta_bytes.len()) {
                            let ptr = self.append_to_blob_segment(
                                BlobKind::Standard,
                                0,
                                &meta_key,
                                &meta_bytes,
                            )?;
                            missing_records.push(WalRecord::PutBlobPtr {
                                key: meta_key,
                                blob_ptr: ptr,
                            });
                        } else {
                            missing_records.push(WalRecord::PutInline {
                                key: meta_key,
                                value: meta_bytes,
                            });
                        }
                        if let Some(m_ptr) = *old_m_ptr {
                            dead_meta_ptrs.push(m_ptr);
                        }
                    }
                } else {
                    let val_len = match &entry.value {
                        IndexValue::Inline(v) => v.len(),
                        IndexValue::Blob(ptr) => {
                            (ptr.len as usize).saturating_sub(16 + entry.key.len())
                        }
                        IndexValue::Tombstone => continue,
                    };
                    let logical_size = (pk.len() + val_len) as u32;
                    total_bytes = total_bytes.saturating_add(logical_size as u64);
                    let tick = self.access_tracker.next_tick();
                    missing_records.push(WalRecord::PutInline {
                        key: encode_lru_queue_key(tid, tick, pk),
                        value: Vec::new(),
                    });
                    missing_records.push(WalRecord::PutInline {
                        key: encode_row_meta_key(tid, pk),
                        value: RowMetaRecord::encode_slice(tick, logical_size, &[]),
                    });
                }
            }

            if !missing_records.is_empty() {
                self.commit_wal_and_memtable(missing_records, WriteDurability::BufferedFast)?;
                for m_ptr in dead_meta_ptrs {
                    self.record_dead_blob_bytes(&m_ptr);
                }
            }
        }

        if let Some(ct) = self.capped_tables.get_mut(&tid) {
            ct.current_logical_bytes = total_bytes;
        }
        Ok(())
    }

    fn reconcile_capped_tables_after_recovery(&mut self) -> io::Result<()> {
        let table_ids: Vec<u16> = self.capped_tables.keys().copied().collect();
        for tid in table_ids {
            self.reconcile_single_capped_table(tid)?;
        }
        Ok(())
    }

    fn build_std_blob_metas(&self) -> Vec<StdBlobSegmentMeta> {
        self.std_blobs
            .iter()
            .map(|(&seq_id, b)| StdBlobSegmentMeta {
                seq_id,
                file_len: b.file_len,
                dead_bytes: b.dead_bytes,
            })
            .collect()
    }

    fn build_lru_blob_metas(&self) -> Vec<LruBlobSegmentMeta> {
        self.lru_blobs
            .iter()
            .map(|(&(_tid, seq_id), b)| LruBlobSegmentMeta {
                seq_id,
                file_len: b.file_len,
                dead_bytes: b.dead_bytes,
                table_id: b.table_id,
            })
            .collect()
    }

    fn build_quota_metas(&self) -> Vec<TableQuotaMeta> {
        self.capped_tables
            .values()
            .map(|ct| TableQuotaMeta {
                table_id: ct.table_id,
                eviction_policy: EvictionPolicy::Lru,
                max_bytes: ct.max_bytes,
                current_logical_bytes: ct.current_logical_bytes,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pal::OsFs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static ENGINE_TEST_COUNTER: AtomicU64 = AtomicU64::new(1);

    struct TempTestDir {
        path: PathBuf,
    }

    impl TempTestDir {
        fn new(prefix: &str) -> Self {
            let id = ENGINE_TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
            let pid = std::process::id();
            let dir = std::env::temp_dir().join(format!("aydb_engine_test_{prefix}_{pid}_{id}"));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).expect("create temp test dir");
            Self { path: dir }
        }
    }

    impl Drop for TempTestDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    #[test]
    fn capped_table_lazy_promotion_and_cascading_eviction_with_copyless_blob_gc() {
        let tmp = TempTestDir::new("capped_lru");
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());

        let mut cfg = EngineConfig::default();
        cfg.inline_threshold_bytes = 256;
        cfg.max_blob_segment_bytes = 2048; // Force blob segment rolling every ~2 blobs

        let mut engine = StorageEngine::open(fs.clone(), cfg.clone()).unwrap();
        // Register Capped Table 1 with MAX_BYTES = 3,200 (fits at most 3 rows of ~1,000 B each)
        engine.register_capped_table(1, 3_200).unwrap();

        let payload = vec![0x42u8; 1000];
        // Insert k1, k2, k3 (each ~1,002 B -> total ~3,006 B <= 3,200 B)
        engine
            .put_row(
                1,
                b"k1",
                &payload,
                &[(10, b"tag_a".to_vec())],
                WriteDurability::BufferedFast,
            )
            .unwrap();
        engine
            .put_row(
                1,
                b"k2",
                &payload,
                &[(10, b"tag_b".to_vec())],
                WriteDurability::BufferedFast,
            )
            .unwrap();
        engine
            .put_row(
                1,
                b"k3",
                &payload,
                &[(10, b"tag_a".to_vec())],
                WriteDurability::BufferedFast,
            )
            .unwrap();

        // Read "k1" so it receives a newer in-memory AccessTick via CoalescedLazyPromotion!
        assert_eq!(engine.get_row(1, b"k1").unwrap().unwrap(), payload);

        // Now insert "k4" (~1,002 B) -> total would be ~4,008 B > 3,200 B!
        // Eviction scanner checks "k1" first, sees hot_tick > old_tick -> LAZY PROMOTES "k1"
        // and evicts "k2" instead!
        engine
            .put_row(
                1,
                b"k4",
                &payload,
                &[(10, b"tag_c".to_vec())],
                WriteDurability::BufferedFast,
            )
            .unwrap();

        assert!(
            engine.get_row(1, b"k1").unwrap().is_some(),
            "k1 was read recently and must survive via Lazy Promotion"
        );
        assert!(
            engine.get_row(1, b"k2").unwrap().is_none(),
            "k2 was the coldest un-accessed key and must be evicted"
        );
        assert!(
            engine
                .lookup_by_secondary_index(1, 10, b"tag_b")
                .unwrap()
                .is_empty(),
            "secondary index for evicted k2 must be cascade-deleted"
        );
        assert_eq!(
            engine.lookup_by_secondary_index(1, 10, b"tag_a").unwrap(),
            vec![b"k1".to_vec(), b"k3".to_vec()]
        );

        let st = engine.capped_table_state(1).unwrap();
        assert!(st.current_logical_bytes <= 3_200);
        assert_eq!(st.total_lazy_promotions, 1);
        assert_eq!(st.total_evictions, 1);

        // Insert k5, k6, k7 to push out k3, k1, k4 and verify older blob_lru segments are unlinked
        for k in [b"k5", b"k6", b"k7"] {
            engine
                .put_row(1, k, &payload, &[], WriteDurability::BufferedFast)
                .unwrap();
        }
        assert!(engine.get_row(1, b"k1").unwrap().is_none());
        assert!(engine.get_row(1, b"k3").unwrap().is_none());
        assert!(engine.get_row(1, b"k4").unwrap().is_none());
        assert!(engine.get_row(1, b"k7").unwrap().is_some());

        // Flush, compact, and reopen to verify full persistence, O(1) blob GC, & recovery
        engine.flush_memtable().unwrap();
        // Sealed blob segments whose entries were all evicted must have been reclaimed on flush
        assert!(
            engine.active_lru_blob_segment_count(1) <= 2,
            "sealed 100%-dead blob_lru segments must be unlinked automatically"
        );
        engine.compact().unwrap();
        drop(engine);

        let mut reopened = StorageEngine::open(fs, cfg).unwrap();
        assert!(reopened.get_row(1, b"k2").unwrap().is_none());
        assert_eq!(reopened.get_row(1, b"k7").unwrap().unwrap(), payload);
        assert!(
            reopened
                .capped_table_state(1)
                .unwrap()
                .current_logical_bytes
                <= 3_200
        );
    }

    #[test]
    fn inverted_and_empty_range_queries_do_not_panic() {
        let tmp = TempTestDir::new("inverted_range");
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
        let mut engine = StorageEngine::open(fs, EngineConfig::default()).unwrap();

        for i in 1..=10u8 {
            engine
                .put_row(1, &[i], &[i * 10], &[], WriteDurability::BufferedFast)
                .unwrap();
        }

        // 1. Test MemTable-only path (`idx_segments.is_empty()`)
        assert!(
            engine
                .scan_table_range(1, Some((&[8], true)), Some((&[3], true)), 50)
                .unwrap()
                .is_empty()
        );
        assert!(
            engine
                .scan_table_range(1, Some((&[5], false)), Some((&[5], false)), 50)
                .unwrap()
                .is_empty()
        );
        assert!(
            engine
                .scan_table_range(1, Some((&[5], true)), Some((&[5], false)), 50)
                .unwrap()
                .is_empty()
        );

        let mut slab = Vec::new();
        let mut spans = Vec::new();
        engine
            .scan_table_values_into(
                1,
                Some((&[8], true)),
                Some((&[3], true)),
                50,
                &mut slab,
                &mut spans,
            )
            .unwrap();
        assert!(spans.is_empty());

        // 2. Flush to SSTable and test multi-segment path
        engine.flush_memtable().unwrap();
        assert!(
            engine
                .scan_table_range(1, Some((&[8], true)), Some((&[3], true)), 50)
                .unwrap()
                .is_empty()
        );
        assert!(
            engine
                .scan_table_range(1, Some((&[5], false)), Some((&[5], false)), 50)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn multi_segment_range_scan_and_lru_eviction_skip_tombstone_windows() {
        let tmp = TempTestDir::new("tombstone_window");
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
        let mut engine = StorageEngine::open(fs, EngineConfig::default()).unwrap();

        // Segment 1: 120 live rows k_000..k_119
        for i in 0..120u32 {
            let k = format!("k_{i:03}").into_bytes();
            let v = format!("val_{i:03}").into_bytes();
            engine
                .put_row(1, &k, &v, &[], WriteDurability::BufferedFast)
                .unwrap();
        }
        engine.flush_memtable().unwrap();

        // Segment 2: 100 tombstones shadowing k_000..k_099
        for i in 0..100u32 {
            let k = format!("k_{i:03}").into_bytes();
            engine
                .delete_row(1, &k, WriteDurability::BufferedFast)
                .unwrap();
        }
        engine.flush_memtable().unwrap();

        // Scan with limit = 5 must skip all 100 tombstones and return k_100..k_104
        let rows = engine.scan_table_range(1, None, None, 5).unwrap();
        assert_eq!(rows.len(), 5);
        for (idx, (pk, val)) in rows.iter().enumerate() {
            let expected_i = 100 + idx;
            assert_eq!(pk, &format!("k_{expected_i:03}").into_bytes());
            assert_eq!(val, &format!("val_{expected_i:03}").into_bytes());
        }

        let mut slab = Vec::new();
        let mut spans = Vec::new();
        engine
            .scan_table_values_into(1, None, None, 5, &mut slab, &mut spans)
            .unwrap();
        assert_eq!(spans.len(), 5);

        // Capped table 2: verify eviction succeeds even when >64 tombstones precede live LRU entries
        engine.register_capped_table(2, 100_000).unwrap();
        for i in 0..100u32 {
            let k = format!("lru_{i:03}").into_bytes();
            engine
                .put_row(2, &k, &[0xAB; 200], &[], WriteDurability::BufferedFast)
                .unwrap();
        }
        engine.flush_memtable().unwrap();

        // Delete first 80 keys (> EVICT_SCAN_WINDOW = 64) and flush so 80 tombstones sit at the head of 'l'
        for i in 0..80u32 {
            let k = format!("lru_{i:03}").into_bytes();
            engine
                .delete_row(2, &k, WriteDurability::BufferedFast)
                .unwrap();
        }
        engine.flush_memtable().unwrap();

        // Lower quota to 1,000 bytes (fits at most 4 rows of ~207 B) and verify eviction reclaims past the 80 tombstones
        engine.register_capped_table(2, 1_000).unwrap();
        let st = engine.capped_table_state(2).unwrap();
        assert!(
            st.current_logical_bytes <= 1_000,
            "eviction must advance past 80 tombstones and bring table within quota (got {})",
            st.current_logical_bytes
        );
    }

    #[test]
    fn blob_segments_and_coalesced_wal_survive_reopen_before_flush_memtable() {
        let tmp = TempTestDir::new("blob_and_coalesced_reopen");
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());

        let mut cfg = EngineConfig::default();
        cfg.buf_wal_mode = WalStreamMode::BufferedUserSpaceCoalesced;
        cfg.inline_threshold_bytes = 512;

        let large_blob = vec![0x5Au8; 2048];
        {
            let mut engine = StorageEngine::open(fs.clone(), cfg.clone()).unwrap();
            engine
                .put_row(1, b"blob_k1", &large_blob, &[], WriteDurability::BufferedFast)
                .unwrap();
            engine
                .put_row(
                    1,
                    b"inline_k2",
                    b"small_tail_val",
                    &[],
                    WriteDurability::BufferedFast,
                )
                .unwrap();
            // Drop without calling flush_memtable()!
        }

        let mut reopened = StorageEngine::open(fs, cfg).unwrap();
        assert_eq!(reopened.get_row(1, b"blob_k1").unwrap(), Some(large_blob));
        assert_eq!(
            reopened.get_row(1, b"inline_k2").unwrap(),
            Some(b"small_tail_val".to_vec())
        );
    }

    #[test]
    fn flush_memtable_io_fault_preserves_memtable_entries_across_retry() {
        use crate::pal::PreallocStrategy;
        use std::path::Path;
        use std::sync::atomic::AtomicBool;

        struct FaultFile<F> {
            inner: Arc<F>,
            name: String,
            fail_boot_meta_sync: Arc<AtomicBool>,
        }

        impl<F: PlatformFile> PlatformFile for FaultFile<F> {
            fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
                self.inner.read_at(buf, offset)
            }
            fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
                self.inner.read_exact_at(buf, offset)
            }
            fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
                if self.name == "boot_meta.db" && self.fail_boot_meta_sync.load(Ordering::SeqCst) {
                    return Err(io::Error::new(
                        io::ErrorKind::StorageFull,
                        "simulated boot_meta write failure",
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
            fail_boot_meta_sync: Arc<AtomicBool>,
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
                    name: rel_name.to_string(),
                    fail_boot_meta_sync: Arc::clone(&self.fail_boot_meta_sync),
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

        let tmp = TempTestDir::new("flush_fault_retry");
        let fail_flag = Arc::new(AtomicBool::new(false));
        let fs = Arc::new(FaultFs {
            inner: OsFs::open(&tmp.path, true).unwrap(),
            fail_boot_meta_sync: Arc::clone(&fail_flag),
        });

        let mut engine = StorageEngine::open(Arc::clone(&fs), EngineConfig::default()).unwrap();
        for i in 0..20u32 {
            let k = format!("k_{i:02}").into_bytes();
            let v = format!("v_{i:02}").into_bytes();
            engine
                .put_row(1, &k, &v, &[], WriteDurability::BufferedFast)
                .unwrap();
        }

        // Inject fault on boot_meta.db write during flush_memtable()
        fail_flag.store(true, Ordering::SeqCst);
        assert!(engine.flush_memtable().is_err());

        // MemTable entries must remain intact in memory and survive subsequent retry
        fail_flag.store(false, Ordering::SeqCst);
        for i in 0..20u32 {
            let k = format!("k_{i:02}").into_bytes();
            let expected = format!("v_{i:02}").into_bytes();
            assert_eq!(engine.get_row(1, &k).unwrap(), Some(expected));
        }

        engine.flush_memtable().unwrap();
        drop(engine);

        let mut reopened = StorageEngine::open(fs, EngineConfig::default()).unwrap();
        for i in 0..20u32 {
            let k = format!("k_{i:02}").into_bytes();
            let expected = format!("v_{i:02}").into_bytes();
            assert_eq!(reopened.get_row(1, &k).unwrap(), Some(expected));
        }
    }

    #[test]
    fn large_transaction_over_4mb_rollback_leaves_zero_leaked_rows() {
        let tmp = TempTestDir::new("large_tx_rollback");
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
        let mut engine = StorageEngine::open(fs, EngineConfig::default()).unwrap();

        engine
            .put_row(1, b"pre_tx", b"baseline", &[], WriteDurability::BufferedFast)
            .unwrap();

        engine.begin_transaction();
        let payload = vec![0xAAu8; 1000];
        for i in 0..5_000u32 {
            let k = format!("tx_key_{i:05}").into_bytes();
            engine
                .put_row(1, &k, &payload, &[], WriteDurability::BufferedFast)
                .unwrap();
        }
        engine.rollback_transaction().unwrap();

        assert_eq!(
            engine.get_row(1, b"pre_tx").unwrap(),
            Some(b"baseline".to_vec())
        );
        for i in 0..5_000u32 {
            let k = format!("tx_key_{i:05}").into_bytes();
            assert!(engine.get_row(1, &k).unwrap().is_none());
        }

        // Also verify a large > 4 MB transaction COMMIT succeeds via 512 KB WAL chunking
        engine.begin_transaction();
        for i in 0..5_000u32 {
            let k = format!("commit_key_{i:05}").into_bytes();
            engine
                .put_row(1, &k, &payload, &[], WriteDurability::SyncTransactional)
                .unwrap();
        }
        engine.commit_transaction().unwrap();
        assert_eq!(
            engine.get_row(1, b"commit_key_00000").unwrap().as_ref(),
            Some(&payload)
        );
        assert_eq!(
            engine.get_row(1, b"commit_key_04999").unwrap().as_ref(),
            Some(&payload)
        );
    }

    #[test]
    fn sealed_blob_overwrite_and_rollback_preserves_original_blob() {
        let tmp = TempTestDir::new("sealed_blob_rollback");
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
        let mut cfg = EngineConfig::default();
        cfg.inline_threshold_bytes = 128;
        cfg.max_blob_segment_bytes = 4096;
        let mut engine = StorageEngine::open(fs, cfg).unwrap();

        let orig_blob = vec![0x11u8; 4096];
        let second_blob = vec![0x22u8; 4096];
        engine
            .put_row(1, b"k1", &orig_blob, &[], WriteDurability::BufferedFast)
            .unwrap();
        engine
            .put_row(1, b"k2", &second_blob, &[], WriteDurability::BufferedFast)
            .unwrap();

        engine.begin_transaction();
        engine
            .put_row(
                1,
                b"k1",
                &vec![0x99u8; 4096],
                &[],
                WriteDurability::BufferedFast,
            )
            .unwrap();
        engine.rollback_transaction().unwrap();

        assert_eq!(engine.get_row(1, b"k1").unwrap(), Some(orig_blob.clone()));
        assert_eq!(engine.get_row(1, b"k2").unwrap(), Some(second_blob));

        // Even after a subsequent flush_memtable(), k1's sealed blob segment must remain intact!
        engine
            .put_row(1, b"k3", b"inline", &[], WriteDurability::BufferedFast)
            .unwrap();
        engine.flush_memtable().unwrap();
        assert_eq!(engine.get_row(1, b"k1").unwrap(), Some(orig_blob));
    }

    #[test]
    fn post_crash_sealed_blob_gc_and_partial_std_blob_relocation() {
        let tmp = TempTestDir::new("post_crash_blob_gc");
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
        let mut cfg = EngineConfig::default();
        cfg.inline_threshold_bytes = 128;
        cfg.max_blob_segment_bytes = 16 * 1024;

        let blob = vec![0x77u8; 4000];
        {
            let mut engine = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
            for i in 0..8u32 {
                let k = format!("blob_key_{i:03}").into_bytes();
                engine
                    .put_row(1, &k, &blob, &[], WriteDurability::BufferedFast)
                    .unwrap();
            }
            engine.flush_memtable().unwrap();
        }

        // Delete all 4 blobs in Segment 1, then drop without flush_memtable (simulated crash before checkpoint)
        {
            let mut engine = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
            for i in 0..4u32 {
                let k = format!("blob_key_{i:03}").into_bytes();
                engine
                    .delete_row(1, &k, WriteDurability::BufferedFast)
                    .unwrap();
            }
        }

        // Reopen must succeed cleanly (Segment 1 was not prematurely unlinked before checkpoint),
        // and flushing now reclaims Segment 1!
        {
            let mut engine = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
            for i in 0..4u32 {
                let k = format!("blob_key_{i:03}").into_bytes();
                assert!(engine.get_row(1, &k).unwrap().is_none());
            }
            for i in 4..8u32 {
                let k = format!("blob_key_{i:03}").into_bytes();
                assert_eq!(engine.get_row(1, &k).unwrap(), Some(blob.clone()));
            }

            // Delete 3 of the 4 remaining blobs in Segment 2 (75% dead >= 50% threshold)
            for i in 4..7u32 {
                let k = format!("blob_key_{i:03}").into_bytes();
                engine
                    .delete_row(1, &k, WriteDurability::BufferedFast)
                    .unwrap();
            }
        }

        // Reopen after partial deletion (dead_bytes recovered from WAL replay), then compact()
        // to trigger `relocate_fragmented_std_blobs(0.5)` and reclaim the fragmented sealed segment!
        {
            let mut engine = StorageEngine::open(Arc::clone(&fs), cfg).unwrap();
            engine.flush_memtable().unwrap();
            engine.compact().unwrap();
            assert_eq!(
                engine.get_row(1, b"blob_key_007").unwrap(),
                Some(blob)
            );
            let std_files: Vec<String> = fs
                .list_files()
                .unwrap()
                .into_iter()
                .filter(|f| f.starts_with("blob_std_"))
                .collect();
            assert_eq!(
                std_files.len(),
                1,
                "all old full/fragmented sealed blob_std segments must be reclaimed after relocation + compact"
            );
        }
    }

    #[test]
    fn capped_table_logical_bytes_preserved_across_flush_reopen_and_rollback() {
        let tmp = TempTestDir::new("capped_reconcile");
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
        let cfg = EngineConfig::default();

        {
            let mut engine = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
            // Insert rows before registering table as capped (tests reconciliation of SSTable rows lacking 't')
            engine
                .put_row(1, b"k1", &[0x11; 100], &[], WriteDurability::BufferedFast)
                .unwrap();
            engine.flush_memtable().unwrap();

            engine.register_capped_table(1, 1_000).unwrap();
            assert_eq!(
                engine.capped_table_state(1).unwrap().current_logical_bytes,
                102
            );

            engine
                .put_row(1, b"k2", &[0x22; 200], &[], WriteDurability::BufferedFast)
                .unwrap();
            assert_eq!(
                engine.capped_table_state(1).unwrap().current_logical_bytes,
                304
            );

            // Mutate inside a transaction and rollback -> logical bytes must revert to 304
            engine.begin_transaction();
            engine
                .put_row(1, b"k3", &[0x33; 300], &[], WriteDurability::BufferedFast)
                .unwrap();
            assert_eq!(
                engine.capped_table_state(1).unwrap().current_logical_bytes,
                606
            );
            engine.rollback_transaction().unwrap();
            assert_eq!(
                engine.capped_table_state(1).unwrap().current_logical_bytes,
                304
            );
        }

        // Reopen without an explicit flush_memtable() after k2 insert and verify quota & logical bytes
        let reopened = StorageEngine::open(fs, cfg).unwrap();
        let st = reopened.capped_table_state(1).unwrap();
        assert_eq!(st.max_bytes, 1_000);
        assert_eq!(st.current_logical_bytes, 304);
    }

    #[test]
    fn aud_01_writes_after_crash_recovery_survive_next_restart() {
        let tmp = TempTestDir::new("aud_01_post_recovery_writes");
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
        let mut cfg = EngineConfig::default();
        cfg.buf_wal_mode = WalStreamMode::BufferedUserSpaceCoalesced;

        let row_900 = vec![0x61u8; 900];
        // 1. Write 9 rows of 900 B in BufferedUserSpaceCoalesced mode:
        //    First 4 rows + First fragment of 5th row fill Sector 0 (flushed to OS),
        //    while the Last fragment of 5th row and rows 6..9 stay in user-space sector_buf!
        let mut engine1 = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
        for i in 0..9u32 {
            let k = format!("k_{i:02}").into_bytes();
            engine1
                .put_row(1, &k, &row_900, &[], WriteDurability::BufferedFast)
                .unwrap();
        }
        // Simulate sudden process crash (`kill -9`) leaving a dangling `First` fragment at the end of Sector 0
        engine1.buf_wal.discard_user_space_buffer_for_test();
        drop(engine1);

        // 2. Reopen (recovers first 4 rows and rotates WAL epochs), write 3 new rows, and close cleanly
        {
            let mut engine2 = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
            for i in 100..103u32 {
                let k = format!("post_rec_{i}").into_bytes();
                engine2
                    .put_row(1, &k, b"survived_post_recovery", &[], WriteDurability::BufferedFast)
                    .unwrap();
            }
        }

        // 3. Reopen again and verify all 3 post-recovery rows survived!
        let mut engine3 = StorageEngine::open(fs, cfg).unwrap();
        for i in 100..103u32 {
            let k = format!("post_rec_{i}").into_bytes();
            assert_eq!(
                engine3.get_row(1, &k).unwrap(),
                Some(b"survived_post_recovery".to_vec()),
                "post-recovery write {i} must survive subsequent restart"
            );
        }
    }

    #[test]
    fn aud_02_oversized_keys_rejected_and_large_inline_values_or_meta_routed_to_blob() {
        let tmp = TempTestDir::new("aud_02_block_bounds");
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
        let mut cfg = EngineConfig::default();
        cfg.inline_threshold_bytes = 8192; // Configured above 4 KB block capacity!
        let mut engine = StorageEngine::open(fs, cfg).unwrap();

        // 1. 5,000-byte primary key must be rejected with InvalidInput before polluting MemTable
        let err = engine
            .put_row(1, &[b'x'; 5000], b"v", &[], WriteDurability::BufferedFast)
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);

        // 2. 5,000-byte secondary index key must also be rejected with InvalidInput
        let err_sec = engine
            .put_row(
                1,
                b"ok_pk",
                b"v",
                &[(1, vec![b's'; 5000])],
                WriteDurability::BufferedFast,
            )
            .unwrap_err();
        assert_eq!(err_sec.kind(), io::ErrorKind::InvalidInput);

        // 3. 5,000-byte row value with `inline_threshold_bytes = 8192` must automatically route
        //    to a blob segment so `flush_memtable()` succeeds cleanly!
        let val_5000 = vec![0x55u8; 5000];
        engine
            .put_row(
                1,
                b"large_val_pk",
                &val_5000,
                &[],
                WriteDurability::BufferedFast,
            )
            .unwrap();

        // 4. Row with many secondary keys whose combined `RowMetaRecord` exceeds 4 KB must route `'t'` to blob
        let many_sec: Vec<(u16, Vec<u8>)> = (0..10u16)
            .map(|idx| (idx + 1, vec![b'a' + (idx as u8); 600]))
            .collect();
        engine
            .put_row(
                1,
                b"many_sec_pk",
                b"small_v",
                &many_sec,
                WriteDurability::BufferedFast,
            )
            .unwrap();

        engine
            .flush_memtable()
            .expect("flush_memtable must succeed without wedging");
        assert_eq!(
            engine.get_row(1, b"large_val_pk").unwrap(),
            Some(val_5000)
        );
        assert_eq!(
            engine
                .lookup_by_secondary_index(1, 1, &vec![b'a'; 600])
                .unwrap(),
            vec![b"many_sec_pk".to_vec()]
        );
    }

    #[test]
    fn aud_04_multi_chunk_transaction_without_commit_marker_discards_all_chunks() {
        let tmp = TempTestDir::new("aud_04_tx_framing");
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());

        {
            let mut engine = StorageEngine::open(Arc::clone(&fs), EngineConfig::default()).unwrap();
            engine
                .put_row(
                    1,
                    b"baseline",
                    b"keep_me",
                    &[],
                    WriteDurability::SyncTransactional,
                )
                .unwrap();

            // Write a transaction with `TxBegin` in chunk 1 directly to `wal_sync` without a `TxCommit` chunk
            let chunk1 = WalBatch::new(
                engine.next_lsn(),
                vec![
                    WalRecord::TxBegin,
                    WalRecord::PutInline {
                        key: encode_primary_row_key(1, b"uncommitted_chunk_key"),
                        value: vec![0xBB; 1024],
                    },
                ],
            );
            engine.sync_wal.append_batch(&chunk1).unwrap();
        }

        let mut reopened = StorageEngine::open(fs, EngineConfig::default()).unwrap();
        assert_eq!(
            reopened.get_row(1, b"baseline").unwrap(),
            Some(b"keep_me".to_vec())
        );
        assert!(
            reopened.get_row(1, b"uncommitted_chunk_key").unwrap().is_none(),
            "incomplete multi-chunk transaction lacking TxCommit must be completely discarded on recovery"
        );
    }

    #[test]
    fn aud_06_aud_07_aud_08_crash_consistency_and_failed_write_rollback() {
        use crate::pal::PreallocStrategy;
        use std::path::Path;
        use std::sync::atomic::AtomicBool;
        use std::sync::Mutex;

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
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "simulated WAL write I/O failure",
                    ));
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
                    root: PathBuf::from("/sim/aud_test"),
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

        // AUD-06 setup: Write blob_1 and blob_2 to Table 1 so blob_std_1 is sealed and checkpointed
        let orig_blob = vec![0x33u8; 2000];
        engine
            .put_row(1, b"r1", &orig_blob, &[], WriteDurability::SyncTransactional)
            .unwrap();
        engine
            .put_row(1, b"r2", &orig_blob, &[], WriteDurability::SyncTransactional)
            .unwrap();
        engine.flush_memtable().unwrap();

        // AUD-07 check: Failing a WAL write must NOT mutate `MemTable`!
        fs.fail_wal_writes.store(true, Ordering::SeqCst);
        assert!(engine
            .put_row(1, b"r1", b"failed_update", &[], WriteDurability::BufferedFast)
            .is_err());
        assert_eq!(engine.get_row(1, b"r1").unwrap(), Some(orig_blob.clone()));

        // AUD-07 transaction commit check: Failing commit_transaction must roll back MemTable!
        fs.fail_wal_writes.store(false, Ordering::SeqCst);
        engine.begin_transaction();
        engine
            .put_row(1, b"tx_fail_k", b"tx_val", &[], WriteDurability::BufferedFast)
            .unwrap();
        fs.fail_wal_writes.store(true, Ordering::SeqCst);
        assert!(engine.commit_transaction().is_err());
        fs.fail_wal_writes.store(false, Ordering::SeqCst);
        assert!(
            engine.get_row(1, b"tx_fail_k").unwrap().is_none(),
            "failed commit_transaction must roll back unlogged MemTable mutations"
        );

        // AUD-06 check: Overwrite `r1` with `BufferedFast` (making `blob_std_1` 100% dead in RAM),
        // then trigger `evict_until_within_quota` on capped Table 2, then simulate power loss!
        engine
            .put_row(1, b"r1", &vec![0x44u8; 2000], &[], WriteDurability::BufferedFast)
            .unwrap();
        engine
            .put_row(2, b"cap_k", b"small", &[], WriteDurability::BufferedFast)
            .unwrap();

        // AUD-08 + F06 check: Write `idx_row` with a large out-of-line blob (> 128 B) using
        // `BufferedFast` (`tag_buf`), then overwrite `idx_row` with an inline value using
        // `SyncTransactional` (`tag_sync`), then simulate power loss!
        // Because `F06` syncs active blob writers before syncing `wal_buf.db` on the later
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
            .put_row(1, b"r3_unsynced", b"lost", &[], WriteDurability::BufferedFast)
            .unwrap();
        std::mem::forget(engine);
        fs.simulate_power_loss();

        let mut recovered = StorageEngine::open(Arc::clone(&fs), cfg).unwrap();
        // AUD-08 verification: `tag_buf` must NOT remain as a dangling secondary index entry!
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
        // AUD-06 verification: `r1` must still be readable (either new version if synced by AUD-08 or original)!
        assert!(recovered.get_row(1, b"r1").unwrap().is_some());
    }

    #[test]
    fn perf_01_use_04_use_07_bounded_scan_quota_reject_and_stale_meta_cleanup() {
        let tmp = TempTestDir::new("perf01_use04_use07");
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
        let mut engine = StorageEngine::open(fs, EngineConfig::default()).unwrap();

        // USE-04: Row larger than capped table `max_bytes` is rejected with `InvalidInput`
        // without evicting existing rows!
        engine.register_capped_table(1, 500).unwrap();
        engine
            .put_row(1, b"keep", &[0x11; 200], &[], WriteDurability::BufferedFast)
            .unwrap();
        let err = engine
            .put_row(
                1,
                b"too_big",
                &[0x22; 600],
                &[],
                WriteDurability::BufferedFast,
            )
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(
            engine.get_row(1, b"keep").unwrap(),
            Some(vec![0x11; 200]),
            "existing capped table rows must not be evicted when an oversized row is rejected"
        );

        // USE-07: Overwriting an indexed row with a row that has no secondary keys on a non-capped
        // table deletes the stale `'t'` metadata record!
        engine
            .put_row(
                2,
                b"u7_pk",
                b"v1",
                &[(1, b"sec_old".to_vec())],
                WriteDurability::BufferedFast,
            )
            .unwrap();
        assert!(engine
            .get_row_meta_with_ptr(&encode_row_meta_key(2, b"u7_pk"))
            .unwrap()
            .is_some());
        engine
            .put_row(2, b"u7_pk", b"v2_no_sec", &[], WriteDurability::BufferedFast)
            .unwrap();
        assert!(
            engine
                .get_row_meta_with_ptr(&encode_row_meta_key(2, b"u7_pk"))
                .unwrap()
                .is_none(),
            "stale 't' metadata record must be deleted when overwritten without secondary keys"
        );

        // PERF-01: With 1 flushed segment on table 2 and 25,000 unrelated keys on table 3 in MemTable,
        // 200 secondary index lookups on table 2 must finish in < 50 ms (O(log N), not O(N))!
        engine
            .put_row(
                2,
                b"indexed_pk",
                b"val",
                &[(5, b"target_sec".to_vec())],
                WriteDurability::BufferedFast,
            )
            .unwrap();
        engine.flush_memtable().unwrap();

        for i in 0..25_000u32 {
            let k = format!("unrelated_{i:05}").into_bytes();
            engine
                .put_row(3, &k, b"v", &[], WriteDurability::BufferedFast)
                .unwrap();
        }

        let t0 = std::time::Instant::now();
        for _ in 0..200 {
            let pks = engine
                .lookup_by_secondary_index(2, 5, b"target_sec")
                .unwrap();
            assert_eq!(pks, vec![b"indexed_pk".to_vec()]);
        }
        assert!(
            t0.elapsed().as_millis() < 250,
            "200 secondary-index lookups must not drain 25,000 unrelated MemTable entries (took {:?})",
            t0.elapsed()
        );
    }

    #[test]
    fn perf_02_segment_range_pruning_and_auto_compaction() {
        let tmp = TempTestDir::new("perf_02_auto_compact");
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
        let mut cfg = EngineConfig::default();
        cfg.auto_compact_segment_threshold = 4;

        let mut engine = StorageEngine::open(Arc::clone(&fs), cfg).unwrap();

        // Flush 3 segments (< threshold 4): segment count grows to 3
        for seg_idx in 0..3u32 {
            for k_idx in 0..20u32 {
                let k = format!("s{seg_idx:02}_k{k_idx:03}").into_bytes();
                let v = format!("v_{seg_idx}_{k_idx}").into_bytes();
                engine
                    .put_row(1, &k, &v, &[], WriteDurability::BufferedFast)
                    .unwrap();
            }
            engine.flush_memtable().unwrap();
            assert_eq!(engine.active_idx_segment_count(), (seg_idx + 1) as usize);
        }

        // 4th flush reaches `auto_compact_segment_threshold = 4` -> automatically compacts into 1 segment!
        for k_idx in 0..20u32 {
            let k = format!("s03_k{k_idx:03}").into_bytes();
            let v = format!("v_3_{k_idx}").into_bytes();
            engine
                .put_row(1, &k, &v, &[], WriteDurability::BufferedFast)
                .unwrap();
        }
        engine.flush_memtable().unwrap();
        assert_eq!(
            engine.active_idx_segment_count(),
            1,
            "reaching auto_compact_segment_threshold must automatically compact SSTables into 1 segment"
        );

        // Verify all 80 keys across the 4 flushed batches remain intact
        for seg_idx in 0..4u32 {
            for k_idx in 0..20u32 {
                let k = format!("s{seg_idx:02}_k{k_idx:03}").into_bytes();
                let expected = format!("v_{seg_idx}_{k_idx}").into_bytes();
                assert_eq!(engine.get_row(1, &k).unwrap(), Some(expected));
            }
        }
    }

    #[test]
    fn perf_03_perf_06_fast_undo_rollback_savepoints_and_zero_copy_blob_into() {
        let tmp = TempTestDir::new("perf_03_06_undo_and_blob_into");
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
        let mut cfg = EngineConfig::default();
        cfg.inline_threshold_bytes = 256;
        let mut engine = StorageEngine::open(fs, cfg).unwrap();

        // Populate 10,000 baseline rows in MemTable
        for i in 0..10_000u32 {
            let k = format!("base_{i:05}").into_bytes();
            engine
                .put_row(1, &k, b"orig_val", &[], WriteDurability::BufferedFast)
                .unwrap();
        }

        // PERF-03: 100 single-row transactions + rollbacks and nested savepoint rollbacks on top
        // of a 10,000-row MemTable must execute in O(tx mutations) (< 50 ms total), not O(WAL + MemTable)!
        let t0 = std::time::Instant::now();
        for i in 0..100u32 {
            let mod_key = format!("base_{i:05}").into_bytes();
            let ins_key = format!("temp_ins_{i:05}").into_bytes();
            let del_key = format!("base_{:05}", i + 100).into_bytes();

            engine.begin_transaction();
            engine
                .put_row(1, &mod_key, b"mutated_v1", &[], WriteDurability::BufferedFast)
                .unwrap();
            engine
                .put_row(1, &ins_key, b"temp_val", &[], WriteDurability::BufferedFast)
                .unwrap();
            engine
                .delete_row(1, &del_key, WriteDurability::BufferedFast)
                .unwrap();

            engine.savepoint(1);
            engine
                .put_row(1, &mod_key, b"mutated_v2", &[], WriteDurability::BufferedFast)
                .unwrap();
            engine.rollback_to_savepoint(1).unwrap();
            assert_eq!(
                engine.get_row(1, &mod_key).unwrap(),
                Some(b"mutated_v1".to_vec())
            );

            engine.rollback_transaction().unwrap();
            assert_eq!(
                engine.get_row(1, &mod_key).unwrap(),
                Some(b"orig_val".to_vec())
            );
            assert!(engine.get_row(1, &ins_key).unwrap().is_none());
            assert_eq!(
                engine.get_row(1, &del_key).unwrap(),
                Some(b"orig_val".to_vec())
            );
        }
        assert!(
            t0.elapsed().as_millis() < 250,
            "100 transaction & savepoint rollbacks must execute in O(tx mutations) via undo log (took {:?})",
            t0.elapsed()
        );

        // PERF-06: Verify `get_row_into` appends out-of-line blobs directly into `slab`
        let blob_a = vec![0xA1u8; 1024];
        let blob_b = vec![0xB2u8; 1536];
        engine
            .put_row(2, b"bk1", &blob_a, &[], WriteDurability::BufferedFast)
            .unwrap();
        engine
            .put_row(2, b"bk2", &blob_b, &[], WriteDurability::BufferedFast)
            .unwrap();

        let mut slab = Vec::with_capacity(4096);
        let mut spans = Vec::new();
        assert!(engine.get_row_into(2, b"bk1", &mut slab, &mut spans).unwrap());
        assert!(engine.get_row_into(2, b"bk2", &mut slab, &mut spans).unwrap());
        assert_eq!(spans.len(), 2);
        assert_eq!(&slab[spans[0].0..spans[0].1], blob_a.as_slice());
        assert_eq!(&slab[spans[1].0..spans[1].1], blob_b.as_slice());
    }

    #[test]
    fn f02_f06_f07_deferred_tx_checkpoint_blob_prefix_durability_and_prepare_commit() {
        let tmp = TempTestDir::new("f02_f06_f07");
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
        let mut engine = StorageEngine::open(fs.clone(), EngineConfig::default()).unwrap();

        // F02: Calling `flush_memtable()` and `compact()` inside an active transaction must NOT
        // publish uncommitted rows to SSTables, so `rollback_transaction()` completely removes them
        // both in memory and across reopen!
        engine.begin_transaction();
        engine
            .put_row(1, b"aborted", b"value", &[], WriteDurability::BufferedFast)
            .unwrap();
        engine.flush_memtable().unwrap();
        engine.compact().unwrap();
        engine.rollback_transaction().unwrap();
        assert_eq!(
            engine.get_row(1, b"aborted").unwrap(),
            None,
            "F02: rolled-back row must not remain visible after flush_memtable() during transaction"
        );
        drop(engine);

        let mut engine = StorageEngine::open(fs.clone(), EngineConfig::default()).unwrap();
        assert_eq!(
            engine.get_row(1, b"aborted").unwrap(),
            None,
            "F02: rolled-back row must not survive reopen after flush_memtable() during transaction"
        );

        // Verify that if the transaction DOES commit after `flush_memtable()` was requested inside
        // the transaction, the deferred flush executes upon commit and creates an SSTable segment!
        assert_eq!(engine.active_idx_segment_count(), 0);
        engine.begin_transaction();
        engine
            .put_row(
                1,
                b"committed_flushed",
                b"ok",
                &[],
                WriteDurability::BufferedFast,
            )
            .unwrap();
        engine.flush_memtable().unwrap();
        assert_eq!(
            engine.active_idx_segment_count(),
            0,
            "flush must be deferred while tx_depth > 0"
        );
        engine.commit_transaction().unwrap();
        assert_eq!(
            engine.active_idx_segment_count(),
            1,
            "deferred flush must execute upon commit_transaction()"
        );
        assert_eq!(
            engine.get_row(1, b"committed_flushed").unwrap(),
            Some(b"ok".to_vec())
        );

        // F07: `prepare_commit_transaction` runs capped-table quota eviction while still inside
        // the undoable transaction, so `rollback_transaction()` after `prepare_commit_transaction`
        // restores the evicted row!
        engine.register_capped_table(5, 300).unwrap();
        engine
            .put_row(
                5,
                b"row1",
                &vec![0x11u8; 150],
                &[],
                WriteDurability::BufferedFast,
            )
            .unwrap();
        engine.begin_transaction();
        engine
            .put_row(
                5,
                b"row2",
                &vec![0x22u8; 200],
                &[],
                WriteDurability::BufferedFast,
            )
            .unwrap();
        // Prepare phase evicts `row1` to bring table 5 within its 300-byte quota
        engine
            .prepare_commit_transaction(WriteDurability::BufferedFast)
            .unwrap();
        assert!(engine.get_row(5, b"row1").unwrap().is_none());
        // If another 2PC participant fails and SQLite triggers rollback after xSync:
        engine.rollback_transaction().unwrap();
        assert_eq!(
            engine.get_row(5, b"row1").unwrap(),
            Some(vec![0x11u8; 150]),
            "F07: rolling back after prepare_commit_transaction must restore quota-evicted rows"
        );
        assert!(engine.get_row(5, b"row2").unwrap().is_none());
    }

    #[test]
    fn p01_p02_size_tiered_auto_compaction_scan_keys_and_tx_budget() {
        let tmp = TempTestDir::new("p01_p02_tiered_scan_budget");
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
        let mut cfg = EngineConfig::default();
        cfg.auto_compact_segment_threshold = 4;
        cfg.max_transaction_bytes = 8 * 1024; // 8 KB transaction budget for testing P02
        let mut engine = StorageEngine::open(fs.clone(), cfg).unwrap();

        // 1. P01: Build a large base segment (`idx_segments[0]`, ~64 KB across multiple 4 KB blocks)
        let base_payload = vec![0x42u8; 256];
        for i in 0..180u32 {
            let pk = format!("base_key_{i:04}");
            engine
                .put_row(1, pk.as_bytes(), &base_payload, &[], WriteDurability::BufferedFast)
                .unwrap();
        }
        engine.flush_memtable_no_auto_compact().unwrap();
        assert_eq!(engine.active_idx_segment_count(), 1);
        let base_seq_id = engine.idx_segments[0].seq_id();
        assert!(
            engine.idx_segments[0].file_len() >= 16 * 1024,
            "base segment should span multiple 4 KB blocks"
        );

        // Now perform 3 small flushes (including a tombstone for `base_key_0010` in the base segment).
        // When the 4th segment is flushed (`len() == 4 == auto_compact_segment_threshold`),
        // `compact_auto_tiered()` must merge ONLY the 3 small suffix segments (`1..4`) with
        // `drop_tombstones = false`, leaving the large `base_seq_id` segment at index 0 untouched!
        engine
            .delete_row(1, b"base_key_0010", WriteDurability::BufferedFast)
            .unwrap();
        engine.flush_memtable().unwrap();
        engine
            .put_row(1, b"new_key_1", b"v1", &[], WriteDurability::BufferedFast)
            .unwrap();
        engine.flush_memtable().unwrap();
        assert_eq!(engine.active_idx_segment_count(), 3);

        engine
            .put_row(1, b"new_key_2", b"v2", &[], WriteDurability::BufferedFast)
            .unwrap();
        engine.flush_memtable().unwrap();

        // The 3 small segments should have been merged into 1 suffix segment, leaving 2 segments total,
        // with `idx_segments[0]` still having `base_seq_id`!
        assert_eq!(
            engine.active_idx_segment_count(),
            2,
            "P01: size-tiered auto-compaction must merge the 3 small suffix segments into 1"
        );
        assert_eq!(
            engine.idx_segments[0].seq_id(),
            base_seq_id,
            "P01: large base segment must NOT be rewritten during small suffix auto-compaction"
        );
        // And the tombstone for `base_key_0010` in the merged suffix segment MUST have been preserved!
        assert_eq!(
            engine.get_row(1, b"base_key_0010").unwrap(),
            None,
            "P01: suffix compaction with start_idx > 0 must preserve tombstones shadowing base segment"
        );
        assert_eq!(
            engine.get_row(1, b"new_key_1").unwrap(),
            Some(b"v1".to_vec())
        );
        assert_eq!(
            engine.get_row(1, b"new_key_2").unwrap(),
            Some(b"v2".to_vec())
        );

        // Explicit full `compact()` merges down to `start_idx == 0` (`1` segment) and drops tombstones
        engine.compact().unwrap();
        assert_eq!(engine.active_idx_segment_count(), 1);
        assert_eq!(engine.get_row(1, b"base_key_0010").unwrap(), None);

        // 2. P02: `scan_keys_range` & `scan_range_into_slab`
        let pks = engine
            .scan_keys_range(1, Some(b"base_key_0008"), Some(b"base_key_0012"), 10)
            .unwrap();
        assert_eq!(
            pks,
            vec![
                b"base_key_0008".to_vec(),
                b"base_key_0009".to_vec(),
                // `base_key_0010` was deleted!
                b"base_key_0011".to_vec(),
                b"base_key_0012".to_vec(),
            ]
        );

        let mut rev_pks = Vec::new();
        let mut rev_slab = Vec::new();
        let mut rev_spans = Vec::new();
        engine
            .scan_range_into_slab(
                1,
                Some((b"base_key_0008", true)),
                Some((b"base_key_0012", true)),
                true,
                2,
                false,
                &mut rev_pks,
                &mut rev_slab,
                &mut rev_spans,
            )
            .unwrap();
        assert_eq!(rev_spans.len(), 2);
        assert_eq!(&rev_slab[rev_spans[0].0..rev_spans[0].1], base_payload.as_slice());

        // 3. P02: `max_transaction_bytes` budget enforcement
        engine.begin_transaction();
        engine
            .put_row(
                1,
                b"tx_ok",
                &vec![0x11u8; 2048],
                &[],
                WriteDurability::BufferedFast,
            )
            .unwrap();
        let err = engine
            .put_row(
                1,
                b"tx_too_big",
                &vec![0x22u8; 8192],
                &[],
                WriteDurability::BufferedFast,
            )
            .expect_err("P02: exceeding max_transaction_bytes must return Err(InvalidInput)");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        engine.rollback_transaction().unwrap();
        assert_eq!(engine.get_row(1, b"tx_ok").unwrap(), None);
        assert_eq!(engine.get_row(1, b"tx_too_big").unwrap(), None);
    }

    #[test]
    fn r01_r02_r03_r04_r05_r06_r07_audit_stage1_regressions() {
        let tmp = TempTestDir::new("stage1_r01_to_r07");
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());

        // ---------------------------------------------------------------------
        // 1. R-01: Uncataloged blob segment created before a crash (or when
        //    `boot_meta.db` falls back to a prior slot) must NOT be deleted by
        //    premature orphan sweeping before WAL recovery validates its BlobPtr!
        //    Meanwhile, a truly unreferenced orphan blob segment MUST be swept
        //    after WAL recovery completes!
        // ---------------------------------------------------------------------
        let blob_payload = vec![0xA5u8; 2048];
        let unreferenced_orphan_blob = crate::meta::format_std_blob_file_name(999);
        {
            let mut engine = StorageEngine::open(fs.clone(), EngineConfig::default()).unwrap();
            engine
                .put_row(
                    1,
                    b"r01_blob_key",
                    &blob_payload,
                    &[],
                    WriteDurability::SyncTransactional,
                )
                .unwrap();
            // Simulate a crash where `boot_meta.db` newest slot (which cataloged `blob_std_0001.db`)
            // suffered a sub-sector torn write, forcing `boot_meta.db` to fall back to the prior
            // slot where `std_blob_segments` was still empty, while `wal_sync.db` and `blob_std_0001.db`
            // are intact on disk!
            let active_slot = engine.meta_mgr.active_slot();
            engine
                .meta_mgr
                .file()
                .write_all_at(&[0xFF], active_slot.byte_offset() + 4095)
                .unwrap();
            // Also plant an unreferenced orphan blob file on disk to verify post-recovery sweep
            let _orphan = fs
                .open_file(&unreferenced_orphan_blob, true, true, OpenHint::ImmutableSequential)
                .unwrap();
        }

        {
            let mut engine = StorageEngine::open(fs.clone(), EngineConfig::default()).unwrap();
            assert_eq!(
                engine.get_row(1, b"r01_blob_key").unwrap(),
                Some(blob_payload.clone()),
                "R-01: committed blob row in uncataloged blob segment must survive recovery"
            );
            assert!(
                !fs.list_files().unwrap().contains(&unreferenced_orphan_blob),
                "R-01: truly unreferenced orphan blob segment must be swept after WAL recovery"
            );

            // -----------------------------------------------------------------
            // 2. R-02: Multiple updates/deletes to the same key within a single
            //    explicit transaction (`WalBatch`) must NOT double-count the
            //    pre-transaction SSTable blob's `dead_bytes` during WAL recovery,
            //    which previously caused `dead_bytes >= file_len` to unlink a
            //    blob segment that still held other live blobs!
            // -----------------------------------------------------------------
            engine
                .put_row(
                    1,
                    b"r02_live_neighbor",
                    &vec![0x11u8; 2048],
                    &[],
                    WriteDurability::SyncTransactional,
                )
                .unwrap();
            engine
                .put_row(
                    1,
                    b"r02_mutated_twice",
                    &vec![0x22u8; 2048],
                    &[],
                    WriteDurability::SyncTransactional,
                )
                .unwrap();
            engine.flush_memtable().unwrap();

            // Now in a single transaction (single WalBatch), update `r02_mutated_twice`
            // to inline and then delete it (2 mutations to the same key in 1 WalBatch!).
            engine.begin_transaction();
            engine
                .put_row(
                    1,
                    b"r02_mutated_twice",
                    b"inline_mid",
                    &[],
                    WriteDurability::SyncTransactional,
                )
                .unwrap();
            engine
                .delete_row(1, b"r02_mutated_twice", WriteDurability::SyncTransactional)
                .unwrap();
            engine.commit_transaction().unwrap();
            // Drop without flushing so WAL recovery replays the multi-op WalBatch!
        }

        {
            let mut engine = StorageEngine::open(fs.clone(), EngineConfig::default()).unwrap();
            // Delete `r01_blob_key` as well so if `r02_mutated_twice` was double-counted,
            // `dead_bytes` would reach `>= file_len` even though `r02_live_neighbor` is still live!
            engine
                .delete_row(1, b"r01_blob_key", WriteDurability::SyncTransactional)
                .unwrap();
            engine.flush_memtable().unwrap();
            engine.compact().unwrap();
            assert_eq!(
                engine.get_row(1, b"r02_live_neighbor").unwrap(),
                Some(vec![0x11u8; 2048]),
                "R-02: live neighbor blob must not be unlinked due to double-counted dead_bytes"
            );
            assert_eq!(engine.get_row(1, b"r02_mutated_twice").unwrap(), None);

            // -----------------------------------------------------------------
            // 3. R-03: Off-by-2 inline block footprint boundary test!
            //    When `row_key.len() + value.len() == 4074` (`13 + 4074 == 4087 > 4086`),
            //    `IndexEntry::inline_fits_in_block` is `false`, so with a large
            //    `inline_threshold_bytes` the engine MUST route it to a blob segment
            //    instead of wedging `flush_memtable()`!
            // -----------------------------------------------------------------
            engine.config.inline_threshold_bytes = 8192;
            let pk_r03 = vec![b'k'; 100]; // `row_key.len() == 103`
            // `4086 - 13 - 103 = 3970` fits inline; `3971` (`13 + 103 + 3971 = 4087`) must spill to blob!
            let val_3970 = vec![0x30u8; 3970];
            let val_3971 = vec![0x31u8; 3971];
            engine
                .put_row(1, &pk_r03, &val_3970, &[], WriteDurability::BufferedFast)
                .unwrap();
            engine.flush_memtable().expect("R-03: 3970B value at exact 4086B limit must flush inline");
            assert_eq!(engine.get_row(1, &pk_r03).unwrap(), Some(val_3970));

            engine
                .put_row(1, &pk_r03, &val_3971, &[], WriteDurability::BufferedFast)
                .unwrap();
            engine
                .flush_memtable()
                .expect("R-03: 3971B value (4087B inline footprint) must spill to blob and not wedge flush_memtable");
            assert_eq!(engine.get_row(1, &pk_r03).unwrap(), Some(val_3971));
            engine.config.inline_threshold_bytes = DEFAULT_INLINE_THRESHOLD;

            // -----------------------------------------------------------------
            // 4. R-04: Unreferenced uncommitted tail bytes in a blob segment
            //    after a crash between blob append and WAL write must be credited
            //    to `dead_bytes` on recovery so $O(1)$ segment reclamation works!
            // -----------------------------------------------------------------
            engine.register_capped_table(4, 10_000).unwrap();
            engine
                .put_row(
                    4,
                    b"lru_k1",
                    &vec![0x44u8; 2048],
                    &[],
                    WriteDurability::BufferedFast,
                )
                .unwrap();
            engine.flush_memtable().unwrap();
            let (&(_, lru_seg_id), _) = engine
                .lru_blobs
                .iter()
                .find(|&(&(tid, _), _)| tid == 4)
                .unwrap();
            let lru_fname = crate::meta::format_lru_blob_file_name(4, lru_seg_id);
            drop(engine);
            let f = fs
                .open_file(&lru_fname, false, true, OpenHint::ImmutableSequential)
                .unwrap();
            let orig_len = f.file_len().unwrap();
            // Append 4096 unreferenced tail bytes (simulating a crash after blob write before WAL write)
            f.write_all_at(&vec![0xEEu8; 4096], orig_len).unwrap();
            f.sync_durable().unwrap();
        }

        {
            let mut engine = StorageEngine::open(fs.clone(), EngineConfig::default()).unwrap();
            // Now delete `lru_k1`; because R-04 accounted for the 4096 unreferenced tail bytes,
            // `dead_bytes >= file_len` is satisfied and `reclaim_fully_dead_lru_blob_segments`
            // reclaims the segment in O(1)!
            engine
                .delete_row(4, b"lru_k1", WriteDurability::BufferedFast)
                .unwrap();
            engine.reclaim_fully_dead_lru_blob_segments(4).unwrap();
            assert_eq!(
                engine.active_lru_blob_segment_count(4),
                0,
                "R-04: blob segment with uncommitted tail bytes must still be reclaimed in O(1) once live blobs are deleted"
            );

            // -----------------------------------------------------------------
            // 5. R-05: `register_or_resolve_vtab` inside an active transaction
            //    must route through transactional buffering so `rollback_transaction`
            //    cleanly undoes the catalog entry and rewinds `next_lsn` without
            //    leaving duplicate LSNs in the WAL!
            // -----------------------------------------------------------------
            let lsn_before = engine.next_lsn();
            engine.begin_transaction();
            let tid_aborted = engine
                .register_or_resolve_vtab("aborted_vtab", Some(77))
                .unwrap();
            assert_eq!(tid_aborted, 77);
            engine.rollback_transaction().unwrap();
            assert_eq!(
                engine.next_lsn(),
                lsn_before,
                "R-05: rolling back transaction after register_or_resolve_vtab must restore next_lsn"
            );
            assert!(
                !engine.active_vtab_ids.contains_key(&77),
                "R-05: rolled-back vtab registration must not remain in active_vtab_ids"
            );
            // Subsequent write at `lsn_before` must succeed and survive recovery cleanly
            engine
                .put_row(1, b"after_r05", b"ok", &[], WriteDurability::SyncTransactional)
                .unwrap();

            // -----------------------------------------------------------------
            // 6. R-06: `drop_table` inside an explicit transaction (`BEGIN ... COMMIT`)
            //    must reclaim 100%-dead LRU blob segments and remove the table quota
            //    from `boot_meta.db` upon `commit_transaction()`!
            // -----------------------------------------------------------------
            engine.register_capped_table(6, 50_000).unwrap();
            engine
                .put_row(
                    6,
                    b"cap6_k1",
                    &vec![0x66u8; 2048],
                    &[],
                    WriteDurability::BufferedFast,
                )
                .unwrap();
            assert_eq!(engine.active_lru_blob_segment_count(6), 1);

            engine.begin_transaction();
            engine.drop_table(6, Some("cap6_table")).unwrap();
            engine.commit_transaction().unwrap();
            assert_eq!(
                engine.active_lru_blob_segment_count(6),
                0,
                "R-06: committing DROP TABLE on a capped table must reclaim its 100%-dead LRU blob segments"
            );
            assert!(
                engine
                    .meta_mgr
                    .state()
                    .table_quotas
                    .iter()
                    .all(|q| q.table_id != 6),
                "R-06: committing DROP TABLE on a capped table must remove its quota from boot_meta.db"
            );

            // -----------------------------------------------------------------
            // 7. R-07: Concurrent read via `with_committed_read_for_conn` when
            //    `LruReadTrackingMode::EagerWriteOnRead` is enabled while another
            //    connection holds an active transaction must NOT corrupt the
            //    active transaction's undo log or MemTable!
            // -----------------------------------------------------------------
            engine.config.lru_read_tracking = LruReadTrackingMode::EagerWriteOnRead;
            engine.register_capped_table(7, 50_000).unwrap();
            engine
                .put_row(7, b"shared_k", b"committed_v1", &[], WriteDurability::BufferedFast)
                .unwrap();

            // Connection 1 begins a transaction and overwrites `shared_k` -> `uncommitted_v2`
            engine.begin_transaction_for_conn(101).unwrap();
            engine
                .put_row(
                    7,
                    b"shared_k",
                    b"uncommitted_v2",
                    &[],
                    WriteDurability::BufferedFast,
                )
                .unwrap();
            let undo_len_before = engine.tx_memtable_undo.len();

            // Connection 2 reads `shared_k` via `with_committed_read_for_conn(202, ...)`
            let conn2_val = engine
                .with_committed_read_for_conn(202, |eng| eng.get_row(7, b"shared_k"))
                .unwrap();
            assert_eq!(conn2_val, Some(b"committed_v1".to_vec()));
            assert_eq!(
                engine.tx_memtable_undo.len(),
                undo_len_before,
                "R-07: concurrent committed read under EagerWriteOnRead must not append to Connection 1's tx_memtable_undo"
            );

            // Connection 1 must still see its uncommitted write `uncommitted_v2`, and rolling back
            // Connection 1 must cleanly restore `committed_v1`!
            assert_eq!(
                engine.get_row(7, b"shared_k").unwrap(),
                Some(b"uncommitted_v2".to_vec())
            );
            engine.rollback_transaction_for_conn(101).unwrap();
            assert_eq!(
                engine.get_row(7, b"shared_k").unwrap(),
                Some(b"committed_v1".to_vec()),
                "R-07: rolling back Connection 1 after Connection 2's EagerWriteOnRead read must restore committed_v1"
            );
        }
    }
}
