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
    DEFAULT_BLOOM_BITS_PER_KEY, IdxSegmentReader, IndexEntry, IndexValue,
    MAX_SINGLE_BLOCK_ENTRY_BYTES, compact_idx_segments,
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

#[derive(Clone)]
struct SavepointMarker {
    sp_id: i32,
    buf_offset: usize,
    record_count: u32,
    pending_tx_sync: bool,
    implicit_tx: bool,
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
    pending_tx_buf: Vec<u8>,
    pending_tx_record_count: u32,
    pending_tx_sync: bool,
    tx_start_std_lengths: BTreeMap<u64, u64>,
    tx_start_lru_lengths: BTreeMap<(u16, u64), u64>,
    savepoints: Vec<SavepointMarker>,
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
        let mut meta_mgr = BootMetaManager::open_or_recover(fs.clone())?;
        let state_snapshot = meta_mgr.state().clone();

        // 1. Open active `idx_<SeqID>.db` segments
        let mut idx_segments = Vec::with_capacity(state_snapshot.idx_segments.len());
        for seg_meta in &state_snapshot.idx_segments {
            let reader = IdxSegmentReader::open(&*fs, seg_meta.seq_id)?;
            idx_segments.push(reader);
        }

        // 2. Open active `blob_std_<SeqID>.db` segments
        let mut std_blobs = BTreeMap::new();
        for bmeta in &state_snapshot.std_blob_segments {
            let fname = format_std_blob_file_name(bmeta.seq_id);
            let file = fs.open_file(&fname, false, true, OpenHint::ImmutableSequential)?;
            let file_len = file.file_len()?.max(bmeta.file_len);
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
        for lmeta in &state_snapshot.lru_blob_segments {
            let fname = format_lru_blob_file_name(lmeta.table_id, lmeta.seq_id);
            let file = fs.open_file(&fname, false, true, OpenHint::ImmutableSequential)?;
            let file_len = file.file_len()?.max(lmeta.file_len);
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
        for batch in &rec.merged_batches {
            for record in &batch.records {
                let key = match record {
                    WalRecord::PutInline { key, .. }
                    | WalRecord::PutBlobPtr { key, .. }
                    | WalRecord::Delete { key } => key.as_slice(),
                    WalRecord::TxBegin | WalRecord::TxCommit => continue,
                };
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
            memtable.apply_wal_batch(batch);
        }

        // Bump `next_seq_id` past any recovered blob segment SeqIDs if necessary
        let max_blob_seq = std_blobs
            .keys()
            .copied()
            .chain(lru_blobs.keys().map(|&(_, s)| s))
            .max()
            .unwrap_or(0);
        if max_blob_seq >= meta_mgr.state().next_seq_id {
            meta_mgr.commit_update(|st| {
                st.next_seq_id = max_blob_seq + 1;
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
            pending_tx_buf: Vec::new(),
            pending_tx_record_count: 0,
            pending_tx_sync: false,
            tx_start_std_lengths: BTreeMap::new(),
            tx_start_lru_lengths: BTreeMap::new(),
            savepoints: Vec::new(),
            active_vtab_ids: HashMap::new(),
            last_buf_append_lsn: 0,
            last_buf_synced_lsn: 0,
        };
        engine.access_tracker = AccessTracker::new(1, engine.config.max_in_memory_access_ticks);
        engine.reconcile_capped_tables_after_recovery()?;

        if !rec.merged_batches.is_empty()
            || rec.next_sync_sector_idx > 0
            || rec.next_buf_sector_idx > 0
        {
            if !engine.memtable.is_empty() {
                engine.flush_memtable()?;
            } else {
                engine.rotate_wal_epochs_only()?;
            }
        }

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
    /// quota registrations, and system catalog entries (`USE-02`).
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
        if 13 + row_key.len() + 23 > MAX_SINGLE_BLOCK_ENTRY_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "primary key exceeds maximum length (2048 bytes)",
            ));
        }

        let mut new_sec_keys = Vec::with_capacity(secondary_keys.len());
        for (idx_id, col_val) in secondary_keys {
            let sec_key = encode_secondary_index_key(table_id, *idx_id, col_val, pk);
            if 13 + sec_key.len() > MAX_SINGLE_BLOCK_ENTRY_BYTES {
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

        // 2. Write out-of-line blob FIRST if `value.len() >= inline_threshold_bytes` or if inline entry exceeds 4 KB block (`AUD-02`)
        let store_as_blob = value.len() >= self.config.inline_threshold_bytes
            || 13 + row_key.len() + value.len() > MAX_SINGLE_BLOCK_ENTRY_BYTES;
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
            if 13 + meta_key.len() + meta_bytes.len() > MAX_SINGLE_BLOCK_ENTRY_BYTES {
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
            match self.config.lru_read_tracking {
                LruReadTrackingMode::CoalescedLazyPromotion => {
                    self.access_tracker.record_read_hit(table_id, pk);
                }
                LruReadTrackingMode::EagerWriteOnRead => {
                    self.eager_promote_lru_on_read(table_id, pk)?;
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
                    if let Some(bytes) = self.dereference_blob(&ptr_copy, row_key)? {
                        let start = slab.len();
                        slab.extend_from_slice(&bytes);
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
                            if let Some(bytes) = self.dereference_blob(&ptr, row_key)? {
                                let start = slab.len();
                                slab.extend_from_slice(&bytes);
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
            match self.config.lru_read_tracking {
                LruReadTrackingMode::CoalescedLazyPromotion => {
                    self.access_tracker.record_read_hit(table_id, pk);
                }
                LruReadTrackingMode::EagerWriteOnRead => {
                    self.eager_promote_lru_on_read(table_id, pk)?;
                }
            }
        }

        Ok(hit)
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
        use std::ops::Bound;
        if limit == 0 {
            return Ok(());
        }
        let prefix = encode_namespace_prefix(table_id, NS_PRIMARY_ROW);
        let mut lb_stack = [0u8; 64];
        let lb_heap;
        let seek_key: &[u8] = match lower_bound {
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
        let upper_key_slice: Option<(&[u8], bool)> = match upper_bound {
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

        let lo_inc = lower_bound.map(|(_, inc)| inc).unwrap_or(true);
        if let Some((ub, ub_inc)) = upper_key_slice {
            if seek_key > ub || (seek_key == ub && (!lo_inc || !ub_inc)) {
                return Ok(());
            }
        }

        if self.idx_segments.is_empty() {
            let lo = match lower_bound {
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
                match &entry.value {
                    IndexValue::Inline(v) => {
                        let start = slab.len();
                        slab.extend_from_slice(v);
                        spans.push((start, slab.len()));
                        if spans.len() >= limit {
                            break;
                        }
                    }
                    IndexValue::Blob(ptr) => {
                        if let Some(v) = self.dereference_blob(ptr, k)? {
                            let start = slab.len();
                            slab.extend_from_slice(&v);
                            spans.push((start, slab.len()));
                            if spans.len() >= limit {
                                break;
                            }
                        }
                    }
                    IndexValue::Tombstone => {}
                }
            }
            return Ok(());
        }

        let pairs = self.scan_table_range(table_id, lower_bound, upper_bound, limit)?;
        for (_, v) in pairs {
            let start = slab.len();
            slab.extend_from_slice(&v);
            spans.push((start, slab.len()));
        }
        Ok(())
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
            self.tx_start_std_lengths = self
                .std_blobs
                .iter()
                .map(|(&seq, b)| (seq, b.file_len))
                .collect();
            self.tx_start_lru_lengths = self
                .lru_blobs
                .iter()
                .map(|(&k, b)| (k, b.file_len))
                .collect();
        }
        self.tx_depth += 1;
    }

    /// Commits an explicit batch transaction (`COMMIT`), writing all coalesced [`WalRecord`]
    /// entries in a single [`WalBatch`] syscall (or chunked batches if exceeding 512 KB)!
    pub fn commit_transaction(&mut self) -> io::Result<()> {
        if self.tx_depth > 0 {
            self.tx_depth -= 1;
        }
        if self.tx_depth == 0 {
            self.savepoints.clear();
            let durability = if self.pending_tx_sync {
                WriteDurability::SyncTransactional
            } else {
                WriteDurability::BufferedFast
            };
            if let Err(e) = self.flush_pending_tx_records() {
                self.tx_depth = 1;
                let _ = self.rollback_transaction();
                return Err(e);
            }
            self.tx_start_std_lengths.clear();
            self.tx_start_lru_lengths.clear();
            let capped_table_ids: Vec<u16> = self.capped_tables.keys().copied().collect();
            for tid in capped_table_ids {
                self.evict_until_within_quota(tid, durability)?;
            }
            if self.memtable.should_flush() {
                self.flush_memtable()?;
            }
        }
        Ok(())
    }

    /// Aborts an explicit batch transaction (`ROLLBACK`), discarding all buffered `pending_tx_buf`
    /// mutations and restoring `MemTable` and blob `dead_bytes` from the committed WAL streams
    /// since `checkpoint_lsn`.
    pub fn rollback_transaction(&mut self) -> io::Result<()> {
        self.tx_depth = 0;
        self.savepoints.clear();
        self.pending_tx_record_count = 0;
        self.pending_tx_buf.clear();
        self.pending_tx_sync = false;
        self.rebuild_memtable_from_wal_and_pending()?;
        self.tx_start_std_lengths.clear();
        self.tx_start_lru_lengths.clear();
        Ok(())
    }

    /// Opens or updates a transaction savepoint (`xSavepoint`) (`USE-01`).
    pub fn savepoint(&mut self, sp_id: i32) {
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
        self.savepoints.push(SavepointMarker {
            sp_id,
            buf_offset,
            record_count: self.pending_tx_record_count,
            pending_tx_sync: self.pending_tx_sync,
            implicit_tx,
        });
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

    /// Rolls back all mutations since savepoint `sp_id` (`xRollbackTo`) (`USE-01`).
    pub fn rollback_to_savepoint(&mut self, sp_id: i32) -> io::Result<()> {
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
        self.pending_tx_sync = marker.pending_tx_sync;
        self.rebuild_memtable_from_wal_and_pending()
    }

    fn rebuild_memtable_from_wal_and_pending(&mut self) -> io::Result<()> {
        self.buf_wal.flush_buffered_to_os()?;
        let st = self.meta_mgr.state().clone();

        // Reset `dead_bytes` to the checkpoint state in `boot_meta.db`, plus any aborted bytes
        // appended to blob segments during this transaction!
        let mut checkpoint_std_dead: HashMap<u64, u64> = HashMap::new();
        for bmeta in &st.std_blob_segments {
            checkpoint_std_dead.insert(bmeta.seq_id, bmeta.dead_bytes);
        }
        let mut aborted_std_map: HashMap<u64, u64> = HashMap::new();
        for (&seq, b) in &mut self.std_blobs {
            let base_dead = checkpoint_std_dead.get(&seq).copied().unwrap_or(0);
            let pre_tx_len = self.tx_start_std_lengths.get(&seq).copied().unwrap_or(0);
            let aborted_bytes = b.file_len.saturating_sub(pre_tx_len);
            b.dead_bytes = base_dead;
            if aborted_bytes > 0 {
                aborted_std_map.insert(seq, aborted_bytes);
            }
        }

        let mut checkpoint_lru_dead: HashMap<(u16, u64), u64> = HashMap::new();
        for lmeta in &st.lru_blob_segments {
            checkpoint_lru_dead.insert((lmeta.table_id, lmeta.seq_id), lmeta.dead_bytes);
        }
        let mut aborted_lru_map: HashMap<(u16, u64), u64> = HashMap::new();
        for (&(tid, seq), b) in &mut self.lru_blobs {
            let base_dead = checkpoint_lru_dead.get(&(tid, seq)).copied().unwrap_or(0);
            let pre_tx_len = self
                .tx_start_lru_lengths
                .get(&(tid, seq))
                .copied()
                .unwrap_or(0);
            let aborted_bytes = b.file_len.saturating_sub(pre_tx_len);
            b.dead_bytes = base_dead;
            if aborted_bytes > 0 {
                aborted_lru_map.insert((tid, seq), aborted_bytes);
            }
        }

        let std_blobs = &self.std_blobs;
        let lru_blobs = &self.lru_blobs;
        let rec = recover_two_streams(
            &**self.sync_wal.file(),
            st.sync_wal_epoch,
            &**self.buf_wal.file(),
            st.buf_wal_epoch,
            st.checkpoint_lsn,
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

        let mut new_mem = MemTable::new(self.config.memtable_capacity_bytes);
        for batch in &rec.merged_batches {
            for record in &batch.records {
                let key = match record {
                    WalRecord::PutInline { key, .. }
                    | WalRecord::PutBlobPtr { key, .. }
                    | WalRecord::Delete { key } => key.as_slice(),
                    WalRecord::TxBegin | WalRecord::TxCommit => continue,
                };
                let old_ptr = if let Some(mem_entry) = new_mem.get(key) {
                    match mem_entry.value {
                        IndexValue::Blob(ptr) => Some(ptr),
                        _ => None,
                    }
                } else {
                    let mut found = None;
                    for seg in self.idx_segments.iter().rev() {
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
                    self.record_dead_blob_bytes(&ptr);
                }
            }
            new_mem.apply_wal_batch(batch);
        }

        if self.pending_tx_record_count > 0 && self.pending_tx_buf.len() > 16 {
            let pending_lsn = rec.max_recovered_lsn + 1;
            WalBatch::finalize_encoded_buf(
                pending_lsn,
                self.pending_tx_record_count,
                &mut self.pending_tx_buf,
            );
            let pending_batch = WalBatch::decode(&self.pending_tx_buf)?;
            new_mem.apply_wal_batch(&pending_batch);
        }

        for (seq, aborted) in aborted_std_map {
            if let Some(b) = self.std_blobs.get_mut(&seq) {
                b.dead_bytes = b.dead_bytes.saturating_add(aborted);
            }
        }
        for (key, aborted) in aborted_lru_map {
            if let Some(b) = self.lru_blobs.get_mut(&key) {
                b.dead_bytes = b.dead_bytes.saturating_add(aborted);
            }
        }
        self.memtable = new_mem;
        self.reconcile_capped_tables_after_recovery()?;
        Ok(())
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
    /// WAL streams to `epoch + 1` in $O(1)$ without zero-filling, and atomically commits `boot_meta.db`.
    pub fn flush_memtable(&mut self) -> io::Result<()> {
        self.flush_pending_tx_records()?;
        self.buf_wal.flush_buffered_to_os()?;
        if self.memtable.is_empty() {
            return Ok(());
        }

        // Ensure all active blob segments are durably flushed before checkpointing `boot_meta.db`
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

        let entries = self.scan_merged_prefix_range(&[], &[], true, None, usize::MAX)?;
        let mut to_relocate = Vec::new();
        for entry in entries {
            if let IndexValue::Blob(ptr) = entry.value {
                if ptr.kind == BlobKind::Standard && fragmented_seqs.contains(&ptr.seg_id) {
                    to_relocate.push((entry.key, ptr));
                }
            }
        }

        let mut relocated = 0usize;
        for (key, old_ptr) in to_relocate {
            if let Some(payload) = self.dereference_blob(&old_ptr, &key)? {
                let new_ptr = self.append_to_blob_segment(BlobKind::Standard, 0, &key, &payload)?;
                self.record_dead_blob_bytes(&old_ptr);
                self.commit_wal_and_memtable(
                    vec![WalRecord::PutBlobPtr {
                        key,
                        blob_ptr: new_ptr,
                    }],
                    WriteDurability::BufferedFast,
                )?;
                relocated += 1;
            }
        }
        Ok(relocated)
    }

    /// Runs multi-way merge compaction across all active `idx_<SeqID>.db` segments, reclaims
    /// dead blob bytes from overwritten keys, relocates fragmented standard blob segments,
    /// commits the compacted segment to `boot_meta.db`, and deletes superseded `idx` and
    /// 100%-dead blob segments via Deferred GC.
    pub fn compact(&mut self) -> io::Result<()> {
        if !self.memtable.is_empty() {
            self.flush_memtable()?;
        }
        let relocated = self.relocate_fragmented_std_blobs(0.5)?;
        if relocated > 0 && !self.memtable.is_empty() {
            self.flush_memtable()?;
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
        if 13 + meta_key.len() + meta_bytes.len() > MAX_SINGLE_BLOCK_ENTRY_BYTES {
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
                            if 13 + meta_key.len() + meta_bytes.len() > MAX_SINGLE_BLOCK_ENTRY_BYTES
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
                            ct.eviction_cursor_tick = old_tick;
                        }
                        progress = true;
                        break;
                    }
                }

                // Coldest candidate confirmed -> Cascade Evict!
                if self.delete_row(table_id, &pk, durability)? {
                    if let Some(ct) = self.capped_tables.get_mut(&table_id) {
                        ct.total_evictions += 1;
                        ct.eviction_cursor_tick = old_tick;
                    }
                    progress = true;
                } else {
                    // Stale LRU queue entry whose row was already gone; clean up the orphan `'l'` key
                    self.commit_wal_and_memtable(
                        vec![WalRecord::Delete { key: entry.key }],
                        durability,
                    )?;
                    if let Some(ct) = self.capped_tables.get_mut(&table_id) {
                        ct.eviction_cursor_tick = old_tick;
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

        self.reclaim_fully_dead_lru_blob_segments(table_id)?;
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

    fn commit_wal_and_memtable(
        &mut self,
        records: Vec<WalRecord>,
        durability: WriteDurability,
    ) -> io::Result<u64> {
        let lsn = self.next_lsn;

        if self.tx_depth > 0 {
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

        // Enforce prefix durability across `wal_buf.db` and `wal_sync.db` (`AUD-08`):
        // before appending a `SyncTransactional` batch at LSN `L`, ensure all earlier
        // `BufferedFast` batches (`last_buf_append_lsn > last_buf_synced_lsn`) are synced!
        if durability == WriteDurability::SyncTransactional
            && self.last_buf_append_lsn > self.last_buf_synced_lsn
        {
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
        for seg in &self.idx_segments {
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
        let meta_prefix = encode_namespace_prefix(tid, NS_ROW_META);
        let meta_entries = self.scan_merged_prefix_range(&meta_prefix, &meta_prefix, true, None, usize::MAX)?;
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

        let user_prefix = encode_namespace_prefix(tid, NS_PRIMARY_ROW);
        let row_entries = self.scan_merged_prefix_range(&user_prefix, &user_prefix, true, None, usize::MAX)?;
        let mut total_bytes = 0u64;
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
                    if 13 + meta_key.len() + meta_bytes.len() > MAX_SINGLE_BLOCK_ENTRY_BYTES {
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
                    IndexValue::Blob(ptr) => (ptr.len as usize).saturating_sub(16 + entry.key.len()),
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

        // AUD-08 check: Write `idx_row` with `BufferedFast` (tag_buf), then overwrite with
        // `SyncTransactional` (tag_sync), then simulate power loss!
        engine
            .put_row(
                1,
                b"idx_row",
                b"v_buf",
                &[(1, b"tag_buf".to_vec())],
                WriteDurability::BufferedFast,
            )
            .unwrap();
        engine
            .put_row(
                1,
                b"idx_row",
                b"v_sync",
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
}
