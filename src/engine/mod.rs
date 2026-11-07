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
    AccessTracker, CappedTableState, NS_LRU_QUEUE, NS_PRIMARY_ROW, NS_ROW_META, RowMetaRecord,
    decode_lru_queue_key, decode_primary_row_key, decode_secondary_index_key, encode_lru_queue_key,
    encode_namespace_prefix, encode_primary_row_key, encode_row_meta_key,
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
    /// Transaction nesting depth (`xBegin` / `xCommit`). While `tx_depth > 0`, `WalRecord` items
    /// are coalesced into `pending_tx_records` and committed as a single atomic `WalBatch` on `commit_transaction`!
    tx_depth: usize,
    pending_tx_records: Vec<WalRecord>,
    pending_tx_sync: bool,
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
            std_blobs.insert(
                bmeta.seq_id,
                TrackedStdBlob {
                    reader: BlobSegmentReader::new(file, bmeta.seq_id),
                    file_len: bmeta.file_len,
                    dead_bytes: bmeta.dead_bytes,
                },
            );
        }

        // 3. Open active `blob_lru_<TblID>_<SeqID>.db` segments
        let mut lru_blobs = BTreeMap::new();
        for lmeta in &state_snapshot.lru_blob_segments {
            let fname = format_lru_blob_file_name(lmeta.table_id, lmeta.seq_id);
            let file = fs.open_file(&fname, false, true, OpenHint::ImmutableSequential)?;
            lru_blobs.insert(
                (lmeta.table_id, lmeta.seq_id),
                TrackedLruBlob {
                    table_id: lmeta.table_id,
                    reader: BlobSegmentReader::new(file, lmeta.seq_id),
                    file_len: lmeta.file_len,
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
            pending_tx_records: Vec::new(),
            pending_tx_sync: false,
        };
        engine.access_tracker = AccessTracker::new(1, engine.config.max_in_memory_access_ticks);
        engine.reconcile_capped_tables_after_recovery()?;

        Ok(engine)
    }

    /// Registers or updates a Capped Table with a strict `max_bytes` quota and automatic LRU eviction.
    pub fn register_capped_table(&mut self, table_id: u16, max_bytes: u64) -> io::Result<()> {
        let entry = self
            .capped_tables
            .entry(table_id)
            .or_insert_with(|| CappedTableState::new(table_id, max_bytes));
        entry.max_bytes = max_bytes;
        self.evict_until_within_quota(table_id, WriteDurability::BufferedFast)?;
        Ok(())
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
        let row_key = encode_primary_row_key(table_id, pk);
        let is_capped = self.capped_tables.contains_key(&table_id);
        let logical_size = (pk.len() + value.len()) as u32;

        // 1. Check if the row already exists so we can clean up old blob bytes, old secondary keys,
        //    and old LRU queue entries (`[TableID | 'l' | old_tick | pk]`).
        let mut records = Vec::with_capacity(3 + secondary_keys.len() * 2);
        let mut old_logical_size = 0u64;

        if let Some((_lsn, old_idx_val)) = self.lookup_raw_entry(&row_key)? {
            if let IndexValue::Blob(old_ptr) = old_idx_val {
                self.record_dead_blob_bytes(&old_ptr);
            }
        }

        let meta_key = encode_row_meta_key(table_id, pk);
        if let Some((_lsn, IndexValue::Inline(meta_bytes))) = self.lookup_raw_entry(&meta_key)? {
            if let Ok(old_meta) = RowMetaRecord::from_bytes(&meta_bytes) {
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

        // 2. Write out-of-line blob FIRST if `value.len() >= inline_threshold_bytes`
        if value.len() >= self.config.inline_threshold_bytes {
            let blob_kind = if is_capped {
                BlobKind::Lru
            } else {
                BlobKind::Standard
            };
            let ptr = self.append_to_blob_segment(blob_kind, table_id, &row_key, value)?;
            records.push(WalRecord::PutBlobPtr {
                key: row_key,
                blob_ptr: ptr,
            });
        } else {
            records.push(WalRecord::PutInline {
                key: row_key,
                value: value.to_vec(),
            });
        }

        // 3. Emit Secondary Index entries (`[TableID | 'i' | IndexID | Escaped(ColVal) | pk]`)
        for (idx_id, col_val) in secondary_keys {
            records.push(WalRecord::PutInline {
                key: encode_secondary_index_key(table_id, *idx_id, col_val, pk),
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

        if is_capped || !secondary_keys.is_empty() {
            let meta_rec = RowMetaRecord {
                recorded_tick: assigned_tick,
                logical_size,
                secondary_keys: secondary_keys.to_vec(),
            };
            records.push(WalRecord::PutInline {
                key: meta_key,
                value: meta_rec.to_bytes(),
            });
        }

        // 5. Append atomic batch to WAL and apply to MemTable
        self.commit_wal_and_memtable(records, durability)?;

        // 6. Update Capped Table logical byte counter and enforce `MAX_BYTES` quota
        if let Some(ct) = self.capped_tables.get_mut(&table_id) {
            ct.current_logical_bytes = ct
                .current_logical_bytes
                .saturating_sub(old_logical_size)
                .saturating_add(logical_size as u64);
        }
        self.reclaim_fully_dead_blob_segments()?;
        if is_capped {
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
        let row_key = encode_primary_row_key(table_id, pk);
        let Some((_lsn, old_idx_val)) = self.lookup_raw_entry(&row_key)? else {
            return Ok(false);
        };
        if matches!(old_idx_val, IndexValue::Tombstone) {
            return Ok(false);
        }
        if let IndexValue::Blob(old_ptr) = old_idx_val {
            self.record_dead_blob_bytes(&old_ptr);
        }

        let mut records = vec![WalRecord::Delete { key: row_key }];
        let mut old_logical_size = 0u64;

        let meta_key = encode_row_meta_key(table_id, pk);
        if let Some((_lsn, IndexValue::Inline(meta_bytes))) = self.lookup_raw_entry(&meta_key)? {
            if let Ok(old_meta) = RowMetaRecord::from_bytes(&meta_bytes) {
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
            }
            records.push(WalRecord::Delete { key: meta_key });
        }

        self.access_tracker.remove_key(table_id, pk);
        self.commit_wal_and_memtable(records, durability)?;

        if let Some(ct) = self.capped_tables.get_mut(&table_id) {
            ct.current_logical_bytes = ct.current_logical_bytes.saturating_sub(old_logical_size);
        }
        self.reclaim_fully_dead_blob_segments()?;
        Ok(true)
    }

    /// Scans all live rows `(pk, value)` in `table_id` in ascending memcomparable primary-key order.
    pub fn scan_table(&mut self, table_id: u16) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan_table_range(table_id, None, None, usize::MAX)
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
        if limit == 0 {
            return Ok(Vec::new());
        }
        let prefix = encode_namespace_prefix(table_id, NS_PRIMARY_ROW);
        let seek_key = match lower_bound {
            Some((lb_pk, _)) => encode_primary_row_key(table_id, lb_pk),
            None => prefix.to_vec(),
        };
        let upper_full = upper_bound.map(|(ub_pk, inc)| (encode_primary_row_key(table_id, ub_pk), inc));

        // Fetch enough entries from the merged prefix scan to satisfy `limit` live rows
        let fetch_cap = if limit == usize::MAX {
            usize::MAX
        } else {
            limit.saturating_add(16)
        };
        let merged = self.scan_merged_prefix_range(
            &prefix,
            &seek_key,
            upper_full.as_ref().map(|(v, inc)| (v.as_slice(), *inc)),
            fetch_cap,
        )?;
        let mut rows = Vec::with_capacity(merged.len().min(limit));

        for entry in merged {
            let Some(pk) = decode_primary_row_key(table_id, &entry.key) else {
                continue;
            };
            if let Some((lb_pk, inclusive)) = lower_bound {
                if inclusive {
                    if pk < lb_pk {
                        continue;
                    }
                } else if pk <= lb_pk {
                    continue;
                }
            }
            if let Some((ub_pk, inclusive)) = upper_bound {
                if inclusive {
                    if pk > ub_pk {
                        break;
                    }
                } else if pk >= ub_pk {
                    break;
                }
            }

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
        let prefix = encode_secondary_index_exact_prefix(table_id, index_id, col_val);
        let merged = self.scan_merged_prefix(&prefix)?;
        let mut pks = Vec::with_capacity(merged.len());
        for entry in merged {
            if matches!(entry.value, IndexValue::Tombstone) {
                continue;
            }
            if let Some((_dec_col, pk)) = decode_secondary_index_key(table_id, index_id, &entry.key)?
            {
                pks.push(pk);
            }
        }
        Ok(pks)
    }

    /// Begins an explicit batch transaction (`BEGIN`). Subsequent mutations apply immediately to
    /// the in-memory `MemTable` (for read-your-own-writes visibility) while coalescing their
    /// [`WalRecord`] entries into a single atomic [`WalBatch`] flushed on [`commit_transaction`].
    pub fn begin_transaction(&mut self) {
        self.tx_depth += 1;
    }

    /// Commits an explicit batch transaction (`COMMIT`), writing all coalesced [`WalRecord`]
    /// entries in a single [`WalBatch`] syscall!
    pub fn commit_transaction(&mut self) -> io::Result<()> {
        if self.tx_depth > 0 {
            self.tx_depth -= 1;
        }
        if self.tx_depth == 0 {
            self.flush_pending_tx_records()?;
            if self.memtable.should_flush() {
                self.flush_memtable()?;
            }
        }
        Ok(())
    }

    fn flush_pending_tx_records(&mut self) -> io::Result<()> {
        if self.pending_tx_records.is_empty() {
            return Ok(());
        }
        let records = core::mem::take(&mut self.pending_tx_records);
        let durability = if self.pending_tx_sync {
            WriteDurability::SyncTransactional
        } else {
            WriteDurability::BufferedFast
        };
        self.pending_tx_sync = false;

        let lsn = self.next_lsn;
        self.next_lsn += 1;
        let batch = WalBatch::new(lsn, records);
        self.append_wal_batch_with_rotation(&batch, durability)
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

        let mut new_seq_id = 0u64;
        self.meta_mgr.commit_update(|st| {
            new_seq_id = st.allocate_seq_id();
        })?;

        let new_reader =
            self.memtable
                .flush_to_segment(&*self.fs, new_seq_id, self.config.bloom_bits_per_key)?;
        let idx_meta = IdxSegmentMeta {
            seq_id: new_reader.seq_id(),
            file_len: new_reader.file_len(),
            level: new_reader.level(),
            record_count: new_reader.record_count(),
        };
        self.idx_segments.push(new_reader);

        let new_sync_epoch = self.sync_wal.active_epoch() + 1;
        let new_buf_epoch = self.buf_wal.active_epoch() + 1;
        let new_checkpoint_lsn = self.next_lsn.saturating_sub(1);

        let std_metas = self.build_std_blob_metas();
        let lru_metas = self.build_lru_blob_metas();
        let quota_metas = self.build_quota_metas();

        self.meta_mgr.commit_update(|st| {
            st.idx_segments.push(idx_meta);
            st.std_blob_segments = std_metas;
            st.lru_blob_segments = lru_metas;
            st.table_quotas = quota_metas;
            st.checkpoint_lsn = new_checkpoint_lsn;
            st.sync_wal_epoch = new_sync_epoch;
            st.buf_wal_epoch = new_buf_epoch;
        })?;

        self.sync_wal.reset_for_new_epoch(new_sync_epoch);
        self.buf_wal.reset_for_new_epoch(new_buf_epoch);
        Ok(())
    }

    /// Runs multi-way merge compaction across all active `idx_<SeqID>.db` segments, reclaims
    /// dead blob bytes from overwritten keys, commits the compacted segment to `boot_meta.db`,
    /// and deletes superseded `idx` segments via Deferred GC.
    pub fn compact(&mut self) -> io::Result<()> {
        if !self.memtable.is_empty() {
            self.flush_memtable()?;
        }
        if self.idx_segments.len() <= 1 {
            return Ok(());
        }

        let mut comp_seq_id = 0u64;
        self.meta_mgr.commit_update(|st| {
            comp_seq_id = st.allocate_seq_id();
        })?;

        // Pass segments ordered newest-first (`rev()`) to `compact_idx_segments`
        let refs_newest_first: Vec<&IdxSegmentReader<FS::File>> =
            self.idx_segments.iter().rev().collect();
        let outcome = compact_idx_segments(
            &*self.fs,
            &refs_newest_first,
            comp_seq_id,
            1,
            self.config.bloom_bits_per_key,
            true,
        )?;

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

        // Drop old segment readers BEFORE committing `boot_meta.db` so `try_process_pending_deletes`
        // can immediately unlink the superseded `idx_<SeqID>.db` files!
        self.idx_segments.clear();
        self.idx_segments.push(outcome.new_segment);

        let std_metas = self.build_std_blob_metas();
        let lru_metas = self.build_lru_blob_metas();
        let quota_metas = self.build_quota_metas();

        self.meta_mgr.commit_update(|st| {
            st.idx_segments = vec![new_meta];
            st.std_blob_segments = std_metas;
            st.lru_blob_segments = lru_metas;
            st.table_quotas = quota_metas;
            for name in old_idx_names {
                st.queue_pending_delete(name);
            }
        })?;

        self.reclaim_fully_dead_blob_segments()?;
        Ok(())
    }

    // =========================================================================
    // Internal Helper Methods
    // =========================================================================

    fn eager_promote_lru_on_read(&mut self, table_id: u16, pk: &[u8]) -> io::Result<()> {
        let meta_key = encode_row_meta_key(table_id, pk);
        let Some((_lsn, IndexValue::Inline(meta_bytes))) = self.lookup_raw_entry(&meta_key)? else {
            return Ok(());
        };
        let mut meta = RowMetaRecord::from_bytes(&meta_bytes)?;
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
        records.push(WalRecord::PutInline {
            key: meta_key,
            value: meta.to_bytes(),
        });
        self.commit_wal_and_memtable(records, WriteDurability::BufferedFast)?;
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
                        if let Some((_lsn, IndexValue::Inline(mbytes))) =
                            self.lookup_raw_entry(&meta_key)?
                        {
                            if let Ok(mut mrec) = RowMetaRecord::from_bytes(&mbytes) {
                                mrec.recorded_tick = hot_tick;
                                records.push(WalRecord::PutInline {
                                    key: meta_key,
                                    value: mrec.to_bytes(),
                                });
                            }
                        }
                        self.commit_wal_and_memtable(records, durability)?;
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
                    let mut seq_id = 0u64;
                    self.meta_mgr.commit_update(|st| {
                        seq_id = st.allocate_seq_id();
                    })?;
                    let writer = BlobSegmentWriter::open(&*self.fs, BlobKind::Standard, 0, seq_id)?;
                    self.std_blobs.insert(
                        seq_id,
                        TrackedStdBlob {
                            reader: BlobSegmentReader::new(writer.file().clone(), seq_id),
                            file_len: 0,
                            dead_bytes: 0,
                        },
                    );
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
                    let mut seq_id = 0u64;
                    self.meta_mgr.commit_update(|st| {
                        seq_id = st.allocate_seq_id();
                    })?;
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

    /// Reclaims any sealed `blob_std` or `blob_lru` segment where `dead_bytes >= file_len`
    /// ($O(1)$ Copy-less Segment Deletion!).
    fn reclaim_fully_dead_blob_segments(&mut self) -> io::Result<()> {
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
        for seq in dead_std {
            self.std_blobs.remove(&seq);
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

        for (tid, seq) in dead_lru {
            self.lru_blobs.remove(&(tid, seq));
            deleted_files.push(format_lru_blob_file_name(tid, seq));
        }

        if !deleted_files.is_empty() {
            for fname in deleted_files {
                if self.fs.remove_file(&fname).is_err() {
                    self.meta_mgr.commit_update(|st| {
                        st.queue_pending_delete(fname);
                    })?;
                }
            }
        }
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
        self.next_lsn += 1;
        let batch = WalBatch::new(lsn, records);

        // Always apply to MemTable immediately for read-your-own-writes visibility inside a transaction
        self.memtable.apply_wal_batch(&batch);

        if self.tx_depth > 0 {
            if durability == WriteDurability::SyncTransactional {
                self.pending_tx_sync = true;
            }
            self.pending_tx_records.extend(batch.records);
            // If pending records approach 256 KB during a huge bulk transaction, flush an intermediate batch
            if self.pending_tx_records.len() >= 2048 {
                self.flush_pending_tx_records()?;
            }
            return Ok(lsn);
        }

        self.append_wal_batch_with_rotation(&batch, durability)?;
        if self.memtable.should_flush() {
            self.flush_memtable()?;
        }
        Ok(lsn)
    }

    fn append_wal_batch_with_rotation(
        &mut self,
        batch: &WalBatch,
        durability: WriteDurability,
    ) -> io::Result<()> {
        let outcome = match durability {
            WriteDurability::SyncTransactional => self.sync_wal.append_batch(batch)?,
            WriteDurability::BufferedFast => self.buf_wal.append_batch(batch)?,
        };

        if outcome == WalAppendOutcome::RingFull {
            self.flush_memtable()?;
            let retry = match durability {
                WriteDurability::SyncTransactional => self.sync_wal.append_batch(batch)?,
                WriteDurability::BufferedFast => self.buf_wal.append_batch(batch)?,
            };
            if retry == WalAppendOutcome::RingFull {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "single WAL batch exceeds entire pre-allocated WAL file capacity",
                ));
            }
        }
        Ok(())
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
        self.scan_merged_prefix_range(prefix, seek_key, None, limit)
    }

    fn scan_merged_prefix_range(
        &self,
        prefix: &[u8],
        seek_key: &[u8],
        upper_key_bound: Option<(&[u8], bool)>,
        limit: usize,
    ) -> io::Result<Vec<IndexEntry>> {
        let mut merged_map: BTreeMap<Vec<u8>, (u64, IndexValue)> = BTreeMap::new();
        for seg in &self.idx_segments {
            for entry in seg.scan_prefix_range(prefix, seek_key, upper_key_bound, limit)? {
                merged_map.insert(entry.key, (entry.lsn, entry.value));
            }
        }
        for entry in self
            .memtable
            .scan_prefix_range(prefix, seek_key, upper_key_bound, limit)
        {
            merged_map.insert(entry.key, (entry.lsn, entry.value));
        }
        Ok(merged_map
            .into_iter()
            .take(limit)
            .map(|(key, (lsn, value))| IndexEntry { key, lsn, value })
            .collect())
    }

    fn reconcile_capped_tables_after_recovery(&mut self) -> io::Result<()> {
        let table_ids: Vec<u16> = self.capped_tables.keys().copied().collect();
        for tid in table_ids {
            let meta_prefix = encode_namespace_prefix(tid, NS_ROW_META);
            let entries = self.scan_merged_prefix(&meta_prefix)?;
            let mut total_bytes = 0u64;
            for entry in entries {
                if let IndexValue::Inline(bytes) = entry.value {
                    if let Ok(rec) = RowMetaRecord::from_bytes(&bytes) {
                        total_bytes += rec.logical_size as u64;
                        self.access_tracker.observe_tick(rec.recorded_tick);
                    }
                }
            }
            if let Some(ct) = self.capped_tables.get_mut(&tid) {
                ct.current_logical_bytes = total_bytes;
            }
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

        // Sealed blob segments whose entries were all evicted must have been reclaimed
        assert!(
            engine.active_lru_blob_segment_count(1) <= 2,
            "sealed 100%-dead blob_lru segments must be unlinked automatically"
        );

        // Flush, compact, and reopen to verify full persistence & recovery
        engine.flush_memtable().unwrap();
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
}
