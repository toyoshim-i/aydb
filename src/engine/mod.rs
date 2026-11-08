//! Layer 4 (Part 2): Unified Phase 1 Storage Engine Coordinator (`StorageEngine`).
//!
//! Integrates `BootMetaManager` (`boot_meta.db`), two-stream Epoch WAL (`wal_sync.db` + `wal_buf.db`),
//! out-of-line blob segments (`blob_std` + per-table `blob_lru`), bounded `MemTable`, 4 KB block
//! `idx_<SeqID>.db` segments with Bloom filters and sparse fence pointers, multi-way merge
//! compaction, and Capped Table (`MAX_BYTES`) delayed LRU eviction.

mod compaction;
pub use compaction::{CompactionJob, CompactionOutput};
mod eviction;
mod reader;
mod scan;
mod tx;

pub(crate) use tx::{CommitStep, GroupRole};

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, HashMap};
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::blob::{
    BLOB_RECORD_HEADER_LEN, BlobSegmentReader, BlobSegmentWriter, DEFAULT_INLINE_THRESHOLD, DEFAULT_MAX_BLOB_SEGMENT_SIZE,
};
use crate::cache::{BlobCacheKey, VerifiedBlobCache, VerifiedBlockCache};
use crate::index::{
    BloomFilter, DEFAULT_BLOOM_BITS_PER_KEY, IdxSegmentReader, IndexEntry, IndexValue,
    RawIdxEntry, decode_lru_queue_key, decode_secondary_index_pk_slice,
};
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

pub use self::reader::{EngineReader, ReadView};
use self::reader::{CxRef, NEED_FLUSH_MSG, ReadCtx};
use self::tx::{SavepointMarker, TxMetaSnapshot};

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
/// If a WAL append or sync fails, the call returns an error and the engine becomes *poisoned*
/// (see [`StorageEngine::is_poisoned`]): the on-disk outcome of the failed write is unknown, so
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

/// A queued automatic deletion of a row whose blob failed verification (`Evict` policy).
struct PendingCorruptEviction {
    /// Key of the record (`'r'` row or `'t'` row-meta) whose blob was corrupt.
    key: Vec<u8>,
    /// The blob pointer observed as corrupt; deletion only proceeds if it is still current.
    ptr: BlobPtr,
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
    pub(super) idx_segments: Vec<Arc<IdxSegmentReader<FS::File>>>,
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
    /// Coordinates writers waiting for their turn and commits sharing a device flush.
    pub(super) commit_gate: Arc<tx::CommitGate>,
    /// Committed batches written to `wal_sync.db` whose device flush has not finished, in LSN
    /// order, as `(batch LSN, entries)`. The next writer's overlay starts from these entries;
    /// readers see them only after they move to the `MemTable`.
    pub(super) pending_group: std::collections::VecDeque<(u64, BTreeMap<Vec<u8>, crate::memtable::MemEntry>)>,
    /// True if `flush_memtable()` was requested during an active transaction.
    pub(super) deferred_flush_on_commit: bool,
    /// True if `compact()` was requested during an active transaction.
    pub(super) deferred_compact_on_commit: bool,
    /// Invoked (under the engine lock, so it must be cheap and non-blocking) when a flush leaves
    /// compaction pending in background mode.
    pub(super) compaction_hook: Option<Arc<dyn Fn() + Send + Sync>>,
    /// True if `prepare_commit_transaction()` (`xSync`) has already run for the current commit.
    pub(super) tx_prepared: bool,
    /// True while `prepare_commit_transaction()` is running internal quota eviction.
    pub(super) tx_preparing: bool,
    pub(super) pending_tx_buf: Vec<u8>,
    pub(super) pending_tx_record_count: u32,
    pub(super) pending_tx_bytes: usize,
    pub(super) pending_tx_sync: bool,
    /// Uncommitted writes of the active explicit transaction (see [`tx::TxOverlay`]).
    pub(super) tx_overlay: tx::TxOverlay,
    pub(super) tx_start_meta: TxMetaSnapshot,
    pub(super) savepoints: Vec<SavepointMarker>,
    /// Tables dropped inside the currently active transaction (`(table_id, was_capped)`).
    pub(super) dropped_tables_in_tx: Vec<(u16, bool)>,
    /// True if any capped-table quota was registered or modified inside the active transaction.
    pub(super) quotas_modified_in_tx: bool,
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
    /// Number of post-commit maintenance steps (flush, quota eviction, reclaim, compaction) that
    /// failed after their triggering write was already committed.
    maintenance_failures: AtomicU64,
    /// Text of the most recent post-commit maintenance failure.
    last_maintenance_error: Mutex<Option<String>>,
    /// Explicit per-table corruption policies (absent -> derived default, see
    /// [`Self::corrupt_policy`]).
    corrupt_policies: HashMap<u16, CorruptPolicy>,
    /// Explicit per-table inline threshold overrides in bytes (see [`Self::set_table_inline_threshold`]).
    table_inline_thresholds: BTreeMap<u16, usize>,
    /// Scratch buffer for encoded WAL batches on commit, avoiding per-commit heap allocation.
    pub(super) scratch_wal_encoded: Vec<u8>,
    /// In-process verified 4 KB index block cache.
    pub(crate) block_cache: Arc<VerifiedBlockCache>,
    /// In-process small verified blob payload cache.
    pub(crate) blob_cache: Arc<VerifiedBlobCache>,
    /// Engine-wide count of blob verification failures observed by reads/cleanup paths.
    corrupt_blob_detections: AtomicU64,
    /// Rows awaiting automatic deletion under [`CorruptPolicy::Evict`]. Filled from `&self` read
    /// paths (hence the `Mutex`); drained only by [`Self::drain_corrupt_evictions`].
    pending_corrupt_evictions: Mutex<HashMap<(u16, Vec<u8>), PendingCorruptEviction>>,
}

/// Opens a blob segment found by directory listing but absent from the catalog. A file that
/// vanished since the listing is skipped; any other error aborts the caller.
fn open_uncataloged_blob<FS: PlatformFs>(
    fs: &FS,
    fname: &str,
) -> io::Result<Option<Arc<FS::File>>> {
    match fs.open_file(fname, false, true, OpenHint::ImmutableSequential) {
        Ok(f) => Ok(Some(f)),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

impl<FS: PlatformFs> StorageEngine<FS> {
    /// Opens or recovers a `StorageEngine` rooted at `fs`.
    pub fn open(fs: Arc<FS>, config: EngineConfig) -> io::Result<Self> {
        let mut meta_mgr = BootMetaManager::open_or_recover_deferred_sweep(fs.clone())?;
        meta_mgr.reserve_seq_ids_above_existing_files()?;
        let state_snapshot = meta_mgr.state().clone();

        let block_cache = Arc::new(VerifiedBlockCache::new(config.block_cache_bytes));
        let blob_cache = Arc::new(VerifiedBlobCache::new(config.blob_cache_bytes));

        // 1. Open active `idx_<SeqID>.db` segments
        let mut idx_segments = Vec::with_capacity(state_snapshot.idx_segments.len());
        for seg_meta in &state_snapshot.idx_segments {
            let mut reader = IdxSegmentReader::open(&*fs, seg_meta.seq_id)?;
            reader.set_block_cache(Some(Arc::clone(&block_cache)));
            idx_segments.push(Arc::new(reader));
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
                        if let Some(file) = open_uncataloged_blob(&*fs, &fname)? {
                            let flen = file.file_len()?;
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
                            if let Some(file) = open_uncataloged_blob(&*fs, &fname)? {
                                let flen = file.file_len()?;
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

        // Snapshot the persisted (checkpointed) dead_bytes before WAL replay mutates the in-memory
        // values. Uncataloged segments have no persisted value (0).
        let std_base_dead: BTreeMap<u64, u64> =
            std_blobs.iter().map(|(&k, b)| (k, b.dead_bytes)).collect();
        let lru_base_dead: BTreeMap<(u16, u64), u64> =
            lru_blobs.iter().map(|(&k, b)| (k, b.dead_bytes)).collect();

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
                BlobKind::Standard => match std_blobs.get(&ptr.seg_id) {
                    Some(b) => b.reader.validate_blob_ptr(ptr),
                    None => Ok(false),
                },
                BlobKind::Lru => match lru_blobs.get(&(ptr.table_id, ptr.seg_id)) {
                    Some(b) => b.reader.validate_blob_ptr(ptr),
                    None => Ok(false),
                },
            },
        )?;

        sync_wal.set_cursor_after_recovery(rec.next_sync_sector_idx);
        buf_wal.set_cursor_after_recovery(rec.next_buf_sector_idx);

        // Recovery that found no valid sector in either stream and replayed no batch leaves the
        // persisted capped-table byte totals authoritative.
        let replayed_wal = !rec.merged_batches.is_empty()
            || rec.next_sync_sector_idx > 0
            || rec.next_buf_sector_idx > 0;

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

        // Invariant: persisted dead_bytes must only reflect mutations at or below checkpoint_lsn
        // (plus idempotent tail garbage). The open-time commit below does not advance
        // checkpoint_lsn, so it must NOT persist the WAL-replay increments: a crash before the
        // post-recovery checkpoint would replay the same batches and double-count, eventually
        // deleting segments that still hold live blobs. Tail garbage is idempotent because the
        // committed file_len becomes the physical length, so a re-open computes no new garbage.
        // The in-memory values (with replay increments) are persisted by the post-recovery
        // flush together with the new checkpoint_lsn.
        let mut std_persist_dead: BTreeMap<u64, u64> = BTreeMap::new();
        let mut lru_persist_dead: BTreeMap<(u16, u64), u64> = BTreeMap::new();
        let mut had_uncataloged_live_blobs = false;
        std_blobs.retain(|&seq_id, b| {
            let valid_end = std_valid_end.get(&seq_id).copied().unwrap_or(0);
            if !cataloged_std_seqs.contains(&seq_id) && valid_end == 0 {
                return false;
            }
            if !cataloged_std_seqs.contains(&seq_id) {
                had_uncataloged_live_blobs = true;
            }
            let mut persisted = std_base_dead.get(&seq_id).copied().unwrap_or(0);
            if b.file_len > valid_end {
                let tail_garbage = b.file_len - valid_end;
                b.dead_bytes = b.dead_bytes.saturating_add(tail_garbage).min(b.file_len);
                persisted = persisted.saturating_add(tail_garbage).min(b.file_len);
            }
            std_persist_dead.insert(seq_id, persisted);
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
            let mut persisted = lru_base_dead.get(&(tid, seq_id)).copied().unwrap_or(0);
            if b.file_len > valid_end {
                let tail_garbage = b.file_len - valid_end;
                b.dead_bytes = b.dead_bytes.saturating_add(tail_garbage).min(b.file_len);
                persisted = persisted.saturating_add(tail_garbage).min(b.file_len);
            }
            lru_persist_dead.insert((tid, seq_id), persisted);
            true
        });

        // Catalog uncataloged blob segments that turned out to hold live records. Their SeqIDs are
        // already below `next_seq_id` (see `reserve_seq_ids_above_existing_files`).
        if had_uncataloged_live_blobs {
            let std_metas: Vec<StdBlobSegmentMeta> = std_blobs
                .iter()
                .map(|(&seq_id, b)| StdBlobSegmentMeta {
                    seq_id,
                    file_len: b.file_len,
                    dead_bytes: std_persist_dead.get(&seq_id).copied().unwrap_or(0),
                })
                .collect();
            let lru_metas: Vec<LruBlobSegmentMeta> = lru_blobs
                .iter()
                .map(|(&(tid, seq_id), b)| LruBlobSegmentMeta {
                    seq_id,
                    file_len: b.file_len,
                    dead_bytes: lru_persist_dead.get(&(tid, seq_id)).copied().unwrap_or(0),
                    table_id: b.table_id,
                })
                .collect();
            meta_mgr.commit_update(|st| {
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
            commit_gate: Arc::new(tx::CommitGate::default()),
            pending_group: std::collections::VecDeque::new(),
            deferred_flush_on_commit: false,
            deferred_compact_on_commit: false,
            compaction_hook: None,
            tx_prepared: false,
            tx_preparing: false,
            pending_tx_buf: Vec::new(),
            pending_tx_record_count: 0,
            pending_tx_bytes: 0,
            pending_tx_sync: false,
            tx_overlay: tx::TxOverlay::default(),
            tx_start_meta: TxMetaSnapshot::default(),
            savepoints: Vec::new(),
            dropped_tables_in_tx: Vec::new(),
            quotas_modified_in_tx: false,
            active_vtab_ids: HashMap::new(),
            table_has_row_meta: BTreeMap::new(),
            last_buf_append_lsn: 0,
            last_buf_synced_lsn: 0,
            poisoned: None,
            maintenance_failures: AtomicU64::new(0),
            last_maintenance_error: Mutex::new(None),
            corrupt_policies: HashMap::new(),
            table_inline_thresholds: BTreeMap::new(),
            scratch_wal_encoded: Vec::with_capacity(1024),
            block_cache,
            blob_cache,
            corrupt_blob_detections: AtomicU64::new(0),
            pending_corrupt_evictions: Mutex::new(HashMap::new()),
        };
        engine.access_tracker = AccessTracker::new(1, engine.config.max_in_memory_access_ticks);

        // Always rotate both WAL epochs after recovery, even when nothing was recovered.
        // Recovery halts at the first torn/old sector, but later sectors of the SAME epoch may
        // already be on disk (write-back reordering before a crash). If we kept the epoch, new
        // writes would restart at that halt point and a later recovery could run past the new
        // data into those stale same-epoch sectors, replaying batches that were never recovered
        // the first time (with colliding LSNs). A fresh epoch makes every pre-crash sector
        // unreadable as part of the new stream. Cost: one boot_meta commit per open.
        // Rotation must precede reconciliation, which appends to the WAL.
        if !engine.memtable.is_empty() {
            engine.flush_memtable()?;
        } else {
            engine.rotate_wal_epochs_only()?;
        }

        // Reconciliation records land in the fresh epoch and are replayed by the next recovery.
        engine.reconcile_capped_tables_after_recovery(replayed_wal)?;

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
    pub(super) fn record_maintenance_failure(&self, e: &io::Error) {
        self.maintenance_failures.fetch_add(1, Ordering::Relaxed);
        *self
            .last_maintenance_error
            .lock()
            .unwrap_or_else(|p| p.into_inner()) = Some(e.to_string());
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

    /// How long a SQL writer waits for its turn before failing with `SQLITE_BUSY`.
    pub(crate) fn writer_wait_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_millis(u64::from(self.config.writer_wait_timeout_ms))
    }

    pub(super) fn has_pending_corrupt_evictions(&self) -> bool {
        self.pending_corrupt_evictions
            .lock()
            .map(|pending| !pending.is_empty())
            .unwrap_or(true)
    }

    pub fn next_lsn(&self) -> u64 {
        self.next_lsn
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
                if let Some(owner) = persisted_id_to_name.get(&id) {
                    if owner != &norm_name {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("table_id {id} is already persisted for virtual table '{owner}'"),
                        ));
                    }
                }
                if let Some(&persisted_id) = persisted_name_to_id.get(&norm_name) {
                    if persisted_id != id {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!(
                                "virtual table '{norm_name}' is already persisted with table_id {persisted_id}"
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
        self.corrupt_policies.remove(&table_id);
        self.table_inline_thresholds.remove(&table_id);
        self.blob_cache.invalidate_table_blobs(table_id);
        if let Ok(mut q) = self.pending_corrupt_evictions.lock() {
            q.retain(|(tid, _), _| *tid != table_id);
        }
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

        // Without an enclosing transaction the deletes run in an internal one: its chunked
        // TxBegin/TxCommit framing and WAL growth let a table larger than the WAL be dropped
        // atomically. The internal transaction is exempt from `max_transaction_bytes`. It begins
        // before any engine state is changed so a rollback restores that state.
        let internal_tx = self.tx_depth == 0;
        if internal_tx {
            self.begin_transaction();
        }
        let removed_name = self.active_vtab_ids.remove(&table_id).map(|s| s.to_string());
        let name_to_clean = table_name
            .map(|s| s.trim().to_ascii_lowercase())
            .or(removed_name);
        if let Some(norm_name) = name_to_clean {
            let mut cat_key = Vec::with_capacity(3 + norm_name.len());
            cat_key.extend_from_slice(&[0u8, 0u8, b'c']);
            cat_key.extend_from_slice(norm_name.as_bytes());
            match self.check_existing_row(&cat_key) {
                Ok(ExistingRowStatus::NotFound) => {}
                Ok(_) => records.push(WalRecord::Delete { key: cat_key }),
                Err(e) => {
                    if internal_tx {
                        let _ = self.rollback_transaction();
                    }
                    return Err(e);
                }
            }
        }

        if !records.is_empty() {
            let was_preparing = core::mem::replace(&mut self.tx_preparing, internal_tx);
            let res = self.commit_wal_and_memtable(records, WriteDurability::BufferedFast);
            self.tx_preparing = was_preparing;
            if let Err(e) = res {
                if internal_tx {
                    let _ = self.rollback_transaction();
                }
                return Err(e);
            }
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

        // Blob reclaim and the quota commit happen exactly once: in `commit_transaction` for the
        // internal and any enclosing transaction alike, via `dropped_tables_in_tx`.
        self.dropped_tables_in_tx.push((table_id, was_capped));
        if internal_tx {
            self.commit_transaction()?;
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
        self.drain_corrupt_evictions()?;
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
            match self.get_row_meta_lookup(&meta_key)? {
                MetaLookup::Found(old_meta, m_ptr) => {
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
                            key: encode_secondary_index_key(
                                table_id,
                                *old_idx_id,
                                old_col_val,
                                pk,
                            ),
                        });
                    }
                }
                MetaLookup::Corrupt(m_ptr) => {
                    // Unreadable metadata: find the old 'i'/'l' entries by scanning for `pk`.
                    had_old_meta = true;
                    old_meta_blob_ptr = m_ptr;
                    let (orphans, est) = self.collect_orphans_for_pk(table_id, pk, &row_key)?;
                    old_logical_size = est;
                    for key in orphans {
                        records.push(WalRecord::Delete { key });
                    }
                }
                MetaLookup::Absent => {}
            }
        }

        // 2. Write out-of-line blob FIRST if `value.len() >= effective_inline_threshold` or if inline entry exceeds 4 KB block
        let inline_thresh = self.effective_inline_threshold(table_id);
        let store_as_blob = value.len() >= inline_thresh
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
            // The row is committed; an exceeded quota is re-evaluated by the next write.
            if let Err(e) = self.evict_until_within_quota(table_id, durability) {
                self.record_maintenance_failure(&e);
            }
        }

        Ok(())
    }

    /// Point lookup of a row (`pk`) in `table_id` (owner view: sees this engine's own
    /// transaction overlay). If `table_id` is a Capped Table, updates its `AccessTick`
    /// according to `config.lru_read_tracking`.
    pub fn get_row(&mut self, table_id: u16, pk: &[u8]) -> io::Result<Option<Vec<u8>>> {
        self.owner_read(|eng, cx| eng.get_row_cx(cx, table_id, pk))
    }

    pub(super) fn get_row_cx(
        &self,
        cx: &ReadCtx,
        table_id: u16,
        pk: &[u8],
    ) -> io::Result<Option<Vec<u8>>> {
        let row_key = encode_primary_row_key(table_id, pk);
        let Some((_lsn, idx_val)) = self.lookup_raw_entry_view(cx.view, &row_key)? else {
            return Ok(None);
        };

        let resolved = match idx_val {
            IndexValue::Inline(bytes) => Some(bytes),
            IndexValue::Blob(ptr) => {
                self.ensure_flushed_cx(cx, &ptr)?;
                match self.read_blob_ref(&ptr, &row_key)? {
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

    /// Records an LRU read hit for capped tables: queued for an eager WAL promotion when the
    /// reader has exclusive access and `EagerWriteOnRead` is configured, otherwise coalesced
    /// in RAM (interior-mutable tracker, safe under a shared lock).
    #[inline]
    pub(super) fn track_read_hit(&self, cx: &ReadCtx, table_id: u16, pk: &[u8]) {
        if !self.capped_tables.contains_key(&table_id) {
            return;
        }
        if cx.eager_ok
            && cx.view == ReadView::Owner
            && self.config.lru_read_tracking == LruReadTrackingMode::EagerWriteOnRead
        {
            cx.eager_hits.borrow_mut().push((table_id, pk.to_vec()));
        } else {
            self.access_tracker.record_read_hit(table_id, pk);
        }
    }

    /// Whether `ptr` lives in an active writer's not-yet-flushed buffer.
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

    /// True if any active blob writer holds buffered (unflushed) bytes.
    pub(super) fn any_blob_unflushed(&self) -> bool {
        self.active_std_writer
            .as_ref()
            .is_some_and(|w| w.current_len() > w.flushed_offset())
            || self
                .active_lru_writers
                .values()
                .any(|w| w.current_len() > w.flushed_offset())
    }

    /// Read-path replacement for `ensure_blob_flushed` under `&self`. Committed blobs are always
    /// flushed to the OS handle before their WAL record is appended, so a committed-view read
    /// never finds an unflushed pointer (debug-asserted). The owner view may reference its own
    /// uncommitted blobs: the read is aborted with `cx.need_flush` set and the exclusive caller
    /// flushes and retries.
    #[inline]
    pub(super) fn ensure_flushed_cx(&self, cx: &ReadCtx, ptr: &BlobPtr) -> io::Result<()> {
        if !self.blob_unflushed(ptr) {
            return Ok(());
        }
        debug_assert!(
            cx.view == ReadView::Owner,
            "committed-view read hit an unflushed blob pointer"
        );
        cx.need_flush.set(true);
        Err(io::Error::other(NEED_FLUSH_MSG))
    }

    /// Owner-view scans that read blob payloads need every buffered byte on the OS handle.
    pub(super) fn ensure_all_flushed_cx(&self, cx: &ReadCtx) -> io::Result<()> {
        if cx.view == ReadView::Owner && self.any_blob_unflushed() {
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
        if self.tx_depth > 0 && self.tx_owner_conn != Some(conn_id) {
            ReadView::Committed
        } else {
            ReadView::Owner
        }
    }

    /// True if reads of `table_id` by `conn_id` need `&mut` access (exclusive lock): the
    /// connection owns the active transaction (its reads may touch its own unflushed blobs), or
    /// `EagerWriteOnRead` tracking applies to a capped table. Otherwise
    /// [`Self::reader_for_conn`] under a shared lock is sufficient.
    pub fn read_needs_exclusive(&self, conn_id: usize, table_id: u16) -> bool {
        if self.read_view_for_conn(conn_id) == ReadView::Committed {
            return false;
        }
        self.tx_depth > 0
            || (self.config.lru_read_tracking == LruReadTrackingMode::EagerWriteOnRead
                && self.capped_tables.contains_key(&table_id))
    }

    /// True if a shared-lock read by `conn_id` on `table_id` must upgrade to an exclusive lock
    /// before attempting [`Self::reader_for_conn`]: either `EagerWriteOnRead` tracking applies to
    /// a capped table, or `conn_id` owns an active transaction that currently has buffered
    /// (unflushed) blob bytes.
    #[inline]
    pub fn vtab_read_needs_exclusive(&self, conn_id: usize, table_id: u16) -> bool {
        if self.read_view_for_conn(conn_id) == ReadView::Committed {
            return false;
        }
        (self.tx_depth > 0 && self.any_blob_unflushed())
            || (self.config.lru_read_tracking == LruReadTrackingMode::EagerWriteOnRead
                && self.capped_tables.contains_key(&table_id))
    }

    /// Shared-access reader for `conn_id` (see [`EngineReader`]). Must only be used when
    /// [`Self::read_needs_exclusive`] is `false`; otherwise reads may fail with an
    /// "unflushed blob" error and `EagerWriteOnRead` is downgraded to coalesced tracking.
    pub fn reader_for_conn(&self, conn_id: usize) -> EngineReader<'_, FS> {
        EngineReader {
            eng: self,
            cx: CxRef::Owned(ReadCtx::new(self.read_view_for_conn(conn_id), false)),
        }
    }

    /// Runs `f` with exclusive access in `view`: flushes buffered blobs and retries when the
    /// reader asks for it, then applies queued eager LRU promotions. The second tuple element
    /// carries a flush/promotion error (the read result itself is returned untouched).
    pub(super) fn run_read<R>(
        &mut self,
        view: ReadView,
        mut f: impl FnMut(&Self, &ReadCtx) -> R,
    ) -> (R, Option<io::Error>) {
        let mut attempts = 0;
        loop {
            let cx = ReadCtx::new(view, true);
            let r = f(&*self, &cx);
            if cx.need_flush.get() && attempts < 2 {
                attempts += 1;
                if let Err(e) = self.flush_active_blob_buffers() {
                    return (r, Some(e));
                }
                continue;
            }
            let hits = cx.eager_hits.take();
            let mut err = None;
            for (table_id, pk) in hits {
                if let Err(e) = self.eager_promote_lru_on_read(table_id, &pk) {
                    err = Some(e);
                    break;
                }
            }
            return (r, err);
        }
    }

    pub(super) fn owner_read<R>(
        &mut self,
        f: impl FnMut(&Self, &ReadCtx) -> io::Result<R>,
    ) -> io::Result<R> {
        let (r, extra) = self.run_read(ReadView::Owner, f);
        match (r, extra) {
            (Ok(v), None) => Ok(v),
            (Ok(_), Some(e)) => Err(e),
            (Err(e), Some(flush_err)) if e.to_string() == NEED_FLUSH_MSG => Err(flush_err),
            (Err(e), _) => Err(e),
        }
    }

    /// Deletes a row (`pk`) in `table_id`, cascading tombstones to its secondary indexes and LRU queue.
    pub fn delete_row(
        &mut self,
        table_id: u16,
        pk: &[u8],
        durability: WriteDurability,
    ) -> io::Result<bool> {
        self.check_not_poisoned()?;
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
        let mut old_logical_size = 0u64;
        let mut old_meta_blob_ptr = None;

        if self.may_have_row_meta(table_id)? {
            let meta_key = encode_row_meta_key(table_id, pk);
            match self.get_row_meta_lookup(&meta_key)? {
                MetaLookup::Found(old_meta, m_ptr) => {
                    old_meta_blob_ptr = m_ptr;
                    old_logical_size = old_meta.logical_size as u64;
                    if old_meta.recorded_tick > 0 {
                        records.push(WalRecord::Delete {
                            key: encode_lru_queue_key(table_id, old_meta.recorded_tick, pk),
                        });
                    }
                    for (old_idx_id, old_col_val) in &old_meta.secondary_keys {
                        records.push(WalRecord::Delete {
                            key: encode_secondary_index_key(
                                table_id,
                                *old_idx_id,
                                old_col_val,
                                pk,
                            ),
                        });
                    }
                    records.push(WalRecord::Delete { key: meta_key });
                }
                MetaLookup::Corrupt(m_ptr) => {
                    // Unreadable metadata: remove the 't' record and every 'i'/'l' entry for
                    // `pk` found by scanning. The capped-table byte counter is reduced by an
                    // estimate (pk length + value length taken from the row record).
                    old_meta_blob_ptr = m_ptr;
                    let (orphans, est) = self.collect_orphans_for_pk(table_id, pk, &row_key)?;
                    old_logical_size = est;
                    for key in orphans {
                        records.push(WalRecord::Delete { key });
                    }
                    records.push(WalRecord::Delete { key: meta_key });
                }
                MetaLookup::Absent => {}
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
        self.owner_read(|eng, cx| eng.get_row_into_cx(cx, table_id, pk, slab, spans))
    }

    pub(super) fn get_row_into_cx(
        &self,
        cx: &ReadCtx,
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
        if let Some(mem_entry) = self.mem_get_view(cx.view, row_key) {
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
            for seg in self.idx_segments.iter().rev() {
                let start = slab.len();
                let probe = seg.visit_hashed(row_key, hash, |raw| probe_raw_into_slab(raw, slab))?;
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
        cx: &ReadCtx,
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
        if let Some(mem_entry) = self.mem_get_view(cx.view, row_key) {
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
                        lazy_blobs.push(Some(self.pin_lazy_blob(ptr)));
                        hit = true;
                    } else {
                        let start = slab.len();
                        if self.read_flushed_blob_append_into(ptr, row_key, slab)? {
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
            for seg in self.idx_segments.iter().rev() {
                let start = slab.len();
                let probe = seg.visit_hashed(row_key, hash, |raw| probe_raw_into_slab(raw, slab))?;
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
                                lazy_blobs.push(Some(self.pin_lazy_blob(&ptr)));
                                hit = true;
                            } else if self.read_flushed_blob_append_into(&ptr, row_key, slab)? {
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

    /// Sets the runtime blob-corruption policy for `table_id` (see [`CorruptPolicy`]).
    pub fn set_table_corrupt_policy(&mut self, table_id: u16, policy: CorruptPolicy) {
        self.corrupt_policies.insert(table_id, policy);
    }

    /// Removes any explicit policy for `table_id`, restoring the derived default.
    pub fn reset_table_corrupt_policy(&mut self, table_id: u16) {
        self.corrupt_policies.remove(&table_id);
    }

    /// Effective policy for `table_id`: the explicit setting, else `Evict` for capped tables and
    /// `Error` for all others.
    pub fn corrupt_policy(&self, table_id: u16) -> CorruptPolicy {
        match self.corrupt_policies.get(&table_id) {
            Some(p) => *p,
            None if self.capped_tables.contains_key(&table_id) => CorruptPolicy::Evict,
            None => CorruptPolicy::Error,
        }
    }

    /// Sets the per-table inline threshold override in bytes.
    pub fn set_table_inline_threshold(&mut self, table_id: u16, bytes: usize) {
        self.table_inline_thresholds.insert(table_id, bytes);
    }

    /// Removes any per-table inline threshold override for `table_id`, restoring default.
    pub fn reset_table_inline_threshold(&mut self, table_id: u16) {
        self.table_inline_thresholds.remove(&table_id);
    }

    /// Effective inline payload threshold for `table_id`.
    /// - If explicitly configured via `inline_max` / `set_table_inline_threshold`, returns that value.
    /// - Else if `table_id` is a capped table, returns `self.config.inline_threshold_bytes.min(DEFAULT_INLINE_THRESHOLD)` (1024),
    ///   preserving O(1) copyless LRU segment eviction.
    /// - Else returns `self.config.inline_threshold_bytes`.
    pub fn effective_inline_threshold(&self, table_id: u16) -> usize {
        if let Some(&thresh) = self.table_inline_thresholds.get(&table_id) {
            return thresh;
        }
        if self.capped_tables.contains_key(&table_id) {
            self.config.inline_threshold_bytes.min(DEFAULT_INLINE_THRESHOLD)
        } else {
            self.config.inline_threshold_bytes
        }
    }

    /// Reference to the engine's in-process verified block cache.
    pub fn block_cache(&self) -> &Arc<VerifiedBlockCache> {
        &self.block_cache
    }

    /// Reference to the engine's in-process small blob cache.
    pub fn blob_cache(&self) -> &Arc<VerifiedBlobCache> {
        &self.blob_cache
    }

    /// Number of blob verification failures (or dangling blob pointers) detected since open.
    pub fn corrupt_blob_detections(&self) -> u64 {
        self.corrupt_blob_detections.load(Ordering::Relaxed)
    }

    /// Records a detection only (used by cleanup paths that must tolerate corruption).
    fn count_corrupt_detection(&self) {
        self.corrupt_blob_detections.fetch_add(1, Ordering::Relaxed);
    }

    /// Applies the table's policy to a failed blob read of the record at `key`
    /// (`[TableID | ns | pk]`), whose pointer is `ptr`. `Error` returns an `InvalidData` error
    /// naming the table and primary key; `Evict` queues the row for deletion and returns `Ok`.
    fn note_corrupt_blob(&self, key: &[u8], ptr: &BlobPtr) -> io::Result<()> {
        self.count_corrupt_detection();
        let (table_id, pk) = if key.len() >= 3 {
            (u16::from_be_bytes([key[0], key[1]]), &key[3..])
        } else {
            (ptr.table_id, &key[..0])
        };
        match self.corrupt_policy(table_id) {
            CorruptPolicy::Error => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "corrupt blob (verification failed or segment missing) for table {table_id} pk 0x{} (segment {}, offset {})",
                    pk.iter().map(|b| format!("{b:02x}")).collect::<String>(),
                    ptr.seg_id,
                    ptr.offset
                ),
            )),
            CorruptPolicy::Evict => {
                if let Ok(mut q) = self.pending_corrupt_evictions.lock() {
                    q.insert(
                        (table_id, pk.to_vec()),
                        PendingCorruptEviction {
                            key: key.to_vec(),
                            ptr: *ptr,
                        },
                    );
                }
                Ok(())
            }
        }
    }

    /// Deletes rows queued by [`CorruptPolicy::Evict`] reads.
    ///
    /// Funnel points: the start of `put_row_owned_with_hint`, `delete_row` and `flush_memtable`,
    /// plus the end of the outermost `commit_transaction` (SQL statements write inside a
    /// transaction, where draining is deferred). Never called from read paths, so a
    /// connection that merely reads never writes. Does nothing while a transaction is active
    /// or the engine is poisoned (entries stay queued). Each entry is re-checked: the row is
    /// only deleted if its record still points to the same corrupt `BlobPtr` (it may have been
    /// overwritten or deleted since). Deletes use `BufferedFast` durability.
    pub(super) fn drain_corrupt_evictions(&mut self) -> io::Result<()> {
        if self.tx_depth > 0 || self.poisoned.is_some() {
            return Ok(());
        }
        let pending: Vec<((u16, Vec<u8>), PendingCorruptEviction)> =
            match self.pending_corrupt_evictions.lock() {
                Ok(mut q) if !q.is_empty() => q.drain().collect(),
                _ => return Ok(()),
            };
        for ((table_id, pk), ev) in pending {
            let still_corrupt = matches!(
                self.lookup_raw_entry(&ev.key)?,
                Some((_, IndexValue::Blob(p))) if p == ev.ptr
            );
            if still_corrupt {
                self.delete_row(table_id, &pk, WriteDurability::BufferedFast)?;
            }
        }
        Ok(())
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
        self.owner_read(|eng, cx| eng.dereference_lazy_blob_into_cx(cx, table_id, pk, lazy, slab))
    }

    pub(super) fn dereference_lazy_blob_into_cx(
        &self,
        cx: &ReadCtx,
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

        // The pinned handle must see the blob bytes: committed blobs are always flushed; the
        // owner view may need its writer buffer flushed first (exclusive caller retries).
        self.ensure_flushed_cx(cx, ptr)?;

        let cache_key = BlobCacheKey {
            kind: ptr.kind as u8,
            table_id: ptr.table_id,
            seg_id: ptr.seg_id,
            offset: ptr.offset,
        };
        let start = slab.len();
        if self.blob_cache.get_append_into(&cache_key, slab) {
            let end = slab.len();
            self.track_read_hit(cx, table_id, pk);
            return Ok(Some((start, end)));
        }

        let Some(reader) = &lazy.reader else {
            // Segment was already absent when the pointer was captured: dangling pointer.
            self.note_corrupt_blob(row_key, ptr)?;
            return Ok(None);
        };
        if !reader.append_blob_into(ptr, Some(row_key), slab)? {
            slab.truncate(start);
            self.note_corrupt_blob(row_key, ptr)?;
            return Ok(None);
        }
        let end = slab.len();

        if self.is_blob_eligible_for_cache(ptr) {
            self.blob_cache.insert(cache_key, &slab[start..end]);
        }

        self.track_read_hit(cx, table_id, pk);

        Ok(Some((start, end)))
    }

    /// Prefetches and verifies a batch of upcoming out-of-line lazy blobs using [`PlatformFile::read_many_at`]
    /// into [`VerifiedBlobCache`], avoiding per-row system calls on subsequent cursor accesses.
    pub fn prefetch_lazy_blobs(
        &mut self,
        table_id: u16,
        candidates: &[(&[u8], &LazyBlob<FS::File>)],
    ) -> io::Result<()> {
        self.owner_read(|eng, cx| eng.prefetch_lazy_blobs_cx(cx, table_id, candidates))
    }

    pub(super) fn prefetch_lazy_blobs_cx(
        &self,
        _cx: &ReadCtx,
        table_id: u16,
        candidates: &[(&[u8], &LazyBlob<FS::File>)],
    ) -> io::Result<()> {
        if self.config.blob_cache_bytes == 0 || candidates.is_empty() {
            return Ok(());
        }

        struct PrefetchItem<'a, F: PlatformFile> {
            cache_key: BlobCacheKey,
            row_key: Vec<u8>,
            ptr: BlobPtr,
            reader: &'a BlobSegmentReader<F>,
        }

        let mut to_fetch: Vec<PrefetchItem<'_, FS::File>> = Vec::with_capacity(candidates.len());
        for (pk, lazy) in candidates {
            let ptr = &lazy.ptr;
            if !self.is_blob_eligible_for_cache(ptr) {
                continue;
            }
            let cache_key = BlobCacheKey {
                kind: ptr.kind as u8,
                table_id: ptr.table_id,
                seg_id: ptr.seg_id,
                offset: ptr.offset,
            };
            if self.blob_cache.contains(&cache_key) {
                continue;
            }
            if let Some(ref reader) = lazy.reader {
                let row_key = encode_primary_row_key(table_id, pk);
                to_fetch.push(PrefetchItem {
                    cache_key,
                    row_key,
                    ptr: *ptr,
                    reader,
                });
            }
        }

        if to_fetch.is_empty() {
            return Ok(());
        }

        to_fetch.sort_unstable_by_key(|c| (c.ptr.kind as u8, c.ptr.table_id, c.ptr.seg_id));

        let mut group_start = 0;
        while group_start < to_fetch.len() {
            let cur_kind = to_fetch[group_start].ptr.kind;
            let cur_tbl = to_fetch[group_start].ptr.table_id;
            let cur_seg = to_fetch[group_start].ptr.seg_id;

            let mut group_end = group_start + 1;
            while group_end < to_fetch.len()
                && to_fetch[group_end].ptr.kind == cur_kind
                && to_fetch[group_end].ptr.table_id == cur_tbl
                && to_fetch[group_end].ptr.seg_id == cur_seg
            {
                group_end += 1;
            }

            let group = &to_fetch[group_start..group_end];
            let reader = group[0].reader;

            let mut bufs: Vec<Vec<u8>> = group
                .iter()
                .map(|c| vec![0u8; c.ptr.len as usize])
                .collect();
            let mut reqs: Vec<(u64, &mut [u8])> = group
                .iter()
                .zip(bufs.iter_mut())
                .map(|(c, buf)| (c.ptr.offset as u64, buf.as_mut_slice()))
                .collect();

            if reader.file().read_many_at(&mut reqs).is_ok() {
                for (cand, buf) in group.iter().zip(bufs.iter()) {
                    if let Some(val) = BlobSegmentReader::<FS::File>::verify_blob_record(
                        buf,
                        cand.ptr.crc32c,
                        Some(&cand.row_key),
                    ) {
                        self.blob_cache.insert(cand.cache_key, val);
                    }
                }
            }

            group_start = group_end;
        }

        Ok(())
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

    /// Reads and verifies a blob; on failure applies the table's [`CorruptPolicy`]
    /// (`Err(InvalidData)` for `Error`, `Ok(None)` + queued eviction for `Evict`).
    pub(super) fn dereference_blob(
        &mut self,
        ptr: &BlobPtr,
        expected_key: &[u8],
    ) -> io::Result<Option<Vec<u8>>> {
        let r = self.dereference_blob_raw(ptr, expected_key)?;
        if r.is_none() {
            self.note_corrupt_blob(expected_key, ptr)?;
        }
        Ok(r)
    }

    /// Like [`Self::dereference_blob`] but never applies the policy: a failure is only counted and
    /// reported as `Ok(None)`. For cleanup/maintenance paths (delete, overwrite, compaction
    /// relocation, capped-table reconciliation) that must tolerate corrupt blobs.
    pub(super) fn dereference_blob_lenient(
        &mut self,
        ptr: &BlobPtr,
        expected_key: &[u8],
    ) -> io::Result<Option<Vec<u8>>> {
        let r = self.dereference_blob_raw(ptr, expected_key)?;
        if r.is_none() {
            self.count_corrupt_detection();
        }
        Ok(r)
    }

    fn dereference_blob_raw(
        &mut self,
        ptr: &BlobPtr,
        expected_key: &[u8],
    ) -> io::Result<Option<Vec<u8>>> {
        self.ensure_blob_flushed(ptr)?;
        self.read_blob_ref(ptr, expected_key)
    }

    fn is_blob_eligible_for_cache(&self, ptr: &BlobPtr) -> bool {
        if self.config.blob_cache_bytes == 0 || (ptr.len as usize) > 16 * 1024 {
            return false;
        }
        match ptr.kind {
            BlobKind::Standard => {
                if let Some(w) = &self.active_std_writer {
                    if w.seq_id() == ptr.seg_id {
                        return (ptr.offset as u64) + (ptr.len as u64) <= w.flushed_offset();
                    }
                }
                true
            }
            BlobKind::Lru => {
                if let Some(w) = self.active_lru_writers.get(&ptr.table_id) {
                    if w.seq_id() == ptr.seg_id {
                        return (ptr.offset as u64) + (ptr.len as u64) <= w.flushed_offset();
                    }
                }
                true
            }
        }
    }

    /// Reads and verifies a blob that is already flushed (no policy applied).
    pub(super) fn read_blob_ref(
        &self,
        ptr: &BlobPtr,
        expected_key: &[u8],
    ) -> io::Result<Option<Vec<u8>>> {
        let mut out = Vec::new();
        let cache_key = BlobCacheKey {
            kind: ptr.kind as u8,
            table_id: ptr.table_id,
            seg_id: ptr.seg_id,
            offset: ptr.offset,
        };
        if self.blob_cache.get_append_into(&cache_key, &mut out) {
            return Ok(Some(out));
        }

        let res = match ptr.kind {
            BlobKind::Standard => {
                let Some(b) = self.std_blobs.get(&ptr.seg_id) else {
                    return Ok(None);
                };
                b.reader.read_blob(ptr, Some(expected_key))?
            }
            BlobKind::Lru => {
                let Some(b) = self.lru_blobs.get(&(ptr.table_id, ptr.seg_id)) else {
                    return Ok(None);
                };
                b.reader.read_blob(ptr, Some(expected_key))?
            }
        };

        if let Some(payload) = &res {
            if self.is_blob_eligible_for_cache(ptr) {
                self.blob_cache.insert(cache_key, payload);
            }
        }
        Ok(res)
    }

    pub(super) fn dereference_blob_append_into_cx(
        &self,
        cx: &ReadCtx,
        ptr: &BlobPtr,
        expected_key: &[u8],
        out: &mut Vec<u8>,
    ) -> io::Result<bool> {
        self.ensure_flushed_cx(cx, ptr)?;
        self.read_flushed_blob_append_into(ptr, expected_key, out)
    }

    pub(super) fn read_flushed_blob_append_into(
        &self,
        ptr: &BlobPtr,
        expected_key: &[u8],
        out: &mut Vec<u8>,
    ) -> io::Result<bool> {
        let cache_key = BlobCacheKey {
            kind: ptr.kind as u8,
            table_id: ptr.table_id,
            seg_id: ptr.seg_id,
            offset: ptr.offset,
        };

        let orig_len = out.len();
        if self.blob_cache.get_append_into(&cache_key, out) {
            return Ok(true);
        }

        let reader = match ptr.kind {
            BlobKind::Standard => self.std_blobs.get(&ptr.seg_id).map(|b| &b.reader),
            BlobKind::Lru => self
                .lru_blobs
                .get(&(ptr.table_id, ptr.seg_id))
                .map(|b| &b.reader),
        };
        let ok = match reader {
            Some(reader) if out.is_empty() => reader.read_blob_into(ptr, Some(expected_key), out)?,
            Some(reader) => reader.append_blob_into(ptr, Some(expected_key), out)?,
            None => false,
        };
        if !ok {
            // Corruption or dangling pointer: apply the table's policy (Err for `Error`).
            self.note_corrupt_blob(expected_key, ptr)?;
        } else if self.is_blob_eligible_for_cache(ptr) {
            self.blob_cache.insert(cache_key, &out[orig_len..]);
        }
        Ok(ok)
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
            let record_undo = !self.savepoints.is_empty();
            for rec in records {
                self.tx_overlay.apply(lsn, rec, record_undo);
            }
            return Ok(lsn);
        }

        self.drain_pending_durable()?;
        let batch = WalBatch::new(lsn, records);
        let mut raw = core::mem::take(&mut self.scratch_wal_encoded);
        batch.encode_into(&mut raw);
        self.flush_active_blob_buffers()?;
        let res = self.append_raw_wal_batch_with_rotation(&raw, durability);
        raw.clear();
        self.scratch_wal_encoded = raw;
        res?;
        self.next_lsn = self.next_lsn.max(lsn + 1);
        self.memtable.apply_wal_batch_owned(batch);
        // The batch is committed; a failed flush is retried because `should_flush` stays true.
        if self.memtable.should_flush() {
            if let Err(e) = self.flush_memtable() {
                self.record_maintenance_failure(&e);
            }
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
            self.make_buf_wal_durable(true)?;
        }

        // A failed append/sync leaves the on-disk outcome of this batch unknown (it may or may not
        // be replayed after restart), so the engine is poisoned and must be reopened.
        let r = match durability {
            WriteDurability::SyncTransactional => self.sync_wal.append_raw_batch(encoded),
            WriteDurability::BufferedFast => self.buf_wal.append_raw_batch(encoded),
        };
        let outcome = r.map_err(|e| self.poison(e))?;

        if outcome == WalAppendOutcome::RingFull {
            self.drain_pending_durable()?;
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
        self.check_existing_row_view(ReadView::Owner, key)
    }

    pub(super) fn check_existing_row_view(
        &self,
        view: ReadView,
        key: &[u8],
    ) -> io::Result<ExistingRowStatus> {
        if let Some(mem_entry) = self.mem_get_view(view, key) {
            return Ok(match &mem_entry.value {
                IndexValue::Tombstone => ExistingRowStatus::NotFound,
                IndexValue::Inline(_) => ExistingRowStatus::Inline,
                IndexValue::Blob(ptr) => ExistingRowStatus::Blob(*ptr),
            });
        }
        let hash = BloomFilter::hash_pair(key);
        for seg in self.idx_segments.iter().rev() {
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
            .scan_merged_prefix_range(&prefix, &prefix, true, None, 1)?
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
                Err(_) => {
                    self.count_corrupt_detection();
                    MetaLookup::Corrupt(None)
                }
            }),
            IndexValue::Blob(ptr) => match self.dereference_blob_lenient(&ptr, meta_key)? {
                Some(bytes) => Ok(match RowMetaRecord::from_bytes(&bytes) {
                    Ok(r) => MetaLookup::Found(r, Some(ptr)),
                    Err(_) => MetaLookup::Corrupt(Some(ptr)),
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
    fn collect_orphans_for_pk(
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

    /// Newest in-memory entry for `key` as seen by `view`: the transaction overlay (hidden for
    /// the committed view) over the committed `MemTable`.
    #[inline]
    pub(super) fn mem_get_view(
        &self,
        view: ReadView,
        key: &[u8],
    ) -> Option<&crate::memtable::MemEntry> {
        if view == ReadView::Owner && self.tx_overlay.may_contain_key(key) {
            if let Some(e) = self.tx_overlay.map.get(key) {
                return Some(e);
            }
        }
        self.memtable.get(key)
    }

    /// Overlay map visible to `view` (empty for the committed view or empty overlay).
    #[inline]
    pub(super) fn visible_overlay(
        &self,
        view: ReadView,
    ) -> &BTreeMap<Vec<u8>, crate::memtable::MemEntry> {
        static EMPTY: BTreeMap<Vec<u8>, crate::memtable::MemEntry> = BTreeMap::new();
        if view == ReadView::Committed || self.tx_overlay.is_empty() {
            &EMPTY
        } else {
            &self.tx_overlay.map
        }
    }

    pub(super) fn lookup_raw_entry(&self, key: &[u8]) -> io::Result<Option<(u64, IndexValue)>> {
        self.lookup_raw_entry_view(ReadView::Owner, key)
    }

    pub(super) fn lookup_raw_entry_view(
        &self,
        view: ReadView,
        key: &[u8],
    ) -> io::Result<Option<(u64, IndexValue)>> {
        if let Some(mem_entry) = self.mem_get_view(view, key) {
            return Ok(Some((mem_entry.lsn, mem_entry.value.clone())));
        }
        let hash = BloomFilter::hash_pair(key);
        for seg in self.idx_segments.iter().rev() {
            if let Some((lsn, val)) = seg.get_hashed(key, hash)? {
                return Ok(Some((lsn, val)));
            }
        }
        Ok(None)
    }
}
