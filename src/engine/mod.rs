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
mod rowid;
mod salvage;
mod scan;
mod snapshot;
mod tx;
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

use crate::blob::{
    BLOB_RECORD_HEADER_LEN, BlobSegmentReader, BlobSegmentWriter, DEFAULT_INLINE_THRESHOLD,
    DEFAULT_MAX_BLOB_SEGMENT_SIZE,
};
use crate::cache::{BlobCacheKey, VerifiedBlobCache, VerifiedBlockCache};
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
use crate::meta::{
    BootMetaManager, EvictionPolicy, LruBlobSegmentMeta, StdBlobSegmentMeta,
    format_lru_blob_file_name, format_std_blob_file_name,
};
use crate::pal::{OpenHint, PlatformFile, PlatformFs};
use crate::wal::{
    BlobKind, BlobPtr, DEFAULT_BUF_WAL_SIZE, DEFAULT_SYNC_WAL_SIZE, WAL_BUF_FILE_NAME,
    WAL_SYNC_FILE_NAME, WalAppendOutcome, WalBatch, WalRecord, WalScanMode, WalStreamMode,
    WalStreamWriter, recover_two_streams_keyed,
};
pub use crate::wal::{StreamCounts, WalDamage, WalDamageReason, WalRecoveryReport};

use self::reader::{CxRef, NEED_FLUSH_MSG, ReadCtx, ReadSource};
pub use self::reader::{EngineReader, ReadView};
use self::tx::{SavepointMarker, TxMetaSnapshot};
use self::wal_links::LogStream;

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

/// A queued automatic deletion of a row with a corrupt blob or malformed metadata.
struct PendingCorruptEviction {
    /// Key of the corrupt record (`'r'` row or `'t'` row-meta).
    key: Vec<u8>,
    /// The record observed as corrupt. A subsequent overwrite cancels the deletion.
    observed: IndexValue,
}

/// The outcome of resolving a virtual table name to a `table_id` without registering it.
pub(crate) struct VtabPlan {
    /// The lower-cased, trimmed table name.
    pub(crate) norm_name: String,
    /// The `table_id` the table resolves to (an unused one for a table that is not persisted).
    pub(crate) table_id: u16,
    /// Whether the committed catalog already maps `norm_name` to `table_id`.
    pub(crate) persisted: bool,
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
    engine_id: u64,
    snapshot_cache: RwLock<snapshot::SnapshotCache<FS::File>>,
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
    pub(super) pending_group:
        std::collections::VecDeque<(u64, BTreeMap<Vec<u8>, crate::memtable::MemEntry>)>,
    /// Fully dead `blob_lru` segments that were dropped from the catalog but not yet queued for
    /// deletion, because a WAL batch of the current epoch may still carry a pointer into them.
    /// They are queued in the commit that rotates the WAL epochs (see
    /// [`Self::release_unlinkable_blob_files`]). A crash before that leaves them as uncataloged
    /// files, which recovery adopts if a batch references them and sweeps otherwise.
    pub(super) unlink_after_wal_rotation: Vec<String>,
    /// True if `flush_memtable()` was requested during an active transaction.
    pub(super) deferred_flush_on_commit: bool,
    /// True if `compact()` was requested during an active transaction.
    pub(super) deferred_compact_on_commit: bool,
    /// Set when a full-WAL rotation flushed the MemTable without auto compaction; consumed by
    /// `run_due_auto_compaction` at the next point where no batch holds an unappended LSN.
    pub(super) auto_compaction_due: bool,
    /// Re-entrancy guard preventing `relocate_fragmented_std_blobs` from triggering another
    /// relocation pass if its internal MemTable flush invokes auto-compaction.
    pub(super) relocating_std_blobs: bool,
    /// Invoked (under the engine lock, so it must be cheap and non-blocking) when a flush leaves
    /// compaction pending in background mode.
    pub(super) compaction_hook: Option<Arc<dyn Fn() + Send + Sync>>,
    /// Generation id of the worker that owns [`Self::compaction_hook`] so an older worker's teardown
    /// cannot clear a newly registered worker's hook.
    pub(super) compaction_hook_id: u64,
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
    pub(super) statement_savepoint_active: bool,
    pub(super) statement_blob_undo: Vec<tx::BlobUndo>,
    /// Tables dropped inside the currently active transaction (`(table_id, was_capped)`).
    pub(super) dropped_tables_in_tx: Vec<(u16, bool)>,
    /// Virtual tables created (`xCreate`) inside the currently active transaction, as
    /// `(table_id, normalised name, normalised sqlite schema name)`.
    pub(super) vtabs_created_in_tx: Vec<(u16, String, String)>,
    /// Virtual-table pointers (`p_vtab as usize`) that joined the active SQLite transaction via
    /// `xBegin` and have not yet reached `xSync`.
    pub(super) tx_pending_sync_vtabs: Vec<usize>,
    /// True if any capped-table quota was registered or modified inside the active transaction.
    pub(super) quotas_modified_in_tx: bool,
    /// Registry of active virtual tables (`table_id -> table_name`) preventing duplicate `table_id` sharing.
    pub(super) active_vtab_ids: HashMap<u16, Arc<str>>,
    /// Monotonic registration token counter for live virtual table instances.
    next_vtab_token: u64,
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
    pub(super) wal_links: wal_links::WalLinks,
    /// What recovery found in the WAL during `open` (section 4.6.8).
    last_wal_recovery: WalRecoveryReport,
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
    pub(super) corrupt_policies: HashMap<u16, CorruptPolicy>,
    /// Explicit per-table inline threshold overrides in bytes (see [`Self::set_table_inline_threshold`]).
    pub(super) table_inline_thresholds: BTreeMap<u16, usize>,
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
    /// Test seam: makes the next `append_raw_wal_batch_with_rotation` fail and poison the engine.
    #[cfg(test)]
    pub(super) debug_fail_next_wal_append: bool,
    /// Test seam: makes the next vtab commit `sync_barrier` fail as a device flush failure would.
    #[cfg(test)]
    pub(super) debug_fail_next_wal_sync: Arc<std::sync::atomic::AtomicBool>,
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

fn record_post_catalog_span<K: Ord>(
    spans: &mut BTreeMap<K, Vec<(u64, u64)>>,
    key: K,
    offset: u64,
    end: u64,
    cataloged_len: u64,
    file_len: u64,
) {
    let start = offset.max(cataloged_len);
    let clamped_end = end.min(file_len);
    if clamped_end > start {
        spans.entry(key).or_default().push((start, clamped_end));
    }
}

/// Make the recovered tail dirty again before syncing it. A failed writeback can leave the
/// correct bytes readable in cache but clean, so a bare sync on reopen may do nothing.
fn persist_recovered_blob_tail(
    file: &impl PlatformFile,
    mut offset: u64,
    end: u64,
) -> io::Result<()> {
    if offset >= end {
        return Ok(());
    }
    let mut buf = vec![0; (end - offset).min(64 * 1024) as usize];
    while offset < end {
        let n = (end - offset).min(buf.len() as u64) as usize;
        file.read_exact_at(&mut buf[..n], offset)?;
        file.write_all_at(&buf[..n], offset)?;
        offset += n as u64;
    }
    file.sync_durable()
}

fn merged_span_bytes(spans: &mut [(u64, u64)]) -> u64 {
    if spans.is_empty() {
        return 0;
    }
    spans.sort_unstable();
    let mut total = 0u64;
    let mut cur_start = spans[0].0;
    let mut cur_end = spans[0].1;
    for &(s, e) in &spans[1..] {
        if s <= cur_end {
            cur_end = cur_end.max(e);
        } else {
            total += cur_end - cur_start;
            cur_start = s;
            cur_end = e;
        }
    }
    total + (cur_end - cur_start)
}

/// Normalizes a virtual-table or schema identifier according to SQLite's ASCII case-folding
/// while preserving any leading, trailing, or internal whitespace.
#[inline]
pub(crate) fn normalize_vtab_ident(name: &str) -> String {
    name.to_ascii_lowercase()
}

pub(crate) fn find_free_table_id(mut is_used: impl FnMut(u16) -> bool) -> io::Result<u16> {
    let mut candidate = 1u16;
    loop {
        if !is_used(candidate) {
            return Ok(candidate);
        }
        match candidate.checked_add(1) {
            Some(next) => candidate = next,
            None => {
                return Err(io::Error::other(
                    "all 65535 virtual table_ids are exhausted",
                ));
            }
        }
    }
}

fn account_post_catalog_gap<K: Ord + std::hash::Hash + Copy>(
    key: K,
    file_len: u64,
    dead_bytes: &mut u64,
    cataloged_len: &HashMap<K, u64>,
    spans: &mut BTreeMap<K, Vec<(u64, u64)>>,
    base_dead: &BTreeMap<K, u64>,
) -> u64 {
    let cat_len = cataloged_len.get(&key).copied().unwrap_or(0);
    let post_catalog_total = file_len.saturating_sub(cat_len);
    let covered = spans
        .get_mut(&key)
        .map(|v| merged_span_bytes(v))
        .unwrap_or(0);
    let unaccounted = post_catalog_total.saturating_sub(covered);
    let mut persisted = base_dead.get(&key).copied().unwrap_or(0);
    if unaccounted > 0 {
        *dead_bytes = dead_bytes.saturating_add(unaccounted).min(file_len);
        persisted = persisted.saturating_add(unaccounted).min(file_len);
    }
    persisted
}

impl<FS: PlatformFs> StorageEngine<FS> {
    /// Opens or recovers a `StorageEngine` rooted at `fs`.
    pub fn open(fs: Arc<FS>, config: EngineConfig) -> io::Result<Self> {
        Ok(Self::open_inner(fs, config, false)?.0)
    }

    /// Opens as [`WalDamagePolicy::Truncate`] does, whatever `config.wal_damage_policy` says,
    /// and returns the transactions that recovery discarded at or after the cut
    /// (ARCHITECTURE.md 4.6.7), in LSN order. With no damage the list is empty and this is
    /// exactly [`Self::open`].
    ///
    /// Only transactions whose batches are all intact are returned, and only their primary-row
    /// operations, in the encoding of the engine (`key` and `value` as `put_row` takes them, with
    /// the table's catalog name; a value kept in a blob is read before the open reclaims it).
    /// Secondary and unique index entries, rowid mappings and row metadata are derived data and
    /// are left out. A transaction that contains a schema change, a row of a table the catalog
    /// does not know, or a value whose blob fails verification is not returned; its operations of
    /// that kind are counted in [`WalDamage::unsalvageable`]. A multi-chunk transaction that lost
    /// a chunk is neither returned nor counted.
    ///
    /// The list lives only in memory. A crash before the open has rotated the WAL epochs
    /// reproduces it at the next open; afterwards the discarded batches are gone.
    pub fn open_with_salvage(
        fs: Arc<FS>,
        mut config: EngineConfig,
    ) -> io::Result<(Self, Vec<SalvagedTx>)> {
        config.wal_damage_policy = WalDamagePolicy::Truncate;
        Self::open_inner(fs, config, true)
    }

    /// The common path of [`Self::open`] and [`Self::open_with_salvage`]. `with_salvage` asks for
    /// the discarded transactions; the `unsalvageable` count of the report is computed either way.
    fn open_inner(
        fs: Arc<FS>,
        config: EngineConfig,
        with_salvage: bool,
    ) -> io::Result<(Self, Vec<SalvagedTx>)> {
        let mut meta_mgr = BootMetaManager::open_or_recover_deferred_sweep(fs.clone())?;
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
        let mut std_post_catalog_spans: BTreeMap<u64, Vec<(u64, u64)>> = BTreeMap::new();
        let mut cataloged_std_len: HashMap<u64, u64> =
            HashMap::with_capacity(state_snapshot.std_blob_segments.len());
        for bmeta in &state_snapshot.std_blob_segments {
            let fname = format_std_blob_file_name(bmeta.seq_id);
            let file = fs.open_file(&fname, false, true, OpenHint::ImmutableSequential)?;
            let physical_len = file.file_len()?;
            let file_len = physical_len.max(bmeta.file_len);
            // The catalog already accounts for every byte below its length, so only bytes beyond
            // it can be unaccounted garbage. A file shorter than the catalog (its tail never
            // reached the device) must not make the missing bytes count as garbage a second
            // time: recovery has to give the same result however often it runs.
            cataloged_std_len.insert(bmeta.seq_id, bmeta.file_len);
            std_blobs.insert(
                bmeta.seq_id,
                TrackedStdBlob {
                    reader: BlobSegmentReader::new_with_len(file, bmeta.seq_id, physical_len),
                    file_len,
                    dead_bytes: bmeta.dead_bytes,
                },
            );
        }

        // 3. Open active `blob_lru_<TblID>_<SeqID>.db` segments
        let mut lru_blobs = BTreeMap::new();
        let mut lru_post_catalog_spans: BTreeMap<(u16, u64), Vec<(u64, u64)>> = BTreeMap::new();
        let mut cataloged_lru_len: HashMap<(u16, u64), u64> =
            HashMap::with_capacity(state_snapshot.lru_blob_segments.len());
        for lmeta in &state_snapshot.lru_blob_segments {
            let fname = format_lru_blob_file_name(lmeta.table_id, lmeta.seq_id);
            let file = fs.open_file(&fname, false, true, OpenHint::ImmutableSequential)?;
            let physical_len = file.file_len()?;
            let file_len = physical_len.max(lmeta.file_len);
            // See the `blob_std` loop above.
            cataloged_lru_len.insert((lmeta.table_id, lmeta.seq_id), lmeta.file_len);
            lru_blobs.insert(
                (lmeta.table_id, lmeta.seq_id),
                TrackedLruBlob {
                    table_id: lmeta.table_id,
                    reader: BlobSegmentReader::new_with_len(file, lmeta.seq_id, physical_len),
                    file_len,
                    dead_bytes: lmeta.dead_bytes,
                },
            );
        }

        // Also discover any uncataloged `blob_std_*` or `blob_lru_*` created during the active epoch
        // before a crash so `Write-Blob-Before-WAL` validation can verify their records!
        //
        // A file queued in `pending_delete_files` was dropped from the catalog because it held no
        // live data, and only its unlink is outstanding. It must never be adopted: a stale pointer
        // in an older `idx` segment (counted by the scan below) would catalog it again, and the
        // pending delete of the same commit would then unlink a cataloged file. Such a pointer
        // behaves like any other pointer into a dropped segment: it is dead.
        for fname in fs.list_files()? {
            if state_snapshot.pending_delete_files.contains(&fname) {
                continue;
            }
            if let Some(hex) = fname
                .strip_prefix("blob_std_")
                .and_then(|s| s.strip_suffix(".db"))
            {
                if let Ok(seq_id) = u64::from_str_radix(hex, 16) {
                    if !std_blobs.contains_key(&seq_id) {
                        if let Some(file) = open_uncataloged_blob(&*fs, &fname)? {
                            let flen = file.file_len()?;
                            std_blobs.insert(
                                seq_id,
                                TrackedStdBlob {
                                    reader: BlobSegmentReader::new_with_len(file, seq_id, flen),
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
                                lru_blobs.insert(
                                    (tbl_id, seq_id),
                                    TrackedLruBlob {
                                        table_id: tbl_id,
                                        reader: BlobSegmentReader::new_with_len(file, seq_id, flen),
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

        // For any cataloged blob segment whose physical `file_len` exceeds its cataloged
        // `bmeta.file_len`, also scan `idx_segments` in case an `idx` segment already
        // references blobs in that segment (e.g. if `boot_meta.db` fell back to an older slot).
        let needs_idx_blob_scan = std_blobs.iter().any(|(&seq, b)| {
            cataloged_std_seqs.contains(&seq)
                && b.file_len > cataloged_std_len.get(&seq).copied().unwrap_or(0)
        }) || lru_blobs.iter().any(|(&k, b)| {
            cataloged_lru_keys.contains(&k)
                && b.file_len > cataloged_lru_len.get(&k).copied().unwrap_or(0)
        });
        if needs_idx_blob_scan {
            for seg in &idx_segments {
                let mut cur = seg.cursor_all()?;
                while let Some(entry) = cur.next_entry()? {
                    if let IndexValue::Blob(ptr) = entry.value {
                        let end = (ptr.offset as u64).saturating_add(ptr.len as u64);
                        match ptr.kind {
                            BlobKind::Standard => {
                                if let (Some(&cataloged_len), Some(b)) = (
                                    cataloged_std_len.get(&ptr.seg_id),
                                    std_blobs.get(&ptr.seg_id),
                                ) {
                                    record_post_catalog_span(
                                        &mut std_post_catalog_spans,
                                        ptr.seg_id,
                                        ptr.offset as u64,
                                        end,
                                        cataloged_len,
                                        b.file_len,
                                    );
                                }
                            }
                            BlobKind::Lru => {
                                let key = (ptr.table_id, ptr.seg_id);
                                if let (Some(&cataloged_len), Some(b)) =
                                    (cataloged_lru_len.get(&key), lru_blobs.get(&key))
                                {
                                    record_post_catalog_span(
                                        &mut lru_post_catalog_spans,
                                        key,
                                        ptr.offset as u64,
                                        end,
                                        cataloged_len,
                                        b.file_len,
                                    );
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

        // 4. Open existing `wal_sync.db` and `wal_buf.db` without creating missing files prior to
        //    the `WalDamagePolicy::Fail` decision, then run Two-Stream Recovery.
        let sync_wal_opt = WalStreamWriter::open_existing_unallocated(
            &*fs,
            WAL_SYNC_FILE_NAME,
            config.sync_wal_size_bytes,
            state_snapshot.sync_wal_epoch,
            WalStreamMode::SyncSectorIsolated,
        )?;
        let buf_wal_opt = WalStreamWriter::open_existing_unallocated(
            &*fs,
            WAL_BUF_FILE_NAME,
            config.buf_wal_size_bytes,
            state_snapshot.buf_wal_epoch,
            config.buf_wal_mode,
        )?;

        let scan_mode = if with_salvage || config.wal_damage_policy == WalDamagePolicy::Fail {
            WalScanMode::FullScan
        } else {
            WalScanMode::StopAtEndOfLog
        };
        let mut rec = recover_two_streams_keyed(
            sync_wal_opt.as_ref().map(|w| &**w.file()),
            state_snapshot.sync_wal_epoch,
            buf_wal_opt.as_ref().map(|w| &**w.file()),
            state_snapshot.buf_wal_epoch,
            state_snapshot.checkpoint_lsn,
            scan_mode,
            |key, ptr| match ptr.kind {
                BlobKind::Standard => match std_blobs.get(&ptr.seg_id) {
                    Some(b) => b.reader.validate_blob_ptr_for_key(ptr, Some(key)),
                    None => Ok(false),
                },
                BlobKind::Lru => match lru_blobs.get(&(ptr.table_id, ptr.seg_id)) {
                    Some(b) => b.reader.validate_blob_ptr_for_key(ptr, Some(key)),
                    None => Ok(false),
                },
            },
        )?;

        // Salvage reads blobs that the reconciliation below may prune, so it comes first. It only
        // reads, so under `Fail` nothing is written yet either. It runs only when damage was
        // found, which is also the only case where a plain open pays for the blob reads.
        let mut salvaged = Vec::new();
        let discarded = core::mem::take(&mut rec.discarded_txs);
        if let Some(damage) = rec.report.damage.as_mut() {
            let found = salvage::salvage(
                &idx_segments,
                &rec.merged_batches,
                discarded,
                &std_blobs,
                &lru_blobs,
                with_salvage,
            )?;
            damage.unsalvageable = found.unsalvageable;
            salvaged = found.txs;
        }

        // Under `Fail` nothing may have been written yet: `ensure_durable()` and creating/extending
        // WAL streams below must come after this check.
        if let (WalDamagePolicy::Fail, Some(damage)) =
            (config.wal_damage_policy, &rec.report.damage)
        {
            return Err(io::Error::new(io::ErrorKind::InvalidData, damage.clone()));
        }
        meta_mgr.ensure_durable()?;
        // Materialize missing WAL streams and extend short files only after passing the `Fail` gate.
        let mut sync_wal = match sync_wal_opt {
            Some(w) => w,
            None => WalStreamWriter::open_unallocated(
                &*fs,
                WAL_SYNC_FILE_NAME,
                config.sync_wal_size_bytes,
                state_snapshot.sync_wal_epoch,
                WalStreamMode::SyncSectorIsolated,
            )?,
        };
        let mut buf_wal = match buf_wal_opt {
            Some(w) => w,
            None => WalStreamWriter::open_unallocated(
                &*fs,
                WAL_BUF_FILE_NAME,
                config.buf_wal_size_bytes,
                state_snapshot.buf_wal_epoch,
                config.buf_wal_mode,
            )?,
        };
        sync_wal.allocate_to_target()?;
        buf_wal.allocate_to_target()?;
        // Recovery allocates no SeqID, so reserving here still precedes every allocation.
        meta_mgr.reserve_seq_ids_above_existing_files()?;

        let wal_report = rec.report;
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
                            if let (Some(&cat_len), Some(b)) = (
                                cataloged_std_len.get(&ptr.seg_id),
                                std_blobs.get(&ptr.seg_id),
                            ) {
                                record_post_catalog_span(
                                    &mut std_post_catalog_spans,
                                    ptr.seg_id,
                                    ptr.offset as u64,
                                    end,
                                    cat_len,
                                    b.file_len,
                                );
                            }
                        }
                        BlobKind::Lru => {
                            let key = (ptr.table_id, ptr.seg_id);
                            if let (Some(&cat_len), Some(b)) =
                                (cataloged_lru_len.get(&key), lru_blobs.get(&key))
                            {
                                record_post_catalog_span(
                                    &mut lru_post_catalog_spans,
                                    key,
                                    ptr.offset as u64,
                                    end,
                                    cat_len,
                                    b.file_len,
                                );
                            }
                        }
                    }
                }
                let key_opt = match &record {
                    WalRecord::PutInline { key, .. }
                    | WalRecord::PutBlobPtr { key, .. }
                    | WalRecord::Delete { key } => Some(key.as_slice()),
                    WalRecord::TxBegin | WalRecord::TxCommit | WalRecord::TxContinue => None,
                };
                if let Some(key) = key_opt {
                    let may_have_blob = (!std_blobs.is_empty() || !lru_blobs.is_empty())
                        && (key.len() < 3 || matches!(key[2], NS_PRIMARY_ROW | NS_ROW_META));
                    let old_ptr = if may_have_blob {
                        if let Some(mem_entry) = memtable.get(key) {
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
                        }
                    } else {
                        None
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

        // Compute exact currently-live bytes for any uncataloged blob segment across `memtable` and
        // unshadowed `idx_segments`. Stale pointers in older `idx` segments (including those
        // already overwritten or deleted before the last checkpoint) do not count as live: if an
        // uncataloged `blob_lru` was reclaimed before a crash while only lingering until WAL
        // rotation, computing exact live bytes avoids re-adopting it as a zombie segment.
        let has_uncataloged_std = std_blobs
            .keys()
            .any(|seq| !cataloged_std_seqs.contains(seq));
        let has_uncataloged_lru = lru_blobs.keys().any(|k| !cataloged_lru_keys.contains(k));
        let mut uncataloged_std_live: BTreeMap<u64, u64> = BTreeMap::new();
        let mut uncataloged_lru_live: BTreeMap<(u16, u64), u64> = BTreeMap::new();
        if has_uncataloged_std || has_uncataloged_lru {
            for (_k, entry) in
                memtable.iter_range(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded)
            {
                if let IndexValue::Blob(ptr) = entry.value {
                    match ptr.kind {
                        BlobKind::Standard
                            if !cataloged_std_seqs.contains(&ptr.seg_id)
                                && std_blobs.contains_key(&ptr.seg_id) =>
                        {
                            let e = uncataloged_std_live.entry(ptr.seg_id).or_default();
                            *e = e.saturating_add(ptr.len as u64);
                        }
                        BlobKind::Lru
                            if !cataloged_lru_keys.contains(&(ptr.table_id, ptr.seg_id))
                                && lru_blobs.contains_key(&(ptr.table_id, ptr.seg_id)) =>
                        {
                            let e = uncataloged_lru_live
                                .entry((ptr.table_id, ptr.seg_id))
                                .or_default();
                            *e = e.saturating_add(ptr.len as u64);
                        }
                        _ => {}
                    }
                }
            }
            for (seg_idx, seg) in idx_segments.iter().enumerate() {
                let mut cur = seg.cursor_all()?;
                while let Some(entry) = cur.next_entry()? {
                    let IndexValue::Blob(ptr) = entry.value else {
                        continue;
                    };
                    let is_target = match ptr.kind {
                        BlobKind::Standard => {
                            !cataloged_std_seqs.contains(&ptr.seg_id)
                                && std_blobs.contains_key(&ptr.seg_id)
                        }
                        BlobKind::Lru => {
                            !cataloged_lru_keys.contains(&(ptr.table_id, ptr.seg_id))
                                && lru_blobs.contains_key(&(ptr.table_id, ptr.seg_id))
                        }
                    };
                    if !is_target {
                        continue;
                    }
                    if memtable.get(&entry.key).is_some() {
                        continue;
                    }
                    let mut shadowed = false;
                    for newer in &idx_segments[seg_idx + 1..] {
                        if newer.get(&entry.key)?.is_some() {
                            shadowed = true;
                            break;
                        }
                    }
                    if shadowed {
                        continue;
                    }
                    match ptr.kind {
                        BlobKind::Standard => {
                            let e = uncataloged_std_live.entry(ptr.seg_id).or_default();
                            *e = e.saturating_add(ptr.len as u64);
                        }
                        BlobKind::Lru => {
                            let e = uncataloged_lru_live
                                .entry((ptr.table_id, ptr.seg_id))
                                .or_default();
                            *e = e.saturating_add(ptr.len as u64);
                        }
                    }
                }
            }
        }

        // Reconcile unreferenced uncommitted tail bytes and prune any uncataloged
        // blob segment that holds zero currently-live bytes.
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
            if !cataloged_std_seqs.contains(&seq_id) {
                let live = uncataloged_std_live.get(&seq_id).copied().unwrap_or(0);
                if live == 0 {
                    return false;
                }
                had_uncataloged_live_blobs = true;
                let wal_dead_delta = b.dead_bytes;
                let total_dead = b.file_len.saturating_sub(live);
                let persisted = total_dead.saturating_sub(wal_dead_delta);
                b.dead_bytes = total_dead;
                std_persist_dead.insert(seq_id, persisted);
                return true;
            }
            let persisted = account_post_catalog_gap(
                seq_id,
                b.file_len,
                &mut b.dead_bytes,
                &cataloged_std_len,
                &mut std_post_catalog_spans,
                &std_base_dead,
            );
            std_persist_dead.insert(seq_id, persisted);
            true
        });
        lru_blobs.retain(|&(tid, seq_id), b| {
            if !cataloged_lru_keys.contains(&(tid, seq_id)) {
                let live = uncataloged_lru_live
                    .get(&(tid, seq_id))
                    .copied()
                    .unwrap_or(0);
                if live == 0 {
                    return false;
                }
                had_uncataloged_live_blobs = true;
                let wal_dead_delta = b.dead_bytes;
                let total_dead = b.file_len.saturating_sub(live);
                let persisted = total_dead.saturating_sub(wal_dead_delta);
                b.dead_bytes = total_dead;
                lru_persist_dead.insert((tid, seq_id), persisted);
                return true;
            }
            let persisted = account_post_catalog_gap(
                (tid, seq_id),
                b.file_len,
                &mut b.dead_bytes,
                &cataloged_lru_len,
                &mut lru_post_catalog_spans,
                &lru_base_dead,
            );
            lru_persist_dead.insert((tid, seq_id), persisted);
            true
        });

        // Cataloged lengths are the durable prefix. Adopted segments start at zero.
        // Finish writeback before publishing a longer length or checkpointing the WAL.
        for (seq_id, b) in &std_blobs {
            let start = cataloged_std_len.get(seq_id).copied().unwrap_or(0);
            persist_recovered_blob_tail(&**b.reader.file(), start, b.file_len)?;
        }
        for (key, b) in &lru_blobs {
            let start = cataloged_lru_len.get(key).copied().unwrap_or(0);
            persist_recovered_blob_tail(&**b.reader.file(), start, b.file_len)?;
        }

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
            engine_id: snapshot::alloc_engine_id(),
            snapshot_cache: RwLock::new(snapshot::SnapshotCache::new()),
            fs,
            config,
            meta_mgr,
            sync_wal,
            buf_wal,
            next_lsn: rec
                .max_recovered_lsn
                .checked_add(1)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "WAL LSN overflow"))?,
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
            unlink_after_wal_rotation: Vec::new(),
            deferred_flush_on_commit: false,
            deferred_compact_on_commit: false,
            auto_compaction_due: false,
            relocating_std_blobs: false,
            compaction_hook: None,
            compaction_hook_id: 0,
            tx_prepared: false,
            tx_preparing: false,
            pending_tx_buf: Vec::new(),
            pending_tx_record_count: 0,
            pending_tx_bytes: 0,
            pending_tx_sync: false,
            tx_overlay: tx::TxOverlay::default(),
            tx_start_meta: TxMetaSnapshot::default(),
            savepoints: Vec::new(),
            statement_savepoint_active: false,
            statement_blob_undo: Vec::new(),
            dropped_tables_in_tx: Vec::new(),
            vtabs_created_in_tx: Vec::new(),
            tx_pending_sync_vtabs: Vec::new(),
            quotas_modified_in_tx: false,
            active_vtab_ids: HashMap::new(),
            next_vtab_token: 1,
            active_vtab_holders: HashMap::new(),
            mutation_epoch: 0,
            table_mutation_epochs: HashMap::new(),
            table_has_synthetic_rowids: BTreeMap::new(),
            data_generation: 0,
            table_has_row_meta: BTreeMap::new(),
            last_buf_append_lsn: 0,
            last_buf_synced_lsn: 0,
            // `max_recovered_lsn` is the highest LSN seen anywhere, discarded batches included.
            // Everything replayed is checkpointed before the epochs rotate (see below), at
            // `next_lsn - 1`, which equals it, so the first new batch links to a predecessor
            // at or below the checkpoint, as recovery requires.
            wal_links: wal_links::WalLinks::new(rec.max_recovered_lsn),
            last_wal_recovery: wal_report,
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
            #[cfg(test)]
            debug_fail_next_wal_append: false,
            #[cfg(test)]
            debug_fail_next_wal_sync: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };
        engine.access_tracker = AccessTracker::new(1, engine.config.max_in_memory_access_ticks);

        // Reconcile capped byte totals in read-only mode before rotating WAL epochs so a non-empty
        // `flush_memtable()` commits the updated `table_quotas` atomically with the checkpoint.
        let needs_capped_repair = if replayed_wal && !engine.capped_tables.is_empty() {
            engine.reconcile_capped_tables_readonly()?
        } else {
            false
        };

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
            if replayed_wal && !engine.capped_tables.is_empty() {
                let mut quota_metas = engine.build_quota_metas();
                quota_metas.sort_by_key(|q| q.table_id);
                let mut stored = engine.meta_mgr.state().table_quotas.clone();
                stored.sort_by_key(|q| q.table_id);
                if stored != quota_metas {
                    engine.meta_mgr.commit_update(|st| {
                        st.table_quotas = quota_metas;
                    })?;
                }
            }
        }

        // Reconciliation records land in the fresh epoch and are replayed by the next recovery.
        if replayed_wal {
            if needs_capped_repair {
                engine.reconcile_capped_tables_after_recovery(true)?;
            }
        } else {
            engine.reconcile_capped_tables_after_recovery(false)?;
        }

        // Now that WAL recovery and the post-recovery checkpoint are complete, sweep any truly
        // orphaned segment/manifest files and process pending deletes!
        engine.meta_mgr.sweep_orphans()?;

        Ok((engine, salvaged))
    }

    /// Returns the next LSN that will be assigned to a WAL batch.
    /// Returns true if a WAL or metadata write/sync failure poisoned this engine. Reads keep working, but
    /// all mutations fail until the engine is reopened.
    pub fn is_poisoned(&self) -> bool {
        self.poisoned.is_some() || self.meta_mgr.is_poisoned()
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

    /// Fails if a prior WAL or metadata I/O failure left the on-disk state uncertain.
    pub(super) fn check_not_poisoned(&self) -> io::Result<()> {
        self.meta_mgr.check_healthy()?;
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
        let plan = self.plan_vtab_registration(table_name, explicit_id)?;
        self.register_vtab_plan(plan, durability, None)
            .map(|(id, _)| id)
    }

    fn alloc_vtab_token(&mut self) -> u64 {
        let tok = self.next_vtab_token;
        self.next_vtab_token = self.next_vtab_token.wrapping_add(1).max(1);
        tok
    }

    fn add_vtab_holder(&mut self, table_id: u16) -> u64 {
        let tok = self.alloc_vtab_token();
        self.active_vtab_holders
            .entry(table_id)
            .or_default()
            .push(tok);
        tok
    }

    /// Returns `true` if `table_id` currently has at least one live primary row visible in
    /// [`ReadView::Owner`].
    fn table_has_any_primary_row(&self, table_id: u16) -> io::Result<bool> {
        let prefix = encode_namespace_prefix(table_id, NS_PRIMARY_ROW);
        Ok(!self
            .scan_merged_prefix_range(&prefix, &prefix, true, None, 1, false)?
            .is_empty())
    }

    /// Registers a virtual table resolved by [`Self::plan_vtab_registration`] as live and, if the
    /// catalog does not hold it yet, persists its catalog entry. Returns `(table_id, reg_token)`.
    ///
    /// When creating a new entry (`!plan.persisted`) and `initial_unique_marker` is `Some`,
    /// writes the `UNIQUE` completion marker in the same atomic batch provided `chosen_id`
    /// currently has no primary rows (an attached non-empty `explicit_table_id` still requires
    /// background/incremental migration).
    pub(crate) fn register_vtab_plan(
        &mut self,
        plan: VtabPlan,
        durability: WriteDurability,
        initial_unique_marker: Option<Vec<u8>>,
    ) -> io::Result<(u16, u64)> {
        let VtabPlan {
            norm_name,
            table_id: chosen_id,
            persisted,
        } = plan;
        if !persisted {
            let catalog_prefix = [0u8, 0u8, b'c'];
            let mut cat_key = Vec::with_capacity(3 + norm_name.len());
            cat_key.extend_from_slice(&catalog_prefix);
            cat_key.extend_from_slice(norm_name.as_bytes());
            if !IndexEntry::inline_fits_in_block(cat_key.len(), 2) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "virtual table name exceeds maximum catalog key length",
                ));
            }
            // The rowid mapping marker (and initial UNIQUE completion marker for an empty new
            // table) goes in the same batch: a table is either absent, or in the catalog with
            // its markers, never half created.
            let mut records = vec![
                WalRecord::PutInline {
                    key: cat_key,
                    value: chosen_id.to_be_bytes().to_vec(),
                },
                rowid::rowid_marker_record(chosen_id),
            ];
            if let Some(marker_val) = initial_unique_marker {
                if !self.table_has_any_primary_row(chosen_id)? {
                    records.push(unique_index::unique_marker_record(chosen_id, marker_val));
                }
            }
            self.commit_wal_and_memtable(records, durability)?;
        }
        self.active_vtab_ids
            .insert(chosen_id, Arc::from(norm_name.as_str()));
        let token = self.add_vtab_holder(chosen_id);
        Ok((chosen_id, token))
    }

    /// Decides which `table_id` a virtual table `(table_name, explicit_id)` resolves to, and
    /// whether the catalog visible in [`ReadView::Owner`] holds it, without changing anything
    /// except draining any pending group commit first.
    pub(crate) fn plan_vtab_registration(
        &mut self,
        table_name: &str,
        explicit_id: Option<u16>,
    ) -> io::Result<VtabPlan> {
        self.plan_vtab_registration_view(ReadView::Owner, table_name, explicit_id)
    }

    /// View-scoped variant of [`Self::plan_vtab_registration`]. A connection that does not own
    /// the active transaction uses [`ReadView::Committed`] so uncommitted `CREATE`/`RENAME`
    /// mutations are hidden and live registrations come from `tx_start_meta`.
    pub(crate) fn plan_vtab_registration_view(
        &mut self,
        view: ReadView,
        table_name: &str,
        explicit_id: Option<u16>,
    ) -> io::Result<VtabPlan> {
        self.check_not_poisoned()?;
        self.drain_group_for_autocommit()?;
        let norm_name = normalize_vtab_ident(table_name);
        if norm_name.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "virtual table name must not be empty",
            ));
        }
        let catalog_prefix = [0u8, 0u8, b'c'];
        let entries = self.scan_merged_prefix_view(view, &catalog_prefix)?;
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

        let active_vtab_ids = if view == ReadView::Committed && self.tx_start_meta.active {
            &self.tx_start_meta.active_vtab_ids
        } else {
            &self.active_vtab_ids
        };

        let chosen_id = match explicit_id {
            Some(id) => {
                if id == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "table_id 0 is reserved for engine system catalog",
                    ));
                }
                if let Some(existing_name) = active_vtab_ids.get(&id) {
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
                            format!(
                                "table_id {id} is already persisted for virtual table '{owner}'"
                            ),
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
                    if let Some(existing_name) = active_vtab_ids.get(&id) {
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
                    find_free_table_id(|id| {
                        active_vtab_ids.contains_key(&id) || persisted_id_to_name.contains_key(&id)
                    })?
                }
            }
        };

        Ok(VtabPlan {
            persisted: persisted_name_to_id.get(&norm_name).copied() == Some(chosen_id),
            norm_name,
            table_id: chosen_id,
        })
    }

    /// Registers an already persisted virtual table as live for a connection that does not own
    /// the active transaction. Unlike a registration made by the transaction owner, it must
    /// survive that transaction's rollback, so it is also recorded in the snapshots a rollback
    /// restores without clobbering any uncommitted rename/drop already recorded by the owner.
    pub(crate) fn activate_persisted_vtab(&mut self, table_id: u16, table_name: &str) -> u64 {
        let name: Arc<str> = Arc::from(normalize_vtab_ident(table_name).as_str());
        let tok = self.alloc_vtab_token();
        if self.tx_start_meta.active {
            let orig = self.tx_start_meta.active_vtab_ids.get(&table_id).cloned();
            let dropped_in_tx = self
                .dropped_tables_in_tx
                .iter()
                .any(|&(tid, _)| tid == table_id);
            if !dropped_in_tx && self.active_vtab_ids.get(&table_id) == orig.as_ref() {
                self.active_vtab_ids.insert(table_id, name.clone());
            }
            if !dropped_in_tx {
                self.active_vtab_holders
                    .entry(table_id)
                    .or_default()
                    .push(tok);
            }
            for marker in &mut self.savepoints {
                if let tx::SavepointMeta::Full(snap) = &mut marker.meta {
                    let prefix_len = snap.dropped_tables_len.min(self.dropped_tables_in_tx.len());
                    let dropped_before_sp = self.dropped_tables_in_tx[..prefix_len]
                        .iter()
                        .any(|&(tid, _)| tid == table_id);
                    if !dropped_before_sp {
                        if snap.active_vtab_ids.get(&table_id) == orig.as_ref() {
                            snap.active_vtab_ids.insert(table_id, name.clone());
                        }
                        snap.active_vtab_holders
                            .entry(table_id)
                            .or_default()
                            .push(tok);
                    }
                }
            }
            self.tx_start_meta.active_vtab_ids.insert(table_id, name);
            self.tx_start_meta
                .active_vtab_holders
                .entry(table_id)
                .or_default()
                .push(tok);
        } else {
            self.active_vtab_ids.insert(table_id, name);
            self.active_vtab_holders
                .entry(table_id)
                .or_default()
                .push(tok);
        }
        tok
    }

    /// Notes that the active transaction created virtual table `(table_id, table_name)` in `db_name`.
    pub(crate) fn note_vtab_created_in_tx(
        &mut self,
        table_id: u16,
        table_name: &str,
        db_name: &str,
    ) {
        if self.tx_depth > 0 {
            self.vtabs_created_in_tx.push((
                table_id,
                normalize_vtab_ident(table_name),
                normalize_vtab_ident(db_name),
            ));
        }
    }

    /// Drops a table that the active transaction created and that SQLite no longer has, but only
    /// if the catalog still maps `table_name` to `table_id`. A savepoint rollback the engine was
    /// told about has already removed the table, and its id (or name) may belong to another
    /// table by now.
    pub(crate) fn drop_stale_created_vtab(
        &mut self,
        table_id: u16,
        table_name: &str,
    ) -> io::Result<()> {
        let norm = normalize_vtab_ident(table_name);
        let mut key = vec![0u8, 0u8, b'c'];
        key.extend_from_slice(norm.as_bytes());
        let mapped = match self.lookup_raw_entry_view(ReadView::Owner, &key)? {
            Some((_, IndexValue::Inline(val))) if val.len() == 2 => {
                Some(u16::from_be_bytes([val[0], val[1]]))
            }
            _ => None,
        };
        if mapped == Some(table_id) {
            self.drop_table_with_durability(table_id, Some(&norm), WriteDurability::BufferedFast)?;
        }
        Ok(())
    }

    /// Removes the tables that the active transaction created under `table_name` or with
    /// `table_id` for which `is_stale(prev_db, prev_name)` holds, and returns them. SQLite only
    /// runs `xCreate` for a name its schema does not have, so one of these is a creation that a
    /// `ROLLBACK TO` undid in the schema but, because SQLite never announces such a savepoint to a
    /// table made inside it, not in the engine.
    pub(crate) fn take_vtabs_created_in_tx(
        &mut self,
        table_id: Option<u16>,
        table_name: &str,
        mut is_stale: impl FnMut(&str, &str) -> bool,
    ) -> Vec<(u16, String)> {
        let norm = normalize_vtab_ident(table_name);
        let (stale, kept): (Vec<_>, Vec<_>) = core::mem::take(&mut self.vtabs_created_in_tx)
            .into_iter()
            .partition(|(id, name, db)| {
                (*name == norm || Some(*id) == table_id) && is_stale(db, name)
            });
        self.vtabs_created_in_tx = kept;
        stale.into_iter().map(|(id, name, _)| (id, name)).collect()
    }

    /// Removes any tables created in the active transaction for which `is_stale(db, name)` holds,
    /// returning them so `vtab_sync_tx` can drop creations undone by SQLite savepoint rollbacks
    /// (including tables that were subsequently renamed).
    pub(crate) fn take_stale_vtabs_created_in_tx(
        &mut self,
        mut is_stale: impl FnMut(&str, &str) -> bool,
    ) -> Vec<(u16, String)> {
        let (stale, kept): (Vec<_>, Vec<_>) = core::mem::take(&mut self.vtabs_created_in_tx)
            .into_iter()
            .partition(|(_, name, db)| is_stale(db, name));
        self.vtabs_created_in_tx = kept;
        stale.into_iter().map(|(id, name, _)| (id, name)).collect()
    }

    /// Returns the live normalized table name registered for `table_id`, if any.
    pub(crate) fn active_vtab_name(&self, table_id: u16) -> Option<String> {
        self.active_vtab_ids.get(&table_id).map(|s| s.to_string())
    }

    /// Per-table mutation epoch for `table_id` (0 if the table has not mutated since open).
    pub(crate) fn table_mutation_epoch(&self, table_id: u16) -> u64 {
        self.table_mutation_epochs
            .get(&table_id)
            .copied()
            .unwrap_or(0)
    }

    #[cfg(test)]
    pub(crate) fn test_table_has_synthetic_rowids(&self, table_id: u16) -> Option<bool> {
        self.table_has_synthetic_rowids.get(&table_id).copied()
    }

    pub(super) fn bump_table_mutation_epoch(&mut self, table_id: u16) {
        let e = self.table_mutation_epochs.entry(table_id).or_insert(0);
        *e = e.wrapping_add(1);
    }

    /// Updates per-table mutation epochs and synthetic-rowid tracking for committed or overlayed WAL
    /// records whose keys belong to user-row or rowid namespaces.
    pub(super) fn note_records_table_mutations(&mut self, records: &[WalRecord]) {
        let mut last_tid: Option<u16> = None;
        for rec in records {
            let (key, is_put) = match rec {
                WalRecord::PutInline { key, .. } => (key.as_slice(), true),
                WalRecord::PutBlobPtr { key, .. } | WalRecord::Delete { key } => {
                    (key.as_slice(), false)
                }
                WalRecord::TxBegin | WalRecord::TxCommit | WalRecord::TxContinue => continue,
            };
            if key.len() < 3 {
                continue;
            }
            let tid = u16::from_be_bytes([key[0], key[1]]);
            if tid == 0 {
                continue;
            }
            let ns = key[2];
            if is_put
                && (ns == rowid::NS_ROWID_BY_PK || (ns == rowid::NS_PK_BY_ROWID && key.len() > 3))
            {
                self.table_has_synthetic_rowids.insert(tid, true);
            }
            if matches!(
                ns,
                NS_PRIMARY_ROW | rowid::NS_ROWID_BY_PK | rowid::NS_PK_BY_ROWID
            ) && last_tid != Some(tid)
            {
                last_tid = Some(tid);
                self.bump_table_mutation_epoch(tid);
            }
        }
    }

    /// Returns `true` if `(table_id, token)` still refers to a live virtual table incarnation in
    /// the view visible to `conn_id`.
    pub(crate) fn is_vtab_token_active(&self, table_id: u16, token: u64, conn_id: usize) -> bool {
        let holders = if self.read_view_for_conn(conn_id) == ReadView::Committed
            && self.tx_start_meta.active
        {
            &self.tx_start_meta.active_vtab_holders
        } else {
            &self.active_vtab_holders
        };
        holders
            .get(&table_id)
            .is_some_and(|list| list.contains(&token))
    }

    /// Unregisters a live virtual table handle identified by `(table_id, token)`, removing the
    /// catalog entry from `active_vtab_ids` only when the last live holder disconnects.
    pub(crate) fn unregister_vtab_token(&mut self, table_id: u16, token: u64) {
        let remove_from = |holders_map: &mut HashMap<u16, Vec<u64>>,
                           ids_map: &mut HashMap<u16, Arc<str>>| {
            let Some(holders) = holders_map.get_mut(&table_id) else {
                return;
            };
            let Some(pos) = holders.iter().position(|&t| t == token) else {
                return;
            };
            holders.swap_remove(pos);
            if holders.is_empty() {
                holders_map.remove(&table_id);
                ids_map.remove(&table_id);
            }
        };
        remove_from(&mut self.active_vtab_holders, &mut self.active_vtab_ids);
        if self.tx_start_meta.active {
            remove_from(
                &mut self.tx_start_meta.active_vtab_holders,
                &mut self.tx_start_meta.active_vtab_ids,
            );
            for marker in &mut self.savepoints {
                if let tx::SavepointMeta::Full(snap) = &mut marker.meta {
                    remove_from(&mut snap.active_vtab_holders, &mut snap.active_vtab_ids);
                }
            }
        }
    }

    /// Renames a live virtual table in the catalog (`xRename` / `ALTER TABLE ... RENAME TO`): the
    /// entry of `old_name` moves to `new_name` and keeps its `table_id`. Both catalog writes and
    /// the live registration follow the enclosing transaction, so a rollback restores the old
    /// name. Fails if another table already holds `new_name`.
    pub(crate) fn rename_vtab(
        &mut self,
        table_id: u16,
        old_name: &str,
        new_name: &str,
        durability: WriteDurability,
    ) -> io::Result<()> {
        self.check_not_poisoned()?;
        self.drain_group_for_autocommit()?;
        let old_norm = normalize_vtab_ident(old_name);
        let new_norm = normalize_vtab_ident(new_name);
        if new_norm.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "virtual table name must not be empty",
            ));
        }
        let catalog_key = |name: &str| {
            let mut key = Vec::with_capacity(3 + name.len());
            key.extend_from_slice(&[0u8, 0u8, b'c']);
            key.extend_from_slice(name.as_bytes());
            key
        };
        let new_key = catalog_key(&new_norm);
        if !IndexEntry::inline_fits_in_block(new_key.len(), 2) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "virtual table name exceeds maximum catalog key length",
            ));
        }
        let mut catalog_old_name: Option<String> = None;
        for entry in self.scan_merged_prefix(&[0u8, 0u8, b'c'])? {
            let IndexValue::Inline(val) = entry.value else {
                continue;
            };
            if val.len() == 2 && entry.key.len() > 3 {
                let id = u16::from_be_bytes([val[0], val[1]]);
                let entry_name = String::from_utf8_lossy(&entry.key[3..]).into_owned();
                if entry_name == new_norm && id != table_id {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("a virtual table named '{new_norm}' already exists"),
                    ));
                }
                if id == table_id && (catalog_old_name.is_none() || entry_name == old_norm) {
                    catalog_old_name = Some(entry_name);
                }
            }
        }
        if let Some(live) = self.active_vtab_ids.iter().find_map(|(id, name)| {
            (name.as_ref() == new_norm.as_str() && *id != table_id).then_some(*id)
        }) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("a virtual table named '{new_norm}' is live as table_id {live}"),
            ));
        }
        let effective_old = catalog_old_name.as_deref().unwrap_or(&old_norm);
        if effective_old == new_norm {
            self.active_vtab_ids
                .insert(table_id, Arc::from(new_norm.as_str()));
            for (id, name, _) in &mut self.vtabs_created_in_tx {
                if *id == table_id {
                    *name = new_norm.clone();
                }
            }
            return Ok(());
        }
        let mut records = Vec::with_capacity(2);
        let old_key = catalog_key(effective_old);
        if catalog_old_name.is_some()
            || !matches!(
                self.check_existing_row(&old_key)?,
                ExistingRowStatus::NotFound
            )
        {
            records.push(WalRecord::Delete { key: old_key });
        }
        records.push(WalRecord::PutInline {
            key: new_key,
            value: table_id.to_be_bytes().to_vec(),
        });
        self.commit_wal_and_memtable(records, durability)?;
        self.active_vtab_ids
            .insert(table_id, Arc::from(new_norm.as_str()));
        for (id, name, _) in &mut self.vtabs_created_in_tx {
            if *id == table_id {
                *name = new_norm.clone();
            }
        }
        Ok(())
    }

    /// Drops a table (`xDestroy` / `DROP TABLE`), deleting all primary rows (`'r'`), secondary
    /// index entries (`'i'`), row metadata (`'t'`), LRU queue entries (`'l'`), capped table
    /// quota registrations, and system catalog entries synchronously
    /// ([`WriteDurability::SyncTransactional`]).
    pub fn drop_table(&mut self, table_id: u16, table_name: Option<&str>) -> io::Result<()> {
        self.drop_table_with_durability(table_id, table_name, WriteDurability::SyncTransactional)
    }

    /// Drops a table using the specified [`WriteDurability`] for its WAL tombstones and catalog
    /// deletion records.
    pub fn drop_table_with_durability(
        &mut self,
        table_id: u16,
        table_name: Option<&str>,
        durability: WriteDurability,
    ) -> io::Result<()> {
        self.check_not_poisoned()?;
        self.drain_group_for_autocommit()?;
        let mut records = Vec::new();
        let mut dead_ptrs = Vec::new();

        for ns in [
            NS_PRIMARY_ROW,
            NS_SECONDARY_INDEX,
            NS_ROW_META,
            NS_LRU_QUEUE,
            rowid::NS_ROWID_BY_PK,
            rowid::NS_PK_BY_ROWID,
        ] {
            let prefix = encode_namespace_prefix(table_id, ns);
            let entries = self.scan_merged_prefix(&prefix)?;
            for entry in entries {
                if let IndexValue::Blob(ptr) = entry.value {
                    dead_ptrs.push(ptr);
                }
                records.push(WalRecord::Delete { key: entry.key });
            }
        }

        let mut catalog_keys_for_id = std::collections::HashSet::new();
        for entry in self.scan_merged_prefix(&[0u8, 0u8, b'c'])? {
            if let IndexValue::Inline(val) = entry.value {
                if val.len() == 2 && u16::from_be_bytes([val[0], val[1]]) == table_id {
                    catalog_keys_for_id.insert(entry.key.clone());
                    records.push(WalRecord::Delete { key: entry.key });
                }
            }
        }

        let candidate_name = table_name
            .map(normalize_vtab_ident)
            .or_else(|| self.active_vtab_ids.get(&table_id).map(|s| s.to_string()));
        if let Some(norm_name) = candidate_name {
            let mut cat_key = Vec::with_capacity(3 + norm_name.len());
            cat_key.extend_from_slice(&[0u8, 0u8, b'c']);
            cat_key.extend_from_slice(norm_name.as_bytes());
            if !catalog_keys_for_id.contains(&cat_key) {
                if let Some((_, IndexValue::Inline(val))) = self.lookup_raw_entry(&cat_key)? {
                    if val.len() == 2 && u16::from_be_bytes([val[0], val[1]]) == table_id {
                        records.push(WalRecord::Delete { key: cat_key });
                    }
                }
            }
        }

        // Without an enclosing transaction the deletes run in an internal one: its chunked
        // TxBegin/TxCommit framing and WAL growth let a table larger than the WAL be dropped
        // atomically. Dropping a table is exempt from `max_transaction_bytes` both standalone and
        // inside a staged DDL transaction.
        let internal_tx = self.tx_depth == 0;
        if internal_tx {
            self.begin_transaction();
        }
        if !records.is_empty() {
            let was_preparing = core::mem::replace(&mut self.tx_preparing, true);
            let res = self.commit_wal_and_memtable(records, durability);
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
        // Mark pre-drop `blob_lru` bytes dead now (rather than waiting until commit) so if this
        // `table_id` is reused later in the same transaction, only bytes from the dropped
        // incarnation are marked dead and live blobs written by the replacement table stay intact.
        for (&(t, _), b) in &mut self.lru_blobs {
            if t == table_id {
                b.dead_bytes = b.file_len;
            }
        }

        self.vtabs_created_in_tx
            .retain(|(id, _, _)| *id != table_id);
        self.active_vtab_ids.remove(&table_id);
        self.active_vtab_holders.remove(&table_id);
        self.corrupt_policies.remove(&table_id);
        self.table_inline_thresholds.remove(&table_id);
        let was_capped = self.capped_tables.remove(&table_id).is_some();
        self.table_has_row_meta.insert(table_id, false);
        self.bump_table_mutation_epoch(table_id);
        self.blob_cache.invalidate_table_blobs(table_id);
        if let Ok(mut q) = self.pending_corrupt_evictions.lock() {
            q.retain(|(tid, _), _| *tid != table_id);
        }

        // Blob reclaim, LRU writer/segment disposal, and quota commit happen once on commit in
        // `publish_committed_transaction` via `dropped_tables_in_tx` so rolling back an enclosing
        // transaction leaves `active_lru_writers` and `lru_blobs` intact.
        self.dropped_tables_in_tx.push((table_id, was_capped));
        if internal_tx {
            self.commit_transaction()?;
            self.table_has_synthetic_rowids.insert(table_id, false);
        } else {
            self.table_has_synthetic_rowids.remove(&table_id);
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
        self.put_row_owned_with_hint_and_extra(
            table_id,
            pk,
            value,
            secondary_keys,
            Vec::new(),
            durability,
            known_absent,
        )
    }

    /// Atomically inserts or overwrites a row (`pk` -> `value`) together with additional derived
    /// `extra_records` (such as persistent SQLite rowid mappings) in a single WAL/MemTable batch.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn put_row_owned_with_hint_and_extra(
        &mut self,
        table_id: u16,
        pk: &[u8],
        value: Vec<u8>,
        secondary_keys: &[(u16, Vec<u8>)],
        extra_records: Vec<WalRecord>,
        durability: WriteDurability,
        known_absent: bool,
    ) -> io::Result<()> {
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
                            key: encode_secondary_index_key(table_id, *old_idx_id, old_col_val, pk),
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
        let extra_bytes: usize = extra_records
            .iter()
            .map(|r| match r {
                WalRecord::PutInline { key, value } => 9 + key.len() + value.len(),
                WalRecord::PutBlobPtr { key, blob_ptr } => 28 + key.len() + (blob_ptr.len as usize),
                WalRecord::Delete { key } => 5 + key.len(),
                WalRecord::TxBegin | WalRecord::TxCommit | WalRecord::TxContinue => 1,
            })
            .sum();
        if self.tx_depth > 0 && !self.tx_preparing && self.config.max_transaction_bytes > 0 {
            let est_row_bytes =
                ((if store_as_blob { 28 } else { 9 }) + row_key.len() + value.len())
                    .saturating_add(extra_bytes);
            if self.pending_tx_bytes.saturating_add(est_row_bytes)
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
            if durability == WriteDurability::SyncTransactional && self.tx_depth == 0 {
                let sync_res = match blob_kind {
                    BlobKind::Standard => self.sync_active_std_writer(),
                    BlobKind::Lru => self.sync_active_lru_writer(table_id),
                };
                if let Err(e) = sync_res {
                    if let Some(ref p) = new_row_blob_ptr {
                        self.record_dead_blob_bytes(p);
                    }
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
        let mut new_meta_blob_ptr = None;
        if need_meta {
            let meta_bytes =
                match RowMetaRecord::encode_slice(assigned_tick, logical_size, secondary_keys) {
                    Ok(bytes) => bytes,
                    Err(e) => {
                        if let Some(ref p) = new_row_blob_ptr {
                            self.record_dead_blob_bytes(p);
                        }
                        return Err(e);
                    }
                };
            new_meta_blob_ptr =
                match self.append_std_meta_record(meta_key, meta_bytes, durability, &mut records) {
                    Ok(ptr_opt) => ptr_opt,
                    Err(e) => {
                        if let Some(ref p) = new_row_blob_ptr {
                            self.record_dead_blob_bytes(p);
                        }
                        return Err(e);
                    }
                };
        } else if had_old_meta {
            records.push(WalRecord::Delete { key: meta_key });
        }

        records.extend(extra_records);

        // 5. Append atomic batch to WAL and apply to MemTable
        if let Err(e) = self.commit_wal_and_memtable(records, durability) {
            if let Some(ref p) = new_row_blob_ptr {
                self.record_dead_blob_bytes(p);
            }
            if let Some(ref p) = new_meta_blob_ptr {
                self.record_dead_blob_bytes(p);
            }
            return Err(e);
        }
        if need_meta {
            self.table_has_row_meta.insert(table_id, true);
        }

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
        if let Err(e) = self.commit_wal_and_memtable(records, durability) {
            if let Some(ref p) = new_meta_blob_ptr {
                self.record_dead_blob_bytes(p);
            }
            return Err(e);
        }
        self.table_has_row_meta.insert(table_id, true);
        if let Some(old_m_ptr) = old_meta_blob_ptr {
            self.record_dead_blob_bytes(&old_m_ptr);
        }
        Ok(true)
    }

    /// Encodes `meta_key` -> `meta_bytes` into `records`, spilling oversized metadata to
    /// `blob_std` and syncing `active_std_writer` before autocommit `SyncTransactional` WAL
    /// appends (`Write-Blob-Before-WAL`).
    pub(super) fn append_std_meta_record(
        &mut self,
        meta_key: Vec<u8>,
        meta_bytes: Vec<u8>,
        durability: WriteDurability,
        records: &mut Vec<WalRecord>,
    ) -> io::Result<Option<BlobPtr>> {
        if !IndexEntry::inline_fits_in_block(meta_key.len(), meta_bytes.len()) {
            let ptr = self.append_to_blob_segment(BlobKind::Standard, 0, &meta_key, &meta_bytes)?;
            if durability == WriteDurability::SyncTransactional && self.tx_depth == 0 {
                if let Err(e) = self.sync_active_std_writer() {
                    self.record_dead_blob_bytes(&ptr);
                    return Err(e);
                }
            }
            records.push(WalRecord::PutBlobPtr {
                key: meta_key,
                blob_ptr: ptr,
            });
            Ok(Some(ptr))
        } else {
            records.push(WalRecord::PutInline {
                key: meta_key,
                value: meta_bytes,
            });
            Ok(None)
        }
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
            && self.tx_depth == 0
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
        if cx.src.is_owner() && self.tx_depth > 0 && self.any_blob_unflushed() {
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
    /// a capped table outside any transaction, or `conn_id` owns an active transaction that
    /// currently has buffered (unflushed) blob bytes.
    #[inline]
    pub fn vtab_read_needs_exclusive(&self, conn_id: usize, table_id: u16) -> bool {
        if self.read_view_for_conn(conn_id) == ReadView::Committed {
            return false;
        }
        (self.tx_depth > 0 && self.any_blob_unflushed())
            || (self.tx_depth == 0
                && !self.is_poisoned()
                && self.config.lru_read_tracking == LruReadTrackingMode::EagerWriteOnRead
                && self.capped_tables.contains_key(&table_id))
    }

    /// True if a snapshot read on `table_id` should upgrade to an exclusive lock so
    /// `EagerWriteOnRead` can persist LRU promotions outside active transactions.
    #[inline]
    pub(crate) fn snapshot_read_needs_exclusive(&self, table_id: u16) -> bool {
        self.tx_depth == 0
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
                if let Err(e) = self.flush_active_blob_buffers() {
                    return (r, Some(e));
                }
                continue;
            }
            // The promotions are an optimisation of the read, never part of its result: a
            // poisoned engine cannot write them, and a write that fails must not fail the read,
            // though a failure on a non-poisoned engine is recorded via `record_maintenance_failure`.
            // Inside an active transaction (`tx_depth > 0`) eager promotions must never mutate the
            // transaction overlay/budget. A hit that is not written is still noted in RAM, so the
            // LRU order learns it lazily as for any coalesced read.
            let hits = cx.eager_hits.take();
            let mut write_failed = self.is_poisoned() || self.tx_depth > 0;
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
                            key: encode_secondary_index_key(table_id, *old_idx_id, old_col_val, pk),
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
            self.config
                .inline_threshold_bytes
                .min(DEFAULT_INLINE_THRESHOLD)
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

    /// What recovery found in the WAL during the `open` that created this engine. It is defined
    /// even when nothing happened (no damage, nothing replayed). The WAL is damaged only when
    /// acknowledged commits were lost; the unsynced tail of a power cut is not damage.
    pub fn last_wal_recovery(&self) -> &WalRecoveryReport {
        &self.last_wal_recovery
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
                self.queue_corrupt_eviction(table_id, pk, key, IndexValue::Blob(*ptr));
                Ok(())
            }
        }
    }

    fn queue_corrupt_eviction(&self, table_id: u16, pk: &[u8], key: &[u8], observed: IndexValue) {
        if let Ok(mut q) = self.pending_corrupt_evictions.lock() {
            q.insert(
                (table_id, pk.to_vec()),
                PendingCorruptEviction {
                    key: key.to_vec(),
                    observed,
                },
            );
        }
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
        if self.tx_depth == 0 {
            self.drain_pending_durable()?;
        }
        Ok(())
    }

    /// Deletes rows queued by [`CorruptPolicy::Evict`] reads.
    ///
    /// Funnel points: the start of `put_row_owned_with_hint`, `delete_row` and `flush_memtable`,
    /// plus the end of the outermost `commit_transaction` (SQL statements write inside a
    /// transaction, where draining is deferred). Never called from read paths, so a
    /// connection that merely reads never writes. Does nothing while a transaction is active
    /// or the engine is poisoned (entries stay queued). Each entry is re-checked: the row is
    /// only deleted if its record still has the observed bytes or blob pointer (it may have been
    /// overwritten or deleted since). Deletes use `BufferedFast` durability.
    pub(super) fn drain_corrupt_evictions(&mut self) -> io::Result<()> {
        if self.tx_depth > 0 || self.is_poisoned() || !self.has_pending_corrupt_evictions() {
            return Ok(());
        }
        self.drain_pending_durable()?;
        let pending: Vec<((u16, Vec<u8>), PendingCorruptEviction)> =
            match self.pending_corrupt_evictions.lock() {
                Ok(mut q) if !q.is_empty() => q.drain().collect(),
                _ => return Ok(()),
            };
        let mut iter = pending.into_iter();
        while let Some(((table_id, pk), ev)) = iter.next() {
            let step = match self.lookup_raw_entry(&ev.key) {
                Ok(Some((_, current))) if current == ev.observed => self
                    .delete_row(table_id, &pk, WriteDurability::BufferedFast)
                    .map(|_| ()),
                Ok(_) => Ok(()),
                Err(e) => Err(e),
            };
            if let Err(e) = step {
                if let Ok(mut q) = self.pending_corrupt_evictions.lock() {
                    q.entry((table_id, pk)).or_insert(ev);
                    for (k, v) in iter {
                        q.entry(k).or_insert(v);
                    }
                }
                return Err(e);
            }
        }
        Ok(())
    }

    /// Captures `ptr` together with a pinned handle to its segment file (see [`LazyBlob`]).
    fn pin_lazy_blob_src(
        &self,
        src: ReadSource<'_, FS::File>,
        ptr: &BlobPtr,
    ) -> LazyBlob<FS::File> {
        let reader = self.blob_reader_for(src, ptr).cloned();
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
        cx: &ReadCtx<'_, FS::File>,
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

        let cache_key = BlobCacheKey::from_ptr(ptr, row_key);
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
        _cx: &ReadCtx<'_, FS::File>,
        table_id: u16,
        candidates: &[(&[u8], &LazyBlob<FS::File>)],
    ) -> io::Result<()> {
        if self.config.blob_cache_bytes == 0 || candidates.is_empty() {
            return Ok(());
        }

        struct PrefetchItem<'a, F: PlatformFile> {
            cache_key: BlobCacheKey,
            key_range: core::ops::Range<usize>,
            ptr: BlobPtr,
            reader: &'a BlobSegmentReader<F>,
        }

        let mut key_slab: Vec<u8> = Vec::new();
        let mut to_fetch: Vec<PrefetchItem<'_, FS::File>> = Vec::with_capacity(candidates.len());
        let mut stack_key = [0u8; 64];
        let mut heap_key = Vec::new();
        for (pk, lazy) in candidates {
            let ptr = &lazy.ptr;
            if !self.is_blob_eligible_for_cache(ptr)
                || (ptr.len as usize) < BLOB_RECORD_HEADER_LEN
                || (ptr.offset as u64).checked_add(ptr.len as u64).is_none()
            {
                continue;
            }
            if let Some(ref reader) = lazy.reader {
                let row_key: &[u8] = if 3 + pk.len() <= stack_key.len() {
                    let tb = table_id.to_be_bytes();
                    stack_key[0] = tb[0];
                    stack_key[1] = tb[1];
                    stack_key[2] = NS_PRIMARY_ROW;
                    stack_key[3..3 + pk.len()].copy_from_slice(pk);
                    &stack_key[..3 + pk.len()]
                } else {
                    heap_key.clear();
                    heap_key.extend_from_slice(&encode_namespace_prefix(table_id, NS_PRIMARY_ROW));
                    heap_key.extend_from_slice(pk);
                    heap_key.as_slice()
                };
                let cache_key = BlobCacheKey::from_ptr(ptr, row_key);
                if self.blob_cache.contains(&cache_key) {
                    continue;
                }
                let start = key_slab.len();
                key_slab.extend_from_slice(row_key);
                to_fetch.push(PrefetchItem {
                    cache_key,
                    key_range: start..key_slab.len(),
                    ptr: *ptr,
                    reader,
                });
            }
        }

        if to_fetch.is_empty() {
            return Ok(());
        }

        to_fetch.sort_unstable_by_key(|c| {
            (c.ptr.kind as u8, c.ptr.table_id, c.ptr.seg_id, c.ptr.offset)
        });

        let verify_and_cache = |cand: &PrefetchItem<'_, FS::File>, record_buf: &[u8]| {
            if let Some(val) = BlobSegmentReader::<FS::File>::verify_blob_record(
                record_buf,
                cand.ptr.crc32c,
                Some(&key_slab[cand.key_range.clone()]),
            ) {
                self.blob_cache.insert(cand.cache_key, val);
            }
        };

        let mut scratch: Vec<u8> = Vec::new();
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

            let mut i = 0;
            while i < group.len() {
                let cluster_start = group[i].ptr.offset as u64;
                let mut cluster_end = cluster_start + group[i].ptr.len as u64;
                let mut j = i + 1;
                while j < group.len() {
                    let next_start = group[j].ptr.offset as u64;
                    let next_end = next_start + group[j].ptr.len as u64;
                    let gap = next_start.saturating_sub(cluster_end);
                    let new_span = next_end.max(cluster_end) - cluster_start;
                    if gap <= 16 * 1024 && new_span <= 64 * 1024 {
                        cluster_end = cluster_end.max(next_end);
                        j += 1;
                    } else {
                        break;
                    }
                }

                if j == i + 1 {
                    let cand = &group[i];
                    let len = cand.ptr.len as usize;
                    scratch.resize(len, 0);
                    if reader
                        .file()
                        .read_exact_at(&mut scratch[..len], cluster_start)
                        .is_ok()
                    {
                        verify_and_cache(cand, &scratch[..len]);
                    }
                } else {
                    let span = (cluster_end - cluster_start) as usize;
                    scratch.resize(span, 0);
                    if reader
                        .file()
                        .read_exact_at(&mut scratch[..span], cluster_start)
                        .is_ok()
                    {
                        for cand in &group[i..j] {
                            let rel = (cand.ptr.offset as u64 - cluster_start) as usize;
                            let len = cand.ptr.len as usize;
                            verify_and_cache(cand, &scratch[rel..rel + len]);
                        }
                    } else {
                        // Fault isolation: fall back to per-item reads so a single corrupt or
                        // out-of-bounds pointer never discards valid sibling blobs.
                        for cand in &group[i..j] {
                            let len = cand.ptr.len as usize;
                            scratch.resize(len, 0);
                            if reader
                                .file()
                                .read_exact_at(&mut scratch[..len], cand.ptr.offset as u64)
                                .is_ok()
                            {
                                verify_and_cache(cand, &scratch[..len]);
                            }
                        }
                    }
                }

                i = j;
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

    /// Syncs every active blob writer. A failure poisons the engine, see
    /// [`Self::sync_active_std_writer`].
    pub(super) fn sync_active_blob_writers(&mut self) -> io::Result<()> {
        self.sync_active_std_writer()?;
        let table_ids: Vec<u16> = self.active_lru_writers.keys().copied().collect();
        for table_id in table_ids {
            self.sync_active_lru_writer(table_id)?;
        }
        Ok(())
    }

    /// Syncs the active standard blob writer (if any) and records its length.
    ///
    /// A failed sync poisons the engine: the kernel may have dropped the dirty pages and marked
    /// them clean, so a retried sync can report success for bytes that never reached the disk.
    /// Continuing would let a later checkpoint or WAL record cover a lost blob. Once poisoned,
    /// no further blob sync is attempted.
    pub(super) fn sync_active_std_writer(&mut self) -> io::Result<()> {
        self.check_not_poisoned()?;
        let Some(w) = &mut self.active_std_writer else {
            return Ok(());
        };
        // A failed write never reached the page cache and its buffer is kept, so it is retryable.
        w.flush_buffer()?;
        let synced = w.sync_durable();
        let (seq_id, len) = (w.seq_id(), w.current_len());
        synced.map_err(|e| self.poison(e))?;
        if let Some(b) = self.std_blobs.get_mut(&seq_id) {
            b.file_len = len;
        }
        Ok(())
    }

    /// Per-table LRU counterpart of [`Self::sync_active_std_writer`].
    pub(super) fn sync_active_lru_writer(&mut self, table_id: u16) -> io::Result<()> {
        self.check_not_poisoned()?;
        let Some(w) = self.active_lru_writers.get_mut(&table_id) else {
            return Ok(());
        };
        // A failed write never reached the page cache and its buffer is kept, so it is retryable.
        w.flush_buffer()?;
        let synced = w.sync_durable();
        let (seq_id, len) = (w.seq_id(), w.current_len());
        synced.map_err(|e| self.poison(e))?;
        if let Some(b) = self.lru_blobs.get_mut(&(table_id, seq_id)) {
            b.file_len = len;
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
                        // Poisons on any error: the writer is already out of the engine, so its
                        // buffer cannot be retried.
                        old_w.sync_durable().map_err(|e| self.poison(e))?;
                        if let Some(b) = self.std_blobs.get_mut(&old_w.seq_id()) {
                            b.file_len = old_w.current_len();
                        }
                    }
                    let seq_id = self.meta_mgr.reserve_seq_id();
                    let writer = BlobSegmentWriter::open(&*self.fs, BlobKind::Standard, 0, seq_id)?;
                    let reader = writer.reader();
                    let mut std_metas = self.build_checkpointed_std_blob_metas();
                    std_metas.push(StdBlobSegmentMeta {
                        seq_id,
                        file_len: 0,
                        dead_bytes: 0,
                    });
                    std_metas.sort_by_key(|m| m.seq_id);
                    if let Err(e) = self.meta_mgr.commit_update(|st| {
                        st.std_blob_segments = std_metas;
                    }) {
                        drop(reader);
                        drop(writer);
                        if !self.meta_mgr.is_poisoned() {
                            let _ = self.fs.remove_file(&format_std_blob_file_name(seq_id));
                        }
                        return Err(e);
                    }
                    self.invalidate_snapshot_std_blobs();
                    self.std_blobs.insert(
                        seq_id,
                        TrackedStdBlob {
                            reader,
                            file_len: 0,
                            dead_bytes: 0,
                        },
                    );
                    self.active_std_writer = Some(writer);
                }
                let writer = self.active_std_writer.as_mut().unwrap();
                let ptr = writer.append_blob(key, value)?;
                if let Some(b) = self.std_blobs.get_mut(&writer.seq_id()) {
                    let prev_len = b.file_len;
                    b.file_len = writer.current_len();
                    if self.statement_savepoint_active {
                        let delta = b.file_len.saturating_sub(prev_len);
                        if delta > 0 {
                            self.statement_blob_undo.push(tx::BlobUndo::Appended {
                                kind: BlobKind::Standard,
                                table_id: 0,
                                seg_id: writer.seq_id(),
                                len: delta,
                            });
                        }
                    }
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
                        old_w.sync_durable().map_err(|e| self.poison(e))?;
                        if let Some(b) = self.lru_blobs.get_mut(&(table_id, old_w.seq_id())) {
                            b.file_len = old_w.current_len();
                        }
                    }
                    let seq_id = self.meta_mgr.reserve_seq_id();
                    let writer =
                        BlobSegmentWriter::open(&*self.fs, BlobKind::Lru, table_id, seq_id)?;
                    let reader = writer.reader();
                    let mut lru_metas = self.build_checkpointed_lru_blob_metas();
                    lru_metas.push(LruBlobSegmentMeta {
                        seq_id,
                        file_len: 0,
                        dead_bytes: 0,
                        table_id,
                    });
                    lru_metas.sort_by_key(|m| (m.table_id, m.seq_id));
                    if let Err(e) = self.meta_mgr.commit_update(|st| {
                        st.lru_blob_segments = lru_metas;
                    }) {
                        drop(reader);
                        drop(writer);
                        if !self.meta_mgr.is_poisoned() {
                            let _ = self
                                .fs
                                .remove_file(&format_lru_blob_file_name(table_id, seq_id));
                        }
                        return Err(e);
                    }
                    self.invalidate_snapshot_lru_blobs();
                    self.lru_blobs.insert(
                        (table_id, seq_id),
                        TrackedLruBlob {
                            table_id,
                            reader,
                            file_len: 0,
                            dead_bytes: 0,
                        },
                    );
                    self.active_lru_writers.insert(table_id, writer);
                }
                let writer = self.active_lru_writers.get_mut(&table_id).unwrap();
                let ptr = writer.append_blob(key, value)?;
                if let Some(b) = self.lru_blobs.get_mut(&(table_id, writer.seq_id())) {
                    let prev_len = b.file_len;
                    b.file_len = writer.current_len();
                    if self.statement_savepoint_active {
                        let delta = b.file_len.saturating_sub(prev_len);
                        if delta > 0 {
                            self.statement_blob_undo.push(tx::BlobUndo::Appended {
                                kind: BlobKind::Lru,
                                table_id,
                                seg_id: writer.seq_id(),
                                len: delta,
                            });
                        }
                    }
                }
                Ok(ptr)
            }
        }
    }

    pub(super) fn record_dead_blob_bytes(&mut self, ptr: &BlobPtr) {
        match ptr.kind {
            BlobKind::Standard => {
                if let Some(b) = self.std_blobs.get_mut(&ptr.seg_id) {
                    let prev_dead = b.dead_bytes;
                    b.dead_bytes = b.dead_bytes.saturating_add(ptr.len as u64);
                    if self.statement_savepoint_active {
                        let delta = b.dead_bytes.saturating_sub(prev_dead);
                        if delta > 0 {
                            self.statement_blob_undo.push(tx::BlobUndo::MarkedDead {
                                kind: BlobKind::Standard,
                                table_id: 0,
                                seg_id: ptr.seg_id,
                                len: delta,
                            });
                        }
                    }
                }
            }
            BlobKind::Lru => {
                if let Some(b) = self.lru_blobs.get_mut(&(ptr.table_id, ptr.seg_id)) {
                    let prev_dead = b.dead_bytes;
                    b.dead_bytes = b.dead_bytes.saturating_add(ptr.len as u64);
                    if self.statement_savepoint_active {
                        let delta = b.dead_bytes.saturating_sub(prev_dead);
                        if delta > 0 {
                            self.statement_blob_undo.push(tx::BlobUndo::MarkedDead {
                                kind: BlobKind::Lru,
                                table_id: ptr.table_id,
                                seg_id: ptr.seg_id,
                                len: delta,
                            });
                        }
                    }
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
        self.read_blob_ref_src(ReadSource::Owner, ptr, expected_key)
    }

    fn read_blob_ref_src(
        &self,
        src: ReadSource<'_, FS::File>,
        ptr: &BlobPtr,
        expected_key: &[u8],
    ) -> io::Result<Option<Vec<u8>>> {
        let mut out = Vec::new();
        let cache_key = BlobCacheKey::from_ptr(ptr, expected_key);
        if self.blob_cache.get_append_into(&cache_key, &mut out) {
            return Ok(Some(out));
        }

        let res = match self.blob_reader_for(src, ptr) {
            Some(reader) => reader.read_blob(ptr, Some(expected_key))?,
            None => return Ok(None),
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
        cx: &ReadCtx<'_, FS::File>,
        ptr: &BlobPtr,
        expected_key: &[u8],
        out: &mut Vec<u8>,
    ) -> io::Result<bool> {
        self.ensure_flushed_cx(cx, ptr)?;
        self.read_flushed_blob_append_into_src(cx.src, ptr, expected_key, out)
    }

    fn read_flushed_blob_append_into_src(
        &self,
        src: ReadSource<'_, FS::File>,
        ptr: &BlobPtr,
        expected_key: &[u8],
        out: &mut Vec<u8>,
    ) -> io::Result<bool> {
        let cache_key = BlobCacheKey::from_ptr(ptr, expected_key);

        let orig_len = out.len();
        if self.blob_cache.get_append_into(&cache_key, out) {
            return Ok(true);
        }

        let reader = self.blob_reader_for(src, ptr);
        let ok = match reader {
            Some(reader) if out.is_empty() => {
                reader.read_blob_into(ptr, Some(expected_key), out)?
            }
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
        self.commit_wal_and_memtable_with_data_flag(records, durability, true)
    }

    pub(super) fn commit_wal_and_memtable_with_data_flag(
        &mut self,
        records: Vec<WalRecord>,
        durability: WriteDurability,
        count_as_data_write: bool,
    ) -> io::Result<u64> {
        self.commit_wal_and_memtable_core(records, durability, count_as_data_write, true)
    }

    pub(super) fn commit_wal_and_memtable_core(
        &mut self,
        records: Vec<WalRecord>,
        durability: WriteDurability,
        count_as_data_write: bool,
        allow_auto_flush: bool,
    ) -> io::Result<u64> {
        if count_as_data_write || self.tx_depth > 0 {
            self.mutation_epoch = self.mutation_epoch.wrapping_add(1);
        }
        self.check_not_poisoned()?;
        let lsn = self.next_lsn;
        let successor = self.lsn_after_batches(1)?;

        if self.tx_depth > 0 {
            let batch_bytes: usize = records
                .iter()
                .map(|r| match r {
                    WalRecord::PutInline { key, value } => 9 + key.len() + value.len(),
                    WalRecord::PutBlobPtr { key, blob_ptr } => {
                        28 + key.len() + (blob_ptr.len as usize)
                    }
                    WalRecord::Delete { key } => 5 + key.len(),
                    WalRecord::TxBegin | WalRecord::TxCommit | WalRecord::TxContinue => 1,
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
            let record_count = u32::try_from(records.len())
                .ok()
                .and_then(|n| self.pending_tx_record_count.checked_add(n))
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "too many records in transaction",
                    )
                })?;
            self.pending_tx_bytes = next_tx_bytes;
            self.next_lsn = successor;
            if durability == WriteDurability::SyncTransactional {
                self.pending_tx_sync = true;
            }
            if self.pending_tx_record_count == 0 {
                self.pending_tx_buf.clear();
                self.pending_tx_buf
                    .extend_from_slice(&[0u8; crate::wal::WAL_BATCH_HEADER_SIZE]);
            }
            WalBatch::append_records_to_buf(&records, &mut self.pending_tx_buf);
            self.pending_tx_record_count = record_count;
            self.note_records_table_mutations(&records);
            let record_undo = !self.savepoints.is_empty();
            for rec in records {
                self.tx_overlay.apply(lsn, rec, record_undo);
            }
            return Ok(lsn);
        }

        let has_records = !records.is_empty();
        self.drain_pending_durable()?;
        let mut batch = WalBatch::new(lsn, records);
        self.flush_active_blob_buffers()?;
        (batch.prev_lsn, batch.durable_lsn) = self.wal_links_for_append(durability)?;
        let mut raw = core::mem::take(&mut self.scratch_wal_encoded);
        batch.encode_into(&mut raw);
        let res = self.append_raw_wal_batch_with_rotation(&raw, durability);
        raw.clear();
        self.scratch_wal_encoded = raw;
        res?;
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
            // The batch is committed; a failed flush is retried because `should_flush` stays true.
            if self.memtable.should_flush() {
                if let Err(e) = self.flush_memtable() {
                    self.record_maintenance_failure(&e);
                }
            }
            // Safe point: the batch is appended and `next_lsn` has advanced.
            self.run_due_auto_compaction();
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
        self.buf_wal
            .make_durable_and_seal(durable)
            .map_err(|e| self.poison(e))?;
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
        let r = match durability {
            WriteDurability::SyncTransactional => self.sync_wal.append_raw_batch(encoded),
            WriteDurability::BufferedFast => self.buf_wal.append_raw_batch(encoded),
        };
        let outcome = r.map_err(|e| self.poison(e))?;

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
                self.next_lsn <= batch_lsn && self.meta_mgr.state().checkpoint_lsn < batch_lsn,
                "WAL rotation must not consume the LSN of the batch being appended"
            );
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
                let synced = !self.sync_wal.barrier_deferred();
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
            if self.tx_overlay.may_contain_key(key) {
                if let Some(e) = self.tx_overlay.map.get(key) {
                    return Some(e);
                }
            }
            if self.tx_depth > 0 && !self.pending_group.is_empty() && !self.is_poisoned() {
                for (_, entries) in self.pending_group.iter().rev() {
                    if let Some(e) = entries.get(key) {
                        return Some(e);
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
