//! Phase 2: SQLite Virtual Table (`sqlite3_module`) Adapter & Safe SQLite Connection Wrapper.
//!
//! Exposes `aydb`'s [`StorageEngine`] as a first-class SQLite Virtual Table (`USING aydb(...)`)
//! with automatic primary-key point lookups (`xBestIndex` Plan 1), secondary index equality
//! lookups (Plan 2), primary-key range scans (Plan 3), secondary index range scans (Plan 4),
//! and Capped Table (`MAX_BYTES`) LRU eviction.

mod compaction_worker;
#[cfg(test)]
mod connect_tests;
pub mod connection;
#[cfg(test)]
mod constraint_tests;
mod constraints;
mod ddl;
pub mod ffi;
pub mod row_codec;
pub mod rowid_map;
#[cfg(test)]
mod rowid_tests;
mod snapshot;
#[cfg(test)]
mod snapshot_tests;
#[cfg(test)]
mod tests;

use core::ffi::{c_char, c_int, c_void};
use std::ffi::CString;
use std::io;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Instant;

use crate::engine::LazyBlob;
use crate::engine::{CommitStep, EngineReader, GroupRole, StorageEngine, WriteDurability};
use crate::pal::{OsFile, OsFs, PlatformFile};
pub use crate::vtab::connection::{SqliteConnection, SqliteStatement};
use crate::vtab::ddl::{ParsedVtabDdl, parse_vtab_ddl};
use crate::vtab::ffi::{
    SQLITE_BUSY, SQLITE_CONSTRAINT, SQLITE_CORRUPT, SQLITE_ERROR, SQLITE_FLOAT, SQLITE_FULL,
    SQLITE_IGNORE, SQLITE_INDEX_CONSTRAINT_EQ, SQLITE_INDEX_CONSTRAINT_GE,
    SQLITE_INDEX_CONSTRAINT_GT, SQLITE_INDEX_CONSTRAINT_LE, SQLITE_INDEX_CONSTRAINT_LT,
    SQLITE_INDEX_SCAN_UNIQUE, SQLITE_INTEGER, SQLITE_IOERR, SQLITE_NULL, SQLITE_OK, SQLITE_REPLACE,
    SQLITE_TEXT, sqlite3, sqlite3_context, sqlite3_declare_vtab, sqlite3_index_info, sqlite3_int64,
    sqlite3_module, sqlite3_result_error_code, sqlite3_result_int64, sqlite3_value,
    sqlite3_value_bytes, sqlite3_value_double, sqlite3_value_int64, sqlite3_value_text,
    sqlite3_value_type, sqlite3_vtab, sqlite3_vtab_collation, sqlite3_vtab_cursor,
    sqlite3_vtab_on_conflict,
};
pub use crate::vtab::row_codec::{
    ColCollation, ColumnAffinity, KEY_TAG_BLOB, KEY_TAG_FLOAT_GE_I64_MAX_PLUS_1,
    KEY_TAG_FLOAT_LT_I64_MIN, KEY_TAG_INTEGER, KEY_TAG_NULL, KEY_TAG_TEXT, SqlValue,
    decode_all_columns, decode_column_at, decode_f64_memcomparable, decode_i64_tagged_key,
    encode_canonical_numeric_key_from_f64, encode_f64_memcomparable, encode_f64_unified_key,
    encode_i64_tagged_key, encode_row, encode_sqlite_values_direct, extract_column_key_bytes,
    extract_column_key_bytes_with_collation, f64_to_exact_i64, format_sqlite_float_text,
    parse_sqlite_numeric_str, project_column_zero_copy, sqlite_value_to_key_bytes,
    sqlite_value_to_key_bytes_with_schema,
};
pub use crate::vtab::rowid_map::hash_pk_to_rowid;
use crate::vtab::rowid_map::{extract_rowid_from_pk_bytes, extract_rowid_from_row};

const PLAN_FULL_SCAN: c_int = 0;
const PLAN_PK_POINT: c_int = 1;
const PLAN_SECONDARY_EQ: c_int = 2;
const PLAN_PK_RANGE: c_int = 3;
const PLAN_SECONDARY_RANGE: c_int = 4;

const POINT_FLAG_BY_ROWID: c_int = 0x01;
/// Point lookup only needs existence + primary key (no value read), so a corrupt out-of-line
/// blob never fails `DELETE ... WHERE pk = ?` / `SELECT pk ... WHERE pk = ?`.
const POINT_FLAG_KEYS_ONLY: c_int = 0x02;

const RANGE_FLAG_LOWER_INCLUSIVE: c_int = 0x01;
const RANGE_FLAG_LOWER_EXCLUSIVE: c_int = 0x02;
const RANGE_FLAG_UPPER_INCLUSIVE: c_int = 0x04;
const RANGE_FLAG_UPPER_EXCLUSIVE: c_int = 0x08;
const RANGE_FLAG_KEYS_ONLY: c_int = 0x20;
const RANGE_FLAG_REVERSE: c_int = 0x40;

/// Maximum number of rows fetched per streaming cursor batch, bounding `cursor.slab`
/// to $O(1)$ memory regardless of table size while eliminating full-scan materialization on `LIMIT` queries.
const CURSOR_STREAM_CHUNK_ROWS: usize = 256;

/// Returns the alternate exact-integer numeric key representation (`9`-byte `Integer(q)` vs
/// `17`-byte `Float(q.0)`) so SQLite's `10 == 10.0` numeric key equality holds across `BLOB`
/// and `REAL` affinities.
#[inline]
fn build_numeric_alt_key(pk: &[u8]) -> Option<Vec<u8>> {
    if pk.first().copied() != Some(KEY_TAG_INTEGER) {
        return None;
    }
    if pk.len() == 9 {
        let mut alt = Vec::with_capacity(17);
        alt.extend_from_slice(pk);
        alt.extend_from_slice(&0u64.to_be_bytes());
        Some(alt)
    } else if pk.len() == 17 && pk[9..17] == [0u8; 8] {
        Some(pk[0..9].to_vec())
    } else {
        None
    }
}

/// Checks if `pk` or its exact-integer numeric dual (`build_numeric_alt_key(pk)`) exists in
/// `table_id`, returning the existing key bytes if found.
///
/// When `pk_affinity` is `ColumnAffinity::Integer` or `ColumnAffinity::Numeric`, all exact-integer
/// floats (`10.0`) are coerced to 9-byte `Integer(q)` before storage, so a 17-byte `Float(q.0)`
/// key can never exist in the table and the second probe is skipped.
#[inline]
fn find_existing_pk_or_numeric_alt(
    eng: &mut StorageEngine<OsFs>,
    table_id: u16,
    pk: &[u8],
    pk_affinity: ColumnAffinity,
) -> io::Result<Option<Vec<u8>>> {
    if eng.row_exists(table_id, pk)? {
        return Ok(Some(pk.to_vec()));
    }
    if !matches!(
        pk_affinity,
        ColumnAffinity::Integer | ColumnAffinity::Numeric
    ) {
        if let Some(alt) = build_numeric_alt_key(pk) {
            if eng.row_exists(table_id, &alt)? {
                return Ok(Some(alt));
            }
        }
    }
    Ok(None)
}

/// Point lookup used by `PLAN_PK_POINT`: reads the row value into the cursor slab, or (for
/// keys-only plans) merely checks existence without touching any out-of-line blob.
fn point_fetch(
    eng: &EngineReader<'_, OsFs>,
    table_id: u16,
    pk: &[u8],
    keys_only: bool,
    slab: &mut Vec<u8>,
    spans: &mut Vec<(usize, usize)>,
) -> io::Result<bool> {
    if keys_only {
        eng.row_exists(table_id, pk)
    } else {
        eng.get_row_into(table_id, pk, slab, spans)
    }
}

/// Runs read callback body `f` with the cheapest sufficient engine lock.
///
/// The lock choice is made while holding the shared lock: if `read_needs_exclusive` is false the
/// body runs on a shared-access [`EngineReader`] (many readers in parallel); otherwise the shared
/// lock is released first (never upgraded in place) and the body runs under the write lock via
/// `with_committed_read_for_conn`, which re-resolves the state and so needs no recheck. Returns
/// `None` if the lock is poisoned. `f` may run a second time if a shared-lock read reports that it
/// needed exclusive access (defensive; cannot happen when the decision rules hold).
fn with_read_access<R>(
    engine: &RwLock<StorageEngine<OsFs>>,
    conn_id: usize,
    table_id: u16,
    mut f: impl FnMut(&EngineReader<'_, OsFs>) -> R,
) -> Option<R> {
    {
        let g = engine.read().ok()?;
        if !g.vtab_read_needs_exclusive(conn_id, table_id) {
            let rd = g.reader_for_conn(conn_id);
            let r = f(&rd);
            if !rd.needs_exclusive_retry() {
                return Some(r);
            }
        }
    }
    let mut g = engine.write().ok()?;
    Some(g.with_committed_read_for_conn(conn_id, f))
}

fn with_snapshot_read_access<R>(
    engine: &RwLock<StorageEngine<OsFs>>,
    conn_id: usize,
    table_id: u16,
    snapshot: Option<&crate::engine::ReadSnapshotToken>,
    f: impl FnMut(&EngineReader<'_, OsFs>) -> R,
) -> Option<R> {
    if let Some(snapshot) = snapshot {
        let engine = engine.read().ok()?;
        let mut f = f;
        Some(f(&engine.reader_for_snapshot(snapshot)))
    } else {
        with_read_access(engine, conn_id, table_id, f)
    }
}

/// Maps a `StorageEngine` [`io::Error`] into the most specific SQLite result code.
#[inline]
fn io_err_to_sqlite_rc(err: &io::Error) -> c_int {
    match err.kind() {
        io::ErrorKind::WouldBlock => SQLITE_BUSY,
        io::ErrorKind::InvalidData => SQLITE_CORRUPT,
        io::ErrorKind::InvalidInput | io::ErrorKind::AlreadyExists => SQLITE_ERROR,
        _ => SQLITE_IOERR,
    }
}

/// Re-establishes the persistent rowid mappings if they are not visible to this connection.
/// Connecting a legacy table migrates it inside whatever transaction is open at that moment; if
/// that transaction (or a savepoint) rolls back, the mappings and their completion marker roll
/// back with it while this vtab instance stays connected. The migration is resumable and runs
/// only when a marker has been lost. Once the marker has been seen committed, `rowids_committed`
/// latches and no further check is made: a committed marker is removed only by `DROP TABLE`,
/// which destroys this vtab instance.
fn ensure_rowids_for_conn(eng: &mut StorageEngine<OsFs>, vtab: &AydbVTab) -> std::io::Result<()> {
    if vtab.rowids_committed.load(Ordering::Acquire) {
        return Ok(());
    }
    rowid_map::ensure_persistent_rowids(
        eng,
        vtab.table_id,
        vtab.durability,
        vtab.pk_affinity == ColumnAffinity::Integer,
    )?;
    latch_if_marker_committed(eng, vtab);
    Ok(())
}

/// Latches only on a marker visible in the committed view, never on one that exists solely in
/// the owner's open transaction, which a rollback would remove.
fn latch_if_marker_committed(eng: &StorageEngine<OsFs>, vtab: &AydbVTab) {
    if eng
        .rowids_initialized_view(crate::engine::ReadView::Committed, vtab.table_id)
        .unwrap_or(false)
    {
        vtab.rowids_committed.store(true, Ordering::Release);
    }
}

/// Scan-side variant of [`ensure_rowids_for_conn`]: no lock at all once latched, a shared-lock
/// marker check before that, and the writer lock only when the mappings must be rebuilt.
fn ensure_rowids_before_scan(vtab: &AydbVTab) -> Result<(), c_int> {
    if vtab.rowids_committed.load(Ordering::Acquire) {
        return Ok(());
    }
    let conn_id = vtab.db as usize;
    {
        let eng = vtab.engine.read().map_err(|_| SQLITE_ERROR)?;
        if eng
            .rowids_initialized_view(crate::engine::ReadView::Committed, vtab.table_id)
            .unwrap_or(false)
        {
            vtab.rowids_committed.store(true, Ordering::Release);
            return Ok(());
        }
        let view = eng.read_view_for_conn(conn_id);
        // An unreadable marker is left to the read itself to report.
        if eng
            .rowids_initialized_view(view, vtab.table_id)
            .unwrap_or(true)
        {
            return Ok(());
        }
    }
    let mut eng = vtab.engine.write().map_err(|_| SQLITE_ERROR)?;
    eng.check_write_allowed_for_conn(conn_id)
        .map_err(|e| io_err_to_sqlite_rc(&e))?;
    ensure_rowids_for_conn(&mut eng, vtab).map_err(|e| io_err_to_sqlite_rc(&e))
}

unsafe fn constraint_matches_col_collation(
    p_info: *mut sqlite3_index_info,
    constraint_idx: usize,
    col_collation: ColCollation,
) -> bool {
    let ptr = unsafe { sqlite3_vtab_collation(p_info, constraint_idx as c_int) };
    if ptr.is_null() {
        return true;
    }
    let Ok(cstr) = (unsafe { core::ffi::CStr::from_ptr(ptr) }).to_str() else {
        return false;
    };
    let parsed = if cstr.eq_ignore_ascii_case("BINARY") {
        Some(ColCollation::Binary)
    } else if cstr.eq_ignore_ascii_case("NOCASE") {
        Some(ColCollation::NoCase)
    } else if cstr.eq_ignore_ascii_case("RTRIM") {
        Some(ColCollation::RTrim)
    } else {
        None
    };
    parsed == Some(col_collation)
}

const I64_MIN_F64: f64 = -9_223_372_036_854_775_808.0;
const I64_MAX_PLUS_1_F64: f64 = 9_223_372_036_854_775_808.0;

#[inline]
fn f64_to_lossless_i64(f: f64) -> Option<i64> {
    if f.is_finite() && f.fract() == 0.0 && (I64_MIN_F64..I64_MAX_PLUS_1_F64).contains(&f) {
        Some(f as i64)
    } else {
        None
    }
}

unsafe fn extract_lossless_rowid_arg(val: *mut sqlite3_value) -> Option<i64> {
    if val.is_null() {
        return None;
    }
    match unsafe { sqlite3_value_type(val) } {
        SQLITE_INTEGER => Some(unsafe { sqlite3_value_int64(val) }),
        SQLITE_FLOAT => f64_to_lossless_i64(unsafe { sqlite3_value_double(val) }),
        SQLITE_TEXT => {
            let bytes = unsafe { sqlite3_value_bytes(val) } as usize;
            let text_ptr = unsafe { sqlite3_value_text(val) };
            if text_ptr.is_null() {
                return None;
            }
            let slice = unsafe { core::slice::from_raw_parts(text_ptr, bytes) };
            let s = core::str::from_utf8(slice).ok()?;
            let trimmed = s.trim();
            if let Ok(i) = trimmed.parse::<i64>() {
                Some(i)
            } else if let Ok(f) = trimmed.parse::<f64>() {
                f64_to_lossless_i64(f)
            } else {
                None
            }
        }
        _ => None,
    }
}

fn resolve_pk_for_rowid(
    vtab: &AydbVTab,
    target_rid: i64,
    row_exists: impl FnOnce(u16, &[u8]) -> io::Result<bool>,
    lookup: impl FnOnce(u16, i64) -> io::Result<Option<Vec<u8>>>,
) -> io::Result<Option<Vec<u8>>> {
    if vtab.pk_affinity == ColumnAffinity::Integer {
        let integer_pk = encode_i64_tagged_key(target_rid);
        if row_exists(vtab.table_id, &integer_pk)? {
            return Ok(Some(integer_pk.to_vec()));
        }
    }
    // A local cache may contain a deleted or reused identity from another connection.
    // Resolve against the same persisted read view used for the row itself.
    let found = lookup(vtab.table_id, target_rid)?;
    Ok(found)
}

/// Static `sqlite3_module` method table registered with `sqlite3_create_module_v2`.
pub(super) static AYDB_VTAB_MODULE: sqlite3_module = sqlite3_module {
    iVersion: 2,
    xCreate: Some(vtab_create_or_connect),
    xConnect: Some(vtab_create_or_connect),
    xBestIndex: Some(vtab_best_index),
    xDisconnect: Some(vtab_disconnect),
    xDestroy: Some(vtab_destroy),
    xOpen: Some(vtab_open),
    xClose: Some(vtab_close),
    xFilter: Some(vtab_filter),
    xNext: Some(vtab_next),
    xEof: Some(vtab_eof),
    xColumn: Some(vtab_column),
    xRowid: Some(vtab_rowid),
    xUpdate: Some(vtab_update),
    xBegin: Some(vtab_begin_tx),
    xSync: Some(vtab_sync_tx),
    xCommit: Some(vtab_commit_tx),
    xRollback: Some(vtab_rollback_tx),
    xFindFunction: None,
    xRename: None,
    xSavepoint: Some(vtab_savepoint),
    xRelease: Some(vtab_release),
    xRollbackTo: Some(vtab_rollback_to),
    xShadowName: None,
    xIntegrity: None,
};

pub(super) struct ModuleAuxContext {
    pub(super) engine: Arc<RwLock<StorageEngine<OsFs>>>,
    read_session: Arc<snapshot::ReadSession>,
    /// Background compaction worker; joined when the module (and thus this context) is dropped.
    pub(super) _compaction_worker: Option<compaction_worker::CompactionWorker>,
}

#[repr(C)]
pub(super) struct AydbVTab {
    base: sqlite3_vtab,
    db: *mut sqlite3,
    engine: Arc<RwLock<StorageEngine<OsFs>>>,
    read_session: Arc<snapshot::ReadSession>,
    table_id: u16,
    table_name: String,
    durability: WriteDurability,
    /// Set once the rowid migration marker has been observed in the committed view.
    rowids_committed: AtomicBool,
    col_names: Vec<String>,
    col_affinities: Vec<ColumnAffinity>,
    col_not_null: Vec<bool>,
    col_unique: Vec<bool>,
    col_collations: Vec<ColCollation>,
    pub(super) pk_col_idx: usize,
    pub(super) pk_affinity: ColumnAffinity,
    pub(super) pk_collation: ColCollation,
    /// Compact list of `(col_idx, index_id)` (`1..=N`) for secondary indexed columns.
    secondary_indexes: Vec<(usize, u16)>,
    /// Next monotonically increasing auto-assigned rowid for `INTEGER PRIMARY KEY`.
    next_auto_rowid: AtomicI64,
}

#[repr(C)]
struct AydbCursor {
    base: sqlite3_vtab_cursor,
    snapshot: Option<crate::engine::ReadSnapshotToken>,
    /// Contiguous reusable byte slab holding scanned `row_bytes` for the current cursor batch.
    slab: Vec<u8>,
    /// `(start_offset, end_offset)` into `slab` for each row.
    spans: Vec<(usize, usize)>,
    /// Primary key bytes per row.
    pks: Vec<Vec<u8>>,
    /// Pending out-of-line blob pointer for lazy dereference in `vtab_column`.
    /// When `Some(ptr)`, the row's blob payload has not been read into `slab` yet.
    /// When `None`, the row was either inline or has already been dereferenced into `slab`.
    lazy_blobs: Vec<Option<LazyBlob<OsFile>>>,
    /// True when the active scan only needs primary keys (`RANGE_FLAG_KEYS_ONLY`),
    /// avoiding any out-of-line blob reads or `slab` allocations.
    keys_only: bool,
    pos: usize,
    /// Exclusive end of the `lazy_blobs` window for which a prefetch was already issued in the
    /// current batch. A new prefetch is built only when `pos` reaches it; it is reset together
    /// with `lazy_blobs`.
    prefetch_end: usize,
    /// True when more chunks may remain in the current range/full/secondary-index scan.
    streaming_active: bool,
    stream_lower: Option<(Vec<u8>, bool)>,
    stream_upper: Option<(Vec<u8>, bool)>,
    stream_reverse: bool,
    stream_last_pk: Option<Vec<u8>>,
    /// Active secondary index ID when streaming `PLAN_SECONDARY_EQ` or `PLAN_SECONDARY_RANGE`.
    stream_sec_index_id: Option<u16>,
    /// Prefix key for the active secondary index scan.
    stream_sec_prefix: Vec<u8>,
}

/// Converts a SQLite range bound value into a storage-class-tagged key for `PLAN_PK_RANGE`,
/// applying the column's declared [`ColumnAffinity`] and [`ColCollation`].
///
/// Because `Integer(q)` (`9` bytes: `[0x10, encode_i64(q)]`) and `Float(q.0)` (`17` bytes:
/// `[0x10, encode_i64(q), 0..0]`) share the same mathematical value `q`, an upper or lower bound
/// at an exact integer `q` adjusts inclusivity so both representations are included or excluded
/// consistently!
///
/// Returns `Ok(Some((key_bytes, is_inclusive)))` for a normal bound, `Ok(None)` if the bound
/// is unbounded in that direction, or `Err(())` if the range is unsatisfiable (empty result).
unsafe fn pk_range_bound_to_key(
    val_ptr: *mut sqlite3_value,
    pk_affinity: ColumnAffinity,
    pk_collation: ColCollation,
    is_lower: bool,
    is_inclusive: bool,
) -> Result<Option<(Vec<u8>, bool)>, ()> {
    let val_type = unsafe { sqlite3_value_type(val_ptr) };
    if val_type == SQLITE_NULL {
        return Err(());
    }
    if val_type == SQLITE_FLOAT {
        let f = unsafe { sqlite3_value_double(val_ptr) };
        if f.is_nan() {
            return Err(());
        }
    }

    let key =
        unsafe { sqlite_value_to_key_bytes_with_schema(val_ptr, pk_affinity, pk_collation, false) };

    // If the bound is an exact integer `q` (either `9`-byte `[0x10, q]` or `17`-byte `[0x10, q, 0..0]`),
    // ensure both `9`-byte `Integer(q)` and `17`-byte `Float(q.0)` are handled identically:
    // - Lower inclusive (`>= q`): start at 9-byte `[0x10, q]` (inclusive) -> includes both!
    // - Lower exclusive (`> q`): start after 17-byte `[0x10, q, 0..0]` (exclusive) -> excludes both!
    // - Upper inclusive (`<= q`): end at 17-byte `[0x10, q, 0..0]` (inclusive) -> includes both!
    // - Upper exclusive (`< q`): end before 9-byte `[0x10, q]` (exclusive) -> excludes both!
    if key.first().copied() == Some(KEY_TAG_INTEGER) {
        if key.len() == 9 {
            if (is_lower && !is_inclusive) || (!is_lower && is_inclusive) {
                let mut ext = Vec::with_capacity(17);
                ext.extend_from_slice(&key);
                ext.extend_from_slice(&0u64.to_be_bytes());
                return Ok(Some((ext, is_inclusive)));
            }
            return Ok(Some((key, is_inclusive)));
        } else if key.len() == 17 && key[9..17] == [0u8; 8] {
            if (is_lower && is_inclusive) || (!is_lower && !is_inclusive) {
                return Ok(Some((key[0..9].to_vec(), is_inclusive)));
            }
            return Ok(Some((key, is_inclusive)));
        }
    }

    Ok(Some((key, is_inclusive)))
}

/// Converts a SQLite range bound value into a full secondary-index key bound for
/// `PLAN_SECONDARY_RANGE`.
///
/// Because secondary index keys have layout `[TableID | 'i' | IndexID | Escaped(col_val) | pk]`
/// where `Escaped(col_val)` terminates with `[0x00, 0x01]` and `pk` begins with a type tag
/// (`<= 0x30 < 0xFF`), appending `0xFF` to the exact prefix of `col_val` yields a key strictly
/// greater than every `(col_val, pk)` entry and strictly less than every `(col_val', pk')` with
/// `col_val' > col_val`.
unsafe fn sec_range_bound_to_key(
    val_ptr: *mut sqlite3_value,
    table_id: u16,
    index_id: u16,
    sec_affinity: ColumnAffinity,
    sec_collation: ColCollation,
    is_lower: bool,
    is_inclusive: bool,
) -> Result<Option<(Vec<u8>, bool)>, ()> {
    let val_type = unsafe { sqlite3_value_type(val_ptr) };
    if val_type == SQLITE_NULL {
        return Err(());
    }
    if val_type == SQLITE_FLOAT {
        let f = unsafe { sqlite3_value_double(val_ptr) };
        if f.is_nan() {
            return Err(());
        }
    }

    let col_val = unsafe {
        sqlite_value_to_key_bytes_with_schema(val_ptr, sec_affinity, sec_collation, true)
    };
    let mut exact_prefix =
        crate::lru::encode_secondary_index_exact_prefix(table_id, index_id, &col_val);

    if (is_lower && !is_inclusive) || (!is_lower && is_inclusive) {
        exact_prefix.push(0xFF);
    }
    Ok(Some((exact_prefix, is_inclusive)))
}

pub(super) unsafe extern "system" fn destroy_aux_context(ptr: *mut c_void) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if !ptr.is_null() {
            unsafe {
                drop(Box::from_raw(ptr as *mut ModuleAuxContext));
            }
        }
    }));
}

/// Returns the largest integer-tagged primary key of `table_id` visible to `conn_id`, or `None`
/// if it has none. A connection that does not own the open transaction sees committed rows only.
fn max_integer_rowid(
    eng: &mut StorageEngine<OsFs>,
    conn_id: usize,
    table_id: u16,
) -> io::Result<Option<i64>> {
    let lower = [KEY_TAG_INTEGER];
    let upper = [KEY_TAG_INTEGER + 1];
    eng.with_committed_read_for_conn(conn_id, |reader| {
        let (mut pks, mut slab, mut spans) = (Vec::new(), Vec::new(), Vec::new());
        reader.scan_range_into_slab(
            table_id,
            Some((&lower, true)),
            Some((&upper, false)),
            true,
            1,
            true,
            &mut pks,
            &mut slab,
            &mut spans,
            None,
        )?;
        Ok(pks.first().and_then(|k| decode_i64_tagged_key(k)))
    })
}

fn integer_rowid_occupied(
    eng: &StorageEngine<OsFs>,
    table_id: u16,
    rowid: i64,
) -> io::Result<bool> {
    Ok(eng.row_exists(table_id, &encode_i64_tagged_key(rowid))?
        || eng
            .pk_for_rowid_view(crate::engine::ReadView::Owner, table_id, rowid)?
            .is_some())
}

/// What a successful connect resolved under the engine lock.
struct Connected {
    table_id: u16,
    initial_auto_rowid: i64,
    rowids_committed: bool,
}

/// Registers the table for a connection and prepares its runtime state, all under one engine
/// lock. Returns `None`, having changed nothing, when the table cannot be connected without
/// writing durable state while another connection's transaction is open: it is not yet in the
/// catalog, its capped quota is not registered as declared, or its rowid migration is pending.
///
/// With another transaction open, an existing migrated table is only registered as live (see
/// [`StorageEngine::activate_persisted_vtab`]) and its runtime-only settings are applied. None of
/// that is transactional state: the corrupt policy and inline threshold are not restored by a
/// rollback, and the live registration is added to the snapshots a rollback restores. The rowid
/// scan reads the committed view, so the other transaction's uncommitted rows cannot leave a gap
/// in the automatic rowid sequence if it rolls back.
fn connect_table(
    eng: &mut StorageEngine<OsFs>,
    conn_id: usize,
    ddl: &ParsedVtabDdl,
) -> io::Result<Option<Connected>> {
    let plan = eng.plan_vtab_registration(&ddl.table_name, ddl.explicit_table_id)?;
    let table_id = plan.table_id;
    if eng.check_write_allowed_for_conn(conn_id).is_err() {
        let quota_registered = ddl.max_bytes == 0
            || eng
                .capped_table_state(table_id)
                .is_some_and(|state| state.max_bytes == ddl.max_bytes);
        let migrated = eng
            .rowids_initialized_view(crate::engine::ReadView::Committed, table_id)
            .unwrap_or(false);
        if !(plan.persisted && quota_registered && migrated) {
            return Ok(None);
        }
        eng.activate_persisted_vtab(table_id, &ddl.table_name);
    } else {
        // Another connection may already use this registration: a failed connect must not
        // remove it.
        let was_active = eng.is_vtab_active(table_id, &ddl.table_name);
        if let Err(e) = register_and_migrate(eng, ddl, plan) {
            if !was_active {
                eng.unregister_vtab(table_id, &ddl.table_name);
            }
            return Err(e);
        }
    }
    // Runtime-only settings: re-derived from the DDL on every connect.
    match ddl.on_corrupt {
        Some(policy) => eng.set_table_corrupt_policy(table_id, policy),
        None => eng.reset_table_corrupt_policy(table_id),
    }
    match ddl.inline_max {
        Some(max_bytes) => eng.set_table_inline_threshold(table_id, max_bytes),
        None => eng.reset_table_inline_threshold(table_id),
    }
    // Resume automatic rowid assignment after the largest existing integer rowid so a
    // reconnect does not restart the sequence at 1 and fall into the random-rowid fallback.
    // A failed scan must not prevent the table from connecting (reads report the error
    // themselves), so it leaves the counter at its initial value.
    let initial_auto_rowid = if ddl.pk_affinity == ColumnAffinity::Integer {
        match max_integer_rowid(eng, conn_id, table_id) {
            Ok(Some(m)) if m >= 0 => m.saturating_add(1),
            _ => 0,
        }
    } else {
        0
    };
    let rowids_committed = eng
        .rowids_initialized_view(crate::engine::ReadView::Committed, table_id)
        .unwrap_or(false);
    Ok(Some(Connected {
        table_id,
        initial_auto_rowid,
        rowids_committed,
    }))
}

/// The writing part of a connect: catalog entry, capped quota, and the rowid migration of an
/// older table. It only runs when no other connection has a transaction open.
fn register_and_migrate(
    eng: &mut StorageEngine<OsFs>,
    ddl: &ParsedVtabDdl,
    plan: crate::engine::VtabPlan,
) -> io::Result<()> {
    let table_id = eng.register_vtab_plan(plan, ddl.durability)?;
    if ddl.max_bytes > 0 {
        eng.register_capped_table(table_id, ddl.max_bytes)?;
    }
    #[cfg(test)]
    if FAIL_CONNECT_AFTER_REGISTRATION.with(|flag| flag.replace(false)) {
        return Err(io::Error::other("injected connect failure"));
    }
    // Older databases have no rowid index. Populate missing identities in key order once;
    // every pair is an atomic WAL batch, so a failed connection can resume safely.
    rowid_map::ensure_persistent_rowids(
        eng,
        table_id,
        ddl.durability,
        ddl.pk_affinity == ColumnAffinity::Integer,
    )
}

#[cfg(test)]
thread_local! {
    /// Test seam: makes the next connect on this thread fail right after it registered the table.
    static FAIL_CONNECT_AFTER_REGISTRATION: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

unsafe extern "system" fn vtab_create_or_connect(
    db: *mut sqlite3,
    p_aux: *mut c_void,
    argc: c_int,
    argv: *const *const c_char,
    pp_vtab: *mut *mut sqlite3_vtab,
    _pz_err: *mut *mut c_char,
) -> c_int {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        if p_aux.is_null() || pp_vtab.is_null() {
            return SQLITE_ERROR;
        }
        let aux = &*(p_aux as *const ModuleAuxContext);

        let ddl = match parse_vtab_ddl(argc, argv) {
            Ok(d) => d,
            Err(rc) => return rc,
        };

        // Declare the columns first: it needs no engine state, so a failure leaves nothing
        // registered.
        let declare_sql = format!("CREATE TABLE x({})", ddl.columns_spec);
        let Ok(c_sql) = CString::new(declare_sql) else {
            return SQLITE_ERROR;
        };
        let rc = sqlite3_declare_vtab(db, c_sql.as_ptr());
        if rc != SQLITE_OK {
            return rc;
        }

        // Connecting an existing, migrated table writes no durable state and never waits.
        // Creating a table, or migrating a legacy one, does: those wait for another connection's
        // transaction to end, bounded like `xBegin`, because the same thread may hold that
        // connection open and could never end it.
        let mut deadline = None;
        let connected = loop {
            let mut eng = match aux.engine.write() {
                Ok(g) => g,
                Err(_) => return SQLITE_ERROR,
            };
            match connect_table(&mut eng, db as usize, &ddl) {
                Ok(Some(connected)) => break connected,
                Ok(None) => {
                    let now = Instant::now();
                    let end = *deadline.get_or_insert(now + eng.writer_wait_timeout());
                    if now >= end {
                        return SQLITE_BUSY;
                    }
                    let gate = eng.commit_gate();
                    let seen = gate.tx_end_count();
                    drop(eng);
                    gate.wait_tx_end(seen, end - now);
                }
                Err(e) => return io_err_to_sqlite_rc(&e),
            }
        };
        let Connected {
            table_id,
            initial_auto_rowid,
            rowids_committed,
        } = connected;

        let vtab = Box::new(AydbVTab {
            base: sqlite3_vtab {
                pModule: &AYDB_VTAB_MODULE,
                nRef: 0,
                zErrMsg: core::ptr::null_mut(),
            },
            db,
            engine: aux.engine.clone(),
            read_session: Arc::clone(&aux.read_session),
            table_id,
            table_name: ddl.table_name,
            durability: ddl.durability,
            rowids_committed: AtomicBool::new(rowids_committed),
            col_names: ddl.col_names,
            col_affinities: ddl.col_affinities,
            col_not_null: ddl.col_not_null,
            col_unique: ddl.col_unique,
            col_collations: ddl.col_collations,
            pk_col_idx: ddl.pk_col_idx,
            pk_affinity: ddl.pk_affinity,
            pk_collation: ddl.pk_collation,
            secondary_indexes: ddl.secondary_indexes,
            next_auto_rowid: AtomicI64::new(initial_auto_rowid),
        });

        *pp_vtab = Box::into_raw(vtab) as *mut sqlite3_vtab;
        SQLITE_OK
    }))
    .unwrap_or(SQLITE_ERROR)
}

unsafe extern "system" fn vtab_disconnect(p_vtab: *mut sqlite3_vtab) -> c_int {
    catch_unwind(AssertUnwindSafe(|| {
        if !p_vtab.is_null() {
            unsafe {
                let vtab = Box::from_raw(p_vtab as *mut AydbVTab);
                if let Ok(mut eng) = vtab.engine.write() {
                    eng.unregister_vtab(vtab.table_id, &vtab.table_name);
                }
            }
        }
        SQLITE_OK
    }))
    .unwrap_or(SQLITE_ERROR)
}

unsafe extern "system" fn vtab_destroy(p_vtab: *mut sqlite3_vtab) -> c_int {
    catch_unwind(AssertUnwindSafe(|| {
        if !p_vtab.is_null() {
            unsafe {
                let vtab_ref = &*(p_vtab as *const AydbVTab);
                {
                    let mut eng = match vtab_ref.engine.write() {
                        Ok(g) => g,
                        Err(_) => return SQLITE_ERROR,
                    };
                    if let Err(e) = eng.check_write_allowed_for_conn(vtab_ref.db as usize) {
                        return io_err_to_sqlite_rc(&e);
                    }
                    if let Err(e) = eng.drop_table(vtab_ref.table_id, Some(&vtab_ref.table_name)) {
                        return io_err_to_sqlite_rc(&e);
                    }
                }
                drop(Box::from_raw(p_vtab as *mut AydbVTab));
            }
        }
        SQLITE_OK
    }))
    .unwrap_or(SQLITE_ERROR)
}

unsafe extern "system" fn vtab_best_index(
    p_vtab: *mut sqlite3_vtab,
    p_info: *mut sqlite3_index_info,
) -> c_int {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        let vtab = &*(p_vtab as *const AydbVTab);
        let info = &mut *p_info;
        let n = info.nConstraint as usize;
        let constraints = core::slice::from_raw_parts(info.aConstraint, n);
        let usages = core::slice::from_raw_parts_mut(info.aConstraintUsage, n);

        // Check if only `rowid` (`colUsed == 0`) or only a binary-collated PK column is projected
        let can_use_keys_only = info.colUsed == 0
            || (vtab.pk_collation == ColCollation::Binary
                && vtab.pk_col_idx < 63
                && info.colUsed == (1u64 << vtab.pk_col_idx));

        // 1. Check for usable equality constraint (`=`) on Primary Key (or ROWID `-1`)
        for i in 0..n {
            if constraints[i].usable == 0 {
                continue;
            }
            let col = constraints[i].iColumn;
            if constraints[i].op == SQLITE_INDEX_CONSTRAINT_EQ
                && (col == vtab.pk_col_idx as i32 || col == -1)
            {
                if col >= 0 && !constraint_matches_col_collation(p_info, i, vtab.pk_collation) {
                    continue;
                }
                usages[i].argvIndex = 1;
                usages[i].omit = 0;
                let mut pt_flags = if col == -1 { POINT_FLAG_BY_ROWID } else { 0 };
                if can_use_keys_only {
                    pt_flags |= POINT_FLAG_KEYS_ONLY;
                }
                info.idxNum = PLAN_PK_POINT | (pt_flags << 8);
                info.idxFlags = SQLITE_INDEX_SCAN_UNIQUE;
                info.estimatedCost = 1.0;
                info.estimatedRows = 1;
                return SQLITE_OK;
            }
        }

        // 2. Check for usable equality constraint (`=`) on a Secondary Index column
        for i in 0..n {
            if constraints[i].usable == 0 || constraints[i].op != SQLITE_INDEX_CONSTRAINT_EQ {
                continue;
            }
            let col = constraints[i].iColumn;
            if col >= 0 {
                if let Some(&(_, index_id)) = vtab
                    .secondary_indexes
                    .iter()
                    .find(|&&(c, _)| c == col as usize)
                {
                    if !constraint_matches_col_collation(
                        p_info,
                        i,
                        vtab.col_collations[col as usize],
                    ) {
                        continue;
                    }
                    usages[i].argvIndex = 1;
                    // Do not set `omit = 1` on secondary index lookups so SQLite verifies the
                    // exact column type and value (`NULL` vs `""` vs `X''`).
                    usages[i].omit = 0;
                    let eq_flags = if can_use_keys_only {
                        RANGE_FLAG_KEYS_ONLY
                    } else {
                        0
                    };
                    info.idxNum = PLAN_SECONDARY_EQ
                        | (((index_id as c_int) & 0xFFFF) << 8)
                        | ((eq_flags & 0x7F) << 24);
                    info.estimatedCost = 5.0;
                    info.estimatedRows = 4;
                    return SQLITE_OK;
                }
            }
        }

        // 3. Check for usable range constraints (`>`, `>=`, `<`, `<=`) on Primary Key
        let mut lower_idx: Option<(usize, c_int)> = None;
        let mut upper_idx: Option<(usize, c_int)> = None;
        for i in 0..n {
            if constraints[i].usable == 0 {
                continue;
            }
            if constraints[i].iColumn != vtab.pk_col_idx as i32 {
                continue;
            }
            if !constraint_matches_col_collation(p_info, i, vtab.pk_collation) {
                continue;
            }
            match constraints[i].op {
                SQLITE_INDEX_CONSTRAINT_GE if lower_idx.is_none() => {
                    lower_idx = Some((i, RANGE_FLAG_LOWER_INCLUSIVE));
                }
                SQLITE_INDEX_CONSTRAINT_GT if lower_idx.is_none() => {
                    lower_idx = Some((i, RANGE_FLAG_LOWER_EXCLUSIVE));
                }
                SQLITE_INDEX_CONSTRAINT_LE if upper_idx.is_none() => {
                    upper_idx = Some((i, RANGE_FLAG_UPPER_INCLUSIVE));
                }
                SQLITE_INDEX_CONSTRAINT_LT if upper_idx.is_none() => {
                    upper_idx = Some((i, RANGE_FLAG_UPPER_EXCLUSIVE));
                }
                _ => {}
            }
        }

        // Check if `ORDER BY pk ASC` or `ORDER BY pk DESC` can be satisfied directly by the memcomparable PK scan
        let mut order_by_pk_consumed = false;
        let mut order_by_pk_reverse = false;
        if info.nOrderBy == 1 {
            let ob = &*info.aOrderBy;
            // Integer affinity also accepts REAL/TEXT/BLOB keys with synthetic rowids. Another
            // connection can insert one after planning, so only PK order is guaranteed here.
            let is_pk_order = ob.iColumn == vtab.pk_col_idx as i32;
            if is_pk_order {
                order_by_pk_consumed = true;
                order_by_pk_reverse = ob.desc != 0;
            }
        }

        if lower_idx.is_some() || upper_idx.is_some() {
            let mut flags = if can_use_keys_only {
                RANGE_FLAG_KEYS_ONLY
            } else {
                0
            };
            let mut next_arg = 1;
            if let Some((i, fl)) = lower_idx {
                usages[i].argvIndex = next_arg;
                usages[i].omit = 0;
                next_arg += 1;
                flags |= fl;
            }
            if let Some((i, fl)) = upper_idx {
                usages[i].argvIndex = next_arg;
                usages[i].omit = 0;
                flags |= fl;
            }
            if order_by_pk_consumed {
                info.orderByConsumed = 1;
                if order_by_pk_reverse {
                    flags |= RANGE_FLAG_REVERSE;
                }
            }
            info.idxNum = PLAN_PK_RANGE | (flags << 8);
            info.estimatedCost = 25.0;
            info.estimatedRows = 32;
            return SQLITE_OK;
        }

        // 4. Check for usable range constraints (`>`, `>=`, `<`, `<=`) on a Secondary Index column
        for &(sec_col_idx, index_id) in &vtab.secondary_indexes {
            let mut sec_lower: Option<(usize, c_int)> = None;
            let mut sec_upper: Option<(usize, c_int)> = None;
            for i in 0..n {
                if constraints[i].usable == 0 || constraints[i].iColumn != sec_col_idx as i32 {
                    continue;
                }
                if !constraint_matches_col_collation(p_info, i, vtab.col_collations[sec_col_idx]) {
                    continue;
                }
                match constraints[i].op {
                    SQLITE_INDEX_CONSTRAINT_GE if sec_lower.is_none() => {
                        sec_lower = Some((i, RANGE_FLAG_LOWER_INCLUSIVE));
                    }
                    SQLITE_INDEX_CONSTRAINT_GT if sec_lower.is_none() => {
                        sec_lower = Some((i, RANGE_FLAG_LOWER_EXCLUSIVE));
                    }
                    SQLITE_INDEX_CONSTRAINT_LE if sec_upper.is_none() => {
                        sec_upper = Some((i, RANGE_FLAG_UPPER_INCLUSIVE));
                    }
                    SQLITE_INDEX_CONSTRAINT_LT if sec_upper.is_none() => {
                        sec_upper = Some((i, RANGE_FLAG_UPPER_EXCLUSIVE));
                    }
                    _ => {}
                }
            }
            if sec_lower.is_some() || sec_upper.is_some() {
                let mut flags = if can_use_keys_only {
                    RANGE_FLAG_KEYS_ONLY
                } else {
                    0
                };
                let mut next_arg = 1;
                if let Some((i, fl)) = sec_lower {
                    usages[i].argvIndex = next_arg;
                    usages[i].omit = 0;
                    next_arg += 1;
                    flags |= fl;
                }
                if let Some((i, fl)) = sec_upper {
                    usages[i].argvIndex = next_arg;
                    usages[i].omit = 0;
                    flags |= fl;
                }
                info.idxNum = PLAN_SECONDARY_RANGE
                    | (((index_id as c_int) & 0xFFFF) << 8)
                    | ((flags & 0x7F) << 24);
                info.estimatedCost = 40.0;
                info.estimatedRows = 32;
                return SQLITE_OK;
            }
        }

        // 5. Fallback: Full Table Scan (let SQLite's VDBE enforce LIMIT/OFFSET after any
        //    residual WHERE predicates like `LIKE`, `OR`, or expressions not passed in `aConstraint`)
        let mut full_scan_flags = if can_use_keys_only {
            RANGE_FLAG_KEYS_ONLY
        } else {
            0
        };
        if order_by_pk_consumed {
            info.orderByConsumed = 1;
            if order_by_pk_reverse {
                full_scan_flags |= RANGE_FLAG_REVERSE;
            }
        }
        info.idxNum = PLAN_FULL_SCAN | (full_scan_flags << 8);
        info.estimatedCost = 10_000.0;
        info.estimatedRows = 1_000;
        SQLITE_OK
    }))
    .unwrap_or(SQLITE_ERROR)
}

unsafe extern "system" fn vtab_open(
    p_vtab: *mut sqlite3_vtab,
    pp_cursor: *mut *mut sqlite3_vtab_cursor,
) -> c_int {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        let cursor = Box::new(AydbCursor {
            base: sqlite3_vtab_cursor { pVtab: p_vtab },
            snapshot: None,
            slab: Vec::with_capacity(512),
            spans: Vec::with_capacity(16),
            pks: Vec::new(),
            lazy_blobs: Vec::with_capacity(16),
            keys_only: false,
            pos: 0,
            prefetch_end: 0,
            streaming_active: false,
            stream_lower: None,
            stream_upper: None,
            stream_reverse: false,
            stream_last_pk: None,
            stream_sec_index_id: None,
            stream_sec_prefix: Vec::new(),
        });
        *pp_cursor = Box::into_raw(cursor) as *mut sqlite3_vtab_cursor;
        SQLITE_OK
    }))
    .unwrap_or(SQLITE_ERROR)
}

unsafe extern "system" fn vtab_close(p_cursor: *mut sqlite3_vtab_cursor) -> c_int {
    catch_unwind(AssertUnwindSafe(|| {
        if !p_cursor.is_null() {
            unsafe {
                let cursor = Box::from_raw(p_cursor as *mut AydbCursor);
                let p_vtab = cursor.base.pVtab;
                drop(cursor);
                if !p_vtab.is_null() {
                    let vtab = &*(p_vtab as *const AydbVTab);
                    if let Ok(engine) = vtab.engine.try_read() {
                        engine.cleanup_read_snapshots();
                    }
                }
            }
        }
        SQLITE_OK
    }))
    .unwrap_or(SQLITE_ERROR)
}

unsafe extern "system" fn vtab_filter(
    p_cursor: *mut sqlite3_vtab_cursor,
    idx_num: c_int,
    _idx_str: *const c_char,
    argc: c_int,
    argv: *mut *mut sqlite3_value,
) -> c_int {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        let cursor = &mut *(p_cursor as *mut AydbCursor);
        let vtab = &*(cursor.base.pVtab as *const AydbVTab);
        cursor.slab.clear();
        cursor.spans.clear();
        cursor.pks.clear();
        cursor.lazy_blobs.clear();
        cursor.keys_only = false;
        cursor.pos = 0;
        cursor.prefetch_end = 0;
        cursor.streaming_active = false;
        cursor.stream_lower = None;
        cursor.stream_upper = None;
        cursor.stream_reverse = false;
        cursor.stream_last_pk = None;
        cursor.stream_sec_index_id = None;
        cursor.stream_sec_prefix.clear();

        let conn_id = vtab.db as usize;
        if let Err(rc) = ensure_rowids_before_scan(vtab) {
            return rc;
        }
        cursor.snapshot = match vtab.read_session.for_cursor(conn_id) {
            Ok(snapshot) => snapshot,
            Err(error) => return io_err_to_sqlite_rc(&error),
        };

        with_snapshot_read_access(
            &vtab.engine,
            conn_id,
            vtab.table_id,
            cursor.snapshot.as_ref(),
            |eng| {
                let base_plan = idx_num & 0xFF;
                match base_plan {
                    PLAN_PK_POINT => {
                        if argc < 1 {
                            return SQLITE_ERROR;
                        }
                        let val_ptr = *argv.offset(0);
                        if sqlite3_value_type(val_ptr) == SQLITE_NULL {
                            return SQLITE_OK;
                        }
                        let pt_flags = (idx_num >> 8) & 0xFF;
                        let pt_keys_only = (pt_flags & POINT_FLAG_KEYS_ONLY) != 0;
                        cursor.keys_only = pt_keys_only;
                        if (pt_flags & POINT_FLAG_BY_ROWID) != 0 {
                            if let Some(rid) = extract_lossless_rowid_arg(val_ptr) {
                                match resolve_pk_for_rowid(
                                    vtab,
                                    rid,
                                    |t, pk| eng.row_exists(t, pk),
                                    |t, rid| eng.pk_for_rowid(t, rid),
                                ) {
                                    Ok(Some(mapped_pk)) => {
                                        match point_fetch(
                                            eng,
                                            vtab.table_id,
                                            &mapped_pk,
                                            pt_keys_only,
                                            &mut cursor.slab,
                                            &mut cursor.spans,
                                        ) {
                                            Ok(true) => {
                                                cursor.pks.push(mapped_pk);
                                                cursor.lazy_blobs.push(None);
                                            }
                                            Ok(false) => {}
                                            Err(e) => return io_err_to_sqlite_rc(&e),
                                        }
                                    }
                                    Ok(None) => {}
                                    Err(e) => return io_err_to_sqlite_rc(&e),
                                }
                            }
                            return SQLITE_OK;
                        } else {
                            let pk_bytes = sqlite_value_to_key_bytes_with_schema(
                                val_ptr,
                                vtab.pk_affinity,
                                vtab.pk_collation,
                                false,
                            );
                            let found = match point_fetch(
                                eng,
                                vtab.table_id,
                                &pk_bytes,
                                pt_keys_only,
                                &mut cursor.slab,
                                &mut cursor.spans,
                            ) {
                                Ok(b) => b,
                                Err(e) => return io_err_to_sqlite_rc(&e),
                            };
                            if found {
                                cursor.pks.push(pk_bytes);
                                cursor.lazy_blobs.push(None);
                            } else if !matches!(
                                vtab.pk_affinity,
                                ColumnAffinity::Integer | ColumnAffinity::Numeric
                            ) {
                                if let Some(alt) = build_numeric_alt_key(&pk_bytes) {
                                    match point_fetch(
                                        eng,
                                        vtab.table_id,
                                        &alt,
                                        pt_keys_only,
                                        &mut cursor.slab,
                                        &mut cursor.spans,
                                    ) {
                                        Ok(true) => {
                                            cursor.pks.push(alt);
                                            cursor.lazy_blobs.push(None);
                                        }
                                        Ok(false) => {}
                                        Err(e) => return io_err_to_sqlite_rc(&e),
                                    }
                                }
                            }
                        }
                    }
                    PLAN_SECONDARY_EQ => {
                        if argc < 1 {
                            return SQLITE_ERROR;
                        }
                        let val_ptr = *argv.offset(0);
                        if sqlite3_value_type(val_ptr) == SQLITE_NULL {
                            return SQLITE_OK;
                        }
                        let index_id = ((idx_num >> 8) & 0xFFFF) as u16;
                        let flags = (idx_num >> 24) & 0x7F;
                        let keys_only = (flags & RANGE_FLAG_KEYS_ONLY) != 0;
                        cursor.keys_only = keys_only;
                        let sec_col_idx = vtab
                            .secondary_indexes
                            .iter()
                            .find(|&&(_, id)| id == index_id)
                            .map(|&(c, _)| c)
                            .unwrap_or(0);
                        let sec_aff = vtab
                            .col_affinities
                            .get(sec_col_idx)
                            .copied()
                            .unwrap_or(ColumnAffinity::Blob);
                        let sec_coll = vtab
                            .col_collations
                            .get(sec_col_idx)
                            .copied()
                            .unwrap_or(ColCollation::Binary);
                        let col_val =
                            sqlite_value_to_key_bytes_with_schema(val_ptr, sec_aff, sec_coll, true);
                        let prefix = crate::lru::encode_secondary_index_exact_prefix(
                            vtab.table_id,
                            index_id,
                            &col_val,
                        );
                        let next_last_sec = match eng.scan_secondary_index_into_slab(
                            vtab.table_id,
                            index_id,
                            &prefix,
                            (&prefix, true),
                            None,
                            CURSOR_STREAM_CHUNK_ROWS,
                            keys_only,
                            &mut cursor.pks,
                            &mut cursor.slab,
                            &mut cursor.spans,
                            Some(&mut cursor.lazy_blobs),
                        ) {
                            Ok(last_opt) => last_opt,
                            Err(e) => return io_err_to_sqlite_rc(&e),
                        };
                        if let Some(last_sec_key) = next_last_sec {
                            cursor.streaming_active = true;
                            cursor.stream_sec_index_id = Some(index_id);
                            cursor.stream_sec_prefix = prefix;
                            cursor.stream_upper = None;
                            cursor.stream_last_pk = Some(last_sec_key);
                        }
                    }
                    PLAN_SECONDARY_RANGE => {
                        let index_id = ((idx_num >> 8) & 0xFFFF) as u16;
                        let flags = (idx_num >> 24) & 0x7F;
                        let sec_col_idx = vtab
                            .secondary_indexes
                            .iter()
                            .find(|&&(_, id)| id == index_id)
                            .map(|&(c, _)| c)
                            .unwrap_or(0);
                        let sec_aff = vtab
                            .col_affinities
                            .get(sec_col_idx)
                            .copied()
                            .unwrap_or(ColumnAffinity::Blob);
                        let sec_coll = vtab
                            .col_collations
                            .get(sec_col_idx)
                            .copied()
                            .unwrap_or(ColCollation::Binary);

                        let mut arg_pos = 0;
                        let lower_bound = if (flags
                            & (RANGE_FLAG_LOWER_INCLUSIVE | RANGE_FLAG_LOWER_EXCLUSIVE))
                            != 0
                            && arg_pos < argc
                        {
                            let inc = (flags & RANGE_FLAG_LOWER_INCLUSIVE) != 0;
                            let val_ptr = *argv.offset(arg_pos as isize);
                            arg_pos += 1;
                            match sec_range_bound_to_key(
                                val_ptr,
                                vtab.table_id,
                                index_id,
                                sec_aff,
                                sec_coll,
                                true,
                                inc,
                            ) {
                                Ok(bound) => bound,
                                Err(()) => return SQLITE_OK,
                            }
                        } else {
                            let mut min_non_null = Vec::with_capacity(6);
                            min_non_null.extend_from_slice(
                                &crate::lru::encode_secondary_index_prefix(vtab.table_id, index_id),
                            );
                            min_non_null.push(KEY_TAG_NULL + 1);
                            Some((min_non_null, true))
                        };
                        let upper_bound = if (flags
                            & (RANGE_FLAG_UPPER_INCLUSIVE | RANGE_FLAG_UPPER_EXCLUSIVE))
                            != 0
                            && arg_pos < argc
                        {
                            let inc = (flags & RANGE_FLAG_UPPER_INCLUSIVE) != 0;
                            let val_ptr = *argv.offset(arg_pos as isize);
                            match sec_range_bound_to_key(
                                val_ptr,
                                vtab.table_id,
                                index_id,
                                sec_aff,
                                sec_coll,
                                false,
                                inc,
                            ) {
                                Ok(bound) => bound,
                                Err(()) => return SQLITE_OK,
                            }
                        } else {
                            None
                        };

                        let keys_only = (flags & RANGE_FLAG_KEYS_ONLY) != 0;
                        cursor.keys_only = keys_only;
                        let idx_prefix =
                            crate::lru::encode_secondary_index_prefix(vtab.table_id, index_id);
                        let (lo_slice, lo_inc) = lower_bound
                            .as_ref()
                            .map(|(b, inc)| (b.as_slice(), *inc))
                            .unwrap_or((&idx_prefix, true));
                        let next_last_sec = match eng.scan_secondary_index_into_slab(
                            vtab.table_id,
                            index_id,
                            &idx_prefix,
                            (lo_slice, lo_inc),
                            upper_bound.as_ref().map(|(b, inc)| (b.as_slice(), *inc)),
                            CURSOR_STREAM_CHUNK_ROWS,
                            keys_only,
                            &mut cursor.pks,
                            &mut cursor.slab,
                            &mut cursor.spans,
                            Some(&mut cursor.lazy_blobs),
                        ) {
                            Ok(last_opt) => last_opt,
                            Err(e) => return io_err_to_sqlite_rc(&e),
                        };
                        if let Some(last_sec_key) = next_last_sec {
                            cursor.streaming_active = true;
                            cursor.stream_sec_index_id = Some(index_id);
                            cursor.stream_sec_prefix.extend_from_slice(&idx_prefix);
                            cursor.stream_upper = upper_bound;
                            cursor.stream_last_pk = Some(last_sec_key);
                        }
                    }
                    PLAN_PK_RANGE => {
                        let flags = idx_num >> 8;
                        let mut arg_pos = 0;
                        let lower_bound = if (flags
                            & (RANGE_FLAG_LOWER_INCLUSIVE | RANGE_FLAG_LOWER_EXCLUSIVE))
                            != 0
                            && arg_pos < argc
                        {
                            let inc = (flags & RANGE_FLAG_LOWER_INCLUSIVE) != 0;
                            let val_ptr = *argv.offset(arg_pos as isize);
                            arg_pos += 1;
                            match pk_range_bound_to_key(
                                val_ptr,
                                vtab.pk_affinity,
                                vtab.pk_collation,
                                true,
                                inc,
                            ) {
                                Ok(bound) => bound,
                                Err(()) => return SQLITE_OK,
                            }
                        } else {
                            None
                        };
                        let upper_bound = if (flags
                            & (RANGE_FLAG_UPPER_INCLUSIVE | RANGE_FLAG_UPPER_EXCLUSIVE))
                            != 0
                            && arg_pos < argc
                        {
                            let inc = (flags & RANGE_FLAG_UPPER_INCLUSIVE) != 0;
                            let val_ptr = *argv.offset(arg_pos as isize);
                            match pk_range_bound_to_key(
                                val_ptr,
                                vtab.pk_affinity,
                                vtab.pk_collation,
                                false,
                                inc,
                            ) {
                                Ok(bound) => bound,
                                Err(()) => return SQLITE_OK,
                            }
                        } else {
                            None
                        };
                        let keys_only = (flags & RANGE_FLAG_KEYS_ONLY) != 0;
                        let reverse = (flags & RANGE_FLAG_REVERSE) != 0;
                        cursor.keys_only = keys_only;
                        if let Err(e) = eng.scan_range_into_slab(
                            vtab.table_id,
                            lower_bound.as_ref().map(|(b, inc)| (b.as_slice(), *inc)),
                            upper_bound.as_ref().map(|(b, inc)| (b.as_slice(), *inc)),
                            reverse,
                            CURSOR_STREAM_CHUNK_ROWS,
                            keys_only,
                            &mut cursor.pks,
                            &mut cursor.slab,
                            &mut cursor.spans,
                            Some(&mut cursor.lazy_blobs),
                        ) {
                            return io_err_to_sqlite_rc(&e);
                        }
                        let fetched_rows = if keys_only {
                            cursor.pks.len()
                        } else {
                            cursor.spans.len()
                        };
                        if fetched_rows >= CURSOR_STREAM_CHUNK_ROWS {
                            if let Some(last_pk) = cursor.pks.last().cloned() {
                                cursor.streaming_active = true;
                                cursor.stream_lower = lower_bound;
                                cursor.stream_upper = upper_bound;
                                cursor.stream_reverse = reverse;
                                cursor.stream_last_pk = Some(last_pk);
                            }
                        }
                    }
                    _ => {
                        let flags = idx_num >> 8;
                        let keys_only = (flags & RANGE_FLAG_KEYS_ONLY) != 0;
                        let reverse = (flags & RANGE_FLAG_REVERSE) != 0;
                        cursor.keys_only = keys_only;
                        if let Err(e) = eng.scan_range_into_slab(
                            vtab.table_id,
                            None,
                            None,
                            reverse,
                            CURSOR_STREAM_CHUNK_ROWS,
                            keys_only,
                            &mut cursor.pks,
                            &mut cursor.slab,
                            &mut cursor.spans,
                            Some(&mut cursor.lazy_blobs),
                        ) {
                            return io_err_to_sqlite_rc(&e);
                        }
                        let fetched_rows = if keys_only {
                            cursor.pks.len()
                        } else {
                            cursor.spans.len()
                        };
                        if fetched_rows >= CURSOR_STREAM_CHUNK_ROWS {
                            if let Some(last_pk) = cursor.pks.last().cloned() {
                                cursor.streaming_active = true;
                                cursor.stream_lower = None;
                                cursor.stream_upper = None;
                                cursor.stream_reverse = reverse;
                                cursor.stream_last_pk = Some(last_pk);
                            }
                        }
                    }
                }

                SQLITE_OK
            },
        )
        .unwrap_or(SQLITE_ERROR)
    }))
    .unwrap_or(SQLITE_ERROR)
}

unsafe extern "system" fn vtab_next(p_cursor: *mut sqlite3_vtab_cursor) -> c_int {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        let cursor = &mut *(p_cursor as *mut AydbCursor);
        cursor.pos += 1;
        let len = if cursor.keys_only {
            cursor.pks.len()
        } else {
            cursor.spans.len()
        };
        if cursor.pos >= len && cursor.streaming_active {
            let Some(last_pk) = cursor.stream_last_pk.take() else {
                cursor.streaming_active = false;
                return SQLITE_OK;
            };
            let vtab = &*(cursor.base.pVtab as *const AydbVTab);
            cursor.slab.clear();
            cursor.spans.clear();
            cursor.pks.clear();
            cursor.lazy_blobs.clear();
            cursor.pos = 0;
            cursor.prefetch_end = 0;

            let conn_id = vtab.db as usize;
            let keys_only = cursor.keys_only;

            if let Some(sec_index_id) = cursor.stream_sec_index_id {
                let upper_ref = cursor
                    .stream_upper
                    .as_ref()
                    .map(|(b, inc)| (b.as_slice(), *inc));
                let mut next_last_sec: Option<Vec<u8>> = None;
                let rc = with_snapshot_read_access(
                    &vtab.engine,
                    conn_id,
                    vtab.table_id,
                    cursor.snapshot.as_ref(),
                    |eng| match eng.scan_secondary_index_into_slab(
                        vtab.table_id,
                        sec_index_id,
                        &cursor.stream_sec_prefix,
                        (last_pk.as_slice(), false),
                        upper_ref,
                        CURSOR_STREAM_CHUNK_ROWS,
                        keys_only,
                        &mut cursor.pks,
                        &mut cursor.slab,
                        &mut cursor.spans,
                        Some(&mut cursor.lazy_blobs),
                    ) {
                        Ok(last_opt) => {
                            next_last_sec = last_opt;
                            SQLITE_OK
                        }
                        Err(e) => io_err_to_sqlite_rc(&e),
                    },
                )
                .unwrap_or(SQLITE_ERROR);
                if rc != SQLITE_OK {
                    cursor.streaming_active = false;
                    return rc;
                }
                if let Some(next_key) = next_last_sec {
                    cursor.stream_last_pk = Some(next_key);
                } else {
                    cursor.streaming_active = false;
                }
                return SQLITE_OK;
            }

            let reverse = cursor.stream_reverse;
            let lower_ref = if reverse {
                cursor
                    .stream_lower
                    .as_ref()
                    .map(|(b, inc)| (b.as_slice(), *inc))
            } else {
                Some((last_pk.as_slice(), false))
            };
            let upper_ref = if reverse {
                Some((last_pk.as_slice(), false))
            } else {
                cursor
                    .stream_upper
                    .as_ref()
                    .map(|(b, inc)| (b.as_slice(), *inc))
            };

            let rc = with_snapshot_read_access(
                &vtab.engine,
                conn_id,
                vtab.table_id,
                cursor.snapshot.as_ref(),
                |eng| {
                    if let Err(e) = eng.scan_range_into_slab(
                        vtab.table_id,
                        lower_ref,
                        upper_ref,
                        reverse,
                        CURSOR_STREAM_CHUNK_ROWS,
                        keys_only,
                        &mut cursor.pks,
                        &mut cursor.slab,
                        &mut cursor.spans,
                        Some(&mut cursor.lazy_blobs),
                    ) {
                        return io_err_to_sqlite_rc(&e);
                    }
                    SQLITE_OK
                },
            )
            .unwrap_or(SQLITE_ERROR);
            if rc != SQLITE_OK {
                cursor.streaming_active = false;
                return rc;
            }
            let fetched_rows = if keys_only {
                cursor.pks.len()
            } else {
                cursor.spans.len()
            };
            if fetched_rows >= CURSOR_STREAM_CHUNK_ROWS {
                if let Some(next_last_pk) = cursor.pks.last().cloned() {
                    cursor.stream_last_pk = Some(next_last_pk);
                } else {
                    cursor.streaming_active = false;
                }
            } else {
                cursor.streaming_active = false;
            }
        }
        SQLITE_OK
    }))
    .unwrap_or(SQLITE_ERROR)
}

unsafe extern "system" fn vtab_eof(p_cursor: *mut sqlite3_vtab_cursor) -> c_int {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        let cursor = &*(p_cursor as *const AydbCursor);
        let len = if cursor.keys_only {
            cursor.pks.len()
        } else {
            cursor.spans.len()
        };
        if cursor.pos >= len { 1 } else { 0 }
    }))
    .unwrap_or(1)
}

unsafe extern "system" fn vtab_column(
    p_cursor: *mut sqlite3_vtab_cursor,
    p_ctx: *mut sqlite3_context,
    i_col: c_int,
) -> c_int {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        let cursor = &mut *(p_cursor as *mut AydbCursor);
        let vtab = &*(cursor.base.pVtab as *const AydbVTab);
        if cursor.keys_only {
            if cursor.pos >= cursor.pks.len() {
                return SQLITE_ERROR;
            }
            let pk_bytes = &cursor.pks[cursor.pos];
            if i_col < 0 {
                let rid =
                    match extract_rowid_from_pk_bytes(pk_bytes, vtab, cursor.snapshot.as_ref()) {
                        Ok(rid) => rid,
                        Err(e) => return io_err_to_sqlite_rc(&e),
                    };
                sqlite3_result_int64(p_ctx, rid);
                return SQLITE_OK;
            }
            if i_col as usize == vtab.pk_col_idx {
                if let Some(val) = SqlValue::from_key_bytes(pk_bytes) {
                    val.set_sqlite_result(p_ctx);
                    return SQLITE_OK;
                }
            }
            return SQLITE_ERROR;
        }
        if cursor.pos >= cursor.spans.len() {
            return SQLITE_ERROR;
        }

        // Fast path 1: Rowid lookup via xColumn(p_cursor, p_ctx, -1)
        if i_col < 0 {
            let rid = if let Some(pk_bytes) = cursor.pks.get(cursor.pos) {
                extract_rowid_from_pk_bytes(pk_bytes, vtab, cursor.snapshot.as_ref())
            } else {
                let (start, end) = cursor.spans[cursor.pos];
                extract_rowid_from_row(&cursor.slab[start..end], vtab, cursor.snapshot.as_ref())
            };
            let rid = match rid {
                Ok(rid) => rid,
                Err(e) => return io_err_to_sqlite_rc(&e),
            };
            sqlite3_result_int64(p_ctx, rid);
            return SQLITE_OK;
        }

        // Fast path 2: Primary Key column projection with Binary collation directly from cursor.pks
        if i_col as usize == vtab.pk_col_idx && vtab.pk_collation == ColCollation::Binary {
            if let Some(pk_bytes) = cursor.pks.get(cursor.pos) {
                if let Some(val) = SqlValue::from_key_bytes(pk_bytes) {
                    val.set_sqlite_result(p_ctx);
                    return SQLITE_OK;
                }
            }
        }

        // Lazy Blob Dereference: If this row has a pending external blob that hasn't been read into slab yet
        if let Some(Some(lazy_blob)) = cursor.lazy_blobs.get(cursor.pos).cloned() {
            let Some(pk_bytes) = cursor.pks.get(cursor.pos) else {
                return SQLITE_ERROR;
            };
            let conn_id = vtab.db as usize;

            // Prefetch the next window of up to 16 rows only once the previous window is used up,
            // so the cache is not re-checked for the same blobs on every row.
            let mut upcoming: Vec<(&[u8], &LazyBlob<OsFile>)> = Vec::new();
            if cursor.pos >= cursor.prefetch_end {
                let end = (cursor.pos + 16).min(cursor.lazy_blobs.len());
                for i in cursor.pos..end {
                    if let Some(Some(lb)) = cursor.lazy_blobs.get(i) {
                        if let Some(pk) = cursor.pks.get(i) {
                            upcoming.push((pk.as_slice(), lb));
                        }
                    }
                }
                cursor.prefetch_end = end;
            }

            let Some(deref_res) = with_snapshot_read_access(
                &vtab.engine,
                conn_id,
                vtab.table_id,
                cursor.snapshot.as_ref(),
                |eng| {
                    if upcoming.len() > 1 {
                        let _ = eng.prefetch_lazy_blobs(vtab.table_id, &upcoming);
                    }
                    eng.dereference_lazy_blob_into(
                        vtab.table_id,
                        pk_bytes,
                        &lazy_blob,
                        &mut cursor.slab,
                    )
                },
            ) else {
                return SQLITE_ERROR;
            };
            let span = match deref_res {
                Ok(Some(s)) => s,
                Ok(None) => {
                    sqlite3_result_error_code(p_ctx, SQLITE_CORRUPT);
                    return SQLITE_CORRUPT;
                }
                Err(e) => {
                    let rc = io_err_to_sqlite_rc(&e);
                    sqlite3_result_error_code(p_ctx, rc);
                    return rc;
                }
            };
            cursor.spans[cursor.pos] = span;
            cursor.lazy_blobs[cursor.pos] = None;
        }

        let (start, end) = cursor.spans[cursor.pos];
        let row_bytes = &cursor.slab[start..end];
        match project_column_zero_copy(row_bytes, i_col as usize, p_ctx) {
            Ok(()) => SQLITE_OK,
            Err(_) => SQLITE_ERROR,
        }
    }))
    .unwrap_or(SQLITE_ERROR)
}

unsafe extern "system" fn vtab_rowid(
    p_cursor: *mut sqlite3_vtab_cursor,
    p_rowid: *mut sqlite3_int64,
) -> c_int {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        let cursor = &*(p_cursor as *const AydbCursor);
        let vtab = &*(cursor.base.pVtab as *const AydbVTab);
        if cursor.keys_only {
            if cursor.pos >= cursor.pks.len() {
                return SQLITE_ERROR;
            }
            *p_rowid = match extract_rowid_from_pk_bytes(
                &cursor.pks[cursor.pos],
                vtab,
                cursor.snapshot.as_ref(),
            ) {
                Ok(rid) => rid,
                Err(e) => return io_err_to_sqlite_rc(&e),
            };
            return SQLITE_OK;
        }
        if cursor.pos >= cursor.spans.len() {
            return SQLITE_ERROR;
        }
        if let Some(pk_bytes) = cursor.pks.get(cursor.pos) {
            *p_rowid = match extract_rowid_from_pk_bytes(pk_bytes, vtab, cursor.snapshot.as_ref()) {
                Ok(rid) => rid,
                Err(e) => return io_err_to_sqlite_rc(&e),
            };
            return SQLITE_OK;
        }
        let (start, end) = cursor.spans[cursor.pos];
        let row_bytes = &cursor.slab[start..end];
        *p_rowid = match extract_rowid_from_row(row_bytes, vtab, cursor.snapshot.as_ref()) {
            Ok(rid) => rid,
            Err(e) => return io_err_to_sqlite_rc(&e),
        };
        SQLITE_OK
    }))
    .unwrap_or(SQLITE_ERROR)
}

/// Savepoint id of the engine savepoint that makes one `xUpdate` call atomic. SQLite numbers its
/// own savepoints from zero, so a negative id never collides with one of them.
const STATEMENT_SAVEPOINT: c_int = -1;

unsafe extern "system" fn vtab_update(
    p_vtab: *mut sqlite3_vtab,
    argc: c_int,
    argv: *mut *mut sqlite3_value,
    p_rowid: *mut sqlite3_int64,
) -> c_int {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        let vtab = &*(p_vtab as *const AydbVTab);
        let mut eng = match vtab.engine.write() {
            Ok(g) => g,
            Err(_) => return SQLITE_ERROR,
        };
        if let Err(e) = eng.check_write_allowed_for_conn(vtab.db as usize) {
            return io_err_to_sqlite_rc(&e);
        }
        if let Err(e) = ensure_rowids_for_conn(&mut eng, vtab) {
            return io_err_to_sqlite_rc(&e);
        }

        // Case 1: DELETE (`argc == 1`)
        if argc == 1 {
            let arg0 = *argv.offset(0);
            if let Some(rid) = extract_lossless_rowid_arg(arg0) {
                let old_pk_opt = match resolve_pk_for_rowid(
                    vtab,
                    rid,
                    |t, pk| eng.row_exists(t, pk),
                    |t, rid| eng.pk_for_rowid_view(crate::engine::ReadView::Owner, t, rid),
                ) {
                    Ok(opt) => opt,
                    Err(e) => return io_err_to_sqlite_rc(&e),
                };
                if let Some(old_pk) = old_pk_opt {
                    if let Err(e) = eng.delete_row(vtab.table_id, &old_pk, vtab.durability) {
                        return io_err_to_sqlite_rc(&e);
                    }
                }
                return SQLITE_OK;
            } else {
                let old_pk = sqlite_value_to_key_bytes_with_schema(
                    arg0,
                    vtab.pk_affinity,
                    vtab.pk_collation,
                    false,
                );
                if let Err(e) = eng.delete_row(vtab.table_id, &old_pk, vtab.durability) {
                    return io_err_to_sqlite_rc(&e);
                }
            }
            return SQLITE_OK;
        }

        // Case 2: INSERT or UPDATE (`argc == 2 + num_cols`)
        let num_cols = vtab.col_names.len();
        if (argc as usize) < 2 + num_cols {
            return SQLITE_ERROR;
        }

        let arg0 = *argv.offset(0);
        let is_insert = sqlite3_value_type(arg0) == SQLITE_NULL;
        for (idx, &not_null) in vtab.col_not_null.iter().enumerate() {
            if not_null
                && !(idx == vtab.pk_col_idx
                    && is_insert
                    && vtab.pk_affinity == ColumnAffinity::Integer)
                && sqlite3_value_type(*argv.offset(2 + idx as isize)) == SQLITE_NULL
            {
                match sqlite3_vtab_on_conflict(vtab.db) {
                    SQLITE_IGNORE => return SQLITE_OK,
                    _ => return SQLITE_CONSTRAINT,
                }
            }
        }
        let pk_val_ptr = *argv.offset(2 + vtab.pk_col_idx as isize);
        let pk_val_type = sqlite3_value_type(pk_val_ptr);

        // Handle `SQLITE_NULL` primary key
        let pk_override = if pk_val_type == SQLITE_NULL {
            if is_insert && vtab.pk_affinity == ColumnAffinity::Integer {
                let mut candidate = vtab.next_auto_rowid.load(Ordering::Relaxed);
                if candidate <= 0 {
                    candidate = 1;
                }
                let mut found_free = None;
                // 1. Bounded sequential probe using checked arithmetic
                for _ in 0..1024 {
                    match integer_rowid_occupied(&eng, vtab.table_id, candidate) {
                        Ok(false) => {
                            found_free = Some(candidate);
                            break;
                        }
                        Ok(true) => match candidate.checked_add(1) {
                            Some(next) => candidate = next,
                            None => break,
                        },
                        Err(e) => return io_err_to_sqlite_rc(&e),
                    }
                }
                // 2. If `i64::MAX` was reached (or 1024 consecutive IDs were occupied),
                //    probe up to 64 deterministic pseudo-random positive `i64` candidates (`1..=i64::MAX`),
                //    matching SQLite's fallback rowid generator, and return `SQLITE_FULL` if exhausted.
                if found_free.is_none() {
                    let mut state =
                        (eng.next_lsn() ^ 0x9E37_79B9_7F4A_7C15).wrapping_add(vtab.table_id as u64);
                    for attempt in 0..64u64 {
                        state = state
                            .wrapping_add(0x9E37_79B9_7F4A_7C15)
                            .wrapping_add(attempt);
                        let mut z = state;
                        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
                        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
                        z ^= z >> 31;
                        let rand_pos = ((z & 0x7FFF_FFFF_FFFF_FFFF) as i64).max(1);
                        match integer_rowid_occupied(&eng, vtab.table_id, rand_pos) {
                            Ok(false) => {
                                found_free = Some(rand_pos);
                                break;
                            }
                            Ok(true) => {}
                            Err(e) => return io_err_to_sqlite_rc(&e),
                        }
                    }
                }
                let Some(chosen) = found_free else {
                    return SQLITE_FULL;
                };
                if let Some(next) = chosen.checked_add(1) {
                    vtab.next_auto_rowid.fetch_max(next, Ordering::Relaxed);
                }
                Some(chosen)
            } else {
                return SQLITE_CONSTRAINT;
            }
        } else {
            None
        };

        let (new_pk, row_bytes, sec_keys) = encode_sqlite_values_direct(
            argv.offset(2),
            num_cols,
            vtab.pk_col_idx,
            pk_override,
            &vtab.col_affinities,
            &vtab.col_collations,
            &vtab.secondary_indexes,
        );

        let coerced_int_pk = if vtab.pk_affinity == ColumnAffinity::Integer {
            decode_i64_tagged_key(&new_pk)
        } else {
            None
        };
        if pk_override.is_none() {
            if let Some(explicit_id) = coerced_int_pk {
                if explicit_id >= 0 {
                    if let Some(next) = explicit_id.checked_add(1) {
                        vtab.next_auto_rowid.fetch_max(next, Ordering::Relaxed);
                    } else {
                        vtab.next_auto_rowid.fetch_max(i64::MAX, Ordering::Relaxed);
                    }
                }
            }
        }

        // Resolve `old_pk` if this is an UPDATE (`!is_insert`)
        let mut old_pk_opt: Option<Vec<u8>> = None;
        if !is_insert {
            if let Some(rid) = extract_lossless_rowid_arg(arg0) {
                let mapped = match resolve_pk_for_rowid(
                    vtab,
                    rid,
                    |t, pk| eng.row_exists(t, pk),
                    |t, rid| eng.pk_for_rowid_view(crate::engine::ReadView::Owner, t, rid),
                ) {
                    Ok(opt) => opt,
                    Err(e) => return io_err_to_sqlite_rc(&e),
                };
                // SQLite collects the rows of a multi-row UPDATE before it applies them, so
                // an earlier row's REPLACE resolution can delete a row that is still queued.
                // A native table skips such a row, and so does this one. The rowid has no
                // row and no identity mapping left, so there is nothing to update; inserting
                // here would resurrect the replaced row.
                let Some(old_pk) = mapped else {
                    return SQLITE_OK;
                };
                old_pk_opt = Some(old_pk);
            } else {
                old_pk_opt = Some(sqlite_value_to_key_bytes_with_schema(
                    arg0,
                    vtab.pk_affinity,
                    vtab.pk_collation,
                    false,
                ));
            }
        }

        // Integer PK values use their exact i64 as rowid. A previously allocated synthetic
        // identity owns that number until its row is deleted, just like an explicit rowid.
        let rowid_conflict_pk = if let Some(rowid) = coerced_int_pk {
            match eng.pk_for_rowid_view(crate::engine::ReadView::Owner, vtab.table_id, rowid) {
                Ok(Some(pk)) if old_pk_opt.as_ref().is_none_or(|old| *old != pk) => Some(pk),
                Ok(_) => None,
                Err(e) => return io_err_to_sqlite_rc(&e),
            }
        } else {
            None
        };
        if rowid_conflict_pk.is_some() {
            match sqlite3_vtab_on_conflict(vtab.db) {
                SQLITE_IGNORE => return SQLITE_OK,
                SQLITE_REPLACE => {}
                _ => return SQLITE_CONSTRAINT,
            }
        }

        // Enforce primary key uniqueness & SQLite `ON CONFLICT` resolution
        let same_pk = match &old_pk_opt {
            Some(old_pk) => {
                old_pk.as_slice() == new_pk.as_slice()
                    || build_numeric_alt_key(old_pk.as_slice()).as_deref()
                        == Some(new_pk.as_slice())
            }
            None => false,
        };
        let mut known_absent = pk_override.is_some();
        let mut pk_conflict_to_delete: Option<Vec<u8>> = None;
        let unique_conflicts = match constraints::find_unique_conflicts(
            &mut eng,
            vtab,
            &row_bytes,
            old_pk_opt.as_deref(),
        ) {
            Ok(conflicts) => conflicts,
            Err(e) => return io_err_to_sqlite_rc(&e),
        };
        if !unique_conflicts.is_empty() {
            match sqlite3_vtab_on_conflict(vtab.db) {
                SQLITE_IGNORE => return SQLITE_OK,
                SQLITE_REPLACE => {}
                _ => return SQLITE_CONSTRAINT,
            }
        }
        if (is_insert || !same_pk) && pk_override.is_none() {
            let existing_conflict = match find_existing_pk_or_numeric_alt(
                &mut eng,
                vtab.table_id,
                &new_pk,
                vtab.pk_affinity,
            ) {
                Ok(opt) => opt,
                Err(e) => return io_err_to_sqlite_rc(&e),
            };
            if let Some(conflict_pk) = existing_conflict {
                let conflict_mode = sqlite3_vtab_on_conflict(vtab.db);
                if conflict_mode == SQLITE_IGNORE {
                    return SQLITE_OK;
                } else if conflict_mode == SQLITE_REPLACE {
                    if conflict_pk.as_slice() != new_pk.as_slice() {
                        pk_conflict_to_delete = Some(conflict_pk);
                        known_absent = true;
                    }
                } else {
                    return SQLITE_CONSTRAINT;
                }
            } else {
                known_absent = true;
            }
        }

        // Everything below mutates the engine. A statement that removes rows (REPLACE conflict
        // resolution, or a PK-changing UPDATE) and then fails to store the new row must leave
        // every row intact. SQLite cannot be relied on for that: it opens a statement journal
        // only for statements that may write several rows, so a single-row statement that
        // fails inside `BEGIN` keeps whatever `xUpdate` already did. The removals and the put
        // are therefore bracketed by an engine savepoint that is rolled back on any failure.
        let removes_rows = pk_conflict_to_delete.is_some()
            || !unique_conflicts.is_empty()
            || rowid_conflict_pk.is_some()
            || old_pk_opt
                .as_ref()
                .is_some_and(|old_pk| old_pk.as_slice() != new_pk.as_slice());
        if removes_rows {
            if let Err(e) = eng.savepoint_for_conn(vtab.db as usize, STATEMENT_SAVEPOINT) {
                return io_err_to_sqlite_rc(&e);
            }
        }
        let applied = (|| -> io::Result<i64> {
            for conflict_pk in pk_conflict_to_delete.into_iter().chain(unique_conflicts) {
                eng.delete_row(vtab.table_id, &conflict_pk, vtab.durability)?;
            }

            if let Some(conflict_pk) = rowid_conflict_pk {
                eng.delete_row(vtab.table_id, &conflict_pk, vtab.durability)?;
            }

            // If UPDATE changed the physical PK bytes, delete the old PK row
            if let Some(old_pk) = old_pk_opt {
                if old_pk.as_slice() != new_pk.as_slice() {
                    eng.delete_row(vtab.table_id, &old_pk, vtab.durability)?;
                    if same_pk {
                        known_absent = true;
                    }
                }
            }

            eng.put_row_owned_with_hint(
                vtab.table_id,
                &new_pk,
                row_bytes,
                &sec_keys,
                vtab.durability,
                known_absent,
            )?;

            if let Some(auto_id) = pk_override {
                Ok(auto_id)
            } else if let Some(int_id) = coerced_int_pk {
                Ok(int_id)
            } else {
                rowid_map::assign_synthetic_rowid(
                    &mut eng,
                    vtab.table_id,
                    &new_pk,
                    vtab.durability,
                    vtab.pk_affinity == ColumnAffinity::Integer,
                )
            }
        })();
        if removes_rows {
            if applied.is_err() {
                let _ = eng.rollback_to_savepoint_for_conn(vtab.db as usize, STATEMENT_SAVEPOINT);
            }
            let _ = eng.release_savepoint_for_conn(vtab.db as usize, STATEMENT_SAVEPOINT);
        }
        let rid = match applied {
            Ok(rid) => rid,
            Err(e) => return io_err_to_sqlite_rc(&e),
        };
        if !p_rowid.is_null() {
            *p_rowid = rid;
        }
        SQLITE_OK
    }))
    .unwrap_or(SQLITE_ERROR)
}

unsafe extern "system" fn vtab_begin_tx(p_vtab: *mut sqlite3_vtab) -> c_int {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        let vtab = &*(p_vtab as *const AydbVTab);
        // Wait for a transaction on another connection to end. The wait is bounded because that
        // connection may be held open by the same thread, which could never end it.
        let mut deadline = None;
        loop {
            let mut eng = match vtab.engine.write() {
                Ok(g) => g,
                Err(_) => return SQLITE_ERROR,
            };
            if let Some(snapshot) = vtab.read_session.current() {
                if let Err(error) = eng.check_snapshot_write_upgrade(vtab.db as usize, &snapshot) {
                    return io_err_to_sqlite_rc(&error);
                }
            }
            match eng.begin_transaction_for_conn(vtab.db as usize) {
                Ok(()) => break,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    let now = Instant::now();
                    let end = *deadline.get_or_insert(now + eng.writer_wait_timeout());
                    if now >= end {
                        return io_err_to_sqlite_rc(&e);
                    }
                    let gate = eng.commit_gate();
                    let seen = gate.tx_end_count();
                    drop(eng);
                    gate.wait_tx_end(seen, end - now);
                }
                Err(e) => return io_err_to_sqlite_rc(&e),
            }
        }
        SQLITE_OK
    }))
    .unwrap_or(SQLITE_ERROR)
}

unsafe extern "system" fn vtab_sync_tx(p_vtab: *mut sqlite3_vtab) -> c_int {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        let vtab = &*(p_vtab as *const AydbVTab);
        let mut eng = match vtab.engine.write() {
            Ok(g) => g,
            Err(_) => return SQLITE_ERROR,
        };
        if let Err(e) = eng.check_write_allowed_for_conn(vtab.db as usize) {
            return io_err_to_sqlite_rc(&e);
        }
        if let Err(e) = eng.prepare_commit_transaction(vtab.durability) {
            return io_err_to_sqlite_rc(&e);
        }
        SQLITE_OK
    }))
    .unwrap_or(SQLITE_ERROR)
}

unsafe extern "system" fn vtab_commit_tx(p_vtab: *mut sqlite3_vtab) -> c_int {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        let vtab = &*(p_vtab as *const AydbVTab);
        let step = match vtab.engine.write() {
            Ok(mut eng) => eng.begin_commit_for_conn(vtab.db as usize),
            Err(_) => return SQLITE_ERROR,
        };
        // The device flush runs without the engine lock so that readers are not held up by it.
        let committed = step.and_then(|step| match step {
            CommitStep::Done => Ok(()),
            CommitStep::AwaitingSync(file) => {
                let flushed = file.sync_barrier();
                match vtab.engine.write() {
                    Ok(mut eng) => eng.finish_commit_for_conn(flushed),
                    Err(_) => Err(io::Error::other("engine lock poisoned")),
                }
            }
            CommitStep::AwaitingGroup { lsn, file, gate } => loop {
                match gate.join_group(lsn)? {
                    GroupRole::Durable => break Ok(()),
                    GroupRole::Lead(target) => {
                        let flushed = file.sync_barrier();
                        match vtab.engine.write() {
                            Ok(mut eng) => eng.finish_group_flush(target, flushed)?,
                            Err(_) => {
                                let e = io::Error::other("engine lock poisoned");
                                gate.abandon_flush(&e);
                                break Err(e);
                            }
                        }
                    }
                }
            },
        });
        if let Err(e) = committed {
            return io_err_to_sqlite_rc(&e);
        }
        SQLITE_OK
    }))
    .unwrap_or(SQLITE_ERROR)
}

unsafe extern "system" fn vtab_rollback_tx(p_vtab: *mut sqlite3_vtab) -> c_int {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        let vtab = &*(p_vtab as *const AydbVTab);
        let mut eng = match vtab.engine.write() {
            Ok(g) => g,
            Err(_) => return SQLITE_ERROR,
        };
        if let Err(e) = eng.rollback_transaction_for_conn(vtab.db as usize) {
            return io_err_to_sqlite_rc(&e);
        }
        SQLITE_OK
    }))
    .unwrap_or(SQLITE_ERROR)
}

unsafe extern "system" fn vtab_savepoint(p_vtab: *mut sqlite3_vtab, i_savepoint: c_int) -> c_int {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        let vtab = &*(p_vtab as *const AydbVTab);
        let mut eng = match vtab.engine.write() {
            Ok(g) => g,
            Err(_) => return SQLITE_ERROR,
        };
        if let Err(e) = eng.savepoint_for_conn(vtab.db as usize, i_savepoint) {
            return io_err_to_sqlite_rc(&e);
        }
        SQLITE_OK
    }))
    .unwrap_or(SQLITE_ERROR)
}

unsafe extern "system" fn vtab_release(p_vtab: *mut sqlite3_vtab, i_savepoint: c_int) -> c_int {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        let vtab = &*(p_vtab as *const AydbVTab);
        let mut eng = match vtab.engine.write() {
            Ok(g) => g,
            Err(_) => return SQLITE_ERROR,
        };
        if let Err(e) = eng.release_savepoint_for_conn(vtab.db as usize, i_savepoint) {
            return io_err_to_sqlite_rc(&e);
        }
        SQLITE_OK
    }))
    .unwrap_or(SQLITE_ERROR)
}

unsafe extern "system" fn vtab_rollback_to(p_vtab: *mut sqlite3_vtab, i_savepoint: c_int) -> c_int {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        let vtab = &*(p_vtab as *const AydbVTab);
        let mut eng = match vtab.engine.write() {
            Ok(g) => g,
            Err(_) => return SQLITE_ERROR,
        };
        if let Err(e) = eng.rollback_to_savepoint_for_conn(vtab.db as usize, i_savepoint) {
            return io_err_to_sqlite_rc(&e);
        }
        SQLITE_OK
    }))
    .unwrap_or(SQLITE_ERROR)
}
