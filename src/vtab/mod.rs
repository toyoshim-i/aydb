//! Phase 2: SQLite Virtual Table (`sqlite3_module`) Adapter & Safe SQLite Connection Wrapper.
//!
//! Exposes `aydb`'s [`StorageEngine`] as a first-class SQLite Virtual Table (`USING aydb(...)`)
//! with automatic primary-key point lookups (`xBestIndex` Plan 1), secondary index equality
//! lookups (Plan 2), primary-key range scans (Plan 3), secondary index range scans (Plan 4),
//! and Capped Table (`MAX_BYTES`) LRU eviction.

#[cfg(test)]
mod auto_rowid_tests;
#[cfg(test)]
mod check_tests;
#[cfg(test)]
mod column_def_tests;
mod compaction_worker;
#[cfg(test)]
mod conflict_tests;
#[cfg(test)]
mod connect_tests;
pub mod connection;
#[cfg(test)]
mod constraint_tests;
mod constraints;
#[cfg(test)]
mod create_tests;
mod ddl;
#[cfg(test)]
mod drop_tests;
pub mod ffi;
#[cfg(test)]
mod integrity_tests;
#[cfg(test)]
mod key_null_tests;
#[cfg(test)]
mod pk_update_tests;
#[cfg(test)]
mod read_poison_tests;
#[cfg(test)]
mod rename_tests;
pub mod row_codec;
pub mod rowid_map;
#[cfg(test)]
mod rowid_tests;
#[cfg(test)]
mod rowid_write_tests;
#[cfg(test)]
mod savepoint_tests;
mod snapshot;
#[cfg(test)]
mod snapshot_tests;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod unique_check_tests;
#[cfg(test)]
mod unique_index_tests;
pub(crate) mod unique_key;
pub(crate) mod unique_migration;
#[cfg(test)]
mod unique_migration_tests;

use core::ffi::{c_char, c_int, c_void};
use std::ffi::{CStr, CString};
use std::io;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Instant;

use crate::engine::LazyBlob;
use crate::engine::{
    CommitStep, EngineReader, GroupRole, STATEMENT_SAVEPOINT, StorageEngine, WriteDurability,
};
use crate::pal::{OsFile, OsFs, PlatformFile};
pub use crate::vtab::connection::{SqliteConnection, SqliteStatement};
use crate::vtab::ddl::{ParsedVtabDdl, parse_vtab_ddl};
use crate::vtab::ffi::{
    SQLITE_BUSY, SQLITE_CONSTRAINT, SQLITE_CORRUPT, SQLITE_DONE, SQLITE_ERROR, SQLITE_FLOAT,
    SQLITE_FULL, SQLITE_INDEX_CONSTRAINT_EQ, SQLITE_INDEX_CONSTRAINT_GE,
    SQLITE_INDEX_CONSTRAINT_GT, SQLITE_INDEX_CONSTRAINT_LE, SQLITE_INDEX_CONSTRAINT_LT,
    SQLITE_INDEX_SCAN_UNIQUE, SQLITE_INTEGER, SQLITE_IOERR, SQLITE_MISMATCH, SQLITE_NULL,
    SQLITE_OK, SQLITE_REPLACE, SQLITE_ROW, SQLITE_TEXT, SQLITE_TXN_WRITE,
    SQLITE_VTAB_CONSTRAINT_SUPPORT, sqlite_column_text_bytes, sqlite_transient,
    sqlite_value_text_bytes, sqlite3, sqlite3_bind_text, sqlite3_context, sqlite3_declare_vtab,
    sqlite3_finalize, sqlite3_get_autocommit, sqlite3_index_info, sqlite3_int64, sqlite3_malloc,
    sqlite3_module, sqlite3_prepare_v2, sqlite3_result_error_code, sqlite3_result_int64,
    sqlite3_step, sqlite3_stmt, sqlite3_txn_state, sqlite3_value, sqlite3_value_double,
    sqlite3_value_int64, sqlite3_value_type, sqlite3_vtab, sqlite3_vtab_collation,
    sqlite3_vtab_config, sqlite3_vtab_cursor, sqlite3_vtab_on_conflict,
};

const CREATE_SAVEPOINT: i32 = STATEMENT_SAVEPOINT + 1;
pub use crate::vtab::row_codec::{
    ColCollation, ColumnAffinity, KEY_TAG_BLOB, KEY_TAG_FLOAT_GE_I64_MAX_PLUS_1,
    KEY_TAG_FLOAT_LT_I64_MIN, KEY_TAG_INTEGER, KEY_TAG_NULL, KEY_TAG_TEXT, SqlValue,
    decode_all_columns, decode_column_at, decode_f64_memcomparable, decode_i64_tagged_key,
    encode_canonical_numeric_key_from_f64, encode_f64_memcomparable, encode_f64_unified_key,
    encode_i64_tagged_key, encode_row, encode_sqlite_values_direct, extract_column_key_bytes,
    extract_column_key_bytes_with_collation, f64_to_exact_i64, format_sqlite_float_text,
    parse_sqlite_numeric_str, project_column_zero_copy, project_key_zero_copy,
    sqlite_value_to_key_bytes, sqlite_value_to_key_bytes_with_schema, try_encode_row,
    try_encode_sqlite_values_direct,
};
pub use crate::vtab::rowid_map::hash_pk_to_rowid;
use crate::vtab::rowid_map::{extract_rowid_from_pk_bytes, extract_rowid_from_row};

const PLAN_FULL_SCAN: c_int = 0;
const PLAN_PK_POINT: c_int = 1;
const PLAN_SECONDARY_EQ: c_int = 2;
const PLAN_PK_RANGE: c_int = 3;
const PLAN_SECONDARY_RANGE: c_int = 4;
const PLAN_UNIQUE_EQ: c_int = 5;

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

/// Shrink `cursor.lazy_row_buf` down to this capacity on cursor reset after a huge single blob row.
const LAZY_ROW_BUF_SHRINK_THRESHOLD: usize = 256 * 1024;

#[cfg(test)]
thread_local! {
    /// Test observability: high-water mark of `cursor.slab.len()` observed in `vtab_column`.
    static CURSOR_SLAB_MAX_LEN: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    /// Test observability: high-water mark of `cursor.lazy_row_buf.len()` observed in `vtab_column`.
    static CURSOR_LAZY_ROW_BUF_MAX_LEN: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    /// Test observability: last `(idxNum, orderByConsumed, argv_indexes)` chosen by `vtab_best_index`.
    static LAST_BEST_INDEX_PLAN: std::cell::RefCell<Option<(c_int, c_int, Vec<c_int>)>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
fn record_best_index_plan(
    info: &sqlite3_index_info,
    usages: &[crate::vtab::ffi::sqlite3_index_constraint_usage],
) {
    let args = usages.iter().map(|u| u.argvIndex).collect();
    LAST_BEST_INDEX_PLAN.with(|cell| {
        *cell.borrow_mut() = Some((info.idxNum, info.orderByConsumed, args));
    });
}

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

#[inline]
fn needs_numeric_alt_probe(pk: &[u8], affinity: ColumnAffinity) -> bool {
    if !matches!(affinity, ColumnAffinity::Integer | ColumnAffinity::Numeric) {
        return true;
    }
    // Under SQLite's strict open coercion interval (-2^63, 2^63), `-9223372036854775808.0` stays
    // REAL (17-byte key) in INTEGER/NUMERIC columns while comparing equal to `i64::MIN` (9-byte
    // key). Check the leading 9-byte integer prefix directly via `decode_i64_tagged_key`.
    pk.len() >= 9 && decode_i64_tagged_key(&pk[..9]) == Some(i64::MIN)
}

/// Checks if `pk` or its exact-integer numeric dual (`build_numeric_alt_key(pk)`) exists in
/// `table_id`, returning the existing key bytes if found.
///
/// When `pk_affinity` is `ColumnAffinity::Integer` or `ColumnAffinity::Numeric`, all exact-integer
/// floats in `(-2^63, 2^63)` are coerced to 9-byte `Integer(q)` before storage, so only
/// `i64::MIN` / `-9223372036854775808.0` can have an uncoerced numeric dual in the table.
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
    if needs_numeric_alt_probe(pk, pk_affinity) {
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
/// `try_with_committed_read_for_conn`, which re-resolves the state and propagates blob flush
/// errors. `f` may run a second time if a shared-lock read reports that it needed exclusive
/// access.
fn with_read_access<R>(
    engine: &RwLock<StorageEngine<OsFs>>,
    conn_id: usize,
    table_id: u16,
    mut f: impl FnMut(&EngineReader<'_, OsFs>) -> R,
) -> io::Result<R> {
    {
        let g = engine
            .read()
            .map_err(|_| io::Error::other("engine lock poisoned"))?;
        if !g.vtab_read_needs_exclusive(conn_id, table_id) {
            let rd = g.reader_for_conn(conn_id);
            let r = f(&rd);
            if !rd.needs_exclusive_retry() {
                return Ok(r);
            }
        }
    }
    let mut g = engine
        .write()
        .map_err(|_| io::Error::other("engine lock poisoned"))?;
    g.try_with_committed_read_for_conn(conn_id, f)
}

fn with_snapshot_read_access<R>(
    engine: &RwLock<StorageEngine<OsFs>>,
    conn_id: usize,
    table_id: u16,
    snapshot: Option<&crate::engine::ReadSnapshotToken>,
    mut f: impl FnMut(&EngineReader<'_, OsFs>) -> R,
) -> io::Result<R> {
    if let Some(snapshot) = snapshot {
        {
            let guard = engine
                .read()
                .map_err(|_| io::Error::other("engine lock poisoned"))?;
            if !guard.snapshot_read_needs_exclusive(table_id) {
                return Ok(f(&guard.reader_for_snapshot(snapshot)));
            }
        }
        let mut guard = engine
            .write()
            .map_err(|_| io::Error::other("engine lock poisoned"))?;
        guard.with_snapshot_read(snapshot, f)
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

unsafe fn extract_lossless_rowid_arg(val: *mut sqlite3_value) -> Option<i64> {
    if val.is_null() {
        return None;
    }
    match unsafe { sqlite3_value_type(val) } {
        SQLITE_INTEGER => Some(unsafe { sqlite3_value_int64(val) }),
        SQLITE_FLOAT => f64_to_exact_i64(unsafe { sqlite3_value_double(val) }),
        SQLITE_TEXT => {
            let slice = unsafe { sqlite_value_text_bytes(val) };
            if slice.is_empty() {
                return None;
            }
            let s = core::str::from_utf8(slice).ok()?;
            match parse_sqlite_numeric_str(s) {
                Some(Ok(i)) => Some(i),
                _ => None,
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
    if vtab.pk_rowid_alias {
        let integer_pk = encode_i64_tagged_key(target_rid);
        if row_exists(vtab.table_id, &integer_pk)? {
            return Ok(Some(integer_pk.to_vec()));
        }
    }
    // A local cache may contain a deleted or reused identity from another connection.
    // Resolve against the same persisted read view used for the row itself.
    lookup(vtab.table_id, target_rid)
}

/// Static `sqlite3_module` method table registered with `sqlite3_create_module_v2`.
pub(super) static AYDB_VTAB_MODULE: sqlite3_module = sqlite3_module {
    iVersion: 2,
    xCreate: Some(vtab_create),
    xConnect: Some(vtab_connect),
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
    xRename: Some(vtab_rename),
    xSavepoint: Some(vtab_savepoint),
    xRelease: Some(vtab_release),
    xRollbackTo: Some(vtab_rollback_to),
    xShadowName: None,
    xIntegrity: None,
};

pub(super) enum PendingDdl {
    Drop {
        engine: Arc<RwLock<StorageEngine<OsFs>>>,
        conn_id: usize,
        durability: WriteDurability,
        destroyed_token: (u16, u64),
    },
    Rename {
        engine: Arc<RwLock<StorageEngine<OsFs>>>,
        conn_id: usize,
        durability: WriteDurability,
        table_name: Arc<std::sync::Mutex<String>>,
        old_name: String,
    },
    TxCommit {
        engine: Arc<RwLock<StorageEngine<OsFs>>>,
        conn_id: usize,
        durability: WriteDurability,
    },
}

pub(super) struct PendingDdlSlot {
    pending: std::sync::Mutex<Option<PendingDdl>>,
    last_error: std::sync::Mutex<Option<String>>,
}

impl PendingDdlSlot {
    pub(super) fn new() -> Self {
        Self {
            pending: std::sync::Mutex::new(None),
            last_error: std::sync::Mutex::new(None),
        }
    }

    fn set(&self, ddl: PendingDdl) -> bool {
        self.clear_error();
        let Ok(mut slot) = self.pending.lock() else {
            return false;
        };
        if slot.is_some() {
            return false;
        }
        *slot = Some(ddl);
        true
    }

    fn set_tx_commit(&self, ddl: PendingDdl) -> bool {
        self.clear_error();
        let Ok(mut slot) = self.pending.lock() else {
            return false;
        };
        if matches!(*slot, None | Some(PendingDdl::TxCommit { .. })) {
            *slot = Some(ddl);
            true
        } else {
            false
        }
    }

    fn clear_tx_commit(&self) {
        if let Ok(mut slot) = self.pending.lock() {
            if matches!(*slot, Some(PendingDdl::TxCommit { .. })) {
                *slot = None;
            }
        }
    }

    fn take(&self) -> Option<PendingDdl> {
        self.pending.lock().ok().and_then(|mut s| s.take())
    }

    fn is_occupied(&self) -> bool {
        self.pending.lock().map_or(true, |s| s.is_some())
    }

    pub(super) fn clear_error(&self) {
        if let Ok(mut err) = self.last_error.lock() {
            *err = None;
        }
    }

    fn set_error(&self, msg: String) {
        if let Ok(mut err) = self.last_error.lock() {
            *err = Some(msg);
        }
    }

    pub(super) fn take_error(&self) -> Option<String> {
        self.last_error.lock().ok().and_then(|mut err| err.take())
    }

    fn cleanup_aborted(ddl: PendingDdl, rollback: bool) {
        match ddl {
            PendingDdl::Drop {
                engine,
                conn_id,
                destroyed_token: (tid, tok),
                ..
            } => {
                if let Ok(mut eng) = engine.write() {
                    if rollback && eng.conn_has_open_transaction(conn_id) {
                        let _ = eng.rollback_transaction_for_conn(conn_id);
                    }
                    eng.unregister_vtab_token(tid, tok);
                }
            }
            PendingDdl::Rename {
                engine,
                conn_id,
                table_name,
                old_name,
                ..
            } => {
                if rollback {
                    if let Ok(mut eng) = engine.write() {
                        if eng.conn_has_open_transaction(conn_id) {
                            let _ = eng.rollback_transaction_for_conn(conn_id);
                        }
                    }
                }
                if let Ok(mut name) = table_name.lock() {
                    *name = old_name;
                }
            }
            PendingDdl::TxCommit {
                engine, conn_id, ..
            } => {
                if rollback {
                    if let Ok(mut eng) = engine.write() {
                        if eng.conn_has_open_transaction(conn_id) {
                            let _ = eng.rollback_transaction_for_conn(conn_id);
                        }
                    }
                }
            }
        }
    }

    pub(super) fn commit_pending(&self) -> c_int {
        let Some(ddl) = self.take() else {
            return 0;
        };
        self.clear_error();
        let (engine, conn_id, durability) = match &ddl {
            PendingDdl::Drop {
                engine,
                conn_id,
                durability,
                ..
            }
            | PendingDdl::Rename {
                engine,
                conn_id,
                durability,
                ..
            }
            | PendingDdl::TxCommit {
                engine,
                conn_id,
                durability,
            } => (Arc::clone(engine), *conn_id, *durability),
        };
        #[cfg(test)]
        let fail_sync: Option<Arc<AtomicBool>>;
        let step = {
            let mut eng = match engine.write() {
                Ok(g) => g,
                Err(_) => {
                    Self::cleanup_aborted(ddl, false);
                    self.set_error("engine lock poisoned".to_string());
                    return 1;
                }
            };
            if !eng.conn_has_open_transaction(conn_id) {
                return 0;
            }
            if let Err(e) = eng.prepare_commit_transaction(durability) {
                drop(eng);
                Self::cleanup_aborted(ddl, true);
                self.set_error(e.to_string());
                return 1;
            }
            #[cfg(test)]
            {
                fail_sync = Some(eng.debug_fail_wal_sync_flag());
            }
            eng.begin_commit_for_conn(conn_id)
        };
        if let Err(e) = complete_commit_step_for_engine(
            &engine,
            step,
            #[cfg(test)]
            fail_sync,
        ) {
            Self::cleanup_aborted(ddl, true);
            self.set_error(e.to_string());
            return 1;
        }
        0
    }

    pub(super) fn rollback_pending(&self) {
        if let Some(ddl) = self.take() {
            Self::cleanup_aborted(ddl, true);
        }
    }
}

pub(super) struct ModuleAuxContext {
    pub(super) engine: Arc<RwLock<StorageEngine<OsFs>>>,
    read_session: Arc<snapshot::ReadSession>,
    pub(super) pending_ddl: Arc<PendingDdlSlot>,
    /// Background compaction worker; joined when the module (and thus this context) is dropped.
    pub(super) _compaction_worker: Option<compaction_worker::CompactionWorker>,
}

#[repr(C)]
pub(super) struct AydbVTab {
    base: core::cell::UnsafeCell<sqlite3_vtab>,
    db: *mut sqlite3,
    engine: Arc<RwLock<StorageEngine<OsFs>>>,
    read_session: Arc<snapshot::ReadSession>,
    pending_ddl: Arc<PendingDdlSlot>,
    table_id: u16,
    reg_token: u64,
    db_name: String,
    /// Normalised at use. Changes when the table is renamed.
    table_name: Arc<std::sync::Mutex<String>>,
    /// Name before an uncommitted rename inside the current transaction, used to clean up live
    /// registration if the rename is rolled back without SQLite invoking `xRollback` on this vtab.
    pre_tx_name: std::sync::Mutex<Option<String>>,
    durability: WriteDurability,
    /// The UNIQUE columns with the collation and affinity that shape their index keys.
    unique_def: unique_key::UniqueDef,
    /// Set once the UNIQUE index marker for `unique_def` has been observed in the committed
    /// view (or at once for a table without UNIQUE columns). Only then may the conflict check
    /// trust the index.
    unique_ready: AtomicBool,
    /// A migration attempt of this connection failed; it is not repeated until reconnect.
    unique_migration_failed: AtomicBool,
    /// Where the next `xBegin` resumes the UNIQUE index migration.
    unique_cursor: std::sync::Mutex<Option<Vec<u8>>>,
    col_names: Vec<String>,
    col_affinities: Vec<ColumnAffinity>,
    col_not_null: Vec<bool>,
    col_unique: Vec<bool>,
    col_collations: Vec<ColCollation>,
    pub(super) pk_col_idx: usize,
    pub(super) pk_affinity: ColumnAffinity,
    /// The key is `INTEGER PRIMARY KEY`, a rowid alias: NULL on insert assigns the next key.
    pk_rowid_alias: bool,
    /// `(table mutation epoch, next automatic key)` as left by this table's last automatic
    /// insert. Valid only while the table's epoch is unchanged: unrelated writes on other tables
    /// do not invalidate it, while any write/commit/rollback/drop on this table bumps it.
    auto_key_cache: std::sync::Mutex<Option<(u64, i64)>>,
    /// The declaration has a clause that is not honoured (`CHECK`, `DEFAULT`):
    /// every write that stores a row fails with this message.
    unsupported: Option<&'static str>,
    pub(super) pk_collation: ColCollation,
    /// Compact list of `(col_idx, index_id)` (`1..=N`) for secondary indexed columns.
    secondary_indexes: Vec<(usize, u16)>,
}

impl AydbVTab {
    /// The table's current name.
    fn name(&self) -> String {
        self.table_name
            .lock()
            .map(|name| name.clone())
            .unwrap_or_default()
    }
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
    /// Lazily batch-populated persistent synthetic rowids for the current chunk's `pks`
    /// (non-`INTEGER PRIMARY KEY` tables). Empty until `vtab_rowid` or `xColumn(-1)` runs.
    rowids: Vec<Option<i64>>,
    /// Pending out-of-line blob pointer for lazy dereference in `vtab_column`.
    /// When `Some(ptr)`, the row's blob payload has not been read into `lazy_row_buf` yet.
    /// When `None`, the row was either inline (in `slab`) or has already been dereferenced.
    lazy_blobs: Vec<Option<LazyBlob<OsFile>>>,
    /// Reusable single-row buffer for dereferencing the active row's out-of-line lazy blob,
    /// preventing `slab` from accumulating up to `CURSOR_STREAM_CHUNK_ROWS` large blobs at once.
    lazy_row_buf: Vec<u8>,
    /// Chunk row index currently materialized in `lazy_row_buf`, if any.
    lazy_row_pos: Option<usize>,
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
/// Returns `true` when searching a `TEXT` or `BLOB` column with an `INTEGER` or `REAL` SQLite
/// value (e.g., `CAST(1 AS INTEGER)` or a join against an integer column).
///
/// Under SQLite's cross-affinity comparison rules, a single typed lookup key cannot match all
/// storage representations (`'1'`, `'01'`, `'1.0'` for `TEXT`; integers and numeric strings under
/// integer-affine joins for `BLOB`), so callers must fall back to a scan that lets SQLite's VDBE
/// filter each candidate row (`omit = 0`).
#[inline]
fn is_cross_affinity_numeric_search(col_affinity: ColumnAffinity, val_type: c_int) -> bool {
    matches!(col_affinity, ColumnAffinity::Text | ColumnAffinity::Blob)
        && matches!(val_type, SQLITE_INTEGER | SQLITE_FLOAT)
}

/// Initializes a streaming primary-key full-table scan on `cursor`, used both for `PLAN_FULL_SCAN`
/// and when an equality filter falls back on cross-affinity numeric comparison.
fn start_pk_full_scan(
    cursor: &mut AydbCursor,
    eng: &EngineReader<'_, OsFs>,
    table_id: u16,
    keys_only: bool,
    reverse: bool,
) -> io::Result<()> {
    cursor.keys_only = keys_only;
    eng.scan_range_into_slab_with_cache(
        table_id,
        None,
        None,
        reverse,
        CURSOR_STREAM_CHUNK_ROWS,
        keys_only,
        false,
        &mut cursor.pks,
        &mut cursor.slab,
        &mut cursor.spans,
        Some(&mut cursor.lazy_blobs),
    )?;
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
    Ok(())
}

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
    if is_cross_affinity_numeric_search(pk_affinity, val_type) {
        return Ok(None);
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
    if is_cross_affinity_numeric_search(sec_affinity, val_type) {
        return Ok(None);
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
    eng.with_committed_read_for_conn(conn_id, |reader| {
        let mut upper = vec![KEY_TAG_INTEGER + 1];
        let mut upper_inclusive = false;
        let mut limit = 1usize;
        let (mut pks, mut slab, mut spans) = (Vec::new(), Vec::new(), Vec::new());
        loop {
            pks.clear();
            slab.clear();
            spans.clear();
            reader.scan_range_into_slab_with_cache(
                table_id,
                Some((&lower, true)),
                Some((&upper, upper_inclusive)),
                true,
                limit,
                true,
                true,
                &mut pks,
                &mut slab,
                &mut spans,
                None,
            )?;
            for k in &pks {
                if let Some(id) = decode_i64_tagged_key(k) {
                    return Ok(Some(id));
                }
            }
            if pks.len() < limit {
                return Ok(None);
            }
            let last = pks.last().expect("limit >= 1 and pks.len() >= limit");
            // Floats in [-2^63, 2^63) share `KEY_TAG_INTEGER` as 17-byte keys sorting strictly
            // after their 9-byte `floor(f)` integer key with no integer keys in between; jumping to
            // `last[..9]` inclusive skips every float in that bucket in one step.
            if last.len() > 9 && last[0] == KEY_TAG_INTEGER {
                upper = last[..9].to_vec();
                upper_inclusive = true;
            } else {
                upper = last.clone();
                upper_inclusive = false;
            }
            limit = 64;
        }
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
    reg_token: u64,
    unique_ready: bool,
    /// The catalog had no entry for the table before this connect: it is a new table.
    newly_created: bool,
}

/// Registers the table for a connection and prepares its runtime state, all under one engine
/// lock. Returns `None`, having changed nothing, when the table cannot be connected without
/// writing durable state while another connection's transaction is open: it is not yet in the
/// catalog, or its capped quota is not registered as declared.
///
/// With another transaction open, an existing table is only registered as live (see
/// [`StorageEngine::activate_persisted_vtab`]) and its runtime-only settings are applied. None of
/// that is transactional state: the corrupt policy and inline threshold are not restored by a
/// rollback, and the live registration is added to the snapshots a rollback restores.
///
/// A table that is in the catalog without its rowid mapping marker is refused with
/// `InvalidData` (see below): it was written by an unsupported older format.
fn connect_table(
    eng: &mut StorageEngine<OsFs>,
    conn_id: usize,
    ddl: &ParsedVtabDdl,
) -> io::Result<Option<Connected>> {
    let view = eng.read_view_for_conn(conn_id);
    let plan = eng.plan_vtab_registration_view(view, &ddl.table_name, ddl.explicit_table_id)?;
    let table_id = plan.table_id;
    let newly_created = !plan.persisted;
    // A table is created together with its rowid mapping marker, in one atomic write. One
    // without it was written by a build that predates the mapping, which is not supported.
    // A marker that cannot be read is left to the reads of the table to report.
    if plan.persisted && !eng.rowids_initialized_view(view, table_id).unwrap_or(true) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "table has no persistent rowid mapping (it was written by an unsupported older \
             format); recreate it",
        ));
    }
    let unique_def = unique_key::UniqueDef::from_columns(
        &ddl.col_unique,
        &ddl.col_collations,
        &ddl.col_affinities,
    );
    let disable_initial_unique_marker = {
        #[cfg(test)]
        {
            UNIQUE_MIGRATION_DISABLED.with(|flag| flag.get())
        }
        #[cfg(not(test))]
        {
            false
        }
    };
    let initial_unique_marker = if !unique_def.is_empty() && !disable_initial_unique_marker {
        Some(unique_def.marker_value())
    } else {
        None
    };
    let reg_token = if eng.check_write_allowed_for_conn(conn_id).is_err() {
        let quota_registered = ddl.max_bytes == 0
            || eng
                .capped_table_state_view(view, table_id)
                .is_some_and(|state| state.max_bytes == ddl.max_bytes);
        if !(plan.persisted && quota_registered) {
            return Ok(None);
        }
        eng.activate_persisted_vtab(table_id, &ddl.table_name)
    } else {
        register_table(eng, ddl, plan, initial_unique_marker)?
    };
    #[cfg(test)]
    if FAIL_CONNECT_AFTER_REGISTRATION.with(|flag| flag.replace(false)) {
        eng.unregister_vtab_token(table_id, reg_token);
        return Err(io::Error::other("injected connect failure"));
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
    let existing_marker = eng.unique_marker_view(view, table_id).ok().flatten();
    let unique_ready = if unique_def.is_empty() {
        existing_marker.is_none()
    } else {
        existing_marker.is_some_and(|found| found == unique_def.marker_value())
    };
    Ok(Some(Connected {
        table_id,
        reg_token,
        unique_ready,
        newly_created,
    }))
}

/// The writing part of a connect: catalog entry (with the table's rowid mapping marker and any
/// empty-table UNIQUE completion marker, in the same atomic write) and capped quota. It only
/// runs when no other connection has a transaction open. Returns the new holder registration token.
fn register_table(
    eng: &mut StorageEngine<OsFs>,
    ddl: &ParsedVtabDdl,
    plan: crate::engine::VtabPlan,
    initial_unique_marker: Option<Vec<u8>>,
) -> io::Result<u64> {
    let (table_id, reg_token) =
        eng.register_vtab_plan(plan, ddl.durability, initial_unique_marker)?;
    if ddl.max_bytes > 0 {
        if let Err(e) = eng.register_capped_table(table_id, ddl.max_bytes) {
            eng.unregister_vtab_token(table_id, reg_token);
            return Err(e);
        }
    }
    Ok(reg_token)
}

#[cfg(test)]
thread_local! {
    /// Test seam: makes the next connect on this thread fail right after it registered the table.
    static FAIL_CONNECT_AFTER_REGISTRATION: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

const RENAME_NEEDS_WRITE: &str = "ALTER TABLE RENAME of an aydb table inside a transaction needs an earlier write to an aydb table in that transaction";
const STALE_VTAB_ERR: &str = "virtual table handle is stale: table was dropped";

/// Copies `msg` into memory owned by SQLite (`sqlite3_free` releases it), as `*pzErr` and
/// `zErrMsg` require. Returns null if the allocation fails; the error code still reports.
unsafe fn sqlite_error_message(msg: &str) -> *mut c_char {
    unsafe {
        let buf = sqlite3_malloc((msg.len() + 1) as c_int) as *mut c_char;
        if !buf.is_null() {
            core::ptr::copy_nonoverlapping(msg.as_ptr() as *const c_char, buf, msg.len());
            *buf.add(msg.len()) = 0;
        }
        buf
    }
}

/// Replaces `(*p_vtab).zErrMsg` with a SQLite-owned copy of `msg`.
///
/// # Safety
/// `p_vtab` must be null or point to a valid `sqlite3_vtab` (such as `vtab.base.get()`).
unsafe fn set_vtab_err_msg(p_vtab: *mut sqlite3_vtab, msg: &str) {
    unsafe {
        if !p_vtab.is_null() {
            if !(*p_vtab).zErrMsg.is_null() {
                crate::vtab::ffi::sqlite3_free((*p_vtab).zErrMsg as *mut c_void);
            }
            (*p_vtab).zErrMsg = sqlite_error_message(msg);
        }
    }
}

/// Waits, bounded like `xBegin`, for another connection's transaction to end: the same thread
/// may hold that connection open and could never end it. Returns false once the wait ran out.
fn wait_for_other_transaction(
    eng: std::sync::RwLockWriteGuard<'_, StorageEngine<OsFs>>,
    deadline: &mut Option<Instant>,
) -> bool {
    let now = Instant::now();
    let end = *deadline.get_or_insert(now + eng.writer_wait_timeout());
    if now >= end {
        return false;
    }
    let gate = eng.commit_gate();
    let seen = gate.tx_end_count();
    drop(eng);
    gate.wait_tx_end(seen, end - now);
    true
}

unsafe extern "system" fn vtab_create(
    db: *mut sqlite3,
    p_aux: *mut c_void,
    argc: c_int,
    argv: *const *const c_char,
    pp_vtab: *mut *mut sqlite3_vtab,
    pz_err: *mut *mut c_char,
) -> c_int {
    unsafe { vtab_create_or_connect(db, p_aux, argc, argv, pp_vtab, pz_err, true) }
}

unsafe extern "system" fn vtab_connect(
    db: *mut sqlite3,
    p_aux: *mut c_void,
    argc: c_int,
    argv: *const *const c_char,
    pp_vtab: *mut *mut sqlite3_vtab,
    pz_err: *mut *mut c_char,
) -> c_int {
    unsafe { vtab_create_or_connect(db, p_aux, argc, argv, pp_vtab, pz_err, false) }
}

unsafe fn vtab_create_or_connect(
    db: *mut sqlite3,
    p_aux: *mut c_void,
    argc: c_int,
    argv: *const *const c_char,
    pp_vtab: *mut *mut sqlite3_vtab,
    pz_err: *mut *mut c_char,
    is_create: bool,
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
        // A new table may not have a CHECK or DEFAULT it cannot honour. An existing one (written
        // by an older build) must still connect so that it can be read and dropped; its writes
        // are refused instead.
        if let (true, Some(reason)) = (is_create, ddl.unsupported) {
            if !pz_err.is_null() {
                *pz_err = sqlite_error_message(reason);
            }
            return SQLITE_ERROR;
        }

        // Declare the columns first: it needs no engine state, so a failure leaves nothing
        // registered.
        let declare_sql = format!("CREATE TABLE x({})", ddl.declared_columns);
        let Ok(c_sql) = CString::new(declare_sql) else {
            return SQLITE_ERROR;
        };
        let rc = sqlite3_declare_vtab(db, c_sql.as_ptr());
        if rc != SQLITE_OK {
            return rc;
        }
        // Promise that `xUpdate` reports `SQLITE_CONSTRAINT` before it changes anything. SQLite
        // then applies the statement's conflict mode (ABORT, FAIL, IGNORE, ROLLBACK) to the
        // violation itself, row counters included, instead of treating it as a generic error.
        let rc = sqlite3_vtab_config(db, SQLITE_VTAB_CONSTRAINT_SUPPORT, 1 as c_int);
        if rc != SQLITE_OK {
            return rc;
        }

        // Connecting an existing table writes no durable state and never waits. Creating a new
        // table (or changing a table's quota) opens or joins an engine transaction: inside `BEGIN`
        // it commits via `xSync`/commit hook or `xCommit`; in autocommit it is staged and
        // committed in `sqlite3_commit_hook` so `SQLITE_BUSY` or WAL sync errors abort atomically.
        let mut deadline = None;
        let autocommit = sqlite3_get_autocommit(db) != 0;
        let mut began_here = false;
        let (mut eng, connected) = loop {
            let mut eng = match aux.engine.write() {
                Ok(g) => g,
                Err(_) => return SQLITE_ERROR,
            };
            if !autocommit {
                aux.pending_ddl.clear_tx_commit();
            }
            if is_create && !eng.conn_has_open_transaction(db as usize) {
                let need_tx = if !autocommit {
                    true
                } else {
                    let view = eng.read_view_for_conn(db as usize);
                    match eng.plan_vtab_registration_view(
                        view,
                        &ddl.table_name,
                        ddl.explicit_table_id,
                    ) {
                        Ok(plan) => {
                            let quota_registered = ddl.max_bytes == 0
                                || eng
                                    .capped_table_state_view(view, plan.table_id)
                                    .is_some_and(|state| state.max_bytes == ddl.max_bytes);
                            !(plan.persisted && quota_registered)
                        }
                        Err(e) => {
                            if e.kind() == io::ErrorKind::InvalidData && !pz_err.is_null() {
                                *pz_err = sqlite_error_message(&e.to_string());
                            }
                            return io_err_to_sqlite_rc(&e);
                        }
                    }
                };
                if need_tx {
                    if autocommit && aux.pending_ddl.is_occupied() {
                        return SQLITE_ERROR;
                    }
                    if let Some(snapshot) = aux.read_session.current() {
                        if let Err(error) = eng.check_snapshot_current_for_upgrade(&snapshot) {
                            if !pz_err.is_null() {
                                *pz_err = sqlite_error_message(&error.to_string());
                            }
                            return io_err_to_sqlite_rc(&error);
                        }
                    }
                    match eng.begin_transaction_for_conn(db as usize) {
                        Ok(()) => {
                            began_here = true;
                        }
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                            if !wait_for_other_transaction(eng, &mut deadline) {
                                return SQLITE_BUSY;
                            }
                            continue;
                        }
                        Err(e) => return io_err_to_sqlite_rc(&e),
                    }
                }
            }
            // A table that this transaction created under this name or id, and that SQLite is
            // creating again, was undone in SQLite's schema by a `ROLLBACK TO` that the table
            // never heard of (it was not announced the savepoint it was created in). Remove
            // what is left of it only if this connection owns the transaction and SQLite's
            // schema no longer has that table.
            if is_create && eng.conn_has_open_transaction(db as usize) {
                let cur_norm_db = crate::engine::normalize_vtab_ident(&ddl.db_name);
                eng = match prune_matching_created_vtabs_unlocked(
                    db,
                    &aux.engine,
                    db as usize,
                    eng,
                    ddl.explicit_table_id,
                    &ddl.table_name,
                    &cur_norm_db,
                    None,
                ) {
                    Ok(g) => g,
                    Err(e) => {
                        if began_here {
                            if let Ok(mut g) = aux.engine.write() {
                                let _ = g.rollback_transaction_for_conn(db as usize);
                            }
                        }
                        return io_err_to_sqlite_rc(&e);
                    }
                };
            }
            let guard_create_sp =
                is_create && !began_here && eng.conn_has_open_transaction(db as usize);
            if guard_create_sp {
                if let Err(e) = eng.savepoint_for_conn(db as usize, CREATE_SAVEPOINT) {
                    return io_err_to_sqlite_rc(&e);
                }
            }
            match connect_table(&mut eng, db as usize, &ddl) {
                Ok(Some(connected)) => {
                    if guard_create_sp {
                        if let Err(e) =
                            eng.release_savepoint_for_conn(db as usize, CREATE_SAVEPOINT)
                        {
                            return io_err_to_sqlite_rc(&e);
                        }
                    }
                    if is_create && connected.newly_created {
                        eng.note_vtab_created_in_tx(
                            connected.table_id,
                            &ddl.table_name,
                            &ddl.db_name,
                        );
                    }
                    break (eng, connected);
                }
                Ok(None) => {
                    if guard_create_sp {
                        let _ = eng.rollback_to_savepoint_for_conn(db as usize, CREATE_SAVEPOINT);
                        let _ = eng.release_savepoint_for_conn(db as usize, CREATE_SAVEPOINT);
                    }
                    if !wait_for_other_transaction(eng, &mut deadline) {
                        return SQLITE_BUSY;
                    }
                }
                Err(e) => {
                    if guard_create_sp {
                        let _ = eng.rollback_to_savepoint_for_conn(db as usize, CREATE_SAVEPOINT);
                        let _ = eng.release_savepoint_for_conn(db as usize, CREATE_SAVEPOINT);
                    } else if began_here {
                        let _ = eng.rollback_transaction_for_conn(db as usize);
                    }
                    if e.kind() == io::ErrorKind::InvalidData && !pz_err.is_null() {
                        *pz_err = sqlite_error_message(&e.to_string());
                    }
                    return io_err_to_sqlite_rc(&e);
                }
            }
        };
        let Connected {
            table_id,
            reg_token,
            unique_ready,
            ..
        } = connected;
        let ddl_durability = ddl.durability;

        let vtab = Box::new(AydbVTab {
            base: core::cell::UnsafeCell::new(sqlite3_vtab {
                pModule: &AYDB_VTAB_MODULE,
                nRef: 0,
                zErrMsg: core::ptr::null_mut(),
            }),
            db,
            engine: aux.engine.clone(),
            read_session: Arc::clone(&aux.read_session),
            pending_ddl: Arc::clone(&aux.pending_ddl),
            table_id,
            reg_token,
            db_name: ddl.db_name,
            table_name: Arc::new(std::sync::Mutex::new(ddl.table_name)),
            pre_tx_name: std::sync::Mutex::new(None),
            durability: ddl_durability,
            unique_def: unique_key::UniqueDef::from_columns(
                &ddl.col_unique,
                &ddl.col_collations,
                &ddl.col_affinities,
            ),
            unique_ready: AtomicBool::new(unique_ready),
            unique_migration_failed: AtomicBool::new(false),
            unique_cursor: std::sync::Mutex::new(None),
            col_names: ddl.col_names,
            col_affinities: ddl.col_affinities,
            col_not_null: ddl.col_not_null,
            col_unique: ddl.col_unique,
            col_collations: ddl.col_collations,
            pk_col_idx: ddl.pk_col_idx,
            pk_affinity: ddl.pk_affinity,
            pk_rowid_alias: ddl.pk_rowid_alias,
            auto_key_cache: std::sync::Mutex::new(None),
            unsupported: ddl.unsupported,
            pk_collation: ddl.pk_collation,
            secondary_indexes: ddl.secondary_indexes,
        });

        let raw_vtab = Box::into_raw(vtab) as *mut sqlite3_vtab;
        if is_create && eng.conn_has_open_transaction(db as usize) {
            eng.attach_vtab_to_tx(db as usize, raw_vtab as usize);
            if autocommit
                && began_here
                && !aux.pending_ddl.set_tx_commit(PendingDdl::TxCommit {
                    engine: Arc::clone(&aux.engine),
                    conn_id: db as usize,
                    durability: ddl_durability,
                })
            {
                let _ = eng.rollback_transaction_for_conn(db as usize);
                eng.unregister_vtab_token(table_id, reg_token);
                drop(eng);
                drop(Box::from_raw(raw_vtab as *mut AydbVTab));
                return SQLITE_ERROR;
            }
        }
        if began_here {
            aux.read_session.clear();
        }
        drop(eng);
        *pp_vtab = raw_vtab;
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
                    eng.detach_vtab_from_tx(vtab.db as usize, p_vtab as usize);
                    eng.unregister_vtab_token(vtab.table_id, vtab.reg_token);
                }
            }
        }
        SQLITE_OK
    }))
    .unwrap_or(SQLITE_ERROR)
}

/// `ALTER TABLE ... RENAME TO`: moves the catalog entry to the new name.
///
/// Transactional renames join an active engine transaction (if an earlier write already opened one)
/// and commit or roll back through `xSync`/`xCommit`/`xRollback`. Autocommit renames open a
/// short engine transaction and hold the final commit until SQLite invokes the connection's
/// `sqlite3_commit_hook` (returning `0` on success, or rolling back SQLite's schema update if WAL
/// commit fails), rolling back cleanly in `sqlite3_rollback_hook` if SQLite aborts before commit
/// (for instance due to `SQLITE_BUSY` / lock contention or broken views).
unsafe extern "system" fn vtab_rename(p_vtab: *mut sqlite3_vtab, z_new: *const c_char) -> c_int {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        if p_vtab.is_null() || z_new.is_null() {
            return SQLITE_ERROR;
        }
        let vtab = &*(p_vtab as *const AydbVTab);
        let new_name = CStr::from_ptr(z_new).to_string_lossy().into_owned();
        let conn_id = vtab.db as usize;
        let autocommit = sqlite3_get_autocommit(vtab.db) != 0;

        if !autocommit {
            vtab.pending_ddl.clear_tx_commit();
            let mut eng = match vtab.engine.write() {
                Ok(g) => g,
                Err(_) => return SQLITE_ERROR,
            };
            if let Err(e) = eng.check_write_allowed_for_conn(conn_id) {
                return io_err_to_sqlite_rc(&e);
            }
            // SQLite does not call `xBegin` for a rename, so inside a user transaction the
            // catalog change joins the engine transaction only if an earlier write opened one.
            if !eng.conn_has_open_transaction(conn_id) {
                set_vtab_err_msg(vtab.base.get(), RENAME_NEEDS_WRITE);
                return SQLITE_ERROR;
            }
            if !eng.is_vtab_token_active(vtab.table_id, vtab.reg_token, conn_id) {
                set_vtab_err_msg(vtab.base.get(), STALE_VTAB_ERR);
                return SQLITE_ERROR;
            }
            let cur_norm_db = crate::engine::normalize_vtab_ident(&vtab.db_name);
            eng = match prune_matching_created_vtabs_unlocked(
                vtab.db,
                &vtab.engine,
                conn_id,
                eng,
                None,
                &new_name,
                &cur_norm_db,
                Some(vtab.table_id),
            ) {
                Ok(g) => g,
                Err(e) => {
                    set_vtab_err_msg(vtab.base.get(), &e.to_string());
                    return io_err_to_sqlite_rc(&e);
                }
            };
            if !eng.conn_has_open_transaction(conn_id) {
                set_vtab_err_msg(vtab.base.get(), RENAME_NEEDS_WRITE);
                return SQLITE_ERROR;
            }
            if !eng.is_vtab_token_active(vtab.table_id, vtab.reg_token, conn_id) {
                set_vtab_err_msg(vtab.base.get(), STALE_VTAB_ERR);
                return SQLITE_ERROR;
            }
            let old_effective = eng
                .active_vtab_name(vtab.table_id)
                .unwrap_or_else(|| vtab.name());
            if let Err(e) =
                eng.rename_vtab(vtab.table_id, &old_effective, &new_name, vtab.durability)
            {
                set_vtab_err_msg(vtab.base.get(), &e.to_string());
                return io_err_to_sqlite_rc(&e);
            }
            if let Ok(mut prev) = vtab.pre_tx_name.lock() {
                prev.get_or_insert(old_effective);
            }
            if let Ok(mut name) = vtab.table_name.lock() {
                *name = new_name;
            }
            return SQLITE_OK;
        }

        if vtab.pending_ddl.is_occupied() {
            return SQLITE_ERROR;
        }
        let mut deadline = None;
        let mut eng = loop {
            let mut eng = match vtab.engine.write() {
                Ok(g) => g,
                Err(_) => return SQLITE_ERROR,
            };
            if eng.conn_has_open_transaction(conn_id) {
                return SQLITE_ERROR;
            }
            if !eng.is_vtab_token_active(vtab.table_id, vtab.reg_token, conn_id) {
                set_vtab_err_msg(vtab.base.get(), STALE_VTAB_ERR);
                return SQLITE_ERROR;
            }
            match eng.begin_transaction_for_conn(conn_id) {
                Ok(()) => break eng,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    if !wait_for_other_transaction(eng, &mut deadline) {
                        return SQLITE_BUSY;
                    }
                }
                Err(e) => {
                    set_vtab_err_msg(vtab.base.get(), &e.to_string());
                    return io_err_to_sqlite_rc(&e);
                }
            }
        };

        vtab.read_session.clear();
        let old_effective = eng
            .active_vtab_name(vtab.table_id)
            .unwrap_or_else(|| vtab.name());
        let prev_table_name = vtab.name();
        if let Err(e) = eng.rename_vtab(vtab.table_id, &old_effective, &new_name, vtab.durability) {
            let _ = eng.rollback_transaction_for_conn(conn_id);
            set_vtab_err_msg(vtab.base.get(), &e.to_string());
            return io_err_to_sqlite_rc(&e);
        }
        if let Ok(mut prev) = vtab.pre_tx_name.lock() {
            *prev = None;
        }
        if let Ok(mut name) = vtab.table_name.lock() {
            *name = new_name;
        }
        if !vtab.pending_ddl.set(PendingDdl::Rename {
            engine: Arc::clone(&vtab.engine),
            conn_id,
            durability: vtab.durability,
            table_name: Arc::clone(&vtab.table_name),
            old_name: prev_table_name.clone(),
        }) {
            let _ = eng.rollback_transaction_for_conn(conn_id);
            if let Ok(mut name) = vtab.table_name.lock() {
                *name = prev_table_name;
            }
            return SQLITE_ERROR;
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
                // SQLite does not call `xBegin` for a DROP, and once `xDestroy` has freed the
                // table it no longer calls `xCommit` or `xRollback` for it. Inside a user
                // transaction we therefore keep refusing `DROP TABLE`. In autocommit mode, we
                // stage `drop_table_with_durability` in an engine transaction and commit it only
                // in `sqlite3_commit_hook` once SQLite commits the schema change (or roll back in
                // `sqlite3_rollback_hook` if SQLite aborts due to locks or errors).
                if sqlite3_get_autocommit(vtab_ref.db) == 0 {
                    return SQLITE_ERROR;
                }
                if vtab_ref.pending_ddl.is_occupied() {
                    return SQLITE_ERROR;
                }
                let conn_id = vtab_ref.db as usize;
                let mut deadline = None;
                let mut eng = loop {
                    let mut eng = match vtab_ref.engine.write() {
                        Ok(g) => g,
                        Err(_) => return SQLITE_ERROR,
                    };
                    if eng.conn_has_open_transaction(conn_id) {
                        return SQLITE_ERROR;
                    }
                    if !eng.is_vtab_token_active(vtab_ref.table_id, vtab_ref.reg_token, conn_id) {
                        set_vtab_err_msg(vtab_ref.base.get(), STALE_VTAB_ERR);
                        return SQLITE_ERROR;
                    }
                    match eng.begin_transaction_for_conn(conn_id) {
                        Ok(()) => break eng,
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                            if !wait_for_other_transaction(eng, &mut deadline) {
                                return SQLITE_BUSY;
                            }
                        }
                        Err(e) => return io_err_to_sqlite_rc(&e),
                    }
                };
                vtab_ref.read_session.clear();
                if let Err(e) =
                    eng.drop_table_with_durability(vtab_ref.table_id, None, vtab_ref.durability)
                {
                    let _ = eng.rollback_transaction_for_conn(conn_id);
                    return io_err_to_sqlite_rc(&e);
                }
                if !vtab_ref.pending_ddl.set(PendingDdl::Drop {
                    engine: Arc::clone(&vtab_ref.engine),
                    conn_id,
                    durability: vtab_ref.durability,
                    destroyed_token: (vtab_ref.table_id, vtab_ref.reg_token),
                }) {
                    let _ = eng.rollback_transaction_for_conn(conn_id);
                    return SQLITE_ERROR;
                }
                drop(eng);
                drop(Box::from_raw(p_vtab as *mut AydbVTab));
            }
        }
        SQLITE_OK
    }))
    .unwrap_or(SQLITE_ERROR)
}

#[inline]
unsafe fn order_by_slice<'a>(
    n_order_by: c_int,
    a_order_by: *const crate::vtab::ffi::sqlite3_index_orderby,
) -> &'a [crate::vtab::ffi::sqlite3_index_orderby] {
    let n = usize::try_from(n_order_by).unwrap_or(0);
    if n == 0 || a_order_by.is_null() {
        &[]
    } else {
        unsafe { core::slice::from_raw_parts(a_order_by, n) }
    }
}

/// Returns `Some(reverse)` if a primary-key scan satisfies `order_by` directly. Because primary
/// keys are unique under their column collation, any trailing tie-breaker columns after `pk`
/// never re-order equal rows.
#[inline]
fn pk_order_by(
    order_by: &[crate::vtab::ffi::sqlite3_index_orderby],
    pk_col_idx: usize,
) -> Option<bool> {
    // Integer affinity also accepts REAL/TEXT/BLOB keys with synthetic rowids; only `pk_col_idx`
    // order (never `iColumn == -1`) is guaranteed to match the LSM key sort order.
    let first = order_by.first()?;
    if first.iColumn == pk_col_idx as i32 {
        Some(first.desc != 0)
    } else {
        None
    }
}

/// Returns `true` when an equality probe on `col_idx` satisfies `order_by`.
///
/// An equality scan yields matching rows in ascending `pk` order (including any cross-affinity
/// fallback via `start_pk_full_scan`), and for numeric/real/integer affinities every match has an
/// equal key so `ORDER BY col_idx[, pk ASC]` is also satisfied.
#[inline]
fn eq_index_consumes_order_by(
    order_by: &[crate::vtab::ffi::sqlite3_index_orderby],
    col_idx: usize,
    col_aff: ColumnAffinity,
    pk_col_idx: usize,
) -> bool {
    let Some(first) = order_by.first() else {
        return false;
    };
    if first.iColumn == pk_col_idx as i32 && first.desc == 0 {
        return true;
    }
    if matches!(col_aff, ColumnAffinity::Text | ColumnAffinity::Blob) {
        return false;
    }
    if first.iColumn == col_idx as i32 {
        return order_by.len() == 1
            || (order_by[1].iColumn == pk_col_idx as i32 && order_by[1].desc == 0);
    }
    false
}

/// Returns `true` when an ascending secondary-index range scan (`Escaped(col_val) | pk`)
/// satisfies `ORDER BY sec_col_idx ASC` or `ORDER BY sec_col_idx ASC, pk ASC`.
#[inline]
fn sec_range_consumes_order_by(
    order_by: &[crate::vtab::ffi::sqlite3_index_orderby],
    sec_col_idx: usize,
    pk_col_idx: usize,
) -> bool {
    let Some(first) = order_by.first() else {
        return false;
    };
    if first.iColumn != sec_col_idx as i32 || first.desc != 0 {
        return false;
    }
    order_by.len() == 1 || (order_by[1].iColumn == pk_col_idx as i32 && order_by[1].desc == 0)
}

struct SelectedBestIndexPlan {
    idx_num: c_int,
    idx_flags: Option<c_int>,
    order_by_consumed: bool,
    estimated_cost: f64,
    estimated_rows: i64,
    used_constraints: [usize; 2],
    num_used: usize,
}

unsafe extern "system" fn vtab_best_index(
    p_vtab: *mut sqlite3_vtab,
    p_info: *mut sqlite3_index_info,
) -> c_int {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        if p_vtab.is_null() || p_info.is_null() {
            return SQLITE_ERROR;
        }
        let vtab = &*(p_vtab as *const AydbVTab);
        // Read input fields via raw pointer without holding a live `&mut sqlite3_index_info`
        // across FFI calls to `sqlite3_vtab_collation(p_info, ...)`, which inspects `p_info`
        // and SQLite's trailing `HiddenIndexInfo`.
        let n_constraint = (*p_info).nConstraint;
        let a_constraint = (*p_info).aConstraint;
        let a_constraint_usage = (*p_info).aConstraintUsage;
        let n_order_by = (*p_info).nOrderBy;
        let a_order_by = (*p_info).aOrderBy;
        let col_used = (*p_info).colUsed;

        let n = usize::try_from(n_constraint).unwrap_or(0);
        let constraints: &[crate::vtab::ffi::sqlite3_index_constraint] = if n == 0 {
            &[]
        } else if a_constraint.is_null() || a_constraint_usage.is_null() {
            return SQLITE_ERROR;
        } else {
            core::slice::from_raw_parts(a_constraint, n)
        };
        let order_by = order_by_slice(n_order_by, a_order_by);
        let plan = select_best_index(vtab, p_info, constraints, order_by, col_used);

        // No more calls into `sqlite3_vtab_collation`: safe to materialize unique references.
        let info = &mut *p_info;
        let usages: &mut [crate::vtab::ffi::sqlite3_index_constraint_usage] = if n == 0 {
            &mut []
        } else {
            core::slice::from_raw_parts_mut(a_constraint_usage, n)
        };
        for slot in 0..plan.num_used {
            let idx = plan.used_constraints[slot];
            usages[idx].argvIndex = (slot + 1) as c_int;
            usages[idx].omit = 0;
        }
        info.idxNum = plan.idx_num;
        if let Some(flags) = plan.idx_flags {
            info.idxFlags = flags;
        }
        if plan.order_by_consumed {
            info.orderByConsumed = 1;
        }
        info.estimatedCost = plan.estimated_cost;
        info.estimatedRows = plan.estimated_rows;

        #[cfg(test)]
        record_best_index_plan(info, usages);
        SQLITE_OK
    }))
    .unwrap_or(SQLITE_ERROR)
}

unsafe fn select_best_index(
    vtab: &AydbVTab,
    p_info: *mut sqlite3_index_info,
    constraints: &[crate::vtab::ffi::sqlite3_index_constraint],
    order_by: &[crate::vtab::ffi::sqlite3_index_orderby],
    col_used: u64,
) -> SelectedBestIndexPlan {
    unsafe {
        let n = constraints.len();

        // Check if only `rowid` (`colUsed == 0`) or only a binary-collated PK column is projected
        let can_use_keys_only = col_used == 0
            || (vtab.pk_collation == ColCollation::Binary
                && vtab.pk_col_idx < 63
                && col_used == (1u64 << vtab.pk_col_idx));

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
                let mut pt_flags = if col == -1 { POINT_FLAG_BY_ROWID } else { 0 };
                if can_use_keys_only {
                    pt_flags |= POINT_FLAG_KEYS_ONLY;
                }
                // Do not promise at most one row when a TEXT or BLOB PK may fall back to a
                // cross-affinity full scan where `'1'`, `'01'`, and `'1.0'` all match `1`.
                let unique_scan = col == -1
                    || matches!(
                        vtab.pk_affinity,
                        ColumnAffinity::Integer | ColumnAffinity::Numeric | ColumnAffinity::Real
                    );
                let idx_flags = unique_scan.then_some(SQLITE_INDEX_SCAN_UNIQUE);
                let order_by_consumed = (unique_scan && !order_by.is_empty())
                    || order_by
                        .first()
                        .is_some_and(|ob| ob.iColumn == vtab.pk_col_idx as i32 && ob.desc == 0);
                return SelectedBestIndexPlan {
                    idx_num: PLAN_PK_POINT | (pt_flags << 8),
                    idx_flags,
                    order_by_consumed,
                    estimated_cost: 1.0,
                    estimated_rows: 1,
                    used_constraints: [i, 0],
                    num_used: 1,
                };
            }
        }

        // 2. Check for usable equality constraint (`=`) on a trusted persistent UNIQUE column
        if vtab.unique_ready.load(Ordering::Acquire) {
            let mut best_unique: Option<(usize, usize, bool)> = None;
            for i in 0..n {
                if constraints[i].usable == 0 || constraints[i].op != SQLITE_INDEX_CONSTRAINT_EQ {
                    continue;
                }
                let col = constraints[i].iColumn;
                if col < 0 {
                    continue;
                }
                let col_idx = col as usize;
                if col_idx >= 0x7FFF || vtab.col_unique.get(col_idx) != Some(&true) {
                    continue;
                }
                if !constraint_matches_col_collation(p_info, i, vtab.col_collations[col_idx]) {
                    continue;
                }
                let ob = eq_index_consumes_order_by(
                    order_by,
                    col_idx,
                    vtab.col_affinities[col_idx],
                    vtab.pk_col_idx,
                );
                if best_unique.is_none_or(|(_, _, prev_ob)| !prev_ob && ob) {
                    best_unique = Some((i, col_idx, ob));
                    if ob {
                        break;
                    }
                }
            }
            if let Some((i, col_idx, ob_consumed)) = best_unique {
                // Keep `omit = 0` so digest collisions and cross-affinity fallbacks filter exact values.
                let eq_flags = if can_use_keys_only {
                    RANGE_FLAG_KEYS_ONLY
                } else {
                    0
                };
                return SelectedBestIndexPlan {
                    idx_num: PLAN_UNIQUE_EQ
                        | (((col_idx as c_int) & 0xFFFF) << 8)
                        | ((eq_flags & 0x7F) << 24),
                    idx_flags: None,
                    order_by_consumed: ob_consumed,
                    estimated_cost: if ob_consumed { 1.8 } else { 2.0 },
                    estimated_rows: 1,
                    used_constraints: [i, 0],
                    num_used: 1,
                };
            }
        }

        // 3. Check for usable equality constraint (`=`) on a Secondary Index column
        let mut best_sec_eq: Option<(usize, usize, u16, bool)> = None;
        for i in 0..n {
            if constraints[i].usable == 0 || constraints[i].op != SQLITE_INDEX_CONSTRAINT_EQ {
                continue;
            }
            let col = constraints[i].iColumn;
            if col >= 0 {
                let col_idx = col as usize;
                if let Some(&(_, index_id)) =
                    vtab.secondary_indexes.iter().find(|&&(c, _)| c == col_idx)
                {
                    if !constraint_matches_col_collation(p_info, i, vtab.col_collations[col_idx]) {
                        continue;
                    }
                    let ob = eq_index_consumes_order_by(
                        order_by,
                        col_idx,
                        vtab.col_affinities[col_idx],
                        vtab.pk_col_idx,
                    );
                    if best_sec_eq.is_none_or(|(_, _, _, prev_ob)| !prev_ob && ob) {
                        best_sec_eq = Some((i, col_idx, index_id, ob));
                        if ob {
                            break;
                        }
                    }
                }
            }
        }
        if let Some((i, _, index_id, ob_consumed)) = best_sec_eq {
            // Do not set `omit = 1` on secondary index lookups so SQLite verifies the
            // exact column type and value (`NULL` vs `""` vs `X''`).
            let eq_flags = if can_use_keys_only {
                RANGE_FLAG_KEYS_ONLY
            } else {
                0
            };
            return SelectedBestIndexPlan {
                idx_num: PLAN_SECONDARY_EQ
                    | (((index_id as c_int) & 0xFFFF) << 8)
                    | ((eq_flags & 0x7F) << 24),
                idx_flags: None,
                order_by_consumed: ob_consumed,
                estimated_cost: if ob_consumed { 4.5 } else { 5.0 },
                estimated_rows: 4,
                used_constraints: [i, 0],
                num_used: 1,
            };
        }

        // 4. Compare usable range constraints (`>`, `>=`, `<`, `<=`) across PK and Secondary
        //    Indexes so bounded ranges beat one-sided ranges (`bounded PK < bounded secondary <
        //    one-sided PK < one-sided secondary`).
        struct RangeCandidate {
            is_pk: bool,
            index_id: u16,
            lower: Option<(usize, c_int)>,
            upper: Option<(usize, c_int)>,
            ob_consumed: bool,
            reverse: bool,
            cost: f64,
            rows: i64,
        }

        let mut best_range: Option<RangeCandidate> = None;

        let mut pk_lower: Option<(usize, c_int)> = None;
        let mut pk_upper: Option<(usize, c_int)> = None;
        for i in 0..n {
            if constraints[i].usable == 0 || constraints[i].iColumn != vtab.pk_col_idx as i32 {
                continue;
            }
            if !constraint_matches_col_collation(p_info, i, vtab.pk_collation) {
                continue;
            }
            match constraints[i].op {
                SQLITE_INDEX_CONSTRAINT_GE if pk_lower.is_none() => {
                    pk_lower = Some((i, RANGE_FLAG_LOWER_INCLUSIVE));
                }
                SQLITE_INDEX_CONSTRAINT_GT if pk_lower.is_none() => {
                    pk_lower = Some((i, RANGE_FLAG_LOWER_EXCLUSIVE));
                }
                SQLITE_INDEX_CONSTRAINT_LE if pk_upper.is_none() => {
                    pk_upper = Some((i, RANGE_FLAG_UPPER_INCLUSIVE));
                }
                SQLITE_INDEX_CONSTRAINT_LT if pk_upper.is_none() => {
                    pk_upper = Some((i, RANGE_FLAG_UPPER_EXCLUSIVE));
                }
                _ => {}
            }
        }
        let pk_ob = pk_order_by(order_by, vtab.pk_col_idx);
        if pk_lower.is_some() || pk_upper.is_some() {
            let bounded = pk_lower.is_some() && pk_upper.is_some();
            let cost = match (bounded, pk_ob.is_some()) {
                (true, true) => 14.0,
                (true, false) => 15.0,
                (false, true) => 75.0,
                (false, false) => 80.0,
            };
            best_range = Some(RangeCandidate {
                is_pk: true,
                index_id: 0,
                lower: pk_lower,
                upper: pk_upper,
                ob_consumed: pk_ob.is_some(),
                reverse: pk_ob.unwrap_or(false),
                cost,
                rows: if bounded { 16 } else { 128 },
            });
        }

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
                let bounded = sec_lower.is_some() && sec_upper.is_some();
                let ob_consumed =
                    sec_range_consumes_order_by(order_by, sec_col_idx, vtab.pk_col_idx);
                let cost = match (bounded, ob_consumed) {
                    (true, true) => 25.0,
                    (true, false) => 30.0,
                    (false, true) => 100.0,
                    (false, false) => 120.0,
                };
                if best_range.as_ref().is_none_or(|prev| cost < prev.cost) {
                    best_range = Some(RangeCandidate {
                        is_pk: false,
                        index_id,
                        lower: sec_lower,
                        upper: sec_upper,
                        ob_consumed,
                        reverse: false,
                        cost,
                        rows: if bounded { 16 } else { 128 },
                    });
                }
            }
        }

        if let Some(winner) = best_range {
            let mut flags = if can_use_keys_only {
                RANGE_FLAG_KEYS_ONLY
            } else {
                0
            };
            let mut used_constraints = [0usize; 2];
            let mut num_used = 0usize;
            if let Some((i, fl)) = winner.lower {
                used_constraints[num_used] = i;
                num_used += 1;
                flags |= fl;
            }
            if let Some((i, fl)) = winner.upper {
                used_constraints[num_used] = i;
                num_used += 1;
                flags |= fl;
            }
            if winner.ob_consumed && winner.reverse {
                flags |= RANGE_FLAG_REVERSE;
            }
            let idx_num = if winner.is_pk {
                PLAN_PK_RANGE | (flags << 8)
            } else {
                PLAN_SECONDARY_RANGE
                    | (((winner.index_id as c_int) & 0xFFFF) << 8)
                    | ((flags & 0x7F) << 24)
            };
            return SelectedBestIndexPlan {
                idx_num,
                idx_flags: None,
                order_by_consumed: winner.ob_consumed,
                estimated_cost: winner.cost,
                estimated_rows: winner.rows,
                used_constraints,
                num_used,
            };
        }

        // 5. Fallback: Full Table Scan (let SQLite's VDBE enforce LIMIT/OFFSET after any
        //    residual WHERE predicates like `LIKE`, `OR`, or expressions not passed in `aConstraint`)
        let mut full_scan_flags = if can_use_keys_only {
            RANGE_FLAG_KEYS_ONLY
        } else {
            0
        };
        if let Some(reverse) = pk_ob {
            if reverse {
                full_scan_flags |= RANGE_FLAG_REVERSE;
            }
        }
        SelectedBestIndexPlan {
            idx_num: PLAN_FULL_SCAN | (full_scan_flags << 8),
            idx_flags: None,
            order_by_consumed: pk_ob.is_some(),
            estimated_cost: 10_000.0,
            estimated_rows: 1_000,
            used_constraints: [0; 2],
            num_used: 0,
        }
    }
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
            rowids: Vec::new(),
            lazy_blobs: Vec::with_capacity(16),
            lazy_row_buf: Vec::new(),
            lazy_row_pos: None,
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
                drop(Box::from_raw(p_cursor as *mut AydbCursor));
            }
        }
        SQLITE_OK
    }))
    .unwrap_or(SQLITE_ERROR)
}

#[inline]
fn clear_cursor_buffers(cursor: &mut AydbCursor) {
    cursor.slab.clear();
    cursor.spans.clear();
    cursor.pks.clear();
    cursor.rowids.clear();
    cursor.lazy_blobs.clear();
    cursor.lazy_row_pos = None;
    cursor.lazy_row_buf.clear();
    if cursor.lazy_row_buf.capacity() > LAZY_ROW_BUF_SHRINK_THRESHOLD {
        cursor.lazy_row_buf.shrink_to(LAZY_ROW_BUF_SHRINK_THRESHOLD);
    }
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
}

#[allow(clippy::too_many_arguments)]
unsafe fn execute_exact_index_lookup(
    cursor: &mut AydbCursor,
    vtab: &AydbVTab,
    eng: &EngineReader<'_, OsFs>,
    col_idx: usize,
    index_id: u16,
    keys_only: bool,
    val_ptr: *mut sqlite3_value,
    is_unique: bool,
) -> c_int {
    unsafe {
        let val_type = sqlite3_value_type(val_ptr);
        if val_type == SQLITE_NULL
            || (val_type == SQLITE_FLOAT && sqlite3_value_double(val_ptr).is_nan())
        {
            return SQLITE_OK;
        }
        cursor.keys_only = keys_only;
        let col_aff = vtab
            .col_affinities
            .get(col_idx)
            .copied()
            .unwrap_or(ColumnAffinity::Blob);
        let col_coll = vtab
            .col_collations
            .get(col_idx)
            .copied()
            .unwrap_or(ColCollation::Binary);
        if is_cross_affinity_numeric_search(col_aff, val_type) {
            if let Err(e) = start_pk_full_scan(cursor, eng, vtab.table_id, keys_only, false) {
                return io_err_to_sqlite_rc(&e);
            }
            return SQLITE_OK;
        }
        let encoded = sqlite_value_to_key_bytes_with_schema(val_ptr, col_aff, col_coll, true);
        let lookup_key = if is_unique {
            let canon = unique_key::canon_unique_key(&encoded);
            unique_key::unique_index_component(&canon)
        } else {
            encoded
        };
        let prefix =
            crate::lru::encode_secondary_index_exact_prefix(vtab.table_id, index_id, &lookup_key);
        let next_last_sec = match eng.scan_secondary_index_into_slab_with_cache(
            vtab.table_id,
            index_id,
            &prefix,
            (&prefix, true),
            None,
            CURSOR_STREAM_CHUNK_ROWS,
            keys_only,
            true,
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
        SQLITE_OK
    }
}

unsafe fn execute_vtab_filter(
    cursor: &mut AydbCursor,
    vtab: &AydbVTab,
    eng: &EngineReader<'_, OsFs>,
    idx_num: c_int,
    argc: c_int,
    argv: *mut *mut sqlite3_value,
) -> c_int {
    clear_cursor_buffers(cursor);
    unsafe {
        let base_plan = idx_num & 0xFF;
        match base_plan {
            PLAN_PK_POINT => {
                if argc < 1 {
                    return SQLITE_ERROR;
                }
                let val_ptr = *argv.offset(0);
                let val_type = sqlite3_value_type(val_ptr);
                if val_type == SQLITE_NULL
                    || (val_type == SQLITE_FLOAT && sqlite3_value_double(val_ptr).is_nan())
                {
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
                    if is_cross_affinity_numeric_search(vtab.pk_affinity, val_type) {
                        if let Err(e) =
                            start_pk_full_scan(cursor, eng, vtab.table_id, pt_keys_only, false)
                        {
                            return io_err_to_sqlite_rc(&e);
                        }
                        return SQLITE_OK;
                    }
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
                    // On an `INTEGER PRIMARY KEY` rowid alias (`pk_rowid_alias`), column equality
                    // matches `rowid` (`SeekRowid`'s strict open interval `(-2^63, 2^63)`), so a
                    // 17-byte `-9223372036854775808.0` search key must not probe the 9-byte
                    // `i64::MIN` key; `INT PRIMARY KEY` (`!pk_rowid_alias`) and 9-byte `i64::MIN`
                    // searches still probe both numeric representations.
                    } else if !(vtab.pk_rowid_alias && pk_bytes.len() == 17)
                        && needs_numeric_alt_probe(&pk_bytes, vtab.pk_affinity)
                    {
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
                let index_id = ((idx_num >> 8) & 0xFFFF) as u16;
                let flags = (idx_num >> 24) & 0x7F;
                let keys_only = (flags & RANGE_FLAG_KEYS_ONLY) != 0;
                let sec_col_idx = vtab
                    .secondary_indexes
                    .iter()
                    .find(|&&(_, id)| id == index_id)
                    .map(|&(c, _)| c)
                    .unwrap_or(0);
                let rc = execute_exact_index_lookup(
                    cursor,
                    vtab,
                    eng,
                    sec_col_idx,
                    index_id,
                    keys_only,
                    *argv.offset(0),
                    false,
                );
                if rc != SQLITE_OK {
                    return rc;
                }
            }
            PLAN_UNIQUE_EQ => {
                if argc < 1 {
                    return SQLITE_ERROR;
                }
                let col_idx = ((idx_num >> 8) & 0xFFFF) as usize;
                let flags = (idx_num >> 24) & 0x7F;
                let keys_only = (flags & RANGE_FLAG_KEYS_ONLY) != 0;
                let index_id = unique_key::unique_index_id(col_idx);
                let rc = execute_exact_index_lookup(
                    cursor,
                    vtab,
                    eng,
                    col_idx,
                    index_id,
                    keys_only,
                    *argv.offset(0),
                    true,
                );
                if rc != SQLITE_OK {
                    return rc;
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
                let parsed_lower =
                    if (flags & (RANGE_FLAG_LOWER_INCLUSIVE | RANGE_FLAG_LOWER_EXCLUSIVE)) != 0
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
                        None
                    };
                let has_user_lower = parsed_lower.is_some();
                let lower_bound = parsed_lower.or_else(|| {
                    let mut min_non_null = Vec::with_capacity(6);
                    min_non_null.extend_from_slice(&crate::lru::encode_secondary_index_prefix(
                        vtab.table_id,
                        index_id,
                    ));
                    min_non_null.push(KEY_TAG_NULL + 1);
                    Some((min_non_null, true))
                });
                let upper_bound =
                    if (flags & (RANGE_FLAG_UPPER_INCLUSIVE | RANGE_FLAG_UPPER_EXCLUSIVE)) != 0
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
                let idx_prefix = crate::lru::encode_secondary_index_prefix(vtab.table_id, index_id);
                let (lo_slice, lo_inc) = lower_bound
                    .as_ref()
                    .map(|(b, inc)| (b.as_slice(), *inc))
                    .unwrap_or((&idx_prefix, true));
                let next_last_sec = match eng.scan_secondary_index_into_slab_with_cache(
                    vtab.table_id,
                    index_id,
                    &idx_prefix,
                    (lo_slice, lo_inc),
                    upper_bound.as_ref().map(|(b, inc)| (b.as_slice(), *inc)),
                    CURSOR_STREAM_CHUNK_ROWS,
                    keys_only,
                    has_user_lower && upper_bound.is_some(),
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
                let lower_bound =
                    if (flags & (RANGE_FLAG_LOWER_INCLUSIVE | RANGE_FLAG_LOWER_EXCLUSIVE)) != 0
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
                let upper_bound =
                    if (flags & (RANGE_FLAG_UPPER_INCLUSIVE | RANGE_FLAG_UPPER_EXCLUSIVE)) != 0
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
                if let Err(e) = eng.scan_range_into_slab_with_cache(
                    vtab.table_id,
                    lower_bound.as_ref().map(|(b, inc)| (b.as_slice(), *inc)),
                    upper_bound.as_ref().map(|(b, inc)| (b.as_slice(), *inc)),
                    reverse,
                    CURSOR_STREAM_CHUNK_ROWS,
                    keys_only,
                    lower_bound.is_some() && upper_bound.is_some(),
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
                if let Err(e) = start_pk_full_scan(cursor, eng, vtab.table_id, keys_only, reverse) {
                    return io_err_to_sqlite_rc(&e);
                }
            }
        }

        SQLITE_OK
    }
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
        clear_cursor_buffers(cursor);
        cursor.snapshot = None;

        let conn_id = vtab.db as usize;
        let snapshot = {
            let guard = match vtab.engine.read() {
                Ok(g) => g,
                Err(_) => return SQLITE_ERROR,
            };
            if !guard.is_vtab_token_active(vtab.table_id, vtab.reg_token, conn_id) {
                set_vtab_err_msg(vtab.base.get(), STALE_VTAB_ERR);
                return SQLITE_ERROR;
            }
            let snapshot = match vtab.read_session.for_cursor_locked(&guard, conn_id) {
                Ok(s) => s,
                Err(error) => return io_err_to_sqlite_rc(&error),
            };
            let need_exclusive = match snapshot.as_ref() {
                Some(_) => guard.snapshot_read_needs_exclusive(vtab.table_id),
                None => guard.vtab_read_needs_exclusive(conn_id, vtab.table_id),
            };
            if !need_exclusive {
                let rd = match snapshot.as_ref() {
                    Some(tok) => guard.reader_for_snapshot(tok),
                    None => guard.reader_for_conn(conn_id),
                };
                let rc = execute_vtab_filter(cursor, vtab, &rd, idx_num, argc, argv);
                if !rd.needs_exclusive_retry() {
                    cursor.snapshot = snapshot;
                    return rc;
                }
            }
            snapshot
        };

        let mut guard = match vtab.engine.write() {
            Ok(g) => g,
            Err(_) => return SQLITE_ERROR,
        };
        if snapshot.is_none() && !guard.is_vtab_token_active(vtab.table_id, vtab.reg_token, conn_id)
        {
            set_vtab_err_msg(vtab.base.get(), STALE_VTAB_ERR);
            return SQLITE_ERROR;
        }
        let rc = match snapshot.as_ref() {
            Some(tok) => guard.with_snapshot_read(tok, |rd| {
                execute_vtab_filter(cursor, vtab, rd, idx_num, argc, argv)
            }),
            None => guard.try_with_committed_read_for_conn(conn_id, |rd| {
                execute_vtab_filter(cursor, vtab, rd, idx_num, argc, argv)
            }),
        };
        let rc = match rc {
            Ok(rc) => rc,
            Err(e) => return io_err_to_sqlite_rc(&e),
        };
        cursor.snapshot = snapshot;
        rc
    }))
    .unwrap_or(SQLITE_ERROR)
}

unsafe extern "system" fn vtab_next(p_cursor: *mut sqlite3_vtab_cursor) -> c_int {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        let cursor = &mut *(p_cursor as *mut AydbCursor);
        cursor.pos += 1;
        cursor.lazy_row_pos = None;
        cursor.lazy_row_buf.clear();
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
            cursor.rowids.clear();
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
                let rc = match with_snapshot_read_access(
                    &vtab.engine,
                    conn_id,
                    vtab.table_id,
                    cursor.snapshot.as_ref(),
                    |eng| {
                        cursor.slab.clear();
                        cursor.spans.clear();
                        cursor.pks.clear();
                        cursor.rowids.clear();
                        cursor.lazy_blobs.clear();
                        match eng.scan_secondary_index_into_slab_with_cache(
                            vtab.table_id,
                            sec_index_id,
                            &cursor.stream_sec_prefix,
                            (last_pk.as_slice(), false),
                            upper_ref,
                            CURSOR_STREAM_CHUNK_ROWS,
                            keys_only,
                            false,
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
                        }
                    },
                ) {
                    Ok(rc) => rc,
                    Err(e) => {
                        cursor.streaming_active = false;
                        return io_err_to_sqlite_rc(&e);
                    }
                };
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

            let rc = match with_snapshot_read_access(
                &vtab.engine,
                conn_id,
                vtab.table_id,
                cursor.snapshot.as_ref(),
                |eng| {
                    cursor.slab.clear();
                    cursor.spans.clear();
                    cursor.pks.clear();
                    cursor.rowids.clear();
                    cursor.lazy_blobs.clear();
                    if let Err(e) = eng.scan_range_into_slab_with_cache(
                        vtab.table_id,
                        lower_ref,
                        upper_ref,
                        reverse,
                        CURSOR_STREAM_CHUNK_ROWS,
                        keys_only,
                        false,
                        &mut cursor.pks,
                        &mut cursor.slab,
                        &mut cursor.spans,
                        Some(&mut cursor.lazy_blobs),
                    ) {
                        return io_err_to_sqlite_rc(&e);
                    }
                    SQLITE_OK
                },
            ) {
                Ok(rc) => rc,
                Err(e) => {
                    cursor.streaming_active = false;
                    return io_err_to_sqlite_rc(&e);
                }
            };
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

/// Resolves the SQLite `rowid` for `cursor.pos`, batch-populating `cursor.rowids` once per chunk
/// for non-`INTEGER PRIMARY KEY` tables instead of reacquiring the engine lock on every row.
fn resolve_cursor_rowid(cursor: &mut AydbCursor, vtab: &AydbVTab) -> io::Result<i64> {
    if vtab.pk_rowid_alias {
        if let Some(pk_bytes) = cursor.pks.get(cursor.pos) {
            if let Some(id) = decode_i64_tagged_key(pk_bytes) {
                return Ok(id);
            }
        }
    } else if !cursor.pks.is_empty() {
        if cursor.rowids.len() != cursor.pks.len() {
            with_snapshot_read_access(
                &vtab.engine,
                vtab.db as usize,
                vtab.table_id,
                cursor.snapshot.as_ref(),
                |eng| eng.rowids_for_pks(vtab.table_id, &cursor.pks, &mut cursor.rowids),
            )??;
        }
        if let Some(Some(rid)) = cursor.rowids.get(cursor.pos) {
            return Ok(*rid);
        }
    }
    if let Some(pk_bytes) = cursor.pks.get(cursor.pos) {
        extract_rowid_from_pk_bytes(pk_bytes, vtab, cursor.snapshot.as_ref())
    } else if cursor.lazy_row_pos == Some(cursor.pos) {
        extract_rowid_from_row(&cursor.lazy_row_buf, vtab, cursor.snapshot.as_ref())
    } else {
        let (start, end) = cursor.spans[cursor.pos];
        extract_rowid_from_row(&cursor.slab[start..end], vtab, cursor.snapshot.as_ref())
    }
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
            if i_col < 0 {
                let rid = match resolve_cursor_rowid(cursor, vtab) {
                    Ok(rid) => rid,
                    Err(e) => return io_err_to_sqlite_rc(&e),
                };
                sqlite3_result_int64(p_ctx, rid);
                return SQLITE_OK;
            }
            let pk_bytes = &cursor.pks[cursor.pos];
            if i_col as usize == vtab.pk_col_idx {
                if project_key_zero_copy(pk_bytes, p_ctx) {
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
            let rid = match resolve_cursor_rowid(cursor, vtab) {
                Ok(rid) => rid,
                Err(e) => return io_err_to_sqlite_rc(&e),
            };
            sqlite3_result_int64(p_ctx, rid);
            return SQLITE_OK;
        }

        // Fast path 2: Primary Key column projection with Binary collation directly from cursor.pks
        if i_col as usize == vtab.pk_col_idx && vtab.pk_collation == ColCollation::Binary {
            if let Some(pk_bytes) = cursor.pks.get(cursor.pos) {
                if project_key_zero_copy(pk_bytes, p_ctx) {
                    return SQLITE_OK;
                }
            }
        }

        // Lazy Blob Dereference: materialize the active out-of-line row into `lazy_row_buf`
        // rather than accumulating all 256 chunk rows' blobs inside `slab`.
        let row_bytes: &[u8] = if cursor.lazy_row_pos == Some(cursor.pos) {
            &cursor.lazy_row_buf
        } else if let Some(lazy_blob) = cursor
            .lazy_blobs
            .get(cursor.pos)
            .and_then(|o| o.as_ref())
            .cloned()
        {
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

            let deref_res = match with_snapshot_read_access(
                &vtab.engine,
                conn_id,
                vtab.table_id,
                cursor.snapshot.as_ref(),
                |eng| {
                    if upcoming.len() > 1 {
                        let _ = eng.prefetch_lazy_blobs(vtab.table_id, &upcoming);
                    }
                    cursor.lazy_row_buf.clear();
                    eng.dereference_lazy_blob_into(
                        vtab.table_id,
                        pk_bytes,
                        &lazy_blob,
                        &mut cursor.lazy_row_buf,
                    )
                },
            ) {
                Ok(res) => res,
                Err(e) => {
                    cursor.lazy_row_buf.clear();
                    let rc = io_err_to_sqlite_rc(&e);
                    sqlite3_result_error_code(p_ctx, rc);
                    return rc;
                }
            };
            match deref_res {
                Ok(Some((start, end))) => {
                    debug_assert_eq!(start, 0);
                    cursor.lazy_row_pos = Some(cursor.pos);
                    cursor.lazy_blobs[cursor.pos] = None;
                    &cursor.lazy_row_buf[start..end]
                }
                Ok(None) => {
                    cursor.lazy_row_buf.clear();
                    sqlite3_result_error_code(p_ctx, SQLITE_CORRUPT);
                    return SQLITE_CORRUPT;
                }
                Err(e) => {
                    cursor.lazy_row_buf.clear();
                    let rc = io_err_to_sqlite_rc(&e);
                    sqlite3_result_error_code(p_ctx, rc);
                    return rc;
                }
            }
        } else {
            let (start, end) = cursor.spans[cursor.pos];
            &cursor.slab[start..end]
        };

        #[cfg(test)]
        {
            let slab_len = cursor.slab.len();
            CURSOR_SLAB_MAX_LEN.with(|m| m.set(m.get().max(slab_len)));
            let lazy_len = cursor.lazy_row_buf.len();
            CURSOR_LAZY_ROW_BUF_MAX_LEN.with(|m| m.set(m.get().max(lazy_len)));
        }

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
        let cursor = &mut *(p_cursor as *mut AydbCursor);
        let vtab = &*(cursor.base.pVtab as *const AydbVTab);
        let len = if cursor.keys_only {
            cursor.pks.len()
        } else {
            cursor.spans.len()
        };
        if cursor.pos >= len {
            return SQLITE_ERROR;
        }
        *p_rowid = match resolve_cursor_rowid(cursor, vtab) {
            Ok(rid) => rid,
            Err(e) => return io_err_to_sqlite_rc(&e),
        };
        SQLITE_OK
    }))
    .unwrap_or(SQLITE_ERROR)
}

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
        if !eng.is_vtab_token_active(vtab.table_id, vtab.reg_token, vtab.db as usize) {
            set_vtab_err_msg(vtab.base.get(), STALE_VTAB_ERR);
            return SQLITE_ERROR;
        }
        vtab.pending_ddl.clear_tx_commit();
        // When `xBegin` skipped migration (for example because the connection held a read snapshot
        // before upgrading to write), remove any stale UNIQUE completion marker transactionally now
        // so removing all UNIQUE constraints cannot leave an obsolete marker behind.
        if vtab.unique_def.is_empty() && !vtab.unique_ready.load(Ordering::Acquire) {
            match eng.unique_marker_view(crate::engine::ReadView::Owner, vtab.table_id) {
                Ok(Some(_)) => {
                    if let Err(e) = eng.remove_unique_marker(vtab.table_id, vtab.durability) {
                        return io_err_to_sqlite_rc(&e);
                    }
                    if !eng.in_active_transaction() {
                        vtab.unique_ready.store(true, Ordering::Release);
                    }
                }
                Ok(None) => {
                    if !eng.in_active_transaction() {
                        vtab.unique_ready.store(true, Ordering::Release);
                    }
                }
                Err(e) => return io_err_to_sqlite_rc(&e),
            }
        }

        let needs_synthetic_delete_check =
            |pk: &[u8]| -> bool { !(vtab.pk_rowid_alias && decode_i64_tagged_key(pk).is_some()) };

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
                    let check_syn = needs_synthetic_delete_check(&old_pk);
                    if let Err(e) = eng.delete_row_with_rowid_hint(
                        vtab.table_id,
                        &old_pk,
                        vtab.durability,
                        check_syn,
                    ) {
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
                let check_syn = needs_synthetic_delete_check(&old_pk);
                if let Err(e) = eng.delete_row_with_rowid_hint(
                    vtab.table_id,
                    &old_pk,
                    vtab.durability,
                    check_syn,
                ) {
                    return io_err_to_sqlite_rc(&e);
                }
            }
            return SQLITE_OK;
        }

        // Case 2: INSERT or UPDATE (`argc == 2 + num_cols`)
        if let Some(reason) = vtab.unsupported {
            // A row cannot be checked (CHECK) or completed (DEFAULT), so none may be stored.
            // SQLITE_CONSTRAINT would be resolved by the statement's conflict mode (OR IGNORE
            // would skip the row and report success, OR REPLACE would delete conflicting rows
            // first), so this is a plain error that every mode reports as such.
            set_vtab_err_msg(vtab.base.get(), reason);
            return SQLITE_ERROR;
        }
        let num_cols = vtab.col_names.len();
        if (argc as usize) < 2 + num_cols {
            return SQLITE_ERROR;
        }

        let arg0 = *argv.offset(0);
        let is_insert = sqlite3_value_type(arg0) == SQLITE_NULL;
        // A native rowid alias refuses to become NULL in an UPDATE ("datatype mismatch"), a hard
        // error that no conflict clause turns into a skipped row.
        if !is_insert
            && vtab.pk_rowid_alias
            && sqlite3_value_type(*argv.offset(2 + vtab.pk_col_idx as isize)) == SQLITE_NULL
        {
            return SQLITE_MISMATCH;
        }
        for (idx, &not_null) in vtab.col_not_null.iter().enumerate() {
            if not_null
                && !(idx == vtab.pk_col_idx && is_insert && vtab.pk_rowid_alias)
                && sqlite3_value_type(*argv.offset(2 + idx as isize)) == SQLITE_NULL
            {
                // NOT NULL has no REPLACE resolution without a DEFAULT, which this adapter
                // does not support, so SQLite handles every mode as the statement requires.
                return SQLITE_CONSTRAINT;
            }
        }
        let pk_val_ptr = *argv.offset(2 + vtab.pk_col_idx as isize);
        let pk_val_type = sqlite3_value_type(pk_val_ptr);

        // `argv[1]` is the rowid the statement asks for: the explicit rowid of an INSERT, or the
        // new rowid of an UPDATE, which equals the old one unless the statement changed it.
        // `new_rowid_req` is set only when the statement really asks for a rowid.
        let new_rowid_arg = *argv.offset(1);
        let old_rowid_arg = if is_insert {
            None
        } else {
            extract_lossless_rowid_arg(arg0)
        };
        let new_rowid_req = if sqlite3_value_type(new_rowid_arg) == SQLITE_NULL {
            None
        } else {
            match extract_lossless_rowid_arg(new_rowid_arg) {
                Some(rowid) if is_insert || old_rowid_arg != Some(rowid) => Some(rowid),
                Some(_) => None,
                None => return SQLITE_MISMATCH,
            }
        };
        // `INTEGER PRIMARY KEY` is the rowid: a rowid without a key of its own sets the key
        // (INSERT with a NULL key), and an UPDATE that leaves the key alone moves it. An
        // explicit key wins over the rowid, as on a native table.
        let alias_rowid = match new_rowid_req {
            Some(rowid) if vtab.pk_rowid_alias => {
                if is_insert {
                    (pk_val_type == SQLITE_NULL).then_some(rowid)
                } else {
                    (pk_val_type == SQLITE_INTEGER
                        && Some(sqlite3_value_int64(pk_val_ptr)) == old_rowid_arg)
                        .then_some(rowid)
                }
            }
            _ => None,
        };
        let mut auto_assigned = false;
        let mut cache_next_key: Option<i64> = None;

        // Handle `SQLITE_NULL` primary key
        let pk_override = if alias_rowid.is_some() {
            alias_rowid
        } else if pk_val_type == SQLITE_NULL {
            if is_insert && vtab.pk_rowid_alias {
                // Like a native table: the largest key at this moment plus one (1 for an empty
                // table). Nothing is remembered between inserts, so a failed statement, a
                // rolled back transaction or savepoint, a delete, or another connection's
                // insert never leave a gap or a duplicate. At `i64::MAX` the random search
                // below takes over.
                //
                // The previous automatic insert of this table left `max + 1` behind together
                // with the table's mutation epoch. While the epoch is unchanged nothing modified
                // this table's rows or rowids since, so that value still is `max + 1`.
                let table_epoch = eng.table_mutation_epoch(vtab.table_id);
                let cached = vtab
                    .auto_key_cache
                    .lock()
                    .ok()
                    .and_then(|guard| *guard)
                    .filter(|(epoch, _)| *epoch == table_epoch)
                    .map(|(_, next)| next);
                let mut candidate = match cached {
                    Some(next) => next,
                    None => match max_integer_rowid(&mut eng, vtab.db as usize, vtab.table_id) {
                        Ok(None) => 1,
                        Ok(Some(max)) => max.saturating_add(1),
                        Err(e) => return io_err_to_sqlite_rc(&e),
                    },
                };
                let first_candidate = candidate;
                let mut found_free = None;
                // 1. Bounded sequential probe using checked arithmetic. It also steps over a
                //    number that a synthetic rowid already owns.
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
                auto_assigned = true;
                // Only a key that is `max + 1` can be remembered as such: not a number found by
                // stepping over synthetic rowids, and not one from the random search.
                if chosen == first_candidate {
                    cache_next_key = chosen.checked_add(1);
                }
                Some(chosen)
            } else {
                return SQLITE_CONSTRAINT;
            }
        } else {
            None
        };

        let (new_pk, row_bytes, mut sec_keys) = match try_encode_sqlite_values_direct(
            argv.offset(2),
            num_cols,
            vtab.pk_col_idx,
            pk_override,
            &vtab.col_affinities,
            &vtab.col_collations,
            &vtab.secondary_indexes,
        ) {
            Ok(row) => row,
            Err(e) if e.kind() == io::ErrorKind::FileTooLarge => return ffi::SQLITE_TOOBIG,
            Err(e) if e.kind() == io::ErrorKind::OutOfMemory => return ffi::SQLITE_NOMEM,
            Err(e) => return io_err_to_sqlite_rc(&e),
        };

        let coerced_int_pk = if vtab.pk_rowid_alias {
            decode_i64_tagged_key(&new_pk)
        } else {
            None
        };
        // Only a true `INTEGER PRIMARY KEY` rowid alias derives its rowid from an integer key;
        // every other key (including non-alias `INT PRIMARY KEY`) keeps an independent persistent
        // rowid, where an explicit rowid is stored as requested.
        let mapped_rowid = if coerced_int_pk.is_none() {
            new_rowid_req
        } else {
            None
        };
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

        // Integer rowid aliases own their exact integer as a rowid; a previously allocated
        // persistent identity owns its number until its row is deleted.
        let rowid_conflict_pk = if let Some(rowid) = coerced_int_pk.or(mapped_rowid) {
            let implicit_int_owner = if vtab.pk_rowid_alias && coerced_int_pk.is_none() {
                let int_pk = encode_i64_tagged_key(rowid);
                if old_pk_opt.as_deref() != Some(int_pk.as_slice())
                    && new_pk.as_slice() != int_pk.as_slice()
                {
                    match eng.row_exists(vtab.table_id, &int_pk) {
                        Ok(true) => Some(int_pk.to_vec()),
                        Ok(false) => None,
                        Err(e) => return io_err_to_sqlite_rc(&e),
                    }
                } else {
                    None
                }
            } else {
                None
            };
            if implicit_int_owner.is_some() {
                implicit_int_owner
            } else {
                match eng.pk_for_rowid_view(crate::engine::ReadView::Owner, vtab.table_id, rowid) {
                    // The row that already owns the rowid is no conflict when it is the row being
                    // written or the row being updated.
                    Ok(Some(pk))
                        if old_pk_opt.as_ref().is_none_or(|old| *old != pk)
                            && (mapped_rowid.is_none() || pk != new_pk) =>
                    {
                        Some(pk)
                    }
                    Ok(_) => None,
                    Err(e) => return io_err_to_sqlite_rc(&e),
                }
            }
        } else {
            None
        };
        if rowid_conflict_pk.is_some() && sqlite3_vtab_on_conflict(vtab.db) != SQLITE_REPLACE {
            return SQLITE_CONSTRAINT;
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
        let mut known_absent = auto_assigned;
        let mut pk_conflict_to_delete: Option<Vec<u8>> = None;
        let unique_probes = if vtab.unique_def.is_empty() {
            Vec::new()
        } else {
            match vtab.unique_def.probes(&row_bytes) {
                Ok(p) => p,
                Err(e) => return io_err_to_sqlite_rc(&e),
            }
        };
        let unique_conflicts = match constraints::find_unique_conflicts(
            &mut eng,
            vtab,
            &unique_probes,
            old_pk_opt.as_deref(),
        ) {
            Ok(conflicts) => conflicts,
            Err(e) => return io_err_to_sqlite_rc(&e),
        };
        if !unique_conflicts.is_empty() {
            if sqlite3_vtab_on_conflict(vtab.db) != SQLITE_REPLACE {
                return SQLITE_CONSTRAINT;
            }
        }
        if (is_insert || !same_pk) && !auto_assigned {
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
                if sqlite3_vtab_on_conflict(vtab.db) != SQLITE_REPLACE {
                    return SQLITE_CONSTRAINT;
                }
                if conflict_pk.as_slice() != new_pk.as_slice() {
                    pk_conflict_to_delete = Some(conflict_pk);
                    known_absent = true;
                }
            } else {
                known_absent = true;
            }
        }
        if !unique_probes.is_empty() {
            sec_keys.extend(unique_probes.into_iter().map(|p| (p.index_id, p.component)));
        }

        // A row whose key changes keeps its rowid, as on a native table. SQLite addresses the
        // rows it still has queued by their old rowids, so a rowid that moved to another row
        // (or a new one derived from the new key) would make them hit the wrong row.
        let kept_rowid = old_rowid_arg.filter(|_| {
            old_pk_opt
                .as_ref()
                .is_some_and(|old| old.as_slice() != new_pk.as_slice())
        });

        // Everything below mutates the engine. A statement that removes rows (REPLACE conflict
        // resolution, or a PK-changing UPDATE) and then fails to store the new row must leave
        // every row and mapping intact. SQLite cannot be relied on for that: it opens a statement
        // journal only for statements that may write several rows, so a single-row statement that
        // fails inside `BEGIN` keeps whatever `xUpdate` already did. Rowid allocation or updates
        // are folded into the single atomic `put_row_owned_with_hint_and_extra` WAL batch so
        // inserts and in-place updates never need a per-row statement savepoint.
        let in_place_same_pk = !is_insert && old_pk_opt.as_deref() == Some(new_pk.as_slice());
        let removes_rows = pk_conflict_to_delete.is_some()
            || !unique_conflicts.is_empty()
            || rowid_conflict_pk.is_some()
            || old_pk_opt
                .as_ref()
                .is_some_and(|old_pk| old_pk.as_slice() != new_pk.as_slice());
        if removes_rows {
            if let Err(e) = eng.statement_savepoint_for_conn(vtab.db as usize, vtab.table_id) {
                return io_err_to_sqlite_rc(&e);
            }
        }
        let applied = (|| -> io::Result<i64> {
            for conflict_pk in pk_conflict_to_delete.into_iter().chain(unique_conflicts) {
                let check_syn = needs_synthetic_delete_check(&conflict_pk);
                eng.delete_row_with_rowid_hint(
                    vtab.table_id,
                    &conflict_pk,
                    vtab.durability,
                    check_syn,
                )?;
            }

            if let Some(conflict_pk) = rowid_conflict_pk {
                let check_syn = needs_synthetic_delete_check(&conflict_pk);
                eng.delete_row_with_rowid_hint(
                    vtab.table_id,
                    &conflict_pk,
                    vtab.durability,
                    check_syn,
                )?;
            }

            // If UPDATE changed the physical PK bytes, delete the old PK row
            if let Some(old_pk) = old_pk_opt {
                if old_pk.as_slice() != new_pk.as_slice() {
                    let check_syn = needs_synthetic_delete_check(&old_pk);
                    eng.delete_row_with_rowid_hint(
                        vtab.table_id,
                        &old_pk,
                        vtab.durability,
                        check_syn,
                    )?;
                    if same_pk {
                        known_absent = true;
                    }
                }
            }

            let (rid, extra_records) = if let Some(auto_id) = pk_override {
                (auto_id, Vec::new())
            } else if let Some(int_id) = coerced_int_pk {
                (int_id, Vec::new())
            } else if let Some(rowid) = mapped_rowid.or(kept_rowid) {
                let extra = eng.plan_set_rowid(vtab.table_id, &new_pk, rowid)?;
                (rowid, extra)
            } else if in_place_same_pk {
                if let Some(old_rid) = old_rowid_arg {
                    (old_rid, Vec::new())
                } else {
                    rowid_map::plan_synthetic_rowid(
                        &mut eng,
                        vtab.table_id,
                        &new_pk,
                        vtab.pk_rowid_alias,
                    )?
                }
            } else {
                rowid_map::plan_synthetic_rowid(
                    &mut eng,
                    vtab.table_id,
                    &new_pk,
                    vtab.pk_rowid_alias,
                )?
            };

            eng.put_row_owned_with_hint_and_extra(
                vtab.table_id,
                &new_pk,
                row_bytes,
                &sec_keys,
                extra_records,
                vtab.durability,
                known_absent,
            )?;

            Ok(rid)
        })();
        if removes_rows {
            if applied.is_err() {
                let _ = eng.rollback_to_savepoint_for_conn(vtab.db as usize, STATEMENT_SAVEPOINT);
                let _ = eng.release_savepoint_for_conn(vtab.db as usize, STATEMENT_SAVEPOINT);
            } else if let Err(e) =
                eng.release_savepoint_for_conn(vtab.db as usize, STATEMENT_SAVEPOINT)
            {
                return io_err_to_sqlite_rc(&e);
            }
        }
        let rid = match applied {
            Ok(rid) => rid,
            Err(e) => return io_err_to_sqlite_rc(&e),
        };
        if let (Some(next), Ok(mut cache)) = (cache_next_key, vtab.auto_key_cache.lock()) {
            // After this insert's own writes: anything touching this table later moves its epoch.
            *cache = Some((eng.table_mutation_epoch(vtab.table_id), next));
        }
        if !p_rowid.is_null() {
            *p_rowid = rid;
        }
        SQLITE_OK
    }))
    .unwrap_or(SQLITE_ERROR)
}

/// Builds the UNIQUE index of a table whose stored index does not match its declared UNIQUE
/// columns, a bounded amount per call (`unique_migration::ROWS_PER_BEGIN`). It runs from
/// `xBegin` before the engine transaction starts and never fails the statement: with a
/// transaction open, or after a failure, the conflict check simply keeps using the scan.
///
/// A connection that holds a read snapshot skips the migration too: its commits would make the
/// snapshot stale and fail the write upgrade that follows. A failure (an unreadable row, an I/O
/// error) is counted as a maintenance failure of the engine and stops further attempts by this
/// connection: re-reading the table up to the bad row at every transaction would tax every
/// write, and a reconnect tries again.
fn migrate_unique_index_before_begin(vtab: &AydbVTab) {
    if vtab.unique_ready.load(Ordering::Acquire)
        || vtab.unique_migration_failed.load(Ordering::Relaxed)
    {
        return;
    }
    #[cfg(test)]
    if UNIQUE_MIGRATION_DISABLED.with(|flag| flag.get()) {
        return;
    }
    if vtab.read_session.current().is_some() {
        return;
    }
    let Ok(mut cursor) = vtab.unique_cursor.lock() else {
        return;
    };
    let params = unique_migration::Params {
        table_id: vtab.table_id,
        vtab_token: Some((vtab.reg_token, vtab.db as usize)),
        durability: vtab.durability,
        def: &vtab.unique_def,
        chunk_rows: unique_migration::CHUNK_ROWS,
        budget_rows: unique_migration::ROWS_PER_BEGIN,
    };
    match unique_migration::migrate(&vtab.engine, &params, &mut cursor) {
        unique_migration::Outcome::Complete => vtab.unique_ready.store(true, Ordering::Release),
        unique_migration::Outcome::Incomplete | unique_migration::Outcome::Skipped => {}
        unique_migration::Outcome::Failed(e) => {
            vtab.unique_migration_failed.store(true, Ordering::Relaxed);
            if let Ok(eng) = vtab.engine.read() {
                eng.record_maintenance_failure(&e);
            }
        }
    }
}

#[cfg(test)]
thread_local! {
    /// Test seam: keeps `xBegin` from migrating, so a test can drive the chunks itself.
    static UNIQUE_MIGRATION_DISABLED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

unsafe extern "system" fn vtab_begin_tx(p_vtab: *mut sqlite3_vtab) -> c_int {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        let vtab = &*(p_vtab as *const AydbVTab);
        vtab.pending_ddl.clear_tx_commit();
        {
            let eng = match vtab.engine.read() {
                Ok(g) => g,
                Err(_) => return SQLITE_ERROR,
            };
            if !eng.is_vtab_token_active(vtab.table_id, vtab.reg_token, vtab.db as usize) {
                set_vtab_err_msg(vtab.base.get(), STALE_VTAB_ERR);
                return SQLITE_ERROR;
            }
        }
        migrate_unique_index_before_begin(vtab);
        // Wait for a transaction on another connection to end. The wait is bounded because that
        // connection may be held open by the same thread, which could never end it.
        let mut deadline = None;
        loop {
            let mut eng = match vtab.engine.write() {
                Ok(g) => g,
                Err(_) => return SQLITE_ERROR,
            };
            if !eng.is_vtab_token_active(vtab.table_id, vtab.reg_token, vtab.db as usize) {
                set_vtab_err_msg(vtab.base.get(), STALE_VTAB_ERR);
                return SQLITE_ERROR;
            }
            if let Some(snapshot) = vtab.read_session.current() {
                if let Err(error) = eng.check_snapshot_current_for_upgrade(&snapshot) {
                    return io_err_to_sqlite_rc(&error);
                }
            }
            match eng.begin_vtab_transaction(vtab.db as usize, p_vtab as usize) {
                Ok(()) => {
                    vtab.read_session.clear();
                    break;
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    if !wait_for_other_transaction(eng, &mut deadline) {
                        return io_err_to_sqlite_rc(&e);
                    }
                }
                Err(e) => return io_err_to_sqlite_rc(&e),
            }
        }
        SQLITE_OK
    }))
    .unwrap_or(SQLITE_ERROR)
}

/// Drops any tables created in the active transaction that are no longer `aydb` virtual tables
/// in SQLite's schema (for instance after a `ROLLBACK TO` that preceded the table's creation).
/// Releases `eng` before running internal `sqlite_schema` queries because `sqlite3_prepare_v2`
/// can trigger schema re-parsing (`xConnect`) that acquires `engine.write()`.
unsafe fn prune_stale_created_vtabs_unlocked<'a>(
    db: *mut sqlite3,
    engine: &'a Arc<RwLock<StorageEngine<OsFs>>>,
    conn_id: usize,
    mut eng: std::sync::RwLockWriteGuard<'a, StorageEngine<OsFs>>,
) -> io::Result<std::sync::RwLockWriteGuard<'a, StorageEngine<OsFs>>> {
    let candidates = eng.vtabs_created_in_tx_snapshot();
    if candidates.is_empty() {
        return Ok(eng);
    }
    drop(eng);
    let mut missing = Vec::new();
    for (_, name, prev_db) in candidates {
        if unsafe { !schema_has_aydb_table(db, &prev_db, &name) } {
            missing.push((prev_db, name));
        }
    }
    eng = engine
        .write()
        .map_err(|_| io::Error::other("engine lock poisoned"))?;
    if !missing.is_empty() && eng.conn_has_open_transaction(conn_id) {
        let stale = eng.take_stale_vtabs_created_in_tx(|prev_db, prev_name| {
            missing.iter().any(|(d, n)| d == prev_db && n == prev_name)
        });
        for (id, stale_name) in stale {
            eng.drop_stale_created_vtab(id, &stale_name)?;
        }
    }
    Ok(eng)
}

/// Drops entries in `vtabs_created_in_tx` matching `table_name` or `table_id` that collide with
/// `(cur_norm_db, table_name)` (excluding `exclude_table_id`) or are absent from SQLite's `aydb`
/// schema, releasing `eng` around any `sqlite_schema` queries.
#[allow(clippy::too_many_arguments)]
unsafe fn prune_matching_created_vtabs_unlocked<'a>(
    db: *mut sqlite3,
    engine: &'a Arc<RwLock<StorageEngine<OsFs>>>,
    conn_id: usize,
    mut eng: std::sync::RwLockWriteGuard<'a, StorageEngine<OsFs>>,
    table_id: Option<u16>,
    table_name: &str,
    cur_norm_db: &str,
    exclude_table_id: Option<u16>,
) -> io::Result<std::sync::RwLockWriteGuard<'a, StorageEngine<OsFs>>> {
    let cur_norm_name = crate::engine::normalize_vtab_ident(table_name);
    let matching: Vec<_> = eng
        .vtabs_created_in_tx_snapshot()
        .into_iter()
        .filter(|(id, name, _)| *name == cur_norm_name || Some(*id) == table_id)
        .collect();
    if matching.is_empty() {
        return Ok(eng);
    }
    let to_probe: Vec<_> = matching
        .into_iter()
        .filter(|(id, name, prev_db)| {
            !(Some(*id) != exclude_table_id && prev_db == cur_norm_db && *name == cur_norm_name)
        })
        .collect();
    let mut probed_missing = Vec::new();
    if !to_probe.is_empty() {
        drop(eng);
        for (_, name, prev_db) in to_probe {
            if unsafe { !schema_has_aydb_table(db, &prev_db, &name) } {
                probed_missing.push((prev_db, name));
            }
        }
        eng = engine
            .write()
            .map_err(|_| io::Error::other("engine lock poisoned"))?;
    }
    if eng.conn_has_open_transaction(conn_id) {
        let stale =
            eng.take_vtabs_created_in_tx(table_id, table_name, |prev_id, prev_db, prev_name| {
                (Some(prev_id) != exclude_table_id
                    && prev_db == cur_norm_db
                    && prev_name == cur_norm_name)
                    || probed_missing
                        .iter()
                        .any(|(d, n)| d == prev_db && n == prev_name)
            });
        for (id, stale_name) in stale {
            eng.drop_stale_created_vtab(id, &stale_name)?;
        }
    }
    Ok(eng)
}

struct SqlScanner<'a> {
    rest: &'a str,
}

impl<'a> SqlScanner<'a> {
    fn skip_ws_and_comments(&mut self) {
        loop {
            self.rest = self
                .rest
                .trim_start_matches(|c: char| c.is_ascii_whitespace());
            if let Some(after) = self.rest.strip_prefix("--") {
                self.rest = match after.find('\n') {
                    Some(idx) => &after[idx + 1..],
                    None => "",
                };
            } else if let Some(after) = self.rest.strip_prefix("/*") {
                self.rest = match after.find("*/") {
                    Some(idx) => &after[idx + 2..],
                    None => "",
                };
            } else {
                break;
            }
        }
    }

    fn scan_bare_token(&mut self) -> Option<&'a str> {
        self.skip_ws_and_comments();
        let len = self
            .rest
            .char_indices()
            .find(|&(_, c)| !(c.is_ascii_alphanumeric() || c == '_' || c == '$' || !c.is_ascii()))
            .map_or(self.rest.len(), |(i, _)| i);
        if len == 0 {
            return None;
        }
        let tok = &self.rest[..len];
        self.rest = &self.rest[len..];
        Some(tok)
    }

    fn next_keyword(&mut self) -> Option<&'a str> {
        self.scan_bare_token()
    }

    fn try_skip_if_not_exists(&mut self) {
        let saved = self.rest;
        if self
            .next_keyword()
            .is_some_and(|w| w.eq_ignore_ascii_case("IF"))
            && self
                .next_keyword()
                .is_some_and(|w| w.eq_ignore_ascii_case("NOT"))
            && self
                .next_keyword()
                .is_some_and(|w| w.eq_ignore_ascii_case("EXISTS"))
        {
            return;
        }
        self.rest = saved;
    }

    fn next_ident(&mut self) -> Option<String> {
        self.skip_ws_and_comments();
        let mut chars = self.rest.char_indices().peekable();
        let (_, first) = chars.next()?;
        match first {
            '"' | '`' | '\'' => {
                let quote = first;
                let mut out = String::new();
                while let Some((i, ch)) = chars.next() {
                    if ch == quote {
                        if let Some((_, next_ch)) = chars.peek().copied() {
                            if next_ch == quote {
                                chars.next();
                                out.push(quote);
                                continue;
                            }
                        }
                        self.rest = &self.rest[i + ch.len_utf8()..];
                        return Some(out);
                    }
                    out.push(ch);
                }
                None
            }
            '[' => {
                let mut out = String::new();
                for (i, ch) in chars {
                    if ch == ']' {
                        self.rest = &self.rest[i + 1..];
                        return Some(out);
                    }
                    out.push(ch);
                }
                None
            }
            _ => self.scan_bare_token().map(str::to_string),
        }
    }

    fn consume_dot(&mut self) -> bool {
        self.skip_ws_and_comments();
        if let Some(after) = self.rest.strip_prefix('.') {
            self.rest = after;
            true
        } else {
            false
        }
    }
}

/// Returns `true` iff `sql` is a `CREATE VIRTUAL TABLE [IF NOT EXISTS] [schema.]table USING aydb`
/// DDL statement stored in `sqlite_schema`.
fn is_aydb_vtab_sql(sql: &str) -> bool {
    let mut s = SqlScanner { rest: sql };
    if !s
        .next_keyword()
        .is_some_and(|w| w.eq_ignore_ascii_case("CREATE"))
    {
        return false;
    }
    if !s
        .next_keyword()
        .is_some_and(|w| w.eq_ignore_ascii_case("VIRTUAL"))
    {
        return false;
    }
    if !s
        .next_keyword()
        .is_some_and(|w| w.eq_ignore_ascii_case("TABLE"))
    {
        return false;
    }
    s.try_skip_if_not_exists();
    if s.next_ident().is_none() {
        return false;
    }
    if s.consume_dot() && s.next_ident().is_none() {
        return false;
    }
    if !s
        .next_keyword()
        .is_some_and(|w| w.eq_ignore_ascii_case("USING"))
    {
        return false;
    }
    let Some(module) = s.next_ident() else {
        return false;
    };
    if !module.eq_ignore_ascii_case("aydb") {
        return false;
    }
    s.skip_ws_and_comments();
    s.rest.is_empty() || s.rest.starts_with('(') || s.rest.starts_with(';')
}

/// Queries one schema's `sqlite_master` (`sqlite_temp_master` for `"temp"`) for `name` and checks
/// whether it is an `aydb` virtual table. Returns `None` if the query could not be completed.
unsafe fn query_schema_for_aydb_table(db: *mut sqlite3, catalog: &str, name: &str) -> Option<bool> {
    let Ok(name_len) = i32::try_from(name.len()) else {
        return None;
    };
    let schema_table = if catalog.eq_ignore_ascii_case("temp") {
        "temp.sqlite_temp_master".to_string()
    } else if catalog.eq_ignore_ascii_case("main") {
        "main.sqlite_master".to_string()
    } else {
        let escaped = catalog.replace('"', "\"\"");
        format!("\"{escaped}\".sqlite_master")
    };
    let sql = format!(
        "SELECT sql FROM {schema_table} WHERE type = 'table' AND name = ?1 COLLATE NOCASE LIMIT 1"
    );
    let Ok(sql_len) = i32::try_from(sql.len()) else {
        return None;
    };
    let mut stmt: *mut sqlite3_stmt = core::ptr::null_mut();
    let prep_rc = unsafe {
        sqlite3_prepare_v2(
            db,
            sql.as_ptr() as *const c_char,
            sql_len,
            &mut stmt,
            core::ptr::null_mut(),
        )
    };
    if prep_rc != SQLITE_OK || stmt.is_null() {
        if !stmt.is_null() {
            unsafe {
                sqlite3_finalize(stmt);
            }
        }
        return None;
    }
    let bind_rc = unsafe {
        sqlite3_bind_text(
            stmt,
            1,
            name.as_ptr() as *const c_char,
            name_len,
            sqlite_transient(),
        )
    };
    let result = if bind_rc != SQLITE_OK {
        None
    } else {
        match unsafe { sqlite3_step(stmt) } {
            SQLITE_ROW => {
                let bytes = unsafe { sqlite_column_text_bytes(stmt, 0) };
                core::str::from_utf8(bytes).ok().map(is_aydb_vtab_sql)
            }
            SQLITE_DONE => Some(false),
            _ => None,
        }
    };
    unsafe {
        sqlite3_finalize(stmt);
    }
    result
}

/// Whether SQLite's schema (`db_name`, or `temp`/`main` if empty) still has an `aydb` virtual
/// table of this name. Any doubt counts as "yes": a table is only removed when the catalog query
/// definitively finds no `USING aydb` virtual table with this name.
unsafe fn schema_has_aydb_table(db: *mut sqlite3, db_name: &str, name: &str) -> bool {
    let _guard = snapshot::enter_internal_schema_query();
    if !db_name.is_empty() {
        return unsafe { query_schema_for_aydb_table(db, db_name, name) }.unwrap_or(true);
    }
    match unsafe { query_schema_for_aydb_table(db, "temp", name) } {
        Some(true) => true,
        Some(false) => unsafe { query_schema_for_aydb_table(db, "main", name) }.unwrap_or(true),
        None => true,
    }
}

fn run_commit_sync_barrier(
    file: &OsFile,
    #[cfg(test)] fail_sync: Option<&Arc<AtomicBool>>,
) -> io::Result<()> {
    #[cfg(test)]
    if fail_sync.is_some_and(|flag| flag.swap(false, Ordering::SeqCst)) {
        return Err(io::Error::other("injected WAL sync failure"));
    }
    file.sync_barrier()
}

fn complete_commit_step_for_engine(
    engine: &Arc<RwLock<StorageEngine<OsFs>>>,
    step: io::Result<CommitStep<OsFile>>,
    #[cfg(test)] fail_sync: Option<Arc<AtomicBool>>,
) -> io::Result<()> {
    // The device flush runs without the engine lock so that readers are not held up by it.
    step.and_then(|step| match step {
        CommitStep::Done => Ok(()),
        CommitStep::AwaitingSync(file) => {
            let flushed = run_commit_sync_barrier(
                &file,
                #[cfg(test)]
                fail_sync.as_ref(),
            );
            match engine.write() {
                Ok(mut eng) => eng.finish_commit_for_conn(flushed),
                Err(_) => Err(io::Error::other("engine lock poisoned")),
            }
        }
        CommitStep::AwaitingGroup { lsn, file, gate } => loop {
            match gate.join_group(lsn)? {
                GroupRole::Durable => break Ok(()),
                GroupRole::Lead(target) => {
                    let mut leader_guard = gate.leader_guard();
                    let flushed = run_commit_sync_barrier(
                        &file,
                        #[cfg(test)]
                        fail_sync.as_ref(),
                    );
                    match engine.write() {
                        Ok(mut eng) => {
                            let res = eng.finish_group_flush(target, flushed);
                            leader_guard.disarm();
                            res?;
                        }
                        Err(_) => {
                            let e = io::Error::other("engine lock poisoned");
                            gate.abandon_flush(&e);
                            leader_guard.disarm();
                            break Err(e);
                        }
                    }
                }
            }
        },
    })
}

unsafe fn complete_commit_step(
    vtab: &AydbVTab,
    step: io::Result<CommitStep<OsFile>>,
    #[cfg(test)] fail_sync: Option<Arc<AtomicBool>>,
) -> c_int {
    let committed = complete_commit_step_for_engine(
        &vtab.engine,
        step,
        #[cfg(test)]
        fail_sync,
    );
    if let Err(e) = committed {
        unsafe {
            set_vtab_err_msg(vtab.base.get(), &e.to_string());
        }
        return io_err_to_sqlite_rc(&e);
    }
    SQLITE_OK
}

unsafe extern "system" fn vtab_sync_tx(p_vtab: *mut sqlite3_vtab) -> c_int {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        let vtab = &*(p_vtab as *const AydbVTab);
        // Run prepare, WAL append, and device flush in `xSync` rather than `xCommit`: SQLite
        // discards `xCommit`'s return code, whereas returning an error from `xSync` aborts
        // `COMMIT` and rolls back any native SQLite tables updated in the same transaction.
        #[cfg(test)]
        let fail_sync_flag: Option<Arc<AtomicBool>>;
        let step = {
            let mut eng = match vtab.engine.write() {
                Ok(g) => g,
                Err(_) => return SQLITE_ERROR,
            };
            if !eng.conn_has_open_transaction(vtab.db as usize) {
                return SQLITE_OK;
            }
            if !eng.mark_vtab_synced_ready_to_commit(p_vtab as usize, vtab.durability) {
                return SQLITE_OK;
            }
            // Tables this transaction created (even if later renamed) whose creation a `ROLLBACK TO`
            // has since undone in SQLite's schema must not be committed.
            eng = match prune_stale_created_vtabs_unlocked(
                vtab.db,
                &vtab.engine,
                vtab.db as usize,
                eng,
            ) {
                Ok(g) => g,
                Err(e) => {
                    set_vtab_err_msg(vtab.base.get(), &e.to_string());
                    return io_err_to_sqlite_rc(&e);
                }
            };
            if !eng.conn_has_open_transaction(vtab.db as usize) {
                return SQLITE_OK;
            }
            if let Err(e) = eng.prepare_commit_transaction(vtab.durability) {
                set_vtab_err_msg(vtab.base.get(), &e.to_string());
                return io_err_to_sqlite_rc(&e);
            }
            if sqlite3_txn_state(vtab.db, core::ptr::null()) == SQLITE_TXN_WRITE {
                if !vtab.pending_ddl.set_tx_commit(PendingDdl::TxCommit {
                    engine: Arc::clone(&vtab.engine),
                    conn_id: vtab.db as usize,
                    durability: vtab.durability,
                }) {
                    return SQLITE_ERROR;
                }
                return SQLITE_OK;
            }
            #[cfg(test)]
            {
                fail_sync_flag = Some(eng.debug_fail_wal_sync_flag());
            }
            eng.begin_commit_for_conn(vtab.db as usize)
        };
        complete_commit_step(
            vtab,
            step,
            #[cfg(test)]
            fail_sync_flag,
        )
    }))
    .unwrap_or(SQLITE_ERROR)
}

unsafe extern "system" fn vtab_commit_tx(p_vtab: *mut sqlite3_vtab) -> c_int {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        let vtab = &*(p_vtab as *const AydbVTab);
        vtab.pending_ddl.clear_tx_commit();
        // SQLite invokes `xCommit` once per virtual table in `aVTrans`. Normal transactions
        // already appended, flushed, and published in `xSync` or `ddl_commit_hook`, so
        // `conn_has_open_transaction` is false here; keep prepare+commit as a fallback only if an
        // active engine transaction remains.
        #[cfg(test)]
        let mut fail_sync_flag: Option<Arc<AtomicBool>> = None;
        let step = match vtab.engine.write() {
            Ok(mut eng) => {
                if !eng.conn_has_open_transaction(vtab.db as usize) {
                    Ok(CommitStep::Done)
                } else if let Err(e) = eng.prepare_commit_transaction(vtab.durability) {
                    set_vtab_err_msg(vtab.base.get(), &e.to_string());
                    return io_err_to_sqlite_rc(&e);
                } else {
                    #[cfg(test)]
                    {
                        fail_sync_flag = Some(eng.debug_fail_wal_sync_flag());
                    }
                    eng.begin_commit_for_conn(vtab.db as usize)
                }
            }
            Err(_) => return SQLITE_ERROR,
        };
        let rc = complete_commit_step(
            vtab,
            step,
            #[cfg(test)]
            fail_sync_flag,
        );
        if rc != SQLITE_OK {
            return rc;
        }
        if let Ok(mut prev) = vtab.pre_tx_name.lock() {
            *prev = None;
        }
        SQLITE_OK
    }))
    .unwrap_or(SQLITE_ERROR)
}

unsafe extern "system" fn vtab_rollback_tx(p_vtab: *mut sqlite3_vtab) -> c_int {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        let vtab = &*(p_vtab as *const AydbVTab);
        vtab.pending_ddl.clear_tx_commit();
        let restored_name = {
            let mut eng = match vtab.engine.write() {
                Ok(g) => g,
                Err(_) => return SQLITE_ERROR,
            };
            if eng.conn_has_open_transaction(vtab.db as usize) {
                if let Err(e) = eng.rollback_transaction_for_conn(vtab.db as usize) {
                    return io_err_to_sqlite_rc(&e);
                }
                eng.active_vtab_name(vtab.table_id)
            } else {
                None
            }
        };
        let prev_name = vtab.pre_tx_name.lock().ok().and_then(|mut p| p.take());
        if let Some(actual) = prev_name.or(restored_name) {
            if let Ok(mut name) = vtab.table_name.lock() {
                *name = actual;
            }
        }
        SQLITE_OK
    }))
    .unwrap_or(SQLITE_ERROR)
}

unsafe extern "system" fn vtab_savepoint(p_vtab: *mut sqlite3_vtab, i_savepoint: c_int) -> c_int {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        let vtab = &*(p_vtab as *const AydbVTab);
        vtab.pending_ddl.clear_tx_commit();
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
        vtab.pending_ddl.clear_tx_commit();
        let mut eng = match vtab.engine.write() {
            Ok(g) => g,
            Err(_) => return SQLITE_ERROR,
        };
        if !eng.conn_has_open_transaction(vtab.db as usize) {
            return SQLITE_OK;
        }
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
        vtab.pending_ddl.clear_tx_commit();
        let mut eng = match vtab.engine.write() {
            Ok(g) => g,
            Err(_) => return SQLITE_ERROR,
        };
        if let Err(e) = eng.rollback_to_savepoint_for_conn(vtab.db as usize, i_savepoint) {
            return io_err_to_sqlite_rc(&e);
        }
        if let Some(actual) = eng.active_vtab_name(vtab.table_id) {
            if let Ok(mut name) = vtab.table_name.lock() {
                *name = actual;
            }
        }
        SQLITE_OK
    }))
    .unwrap_or(SQLITE_ERROR)
}

#[cfg(test)]
mod schema_sql_tests {
    use super::is_aydb_vtab_sql;

    #[test]
    fn recognizes_aydb_virtual_table_ddl_and_rejects_other_tables() {
        assert!(is_aydb_vtab_sql(
            "CREATE VIRTUAL TABLE t USING aydb(columns='k INT PRIMARY KEY')"
        ));
        assert!(is_aydb_vtab_sql(
            "  create /* c1 */ virtual -- line comment\n table if not exists main.[my.t] using \"AYDB\" (columns='k INT')"
        ));
        assert!(is_aydb_vtab_sql(
            "CREATE VIRTUAL TABLE \"t\"\"1\" USING `aydb`"
        ));
        assert!(is_aydb_vtab_sql(
            "CREATE VIRTUAL TABLE aux.`if` USING [AyDb];"
        ));

        assert!(!is_aydb_vtab_sql(""));
        assert!(!is_aydb_vtab_sql("CREATE TABLE t(a INT)"));
        assert!(!is_aydb_vtab_sql(
            "CREATE TABLE t(a TEXT DEFAULT 'USING aydb')"
        ));
        assert!(!is_aydb_vtab_sql("CREATE VIRTUAL TABLE t USING fts5(a)"));
        assert!(!is_aydb_vtab_sql(
            "CREATE VIRTUAL TABLE t USING aydb_other(columns='k INT')"
        ));
        assert!(!is_aydb_vtab_sql(
            "CREATE VIRTUAL TABLE t USING \"aydb\"extra(columns='k INT')"
        ));
    }
}
