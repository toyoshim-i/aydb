//! Phase 2: SQLite Virtual Table (`sqlite3_module`) Adapter & Safe SQLite Connection Wrapper.
//!
//! Exposes `aydb`'s [`StorageEngine`] as a first-class SQLite Virtual Table (`USING aydb(...)`)
//! with automatic primary-key point lookups (`xBestIndex` Plan 1), secondary index equality
//! lookups (Plan 2), primary-key range scans (Plan 3), secondary index range scans (Plan 4),
//! persistent `UNIQUE` index equality lookups (Plan 5), and Capped Table (`MAX_BYTES`) LRU
//! eviction.

#[cfg(test)]
mod auto_rowid_tests;
mod catalog_probe;
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
mod cursor;
mod ddl;
mod ddl_slot;
mod dml;
#[cfg(test)]
mod drop_tests;
pub mod ffi;
#[cfg(test)]
mod integrity_tests;
#[cfg(test)]
mod key_null_tests;
mod lifecycle;
#[cfg(test)]
mod multi_conn_diff_tests;
#[cfg(test)]
mod pk_update_tests;
mod planner;
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
mod tx_callbacks;
#[cfg(test)]
mod unique_check_tests;
#[cfg(test)]
mod unique_index_tests;
pub(crate) mod unique_key;
pub(crate) mod unique_migration;
#[cfg(test)]
mod unique_migration_tests;

use core::ffi::c_int;
#[cfg(test)]
use std::ffi::CString;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

#[cfg(test)]
use crate::engine::{CommitStep, GroupRole};
use crate::engine::{EngineReader, StorageEngine, WriteDurability};
use crate::pal::OsFs;
pub use crate::vtab::connection::{SqliteConnection, SqliteStatement};
#[cfg(test)]
use crate::vtab::cursor::{CURSOR_LAZY_ROW_BUF_MAX_LEN, CURSOR_SLAB_MAX_LEN};
use crate::vtab::cursor::{
    vtab_close, vtab_column, vtab_eof, vtab_filter, vtab_next, vtab_open, vtab_rowid,
};
use crate::vtab::ddl_slot::ConnDdlSlot;
#[cfg(test)]
use crate::vtab::ddl_slot::{AutocommitDdl, InTxRename, TxCommitHook};
use crate::vtab::dml::vtab_update;
pub(in crate::vtab) use crate::vtab::dml::{
    build_numeric_alt_key, extract_lossless_rowid_arg, needs_numeric_alt_probe,
    resolve_pk_for_rowid,
};
use crate::vtab::ffi::{
    SQLITE_BUSY, SQLITE_CORRUPT, SQLITE_ERROR, SQLITE_IOERR, SQLITE_TOOBIG, sqlite3,
    sqlite3_module, sqlite3_vtab,
};
#[cfg(test)]
use crate::vtab::ffi::{SQLITE_OK, sqlite3_index_info};
#[cfg(test)]
use crate::vtab::lifecycle::{FAIL_CONNECT_AFTER_REGISTRATION, connect_table};
use crate::vtab::lifecycle::{
    destroy_aux_context, vtab_connect, vtab_create, vtab_destroy, vtab_disconnect, vtab_rename,
};
use crate::vtab::planner::vtab_best_index;
#[cfg(test)]
use crate::vtab::planner::{
    LAST_BEST_INDEX_PLAN, PLAN_FULL_SCAN, PLAN_PK_RANGE, PLAN_SECONDARY_RANGE,
    POINT_FLAG_KEYS_ONLY, RANGE_FLAG_KEYS_ONLY,
};
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
#[cfg(test)]
use crate::vtab::tx_callbacks::UNIQUE_MIGRATION_DISABLED;
use crate::vtab::tx_callbacks::{
    vtab_begin_tx, vtab_commit_tx, vtab_release, vtab_rollback_to, vtab_rollback_tx,
    vtab_savepoint, vtab_sync_tx,
};

/// Point lookup used by `PLAN_PK_POINT`: reads the row value into the cursor slab, or (for
/// keys-only plans) merely checks existence without touching any out-of-line blob.
pub(in crate::vtab) fn point_fetch(
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

pub(in crate::vtab) fn with_snapshot_read_access<R>(
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
pub(in crate::vtab) fn io_err_to_sqlite_rc(err: &io::Error) -> c_int {
    match err.kind() {
        io::ErrorKind::WouldBlock => SQLITE_BUSY,
        io::ErrorKind::InvalidData => SQLITE_CORRUPT,
        io::ErrorKind::FileTooLarge => SQLITE_TOOBIG,
        io::ErrorKind::InvalidInput | io::ErrorKind::AlreadyExists => SQLITE_ERROR,
        _ => SQLITE_IOERR,
    }
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

/// Monotonic generation latch recording the committed `unique_marker_gen` at which this virtual
/// table's UNIQUE index (or absence of stale markers when `unique_def.is_empty()`) was verified.
///
/// Storing `0` or an uncommitted in-transaction marker generation is forbidden, ensuring `0`
/// always means unverified and uncommitted schema changes never poison later plans or writes.
pub(in crate::vtab) struct UniqueReadyLatch {
    verified_gen: AtomicU64,
}

impl UniqueReadyLatch {
    #[cfg(test)]
    #[inline]
    pub(in crate::vtab) const fn unlatched() -> Self {
        Self {
            verified_gen: AtomicU64::new(0),
        }
    }

    #[inline]
    pub(in crate::vtab) const fn from_verified_gen(marker_gen: u64) -> Self {
        Self {
            verified_gen: AtomicU64::new(marker_gen),
        }
    }

    #[inline]
    pub(in crate::vtab) fn is_ready_for(&self, marker_gen: u64) -> bool {
        marker_gen != 0 && self.verified_gen.load(Ordering::Acquire) == marker_gen
    }

    #[inline]
    pub(in crate::vtab) fn latch_if_committed(&self, marker_gen: u64, allow_latch: bool) -> bool {
        if allow_latch && marker_gen != 0 {
            self.verified_gen.store(marker_gen, Ordering::Release);
            true
        } else {
            false
        }
    }

    #[inline]
    pub(in crate::vtab) fn invalidate(&self) {
        self.verified_gen.store(0, Ordering::Release);
    }

    #[cfg(test)]
    #[inline]
    pub(in crate::vtab) fn raw_gen(&self) -> u64 {
        self.verified_gen.load(Ordering::Acquire)
    }
}

pub(in crate::vtab) struct ModuleAuxContext {
    pub(in crate::vtab) engine: Arc<RwLock<StorageEngine<OsFs>>>,
    pub(in crate::vtab) read_session: Arc<snapshot::ReadSession>,
    pub(in crate::vtab) pending_ddl: Arc<ConnDdlSlot>,
    /// Background compaction worker; joined when the module (and thus this context) is dropped.
    pub(in crate::vtab) _compaction_worker: Option<compaction_worker::CompactionWorker>,
}

#[repr(C)]
pub(in crate::vtab) struct AydbVTab {
    pub(in crate::vtab) base: core::cell::UnsafeCell<sqlite3_vtab>,
    pub(in crate::vtab) db: *mut sqlite3,
    pub(in crate::vtab) engine: Arc<RwLock<StorageEngine<OsFs>>>,
    pub(in crate::vtab) read_session: Arc<snapshot::ReadSession>,
    pub(in crate::vtab) pending_ddl: Arc<ConnDdlSlot>,
    pub(in crate::vtab) table_id: u16,
    pub(in crate::vtab) reg_token: u64,
    pub(in crate::vtab) db_name: String,
    /// Normalised at use. Changes when the table is renamed.
    pub(in crate::vtab) table_name: Arc<std::sync::Mutex<String>>,
    pub(in crate::vtab) durability: WriteDurability,
    /// The UNIQUE columns with the collation and affinity that shape their index keys.
    pub(in crate::vtab) unique_def: unique_key::UniqueDef,
    /// Committed `unique_marker_gen` latch guarded by [`UniqueReadyLatch`].
    pub(in crate::vtab) unique_ready: UniqueReadyLatch,
    /// A migration attempt of this connection failed; it is not repeated until reconnect.
    pub(in crate::vtab) unique_migration_failed: AtomicBool,
    /// Where the next `xBegin` resumes the UNIQUE index migration.
    pub(in crate::vtab) unique_cursor: std::sync::Mutex<Option<Vec<u8>>>,
    pub(in crate::vtab) col_names: Vec<String>,
    pub(in crate::vtab) col_affinities: Vec<ColumnAffinity>,
    pub(in crate::vtab) col_not_null: Vec<bool>,
    pub(in crate::vtab) col_unique: Vec<bool>,
    pub(in crate::vtab) col_collations: Vec<ColCollation>,
    pub(in crate::vtab) pk_col_idx: usize,
    pub(in crate::vtab) pk_affinity: ColumnAffinity,
    /// The key is `INTEGER PRIMARY KEY`, a rowid alias: NULL on insert assigns the next key.
    pub(in crate::vtab) pk_rowid_alias: bool,
    /// `(table mutation epoch, next automatic key)` as left by this table's last automatic
    /// insert. Valid only while the table's epoch is unchanged: unrelated writes on other tables
    /// do not invalidate it, while any write/commit/rollback/drop on this table bumps it.
    pub(in crate::vtab) auto_key_cache: std::sync::Mutex<Option<(u64, i64)>>,
    /// The declaration has a clause that is not honoured (`CHECK`, `DEFAULT`):
    /// every write that stores a row fails with this message.
    pub(in crate::vtab) unsupported: Option<&'static str>,
    pub(in crate::vtab) pk_collation: ColCollation,
    /// Compact list of `(col_idx, index_id)` (`1..=N`) for secondary indexed columns.
    pub(in crate::vtab) secondary_indexes: Vec<(usize, u16)>,
}

impl AydbVTab {
    /// The table's current name.
    pub(in crate::vtab) fn name(&self) -> String {
        self.table_name
            .lock()
            .map(|name| name.clone())
            .unwrap_or_default()
    }
}
