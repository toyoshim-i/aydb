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
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

#[cfg(test)]
use crate::engine::{CommitStep, GroupRole};
use crate::engine::{
    EngineReader, PutRowOptions, RangeScanOptions, STATEMENT_SAVEPOINT, SlabScanOutput,
    StorageEngine, WriteDurability,
};
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
#[cfg(test)]
use crate::vtab::ffi::sqlite3_index_info;
use crate::vtab::ffi::{
    SQLITE_BUSY, SQLITE_CONSTRAINT, SQLITE_CORRUPT, SQLITE_ERROR, SQLITE_FLOAT, SQLITE_FULL,
    SQLITE_INTEGER, SQLITE_IOERR, SQLITE_MISMATCH, SQLITE_NULL, SQLITE_OK, SQLITE_REPLACE,
    SQLITE_TEXT, SQLITE_TOOBIG, set_vtab_err_msg, sqlite_value_text_bytes, sqlite3, sqlite3_int64,
    sqlite3_module, sqlite3_value, sqlite3_value_double, sqlite3_value_int64, sqlite3_value_type,
    sqlite3_vtab, sqlite3_vtab_on_conflict,
};
use crate::vtab::lifecycle::STALE_VTAB_ERR;
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

/// Returns the alternate exact-integer numeric key representation (`9`-byte `Integer(q)` vs
/// `17`-byte `Float(q.0)`) so SQLite's `10 == 10.0` numeric key equality holds across `BLOB`
/// and `REAL` affinities.
#[inline]
pub(in crate::vtab) fn build_numeric_alt_key(pk: &[u8]) -> Option<Vec<u8>> {
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
pub(in crate::vtab) fn needs_numeric_alt_probe(pk: &[u8], affinity: ColumnAffinity) -> bool {
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

pub(in crate::vtab) unsafe fn extract_lossless_rowid_arg(val: *mut sqlite3_value) -> Option<i64> {
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

pub(in crate::vtab) fn resolve_pk_for_rowid(
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
                RangeScanOptions {
                    lower: Some((&lower, true)),
                    upper: Some((&upper, upper_inclusive)),
                    reverse: true,
                    limit,
                    keys_only: true,
                },
                true,
                SlabScanOutput {
                    pks: &mut pks,
                    slab: &mut slab,
                    spans: &mut spans,
                    lazy_blobs: None,
                },
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

fn has_stale_unique_marker(eng: &StorageEngine<OsFs>, vtab: &AydbVTab) -> io::Result<bool> {
    let marker_gen = eng.unique_marker_gen();
    if !eng.unique_marker_modified_in_tx() && vtab.unique_ready.is_ready_for(marker_gen) {
        return Ok(false);
    }
    let existing = eng.unique_marker_view(crate::engine::ReadView::Owner, vtab.table_id)?;
    if vtab.unique_def.is_empty() {
        Ok(existing.is_some())
    } else {
        let want = vtab.unique_def.marker_value();
        Ok(existing.is_some_and(|found| found != want))
    }
}

fn invalidate_stale_unique_marker_if_needed(
    eng: &mut StorageEngine<OsFs>,
    vtab: &AydbVTab,
) -> io::Result<()> {
    let marker_gen = eng.unique_marker_gen();
    if !eng.unique_marker_modified_in_tx() && vtab.unique_ready.is_ready_for(marker_gen) {
        return Ok(());
    }
    let existing = eng.unique_marker_view(crate::engine::ReadView::Owner, vtab.table_id)?;
    if vtab.unique_def.is_empty() {
        if existing.is_some() {
            eng.remove_unique_marker(vtab.table_id, vtab.durability)?;
        }
        vtab.unique_ready.latch_if_committed(
            eng.unique_marker_gen(),
            !eng.in_active_transaction() && !eng.unique_marker_modified_in_tx(),
        );
    } else {
        let want = vtab.unique_def.marker_value();
        if existing.is_some_and(|found| found != want) {
            eng.remove_unique_marker(vtab.table_id, vtab.durability)?;
        }
    }
    Ok(())
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
        vtab.pending_ddl.disarm_tx_commit();

        let needs_synthetic_delete_check =
            |pk: &[u8]| -> bool { !(vtab.pk_rowid_alias && decode_i64_tagged_key(pk).is_some()) };

        // Case 1: DELETE (`argc == 1`)
        if argc == 1 {
            let arg0 = *argv.offset(0);
            let old_pk_opt = if let Some(rid) = extract_lossless_rowid_arg(arg0) {
                match resolve_pk_for_rowid(
                    vtab,
                    rid,
                    |t, pk| eng.row_exists(t, pk),
                    |t, rid| eng.pk_for_rowid_view(crate::engine::ReadView::Owner, t, rid),
                ) {
                    Ok(opt) => opt,
                    Err(e) => return io_err_to_sqlite_rc(&e),
                }
            } else {
                Some(sqlite_value_to_key_bytes_with_schema(
                    arg0,
                    vtab.pk_affinity,
                    vtab.pk_collation,
                    false,
                ))
            };
            if let Some(old_pk) = old_pk_opt {
                let stale_marker = match has_stale_unique_marker(&eng, vtab) {
                    Ok(b) => b,
                    Err(e) => return io_err_to_sqlite_rc(&e),
                };
                if stale_marker {
                    if let Err(e) =
                        eng.statement_savepoint_for_conn(vtab.db as usize, vtab.table_id)
                    {
                        return io_err_to_sqlite_rc(&e);
                    }
                }
                let del_res = (|| -> io::Result<()> {
                    let check_syn = needs_synthetic_delete_check(&old_pk);
                    let deleted = eng.delete_row_with_rowid_hint(
                        vtab.table_id,
                        &old_pk,
                        vtab.durability,
                        check_syn,
                    )?;
                    if deleted {
                        invalidate_stale_unique_marker_if_needed(&mut eng, vtab)?;
                    }
                    Ok(())
                })();
                if stale_marker {
                    if del_res.is_err() {
                        let _ = eng
                            .rollback_to_savepoint_for_conn(vtab.db as usize, STATEMENT_SAVEPOINT);
                        let _ =
                            eng.release_savepoint_for_conn(vtab.db as usize, STATEMENT_SAVEPOINT);
                    } else if let Err(e) =
                        eng.release_savepoint_for_conn(vtab.db as usize, STATEMENT_SAVEPOINT)
                    {
                        return io_err_to_sqlite_rc(&e);
                    }
                }
                if let Err(e) = del_res {
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
        if !is_insert && sqlite3_value_type(new_rowid_arg) == SQLITE_NULL {
            return SQLITE_MISMATCH;
        }
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
        let stale_marker = match has_stale_unique_marker(&eng, vtab) {
            Ok(b) => b,
            Err(e) => return io_err_to_sqlite_rc(&e),
        };
        let removes_rows = stale_marker
            || pk_conflict_to_delete.is_some()
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
                PutRowOptions {
                    secondary_keys: &sec_keys,
                    extra_records,
                    durability: vtab.durability,
                    known_absent,
                },
            )?;
            invalidate_stale_unique_marker_if_needed(&mut eng, vtab)?;

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
