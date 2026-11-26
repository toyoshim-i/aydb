//! Virtual table module lifecycle callbacks (`xCreate`, `xConnect`, `xDisconnect`,
//! `xRename`, `xDestroy`) and table registration helpers.

use core::ffi::{c_char, c_int, c_void};
use std::ffi::{CStr, CString};
use std::io;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Instant;

use crate::engine::{STATEMENT_SAVEPOINT, StorageEngine};
use crate::pal::OsFs;
use crate::vtab::catalog_probe::{CreatedVtabMatch, prune_matching_created_vtabs_unlocked};
use crate::vtab::ddl::{ParsedVtabDdl, parse_vtab_ddl};
use crate::vtab::ddl_slot::{AutocommitDdl, ConnDdlSlot, InTxRename, TxCommitHook};
use crate::vtab::ffi::{
    SQLITE_BUSY, SQLITE_ERROR, SQLITE_OK, SQLITE_VTAB_CONSTRAINT_SUPPORT, set_pz_err,
    set_vtab_err_msg, sqlite3, sqlite3_declare_vtab, sqlite3_get_autocommit, sqlite3_vtab,
    sqlite3_vtab_config,
};
use crate::vtab::unique_key;
use crate::vtab::{
    AYDB_VTAB_MODULE, AydbVTab, ModuleAuxContext, UniqueReadyLatch, io_err_to_sqlite_rc,
};

const CREATE_SAVEPOINT: i32 = STATEMENT_SAVEPOINT + 1;
pub(in crate::vtab) const RENAME_NEEDS_WRITE: &str = "ALTER TABLE RENAME of an aydb table inside a transaction needs an earlier write to an aydb table in that transaction";
pub(in crate::vtab) const STALE_VTAB_ERR: &str = "virtual table handle is stale: table was dropped";

#[cfg(test)]
thread_local! {
    /// Test seam: makes the next connect on this thread fail right after it registered the table.
    pub(in crate::vtab) static FAIL_CONNECT_AFTER_REGISTRATION: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
}

pub(in crate::vtab) unsafe extern "system" fn destroy_aux_context(ptr: *mut c_void) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if !ptr.is_null() {
            unsafe {
                drop(Box::from_raw(ptr as *mut ModuleAuxContext));
            }
        }
    }));
}

/// What a successful connect resolved under the engine lock.
pub(in crate::vtab) struct Connected {
    pub(in crate::vtab) table_id: u16,
    pub(in crate::vtab) reg_token: u64,
    pub(in crate::vtab) unique_ready_gen: u64,
    /// The catalog had no entry for the table before this connect: it is a new table.
    pub(in crate::vtab) newly_created: bool,
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
pub(in crate::vtab) fn connect_table(
    eng: &mut StorageEngine<OsFs>,
    conn_id: usize,
    ddl: &ParsedVtabDdl,
    is_create: bool,
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
            crate::vtab::tx_callbacks::UNIQUE_MIGRATION_DISABLED.with(|flag| flag.get())
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
    let quota_registered = ddl.max_bytes == 0
        || eng
            .capped_table_state_view(view, table_id)
            .is_some_and(|state| state.max_bytes == ddl.max_bytes);
    let (use_persisted_activation, needs_owner_quota_register) =
        if eng.check_write_allowed_for_conn(conn_id).is_err() {
            if !(plan.persisted && quota_registered) {
                return Ok(None);
            }
            (true, false)
        } else {
            let persisted_in_owner_tx = !is_create
                && eng.conn_has_open_transaction(conn_id)
                && plan.persisted
                && !eng.is_vtab_created_in_tx(table_id);
            (
                persisted_in_owner_tx,
                persisted_in_owner_tx && !quota_registered && ddl.max_bytes > 0,
            )
        };
    if needs_owner_quota_register {
        eng.register_capped_table(table_id, ddl.max_bytes)?;
    }
    let reg_token = if use_persisted_activation {
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
    if use_persisted_activation {
        eng.apply_persisted_vtab_runtime_settings(table_id, ddl.on_corrupt, ddl.inline_max);
    } else {
        match ddl.on_corrupt {
            Some(policy) => eng.set_table_corrupt_policy(table_id, policy),
            None => eng.reset_table_corrupt_policy(table_id),
        }
        match ddl.inline_max {
            Some(max_bytes) => eng.set_table_inline_threshold(table_id, max_bytes),
            None => eng.reset_table_inline_threshold(table_id),
        }
    }
    let marker_gen = eng.unique_marker_gen();
    let existing_marker = eng.unique_marker_view(crate::engine::ReadView::Committed, table_id);
    let marker_matches = if unique_def.is_empty() {
        matches!(existing_marker, Ok(None))
    } else {
        matches!(existing_marker, Ok(Some(ref found)) if *found == unique_def.marker_value())
    };
    let unique_ready_gen = if marker_matches && marker_gen != 0 {
        marker_gen
    } else {
        0
    };
    Ok(Some(Connected {
        table_id,
        reg_token,
        unique_ready_gen,
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

/// Waits, bounded like `xBegin`, for another connection's transaction to end: the same thread
/// may hold that connection open and could never end it. Returns false once the wait ran out.
pub(in crate::vtab) fn wait_for_other_transaction(
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

/// Opens a fresh autocommit engine transaction for `vtab_rename` or `vtab_destroy`, waiting up
/// to `writer_wait_timeout` if another connection currently holds a transaction open.
unsafe fn acquire_autocommit_ddl_tx<'a>(
    vtab: &'a AydbVTab,
    pending_ddl: &ConnDdlSlot,
    conn_id: usize,
    report_begin_err: bool,
) -> Result<std::sync::RwLockWriteGuard<'a, StorageEngine<OsFs>>, c_int> {
    if pending_ddl.is_occupied() {
        return Err(SQLITE_ERROR);
    }
    let mut deadline = None;
    loop {
        let mut eng = vtab.engine.write().map_err(|_| SQLITE_ERROR)?;
        if eng.conn_has_open_transaction(conn_id) {
            return Err(SQLITE_ERROR);
        }
        if !eng.is_vtab_token_active(vtab.table_id, vtab.reg_token, conn_id) {
            unsafe { set_vtab_err_msg(vtab.base.get(), STALE_VTAB_ERR) };
            return Err(SQLITE_ERROR);
        }
        match eng.begin_transaction_for_conn(conn_id) {
            Ok(()) => return Ok(eng),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                if !wait_for_other_transaction(eng, &mut deadline) {
                    return Err(SQLITE_BUSY);
                }
            }
            Err(e) => {
                if report_begin_err {
                    unsafe { set_vtab_err_msg(vtab.base.get(), &e.to_string()) };
                }
                return Err(io_err_to_sqlite_rc(&e));
            }
        }
    }
}

pub(in crate::vtab) unsafe extern "system" fn vtab_create(
    db: *mut sqlite3,
    p_aux: *mut c_void,
    argc: c_int,
    argv: *const *const c_char,
    pp_vtab: *mut *mut sqlite3_vtab,
    pz_err: *mut *mut c_char,
) -> c_int {
    unsafe { vtab_create_or_connect(db, p_aux, argc, argv, pp_vtab, pz_err, true) }
}

pub(in crate::vtab) unsafe extern "system" fn vtab_connect(
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
            set_pz_err(pz_err, reason);
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
        if is_create && !autocommit {
            if let Err(e) = aux.pending_ddl.reconcile_in_tx_renames(db) {
                set_pz_err(pz_err, &e.to_string());
                return io_err_to_sqlite_rc(&e);
            }
        }
        let mut began_here = false;
        let (mut eng, connected) = loop {
            let mut eng = match aux.engine.write() {
                Ok(g) => g,
                Err(_) => return SQLITE_ERROR,
            };
            if !autocommit {
                aux.pending_ddl.disarm_tx_commit();
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
                            if e.kind() == io::ErrorKind::InvalidData {
                                set_pz_err(pz_err, &e.to_string());
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
                            set_pz_err(pz_err, &error.to_string());
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
                    CreatedVtabMatch {
                        table_id: ddl.explicit_table_id,
                        table_name: &ddl.table_name,
                        cur_norm_db: &cur_norm_db,
                        exclude_table_id: None,
                    },
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
            let guard_create_sp = (is_create || ddl.max_bytes > 0)
                && !began_here
                && eng.conn_has_open_transaction(db as usize);
            if guard_create_sp {
                if let Err(e) = eng.savepoint_for_conn(db as usize, CREATE_SAVEPOINT) {
                    return io_err_to_sqlite_rc(&e);
                }
            }
            match connect_table(&mut eng, db as usize, &ddl, is_create) {
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
                    if e.kind() == io::ErrorKind::InvalidData {
                        set_pz_err(pz_err, &e.to_string());
                    }
                    return io_err_to_sqlite_rc(&e);
                }
            }
        };
        let Connected {
            table_id,
            reg_token,
            unique_ready_gen,
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
            durability: ddl_durability,
            unique_def: unique_key::UniqueDef::from_columns(
                &ddl.col_unique,
                &ddl.col_collations,
                &ddl.col_affinities,
            ),
            unique_ready: UniqueReadyLatch::from_verified_gen(unique_ready_gen),
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
                && !aux
                    .pending_ddl
                    .stage_autocommit(AutocommitDdl::Create(TxCommitHook {
                        engine: Arc::clone(&aux.engine),
                        conn_id: db as usize,
                        durability: ddl_durability,
                    }))
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

pub(in crate::vtab) unsafe extern "system" fn vtab_disconnect(p_vtab: *mut sqlite3_vtab) -> c_int {
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
pub(in crate::vtab) unsafe extern "system" fn vtab_rename(
    p_vtab: *mut sqlite3_vtab,
    z_new: *const c_char,
) -> c_int {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        if p_vtab.is_null() || z_new.is_null() {
            return SQLITE_ERROR;
        }
        let vtab = &*(p_vtab as *const AydbVTab);
        let new_name = CStr::from_ptr(z_new).to_string_lossy().into_owned();
        let conn_id = vtab.db as usize;
        let autocommit = sqlite3_get_autocommit(vtab.db) != 0;

        if !autocommit {
            vtab.pending_ddl.disarm_tx_commit();
            if let Err(e) = vtab.pending_ddl.reconcile_in_tx_renames(vtab.db) {
                set_vtab_err_msg(vtab.base.get(), &e.to_string());
                return io_err_to_sqlite_rc(&e);
            }
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
                CreatedVtabMatch {
                    table_id: None,
                    table_name: &new_name,
                    cur_norm_db: &cur_norm_db,
                    exclude_table_id: Some(vtab.table_id),
                },
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
            if crate::engine::normalize_vtab_ident(&old_effective)
                != crate::engine::normalize_vtab_ident(&new_name)
            {
                let recorded = vtab.pending_ddl.record_in_tx_rename(InTxRename {
                    engine: Arc::clone(&vtab.engine),
                    conn_id,
                    table_id: vtab.table_id,
                    db_name: vtab.db_name.clone(),
                    old_name: old_effective,
                    new_name: new_name.clone(),
                    table_name: Arc::clone(&vtab.table_name),
                    durability: vtab.durability,
                });
                debug_assert!(recorded);
            }
            if let Ok(mut name) = vtab.table_name.lock() {
                *name = new_name;
            }
            return SQLITE_OK;
        }

        let mut eng = match acquire_autocommit_ddl_tx(vtab, &vtab.pending_ddl, conn_id, true) {
            Ok(g) => g,
            Err(rc) => return rc,
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
        if let Ok(mut name) = vtab.table_name.lock() {
            *name = new_name;
        }
        if !vtab.pending_ddl.stage_autocommit(AutocommitDdl::Rename {
            commit: TxCommitHook {
                engine: Arc::clone(&vtab.engine),
                conn_id,
                durability: vtab.durability,
            },
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

pub(in crate::vtab) unsafe extern "system" fn vtab_destroy(p_vtab: *mut sqlite3_vtab) -> c_int {
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
                let conn_id = vtab_ref.db as usize;
                let mut eng = match acquire_autocommit_ddl_tx(
                    vtab_ref,
                    &vtab_ref.pending_ddl,
                    conn_id,
                    false,
                ) {
                    Ok(g) => g,
                    Err(rc) => return rc,
                };
                vtab_ref.read_session.clear();
                if let Err(e) =
                    eng.drop_table_with_durability(vtab_ref.table_id, None, vtab_ref.durability)
                {
                    let _ = eng.rollback_transaction_for_conn(conn_id);
                    return io_err_to_sqlite_rc(&e);
                }
                if !vtab_ref.pending_ddl.stage_autocommit(AutocommitDdl::Drop {
                    commit: TxCommitHook {
                        engine: Arc::clone(&vtab_ref.engine),
                        conn_id,
                        durability: vtab_ref.durability,
                    },
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
