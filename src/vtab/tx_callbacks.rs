//! Virtual table transaction, two-phase commit / group-commit barrier, and savepoint
//! callbacks (`xBegin`, `xSync`, `xCommit`, `xRollback`, `xSavepoint`, `xRelease`,
//! `xRollbackTo`).

use core::ffi::c_int;
use std::io;
use std::panic::{AssertUnwindSafe, catch_unwind};
#[cfg(test)]
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock};

use crate::engine::{CommitStep, GroupRole, StorageEngine};
use crate::pal::{OsFile, OsFs, PlatformFile};
use crate::vtab::catalog_probe::prune_stale_created_vtabs_unlocked;
use crate::vtab::ddl_slot::TxCommitHook;
use crate::vtab::ffi::{SQLITE_ERROR, SQLITE_OK, sqlite3_vtab};
use crate::vtab::lifecycle::{STALE_VTAB_ERR, wait_for_other_transaction};
use crate::vtab::{AydbVTab, io_err_to_sqlite_rc, unique_migration};

#[cfg(test)]
thread_local! {
    /// Test seam: keeps `xBegin` from migrating, so a test can drive the chunks itself.
    pub(in crate::vtab) static UNIQUE_MIGRATION_DISABLED: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
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
    if vtab.unsupported.is_some() || vtab.unique_migration_failed.load(Ordering::Relaxed) {
        return;
    }
    {
        let Ok(eng) = vtab.engine.read() else {
            return;
        };
        let marker_gen = eng.unique_marker_gen();
        if !eng.unique_marker_modified_in_tx() && vtab.unique_ready.is_ready_for(marker_gen) {
            return;
        }
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
        unique_migration::Outcome::Complete => {
            if let Ok(eng) = vtab.engine.read() {
                vtab.unique_ready.latch_if_committed(
                    eng.unique_marker_gen(),
                    !eng.in_active_transaction() && !eng.unique_marker_modified_in_tx(),
                );
            }
        }
        unique_migration::Outcome::Incomplete | unique_migration::Outcome::Skipped => {}
        unique_migration::Outcome::Failed(e) => {
            vtab.unique_migration_failed.store(true, Ordering::Relaxed);
            if let Ok(eng) = vtab.engine.read() {
                eng.record_maintenance_failure(&e);
            }
        }
    }
}

pub(in crate::vtab) unsafe extern "system" fn vtab_begin_tx(p_vtab: *mut sqlite3_vtab) -> c_int {
    catch_unwind(AssertUnwindSafe(|| {
        let Some(vtab) = (unsafe { AydbVTab::from_raw(p_vtab) }) else {
            return SQLITE_ERROR;
        };
        let conn_id = vtab.conn_id();
        vtab.pending_ddl.disarm_tx_commit();
        {
            let eng = match vtab.engine.read() {
                Ok(g) => g,
                Err(_) => return SQLITE_ERROR,
            };
            if !eng.is_vtab_token_active(vtab.table_id, vtab.reg_token, conn_id) {
                vtab.set_err(STALE_VTAB_ERR);
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
            if !eng.is_vtab_token_active(vtab.table_id, vtab.reg_token, conn_id) {
                vtab.set_err(STALE_VTAB_ERR);
                return SQLITE_ERROR;
            }
            if let Some(snapshot) = vtab.read_session.current() {
                if let Err(error) = eng.check_snapshot_current_for_upgrade(&snapshot) {
                    return io_err_to_sqlite_rc(&error);
                }
            }
            match eng.begin_vtab_transaction(conn_id, p_vtab as usize) {
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

pub(in crate::vtab) fn complete_commit_step_for_engine(
    engine: &Arc<RwLock<StorageEngine<OsFs>>>,
    step: io::Result<CommitStep<OsFile>>,
    #[cfg(test)] fail_sync: Option<Arc<AtomicBool>>,
) -> io::Result<()> {
    // The device flush runs without the engine lock so that readers are not held up by it.
    step.and_then(|step| match step {
        CommitStep::Done => Ok(()),
        CommitStep::AwaitingSync(file) => {
            struct PendingSyncGuard<'a> {
                engine: &'a Arc<RwLock<StorageEngine<OsFs>>>,
                armed: bool,
            }
            impl Drop for PendingSyncGuard<'_> {
                fn drop(&mut self) {
                    if self.armed {
                        if let Ok(mut eng) = self.engine.write() {
                            let _ = eng.finish_commit_for_conn(Err(io::Error::other(
                                "WAL sync aborted before completion",
                            )));
                        }
                    }
                }
            }
            let mut guard = PendingSyncGuard {
                engine,
                armed: true,
            };
            let flushed = run_commit_sync_barrier(
                &file,
                #[cfg(test)]
                fail_sync.as_ref(),
            );
            let res = match engine.write() {
                Ok(mut eng) => eng.finish_commit_for_conn(flushed),
                Err(_) => Err(io::Error::other("engine lock poisoned")),
            };
            guard.armed = false;
            res
        }
        CommitStep::AwaitingGroup { lsn, file, gate } => loop {
            match gate.join_group(lsn)? {
                GroupRole::Durable => break Ok(()),
                GroupRole::Lead(target) => {
                    let mut leader_guard = gate.leader_guard();
                    let flushed = match gate.lock_sync() {
                        Ok(_sync_guard) => {
                            if let Some(err) = gate.failed_error() {
                                Err(err)
                            } else if gate.is_durable_through(target) {
                                Ok(())
                            } else {
                                let res = run_commit_sync_barrier(
                                    &file,
                                    #[cfg(test)]
                                    fail_sync.as_ref(),
                                );
                                if let Err(ref e) = res {
                                    // Record the failure before dropping `sync_mu` so a concurrent
                                    // `drain_pending_durable` cannot issue a second `sync_barrier`
                                    // after an `errseq_t`-clearing error.
                                    gate.fail(e);
                                }
                                res
                            }
                        }
                        Err(e) => {
                            gate.fail(&e);
                            Err(e)
                        }
                    };
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

fn complete_commit_step(
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
        vtab.set_err(&e.to_string());
        return io_err_to_sqlite_rc(&e);
    }
    SQLITE_OK
}

pub(in crate::vtab) unsafe extern "system" fn vtab_sync_tx(p_vtab: *mut sqlite3_vtab) -> c_int {
    catch_unwind(AssertUnwindSafe(|| {
        let Some(vtab) = (unsafe { AydbVTab::from_raw(p_vtab) }) else {
            return SQLITE_ERROR;
        };
        let conn_id = vtab.conn_id();
        if let Err(e) = vtab.check_schema_renames() {
            vtab.set_err(&e.to_string());
            return io_err_to_sqlite_rc(&e);
        }
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
            if !eng.conn_has_open_transaction(conn_id) {
                return SQLITE_OK;
            }
            if !eng.mark_vtab_synced_ready_to_commit(p_vtab as usize, vtab.durability) {
                return SQLITE_OK;
            }
            let commit_durability = eng.effective_tx_durability(vtab.durability);
            // Tables this transaction created (even if later renamed) whose creation a `ROLLBACK TO`
            // has since undone in SQLite's schema must not be committed.
            eng = match unsafe {
                prune_stale_created_vtabs_unlocked(vtab.db, &vtab.engine, conn_id, eng)
            } {
                Ok(g) => g,
                Err(e) => {
                    vtab.set_err(&e.to_string());
                    return io_err_to_sqlite_rc(&e);
                }
            };
            if !eng.conn_has_open_transaction(conn_id) {
                return SQLITE_OK;
            }
            if let Err(e) = eng.prepare_commit_transaction(commit_durability) {
                vtab.set_err(&e.to_string());
                return io_err_to_sqlite_rc(&e);
            }
            if vtab.txn_in_write() {
                let durability = eng.effective_tx_durability(commit_durability);
                if !vtab.pending_ddl.arm_tx_commit(TxCommitHook {
                    engine: Arc::clone(&vtab.engine),
                    conn_id,
                    durability,
                }) {
                    return SQLITE_ERROR;
                }
                return SQLITE_OK;
            }
            #[cfg(test)]
            {
                fail_sync_flag = Some(eng.debug_fail_wal_sync_flag());
            }
            eng.begin_commit_for_conn(conn_id)
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

pub(in crate::vtab) unsafe extern "system" fn vtab_commit_tx(p_vtab: *mut sqlite3_vtab) -> c_int {
    catch_unwind(AssertUnwindSafe(|| {
        let Some(vtab) = (unsafe { AydbVTab::from_raw(p_vtab) }) else {
            return SQLITE_ERROR;
        };
        let conn_id = vtab.conn_id();
        vtab.pending_ddl.clear_explicit_tx();
        // SQLite invokes `xCommit` once per virtual table in `aVTrans`. Normal transactions
        // already appended, flushed, and published in `xSync` or `ddl_commit_hook`, so
        // `conn_has_open_transaction` is false here; keep prepare+commit as a fallback only if an
        // active engine transaction remains.
        #[cfg(test)]
        let mut fail_sync_flag: Option<Arc<AtomicBool>> = None;
        let step = match vtab.engine.write() {
            Ok(mut eng) => {
                if !eng.conn_has_open_transaction(conn_id) {
                    Ok(CommitStep::Done)
                } else {
                    let durability = eng.effective_tx_durability(vtab.durability);
                    if let Err(e) = eng.prepare_commit_transaction(durability) {
                        vtab.set_err(&e.to_string());
                        return io_err_to_sqlite_rc(&e);
                    }
                    #[cfg(test)]
                    {
                        fail_sync_flag = Some(eng.debug_fail_wal_sync_flag());
                    }
                    eng.begin_commit_for_conn(conn_id)
                }
            }
            Err(_) => return SQLITE_ERROR,
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

pub(in crate::vtab) unsafe extern "system" fn vtab_rollback_tx(p_vtab: *mut sqlite3_vtab) -> c_int {
    catch_unwind(AssertUnwindSafe(|| {
        let Some(vtab) = (unsafe { AydbVTab::from_raw(p_vtab) }) else {
            return SQLITE_ERROR;
        };
        let conn_id = vtab.conn_id();
        vtab.pending_ddl.disarm_tx_commit();
        let restored_name = {
            let mut eng = match vtab.engine.write() {
                Ok(g) => g,
                Err(_) => return SQLITE_ERROR,
            };
            if eng.conn_has_open_transaction(conn_id) {
                if let Err(e) = eng.rollback_transaction_for_conn(conn_id) {
                    return io_err_to_sqlite_rc(&e);
                }
            }
            vtab.pending_ddl.sync_tx_renamed_handles(&eng);
            eng.active_vtab_name(vtab.table_id)
        };
        vtab.pending_ddl.clear_explicit_tx();
        if let Some(actual) = restored_name {
            if let Ok(mut name) = vtab.table_name.lock() {
                *name = actual;
            }
        }
        SQLITE_OK
    }))
    .unwrap_or(SQLITE_ERROR)
}

pub(in crate::vtab) unsafe extern "system" fn vtab_savepoint(
    p_vtab: *mut sqlite3_vtab,
    i_savepoint: c_int,
) -> c_int {
    catch_unwind(AssertUnwindSafe(|| {
        let Some(vtab) = (unsafe { AydbVTab::from_raw(p_vtab) }) else {
            return SQLITE_ERROR;
        };
        let conn_id = vtab.conn_id();
        vtab.pending_ddl.disarm_tx_commit();
        let mut eng = match vtab.engine.write() {
            Ok(g) => g,
            Err(_) => return SQLITE_ERROR,
        };
        if let Err(e) = eng.savepoint_for_conn(conn_id, i_savepoint) {
            return io_err_to_sqlite_rc(&e);
        }
        SQLITE_OK
    }))
    .unwrap_or(SQLITE_ERROR)
}

pub(in crate::vtab) unsafe extern "system" fn vtab_release(
    p_vtab: *mut sqlite3_vtab,
    i_savepoint: c_int,
) -> c_int {
    catch_unwind(AssertUnwindSafe(|| {
        let Some(vtab) = (unsafe { AydbVTab::from_raw(p_vtab) }) else {
            return SQLITE_ERROR;
        };
        let conn_id = vtab.conn_id();
        vtab.pending_ddl.disarm_tx_commit();
        let mut eng = match vtab.engine.write() {
            Ok(g) => g,
            Err(_) => return SQLITE_ERROR,
        };
        if !eng.conn_has_open_transaction(conn_id) {
            return SQLITE_OK;
        }
        if let Err(e) = eng.release_savepoint_for_conn(conn_id, i_savepoint) {
            return io_err_to_sqlite_rc(&e);
        }
        SQLITE_OK
    }))
    .unwrap_or(SQLITE_ERROR)
}

pub(in crate::vtab) unsafe extern "system" fn vtab_rollback_to(
    p_vtab: *mut sqlite3_vtab,
    i_savepoint: c_int,
) -> c_int {
    catch_unwind(AssertUnwindSafe(|| {
        let Some(vtab) = (unsafe { AydbVTab::from_raw(p_vtab) }) else {
            return SQLITE_ERROR;
        };
        let conn_id = vtab.conn_id();
        vtab.pending_ddl.disarm_tx_commit();
        if let Err(e) = vtab.check_schema_renames() {
            vtab.set_err(&e.to_string());
            return io_err_to_sqlite_rc(&e);
        }
        let mut eng = match vtab.engine.write() {
            Ok(g) => g,
            Err(_) => return SQLITE_ERROR,
        };
        if let Err(e) = eng.rollback_to_savepoint_for_conn(conn_id, i_savepoint) {
            return io_err_to_sqlite_rc(&e);
        }
        vtab.pending_ddl.sync_tx_renamed_handles(&eng);
        if let Some(actual) = eng.active_vtab_name(vtab.table_id) {
            if let Ok(mut name) = vtab.table_name.lock() {
                *name = actual;
            }
        }
        SQLITE_OK
    }))
    .unwrap_or(SQLITE_ERROR)
}
