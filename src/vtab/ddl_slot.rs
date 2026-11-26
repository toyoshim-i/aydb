//! Per-connection DDL and commit-hook state machine (`ConnDdlSlot`).
//!
//! Tracks staged autocommit DDL (`CREATE` / `DROP` / `ALTER TABLE ... RENAME`) and explicit
//! transaction commit/rename hooks under a single leaf mutex so SQLite commit/rollback hooks
//! stay atomic with the storage engine.

use core::ffi::c_int;
use std::io;
#[cfg(test)]
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, RwLock};

use crate::engine::{StorageEngine, WriteDurability};
use crate::pal::OsFs;
use crate::vtab::catalog_probe::schema_has_aydb_table_exact;
use crate::vtab::ffi::sqlite3;
use crate::vtab::tx_callbacks::complete_commit_step_for_engine;

pub(in crate::vtab) struct TxCommitHook {
    pub(in crate::vtab) engine: Arc<RwLock<StorageEngine<OsFs>>>,
    pub(in crate::vtab) conn_id: usize,
    pub(in crate::vtab) durability: WriteDurability,
}

pub(in crate::vtab) enum AutocommitDdl {
    Create(TxCommitHook),
    Drop {
        commit: TxCommitHook,
        destroyed_token: (u16, u64),
    },
    Rename {
        commit: TxCommitHook,
        table_name: Arc<std::sync::Mutex<String>>,
        old_name: String,
    },
}

pub(in crate::vtab) struct InTxRename {
    pub(in crate::vtab) engine: Arc<RwLock<StorageEngine<OsFs>>>,
    pub(in crate::vtab) conn_id: usize,
    pub(in crate::vtab) table_id: u16,
    pub(in crate::vtab) db_name: String,
    pub(in crate::vtab) old_name: String,
    pub(in crate::vtab) new_name: String,
    pub(in crate::vtab) table_name: Arc<std::sync::Mutex<String>>,
    pub(in crate::vtab) durability: WriteDurability,
}

enum ConnDdlPhase {
    Idle,
    StagedAutocommit(AutocommitDdl),
    ExplicitTx {
        commit_hook: Option<TxCommitHook>,
        renames: Vec<InTxRename>,
        renamed_handles: Vec<(u16, Arc<std::sync::Mutex<String>>)>,
    },
}

struct ConnDdlInner {
    phase: ConnDdlPhase,
    last_error: Option<String>,
}

enum PendingCommit {
    Autocommit(AutocommitDdl),
    ExplicitTx(TxCommitHook),
}

/// Per-connection DDL and commit-hook state protected by a single leaf mutex.
///
/// Lock-ordering invariant: `self.inner` MUST remain a leaf lock and is NEVER held across
/// `engine.write()`, `table_name.lock()`, or SQLite C API calls.
pub(in crate::vtab) struct ConnDdlSlot {
    inner: std::sync::Mutex<ConnDdlInner>,
}

impl ConnDdlSlot {
    pub(in crate::vtab) fn new() -> Self {
        Self {
            inner: std::sync::Mutex::new(ConnDdlInner {
                phase: ConnDdlPhase::Idle,
                last_error: None,
            }),
        }
    }

    pub(in crate::vtab) fn stage_autocommit(&self, ddl: AutocommitDdl) -> bool {
        let Ok(mut inner) = self.inner.lock() else {
            return false;
        };
        inner.last_error = None;
        if !matches!(inner.phase, ConnDdlPhase::Idle) {
            return false;
        }
        inner.phase = ConnDdlPhase::StagedAutocommit(ddl);
        true
    }

    pub(in crate::vtab) fn arm_tx_commit(&self, hook: TxCommitHook) -> bool {
        let Ok(mut inner) = self.inner.lock() else {
            return false;
        };
        inner.last_error = None;
        match &mut inner.phase {
            ConnDdlPhase::Idle => {
                inner.phase = ConnDdlPhase::ExplicitTx {
                    commit_hook: Some(hook),
                    renames: Vec::new(),
                    renamed_handles: Vec::new(),
                };
                true
            }
            ConnDdlPhase::ExplicitTx { commit_hook, .. } => {
                *commit_hook = Some(hook);
                true
            }
            // SQLite invokes `xSync` on a newly created virtual table before `sqlite3_commit_hook`
            // for autocommit `CREATE VIRTUAL TABLE`. Allow refreshing the staged `Create` hook for
            // the same connection while still rejecting staged `Drop` and `Rename`.
            ConnDdlPhase::StagedAutocommit(AutocommitDdl::Create(existing))
                if existing.conn_id == hook.conn_id =>
            {
                *existing = hook;
                true
            }
            ConnDdlPhase::StagedAutocommit(_) => false,
        }
    }

    pub(in crate::vtab) fn disarm_tx_commit(&self) {
        if let Ok(mut inner) = self.inner.lock() {
            if let ConnDdlPhase::ExplicitTx {
                commit_hook,
                renames,
                renamed_handles,
            } = &mut inner.phase
            {
                *commit_hook = None;
                if renames.is_empty() && renamed_handles.is_empty() {
                    inner.phase = ConnDdlPhase::Idle;
                }
            }
        }
    }

    pub(in crate::vtab) fn record_in_tx_rename(&self, item: InTxRename) -> bool {
        let Ok(mut inner) = self.inner.lock() else {
            return false;
        };
        let handle_entry = (item.table_id, Arc::clone(&item.table_name));
        match &mut inner.phase {
            ConnDdlPhase::Idle => {
                inner.phase = ConnDdlPhase::ExplicitTx {
                    commit_hook: None,
                    renames: vec![item],
                    renamed_handles: vec![handle_entry],
                };
                true
            }
            ConnDdlPhase::ExplicitTx {
                renames,
                renamed_handles,
                ..
            } => {
                if !renamed_handles
                    .iter()
                    .any(|(tid, h)| *tid == handle_entry.0 && Arc::ptr_eq(h, &handle_entry.1))
                {
                    renamed_handles.push(handle_entry);
                }
                renames.push(item);
                true
            }
            ConnDdlPhase::StagedAutocommit(_) => false,
        }
    }

    pub(in crate::vtab) fn clear_explicit_tx(&self) {
        if let Ok(mut inner) = self.inner.lock() {
            if matches!(inner.phase, ConnDdlPhase::ExplicitTx { .. }) {
                inner.phase = ConnDdlPhase::Idle;
            }
        }
    }

    fn apply_renamed_handles(
        eng: &StorageEngine<OsFs>,
        handles: &[(u16, Arc<std::sync::Mutex<String>>)],
    ) {
        for (tid, handle) in handles {
            if let Some(actual) = eng.active_vtab_name(*tid) {
                if let Ok(mut name) = handle.lock() {
                    *name = actual;
                }
            }
        }
    }

    pub(in crate::vtab) fn sync_tx_renamed_handles(&self, eng: &StorageEngine<OsFs>) {
        let handles = {
            let Ok(inner) = self.inner.lock() else {
                return;
            };
            let ConnDdlPhase::ExplicitTx {
                renamed_handles, ..
            } = &inner.phase
            else {
                return;
            };
            renamed_handles.clone()
        };
        Self::apply_renamed_handles(eng, &handles);
    }

    /// Reconciles in-transaction `ALTER TABLE ... RENAME TO` operations whose schema change SQLite
    /// silently rolled back via its statement journal (for instance because a view or trigger was
    /// invalid), where SQLite invokes neither `xRollback` nor `xRollbackTo`.
    pub(in crate::vtab) unsafe fn reconcile_in_tx_renames(
        &self,
        db: *mut sqlite3,
    ) -> io::Result<()> {
        let items: Vec<InTxRename> = {
            let Ok(mut inner) = self.inner.lock() else {
                return Err(io::Error::other(
                    "pending DDL rename queue mutex poisoned during reconcile",
                ));
            };
            let ConnDdlPhase::ExplicitTx { renames, .. } = &mut inner.phase else {
                return Ok(());
            };
            if renames.is_empty() {
                return Ok(());
            }
            core::mem::take(renames)
        };
        let mut retry = Vec::new();
        let mut first_err: Option<io::Error> = None;
        for item in items.into_iter().rev() {
            let has_new = unsafe { schema_has_aydb_table_exact(db, &item.db_name, &item.new_name) };
            let has_old = unsafe { schema_has_aydb_table_exact(db, &item.db_name, &item.old_name) };
            match (has_new, has_old) {
                (Some(false), Some(true)) => {
                    let revert_res = {
                        match item.engine.write() {
                            Ok(mut eng) => {
                                let new_norm = crate::engine::normalize_vtab_ident(&item.new_name);
                                if eng.conn_has_open_transaction(item.conn_id)
                                    && eng.active_vtab_name(item.table_id).as_deref()
                                        == Some(new_norm.as_str())
                                {
                                    eng.revert_in_tx_rename_vtab(
                                        item.table_id,
                                        &item.new_name,
                                        &item.old_name,
                                        item.durability,
                                    )
                                } else {
                                    Ok(())
                                }
                            }
                            Err(_) => Err(io::Error::other(
                                "engine lock poisoned while reverting in-transaction table rename",
                            )),
                        }
                    };
                    match revert_res {
                        Ok(()) => {
                            if let Ok(mut name) = item.table_name.lock() {
                                *name = item.old_name;
                            }
                        }
                        Err(e) => {
                            if first_err.is_none() {
                                first_err = Some(e);
                            }
                            retry.push(item);
                        }
                    }
                }
                (None, _) | (_, None) => {
                    retry.push(item);
                }
                _ => {}
            }
        }
        if !retry.is_empty() {
            retry.reverse();
            if let Ok(mut inner) = self.inner.lock() {
                if let ConnDdlPhase::ExplicitTx { renames, .. } = &mut inner.phase {
                    retry.append(renames);
                    *renames = retry;
                }
            }
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    pub(in crate::vtab) fn is_occupied(&self) -> bool {
        self.inner
            .lock()
            .map_or(true, |inner| !matches!(inner.phase, ConnDdlPhase::Idle))
    }

    pub(in crate::vtab) fn clear_error(&self) {
        if let Ok(mut inner) = self.inner.lock() {
            inner.last_error = None;
        }
    }

    fn set_error(&self, msg: String) {
        if let Ok(mut inner) = self.inner.lock() {
            inner.last_error = Some(msg);
        }
    }

    pub(in crate::vtab) fn take_error(&self) -> Option<String> {
        self.inner
            .lock()
            .ok()
            .and_then(|mut inner| inner.last_error.take())
    }

    fn cleanup_aborted_autocommit(ddl: AutocommitDdl, rollback: bool) {
        match ddl {
            AutocommitDdl::Create(commit) => {
                if rollback {
                    if let Ok(mut eng) = commit.engine.write() {
                        if eng.conn_has_open_transaction(commit.conn_id) {
                            let _ = eng.rollback_transaction_for_conn(commit.conn_id);
                        }
                    }
                }
            }
            AutocommitDdl::Drop {
                commit,
                destroyed_token: (tid, tok),
            } => {
                if let Ok(mut eng) = commit.engine.write() {
                    if rollback && eng.conn_has_open_transaction(commit.conn_id) {
                        let _ = eng.rollback_transaction_for_conn(commit.conn_id);
                    }
                    eng.unregister_vtab_token(tid, tok);
                }
            }
            AutocommitDdl::Rename {
                commit,
                table_name,
                old_name,
            } => {
                if rollback {
                    if let Ok(mut eng) = commit.engine.write() {
                        if eng.conn_has_open_transaction(commit.conn_id) {
                            let _ = eng.rollback_transaction_for_conn(commit.conn_id);
                        }
                    }
                }
                if let Ok(mut name) = table_name.lock() {
                    *name = old_name;
                }
            }
        }
    }

    fn cleanup_aborted_commit(pending: PendingCommit, rollback: bool) {
        match pending {
            PendingCommit::Autocommit(ddl) => Self::cleanup_aborted_autocommit(ddl, rollback),
            PendingCommit::ExplicitTx(hook) => {
                // Keep `ExplicitTx::renamed_handles` intact so SQLite's immediate `xRollback` can
                // synchronize any remaining `AydbVTab::table_name` handles.
                if rollback {
                    if let Ok(mut eng) = hook.engine.write() {
                        if eng.conn_has_open_transaction(hook.conn_id) {
                            let _ = eng.rollback_transaction_for_conn(hook.conn_id);
                        }
                    }
                }
            }
        }
    }

    pub(in crate::vtab) fn commit_pending(&self) -> c_int {
        let pending = {
            let Ok(mut inner) = self.inner.lock() else {
                return 1;
            };
            inner.last_error = None;
            match &mut inner.phase {
                ConnDdlPhase::Idle => return 0,
                ConnDdlPhase::StagedAutocommit(_) => {
                    let prev = core::mem::replace(&mut inner.phase, ConnDdlPhase::Idle);
                    let ConnDdlPhase::StagedAutocommit(ddl) = prev else {
                        unreachable!();
                    };
                    PendingCommit::Autocommit(ddl)
                }
                ConnDdlPhase::ExplicitTx { commit_hook, .. } => {
                    let Some(hook) = commit_hook.take() else {
                        return 0;
                    };
                    PendingCommit::ExplicitTx(hook)
                }
            }
        };
        let (engine, conn_id, durability) = match &pending {
            PendingCommit::Autocommit(
                AutocommitDdl::Create(h)
                | AutocommitDdl::Drop { commit: h, .. }
                | AutocommitDdl::Rename { commit: h, .. },
            )
            | PendingCommit::ExplicitTx(h) => (Arc::clone(&h.engine), h.conn_id, h.durability),
        };
        #[cfg(test)]
        let fail_sync: Option<Arc<AtomicBool>>;
        let step = {
            let mut eng = match engine.write() {
                Ok(g) => g,
                Err(_) => {
                    Self::cleanup_aborted_commit(pending, false);
                    self.set_error("engine lock poisoned".to_string());
                    return 1;
                }
            };
            if !eng.conn_has_open_transaction(conn_id) {
                drop(eng);
                if matches!(pending, PendingCommit::ExplicitTx(_)) {
                    self.clear_explicit_tx();
                }
                return 0;
            }
            if let Err(e) = eng.prepare_commit_transaction(durability) {
                drop(eng);
                Self::cleanup_aborted_commit(pending, true);
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
            Self::cleanup_aborted_commit(pending, true);
            self.set_error(e.to_string());
            return 1;
        }
        if matches!(pending, PendingCommit::ExplicitTx(_)) {
            self.clear_explicit_tx();
        }
        0
    }

    pub(in crate::vtab) fn rollback_pending(&self) {
        let prev = match self.inner.lock() {
            Ok(mut inner) => core::mem::replace(&mut inner.phase, ConnDdlPhase::Idle),
            Err(_) => return,
        };
        match prev {
            ConnDdlPhase::Idle => {}
            ConnDdlPhase::StagedAutocommit(ddl) => Self::cleanup_aborted_autocommit(ddl, true),
            ConnDdlPhase::ExplicitTx {
                commit_hook,
                renamed_handles,
                ..
            } => {
                if let Some(hook) = commit_hook {
                    if let Ok(mut eng) = hook.engine.write() {
                        if eng.conn_has_open_transaction(hook.conn_id) {
                            let _ = eng.rollback_transaction_for_conn(hook.conn_id);
                        }
                        Self::apply_renamed_handles(&eng, &renamed_handles);
                    }
                }
            }
        }
    }
}
