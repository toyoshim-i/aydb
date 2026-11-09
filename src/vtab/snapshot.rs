//! Connection and cursor lifetimes for committed read snapshots.

use core::ffi::{c_int, c_uint, c_void};
use std::cell::{Cell, RefCell};
use std::collections::HashSet;
use std::io;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex, RwLock, Weak};

use crate::engine::{ReadSnapshotToken, ReadView, StorageEngine};
use crate::pal::OsFs;

use super::ffi::{sqlite3, sqlite3_get_autocommit};

pub(super) const TRACE_STATEMENT: c_uint = 1;
pub(super) const TRACE_PROFILE: c_uint = 2;

/// All virtual tables on a connection use one committed view of their shared engine.
pub(super) struct ReadSession {
    engine: Arc<RwLock<StorageEngine<OsFs>>>,
    token: Mutex<Option<ReadSnapshotToken>>,
}

impl ReadSession {
    pub(super) fn new(engine: Arc<RwLock<StorageEngine<OsFs>>>) -> Self {
        Self {
            engine,
            token: Mutex::new(None),
        }
    }

    pub(super) fn current(&self) -> Option<ReadSnapshotToken> {
        self.token.lock().ok()?.clone()
    }

    pub(super) fn for_cursor(&self, connection: usize) -> io::Result<Option<ReadSnapshotToken>> {
        {
            let engine = self
                .engine
                .read()
                .map_err(|_| io::Error::other("engine lock poisoned"))?;
            if engine.in_active_transaction()
                && engine.read_view_for_conn(connection) == ReadView::Owner
            {
                return Ok(None);
            }
            if let Some(token) = self.current() {
                return Ok(Some(token));
            }
        }
        let mut engine = self
            .engine
            .write()
            .map_err(|_| io::Error::other("engine lock poisoned"))?;
        let mut token = self
            .token
            .lock()
            .map_err(|_| io::Error::other("snapshot lock poisoned"))?;
        Ok(Some(
            token
                .get_or_insert_with(|| engine.capture_read_snapshot())
                .clone(),
        ))
    }

    fn end_transaction(&self) {
        let token = self.token.lock().ok().and_then(|mut token| token.take());
        // SQLite can finish a nested statement while an engine callback holds this lock.
        if let Ok(mut engine) = self.engine.try_write() {
            if let Some(token) = token {
                engine.release_read_snapshot(token);
            } else {
                engine.cleanup_read_snapshots();
            }
        }
    }
}

impl Drop for ReadSession {
    fn drop(&mut self) {
        self.end_transaction();
    }
}

/// The connection owns this boxed context for the entire SQLite trace registration.
pub(super) struct ReadTrace {
    database: *mut sqlite3,
    sessions: RefCell<Vec<Weak<ReadSession>>>,
    statements: RefCell<HashSet<usize>>,
    autocommit: Cell<bool>,
}

impl ReadTrace {
    pub(super) fn new(database: *mut sqlite3) -> Self {
        Self {
            database,
            sessions: RefCell::new(Vec::new()),
            statements: RefCell::new(HashSet::new()),
            autocommit: Cell::new(true),
        }
    }

    pub(super) fn add_session(&self, session: &Arc<ReadSession>) {
        self.sessions.borrow_mut().push(Arc::downgrade(session));
    }

    pub(super) fn clear(&self) {
        self.sessions.borrow_mut().retain(|weak| {
            if let Some(session) = weak.upgrade() {
                session.end_transaction();
                true
            } else {
                false
            }
        });
    }
}

pub(super) unsafe extern "system" fn trace_reads(
    event: c_uint,
    context: *mut c_void,
    statement: *mut c_void,
    _detail: *mut c_void,
) -> c_int {
    let _ = catch_unwind(AssertUnwindSafe(|| unsafe {
        let trace = &*(context as *const ReadTrace);
        let autocommit = sqlite3_get_autocommit(trace.database) != 0;
        let previously_autocommit = trace.autocommit.replace(autocommit);
        let transaction_ended = autocommit && !previously_autocommit;
        let idle = {
            let mut statements = trace.statements.borrow_mut();
            if event == TRACE_STATEMENT {
                statements.insert(statement as usize);
            } else if event == TRACE_PROFILE {
                statements.remove(&(statement as usize));
            }
            statements.is_empty()
        };
        if transaction_ended || (autocommit && idle) {
            trace.clear();
        }
    }));
    0
}
