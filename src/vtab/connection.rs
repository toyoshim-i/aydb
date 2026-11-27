//! Safe, ergonomic Rust wrappers for a native `sqlite3*` database connection and prepared statements.

#[cfg(test)]
#[path = "snapshot_cursor_tests.rs"]
mod snapshot_cursor_tests;

use core::ffi::{c_int, c_void};
use std::ffi::{CStr, CString};
use std::io;
use std::path::Path;
use std::sync::{Arc, RwLock};

use crate::engine::StorageEngine;
use crate::pal::OsFs;
use crate::vtab::ffi::{
    SQLITE_DONE, SQLITE_FLOAT, SQLITE_INTEGER, SQLITE_NULL, SQLITE_OK, SQLITE_ROW, SQLITE_TEXT,
    SQLITE_TOOBIG, SqliteStmtGuard, sqlite3, sqlite3_close, sqlite3_commit_hook,
    sqlite3_create_module_v2, sqlite3_errmsg, sqlite3_exec, sqlite3_open, sqlite3_rollback_hook,
};
use crate::vtab::row_codec::SqlValue;
use crate::vtab::snapshot::{
    ReadSession, ReadTrace, TRACE_PROFILE, TRACE_STATEMENT, ddl_commit_hook, ddl_rollback_hook,
    trace_reads,
};
use crate::vtab::{AYDB_VTAB_MODULE, ModuleAuxContext, destroy_aux_context};

/// Safe, ergonomic wrapper around a native `sqlite3*` database connection with `aydb`
/// Virtual Table registration support.
pub struct SqliteConnection {
    db: *mut sqlite3,
    read_trace: *mut ReadTrace,
}

impl SqliteConnection {
    /// Opens an in-memory SQLite database (`:memory:`).
    pub fn open_in_memory() -> io::Result<Self> {
        Self::open_path(":memory:")
    }

    /// Opens a file-backed SQLite database at `path`.
    /// Returns `InvalidInput` if the path cannot be represented by SQLite's UTF-8 API.
    pub fn open_file(path: &Path) -> io::Result<Self> {
        let path = path.to_str().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "SQLite path is not valid UTF-8",
            )
        })?;
        Self::open_path(path)
    }

    fn open_path(path_str: &str) -> io::Result<Self> {
        let c_path = CString::new(path_str)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "NUL byte in SQLite path"))?;
        let mut db: *mut sqlite3 = core::ptr::null_mut();
        let rc = unsafe { sqlite3_open(c_path.as_ptr(), &mut db) };
        if rc != SQLITE_OK || db.is_null() {
            if !db.is_null() {
                unsafe {
                    sqlite3_close(db);
                }
            }
            return Err(io::Error::other(format!(
                "sqlite3_open failed with rc={rc}"
            )));
        }
        let read_trace = Box::into_raw(Box::new(ReadTrace::new(db)));
        let rc = unsafe {
            crate::vtab::ffi::sqlite3_trace_v2(
                db,
                TRACE_STATEMENT | TRACE_PROFILE,
                Some(trace_reads),
                read_trace.cast(),
            )
        };
        if rc != SQLITE_OK {
            unsafe {
                sqlite3_close(db);
                drop(Box::from_raw(read_trace));
            }
            return Err(io::Error::other(
                "could not register SQLite read lifecycle trace",
            ));
        }
        unsafe {
            sqlite3_commit_hook(db, Some(ddl_commit_hook), read_trace.cast());
            sqlite3_rollback_hook(db, Some(ddl_rollback_hook), read_trace.cast());
        }
        Ok(Self { db, read_trace })
    }

    /// Registers the `"aydb"` Virtual Table module (`USING aydb(...)`) backed by `engine`.
    ///
    /// Index compaction runs on a dedicated background thread (merge executed with the engine
    /// mutex released) so other connections are not blocked behind a long merge. The thread is
    /// joined when the module registration is dropped (connection close). Use
    /// [`Self::register_aydb_module_with_options`] to keep compaction inline instead.
    pub fn register_aydb_module(&self, engine: Arc<RwLock<StorageEngine<OsFs>>>) -> io::Result<()> {
        self.register_aydb_module_with_options(engine, true)
    }

    /// Like [`Self::register_aydb_module`], choosing whether to spawn the background compaction
    /// worker. With `false`, the engine's own `background_compaction` setting is left untouched
    /// (default: inline compaction inside the write path).
    pub fn register_aydb_module_with_options(
        &self,
        engine: Arc<RwLock<StorageEngine<OsFs>>>,
        background_compaction: bool,
    ) -> io::Result<()> {
        let c_name = CString::new("aydb").unwrap();
        let worker = if background_compaction {
            crate::vtab::compaction_worker::CompactionWorker::spawn(Arc::clone(&engine))
        } else {
            None
        };
        let read_session = Arc::new(ReadSession::new(Arc::clone(&engine)));
        let pending_ddl = unsafe {
            (*self.read_trace).add_session(&read_session);
            (*self.read_trace).pending_ddl()
        };
        let aux = Box::new(ModuleAuxContext {
            engine,
            read_session,
            pending_ddl,
            _compaction_worker: worker,
        });
        let rc = unsafe {
            sqlite3_create_module_v2(
                self.db,
                c_name.as_ptr(),
                &AYDB_VTAB_MODULE,
                Box::into_raw(aux) as *mut c_void,
                Some(destroy_aux_context),
            )
        };
        if rc != SQLITE_OK {
            return Err(self.last_error("sqlite3_create_module_v2"));
        }
        Ok(())
    }

    /// Executes one or more SQL statements that do not return rows (`CREATE`, `INSERT`, `PRAGMA`, etc.).
    pub fn execute_batch(&self, sql: &str) -> io::Result<()> {
        let c_sql = CString::new(sql)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "NUL byte in SQL"))?;
        let rc = unsafe {
            sqlite3_exec(
                self.db,
                c_sql.as_ptr(),
                None,
                core::ptr::null_mut(),
                core::ptr::null_mut(),
            )
        };
        if rc != SQLITE_OK {
            return Err(self.last_error("sqlite3_exec"));
        }
        Ok(())
    }

    /// Prepares a parameterized SQL statement for repeated low-overhead execution.
    pub fn prepare(&self, sql: &str) -> io::Result<SqliteStatement<'_>> {
        if sql.as_bytes().contains(&0) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "NUL byte in SQL",
            ));
        }
        let stmt = unsafe { SqliteStmtGuard::prepare(self.db, sql) }
            .map_err(|_| self.last_error("sqlite3_prepare_v2"))?;
        Ok(SqliteStatement { conn: self, stmt })
    }

    /// Executes a SQL query and collects all result rows as `Vec<Vec<SqlValue>>`.
    pub fn query_all(&self, sql: &str, params: &[SqlValue]) -> io::Result<Vec<Vec<SqlValue>>> {
        let mut stmt = self.prepare(sql)?;
        stmt.query(params)
    }

    pub(super) fn last_error(&self, context: &str) -> io::Error {
        let ddl_err = if !self.read_trace.is_null() {
            unsafe { (*self.read_trace).take_ddl_error() }
        } else {
            None
        };
        let msg = unsafe {
            let ptr = sqlite3_errmsg(self.db);
            if ptr.is_null() {
                "unknown SQLite error".to_string()
            } else {
                CStr::from_ptr(ptr).to_string_lossy().into_owned()
            }
        };
        match ddl_err {
            Some(err) => io::Error::other(format!("{context}: {msg} ({err})")),
            None => io::Error::other(format!("{context}: {msg}")),
        }
    }
}

impl Drop for SqliteConnection {
    fn drop(&mut self) {
        if !self.db.is_null() {
            unsafe {
                crate::vtab::ffi::sqlite3_trace_v2(self.db, 0, None, core::ptr::null_mut());
                sqlite3_commit_hook(self.db, None, core::ptr::null_mut());
                sqlite3_rollback_hook(self.db, None, core::ptr::null_mut());
                if !self.read_trace.is_null() {
                    (*self.read_trace).abort_pending_ddl();
                    (*self.read_trace).clear();
                }
                sqlite3_close(self.db);
            }
        }
        if !self.read_trace.is_null() {
            unsafe {
                drop(Box::from_raw(self.read_trace));
            }
            self.read_trace = core::ptr::null_mut();
        }
    }
}

/// Prepared SQLite statement supporting reusable parameter binding (`execute` / `query`).
pub struct SqliteStatement<'a> {
    conn: &'a SqliteConnection,
    stmt: SqliteStmtGuard,
}

impl<'a> SqliteStatement<'a> {
    /// Binds parameters, steps the statement to completion (draining any returned rows, such as
    /// from `RETURNING`), and resets the statement.
    pub fn execute(&mut self, params: &[SqlValue]) -> io::Result<()> {
        self.bind_all(params)?;
        loop {
            let rc = self.stmt.step();
            if rc == SQLITE_ROW {
                continue;
            }
            if rc == SQLITE_DONE {
                break;
            }
            let err = self.conn.last_error("sqlite3_step(execute)");
            self.stmt.reset();
            return Err(err);
        }
        if self.stmt.reset() != SQLITE_OK {
            return Err(self.conn.last_error("sqlite3_reset(execute)"));
        }
        Ok(())
    }

    /// Binds parameters, collects all result rows, and resets the statement.
    pub fn query(&mut self, params: &[SqlValue]) -> io::Result<Vec<Vec<SqlValue>>> {
        self.bind_all(params)?;
        let col_count = self.stmt.column_count();
        let mut rows = Vec::new();
        loop {
            let rc = self.stmt.step();
            if rc == SQLITE_ROW {
                let mut row = Vec::with_capacity(col_count);
                for c in 0..col_count {
                    row.push(self.read_column(c as c_int));
                }
                rows.push(row);
            } else if rc == SQLITE_DONE {
                break;
            } else {
                let err = self.conn.last_error("sqlite3_step(query)");
                self.stmt.reset();
                return Err(err);
            }
        }
        if self.stmt.reset() != SQLITE_OK {
            return Err(self.conn.last_error("sqlite3_reset(query)"));
        }
        Ok(rows)
    }

    fn bind_all(&mut self, params: &[SqlValue]) -> io::Result<()> {
        self.stmt.reset();
        self.stmt.clear_bindings();
        for (i, p) in params.iter().enumerate() {
            let idx = (i + 1) as c_int;
            let rc = match p {
                SqlValue::Null => self.stmt.bind_null(idx),
                SqlValue::Integer(v) => self.stmt.bind_i64(idx, *v),
                SqlValue::Float(v) => self.stmt.bind_f64(idx, *v),
                SqlValue::Text(s) => {
                    let rc = self.stmt.bind_text(idx, s);
                    if rc == SQLITE_TOOBIG {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "Text parameter exceeds i32::MAX bytes",
                        ));
                    }
                    rc
                }
                SqlValue::Blob(b) => {
                    let rc = self.stmt.bind_blob(idx, b);
                    if rc == SQLITE_TOOBIG {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "Blob parameter exceeds i32::MAX bytes",
                        ));
                    }
                    rc
                }
            };
            if rc != SQLITE_OK {
                return Err(self.conn.last_error("sqlite3_bind"));
            }
        }
        Ok(())
    }

    fn read_column(&self, col: c_int) -> SqlValue {
        match self.stmt.column_type(col) {
            SQLITE_NULL => SqlValue::Null,
            SQLITE_INTEGER => SqlValue::Integer(self.stmt.column_i64(col)),
            SQLITE_FLOAT => SqlValue::Float(self.stmt.column_f64(col)),
            SQLITE_TEXT => {
                let s = self.stmt.column_text_bytes(col);
                SqlValue::Text(String::from_utf8_lossy(s).into_owned())
            }
            _ => SqlValue::Blob(self.stmt.column_blob_bytes(col).to_vec()),
        }
    }
}
