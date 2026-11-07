//! Safe, ergonomic Rust wrappers for a native `sqlite3*` database connection and prepared statements.

use core::ffi::{c_char, c_int, c_void};
use std::ffi::{CStr, CString};
use std::io;
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::engine::StorageEngine;
use crate::pal::OsFs;
use crate::vtab::ffi::{
    SQLITE_DONE, SQLITE_FLOAT, SQLITE_INTEGER, SQLITE_NULL, SQLITE_OK, SQLITE_ROW,
    SQLITE_TEXT, sqlite_transient, sqlite3, sqlite3_bind_blob, sqlite3_bind_double,
    sqlite3_bind_int64, sqlite3_bind_null, sqlite3_bind_text, sqlite3_clear_bindings,
    sqlite3_close, sqlite3_column_blob, sqlite3_column_bytes, sqlite3_column_count,
    sqlite3_column_double, sqlite3_column_int64, sqlite3_column_text, sqlite3_column_type,
    sqlite3_create_module_v2, sqlite3_errmsg, sqlite3_exec, sqlite3_finalize, sqlite3_open,
    sqlite3_prepare_v2, sqlite3_reset, sqlite3_step, sqlite3_stmt,
};
use crate::vtab::row_codec::SqlValue;
use crate::vtab::{AYDB_VTAB_MODULE, ModuleAuxContext, destroy_aux_context};

/// Safe, ergonomic wrapper around a native `sqlite3*` database connection with `aydb`
/// Virtual Table registration support.
pub struct SqliteConnection {
    db: *mut sqlite3,
}

impl SqliteConnection {
    /// Opens an in-memory SQLite database (`:memory:`).
    pub fn open_in_memory() -> io::Result<Self> {
        Self::open_path(":memory:")
    }

    /// Opens a file-backed SQLite database at `path`.
    pub fn open_file(path: &Path) -> io::Result<Self> {
        Self::open_path(&path.to_string_lossy())
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
            return Err(io::Error::other(format!("sqlite3_open failed with rc={rc}")));
        }
        Ok(Self { db })
    }

    /// Registers the `"aydb"` Virtual Table module (`USING aydb(...)`) backed by `engine`.
    pub fn register_aydb_module(
        &self,
        engine: Arc<Mutex<StorageEngine<OsFs>>>,
    ) -> io::Result<()> {
        let c_name = CString::new("aydb").unwrap();
        let aux = Box::new(ModuleAuxContext { engine });
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
        if sql.len() > i32::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "SQL statement exceeds i32::MAX bytes",
            ));
        }
        let mut stmt: *mut sqlite3_stmt = core::ptr::null_mut();
        let rc = unsafe {
            sqlite3_prepare_v2(
                self.db,
                sql.as_ptr() as *const c_char,
                sql.len() as c_int,
                &mut stmt,
                core::ptr::null_mut(),
            )
        };
        if rc != SQLITE_OK || stmt.is_null() {
            return Err(self.last_error("sqlite3_prepare_v2"));
        }
        Ok(SqliteStatement { conn: self, stmt })
    }

    /// Executes a SQL query and collects all result rows as `Vec<Vec<SqlValue>>`.
    pub fn query_all(&self, sql: &str, params: &[SqlValue]) -> io::Result<Vec<Vec<SqlValue>>> {
        let mut stmt = self.prepare(sql)?;
        stmt.query(params)
    }

    pub(super) fn last_error(&self, context: &str) -> io::Error {
        let msg = unsafe {
            let ptr = sqlite3_errmsg(self.db);
            if ptr.is_null() {
                "unknown SQLite error".to_string()
            } else {
                CStr::from_ptr(ptr).to_string_lossy().into_owned()
            }
        };
        io::Error::other(format!("{context}: {msg}"))
    }
}

impl Drop for SqliteConnection {
    fn drop(&mut self) {
        if !self.db.is_null() {
            unsafe {
                sqlite3_close(self.db);
            }
        }
    }
}

/// Prepared SQLite statement supporting reusable parameter binding (`execute` / `query`).
pub struct SqliteStatement<'a> {
    conn: &'a SqliteConnection,
    stmt: *mut sqlite3_stmt,
}

impl<'a> SqliteStatement<'a> {
    /// Binds parameters, steps once (for `INSERT`/`UPDATE`/`DELETE`), and resets the statement.
    pub fn execute(&mut self, params: &[SqlValue]) -> io::Result<()> {
        self.bind_all(params)?;
        let rc = unsafe { sqlite3_step(self.stmt) };
        unsafe {
            sqlite3_reset(self.stmt);
        }
        if rc != SQLITE_DONE && rc != SQLITE_ROW {
            return Err(self.conn.last_error("sqlite3_step(execute)"));
        }
        Ok(())
    }

    /// Binds parameters, collects all result rows, and resets the statement.
    pub fn query(&mut self, params: &[SqlValue]) -> io::Result<Vec<Vec<SqlValue>>> {
        self.bind_all(params)?;
        let col_count = unsafe { sqlite3_column_count(self.stmt) as usize };
        let mut rows = Vec::new();
        loop {
            let rc = unsafe { sqlite3_step(self.stmt) };
            if rc == SQLITE_ROW {
                let mut row = Vec::with_capacity(col_count);
                for c in 0..col_count {
                    row.push(unsafe { self.read_column(c as c_int) });
                }
                rows.push(row);
            } else if rc == SQLITE_DONE {
                break;
            } else {
                unsafe {
                    sqlite3_reset(self.stmt);
                }
                return Err(self.conn.last_error("sqlite3_step(query)"));
            }
        }
        unsafe {
            sqlite3_reset(self.stmt);
        }
        Ok(rows)
    }

    fn bind_all(&mut self, params: &[SqlValue]) -> io::Result<()> {
        unsafe {
            sqlite3_reset(self.stmt);
            sqlite3_clear_bindings(self.stmt);
        }
        for (i, p) in params.iter().enumerate() {
            let idx = (i + 1) as c_int;
            let rc = unsafe {
                match p {
                    SqlValue::Null => sqlite3_bind_null(self.stmt, idx),
                    SqlValue::Integer(v) => sqlite3_bind_int64(self.stmt, idx, *v),
                    SqlValue::Float(v) => sqlite3_bind_double(self.stmt, idx, *v),
                    SqlValue::Text(s) => {
                        if s.len() > i32::MAX as usize {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidInput,
                                "Text parameter exceeds i32::MAX bytes",
                            ));
                        }
                        sqlite3_bind_text(
                            self.stmt,
                            idx,
                            s.as_ptr() as *const c_char,
                            s.len() as c_int,
                            sqlite_transient(),
                        )
                    }
                    SqlValue::Blob(b) => {
                        if b.len() > i32::MAX as usize {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidInput,
                                "Blob parameter exceeds i32::MAX bytes",
                            ));
                        }
                        sqlite3_bind_blob(
                            self.stmt,
                            idx,
                            b.as_ptr() as *const c_void,
                            b.len() as c_int,
                            sqlite_transient(),
                        )
                    }
                }
            };
            if rc != SQLITE_OK {
                return Err(self.conn.last_error("sqlite3_bind"));
            }
        }
        Ok(())
    }

    unsafe fn read_column(&self, col: c_int) -> SqlValue {
        unsafe {
            match sqlite3_column_type(self.stmt, col) {
                SQLITE_NULL => SqlValue::Null,
                SQLITE_INTEGER => SqlValue::Integer(sqlite3_column_int64(self.stmt, col)),
                SQLITE_FLOAT => SqlValue::Float(sqlite3_column_double(self.stmt, col)),
                SQLITE_TEXT => {
                    let len = sqlite3_column_bytes(self.stmt, col) as usize;
                    let ptr = sqlite3_column_text(self.stmt, col);
                    if ptr.is_null() || len == 0 {
                        SqlValue::Text(String::new())
                    } else {
                        let s = core::slice::from_raw_parts(ptr, len);
                        SqlValue::Text(String::from_utf8_lossy(s).into_owned())
                    }
                }
                _ => {
                    let len = sqlite3_column_bytes(self.stmt, col) as usize;
                    let ptr = sqlite3_column_blob(self.stmt, col) as *const u8;
                    if ptr.is_null() || len == 0 {
                        SqlValue::Blob(Vec::new())
                    } else {
                        let s = core::slice::from_raw_parts(ptr, len);
                        SqlValue::Blob(s.to_vec())
                    }
                }
            }
        }
    }
}

impl Drop for SqliteStatement<'_> {
    fn drop(&mut self) {
        if !self.stmt.is_null() {
            unsafe {
                sqlite3_finalize(self.stmt);
            }
        }
    }
}
