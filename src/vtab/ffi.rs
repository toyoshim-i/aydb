//! Zero-dependency FFI bindings to the operating system's native SQLite library
//! (`winsqlite3.dll` on Windows 10/11, `libsqlite3` on Unix/macOS).

#![allow(non_camel_case_types, non_snake_case, dead_code)]

use core::ffi::{c_char, c_double, c_int, c_uchar, c_void};

pub type sqlite3_int64 = i64;
pub type sqlite3_uint64 = u64;

#[repr(C)]
pub struct sqlite3 {
    _private: [u8; 0],
}

#[repr(C)]
pub struct sqlite3_stmt {
    _private: [u8; 0],
}

#[repr(C)]
pub struct sqlite3_value {
    _private: [u8; 0],
}

#[repr(C)]
pub struct sqlite3_context {
    _private: [u8; 0],
}

// SQLite result codes
pub const SQLITE_OK: c_int = 0;
pub const SQLITE_ERROR: c_int = 1;
pub const SQLITE_BUSY: c_int = 5;
pub const SQLITE_NOMEM: c_int = 7;
pub const SQLITE_READONLY: c_int = 8;
pub const SQLITE_IOERR: c_int = 10;
pub const SQLITE_CORRUPT: c_int = 11;
pub const SQLITE_FULL: c_int = 13;
pub const SQLITE_TOOBIG: c_int = 18;
pub const SQLITE_CONSTRAINT: c_int = 19;
pub const SQLITE_MISMATCH: c_int = 20;
pub const SQLITE_MISUSE: c_int = 21;
pub const SQLITE_ROW: c_int = 100;
pub const SQLITE_DONE: c_int = 101;

// SQLite conflict resolution modes returned by `sqlite3_vtab_on_conflict`
pub const SQLITE_ROLLBACK: c_int = 1;
pub const SQLITE_IGNORE: c_int = 2;
pub const SQLITE_FAIL: c_int = 3;
pub const SQLITE_ABORT: c_int = 4;
pub const SQLITE_REPLACE: c_int = 5;

// SQLite fundamental datatypes
pub const SQLITE_INTEGER: c_int = 1;
pub const SQLITE_FLOAT: c_int = 2;
pub const SQLITE_TEXT: c_int = 3;
pub const SQLITE_BLOB: c_int = 4;
pub const SQLITE_NULL: c_int = 5;

// Virtual table index constraint operators
pub const SQLITE_INDEX_CONSTRAINT_EQ: c_uchar = 2;
pub const SQLITE_INDEX_CONSTRAINT_GT: c_uchar = 4;
pub const SQLITE_INDEX_CONSTRAINT_LE: c_uchar = 8;
pub const SQLITE_INDEX_CONSTRAINT_LT: c_uchar = 16;
pub const SQLITE_INDEX_CONSTRAINT_GE: c_uchar = 32;
pub const SQLITE_INDEX_CONSTRAINT_LIMIT: c_uchar = 73;

pub const SQLITE_INDEX_SCAN_UNIQUE: c_int = 1;

/// `sqlite3_vtab_config` option: `xUpdate` returns `SQLITE_CONSTRAINT` before it changes anything.
pub const SQLITE_VTAB_CONSTRAINT_SUPPORT: c_int = 1;

// Transaction states returned by `sqlite3_txn_state`
pub const SQLITE_TXN_NONE: c_int = 0;
pub const SQLITE_TXN_READ: c_int = 1;
pub const SQLITE_TXN_WRITE: c_int = 2;

/// `SQLITE_TRANSIENT` instructs SQLite to make its own private copy of a string/blob immediately.
pub const fn sqlite_transient() -> Option<unsafe extern "system" fn(*mut c_void)> {
    unsafe { core::mem::transmute::<isize, Option<unsafe extern "system" fn(*mut c_void)>>(-1) }
}

#[repr(C)]
pub struct sqlite3_vtab {
    pub pModule: *const sqlite3_module,
    pub nRef: c_int,
    pub zErrMsg: *mut c_char,
}

#[repr(C)]
pub struct sqlite3_vtab_cursor {
    pub pVtab: *mut sqlite3_vtab,
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct sqlite3_index_constraint {
    pub iColumn: c_int,
    pub op: c_uchar,
    pub usable: c_uchar,
    pub iTermOffset: c_int,
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct sqlite3_index_orderby {
    pub iColumn: c_int,
    pub desc: c_uchar,
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct sqlite3_index_constraint_usage {
    pub argvIndex: c_int,
    pub omit: c_uchar,
}

#[repr(C)]
pub struct sqlite3_index_info {
    pub nConstraint: c_int,
    pub aConstraint: *const sqlite3_index_constraint,
    pub nOrderBy: c_int,
    pub aOrderBy: *const sqlite3_index_orderby,
    pub aConstraintUsage: *mut sqlite3_index_constraint_usage,
    pub idxNum: c_int,
    pub idxStr: *mut c_char,
    pub needToFreeIdxStr: c_int,
    pub orderByConsumed: c_int,
    pub estimatedCost: c_double,
    pub estimatedRows: sqlite3_int64,
    pub idxFlags: c_int,
    pub colUsed: sqlite3_uint64,
}

#[repr(C)]
pub struct sqlite3_module {
    pub iVersion: c_int,
    pub xCreate: Option<
        unsafe extern "system" fn(
            db: *mut sqlite3,
            pAux: *mut c_void,
            argc: c_int,
            argv: *const *const c_char,
            ppVTab: *mut *mut sqlite3_vtab,
            pzErr: *mut *mut c_char,
        ) -> c_int,
    >,
    pub xConnect: Option<
        unsafe extern "system" fn(
            db: *mut sqlite3,
            pAux: *mut c_void,
            argc: c_int,
            argv: *const *const c_char,
            ppVTab: *mut *mut sqlite3_vtab,
            pzErr: *mut *mut c_char,
        ) -> c_int,
    >,
    pub xBestIndex: Option<
        unsafe extern "system" fn(
            pVTab: *mut sqlite3_vtab,
            pInfo: *mut sqlite3_index_info,
        ) -> c_int,
    >,
    pub xDisconnect: Option<unsafe extern "system" fn(pVTab: *mut sqlite3_vtab) -> c_int>,
    pub xDestroy: Option<unsafe extern "system" fn(pVTab: *mut sqlite3_vtab) -> c_int>,
    pub xOpen: Option<
        unsafe extern "system" fn(
            pVTab: *mut sqlite3_vtab,
            ppCursor: *mut *mut sqlite3_vtab_cursor,
        ) -> c_int,
    >,
    pub xClose: Option<unsafe extern "system" fn(pCursor: *mut sqlite3_vtab_cursor) -> c_int>,
    pub xFilter: Option<
        unsafe extern "system" fn(
            pCursor: *mut sqlite3_vtab_cursor,
            idxNum: c_int,
            idxStr: *const c_char,
            argc: c_int,
            argv: *mut *mut sqlite3_value,
        ) -> c_int,
    >,
    pub xNext: Option<unsafe extern "system" fn(pCursor: *mut sqlite3_vtab_cursor) -> c_int>,
    pub xEof: Option<unsafe extern "system" fn(pCursor: *mut sqlite3_vtab_cursor) -> c_int>,
    pub xColumn: Option<
        unsafe extern "system" fn(
            pCursor: *mut sqlite3_vtab_cursor,
            pCtx: *mut sqlite3_context,
            iCol: c_int,
        ) -> c_int,
    >,
    pub xRowid: Option<
        unsafe extern "system" fn(
            pCursor: *mut sqlite3_vtab_cursor,
            pRowid: *mut sqlite3_int64,
        ) -> c_int,
    >,
    pub xUpdate: Option<
        unsafe extern "system" fn(
            pVTab: *mut sqlite3_vtab,
            argc: c_int,
            argv: *mut *mut sqlite3_value,
            pRowid: *mut sqlite3_int64,
        ) -> c_int,
    >,
    pub xBegin: Option<unsafe extern "system" fn(pVTab: *mut sqlite3_vtab) -> c_int>,
    pub xSync: Option<unsafe extern "system" fn(pVTab: *mut sqlite3_vtab) -> c_int>,
    pub xCommit: Option<unsafe extern "system" fn(pVTab: *mut sqlite3_vtab) -> c_int>,
    pub xRollback: Option<unsafe extern "system" fn(pVTab: *mut sqlite3_vtab) -> c_int>,
    pub xFindFunction: Option<
        unsafe extern "system" fn(
            pVTab: *mut sqlite3_vtab,
            nArg: c_int,
            zName: *const c_char,
            pxFunc: *mut Option<
                unsafe extern "system" fn(*mut sqlite3_context, c_int, *mut *mut sqlite3_value),
            >,
            ppArg: *mut *mut c_void,
        ) -> c_int,
    >,
    pub xRename:
        Option<unsafe extern "system" fn(pVTab: *mut sqlite3_vtab, zNew: *const c_char) -> c_int>,
    pub xSavepoint:
        Option<unsafe extern "system" fn(pVTab: *mut sqlite3_vtab, iSavepoint: c_int) -> c_int>,
    pub xRelease:
        Option<unsafe extern "system" fn(pVTab: *mut sqlite3_vtab, iSavepoint: c_int) -> c_int>,
    pub xRollbackTo:
        Option<unsafe extern "system" fn(pVTab: *mut sqlite3_vtab, iSavepoint: c_int) -> c_int>,
    pub xShadowName: Option<unsafe extern "system" fn(zName: *const c_char) -> c_int>,
    pub xIntegrity: Option<
        unsafe extern "system" fn(
            pVTab: *mut sqlite3_vtab,
            zSchema: *const c_char,
            zTabName: *const c_char,
            mFlags: c_int,
            pzErr: *mut *mut c_char,
        ) -> c_int,
    >,
}

unsafe impl Sync for sqlite3_module {}

// `sqlite3_vtab_config` is variadic, which only the C calling convention can express.
#[cfg_attr(windows, link(name = "winsqlite3"))]
#[cfg_attr(all(unix, not(kani)), link(name = "sqlite3"))]
unsafe extern "C" {
    pub fn sqlite3_vtab_config(db: *mut sqlite3, op: c_int, ...) -> c_int;
}

#[cfg_attr(windows, link(name = "winsqlite3"))]
#[cfg_attr(all(unix, not(kani)), link(name = "sqlite3"))]
unsafe extern "system" {
    pub fn sqlite3_open(filename: *const c_char, ppDb: *mut *mut sqlite3) -> c_int;
    pub fn sqlite3_close(db: *mut sqlite3) -> c_int;
    pub fn sqlite3_errmsg(db: *mut sqlite3) -> *const c_char;
    pub fn sqlite3_exec(
        db: *mut sqlite3,
        sql: *const c_char,
        callback: Option<
            unsafe extern "system" fn(
                *mut c_void,
                c_int,
                *mut *mut c_char,
                *mut *mut c_char,
            ) -> c_int,
        >,
        arg: *mut c_void,
        errmsg: *mut *mut c_char,
    ) -> c_int;
    pub fn sqlite3_free(ptr: *mut c_void);
    pub fn sqlite3_malloc(n: c_int) -> *mut c_void;

    pub fn sqlite3_prepare_v2(
        db: *mut sqlite3,
        zSql: *const c_char,
        nByte: c_int,
        ppStmt: *mut *mut sqlite3_stmt,
        pzTail: *mut *const c_char,
    ) -> c_int;
    pub fn sqlite3_step(pStmt: *mut sqlite3_stmt) -> c_int;
    pub fn sqlite3_get_autocommit(db: *mut sqlite3) -> c_int;
    pub fn sqlite3_txn_state(db: *mut sqlite3, zSchema: *const c_char) -> c_int;
    pub fn sqlite3_trace_v2(
        db: *mut sqlite3,
        mask: core::ffi::c_uint,
        callback: Option<
            unsafe extern "system" fn(
                core::ffi::c_uint,
                *mut c_void,
                *mut c_void,
                *mut c_void,
            ) -> c_int,
        >,
        context: *mut c_void,
    ) -> c_int;
    pub fn sqlite3_commit_hook(
        db: *mut sqlite3,
        callback: Option<unsafe extern "system" fn(*mut c_void) -> c_int>,
        arg: *mut c_void,
    ) -> *mut c_void;
    pub fn sqlite3_rollback_hook(
        db: *mut sqlite3,
        callback: Option<unsafe extern "system" fn(*mut c_void)>,
        arg: *mut c_void,
    ) -> *mut c_void;
    pub fn sqlite3_reset(pStmt: *mut sqlite3_stmt) -> c_int;
    pub fn sqlite3_clear_bindings(pStmt: *mut sqlite3_stmt) -> c_int;
    pub fn sqlite3_finalize(pStmt: *mut sqlite3_stmt) -> c_int;

    pub fn sqlite3_bind_null(pStmt: *mut sqlite3_stmt, idx: c_int) -> c_int;
    pub fn sqlite3_bind_int64(pStmt: *mut sqlite3_stmt, idx: c_int, val: sqlite3_int64) -> c_int;
    pub fn sqlite3_bind_double(pStmt: *mut sqlite3_stmt, idx: c_int, val: c_double) -> c_int;
    pub fn sqlite3_bind_text(
        pStmt: *mut sqlite3_stmt,
        idx: c_int,
        val: *const c_char,
        nByte: c_int,
        xDel: Option<unsafe extern "system" fn(*mut c_void)>,
    ) -> c_int;
    pub fn sqlite3_bind_blob(
        pStmt: *mut sqlite3_stmt,
        idx: c_int,
        val: *const c_void,
        nByte: c_int,
        xDel: Option<unsafe extern "system" fn(*mut c_void)>,
    ) -> c_int;

    pub fn sqlite3_column_count(pStmt: *mut sqlite3_stmt) -> c_int;
    pub fn sqlite3_column_type(pStmt: *mut sqlite3_stmt, iCol: c_int) -> c_int;
    pub fn sqlite3_column_int64(pStmt: *mut sqlite3_stmt, iCol: c_int) -> sqlite3_int64;
    pub fn sqlite3_column_double(pStmt: *mut sqlite3_stmt, iCol: c_int) -> c_double;
    pub fn sqlite3_column_text(pStmt: *mut sqlite3_stmt, iCol: c_int) -> *const c_uchar;
    pub fn sqlite3_column_blob(pStmt: *mut sqlite3_stmt, iCol: c_int) -> *const c_void;
    pub fn sqlite3_column_bytes(pStmt: *mut sqlite3_stmt, iCol: c_int) -> c_int;
    pub fn sqlite3_column_value(pStmt: *mut sqlite3_stmt, iCol: c_int) -> *mut sqlite3_value;

    pub fn sqlite3_create_module_v2(
        db: *mut sqlite3,
        zName: *const c_char,
        p: *const sqlite3_module,
        pClientData: *mut c_void,
        xDestroy: Option<unsafe extern "system" fn(*mut c_void)>,
    ) -> c_int;
    pub fn sqlite3_declare_vtab(db: *mut sqlite3, zSQL: *const c_char) -> c_int;
    pub fn sqlite3_vtab_on_conflict(db: *mut sqlite3) -> c_int;
    pub fn sqlite3_vtab_collation(
        pIdxInfo: *mut sqlite3_index_info,
        iConstraint: c_int,
    ) -> *const c_char;

    pub fn sqlite3_value_type(pVal: *mut sqlite3_value) -> c_int;
    pub fn sqlite3_value_int64(pVal: *mut sqlite3_value) -> sqlite3_int64;
    pub fn sqlite3_value_double(pVal: *mut sqlite3_value) -> c_double;
    pub fn sqlite3_value_text(pVal: *mut sqlite3_value) -> *const c_uchar;
    pub fn sqlite3_value_blob(pVal: *mut sqlite3_value) -> *const c_void;
    pub fn sqlite3_value_bytes(pVal: *mut sqlite3_value) -> c_int;

    pub fn sqlite3_result_null(pCtx: *mut sqlite3_context);
    pub fn sqlite3_result_int64(pCtx: *mut sqlite3_context, val: sqlite3_int64);
    pub fn sqlite3_result_double(pCtx: *mut sqlite3_context, val: c_double);
    pub fn sqlite3_result_text(
        pCtx: *mut sqlite3_context,
        val: *const c_char,
        nByte: c_int,
        xDel: Option<unsafe extern "system" fn(*mut c_void)>,
    );
    pub fn sqlite3_result_blob(
        pCtx: *mut sqlite3_context,
        val: *const c_void,
        nByte: c_int,
        xDel: Option<unsafe extern "system" fn(*mut c_void)>,
    );
    pub fn sqlite3_result_error_toobig(pCtx: *mut sqlite3_context);
    pub fn sqlite3_result_error_code(pCtx: *mut sqlite3_context, errCode: c_int);
}

/// Reads a `sqlite3_value` UTF-8 text slice, calling `sqlite3_value_text` before
/// `sqlite3_value_bytes` as required by SQLite's pointer/length C API contract.
///
/// # Safety
/// `val` must be a valid non-null `*mut sqlite3_value` whose buffer stays valid for `'a`.
#[inline]
pub(in crate::vtab) unsafe fn sqlite_value_text_bytes<'a>(val: *mut sqlite3_value) -> &'a [u8] {
    unsafe {
        let ptr = sqlite3_value_text(val);
        if ptr.is_null() {
            return &[];
        }
        let len = sqlite3_value_bytes(val).max(0) as usize;
        if len == 0 {
            &[]
        } else {
            core::slice::from_raw_parts(ptr, len)
        }
    }
}

/// Reads a `sqlite3_value` blob slice, calling `sqlite3_value_blob` before `sqlite3_value_bytes`
/// as required by SQLite's pointer/length C API contract.
///
/// # Safety
/// `val` must be a valid non-null `*mut sqlite3_value` whose buffer stays valid for `'a`.
#[inline]
pub(in crate::vtab) unsafe fn sqlite_value_blob_bytes<'a>(val: *mut sqlite3_value) -> &'a [u8] {
    unsafe {
        let ptr = sqlite3_value_blob(val) as *const u8;
        if ptr.is_null() {
            return &[];
        }
        let len = sqlite3_value_bytes(val).max(0) as usize;
        if len == 0 {
            &[]
        } else {
            core::slice::from_raw_parts(ptr, len)
        }
    }
}

/// Reads a `sqlite3_stmt` column UTF-8 text slice, calling `sqlite3_column_text` before
/// `sqlite3_column_bytes` as required by SQLite's pointer/length C API contract.
///
/// # Safety
/// `stmt` must be positioned on a valid row and the returned slice stays valid only until the
/// next SQLite step/reset/type conversion on the same column.
#[inline]
pub(in crate::vtab) unsafe fn sqlite_column_text_bytes<'a>(
    stmt: *mut sqlite3_stmt,
    col: c_int,
) -> &'a [u8] {
    unsafe {
        let ptr = sqlite3_column_text(stmt, col);
        if ptr.is_null() {
            return &[];
        }
        let len = sqlite3_column_bytes(stmt, col).max(0) as usize;
        if len == 0 {
            &[]
        } else {
            core::slice::from_raw_parts(ptr, len)
        }
    }
}

/// Reads a `sqlite3_stmt` column blob slice, calling `sqlite3_column_blob` before
/// `sqlite3_column_bytes` as required by SQLite's pointer/length C API contract.
///
/// # Safety
/// `stmt` must be positioned on a valid row and the returned slice stays valid only until the
/// next SQLite step/reset/type conversion on the same column.
#[inline]
pub(in crate::vtab) unsafe fn sqlite_column_blob_bytes<'a>(
    stmt: *mut sqlite3_stmt,
    col: c_int,
) -> &'a [u8] {
    unsafe {
        let ptr = sqlite3_column_blob(stmt, col) as *const u8;
        if ptr.is_null() {
            return &[];
        }
        let len = sqlite3_column_bytes(stmt, col).max(0) as usize;
        if len == 0 {
            &[]
        } else {
            core::slice::from_raw_parts(ptr, len)
        }
    }
}

/// Safe lifetime-bound borrow of a `*mut sqlite3_value` passed by SQLite to FFI callbacks.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub(in crate::vtab) struct SqliteValueRef<'a>(
    *mut sqlite3_value,
    core::marker::PhantomData<&'a ()>,
);

impl<'a> SqliteValueRef<'a> {
    /// Wraps a raw non-null SQLite value pointer valid for `'a`.
    ///
    /// # Safety
    /// `ptr` must be valid for the lifetime `'a`.
    #[inline]
    pub(in crate::vtab) unsafe fn from_raw(ptr: *mut sqlite3_value) -> Self {
        Self(ptr, core::marker::PhantomData)
    }

    /// Reinterprets an FFI `argv` array of `argc` value pointers as a slice of [`SqliteValueRef`].
    ///
    /// # Safety
    /// When `argc > 0` and `!argv.is_null()`, `argv` must point to `argc` valid `sqlite3_value`
    /// pointers valid for `'a`.
    #[inline]
    pub(in crate::vtab) unsafe fn slice_from_argv(
        argv: *mut *mut sqlite3_value,
        argc: c_int,
    ) -> &'a [Self] {
        if argc <= 0 || argv.is_null() {
            return &[];
        }
        unsafe { core::slice::from_raw_parts(argv as *const Self, argc as usize) }
    }

    #[inline]
    pub(in crate::vtab) fn as_ptr(self) -> *mut sqlite3_value {
        self.0
    }

    #[inline]
    pub(in crate::vtab) fn value_type(self) -> c_int {
        unsafe { sqlite3_value_type(self.0) }
    }

    #[inline]
    pub(in crate::vtab) fn is_null(self) -> bool {
        self.0.is_null() || self.value_type() == SQLITE_NULL
    }

    #[inline]
    pub(in crate::vtab) fn is_null_or_nan(self) -> bool {
        if self.0.is_null() {
            return true;
        }
        let ty = self.value_type();
        ty == SQLITE_NULL || (ty == SQLITE_FLOAT && self.as_double().is_nan())
    }

    #[inline]
    pub(in crate::vtab) fn as_int64(self) -> i64 {
        unsafe { sqlite3_value_int64(self.0) }
    }

    #[inline]
    pub(in crate::vtab) fn as_double(self) -> f64 {
        unsafe { sqlite3_value_double(self.0) }
    }

    #[inline]
    pub(in crate::vtab) fn as_text_bytes(self) -> &'a [u8] {
        unsafe { sqlite_value_text_bytes(self.0) }
    }

    #[inline]
    pub(in crate::vtab) fn as_blob_bytes(self) -> &'a [u8] {
        unsafe { sqlite_value_blob_bytes(self.0) }
    }
}

/// Safe wrapper around a `*mut sqlite3_context` passed to `xColumn` / `xRowid`.
#[repr(transparent)]
pub(in crate::vtab) struct SqliteContextRef<'a>(
    *mut sqlite3_context,
    core::marker::PhantomData<&'a mut ()>,
);

impl<'a> SqliteContextRef<'a> {
    /// Wraps a raw SQLite result context pointer valid for `'a`.
    ///
    /// # Safety
    /// `ptr` must be a valid non-null `*mut sqlite3_context` for `'a`.
    #[inline]
    pub(in crate::vtab) unsafe fn from_raw(ptr: *mut sqlite3_context) -> Self {
        Self(ptr, core::marker::PhantomData)
    }

    #[inline]
    pub(in crate::vtab) fn as_ptr(&self) -> *mut sqlite3_context {
        self.0
    }

    #[inline]
    pub(in crate::vtab) fn set_null(&self) {
        unsafe { sqlite3_result_null(self.0) };
    }

    #[inline]
    pub(in crate::vtab) fn set_i64(&self, val: i64) {
        unsafe { sqlite3_result_int64(self.0, val) };
    }

    #[inline]
    pub(in crate::vtab) fn set_f64(&self, val: f64) {
        unsafe { sqlite3_result_double(self.0, val) };
    }

    #[inline]
    pub(in crate::vtab) fn set_error_toobig(&self) {
        unsafe { sqlite3_result_error_toobig(self.0) };
    }

    /// Passes a raw slice to `sqlite3_result_text` or `sqlite3_result_blob`, reporting `TOOBIG`
    /// and returning `false` when `payload.len() > i32::MAX as usize`.
    #[inline]
    pub(in crate::vtab) fn set_result_bytes(&self, payload: &[u8], is_text: bool) -> bool {
        let Ok(n) = c_int::try_from(payload.len()) else {
            self.set_error_toobig();
            return false;
        };
        unsafe {
            if is_text {
                sqlite3_result_text(self.0, payload.as_ptr() as *const _, n, sqlite_transient());
            } else {
                sqlite3_result_blob(self.0, payload.as_ptr() as *const _, n, sqlite_transient());
            }
        }
        true
    }

    #[inline]
    pub(in crate::vtab) fn set_error_code(&self, rc: c_int) {
        unsafe { sqlite3_result_error_code(self.0, rc) };
    }

    /// Maps an `io::Result<()>` to a SQLite return code, setting the error code on the context
    /// unless `rc == SQLITE_TOOBIG` (where SQLite relies on the `sqlite3_result_error_toobig`
    /// already reported or the mapped `SQLITE_TOOBIG` rc).
    #[inline]
    pub(in crate::vtab) fn finish_io_result(
        &self,
        res: std::io::Result<()>,
        map_err: fn(&std::io::Error) -> c_int,
    ) -> c_int {
        match res {
            Ok(()) => SQLITE_OK,
            Err(e) => {
                let rc = map_err(&e);
                if rc != SQLITE_TOOBIG {
                    self.set_error_code(rc);
                }
                rc
            }
        }
    }
}

/// RAII guard that calls `sqlite3_finalize` when dropped.
pub(in crate::vtab) struct SqliteStmtGuard(*mut sqlite3_stmt);

impl SqliteStmtGuard {
    #[inline]
    pub(in crate::vtab) fn new(stmt: *mut sqlite3_stmt) -> Self {
        Self(stmt)
    }

    /// Compiles `sql` on `db`, wrapping the resulting prepared statement in [`SqliteStmtGuard`].
    ///
    /// # Safety
    /// `db` must be a valid non-null `*mut sqlite3`.
    pub(in crate::vtab) unsafe fn prepare(db: *mut sqlite3, sql: &str) -> Result<Self, c_int> {
        let Ok(sql_len) = c_int::try_from(sql.len()) else {
            return Err(SQLITE_ERROR);
        };
        let mut raw_stmt: *mut sqlite3_stmt = core::ptr::null_mut();
        let rc = unsafe {
            sqlite3_prepare_v2(
                db,
                sql.as_ptr() as *const c_char,
                sql_len,
                &mut raw_stmt,
                core::ptr::null_mut(),
            )
        };
        let guard = Self::new(raw_stmt);
        if rc != SQLITE_OK || guard.0.is_null() {
            return Err(rc);
        }
        Ok(guard)
    }

    #[inline]
    pub(in crate::vtab) fn as_ptr(&self) -> *mut sqlite3_stmt {
        self.0
    }

    #[inline]
    pub(in crate::vtab) fn step(&mut self) -> c_int {
        unsafe { sqlite3_step(self.0) }
    }

    #[inline]
    pub(in crate::vtab) fn reset(&mut self) -> c_int {
        unsafe { sqlite3_reset(self.0) }
    }

    #[inline]
    pub(in crate::vtab) fn clear_bindings(&mut self) {
        unsafe {
            sqlite3_clear_bindings(self.0);
        }
    }

    #[inline]
    pub(in crate::vtab) fn bind_null(&mut self, col: c_int) -> c_int {
        unsafe { sqlite3_bind_null(self.0, col) }
    }

    #[inline]
    pub(in crate::vtab) fn bind_i64(&mut self, col: c_int, val: i64) -> c_int {
        unsafe { sqlite3_bind_int64(self.0, col, val) }
    }

    #[inline]
    pub(in crate::vtab) fn bind_f64(&mut self, col: c_int, val: f64) -> c_int {
        unsafe { sqlite3_bind_double(self.0, col, val) }
    }

    #[inline]
    pub(in crate::vtab) fn bind_text(&mut self, col: c_int, text: &str) -> c_int {
        let Ok(n) = c_int::try_from(text.len()) else {
            return SQLITE_TOOBIG;
        };
        unsafe {
            sqlite3_bind_text(
                self.0,
                col,
                text.as_ptr() as *const c_char,
                n,
                sqlite_transient(),
            )
        }
    }

    #[inline]
    pub(in crate::vtab) fn bind_blob(&mut self, col: c_int, bytes: &[u8]) -> c_int {
        let Ok(n) = c_int::try_from(bytes.len()) else {
            return SQLITE_TOOBIG;
        };
        unsafe {
            sqlite3_bind_blob(
                self.0,
                col,
                bytes.as_ptr() as *const c_void,
                n,
                sqlite_transient(),
            )
        }
    }

    #[inline]
    pub(in crate::vtab) fn column_count(&self) -> usize {
        unsafe { sqlite3_column_count(self.0).max(0) as usize }
    }

    #[inline]
    pub(in crate::vtab) fn column_type(&self, col: c_int) -> c_int {
        unsafe { sqlite3_column_type(self.0, col) }
    }

    #[inline]
    pub(in crate::vtab) fn column_i64(&self, col: c_int) -> i64 {
        unsafe { sqlite3_column_int64(self.0, col) }
    }

    #[inline]
    pub(in crate::vtab) fn column_f64(&self, col: c_int) -> f64 {
        unsafe { sqlite3_column_double(self.0, col) }
    }

    #[inline]
    pub(in crate::vtab) fn column_text_bytes(&self, col: c_int) -> &[u8] {
        unsafe { sqlite_column_text_bytes(self.0, col) }
    }

    #[inline]
    pub(in crate::vtab) fn column_blob_bytes(&self, col: c_int) -> &[u8] {
        unsafe { sqlite_column_blob_bytes(self.0, col) }
    }
}

impl Drop for SqliteStmtGuard {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe {
                sqlite3_finalize(self.0);
            }
        }
    }
}

/// Copies `msg` into memory owned by SQLite (`sqlite3_free` releases it), as `*pzErr` and
/// `zErrMsg` require. Returns null if the allocation fails; the error code still reports.
pub(in crate::vtab) unsafe fn sqlite_error_message(msg: &str) -> *mut c_char {
    let Some(alloc_len) = msg
        .len()
        .checked_add(1)
        .and_then(|n| c_int::try_from(n).ok())
    else {
        return core::ptr::null_mut();
    };
    unsafe {
        let buf = sqlite3_malloc(alloc_len) as *mut c_char;
        if !buf.is_null() {
            core::ptr::copy_nonoverlapping(msg.as_ptr() as *const c_char, buf, msg.len());
            *buf.add(msg.len()) = 0;
        }
        buf
    }
}

/// Writes a SQLite-owned copy of `msg` into `*pz_err` when `pz_err` is non-null.
///
/// # Safety
/// `pz_err` must be null or point to a valid writable `*mut c_char` slot (usually uninitialized or
/// null on entry from SQLite `xCreate`/`xConnect`).
pub(in crate::vtab) unsafe fn set_pz_err(pz_err: *mut *mut c_char, msg: &str) {
    if !pz_err.is_null() {
        unsafe {
            *pz_err = sqlite_error_message(msg);
        }
    }
}

/// Replaces `(*p_vtab).zErrMsg` with a SQLite-owned copy of `msg`.
///
/// # Safety
/// `p_vtab` must be null or point to a valid `sqlite3_vtab` (such as `vtab.base.get()`).
pub(in crate::vtab) unsafe fn set_vtab_err_msg(p_vtab: *mut sqlite3_vtab, msg: &str) {
    unsafe {
        if !p_vtab.is_null() {
            if !(*p_vtab).zErrMsg.is_null() {
                sqlite3_free((*p_vtab).zErrMsg as *mut c_void);
            }
            (*p_vtab).zErrMsg = sqlite_error_message(msg);
        }
    }
}
