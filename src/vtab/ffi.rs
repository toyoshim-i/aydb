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
                unsafe extern "system" fn(
                    *mut sqlite3_context,
                    c_int,
                    *mut *mut sqlite3_value,
                ),
            >,
            ppArg: *mut *mut c_void,
        ) -> c_int,
    >,
    pub xRename: Option<
        unsafe extern "system" fn(pVTab: *mut sqlite3_vtab, zNew: *const c_char) -> c_int,
    >,
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

#[cfg_attr(windows, link(name = "winsqlite3"))]
#[cfg_attr(unix, link(name = "sqlite3"))]
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
    pub fn sqlite3_reset(pStmt: *mut sqlite3_stmt) -> c_int;
    pub fn sqlite3_clear_bindings(pStmt: *mut sqlite3_stmt) -> c_int;
    pub fn sqlite3_finalize(pStmt: *mut sqlite3_stmt) -> c_int;

    pub fn sqlite3_bind_null(pStmt: *mut sqlite3_stmt, idx: c_int) -> c_int;
    pub fn sqlite3_bind_int64(
        pStmt: *mut sqlite3_stmt,
        idx: c_int,
        val: sqlite3_int64,
    ) -> c_int;
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

    pub fn sqlite3_create_module_v2(
        db: *mut sqlite3,
        zName: *const c_char,
        p: *const sqlite3_module,
        pClientData: *mut c_void,
        xDestroy: Option<unsafe extern "system" fn(*mut c_void)>,
    ) -> c_int;
    pub fn sqlite3_declare_vtab(db: *mut sqlite3, zSQL: *const c_char) -> c_int;
    pub fn sqlite3_vtab_on_conflict(db: *mut sqlite3) -> c_int;

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
}
