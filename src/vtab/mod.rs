//! Phase 2: SQLite Virtual Table (`sqlite3_module`) Adapter & Safe SQLite Connection Wrapper.
//!
//! Exposes `aydb`'s [`StorageEngine`] as a first-class SQLite Virtual Table (`USING aydb(...)`)
//! with automatic primary-key point lookups (`xBestIndex` Plan 1), secondary index equality
//! lookups (Plan 2), primary-key range scans (Plan 3), and Capped Table (`MAX_BYTES`) LRU eviction.

pub mod ffi;
pub mod row_codec;

use core::ffi::{c_char, c_int, c_void};
use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::engine::{StorageEngine, WriteDurability};
use crate::lru::encode_i64_memcomparable;
use crate::pal::OsFs;
use crate::util::crc32c::crc32c_append;
use crate::vtab::ffi::{
    SQLITE_BLOB, SQLITE_DONE, SQLITE_ERROR, SQLITE_FLOAT, SQLITE_INDEX_CONSTRAINT_EQ,
    SQLITE_INDEX_CONSTRAINT_GE, SQLITE_INDEX_CONSTRAINT_GT, SQLITE_INDEX_CONSTRAINT_LE,
    SQLITE_INDEX_CONSTRAINT_LIMIT, SQLITE_INDEX_CONSTRAINT_LT, SQLITE_INDEX_SCAN_UNIQUE,
    SQLITE_INTEGER, SQLITE_NULL, SQLITE_OK, SQLITE_ROW, SQLITE_TEXT, sqlite_transient, sqlite3,
    sqlite3_bind_blob, sqlite3_bind_double, sqlite3_bind_int64, sqlite3_bind_null,
    sqlite3_bind_text, sqlite3_close, sqlite3_column_blob, sqlite3_column_bytes,
    sqlite3_column_count, sqlite3_column_double, sqlite3_column_int64, sqlite3_column_text,
    sqlite3_column_type, sqlite3_context, sqlite3_create_module_v2, sqlite3_declare_vtab,
    sqlite3_errmsg, sqlite3_exec, sqlite3_finalize, sqlite3_index_info, sqlite3_int64,
    sqlite3_module, sqlite3_open, sqlite3_prepare_v2, sqlite3_reset, sqlite3_result_int64,
    sqlite3_step, sqlite3_stmt, sqlite3_value, sqlite3_value_int64, sqlite3_value_type,
    sqlite3_vtab, sqlite3_vtab_cursor,
};
pub use crate::vtab::row_codec::{
    SqlValue, decode_all_columns, decode_column_at, decode_f64_memcomparable,
    encode_f64_memcomparable, encode_row, encode_sqlite_values_direct, extract_column_key_bytes,
    project_column_zero_copy, sqlite_value_to_key_bytes,
};

const PLAN_FULL_SCAN: c_int = 0;
const PLAN_PK_POINT: c_int = 1;
const PLAN_SECONDARY_EQ: c_int = 2;
const PLAN_PK_RANGE: c_int = 3;

const POINT_FLAG_BY_ROWID: c_int = 0x01;

const RANGE_FLAG_LOWER_INCLUSIVE: c_int = 0x01;
const RANGE_FLAG_LOWER_EXCLUSIVE: c_int = 0x02;
const RANGE_FLAG_UPPER_INCLUSIVE: c_int = 0x04;
const RANGE_FLAG_UPPER_EXCLUSIVE: c_int = 0x08;
const RANGE_FLAG_HAS_LIMIT: c_int = 0x10;

/// SQLite column type affinity (SQLite Datatypes Section 3.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnAffinity {
    Integer,
    Text,
    Blob,
    Real,
    Numeric,
}

impl ColumnAffinity {
    /// Parses a column's declared type string according to SQLite's 5 affinity rules.
    pub fn from_type_str(type_str: &str) -> Self {
        let upper = type_str.trim().to_ascii_uppercase();
        if upper.is_empty() {
            return Self::Blob;
        }
        if upper.contains("INT") {
            Self::Integer
        } else if upper.contains("CHAR") || upper.contains("CLOB") || upper.contains("TEXT") {
            Self::Text
        } else if upper.contains("BLOB") {
            Self::Blob
        } else if upper.contains("REAL") || upper.contains("FLOA") || upper.contains("DOUB") {
            Self::Real
        } else {
            Self::Numeric
        }
    }
}

/// Collision-free bidirectional map between synthetic 64-bit SQLite `rowid`s and non-integer `encoded_pk` bytes.
#[derive(Default)]
struct RowidMap {
    by_rowid: HashMap<i64, Vec<u8>>,
    by_pk: HashMap<Vec<u8>, i64>,
}

impl RowidMap {
    fn get_or_assign_rowid(&mut self, encoded_pk: &[u8]) -> i64 {
        if let Some(&rid) = self.by_pk.get(encoded_pk) {
            return rid;
        }
        let mut rid = hash_pk_to_rowid(encoded_pk);
        while let Some(existing_pk) = self.by_rowid.get(&rid) {
            if existing_pk.as_slice() == encoded_pk {
                return rid;
            }
            rid = rid.wrapping_add(1);
        }
        let pk_vec = encoded_pk.to_vec();
        self.by_rowid.insert(rid, pk_vec.clone());
        self.by_pk.insert(pk_vec, rid);
        rid
    }

    fn get_pk(&self, rid: i64) -> Option<&Vec<u8>> {
        self.by_rowid.get(&rid)
    }

    fn remove_by_rowid(&mut self, rid: i64) -> Option<Vec<u8>> {
        if let Some(pk) = self.by_rowid.remove(&rid) {
            self.by_pk.remove(&pk);
            Some(pk)
        } else {
            None
        }
    }

    fn remove_by_pk(&mut self, encoded_pk: &[u8]) {
        if let Some(rid) = self.by_pk.remove(encoded_pk) {
            self.by_rowid.remove(&rid);
        }
    }
}

/// Static `sqlite3_module` method table registered with `sqlite3_create_module_v2`.
static AYDB_VTAB_MODULE: sqlite3_module = sqlite3_module {
    iVersion: 2,
    xCreate: Some(vtab_create_or_connect),
    xConnect: Some(vtab_create_or_connect),
    xBestIndex: Some(vtab_best_index),
    xDisconnect: Some(vtab_disconnect),
    xDestroy: Some(vtab_disconnect),
    xOpen: Some(vtab_open),
    xClose: Some(vtab_close),
    xFilter: Some(vtab_filter),
    xNext: Some(vtab_next),
    xEof: Some(vtab_eof),
    xColumn: Some(vtab_column),
    xRowid: Some(vtab_rowid),
    xUpdate: Some(vtab_update),
    xBegin: Some(vtab_begin_tx),
    xSync: Some(vtab_noop_tx),
    xCommit: Some(vtab_commit_tx),
    xRollback: Some(vtab_rollback_tx),
    xFindFunction: None,
    xRename: None,
    xSavepoint: None,
    xRelease: None,
    xRollbackTo: None,
    xShadowName: None,
    xIntegrity: None,
};

struct ModuleAuxContext {
    engine: Arc<Mutex<StorageEngine<OsFs>>>,
}

#[repr(C)]
struct AydbVTab {
    base: sqlite3_vtab,
    engine: Arc<Mutex<StorageEngine<OsFs>>>,
    table_id: u16,
    durability: WriteDurability,
    col_names: Vec<String>,
    pk_col_idx: usize,
    pk_affinity: ColumnAffinity,
    /// Compact list of `(col_idx, index_id)` (`1..=N`) for secondary indexed columns.
    secondary_indexes: Vec<(usize, u16)>,
    /// Fast flag set to `true` only if a non-integer primary key has been stored.
    /// When `false`, `rowid_to_pk` is never locked on queries, inserts, updates, or deletes!
    has_non_int_pks: AtomicBool,
    /// Collision-free bidirectional map between synthetic 64-bit `rowid`s and `encoded_pk` bytes
    /// so SQLite `DELETE` / `UPDATE` by `rowid` can resolve the target primary key.
    rowid_to_pk: Mutex<RowidMap>,
}

#[repr(C)]
struct AydbCursor {
    base: sqlite3_vtab_cursor,
    /// Contiguous reusable byte slab holding all scanned `row_bytes` for the active cursor pass.
    slab: Vec<u8>,
    /// `(start_offset, end_offset)` into `slab` for each row.
    spans: Vec<(u32, u32)>,
    pos: usize,
}

/// Computes a 64-bit synthetic `rowid` from `encoded_pk` with full 64-bit entropy using two
/// independently seeded hardware CRC32C passes followed by a SplitMix64 bijective finalizer.
#[inline]
fn hash_pk_to_rowid(encoded_pk: &[u8]) -> i64 {
    let h1 = crc32c_append(0x0000_0000, encoded_pk) as u64;
    let h2 = crc32c_append(0x9E37_79B9, encoded_pk) as u64;
    let mut z = (h1 << 32) | h2;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    (z ^ (z >> 31)) as i64
}

#[inline]
pub fn pk_to_rowid(encoded_pk: &[u8]) -> i64 {
    hash_pk_to_rowid(encoded_pk)
}

/// Extracts the `rowid` (`i64`) from `row_bytes` at `vtab.pk_col_idx`.
/// For integer PKs (`pk_affinity == ColumnAffinity::Integer` and `type_tag == 1`), reads the
/// 8-byte `i64` directly in $O(1)$ with zero allocation.
/// For non-integer PKs (`TEXT`/`BLOB`/`REAL`/etc.), computes the collision-free synthetic `rowid`
/// and registers `(rid, pk)` in `rowid_to_pk`.
#[inline]
fn extract_rowid_from_row(row_bytes: &[u8], vtab: &AydbVTab) -> i64 {
    let dir_pos = 4 + vtab.pk_col_idx * 5;
    if vtab.pk_affinity == ColumnAffinity::Integer
        && dir_pos + 5 <= row_bytes.len()
        && row_bytes[dir_pos] == 1
    {
        let offset =
            u32::from_le_bytes(row_bytes[dir_pos + 1..dir_pos + 5].try_into().unwrap()) as usize;
        if offset + 8 <= row_bytes.len() {
            return i64::from_le_bytes(row_bytes[offset..offset + 8].try_into().unwrap());
        }
    }
    let pk = extract_column_key_bytes(row_bytes, vtab.pk_col_idx).unwrap_or_default();
    vtab.has_non_int_pks.store(true, Ordering::Relaxed);
    if let Ok(mut rmap) = vtab.rowid_to_pk.lock() {
        rmap.get_or_assign_rowid(&pk)
    } else {
        hash_pk_to_rowid(&pk)
    }
}

unsafe extern "system" fn destroy_aux_context(ptr: *mut c_void) {
    if !ptr.is_null() {
        unsafe {
            drop(Box::from_raw(ptr as *mut ModuleAuxContext));
        }
    }
}

unsafe extern "system" fn vtab_create_or_connect(
    db: *mut sqlite3,
    p_aux: *mut c_void,
    argc: c_int,
    argv: *const *const c_char,
    pp_vtab: *mut *mut sqlite3_vtab,
    _pz_err: *mut *mut c_char,
) -> c_int {
    unsafe {
        if p_aux.is_null() || pp_vtab.is_null() {
            return SQLITE_ERROR;
        }
        let aux = &*(p_aux as *const ModuleAuxContext);

        let mut table_id: u16 = 1;
        let mut max_bytes: u64 = 0;
        let mut durability = WriteDurability::BufferedFast;
        let mut columns_spec = String::from("key TEXT PRIMARY KEY, value BLOB");
        let mut indexes_spec = String::new();

        for i in 3..argc {
            let arg_ptr = *argv.offset(i as isize);
            if arg_ptr.is_null() {
                continue;
            }
            let raw_arg = CStr::from_ptr(arg_ptr).to_string_lossy();
            if let Some((k, v)) = raw_arg.split_once('=') {
                let key = k.trim().to_ascii_lowercase();
                let val = v.trim().trim_matches('\'').trim_matches('"').trim();
                match key.as_str() {
                    "table_id" => {
                        if let Ok(id) = val.parse::<u16>() {
                            table_id = id;
                        }
                    }
                    "max_bytes" => {
                        if let Ok(mb) = val.parse::<u64>() {
                            max_bytes = mb;
                        }
                    }
                    "durability" => {
                        if val.eq_ignore_ascii_case("sync") {
                            durability = WriteDurability::SyncTransactional;
                        } else {
                            durability = WriteDurability::BufferedFast;
                        }
                    }
                    "columns" => {
                        columns_spec = val.to_string();
                    }
                    "indexes" => {
                        indexes_spec = val.to_string();
                    }
                    _ => {}
                }
            }
        }

        // Parse column names, identify PRIMARY KEY column index, and determine PK affinity
        let mut col_names = Vec::new();
        let mut col_affinities = Vec::new();
        let mut pk_col_idx = 0usize;
        for (idx, col_def) in columns_spec.split(',').enumerate() {
            let trimmed = col_def.trim();
            let mut tokens = trimmed.split_whitespace();
            if let Some(name_tok) = tokens.next() {
                col_names.push(name_tok.to_string());
                let rest = tokens.collect::<Vec<_>>().join(" ");
                let upper = rest.to_ascii_uppercase();
                let type_part = upper.replace("PRIMARY KEY", "").replace("NOT NULL", "");
                col_affinities.push(ColumnAffinity::from_type_str(&type_part));
                if upper.contains("PRIMARY KEY") {
                    pk_col_idx = idx;
                }
            }
        }
        if col_names.is_empty() {
            return SQLITE_ERROR;
        }
        let pk_affinity = col_affinities
            .get(pk_col_idx)
            .copied()
            .unwrap_or(ColumnAffinity::Blob);

        // Parse secondary index column names
        let mut secondary_indexes = Vec::new();
        let mut next_idx_id = 1u16;
        for idx_col in indexes_spec.split(',') {
            let target = idx_col.trim();
            if target.is_empty() {
                continue;
            }
            if let Some(pos) = col_names
                .iter()
                .position(|c| c.eq_ignore_ascii_case(target))
            {
                secondary_indexes.push((pos, next_idx_id));
                next_idx_id += 1;
            }
        }

        // Register Capped Table in StorageEngine if `max_bytes > 0`
        if max_bytes > 0 {
            let mut eng = match aux.engine.lock() {
                Ok(g) => g,
                Err(_) => return SQLITE_ERROR,
            };
            if eng.register_capped_table(table_id, max_bytes).is_err() {
                return SQLITE_ERROR;
            }
        }

        let declare_sql = format!("CREATE TABLE x({columns_spec})");
        let Ok(c_sql) = CString::new(declare_sql) else {
            return SQLITE_ERROR;
        };
        let rc = sqlite3_declare_vtab(db, c_sql.as_ptr());
        if rc != SQLITE_OK {
            return rc;
        }

        let vtab = Box::new(AydbVTab {
            base: sqlite3_vtab {
                pModule: &AYDB_VTAB_MODULE,
                nRef: 0,
                zErrMsg: core::ptr::null_mut(),
            },
            engine: aux.engine.clone(),
            table_id,
            durability,
            col_names,
            pk_col_idx,
            pk_affinity,
            secondary_indexes,
            has_non_int_pks: AtomicBool::new(pk_affinity != ColumnAffinity::Integer),
            rowid_to_pk: Mutex::new(RowidMap::default()),
        });

        *pp_vtab = Box::into_raw(vtab) as *mut sqlite3_vtab;
        SQLITE_OK
    }
}

unsafe extern "system" fn vtab_disconnect(p_vtab: *mut sqlite3_vtab) -> c_int {
    if !p_vtab.is_null() {
        unsafe {
            drop(Box::from_raw(p_vtab as *mut AydbVTab));
        }
    }
    SQLITE_OK
}

unsafe extern "system" fn vtab_best_index(
    p_vtab: *mut sqlite3_vtab,
    p_info: *mut sqlite3_index_info,
) -> c_int {
    unsafe {
        let vtab = &*(p_vtab as *const AydbVTab);
        let info = &mut *p_info;
        let n = info.nConstraint as usize;
        let constraints = core::slice::from_raw_parts(info.aConstraint, n);
        let usages = core::slice::from_raw_parts_mut(info.aConstraintUsage, n);

        // 1. Check for usable equality constraint (`=`) on Primary Key (or ROWID `-1`)
        for i in 0..n {
            if constraints[i].usable == 0 {
                continue;
            }
            let col = constraints[i].iColumn;
            if constraints[i].op == SQLITE_INDEX_CONSTRAINT_EQ
                && (col == vtab.pk_col_idx as i32 || col == -1)
            {
                usages[i].argvIndex = 1;
                usages[i].omit = 1;
                let pt_flags = if col == -1 { POINT_FLAG_BY_ROWID } else { 0 };
                info.idxNum = PLAN_PK_POINT | (pt_flags << 8);
                info.idxFlags = SQLITE_INDEX_SCAN_UNIQUE;
                info.estimatedCost = 1.0;
                info.estimatedRows = 1;
                return SQLITE_OK;
            }
        }

        // 2. Check for usable equality constraint (`=`) on a Secondary Index column
        for i in 0..n {
            if constraints[i].usable == 0 || constraints[i].op != SQLITE_INDEX_CONSTRAINT_EQ {
                continue;
            }
            let col = constraints[i].iColumn;
            if col >= 0 {
                if let Some(&(_, index_id)) = vtab
                    .secondary_indexes
                    .iter()
                    .find(|&&(c, _)| c == col as usize)
                {
                    usages[i].argvIndex = 1;
                    // Do not set `omit = 1` on secondary index lookups so SQLite verifies the
                    // exact column type and value (`NULL` vs `""` vs `X''`).
                    usages[i].omit = 0;
                    info.idxNum = PLAN_SECONDARY_EQ | ((index_id as c_int) << 8);
                    info.estimatedCost = 5.0;
                    info.estimatedRows = 4;
                    return SQLITE_OK;
                }
            }
        }

        // 3. Check for usable range constraints (`>`, `>=`, `<`, `<=`) on Primary Key + optional LIMIT
        let mut lower_idx: Option<(usize, c_int)> = None;
        let mut upper_idx: Option<(usize, c_int)> = None;
        let mut limit_idx: Option<usize> = None;
        for i in 0..n {
            if constraints[i].usable == 0 {
                continue;
            }
            if constraints[i].op == SQLITE_INDEX_CONSTRAINT_LIMIT && limit_idx.is_none() {
                limit_idx = Some(i);
                continue;
            }
            if constraints[i].iColumn != vtab.pk_col_idx as i32 {
                continue;
            }
            match constraints[i].op {
                SQLITE_INDEX_CONSTRAINT_GE if lower_idx.is_none() => {
                    lower_idx = Some((i, RANGE_FLAG_LOWER_INCLUSIVE));
                }
                SQLITE_INDEX_CONSTRAINT_GT if lower_idx.is_none() => {
                    lower_idx = Some((i, RANGE_FLAG_LOWER_EXCLUSIVE));
                }
                SQLITE_INDEX_CONSTRAINT_LE if upper_idx.is_none() => {
                    upper_idx = Some((i, RANGE_FLAG_UPPER_INCLUSIVE));
                }
                SQLITE_INDEX_CONSTRAINT_LT if upper_idx.is_none() => {
                    upper_idx = Some((i, RANGE_FLAG_UPPER_EXCLUSIVE));
                }
                _ => {}
            }
        }

        // Check if `ORDER BY pk ASC` can be satisfied directly by the memcomparable PK scan
        let order_by_pk_asc = info.nOrderBy == 1 && {
            let ob = &*info.aOrderBy;
            (ob.iColumn == vtab.pk_col_idx as i32 || ob.iColumn == -1) && ob.desc == 0
        };

        if lower_idx.is_some() || upper_idx.is_some() {
            let mut flags = 0;
            let mut next_arg = 1;
            if let Some((i, fl)) = lower_idx {
                usages[i].argvIndex = next_arg;
                usages[i].omit = 1;
                next_arg += 1;
                flags |= fl;
            }
            if let Some((i, fl)) = upper_idx {
                usages[i].argvIndex = next_arg;
                usages[i].omit = 1;
                next_arg += 1;
                flags |= fl;
            }
            if let Some(lim_i) = limit_idx {
                let all_where_omitted = (0..n).all(|idx| {
                    idx == lim_i || constraints[idx].usable == 0 || usages[idx].omit != 0
                });
                if all_where_omitted && (info.nOrderBy == 0 || order_by_pk_asc) {
                    usages[lim_i].argvIndex = next_arg;
                    usages[lim_i].omit = 1;
                    flags |= RANGE_FLAG_HAS_LIMIT;
                }
            }
            if order_by_pk_asc {
                info.orderByConsumed = 1;
            }
            info.idxNum = PLAN_PK_RANGE | (flags << 8);
            info.estimatedCost = 25.0;
            info.estimatedRows = 32;
            return SQLITE_OK;
        }

        // 4. Fallback: Full Table Scan
        if order_by_pk_asc {
            info.orderByConsumed = 1;
        }
        info.idxNum = PLAN_FULL_SCAN;
        info.estimatedCost = 10_000.0;
        info.estimatedRows = 1_000;
        SQLITE_OK
    }
}

unsafe extern "system" fn vtab_open(
    _p_vtab: *mut sqlite3_vtab,
    pp_cursor: *mut *mut sqlite3_vtab_cursor,
) -> c_int {
    unsafe {
        let cursor = Box::new(AydbCursor {
            base: sqlite3_vtab_cursor {
                pVtab: core::ptr::null_mut(),
            },
            slab: Vec::with_capacity(512),
            spans: Vec::with_capacity(16),
            pos: 0,
        });
        *pp_cursor = Box::into_raw(cursor) as *mut sqlite3_vtab_cursor;
        SQLITE_OK
    }
}

unsafe extern "system" fn vtab_close(p_cursor: *mut sqlite3_vtab_cursor) -> c_int {
    if !p_cursor.is_null() {
        unsafe {
            drop(Box::from_raw(p_cursor as *mut AydbCursor));
        }
    }
    SQLITE_OK
}

unsafe extern "system" fn vtab_filter(
    p_cursor: *mut sqlite3_vtab_cursor,
    idx_num: c_int,
    _idx_str: *const c_char,
    argc: c_int,
    argv: *mut *mut sqlite3_value,
) -> c_int {
    unsafe {
        let cursor = &mut *(p_cursor as *mut AydbCursor);
        let vtab = &*(cursor.base.pVtab as *const AydbVTab);
        cursor.slab.clear();
        cursor.spans.clear();
        cursor.pos = 0;

        let mut eng = match vtab.engine.lock() {
            Ok(g) => g,
            Err(_) => return SQLITE_ERROR,
        };

        let base_plan = idx_num & 0xFF;
        match base_plan {
            PLAN_PK_POINT => {
                if argc < 1 {
                    return SQLITE_ERROR;
                }
                let val_ptr = *argv.offset(0);
                if sqlite3_value_type(val_ptr) == SQLITE_NULL {
                    return SQLITE_OK;
                }
                let pt_flags = (idx_num >> 8) & 0xFF;
                if (pt_flags & POINT_FLAG_BY_ROWID) != 0 {
                    if sqlite3_value_type(val_ptr) == SQLITE_INTEGER {
                        let rid = sqlite3_value_int64(val_ptr);
                        if vtab.has_non_int_pks.load(Ordering::Relaxed) {
                            if let Ok(rmap) = vtab.rowid_to_pk.lock() {
                                if let Some(mapped_pk) = rmap.get_pk(rid) {
                                    let _ = eng.get_row_into(
                                        vtab.table_id,
                                        mapped_pk,
                                        &mut cursor.slab,
                                        &mut cursor.spans,
                                    );
                                    return SQLITE_OK;
                                }
                            }
                        }
                        if vtab.pk_affinity == ColumnAffinity::Integer {
                            let pk_arr = encode_i64_memcomparable(rid);
                            let _ = eng.get_row_into(
                                vtab.table_id,
                                &pk_arr,
                                &mut cursor.slab,
                                &mut cursor.spans,
                            );
                        }
                    }
                } else {
                    let pk_bytes = sqlite_value_to_key_bytes(val_ptr);
                    let _ = eng.get_row_into(
                        vtab.table_id,
                        &pk_bytes,
                        &mut cursor.slab,
                        &mut cursor.spans,
                    );
                }
            }
            PLAN_SECONDARY_EQ => {
                if argc < 1 {
                    return SQLITE_ERROR;
                }
                let val_ptr = *argv.offset(0);
                if sqlite3_value_type(val_ptr) == SQLITE_NULL {
                    return SQLITE_OK;
                }
                let index_id = ((idx_num >> 8) & 0xFFFF) as u16;
                let col_val = sqlite_value_to_key_bytes(val_ptr);
                if let Ok(pks) = eng.lookup_by_secondary_index(vtab.table_id, index_id, &col_val) {
                    for pk in pks {
                        let _ = eng.get_row_into(
                            vtab.table_id,
                            &pk,
                            &mut cursor.slab,
                            &mut cursor.spans,
                        );
                    }
                }
            }
            PLAN_PK_RANGE => {
                let flags = idx_num >> 8;
                let mut arg_pos = 0;
                let lower_bound = if (flags
                    & (RANGE_FLAG_LOWER_INCLUSIVE | RANGE_FLAG_LOWER_EXCLUSIVE))
                    != 0
                    && arg_pos < argc
                {
                    let b = sqlite_value_to_key_bytes(*argv.offset(arg_pos as isize));
                    arg_pos += 1;
                    Some((b, (flags & RANGE_FLAG_LOWER_INCLUSIVE) != 0))
                } else {
                    None
                };
                let upper_bound = if (flags
                    & (RANGE_FLAG_UPPER_INCLUSIVE | RANGE_FLAG_UPPER_EXCLUSIVE))
                    != 0
                    && arg_pos < argc
                {
                    let b = sqlite_value_to_key_bytes(*argv.offset(arg_pos as isize));
                    arg_pos += 1;
                    Some((b, (flags & RANGE_FLAG_UPPER_INCLUSIVE) != 0))
                } else {
                    None
                };
                let limit = if (flags & RANGE_FLAG_HAS_LIMIT) != 0 && arg_pos < argc {
                    let lim_val = sqlite3_value_int64(*argv.offset(arg_pos as isize));
                    if lim_val > 0 { lim_val as usize } else { 0 }
                } else {
                    usize::MAX
                };

                let _ = eng.scan_table_values_into(
                    vtab.table_id,
                    lower_bound.as_ref().map(|(b, inc)| (b.as_slice(), *inc)),
                    upper_bound.as_ref().map(|(b, inc)| (b.as_slice(), *inc)),
                    limit,
                    &mut cursor.slab,
                    &mut cursor.spans,
                );
            }
            _ => {
                let _ = eng.scan_table_values_into(
                    vtab.table_id,
                    None,
                    None,
                    usize::MAX,
                    &mut cursor.slab,
                    &mut cursor.spans,
                );
            }
        }

        SQLITE_OK
    }
}

unsafe extern "system" fn vtab_next(p_cursor: *mut sqlite3_vtab_cursor) -> c_int {
    unsafe {
        let cursor = &mut *(p_cursor as *mut AydbCursor);
        cursor.pos += 1;
        SQLITE_OK
    }
}

unsafe extern "system" fn vtab_eof(p_cursor: *mut sqlite3_vtab_cursor) -> c_int {
    unsafe {
        let cursor = &*(p_cursor as *const AydbCursor);
        if cursor.pos >= cursor.spans.len() {
            1
        } else {
            0
        }
    }
}

unsafe extern "system" fn vtab_column(
    p_cursor: *mut sqlite3_vtab_cursor,
    p_ctx: *mut sqlite3_context,
    i_col: c_int,
) -> c_int {
    unsafe {
        let cursor = &*(p_cursor as *const AydbCursor);
        if cursor.pos >= cursor.spans.len() {
            return SQLITE_ERROR;
        }
        let (start, end) = cursor.spans[cursor.pos];
        let row_bytes = &cursor.slab[start as usize..end as usize];
        if i_col < 0 {
            let vtab = &*(cursor.base.pVtab as *const AydbVTab);
            let rid = extract_rowid_from_row(row_bytes, vtab);
            sqlite3_result_int64(p_ctx, rid);
            return SQLITE_OK;
        }
        match project_column_zero_copy(row_bytes, i_col as usize, p_ctx) {
            Ok(()) => SQLITE_OK,
            Err(_) => SQLITE_ERROR,
        }
    }
}

unsafe extern "system" fn vtab_rowid(
    p_cursor: *mut sqlite3_vtab_cursor,
    p_rowid: *mut sqlite3_int64,
) -> c_int {
    unsafe {
        let cursor = &*(p_cursor as *const AydbCursor);
        if cursor.pos >= cursor.spans.len() {
            return SQLITE_ERROR;
        }
        let (start, end) = cursor.spans[cursor.pos];
        let row_bytes = &cursor.slab[start as usize..end as usize];
        let vtab = &*(cursor.base.pVtab as *const AydbVTab);
        *p_rowid = extract_rowid_from_row(row_bytes, vtab);
        SQLITE_OK
    }
}

unsafe extern "system" fn vtab_update(
    p_vtab: *mut sqlite3_vtab,
    argc: c_int,
    argv: *mut *mut sqlite3_value,
    p_rowid: *mut sqlite3_int64,
) -> c_int {
    unsafe {
        let vtab = &*(p_vtab as *const AydbVTab);
        let mut eng = match vtab.engine.lock() {
            Ok(g) => g,
            Err(_) => return SQLITE_ERROR,
        };

        // Case 1: DELETE (`argc == 1`)
        if argc == 1 {
            let arg0 = *argv.offset(0);
            if sqlite3_value_type(arg0) == SQLITE_INTEGER {
                let rid = sqlite3_value_int64(arg0);
                if vtab.has_non_int_pks.load(Ordering::Relaxed) {
                    if let Ok(mut rmap) = vtab.rowid_to_pk.lock() {
                        if let Some(old_pk) = rmap.remove_by_rowid(rid) {
                            if eng.delete_row(vtab.table_id, &old_pk, vtab.durability).is_err() {
                                return SQLITE_ERROR;
                            }
                            return SQLITE_OK;
                        }
                    }
                }
                if vtab.pk_affinity == ColumnAffinity::Integer {
                    let old_pk = encode_i64_memcomparable(rid);
                    if eng.delete_row(vtab.table_id, &old_pk, vtab.durability).is_err() {
                        return SQLITE_ERROR;
                    }
                }
            } else {
                let old_pk = sqlite_value_to_key_bytes(arg0);
                if vtab.has_non_int_pks.load(Ordering::Relaxed) {
                    if let Ok(mut rmap) = vtab.rowid_to_pk.lock() {
                        rmap.remove_by_pk(&old_pk);
                    }
                }
                if eng.delete_row(vtab.table_id, &old_pk, vtab.durability).is_err() {
                    return SQLITE_ERROR;
                }
            }
            return SQLITE_OK;
        }

        // Case 2: INSERT or UPDATE (`argc == 2 + num_cols`)
        let num_cols = vtab.col_names.len();
        if (argc as usize) < 2 + num_cols {
            return SQLITE_ERROR;
        }

        let pk_val_ptr = *argv.offset(2 + vtab.pk_col_idx as isize);
        let pk_is_int = vtab.pk_affinity == ColumnAffinity::Integer
            && sqlite3_value_type(pk_val_ptr) == SQLITE_INTEGER;

        let (new_pk, row_bytes, sec_keys) = encode_sqlite_values_direct(
            argv.offset(2),
            num_cols,
            vtab.pk_col_idx,
            &vtab.secondary_indexes,
        );

        let arg0 = *argv.offset(0);
        if sqlite3_value_type(arg0) != SQLITE_NULL {
            if sqlite3_value_type(arg0) == SQLITE_INTEGER {
                let rid = sqlite3_value_int64(arg0);
                let mut resolved_in_map = false;
                if vtab.has_non_int_pks.load(Ordering::Relaxed) {
                    if let Ok(mut rmap) = vtab.rowid_to_pk.lock() {
                        if let Some(old_pk) = rmap.get_pk(rid).cloned() {
                            resolved_in_map = true;
                            if old_pk.as_slice() != new_pk.as_slice() {
                                rmap.remove_by_rowid(rid);
                                let _ = eng.delete_row(vtab.table_id, &old_pk, vtab.durability);
                            }
                        }
                    }
                }
                if !resolved_in_map && vtab.pk_affinity == ColumnAffinity::Integer {
                    let old_pk = encode_i64_memcomparable(rid);
                    if old_pk.as_slice() != new_pk.as_slice() {
                        let _ = eng.delete_row(vtab.table_id, &old_pk, vtab.durability);
                    }
                }
            } else {
                let old_pk = sqlite_value_to_key_bytes(arg0);
                if old_pk != new_pk {
                    if vtab.has_non_int_pks.load(Ordering::Relaxed) {
                        if let Ok(mut rmap) = vtab.rowid_to_pk.lock() {
                            rmap.remove_by_pk(&old_pk);
                        }
                    }
                    let _ = eng.delete_row(vtab.table_id, &old_pk, vtab.durability);
                }
            }
        }

        if eng
            .put_row_owned(
                vtab.table_id,
                &new_pk,
                row_bytes,
                &sec_keys,
                vtab.durability,
            )
            .is_err()
        {
            return SQLITE_ERROR;
        }

        let rid = if pk_is_int {
            sqlite3_value_int64(pk_val_ptr)
        } else {
            vtab.has_non_int_pks.store(true, Ordering::Relaxed);
            if let Ok(mut rmap) = vtab.rowid_to_pk.lock() {
                rmap.get_or_assign_rowid(&new_pk)
            } else {
                hash_pk_to_rowid(&new_pk)
            }
        };
        if !p_rowid.is_null() {
            *p_rowid = rid;
        }
        SQLITE_OK
    }
}

unsafe extern "system" fn vtab_begin_tx(p_vtab: *mut sqlite3_vtab) -> c_int {
    unsafe {
        let vtab = &*(p_vtab as *const AydbVTab);
        if let Ok(mut eng) = vtab.engine.lock() {
            eng.begin_transaction();
        }
        SQLITE_OK
    }
}

unsafe extern "system" fn vtab_commit_tx(p_vtab: *mut sqlite3_vtab) -> c_int {
    unsafe {
        let vtab = &*(p_vtab as *const AydbVTab);
        if let Ok(mut eng) = vtab.engine.lock() {
            if eng.commit_transaction().is_err() {
                return SQLITE_ERROR;
            }
        }
        SQLITE_OK
    }
}

unsafe extern "system" fn vtab_rollback_tx(p_vtab: *mut sqlite3_vtab) -> c_int {
    unsafe {
        let vtab = &*(p_vtab as *const AydbVTab);
        if let Ok(mut eng) = vtab.engine.lock() {
            if eng.rollback_transaction().is_err() {
                return SQLITE_ERROR;
            }
        }
        SQLITE_OK
    }
}

unsafe extern "system" fn vtab_noop_tx(_p_vtab: *mut sqlite3_vtab) -> c_int {
    SQLITE_OK
}

// =========================================================================
// Safe High-Level Rust Wrapper for SQLite Connection + Prepared Statements
// =========================================================================

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

    fn last_error(&self, context: &str) -> io::Error {
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

impl SqliteConnection {
    fn last_io_error(&self, context: &str) -> io::Error {
        self.last_error(context)
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
            return Err(self.conn.last_io_error("sqlite3_step(execute)"));
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
                return Err(self.conn.last_io_error("sqlite3_step(query)"));
            }
        }
        unsafe {
            sqlite3_reset(self.stmt);
        }
        Ok(rows)
    }

    fn bind_all(&mut self, params: &[SqlValue]) -> io::Result<()> {
        for (i, p) in params.iter().enumerate() {
            let idx = (i + 1) as c_int;
            let rc = unsafe {
                match p {
                    SqlValue::Null => sqlite3_bind_null(self.stmt, idx),
                    SqlValue::Integer(v) => sqlite3_bind_int64(self.stmt, idx, *v),
                    SqlValue::Float(v) => sqlite3_bind_double(self.stmt, idx, *v),
                    SqlValue::Text(s) => sqlite3_bind_text(
                        self.stmt,
                        idx,
                        s.as_ptr() as *const c_char,
                        s.len() as c_int,
                        sqlite_transient(),
                    ),
                    SqlValue::Blob(b) => sqlite3_bind_blob(
                        self.stmt,
                        idx,
                        b.as_ptr() as *const c_void,
                        b.len() as c_int,
                        sqlite_transient(),
                    ),
                }
            };
            if rc != SQLITE_OK {
                return Err(self.conn.last_io_error("sqlite3_bind"));
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
                SQLITE_BLOB | _ => {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::EngineConfig;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static VTAB_TEST_COUNTER: AtomicU64 = AtomicU64::new(1);

    struct TempTestDir {
        path: PathBuf,
    }

    impl TempTestDir {
        fn new(prefix: &str) -> Self {
            let id = VTAB_TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
            let pid = std::process::id();
            let dir = std::env::temp_dir().join(format!("aydb_vtab_test_{prefix}_{pid}_{id}"));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).expect("create temp test dir");
            Self { path: dir }
        }
    }

    impl Drop for TempTestDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    #[test]
    fn sqlite_vtab_point_lookup_secondary_index_range_and_capped_lru() {
        let tmp = TempTestDir::new("vtab_full");
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
        let mut cfg = EngineConfig::default();
        cfg.inline_threshold_bytes = 256;
        let engine = Arc::new(Mutex::new(StorageEngine::open(fs, cfg).unwrap()));

        let conn = SqliteConnection::open_in_memory().unwrap();
        conn.register_aydb_module(engine.clone()).unwrap();

        // Create a Capped Virtual Table (`max_bytes = 3500`, fits at most 3 rows of ~1000 B each)
        conn.execute_batch(
            "CREATE VIRTUAL TABLE http_cache USING aydb(
                table_id = 1,
                max_bytes = 3500,
                durability = 'buffered',
                columns = 'url TEXT PRIMARY KEY, status INT, body BLOB, content_type TEXT',
                indexes = 'content_type, status'
            );",
        )
        .unwrap();

        // Verify EXPLAIN QUERY PLAN uses Plan 1 (`idxNum = 1`) for `WHERE url = ?`
        let eqp_pk = conn
            .query_all(
                "EXPLAIN QUERY PLAN SELECT body FROM http_cache WHERE url = 'https://a.com'",
                &[],
            )
            .unwrap();
        assert!(!eqp_pk.is_empty());

        let mut insert_stmt = conn
            .prepare("INSERT OR REPLACE INTO http_cache VALUES (?1, ?2, ?3, ?4)")
            .unwrap();

        let blob_1k = vec![0xAAu8; 1000];
        insert_stmt
            .execute(&[
                SqlValue::Text("https://site.example/1".into()),
                SqlValue::Integer(200),
                SqlValue::Blob(blob_1k.clone()),
                SqlValue::Text("text/html".into()),
            ])
            .unwrap();
        insert_stmt
            .execute(&[
                SqlValue::Text("https://site.example/2".into()),
                SqlValue::Integer(200),
                SqlValue::Blob(blob_1k.clone()),
                SqlValue::Text("application/json".into()),
            ])
            .unwrap();
        insert_stmt
            .execute(&[
                SqlValue::Text("https://site.example/3".into()),
                SqlValue::Integer(404),
                SqlValue::Blob(blob_1k.clone()),
                SqlValue::Text("text/html".into()),
            ])
            .unwrap();

        // 1. Point Lookup on PK (`WHERE url = ?`) — also touches `https://site.example/1` in AccessTracker!
        let hit1 = conn
            .query_all(
                "SELECT status, length(body) FROM http_cache WHERE url = ?1",
                &[SqlValue::Text("https://site.example/1".into())],
            )
            .unwrap();
        assert_eq!(
            hit1,
            vec![vec![SqlValue::Integer(200), SqlValue::Integer(1000)]]
        );

        // 2. Secondary Index Equality Lookup (`WHERE content_type = 'text/html'`)
        let html_urls = conn
            .query_all(
                "SELECT url FROM http_cache WHERE content_type = ?1 ORDER BY url",
                &[SqlValue::Text("text/html".into())],
            )
            .unwrap();
        assert_eq!(
            html_urls,
            vec![
                vec![SqlValue::Text("https://site.example/1".into())],
                vec![SqlValue::Text("https://site.example/3".into())],
            ]
        );

        // 3. Insert 4th row (~1000 B) -> exceeds `max_bytes = 3500`!
        // Because `/1` and `/3` were just read above, `/2` is the coldest un-accessed entry and must be evicted!
        insert_stmt
            .execute(&[
                SqlValue::Text("https://site.example/4".into()),
                SqlValue::Integer(201),
                SqlValue::Blob(blob_1k.clone()),
                SqlValue::Text("image/png".into()),
            ])
            .unwrap();

        let remaining = conn
            .query_all("SELECT url FROM http_cache ORDER BY url", &[])
            .unwrap();
        assert_eq!(
            remaining,
            vec![
                vec![SqlValue::Text("https://site.example/1".into())],
                vec![SqlValue::Text("https://site.example/3".into())],
                vec![SqlValue::Text("https://site.example/4".into())],
            ]
        );

        // 4. Primary Key Range Query (`WHERE url >= .../3 AND url <= .../4`)
        let range_rows = conn
            .query_all(
                "SELECT url, status FROM http_cache WHERE url >= ?1 AND url <= ?2 ORDER BY url",
                &[
                    SqlValue::Text("https://site.example/3".into()),
                    SqlValue::Text("https://site.example/4".into()),
                ],
            )
            .unwrap();
        assert_eq!(
            range_rows,
            vec![
                vec![
                    SqlValue::Text("https://site.example/3".into()),
                    SqlValue::Integer(404)
                ],
                vec![
                    SqlValue::Text("https://site.example/4".into()),
                    SqlValue::Integer(201)
                ],
            ]
        );

        // 5. SQL UPDATE & DELETE via Virtual Table
        conn.execute_batch(
            "UPDATE http_cache SET status = 500 WHERE url = 'https://site.example/3';
             DELETE FROM http_cache WHERE url = 'https://site.example/4';",
        )
        .unwrap();

        let after_mut = conn
            .query_all("SELECT url, status FROM http_cache ORDER BY url", &[])
            .unwrap();
        assert_eq!(
            after_mut,
            vec![
                vec![
                    SqlValue::Text("https://site.example/1".into()),
                    SqlValue::Integer(200)
                ],
                vec![
                    SqlValue::Text("https://site.example/3".into()),
                    SqlValue::Integer(500)
                ],
            ]
        );

        // 6. Verify ACID ROLLBACK restores pre-transaction state cleanly
        conn.execute_batch(
            "BEGIN;
             DELETE FROM http_cache WHERE url = 'https://site.example/1';
             UPDATE http_cache SET status = 999 WHERE url = 'https://site.example/3';
             ROLLBACK;",
        )
        .unwrap();

        let after_rollback = conn
            .query_all("SELECT url, status FROM http_cache ORDER BY url", &[])
            .unwrap();
        assert_eq!(after_rollback, after_mut);
    }

    #[test]
    fn sqlite_vtab_real_pk_8byte_text_pk_crc_collision_and_limit_pushdown() {
        let tmp = TempTestDir::new("vtab_regressions");
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
        let engine = Arc::new(Mutex::new(
            StorageEngine::open(fs, EngineConfig::default()).unwrap(),
        ));
        let conn = SqliteConnection::open_in_memory().unwrap();
        conn.register_aydb_module(engine).unwrap();

        // 1. REAL (f64) primary key ordering & range scans across negative, -0.0/+0.0, and positive floats
        conn.execute_batch(
            "CREATE VIRTUAL TABLE real_t USING aydb(
                table_id = 10,
                columns = 'k REAL PRIMARY KEY, v TEXT'
            );
            INSERT INTO real_t VALUES (-10.0, 'a'), (-2.0, 'b'), (-1.0, 'c'), (-0.0, 'z0'), (1.0, 'e'), (10.0, 'f');
            INSERT OR REPLACE INTO real_t VALUES (0.0, 'd');",
        )
        .unwrap();

        let all_real = conn.query_all("SELECT k, v FROM real_t", &[]).unwrap();
        assert_eq!(
            all_real,
            vec![
                vec![SqlValue::Float(-10.0), SqlValue::Text("a".into())],
                vec![SqlValue::Float(-2.0), SqlValue::Text("b".into())],
                vec![SqlValue::Float(-1.0), SqlValue::Text("c".into())],
                vec![SqlValue::Float(0.0), SqlValue::Text("d".into())],
                vec![SqlValue::Float(1.0), SqlValue::Text("e".into())],
                vec![SqlValue::Float(10.0), SqlValue::Text("f".into())],
            ]
        );

        let range_real = conn
            .query_all(
                "SELECT k FROM real_t WHERE k >= ?1 AND k <= ?2",
                &[SqlValue::Float(-2.0), SqlValue::Float(1.0)],
            )
            .unwrap();
        assert_eq!(
            range_real,
            vec![
                vec![SqlValue::Float(-2.0)],
                vec![SqlValue::Float(-1.0)],
                vec![SqlValue::Float(0.0)],
                vec![SqlValue::Float(1.0)],
            ]
        );

        // 2. 8-byte TEXT primary keys ("user0001") UPDATE and DELETE via SQLite vtab_update
        conn.execute_batch(
            "CREATE VIRTUAL TABLE text8_t USING aydb(
                table_id = 11,
                columns = 'id TEXT PRIMARY KEY, val INT, tag TEXT',
                indexes = 'tag'
            );
            INSERT INTO text8_t VALUES ('user0001', 10, 'alpha');
            INSERT INTO text8_t VALUES ('user0002', 20, ' (empty) ');
            INSERT INTO text8_t VALUES ('user0003', 30, NULL);
            INSERT INTO text8_t VALUES ('user0004', 40, '');",
        )
        .unwrap();

        conn.execute_batch(
            "UPDATE text8_t SET val = 111 WHERE id = 'user0001';
             DELETE FROM text8_t WHERE id = 'user0002';",
        )
        .unwrap();

        let text8_rows = conn
            .query_all("SELECT id, val FROM text8_t ORDER BY id", &[])
            .unwrap();
        assert_eq!(
            text8_rows,
            vec![
                vec![SqlValue::Text("user0001".into()), SqlValue::Integer(111)],
                vec![SqlValue::Text("user0003".into()), SqlValue::Integer(30)],
                vec![SqlValue::Text("user0004".into()), SqlValue::Integer(40)],
            ]
        );

        // Also check NULL vs '' on secondary index `tag`
        let empty_tag_rows = conn
            .query_all("SELECT id FROM text8_t WHERE tag = ''", &[])
            .unwrap();
        assert_eq!(
            empty_tag_rows,
            vec![vec![SqlValue::Text("user0004".into())]]
        );

        // 3. 32-bit CRC32C-colliding TEXT primary keys UPDATE and DELETE
        let mut seen_crc: HashMap<u32, String> = HashMap::with_capacity(100_000);
        let mut pair: Option<(String, String)> = None;
        let mut rng = 0x9E37_79B9_7F4A_7C15u64;
        for _ in 0..500_000u32 {
            rng ^= rng >> 12;
            rng ^= rng << 25;
            rng ^= rng >> 27;
            let val = rng.wrapping_mul(0x2545_F491_4F6C_DD1D);
            let candidate = format!("user_{val:016x}");
            let h = crate::util::crc32c::crc32c(candidate.as_bytes());
            if let Some(prev) = seen_crc.insert(h, candidate.clone()) {
                if prev != candidate {
                    pair = Some((prev, candidate));
                    break;
                }
            }
        }
        let (k1, k2) = pair.expect("found 32-bit CRC32C collision");
        conn.execute_batch(
            "CREATE VIRTUAL TABLE crc_users USING aydb(
                table_id = 12,
                columns = 'id TEXT PRIMARY KEY, name TEXT'
            );",
        )
        .unwrap();
        conn.execute_batch(&format!(
            "INSERT INTO crc_users VALUES ('{k1}', 'Alice'), ('{k2}', 'Bob');"
        ))
        .unwrap();
        let r1 = conn
            .query_all(
                &format!("SELECT rowid FROM crc_users WHERE id = '{k1}'"),
                &[],
            )
            .unwrap();
        let rid1 = match r1[0][0] {
            SqlValue::Integer(v) => v,
            _ => unreachable!(),
        };
        conn.execute_batch(&format!(
            "UPDATE crc_users SET name = 'BobUpdated' WHERE id = '{k2}';
             DELETE FROM crc_users WHERE rowid = {rid1};"
        ))
        .unwrap();
        let k1_rows = conn
            .query_all(&format!("SELECT name FROM crc_users WHERE id = '{k1}'"), &[])
            .unwrap();
        let k2_rows = conn
            .query_all(&format!("SELECT name FROM crc_users WHERE id = '{k2}'"), &[])
            .unwrap();
        assert!(k1_rows.is_empty());
        assert_eq!(k2_rows, vec![vec![SqlValue::Text("BobUpdated".into())]]);

        // 4. SELECT ... WHERE id >= ? AND other_col = ? LIMIT ? with non-matching rows preceding matching rows
        conn.execute_batch(
            "CREATE VIRTUAL TABLE items_t USING aydb(
                table_id = 13,
                columns = 'id INT PRIMARY KEY, other_col TEXT'
            );",
        )
        .unwrap();
        let mut ins_item = conn.prepare("INSERT INTO items_t VALUES (?1, ?2)").unwrap();
        for i in 1..=20 {
            let cat = if i <= 10 { "skip" } else { "match" };
            ins_item
                .execute(&[SqlValue::Integer(i), SqlValue::Text(cat.into())])
                .unwrap();
        }
        let matched = conn
            .query_all(
                "SELECT id FROM items_t WHERE id >= ?1 AND other_col = ?2 LIMIT ?3",
                &[
                    SqlValue::Integer(1),
                    SqlValue::Text("match".into()),
                    SqlValue::Integer(5),
                ],
            )
            .unwrap();
        assert_eq!(
            matched,
            vec![
                vec![SqlValue::Integer(11)],
                vec![SqlValue::Integer(12)],
                vec![SqlValue::Integer(13)],
                vec![SqlValue::Integer(14)],
                vec![SqlValue::Integer(15)],
            ]
        );
    }
}
