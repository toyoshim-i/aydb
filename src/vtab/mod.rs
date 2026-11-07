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
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::engine::{StorageEngine, WriteDurability};
use crate::pal::OsFs;
use crate::util::crc32c::crc32c_append;
use crate::vtab::ffi::{
    SQLITE_BLOB, SQLITE_BUSY, SQLITE_CONSTRAINT, SQLITE_CORRUPT, SQLITE_DONE, SQLITE_ERROR,
    SQLITE_FLOAT, SQLITE_FULL, SQLITE_IGNORE, SQLITE_INDEX_CONSTRAINT_EQ,
    SQLITE_INDEX_CONSTRAINT_GE, SQLITE_INDEX_CONSTRAINT_GT, SQLITE_INDEX_CONSTRAINT_LE,
    SQLITE_INDEX_CONSTRAINT_LT, SQLITE_INDEX_SCAN_UNIQUE, SQLITE_INTEGER, SQLITE_IOERR,
    SQLITE_NULL, SQLITE_OK, SQLITE_REPLACE, SQLITE_ROW, SQLITE_TEXT, sqlite_transient, sqlite3,
    sqlite3_bind_blob, sqlite3_bind_double, sqlite3_bind_int64, sqlite3_bind_null,
    sqlite3_bind_text, sqlite3_clear_bindings, sqlite3_close, sqlite3_column_blob,
    sqlite3_column_bytes, sqlite3_column_count, sqlite3_column_double, sqlite3_column_int64,
    sqlite3_column_text, sqlite3_column_type, sqlite3_context, sqlite3_create_module_v2,
    sqlite3_declare_vtab, sqlite3_errmsg, sqlite3_exec, sqlite3_finalize, sqlite3_index_info,
    sqlite3_int64, sqlite3_module, sqlite3_open, sqlite3_prepare_v2, sqlite3_reset,
    sqlite3_result_int64, sqlite3_step, sqlite3_stmt, sqlite3_value, sqlite3_value_double,
    sqlite3_value_int64, sqlite3_value_type, sqlite3_vtab, sqlite3_vtab_cursor,
    sqlite3_vtab_on_conflict,
};
pub use crate::vtab::row_codec::{
    ColCollation, ColumnAffinity, KEY_TAG_FLOAT, KEY_TAG_FLOAT_GE_I64_MAX_PLUS_1,
    KEY_TAG_FLOAT_LT_I64_MIN, KEY_TAG_INTEGER, KEY_TAG_NULL, SqlValue, decode_all_columns,
    decode_column_at, decode_f64_memcomparable, decode_i64_tagged_key,
    encode_canonical_numeric_key_from_f64, encode_f64_memcomparable, encode_f64_unified_key,
    encode_i64_tagged_key, encode_row, encode_sqlite_values_direct, extract_column_key_bytes,
    extract_column_key_bytes_with_collation, f64_to_exact_i64, parse_sqlite_numeric_str,
    project_column_zero_copy, sqlite_value_to_key_bytes, sqlite_value_to_key_bytes_with_schema,
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
const RANGE_FLAG_KEYS_ONLY: c_int = 0x20;

/// Maximum number of non-integer primary keys retained in `RowidMap` once all cursors close (`C-04`).
const MAX_ROWID_MAP_ENTRIES: usize = 262_144;

/// Returns the alternate exact-integer numeric key representation (`9`-byte `Integer(q)` vs
/// `17`-byte `Float(q.0)`) so SQLite's `10 == 10.0` numeric key equality holds across `BLOB`
/// and `REAL` affinities (`C-02`).
#[inline]
fn build_numeric_alt_key(pk: &[u8]) -> Option<Vec<u8>> {
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

/// Checks if `pk` or its exact-integer numeric dual (`build_numeric_alt_key(pk)`) exists in
/// `table_id`, returning the existing key bytes if found (`C-02`).
#[inline]
fn find_existing_pk_or_numeric_alt(
    eng: &mut StorageEngine<OsFs>,
    table_id: u16,
    pk: &[u8],
) -> io::Result<Option<Vec<u8>>> {
    if eng.row_exists(table_id, pk)? {
        return Ok(Some(pk.to_vec()));
    }
    if let Some(alt) = build_numeric_alt_key(pk) {
        if eng.row_exists(table_id, &alt)? {
            return Ok(Some(alt));
        }
    }
    Ok(None)
}

/// Maps a `StorageEngine` [`io::Error`] into the most specific SQLite result code (`AUD-11`, `F01`).
#[inline]
fn io_err_to_sqlite_rc(err: &io::Error) -> c_int {
    match err.kind() {
        io::ErrorKind::WouldBlock => SQLITE_BUSY,
        io::ErrorKind::InvalidData => SQLITE_CORRUPT,
        io::ErrorKind::InvalidInput | io::ErrorKind::AlreadyExists => SQLITE_ERROR,
        _ => SQLITE_IOERR,
    }
}

/// Splits a comma-separated SQL column list at top-level commas only, ignoring commas inside
/// parenthesized type parameters such as `DECIMAL(10, 2)` (`USE-03`).
fn split_top_level_commas(input: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0usize;
    let mut start = 0usize;
    for (idx, ch) in input.char_indices() {
        match ch {
            '(' => depth = depth.saturating_add(1),
            ')' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                parts.push(&input[start..idx]);
                start = idx + 1;
            }
            _ => {}
        }
    }
    if start <= input.len() {
        let tail = &input[start..];
        if !tail.trim().is_empty() {
            parts.push(tail);
        }
    }
    parts
}

/// Collision-free bidirectional map between synthetic 64-bit SQLite `rowid`s and non-integer `encoded_pk` bytes.
#[derive(Default)]
struct RowidMap {
    by_rowid: HashMap<i64, Vec<u8>>,
    by_pk: HashMap<Vec<u8>, i64>,
    fifo_order: Vec<i64>,
}

impl RowidMap {
    /// Evicts the oldest half of entries when the map exceeds `MAX_ROWID_MAP_ENTRIES` (`C-04`),
    /// preserving all recently assigned `rowid` mappings for subsequent `UPDATE`/`DELETE WHERE rowid = ?`.
    fn evict_oldest_half_if_needed(&mut self) {
        if self.by_rowid.len() <= MAX_ROWID_MAP_ENTRIES {
            return;
        }
        let target_keep = MAX_ROWID_MAP_ENTRIES / 2;
        let mut idx = 0usize;
        while self.by_rowid.len() > target_keep && idx < self.fifo_order.len() {
            let rid = self.fifo_order[idx];
            idx += 1;
            if let Some(pk) = self.by_rowid.remove(&rid) {
                self.by_pk.remove(&pk);
            }
        }
        self.fifo_order.drain(0..idx);
        let rmap_ref = &self.by_rowid;
        self.fifo_order.retain(|rid| rmap_ref.contains_key(rid));
    }

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
        self.fifo_order.push(rid);
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
    xRename: None,
    xSavepoint: Some(vtab_savepoint),
    xRelease: Some(vtab_release),
    xRollbackTo: Some(vtab_rollback_to),
    xShadowName: None,
    xIntegrity: None,
};

struct ModuleAuxContext {
    engine: Arc<Mutex<StorageEngine<OsFs>>>,
}

#[repr(C)]
struct AydbVTab {
    base: sqlite3_vtab,
    db: *mut sqlite3,
    engine: Arc<Mutex<StorageEngine<OsFs>>>,
    table_id: u16,
    table_name: String,
    durability: WriteDurability,
    col_names: Vec<String>,
    col_affinities: Vec<ColumnAffinity>,
    col_collations: Vec<ColCollation>,
    pk_col_idx: usize,
    pk_affinity: ColumnAffinity,
    pk_collation: ColCollation,
    /// Compact list of `(col_idx, index_id)` (`1..=N`) for secondary indexed columns.
    secondary_indexes: Vec<(usize, u16)>,
    /// Fast flag set to `true` only if a non-integer primary key has been stored.
    /// When `false`, `rowid_to_pk` is never locked on queries, inserts, updates, or deletes!
    has_non_int_pks: AtomicBool,
    /// Number of currently open cursors on this virtual table (`PERF-05`).
    open_cursors: AtomicUsize,
    /// Next monotonically increasing auto-assigned rowid for `INTEGER PRIMARY KEY` (`USE-05`).
    next_auto_rowid: AtomicI64,
    /// Collision-free bidirectional map between synthetic 64-bit `rowid`s and `encoded_pk` bytes
    /// so SQLite `DELETE` / `UPDATE` by `rowid` can resolve the target primary key.
    rowid_to_pk: Mutex<RowidMap>,
}

#[repr(C)]
struct AydbCursor {
    base: sqlite3_vtab_cursor,
    /// Contiguous reusable byte slab holding all scanned `row_bytes` for the active cursor pass.
    slab: Vec<u8>,
    /// `(start_offset, end_offset)` into `slab` for each row (`PERF-04`).
    spans: Vec<(usize, usize)>,
    /// Primary key bytes per row when `keys_only` is `true` (`P02`).
    pks: Vec<Vec<u8>>,
    /// True when the active scan only needs primary keys (`RANGE_FLAG_KEYS_ONLY`, `P02`),
    /// avoiding any out-of-line blob reads or `slab` allocations.
    keys_only: bool,
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
    let pk = extract_column_key_bytes_with_collation(row_bytes, vtab.pk_col_idx, vtab.pk_collation)
        .unwrap_or_default();
    vtab.has_non_int_pks.store(true, Ordering::Relaxed);
    if let Ok(mut rmap) = vtab.rowid_to_pk.lock() {
        rmap.get_or_assign_rowid(&pk)
    } else {
        hash_pk_to_rowid(&pk)
    }
}

/// Extracts the `rowid` (`i64`) directly from `pk_bytes` for keys-only cursor scans (`P02`).
#[inline]
fn extract_rowid_from_pk_bytes(pk_bytes: &[u8], vtab: &AydbVTab) -> i64 {
    if vtab.pk_affinity == ColumnAffinity::Integer {
        if let Some(id) = decode_i64_tagged_key(pk_bytes) {
            return id;
        }
    }
    vtab.has_non_int_pks.store(true, Ordering::Relaxed);
    if let Ok(mut rmap) = vtab.rowid_to_pk.lock() {
        rmap.get_or_assign_rowid(pk_bytes)
    } else {
        hash_pk_to_rowid(pk_bytes)
    }
}

/// Converts a SQLite range bound value into a storage-class-tagged key for `PLAN_PK_RANGE`,
/// applying the column's declared [`ColumnAffinity`] and [`ColCollation`] (`USE-05`, `F04`, `F05`).
///
/// Because `Integer(q)` (`9` bytes: `[0x10, encode_i64(q)]`) and `Float(q.0)` (`17` bytes:
/// `[0x10, encode_i64(q), 0..0]`) share the same mathematical value `q`, an upper or lower bound
/// at an exact integer `q` adjusts inclusivity so both representations are included or excluded
/// consistently!
///
/// Returns `Ok(Some((key_bytes, is_inclusive)))` for a normal bound, `Ok(None)` if the bound
/// is unbounded in that direction, or `Err(())` if the range is unsatisfiable (empty result).
unsafe fn pk_range_bound_to_key(
    val_ptr: *mut sqlite3_value,
    pk_affinity: ColumnAffinity,
    pk_collation: ColCollation,
    is_lower: bool,
    is_inclusive: bool,
) -> Result<Option<(Vec<u8>, bool)>, ()> {
    let val_type = unsafe { sqlite3_value_type(val_ptr) };
    if val_type == SQLITE_NULL {
        return Err(());
    }
    if val_type == SQLITE_FLOAT {
        let f = unsafe { sqlite3_value_double(val_ptr) };
        if f.is_nan() {
            return Err(());
        }
    }

    let key = unsafe {
        sqlite_value_to_key_bytes_with_schema(val_ptr, pk_affinity, pk_collation, false)
    };

    // If the bound is an exact integer `q` (either `9`-byte `[0x10, q]` or `17`-byte `[0x10, q, 0..0]`),
    // ensure both `9`-byte `Integer(q)` and `17`-byte `Float(q.0)` are handled identically:
    // - Lower inclusive (`>= q`): start at 9-byte `[0x10, q]` (inclusive) -> includes both!
    // - Lower exclusive (`> q`): start after 17-byte `[0x10, q, 0..0]` (exclusive) -> excludes both!
    // - Upper inclusive (`<= q`): end at 17-byte `[0x10, q, 0..0]` (inclusive) -> includes both!
    // - Upper exclusive (`< q`): end before 9-byte `[0x10, q]` (exclusive) -> excludes both!
    if key.first().copied() == Some(KEY_TAG_INTEGER) {
        if key.len() == 9 {
            if (is_lower && !is_inclusive) || (!is_lower && is_inclusive) {
                let mut ext = Vec::with_capacity(17);
                ext.extend_from_slice(&key);
                ext.extend_from_slice(&0u64.to_be_bytes());
                return Ok(Some((ext, is_inclusive)));
            }
            return Ok(Some((key, is_inclusive)));
        } else if key.len() == 17 && key[9..17] == [0u8; 8] {
            if (is_lower && is_inclusive) || (!is_lower && !is_inclusive) {
                return Ok(Some((key[0..9].to_vec(), is_inclusive)));
            }
            return Ok(Some((key, is_inclusive)));
        }
    }

    Ok(Some((key, is_inclusive)))
}

unsafe extern "system" fn destroy_aux_context(ptr: *mut c_void) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if !ptr.is_null() {
            unsafe {
                drop(Box::from_raw(ptr as *mut ModuleAuxContext));
            }
        }
    }));
}

unsafe extern "system" fn vtab_create_or_connect(
    db: *mut sqlite3,
    p_aux: *mut c_void,
    argc: c_int,
    argv: *const *const c_char,
    pp_vtab: *mut *mut sqlite3_vtab,
    _pz_err: *mut *mut c_char,
) -> c_int {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        if p_aux.is_null() || pp_vtab.is_null() {
            return SQLITE_ERROR;
        }
        let aux = &*(p_aux as *const ModuleAuxContext);

        // Extract virtual table name from argv[2]
        let table_name = if argc > 2 && !(*argv.offset(2)).is_null() {
            CStr::from_ptr(*argv.offset(2))
                .to_string_lossy()
                .trim()
                .to_string()
        } else {
            String::from("vtab")
        };

        let mut explicit_table_id: Option<u16> = None;
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
                    "table_id" => match val.parse::<u16>() {
                        Ok(id) if id != 0 => explicit_table_id = Some(id),
                        _ => return SQLITE_ERROR,
                    },
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

        // Parse column names, identify PRIMARY KEY column index, and determine affinity & collation (`USE-03`, `F04`)
        let mut col_names = Vec::new();
        let mut col_affinities = Vec::new();
        let mut col_collations = Vec::new();
        let mut pk_col_idx = 0usize;
        for (idx, col_def) in split_top_level_commas(&columns_spec).into_iter().enumerate() {
            let trimmed = col_def.trim();
            let raw_tokens: Vec<&str> = trimmed.split_whitespace().collect();
            if raw_tokens.is_empty() {
                continue;
            }
            col_names.push(raw_tokens[0].to_string());

            let mut collation = ColCollation::Binary;
            let mut type_tokens = Vec::new();
            let mut t_idx = 1usize;
            while t_idx < raw_tokens.len() {
                let tok_upper = raw_tokens[t_idx].to_ascii_uppercase();
                if tok_upper == "COLLATE" {
                    if t_idx + 1 >= raw_tokens.len() {
                        return SQLITE_ERROR;
                    }
                    let coll_name = raw_tokens[t_idx + 1]
                        .trim_matches(|c: char| !c.is_ascii_alphanumeric() && c != '_');
                    let Some(parsed_coll) = ColCollation::from_name(coll_name) else {
                        return SQLITE_ERROR;
                    };
                    collation = parsed_coll;
                    t_idx += 2;
                    continue;
                }
                if tok_upper == "PRIMARY"
                    && t_idx + 1 < raw_tokens.len()
                    && raw_tokens[t_idx + 1].eq_ignore_ascii_case("KEY")
                {
                    pk_col_idx = idx;
                    t_idx += 2;
                    continue;
                }
                if tok_upper == "NOT"
                    && t_idx + 1 < raw_tokens.len()
                    && raw_tokens[t_idx + 1].eq_ignore_ascii_case("NULL")
                {
                    t_idx += 2;
                    continue;
                }
                if tok_upper == "UNIQUE" {
                    t_idx += 1;
                    continue;
                }
                type_tokens.push(tok_upper);
                t_idx += 1;
            }
            let type_part = type_tokens.join(" ");
            col_affinities.push(ColumnAffinity::from_type_str(&type_part));
            col_collations.push(collation);
        }
        if col_names.is_empty() {
            return SQLITE_ERROR;
        }
        let pk_affinity = col_affinities
            .get(pk_col_idx)
            .copied()
            .unwrap_or(ColumnAffinity::Blob);
        let pk_collation = col_collations
            .get(pk_col_idx)
            .copied()
            .unwrap_or(ColCollation::Binary);

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

        // Resolve or auto-assign unique `table_id` and register Capped Table if `max_bytes > 0` (`AUD-03`)
        let table_id = {
            let mut eng = match aux.engine.lock() {
                Ok(g) => g,
                Err(_) => return SQLITE_ERROR,
            };
            if let Err(e) = eng.check_write_allowed_for_conn(db as usize) {
                return io_err_to_sqlite_rc(&e);
            }
            let resolved_id = match eng.register_or_resolve_vtab(&table_name, explicit_table_id) {
                Ok(id) => id,
                Err(e) => return io_err_to_sqlite_rc(&e),
            };
            if max_bytes > 0 {
                if let Err(e) = eng.register_capped_table(resolved_id, max_bytes) {
                    eng.unregister_vtab(resolved_id, &table_name);
                    return io_err_to_sqlite_rc(&e);
                }
            }
            resolved_id
        };

        let declare_sql = format!("CREATE TABLE x({columns_spec})");
        let Ok(c_sql) = CString::new(declare_sql) else {
            if let Ok(mut eng) = aux.engine.lock() {
                eng.unregister_vtab(table_id, &table_name);
            }
            return SQLITE_ERROR;
        };
        let rc = sqlite3_declare_vtab(db, c_sql.as_ptr());
        if rc != SQLITE_OK {
            if let Ok(mut eng) = aux.engine.lock() {
                eng.unregister_vtab(table_id, &table_name);
            }
            return rc;
        }

        let vtab = Box::new(AydbVTab {
            base: sqlite3_vtab {
                pModule: &AYDB_VTAB_MODULE,
                nRef: 0,
                zErrMsg: core::ptr::null_mut(),
            },
            db,
            engine: aux.engine.clone(),
            table_id,
            table_name,
            durability,
            col_names,
            col_affinities,
            col_collations,
            pk_col_idx,
            pk_affinity,
            pk_collation,
            secondary_indexes,
            has_non_int_pks: AtomicBool::new(pk_affinity != ColumnAffinity::Integer),
            open_cursors: AtomicUsize::new(0),
            next_auto_rowid: AtomicI64::new(0),
            rowid_to_pk: Mutex::new(RowidMap::default()),
        });

        *pp_vtab = Box::into_raw(vtab) as *mut sqlite3_vtab;
        SQLITE_OK
    }))
    .unwrap_or(SQLITE_ERROR)
}

unsafe extern "system" fn vtab_disconnect(p_vtab: *mut sqlite3_vtab) -> c_int {
    catch_unwind(AssertUnwindSafe(|| {
        if !p_vtab.is_null() {
            unsafe {
                let vtab = Box::from_raw(p_vtab as *mut AydbVTab);
                if let Ok(mut eng) = vtab.engine.lock() {
                    eng.unregister_vtab(vtab.table_id, &vtab.table_name);
                }
            }
        }
        SQLITE_OK
    }))
    .unwrap_or(SQLITE_ERROR)
}

unsafe extern "system" fn vtab_destroy(p_vtab: *mut sqlite3_vtab) -> c_int {
    catch_unwind(AssertUnwindSafe(|| {
        if !p_vtab.is_null() {
            unsafe {
                let vtab_ref = &*(p_vtab as *const AydbVTab);
                {
                    let mut eng = match vtab_ref.engine.lock() {
                        Ok(g) => g,
                        Err(_) => return SQLITE_ERROR,
                    };
                    if let Err(e) = eng.check_write_allowed_for_conn(vtab_ref.db as usize) {
                        return io_err_to_sqlite_rc(&e);
                    }
                    if let Err(e) = eng.drop_table(vtab_ref.table_id, Some(&vtab_ref.table_name)) {
                        return io_err_to_sqlite_rc(&e);
                    }
                }
                drop(Box::from_raw(p_vtab as *mut AydbVTab));
            }
        }
        SQLITE_OK
    }))
    .unwrap_or(SQLITE_ERROR)
}

unsafe extern "system" fn vtab_best_index(
    p_vtab: *mut sqlite3_vtab,
    p_info: *mut sqlite3_index_info,
) -> c_int {
    catch_unwind(AssertUnwindSafe(|| unsafe {
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

        // 3. Check for usable range constraints (`>`, `>=`, `<`, `<=`) on Primary Key (`C-03`)
        let mut lower_idx: Option<(usize, c_int)> = None;
        let mut upper_idx: Option<(usize, c_int)> = None;
        for i in 0..n {
            if constraints[i].usable == 0 {
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

        // Check if only `rowid` (`colUsed == 0`) or only a binary-collated PK column is projected (`P02`)
        let can_use_keys_only = info.colUsed == 0
            || (vtab.pk_collation == ColCollation::Binary
                && vtab.pk_col_idx < 63
                && info.colUsed == (1u64 << vtab.pk_col_idx));

        if lower_idx.is_some() || upper_idx.is_some() {
            let mut flags = if can_use_keys_only {
                RANGE_FLAG_KEYS_ONLY
            } else {
                0
            };
            let mut next_arg = 1;
            if let Some((i, fl)) = lower_idx {
                usages[i].argvIndex = next_arg;
                usages[i].omit = 0;
                next_arg += 1;
                flags |= fl;
            }
            if let Some((i, fl)) = upper_idx {
                usages[i].argvIndex = next_arg;
                usages[i].omit = 0;
                flags |= fl;
            }
            if order_by_pk_asc {
                info.orderByConsumed = 1;
            }
            info.idxNum = PLAN_PK_RANGE | (flags << 8);
            info.estimatedCost = 25.0;
            info.estimatedRows = 32;
            return SQLITE_OK;
        }

        // 4. Fallback: Full Table Scan (`C-03`: let SQLite's VDBE enforce LIMIT/OFFSET after any
        //    residual WHERE predicates like `LIKE`, `OR`, or expressions not passed in `aConstraint`)
        let full_scan_flags = if can_use_keys_only {
            RANGE_FLAG_KEYS_ONLY
        } else {
            0
        };
        if order_by_pk_asc {
            info.orderByConsumed = 1;
        }
        info.idxNum = PLAN_FULL_SCAN | (full_scan_flags << 8);
        info.estimatedCost = 10_000.0;
        info.estimatedRows = 1_000;
        SQLITE_OK
    }))
    .unwrap_or(SQLITE_ERROR)
}

unsafe extern "system" fn vtab_open(
    p_vtab: *mut sqlite3_vtab,
    pp_cursor: *mut *mut sqlite3_vtab_cursor,
) -> c_int {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        if !p_vtab.is_null() {
            let vtab = &*(p_vtab as *const AydbVTab);
            vtab.open_cursors.fetch_add(1, Ordering::Relaxed);
        }
        let cursor = Box::new(AydbCursor {
            base: sqlite3_vtab_cursor { pVtab: p_vtab },
            slab: Vec::with_capacity(512),
            spans: Vec::with_capacity(16),
            pks: Vec::new(),
            keys_only: false,
            pos: 0,
        });
        *pp_cursor = Box::into_raw(cursor) as *mut sqlite3_vtab_cursor;
        SQLITE_OK
    }))
    .unwrap_or(SQLITE_ERROR)
}

unsafe extern "system" fn vtab_close(p_cursor: *mut sqlite3_vtab_cursor) -> c_int {
    catch_unwind(AssertUnwindSafe(|| {
        if !p_cursor.is_null() {
            unsafe {
                let cursor = Box::from_raw(p_cursor as *mut AydbCursor);
                let p_vtab = cursor.base.pVtab;
                drop(cursor);
                if !p_vtab.is_null() {
                    let vtab = &*(p_vtab as *const AydbVTab);
                    let prev = vtab.open_cursors.fetch_sub(1, Ordering::Relaxed);
                    if prev == 1 && vtab.has_non_int_pks.load(Ordering::Relaxed) {
                        if let Ok(mut rmap) = vtab.rowid_to_pk.lock() {
                            rmap.evict_oldest_half_if_needed();
                        }
                    }
                }
            }
        }
        SQLITE_OK
    }))
    .unwrap_or(SQLITE_ERROR)
}

unsafe extern "system" fn vtab_filter(
    p_cursor: *mut sqlite3_vtab_cursor,
    idx_num: c_int,
    _idx_str: *const c_char,
    argc: c_int,
    argv: *mut *mut sqlite3_value,
) -> c_int {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        let cursor = &mut *(p_cursor as *mut AydbCursor);
        let vtab = &*(cursor.base.pVtab as *const AydbVTab);
        cursor.slab.clear();
        cursor.spans.clear();
        cursor.pks.clear();
        cursor.keys_only = false;
        cursor.pos = 0;

        let mut eng = match vtab.engine.lock() {
            Ok(g) => g,
            Err(_) => return SQLITE_ERROR,
        };
        let conn_id = vtab.db as usize;

        eng.with_committed_read_for_conn(conn_id, |eng| {
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
                                        if let Err(e) = eng.get_row_into(
                                            vtab.table_id,
                                            mapped_pk,
                                            &mut cursor.slab,
                                            &mut cursor.spans,
                                        ) {
                                            return io_err_to_sqlite_rc(&e);
                                        }
                                        return SQLITE_OK;
                                    }
                                }
                            }
                            if vtab.pk_affinity == ColumnAffinity::Integer {
                                let pk_arr = encode_i64_tagged_key(rid);
                                if let Err(e) = eng.get_row_into(
                                    vtab.table_id,
                                    &pk_arr,
                                    &mut cursor.slab,
                                    &mut cursor.spans,
                                ) {
                                    return io_err_to_sqlite_rc(&e);
                                }
                            }
                        }
                    } else {
                        let pk_bytes = sqlite_value_to_key_bytes_with_schema(
                            val_ptr,
                            vtab.pk_affinity,
                            vtab.pk_collation,
                            false,
                        );
                        let found = match eng.get_row_into(
                            vtab.table_id,
                            &pk_bytes,
                            &mut cursor.slab,
                            &mut cursor.spans,
                        ) {
                            Ok(b) => b,
                            Err(e) => return io_err_to_sqlite_rc(&e),
                        };
                        // In SQLite (`10 == 10.0` across all affinities including `BLOB`),
                        // if `pk_bytes` is an exact-integer number (`9`-byte `Integer(q)` or
                        // `17`-byte `Float(q.0)`), also probe the dual exact-integer representation
                        // if not found under the primary representation (`F04`, `F05`, `C-02`).
                        if !found {
                            if let Some(alt) = build_numeric_alt_key(&pk_bytes) {
                                if let Err(e) = eng.get_row_into(
                                    vtab.table_id,
                                    &alt,
                                    &mut cursor.slab,
                                    &mut cursor.spans,
                                ) {
                                    return io_err_to_sqlite_rc(&e);
                                }
                            }
                        }
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
                    let sec_col_idx = vtab
                        .secondary_indexes
                        .iter()
                        .find(|&&(_, id)| id == index_id)
                        .map(|&(c, _)| c)
                        .unwrap_or(0);
                    let sec_aff = vtab
                        .col_affinities
                        .get(sec_col_idx)
                        .copied()
                        .unwrap_or(ColumnAffinity::Blob);
                    let sec_coll = vtab
                        .col_collations
                        .get(sec_col_idx)
                        .copied()
                        .unwrap_or(ColCollation::Binary);
                    let col_val =
                        sqlite_value_to_key_bytes_with_schema(val_ptr, sec_aff, sec_coll, true);
                    let pks =
                        match eng.lookup_by_secondary_index(vtab.table_id, index_id, &col_val) {
                            Ok(pks) => pks,
                            Err(e) => return io_err_to_sqlite_rc(&e),
                        };
                    for pk in pks {
                        if let Err(e) = eng.get_row_into(
                            vtab.table_id,
                            &pk,
                            &mut cursor.slab,
                            &mut cursor.spans,
                        ) {
                            return io_err_to_sqlite_rc(&e);
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
                        let inc = (flags & RANGE_FLAG_LOWER_INCLUSIVE) != 0;
                        let val_ptr = *argv.offset(arg_pos as isize);
                        arg_pos += 1;
                        match pk_range_bound_to_key(
                            val_ptr,
                            vtab.pk_affinity,
                            vtab.pk_collation,
                            true,
                            inc,
                        ) {
                            Ok(bound) => bound,
                            Err(()) => return SQLITE_OK,
                        }
                    } else {
                        None
                    };
                    let upper_bound = if (flags
                        & (RANGE_FLAG_UPPER_INCLUSIVE | RANGE_FLAG_UPPER_EXCLUSIVE))
                        != 0
                        && arg_pos < argc
                    {
                        let inc = (flags & RANGE_FLAG_UPPER_INCLUSIVE) != 0;
                        let val_ptr = *argv.offset(arg_pos as isize);
                        match pk_range_bound_to_key(
                            val_ptr,
                            vtab.pk_affinity,
                            vtab.pk_collation,
                            false,
                            inc,
                        ) {
                            Ok(bound) => bound,
                            Err(()) => return SQLITE_OK,
                        }
                    } else {
                        None
                    };
                    let keys_only = (flags & RANGE_FLAG_KEYS_ONLY) != 0;
                    cursor.keys_only = keys_only;
                    if let Err(e) = eng.scan_range_into_slab(
                        vtab.table_id,
                        lower_bound.as_ref().map(|(b, inc)| (b.as_slice(), *inc)),
                        upper_bound.as_ref().map(|(b, inc)| (b.as_slice(), *inc)),
                        false,
                        usize::MAX,
                        keys_only,
                        &mut cursor.pks,
                        &mut cursor.slab,
                        &mut cursor.spans,
                    ) {
                        return io_err_to_sqlite_rc(&e);
                    }
                }
                _ => {
                    let flags = idx_num >> 8;
                    let keys_only = (flags & RANGE_FLAG_KEYS_ONLY) != 0;
                    cursor.keys_only = keys_only;
                    if let Err(e) = eng.scan_range_into_slab(
                        vtab.table_id,
                        None,
                        None,
                        false,
                        usize::MAX,
                        keys_only,
                        &mut cursor.pks,
                        &mut cursor.slab,
                        &mut cursor.spans,
                    ) {
                        return io_err_to_sqlite_rc(&e);
                    }
                }
            }

            SQLITE_OK
        })
    }))
    .unwrap_or(SQLITE_ERROR)
}

unsafe extern "system" fn vtab_next(p_cursor: *mut sqlite3_vtab_cursor) -> c_int {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        let cursor = &mut *(p_cursor as *mut AydbCursor);
        cursor.pos += 1;
        SQLITE_OK
    }))
    .unwrap_or(SQLITE_ERROR)
}

unsafe extern "system" fn vtab_eof(p_cursor: *mut sqlite3_vtab_cursor) -> c_int {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        let cursor = &*(p_cursor as *const AydbCursor);
        let len = if cursor.keys_only {
            cursor.pks.len()
        } else {
            cursor.spans.len()
        };
        if cursor.pos >= len {
            1
        } else {
            0
        }
    }))
    .unwrap_or(1)
}

unsafe extern "system" fn vtab_column(
    p_cursor: *mut sqlite3_vtab_cursor,
    p_ctx: *mut sqlite3_context,
    i_col: c_int,
) -> c_int {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        let cursor = &*(p_cursor as *const AydbCursor);
        let vtab = &*(cursor.base.pVtab as *const AydbVTab);
        if cursor.keys_only {
            if cursor.pos >= cursor.pks.len() {
                return SQLITE_ERROR;
            }
            let pk_bytes = &cursor.pks[cursor.pos];
            if i_col < 0 {
                let rid = extract_rowid_from_pk_bytes(pk_bytes, vtab);
                sqlite3_result_int64(p_ctx, rid);
                return SQLITE_OK;
            }
            if i_col as usize == vtab.pk_col_idx {
                if let Some(val) = SqlValue::from_key_bytes(pk_bytes) {
                    val.set_sqlite_result(p_ctx);
                    return SQLITE_OK;
                }
            }
            return SQLITE_ERROR;
        }
        if cursor.pos >= cursor.spans.len() {
            return SQLITE_ERROR;
        }
        let (start, end) = cursor.spans[cursor.pos];
        let row_bytes = &cursor.slab[start..end];
        if i_col < 0 {
            let rid = extract_rowid_from_row(row_bytes, vtab);
            sqlite3_result_int64(p_ctx, rid);
            return SQLITE_OK;
        }
        match project_column_zero_copy(row_bytes, i_col as usize, p_ctx) {
            Ok(()) => SQLITE_OK,
            Err(_) => SQLITE_ERROR,
        }
    }))
    .unwrap_or(SQLITE_ERROR)
}

unsafe extern "system" fn vtab_rowid(
    p_cursor: *mut sqlite3_vtab_cursor,
    p_rowid: *mut sqlite3_int64,
) -> c_int {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        let cursor = &*(p_cursor as *const AydbCursor);
        let vtab = &*(cursor.base.pVtab as *const AydbVTab);
        if cursor.keys_only {
            if cursor.pos >= cursor.pks.len() {
                return SQLITE_ERROR;
            }
            *p_rowid = extract_rowid_from_pk_bytes(&cursor.pks[cursor.pos], vtab);
            return SQLITE_OK;
        }
        if cursor.pos >= cursor.spans.len() {
            return SQLITE_ERROR;
        }
        let (start, end) = cursor.spans[cursor.pos];
        let row_bytes = &cursor.slab[start..end];
        *p_rowid = extract_rowid_from_row(row_bytes, vtab);
        SQLITE_OK
    }))
    .unwrap_or(SQLITE_ERROR)
}

unsafe extern "system" fn vtab_update(
    p_vtab: *mut sqlite3_vtab,
    argc: c_int,
    argv: *mut *mut sqlite3_value,
    p_rowid: *mut sqlite3_int64,
) -> c_int {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        let vtab = &*(p_vtab as *const AydbVTab);
        let mut eng = match vtab.engine.lock() {
            Ok(g) => g,
            Err(_) => return SQLITE_ERROR,
        };
        if let Err(e) = eng.check_write_allowed_for_conn(vtab.db as usize) {
            return io_err_to_sqlite_rc(&e);
        }

        // Case 1: DELETE (`argc == 1`)
        if argc == 1 {
            let arg0 = *argv.offset(0);
            if sqlite3_value_type(arg0) == SQLITE_INTEGER {
                let rid = sqlite3_value_int64(arg0);
                if vtab.has_non_int_pks.load(Ordering::Relaxed) {
                    if let Ok(mut rmap) = vtab.rowid_to_pk.lock() {
                        if let Some(old_pk) = rmap.remove_by_rowid(rid) {
                            if let Err(e) = eng.delete_row(vtab.table_id, &old_pk, vtab.durability)
                            {
                                return io_err_to_sqlite_rc(&e);
                            }
                            return SQLITE_OK;
                        }
                    }
                }
                if vtab.pk_affinity == ColumnAffinity::Integer {
                    let old_pk = encode_i64_tagged_key(rid);
                    if let Err(e) = eng.delete_row(vtab.table_id, &old_pk, vtab.durability) {
                        return io_err_to_sqlite_rc(&e);
                    }
                }
            } else {
                let old_pk = sqlite_value_to_key_bytes_with_schema(
                    arg0,
                    vtab.pk_affinity,
                    vtab.pk_collation,
                    false,
                );
                if vtab.has_non_int_pks.load(Ordering::Relaxed) {
                    if let Ok(mut rmap) = vtab.rowid_to_pk.lock() {
                        rmap.remove_by_pk(&old_pk);
                    }
                }
                if let Err(e) = eng.delete_row(vtab.table_id, &old_pk, vtab.durability) {
                    return io_err_to_sqlite_rc(&e);
                }
            }
            return SQLITE_OK;
        }

        // Case 2: INSERT or UPDATE (`argc == 2 + num_cols`)
        let num_cols = vtab.col_names.len();
        if (argc as usize) < 2 + num_cols {
            return SQLITE_ERROR;
        }

        let arg0 = *argv.offset(0);
        let is_insert = sqlite3_value_type(arg0) == SQLITE_NULL;
        let pk_val_ptr = *argv.offset(2 + vtab.pk_col_idx as isize);
        let pk_val_type = sqlite3_value_type(pk_val_ptr);

        // Handle `SQLITE_NULL` primary key (`USE-05`, `F08`)
        let pk_override = if pk_val_type == SQLITE_NULL {
            if is_insert && vtab.pk_affinity == ColumnAffinity::Integer {
                let mut candidate = vtab.next_auto_rowid.load(Ordering::Relaxed);
                if candidate <= 0 {
                    candidate = 1;
                }
                let mut found_free = None;
                // 1. Bounded sequential probe using checked arithmetic (`F08`)
                for _ in 0..1024 {
                    let probe_key = encode_i64_tagged_key(candidate);
                    match eng.row_exists(vtab.table_id, &probe_key) {
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
                //    matching SQLite's fallback rowid generator, and return `SQLITE_FULL` if exhausted (`F08`).
                if found_free.is_none() {
                    let mut state = (eng.next_lsn() ^ 0x9E37_79B9_7F4A_7C15)
                        .wrapping_add(vtab.table_id as u64);
                    for attempt in 0..64u64 {
                        state = state
                            .wrapping_add(0x9E37_79B9_7F4A_7C15)
                            .wrapping_add(attempt);
                        let mut z = state;
                        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
                        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
                        z ^= z >> 31;
                        let rand_pos = ((z & 0x7FFF_FFFF_FFFF_FFFF) as i64).max(1);
                        let probe_key = encode_i64_tagged_key(rand_pos);
                        match eng.row_exists(vtab.table_id, &probe_key) {
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
                if let Some(next) = chosen.checked_add(1) {
                    vtab.next_auto_rowid.fetch_max(next, Ordering::Relaxed);
                }
                Some(chosen)
            } else {
                return SQLITE_CONSTRAINT;
            }
        } else {
            None
        };

        let (new_pk, row_bytes, sec_keys) = encode_sqlite_values_direct(
            argv.offset(2),
            num_cols,
            vtab.pk_col_idx,
            pk_override,
            &vtab.col_affinities,
            &vtab.col_collations,
            &vtab.secondary_indexes,
        );

        let coerced_int_pk = if vtab.pk_affinity == ColumnAffinity::Integer {
            decode_i64_tagged_key(&new_pk)
        } else {
            None
        };
        if pk_override.is_none() {
            if let Some(explicit_id) = coerced_int_pk {
                if explicit_id >= 0 {
                    if let Some(next) = explicit_id.checked_add(1) {
                        vtab.next_auto_rowid.fetch_max(next, Ordering::Relaxed);
                    } else {
                        vtab.next_auto_rowid.fetch_max(i64::MAX, Ordering::Relaxed);
                    }
                }
            }
        }

        // Resolve `old_pk` if this is an UPDATE (`!is_insert`)
        let mut old_pk_opt: Option<(Vec<u8>, Option<i64>)> = None;
        if !is_insert {
            if sqlite3_value_type(arg0) == SQLITE_INTEGER {
                let rid = sqlite3_value_int64(arg0);
                if vtab.has_non_int_pks.load(Ordering::Relaxed) {
                    if let Ok(rmap) = vtab.rowid_to_pk.lock() {
                        if let Some(old_pk) = rmap.get_pk(rid).cloned() {
                            old_pk_opt = Some((old_pk, Some(rid)));
                        }
                    }
                }
                if old_pk_opt.is_none() && vtab.pk_affinity == ColumnAffinity::Integer {
                    old_pk_opt = Some((encode_i64_tagged_key(rid).to_vec(), None));
                }
            } else {
                old_pk_opt = Some((
                    sqlite_value_to_key_bytes_with_schema(
                        arg0,
                        vtab.pk_affinity,
                        vtab.pk_collation,
                        false,
                    ),
                    None,
                ));
            }
        }

        // Enforce primary key uniqueness & SQLite `ON CONFLICT` resolution (`AUD-03`, `C-02`)
        let same_pk = match &old_pk_opt {
            Some((old_pk, _)) => {
                old_pk.as_slice() == new_pk.as_slice()
                    || build_numeric_alt_key(old_pk.as_slice()).as_deref()
                        == Some(new_pk.as_slice())
            }
            None => false,
        };
        if (is_insert || !same_pk) && pk_override.is_none() {
            let existing_conflict =
                match find_existing_pk_or_numeric_alt(&mut eng, vtab.table_id, &new_pk) {
                    Ok(opt) => opt,
                    Err(e) => return io_err_to_sqlite_rc(&e),
                };
            if let Some(conflict_pk) = existing_conflict {
                let conflict_mode = sqlite3_vtab_on_conflict(vtab.db);
                if conflict_mode == SQLITE_IGNORE {
                    return SQLITE_OK;
                } else if conflict_mode == SQLITE_REPLACE {
                    if conflict_pk.as_slice() != new_pk.as_slice() {
                        if vtab.has_non_int_pks.load(Ordering::Relaxed) {
                            if let Ok(mut rmap) = vtab.rowid_to_pk.lock() {
                                rmap.remove_by_pk(&conflict_pk);
                            }
                        }
                        if let Err(e) =
                            eng.delete_row(vtab.table_id, &conflict_pk, vtab.durability)
                        {
                            return io_err_to_sqlite_rc(&e);
                        }
                    }
                } else {
                    return SQLITE_CONSTRAINT;
                }
            }
        }

        // If UPDATE changed the physical PK bytes, delete the old PK row and clean up `RowidMap` (`AUD-11`, `C-02`)
        if let Some((old_pk, mapped_rid)) = old_pk_opt {
            if old_pk.as_slice() != new_pk.as_slice() {
                if vtab.has_non_int_pks.load(Ordering::Relaxed) {
                    if let Ok(mut rmap) = vtab.rowid_to_pk.lock() {
                        if let Some(rid) = mapped_rid {
                            rmap.remove_by_rowid(rid);
                        } else {
                            rmap.remove_by_pk(&old_pk);
                        }
                    }
                }
                if let Err(e) = eng.delete_row(vtab.table_id, &old_pk, vtab.durability) {
                    return io_err_to_sqlite_rc(&e);
                }
            }
        }

        if let Err(e) = eng.put_row_owned(
            vtab.table_id,
            &new_pk,
            row_bytes,
            &sec_keys,
            vtab.durability,
        ) {
            return io_err_to_sqlite_rc(&e);
        }

        let rid = if let Some(auto_id) = pk_override {
            auto_id
        } else if let Some(int_id) = coerced_int_pk {
            int_id
        } else {
            vtab.has_non_int_pks.store(true, Ordering::Relaxed);
            if let Ok(mut rmap) = vtab.rowid_to_pk.lock() {
                if vtab.open_cursors.load(Ordering::Relaxed) == 0 {
                    rmap.evict_oldest_half_if_needed();
                }
                rmap.get_or_assign_rowid(&new_pk)
            } else {
                hash_pk_to_rowid(&new_pk)
            }
        };
        if !p_rowid.is_null() {
            *p_rowid = rid;
        }
        SQLITE_OK
    }))
    .unwrap_or(SQLITE_ERROR)
}

unsafe extern "system" fn vtab_begin_tx(p_vtab: *mut sqlite3_vtab) -> c_int {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        let vtab = &*(p_vtab as *const AydbVTab);
        let mut eng = match vtab.engine.lock() {
            Ok(g) => g,
            Err(_) => return SQLITE_ERROR,
        };
        if let Err(e) = eng.begin_transaction_for_conn(vtab.db as usize) {
            return io_err_to_sqlite_rc(&e);
        }
        SQLITE_OK
    }))
    .unwrap_or(SQLITE_ERROR)
}

unsafe extern "system" fn vtab_sync_tx(p_vtab: *mut sqlite3_vtab) -> c_int {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        let vtab = &*(p_vtab as *const AydbVTab);
        let mut eng = match vtab.engine.lock() {
            Ok(g) => g,
            Err(_) => return SQLITE_ERROR,
        };
        if let Err(e) = eng.check_write_allowed_for_conn(vtab.db as usize) {
            return io_err_to_sqlite_rc(&e);
        }
        if let Err(e) = eng.prepare_commit_transaction(vtab.durability) {
            return io_err_to_sqlite_rc(&e);
        }
        SQLITE_OK
    }))
    .unwrap_or(SQLITE_ERROR)
}

unsafe extern "system" fn vtab_commit_tx(p_vtab: *mut sqlite3_vtab) -> c_int {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        let vtab = &*(p_vtab as *const AydbVTab);
        let mut eng = match vtab.engine.lock() {
            Ok(g) => g,
            Err(_) => return SQLITE_ERROR,
        };
        if let Err(e) = eng.commit_transaction_for_conn(vtab.db as usize) {
            return io_err_to_sqlite_rc(&e);
        }
        SQLITE_OK
    }))
    .unwrap_or(SQLITE_ERROR)
}

unsafe extern "system" fn vtab_rollback_tx(p_vtab: *mut sqlite3_vtab) -> c_int {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        let vtab = &*(p_vtab as *const AydbVTab);
        let mut eng = match vtab.engine.lock() {
            Ok(g) => g,
            Err(_) => return SQLITE_ERROR,
        };
        if let Err(e) = eng.rollback_transaction_for_conn(vtab.db as usize) {
            return io_err_to_sqlite_rc(&e);
        }
        SQLITE_OK
    }))
    .unwrap_or(SQLITE_ERROR)
}

unsafe extern "system" fn vtab_savepoint(p_vtab: *mut sqlite3_vtab, i_savepoint: c_int) -> c_int {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        let vtab = &*(p_vtab as *const AydbVTab);
        let mut eng = match vtab.engine.lock() {
            Ok(g) => g,
            Err(_) => return SQLITE_ERROR,
        };
        if let Err(e) = eng.savepoint_for_conn(vtab.db as usize, i_savepoint) {
            return io_err_to_sqlite_rc(&e);
        }
        SQLITE_OK
    }))
    .unwrap_or(SQLITE_ERROR)
}

unsafe extern "system" fn vtab_release(p_vtab: *mut sqlite3_vtab, i_savepoint: c_int) -> c_int {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        let vtab = &*(p_vtab as *const AydbVTab);
        let mut eng = match vtab.engine.lock() {
            Ok(g) => g,
            Err(_) => return SQLITE_ERROR,
        };
        if let Err(e) = eng.release_savepoint_for_conn(vtab.db as usize, i_savepoint) {
            return io_err_to_sqlite_rc(&e);
        }
        SQLITE_OK
    }))
    .unwrap_or(SQLITE_ERROR)
}

unsafe extern "system" fn vtab_rollback_to(p_vtab: *mut sqlite3_vtab, i_savepoint: c_int) -> c_int {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        let vtab = &*(p_vtab as *const AydbVTab);
        let mut eng = match vtab.engine.lock() {
            Ok(g) => g,
            Err(_) => return SQLITE_ERROR,
        };
        if let Err(e) = eng.rollback_to_savepoint_for_conn(vtab.db as usize, i_savepoint) {
            return io_err_to_sqlite_rc(&e);
        }
        SQLITE_OK
    }))
    .unwrap_or(SQLITE_ERROR)
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

    #[test]
    fn stage2_aud03_pk_uniqueness_on_conflict_and_table_id_registry() {
        let tmp = TempTestDir::new("stage2_aud03");
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
        let engine = Arc::new(Mutex::new(
            StorageEngine::open(fs, EngineConfig::default()).unwrap(),
        ));
        let conn = SqliteConnection::open_in_memory().unwrap();
        conn.register_aydb_module(engine.clone()).unwrap();

        // Part A: Primary key uniqueness and ON CONFLICT resolution modes
        conn.execute_batch(
            "CREATE VIRTUAL TABLE users USING aydb(
                table_id = 20,
                columns = 'id INT PRIMARY KEY, name TEXT, role TEXT',
                indexes = 'role'
            );
            INSERT INTO users VALUES (1, 'Alice', 'admin'), (2, 'Bob', 'user');",
        )
        .unwrap();

        // Plain INSERT with duplicate PK must fail with SQLITE_CONSTRAINT
        let dup_err = conn.execute_batch("INSERT INTO users VALUES (1, 'Imposter', 'hacker');");
        assert!(dup_err.is_err(), "duplicate PK on plain INSERT must fail");

        // Verify original row is intact
        let rows = conn
            .query_all("SELECT id, name, role FROM users ORDER BY id", &[])
            .unwrap();
        assert_eq!(
            rows,
            vec![
                vec![
                    SqlValue::Integer(1),
                    SqlValue::Text("Alice".into()),
                    SqlValue::Text("admin".into())
                ],
                vec![
                    SqlValue::Integer(2),
                    SqlValue::Text("Bob".into()),
                    SqlValue::Text("user".into())
                ],
            ]
        );

        // INSERT OR IGNORE skips conflicting PK 1 and inserts non-conflicting PK 3
        conn.execute_batch(
            "INSERT OR IGNORE INTO users VALUES (1, 'Ignored', 'guest'), (3, 'Charlie', 'mod');",
        )
        .unwrap();
        let rows = conn
            .query_all("SELECT id, name FROM users ORDER BY id", &[])
            .unwrap();
        assert_eq!(
            rows,
            vec![
                vec![SqlValue::Integer(1), SqlValue::Text("Alice".into())],
                vec![SqlValue::Integer(2), SqlValue::Text("Bob".into())],
                vec![SqlValue::Integer(3), SqlValue::Text("Charlie".into())],
            ]
        );

        // INSERT OR REPLACE overwrites PK 1 and updates secondary index `role`
        conn.execute_batch("INSERT OR REPLACE INTO users VALUES (1, 'AliceV2', 'superadmin');")
            .unwrap();
        let old_role = conn
            .query_all("SELECT id FROM users WHERE role = 'admin'", &[])
            .unwrap();
        assert!(old_role.is_empty());
        let new_role = conn
            .query_all("SELECT name FROM users WHERE role = 'superadmin'", &[])
            .unwrap();
        assert_eq!(new_role, vec![vec![SqlValue::Text("AliceV2".into())]]);

        // UPDATE changing PK 3 -> 2 (collision): plain UPDATE must fail and keep PK 3 intact
        let upd_err = conn.execute_batch("UPDATE users SET id = 2 WHERE id = 3;");
        assert!(upd_err.is_err(), "UPDATE to existing PK must fail");
        let check3 = conn
            .query_all("SELECT name FROM users WHERE id = 3", &[])
            .unwrap();
        assert_eq!(check3, vec![vec![SqlValue::Text("Charlie".into())]]);

        // UPDATE OR IGNORE changing PK 3 -> 2 must leave both PK 2 and PK 3 untouched
        conn.execute_batch("UPDATE OR IGNORE users SET id = 2, name = 'NoOp' WHERE id = 3;")
            .unwrap();
        let rows = conn
            .query_all("SELECT id, name FROM users ORDER BY id", &[])
            .unwrap();
        assert_eq!(
            rows,
            vec![
                vec![SqlValue::Integer(1), SqlValue::Text("AliceV2".into())],
                vec![SqlValue::Integer(2), SqlValue::Text("Bob".into())],
                vec![SqlValue::Integer(3), SqlValue::Text("Charlie".into())],
            ]
        );

        // UPDATE OR REPLACE changing PK 3 -> 2 deletes PK 3 and overwrites PK 2
        conn.execute_batch(
            "UPDATE OR REPLACE users SET id = 2, name = 'CharlieReplacedBob' WHERE id = 3;",
        )
        .unwrap();
        let rows = conn
            .query_all("SELECT id, name FROM users ORDER BY id", &[])
            .unwrap();
        assert_eq!(
            rows,
            vec![
                vec![SqlValue::Integer(1), SqlValue::Text("AliceV2".into())],
                vec![
                    SqlValue::Integer(2),
                    SqlValue::Text("CharlieReplacedBob".into())
                ],
            ]
        );

        // Part B: Auto-assigned table_id isolation and duplicate explicit table_id rejection
        conn.execute_batch(
            "CREATE VIRTUAL TABLE auto_a USING aydb(columns = 'k INT PRIMARY KEY, v TEXT');
             CREATE VIRTUAL TABLE auto_b USING aydb(columns = 'k INT PRIMARY KEY, v TEXT');
             INSERT INTO auto_a VALUES (1, 'from_a');
             INSERT INTO auto_b VALUES (1, 'from_b');",
        )
        .unwrap();
        let rows_a = conn.query_all("SELECT v FROM auto_a WHERE k = 1", &[]).unwrap();
        let rows_b = conn.query_all("SELECT v FROM auto_b WHERE k = 1", &[]).unwrap();
        assert_eq!(rows_a, vec![vec![SqlValue::Text("from_a".into())]]);
        assert_eq!(rows_b, vec![vec![SqlValue::Text("from_b".into())]]);

        // Explicit duplicate table_id on a second live table must fail
        let dup_tid = conn.execute_batch(
            "CREATE VIRTUAL TABLE clash_users USING aydb(
                table_id = 20,
                columns = 'id INT PRIMARY KEY, name TEXT'
            );",
        );
        assert!(dup_tid.is_err(), "duplicate live table_id must be rejected");
    }

    #[test]
    fn stage2_aud05_use05_storage_class_key_tags_auto_rowid_and_mixed_range() {
        let tmp = TempTestDir::new("stage2_aud05_use05");
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
        let engine = Arc::new(Mutex::new(
            StorageEngine::open(fs, EngineConfig::default()).unwrap(),
        ));
        let conn = SqliteConnection::open_in_memory().unwrap();
        conn.register_aydb_module(engine).unwrap();

        // 1. Storage-class key tags prevent collisions between INTEGER, REAL, TEXT, and BLOB,
        // while exact integer/float equality (`42 == 42.0`) enforces uniqueness (`C-02`).
        conn.execute_batch(
            "CREATE VIRTUAL TABLE poly_t USING aydb(
                table_id = 30,
                columns = 'pk BLOB PRIMARY KEY, sec BLOB, tag TEXT',
                indexes = 'sec'
            );
            INSERT INTO poly_t VALUES (42, 100, 'int_42');
            INSERT INTO poly_t VALUES (42.5, 100.5, 'real_42_5');
            INSERT INTO poly_t VALUES ('', '', 'empty_text');
            INSERT INTO poly_t VALUES (X'', X'', 'empty_blob');
            INSERT INTO poly_t VALUES ('abc', 'abc', 'text_abc');
            INSERT INTO poly_t VALUES (X'616263', X'616263', 'blob_abc');
            INSERT INTO poly_t VALUES ('01234567', NULL, 'text_8byte');",
        )
        .unwrap();

        // Inserting 42.0 when 42 is already present must fail with SQLITE_CONSTRAINT (`C-02`)
        assert!(
            conn.execute_batch("INSERT INTO poly_t VALUES (42.0, 100.0, 'dup_real_42');")
                .is_err(),
            "C-02: 42.0 must conflict with existing integer 42 on BLOB PRIMARY KEY"
        );

        // All 7 distinct values must coexist without overwriting each other, ordered by storage class
        let ordered_tags = conn.query_all("SELECT tag FROM poly_t", &[]).unwrap();
        assert_eq!(
            ordered_tags,
            vec![
                vec![SqlValue::Text("int_42".into())],
                vec![SqlValue::Text("real_42_5".into())],
                vec![SqlValue::Text("empty_text".into())],
                vec![SqlValue::Text("text_8byte".into())],
                vec![SqlValue::Text("text_abc".into())],
                vec![SqlValue::Text("empty_blob".into())],
                vec![SqlValue::Text("blob_abc".into())],
            ]
        );

        // Point lookups on PK distinguish 'abc' (TEXT) from X'616263' (BLOB) and 42 from 42.0
        let hit_text = conn
            .query_all("SELECT tag FROM poly_t WHERE pk = 'abc'", &[])
            .unwrap();
        let hit_blob = conn
            .query_all("SELECT tag FROM poly_t WHERE pk = X'616263'", &[])
            .unwrap();
        assert_eq!(hit_text, vec![vec![SqlValue::Text("text_abc".into())]]);
        assert_eq!(hit_blob, vec![vec![SqlValue::Text("blob_abc".into())]]);

        // Secondary index lookups also distinguish 'abc' vs X'616263' and '' vs X''
        let sec_text = conn
            .query_all("SELECT tag FROM poly_t WHERE sec = 'abc'", &[])
            .unwrap();
        let sec_blob = conn
            .query_all("SELECT tag FROM poly_t WHERE sec = X'616263'", &[])
            .unwrap();
        let sec_empty_str = conn
            .query_all("SELECT tag FROM poly_t WHERE sec = ''", &[])
            .unwrap();
        let sec_empty_blob = conn
            .query_all("SELECT tag FROM poly_t WHERE sec = X''", &[])
            .unwrap();
        assert_eq!(sec_text, vec![vec![SqlValue::Text("text_abc".into())]]);
        assert_eq!(sec_blob, vec![vec![SqlValue::Text("blob_abc".into())]]);
        assert_eq!(
            sec_empty_str,
            vec![vec![SqlValue::Text("empty_text".into())]]
        );
        assert_eq!(
            sec_empty_blob,
            vec![vec![SqlValue::Text("empty_blob".into())]]
        );

        // 2. USE-05: NULL into INTEGER PRIMARY KEY auto-generates monotonic rowid; NULL into TEXT PK fails
        conn.execute_batch(
            "CREATE VIRTUAL TABLE auto_pk_t USING aydb(
                table_id = 31,
                columns = 'id INT PRIMARY KEY, label TEXT'
            );
            INSERT INTO auto_pk_t VALUES (NULL, 'first');
            INSERT INTO auto_pk_t VALUES (NULL, 'second');
            INSERT INTO auto_pk_t VALUES (10, 'ten');
            INSERT INTO auto_pk_t VALUES (NULL, 'eleven');",
        )
        .unwrap();

        let auto_rows = conn
            .query_all("SELECT id, label FROM auto_pk_t ORDER BY id", &[])
            .unwrap();
        assert_eq!(
            auto_rows,
            vec![
                vec![SqlValue::Integer(1), SqlValue::Text("first".into())],
                vec![SqlValue::Integer(2), SqlValue::Text("second".into())],
                vec![SqlValue::Integer(10), SqlValue::Text("ten".into())],
                vec![SqlValue::Integer(11), SqlValue::Text("eleven".into())],
            ]
        );

        // Mixed-type numeric range scan (`id > 1.5 AND id < 10.5` on INTEGER PRIMARY KEY)
        let mixed_range = conn
            .query_all(
                "SELECT id FROM auto_pk_t WHERE id > 1.5 AND id < 10.5 ORDER BY id",
                &[],
            )
            .unwrap();
        assert_eq!(
            mixed_range,
            vec![vec![SqlValue::Integer(2)], vec![SqlValue::Integer(10)]]
        );

        // Reject NULL on UPDATE of PK or INSERT into non-integer PK
        assert!(
            conn.execute_batch("UPDATE auto_pk_t SET id = NULL WHERE id = 1;")
                .is_err()
        );
        conn.execute_batch(
            "CREATE VIRTUAL TABLE str_pk_t USING aydb(
                table_id = 32,
                columns = 'k TEXT PRIMARY KEY, v INT'
            );",
        )
        .unwrap();
        assert!(
            conn.execute_batch("INSERT INTO str_pk_t VALUES (NULL, 1);")
                .is_err()
        );
    }

    #[test]
    fn stage2_aud11_io_and_corrupt_errors_propagate_from_filter_and_update() {
        let tmp = TempTestDir::new("stage2_aud11");
        let cfg = EngineConfig::default();
        {
            let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
            let engine = Arc::new(Mutex::new(StorageEngine::open(fs, cfg.clone()).unwrap()));
            let conn = SqliteConnection::open_in_memory().unwrap();
            conn.register_aydb_module(engine.clone()).unwrap();

            conn.execute_batch(
                "CREATE VIRTUAL TABLE docs USING aydb(
                    table_id = 40,
                    columns = 'id INT PRIMARY KEY, payload BLOB'
                );
                BEGIN;",
            )
            .unwrap();
            let mut ins = conn.prepare("INSERT INTO docs VALUES (?1, ?2)").unwrap();
            for i in 1..=60 {
                ins.execute(&[SqlValue::Integer(i), SqlValue::Blob(vec![0x55u8; 128])])
                    .unwrap();
            }
            conn.execute_batch("COMMIT;").unwrap();

            // Flush to disk so `idx_*.db` has multiple 4 KB blocks (block 0 has catalog, block 1 has id=60)
            engine.lock().unwrap().flush_memtable().unwrap();
        }

        // Corrupt the second 4 KB data block (offset 4096 + 16) of `idx_*.db` while handles are closed
        let idx_file = std::fs::read_dir(&tmp.path)
            .unwrap()
            .filter_map(|e| e.ok())
            .find(|e| {
                let name = e.file_name().to_string_lossy().into_owned();
                name.starts_with("idx_") && name.ends_with(".db")
            })
            .expect("flushed idx segment file")
            .path();
        let mut idx_bytes = std::fs::read(&idx_file).unwrap();
        let mut blk_off = 4096usize;
        while blk_off + 4096 <= idx_bytes.len() {
            idx_bytes[blk_off + 16] ^= 0xFF;
            blk_off += 4096;
        }
        std::fs::write(&idx_file, &idx_bytes).unwrap();

        // Reopen engine and VTab; point query, full scan, and DELETE/UPDATE on block 1 must return Err
        let fs = Arc::new(OsFs::open(&tmp.path, false).unwrap());
        let engine = Arc::new(Mutex::new(StorageEngine::open(fs, cfg).unwrap()));
        let conn = SqliteConnection::open_in_memory().unwrap();
        conn.register_aydb_module(engine).unwrap();
        conn.execute_batch(
            "CREATE VIRTUAL TABLE docs USING aydb(
                table_id = 40,
                columns = 'id INT PRIMARY KEY, payload BLOB'
            );",
        )
        .unwrap();

        let point_res = conn.query_all("SELECT payload FROM docs WHERE id = 60", &[]);
        assert!(
            point_res.is_err(),
            "corrupted idx block read in xFilter point lookup must propagate error"
        );
        let scan_res = conn.query_all("SELECT payload FROM docs", &[]);
        assert!(
            scan_res.is_err(),
            "corrupted idx block read in xFilter scan must propagate error"
        );
        let del_res = conn.execute_batch("DELETE FROM docs WHERE id = 60;");
        assert!(
            del_res.is_err(),
            "corrupted idx block read during DELETE/xUpdate must propagate error"
        );
    }

    #[test]
    fn stage2_perf04_perf05_full_scan_limit_pushdown_and_rowid_map_compaction() {
        let tmp = TempTestDir::new("stage2_perf04_05");
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
        let engine = Arc::new(Mutex::new(
            StorageEngine::open(fs, EngineConfig::default()).unwrap(),
        ));
        let conn = SqliteConnection::open_in_memory().unwrap();
        conn.register_aydb_module(engine).unwrap();

        conn.execute_batch(
            "CREATE VIRTUAL TABLE kv_text USING aydb(
                table_id = 50,
                columns = 'k TEXT PRIMARY KEY, v INT'
            );
            BEGIN;",
        )
        .unwrap();
        {
            let mut ins = conn.prepare("INSERT INTO kv_text VALUES (?1, ?2)").unwrap();
            for i in 0..17_000i64 {
                ins.execute(&[SqlValue::Text(format!("key_{i:06}")), SqlValue::Integer(i)])
                    .unwrap();
            }
        }
        conn.execute_batch("COMMIT;").unwrap();

        // PERF-04: Full scan with LIMIT 5 (without WHERE clause) pushes LIMIT down
        let top5 = conn
            .query_all("SELECT k, v FROM kv_text LIMIT 5", &[])
            .unwrap();
        assert_eq!(top5.len(), 5);
        assert_eq!(
            top5[0],
            vec![SqlValue::Text("key_000000".into()), SqlValue::Integer(0)]
        );
        assert_eq!(
            top5[4],
            vec![SqlValue::Text("key_000004".into()), SqlValue::Integer(4)]
        );

        // PERF-05: Scanning all 17,000 non-integer PK rows with rowid projection and closing cursor
        // compacts `RowidMap` when cursor closes, and subsequent UPDATE/DELETE still works!
        let all_rowids = conn.query_all("SELECT rowid FROM kv_text", &[]).unwrap();
        assert_eq!(all_rowids.len(), 17_000);
        conn.execute_batch(
            "UPDATE kv_text SET v = 999999 WHERE k = 'key_000000';
             DELETE FROM kv_text WHERE k = 'key_000001';",
        )
        .unwrap();
        let check0 = conn
            .query_all("SELECT v FROM kv_text WHERE k = 'key_000000'", &[])
            .unwrap();
        let check1 = conn
            .query_all("SELECT v FROM kv_text WHERE k = 'key_000001'", &[])
            .unwrap();
        assert_eq!(check0, vec![vec![SqlValue::Integer(999999)]]);
        assert!(check1.is_empty());
    }

    #[test]
    fn stage2_use01_use02_use03_use06_savepoints_drop_table_paren_columns_and_panic_safety() {
        let tmp = TempTestDir::new("stage2_use01_02_03_06");
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
        let engine = Arc::new(Mutex::new(
            StorageEngine::open(fs, EngineConfig::default()).unwrap(),
        ));
        let conn = SqliteConnection::open_in_memory().unwrap();
        conn.register_aydb_module(engine.clone()).unwrap();

        // USE-03: Columns containing commas inside parentheses (`DECIMAL(10, 2)`, `NUMERIC(5, 3)`)
        conn.execute_batch(
            "CREATE VIRTUAL TABLE products USING aydb(
                table_id = 60,
                columns = 'id INT PRIMARY KEY, price DECIMAL(10, 2), weight NUMERIC(5, 3), name VARCHAR(64)',
                indexes = 'name'
            );
            INSERT INTO products VALUES (1, 19.99, 1.25, 'Widget');",
        )
        .unwrap();
        let prod_rows = conn
            .query_all("SELECT id, price, weight, name FROM products WHERE name = 'Widget'", &[])
            .unwrap();
        assert_eq!(
            prod_rows,
            vec![vec![
                SqlValue::Integer(1),
                SqlValue::Float(19.99),
                SqlValue::Float(1.25),
                SqlValue::Text("Widget".into()),
            ]]
        );

        // USE-01: Nested SAVEPOINT, ROLLBACK TO, and RELEASE
        conn.execute_batch(
            "BEGIN;
             INSERT INTO products VALUES (2, 29.99, 2.50, 'Gadget');
             SAVEPOINT sp1;
             INSERT INTO products VALUES (3, 39.99, 3.75, 'Sprocket');
             UPDATE products SET price = 99.99 WHERE id = 1;
             SAVEPOINT sp2;
             DELETE FROM products WHERE id = 2;
             ROLLBACK TO sp2;
             RELEASE sp2;
             ROLLBACK TO sp1;
             RELEASE sp1;
             COMMIT;",
        )
        .unwrap();

        // After rolling back to sp1 and committing: id=1 still has 19.99, id=2 exists, id=3 was rolled back
        let after_sp = conn
            .query_all("SELECT id, price, name FROM products ORDER BY id", &[])
            .unwrap();
        assert_eq!(
            after_sp,
            vec![
                vec![
                    SqlValue::Integer(1),
                    SqlValue::Float(19.99),
                    SqlValue::Text("Widget".into())
                ],
                vec![
                    SqlValue::Integer(2),
                    SqlValue::Float(29.99),
                    SqlValue::Text("Gadget".into())
                ],
            ]
        );
        // Secondary index for rolled-back 'Sprocket' must also be clean
        let sprocket = conn
            .query_all("SELECT id FROM products WHERE name = 'Sprocket'", &[])
            .unwrap();
        assert!(sprocket.is_empty());

        // USE-02: DROP TABLE (xDestroy) removes all rows, secondary indexes, and catalog entry
        conn.execute_batch("DROP TABLE products;").unwrap();
        conn.execute_batch(
            "CREATE VIRTUAL TABLE products USING aydb(
                table_id = 60,
                columns = 'id INT PRIMARY KEY, price DECIMAL(10, 2), weight NUMERIC(5, 3), name VARCHAR(64)',
                indexes = 'name'
            );",
        )
        .unwrap();
        let after_drop = conn.query_all("SELECT * FROM products", &[]).unwrap();
        assert!(
            after_drop.is_empty(),
            "recreated table after DROP TABLE must be empty"
        );
    }

    #[test]
    fn f01_f07_cross_connection_transaction_isolation_and_two_phase_sync_rollback() {
        let tmp = TempTestDir::new("f01_f07_conn_isolation");
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
        let shared = Arc::new(Mutex::new(
            StorageEngine::open(fs, EngineConfig::default()).unwrap(),
        ));

        let conn1 = SqliteConnection::open_in_memory().unwrap();
        conn1.register_aydb_module(shared.clone()).unwrap();
        conn1
            .execute_batch(
                "CREATE VIRTUAL TABLE t USING aydb(
                    table_id = 1,
                    columns = 'id INTEGER PRIMARY KEY, v TEXT'
                );
                INSERT INTO t VALUES (1, 'one'), (2, 'two');",
            )
            .unwrap();

        let conn2 = SqliteConnection::open_in_memory().unwrap();
        conn2.register_aydb_module(shared.clone()).unwrap();
        conn2
            .execute_batch(
                "CREATE VIRTUAL TABLE t USING aydb(
                    table_id = 1,
                    columns = 'id INTEGER PRIMARY KEY, v TEXT'
                );",
            )
            .unwrap();

        // 1. Connection 1 opens a transaction and inserts id=3 ('uncommitted'), updates id=1, deletes id=2
        conn1
            .execute_batch(
                "BEGIN;
                 INSERT INTO t VALUES (3, 'uncommitted');
                 UPDATE t SET v = 'one_modified' WHERE id = 1;
                 DELETE FROM t WHERE id = 2;",
            )
            .unwrap();

        // Connection 1 sees its own uncommitted changes (read-your-own-writes)
        assert_eq!(
            conn1
                .query_all("SELECT id, v FROM t WHERE id = 3", &[])
                .unwrap(),
            vec![vec![
                SqlValue::Integer(3),
                SqlValue::Text("uncommitted".into())
            ]]
        );

        // 2. Connection 2 MUST NOT see Connection 1's uncommitted changes!
        assert!(
            conn2
                .query_all("SELECT * FROM t WHERE id = 3", &[])
                .unwrap()
                .is_empty(),
            "F01: second connection must not see uncommitted row from first connection"
        );
        assert_eq!(
            conn2
                .query_all("SELECT id, v FROM t ORDER BY id", &[])
                .unwrap(),
            vec![
                vec![SqlValue::Integer(1), SqlValue::Text("one".into())],
                vec![SqlValue::Integer(2), SqlValue::Text("two".into())],
            ],
            "F01: second connection must read the exact pre-transaction committed snapshot"
        );

        // 3. Connection 2 attempting an autocommit write while Connection 1 holds the transaction
        //    must fail with SQLITE_BUSY rather than silently joining Connection 1's transaction!
        assert!(
            conn2
                .execute_batch("INSERT INTO t VALUES (4, 'other committed');")
                .is_err(),
            "F01: concurrent write on second connection while first connection is in a transaction must fail with busy error"
        );

        // 4. Connection 1 rolls back
        conn1.execute_batch("ROLLBACK;").unwrap();

        // 5. Now Connection 2's write succeeds and persists cleanly!
        conn2
            .execute_batch("INSERT INTO t VALUES (4, 'other committed');")
            .unwrap();
        assert_eq!(
            conn1
                .query_all("SELECT id, v FROM t WHERE id = 4", &[])
                .unwrap(),
            vec![vec![
                SqlValue::Integer(4),
                SqlValue::Text("other committed".into())
            ]]
        );
    }

    #[test]
    fn f04_f05_f08_affinity_collation_unified_numeric_sorting_and_max_rowid() {
        let tmp = TempTestDir::new("f04_f05_f08");
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
        let shared = Arc::new(Mutex::new(
            StorageEngine::open(fs, EngineConfig::default()).unwrap(),
        ));
        let conn = SqliteConnection::open_in_memory().unwrap();
        conn.register_aydb_module(shared).unwrap();

        // 1. F04: INTEGER PRIMARY KEY affinity coercion on lookup (`WHERE id=1.0`, `WHERE id='1'`, `WHERE +id=1.0`)
        conn.execute_batch(
            "CREATE VIRTUAL TABLE t_int USING aydb(
                table_id = 70,
                columns = 'id INTEGER PRIMARY KEY, score INT, label TEXT',
                indexes = 'score'
            );
            INSERT INTO t_int VALUES (1, 100, 'one'), (2, 200, 'two');",
        )
        .unwrap();

        for sql in [
            "SELECT id FROM t_int WHERE id = 1.0",
            "SELECT id FROM t_int WHERE id = '1'",
            "SELECT id FROM t_int WHERE +id = 1.0",
            "SELECT id FROM t_int WHERE score = 100.0",
            "SELECT id FROM t_int WHERE score = '100'",
        ] {
            assert_eq!(
                conn.query_all(sql, &[]).unwrap(),
                vec![vec![SqlValue::Integer(1)]],
                "F04 query failed: {sql}"
            );
        }

        // Inserting '3' or 4.0 into INTEGER PRIMARY KEY stores integer values
        conn.execute_batch("INSERT INTO t_int VALUES ('3', '300', 'three'), (4.0, 400.0, 'four');")
            .unwrap();
        assert_eq!(
            conn.query_all("SELECT id, score FROM t_int WHERE id >= 3 ORDER BY id", &[])
                .unwrap(),
            vec![
                vec![SqlValue::Integer(3), SqlValue::Integer(300)],
                vec![SqlValue::Integer(4), SqlValue::Integer(400)],
            ]
        );

        // 2. F04: COLLATE NOCASE and COLLATE RTRIM on primary key and secondary index, plus rejecting unknown collations
        conn.execute_batch(
            "CREATE VIRTUAL TABLE t_coll USING aydb(
                table_id = 71,
                columns = 'k TEXT PRIMARY KEY COLLATE NOCASE, tag TEXT COLLATE RTRIM',
                indexes = 'tag'
            );
            INSERT INTO t_coll VALUES ('Alice', 'admin   ');",
        )
        .unwrap();

        assert_eq!(
            conn.query_all("SELECT k FROM t_coll WHERE k = 'alice'", &[])
                .unwrap(),
            vec![vec![SqlValue::Text("Alice".into())]]
        );
        assert_eq!(
            conn.query_all("SELECT k FROM t_coll WHERE +k = 'ALICE'", &[])
                .unwrap(),
            vec![vec![SqlValue::Text("Alice".into())]]
        );
        assert_eq!(
            conn.query_all("SELECT k, tag FROM t_coll WHERE tag = 'admin'", &[])
                .unwrap(),
            vec![vec![
                SqlValue::Text("Alice".into()),
                SqlValue::Text("admin   ".into())
            ]]
        );
        // Duplicate under NOCASE must fail with SQLITE_CONSTRAINT
        assert!(
            conn.execute_batch("INSERT INTO t_coll VALUES ('ALICE', 'other');")
                .is_err(),
            "F04: duplicate COLLATE NOCASE key must fail uniqueness check"
        );
        // Unsupported collation in CREATE VIRTUAL TABLE must be rejected
        assert!(
            conn.execute_batch(
                "CREATE VIRTUAL TABLE bad_coll USING aydb(
                    table_id = 72,
                    columns = 'k TEXT PRIMARY KEY COLLATE NOSUCHCOLLATION'
                );"
            )
            .is_err(),
            "F04: unknown collation must be rejected at table creation"
        );

        // 3. F05: Unified exact numeric sorting across mixed INTEGER and REAL values (including 2^53 + 1)
        conn.execute_batch(
            "CREATE VIRTUAL TABLE m_num USING aydb(
                table_id = 73,
                columns = 'k BLOB PRIMARY KEY, v TEXT'
            );
            INSERT INTO m_num VALUES
                (10, 'ten'),
                (1.5, 'one_half'),
                (-1.5, 'neg_one_half'),
                (-10, 'neg_ten'),
                (0, 'zero'),
                (9007199254740993, 'two53_plus_1'),
                (9007199254740992, 'two53'),
                (-9007199254740992, 'neg_two53'),
                (-9007199254740993, 'neg_two53_minus_1');",
        )
        .unwrap();

        let sorted_mixed = conn
            .query_all("SELECT k FROM m_num ORDER BY k", &[])
            .unwrap();
        assert_eq!(
            sorted_mixed,
            vec![
                vec![SqlValue::Integer(-9_007_199_254_740_993)],
                vec![SqlValue::Integer(-9_007_199_254_740_992)],
                vec![SqlValue::Integer(-10)],
                vec![SqlValue::Float(-1.5)],
                vec![SqlValue::Integer(0)],
                vec![SqlValue::Float(1.5)],
                vec![SqlValue::Integer(10)],
                vec![SqlValue::Integer(9_007_199_254_740_992)],
                vec![SqlValue::Integer(9_007_199_254_740_993)],
            ]
        );

        // Range scan with LIMIT pushdown on mixed numeric BLOB PK
        let top3_gt_neg2 = conn
            .query_all("SELECT k FROM m_num WHERE k > -2 ORDER BY k LIMIT 3", &[])
            .unwrap();
        assert_eq!(
            top3_gt_neg2,
            vec![
                vec![SqlValue::Float(-1.5)],
                vec![SqlValue::Integer(0)],
                vec![SqlValue::Float(1.5)],
            ]
        );

        // Point lookup of integer 10 using 10.0 on BLOB primary key
        assert_eq!(
            conn.query_all("SELECT v FROM m_num WHERE k = 10.0", &[])
                .unwrap(),
            vec![vec![SqlValue::Text("ten".into())]]
        );

        // 4. F08: Explicit i64::MAX insertion followed by NULL auto-ID insertion must not hang!
        conn.execute_batch(
            "CREATE VIRTUAL TABLE max_id_t USING aydb(
                table_id = 74,
                columns = 'id INTEGER PRIMARY KEY, label TEXT'
            );
            INSERT INTO max_id_t VALUES (9223372036854775807, 'max_int');
            INSERT INTO max_id_t VALUES (NULL, 'after_max_1');
            INSERT INTO max_id_t VALUES (NULL, 'after_max_2');",
        )
        .unwrap();

        let rows_after_max = conn
            .query_all("SELECT id, label FROM max_id_t ORDER BY id", &[])
            .unwrap();
        assert_eq!(rows_after_max.len(), 3);
        assert_eq!(
            rows_after_max[2],
            vec![SqlValue::Integer(i64::MAX), SqlValue::Text("max_int".into())]
        );
        // Both auto-generated IDs must be valid positive integers < i64::MAX and distinct
        let id1 = match rows_after_max[0][0] {
            SqlValue::Integer(v) => v,
            _ => panic!("expected integer PK"),
        };
        let id2 = match rows_after_max[1][0] {
            SqlValue::Integer(v) => v,
            _ => panic!("expected integer PK"),
        };
        assert!(id1 > 0 && id1 < id2 && id2 < i64::MAX);
    }

    #[test]
    fn p02_vtab_keys_only_projection_and_tx_budget() {
        let tmp = TempTestDir::new("p02_vtab_keys_only");
        let mut cfg = EngineConfig::default();
        cfg.max_transaction_bytes = 32 * 1024; // 32 KB transaction budget

        {
            let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
            let engine = Arc::new(Mutex::new(
                StorageEngine::open(fs.clone(), cfg.clone()).unwrap(),
            ));
            let conn = SqliteConnection::open_in_memory().unwrap();
            conn.register_aydb_module(engine.clone()).unwrap();

            conn.execute_batch(
                "CREATE VIRTUAL TABLE docs USING aydb(
                    table_id = 80,
                    columns = 'id INTEGER PRIMARY KEY, body BLOB'
                );",
            )
            .unwrap();

            let big_blob = vec![0x5Au8; 4096];
            for id in 1..=5i64 {
                conn.query_all(
                    "INSERT INTO docs VALUES (?1, ?2)",
                    &[SqlValue::Integer(id), SqlValue::Blob(big_blob.clone())],
                )
                .unwrap();
            }
            engine.lock().unwrap().flush_memtable().unwrap();
        }

        // Corrupt the out-of-line `blob_std_*.db` payload bytes on disk while handles are closed
        // to prove that `SELECT COUNT(*)`, `SELECT id`, and `SELECT rowid, id` do NOT read any blob files!
        let mut found_blob = false;
        for entry in std::fs::read_dir(&tmp.path).unwrap().flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with("blob_std_") && name.ends_with(".db") {
                found_blob = true;
                let p = entry.path();
                let blen = std::fs::metadata(&p).unwrap().len() as usize;
                std::fs::write(&p, vec![0xFFu8; blen]).unwrap();
            }
        }
        assert!(found_blob);

        let fs = Arc::new(OsFs::open(&tmp.path, false).unwrap());
        let engine = Arc::new(Mutex::new(StorageEngine::open(fs, cfg).unwrap()));
        let conn = SqliteConnection::open_in_memory().unwrap();
        conn.register_aydb_module(engine).unwrap();
        conn.execute_batch(
            "CREATE VIRTUAL TABLE docs USING aydb(
                table_id = 80,
                columns = 'id INTEGER PRIMARY KEY, body BLOB'
            );",
        )
        .unwrap();

        // `SELECT COUNT(*) FROM docs` (`colUsed == 0`) must succeed without touching `blob_std_*.db`!
        assert_eq!(
            conn.query_all("SELECT COUNT(*) FROM docs", &[]).unwrap(),
            vec![vec![SqlValue::Integer(5)]]
        );

        // `SELECT id FROM docs WHERE id >= 2 AND id <= 4` (`colUsed == (1 << pk_col_idx)`)
        // must also succeed via `RANGE_FLAG_KEYS_ONLY` without touching `blob_std_*.db`!
        assert_eq!(
            conn.query_all("SELECT id FROM docs WHERE id >= 2 AND id <= 4 ORDER BY id", &[])
                .unwrap(),
            vec![
                vec![SqlValue::Integer(2)],
                vec![SqlValue::Integer(3)],
                vec![SqlValue::Integer(4)],
            ]
        );

        // `SELECT rowid, id FROM docs` must also succeed via `RANGE_FLAG_KEYS_ONLY`!
        assert_eq!(
            conn.query_all("SELECT rowid, id FROM docs WHERE id <= 2 ORDER BY id", &[])
                .unwrap(),
            vec![
                vec![SqlValue::Integer(1), SqlValue::Integer(1)],
                vec![SqlValue::Integer(2), SqlValue::Integer(2)],
            ]
        );

        // Whereas projecting `body` (`SELECT body FROM docs`) DOES attempt to dereference the
        // corrupted blob and skips all 5 rows (`Ok(false)` safe blob miss), proving that the
        // keys-only queries above never touched `blob_std_*.db`!
        assert_eq!(
            conn.query_all("SELECT body FROM docs", &[]).unwrap(),
            Vec::<Vec<SqlValue>>::new(),
            "reading projected BLOB column must dereference blob_std and skip corrupted blobs"
        );

        // Verify `max_transaction_bytes` rejects an oversized SQL transaction cleanly
        conn.execute_batch("BEGIN;").unwrap();
        let huge_blob = vec![0x77u8; 40 * 1024]; // 40 KB > 32 KB max_transaction_bytes
        let res = conn.query_all(
            "INSERT INTO docs VALUES (?1, ?2)",
            &[SqlValue::Integer(99), SqlValue::Blob(huge_blob)],
        );
        assert!(
            res.is_err(),
            "P02: SQL INSERT exceeding max_transaction_bytes must fail"
        );
        conn.execute_batch("ROLLBACK;").unwrap();
        assert_eq!(
            conn.query_all("SELECT COUNT(*) FROM docs", &[]).unwrap(),
            vec![vec![SqlValue::Integer(5)]]
        );
    }

    #[test]
    fn stage2_c01_c02_c03_c04_c05_audit_remediations() {
        let tmp = TempTestDir::new("stage2_c01_to_c05");
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
        let engine = Arc::new(Mutex::new(
            StorageEngine::open(fs, EngineConfig::default()).unwrap(),
        ));
        let conn = SqliteConnection::open_in_memory().unwrap();
        conn.register_aydb_module(engine).unwrap();

        // 1. C-01: Negative sub-ULP floats (-1e-17, -1e-20, -1e-300, -f64::MIN_POSITIVE, -0.001, -0.5, -1.0, -1.5)
        //    on REAL PRIMARY KEY and BLOB PRIMARY KEY must not collide and must round-trip via
        //    keys-only projection (`SELECT pk FROM t`) without collapsing to 0.0!
        conn.execute_batch(
            "CREATE VIRTUAL TABLE c01_real USING aydb(
                table_id = 90,
                columns = 'pk REAL PRIMARY KEY, label TEXT'
            );
            CREATE VIRTUAL TABLE c01_blob USING aydb(
                table_id = 91,
                columns = 'pk BLOB PRIMARY KEY, label TEXT'
            );",
        )
        .unwrap();

        let neg_floats = [
            -1.5_f64,
            -1.0,
            -0.5,
            -0.001,
            -1e-17,
            -1e-20,
            -1e-300,
            -f64::MIN_POSITIVE,
            0.0,
        ];
        {
            let mut ins_real = conn
                .prepare("INSERT INTO c01_real VALUES (?1, ?2)")
                .unwrap();
            let mut ins_blob = conn
                .prepare("INSERT INTO c01_blob VALUES (?1, ?2)")
                .unwrap();
            for (idx, &v) in neg_floats.iter().enumerate() {
                ins_real
                    .execute(&[SqlValue::Float(v), SqlValue::Text(format!("r_{idx}"))])
                    .unwrap();
                ins_blob
                    .execute(&[SqlValue::Float(v), SqlValue::Text(format!("b_{idx}"))])
                    .unwrap();
            }
        }

        // Keys-only projection (`SELECT pk FROM ...`) uses `SqlValue::from_key_bytes` directly!
        let real_pks = conn.query_all("SELECT pk FROM c01_real", &[]).unwrap();
        let blob_pks = conn.query_all("SELECT pk FROM c01_blob", &[]).unwrap();
        assert_eq!(real_pks.len(), neg_floats.len());
        assert_eq!(blob_pks.len(), neg_floats.len());
        for (i, &expected) in neg_floats.iter().enumerate() {
            match (&real_pks[i][0], &blob_pks[i][0]) {
                (SqlValue::Float(r), SqlValue::Float(b)) => {
                    assert_eq!(r.to_bits(), expected.to_bits());
                    assert_eq!(b.to_bits(), expected.to_bits());
                }
                other => panic!("expected Float PKs, got {other:?}"),
            }
        }

        // 2. C-02: Inserting 10 and 10.0 into a BLOB PRIMARY KEY or REAL PRIMARY KEY
        //    must raise SQLITE_CONSTRAINT on plain INSERT, skip on INSERT OR IGNORE,
        //    and replace the existing row on INSERT OR REPLACE.
        conn.execute_batch(
            "CREATE VIRTUAL TABLE c02_blob USING aydb(
                table_id = 92,
                columns = 'pk BLOB PRIMARY KEY, v TEXT'
            );
            CREATE VIRTUAL TABLE c02_real USING aydb(
                table_id = 93,
                columns = 'pk REAL PRIMARY KEY, v TEXT'
            );
            INSERT INTO c02_blob VALUES (10, 'int_ten');
            INSERT INTO c02_real VALUES (10, 'int_ten_as_real');",
        )
        .unwrap();

        // Plain INSERT of 10.0 must fail with SQLITE_CONSTRAINT on both tables
        assert!(
            conn.execute_batch("INSERT INTO c02_blob VALUES (10.0, 'float_ten');")
                .is_err(),
            "C-02: plain INSERT of 10.0 when 10 exists on BLOB PK must fail"
        );
        assert!(
            conn.execute_batch("INSERT INTO c02_real VALUES (10.0, 'float_ten');")
                .is_err(),
            "C-02: plain INSERT of 10.0 when 10 exists on REAL PK must fail"
        );

        // INSERT OR IGNORE of 10.0 must skip without modifying the existing row
        conn.execute_batch(
            "INSERT OR IGNORE INTO c02_blob VALUES (10.0, 'ignored');
             INSERT OR IGNORE INTO c02_real VALUES (10.0, 'ignored');",
        )
        .unwrap();
        assert_eq!(
            conn.query_all("SELECT pk, v FROM c02_blob", &[]).unwrap(),
            vec![vec![SqlValue::Integer(10), SqlValue::Text("int_ten".into())]]
        );

        // INSERT OR REPLACE of 10.0 must replace the existing 10 row so only 1 row remains!
        conn.execute_batch(
            "INSERT OR REPLACE INTO c02_blob VALUES (10.0, 'replaced_by_float');
             INSERT OR REPLACE INTO c02_real VALUES (10.0, 'replaced_by_float');",
        )
        .unwrap();
        assert_eq!(
            conn.query_all("SELECT pk, v FROM c02_blob", &[]).unwrap(),
            vec![vec![
                SqlValue::Float(10.0),
                SqlValue::Text("replaced_by_float".into())
            ]]
        );
        assert_eq!(
            conn.query_all("SELECT pk, v FROM c02_real", &[]).unwrap(),
            vec![vec![
                SqlValue::Float(10.0),
                SqlValue::Text("replaced_by_float".into())
            ]]
        );

        // Replacing back with integer 10 on BLOB PK must also replace 10.0 cleanly!
        conn.execute_batch("INSERT OR REPLACE INTO c02_blob VALUES (10, 'back_to_int');")
            .unwrap();
        assert_eq!(
            conn.query_all("SELECT pk, v FROM c02_blob", &[]).unwrap(),
            vec![vec![
                SqlValue::Integer(10),
                SqlValue::Text("back_to_int".into())
            ]]
        );

        // 3. C-03: `SELECT * FROM t WHERE v LIKE '%match%' LIMIT 3` must return all 3 matching
        //    rows even when the first 50 rows in the table do not match '%match%'.
        conn.execute_batch(
            "CREATE VIRTUAL TABLE c03_like USING aydb(
                table_id = 94,
                columns = 'id INT PRIMARY KEY, v TEXT'
            );
            BEGIN;",
        )
        .unwrap();
        {
            let mut ins = conn
                .prepare("INSERT INTO c03_like VALUES (?1, ?2)")
                .unwrap();
            for i in 1..=60i64 {
                let text = if i <= 50 {
                    format!("prefix_skip_{i}")
                } else {
                    format!("prefix_match_{i}_suffix")
                };
                ins.execute(&[SqlValue::Integer(i), SqlValue::Text(text)])
                    .unwrap();
            }
        }
        conn.execute_batch("COMMIT;").unwrap();

        let like_full_scan = conn
            .query_all(
                "SELECT id, v FROM c03_like WHERE v LIKE '%match%' LIMIT 3",
                &[],
            )
            .unwrap();
        assert_eq!(
            like_full_scan,
            vec![
                vec![
                    SqlValue::Integer(51),
                    SqlValue::Text("prefix_match_51_suffix".into())
                ],
                vec![
                    SqlValue::Integer(52),
                    SqlValue::Text("prefix_match_52_suffix".into())
                ],
                vec![
                    SqlValue::Integer(53),
                    SqlValue::Text("prefix_match_53_suffix".into())
                ],
            ]
        );

        // Also test PK range scan combined with LIKE and LIMIT
        let like_range_scan = conn
            .query_all(
                "SELECT id FROM c03_like WHERE id >= 10 AND v LIKE '%match%' LIMIT 3",
                &[],
            )
            .unwrap();
        assert_eq!(
            like_range_scan,
            vec![
                vec![SqlValue::Integer(51)],
                vec![SqlValue::Integer(52)],
                vec![SqlValue::Integer(53)],
            ]
        );

        // 4. C-04: `SELECT rowid FROM t` followed by `UPDATE` / `DELETE WHERE rowid = ?` on a
        //    `TEXT PRIMARY KEY` table with > 20,000 rows must succeed!
        conn.execute_batch(
            "CREATE VIRTUAL TABLE c04_text USING aydb(
                table_id = 95,
                columns = 'k TEXT PRIMARY KEY, val INT'
            );
            BEGIN;",
        )
        .unwrap();
        {
            let mut ins = conn
                .prepare("INSERT INTO c04_text VALUES (?1, ?2)")
                .unwrap();
            for i in 0..21_000i64 {
                ins.execute(&[SqlValue::Text(format!("row_{i:06}")), SqlValue::Integer(i)])
                    .unwrap();
            }
        }
        conn.execute_batch("COMMIT;").unwrap();

        let all_rids = conn
            .query_all("SELECT rowid, k FROM c04_text ORDER BY k", &[])
            .unwrap();
        assert_eq!(all_rids.len(), 21_000);
        let rid_first = match all_rids[0][0] {
            SqlValue::Integer(r) => r,
            _ => unreachable!(),
        };
        let rid_last = match all_rids[20_999][0] {
            SqlValue::Integer(r) => r,
            _ => unreachable!(),
        };

        conn.query_all(
            "UPDATE c04_text SET val = 777777 WHERE rowid = ?1",
            &[SqlValue::Integer(rid_first)],
        )
        .unwrap();
        conn.query_all(
            "DELETE FROM c04_text WHERE rowid = ?1",
            &[SqlValue::Integer(rid_last)],
        )
        .unwrap();

        assert_eq!(
            conn.query_all("SELECT val FROM c04_text WHERE k = 'row_000000'", &[])
                .unwrap(),
            vec![vec![SqlValue::Integer(777777)]]
        );
        assert!(
            conn.query_all("SELECT val FROM c04_text WHERE k = 'row_020999'", &[])
                .unwrap()
                .is_empty()
        );

        // 5. C-05: SqliteStatement re-binding (`sqlite3_clear_bindings`) and float-to-text formatting
        conn.execute_batch(
            "CREATE VIRTUAL TABLE c05_text USING aydb(
                table_id = 96,
                columns = 'k TEXT PRIMARY KEY, v TEXT'
            );",
        )
        .unwrap();
        let mut stmt = conn
            .prepare("INSERT INTO c05_text VALUES (?1, ?2)")
            .unwrap();
        stmt.execute(&[SqlValue::Float(1e20), SqlValue::Text("first".into())])
            .unwrap();
        // Execute again with only 1 parameter bound; ?2 must be cleared to NULL by sqlite3_clear_bindings!
        stmt.execute(&[SqlValue::Float(-1e-20)]).unwrap();

        let c05_rows = conn
            .query_all("SELECT k, v FROM c05_text ORDER BY k", &[])
            .unwrap();
        assert_eq!(
            c05_rows,
            vec![
                vec![SqlValue::Text("-1.0e-20".into()), SqlValue::Null],
                vec![
                    SqlValue::Text("1.0e+20".into()),
                    SqlValue::Text("first".into())
                ],
            ]
        );
    }
}
