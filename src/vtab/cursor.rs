//! Virtual table streaming cursor, filter execution, column projection, and rowid lookup.

use core::ffi::{c_char, c_int};
use std::io;
use std::panic::{AssertUnwindSafe, catch_unwind};

use crate::engine::{
    EngineReader, LazyBlob, RangeScanOptions, SecondaryScanOptions, SlabScanOutput,
};
use crate::pal::{OsFile, OsFs};
use crate::vtab::ffi::{
    SQLITE_CORRUPT, SQLITE_ERROR, SQLITE_FLOAT, SQLITE_INTEGER, SQLITE_OK, SQLITE_TEXT,
    SQLITE_TOOBIG, SqliteContextRef, SqliteValueRef, set_vtab_err_msg, sqlite3_context,
    sqlite3_int64, sqlite3_value, sqlite3_vtab, sqlite3_vtab_cursor,
};
use crate::vtab::lifecycle::STALE_VTAB_ERR;
use crate::vtab::planner::{DecodedPlan, RangePlan};
use crate::vtab::row_codec::{
    ColCollation, ColumnAffinity, KEY_TAG_NULL, decode_i64_tagged_key, is_canonical_float_zero_key,
    project_column_zero_copy, project_key_zero_copy, sqlite_value_to_key_bytes_with_schema,
};
use crate::vtab::rowid_map::{extract_rowid_from_pk_bytes, extract_rowid_from_row};
use crate::vtab::{
    AydbVTab, build_numeric_alt_key, extract_lossless_rowid_arg, io_err_to_sqlite_rc,
    needs_numeric_alt_probe, point_fetch, resolve_pk_for_rowid, unique_key,
    with_snapshot_read_access,
};

/// Maximum number of rows fetched per streaming cursor batch, bounding `cursor.slab`
/// to $O(1)$ memory regardless of table size while eliminating full-scan materialization on `LIMIT` queries.
const CURSOR_STREAM_CHUNK_ROWS: usize = 256;

/// Shrink `cursor.lazy_row_buf` down to this capacity on cursor reset after a huge single blob row.
const LAZY_ROW_BUF_SHRINK_THRESHOLD: usize = 256 * 1024;

#[cfg(test)]
thread_local! {
    /// Test observability: high-water mark of `cursor.slab.len()` observed in `vtab_column`.
    pub(in crate::vtab) static CURSOR_SLAB_MAX_LEN: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    /// Test observability: high-water mark of `cursor.lazy_row_buf.len()` observed in `vtab_column`.
    pub(in crate::vtab) static CURSOR_LAZY_ROW_BUF_MAX_LEN: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}

#[repr(C)]
pub(in crate::vtab) struct AydbCursor {
    base: sqlite3_vtab_cursor,
    snapshot: Option<crate::engine::ReadSnapshotToken>,
    /// Contiguous reusable byte slab holding scanned `row_bytes` for the current cursor batch.
    slab: Vec<u8>,
    /// `(start_offset, end_offset)` into `slab` for each row.
    spans: Vec<(usize, usize)>,
    /// Primary key bytes per row.
    pks: Vec<Vec<u8>>,
    /// Lazily batch-populated persistent synthetic rowids for the current chunk's `pks`
    /// (non-`INTEGER PRIMARY KEY` tables). Empty until `vtab_rowid` or `xColumn(-1)` runs.
    rowids: Vec<Option<i64>>,
    /// Pending out-of-line blob pointer for lazy dereference in `vtab_column`.
    /// When `Some(ptr)`, the row's blob payload has not been read into `lazy_row_buf` yet.
    /// When `None`, the row was either inline (in `slab`) or has already been dereferenced.
    lazy_blobs: Vec<Option<LazyBlob<OsFile>>>,
    /// Reusable single-row buffer for dereferencing the active row's out-of-line lazy blob,
    /// preventing `slab` from accumulating up to `CURSOR_STREAM_CHUNK_ROWS` large blobs at once.
    lazy_row_buf: Vec<u8>,
    /// Chunk row index currently materialized in `lazy_row_buf`, if any.
    lazy_row_pos: Option<usize>,
    /// True when the active scan only needs primary keys (`RANGE_FLAG_KEYS_ONLY`),
    /// avoiding any out-of-line blob reads or `slab` allocations.
    keys_only: bool,
    pos: usize,
    /// Exclusive end of the `lazy_blobs` window for which a prefetch was already issued in the
    /// current batch. A new prefetch is built only when `pos` reaches it; it is reset together
    /// with `lazy_blobs`.
    prefetch_end: usize,
    /// True when more chunks may remain in the current range/full/secondary-index scan.
    streaming_active: bool,
    stream_lower: Option<(Vec<u8>, bool)>,
    stream_upper: Option<(Vec<u8>, bool)>,
    stream_reverse: bool,
    stream_last_pk: Option<Vec<u8>>,
    /// Active secondary index ID when streaming `PLAN_SECONDARY_EQ` or `PLAN_SECONDARY_RANGE`.
    stream_sec_index_id: Option<u16>,
    /// Prefix key for the active secondary index scan.
    stream_sec_prefix: Vec<u8>,
}

fn clear_chunk_vecs(
    slab: &mut Vec<u8>,
    spans: &mut Vec<(usize, usize)>,
    pks: &mut Vec<Vec<u8>>,
    rowids: &mut Vec<Option<i64>>,
    lazy_blobs: &mut Vec<Option<LazyBlob<OsFile>>>,
) {
    slab.clear();
    spans.clear();
    pks.clear();
    rowids.clear();
    lazy_blobs.clear();
}

impl AydbCursor {
    #[inline]
    fn clear_chunk_buffers(&mut self) {
        clear_chunk_vecs(
            &mut self.slab,
            &mut self.spans,
            &mut self.pks,
            &mut self.rowids,
            &mut self.lazy_blobs,
        );
    }

    #[inline]
    fn row_count(&self) -> usize {
        if self.keys_only {
            self.pks.len()
        } else {
            self.spans.len()
        }
    }
}

/// Returns `true` when an equality probe `(col = rhs)` cannot map SQLite's comparison
/// semantics onto a single stored key probe and must fall back to a scan with `omit = 0`:
/// - `TEXT` column vs `INTEGER | FLOAT` rhs: numeric literals compare via text/numeric conversion.
/// - `BLOB` column vs `INTEGER | FLOAT | TEXT` rhs: `SQLITE_INTEGER`/`SQLITE_FLOAT` or
///   `SQLITE_TEXT` probes from numeric-affine joins may have non-BLOB affinity in SQLite's
///   comparison operator, so fall back cleanly to SQLite's VDBE filter (`omit = 0`).
#[inline]
fn is_cross_affinity_eq_search(col_affinity: ColumnAffinity, val_type: c_int) -> bool {
    match col_affinity {
        ColumnAffinity::Text => matches!(val_type, SQLITE_INTEGER | SQLITE_FLOAT),
        ColumnAffinity::Blob => matches!(val_type, SQLITE_INTEGER | SQLITE_FLOAT | SQLITE_TEXT),
        _ => false,
    }
}

/// Returns `true` when a range bound (`>`, `>=`, `<`, `<=`) on `col_affinity` cannot be safely
/// encoded as a single key bound and must be left unbounded (`Ok(None)`) for SQLite (`omit = 0`):
/// - `BLOB` columns with `INTEGER | FLOAT | TEXT` bounds.
/// - `TEXT` columns with `INTEGER | FLOAT` bounds, or an upper bound (`is_lower == false`) with
///   `SQLITE_TEXT` (since cross-affinity joins can coerce or match stored keys below `KEY_TAG_TEXT`
///   and numeric/blob ordering diverges under upper bounds).
#[inline]
fn is_cross_affinity_range_bound(
    col_affinity: ColumnAffinity,
    val_type: c_int,
    is_lower: bool,
) -> bool {
    match col_affinity {
        ColumnAffinity::Blob => matches!(val_type, SQLITE_INTEGER | SQLITE_FLOAT | SQLITE_TEXT),
        ColumnAffinity::Text => {
            matches!(val_type, SQLITE_INTEGER | SQLITE_FLOAT)
                || (!is_lower && val_type == SQLITE_TEXT)
        }
        _ => false,
    }
}

/// Initializes a streaming primary-key full-table scan on `cursor`, used both for `PLAN_FULL_SCAN`
/// and when an equality filter falls back on cross-affinity numeric comparison.
fn start_pk_full_scan(
    cursor: &mut AydbCursor,
    eng: &EngineReader<'_, OsFs>,
    table_id: u16,
    keys_only: bool,
    reverse: bool,
) -> io::Result<()> {
    cursor.keys_only = keys_only;
    eng.scan_range_into_slab_with_cache(
        table_id,
        RangeScanOptions {
            lower: None,
            upper: None,
            reverse,
            limit: CURSOR_STREAM_CHUNK_ROWS,
            keys_only,
        },
        false,
        SlabScanOutput {
            pks: &mut cursor.pks,
            slab: &mut cursor.slab,
            spans: &mut cursor.spans,
            lazy_blobs: Some(&mut cursor.lazy_blobs),
        },
    )?;
    if cursor.row_count() >= CURSOR_STREAM_CHUNK_ROWS {
        if let Some(last_pk) = cursor.pks.last().cloned() {
            cursor.streaming_active = true;
            cursor.stream_lower = None;
            cursor.stream_upper = None;
            cursor.stream_reverse = reverse;
            cursor.stream_last_pk = Some(last_pk);
        }
    }
    Ok(())
}

/// Encodes `val` under `(affinity, collation)` if it produces a usable range bound, returning
/// `Ok(None)` for cross-affinity scans (leave unbounded) or `Err(())` for `NULL`/`NaN` bounds
/// (unsatisfiable empty range).
#[inline]
fn encode_range_bound_key(
    val: SqliteValueRef<'_>,
    affinity: ColumnAffinity,
    collation: ColCollation,
    is_lower: bool,
    is_secondary: bool,
) -> Result<Option<Vec<u8>>, ()> {
    if val.is_null_or_nan() {
        return Err(());
    }
    if is_cross_affinity_range_bound(affinity, val.value_type(), is_lower) {
        return Ok(None);
    }
    Ok(Some(unsafe {
        sqlite_value_to_key_bytes_with_schema(val.as_ptr(), affinity, collation, is_secondary)
    }))
}

/// Converts a SQLite range bound value into a storage-class-tagged key for `PLAN_PK_RANGE`,
/// applying the column's declared [`ColumnAffinity`] and [`ColCollation`].
fn pk_range_bound_to_key(
    val: SqliteValueRef<'_>,
    pk_affinity: ColumnAffinity,
    pk_collation: ColCollation,
    is_lower: bool,
    is_inclusive: bool,
) -> Result<Option<(Vec<u8>, bool)>, ()> {
    let Some(key) = encode_range_bound_key(val, pk_affinity, pk_collation, is_lower, false)? else {
        return Ok(None);
    };

    // If the bound is an exact integer `q` (either `9`-byte `[0x10, q]` or `17`-byte `[0x10, q, 0..0]`),
    // ensure both `9`-byte `Integer(q)` and `17`-byte `Float(q.0)` are handled identically:
    // - Lower inclusive (`>= q`): start at 9-byte `[0x10, q]` (inclusive) -> includes both!
    // - Lower exclusive (`> q`): start after 17-byte `[0x10, q, 0..0]` (exclusive) -> excludes both!
    // - Upper inclusive (`<= q`): end at 17-byte `[0x10, q, 0..0]` (inclusive) -> includes both!
    // - Upper exclusive (`< q`): end before 9-byte `[0x10, q]` (exclusive) -> excludes both!
    if key.first().copied() == Some(crate::vtab::KEY_TAG_INTEGER) {
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

/// Converts a SQLite range bound value into a full secondary-index key bound for
/// `PLAN_SECONDARY_RANGE`.
fn sec_range_bound_to_key(
    val: SqliteValueRef<'_>,
    table_id: u16,
    index_id: u16,
    sec_affinity: ColumnAffinity,
    sec_collation: ColCollation,
    is_lower: bool,
    is_inclusive: bool,
) -> Result<Option<(Vec<u8>, bool)>, ()> {
    let Some(col_val) = encode_range_bound_key(val, sec_affinity, sec_collation, is_lower, true)?
    else {
        return Ok(None);
    };
    let mut exact_prefix =
        crate::lru::encode_secondary_index_exact_prefix(table_id, index_id, &col_val);

    if (is_lower && !is_inclusive) || (!is_lower && is_inclusive) {
        exact_prefix.push(0xFF);
    }
    Ok(Some((exact_prefix, is_inclusive)))
}

pub(in crate::vtab) unsafe extern "system" fn vtab_open(
    p_vtab: *mut sqlite3_vtab,
    pp_cursor: *mut *mut sqlite3_vtab_cursor,
) -> c_int {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        if p_vtab.is_null() || pp_cursor.is_null() {
            return SQLITE_ERROR;
        }
        let cursor = Box::new(AydbCursor {
            base: sqlite3_vtab_cursor { pVtab: p_vtab },
            snapshot: None,
            slab: Vec::with_capacity(512),
            spans: Vec::with_capacity(16),
            pks: Vec::new(),
            rowids: Vec::new(),
            lazy_blobs: Vec::with_capacity(16),
            lazy_row_buf: Vec::new(),
            lazy_row_pos: None,
            keys_only: false,
            pos: 0,
            prefetch_end: 0,
            streaming_active: false,
            stream_lower: None,
            stream_upper: None,
            stream_reverse: false,
            stream_last_pk: None,
            stream_sec_index_id: None,
            stream_sec_prefix: Vec::new(),
        });
        *pp_cursor = Box::into_raw(cursor) as *mut sqlite3_vtab_cursor;
        SQLITE_OK
    }))
    .unwrap_or(SQLITE_ERROR)
}

pub(in crate::vtab) unsafe extern "system" fn vtab_close(
    p_cursor: *mut sqlite3_vtab_cursor,
) -> c_int {
    catch_unwind(AssertUnwindSafe(|| {
        if !p_cursor.is_null() {
            unsafe {
                drop(Box::from_raw(p_cursor as *mut AydbCursor));
            }
        }
        SQLITE_OK
    }))
    .unwrap_or(SQLITE_ERROR)
}

#[inline]
fn clear_cursor_buffers(cursor: &mut AydbCursor) {
    cursor.clear_chunk_buffers();
    cursor.lazy_row_pos = None;
    cursor.lazy_row_buf.clear();
    if cursor.lazy_row_buf.capacity() > LAZY_ROW_BUF_SHRINK_THRESHOLD {
        cursor.lazy_row_buf.shrink_to(LAZY_ROW_BUF_SHRINK_THRESHOLD);
    }
    cursor.keys_only = false;
    cursor.pos = 0;
    cursor.prefetch_end = 0;
    cursor.streaming_active = false;
    cursor.stream_lower = None;
    cursor.stream_upper = None;
    cursor.stream_reverse = false;
    cursor.stream_last_pk = None;
    cursor.stream_sec_index_id = None;
    cursor.stream_sec_prefix.clear();
}

struct ExactIndexLookup {
    col_idx: usize,
    index_id: u16,
    keys_only: bool,
    is_unique: bool,
}

fn execute_exact_index_lookup(
    cursor: &mut AydbCursor,
    vtab: &AydbVTab,
    eng: &EngineReader<'_, OsFs>,
    spec: ExactIndexLookup,
    val: SqliteValueRef<'_>,
) -> c_int {
    if val.is_null_or_nan() {
        return SQLITE_OK;
    }
    let val_type = val.value_type();
    cursor.keys_only = spec.keys_only;
    let col_aff = vtab
        .col_affinities
        .get(spec.col_idx)
        .copied()
        .unwrap_or(ColumnAffinity::Blob);
    let col_coll = vtab
        .col_collations
        .get(spec.col_idx)
        .copied()
        .unwrap_or(ColCollation::Binary);
    if is_cross_affinity_eq_search(col_aff, val_type) {
        if let Err(e) = start_pk_full_scan(cursor, eng, vtab.table_id, spec.keys_only, false) {
            return io_err_to_sqlite_rc(&e);
        }
        return SQLITE_OK;
    }
    let encoded =
        unsafe { sqlite_value_to_key_bytes_with_schema(val.as_ptr(), col_aff, col_coll, true) };
    let lookup_key = if spec.is_unique {
        let canon = unique_key::canon_unique_key(&encoded);
        unique_key::unique_index_component(&canon)
    } else {
        encoded
    };
    let prefix =
        crate::lru::encode_secondary_index_exact_prefix(vtab.table_id, spec.index_id, &lookup_key);
    let next_last_sec = match eng.scan_secondary_index_into_slab_with_cache(
        vtab.table_id,
        spec.index_id,
        SecondaryScanOptions {
            prefix: &prefix,
            lower_sec_key: (&prefix, true),
            upper_sec_key: None,
            limit: CURSOR_STREAM_CHUNK_ROWS,
            keys_only: spec.keys_only,
        },
        true,
        SlabScanOutput {
            pks: &mut cursor.pks,
            slab: &mut cursor.slab,
            spans: &mut cursor.spans,
            lazy_blobs: Some(&mut cursor.lazy_blobs),
        },
    ) {
        Ok(last_opt) => last_opt,
        Err(e) => return io_err_to_sqlite_rc(&e),
    };
    if let Some(last_sec_key) = next_last_sec {
        cursor.streaming_active = true;
        cursor.stream_sec_index_id = Some(spec.index_id);
        cursor.stream_sec_prefix = prefix;
        cursor.stream_upper = None;
        cursor.stream_last_pk = Some(last_sec_key);
    }
    SQLITE_OK
}

fn filter_pk_point(
    cursor: &mut AydbCursor,
    vtab: &AydbVTab,
    eng: &EngineReader<'_, OsFs>,
    val: SqliteValueRef<'_>,
    by_rowid: bool,
    keys_only: bool,
) -> c_int {
    if val.is_null_or_nan() {
        return SQLITE_OK;
    }
    let val_type = val.value_type();
    cursor.keys_only = keys_only;
    if by_rowid {
        if let Some(rid) = unsafe { extract_lossless_rowid_arg(val.as_ptr()) } {
            match resolve_pk_for_rowid(
                vtab,
                rid,
                |t, pk| eng.row_exists(t, pk),
                |t, rid| eng.pk_for_rowid(t, rid),
            ) {
                Ok(Some(mapped_pk)) => {
                    match point_fetch(
                        eng,
                        vtab.table_id,
                        &mapped_pk,
                        keys_only,
                        &mut cursor.slab,
                        &mut cursor.spans,
                    ) {
                        Ok(true) => {
                            cursor.pks.push(mapped_pk);
                            cursor.lazy_blobs.push(None);
                        }
                        Ok(false) => {}
                        Err(e) => return io_err_to_sqlite_rc(&e),
                    }
                }
                Ok(None) => {}
                Err(e) => return io_err_to_sqlite_rc(&e),
            }
        }
        return SQLITE_OK;
    }

    if is_cross_affinity_eq_search(vtab.pk_affinity, val_type) {
        if let Err(e) = start_pk_full_scan(cursor, eng, vtab.table_id, keys_only, false) {
            return io_err_to_sqlite_rc(&e);
        }
        return SQLITE_OK;
    }
    let pk_bytes = unsafe {
        sqlite_value_to_key_bytes_with_schema(
            val.as_ptr(),
            vtab.pk_affinity,
            vtab.pk_collation,
            false,
        )
    };
    let found = match point_fetch(
        eng,
        vtab.table_id,
        &pk_bytes,
        keys_only,
        &mut cursor.slab,
        &mut cursor.spans,
    ) {
        Ok(b) => b,
        Err(e) => return io_err_to_sqlite_rc(&e),
    };
    if found {
        cursor.pks.push(pk_bytes);
        cursor.lazy_blobs.push(None);
    // An `INTEGER PRIMARY KEY` column ignores fractional floats when SQLite seeks a PK value
    // as a rowid (`SeekRowid` uses the strict open interval `(-2^63, 2^63)`), so a 17-byte
    // `-9223372036854775808.0` probe must not match a stored 9-byte `i64::MIN`; `INT PRIMARY KEY`
    // (`!pk_rowid_alias`) and 9-byte `i64::MIN` searches still check both numeric duals.
    } else if !(vtab.pk_rowid_alias && pk_bytes.len() == 17)
        && needs_numeric_alt_probe(&pk_bytes, vtab.pk_affinity)
    {
        if let Some(alt) = build_numeric_alt_key(&pk_bytes) {
            match point_fetch(
                eng,
                vtab.table_id,
                &alt,
                keys_only,
                &mut cursor.slab,
                &mut cursor.spans,
            ) {
                Ok(true) => {
                    cursor.pks.push(alt);
                    cursor.lazy_blobs.push(None);
                }
                Ok(false) => {}
                Err(e) => return io_err_to_sqlite_rc(&e),
            }
        }
    }
    SQLITE_OK
}

fn filter_secondary_range(
    cursor: &mut AydbCursor,
    vtab: &AydbVTab,
    eng: &EngineReader<'_, OsFs>,
    index_id: u16,
    range: RangePlan,
    args: &[SqliteValueRef<'_>],
) -> c_int {
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

    let mut arg_pos = 0usize;
    let parsed_lower = match (range.lower_inclusive, args.get(arg_pos)) {
        (Some(inc), Some(&val)) => {
            arg_pos += 1;
            match sec_range_bound_to_key(val, vtab.table_id, index_id, sec_aff, sec_coll, true, inc)
            {
                Ok(bound) => bound,
                Err(()) => return SQLITE_OK,
            }
        }
        _ => None,
    };
    let has_user_lower = parsed_lower.is_some();
    let lower_bound = parsed_lower.or_else(|| {
        let mut min_non_null = Vec::with_capacity(6);
        min_non_null.extend_from_slice(&crate::lru::encode_secondary_index_prefix(
            vtab.table_id,
            index_id,
        ));
        min_non_null.push(KEY_TAG_NULL + 1);
        Some((min_non_null, true))
    });
    let upper_bound = match (range.upper_inclusive, args.get(arg_pos)) {
        (Some(inc), Some(&val)) => {
            match sec_range_bound_to_key(
                val,
                vtab.table_id,
                index_id,
                sec_aff,
                sec_coll,
                false,
                inc,
            ) {
                Ok(bound) => bound,
                Err(()) => return SQLITE_OK,
            }
        }
        _ => None,
    };

    cursor.keys_only = range.keys_only;
    let idx_prefix = crate::lru::encode_secondary_index_prefix(vtab.table_id, index_id);
    let (lo_slice, lo_inc) = lower_bound
        .as_ref()
        .map(|(b, inc)| (b.as_slice(), *inc))
        .unwrap_or((&idx_prefix, true));
    let next_last_sec = match eng.scan_secondary_index_into_slab_with_cache(
        vtab.table_id,
        index_id,
        SecondaryScanOptions {
            prefix: &idx_prefix,
            lower_sec_key: (lo_slice, lo_inc),
            upper_sec_key: upper_bound.as_ref().map(|(b, inc)| (b.as_slice(), *inc)),
            limit: CURSOR_STREAM_CHUNK_ROWS,
            keys_only: range.keys_only,
        },
        has_user_lower && upper_bound.is_some(),
        SlabScanOutput {
            pks: &mut cursor.pks,
            slab: &mut cursor.slab,
            spans: &mut cursor.spans,
            lazy_blobs: Some(&mut cursor.lazy_blobs),
        },
    ) {
        Ok(last_opt) => last_opt,
        Err(e) => return io_err_to_sqlite_rc(&e),
    };
    if let Some(last_sec_key) = next_last_sec {
        cursor.streaming_active = true;
        cursor.stream_sec_index_id = Some(index_id);
        cursor.stream_sec_prefix.extend_from_slice(&idx_prefix);
        cursor.stream_upper = upper_bound;
        cursor.stream_last_pk = Some(last_sec_key);
    }
    SQLITE_OK
}

fn filter_pk_range(
    cursor: &mut AydbCursor,
    vtab: &AydbVTab,
    eng: &EngineReader<'_, OsFs>,
    range: RangePlan,
    args: &[SqliteValueRef<'_>],
) -> c_int {
    let mut arg_pos = 0usize;
    let lower_bound = match (range.lower_inclusive, args.get(arg_pos)) {
        (Some(inc), Some(&val)) => {
            arg_pos += 1;
            match pk_range_bound_to_key(val, vtab.pk_affinity, vtab.pk_collation, true, inc) {
                Ok(bound) => bound,
                Err(()) => return SQLITE_OK,
            }
        }
        _ => None,
    };
    let upper_bound = match (range.upper_inclusive, args.get(arg_pos)) {
        (Some(inc), Some(&val)) => {
            match pk_range_bound_to_key(val, vtab.pk_affinity, vtab.pk_collation, false, inc) {
                Ok(bound) => bound,
                Err(()) => return SQLITE_OK,
            }
        }
        _ => None,
    };
    cursor.keys_only = range.keys_only;
    if let Err(e) = eng.scan_range_into_slab_with_cache(
        vtab.table_id,
        RangeScanOptions {
            lower: lower_bound.as_ref().map(|(b, inc)| (b.as_slice(), *inc)),
            upper: upper_bound.as_ref().map(|(b, inc)| (b.as_slice(), *inc)),
            reverse: range.reverse,
            limit: CURSOR_STREAM_CHUNK_ROWS,
            keys_only: range.keys_only,
        },
        lower_bound.is_some() && upper_bound.is_some(),
        SlabScanOutput {
            pks: &mut cursor.pks,
            slab: &mut cursor.slab,
            spans: &mut cursor.spans,
            lazy_blobs: Some(&mut cursor.lazy_blobs),
        },
    ) {
        return io_err_to_sqlite_rc(&e);
    }
    if cursor.row_count() >= CURSOR_STREAM_CHUNK_ROWS {
        if let Some(last_pk) = cursor.pks.last().cloned() {
            cursor.streaming_active = true;
            cursor.stream_lower = lower_bound;
            cursor.stream_upper = upper_bound;
            cursor.stream_reverse = range.reverse;
            cursor.stream_last_pk = Some(last_pk);
        }
    }
    SQLITE_OK
}

fn execute_vtab_filter(
    cursor: &mut AydbCursor,
    vtab: &AydbVTab,
    eng: &EngineReader<'_, OsFs>,
    idx_num: c_int,
    args: &[SqliteValueRef<'_>],
) -> c_int {
    clear_cursor_buffers(cursor);
    match DecodedPlan::decode(idx_num) {
        DecodedPlan::PkPoint {
            by_rowid,
            keys_only,
        } => {
            let Some(&val) = args.first() else {
                return SQLITE_ERROR;
            };
            filter_pk_point(cursor, vtab, eng, val, by_rowid, keys_only)
        }
        DecodedPlan::SecondaryEq {
            index_id,
            keys_only,
        } => {
            let Some(&val) = args.first() else {
                return SQLITE_ERROR;
            };
            let sec_col_idx = vtab
                .secondary_indexes
                .iter()
                .find(|&&(_, id)| id == index_id)
                .map(|&(c, _)| c)
                .unwrap_or(0);
            execute_exact_index_lookup(
                cursor,
                vtab,
                eng,
                ExactIndexLookup {
                    col_idx: sec_col_idx,
                    index_id,
                    keys_only,
                    is_unique: false,
                },
                val,
            )
        }
        DecodedPlan::UniqueEq { col_idx, keys_only } => {
            let Some(&val) = args.first() else {
                return SQLITE_ERROR;
            };
            let col_idx = col_idx as usize;
            let want = vtab.unique_def.marker_value();
            let marker_ok = match eng.unique_marker(vtab.table_id) {
                Ok(found) => found.as_deref() == Some(want.as_slice()),
                Err(e) => return io_err_to_sqlite_rc(&e),
            };
            if !marker_ok {
                vtab.unique_ready.invalidate();
                if let Err(e) = start_pk_full_scan(cursor, eng, vtab.table_id, keys_only, false) {
                    return io_err_to_sqlite_rc(&e);
                }
                SQLITE_OK
            } else {
                let index_id = unique_key::unique_index_id(col_idx);
                execute_exact_index_lookup(
                    cursor,
                    vtab,
                    eng,
                    ExactIndexLookup {
                        col_idx,
                        index_id,
                        keys_only,
                        is_unique: true,
                    },
                    val,
                )
            }
        }
        DecodedPlan::SecondaryRange { index_id, range } => {
            filter_secondary_range(cursor, vtab, eng, index_id, range, args)
        }
        DecodedPlan::PkRange(range) => filter_pk_range(cursor, vtab, eng, range, args),
        DecodedPlan::FullScan { keys_only, reverse } => {
            if let Err(e) = start_pk_full_scan(cursor, eng, vtab.table_id, keys_only, reverse) {
                return io_err_to_sqlite_rc(&e);
            }
            SQLITE_OK
        }
    }
}

pub(in crate::vtab) unsafe extern "system" fn vtab_filter(
    p_cursor: *mut sqlite3_vtab_cursor,
    idx_num: c_int,
    _idx_str: *const c_char,
    argc: c_int,
    argv: *mut *mut sqlite3_value,
) -> c_int {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        if p_cursor.is_null() {
            return SQLITE_ERROR;
        }
        let cursor = &mut *(p_cursor as *mut AydbCursor);
        let vtab = &*(cursor.base.pVtab as *const AydbVTab);
        let args = SqliteValueRef::slice_from_argv(argv, argc);
        clear_cursor_buffers(cursor);
        cursor.snapshot = None;

        let conn_id = vtab.db as usize;
        let snapshot = {
            let guard = match vtab.engine.read() {
                Ok(g) => g,
                Err(_) => return SQLITE_ERROR,
            };
            if !guard.is_vtab_token_active(vtab.table_id, vtab.reg_token, conn_id) {
                set_vtab_err_msg(vtab.base.get(), STALE_VTAB_ERR);
                return SQLITE_ERROR;
            }
            let snapshot = match vtab.read_session.for_cursor_locked(&guard, conn_id) {
                Ok(s) => s,
                Err(error) => return io_err_to_sqlite_rc(&error),
            };
            let need_exclusive = match snapshot.as_ref() {
                Some(_) => guard.snapshot_read_needs_exclusive(vtab.table_id),
                None => guard.vtab_read_needs_exclusive(conn_id, vtab.table_id),
            };
            if !need_exclusive {
                let rd = match snapshot.as_ref() {
                    Some(tok) => guard.reader_for_snapshot(tok),
                    None => guard.reader_for_conn(conn_id),
                };
                let rc = execute_vtab_filter(cursor, vtab, &rd, idx_num, args);
                if !rd.needs_exclusive_retry() {
                    cursor.snapshot = snapshot;
                    return rc;
                }
            }
            snapshot
        };

        let mut guard = match vtab.engine.write() {
            Ok(g) => g,
            Err(_) => return SQLITE_ERROR,
        };
        if snapshot.is_none() && !guard.is_vtab_token_active(vtab.table_id, vtab.reg_token, conn_id)
        {
            set_vtab_err_msg(vtab.base.get(), STALE_VTAB_ERR);
            return SQLITE_ERROR;
        }
        let rc = match snapshot.as_ref() {
            Some(tok) => guard.with_snapshot_read(tok, |rd| {
                execute_vtab_filter(cursor, vtab, rd, idx_num, args)
            }),
            None => guard.try_with_committed_read_for_conn(conn_id, |rd| {
                execute_vtab_filter(cursor, vtab, rd, idx_num, args)
            }),
        };
        let rc = match rc {
            Ok(rc) => rc,
            Err(e) => return io_err_to_sqlite_rc(&e),
        };
        cursor.snapshot = snapshot;
        rc
    }))
    .unwrap_or(SQLITE_ERROR)
}

fn refill_streaming_chunk(cursor: &mut AydbCursor, vtab: &AydbVTab, last_pk: Vec<u8>) -> c_int {
    let conn_id = vtab.db as usize;
    let keys_only = cursor.keys_only;

    if let Some(sec_index_id) = cursor.stream_sec_index_id {
        let upper_ref = cursor
            .stream_upper
            .as_ref()
            .map(|(b, inc)| (b.as_slice(), *inc));
        let sec_prefix = &cursor.stream_sec_prefix;
        let pks = &mut cursor.pks;
        let slab = &mut cursor.slab;
        let spans = &mut cursor.spans;
        let rowids = &mut cursor.rowids;
        let lazy_blobs = &mut cursor.lazy_blobs;
        let mut next_last_sec: Option<Vec<u8>> = None;
        let rc = match with_snapshot_read_access(
            &vtab.engine,
            conn_id,
            vtab.table_id,
            cursor.snapshot.as_ref(),
            |eng| {
                clear_chunk_vecs(slab, spans, pks, rowids, lazy_blobs);
                match eng.scan_secondary_index_into_slab_with_cache(
                    vtab.table_id,
                    sec_index_id,
                    SecondaryScanOptions {
                        prefix: sec_prefix,
                        lower_sec_key: (last_pk.as_slice(), false),
                        upper_sec_key: upper_ref,
                        limit: CURSOR_STREAM_CHUNK_ROWS,
                        keys_only,
                    },
                    false,
                    SlabScanOutput {
                        pks,
                        slab,
                        spans,
                        lazy_blobs: Some(lazy_blobs),
                    },
                ) {
                    Ok(last_opt) => {
                        next_last_sec = last_opt;
                        SQLITE_OK
                    }
                    Err(e) => io_err_to_sqlite_rc(&e),
                }
            },
        ) {
            Ok(rc) => rc,
            Err(e) => {
                cursor.streaming_active = false;
                return io_err_to_sqlite_rc(&e);
            }
        };
        if rc != SQLITE_OK {
            cursor.streaming_active = false;
            return rc;
        }
        if let Some(next_key) = next_last_sec {
            cursor.stream_last_pk = Some(next_key);
        } else {
            cursor.streaming_active = false;
        }
        return SQLITE_OK;
    }

    let reverse = cursor.stream_reverse;
    let lower_ref = if reverse {
        cursor
            .stream_lower
            .as_ref()
            .map(|(b, inc)| (b.as_slice(), *inc))
    } else {
        Some((last_pk.as_slice(), false))
    };
    let upper_ref = if reverse {
        Some((last_pk.as_slice(), false))
    } else {
        cursor
            .stream_upper
            .as_ref()
            .map(|(b, inc)| (b.as_slice(), *inc))
    };
    let pks = &mut cursor.pks;
    let slab = &mut cursor.slab;
    let spans = &mut cursor.spans;
    let rowids = &mut cursor.rowids;
    let lazy_blobs = &mut cursor.lazy_blobs;

    let rc = match with_snapshot_read_access(
        &vtab.engine,
        conn_id,
        vtab.table_id,
        cursor.snapshot.as_ref(),
        |eng| {
            clear_chunk_vecs(slab, spans, pks, rowids, lazy_blobs);
            if let Err(e) = eng.scan_range_into_slab_with_cache(
                vtab.table_id,
                RangeScanOptions {
                    lower: lower_ref,
                    upper: upper_ref,
                    reverse,
                    limit: CURSOR_STREAM_CHUNK_ROWS,
                    keys_only,
                },
                false,
                SlabScanOutput {
                    pks,
                    slab,
                    spans,
                    lazy_blobs: Some(lazy_blobs),
                },
            ) {
                return io_err_to_sqlite_rc(&e);
            }
            SQLITE_OK
        },
    ) {
        Ok(rc) => rc,
        Err(e) => {
            cursor.streaming_active = false;
            return io_err_to_sqlite_rc(&e);
        }
    };
    if rc != SQLITE_OK {
        cursor.streaming_active = false;
        return rc;
    }
    if cursor.row_count() >= CURSOR_STREAM_CHUNK_ROWS {
        if let Some(next_last_pk) = cursor.pks.last().cloned() {
            cursor.stream_last_pk = Some(next_last_pk);
        } else {
            cursor.streaming_active = false;
        }
    } else {
        cursor.streaming_active = false;
    }
    SQLITE_OK
}

pub(in crate::vtab) unsafe extern "system" fn vtab_next(
    p_cursor: *mut sqlite3_vtab_cursor,
) -> c_int {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        if p_cursor.is_null() {
            return SQLITE_ERROR;
        }
        let cursor = &mut *(p_cursor as *mut AydbCursor);
        cursor.pos += 1;
        cursor.lazy_row_pos = None;
        cursor.lazy_row_buf.clear();
        if cursor.pos >= cursor.row_count() && cursor.streaming_active {
            let Some(last_pk) = cursor.stream_last_pk.take() else {
                cursor.streaming_active = false;
                return SQLITE_OK;
            };
            let vtab = &*(cursor.base.pVtab as *const AydbVTab);
            cursor.clear_chunk_buffers();
            cursor.pos = 0;
            cursor.prefetch_end = 0;
            return refill_streaming_chunk(cursor, vtab, last_pk);
        }
        SQLITE_OK
    }))
    .unwrap_or(SQLITE_ERROR)
}

pub(in crate::vtab) unsafe extern "system" fn vtab_eof(
    p_cursor: *mut sqlite3_vtab_cursor,
) -> c_int {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        if p_cursor.is_null() {
            return 1;
        }
        let cursor = &*(p_cursor as *const AydbCursor);
        if cursor.pos >= cursor.row_count() {
            1
        } else {
            0
        }
    }))
    .unwrap_or(1)
}

/// Resolves the SQLite `rowid` for `cursor.pos`, batch-populating `cursor.rowids` once per chunk
/// for non-`INTEGER PRIMARY KEY` tables instead of reacquiring the engine lock on every row.
fn resolve_cursor_rowid(cursor: &mut AydbCursor, vtab: &AydbVTab) -> io::Result<i64> {
    if vtab.pk_rowid_alias {
        if let Some(pk_bytes) = cursor.pks.get(cursor.pos) {
            if let Some(id) = decode_i64_tagged_key(pk_bytes) {
                return Ok(id);
            }
        }
    } else if !cursor.pks.is_empty() {
        if cursor.rowids.len() != cursor.pks.len() {
            with_snapshot_read_access(
                &vtab.engine,
                vtab.db as usize,
                vtab.table_id,
                cursor.snapshot.as_ref(),
                |eng| eng.rowids_for_pks(vtab.table_id, &cursor.pks, &mut cursor.rowids),
            )??;
        }
        if let Some(Some(rid)) = cursor.rowids.get(cursor.pos) {
            return Ok(*rid);
        }
    }
    if let Some(pk_bytes) = cursor.pks.get(cursor.pos) {
        extract_rowid_from_pk_bytes(pk_bytes, vtab, cursor.snapshot.as_ref())
    } else if cursor.lazy_row_pos == Some(cursor.pos) {
        extract_rowid_from_row(&cursor.lazy_row_buf, vtab, cursor.snapshot.as_ref())
    } else {
        let (start, end) = cursor.spans[cursor.pos];
        extract_rowid_from_row(&cursor.slab[start..end], vtab, cursor.snapshot.as_ref())
    }
}

fn project_keys_only_column(
    cursor: &mut AydbCursor,
    vtab: &AydbVTab,
    ctx: &SqliteContextRef<'_>,
    i_col: c_int,
) -> c_int {
    if cursor.pos >= cursor.pks.len() {
        return SQLITE_ERROR;
    }
    if i_col < 0 {
        let rid = match resolve_cursor_rowid(cursor, vtab) {
            Ok(rid) => rid,
            Err(e) => return io_err_to_sqlite_rc(&e),
        };
        ctx.set_i64(rid);
        return SQLITE_OK;
    }
    if i_col as usize != vtab.pk_col_idx {
        return SQLITE_ERROR;
    }
    let pk_bytes = &cursor.pks[cursor.pos];
    if is_canonical_float_zero_key(pk_bytes) {
        let pk_copy = pk_bytes.clone();
        let row_res = with_snapshot_read_access(
            &vtab.engine,
            vtab.db as usize,
            vtab.table_id,
            cursor.snapshot.as_ref(),
            |eng| {
                cursor.lazy_row_buf.clear();
                let mut spans = Vec::with_capacity(1);
                let found = eng.get_row_into(
                    vtab.table_id,
                    &pk_copy,
                    &mut cursor.lazy_row_buf,
                    &mut spans,
                )?;
                Ok((found, spans))
            },
        );
        return match row_res {
            Ok(Ok((true, spans))) => {
                let (start, end) = spans
                    .first()
                    .copied()
                    .unwrap_or((0, cursor.lazy_row_buf.len()));
                let res = unsafe {
                    project_column_zero_copy(
                        &cursor.lazy_row_buf[start..end],
                        i_col as usize,
                        ctx.as_ptr(),
                    )
                };
                ctx.finish_io_result(res, io_err_to_sqlite_rc)
            }
            Ok(Ok((false, _))) => {
                let res = unsafe { project_key_zero_copy(&pk_copy, ctx.as_ptr()) };
                ctx.finish_io_result(res, io_err_to_sqlite_rc)
            }
            Ok(Err(e)) | Err(e) if e.kind() == io::ErrorKind::InvalidData => {
                let res = unsafe { project_key_zero_copy(&pk_copy, ctx.as_ptr()) };
                ctx.finish_io_result(res, io_err_to_sqlite_rc)
            }
            Ok(Err(e)) | Err(e) => ctx.finish_io_result(Err(e), io_err_to_sqlite_rc),
        };
    }
    let res = unsafe { project_key_zero_copy(pk_bytes, ctx.as_ptr()) };
    ctx.finish_io_result(res, io_err_to_sqlite_rc)
}

fn materialize_column_row<'a>(
    cursor: &'a mut AydbCursor,
    vtab: &AydbVTab,
    ctx: &SqliteContextRef<'_>,
) -> Result<&'a [u8], c_int> {
    if cursor.lazy_row_pos == Some(cursor.pos) {
        return Ok(&cursor.lazy_row_buf);
    }
    if let Some(lazy_blob) = cursor
        .lazy_blobs
        .get(cursor.pos)
        .and_then(|o| o.as_ref())
        .cloned()
    {
        let Some(pk_bytes) = cursor.pks.get(cursor.pos) else {
            return Err(SQLITE_ERROR);
        };
        let conn_id = vtab.db as usize;

        // Prefetch the next window of up to 16 rows only once the previous window is used up,
        // so the cache is not re-checked for the same blobs on every row.
        let mut upcoming: Vec<(&[u8], &LazyBlob<OsFile>)> = Vec::new();
        if cursor.pos >= cursor.prefetch_end {
            let end = (cursor.pos + 16).min(cursor.lazy_blobs.len());
            for i in cursor.pos..end {
                if let Some(Some(lb)) = cursor.lazy_blobs.get(i) {
                    if let Some(pk) = cursor.pks.get(i) {
                        upcoming.push((pk.as_slice(), lb));
                    }
                }
            }
            cursor.prefetch_end = end;
        }

        let deref_res = match with_snapshot_read_access(
            &vtab.engine,
            conn_id,
            vtab.table_id,
            cursor.snapshot.as_ref(),
            |eng| {
                if upcoming.len() > 1 {
                    let _ = eng.prefetch_lazy_blobs(vtab.table_id, &upcoming);
                }
                cursor.lazy_row_buf.clear();
                eng.dereference_lazy_blob_into(
                    vtab.table_id,
                    pk_bytes,
                    &lazy_blob,
                    &mut cursor.lazy_row_buf,
                )
            },
        ) {
            Ok(res) => res,
            Err(e) => {
                cursor.lazy_row_buf.clear();
                let rc = io_err_to_sqlite_rc(&e);
                ctx.set_error_code(rc);
                return Err(rc);
            }
        };
        return match deref_res {
            Ok(Some((start, end))) => {
                debug_assert_eq!(start, 0);
                cursor.lazy_row_pos = Some(cursor.pos);
                cursor.lazy_blobs[cursor.pos] = None;
                Ok(&cursor.lazy_row_buf[start..end])
            }
            Ok(None) => {
                cursor.lazy_row_buf.clear();
                ctx.set_error_code(SQLITE_CORRUPT);
                Err(SQLITE_CORRUPT)
            }
            Err(e) => {
                cursor.lazy_row_buf.clear();
                let rc = io_err_to_sqlite_rc(&e);
                ctx.set_error_code(rc);
                Err(rc)
            }
        };
    }
    let (start, end) = cursor.spans[cursor.pos];
    Ok(&cursor.slab[start..end])
}

pub(in crate::vtab) unsafe extern "system" fn vtab_column(
    p_cursor: *mut sqlite3_vtab_cursor,
    p_ctx: *mut sqlite3_context,
    i_col: c_int,
) -> c_int {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        if p_cursor.is_null() || p_ctx.is_null() {
            return SQLITE_ERROR;
        }
        let cursor = &mut *(p_cursor as *mut AydbCursor);
        let vtab = &*(cursor.base.pVtab as *const AydbVTab);
        let ctx = SqliteContextRef::from_raw(p_ctx);

        if cursor.keys_only {
            return project_keys_only_column(cursor, vtab, &ctx, i_col);
        }
        if cursor.pos >= cursor.spans.len() {
            return SQLITE_ERROR;
        }

        // Fast path 1: Rowid lookup via xColumn(p_cursor, p_ctx, -1)
        if i_col < 0 {
            let rid = match resolve_cursor_rowid(cursor, vtab) {
                Ok(rid) => rid,
                Err(e) => return io_err_to_sqlite_rc(&e),
            };
            ctx.set_i64(rid);
            return SQLITE_OK;
        }

        // Fast path 2: Primary Key column projection with Binary collation directly from cursor.pks
        if i_col as usize == vtab.pk_col_idx && vtab.pk_collation == ColCollation::Binary {
            if let Some(pk_bytes) = cursor.pks.get(cursor.pos) {
                if !is_canonical_float_zero_key(pk_bytes) {
                    match project_key_zero_copy(pk_bytes, ctx.as_ptr()) {
                        Ok(()) => return SQLITE_OK,
                        Err(e) if e.kind() == io::ErrorKind::FileTooLarge => {
                            return SQLITE_TOOBIG;
                        }
                        Err(e) if e.kind() == io::ErrorKind::InvalidData => {
                            let rc = io_err_to_sqlite_rc(&e);
                            ctx.set_error_code(rc);
                            return rc;
                        }
                        Err(_) => {}
                    }
                }
            }
        }

        let row_bytes = match materialize_column_row(cursor, vtab, &ctx) {
            Ok(bytes) => bytes,
            Err(rc) => return rc,
        };
        let res = project_column_zero_copy(row_bytes, i_col as usize, ctx.as_ptr());

        #[cfg(test)]
        {
            let slab_len = cursor.slab.len();
            CURSOR_SLAB_MAX_LEN.with(|m| m.set(m.get().max(slab_len)));
            let lazy_len = cursor.lazy_row_buf.len();
            CURSOR_LAZY_ROW_BUF_MAX_LEN.with(|m| m.set(m.get().max(lazy_len)));
        }

        ctx.finish_io_result(res, io_err_to_sqlite_rc)
    }))
    .unwrap_or(SQLITE_ERROR)
}

pub(in crate::vtab) unsafe extern "system" fn vtab_rowid(
    p_cursor: *mut sqlite3_vtab_cursor,
    p_rowid: *mut sqlite3_int64,
) -> c_int {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        if p_cursor.is_null() || p_rowid.is_null() {
            return SQLITE_ERROR;
        }
        let cursor = &mut *(p_cursor as *mut AydbCursor);
        let vtab = &*(cursor.base.pVtab as *const AydbVTab);
        if cursor.pos >= cursor.row_count() {
            return SQLITE_ERROR;
        }
        *p_rowid = match resolve_cursor_rowid(cursor, vtab) {
            Ok(rid) => rid,
            Err(e) => return io_err_to_sqlite_rc(&e),
        };
        SQLITE_OK
    }))
    .unwrap_or(SQLITE_ERROR)
}
