//! Compact binary row serialization with an $O(1)$ column offset directory,
//! plus memcomparable key conversion for SQLite [`sqlite3_value`] items.

use std::io::{self, ErrorKind};

use crate::lru::{decode_i64_memcomparable, encode_i64_memcomparable};
use crate::vtab::ffi::{
    SQLITE_FLOAT, SQLITE_INTEGER, SQLITE_NULL, SQLITE_TEXT, sqlite_transient, sqlite3_context,
    sqlite3_result_blob, sqlite3_result_double, sqlite3_result_error_toobig, sqlite3_result_int64,
    sqlite3_result_null, sqlite3_result_text, sqlite3_value, sqlite3_value_blob,
    sqlite3_value_bytes, sqlite3_value_double, sqlite3_value_int64, sqlite3_value_text,
    sqlite3_value_type,
};

/// 1-byte key storage-class type tags ordered according to SQLite's comparison rules:
/// `NULL < Number (INTEGER & REAL) < TEXT < BLOB`.
///
/// Unified exact numeric key domain (`0x0E..=0x12`):
/// - `KEY_TAG_FLOAT_LT_I64_MIN` (`0x0E`): `f64` values with `f < -9223372036854775808.0` (`-2^63`),
///   including `-inf` (9 bytes: `[0x0E, encode_f64_memcomparable(f)]`).
/// - `KEY_TAG_INTEGER` (`0x10`): All `i64` values (`9` bytes: `[0x10, encode_i64_memcomparable(i)]`)
///   AND all finite `f64` values in `[-2^63, 2^63)` (`17` bytes:
///   `[0x10, encode_i64_memcomparable(floor(f) as i64), r_bytes]` where `r_bytes` is `[0u8; 8]`
///   for exact-integer floats (`norm == floor(norm)`) or `encode_f64_memcomparable(norm)` for
///   non-integer floats (`norm != floor(norm)`), preserving full IEEE-754 bit identity and
///   sub-ULP ordering without floating-point subtraction cancellation).
///   Under lexicographical slice comparison:
///   - `Integer(q)` (`9` bytes) sorts immediately before `Float(q.0)` (`17` bytes with `r_bytes == [0; 8]`),
///     which sorts before any `Float(q + r)` with `r > 0`, which sorts before `Integer(q + 1)`!
///   - Any two `i64` values compare in exact 64-bit integer order (including `2^53` vs `2^53 + 1`).
///   - Any `i64` and `f64` compare in exact mathematical order (`1.5 < 10`, `-10 < -1.5`).
///   - Both `SqlValue::Integer(q)` and `SqlValue::Float(f)` round-trip losslessly via `from_key_bytes`.
/// - `KEY_TAG_FLOAT_GE_I64_MAX_PLUS_1` (`0x12`): `f64` values with `f >= 9223372036854775808.0` (`2^63`),
///   including `+inf` and `NaN` (9 bytes: `[0x12, encode_f64_memcomparable(f)]`).
pub const KEY_TAG_NULL: u8 = 0x05;
pub const KEY_TAG_FLOAT_LT_I64_MIN: u8 = 0x0E;
pub const KEY_TAG_INTEGER: u8 = 0x10;
pub const KEY_TAG_FLOAT_GE_I64_MAX_PLUS_1: u8 = 0x12;
const LEGACY_KEY_TAG_FLOAT: u8 = 0x18;
pub const KEY_TAG_TEXT: u8 = 0x20;
pub const KEY_TAG_BLOB: u8 = 0x30;

/// Exact `f64` value of `i64::MIN` (`-9223372036854775808.0 == -2.0^63`).
const F64_I64_MIN: f64 = -9_223_372_036_854_775_808.0_f64;
/// Exact `f64` value of `i64::MAX + 1` (`+9223372036854775808.0 == +2.0^63`).
const F64_I64_UPPER_EXCL: f64 = 9_223_372_036_854_775_808.0_f64;

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

/// Built-in SQLite text collating sequences.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ColCollation {
    #[default]
    Binary,
    NoCase,
    RTrim,
}

impl ColCollation {
    /// Parses a collation name (`BINARY`, `NOCASE`, `RTRIM`), returning `None` for unsupported collations.
    pub fn from_name(name: &str) -> Option<Self> {
        let upper = name.trim().to_ascii_uppercase();
        match upper.as_str() {
            "BINARY" => Some(Self::Binary),
            "NOCASE" => Some(Self::NoCase),
            "RTRIM" => Some(Self::RTrim),
            _ => None,
        }
    }

    /// Appends collation-normalized text bytes (`BINARY`, `NOCASE`, or `RTRIM`) into `out`.
    #[inline]
    pub fn push_normalized_text(self, bytes: &[u8], out: &mut Vec<u8>) {
        match self {
            Self::Binary => out.extend_from_slice(bytes),
            Self::NoCase => {
                out.extend(bytes.iter().map(|b| b.to_ascii_lowercase()));
            }
            Self::RTrim => {
                let mut end = bytes.len();
                while end > 0 && bytes[end - 1] == b' ' {
                    end -= 1;
                }
                out.extend_from_slice(&bytes[..end]);
            }
        }
    }
}

/// Checks if a finite `f64` is an exact integer within `[i64::MIN, i64::MAX]`.
#[inline]
pub fn f64_to_exact_i64(f: f64) -> Option<i64> {
    if f.is_finite() && f.fract() == 0.0 && (F64_I64_MIN..F64_I64_UPPER_EXCL).contains(&f) {
        Some(f as i64)
    } else {
        None
    }
}

/// Attempts to parse a string slice as a well-formed SQLite numeric literal (`i64` or finite `f64`).
/// Returns `Some(Ok(i64))` if it is an exact 64-bit integer, `Some(Err(f64))` if it is a valid
/// floating-point number that cannot be represented as `i64`, or `None` if it is not numeric.
pub fn parse_sqlite_numeric_str(s: &str) -> Option<Result<i64, f64>> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Ok(i) = trimmed.parse::<i64>() {
        return Some(Ok(i));
    }
    if let Ok(f) = trimmed.parse::<f64>() {
        if f.is_finite() {
            if let Some(i) = f64_to_exact_i64(f) {
                return Some(Ok(i));
            }
            return Some(Err(f));
        }
    }
    None
}

/// Formats an `f64` into SQLite's canonical text representation (`%.15g`) when coerced by
/// `TEXT` affinity.
#[inline]
pub fn format_sqlite_float_text(f: f64) -> String {
    if f == 0.0 {
        return "0.0".to_string();
    }
    if !f.is_finite() {
        return format!("{f}");
    }
    let abs = f.abs();
    if abs >= 1e15 || abs < 1e-4 {
        let s = format!("{f:.14e}");
        if let Some((mant, exp_str)) = s.split_once('e') {
            let trimmed_mant = mant.trim_end_matches('0');
            let mant_norm = if trimmed_mant.ends_with('.') {
                format!("{trimmed_mant}0")
            } else if !trimmed_mant.contains('.') {
                format!("{trimmed_mant}.0")
            } else {
                trimmed_mant.to_string()
            };
            let exp: i32 = exp_str.parse().unwrap_or(0);
            let sign = if exp >= 0 { '+' } else { '-' };
            let abs_exp = exp.unsigned_abs();
            if abs_exp < 100 {
                return format!("{mant_norm}e{sign}{abs_exp:02}");
            } else {
                return format!("{mant_norm}e{sign}{abs_exp}");
            }
        }
        return s;
    }
    if f.fract() == 0.0 {
        return format!("{f:.1}");
    }
    let log10 = abs.log10().floor() as i32;
    let frac_digits = (14 - log10).clamp(1, 18) as usize;
    let formatted = format!("{f:.frac_digits$}");
    let trimmed = formatted.trim_end_matches('0');
    if trimmed.ends_with('.') {
        format!("{trimmed}0")
    } else {
        trimmed.to_string()
    }
}

/// Encodes an `f64` into the unified exact numeric key domain:
/// - `f < -2^63`: `[0x0E, encode_f64_memcomparable(f)]` (9 bytes).
/// - `-2^63 <= f < 2^63`: `[0x10, encode_i64_memcomparable(q), r_bytes]` (17 bytes),
///   where `r_bytes` is `[0; 8]` if `f` is an exact integer (`norm == fl`), or
///   `encode_f64_memcomparable(norm)` if `norm != fl` (lossless IEEE-754 bit preservation).
/// - `f >= 2^63` or `NaN`: `[0x12, encode_f64_memcomparable(f)]` (9 bytes).
#[inline]
pub fn encode_f64_unified_key(v: f64) -> Vec<u8> {
    let norm = if v == 0.0 { 0.0 } else { v };
    if norm < F64_I64_MIN {
        let mut out = Vec::with_capacity(9);
        out.push(KEY_TAG_FLOAT_LT_I64_MIN);
        out.extend_from_slice(&encode_f64_memcomparable(norm));
        out
    } else if norm < F64_I64_UPPER_EXCL {
        let fl = norm.floor();
        let q = fl as i64;
        let r_bytes = if norm == fl {
            [0u8; 8]
        } else {
            encode_f64_memcomparable(norm)
        };
        let mut out = Vec::with_capacity(17);
        out.push(KEY_TAG_INTEGER);
        out.extend_from_slice(&encode_i64_memcomparable(q));
        out.extend_from_slice(&r_bytes);
        out
    } else {
        let mut out = Vec::with_capacity(9);
        out.push(KEY_TAG_FLOAT_GE_I64_MAX_PLUS_1);
        out.extend_from_slice(&encode_f64_memcomparable(norm));
        out
    }
}

/// Encodes any numeric value (`i64` or `f64`) into a fixed-width standalone/secondary-index
/// numeric key where mathematically equal `i64` and `f64` values (`10` and `10.0`) produce
/// identical bytes (`[0x10, encode_i64_memcomparable(q)]` when `r == 0.0`).
#[inline]
pub fn encode_canonical_numeric_key_from_f64(v: f64) -> Vec<u8> {
    if let Some(i) = f64_to_exact_i64(v) {
        encode_i64_tagged_key(i).to_vec()
    } else {
        encode_f64_unified_key(v)
    }
}

/// Encodes an IEEE-754 `f64` into an 8-byte order-preserving big-endian representation:
/// - Normalizes `-0.0` to `+0.0`.
/// - Positive/zero floats (`sign bit == 0`) flip the sign bit (`bits ^ (1 << 63)`).
/// - Negative floats (`sign bit == 1`) flip all 64 bits (`!bits`).
#[inline]
pub fn encode_f64_memcomparable(v: f64) -> [u8; 8] {
    let bits = if v == 0.0 {
        0.0f64.to_bits()
    } else {
        v.to_bits()
    };
    let transformed = if (bits & (1u64 << 63)) == 0 {
        bits ^ (1u64 << 63)
    } else {
        !bits
    };
    transformed.to_be_bytes()
}

/// Decodes an 8-byte order-preserving big-endian representation back into an IEEE-754 `f64`.
#[inline]
pub fn decode_f64_memcomparable(bytes: [u8; 8]) -> f64 {
    let transformed = u64::from_be_bytes(bytes);
    let bits = if (transformed & (1u64 << 63)) != 0 {
        transformed ^ (1u64 << 63)
    } else {
        !transformed
    };
    f64::from_bits(bits)
}

/// A dynamically typed SQL column value matching SQLite's 5 fundamental storage classes.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue {
    Null,
    Integer(i64),
    Float(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl SqlValue {
    /// Extracts a [`SqlValue`] from a raw SQLite `sqlite3_value*`.
    ///
    /// # Safety
    /// `val` must be a valid non-null `*mut sqlite3_value` provided by SQLite.
    pub unsafe fn from_sqlite_value(val: *mut sqlite3_value) -> Self {
        unsafe {
            match sqlite3_value_type(val) {
                SQLITE_NULL => Self::Null,
                SQLITE_INTEGER => Self::Integer(sqlite3_value_int64(val)),
                SQLITE_FLOAT => Self::Float(sqlite3_value_double(val)),
                SQLITE_TEXT => {
                    let len = sqlite3_value_bytes(val) as usize;
                    let ptr = sqlite3_value_text(val);
                    if ptr.is_null() || len == 0 {
                        Self::Text(String::new())
                    } else {
                        let slice = core::slice::from_raw_parts(ptr, len);
                        Self::Text(String::from_utf8_lossy(slice).into_owned())
                    }
                }
                _ => {
                    let len = sqlite3_value_bytes(val) as usize;
                    let ptr = sqlite3_value_blob(val) as *const u8;
                    if ptr.is_null() || len == 0 {
                        Self::Blob(Vec::new())
                    } else {
                        let slice = core::slice::from_raw_parts(ptr, len);
                        Self::Blob(slice.to_vec())
                    }
                }
            }
        }
    }

    /// Writes this [`SqlValue`] into a SQLite `sqlite3_context*`.
    ///
    /// # Safety
    /// `ctx` must be a valid non-null `*mut sqlite3_context` passed to `xColumn`.
    pub unsafe fn set_sqlite_result(&self, ctx: *mut sqlite3_context) {
        unsafe {
            match self {
                Self::Null => sqlite3_result_null(ctx),
                Self::Integer(v) => sqlite3_result_int64(ctx, *v),
                Self::Float(v) => sqlite3_result_double(ctx, *v),
                Self::Text(s) => {
                    if s.len() > i32::MAX as usize {
                        sqlite3_result_error_toobig(ctx);
                    } else {
                        sqlite3_result_text(
                            ctx,
                            s.as_ptr() as *const _,
                            s.len() as i32,
                            sqlite_transient(),
                        );
                    }
                }
                Self::Blob(b) => {
                    if b.len() > i32::MAX as usize {
                        sqlite3_result_error_toobig(ctx);
                    } else {
                        sqlite3_result_blob(
                            ctx,
                            b.as_ptr() as *const _,
                            b.len() as i32,
                            sqlite_transient(),
                        );
                    }
                }
            }
        }
    }

    /// Encodes this value into a standalone type-tagged memcomparable primary/secondary key component
    /// ordered according to SQLite's storage-class comparison rules (`NULL < Number < TEXT < BLOB`):
    /// - `Null` -> `[0x05]` (1 byte).
    /// - `Integer(i)` -> `[0x10, encode_i64_memcomparable(i)..]` (9 bytes).
    /// - `Float(f)` -> Unified numeric encoding (`0x0E` for `f < -2^63`, `0x10` + 16 bytes for
    ///   `-2^63 <= f < 2^63`, `0x12` for `f >= 2^63`).
    /// - `Text(s)` -> `[0x20, s.as_bytes()..]`.
    /// - `Blob(b)` -> `[0x30, b..]`.
    pub fn to_key_bytes(&self) -> Vec<u8> {
        match self {
            Self::Null => vec![KEY_TAG_NULL],
            Self::Integer(v) => {
                let mut out = Vec::with_capacity(9);
                out.push(KEY_TAG_INTEGER);
                out.extend_from_slice(&encode_i64_memcomparable(*v));
                out
            }
            Self::Float(v) => encode_f64_unified_key(*v),
            Self::Text(s) => {
                let bytes = s.as_bytes();
                let mut out = Vec::with_capacity(1 + bytes.len());
                out.push(KEY_TAG_TEXT);
                out.extend_from_slice(bytes);
                out
            }
            Self::Blob(b) => {
                let mut out = Vec::with_capacity(1 + b.len());
                out.push(KEY_TAG_BLOB);
                out.extend_from_slice(b);
                out
            }
        }
    }

    /// Decodes a type-tagged key slice back into its exact [`SqlValue`].
    pub fn from_key_bytes(key: &[u8]) -> Option<Self> {
        let (&tag, payload) = key.split_first()?;
        match tag {
            KEY_TAG_NULL if payload.is_empty() => Some(Self::Null),
            KEY_TAG_INTEGER if payload.len() == 8 => {
                let arr: [u8; 8] = payload.try_into().ok()?;
                Some(Self::Integer(decode_i64_memcomparable(arr)))
            }
            KEY_TAG_INTEGER if payload.len() == 16 => {
                let q_arr: [u8; 8] = payload[0..8].try_into().ok()?;
                let r_arr: [u8; 8] = payload[8..16].try_into().ok()?;
                let r_bits = u64::from_be_bytes(r_arr);
                if r_bits == 0 {
                    let q = decode_i64_memcomparable(q_arr);
                    Some(Self::Float(q as f64))
                } else {
                    Some(Self::Float(decode_f64_memcomparable(r_arr)))
                }
            }
            KEY_TAG_FLOAT_LT_I64_MIN | KEY_TAG_FLOAT_GE_I64_MAX_PLUS_1 | LEGACY_KEY_TAG_FLOAT
                if payload.len() == 8 =>
            {
                let arr: [u8; 8] = payload.try_into().ok()?;
                Some(Self::Float(decode_f64_memcomparable(arr)))
            }
            KEY_TAG_TEXT => Some(Self::Text(String::from_utf8_lossy(payload).into_owned())),
            KEY_TAG_BLOB => Some(Self::Blob(payload.to_vec())),
            _ => None,
        }
    }
}

/// Encodes a signed `i64` into a 9-byte type-tagged memcomparable key (`[KEY_TAG_INTEGER, be_bytes]`).
#[inline]
pub fn encode_i64_tagged_key(val: i64) -> [u8; 9] {
    let be = encode_i64_memcomparable(val);
    [
        KEY_TAG_INTEGER,
        be[0],
        be[1],
        be[2],
        be[3],
        be[4],
        be[5],
        be[6],
        be[7],
    ]
}

/// Decodes a 9-byte type-tagged integer key back into `Some(i64)`.
#[inline]
pub fn decode_i64_tagged_key(key: &[u8]) -> Option<i64> {
    if key.len() == 9 && key[0] == KEY_TAG_INTEGER {
        let arr: [u8; 8] = key[1..9].try_into().ok()?;
        Some(decode_i64_memcomparable(arr))
    } else {
        None
    }
}

/// Encodes a slice of [`SqlValue`] columns into the compact binary row layout:
/// `[col_count: u16 LE | flags: u16 LE | C * (type_tag: u8, offset: u32 LE) | payloads...]`.
pub fn encode_row(cols: &[SqlValue]) -> Vec<u8> {
    let col_count = cols.len();
    let dir_start = 4;
    let payload_start = dir_start + col_count * 5;

    let mut total_payload = 0usize;
    for c in cols {
        total_payload += match c {
            SqlValue::Null => 0,
            SqlValue::Integer(_) | SqlValue::Float(_) => 8,
            SqlValue::Text(s) => 4 + s.len(),
            SqlValue::Blob(b) => 4 + b.len(),
        };
    }

    let mut buf = vec![0u8; payload_start + total_payload];
    buf[0..2].copy_from_slice(&(col_count as u16).to_le_bytes());
    buf[2..4].copy_from_slice(&0u16.to_le_bytes());

    let mut write_pos = payload_start;
    for (idx, c) in cols.iter().enumerate() {
        let dir_pos = dir_start + idx * 5;
        let offset_u32 = write_pos as u32;
        buf[dir_pos + 1..dir_pos + 5].copy_from_slice(&offset_u32.to_le_bytes());

        match c {
            SqlValue::Null => {
                buf[dir_pos] = 0;
            }
            SqlValue::Integer(v) => {
                buf[dir_pos] = 1;
                buf[write_pos..write_pos + 8].copy_from_slice(&v.to_le_bytes());
                write_pos += 8;
            }
            SqlValue::Float(v) => {
                buf[dir_pos] = 2;
                buf[write_pos..write_pos + 8].copy_from_slice(&v.to_le_bytes());
                write_pos += 8;
            }
            SqlValue::Text(s) => {
                buf[dir_pos] = 3;
                let bytes = s.as_bytes();
                buf[write_pos..write_pos + 4].copy_from_slice(&(bytes.len() as u32).to_le_bytes());
                buf[write_pos + 4..write_pos + 4 + bytes.len()].copy_from_slice(bytes);
                write_pos += 4 + bytes.len();
            }
            SqlValue::Blob(b) => {
                buf[dir_pos] = 4;
                buf[write_pos..write_pos + 4].copy_from_slice(&(b.len() as u32).to_le_bytes());
                buf[write_pos + 4..write_pos + 4 + b.len()].copy_from_slice(b);
                write_pos += 4 + b.len();
            }
        }
    }

    buf
}

/// Encodes a slice of raw `sqlite3_value*` pointers (`argv[2 .. 2 + col_count]`) directly into
/// a compact binary row byte vector without allocating intermediate `SqlValue` structs!
/// Applies each column's declared [`ColumnAffinity`] and [`ColCollation`], and extracts
/// the primary key bytes (`pk_col_idx`) and any secondary index `(index_id, col_key_bytes)`.
pub type DirectEncodedRow = (Vec<u8>, Vec<u8>, Vec<(u16, Vec<u8>)>);

/// If `pk_override` is `Some(auto_id)` (for `INTEGER PRIMARY KEY` when `SQLITE_NULL` is inserted),
/// serializes `auto_id` as an `SQLITE_INTEGER` in the `pk_col_idx` slot.
///
/// # Safety
/// `values` must point to `col_count` valid `*mut sqlite3_value` pointers.
pub unsafe fn encode_sqlite_values_direct(
    values: *mut *mut sqlite3_value,
    col_count: usize,
    pk_col_idx: usize,
    pk_override: Option<i64>,
    col_affinities: &[ColumnAffinity],
    col_collations: &[ColCollation],
    secondary_indexes: &[(usize, u16)],
) -> DirectEncodedRow {
    enum CoercedSlot<'a> {
        Null,
        Int(i64),
        Float(f64),
        TextSlice(&'a [u8]),
        TextOwned(String),
        BlobSlice(&'a [u8]),
    }

    unsafe {
        let dir_start = 4;
        let payload_start = dir_start + col_count * 5;

        let mut slots: Vec<CoercedSlot<'_>> = Vec::with_capacity(col_count);
        let mut total_payload = 0usize;

        for i in 0..col_count {
            if i == pk_col_idx {
                if let Some(auto_id) = pk_override {
                    slots.push(CoercedSlot::Int(auto_id));
                    total_payload += 8;
                    continue;
                }
            }
            let vptr = *values.add(i);
            let aff = col_affinities
                .get(i)
                .copied()
                .unwrap_or(ColumnAffinity::Blob);
            let raw_type = sqlite3_value_type(vptr);
            let slot = match raw_type {
                SQLITE_NULL => CoercedSlot::Null,
                SQLITE_INTEGER => {
                    let v = sqlite3_value_int64(vptr);
                    match aff {
                        ColumnAffinity::Real => CoercedSlot::Float(v as f64),
                        ColumnAffinity::Text => CoercedSlot::TextOwned(v.to_string()),
                        _ => CoercedSlot::Int(v),
                    }
                }
                SQLITE_FLOAT => {
                    let f = sqlite3_value_double(vptr);
                    match aff {
                        ColumnAffinity::Integer | ColumnAffinity::Numeric => {
                            if let Some(i_val) = f64_to_exact_i64(f) {
                                CoercedSlot::Int(i_val)
                            } else {
                                CoercedSlot::Float(f)
                            }
                        }
                        ColumnAffinity::Text => {
                            let ptr = sqlite3_value_text(vptr);
                            let len = sqlite3_value_bytes(vptr).max(0) as usize;
                            if !ptr.is_null() && len > 0 {
                                CoercedSlot::TextSlice(core::slice::from_raw_parts(ptr, len))
                            } else {
                                CoercedSlot::TextOwned(format_sqlite_float_text(f))
                            }
                        }
                        _ => CoercedSlot::Float(f),
                    }
                }
                SQLITE_TEXT => {
                    let len = sqlite3_value_bytes(vptr).max(0) as usize;
                    let ptr = sqlite3_value_text(vptr);
                    let slice: &[u8] = if !ptr.is_null() && len > 0 {
                        core::slice::from_raw_parts(ptr, len)
                    } else {
                        &[]
                    };
                    match aff {
                        ColumnAffinity::Integer | ColumnAffinity::Numeric => {
                            if let Ok(s) = core::str::from_utf8(slice) {
                                match parse_sqlite_numeric_str(s) {
                                    Some(Ok(i_val)) => CoercedSlot::Int(i_val),
                                    Some(Err(f_val)) => CoercedSlot::Float(f_val),
                                    None => CoercedSlot::TextSlice(slice),
                                }
                            } else {
                                CoercedSlot::TextSlice(slice)
                            }
                        }
                        ColumnAffinity::Real => {
                            if let Ok(s) = core::str::from_utf8(slice) {
                                match parse_sqlite_numeric_str(s) {
                                    Some(Ok(i_val)) => CoercedSlot::Float(i_val as f64),
                                    Some(Err(f_val)) => CoercedSlot::Float(f_val),
                                    None => CoercedSlot::TextSlice(slice),
                                }
                            } else {
                                CoercedSlot::TextSlice(slice)
                            }
                        }
                        _ => CoercedSlot::TextSlice(slice),
                    }
                }
                _ => {
                    let len = sqlite3_value_bytes(vptr).max(0) as usize;
                    let ptr = sqlite3_value_blob(vptr) as *const u8;
                    let slice: &[u8] = if !ptr.is_null() && len > 0 {
                        core::slice::from_raw_parts(ptr, len)
                    } else {
                        &[]
                    };
                    CoercedSlot::BlobSlice(slice)
                }
            };
            total_payload += match &slot {
                CoercedSlot::Null => 0,
                CoercedSlot::Int(_) | CoercedSlot::Float(_) => 8,
                CoercedSlot::TextSlice(s) | CoercedSlot::BlobSlice(s) => 4 + s.len(),
                CoercedSlot::TextOwned(s) => 4 + s.len(),
            };
            slots.push(slot);
        }

        let mut buf = vec![0u8; payload_start + total_payload];
        buf[0..2].copy_from_slice(&(col_count as u16).to_le_bytes());
        buf[2..4].copy_from_slice(&0u16.to_le_bytes());

        let mut write_pos = payload_start;
        for (i, slot) in slots.iter().enumerate() {
            let dir_pos = dir_start + i * 5;
            let offset_u32 = write_pos as u32;
            buf[dir_pos + 1..dir_pos + 5].copy_from_slice(&offset_u32.to_le_bytes());

            match slot {
                CoercedSlot::Null => {
                    buf[dir_pos] = 0;
                }
                CoercedSlot::Int(v) => {
                    buf[dir_pos] = 1;
                    buf[write_pos..write_pos + 8].copy_from_slice(&v.to_le_bytes());
                    write_pos += 8;
                }
                CoercedSlot::Float(v) => {
                    buf[dir_pos] = 2;
                    buf[write_pos..write_pos + 8].copy_from_slice(&v.to_le_bytes());
                    write_pos += 8;
                }
                CoercedSlot::TextSlice(slice) => {
                    buf[dir_pos] = 3;
                    let len = slice.len();
                    buf[write_pos..write_pos + 4].copy_from_slice(&(len as u32).to_le_bytes());
                    if len > 0 {
                        buf[write_pos + 4..write_pos + 4 + len].copy_from_slice(slice);
                    }
                    write_pos += 4 + len;
                }
                CoercedSlot::TextOwned(s) => {
                    buf[dir_pos] = 3;
                    let slice = s.as_bytes();
                    let len = slice.len();
                    buf[write_pos..write_pos + 4].copy_from_slice(&(len as u32).to_le_bytes());
                    if len > 0 {
                        buf[write_pos + 4..write_pos + 4 + len].copy_from_slice(slice);
                    }
                    write_pos += 4 + len;
                }
                CoercedSlot::BlobSlice(slice) => {
                    buf[dir_pos] = 4;
                    let len = slice.len();
                    buf[write_pos..write_pos + 4].copy_from_slice(&(len as u32).to_le_bytes());
                    if len > 0 {
                        buf[write_pos + 4..write_pos + 4 + len].copy_from_slice(slice);
                    }
                    write_pos += 4 + len;
                }
            }
        }

        let encode_slot_key = |col_idx: usize| -> Vec<u8> {
            let coll = col_collations
                .get(col_idx)
                .copied()
                .unwrap_or(ColCollation::Binary);
            match &slots[col_idx] {
                CoercedSlot::Null => vec![KEY_TAG_NULL],
                CoercedSlot::Int(v) => encode_i64_tagged_key(*v).to_vec(),
                CoercedSlot::Float(v) => encode_f64_unified_key(*v),
                CoercedSlot::TextSlice(slice) => {
                    let mut out = Vec::with_capacity(1 + slice.len());
                    out.push(KEY_TAG_TEXT);
                    coll.push_normalized_text(slice, &mut out);
                    out
                }
                CoercedSlot::TextOwned(s) => {
                    let slice = s.as_bytes();
                    let mut out = Vec::with_capacity(1 + slice.len());
                    out.push(KEY_TAG_TEXT);
                    coll.push_normalized_text(slice, &mut out);
                    out
                }
                CoercedSlot::BlobSlice(slice) => {
                    let mut out = Vec::with_capacity(1 + slice.len());
                    out.push(KEY_TAG_BLOB);
                    out.extend_from_slice(slice);
                    out
                }
            }
        };

        let pk_bytes = encode_slot_key(pk_col_idx);
        let mut sec_keys = Vec::with_capacity(secondary_indexes.len());
        for &(col_idx, index_id) in secondary_indexes {
            if col_idx < col_count {
                let sec_val = match &slots[col_idx] {
                    CoercedSlot::Float(v) => encode_canonical_numeric_key_from_f64(*v),
                    _ => encode_slot_key(col_idx),
                };
                sec_keys.push((index_id, sec_val));
            }
        }

        (pk_bytes, buf, sec_keys)
    }
}

/// Converts a raw `*mut sqlite3_value` directly into type-tagged memcomparable key bytes
/// ordered according to SQLite's storage-class comparison rules (`NULL < Number < TEXT < BLOB`).
///
/// # Safety
/// `vptr` must be a valid non-null `*mut sqlite3_value`.
#[inline]
pub unsafe fn sqlite_value_to_key_bytes(vptr: *mut sqlite3_value) -> Vec<u8> {
    unsafe {
        sqlite_value_to_key_bytes_with_schema(
            vptr,
            ColumnAffinity::Blob,
            ColCollation::Binary,
            false,
        )
    }
}

/// Converts a raw `*mut sqlite3_value` into type-tagged memcomparable key bytes, applying the
/// target column's [`ColumnAffinity`] and [`ColCollation`].
/// When `canonical_numeric` is `true`, exact integer floats (`10.0`) encode identically to
/// `Integer(10)` (`9` bytes).
///
/// # Safety
/// `vptr` must be a valid non-null `*mut sqlite3_value`.
#[inline]
pub unsafe fn sqlite_value_to_key_bytes_with_schema(
    vptr: *mut sqlite3_value,
    affinity: ColumnAffinity,
    collation: ColCollation,
    canonical_numeric: bool,
) -> Vec<u8> {
    unsafe {
        match sqlite3_value_type(vptr) {
            SQLITE_NULL => vec![KEY_TAG_NULL],
            SQLITE_INTEGER => {
                let v = sqlite3_value_int64(vptr);
                match affinity {
                    ColumnAffinity::Real => {
                        if canonical_numeric {
                            if let Some(i_val) = f64_to_exact_i64(v as f64) {
                                return encode_i64_tagged_key(i_val).to_vec();
                            }
                        }
                        encode_f64_unified_key(v as f64)
                    }
                    ColumnAffinity::Text => {
                        let s = v.to_string();
                        let mut out = Vec::with_capacity(1 + s.len());
                        out.push(KEY_TAG_TEXT);
                        collation.push_normalized_text(s.as_bytes(), &mut out);
                        out
                    }
                    _ => encode_i64_tagged_key(v).to_vec(),
                }
            }
            SQLITE_FLOAT => {
                let f = sqlite3_value_double(vptr);
                match affinity {
                    ColumnAffinity::Integer | ColumnAffinity::Numeric => {
                        if let Some(i_val) = f64_to_exact_i64(f) {
                            encode_i64_tagged_key(i_val).to_vec()
                        } else {
                            encode_f64_unified_key(f)
                        }
                    }
                    ColumnAffinity::Text => {
                        let ptr = sqlite3_value_text(vptr);
                        let len = sqlite3_value_bytes(vptr).max(0) as usize;
                        if !ptr.is_null() && len > 0 {
                            let slice = core::slice::from_raw_parts(ptr, len);
                            let mut out = Vec::with_capacity(1 + len);
                            out.push(KEY_TAG_TEXT);
                            collation.push_normalized_text(slice, &mut out);
                            out
                        } else {
                            let s = format_sqlite_float_text(f);
                            let mut out = Vec::with_capacity(1 + s.len());
                            out.push(KEY_TAG_TEXT);
                            collation.push_normalized_text(s.as_bytes(), &mut out);
                            out
                        }
                    }
                    _ => {
                        if canonical_numeric {
                            encode_canonical_numeric_key_from_f64(f)
                        } else {
                            encode_f64_unified_key(f)
                        }
                    }
                }
            }
            SQLITE_TEXT => {
                let len = sqlite3_value_bytes(vptr).max(0) as usize;
                let ptr = sqlite3_value_text(vptr);
                let slice: &[u8] = if !ptr.is_null() && len > 0 {
                    core::slice::from_raw_parts(ptr, len)
                } else {
                    &[]
                };
                match affinity {
                    ColumnAffinity::Integer | ColumnAffinity::Numeric => {
                        if let Ok(s) = core::str::from_utf8(slice) {
                            match parse_sqlite_numeric_str(s) {
                                Some(Ok(i_val)) => return encode_i64_tagged_key(i_val).to_vec(),
                                Some(Err(f_val)) => {
                                    return if canonical_numeric {
                                        encode_canonical_numeric_key_from_f64(f_val)
                                    } else {
                                        encode_f64_unified_key(f_val)
                                    };
                                }
                                None => {}
                            }
                        }
                    }
                    ColumnAffinity::Real => {
                        if let Ok(s) = core::str::from_utf8(slice) {
                            match parse_sqlite_numeric_str(s) {
                                Some(Ok(i_val)) => {
                                    let f_val = i_val as f64;
                                    return if canonical_numeric {
                                        encode_canonical_numeric_key_from_f64(f_val)
                                    } else {
                                        encode_f64_unified_key(f_val)
                                    };
                                }
                                Some(Err(f_val)) => {
                                    return if canonical_numeric {
                                        encode_canonical_numeric_key_from_f64(f_val)
                                    } else {
                                        encode_f64_unified_key(f_val)
                                    };
                                }
                                None => {}
                            }
                        }
                    }
                    _ => {}
                }
                let mut out = Vec::with_capacity(1 + slice.len());
                out.push(KEY_TAG_TEXT);
                collation.push_normalized_text(slice, &mut out);
                out
            }
            _ => {
                let len = sqlite3_value_bytes(vptr).max(0) as usize;
                let ptr = sqlite3_value_blob(vptr) as *const u8;
                let mut out = Vec::with_capacity(1 + len);
                out.push(KEY_TAG_BLOB);
                if !ptr.is_null() && len > 0 {
                    out.extend_from_slice(core::slice::from_raw_parts(ptr, len));
                }
                out
            }
        }
    }
}

/// Projects column `col_idx` from `row_bytes` directly into `ctx` (`sqlite3_result_*`) with
/// **zero Rust heap allocations** (`0` `String` / `Vec` allocations)!
///
/// # Safety
/// `ctx` must be a valid non-null `*mut sqlite3_context`.
#[inline]
pub unsafe fn project_column_zero_copy(
    row_bytes: &[u8],
    col_idx: usize,
    ctx: *mut sqlite3_context,
) -> io::Result<()> {
    unsafe {
        if row_bytes.len() < 4 {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                "row_bytes shorter than 4-byte header",
            ));
        }
        let col_count = u16::from_le_bytes([row_bytes[0], row_bytes[1]]) as usize;
        if col_idx >= col_count {
            sqlite3_result_null(ctx);
            return Ok(());
        }

        let dir_pos = 4 + col_idx * 5;
        if dir_pos + 5 > row_bytes.len() {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                "truncated column directory",
            ));
        }
        let type_tag = row_bytes[dir_pos];
        let offset =
            u32::from_le_bytes(row_bytes[dir_pos + 1..dir_pos + 5].try_into().unwrap()) as usize;

        match type_tag {
            0 => sqlite3_result_null(ctx),
            1 => {
                if offset + 8 > row_bytes.len() {
                    return Err(io::Error::new(ErrorKind::InvalidData, "truncated Integer"));
                }
                let v = i64::from_le_bytes(row_bytes[offset..offset + 8].try_into().unwrap());
                sqlite3_result_int64(ctx, v);
            }
            2 => {
                if offset + 8 > row_bytes.len() {
                    return Err(io::Error::new(ErrorKind::InvalidData, "truncated Float"));
                }
                let v = f64::from_le_bytes(row_bytes[offset..offset + 8].try_into().unwrap());
                sqlite3_result_double(ctx, v);
            }
            3 => {
                if offset + 4 > row_bytes.len() {
                    return Err(io::Error::new(ErrorKind::InvalidData, "truncated Text len"));
                }
                let len =
                    u32::from_le_bytes(row_bytes[offset..offset + 4].try_into().unwrap()) as usize;
                if offset + 4 + len > row_bytes.len() {
                    return Err(io::Error::new(ErrorKind::InvalidData, "truncated Text"));
                }
                if len > i32::MAX as usize {
                    sqlite3_result_error_toobig(ctx);
                    return Err(io::Error::new(
                        ErrorKind::InvalidInput,
                        "Text column exceeds i32::MAX bytes",
                    ));
                }
                let ptr = row_bytes[offset + 4..offset + 4 + len].as_ptr();
                sqlite3_result_text(ctx, ptr as *const _, len as i32, sqlite_transient());
            }
            4 => {
                if offset + 4 > row_bytes.len() {
                    return Err(io::Error::new(ErrorKind::InvalidData, "truncated Blob len"));
                }
                let len =
                    u32::from_le_bytes(row_bytes[offset..offset + 4].try_into().unwrap()) as usize;
                if offset + 4 + len > row_bytes.len() {
                    return Err(io::Error::new(ErrorKind::InvalidData, "truncated Blob"));
                }
                if len > i32::MAX as usize {
                    sqlite3_result_error_toobig(ctx);
                    return Err(io::Error::new(
                        ErrorKind::InvalidInput,
                        "Blob column exceeds i32::MAX bytes",
                    ));
                }
                let ptr = row_bytes[offset + 4..offset + 4 + len].as_ptr();
                sqlite3_result_blob(ctx, ptr as *const _, len as i32, sqlite_transient());
            }
            other => {
                return Err(io::Error::new(
                    ErrorKind::InvalidData,
                    format!("unknown column type_tag {other}"),
                ));
            }
        }
        Ok(())
    }
}

/// Extracts the type-tagged memcomparable key bytes for column `col_idx` directly from `row_bytes`
/// without decoding other columns (used when cleaning up old secondary indexes on overwrite/delete).
pub fn extract_column_key_bytes(row_bytes: &[u8], col_idx: usize) -> io::Result<Vec<u8>> {
    extract_column_key_bytes_with_collation(row_bytes, col_idx, ColCollation::Binary)
}

/// Extracts the type-tagged memcomparable key bytes for column `col_idx` directly from `row_bytes`,
/// applying `collation` for `TEXT` columns.
pub fn extract_column_key_bytes_with_collation(
    row_bytes: &[u8],
    col_idx: usize,
    collation: ColCollation,
) -> io::Result<Vec<u8>> {
    if row_bytes.len() < 4 {
        return Ok(vec![KEY_TAG_NULL]);
    }
    let col_count = u16::from_le_bytes([row_bytes[0], row_bytes[1]]) as usize;
    if col_idx >= col_count {
        return Ok(vec![KEY_TAG_NULL]);
    }
    let dir_pos = 4 + col_idx * 5;
    if dir_pos + 5 > row_bytes.len() {
        return Ok(vec![KEY_TAG_NULL]);
    }
    let type_tag = row_bytes[dir_pos];
    let offset =
        u32::from_le_bytes(row_bytes[dir_pos + 1..dir_pos + 5].try_into().unwrap()) as usize;

    match type_tag {
        0 => Ok(vec![KEY_TAG_NULL]),
        1 => {
            if offset + 8 > row_bytes.len() {
                return Ok(vec![KEY_TAG_NULL]);
            }
            let v = i64::from_le_bytes(row_bytes[offset..offset + 8].try_into().unwrap());
            Ok(encode_i64_tagged_key(v).to_vec())
        }
        2 => {
            if offset + 8 > row_bytes.len() {
                return Ok(vec![KEY_TAG_NULL]);
            }
            let v = f64::from_le_bytes(row_bytes[offset..offset + 8].try_into().unwrap());
            Ok(encode_f64_unified_key(v))
        }
        3 => {
            if offset + 4 > row_bytes.len() {
                return Ok(vec![KEY_TAG_NULL]);
            }
            let len =
                u32::from_le_bytes(row_bytes[offset..offset + 4].try_into().unwrap()) as usize;
            if offset + 4 + len > row_bytes.len() {
                return Ok(vec![KEY_TAG_NULL]);
            }
            let slice = &row_bytes[offset + 4..offset + 4 + len];
            let mut out = Vec::with_capacity(1 + len);
            out.push(KEY_TAG_TEXT);
            collation.push_normalized_text(slice, &mut out);
            Ok(out)
        }
        4 => {
            if offset + 4 > row_bytes.len() {
                return Ok(vec![KEY_TAG_NULL]);
            }
            let len =
                u32::from_le_bytes(row_bytes[offset..offset + 4].try_into().unwrap()) as usize;
            if offset + 4 + len > row_bytes.len() {
                return Ok(vec![KEY_TAG_NULL]);
            }
            let mut out = Vec::with_capacity(1 + len);
            out.push(KEY_TAG_BLOB);
            out.extend_from_slice(&row_bytes[offset + 4..offset + 4 + len]);
            Ok(out)
        }
        _ => Ok(vec![KEY_TAG_NULL]),
    }
}

/// Projects a single column `col_idx` from a packed row byte slice in $O(1)$ time without
/// decoding any other columns in the row!
pub fn decode_column_at(row_bytes: &[u8], col_idx: usize) -> io::Result<SqlValue> {
    if row_bytes.len() < 4 {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "row_bytes shorter than 4-byte header",
        ));
    }
    let col_count = u16::from_le_bytes([row_bytes[0], row_bytes[1]]) as usize;
    if col_idx >= col_count {
        return Ok(SqlValue::Null);
    }

    let dir_pos = 4 + col_idx * 5;
    if dir_pos + 5 > row_bytes.len() {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "truncated column directory in row_bytes",
        ));
    }
    let type_tag = row_bytes[dir_pos];
    let offset =
        u32::from_le_bytes(row_bytes[dir_pos + 1..dir_pos + 5].try_into().unwrap()) as usize;

    match type_tag {
        0 => Ok(SqlValue::Null),
        1 => {
            if offset + 8 > row_bytes.len() {
                return Err(io::Error::new(
                    ErrorKind::InvalidData,
                    "truncated Integer column",
                ));
            }
            let v = i64::from_le_bytes(row_bytes[offset..offset + 8].try_into().unwrap());
            Ok(SqlValue::Integer(v))
        }
        2 => {
            if offset + 8 > row_bytes.len() {
                return Err(io::Error::new(
                    ErrorKind::InvalidData,
                    "truncated Float column",
                ));
            }
            let v = f64::from_le_bytes(row_bytes[offset..offset + 8].try_into().unwrap());
            Ok(SqlValue::Float(v))
        }
        3 => {
            if offset + 4 > row_bytes.len() {
                return Err(io::Error::new(
                    ErrorKind::InvalidData,
                    "truncated Text length",
                ));
            }
            let len =
                u32::from_le_bytes(row_bytes[offset..offset + 4].try_into().unwrap()) as usize;
            if offset + 4 + len > row_bytes.len() {
                return Err(io::Error::new(
                    ErrorKind::InvalidData,
                    "truncated Text payload",
                ));
            }
            let s = String::from_utf8_lossy(&row_bytes[offset + 4..offset + 4 + len]).into_owned();
            Ok(SqlValue::Text(s))
        }
        4 => {
            if offset + 4 > row_bytes.len() {
                return Err(io::Error::new(
                    ErrorKind::InvalidData,
                    "truncated Blob length",
                ));
            }
            let len =
                u32::from_le_bytes(row_bytes[offset..offset + 4].try_into().unwrap()) as usize;
            if offset + 4 + len > row_bytes.len() {
                return Err(io::Error::new(
                    ErrorKind::InvalidData,
                    "truncated Blob payload",
                ));
            }
            Ok(SqlValue::Blob(
                row_bytes[offset + 4..offset + 4 + len].to_vec(),
            ))
        }
        other => Err(io::Error::new(
            ErrorKind::InvalidData,
            format!("unknown column type_tag {other}"),
        )),
    }
}

/// Decodes all columns from `row_bytes`.
pub fn decode_all_columns(row_bytes: &[u8]) -> io::Result<Vec<SqlValue>> {
    if row_bytes.len() < 4 {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "row_bytes shorter than 4-byte header",
        ));
    }
    let col_count = u16::from_le_bytes([row_bytes[0], row_bytes[1]]) as usize;
    let mut out = Vec::with_capacity(col_count);
    for i in 0..col_count {
        out.push(decode_column_at(row_bytes, i)?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memcomparable_f64_preserves_order_and_normalizes_neg_zero() {
        assert_eq!(
            encode_f64_memcomparable(-0.0),
            encode_f64_memcomparable(0.0)
        );
        assert_eq!(
            decode_f64_memcomparable(encode_f64_memcomparable(-0.0)),
            0.0
        );

        let vals = [
            f64::NEG_INFINITY,
            -f64::MAX,
            -1_000_000.5,
            -10.0,
            -2.0,
            -1.0,
            -f64::MIN_POSITIVE,
            0.0,
            f64::MIN_POSITIVE,
            1.0,
            2.0,
            10.0,
            1_000_000.5,
            f64::MAX,
            f64::INFINITY,
        ];
        for pair in vals.windows(2) {
            let a = encode_f64_memcomparable(pair[0]);
            let b = encode_f64_memcomparable(pair[1]);
            assert!(a < b, "{} should encode smaller than {}", pair[0], pair[1]);
            assert_eq!(decode_f64_memcomparable(a), pair[0]);
            assert_eq!(decode_f64_memcomparable(b), pair[1]);
        }

        let row = encode_row(&[SqlValue::Float(-2.5), SqlValue::Float(2.5)]);
        assert_eq!(
            extract_column_key_bytes(&row, 0).unwrap(),
            SqlValue::Float(-2.5).to_key_bytes()
        );
        assert_eq!(
            extract_column_key_bytes(&row, 1).unwrap(),
            SqlValue::Float(2.5).to_key_bytes()
        );
        assert!(SqlValue::Float(-2.5).to_key_bytes() < SqlValue::Float(-1.0).to_key_bytes());
        assert!(SqlValue::Float(-1.0).to_key_bytes() < SqlValue::Float(0.0).to_key_bytes());

        // Verify unified cross-type key ordering (`NULL < Number (INTEGER & REAL) < TEXT < BLOB`)
        // including exact 64-bit integer precision at 2^53, negative floats, signed zero, and infinities
        let ordered_values = [
            SqlValue::Null,
            SqlValue::Float(f64::NEG_INFINITY),
            SqlValue::Float(-1e30),
            SqlValue::Integer(i64::MIN),
            SqlValue::Integer(i64::MIN + 1),
            SqlValue::Integer(-9_007_199_254_740_993),
            SqlValue::Integer(-9_007_199_254_740_992),
            SqlValue::Float(-9_007_199_254_740_992.0),
            SqlValue::Integer(-100),
            SqlValue::Float(-100.0),
            SqlValue::Float(-10.5),
            SqlValue::Integer(-10),
            SqlValue::Float(-1.5),
            SqlValue::Integer(-1),
            SqlValue::Integer(0),
            SqlValue::Float(0.0),
            SqlValue::Float(1.5),
            SqlValue::Integer(10),
            SqlValue::Float(10.5),
            SqlValue::Integer(100),
            SqlValue::Float(100.0),
            SqlValue::Integer(9_007_199_254_740_992),
            SqlValue::Float(9_007_199_254_740_992.0),
            SqlValue::Integer(9_007_199_254_740_993),
            SqlValue::Integer(i64::MAX - 1),
            SqlValue::Integer(i64::MAX),
            SqlValue::Float(9_223_372_036_854_775_808.0),
            SqlValue::Float(1e30),
            SqlValue::Float(f64::INFINITY),
            SqlValue::Text(String::new()),
            SqlValue::Text("abc".to_string()),
            SqlValue::Blob(Vec::new()),
            SqlValue::Blob(b"abc".to_vec()),
        ];
        for pair in ordered_values.windows(2) {
            let ka = pair[0].to_key_bytes();
            let kb = pair[1].to_key_bytes();
            assert!(ka < kb, "{:?} must sort before {:?}", pair[0], pair[1]);
            assert_eq!(SqlValue::from_key_bytes(&ka).as_ref(), Some(&pair[0]));
            assert_eq!(SqlValue::from_key_bytes(&kb).as_ref(), Some(&pair[1]));
        }
    }

    #[test]
    fn collation_and_numeric_affinity_helpers() {
        let mut out = Vec::new();
        ColCollation::NoCase.push_normalized_text(b"AlIcE_123!", &mut out);
        assert_eq!(out, b"alice_123!");

        out.clear();
        ColCollation::RTrim.push_normalized_text(b"hello   ", &mut out);
        assert_eq!(out, b"hello");

        assert_eq!(parse_sqlite_numeric_str(" 42 "), Some(Ok(42)));
        assert_eq!(parse_sqlite_numeric_str(" 42.0 "), Some(Ok(42)));
        assert_eq!(parse_sqlite_numeric_str(" -1.5 "), Some(Err(-1.5)));
        assert_eq!(parse_sqlite_numeric_str("abc"), None);
    }

    #[test]
    fn negative_sub_ulp_float_key_roundtrip_and_sqlite_float_text_format() {
        // Negative sub-ULP and small floats in (-1.0, 0.0) must encode to distinct keys
        // in strict mathematical order and round-trip with 0 bits of difference!
        let vals = [
            f64::NEG_INFINITY,
            -f64::MAX,
            -9_223_372_036_854_775_808.0,
            -2.0,
            -1.5,
            -1.0,
            f64::from_bits((-1.0f64).to_bits() - 1), // immediately above -1.0
            -0.5,
            -0.001,
            -1e-5,
            -1e-16,
            -1e-17,
            -1e-20,
            -1e-300,
            -f64::MIN_POSITIVE,
            -f64::from_bits(1), // smallest negative subnormal
            0.0,
            f64::from_bits(1), // smallest positive subnormal
            f64::MIN_POSITIVE,
            1e-300,
            1e-20,
            0.001,
            0.5,
            1.0,
            1.5,
            2.0,
            9_223_372_036_854_775_808.0,
            f64::MAX,
            f64::INFINITY,
        ];
        for &v in &vals {
            let key = SqlValue::Float(v).to_key_bytes();
            let decoded = SqlValue::from_key_bytes(&key).expect("decode float key");
            match decoded {
                SqlValue::Float(d) => {
                    assert_eq!(
                        d.to_bits(),
                        v.to_bits(),
                        "{v:e} failed exact bit-level round-trip (got {d:e})"
                    );
                }
                other => panic!("expected Float, got {other:?}"),
            }
        }
        // Verify -0.0 normalizes to +0.0
        let neg_zero_key = SqlValue::Float(-0.0).to_key_bytes();
        assert_eq!(neg_zero_key, SqlValue::Float(0.0).to_key_bytes());
        assert_eq!(
            SqlValue::from_key_bytes(&neg_zero_key),
            Some(SqlValue::Float(0.0))
        );

        // Verify strict lexicographical key order across all pairs
        for pair in vals.windows(2) {
            let ka = SqlValue::Float(pair[0]).to_key_bytes();
            let kb = SqlValue::Float(pair[1]).to_key_bytes();
            assert!(
                ka < kb,
                "{:e} (key {:02x?}) must sort strictly before {:e} (key {:02x?})",
                pair[0],
                ka,
                pair[1],
                kb
            );
        }

        // format_sqlite_float_text matches SQLite's %.15g / %!.15g formatting
        assert_eq!(format_sqlite_float_text(0.0), "0.0");
        assert_eq!(format_sqlite_float_text(-0.0), "0.0");
        assert_eq!(format_sqlite_float_text(42.0), "42.0");
        assert_eq!(format_sqlite_float_text(-1.5), "-1.5");
        assert_eq!(format_sqlite_float_text(1e20), "1.0e+20");
        assert_eq!(format_sqlite_float_text(1.25e20), "1.25e+20");
        assert_eq!(format_sqlite_float_text(-1e-20), "-1.0e-20");
    }
}
