//! Compact binary row serialization with an $O(1)$ column offset directory,
//! plus memcomparable key conversion for SQLite [`sqlite3_value`] items.

use std::io::{self, ErrorKind};

use crate::lru::{decode_i64_memcomparable, encode_i64_memcomparable};
use crate::util::io_err;
use crate::vtab::ffi::{
    SQLITE_FLOAT, SQLITE_INTEGER, SQLITE_NULL, SQLITE_TEXT, sqlite_transient,
    sqlite_value_blob_bytes, sqlite_value_text_bytes, sqlite3_context, sqlite3_result_blob,
    sqlite3_result_double, sqlite3_result_error_toobig, sqlite3_result_int64, sqlite3_result_null,
    sqlite3_result_text, sqlite3_value, sqlite3_value_double, sqlite3_value_int64,
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
                out.reserve(bytes.len());
                if let Some(nul_pos) = bytes.iter().position(|&b| b == 0) {
                    out.extend(bytes[..nul_pos].iter().map(|b| b.to_ascii_lowercase()));
                    out.resize(out.len() + (bytes.len() - nul_pos), 0);
                } else {
                    out.extend(bytes.iter().map(|b| b.to_ascii_lowercase()));
                }
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

/// Checks if a finite `f64` is an exact integer within SQLite's strict open coercion interval
/// `(-2^63, 2^63)` (`-9223372036854775808.0 < f < 9223372036854775808.0`).
///
/// SQLite's `INTEGER`/`NUMERIC` affinity storage coercion and rowid conversion exclude `-2^63`
/// (`-9223372036854775808.0`), keeping it as `REAL`.
#[inline]
pub fn f64_to_exact_i64(f: f64) -> Option<i64> {
    if f.is_finite() && f.fract() == 0.0 && f > F64_I64_MIN && f < F64_I64_UPPER_EXCL {
        Some(f as i64)
    } else {
        None
    }
}

/// Checks if a finite `f64` represents an exact integer within `[-2^63, 2^63)` for canonical
/// numeric index equivalence (where `-9223372036854775808.0 == -9223372036854775808`).
#[inline]
fn f64_to_canonical_i64(f: f64) -> Option<i64> {
    if f.is_finite() && f.fract() == 0.0 && (F64_I64_MIN..F64_I64_UPPER_EXCL).contains(&f) {
        Some(f as i64)
    } else {
        None
    }
}

fn is_sqlite_ascii_ws(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\n' | '\x0b' | '\x0c' | '\r')
}

fn is_sqlite_decimal_literal(s: &str) -> bool {
    let b = s.as_bytes();
    let mut i = 0;
    if i < b.len() && matches!(b[i], b'+' | b'-') {
        i += 1;
    }
    let mut digits = 0usize;
    while i < b.len() && b[i].is_ascii_digit() {
        digits += 1;
        i += 1;
    }
    if i < b.len() && b[i] == b'.' {
        i += 1;
        while i < b.len() && b[i].is_ascii_digit() {
            digits += 1;
            i += 1;
        }
    }
    if digits == 0 {
        return false;
    }
    if i < b.len() && matches!(b[i], b'e' | b'E') {
        i += 1;
        if i < b.len() && matches!(b[i], b'+' | b'-') {
            i += 1;
        }
        let mut exp_digits = 0usize;
        while i < b.len() && b[i].is_ascii_digit() {
            exp_digits += 1;
            i += 1;
        }
        if exp_digits == 0 {
            return false;
        }
    }
    i == b.len()
}

/// Attempts to parse a string slice as a well-formed SQLite numeric literal (`i64` or `f64`).
/// Returns `Some(Ok(i64))` if it is an exact 64-bit integer, `Some(Err(f64))` if it is a valid
/// numeric floating-point value (including overflowing values such as `1e500` -> `+Inf`) that
/// cannot be represented as `i64`, or `None` if it is not numeric.
pub fn parse_sqlite_numeric_str(s: &str) -> Option<Result<i64, f64>> {
    let trimmed = s.trim_matches(is_sqlite_ascii_ws);
    if trimmed.is_empty() || !is_sqlite_decimal_literal(trimmed) {
        return None;
    }
    if let Ok(i) = trimmed.parse::<i64>() {
        return Some(Ok(i));
    }
    if let Ok(f) = trimmed.parse::<f64>() {
        if !f.is_nan() {
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
    if f.is_infinite() {
        return if f.is_sign_negative() {
            "-Inf".to_string()
        } else {
            "Inf".to_string()
        };
    }
    if f.is_nan() {
        return "NaN".to_string();
    }
    let sign = if f.is_sign_negative() { "-" } else { "" };
    // Round to 15 significant digits before choosing fixed vs scientific notation, matching
    // SQLite's `%.15g` threshold (`-4 <= exp < 15` uses fixed point) on rounding boundaries.
    // Note that Rust's `{:.14e}` uses IEEE round-half-to-even (matching SQLite's `%.15g` on
    // glibc/musl/MSVC targets), while arbitrary-precision sprintf implementations can differ
    // only on sub-ULP halfway rounding boundaries.
    let sci = format!("{:.14e}", f.abs());
    let Some((mant_raw, exp_raw)) = sci.split_once('e') else {
        return format!("{f}");
    };
    let exp: i32 = exp_raw.parse().unwrap_or(0);
    let mut digits = String::with_capacity(16);
    for b in mant_raw.bytes() {
        if b != b'.' {
            digits.push(b as char);
        }
    }
    let sig = digits.trim_end_matches('0');
    let sig = if sig.is_empty() { "0" } else { sig };
    if (-4..15).contains(&exp) {
        if exp < 0 {
            let zeros = (-exp - 1) as usize;
            format!("{sign}0.{}{sig}", "0".repeat(zeros))
        } else {
            let int_digits = (exp + 1) as usize;
            if sig.len() <= int_digits {
                format!("{sign}{sig}{}.0", "0".repeat(int_digits - sig.len()))
            } else {
                format!("{sign}{}.{}", &sig[..int_digits], &sig[int_digits..])
            }
        }
    } else {
        let mant = if sig.len() == 1 {
            format!("{sig}.0")
        } else {
            format!("{}.{}", &sig[..1], &sig[1..])
        };
        let exp_sign = if exp >= 0 { '+' } else { '-' };
        let abs_exp = exp.unsigned_abs();
        if abs_exp < 100 {
            format!("{sign}{mant}e{exp_sign}{abs_exp:02}")
        } else {
            format!("{sign}{mant}e{exp_sign}{abs_exp}")
        }
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
    if let Some(i) = f64_to_canonical_i64(v) {
        encode_i64_tagged_key(i).to_vec()
    } else {
        encode_f64_unified_key(v)
    }
}

/// True when `key` is the canonical 17-byte float zero (`[KEY_TAG_INTEGER, +0, 0]`), which
/// collapses `-0.0` and `+0.0` so callers needing `-0.0`'s sign must read from `row_bytes`.
#[inline]
pub(super) fn is_canonical_float_zero_key(key: &[u8]) -> bool {
    key.len() == 17
        && key[0] == KEY_TAG_INTEGER
        && key[1..9] == encode_i64_memcomparable(0)
        && key[9..17] == [0u8; 8]
}

/// Encodes an IEEE-754 `f64` into an 8-byte order-preserving big-endian representation:
/// - Normalizes `-0.0` to `+0.0` and any `NaN` bit pattern (including `-NaN`) to canonical `f64::NAN`.
/// - Positive/zero floats (`sign bit == 0`) flip the sign bit (`bits ^ (1 << 63)`).
/// - Negative floats (`sign bit == 1`) flip all 64 bits (`!bits`).
#[inline]
pub fn encode_f64_memcomparable(v: f64) -> [u8; 8] {
    let bits = if v == 0.0 {
        0.0f64.to_bits()
    } else if v.is_nan() {
        f64::NAN.to_bits()
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
                    let slice = sqlite_value_text_bytes(val);
                    Self::Text(String::from_utf8_lossy(slice).into_owned())
                }
                _ => {
                    let slice = sqlite_value_blob_bytes(val);
                    Self::Blob(slice.to_vec())
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
            KEY_TAG_FLOAT_LT_I64_MIN | KEY_TAG_FLOAT_GE_I64_MAX_PLUS_1 if payload.len() == 8 => {
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
///
/// # Panics
/// Panics if the row exceeds the format limits or its output buffer cannot be allocated.
/// Use [`try_encode_row`] to handle these errors.
pub fn encode_row(cols: &[SqlValue]) -> Vec<u8> {
    try_encode_row(cols).expect("cannot encode row")
}

fn row_too_large() -> io::Error {
    io_err(
        ErrorKind::FileTooLarge,
        "row exceeds the binary row format limits",
    )
}

fn row_header_len(count: usize) -> io::Result<u32> {
    Ok(4 + 5 * u32::from(u16::try_from(count).map_err(|_| row_too_large())?))
}

fn add_cell_size(total: u32, prefix: u32, payload: usize) -> io::Result<u32> {
    let payload = u32::try_from(payload).map_err(|_| row_too_large())?;
    total
        .checked_add(prefix)
        .and_then(|n| n.checked_add(payload))
        .ok_or_else(row_too_large)
}

fn allocate_row(size: u32) -> io::Result<Vec<u8>> {
    #[cfg(kani)]
    {
        Ok(vec![0u8; size as usize])
    }
    #[cfg(not(kani))]
    {
        let mut buf = Vec::new();
        buf.try_reserve_exact(size as usize)
            .map_err(|_| io::Error::from(ErrorKind::OutOfMemory))?;
        buf.resize(size as usize, 0);
        Ok(buf)
    }
}

/// Encodes a row, returning `FileTooLarge` when the column count or total length exceeds
/// the on-disk representation. Performs the size checks before allocating the output.
pub fn try_encode_row(cols: &[SqlValue]) -> io::Result<Vec<u8>> {
    let col_count = cols.len();
    let dir_start = 4;
    let payload_start = row_header_len(col_count)?;

    let mut size = payload_start;
    for c in cols {
        size = match c {
            SqlValue::Null => size,
            SqlValue::Integer(_) | SqlValue::Float(_) => add_cell_size(size, 0, 8)?,
            SqlValue::Text(s) => add_cell_size(size, 4, s.len())?,
            SqlValue::Blob(b) => add_cell_size(size, 4, b.len())?,
        };
    }

    let mut buf = allocate_row(size)?;
    buf[0..2].copy_from_slice(&(col_count as u16).to_le_bytes());
    buf[2..4].copy_from_slice(&0u16.to_le_bytes());

    let mut write_pos = payload_start as usize;
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

    Ok(buf)
}

/// Encodes a slice of raw `sqlite3_value*` pointers (`argv[2 .. 2 + col_count]`) directly into
/// a compact binary row byte vector without allocating intermediate `SqlValue` structs!
/// Applies each column's declared [`ColumnAffinity`] and [`ColCollation`], and extracts
/// the primary key bytes (`pk_col_idx`) and any secondary index `(index_id, col_key_bytes)`.
pub type DirectEncodedRow = (Vec<u8>, Vec<u8>, Vec<(u16, Vec<u8>)>);

/// If `pk_override` is `Some(auto_id)` (for `INTEGER PRIMARY KEY` when `SQLITE_NULL` is inserted),
/// serializes `auto_id` as an `SQLITE_INTEGER` in the `pk_col_idx` slot.
///
/// # Panics
/// Panics if the row cannot be encoded. Use [`try_encode_sqlite_values_direct`] for fallible encoding.
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
    unsafe {
        try_encode_sqlite_values_direct(
            values,
            col_count,
            pk_col_idx,
            pk_override,
            col_affinities,
            col_collations,
            secondary_indexes,
        )
    }
    .expect("cannot encode SQLite row")
}

/// Fallible counterpart to [`encode_sqlite_values_direct`].
///
/// # Safety
/// `values` must point to `col_count` valid `*mut sqlite3_value` pointers.
pub unsafe fn try_encode_sqlite_values_direct(
    values: *mut *mut sqlite3_value,
    col_count: usize,
    pk_col_idx: usize,
    pk_override: Option<i64>,
    col_affinities: &[ColumnAffinity],
    col_collations: &[ColCollation],
    secondary_indexes: &[(usize, u16)],
) -> io::Result<DirectEncodedRow> {
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
        let payload_start = row_header_len(col_count)?;
        if pk_col_idx >= col_count {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "primary key column outside row",
            ));
        }

        let mut slots: Vec<CoercedSlot<'_>> = Vec::with_capacity(col_count);
        let mut size = payload_start;

        for i in 0..col_count {
            if i == pk_col_idx {
                if let Some(auto_id) = pk_override {
                    slots.push(CoercedSlot::Int(auto_id));
                    size = add_cell_size(size, 0, 8)?;
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
                            let slice = sqlite_value_text_bytes(vptr);
                            if !slice.is_empty() {
                                CoercedSlot::TextSlice(slice)
                            } else {
                                CoercedSlot::TextOwned(format_sqlite_float_text(f))
                            }
                        }
                        _ => CoercedSlot::Float(f),
                    }
                }
                SQLITE_TEXT => {
                    let slice = sqlite_value_text_bytes(vptr);
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
                _ => CoercedSlot::BlobSlice(sqlite_value_blob_bytes(vptr)),
            };
            size = match &slot {
                CoercedSlot::Null => size,
                CoercedSlot::Int(_) | CoercedSlot::Float(_) => add_cell_size(size, 0, 8)?,
                CoercedSlot::TextSlice(s) | CoercedSlot::BlobSlice(s) => {
                    add_cell_size(size, 4, s.len())?
                }
                CoercedSlot::TextOwned(s) => add_cell_size(size, 4, s.len())?,
            };
            slots.push(slot);
        }

        let mut buf = allocate_row(size)?;
        buf[0..2].copy_from_slice(&(col_count as u16).to_le_bytes());
        buf[2..4].copy_from_slice(&0u16.to_le_bytes());

        let mut write_pos = payload_start as usize;
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

        Ok((pk_bytes, buf, sec_keys))
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
                    ColumnAffinity::Text => {
                        let s = v.to_string();
                        let mut out = Vec::with_capacity(1 + s.len());
                        out.push(KEY_TAG_TEXT);
                        collation.push_normalized_text(s.as_bytes(), &mut out);
                        out
                    }
                    // Compare exact 64-bit integers mathematically even against REAL columns.
                    _ => encode_i64_tagged_key(v).to_vec(),
                }
            }
            SQLITE_FLOAT => {
                let f = sqlite3_value_double(vptr);
                match affinity {
                    ColumnAffinity::Integer | ColumnAffinity::Numeric => {
                        if canonical_numeric {
                            encode_canonical_numeric_key_from_f64(f)
                        } else if let Some(i_val) = f64_to_exact_i64(f) {
                            encode_i64_tagged_key(i_val).to_vec()
                        } else {
                            encode_f64_unified_key(f)
                        }
                    }
                    ColumnAffinity::Text => {
                        let slice = sqlite_value_text_bytes(vptr);
                        if !slice.is_empty() {
                            let mut out = Vec::with_capacity(1 + slice.len());
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
                let slice = sqlite_value_text_bytes(vptr);
                match affinity {
                    ColumnAffinity::Integer | ColumnAffinity::Numeric | ColumnAffinity::Real => {
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
                    _ => {}
                }
                let mut out = Vec::with_capacity(1 + slice.len());
                out.push(KEY_TAG_TEXT);
                collation.push_normalized_text(slice, &mut out);
                out
            }
            _ => {
                let slice = sqlite_value_blob_bytes(vptr);
                let mut out = Vec::with_capacity(1 + slice.len());
                out.push(KEY_TAG_BLOB);
                out.extend_from_slice(slice);
                out
            }
        }
    }
}

/// Passes a raw slice to `sqlite3_result_text` or `sqlite3_result_blob`, reporting `TOOBIG` and
/// returning `false` when `payload.len() > i32::MAX as usize`.
#[inline]
unsafe fn result_bytes(ctx: *mut sqlite3_context, payload: &[u8], is_text: bool) -> bool {
    if payload.len() > i32::MAX as usize {
        unsafe { sqlite3_result_error_toobig(ctx) };
        return false;
    }
    unsafe {
        if is_text {
            sqlite3_result_text(
                ctx,
                payload.as_ptr() as *const _,
                payload.len() as i32,
                sqlite_transient(),
            );
        } else {
            sqlite3_result_blob(
                ctx,
                payload.as_ptr() as *const _,
                payload.len() as i32,
                sqlite_transient(),
            );
        }
    }
    true
}

/// Projects a type-tagged primary key slice directly into `ctx` (`sqlite3_result_*`) with
/// zero heap allocation and exact byte preservation (including invalid UTF-8 `TEXT` keys).
///
/// Returns `Err(ErrorKind::InvalidData)` if `key` is malformed or has an unknown tag, or
/// `Err(ErrorKind::FileTooLarge)` (after invoking `sqlite3_result_error_toobig`) if a `TEXT` or
/// `BLOB` key exceeds `i32::MAX` bytes.
///
/// # Safety
/// `ctx` must be a valid non-null `*mut sqlite3_context`.
#[inline]
pub unsafe fn project_key_zero_copy(key: &[u8], ctx: *mut sqlite3_context) -> io::Result<()> {
    let invalid = || io::Error::new(ErrorKind::InvalidData, "malformed primary key bytes");
    let Some((&tag, payload)) = key.split_first() else {
        return Err(invalid());
    };
    unsafe {
        match tag {
            KEY_TAG_NULL if payload.is_empty() => {
                sqlite3_result_null(ctx);
                Ok(())
            }
            KEY_TAG_INTEGER if payload.len() == 8 => {
                let arr: [u8; 8] = payload.try_into().unwrap();
                sqlite3_result_int64(ctx, decode_i64_memcomparable(arr));
                Ok(())
            }
            KEY_TAG_INTEGER if payload.len() == 16 => {
                let q_arr: [u8; 8] = payload[0..8].try_into().unwrap();
                let r_arr: [u8; 8] = payload[8..16].try_into().unwrap();
                let f = if u64::from_be_bytes(r_arr) == 0 {
                    decode_i64_memcomparable(q_arr) as f64
                } else {
                    decode_f64_memcomparable(r_arr)
                };
                sqlite3_result_double(ctx, f);
                Ok(())
            }
            KEY_TAG_FLOAT_LT_I64_MIN | KEY_TAG_FLOAT_GE_I64_MAX_PLUS_1 if payload.len() == 8 => {
                let arr: [u8; 8] = payload.try_into().unwrap();
                sqlite3_result_double(ctx, decode_f64_memcomparable(arr));
                Ok(())
            }
            KEY_TAG_TEXT => {
                if !result_bytes(ctx, payload, true) {
                    return Err(io::Error::new(
                        ErrorKind::FileTooLarge,
                        "Text key exceeds i32::MAX bytes",
                    ));
                }
                Ok(())
            }
            KEY_TAG_BLOB => {
                if !result_bytes(ctx, payload, false) {
                    return Err(io::Error::new(
                        ErrorKind::FileTooLarge,
                        "Blob key exceeds i32::MAX bytes",
                    ));
                }
                Ok(())
            }
            _ => Err(invalid()),
        }
    }
}

enum StoredColumn<'a> {
    Null,
    Integer(i64),
    Float(f64),
    Text(&'a [u8]),
    Blob(&'a [u8]),
}

fn stored_column_count(row: &[u8]) -> io::Result<usize> {
    if row.len() < 4 {
        return Err(io_err(ErrorKind::InvalidData, "truncated row header"));
    }
    let count = usize::from(u16::from_le_bytes([row[0], row[1]]));
    if row.len() < 4 + count * 5 {
        return Err(io_err(ErrorKind::InvalidData, "truncated column directory"));
    }
    Ok(count)
}

fn stored_column(row: &[u8], index: usize) -> io::Result<StoredColumn<'_>> {
    let count = stored_column_count(row)?;
    if index >= count {
        return Ok(StoredColumn::Null);
    }
    let pos = 4 + index * 5;
    let offset_at =
        |pos: usize| u32::from_le_bytes(row[pos + 1..pos + 5].try_into().unwrap()) as usize;
    let start = offset_at(pos);
    let end = if index + 1 == count {
        row.len()
    } else {
        offset_at(pos + 5)
    };
    let invalid = || {
        io_err(
            ErrorKind::InvalidData,
            "invalid stored column extent or type",
        )
    };
    let directory_end = 4 + count * 5;
    if start < directory_end || (index == 0 && start != directory_end) {
        return Err(invalid());
    }
    let cell = row.get(start..end).ok_or_else(invalid)?;
    match row[pos] {
        0 if cell.is_empty() => Ok(StoredColumn::Null),
        1 => Ok(StoredColumn::Integer(i64::from_le_bytes(
            cell.try_into().map_err(|_| invalid())?,
        ))),
        2 => Ok(StoredColumn::Float(f64::from_le_bytes(
            cell.try_into().map_err(|_| invalid())?,
        ))),
        tag @ (3 | 4) => {
            let (len, payload) = cell.split_first_chunk::<4>().ok_or_else(invalid)?;
            if u32::from_le_bytes(*len) as usize != payload.len() {
                return Err(invalid());
            }
            Ok(if tag == 3 {
                StoredColumn::Text(payload)
            } else {
                StoredColumn::Blob(payload)
            })
        }
        _ => Err(invalid()),
    }
}

/// Reads column `col_idx` from `row` when stored as an 8-byte `INTEGER`, validating the row
/// header, directory, and column payload bounds first.
pub(super) fn read_stored_i64_column(row: &[u8], col_idx: usize) -> io::Result<Option<i64>> {
    match stored_column(row, col_idx)? {
        StoredColumn::Integer(v) => Ok(Some(v)),
        _ => Ok(None),
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
        match stored_column(row_bytes, col_idx)? {
            StoredColumn::Null => sqlite3_result_null(ctx),
            StoredColumn::Integer(v) => sqlite3_result_int64(ctx, v),
            StoredColumn::Float(v) => sqlite3_result_double(ctx, v),
            StoredColumn::Text(bytes) if !result_bytes(ctx, bytes, true) => {
                return Err(io::Error::new(
                    ErrorKind::FileTooLarge,
                    "Text column exceeds i32::MAX bytes",
                ));
            }
            StoredColumn::Blob(bytes) if !result_bytes(ctx, bytes, false) => {
                return Err(io::Error::new(
                    ErrorKind::FileTooLarge,
                    "Blob column exceeds i32::MAX bytes",
                ));
            }
            StoredColumn::Text(_) | StoredColumn::Blob(_) => {}
        }
    }
    Ok(())
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
    Ok(match stored_column(row_bytes, col_idx)? {
        StoredColumn::Null => vec![KEY_TAG_NULL],
        StoredColumn::Integer(v) => encode_i64_tagged_key(v).to_vec(),
        StoredColumn::Float(v) => encode_f64_unified_key(v),
        StoredColumn::Text(bytes) => {
            let mut out = vec![KEY_TAG_TEXT];
            collation.push_normalized_text(bytes, &mut out);
            out
        }
        StoredColumn::Blob(bytes) => {
            let mut out = Vec::with_capacity(1 + bytes.len());
            out.push(KEY_TAG_BLOB);
            out.extend_from_slice(bytes);
            out
        }
    })
}

/// Projects a single column `col_idx` from a packed row byte slice in $O(1)$ time without
/// decoding any other columns in the row!
pub fn decode_column_at(row_bytes: &[u8], col_idx: usize) -> io::Result<SqlValue> {
    Ok(match stored_column(row_bytes, col_idx)? {
        StoredColumn::Null => SqlValue::Null,
        StoredColumn::Integer(v) => SqlValue::Integer(v),
        StoredColumn::Float(v) => SqlValue::Float(v),
        StoredColumn::Text(bytes) => SqlValue::Text(String::from_utf8_lossy(bytes).into_owned()),
        StoredColumn::Blob(bytes) => SqlValue::Blob(bytes.to_vec()),
    })
}

/// Decodes all columns from `row_bytes`.
pub fn decode_all_columns(row_bytes: &[u8]) -> io::Result<Vec<SqlValue>> {
    let col_count = stored_column_count(row_bytes)?;
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
    fn encoder_checks_row_format_limits_before_narrowing() {
        let mut columns = vec![SqlValue::Null; usize::from(u16::MAX)];
        let row = try_encode_row(&columns).unwrap();
        assert_eq!(decode_all_columns(&row).unwrap(), columns);
        columns.push(SqlValue::Null);
        assert_eq!(
            try_encode_row(&columns).unwrap_err().kind(),
            ErrorKind::FileTooLarge
        );
        assert_eq!(add_cell_size(u32::MAX - 5, 4, 1).unwrap(), u32::MAX);
        assert_eq!(
            add_cell_size(u32::MAX - 5, 4, 2).unwrap_err().kind(),
            ErrorKind::FileTooLarge
        );
    }

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

        let mut nul_b = Vec::new();
        let mut nul_c = Vec::new();
        let mut nul_short = Vec::new();
        let mut plain_a = Vec::new();
        let mut nul_bb = Vec::new();
        let mut plain_aa = Vec::new();
        ColCollation::NoCase.push_normalized_text(b"a\0b", &mut nul_b);
        ColCollation::NoCase.push_normalized_text(b"A\0c", &mut nul_c);
        ColCollation::NoCase.push_normalized_text(b"a\0", &mut nul_short);
        ColCollation::NoCase.push_normalized_text(b"a", &mut plain_a);
        ColCollation::NoCase.push_normalized_text(b"a\0bb", &mut nul_bb);
        ColCollation::NoCase.push_normalized_text(b"aa", &mut plain_aa);
        assert_eq!(nul_b, b"a\0\0");
        assert_eq!(nul_b, nul_c);
        assert!(plain_a < nul_short);
        assert!(nul_short < nul_b);
        assert!(nul_b < nul_bb);
        assert!(nul_bb < plain_aa);

        out.clear();
        ColCollation::RTrim.push_normalized_text(b"hello   ", &mut out);
        assert_eq!(out, b"hello");

        assert_eq!(parse_sqlite_numeric_str(" 42 "), Some(Ok(42)));
        assert_eq!(parse_sqlite_numeric_str(" 42.0 "), Some(Ok(42)));
        assert_eq!(parse_sqlite_numeric_str(" -1.5 "), Some(Err(-1.5)));
        assert_eq!(parse_sqlite_numeric_str(" 42\t\n\x0b\x0c\r "), Some(Ok(42)));
        assert_eq!(parse_sqlite_numeric_str("\x0b42\x0b"), Some(Ok(42)));
        assert_eq!(parse_sqlite_numeric_str("\u{00a0}42"), None);
        assert_eq!(parse_sqlite_numeric_str("\u{2003}42"), None);
        assert_eq!(parse_sqlite_numeric_str("1e500"), Some(Err(f64::INFINITY)));
        assert_eq!(
            parse_sqlite_numeric_str("-1e500"),
            Some(Err(f64::NEG_INFINITY))
        );
        assert_eq!(parse_sqlite_numeric_str("inf"), None);
        assert_eq!(parse_sqlite_numeric_str("NaN"), None);
        assert_eq!(parse_sqlite_numeric_str("abc"), None);

        // SQLite strict open coercion interval (-2^63, 2^63): -2^63.0 stays REAL on storage
        // coercion while -2^63+1024 coerces to Integer and canonical keys remain equivalent.
        assert_eq!(f64_to_exact_i64(-9_223_372_036_854_775_808.0), None);
        assert_eq!(
            f64_to_exact_i64(-9_223_372_036_854_774_784.0),
            Some(-9_223_372_036_854_774_784)
        );
        assert_eq!(f64_to_exact_i64(9_223_372_036_854_775_808.0), None);
        assert_eq!(
            parse_sqlite_numeric_str("-9223372036854775808.0"),
            Some(Err(-9_223_372_036_854_775_808.0))
        );
        assert_eq!(
            parse_sqlite_numeric_str("-9223372036854775808"),
            Some(Ok(i64::MIN))
        );
        assert_eq!(
            encode_canonical_numeric_key_from_f64(-9_223_372_036_854_775_808.0),
            encode_i64_tagged_key(i64::MIN).to_vec()
        );
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

        // Positive and negative NaN patterns normalize identically and sort strictly after +Inf.
        assert_eq!(
            encode_f64_memcomparable(-f64::NAN),
            encode_f64_memcomparable(f64::NAN)
        );
        assert!(encode_f64_memcomparable(f64::INFINITY) < encode_f64_memcomparable(-f64::NAN));
        assert_eq!(
            SqlValue::Float(-f64::NAN).to_key_bytes(),
            SqlValue::Float(f64::NAN).to_key_bytes()
        );
        assert!(
            SqlValue::Float(f64::INFINITY).to_key_bytes()
                < SqlValue::Float(-f64::NAN).to_key_bytes()
        );

        // format_sqlite_float_text matches SQLite's %.15g / %!.15g formatting
        assert_eq!(format_sqlite_float_text(0.0), "0.0");
        assert_eq!(format_sqlite_float_text(-0.0), "0.0");
        assert_eq!(format_sqlite_float_text(42.0), "42.0");
        assert_eq!(format_sqlite_float_text(-1.5), "-1.5");
        assert_eq!(
            format_sqlite_float_text(123_456_789_012_345.5),
            "123456789012346.0"
        );
        assert_eq!(
            format_sqlite_float_text(100_000_000_000_000.5),
            "100000000000000.0"
        );
        assert_eq!(format_sqlite_float_text(999_999_999_999_999.9), "1.0e+15");
        assert_eq!(format_sqlite_float_text(-999_999_999_999_999.9), "-1.0e+15");
        assert_eq!(format_sqlite_float_text(9.999_999_999_999_999e-5), "0.0001");
        assert_eq!(format_sqlite_float_text(1e-5), "1.0e-05");
        assert_eq!(format_sqlite_float_text(1e20), "1.0e+20");
        assert_eq!(format_sqlite_float_text(1.25e20), "1.25e+20");
        assert_eq!(format_sqlite_float_text(-1e-20), "-1.0e-20");
        assert_eq!(format_sqlite_float_text(f64::INFINITY), "Inf");
        assert_eq!(format_sqlite_float_text(f64::NEG_INFINITY), "-Inf");
        assert_eq!(format_sqlite_float_text(f64::NAN), "NaN");

        {
            use crate::vtab::ffi::{
                SQLITE_OK, SQLITE_ROW, sqlite_column_text_bytes, sqlite3_close, sqlite3_finalize,
                sqlite3_open, sqlite3_prepare_v2, sqlite3_step,
            };
            use core::ffi::c_char;

            let mut db = core::ptr::null_mut();
            let mem = b":memory:\0";
            assert_eq!(
                unsafe { sqlite3_open(mem.as_ptr() as *const c_char, &mut db) },
                SQLITE_OK
            );
            let mut stmt = core::ptr::null_mut();
            let sql = b"SELECT CAST(1e500 AS TEXT), CAST(-1e500 AS TEXT)\0";
            assert_eq!(
                unsafe {
                    sqlite3_prepare_v2(
                        db,
                        sql.as_ptr() as *const c_char,
                        -1,
                        &mut stmt,
                        core::ptr::null_mut(),
                    )
                },
                SQLITE_OK
            );
            assert_eq!(unsafe { sqlite3_step(stmt) }, SQLITE_ROW);
            let pos_inf = unsafe { sqlite_column_text_bytes(stmt, 0) };
            let neg_inf = unsafe { sqlite_column_text_bytes(stmt, 1) };
            assert_eq!(pos_inf, format_sqlite_float_text(f64::INFINITY).as_bytes());
            assert_eq!(
                neg_inf,
                format_sqlite_float_text(f64::NEG_INFINITY).as_bytes()
            );
            unsafe {
                sqlite3_finalize(stmt);
                sqlite3_close(db);
            }
        }
    }

    #[test]
    fn sqlite_pointer_before_bytes_helpers_convert_numeric_values() {
        use crate::vtab::ffi::{
            SQLITE_OK, SQLITE_ROW, sqlite_column_blob_bytes, sqlite_column_text_bytes,
            sqlite3_close, sqlite3_column_value, sqlite3_finalize, sqlite3_open,
            sqlite3_prepare_v2, sqlite3_step,
        };
        use core::ffi::c_char;

        let mut db = core::ptr::null_mut();
        let mem = b":memory:\0";
        assert_eq!(
            unsafe { sqlite3_open(mem.as_ptr() as *const c_char, &mut db) },
            SQLITE_OK
        );
        let mut stmt = core::ptr::null_mut();
        let sql = b"SELECT 123456789012345, -42.5, x'00ff'\0";
        assert_eq!(
            unsafe {
                sqlite3_prepare_v2(
                    db,
                    sql.as_ptr() as *const c_char,
                    -1,
                    &mut stmt,
                    core::ptr::null_mut(),
                )
            },
            SQLITE_OK
        );
        assert_eq!(unsafe { sqlite3_step(stmt) }, SQLITE_ROW);

        unsafe {
            assert_eq!(
                sqlite_value_text_bytes(sqlite3_column_value(stmt, 0)),
                b"123456789012345"
            );
            assert_eq!(sqlite_column_text_bytes(stmt, 1), b"-42.5");
            assert_eq!(
                sqlite_value_blob_bytes(sqlite3_column_value(stmt, 2)),
                &[0x00, 0xFF]
            );
            assert_eq!(sqlite_column_blob_bytes(stmt, 2), &[0x00, 0xFF]);
            sqlite3_finalize(stmt);
            sqlite3_close(db);
        }
    }

    #[test]
    fn extract_column_key_bytes_fails_closed_on_corrupt_rows() {
        // Short headers must fail with InvalidData.
        for short in [&[][..], &[1, 0, 0][..]] {
            let err = extract_column_key_bytes(short, 0).unwrap_err();
            assert_eq!(err.kind(), ErrorKind::InvalidData);
        }

        // Valid NULL and out-of-range column index on a well-formed header remain Ok(KEY_TAG_NULL).
        let valid_row = encode_row(&[SqlValue::Null, SqlValue::Integer(42)]);
        let mut overlapping = encode_row(&[SqlValue::Blob(vec![1, 2]), SqlValue::Integer(42)]);
        overlapping[14..18].copy_from_slice(&3u32.to_le_bytes());
        let mut inside_directory = valid_row.clone();
        inside_directory[10..14].copy_from_slice(&4u32.to_le_bytes());
        for (row, column) in [(&overlapping, 0), (&inside_directory, 1)] {
            assert_eq!(
                decode_column_at(row, column).unwrap_err().kind(),
                ErrorKind::InvalidData
            );
            assert_eq!(
                extract_column_key_bytes(row, column).unwrap_err().kind(),
                ErrorKind::InvalidData
            );
        }
        assert_eq!(
            extract_column_key_bytes(&valid_row, 0).unwrap(),
            vec![KEY_TAG_NULL]
        );
        assert_eq!(
            extract_column_key_bytes(&valid_row, 2).unwrap(),
            vec![KEY_TAG_NULL]
        );

        // Truncated column directory.
        let mut trunc_dir = vec![2, 0, 0, 0];
        trunc_dir.extend_from_slice(&[0, 0, 0, 0, 0]);
        assert_eq!(
            extract_column_key_bytes(&trunc_dir, 1).unwrap_err().kind(),
            ErrorKind::InvalidData
        );

        // Helper for building a single-column row header with arbitrary (type_tag, offset, tail).
        let make_row = |type_tag: u8, offset: u32, tail: &[u8]| {
            let mut buf = vec![1, 0, 0, 0, type_tag];
            buf.extend_from_slice(&offset.to_le_bytes());
            buf.extend_from_slice(tail);
            buf
        };

        // Truncated Integer and Float payloads (and offset overflow bounds).
        for (tag, tail) in [(1u8, &[1u8, 2, 3][..]), (2u8, &[1u8, 2, 3, 4, 5, 6, 7][..])] {
            let row = make_row(tag, 9, tail);
            assert_eq!(
                extract_column_key_bytes(&row, 0).unwrap_err().kind(),
                ErrorKind::InvalidData
            );
            let overflow = make_row(tag, u32::MAX - 2, &[]);
            assert_eq!(
                extract_column_key_bytes(&overflow, 0).unwrap_err().kind(),
                ErrorKind::InvalidData
            );
        }

        // Truncated Text / Blob length prefixes, truncated payloads, and overflowing u32::MAX len.
        for tag in [3u8, 4u8] {
            let trunc_len = make_row(tag, 9, &[1, 0]);
            assert_eq!(
                extract_column_key_bytes(&trunc_len, 0).unwrap_err().kind(),
                ErrorKind::InvalidData
            );

            let mut trunc_payload = 5u32.to_le_bytes().to_vec();
            trunc_payload.extend_from_slice(b"abc");
            let row_trunc_payload = make_row(tag, 9, &trunc_payload);
            assert_eq!(
                extract_column_key_bytes(&row_trunc_payload, 0)
                    .unwrap_err()
                    .kind(),
                ErrorKind::InvalidData
            );

            let row_huge_len = make_row(tag, 9, &u32::MAX.to_le_bytes());
            assert_eq!(
                extract_column_key_bytes(&row_huge_len, 0)
                    .unwrap_err()
                    .kind(),
                ErrorKind::InvalidData
            );
        }

        // Unknown type tag must fail with InvalidData instead of impersonating NULL.
        let unknown_tag = make_row(99, 9, &[]);
        assert_eq!(
            extract_column_key_bytes(&unknown_tag, 0)
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidData
        );

        // `read_stored_i64_column` rejects truncated/overflowed integer directories safely.
        assert_eq!(
            read_stored_i64_column(&make_row(1, 9, &[1, 2, 3]), 0)
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidData
        );
        assert_eq!(
            read_stored_i64_column(&make_row(1, u32::MAX - 2, &[]), 0)
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidData
        );
        assert_eq!(read_stored_i64_column(&valid_row, 0).unwrap(), None);
    }
}

#[cfg(kani)]
mod kani_proofs {
    use super::*;

    fn assert_bytes_eq(a: &[u8], b: &[u8]) {
        assert_eq!(a.len(), b.len());
        let mut i = 0;
        while i < a.len() {
            assert_eq!(a[i], b[i]);
            i += 1;
        }
    }

    #[kani::proof]
    #[kani::unwind(4)]
    fn verify_row_codec_decode_arbitrary_bytes_no_panic() {
        let buf: [u8; 32] = kani::any();
        let len: usize = kani::any();
        kani::assume(len <= buf.len());
        let row = &buf[..len];
        let col_idx: usize = kani::any();
        kani::assume(col_idx <= 6);

        let _ = stored_column_count(row);
        let _ = stored_column(row, col_idx);
        let _ = read_stored_i64_column(row, col_idx);
    }

    #[kani::proof]
    #[kani::unwind(6)]
    fn verify_row_codec_extract_key_binary_no_panic() {
        let buf: [u8; 20] = kani::any();
        let len: usize = kani::any();
        kani::assume(len <= buf.len());
        let row = &buf[..len];
        let col_idx: usize = kani::any();
        kani::assume(col_idx <= 3);

        if row.len() >= 4 + (col_idx + 1) * 5 && row[4 + col_idx * 5] == 2 {
            return;
        }
        let _ = extract_column_key_bytes_with_collation(row, col_idx, ColCollation::Binary);
    }

    #[kani::proof]
    #[kani::unwind(6)]
    fn verify_row_codec_collation_nocase_props() {
        let buf: [u8; 4] = kani::any();
        let len: usize = kani::any();
        kani::assume(len <= buf.len());
        let s = &buf[..len];

        let mut nocase = Vec::with_capacity(6);
        ColCollation::NoCase.push_normalized_text(s, &mut nocase);
        assert_eq!(nocase.len(), s.len());
        let mut saw_nul = false;
        let mut i = 0;
        while i < s.len() {
            if s[i] == 0 {
                saw_nul = true;
            }
            if saw_nul {
                assert_eq!(nocase[i], 0);
            } else {
                assert!(!(b'A'..=b'Z').contains(&nocase[i]));
            }
            i += 1;
        }
    }

    #[kani::proof]
    #[kani::unwind(6)]
    fn verify_row_codec_collation_rtrim_props() {
        let buf: [u8; 4] = kani::any();
        let len: usize = kani::any();
        kani::assume(len <= buf.len());
        let s = &buf[..len];

        let mut rtrim = Vec::with_capacity(6);
        ColCollation::RTrim.push_normalized_text(s, &mut rtrim);
        assert!(rtrim.len() <= s.len());
        if !rtrim.is_empty() {
            assert_ne!(rtrim[rtrim.len() - 1], b' ');
        }
        assert_bytes_eq(&rtrim, &s[..rtrim.len()]);
    }

    #[kani::proof]
    #[kani::unwind(4)]
    fn verify_row_codec_try_encode_one_col_roundtrip() {
        let tag: u8 = kani::any();
        kani::assume(tag < 5);
        match tag {
            0 => {
                let encoded = try_encode_row(&[SqlValue::Null]).unwrap();
                assert_eq!(stored_column_count(&encoded).unwrap(), 1);
                assert!(matches!(
                    stored_column(&encoded, 0).unwrap(),
                    StoredColumn::Null
                ));
            }
            1 => {
                let v: i64 = kani::any();
                let encoded = try_encode_row(&[SqlValue::Integer(v)]).unwrap();
                assert_eq!(stored_column_count(&encoded).unwrap(), 1);
                assert!(matches!(
                    stored_column(&encoded, 0).unwrap(),
                    StoredColumn::Integer(dv) if dv == v
                ));
            }
            2 => {
                let bits: u64 = kani::any();
                let encoded = try_encode_row(&[SqlValue::Float(f64::from_bits(bits))]).unwrap();
                assert_eq!(stored_column_count(&encoded).unwrap(), 1);
                match stored_column(&encoded, 0).unwrap() {
                    StoredColumn::Float(dv) => assert_eq!(dv.to_bits(), bits),
                    _ => panic!("decoded column variant mismatch"),
                }
            }
            3 => {
                let encoded = try_encode_row(&[SqlValue::Text(String::from("ab"))]).unwrap();
                assert_eq!(stored_column_count(&encoded).unwrap(), 1);
                match stored_column(&encoded, 0).unwrap() {
                    StoredColumn::Text(db) => assert_bytes_eq(db, b"ab"),
                    _ => panic!("decoded column variant mismatch"),
                }
            }
            _ => {
                let b0: u8 = kani::any();
                let b1: u8 = kani::any();
                let expected = [b0, b1];
                let encoded = try_encode_row(&[SqlValue::Blob(vec![b0, b1])]).unwrap();
                assert_eq!(stored_column_count(&encoded).unwrap(), 1);
                match stored_column(&encoded, 0).unwrap() {
                    StoredColumn::Blob(db) => assert_bytes_eq(db, &expected),
                    _ => panic!("decoded column variant mismatch"),
                }
            }
        }
    }

    #[kani::proof]
    #[kani::unwind(4)]
    fn verify_row_codec_try_encode_two_col_roundtrip() {
        let a: i64 = kani::any();
        let b0: u8 = kani::any();
        let b1: u8 = kani::any();
        let expected = [b0, b1];
        let enc2 = try_encode_row(&[SqlValue::Integer(a), SqlValue::Blob(vec![b0, b1])]).unwrap();
        assert_eq!(stored_column_count(&enc2).unwrap(), 2);
        assert!(matches!(
            stored_column(&enc2, 0).unwrap(),
            StoredColumn::Integer(v) if v == a
        ));
        match stored_column(&enc2, 1).unwrap() {
            StoredColumn::Blob(db) => assert_bytes_eq(db, &expected),
            _ => panic!("expected second column to be Blob"),
        }
    }

    #[kani::proof]
    #[kani::unwind(2)]
    fn verify_row_codec_i64_and_f64_memcomparable_roundtrip_and_order() {
        let a: i64 = kani::any();
        let b: i64 = kani::any();
        let ka = u64::from_be_bytes(encode_i64_memcomparable(a));
        let kb = u64::from_be_bytes(encode_i64_memcomparable(b));
        assert_eq!(decode_i64_memcomparable(ka.to_be_bytes()), a);
        assert_eq!(a < b, ka < kb);
        let ta = encode_i64_tagged_key(a);
        assert_eq!(ta[0], KEY_TAG_INTEGER);
        assert_eq!(decode_i64_tagged_key(&ta), Some(a));

        assert_eq!(
            u64::from_be_bytes(encode_f64_memcomparable(-0.0)),
            u64::from_be_bytes(encode_f64_memcomparable(0.0))
        );
        assert_eq!(
            u64::from_be_bytes(encode_f64_memcomparable(-f64::NAN)),
            u64::from_be_bytes(encode_f64_memcomparable(f64::NAN))
        );

        let bits: u64 = kani::any();
        let is_zero = (bits & 0x7FFF_FFFF_FFFF_FFFF) == 0;
        let is_nan = (bits & 0x7FF0_0000_0000_0000) == 0x7FF0_0000_0000_0000
            && (bits & 0x000F_FFFF_FFFF_FFFF) != 0;
        if !is_zero && !is_nan {
            let decoded = decode_f64_memcomparable(encode_f64_memcomparable(f64::from_bits(bits)));
            assert_eq!(decoded.to_bits(), bits);
        }

        let fx: f64 = kani::any();
        let fy: f64 = kani::any();
        kani::assume(fx.is_finite() && fy.is_finite());
        if fx < fy {
            assert!(
                u64::from_be_bytes(encode_f64_memcomparable(fx))
                    < u64::from_be_bytes(encode_f64_memcomparable(fy))
            );
        }
    }
}
