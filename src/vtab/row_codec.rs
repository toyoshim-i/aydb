//! Compact binary row serialization with an $O(1)$ column offset directory,
//! plus memcomparable key conversion for SQLite [`sqlite3_value`] items.

use std::io::{self, ErrorKind};

use crate::lru::{decode_i64_memcomparable, encode_i64_memcomparable};
use crate::vtab::ffi::{
    SQLITE_BLOB, SQLITE_FLOAT, SQLITE_INTEGER, SQLITE_NULL, SQLITE_TEXT, sqlite_transient,
    sqlite3_context, sqlite3_result_blob, sqlite3_result_double, sqlite3_result_int64,
    sqlite3_result_null, sqlite3_result_text, sqlite3_value, sqlite3_value_blob,
    sqlite3_value_bytes, sqlite3_value_double, sqlite3_value_int64, sqlite3_value_text,
    sqlite3_value_type,
};

/// 1-byte key storage-class type tags ordered according to SQLite's comparison rules:
/// `NULL < INTEGER/REAL < TEXT < BLOB` (`AUD-05`).
pub const KEY_TAG_NULL: u8 = 0x05;
pub const KEY_TAG_INTEGER: u8 = 0x10;
pub const KEY_TAG_FLOAT: u8 = 0x18;
pub const KEY_TAG_TEXT: u8 = 0x20;
pub const KEY_TAG_BLOB: u8 = 0x30;

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
                SQLITE_BLOB | _ => {
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
                    sqlite3_result_text(
                        ctx,
                        s.as_ptr() as *const _,
                        s.len() as i32,
                        sqlite_transient(),
                    );
                }
                Self::Blob(b) => {
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

    /// Encodes this value into a standalone type-tagged memcomparable primary/secondary key component
    /// ordered according to SQLite's storage-class comparison rules (`NULL < INTEGER/REAL < TEXT < BLOB`):
    /// - `Null` -> `[0x05]` (1 byte).
    /// - `Integer(i)` -> `[0x10, encode_i64_memcomparable(i)..]` (9 bytes).
    /// - `Float(f)` -> `[0x18, encode_f64_memcomparable(f)..]` (9 bytes).
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
            Self::Float(v) => {
                let mut out = Vec::with_capacity(9);
                out.push(KEY_TAG_FLOAT);
                out.extend_from_slice(&encode_f64_memcomparable(*v));
                out
            }
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
            KEY_TAG_FLOAT if payload.len() == 8 => {
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
/// Also extracts the primary key bytes (`pk_col_idx`) and any secondary index `(index_id, col_key_bytes)`.
/// If `pk_override` is `Some(auto_id)` (for `INTEGER PRIMARY KEY` when `SQLITE_NULL` is inserted),
/// serializes `auto_id` as an `SQLITE_INTEGER` in the `pk_col_idx` slot (`USE-05`).
///
/// # Safety
/// `values` must point to `col_count` valid `*mut sqlite3_value` pointers.
pub unsafe fn encode_sqlite_values_direct(
    values: *mut *mut sqlite3_value,
    col_count: usize,
    pk_col_idx: usize,
    pk_override: Option<i64>,
    secondary_indexes: &[(usize, u16)],
) -> (Vec<u8>, Vec<u8>, Vec<(u16, Vec<u8>)>) {
    unsafe {
        let dir_start = 4;
        let payload_start = dir_start + col_count * 5;

        let mut total_payload = 0usize;
        for i in 0..col_count {
            if i == pk_col_idx && pk_override.is_some() {
                total_payload += 8;
                continue;
            }
            let vptr = *values.add(i);
            total_payload += match sqlite3_value_type(vptr) {
                SQLITE_NULL => 0,
                SQLITE_INTEGER | SQLITE_FLOAT => 8,
                SQLITE_TEXT | SQLITE_BLOB | _ => 4 + (sqlite3_value_bytes(vptr).max(0) as usize),
            };
        }

        let mut buf = vec![0u8; payload_start + total_payload];
        buf[0..2].copy_from_slice(&(col_count as u16).to_le_bytes());
        buf[2..4].copy_from_slice(&0u16.to_le_bytes());

        let mut write_pos = payload_start;
        for i in 0..col_count {
            let dir_pos = dir_start + i * 5;
            let offset_u32 = write_pos as u32;
            buf[dir_pos + 1..dir_pos + 5].copy_from_slice(&offset_u32.to_le_bytes());

            if i == pk_col_idx {
                if let Some(auto_id) = pk_override {
                    buf[dir_pos] = 1;
                    buf[write_pos..write_pos + 8].copy_from_slice(&auto_id.to_le_bytes());
                    write_pos += 8;
                    continue;
                }
            }

            let vptr = *values.add(i);
            match sqlite3_value_type(vptr) {
                SQLITE_NULL => {
                    buf[dir_pos] = 0;
                }
                SQLITE_INTEGER => {
                    buf[dir_pos] = 1;
                    let v = sqlite3_value_int64(vptr);
                    buf[write_pos..write_pos + 8].copy_from_slice(&v.to_le_bytes());
                    write_pos += 8;
                }
                SQLITE_FLOAT => {
                    buf[dir_pos] = 2;
                    let v = sqlite3_value_double(vptr);
                    buf[write_pos..write_pos + 8].copy_from_slice(&v.to_le_bytes());
                    write_pos += 8;
                }
                SQLITE_TEXT => {
                    buf[dir_pos] = 3;
                    let len = sqlite3_value_bytes(vptr).max(0) as usize;
                    buf[write_pos..write_pos + 4].copy_from_slice(&(len as u32).to_le_bytes());
                    let ptr = sqlite3_value_text(vptr);
                    if !ptr.is_null() && len > 0 {
                        let slice = core::slice::from_raw_parts(ptr, len);
                        buf[write_pos + 4..write_pos + 4 + len].copy_from_slice(slice);
                    }
                    write_pos += 4 + len;
                }
                SQLITE_BLOB | _ => {
                    buf[dir_pos] = 4;
                    let len = sqlite3_value_bytes(vptr).max(0) as usize;
                    buf[write_pos..write_pos + 4].copy_from_slice(&(len as u32).to_le_bytes());
                    let ptr = sqlite3_value_blob(vptr) as *const u8;
                    if !ptr.is_null() && len > 0 {
                        let slice = core::slice::from_raw_parts(ptr, len);
                        buf[write_pos + 4..write_pos + 4 + len].copy_from_slice(slice);
                    }
                    write_pos += 4 + len;
                }
            }
        }

        let pk_bytes = if let Some(auto_id) = pk_override {
            encode_i64_tagged_key(auto_id).to_vec()
        } else {
            sqlite_value_to_key_bytes(*values.add(pk_col_idx))
        };
        let mut sec_keys = Vec::with_capacity(secondary_indexes.len());
        for &(col_idx, index_id) in secondary_indexes {
            if col_idx < col_count {
                let sec_val = if col_idx == pk_col_idx {
                    if let Some(auto_id) = pk_override {
                        encode_i64_tagged_key(auto_id).to_vec()
                    } else {
                        sqlite_value_to_key_bytes(*values.add(col_idx))
                    }
                } else {
                    sqlite_value_to_key_bytes(*values.add(col_idx))
                };
                sec_keys.push((index_id, sec_val));
            }
        }

        (pk_bytes, buf, sec_keys)
    }
}

/// Converts a raw `*mut sqlite3_value` directly into type-tagged memcomparable key bytes
/// ordered according to SQLite's storage-class comparison rules (`NULL < INTEGER/REAL < TEXT < BLOB`).
///
/// # Safety
/// `vptr` must be a valid non-null `*mut sqlite3_value`.
#[inline]
pub unsafe fn sqlite_value_to_key_bytes(vptr: *mut sqlite3_value) -> Vec<u8> {
    unsafe {
        match sqlite3_value_type(vptr) {
            SQLITE_NULL => vec![KEY_TAG_NULL],
            SQLITE_INTEGER => encode_i64_tagged_key(sqlite3_value_int64(vptr)).to_vec(),
            SQLITE_FLOAT => {
                let mut out = Vec::with_capacity(9);
                out.push(KEY_TAG_FLOAT);
                out.extend_from_slice(&encode_f64_memcomparable(sqlite3_value_double(vptr)));
                out
            }
            SQLITE_TEXT => {
                let len = sqlite3_value_bytes(vptr).max(0) as usize;
                let ptr = sqlite3_value_text(vptr);
                let mut out = Vec::with_capacity(1 + len);
                out.push(KEY_TAG_TEXT);
                if !ptr.is_null() && len > 0 {
                    out.extend_from_slice(core::slice::from_raw_parts(ptr, len));
                }
                out
            }
            SQLITE_BLOB | _ => {
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
            let mut out = Vec::with_capacity(9);
            out.push(KEY_TAG_FLOAT);
            out.extend_from_slice(&encode_f64_memcomparable(v));
            Ok(out)
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
            let mut out = Vec::with_capacity(1 + len);
            out.push(KEY_TAG_TEXT);
            out.extend_from_slice(&row_bytes[offset + 4..offset + 4 + len]);
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
    let offset = u32::from_le_bytes(row_bytes[dir_pos + 1..dir_pos + 5].try_into().unwrap()) as usize;

    match type_tag {
        0 => Ok(SqlValue::Null),
        1 => {
            if offset + 8 > row_bytes.len() {
                return Err(io::Error::new(ErrorKind::InvalidData, "truncated Integer column"));
            }
            let v = i64::from_le_bytes(row_bytes[offset..offset + 8].try_into().unwrap());
            Ok(SqlValue::Integer(v))
        }
        2 => {
            if offset + 8 > row_bytes.len() {
                return Err(io::Error::new(ErrorKind::InvalidData, "truncated Float column"));
            }
            let v = f64::from_le_bytes(row_bytes[offset..offset + 8].try_into().unwrap());
            Ok(SqlValue::Float(v))
        }
        3 => {
            if offset + 4 > row_bytes.len() {
                return Err(io::Error::new(ErrorKind::InvalidData, "truncated Text length"));
            }
            let len = u32::from_le_bytes(row_bytes[offset..offset + 4].try_into().unwrap()) as usize;
            if offset + 4 + len > row_bytes.len() {
                return Err(io::Error::new(ErrorKind::InvalidData, "truncated Text payload"));
            }
            let s = String::from_utf8_lossy(&row_bytes[offset + 4..offset + 4 + len]).into_owned();
            Ok(SqlValue::Text(s))
        }
        4 => {
            if offset + 4 > row_bytes.len() {
                return Err(io::Error::new(ErrorKind::InvalidData, "truncated Blob length"));
            }
            let len = u32::from_le_bytes(row_bytes[offset..offset + 4].try_into().unwrap()) as usize;
            if offset + 4 + len > row_bytes.len() {
                return Err(io::Error::new(ErrorKind::InvalidData, "truncated Blob payload"));
            }
            Ok(SqlValue::Blob(row_bytes[offset + 4..offset + 4 + len].to_vec()))
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
        assert_eq!(decode_f64_memcomparable(encode_f64_memcomparable(-0.0)), 0.0);

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
        assert!(
            SqlValue::Float(-2.5).to_key_bytes() < SqlValue::Float(-1.0).to_key_bytes()
        );
        assert!(
            SqlValue::Float(-1.0).to_key_bytes() < SqlValue::Float(0.0).to_key_bytes()
        );

        // Verify cross-type key ordering: NULL < INTEGER < FLOAT < TEXT < BLOB
        let ordered_values = [
            SqlValue::Null,
            SqlValue::Integer(-100),
            SqlValue::Integer(0),
            SqlValue::Integer(100),
            SqlValue::Float(-100.0),
            SqlValue::Float(0.0),
            SqlValue::Float(100.0),
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
}
