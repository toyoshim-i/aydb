//! Compact binary row serialization with an $O(1)$ column offset directory,
//! plus memcomparable key conversion for SQLite [`sqlite3_value`] items.

use std::io::{self, ErrorKind};

use crate::lru::encode_i64_memcomparable;
use crate::vtab::ffi::{
    SQLITE_BLOB, SQLITE_FLOAT, SQLITE_INTEGER, SQLITE_NULL, SQLITE_TEXT, sqlite_transient,
    sqlite3_context, sqlite3_result_blob, sqlite3_result_double, sqlite3_result_int64,
    sqlite3_result_null, sqlite3_result_text, sqlite3_value, sqlite3_value_blob,
    sqlite3_value_bytes, sqlite3_value_double, sqlite3_value_int64, sqlite3_value_text,
    sqlite3_value_type,
};

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

    /// Encodes this value into a standalone memcomparable primary/secondary key component.
    /// - `Integer(i)` -> 1-byte tag `0x10` + 8-byte sign-flipped big-endian (`encode_i64_memcomparable(i)`).
    /// - `Text(s)` -> raw UTF-8 bytes (so string primary keys have zero tag overhead and preserve
    ///   exact lexicographical order).
    /// - `Blob(b)` -> raw bytes.
    pub fn to_key_bytes(&self) -> Vec<u8> {
        match self {
            Self::Null => Vec::new(),
            Self::Integer(v) => encode_i64_memcomparable(*v).to_vec(),
            Self::Float(v) => v.to_be_bytes().to_vec(),
            Self::Text(s) => s.as_bytes().to_vec(),
            Self::Blob(b) => b.clone(),
        }
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
