//! Memcomparable binary key encoding (`TableID`, namespace tags, order-preserving integers,
//! and escape-terminated composite byte strings).
//!
//! All keys encoded by this module preserve logical sort order under standard unsigned
//! lexicographical byte comparison (`[u8]::cmp` / `memcmp`).

use std::io::{self, ErrorKind};

/// Namespace tag for Primary Row Keys (`[TableID: u16 BE | b'r' | EncodedPK]`).
pub const NS_PRIMARY_ROW: u8 = b'r';
/// Namespace tag for Secondary Index Keys (`[TableID: u16 BE | b'i' | IndexID: u16 BE | EncodedCol | EncodedPK]`).
pub const NS_SECONDARY_INDEX: u8 = b'i';
/// Namespace tag for LRU Eviction Queue Keys (`[TableID: u16 BE | b'l' | AccessTick: u64 BE | EncodedPK]`).
pub const NS_LRU_QUEUE: u8 = b'l';
/// Namespace tag for Reverse Tick & Metadata Keys (`[TableID: u16 BE | b't' | EncodedPK]`).
pub const NS_ROW_META: u8 = b't';

/// Encodes a signed `i64` into an 8-byte order-preserving big-endian representation
/// by flipping the sign bit (`1 << 63`).
#[inline]
pub fn encode_i64_memcomparable(val: i64) -> [u8; 8] {
    let flipped = (val as u64) ^ (1u64 << 63);
    flipped.to_be_bytes()
}

/// Decodes an 8-byte order-preserving big-endian representation back into a signed `i64`.
#[inline]
pub fn decode_i64_memcomparable(bytes: [u8; 8]) -> i64 {
    let flipped = u64::from_be_bytes(bytes);
    (flipped ^ (1u64 << 63)) as i64
}

/// Appends a variable-length byte slice in memcomparable escape-terminated format
/// (`0x00` -> `0x00 0xFF`, terminated by `0x00 0x01`) so composite keys preserve
/// component-wise lexicographical order without prefix collisions.
pub fn encode_bytes_escaped(src: &[u8], dst: &mut Vec<u8>) {
    dst.reserve(src.len() + 2);
    for &b in src {
        if b == 0x00 {
            dst.push(0x00);
            dst.push(0xFF);
        } else {
            dst.push(b);
        }
    }
    dst.push(0x00);
    dst.push(0x01);
}

/// Decodes the first escape-terminated byte slice from `input`, returning `(decoded_bytes, remaining_slice)`.
pub fn decode_bytes_escaped(input: &[u8]) -> io::Result<(Vec<u8>, &[u8])> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < input.len() {
        let b = input[i];
        if b == 0x00 {
            if i + 1 >= input.len() {
                return Err(io::Error::new(
                    ErrorKind::InvalidData,
                    "truncated escape sequence in memcomparable key",
                ));
            }
            match input[i + 1] {
                0x01 => return Ok((out, &input[i + 2..])),
                0xFF => {
                    out.push(0x00);
                    i += 2;
                }
                other => {
                    return Err(io::Error::new(
                        ErrorKind::InvalidData,
                        format!("invalid escape byte 0x{other:02X} in memcomparable key"),
                    ));
                }
            }
        } else {
            out.push(b);
            i += 1;
        }
    }
    Err(io::Error::new(
        ErrorKind::InvalidData,
        "missing 0x00 0x01 terminator in memcomparable key",
    ))
}

/// Encodes a 3-byte table + namespace prefix (`[TableID: u16 BE | ns_tag: u8]`).
#[inline]
pub fn encode_namespace_prefix(table_id: u16, ns_tag: u8) -> [u8; 3] {
    let tb = table_id.to_be_bytes();
    [tb[0], tb[1], ns_tag]
}

/// Encodes a Primary Row Key: `[TableID: u16 BE | 'r' | pk]`.
pub fn encode_primary_row_key(table_id: u16, pk: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(3 + pk.len());
    out.extend_from_slice(&encode_namespace_prefix(table_id, NS_PRIMARY_ROW));
    out.extend_from_slice(pk);
    out
}

/// Extracts the raw `pk` slice from a Primary Row Key belonging to `table_id`.
pub fn decode_primary_row_key(table_id: u16, full_key: &[u8]) -> Option<&[u8]> {
    let prefix = encode_namespace_prefix(table_id, NS_PRIMARY_ROW);
    if full_key.starts_with(&prefix) {
        Some(&full_key[3..])
    } else {
        None
    }
}

/// Encodes a Reverse Tick & Metadata Key: `[TableID: u16 BE | 't' | pk]`.
pub fn encode_row_meta_key(table_id: u16, pk: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(3 + pk.len());
    out.extend_from_slice(&encode_namespace_prefix(table_id, NS_ROW_META));
    out.extend_from_slice(pk);
    out
}

/// Encodes an LRU Queue Key: `[TableID: u16 BE | 'l' | AccessTick: u64 BE | pk]`.
pub fn encode_lru_queue_key(table_id: u16, access_tick: u64, pk: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(11 + pk.len());
    out.extend_from_slice(&encode_namespace_prefix(table_id, NS_LRU_QUEUE));
    out.extend_from_slice(&access_tick.to_be_bytes());
    out.extend_from_slice(pk);
    out
}

/// Decodes `(access_tick, pk)` from an LRU Queue Key belonging to `table_id`.
pub fn decode_lru_queue_key(table_id: u16, full_key: &[u8]) -> Option<(u64, &[u8])> {
    let prefix = encode_namespace_prefix(table_id, NS_LRU_QUEUE);
    if full_key.len() < 11 || !full_key.starts_with(&prefix) {
        return None;
    }
    let tick = u64::from_be_bytes(full_key[3..11].try_into().ok()?);
    Some((tick, &full_key[11..]))
}

/// Encodes a Secondary Index Key:
/// `[TableID: u16 BE | 'i' | IndexID: u16 BE | Escaped(col_val) | pk]`.
pub fn encode_secondary_index_key(
    table_id: u16,
    index_id: u16,
    col_val: &[u8],
    pk: &[u8],
) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + col_val.len() + 2 + pk.len());
    out.extend_from_slice(&encode_namespace_prefix(table_id, NS_SECONDARY_INDEX));
    out.extend_from_slice(&index_id.to_be_bytes());
    encode_bytes_escaped(col_val, &mut out);
    out.extend_from_slice(pk);
    out
}

/// Encodes the 5-byte prefix `[TableID: u16 BE | 'i' | IndexID: u16 BE]` for scanning all
/// entries belonging to secondary index `(table_id, index_id)` (`P1`).
#[inline]
pub fn encode_secondary_index_prefix(table_id: u16, index_id: u16) -> [u8; 5] {
    let tb = table_id.to_be_bytes();
    let ib = index_id.to_be_bytes();
    [tb[0], tb[1], NS_SECONDARY_INDEX, ib[0], ib[1]]
}

/// Encodes the prefix for scanning all Secondary Index entries matching `(table_id, index_id, col_val)`.
pub fn encode_secondary_index_exact_prefix(
    table_id: u16,
    index_id: u16,
    col_val: &[u8],
) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + col_val.len() + 2);
    out.extend_from_slice(&encode_secondary_index_prefix(table_id, index_id));
    encode_bytes_escaped(col_val, &mut out);
    out
}

/// Decodes `(col_val, pk)` from a Secondary Index Key belonging to `(table_id, index_id)`.
pub fn decode_secondary_index_key(
    table_id: u16,
    index_id: u16,
    full_key: &[u8],
) -> io::Result<Option<(Vec<u8>, Vec<u8>)>> {
    let mut prefix = [0u8; 5];
    prefix[0..3].copy_from_slice(&encode_namespace_prefix(table_id, NS_SECONDARY_INDEX));
    prefix[3..5].copy_from_slice(&index_id.to_be_bytes());
    if !full_key.starts_with(&prefix) {
        return Ok(None);
    }
    let (col_val, rem) = decode_bytes_escaped(&full_key[5..])?;
    Ok(Some((col_val, rem.to_vec())))
}

/// Extracts the borrowed `pk` suffix directly from a Secondary Index Key belonging to
/// `(table_id, index_id)` with zero heap allocations (`P1`).
#[inline]
pub fn decode_secondary_index_pk_slice(
    table_id: u16,
    index_id: u16,
    full_key: &[u8],
) -> Option<&[u8]> {
    let mut prefix = [0u8; 5];
    prefix[0..3].copy_from_slice(&encode_namespace_prefix(table_id, NS_SECONDARY_INDEX));
    prefix[3..5].copy_from_slice(&index_id.to_be_bytes());
    if !full_key.starts_with(&prefix) {
        return None;
    }
    let body = &full_key[5..];
    let mut i = 0;
    while i < body.len() {
        if body[i] == 0x00 {
            if i + 1 >= body.len() {
                return None;
            }
            match body[i + 1] {
                0x01 => return Some(&body[i + 2..]),
                0xFF => i += 2,
                _ => return None,
            }
        } else {
            i += 1;
        }
    }
    None
}

/// Per-row metadata stored under `[TableID | 't' | pk]` for Capped Tables.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowMetaRecord {
    /// The `AccessTick` currently indexed under `[TableID | 'l' | recorded_tick | pk]`.
    pub recorded_tick: u64,
    /// The logical byte footprint of the row (`key.len() + val.len()`) counted toward `MAX_BYTES`.
    pub logical_size: u32,
    /// Secondary index values (`(index_id, col_val)`) associated with this row so cascading
    /// eviction or overwrite can delete secondary index entries without deserializing the row blob.
    pub secondary_keys: Vec<(u16, Vec<u8>)>,
}

impl RowMetaRecord {
    pub fn encode_slice(
        recorded_tick: u64,
        logical_size: u32,
        secondary_keys: &[(u16, Vec<u8>)],
    ) -> Vec<u8> {
        let mut out = Vec::with_capacity(14 + secondary_keys.len() * 12);
        out.extend_from_slice(&recorded_tick.to_be_bytes());
        out.extend_from_slice(&logical_size.to_be_bytes());
        out.extend_from_slice(&(secondary_keys.len() as u16).to_be_bytes());
        for (idx_id, col_val) in secondary_keys {
            out.extend_from_slice(&idx_id.to_be_bytes());
            out.extend_from_slice(&(col_val.len() as u16).to_be_bytes());
            out.extend_from_slice(col_val);
        }
        out
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        Self::encode_slice(self.recorded_tick, self.logical_size, &self.secondary_keys)
    }

    pub fn from_bytes(buf: &[u8]) -> io::Result<Self> {
        if buf.len() < 14 {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                "RowMetaRecord shorter than 14 bytes",
            ));
        }
        let recorded_tick = u64::from_be_bytes(buf[0..8].try_into().unwrap());
        let logical_size = u32::from_be_bytes(buf[8..12].try_into().unwrap());
        let sec_count = u16::from_be_bytes(buf[12..14].try_into().unwrap()) as usize;
        let mut pos = 14;
        let mut secondary_keys = Vec::with_capacity(sec_count);
        for _ in 0..sec_count {
            if pos + 4 > buf.len() {
                return Err(io::Error::new(
                    ErrorKind::InvalidData,
                    "truncated secondary key header in RowMetaRecord",
                ));
            }
            let idx_id = u16::from_be_bytes(buf[pos..pos + 2].try_into().unwrap());
            let vlen = u16::from_be_bytes(buf[pos + 2..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            if pos + vlen > buf.len() {
                return Err(io::Error::new(
                    ErrorKind::InvalidData,
                    "truncated secondary key payload in RowMetaRecord",
                ));
            }
            secondary_keys.push((idx_id, buf[pos..pos + vlen].to_vec()));
            pos += vlen;
        }
        Ok(Self {
            recorded_tick,
            logical_size,
            secondary_keys,
        })
    }
}
