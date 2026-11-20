//! Persistent `rowid` identities for non-integer SQLite primary keys: synthetic `rowid`
//! assignment, and 64-bit hardware CRC32C + SplitMix64 `rowid` hashing helpers.

use std::io;

use crate::engine::StorageEngine;
#[cfg(test)]
use crate::engine::WriteDurability;
use crate::pal::PlatformFs;
use crate::util::crc32c::crc32c_append;
use crate::vtab::AydbVTab;
use crate::vtab::row_codec::{decode_i64_tagged_key, extract_column_key_bytes_with_collation};
use crate::wal::WalRecord;

pub(super) fn plan_synthetic_rowid<FS: PlatformFs>(
    engine: &mut StorageEngine<FS>,
    table_id: u16,
    pk: &[u8],
    pk_rowid_alias: bool,
) -> io::Result<(i64, Vec<WalRecord>)> {
    engine.plan_assign_rowid(table_id, pk, hash_pk_to_rowid(pk), |eng, rowid| {
        Ok(pk_rowid_alias && eng.row_exists(table_id, &super::encode_i64_tagged_key(rowid))?)
    })
}

#[cfg(test)]
pub(super) fn assign_synthetic_rowid<FS: PlatformFs>(
    engine: &mut StorageEngine<FS>,
    table_id: u16,
    pk: &[u8],
    durability: WriteDurability,
    pk_rowid_alias: bool,
) -> io::Result<i64> {
    engine.assign_rowid(
        table_id,
        pk,
        hash_pk_to_rowid(pk),
        durability,
        |engine, rowid| {
            Ok(pk_rowid_alias
                && engine.row_exists(table_id, &super::encode_i64_tagged_key(rowid))?)
        },
    )
}

/// Computes the first rowid candidate. CRC collisions are resolved by the persistent reverse
/// index, because changing the CRC seed does not provide independent entropy for equal lengths.
#[inline]
pub fn hash_pk_to_rowid(encoded_pk: &[u8]) -> i64 {
    let h1 = crc32c_append(0x0000_0000, encoded_pk) as u64;
    let h2 = crc32c_append(0x9E37_79B9, encoded_pk) as u64;
    let mut z = (h1 << 32) | h2;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    (z ^ (z >> 31)) as i64
}

/// Extracts the `rowid` (`i64`) from `row_bytes` at `vtab.pk_col_idx`.
/// For `INTEGER PRIMARY KEY` rowid aliases (`vtab.pk_rowid_alias` and `type_tag == 1`), reads
/// the 8-byte `i64` directly in $O(1)$ with zero allocation.
/// For all other primary keys (including non-alias `INT PRIMARY KEY` and `TEXT`/`BLOB`/`REAL`),
/// reads the persistent synthetic `rowid` from the engine.
#[inline]
pub(super) fn extract_rowid_from_row(
    row_bytes: &[u8],
    vtab: &AydbVTab,
    snapshot: Option<&crate::engine::ReadSnapshotToken>,
) -> io::Result<i64> {
    let dir_pos = 4 + vtab.pk_col_idx * 5;
    if vtab.pk_rowid_alias && dir_pos + 5 <= row_bytes.len() && row_bytes[dir_pos] == 1 {
        let offset =
            u32::from_le_bytes(row_bytes[dir_pos + 1..dir_pos + 5].try_into().unwrap()) as usize;
        if offset + 8 <= row_bytes.len() {
            return Ok(i64::from_le_bytes(
                row_bytes[offset..offset + 8].try_into().unwrap(),
            ));
        }
    }
    let pk =
        extract_column_key_bytes_with_collation(row_bytes, vtab.pk_col_idx, vtab.pk_collation)?;
    extract_rowid_from_pk_bytes(&pk, vtab, snapshot)
}

/// Extracts the `rowid` (`i64`) directly from `pk_bytes` for keys-only cursor scans.
#[inline]
pub(super) fn extract_rowid_from_pk_bytes(
    pk_bytes: &[u8],
    vtab: &AydbVTab,
    snapshot: Option<&crate::engine::ReadSnapshotToken>,
) -> io::Result<i64> {
    if vtab.pk_rowid_alias {
        if let Some(id) = decode_i64_tagged_key(pk_bytes) {
            return Ok(id);
        }
    }
    super::with_snapshot_read_access(
        &vtab.engine,
        vtab.db as usize,
        vtab.table_id,
        snapshot,
        |eng| {
            eng.rowid_for_pk(vtab.table_id, pk_bytes)?.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "row has no persistent SQLite rowid",
                )
            })
        },
    )
    .unwrap_or_else(|| Err(io::Error::other("engine lock poisoned")))
}
