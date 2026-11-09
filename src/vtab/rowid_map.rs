//! Persistent `rowid` identities for non-integer SQLite primary keys: migration of older rows,
//! synthetic `rowid` assignment, and 64-bit hardware CRC32C + SplitMix64 `rowid` hashing helpers.

use std::io;

use crate::engine::{StorageEngine, WriteDurability};
use crate::pal::PlatformFs;
use crate::util::crc32c::crc32c_append;
use crate::vtab::AydbVTab;
use crate::vtab::row_codec::{
    ColumnAffinity, decode_i64_tagged_key, extract_column_key_bytes_with_collation,
};

/// Migrates older rows in bounded batches. Each mapping pair is one atomic WAL batch, and the
/// completion marker is written last so a failed connection can resume without changing IDs.
///
/// The mapping pairs are written with buffered durability whatever the table requests: a power
/// cut that loses some of them only leaves the marker absent, and the next run assigns the rest
/// without changing the identities already kept (`assign_rowid` is idempotent). Only the marker
/// uses the table's `durability`. A synchronous marker commit first makes every earlier buffered
/// batch durable (the prefix rule of `append_raw_wal_batch_with_rotation`), so a durable marker
/// never outlives its mappings, and the migration costs one sync instead of one per row. Inside
/// a user transaction the writes join it and `durability` only marks it for a sync at commit.
pub(crate) fn ensure_persistent_rowids<FS: PlatformFs>(
    engine: &mut StorageEngine<FS>,
    table_id: u16,
    durability: WriteDurability,
    integer_primary_keys: bool,
) -> io::Result<()> {
    match engine.rowids_initialized(table_id) {
        Ok(false) => {}
        Ok(true) | Err(_) => return Ok(()),
    }
    let mut lower = None;
    loop {
        let keys = match engine.scan_keys_range(table_id, lower.as_deref(), None, 256) {
            Ok(keys) => keys,
            // Preserve SQLite's ability to connect a damaged table and report its read errors
            // through xFilter. An absent marker makes the next connection retry discovery.
            Err(_) => return Ok(()),
        };
        let mut progress = false;
        for pk in keys {
            if lower.as_deref() == Some(pk.as_slice()) {
                continue;
            }
            if !integer_primary_keys || decode_i64_tagged_key(&pk).is_none() {
                assign_synthetic_rowid(
                    engine,
                    table_id,
                    &pk,
                    WriteDurability::BufferedFast,
                    integer_primary_keys,
                )?;
            }
            lower = Some(pk);
            progress = true;
        }
        if !progress {
            return engine.mark_rowids_initialized(table_id, durability);
        }
    }
}

pub(super) fn assign_synthetic_rowid<FS: PlatformFs>(
    engine: &mut StorageEngine<FS>,
    table_id: u16,
    pk: &[u8],
    durability: WriteDurability,
    integer_primary_keys: bool,
) -> io::Result<i64> {
    engine.assign_rowid(
        table_id,
        pk,
        hash_pk_to_rowid(pk),
        durability,
        |engine, rowid| {
            Ok(integer_primary_keys
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
/// For integer PKs (`pk_affinity == ColumnAffinity::Integer` and `type_tag == 1`), reads the
/// 8-byte `i64` directly in $O(1)$ with zero allocation.
/// For non-integer PKs (`TEXT`/`BLOB`/`REAL`/etc.), reads the persistent synthetic
/// `rowid` from the engine.
#[inline]
pub(super) fn extract_rowid_from_row(
    row_bytes: &[u8],
    vtab: &AydbVTab,
    snapshot: Option<&crate::engine::ReadSnapshotToken>,
) -> io::Result<i64> {
    let dir_pos = 4 + vtab.pk_col_idx * 5;
    if vtab.pk_affinity == ColumnAffinity::Integer
        && dir_pos + 5 <= row_bytes.len()
        && row_bytes[dir_pos] == 1
    {
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
    if vtab.pk_affinity == ColumnAffinity::Integer {
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
