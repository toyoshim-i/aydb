//! Bidirectional `RowidMap` for non-integer SQLite primary keys, transactional undo log,
//! and 64-bit hardware CRC32C + SplitMix64 `rowid` hashing helpers.

use core::ffi::c_int;
use std::collections::HashMap;
use std::io;
use std::sync::atomic::Ordering;

use crate::engine::{StorageEngine, WriteDurability};
use crate::pal::OsFs;
use crate::util::crc32c::crc32c_append;
use crate::vtab::AydbVTab;
use crate::vtab::row_codec::{
    ColumnAffinity, decode_i64_tagged_key, extract_column_key_bytes_with_collation,
};

/// Maximum number of non-integer primary keys retained in `RowidMap` once all cursors close.
pub(super) const MAX_ROWID_MAP_ENTRIES: usize = 262_144;

/// Migrates older rows in bounded batches. Each mapping pair is one atomic WAL batch, and the
/// completion marker is written last so a failed connection can resume without changing IDs.
pub(super) fn ensure_persistent_rowids(
    engine: &mut StorageEngine<OsFs>,
    table_id: u16,
    durability: WriteDurability,
) -> io::Result<()> {
    if engine.rowids_initialized(table_id)? {
        return Ok(());
    }
    let mut lower = None;
    loop {
        let keys = engine.scan_keys_range(table_id, lower.as_deref(), None, 256)?;
        let mut progress = false;
        for pk in keys {
            if lower.as_deref() == Some(pk.as_slice()) {
                continue;
            }
            engine.assign_rowid(table_id, &pk, hash_pk_to_rowid(&pk), durability)?;
            lower = Some(pk);
            progress = true;
        }
        if !progress {
            return engine.mark_rowids_initialized(table_id, durability);
        }
    }
}

/// Undo record for transactional mutations to [`RowidMap`].
#[derive(Clone, Debug)]
enum RowidUndoOp {
    Removed { rowid: i64, pk: Vec<u8> },
    Inserted { rowid: i64, pk: Vec<u8> },
}

/// Collision-free bidirectional map between synthetic 64-bit SQLite `rowid`s and non-integer `encoded_pk` bytes.
#[derive(Default)]
pub(crate) struct RowidMap {
    pub(super) by_rowid: HashMap<i64, Vec<u8>>,
    pub(super) by_pk: HashMap<Vec<u8>, i64>,
    pub(super) fifo_order: Vec<i64>,
    in_tx: bool,
    tx_undo: Vec<RowidUndoOp>,
    savepoint_markers: Vec<(c_int, usize)>,
}

impl RowidMap {
    /// Evicts the oldest half of entries when the map exceeds `MAX_ROWID_MAP_ENTRIES`,
    /// preserving all recently assigned `rowid` mappings for subsequent `UPDATE`/`DELETE WHERE rowid = ?`.
    /// Also compacts stale deleted `rowid`s out of `fifo_order` if it reaches `MAX_ROWID_MAP_ENTRIES`
    /// under high insert/delete churn while live entries remain below the threshold.
    pub(super) fn evict_oldest_half_if_needed(&mut self) {
        if self.by_rowid.len() <= MAX_ROWID_MAP_ENTRIES {
            if self.fifo_order.len() >= MAX_ROWID_MAP_ENTRIES {
                let rmap_ref = &self.by_rowid;
                self.fifo_order.retain(|rid| rmap_ref.contains_key(rid));
            }
            return;
        }
        let target_keep = MAX_ROWID_MAP_ENTRIES / 2;
        let mut idx = 0usize;
        while self.by_rowid.len() > target_keep && idx < self.fifo_order.len() {
            let rid = self.fifo_order[idx];
            idx += 1;
            if let Some(pk) = self.by_rowid.remove(&rid) {
                self.by_pk.remove(&pk);
                if self.in_tx {
                    self.tx_undo.push(RowidUndoOp::Removed { rowid: rid, pk });
                }
            }
        }
        self.fifo_order.drain(0..idx);
        let rmap_ref = &self.by_rowid;
        self.fifo_order.retain(|rid| rmap_ref.contains_key(rid));
    }

    #[cfg(test)]
    pub(super) fn get_or_assign_rowid(&mut self, encoded_pk: &[u8]) -> i64 {
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
        self.by_pk.insert(pk_vec.clone(), rid);
        self.fifo_order.push(rid);
        if self.in_tx {
            self.tx_undo.push(RowidUndoOp::Inserted {
                rowid: rid,
                pk: pk_vec,
            });
        }
        rid
    }

    #[cfg(test)]
    pub(super) fn get_pk(&self, rid: i64) -> Option<&Vec<u8>> {
        self.by_rowid.get(&rid)
    }

    /// Caches an identity selected by the persistent reverse index.
    pub(super) fn remember_rowid(&mut self, encoded_pk: &[u8], rowid: i64) {
        if self.by_pk.get(encoded_pk) == Some(&rowid) {
            return;
        }
        self.remove_by_pk(encoded_pk);
        self.remove_by_rowid(rowid);
        let pk = encoded_pk.to_vec();
        self.by_rowid.insert(rowid, pk.clone());
        self.by_pk.insert(pk.clone(), rowid);
        self.fifo_order.push(rowid);
        if self.in_tx {
            self.tx_undo.push(RowidUndoOp::Inserted { rowid, pk });
        }
    }

    pub(super) fn remove_by_rowid(&mut self, rid: i64) -> Option<Vec<u8>> {
        if let Some(pk) = self.by_rowid.remove(&rid) {
            self.by_pk.remove(&pk);
            if self.in_tx {
                self.tx_undo.push(RowidUndoOp::Removed {
                    rowid: rid,
                    pk: pk.clone(),
                });
            }
            Some(pk)
        } else {
            None
        }
    }

    pub(super) fn remove_by_pk(&mut self, encoded_pk: &[u8]) {
        if let Some(rid) = self.by_pk.remove(encoded_pk) {
            if let Some(pk) = self.by_rowid.remove(&rid) {
                if self.in_tx {
                    self.tx_undo.push(RowidUndoOp::Removed { rowid: rid, pk });
                }
            }
        }
    }

    pub(super) fn begin_tx(&mut self) {
        if !self.in_tx {
            self.in_tx = true;
            self.tx_undo.clear();
            self.savepoint_markers.clear();
        }
    }

    pub(super) fn commit_tx(&mut self) {
        self.in_tx = false;
        self.tx_undo.clear();
        self.savepoint_markers.clear();
    }

    pub(super) fn savepoint(&mut self, sp_id: c_int) {
        if !self.in_tx {
            self.in_tx = true;
            self.tx_undo.clear();
            self.savepoint_markers.clear();
        }
        if let Some(pos) = self
            .savepoint_markers
            .iter()
            .position(|&(id, _)| id == sp_id)
        {
            self.savepoint_markers.truncate(pos);
        }
        self.savepoint_markers.push((sp_id, self.tx_undo.len()));
    }

    pub(super) fn release_savepoint(&mut self, sp_id: c_int, tx_finished: bool) {
        if let Some(pos) = self
            .savepoint_markers
            .iter()
            .position(|&(id, _)| id == sp_id)
        {
            self.savepoint_markers.truncate(pos);
        }
        if tx_finished {
            self.commit_tx();
        }
    }

    pub(super) fn rollback_to_savepoint(&mut self, sp_id: c_int) {
        let Some(pos) = self
            .savepoint_markers
            .iter()
            .position(|&(id, _)| id == sp_id)
        else {
            return;
        };
        let target_len = self.savepoint_markers[pos].1;
        self.savepoint_markers.truncate(pos + 1);
        self.undo_to_len(target_len);
    }

    pub(super) fn rollback_tx(&mut self) {
        self.undo_to_len(0);
        self.in_tx = false;
        self.savepoint_markers.clear();
    }

    fn undo_to_len(&mut self, target_len: usize) {
        while self.tx_undo.len() > target_len {
            match self.tx_undo.pop().unwrap() {
                RowidUndoOp::Inserted { rowid, pk } => {
                    if self.by_rowid.get(&rowid) == Some(&pk) {
                        self.by_rowid.remove(&rowid);
                    }
                    if self.by_pk.get(&pk) == Some(&rowid) {
                        self.by_pk.remove(&pk);
                    }
                }
                RowidUndoOp::Removed { rowid, pk } => {
                    if let Some(prev_pk) = self.by_rowid.insert(rowid, pk.clone()) {
                        if prev_pk != pk {
                            self.by_pk.remove(&prev_pk);
                        }
                    }
                    if let Some(prev_rid) = self.by_pk.insert(pk, rowid) {
                        if prev_rid != rowid {
                            self.by_rowid.remove(&prev_rid);
                        }
                    }
                    self.fifo_order.push(rowid);
                }
            }
        }
    }
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
/// For non-integer PKs (`TEXT`/`BLOB`/`REAL`/etc.), computes the collision-free synthetic `rowid`
/// and registers `(rid, pk)` in `rowid_to_pk`.
#[inline]
pub(super) fn extract_rowid_from_row(row_bytes: &[u8], vtab: &AydbVTab) -> io::Result<i64> {
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
    extract_rowid_from_pk_bytes(&pk, vtab)
}

/// Extracts the `rowid` (`i64`) directly from `pk_bytes` for keys-only cursor scans.
#[inline]
pub(super) fn extract_rowid_from_pk_bytes(pk_bytes: &[u8], vtab: &AydbVTab) -> io::Result<i64> {
    if vtab.pk_affinity == ColumnAffinity::Integer {
        if let Some(id) = decode_i64_tagged_key(pk_bytes) {
            return Ok(id);
        }
    }
    vtab.has_non_int_pks.store(true, Ordering::Relaxed);
    super::with_read_access(&vtab.engine, vtab.db as usize, vtab.table_id, |eng| {
        eng.rowid_for_pk(vtab.table_id, pk_bytes)?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "row has no persistent SQLite rowid",
            )
        })
    })
    .unwrap_or_else(|| Err(io::Error::other("engine lock poisoned")))
}
