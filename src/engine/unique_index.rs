//! The completion marker and the per-row reconciliation of the persistent UNIQUE index.
//!
//! Unique entries share the `'i'` namespace with declared secondary indexes: the entry of a
//! `UNIQUE` column `c` lives under index id `0x8000 | c`, and the marker under `0xFFFF`. The
//! marker key has no primary-key part, so orphan collection never matches it.

use std::io;

use crate::index::{IndexEntry, IndexValue, encode_secondary_index_prefix};
use crate::lru::{
    RowMetaRecord, encode_primary_row_key, encode_row_meta_key, encode_secondary_index_key,
};
use crate::pal::PlatformFs;
use crate::wal::{BlobKind, WalRecord};

use super::{
    BLOB_RECORD_HEADER_LEN, ExistingRowStatus, MetaLookup, ReadView, StorageEngine, WriteDurability,
};

/// Index id under which the completion marker is stored.
pub(crate) const UNIQUE_MARKER_INDEX_ID: u16 = 0xFFFF;

/// True for the index ids of UNIQUE columns (`0x8000 | column`), which excludes the marker id.
#[inline]
pub(crate) fn is_unique_index_id(index_id: u16) -> bool {
    index_id & 0x8000 != 0 && index_id != UNIQUE_MARKER_INDEX_ID
}

fn marker_key(table_id: u16) -> Vec<u8> {
    encode_secondary_index_prefix(table_id, UNIQUE_MARKER_INDEX_ID).to_vec()
}

impl<FS: PlatformFs> StorageEngine<FS> {
    /// The marker value visible in `view`, if any. A marker written in an open transaction is
    /// visible only to its owner.
    pub(crate) fn unique_marker_view(
        &self,
        view: ReadView,
        table_id: u16,
    ) -> io::Result<Option<Vec<u8>>> {
        Ok(
            match self.lookup_raw_entry_view(view, &marker_key(table_id))? {
                Some((_, IndexValue::Inline(value))) => Some(value),
                _ => None,
            },
        )
    }

    /// Writes the marker as one WAL batch. Everything written before it is durable no later than
    /// the marker (the prefix rule of the WAL), so a durable marker never outlives its entries.
    pub(crate) fn write_unique_marker(
        &mut self,
        table_id: u16,
        value: Vec<u8>,
        durability: WriteDurability,
    ) -> io::Result<()> {
        self.commit_wal_and_memtable(
            vec![WalRecord::PutInline {
                key: marker_key(table_id),
                value,
            }],
            durability,
        )?;
        Ok(())
    }

    /// Removes the marker (the index no longer describes the table's definition).
    pub(crate) fn remove_unique_marker(
        &mut self,
        table_id: u16,
        durability: WriteDurability,
    ) -> io::Result<()> {
        self.commit_wal_and_memtable(
            vec![WalRecord::Delete {
                key: marker_key(table_id),
            }],
            durability,
        )?;
        Ok(())
    }

    /// Makes the UNIQUE entries of an existing row equal `desired`, in one WAL batch.
    ///
    /// `desired` is derived by the caller from the row it just read under the same engine lock,
    /// so it describes the row's current value. Entries recorded in the row's `'t'` record under
    /// a UNIQUE index id that `desired` lacks (a column that is no longer UNIQUE, or a different
    /// collation or affinity) are deleted, missing ones are added, and the record is rewritten
    /// with the declared-index pairs first and `desired` after them, as `put_row` would. The row
    /// value, blob pointer, `recorded_tick` and logical size stay as they are.
    ///
    /// Returns `Ok(false)` when nothing had to change (idempotent; also an absent row, or a row
    /// without keys that needs none). A row whose `'t'` record is unreadable, or a capped row
    /// without one, is `InvalidData` and nothing is written.
    pub(crate) fn reconcile_unique_keys(
        &mut self,
        table_id: u16,
        pk: &[u8],
        desired: &[(u16, Vec<u8>)],
        durability: WriteDurability,
    ) -> io::Result<bool> {
        self.check_not_poisoned()?;
        self.drain_group_for_autocommit()?;
        self.drain_corrupt_evictions()?;
        if pk.len() > 2048 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "primary key exceeds maximum length (2048 bytes)",
            ));
        }
        let row_key = encode_primary_row_key(table_id, pk);
        if self.check_existing_row(&row_key)? == ExistingRowStatus::NotFound {
            return Ok(false);
        }
        let meta_key = encode_row_meta_key(table_id, pk);
        let lookup = if self.may_have_row_meta(table_id)? {
            self.get_row_meta_lookup(&meta_key)?
        } else {
            MetaLookup::Absent
        };
        let (old_meta, old_meta_blob_ptr) = match lookup {
            MetaLookup::Found(meta, ptr) => (Some(meta), ptr),
            MetaLookup::Absent => (None, None),
            MetaLookup::Corrupt(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "row metadata is unreadable; unique keys cannot be reconciled",
                ));
            }
        };
        if old_meta.is_none() && self.capped_tables.contains_key(&table_id) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "row of a capped table has no metadata; unique keys cannot be reconciled",
            ));
        }
        let (recorded_tick, logical_size, old_keys) = match old_meta {
            Some(m) => (m.recorded_tick, m.logical_size, m.secondary_keys),
            None => {
                let value_len = match self.lookup_raw_entry(&row_key)? {
                    Some((_, IndexValue::Inline(v))) => v.len(),
                    Some((_, IndexValue::Blob(p))) => {
                        (p.len as usize).saturating_sub(BLOB_RECORD_HEADER_LEN + row_key.len())
                    }
                    _ => 0,
                };
                (0, (pk.len() + value_len) as u32, Vec::new())
            }
        };

        let (old_unique, mut keys): (Vec<_>, Vec<_>) = old_keys
            .into_iter()
            .partition(|(id, _)| is_unique_index_id(*id));
        let mut records = Vec::new();
        for pair in &old_unique {
            if !desired.contains(pair) {
                records.push(WalRecord::Delete {
                    key: encode_secondary_index_key(table_id, pair.0, &pair.1, pk),
                });
            }
        }
        for pair in desired {
            if keys.contains(pair) {
                continue;
            }
            if !old_unique.contains(pair) {
                if pair.1.len() > u16::MAX as usize {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "secondary index value exceeds 65535 bytes",
                    ));
                }
                let sec_key = encode_secondary_index_key(table_id, pair.0, &pair.1, pk);
                if !IndexEntry::inline_fits_in_block(sec_key.len(), 0) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "secondary index key exceeds 4 KB block capacity",
                    ));
                }
                records.push(WalRecord::PutInline {
                    key: sec_key,
                    value: Vec::new(),
                });
            }
            keys.push(pair.clone());
        }
        if records.is_empty() {
            return Ok(false);
        }

        // An uncapped row left without any key needs no record at all.
        if keys.is_empty() && recorded_tick == 0 {
            records.push(WalRecord::Delete { key: meta_key });
        } else {
            let meta_bytes = RowMetaRecord::encode_slice(recorded_tick, logical_size, &keys);
            if !IndexEntry::inline_fits_in_block(meta_key.len(), meta_bytes.len()) {
                let ptr =
                    self.append_to_blob_segment(BlobKind::Standard, 0, &meta_key, &meta_bytes)?;
                if durability == WriteDurability::SyncTransactional && self.tx_depth == 0 {
                    self.sync_active_std_writer()?;
                }
                records.push(WalRecord::PutBlobPtr {
                    key: meta_key,
                    blob_ptr: ptr,
                });
            } else {
                records.push(WalRecord::PutInline {
                    key: meta_key,
                    value: meta_bytes,
                });
            }
            self.table_has_row_meta.insert(table_id, true);
        }
        self.commit_wal_and_memtable(records, durability)?;
        if let Some(old_m_ptr) = old_meta_blob_ptr {
            self.record_dead_blob_bytes(&old_m_ptr);
        }
        Ok(true)
    }
}
