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
use crate::wal::{BlobKind, BlobPtr, WalRecord};

use super::eviction::est_wal_record_bytes;
use super::{
    BLOB_RECORD_HEADER_LEN, ExistingRowStatus, MetaLookup, ReadView, StorageEngine, WriteDurability,
};

/// Per-row input for [`StorageEngine::reconcile_unique_keys_batch`]: `(pk, row_value_len, desired_keys)`.
pub(crate) type UniqueReconcileItem = (Vec<u8>, usize, Vec<(u16, Vec<u8>)>);

struct PlannedRowReconcile {
    meta_key: Vec<u8>,
    records: Vec<WalRecord>,
    meta_write: Option<Vec<u8>>,
    old_meta_blob_ptr: Option<BlobPtr>,
}

#[derive(Default)]
struct ReconcileBatchState {
    records: Vec<WalRecord>,
    est_bytes: usize,
    new_meta_ptrs: Vec<BlobPtr>,
    old_meta_ptrs: Vec<BlobPtr>,
    wrote_any_meta: bool,
}

/// Index id under which the completion marker is stored.
pub(crate) const UNIQUE_MARKER_INDEX_ID: u16 = 0xFFFF;

/// True for the index ids of UNIQUE columns (`0x8000 | column`), which excludes the marker id.
#[inline]
pub(crate) fn is_unique_index_id(index_id: u16) -> bool {
    index_id & 0x8000 != 0 && index_id != UNIQUE_MARKER_INDEX_ID
}

pub(super) fn marker_key(table_id: u16) -> Vec<u8> {
    encode_secondary_index_prefix(table_id, UNIQUE_MARKER_INDEX_ID).to_vec()
}

/// The record that marks a table's UNIQUE index as complete for a given definition.
/// Written atomically with the catalog entry when creating a new empty table.
pub(super) fn unique_marker_record(table_id: u16, value: Vec<u8>) -> WalRecord {
    WalRecord::PutInline {
        key: marker_key(table_id),
        value,
    }
}

impl<FS: PlatformFs> StorageEngine<FS> {
    /// Current committed UNIQUE-marker generation (`>= 1`). Bumps whenever a marker is written or
    /// removed outside a transaction, or when an inner transaction modifying markers finishes.
    #[inline]
    pub(crate) fn unique_marker_gen(&self) -> u64 {
        self.unique_marker_gen
    }

    /// True when the active transaction has modified any UNIQUE completion marker (`0xFFFF`).
    #[inline]
    pub(crate) fn unique_marker_modified_in_tx(&self) -> bool {
        self.tx_state
            .any_tx()
            .is_some_and(|tx| tx.unique_marker_modified)
    }

    #[inline]
    pub(super) fn bump_unique_marker_gen(&mut self) {
        self.unique_marker_gen = self.unique_marker_gen.wrapping_add(1).max(1);
    }

    #[inline]
    pub(super) fn note_unique_marker_changed(&mut self) {
        if let Some(tx) = self.tx_state.active_tx_mut() {
            tx.unique_marker_modified = true;
        } else {
            self.bump_unique_marker_gen();
        }
    }

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
        self.note_unique_marker_changed();
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
        self.note_unique_marker_changed();
        Ok(())
    }

    fn plan_row_unique_reconcile(
        &mut self,
        table_id: u16,
        pk: &[u8],
        desired: &[(u16, Vec<u8>)],
        may_have_meta: bool,
        is_capped: bool,
        known_row_value_len: Option<usize>,
    ) -> io::Result<Option<PlannedRowReconcile>> {
        if pk.len() > 2048 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "primary key exceeds maximum length (2048 bytes)",
            ));
        }
        let row_key = if known_row_value_len.is_none() {
            let rk = encode_primary_row_key(table_id, pk);
            if self.check_existing_row(&rk)? == ExistingRowStatus::NotFound {
                return Ok(None);
            }
            Some(rk)
        } else {
            None
        };
        let meta_key = encode_row_meta_key(table_id, pk);
        let lookup = if may_have_meta {
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
        if old_meta.is_none() && is_capped {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "row of a capped table has no metadata; unique keys cannot be reconciled",
            ));
        }
        let (recorded_tick, logical_size, old_keys) = match old_meta {
            Some(m) => (m.recorded_tick, m.logical_size, m.secondary_keys),
            None => {
                let value_len = match (known_row_value_len, row_key.as_deref()) {
                    (Some(len), _) => len,
                    (None, Some(rk)) => match self.lookup_raw_entry(rk)? {
                        Some((_, IndexValue::Inline(v))) => v.len(),
                        Some((_, IndexValue::Blob(p))) => {
                            (p.len as usize).saturating_sub(BLOB_RECORD_HEADER_LEN + rk.len())
                        }
                        _ => 0,
                    },
                    (None, None) => 0,
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
            return Ok(None);
        }

        // An uncapped row left without any key needs no record at all.
        let meta_write = if keys.is_empty() && recorded_tick == 0 {
            None
        } else {
            Some(RowMetaRecord::encode_slice(
                recorded_tick,
                logical_size,
                &keys,
            )?)
        };
        Ok(Some(PlannedRowReconcile {
            meta_key,
            records,
            meta_write,
            old_meta_blob_ptr,
        }))
    }

    fn commit_reconcile_sub_batch(
        &mut self,
        table_id: u16,
        durability: WriteDurability,
        state: &mut ReconcileBatchState,
    ) -> io::Result<()> {
        if state.records.is_empty() {
            return Ok(());
        }
        if !state.new_meta_ptrs.is_empty()
            && durability == WriteDurability::SyncTransactional
            && !self.tx_state.is_active()
        {
            if let Err(e) = self.sync_active_std_writer() {
                for p in state.new_meta_ptrs.drain(..) {
                    self.record_dead_blob_bytes(&p);
                }
                return Err(e);
            }
        }
        let batch = std::mem::take(&mut state.records);
        if let Err(e) = self.commit_wal_and_memtable_core(
            batch,
            durability,
            true,
            false,
            super::tx::WriteScope::Normal,
        ) {
            for p in state.new_meta_ptrs.drain(..) {
                self.record_dead_blob_bytes(&p);
            }
            return Err(e);
        }
        if state.wrote_any_meta {
            self.table_has_row_meta.insert(table_id, true);
        }
        for p in state.old_meta_ptrs.drain(..) {
            self.record_dead_blob_bytes(&p);
        }
        state.new_meta_ptrs.clear();
        state.wrote_any_meta = false;
        state.est_bytes = 0;
        self.maybe_auto_flush_and_compact();
        Ok(())
    }

    /// Makes the UNIQUE entries of an existing row equal `desired`, in one WAL batch.
    #[cfg(test)]
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
        let may_have_meta = self.may_have_row_meta(table_id)?;
        let is_capped = self.capped_tables.contains_key(&table_id);
        let Some(planned) =
            self.plan_row_unique_reconcile(table_id, pk, desired, may_have_meta, is_capped, None)?
        else {
            return Ok(false);
        };

        let mut records = planned.records;
        let mut new_meta_blob_ptr = None;
        let wrote_meta = planned.meta_write.is_some();
        match planned.meta_write {
            None => records.push(WalRecord::Delete {
                key: planned.meta_key,
            }),
            Some(meta_bytes) => {
                new_meta_blob_ptr = self.append_std_meta_record(
                    planned.meta_key,
                    meta_bytes,
                    durability,
                    &mut records,
                )?;
            }
        }
        if let Err(e) = self.commit_wal_and_memtable_core(
            records,
            durability,
            true,
            false,
            super::tx::WriteScope::Normal,
        ) {
            if let Some(ref p) = new_meta_blob_ptr {
                self.record_dead_blob_bytes(p);
            }
            return Err(e);
        }
        if wrote_meta {
            self.table_has_row_meta.insert(table_id, true);
        }
        if let Some(old_m_ptr) = planned.old_meta_blob_ptr {
            self.record_dead_blob_bytes(&old_m_ptr);
        }
        self.maybe_auto_flush_and_compact();
        Ok(true)
    }

    /// Reconciles the UNIQUE index entries of a batch of live rows that the caller just scanned
    /// under the same exclusive engine lock, committing them in as few WAL batches as fit within
    /// the WAL safety cap (normally one batch per migration chunk).
    ///
    /// Each item is `(pk, row_value_len, desired_unique_keys)`. Because the caller already
    /// verified row liveness and read the row payload under the same lock, this skips per-row
    /// existence checks and row-length lookups. Returns the number of rows whose metadata or
    /// index entries changed.
    pub(crate) fn reconcile_unique_keys_batch(
        &mut self,
        table_id: u16,
        rows: &[UniqueReconcileItem],
        durability: WriteDurability,
    ) -> io::Result<usize> {
        self.check_not_poisoned()?;
        self.drain_group_for_autocommit()?;
        let drained_for_table =
            !self.tx_state.is_active() && self.has_pending_corrupt_evictions_for_table(table_id);
        self.drain_corrupt_evictions()?;
        if rows.is_empty() {
            return Ok(0);
        }

        let may_have_meta = self.may_have_row_meta(table_id)?;
        let is_capped = self.capped_tables.contains_key(&table_id);
        let max_batch_wal_bytes = if !self.tx_state.is_active() {
            let sectors = match durability {
                WriteDurability::SyncTransactional => self.sync_wal().total_sectors(),
                WriteDurability::BufferedFast => self.buf_wal().total_sectors(),
            };
            super::eviction::max_wal_sub_batch_bytes(sectors, 512 * 1024)
        } else {
            usize::MAX
        };

        let mut state = ReconcileBatchState::default();
        let mut reconciled_rows = 0usize;

        let res: io::Result<()> = (|| {
            for (pk, row_value_len, desired) in rows {
                let Some(planned) = self.plan_row_unique_reconcile(
                    table_id,
                    pk,
                    desired,
                    may_have_meta,
                    is_capped,
                    (!drained_for_table).then_some(*row_value_len),
                )?
                else {
                    continue;
                };

                let meta_est_bytes = match &planned.meta_write {
                    None => 5 + planned.meta_key.len(),
                    Some(mb)
                        if !IndexEntry::inline_fits_in_block(planned.meta_key.len(), mb.len()) =>
                    {
                        28 + planned.meta_key.len()
                    }
                    Some(mb) => 9 + planned.meta_key.len() + mb.len(),
                };
                let row_est_bytes = planned
                    .records
                    .iter()
                    .map(est_wal_record_bytes)
                    .sum::<usize>()
                    .saturating_add(meta_est_bytes);

                if !state.records.is_empty()
                    && state.est_bytes.saturating_add(row_est_bytes) > max_batch_wal_bytes
                {
                    self.commit_reconcile_sub_batch(table_id, durability, &mut state)?;
                }

                state.records.extend(planned.records);
                match planned.meta_write {
                    None => {
                        state.records.push(WalRecord::Delete {
                            key: planned.meta_key,
                        });
                    }
                    Some(meta_bytes) => {
                        state.wrote_any_meta = true;
                        if !IndexEntry::inline_fits_in_block(
                            planned.meta_key.len(),
                            meta_bytes.len(),
                        ) {
                            let ptr = self.append_to_blob_segment(
                                BlobKind::Standard,
                                0,
                                &planned.meta_key,
                                &meta_bytes,
                            )?;
                            state.new_meta_ptrs.push(ptr);
                            state.records.push(WalRecord::PutBlobPtr {
                                key: planned.meta_key,
                                blob_ptr: ptr,
                            });
                        } else {
                            state.records.push(WalRecord::PutInline {
                                key: planned.meta_key,
                                value: meta_bytes,
                            });
                        }
                    }
                }
                if let Some(old_m_ptr) = planned.old_meta_blob_ptr {
                    state.old_meta_ptrs.push(old_m_ptr);
                }
                state.est_bytes = state.est_bytes.saturating_add(row_est_bytes);
                reconciled_rows += 1;
            }

            self.commit_reconcile_sub_batch(table_id, durability, &mut state)
        })();

        if let Err(e) = res {
            if !self.tx_state.is_active() {
                self.in_flight_autocommit_blob_starts.clear();
            }
            for p in state.new_meta_ptrs.drain(..) {
                self.record_dead_blob_bytes(&p);
            }
            return Err(e);
        }
        Ok(reconciled_rows)
    }
}
