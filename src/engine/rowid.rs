//! Persistent SQLite row identities. Both directions participate in the row's WAL transaction.

use std::io;

use crate::index::IndexValue;
use crate::lru::encode_namespace_prefix;
use crate::pal::PlatformFs;
use crate::wal::WalRecord;

#[cfg(test)]
use super::WriteDurability;
use super::{EngineReader, ReadView, StorageEngine};

pub(super) const NS_ROWID_BY_PK: u8 = b'u';
pub(super) const NS_PK_BY_ROWID: u8 = b'v';

fn rowid_key(table_id: u16, namespace: u8, suffix: &[u8]) -> Vec<u8> {
    let mut key = encode_namespace_prefix(table_id, namespace).to_vec();
    key.extend_from_slice(suffix);
    key
}

/// The record that marks a table's rowid mapping as present. Written with the catalog entry.
pub(super) fn rowid_marker_record(table_id: u16) -> WalRecord {
    WalRecord::PutInline {
        key: rowid_key(table_id, NS_PK_BY_ROWID, &[]),
        value: vec![1],
    }
}

impl<FS: PlatformFs> StorageEngine<FS> {
    #[cfg(test)]
    pub(crate) fn rowids_initialized(&self, table_id: u16) -> io::Result<bool> {
        self.rowids_initialized_view(ReadView::Owner, table_id)
    }

    /// Whether the rowid mapping marker is visible in `view`. A marker written inside an open
    /// transaction is visible only to its owner, and disappears if that transaction rolls back.
    pub(crate) fn rowids_initialized_view(
        &self,
        view: ReadView,
        table_id: u16,
    ) -> io::Result<bool> {
        let marker = rowid_key(table_id, NS_PK_BY_ROWID, &[]);
        Ok(
            matches!(self.lookup_raw_entry_view(view, &marker)?, Some((_, IndexValue::Inline(value))) if value == [1]),
        )
    }

    /// Test seam: removes the marker, as a table of an unsupported older format lacks it.
    #[cfg(test)]
    pub(crate) fn debug_remove_rowid_marker(&mut self, table_id: u16) -> io::Result<()> {
        self.commit_wal_and_memtable(
            vec![WalRecord::Delete {
                key: rowid_key(table_id, NS_PK_BY_ROWID, &[]),
            }],
            WriteDurability::BufferedFast,
        )?;
        Ok(())
    }

    pub(crate) fn rowid_for_pk_view(
        &self,
        view: ReadView,
        table_id: u16,
        pk: &[u8],
    ) -> io::Result<Option<i64>> {
        let key = rowid_key(table_id, NS_ROWID_BY_PK, pk);
        match self.lookup_raw_entry_view(view, &key)? {
            Some((_, IndexValue::Inline(value))) if value.len() == 8 => {
                Ok(Some(i64::from_le_bytes(value.try_into().unwrap())))
            }
            Some((_, IndexValue::Tombstone)) | None => Ok(None),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid persisted rowid",
            )),
        }
    }

    pub(crate) fn pk_for_rowid_view(
        &self,
        view: ReadView,
        table_id: u16,
        rowid: i64,
    ) -> io::Result<Option<Vec<u8>>> {
        let key = rowid_key(table_id, NS_PK_BY_ROWID, &rowid.to_le_bytes());
        match self.lookup_raw_entry_view(view, &key)? {
            Some((_, IndexValue::Inline(pk))) => Ok(Some(pk)),
            Some((_, IndexValue::Tombstone)) | None => Ok(None),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid persisted rowid key",
            )),
        }
    }

    /// Computes the `(rowid, wal_records)` needed to ensure `pk` has a persistent rowid without
    /// mutating the WAL or MemTable yet, so callers can commit the row and its rowid atomically
    /// in a single batch.
    pub(crate) fn plan_assign_rowid(
        &mut self,
        table_id: u16,
        pk: &[u8],
        preferred: i64,
        reserved: impl Fn(&Self, i64) -> io::Result<bool>,
    ) -> io::Result<(i64, Vec<WalRecord>)> {
        self.check_not_poisoned()?;
        self.drain_group_for_autocommit()?;
        if let Some(rowid) = self.rowid_for_pk_view(ReadView::Owner, table_id, pk)? {
            return Ok((rowid, Vec::new()));
        }
        let mut rowid = preferred;
        loop {
            let existing = self.pk_for_rowid_view(ReadView::Owner, table_id, rowid)?;
            if !reserved(self, rowid)? && existing.as_deref().is_none_or(|owner| owner == pk) {
                break;
            }
            rowid = rowid.wrapping_add(1);
            if rowid == preferred {
                return Err(io::Error::other("SQLite rowid space exhausted"));
            }
        }
        Ok((
            rowid,
            vec![
                WalRecord::PutInline {
                    key: rowid_key(table_id, NS_ROWID_BY_PK, pk),
                    value: rowid.to_le_bytes().to_vec(),
                },
                WalRecord::PutInline {
                    key: rowid_key(table_id, NS_PK_BY_ROWID, &rowid.to_le_bytes()),
                    value: pk.to_vec(),
                },
            ],
        ))
    }

    /// Keeps a row's identity stable across connections, cache eviction and recovery. A collision
    /// is resolved against the persistent reverse index while the engine writer lock is held.
    #[cfg(test)]
    pub(crate) fn assign_rowid(
        &mut self,
        table_id: u16,
        pk: &[u8],
        preferred: i64,
        durability: WriteDurability,
        reserved: impl Fn(&Self, i64) -> io::Result<bool>,
    ) -> io::Result<i64> {
        let (rowid, records) = self.plan_assign_rowid(table_id, pk, preferred, reserved)?;
        if !records.is_empty() {
            self.commit_wal_and_memtable(records, durability)?;
        }
        Ok(rowid)
    }

    /// Computes the WAL records needed to assign `pk` exactly `rowid`, replacing any previous
    /// mapping without mutating WAL or MemTable yet. Caller has verified no other row owns `rowid`.
    pub(crate) fn plan_set_rowid(
        &mut self,
        table_id: u16,
        pk: &[u8],
        rowid: i64,
    ) -> io::Result<Vec<WalRecord>> {
        self.check_not_poisoned()?;
        self.drain_group_for_autocommit()?;
        let mut records = Vec::new();
        if let Some(old) = self.rowid_for_pk_view(ReadView::Owner, table_id, pk)? {
            if old == rowid {
                return Ok(Vec::new());
            }
            records.push(WalRecord::Delete {
                key: rowid_key(table_id, NS_PK_BY_ROWID, &old.to_le_bytes()),
            });
        }
        records.push(WalRecord::PutInline {
            key: rowid_key(table_id, NS_ROWID_BY_PK, pk),
            value: rowid.to_le_bytes().to_vec(),
        });
        records.push(WalRecord::PutInline {
            key: rowid_key(table_id, NS_PK_BY_ROWID, &rowid.to_le_bytes()),
            value: pk.to_vec(),
        });
        Ok(records)
    }

    pub(super) fn append_rowid_deletes(
        &self,
        table_id: u16,
        pk: &[u8],
        records: &mut Vec<WalRecord>,
    ) -> io::Result<()> {
        if let Some(rowid) = self.rowid_for_pk_view(ReadView::Owner, table_id, pk)? {
            records.push(WalRecord::Delete {
                key: rowid_key(table_id, NS_ROWID_BY_PK, pk),
            });
            records.push(WalRecord::Delete {
                key: rowid_key(table_id, NS_PK_BY_ROWID, &rowid.to_le_bytes()),
            });
        }
        Ok(())
    }
}

impl<FS: PlatformFs> EngineReader<'_, FS> {
    pub(crate) fn rowid_for_pk(&self, table_id: u16, pk: &[u8]) -> io::Result<Option<i64>> {
        self.eng.rowid_for_pk_view(self.view(), table_id, pk)
    }

    pub(crate) fn pk_for_rowid(&self, table_id: u16, rowid: i64) -> io::Result<Option<Vec<u8>>> {
        self.eng.pk_for_rowid_view(self.view(), table_id, rowid)
    }
}
