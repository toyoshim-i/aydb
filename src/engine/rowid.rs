//! Persistent SQLite row identities. Both directions participate in the row's WAL transaction.

use std::io;

use crate::index::IndexValue;
use crate::lru::encode_namespace_prefix;
use crate::pal::PlatformFs;
use crate::wal::WalRecord;

use super::{EngineReader, ReadView, StorageEngine, WriteDurability};

pub(super) const NS_ROWID_BY_PK: u8 = b'u';
pub(super) const NS_PK_BY_ROWID: u8 = b'v';

fn rowid_key(table_id: u16, namespace: u8, suffix: &[u8]) -> Vec<u8> {
    let mut key = encode_namespace_prefix(table_id, namespace).to_vec();
    key.extend_from_slice(suffix);
    key
}

impl<FS: PlatformFs> StorageEngine<FS> {
    pub(crate) fn rowids_initialized(&self, table_id: u16) -> io::Result<bool> {
        self.rowids_initialized_view(ReadView::Owner, table_id)
    }

    /// Whether the migration marker is visible in `view`. A marker written inside an open
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

    pub(crate) fn mark_rowids_initialized(
        &mut self,
        table_id: u16,
        durability: WriteDurability,
    ) -> io::Result<()> {
        self.commit_wal_and_memtable(
            vec![WalRecord::PutInline {
                key: rowid_key(table_id, NS_PK_BY_ROWID, &[]),
                value: vec![1],
            }],
            durability,
        )?;
        Ok(())
    }

    /// Test seam: removes the completion marker, as a lost migration would.
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

    /// Keeps a row's identity stable across connections, cache eviction and recovery. A collision
    /// is resolved against the persistent reverse index while the engine writer lock is held.
    pub(crate) fn assign_rowid(
        &mut self,
        table_id: u16,
        pk: &[u8],
        preferred: i64,
        durability: WriteDurability,
        reserved: impl Fn(&Self, i64) -> io::Result<bool>,
    ) -> io::Result<i64> {
        if let Some(rowid) = self.rowid_for_pk_view(ReadView::Owner, table_id, pk)? {
            return Ok(rowid);
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
        self.commit_wal_and_memtable(
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
            durability,
        )?;
        Ok(rowid)
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
