//! Persistent SQLite row identities. Both directions participate in the row's WAL transaction.

use std::io;

use crate::index::IndexValue;
use crate::lru::encode_namespace_prefix;
use crate::pal::PlatformFs;
use crate::wal::WalRecord;

#[cfg(test)]
use super::WriteDurability;
use super::reader::ReadSource;
use super::{EngineReader, ReadView, StorageEngine};

pub(super) const NS_ROWID_BY_PK: u8 = b'u';
pub(super) const NS_PK_BY_ROWID: u8 = b'v';

fn rowid_key(table_id: u16, namespace: u8, suffix: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(3 + suffix.len());
    key.extend_from_slice(&encode_namespace_prefix(table_id, namespace));
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

    pub(super) fn may_have_synthetic_rowids(&mut self, table_id: u16) -> io::Result<bool> {
        if let Some(&has) = self.table_has_synthetic_rowids.get(&table_id) {
            return Ok(has);
        }
        let by_pk_prefix = encode_namespace_prefix(table_id, NS_ROWID_BY_PK);
        let mut has = !self
            .scan_merged_prefix_range(&by_pk_prefix, &by_pk_prefix, true, None, 1, false)?
            .is_empty();
        if !has {
            // Skip the 3-byte empty-suffix marker `[tid | 'v']` with `lower_inclusive = false`.
            let pk_by_rowid_prefix = encode_namespace_prefix(table_id, NS_PK_BY_ROWID);
            has = !self
                .scan_merged_prefix_range(
                    &pk_by_rowid_prefix,
                    &pk_by_rowid_prefix,
                    false,
                    None,
                    1,
                    false,
                )?
                .is_empty();
        }
        self.table_has_synthetic_rowids.insert(table_id, has);
        Ok(has)
    }

    pub(crate) fn rowid_for_pk_view(
        &self,
        view: ReadView,
        table_id: u16,
        pk: &[u8],
    ) -> io::Result<Option<i64>> {
        self.rowid_for_pk_src(ReadSource::from_view(view), table_id, pk)
    }

    #[inline]
    fn table_definitely_has_no_synthetic_rowids(
        &self,
        src: ReadSource<'_, FS::File>,
        table_id: u16,
    ) -> bool {
        match src {
            ReadSource::Snapshot(_) => false,
            ReadSource::Committed => {
                let map = self
                    .tx_state
                    .start_meta()
                    .map_or(&self.table_has_synthetic_rowids, |s| {
                        &s.table_has_synthetic_rowids
                    });
                map.get(&table_id) == Some(&false)
            }
            ReadSource::Owner => self.table_has_synthetic_rowids.get(&table_id) == Some(&false),
        }
    }

    pub(super) fn rowid_for_pk_src(
        &self,
        src: ReadSource<'_, FS::File>,
        table_id: u16,
        pk: &[u8],
    ) -> io::Result<Option<i64>> {
        if self.table_definitely_has_no_synthetic_rowids(src, table_id) {
            return Ok(None);
        }
        let key = rowid_key(table_id, NS_ROWID_BY_PK, pk);
        match self.lookup_raw_entry_src(src, &key)? {
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

    pub(super) fn rowids_for_pks_src(
        &self,
        src: ReadSource<'_, FS::File>,
        table_id: u16,
        pks: &[Vec<u8>],
        out: &mut Vec<Option<i64>>,
    ) -> io::Result<()> {
        out.clear();
        out.reserve(pks.len());
        let prefix = encode_namespace_prefix(table_id, NS_ROWID_BY_PK);
        let mut key = Vec::with_capacity(3 + 64);
        for pk in pks {
            key.clear();
            key.extend_from_slice(&prefix);
            key.extend_from_slice(pk);
            match self.lookup_raw_entry_src(src, &key)? {
                Some((_, IndexValue::Inline(value))) if value.len() == 8 => {
                    out.push(Some(i64::from_le_bytes(value.try_into().unwrap())));
                }
                // Defer missing/invalid rowid error reporting to the row-level fallback so a
                // LIMIT query never fails because of an unvisited row later in the chunk.
                _ => out.push(None),
            }
        }
        Ok(())
    }

    pub(crate) fn pk_for_rowid_view(
        &self,
        view: ReadView,
        table_id: u16,
        rowid: i64,
    ) -> io::Result<Option<Vec<u8>>> {
        self.pk_for_rowid_src(ReadSource::from_view(view), table_id, rowid)
    }

    pub(super) fn pk_for_rowid_src(
        &self,
        src: ReadSource<'_, FS::File>,
        table_id: u16,
        rowid: i64,
    ) -> io::Result<Option<Vec<u8>>> {
        if self.table_definitely_has_no_synthetic_rowids(src, table_id) {
            return Ok(None);
        }
        let key = rowid_key(table_id, NS_PK_BY_ROWID, &rowid.to_le_bytes());
        match self.lookup_raw_entry_src(src, &key)? {
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
        &mut self,
        table_id: u16,
        pk: &[u8],
        records: &mut Vec<WalRecord>,
    ) -> io::Result<()> {
        if !self.may_have_synthetic_rowids(table_id)? {
            return Ok(());
        }
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
        self.eng.rowid_for_pk_src(self.cx.src, table_id, pk)
    }

    pub(crate) fn rowids_for_pks(
        &self,
        table_id: u16,
        pks: &[Vec<u8>],
        out: &mut Vec<Option<i64>>,
    ) -> io::Result<()> {
        self.eng.rowids_for_pks_src(self.cx.src, table_id, pks, out)
    }

    pub(crate) fn pk_for_rowid(&self, table_id: u16, rowid: i64) -> io::Result<Option<Vec<u8>>> {
        self.eng.pk_for_rowid_src(self.cx.src, table_id, rowid)
    }
}
