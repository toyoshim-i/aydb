//! Salvage of the transactions that WAL damage made recovery discard (ARCHITECTURE.md 4.6.7).
//!
//! Recovery replays the batches below the cut and discards the rest, although many of them are
//! intact. [`salvage`] turns the transactions that lie completely at or after the cut into
//! [`SalvagedTx`] values, in the encoding of the engine, for the application to replay or drop.
//! It runs inside `open`, before blob reconciliation, because the open reclaims blobs that only
//! discarded batches reference.

use std::collections::{BTreeMap, HashMap};
use std::io;
use std::sync::Arc;

use super::catalog::CATALOG_PREFIX;
use super::{TrackedLruBlob, TrackedStdBlob};
use crate::index::{IdxSegmentReader, IndexValue};
use crate::lru::NS_PRIMARY_ROW;
use crate::pal::PlatformFile;
use crate::wal::{BlobKind, BlobPtr, DiscardedTx, WalBatch, WalRecord};

/// A transaction that WAL damage made recovery discard, handed to the application by
/// [`StorageEngine::open_with_salvage`](super::StorageEngine::open_with_salvage).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SalvagedTx {
    /// LSN of the transaction's last batch.
    pub lsn: u64,
    /// The primary-row operations of the transaction, in log order.
    pub ops: Vec<SalvagedOp>,
}

/// One primary-row operation of a [`SalvagedTx`]. `key` and `value` are the raw bytes the engine
/// stores, as `put_row` and `delete_row` take them, so writing them back through those functions
/// restores the row. `table` is the normalized (lower-case) name in the catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SalvagedOp {
    Put {
        table: String,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        table: String,
        key: Vec<u8>,
    },
}

/// What [`salvage`] found.
pub(super) struct Salvage {
    /// Empty unless values were asked for.
    pub txs: Vec<SalvagedTx>,
    /// The `unsalvageable` count of the report.
    pub unsalvageable: u64,
}

fn table_id_of(value: &[u8]) -> Option<u16> {
    (value.len() == 2).then(|| u16::from_be_bytes([value[0], value[1]]))
}

/// The catalog as it stands after the replay of `prefix_batches` on top of `segments`: table id
/// to normalized name. Recovery has no engine yet, so it is rebuilt from the `idx` segments and
/// the catalog records of the batches that are about to be replayed.
fn catalog_names<F: PlatformFile>(
    segments: &[Arc<IdxSegmentReader<F>>],
    prefix_batches: &[WalBatch],
) -> io::Result<HashMap<u16, String>> {
    // Name key -> (lsn of the newest record, table id; `None` for a tombstone).
    let mut by_name: BTreeMap<Vec<u8>, (u64, Option<u16>)> = BTreeMap::new();
    for seg in segments {
        for entry in seg.scan_prefix(&CATALOG_PREFIX)? {
            let id = match &entry.value {
                IndexValue::Inline(v) => table_id_of(v),
                _ => None,
            };
            if by_name
                .get(&entry.key)
                .is_none_or(|(lsn, _)| *lsn <= entry.lsn)
            {
                by_name.insert(entry.key, (entry.lsn, id));
            }
        }
    }
    for batch in prefix_batches {
        for rec in &batch.records {
            match rec {
                WalRecord::PutInline { key, value } if key.starts_with(&CATALOG_PREFIX) => {
                    by_name.insert(key.clone(), (batch.lsn, table_id_of(value)));
                }
                WalRecord::Delete { key } if key.starts_with(&CATALOG_PREFIX) => {
                    by_name.insert(key.clone(), (batch.lsn, None));
                }
                _ => {}
            }
        }
    }
    Ok(by_name
        .into_iter()
        .filter(|(key, _)| key.len() > CATALOG_PREFIX.len())
        .filter_map(|(key, (_, id))| {
            let name = String::from_utf8_lossy(&key[CATALOG_PREFIX.len()..]).into_owned();
            id.map(|id| (id, name))
        })
        .collect())
}

fn taint_catalog_names(key: &[u8], put_val: Option<&[u8]>, names: &mut HashMap<u16, String>) {
    if key.len() > CATALOG_PREFIX.len() {
        let cat_name = String::from_utf8_lossy(&key[CATALOG_PREFIX.len()..]);
        names.retain(|_, v| v.as_str() != cat_name.as_ref());
    }
    if let Some(val) = put_val {
        if let Some(reused_tid) = table_id_of(val) {
            names.remove(&reused_tid);
        }
    }
}

/// Classifies the operations of the `discarded` transactions. A transaction with an operation
/// that cannot be salvaged is dropped as a whole and each such operation counts once in
/// [`Salvage::unsalvageable`]: a catalog record (a schema change), a primary-row record of a table
/// the catalog does not know, a value whose blob fails verification. Records of derived data
/// (secondary and unique index entries, rowid mappings, row metadata, the LRU queue) are skipped
/// without a count: writing the rows back rebuilds them. With `keep_values` false the blobs are
/// still read, to count the failures, but nothing is returned.
///
/// `prefix_batches` are the batches that recovery replays; `skipped_catalog` carries any catalog
/// records from transactions that were neither replayed nor returned in `discarded` (straddling or
/// incomplete multi-chunk transactions); `std_blobs` and `lru_blobs` must still hold every
/// segment, before reconciliation prunes them.
pub(super) fn salvage<F: PlatformFile>(
    segments: &[Arc<IdxSegmentReader<F>>],
    prefix_batches: &[WalBatch],
    skipped_catalog: &[(u64, Vec<WalRecord>)],
    discarded: Vec<DiscardedTx>,
    std_blobs: &BTreeMap<u64, TrackedStdBlob<F>>,
    lru_blobs: &BTreeMap<(u16, u64), TrackedLruBlob<F>>,
    keep_values: bool,
) -> io::Result<Salvage> {
    let mut out = Salvage {
        txs: Vec::new(),
        unsalvageable: 0,
    };
    if discarded.is_empty() {
        return Ok(out);
    }
    let mut names = catalog_names(segments, prefix_batches)?;
    let read_blob = |ptr: &BlobPtr, key: &[u8]| -> io::Result<Option<Vec<u8>>> {
        let reader = match ptr.kind {
            BlobKind::Standard => std_blobs.get(&ptr.seg_id).map(|b| &b.reader),
            BlobKind::Lru => lru_blobs
                .get(&(ptr.table_id, ptr.seg_id))
                .map(|b| &b.reader),
        };
        match reader {
            Some(r) => r.read_blob(ptr, Some(key)),
            None => Ok(None),
        }
    };
    let validate_blob = |ptr: &BlobPtr, key: &[u8]| -> io::Result<bool> {
        let reader = match ptr.kind {
            BlobKind::Standard => std_blobs.get(&ptr.seg_id).map(|b| &b.reader),
            BlobKind::Lru => lru_blobs
                .get(&(ptr.table_id, ptr.seg_id))
                .map(|b| &b.reader),
        };
        match reader {
            Some(r) => r.validate_blob_ptr_for_key(ptr, Some(key)),
            None => Ok(false),
        }
    };

    let mut skip_idx = 0usize;
    for tx in discarded {
        while skip_idx < skipped_catalog.len() && skipped_catalog[skip_idx].0 <= tx.lsn {
            for rec in &skipped_catalog[skip_idx].1 {
                match rec {
                    WalRecord::PutInline { key, value } => {
                        taint_catalog_names(key, Some(value), &mut names);
                    }
                    WalRecord::PutBlobPtr { key, .. } | WalRecord::Delete { key } => {
                        taint_catalog_names(key, None, &mut names);
                    }
                    WalRecord::TxBegin | WalRecord::TxCommit | WalRecord::TxContinue => {}
                }
            }
            skip_idx += 1;
        }
        let mut ops = Vec::new();
        let mut bad = false;
        for rec in tx.records {
            let (key, put) = match rec {
                WalRecord::PutInline { key, value } => (key, Some(Ok(value))),
                WalRecord::PutBlobPtr { key, blob_ptr } => (key, Some(Err(blob_ptr))),
                WalRecord::Delete { key } => (key, None),
                WalRecord::TxBegin | WalRecord::TxCommit | WalRecord::TxContinue => continue,
            };
            if key.len() < 3 {
                continue;
            }
            let (table_id, ns) = (u16::from_be_bytes([key[0], key[1]]), key[2]);
            if table_id == 0 && ns == CATALOG_PREFIX[2] {
                out.unsalvageable += 1;
                bad = true;
                ops.clear();
                let put_val = match &put {
                    Some(Ok(val)) => Some(val.as_slice()),
                    _ => None,
                };
                taint_catalog_names(&key, put_val, &mut names);
                continue;
            }
            if table_id == 0 || ns != NS_PRIMARY_ROW {
                continue;
            }
            let Some(table) = names.get(&table_id) else {
                out.unsalvageable += 1;
                bad = true;
                ops.clear();
                continue;
            };
            let value = match put {
                None => None,
                Some(Ok(value)) => Some(value),
                Some(Err(ptr)) => {
                    if keep_values && !bad {
                        match read_blob(&ptr, &key)? {
                            Some(value) => Some(value),
                            None => {
                                out.unsalvageable += 1;
                                bad = true;
                                ops.clear();
                                continue;
                            }
                        }
                    } else {
                        if !validate_blob(&ptr, &key)? {
                            out.unsalvageable += 1;
                            bad = true;
                            ops.clear();
                        }
                        continue;
                    }
                }
            };
            if keep_values && !bad {
                let key = key[3..].to_vec();
                let table = table.clone();
                ops.push(match value {
                    Some(value) => SalvagedOp::Put { table, key, value },
                    None => SalvagedOp::Delete { table, key },
                });
            }
        }
        if keep_values && !bad && !ops.is_empty() {
            out.txs.push(SalvagedTx { lsn: tx.lsn, ops });
        }
    }
    Ok(out)
}
