//! Engine startup, WAL recovery, salvage invocation, uncataloged blob adoption, and
//! post-recovery capped-table reconciliation.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex, RwLock};

use crate::blob::BlobSegmentReader;
use crate::cache::{VerifiedBlobCache, VerifiedBlockCache};
use crate::index::{IdxSegmentReader, IndexValue};
use crate::lru::{AccessTracker, CappedTableState, NS_PRIMARY_ROW, NS_ROW_META};
use crate::memtable::MemTable;
use crate::meta::{
    BootMetaManager, BootMetaState, EvictionPolicy, LruBlobSegmentMeta, StdBlobSegmentMeta,
    format_lru_blob_file_name, format_std_blob_file_name,
};
use crate::pal::{OpenHint, PlatformFile, PlatformFs};
use crate::wal::{
    BlobKind, WAL_BUF_FILE_NAME, WAL_SYNC_FILE_NAME, WalBatch, WalRecord, WalRecoveryReport,
    WalScanMode, WalStreamMode, WalStreamWriter, recover_two_streams_keyed,
};

use super::health;
use super::salvage::{self, SalvagedTx};
use super::scoped_guards::{ScopedReconcilingTable, ScopedRelocatingFlag};
use super::snapshot;
use super::tx;
use super::wal_links;
use super::{EngineConfig, StorageEngine, TrackedLruBlob, TrackedStdBlob, WalDamagePolicy};

struct OpenedSegments<F: PlatformFile> {
    idx_segments: Vec<Arc<IdxSegmentReader<F>>>,
    std_blobs: BTreeMap<u64, TrackedStdBlob<F>>,
    lru_blobs: BTreeMap<(u16, u64), TrackedLruBlob<F>>,
    cataloged_std_seqs: HashSet<u64>,
    cataloged_lru_keys: HashSet<(u16, u64)>,
    cataloged_std_len: HashMap<u64, u64>,
    cataloged_lru_len: HashMap<(u16, u64), u64>,
    std_post_catalog_spans: BTreeMap<u64, Vec<(u64, u64)>>,
    lru_post_catalog_spans: BTreeMap<(u16, u64), Vec<(u64, u64)>>,
    std_base_dead: BTreeMap<u64, u64>,
    lru_base_dead: BTreeMap<(u16, u64), u64>,
}

struct RecoveredWalState<F: PlatformFile> {
    sync_wal: WalStreamWriter<F>,
    buf_wal: WalStreamWriter<F>,
    max_recovered_lsn: u64,
    wal_report: WalRecoveryReport,
    replayed_wal: bool,
    salvaged: Vec<SalvagedTx>,
}

/// Opens a blob segment found by directory listing but absent from the catalog. A file that
/// vanished since the listing is skipped; any other error aborts the caller.
fn open_uncataloged_blob<FS: PlatformFs>(
    fs: &FS,
    fname: &str,
) -> io::Result<Option<Arc<FS::File>>> {
    match fs.open_file(fname, false, true, OpenHint::ImmutableSequential) {
        Ok(f) => Ok(Some(f)),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

fn record_post_catalog_span<K: Ord>(
    spans: &mut BTreeMap<K, Vec<(u64, u64)>>,
    key: K,
    offset: u64,
    end: u64,
    cataloged_len: u64,
    file_len: u64,
) {
    let start = offset.max(cataloged_len);
    let clamped_end = end.min(file_len);
    if clamped_end > start {
        spans.entry(key).or_default().push((start, clamped_end));
    }
}

/// Make the recovered tail dirty again before syncing it. A failed writeback can leave the
/// correct bytes readable in cache but clean, so a bare sync on reopen may do nothing.
fn persist_recovered_blob_tail(
    file: &impl PlatformFile,
    mut offset: u64,
    end: u64,
) -> io::Result<()> {
    if offset >= end {
        return Ok(());
    }
    let mut buf = vec![0; (end - offset).min(64 * 1024) as usize];
    while offset < end {
        let n = (end - offset).min(buf.len() as u64) as usize;
        file.read_exact_at(&mut buf[..n], offset)?;
        file.write_all_at(&buf[..n], offset)?;
        offset += n as u64;
    }
    file.sync_durable()
}

fn merged_span_bytes(spans: &mut [(u64, u64)]) -> u64 {
    if spans.is_empty() {
        return 0;
    }
    spans.sort_unstable();
    let mut total = 0u64;
    let mut cur_start = spans[0].0;
    let mut cur_end = spans[0].1;
    for &(s, e) in &spans[1..] {
        if s <= cur_end {
            cur_end = cur_end.max(e);
        } else {
            total += cur_end - cur_start;
            cur_start = s;
            cur_end = e;
        }
    }
    total + (cur_end - cur_start)
}

fn account_post_catalog_gap<K: Ord + std::hash::Hash + Copy>(
    key: K,
    file_len: u64,
    dead_bytes: &mut u64,
    cataloged_len: &HashMap<K, u64>,
    spans: &mut BTreeMap<K, Vec<(u64, u64)>>,
    base_dead: &BTreeMap<K, u64>,
) -> u64 {
    let cat_len = cataloged_len.get(&key).copied().unwrap_or(0);
    let post_catalog_total = file_len.saturating_sub(cat_len);
    let covered = spans
        .get_mut(&key)
        .map(|v| merged_span_bytes(v))
        .unwrap_or(0);
    let unaccounted = post_catalog_total.saturating_sub(covered);
    let mut persisted = base_dead.get(&key).copied().unwrap_or(0);
    if unaccounted > 0 {
        *dead_bytes = dead_bytes.saturating_add(unaccounted).min(file_len);
        persisted = persisted.saturating_add(unaccounted).min(file_len);
    }
    persisted
}

/// Phase 1: Opens cataloged `idx`, `blob_std`, and `blob_lru` segments, adopts uncataloged blob
/// segments not queued in `pending_delete_files`, and scans `idx` tails if physical blob lengths
/// exceed cataloged lengths.
fn open_cataloged_and_uncataloged_segments<FS: PlatformFs>(
    fs: &FS,
    state_snapshot: &BootMetaState,
    block_cache: &Arc<VerifiedBlockCache>,
) -> io::Result<OpenedSegments<FS::File>> {
    // 1. Open active `idx_<SeqID>.db` segments
    let mut idx_segments = Vec::with_capacity(state_snapshot.idx_segments.len());
    for seg_meta in &state_snapshot.idx_segments {
        let mut reader = IdxSegmentReader::open(fs, seg_meta.seq_id)?;
        reader.set_block_cache(Some(Arc::clone(block_cache)));
        idx_segments.push(Arc::new(reader));
    }

    // 2. Open active `blob_std_<SeqID>.db` segments
    let mut std_blobs = BTreeMap::new();
    let mut std_post_catalog_spans: BTreeMap<u64, Vec<(u64, u64)>> = BTreeMap::new();
    let mut cataloged_std_len: HashMap<u64, u64> =
        HashMap::with_capacity(state_snapshot.std_blob_segments.len());
    for bmeta in &state_snapshot.std_blob_segments {
        let fname = format_std_blob_file_name(bmeta.seq_id);
        let file = fs.open_file(&fname, false, true, OpenHint::ImmutableSequential)?;
        let physical_len = file.file_len()?;
        let file_len = physical_len.max(bmeta.file_len);
        // The catalog already accounts for every byte below its length, so only bytes beyond
        // it can be unaccounted garbage. A file shorter than the catalog (its tail never
        // reached the device) must not make the missing bytes count as garbage a second
        // time: recovery has to give the same result however often it runs.
        cataloged_std_len.insert(bmeta.seq_id, bmeta.file_len);
        std_blobs.insert(
            bmeta.seq_id,
            TrackedStdBlob {
                reader: BlobSegmentReader::new_with_len(file, bmeta.seq_id, physical_len),
                file_len,
                dead_bytes: bmeta.dead_bytes,
            },
        );
    }

    // 3. Open active `blob_lru_<TblID>_<SeqID>.db` segments
    let mut lru_blobs = BTreeMap::new();
    let mut lru_post_catalog_spans: BTreeMap<(u16, u64), Vec<(u64, u64)>> = BTreeMap::new();
    let mut cataloged_lru_len: HashMap<(u16, u64), u64> =
        HashMap::with_capacity(state_snapshot.lru_blob_segments.len());
    for lmeta in &state_snapshot.lru_blob_segments {
        let fname = format_lru_blob_file_name(lmeta.table_id, lmeta.seq_id);
        let file = fs.open_file(&fname, false, true, OpenHint::ImmutableSequential)?;
        let physical_len = file.file_len()?;
        let file_len = physical_len.max(lmeta.file_len);
        // See the `blob_std` loop above.
        cataloged_lru_len.insert((lmeta.table_id, lmeta.seq_id), lmeta.file_len);
        lru_blobs.insert(
            (lmeta.table_id, lmeta.seq_id),
            TrackedLruBlob {
                table_id: lmeta.table_id,
                reader: BlobSegmentReader::new_with_len(file, lmeta.seq_id, physical_len),
                file_len,
                dead_bytes: lmeta.dead_bytes,
            },
        );
    }

    // Also discover any uncataloged `blob_std_*` or `blob_lru_*` created during the active epoch
    // before a crash so `Write-Blob-Before-WAL` validation can verify their records.
    //
    // A file queued in `pending_delete_files` was dropped from the catalog because it held no
    // live data, and only its unlink is outstanding. It must never be adopted: a stale pointer
    // in an older `idx` segment (counted by the scan below) would catalog it again, and the
    // pending delete of the same commit would then unlink a cataloged file. Such a pointer
    // behaves like any other pointer into a dropped segment: it is dead.
    for fname in fs.list_files()? {
        if state_snapshot.pending_delete_files.contains(&fname) {
            continue;
        }
        if let Some(hex) = fname
            .strip_prefix("blob_std_")
            .and_then(|s| s.strip_suffix(".db"))
        {
            if let Ok(seq_id) = u64::from_str_radix(hex, 16) {
                if fname == format_std_blob_file_name(seq_id) && !std_blobs.contains_key(&seq_id) {
                    if let Some(file) = open_uncataloged_blob(fs, &fname)? {
                        let flen = file.file_len()?;
                        std_blobs.insert(
                            seq_id,
                            TrackedStdBlob {
                                reader: BlobSegmentReader::new_with_len(file, seq_id, flen),
                                file_len: flen,
                                dead_bytes: 0,
                            },
                        );
                    }
                }
            }
        } else if let Some(rest) = fname
            .strip_prefix("blob_lru_")
            .and_then(|s| s.strip_suffix(".db"))
        {
            if let Some((tbl_hex, seq_hex)) = rest.split_once('_') {
                if let (Ok(tbl_id), Ok(seq_id)) = (
                    u16::from_str_radix(tbl_hex, 16),
                    u64::from_str_radix(seq_hex, 16),
                ) {
                    if fname == format_lru_blob_file_name(tbl_id, seq_id)
                        && !lru_blobs.contains_key(&(tbl_id, seq_id))
                    {
                        if let Some(file) = open_uncataloged_blob(fs, &fname)? {
                            let flen = file.file_len()?;
                            lru_blobs.insert(
                                (tbl_id, seq_id),
                                TrackedLruBlob {
                                    table_id: tbl_id,
                                    reader: BlobSegmentReader::new_with_len(file, seq_id, flen),
                                    file_len: flen,
                                    dead_bytes: 0,
                                },
                            );
                        }
                    }
                }
            }
        }
    }

    let cataloged_std_seqs: HashSet<u64> = state_snapshot
        .std_blob_segments
        .iter()
        .map(|s| s.seq_id)
        .collect();
    let cataloged_lru_keys: HashSet<(u16, u64)> = state_snapshot
        .lru_blob_segments
        .iter()
        .map(|s| (s.table_id, s.seq_id))
        .collect();

    // For any cataloged blob segment whose physical `file_len` exceeds its cataloged
    // `bmeta.file_len`, also scan `idx_segments` in case an `idx` segment already
    // references blobs in that segment (e.g. if `boot_meta.db` fell back to an older slot).
    let needs_idx_blob_scan = std_blobs.iter().any(|(&seq, b)| {
        cataloged_std_seqs.contains(&seq)
            && b.file_len > cataloged_std_len.get(&seq).copied().unwrap_or(0)
    }) || lru_blobs.iter().any(|(&k, b)| {
        cataloged_lru_keys.contains(&k)
            && b.file_len > cataloged_lru_len.get(&k).copied().unwrap_or(0)
    });
    if needs_idx_blob_scan {
        for seg in &idx_segments {
            let mut cur = seg.cursor_all()?;
            while let Some(entry) = cur.next_entry()? {
                if let IndexValue::Blob(ptr) = entry.value {
                    let end = (ptr.offset as u64).saturating_add(ptr.len as u64);
                    match ptr.kind {
                        BlobKind::Standard => {
                            if let (Some(&cataloged_len), Some(b)) = (
                                cataloged_std_len.get(&ptr.seg_id),
                                std_blobs.get(&ptr.seg_id),
                            ) {
                                record_post_catalog_span(
                                    &mut std_post_catalog_spans,
                                    ptr.seg_id,
                                    ptr.offset as u64,
                                    end,
                                    cataloged_len,
                                    b.file_len,
                                );
                            }
                        }
                        BlobKind::Lru => {
                            let key = (ptr.table_id, ptr.seg_id);
                            if let (Some(&cataloged_len), Some(b)) =
                                (cataloged_lru_len.get(&key), lru_blobs.get(&key))
                            {
                                record_post_catalog_span(
                                    &mut lru_post_catalog_spans,
                                    key,
                                    ptr.offset as u64,
                                    end,
                                    cataloged_len,
                                    b.file_len,
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    // Snapshot the persisted (checkpointed) dead_bytes before WAL replay mutates the in-memory
    // values. Uncataloged segments have no persisted value (0).
    let std_base_dead: BTreeMap<u64, u64> =
        std_blobs.iter().map(|(&k, b)| (k, b.dead_bytes)).collect();
    let lru_base_dead: BTreeMap<(u16, u64), u64> =
        lru_blobs.iter().map(|(&k, b)| (k, b.dead_bytes)).collect();

    Ok(OpenedSegments {
        idx_segments,
        std_blobs,
        lru_blobs,
        cataloged_std_seqs,
        cataloged_lru_keys,
        cataloged_std_len,
        cataloged_lru_len,
        std_post_catalog_spans,
        lru_post_catalog_spans,
        std_base_dead,
        lru_base_dead,
    })
}

/// Phase 2: Runs Two-Stream WAL recovery and optional salvage, enforces `WalDamagePolicy::Fail`
/// before any durable write or WAL file creation, and then materializes WAL streams and SeqIDs.
fn recover_wal_and_salvage<FS: PlatformFs>(
    fs: &FS,
    config: &EngineConfig,
    meta_mgr: &mut BootMetaManager<FS>,
    segs: &OpenedSegments<FS::File>,
    with_salvage: bool,
) -> io::Result<(RecoveredWalState<FS::File>, Vec<WalBatch>)> {
    let state_snapshot = meta_mgr.state().clone();
    // Open existing `wal_sync.db` and `wal_buf.db` without creating missing files prior to the
    // `WalDamagePolicy::Fail` decision, then run Two-Stream Recovery.
    let sync_wal_opt = WalStreamWriter::open_existing_unallocated(
        fs,
        WAL_SYNC_FILE_NAME,
        config.sync_wal_size_bytes,
        state_snapshot.sync_wal_epoch,
        WalStreamMode::SyncSectorIsolated,
    )?;
    let buf_wal_opt = WalStreamWriter::open_existing_unallocated(
        fs,
        WAL_BUF_FILE_NAME,
        config.buf_wal_size_bytes,
        state_snapshot.buf_wal_epoch,
        config.buf_wal_mode,
    )?;

    let scan_mode = if with_salvage || config.wal_damage_policy == WalDamagePolicy::Fail {
        WalScanMode::FullScan
    } else {
        WalScanMode::StopAtEndOfLog
    };
    let mut rec = recover_two_streams_keyed(
        sync_wal_opt.as_ref().map(|w| &**w.file()),
        state_snapshot.sync_wal_epoch,
        buf_wal_opt.as_ref().map(|w| &**w.file()),
        state_snapshot.buf_wal_epoch,
        state_snapshot.checkpoint_lsn,
        scan_mode,
        |key, ptr| match ptr.kind {
            BlobKind::Standard => match segs.std_blobs.get(&ptr.seg_id) {
                Some(b) => b.reader.validate_blob_ptr_for_key(ptr, Some(key)),
                None => Ok(false),
            },
            BlobKind::Lru => match segs.lru_blobs.get(&(ptr.table_id, ptr.seg_id)) {
                Some(b) => b.reader.validate_blob_ptr_for_key(ptr, Some(key)),
                None => Ok(false),
            },
        },
    )?;

    // Salvage reads blobs that the reconciliation below may prune, so it comes first. It only
    // reads, so under `Fail` nothing is written yet either. It runs only when damage was
    // found, which is also the only case where a plain open pays for the blob reads.
    let mut salvaged = Vec::new();
    let discarded = core::mem::take(&mut rec.discarded_txs);
    let skipped_catalog = core::mem::take(&mut rec.skipped_catalog_records);
    if let Some(damage) = rec.report.damage.as_mut() {
        let found = salvage::salvage(
            &segs.idx_segments,
            &rec.merged_batches,
            &skipped_catalog,
            discarded,
            &segs.std_blobs,
            &segs.lru_blobs,
            with_salvage,
        )?;
        damage.unsalvageable = found.unsalvageable;
        salvaged = found.txs;
    }

    // Under `Fail` nothing may have been written yet: `ensure_durable()` and creating/extending
    // WAL streams below must come after this check.
    if let (WalDamagePolicy::Fail, Some(damage)) = (config.wal_damage_policy, &rec.report.damage) {
        return Err(io::Error::new(io::ErrorKind::InvalidData, damage.clone()));
    }
    meta_mgr.ensure_durable()?;
    // Materialize missing WAL streams and extend short files only after passing the `Fail` gate.
    let mut sync_wal = match sync_wal_opt {
        Some(w) => w,
        None => WalStreamWriter::open_unallocated(
            fs,
            WAL_SYNC_FILE_NAME,
            config.sync_wal_size_bytes,
            state_snapshot.sync_wal_epoch,
            WalStreamMode::SyncSectorIsolated,
        )?,
    };
    let mut buf_wal = match buf_wal_opt {
        Some(w) => w,
        None => WalStreamWriter::open_unallocated(
            fs,
            WAL_BUF_FILE_NAME,
            config.buf_wal_size_bytes,
            state_snapshot.buf_wal_epoch,
            config.buf_wal_mode,
        )?,
    };
    sync_wal.allocate_to_target()?;
    buf_wal.allocate_to_target()?;
    // Recovery allocates no SeqID, so reserving here still precedes every allocation.
    meta_mgr.reserve_seq_ids_above_existing_files()?;

    let wal_report = rec.report;
    sync_wal.set_cursor_after_recovery(rec.next_sync_sector_idx);
    buf_wal.set_cursor_after_recovery(rec.next_buf_sector_idx);

    // Recovery that found no valid sector, no batch, no damage, and no torn tail sectors in
    // either stream leaves the persisted capped-table byte totals authoritative. Sector-0
    // damage or torn tails can drop acknowledged writes and still require reconciliation.
    let replayed_wal = !rec.merged_batches.is_empty()
        || rec.next_sync_sector_idx > 0
        || rec.next_buf_sector_idx > 0
        || wal_report.damage.is_some()
        || wal_report.torn_tail_sectors.sync > 0
        || wal_report.torn_tail_sectors.buffered > 0;

    Ok((
        RecoveredWalState {
            sync_wal,
            buf_wal,
            max_recovered_lsn: rec.max_recovered_lsn,
            wal_report,
            replayed_wal,
            salvaged,
        },
        rec.merged_batches,
    ))
}

/// Phase 3: Replays recovered WAL batches into a fresh `MemTable`, tracking post-catalog blob spans
/// and dead-byte increments from overwritten/deleted blob pointers.
fn replay_recovered_batches<F: PlatformFile>(
    memtable_capacity_bytes: usize,
    merged_batches: Vec<WalBatch>,
    segs: &mut OpenedSegments<F>,
) -> io::Result<MemTable> {
    let mut memtable = MemTable::new(memtable_capacity_bytes);
    for batch in merged_batches {
        let batch_lsn = batch.lsn;
        for record in batch.records {
            if let WalRecord::PutBlobPtr { blob_ptr: ptr, .. } = &record {
                let end = (ptr.offset as u64).saturating_add(ptr.len as u64);
                match ptr.kind {
                    BlobKind::Standard => {
                        if let (Some(&cat_len), Some(b)) = (
                            segs.cataloged_std_len.get(&ptr.seg_id),
                            segs.std_blobs.get(&ptr.seg_id),
                        ) {
                            record_post_catalog_span(
                                &mut segs.std_post_catalog_spans,
                                ptr.seg_id,
                                ptr.offset as u64,
                                end,
                                cat_len,
                                b.file_len,
                            );
                        }
                    }
                    BlobKind::Lru => {
                        let key = (ptr.table_id, ptr.seg_id);
                        if let (Some(&cat_len), Some(b)) =
                            (segs.cataloged_lru_len.get(&key), segs.lru_blobs.get(&key))
                        {
                            record_post_catalog_span(
                                &mut segs.lru_post_catalog_spans,
                                key,
                                ptr.offset as u64,
                                end,
                                cat_len,
                                b.file_len,
                            );
                        }
                    }
                }
            }
            let key_opt = match &record {
                WalRecord::PutInline { key, .. }
                | WalRecord::PutBlobPtr { key, .. }
                | WalRecord::Delete { key } => Some(key.as_slice()),
                WalRecord::TxBegin | WalRecord::TxCommit | WalRecord::TxContinue => None,
            };
            if let Some(key) = key_opt {
                let may_have_blob = (!segs.std_blobs.is_empty() || !segs.lru_blobs.is_empty())
                    && (key.len() < 3 || matches!(key[2], NS_PRIMARY_ROW | NS_ROW_META));
                let old_ptr = if may_have_blob {
                    if let Some(mem_entry) = memtable.get(key) {
                        match mem_entry.value {
                            IndexValue::Blob(ptr) => Some(ptr),
                            _ => None,
                        }
                    } else {
                        let mut found = None;
                        for seg in segs.idx_segments.iter().rev() {
                            if let Some((_lsn, val)) = seg.get(key)? {
                                if let IndexValue::Blob(ptr) = val {
                                    found = Some(ptr);
                                }
                                break;
                            }
                        }
                        found
                    }
                } else {
                    None
                };
                if let Some(ptr) = old_ptr {
                    match ptr.kind {
                        BlobKind::Standard => {
                            if let Some(b) = segs.std_blobs.get_mut(&ptr.seg_id) {
                                b.dead_bytes = b.dead_bytes.saturating_add(ptr.len as u64);
                            }
                        }
                        BlobKind::Lru => {
                            if let Some(b) = segs.lru_blobs.get_mut(&(ptr.table_id, ptr.seg_id)) {
                                b.dead_bytes = b.dead_bytes.saturating_add(ptr.len as u64);
                            }
                        }
                    }
                }
            }
            // Apply each record immediately so subsequent mutations to the same key within the
            // same `WalBatch` observe the updated `MemTable` entry.
            memtable.apply_single_wal_record(batch_lsn, record);
        }
    }
    Ok(memtable)
}

/// Phase 4: Prunes zero-live uncataloged blob segments, accounts post-catalog gaps, syncs any
/// recovered blob tails, and catalogs adopted uncataloged live segments.
fn reconcile_recovered_blobs<FS: PlatformFs>(
    meta_mgr: &mut BootMetaManager<FS>,
    segs: &mut OpenedSegments<FS::File>,
    memtable: &MemTable,
) -> io::Result<()> {
    // Compute exact currently-live bytes for any uncataloged blob segment across `memtable` and
    // unshadowed `idx_segments`. Stale pointers in older `idx` segments (including those
    // already overwritten or deleted before the last checkpoint) do not count as live: if an
    // uncataloged `blob_lru` was reclaimed before a crash while only lingering until WAL
    // rotation, computing exact live bytes avoids re-adopting it as a zombie segment.
    let has_uncataloged_std = segs
        .std_blobs
        .keys()
        .any(|seq| !segs.cataloged_std_seqs.contains(seq));
    let has_uncataloged_lru = segs
        .lru_blobs
        .keys()
        .any(|k| !segs.cataloged_lru_keys.contains(k));
    let mut uncataloged_std_live: BTreeMap<u64, u64> = BTreeMap::new();
    let mut uncataloged_lru_live: BTreeMap<(u16, u64), u64> = BTreeMap::new();
    if has_uncataloged_std || has_uncataloged_lru {
        for (_k, entry) in
            memtable.iter_range(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded)
        {
            if let IndexValue::Blob(ptr) = entry.value {
                match ptr.kind {
                    BlobKind::Standard
                        if !segs.cataloged_std_seqs.contains(&ptr.seg_id)
                            && segs.std_blobs.contains_key(&ptr.seg_id) =>
                    {
                        let e = uncataloged_std_live.entry(ptr.seg_id).or_default();
                        *e = e.saturating_add(ptr.len as u64);
                    }
                    BlobKind::Lru
                        if !segs
                            .cataloged_lru_keys
                            .contains(&(ptr.table_id, ptr.seg_id))
                            && segs.lru_blobs.contains_key(&(ptr.table_id, ptr.seg_id)) =>
                    {
                        let e = uncataloged_lru_live
                            .entry((ptr.table_id, ptr.seg_id))
                            .or_default();
                        *e = e.saturating_add(ptr.len as u64);
                    }
                    _ => {}
                }
            }
        }
        for (seg_idx, seg) in segs.idx_segments.iter().enumerate() {
            let mut cur = seg.cursor_all()?;
            while let Some(entry) = cur.next_entry()? {
                let IndexValue::Blob(ptr) = entry.value else {
                    continue;
                };
                let is_target = match ptr.kind {
                    BlobKind::Standard => {
                        !segs.cataloged_std_seqs.contains(&ptr.seg_id)
                            && segs.std_blobs.contains_key(&ptr.seg_id)
                    }
                    BlobKind::Lru => {
                        !segs
                            .cataloged_lru_keys
                            .contains(&(ptr.table_id, ptr.seg_id))
                            && segs.lru_blobs.contains_key(&(ptr.table_id, ptr.seg_id))
                    }
                };
                if !is_target || memtable.get(&entry.key).is_some() {
                    continue;
                }
                let mut shadowed = false;
                for newer in &segs.idx_segments[seg_idx + 1..] {
                    if newer.get(&entry.key)?.is_some() {
                        shadowed = true;
                        break;
                    }
                }
                if shadowed {
                    continue;
                }
                match ptr.kind {
                    BlobKind::Standard => {
                        let e = uncataloged_std_live.entry(ptr.seg_id).or_default();
                        *e = e.saturating_add(ptr.len as u64);
                    }
                    BlobKind::Lru => {
                        let e = uncataloged_lru_live
                            .entry((ptr.table_id, ptr.seg_id))
                            .or_default();
                        *e = e.saturating_add(ptr.len as u64);
                    }
                }
            }
        }
    }

    // Reconcile unreferenced uncommitted tail bytes and prune any uncataloged
    // blob segment that holds zero currently-live bytes.
    //
    // Invariant: persisted dead_bytes must only reflect mutations at or below checkpoint_lsn
    // (plus idempotent tail garbage). The open-time commit below does not advance
    // checkpoint_lsn, so it must NOT persist the WAL-replay increments: a crash before the
    // post-recovery checkpoint would replay the same batches and double-count, eventually
    // deleting segments that still hold live blobs. Tail garbage is idempotent because the
    // committed file_len becomes the physical length, so a re-open computes no new garbage.
    // The in-memory values (with replay increments) are persisted by the post-recovery
    // flush together with the new checkpoint_lsn.
    let mut std_persist_dead: BTreeMap<u64, u64> = BTreeMap::new();
    let mut lru_persist_dead: BTreeMap<(u16, u64), u64> = BTreeMap::new();
    let mut had_uncataloged_live_blobs = false;
    segs.std_blobs.retain(|&seq_id, b| {
        if !segs.cataloged_std_seqs.contains(&seq_id) {
            let live = uncataloged_std_live.get(&seq_id).copied().unwrap_or(0);
            if live == 0 {
                return false;
            }
            had_uncataloged_live_blobs = true;
            let wal_dead_delta = b.dead_bytes;
            let total_dead = b.file_len.saturating_sub(live);
            let persisted = total_dead.saturating_sub(wal_dead_delta);
            b.dead_bytes = total_dead;
            std_persist_dead.insert(seq_id, persisted);
            return true;
        }
        let persisted = account_post_catalog_gap(
            seq_id,
            b.file_len,
            &mut b.dead_bytes,
            &segs.cataloged_std_len,
            &mut segs.std_post_catalog_spans,
            &segs.std_base_dead,
        );
        std_persist_dead.insert(seq_id, persisted);
        true
    });
    segs.lru_blobs.retain(|&(tid, seq_id), b| {
        if !segs.cataloged_lru_keys.contains(&(tid, seq_id)) {
            let live = uncataloged_lru_live
                .get(&(tid, seq_id))
                .copied()
                .unwrap_or(0);
            if live == 0 {
                return false;
            }
            had_uncataloged_live_blobs = true;
            let wal_dead_delta = b.dead_bytes;
            let total_dead = b.file_len.saturating_sub(live);
            let persisted = total_dead.saturating_sub(wal_dead_delta);
            b.dead_bytes = total_dead;
            lru_persist_dead.insert((tid, seq_id), persisted);
            return true;
        }
        let persisted = account_post_catalog_gap(
            (tid, seq_id),
            b.file_len,
            &mut b.dead_bytes,
            &segs.cataloged_lru_len,
            &mut segs.lru_post_catalog_spans,
            &segs.lru_base_dead,
        );
        lru_persist_dead.insert((tid, seq_id), persisted);
        true
    });

    // Cataloged lengths are the durable prefix. Adopted segments start at zero.
    // Finish writeback before publishing a longer length or checkpointing the WAL.
    for (seq_id, b) in &segs.std_blobs {
        let start = segs.cataloged_std_len.get(seq_id).copied().unwrap_or(0);
        persist_recovered_blob_tail(&**b.reader.file(), start, b.file_len)?;
    }
    for (key, b) in &segs.lru_blobs {
        let start = segs.cataloged_lru_len.get(key).copied().unwrap_or(0);
        persist_recovered_blob_tail(&**b.reader.file(), start, b.file_len)?;
    }

    // Catalog uncataloged blob segments that turned out to hold live records. Their SeqIDs are
    // already below `next_seq_id` (see `reserve_seq_ids_above_existing_files`).
    if had_uncataloged_live_blobs {
        let std_metas: Vec<StdBlobSegmentMeta> = segs
            .std_blobs
            .iter()
            .map(|(&seq_id, b)| StdBlobSegmentMeta {
                seq_id,
                file_len: b.file_len,
                dead_bytes: std_persist_dead.get(&seq_id).copied().unwrap_or(0),
            })
            .collect();
        let lru_metas: Vec<LruBlobSegmentMeta> = segs
            .lru_blobs
            .iter()
            .map(|(&(tid, seq_id), b)| LruBlobSegmentMeta {
                seq_id,
                file_len: b.file_len,
                dead_bytes: lru_persist_dead.get(&(tid, seq_id)).copied().unwrap_or(0),
                table_id: b.table_id,
            })
            .collect();
        meta_mgr.commit_update(|st| {
            st.std_blob_segments = std_metas;
            st.lru_blob_segments = lru_metas;
        })?;
    }
    Ok(())
}

/// Phase 5: Constructs `StorageEngine`, rotates WAL epochs to isolate stale same-epoch sectors,
/// reconciles capped table quotas/ticks, and sweeps unreferenced orphan files.
fn finalize_open_engine<FS: PlatformFs>(
    fs: Arc<FS>,
    config: EngineConfig,
    meta_mgr: BootMetaManager<FS>,
    caches: (Arc<VerifiedBlockCache>, Arc<VerifiedBlobCache>),
    segs: OpenedSegments<FS::File>,
    memtable: MemTable,
    wal: RecoveredWalState<FS::File>,
) -> io::Result<StorageEngine<FS>> {
    let mut capped_tables = HashMap::new();
    for q in &meta_mgr.state().table_quotas {
        if q.eviction_policy == EvictionPolicy::Lru {
            let mut ct = CappedTableState::new(q.table_id, q.max_bytes);
            ct.current_logical_bytes = q.current_logical_bytes;
            capped_tables.insert(q.table_id, ct);
        }
    }

    let (block_cache, blob_cache) = caches;
    let access_tracker = AccessTracker::new(1, config.max_in_memory_access_ticks);
    let next_lsn = wal
        .max_recovered_lsn
        .checked_add(1)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "WAL LSN overflow"))?;
    let mut engine = StorageEngine {
        engine_id: snapshot::alloc_engine_id(),
        snapshot_cache: RwLock::new(snapshot::SnapshotCache::new()),
        fs,
        config,
        storage: health::HealthGatedStorage::new(meta_mgr, wal.sync_wal, wal.buf_wal),
        next_lsn,
        memtable,
        idx_segments: segs.idx_segments,
        std_blobs: segs.std_blobs,
        lru_blobs: segs.lru_blobs,
        capped_tables,
        access_tracker,
        tx_state: tx::TxState::Idle,
        scratch_tx_buf: Vec::new(),
        commit_gate: Arc::new(tx::CommitGate::default()),
        pending_group: std::collections::VecDeque::new(),
        unlink_after_wal_rotation: Vec::new(),
        auto_compaction_due: false,
        relocating_std_blobs: ScopedRelocatingFlag::default(),
        reconciling_capped_table: ScopedReconcilingTable::default(),
        unrelocatable_std_blobs: HashSet::new(),
        compaction_hook: None,
        compaction_hook_id: 0,
        in_flight_autocommit_blob_starts: Vec::new(),
        active_vtab_ids: HashMap::new(),
        unique_marker_gen: 1,
        next_vtab_token: 1,
        active_vtab_holders: HashMap::new(),
        mutation_epoch: 0,
        table_mutation_epochs: HashMap::new(),
        table_has_synthetic_rowids: BTreeMap::new(),
        data_generation: 0,
        table_has_row_meta: BTreeMap::new(),
        last_buf_append_lsn: 0,
        last_buf_synced_lsn: 0,
        // `max_recovered_lsn` is the highest LSN seen anywhere, discarded batches included.
        // Everything replayed is checkpointed before the epochs rotate (see below), at
        // `next_lsn - 1`, which equals it, so the first new batch links to a predecessor
        // at or below the checkpoint, as recovery requires.
        wal_links: wal_links::WalLinks::new(wal.max_recovered_lsn),
        last_wal_recovery: wal.wal_report,
        maintenance_failures: AtomicU64::new(0),
        last_maintenance_error: Mutex::new(None),
        corrupt_policies: HashMap::new(),
        table_inline_thresholds: BTreeMap::new(),
        scratch_wal_encoded: Vec::with_capacity(1024),
        block_cache,
        blob_cache,
        corrupt_blob_detections: AtomicU64::new(0),
        pending_corrupt_evictions: Mutex::new(HashMap::new()),
        #[cfg(test)]
        debug_fail_next_wal_append: false,
        #[cfg(test)]
        debug_fail_next_wal_sync: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        #[cfg(test)]
        tx_trace: None,
    };

    // Reconcile capped byte totals in read-only mode before rotating WAL epochs so a non-empty
    // `flush_memtable()` commits the updated `table_quotas` atomically with the checkpoint.
    let needs_capped_repair = if wal.replayed_wal && !engine.capped_tables.is_empty() {
        engine.reconcile_capped_tables_readonly()?
    } else {
        false
    };

    // Always rotate both WAL epochs after recovery, even when nothing was recovered.
    // Recovery halts at the first torn/old sector, but later sectors of the SAME epoch may
    // already be on disk (write-back reordering before a crash). If we kept the epoch, new
    // writes would restart at that halt point and a later recovery could run past the new
    // data into those stale same-epoch sectors, replaying batches that were never recovered
    // the first time (with colliding LSNs). A fresh epoch makes every pre-crash sector
    // unreadable as part of the new stream. Cost: one boot_meta commit per open.
    // Rotation must precede reconciliation, which appends to the WAL.
    if !engine.memtable.is_empty() {
        engine.flush_memtable()?;
    } else {
        engine.rotate_wal_epochs_only_with_quotas(
            wal.replayed_wal && !engine.capped_tables.is_empty(),
        )?;
    }

    // Reconciliation records land in the fresh epoch and are replayed by the next recovery.
    if wal.replayed_wal {
        if needs_capped_repair {
            engine.reconcile_capped_tables_after_recovery(true)?;
        }
    } else {
        engine.reconcile_capped_tables_after_recovery(false)?;
    }

    // Now that WAL recovery and the post-recovery checkpoint are complete, sweep any truly
    // orphaned segment/manifest files and process pending deletes!
    engine.healthy()?.sweep_orphans()?;

    Ok(engine)
}

impl<FS: PlatformFs> StorageEngine<FS> {
    /// Opens or recovers a `StorageEngine` rooted at `fs`.
    pub fn open(fs: Arc<FS>, config: EngineConfig) -> io::Result<Self> {
        Ok(Self::open_inner(fs, config, false)?.0)
    }

    /// Opens as [`WalDamagePolicy::Truncate`] does, whatever `config.wal_damage_policy` says,
    /// and returns the transactions that recovery discarded at or after the cut
    /// (ARCHITECTURE.md 4.6.7), in LSN order. With no damage the list is empty and this is
    /// exactly [`Self::open`].
    ///
    /// Only transactions whose batches are all intact are returned, and only their primary-row
    /// operations, in the encoding of the engine (`key` and `value` as `put_row` takes them, with
    /// the table's catalog name; a value kept in a blob is read before the open reclaims it).
    /// Secondary and unique index entries, rowid mappings and row metadata are derived data and
    /// are left out. A transaction that contains a schema change, a row of a table the catalog
    /// does not know, or a value whose blob fails verification is not returned; its operations of
    /// that kind are counted in [`crate::wal::WalDamage::unsalvageable`]. A multi-chunk transaction that lost
    /// a chunk is neither returned nor counted.
    ///
    /// The list lives only in memory. A crash before the open has rotated the WAL epochs
    /// reproduces it at the next open; afterwards the discarded batches are gone.
    pub fn open_with_salvage(
        fs: Arc<FS>,
        mut config: EngineConfig,
    ) -> io::Result<(Self, Vec<SalvagedTx>)> {
        config.wal_damage_policy = WalDamagePolicy::Truncate;
        Self::open_inner(fs, config, true)
    }

    /// The common path of [`Self::open`] and [`Self::open_with_salvage`]. `with_salvage` asks for
    /// the discarded transactions; the `unsalvageable` count of the report is computed either way.
    fn open_inner(
        fs: Arc<FS>,
        config: EngineConfig,
        with_salvage: bool,
    ) -> io::Result<(Self, Vec<SalvagedTx>)> {
        let mut meta_mgr = BootMetaManager::open_or_recover_deferred_sweep(fs.clone())?;
        let state_snapshot = meta_mgr.state().clone();

        let block_cache = Arc::new(VerifiedBlockCache::new(config.block_cache_bytes));
        let blob_cache = Arc::new(VerifiedBlobCache::new(config.blob_cache_bytes));

        let mut segs =
            open_cataloged_and_uncataloged_segments(&*fs, &state_snapshot, &block_cache)?;
        let (mut wal, merged_batches) =
            recover_wal_and_salvage(&*fs, &config, &mut meta_mgr, &segs, with_salvage)?;
        let salvaged = core::mem::take(&mut wal.salvaged);
        let memtable =
            replay_recovered_batches(config.memtable_capacity_bytes, merged_batches, &mut segs)?;
        reconcile_recovered_blobs(&mut meta_mgr, &mut segs, &memtable)?;
        let engine = finalize_open_engine(
            fs,
            config,
            meta_mgr,
            (block_cache, blob_cache),
            segs,
            memtable,
            wal,
        )?;
        Ok((engine, salvaged))
    }
}
