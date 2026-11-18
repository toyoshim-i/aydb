//! Tests of ordered recovery (ARCHITECTURE.md 4.6, 4.6.11): the chain walk, the damage rule and
//! what the scans find past an unusable sector.

use super::recovery::{
    CutKind, Found, StopFlags, Stream, complete_transactions, judge, walk_chain,
};
use super::*;
use crate::pal::OsFs;
use crate::util::test_util::TempTestDir;

// ---- The chain walk and the damage rule, on batches built in memory ----

fn found(lsn: u64, prev: u64, durable: u64) -> Found {
    let mut batch = WalBatch::new(
        lsn,
        vec![WalRecord::PutInline {
            key: lsn.to_le_bytes().to_vec(),
            value: Vec::new(),
        }],
    );
    batch.prev_lsn = prev;
    batch.durable_lsn = durable;
    Found {
        batch,
        stream: Stream::Sync,
        in_prefix: true,
    }
}

fn walk(chain: &[Found], checkpoint: u64) -> (usize, Option<(u64, CutKind)>) {
    let w = walk_chain(chain, checkpoint, |_| Ok(true)).unwrap();
    (w.valid, w.cut.map(|c| (c.lsn, c.kind)))
}

#[test]
fn lsn_gaps_are_not_breaks() {
    // Transactions and multi-chunk transactions leave LSNs unused; the links still chain.
    let chain = [
        found(3, 0, 0),
        found(9, 3, 0),
        found(10, 9, 0),
        found(40, 10, 0),
    ];
    assert_eq!(walk(&chain, 0), (4, None));
}

#[test]
fn first_batch_links_to_the_checkpoint_or_below() {
    // At the checkpoint, below it (the predecessor was folded into an idx segment, and the LSN
    // between was a transaction statement), but not above it.
    for prev in [10, 9, 0] {
        assert_eq!(walk(&[found(12, prev, 0)], 10), (1, None), "prev {prev}");
    }
    assert_eq!(
        walk(&[found(13, 11, 0)], 10),
        (0, Some((11, CutKind::Link)))
    );
}

#[test]
fn a_broken_link_names_the_missing_predecessor() {
    // 7 is missing; 9 names it. The cut is 7 (a real LSN), not 6 or 8.
    let chain = [found(5, 4, 0), found(9, 7, 0), found(11, 9, 0)];
    assert_eq!(walk(&chain, 4), (1, Some((7, CutKind::Link))));
}

#[test]
fn a_link_that_cannot_name_a_missing_batch_cuts_at_the_batch_itself() {
    // prev_lsn at or before the batch before it, or at or after the batch itself.
    let chain = [found(5, 4, 0), found(9, 5, 0), found(11, 5, 0)];
    assert_eq!(walk(&chain, 4), (2, Some((11, CutKind::Link))));
    let chain = [found(5, 4, 0), found(9, 12, 0)];
    assert_eq!(walk(&chain, 4), (1, Some((9, CutKind::Link))));
}

#[test]
fn an_unverifiable_blob_is_the_cut_at_its_own_lsn() {
    let mut chain = [found(5, 4, 0), found(9, 5, 0), found(11, 9, 0)];
    chain[1].batch.records = vec![WalRecord::PutBlobPtr {
        key: b"k".to_vec(),
        blob_ptr: BlobPtr {
            kind: BlobKind::Standard,
            table_id: 1,
            seg_id: 77,
            offset: 0,
            len: 8,
            crc32c: 0,
        },
    }];
    let w = walk_chain(&chain, 4, |p| Ok(p.seg_id != 77)).unwrap();
    assert_eq!((w.valid, w.cut.unwrap().lsn), (1, 9));
    assert_eq!(w.cut.unwrap().kind, CutKind::Blob);
    // A validator error is an I/O failure, not a missing blob.
    let err = walk_chain(&chain, 4, |_| Err(io::Error::other("disk"))).unwrap_err();
    assert_eq!(err.to_string(), "disk");
}

#[test]
fn a_batch_past_an_unusable_sector_is_never_replayed() {
    let mut chain = [found(5, 4, 0), found(6, 5, 0)];
    chain[1].in_prefix = false;
    assert_eq!(walk(&chain, 4), (1, Some((6, CutKind::PastStop))));
}

#[test]
fn damage_rule_is_cut_at_or_below_floor() {
    let none = StopFlags::default();
    let link = |lsn| {
        Some(super::recovery::Cut {
            lsn,
            kind: CutKind::Link,
        })
    };
    // Above the floor: the unsynced tail of a power cut.
    assert_eq!(judge(link(11), 10, 10, none), None);
    // At or below it: acknowledged data is gone.
    assert_eq!(
        judge(link(10), 10, 9, none),
        Some((WalDamageReason::MissingPredecessor, 10))
    );
    // No break, nothing missing above the last good batch.
    assert_eq!(judge(None, 10, 10, none), None);
    assert_eq!(judge(None, 4, 10, none), None);
    // No break, but the batch at the floor is gone: the tail beyond the chain reaches below it.
    assert_eq!(
        judge(None, 12, 10, none),
        Some((WalDamageReason::AcknowledgedTailLost, 12))
    );
}

#[test]
fn damage_reason_prefers_the_blob_then_the_format_then_the_corrupt_sector() {
    let all = StopFlags {
        unsupported_format: true,
        corrupt_sector: true,
    };
    let cut = |kind| Some(super::recovery::Cut { lsn: 5, kind });
    let reason = |c, f| judge(c, 9, 4, f).map(|d| d.0);
    assert_eq!(
        reason(cut(CutKind::Blob), all),
        Some(WalDamageReason::UnverifiableBlob)
    );
    assert_eq!(
        reason(cut(CutKind::Link), all),
        Some(WalDamageReason::UnsupportedFormat)
    );
    let corrupt = StopFlags {
        unsupported_format: false,
        corrupt_sector: true,
    };
    assert_eq!(
        reason(cut(CutKind::Link), corrupt),
        Some(WalDamageReason::CorruptSector)
    );
    assert_eq!(reason(None, corrupt), Some(WalDamageReason::CorruptSector));
}

#[test]
fn an_interrupted_transaction_is_dropped_as_a_whole() {
    let tx = |lsn, prev, begin: bool, commit: bool| {
        let mut f = found(lsn, prev, 0);
        if begin {
            f.batch.records.insert(0, WalRecord::TxBegin);
        }
        if commit {
            f.batch.records.push(WalRecord::TxCommit);
        }
        f
    };
    let lsns = |v: Vec<Found>| v.iter().map(|f| f.batch.lsn).collect::<Vec<_>>();
    // A plain batch, a complete three-chunk transaction, a plain one, then a transaction whose
    // last chunk the cut removed.
    let batches = vec![
        found(1, 0, 0),
        tx(2, 1, true, false),
        tx(3, 2, false, false),
        tx(4, 3, false, true),
        found(5, 4, 0),
        tx(6, 5, true, false),
        tx(7, 6, false, false),
    ];
    let out = complete_transactions(batches);
    assert_eq!(lsns(out), vec![1, 2, 3, 4, 5]);
}

#[test]
fn transaction_markers_are_stripped_and_a_new_begin_drops_an_open_transaction() {
    let mut a = found(1, 0, 0);
    a.batch.records.insert(0, WalRecord::TxBegin);
    let mut b = found(2, 1, 0);
    b.batch.records.insert(0, WalRecord::TxBegin);
    b.batch.records.push(WalRecord::TxCommit);
    let out = complete_transactions(vec![a, b]);
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].batch.lsn, 2);
    assert_eq!(out[0].batch.records.len(), 1, "markers are stripped");
}

// ---- Recovery of real log files ----

type File = <OsFs as PlatformFs>::File;

struct Logs {
    _tmp: TempTestDir,
    sync: WalStreamWriter<File>,
    buf: WalStreamWriter<File>,
}

fn logs(name: &str) -> Logs {
    let tmp = TempTestDir::new(name);
    let fs = OsFs::open(&tmp.path, true).unwrap();
    let size = 256 * 1024u64;
    let sync = WalStreamWriter::open(
        &fs,
        WAL_SYNC_FILE_NAME,
        size,
        1,
        WalStreamMode::SyncSectorIsolated,
    )
    .unwrap();
    let buf = WalStreamWriter::open(
        &fs,
        WAL_BUF_FILE_NAME,
        size,
        1,
        WalStreamMode::BufferedImmediateOsCache,
    )
    .unwrap();
    Logs {
        _tmp: tmp,
        sync,
        buf,
    }
}

fn put(lsn: u64, prev: u64, durable: u64) -> WalBatch {
    let mut b = WalBatch::new(
        lsn,
        vec![WalRecord::PutInline {
            key: format!("k{lsn}").into_bytes(),
            value: b"v".to_vec(),
        }],
    );
    b.prev_lsn = prev;
    b.durable_lsn = durable;
    b
}

impl Logs {
    /// Appends to `wal_sync.db`; every batch has a sector of its own.
    fn sync(&mut self, b: &WalBatch) {
        self.sync.append_batch(b).unwrap();
    }

    /// Appends to `wal_buf.db` and seals the sector, so that every batch can be lost alone.
    fn buf(&mut self, b: &WalBatch) {
        self.buf.append_batch(b).unwrap();
        self.buf.seal_open_sector_for_durability().unwrap();
    }

    fn flip_bit(w: &WalStreamWriter<File>, sector: usize) {
        let off = (sector * WAL_SECTOR_SIZE + 100) as u64;
        let mut byte = [0u8; 1];
        w.file().read_exact_at(&mut byte, off).unwrap();
        byte[0] ^= 1;
        w.file().write_all_at(&byte, off).unwrap();
    }

    fn zero(w: &WalStreamWriter<File>, sector: usize) {
        let zeros = vec![0u8; WAL_SECTOR_SIZE];
        w.file()
            .write_all_at(&zeros, (sector * WAL_SECTOR_SIZE) as u64)
            .unwrap();
    }

    fn recover(&self, checkpoint: u64) -> TwoStreamRecoveryResult {
        recover_two_streams(
            &**self.sync.file(),
            1,
            &**self.buf.file(),
            1,
            checkpoint,
            |_| Ok(true),
        )
        .unwrap()
    }
}

fn lsns(r: &TwoStreamRecoveryResult) -> Vec<u64> {
    r.merged_batches.iter().map(|b| b.lsn).collect()
}

/// The first example of ARCHITECTURE.md 4.6.4: 10 (sync), 11 (buffered, lost), 12 (buffered,
/// kept). Nothing past 10 was synced, so floor is 10.
fn out_of_order_buffered_persistence() -> Logs {
    let mut l = logs("wal_ooo_buffered");
    l.sync(&put(10, 9, 9));
    // Appended after the sync of 10 completed: they carry watermark 10.
    l.buf(&put(11, 10, 10));
    l.buf(&put(12, 11, 10));
    // Batch 11 lived in sector 0 of the buffered log, and write-back left it torn. (A sector
    // that came back as zeros would end the scan instead, and 12 would not be found.)
    Logs::flip_bit(&l.buf, 0);
    l
}

#[test]
fn out_of_order_buffered_persistence_is_silent() {
    let l = out_of_order_buffered_persistence();
    let r = l.recover(9);
    assert_eq!(lsns(&r), vec![10]);
    assert_eq!(r.report.damage, None, "11 > floor 10 is not damage");
    assert_eq!(
        r.report.replayed_batches,
        StreamCounts {
            sync: 1,
            buffered: 0
        }
    );
    assert_eq!(r.report.last_replayed_lsn, 10);
    // Batch 12 was found past the stop and is discarded, but its LSN is never reused.
    assert_eq!(r.max_recovered_lsn, 12);
}

#[test]
fn a_later_sync_batch_turns_the_same_loss_into_damage() {
    // The second example of 4.6.4: a sync batch 13, appended after 11 and 12 were made durable,
    // carries watermark 12. Batch 11 was acknowledged and is gone.
    let mut l = out_of_order_buffered_persistence();
    l.sync(&put(13, 12, 12));
    let r = l.recover(9);
    assert_eq!(lsns(&r), vec![10]);
    let d = r.report.damage.expect("damage");
    // The loss is a sector that fails its CRC32C, which the reason names (4.6.6).
    assert_eq!(d.reason, WalDamageReason::CorruptSector);
    assert_eq!(d.first_lost_lsn, 11);
    assert_eq!(d.last_lsn_seen, 13);
    assert_eq!(
        d.discarded_batches,
        StreamCounts {
            sync: 1,
            buffered: 1
        }
    );
    assert_eq!(d.unsalvageable, 0);
    assert_eq!(r.max_recovered_lsn, 13);
}

#[test]
fn a_lost_power_cut_tail_above_floor_is_silent() {
    let mut l = logs("wal_power_tail");
    l.sync(&put(10, 9, 9));
    l.buf(&put(11, 10, 10));
    l.buf(&put(12, 11, 10));
    l.buf(&put(13, 12, 10));
    // The last two buffered sectors never reached the disk (torn, then zero).
    Logs::flip_bit(&l.buf, 1);
    Logs::zero(&l.buf, 2);
    let r = l.recover(9);
    assert_eq!(lsns(&r), vec![10, 11]);
    assert_eq!(r.report.damage, None);
    assert_eq!(
        r.report.torn_tail_sectors,
        StreamCounts {
            sync: 0,
            buffered: 1
        }
    );
}

#[test]
fn floor_is_taken_from_batches_past_a_stop() {
    // Sync batches 1, 2, 3: sector of 2 is corrupt. Batch 3, found past the stop, says that 2
    // was durable (watermark 2), so its loss is damage. Without 3 the log would just end at 1.
    let mut l = logs("wal_floor_past_stop");
    l.sync(&put(1, 0, 0));
    l.sync(&put(2, 1, 1));
    l.sync(&put(3, 2, 2));
    Logs::flip_bit(&l.sync, 1);
    let r = l.recover(0);
    assert_eq!(lsns(&r), vec![1]);
    let d = r.report.damage.expect("damage");
    assert_eq!(d.reason, WalDamageReason::CorruptSector);
    assert_eq!(d.first_lost_lsn, 2);
    assert_eq!(
        d.discarded_batches,
        StreamCounts {
            sync: 1,
            buffered: 0
        }
    );
    assert_eq!(r.max_recovered_lsn, 3);
    assert_eq!(r.report.torn_tail_sectors.sync, 1);

    // The same loss with batch 3 carrying a lower watermark is only an unsynced tail.
    let mut l = logs("wal_floor_past_stop_low");
    l.sync(&put(1, 0, 0));
    l.sync(&put(2, 1, 1));
    l.sync(&put(3, 2, 1));
    Logs::flip_bit(&l.sync, 1);
    let r = l.recover(0);
    assert_eq!(lsns(&r), vec![1]);
    assert_eq!(r.report.damage, None);
}

#[test]
fn a_lost_batch_below_the_watermark_is_named_by_the_link_of_the_batch_that_proves_it() {
    // Everything above 2 in the sync log is gone except a buffered batch 5 that records
    // watermark 4. It proves that 3 and 4 existed and were durable, and its link names 4.
    let mut l = logs("wal_tail_named");
    l.sync(&put(1, 0, 0));
    l.buf(&put(2, 1, 1));
    l.buf(&put(5, 4, 4));
    let r = l.recover(0);
    assert_eq!(lsns(&r), vec![1, 2]);
    let d = r.report.damage.expect("damage");
    assert_eq!(d.reason, WalDamageReason::MissingPredecessor);
    assert_eq!(d.first_lost_lsn, 4);
}

#[test]
fn a_watermark_above_the_log_with_an_intact_chain_is_acknowledged_tail_lost() {
    // Not reachable with headers the engine writes: a watermark names a batch below the batch
    // that carries it, and an intact chain contains every such batch. A header that breaks that
    // (a bug, or a CRC collision) is still reported rather than trusted.
    let mut l = logs("wal_tail_lost");
    l.sync(&put(1, 0, 0));
    l.buf(&put(2, 1, 3));
    let r = l.recover(0);
    assert_eq!(lsns(&r), vec![1, 2]);
    let d = r.report.damage.expect("damage");
    assert_eq!(d.reason, WalDamageReason::AcknowledgedTailLost);
    assert_eq!(d.first_lost_lsn, 3);
    assert_eq!(d.discarded_batches, StreamCounts::default());
    assert_eq!(r.max_recovered_lsn, 3);
}

#[test]
fn a_continuation_fragment_after_a_hole_is_skipped() {
    let mut l = logs("wal_continuation");
    l.sync(&put(1, 0, 0));
    // Batch 2 spans three sectors (1: First, 2: Middle, 3: Last).
    let mut big = put(2, 1, 1);
    big.records = vec![WalRecord::PutInline {
        key: b"big".to_vec(),
        value: vec![7u8; 9000],
    }];
    l.sync(&big);
    l.sync(&put(3, 2, 2));
    Logs::flip_bit(&l.sync, 1);

    let rec = recover_single_wal_stream(&**l.sync.file(), 1).unwrap();
    assert_eq!(
        rec.batches.iter().map(|b| b.lsn).collect::<Vec<_>>(),
        vec![1]
    );
    // The Middle and Last fragments of batch 2 are skipped without being counted as corruption:
    // only the sector that was hit is.
    assert_eq!(rec.corrupt_sectors, 1);
    assert_eq!(
        rec.past_stop.iter().map(|b| b.lsn).collect::<Vec<_>>(),
        vec![3]
    );
    assert_eq!(rec.next_sector_idx, 1);

    let r = l.recover(0);
    assert_eq!(lsns(&r), vec![1]);
    let d = r.report.damage.expect("damage");
    assert_eq!(d.first_lost_lsn, 2);
    assert_eq!(d.reason, WalDamageReason::CorruptSector);
}

fn tx_chunk(lsn: u64, prev: u64, begin: bool, commit: bool) -> WalBatch {
    let mut b = put(lsn, prev, 0);
    if begin {
        b.records.insert(0, WalRecord::TxBegin);
    }
    if commit {
        b.records.push(WalRecord::TxCommit);
    }
    b
}

#[test]
fn a_multi_chunk_transaction_cut_in_the_middle_is_not_applied() {
    // A plain batch, a complete transaction (2, 3), and a transaction (4, 5, 6) whose last chunk
    // has a blob that did not reach the disk.
    let mut l = logs("wal_tx_cut");
    l.buf(&put(1, 0, 0));
    l.buf(&tx_chunk(2, 1, true, false));
    l.buf(&tx_chunk(3, 2, false, true));
    l.buf(&tx_chunk(4, 3, true, false));
    l.buf(&tx_chunk(5, 4, false, false));
    let mut last = tx_chunk(6, 5, false, true);
    last.records.push(WalRecord::PutBlobPtr {
        key: b"blob".to_vec(),
        blob_ptr: BlobPtr {
            kind: BlobKind::Standard,
            table_id: 1,
            seg_id: 9,
            offset: 0,
            len: 8,
            crc32c: 0,
        },
    });
    l.buf(&last);
    let r = recover_two_streams(&**l.sync.file(), 1, &**l.buf.file(), 1, 0, |p| {
        Ok(p.seg_id != 9)
    })
    .unwrap();
    assert_eq!(lsns(&r), vec![1, 2, 3], "chunks 4 and 5 are not applied");
    assert!(
        r.merged_batches
            .iter()
            .flat_map(|b| &b.records)
            .all(|r| !matches!(r, WalRecord::TxBegin | WalRecord::TxCommit))
    );
    assert_eq!(r.max_recovered_lsn, 6);
    assert_eq!(r.report.damage, None, "nothing was acknowledged");
    assert_eq!(r.report.replayed_batches.buffered, 3);
}

#[test]
fn a_transaction_missing_its_last_chunk_is_not_applied() {
    let mut l = logs("wal_tx_open");
    l.buf(&put(1, 0, 0));
    l.buf(&tx_chunk(2, 1, true, false));
    l.buf(&tx_chunk(3, 2, false, false));
    let r = l.recover(0);
    assert_eq!(lsns(&r), vec![1]);
    assert_eq!(r.report.damage, None);
    assert_eq!(r.max_recovered_lsn, 3);
}

#[test]
fn max_recovered_lsn_covers_discarded_batches_and_watermarks() {
    let mut l = logs("wal_max_lsn");
    l.sync(&put(1, 0, 0));
    l.sync(&put(2, 1, 1));
    // 7 is found past a stop (4 is lost) and records a watermark above every batch found.
    l.buf(&put(5, 4, 1));
    l.buf(&put(7, 5, 9));
    let r = l.recover(0);
    assert_eq!(lsns(&r), vec![1, 2]);
    assert_eq!(r.max_recovered_lsn, 9);
    // A watermark above the checkpoint that names no batch found is damage, too.
    assert!(r.report.damage.is_some());

    // Nothing found at all: the checkpoint.
    assert_eq!(logs("wal_max_lsn_empty").recover(33).max_recovered_lsn, 33);
}

/// Rewrites the format version of a sector and re-seals its CRC32C.
fn set_version(w: &WalStreamWriter<File>, sector: usize, version: u16) {
    let off = (sector * WAL_SECTOR_SIZE) as u64;
    let mut sec = vec![0u8; WAL_SECTOR_SIZE];
    w.file().read_exact_at(&mut sec, off).unwrap();
    sec[14..16].copy_from_slice(&version.to_le_bytes());
    let crc = crc32c(&sec[4..]);
    sec[0..4].copy_from_slice(&crc.to_le_bytes());
    w.file().write_all_at(&sec, off).unwrap();
}

#[test]
fn old_format_sector_with_acknowledged_data_beyond_it_is_unsupported_format() {
    let mut l = logs("wal_old_format");
    l.sync(&put(1, 0, 0));
    l.sync(&put(2, 1, 1));
    l.buf(&put(3, 2, 2));
    l.sync(&put(4, 3, 3));
    set_version(&l.sync, 1, 0);
    let r = l.recover(0);
    assert_eq!(lsns(&r), vec![1]);
    let d = r.report.damage.expect("damage");
    assert_eq!(d.reason, WalDamageReason::UnsupportedFormat);
    assert_eq!(d.first_lost_lsn, 2);
    assert_eq!(
        d.discarded_batches,
        StreamCounts {
            sync: 1,
            buffered: 1
        }
    );
    assert_eq!(r.report.torn_tail_sectors, StreamCounts::default());
}

#[test]
fn old_format_sector_with_nothing_beyond_it_cannot_be_told_from_a_tail() {
    // No batch past the sector names a lost LSN or records a watermark, so the damage rule has
    // nothing to compare: the rule needs a floor, and an unreadable sector carries no LSN.
    let mut l = logs("wal_old_format_tail");
    l.sync(&put(1, 0, 0));
    l.sync(&put(2, 1, 1));
    set_version(&l.sync, 1, 0);
    let r = l.recover(0);
    assert_eq!(lsns(&r), vec![1]);
    assert_eq!(r.report.damage, None);
}

#[test]
fn stale_epoch_sectors_past_the_tail_are_ignored() {
    // A sector of an older epoch after the tail is the ordinary end of the log.
    let mut l = logs("wal_stale_epoch");
    l.sync(&put(1, 0, 0));
    l.sync(&put(2, 1, 1));
    let r = recover_two_streams(&**l.sync.file(), 2, &**l.buf.file(), 1, 0, |_| Ok(true)).unwrap();
    assert!(r.merged_batches.is_empty());
    assert_eq!(r.report.damage, None);
    assert_eq!(r.max_recovered_lsn, 0);
}

// ---- Where the scan of a file ends (ARCHITECTURE.md 4.6.4, 4.6.9) ----

/// Rewrites the epoch of a sector and, when `reseal` is set, its CRC32C as well. A resealed
/// sector with a lower epoch looks like a leftover of an earlier epoch; an unsealed one like a
/// torn write.
fn set_epoch(w: &WalStreamWriter<File>, sector: usize, epoch: u64, reseal: bool) {
    let off = (sector * WAL_SECTOR_SIZE) as u64;
    let mut sec = vec![0u8; WAL_SECTOR_SIZE];
    w.file().read_exact_at(&mut sec, off).unwrap();
    sec[4..12].copy_from_slice(&epoch.to_le_bytes());
    if reseal {
        let crc = crc32c(&sec[4..]);
        sec[0..4].copy_from_slice(&crc.to_le_bytes());
    }
    w.file().write_all_at(&sec, off).unwrap();
}

fn sync_log_of_three(name: &str) -> Logs {
    let mut l = logs(name);
    l.sync(&put(1, 0, 0));
    l.sync(&put(2, 1, 1));
    l.sync(&put(3, 2, 2));
    l
}

fn scan_sync(l: &Logs) -> SingleStreamRecovery {
    recover_single_wal_stream(&**l.sync.file(), 1).unwrap()
}

fn past_lsns(rec: &SingleStreamRecovery) -> Vec<u64> {
    rec.past_stop.iter().map(|b| b.lsn).collect()
}

#[test]
fn an_intact_older_epoch_sector_ends_the_scan() {
    let l = sync_log_of_three("wal_end_stale");
    set_epoch(&l.sync, 1, 0, true);
    let rec = scan_sync(&l);
    assert_eq!(rec.batches.iter().map(|b| b.lsn).collect::<Vec<_>>(), [1]);
    assert_eq!(
        past_lsns(&rec),
        Vec::<u64>::new(),
        "nothing is read past the end"
    );
    assert_eq!(rec.corrupt_sectors, 0);
    assert_eq!(rec.next_sector_idx, 1);

    // Batch 3 must not reach the report, the floor or the LSN high-water mark either.
    let r = l.recover(0);
    assert_eq!(lsns(&r), vec![1]);
    assert_eq!(r.report.damage, None);
    assert_eq!(r.max_recovered_lsn, 1);
}

#[test]
fn an_all_zero_sector_ends_the_scan() {
    let l = sync_log_of_three("wal_end_zero");
    Logs::zero(&l.sync, 1);
    let rec = scan_sync(&l);
    assert_eq!(rec.batches.iter().map(|b| b.lsn).collect::<Vec<_>>(), [1]);
    assert_eq!(
        past_lsns(&rec),
        Vec::<u64>::new(),
        "nothing is read past the end"
    );
    assert_eq!(rec.corrupt_sectors, 0);

    let r = l.recover(0);
    assert_eq!(lsns(&r), vec![1]);
    assert_eq!(r.report.damage, None);
    assert_eq!(r.max_recovered_lsn, 1);
}

#[test]
fn a_crc_failure_does_not_end_the_scan() {
    // Guard: a corrupt sector is damage, not an end; the intact batch behind it is still found.
    let l = sync_log_of_three("wal_read_on_crc");
    Logs::flip_bit(&l.sync, 1);
    let rec = scan_sync(&l);
    assert_eq!(rec.batches.iter().map(|b| b.lsn).collect::<Vec<_>>(), [1]);
    assert_eq!(past_lsns(&rec), vec![3]);
    assert_eq!(rec.corrupt_sectors, 1);
}

#[test]
fn a_torn_older_epoch_sector_does_not_end_the_scan() {
    // An older epoch whose CRC32C fails cannot be told from a torn write: it is read past.
    let l = sync_log_of_three("wal_read_on_torn_stale");
    set_epoch(&l.sync, 1, 0, false);
    let rec = scan_sync(&l);
    assert_eq!(rec.batches.iter().map(|b| b.lsn).collect::<Vec<_>>(), [1]);
    assert_eq!(past_lsns(&rec), vec![3]);
    assert_eq!(
        rec.corrupt_sectors, 0,
        "an epoch mismatch was never counted as corruption"
    );
}

#[test]
fn a_newer_epoch_sector_does_not_end_the_scan() {
    let l = sync_log_of_three("wal_read_on_newer");
    set_epoch(&l.sync, 1, 2, true);
    let rec = scan_sync(&l);
    assert_eq!(rec.batches.iter().map(|b| b.lsn).collect::<Vec<_>>(), [1]);
    assert_eq!(past_lsns(&rec), vec![3]);
}

/// A file that counts what is read from it.
struct CountingFile {
    inner: Arc<File>,
    reads: std::sync::atomic::AtomicUsize,
    bytes: std::sync::atomic::AtomicUsize,
}

impl PlatformFile for CountingFile {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        self.reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.bytes
            .fetch_add(buf.len(), std::sync::atomic::Ordering::Relaxed);
        self.inner.read_at(buf, offset)
    }
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        self.reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.bytes
            .fetch_add(buf.len(), std::sync::atomic::Ordering::Relaxed);
        self.inner.read_exact_at(buf, offset)
    }
    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        self.inner.write_all_at(buf, offset)
    }
    fn file_len(&self) -> io::Result<u64> {
        self.inner.file_len()
    }
    fn preallocate(&self, target_len: u64, strategy: PreallocStrategy) -> io::Result<()> {
        self.inner.preallocate(target_len, strategy)
    }
    fn sync_barrier(&self) -> io::Result<()> {
        self.inner.sync_barrier()
    }
    fn sync_durable(&self) -> io::Result<()> {
        self.inner.sync_durable()
    }
    fn evict_os_cache(&self, offset: u64, len: u64) -> io::Result<()> {
        self.inner.evict_os_cache(offset, len)
    }
}

#[test]
fn a_file_left_over_from_an_older_epoch_is_read_one_window_at_most() {
    // What a clean reopen sees: the epoch was rotated, so sector 0 is of the epoch before.
    let tmp = TempTestDir::new("wal_clean_reopen_reads");
    let fs = OsFs::open(&tmp.path, true).unwrap();
    let mut w = WalStreamWriter::open(
        &fs,
        WAL_BUF_FILE_NAME,
        16 * 1024 * 1024,
        1,
        WalStreamMode::BufferedImmediateOsCache,
    )
    .unwrap();
    w.append_batch(&put(1, 0, 0)).unwrap();
    w.seal_open_sector_for_durability().unwrap();
    set_epoch(&w, 0, 0, true);
    let counting = CountingFile {
        inner: Arc::clone(w.file()),
        reads: Default::default(),
        bytes: Default::default(),
    };
    let rec = recover_single_wal_stream(&counting, 1).unwrap();
    assert!(rec.batches.is_empty());
    let reads = counting.reads.load(std::sync::atomic::Ordering::Relaxed);
    let bytes = counting.bytes.load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(reads, 1);
    assert!(bytes <= 64 * WAL_SECTOR_SIZE, "read {bytes} bytes");
}
