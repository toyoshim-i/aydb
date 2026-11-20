//! Ordered two-stream WAL recovery (ARCHITECTURE.md 4.6).
//!
//! Recovery replays exactly the batches below one **cut point** in the global LSN order and then
//! decides, from the durable watermark, whether the cut lost anything that had been acknowledged:
//!
//! 1. Both streams are scanned past their first unusable sector ([`recover_single_wal_stream`]).
//! 2. Every intact batch is merged by LSN and the predecessor chain is walked ([`walk_chain`]).
//!    The first broken link, unverifiable blob or batch that may not be replayed is the cut.
//! 3. `floor`, the highest `durable_lsn` found anywhere, tells damage from the benign unsynced
//!    tail of a power cut ([`judge`]): damage if and only if the batch at `floor` is not in the
//!    replayed prefix.
//! 4. The batches below the cut are replayed, except a multi-chunk transaction that the cut
//!    interrupted ([`complete_transactions`]).

use std::io;

use super::{
    BlobPtr, SingleStreamRecovery, WalBatch, WalRecord, WalScanMode,
    recover_single_wal_stream_with_mode,
};
use crate::pal::PlatformFile;

/// A count for each of the two log streams.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StreamCounts {
    /// `wal_sync.db`.
    pub sync: u64,
    /// `wal_buf.db`.
    pub buffered: u64,
}

impl StreamCounts {
    fn bump(&mut self, stream: Stream) {
        match stream {
            Stream::Sync => self.sync += 1,
            Stream::Buffered => self.buffered += 1,
        }
    }
}

/// Why recovery considers the WAL damaged (ARCHITECTURE.md 4.6.8). Media damage, not the unsynced
/// tail of a power cut, which is never reported.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalDamageReason {
    /// The chain is broken: a batch names a predecessor that is not there, and nothing more
    /// specific explains it.
    MissingPredecessor,
    /// A batch is present but a blob it points to failed validation.
    UnverifiableBlob,
    /// A current-epoch sector failed its CRC32C or did not parse, with acknowledged data beyond
    /// the lost range. When a corrupt sector and a missing predecessor both exist, this is
    /// reported: the two cannot be told apart from the fields the log keeps.
    CorruptSector,
    /// No batch names the lost range, but the batch at the durable watermark is gone: the log
    /// ends below something that was acknowledged.
    AcknowledgedTailLost,
    /// A current-epoch sector was written in another WAL format version (including the old
    /// format), with acknowledged data beyond the lost range.
    UnsupportedFormat,
}

/// A loss of acknowledged commits found by recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalDamage {
    pub reason: WalDamageReason,
    /// The lowest LSN proven lost: `min(cut, floor)`, where the cut is the exact LSN of the first
    /// missing or unverifiable batch that the chain names and `floor` is the durable watermark.
    /// Nothing at or after it was replayed. A lost run of batches is named by its last member, so
    /// when the watermark lies below the cut it is the lowest lost LSN known; the real first lost
    /// batch lies above the last replayed one and at or below it.
    pub first_lost_lsn: u64,
    /// The highest LSN found anywhere, in a batch header or as a `durable_lsn`.
    pub last_lsn_seen: u64,
    /// Intact batches found above the checkpoint that were not replayed: those at or after the
    /// cut, those past an unusable sector, and the chunks of a transaction the cut interrupted.
    pub discarded_batches: StreamCounts,
    /// Operations after the cut that salvage cannot return (ARCHITECTURE.md 4.6.7): a schema
    /// change (a catalog record), a primary-row record of a table the catalog does not know, and
    /// a value whose blob fails verification. Each such operation counts once, and its whole
    /// transaction is dropped from the salvage list. The multi-chunk transactions that lost a
    /// chunk are not counted; like the unsynced tail of a power cut they are simply lost. It is
    /// the same whether the log is opened by `open` or `open_with_salvage`. Recovery itself does
    /// not know tables or blobs, so the engine fills it in; recovery leaves it at 0.
    pub unsalvageable: u64,
}

impl std::fmt::Display for WalDamageReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::MissingPredecessor => "a batch is missing from the log",
            Self::UnverifiableBlob => "a blob of a logged batch failed verification",
            Self::CorruptSector => "a log sector is corrupt",
            Self::AcknowledgedTailLost => "the log ends below an acknowledged commit",
            Self::UnsupportedFormat => "a log sector has an unsupported format version",
        })
    }
}

impl std::fmt::Display for WalDamage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "WAL damaged: {} (first lost LSN {}, highest LSN seen {})",
            self.reason, self.first_lost_lsn, self.last_lsn_seen
        )
    }
}

/// Lets `WalDamagePolicy::Fail` hand the damage to the caller inside an `io::Error`.
impl std::error::Error for WalDamage {}

/// What the most recent `open` found in the WAL.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WalRecoveryReport {
    /// Current-epoch sectors that failed their CRC32C or did not parse. Informational: after a
    /// power cut these are torn writes of the unsynced tail, which are not damage. The ordinary
    /// end of the log (a never-written sector, or an intact sector of an older epoch) is not
    /// counted, and the scan reads nothing past it: a lost sector that still holds such content
    /// is not reported either.
    pub torn_tail_sectors: StreamCounts,
    /// Batches handed to replay, per stream (batches without a record to apply, such as a lone
    /// transaction marker, are not counted).
    pub replayed_batches: StreamCounts,
    /// The highest LSN replayed, or the checkpoint LSN when nothing was.
    pub last_replayed_lsn: u64,
    /// Set when acknowledged commits were lost.
    pub damage: Option<WalDamage>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Stream {
    Sync,
    Buffered,
}

/// An intact batch found in one of the logs.
#[derive(Debug)]
pub(crate) struct Found {
    pub batch: WalBatch,
    pub stream: Stream,
    /// Whether it lies in the valid prefix of its stream. A batch found past an unusable sector
    /// takes part in the chain walk but is never replayed.
    pub in_prefix: bool,
}

/// How the chain walk ended before the end of the batches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CutKind {
    /// A batch names a predecessor that is not there.
    Link,
    /// The batch is present but its blob failed validation.
    Blob,
    /// The chain reached a batch that lies past an unusable sector.
    PastStop,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Cut {
    pub lsn: u64,
    pub kind: CutKind,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Walk {
    /// Number of leading batches that are chain-valid and may be replayed.
    pub valid: usize,
    pub cut: Option<Cut>,
}

/// Walks the predecessor chain over `chain`, the intact batches above `checkpoint_lsn` in LSN
/// order. The first batch must link to an LSN at or below the checkpoint, every other one to the
/// batch before it, whatever LSN gaps lie between them (transactions leave gaps). The walk ends
/// at the first batch that does not link, whose blob does not verify, or that lies past an
/// unusable sector. A broken link names the cut: it is the missing predecessor's own LSN. A link
/// that points at or before the previous batch, or at or after its own batch, names nothing that
/// can be missing, so the batch itself is the cut.
pub(crate) fn walk_chain<V>(
    chain: &[Found],
    checkpoint_lsn: u64,
    mut validate_blob: V,
) -> io::Result<Walk>
where
    V: FnMut(&BlobPtr) -> io::Result<bool>,
{
    let mut last = checkpoint_lsn;
    for (i, found) in chain.iter().enumerate() {
        let b = &found.batch;
        let linked = if i == 0 {
            b.prev_lsn <= checkpoint_lsn
        } else {
            b.prev_lsn == last
        };
        let cut = if !linked {
            let named = if b.prev_lsn > last && b.prev_lsn < b.lsn {
                b.prev_lsn
            } else {
                b.lsn
            };
            Some(Cut {
                lsn: named,
                kind: CutKind::Link,
            })
        } else if !found.in_prefix {
            Some(Cut {
                lsn: b.lsn,
                kind: CutKind::PastStop,
            })
        } else if !blobs_valid(b, &mut validate_blob)? {
            Some(Cut {
                lsn: b.lsn,
                kind: CutKind::Blob,
            })
        } else {
            None
        };
        if cut.is_some() {
            return Ok(Walk { valid: i, cut });
        }
        last = b.lsn;
    }
    Ok(Walk {
        valid: chain.len(),
        cut: None,
    })
}

/// Whether every `PutBlobPtr` of `batch` passes `validate_blob`. An `Err` from the validator is
/// a real I/O failure and is propagated instead of being read as "blob absent".
fn blobs_valid<V>(batch: &WalBatch, validate_blob: &mut V) -> io::Result<bool>
where
    V: FnMut(&BlobPtr) -> io::Result<bool>,
{
    for rec in &batch.records {
        if let WalRecord::PutBlobPtr { blob_ptr, .. } = rec {
            if !validate_blob(blob_ptr)? {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

/// What the scans say about unusable sectors; they decide which reason a damage gets.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct StopFlags {
    pub unsupported_format: bool,
    pub corrupt_sector: bool,
}

/// Applies the damage rule of ARCHITECTURE.md 4.6.4: the WAL is damaged if and only if the batch
/// at `floor` was acknowledged and is not in the replayed prefix, that is `floor > last_good`.
/// `floor` is the `durable_lsn` of a batch that was found, so it names a real batch; one above
/// the last chain-valid batch is either missing or at or after the cut. This subsumes
/// "the cut is `<= floor`" (the cut is always above `last_good`) and also catches a lost
/// acknowledged batch when the chain names a later one as the cut, because a lost run of batches
/// is named by its last member.
///
/// Returns the reason and the lowest LSN proven lost, `min(cut, floor)`.
///
/// `last_good` is the LSN of the last chain-valid batch, or the checkpoint LSN. When the chain
/// has no break, the batch at `floor` is gone and the log ends below an acknowledged commit.
pub(crate) fn judge(
    cut: Option<Cut>,
    floor: u64,
    last_good: u64,
    flags: StopFlags,
) -> Option<(WalDamageReason, u64)> {
    if floor <= last_good {
        // Nothing acknowledged is missing: the unsynced tail of a power cut, however the
        // buffered stream was persisted.
        return None;
    }
    let (lost, base) = match cut {
        Some(c) if c.lsn <= floor => {
            let base = match c.kind {
                CutKind::Link => WalDamageReason::MissingPredecessor,
                CutKind::Blob => WalDamageReason::UnverifiableBlob,
                CutKind::PastStop => WalDamageReason::CorruptSector,
            };
            (c.lsn, base)
        }
        // The cut lies above the batch at `floor`, which is gone on its own.
        Some(c) if c.kind == CutKind::Link => (floor, WalDamageReason::MissingPredecessor),
        _ => (floor, WalDamageReason::AcknowledgedTailLost),
    };
    let reason = match base {
        // A blob that rotted is the cause whatever else is wrong with the logs.
        WalDamageReason::UnverifiableBlob => base,
        _ if flags.unsupported_format => WalDamageReason::UnsupportedFormat,
        _ if flags.corrupt_sector => WalDamageReason::CorruptSector,
        _ => base,
    };
    Some((reason, lost))
}

/// How a batch frames a transaction. A batch without any marker is a complete single-batch
/// transaction; a multi-chunk transaction is `Begin`, `Continue`.., `Commit`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Framing {
    None,
    Begin,
    Continue,
    Commit,
}

fn framing(batch: &WalBatch) -> Framing {
    let has = |m: &WalRecord| batch.records.contains(m);
    if has(&WalRecord::TxBegin) {
        Framing::Begin
    } else if has(&WalRecord::TxContinue) {
        Framing::Continue
    } else if has(&WalRecord::TxCommit) {
        Framing::Commit
    } else {
        Framing::None
    }
}

fn is_marker(r: &WalRecord) -> bool {
    matches!(
        r,
        WalRecord::TxBegin | WalRecord::TxContinue | WalRecord::TxCommit
    )
}

/// Keeps the transactions that completed within `batches`, the chain-valid batches below the cut
/// in LSN order. The `TxBegin`/`TxContinue`/`TxCommit` markers are stripped. A multi-chunk
/// transaction whose `TxCommit` is missing, because the cut lies inside it or the log ends there,
/// is dropped as a whole, so it is never partly applied. A new `TxBegin`, or a batch without
/// markers, also drops an earlier open transaction, and a `TxContinue` or `TxCommit` batch
/// without an open transaction is an orphan and is dropped.
pub(crate) fn complete_transactions(batches: Vec<Found>) -> Vec<Found> {
    let mut out = Vec::with_capacity(batches.len());
    let mut open: Vec<Found> = Vec::new();
    for mut found in batches {
        let kind = framing(&found.batch);
        found.batch.records.retain(|r| !is_marker(r));
        match kind {
            Framing::Begin => {
                open.clear();
                open.push(found);
            }
            Framing::Continue if !open.is_empty() => open.push(found),
            Framing::Commit if !open.is_empty() => {
                open.push(found);
                out.append(&mut open);
            }
            Framing::None => {
                open.clear();
                out.push(found);
            }
            Framing::Continue | Framing::Commit => {}
        }
    }
    out
}

/// A transaction that lies entirely at or after the cut, every batch of it intact, and that
/// recovery therefore discarded although it could be read (ARCHITECTURE.md 4.6.7).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscardedTx {
    /// LSN of the transaction's last batch.
    pub lsn: u64,
    /// All records of its batches in log order, without transaction markers.
    pub records: Vec<WalRecord>,
}

/// Finds the transactions that are discarded as a whole although their batches are intact:
/// those that lie completely at or after the cut (every index `>= valid` of the merged chain
/// `all`). A batch without markers is a complete single-batch transaction. A multi-chunk
/// transaction (`TxBegin`, `TxContinue`.., `TxCommit`) counts only if every chunk is present and
/// each links to the one before it by `prev_lsn` (the engine appends the chunks of a transaction
/// back to back). A `TxContinue` or `TxCommit` batch without its open transaction, because the
/// begin chunk or an earlier chunk is lost, is an orphan: it is never returned and not counted.
pub(crate) fn post_cut_transactions(all: &[Found], valid: usize) -> Vec<DiscardedTx> {
    // Each group is a list of indices into `all`; an open group is the last one, until it ends.
    let mut groups: Vec<Vec<usize>> = Vec::new();
    let mut open = false;
    for (i, found) in all.iter().enumerate() {
        let kind = framing(&found.batch);
        if open && kind != Framing::Begin && kind != Framing::None {
            let group = groups.last_mut().expect("an open group exists");
            if found.batch.prev_lsn == all[group[group.len() - 1]].batch.lsn {
                group.push(i);
                open = kind != Framing::Commit;
                continue;
            }
        }
        if open {
            // The open group did not finish: a chunk is missing. It is dropped.
            groups.pop();
            open = false;
        }
        match kind {
            Framing::Begin => {
                groups.push(vec![i]);
                open = true;
            }
            Framing::None => groups.push(vec![i]),
            Framing::Continue | Framing::Commit => {}
        }
    }
    if open {
        groups.pop();
    }
    groups
        .into_iter()
        .filter(|g| g.iter().all(|&i| i >= valid))
        .map(|g| {
            let last = g.last().map_or(0, |&i| all[i].batch.lsn);
            let records = g
                .iter()
                .flat_map(|&i| all[i].batch.records.iter())
                .filter(|r| !is_marker(r))
                .cloned()
                .collect();
            DiscardedTx { lsn: last, records }
        })
        .collect()
}

/// Output of the two-stream (`wal_sync.db` + `wal_buf.db`) crash-recovery coordinator.
#[derive(Debug, Clone)]
pub struct TwoStreamRecoveryResult {
    /// The batches to replay, ordered by ascending `lsn`: the chain-valid batches of both streams
    /// below the cut, without transaction markers.
    pub merged_batches: Vec<WalBatch>,
    /// The highest LSN seen anywhere (the checkpoint LSN if nothing higher): every batch found,
    /// replayed or discarded, and every `durable_lsn` read. The next LSN is one above it.
    pub max_recovered_lsn: u64,
    /// Next sector index for `wal_sync.db`.
    pub next_sync_sector_idx: usize,
    /// Next sector index for `wal_buf.db`.
    pub next_buf_sector_idx: usize,
    pub report: WalRecoveryReport,
    /// The salvageable transactions at or after the cut, in LSN order; filled in only when
    /// `report.damage` is set.
    pub discarded_txs: Vec<DiscardedTx>,
}

/// Recovers both `wal_sync.db` and `wal_buf.db` as one log (ARCHITECTURE.md 4.6): the batches
/// below a single cut point, in LSN order, are replayed in both streams, and nothing at or after
/// it. `validate_blob` enforces the `Write-Blob-Before-WAL` protocol: it returns `Ok(false)` for
/// a blob that was never durably written, which makes its batch the cut, and `Err` for a genuine
/// I/O error, which aborts recovery so that no committed batch is dropped because of a transient
/// failure.
pub fn recover_two_streams<F, V>(
    sync_file: &F,
    sync_epoch: u64,
    buf_file: &F,
    buf_epoch: u64,
    checkpoint_lsn: u64,
    validate_blob: V,
) -> io::Result<TwoStreamRecoveryResult>
where
    F: PlatformFile,
    V: FnMut(&BlobPtr) -> io::Result<bool>,
{
    recover_two_streams_with_mode(
        sync_file,
        sync_epoch,
        buf_file,
        buf_epoch,
        checkpoint_lsn,
        WalScanMode::StopAtEndOfLog,
        validate_blob,
    )
}

/// [`recover_two_streams`] with explicit control over whether end-of-log sectors end the stream
/// scan early ([`WalScanMode::StopAtEndOfLog`]) or are read past ([`WalScanMode::FullScan`]).
pub fn recover_two_streams_with_mode<F, V>(
    sync_file: &F,
    sync_epoch: u64,
    buf_file: &F,
    buf_epoch: u64,
    checkpoint_lsn: u64,
    scan_mode: WalScanMode,
    validate_blob: V,
) -> io::Result<TwoStreamRecoveryResult>
where
    F: PlatformFile,
    V: FnMut(&BlobPtr) -> io::Result<bool>,
{
    let sync_rec = recover_single_wal_stream_with_mode(sync_file, sync_epoch, scan_mode)?;
    let buf_rec = recover_single_wal_stream_with_mode(buf_file, buf_epoch, scan_mode)?;
    recover_ordered(sync_rec, buf_rec, checkpoint_lsn, validate_blob)
}

/// The part of [`recover_two_streams`] that works on the scans.
fn recover_ordered<V>(
    sync_rec: SingleStreamRecovery,
    buf_rec: SingleStreamRecovery,
    checkpoint_lsn: u64,
    validate_blob: V,
) -> io::Result<TwoStreamRecoveryResult>
where
    V: FnMut(&BlobPtr) -> io::Result<bool>,
{
    let flags = StopFlags {
        unsupported_format: sync_rec.unsupported_format.is_some()
            || buf_rec.unsupported_format.is_some(),
        corrupt_sector: sync_rec.corrupt_sectors > 0 || buf_rec.corrupt_sectors > 0,
    };
    let mut report = WalRecoveryReport {
        torn_tail_sectors: StreamCounts {
            sync: sync_rec.corrupt_sectors as u64,
            buffered: buf_rec.corrupt_sectors as u64,
        },
        last_replayed_lsn: checkpoint_lsn,
        ..Default::default()
    };
    let (next_sync_sector_idx, next_buf_sector_idx) =
        (sync_rec.next_sector_idx, buf_rec.next_sector_idx);

    // Everything found: the highest LSN of any kind, and the durable watermark.
    let mut max_lsn = checkpoint_lsn;
    let mut floor = 0u64;
    let mut all = Vec::new();
    for (stream, rec) in [(Stream::Sync, sync_rec), (Stream::Buffered, buf_rec)] {
        let prefix = rec.batches.into_iter().map(|b| (b, true));
        let past = rec.past_stop.into_iter().map(|b| (b, false));
        for (batch, in_prefix) in prefix.chain(past) {
            max_lsn = max_lsn.max(batch.lsn).max(batch.durable_lsn);
            floor = floor.max(batch.durable_lsn);
            all.push(Found {
                batch,
                stream,
                in_prefix,
            });
        }
    }
    // Batches at or below the checkpoint are already in an `idx` segment. They only count above.
    all.retain(|f| f.batch.lsn > checkpoint_lsn);
    all.sort_by_key(|f| f.batch.lsn);

    let walk = walk_chain(&all, checkpoint_lsn, validate_blob)?;
    let last_good = match walk.valid {
        0 => checkpoint_lsn,
        n => all[n - 1].batch.lsn,
    };
    let mut found_per_stream = StreamCounts::default();
    for f in &all {
        found_per_stream.bump(f.stream);
    }
    let verdict = judge(walk.cut, floor, last_good, flags);
    let discarded_txs = if verdict.is_some() {
        post_cut_transactions(&all, walk.valid)
    } else {
        Vec::new()
    };
    all.truncate(walk.valid);
    let completed = complete_transactions(all);

    // Count what is applied per stream, and what was found but not applied.
    let mut applied = StreamCounts::default();
    let mut merged = Vec::with_capacity(completed.len());
    let mut kept = StreamCounts::default();
    for found in completed {
        kept.bump(found.stream);
        if found.batch.records.is_empty() {
            continue;
        }
        applied.bump(found.stream);
        report.last_replayed_lsn = found.batch.lsn;
        merged.push(found.batch);
    }
    report.replayed_batches = applied;

    if let Some((reason, first_lost_lsn)) = verdict {
        report.damage = Some(WalDamage {
            reason,
            first_lost_lsn,
            last_lsn_seen: max_lsn,
            discarded_batches: StreamCounts {
                sync: found_per_stream.sync - kept.sync,
                buffered: found_per_stream.buffered - kept.buffered,
            },
            unsalvageable: 0,
        });
    }

    Ok(TwoStreamRecoveryResult {
        merged_batches: merged,
        max_recovered_lsn: max_lsn,
        next_sync_sector_idx,
        next_buf_sector_idx,
        report,
        discarded_txs,
    })
}
