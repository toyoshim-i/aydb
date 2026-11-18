//! Media-damage and group-commit modes of the crash harness (ARCHITECTURE.md 4.6.11).
//!
//! # Media damage
//!
//! A power cut or `kill -9` only loses an unsynced tail. Media damage hits data that was already
//! synced. After a run and its crash, [`Harness::damage_wal`] corrupts the surviving log files
//! (bit flips, zeroed sectors, a truncated file tail, an intact sector of an older epoch) in
//! the sync stream, the buffered stream, or both, as chosen by the seed, and then checks what
//! recovery makes of it, on copies of the damaged directory:
//!
//! * `Fail` either opens without damage or refuses with `WalDamage`, and a refusal leaves every
//!   file, `boot_meta.db` included, byte-identical;
//! * the default `Truncate` opens and reports exactly what `Fail` found;
//! * `open_with_salvage` returns only whole transactions that the workload committed, with the
//!   right operations, none at or below the replayed prefix, and misses none that it could see.
//!
//! The real directory is then opened under `Truncate`, the replayed LSNs must be a prefix of the
//! logged ones (the existing LSN-order oracle) and the recovered state must be a state the
//! workload went through.
//!
//! # What the oracle expects to see reported
//!
//! The expectation is computed from the logs, never from the engine's verdict:
//!
//! 1. `logged`: the batches in the page-cache images of both logs just before the crash.
//! 2. `visible`: the batches the scan can see in the damaged files. The scan ends at the first
//!    sector that is all zeros or an intact sector of an older epoch (the ordinary end of the
//!    log, ARCHITECTURE.md 4.6.4). The oracle finds that sector itself ([`scan_end`]) and
//!    checks that the scan reads nothing at or past it.
//! 3. `lost = logged - visible` above the checkpoint, and `floor` = the highest `durable_lsn`
//!    among the visible batches.
//!
//! Damage must be reported if and only if some lost LSN is `<= floor`: a visible batch then
//! proves that an acknowledged batch is gone. A loss that no visible batch proves goes
//! unreported, and that is the accepted limitation, but only when that is really the case:
//! a lost batch above `floor`, or one whose proof sits behind a zeroed or stale sector (counted
//! as `hidden`) or beyond a truncated tail (counted as `unprovable`). It is never assumed away
//! wholesale for a seed.
//!
//! # Group commit
//!
//! With `group` set the workload drives the group commit the way the SQL layer does: several
//! connections append synchronous batches through `begin_commit_for_conn` without flushing, and
//! a separate step picks a waiting commit and either finds it durable or becomes the flush
//! leader. The steps run in one thread in a seed-chosen order so that a seed replays exactly.
//! A commit is acknowledged when the engine has published it, which happens only after the flush
//! that covers it, so the durable-watermark oracle must hold for batches whose flush was shared.
//! Group commit is only used without capped tables (the engine refuses to group otherwise), so a
//! group run writes tables 1 and 2 only.

use super::*;
use crate::util::crc32c::crc32c;
use crate::wal::{SingleStreamRecovery, recover_single_wal_stream};

/// What a seed exercises beyond the plain power-cut workload.
#[derive(Clone, Copy, Default, Debug)]
pub(super) struct Mode {
    /// Corrupt synced WAL data after the crash.
    pub media: bool,
    /// Drive concurrent commits through the group commit.
    pub group: bool,
}

/// Coverage counters of the two modes.
#[derive(Default)]
pub(super) struct MediaCov {
    pub damaged: AtomicU64,
    pub flips: AtomicU64,
    pub zeroed: AtomicU64,
    pub truncated: AtomicU64,
    pub stale: AtomicU64,
    /// Damaged directories where `Fail` refused (and left every file alone).
    pub refused: AtomicU64,
    /// ... of which lengthened a truncated log file (known bug).
    pub refused_extended: AtomicU64,
    /// Damaged directories that opened without a report although batches were lost.
    pub lost_unreported: AtomicU64,
    /// ... of which a batch behind a zeroed or stale sector would have proved the loss.
    pub hidden: AtomicU64,
    /// ... of which nothing in the logs could have proved it.
    pub unprovable: AtomicU64,
    /// Damage with a lost batch that had been acknowledged durable.
    pub lost_acked: AtomicU64,
    pub salvage_checks: AtomicU64,
    pub salvaged_txs: AtomicU64,
    pub salvage_visible_discarded: AtomicU64,
    pub group_tickets: AtomicU64,
    pub group_leads: AtomicU64,
    pub group_shared: AtomicU64,
    pub group_sync_path: AtomicU64,
}

impl MediaCov {
    pub(super) fn report(&self) -> String {
        let g = |c: &AtomicU64| c.load(Ordering::Relaxed);
        format!(
            "damaged={} (flip={} zero={} truncate={} stale={}) refused={} (extended_wal={}) \
             lost_unreported={} (hidden={} unprovable={}) lost_acked={} \
             salvage_checks={} salvaged_txs={} salvage_visible_discarded={} group_tickets={} \
             group_leads={} group_shared={} group_sync_path={}",
            g(&self.damaged),
            g(&self.flips),
            g(&self.zeroed),
            g(&self.truncated),
            g(&self.stale),
            g(&self.refused),
            g(&self.refused_extended),
            g(&self.lost_unreported),
            g(&self.hidden),
            g(&self.unprovable),
            g(&self.lost_acked),
            g(&self.salvage_checks),
            g(&self.salvaged_txs),
            g(&self.salvage_visible_discarded),
            g(&self.group_tickets),
            g(&self.group_leads),
            g(&self.group_shared),
            g(&self.group_sync_path),
        )
    }
}

/// The values one workload operation wrote to each row, in order (`None` = deleted). A
/// transaction that puts a row and deletes it leaves both records in its batch.
pub(super) type Touched = BTreeMap<(u16, u8), Vec<Option<Vec<u8>>>>;

/// The LSNs `[lo, hi)` that one workload operation allocated, and what it wrote.
pub(super) struct OpRec {
    pub lo: u64,
    pub hi: u64,
    pub touched: Touched,
}

/// A group commit that was appended but whose flush has not been seen to finish.
pub(super) struct Ticket {
    pub lsn: u64,
    /// The rows it writes.
    pub rows: Vec<(u16, u8)>,
    /// Index into `Harness::states` of the state the commit produces.
    pub state: usize,
    pub file: Arc<CrashFile>,
    pub gate: Arc<tx::CommitGate>,
}

const WAL_FILES: [&str; 2] = ["wal_sync.db", "wal_buf.db"];
const SECT: usize = crate::wal::WAL_SECTOR_SIZE;

// ---------------------------------------------------------------------------------------------
// Sector classification, as the scan sees it
// ---------------------------------------------------------------------------------------------

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
enum Sec {
    /// All zeros, or intact and of an older epoch: the ordinary end of the log.
    End,
    /// Current epoch with a valid CRC32C.
    Live,
    /// Anything else: torn, rotten, or of a newer epoch.
    Other,
}

fn classify(sector: &[u8], epoch: u64) -> Sec {
    if sector.iter().all(|&b| b == 0) {
        return Sec::End;
    }
    let sector_epoch = u64::from_le_bytes(sector[4..12].try_into().unwrap());
    let intact = crc32c(&sector[4..SECT]) == u32::from_le_bytes(sector[0..4].try_into().unwrap());
    if sector_epoch == epoch {
        if intact { Sec::Live } else { Sec::Other }
    } else if sector_epoch < epoch && intact {
        Sec::End
    } else {
        Sec::Other
    }
}

/// Index of the first sector the scan must not read past: the first `End` sector, or the number
/// of whole sectors in the file.
fn scan_end(img: &[u8], epoch: u64) -> usize {
    let total = img.len() / SECT;
    (0..total)
        .find(|&i| classify(&img[i * SECT..(i + 1) * SECT], epoch) == Sec::End)
        .unwrap_or(total)
}

fn live_sectors(img: &[u8], epoch: u64) -> Vec<usize> {
    (0..img.len() / SECT)
        .filter(|&i| classify(&img[i * SECT..(i + 1) * SECT], epoch) == Sec::Live)
        .collect()
}

fn scan_img(name: &str, img: Vec<u8>, epoch: u64) -> io::Result<SingleStreamRecovery> {
    let fs = scratch_fs(vec![(name.to_string(), img)]);
    recover_single_wal_stream(&*raw_file(&fs, name), epoch)
}

fn found(r: SingleStreamRecovery) -> Vec<WalBatch> {
    r.batches.into_iter().chain(r.past_stop).collect()
}

fn lsns_of(b: &[WalBatch]) -> Vec<u64> {
    b.iter().map(|b| b.lsn).collect()
}

// ---------------------------------------------------------------------------------------------
// The expectation
// ---------------------------------------------------------------------------------------------

/// What the logs say recovery has to report (module documentation).
pub(super) struct Expect {
    /// Some lost LSN is at or below the watermark of a visible batch: damage must be reported.
    pub required: bool,
    /// At least one logged batch above the checkpoint is no longer visible.
    pub any_lost: bool,
    /// A lost batch lay at or below the watermark of the logs before the damage.
    pub lost_acked: bool,
    /// Not required, but a batch hidden behind a zeroed or stale sector would have proved it.
    pub hidden: bool,
    /// The batches the scan sees in the damaged logs, by LSN.
    pub visible: Vec<WalBatch>,
    /// The batches of the logs before the crash, as `(lsn, prev_lsn, durable_lsn)`, and the
    /// checkpoint.
    pub logged: Vec<(u64, u64, u64)>,
    pub checkpoint: u64,
}

/// A primary-row record: `(table id, key without prefix, put?)`; a put holds its inline value,
/// `None` for a value in a blob.
type PrimaryRecord = (u16, Vec<u8>, Option<Option<Vec<u8>>>);

/// Primary-row records of a batch.
fn primary_records(b: &WalBatch) -> Vec<PrimaryRecord> {
    let mut out = Vec::new();
    for rec in &b.records {
        let (key, op) = match rec {
            WalRecord::PutInline { key, value } => (key, Some(Some(value.clone()))),
            WalRecord::PutBlobPtr { key, .. } => (key, Some(None)),
            WalRecord::Delete { key } => (key, None),
            _ => continue,
        };
        if key.len() >= 3 && key[2] == NS_PRIMARY_ROW && (key[0] != 0 || key[1] != 0) {
            out.push((u16::from_be_bytes([key[0], key[1]]), key[3..].to_vec(), op));
        }
    }
    out
}

fn has_catalog_record(b: &WalBatch) -> bool {
    b.records.iter().any(|r| match r {
        WalRecord::PutInline { key, .. }
        | WalRecord::PutBlobPtr { key, .. }
        | WalRecord::Delete { key } => key.len() >= 3 && key[..3] == [0, 0, b'c'],
        _ => false,
    })
}

fn has_marker(b: &WalBatch) -> bool {
    b.records.iter().any(|r| {
        matches!(
            r,
            WalRecord::TxBegin | WalRecord::TxContinue | WalRecord::TxCommit
        )
    })
}

/// `key07` -> 7.
fn key_index(key: &[u8]) -> Option<u8> {
    let s = std::str::from_utf8(key).ok()?;
    s.strip_prefix("key")?.parse().ok()
}

impl Harness<'_> {
    fn all_files(&self) -> Vec<(String, Vec<u8>)> {
        let st = self.fs.st();
        st.dir
            .iter()
            .map(|(n, &id)| (n.clone(), st.inodes[id].cache.clone()))
            .collect()
    }

    fn boot_state(&self) -> Option<crate::meta::BootMetaState> {
        let fs = scratch_fs(self.all_files());
        let meta = crate::meta::BootMetaManager::open_or_recover_deferred_sweep(fs).ok()?;
        Some(meta.state().clone())
    }

    /// Computes the expectation from the damaged logs (now in the filesystem) and the logs as
    /// they were before the crash. `None` when a log or the catalog is not there to compare.
    fn media_expect(&self, logged: &[(String, Vec<u8>)]) -> Result<Option<Expect>, String> {
        let Some(m) = self.boot_state() else {
            return Ok(None);
        };
        let now = self.all_files();
        let (mut before, mut visible, mut alt) = (Vec::new(), Vec::new(), Vec::new());
        let io = |what: &str, e: io::Error| self.fail(format!("oracle: {what}: {e}"));
        for (name, epoch) in [
            (WAL_FILES[0], m.sync_wal_epoch),
            (WAL_FILES[1], m.buf_wal_epoch),
        ] {
            let (Some((_, lg)), Some((_, cur))) = (
                logged.iter().find(|(n, _)| n == name),
                now.iter().find(|(n, _)| n == name),
            ) else {
                return Ok(None);
            };
            // The engine lengthens a log file that is shorter than configured with zeros before
            // it scans it, which also completes a sector that was only partly persisted.
            let size = [self.cfg.sync_wal_size_bytes, self.cfg.buf_wal_size_bytes]
                [(name == WAL_FILES[1]) as usize] as usize;
            let (mut lg, mut cur) = (lg.clone(), cur.clone());
            for img in [&mut lg, &mut cur] {
                if img.len() < size {
                    img.resize(size, 0);
                }
            }
            let (lg, cur) = (&lg, &cur);
            before.extend(found(
                scan_img(name, lg.clone(), epoch).map_err(|e| io("scan", e))?,
            ));
            // The scan must stop where the oracle says the log ends: what lies at or past that
            // sector must not change what it returns.
            let end = scan_end(cur, epoch);
            let full = scan_img(name, cur.clone(), epoch).map_err(|e| io("scan", e))?;
            let head =
                scan_img(name, cur[..end * SECT].to_vec(), epoch).map_err(|e| io("scan", e))?;
            if lsns_of(&full.batches) != lsns_of(&head.batches)
                || lsns_of(&full.past_stop) != lsns_of(&head.past_stop)
            {
                return Err(self.fail(format!(
                    "{name}: the scan read past the end of the log (sector {end}): \
                     {:?}/{:?} against {:?}/{:?} without the rest",
                    lsns_of(&full.batches),
                    lsns_of(&full.past_stop),
                    lsns_of(&head.batches),
                    lsns_of(&head.past_stop),
                )));
            }
            visible.extend(found(full));
            // What a scan that did not stop at the ordinary end would see: end markers are
            // replaced by garbage. Only used to tell "hidden" from "unprovable" in the counters.
            let mut open = cur.clone();
            for i in end..open.len() / SECT {
                if classify(&open[i * SECT..(i + 1) * SECT], epoch) == Sec::End {
                    open[i * SECT..(i + 1) * SECT].fill(0xFF);
                }
            }
            alt.extend(found(
                scan_img(name, open, epoch).map_err(|e| io("scan", e))?,
            ));
        }
        visible.sort_by_key(|b| b.lsn);
        let seen: BTreeSet<u64> = visible.iter().map(|b| b.lsn).collect();
        let lost: Vec<u64> = before
            .iter()
            .map(|b| b.lsn)
            .filter(|&l| l > m.checkpoint_lsn && !seen.contains(&l))
            .collect();
        let floor = |b: &[WalBatch]| b.iter().map(|b| b.durable_lsn).max().unwrap_or(0);
        let (floor_vis, floor_alt) = (floor(&visible), floor(&alt));
        let floor_before = before
            .iter()
            .filter(|b| b.lsn > m.checkpoint_lsn)
            .map(|b| b.durable_lsn)
            .max()
            .unwrap_or(0);
        let required = lost.iter().any(|&l| l <= floor_vis);
        Ok(Some(Expect {
            required,
            any_lost: !lost.is_empty(),
            lost_acked: lost.iter().any(|&l| l <= floor_before),
            hidden: !required && lost.iter().any(|&l| l <= floor_alt),
            logged: {
                let mut v: Vec<_> = before
                    .iter()
                    .map(|b| (b.lsn, b.prev_lsn, b.durable_lsn))
                    .collect();
                v.sort();
                v
            },
            checkpoint: m.checkpoint_lsn,
            visible,
        }))
    }

    // -----------------------------------------------------------------------------------------
    // Damage
    // -----------------------------------------------------------------------------------------

    /// Corrupts the synced logs in the filesystem. Returns whether anything changed.
    fn apply_damage(&mut self, rng: &mut Rng, m: &crate::meta::BootMetaState) -> bool {
        let streams = match rng.below(100) {
            0..=44 => vec![0],
            45..=84 => vec![1],
            _ => vec![0, 1],
        };
        let mut changed = false;
        for s in streams {
            let name = WAL_FILES[s];
            let epoch = [m.sync_wal_epoch, m.buf_wal_epoch][s];
            let Some(mut img) = self
                .all_files()
                .into_iter()
                .find(|(n, _)| n == name)
                .map(|f| f.1)
            else {
                continue;
            };
            let kinds = 1 + rng.chance(1, 4) as u32;
            for _ in 0..kinds {
                let live = live_sectors(&img, epoch);
                if live.is_empty() {
                    break;
                }
                let at = live[rng.below(live.len() as u64) as usize];
                let cov = &self.cov.media;
                match rng.below(100) {
                    0..=34 => {
                        for _ in 0..=rng.below(2) {
                            let sec = live[rng.below(live.len() as u64) as usize];
                            let off = sec * SECT + rng.below(SECT as u64) as usize;
                            img[off] ^= 1 << rng.below(8);
                        }
                        Coverage::bump(&cov.flips);
                        self.log(format!("damage: bit flips in {name}"));
                    }
                    35..=59 => {
                        let n = 1 + rng.below(3) as usize;
                        let end = (at + n).min(img.len() / SECT);
                        img[at * SECT..end * SECT].fill(0);
                        Coverage::bump(&cov.zeroed);
                        self.log(format!("damage: zeroed {name} sectors {at}..{end}"));
                    }
                    60..=84 => {
                        img.truncate(at * SECT);
                        Coverage::bump(&cov.truncated);
                        self.log(format!("damage: {name} truncated to {at} sectors"));
                    }
                    _ => {
                        // An intact sector of the epoch before: the scan reads it as the end.
                        if epoch == 0 {
                            continue;
                        }
                        let sec = &mut img[at * SECT..(at + 1) * SECT];
                        sec[4..12].copy_from_slice(&(epoch - 1).to_le_bytes());
                        let crc = crc32c(&sec[4..]);
                        sec[0..4].copy_from_slice(&crc.to_le_bytes());
                        Coverage::bump(&cov.stale);
                        self.log(format!(
                            "damage: {name} sector {at} replaced by a stale one"
                        ));
                    }
                }
                changed = true;
            }
            let new = img;
            damage_synced(&self.fs, name, |old| old.clone_from(&new));
        }
        changed
    }

    /// Damages the logs after the crash (when the seed says so) and checks what recovery makes
    /// of them on copies. `logged` are the logs before the crash. Returns whether the real
    /// directory is now damaged; the next open of it must then report what the copies did.
    pub(super) fn damage_wal(
        &mut self,
        rng: &mut Rng,
        logged: &[(String, Vec<u8>)],
    ) -> Result<bool, String> {
        if !rng.chance(2, 3) {
            return Ok(false);
        }
        let Some(m) = self.boot_state() else {
            return Ok(false);
        };
        if !self.apply_damage(rng, &m) {
            return Ok(false);
        }
        Coverage::bump(&self.cov.media.damaged);
        let Some(x) = self.media_expect(logged)? else {
            return Ok(false);
        };
        let damage = self.recovery_on_copies(&x)?;
        self.damage_pending = true;
        self.relaxed = x.any_lost;
        self.expect_damage = Some(damage);
        Ok(true)
    }

    /// Opens two copies of the damaged directory, one under `Fail` and one with salvage, and
    /// checks the report, the refusal and the salvage list against the expectation. Returns the
    /// damage the copies agree on.
    fn recovery_on_copies(&self, x: &Expect) -> Result<Option<WalDamage>, String> {
        let files = self.all_files();
        let cov = &self.cov.media;

        let (salvaged_fs, copy_fs) = (scratch_fs(files.clone()), scratch_fs(files));
        let (eng, txs) =
            StorageEngine::open_with_salvage(Arc::clone(&salvaged_fs), self.cfg.clone()).map_err(
                |e| self.fail(format!("open_with_salvage of the damaged logs failed: {e}")),
            )?;
        let report = eng.last_wal_recovery().clone();
        let damage = report.damage.clone();
        if damage.is_some() != x.required {
            return Err(self.fail(format!(
                "recovery {} damage, but the logs {} it: a lost batch is {} the watermark of a \
                 visible batch\n  report: {report:?}\n  checkpoint {}, visible (lsn, prev, \
                 durable): {:?}\n  logged (lsn, prev, durable): {:?}",
                if damage.is_some() {
                    "reports"
                } else {
                    "does not report"
                },
                if x.required { "prove" } else { "do not prove" },
                if x.required { "at or below" } else { "above" },
                x.checkpoint,
                x.visible
                    .iter()
                    .map(|b| (b.lsn, b.prev_lsn, b.durable_lsn))
                    .collect::<Vec<_>>(),
                x.logged,
            )));
        }
        if x.lost_acked {
            Coverage::bump(&cov.lost_acked);
        }
        if x.any_lost && damage.is_none() {
            Coverage::bump(&cov.lost_unreported);
            Coverage::bump(if x.hidden {
                &cov.hidden
            } else {
                &cov.unprovable
            });
        }

        // `Fail`: refuses exactly when damage is reported, and then touches nothing.
        let before = dir_state(&copy_fs);
        let mut cfg = self.cfg.clone();
        cfg.wal_damage_policy = WalDamagePolicy::Fail;
        match StorageEngine::open(Arc::clone(&copy_fs), cfg) {
            Ok(e) => {
                if damage.is_some() {
                    return Err(self.fail(format!(
                        "Fail opened a WAL whose damage Truncate reports: {damage:?}"
                    )));
                }
                if e.last_wal_recovery().damage.is_some() {
                    return Err(self.fail("Fail opened and reports damage".into()));
                }
            }
            Err(err) => {
                let inner = err.get_ref().and_then(|i| i.downcast_ref::<WalDamage>());
                if err.kind() != io::ErrorKind::InvalidData || inner != damage.as_ref() {
                    return Err(self.fail(format!(
                        "Fail refused with {err} ({:?}); Truncate reports {damage:?}",
                        err.kind()
                    )));
                }
                let after = dir_state(&copy_fs);
                let changed: Vec<&String> = after
                    .keys()
                    .chain(before.keys())
                    .filter(|n| after.get(*n) != before.get(*n))
                    .collect();
                // KNOWN ENGINE BUG (`fail_policy_leaves_a_truncated_wal_file_untouched`): the open
                // lengthens a log file that is shorter than configured before it looks at the
                // damage. Nothing else may change, and what was there stays.
                let only_growth = changed.iter().all(|n| {
                    WAL_FILES.contains(&n.as_str())
                        && match (before.get(*n), after.get(*n)) {
                            (Some(b), Some(a)) => {
                                a.0.len() > b.0.len()
                                    && a.0[..b.0.len()] == b.0[..]
                                    && a.0[b.0.len()..].iter().all(|&x| x == 0)
                                    && a.1.len() >= b.1.len()
                                    && a.1[..b.1.len()] == b.1[..]
                            }
                            _ => false,
                        }
                });
                if !changed.is_empty() && !only_growth {
                    return Err(self.fail(format!(
                        "a failed open under Fail changed a file (boot_meta.db included): \
                         {changed:?}"
                    )));
                }
                if !changed.is_empty() {
                    Coverage::bump(&cov.refused_extended);
                }
                Coverage::bump(&cov.refused);
            }
        }
        self.check_salvage(&eng, &txs, &report, x)?;
        Ok(damage)
    }

    /// Salvage oracle: every returned transaction is a whole transaction that the workload
    /// committed with those operations, lies above the replayed prefix, and none that the logs
    /// still show intact is missing.
    fn check_salvage(
        &self,
        eng: &Eng,
        txs: &[SalvagedTx],
        report: &WalRecoveryReport,
        x: &Expect,
    ) -> Result<(), String> {
        let cov = &self.cov.media;
        Coverage::bump(&cov.salvage_checks);
        if report.damage.is_none() {
            if !txs.is_empty() {
                return Err(self.fail(format!("salvage without damage returned {txs:?}")));
            }
            return Ok(());
        }
        let name = |id: u16| format!("t{id}");
        let mut last = report.last_replayed_lsn;
        for tx in txs {
            Coverage::bump(&cov.salvaged_txs);
            if tx.lsn <= last {
                return Err(self.fail(format!(
                    "salvaged transaction {} is not above the replayed prefix / not in LSN order \
                     (last {last})",
                    tx.lsn
                )));
            }
            last = tx.lsn;
            let Some(batch) = x.visible.iter().find(|b| b.lsn == tx.lsn) else {
                return Err(self.fail(format!(
                    "salvaged transaction {} is in no intact batch of the logs",
                    tx.lsn
                )));
            };
            if has_marker(batch) {
                return Err(self.fail(format!(
                    "salvaged transaction {} is a chunk of a multi-batch transaction",
                    tx.lsn
                )));
            }
            // The operations are the primary-row records of the batch, in order.
            let recs = primary_records(batch);
            if recs.len() != tx.ops.len() {
                return Err(self.fail(format!(
                    "salvaged transaction {} has {} operations, the batch {}",
                    tx.lsn,
                    tx.ops.len(),
                    recs.len()
                )));
            }
            // Operations whose LSNs cover the batch (an operation that logged nothing shares its
            // first LSN with the next one).
            let recs_of: Vec<&OpRec> = self
                .oplog
                .iter()
                .filter(|o| o.lo <= tx.lsn && tx.lsn < o.hi)
                .collect();
            for ((id, key, put), op) in recs.iter().zip(&tx.ops) {
                let (t, k) = (*id, key_index(key));
                let same = match (op, put) {
                    (
                        SalvagedOp::Put {
                            table,
                            key: k2,
                            value,
                        },
                        Some(inline),
                    ) => {
                        *table == name(t) && k2 == key && inline.as_ref().is_none_or(|v| v == value)
                    }
                    (SalvagedOp::Delete { table, key: k2 }, None) => *table == name(t) && k2 == key,
                    _ => false,
                };
                if !same {
                    return Err(self.fail(format!(
                        "salvaged operation {op:?} of transaction {} differs from the logged \
                         record of table {t}",
                        tx.lsn
                    )));
                }
                // Against what the workload wrote: some operation that covers the batch wrote
                // this value (or deleted this row; the capped tables also evict on their own).
                fn wrote(r: &OpRec, t: u16, k: Option<u8>) -> Option<&Vec<Option<Vec<u8>>>> {
                    k.and_then(|k| r.touched.get(&(t, k)))
                }
                let ok = match op {
                    SalvagedOp::Put { value, .. } => recs_of.iter().any(|r| {
                        wrote(r, t, k).is_some_and(|h| h.iter().any(|w| w.as_ref() == Some(value)))
                    }),
                    SalvagedOp::Delete { .. } => {
                        is_capped(t)
                            || recs_of
                                .iter()
                                .any(|r| wrote(r, t, k).is_some_and(|h| h.contains(&None)))
                    }
                };
                if !ok {
                    return Err(self.fail(format!(
                        "salvaged transaction {} was not committed like that by the workload: \
                         {op:?} (operations covering it: {})",
                        tx.lsn,
                        recs_of.len()
                    )));
                }
            }
            // The transaction is whole: the operation that wrote it has every row it put here.
            let puts_here = recs.iter().any(|(_, _, put)| put.is_some());
            let whole = recs_of.iter().any(|r| {
                r.touched.iter().all(|((t, k), h)| {
                    h.last().is_some_and(|v| v.is_none())
                        || recs
                            .iter()
                            .any(|(rt, key, _)| rt == t && key_index(key) == Some(*k))
                })
            });
            if puts_here && !whole {
                return Err(self.fail(format!(
                    "salvaged transaction {} lacks rows that the operation put: {:?}",
                    tx.lsn,
                    recs_of
                        .iter()
                        .map(|r| r.touched.keys().collect::<Vec<_>>())
                        .collect::<Vec<_>>()
                )));
            }
        }
        // Nothing that the logs show intact, after the prefix, with only known tables is missing.
        let returned: BTreeSet<u64> = txs.iter().map(|t| t.lsn).collect();
        for b in x
            .visible
            .iter()
            .filter(|b| b.lsn > report.last_replayed_lsn)
        {
            Coverage::bump(&cov.salvage_visible_discarded);
            let recs = primary_records(b);
            let known = recs
                .iter()
                .all(|(t, ..)| eng.active_vtab_ids.contains_key(t));
            if recs.is_empty() || has_marker(b) || has_catalog_record(b) || !known {
                continue;
            }
            if !returned.contains(&b.lsn) {
                return Err(self.fail(format!(
                    "batch {} is intact, lies after the replayed prefix ({}) and holds only \
                     known tables, but salvage did not return it: {:?}",
                    b.lsn, report.last_replayed_lsn, txs
                )));
            }
        }
        Ok(())
    }

    // -----------------------------------------------------------------------------------------
    // Operation log (salvage oracle) and group commit
    // -----------------------------------------------------------------------------------------

    /// Remembers an operation that allocated the LSNs `[lo, hi)`. A failed one may have logged a
    /// batch and given its LSNs back: callers pass the LSN counter before the commit as `hi`
    /// too, and the range always covers one LSN past it.
    pub(super) fn note_op(&mut self, lo: u64, hi: u64, touched: Touched) {
        if self.mode.media {
            self.oplog.push(OpRec {
                lo,
                hi: hi.max(lo) + 1,
                touched,
            });
        }
    }

    /// Acknowledges the group commits that the engine has published since: they left
    /// `pending_group` because a flush that covers them returned.
    pub(super) fn settle_tickets(&mut self, e: &Eng) {
        if self.fs.cut_tripped() {
            return;
        }
        let pending = |lsn: u64| e.pending_group.iter().any(|(l, _)| *l == lsn);
        let mut acked = None;
        self.tickets.retain(|t| {
            if pending(t.lsn) {
                return true;
            }
            acked = acked.max(Some(t.state));
            false
        });
        if let Some(idx) = acked {
            self.acked_any = self.acked_any.max(idx);
            self.acked_sync = self.acked_sync.max(idx);
            self.log(format!("group commits acknowledged up to state {idx}"));
        }
    }

    pub(super) fn run_group_segment(&mut self, e: &mut Eng, nops: usize) -> Result<(), String> {
        if !self.ensure_tables(e)? {
            return Ok(());
        }
        for _ in 0..nops {
            let r = self.rng.below(100);
            let go = if r < 40 {
                self.op_group_tx(e)?
            } else if r < 58 {
                self.op_flush_ticket(e)?
            } else if r < 70 {
                self.op_put(e)?
            } else if r < 76 {
                self.op_delete(e)?
            } else if r < 84 {
                self.op_tx(e)?
            } else if r < 88 {
                self.op_maintenance(e, 0)?
            } else if r < 91 {
                self.op_maintenance(e, 1)?
            } else {
                self.op_group_tx(e)?
            };
            self.settle_tickets(e);
            if !go {
                return Ok(());
            }
            // Readers do not see group commits before their flush, the model does.
            if e.pending_group.is_empty() && self.rng.chance(1, 3) {
                self.live_check(e)?;
            }
        }
        if e.pending_group.is_empty() {
            self.live_check(e)?;
        }
        Ok(())
    }

    /// One transaction of one to three statements on a connection of its own, committed through
    /// the group commit: appended now, flushed later by [`Self::op_flush_ticket`] or by whatever
    /// the engine does next.
    fn op_group_tx(&mut self, e: &mut Eng) -> Result<bool, String> {
        let conn = 1 + self.rng.below(3) as usize;
        let lsn0 = e.next_lsn();
        let r = e.begin_transaction_for_conn(conn);
        if self.io_ok(r, "begin_transaction_for_conn")?.is_none() {
            return Ok(false);
        }
        let mut work = self.last().clone();
        let mut touched = Touched::new();
        for _ in 0..1 + self.rng.below(3) {
            let t = TABLES[self.rng.below(2) as usize];
            let k = self.rng.below(KEYS as u64) as u8;
            if self.rng.chance(4, 5) {
                let v = make_value(&mut self.rng, t, k);
                let res = e.put_row(
                    t,
                    &key_bytes(k),
                    &v,
                    &put_keys(t, &v),
                    WriteDurability::SyncTransactional,
                );
                self.log(format!(
                    "  group tx (conn {conn}) put t{t}/key{k:02} {}",
                    short(&v)
                ));
                if self.io_ok(res, "put_row in group transaction")?.is_none() {
                    return self.abort_tx(e);
                }
                work.insert((t, k), v.clone());
                touched.entry((t, k)).or_default().push(Some(v));
            } else {
                let res = e.delete_row(t, &key_bytes(k), WriteDurability::SyncTransactional);
                self.log(format!("  group tx (conn {conn}) delete t{t}/key{k:02}"));
                if self
                    .io_ok(res, "delete_row in group transaction")?
                    .is_none()
                {
                    return self.abort_tx(e);
                }
                work.remove(&(t, k));
                touched.entry((t, k)).or_default().push(None);
            }
        }
        let sync = WriteDurability::SyncTransactional;
        self.log(format!("commit through group commit (conn {conn})"));
        if let Err(err) = e.prepare_commit_transaction(sync) {
            if !self.fs.fault_happened() {
                return Err(self.fail(format!("prepare_commit_transaction failed: {err}")));
            }
            let dead = self.dead(e);
            let _ = e.rollback_transaction();
            if dead {
                self.states.push(work);
            }
            return Ok(!dead);
        }
        let logged = e.pending_tx_record_count > 0;
        let before_commit = e.next_lsn();
        let step = e.begin_commit_for_conn(conn);
        let touched_rows: Vec<(u16, u8)> = touched.keys().copied().collect();
        self.note_op(lsn0, e.next_lsn().max(before_commit), touched);
        let alive = !e.is_poisoned();
        match step {
            Ok(CommitStep::AwaitingGroup { lsn, file, gate }) => {
                self.states.push(work);
                let state = self.states.len() - 1;
                self.log(format!(
                    "  appended as lsn {lsn}, state {state}, flush pending"
                ));
                Coverage::bump(&self.cov.media.group_tickets);
                let rows = touched_rows;
                self.tickets.push(Ticket {
                    lsn,
                    rows,
                    state,
                    file,
                    gate,
                });
                Ok(!self.fs.cut_tripped())
            }
            Ok(CommitStep::AwaitingSync(file)) => {
                Coverage::bump(&self.cov.media.group_sync_path);
                let flushed = file.sync_barrier();
                let r = e.finish_commit_for_conn(flushed);
                let alive = !e.is_poisoned();
                self.record_commit(r, work, true, logged, alive, "group tx commit (own flush)")
            }
            Ok(CommitStep::Done) => {
                self.record_commit(Ok(()), work, true, logged, alive, "group tx commit")
            }
            Err(err) => {
                if !self.fs.fault_happened() {
                    return Err(self.fail(format!("begin_commit_for_conn failed: {err}")));
                }
                self.record_commit(Err(err), work, true, logged, alive, "group tx begin_commit")
            }
        }
    }

    /// Flushes every waiting group commit, as the leader of the flush.
    pub(super) fn drain_group(&mut self, e: &mut Eng) -> Result<bool, String> {
        while let Some(t) = self.tickets.first() {
            let (lsn, file, gate) = (t.lsn, Arc::clone(&t.file), Arc::clone(&t.gate));
            let go = self.flush_ticket(e, lsn, &file, &gate)?;
            self.settle_tickets(e);
            if !go {
                return Ok(false);
            }
            if self.tickets.first().is_some_and(|t| t.lsn == lsn) {
                // Not published: the engine is dead (a fault), nothing more to flush.
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// KNOWN ENGINE BUG (`delete_after_a_pending_group_commit_removes_its_secondary_entries`): a
    /// write outside a transaction reads the committed rows and misses the one that a pending
    /// group commit wrote. Until that is fixed, an autocommit operation on a row that a group
    /// commit still holds waits for its flush.
    pub(super) fn avoid_pending_row(
        &mut self,
        e: &mut Eng,
        table: u16,
        key: u8,
    ) -> Result<bool, String> {
        if self.mode.group && self.tickets.iter().any(|t| t.rows.contains(&(table, key))) {
            return self.drain_group(e);
        }
        Ok(true)
    }

    /// Picks a waiting group commit and finds it durable or runs the flush it leads.
    fn op_flush_ticket(&mut self, e: &mut Eng) -> Result<bool, String> {
        if self.tickets.is_empty() {
            return Ok(true);
        }
        let i = self.rng.below(self.tickets.len() as u64) as usize;
        let (lsn, file, gate) = {
            let t = &self.tickets[i];
            (t.lsn, Arc::clone(&t.file), Arc::clone(&t.gate))
        };
        self.flush_ticket(e, lsn, &file, &gate)
    }

    fn flush_ticket(
        &mut self,
        e: &mut Eng,
        lsn: u64,
        file: &Arc<CrashFile>,
        gate: &Arc<tx::CommitGate>,
    ) -> Result<bool, String> {
        match gate.join_group(lsn) {
            Ok(GroupRole::Durable) => {
                Coverage::bump(&self.cov.media.group_shared);
                self.log(format!(
                    "group commit {lsn} was made durable by another flush"
                ));
                Ok(true)
            }
            Ok(GroupRole::Lead(target)) => {
                Coverage::bump(&self.cov.media.group_leads);
                self.log(format!("leading the flush of group commits up to {target}"));
                let flushed = file.sync_barrier();
                let r = e.finish_group_flush(target, flushed);
                self.io_ok(r, "finish_group_flush")?;
                Ok(!self.dead(e))
            }
            Err(err) => {
                if !self.fs.fault_happened() {
                    return Err(self.fail(format!("join_group failed without a fault: {err}")));
                }
                Ok(false)
            }
        }
    }
}
