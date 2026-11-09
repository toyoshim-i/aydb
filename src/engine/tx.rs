//! Transaction & savepoint state machine, undo logs, and cross-connection snapshot isolation.

use std::collections::{BTreeMap, HashMap};
use std::io;
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::time::Duration;

use crate::index::IndexValue;
use crate::lru::CappedTableState;
use crate::memtable::MemEntry;
use crate::pal::{PlatformFile, PlatformFs};
use crate::wal::{WalBatch, WalRecord};

use super::reader::CxRef;
use super::{EngineReader, StorageEngine, WriteDurability};

/// Transactions larger than this are split into several WAL batches.
const MAX_WAL_CHUNK_BYTES: usize = 512 * 1024;

/// Upper bound on one condition-variable wait. Callers re-check their condition afterwards.
const COMMIT_GATE_WAIT: Duration = Duration::from_millis(50);

/// A failed group flush as reported to every commit of the group.
fn group_flush_error(message: &str) -> io::Error {
    io::Error::other(format!("group commit flush failed: {message}"))
}

#[derive(Default)]
struct GateState {
    /// Number of transactions that ended, used by writers waiting for their turn.
    tx_ends: u64,
    /// Highest LSN written to `wal_sync.db`.
    appended: u64,
    /// Every LSN below this value is durable.
    durable_end: u64,
    /// True while one commit is running the device flush for the group.
    leader_active: bool,
    failed: Option<String>,
}

/// Coordinates writers that wait for a transaction to end and commits that share a device flush.
#[derive(Default)]
pub(crate) struct CommitGate {
    state: Mutex<GateState>,
    changed: Condvar,
}

/// What a commit waiting for durability has to do next.
pub(crate) enum GroupRole {
    /// The commit's WAL batch is durable and published.
    Durable,
    /// The caller runs the device flush, which covers every batch up to this LSN.
    Lead(u64),
}

impl CommitGate {
    fn lock(&self) -> std::sync::MutexGuard<'_, GateState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn tx_ended(&self) {
        self.lock().tx_ends += 1;
        self.changed.notify_all();
    }

    fn note_appended(&self, lsn: u64) {
        let mut state = self.lock();
        state.appended = state.appended.max(lsn);
    }

    fn mark_durable(&self, lsn: u64, ends_leadership: bool) {
        let mut state = self.lock();
        state.durable_end = state.durable_end.max(lsn + 1);
        if ends_leadership {
            state.leader_active = false;
        }
        drop(state);
        self.changed.notify_all();
    }

    fn fail(&self, error: &io::Error) {
        let mut state = self.lock();
        state.failed = Some(error.to_string());
        state.leader_active = false;
        drop(state);
        self.changed.notify_all();
    }

    /// Count of ended transactions. Read it under the engine lock before waiting, so that a
    /// transaction ending right after the lock is released is not missed.
    pub(crate) fn tx_end_count(&self) -> u64 {
        self.lock().tx_ends
    }

    /// Blocks until a transaction ended after `seen` or `timeout` passed.
    pub(crate) fn wait_tx_end(&self, seen: u64, timeout: Duration) {
        let guard = self.lock();
        let _ = self
            .changed
            .wait_timeout_while(guard, timeout.min(COMMIT_GATE_WAIT), |s| s.tx_ends == seen);
    }

    /// Waits until the batch at `lsn` is durable or this caller has to run the device flush.
    pub(crate) fn join_group(&self, lsn: u64) -> io::Result<GroupRole> {
        let mut state = self.lock();
        loop {
            if let Some(message) = &state.failed {
                return Err(group_flush_error(message));
            }
            if state.durable_end > lsn {
                return Ok(GroupRole::Durable);
            }
            if !state.leader_active {
                state.leader_active = true;
                return Ok(GroupRole::Lead(state.appended));
            }
            state = self
                .changed
                .wait_timeout(state, COMMIT_GATE_WAIT)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
    }

    /// Records that the flush leader could not report a result.
    pub(crate) fn abandon_flush(&self, error: &io::Error) {
        self.fail(error);
    }
}

/// Outcome of [`StorageEngine::begin_commit_for_conn`].
pub(crate) enum CommitStep<F: PlatformFile> {
    /// The transaction is fully committed.
    Done,
    /// The WAL write is issued and the transaction stays active. Call `sync_barrier` on the file
    /// without holding the engine lock, then pass the result to
    /// [`StorageEngine::finish_commit_for_conn`].
    AwaitingSync(Arc<F>),
    /// The WAL write is issued and the next writer may start. Loop on
    /// [`CommitGate::join_group`] until the batch is durable.
    AwaitingGroup {
        lsn: u64,
        file: Arc<F>,
        gate: Arc<CommitGate>,
    },
}

#[derive(Clone, Default)]
pub(crate) struct TxMetaSnapshot {
    pub(super) next_lsn: u64,
    pub(super) std_blobs: BTreeMap<u64, (u64, u64)>,
    pub(super) lru_blobs: BTreeMap<(u16, u64), (u64, u64)>,
    pub(super) capped_tables: HashMap<u16, CappedTableState>,
    pub(super) active_vtab_ids: HashMap<u16, Arc<str>>,
    pub(super) table_has_row_meta: BTreeMap<u16, bool>,
    pub(super) dropped_tables_len: usize,
    pub(super) quotas_modified_in_tx: bool,
}

/// Private, ordered write set of the active explicit transaction.
///
/// While `tx_depth > 0` every mutation lands here instead of in the shared `MemTable`, so the
/// `MemTable` only ever holds committed data and non-owner readers never need to hide anything.
/// Owner reads treat the overlay as the highest-precedence source (overlay > memtable > segments).
///
/// LSN note: overlay entries carry the `next_lsn` value of their mutation time, which is greater
/// than every committed LSN. Merged scans nevertheless do not compare overlay LSNs; precedence is
/// by source (see `MemView`), so correctness does not depend on that numeric property.
#[derive(Default)]
pub(crate) struct TxOverlay {
    pub(super) map: BTreeMap<Vec<u8>, MemEntry>,
    /// Per-key undo log (`key`, previous overlay entry), recorded only while at least one
    /// savepoint exists. `rollback_to_savepoint` pops it back to the marker's length.
    pub(super) undo: Vec<(Vec<u8>, Option<MemEntry>)>,
    /// Cached bounds of keys present in `map` for O(1) membership rejection.
    pub(super) min_key: Option<Vec<u8>>,
    pub(super) max_key: Option<Vec<u8>>,
    /// Fast 1024-bit Bloom filter over keys currently in `map`.
    pub(super) filter: [u64; 16],
}

impl TxOverlay {
    #[inline]
    pub(super) fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub(super) fn clear(&mut self) {
        self.map.clear();
        self.undo.clear();
        self.min_key = None;
        self.max_key = None;
        self.filter = [0; 16];
    }

    #[inline]
    pub(super) fn may_contain_key(&self, key: &[u8]) -> bool {
        if self.map.is_empty() {
            return false;
        }
        if let Some(min) = &self.min_key {
            if key < min.as_slice() {
                return false;
            }
        }
        if let Some(max) = &self.max_key {
            if key > max.as_slice() {
                return false;
            }
        }
        let h1 = crate::util::crc32c::crc32c_append(0, key);
        if (self.filter[(h1 as usize & 1023) / 64] & (1 << (h1 & 63))) == 0 {
            return false;
        }
        let h2 = crate::util::crc32c::crc32c_append(0x9E37_79B9, key);
        if (self.filter[(h2 as usize & 1023) / 64] & (1 << (h2 & 63))) == 0 {
            return false;
        }
        true
    }

    #[inline]
    pub(super) fn overlaps_range(
        &self,
        lo: std::ops::Bound<&[u8]>,
        hi: std::ops::Bound<&[u8]>,
    ) -> bool {
        if self.map.is_empty() {
            return false;
        }
        if let Some(max) = &self.max_key {
            match lo {
                std::ops::Bound::Included(lo_k) if lo_k > max.as_slice() => return false,
                std::ops::Bound::Excluded(lo_k) if lo_k >= max.as_slice() => return false,
                _ => {}
            }
        }
        if let Some(min) = &self.min_key {
            match hi {
                std::ops::Bound::Included(hi_k) if hi_k < min.as_slice() => return false,
                std::ops::Bound::Excluded(hi_k) if hi_k <= min.as_slice() => return false,
                _ => {}
            }
        }
        true
    }

    fn note_key(&mut self, key: &[u8]) {
        match &mut self.min_key {
            Some(min) if key < min.as_slice() => *min = key.to_vec(),
            None => self.min_key = Some(key.to_vec()),
            _ => {}
        }
        match &mut self.max_key {
            Some(max) if key > max.as_slice() => *max = key.to_vec(),
            None => self.max_key = Some(key.to_vec()),
            _ => {}
        }
        let h1 = crate::util::crc32c::crc32c_append(0, key);
        let h2 = crate::util::crc32c::crc32c_append(0x9E37_79B9, key);
        self.filter[(h1 as usize & 1023) / 64] |= 1 << (h1 & 63);
        self.filter[(h2 as usize & 1023) / 64] |= 1 << (h2 & 63);
    }

    /// Adds `entries`, which belong to earlier transactions, without an undo record.
    fn seed(&mut self, entries: &BTreeMap<Vec<u8>, MemEntry>) {
        for (key, entry) in entries {
            self.note_key(key);
            self.map.insert(key.clone(), entry.clone());
        }
    }

    pub(super) fn apply(&mut self, lsn: u64, rec: WalRecord, record_undo: bool) {
        let (key, value) = match rec {
            WalRecord::PutInline { key, value } => (key, IndexValue::Inline(value)),
            WalRecord::PutBlobPtr { key, blob_ptr } => (key, IndexValue::Blob(blob_ptr)),
            WalRecord::Delete { key } => (key, IndexValue::Tombstone),
            WalRecord::TxBegin | WalRecord::TxCommit => return,
        };
        self.note_key(&key);

        if record_undo {
            let undo_key = key.clone();
            let prev = self.map.insert(key, MemEntry { lsn, value });
            self.undo.push((undo_key, prev));
        } else {
            self.map.insert(key, MemEntry { lsn, value });
        }
    }

    /// Pops undo entries until `undo.len() == target_len`, restoring the overlay exactly.
    pub(super) fn rollback_to(&mut self, target_len: usize) {
        let mut popped = false;
        while self.undo.len() > target_len {
            popped = true;
            let (key, prev) = self.undo.pop().unwrap();
            match prev {
                Some(e) => {
                    self.map.insert(key, e);
                }
                None => {
                    self.map.remove(&key);
                }
            }
        }
        if popped {
            if self.map.is_empty() {
                self.min_key = None;
                self.max_key = None;
                self.filter = [0; 16];
            } else {
                self.min_key = self.map.first_key_value().map(|(k, _)| k.clone());
                self.max_key = self.map.last_key_value().map(|(k, _)| k.clone());
                self.filter = [0; 16];
                for k in self.map.keys() {
                    let h1 = crate::util::crc32c::crc32c_append(0, k);
                    let h2 = crate::util::crc32c::crc32c_append(0x9E37_79B9, k);
                    self.filter[(h1 as usize & 1023) / 64] |= 1 << (h1 & 63);
                    self.filter[(h2 as usize & 1023) / 64] |= 1 << (h2 & 63);
                }
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct SavepointMarker {
    pub(super) sp_id: i32,
    pub(super) buf_offset: usize,
    pub(super) record_count: u32,
    pub(super) pending_tx_bytes: usize,
    pub(super) pending_tx_sync: bool,
    pub(super) implicit_tx: bool,
    pub(super) undo_len: usize,
    pub(super) meta_snapshot: TxMetaSnapshot,
}

impl<FS: PlatformFs> StorageEngine<FS> {
    /// Returns `true` if an explicit or savepoint-backed transaction is currently active.
    #[inline]
    pub fn in_active_transaction(&self) -> bool {
        self.tx_depth > 0
    }

    /// Begins an explicit batch transaction (`BEGIN`). Subsequent mutations apply immediately to
    /// the private transaction overlay (for read-your-own-writes visibility) while coalescing their
    /// [`crate::wal::WalRecord`] entries into a single atomic [`WalBatch`] flushed on [`Self::commit_transaction`].
    pub fn begin_transaction(&mut self) {
        self.cleanup_read_snapshots();
        if self.tx_depth == 0 {
            self.tx_overlay.clear();
            self.dropped_tables_in_tx.clear();
            self.quotas_modified_in_tx = false;
            self.tx_start_meta = self.capture_tx_meta_snapshot();
            self.tx_owner_conn = None;
            self.deferred_flush_on_commit = false;
            self.deferred_compact_on_commit = false;
            self.tx_prepared = false;
            self.tx_preparing = false;
            self.pending_tx_bytes = 0;
            self.seed_overlay_from_pending();
        }
        self.tx_depth += 1;
    }

    /// Begins an explicit transaction owned by SQLite connection `conn_id`.
    /// Returns `io::ErrorKind::WouldBlock` (`SQLITE_BUSY`) if another connection already holds
    /// an active transaction on this `StorageEngine`.
    pub fn begin_transaction_for_conn(&mut self, conn_id: usize) -> io::Result<()> {
        self.cleanup_read_snapshots();
        self.check_not_poisoned()?;
        if self.tx_depth > 0 && self.tx_owner_conn != Some(conn_id) {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "database is locked by another connection's active transaction",
            ));
        }
        if self.tx_depth == 0 {
            self.tx_overlay.clear();
            self.dropped_tables_in_tx.clear();
            self.quotas_modified_in_tx = false;
            self.tx_start_meta = self.capture_tx_meta_snapshot();
            self.tx_owner_conn = Some(conn_id);
            self.deferred_flush_on_commit = false;
            self.deferred_compact_on_commit = false;
            self.tx_prepared = false;
            self.tx_preparing = false;
            self.pending_tx_bytes = 0;
            self.seed_overlay_from_pending();
        }
        self.tx_depth += 1;
        Ok(())
    }

    /// Checks whether `conn_id` is permitted to mutate this `StorageEngine`.
    /// Returns `io::ErrorKind::WouldBlock` (`SQLITE_BUSY`) if another connection holds an active transaction.
    #[inline]
    pub fn check_write_allowed_for_conn(&self, conn_id: usize) -> io::Result<()> {
        if self.tx_depth > 0 && self.tx_owner_conn != Some(conn_id) {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "database is locked by another connection's active transaction",
            ));
        }
        Ok(())
    }

    /// Executes a read closure `f` for `conn_id` with exclusive engine access.
    ///
    /// If `conn_id` owns the active transaction (or no transaction is active), `f` sees the
    /// transaction overlay on top of the `MemTable` (read-your-own-writes). Otherwise `f` sees
    /// the committed data only. The view is a cheap per-call selection: no engine state is
    /// modified and the cost is independent of the number of uncommitted mutations.
    ///
    /// Because access is exclusive, the closure may be re-run once after the engine flushed a
    /// buffered blob the reader needed (it must be free of side effects before its first read
    /// result), and `EagerWriteOnRead` promotions queued by the closure are applied afterwards
    /// (their errors are not reported). Prefer [`Self::reader_for_conn`] under a shared lock
    /// when [`Self::read_needs_exclusive`] is `false`. The closure must return read errors (not
    /// panic on them) so the retry can happen.
    pub fn with_committed_read_for_conn<F, R>(&mut self, conn_id: usize, mut f: F) -> R
    where
        F: FnMut(&EngineReader<'_, FS>) -> R,
    {
        let view = self.read_view_for_conn(conn_id);
        self.run_read(view, |eng, cx| {
            f(&EngineReader {
                eng,
                cx: CxRef::Borrowed(cx),
            })
        })
        .0
    }

    /// Prepares the active transaction for commit (`xSync` two-phase commit phase 1).
    /// Runs deferred capped-table quota evictions inside the undoable transaction context,
    /// flushes/syncs out-of-line blob segments, and pre-checks WAL capacity so any fallible I/O
    /// or space errors surface during `xSync` while `xRollback` can still cleanly undo all state.
    pub fn prepare_commit_transaction(&mut self, durability: WriteDurability) -> io::Result<()> {
        if self.tx_depth == 0 {
            return Ok(());
        }
        if durability == WriteDurability::SyncTransactional {
            self.pending_tx_sync = true;
        }
        let effective_durability = if self.pending_tx_sync {
            WriteDurability::SyncTransactional
        } else {
            WriteDurability::BufferedFast
        };

        // 1. Run capped-table quota evictions while `tx_depth > 0` so eviction mutations are
        //    recorded in `pending_tx_buf` and `tx_overlay` (fully undoable on `xRollback`!).
        self.tx_preparing = true;
        let capped_table_ids: Vec<u16> = self.capped_tables.keys().copied().collect();
        for tid in capped_table_ids {
            if let Err(e) = self.evict_until_within_quota(tid, effective_durability) {
                self.tx_preparing = false;
                return Err(e);
            }
        }
        self.tx_preparing = false;

        // 2. Flush/sync active blob writers (`Write-Blob-Before-WAL` & prefix durability).
        self.flush_active_blob_buffers()?;
        if effective_durability == WriteDurability::SyncTransactional {
            self.sync_active_blob_writers()?;
            if self.last_buf_append_lsn > self.last_buf_synced_lsn {
                self.make_buf_wal_durable(true)?;
            }
        }

        // 3. Pre-verify/grow WAL capacity if this is a multi-chunk transaction (> 512 KB) or a
        //    single batch that cannot fit even in an empty WAL file.
        let wal_total_sectors = match effective_durability {
            WriteDurability::SyncTransactional => self.sync_wal.total_sectors(),
            WriteDurability::BufferedFast => self.buf_wal.total_sectors(),
        };
        let exceeds_wal = self.pending_tx_buf.len() / 4070 + 2 > wal_total_sectors;
        if self.pending_tx_buf.len() > MAX_WAL_CHUNK_BYTES || exceeds_wal {
            let required_sectors = (self.pending_tx_buf.len() / 4070) + 8;
            match effective_durability {
                WriteDurability::SyncTransactional => {
                    let target_sectors = self.sync_wal.current_sector_idx() + required_sectors;
                    if target_sectors > self.sync_wal.total_sectors() {
                        self.sync_wal.ensure_min_size(
                            (target_sectors * crate::wal::WAL_SECTOR_SIZE) as u64,
                        )?;
                    }
                }
                WriteDurability::BufferedFast => {
                    let target_sectors = self.buf_wal.current_sector_idx() + required_sectors;
                    if target_sectors > self.buf_wal.total_sectors() {
                        self.buf_wal.ensure_min_size(
                            (target_sectors * crate::wal::WAL_SECTOR_SIZE) as u64,
                        )?;
                    }
                }
            }
        }

        self.tx_prepared = true;
        Ok(())
    }

    /// Commits an explicit batch transaction (`COMMIT`), writing all coalesced [`crate::wal::WalRecord`]
    /// entries in a single [`WalBatch`] syscall (or chunked batches if exceeding 512 KB)!
    ///
    /// An `Err` means the transaction was not committed (preparation or the WAL append failed, and
    /// the transaction was rolled back). Once the WAL append has succeeded the transaction is
    /// durable and this returns `Ok`, even if the follow-up maintenance (blob reclaim, quota
    /// persistence, compaction, flush) fails; such failures are counted in
    /// [`Self::maintenance_failures`] and retained in [`Self::last_maintenance_error`].
    pub fn commit_transaction(&mut self) -> io::Result<()> {
        if self.tx_depth == 1 && !self.tx_prepared {
            let durability = if self.pending_tx_sync {
                WriteDurability::SyncTransactional
            } else {
                WriteDurability::BufferedFast
            };
            if let Err(e) = self.prepare_commit_transaction(durability) {
                let _ = self.rollback_transaction();
                return Err(e);
            }
        }
        if self.tx_depth == 1 {
            if self.pending_tx_record_count == 0 {
                // Nothing of its own to publish: drop what it saw from earlier, undurable commits.
                self.tx_overlay.clear();
            } else if let Err(e) = self.drain_pending_durable() {
                let _ = self.rollback_transaction();
                return Err(e);
            }
        }
        if self.tx_depth > 0 {
            self.tx_depth -= 1;
        }
        if self.tx_depth == 0 {
            if let Err(e) = self.flush_pending_tx_records() {
                self.tx_depth = 1;
                let _ = self.rollback_transaction();
                return Err(e);
            }
            self.publish_committed_transaction();
        }
        Ok(())
    }

    /// Starts a commit for `conn_id` so that the device flush can run outside the engine lock.
    ///
    /// A plain prepared synchronous transaction is written to the log and handed over to the
    /// group commit: the next writer can start at once and sees this transaction, while readers
    /// do not until a flush has made it durable. A synchronous transaction that needs
    /// post-commit maintenance is written to the log and stays active during its flush. Any
    /// other transaction is committed here and returns [`CommitStep::Done`].
    pub(crate) fn begin_commit_for_conn(
        &mut self,
        conn_id: usize,
    ) -> io::Result<CommitStep<FS::File>> {
        self.check_write_allowed_for_conn(conn_id)?;
        let deferrable = self.tx_depth == 1
            && self.tx_prepared
            && self.pending_tx_sync
            && self.pending_tx_record_count > 0
            && self.pending_tx_buf.len() <= MAX_WAL_CHUNK_BYTES;
        if !deferrable {
            self.commit_transaction()?;
            return Ok(CommitStep::Done);
        }
        let groupable = self.capped_tables.is_empty()
            && self.dropped_tables_in_tx.is_empty()
            && !self.quotas_modified_in_tx
            && !self.deferred_flush_on_commit
            && !self.deferred_compact_on_commit
            && !self.memtable.should_flush()
            && !self.has_pending_corrupt_evictions();
        if !groupable {
            if let Err(e) = self.drain_pending_durable() {
                let _ = self.rollback_transaction();
                return Err(e);
            }
        }

        // The depth is zero during the append so that a full WAL can rotate, as in
        // `commit_transaction`.
        self.tx_depth = 0;
        self.sync_wal.set_defer_barrier(true);
        let appended = self.flush_pending_tx_records();
        self.sync_wal.set_defer_barrier(false);
        self.tx_depth = 1;
        if let Err(e) = appended {
            let _ = self.rollback_transaction();
            return Err(e);
        }
        if !groupable {
            return Ok(CommitStep::AwaitingSync(self.sync_wal.file_handle()));
        }

        let lsn = self.next_lsn - 1;
        let first_own_lsn = self.tx_start_meta.next_lsn;
        let mut own_entries = core::mem::take(&mut self.tx_overlay.map);
        self.tx_overlay.clear();
        own_entries.retain(|_, entry| entry.lsn >= first_own_lsn);
        self.pending_group.push_back((lsn, own_entries));
        self.commit_gate.note_appended(lsn);
        self.tx_depth = 0;
        self.finalize_tx_state();
        Ok(CommitStep::AwaitingGroup {
            lsn,
            file: self.sync_wal.file_handle(),
            gate: Arc::clone(&self.commit_gate),
        })
    }

    /// Completes a commit started by [`Self::begin_commit_for_conn`] with the flush result.
    ///
    /// A failed flush leaves the durability of the batch unknown, so the engine is poisoned and
    /// the transaction is discarded from memory.
    pub(crate) fn finish_commit_for_conn(&mut self, flushed: io::Result<()>) -> io::Result<()> {
        match flushed {
            Ok(()) => {
                self.tx_depth = 0;
                self.publish_committed_transaction();
                Ok(())
            }
            Err(e) => {
                let e = self.poison(e);
                let _ = self.rollback_transaction();
                Err(e)
            }
        }
    }

    /// Publishes every group-committed batch up to `target` after the leader's device flush.
    ///
    /// A failed flush poisons the engine and fails every commit waiting on the group.
    pub(crate) fn finish_group_flush(
        &mut self,
        target: u64,
        flushed: io::Result<()>,
    ) -> io::Result<()> {
        match flushed {
            Ok(()) => {
                self.absorb_durable_through(target);
                self.commit_gate.mark_durable(target, true);
                Ok(())
            }
            Err(e) => {
                self.commit_gate.fail(&e);
                Err(self.poison(e))
            }
        }
    }

    /// Flushes and publishes every group-committed batch. Paths that rotate the WAL, flush the
    /// MemTable, or write to it directly call this first, because they rely on the MemTable
    /// holding all earlier commits.
    pub(super) fn drain_pending_durable(&mut self) -> io::Result<()> {
        let Some(&(target, _)) = self.pending_group.back() else {
            return Ok(());
        };
        if let Err(e) = self.sync_wal.file_handle().sync_barrier() {
            self.commit_gate.fail(&e);
            return Err(self.poison(e));
        }
        self.absorb_durable_through(target);
        self.commit_gate.mark_durable(target, false);
        Ok(())
    }

    fn absorb_durable_through(&mut self, target: u64) {
        while self.pending_group.front().is_some_and(|(lsn, _)| *lsn <= target) {
            if let Some((_, entries)) = self.pending_group.pop_front() {
                self.memtable.absorb(entries);
            }
        }
    }

    fn seed_overlay_from_pending(&mut self) {
        let Self {
            pending_group,
            tx_overlay,
            ..
        } = self;
        for (_, entries) in pending_group.iter() {
            tx_overlay.seed(entries);
        }
    }

    /// Clears the per-transaction state once the transaction no longer owns the writer slot.
    fn finalize_tx_state(&mut self) {
        self.tx_owner_conn = None;
        self.tx_prepared = false;
        self.tx_preparing = false;
        self.pending_tx_bytes = 0;
        self.savepoints.clear();
        self.tx_start_meta = TxMetaSnapshot::default();
        self.commit_gate.tx_ended();
    }

    /// Returns the gate that coordinates waiting writers and group commits.
    pub(crate) fn commit_gate(&self) -> Arc<CommitGate> {
        Arc::clone(&self.commit_gate)
    }

    /// Makes a durably logged transaction visible and runs the post-commit maintenance.
    fn publish_committed_transaction(&mut self) {
        // The WAL batch(es) are durably appended: publish the overlay into the MemTable by
        // moving its entries (no clones). On the failure path above the overlay is discarded
        // and the MemTable never saw any of the transaction's data.
        let overlay = core::mem::take(&mut self.tx_overlay.map);
        self.tx_overlay.clear();
        self.memtable.absorb(overlay);
        self.finalize_tx_state();

        let capped_table_ids: Vec<u16> = self.capped_tables.keys().copied().collect();
        for tid in capped_table_ids {
            if let Err(e) = self.reclaim_fully_dead_lru_blob_segments(tid) {
                self.record_maintenance_failure(&e);
            }
        }

        let dropped_tables = core::mem::take(&mut self.dropped_tables_in_tx);
        let quotas_modified = core::mem::replace(&mut self.quotas_modified_in_tx, false);
        let mut dropped_any_capped = false;
        for (tid, was_capped) in dropped_tables {
            if let Err(e) = self.reclaim_fully_dead_lru_blob_segments(tid) {
                self.record_maintenance_failure(&e);
            }
            if was_capped {
                dropped_any_capped = true;
            }
        }
        let do_compact = core::mem::replace(&mut self.deferred_compact_on_commit, false);
        let do_flush = core::mem::replace(&mut self.deferred_flush_on_commit, false);
        let will_flush_or_compact = do_compact || do_flush || self.memtable.should_flush();
        if (dropped_any_capped || quotas_modified) && !will_flush_or_compact {
            let quota_metas = self.build_quota_metas();
            if let Err(e) = self.meta_mgr.commit_update(|st| {
                st.table_quotas = quota_metas;
            }) {
                self.record_maintenance_failure(&e);
            }
        }

        let maintenance = if do_compact {
            self.compact()
        } else if do_flush || self.memtable.should_flush() {
            self.flush_memtable()
        } else {
            Ok(())
        };
        if let Err(e) = maintenance {
            self.record_maintenance_failure(&e);
        }

        // The transaction is durable at this point; remove rows queued by `Evict`-policy
        // reads. Best effort: a failure here must not turn a successful commit into an error
        // (the rows are simply re-detected and re-queued by the next read).
        let _ = self.drain_corrupt_evictions();
    }

    /// Commits an explicit transaction for SQLite connection `conn_id`.
    pub fn commit_transaction_for_conn(&mut self, conn_id: usize) -> io::Result<()> {
        self.check_write_allowed_for_conn(conn_id)?;
        self.commit_transaction()
    }

    /// Aborts an explicit batch transaction (`ROLLBACK`), discarding all buffered `pending_tx_buf`
    /// mutations and restoring `MemTable`, `capped_tables`, and blob `dead_bytes` in
    /// $O(\text{tx mutations})$ time via the in-memory undo log.
    ///
    /// SQLite invokes `xRollback` once per virtual table that took part in the transaction, so a
    /// rollback with no active transaction is a no-op: there is no start snapshot to restore.
    pub fn rollback_transaction(&mut self) -> io::Result<()> {
        if self.tx_depth == 0 {
            return Ok(());
        }
        self.tx_depth = 0;
        self.tx_owner_conn = None;
        self.deferred_flush_on_commit = false;
        self.deferred_compact_on_commit = false;
        self.tx_prepared = false;
        self.tx_preparing = false;
        self.savepoints.clear();
        self.pending_tx_record_count = 0;
        self.pending_tx_bytes = 0;
        self.pending_tx_buf.clear();
        self.pending_tx_sync = false;
        self.tx_overlay.clear();
        let snap = core::mem::take(&mut self.tx_start_meta);
        self.restore_tx_state(0, &snap);
        self.commit_gate.tx_ended();
        Ok(())
    }

    /// Aborts an explicit transaction for SQLite connection `conn_id`.
    pub fn rollback_transaction_for_conn(&mut self, conn_id: usize) -> io::Result<()> {
        self.check_write_allowed_for_conn(conn_id)?;
        self.rollback_transaction()
    }

    /// Opens or updates a transaction savepoint (`xSavepoint`).
    pub fn savepoint(&mut self, sp_id: i32) {
        self.tx_prepared = false;
        let mut implicit_tx = false;
        if self.tx_depth == 0 {
            self.begin_transaction();
            implicit_tx = true;
        }
        if let Some(pos) = self.savepoints.iter().position(|s| s.sp_id == sp_id) {
            implicit_tx = self.savepoints[pos].implicit_tx;
            self.savepoints.truncate(pos);
            if self.savepoints.is_empty() {
                self.tx_overlay.undo.clear();
            }
        }
        let buf_offset = if self.pending_tx_record_count == 0 {
            0
        } else {
            self.pending_tx_buf.len()
        };
        let meta_snapshot = self.capture_tx_meta_snapshot();
        self.savepoints.push(SavepointMarker {
            sp_id,
            buf_offset,
            record_count: self.pending_tx_record_count,
            pending_tx_bytes: self.pending_tx_bytes,
            pending_tx_sync: self.pending_tx_sync,
            implicit_tx,
            undo_len: self.tx_overlay.undo.len(),
            meta_snapshot,
        });
    }

    /// Opens or updates a transaction savepoint for SQLite connection `conn_id`.
    pub fn savepoint_for_conn(&mut self, conn_id: usize, sp_id: i32) -> io::Result<()> {
        self.check_write_allowed_for_conn(conn_id)?;
        let was_zero = self.tx_depth == 0;
        self.savepoint(sp_id);
        if was_zero {
            self.tx_owner_conn = Some(conn_id);
        }
        Ok(())
    }

    /// Releases savepoints `>= sp_id` (`xRelease`).
    pub fn release_savepoint(&mut self, sp_id: i32) -> io::Result<()> {
        if let Some(pos) = self.savepoints.iter().position(|s| s.sp_id == sp_id) {
            let implicit_tx = self.savepoints[pos].implicit_tx;
            self.savepoints.truncate(pos);
            if self.savepoints.is_empty() {
                self.tx_overlay.undo.clear();
            }
            if implicit_tx && self.tx_depth == 1 {
                self.commit_transaction()?;
            }
        }
        Ok(())
    }

    /// Releases savepoints `>= sp_id` for SQLite connection `conn_id`.
    pub fn release_savepoint_for_conn(&mut self, conn_id: usize, sp_id: i32) -> io::Result<()> {
        self.check_write_allowed_for_conn(conn_id)?;
        self.release_savepoint(sp_id)
    }

    /// Rolls back all mutations since savepoint `sp_id` (`xRollbackTo`) in
    /// $O(\text{mutations since savepoint})$ time via the undo log.
    pub fn rollback_to_savepoint(&mut self, sp_id: i32) -> io::Result<()> {
        self.tx_prepared = false;
        let Some(pos) = self.savepoints.iter().position(|s| s.sp_id == sp_id) else {
            return Ok(());
        };
        let marker = self.savepoints[pos].clone();
        self.savepoints.truncate(pos + 1);
        if marker.record_count == 0 {
            self.pending_tx_buf.clear();
        } else {
            self.pending_tx_buf.truncate(marker.buf_offset);
        }
        self.pending_tx_record_count = marker.record_count;
        self.pending_tx_bytes = marker.pending_tx_bytes;
        self.pending_tx_sync = marker.pending_tx_sync;
        self.restore_tx_state(marker.undo_len, &marker.meta_snapshot);
        Ok(())
    }

    /// Rolls back to savepoint `sp_id` for SQLite connection `conn_id`.
    pub fn rollback_to_savepoint_for_conn(&mut self, conn_id: usize, sp_id: i32) -> io::Result<()> {
        self.check_write_allowed_for_conn(conn_id)?;
        self.rollback_to_savepoint(sp_id)
    }

    pub(super) fn capture_tx_meta_snapshot(&self) -> TxMetaSnapshot {
        TxMetaSnapshot {
            next_lsn: self.next_lsn,
            std_blobs: self
                .std_blobs
                .iter()
                .map(|(&seq, b)| (seq, (b.file_len, b.dead_bytes)))
                .collect(),
            lru_blobs: self
                .lru_blobs
                .iter()
                .map(|(&k, b)| (k, (b.file_len, b.dead_bytes)))
                .collect(),
            capped_tables: self.capped_tables.clone(),
            active_vtab_ids: self.active_vtab_ids.clone(),
            table_has_row_meta: self.table_has_row_meta.clone(),
            dropped_tables_len: self.dropped_tables_in_tx.len(),
            quotas_modified_in_tx: self.quotas_modified_in_tx,
        }
    }

    pub(super) fn restore_tx_state(&mut self, undo_target_len: usize, snap: &TxMetaSnapshot) {
        self.tx_overlay.rollback_to(undo_target_len);
        self.next_lsn = snap.next_lsn;
        for (&seq, b) in &mut self.std_blobs {
            let (snap_len, snap_dead) = snap.std_blobs.get(&seq).copied().unwrap_or((0, 0));
            let aborted = b.file_len.saturating_sub(snap_len);
            b.dead_bytes = snap_dead.saturating_add(aborted);
        }
        for (&k, b) in &mut self.lru_blobs {
            let (snap_len, snap_dead) = snap.lru_blobs.get(&k).copied().unwrap_or((0, 0));
            let aborted = b.file_len.saturating_sub(snap_len);
            b.dead_bytes = snap_dead.saturating_add(aborted);
        }
        self.capped_tables = snap.capped_tables.clone();
        self.active_vtab_ids = snap.active_vtab_ids.clone();
        self.table_has_row_meta = snap.table_has_row_meta.clone();
        self.dropped_tables_in_tx.truncate(snap.dropped_tables_len);
        self.quotas_modified_in_tx = snap.quotas_modified_in_tx;
    }

    pub(super) fn flush_pending_tx_records(&mut self) -> io::Result<()> {
        if self.pending_tx_record_count == 0 {
            return Ok(());
        }
        self.check_not_poisoned()?;
        let record_count = self.pending_tx_record_count;
        self.pending_tx_record_count = 0;
        let durability = if self.pending_tx_sync {
            WriteDurability::SyncTransactional
        } else {
            WriteDurability::BufferedFast
        };
        self.pending_tx_sync = false;

        if durability == WriteDurability::SyncTransactional {
            self.sync_active_blob_writers()?;
        } else {
            self.flush_active_blob_buffers()?;
        }

        if self.pending_tx_buf.len() <= MAX_WAL_CHUNK_BYTES {
            // `next_lsn` advances only after the append: a ring-full rotation or flush derives the
            // checkpoint LSN from `next_lsn - 1`, which must stay below this batch's LSN.
            let lsn = self.next_lsn;
            WalBatch::finalize_encoded_buf(lsn, record_count, &mut self.pending_tx_buf);
            let mut raw_buf = core::mem::take(&mut self.pending_tx_buf);
            let res = self.append_raw_wal_batch_with_rotation(&raw_buf, durability);
            raw_buf.clear();
            self.pending_tx_buf = raw_buf;
            if res.is_ok() {
                self.next_lsn = lsn + 1;
            }
            return res;
        }

        // Large transaction buffer (> 512 KB): chunk serialized records into <= 512 KB WalBatches
        // framed by `WalRecord::TxBegin` in the first chunk and `WalRecord::TxCommit` in the last
        // chunk. Ensure the target WAL file has enough capacity so all chunks of the
        // transaction remain in the same WAL epoch without flushing uncommitted `MemTable` state.
        let mut raw_buf = core::mem::take(&mut self.pending_tx_buf);
        let mut cursor = 16usize;
        let total_len = raw_buf.len();
        let mut chunks: Vec<(Vec<u8>, u32)> = Vec::new();
        let mut chunk_buf = Vec::with_capacity(MAX_WAL_CHUNK_BYTES + 4096);
        let mut chunk_records = 0u32;

        while cursor < total_len {
            if chunk_records == 0 {
                chunk_buf.clear();
                chunk_buf.extend_from_slice(&[0u8; 16]);
                if chunks.is_empty() {
                    // Prepend `WalRecord::TxBegin` (`4u8`) to the first chunk.
                    chunk_buf.push(4u8);
                    chunk_records += 1;
                }
            }
            let rec_start = cursor;
            let op = raw_buf[cursor];
            cursor += 1;
            match op {
                1 => {
                    let klen =
                        u32::from_le_bytes(raw_buf[cursor..cursor + 4].try_into().unwrap())
                            as usize;
                    cursor += 4 + klen;
                    let vlen =
                        u32::from_le_bytes(raw_buf[cursor..cursor + 4].try_into().unwrap())
                            as usize;
                    cursor += 4 + vlen;
                }
                2 => {
                    let klen =
                        u32::from_le_bytes(raw_buf[cursor..cursor + 4].try_into().unwrap())
                            as usize;
                    cursor += 4 + klen + 23;
                }
                3 => {
                    let klen =
                        u32::from_le_bytes(raw_buf[cursor..cursor + 4].try_into().unwrap())
                            as usize;
                    cursor += 4 + klen;
                }
                4 | 5 => {}
                _ => unreachable!("invalid op in pending_tx_buf"),
            }
            let rec_slice = &raw_buf[rec_start..cursor];
            let min_records_before_split = if chunks.is_empty() { 2 } else { 1 };
            if chunk_records >= min_records_before_split
                && chunk_buf.len() + rec_slice.len() > MAX_WAL_CHUNK_BYTES
            {
                chunks.push((core::mem::take(&mut chunk_buf), chunk_records));
                chunk_buf = Vec::with_capacity(MAX_WAL_CHUNK_BYTES + 4096);
                chunk_buf.extend_from_slice(&[0u8; 16]);
                chunk_records = 0;
            }
            chunk_buf.extend_from_slice(rec_slice);
            chunk_records += 1;
        }

        if chunk_records > 0 {
            chunks.push((chunk_buf, chunk_records));
        }

        raw_buf.clear();
        self.pending_tx_buf = raw_buf;

        if chunks.len() == 1 {
            let (mut single_buf, mut single_records) = chunks.pop().unwrap();
            // Strip the leading `TxBegin` (`4u8` at index 16) since it fit in a single chunk
            single_buf.remove(16);
            single_records -= 1;
            let lsn = self.next_lsn;
            WalBatch::finalize_encoded_buf(lsn, single_records, &mut single_buf);
            self.append_raw_wal_batch_with_rotation(&single_buf, durability)?;
            self.next_lsn = lsn + 1;
            return Ok(());
        }

        if let Some((last_buf, last_records)) = chunks.last_mut() {
            // Append `WalRecord::TxCommit` (`5u8`) to the final chunk.
            last_buf.push(5u8);
            *last_records += 1;
        }

        // Ensure the target WAL stream has sufficient sectors to fit all chunks in the current epoch
        let mut required_sectors = 0usize;
        for (cbuf, _) in &chunks {
            required_sectors += (cbuf.len() / 4070) + 2;
        }
        match durability {
            WriteDurability::SyncTransactional => {
                let target_sectors = self.sync_wal.current_sector_idx() + required_sectors;
                if target_sectors > self.sync_wal.total_sectors() {
                    self.sync_wal
                        .ensure_min_size((target_sectors * crate::wal::WAL_SECTOR_SIZE) as u64)?;
                }
            }
            WriteDurability::BufferedFast => {
                let target_sectors = self.buf_wal.current_sector_idx() + required_sectors;
                if target_sectors > self.buf_wal.total_sectors() {
                    self.buf_wal
                        .ensure_min_size((target_sectors * crate::wal::WAL_SECTOR_SIZE) as u64)?;
                }
            }
        }

        for (mut cbuf, crec) in chunks {
            let lsn = self.next_lsn;
            WalBatch::finalize_encoded_buf(lsn, crec, &mut cbuf);
            self.append_raw_wal_batch_with_rotation(&cbuf, durability)?;
            self.next_lsn = lsn + 1;
        }

        Ok(())
    }
}
