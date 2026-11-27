//! Transaction & savepoint state machine, undo logs, and cross-connection snapshot isolation.

use std::collections::{BTreeMap, HashMap};
use std::io;
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::time::Duration;

use crate::index::{BloomFilter, IndexValue};
use crate::lru::CappedTableState;
use crate::memtable::MemEntry;
use crate::pal::{PlatformFile, PlatformFs};
use crate::wal::{WalBatch, WalRecord};

use super::reader::CxRef;
#[cfg(test)]
pub(super) use super::tx_fsm::TxPhase;
pub(super) use super::tx_fsm::WriteScope;
use super::tx_fsm::{
    self, FlushCompleteMode, HealthState, TxCore, TxCoreState, TxEffect, TxEffects, TxError,
    TxEvent, TxMachine,
};
use super::{EngineReader, StorageEngine, WriteDurability};

/// Savepoint id of the engine savepoint that makes one `xUpdate` call atomic. SQLite numbers its
/// savepoints from -1 (the savepoint that started the transaction, when there was no `BEGIN`)
/// upwards, so this id never collides with one of them. It is opened and closed within one
/// call, so it is never open while SQLite addresses a savepoint.
pub(crate) const STATEMENT_SAVEPOINT: i32 = i32::MIN;

/// Transactions larger than this are split into several WAL batches.
const MAX_WAL_CHUNK_BYTES: usize = 512 * 1024;

/// Upper bound on one condition-variable wait. Callers re-check their condition afterwards.
const COMMIT_GATE_WAIT: Duration = Duration::from_millis(50);

#[inline]
fn checked_wal_bytes(sectors: usize) -> io::Result<u64> {
    (sectors as u64)
        .checked_mul(crate::wal::WAL_SECTOR_SIZE as u64)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "WAL target size overflow"))
}

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
    /// Serializes device flush calls (`sync_barrier`) between a group-commit leader and
    /// `drain_pending_durable()` so two callers never run `fdatasync` on the same fd concurrently.
    /// Lock ordering: always acquired before `state`, and NEVER held while acquiring `engine.write()`.
    sync_mu: Mutex<()>,
    /// Latch set immediately inside `fail` / `abandon_flush` so `check_not_poisoned()` detects
    /// a dropped/unwound flush leader before entering engine writes.
    failed_flag: std::sync::atomic::AtomicBool,
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

    pub(super) fn mark_durable(&self, lsn: u64, ends_leadership: bool) {
        let mut state = self.lock();
        state.durable_end = state.durable_end.max(lsn + 1);
        if ends_leadership {
            state.leader_active = false;
        }
        drop(state);
        self.changed.notify_all();
    }

    pub(crate) fn fail(&self, error: &io::Error) {
        self.failed_flag
            .store(true, std::sync::atomic::Ordering::Release);
        let mut state = self.lock();
        state.failed = Some(error.to_string());
        state.leader_active = false;
        drop(state);
        self.changed.notify_all();
    }

    #[inline]
    pub(super) fn is_failed(&self) -> bool {
        self.failed_flag.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Acquires the mutex serializing `wal_sync.db` device barriers.
    pub(crate) fn lock_sync(&self) -> io::Result<std::sync::MutexGuard<'_, ()>> {
        self.sync_mu
            .lock()
            .map_err(|_| io::Error::other("WAL sync mutex poisoned"))
    }

    /// Returns the recorded group flush error, if the gate has failed.
    pub(crate) fn failed_error(&self) -> Option<io::Error> {
        self.lock().failed.as_deref().map(group_flush_error)
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

    /// Returns `true` if `lsn` has already been marked durable by a completed flush or barrier.
    pub(crate) fn is_durable_through(&self, lsn: u64) -> bool {
        self.lock().durable_end > lsn
    }

    /// Waits until the batch at `lsn` is durable or this caller has to run the device flush.
    pub(crate) fn join_group(&self, lsn: u64) -> io::Result<GroupRole> {
        let mut state = self.lock();
        loop {
            if state.durable_end > lsn {
                return Ok(GroupRole::Durable);
            }
            if let Some(message) = &state.failed {
                return Err(group_flush_error(message));
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

    /// Returns a RAII guard that calls [`Self::abandon_flush`] if the flush leader unwinds before
    /// disarming it after [`StorageEngine::finish_group_flush`].
    pub(crate) fn leader_guard(&self) -> CommitLeaderGuard<'_> {
        CommitLeaderGuard {
            gate: self,
            armed: true,
        }
    }
}

/// Ensures a panicked or early-returning group-commit flush leader clears `leader_active` so
/// waiting followers never hang indefinitely.
pub(crate) struct CommitLeaderGuard<'a> {
    gate: &'a CommitGate,
    armed: bool,
}

impl CommitLeaderGuard<'_> {
    #[inline]
    pub(crate) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for CommitLeaderGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.gate
                .abandon_flush(&io::Error::other("group-commit leader aborted"));
        }
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

#[derive(Clone)]
pub(super) struct TxMetaSnapshot {
    pub(super) next_lsn: u64,
    pub(super) std_blobs: BTreeMap<u64, (u64, u64)>,
    pub(super) lru_blobs: BTreeMap<(u16, u64), (u64, u64)>,
    pub(super) capped_tables: HashMap<u16, CappedTableState>,
    pub(super) active_vtab_ids: HashMap<u16, Arc<str>>,
    pub(super) active_vtab_holders: HashMap<u16, Vec<u64>>,
    pub(super) corrupt_policies: HashMap<u16, super::CorruptPolicy>,
    pub(super) table_inline_thresholds: BTreeMap<u16, usize>,
    pub(super) table_has_synthetic_rowids: BTreeMap<u16, bool>,
    pub(super) table_has_row_meta: BTreeMap<u16, bool>,
    pub(super) dropped_tables_len: usize,
    pub(super) lru_clears_len: usize,
    pub(super) vtabs_created_in_tx: Vec<(u16, String, String)>,
    pub(super) quotas_modified_in_tx: bool,
}

#[derive(Clone)]
pub(super) struct SavepointPayload {
    pub(super) buf_offset: usize,
    pub(super) undo_len: usize,
    pub(super) meta: SavepointMeta,
}

/// Per-transaction data payload owned alongside [`TxCore`] by [`TxState::InTx`] and dropped
/// or moved out atomically on return to [`TxState::Idle`].
pub(super) struct ActiveTxData {
    pub(super) pending_buf: Vec<u8>,
    /// Uncommitted writes of the active explicit transaction (see [`TxOverlay`]).
    pub(super) overlay: TxOverlay,
    pub(super) start_meta: TxMetaSnapshot,
    pub(super) savepoint_payloads: Vec<SavepointPayload>,
    pub(super) statement_blob_undo: Vec<BlobUndo>,
    /// Deferred `(table_id, pk, max_tick)` access-tracker clears recorded by in-transaction
    /// writes/evictions and applied only once the transaction commits.
    pub(super) pending_lru_clears: Vec<(u16, Vec<u8>, u64)>,
    /// Tables dropped inside the currently active transaction (`(table_id, was_capped)`).
    pub(super) dropped_tables: Vec<(u16, bool)>,
    /// Virtual tables created (`xCreate`) inside the currently active transaction, as
    /// `(table_id, normalised name, normalised sqlite schema name)`.
    pub(super) vtabs_created: Vec<(u16, String, String)>,
    /// True if any capped-table quota was registered or modified inside the active transaction.
    pub(super) quotas_modified: bool,
    /// True if the active transaction modified any UNIQUE completion marker (`0xFFFF`).
    pub(super) unique_marker_modified: bool,
    /// True if `flush_memtable()` was requested during an active transaction.
    pub(super) deferred_flush: bool,
    /// True if `compact()` was requested during an active transaction.
    pub(super) deferred_compact: bool,
}

impl ActiveTxData {
    fn new(start_meta: TxMetaSnapshot, mut pending_buf: Vec<u8>) -> Self {
        pending_buf.clear();
        Self {
            pending_buf,
            overlay: TxOverlay::default(),
            start_meta,
            savepoint_payloads: Vec::new(),
            statement_blob_undo: Vec::new(),
            pending_lru_clears: Vec::new(),
            dropped_tables: Vec::new(),
            vtabs_created: Vec::new(),
            quotas_modified: false,
            unique_marker_modified: false,
            deferred_flush: false,
            deferred_compact: false,
        }
    }
}

/// Top-level transaction state machine wrapper on [`StorageEngine`], pairing pure [`TxCore`]
/// with its shell-managed [`ActiveTxData`] payload.
#[derive(Default)]
pub(super) enum TxState {
    #[default]
    Idle,
    InTx {
        core: TxCore,
        data: Box<ActiveTxData>,
    },
}

impl TxState {
    /// True while a transaction owns the writer slot and has not entered `FlushingWal`.
    /// During `FlushingWal`, `RingFull` WAL rotations must execute without deferring while still
    /// observing [`Self::start_meta`].
    #[inline]
    pub(super) fn is_active(&self) -> bool {
        matches!(self, Self::InTx { core, .. } if core.is_active())
    }

    /// True whenever a transaction is present, including [`TxPhase::FlushingWal`].
    #[inline]
    pub(super) fn is_in_tx(&self) -> bool {
        !matches!(self, Self::Idle)
    }

    #[inline]
    pub(super) fn core(&self) -> Option<&TxCore> {
        match self {
            Self::InTx { core, .. } => Some(core),
            Self::Idle => None,
        }
    }

    #[inline]
    pub(super) fn active_core(&self) -> Option<&TxCore> {
        match self {
            Self::InTx { core, .. } if core.is_active() => Some(core),
            Self::Idle | Self::InTx { .. } => None,
        }
    }

    #[inline]
    pub(super) fn active_tx(&self) -> Option<&ActiveTxData> {
        match self {
            Self::InTx { core, data } if core.is_active() => Some(data),
            Self::Idle | Self::InTx { .. } => None,
        }
    }

    #[inline]
    pub(super) fn active_tx_mut(&mut self) -> Option<&mut ActiveTxData> {
        match self {
            Self::InTx { core, data } if core.is_active() => Some(data),
            Self::Idle | Self::InTx { .. } => None,
        }
    }

    #[inline]
    pub(super) fn any_tx(&self) -> Option<&ActiveTxData> {
        match self {
            Self::InTx { data, .. } => Some(data),
            Self::Idle => None,
        }
    }

    #[inline]
    pub(super) fn any_tx_mut(&mut self) -> Option<&mut ActiveTxData> {
        match self {
            Self::InTx { data, .. } => Some(data),
            Self::Idle => None,
        }
    }

    #[inline]
    pub(super) fn start_meta(&self) -> Option<&TxMetaSnapshot> {
        self.any_tx().map(|tx| &tx.start_meta)
    }

    #[cfg(test)]
    #[inline]
    pub(super) fn phase(&self) -> Option<TxPhase> {
        self.core().map(TxCore::phase)
    }

    #[cfg(test)]
    #[inline]
    pub(super) fn is_prepared(&self) -> bool {
        self.phase() == Some(TxPhase::Prepared)
    }

    #[inline]
    pub(super) fn is_preparing(&self) -> bool {
        self.core().is_some_and(TxCore::is_preparing)
    }

    #[inline]
    pub(super) fn pending_record_count(&self) -> u32 {
        self.core().map_or(0, TxCore::pending_record_count)
    }

    #[inline]
    pub(super) fn pending_bytes(&self) -> usize {
        self.core().map_or(0, TxCore::pending_bytes)
    }

    #[inline]
    pub(super) fn statement_savepoint_active(&self) -> bool {
        self.core().is_some_and(TxCore::statement_savepoint_active)
    }

    #[cfg(test)]
    #[inline]
    pub(super) fn pending_sync(&self) -> bool {
        self.core().is_some_and(TxCore::pending_sync)
    }

    #[inline]
    pub(super) fn can_record_write(
        &self,
        poisoned: bool,
        scope: WriteScope,
        record_count: usize,
        batch_bytes: usize,
        max_transaction_bytes: usize,
    ) -> Result<bool, TxError> {
        let health = if poisoned {
            HealthState::Poisoned
        } else {
            HealthState::Healthy
        };
        tx_fsm::can_record_write(
            health,
            self.core(),
            scope,
            record_count,
            batch_bytes,
            max_transaction_bytes,
        )
    }

    #[inline]
    fn take_parts(&mut self) -> (TxCoreState, Option<Box<ActiveTxData>>) {
        match core::mem::take(self) {
            Self::Idle => (TxCoreState::Idle, None),
            Self::InTx { core, data } => (TxCoreState::InTx(core), Some(data)),
        }
    }

    #[inline]
    fn install_in_tx(&mut self, core: TxCore, data: Box<ActiveTxData>) {
        *self = Self::InTx { core, data };
    }
}

#[derive(Clone, Copy)]
pub(super) enum BlobUndo {
    Appended {
        kind: crate::wal::BlobKind,
        table_id: u16,
        seg_id: u64,
        len: u64,
    },
    MarkedDead {
        kind: crate::wal::BlobKind,
        table_id: u16,
        seg_id: u64,
        len: u64,
    },
}

#[derive(Clone)]
pub(super) struct StatementMetaSnapshot {
    pub(super) next_lsn: u64,
    pub(super) table_id: u16,
    pub(super) capped_table: Option<CappedTableState>,
    pub(super) table_has_synthetic_rowids: Option<bool>,
    pub(super) table_has_row_meta: Option<bool>,
    pub(super) blob_undo_len: usize,
    pub(super) lru_clears_len: usize,
}

#[derive(Clone)]
pub(super) enum SavepointMeta {
    Full(Box<TxMetaSnapshot>),
    Statement(StatementMetaSnapshot),
}

/// Private, ordered write set of the active explicit transaction.
///
/// While a transaction is active, every mutation lands here instead of in the shared `MemTable`, so the
/// `MemTable` only ever holds committed data and non-owner readers never need to hide anything.
/// Owner reads treat the overlay as the highest-precedence source (overlay > memtable > segments).
///
/// LSN note: overlay entries carry the `next_lsn` value of their mutation time, which is greater
/// than every committed LSN. Merged scans nevertheless do not compare overlay LSNs; precedence is
/// by source (see `MemView`), so correctness does not depend on that numeric property.
#[derive(Default)]
pub(super) struct TxOverlay {
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
    pub(super) fn clear(&mut self) {
        self.map.clear();
        self.undo.clear();
        self.min_key = None;
        self.max_key = None;
        self.filter = [0; 16];
    }

    #[inline]
    fn bloom_indices(key: &[u8]) -> ((usize, u64), (usize, u64)) {
        let (h1, h2) = BloomFilter::hash_pair(key);
        let slot1 = ((h1 as usize & 1023) / 64, 1u64 << (h1 & 63));
        let slot2 = ((h2 as usize & 1023) / 64, 1u64 << (h2 & 63));
        (slot1, slot2)
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
        let ((w1, m1), (w2, m2)) = Self::bloom_indices(key);
        (self.filter[w1] & m1) != 0 && (self.filter[w2] & m2) != 0
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
        let ((w1, m1), (w2, m2)) = Self::bloom_indices(key);
        self.filter[w1] |= m1;
        self.filter[w2] |= m2;
    }

    pub(super) fn apply(&mut self, lsn: u64, rec: WalRecord, record_undo: bool) {
        let (key, value) = match rec {
            WalRecord::PutInline { key, value } => (key, IndexValue::Inline(value)),
            WalRecord::PutBlobPtr { key, blob_ptr } => (key, IndexValue::Blob(blob_ptr)),
            WalRecord::Delete { key } => (key, IndexValue::Tombstone),
            WalRecord::TxBegin | WalRecord::TxCommit | WalRecord::TxContinue => return,
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
        let mut removed_min_or_max = false;
        while self.undo.len() > target_len {
            popped = true;
            let (key, prev) = self.undo.pop().unwrap();
            match prev {
                Some(e) => {
                    self.map.insert(key, e);
                }
                None => {
                    if self.min_key.as_deref() == Some(key.as_slice())
                        || self.max_key.as_deref() == Some(key.as_slice())
                    {
                        removed_min_or_max = true;
                    }
                    self.map.remove(&key);
                }
            }
        }
        if popped {
            if self.map.is_empty() {
                self.min_key = None;
                self.max_key = None;
                self.filter = [0; 16];
            } else if removed_min_or_max {
                self.min_key = self.map.first_key_value().map(|(k, _)| k.clone());
                self.max_key = self.map.last_key_value().map(|(k, _)| k.clone());
            }
            // Bloom bits remain conservatively valid until the overlay empties (zero false
            // negatives), avoiding an O(M) filter rebuild on partial rollback.
        }
    }
}

impl<FS: PlatformFs> StorageEngine<FS> {
    pub(super) fn tx_error_to_io(&self, err: TxError) -> io::Error {
        match err {
            TxError::Poisoned => self
                .check_not_poisoned()
                .err()
                .unwrap_or_else(|| io::Error::other("engine is poisoned and must be reopened")),
            TxError::Busy => io::Error::new(
                io::ErrorKind::WouldBlock,
                "database is locked by another connection's active transaction",
            ),
            TxError::TxBudgetExceeded => io::Error::new(
                io::ErrorKind::InvalidInput,
                "transaction exceeds max_transaction_bytes budget",
            ),
            TxError::TooManyRecords => io::Error::new(
                io::ErrorKind::InvalidInput,
                "too many records in transaction",
            ),
            TxError::UnknownSavepoint(sp_id) => io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("cannot roll back to unknown savepoint {sp_id}"),
            ),
            TxError::InvalidPhase => io::Error::other("invalid transaction state transition"),
        }
    }

    #[cfg(test)]
    pub(crate) fn enable_tx_trace(&mut self) {
        self.tx_trace = Some(tx_fsm::TxTraceRecorder::new());
    }

    #[cfg(test)]
    pub(super) fn tx_trace(&self) -> Option<&tx_fsm::TxTraceRecorder> {
        self.tx_trace.as_ref()
    }

    #[cfg(test)]
    pub(crate) fn take_tx_trace(&mut self) -> Option<tx_fsm::TxTraceRecorder> {
        self.tx_trace.take()
    }

    pub(super) fn apply_tx_event(
        &mut self,
        event: TxEvent,
    ) -> Result<(TxEffects, Option<Box<ActiveTxData>>), TxError> {
        let health = if self.is_poisoned() {
            HealthState::Poisoned
        } else {
            HealthState::Healthy
        };
        let (prev_core, data_opt) = self.tx_state.take_parts();
        #[cfg(test)]
        let machine = {
            let pre_health = if let Some(recorder) = self.tx_trace.as_mut() {
                if recorder.last_health() == Some(HealthState::Healthy)
                    && health == HealthState::Poisoned
                {
                    if event == TxEvent::MarkPoisoned {
                        HealthState::Healthy
                    } else {
                        recorder.record_external_poison(&prev_core);
                        HealthState::Poisoned
                    }
                } else {
                    health
                }
            } else {
                health
            };
            TxMachine::from_parts(pre_health, prev_core)
        };
        #[cfg(not(test))]
        let machine = TxMachine::from_parts(health, prev_core);
        #[cfg(test)]
        let pre = self.tx_trace.is_some().then(|| machine.clone());
        let (next_machine, outcome) = tx_fsm::step(machine, event);
        #[cfg(test)]
        if let (Some(recorder), Some(pre)) = (self.tx_trace.as_mut(), pre) {
            recorder.record(
                pre,
                event,
                next_machine.clone(),
                tx_fsm::TxTraceOutcome::from_result(&outcome),
            );
        }
        let taken = match (next_machine.state, data_opt) {
            (TxCoreState::InTx(core), Some(data)) => {
                self.tx_state.install_in_tx(core, data);
                None
            }
            (TxCoreState::InTx(core), None) => {
                debug_assert!(
                    outcome
                        .as_ref()
                        .is_ok_and(|eff| eff.iter().any(|e| matches!(e, TxEffect::InitTxData)))
                );
                self.in_flight_autocommit_blob_starts.clear();
                let start_meta = self.capture_initial_tx_meta_snapshot();
                let buf = self.take_scratch_tx_buf();
                let data = Box::new(ActiveTxData::new(start_meta, buf));
                self.tx_state.install_in_tx(core, data);
                None
            }
            (TxCoreState::Idle, data) => data,
        };
        outcome.map(|effects| (effects, taken))
    }

    /// Returns `true` if an explicit or savepoint-backed transaction is currently active.
    #[inline]
    pub fn in_active_transaction(&self) -> bool {
        self.tx_state.is_active()
    }

    /// Begins an explicit batch transaction (`BEGIN`). Subsequent mutations apply immediately to
    /// the private transaction overlay (for read-your-own-writes visibility) while coalescing their
    /// [`crate::wal::WalRecord`] entries into a single atomic [`WalBatch`] flushed on [`Self::commit_transaction`].
    pub fn begin_transaction(&mut self) {
        self.mutation_epoch = self.mutation_epoch.wrapping_add(1);
        let _ = self.apply_tx_event(TxEvent::Begin);
    }

    /// Begins an explicit transaction owned by SQLite connection `conn_id`.
    /// Returns `io::ErrorKind::WouldBlock` (`SQLITE_BUSY`) if another connection already holds
    /// an active transaction on this `StorageEngine`.
    pub fn begin_transaction_for_conn(&mut self, conn_id: usize) -> io::Result<()> {
        self.mutation_epoch = self.mutation_epoch.wrapping_add(1);
        self.apply_tx_event(TxEvent::BeginForConn { conn_id })
            .map(|_| ())
            .map_err(|e| self.tx_error_to_io(e))
    }

    /// Begins (or joins) the engine transaction for SQLite connection `conn_id` on behalf of
    /// virtual table `vtab_id`, keeping `tx.depth == 1` across multiple tables in one transaction
    /// and tracking `vtab_id` for `xSync`.
    pub(crate) fn begin_vtab_transaction(
        &mut self,
        conn_id: usize,
        vtab_id: usize,
    ) -> io::Result<()> {
        self.mutation_epoch = self.mutation_epoch.wrapping_add(1);
        self.apply_tx_event(TxEvent::BeginVtab { conn_id, vtab_id })
            .map(|_| ())
            .map_err(|e| self.tx_error_to_io(e))
    }

    /// Registers an already-created `vtab_id` (`p_vtab as usize`) with the active transaction
    /// owned by `conn_id` (used when `xCreate` adds a table without SQLite invoking `xBegin`).
    pub(crate) fn attach_vtab_to_tx(&mut self, conn_id: usize, vtab_id: usize) {
        let _ = self.apply_tx_event(TxEvent::AttachVtab { conn_id, vtab_id });
    }

    /// Removes `vtab_id` from the active transaction's participating tables on disconnect.
    pub(crate) fn detach_vtab_from_tx(&mut self, conn_id: usize, vtab_id: usize) {
        let _ = self.apply_tx_event(TxEvent::DetachVtab { conn_id, vtab_id });
    }

    /// Returns the effective durability of the active transaction, upgrading `fallback` if any
    /// participating table requested synchronous durability.
    pub(crate) fn effective_tx_durability(&self, fallback: WriteDurability) -> WriteDurability {
        self.tx_state
            .core()
            .map_or(fallback, |core| core.effective_durability(fallback))
    }

    /// Records that `vtab_id` has reached `xSync` with `durability` and returns `true` when all
    /// participating virtual tables have synced (clearing per-attempt sync progress so a
    /// `SQLITE_BUSY` retry of `COMMIT` re-syncs every participating table).
    pub(crate) fn mark_vtab_synced_ready_to_commit(
        &mut self,
        vtab_id: usize,
        durability: WriteDurability,
    ) -> bool {
        let Ok((effects, _)) = self.apply_tx_event(TxEvent::VtabSync {
            vtab_id,
            durability,
        }) else {
            return true;
        };
        for eff in effects.iter() {
            if let TxEffect::VtabSyncOutcome { all_synced } = eff {
                return all_synced;
            }
        }
        true
    }

    /// Whether `conn_id` has a transaction open in the engine (it has written since `BEGIN`).
    pub(crate) fn conn_has_open_transaction(&self, conn_id: usize) -> bool {
        self.tx_state
            .active_core()
            .is_some_and(|core| core.owner_conn() == Some(conn_id))
    }

    /// Checks whether `conn_id` is permitted to mutate this `StorageEngine`.
    /// Returns `io::ErrorKind::WouldBlock` (`SQLITE_BUSY`) if another connection holds an active transaction.
    #[inline]
    pub fn check_write_allowed_for_conn(&self, conn_id: usize) -> io::Result<()> {
        if let Some(core) = self.tx_state.active_core() {
            if core.owner_conn() != Some(conn_id) {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "database is locked by another connection's active transaction",
                ));
            }
        }
        Ok(())
    }

    /// Executes a fallible read closure `f` for `conn_id` with exclusive engine access.
    ///
    /// If `conn_id` owns the active transaction (or no transaction is active), `f` sees the
    /// transaction overlay on top of the `MemTable` (read-your-own-writes). Otherwise `f` sees
    /// the committed data only. The view is a cheap per-call selection: no engine state is
    /// modified and the cost is independent of the number of uncommitted mutations.
    ///
    /// Because access is exclusive, the closure may be re-run once after the engine flushed a
    /// buffered blob the reader needed (it must be free of side effects before its first read
    /// result), and `EagerWriteOnRead` promotions queued by the closure are applied afterwards
    /// (their errors are not reported). If flushing buffered blobs fails, that I/O error is
    /// returned. Prefer [`Self::reader_for_conn`] under a shared lock when
    /// [`Self::read_needs_exclusive`] is `false`.
    pub fn with_committed_read_for_conn<F, R>(&mut self, conn_id: usize, mut f: F) -> io::Result<R>
    where
        F: FnMut(&EngineReader<'_, FS>) -> io::Result<R>,
    {
        let src = super::reader::ReadSource::from_view(self.read_view_for_conn(conn_id));
        Self::combine_read_result(self.run_read_src(src, |eng, cx| {
            f(&EngineReader {
                eng,
                cx: CxRef::Borrowed(cx),
            })
        }))
    }

    pub(crate) fn try_with_committed_read_for_conn<F, R>(
        &mut self,
        conn_id: usize,
        mut f: F,
    ) -> io::Result<R>
    where
        F: FnMut(&EngineReader<'_, FS>) -> R,
    {
        let src = super::reader::ReadSource::from_view(self.read_view_for_conn(conn_id));
        let (r, extra) = self.run_read_src(src, |eng, cx| {
            f(&EngineReader {
                eng,
                cx: CxRef::Borrowed(cx),
            })
        });
        extra.map_or(Ok(r), Err)
    }

    /// Prepares the active transaction for commit (`xSync` two-phase commit phase 1).
    /// Runs deferred capped-table quota evictions inside the undoable transaction context,
    /// flushes/syncs out-of-line blob segments, and pre-checks WAL capacity so any fallible I/O
    /// or space errors surface during `xSync` while `xRollback` can still cleanly undo all state.
    pub fn prepare_commit_transaction(&mut self, durability: WriteDurability) -> io::Result<()> {
        let (effects, _) = self
            .apply_tx_event(TxEvent::BeginPrepare { durability })
            .map_err(|e| self.tx_error_to_io(e))?;
        let Some(effective_durability) = effects.iter().find_map(|eff| match eff {
            TxEffect::RunPrepare {
                effective_durability,
            } => Some(effective_durability),
            _ => None,
        }) else {
            return Ok(());
        };

        let res: io::Result<()> = (|| {
            // 1. Run capped-table quota evictions while active so eviction mutations are
            //    recorded in `pending_buf` and `overlay` (fully undoable on `xRollback`!).
            let capped_table_ids: Vec<u16> = self.capped_tables.keys().copied().collect();
            for tid in capped_table_ids {
                self.evict_until_within_quota(tid, effective_durability)?;
            }

            // 2. Flush/sync active blob writers (`Write-Blob-Before-WAL` & prefix durability).
            self.flush_active_blob_buffers()?;
            if effective_durability == WriteDurability::SyncTransactional {
                if self.last_buf_append_lsn > self.last_buf_synced_lsn {
                    // Syncs the active blob writers before the buffered log.
                    self.make_buf_wal_durable(true)?;
                } else {
                    self.sync_active_blob_writers()?;
                }
            }

            // 3. Pre-verify/grow WAL capacity if this is a multi-chunk transaction (> 512 KB) or a
            //    single batch that cannot fit even in an empty WAL file.
            let pending_buf_len = self
                .tx_state
                .active_tx()
                .map_or(0, |tx| tx.pending_buf.len());
            let usable = crate::wal::EST_WAL_SECTOR_USABLE_BYTES;
            let wal_total_sectors = match effective_durability {
                WriteDurability::SyncTransactional => self.sync_wal().total_sectors(),
                WriteDurability::BufferedFast => self.buf_wal().total_sectors(),
            };
            let exceeds_wal = pending_buf_len / usable + 2 > wal_total_sectors;
            if pending_buf_len > MAX_WAL_CHUNK_BYTES || exceeds_wal {
                let required_sectors = (pending_buf_len / usable) + 8;
                self.ensure_wal_sector_headroom(effective_durability, required_sectors)?;
            }
            Ok(())
        })();

        let _ = self.apply_tx_event(TxEvent::FinishPrepare { ok: res.is_ok() });
        res
    }

    #[inline]
    fn tx_has_metadata_changes(&self) -> bool {
        let mem_should_flush = self.memtable.should_flush();
        self.tx_state.active_tx().is_some_and(|tx| {
            tx.quotas_modified
                || !tx.dropped_tables.is_empty()
                || tx.deferred_flush
                || tx.deferred_compact
                || mem_should_flush
        })
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
        let mut clear_empty_overlay = false;
        let mut need_drain = false;
        for attempt in 0..2 {
            let has_metadata_changes = self.tx_has_metadata_changes();
            let (effects, _) = match self.apply_tx_event(TxEvent::PlanCommit {
                conn_id: None,
                defer_sync_allowed: false,
                can_group: false,
                has_metadata_changes,
                buf_len_exceeds_chunk: false,
            }) {
                Ok(res) => res,
                Err(err) => {
                    let io_err = self.tx_error_to_io(err);
                    let _ = self.rollback_transaction();
                    return Err(io_err);
                }
            };
            if effects.iter().next().is_none() {
                return Ok(());
            }
            let mut need_prepare = None;
            clear_empty_overlay = false;
            need_drain = false;
            for eff in effects.iter() {
                match eff {
                    TxEffect::DecDepthDone => return Ok(()),
                    TxEffect::EnsurePreparedBeforeCommit {
                        effective_durability,
                    } if attempt == 0 => {
                        need_prepare = Some(effective_durability);
                    }
                    TxEffect::ClearEmptyOverlay => clear_empty_overlay = true,
                    TxEffect::DrainPendingGroup => need_drain = true,
                    _ => unreachable!("unexpected effect from commit PlanCommit"),
                }
            }
            let Some(durability) = need_prepare else {
                break;
            };
            if let Err(e) = self.prepare_commit_transaction(durability) {
                let _ = self.rollback_transaction();
                return Err(e);
            }
        }
        if clear_empty_overlay {
            if let Some(tx) = self.tx_state.active_tx_mut() {
                tx.overlay.clear();
            }
        }
        if need_drain {
            if let Err(e) = self.drain_pending_durable() {
                let _ = self.rollback_transaction();
                return Err(e);
            }
        }
        self.enter_wal_flush_and_write(false)?;
        let (effects, taken) = self
            .apply_tx_event(TxEvent::CompleteWalFlush {
                mode: FlushCompleteMode::PublishImmediate,
            })
            .map_err(|e| self.tx_error_to_io(e))?;
        debug_assert!(
            effects
                .iter()
                .any(|e| matches!(e, TxEffect::PublishCommittedTx))
        );
        if let Some(data) = taken {
            self.publish_committed_transaction_data(data);
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
        let can_group = self.capped_tables.is_empty()
            && !self.memtable.should_flush()
            && !self.has_pending_corrupt_evictions()
            && self.tx_state.active_tx().is_some_and(|tx| {
                tx.dropped_tables.is_empty()
                    && !tx.quotas_modified
                    && !tx.deferred_flush
                    && !tx.deferred_compact
            });
        let buf_len_exceeds_chunk = self
            .tx_state
            .active_tx()
            .is_some_and(|tx| tx.pending_buf.len() > MAX_WAL_CHUNK_BYTES);
        let (effects, _) = match self.apply_tx_event(TxEvent::PlanCommit {
            conn_id: Some(conn_id),
            defer_sync_allowed: true,
            can_group,
            has_metadata_changes: false,
            buf_len_exceeds_chunk,
        }) {
            Ok(res) => res,
            Err(TxError::Busy) => return Err(self.tx_error_to_io(TxError::Busy)),
            Err(err) => {
                let io_err = self.tx_error_to_io(err);
                let _ = self.rollback_transaction();
                return Err(io_err);
            }
        };
        for eff in effects.iter() {
            match eff {
                TxEffect::FallbackFullCommit => {
                    self.commit_transaction()?;
                    return Ok(CommitStep::Done);
                }
                TxEffect::DrainPendingGroup => {
                    if let Err(e) = self.drain_pending_durable() {
                        let _ = self.rollback_transaction();
                        return Err(e);
                    }
                }
                _ => unreachable!("unexpected effect in begin_commit_for_conn PlanCommit"),
            }
        }

        self.enter_wal_flush_and_write(true)?;

        let mode = if can_group {
            FlushCompleteMode::HandOffGroup
        } else {
            FlushCompleteMode::AwaitSync
        };
        let (effects, taken) = self
            .apply_tx_event(TxEvent::CompleteWalFlush { mode })
            .map_err(|e| self.tx_error_to_io(e))?;
        match effects.iter().next() {
            Some(TxEffect::ReturnAwaitingSync) => {
                Ok(CommitStep::AwaitingSync(self.sync_wal().file_handle()))
            }
            Some(TxEffect::HandOffPendingGroup) => {
                let mut data = taken.expect("HandOffPendingGroup must take ActiveTxData");
                let lsn = self.next_lsn - 1;
                let own_entries = core::mem::take(&mut data.overlay.map);
                debug_assert!(
                    own_entries
                        .values()
                        .all(|entry| entry.lsn >= data.start_meta.next_lsn)
                );
                self.pending_group.push_back((lsn, own_entries));
                self.commit_gate.note_appended(lsn);
                self.release_tx_slot(data.pending_buf, data.unique_marker_modified);
                Ok(CommitStep::AwaitingGroup {
                    lsn,
                    file: self.sync_wal().file_handle(),
                    gate: Arc::clone(&self.commit_gate),
                })
            }
            _ => unreachable!("unexpected effect in CompleteWalFlush"),
        }
    }

    /// Completes a commit started by [`Self::begin_commit_for_conn`] with the flush result.
    ///
    /// A failed flush leaves the durability of the batch unknown, so the engine is poisoned and
    /// the transaction is discarded from memory.
    pub(crate) fn finish_commit_for_conn(&mut self, flushed: io::Result<()>) -> io::Result<()> {
        let ok = flushed.is_ok();
        let (effects, taken) = match self.apply_tx_event(TxEvent::FinishSyncCommit { ok }) {
            Ok(res) => res,
            Err(TxError::Poisoned) => {
                let poison_err = self.tx_error_to_io(TxError::Poisoned);
                if self.tx_state.is_in_tx() {
                    let _ = self.rollback_transaction();
                }
                return Err(poison_err);
            }
            Err(err) => return Err(self.tx_error_to_io(err)),
        };
        if effects
            .iter()
            .any(|e| matches!(e, TxEffect::PublishCommittedTx))
        {
            self.wal_links.sync_flushed_all();
            if let Some(data) = taken {
                self.publish_committed_transaction_data(data);
            }
            return Ok(());
        }
        let err = flushed.expect_err("non-publish FinishSyncCommit must carry flush error");
        let poisoned_err = self.storage.poison(err);
        if let Some(data) = taken {
            self.restore_aborted_tx_data(data);
        }
        Err(poisoned_err)
    }

    /// Publishes every group-committed batch up to `target` after the leader's device flush.
    ///
    /// A failed flush poisons the engine and fails every commit waiting on the group.
    pub(crate) fn finish_group_flush(
        &mut self,
        target: u64,
        flushed: io::Result<()>,
    ) -> io::Result<()> {
        if self.commit_gate.is_durable_through(target) {
            self.commit_gate.mark_durable(target, true);
            if let Err(e) = flushed {
                let _ = self.poison(e);
            } else if !self.is_poisoned() {
                self.run_due_auto_compaction();
            }
            return Ok(());
        }
        match flushed {
            Ok(()) => {
                if let Err(e) = self.check_not_poisoned() {
                    self.commit_gate.fail(&e);
                    return Err(e);
                }
                // Only batches up to `target` were written when the flush started.
                self.wal_links.sync_flushed(target);
                self.absorb_durable_through(target);
                self.commit_gate.mark_durable(target, true);
                // Safe point unless another transaction is active (checked by the callee).
                self.run_due_auto_compaction();
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
        self.check_not_poisoned()?;
        if self.pending_group.is_empty() {
            return Ok(());
        }
        let gate = Arc::clone(&self.commit_gate);
        let sync_res = {
            let _sync_guard = match gate.lock_sync() {
                Ok(g) => g,
                Err(e) => {
                    gate.fail(&e);
                    return Err(self.poison(e));
                }
            };
            // Re-check after acquiring `sync_mu` in case a concurrent group leader failed its
            // `sync_barrier` right before releasing `sync_mu`.
            self.check_not_poisoned()?;
            let Some(&(target, _)) = self.pending_group.back() else {
                return Ok(());
            };
            let res = if !gate.is_durable_through(target) {
                self.sync_wal().file_handle().sync_barrier()
            } else {
                Ok(())
            };
            if let Err(ref e) = res {
                gate.fail(e);
            }
            res.map(|()| target)
        };
        let target = match sync_res {
            Ok(t) => t,
            Err(e) => return Err(self.poison(e)),
        };
        // The barrier ran under both `engine` and `sync_mu`, after every append to the file.
        self.wal_links.sync_flushed_all();
        self.absorb_durable_through(target);
        self.commit_gate.mark_durable(target, false);
        Ok(())
    }

    fn bump_table_epochs_for_ids(&mut self, tids: &[u16]) {
        for &tid in tids {
            self.bump_table_mutation_epoch(tid);
        }
    }

    fn collect_table_ids_from_overlay_map(
        map: &BTreeMap<Vec<u8>, crate::memtable::MemEntry>,
    ) -> Vec<u16> {
        let mut tids: Vec<u16> = Vec::new();
        let mut last_tid: Option<u16> = None;
        for key in map.keys() {
            if key.len() < 3 {
                continue;
            }
            let tid = u16::from_be_bytes([key[0], key[1]]);
            if tid != 0
                && last_tid != Some(tid)
                && matches!(
                    key[2],
                    crate::lru::NS_PRIMARY_ROW
                        | super::rowid::NS_ROWID_BY_PK
                        | super::rowid::NS_PK_BY_ROWID
                )
            {
                last_tid = Some(tid);
                tids.push(tid);
            }
        }
        tids
    }

    fn collect_table_ids_from_undo_suffix(
        undo: &[(Vec<u8>, Option<MemEntry>)],
        undo_target_len: usize,
    ) -> Vec<u16> {
        let mut tids: Vec<u16> = Vec::new();
        let mut last_tid: Option<u16> = None;
        for (key, _) in &undo[undo_target_len..] {
            if key.len() < 3 {
                continue;
            }
            let tid = u16::from_be_bytes([key[0], key[1]]);
            if tid != 0
                && last_tid != Some(tid)
                && matches!(
                    key[2],
                    crate::lru::NS_PRIMARY_ROW
                        | super::rowid::NS_ROWID_BY_PK
                        | super::rowid::NS_PK_BY_ROWID
                )
            {
                last_tid = Some(tid);
                if !tids.contains(&tid) {
                    tids.push(tid);
                }
            }
        }
        tids
    }

    fn absorb_durable_through(&mut self, target: u64) {
        self.mutation_epoch = self.mutation_epoch.wrapping_add(1);
        while self
            .pending_group
            .front()
            .is_some_and(|(lsn, _)| *lsn <= target)
        {
            if let Some((_, entries)) = self.pending_group.pop_front() {
                if !entries.is_empty() {
                    self.data_generation += 1;
                    let tids = Self::collect_table_ids_from_overlay_map(&entries);
                    self.bump_table_epochs_for_ids(&tids);
                }
                self.invalidate_snapshot_memtable();
                self.memtable.absorb(entries);
            }
        }
    }

    /// Releases buffer capacity, updates the UNIQUE marker generation if modified, and notifies
    /// waiting writers after transitioning `tx_state` to `TxState::Idle`.
    fn release_tx_slot(&mut self, pending_buf: Vec<u8>, unique_marker_modified: bool) {
        self.recycle_tx_buf(pending_buf);
        if unique_marker_modified {
            self.bump_unique_marker_gen();
        }
        self.commit_gate.tx_ended();
    }

    /// Returns the gate that coordinates waiting writers and group commits.
    pub(crate) fn commit_gate(&self) -> Arc<CommitGate> {
        Arc::clone(&self.commit_gate)
    }

    /// Makes a durably logged transaction visible and runs the post-commit maintenance.
    fn publish_committed_transaction_data(&mut self, mut tx: Box<ActiveTxData>) {
        self.mutation_epoch = self.mutation_epoch.wrapping_add(1);
        // The WAL batch(es) are durably appended: publish the overlay into the MemTable by
        // moving its entries (no clones). On the failure path above the overlay is discarded
        // and the MemTable never saw any of the transaction's data.
        let overlay = core::mem::take(&mut tx.overlay.map);
        debug_assert!(
            overlay
                .values()
                .all(|entry| entry.lsn >= tx.start_meta.next_lsn)
        );
        if !overlay.is_empty() {
            self.data_generation += 1;
        }
        self.invalidate_snapshot_memtable();
        self.memtable.absorb(overlay);
        for (tid, pk, max_tick) in tx.pending_lru_clears {
            self.access_tracker.clear_if_tick_le(tid, &pk, max_tick);
        }
        let dropped_tables = tx.dropped_tables;
        let quotas_modified = tx.quotas_modified;
        let do_compact = tx.deferred_compact;
        let do_flush = tx.deferred_flush;
        self.release_tx_slot(tx.pending_buf, tx.unique_marker_modified);

        let capped_table_ids: Vec<u16> = self.capped_tables.keys().copied().collect();
        for tid in capped_table_ids {
            if self.is_poisoned() {
                break;
            }
            if let Err(e) = self.reclaim_fully_dead_lru_blob_segments(tid) {
                self.record_maintenance_failure(&e);
            }
        }

        let mut dropped_any_capped = false;
        for (tid, was_capped) in dropped_tables {
            if self.is_poisoned() {
                break;
            }
            let tid_reused =
                self.active_vtab_ids.contains_key(&tid) || self.capped_tables.contains_key(&tid);
            let tid_recapped = self.capped_tables.contains_key(&tid);
            if !tid_reused {
                self.table_has_synthetic_rowids.remove(&tid);
            }
            if !tid_recapped {
                self.access_tracker.remove_table(tid);
                if self.active_lru_seq_id(tid).is_some() {
                    if let Err(e) = self.storage.try_healthy(&self.commit_gate).and_then(|h| {
                        h.sync_and_remove_dropped_lru_writer(tid, &mut self.lru_blobs)
                    }) {
                        self.record_maintenance_failure(&e);
                        break;
                    }
                }
                for (&(t, _), b) in &mut self.lru_blobs {
                    if t == tid {
                        b.dead_bytes = b.file_len;
                    }
                }
            }
            if let Err(e) = self.reclaim_fully_dead_lru_blob_segments(tid) {
                self.record_maintenance_failure(&e);
                if self.is_poisoned() {
                    break;
                }
            }
            if was_capped {
                dropped_any_capped = true;
            }
        }
        if !self.is_poisoned() {
            let maintenance = if do_compact {
                self.auto_compaction_due = false;
                self.compact()
            } else if do_flush || self.memtable.should_flush() {
                self.flush_memtable()
            } else {
                Ok(())
            };
            if let Err(e) = maintenance {
                self.record_maintenance_failure(&e);
            }
        }
        if (dropped_any_capped || quotas_modified) && !self.is_poisoned() {
            let mut quota_metas = self.build_quota_metas();
            quota_metas.sort_by_key(|q| q.table_id);
            let mut stored = self.meta_mgr().state().table_quotas.clone();
            stored.sort_by_key(|q| q.table_id);
            if stored != quota_metas {
                // Persist quota additions/drops/changes even when deferred flush/compact ran on
                // an empty MemTable (where `flush_memtable` and `compact` return early). Sync any
                // buffered WAL entries first, matching `register_capped_table`, so a power cut
                // cannot leave unverified byte totals.
                let sync_res = if self.last_buf_append_lsn > self.last_buf_synced_lsn {
                    self.make_buf_wal_durable(true)
                } else {
                    Ok(())
                };
                if let Err(e) = sync_res.and_then(|()| {
                    self.healthy()?.commit_meta_update(|st| {
                        st.table_quotas = quota_metas;
                    })
                }) {
                    self.record_maintenance_failure(&e);
                }
            }
        }
        // Safe point: the commit's batch is appended and `next_lsn` has advanced.
        self.run_due_auto_compaction();

        // The transaction is durable at this point; remove rows queued by `Evict`-policy
        // reads. Best effort: a failure here must not turn a successful commit into an error
        // (the rows are simply re-detected and re-queued by the next read).
        if !self.is_poisoned() {
            let _ = self.drain_corrupt_evictions();
        }
    }

    /// Commits an explicit transaction for SQLite connection `conn_id`.
    pub fn commit_transaction_for_conn(&mut self, conn_id: usize) -> io::Result<()> {
        self.check_write_allowed_for_conn(conn_id)?;
        self.commit_transaction()
    }

    fn restore_aborted_tx_data(&mut self, data: Box<ActiveTxData>) {
        let tids = Self::collect_table_ids_from_overlay_map(&data.overlay.map);
        self.bump_table_epochs_for_ids(&tids);
        self.restore_engine_meta_from_snapshot(&data.start_meta);
        let had_deferred = data.deferred_flush || data.deferred_compact;
        self.release_tx_slot(data.pending_buf, data.unique_marker_modified);
        if had_deferred
            && !self.is_poisoned()
            && (self.compaction_pending()
                || self.has_fragmented_std_blobs(0.5)
                || self.has_fully_dead_std_blobs())
        {
            if let Some(hook) = &self.compaction_hook {
                hook();
            }
        }
    }

    fn rollback_inner(&mut self, conn_id: Option<usize>) -> io::Result<()> {
        self.mutation_epoch = self.mutation_epoch.wrapping_add(1);
        let (effects, taken) = self
            .apply_tx_event(TxEvent::Rollback { conn_id })
            .map_err(|e| self.tx_error_to_io(e))?;
        if effects
            .iter()
            .any(|e| matches!(e, TxEffect::PoisonEngineInterruptedSync))
        {
            let _ = self
                .storage
                .poison(io::Error::other("WAL sync interrupted before completion"));
        }
        if let Some(data) = taken {
            self.restore_aborted_tx_data(data);
        }
        Ok(())
    }

    /// Aborts an explicit batch transaction (`ROLLBACK`), discarding all buffered `pending_buf`
    /// mutations and restoring `MemTable`, `capped_tables`, and blob `dead_bytes` in
    /// $O(\text{tx mutations})$ time via the in-memory undo log.
    ///
    /// SQLite invokes `xRollback` once per virtual table that took part in the transaction, so a
    /// rollback with no active transaction is a no-op: there is no start snapshot to restore.
    pub fn rollback_transaction(&mut self) -> io::Result<()> {
        self.rollback_inner(None)
    }

    /// Aborts an explicit transaction for SQLite connection `conn_id`.
    pub fn rollback_transaction_for_conn(&mut self, conn_id: usize) -> io::Result<()> {
        self.rollback_inner(Some(conn_id))
    }

    fn savepoint_inner(
        &mut self,
        owner_conn: Option<usize>,
        sp_id: i32,
        is_statement: bool,
        table_id: u16,
        check_poison: bool,
    ) -> io::Result<()> {
        let (effects, _) = self
            .apply_tx_event(TxEvent::Savepoint {
                owner_conn,
                sp_id,
                is_statement,
                check_poison,
            })
            .map_err(|e| self.tx_error_to_io(e))?;
        for eff in effects.iter() {
            if let TxEffect::CaptureSavepointPayload {
                is_statement,
                zero_buf_offset,
                ..
            } = eff
            {
                let (buf_offset, undo_len, meta) = {
                    let tx = self
                        .tx_state
                        .active_tx()
                        .expect("savepoint must have an active transaction");
                    let buf_offset = if zero_buf_offset {
                        0
                    } else {
                        tx.pending_buf.len()
                    };
                    let undo_len = tx.overlay.undo.len();
                    let meta = if is_statement {
                        SavepointMeta::Statement(StatementMetaSnapshot {
                            next_lsn: self.next_lsn,
                            table_id,
                            capped_table: self.capped_tables.get(&table_id).cloned(),
                            table_has_synthetic_rowids: self
                                .table_has_synthetic_rowids
                                .get(&table_id)
                                .copied(),
                            table_has_row_meta: self.table_has_row_meta.get(&table_id).copied(),
                            blob_undo_len: tx.statement_blob_undo.len(),
                            lru_clears_len: tx.pending_lru_clears.len(),
                        })
                    } else {
                        SavepointMeta::Full(Box::new(self.capture_savepoint_tx_meta_snapshot(tx)))
                    };
                    (buf_offset, undo_len, meta)
                };
                if let Some(tx) = self.tx_state.active_tx_mut() {
                    tx.savepoint_payloads.push(SavepointPayload {
                        buf_offset,
                        undo_len,
                        meta,
                    });
                }
                debug_assert_eq!(
                    self.tx_state.core().map_or(0, |c| c.savepoints().len()),
                    self.tx_state
                        .any_tx()
                        .map_or(0, |d| d.savepoint_payloads.len())
                );
            }
        }
        Ok(())
    }

    /// Opens a transaction savepoint (`xSavepoint`).
    ///
    /// SQLite announces a savepoint to a virtual table only once the table has joined the
    /// transaction, and then only the innermost open one (and none at all when the only open
    /// savepoint is the one that started the transaction). The savepoints below it were opened
    /// before the table had any state in the transaction, so each of them stands for the
    /// transaction's start (see [`Self::rollback_to_savepoint`]).
    pub fn savepoint(&mut self, sp_id: i32) {
        let _ = self.savepoint_inner(None, sp_id, false, 0, false);
    }

    /// Opens a lightweight statement-scoped savepoint for a single `vtab_update` call on
    /// `table_id`, capturing only the per-statement fields an `xUpdate` can mutate.
    pub(crate) fn statement_savepoint_for_conn(
        &mut self,
        conn_id: usize,
        table_id: u16,
    ) -> io::Result<()> {
        self.savepoint_inner(Some(conn_id), STATEMENT_SAVEPOINT, true, table_id, true)
    }

    /// Opens a transaction savepoint for SQLite connection `conn_id`.
    pub fn savepoint_for_conn(&mut self, conn_id: usize, sp_id: i32) -> io::Result<()> {
        self.savepoint_inner(Some(conn_id), sp_id, false, 0, true)
    }

    fn release_savepoint_inner(&mut self, conn_id: Option<usize>, sp_id: i32) -> io::Result<()> {
        let (effects, _) = self
            .apply_tx_event(TxEvent::ReleaseSavepoint { conn_id, sp_id })
            .map_err(|e| self.tx_error_to_io(e))?;
        let mut commit_implicit = false;
        for eff in effects.iter() {
            match eff {
                TxEffect::TruncateSavepointPayloads {
                    new_len,
                    clear_overlay_undo,
                    clear_stmt_blob_undo,
                } => {
                    if let Some(tx) = self.tx_state.active_tx_mut() {
                        tx.savepoint_payloads.truncate(new_len);
                        if clear_overlay_undo {
                            tx.overlay.undo.clear();
                        }
                        if clear_stmt_blob_undo {
                            tx.statement_blob_undo.clear();
                        }
                    }
                    debug_assert_eq!(
                        self.tx_state.core().map_or(0, |c| c.savepoints().len()),
                        self.tx_state
                            .any_tx()
                            .map_or(0, |d| d.savepoint_payloads.len())
                    );
                }
                TxEffect::CommitImplicitTx => commit_implicit = true,
                _ => unreachable!("unexpected effect in ReleaseSavepoint"),
            }
        }
        if commit_implicit {
            self.commit_transaction()?;
        }
        Ok(())
    }

    /// Releases savepoints `>= sp_id` (`xRelease`).
    ///
    /// A release of an id the engine never saw (a savepoint opened before the table joined the
    /// transaction) removes every savepoint it knows with a higher id.
    pub fn release_savepoint(&mut self, sp_id: i32) -> io::Result<()> {
        self.release_savepoint_inner(None, sp_id)
    }

    /// Releases savepoints `>= sp_id` for SQLite connection `conn_id`.
    pub fn release_savepoint_for_conn(&mut self, conn_id: usize, sp_id: i32) -> io::Result<()> {
        self.release_savepoint_inner(Some(conn_id), sp_id)
    }

    /// Rolls back all mutations since savepoint `sp_id` (`xRollbackTo`) in
    /// $O(\text{mutations since savepoint})$ time via the undo log. The savepoint stays open and
    /// every savepoint above it is discarded.
    ///
    /// An id the engine was never told about belongs to a savepoint that SQLite opened before the
    /// table joined the transaction. It is then below every savepoint the engine knows, and its
    /// state is the start of the transaction: everything is undone and the savepoint stays
    /// open. An unknown id above a known one cannot come from SQLite, which announces every
    /// savepoint opened after the table joined; it is an error, so that a rollback is never
    /// silently skipped.
    pub fn rollback_to_savepoint(&mut self, sp_id: i32) -> io::Result<()> {
        self.rollback_to_savepoint_inner(None, sp_id)
    }

    fn rollback_to_savepoint_inner(
        &mut self,
        conn_id: Option<usize>,
        sp_id: i32,
    ) -> io::Result<()> {
        self.mutation_epoch = self.mutation_epoch.wrapping_add(1);
        let (effects, _) =
            match self.apply_tx_event(TxEvent::RollbackToSavepoint { conn_id, sp_id }) {
                Ok(res) => res,
                Err(err @ TxError::UnknownSavepoint(_)) => {
                    if self
                        .tx_state
                        .any_tx()
                        .is_some_and(|tx| tx.unique_marker_modified)
                    {
                        self.bump_unique_marker_gen();
                    }
                    return Err(self.tx_error_to_io(err));
                }
                Err(err) => return Err(self.tx_error_to_io(err)),
            };
        for eff in effects.iter() {
            match eff {
                TxEffect::BumpUniqueGenIfModified => {
                    if self
                        .tx_state
                        .any_tx()
                        .is_some_and(|tx| tx.unique_marker_modified)
                    {
                        self.bump_unique_marker_gen();
                    }
                }
                TxEffect::RestoreTxToStart => {
                    self.restore_tx_payload_to_start();
                }
                TxEffect::RestoreSavepointPayload {
                    index,
                    clear_pending_buf,
                } => {
                    self.restore_savepoint_payload(index, clear_pending_buf);
                }
                _ => unreachable!("unexpected effect in RollbackToSavepoint"),
            }
        }
        debug_assert_eq!(
            self.tx_state.core().map_or(0, |c| c.savepoints().len()),
            self.tx_state
                .any_tx()
                .map_or(0, |d| d.savepoint_payloads.len())
        );
        Ok(())
    }

    /// Undoes everything in `ActiveTxData` back to transaction start while keeping it open.
    fn restore_tx_payload_to_start(&mut self) {
        self.mutation_epoch = self.mutation_epoch.wrapping_add(1);
        let (tids, start_meta) = {
            let Some(tx) = self.tx_state.any_tx() else {
                return;
            };
            (
                Self::collect_table_ids_from_overlay_map(&tx.overlay.map),
                tx.start_meta.clone(),
            )
        };
        self.bump_table_epochs_for_ids(&tids);
        self.restore_engine_meta_from_snapshot(&start_meta);
        if let Some(tx) = self.tx_state.any_tx_mut() {
            tx.savepoint_payloads.clear();
            tx.statement_blob_undo.clear();
            tx.pending_lru_clears.clear();
            tx.pending_buf.clear();
            tx.overlay.clear();
            tx.dropped_tables.truncate(start_meta.dropped_tables_len);
            tx.vtabs_created = start_meta.vtabs_created_in_tx;
            tx.quotas_modified = start_meta.quotas_modified_in_tx;
        }
    }

    fn restore_savepoint_payload(&mut self, index: usize, clear_pending_buf: bool) {
        let Some(payload) = self.tx_state.any_tx_mut().and_then(|tx| {
            tx.savepoint_payloads.truncate(index + 1);
            let p = tx.savepoint_payloads.pop()?;
            if clear_pending_buf {
                tx.pending_buf.clear();
            } else {
                tx.pending_buf.truncate(p.buf_offset);
            }
            Some(p)
        }) else {
            return;
        };
        match &payload.meta {
            SavepointMeta::Statement(st) => {
                self.restore_statement_payload(payload.undo_len, st);
            }
            SavepointMeta::Full(snap) => {
                self.mutation_epoch = self.mutation_epoch.wrapping_add(1);
                let tids = self.tx_state.any_tx().map_or_else(Vec::new, |tx| {
                    Self::collect_table_ids_from_undo_suffix(&tx.overlay.undo, payload.undo_len)
                });
                self.bump_table_epochs_for_ids(&tids);
                if let Some(tx) = self.tx_state.any_tx_mut() {
                    tx.overlay.rollback_to(payload.undo_len);
                }
                self.restore_engine_meta_from_snapshot(snap);
                if let Some(tx) = self.tx_state.any_tx_mut() {
                    tx.dropped_tables.truncate(snap.dropped_tables_len);
                    tx.pending_lru_clears.truncate(snap.lru_clears_len);
                    tx.vtabs_created = snap.vtabs_created_in_tx.clone();
                    tx.quotas_modified = snap.quotas_modified_in_tx;
                }
            }
        }
        let stmt_active = self.tx_state.statement_savepoint_active();
        if let Some(tx) = self.tx_state.any_tx_mut() {
            if !stmt_active {
                tx.statement_blob_undo.clear();
            }
            tx.savepoint_payloads.push(payload);
        }
    }

    fn restore_statement_payload(&mut self, undo_target_len: usize, st: &StatementMetaSnapshot) {
        self.mutation_epoch = self.mutation_epoch.wrapping_add(1);
        let tids = self.tx_state.any_tx().map_or_else(Vec::new, |tx| {
            Self::collect_table_ids_from_undo_suffix(&tx.overlay.undo, undo_target_len)
        });
        self.bump_table_epochs_for_ids(&tids);
        self.next_lsn = st.next_lsn;
        let (Some(tx), std_blobs, lru_blobs) = (
            self.tx_state.any_tx_mut(),
            &mut self.std_blobs,
            &mut self.lru_blobs,
        ) else {
            return;
        };
        tx.overlay.rollback_to(undo_target_len);
        tx.pending_lru_clears.truncate(st.lru_clears_len);
        for entry in tx.statement_blob_undo.drain(st.blob_undo_len..).rev() {
            let (kind, table_id, seg_id, len, was_append) = match entry {
                BlobUndo::Appended {
                    kind,
                    table_id,
                    seg_id,
                    len,
                } => (kind, table_id, seg_id, len, true),
                BlobUndo::MarkedDead {
                    kind,
                    table_id,
                    seg_id,
                    len,
                } => (kind, table_id, seg_id, len, false),
            };
            let dead_bytes = match kind {
                crate::wal::BlobKind::Standard => {
                    std_blobs.get_mut(&seg_id).map(|b| &mut b.dead_bytes)
                }
                crate::wal::BlobKind::Lru => lru_blobs
                    .get_mut(&(table_id, seg_id))
                    .map(|b| &mut b.dead_bytes),
            };
            if let Some(dead) = dead_bytes {
                *dead = if was_append {
                    dead.saturating_add(len)
                } else {
                    dead.saturating_sub(len)
                };
            }
        }
        if let Some(ct) = &st.capped_table {
            self.capped_tables.insert(st.table_id, ct.clone());
        }
        match st.table_has_synthetic_rowids {
            Some(v) => {
                self.table_has_synthetic_rowids.insert(st.table_id, v);
            }
            None => {
                self.table_has_synthetic_rowids.remove(&st.table_id);
            }
        }
        match st.table_has_row_meta {
            Some(v) => {
                self.table_has_row_meta.insert(st.table_id, v);
            }
            None => {
                self.table_has_row_meta.remove(&st.table_id);
            }
        }
    }

    /// Rolls back to savepoint `sp_id` for SQLite connection `conn_id`.
    pub fn rollback_to_savepoint_for_conn(&mut self, conn_id: usize, sp_id: i32) -> io::Result<()> {
        self.rollback_to_savepoint_inner(Some(conn_id), sp_id)
    }

    fn capture_initial_tx_meta_snapshot(&self) -> TxMetaSnapshot {
        self.capture_tx_meta_snapshot_fields(0, 0, Vec::new(), false)
    }

    fn capture_savepoint_tx_meta_snapshot(&self, tx: &ActiveTxData) -> TxMetaSnapshot {
        self.capture_tx_meta_snapshot_fields(
            tx.dropped_tables.len(),
            tx.pending_lru_clears.len(),
            tx.vtabs_created.clone(),
            tx.quotas_modified,
        )
    }

    fn capture_tx_meta_snapshot_fields(
        &self,
        dropped_tables_len: usize,
        lru_clears_len: usize,
        vtabs_created_in_tx: Vec<(u16, String, String)>,
        quotas_modified_in_tx: bool,
    ) -> TxMetaSnapshot {
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
            active_vtab_holders: self.active_vtab_holders.clone(),
            corrupt_policies: self.corrupt_policies.clone(),
            table_inline_thresholds: self.table_inline_thresholds.clone(),
            table_has_synthetic_rowids: self.table_has_synthetic_rowids.clone(),
            table_has_row_meta: self.table_has_row_meta.clone(),
            dropped_tables_len,
            lru_clears_len,
            vtabs_created_in_tx,
            quotas_modified_in_tx,
        }
    }

    fn restore_engine_meta_from_snapshot(&mut self, snap: &TxMetaSnapshot) {
        self.mutation_epoch = self.mutation_epoch.wrapping_add(1);
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
        self.active_vtab_holders = snap.active_vtab_holders.clone();
        self.corrupt_policies = snap.corrupt_policies.clone();
        self.table_inline_thresholds = snap.table_inline_thresholds.clone();
        self.table_has_synthetic_rowids = snap.table_has_synthetic_rowids.clone();
        self.table_has_row_meta = snap.table_has_row_meta.clone();
    }

    fn enter_wal_flush_and_write(&mut self, defer_sync_barrier: bool) -> io::Result<()> {
        let (effects, _) = match self.apply_tx_event(TxEvent::EnterWalFlush { defer_sync_barrier })
        {
            Ok(res) => res,
            Err(err) => {
                let io_err = self.tx_error_to_io(err);
                let _ = self.rollback_transaction();
                return Err(io_err);
            }
        };
        let (durability, record_count, defer) = match effects.iter().next() {
            Some(TxEffect::FlushWal {
                durability,
                record_count,
                defer_sync_barrier,
            }) => (durability, record_count, defer_sync_barrier),
            _ => unreachable!("EnterWalFlush must emit FlushWal"),
        };
        if defer {
            self.storage.set_sync_defer_barrier(true);
        }
        let res = self.flush_pending_tx_records(durability, record_count);
        if defer {
            self.storage.set_sync_defer_barrier(false);
        }
        if let Err(e) = res {
            let _ = self.rollback_transaction();
            return Err(e);
        }
        Ok(())
    }

    fn ensure_wal_sector_headroom(
        &mut self,
        durability: WriteDurability,
        required_sectors: usize,
    ) -> io::Result<()> {
        let (cur_sector, total_sectors) = match durability {
            WriteDurability::SyncTransactional => (
                self.sync_wal().current_sector_idx(),
                self.sync_wal().total_sectors(),
            ),
            WriteDurability::BufferedFast => (
                self.buf_wal().current_sector_idx(),
                self.buf_wal().total_sectors(),
            ),
        };
        let target_sectors = cur_sector.checked_add(required_sectors).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "WAL sector count overflow")
        })?;
        if target_sectors > total_sectors {
            self.healthy()?
                .ensure_wal_min_size(durability, checked_wal_bytes(target_sectors)?)?;
        }
        Ok(())
    }

    fn finalize_and_append_tx_buf(
        &mut self,
        durability: WriteDurability,
        record_count: u32,
        buf: &mut [u8],
    ) -> io::Result<()> {
        // `next_lsn` advances only after the append: a ring-full rotation or flush derives the
        // checkpoint LSN from `next_lsn - 1`, which must stay below this batch's LSN.
        let lsn = self.next_lsn;
        let (prev, durable) = self.wal_links_for_append(durability)?;
        WalBatch::finalize_encoded_buf(lsn, prev, durable, record_count, buf);
        self.append_raw_wal_batch_with_rotation(buf, durability)?;
        self.next_lsn = lsn + 1;
        Ok(())
    }

    fn flush_chunked_tx_records(
        &mut self,
        durability: WriteDurability,
        raw_buf: &[u8],
        chunk_spans: &[(usize, usize, u32)],
    ) -> io::Result<()> {
        let usable = crate::wal::EST_WAL_SECTOR_USABLE_BYTES;
        let mut required_sectors = 0usize;
        // Refuse the entire transaction before the first chunk reaches either log.
        self.lsn_after_batches(chunk_spans.len())?;
        let mut max_chunk_len = 0usize;
        for &(start, end, _) in chunk_spans {
            let chunk_len = crate::wal::WAL_BATCH_HEADER_SIZE + 1 + (end - start);
            required_sectors += (chunk_len / usable) + 2;
            max_chunk_len = max_chunk_len.max(chunk_len);
        }

        let caller_deferred = self.sync_wal().barrier_deferred();
        let chunked_res = (|| -> io::Result<()> {
            self.ensure_wal_sector_headroom(durability, required_sectors)?;

            let mut chunk_buf = Vec::with_capacity(max_chunk_len);
            let last_idx = chunk_spans.len() - 1;
            for (idx, &(start, end, user_records)) in chunk_spans.iter().enumerate() {
                chunk_buf.clear();
                chunk_buf.extend_from_slice(&[0u8; crate::wal::WAL_BATCH_HEADER_SIZE]);
                if idx == 0 {
                    chunk_buf.push(crate::wal::OP_TX_BEGIN);
                } else if idx < last_idx {
                    chunk_buf.push(crate::wal::OP_TX_CONTINUE);
                }
                chunk_buf.extend_from_slice(&raw_buf[start..end]);
                if idx == last_idx {
                    chunk_buf.push(crate::wal::OP_TX_COMMIT);
                }
                let chunk_records = user_records + 1;
                if durability == WriteDurability::SyncTransactional {
                    self.storage.set_sync_defer_barrier(if idx < last_idx {
                        true
                    } else {
                        caller_deferred
                    });
                }
                self.finalize_and_append_tx_buf(durability, chunk_records, &mut chunk_buf)?;
            }
            Ok(())
        })();

        self.storage.set_sync_defer_barrier(caller_deferred);
        chunked_res
    }

    fn flush_pending_tx_records(
        &mut self,
        durability: WriteDurability,
        record_count: u32,
    ) -> io::Result<()> {
        self.check_not_poisoned()?;
        if record_count == 0 {
            return Ok(());
        }
        self.lsn_after_batches(1)?;
        let Some(tx) = self.tx_state.any_tx_mut() else {
            return Ok(());
        };
        let mut raw_buf = core::mem::take(&mut tx.pending_buf);

        let res = (|| -> io::Result<()> {
            if durability == WriteDurability::SyncTransactional {
                self.sync_active_blob_writers()?;
            } else {
                self.flush_active_blob_buffers()?;
            }

            if raw_buf.len() <= MAX_WAL_CHUNK_BYTES {
                return self.finalize_and_append_tx_buf(durability, record_count, &mut raw_buf);
            }

            // Large transaction buffer (> 512 KB): slice at record boundaries into <= 512 KB WAL
            // batches framed by `TxBegin` / `TxContinue` / `TxCommit`, ensuring sufficient capacity
            // up front so all chunks share one WAL epoch with the barrier deferred to `TxCommit`.
            let chunk_spans = WalBatch::plan_encoded_chunks(&raw_buf, MAX_WAL_CHUNK_BYTES);
            if chunk_spans.len() <= 1 {
                // Single oversized record: `raw_buf` already holds the 32-byte header space with
                // no transaction framing byte, so finalize and write it in-place with zero copies.
                debug_assert_eq!(chunk_spans.first().map_or(0, |&(_, _, n)| n), record_count);
                let required_sectors =
                    (raw_buf.len() / crate::wal::EST_WAL_SECTOR_USABLE_BYTES) + 2;
                self.ensure_wal_sector_headroom(durability, required_sectors)?;
                return self.finalize_and_append_tx_buf(durability, record_count, &mut raw_buf);
            }

            self.flush_chunked_tx_records(durability, &raw_buf, &chunk_spans)
        })();

        self.recycle_tx_buf(raw_buf);
        res
    }
}
