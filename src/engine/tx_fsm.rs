//! Pure Sans-I/O transaction lifecycle state machine (`SM-1`).
//!
//! This module owns all phase, nesting-depth, durability-upgrade, participating-vtab, and
//! savepoint-frame transitions for [`super::StorageEngine`]. It contains no filesystem, locking,
//! timing, SQLite FFI, or `unsafe` primitives; callers apply [`TxEvent`] transitions through
//! [`step`] and execute every returned [`TxEffect`] in the imperative shell.

use core::num::NonZeroUsize;

use super::WriteDurability;

/// Explicit write scope for internal vs. user mutations inside an active transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WriteScope {
    /// Normal user writes obeying `max_transaction_bytes` and invalidating prior commit preparation.
    Normal,
    /// Internal recovery/reconcile/drop/eviction writes bypassing `max_transaction_bytes`
    /// without resetting outer commit preparation or durability upgrade state.
    InternalBypass,
}

/// Phase of an active explicit or savepoint-backed transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TxPhase {
    Active,
    Preparing,
    Prepared,
    FlushingWal,
    SyncInFlight,
}

/// Coarse health state tracked inside [`TxMachine`] for `INV-TX-4` enforcement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum HealthState {
    Healthy,
    Poisoned,
}

/// Terminal disposition of a WAL batch flush executed after entering [`TxPhase::FlushingWal`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FlushCompleteMode {
    PublishImmediate,
    AwaitSync,
    HandOffGroup,
}

/// Control-plane metadata for one savepoint frame on [`TxCore`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct SavepointFrame {
    pub(super) sp_id: i32,
    pub(super) is_statement: bool,
    pub(super) record_count: u32,
    pub(super) pending_bytes: usize,
    pub(super) pending_sync: bool,
    pub(super) implicit_tx: bool,
}

/// Pure transaction control state. All fields are private to enforce [`step`] as the sole
/// transition boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct TxCore {
    phase: TxPhase,
    depth: NonZeroUsize,
    owner_conn: Option<usize>,
    pending_record_count: u32,
    pending_bytes: usize,
    pending_sync: bool,
    sync_durability_upgrade: bool,
    vtabs: Vec<usize>,
    synced_vtabs: Vec<usize>,
    savepoints: Vec<SavepointFrame>,
    statement_savepoint_active: bool,
}

impl TxCore {
    fn new(owner_conn: Option<usize>) -> Self {
        Self {
            phase: TxPhase::Active,
            depth: NonZeroUsize::MIN,
            owner_conn,
            pending_record_count: 0,
            pending_bytes: 0,
            pending_sync: false,
            sync_durability_upgrade: false,
            vtabs: Vec::new(),
            synced_vtabs: Vec::new(),
            savepoints: Vec::new(),
            statement_savepoint_active: false,
        }
    }

    /// True while the transaction owns the writer slot and has not entered `FlushingWal`.
    #[inline]
    pub(super) fn is_active(&self) -> bool {
        !matches!(self.phase, TxPhase::FlushingWal)
    }

    #[inline]
    pub(super) fn is_preparing(&self) -> bool {
        self.phase == TxPhase::Preparing
    }

    #[inline]
    pub(super) fn owner_conn(&self) -> Option<usize> {
        self.owner_conn
    }

    #[inline]
    pub(super) fn pending_record_count(&self) -> u32 {
        self.pending_record_count
    }

    #[inline]
    pub(super) fn pending_bytes(&self) -> usize {
        self.pending_bytes
    }

    #[inline]
    pub(super) fn effective_durability(&self, fallback: WriteDurability) -> WriteDurability {
        if self.pending_sync || self.sync_durability_upgrade {
            WriteDurability::SyncTransactional
        } else {
            fallback
        }
    }

    #[inline]
    pub(super) fn savepoints(&self) -> &[SavepointFrame] {
        &self.savepoints
    }

    #[inline]
    pub(super) fn statement_savepoint_active(&self) -> bool {
        self.statement_savepoint_active
    }

    #[inline]
    fn invalidate_prepare(&mut self) {
        self.phase = TxPhase::Active;
        self.synced_vtabs.clear();
        self.sync_durability_upgrade = false;
    }

    #[inline]
    fn refresh_statement_savepoint_active(&mut self) {
        self.statement_savepoint_active = self.savepoints.iter().any(|s| s.is_statement);
    }
}

/// Top-level pure control state (`Idle` or owning a `TxCore`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) enum TxCoreState {
    #[default]
    Idle,
    InTx(TxCore),
}

impl TxCoreState {
    #[inline]
    pub(super) fn is_in_tx(&self) -> bool {
        !matches!(self, Self::Idle)
    }

    #[inline]
    pub(super) fn core(&self) -> Option<&TxCore> {
        match self {
            Self::InTx(core) => Some(core),
            Self::Idle => None,
        }
    }
}

/// Combined health and transaction state accepted and returned by [`step`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct TxMachine {
    pub(super) health: HealthState,
    pub(super) state: TxCoreState,
}

impl TxMachine {
    #[inline]
    pub(super) fn from_parts(health: HealthState, state: TxCoreState) -> Self {
        Self { health, state }
    }
}

/// Pure transition rejection causes returned by [`step`] and [`can_record_write`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TxError {
    Poisoned,
    Busy,
    TxBudgetExceeded,
    TooManyRecords,
    UnknownSavepoint(i32),
    InvalidPhase,
}

/// Input event driving [`step`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TxEvent {
    Begin,
    BeginForConn {
        conn_id: usize,
    },
    BeginVtab {
        conn_id: usize,
        vtab_id: usize,
    },
    AttachVtab {
        conn_id: usize,
        vtab_id: usize,
    },
    DetachVtab {
        conn_id: usize,
        vtab_id: usize,
    },
    VtabSync {
        vtab_id: usize,
        durability: WriteDurability,
    },
    RecordWrite {
        scope: WriteScope,
        durability: WriteDurability,
        record_count: usize,
        batch_bytes: usize,
        max_transaction_bytes: usize,
    },
    InvalidatePrepare,
    BeginPrepare {
        durability: WriteDurability,
    },
    FinishPrepare {
        ok: bool,
    },
    Savepoint {
        owner_conn: Option<usize>,
        sp_id: i32,
        is_statement: bool,
        check_poison: bool,
    },
    ReleaseSavepoint {
        conn_id: Option<usize>,
        sp_id: i32,
    },
    RollbackToSavepoint {
        conn_id: Option<usize>,
        sp_id: i32,
    },
    PlanCommit {
        conn_id: Option<usize>,
        defer_sync_allowed: bool,
        can_group: bool,
        has_metadata_changes: bool,
        buf_len_exceeds_chunk: bool,
    },
    EnterWalFlush {
        defer_sync_barrier: bool,
    },
    CompleteWalFlush {
        mode: FlushCompleteMode,
    },
    FinishSyncCommit {
        ok: bool,
    },
    Rollback {
        conn_id: Option<usize>,
    },
    MarkPoisoned,
}

/// Side effect command emitted by [`step`] for execution by the engine shell.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TxEffect {
    InitTxData,
    AppendTxBatch {
        reset_buf: bool,
        record_undo: bool,
    },
    VtabSyncOutcome {
        all_synced: bool,
    },
    RunPrepare {
        effective_durability: WriteDurability,
    },
    CaptureSavepointPayload {
        sp_id: i32,
        is_statement: bool,
        zero_buf_offset: bool,
    },
    TruncateSavepointPayloads {
        new_len: usize,
        clear_overlay_undo: bool,
        clear_stmt_blob_undo: bool,
    },
    CommitImplicitTx,
    BumpUniqueGenIfModified,
    RestoreTxToStart,
    RestoreSavepointPayload {
        index: usize,
        clear_pending_buf: bool,
    },
    DecDepthDone,
    EnsurePreparedBeforeCommit {
        effective_durability: WriteDurability,
    },
    FallbackFullCommit,
    DrainPendingGroup,
    ClearEmptyOverlay,
    FlushWal {
        durability: WriteDurability,
        record_count: u32,
        defer_sync_barrier: bool,
    },
    PublishCommittedTx,
    ReturnAwaitingSync,
    HandOffPendingGroup,
    PoisonEngineInterruptedSync,
    PoisonEngineSyncFailed,
    RestoreStartMetaAndRelease,
}

/// Fixed-capacity inline effect list emitted by [`step`] with zero heap allocation.
#[must_use = "every TxEffect emitted by tx_fsm::step must be executed by the engine shell"]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(super) struct TxEffects {
    items: [Option<TxEffect>; 4],
    len: usize,
}

impl TxEffects {
    #[inline]
    pub(super) const fn empty() -> Self {
        Self {
            items: [None; 4],
            len: 0,
        }
    }

    #[inline]
    pub(super) const fn one(effect: TxEffect) -> Self {
        Self {
            items: [Some(effect), None, None, None],
            len: 1,
        }
    }

    #[inline]
    pub(super) fn push(&mut self, effect: TxEffect) {
        assert!(self.len < self.items.len(), "TxEffects capacity exceeded");
        self.items[self.len] = Some(effect);
        self.len += 1;
    }

    #[inline]
    pub(super) fn iter(&self) -> impl Iterator<Item = TxEffect> + '_ {
        self.items[..self.len].iter().flatten().copied()
    }
}

/// Read-only pre-flight check matching [`TxEvent::RecordWrite`] validation before touching `pending_buf`.
pub(super) fn can_record_write(
    health: HealthState,
    core: Option<&TxCore>,
    scope: WriteScope,
    record_count: usize,
    batch_bytes: usize,
    max_transaction_bytes: usize,
) -> Result<bool, TxError> {
    if health == HealthState::Poisoned {
        return Err(TxError::Poisoned);
    }
    let Some(core) = core else {
        return Err(TxError::InvalidPhase);
    };
    if !core.is_active() {
        return Err(TxError::InvalidPhase);
    }
    let bypass = scope == WriteScope::InternalBypass || core.is_preparing();
    let next_bytes = core.pending_bytes.saturating_add(batch_bytes);
    if !bypass && max_transaction_bytes > 0 && next_bytes > max_transaction_bytes {
        return Err(TxError::TxBudgetExceeded);
    }
    let next_count = u32::try_from(record_count)
        .ok()
        .and_then(|n| core.pending_record_count.checked_add(n));
    if next_count.is_none() {
        return Err(TxError::TooManyRecords);
    }
    Ok(core.pending_record_count == 0)
}

fn check_active_conn(core: &TxCore, conn_id: Option<usize>) -> Result<(), TxError> {
    if let Some(cid) = conn_id {
        if core.owner_conn != Some(cid) {
            return Err(TxError::Busy);
        }
    }
    Ok(())
}

/// Pure transition function `(TxMachine, TxEvent) -> (TxMachine, Result<TxEffects, TxError>)`.
pub(super) fn step(
    mut machine: TxMachine,
    event: TxEvent,
) -> (TxMachine, Result<TxEffects, TxError>) {
    match event {
        TxEvent::Begin => {
            if let TxCoreState::InTx(ref mut core) = machine.state {
                core.depth = core
                    .depth
                    .checked_add(1)
                    .expect("transaction depth overflow");
                (machine, Ok(TxEffects::empty()))
            } else {
                machine.state = TxCoreState::InTx(TxCore::new(None));
                (machine, Ok(TxEffects::one(TxEffect::InitTxData)))
            }
        }
        TxEvent::BeginForConn { conn_id } => {
            if machine.health == HealthState::Poisoned {
                return (machine, Err(TxError::Poisoned));
            }
            if let TxCoreState::InTx(ref mut core) = machine.state {
                if core.owner_conn != Some(conn_id) {
                    return (machine, Err(TxError::Busy));
                }
                core.depth = core
                    .depth
                    .checked_add(1)
                    .expect("transaction depth overflow");
                (machine, Ok(TxEffects::empty()))
            } else {
                machine.state = TxCoreState::InTx(TxCore::new(Some(conn_id)));
                (machine, Ok(TxEffects::one(TxEffect::InitTxData)))
            }
        }
        TxEvent::BeginVtab { conn_id, vtab_id } => {
            if machine.health == HealthState::Poisoned {
                return (machine, Err(TxError::Poisoned));
            }
            let mut effects = TxEffects::empty();
            match &mut machine.state {
                TxCoreState::Idle => {
                    let mut core = TxCore::new(Some(conn_id));
                    core.vtabs.push(vtab_id);
                    machine.state = TxCoreState::InTx(core);
                    effects.push(TxEffect::InitTxData);
                }
                TxCoreState::InTx(core) if core.is_active() => {
                    if core.owner_conn != Some(conn_id) {
                        return (machine, Err(TxError::Busy));
                    }
                    if !core.vtabs.contains(&vtab_id) {
                        core.vtabs.push(vtab_id);
                        core.synced_vtabs.clear();
                        core.sync_durability_upgrade = false;
                    }
                }
                TxCoreState::InTx(_) => {
                    return (machine, Err(TxError::InvalidPhase));
                }
            }
            (machine, Ok(effects))
        }
        TxEvent::AttachVtab { conn_id, vtab_id } => {
            if let TxCoreState::InTx(ref mut core) = machine.state {
                if core.is_active()
                    && core.owner_conn == Some(conn_id)
                    && !core.vtabs.contains(&vtab_id)
                {
                    core.vtabs.push(vtab_id);
                    core.synced_vtabs.clear();
                    core.sync_durability_upgrade = false;
                }
            }
            (machine, Ok(TxEffects::empty()))
        }
        TxEvent::DetachVtab { conn_id, vtab_id } => {
            if let TxCoreState::InTx(ref mut core) = machine.state {
                if core.is_active() && core.owner_conn == Some(conn_id) {
                    core.vtabs.retain(|&id| id != vtab_id);
                    core.synced_vtabs.retain(|&id| id != vtab_id);
                }
            }
            (machine, Ok(TxEffects::empty()))
        }
        TxEvent::VtabSync {
            vtab_id,
            durability,
        } => {
            let TxCoreState::InTx(ref mut core) = machine.state else {
                return (
                    machine,
                    Ok(TxEffects::one(TxEffect::VtabSyncOutcome {
                        all_synced: true,
                    })),
                );
            };
            if !core.is_active() {
                return (
                    machine,
                    Ok(TxEffects::one(TxEffect::VtabSyncOutcome {
                        all_synced: true,
                    })),
                );
            }
            if durability == WriteDurability::SyncTransactional {
                core.sync_durability_upgrade = true;
            }
            if !core.synced_vtabs.contains(&vtab_id) {
                core.synced_vtabs.push(vtab_id);
            }
            let all_synced = core.vtabs.iter().all(|id| core.synced_vtabs.contains(id));
            if all_synced {
                core.synced_vtabs.clear();
            }
            (
                machine,
                Ok(TxEffects::one(TxEffect::VtabSyncOutcome { all_synced })),
            )
        }
        TxEvent::RecordWrite {
            scope,
            durability,
            record_count,
            batch_bytes,
            max_transaction_bytes,
        } => {
            let reset_buf = match can_record_write(
                machine.health,
                machine.state.core(),
                scope,
                record_count,
                batch_bytes,
                max_transaction_bytes,
            ) {
                Ok(reset) => reset,
                Err(e) => return (machine, Err(e)),
            };
            let TxCoreState::InTx(ref mut core) = machine.state else {
                unreachable!("validated active transaction in can_record_write");
            };
            let bypass = scope == WriteScope::InternalBypass || core.is_preparing();
            if !bypass {
                core.invalidate_prepare();
            }
            core.pending_bytes = core.pending_bytes.saturating_add(batch_bytes);
            core.pending_record_count +=
                u32::try_from(record_count).expect("validated record_count in can_record_write");
            if durability == WriteDurability::SyncTransactional {
                core.pending_sync = true;
            }
            let record_undo = !core.savepoints.is_empty();
            (
                machine,
                Ok(TxEffects::one(TxEffect::AppendTxBatch {
                    reset_buf,
                    record_undo,
                })),
            )
        }
        TxEvent::InvalidatePrepare => {
            if let TxCoreState::InTx(ref mut core) = machine.state {
                if core.is_active() {
                    core.invalidate_prepare();
                }
            }
            (machine, Ok(TxEffects::empty()))
        }
        TxEvent::BeginPrepare { durability } => {
            let TxCoreState::InTx(ref mut core) = machine.state else {
                return (machine, Ok(TxEffects::empty()));
            };
            if !core.is_active() {
                return (machine, Ok(TxEffects::empty()));
            }
            if machine.health == HealthState::Poisoned {
                return (machine, Err(TxError::Poisoned));
            }
            let requested_sync =
                durability == WriteDurability::SyncTransactional || core.sync_durability_upgrade;
            let need_sync_upgrade = requested_sync && !core.pending_sync;
            if core.phase == TxPhase::Prepared && !need_sync_upgrade {
                return (machine, Ok(TxEffects::empty()));
            }
            if requested_sync {
                core.pending_sync = true;
            }
            let effective = if core.pending_sync {
                WriteDurability::SyncTransactional
            } else {
                WriteDurability::BufferedFast
            };
            core.phase = TxPhase::Preparing;
            (
                machine,
                Ok(TxEffects::one(TxEffect::RunPrepare {
                    effective_durability: effective,
                })),
            )
        }
        TxEvent::FinishPrepare { ok } => {
            if let TxCoreState::InTx(ref mut core) = machine.state {
                core.phase = if ok {
                    TxPhase::Prepared
                } else {
                    TxPhase::Active
                };
            }
            (machine, Ok(TxEffects::empty()))
        }
        TxEvent::Savepoint {
            owner_conn,
            sp_id,
            is_statement,
            check_poison,
        } => {
            if check_poison && machine.health == HealthState::Poisoned {
                return (machine, Err(TxError::Poisoned));
            }
            let mut effects = TxEffects::empty();
            match &mut machine.state {
                TxCoreState::InTx(core) if core.is_active() => {
                    if let Err(e) = check_active_conn(core, owner_conn) {
                        return (machine, Err(e));
                    }
                    core.invalidate_prepare();
                    if core.savepoints.iter().any(|s| s.sp_id == sp_id) {
                        return (machine, Ok(effects));
                    }
                    let zero_buf_offset = core.pending_record_count == 0;
                    core.savepoints.push(SavepointFrame {
                        sp_id,
                        is_statement,
                        record_count: core.pending_record_count,
                        pending_bytes: core.pending_bytes,
                        pending_sync: core.pending_sync,
                        implicit_tx: false,
                    });
                    core.refresh_statement_savepoint_active();
                    effects.push(TxEffect::CaptureSavepointPayload {
                        sp_id,
                        is_statement,
                        zero_buf_offset,
                    });
                }
                TxCoreState::Idle => {
                    let mut core = TxCore::new(owner_conn);
                    core.savepoints.push(SavepointFrame {
                        sp_id,
                        is_statement,
                        record_count: 0,
                        pending_bytes: 0,
                        pending_sync: false,
                        implicit_tx: true,
                    });
                    core.refresh_statement_savepoint_active();
                    machine.state = TxCoreState::InTx(core);
                    effects.push(TxEffect::InitTxData);
                    effects.push(TxEffect::CaptureSavepointPayload {
                        sp_id,
                        is_statement,
                        zero_buf_offset: true,
                    });
                }
                TxCoreState::InTx(_) => return (machine, Err(TxError::InvalidPhase)),
            }
            (machine, Ok(effects))
        }
        TxEvent::ReleaseSavepoint { conn_id, sp_id } => {
            let TxCoreState::InTx(ref mut core) = machine.state else {
                return (machine, Ok(TxEffects::empty()));
            };
            if !core.is_active() {
                return (machine, Ok(TxEffects::empty()));
            }
            if let Err(e) = check_active_conn(core, conn_id) {
                return (machine, Err(e));
            }
            let pos = core
                .savepoints
                .iter()
                .position(|s| s.sp_id == sp_id)
                .or_else(|| core.savepoints.iter().position(|s| s.sp_id > sp_id));
            let mut effects = TxEffects::empty();
            if let Some(pos) = pos {
                let implicit_tx = core.savepoints[pos].implicit_tx;
                core.savepoints.truncate(pos);
                core.refresh_statement_savepoint_active();
                let clear_overlay_undo = core.savepoints.is_empty();
                let clear_stmt_blob_undo = !core.statement_savepoint_active;
                effects.push(TxEffect::TruncateSavepointPayloads {
                    new_len: pos,
                    clear_overlay_undo,
                    clear_stmt_blob_undo,
                });
                if implicit_tx && core.depth.get() == 1 {
                    effects.push(TxEffect::CommitImplicitTx);
                }
            }
            (machine, Ok(effects))
        }
        TxEvent::RollbackToSavepoint { conn_id, sp_id } => {
            if let TxCoreState::InTx(ref core) = machine.state {
                if core.is_active() {
                    if let Err(e) = check_active_conn(core, conn_id) {
                        return (machine, Err(e));
                    }
                } else if !matches!(core.phase, TxPhase::FlushingWal) {
                    return (machine, Ok(TxEffects::empty()));
                }
            }
            let TxCoreState::InTx(ref mut core) = machine.state else {
                return (machine, Ok(TxEffects::empty()));
            };
            core.invalidate_prepare();
            let Some(pos) = core.savepoints.iter().position(|s| s.sp_id == sp_id) else {
                if core.savepoints.iter().any(|s| s.sp_id < sp_id) {
                    return (machine, Err(TxError::UnknownSavepoint(sp_id)));
                }
                core.savepoints.clear();
                core.statement_savepoint_active = false;
                core.pending_record_count = 0;
                core.pending_bytes = 0;
                core.pending_sync = false;
                let mut effects = TxEffects::empty();
                effects.push(TxEffect::BumpUniqueGenIfModified);
                effects.push(TxEffect::RestoreTxToStart);
                return (machine, Ok(effects));
            };
            core.savepoints.truncate(pos + 1);
            let marker = core.savepoints[pos];
            core.pending_record_count = marker.record_count;
            core.pending_bytes = marker.pending_bytes;
            core.pending_sync = marker.pending_sync;
            core.refresh_statement_savepoint_active();
            let mut effects = TxEffects::empty();
            effects.push(TxEffect::BumpUniqueGenIfModified);
            effects.push(TxEffect::RestoreSavepointPayload {
                index: pos,
                clear_pending_buf: marker.record_count == 0,
            });
            (machine, Ok(effects))
        }
        TxEvent::PlanCommit {
            conn_id,
            defer_sync_allowed,
            can_group,
            has_metadata_changes,
            buf_len_exceeds_chunk,
        } => {
            if let TxCoreState::InTx(ref core) = machine.state {
                if core.is_active() {
                    if let Err(e) = check_active_conn(core, conn_id) {
                        return (machine, Err(e));
                    }
                }
            }
            if defer_sync_allowed {
                let deferrable = matches!(
                    &machine.state,
                    TxCoreState::InTx(core)
                        if core.phase == TxPhase::Prepared
                            && core.depth.get() == 1
                            && core.pending_sync
                            && core.pending_record_count > 0
                            && !buf_len_exceeds_chunk
                );
                if !deferrable {
                    return (machine, Ok(TxEffects::one(TxEffect::FallbackFullCommit)));
                }
                if machine.health == HealthState::Poisoned {
                    return (machine, Err(TxError::Poisoned));
                }
                let effects = if !can_group {
                    TxEffects::one(TxEffect::DrainPendingGroup)
                } else {
                    TxEffects::empty()
                };
                return (machine, Ok(effects));
            }
            let TxCoreState::InTx(ref mut core) = machine.state else {
                return (machine, Ok(TxEffects::empty()));
            };
            if !core.is_active() {
                return (machine, Ok(TxEffects::empty()));
            }
            if core.depth.get() > 1 {
                core.depth = NonZeroUsize::new(core.depth.get() - 1).unwrap();
                return (machine, Ok(TxEffects::one(TxEffect::DecDepthDone)));
            }
            if machine.health == HealthState::Poisoned {
                return (machine, Err(TxError::Poisoned));
            }
            if core.phase != TxPhase::Prepared {
                let effective_durability = core.effective_durability(WriteDurability::BufferedFast);
                return (
                    machine,
                    Ok(TxEffects::one(TxEffect::EnsurePreparedBeforeCommit {
                        effective_durability,
                    })),
                );
            }
            if core.pending_record_count == 0 && !has_metadata_changes {
                (machine, Ok(TxEffects::one(TxEffect::ClearEmptyOverlay)))
            } else {
                (machine, Ok(TxEffects::one(TxEffect::DrainPendingGroup)))
            }
        }
        TxEvent::EnterWalFlush { defer_sync_barrier } => {
            if machine.health == HealthState::Poisoned {
                return (machine, Err(TxError::Poisoned));
            }
            let TxCoreState::InTx(ref mut core) = machine.state else {
                return (machine, Err(TxError::InvalidPhase));
            };
            core.phase = TxPhase::FlushingWal;
            let durability = if core.pending_sync {
                WriteDurability::SyncTransactional
            } else {
                WriteDurability::BufferedFast
            };
            let record_count = core.pending_record_count;
            core.pending_record_count = 0;
            core.pending_sync = false;
            (
                machine,
                Ok(TxEffects::one(TxEffect::FlushWal {
                    durability,
                    record_count,
                    defer_sync_barrier,
                })),
            )
        }
        TxEvent::CompleteWalFlush { mode } => {
            let TxCoreState::InTx(ref mut core) = machine.state else {
                return (machine, Err(TxError::InvalidPhase));
            };
            if core.phase != TxPhase::FlushingWal {
                return (machine, Err(TxError::InvalidPhase));
            }
            match mode {
                FlushCompleteMode::PublishImmediate => {
                    machine.state = TxCoreState::Idle;
                    (machine, Ok(TxEffects::one(TxEffect::PublishCommittedTx)))
                }
                FlushCompleteMode::AwaitSync => {
                    core.phase = TxPhase::SyncInFlight;
                    (machine, Ok(TxEffects::one(TxEffect::ReturnAwaitingSync)))
                }
                FlushCompleteMode::HandOffGroup => {
                    machine.state = TxCoreState::Idle;
                    (machine, Ok(TxEffects::one(TxEffect::HandOffPendingGroup)))
                }
            }
        }
        TxEvent::FinishSyncCommit { ok } => {
            if let TxCoreState::InTx(ref mut core) = machine.state {
                if core.phase == TxPhase::SyncInFlight {
                    core.phase = TxPhase::FlushingWal;
                }
            }
            if machine.health == HealthState::Poisoned {
                return (machine, Err(TxError::Poisoned));
            }
            if !machine.state.is_in_tx() {
                return (machine, Err(TxError::InvalidPhase));
            }
            if ok {
                machine.state = TxCoreState::Idle;
                (machine, Ok(TxEffects::one(TxEffect::PublishCommittedTx)))
            } else {
                machine.health = HealthState::Poisoned;
                machine.state = TxCoreState::Idle;
                let mut effects = TxEffects::empty();
                effects.push(TxEffect::PoisonEngineSyncFailed);
                effects.push(TxEffect::RestoreStartMetaAndRelease);
                (machine, Ok(effects))
            }
        }
        TxEvent::Rollback { conn_id } => {
            if let TxCoreState::InTx(ref core) = machine.state {
                if core.is_active() {
                    if let Err(e) = check_active_conn(core, conn_id) {
                        return (machine, Err(e));
                    }
                }
            }
            let prev = core::mem::take(&mut machine.state);
            let TxCoreState::InTx(core) = prev else {
                return (machine, Ok(TxEffects::empty()));
            };
            let mut effects = TxEffects::empty();
            if core.phase == TxPhase::SyncInFlight {
                machine.health = HealthState::Poisoned;
                effects.push(TxEffect::PoisonEngineInterruptedSync);
            }
            effects.push(TxEffect::RestoreStartMetaAndRelease);
            (machine, Ok(effects))
        }
        TxEvent::MarkPoisoned => {
            machine.health = HealthState::Poisoned;
            (machine, Ok(TxEffects::empty()))
        }
    }
}

// === TEST-ONLY SECTION ===

#[cfg(test)]
impl TxCore {
    #[inline]
    pub(super) fn phase(&self) -> TxPhase {
        self.phase
    }

    #[inline]
    pub(super) fn pending_sync(&self) -> bool {
        self.pending_sync
    }

    #[inline]
    pub(super) fn sync_durability_upgrade(&self) -> bool {
        self.sync_durability_upgrade
    }

    #[inline]
    pub(super) fn synced_vtabs(&self) -> &[usize] {
        &self.synced_vtabs
    }
}

#[cfg(test)]
impl TxMachine {
    #[inline]
    pub(super) fn new() -> Self {
        Self {
            health: HealthState::Healthy,
            state: TxCoreState::Idle,
        }
    }
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TxTraceOutcome {
    Ok(TxEffects),
    Err(TxError),
}

#[cfg(test)]
impl TxTraceOutcome {
    pub(super) fn from_result(res: &Result<TxEffects, TxError>) -> Self {
        match res {
            Ok(eff) => Self::Ok(*eff),
            Err(err) => Self::Err(*err),
        }
    }
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct TxTraceStep {
    pub(super) seq: u64,
    pub(super) pre: TxMachine,
    pub(super) event: TxEvent,
    pub(super) post: TxMachine,
    pub(super) outcome: TxTraceOutcome,
}

/// Deterministic `#[cfg(test)]` trace recorder for [`TxMachine`] transitions, supporting
/// pointer-ID normalization, self-contained FSM replay verification, and zero-dependency
/// Schema v1 NDJSON serialization.
#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TxTraceRecorder {
    steps: Vec<TxTraceStep>,
}

#[cfg(test)]
impl TxTraceRecorder {
    pub(super) fn new() -> Self {
        Self { steps: Vec::new() }
    }

    #[inline]
    pub(super) fn last_health(&self) -> Option<HealthState> {
        self.steps.last().map(|s| s.post.health)
    }

    pub(super) fn record(
        &mut self,
        pre: TxMachine,
        event: TxEvent,
        post: TxMachine,
        outcome: TxTraceOutcome,
    ) {
        let seq = self.steps.len() as u64;
        self.steps.push(TxTraceStep {
            seq,
            pre,
            event,
            post,
            outcome,
        });
    }

    pub(super) fn record_external_poison(&mut self, core: &TxCoreState) {
        let pre = TxMachine::from_parts(HealthState::Healthy, core.clone());
        let (post, res) = step(pre.clone(), TxEvent::MarkPoisoned);
        self.record(
            pre,
            TxEvent::MarkPoisoned,
            post,
            TxTraceOutcome::from_result(&res),
        );
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.steps.len()
    }

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.steps.is_empty()
    }

    /// Renumbers `conn_id` and `vtab_id` values to `1, 2, ...` in first-appearance order across
    /// `pre -> event -> post` for deterministic ASLR-free comparison.
    pub(crate) fn normalize_ids(&self) -> Self {
        let mut conns: Vec<usize> = Vec::new();
        let mut vtabs: Vec<usize> = Vec::new();
        let steps = self
            .steps
            .iter()
            .map(|s| {
                let pre = normalize_machine(&s.pre, &mut conns, &mut vtabs);
                let event = normalize_event(s.event, &mut conns, &mut vtabs);
                let post = normalize_machine(&s.post, &mut conns, &mut vtabs);
                TxTraceStep {
                    seq: s.seq,
                    pre,
                    event,
                    post,
                    outcome: s.outcome,
                }
            })
            .collect();
        Self { steps }
    }

    /// Replays every recorded transition from `steps[0].pre` through [`step`] and checks exact
    /// sequence continuity, post-state equality, and emitted effect/error equivalence.
    pub(crate) fn replay_verify(&self) -> Result<(), String> {
        let Some(first) = self.steps.first() else {
            return Ok(());
        };
        let mut cur = first.pre.clone();
        for (idx, s) in self.steps.iter().enumerate() {
            if s.seq != idx as u64 {
                return Err(format!("trace step {idx} has non-sequential seq {}", s.seq));
            }
            if cur != s.pre {
                return Err(format!(
                    "trace continuity mismatch at seq {idx}: expected pre {:?}, got {:?}",
                    s.pre, cur
                ));
            }
            let (next, res) = step(cur, s.event);
            if next != s.post {
                return Err(format!(
                    "post-state mismatch at seq {idx} ({:?}): expected {:?}, got {:?}",
                    s.event, s.post, next
                ));
            }
            let actual_outcome = TxTraceOutcome::from_result(&res);
            if actual_outcome != s.outcome {
                return Err(format!(
                    "outcome mismatch at seq {idx} ({:?}): expected {:?}, got {:?}",
                    s.event, s.outcome, actual_outcome
                ));
            }
            cur = next;
        }
        Ok(())
    }

    /// Serializes the recorded trace into deterministic Schema v1 NDJSON without external crates.
    pub(crate) fn to_ndjson(&self) -> String {
        let mut out = String::with_capacity(64 + self.steps.len() * 384);
        write_ndjson_trace(&mut out, &self.steps).expect("fmt::Write to String cannot fail");
        out
    }
}

#[cfg(test)]
fn intern_id(table: &mut Vec<usize>, raw: usize) -> usize {
    if let Some(pos) = table.iter().position(|&id| id == raw) {
        pos + 1
    } else {
        table.push(raw);
        table.len()
    }
}

#[cfg(test)]
fn normalize_machine(
    machine: &TxMachine,
    conns: &mut Vec<usize>,
    vtabs: &mut Vec<usize>,
) -> TxMachine {
    let state = match &machine.state {
        TxCoreState::Idle => TxCoreState::Idle,
        TxCoreState::InTx(core) => TxCoreState::InTx(TxCore {
            phase: core.phase,
            depth: core.depth,
            owner_conn: core.owner_conn.map(|cid| intern_id(conns, cid)),
            pending_record_count: core.pending_record_count,
            pending_bytes: core.pending_bytes,
            pending_sync: core.pending_sync,
            sync_durability_upgrade: core.sync_durability_upgrade,
            vtabs: core
                .vtabs
                .iter()
                .map(|&vid| intern_id(vtabs, vid))
                .collect(),
            synced_vtabs: core
                .synced_vtabs
                .iter()
                .map(|&vid| intern_id(vtabs, vid))
                .collect(),
            savepoints: core.savepoints.clone(),
            statement_savepoint_active: core.statement_savepoint_active,
        }),
    };
    TxMachine {
        health: machine.health,
        state,
    }
}

#[cfg(test)]
fn normalize_event(event: TxEvent, conns: &mut Vec<usize>, vtabs: &mut Vec<usize>) -> TxEvent {
    match event {
        TxEvent::BeginForConn { conn_id } => TxEvent::BeginForConn {
            conn_id: intern_id(conns, conn_id),
        },
        TxEvent::BeginVtab { conn_id, vtab_id } => TxEvent::BeginVtab {
            conn_id: intern_id(conns, conn_id),
            vtab_id: intern_id(vtabs, vtab_id),
        },
        TxEvent::AttachVtab { conn_id, vtab_id } => TxEvent::AttachVtab {
            conn_id: intern_id(conns, conn_id),
            vtab_id: intern_id(vtabs, vtab_id),
        },
        TxEvent::DetachVtab { conn_id, vtab_id } => TxEvent::DetachVtab {
            conn_id: intern_id(conns, conn_id),
            vtab_id: intern_id(vtabs, vtab_id),
        },
        TxEvent::VtabSync {
            vtab_id,
            durability,
        } => TxEvent::VtabSync {
            vtab_id: intern_id(vtabs, vtab_id),
            durability,
        },
        TxEvent::Savepoint {
            owner_conn,
            sp_id,
            is_statement,
            check_poison,
        } => TxEvent::Savepoint {
            owner_conn: owner_conn.map(|cid| intern_id(conns, cid)),
            sp_id,
            is_statement,
            check_poison,
        },
        TxEvent::ReleaseSavepoint { conn_id, sp_id } => TxEvent::ReleaseSavepoint {
            conn_id: conn_id.map(|cid| intern_id(conns, cid)),
            sp_id,
        },
        TxEvent::RollbackToSavepoint { conn_id, sp_id } => TxEvent::RollbackToSavepoint {
            conn_id: conn_id.map(|cid| intern_id(conns, cid)),
            sp_id,
        },
        TxEvent::PlanCommit {
            conn_id,
            defer_sync_allowed,
            can_group,
            has_metadata_changes,
            buf_len_exceeds_chunk,
        } => TxEvent::PlanCommit {
            conn_id: conn_id.map(|cid| intern_id(conns, cid)),
            defer_sync_allowed,
            can_group,
            has_metadata_changes,
            buf_len_exceeds_chunk,
        },
        TxEvent::Rollback { conn_id } => TxEvent::Rollback {
            conn_id: conn_id.map(|cid| intern_id(conns, cid)),
        },
        other @ (TxEvent::Begin
        | TxEvent::RecordWrite { .. }
        | TxEvent::InvalidatePrepare
        | TxEvent::BeginPrepare { .. }
        | TxEvent::FinishPrepare { .. }
        | TxEvent::EnterWalFlush { .. }
        | TxEvent::CompleteWalFlush { .. }
        | TxEvent::FinishSyncCommit { .. }
        | TxEvent::MarkPoisoned) => other,
    }
}

#[cfg(test)]
fn write_ndjson_trace(out: &mut String, steps: &[TxTraceStep]) -> core::fmt::Result {
    use core::fmt::Write as _;
    out.push_str("{\"kind\":\"header\",\"schema\":1,\"fsm\":\"tx_fsm\"}\n");
    for s in steps {
        write!(out, "{{\"kind\":\"step\",\"seq\":{},\"pre\":", s.seq)?;
        write_tx_machine(out, &s.pre)?;
        out.push_str(",\"event\":");
        write_tx_event(out, &s.event)?;
        out.push_str(",\"post\":");
        write_tx_machine(out, &s.post)?;
        out.push_str(",\"outcome\":");
        write_tx_outcome(out, &s.outcome)?;
        out.push_str("}\n");
    }
    Ok(())
}

#[cfg(test)]
fn health_str(h: HealthState) -> &'static str {
    match h {
        HealthState::Healthy => "Healthy",
        HealthState::Poisoned => "Poisoned",
    }
}

#[cfg(test)]
fn phase_str(p: TxPhase) -> &'static str {
    match p {
        TxPhase::Active => "Active",
        TxPhase::Preparing => "Preparing",
        TxPhase::Prepared => "Prepared",
        TxPhase::FlushingWal => "FlushingWal",
        TxPhase::SyncInFlight => "SyncInFlight",
    }
}

#[cfg(test)]
fn scope_str(s: WriteScope) -> &'static str {
    match s {
        WriteScope::Normal => "Normal",
        WriteScope::InternalBypass => "InternalBypass",
    }
}

#[cfg(test)]
fn durability_str(d: WriteDurability) -> &'static str {
    match d {
        WriteDurability::BufferedFast => "BufferedFast",
        WriteDurability::SyncTransactional => "SyncTransactional",
    }
}

#[cfg(test)]
fn flush_mode_str(m: FlushCompleteMode) -> &'static str {
    match m {
        FlushCompleteMode::PublishImmediate => "PublishImmediate",
        FlushCompleteMode::AwaitSync => "AwaitSync",
        FlushCompleteMode::HandOffGroup => "HandOffGroup",
    }
}

#[cfg(test)]
fn write_opt_usize(out: &mut String, val: Option<usize>) -> core::fmt::Result {
    use core::fmt::Write as _;
    match val {
        Some(v) => write!(out, "{v}"),
        None => {
            out.push_str("null");
            Ok(())
        }
    }
}

#[cfg(test)]
fn write_usize_array(out: &mut String, items: &[usize]) -> core::fmt::Result {
    use core::fmt::Write as _;
    out.push('[');
    for (idx, item) in items.iter().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        write!(out, "{item}")?;
    }
    out.push(']');
    Ok(())
}

#[cfg(test)]
fn write_savepoint_frame(out: &mut String, f: &SavepointFrame) -> core::fmt::Result {
    use core::fmt::Write as _;
    write!(
        out,
        "{{\"sp_id\":{},\"is_statement\":{},\"record_count\":{},\"pending_bytes\":{},\"pending_sync\":{},\"implicit_tx\":{}}}",
        f.sp_id, f.is_statement, f.record_count, f.pending_bytes, f.pending_sync, f.implicit_tx
    )
}

#[cfg(test)]
fn write_tx_machine(out: &mut String, m: &TxMachine) -> core::fmt::Result {
    use core::fmt::Write as _;
    write!(out, "{{\"health\":\"{}\",\"state\":", health_str(m.health))?;
    match &m.state {
        TxCoreState::Idle => out.push_str("{\"tag\":\"Idle\"}"),
        TxCoreState::InTx(c) => {
            write!(
                out,
                "{{\"tag\":\"InTx\",\"phase\":\"{}\",\"depth\":{},\"owner_conn\":",
                phase_str(c.phase),
                c.depth.get()
            )?;
            write_opt_usize(out, c.owner_conn)?;
            write!(
                out,
                ",\"pending_record_count\":{},\"pending_bytes\":{},\"pending_sync\":{},\"sync_durability_upgrade\":{},\"vtabs\":",
                c.pending_record_count, c.pending_bytes, c.pending_sync, c.sync_durability_upgrade
            )?;
            write_usize_array(out, &c.vtabs)?;
            out.push_str(",\"synced_vtabs\":");
            write_usize_array(out, &c.synced_vtabs)?;
            out.push_str(",\"savepoints\":[");
            for (idx, sp) in c.savepoints.iter().enumerate() {
                if idx > 0 {
                    out.push(',');
                }
                write_savepoint_frame(out, sp)?;
            }
            write!(
                out,
                "],\"statement_savepoint_active\":{}}}",
                c.statement_savepoint_active
            )?;
        }
    }
    out.push('}');
    Ok(())
}

#[cfg(test)]
fn write_tx_event(out: &mut String, ev: &TxEvent) -> core::fmt::Result {
    use core::fmt::Write as _;
    match ev {
        TxEvent::Begin => out.push_str("{\"tag\":\"Begin\"}"),
        TxEvent::BeginForConn { conn_id } => {
            write!(out, "{{\"tag\":\"BeginForConn\",\"conn_id\":{conn_id}}}")?;
        }
        TxEvent::BeginVtab { conn_id, vtab_id } => {
            write!(
                out,
                "{{\"tag\":\"BeginVtab\",\"conn_id\":{conn_id},\"vtab_id\":{vtab_id}}}"
            )?;
        }
        TxEvent::AttachVtab { conn_id, vtab_id } => {
            write!(
                out,
                "{{\"tag\":\"AttachVtab\",\"conn_id\":{conn_id},\"vtab_id\":{vtab_id}}}"
            )?;
        }
        TxEvent::DetachVtab { conn_id, vtab_id } => {
            write!(
                out,
                "{{\"tag\":\"DetachVtab\",\"conn_id\":{conn_id},\"vtab_id\":{vtab_id}}}"
            )?;
        }
        TxEvent::VtabSync {
            vtab_id,
            durability,
        } => {
            write!(
                out,
                "{{\"tag\":\"VtabSync\",\"vtab_id\":{vtab_id},\"durability\":\"{}\"}}",
                durability_str(*durability)
            )?;
        }
        TxEvent::RecordWrite {
            scope,
            durability,
            record_count,
            batch_bytes,
            max_transaction_bytes,
        } => {
            write!(
                out,
                "{{\"tag\":\"RecordWrite\",\"scope\":\"{}\",\"durability\":\"{}\",\"record_count\":{record_count},\"batch_bytes\":{batch_bytes},\"max_transaction_bytes\":{max_transaction_bytes}}}",
                scope_str(*scope),
                durability_str(*durability)
            )?;
        }
        TxEvent::InvalidatePrepare => out.push_str("{\"tag\":\"InvalidatePrepare\"}"),
        TxEvent::BeginPrepare { durability } => {
            write!(
                out,
                "{{\"tag\":\"BeginPrepare\",\"durability\":\"{}\"}}",
                durability_str(*durability)
            )?;
        }
        TxEvent::FinishPrepare { ok } => {
            write!(out, "{{\"tag\":\"FinishPrepare\",\"ok\":{ok}}}")?;
        }
        TxEvent::Savepoint {
            owner_conn,
            sp_id,
            is_statement,
            check_poison,
        } => {
            out.push_str("{\"tag\":\"Savepoint\",\"owner_conn\":");
            write_opt_usize(out, *owner_conn)?;
            write!(
                out,
                ",\"sp_id\":{sp_id},\"is_statement\":{is_statement},\"check_poison\":{check_poison}}}"
            )?;
        }
        TxEvent::ReleaseSavepoint { conn_id, sp_id } => {
            out.push_str("{\"tag\":\"ReleaseSavepoint\",\"conn_id\":");
            write_opt_usize(out, *conn_id)?;
            write!(out, ",\"sp_id\":{sp_id}}}")?;
        }
        TxEvent::RollbackToSavepoint { conn_id, sp_id } => {
            out.push_str("{\"tag\":\"RollbackToSavepoint\",\"conn_id\":");
            write_opt_usize(out, *conn_id)?;
            write!(out, ",\"sp_id\":{sp_id}}}")?;
        }
        TxEvent::PlanCommit {
            conn_id,
            defer_sync_allowed,
            can_group,
            has_metadata_changes,
            buf_len_exceeds_chunk,
        } => {
            out.push_str("{\"tag\":\"PlanCommit\",\"conn_id\":");
            write_opt_usize(out, *conn_id)?;
            write!(
                out,
                ",\"defer_sync_allowed\":{defer_sync_allowed},\"can_group\":{can_group},\"has_metadata_changes\":{has_metadata_changes},\"buf_len_exceeds_chunk\":{buf_len_exceeds_chunk}}}"
            )?;
        }
        TxEvent::EnterWalFlush { defer_sync_barrier } => {
            write!(
                out,
                "{{\"tag\":\"EnterWalFlush\",\"defer_sync_barrier\":{defer_sync_barrier}}}"
            )?;
        }
        TxEvent::CompleteWalFlush { mode } => {
            write!(
                out,
                "{{\"tag\":\"CompleteWalFlush\",\"mode\":\"{}\"}}",
                flush_mode_str(*mode)
            )?;
        }
        TxEvent::FinishSyncCommit { ok } => {
            write!(out, "{{\"tag\":\"FinishSyncCommit\",\"ok\":{ok}}}")?;
        }
        TxEvent::Rollback { conn_id } => {
            out.push_str("{\"tag\":\"Rollback\",\"conn_id\":");
            write_opt_usize(out, *conn_id)?;
            out.push('}');
        }
        TxEvent::MarkPoisoned => out.push_str("{\"tag\":\"MarkPoisoned\"}"),
    }
    Ok(())
}

#[cfg(test)]
fn write_tx_effect(out: &mut String, eff: &TxEffect) -> core::fmt::Result {
    use core::fmt::Write as _;
    match eff {
        TxEffect::InitTxData => out.push_str("{\"tag\":\"InitTxData\"}"),
        TxEffect::AppendTxBatch {
            reset_buf,
            record_undo,
        } => {
            write!(
                out,
                "{{\"tag\":\"AppendTxBatch\",\"reset_buf\":{reset_buf},\"record_undo\":{record_undo}}}"
            )?;
        }
        TxEffect::VtabSyncOutcome { all_synced } => {
            write!(
                out,
                "{{\"tag\":\"VtabSyncOutcome\",\"all_synced\":{all_synced}}}"
            )?;
        }
        TxEffect::RunPrepare {
            effective_durability,
        } => {
            write!(
                out,
                "{{\"tag\":\"RunPrepare\",\"effective_durability\":\"{}\"}}",
                durability_str(*effective_durability)
            )?;
        }
        TxEffect::CaptureSavepointPayload {
            sp_id,
            is_statement,
            zero_buf_offset,
        } => {
            write!(
                out,
                "{{\"tag\":\"CaptureSavepointPayload\",\"sp_id\":{sp_id},\"is_statement\":{is_statement},\"zero_buf_offset\":{zero_buf_offset}}}"
            )?;
        }
        TxEffect::TruncateSavepointPayloads {
            new_len,
            clear_overlay_undo,
            clear_stmt_blob_undo,
        } => {
            write!(
                out,
                "{{\"tag\":\"TruncateSavepointPayloads\",\"new_len\":{new_len},\"clear_overlay_undo\":{clear_overlay_undo},\"clear_stmt_blob_undo\":{clear_stmt_blob_undo}}}"
            )?;
        }
        TxEffect::CommitImplicitTx => out.push_str("{\"tag\":\"CommitImplicitTx\"}"),
        TxEffect::BumpUniqueGenIfModified => {
            out.push_str("{\"tag\":\"BumpUniqueGenIfModified\"}");
        }
        TxEffect::RestoreTxToStart => out.push_str("{\"tag\":\"RestoreTxToStart\"}"),
        TxEffect::RestoreSavepointPayload {
            index,
            clear_pending_buf,
        } => {
            write!(
                out,
                "{{\"tag\":\"RestoreSavepointPayload\",\"index\":{index},\"clear_pending_buf\":{clear_pending_buf}}}"
            )?;
        }
        TxEffect::DecDepthDone => out.push_str("{\"tag\":\"DecDepthDone\"}"),
        TxEffect::EnsurePreparedBeforeCommit {
            effective_durability,
        } => {
            write!(
                out,
                "{{\"tag\":\"EnsurePreparedBeforeCommit\",\"effective_durability\":\"{}\"}}",
                durability_str(*effective_durability)
            )?;
        }
        TxEffect::FallbackFullCommit => out.push_str("{\"tag\":\"FallbackFullCommit\"}"),
        TxEffect::DrainPendingGroup => out.push_str("{\"tag\":\"DrainPendingGroup\"}"),
        TxEffect::ClearEmptyOverlay => out.push_str("{\"tag\":\"ClearEmptyOverlay\"}"),
        TxEffect::FlushWal {
            durability,
            record_count,
            defer_sync_barrier,
        } => {
            write!(
                out,
                "{{\"tag\":\"FlushWal\",\"durability\":\"{}\",\"record_count\":{record_count},\"defer_sync_barrier\":{defer_sync_barrier}}}",
                durability_str(*durability)
            )?;
        }
        TxEffect::PublishCommittedTx => out.push_str("{\"tag\":\"PublishCommittedTx\"}"),
        TxEffect::ReturnAwaitingSync => out.push_str("{\"tag\":\"ReturnAwaitingSync\"}"),
        TxEffect::HandOffPendingGroup => out.push_str("{\"tag\":\"HandOffPendingGroup\"}"),
        TxEffect::PoisonEngineInterruptedSync => {
            out.push_str("{\"tag\":\"PoisonEngineInterruptedSync\"}");
        }
        TxEffect::PoisonEngineSyncFailed => out.push_str("{\"tag\":\"PoisonEngineSyncFailed\"}"),
        TxEffect::RestoreStartMetaAndRelease => {
            out.push_str("{\"tag\":\"RestoreStartMetaAndRelease\"}");
        }
    }
    Ok(())
}

#[cfg(test)]
fn write_tx_outcome(out: &mut String, outcome: &TxTraceOutcome) -> core::fmt::Result {
    use core::fmt::Write as _;
    match outcome {
        TxTraceOutcome::Ok(effects) => {
            out.push_str("{\"status\":\"Ok\",\"effects\":[");
            for (idx, eff) in effects.iter().enumerate() {
                if idx > 0 {
                    out.push(',');
                }
                write_tx_effect(out, &eff)?;
            }
            out.push_str("]}");
        }
        TxTraceOutcome::Err(err) => {
            out.push_str("{\"status\":\"Err\",\"error\":");
            match err {
                TxError::Poisoned => out.push_str("{\"tag\":\"Poisoned\"}"),
                TxError::Busy => out.push_str("{\"tag\":\"Busy\"}"),
                TxError::TxBudgetExceeded => out.push_str("{\"tag\":\"TxBudgetExceeded\"}"),
                TxError::TooManyRecords => out.push_str("{\"tag\":\"TooManyRecords\"}"),
                TxError::UnknownSavepoint(sp_id) => {
                    write!(out, "{{\"tag\":\"UnknownSavepoint\",\"sp_id\":{sp_id}}}")?;
                }
                TxError::InvalidPhase => out.push_str("{\"tag\":\"InvalidPhase\"}"),
            }
            out.push('}');
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn apply_ok(m: &mut TxMachine, ev: TxEvent) -> TxEffects {
        let prev = core::mem::replace(m, TxMachine::new());
        let (next, res) = step(prev, ev);
        *m = next;
        res.expect("expected valid transition")
    }

    fn step_ok(m: &mut TxMachine, ev: TxEvent) {
        let _ = apply_ok(m, ev);
    }

    fn apply_err(m: &mut TxMachine, ev: TxEvent) -> TxError {
        let prev = core::mem::replace(m, TxMachine::new());
        let (next, res) = step(prev, ev);
        *m = next;
        res.expect_err("expected rejected transition")
    }

    #[test]
    fn tx_fsm_production_code_has_no_io_lock_ffi_or_unsafe() {
        let src = include_str!("tx_fsm.rs");
        let prod = src
            .split("// === TEST-ONLY SECTION ===")
            .next()
            .expect("test marker must exist");
        assert!(
            !prod.contains("#[cfg(test)]"),
            "production section of tx_fsm.rs must stay strictly outside #[cfg(test)]"
        );
        let forbidden = [
            "std::io",
            "std::fs",
            "std::sync",
            "std::thread",
            "std::time",
            "PlatformFs",
            "PlatformFile",
            "sqlite3",
            "unsafe",
        ];
        for (idx, line) in prod.lines().enumerate() {
            let trimmed = line.trim();
            if trimmed.starts_with("//") {
                continue;
            }
            let code_part = trimmed.split("//").next().unwrap_or("");
            for token in forbidden {
                assert!(
                    !code_part.contains(token),
                    "forbidden token `{token}` found at tx_fsm.rs line {}: {line}",
                    idx + 1
                );
            }
        }
    }

    #[test]
    fn tx_fsm_enforces_inv_tx_1_through_5() {
        let mut m = TxMachine::new();
        assert!(!m.state.is_in_tx());

        // INV-TX-2 & INV-TX-3: InternalBypass keeps Prepared + sync_durability_upgrade + synced_vtabs;
        // Normal invalidates back to Active and enforces budget.
        let eff = apply_ok(
            &mut m,
            TxEvent::BeginVtab {
                conn_id: 1,
                vtab_id: 1001,
            },
        );
        assert_eq!(eff.iter().collect::<Vec<_>>(), vec![TxEffect::InitTxData]);
        step_ok(
            &mut m,
            TxEvent::BeginVtab {
                conn_id: 1,
                vtab_id: 1002,
            },
        );
        step_ok(
            &mut m,
            TxEvent::RecordWrite {
                scope: WriteScope::Normal,
                durability: WriteDurability::BufferedFast,
                record_count: 1,
                batch_bytes: 100,
                max_transaction_bytes: 256,
            },
        );
        step_ok(
            &mut m,
            TxEvent::BeginPrepare {
                durability: WriteDurability::BufferedFast,
            },
        );
        step_ok(&mut m, TxEvent::FinishPrepare { ok: true });
        assert_eq!(m.state.core().unwrap().phase(), TxPhase::Prepared);

        let vsync = apply_ok(
            &mut m,
            TxEvent::VtabSync {
                vtab_id: 1001,
                durability: WriteDurability::SyncTransactional,
            },
        );
        assert_eq!(
            vsync.iter().collect::<Vec<_>>(),
            vec![TxEffect::VtabSyncOutcome { all_synced: false }]
        );

        step_ok(
            &mut m,
            TxEvent::RecordWrite {
                scope: WriteScope::InternalBypass,
                durability: WriteDurability::BufferedFast,
                record_count: 1,
                batch_bytes: 400,
                max_transaction_bytes: 256,
            },
        );
        assert_eq!(m.state.core().unwrap().phase(), TxPhase::Prepared);
        assert!(m.state.core().unwrap().sync_durability_upgrade());
        assert_eq!(m.state.core().unwrap().synced_vtabs(), &[1001]);

        let budget_err = apply_err(
            &mut m,
            TxEvent::RecordWrite {
                scope: WriteScope::Normal,
                durability: WriteDurability::BufferedFast,
                record_count: 1,
                batch_bytes: 64,
                max_transaction_bytes: 256,
            },
        );
        assert_eq!(budget_err, TxError::TxBudgetExceeded);
        assert_eq!(m.state.core().unwrap().phase(), TxPhase::Prepared);

        step_ok(
            &mut m,
            TxEvent::RecordWrite {
                scope: WriteScope::Normal,
                durability: WriteDurability::BufferedFast,
                record_count: 1,
                batch_bytes: 10,
                max_transaction_bytes: 4096,
            },
        );
        assert_eq!(m.state.core().unwrap().phase(), TxPhase::Active);
        assert!(!m.state.core().unwrap().sync_durability_upgrade());
        assert!(m.state.core().unwrap().synced_vtabs().is_empty());

        // INV-TX-5: Savepoint LIFO, duplicate no-op, unknown SP error, start rollback.
        let sp1 = apply_ok(
            &mut m,
            TxEvent::Savepoint {
                owner_conn: Some(1),
                sp_id: 1,
                is_statement: false,
                check_poison: true,
            },
        );
        assert_eq!(
            sp1.iter().collect::<Vec<_>>(),
            vec![TxEffect::CaptureSavepointPayload {
                sp_id: 1,
                is_statement: false,
                zero_buf_offset: false
            }]
        );
        let dup = apply_ok(
            &mut m,
            TxEvent::Savepoint {
                owner_conn: Some(1),
                sp_id: 1,
                is_statement: false,
                check_poison: true,
            },
        );
        assert!(dup.iter().next().is_none());

        assert_eq!(
            apply_err(
                &mut m,
                TxEvent::RollbackToSavepoint {
                    conn_id: Some(1),
                    sp_id: 99,
                },
            ),
            TxError::UnknownSavepoint(99)
        );
        let to_start = apply_ok(
            &mut m,
            TxEvent::RollbackToSavepoint {
                conn_id: Some(1),
                sp_id: -1,
            },
        );
        assert_eq!(
            to_start.iter().collect::<Vec<_>>(),
            vec![
                TxEffect::BumpUniqueGenIfModified,
                TxEffect::RestoreTxToStart
            ]
        );
        assert_eq!(m.state.core().unwrap().pending_record_count(), 0);
        assert!(!m.state.core().unwrap().pending_sync());

        // Implicit savepoint release emits CommitImplicitTx.
        step_ok(&mut m, TxEvent::Rollback { conn_id: Some(1) });
        assert!(!m.state.is_in_tx());
        let imp = apply_ok(
            &mut m,
            TxEvent::Savepoint {
                owner_conn: Some(2),
                sp_id: 5,
                is_statement: true,
                check_poison: true,
            },
        );
        assert!(imp.iter().any(|e| matches!(e, TxEffect::InitTxData)));
        assert!(m.state.core().unwrap().statement_savepoint_active());
        let rel = apply_ok(
            &mut m,
            TxEvent::ReleaseSavepoint {
                conn_id: Some(2),
                sp_id: 5,
            },
        );
        assert!(rel.iter().any(|e| matches!(e, TxEffect::CommitImplicitTx)));

        // SyncInFlight interrupted rollback poisons engine (INV-TX-4) and blocks later mutators.
        step_ok(
            &mut m,
            TxEvent::RecordWrite {
                scope: WriteScope::Normal,
                durability: WriteDurability::SyncTransactional,
                record_count: 1,
                batch_bytes: 32,
                max_transaction_bytes: 4096,
            },
        );
        step_ok(
            &mut m,
            TxEvent::EnterWalFlush {
                defer_sync_barrier: true,
            },
        );
        let await_eff = apply_ok(
            &mut m,
            TxEvent::CompleteWalFlush {
                mode: FlushCompleteMode::AwaitSync,
            },
        );
        assert_eq!(
            await_eff.iter().collect::<Vec<_>>(),
            vec![TxEffect::ReturnAwaitingSync]
        );
        assert_eq!(m.state.core().unwrap().phase(), TxPhase::SyncInFlight);

        let rb = apply_ok(&mut m, TxEvent::Rollback { conn_id: Some(2) });
        assert_eq!(
            rb.iter().collect::<Vec<_>>(),
            vec![
                TxEffect::PoisonEngineInterruptedSync,
                TxEffect::RestoreStartMetaAndRelease,
            ]
        );
        assert_eq!(m.health, HealthState::Poisoned);
        assert!(!m.state.is_in_tx());

        assert_eq!(
            apply_err(&mut m, TxEvent::BeginForConn { conn_id: 3 }),
            TxError::Poisoned
        );
        assert_eq!(
            apply_err(
                &mut m,
                TxEvent::Savepoint {
                    owner_conn: Some(3),
                    sp_id: 1,
                    is_statement: false,
                    check_poison: true,
                },
            ),
            TxError::Poisoned
        );
    }

    fn record_trace_sequence(
        conn1: usize,
        conn2: usize,
        vtab1: usize,
        vtab2: usize,
    ) -> TxTraceRecorder {
        let mut rec = TxTraceRecorder::new();
        let mut machine = TxMachine::new();
        let events = [
            TxEvent::BeginVtab {
                conn_id: conn1,
                vtab_id: vtab1,
            },
            TxEvent::BeginVtab {
                conn_id: conn1,
                vtab_id: vtab2,
            },
            TxEvent::Savepoint {
                owner_conn: Some(conn1),
                sp_id: 0,
                is_statement: false,
                check_poison: true,
            },
            TxEvent::RecordWrite {
                scope: WriteScope::Normal,
                durability: WriteDurability::BufferedFast,
                record_count: 1,
                batch_bytes: 64,
                max_transaction_bytes: 4096,
            },
            TxEvent::RollbackToSavepoint {
                conn_id: Some(conn1),
                sp_id: 0,
            },
            TxEvent::RecordWrite {
                scope: WriteScope::Normal,
                durability: WriteDurability::SyncTransactional,
                record_count: 1,
                batch_bytes: 128,
                max_transaction_bytes: 4096,
            },
            TxEvent::ReleaseSavepoint {
                conn_id: Some(conn1),
                sp_id: 0,
            },
            TxEvent::VtabSync {
                vtab_id: vtab1,
                durability: WriteDurability::SyncTransactional,
            },
            TxEvent::VtabSync {
                vtab_id: vtab2,
                durability: WriteDurability::BufferedFast,
            },
            TxEvent::BeginPrepare {
                durability: WriteDurability::BufferedFast,
            },
            TxEvent::FinishPrepare { ok: true },
            TxEvent::PlanCommit {
                conn_id: Some(conn1),
                defer_sync_allowed: true,
                can_group: false,
                has_metadata_changes: false,
                buf_len_exceeds_chunk: false,
            },
            TxEvent::EnterWalFlush {
                defer_sync_barrier: true,
            },
            TxEvent::CompleteWalFlush {
                mode: FlushCompleteMode::AwaitSync,
            },
            TxEvent::FinishSyncCommit { ok: false },
            TxEvent::BeginForConn { conn_id: conn2 },
        ];
        for ev in events {
            let pre = machine.clone();
            let (post, res) = step(machine, ev);
            rec.record(pre, ev, post.clone(), TxTraceOutcome::from_result(&res));
            machine = post;
        }
        rec
    }

    #[test]
    fn tx_fsm_trace_normalize_ids_and_ndjson_v1_replay_are_deterministic() {
        let t1 = record_trace_sequence(0x7F11_0000, 0x7F11_0008, 0x7F11_1100, 0x7F11_2200);
        let t2 = record_trace_sequence(0x55AA_0010, 0x55AA_0020, 0x55AA_0030, 0x55AA_0040);

        assert!(!t1.is_empty());
        assert_eq!(t1.len(), 16);
        assert!(t1.replay_verify().is_ok());
        assert!(t2.replay_verify().is_ok());

        let n1 = t1.normalize_ids();
        let n2 = t2.normalize_ids();
        assert_eq!(n1, n2);
        assert!(n1.replay_verify().is_ok());

        let nd1 = n1.to_ndjson();
        let nd2 = n2.to_ndjson();
        assert_eq!(nd1, nd2);

        let lines: Vec<&str> = nd1.lines().collect();
        assert_eq!(lines.len(), 17);
        assert_eq!(
            lines[0],
            "{\"kind\":\"header\",\"schema\":1,\"fsm\":\"tx_fsm\"}"
        );
        assert!(lines[1].contains("\"conn_id\":1,\"vtab_id\":1"));
        assert!(lines[2].contains("\"conn_id\":1,\"vtab_id\":2"));
        assert!(lines[16].contains("\"conn_id\":2"));
        assert!(lines[16].contains("\"status\":\"Err\",\"error\":{\"tag\":\"Poisoned\"}"));

        let mut tampered_post = n1.clone();
        tampered_post.steps[3].post.health = HealthState::Poisoned;
        assert!(tampered_post.replay_verify().is_err());

        let mut tampered_chain = n1;
        tampered_chain.steps[4].pre.state = TxCoreState::Idle;
        assert!(tampered_chain.replay_verify().is_err());
    }
}
