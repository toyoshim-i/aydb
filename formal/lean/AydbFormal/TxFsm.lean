set_option autoImplicit false

/-!
  SM-1: Pure Sans-I/O model of `src/engine/tx_fsm.rs` and unbounded proofs of `INV-TX-1`
  through `INV-TX-5`.
-/

namespace AydbFormal.TxFsm

inductive WriteScope where
  | normal
  | internalBypass
  deriving DecidableEq, Repr, Inhabited

inductive WriteDurability where
  | bufferedFast
  | syncTransactional
  deriving DecidableEq, Repr, Inhabited

inductive TxPhase where
  | active
  | preparing
  | prepared
  | flushingWal
  | syncInFlight
  deriving DecidableEq, Repr, Inhabited

inductive HealthState where
  | healthy
  | poisoned
  deriving DecidableEq, Repr, Inhabited

inductive FlushCompleteMode where
  | publishImmediate
  | awaitSync
  | handOffGroup
  deriving DecidableEq, Repr, Inhabited

structure SavepointFrame where
  spId : Int
  isStatement : Bool
  recordCount : Nat
  pendingBytes : Nat
  pendingSync : Bool
  implicitTx : Bool
  deriving DecidableEq, Repr, Inhabited

structure TxCore where
  phase : TxPhase
  depth : Nat
  ownerConn : Option Nat
  pendingRecordCount : Nat
  pendingBytes : Nat
  pendingSync : Bool
  syncDurabilityUpgrade : Bool
  vtabs : List Nat
  syncedVtabs : List Nat
  savepoints : List SavepointFrame
  statementSavepointActive : Bool
  deriving DecidableEq, Repr, Inhabited

inductive TxCoreState where
  | idle
  | inTx (core : TxCore)
  deriving DecidableEq, Repr, Inhabited

structure TxMachine where
  health : HealthState
  state : TxCoreState
  deriving DecidableEq, Repr, Inhabited

inductive TxError where
  | poisoned
  | busy
  | txBudgetExceeded
  | tooManyRecords
  | unknownSavepoint (spId : Int)
  | invalidPhase
  deriving DecidableEq, Repr, Inhabited

inductive TxEvent where
  | begin
  | beginForConn (connId : Nat)
  | beginVtab (connId vtabId : Nat)
  | attachVtab (connId vtabId : Nat)
  | detachVtab (connId vtabId : Nat)
  | vtabSync (vtabId : Nat) (durability : WriteDurability)
  | recordWrite (scope : WriteScope) (durability : WriteDurability)
      (recordCount batchBytes maxTransactionBytes : Nat)
  | invalidatePrepare
  | beginPrepare (durability : WriteDurability)
  | finishPrepare (ok : Bool)
  | savepoint (ownerConn : Option Nat) (spId : Int) (isStatement checkPoison : Bool)
  | releaseSavepoint (connId : Option Nat) (spId : Int)
  | rollbackToSavepoint (connId : Option Nat) (spId : Int)
  | planCommit (connId : Option Nat)
      (deferSyncAllowed canGroup hasMetadataChanges bufLenExceedsChunk : Bool)
  | enterWalFlush (deferSyncBarrier : Bool)
  | completeWalFlush (mode : FlushCompleteMode)
  | finishSyncCommit (ok : Bool)
  | rollback (connId : Option Nat)
  | markPoisoned
  deriving DecidableEq, Repr, Inhabited

inductive TxEffect where
  | initTxData
  | appendTxBatch (resetBuf recordUndo : Bool)
  | vtabSyncOutcome (allSynced : Bool)
  | runPrepare (effectiveDurability : WriteDurability)
  | captureSavepointPayload (spId : Int) (isStatement zeroBufOffset : Bool)
  | truncateSavepointPayloads (newLen : Nat) (clearOverlayUndo clearStmtBlobUndo : Bool)
  | commitImplicitTx
  | bumpUniqueGenIfModified
  | restoreTxToStart
  | restoreSavepointPayload (index : Nat) (clearPendingBuf : Bool)
  | decDepthDone
  | ensurePreparedBeforeCommit (effectiveDurability : WriteDurability)
  | fallbackFullCommit
  | drainPendingGroup
  | clearEmptyOverlay
  | flushWal (durability : WriteDurability) (recordCount : Nat) (deferSyncBarrier : Bool)
  | publishCommittedTx
  | returnAwaitingSync
  | handOffPendingGroup
  | poisonEngineInterruptedSync
  | poisonEngineSyncFailed
  | restoreStartMetaAndRelease
  deriving DecidableEq, Repr, Inhabited

inductive TxOutcome where
  | ok (effects : List TxEffect)
  | err (error : TxError)
  deriving DecidableEq, Repr, Inhabited

@[simp] def TxOutcome.effects : TxOutcome → List TxEffect
  | .ok effs => effs
  | .err _ => []

def U32_MAX : Nat := 4294967295

@[simp] def TxCore.new (ownerConn : Option Nat) : TxCore :=
  { phase := .active
    depth := 1
    ownerConn := ownerConn
    pendingRecordCount := 0
    pendingBytes := 0
    pendingSync := false
    syncDurabilityUpgrade := false
    vtabs := []
    syncedVtabs := []
    savepoints := []
    statementSavepointActive := false }

@[simp] def TxMachine.init : TxMachine :=
  { health := .healthy, state := .idle }

def TxCore.isActive (c : TxCore) : Bool :=
  c.phase != .flushingWal

def TxCore.isPreparing (c : TxCore) : Bool :=
  c.phase == .preparing

def TxCore.effectiveDurability (c : TxCore) (fallback : WriteDurability) : WriteDurability :=
  if c.pendingSync || c.syncDurabilityUpgrade then .syncTransactional else fallback

def TxCore.invalidatePrepare (c : TxCore) : TxCore :=
  { c with
    phase := .active
    syncedVtabs := []
    syncDurabilityUpgrade := false }

def TxCore.refreshStatementSavepointActive (c : TxCore) : TxCore :=
  { c with statementSavepointActive := c.savepoints.any (·.isStatement) }

@[simp] def TxCoreState.isInTx : TxCoreState → Bool
  | .idle => false
  | .inTx _ => true

@[simp] def TxCoreState.core? : TxCoreState → Option TxCore
  | .idle => none
  | .inTx c => some c

def findFrameWithIdx? (p : SavepointFrame → Bool) :
    List SavepointFrame → Nat → Option (Nat × SavepointFrame)
  | [], _ => none
  | f :: fs, idx =>
      if p f then some (idx, f) else findFrameWithIdx? p fs (idx + 1)

def canRecordWrite (health : HealthState) (core : Option TxCore) (scope : WriteScope)
    (recordCount batchBytes maxTransactionBytes : Nat) : Except TxError Bool :=
  if health == .poisoned then
    .error .poisoned
  else
    match core with
    | none => .error .invalidPhase
    | some c =>
        if !c.isActive then
          .error .invalidPhase
        else
          let bypass := scope == .internalBypass || c.isPreparing
          let nextBytes := c.pendingBytes + batchBytes
          if !bypass && maxTransactionBytes > 0 && nextBytes > maxTransactionBytes then
            .error .txBudgetExceeded
          else if c.pendingRecordCount + recordCount > U32_MAX then
            .error .tooManyRecords
          else
            .ok (c.pendingRecordCount == 0)

def checkActiveConn (c : TxCore) (connId : Option Nat) : Option TxError :=
  match connId with
  | none => none
  | some cid => if c.ownerConn != some cid then some .busy else none

def pushUniqueNat (xs : List Nat) (v : Nat) : List Nat :=
  if xs.contains v then xs else xs ++ [v]

def step (machine : TxMachine) (event : TxEvent) : TxMachine × TxOutcome :=
  match event with
  | .begin =>
      match machine.state with
      | .inTx c =>
          let c' := { c with depth := c.depth + 1 }
          ({ machine with state := .inTx c' }, .ok [])
      | .idle =>
          ({ machine with state := .inTx (TxCore.new none) }, .ok [.initTxData])
  | .beginForConn connId =>
      if machine.health == .poisoned then
        (machine, .err .poisoned)
      else
        match machine.state with
        | .inTx c =>
            if c.ownerConn != some connId then
              (machine, .err .busy)
            else
              let c' := { c with depth := c.depth + 1 }
              ({ machine with state := .inTx c' }, .ok [])
        | .idle =>
            ({ machine with state := .inTx (TxCore.new (some connId)) }, .ok [.initTxData])
  | .beginVtab connId vtabId =>
      if machine.health == .poisoned then
        (machine, .err .poisoned)
      else
        match machine.state with
        | .idle =>
            let c := { TxCore.new (some connId) with vtabs := [vtabId] }
            ({ machine with state := .inTx c }, .ok [.initTxData])
        | .inTx c =>
            if c.isActive then
              if c.ownerConn != some connId then
                (machine, .err .busy)
              else if !c.vtabs.contains vtabId then
                let c' :=
                  { c with
                    vtabs := c.vtabs ++ [vtabId]
                    syncedVtabs := []
                    syncDurabilityUpgrade := false }
                ({ machine with state := .inTx c' }, .ok [])
              else
                (machine, .ok [])
            else
              (machine, .err .invalidPhase)
  | .attachVtab connId vtabId =>
      match machine.state with
      | .inTx c =>
          if c.isActive && c.ownerConn == some connId && !c.vtabs.contains vtabId then
            let c' :=
              { c with
                vtabs := c.vtabs ++ [vtabId]
                syncedVtabs := []
                syncDurabilityUpgrade := false }
            ({ machine with state := .inTx c' }, .ok [])
          else
            (machine, .ok [])
      | .idle => (machine, .ok [])
  | .detachVtab connId vtabId =>
      match machine.state with
      | .inTx c =>
          if c.isActive && c.ownerConn == some connId then
            let c' :=
              { c with
                vtabs := c.vtabs.filter (· != vtabId)
                syncedVtabs := c.syncedVtabs.filter (· != vtabId) }
            ({ machine with state := .inTx c' }, .ok [])
          else
            (machine, .ok [])
      | .idle => (machine, .ok [])
  | .vtabSync vtabId durability =>
      match machine.state with
      | .idle => (machine, .ok [.vtabSyncOutcome true])
      | .inTx c =>
          if !c.isActive then
            (machine, .ok [.vtabSyncOutcome true])
          else
            let upgrade :=
              if durability == .syncTransactional then true else c.syncDurabilityUpgrade
            let synced1 := pushUniqueNat c.syncedVtabs vtabId
            let allSynced := c.vtabs.all (fun id => synced1.contains id)
            let synced2 := if allSynced then [] else synced1
            let c' :=
              { c with
                syncDurabilityUpgrade := upgrade
                syncedVtabs := synced2 }
            ({ machine with state := .inTx c' }, .ok [.vtabSyncOutcome allSynced])
  | .recordWrite scope durability recordCount batchBytes maxTransactionBytes =>
      match canRecordWrite machine.health machine.state.core? scope recordCount batchBytes maxTransactionBytes with
      | .error e => (machine, .err e)
      | .ok resetBuf =>
          match machine.state with
          | .idle => (machine, .err .invalidPhase)
          | .inTx c =>
              let bypass := scope == .internalBypass || c.isPreparing
              let c1 := if !bypass then c.invalidatePrepare else c
              let c2 :=
                { c1 with
                  pendingBytes := c1.pendingBytes + batchBytes
                  pendingRecordCount := c1.pendingRecordCount + recordCount
                  pendingSync :=
                    if durability == .syncTransactional then true else c1.pendingSync }
              let recordUndo := !c2.savepoints.isEmpty
              ({ machine with state := .inTx c2 }, .ok [.appendTxBatch resetBuf recordUndo])
  | .invalidatePrepare =>
      match machine.state with
      | .inTx c =>
          if c.isActive then
            ({ machine with state := .inTx c.invalidatePrepare }, .ok [])
          else
            (machine, .ok [])
      | .idle => (machine, .ok [])
  | .beginPrepare durability =>
      match machine.state with
      | .idle => (machine, .ok [])
      | .inTx c =>
          if !c.isActive then
            (machine, .ok [])
          else if machine.health == .poisoned then
            (machine, .err .poisoned)
          else
            let requestedSync :=
              durability == .syncTransactional || c.syncDurabilityUpgrade
            let needSyncUpgrade := requestedSync && !c.pendingSync
            if c.phase == .prepared && !needSyncUpgrade then
              (machine, .ok [])
            else
              let pendSync := if requestedSync then true else c.pendingSync
              let effective : WriteDurability :=
                if pendSync then .syncTransactional else .bufferedFast
              let c' := { c with pendingSync := pendSync, phase := .preparing }
              ({ machine with state := .inTx c' }, .ok [.runPrepare effective])
  | .finishPrepare ok =>
      match machine.state with
      | .inTx c =>
          let nextPhase := if ok then TxPhase.prepared else TxPhase.active
          ({ machine with state := .inTx { c with phase := nextPhase } }, .ok [])
      | .idle => (machine, .ok [])
  | .savepoint ownerConn spId isStatement checkPoison =>
      if checkPoison && machine.health == .poisoned then
        (machine, .err .poisoned)
      else
        match machine.state with
        | .inTx c =>
            if c.isActive then
              match checkActiveConn c ownerConn with
              | some e => (machine, .err e)
              | none =>
                  let c1 := c.invalidatePrepare
                  if c1.savepoints.any (fun s => s.spId == spId) then
                    ({ machine with state := .inTx c1 }, .ok [])
                  else
                    let zeroBuf := c1.pendingRecordCount == 0
                    let frame : SavepointFrame :=
                      { spId := spId
                        isStatement := isStatement
                        recordCount := c1.pendingRecordCount
                        pendingBytes := c1.pendingBytes
                        pendingSync := c1.pendingSync
                        implicitTx := false }
                    let c2 :=
                      ({ c1 with savepoints := c1.savepoints ++ [frame] }).refreshStatementSavepointActive
                    ({ machine with state := .inTx c2 },
                      .ok [.captureSavepointPayload spId isStatement zeroBuf])
            else
              (machine, .err .invalidPhase)
        | .idle =>
            let frame : SavepointFrame :=
              { spId := spId
                isStatement := isStatement
                recordCount := 0
                pendingBytes := 0
                pendingSync := false
                implicitTx := true }
            let c0 := TxCore.new ownerConn
            let c1 := ({ c0 with savepoints := [frame] }).refreshStatementSavepointActive
            ({ machine with state := .inTx c1 },
              .ok [.initTxData, .captureSavepointPayload spId isStatement true])
  | .releaseSavepoint connId spId =>
      match machine.state with
      | .idle => (machine, .ok [])
      | .inTx c =>
          if !c.isActive then
            (machine, .ok [])
          else
            match checkActiveConn c connId with
            | some e => (machine, .err e)
            | none =>
                let found :=
                  match findFrameWithIdx? (fun s => s.spId == spId) c.savepoints 0 with
                  | some x => some x
                  | none => findFrameWithIdx? (fun s => decide (s.spId > spId)) c.savepoints 0
                match found with
                | none => (machine, .ok [])
                | some (pos, frame) =>
                    let sps' := c.savepoints.take pos
                    let c' := ({ c with savepoints := sps' }).refreshStatementSavepointActive
                    let clearOverlayUndo := c'.savepoints.isEmpty
                    let clearStmtBlobUndo := !c'.statementSavepointActive
                    let baseEff := TxEffect.truncateSavepointPayloads pos clearOverlayUndo clearStmtBlobUndo
                    let effs :=
                      if frame.implicitTx && c'.depth == 1 then
                        [baseEff, .commitImplicitTx]
                      else
                        [baseEff]
                    ({ machine with state := .inTx c' }, .ok effs)
  | .rollbackToSavepoint connId spId =>
      match machine.state with
      | .idle => (machine, .ok [])
      | .inTx c =>
          let connErr :=
            if c.isActive then checkActiveConn c connId
            else none
          match connErr with
          | some e => (machine, .err e)
          | none =>
              if !c.isActive && c.phase != .flushingWal then
                (machine, .ok [])
              else
                let c1 := c.invalidatePrepare
                match findFrameWithIdx? (fun s => s.spId == spId) c1.savepoints 0 with
                | none =>
                    if c1.savepoints.any (fun s => decide (s.spId < spId)) then
                      ({ machine with state := .inTx c1 }, .err (.unknownSavepoint spId))
                    else
                      let c2 :=
                        { c1 with
                          savepoints := []
                          statementSavepointActive := false
                          pendingRecordCount := 0
                          pendingBytes := 0
                          pendingSync := false }
                      ({ machine with state := .inTx c2 },
                        .ok [.bumpUniqueGenIfModified, .restoreTxToStart])
                | some (pos, marker) =>
                    let sps' := c1.savepoints.take (pos + 1)
                    let c2 :=
                      ({ c1 with
                         savepoints := sps'
                         pendingRecordCount := marker.recordCount
                         pendingBytes := marker.pendingBytes
                         pendingSync := marker.pendingSync }).refreshStatementSavepointActive
                    ({ machine with state := .inTx c2 },
                      .ok [.bumpUniqueGenIfModified, .restoreSavepointPayload pos (marker.recordCount == 0)])
  | .planCommit connId deferSyncAllowed canGroup hasMetadataChanges bufLenExceedsChunk =>
      let connErr :=
        match machine.state with
        | .inTx c => if c.isActive then checkActiveConn c connId else none
        | .idle => none
      match connErr with
      | some e => (machine, .err e)
      | none =>
          if deferSyncAllowed then
            let deferrable :=
              match machine.state with
              | .inTx c =>
                  c.phase == .prepared &&
                  c.depth == 1 &&
                  c.pendingSync &&
                  c.pendingRecordCount > 0 &&
                  !bufLenExceedsChunk
              | .idle => false
            if !deferrable then
              (machine, .ok [.fallbackFullCommit])
            else if machine.health == .poisoned then
              (machine, .err .poisoned)
            else if !canGroup then
              (machine, .ok [.drainPendingGroup])
            else
              (machine, .ok [])
          else
            match machine.state with
            | .idle => (machine, .ok [])
            | .inTx c =>
                if !c.isActive then
                  (machine, .ok [])
                else if c.depth > 1 then
                  let c' := { c with depth := c.depth - 1 }
                  ({ machine with state := .inTx c' }, .ok [.decDepthDone])
                else if machine.health == .poisoned then
                  (machine, .err .poisoned)
                else if c.phase != .prepared then
                  let effDur := c.effectiveDurability .bufferedFast
                  (machine, .ok [.ensurePreparedBeforeCommit effDur])
                else if c.pendingRecordCount == 0 && !hasMetadataChanges then
                  (machine, .ok [.clearEmptyOverlay])
                else
                  (machine, .ok [.drainPendingGroup])
  | .enterWalFlush deferSyncBarrier =>
      if machine.health == .poisoned then
        (machine, .err .poisoned)
      else
        match machine.state with
        | .idle => (machine, .err .invalidPhase)
        | .inTx c =>
            let dur := if c.pendingSync then WriteDurability.syncTransactional else WriteDurability.bufferedFast
            let rc := c.pendingRecordCount
            let c' :=
              { c with
                phase := .flushingWal
                pendingRecordCount := 0
                pendingSync := false }
            ({ machine with state := .inTx c' }, .ok [.flushWal dur rc deferSyncBarrier])
  | .completeWalFlush mode =>
      match machine.state with
      | .idle => (machine, .err .invalidPhase)
      | .inTx c =>
          if c.phase != .flushingWal then
            (machine, .err .invalidPhase)
          else
            match mode with
            | .publishImmediate =>
                ({ machine with state := .idle }, .ok [.publishCommittedTx])
            | .awaitSync =>
                ({ machine with state := .inTx { c with phase := .syncInFlight } }, .ok [.returnAwaitingSync])
            | .handOffGroup =>
                ({ machine with state := .idle }, .ok [.handOffPendingGroup])
  | .finishSyncCommit ok =>
      let m1 : TxMachine :=
        match machine.state with
        | .inTx c =>
            if c.phase == .syncInFlight then
              { machine with state := .inTx { c with phase := .flushingWal } }
            else
              machine
        | .idle => machine
      if m1.health == .poisoned then
        (m1, .err .poisoned)
      else if !m1.state.isInTx then
        (m1, .err .invalidPhase)
      else if ok then
        ({ m1 with state := .idle }, .ok [.publishCommittedTx])
      else
        ({ health := .poisoned, state := .idle },
          .ok [.poisonEngineSyncFailed, .restoreStartMetaAndRelease])
  | .rollback connId =>
      let connErr :=
        match machine.state with
        | .inTx c => if c.isActive then checkActiveConn c connId else none
        | .idle => none
      match connErr with
      | some e => (machine, .err e)
      | none =>
          match machine.state with
          | .idle => (machine, .ok [])
          | .inTx c =>
              if c.phase == .syncInFlight then
                ({ health := .poisoned, state := .idle },
                  .ok [.poisonEngineInterruptedSync, .restoreStartMetaAndRelease])
              else
                ({ machine with state := .idle }, .ok [.restoreStartMetaAndRelease])
  | .markPoisoned =>
      ({ machine with health := .poisoned }, .ok [])

def stepList : TxMachine → List TxEvent → TxMachine
  | m, [] => m
  | m, ev :: rest => stepList (step m ev).1 rest

inductive Reachable : TxMachine → Prop where
  | init : Reachable TxMachine.init
  | step {m : TxMachine} (ev : TxEvent) : Reachable m → Reachable (step m ev).1

@[simp] def txWellFormedCoreBool (c : TxCore) : Bool :=
  decide (c.depth ≥ 1) &&
  (c.statementSavepointActive == c.savepoints.any (·.isStatement))

@[simp] def txWellFormedBool (m : TxMachine) : Bool :=
  match m.state with
  | .idle => true
  | .inTx c => txWellFormedCoreBool c

def TxWellFormed (m : TxMachine) : Prop :=
  txWellFormedBool m = true

@[simp] def isStorageMutatingEffect : TxEffect → Bool
  | .appendTxBatch _ _ => true
  | .runPrepare _ => true
  | .ensurePreparedBeforeCommit _ => true
  | .flushWal _ _ _ => true
  | .publishCommittedTx => true
  | .handOffPendingGroup => true
  | .returnAwaitingSync => true
  | _ => false

theorem tx_well_formed_init : TxWellFormed TxMachine.init := by
  simp [TxWellFormed]

theorem tx_well_formed_step (m : TxMachine) (ev : TxEvent)
    (hwf : TxWellFormed m) : TxWellFormed (step m ev).1 := by
  rcases m with ⟨health, state⟩
  unfold TxWellFormed at hwf ⊢
  cases ev with
  | begin =>
      cases state with
      | idle => simp [step]
      | inTx c =>
          simp only [step, txWellFormedBool, txWellFormedCoreBool, Bool.and_eq_true,
            decide_eq_true_eq, beq_iff_eq] at hwf ⊢
          exact ⟨by omega, hwf.2⟩
  | beginForConn connId =>
      by_cases hp : health == .poisoned
      · simpa [step, hp] using hwf
      · cases state with
        | idle => simp [step, hp]
        | inTx c =>
            have hwfParts := hwf
            simp only [txWellFormedBool, txWellFormedCoreBool, Bool.and_eq_true,
              decide_eq_true_eq, beq_iff_eq] at hwfParts
            simp only [step, hp, Bool.false_eq_true, ↓reduceIte]
            split
            · exact hwf
            · simp only [txWellFormedBool, txWellFormedCoreBool, Bool.and_eq_true,
                decide_eq_true_eq, beq_iff_eq]
              exact ⟨by omega, hwfParts.2⟩
  | beginVtab connId vtabId =>
      by_cases hp : health == .poisoned
      · simpa [step, hp] using hwf
      · cases state with
        | idle => simp [step, hp]
        | inTx c =>
            simp only [step, hp, Bool.false_eq_true, ↓reduceIte]
            split
            · split
              · exact hwf
              · split <;> exact hwf
            · exact hwf
  | attachVtab connId vtabId =>
      cases state with
      | idle => simp [step]
      | inTx c =>
          simp only [step]
          split <;> exact hwf
  | detachVtab connId vtabId =>
      cases state with
      | idle => simp [step]
      | inTx c =>
          simp only [step]
          split <;> exact hwf
  | vtabSync vtabId dur =>
      cases state with
      | idle => simp [step]
      | inTx c =>
          simp only [step]
          split <;> exact hwf
  | recordWrite scope dur rc bb maxB =>
      dsimp only [step]
      split
      · exact hwf
      · cases state with
        | idle => simp
        | inTx c =>
            simp only [txWellFormedBool, txWellFormedCoreBool, Bool.and_eq_true,
              decide_eq_true_eq, beq_iff_eq] at hwf ⊢
            split
            · simp [TxCore.invalidatePrepare, hwf]
            · simp [hwf]
  | invalidatePrepare =>
      cases state with
      | idle => simp [step]
      | inTx c =>
          simp only [step]
          split
          · simpa [TxCore.invalidatePrepare, txWellFormedBool, txWellFormedCoreBool] using hwf
          · exact hwf
  | beginPrepare dur =>
      cases state with
      | idle => simp [step]
      | inTx c =>
          simp only [step]
          split
          · exact hwf
          · split
            · exact hwf
            · split <;> exact hwf
  | finishPrepare ok =>
      cases state with
      | idle => simp [step]
      | inTx c =>
          simp only [step]
          exact hwf
  | savepoint ownerConn spId isStmt checkPoison =>
      cases state with
      | idle =>
          dsimp only [step]
          split
          · exact hwf
          · simp [TxCore.refreshStatementSavepointActive]
      | inTx c =>
          have hwfParts := hwf
          simp only [txWellFormedBool, txWellFormedCoreBool, Bool.and_eq_true,
            decide_eq_true_eq, beq_iff_eq] at hwfParts
          dsimp only [step]
          split
          · exact hwf
          · split
            · split
              · exact hwf
              · split
                · simpa [TxCore.invalidatePrepare, txWellFormedBool, txWellFormedCoreBool,
                    Bool.and_eq_true, decide_eq_true_eq, beq_iff_eq] using hwfParts
                · simp [TxCore.refreshStatementSavepointActive, TxCore.invalidatePrepare, hwfParts]
            · exact hwf
  | releaseSavepoint connId spId =>
      cases state with
      | idle => simp [step]
      | inTx c =>
          have hwfParts := hwf
          simp only [txWellFormedBool, txWellFormedCoreBool, Bool.and_eq_true,
            decide_eq_true_eq, beq_iff_eq] at hwfParts
          simp only [step]
          split
          · exact hwf
          · split
            · exact hwf
            · split
              · exact hwf
              · simp [TxCore.refreshStatementSavepointActive, hwfParts]
  | rollbackToSavepoint connId spId =>
      cases state with
      | idle => simp [step]
      | inTx c =>
          have hwfParts := hwf
          simp only [txWellFormedBool, txWellFormedCoreBool, Bool.and_eq_true,
            decide_eq_true_eq, beq_iff_eq] at hwfParts
          simp only [step]
          split
          · exact hwf
          · split
            · exact hwf
            · split
              · split
                · simpa [TxCore.invalidatePrepare, txWellFormedBool, txWellFormedCoreBool,
                    Bool.and_eq_true, decide_eq_true_eq, beq_iff_eq] using hwfParts
                · simp [TxCore.invalidatePrepare, hwfParts]
              · simp [TxCore.refreshStatementSavepointActive, TxCore.invalidatePrepare, hwfParts]
  | planCommit connId deferSyncAllowed canGroup hasMeta bufExceeds =>
      cases state with
      | idle =>
          dsimp only [step]
          split
          · split
            · simp
            · split
              · simp
              · split <;> simp
          · simp
      | inTx c =>
          have hwfParts := hwf
          simp only [txWellFormedBool, txWellFormedCoreBool, Bool.and_eq_true,
            decide_eq_true_eq, beq_iff_eq] at hwfParts
          dsimp only [step]
          split
          · exact hwf
          · split
            · split
              · exact hwf
              · split
                · exact hwf
                · split <;> exact hwf
            · split
              · exact hwf
              · split
                · next hGt =>
                    simp only [txWellFormedBool, txWellFormedCoreBool, Bool.and_eq_true,
                      decide_eq_true_eq, beq_iff_eq]
                    exact ⟨by omega, hwfParts.2⟩
                · split
                  · exact hwf
                  · split
                    · exact hwf
                    · split <;> exact hwf
  | enterWalFlush deferSyncBarrier =>
      dsimp only [step]
      split
      · exact hwf
      · cases state with
        | idle => simp
        | inTx c => exact hwf
  | completeWalFlush mode =>
      cases state with
      | idle => simp [step]
      | inTx c =>
          simp only [step]
          split
          · exact hwf
          · cases mode with
            | publishImmediate => simp
            | awaitSync => exact hwf
            | handOffGroup => simp
  | finishSyncCommit ok =>
      cases state with
      | idle =>
          dsimp only [step]
          split
          · simp
          · simp
      | inTx c =>
          dsimp only [step]
          by_cases hsf : c.phase == .syncInFlight
          · simp only [hsf, ↓reduceIte]
            split
            · exact hwf
            · split
              · exact hwf
              · split <;> simp
          · simp only [hsf, Bool.false_eq_true, ↓reduceIte]
            split
            · exact hwf
            · split
              · exact hwf
              · split <;> simp
  | rollback connId =>
      cases state with
      | idle =>
          dsimp only [step]
          simp
      | inTx c =>
          dsimp only [step]
          split
          · exact hwf
          · split <;> simp
  | markPoisoned =>
      simpa [step] using hwf

theorem tx_well_formed_reachable {m : TxMachine} (h : Reachable m) : TxWellFormed m := by
  induction h with
  | init => exact tx_well_formed_init
  | step ev _ ih => exact tx_well_formed_step _ ev ih

/-! ### INV-TX-1: Idle Cleanliness -/

theorem inv_tx_1_rollback_clears_to_idle (m : TxMachine) (connId : Option Nat)
    (effs : List TxEffect) (h : (step m (.rollback connId)).2 = .ok effs) :
    (step m (.rollback connId)).1.state = .idle := by
  rcases m with ⟨health, state⟩
  cases state with
  | idle => simp [step]
  | inTx c =>
      dsimp only [step] at h ⊢
      split at h
      · contradiction
      · split <;> simp

theorem inv_tx_1_finish_commit_clears_to_idle (m : TxMachine) (ok : Bool)
    (effs : List TxEffect) (h : (step m (.finishSyncCommit ok)).2 = .ok effs) :
    (step m (.finishSyncCommit ok)).1.state = .idle := by
  rcases m with ⟨health, state⟩
  cases state with
  | idle =>
      dsimp only [step] at h ⊢
      split at h <;> contradiction
  | inTx c =>
      dsimp only [step] at h ⊢
      by_cases hp : health == .poisoned
      · by_cases hsf : c.phase == .syncInFlight <;> simp [hsf, hp] at h
      · by_cases hsf : c.phase == .syncInFlight <;>
          simp only [hsf, hp, ↓reduceIte, Bool.false_eq_true, TxCoreState.isInTx, Bool.not_true] at h ⊢ <;>
          (cases ok <;> simp)

theorem inv_tx_1_complete_flush_non_await_clears_to_idle (m : TxMachine)
    (mode : FlushCompleteMode) (effs : List TxEffect)
    (hmode : mode ≠ .awaitSync)
    (h : (step m (.completeWalFlush mode)).2 = .ok effs) :
    (step m (.completeWalFlush mode)).1.state = .idle := by
  rcases m with ⟨health, state⟩
  cases state with
  | idle => simp [step] at h
  | inTx c =>
      dsimp only [step] at h ⊢
      by_cases hfl : c.phase != .flushingWal <;>
        simp only [hfl, ↓reduceIte, Bool.false_eq_true] at h ⊢
      · contradiction
      · cases mode with
        | publishImmediate => simp
        | awaitSync => exact False.elim (hmode rfl)
        | handOffGroup => simp

/-! ### INV-TX-2: Sync Durability Monotonicity Across InternalBypass & Prepare -/

theorem inv_tx_2_internal_bypass_preserves_prepare_and_upgrade
    (m : TxMachine) (c : TxCore) (dur : WriteDurability) (rc bb maxB : Nat)
    (effs : List TxEffect)
    (hState : m.state = .inTx c)
    (hPhase : c.phase = .prepared)
    (hStep : (step m (.recordWrite .internalBypass dur rc bb maxB)).2 = .ok effs) :
    ∃ c',
      (step m (.recordWrite .internalBypass dur rc bb maxB)).1.state = .inTx c' ∧
      c'.phase = .prepared ∧
      c'.syncedVtabs = c.syncedVtabs ∧
      c'.syncDurabilityUpgrade = c.syncDurabilityUpgrade ∧
      (c.pendingSync = true → c'.pendingSync = true) := by
  rcases m with ⟨health, state⟩
  simp only at hState
  subst hState
  dsimp only [step] at hStep ⊢
  simp only [TxCoreState.core?] at hStep ⊢
  cases hc : canRecordWrite health (some c) .internalBypass rc bb maxB with
  | error e => simp [hc] at hStep
  | ok resetBuf =>
      dsimp only
      refine ⟨_, rfl, hPhase, rfl, rfl, ?_⟩
      intro hps
      cases hd : (dur == .syncTransactional) with
      | true => simp
      | false => simp [hps]

theorem inv_tx_2_prepare_honors_sync_upgrade
    (m : TxMachine) (c : TxCore) (dur : WriteDurability)
    (hHealthy : m.health = .healthy)
    (hState : m.state = .inTx c)
    (hActive : c.isActive = true)
    (hNeed : c.phase ≠ .prepared ∨ ((dur = .syncTransactional ∨ c.syncDurabilityUpgrade = true) ∧ c.pendingSync = false))
    (hSync : dur = .syncTransactional ∨ c.syncDurabilityUpgrade = true ∨ c.pendingSync = true) :
    (step m (.beginPrepare dur)).2 = .ok [.runPrepare .syncTransactional] := by
  rcases m with ⟨health, state⟩
  simp only at hHealthy hState
  subst hHealthy hState
  have hNotAct : (!c.isActive) = false := by simp [hActive]
  have hReq : (dur == .syncTransactional || c.syncDurabilityUpgrade || c.pendingSync) = true := by
    rcases hSync with rfl | h1 | h2
    · simp
    · simp [h1]
    · simp [h2]
  dsimp only [step]
  simp only [hNotAct, Bool.false_eq_true, ↓reduceIte]
  have hSkip : (c.phase == .prepared && !((dur == .syncTransactional || c.syncDurabilityUpgrade) && !c.pendingSync)) = false := by
    rcases hNeed with hnp | ⟨hReq2, hNotPend⟩
    · have hnpB : (c.phase == .prepared) = false := beq_false_of_ne hnp
      simp [hnpB]
    · have hReqB : (dur == .syncTransactional || c.syncDurabilityUpgrade) = true := by
        rcases hReq2 with rfl | h1
        · simp
        · simp [h1]
      simp [hReqB, hNotPend]
  have hNotPois : (HealthState.healthy == HealthState.poisoned) = false := rfl
  simp only [hNotPois, Bool.false_eq_true, ↓reduceIte, hSkip]
  rcases hSync with rfl | hup | hpend
  · simp
  · simp [hup]
  · simp [hpend]

/-! ### INV-TX-3: 2PC Exclusivity, Budget Rejection, and Rollback Poisoning -/

theorem inv_tx_3_normal_write_budget_reject_preserves_state
    (m : TxMachine) (c : TxCore) (dur : WriteDurability) (rc bb maxB : Nat)
    (hHealthy : m.health = .healthy)
    (hState : m.state = .inTx c)
    (hActive : c.isActive = true)
    (hPhase : c.phase = .prepared)
    (hMax : maxB > 0)
    (hOver : c.pendingBytes + bb > maxB) :
    step m (.recordWrite .normal dur rc bb maxB) = (m, .err .txBudgetExceeded) := by
  have hp : (m.health == .poisoned) = false := by simp [hHealthy]
  have hnp : c.isPreparing = false := by simp [TxCore.isPreparing, hPhase]
  have hna : (!c.isActive) = false := by simp [hActive]
  simp [step, canRecordWrite, hState, hp, hna, hnp, hMax, hOver]

theorem inv_tx_3_normal_write_invalidates_prepare
    (m : TxMachine) (c : TxCore) (dur : WriteDurability) (rc bb maxB : Nat)
    (effs : List TxEffect)
    (hState : m.state = .inTx c)
    (hNotPreparing : c.phase ≠ .preparing)
    (hStep : (step m (.recordWrite .normal dur rc bb maxB)).2 = .ok effs) :
    ∃ c',
      (step m (.recordWrite .normal dur rc bb maxB)).1.state = .inTx c' ∧
      c'.phase = .active ∧
      c'.syncedVtabs = [] ∧
      c'.syncDurabilityUpgrade = false := by
  rcases m with ⟨health, state⟩
  simp only at hState
  subst hState
  have hnp : c.isPreparing = false := by simp [TxCore.isPreparing, hNotPreparing]
  dsimp only [step] at hStep ⊢
  simp only [TxCoreState.core?] at hStep ⊢
  cases hc : canRecordWrite health (some c) .normal rc bb maxB with
  | error e => simp [hc] at hStep
  | ok resetBuf =>
      dsimp only
      have hnb : (WriteScope.normal == WriteScope.internalBypass) = false := rfl
      simp only [hnb, hnp, Bool.or_false, Bool.not_false, ↓reduceIte, TxCore.invalidatePrepare]
      exact ⟨_, rfl, rfl, rfl, rfl⟩

theorem inv_tx_3_sync_in_flight_rollback_poisons
    (m : TxMachine) (c : TxCore) (connId : Option Nat)
    (hState : m.state = .inTx c)
    (hPhase : c.phase = .syncInFlight)
    (hConn : ∀ cid, connId = some cid → c.ownerConn = some cid) :
    (step m (.rollback connId)).1.health = .poisoned ∧
    (step m (.rollback connId)).1.state = .idle ∧
    TxEffect.poisonEngineInterruptedSync ∈ (step m (.rollback connId)).2.effects := by
  have hErr : checkActiveConn c connId = none := by
    cases connId with
    | none => simp [checkActiveConn]
    | some cid => simp [checkActiveConn, hConn cid rfl]
  rcases m with ⟨health, state⟩
  simp only at hState
  subst hState
  simp [step, TxCore.isActive, hPhase, hErr]

/-! ### INV-TX-4: Poison Absorption -/

theorem inv_tx_4_poison_monotonic (m : TxMachine) (ev : TxEvent)
    (hPoison : m.health = .poisoned) :
    (step m ev).1.health = .poisoned := by
  rcases m with ⟨health, state⟩
  simp only at hPoison
  subst hPoison
  cases ev <;> cases state <;> dsimp only [step] <;>
    repeat (first
      | rfl
      | split <;> try rfl
      | (next => try rfl))

theorem inv_tx_4_poison_monotonic_trace (m : TxMachine) (evs : List TxEvent)
    (hPoison : m.health = .poisoned) :
    (stepList m evs).health = .poisoned := by
  induction evs generalizing m with
  | nil => simpa [stepList] using hPoison
  | cons ev rest ih =>
      simp only [stepList]
      exact ih (step m ev).1 (inv_tx_4_poison_monotonic m ev hPoison)

theorem inv_tx_4_poison_no_mutating_effects
    (m : TxMachine) (ev : TxEvent)
    (hPoison : m.health = .poisoned)
    (hNotFlushing : ∀ c, m.state = .inTx c → c.phase ≠ .flushingWal) :
    ∀ eff ∈ (step m ev).2.effects, isStorageMutatingEffect eff = false := by
  rcases m with ⟨health, state⟩
  simp only at hPoison hNotFlushing
  subst hPoison
  cases ev with
  | begin =>
      cases state <;> simp [step]
  | beginForConn connId =>
      simp [step]
  | beginVtab connId vtabId =>
      simp [step]
  | attachVtab connId vtabId =>
      cases state with
      | idle => simp [step]
      | inTx c => dsimp only [step]; split <;> simp
  | detachVtab connId vtabId =>
      cases state with
      | idle => simp [step]
      | inTx c => dsimp only [step]; split <;> simp
  | vtabSync vtabId dur =>
      cases state with
      | idle => simp [step]
      | inTx c => dsimp only [step]; split <;> simp
  | recordWrite scope dur rc bb maxB =>
      simp [step, canRecordWrite]
  | invalidatePrepare =>
      cases state with
      | idle => simp [step]
      | inTx c => dsimp only [step]; split <;> simp
  | beginPrepare dur =>
      cases state with
      | idle => simp [step]
      | inTx c => dsimp only [step]; split <;> simp
  | finishPrepare ok =>
      cases state <;> simp [step]
  | savepoint ownerConn spId isStmt checkPoison =>
      cases state with
      | idle =>
          dsimp only [step]
          split <;> simp
      | inTx c =>
          dsimp only [step]
          split
          · simp
          · split
            · split
              · simp
              · split <;> simp
            · simp
  | releaseSavepoint connId spId =>
      cases state with
      | idle => simp [step]
      | inTx c =>
          dsimp only [step]
          split
          · simp
          · split
            · simp
            · split
              · simp
              · simp only [TxOutcome.effects]
                split <;> simp
  | rollbackToSavepoint connId spId =>
      cases state with
      | idle => simp [step]
      | inTx c =>
          dsimp only [step]
          split
          · simp
          · split
            · simp
            · split
              · split <;> simp
              · simp
  | planCommit connId deferSyncAllowed canGroup hasMeta bufExceeds =>
      cases state with
      | idle =>
          dsimp only [step]
          split
          · split
            · simp
            · simp
          · simp
      | inTx c =>
          dsimp only [step]
          split
          · simp
          · split
            · split
              · simp
              · simp
            · split
              · simp
              · split
                · simp
                · simp
  | enterWalFlush deferSyncBarrier =>
      simp [step]
  | completeWalFlush mode =>
      cases state with
      | idle => simp [step]
      | inTx c =>
          have hnf := hNotFlushing c rfl
          simp [step, hnf]
  | finishSyncCommit ok =>
      cases state with
      | idle => simp [step]
      | inTx c =>
          dsimp only [step]
          by_cases hsf : c.phase == .syncInFlight
          · simp [hsf]
          · simp [hsf]
  | rollback connId =>
      cases state with
      | idle => simp [step]
      | inTx c =>
          dsimp only [step]
          split
          · simp
          · split <;> simp
  | markPoisoned =>
      simp [step]

/-! ### INV-TX-5: Savepoint Stack Discipline & Watermark Restoration -/

theorem inv_tx_5_statement_active_iff {m : TxMachine} {c : TxCore}
    (hwf : TxWellFormed m) (hState : m.state = .inTx c) :
    c.statementSavepointActive = true ↔ ∃ f ∈ c.savepoints, f.isStatement = true := by
  unfold TxWellFormed at hwf
  simp [hState, txWellFormedBool, txWellFormedCoreBool, Bool.and_eq_true, beq_iff_eq] at hwf
  rw [hwf.2]
  exact List.any_eq_true

theorem inv_tx_5_release_truncates_and_preserves_owner
    (m : TxMachine) (c : TxCore) (connId : Option Nat) (spId : Int) (effs : List TxEffect)
    (hState : m.state = .inTx c)
    (hStep : (step m (.releaseSavepoint connId spId)).2 = .ok effs) :
    ∃ c',
      (step m (.releaseSavepoint connId spId)).1.state = .inTx c' ∧
      c'.savepoints.length ≤ c.savepoints.length ∧
      c'.ownerConn = c.ownerConn := by
  rcases m with ⟨health, state⟩
  simp only at hState
  subst hState
  dsimp only [step] at hStep ⊢
  by_cases ha : !c.isActive
  · simp only [ha, ↓reduceIte]
    exact ⟨c, rfl, Nat.le_refl _, rfl⟩
  · simp only [ha, Bool.false_eq_true, ↓reduceIte] at hStep ⊢
    cases hConn : checkActiveConn c connId with
    | some e => simp [hConn] at hStep
    | none =>
        dsimp only
        cases hF :
            (match findFrameWithIdx? (fun s => s.spId == spId) c.savepoints 0 with
             | some x => some x
             | none => findFrameWithIdx? (fun s => decide (s.spId > spId)) c.savepoints 0) with
        | none => exact ⟨c, rfl, Nat.le_refl _, rfl⟩
        | some pf =>
            refine ⟨_, rfl, ?_, rfl⟩
            simp [TxCore.refreshStatementSavepointActive, List.length_take]
            exact Nat.min_le_right _ _

theorem inv_tx_5_rollback_to_restores_watermark
    (m : TxMachine) (c : TxCore) (connId : Option Nat) (spId : Int)
    (pos : Nat) (marker : SavepointFrame) (effs : List TxEffect)
    (hState : m.state = .inTx c)
    (hFound : findFrameWithIdx? (fun s => s.spId == spId) c.savepoints 0 = some (pos, marker))
    (hStep : (step m (.rollbackToSavepoint connId spId)).2 = .ok effs)
    (hRun : c.isActive = true ∨ c.phase = .flushingWal) :
    ∃ c',
      (step m (.rollbackToSavepoint connId spId)).1.state = .inTx c' ∧
      c'.pendingRecordCount = marker.recordCount ∧
      c'.pendingBytes = marker.pendingBytes ∧
      c'.pendingSync = marker.pendingSync ∧
      c'.savepoints = c.savepoints.take (pos + 1) ∧
      c'.phase = .active ∧
      c'.syncedVtabs = [] ∧
      c'.syncDurabilityUpgrade = false := by
  rcases m with ⟨health, state⟩
  simp only at hState
  subst hState
  dsimp only [step] at hStep ⊢
  split at hStep
  · contradiction
  · have hNotSkip : (!c.isActive && c.phase != .flushingWal) = false := by
      rcases hRun with h | h <;> simp [TxCore.isActive, h]
    simp only [hNotSkip, Bool.false_eq_true, ↓reduceIte] at hStep ⊢
    have hInvSp : (TxCore.invalidatePrepare c).savepoints = c.savepoints := rfl
    simp only [hInvSp, hFound]
    exact ⟨_, rfl, rfl, rfl, rfl, rfl, rfl, rfl, rfl⟩

end AydbFormal.TxFsm
