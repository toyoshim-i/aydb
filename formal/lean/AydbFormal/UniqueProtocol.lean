set_option autoImplicit false

/-!
  SM-2: Formal model of `UniqueReadyLatch`, `unique_marker_gen`, `unsupported` DDL handling,
  and `ConnDdlSlot` (`src/vtab/mod.rs`, `src/engine/unique_index.rs`), with proofs of
  `INV-VTAB-1` through `INV-VTAB-3`.
-/

namespace AydbFormal.UniqueProtocol

structure VtabHandle where
  connId : Nat
  schemaSig : Option Nat
  verifiedGen : Nat
  unsupported : Bool
  deriving DecidableEq, Repr, Inhabited

structure UniqueSystem where
  committedGen : Nat
  committedMarker : Option Nat
  txOwner : Option Nat
  txModifiedMarker : Bool
  ownerMarker : Option Nat
  handles : Nat → VtabHandle

def UniqueSystem.init
    (schemaSig : Nat → Option Nat)
    (connId : Nat → Nat)
    (unsup : Nat → Bool)
    (initialMarker : Option Nat) : UniqueSystem :=
  { committedGen := 1
    committedMarker := initialMarker
    txOwner := none
    txModifiedMarker := false
    ownerMarker := initialMarker
    handles := fun hid =>
      { connId := connId hid
        schemaSig := schemaSig hid
        verifiedGen := 0
        unsupported := unsup hid } }

inductive UniqueEvent where
  | beginTx (connId : Nat)
  | latchFromCommitted (hid : Nat)
  | writeRow (hid : Nat)
  | updateMarker (connId : Nat) (newMarker : Option Nat)
  | commitTx
  | rollbackTx
  | disconnectInvalidate (hid : Nat)
  deriving DecidableEq, Repr, Inhabited

def setHandleGen (handles : Nat → VtabHandle) (hid newGen : Nat) : Nat → VtabHandle :=
  fun k => if k == hid then { handles hid with verifiedGen := newGen } else handles k

def latchReadyFor (sys : UniqueSystem) (hid : Nat) : Bool :=
  let h := sys.handles hid
  sys.committedGen != 0 && h.verifiedGen == sys.committedGen

def planUniqueReady (sys : UniqueSystem) (hid : Nat) : Bool :=
  let h := sys.handles hid
  if sys.txOwner == some h.connId && sys.txModifiedMarker then
    sys.ownerMarker == h.schemaSig
  else
    latchReadyFor sys hid

def stepUnique (sys : UniqueSystem) (ev : UniqueEvent) : UniqueSystem × Bool :=
  match ev with
  | .beginTx connId =>
      match sys.txOwner with
      | none =>
          ({ sys with
             txOwner := some connId
             txModifiedMarker := false
             ownerMarker := sys.committedMarker }, true)
      | some cid =>
          (sys, cid == connId)
  | .latchFromCommitted hid =>
      let h := sys.handles hid
      let allowLatch := !sys.txModifiedMarker && sys.committedGen != 0
      if sys.committedMarker == h.schemaSig then
        let handles' :=
          if allowLatch then setHandleGen sys.handles hid sys.committedGen
          else sys.handles
        ({ sys with handles := handles' }, true)
      else
        ({ sys with handles := setHandleGen sys.handles hid 0 }, false)
  | .writeRow hid =>
      let h := sys.handles hid
      if h.unsupported then
        (sys, false)
      else
        (sys, true)
  | .updateMarker connId newMarker =>
      match sys.txOwner with
      | some cid =>
          if cid == connId then
            ({ sys with
               ownerMarker := newMarker
               txModifiedMarker := true }, true)
          else
            (sys, false)
      | none =>
          ({ sys with
             committedMarker := newMarker
             committedGen := sys.committedGen + 1
             ownerMarker := newMarker }, true)
  | .commitTx =>
      match sys.txOwner with
      | none => (sys, false)
      | some _ =>
          if sys.txModifiedMarker then
            ({ sys with
               committedMarker := sys.ownerMarker
               committedGen := sys.committedGen + 1
               txOwner := none
               txModifiedMarker := false }, true)
          else
            ({ sys with
               txOwner := none
               txModifiedMarker := false }, true)
  | .rollbackTx =>
      match sys.txOwner with
      | none => (sys, false)
      | some _ =>
          let nextGen :=
            if sys.txModifiedMarker then sys.committedGen + 1 else sys.committedGen
          ({ sys with
             committedGen := nextGen
             txOwner := none
             txModifiedMarker := false
             ownerMarker := sys.committedMarker }, true)
  | .disconnectInvalidate hid =>
      ({ sys with handles := setHandleGen sys.handles hid 0 }, true)

inductive ReachableUnique (schemaSig : Nat → Option Nat) (connId : Nat → Nat)
    (unsup : Nat → Bool) (initialMarker : Option Nat) : UniqueSystem → Prop where
  | init : ReachableUnique schemaSig connId unsup initialMarker
      (UniqueSystem.init schemaSig connId unsup initialMarker)
  | step {sys : UniqueSystem} (ev : UniqueEvent) :
      ReachableUnique schemaSig connId unsup initialMarker sys →
      ReachableUnique schemaSig connId unsup initialMarker (stepUnique sys ev).1

def UniqueWellFormed (sys : UniqueSystem) : Prop :=
  sys.committedGen ≥ 1 ∧
  (∀ hid,
    let h := sys.handles hid
    h.verifiedGen ≤ sys.committedGen ∧
    (h.verifiedGen = sys.committedGen → sys.committedMarker = h.schemaSig))

theorem unique_well_formed_init
    (schemaSig : Nat → Option Nat) (connId : Nat → Nat) (unsup : Nat → Bool)
    (initialMarker : Option Nat) :
    UniqueWellFormed (UniqueSystem.init schemaSig connId unsup initialMarker) := by
  unfold UniqueWellFormed
  refine ⟨Nat.le_refl 1, ?_⟩
  intro hid
  simp [UniqueSystem.init]

theorem unique_well_formed_step (sys : UniqueSystem) (ev : UniqueEvent)
    (hwf : UniqueWellFormed sys) : UniqueWellFormed (stepUnique sys ev).1 := by
  unfold UniqueWellFormed at hwf ⊢
  rcases hwf with ⟨hGen, hHandles⟩
  cases ev with
  | beginTx connId =>
      cases hOwn : sys.txOwner with
      | none =>
          simp only [stepUnique, hOwn]
          exact ⟨hGen, hHandles⟩
      | some cid =>
          simp only [stepUnique, hOwn]
          exact ⟨hGen, hHandles⟩
  | latchFromCommitted hid =>
      simp only [stepUnique]
      by_cases hEq : sys.committedMarker == (sys.handles hid).schemaSig
      · simp only [hEq, ↓reduceIte]
        by_cases hAllow : (!sys.txModifiedMarker && sys.committedGen != 0)
        · simp only [hAllow, ↓reduceIte]
          refine ⟨hGen, ?_⟩
          intro k
          simp only [setHandleGen]
          by_cases hk : k == hid
          · have hkeq : k = hid := beq_iff_eq.mp hk
            subst hkeq
            simp only [beq_self_eq_true, ↓reduceIte]
            exact ⟨Nat.le_refl _, fun _ => beq_iff_eq.mp hEq⟩
          · simp only [hk, Bool.false_eq_true, ↓reduceIte]
            exact hHandles k
        · simp only [hAllow, Bool.false_eq_true, ↓reduceIte]
          exact ⟨hGen, hHandles⟩
      · simp only [hEq, Bool.false_eq_true, ↓reduceIte]
        refine ⟨hGen, ?_⟩
        intro k
        simp only [setHandleGen]
        by_cases hk : k == hid
        · have hkeq : k = hid := beq_iff_eq.mp hk
          subst hkeq
          simp only [beq_self_eq_true, ↓reduceIte]
          exact ⟨Nat.zero_le _, fun hZero => False.elim (by omega)⟩
        · simp only [hk, Bool.false_eq_true, ↓reduceIte]
          exact hHandles k
  | writeRow hid =>
      by_cases hu : (sys.handles hid).unsupported
      · simp only [stepUnique, hu, ↓reduceIte]
        exact ⟨hGen, hHandles⟩
      · simp only [stepUnique, hu, Bool.false_eq_true, ↓reduceIte]
        exact ⟨hGen, hHandles⟩
  | updateMarker connId newMarker =>
      cases hOwn : sys.txOwner with
      | some cid =>
          by_cases hcid : cid == connId
          · simp only [stepUnique, hOwn, hcid, ↓reduceIte]
            exact ⟨hGen, hHandles⟩
          · simp only [stepUnique, hOwn, hcid, Bool.false_eq_true, ↓reduceIte]
            exact ⟨hGen, hHandles⟩
      | none =>
          simp only [stepUnique, hOwn]
          refine ⟨by omega, ?_⟩
          intro hid
          have hk := hHandles hid
          exact ⟨by omega, fun hImpossible => False.elim (by omega)⟩
  | commitTx =>
      cases hOwn : sys.txOwner with
      | none =>
          simp only [stepUnique, hOwn]
          exact ⟨hGen, hHandles⟩
      | some _ =>
          by_cases hmod : sys.txModifiedMarker
          · simp only [stepUnique, hOwn, hmod, ↓reduceIte]
            refine ⟨by omega, ?_⟩
            intro hid
            have hk := hHandles hid
            exact ⟨by omega, fun hImpossible => False.elim (by omega)⟩
          · simp only [stepUnique, hOwn, hmod, Bool.false_eq_true, ↓reduceIte]
            exact ⟨hGen, hHandles⟩
  | rollbackTx =>
      cases hOwn : sys.txOwner with
      | none =>
          simp only [stepUnique, hOwn]
          exact ⟨hGen, hHandles⟩
      | some _ =>
          by_cases hmod : sys.txModifiedMarker
          · simp only [stepUnique, hOwn, hmod, ↓reduceIte]
            refine ⟨by omega, ?_⟩
            intro hid
            have hk := hHandles hid
            exact ⟨by omega, fun hImpossible => False.elim (by omega)⟩
          · simp only [stepUnique, hOwn, hmod, Bool.false_eq_true, ↓reduceIte]
            exact ⟨hGen, hHandles⟩
  | disconnectInvalidate hid =>
      simp only [stepUnique]
      refine ⟨hGen, ?_⟩
      intro k
      simp only [setHandleGen]
      by_cases hk : k == hid
      · have hkeq : k = hid := beq_iff_eq.mp hk
        subst hkeq
        simp only [beq_self_eq_true, ↓reduceIte]
        exact ⟨Nat.zero_le _, fun hZero => False.elim (by omega)⟩
      · simp only [hk, Bool.false_eq_true, ↓reduceIte]
        exact hHandles k

theorem unique_well_formed_reachable
    {schemaSig : Nat → Option Nat} {connId : Nat → Nat} {unsup : Nat → Bool}
    {initialMarker : Option Nat} {sys : UniqueSystem}
    (h : ReachableUnique schemaSig connId unsup initialMarker sys) :
    UniqueWellFormed sys := by
  induction h with
  | init => exact unique_well_formed_init schemaSig connId unsup initialMarker
  | step ev _ ih => exact unique_well_formed_step _ ev ih

/-! ### INV-VTAB-1: Unique Latch Soundness -/

theorem inv_vtab_1_unique_latch_soundness (sys : UniqueSystem) (hid : Nat)
    (hwf : UniqueWellFormed sys) (hReady : latchReadyFor sys hid = true) :
    sys.committedGen ≠ 0 ∧ sys.committedMarker = (sys.handles hid).schemaSig := by
  simp only [latchReadyFor, Bool.and_eq_true, bne_iff_ne, ne_eq, beq_iff_eq] at hReady
  exact ⟨hReady.1, (hwf.2 hid).2 hReady.2⟩

theorem inv_vtab_1_plan_ready_soundness (sys : UniqueSystem) (hid : Nat)
    (hwf : UniqueWellFormed sys) (hReady : planUniqueReady sys hid = true) :
    (if sys.txOwner == some (sys.handles hid).connId && sys.txModifiedMarker then
       sys.ownerMarker
     else
       sys.committedMarker) = (sys.handles hid).schemaSig := by
  unfold planUniqueReady at hReady
  by_cases hOwnerMod : sys.txOwner == some (sys.handles hid).connId && sys.txModifiedMarker
  · simp only [hOwnerMod, ↓reduceIte, beq_iff_eq] at hReady ⊢
    exact hReady
  · simp only [hOwnerMod, Bool.false_eq_true, ↓reduceIte] at hReady ⊢
    exact (inv_vtab_1_unique_latch_soundness sys hid hwf hReady).2

theorem inv_vtab_1_zero_never_ready (sys : UniqueSystem) (hid : Nat)
    (hZero : sys.committedGen = 0 ∨ (sys.handles hid).verifiedGen = 0) :
    latchReadyFor sys hid = false := by
  rcases hZero with hGenZero | hLatchZero
  · simp [latchReadyFor, hGenZero]
  · by_cases hGenZero : sys.committedGen == 0
    · have hEq : sys.committedGen = 0 := beq_iff_eq.mp hGenZero
      simp [latchReadyFor, hEq]
    · have hne : sys.committedGen ≠ 0 := fun h => hGenZero (beq_iff_eq.mpr h)
      simp [latchReadyFor, hLatchZero, hne.symm]

theorem inv_vtab_1_uncommitted_never_latches (sys : UniqueSystem) (hid : Nat)
    (hMod : sys.txModifiedMarker = true) (hMatch : sys.committedMarker = (sys.handles hid).schemaSig) :
    (stepUnique sys (.latchFromCommitted hid)).1.handles hid = sys.handles hid := by
  simp [stepUnique, hMod, hMatch]

theorem inv_vtab_1_cross_conn_bump_invalidates (sys : UniqueSystem) (hid : Nat)
    (hwf : UniqueWellFormed sys) (hMod : sys.txModifiedMarker = true) (hOwn : sys.txOwner.isSome) :
    latchReadyFor (stepUnique sys .commitTx).1 hid = false := by
  obtain ⟨cid, hcid⟩ := Option.isSome_iff_exists.mp hOwn
  have hLe := (hwf.2 hid).1
  simp only [latchReadyFor, stepUnique, hcid, hMod, ↓reduceIte]
  have hNe : ((sys.handles hid).verifiedGen == sys.committedGen + 1) = false := by
    simp [beq_eq_false_iff_ne]
    omega
  simp [hNe]

/-! ### INV-VTAB-2: Unsupported DDL Handle Immutability -/

theorem inv_vtab_2_unsupported_immutability (sys : UniqueSystem) (hid : Nat)
    (hUnsup : (sys.handles hid).unsupported = true) :
    stepUnique sys (.writeRow hid) = (sys, false) := by
  simp [stepUnique, hUnsup]

/-! ### INV-VTAB-3: ConnDdlSlot Phase Rules and Fail-Closed Rename Reconcile -/

inductive ConnDdlPhase where
  | idle
  | stagedCreate (connId : Nat)
  | stagedDrop (connId : Nat)
  | stagedRename (connId : Nat)
  | explicitTx (commitHook : Option Nat) (renames : List Nat) (renamedHandles : List Nat)
  deriving DecidableEq, Repr, Inhabited

structure ConnDdlSlot where
  phase : ConnDdlPhase
  lastError : Option String
  deriving DecidableEq, Repr, Inhabited

def armTxCommit (slot : ConnDdlSlot) (connId : Nat) : ConnDdlSlot × Bool :=
  let s := { slot with lastError := none }
  match s.phase with
  | .idle => ({ s with phase := .explicitTx (some connId) [] [] }, true)
  | .explicitTx _ r h => ({ s with phase := .explicitTx (some connId) r h }, true)
  | .stagedCreate existing =>
      if existing == connId then
        ({ s with phase := .stagedCreate connId }, true)
      else
        (s, false)
  | .stagedDrop _ => (s, false)
  | .stagedRename _ => (s, false)

def disarmTxCommit (slot : ConnDdlSlot) : ConnDdlSlot :=
  match slot.phase with
  | .explicitTx _ r h =>
      let nextPhase :=
        if r.isEmpty && h.isEmpty then ConnDdlPhase.idle
        else ConnDdlPhase.explicitTx none r h
      { slot with phase := nextPhase }
  | _ => slot

def commitPending (slot : ConnDdlSlot) (ok : Bool) : ConnDdlSlot :=
  let s := { slot with lastError := none }
  match s.phase with
  | .idle => s
  | .stagedCreate _ => { s with phase := .idle }
  | .stagedDrop _ => { s with phase := .idle }
  | .stagedRename _ => { s with phase := .idle }
  | .explicitTx hook r h =>
      match hook with
      | none => s
      | some _ =>
          if ok then
            { s with phase := .idle }
          else
            { phase := .explicitTx none r h, lastError := some "commit failed" }

def reconcileRenamesLoop : List (Nat × Bool) → List Nat × Option String
  | [] => ([], none)
  | (id, ok) :: rest =>
      let (retryRest, errRest) := reconcileRenamesLoop rest
      if ok then
        (retryRest, errRest)
      else
        (id :: retryRest, some "rename revert failed")

def reconcileInTxRenames (slot : ConnDdlSlot) (results : List (Nat × Bool)) :
    ConnDdlSlot × Option String :=
  match slot.phase with
  | .explicitTx hook _ h =>
      let (retry, err) := reconcileRenamesLoop results
      ({ slot with phase := .explicitTx hook retry h }, err)
  | _ => (slot, none)

theorem inv_vtab_3_arm_tx_commit_rules (slot : ConnDdlSlot) (connId : Nat) :
    (∀ cid, slot.phase = .stagedCreate cid → (armTxCommit slot connId).2 = (cid == connId)) ∧
    (∀ cid, slot.phase = .stagedDrop cid → (armTxCommit slot connId).2 = false) ∧
    (∀ cid, slot.phase = .stagedRename cid → (armTxCommit slot connId).2 = false) := by
  refine ⟨?_, ?_, ?_⟩
  · intro cid hp
    simp only [armTxCommit, hp]
    split <;> simp_all
  · intro cid hp
    simp [armTxCommit, hp]
  · intro cid hp
    simp [armTxCommit, hp]

theorem inv_vtab_3_disarm_and_failed_commit_preserve_handles
    (slot : ConnDdlSlot) (hook : Option Nat) (r h : List Nat) (hh : h ≠ []) :
    (disarmTxCommit { slot with phase := .explicitTx hook r h }).phase = .explicitTx none r h ∧
    (∀ cid, hook = some cid →
      (commitPending { slot with phase := .explicitTx hook r h } false).phase = .explicitTx none r h) := by
  have hNotEmpty : h.isEmpty = false := by
    cases h with
    | nil => exact False.elim (hh rfl)
    | cons _ _ => rfl
  refine ⟨?_, ?_⟩
  · simp [disarmTxCommit, hNotEmpty]
  · intro cid hHook
    subst hHook
    simp [commitPending]

theorem reconcileRenamesLoop_keeps_failed (xs : List (Nat × Bool)) (id : Nat)
    (hMem : (id, false) ∈ xs) :
    id ∈ (reconcileRenamesLoop xs).1 ∧ (reconcileRenamesLoop xs).2.isSome = true := by
  induction xs with
  | nil => contradiction
  | cons p rest ih =>
      rcases p with ⟨pid, pok⟩
      simp only [reconcileRenamesLoop]
      rcases List.mem_cons.mp hMem with hpEq | hInRest
      · rcases Prod.mk.inj hpEq with ⟨rfl, rfl⟩
        simp
      · have ih' := ih hInRest
        cases pok with
        | true => simpa using ih'
        | false =>
            simp [ih'.1]

theorem inv_vtab_3_fail_closed_rename_reconcile
    (slot : ConnDdlSlot) (hook : Option Nat) (r h : List Nat)
    (results : List (Nat × Bool)) (id : Nat)
    (hPhase : slot.phase = .explicitTx hook r h)
    (hFail : (id, false) ∈ results) :
    let res := reconcileInTxRenames slot results
    res.2.isSome = true ∧
    ∃ r', res.1.phase = .explicitTx hook r' h ∧ id ∈ r' := by
  have hLoop := reconcileRenamesLoop_keeps_failed results id hFail
  simp only [reconcileInTxRenames, hPhase]
  exact ⟨hLoop.2, (reconcileRenamesLoop results).1, rfl, hLoop.1⟩

end AydbFormal.UniqueProtocol
