set_option autoImplicit false

/-!
  SM-3: Formal model of `WalLinks` (`src/engine/wal_links.rs`), two-stream WAL recovery & damage
  judgement (`src/wal/recovery.rs`), and salvage catalog taint propagation
  (`src/engine/salvage.rs`), proving `INV-WAL-1` through `INV-WAL-4`.
-/

namespace AydbFormal.WalRecovery

/-! ### Part 1: WalLinks State Machine (`src/engine/wal_links.rs`) -/

structure Unsynced where
  lsn : Nat
  prev : Nat
  deriving DecidableEq, Repr, Inhabited

inductive LogStream where
  | sync
  | buffered
  deriving DecidableEq, Repr, Inhabited

structure WalLinksState where
  lastAppended : Nat
  durable : Nat
  lastSyncAppend : Nat
  syncHead : Option Unsynced
  bufHead : Option Unsynced
  deriving DecidableEq, Repr, Inhabited

inductive WalLinksEvent where
  | appended (stream : LogStream) (lsn prev : Nat) (synced : Bool)
  | bufSynced
  | syncFlushed (through : Nat)
  | syncFlushedAll
  | rotated
  deriving DecidableEq, Repr, Inhabited

@[simp] def WalLinksState.init (base : Nat) : WalLinksState :=
  { lastAppended := base
    durable := base
    lastSyncAppend := 0
    syncHead := none
    bufHead := none }

@[simp] def WalLinksState.stamp (s : WalLinksState) : Nat × Nat :=
  (s.lastAppended, s.durable)

@[simp] def WalLinksState.advance (s : WalLinksState) : WalLinksState :=
  let bound :=
    match s.syncHead, s.bufHead with
    | none, none => s.lastAppended
    | some h, none => h.prev
    | none, some h => h.prev
    | some sy, some bu => if sy.lsn ≤ bu.lsn then sy.prev else bu.prev
  { s with durable := Nat.max s.durable bound }

def stepWalLinks (s : WalLinksState) (ev : WalLinksEvent) : WalLinksState × (Nat × Nat) :=
  let s' :=
    match ev with
    | .appended stream lsn prev synced =>
        match stream with
        | .sync =>
            let syncHead' :=
              if synced then none
              else
                match s.syncHead with
                | none => some { lsn := lsn, prev := prev }
                | some h => some h
            ({ s with
               lastAppended := lsn
               lastSyncAppend := lsn
               syncHead := syncHead' }).advance
        | .buffered =>
            let bufHead' :=
              if synced then none
              else
                match s.bufHead with
                | none => some { lsn := lsn, prev := prev }
                | some h => some h
            ({ s with
               lastAppended := lsn
               bufHead := bufHead' }).advance
    | .bufSynced =>
        ({ s with bufHead := none }).advance
    | .syncFlushed through =>
        let syncHead' :=
          match s.syncHead with
          | none => none
          | some head =>
              if head.lsn ≤ through then
                if s.lastSyncAppend > through then
                  some { lsn := through + 1, prev := through }
                else
                  none
              else
                some head
        ({ s with syncHead := syncHead' }).advance
    | .syncFlushedAll =>
        let through := s.lastSyncAppend
        let syncHead' :=
          match s.syncHead with
          | none => none
          | some head =>
              if head.lsn ≤ through then
                if s.lastSyncAppend > through then
                  some { lsn := through + 1, prev := through }
                else
                  none
              else
                some head
        ({ s with syncHead := syncHead' }).advance
    | .rotated =>
        ({ s with syncHead := none, bufHead := none }).advance
  (s', s'.stamp)

theorem inv_wal_1_links_durable_monotonic (s : WalLinksState) (ev : WalLinksEvent) :
    (stepWalLinks s ev).1.durable ≥ s.durable := by
  cases ev with
  | appended stream lsn prev synced =>
      cases stream <;> simp [stepWalLinks, WalLinksState.advance, Nat.le_max_left]
  | bufSynced => simp [stepWalLinks, WalLinksState.advance, Nat.le_max_left]
  | syncFlushed through => simp [stepWalLinks, WalLinksState.advance, Nat.le_max_left]
  | syncFlushedAll => simp [stepWalLinks, WalLinksState.advance, Nat.le_max_left]
  | rotated => simp [stepWalLinks, WalLinksState.advance, Nat.le_max_left]

theorem inv_wal_1_cross_stream_gap_holds_durable_back (s : WalLinksState)
    (b : Unsynced) (hBuf : s.bufHead = some b) (hSync : s.syncHead = none)
    (hPrior : s.durable ≤ b.prev) :
    s.advance.durable ≤ b.prev := by
  simp only [WalLinksState.advance, hBuf, hSync]
  exact Nat.max_le.mpr ⟨hPrior, Nat.le_refl _⟩

/-- Augmented model tracking currently unsynced LSNs in each stream. -/
structure WalLinksTraceState where
  links : WalLinksState
  unsyncedSync : List Nat
  unsyncedBuf : List Nat

def WalLinksTraceState.init (base : Nat) : WalLinksTraceState :=
  { links := WalLinksState.init base
    unsyncedSync := []
    unsyncedBuf := [] }

def WalEventValid (st : WalLinksTraceState) : WalLinksEvent → Prop
  | .appended _ lsn prev _ => lsn > st.links.lastAppended ∧ prev = st.links.lastAppended
  | _ => True

def stepWalLinksTrace (st : WalLinksTraceState) (ev : WalLinksEvent) : WalLinksTraceState :=
  match ev with
  | .appended stream lsn _ synced =>
      let links' := (stepWalLinks st.links ev).1
      match stream with
      | .sync =>
          let u := if synced then [] else st.unsyncedSync ++ [lsn]
          { st with links := links', unsyncedSync := u }
      | .buffered =>
          let u := if synced then [] else st.unsyncedBuf ++ [lsn]
          { st with links := links', unsyncedBuf := u }
  | .bufSynced =>
      { st with links := (stepWalLinks st.links ev).1, unsyncedBuf := [] }
  | .syncFlushed through =>
      { st with
        links := (stepWalLinks st.links ev).1
        unsyncedSync := st.unsyncedSync.filter (fun x => decide (x > through)) }
  | .syncFlushedAll =>
      { st with
        links := (stepWalLinks st.links ev).1
        unsyncedSync := [] }
  | .rotated =>
      { st with
        links := (stepWalLinks st.links ev).1
        unsyncedSync := []
        unsyncedBuf := [] }

def LinksSound (st : WalLinksTraceState) : Prop :=
  st.links.durable ≤ st.links.lastAppended ∧
  (∀ x ∈ st.unsyncedSync, st.links.durable < x ∧ x ≤ st.links.lastSyncAppend ∧ x ≤ st.links.lastAppended) ∧
  (∀ x ∈ st.unsyncedBuf, st.links.durable < x ∧ x ≤ st.links.lastAppended) ∧
  (match st.links.syncHead with
   | none => st.unsyncedSync = []
   | some h =>
       h.prev < h.lsn ∧
       (∃ m ∈ st.unsyncedSync, m = st.links.lastSyncAppend ∧ h.lsn ≤ m) ∧
       ∀ x ∈ st.unsyncedSync, h.lsn ≤ x) ∧
  (match st.links.bufHead with
   | none => st.unsyncedBuf = []
   | some h =>
       h.prev < h.lsn ∧
       (∃ b0 ∈ st.unsyncedBuf, h.lsn ≤ b0) ∧
       ∀ x ∈ st.unsyncedBuf, h.lsn ≤ x)

theorem links_sound_init (base : Nat) : LinksSound (WalLinksTraceState.init base) := by
  simp [LinksSound, WalLinksTraceState.init]

theorem links_sound_advance (links : WalLinksState) (uSync uBuf : List Nat)
    (hLast : links.durable ≤ links.lastAppended)
    (hSyncL : ∀ x ∈ uSync, links.durable < x ∧ x ≤ links.lastSyncAppend ∧ x ≤ links.lastAppended)
    (hBufL : ∀ x ∈ uBuf, links.durable < x ∧ x ≤ links.lastAppended)
    (hSyncHead : match links.syncHead with
      | none => uSync = []
      | some h =>
          h.prev < h.lsn ∧
          (∃ m ∈ uSync, m = links.lastSyncAppend ∧ h.lsn ≤ m) ∧
          ∀ x ∈ uSync, h.lsn ≤ x)
    (hBufHead : match links.bufHead with
      | none => uBuf = []
      | some h =>
          h.prev < h.lsn ∧
          (∃ b0 ∈ uBuf, h.lsn ≤ b0) ∧
          ∀ x ∈ uBuf, h.lsn ≤ x) :
    LinksSound { links := links.advance, unsyncedSync := uSync, unsyncedBuf := uBuf } := by
  have hBound :
      links.advance.durable ≤ links.lastAppended ∧
      (∀ x ∈ uSync, links.advance.durable < x) ∧
      (∀ x ∈ uBuf, links.advance.durable < x) := by
    cases hSH : links.syncHead with
    | none =>
        cases hBH : links.bufHead with
        | none =>
            simp only [hSH] at hSyncHead
            simp only [hBH] at hBufHead
            subst hSyncHead hBufHead
            simp [WalLinksState.advance, hSH, hBH, Nat.max_le.mpr ⟨hLast, Nat.le_refl _⟩]
        | some bh =>
            simp only [hSH] at hSyncHead
            simp only [hBH] at hBufHead
            subst hSyncHead
            rcases hBufHead with ⟨hbhLt, ⟨b0, hb0Mem, hbhLe⟩, hbAll⟩
            have hb0Bound := hBufL b0 hb0Mem
            refine ⟨?_, ?_, ?_⟩
            · simp only [WalLinksState.advance, hSH, hBH]
              exact Nat.max_le.mpr ⟨hLast, by omega⟩
            · intro x hx; contradiction
            · intro x hx
              have hxL := hBufL x hx
              have hxB := hbAll x hx
              simp only [WalLinksState.advance, hSH, hBH]
              exact Nat.max_lt.mpr ⟨hxL.1, by omega⟩
    | some sy =>
        cases hBH : links.bufHead with
        | none =>
            simp only [hSH] at hSyncHead
            simp only [hBH] at hBufHead
            subst hBufHead
            rcases hSyncHead with ⟨hsyLt, ⟨m, hmMem, _, hsyLe⟩, hsyAll⟩
            have hmBound := hSyncL m hmMem
            refine ⟨?_, ?_, ?_⟩
            · simp only [WalLinksState.advance, hSH, hBH]
              exact Nat.max_le.mpr ⟨hLast, by omega⟩
            · intro x hx
              have hxL := hSyncL x hx
              have hxS := hsyAll x hx
              simp only [WalLinksState.advance, hSH, hBH]
              exact Nat.max_lt.mpr ⟨hxL.1, by omega⟩
            · intro x hx; contradiction
        | some bh =>
            simp only [hSH] at hSyncHead
            simp only [hBH] at hBufHead
            rcases hSyncHead with ⟨hsyLt, ⟨m, hmMem, _, hsyLe⟩, hsyAll⟩
            rcases hBufHead with ⟨hbhLt, ⟨b0, hb0Mem, hbhLe⟩, hbAll⟩
            have hmBound := hSyncL m hmMem
            have hb0Bound := hBufL b0 hb0Mem
            by_cases hOrder : sy.lsn ≤ bh.lsn
            · refine ⟨?_, ?_, ?_⟩
              · simp only [WalLinksState.advance, hSH, hBH, hOrder, ↓reduceIte]
                exact Nat.max_le.mpr ⟨hLast, by omega⟩
              · intro x hx
                have hxL := hSyncL x hx
                have hxS := hsyAll x hx
                simp only [WalLinksState.advance, hSH, hBH, hOrder, ↓reduceIte]
                exact Nat.max_lt.mpr ⟨hxL.1, by omega⟩
              · intro x hx
                have hxL := hBufL x hx
                have hxB := hbAll x hx
                simp only [WalLinksState.advance, hSH, hBH, hOrder, ↓reduceIte]
                exact Nat.max_lt.mpr ⟨hxL.1, by omega⟩
            · refine ⟨?_, ?_, ?_⟩
              · simp only [WalLinksState.advance, hSH, hBH, hOrder, ↓reduceIte]
                exact Nat.max_le.mpr ⟨hLast, by omega⟩
              · intro x hx
                have hxL := hSyncL x hx
                have hxS := hsyAll x hx
                simp only [WalLinksState.advance, hSH, hBH, hOrder, ↓reduceIte]
                exact Nat.max_lt.mpr ⟨hxL.1, by omega⟩
              · intro x hx
                have hxL := hBufL x hx
                have hxB := hbAll x hx
                simp only [WalLinksState.advance, hSH, hBH, hOrder, ↓reduceIte]
                exact Nat.max_lt.mpr ⟨hxL.1, by omega⟩
  refine ⟨hBound.1, ?_, ?_, hSyncHead, hBufHead⟩
  · intro x hx
    exact ⟨hBound.2.1 x hx, (hSyncL x hx).2.1, (hSyncL x hx).2.2⟩
  · intro x hx
    exact ⟨hBound.2.2 x hx, (hBufL x hx).2⟩

theorem inv_wal_1_links_durable_sound (st : WalLinksTraceState) (ev : WalLinksEvent)
    (hSound : LinksSound st) (hValid : WalEventValid st ev) :
    LinksSound (stepWalLinksTrace st ev) := by
  rcases hSound with ⟨hLast, hSyncL, hBufL, hSH, hBH⟩
  cases ev with
  | appended stream lsn prev synced =>
      rcases hValid with ⟨hGt, rfl⟩
      cases stream with
      | sync =>
          cases synced with
          | true =>
              simp only [stepWalLinksTrace, stepWalLinks]
              apply links_sound_advance
              · exact Nat.le_trans hLast (Nat.le_of_lt hGt)
              · intro x hx; cases hx
              · intro x hx
                have hxB := hBufL x hx
                exact ⟨hxB.1, Nat.le_of_lt (Nat.lt_of_le_of_lt hxB.2 hGt)⟩
              · simp
              · simpa using hBH
          | false =>
              simp only [stepWalLinksTrace, stepWalLinks, Bool.false_eq_true, ↓reduceIte]
              apply links_sound_advance
              · exact Nat.le_trans hLast (Nat.le_of_lt hGt)
              · intro x hx
                simp only [List.mem_append, List.mem_singleton] at hx
                rcases hx with hxOld | rfl
                · have ho := hSyncL x hxOld
                  exact ⟨ho.1, Nat.le_of_lt (Nat.lt_of_le_of_lt ho.2.2 hGt),
                    Nat.le_of_lt (Nat.lt_of_le_of_lt ho.2.2 hGt)⟩
                · exact ⟨Nat.lt_of_le_of_lt hLast hGt, Nat.le_refl _, Nat.le_refl _⟩
              · intro x hx
                have hxB := hBufL x hx
                exact ⟨hxB.1, Nat.le_of_lt (Nat.lt_of_le_of_lt hxB.2 hGt)⟩
              · cases hCur : st.links.syncHead with
                | none =>
                    simp only [hCur] at hSH
                    rw [hSH]
                    simp only [List.nil_append]
                    exact ⟨hGt, ⟨lsn, by simp, rfl, Nat.le_refl _⟩, fun x hx => by
                      simp only [List.mem_singleton] at hx; subst hx; exact Nat.le_refl _⟩
                | some h =>
                    simp only [hCur] at hSH
                    rcases hSH with ⟨hhLt, ⟨m, hmMem, rfl, hmLe⟩, hhAll⟩
                    have hmB := hSyncL st.links.lastSyncAppend hmMem
                    refine ⟨hhLt, ⟨lsn, by simp, rfl, by omega⟩, ?_⟩
                    intro x hx
                    simp only [List.mem_append, List.mem_singleton] at hx
                    rcases hx with hxOld | rfl
                    · exact hhAll x hxOld
                    · omega
              · simpa using hBH
      | buffered =>
          cases synced with
          | true =>
              simp only [stepWalLinksTrace, stepWalLinks, ↓reduceIte]
              apply links_sound_advance
              · exact Nat.le_trans hLast (Nat.le_of_lt hGt)
              · intro x hx
                have hs := hSyncL x hx
                exact ⟨hs.1, hs.2.1, Nat.le_of_lt (Nat.lt_of_le_of_lt hs.2.2 hGt)⟩
              · intro x hx; cases hx
              · simpa using hSH
              · simp
          | false =>
              simp only [stepWalLinksTrace, stepWalLinks, Bool.false_eq_true, ↓reduceIte]
              apply links_sound_advance
              · exact Nat.le_trans hLast (Nat.le_of_lt hGt)
              · intro x hx
                have hs := hSyncL x hx
                exact ⟨hs.1, hs.2.1, Nat.le_of_lt (Nat.lt_of_le_of_lt hs.2.2 hGt)⟩
              · intro x hx
                simp only [List.mem_append, List.mem_singleton] at hx
                rcases hx with hxOld | rfl
                · have ho := hBufL x hxOld
                  exact ⟨ho.1, Nat.le_of_lt (Nat.lt_of_le_of_lt ho.2 hGt)⟩
                · exact ⟨Nat.lt_of_le_of_lt hLast hGt, Nat.le_refl _⟩
              · simpa using hSH
              · cases hCur : st.links.bufHead with
                | none =>
                    simp only [hCur] at hBH
                    rw [hBH]
                    simp only [List.nil_append]
                    exact ⟨hGt, ⟨lsn, by simp, Nat.le_refl _⟩, fun x hx => by
                      simp only [List.mem_singleton] at hx; subst hx; exact Nat.le_refl _⟩
                | some h =>
                    simp only [hCur] at hBH
                    rcases hBH with ⟨hhLt, ⟨b0, hb0Mem, hb0Le⟩, hhAll⟩
                    have hb0B := hBufL b0 hb0Mem
                    refine ⟨hhLt, ⟨b0, by simp [hb0Mem], hb0Le⟩, ?_⟩
                    intro x hx
                    simp only [List.mem_append, List.mem_singleton] at hx
                    rcases hx with hxOld | rfl
                    · exact hhAll x hxOld
                    · omega
  | bufSynced =>
      simp only [stepWalLinksTrace, stepWalLinks]
      apply links_sound_advance
      · exact hLast
      · exact hSyncL
      · intro x hx; cases hx
      · simpa using hSH
      · simp
  | syncFlushed through =>
      simp only [stepWalLinksTrace, stepWalLinks]
      apply links_sound_advance
      · exact hLast
      · intro x hx
        simp only [List.mem_filter, decide_eq_true_eq] at hx
        exact hSyncL x hx.1
      · exact hBufL
      · cases hCur : st.links.syncHead with
        | none =>
            simp only [hCur] at hSH
            rw [hSH]
            simp
        | some head =>
            simp only [hCur] at hSH
            rcases hSH with ⟨hhLt, ⟨m, hmMem, rfl, hmLe⟩, hhAll⟩
            by_cases hLeTh : head.lsn ≤ through
            · simp only [hLeTh, ↓reduceIte]
              by_cases hGtTh : st.links.lastSyncAppend > through
              · simp only [hGtTh, ↓reduceIte]
                refine ⟨by omega, ?_, ?_⟩
                · refine ⟨st.links.lastSyncAppend, ?_, rfl, by omega⟩
                  simp only [List.mem_filter, decide_eq_true_eq]
                  exact ⟨hmMem, hGtTh⟩
                · intro x hx
                  simp only [List.mem_filter, decide_eq_true_eq] at hx
                  omega
              · simp only [hGtTh, ↓reduceIte]
                rw [List.filter_eq_nil_iff]
                intro x hx
                have hLeSync := (hSyncL x hx).2.1
                simp only [decide_eq_true_eq]
                omega
            · simp only [hLeTh, ↓reduceIte]
              have hFilterSame : st.unsyncedSync.filter (fun x => decide (x > through)) = st.unsyncedSync := by
                rw [List.filter_eq_self]
                intro x hx
                have := hhAll x hx
                simp only [decide_eq_true_eq]
                omega
              rw [hFilterSame]
              exact ⟨hhLt, ⟨st.links.lastSyncAppend, hmMem, rfl, hmLe⟩, hhAll⟩
      · simpa using hBH
  | syncFlushedAll =>
      simp only [stepWalLinksTrace, stepWalLinks]
      apply links_sound_advance
      · exact hLast
      · intro x hx; cases hx
      · exact hBufL
      · cases hCur : st.links.syncHead with
        | none => simp
        | some head =>
            simp only [hCur] at hSH
            rcases hSH with ⟨_, ⟨m, hmMem, rfl, hmLe⟩, _⟩
            have hNotGt : ¬(st.links.lastSyncAppend > st.links.lastSyncAppend) := Nat.lt_irrefl _
            simp [hmLe, hNotGt]
      · simpa using hBH
  | rotated =>
      simp only [stepWalLinksTrace, stepWalLinks]
      apply links_sound_advance
      · exact hLast
      · intro x hx; cases hx
      · intro x hx; cases hx
      · simp
      · simp

/-! ### Part 2: `walk_chain`, `complete_transactions`, `judge`, and `Fail` Policy (`src/wal/recovery.rs`) -/

inductive CutKind where
  | link
  | blob
  | pastStop
  deriving DecidableEq, Repr, Inhabited

structure Cut where
  lsn : Nat
  kind : CutKind
  deriving DecidableEq, Repr, Inhabited

structure StopFlags where
  unsupportedFormat : Bool
  corruptSector : Bool
  deriving DecidableEq, Repr, Inhabited

inductive WalDamageReason where
  | missingPredecessor
  | unverifiableBlob
  | corruptSector
  | acknowledgedTailLost
  | unsupportedFormat
  deriving DecidableEq, Repr, Inhabited

inductive Framing where
  | none
  | begin
  | cont
  | commit
  deriving DecidableEq, Repr, Inhabited

structure FoundBatch where
  lsn : Nat
  prevLsn : Nat
  inPrefix : Bool
  blobsValid : Bool
  framing : Framing
  hasUserRecords : Bool
  deriving DecidableEq, Repr, Inhabited

structure Walk where
  valid : Nat
  cut : Option Cut
  deriving DecidableEq, Repr, Inhabited

def walkChainLoop (chain : List FoundBatch) (idx last : Nat) (isFirst : Bool) : Walk :=
  match chain with
  | [] => { valid := idx, cut := none }
  | b :: rest =>
      let linked :=
        if isFirst then b.lsn > last && b.prevLsn ≤ last
        else b.lsn > last && b.prevLsn == last
      if !linked then
        let named := if b.prevLsn > last && b.prevLsn < b.lsn then b.prevLsn else b.lsn
        { valid := idx, cut := some { lsn := named, kind := .link } }
      else if !b.inPrefix then
        { valid := idx, cut := some { lsn := b.lsn, kind := .pastStop } }
      else if !b.blobsValid then
        { valid := idx, cut := some { lsn := b.lsn, kind := .blob } }
      else
        walkChainLoop rest (idx + 1) b.lsn false

def walkChain (chain : List FoundBatch) (checkpointLsn : Nat) : Walk :=
  walkChainLoop chain 0 checkpointLsn true

theorem walkChainLoop_prefix_valid (chain : List FoundBatch) (idx last : Nat)
    (isFirst : Bool) :
    let w := walkChainLoop chain idx last isFirst
    idx ≤ w.valid ∧
    w.valid - idx ≤ chain.length ∧
    (∀ k, k < w.valid - idx →
      ∃ b, chain[k]? = some b ∧ b.inPrefix = true ∧ b.blobsValid = true ∧ b.lsn > last) := by
  induction chain generalizing idx last isFirst with
  | nil =>
      simp [walkChainLoop]
  | cons b rest ih =>
      simp only [walkChainLoop]
      cases hlink : (if isFirst then b.lsn > last && b.prevLsn ≤ last else b.lsn > last && b.prevLsn == last) with
      | false =>
          simp
      | true =>
          have hLsnGt : b.lsn > last := by
            cases isFirst <;> simp_all
          by_cases hpref : !b.inPrefix
          · simp [hpref]
          · have hPrefT : b.inPrefix = true := by simpa using hpref
            by_cases hblob : !b.blobsValid
            · simp [hpref, hblob]
            · have hBlobT : b.blobsValid = true := by simpa using hblob
              simp only [Bool.not_true, Bool.false_eq_true, ↓reduceIte, hpref, hblob]
              have ih' := ih (idx + 1) b.lsn false
              simp only [List.length_cons]
              refine ⟨by omega, by omega, ?_⟩
              intro k hk
              cases k with
              | zero =>
                  exact ⟨b, rfl, hPrefT, hBlobT, hLsnGt⟩
              | succ k' =>
                  have hk' : k' < (walkChainLoop rest (idx + 1) b.lsn false).valid - (idx + 1) := by
                    omega
                  rcases ih'.2.2 k' hk' with ⟨b', hGet, hp', hb', hgt'⟩
                  exact ⟨b', hGet, hp', hb', Nat.lt_trans hLsnGt hgt'⟩

theorem inv_wal_1_walk_chain_prefix_sound (chain : List FoundBatch) (checkpointLsn : Nat) :
    let w := walkChain chain checkpointLsn
    w.valid ≤ chain.length ∧
    ∀ k, k < w.valid →
      ∃ b, chain[k]? = some b ∧ b.inPrefix = true ∧ b.blobsValid = true ∧ b.lsn > checkpointLsn := by
  have h := walkChainLoop_prefix_valid chain 0 checkpointLsn true
  simpa [walkChain] using h.2

def completeTxLoop (batches : List FoundBatch) (openGroup : List FoundBatch)
    (acc : List FoundBatch) : List FoundBatch :=
  match batches with
  | [] => acc
  | b :: rest =>
      match b.framing with
      | .begin => completeTxLoop rest [b] acc
      | .cont =>
          if openGroup.isEmpty then completeTxLoop rest openGroup acc
          else completeTxLoop rest (openGroup ++ [b]) acc
      | .commit =>
          if openGroup.isEmpty then completeTxLoop rest openGroup acc
          else completeTxLoop rest [] (acc ++ openGroup ++ [b])
      | .none => completeTxLoop rest [] (acc ++ [b])

def completeTransactions (batches : List FoundBatch) : List FoundBatch :=
  completeTxLoop batches [] []

theorem inv_wal_1_complete_transactions_drops_unclosed_multi_chunk
    (first : FoundBatch) (rest : List FoundBatch) (acc openGrp : List FoundBatch)
    (hFirst : first.framing = .begin)
    (hOnlyCont : ∀ b ∈ rest, b.framing = .cont) :
    completeTxLoop (first :: rest) openGrp acc = acc := by
  simp only [completeTxLoop, hFirst]
  suffices h : ∀ (xs openG : List FoundBatch),
      (∀ b ∈ xs, b.framing = .cont) →
      completeTxLoop xs openG acc = acc from
    h rest [first] hOnlyCont
  intro xs
  induction xs with
  | nil =>
      intro openG _
      rfl
  | cons x xs ih =>
      intro openG hCont
      have hx : x.framing = .cont := hCont x List.mem_cons_self
      have hxs : ∀ b ∈ xs, b.framing = .cont := fun b hb => hCont b (List.mem_cons_of_mem x hb)
      simp only [completeTxLoop, hx]
      split <;> exact ih _ hxs

def judge (cut : Option Cut) (floor lastGood : Nat) (flags : StopFlags) :
    Option (WalDamageReason × Nat) :=
  if floor ≤ lastGood then
    none
  else
    let (lost, base) :=
      match cut with
      | some c =>
          if c.lsn ≤ floor then
            let base :=
              match c.kind with
              | CutKind.link => WalDamageReason.missingPredecessor
              | CutKind.blob => WalDamageReason.unverifiableBlob
              | CutKind.pastStop => WalDamageReason.corruptSector
            (c.lsn, base)
          else if c.kind == CutKind.link then
            (floor, WalDamageReason.missingPredecessor)
          else
            (floor, WalDamageReason.acknowledgedTailLost)
      | none => (floor, WalDamageReason.acknowledgedTailLost)
    let reason :=
      match base with
      | .unverifiableBlob => base
      | _ =>
          if flags.unsupportedFormat then WalDamageReason.unsupportedFormat
          else if flags.corruptSector then WalDamageReason.corruptSector
          else base
    some (reason, lost)

theorem inv_wal_2_judge_iff_floor_gt_last_good (cut : Option Cut) (floor lastGood : Nat)
    (flags : StopFlags) :
    (judge cut floor lastGood flags).isSome = true ↔ floor > lastGood := by
  simp only [judge]
  by_cases h : floor ≤ lastGood
  · simp [h]
  · have hgt : floor > lastGood := Nat.lt_of_not_ge h
    simp [h, hgt]

theorem inv_wal_2_judge_first_lost_eq_min (c : Cut) (floor lastGood : Nat)
    (flags : StopFlags) (reason : WalDamageReason) (lost : Nat)
    (h : judge (some c) floor lastGood flags = some (reason, lost)) :
    lost = Nat.min c.lsn floor := by
  simp only [judge] at h
  by_cases hle : floor ≤ lastGood
  · simp [hle] at h
  · simp only [hle, ↓reduceIte] at h
    by_cases hcl : c.lsn ≤ floor
    · simp only [hcl, ↓reduceIte, Option.some.injEq, Prod.mk.injEq] at h
      rcases h with ⟨_, rfl⟩
      exact (Nat.min_eq_left hcl).symm
    · have hfc : floor ≤ c.lsn := Nat.le_of_not_ge hcl
      simp only [hcl, ↓reduceIte] at h
      by_cases hkind : c.kind == CutKind.link
      · simp only [hkind, ↓reduceIte, Option.some.injEq, Prod.mk.injEq] at h
        rcases h with ⟨_, rfl⟩
        exact (Nat.min_eq_right hfc).symm
      · simp only [hkind, Bool.false_eq_true, ↓reduceIte, Option.some.injEq, Prod.mk.injEq] at h
        rcases h with ⟨_, rfl⟩
        exact (Nat.min_eq_right hfc).symm

theorem inv_wal_2_blob_corruption_priority (k floor lastGood : Nat) (flags : StopFlags)
    (hDamaged : floor > lastGood) (hCutLe : k ≤ floor) :
    judge (some { lsn := k, kind := .blob }) floor lastGood flags =
      some (.unverifiableBlob, k) := by
  have hnotle : ¬ (floor ≤ lastGood) := by omega
  simp [judge, hnotle, hCutLe]

inductive WalDamagePolicy where
  | truncate
  | fail
  deriving DecidableEq, Repr, Inhabited

structure DiskMutationSummary where
  mutatesFiles : Bool
  truncatesWal : Bool
  preallocates : Bool
  createsFiles : Bool
  deriving DecidableEq, Repr, Inhabited

inductive OpenAction where
  | openWithTruncate
  | openClean
  | refuseFail (summary : DiskMutationSummary)
  deriving DecidableEq, Repr, Inhabited

def planRecoveryOpen (policy : WalDamagePolicy) (damage : Option (WalDamageReason × Nat)) :
    OpenAction :=
  match damage with
  | none => .openClean
  | some _ =>
      match policy with
      | .truncate => .openWithTruncate
      | .fail =>
          .refuseFail
            { mutatesFiles := false
              truncatesWal := false
              preallocates := false
              createsFiles := false }

theorem inv_wal_3_fail_policy_non_mutation (damage : Option (WalDamageReason × Nat))
    (hDamaged : damage.isSome = true) :
    planRecoveryOpen .fail damage =
      .refuseFail
        { mutatesFiles := false
          truncatesWal := false
          preallocates := false
          createsFiles := false } := by
  cases damage with
  | none => simp at hDamaged
  | some d => simp [planRecoveryOpen]

/-! ### Part 3: Salvage Catalog Taint Propagation (`src/engine/salvage.rs`) -/

def NameMap := List (Nat × String)

def taintCatalogNames (catName : String) (putTid : Option Nat) (names : List (Nat × String)) :
    List (Nat × String) :=
  names.filter (fun p => p.2 != catName && putTid != some p.1)

inductive SalvageRecord where
  | catalog (name : String) (putTid : Option Nat)
  | primary (tableId : Nat)
  | blobBad
  deriving DecidableEq, Repr, Inhabited

structure EmittedSalvagedOp where
  lsn : Nat
  tableId : Nat
  tableName : String
  deriving DecidableEq, Repr, Inhabited

def processTxRecords (txLsn : Nat) :
    List SalvageRecord → List (Nat × String) → List EmittedSalvagedOp → Bool →
      List (Nat × String) × List EmittedSalvagedOp
  | [], nm, ops, bad => (nm, if bad then [] else ops)
  | rec :: rest, nm, ops, bad =>
      match rec with
      | .catalog name putTid =>
          processTxRecords txLsn rest (taintCatalogNames name putTid nm) [] true
      | .primary tid =>
          match nm.find? (fun p => p.1 == tid) with
          | none => processTxRecords txLsn rest nm [] true
          | some (_, tname) =>
              if bad then
                processTxRecords txLsn rest nm [] true
              else
                processTxRecords txLsn rest nm
                  (ops ++ [{ lsn := txLsn, tableId := tid, tableName := tname }]) false
      | .blobBad =>
          processTxRecords txLsn rest nm [] true

theorem taintCatalogNames_subset (catName : String) (putTid : Option Nat)
    (names : List (Nat × String)) :
    ∀ p ∈ taintCatalogNames catName putTid names, p ∈ names := by
  intro p hp
  simp only [taintCatalogNames, List.mem_filter] at hp
  exact hp.1

theorem inv_wal_4_taint_removes_name_and_tid
    (catName : String) (putTid : Option Nat) (names : List (Nat × String))
    (tid : Nat) (tname : String)
    (hMem : (tid, tname) ∈ taintCatalogNames catName putTid names) :
    tname ≠ catName ∧ putTid ≠ some tid := by
  simp only [taintCatalogNames, List.mem_filter, Bool.and_eq_true, bne_iff_ne, ne_eq] at hMem
  exact ⟨hMem.2.1, fun hEq => hMem.2.2 hEq⟩

theorem findPair_id_eq {tid : Nat} {pair : Nat × String} {xs : List (Nat × String)}
    (h : xs.find? (fun p => p.1 == tid) = some pair) : pair.1 = tid := by
  induction xs with
  | nil => contradiction
  | cons x xs ih =>
      simp only [List.find?] at h
      cases hx : (x.1 == tid) with
      | true =>
          simp only [hx] at h
          cases h
          exact beq_iff_eq.mp hx
      | false =>
          simp only [hx] at h
          exact ih h

theorem processTxRecords_invariant (txLsn : Nat) (recs : List SalvageRecord)
    (nm0 : List (Nat × String)) (ops0 : List EmittedSalvagedOp) (bad0 : Bool)
    (hOps0 : bad0 = true ∨ ∀ op ∈ ops0, (op.tableId, op.tableName) ∈ nm0) :
    let res := processTxRecords txLsn recs nm0 ops0 bad0
    (∀ p ∈ res.1, p ∈ nm0) ∧
    (∀ op ∈ res.2, (op.tableId, op.tableName) ∈ res.1) := by
  induction recs generalizing nm0 ops0 bad0 with
  | nil =>
      simp only [processTxRecords]
      refine ⟨fun p hp => hp, ?_⟩
      cases bad0 with
      | true => simp
      | false =>
          rcases hOps0 with hAbs | hMem
          · contradiction
          · simpa using hMem
  | cons r rest ih =>
      cases r with
      | catalog catName putTid =>
          simp only [processTxRecords]
          have hSub := taintCatalogNames_subset catName putTid nm0
          have ih' := ih (taintCatalogNames catName putTid nm0) [] true (Or.inl rfl)
          exact ⟨fun p hp => hSub p (ih'.1 p hp), ih'.2⟩
      | primary tid =>
          simp only [processTxRecords]
          cases hFind : nm0.find? (fun p => p.1 == tid) with
          | none =>
              exact ih nm0 [] true (Or.inl rfl)
          | some pair =>
              rcases pair with ⟨pid, tname⟩
              have hFoundMem : (pid, tname) ∈ nm0 := List.mem_of_find?_eq_some hFind
              have hIdEq : pid = tid := findPair_id_eq hFind
              subst hIdEq
              cases bad0 with
              | true => exact ih nm0 [] true (Or.inl rfl)
              | false =>
                  rcases hOps0 with hAbs | hMem
                  · contradiction
                  · refine ih nm0 (ops0 ++ [{ lsn := txLsn, tableId := pid, tableName := tname }]) false (Or.inr ?_)
                    intro op hOp
                    simp only [List.mem_append, List.mem_singleton] at hOp
                    rcases hOp with hOld | rfl
                    · exact hMem op hOld
                    · exact hFoundMem
      | blobBad =>
          simp only [processTxRecords]
          exact ih nm0 [] true (Or.inl rfl)

theorem inv_wal_4_salvage_never_emits_tainted_op
    (catName : String) (putTid : Option Nat) (txLsn : Nat)
    (names : List (Nat × String)) (recs : List SalvageRecord) :
    ∀ op ∈ (processTxRecords txLsn recs (taintCatalogNames catName putTid names) [] false).2,
      op.tableName ≠ catName ∧ putTid ≠ some op.tableId := by
  intro op hOp
  have hInv := processTxRecords_invariant txLsn recs (taintCatalogNames catName putTid names)
    [] false (Or.inr (by intro x hx; contradiction))
  have hInOut := hInv.2 op hOp
  have hInTainted := hInv.1 (op.tableId, op.tableName) hInOut
  exact inv_wal_4_taint_removes_name_and_tid catName putTid names op.tableId op.tableName hInTainted

end AydbFormal.WalRecovery
