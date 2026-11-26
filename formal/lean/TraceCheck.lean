import AydbFormal
import AydbFormal.Json

set_option autoImplicit false

/-!
  Executable trace-refinement oracle `aydb-trace-check`.

  Verifies Schema v1 NDJSON execution traces exported from `aydb`'s live
  storage engine, SQLite VTab adapter, `WalLinks`, and `judge` tests against
  the mechanized Lean 4 state machines (`TxFsm` and `WalRecovery`).

  Zero `import Lean`, zero `partial`, zero `unsafe`, zero `sorry`.
-/

namespace AydbFormal.TraceCheck

open AydbFormal.Json
open AydbFormal.TxFsm
open AydbFormal.WalRecovery

private def isBlankLine (line : String) : Bool :=
  line.toList.all (fun c => c == ' ' || c == '\t' || c == '\r' || c == '\n')

private def filterNonBlank (raw : List String) : List String :=
  raw.filter (fun s => !isBlankLine s)

private def decodeList {α : Type} (dec : Json → Except String α) : List Json → Except String (List α)
  | [] => .ok []
  | j :: rest => do
      let x ← dec j
      let xs ← decodeList dec rest
      return x :: xs

private def decodeHealth (j : Json) : Except String HealthState := do
  match ← asStr j with
  | "Healthy" => .ok .healthy
  | "Poisoned" => .ok .poisoned
  | other => .error s!"unknown HealthState `{other}`"

private def decodeTxPhase (j : Json) : Except String TxPhase := do
  match ← asStr j with
  | "Active" => .ok .active
  | "Preparing" => .ok .preparing
  | "Prepared" => .ok .prepared
  | "FlushingWal" => .ok .flushingWal
  | "SyncInFlight" => .ok .syncInFlight
  | other => .error s!"unknown TxPhase `{other}`"

private def decodeWriteScope (j : Json) : Except String WriteScope := do
  match ← asStr j with
  | "Normal" => .ok .normal
  | "InternalBypass" => .ok .internalBypass
  | other => .error s!"unknown WriteScope `{other}`"

private def decodeWriteDurability (j : Json) : Except String WriteDurability := do
  match ← asStr j with
  | "BufferedFast" => .ok .bufferedFast
  | "SyncTransactional" => .ok .syncTransactional
  | other => .error s!"unknown WriteDurability `{other}`"

private def decodeFlushMode (j : Json) : Except String FlushCompleteMode := do
  match ← asStr j with
  | "PublishImmediate" => .ok .publishImmediate
  | "AwaitSync" => .ok .awaitSync
  | "HandOffGroup" => .ok .handOffGroup
  | other => .error s!"unknown FlushCompleteMode `{other}`"

private def decodeSavepointFrame (j : Json) : Except String SavepointFrame := do
  let spId ← reqInt j "sp_id"
  let isStatement ← reqBool j "is_statement"
  let recordCount ← reqNat j "record_count"
  let pendingBytes ← reqNat j "pending_bytes"
  let pendingSync ← reqBool j "pending_sync"
  let implicitTx ← reqBool j "implicit_tx"
  return {
    spId := spId
    isStatement := isStatement
    recordCount := recordCount
    pendingBytes := pendingBytes
    pendingSync := pendingSync
    implicitTx := implicitTx
  }

private def decodeTxCoreState (j : Json) : Except String TxCoreState := do
  let tag ← reqStr j "tag"
  match tag with
  | "Idle" => .ok .idle
  | "InTx" =>
      let phase ← reqField j "phase" >>= decodeTxPhase
      let depth ← reqNat j "depth"
      let ownerConn ← reqOptNat j "owner_conn"
      let pendingRecordCount ← reqNat j "pending_record_count"
      let pendingBytes ← reqNat j "pending_bytes"
      let pendingSync ← reqBool j "pending_sync"
      let syncDurabilityUpgrade ← reqBool j "sync_durability_upgrade"
      let vtabs ← reqArr j "vtabs" >>= decodeList asNat
      let syncedVtabs ← reqArr j "synced_vtabs" >>= decodeList asNat
      let savepoints ← reqArr j "savepoints" >>= decodeList decodeSavepointFrame
      let statementSavepointActive ← reqBool j "statement_savepoint_active"
      return .inTx {
        phase := phase
        depth := depth
        ownerConn := ownerConn
        pendingRecordCount := pendingRecordCount
        pendingBytes := pendingBytes
        pendingSync := pendingSync
        syncDurabilityUpgrade := syncDurabilityUpgrade
        vtabs := vtabs
        syncedVtabs := syncedVtabs
        savepoints := savepoints
        statementSavepointActive := statementSavepointActive
      }
  | other => .error s!"unknown TxCoreState tag `{other}`"

private def decodeTxMachine (j : Json) : Except String TxMachine := do
  let health ← reqField j "health" >>= decodeHealth
  let state ← reqField j "state" >>= decodeTxCoreState
  return { health := health, state := state }

private def decodeTxEvent (j : Json) : Except String TxEvent := do
  let tag ← reqStr j "tag"
  match tag with
  | "Begin" => .ok .begin
  | "BeginForConn" =>
      let connId ← reqNat j "conn_id"
      .ok (.beginForConn connId)
  | "BeginVtab" =>
      let connId ← reqNat j "conn_id"
      let vtabId ← reqNat j "vtab_id"
      .ok (.beginVtab connId vtabId)
  | "AttachVtab" =>
      let connId ← reqNat j "conn_id"
      let vtabId ← reqNat j "vtab_id"
      .ok (.attachVtab connId vtabId)
  | "DetachVtab" =>
      let connId ← reqNat j "conn_id"
      let vtabId ← reqNat j "vtab_id"
      .ok (.detachVtab connId vtabId)
  | "VtabSync" =>
      let vtabId ← reqNat j "vtab_id"
      let dur ← reqField j "durability" >>= decodeWriteDurability
      .ok (.vtabSync vtabId dur)
  | "RecordWrite" =>
      let scope ← reqField j "scope" >>= decodeWriteScope
      let dur ← reqField j "durability" >>= decodeWriteDurability
      let rc ← reqNat j "record_count"
      let bb ← reqNat j "batch_bytes"
      let maxB ← reqNat j "max_transaction_bytes"
      .ok (.recordWrite scope dur rc bb maxB)
  | "InvalidatePrepare" => .ok .invalidatePrepare
  | "BeginPrepare" =>
      let dur ← reqField j "durability" >>= decodeWriteDurability
      .ok (.beginPrepare dur)
  | "FinishPrepare" =>
      let okFlag ← reqBool j "ok"
      .ok (.finishPrepare okFlag)
  | "Savepoint" =>
      let ownerConn ← reqOptNat j "owner_conn"
      let spId ← reqInt j "sp_id"
      let isStmt ← reqBool j "is_statement"
      let checkPoison ← reqBool j "check_poison"
      .ok (.savepoint ownerConn spId isStmt checkPoison)
  | "ReleaseSavepoint" =>
      let connId ← reqOptNat j "conn_id"
      let spId ← reqInt j "sp_id"
      .ok (.releaseSavepoint connId spId)
  | "RollbackToSavepoint" =>
      let connId ← reqOptNat j "conn_id"
      let spId ← reqInt j "sp_id"
      .ok (.rollbackToSavepoint connId spId)
  | "PlanCommit" =>
      let connId ← reqOptNat j "conn_id"
      let dsa ← reqBool j "defer_sync_allowed"
      let cg ← reqBool j "can_group"
      let hmc ← reqBool j "has_metadata_changes"
      let blec ← reqBool j "buf_len_exceeds_chunk"
      .ok (.planCommit connId dsa cg hmc blec)
  | "EnterWalFlush" =>
      let dsb ← reqBool j "defer_sync_barrier"
      .ok (.enterWalFlush dsb)
  | "CompleteWalFlush" =>
      let mode ← reqField j "mode" >>= decodeFlushMode
      .ok (.completeWalFlush mode)
  | "FinishSyncCommit" =>
      let okFlag ← reqBool j "ok"
      .ok (.finishSyncCommit okFlag)
  | "Rollback" =>
      let connId ← reqOptNat j "conn_id"
      .ok (.rollback connId)
  | "MarkPoisoned" => .ok .markPoisoned
  | other => .error s!"unknown TxEvent tag `{other}`"

private def decodeTxEffect (j : Json) : Except String TxEffect := do
  let tag ← reqStr j "tag"
  match tag with
  | "InitTxData" => .ok .initTxData
  | "AppendTxBatch" =>
      let rb ← reqBool j "reset_buf"
      let ru ← reqBool j "record_undo"
      .ok (.appendTxBatch rb ru)
  | "VtabSyncOutcome" =>
      let allSynced ← reqBool j "all_synced"
      .ok (.vtabSyncOutcome allSynced)
  | "RunPrepare" =>
      let ed ← reqField j "effective_durability" >>= decodeWriteDurability
      .ok (.runPrepare ed)
  | "CaptureSavepointPayload" =>
      let spId ← reqInt j "sp_id"
      let isStmt ← reqBool j "is_statement"
      let zbo ← reqBool j "zero_buf_offset"
      .ok (.captureSavepointPayload spId isStmt zbo)
  | "TruncateSavepointPayloads" =>
      let newLen ← reqNat j "new_len"
      let cou ← reqBool j "clear_overlay_undo"
      let csbu ← reqBool j "clear_stmt_blob_undo"
      .ok (.truncateSavepointPayloads newLen cou csbu)
  | "CommitImplicitTx" => .ok .commitImplicitTx
  | "BumpUniqueGenIfModified" => .ok .bumpUniqueGenIfModified
  | "RestoreTxToStart" => .ok .restoreTxToStart
  | "RestoreSavepointPayload" =>
      let index ← reqNat j "index"
      let cpb ← reqBool j "clear_pending_buf"
      .ok (.restoreSavepointPayload index cpb)
  | "DecDepthDone" => .ok .decDepthDone
  | "EnsurePreparedBeforeCommit" =>
      let ed ← reqField j "effective_durability" >>= decodeWriteDurability
      .ok (.ensurePreparedBeforeCommit ed)
  | "FallbackFullCommit" => .ok .fallbackFullCommit
  | "DrainPendingGroup" => .ok .drainPendingGroup
  | "ClearEmptyOverlay" => .ok .clearEmptyOverlay
  | "FlushWal" =>
      let dur ← reqField j "durability" >>= decodeWriteDurability
      let rc ← reqNat j "record_count"
      let dsb ← reqBool j "defer_sync_barrier"
      .ok (.flushWal dur rc dsb)
  | "PublishCommittedTx" => .ok .publishCommittedTx
  | "ReturnAwaitingSync" => .ok .returnAwaitingSync
  | "HandOffPendingGroup" => .ok .handOffPendingGroup
  | "PoisonEngineInterruptedSync" => .ok .poisonEngineInterruptedSync
  | "PoisonEngineSyncFailed" => .ok .poisonEngineSyncFailed
  | "RestoreStartMetaAndRelease" => .ok .restoreStartMetaAndRelease
  | other => .error s!"unknown TxEffect tag `{other}`"

private def decodeTxError (j : Json) : Except String TxError := do
  let tag ← reqStr j "tag"
  match tag with
  | "Poisoned" => .ok .poisoned
  | "Busy" => .ok .busy
  | "TxBudgetExceeded" => .ok .txBudgetExceeded
  | "TooManyRecords" => .ok .tooManyRecords
  | "UnknownSavepoint" =>
      let spId ← reqInt j "sp_id"
      .ok (.unknownSavepoint spId)
  | "InvalidPhase" => .ok .invalidPhase
  | other => .error s!"unknown TxError tag `{other}`"

private def decodeTxOutcome (j : Json) : Except String TxOutcome := do
  match ← reqStr j "status" with
  | "Ok" =>
      let effs ← reqArr j "effects" >>= decodeList decodeTxEffect
      .ok (.ok effs)
  | "Err" =>
      let err ← reqField j "error" >>= decodeTxError
      .ok (.err err)
  | other => .error s!"unknown TxOutcome status `{other}`"

private def verifyTxFsmSteps :
    List String → Nat → Option TxMachine → Except String Nat
  | [], count, _ =>
      if count == 0 then
        .error "tx_fsm trace contains zero steps"
      else
        .ok count
  | line :: rest, idx, prevPost => do
      let j ← parseJson line
      let kind ← reqStr j "kind"
      if kind != "step" then
        .error s!"tx_fsm seq {idx}: expected kind=\"step\", got `{kind}`"
      else
        let seq ← reqNat j "seq"
        if seq != idx then
          .error s!"tx_fsm seq mismatch: expected {idx}, got {seq}"
        else
          let pre ← reqField j "pre" >>= decodeTxMachine
          match prevPost with
          | some prev =>
              if pre != prev then
                .error s!"tx_fsm seq {seq}: trace continuity mismatch"
              else pure ()
          | none => pure ()
          if !txWellFormedBool pre then
            .error s!"tx_fsm seq {seq}: pre-state failed txWellFormedBool"
          else pure ()
          let ev ← reqField j "event" >>= decodeTxEvent
          let post ← reqField j "post" >>= decodeTxMachine
          let outcome ← reqField j "outcome" >>= decodeTxOutcome
          let (actualPost, actualOutcome) := AydbFormal.TxFsm.step pre ev
          if !txWellFormedBool actualPost then
            .error s!"tx_fsm seq {seq}: actual post-state failed txWellFormedBool"
          else if actualPost != post then
            .error s!"tx_fsm seq {seq}: post-state mismatch against Lean step: expected {reprStr post}, got {reprStr actualPost}"
          else if actualOutcome != outcome then
            .error s!"tx_fsm seq {seq}: outcome mismatch against Lean step: expected {reprStr outcome}, got {reprStr actualOutcome}"
          else
            verifyTxFsmSteps rest (idx + 1) (some actualPost)

private def decodeLogStream (j : Json) : Except String LogStream := do
  match ← asStr j with
  | "Sync" => .ok .sync
  | "Buffered" => .ok .buffered
  | other => .error s!"unknown LogStream `{other}`"

private def decodeWalLinksEvent (j : Json) : Except String WalLinksEvent := do
  let tag ← reqStr j "tag"
  match tag with
  | "Appended" =>
      let stream ← reqField j "stream" >>= decodeLogStream
      let lsn ← reqNat j "lsn"
      let prev ← reqNat j "prev"
      let synced ← reqBool j "synced"
      .ok (.appended stream lsn prev synced)
  | "BufSynced" => .ok .bufSynced
  | "SyncFlushed" =>
      let through ← reqNat j "through"
      .ok (.syncFlushed through)
  | "SyncFlushedAll" => .ok .syncFlushedAll
  | "Rotated" => .ok .rotated
  | other => .error s!"unknown WalLinksEvent tag `{other}`"

private def decodeStamp (j : Json) : Except String (Nat × Nat) := do
  let xs ← asArr j
  match xs with
  | [a, b] =>
      let lastApp ← asNat a
      let durable ← asNat b
      .ok (lastApp, durable)
  | _ => .error "expected 2-element integer array for stamp"

private def verifyWalLinksLines :
    List String → Nat → Option WalLinksState → Except String Nat
  | [], stepCount, _ =>
      if stepCount == 0 then
        .error "wal_links trace contains zero steps"
      else
        .ok stepCount
  | line :: rest, seqIdx, curOpt => do
      let j ← parseJson line
      match ← reqStr j "kind" with
      | "init" =>
          let base ← reqNat j "base"
          let st := WalLinksState.init base
          verifyWalLinksLines rest seqIdx (some st)
      | "step" =>
          match curOpt with
          | none => .error s!"wal_links step {seqIdx}: missing prior `init` record"
          | some cur =>
              let seq ← reqNat j "seq"
              if seq != seqIdx then
                .error s!"wal_links seq mismatch: expected {seqIdx}, got {seq}"
              else
                let ev ← reqField j "event" >>= decodeWalLinksEvent
                let expectedStamp ← reqField j "stamp" >>= decodeStamp
                let (next, actualStamp) := stepWalLinks cur ev
                if next.durable < cur.durable then
                  .error s!"wal_links seq {seq}: durable regressed from {cur.durable} to {next.durable}"
                else if actualStamp != expectedStamp then
                  .error s!"wal_links seq {seq}: stamp mismatch: expected {reprStr expectedStamp}, got {reprStr actualStamp}"
                else
                  verifyWalLinksLines rest (seqIdx + 1) (some next)
      | other => .error s!"wal_links: unexpected record kind `{other}`"

private def decodeCutKind (j : Json) : Except String CutKind := do
  match ← asStr j with
  | "Link" => .ok .link
  | "Blob" => .ok .blob
  | "PastStop" => .ok .pastStop
  | other => .error s!"unknown CutKind `{other}`"

private def decodeCut (j : Json) : Except String (Option Cut) :=
  match j with
  | .null => .ok none
  | _ => do
      let lsn ← reqNat j "lsn"
      let kind ← reqField j "kind" >>= decodeCutKind
      return some { lsn := lsn, kind := kind }

private def decodeStopFlags (j : Json) : Except String StopFlags := do
  let uf ← reqBool j "unsupported_format"
  let cs ← reqBool j "corrupt_sector"
  return { unsupportedFormat := uf, corruptSector := cs }

private def decodeDamageReason (j : Json) : Except String WalDamageReason := do
  match ← asStr j with
  | "MissingPredecessor" => .ok .missingPredecessor
  | "UnverifiableBlob" => .ok .unverifiableBlob
  | "CorruptSector" => .ok .corruptSector
  | "AcknowledgedTailLost" => .ok .acknowledgedTailLost
  | "UnsupportedFormat" => .ok .unsupportedFormat
  | other => .error s!"unknown WalDamageReason `{other}`"

private def decodeJudgeResult (j : Json) : Except String (Option (WalDamageReason × Nat)) :=
  match j with
  | .null => .ok none
  | _ => do
      let reason ← reqField j "reason" >>= decodeDamageReason
      let lost ← reqNat j "lost"
      return some (reason, lost)

private def verifyWalJudgeCases : List String → Nat → Except String Nat
  | [], count =>
      if count == 0 then
        .error "wal_judge trace contains zero cases"
      else
        .ok count
  | line :: rest, idx => do
      let j ← parseJson line
      let kind ← reqStr j "kind"
      if kind != "case" then
        .error s!"wal_judge seq {idx}: expected kind=\"case\", got `{kind}`"
      else
        let seq ← reqNat j "seq"
        if seq != idx then
          .error s!"wal_judge seq mismatch: expected {idx}, got {seq}"
        else
          let cut ← reqField j "cut" >>= decodeCut
          let floor ← reqNat j "floor"
          let lastGood ← reqNat j "last_good"
          let flags ← reqField j "flags" >>= decodeStopFlags
          let expected ← reqField j "result" >>= decodeJudgeResult
          let actual := AydbFormal.WalRecovery.judge cut floor lastGood flags
          if actual != expected then
            .error s!"wal_judge seq {seq}: mismatch: expected {reprStr expected}, got {reprStr actual}"
          else
            verifyWalJudgeCases rest (idx + 1)

private def verifyTraceContents (contents : String) : Except String (String × Nat) := do
  let lines := filterNonBlank (contents.splitOn "\n")
  match lines with
  | [] => .error "trace file is empty"
  | headerLine :: body =>
      let hdr ← parseJson headerLine
      let kind ← reqStr hdr "kind"
      if kind != "header" then
        .error s!"line 1 must be a header record, got kind=`{kind}`"
      else
        let schema ← reqNat hdr "schema"
        if schema != 1 then
          .error s!"unsupported schema version {schema} (expected 1)"
        else
          let fsm ← reqStr hdr "fsm"
          match fsm with
          | "tx_fsm" =>
              let n ← verifyTxFsmSteps body 0 none
              .ok (fsm, n)
          | "wal_links" =>
              let n ← verifyWalLinksLines body 0 none
              .ok (fsm, n)
          | "wal_judge" =>
              let n ← verifyWalJudgeCases body 0
              .ok (fsm, n)
          | other => .error s!"unsupported fsm `{other}`"

private def verifyFiles : List String → IO UInt32
  | [] => return 0
  | path :: rest => do
      let contents ← IO.FS.readFile ⟨path⟩
      match verifyTraceContents contents with
      | .ok (fsm, count) =>
          IO.println s!"TRACE_OK {path} ({fsm}, {count} entries)"
          verifyFiles rest
      | .error msg =>
          IO.eprintln s!"TRACE_FAIL {path}: {msg}"
          return 1

end AydbFormal.TraceCheck

def main (args : List String) : IO UInt32 := do
  if args.isEmpty then
    IO.eprintln "usage: aydb-trace-check <trace.ndjson> [<trace2.ndjson> ...]"
    return 1
  AydbFormal.TraceCheck.verifyFiles args
