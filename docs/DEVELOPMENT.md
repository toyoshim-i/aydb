# Development Guidelines

This document defines the workflow, conventions, and engineering standards for developing `aydb`.

## 1. Language Policy

* **Repository Artifacts (English Only):** All files committed to the repository—including documentation under `docs/`, `README.md`, source code comments, docstrings, error messages, test descriptions, and Git commit messages—must be written in **English**.
* **Interactive Discussions:** Interactive design discussions and pair-programming conversations outside the repository may be conducted in the developer's preferred language.

## 2. Git & Version Control Policy

* **Never Push Automatically:** Do **not** execute `git push` (or any remote-mutating Git command) automatically. Pushing to a remote repository must only be performed by the user or when explicitly instructed by the user for a specific commit/branch.
* **Clean, Atomic Commits:** Keep commits focused on a single logical change (e.g., design documentation update, a specific sub-module implementation, or a self-contained test suite).

## 3. Design-First, Phase-by-Phase Workflow

`aydb` is developed incrementally across three major phases (as outlined in [ARCHITECTURE.md](./ARCHITECTURE.md)). To maintain architectural integrity and ensure every subsystem is thoroughly understood before code is written:

1. **No Premature Implementation:** Do not jump ahead into writing production code for a phase or subsystem before its detailed design has been discussed and agreed upon.
2. **Subsystem Detailed Design First:** Each phase is broken down into concrete subsystems (e.g., in Phase 1: PAL/VFS, `boot_meta.db`, fixed-size WAL, MemTable & immutable `idx`/`blob` segments, crash recovery, and LRU eviction). Before implementing a subsystem, we first clarify and document:
   * Exact binary layouts, alignment invariants, and endianness.
   * Concurrency model, lock granularity, and lifetime/handle ownership.
   * Failure modes (torn writes, partial flushes, `SHARING_VIOLATION`, SMB write-behind errors) and how they are tested.
   * Public and internal Rust API boundaries.
3. **Incremental Verification:** Each subsystem must be accompanied by deterministic unit tests and fault-injection / crash-recovery tests before moving on to the next layer.

## 4. Living Textbook Documentation ("Build-to-Learn" Philosophy)

A core goal of the `aydb` project is to produce a **living, textbook-grade record of how a production-quality embedded database is designed and built from the ground up**. Design documents under `docs/` should read like chapters of a systems engineering textbook (in the spirit of *Database Internals* or *Designing Data-Intensive Applications*) rather than terse API notes:

* **Explain the "Why" and Fundamental Trade-Offs:** Do not merely state the chosen data structure or protocol. Contrast it with classic alternatives (e.g., B-Tree vs. LSM-Tree vs. WiscKey, user-space Buffer Pool vs. OS Page Cache delegation, double `fsync` vs. `Write-Blob-Before-WAL` with CRC verification) and explain the hardware and OS mechanics driving the decision.
* **Demystify OS & Filesystem Internals:** Explicitly document how the storage engine interacts with kernel subsystems—such as Windows Cache Manager 256 KB Virtual Address Control Blocks (VACBs), SMB Read-Write-Handle (RWH) leases, NTFS MFT metadata locking, antivirus minifilter behavior, and POSIX `pread`/`pwrite`/`fcntl` semantics.
* **Diagramming Standards:**
  * **Mermaid Diagrams:** Use for architectural overviews, read/write control flows, sequence diagrams, state machines, and module dependency graphs so they remain easy to diff and evolve inline.
  * **RFC-Style ASCII Box Diagrams:** Use for exact byte/bit offsets, sector headers, and fixed-size binary slot layouts.
  * **SVG Figures (`docs/assets/*.svg`):** Use when illustrating complex multi-layer memory/disk cross-sections or pointer topologies that require pixel-precise spatial alignment.


## 5. Crash-Consistency Tests

`src/engine/crash_tests.rs` drives the engine with a seeded random workload on a simulated filesystem (`CrashFs`), crashes it at a seed-chosen point, reopens it and compares the result with an oracle. Every run is determined by its seed.

**What `CrashFs` models**

* Each file has a page-cache image (what reads see) and a durable image (as of the last successful sync). A power loss lets each unsynced write persist completely, be lost, or persist only in the 512-byte sectors the seed picks. Later writes may persist while earlier ones are lost (reordering). A write that extends a file may extend its length without its data, which then reads as zeros.
* A process crash (`kill -9`) keeps the whole page cache and directory.
* `preallocate` with `PhysicalZeroFill` ends with a sync, as the real implementations do. `LogicalExtendOnly` is not durable until a sync.
* Directory entries follow the platform. Unix: `open_file` (create) and `remove_file` flush the directory, so entries are durable on return. Windows (assumption): the directory log is flushed by any successful `FlushFileBuffers`, and operations after the last flush survive as a random prefix, so an unflushed creation may be missing and an unflushed removal may be undone. Under both modes a data write never reaches the device before the directory operations issued before it. The mode is chosen per seed.
* Fault injection: a power cut at a global operation index (that operation and all later ones fail), or a one-shot failure of the n-th write, sync, pre-allocation, creation or removal on a chosen file kind (`boot_meta`, `wal_sync`, `wal_buf`, `idx`, blob, manifest). A failed sync may leave the writes pending, may have flushed them, or may drop them while later syncs succeed ("fsyncgate").

Not modelled: torn writes below 512 bytes, reordering within one write other than by sector, corruption of synced data, rename, multi-process access, and the real timing of group commit (the engine API is driven single-threaded).

**What the oracle checks** (ARCHITECTURE.md section 4)

The workload issues puts, overwrites, deletes, explicit transactions with savepoints, release and rollback, mixed durabilities, inline and out-of-line values, two capped tables, flushes, checkpoints, compaction, catalogs large enough to need a manifest (with table drops that shrink them), and small logs so that rotation collides with commits. The model keeps the state after every commit that was issued. After a crash the recovered state must equal the model state after `k` commits for some `k` between the last guaranteed commit and the newest issued one. This is the prefix property and atomicity at once. The guaranteed commit is:

* after a power loss: the last acknowledged `SyncTransactional` commit (it makes every earlier buffered commit durable) or successful `checkpoint`;
* after a process crash: the last acknowledged commit of either kind, and the state returned by an earlier recovery.

Interpretations: an operation that logged nothing (a delete of a missing row, a transaction without records) is not a commit and acknowledges nothing; a commit that returned an error while the engine is poisoned, or was interrupted by the power cut, may or may not be present; a failed operation on an engine that is not poisoned must have no effect and the workload continues; the capped tables evict on their own, so a row may be absent, but a present row must carry the value the model has for `k`. Reads must never fail. Reopen must succeed after any crash, including a second crash during or right after recovery, and must leave an engine whose `checkpoint` and `compact` work. After a checkpoint the state must survive another crash unchanged.

**Running**

The regular test runs 1,000 seeds (about 10 seconds in a debug build, on four threads). Environment variables:

* `AYDB_CRASH_SEEDS=<n>`: number of seeds (for example 2000 or more for a long run).
* `AYDB_CRASH_SEED_START=<n>`: first seed.
* `AYDB_CRASH_SEED=<n>`: replay one seed with the complete operation and filesystem trace in the failure report.
* `AYDB_CRASH_KNOWN_BUGS=1`: stop working around the open engine bugs listed below (B and C).

Each seed runs twice: once clean, which also measures the operation counts, and once with crash points and failures drawn from those counts.

**Open engine bugs found by the harness.** The workload avoids them by default, and each has an ignored reproduction (`cargo test --lib known_bug -- --ignored`): (B) reclaiming an LRU blob segment makes recovery drop whole log batches that reference it, and in the buffered stream every later batch; (C) reclaiming an LRU blob segment syncs `wal_buf.db` without syncing the standard blob writer, so a later synchronous commit skips that sync. B and C are why capped tables hold inline values only in the default workload.

A third bug found by the harness is fixed: recovery used to adopt a blob segment that was queued in `pending_delete_files` when a stale pointer in an older `idx` segment referenced it, and then unlinked it in the same commit, so the next open failed with `NotFound`. Recovery now never adopts a file that is queued for deletion (regression test `pending_delete_of_dead_blob_segment_is_not_adopted`).
