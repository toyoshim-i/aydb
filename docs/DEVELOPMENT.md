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

