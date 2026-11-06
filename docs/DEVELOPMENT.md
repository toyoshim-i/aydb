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
