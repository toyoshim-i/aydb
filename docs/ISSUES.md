# Technical Audit & Known Issues (`aydb`)

This document records the findings from a two-pass deep architectural, implementation, and benchmark audit of `aydb` across Phase 1 (Core Storage Engine) and Phase 2 (SQLite Virtual Table & Benchmarks).

Issues are organized into five categories:
1. **WAL Durability & Crash Recovery Defects** (`ISSUE-01` through `ISSUE-04`)
2. **Transaction Atomicity (`BEGIN`/`COMMIT`/`ROLLBACK`) & Quota Accounting Bugs** (`ISSUE-05` through `ISSUE-10`)
3. **Range Scan, LRU Eviction, & Virtual Table Query Correctness Bugs** (`ISSUE-11` through `ISSUE-14`)
4. **Memory Ceiling Violations & Storage Bloat** (`ISSUE-15` through `ISSUE-18`)
5. **Specification Divergences & Benchmark Defects** (`SPEC-01` through `SPEC-05`, `BENCH-01` through `BENCH-08`)

---

## Summary Matrix

| ID | Category | Severity | Component | Title |
| :--- | :--- | :--- | :--- | :--- |
| **ISSUE-01** | Durability | **Critical** | `wal` / `pal` / `blob` | Synchronous WAL (`SyncTransactional`) fails to sync on Unix, never calls `sync_barrier()`, and leaves out-of-line blobs (`>= 1 KB`) unsynced in OS cache |
| **ISSUE-02** | Crash Recovery | **Critical** | `meta` / `engine` | `BootMetaManager::open_or_recover` deletes uncataloged `blob_std_*` and `blob_lru_*` files on startup before `StorageEngine::open` can recover them |
| **ISSUE-03** | Durability | **High** | `wal` / `engine` | `WalStreamMode::BufferedUserSpaceCoalesced` silently loses un-flushed tail sectors (`< 4 KB`) on clean `Drop` |
| **ISSUE-04** | Crash Recovery | **High** | `wal` | Partial `WalBatch` record filtering in `recover_two_streams` corrupts secondary index, LRU queue, and quota invariants when a blob is unverified |
| **ISSUE-05** | Transactions | **Critical** | `engine` | Transactions exceeding 4 MB auto-flush uncommitted records to WAL (`flush_pending_tx_records`), making `ROLLBACK` impossible |
| **ISSUE-06** | Transactions | **Critical** | `engine` | Mid-transaction `MemTable` flush or WAL ring rotation permanently commits uncommitted data to `idx_<SeqID>.db` and `boot_meta.db` |
| **ISSUE-07** | Crash Consistency | **High** | `memtable` / `engine` | `flush_memtable()` clears `MemTable` (`self.map.clear()`) *before* committing `boot_meta.db` |
| **ISSUE-08** | Quota Accounting | **High** | `engine` / `lru` | Capped Table `current_logical_bytes` is reset to `0` or undercounted on DB reopen and `ROLLBACK`, allowing `MAX_BYTES` quota breaches |
| **ISSUE-09** | Durability / GC | **High** | `engine` | Premature physical deletion (GC) of sealed blob segments before `MemTable` flush or transaction commit, plus stale `file_len` on recovery |
| **ISSUE-10** | Storage GC | **High** | `engine` / `index` | Compaction discards `outcome.newly_dead_blob_bytes` and WAL replay loses unpersisted `dead_bytes` increments |
| **ISSUE-11** | Correctness | **High** | `index` / `engine` | Multi-segment prefix/range scans with `limit` drop live rows and stall LRU eviction when shadowed by tombstones |
| **ISSUE-12** | SQL Correctness | **High** | `vtab` | `xBestIndex` pushes down `LIMIT` on PK range scans even when un-omitted `WHERE` predicates remain on other columns |
| **ISSUE-13** | SQL Correctness | **Medium** | `vtab` / `row_codec` | 32-bit entropy in 64-bit `pk_to_rowid` hash, 8-byte `TEXT`/`BLOB`/`FLOAT` PK misidentification, untagged `NULL`/`""` index collisions, and unbounded `rowid_to_pk` growth |
| **ISSUE-14** | SQL Correctness | **Medium** | `vtab::row_codec` | Non-memcomparable IEEE-754 `f64` (`REAL`) key encoding inverts negative sort order |
| **ISSUE-15** | Storage Bloat | **High** | `engine` / `blob` | Standard table blob segments (`blob_std_<SeqID>.db`) have no partial GC or live-blob relocation |
| **ISSUE-16** | Memory Budget | **High** | `index` | `compact_idx_segments` materializes all entries across all SSTables into a single in-memory `BTreeMap` |
| **ISSUE-17** | Memory Budget | **Medium** | `index` | `IdxSegmentBuilder` clones every key byte vector into `keys_for_bloom: Vec<Vec<u8>>` |
| **ISSUE-18** | Performance | **Medium** | `engine` / `meta` | Redundant `boot_meta.db` `FlushFileBuffers` (`fsync`) during sequence ID allocation in flush, compaction, and blob rollover |
| **SPEC-01..05** | Spec Divergence | Low–Med | `index` / `pal` / `util` | Unimplemented SSTable prefix compression, 23B vs. 20B `BlobPtr`, Unix PAL stub, and x86-only hardware CRC32C |
| **BENCH-01..08** | Benchmarks | Low–High | `examples/` | 100% `MemTable` residency, `INSERT OR REPLACE` bypassing `UPDATE` in TPC-B/YCSB, halved Speedtest1 counts, and asymmetric disk metrics |

---

## 1. WAL Durability & Crash Recovery Defects

### `ISSUE-01` [Critical]: Synchronous WAL (`SyncTransactional`) fails to sync on Unix, never calls `sync_barrier()`, and leaves out-of-line blobs (`>= 1 KB`) unsynced in OS cache

* **Locations**:
  * `src/wal/mod.rs` (`WalStreamWriter::append_raw_batch`, lines 460–476, 532–536)
  * `src/pal/unix.rs` (`UnixPlatformFile::open_internal`, lines 16–35; `sync_barrier`, lines 74–76)
  * `src/pal/windows.rs` (`WinPlatformFile::sync_barrier`, lines 484–491)
  * `src/engine/mod.rs` (`StorageEngine::put_row_owned`, lines 412–423; `append_to_blob_segment`, lines 1237–1325)
* **Root Causes & Destruction Paths**:
  1. **`PlatformFile::sync_barrier()` is Dead Code (Zero Call Sites)**:
     Although `PlatformFile` defines `sync_barrier()` specifically to enforce durability after WAL writes, `WalStreamWriter::append_raw_batch` only calls `self.file.write_all_at(...)` and **never invokes `self.file.sync_barrier()` or `self.file.sync_durable()`**.
  2. **Complete Lack of Sync Durability on Unix/macOS**:
     `UnixPlatformFile::open_internal` (`src/pal/unix.rs:28-34`) marks `hint: OpenHint` with `#[allow(dead_code)]` and opens `wal_sync.db` with plain `OpenOptions::new().read(true).write(writable).create(create)` (no `O_DSYNC` or `O_SYNC`). Combined with `WalStreamWriter` never calling `sync_barrier()`, `WriteDurability::SyncTransactional` on Unix performs **zero `fdatasync`/`fsync` calls** and leaves all synchronous commits in the volatile OS page cache.
  3. **Out-of-Line Blobs (`>= 1 KB`) Are Not Synced During `SyncTransactional` Commits (Windows & Unix)**:
     In `StorageEngine::put_row_owned` (`src/engine/mod.rs:412-423`), when `value.len() >= inline_threshold_bytes` (default `1,024 B`) and `durability == WriteDurability::SyncTransactional`, the payload is written to `blob_std_<SeqID>.db` or `blob_lru_<TblID>_<SeqID>.db`. Those blob files are opened with `OpenHint::ImmutableSequential` (`FILE_FLAG_SEQUENTIAL_SCAN`, **not** `FILE_FLAG_WRITE_THROUGH`), and `append_to_blob_segment` **never calls `writer.sync_durable()`**.
     If power fails after `put_row(..., WriteDurability::SyncTransactional)` returns:
     * `wal_sync.db` retains the `PutBlobPtr` record on disk (via `FILE_FLAG_WRITE_THROUGH` on Windows),
     * The blob payload in `blob_std_<SeqID>.db` was still in the volatile OS page cache and is lost/truncated on disk,
     * On reboot, `recover_two_streams` calls `validate_blob_ptr(ptr)`, sees the missing/truncated blob, and **silently discards the committed `SyncTransactional` record**.
* **Recommended Fix**:
  * Call `self.file.sync_barrier()?` at the end of `WalStreamWriter::append_raw_batch` when `self.mode == WalStreamMode::SyncSectorIsolated` (and pass `O_DSYNC` on Unix when `hint == OpenHint::MutableSyncLog`).
  * In `put_row_owned`, when `durability == WriteDurability::SyncTransactional` and a blob is written out-of-line (or on `commit_transaction` when `pending_tx_sync` is true), call `sync_durable()` on the active blob writer **before** writing the `WalBatch` to `wal_sync.db`.

---

### `ISSUE-02` [Critical]: `BootMetaManager::open_or_recover` deletes uncataloged `blob_std_*` and `blob_lru_*` files on startup before `StorageEngine::open` can recover them

* **Locations**:
  * `src/engine/mod.rs` (`StorageEngine::open`, line 141 vs. lines 184–234; `append_to_blob_segment`, lines 1262–1276, 1300–1315)
  * `src/meta/mod.rs` (`is_managed_segment_or_manifest`, lines 62–69; `BootMetaManager::open_or_recover`, line 527; `sweep_orphans_and_pending_deletes`, lines 815–833)
* **Root Cause & Destruction Path**:
  1. When `append_to_blob_segment` rolls over or creates a new `blob_std_<SeqID>.db` or `blob_lru_<TblID>_<SeqID>.db` file, it calls `self.meta_mgr.commit_update(|st| { seq_id = st.allocate_seq_id(); })`, which only increments `st.next_seq_id` in `boot_meta.db`. It **does not** add the new blob segment to `st.std_blob_segments` or `st.lru_blob_segments` (those lists are only updated in `boot_meta.db` when `flush_memtable()` or `compact()` runs).
  2. When the database is reopened after a crash (or after a clean shutdown that did not call `flush_memtable()`), `StorageEngine::open` first executes line 141:
     ```rust
     let mut meta_mgr = BootMetaManager::open_or_recover(fs.clone())?;
     ```
  3. Inside `BootMetaManager::open_or_recover` (`src/meta/mod.rs:527`), `mgr.sweep_orphans_and_pending_deletes()` enumerates all files on disk matching `is_managed_segment_or_manifest` (`idx_*`, `blob_std_*`, `blob_lru_*`, `manifest_*`) that are not in `boot_meta.db`'s `referenced_file_names()`, queues them for deletion, and **immediately deletes them from disk (`try_process_pending_deletes()`)**!
  4. Forty-three lines later (`src/engine/mod.rs:184-234`), `StorageEngine::open` runs a loop intended to discover uncataloged `blob_std_*` and `blob_lru_*` files created during the active WAL epoch—**but `BootMetaManager::open_or_recover` has already deleted every single one of those files at line 141!**
  5. `recover_two_streams` then fails `validate_blob_ptr` for every blob written since the last `flush_memtable()` and silently drops all corresponding rows.
* **Recommended Fix**:
  Do not treat `blob_std_*` and `blob_lru_*` files as unreferenced orphans inside `BootMetaManager::open_or_recover` before WAL recovery has run; instead, run the orphan sweep *after* `recover_two_streams` has validated and registered all blob segments referenced by live WAL batches, or register new blob segment metadata in `boot_meta.db` at creation time inside `append_to_blob_segment`.

---

### `ISSUE-03` [High]: `WalStreamMode::BufferedUserSpaceCoalesced` silently loses un-flushed tail sectors (`< 4 KB`) on clean `Drop`

* **Locations**:
  * `src/wal/mod.rs` (`WalStreamWriter`, lines 300–315, 507–528, 545–551)
  * `src/engine/mod.rs` (`StorageEngine`, lines 109–136)
* **Root Cause**:
  In `WalStreamMode::BufferedUserSpaceCoalesced`, when an appended batch does not fill the active 4 KB sector, `append_raw_batch` sets `self.dirty_in_user_space = true` (line 526) and leaves the bytes solely in `self.sector_buf` in user-space RAM.
  Neither `WalStreamWriter` nor `StorageEngine` implements `Drop` to call `flush_buffered_to_os()` (and `flush_pending_tx_records()`). When `StorageEngine` is dropped on normal shutdown without an explicit `flush_memtable()` call (as happens in `examples/standard_sql_bench.rs:308`), up to 4,077 bytes of committed transactions in the tail sector are never written to the OS file handle and are permanently lost.
* **Recommended Fix**:
  Implement `Drop` for `WalStreamWriter` (calling `let _ = self.flush_buffered_to_os();`) and `Drop` for `StorageEngine` (flushing any buffered WAL data and syncing active blob writers).

---

### `ISSUE-04` [High]: Partial `WalBatch` record filtering in `recover_two_streams` corrupts secondary index, LRU queue, and quota invariants when a blob is unverified

* **Location**:
  * `src/wal/mod.rs` (`recover_two_streams`, lines 831–842)
* **Root Cause**:
  A single logical `put_row` call emits an atomic `WalBatch` containing multiple coupled `WalRecord` items:
  1. `WalRecord::Delete` for old secondary index (`'i'`) and old LRU queue (`'l'`) entries,
  2. `WalRecord::PutBlobPtr` for the primary row (`'r'`),
  3. `WalRecord::PutInline` for new secondary index (`'i'`) entries,
  4. `WalRecord::PutInline` for the new LRU queue (`'l'`) entry, and
  5. `WalRecord::PutInline` for the row metadata (`'t'`, `RowMetaRecord` carrying `logical_size`).
  During crash recovery, `recover_two_streams` executes:
  ```rust
  batch.records.retain(|rec| match rec {
      WalRecord::PutBlobPtr { blob_ptr, .. } => validate_blob(blob_ptr),
      _ => true,
  });
  ```
  If `validate_blob(blob_ptr)` returns `false` (because power failed before the out-of-line blob reached disk), `retain` drops *only* the `PutBlobPtr` record and applies all the companion `Delete` and `PutInline` (`'i'`, `'l'`, `'t'`) records! This leaves dangling secondary index entries pointing to a missing/old primary row, orphan LRU queue entries, and phantom `logical_size` bytes in `'t'` metadata.
* **Recommended Fix**:
  Treat each `WalBatch` as an atomic unit during `recover_two_streams`: if any `PutBlobPtr` record in a `WalBatch` fails `validate_blob`, discard the entire `WalBatch` (or group per-row records atomically) rather than partially mutating the batch.

---

## 2. Transaction Atomicity (`BEGIN`/`COMMIT`/`ROLLBACK`) & Quota Accounting Bugs

### `ISSUE-05` [Critical]: Transactions exceeding 4 MB auto-flush uncommitted records to WAL (`flush_pending_tx_records`), making `ROLLBACK` impossible

* **Locations**:
  * `src/engine/mod.rs` (`StorageEngine::commit_wal_and_memtable`, lines 1413–1428; `flush_pending_tx_records`, lines 935–956; `rollback_transaction`, lines 898–933)
* **Root Cause**:
  When `self.tx_depth > 0` (inside an explicit `BEGIN ...` transaction), `commit_wal_and_memtable` buffers encoded records in `self.pending_tx_buf` and applies them to `self.memtable`. However, lines 1425–1427 check:
  ```rust
  if self.pending_tx_buf.len() >= 4 * 1024 * 1024 {
      self.flush_pending_tx_records()?;
  }
  ```
  Once `pending_tx_buf` reaches 4 MB, `flush_pending_tx_records()` assigns an LSN and writes the uncommitted records into `wal_sync.db` or `wal_buf.db` **before `COMMIT`**.
  If the application subsequently executes `ROLLBACK` (`rollback_transaction()`), `rollback_transaction()` clears `pending_tx_buf` and reconstructs `self.memtable` by calling `recover_two_streams` on `wal_sync.db` and `wal_buf.db`—which **replays the already-flushed 4 MB+ of uncommitted WAL batches back into `self.memtable`**! Similarly, a crash mid-transaction recovers and commits the partial transaction.
* **Recommended Fix**:
  Never flush uncommitted `pending_tx_buf` records into the committed WAL streams prior to `commit_transaction()`, or introduce explicit transaction begin/commit markers in the WAL so recovery and `rollback_transaction()` ignore uncommitted `WalBatch`es.

---

### `ISSUE-06` [Critical]: Mid-transaction `MemTable` flush or WAL ring rotation permanently commits uncommitted data to `idx_<SeqID>.db` and `boot_meta.db`

* **Locations**:
  * `src/engine/mod.rs` (`commit_wal_and_memtable`, line 1426 -> `flush_pending_tx_records`, line 952 -> `append_raw_wal_batch_with_rotation`, line 1452 -> `flush_memtable`, lines 960–1018)
* **Root Causes & Destruction Paths**:
  1. **SSTable Flush Mid-Transaction**:
     When a transaction's WAL write hits `WalAppendOutcome::RingFull` (for example, `wal_sync.db` is only 2 MB by default, so any 4 MB spill from `pending_tx_buf` in `SyncTransactional` mode immediately triggers `RingFull` in `append_raw_wal_batch_with_rotation` at line 1451, or if `flush_memtable()` is called while `tx_depth > 0`), `self.flush_memtable()` is executed mid-transaction.
     `flush_memtable()` writes the entire `self.memtable` (including uncommitted transaction rows) into a new immutable `idx_<SeqID>.db` SSTable, updates `checkpoint_lsn`, rotates both WAL epochs, and commits `boot_meta.db`. Once written to `idx_<SeqID>.db` and `boot_meta.db`, `rollback_transaction()` (which only rebuilds `self.memtable` from WAL batches after `checkpoint_lsn`) **cannot undo the SSTable flush**, permanently committing the aborted transaction.
  2. **Un- reverted Side Effects on `ROLLBACK` Even for Small (`< 4 MB`) Transactions**:
     Even when a transaction is small and stays in `pending_tx_buf`:
     * `put_row_owned` and `delete_row` immediately mutate `self.std_blobs` and `self.lru_blobs` (`dead_bytes` and `file_len`),
     * `put_row_owned` immediately executes `self.evict_until_within_quota()`, mutating `ct.total_evictions`, `ct.total_lazy_promotions`, `ct.eviction_cursor_tick`, and `self.access_tracker`,
     * Out-of-line blobs written during the aborted transaction remain appended in the active blob segment (`file_len` increased) without their bytes being added to `dead_bytes` on `ROLLBACK`, permanently preventing that blob segment from ever reaching `dead_bytes >= file_len`.

---

### `ISSUE-07` [High]: `flush_memtable()` clears `MemTable` (`self.map.clear()`) *before* committing `boot_meta.db`

* **Locations**:
  * `src/memtable/mod.rs` (`MemTable::flush_to_segment`, lines 191–209)
  * `src/engine/mod.rs` (`StorageEngine::flush_memtable`, lines 986–1017)
* **Root Cause**:
  In `StorageEngine::flush_memtable`, line 987 calls `self.memtable.flush_to_segment(...)`, which writes `idx_<SeqID>.db` and immediately executes:
  ```rust
  self.map.clear();
  self.approximate_bytes = 0;
  ```
  at lines 206–207 of `src/memtable/mod.rs`—**before** `StorageEngine::flush_memtable` commits the new segment, `checkpoint_lsn`, and new WAL epochs to `boot_meta.db` at lines 1005–1013!
  If `self.meta_mgr.commit_update(...)` fails (e.g., I/O error or disk full when writing an overflow `manifest_<SeqID>.db` or `boot_meta.db`), `flush_memtable` returns `Err`, but `self.memtable` has already been cleared, `self.idx_segments.push(new_reader)` has already executed, and `sync_wal`/`buf_wal` have *not* been rotated to the new epoch, leaving engine state corrupted.
* **Recommended Fix**:
  Do not clear `self.memtable` inside `flush_to_segment`; clear `self.memtable`, push `new_reader` to `self.idx_segments`, and rotate the WAL streams only **after** `self.meta_mgr.commit_update(...)` succeeds.

---

### `ISSUE-08` [High]: Capped Table `current_logical_bytes` is reset to `0` or undercounted on DB reopen and `ROLLBACK`, allowing `MAX_BYTES` quota breaches

* **Locations**:
  * `src/engine/mod.rs` (`StorageEngine::open`, lines 291–323; `register_capped_table`, lines 328–336; `put_row_owned`, line 451; `rollback_transaction`, lines 898–933; `reconcile_capped_tables_after_recovery`, lines 1562–1581)
* **Root Causes & Destruction Paths**:
  1. **`register_capped_table` Never Persists `table_quotas` to `boot_meta.db` (`current_logical_bytes` Reset to `0` on Reopen)**:
     `register_capped_table(table_id, max_bytes)` (`src/engine/mod.rs:328-336`) only inserts into the in-memory `self.capped_tables` map. `table_quotas` is only written to `boot_meta.db` when `flush_memtable()` or `compact()` runs.
     If a database registers a Capped Table (e.g., via SQLite `CREATE VIRTUAL TABLE ... USING aydb(max_bytes=...)`), inserts data into the WAL/`MemTable`, and closes or crashes before `flush_memtable()`:
     * On reopen, `StorageEngine::open` reads `state_snapshot.table_quotas` from `boot_meta.db`, which is **empty (`[]`)**.
     * `engine.reconcile_capped_tables_after_recovery()` at line 322 iterates over `self.capped_tables.keys()` (which is empty!) and does nothing.
     * When the caller or Virtual Table subsequently calls `register_capped_table(table_id, max_bytes)`, `or_insert_with(|| CappedTableState::new(table_id, max_bytes))` initializes **`current_logical_bytes = 0`** without calling `reconcile_capped_tables_after_recovery()`! All previously recovered rows are now invisible to quota accounting, allowing the table to exceed `max_bytes` by up to an entire pre-recovery quota!
  2. **Tombstone Truncation in `reconcile_capped_tables_after_recovery` / Missing `'t'` Records**:
     `reconcile_capped_tables_after_recovery()` computes `total_bytes` by scanning `[TableID | 't' | pk]` (`NS_ROW_META`) entries. If rows were inserted into `table_id` before `register_capped_table` was called (and without secondary indexes), `put_row_owned` (line 451: `if is_capped || !secondary_keys.is_empty()`) did not emit `'t'` records for those rows, so their logical size is lost on recovery/`ROLLBACK`.
* **Recommended Fix**:
  * Persist `table_quotas` in `boot_meta.db` when `register_capped_table` is called, and call `reconcile_capped_tables_after_recovery` whenever a table is newly registered in `self.capped_tables` on a non-empty engine.

---

### `ISSUE-09` [High]: Premature physical deletion (GC) of sealed blob segments before `MemTable` flush or transaction commit, plus stale `file_len` on recovery

* **Locations**:
  * `src/engine/mod.rs` (`StorageEngine::open`, lines 152–180; `put_row_owned`, lines 390–392, 469; `delete_row`, lines 518–520, 548; `reclaim_fully_dead_blob_segments`, lines 1344–1386)
* **Root Causes & Destruction Paths**:
  1. **Immediate Unlinking Before Index State Is Durable or Committed**:
     `put_row_owned` and `delete_row` immediately increment in-memory `dead_bytes` and call `reclaim_fully_dead_blob_segments()`, which physically unlinks (`fs.remove_file`) any sealed `blob_std` or `blob_lru` segment where `dead_bytes >= file_len`. If the overwriting/deleting mutations are inside an uncommitted transaction that later executes `ROLLBACK`, or are only in `MemTable`/`wal_buf.db` when power fails before `flush_memtable()`, the restored index state points to a blob file that was already deleted from disk.
  2. **Stale `bmeta.file_len` on Startup Triggers Premature Deletion of Live Recovered Blobs**:
     In `StorageEngine::open` (`src/engine/mod.rs:152-180`), cataloged blob segments initialize `TrackedStdBlob::file_len` and `TrackedLruBlob::file_len` from `bmeta.file_len` (the length recorded at the last `boot_meta.db` checkpoint) rather than the actual physical file length `file.file_len()`. If additional blobs were appended to that segment after the checkpoint and recovered via WAL replay, `b.file_len` in memory is smaller than the actual bytes in the file. As soon as `dead_bytes` reaches the old `bmeta.file_len`, `b.dead_bytes >= b.file_len` evaluates to `true` and `reclaim_fully_dead_blob_segments()` deletes the file while the post-checkpoint recovered blobs in that segment are still live!

---

### `ISSUE-10` [High]: Compaction discards `outcome.newly_dead_blob_bytes` and WAL replay loses unpersisted `dead_bytes` increments

* **Locations**:
  * `src/index/mod.rs` (`compact_idx_segments`, lines 801–877)
  * `src/engine/mod.rs` (`StorageEngine::open`, lines 252–276; `StorageEngine::compact`, lines 1023–1081)
* **Root Cause**:
  `compact_idx_segments` computes `CompactionResult::newly_dead_blob_bytes` whenever older `BlobPtr` entries are shadowed or tombstoned during SSTable merging, but `StorageEngine::compact()` ignores `outcome.newly_dead_blob_bytes`. Meanwhile, `put_row` and `delete_row` only increment `dead_bytes` in memory (not in the WAL), and WAL replay in `StorageEngine::open` applies batches via `memtable.apply_wal_batch(batch)` without looking up overwritten keys or calling `record_dead_blob_bytes`. Any crash before `flush_memtable()` permanently leaks the `dead_bytes` increments of all blobs overwritten or deleted during that epoch.

---

## 3. Range Scan, LRU Eviction, & Virtual Table Query Correctness Bugs

### `ISSUE-11` [High]: Multi-segment prefix/range scans with `limit` drop live rows and stall LRU eviction when shadowed by tombstones

* **Locations**:
  * `src/index/mod.rs` (`IdxSegmentReader::scan_prefix_range`, lines 750–797)
  * `src/memtable/mod.rs` (`MemTable::scan_prefix_range`, lines 145–177)
  * `src/engine/mod.rs` (`StorageEngine::scan_table_range`, lines 784–833; `evict_until_within_quota`, lines 1134–1135; `scan_merged_prefix_range`, lines 1530–1560)
* **Root Causes & Destruction Paths**:
  1. **Per-Segment Tombstone Truncation**:
     Both `IdxSegmentReader::scan_prefix_range` and `MemTable::scan_prefix_range` stop scanning as soon as `out.len() >= limit`, counting `IndexValue::Tombstone` entries toward `limit`.
  2. **Post-Merge Pre-Filter Truncation**:
     Even after merging into `merged_map: BTreeMap<Vec<u8>, (u64, IndexValue)>`, `StorageEngine::scan_merged_prefix_range` (`src/engine/mod.rs:1555-1559`) executes:
     ```rust
     Ok(merged_map
         .into_iter()
         .take(limit)
         .map(|(key, (lsn, value))| IndexEntry { key, lsn, value })
         .collect())
     ```
     which truncates the merged output to `limit` entries **before** `IndexValue::Tombstone` entries are filtered out!
  3. **Destruction Path A (`scan_table_range` / `scan_table_values_into` returns empty/incomplete results)**:
     `scan_table_range` sets `fetch_cap = limit.saturating_add(16)`. Whenever a table has $\ge 1$ SSTable segment (`!self.idx_segments.is_empty()`) and the first `limit + 16` keys in the scan range have been deleted (e.g., 17 tombstones in `MemTable` or a newer SSTable shadowing Segment 1), `scan_merged_prefix_range` returns 17 `Tombstone` entries and `scan_table_range` returns **0 live rows**, ignoring all live rows that immediately follow the 17 deleted keys.
  4. **Destruction Path B (`evict_until_within_quota` fails to evict and leaves Capped Table over `MAX_BYTES`)**:
     `evict_until_within_quota` (`src/engine/mod.rs:1135`) calls `self.scan_merged_prefix_from(&lru_prefix, &seek_key, EVICT_SCAN_WINDOW)` with `EVICT_SCAN_WINDOW = 64`. If an older SSTable has live `'l'` queue keys `tick_1..tick_64` and a newer SSTable (or `MemTable`) has 64 Tombstones for `tick_1..tick_64` (or if 64 tombstones precede live keys across segments), `scan_merged_prefix_from` returns 64 Tombstones. If `last_scanned_tick` does not advance (e.g., if `candidates` is empty or `last_scanned_tick < cursor_tick`), the eviction loop breaks out early while `is_over_quota()` is still true.
  5. **Destruction Path C (`BTreeMap::range` Panic / FFI `SIGABRT` on Empty or Inverted Range Bounds)**:
     `MemTable::scan_prefix_range`, `MemTable::scan_live_prefix_values_into`, and `StorageEngine::scan_table_range` pass `(start_bound, end_bound)` directly to `BTreeMap::range(...)` without checking whether `lower > upper` (or `lower == upper` when either bound is `Bound::Excluded`). An empty SQL range query such as `SELECT * FROM t WHERE k >= 10 AND k <= 5` (or `WHERE k > 5 AND k < 5`) triggers a Rust panic inside `BTreeMap::range` across `extern "system" fn vtab_filter`, immediately aborting the process (`SIGABRT`).
* **Recommended Fix**:
  Guard all range scans so that inverted/empty bounds (`lower > upper` or `lower == upper` with an excluded bound) immediately return an empty result, and implement a streaming K-way merge iterator across `self.memtable` and `self.idx_segments` that resolves the newest LSN per key, discards `IndexValue::Tombstone` entries on the fly, and applies `limit` strictly to **live** entries.

---

### `ISSUE-12` [High]: `xBestIndex` pushes down `LIMIT` on PK range scans even when un-omitted `WHERE` predicates remain on other columns

* **Locations**:
  * `src/vtab/mod.rs` (`vtab_best_index`, lines 349–415; `vtab_filter`, lines 531–570)
* **Root Cause**:
  In `vtab_best_index` (Plan 3: `PLAN_PK_RANGE`), when a SQL query contains a primary-key range constraint (`id >= ?` or `id <= ?`) and a `LIMIT N` clause (`SQLITE_INDEX_CONSTRAINT_LIMIT`), lines 402–407 push `LIMIT N` down into `vtab_filter` (`RANGE_FLAG_HAS_LIMIT`) whenever `info.nOrderBy == 0 || order_by_pk_asc`.
  However, `vtab_best_index` **never checks whether there are other `WHERE` constraints in `info.aConstraint`** (such as `AND category = 'cat_05'` or `AND uval > 100`) that `aydb` does *not* evaluate and that SQLite's VDBE must filter after `xNext`!
  Consequently, for `SELECT * FROM t1 WHERE id >= 1 AND category = 'cat_05' LIMIT 10`, `vtab_filter` stops scanning `t1` after the first 10 rows with `id >= 1`, and SQLite filters out the non-matching rows from those 10, returning far fewer than 10 rows even when thousands of matching rows exist!
* **Recommended Fix**:
  Only push down `SQLITE_INDEX_CONSTRAINT_LIMIT` (`RANGE_FLAG_HAS_LIMIT`) in `vtab_best_index` when **every** constraint in `info.aConstraint` (other than `LIMIT` itself) is marked `omit = 1` and fully satisfied by the range scan.

---

### `ISSUE-13` [Medium]: 32-bit entropy in 64-bit `pk_to_rowid` hash, 8-byte `TEXT`/`BLOB`/`FLOAT` PK misidentification, untagged `NULL`/`""` index collisions, and unbounded `rowid_to_pk` growth

* **Locations**:
  * `src/vtab/mod.rs` (`pk_to_rowid`, lines 114–122; `extract_rowid_from_row`, lines 128–146; `vtab_filter`, lines 481–529; `vtab_update`, lines 663–751)
  * `src/vtab/row_codec.rs` (`SqlValue::to_key_bytes`, lines 95–103; `sqlite_value_to_key_bytes`, lines 262–288)
* **Root Causes**:
  1. **32-Bit Effective Entropy in 64-Bit `pk_to_rowid`**:
     `pk_to_rowid` computes `let h1 = crc32c(encoded_pk) as u64; let h2 = crc32c(&h1.to_le_bytes()) as u64; ((h1 << 32) | h2) as i64`. Because `h2` is a function of `h1` alone, the 64-bit `rowid` has only 32 bits of entropy (~50% collision probability at ~65,536 `TEXT` keys). When a collision occurs in `vtab.rowid_to_pk`, SQLite `UPDATE` and `DELETE` statements silently overwrite or delete the wrong row.
  2. **8-Byte `TEXT` / `BLOB` / `FLOAT` Primary Key Misidentification**:
     `pk_to_rowid` (`line 115`), `extract_rowid_from_row` (`line 139`), and `vtab_update` (`line 746`) assume `encoded_pk.len() == 8` if and only if the primary key is `SQLITE_INTEGER`. An 8-character ASCII `TEXT` key (e.g., `"user0001"`), an 8-byte `BLOB`, or an 8-byte `FLOAT` key has `len() == 8` and is never registered in `rowid_to_pk`. Furthermore, on a `TEXT` PK table, `SELECT * FROM t WHERE pk = 123` (`SQLITE_INTEGER` parameter) is misinterpreted by `vtab_filter` (`line 481`) as a `rowid` lookup.
  3. **Untagged Key Encoding Collides `NULL`, Empty `TEXT` (`""`), and Empty `BLOB` (`X''`) With `omit = 1`**:
     `sqlite_value_to_key_bytes` encodes `SQLITE_NULL`, `""`, and `X''` to the exact same empty byte vector `Vec::new()`. Because `vtab_best_index` sets `omit = 1` on `PLAN_PK_POINT`, `PLAN_SECONDARY_EQ`, and `PLAN_PK_RANGE`, SQLite does not re-verify the column value or type, so `WHERE col = ''` on a secondary index also returns rows where `col IS NULL`.
  4. **Unbounded `rowid_to_pk` Growth**:
     `vtab.rowid_to_pk` inserts entries on every read (`extract_rowid_from_row`) and write (`vtab_update`), while background Capped LRU evictions inside `StorageEngine` never remove evicted keys from `vtab.rowid_to_pk`.

---

### `ISSUE-14` [Medium]: Non-memcomparable IEEE-754 `f64` (`REAL`) key encoding inverts negative sort order

* **Location**:
  * `src/vtab/row_codec.rs` (`SqlValue::to_key_bytes`, line 99; `sqlite_value_to_key_bytes`, line 267; `extract_column_key_bytes`, line 408)
* **Root Cause**:
  `Float(v)` (`f64`) is encoded as `v.to_be_bytes().to_vec()` without IEEE-754 sign/magnitude bit transformation. Under unsigned byte comparison, negative floats sort *after* positive floats (`-1.0 > +1.0`) and in reverse magnitude order (`-2.0 > -1.0`), breaking `REAL` primary-key range scans and `ORDER BY`.

---

## 4. Memory Ceiling Violations & Storage Bloat

### `ISSUE-15` [High]: Standard table blob segments (`blob_std_<SeqID>.db`) have no partial GC or live-blob relocation

* **Locations**:
  * `src/engine/mod.rs` (`reclaim_fully_dead_blob_segments`, lines 1344–1359)
  * `src/index/mod.rs` (`compact_idx_segments`, lines 809–877)
* **Root Cause**:
  `reclaim_fully_dead_blob_segments` only deletes a sealed `blob_std_<SeqID>.db` segment (default `64 MB`) when `b.dead_bytes >= b.file_len` (100% dead). Unlike Capped LRU tables (`blob_lru`), standard tables do not evict old rows in FIFO/LRU order. If 99% of a 64 MB `blob_std` segment is overwritten or deleted while a single 1 KB blob remains live, that 64 MB file is pinned on disk forever because `compact()` never relocates surviving blobs out of high-dead-ratio `blob_std` segments.

---

### `ISSUE-16` [High]: `compact_idx_segments` materializes all entries across all SSTables into a single in-memory `BTreeMap`

* **Locations**:
  * `src/index/mod.rs` (`IdxSegmentReader::scan_all`, lines 719–731; `compact_idx_segments`, lines 820–851)
* **Root Cause**:
  `compact_idx_segments` calls `seg.scan_all()?` on each input SSTable (allocating a `Vec<IndexEntry>` of all entries in that segment) and inserts every entry from every segment into a single `merged_map: BTreeMap<Vec<u8>, IndexEntry>` in RAM before writing the output segment. This requires $O(\text{Total Index Size})$ heap memory during compaction instead of $O(K \times 4\text{ KB})$ streaming merge blocks, violating the engine's strict bounded-memory guarantee.

---

### `ISSUE-17` [Medium]: `IdxSegmentBuilder` clones every key byte vector into `keys_for_bloom: Vec<Vec<u8>>`

* **Locations**:
  * `src/index/mod.rs` (`IdxSegmentBuilder`, lines 408, 443–445, 479–482)
* **Root Cause**:
  During SSTable construction (`flush_to_segment` and `compact_idx_segments`), `IdxSegmentBuilder::add_entry` clones every `entry.key` into `self.keys_for_bloom: Vec<Vec<u8>>` so `finish()` can construct the `BloomFilter` at the end. Because the upper bound on keys is already known before building the segment (or a 32-bit/64-bit hash per key suffices), cloning every `Vec<u8>` key wastes significant heap memory and allocator traffic.

---

### `ISSUE-18` [Medium]: Redundant `boot_meta.db` `FlushFileBuffers` (`fsync`) during sequence ID allocation in flush, compaction, and blob rollover

* **Locations**:
  * `src/engine/mod.rs` (`flush_memtable`, lines 982–984; `compact`, lines 1032–1034; `append_to_blob_segment`, lines 1263–1265, 1301–1303)
* **Root Cause**:
  Allocating a `seq_id` calls `self.meta_mgr.commit_update(|st| { seq_id = st.allocate_seq_id(); })`, performing a full 4 KB write + `FlushFileBuffers()` on `boot_meta.db` *before* writing the segment, followed by a second `commit_update` after writing the segment.

---

## 5. Specification Divergences & Benchmark Defects

### 5.1 Specification vs. Implementation Divergences (`SPEC-01` – `SPEC-05`)

| ID | File / Reference | Documented Behavior | Actual Implementation |
| :--- | :--- | :--- | :--- |
| **SPEC-01** | `README.md` (L32, L48), `docs/phase1/00_OVERVIEW.md` | 4 KB SSTable blocks use prefix compression (front-coding) for sorted keys. | `IdxBlockBuilder` (`src/index/mod.rs:186-212`) stores uncompressed keys with a downward-growing `u16` offset directory. |
| **SPEC-02** | `README.md` (L31), `docs/ARCHITECTURE.md` (L124), `src/memtable/mod.rs` (L6, L78) | Refers to a **20-byte** or **21-byte** `BlobPtr`. | `BlobPtr` (`src/wal/mod.rs:87-115`, `src/index/mod.rs:64`) is **23 bytes** (`kind: 1B + table_id: 2B + seg_id: 8B + offset: 4B + len: 4B + crc32c: 4B`). |
| **SPEC-03** | `src/vtab/row_codec.rs` (L91) | Doc comment states `SqlValue::to_key_bytes` prefixes `Integer(i)` with a 1-byte tag (`0x10`) + 8-byte payload. | Emits raw 8-byte `encode_i64_memcomparable(*v)` without a leading tag byte. |
| **SPEC-04** | `docs/phase1/01_PAL_AND_OS_CACHE.md` (Sections 5.1, 5.3) | Specifies `flock(LOCK_EX \| LOCK_NB)` on `db.lock`, `posix_fadvise` / `fcntl(F_RDADVISE)` hints, and a 128-FD `HandlePool` on POSIX/macOS. | `src/pal/unix.rs` is a minimal `std::fs::File` wrapper where `OpenHint` is unused and `evict_os_cache` is a no-op. |
| **SPEC-05** | `docs/phase1/02_BOOT_META_AND_GC.md` (Section 6) | Claims hardware CRC32C acceleration on both x86_64 SSE4.2 and ARM64 (`is_aarch64_feature_detected!("crc")`). | `src/util/crc32c.rs:51-65` implements hardware CRC32C only for `x86` / `x86_64` SSE4.2 and falls back to software Slice-by-8 on AArch64. |

---

### 5.2 Benchmark Implementation Defects & Fairness Issues (`BENCH-01` – `BENCH-08`)

#### `BENCH-01` [High]: 100% In-Memory `MemTable` Residency in YCSB (256 B & 2 KB), TPC-B, and Speedtest1
* **Location**: `examples/standard_sql_bench.rs` (`bench_speedtest1_suite`, `bench_ycsb_suite`, `bench_tpcb_suite`)
* **Defect**: `EngineConfig::default()` sets `memtable_capacity_bytes = 4 MB`. Because WiscKey separation stores only keys and 23-byte `BlobPtr`s in `MemTable`, the total `MemTable` footprint in Speedtest1 (~600 KB), TPC-B (~300 KB), YCSB 256 B (~200 KB), and YCSB 2 KB (~100 KB) **never reaches 4 MB**. Zero `idx_<SeqID>.db` SSTables are ever flushed or read during these benchmarks; all `aydb` point lookups hit the in-memory `BTreeMap` (`self.memtable`) and all Speedtest1 scans take the `if self.idx_segments.is_empty()` in-memory fast path (`src/engine/mod.rs:686`).

#### `BENCH-02` [High]: TPC-B (`standard_sql_bench.rs`) replaces SQL `UPDATE` statements with `SELECT` + `INSERT OR REPLACE` (7 statements instead of 5) and overwrites foreign keys
* **Location**: `examples/standard_sql_bench.rs` (`run_tpcb_transactions`, lines 634–708)
* **Defect**: Although the file header (line 26) and benchmark title (line 716) state "5 SQL Statements per Transaction (`UPDATE accounts` -> `SELECT abalance` -> `UPDATE tellers` -> `UPDATE branches` -> `INSERT history`)", the implementation executes **3 `SELECT` queries + 4 `INSERT OR REPLACE` statements (7 SQL statements)**. Using `INSERT OR REPLACE` instead of `UPDATE` passes `argv[0] = SQLITE_NULL` to `vtab_update`, completely bypassing SQLite's Virtual Table `UPDATE` path (`xFilter` + `xRowid` + `xColumn` + `xUpdate`). It also overwrites `accounts.bid` and `tellers.bid` with random branch IDs on every transaction.

#### `BENCH-03` [High]: YCSB Workloads A, B, and F (`standard_sql_bench.rs`) use `INSERT OR REPLACE` instead of `UPDATE`, bypassing VTab `rowid_to_pk` resolution
* **Location**: `examples/standard_sql_bench.rs` (`run_ycsb_on_connection`, lines 407–409, 424, 438, 499)
* **Defect**: `upd_stmt` is prepared as `INSERT OR REPLACE INTO usertable VALUES (?1, ?2)` rather than `UPDATE usertable SET field0 = ?2 WHERE ycsb_key = ?1`. For a table with a `TEXT` primary key (`ycsb_key`), `INSERT OR REPLACE` skips `xFilter(PLAN_PK_POINT)` (which would read the existing 2 KB blob from disk), skips `xRowid`, and avoids locking `vtab.rowid_to_pk` to map the synthetic 64-bit `rowid` back to `ycsb_key` in `vtab_update` (and avoids triggering the 32-bit `pk_to_rowid` hash collision in `ISSUE-13`).

#### `BENCH-04` [Medium]: Speedtest1 (`standard_sql_bench.rs`) uses `BufferedUserSpaceCoalesced` (dropping the tail sector on exit) vs. SQLite `synchronous = NORMAL`, halves row/op counts, and runs Test 240 sequentially
* **Location**: `examples/standard_sql_bench.rs` (lines 6–13 vs. 109–112, 217–235, 279 vs. 292, 308)
* **Defects**:
  1. `aydb` is configured with `WalStreamMode::BufferedUserSpaceCoalesced` (64 KB user-space buffering, zero `fsync`, and loses its un-flushed tail sector on `drop(engine)` at line 308 per `ISSUE-03`), whereas Vanilla SQLite runs `synchronous = NORMAL` (OS write per commit + blocking `FlushFileBuffers` auto-checkpoint every 1,000 pages).
  2. Actual row and query counts in `run_speedtest1_on_connection` (`N_ROWS = 5,000`, `N_POINT = 5,000`, `N_RANGE = 1,000`, `N_MUT = 2,500`) are half of the numbers stated in the file's doc comments (`10,000`, `10,000`, `2,000`, `5,000`).
  3. Test 240 ("2,500 Random DELETE + INSERTs") iterates sequentially (`for i in 1..=N_MUT { let id = i as i64; ... }`) instead of using random IDs.

#### `BENCH-05` [Medium]: Asymmetric schema, index, and transaction overhead in `sqlite_vs_aydb_bench.rs` (Benchmarks 2 & 3)
* **Location**: `examples/sqlite_vs_aydb_bench.rs` (lines 210–241, 275–280, 347–385, 417–422)
* **Defects**:
  1. In Benchmark 2 (Bounded Blob Cache), Vanilla SQLite maintains a secondary B-Tree index (`idx_http_cache_tick`) and executes **two separate auto-commit transactions per insert** (`ins_stmt` + `evict_stmt`), whereas `aydb` executes a single statement without a secondary SQL index. Moreover, SQLite retains `256` rows of `16 KB` (`4.00 MB` of raw blobs + B-Tree/key overhead), whereas `aydb` counts `pk.len() + row_bytes.len()` (`16,443 B`) against `max_bytes = 4 MB` and retains `255` rows.
  2. In Benchmark 3 (LRU Cache-Hit Reads), Vanilla SQLite's table has 3 columns + 1 secondary index and executes `SELECT` + `UPDATE` (2 transactions per read), whereas `aydb`'s table omits the `access_tick` column (`columns = 'url TEXT PRIMARY KEY, body BLOB'`).

#### `BENCH-06` [Medium]: Asymmetric Disk Writes & Data Footprint calculations in `sqlite_vs_aydb_bench.rs`
* **Location**: `examples/sqlite_vs_aydb_bench.rs` (lines 111 vs. 161, 247 vs. 302, 388)
* **Defects**:
  1. Benchmark 1 reports `sum_dir_bytes(&sub)` for SQLite (`1,244 KB`), but hardcodes `((N_COMMITS * 4096) as f64) / 1024.0` (`1,200 KB`) for `aydb`, ignoring the pre-allocated `wal_sync.db` (2 MB), `wal_buf.db` (16 MB), and `boot_meta.db` (8 KB) (`sum_dir_bytes` is `18,440 KB`).
  2. Benchmark 2 reports `sum_dir_bytes(&sub)` for SQLite (`8.18 MB`, including `-wal`), but uses `sum_aydb_data_segment_bytes(&sub)` (`3.98 MB`) for `aydb`, excluding `wal_sync.db` (2 MB) and `wal_buf.db` (16 MB) while all 2,000 index entries sit unflushed in the 4 MB in-memory `MemTable`.
  3. Benchmark 3 computes SQLite WAL growth as `after_bytes - before_bytes` (`0.03 MB`), which only measures the final file-size delta after WAL wrap-around/auto-checkpoint rather than cumulative bytes written (`~98 MB`).

#### `BENCH-07` [Low]: Phase 1 microbenchmark flaws in `lru_engine_bench.rs`, `wisckey_bench.rs`, and `pal_bench.rs`
* **Locations**:
  * `examples/lru_engine_bench.rs` (lines 73–85)
  * `examples/wisckey_bench.rs` (lines 53–68, 105–129)
  * `examples/pal_bench.rs` (lines 258–260)
* **Defects**:
  1. `lru_engine_bench.rs`: In Benchmark 1, "WAL Batches Emitted" for `EagerWriteOnRead` is hardcoded to the constant `READ_OPS` (`50,000`), and `st.total_lazy_promotions` for `CoalescedLazyPromotion` is measured *after* inserting 100 new keys outside the timed read loop.
  2. `wisckey_bench.rs`: In Benchmark 1 ("4 Segments x 1,500 Keys"), all 4 segments are populated with the exact same 1,500 keys (`key_000000..key_001499`), and point-read latency only queries `segs.last().unwrap()` (the newest segment, 100% hit rate on the first check).
  3. `pal_bench.rs`: In Part 2B, the background staller thread for `std::fs::File` wraps `f_staller.seek_write(&page, off)` in `#[cfg(windows)]` with no `#[cfg(unix)]` counterpart, so on Unix it loops on `sync_data()` without writing any dirty pages.

#### `BENCH-08` [Low]: `README.md` vs. `02_SQLITE_VS_AYDB_BENCHMARKS.md` metric mismatches
* **Locations**: `README.md` (lines 89–90) vs. `docs/phase2/02_SQLITE_VS_AYDB_BENCHMARKS.md` (lines 35, 61)
* **Discrepancies**: `README.md` states `500 x 16 KB Churn` (actual is `2,000 x 16 KB Churn`) and `~98 MB WAL` (Phase 2 doc table states `0.03 MB`).

---

## 6. Prioritized Remediation Roadmap

1. **P0 — Critical Durability, Crash Recovery, & Transaction Atomicity (`ISSUE-01` – `ISSUE-06`)**:
   * Sync out-of-line blobs on `SyncTransactional` commits and invoke `sync_barrier()` in `WalStreamWriter` (`ISSUE-01`).
   * Delay orphan sweeping of `blob_std_*` / `blob_lru_*` until after WAL recovery in `StorageEngine::open` (`ISSUE-02`).
   * Implement `Drop` on `WalStreamWriter` / `StorageEngine` to flush `BufferedUserSpaceCoalesced` tail sectors (`ISSUE-03`).
   * Reject incomplete `WalBatch`es atomically in `recover_two_streams` when a `PutBlobPtr` fails validation (`ISSUE-04`).
   * Prevent uncommitted transactions from flushing to WAL or SSTables prior to `commit_transaction()`, and properly snapshot/restore engine metadata on `rollback_transaction()` (`ISSUE-05`, `ISSUE-06`).
2. **P1 — Index Scan, Quota Accounting, GC Safety, & VTab Correctness (`ISSUE-07` – `ISSUE-14`)**:
   * Clear `MemTable` only after `boot_meta.db` commit succeeds in `flush_memtable()` (`ISSUE-07`).
   * Persist `table_quotas` in `register_capped_table` and reconcile `current_logical_bytes` on reopen (`ISSUE-08`).
   * Defer physical deletion in `reclaim_fully_dead_blob_segments()` to post-flush/compaction checkpoints, initialize `file_len` from physical file size on recovery, and apply `newly_dead_blob_bytes` (`ISSUE-09`, `ISSUE-10`).
   * Replace per-segment pre-truncated `scan_prefix_range` merging with a streaming K-way merge iterator that filters tombstones before applying `limit` (`ISSUE-11`).
   * Guard `LIMIT` pushdown in `vtab_best_index` when un-omitted `WHERE` constraints exist (`ISSUE-12`).
   * Fix 64-bit `pk_to_rowid` entropy, schema-based PK type detection, type-tagged index keys, and memcomparable `f64` encoding (`ISSUE-13`, `ISSUE-14`).
3. **P2 — Memory Ceiling Compliance, Standard Blob GC, & Benchmark Alignment (`ISSUE-15` – `ISSUE-18`, `SPEC-*`, `BENCH-*`)**:
   * Replace `compact_idx_segments`'s full-table `BTreeMap` and `IdxSegmentBuilder::keys_for_bloom` with streaming block merge and pre-sized Bloom filters (`ISSUE-16`, `ISSUE-17`), and add partial GC / live-blob relocation for `blob_std` segments (`ISSUE-15`).
   * Update benchmarks to test true multi-segment on-disk SSTables and standard SQL `UPDATE` statements (`BENCH-01..08`).
