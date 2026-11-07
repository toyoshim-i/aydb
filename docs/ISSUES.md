# Technical Audit & Known Issues (`aydb`)

This document records the findings from a comprehensive architectural, implementation, and benchmark audit of `aydb` across Phase 1 (Core Storage Engine) and Phase 2 (SQLite Virtual Table & Benchmarks).

Issues are categorized into:
1. **Storage Engine & VTab Correctness / Durability Bugs** (`ISSUE-01` through `ISSUE-07`)
2. **Specification vs. Implementation Divergences** (`SPEC-01` through `SPEC-05`)
3. **Benchmark Methodology & Reporting Discrepancies** (`BENCH-01` through `BENCH-04`)

---

## Summary Matrix

| ID | Category | Severity | Component | Title |
| :--- | :--- | :--- | :--- | :--- |
| **ISSUE-01** | Correctness | **High** | `index` / `engine` | Multi-segment prefix/range scans with `limit` drop live rows when shadowed by tombstones |
| **ISSUE-02** | Durability / Rollback | **High** | `engine` | Premature physical deletion (GC) of sealed blob segments before `MemTable` flush or transaction commit |
| **ISSUE-03** | Crash Consistency | **High** | `engine` / `index` | Compaction discards `newly_dead_blob_bytes` and WAL replay loses unpersisted `dead_bytes` increments |
| **ISSUE-04** | Correctness | **Medium** | `vtab` | 32-bit entropy collision in 64-bit `pk_to_rowid` hash, 8-byte `TEXT`/`BLOB` PK misidentification, and unbounded `rowid_to_pk` growth |
| **ISSUE-05** | Correctness | **Medium** | `vtab::row_codec` | Non-memcomparable IEEE-754 `f64` (`REAL`) primary key encoding inverts negative sort order |
| **ISSUE-06** | Performance | **Medium** | `engine` / `meta` | Redundant `boot_meta.db` `FlushFileBuffers` (`fsync`) during `flush_memtable` and `compact` sequence ID allocation |
| **ISSUE-07** | Eviction Quality | **Low** | `lru` | `AccessTracker` drops access updates for new hot keys once `hot_ticks.len() >= max_tracked_keys` |
| **SPEC-01** | Spec Divergence | Low | `index` / `README` | 4 KB SSTable blocks do not implement front-coding (prefix compression) |
| **SPEC-02** | Spec Divergence | Low | `wal` / `index` | `BlobPtr` is 23 bytes on disk rather than the documented 20 bytes |
| **SPEC-03** | Spec Divergence | Low | `vtab::row_codec` | `SqlValue::to_key_bytes` doc comment mismatches raw 8-byte `INTEGER` key format |
| **SPEC-04** | Spec Divergence | Medium | `pal::unix` | Unix/macOS PAL lacks `db.lock` exclusivity, `posix_fadvise`/`fcntl` hints, and FD LRU pooling |
| **SPEC-05** | Spec Divergence | Low | `util::crc32c` | Hardware CRC32C is only implemented for x86/x86_64 SSE4.2 (AArch64 uses software Slice-by-8) |
| **BENCH-01** | Benchmark Fairness | **High** | `standard_sql_bench` | YCSB, TPC-B, and Speedtest1 datasets fit 100% inside the 4 MB `MemTable` (zero SSTable flushes or disk index reads) |
| **BENCH-02** | Benchmark Fairness | **Medium** | `standard_sql_bench` | Speedtest1 compares `aydb` `BufferedUserSpaceCoalesced` (no `fsync`, 64 KB user-space coalescing) against SQLite `synchronous = NORMAL` |
| **BENCH-03** | Benchmark Metrics | **Medium** | `sqlite_vs_aydb_bench` | Asymmetric disk footprint & write volume accounting between Vanilla SQLite and `aydb` |
| **BENCH-04** | Documentation | Low | `README.md` | Benchmark summary table in `README.md` diverges from `02_SQLITE_VS_AYDB_BENCHMARKS.md` and benchmark source code |

---

## 1. Storage Engine & VTab Correctness / Durability Bugs

### `ISSUE-01` [High]: Multi-segment prefix/range scans with `limit` drop live rows when shadowed by tombstones

* **Location**:
  * `src/index/mod.rs` (`IdxSegmentReader::scan_prefix_range`, lines 750–797)
  * `src/engine/mod.rs` (`StorageEngine::scan_merged_prefix_range`, lines 1530–1560; `StorageEngine::scan_table_range`, lines 636–678)
* **Root Cause**:
  When `StorageEngine::scan_table_range` queries up to `limit` rows, it sets `fetch_cap = limit.saturating_add(16)` and invokes `scan_merged_prefix_range(prefix, start_after, fetch_cap)`.
  Inside `IdxSegmentReader::scan_prefix_range`, each SSTable segment stops scanning as soon as `out.len() >= limit` (i.e. `fetch_cap`), **counting `IndexValue::Tombstone` entries toward `limit`**.
* **Reproduction Scenario**:
  1. Segment 1 (older SSTable) contains 100 live keys `k001..k100`.
  2. Keys `k001..k020` are deleted (or evicted by LRU) and flushed to Segment 2 (newer SSTable) as 20 `IndexValue::Tombstone` records.
  3. A caller runs `scan_table_range` with `limit = 1` (`fetch_cap = 17`) from the beginning of the table:
     * Segment 2 returns its first 17 entries (`k001..k017`, all `Tombstone`) and stops early.
     * Segment 1 returns its first 17 entries (`k001..k017`, all live `BlobPtr`) and stops early.
     * `scan_merged_prefix_range` merges Segment 1 and Segment 2 into `merged_map`: `k001..k017` are all overwritten by Segment 2's tombstones.
     * `scan_table_range` filters out all tombstones and returns **0 rows**, even though 80 live rows (`k021..k100`) exist immediately after `k020`.
* **Recommended Fix**:
  Replace the per-segment pre-truncated `Vec` collection in `scan_merged_prefix_range` with a streaming K-way merge iterator across `self.memtable` and all `IdxSegmentReader` instances, applying `limit` only **after** deduplicating versions across levels and filtering out `IndexValue::Tombstone`.

---

### `ISSUE-02` [High]: Premature physical deletion (GC) of sealed blob segments before `MemTable` flush or transaction commit

* **Location**:
  * `src/engine/mod.rs` (`StorageEngine::put_row`, lines 390–392, 469; `StorageEngine::delete_row`, lines 518–520, 548; `StorageEngine::reclaim_fully_dead_blob_segments`, lines 1344–1386; `StorageEngine::rollback_transaction`, lines 896–914)
* **Root Cause**:
  Whenever `put_row` or `delete_row` overwrites or deletes a key whose previous value was a `BlobPtr`, it immediately increments `dead_bytes` on the target `BlobSegmentMeta` in memory (`record_dead_blob_bytes`) and calls `reclaim_fully_dead_blob_segments()`.
  If the segment is no longer the active writer (`seq_id != active_std_seg`) and `dead_bytes >= valid_bytes`, `reclaim_fully_dead_blob_segments()` immediately unlinks the physical `blob_std_<SeqID>.db` / `blob_lru_<SeqID>.db` file from disk (`self.fs.remove_file(&fname)`), before the new index state is durable or committed.
* **Failure Modes**:
  1. **Transaction `ROLLBACK` Data Loss**:
     Inside an explicit `BEGIN ...` transaction, updating or deleting rows that invalidate the remaining live blobs of a sealed blob segment triggers immediate physical deletion of that `.db` file. If the user subsequently executes `ROLLBACK`, `rollback_transaction()` restores `self.memtable` from `TxSnapshot` (which still points to the old `BlobPtr`s), and also does not restore `std_blobs`/`lru_blobs`. Subsequent reads fail with `AydbError::Io` / `CorruptRecord` because the underlying blob segment file was already deleted.
  2. **Crash Before `MemTable` Flush**:
     If an older SSTable (`idx_<SeqID>.db`) references blobs in sealed Blob Segment `S`, and buffered mutations in `self.memtable` (or un-synced `wal_buf.db`) overwrite those keys, Blob Segment `S` is physically deleted immediately. If power fails before `flush_memtable()` commits the updated SSTable and `boot_meta.db`, recovery restores the older SSTable whose `BlobPtr`s now point to a deleted blob segment file.
* **Recommended Fix**:
  * Defer `reclaim_fully_dead_blob_segments()` so that physical file reclamation only occurs during `flush_memtable()` and `compact()` **after** `boot_meta.db` has been durably committed, and never while an uncommitted transaction (`self.tx_snapshot.is_some()`) is active.
  * Include `std_blobs` and `lru_blobs` state in `TxSnapshot` so `rollback_transaction()` accurately restores `dead_bytes` counters.

---

### `ISSUE-03` [High]: Compaction discards `newly_dead_blob_bytes` and WAL replay loses unpersisted `dead_bytes` increments

* **Location**:
  * `src/index/mod.rs` (`compact_idx_segments`, lines 801–877)
  * `src/engine/mod.rs` (`StorageEngine::compact`, lines 1023–1081; `StorageEngine::open_with_config`, lines 228–320)
* **Root Cause**:
  1. `compact_idx_segments` computes `CompactionOutcome::newly_dead_blob_bytes` whenever newer SSTable entries shadow older `BlobPtr` entries or annihilate them via tombstones. However, `StorageEngine::compact()` ignores `outcome.newly_dead_blob_bytes` completely.
  2. Currently, `dead_bytes` is tracked eagerly in memory during `put_row` and `delete_row` (`record_dead_blob_bytes`), and persisted to `boot_meta.db` only when `flush_memtable()` or `compact()` commits. Because WAL frames only record the new key/value (and not the overwritten old `BlobPtr`), if the process crashes after writing to WAL but before `flush_memtable()`, WAL replay on startup (`open_with_config`) inserts records into `self.memtable` without calling `record_dead_blob_bytes` against the shadowed SSTable entries.
  3. Consequently, those shadowed blob bytes are neither counted during WAL replay nor applied when `compact()` later merges the SSTables, causing a permanent `dead_bytes` undercount that prevents dead blob segments from ever reaching `dead_bytes >= valid_bytes`.
* **Recommended Fix**:
  Either:
  * Lookup and account for overwritten keys during WAL replay just like `put_row`/`delete_row`, **or**
  * Track `dead_bytes` solely at flush/compaction boundaries (or recompute live/dead bytes per blob segment during compaction) so that `dead_bytes` is always a deterministic function of committed index state.

---

### `ISSUE-04` [Medium]: 32-bit entropy collision in 64-bit `pk_to_rowid` hash, 8-byte `TEXT`/`BLOB` PK misidentification, and unbounded `rowid_to_pk` growth

* **Location**:
  * `src/vtab/mod.rs` (`pk_to_rowid`, lines 114–123; `extract_rowid_from_row`, lines 126–148; `vtab_update`, lines 653–751)
* **Root Causes**:
  1. **32-Bit Effective Entropy in 64-Bit `rowid` Hash**:
     For non-8-byte primary keys, `pk_to_rowid` computes:
     ```rust
     let h1 = crc32c(encoded_pk) as u64;
     let h2 = crc32c(&h1.to_le_bytes()) as u64;
     ((h1 << 32) | h2) as i64
     ```
     Because `h2` hashes `h1.to_le_bytes()` rather than `encoded_pk` with an independent seed, `h2` is 100% determined by `h1`. The 64-bit `rowid` therefore has only **32 bits of entropy**. By the birthday bound, a table with ~65,536 `TEXT` primary keys has a >50% probability of a `rowid` collision in `vtab.rowid_to_pk`, causing SQLite `UPDATE` or `DELETE` statements (which identify target rows by `rowid` via `xRowid` -> `xUpdate`) to silently overwrite or delete the wrong primary key.
  2. **8-Byte `TEXT` / `BLOB` Primary Key Misidentification**:
     `vtab_update` (line 746) checks `if new_pk.len() != 8` to decide whether a primary key is non-integer (`vtab.has_non_int_pks = true` and `vtab.rowid_to_pk.insert(rid, new_pk)`). If a `TEXT` or `BLOB` primary key happens to be **exactly 8 bytes long** (e.g., `"user0001"`), it is treated as an `INTEGER` key: it is not inserted into `rowid_to_pk`, and `extract_rowid_from_row` vs. `vtab_filter` (`PLAN_PK_POINT`) apply inconsistent integer-vs-bytes conversions.
  3. **Unbounded `rowid_to_pk` Memory Growth on Capped LRU Tables**:
     Every read/write on a non-integer PK table inserts into `vtab.rowid_to_pk`, and only explicit SQL `DELETE` statements remove entries from `rowid_to_pk`. Background LRU evictions inside `StorageEngine` never purge evicted keys from `vtab.rowid_to_pk`.
* **Recommended Fix**:
  * Determine whether the primary key is `INTEGER` from the table schema (`ColumnSchema`) or a 1-byte type tag rather than `encoded_pk.len() == 8`.
  * Compute `h2` using a distinct seed over `encoded_pk` (e.g., `crc32c_append(0x9E37_79B9, encoded_pk)` or a 64-bit hash such as SipHash/FxHash64) and handle hash collisions via a bucket list or secondary probe in `StorageEngine`.

---

### `ISSUE-05` [Medium]: Non-memcomparable IEEE-754 `f64` (`REAL`) primary key encoding inverts negative sort order

* **Location**:
  * `src/vtab/row_codec.rs` (`SqlValue::to_key_bytes`, line 99; `sqlite_value_to_key_bytes`, line 267; `decode_pk_to_sql_value`, line 408)
* **Root Cause**:
  While `INTEGER` (`i64`) primary keys use `encode_i64_memcomparable` (flipping the sign bit `^ (1 << 63)` before big-endian serialization), `Float(v)` (`f64`) is serialized directly via `v.to_be_bytes().to_vec()`.
  Because IEEE-754 floating-point numbers use sign-magnitude representation, raw big-endian byte comparison places all negative numbers *after* positive numbers (`0x80... > 0x3F...`) and reverses the relative order of negative numbers (`-2.0` sorts after `-1.0`).
* **Recommended Fix**:
  Encode `f64` keys using standard memcomparable IEEE-754 transformation: if the sign bit (`1 << 63`) is `0` (positive/zero), flip the sign bit (`bits ^ (1 << 63)`); if the sign bit is `1` (negative), flip all 64 bits (`!bits`). Apply the inverse transformation in `decode_pk_to_sql_value`.

---

### `ISSUE-06` [Medium]: Redundant `boot_meta.db` `FlushFileBuffers` (`fsync`) during `flush_memtable` and `compact` sequence ID allocation

* **Location**:
  * `src/engine/mod.rs` (`StorageEngine::flush_memtable`, lines 982–1016; `StorageEngine::compact`, lines 1032–1075)
* **Root Cause**:
  Both `flush_memtable()` and `compact()` call `self.meta_mgr.commit_update(|p| { seg_id = p.allocate_seq_id(); })` *before* writing the new SSTable, and then call `self.meta_mgr.commit_update(...)` a second time immediately after writing the SSTable to register the segment in the manifest. Each `commit_update` performs a 4 KB write to `boot_meta.db` followed by `FlushFileBuffers()`.
* **Recommended Fix**:
  Allocate the next `seq_id` in memory (or via a non-flushing helper on `BootMetaManager`) and persist the updated `next_seq_id` together with the new `IdxSegmentMeta` in a **single** `commit_update` call after the SSTable file is synced.

---

### `ISSUE-07` [Low]: `AccessTracker` drops access updates for new hot keys once `hot_ticks.len() >= max_tracked_keys`

* **Location**:
  * `src/lru/mod.rs` (`AccessTracker::record_access`, lines 112–120)
* **Root Cause**:
  When `self.hot_ticks.len() >= self.max_tracked_keys` (default `65,536`), `record_access` updates already-tracked keys but silently drops access tracking for any newly accessed key until an eviction pass drains entries from `hot_ticks`.
* **Recommended Fix**:
  When `hot_ticks` reaches capacity, either evict a random/oldest entry from `hot_ticks` or periodically fold/clear stale entries whose tick is older than `eviction_cursor_tick`.

---

## 2. Specification vs. Implementation Divergences

| ID | File / Reference | Documented Behavior | Actual Implementation |
| :--- | :--- | :--- | :--- |
| **SPEC-01** | `README.md` (L32, L48), `docs/phase1/00_OVERVIEW.md` | 4 KB SSTable blocks use prefix compression (front-coding) for sorted keys. | `IdxBlockBuilder` (`src/index/mod.rs:186-270`) stores full uncompressed keys with a downward-growing `u16` offset directory at the end of the 4 KB block. |
| **SPEC-02** | `README.md` (L31), `docs/ARCHITECTURE.md` (L124) | Refers to a **20-byte** `BlobPtr`. | `BlobPtr` (`src/wal/mod.rs:87-115`) is **23 bytes** on disk (`kind: 1B + table_id: 2B + seg_id: 8B + offset: 4B + len: 4B + crc32c: 4B`). |
| **SPEC-03** | `src/vtab/row_codec.rs` (L89–97) | Doc comment states `SqlValue::to_key_bytes` prefixes `Integer(i)` with a 1-byte tag (`0x10`) + 8-byte payload. | Emits raw 8-byte `encode_i64_memcomparable(*v)` without a leading tag byte. |
| **SPEC-04** | `docs/phase1/01_PAL_AND_OS_CACHE.md` (Sections 5.1, 5.3) | Specifies `flock(LOCK_EX \| LOCK_NB)` on `db.lock`, `posix_fadvise` / `fcntl(F_RDADVISE)` hints, and a 128-FD `HandlePool` on POSIX/macOS. | `src/pal/unix.rs` is a minimal `std::fs::File` wrapper where `OpenHint` is unused (`#[allow(dead_code)]`) and `evict_os_cache` is a no-op. |
| **SPEC-05** | `docs/phase1/02_BOOT_META_AND_GC.md` (Section 6) | Claims hardware CRC32C acceleration on both x86_64 SSE4.2 and ARM64 (`is_aarch64_feature_detected!("crc")`). | `src/util/crc32c.rs:51-65` implements hardware CRC32C only for `x86` / `x86_64` SSE4.2 and falls back to software Slice-by-8 on all other architectures. |

---

## 3. Benchmark Methodology & Reporting Discrepancies

### `BENCH-01` [High]: YCSB, TPC-B, and Speedtest1 datasets fit 100% inside the 4 MB `MemTable` (zero SSTable flushes or disk index reads)

* **Location**:
  * `examples/standard_sql_bench.rs` (`bench_speedtest1_suite`, lines 291–295; `bench_tpcb`, lines 483–486; `bench_ycsb_suite`, lines 570–590)
  * `src/engine/mod.rs` (`DEFAULT_MEMTABLE_CAPACITY_BYTES = 4 * 1024 * 1024`, line 32; `scan_table_values_into`, line 686)
* **Analysis**:
  In `standard_sql_bench.rs`, `aydb` uses the default 4 MB `memtable_capacity_bytes`. Because WiscKey separation stores only keys and 23-byte `BlobPtr`s in the `MemTable`:
  * **Speedtest1** (5,000 rows, inline values): ~600 KB in `MemTable`.
  * **TPC-B** (10,000 accounts + 1,000 history rows): ~300 KB in `MemTable`.
  * **YCSB 256 B** (4,000 initial + 5,000 ops): ~200 KB in `MemTable` (blobs go to `blob_std`).
  * **YCSB 2 KB** (2,000 initial + 2,000 ops): ~100 KB in `MemTable`.
  Consequently, **not a single `MemTable` flush (`idx_<SeqID>.db`) occurs during any of these benchmarks**.
  * Every point lookup (`1.23M ops/s` in YCSB-C) is served directly from the in-memory Rust `BTreeMap` (`self.memtable`) without touching Bloom filters, Sparse Fence Pointers, or 4 KB SSTable blocks.
  * Every full-table and range scan in Speedtest1 takes the `if self.idx_segments.is_empty()` in-memory fast path in `scan_table_values_into` (`src/engine/mod.rs:686`).
  * Meanwhile, Vanilla SQLite reads and writes through its 4 KB B-Tree pager, `-shm` WAL index, and periodic blocking `wal_autocheckpoint` (`FlushFileBuffers`) every 1,000 pages (~4 MB).
* **Recommendation**:
  Add benchmark configurations that either lower `memtable_capacity_bytes` (e.g., `64 KB`–`256 KB`) or scale row counts so that multiple `idx_<SeqID>.db` SSTable segments are flushed and queried on disk, and clearly distinguish "In-Memory MemTable Working Set" vs. "Multi-Segment On-Disk SSTable Working Set" in documentation.

---

### `BENCH-02` [Medium]: Speedtest1 compares `aydb` `BufferedUserSpaceCoalesced` against SQLite `synchronous = NORMAL`

* **Location**:
  * `examples/standard_sql_bench.rs` (lines 279 vs. 292)
* **Analysis**:
  In `bench_speedtest1_suite`, Vanilla SQLite runs with `PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL` (which issues OS `WriteFile` calls on every transaction commit and `FlushFileBuffers` on WAL auto-checkpoints), whereas `aydb` explicitly overrides `cfg.buf_wal_mode = WalStreamMode::BufferedUserSpaceCoalesced` (which buffers WAL frames in a 64 KB user-space `Vec<u8>` without issuing `WriteFile` on commits and performs zero `fsync` calls).
* **Recommendation**:
  Use `WalStreamMode::BufferedImmediateOsCache` when comparing against SQLite `synchronous = NORMAL`, or compare `BufferedUserSpaceCoalesced` against SQLite `PRAGMA synchronous = OFF`.

---

### `BENCH-03` [Medium]: Asymmetric disk footprint & write volume accounting between Vanilla SQLite and `aydb`

* **Location**:
  * `examples/sqlite_vs_aydb_bench.rs` (Benchmark 1: lines 111 vs. 161; Benchmark 2: lines 247 vs. 302; Benchmark 3: line 388)
* **Analysis**:
  1. **Benchmark 1 (Durable Micro-Transactions)**:
     Vanilla SQLite's "Disk Writes" is reported via `sum_dir_bytes(&sub)` (final directory size on disk: `1,244.0 KB`), whereas `aydb`'s "Disk Writes" is hardcoded to `((N_COMMITS * 4096) as f64) / 1024.0` (`1,200.0 KB`), omitting the pre-allocated `wal_sync.db` (2 MB), `wal_buf.db` (16 MB), and `boot_meta.db` (8 KB) (`sum_dir_bytes` for `aydb` is `18,440 KB`).
  2. **Benchmark 2 (Bounded Blob Cache)**:
     Vanilla SQLite's "Data Footprint" is measured with `sum_dir_bytes(&sub)` (`8.18 MB`, which includes the un-checkpointed `-wal` file and free list pages), whereas `aydb`'s "Data Footprint" uses `sum_aydb_data_segment_bytes(&sub)` (`3.98 MB`), which explicitly excludes `wal_sync.db` (2 MB) and `wal_buf.db` (16 MB) while all 2,000 index entries (`'r'`, `'l'`, `'t'`) remain unflushed in the 4 MB in-memory `MemTable`.
  3. **Benchmark 3 (LRU Cache-Hit Reads)**:
     SQLite's WAL growth is computed as `after_bytes - before_bytes` using `sum_dir_bytes(&sub)`. Because SQLite auto-checkpoints and reuses its `-wal` file every 1,000 pages (~4 MB), `after_bytes - before_bytes` only measures final file-size delta (`0.03 MB`) rather than cumulative bytes written (`~98 MB`).

---

### `BENCH-04` [Low]: Benchmark summary table in `README.md` diverges from `02_SQLITE_VS_AYDB_BENCHMARKS.md` and benchmark source code

* **Location**:
  * `README.md` (lines 89–90)
  * `docs/phase2/02_SQLITE_VS_AYDB_BENCHMARKS.md` (lines 35, 61)
  * `examples/sqlite_vs_aydb_bench.rs` (line 195)
* **Discrepancies**:
  * `README.md` line 89 lists `500 x 16 KB Churn (4.0 MB Quota Enforcement)` for the Bounded Blob Cache benchmark, whereas `examples/sqlite_vs_aydb_bench.rs` and `02_SQLITE_VS_AYDB_BENCHMARKS.md` execute **`2,000` x 16 KB Churn (32.0 MB written)**.
  * `README.md` line 90 lists `~98 MB WAL` for Vanilla SQLite in the LRU Cache-Hit Reads benchmark, whereas `02_SQLITE_VS_AYDB_BENCHMARKS.md` reports `0.03 MB` WAL growth.

---

## 4. Prioritized Remediation Roadmap

1. **P0 — Fix Storage Engine Correctness & Crash/Rollback Safety (`ISSUE-01`, `ISSUE-02`, `ISSUE-03`)**:
   * Implement streaming K-way merge iteration in `StorageEngine::scan_merged_prefix_range` so `limit` is applied after Tombstone filtering (`ISSUE-01`).
   * Move `reclaim_fully_dead_blob_segments()` out of `put_row`/`delete_row` into `flush_memtable()`/`compact()` post-commit, include `std_blobs`/`lru_blobs` in `TxSnapshot`, and reconcile `dead_bytes` across WAL replay and compaction (`ISSUE-02`, `ISSUE-03`).
2. **P1 — Fix VTab Primary Key & Rowid Semantics (`ISSUE-04`, `ISSUE-05`)**:
   * Replace the 32-bit-entropy `pk_to_rowid` hash with a true 64-bit hash, detect `INTEGER` primary keys from schema metadata rather than `len() == 8`, and implement memcomparable IEEE-754 `f64` key encoding (`ISSUE-04`, `ISSUE-05`).
3. **P2 — Optimize Metadata Syncs & Align Documentation/Benchmarks (`ISSUE-06`, `SPEC-*`, `BENCH-*`)**:
   * Coalesce `allocate_seq_id()` into the post-flush `boot_meta.db` commit (`ISSUE-06`).
   * Add SSTable-exercising benchmark profiles and synchronize `README.md` and `docs/` with the implementation (`SPEC-01..05`, `BENCH-01..04`).
