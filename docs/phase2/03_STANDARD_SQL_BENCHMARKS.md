# Chapter 2.3: Selected Standard SQL Benchmark Workloads (`Speedtest1`-Style, `YCSB`-Style `A–F`, and `TPC-B`-Style)

To evaluate `aydb` objectively beyond custom cache/config microbenchmarks, this chapter presents results from three representative standard database workload styles (`examples/standard_sql_bench.rs`) executed via identical SQL statements against **Vanilla SQLite (`PRAGMA journal_mode = WAL`)** and **`aydb` Virtual Table (`USING aydb(...)`)** after passing a **pre-benchmark differential SQL equivalence verification suite (`verify_differential_sql_equivalence`)**:

0. **Pre-Benchmark Differential SQL Equivalence Verification (`Suite 0`):** Asserts 100% identical query outputs and final table states between a native SQLite table and an `aydb` virtual table across mixed numeric sorting (`INTEGER` & `REAL` Unified Numeric `ORDER BY`, `F05`), column affinity coercions (`WHERE id = 1.0`, `WHERE id = '1'`, `F04`), `COLLATE NOCASE` / `COLLATE RTRIM` lookups (`F04`), failed `UNIQUE` constraint statements, nested `SAVEPOINT` / `ROLLBACK TO`, cross-connection snapshot isolation (`SQLITE_BUSY`, `F01`), and mixed native-SQLite + `aydb` virtual table two-phase transactions (`F07`).
1. **SQLite `"Speedtest1"`-Style Workload (Selected `Tests 100..240`):** Selected core SQL sub-tests modeled on SQLite's `Speedtest1` workload.
2. **`YCSB`-Style Workloads `A` through `F` (Zipfian $\theta = 0.99$):** Evaluated at both **`256 B` (Inline Storage)** and **`2,048 B` (WiscKey Separated Blob Storage)** to expose the exact boundary where WiscKey outperforms B-Trees and where WiscKey incurs range-scan dereference overhead.
3. **`TPC-B`-Style (`pgbench`-like) 4-Table OLTP Workload:** Multi-statement ACID transactions across `branches`, `tellers`, `accounts`, and `history`.

> **Scope & Interpretation Notes:**
> * **Workload Coverage:** These examples implement selected `Speedtest1`-style, `YCSB`-style, and `TPC-B`-style workloads rather than full certified/official benchmark suites.
> * **Warm OS Cache vs. Larger-Than-RAM Datasets:** The `5,000`-row / `4,000`-record / `2,000`-account workloads fit in RAM. Lowering `memtable_capacity_bytes = 128 KB` (§4) forces `idx_*.db` SSTable flushes and size-tiered compactions (`P01`), exercising positional `ReadFile(OVERLAPPED)` calls against warm OS page cache rather than cold physical storage reads.
> * **Single-Host Execution & Simulated Fault Model:** Throughput measurements are single-host local Windows NTFS runs; power-loss and I/O fault tests in `examples/reliability_bench.rs` use deterministic in-memory simulation (`FaultInjectingFs`) and should be interpreted within that tested failure model rather than as physical hardware power-cut or live SMB network-outage tests.

<p align="center">
  <img src="../assets/bench_speedup_overview.svg" alt="aydb Relative Speedup Overview" width="100%"/>
</p>

---

## 1. Suite 1: SQLite `"Speedtest1"`-Style Workload (Selected `Tests 100..240`, `5,000` Indexed Rows)

* **Schema:** `t1(id INT PRIMARY KEY, uval INT, category TEXT, payload TEXT)` with secondary index on `uval` (`120 B` payload, `~600 KB` working set).
* **Durability & Scale Parity (`BENCH-01`, `BENCH-03`):** Both engines run with equivalent OS-page-cache commit durability (`PRAGMA synchronous = NORMAL` on SQLite vs. `WalStreamMode::BufferedImmediateOsCache` on `aydb` `wal_buf.db`) and full `PRIMARY KEY` uniqueness enforcement (`AUD-03`). At `5,000` rows (`~600 KB < 4 MB` default `MemTable`), `aydb` serves index reads from `MemTable` (`BTreeMap`) while SQLite serves them from its `2 MB` in-process `PCache`; see Section 4 below for on-disk `idx_*.db` SSTable performance (`memtable_capacity_bytes = 128 KB`).
* **Note on Workload Structure:** In `Speedtest1`-style workloads, Tests `100`, `230`, and `240` wrap thousands of mutations inside a **single giant `BEGIN ... COMMIT` transaction** where SQLite's native B-Tree mutates pages in its in-process user-space page cache without FFI boundary crossings per row.

<p align="center">
  <img src="../assets/bench_speedtest1_breakdown.svg" alt="SQLite Speedtest1-Style Execution Time Breakdown" width="100%"/>
</p>

| Speedtest1-Style Sub-Benchmark (`BufferedImmediateOsCache` vs `NORMAL`) | Vanilla SQLite (`WAL`) | `aydb` Virtual Table | Relative Speed (`aydb` vs. SQLite) | Architectural Analysis |
| :--- | ---: | ---: | ---: | :--- |
| **Test 100:** `5,000` Bulk `INSERT`s (`BEGIN..COMMIT`, Indexed) | `5.00 ms` | `14.78 ms` | `0.34×` (`2.96×` slower) | Per-row `xUpdate` Virtual Table C ABI call + `Mutex` lock + `row_exists` PK uniqueness check (`AUD-03`) + `RowMetaRecord` secondary index maintenance vs. native in-page B-Tree append. |
| **Test 130:** `5,000` Random PK Point `SELECT`s (`WHERE id = ?`) | `14.37 ms` | **`5.33 ms`** | **`2.70×` faster** | Zero-allocation stack-key `get_row_into` + reusable `AydbCursor` byte slab + zero-copy `xColumn` (`project_column_zero_copy`). |
| **Test 140:** `5,000` Secondary Index `SELECT`s (`WHERE uval = ?`) | `15.15 ms` | **`6.62 ms`** | **`2.29×` faster** | Direct memcomparable prefix range iteration (`iter_range`) + zero-allocation PK slice extraction. |
| **Test 160:** `1,000` PK Range Scans (`WHERE id >= ? AND id < ?`, 20 rows) | `5.47 ms` | **`4.17 ms`** | **`1.31×` faster** | `scan_range_into_slab` appends matching range slices directly into the reusable cursor byte slab without per-row `Vec<u8>` allocations (`P02`). |
| **Test 210:** `10×` Full-Table `GROUP BY` Aggregation (`5,000` rows) | `10.11 ms` | `16.51 ms` | `0.61×` (`1.63×` slower) | Virtual Table full-table scan copies 5,000 rows per pass across `xFilter`/`xColumn` (`sqlite_transient()`) into VDBE registers; Phase 3 Native SQL eliminates this FFI copy. |
| **Test 230:** `2,500` Random Indexed `UPDATE`s (`BEGIN..COMMIT`) | `4.14 ms` | `12.09 ms` | `0.34×` (`2.92×` slower) | Each Virtual Table `UPDATE` executes a full `xFilter` + `xRowid` + `xColumn` + `xUpdate` cycle across the C ABI boundary plus undo-log tracking (`PERF-03`). |
| **Test 240:** `2,500` Random `DELETE` + Re-`INSERT`s (`BEGIN..COMMIT`) | `4.96 ms` | `15.29 ms` | `0.32×` (`3.08×` slower) | `5,000` Virtual Table `xUpdate` FFI crossings + PK existence checks inside a single in-memory transaction. |
| **TOTAL `Speedtest1`-Style Suite Time (`4 MB` `MemTable`)** | `59.21 ms` | `74.78 ms` | `0.79×` (`234.51 ms` / `0.27×` at `128 KB` SSTable) | `aydb` leads `1.31×–2.70×` on PK point, secondary-index, and range queries (`Tests 130, 140, 160`), while single-transaction bulk mutation loops (`Tests 100, 230, 240`) pay per-row `sqlite3_module` C ABI and PK uniqueness checks. |

### 1.1 Virtual Table & StorageEngine Hot-Path Optimizations Applied
To accelerate `Speedtest1`-style query and range-scan execution (`Tests 130, 140, 160` running **`1.31×–2.70×` faster than Vanilla SQLite**) and reduce Virtual Table FFI overhead, six targeted hot-path optimizations were engineered across `src/vtab/` and `src/engine/`:
1. **Zero-Copy `xColumn` Projection (`project_column_zero_copy`) & Keys-Only Pushdown (`RANGE_FLAG_KEYS_ONLY`, `P02`):** Instead of decoding a column into a heap-allocated `SqlValue::Text(String)` or `SqlValue::Blob(Vec<u8>)` and immediately dropping it after `sqlite3_result_text`, `xColumn` indexes the $O(1)$ column offset directory and passes `&row_bytes[offset + 4 .. offset + 4 + len]` directly to `sqlite3_result_text` / `sqlite3_result_blob` with **zero Rust heap allocations**. Furthermore, when `sqlite3_index_info::colUsed` indicates only `rowid` or a binary-collated PK column is needed (such as `SELECT COUNT(*)`), `xFilter` skips reading out-of-line blobs and skips populating the cursor byte slab entirely (`P02`).
2. **Direct `sqlite3_value*` Row Encoding (`encode_sqlite_values_direct`):** `xUpdate` encodes SQLite's raw `*mut *mut sqlite3_value` array directly into the binary row buffer in a single pass without allocating an intermediate `Vec<SqlValue>`.
3. **Reusable Contiguous Cursor Byte Slab (`AydbCursor::slab` & `spans`) & Streaming Slab Scans (`scan_range_into_slab`, `P02`):** Rather than allocating and freeing thousands of individual `Vec<u8>` row buffers on every `xFilter` call (`50,000` allocations in `Test 210` and `20,000` in `Test 160`), `AydbCursor` retains a single contiguous `slab: Vec<u8>` and `spans: Vec<(usize, usize)>`, populated directly in-place via `get_row_into` and `scan_range_into_slab`.
4. **Lock-Free Reversible Integer `rowid` Mapping (`has_non_int_pks`):** Because 9-byte tagged integer primary keys (`[KEY_TAG_NUM_NARROW | memcomparable_i64 | 0]`, `F05`) map bijectively to 64-bit SQLite `rowid`s via `encode_i64_tagged_key` / `decode_i64_tagged_key`, integer-PK tables completely bypass locking and inserting into `rowid_to_pk: Mutex<RowidMap>`.
5. **Zero-Clone `MemTable` Ownership Transfer (`apply_wal_batch_owned` & `pending_tx_buf`) + $O(\text{tx})$ Undo Log (`PERF-03`):** Transaction mutations serialize directly into a reusable `pending_tx_buf: Vec<u8>` and move their owned `key: Vec<u8>` and `value: Vec<u8>` into `MemTable`'s `BTreeMap` while recording prior entries in an in-memory undo log for instant $O(\text{tx})$ rollback, `SAVEPOINT` restoration, and cross-connection snapshot isolation (`F01`).
6. **Multi-Sector Coalesced WAL Writes (`multi_sector_buf`):** When a large transaction batch spans $K > 1$ sectors (e.g., ~250 sectors in `Test 100`), `WalStreamWriter::append_raw_batch` seals all $K$ sectors in memory and issues a **single contiguous `write_all_at` syscall** instead of $K$ separate 4 KB `WriteFile` syscalls.

---

## 2. Suite 2: `YCSB`-Style Workloads `A` through `F` (Zipfian $\theta = 0.99$)

`YCSB`-style workloads evaluate realistic autocommit request streams across 6 standard access mixes (`4,000` initial rows, `5,000` operations per workload, Zipfian skew $\theta = 0.99$, using true SQL `UPDATE` statements, with `WalStreamMode::BufferedImmediateOsCache` on `aydb` vs. `PRAGMA synchronous = NORMAL` on SQLite). We run the entire `YCSB`-style suite twice: once at **`256 B` (Inline Storage, `~1.1 MB` total)** and once at **`2,048 B` (WiscKey Out-of-Line Blob Storage, `~8.2 MB` initial + `~5 MB` updates in `blob_std_*.db`)**.

<p align="center">
  <img src="../assets/bench_ycsb_throughput.svg" alt="YCSB-Style Workloads A-F Throughput Comparison" width="100%"/>
</p>

### 2.1 `YCSB`-Style at `256 B` Value Size (Below `1024 B` Inline Threshold)

| YCSB-Style Workload | Mix | Vanilla SQLite (`WAL`, `NORMAL`) | `aydb` Virtual Table (`4 MB` Default / `128 KB` SSTable) | Speedup (`aydb` / SQLite) |
| :--- | :--- | ---: | ---: | ---: |
| **Workload A (Update Heavy)** | 50% Read / 50% Update | `159,405 ops/s` | **`294,391 ops/s`** | **`1.85×` faster** |
| **Workload B (Read Mostly)** | 95% Read / 5% Update | `267,224 ops/s` | **`895,271 ops/s`** (SSTable: **`347,321 ops/s`**) | **`3.35×` faster** (**`1.17×` SSTable**) |
| **Workload C (Read Only)** | 100% Point Read | `284,743 ops/s` | **`1,248,252 ops/s`** (SSTable: **`433,783 ops/s`**) | **`4.38×` faster** (**`1.42×` SSTable**) |
| **Workload D (Read Latest)** | 95% Read Latest / 5% Insert | `221,094 ops/s` | **`928,143 ops/s`** | **`4.20×` faster** |
| **Workload E (Short Ranges)** | 95% Scan (10 rows) / 5% Insert | `104,369 ops/s` | **`265,758 ops/s`** | **`2.55×` faster** |
| **Workload F (Read-Modify-Write)** | 50% Read / 50% RMW | `166,273 ops/s` | **`253,175 ops/s`** | **`1.52×` faster** |

### 2.2 `YCSB`-Style at `2,048 B` Value Size (Above `1024 B` — WiscKey Separated Blob Storage)

| YCSB-Style Workload | Mix | Vanilla SQLite (`WAL`, `NORMAL`) | `aydb` Virtual Table (`blob_std_*.db`) | Speedup (`aydb` / SQLite) |
| :--- | :--- | ---: | ---: | ---: |
| **Workload A (Update Heavy)** | 50% Read / 50% Update | **`151,130 ops/s`** | `81,924 ops/s` | `0.54×` (`1.84×` slower) |
| **Workload B (Read Mostly)** | 95% Read / 5% Update | **`179,524 ops/s`** | `159,857 ops/s` | `0.89×` |
| **Workload C (Read Only)** | 100% Point Read | **`200,750 ops/s`** | `186,639 ops/s` | `0.93×` |
| **Workload D (Read Latest)** | 95% Read Latest / 5% Insert | `103,146 ops/s` | **`174,697 ops/s`** | **`1.69×` faster** |
| **Workload E (Short Ranges)** | 95% Scan (10 rows) / 5% Insert | **`42,340 ops/s`** | `24,030 ops/s` | `0.57×` (`1.76×` slower) |
| **Workload F (Read-Modify-Write)** | 50% Read / 50% RMW | **`122,839 ops/s`** | `70,536 ops/s` | `0.57×` (`1.74×` slower) |

### Architectural Analysis of the `YCSB`-Style Results
1. **Inline Storage Advantage Across All 6 `YCSB`-Style Workloads (`256 B`, `1.52×` to `4.38×` Faster):**
   * For `256 B` records stored inline, `aydb` outperforms Vanilla SQLite across all six workloads (`A–F`), reaching **`1,248,252 ops/s`** (`4.38×` faster) on point reads (`Workload C`), **`895,271 ops/s`** (`3.35×` faster) on read-mostly (`Workload B`), **`928,143 ops/s`** (`4.20×` faster) on read-latest (`Workload D`), and **`265,758 ops/s`** (`2.55×` faster) on short range scans (`Workload E`).
2. **Why `2,048 B` WiscKey `UPDATE`s (`Workload A` & `F`, `0.54×`–`0.57×`) Trail In-Cache SQLite, While `Workload D` (`1.69×`) Wins:**
   * **Virtual Table `UPDATE` Protocol on Separated Blobs (`Workload A` `0.54×`, `Workload F` `0.57×`):** With 4,000 × 2 KB rows (~8 MB), hot Zipfian keys reside inside SQLite's 2 MB in-process user-space `PCache`, so SQLite mutates B-Tree pages in-place in user-space RAM. By contrast, SQLite's Virtual Table `UPDATE` protocol MUST execute `xFilter` first—which reads the old 2 KB blob via a positional `ReadFile` syscall from `blob_std_*.db`—followed by `xRowid`/`xColumn`, and then appends a new 2 KB blob via `WriteFile` in `xUpdate`.
   * **Near-Parity Point Reads (`Workload C` `0.93×`) & `Workload D` Win (`1.69×`):** On pure point lookups (`Workload C`), `aydb` reaches `186.6K ops/s` vs. SQLite's `200.8K ops/s`, reading directly into the cursor slab without intermediate buffer copies (`PERF-06`). On `Workload D` (95% Read Latest / 5% Insert), `aydb` is **`1.69×` faster** (`174,697` vs. `103,146 ops/s`) because new inserts on a SQLite `TEXT PRIMARY KEY` table must allocate a new `rowid` and insert into both the main table B-Tree and `sqlite_autoindex`, whereas `aydb` appends the blob once and inserts a single 23-byte `BlobPtr` into `MemTable`.
3. **The Classic WiscKey Range-Scan Trade-off (`Workload E` at `256 B` vs. `2,048 B`):**
   * At **`256 B` (Inline)**, values live inside the sorted index (`MemTable` / `idx`), so scanning 10 consecutive keys in `Workload E` requires **zero blob dereferences**, and `aydb` beats SQLite by **`2.55×`** (`265,758 ops/s` vs. `104,369 ops/s`).
   * At **`2,048 B` (WiscKey Separated)**, values live in `blob_std_<SeqID>.db`. When `Workload E` scans 10 consecutive keys (`WHERE ycsb_key >= ? ORDER BY ycsb_key ASC LIMIT 10`) that were updated at different times, `aydb` must issue **10 separate positional `ReadFile` blob dereferences** per scan (`24,030 ops/s` vs. `42,340 ops/s`, `0.57×`).

---

## 3. Suite 3: `TPC-B`-Style (`pgbench`-Like) 4-Table OLTP Workload

Each `TPC-B`-style transaction executes `BEGIN` + 5 parameterized SQL statements with true `UPDATE`s across 4 tables (`UPDATE accounts`, `SELECT abalance`, `UPDATE tellers`, `UPDATE branches`, `INSERT INTO history`) + `COMMIT`.

| Durability Mode | Vanilla SQLite (`WAL`) | `aydb` Virtual Table (`4 MB` Default / `128 KB` SSTable) | Speedup (`4 MB` / `SSTable`) |
| :--- | ---: | ---: | ---: |
| **Buffered (`synchronous = NORMAL` vs. `wal_buf.db` `ImmediateOsCache`)** | `14,896 TPS` (`15,106` SST) | **`102,792 TPS`** (SSTable: **`42,723 TPS`**) | **`6.90×` (`2.83×` SSTable)** |
| **Full Sync (`synchronous = FULL` vs. `wal_sync.db` + `sync_barrier()`)** | `494 TPS` (`497` SST) | **`12,864 TPS`** (SSTable: **`10,572 TPS`**) | **`26.04×` (`21.28×` SSTable)** |

* **Why Transaction Coalescing (`xBegin` $\rightarrow$ `xSync` $\rightarrow$ `xCommit`) Shines in `TPC-B`-Style OLTP:**
  By coalescing the 4 table mutations (`accounts`, `tellers`, `branches`, `history`) inside `StorageEngine::begin_transaction()` / `prepare_commit_transaction()` / `commit_transaction()` into a **single atomic `WalBatch`** written to a single pre-zeroed 4 KB sector in `wal_sync.db` followed by `sync_barrier()`, `aydb` achieves **`12,864 TPS` (`~77.7 µs` per 4-table durable transaction)**—a **`26.04×` speedup** over Vanilla SQLite (`494 TPS`), which must write and `FlushFileBuffers()` multiple dirty B-Tree pages across 4 different B-Trees on every commit!

---

## 4. On-Disk SSTable Flush Evaluation (`memtable_capacity_bytes = 128 KB`, `BENCH-01`, `BENCH-02`, `P01`)

With the default `memtable_capacity_bytes = 4 MB`, the index footprints of `Speedtest1`-style (~600 KB), `YCSB`-style (~100–200 KB), and `TPC-B`-style (~300 KB) fit entirely within `MemTable`. To evaluate on-disk `idx_<SeqID>.db` SSTable lookup, per-segment `[min_key, max_key]` pruning (`PERF-02`), size-tiered automatic compaction (`compact_auto_tiered`, `P01`, `auto_compact_segment_threshold = 8`), and multi-segment merge performance directly (`Suite 4` of `examples/standard_sql_bench.rs`), we run the suite with **`memtable_capacity_bytes = 128 KB`**, forcing SSTable flushes after initial loading and during write-heavy phases (while keeping OS file cache warm):

| Benchmark Workload (`memtable_capacity_bytes = 128 KB`) | Vanilla SQLite (`WAL`, `2 MB` User-Space `PCache`) | `aydb` Virtual Table (On-Disk `idx_*.db` SSTables) | Relative Speed (`aydb` / SQLite) |
| :--- | ---: | ---: | ---: |
| **`Speedtest1`-Style Total (`Tests 100–240`)** | `63.08 ms` | `234.51 ms` | `0.27×` |
| **`YCSB`-Style `256 B` Workload C (100% Point Read)** | `306,082 ops/s` | **`433,783 ops/s`** | **`1.42×` faster** |
| **`YCSB`-Style `256 B` Workload B (95% Read / 5% Update)** | `296,432 ops/s` | **`347,321 ops/s`** | **`1.17×` faster** |
| **`TPC-B`-Style Buffered (`wal_buf.db` vs. `NORMAL`)** | `15,106 TPS` | **`42,723 TPS`** | **`2.83×` faster** |
| **`TPC-B`-Style Full Sync (`wal_sync.db` vs. `FULL`)** | `497 TPS` | **`10,572 TPS`** | **`21.28×` faster** |

### Architectural Analysis of On-Disk SSTable Performance
1. **Still `1.17×`–`21.28×` Faster on `YCSB`-Style Point Reads & `TPC-B`-Style OLTP Even From On-Disk `idx_*.db` SSTables:**
   * Even when `memtable_capacity_bytes = 128 KB` forces keys out of RAM into on-disk `idx_<SeqID>.db` SSTables, `aydb`'s in-heap **10-bit/key Bloom Filters**, **per-segment `[min_key, max_key]` bounds (`PERF-02`)**, **Size-Tiered Suffix Compaction (`P01`)**, and **Sparse Fence Pointers** pinpoint the exact 4 KB SSTable block in a single positional `ReadFile(OVERLAPPED)` call—achieving **`433,783 ops/s` (`1.42×` faster than SQLite)** on `YCSB-Style Workload C`, **`347,321 ops/s` (`1.17×` faster)** on `YCSB-Style Workload B`, **`42,723 TPS` (`2.83×` faster)** on Buffered `TPC-B-Style`, and **`10,572 TPS` (`21.28×` faster)** on Full-Sync `TPC-B-Style`.
2. **Why `Speedtest1`-Style Total Drops to `0.27×` at `128 KB` `MemTable` Capacity (OS Page Cache Syscall Overhead vs. In-Process `PCache`):**
   * Because `aydb` strictly bounds its process heap and delegates 4 KB data block caching to the **OS Page Cache** via `ReadFile(OVERLAPPED)` + per-block hardware `CRC32C` verification—rather than duplicating a `2 MB` user-space page cache (`PCache`) inside the process like Vanilla SQLite—every SSTable block read crosses the user/kernel boundary (`ReadFile` syscall) and verifies `block_crc32c`. On small datasets (~600 KB in `Speedtest1`-style) that fit 100% inside SQLite's `2 MB` in-process `PCache` with zero syscalls, forcing `aydb` to spill into multiple `128 KB` on-disk SSTable segments incurs kernel syscall and multi-segment K-way merge overhead during `Speedtest1`'s 10× full-table scans and 5,000-row `INSERT` PK uniqueness probes (`AUD-03`).
