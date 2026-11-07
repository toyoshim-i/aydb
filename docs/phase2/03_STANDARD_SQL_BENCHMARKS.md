# Chapter 2.3: Industry-Standard SQL Benchmark Suite (`Speedtest1`, `YCSB A–F`, and `TPC-B`)

To evaluate `aydb` objectively beyond custom cache/config microbenchmarks, this chapter presents results from three industry-standard database benchmark suites (`examples/standard_sql_bench.rs`) executed via identical SQL statements against **Vanilla SQLite (`PRAGMA journal_mode = WAL`)** and **`aydb` Virtual Table (`USING aydb(...)`)**:

1. **SQLite Official `"Speedtest1"` Suite (`Tests 100..240`):** The canonical multi-faceted SQL workload used by the SQLite core team.
2. **YCSB (Yahoo! Cloud Serving Benchmark) Workloads `A` through `F` (Zipfian $\theta = 0.99$):** Evaluated at both **`256 B` (Inline Storage)** and **`2,048 B` (WiscKey Separated Blob Storage)** to expose the exact boundary where WiscKey outperforms B-Trees and where WiscKey incurs range-scan dereference overhead.
3. **TPC-B (`pgbench`-style) 4-Table OLTP Benchmark:** Multi-statement ACID transactions across `branches`, `tellers`, `accounts`, and `history`.

<p align="center">
  <img src="../assets/bench_speedup_overview.svg" alt="aydb Relative Speedup Overview" width="100%"/>
</p>

---

## 1. Suite 1: SQLite Official `"Speedtest1"` Standard Workload (`5,000` Indexed Rows)

* **Schema:** `t1(id INT PRIMARY KEY, uval INT, category TEXT, payload TEXT)` with secondary index on `uval` (`120 B` payload).
* **Note on Workload Structure:** In `Speedtest1`, Tests `100`, `230`, and `240` wrap thousands of mutations inside a **single giant `BEGIN ... COMMIT` transaction** where SQLite's native B-Tree mutates pages in its in-process user-space page cache without FFI boundary crossings per row.

<p align="center">
  <img src="../assets/bench_speedtest1_breakdown.svg" alt="SQLite Official Speedtest1 Execution Time Breakdown" width="100%"/>
</p>

| Speedtest1 Sub-Benchmark | Vanilla SQLite (`WAL`) | `aydb` Virtual Table | Relative Speed (`aydb` vs. SQLite) | Architectural Analysis |
| :--- | ---: | ---: | ---: | :--- |
| **Test 100:** `5,000` Bulk `INSERT`s (`BEGIN..COMMIT`, Indexed) | `6.43 ms` | `9.88 ms` | `0.65×` (`1.54×` slower) | Per-row `xUpdate` Virtual Table C ABI call + `Mutex` lock + `RowMetaRecord` secondary index maintenance vs. native in-page B-Tree append. |
| **Test 130:** `5,000` Random PK Point `SELECT`s (`WHERE id = ?`) | `16.54 ms` | **`4.92 ms`** | **`3.37×` faster** | Zero-allocation stack-key `get_row_into` + reusable `AydbCursor` byte slab + zero-copy `xColumn` (`project_column_zero_copy`). |
| **Test 140:** `5,000` Secondary Index `SELECT`s (`WHERE uval = ?`) | `18.65 ms` | **`7.31 ms`** | **`2.55×` faster** | Direct memcomparable prefix range iteration (`iter_range`) + zero-allocation PK slice extraction. |
| **Test 160:** `1,000` PK Range Scans (`WHERE id >= ? AND id < ?`, 20 rows) | `6.04 ms` | **`4.15 ms`** | **`1.46×` faster** | `scan_table_values_into` appends matching range slices directly into the reusable cursor byte slab without per-row `Vec<u8>` allocations. |
| **Test 210:** `10×` Full-Table `GROUP BY` Aggregation (`5,000` rows) | `11.88 ms` | `19.03 ms` | `0.62×` (`1.60×` slower) | Virtual Table full-table scan copies 5,000 rows per pass across `xFilter`/`xColumn` (`sqlite_transient()`) into VDBE registers; Phase 3 Native SQL eliminates this FFI copy. |
| **Test 230:** `2,500` Random Indexed `UPDATE`s (`BEGIN..COMMIT`) | `5.08 ms` | `8.14 ms` | `0.62×` (`1.60×` slower) | Each Virtual Table `UPDATE` executes a full `xFilter` + `xRowid` + `xColumn` + `xUpdate` cycle across the C ABI boundary. |
| **Test 240:** `2,500` Random `DELETE` + Re-`INSERT`s (`BEGIN..COMMIT`) | `5.45 ms` | `9.70 ms` | `0.56×` (`1.78×` slower) | `5,000` Virtual Table `xUpdate` FFI crossings inside a single in-memory transaction. |
| **TOTAL `Speedtest1` Suite Execution Time** | `70.06 ms` | **`63.12 ms`** | **`1.11×` faster overall** | `aydb`'s `1.46×–3.37×` query & range-scan lead outweighs the Virtual Table FFI overhead on single-transaction bulk loops. |

### 1.1 Virtual Table & StorageEngine Hot-Path Optimizations Applied
To bring `aydb` ahead of Vanilla SQLite on total `Speedtest1` execution time (`93.36 ms` $\rightarrow$ **`63.12 ms`**, a **`1.11×` overall speedup vs. SQLite**) and flip `Test 160` (PK Range Scans) from a `0.57×` loss to a **`1.46×` win**, six targeted hot-path optimizations were engineered across `src/vtab/` and `src/engine/`:
1. **Zero-Copy `xColumn` Projection (`project_column_zero_copy`):** Instead of decoding a column into a heap-allocated `SqlValue::Text(String)` or `SqlValue::Blob(Vec<u8>)` and immediately dropping it after `sqlite3_result_text`, `xColumn` now indexes the $O(1)$ column offset directory and passes `&row_bytes[offset + 4 .. offset + 4 + len]` directly to `sqlite3_result_text` / `sqlite3_result_blob` with **zero Rust heap allocations**.
2. **Direct `sqlite3_value*` Row Encoding (`encode_sqlite_values_direct`):** `xUpdate` now encodes SQLite's raw `*mut *mut sqlite3_value` array directly into the binary row buffer in a single pass without allocating an intermediate `Vec<SqlValue>`.
3. **Reusable Contiguous Cursor Byte Slab (`AydbCursor::slab` & `spans`):** Rather than allocating and freeing thousands of individual `Vec<u8>` row buffers on every `xFilter` call (`50,000` allocations in `Test 210` and `20,000` in `Test 160`), `AydbCursor` now retains a single contiguous `slab: Vec<u8>` and `spans: Vec<(u32, u32)>`, populated in-place via `get_row_into` and `scan_table_values_into`.
4. **Lock-Free Reversible Integer `rowid` Mapping (`has_non_int_pks`):** Because 8-byte memcomparable `i64` primary keys map bijectively to 64-bit SQLite `rowid`s via `encode_i64_memcomparable` / `decode_i64_memcomparable`, integer-PK tables completely bypass locking and inserting into `rowid_to_pk: Mutex<HashMap<i64, Vec<u8>>>`.
5. **Zero-Clone `MemTable` Ownership Transfer (`apply_wal_batch_owned` & `pending_tx_buf`):** Transaction mutations serialize directly into a reusable `pending_tx_buf: Vec<u8>` and then move their owned `key: Vec<u8>` and `value: Vec<u8>` directly into `MemTable`'s `BTreeMap` without cloning.
6. **Multi-Sector Coalesced WAL Writes (`multi_sector_buf`):** When a large transaction batch spans $K > 1$ sectors (e.g., ~250 sectors in `Test 100`), `WalStreamWriter::append_raw_batch` seals all $K$ sectors in memory and issues a **single contiguous `write_all_at` syscall** instead of $K$ separate 4 KB `WriteFile` syscalls.

---

## 2. Suite 2: YCSB Workloads `A` through `F` (Zipfian $\theta = 0.99$)

YCSB evaluates realistic autocommit request streams across 6 industry-standard access mixes (`4,000` initial rows, `5,000` operations per workload, Zipfian skew $\theta = 0.99$, using true SQL `UPDATE` statements). We run the entire YCSB suite twice: once at **`256 B` (Inline Storage)** and once at **`2,048 B` (WiscKey Out-of-Line Blob Storage)**.

<p align="center">
  <img src="../assets/bench_ycsb_throughput.svg" alt="YCSB Workloads A-F Throughput Comparison" width="100%"/>
</p>

### 2.1 YCSB at `256 B` Value Size (Below `1024 B` Inline Threshold)

| YCSB Workload | Mix | Vanilla SQLite (`WAL`, `NORMAL`) | `aydb` Virtual Table | Speedup (`aydb` / SQLite) |
| :--- | :--- | ---: | ---: | ---: |
| **Workload A (Update Heavy)** | 50% Read / 50% Update | `209,123 ops/s` | **`278,034 ops/s`** | **`1.33×` faster** |
| **Workload B (Read Mostly)** | 95% Read / 5% Update | `258,893 ops/s` | **`871,657 ops/s`** | **`3.37×` faster** |
| **Workload C (Read Only)** | 100% Point Read | `274,894 ops/s` | **`1,216,131 ops/s`** | **`4.42×` faster** |
| **Workload D (Read Latest)** | 95% Read Latest / 5% Insert | `191,456 ops/s` | **`747,529 ops/s`** | **`3.90×` faster** |
| **Workload E (Short Ranges)** | 95% Scan (10 rows) / 5% Insert | `96,550 ops/s` | **`224,617 ops/s`** | **`2.33×` faster** |
| **Workload F (Read-Modify-Write)** | 50% Read / 50% RMW | `153,916 ops/s` | **`240,426 ops/s`** | **`1.56×` faster** |

### 2.2 YCSB at `2,048 B` Value Size (Above `1024 B` — WiscKey Separated Blob Storage)

| YCSB Workload | Mix | Vanilla SQLite (`WAL`, `NORMAL`) | `aydb` Virtual Table | Speedup (`aydb` / SQLite) |
| :--- | :--- | ---: | ---: | ---: |
| **Workload A (Update Heavy)** | 50% Read / 50% Update | **`133,914 ops/s`** | `81,792 ops/s` | `0.61×` (`1.64×` slower) |
| **Workload B (Read Mostly)** | 95% Read / 5% Update | **`152,681 ops/s`** | `148,423 ops/s` | `0.97×` (Parity) |
| **Workload C (Read Only)** | 100% Point Read | **`171,721 ops/s`** | `171,326 ops/s` | `1.00×` (Parity) |
| **Workload D (Read Latest)** | 95% Read Latest / 5% Insert | `94,345 ops/s` | **`162,460 ops/s`** | **`1.72×` faster** |
| **Workload E (Short Ranges)** | 95% Scan (10 rows) / 5% Insert | **`40,577 ops/s`** | `20,615 ops/s` | `0.51×` (`1.97×` slower) |
| **Workload F (Read-Modify-Write)** | 50% Read / 50% RMW | **`106,550 ops/s`** | `67,204 ops/s` | `0.63×` (`1.59×` slower) |

### Architectural Analysis of the YCSB Results
1. **Inline Storage Advantage Across All 6 YCSB Workloads (`256 B`, `1.33×` to `4.42×` Faster):**
   * For `256 B` records stored inline, `aydb` outperforms Vanilla SQLite across all six workloads (`A–F`), reaching **`1,216,131 ops/s`** (`4.42×` faster) on point reads (`Workload C`), **`871,657 ops/s`** (`3.37×` faster) on read-mostly (`Workload B`), **`747,529 ops/s`** (`3.90×` faster) on read-latest (`Workload D`), and **`224,617 ops/s`** (`2.33×` faster) on short range scans (`Workload E`).
2. **Why `2,048 B` WiscKey `UPDATE`s (`Workload A` & `F`, `0.61×`–`0.63×`) Trail In-Cache SQLite, While `Workload D` (`1.72×`) Wins:**
   * **Virtual Table `UPDATE` Protocol on Separated Blobs (`Workload A` `0.61×`, `Workload F` `0.63×`):** With 4,000 × 2 KB rows (~8 MB), hot Zipfian keys reside inside SQLite's 2 MB in-process user-space `PCache`, so SQLite mutates B-Tree pages in-place in user-space RAM. By contrast, SQLite's Virtual Table `UPDATE` protocol MUST execute `xFilter` first—which reads the old 2 KB blob via a positional `ReadFile` syscall from `blob_std_*.db`—followed by `xRowid`/`xColumn`, and then appends a new 2 KB blob via `WriteFile` in `xUpdate`.
   * **Point-Read Parity (`Workload B` `0.97×`, `Workload C` `1.00×`) & `Workload D` Win (`1.72×`):** On pure/read-mostly point lookups (`B` and `C`), `aydb` matches SQLite (`171.3K` vs. `171.7K ops/s`). On `Workload D` (95% Read Latest / 5% Insert), `aydb` is **`1.72×` faster** (`162,460` vs. `94,345 ops/s`) because new inserts on a SQLite `TEXT PRIMARY KEY` table must allocate a new `rowid` and insert into both the main table B-Tree and `sqlite_autoindex`, whereas `aydb` appends the blob once and inserts a single 23-byte `BlobPtr` into `MemTable`.
3. **The Classic WiscKey Range-Scan Trade-off (`Workload E` at `256 B` vs. `2,048 B`):**
   * At **`256 B` (Inline)**, values live inside the sorted index (`MemTable` / `idx`), so scanning 10 consecutive keys in `Workload E` requires **zero blob dereferences**, and `aydb` beats SQLite by **`2.33×`** (`224,617 ops/s` vs. `96,550 ops/s`).
   * At **`2,048 B` (WiscKey Separated)**, values live in `blob_std_<SeqID>.db`. When `Workload E` scans 10 consecutive keys (`WHERE ycsb_key >= ? ORDER BY ycsb_key ASC LIMIT 10`) that were updated at different times, `aydb` must issue **10 separate positional `ReadFile` blob dereferences** per scan (`20,615 ops/s` vs. `40,577 ops/s`, `0.51×`).

---

## 3. Suite 3: TPC-B (`pgbench`-Style) 4-Table OLTP Benchmark

Each TPC-B transaction executes `BEGIN` + 5 parameterized SQL statements with true `UPDATE`s across 4 tables (`UPDATE accounts`, `SELECT abalance`, `UPDATE tellers`, `UPDATE branches`, `INSERT INTO history`) + `COMMIT`.

| Durability Mode | Vanilla SQLite (`WAL`) | `aydb` Virtual Table (`xBegin`/`xCommit` Batch Coalescing) | Speedup (`aydb` / SQLite) |
| :--- | ---: | ---: | ---: |
| **Buffered (`synchronous = NORMAL` vs. `wal_buf.db`)** | `15,604 TPS` (`78,020 SQL/s`) | **`112,610 TPS` (`563,050 SQL/s`)** | **`7.22×` faster** |
| **Full Sync (`synchronous = FULL` vs. `wal_sync.db` + `sync_barrier()`)** | `566 TPS` (`2,830 SQL/s`) | **`10,668 TPS` (`53,340 SQL/s`)** | **`18.85×` faster** |

* **Why Transaction Coalescing (`xBegin` $\rightarrow$ `xCommit`) Shines in TPC-B:**
  By coalescing the 4 table mutations (`accounts`, `tellers`, `branches`, `history`) inside `StorageEngine::begin_transaction()` / `commit_transaction()` into a **single atomic `WalBatch`** written to a single pre-zeroed 4 KB sector in `wal_sync.db` followed by `sync_barrier()`, `aydb` achieves **`10,668 TPS` (`~93.7 µs` per 4-table durable transaction)**—an **`18.85×` speedup** over Vanilla SQLite (`566 TPS`), which must write and `FlushFileBuffers()` multiple dirty B-Tree pages across 4 different B-Trees on every commit!

---

## 4. On-Disk SSTable Flush Evaluation (`memtable_capacity_bytes = 128 KB`, `BENCH-01`)

With the default `memtable_capacity_bytes = 4 MB`, the index footprints of `Speedtest1` (~600 KB), `YCSB` (~100–200 KB), and `TPC-B` (~300 KB) fit entirely within `MemTable`. To evaluate on-disk `idx_<SeqID>.db` SSTable lookup and multi-segment merge performance directly (`BENCH-01`), we also run the suite with **`memtable_capacity_bytes = 128 KB`**, forcing SSTable flushes after initial loading and during write-heavy phases:

| Benchmark Workload (`memtable_capacity_bytes = 128 KB`) | Vanilla SQLite (`WAL`, `2 MB` User-Space `PCache`) | `aydb` Virtual Table (On-Disk `idx_*.db` SSTables) | Relative Speed (`aydb` / SQLite) |
| :--- | ---: | ---: | ---: |
| **Speedtest1 Total (`Tests 100–240`)** | `70.37 ms` | `228.73 ms` | `0.31×` |
| **YCSB `256 B` Workload C (100% Point Read)** | `266,761 ops/s` | **`381,636 ops/s`** | **`1.43×` faster** |
| **YCSB `256 B` Workload B (95% Read / 5% Update)** | `234,402 ops/s` | **`315,292 ops/s`** | **`1.35×` faster** |
| **TPC-B Buffered (`wal_buf.db` vs. `NORMAL`)** | `19,986 TPS` | **`48,683 TPS`** | **`2.44×` faster** |
| **TPC-B Full Sync (`wal_sync.db` vs. `FULL`)** | `1,098 TPS` | **`10,253 TPS`** | **`9.34×` faster** |

### Architectural Analysis of On-Disk SSTable Performance
1. **Still `1.35×`–`9.34×` Faster on YCSB Point Reads & TPC-B OLTP Even From On-Disk `idx_*.db` SSTables:**
   * Even when `memtable_capacity_bytes = 128 KB` forces keys out of RAM into on-disk `idx_<SeqID>.db` SSTables, `aydb`'s in-heap **10-bit/key Bloom Filters** and **Sparse Fence Pointers** pinpoint the exact 4 KB SSTable block in a single positional `ReadFile(OVERLAPPED)` call—achieving **`381,636 ops/s` (`1.43×` faster than SQLite)** on `YCSB Workload C`, **`315,292 ops/s` (`1.35×` faster)** on `YCSB Workload B`, **`48,683 TPS` (`2.44×` faster)** on Buffered `TPC-B`, and **`10,253 TPS` (`9.34×` faster)** on Full-Sync `TPC-B`.
2. **Why `Speedtest1` Drops to `0.31×` at `128 KB` `MemTable` Capacity (OS Page Cache Syscall Overhead vs. In-Process `PCache`):**
   * Because `aydb` strictly bounds its process heap and delegates 4 KB data block caching to the **OS Page Cache** via `ReadFile(OVERLAPPED)` + per-block hardware `CRC32C` verification—rather than duplicating a `2 MB` user-space page cache (`PCache`) inside the process like Vanilla SQLite—every SSTable block read crosses the user/kernel boundary (`ReadFile` syscall) and verifies `block_crc32c`. On small datasets (~600 KB in `Speedtest1`) that fit 100% inside SQLite's `2 MB` in-process `PCache` with zero syscalls, forcing `aydb` to spill into multiple `128 KB` on-disk SSTable segments incurs kernel syscall and multi-segment K-way merge overhead during `Speedtest1`'s 10× full-table scans and indexed loops.
