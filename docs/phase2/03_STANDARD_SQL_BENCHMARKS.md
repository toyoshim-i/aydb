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
| **Test 100:** `5,000` Bulk `INSERT`s (`BEGIN..COMMIT`, Indexed) | `6.12 ms` | `11.69 ms` | `0.52×` (`1.9×` slower) | Per-row `xUpdate` Virtual Table C ABI call + `Mutex` lock + `RowMetaRecord` secondary index maintenance vs. native in-page B-Tree append. |
| **Test 130:** `5,000` Random PK Point `SELECT`s (`WHERE id = ?`) | `15.83 ms` | **`4.75 ms`** | **`3.33×` faster** | Zero-allocation stack-key `get_row_into` + reusable `AydbCursor` byte slab + zero-copy `xColumn` (`project_column_zero_copy`). |
| **Test 140:** `5,000` Secondary Index `SELECT`s (`WHERE uval = ?`) | `16.66 ms` | **`6.77 ms`** | **`2.46×` faster** | Direct memcomparable prefix range iteration (`iter_range`) + zero-allocation PK slice extraction. |
| **Test 160:** `1,000` PK Range Scans (`WHERE id >= ? AND id < ?`, 20 rows) | `5.80 ms` | **`4.22 ms`** | **`1.37×` faster** | `scan_table_values_into` appends matching range slices directly into the reusable cursor byte slab without per-row `Vec<u8>` allocations. |
| **Test 210:** `10×` Full-Table `GROUP BY` Aggregation (`5,000` rows) | `10.39 ms` | `14.35 ms` | `0.72×` (`1.38×` slower) | Virtual Table full-table scan copies 5,000 rows per pass across `xFilter`/`xColumn` (`sqlite_transient()`) into VDBE registers; Phase 3 Native SQL eliminates this FFI copy. |
| **Test 230:** `2,500` Random Indexed `UPDATE`s (`BEGIN..COMMIT`) | `4.20 ms` | `6.96 ms` | `0.60×` (`1.66×` slower) | Each Virtual Table `UPDATE` executes a full `xFilter` + `xRowid` + `xColumn` + `xUpdate` cycle across the C ABI boundary. |
| **Test 240:** `2,500` Random `DELETE` + Re-`INSERT`s (`BEGIN..COMMIT`) | `3.85 ms` | `7.55 ms` | `0.51×` (`1.96×` slower) | `5,000` Virtual Table `xUpdate` FFI crossings inside a single in-memory transaction. |
| **TOTAL `Speedtest1` Suite Execution Time** | `62.85 ms` | **`56.29 ms`** | **`1.12×` faster overall** | `aydb`'s `1.37×–3.33×` query & range-scan lead outweighs the Virtual Table FFI overhead on single-transaction bulk loops. |

### 1.1 Virtual Table & StorageEngine Hot-Path Optimizations Applied
To bring `aydb` ahead of Vanilla SQLite on total `Speedtest1` execution time (`93.36 ms` $\rightarrow$ **`56.29 ms`**, a **`1.66×` overall speedup**) and flip `Test 160` (PK Range Scans) from a `0.57×` loss to a **`1.37×` win**, six targeted hot-path optimizations were engineered across `src/vtab/` and `src/engine/`:
1. **Zero-Copy `xColumn` Projection (`project_column_zero_copy`):** Instead of decoding a column into a heap-allocated `SqlValue::Text(String)` or `SqlValue::Blob(Vec<u8>)` and immediately dropping it after `sqlite3_result_text`, `xColumn` now indexes the $O(1)$ column offset directory and passes `&row_bytes[offset + 4 .. offset + 4 + len]` directly to `sqlite3_result_text` / `sqlite3_result_blob` with **zero Rust heap allocations**.
2. **Direct `sqlite3_value*` Row Encoding (`encode_sqlite_values_direct`):** `xUpdate` now encodes SQLite's raw `*mut *mut sqlite3_value` array directly into the binary row buffer in a single pass without allocating an intermediate `Vec<SqlValue>`.
3. **Reusable Contiguous Cursor Byte Slab (`AydbCursor::slab` & `spans`):** Rather than allocating and freeing thousands of individual `Vec<u8>` row buffers on every `xFilter` call (`50,000` allocations in `Test 210` and `20,000` in `Test 160`), `AydbCursor` now retains a single contiguous `slab: Vec<u8>` and `spans: Vec<(u32, u32)>`, populated in-place via `get_row_into` and `scan_table_values_into`.
4. **Lock-Free Reversible Integer `rowid` Mapping (`has_non_int_pks`):** Because 8-byte memcomparable `i64` primary keys map bijectively to 64-bit SQLite `rowid`s via `encode_i64_memcomparable` / `decode_i64_memcomparable`, integer-PK tables completely bypass locking and inserting into `rowid_to_pk: Mutex<HashMap<i64, Vec<u8>>>`.
5. **Zero-Clone `MemTable` Ownership Transfer (`apply_wal_batch_owned` & `pending_tx_buf`):** Transaction mutations serialize directly into a reusable `pending_tx_buf: Vec<u8>` and then move their owned `key: Vec<u8>` and `value: Vec<u8>` directly into `MemTable`'s `BTreeMap` without cloning.
6. **Multi-Sector Coalesced WAL Writes (`multi_sector_buf`):** When a large transaction batch spans $K > 1$ sectors (e.g., ~250 sectors in `Test 100`), `WalStreamWriter::append_raw_batch` seals all $K$ sectors in memory and issues a **single contiguous `write_all_at` syscall** instead of $K$ separate 4 KB `WriteFile` syscalls.

---

## 2. Suite 2: YCSB Workloads `A` through `F` (Zipfian $\theta = 0.99$)

YCSB evaluates realistic autocommit request streams across 6 industry-standard access mixes (`4,000` initial rows, `5,000` operations per workload, Zipfian skew $\theta = 0.99$). We run the entire YCSB suite twice: once at **`256 B` (Inline Storage)** and once at **`2,048 B` (WiscKey Out-of-Line Blob Storage)**.

<p align="center">
  <img src="../assets/bench_ycsb_throughput.svg" alt="YCSB Workloads A-F Throughput Comparison" width="100%"/>
</p>

### 2.1 YCSB at `256 B` Value Size (Below `1024 B` Inline Threshold)

| YCSB Workload | Mix | Vanilla SQLite (`WAL`, `NORMAL`) | `aydb` Virtual Table | Speedup (`aydb` / SQLite) |
| :--- | :--- | ---: | ---: | ---: |
| **Workload A (Update Heavy)** | 50% Read / 50% Update | `30,483 ops/s` | **`323,256 ops/s`** | **`10.60×` faster** |
| **Workload B (Read Mostly)** | 95% Read / 5% Update | `212,505 ops/s` | **`902,006 ops/s`** | **`4.24×` faster** |
| **Workload C (Read Only)** | 100% Point Read | `286,410 ops/s` | **`1,233,745 ops/s`** | **`4.31×` faster** |
| **Workload D (Read Latest)** | 95% Read Latest / 5% Insert | `147,413 ops/s` | **`847,989 ops/s`** | **`5.75×` faster** |
| **Workload E (Short Ranges)** | 95% Scan (10 rows) / 5% Insert | `106,311 ops/s` | **`254,928 ops/s`** | **`2.40×` faster** |
| **Workload F (Read-Modify-Write)** | 50% Read / 50% RMW | `28,454 ops/s` | **`294,487 ops/s`** | **`10.35×` faster** |

### 2.2 YCSB at `2,048 B` Value Size (Above `1024 B` — WiscKey Separated Blob Storage)

| YCSB Workload | Mix | Vanilla SQLite (`WAL`, `NORMAL`) | `aydb` Virtual Table | Speedup (`aydb` / SQLite) |
| :--- | :--- | ---: | ---: | ---: |
| **Workload A (Update Heavy)** | 50% Read / 50% Update | `13,288 ops/s` | **`107,592 ops/s`** | **`8.10×` faster** |
| **Workload B (Read Mostly)** | 95% Read / 5% Update | `93,158 ops/s` | **`172,109 ops/s`** | **`1.85×` faster** |
| **Workload C (Read Only)** | 100% Point Read | **`195,632 ops/s`** | `190,749 ops/s` | `0.98×` (Parity) |
| **Workload D (Read Latest)** | 95% Read Latest / 5% Insert | `88,198 ops/s` | **`177,782 ops/s`** | **`2.02×` faster** |
| **Workload E (Short Ranges)** | 95% Scan (10 rows) / 5% Insert | **`32,652 ops/s`** | `23,146 ops/s` | `0.71×` (`1.4×` slower) |
| **Workload F (Read-Modify-Write)** | 50% Read / 50% RMW | `11,937 ops/s` | **`84,809 ops/s`** | **`7.10×` faster** |

### Architectural Analysis of the YCSB Results
1. **Massive Write/Update & Point-Read Advantage (`1.23M ops/s` Point Read, `7.1×` to `10.6×` Faster Updates):**
   * In autocommit OLTP workloads (`Workload A` & `F`), Vanilla SQLite must append full `4 KB` B-Tree leaf pages to the `-wal` file on every update. `aydb` packs mutations densely into `wal_buf.db` 4 KB sectors and `MemTable`, achieving **`8.10×` to `10.60×` higher throughput**, while reaching **`1,233,745 ops/s`** (`4.31×` faster than SQLite) on `256 B` point reads (`Workload C`).
2. **The Classic WiscKey Range-Scan Trade-off (`Workload E` at `256 B` vs. `2,048 B`):**
   * At **`256 B` (Inline)**, values live inside the sorted index (`MemTable` / `idx`), so scanning 10 consecutive keys in `Workload E` requires **zero blob dereferences**, and `aydb` beats SQLite by **`2.40×`** (`254,928 ops/s` vs. `106,311 ops/s`).
   * At **`2,048 B` (WiscKey Separated)**, values live in `blob_std_<SeqID>.db`. When `Workload E` scans 10 consecutive keys (`WHERE ycsb_key >= ? ORDER BY ycsb_key ASC LIMIT 10`) that were updated at different times, `aydb` must issue **10 separate positional `pread` calls (`dereference_blob`)** into `blob_std`, whereas SQLite reads contiguous B-Tree pages from its user-space page cache (`23,146 ops/s` vs. `32,652 ops/s`, `0.71×`). This empirically confirms the textbook WiscKey trade-off: **KV separation trades sequential range-scan locality on large blobs in exchange for `7.1×–8.1×` faster updates and `10×` lower compaction write amplification**.

---

## 3. Suite 3: TPC-B (`pgbench`-Style) 4-Table OLTP Benchmark

Each TPC-B transaction executes `BEGIN` + 5 parameterized SQL operations across 4 tables (`accounts`, `tellers`, `branches`, `history`) + `COMMIT`.

| Durability Mode | Vanilla SQLite (`WAL`) | `aydb` Virtual Table (`xBegin`/`xCommit` Batch Coalescing) | Speedup (`aydb` / SQLite) |
| :--- | ---: | ---: | ---: |
| **Buffered (`synchronous = NORMAL` vs. `wal_buf.db`)** | `15,567 TPS` (`77,835 SQL/s`) | **`117,368 TPS` (`586,840 SQL/s`)** | **`7.54×` faster** |
| **Full Sync (`synchronous = FULL` vs. `wal_sync.db`)** | `581 TPS` (`2,905 SQL/s`) | **`11,417 TPS` (`57,085 SQL/s`)** | **`19.65×` faster** |

* **Why Transaction Coalescing (`xBegin` $\rightarrow$ `xCommit`) Shines in TPC-B:**
  By coalescing the 4 table mutations (`accounts`, `tellers`, `branches`, `history`) inside `StorageEngine::begin_transaction()` / `commit_transaction()` into a **single atomic `WalBatch`** written to a single pre-zeroed 4 KB sector in `wal_sync.db`, `aydb` achieves **`11,417 TPS` (`~87.6 µs` per 4-table durable transaction)**—a **`19.65×` speedup** over Vanilla SQLite (`581 TPS`), which must write and `FlushFileBuffers()` multiple dirty B-Tree pages across 4 different B-Trees on every commit!
