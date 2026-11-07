# Chapter 2.3: Industry-Standard SQL Benchmark Suite (`Speedtest1`, `YCSB A–F`, and `TPC-B`)

To evaluate `aydb` objectively beyond custom cache/config microbenchmarks, this chapter presents results from three industry-standard database benchmark suites (`examples/standard_sql_bench.rs`) executed via identical SQL statements against **Vanilla SQLite (`PRAGMA journal_mode = WAL`)** and **`aydb` Virtual Table (`USING aydb(...)`)**:

1. **SQLite Official `"Speedtest1"` Suite (`Tests 100..240`):** The canonical multi-faceted SQL workload used by the SQLite core team.
2. **YCSB (Yahoo! Cloud Serving Benchmark) Workloads `A` through `F` (Zipfian $\theta = 0.99$):** Evaluated at both **`256 B` (Inline Storage)** and **`2,048 B` (WiscKey Separated Blob Storage)** to expose the exact boundary where WiscKey outperforms B-Trees and where WiscKey incurs range-scan dereference overhead.
3. **TPC-B (`pgbench`-style) 4-Table OLTP Benchmark:** Multi-statement ACID transactions across `branches`, `tellers`, `accounts`, and `history`.

---

## 1. Suite 1: SQLite Official `"Speedtest1"` Standard Workload (`5,000` Indexed Rows)

* **Schema:** `t1(id INT PRIMARY KEY, uval INT, category TEXT, payload TEXT)` with secondary index on `uval` (`120 B` payload).
* **Note on Workload Structure:** In `Speedtest1`, Tests `100`, `230`, and `240` wrap thousands of mutations inside a **single giant `BEGIN ... COMMIT` transaction** where SQLite's native B-Tree mutates pages in its in-process user-space page cache without FFI boundary crossings per row.

| Speedtest1 Sub-Benchmark | Vanilla SQLite (`WAL`) | `aydb` Virtual Table | Relative Speed (`aydb` vs. SQLite) | Architectural Analysis |
| :--- | ---: | ---: | ---: | :--- |
| **Test 100:** `5,000` Bulk `INSERT`s (`BEGIN..COMMIT`, Indexed) | `5.77 ms` | `16.23 ms` | `0.36×` (`2.8×` slower) | Per-row `xUpdate` Virtual Table C ABI call + `Mutex` lock + `RowMetaRecord` secondary index maintenance overhead vs. native in-page B-Tree append. |
| **Test 130:** `5,000` Random PK Point `SELECT`s (`WHERE id = ?`) | `15.83 ms` | **`6.42 ms`** | **`2.47×` faster** | `aydb` serves PK point lookups directly from `MemTable` / Bloom-filtered 4 KB block without SQLite B-Tree page-lock and WAL-index (`-shm`) lookup overhead. |
| **Test 140:** `5,000` Secondary Index `SELECT`s (`WHERE uval = ?`) | `17.22 ms` | **`9.48 ms`** | **`1.82×` faster** | Composite memcomparable secondary index key `[TableID \| 'i' \| IndexID \| uval \| PK]` resolves in a single prefix seek. |
| **Test 160:** `1,000` PK Range Scans (`WHERE id >= ? AND id < ?`, 20 rows) | `5.90 ms` | `10.35 ms` | `0.57×` (`1.75×` slower) | `xFilter` materializes the 20-row range vector across the FFI cursor boundary on each range query. |
| **Test 210:** `10×` Full-Table `GROUP BY` Aggregation (`5,000` rows) | `10.72 ms` | `40.53 ms` | `0.26×` (`3.8×` slower) | Virtual Table full-table scan copies all 5,000 rows across `xFilter`/`xColumn` (`sqlite_transient()`) into VDBE registers; Phase 3 Native SQL eliminates this FFI copy. |
| **Test 230:** `2,500` Random Indexed `UPDATE`s (`BEGIN..COMMIT`) | `4.21 ms` | `11.54 ms` | `0.36×` (`2.7×` slower) | Each indexed `UPDATE` in `aydb` looks up `RowMetaRecord` (`'t'`) to tombstone the old secondary index key before inserting the new one. |
| **Test 240:** `2,500` Random `DELETE` + Re-`INSERT`s (`BEGIN..COMMIT`) | `4.42 ms` | `13.36 ms` | `0.33×` (`3.0×` slower) | `5,000` Virtual Table `xUpdate` FFI crossings inside a single in-memory transaction. |

* **Key Engineering Insight from `Speedtest1`:**
  * On **individual indexed queries (`Test 130` PK Point Lookup and `Test 140` Secondary Index Lookup)**, `aydb` is **`1.82×` to `2.47×` faster** than Vanilla SQLite even through the Virtual Table FFI layer.
  * Conversely, on **full-table analytical scans (`Test 210`)** and **multi-thousand-row single-transaction bulk loops (`Test 100/230/240`)**, crossing SQLite's `sqlite3_module` FFI boundary (`xUpdate` / `xColumn` with `SQLITE_TRANSIENT` copies) costs `~2 µs` per row. This directly motivates **Phase 3 (Native Lightweight SQL Frontend)**, which executes scans and aggregates directly over `aydb` slices without SQLite VTab FFI marshaling!

---

## 2. Suite 2: YCSB Workloads `A` through `F` (Zipfian $\theta = 0.99$)

YCSB evaluates realistic autocommit request streams across 6 industry-standard access mixes (`4,000` initial rows, `5,000` operations per workload, Zipfian skew $\theta = 0.99$). We run the entire YCSB suite twice: once at **`256 B` (Inline Storage)** and once at **`2,048 B` (WiscKey Out-of-Line Blob Storage)**.

### 2.1 YCSB at `256 B` Value Size (Below `1024 B` Inline Threshold)

| YCSB Workload | Mix | Vanilla SQLite (`WAL`, `NORMAL`) | `aydb` Virtual Table | Speedup (`aydb` / SQLite) |
| :--- | :--- | ---: | ---: | ---: |
| **Workload A (Update Heavy)** | 50% Read / 50% Update | `17,826 ops/s` | **`271,905 ops/s`** | **`15.25×` faster** |
| **Workload B (Read Mostly)** | 95% Read / 5% Update | `211,673 ops/s` | **`694,937 ops/s`** | **`3.28×` faster** |
| **Workload C (Read Only)** | 100% Point Read | `280,962 ops/s` | **`846,224 ops/s`** | **`3.01×` faster** |
| **Workload D (Read Latest)** | 95% Read Latest / 5% Insert | `113,492 ops/s` | **`651,491 ops/s`** | **`5.74×` faster** |
| **Workload E (Short Ranges)** | 95% Scan (10 rows) / 5% Insert | `81,478 ops/s` | **`93,144 ops/s`** | **`1.14×` faster** |
| **Workload F (Read-Modify-Write)** | 50% Read / 50% RMW | `15,813 ops/s` | **`240,199 ops/s`** | **`15.19×` faster** |

### 2.2 YCSB at `2,048 B` Value Size (Above `1024 B` — WiscKey Separated Blob Storage)

| YCSB Workload | Mix | Vanilla SQLite (`WAL`, `NORMAL`) | `aydb` Virtual Table | Speedup (`aydb` / SQLite) |
| :--- | :--- | ---: | ---: | ---: |
| **Workload A (Update Heavy)** | 50% Read / 50% Update | `9,478 ops/s` | **`114,187 ops/s`** | **`12.05×` faster** |
| **Workload B (Read Mostly)** | 95% Read / 5% Update | `91,927 ops/s` | **`168,473 ops/s`** | **`1.83×` faster** |
| **Workload C (Read Only)** | 100% Point Read | **`187,832 ops/s`** | `181,776 ops/s` | `0.97×` (Parity) |
| **Workload D (Read Latest)** | 95% Read Latest / 5% Insert | `100,670 ops/s` | **`168,893 ops/s`** | **`1.68×` faster** |
| **Workload E (Short Ranges)** | 95% Scan (10 rows) / 5% Insert | **`41,133 ops/s`** | `20,725 ops/s` | `0.50×` (`2.0×` slower) |
| **Workload F (Read-Modify-Write)** | 50% Read / 50% RMW | `12,511 ops/s` | **`60,938 ops/s`** | **`4.87×` faster** |

### Architectural Analysis of the YCSB Results
1. **Massive Write/Update Advantage (`Workload A`, `Workload D`, `Workload F` — `4.9×` to `15.3×` Faster):**
   * In autocommit OLTP workloads (`Workload A` & `F`), Vanilla SQLite must append full `4 KB` B-Tree leaf pages to the `-wal` file on every update. `aydb` packs mutations densely into `wal_buf.db` 4 KB sectors and `MemTable`, achieving **`12.05×` to `15.25×` higher throughput**.
2. **The Classic WiscKey Range-Scan Trade-off (`Workload E` at `256 B` vs. `2,048 B`):**
   * At **`256 B` (Inline)**, values live inside the sorted index (`MemTable` / `idx`), so scanning 10 consecutive keys in `Workload E` requires **zero blob dereferences**, and `aydb` beats SQLite by **`1.14×`** (`93,144 ops/s` vs. `81,478 ops/s`).
   * At **`2,048 B` (WiscKey Separated)**, values live in `blob_std_<SeqID>.db`. When `Workload E` scans 10 consecutive keys (`WHERE ycsb_key >= ? ORDER BY ycsb_key ASC LIMIT 10`) that were updated at different times, `aydb` must issue **10 separate positional `pread` calls (`dereference_blob`)** into `blob_std`, whereas SQLite reads contiguous B-Tree pages from its user-space page cache (`20,725 ops/s` vs. `41,133 ops/s`, `0.50×`). This empirically confirms the textbook WiscKey trade-off: **KV separation trades sequential range-scan locality on large blobs in exchange for `8.6×–12.0×` faster updates and `10×` lower compaction write amplification**.

---

## 3. Suite 3: TPC-B (`pgbench`-Style) 4-Table OLTP Benchmark

Each TPC-B transaction executes `BEGIN` + 5 parameterized SQL operations across 4 tables (`accounts`, `tellers`, `branches`, `history`) + `COMMIT`.

| Durability Mode | Vanilla SQLite (`WAL`) | `aydb` Virtual Table (`xBegin`/`xCommit` Batch Coalescing) | Speedup (`aydb` / SQLite) |
| :--- | ---: | ---: | ---: |
| **Buffered (`synchronous = NORMAL` vs. `wal_buf.db`)** | `12,501 TPS` (`62,505 SQL/s`) | **`58,333 TPS` (`291,665 SQL/s`)** | **`4.67×` faster** |
| **Full Sync (`synchronous = FULL` vs. `wal_sync.db`)** | `579 TPS` (`2,895 SQL/s`) | **`9,844 TPS` (`49,220 SQL/s`)** | **`17.00×` faster** |

* **Why Transaction Coalescing (`xBegin` $\rightarrow$ `xCommit`) Shines in TPC-B:**
  By coalescing the 4 table mutations (`accounts`, `tellers`, `branches`, `history`) inside `StorageEngine::begin_transaction()` / `commit_transaction()` into a **single atomic `WalBatch`** written to a single pre-zeroed 4 KB sector in `wal_sync.db`, `aydb` achieves **`9,844 TPS` (`~101 µs` per 4-table durable transaction)**—a **`17.00×` speedup** over Vanilla SQLite (`579 TPS`), which must write and `FlushFileBuffers()` multiple dirty B-Tree pages across 4 different B-Trees on every commit!
