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

* **Schema:** `t1(id INT PRIMARY KEY, uval INT, category TEXT, payload TEXT)` with secondary index on `uval` (`120 B` payload, `~600 KB` working set).
* **Durability & Scale Parity (`BENCH-01`, `BENCH-03`):** Both engines run with equivalent OS-page-cache commit durability (`PRAGMA synchronous = NORMAL` on SQLite vs. `WalStreamMode::BufferedImmediateOsCache` on `aydb` `wal_buf.db`) and full `PRIMARY KEY` uniqueness enforcement (`AUD-03`). At `5,000` rows (`~600 KB < 4 MB` default `MemTable`), `aydb` serves index reads from `MemTable` (`BTreeMap`) while SQLite serves them from its `2 MB` in-process `PCache`; see Section 4 below for on-disk `idx_*.db` SSTable performance (`memtable_capacity_bytes = 128 KB`).
* **Note on Workload Structure:** In `Speedtest1`, Tests `100`, `230`, and `240` wrap thousands of mutations inside a **single giant `BEGIN ... COMMIT` transaction** where SQLite's native B-Tree mutates pages in its in-process user-space page cache without FFI boundary crossings per row.

<p align="center">
  <img src="../assets/bench_speedtest1_breakdown.svg" alt="SQLite Official Speedtest1 Execution Time Breakdown" width="100%"/>
</p>

| Speedtest1 Sub-Benchmark (`BufferedImmediateOsCache` vs `NORMAL`) | Vanilla SQLite (`WAL`) | `aydb` Virtual Table | Relative Speed (`aydb` vs. SQLite) | Architectural Analysis |
| :--- | ---: | ---: | ---: | :--- |
| **Test 100:** `5,000` Bulk `INSERT`s (`BEGIN..COMMIT`, Indexed) | `5.75 ms` | `16.38 ms` | `0.35×` (`2.85×` slower) | Per-row `xUpdate` Virtual Table C ABI call + `Mutex` lock + `row_exists` PK uniqueness check (`AUD-03`) + `RowMetaRecord` secondary index maintenance vs. native in-page B-Tree append. |
| **Test 130:** `5,000` Random PK Point `SELECT`s (`WHERE id = ?`) | `16.75 ms` | **`5.02 ms`** | **`3.33×` faster** | Zero-allocation stack-key `get_row_into` + reusable `AydbCursor` byte slab + zero-copy `xColumn` (`project_column_zero_copy`). |
| **Test 140:** `5,000` Secondary Index `SELECT`s (`WHERE uval = ?`) | `17.01 ms` | **`7.86 ms`** | **`2.16×` faster** | Direct memcomparable prefix range iteration (`iter_range`) + zero-allocation PK slice extraction. |
| **Test 160:** `1,000` PK Range Scans (`WHERE id >= ? AND id < ?`, 20 rows) | `7.38 ms` | **`5.42 ms`** | **`1.36×` faster** | `scan_table_values_into` appends matching range slices directly into the reusable cursor byte slab without per-row `Vec<u8>` allocations. |
| **Test 210:** `10×` Full-Table `GROUP BY` Aggregation (`5,000` rows) | `11.01 ms` | `16.93 ms` | `0.65×` (`1.54×` slower) | Virtual Table full-table scan copies 5,000 rows per pass across `xFilter`/`xColumn` (`sqlite_transient()`) into VDBE registers; Phase 3 Native SQL eliminates this FFI copy. |
| **Test 230:** `2,500` Random Indexed `UPDATE`s (`BEGIN..COMMIT`) | `4.88 ms` | `11.87 ms` | `0.41×` (`2.43×` slower) | Each Virtual Table `UPDATE` executes a full `xFilter` + `xRowid` + `xColumn` + `xUpdate` cycle across the C ABI boundary plus undo-log tracking (`PERF-03`). |
| **Test 240:** `2,500` Random `DELETE` + Re-`INSERT`s (`BEGIN..COMMIT`) | `5.64 ms` | `15.50 ms` | `0.36×` (`2.75×` slower) | `5,000` Virtual Table `xUpdate` FFI crossings + PK existence checks inside a single in-memory transaction. |
| **TOTAL `Speedtest1` Suite Execution Time (`4 MB` `MemTable`)** | `68.42 ms` | `79.00 ms` | `0.87×` (`256.31 ms` / `0.27×` at `128 KB` SSTable) | `aydb` leads `1.36×–3.33×` on PK point, secondary-index, and range queries (`Tests 130, 140, 160`), while single-transaction bulk mutation loops (`Tests 100, 230, 240`) pay per-row `sqlite3_module` C ABI and PK uniqueness checks. |

### 1.1 Virtual Table & StorageEngine Hot-Path Optimizations Applied
To accelerate `Speedtest1` query and range-scan execution (`Tests 130, 140, 160` running **`1.36×–3.33×` faster than Vanilla SQLite**) and reduce Virtual Table FFI overhead, six targeted hot-path optimizations were engineered across `src/vtab/` and `src/engine/`:
1. **Zero-Copy `xColumn` Projection (`project_column_zero_copy`):** Instead of decoding a column into a heap-allocated `SqlValue::Text(String)` or `SqlValue::Blob(Vec<u8>)` and immediately dropping it after `sqlite3_result_text`, `xColumn` now indexes the $O(1)$ column offset directory and passes `&row_bytes[offset + 4 .. offset + 4 + len]` directly to `sqlite3_result_text` / `sqlite3_result_blob` with **zero Rust heap allocations**.
2. **Direct `sqlite3_value*` Row Encoding (`encode_sqlite_values_direct`):** `xUpdate` now encodes SQLite's raw `*mut *mut sqlite3_value` array directly into the binary row buffer in a single pass without allocating an intermediate `Vec<SqlValue>`.
3. **Reusable Contiguous Cursor Byte Slab (`AydbCursor::slab` & `spans`):** Rather than allocating and freeing thousands of individual `Vec<u8>` row buffers on every `xFilter` call (`50,000` allocations in `Test 210` and `20,000` in `Test 160`), `AydbCursor` now retains a single contiguous `slab: Vec<u8>` and `spans: Vec<(usize, usize)>`, populated in-place via `get_row_into` and `scan_table_values_into`.
4. **Lock-Free Reversible Integer `rowid` Mapping (`has_non_int_pks`):** Because 9-byte tagged integer primary keys (`[KEY_TAG_INTEGER | memcomparable_i64]`, `AUD-05`) map bijectively to 64-bit SQLite `rowid`s via `encode_i64_tagged_key` / `decode_i64_tagged_key`, integer-PK tables completely bypass locking and inserting into `rowid_to_pk: Mutex<RowidMap>`.
5. **Zero-Clone `MemTable` Ownership Transfer (`apply_wal_batch_owned` & `pending_tx_buf`) + $O(\text{tx})$ Undo Log (`PERF-03`):** Transaction mutations serialize directly into a reusable `pending_tx_buf: Vec<u8>` and move their owned `key: Vec<u8>` and `value: Vec<u8>` into `MemTable`'s `BTreeMap` while recording prior entries in an in-memory undo log for instant $O(\text{tx})$ rollback and `SAVEPOINT` restoration.
6. **Multi-Sector Coalesced WAL Writes (`multi_sector_buf`):** When a large transaction batch spans $K > 1$ sectors (e.g., ~250 sectors in `Test 100`), `WalStreamWriter::append_raw_batch` seals all $K$ sectors in memory and issues a **single contiguous `write_all_at` syscall** instead of $K$ separate 4 KB `WriteFile` syscalls.

---

## 2. Suite 2: YCSB Workloads `A` through `F` (Zipfian $\theta = 0.99$)

YCSB evaluates realistic autocommit request streams across 6 industry-standard access mixes (`4,000` initial rows, `5,000` operations per workload, Zipfian skew $\theta = 0.99$, using true SQL `UPDATE` statements, with `WalStreamMode::BufferedImmediateOsCache` on `aydb` vs. `PRAGMA synchronous = NORMAL` on SQLite). We run the entire YCSB suite twice: once at **`256 B` (Inline Storage, `~1.1 MB` total)** and once at **`2,048 B` (WiscKey Out-of-Line Blob Storage, `~8.2 MB` initial + `~5 MB` updates in `blob_std_*.db`)**.

<p align="center">
  <img src="../assets/bench_ycsb_throughput.svg" alt="YCSB Workloads A-F Throughput Comparison" width="100%"/>
</p>

### 2.1 YCSB at `256 B` Value Size (Below `1024 B` Inline Threshold)

| YCSB Workload | Mix | Vanilla SQLite (`WAL`, `NORMAL`) | `aydb` Virtual Table (`4 MB` Default / `128 KB` SSTable) | Speedup (`aydb` / SQLite) |
| :--- | :--- | ---: | ---: | ---: |
| **Workload A (Update Heavy)** | 50% Read / 50% Update | `210,264 ops/s` | **`272,437 ops/s`** | **`1.30×` faster** |
| **Workload B (Read Mostly)** | 95% Read / 5% Update | `270,954 ops/s` | **`862,738 ops/s`** (SSTable: **`304,620 ops/s`**) | **`3.18×` faster** (**`1.15×` SSTable**) |
| **Workload C (Read Only)** | 100% Point Read | `279,788 ops/s` | **`1,191,526 ops/s`** (SSTable: **`368,957 ops/s`**) | **`4.26×` faster** (**`1.35×` SSTable**) |
| **Workload D (Read Latest)** | 95% Read Latest / 5% Insert | `176,532 ops/s` | **`777,823 ops/s`** | **`4.41×` faster** |
| **Workload E (Short Ranges)** | 95% Scan (10 rows) / 5% Insert | `98,866 ops/s` | **`228,710 ops/s`** | **`2.31×` faster** |
| **Workload F (Read-Modify-Write)** | 50% Read / 50% RMW | `154,875 ops/s` | **`249,698 ops/s`** | **`1.61×` faster** |

### 2.2 YCSB at `2,048 B` Value Size (Above `1024 B` — WiscKey Separated Blob Storage)

| YCSB Workload | Mix | Vanilla SQLite (`WAL`, `NORMAL`) | `aydb` Virtual Table (`blob_std_*.db`) | Speedup (`aydb` / SQLite) |
| :--- | :--- | ---: | ---: | ---: |
| **Workload A (Update Heavy)** | 50% Read / 50% Update | **`143,144 ops/s`** | `79,080 ops/s` | `0.55×` (`1.81×` slower) |
| **Workload B (Read Mostly)** | 95% Read / 5% Update | **`168,804 ops/s`** | `149,648 ops/s` | `0.89×` |
| **Workload C (Read Only)** | 100% Point Read | `177,322 ops/s` | **`177,906 ops/s`** | **`1.00×` (Parity)** |
| **Workload D (Read Latest)** | 95% Read Latest / 5% Insert | `94,144 ops/s` | **`158,633 ops/s`** | **`1.69×` faster** |
| **Workload E (Short Ranges)** | 95% Scan (10 rows) / 5% Insert | **`41,615 ops/s`** | `22,340 ops/s` | `0.54×` (`1.86×` slower) |
| **Workload F (Read-Modify-Write)** | 50% Read / 50% RMW | **`111,891 ops/s`** | `67,089 ops/s` | `0.60×` (`1.67×` slower) |

### Architectural Analysis of the YCSB Results
1. **Inline Storage Advantage Across All 6 YCSB Workloads (`256 B`, `1.30×` to `4.41×` Faster):**
   * For `256 B` records stored inline, `aydb` outperforms Vanilla SQLite across all six workloads (`A–F`), reaching **`1,191,526 ops/s`** (`4.26×` faster) on point reads (`Workload C`), **`862,738 ops/s`** (`3.18×` faster) on read-mostly (`Workload B`), **`777,823 ops/s`** (`4.41×` faster) on read-latest (`Workload D`), and **`228,710 ops/s`** (`2.31×` faster) on short range scans (`Workload E`).
2. **Why `2,048 B` WiscKey `UPDATE`s (`Workload A` & `F`, `0.55×`–`0.60×`) Trail In-Cache SQLite, While `Workload D` (`1.69×`) Wins:**
   * **Virtual Table `UPDATE` Protocol on Separated Blobs (`Workload A` `0.55×`, `Workload F` `0.60×`):** With 4,000 × 2 KB rows (~8 MB), hot Zipfian keys reside inside SQLite's 2 MB in-process user-space `PCache`, so SQLite mutates B-Tree pages in-place in user-space RAM. By contrast, SQLite's Virtual Table `UPDATE` protocol MUST execute `xFilter` first—which reads the old 2 KB blob via a positional `ReadFile` syscall from `blob_std_*.db`—followed by `xRowid`/`xColumn`, and then appends a new 2 KB blob via `WriteFile` in `xUpdate`.
   * **Point-Read Parity (`Workload C` `1.00×`) & `Workload D` Win (`1.69×`):** On pure point lookups (`Workload C`), `aydb` matches SQLite (`177.9K` vs. `177.3K ops/s`), reading directly into the cursor slab without intermediate buffer copies (`PERF-06`). On `Workload D` (95% Read Latest / 5% Insert), `aydb` is **`1.69×` faster** (`158,633` vs. `94,144 ops/s`) because new inserts on a SQLite `TEXT PRIMARY KEY` table must allocate a new `rowid` and insert into both the main table B-Tree and `sqlite_autoindex`, whereas `aydb` appends the blob once and inserts a single 23-byte `BlobPtr` into `MemTable`.
3. **The Classic WiscKey Range-Scan Trade-off (`Workload E` at `256 B` vs. `2,048 B`):**
   * At **`256 B` (Inline)**, values live inside the sorted index (`MemTable` / `idx`), so scanning 10 consecutive keys in `Workload E` requires **zero blob dereferences**, and `aydb` beats SQLite by **`2.31×`** (`228,710 ops/s` vs. `98,866 ops/s`).
   * At **`2,048 B` (WiscKey Separated)**, values live in `blob_std_<SeqID>.db`. When `Workload E` scans 10 consecutive keys (`WHERE ycsb_key >= ? ORDER BY ycsb_key ASC LIMIT 10`) that were updated at different times, `aydb` must issue **10 separate positional `ReadFile` blob dereferences** per scan (`22,340 ops/s` vs. `41,615 ops/s`, `0.54×`).

---

## 3. Suite 3: TPC-B (`pgbench`-Style) 4-Table OLTP Benchmark

Each TPC-B transaction executes `BEGIN` + 5 parameterized SQL statements with true `UPDATE`s across 4 tables (`UPDATE accounts`, `SELECT abalance`, `UPDATE tellers`, `UPDATE branches`, `INSERT INTO history`) + `COMMIT`.

| Durability Mode | Vanilla SQLite (`WAL`) | `aydb` Virtual Table (`4 MB` Default / `128 KB` SSTable) | Speedup (`4 MB` / `SSTable`) |
| :--- | ---: | ---: | ---: |
| **Buffered (`synchronous = NORMAL` vs. `wal_buf.db` `ImmediateOsCache`)** | `15,277 TPS` (`15,840` SST) | **`100,276 TPS`** (SSTable: **`43,163 TPS`**) | **`6.56×` (`2.72×` SSTable)** |
| **Full Sync (`synchronous = FULL` vs. `wal_sync.db` + `sync_barrier()`)** | `578 TPS` (`574` SST) | **`10,495 TPS`** (SSTable: **`8,194 TPS`**) | **`18.17×` (`14.28×` SSTable)** |

* **Why Transaction Coalescing (`xBegin` $\rightarrow$ `xCommit`) Shines in TPC-B:**
  By coalescing the 4 table mutations (`accounts`, `tellers`, `branches`, `history`) inside `StorageEngine::begin_transaction()` / `commit_transaction()` into a **single atomic `WalBatch`** written to a single pre-zeroed 4 KB sector in `wal_sync.db` followed by `sync_barrier()`, `aydb` achieves **`10,495 TPS` (`~95.3 µs` per 4-table durable transaction)**—an **`18.17×` speedup** over Vanilla SQLite (`578 TPS`), which must write and `FlushFileBuffers()` multiple dirty B-Tree pages across 4 different B-Trees on every commit!

---

## 4. On-Disk SSTable Flush Evaluation (`memtable_capacity_bytes = 128 KB`, `BENCH-01`, `BENCH-02`)

With the default `memtable_capacity_bytes = 4 MB`, the index footprints of `Speedtest1` (~600 KB), `YCSB` (~100–200 KB), and `TPC-B` (~300 KB) fit entirely within `MemTable`. To evaluate on-disk `idx_<SeqID>.db` SSTable lookup, per-segment `[min_key, max_key]` pruning (`PERF-02`), automatic compaction (`auto_compact_segment_threshold = 8`), and multi-segment merge performance directly (`Suite 4` of `examples/standard_sql_bench.rs`), we run the suite with **`memtable_capacity_bytes = 128 KB`**, forcing SSTable flushes after initial loading and during write-heavy phases:

| Benchmark Workload (`memtable_capacity_bytes = 128 KB`) | Vanilla SQLite (`WAL`, `2 MB` User-Space `PCache`) | `aydb` Virtual Table (On-Disk `idx_*.db` SSTables) | Relative Speed (`aydb` / SQLite) |
| :--- | ---: | ---: | ---: |
| **Speedtest1 Total (`Tests 100–240`)** | `68.64 ms` | `256.31 ms` | `0.27×` |
| **YCSB `256 B` Workload C (100% Point Read)** | `272,374 ops/s` | **`368,957 ops/s`** | **`1.35×` faster** |
| **YCSB `256 B` Workload B (95% Read / 5% Update)** | `264,401 ops/s` | **`304,620 ops/s`** | **`1.15×` faster** |
| **TPC-B Buffered (`wal_buf.db` vs. `NORMAL`)** | `15,840 TPS` | **`43,163 TPS`** | **`2.72×` faster** |
| **TPC-B Full Sync (`wal_sync.db` vs. `FULL`)** | `574 TPS` | **`8,194 TPS`** | **`14.28×` faster** |

### Architectural Analysis of On-Disk SSTable Performance
1. **Still `1.15×`–`14.28×` Faster on YCSB Point Reads & TPC-B OLTP Even From On-Disk `idx_*.db` SSTables:**
   * Even when `memtable_capacity_bytes = 128 KB` forces keys out of RAM into on-disk `idx_<SeqID>.db` SSTables, `aydb`'s in-heap **10-bit/key Bloom Filters**, **per-segment `[min_key, max_key]` bounds (`PERF-02`)**, and **Sparse Fence Pointers** pinpoint the exact 4 KB SSTable block in a single positional `ReadFile(OVERLAPPED)` call—achieving **`368,957 ops/s` (`1.35×` faster than SQLite)** on `YCSB Workload C`, **`304,620 ops/s` (`1.15×` faster)** on `YCSB Workload B`, **`43,163 TPS` (`2.72×` faster)** on Buffered `TPC-B`, and **`8,194 TPS` (`14.28×` faster)** on Full-Sync `TPC-B`.
2. **Why `Speedtest1` Drops to `0.27×` at `128 KB` `MemTable` Capacity (OS Page Cache Syscall Overhead vs. In-Process `PCache`):**
   * Because `aydb` strictly bounds its process heap and delegates 4 KB data block caching to the **OS Page Cache** via `ReadFile(OVERLAPPED)` + per-block hardware `CRC32C` verification—rather than duplicating a `2 MB` user-space page cache (`PCache`) inside the process like Vanilla SQLite—every SSTable block read crosses the user/kernel boundary (`ReadFile` syscall) and verifies `block_crc32c`. On small datasets (~600 KB in `Speedtest1`) that fit 100% inside SQLite's `2 MB` in-process `PCache` with zero syscalls, forcing `aydb` to spill into multiple `128 KB` on-disk SSTable segments incurs kernel syscall and multi-segment K-way merge overhead during `Speedtest1`'s 10× full-table scans and 5,000-row `INSERT` PK uniqueness probes (`AUD-03`).
