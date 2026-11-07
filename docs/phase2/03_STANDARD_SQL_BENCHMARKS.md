# Chapter 2.3: Selected Standard SQL Benchmark Workloads (`Speedtest1`-Style, `YCSB`-Style `A–F`, and `TPC-B`-Style)

To evaluate `aydb` objectively beyond custom cache/config microbenchmarks, this chapter presents results from three representative standard database workload styles (`examples/standard_sql_bench.rs`) executed via identical SQL statements against **Vanilla SQLite (`PRAGMA journal_mode = WAL`)** and **`aydb` Virtual Table (`USING aydb(...)`)** after passing a **pre-benchmark differential SQL equivalence verification suite (`verify_differential_sql_equivalence`)**:

0. **Pre-Benchmark Differential SQL Equivalence Verification (`Suite 0`):** Asserts 100% identical query outputs and final table states between a native SQLite table and an `aydb` virtual table across mixed numeric sorting (`INTEGER` & `REAL` Unified Numeric `ORDER BY`), column affinity coercions (`WHERE id = 1.0`, `WHERE id = '1'`), `COLLATE NOCASE` / `COLLATE RTRIM` lookups, failed `UNIQUE` constraint statements, nested `SAVEPOINT` / `ROLLBACK TO`, cross-connection snapshot isolation (`SQLITE_BUSY`), and mixed native-SQLite + `aydb` virtual table two-phase transactions.
1. **SQLite `"Speedtest1"`-Style Workload (Selected `Tests 100..240`):** Selected core SQL sub-tests modeled on SQLite's `Speedtest1` workload.
2. **`YCSB`-Style Workloads `A` through `F` (Zipfian $\theta = 0.99$):** Evaluated at both **`256 B` (Inline Storage)** and **`2,048 B` (WiscKey Separated Blob Storage)** to expose the exact boundary where WiscKey outperforms B-Trees and where WiscKey incurs range-scan dereference overhead.
3. **`TPC-B`-Style (`pgbench`-like) 4-Table OLTP Workload:** Multi-statement ACID transactions across `branches`, `tellers`, `accounts`, and `history`.

> **Scope & Interpretation Notes:**
> * **Workload Coverage:** These examples implement selected `Speedtest1`-style, `YCSB`-style, and `TPC-B`-style workloads rather than full certified/official benchmark suites.
> * **Warm OS Cache vs. Larger-Than-RAM Datasets:** The `5,000`-row / `4,000`-record / `2,000`-account workloads fit in RAM. Lowering `memtable_capacity_bytes = 128 KB` (§4) forces `idx_*.db` SSTable flushes and size-tiered compactions, exercising positional `ReadFile(OVERLAPPED)` calls against warm OS page cache rather than cold physical storage reads.
> * **Single-Host Execution & Simulated Fault Model:** Throughput measurements are single-host local Windows NTFS runs; power-loss and I/O fault tests in `examples/reliability_bench.rs` use deterministic in-memory simulation (`FaultInjectingFs`) and should be interpreted within that tested failure model rather than as physical hardware power-cut or live SMB network-outage tests.

<p align="center">
  <img src="../assets/bench_speedup_overview.svg" alt="aydb Relative Speedup Overview" width="100%"/>
</p>

---

## 1. Suite 1: SQLite `"Speedtest1"`-Style Workload (Selected `Tests 100..240`, `5,000` Indexed Rows)

* **Schema:** `t1(id INT PRIMARY KEY, uval INT, category TEXT, payload TEXT)` with secondary index on `uval` (`120 B` payload, `~600 KB` working set).
* **Durability & Scale Parity:** Both engines run with equivalent OS-page-cache commit durability (`PRAGMA synchronous = NORMAL` on SQLite vs. `WalStreamMode::BufferedImmediateOsCache` on `aydb` `wal_buf.db`) and full `PRIMARY KEY` uniqueness enforcement. At `5,000` rows (`~600 KB < 4 MB` default `MemTable`), `aydb` serves index reads from `MemTable` (`BTreeMap`) while SQLite serves them from its `2 MB` in-process `PCache`; see Section 4 below for on-disk `idx_*.db` SSTable performance (`memtable_capacity_bytes = 128 KB`).
* **Note on Workload Structure:** In `Speedtest1`-style workloads, Tests `100`, `230`, and `240` wrap thousands of mutations inside a **single giant `BEGIN ... COMMIT` transaction** where SQLite's native B-Tree mutates pages in its in-process user-space page cache without FFI boundary crossings per row.

<p align="center">
  <img src="../assets/bench_speedtest1_breakdown.svg" alt="SQLite Speedtest1-Style Execution Time Breakdown" width="100%"/>
</p>

| Speedtest1-Style Sub-Benchmark (`BufferedImmediateOsCache` vs `NORMAL`) | Vanilla SQLite (`WAL`) | `aydb` Virtual Table | Relative Speed (`aydb` vs. SQLite) | Architectural Analysis |
| :--- | ---: | ---: | ---: | :--- |
| **Test 100:** `5,000` Bulk `INSERT`s (`BEGIN..COMMIT`, Indexed) | `5.46 ms` | `14.11 ms` | `0.39×` (`2.58×` slower) | Per-row `xUpdate` Virtual Table C ABI call + `Mutex` lock + `row_exists` PK uniqueness check + `RowMetaRecord` secondary index maintenance vs. native in-page B-Tree append. |
| **Test 130:** `5,000` Random PK Point `SELECT`s (`WHERE id = ?`) | `14.69 ms` | **`5.33 ms`** | **`2.76×` faster** | Zero-allocation stack-key `get_row_into` + reusable `AydbCursor` byte slab + zero-copy `xColumn` (`project_column_zero_copy`). |
| **Test 140:** `5,000` Secondary Index `SELECT`s (`WHERE uval = ?`) | `15.97 ms` | **`7.58 ms`** | **`2.11×` faster** | Direct memcomparable prefix range iteration (`iter_range`) + zero-allocation PK slice extraction. |
| **Test 160:** `1,000` PK Range Scans (`WHERE id >= ? AND id < ?`, 20 rows) | `5.34 ms` | **`4.66 ms`** | **`1.15×` faster** | `scan_range_into_slab` appends matching range slices directly into the reusable cursor byte slab in bounded 256-row streaming chunks without per-row `Vec<u8>` allocations. |
| **Test 210:** `10×` Full-Table `GROUP BY` Aggregation (`5,000` rows) | `11.35 ms` | **`10.25 ms`** | **`1.11×` faster** | Chunked 256-row streaming cursor bounds `cursor.slab` memory and streams 5,000 rows per pass across `xFilter`/`xNext`/`xColumn`; Phase 3 Native SQL eliminates this FFI copy. |
| **Test 230:** `2,500` Random Indexed `UPDATE`s (`BEGIN..COMMIT`) | `4.51 ms` | `12.05 ms` | `0.37×` (`2.67×` slower) | Each Virtual Table `UPDATE` executes a full `xFilter` + `xRowid` + `xColumn` + `xUpdate` cycle across the C ABI boundary plus undo-log tracking. |
| **Test 240:** `2,500` Random `DELETE` + Re-`INSERT`s (`BEGIN..COMMIT`) | `5.63 ms` | `13.57 ms` | `0.42×` (`2.41×` slower) | `5,000` Virtual Table `xUpdate` FFI crossings + PK existence checks inside a single in-memory transaction. |
| **TOTAL `Speedtest1`-Style Suite Time (`4 MB` `MemTable`)** | `62.95 ms` | **`67.55 ms`** | **`0.93×`** (`205.99 ms` / `0.31×` at `128 KB` SSTable) | `aydb` leads `1.11×–2.76×` on PK point, secondary-index, and range queries (`Tests 130, 140, 160, 210`), while single-transaction bulk mutation loops (`Tests 100, 230, 240`) pay per-row `sqlite3_module` C ABI and PK uniqueness checks. |

### 1.1 Virtual Table & StorageEngine Hot-Path Optimizations Applied
To accelerate `Speedtest1`-style query and range-scan execution (`Tests 130, 140, 160, 210` running **`1.11×–2.76×` faster than Vanilla SQLite**) and reduce Virtual Table FFI overhead, six targeted hot-path optimizations were engineered across `src/vtab/` and `src/engine/`:
1. **Zero-Copy `xColumn` Projection (`project_column_zero_copy`) & Keys-Only Pushdown (`RANGE_FLAG_KEYS_ONLY`):** Instead of decoding a column into a heap-allocated `SqlValue::Text(String)` or `SqlValue::Blob(Vec<u8>)` and immediately dropping it after `sqlite3_result_text`, `xColumn` indexes the $O(1)$ column offset directory and passes `&row_bytes[offset + 4 .. offset + 4 + len]` directly to `sqlite3_result_text` / `sqlite3_result_blob` with **zero Rust heap allocations**. Furthermore, when `sqlite3_index_info::colUsed` indicates only `rowid` or a binary-collated PK column is needed (such as `SELECT COUNT(*)`), `xFilter` skips reading out-of-line blobs and skips populating the cursor byte slab entirely.
2. **Direct `sqlite3_value*` Row Encoding (`encode_sqlite_values_direct`) & Row-Metadata Negative Lookup Skip (`may_have_row_meta`):** `xUpdate` encodes SQLite's raw `*mut *mut sqlite3_value` array directly into the binary row buffer in a single pass without allocating an intermediate `Vec<SqlValue>`, while `StorageEngine::may_have_row_meta` caches `false` for uncapped tables without secondary indexes so `UPDATE` and `DELETE` skip negative `'t'` metadata probes across SSTables.
3. **Reusable Contiguous Cursor Byte Slab (`AydbCursor::slab` & `spans`), Chunked 256-Row Streaming, & Reverse PK Scan (`RANGE_FLAG_REVERSE`):** Rather than allocating and freeing thousands of individual `Vec<u8>` row buffers or materializing entire tables into RAM on `xFilter`, `AydbCursor` streams `CURSOR_STREAM_CHUNK_ROWS` (`256`) rows at a time via `scan_range_into_slab` and pushes down both `ORDER BY pk ASC` and `ORDER BY pk DESC` (`RANGE_FLAG_REVERSE`).
4. **Lock-Free Reversible Integer `rowid` Mapping (`has_non_int_pks`):** Because 9-byte tagged integer primary keys (`[KEY_TAG_NUM_NARROW | memcomparable_i64 | 0]`) map bijectively to 64-bit SQLite `rowid`s via `encode_i64_tagged_key` / `decode_i64_tagged_key`, integer-PK tables completely bypass locking and inserting into `rowid_to_pk: Mutex<RowidMap>`.
5. **Zero-Clone `MemTable` Ownership Transfer (`apply_wal_batch_owned` & `pending_tx_buf`) + $O(\text{tx})$ Undo Log:** Transaction mutations serialize directly into a reusable `pending_tx_buf: Vec<u8>` and move their owned `key: Vec<u8>` and `value: Vec<u8>` into `MemTable`'s `BTreeMap` while recording prior entries in an in-memory undo log for instant $O(\text{tx})$ rollback, `SAVEPOINT` restoration, and cross-connection snapshot isolation.
6. **Multi-Sector Coalesced WAL Writes (`multi_sector_buf`) & 256 KB Buffered Blob Staging:** When a large transaction batch spans $K > 1$ sectors (e.g., ~250 sectors in `Test 100`), `WalStreamWriter::append_raw_batch` seals all $K$ sectors in memory and issues a **single contiguous `write_all_at` syscall**, while `BlobSegmentWriter` stages out-of-line blob appends in a 256 KB buffer flushed before WAL append.

---

## 2. Suite 2: `YCSB`-Style Workloads `A` through `F` (Zipfian $\theta = 0.99$)

`YCSB`-style workloads evaluate realistic autocommit request streams across 6 standard access mixes (`4,000` initial rows, `5,000` operations per workload, Zipfian skew $\theta = 0.99$, using true SQL `UPDATE` statements, with `WalStreamMode::BufferedImmediateOsCache` on `aydb` vs. `PRAGMA synchronous = NORMAL` on SQLite). We run the entire `YCSB`-style suite twice: once at **`256 B` (Inline Storage, `~1.1 MB` total)** and once at **`2,048 B` (WiscKey Out-of-Line Blob Storage, `~8.2 MB` initial + `~5 MB` updates in `blob_std_*.db`)**.

<p align="center">
  <img src="../assets/bench_ycsb_throughput.svg" alt="YCSB-Style Workloads A-F Throughput Comparison" width="100%"/>
</p>

### 2.1 `YCSB`-Style at `256 B` Value Size (Below `1024 B` Inline Threshold)

| YCSB-Style Workload | Mix | Vanilla SQLite (`WAL`, `NORMAL`) | `aydb` Virtual Table (`4 MB` Default / `128 KB` SSTable) | Speedup (`aydb` / SQLite) |
| :--- | :--- | ---: | ---: | ---: |
| **Workload A (Update Heavy)** | 50% Read / 50% Update | `220,925 ops/s` | **`298,759 ops/s`** | **`1.35×` faster** |
| **Workload B (Read Mostly)** | 95% Read / 5% Update | `291,089 ops/s` | **`886,541 ops/s`** (SSTable: **`362,245 ops/s`**) | **`3.05×` faster** (**`1.24×` SSTable**) |
| **Workload C (Read Only)** | 100% Point Read | `303,317 ops/s` | **`1,207,788 ops/s`** (SSTable: **`435,441 ops/s`**) | **`3.98×` faster** (**`1.44×` SSTable**) |
| **Workload D (Read Latest)** | 95% Read Latest / 5% Insert | `205,846 ops/s` | **`889,616 ops/s`** | **`4.32×` faster** |
| **Workload E (Short Ranges)** | 95% Scan (10 rows) / 5% Insert | `107,306 ops/s` | `105,312 ops/s` (`256`-row chunk) | `0.98×` |
| **Workload F (Read-Modify-Write)** | 50% Read / 50% RMW | `176,198 ops/s` | **`264,328 ops/s`** | **`1.50×` faster** |

### 2.2 `YCSB`-Style at `2,048 B` Value Size (Above `1024 B` — WiscKey Separated Blob Storage)

| YCSB-Style Workload | Mix | Vanilla SQLite (`WAL`, `NORMAL`) | `aydb` Virtual Table (`blob_std_*.db`) | Speedup (`aydb` / SQLite) |
| :--- | :--- | ---: | ---: | ---: |
| **Workload A (Update Heavy)** | 50% Read / 50% Update | **`155,039 ops/s`** | `85,945 ops/s` | `0.55×` (`1.80×` slower) |
| **Workload B (Read Mostly)** | 95% Read / 5% Update | **`184,106 ops/s`** | `153,488 ops/s` | `0.83×` |
| **Workload C (Read Only)** | 100% Point Read | **`191,615 ops/s`** | `187,949 ops/s` | `0.98×` |
| **Workload D (Read Latest)** | 95% Read Latest / 5% Insert | `104,568 ops/s` | **`167,026 ops/s`** | **`1.60×` faster** |
| **Workload E (Short Ranges)** | 95% Scan (10 rows) / 5% Insert | **`42,821 ops/s`** | `927 ops/s` (`256`-row chunk) | `0.02×` |
| **Workload F (Read-Modify-Write)** | 50% Read / 50% RMW | **`120,908 ops/s`** | `69,976 ops/s` | `0.58×` (`1.73×` slower) |

### Architectural Analysis of the `YCSB`-Style Results
1. **Inline Storage Advantage Across Point-Read & Update `YCSB`-Style Workloads (`256 B`, `1.35×` to `4.32×` Faster):**
   * For `256 B` records stored inline, `aydb` outperforms Vanilla SQLite across all five point-lookup and update workloads (`A, B, C, D, F`), reaching **`1,207,788 ops/s`** (`3.98×` faster) on point reads (`Workload C`), **`886,541 ops/s`** (`3.05×` faster) on read-mostly (`Workload B`), **`889,616 ops/s`** (`4.32×` faster) on read-latest (`Workload D`), and **`264,328 ops/s`** (`1.50×` faster) on read-modify-write (`Workload F`).
2. **Why `2,048 B` WiscKey `UPDATE`s (`Workload A` & `F`, `0.55×`–`0.58×`) Trail In-Cache SQLite, While `Workload D` (`1.60×`) Wins:**
   * **Virtual Table `UPDATE` Protocol on Separated Blobs (`Workload A` `0.55×`, `Workload F` `0.58×`):** With 4,000 × 2 KB rows (~8 MB), hot Zipfian keys reside inside SQLite's 2 MB in-process user-space `PCache`, so SQLite mutates B-Tree pages in-place in user-space RAM. By contrast, SQLite's Virtual Table `UPDATE` protocol MUST execute `xFilter` first—which reads the old 2 KB blob via a positional `ReadFile` syscall from `blob_std_*.db`—followed by `xRowid`/`xColumn`, and then appends a new 2 KB blob in `xUpdate`.
   * **Near-Parity Point Reads (`Workload C` `0.98×`) & `Workload D` Win (`1.60×`):** On pure point lookups (`Workload C`), `aydb` reaches `187.9K ops/s` vs. SQLite's `191.6K ops/s` (`0.98×`), reading directly into the cursor slab via stack-framed single-`ReadFile` dereferences. On `Workload D` (95% Read Latest / 5% Insert), `aydb` is **`1.60×` faster** (`167,026` vs. `104,568 ops/s`) because new inserts on a SQLite `TEXT PRIMARY KEY` table must allocate a new `rowid` and insert into both the main table B-Tree and `sqlite_autoindex`, whereas `aydb` stages the blob and inserts a single 23-byte `BlobPtr` into `MemTable`.
3. **Chunked Streaming Cursor (`CURSOR_STREAM_CHUNK_ROWS = 256`) and `Workload E` (`LIMIT 10`):**
   * To guarantee 100% SQL correctness when queries combine `LIMIT` with residual `WHERE` predicates that SQLite does not pass in `aConstraint` (such as `LIKE`, `OR`, or arbitrary expressions), `xBestIndex` lets SQLite's VDBE enforce `LIMIT`/`OFFSET` while `xFilter` streams rows in bounded `256`-row chunks (`CURSOR_STREAM_CHUNK_ROWS = 256`). On `Workload E` (`WHERE ycsb_key >= ? ORDER BY ycsb_key ASC LIMIT 10`), `xFilter` materializes one `256`-row chunk per scan (`105,312 ops/s` at `256 B` inline where values live in `MemTable`, and `927 ops/s` at `2,048 B` WiscKey where `xFilter` dereferences `256` separated blobs per chunk).

---

## 3. Suite 3: `TPC-B`-Style (`pgbench`-Like) 4-Table OLTP Workload

Each `TPC-B`-style transaction executes `BEGIN` + 5 parameterized SQL statements with true `UPDATE`s across 4 tables (`UPDATE accounts`, `SELECT abalance`, `UPDATE tellers`, `UPDATE branches`, `INSERT INTO history`) + `COMMIT`.

| Durability Mode | Vanilla SQLite (`WAL`) | `aydb` Virtual Table (`4 MB` Default / `128 KB` SSTable) | Speedup (`4 MB` / `SSTable`) |
| :--- | ---: | ---: | ---: |
| **Buffered (`synchronous = NORMAL` vs. `wal_buf.db` `ImmediateOsCache`)** | `15,576 TPS` (`15,506` SST) | **`110,257 TPS`** (SSTable: **`45,774 TPS`**) | **`7.08×` (`2.95×` SSTable)** |
| **Full Sync (`synchronous = FULL` vs. `wal_sync.db` + `sync_barrier()`)** | `529 TPS` (`439` SST) | **`13,817 TPS`** (SSTable: **`9,969 TPS`**) | **`26.12×` (`22.73×` SSTable)** |

* **Why Transaction Coalescing (`xBegin` $\rightarrow$ `xSync` $\rightarrow$ `xCommit`) & Row-Metadata Skip Shine in `TPC-B`-Style OLTP:**
  By coalescing the 4 table mutations (`accounts`, `tellers`, `branches`, `history`) inside `StorageEngine::begin_transaction()` / `prepare_commit_transaction()` / `commit_transaction()` into a **single atomic `WalBatch`** written to a single pre-zeroed 4 KB sector in `wal_sync.db` followed by `sync_barrier()`, and skipping negative `'t'` metadata probes on uncapped tables without secondary indexes, `aydb` achieves **`13,817 TPS` (`~72.4 µs` per 4-table durable transaction)**—a **`26.12×` speedup** over Vanilla SQLite (`529 TPS`), which must write and `FlushFileBuffers()` multiple dirty B-Tree pages across 4 different B-Trees on every commit!

---

## 4. On-Disk SSTable Flush Evaluation (`memtable_capacity_bytes = 128 KB`)

With the default `memtable_capacity_bytes = 4 MB`, the index footprints of `Speedtest1`-style (~600 KB), `YCSB`-style (~100–200 KB), and `TPC-B`-style (~300 KB) fit entirely within `MemTable`. To evaluate on-disk `idx_<SeqID>.db` SSTable lookup, per-segment `[min_key, max_key]` pruning, size-tiered automatic compaction (`compact_auto_tiered`, `auto_compact_segment_threshold = 8`), and multi-segment merge performance directly (`Suite 4` of `examples/standard_sql_bench.rs`), we run the suite with **`memtable_capacity_bytes = 128 KB`**, forcing SSTable flushes after initial loading and during write-heavy phases (while keeping OS file cache warm):

| Benchmark Workload (`memtable_capacity_bytes = 128 KB`) | Vanilla SQLite (`WAL`, `2 MB` User-Space `PCache`) | `aydb` Virtual Table (On-Disk `idx_*.db` SSTables) | Relative Speed (`aydb` / SQLite) |
| :--- | ---: | ---: | ---: |
| **`Speedtest1`-Style Total (`Tests 100–240`)** | `64.13 ms` | `205.99 ms` | `0.31×` |
| **`YCSB`-Style `256 B` Workload C (100% Point Read)** | `301,723 ops/s` | **`435,441 ops/s`** | **`1.44×` faster** |
| **`YCSB`-Style `256 B` Workload B (95% Read / 5% Update)** | `292,394 ops/s` | **`362,245 ops/s`** | **`1.24×` faster** |
| **`TPC-B`-Style Buffered (`wal_buf.db` vs. `NORMAL`)** | `15,506 TPS` | **`45,774 TPS`** | **`2.95×` faster** |
| **`TPC-B`-Style Full Sync (`wal_sync.db` vs. `FULL`)** | `439 TPS` | **`9,969 TPS`** | **`22.73×` faster** |

### Architectural Analysis of On-Disk SSTable Performance
1. **Still `1.24×`–`22.73×` Faster on `YCSB`-Style Point Reads & `TPC-B`-Style OLTP Even From On-Disk `idx_*.db` SSTables:**
   * Even when `memtable_capacity_bytes = 128 KB` forces keys out of RAM into on-disk `idx_<SeqID>.db` SSTables, `aydb`'s in-heap **10-bit/key Bloom Filters**, **per-segment `[min_key, max_key]` bounds**, **Row-Metadata Skip**, **Size-Tiered Suffix Compaction**, and **Sparse Fence Pointers** pinpoint the exact 4 KB SSTable block in a single positional `ReadFile(OVERLAPPED)` call—achieving **`435,441 ops/s` (`1.44×` faster than SQLite)** on `YCSB-Style Workload C`, **`362,245 ops/s` (`1.24×` faster)** on `YCSB-Style Workload B`, **`45,774 TPS` (`2.95×` faster)** on Buffered `TPC-B-Style`, and **`9,969 TPS` (`22.73×` faster)** on Full-Sync `TPC-B-Style`.
2. **Why `Speedtest1`-Style Total Drops to `0.31×` at `128 KB` `MemTable` Capacity (OS Page Cache Syscall Overhead vs. In-Process `PCache`):**
   * Because `aydb` strictly bounds its process heap and delegates 4 KB data block caching to the **OS Page Cache** via `ReadFile(OVERLAPPED)` + per-block hardware `CRC32C` verification—rather than duplicating a `2 MB` user-space page cache (`PCache`) inside the process like Vanilla SQLite—every SSTable block read crosses the user/kernel boundary (`ReadFile` syscall) and verifies `block_crc32c`. On small datasets (~600 KB in `Speedtest1`-style) that fit 100% inside SQLite's `2 MB` in-process `PCache` with zero syscalls, forcing `aydb` to spill into multiple `128 KB` on-disk SSTable segments incurs kernel syscall and multi-segment K-way merge overhead during `Speedtest1`'s 10× full-table scans and 5,000-row `INSERT` PK uniqueness probes.
