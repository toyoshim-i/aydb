# Chapter 2.3: Selected Standard SQL Benchmark Workloads (`Speedtest1`-Style, `YCSB`-Style `A–F`, and `TPC-B`-Style)

> Figures are medians of five runs at commit `6ae760e` on Windows 11 (local NTFS, Crucial P3 NVMe SSD). Ratios are the median of the per-run ratios, so they can differ from the ratio of the two medians.

To evaluate `aydb` objectively beyond custom cache/config microbenchmarks, this chapter presents results from three representative standard database workload styles (`examples/standard_sql_bench.rs`) executed via identical SQL statements against **Vanilla SQLite (`PRAGMA journal_mode = WAL`)** and **`aydb` Virtual Table (`USING aydb(...)`)** after passing a **pre-benchmark differential SQL equivalence verification suite (`verify_differential_sql_equivalence`)**:

0. **Pre-Benchmark Differential SQL Equivalence Verification (`Suite 0`):** Asserts 100% identical query outputs and final table states between a native SQLite table and an `aydb` virtual table across mixed numeric sorting (`INTEGER` & `REAL` Unified Numeric `ORDER BY`), column affinity coercions (`WHERE id = 1.0`, `WHERE id = '1'`), `COLLATE NOCASE` / `COLLATE RTRIM` lookups, failed `UNIQUE` constraint statements, nested `SAVEPOINT` / `ROLLBACK TO`, cross-connection snapshot isolation (`SQLITE_BUSY`), and mixed native-SQLite + `aydb` virtual table two-phase transactions.
1. **SQLite `"Speedtest1"`-Style Workload (Selected `Tests 100..240`):** Selected core SQL sub-tests modeled on SQLite's `Speedtest1` workload.
2. **`YCSB`-Style Workloads `A` through `F` (Zipfian $\theta = 0.99$):** Evaluated at both **`256 B` (Inline Storage)** and **`2,048 B` (WiscKey Separated Blob Storage)** to expose the exact boundary where WiscKey outperforms B-Trees and where WiscKey incurs range-scan dereference overhead.
3. **`TPC-B`-Style (`pgbench`-like) 4-Table OLTP Workload:** Multi-statement ACID transactions across `branches`, `tellers`, `accounts`, and `history`.

> **Scope & Interpretation Notes:**
> * **Workload Coverage:** These examples implement selected `Speedtest1`-style, `YCSB`-style, and `TPC-B`-style workloads rather than full certified/official benchmark suites.
> * **Warm OS Cache vs. Larger-Than-RAM Datasets:** The `5,000`-row / `4,000`-record / `2,000`-account workloads fit in RAM. Lowering `memtable_capacity_bytes = 128 KB` (§4) forces `idx_*.db` SSTable flushes and size-tiered compactions, exercising positional `ReadFile(OVERLAPPED)` calls against warm OS page cache rather than cold physical storage reads.
> * **Single-Host Execution & Simulated Fault Model:** Throughput measurements are single-host local Windows NTFS runs; power-loss and I/O fault tests in `examples/reliability_bench.rs` use deterministic in-memory simulation (`FaultInjectingFs`) and should be interpreted within that tested failure model rather than as physical hardware power-cut or live SMB network-outage tests.

---

## 1. Suite 1: SQLite `"Speedtest1"`-Style Workload (Selected `Tests 100..240`, `5,000` Indexed Rows)

* **Schema:** `t1(id INT PRIMARY KEY, uval INT, category TEXT, payload TEXT)` with secondary index on `uval` (`120 B` payload, `~600 KB` working set).
* **Durability & Scale Parity:** Both engines run with equivalent OS-page-cache commit durability (`PRAGMA synchronous = NORMAL` on SQLite vs. `WalStreamMode::BufferedImmediateOsCache` on `aydb` `wal_buf.db`) and full `PRIMARY KEY` uniqueness enforcement. At `5,000` rows (`~600 KB < 4 MB` default `MemTable`), `aydb` serves index reads from `MemTable` (`BTreeMap`) while SQLite serves them from its `2 MB` in-process `PCache`; see Section 4 below for on-disk `idx_*.db` SSTable performance (`memtable_capacity_bytes = 128 KB`).
* **Note on Workload Structure:** In `Speedtest1`-style workloads, Tests `100`, `230`, and `240` wrap thousands of mutations inside a **single giant `BEGIN ... COMMIT` transaction** where SQLite's native B-Tree mutates pages in its in-process user-space page cache without FFI boundary crossings per row.

| Speedtest1-Style Sub-Benchmark (`BufferedImmediateOsCache` vs `NORMAL`) | Vanilla SQLite (`WAL`) | `aydb` Virtual Table | Relative Speed (`aydb` vs. SQLite) | Architectural Analysis |
| :--- | ---: | ---: | ---: | :--- |
| **Test 100:** `5,000` Bulk `INSERT`s (`BEGIN..COMMIT`, Indexed) | `5.13 ms` | `11.18 ms` | `0.46×` (`2.17×` slower) | Per-row `xUpdate` Virtual Table C ABI call + `Mutex` lock + `row_exists` PK uniqueness check + `RowMetaRecord` secondary index maintenance vs. native in-page B-Tree append. |
| **Test 130:** `5,000` Random PK Point `SELECT`s (`WHERE id = ?`) | `13.83 ms` | **`5.02 ms`** | **`2.76×` faster** | Zero-allocation stack-key `get_row_into` + reusable `AydbCursor` byte slab + zero-copy `xColumn` (`project_column_zero_copy`). |
| **Test 140:** `5,000` Secondary Index `SELECT`s (`WHERE uval = ?`) | `15.72 ms` | **`6.92 ms`** | **`2.28×` faster** | Direct memcomparable prefix range iteration (`iter_range`) + zero-allocation PK slice extraction. |
| **Test 160:** `1,000` PK Range Scans (`WHERE id >= ? AND id < ?`, 20 rows) | `5.15 ms` | `5.27 ms` | `0.99×` | `scan_range_into_slab` appends matching range slices directly into the reusable cursor byte slab in bounded 256-row streaming chunks without per-row `Vec<u8>` allocations. |
| **Test 210:** `10×` Full-Table `GROUP BY` Aggregation (`5,000` rows) | `9.16 ms` | `11.92 ms` | `0.76×` | Chunked 256-row streaming cursor bounds `cursor.slab` memory and streams 5,000 rows per pass across `xFilter`/`xNext`/`xColumn`; Phase 3 Native SQL eliminates this FFI copy. |
| **Test 230:** `2,500` Random Indexed `UPDATE`s (`BEGIN..COMMIT`) | `3.90 ms` | `10.02 ms` | `0.38×` (`2.63×` slower) | Each Virtual Table `UPDATE` executes a full `xFilter` + `xRowid` + `xColumn` + `xUpdate` cycle across the C ABI boundary plus transaction overlay maintenance. |
| **Test 240:** `2,500` Random `DELETE` + Re-`INSERT`s (`BEGIN..COMMIT`) | `4.87 ms` | `11.76 ms` | `0.41×` (`2.44×` slower) | `5,000` Virtual Table `xUpdate` FFI crossings + PK existence checks inside a single in-memory transaction. |
| **TOTAL `Speedtest1`-Style Suite Time (`4 MB` `MemTable`)** | `57.97 ms` | **`62.81 ms`** | **`0.92×`** (**`79.23 ms`** / **`0.71×`** at `128 KB` SSTable) | `aydb` leads on PK point and secondary-index queries (`Tests 130, 140`) and is level on range scans (`Test 160`), while single-transaction bulk mutation loops (`Tests 100, 230, 240`) pay per-row `sqlite3_module` C ABI and PK uniqueness checks. |

### 1.1 Virtual Table & StorageEngine Hot-Path Optimizations Applied
To accelerate `Speedtest1`-style query and range-scan execution and reduce Virtual Table FFI overhead, six targeted hot-path optimizations were engineered across `src/vtab/` and `src/engine/`:
1. **Zero-Copy `xColumn` Projection (`project_column_zero_copy`) & Keys-Only Pushdown (`RANGE_FLAG_KEYS_ONLY`):** Instead of decoding a column into a heap-allocated `SqlValue::Text(String)` or `SqlValue::Blob(Vec<u8>)` and immediately dropping it after `sqlite3_result_text`, `xColumn` indexes the $O(1)$ column offset directory and passes `&row_bytes[offset + 4 .. offset + 4 + len]` directly to `sqlite3_result_text` / `sqlite3_result_blob` with **zero Rust heap allocations**. Furthermore, when `sqlite3_index_info::colUsed` indicates only `rowid` or a binary-collated PK column is needed (such as `SELECT COUNT(*)`), `xFilter` skips reading out-of-line blobs and skips populating the cursor byte slab entirely.
2. **Direct `sqlite3_value*` Row Encoding (`encode_sqlite_values_direct`) & Row-Metadata Negative Lookup Skip (`may_have_row_meta`):** `xUpdate` encodes SQLite's raw `*mut *mut sqlite3_value` array directly into the binary row buffer in a single pass without allocating an intermediate `Vec<SqlValue>`, while `StorageEngine::may_have_row_meta` caches `false` for uncapped tables without secondary indexes so `UPDATE` and `DELETE` skip negative `'t'` metadata probes across SSTables.
3. **Reusable Contiguous Cursor Byte Slab (`AydbCursor::slab` & `spans`), Chunked 256-Row Streaming, & Reverse PK Scan (`RANGE_FLAG_REVERSE`):** Rather than allocating and freeing thousands of individual `Vec<u8>` row buffers or materializing entire tables into RAM on `xFilter`, `AydbCursor` streams `CURSOR_STREAM_CHUNK_ROWS` (`256`) rows at a time via `scan_range_into_slab` and pushes down both `ORDER BY pk ASC` and `ORDER BY pk DESC` (`RANGE_FLAG_REVERSE`).
4. **Reversible Integer `rowid` Mapping:** Because 9-byte tagged integer primary keys (`[KEY_TAG_NUM_NARROW | memcomparable_i64 | 0]`) map bijectively to 64-bit SQLite `rowid`s via `encode_i64_tagged_key` / `decode_i64_tagged_key`, integer-PK tables need no rowid identity lookup at all. Non-integer keys resolve through the engine's persistent rowid index; the adapter keeps no in-memory rowid cache.
5. **Private Transaction Overlay (`TxOverlay`) & Zero-Clone Publish (`pending_tx_buf`):** Transaction mutations serialize directly into a reusable `pending_tx_buf: Vec<u8>` and land in a private overlay, so other connections read committed data at a cost independent of the transaction size. At commit the overlay's entries move into the `MemTable` without cloning. An undo log inside the overlay serves `SAVEPOINT` restoration, and a full rollback discards the overlay.
6. **Multi-Sector Coalesced WAL Writes (`multi_sector_buf`) & 256 KB Buffered Blob Staging:** When a large transaction batch spans $K > 1$ sectors (e.g., ~250 sectors in `Test 100`), `WalStreamWriter::append_raw_batch` seals all $K$ sectors in memory and issues a **single contiguous `write_all_at` syscall**, while `BlobSegmentWriter` stages out-of-line blob appends in a 256 KB buffer flushed before WAL append.

---

## 2. Suite 2: `YCSB`-Style Workloads `A` through `F` (Zipfian $\theta = 0.99$)

`YCSB`-style workloads evaluate realistic autocommit request streams across 6 standard access mixes (`4,000` initial rows, `5,000` operations per workload, Zipfian skew $\theta = 0.99$, using true SQL `UPDATE` statements, with `WalStreamMode::BufferedImmediateOsCache` on `aydb` vs. `PRAGMA synchronous = NORMAL` on SQLite). We run the entire `YCSB`-style suite twice: once at **`256 B` (Inline Storage, `~1.1 MB` total)** and once at **`2,048 B` (WiscKey Out-of-Line Blob Storage, `~8.2 MB` initial + `~5 MB` updates in `blob_std_*.db`)**.

### 2.1 `YCSB`-Style at `256 B` Value Size (Below `1024 B` Inline Threshold)

| YCSB-Style Workload | Mix | Vanilla SQLite (`WAL`, `NORMAL`) | `aydb` Virtual Table (`4 MB` Default / `128 KB` SSTable) | Speedup (`aydb` / SQLite) |
| :--- | :--- | ---: | ---: | ---: |
| **Workload A (Update Heavy)** | 50% Read / 50% Update | `245,186 ops/s` | **`304,364 ops/s`** | **`1.26×` faster** |
| **Workload B (Read Mostly)** | 95% Read / 5% Update | `307,084 ops/s` | **`829,284 ops/s`** (SSTable: **`730,588 ops/s`**) | **`2.70×` faster** (**`2.43×` SSTable**) |
| **Workload C (Read Only)** | 100% Point Read | `316,793 ops/s` | **`1,039,285 ops/s`** (SSTable: **`919,219 ops/s`**) | **`3.24×` faster** (**`2.95×` SSTable**) |
| **Workload D (Read Latest)** | 95% Read Latest / 5% Insert | `221,838 ops/s` | **`804,609 ops/s`** | **`3.63×` faster** |
| **Workload E (Short Ranges)** | 95% Scan (10 rows) / 5% Insert | `124,360 ops/s` | `52,219 ops/s` (`256`-row chunk) | `0.42×` |
| **Workload F (Read-Modify-Write)** | 50% Read / 50% RMW | `180,072 ops/s` | **`268,394 ops/s`** | **`1.49×` faster** |

### 2.2 `YCSB`-Style at `2,048 B` Value Size (Above `1024 B` — WiscKey Separated Blob Storage)

| YCSB-Style Workload | Mix | Vanilla SQLite (`WAL`, `NORMAL`) | `aydb` Virtual Table (`blob_std_*.db`) | Speedup (`aydb` / SQLite) |
| :--- | :--- | ---: | ---: | ---: |
| **Workload A (Update Heavy)** | 50% Read / 50% Update | **`169,872 ops/s`** | `101,415 ops/s` | `0.59×` |
| **Workload B (Read Mostly)** | 95% Read / 5% Update | `210,231 ops/s` | **`302,913 ops/s`** | **`1.43×` faster** |
| **Workload C (Read Only)** | 100% Point Read | `215,393 ops/s` | **`481,237 ops/s`** | **`2.25×` faster** |
| **Workload D (Read Latest)** | 95% Read Latest / 5% Insert | `137,711 ops/s` | **`325,869 ops/s`** | **`2.40×` faster** |
| **Workload E (Short Ranges)** | 95% Scan (10 rows) / 5% Insert | **`55,922 ops/s`** | `39,091 ops/s` (Prefetched Blobs) | `0.69×` |
| **Workload F (Read-Modify-Write)** | 50% Read / 50% RMW | **`137,210 ops/s`** | `100,643 ops/s` | `0.73×` |

### Architectural Analysis of the `YCSB`-Style Results
1. **Inline Storage Advantage Across Point-Read & Update `YCSB`-Style Workloads (`256 B`, `1.26×` to `3.63×` Faster):**
   * For `256 B` records stored inline, `aydb` outperforms Vanilla SQLite across all five point-lookup and update workloads (`A, B, C, D, F`), reaching **`1,039,285 ops/s`** (`3.24×` faster) on point reads (`Workload C`), **`829,284 ops/s`** (`2.70×` faster) on read-mostly (`Workload B`), **`804,609 ops/s`** (`3.63×` faster) on read-latest (`Workload D`), and **`268,394 ops/s`** (`1.49×` faster) on read-modify-write (`Workload F`).
2. **Why `2,048 B` WiscKey Point Reads (`Workload B, C, D`, `1.43×`–`2.40×`) Outperform SQLite with Verified Blob Cache:**
   * With the bounded `VerifiedBlobCache` and 3-way interleaved CRC32C, hot 2 KB blobs hit user-space memory directly with zero kernel transitions, while uncached reads benefit from zero-copy dereference. Point reads (`Workload C`) run at **`481,237 ops/s` (`2.25×` faster)**, read-mostly (`Workload B`) runs at **`302,913 ops/s` (`1.43×` faster)**, and read-latest (`Workload D`) reaches **`325,869 ops/s` (`2.40×` faster)**.
3. **Chunked Streaming Cursor and Range-Scan Blob Prefetch (`Workload E` `LIMIT 10`):**
   * Range scans projecting blob columns benefit from `read_many_at` coalescing: when row 0 dereferences a lazy blob, upcoming rows in the cursor batch are prefetched and verified into `VerifiedBlobCache` in a single coalesced kernel read, eliminating 9 out of 10 syscalls on short range scans!

---

## 3. Suite 3: `TPC-B`-Style (`pgbench`-Like) 4-Table OLTP Workload

Each `TPC-B`-style transaction executes `BEGIN` + 5 parameterized SQL statements with true `UPDATE`s across 4 tables (`UPDATE accounts`, `SELECT abalance`, `UPDATE tellers`, `UPDATE branches`, `INSERT INTO history`) + `COMMIT`.

| Durability Mode | Vanilla SQLite (`WAL`) | `aydb` Virtual Table (`4 MB` Default / `128 KB` SSTable) | Speedup (`4 MB` / `SSTable`) |
| :--- | ---: | ---: | ---: |
| **Buffered (`synchronous = NORMAL` vs. `wal_buf.db` `ImmediateOsCache`)** | `22,437 TPS` (`21,906` SST) | **`108,711 TPS`** (SSTable: **`80,864 TPS`**) | **`4.85×` (`3.77×` SSTable)** |
| **Full Sync (`synchronous = FULL` vs. `wal_sync.db` + `sync_barrier()`)** | `982 TPS` (`1,004` SST) | `1,025 TPS` (SSTable: `1,068 TPS`) | `1.04×` (`1.05×` SSTable) |
| **Sustained Full Sync (8,000 transactions)** | `817 TPS` | `1,088 TPS` | `1.07×` |

* **Why Transaction Coalescing (`xBegin` $\rightarrow$ `xSync` $\rightarrow$ `xCommit`) & Row-Metadata Skip Shine in `TPC-B`-Style OLTP:**
  `aydb` coalesces the 4 table mutations into a **single atomic `WalBatch`** written to a pre-zeroed 4 KB sector in `wal_sync.db`, and skips negative `'t'` metadata probes on uncapped tables without secondary indexes. In buffered mode, where no commit waits for the device, this gives `4.85×` the throughput of SQLite. With full sync both engines wait for one `FlushFileBuffers` per commit, which dominates, so the throughput is about equal (`1.04×`). These runs use a single writer. Concurrent synchronous writers, which benefit from group commit, are measured in Chapter 2.2 §4.

---

## 4. On-Disk SSTable Flush Evaluation (`memtable_capacity_bytes = 128 KB`)

With the default `memtable_capacity_bytes = 4 MB`, the index footprints of `Speedtest1`-style (~600 KB), `YCSB`-style (~100–200 KB), and `TPC-B`-style (~300 KB) fit entirely within `MemTable`. To evaluate on-disk `idx_<SeqID>.db` SSTable lookup, per-segment `[min_key, max_key]` pruning, size-tiered automatic compaction (`compact_auto_tiered`, `auto_compact_segment_threshold = 8`), and multi-segment merge performance directly (`Suite 4` of `examples/standard_sql_bench.rs`), we run the suite with **`memtable_capacity_bytes = 128 KB`**, forcing SSTable flushes after initial loading and during write-heavy phases (while keeping OS file cache warm):

| Benchmark Workload (`memtable_capacity_bytes = 128 KB`) | Vanilla SQLite (`WAL`, `2 MB` User-Space `PCache`) | `aydb` Virtual Table (On-Disk `idx_*.db` SSTables) | Relative Speed (`aydb` / SQLite) |
| :--- | ---: | ---: | ---: |
| **`Speedtest1`-Style Total (`Tests 100–240`)** | `57.21 ms` | **`79.23 ms`** | **`0.71×`** |
| **`YCSB`-Style `256 B` Workload C (100% Point Read)** | `316,242 ops/s` | **`919,219 ops/s`** | **`2.95×` faster** |
| **`YCSB`-Style `256 B` Workload B (95% Read / 5% Update)** | `306,087 ops/s` | **`730,588 ops/s`** | **`2.43×` faster** |
| **`TPC-B`-Style Buffered (`wal_buf.db` vs. `NORMAL`)** | `21,906 TPS` | **`80,864 TPS`** | **`3.77×` faster** |
| **`TPC-B`-Style Full Sync (`wal_sync.db` vs. `FULL`)** | `1,004 TPS` | `1,068 TPS` | `1.05×` |

### Architectural Analysis of On-Disk SSTable Performance
1. **Faster on `YCSB`-Style Point Reads and Buffered `TPC-B`-Style OLTP Even From On-Disk `idx_*.db` SSTables:**
   * Even when `memtable_capacity_bytes = 128 KB` forces keys out of RAM into on-disk `idx_<SeqID>.db` SSTables, in-heap **10-bit/key Bloom Filters**, **per-segment `[min_key, max_key]` bounds**, and **Sparse Fence Pointers** pinpoint the 4 KB SSTable block in a single positional `ReadFile(OVERLAPPED)` call, giving `2.43×`–`2.95×` the point-read throughput of SQLite and `3.77×` in buffered TPC-B. Full-sync TPC-B remains about equal (`1.05×`) because the device flush dominates.
2. **Why `Speedtest1`-Style Total Drops to `0.71×` at `128 KB` `MemTable` Capacity (OS Page Cache Syscall Overhead vs. In-Process `PCache`):**
   * Because `aydb` strictly bounds its process heap and delegates 4 KB data block caching to the **OS Page Cache** via `ReadFile(OVERLAPPED)` + per-block hardware `CRC32C` verification—with only a small verified block cache on top, rather than a page cache that holds the whole dataset like SQLite's `PCache`—every block read that misses that cache crosses the user/kernel boundary (`ReadFile` syscall) and verifies `block_crc32c`. On small datasets (~600 KB in `Speedtest1`-style) that fit 100% inside SQLite's `2 MB` in-process `PCache` with zero syscalls, forcing `aydb` to spill into multiple `128 KB` on-disk SSTable segments incurs kernel syscall and multi-segment K-way merge overhead during `Speedtest1`'s 10× full-table scans and 5,000-row `INSERT` PK uniqueness probes.
