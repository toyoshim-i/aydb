# Chapter 2.2: Comparative Benchmarks — Vanilla SQLite (`WAL`) vs. `aydb` Virtual Table

> Figures are medians of five runs at commit `6ae760e` on Windows 11 (local NTFS, Crucial P3 NVMe SSD).

This chapter documents the head-to-head empirical benchmarks (`examples/sqlite_vs_aydb_bench.rs`) comparing **Vanilla SQLite (`PRAGMA journal_mode = WAL`)** against **`aydb` mounted via the SQLite Virtual Table Adapter (`USING aydb(...)`)**.

To ensure an apples-to-apples comparison, **both engines execute identical parameterized SQL statements (`INSERT OR REPLACE INTO ...`, `SELECT ... WHERE ...`) through the exact same native SQLite C API (`sqlite3_prepare_v2` / `sqlite3_step`)** on Windows 11 (`winsqlite3.dll`).

---

## 1. Benchmark 1: Small Synchronous Configuration Updates (`128 B` Payload, Full Durability)

* **Workload:** `300` individual synchronous transactions (`INSERT OR REPLACE INTO app_config VALUES (?1, ?2)`) updating 50 configuration keys with `128 B` values.
* **Compared Configurations:**
  1. **Vanilla SQLite:** `PRAGMA journal_mode = WAL; PRAGMA synchronous = FULL;`
  2. **`aydb` Virtual Table:** `durability = 'sync'` (writes to `wal_sync.db` with `PhysicalZeroFill` pre-allocation, followed by `FlushFileBuffers` on every commit).

| Engine & Durability Mode | Throughput | P50 Latency | P99 Latency | Physical Bytes Written |
| :--- | ---: | ---: | ---: | ---: |
| **Vanilla SQLite (`WAL`, `synchronous = FULL`)** | `1,041 tx/s` | `949.2 µs` | `1,161.3 µs` | `3,769.8 KB` |
| **`aydb` Virtual Table (`wal_sync.db`, flushed per commit)** | `1,113 tx/s` (`1.07×`) | `893.1 µs` | `974.1 µs` | **`1,200.0 KB` (`3.1×` less)** |

### Architectural Root Cause Analysis
1. **Why Vanilla SQLite Takes `949 µs` per Sync Commit:**
   * Even for a `128 B` row, SQLite's B-Tree + WAL must write an entire 4 KB database page image + 24-byte WAL frame header to the `-wal` file, extend the `-wal` file's NTFS `ValidDataLength (VDL)` (triggering a synchronous NTFS `$LogFile` metadata transaction), and invoke `FlushFileBuffers()`, which stalls the calling thread until the full storage flush barrier completes.
2. **Why `aydb` Is About Equal:**
   * `aydb` pre-zeroes `wal_sync.db` (`8 MB`) at initialization so `VDL == EOF` permanently, and writes only a single 4 KB sector per transaction, so it writes about a third of the bytes. Both engines then wait for `FlushFileBuffers`, which dominates the commit on a consumer SSD, so throughput is about the same.

---

## 2. Benchmark 2: High-Churn HTTP Blob Cache (`16 KB` Blobs, `4.0 MB` Quota, `32.0 MB` Written)

* **Workload:** Continuously inserting `2,000` HTTP cache responses of **`16 KB` each (`32.0 MB` total)** via SQL into a table capped at **`4.0 MB` (`~256` live blobs — an `8×` full cache turnover)**.
* **Compared Configurations:**
  1. **Vanilla SQLite (`WAL`, `synchronous = NORMAL`):** Standard table `http_cache(url PRIMARY KEY, access_tick, body BLOB)` executing insert + LRU eviction inside a single transaction (`BEGIN ... COMMIT`) without an extra secondary index.
  2. **`aydb` Virtual Table (`max_bytes = 4194304`, `durability = 'buffered'`, `WalStreamMode::BufferedImmediateOsCache`):** Automatic WiscKey separation (`16 KB` blobs routed to `blob_lru_0001_<SeqID>.db` via 256 KB buffered `BlobSegmentWriter` staging), lazy LRU eviction, and crash-safe $O(1)$ copy-less blob segment deletion (syncing `wal_buf.db` before unlinking 100% evicted sealed segments).

| Engine & Cache Eviction Architecture | Sustained Write Speed | P50 Latency | P99 Latency | Final Data Footprint on Disk |
| :--- | ---: | ---: | ---: | ---: |
| **Vanilla SQLite (`WAL` + SQL LRU)** | `106.7 MB/s` | `78.1 µs` | `4,480.8 µs` | `8.15 MB` (`2.04×` quota bloat) |
| **`aydb` Virtual Table (WiscKey + Copy-less GC)** | **`174.6 MB/s` (`1.64×` faster)** | **`21.5 µs` (`3.63×` lower)** | **`2,277.6 µs` (`1.97×` lower)** | **`3.98 MB` (Strict `4.0 MB` cap)** |

### Architectural Root Cause Analysis
1. **B-Tree Overflow Page Churn vs. WiscKey Separation:**
   * In Vanilla SQLite, a `16 KB` blob spans 4 linked B-Tree overflow pages (`4 × 4 KB`). Inserting and deleting blobs continuously fragments the B-Tree freelist, writes full `16 KB` page images into both the `-wal` file and the main `.sqlite` database during WAL checkpoints (`4.5 ms` P99 stalls), and leaves the database + WAL footprint bloated at **`8.15 MB`** (`2.04×` the `4.0 MB` logical cap).
   * In `aydb`, `16 KB` blobs are staged and appended sequentially once to `blob_lru_0001_<SeqID>.db` via a 256 KB buffer, while the index and WAL store only a **23-byte `BlobPtr`**. When older `1.0 MB` `blob_lru` segments reach 100% evicted bytes, `aydb` ensures the eviction tombstones in `wal_buf.db` are durable and unlinks the segment file in $O(1)$ time with zero blob copying, keeping the physical data footprint at **`3.98 MB`**.

---

## 3. Benchmark 3: Read-Heavy Cache Hits with LRU Timestamp Tracking (`20,000` `SELECT` Queries)

* **Workload:** `20,000` SQL point-read hits (`SELECT body FROM http_cache WHERE url = ?1`) over `200` cached `4 KB` blobs (`~800 KB` working set in `blob_lru_0001_*.db`) while maintaining accurate LRU recency order across matching column schemas.
* **Compared Configurations:**
  1. **Vanilla SQLite:** `SELECT body FROM http_cache WHERE url = ?1` followed by `UPDATE http_cache SET access_tick = ?1 WHERE url = ?2` so SQLite's `idx_http_cache_tick` index reflects the read hit.
  2. **`aydb` Virtual Table:** `SELECT body FROM http_cache WHERE url = ?1` alone—because `xFilter` (Plan 1: `PLAN_PK_POINT`) calls `engine.get_row_into()`, which updates the two-level in-memory `AccessTracker` in $O(1)$ with zero heap allocations and zero disk writes!

| SQL Read + LRU Tracking Strategy | Read Throughput | Average Read Latency | Disk Write I/O Generated |
| :--- | ---: | ---: | ---: |
| **Vanilla SQLite (`SELECT` + `UPDATE access_tick`)** | `30,783 ops/s` | `32.49 µs` | `0.03 MB` WAL + B-Tree Page Dirtying |
| **`aydb` Virtual Table (`SELECT` w/ Lazy `AccessTracker`)** | **`653,932 ops/s` (`21.24×` faster)** | **`1.53 µs` (`21.2×` lower)** | **`0.00 MB` (Pure In-Memory Update)** |

---

## 4. Benchmark 4: Concurrent Synchronous Commits (`examples/concurrent_write_bench.rs`)

Figures are medians of five runs at commit `ffdb0de` on the same host. Both engines use a full device flush per durable commit (`synchronous = FULL` with `fullfsync` on SQLite, `durability = 'sync'` on `aydb`). The flush time on this SSD alternated between runs, so absolute figures vary by about a factor of two. Ratios are the median of the per-run ratios and are more stable.

### 4.1 Concurrent Writers (Autocommit `UPDATE`, `100 B` Values)

| Writer Connections | Vanilla SQLite | `aydb` Virtual Table | `aydb` `SQLITE_BUSY` | `aydb` / SQLite |
| ---: | ---: | ---: | ---: | ---: |
| **1** | `524 tx/s` | `547 tx/s` | `0` | `0.96×` |
| **2** | `1,070 tx/s` | **`1,522 tx/s`** | `0` | **`1.74×`** |
| **4** | `490 tx/s` | **`1,437 tx/s`** | `0` | **`2.93×`** |
| **8** | `413 tx/s` | **`2,110 tx/s`** | `0` | **`5.20×`** |

A single writer is about level with SQLite, because each commit still waits for one device flush. With more writers, SQLite performs one flush per commit, so its throughput does not rise. `aydb` covers every commit written before a flush starts with that one flush (`ARCHITECTURE.md` §4.5), so throughput rises with concurrency.

### 4.2 Point Reads From 4 Connections During Synchronous Commits

| Engine & Writer | Commits/s | Reads/s | P50 | P99 | P99.9 |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **Vanilla SQLite**, no writer | `0` | `661,837` | `5.2 µs` | `18.1 µs` | `41.0 µs` |
| **`aydb`**, no writer | `0` | **`2,001,071`** | **`1.8 µs`** | **`2.8 µs`** | **`8.6 µs`** |
| **Vanilla SQLite**, 1 synchronous writer | `538` | `339,521` | `9.1 µs` | `50.7 µs` | `138.1 µs` |
| **`aydb`**, 1 synchronous writer | `516` | **`2,217,183`** | **`1.6 µs`** | **`3.1 µs`** | **`28.6 µs`** |

`aydb` waits for the device flush without the engine lock, and readers keep using the committed state, so a synchronous writer leaves read throughput nearly unchanged.
