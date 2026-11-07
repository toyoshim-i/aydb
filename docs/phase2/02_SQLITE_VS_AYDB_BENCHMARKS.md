# Chapter 2.2: Comparative Benchmarks — Vanilla SQLite (`WAL`) vs. `aydb` Virtual Table

This chapter documents the head-to-head empirical benchmarks (`examples/sqlite_vs_aydb_bench.rs`) comparing **Vanilla SQLite (`PRAGMA journal_mode = WAL`)** against **`aydb` mounted via the SQLite Virtual Table Adapter (`USING aydb(...)`)**.

To ensure an apples-to-apples comparison, **both engines execute identical parameterized SQL statements (`INSERT OR REPLACE INTO ...`, `SELECT ... WHERE ...`) through the exact same native SQLite C API (`sqlite3_prepare_v2` / `sqlite3_step`)** on Windows 11 (`winsqlite3.dll`).

<p align="center">
  <img src="../assets/bench_hybrid_and_tpcb.svg" alt="Hybrid Desktop Workloads and TPC-B Comparison" width="100%"/>
</p>

---

## 1. Benchmark 1: Small Synchronous Configuration Updates (`128 B` Payload, Full Durability)

* **Workload:** `300` individual synchronous transactions (`INSERT OR REPLACE INTO app_config VALUES (?1, ?2)`) updating 50 configuration keys with `128 B` values.
* **Compared Configurations:**
  1. **Vanilla SQLite:** `PRAGMA journal_mode = WAL; PRAGMA synchronous = FULL;`
  2. **`aydb` Virtual Table:** `durability = 'sync'` (writes to `wal_sync.db` opened with `FILE_FLAG_WRITE_THROUGH` + `PhysicalZeroFill` pre-allocation).

| Engine & Durability Mode | Throughput | P50 Latency | P99 Latency | Physical Bytes Written |
| :--- | ---: | ---: | ---: | ---: |
| **Vanilla SQLite (`WAL`, `synchronous = FULL`)** | `1,032 tx/s` | `964.0 µs` | `1,140.4 µs` | `3,769.8 KB` |
| **`aydb` Virtual Table (`wal_sync.db` `WRITE_THROUGH`)** | **`15,467 tx/s` (`15.0×` faster)** | **`51.8 µs` (`18.6×` lower)** | **`131.5 µs` (`8.7×` lower)** | **`1,200.0 KB` (`3.1×` less)** |

### Architectural Root Cause Analysis
1. **Why Vanilla SQLite Takes `964 µs` per Sync Commit:**
   * Even for a `128 B` row, SQLite's B-Tree + WAL must write an entire 4 KB database page image + 24-byte WAL frame header to the `-wal` file, extend the `-wal` file's NTFS `ValidDataLength (VDL)` (triggering a synchronous NTFS `$LogFile` metadata transaction), and invoke `FlushFileBuffers()`, which stalls the calling thread until the full storage flush barrier completes.
2. **Why `aydb` Completes in `51.8 µs` (`15.0×` Faster):**
   * `aydb` pre-zeroes `wal_sync.db` (`2 MB`) at initialization so `VDL == EOF` permanently, and writes only a single 4 KB sector per transaction directly through `FILE_FLAG_WRITE_THROUGH` (`Force Unit Access` + `sync_barrier()`) without mutating NTFS file-length metadata or invoking a full-handle `FlushFileBuffers()` barrier.

---

## 2. Benchmark 2: High-Churn HTTP Blob Cache (`16 KB` Blobs, `4.0 MB` Quota, `32.0 MB` Written)

* **Workload:** Continuously inserting `2,000` HTTP cache responses of **`16 KB` each (`32.0 MB` total)** via SQL into a table capped at **`4.0 MB` (`~256` live blobs — an `8×` full cache turnover)**.
* **Compared Configurations:**
  1. **Vanilla SQLite (`WAL`, `synchronous = NORMAL`):** Standard table `http_cache(url PRIMARY KEY, access_tick, body BLOB)` executing insert + LRU eviction inside a single transaction (`BEGIN ... COMMIT`) without an extra secondary index.
  2. **`aydb` Virtual Table (`max_bytes = 4194304`, `durability = 'buffered'`, `WalStreamMode::BufferedImmediateOsCache`):** Automatic WiscKey separation (`16 KB` blobs routed to `blob_lru_0001_<SeqID>.db` via 256 KB buffered `BlobSegmentWriter` staging `P-03`), lazy LRU eviction, and crash-safe $O(1)$ copy-less blob segment deletion (`AUD-06`: syncing `wal_buf.db` before unlinking 100% evicted sealed segments).

| Engine & Cache Eviction Architecture | Sustained Write Speed | P50 Latency | P99 Latency | Final Data Footprint on Disk |
| :--- | ---: | ---: | ---: | ---: |
| **Vanilla SQLite (`WAL` + SQL LRU)** | `92.6 MB/s` | `92.0 µs` | `5,100.5 µs` | `8.15 MB` (`2.04×` quota bloat) |
| **`aydb` Virtual Table (WiscKey + Copy-less GC)** | **`154.1 MB/s` (`1.66×` faster)** | **`25.7 µs` (`3.58×` lower)** | **`2,487.7 µs` (`2.05×` lower)** | **`3.98 MB` (Strict `4.0 MB` cap)** |

### Architectural Root Cause Analysis
1. **B-Tree Overflow Page Churn vs. WiscKey Separation:**
   * In Vanilla SQLite, a `16 KB` blob spans 4 linked B-Tree overflow pages (`4 × 4 KB`). Inserting and deleting blobs continuously fragments the B-Tree freelist, writes full `16 KB` page images into both the `-wal` file and the main `.sqlite` database during WAL checkpoints (`5.1 ms` P99 stalls), and leaves the database + WAL footprint bloated at **`8.15 MB`** (`2.04×` the `4.0 MB` logical cap).
   * In `aydb`, `16 KB` blobs are staged and appended sequentially once to `blob_lru_0001_<SeqID>.db` (`P-03`), while the index and WAL store only a **23-byte `BlobPtr`**. When older `1.0 MB` `blob_lru` segments reach 100% evicted bytes, `aydb` ensures the eviction tombstones in `wal_buf.db` are durable (`AUD-06`) and unlinks the segment file in $O(1)$ time with zero blob copying, keeping the physical data footprint at **`3.98 MB`**.

---

## 3. Benchmark 3: Read-Heavy Cache Hits with LRU Timestamp Tracking (`20,000` `SELECT` Queries)

* **Workload:** `20,000` SQL point-read hits (`SELECT body FROM http_cache WHERE url = ?1`) over `200` cached `4 KB` blobs (`~800 KB` working set in `blob_lru_0001_*.db`) while maintaining accurate LRU recency order across matching column schemas.
* **Compared Configurations:**
  1. **Vanilla SQLite:** `SELECT body FROM http_cache WHERE url = ?1` followed by `UPDATE http_cache SET access_tick = ?1 WHERE url = ?2` so SQLite's `idx_http_cache_tick` index reflects the read hit.
  2. **`aydb` Virtual Table:** `SELECT body FROM http_cache WHERE url = ?1` alone—because `xFilter` (Plan 1: `PLAN_PK_POINT`) calls `engine.get_row_into()`, which updates the two-level in-memory `AccessTracker` (`P-04`) in $O(1)$ with zero heap allocations and zero disk writes!

| SQL Read + LRU Tracking Strategy | Read Throughput | Average Read Latency | Disk Write I/O Generated |
| :--- | ---: | ---: | ---: |
| **Vanilla SQLite (`SELECT` + `UPDATE access_tick`)** | `27,526 ops/s` | `36.33 µs` | `0.03 MB` WAL + B-Tree Page Dirtying |
| **`aydb` Virtual Table (`SELECT` w/ Lazy `AccessTracker`)** | **`222,659 ops/s` (`8.09×` faster)** | **`4.49 µs` (`8.09×` lower)** | **`0.00 MB` (Pure In-Memory Update)** |
