# aydb — *All Your DataBase*

**Windows/SMB-Optimized Embedded Storage Engine & Lightweight SQL Runtime in Zero-Dependency Rust**

`aydb` is an embedded storage engine designed from first principles to unify two workloads that traditionally conflict in desktop and edge applications:
1. **High-Frequency ACID State & Configuration Updates:** Small records (tens to hundreds of bytes) requiring immediate crash durability (`FILE_FLAG_WRITE_THROUGH`) without flushing multi-megabyte buffered cache pages or stalling on random B-Tree page writes.
2. **Large-Scale Bounded Blob Caching (`MAX_BYTES` + LRU):** Multi-kilobyte to multi-megabyte payloads (such as HTTP caches or media assets) experiencing high churn, strict quota enforcement, and read-heavy access—where traditional B-Trees suffer from overflow-page fragmentation and classic LSM-Trees suffer from compaction write amplification.

Built with **zero external runtime crate dependencies**, `aydb` is engineered specifically for the realities of **Windows NTFS, SMB/CIFS network shares (roaming profiles / folder redirection), and enterprise antivirus minifilter drivers**, while providing clean cross-platform portability to Linux and macOS.

---

## Key Architectural Highlights

```mermaid
flowchart TB
    subgraph SQL["SQL & API Layer"]
        VTAB["SQLite Virtual Table Adapter (src/vtab/)<br/>Zero-Copy FFI Column Projection + Constraint/LIMIT Pushdown"]
        KVAPI["Native Ordered KV & Table API (src/engine/)<br/>get_row / put_row / scan_table / lookup_by_secondary_index"]
    end

    subgraph Memory["Bounded Process Heap (~4-8 MB Budget)"]
        MT["Writable MemTable (src/memtable/)<br/>Inline Values (< 1 KB) + 20-Byte BlobPtrs (>= 1 KB)"]
        FENCE["Sparse Fence Pointers & Bloom Filters (src/index/)<br/>1st Key per 4 KB Block + 10 bits/key Filter"]
        LRU["Zero-I/O LRU AccessTracker (src/lru/)<br/>Coalesces SELECT Timestamp Hits in RAM"]
    end

    subgraph Disk["OS Cache Manager & Non-Volatile Storage (dwShareMode = 0)"]
        META["boot_meta.db (8 KB Ping-Pong)<br/>Rename-Free Atomic Manifest + Deferred GC"]
        WAL["Dual-Stream Epoch WAL (src/wal/)<br/>wal_sync.db (2 MB WRITE_THROUGH) & wal_buf.db (16 MB Buffered)"]
        IDX["idx_<SeqID>.db (RANDOM_ACCESS)<br/>Immutable 4 KB Prefix-Compressed SSTables"]
        BLOB["blob_std_<SeqID>.db & blob_lru_<TblID>_<SeqID>.db<br/>Append-Only Blobs (SEQUENTIAL_SCAN / 256 KB Read-Ahead)"]
    end

    VTAB --> KVAPI
    KVAPI --> MT
    KVAPI --> FENCE
    KVAPI --> LRU
    MT -->|"Multi-Sector Coalesced Commit"| WAL
    MT -->|"Sequential Flush"| IDX
    FENCE -->|"Single 4 KB pread"| IDX
    KVAPI -->|"Write-Blob-Before-WAL"| BLOB
    IDX -.->|"20-Byte BlobPtr Dereference"| BLOB
```

* **WiscKey-Style Key-Value Separation:** Values `< 1 KB` are stored inline inside the sorted index so point lookups finish in a single read. Values `>= 1 KB` are appended directly to immutable `blob_*.db` files using a **`Write-Blob-Before-WAL`** protocol protected by end-to-end `CRC32C`, while the WAL and `idx_*.db` store only a **20-byte `BlobPtr`**. Index compaction never copies large blobs.
* **Strict Heap Budget via OS Page Cache Delegation:** Instead of duplicating kernel page cache memory inside a custom user-space buffer pool, `aydb` retains only **Sparse Fence Pointers** (the first key of each 4 KB block) and **10-bit/key Bloom Filters** in heap memory (~0.75% of index size), issuing direct positional `ReadFile` (`OVERLAPPED`) / `pread` calls backed by the Windows Cache Manager.
* **Dual-Stream Fixed-Size Epoch WAL (`wal_sync.db` & `wal_buf.db`):** Pre-allocated fixed-size ring buffers recycled via monotonically increasing `WalEpoch` headers without file truncation or zero-fill stalls. Separating `wal_sync.db` (`FILE_FLAG_WRITE_THROUGH`) from `wal_buf.db` allows synchronous configuration commits to complete in **~75 µs** without inadvertently flushing megabytes of dirty buffered cache pages.
* **Rename-Free Atomic Metadata & Antivirus Resilience:** Never invokes `MoveFileEx` (atomic rename), which frequently triggers `ERROR_SHARING_VIOLATION` under Windows Defender or Samba shares. Segment switches are committed atomically via an 8 KB double-buffered (`Slot A` / `Slot B`) `boot_meta.db` with a persistent `PendingDeleteList` for deferred garbage collection.
* **Copy-Less Per-Table LRU Eviction & Zero-Write `SELECT` Tracking:** Tables configured with `max_bytes` isolate their blobs into per-table generational files (`blob_lru_<TblID>_<SeqID>.db`). Read hits coalesce access timestamps in RAM (`AccessTracker`) with **zero disk writes**, and when an older blob generation becomes 100% evicted, the entire file is unlinked in $O(1)$ without copying a single live byte.

---

## Benchmark Highlights vs. Vanilla SQLite (`WAL` Mode)

All benchmarks compare `aydb` (exercised through its SQLite `sqlite3_module` Virtual Table adapter over identical SQL queries) against native Vanilla SQLite (`PRAGMA journal_mode = WAL`) on the same Windows NTFS host:

<p align="center">
  <img src="docs/assets/bench_speedup_overview.svg" alt="aydb Relative Speedup Overview vs Vanilla SQLite WAL" width="100%"/>
</p>

<details>
<summary><b>📊 View Detailed Benchmark Comparison Charts (YCSB A–F, Speedtest1 Breakdown, TPC-B &amp; Hybrid Workloads)</b></summary>
<br/>
<p align="center">
  <img src="docs/assets/bench_ycsb_throughput.svg" alt="YCSB Workloads A-F Throughput Comparison" width="100%"/>
</p>
<p align="center">
  <img src="docs/assets/bench_speedtest1_breakdown.svg" alt="SQLite Official Speedtest1 Execution Time Breakdown" width="100%"/>
</p>
<p align="center">
  <img src="docs/assets/bench_hybrid_and_tpcb.svg" alt="Hybrid Desktop Workloads and TPC-B Comparison" width="100%"/>
</p>
</details>

| Benchmark Suite | Workload Description | `aydb` | Vanilla SQLite (`WAL`) | Speedup |
| :--- | :--- | ---: | ---: | ---: |
| **SQLite `Speedtest1`** | Official 5,000-row full SQL workload suite (Tests 100–240 total) | **`63.12 ms`** | `70.06 ms` | **`1.11×` faster** |
| └─ *Speedtest1 Test 130* | 5,000 Primary Key Point `SELECT`s | **`4.92 ms`** (`1,016K qps`) | `16.54 ms` (`302K qps`) | **`3.37×` faster** |
| └─ *Speedtest1 Test 140* | 5,000 Secondary Index `SELECT`s (`WHERE b = ?`) | **`7.31 ms`** (`684K qps`) | `18.65 ms` (`268K qps`) | **`2.55×` faster** |
| └─ *Speedtest1 Test 160* | 1,000 PK Range Scans (`20` rows each) | **`4.15 ms`** (`241K qps`) | `6.04 ms` (`166K qps`) | **`1.46×` faster** |
| **YCSB Workload A** | 50% Read / 50% Update (`256 B` inline / `2 KB` WiscKey) | **`278K`** / `81.8K ops/s` | `209K` / `134K ops/s` | **`1.33×`** / `0.61×` |
| **YCSB Workload B** | 95% Read / 5% Update (`256 B` inline / `2 KB` WiscKey) | **`872K`** / `148K ops/s` | `259K` / `153K ops/s` | **`3.37×`** / `0.97×` |
| **YCSB Workload C** | 100% Point Read (`256 B` inline / `2 KB` WiscKey) | **`1,216K`** / `171K ops/s` | `275K` / `172K ops/s` | **`4.42×`** / `1.00×` |
| **YCSB Workload D** | 95% Read Latest / 5% Insert (`256 B` inline / `2 KB` WiscKey) | **`748K` / `162K ops/s`** | `191K` / `94.3K ops/s` | **`3.90×` / `1.72×` faster** |
| **YCSB Workload E** | 95% Short Range / 5% Insert (`256 B` inline / `2 KB` WiscKey) | **`225K`** / `20.6K ops/s` | `96.6K` / `40.6K ops/s` | **`2.33×`** / `0.51×` |
| **YCSB Workload F** | 50% Read / 50% Read-Modify-Write (`256 B` inline / `2 KB` WiscKey) | **`240K`** / `67.2K ops/s` | `154K` / `107K ops/s` | **`1.56×`** / `0.63×` |
| **TPC-B (`FULL` Sync)** | 4-Table OLTP Debit/Credit (`wal_sync.db` vs `synchronous=FULL`) | **`10,668 TPS`** (`93.7 µs`) | `566 TPS` (`1,767 µs`) | **`18.85×` faster** |
| **TPC-B (`Buffered`)** | 4-Table OLTP Debit/Credit (`wal_buf.db` vs `synchronous=NORMAL`) | **`112,610 TPS`** (`8.9 µs`) | `15,604 TPS` (`64.1 µs`) | **`7.22×` faster** |
| **Hybrid Config Sync** | 300 Synchronous `128 B` Settings Updates | **`12,523 tx/s`** (`78.8 µs` P50) | `535 tx/s` (`1,858.0 µs` P50) | **`23.4×` faster** |
| **Bounded Blob Cache** | 2,000 × `16 KB` Churn (`4.0 MB` Quota Enforcement) | **`167.1 MB/s`** (`3.98 MB` disk) | `69.0 MB/s` (`8.15 MB` disk) | **`2.42×` faster (`0.5×` space)** |
| **LRU Cache-Hit Reads** | 20,000 `SELECT`s with Active LRU Access Tracking | **`193,852 ops/s`** (`0.00 MB` RAM) | `24,036 ops/s` (`0.03 MB` WAL) | **`8.06×` faster** |

---

## Documentation: The `aydb` "Living Textbook" (`docs/`)

This repository is structured as a **"Build-to-Learn" Living Textbook**. Every subsystem is accompanied by a dedicated chapter in [`docs/`](docs/) featuring architectural motivation, Mermaid state and sequence diagrams, RFC-style ASCII byte layouts, crash-safety proofs, and reproducible empirical benchmarks.

### Master Architecture Specification
* **[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)** — High-level requirements, single-host exclusive SMB handle model (`dwShareMode = 0`), OS Cache Manager file segregation, WiscKey separation, crash recovery protocols, and 3-phase roadmap.

### Phase 1: Core Storage Engine & Ordered KV API (`docs/phase1/`)
* **[Chapter 1.0: Phase 1 Architecture Overview](docs/phase1/00_OVERVIEW.md)** — Why B-Trees and classic LSM-Trees struggle on hybrid desktop workloads; memory hierarchy across Process Heap, OS Page Cache (256 KB VACBs), and Disk; end-to-end read, write, flush, and compaction walkthroughs.
* **[Chapter 1.1: PAL (`PlatformFile`) & OS Cache Interaction](docs/phase1/01_PAL_AND_OS_CACHE.md)** — Win32 `OVERLAPPED` positional I/O, SMB `Read-Write-Handle (RWH)` lease preservation, `\\?\` extended-length path normalization, `RANDOM_ACCESS` vs. `SEQUENTIAL_SCAN` kernel hints, and POSIX equivalents.
* **[Chapter 1.2: `boot_meta.db` Ping-Pong Manifest & Deferred GC](docs/phase1/02_BOOT_META_AND_GC.md)** — 8 KB double-buffered (`Slot A` / `Slot B`) atomic metadata layout, dual `CRC32C` verification, generation promotion, and rename-free deferred garbage collection (`PendingDeleteList`).
* **[Chapter 1.3: Dual-Stream Fixed-Size Epoch WAL & Crash Recovery](docs/phase1/03_EPOCH_WAL_AND_RECOVERY.md)** — Pre-allocated 4 KB sector rings (`wal_sync.db` & `wal_buf.db`), `WalEpoch` wrap-around without zero-filling, multi-sector record fragmentation (`Full`/`First`/`Middle`/`Last`), multi-sector batch coalescing, and deterministic LSN merge-sort recovery.
* **[Chapter 1.4: WiscKey Blob Separation, 4 KB Index Segments, & MemTable](docs/phase1/04_WISCKEY_AND_SEGMENTS.md)** — `Write-Blob-Before-WAL` consistency protocol, append-only `blob_*.db` framing, prefix-compressed 4 KB SSTable blocks (`idx_*.db`), Sparse Fence Pointers, double-hashed Bloom Filters, and copy-free blob compaction.
* **[Chapter 1.5: Capped Tables (`MAX_BYTES`), Zero-Write LRU, & Ordered KV API](docs/phase1/05_CAPPED_LRU_AND_KV_API.md)** — Memcomparable key encoding (`src/lru/key_codec.rs`), bounded in-memory `AccessTracker` coalescing, lazy promotion during quota eviction, copy-less `blob_lru` segment unlinking, and the `StorageEngine` API.

### Phase 2: SQLite Virtual Table Adapter & Comparative Benchmarks (`docs/phase2/`)
* **[Chapter 2.0: Phase 2 Overview](docs/phase2/00_OVERVIEW.md)** — Bridging SQLite's VDBE bytecode engine to `aydb` via `sqlite3_module` and zero-dependency `winsqlite3.dll` / `libsqlite3` FFI.
* **[Chapter 2.1: SQLite Virtual Table Adapter, Row Codec, & Constraint Pushdown](docs/phase2/01_SQLITE_VTAB_ADAPTER.md)** — $O(1)$ offset-indexed binary `RowCodec`, zero-copy FFI column projection (`project_column_zero_copy`), `xBestIndex` constraint/`LIMIT`/`ORDER BY` pushdown, reusable contiguous cursor byte slabs (`AydbCursor::slab`), lock-free reversible 8-byte `ROWID` encoding, and `xBegin`/`xCommit`/`xRollback` transaction coalescing.
* **[Chapter 2.2: Hybrid Workload Benchmarks vs. Vanilla SQLite](docs/phase2/02_SQLITE_VS_AYDB_BENCHMARKS.md)** — Deep-dive evaluation of synchronous configuration updates, `MAX_BYTES`-bounded 16 KB blob caching, and read-heavy LRU access tracking.
* **[Chapter 2.3: Industry-Standard SQL Benchmark Suite (`Speedtest1`, `YCSB`, `TPC-B`)](docs/phase2/03_STANDARD_SQL_BENCHMARKS.md)** — Methodology, results, and architectural root-cause analysis across SQLite's official `Speedtest1`, Yahoo! Cloud Serving Benchmark (`Workloads A–F`), and `TPC-B`.

---

## Repository Structure

```text
aydb/
├── Cargo.toml
├── README.md
├── docs/
│   ├── ARCHITECTURE.md                       # Master architecture specification
│   ├── phase1/                               # Phase 1: Storage Engine textbook (Chapters 1.0 - 1.5)
│   └── phase2/                               # Phase 2: SQLite VTab & Benchmarks (Chapters 2.0 - 2.3)
├── src/
│   ├── lib.rs                                # Crate root & module exports
│   ├── util/crc32c.rs                        # Castagnoli CRC32C checksum implementation
│   ├── pal/                                  # Platform Abstraction Layer (Win32 OVERLAPPED / Unix pread)
│   ├── meta/mod.rs                           # 8 KB Ping-Pong boot_meta.db & Deferred GC
│   ├── wal/mod.rs                            # Dual-Stream Fixed-Size Epoch WAL & Recovery
│   ├── blob/mod.rs                           # Append-only Standard & Per-Table LRU Blob Segments
│   ├── index/                                # 4 KB Block SSTables, Sparse Fence Pointers & Bloom Filter
│   ├── memtable/mod.rs                       # Bounded In-Memory B-Tree MemTable
│   ├── lru/                                  # Memcomparable Key Codec & Zero-I/O AccessTracker
│   ├── engine/mod.rs                         # StorageEngine Coordinator, Compaction, & Table API
│   └── vtab/                                 # SQLite Virtual Table Adapter (ffi, row_codec, mod)
└── examples/
    ├── pal_bench.rs                          # Step 1-1: OS Cache & Positional I/O microbenchmarks
    ├── meta_wal_bench.rs                     # Step 1-2/1-3: Ping-Pong Meta & Dual-Stream WAL benchmarks
    ├── wisckey_bench.rs                      # Step 1-4: WiscKey vs. Inline LSM Compaction benchmarks
    ├── lru_engine_bench.rs                   # Step 1-5: Zero-I/O LRU & Quota Eviction benchmarks
    ├── sqlite_vs_aydb_bench.rs               # Step 2-2: Hybrid Workload SQLite vs. aydb benchmarks
    └── standard_sql_bench.rs                 # Step 2-3: Speedtest1, YCSB (A-F), & TPC-B SQL benchmarks
```

---

## Quick Start: Running Tests & Benchmarks

### 1. Run Unit & Integration Tests
`aydb` includes comprehensive unit, crash-recovery, torn-write, compaction, LRU eviction, and SQLite Virtual Table integration tests:

```powershell
cargo test
```

### 2. Run Comparative SQL Benchmarks (Phase 2)
Compare `aydb` against Vanilla SQLite (`WAL` mode) on your machine:

```powershell
# Industry-Standard SQL Suite: Speedtest1, YCSB (Workloads A-F), and TPC-B
cargo run --release --example standard_sql_bench

# Hybrid Desktop Workloads: Sync Config Updates, Bounded 16 KB Blob Cache, & LRU Reads
cargo run --release --example sqlite_vs_aydb_bench
```

### 3. Run Storage Subsystem Microbenchmarks (Phase 1)

```powershell
cargo run --release --example pal_bench
cargo run --release --example meta_wal_bench
cargo run --release --example wisckey_bench
cargo run --release --example lru_engine_bench
```

---

## Roadmap Status

- [x] **Phase 1:** Core Storage Engine in Rust & Low-Level Ordered KV API (`pal`, `meta`, `wal`, `blob`, `index`, `memtable`, `lru`, `engine`)
- [x] **Phase 2:** SQLite Virtual Table Adapter (`sqlite3_module`) & Comparative Benchmark Verification (`Speedtest1`, `YCSB A–F`, `TPC-B`, Hybrid Workloads)
- [ ] **Phase 3:** Native Lightweight SQL Frontend (Self-contained SQL Parser + Deterministic Rule-Based Optimizer + Volcano/Pipeline Iterator Execution Engine with native `WITH (MAX_BYTES = ..., EVICTION = 'LRU')` DDL)
