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
        MT["Writable MemTable (src/memtable/)<br/>Inline Values (< 1 KB) + 23-Byte BlobPtrs (>= 1 KB)"]
        FENCE["Sparse Fence Pointers & Bloom Filters (src/index/)<br/>1st Key per 4 KB Block + 10 bits/key Filter"]
        LRU["Zero-I/O LRU AccessTracker (src/lru/)<br/>Coalesces SELECT Timestamp Hits in RAM"]
    end

    subgraph Disk["OS Cache Manager & Non-Volatile Storage (dwShareMode = 0)"]
        META["boot_meta.db (8 KB Ping-Pong)<br/>Rename-Free Atomic Manifest + Deferred GC"]
        WAL["Dual-Stream Epoch WAL (src/wal/)<br/>wal_sync.db (2 MB WRITE_THROUGH) & wal_buf.db (16 MB Buffered)"]
        IDX["idx_<SeqID>.db (RANDOM_ACCESS; SEQUENTIAL_SCAN in Compaction)<br/>Immutable 4 KB Offset-Indexed SSTables"]
        BLOB["blob_std_<SeqID>.db & blob_lru_<TblID>_<SeqID>.db<br/>Append-Only Blobs (Random Point Read + Buffered Append)"]
    end

    VTAB --> KVAPI
    KVAPI --> MT
    KVAPI --> FENCE
    KVAPI --> LRU
    MT -->|"Multi-Sector Coalesced Commit"| WAL
    MT -->|"Sequential Flush"| IDX
    FENCE -->|"Single 4 KB pread"| IDX
    KVAPI -->|"Write-Blob-Before-WAL"| BLOB
    IDX -.->|"23-Byte BlobPtr Dereference"| BLOB
```

* **WiscKey-Style Key-Value Separation:** Values `< 1 KB` are stored inline inside the sorted index so point lookups finish in a single read. Values `>= 1 KB` are appended directly to immutable `blob_*.db` files using a **`Write-Blob-Before-WAL`** protocol protected by end-to-end `CRC32C`, while the WAL and `idx_*.db` store only a **23-byte `BlobPtr`**. Index compaction never copies large blobs.
* **Strict Heap Budget via OS Page Cache Delegation:** Instead of duplicating kernel page cache memory inside a custom user-space buffer pool, `aydb` retains only **Sparse Fence Pointers** (the first key of each 4 KB block) and **10-bit/key Bloom Filters** in heap memory (~0.75% of index size), issuing direct positional `ReadFile` (`OVERLAPPED`) / `pread` calls backed by the Windows Cache Manager.
* **Dual-Stream Fixed-Size Epoch WAL (`wal_sync.db` & `wal_buf.db`):** Pre-allocated fixed-size ring buffers recycled via monotonically increasing `WalEpoch` headers without file truncation or zero-fill stalls. Separating `wal_sync.db` (`FILE_FLAG_WRITE_THROUGH`) from `wal_buf.db` allows synchronous configuration commits to complete in **~75 µs** without inadvertently flushing megabytes of dirty buffered cache pages.
* **Rename-Free Atomic Metadata & Antivirus Resilience:** Never invokes `MoveFileEx` (atomic rename), which frequently triggers `ERROR_SHARING_VIOLATION` under Windows Defender or Samba shares. Segment switches are committed atomically via an 8 KB double-buffered (`Slot A` / `Slot B`) `boot_meta.db` with a persistent `PendingDeleteList` for deferred garbage collection.
* **Copy-Less Per-Table LRU Eviction & Zero-Write `SELECT` Tracking:** Tables configured with `max_bytes` isolate their blobs into per-table generational files (`blob_lru_<TblID>_<SeqID>.db`). Read hits coalesce access timestamps in RAM (`AccessTracker`) with **zero disk writes**, and when an older blob generation becomes 100% evicted, the entire file is unlinked in $O(1)$ without copying a single live byte.

---

## Benchmark Highlights vs. Vanilla SQLite (`WAL` Mode)

All benchmarks compare `aydb` (exercised through its SQLite `sqlite3_module` Virtual Table adapter over identical SQL queries) against native Vanilla SQLite (`PRAGMA journal_mode = WAL`) on the same Windows NTFS host with **equivalent durability guarantees** (`WalStreamMode::BufferedImmediateOsCache` writing every commit to the OS page cache vs. SQLite `PRAGMA synchronous = NORMAL`, and `WriteDurability::SyncTransactional` on `wal_sync.db` vs. SQLite `PRAGMA synchronous = FULL`) after passing a **pre-benchmark differential SQL result/state verification suite** (`verify_differential_sql_equivalence` in `examples/standard_sql_bench.rs` covering mixed numeric types, column affinities, `COLLATE NOCASE`/`RTRIM`, failed `UNIQUE` constraint statements, `SAVEPOINT`/`ROLLBACK TO`, cross-connection snapshot isolation, and mixed native-SQLite + `aydb` 2PC transactions):

<p align="center">
  <img src="docs/assets/bench_speedup_overview.svg" alt="aydb Relative Speedup Overview vs Vanilla SQLite WAL" width="100%"/>
</p>

<details>
<summary><b>📊 View Detailed Benchmark Comparison Charts (YCSB-Style A–F, Speedtest1-Style Breakdown, TPC-B-Style &amp; Hybrid Workloads)</b></summary>
<br/>
<p align="center">
  <img src="docs/assets/bench_ycsb_throughput.svg" alt="YCSB-Style Workloads A-F Throughput Comparison" width="100%"/>
</p>
<p align="center">
  <img src="docs/assets/bench_speedtest1_breakdown.svg" alt="SQLite Speedtest1-Style Execution Time Breakdown" width="100%"/>
</p>
<p align="center">
  <img src="docs/assets/bench_hybrid_and_tpcb.svg" alt="Hybrid Desktop Workloads and TPC-B-Style Comparison" width="100%"/>
</p>
</details>

> **Dataset Scale, Workload Scope, & Failure-Model Transparency:**
> * **Workload Coverage:** `examples/standard_sql_bench.rs` implements selected **`Speedtest1`-style** (`Tests 100–240`), **`YCSB`-style** (`Workloads A–F`), and **`TPC-B`-style** (`pgbench`-like 4-table OLTP) workloads rather than full certified/official benchmark suites.
> * **Warm OS Cache vs. Larger-Than-RAM Datasets:** With the default `memtable_capacity_bytes = 4 MB`, datasets whose index footprint is `< 4 MB` (`Speedtest1`-style `5,000` rows $\approx 600\text{ KB}$, `YCSB`-style `256 B` `4,000` rows $\approx 1.1\text{ MB}$, `TPC-B`-style `2,000` accounts $\approx 300\text{ KB}$) serve index lookups from the in-memory `MemTable` (`BTreeMap`) while logging every commit to the OS page cache (`wal_buf.db`) or non-volatile media (`wal_sync.db`). Lowering `memtable_capacity_bytes = 128 KB` forces `idx_*.db` SSTable flushes and size-tiered compactions ([Chapter 2.3 §4](docs/phase2/03_STANDARD_SQL_BENCHMARKS.md)), exercising positional `ReadFile(OVERLAPPED)` calls against warm OS page cache; establishing cold-storage or larger-than-RAM behavior requires multi-gigabyte datasets beyond these quick-running examples.
> * **Local Host & Simulated Fault Model:** Reported figures are single-host local Windows NTFS measurements. Power-loss and I/O fault tests in `examples/reliability_bench.rs` use deterministic in-memory simulation (`FaultInjectingFs`) and do not substitute for physical power-cut hardware testing or live SMB network-outage/reconnection testing.

| Benchmark Workload | Workload & Durability / Scale Note | `aydb` (`4 MB` Default / `128 KB` SSTable) | Vanilla SQLite (`WAL`) | Speedup (`4 MB` / `SSTable`) |
| :--- | :--- | ---: | ---: | ---: |
| **`Speedtest1`-Style Suite** | Selected 5,000-row SQL workload (`Tests 100–240`, `BufferedImmediateOsCache` vs `NORMAL`, ~600 KB) | `67.55 ms` (`205.99 ms` SSTable) | `62.95 ms` (`64.13 ms`) | `0.93×` (`0.31×` SSTable) |
| └─ *Speedtest1 Test 130* | 5,000 Primary Key Point `SELECT`s (`MemTable` `BTreeMap` vs SQLite `PCache`) | **`5.33 ms`** (`938K qps`) | `14.69 ms` (`340K qps`) | **`2.76×` faster** |
| └─ *Speedtest1 Test 140* | 5,000 Secondary Index `SELECT`s (`WHERE uval = ?`) | **`7.58 ms`** (`660K qps`) | `15.97 ms` (`313K qps`) | **`2.11×` faster** |
| └─ *Speedtest1 Test 160* | 1,000 PK Range Scans (`20` rows each) | **`4.66 ms`** (`215K qps`) | `5.34 ms` (`187K qps`) | **`1.15×` faster** |
| **YCSB-Style Workload A** | 50% Read / 50% Update (`256 B` inline / `2 KB` WiscKey `blob_std_*.db`) | **`299K`** / `85.9K ops/s` | `221K` / `155K ops/s` | **`1.35×`** / `0.55×` |
| **YCSB-Style Workload B** | 95% Read / 5% Update (`256 B` inline [SSTable: **`362K`**] / `2 KB` WiscKey) | **`887K`** / `153K ops/s` | `291K` / `184K ops/s` | **`3.05×` [`1.24×` SST]** / `0.83×` |
| **YCSB-Style Workload C** | 100% Point Read (`256 B` inline [SSTable: **`435K`**] / `2 KB` WiscKey) | **`1,208K`** / `188K ops/s` | `303K` / `192K ops/s` | **`3.98×` [`1.44×` SST]** / `0.98×` |
| **YCSB-Style Workload D** | 95% Read Latest / 5% Insert (`256 B` inline / `2 KB` WiscKey `blob_std_*.db`) | **`890K` / `167K ops/s`** | `206K` / `105K ops/s` | **`4.32×` / `1.60×` faster** |
| **YCSB-Style Workload E** | 95% Short Range / 5% Insert (`256 B` inline / `2 KB` WiscKey, 256-row chunk) | `105K` / `0.93K ops/s` | `107K` / `42.8K ops/s` | `0.98×` / `0.02×` |
| **YCSB-Style Workload F** | 50% Read / 50% Read-Modify-Write (`256 B` inline / `2 KB` WiscKey `blob_std_*.db`) | **`264K`** / `70.0K ops/s` | `176K` / `121K ops/s` | **`1.50×`** / `0.58×` |
| **TPC-B-Style (`FULL` Sync)** | 4-Table OLTP Debit/Credit (`wal_sync.db` `WRITE_THROUGH` vs `synchronous=FULL`) | **`13,817 TPS`** (**`9,969`** SSTable) | `529 TPS` (`439` SST) | **`26.12×` (`22.73×` SSTable)** |
| **TPC-B-Style (`Buffered`)** | 4-Table OLTP Debit/Credit (`wal_buf.db` `ImmediateOsCache` vs `synchronous=NORMAL`) | **`110,257 TPS`** (**`45,774`** SSTable) | `15,576 TPS` (`15,506` SST) | **`7.08×` (`2.95×` SSTable)** |
| **Hybrid Config Sync** | 300 Synchronous `128 B` Settings Updates (`wal_sync.db` vs `synchronous=FULL`) | **`15,322 tx/s`** (`56.6 µs` P50) | `1,020 tx/s` (`978.6 µs` P50) | **`15.0×` faster** |
| **Bounded Blob Cache** | 2,000 × `16 KB` Churn (`32.0 MB` written, `4.0 MB` Quota Enforcement) | **`154.6 MB/s`** (`3.98 MB` disk) | `86.5 MB/s` (`8.15 MB` disk) | **`1.79×` faster (`0.49×` space)** |
| **LRU Cache-Hit Reads** | 20,000 `SELECT`s with Active LRU Access Tracking (`4 KB` blobs in `blob_lru_*.db`) | **`215,506 ops/s`** (`0.00 MB` RAM) | `27,376 ops/s` (`0.03 MB` WAL) | **`7.87×` faster** |

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
* **[Chapter 1.4: WiscKey Blob Separation, 4 KB Index Segments, & MemTable](docs/phase1/04_WISCKEY_AND_SEGMENTS.md)** — `Write-Blob-Before-WAL` consistency protocol, append-only `blob_*.db` framing, offset-indexed 4 KB SSTable blocks (`idx_*.db`), Sparse Fence Pointers, double-hashed Bloom Filters, and copy-free blob compaction.
* **[Chapter 1.5: Capped Tables (`MAX_BYTES`), Zero-Write LRU, & Ordered KV API](docs/phase1/05_CAPPED_LRU_AND_KV_API.md)** — Memcomparable key encoding (`src/index/key_codec.rs`), bounded in-memory `AccessTracker` coalescing, lazy promotion during quota eviction, copy-less `blob_lru` segment unlinking, and the `StorageEngine` API.

### Phase 2: SQLite Virtual Table Adapter & Comparative Benchmarks (`docs/phase2/`)
* **[Chapter 2.0: Phase 2 Overview](docs/phase2/00_OVERVIEW.md)** — Bridging SQLite's VDBE bytecode engine to `aydb` via `sqlite3_module` and zero-dependency `winsqlite3.dll` / `libsqlite3` FFI.
* **[Chapter 2.1: SQLite Virtual Table Adapter, Row Codec, & Constraint Pushdown](docs/phase2/01_SQLITE_VTAB_ADAPTER.md)** — $O(1)$ offset-indexed binary `RowCodec`, zero-copy FFI column projection (`project_column_zero_copy`), `xBestIndex` constraint/`LIMIT`/`ORDER BY` pushdown, reusable contiguous cursor byte slabs (`AydbCursor::slab`), lock-free reversible 8-byte `ROWID` encoding, and `xBegin`/`xCommit`/`xRollback` transaction coalescing.
* **[Chapter 2.2: Hybrid Workload Benchmarks vs. Vanilla SQLite](docs/phase2/02_SQLITE_VS_AYDB_BENCHMARKS.md)** — Deep-dive evaluation of synchronous configuration updates, `MAX_BYTES`-bounded 16 KB blob caching, and read-heavy LRU access tracking.
* **[Chapter 2.3: Selected Standard SQL Benchmark Workloads (`Speedtest1`-Style, `YCSB`-Style, `TPC-B`-Style)](docs/phase2/03_STANDARD_SQL_BENCHMARKS.md)** — Pre-benchmark differential SQL equivalence verification, methodology, results, and architectural root-cause analysis across `Speedtest1`-style, `YCSB`-style (`Workloads A–F`), and `TPC-B`-style workloads.

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
│   ├── util/                                 # Shared utilities (CRC32C, ByteReader, test helpers)
│   │   ├── mod.rs
│   │   ├── crc32c.rs
│   │   ├── byte_reader.rs
│   │   └── test_util.rs
│   ├── pal/                                  # Platform Abstraction Layer (Win32 OVERLAPPED / Unix pread)
│   ├── meta/mod.rs                           # 8 KB Ping-Pong boot_meta.db & Deferred GC
│   ├── wal/mod.rs                            # Dual-Stream Fixed-Size Epoch WAL & Recovery
│   ├── blob/mod.rs                           # Append-only Standard & Per-Table LRU Blob Segments
│   ├── index/                                # 4 KB Block SSTables, Key Codec, Sparse Fence Pointers & Bloom Filter
│   │   ├── mod.rs
│   │   ├── bloom.rs
│   │   └── key_codec.rs
│   ├── memtable/mod.rs                       # Bounded In-Memory B-Tree MemTable
│   ├── lru/mod.rs                            # Zero-I/O AccessTracker & Capped Table Metadata
│   ├── engine/                               # StorageEngine Coordinator, Transactions, Scans, Compaction & Eviction
│   │   ├── mod.rs
│   │   ├── tx.rs
│   │   ├── scan.rs
│   │   ├── compaction.rs
│   │   ├── eviction.rs
│   │   └── tests.rs
│   └── vtab/                                 # SQLite Virtual Table Adapter
│       ├── mod.rs
│       ├── ddl.rs
│       ├── rowid_map.rs
│       ├── connection.rs
│       ├── row_codec.rs
│       ├── ffi.rs
│       └── tests.rs
└── examples/
    ├── pal_bench.rs                          # Step 1-1: OS Cache & Positional I/O microbenchmarks
    ├── meta_wal_bench.rs                     # Step 1-2/1-3: Ping-Pong Meta & Dual-Stream WAL benchmarks
    ├── wisckey_bench.rs                      # Step 1-4: WiscKey vs. Inline LSM Compaction benchmarks
    ├── lru_engine_bench.rs                   # Step 1-5: Zero-I/O LRU & Quota Eviction benchmarks
    ├── reliability_bench.rs                  # Crash-Consistency, Fault-Injection, & Audit Regression Suite
    ├── sqlite_vs_aydb_bench.rs               # Step 2-2: Hybrid Workload SQLite vs. aydb benchmarks
    └── standard_sql_bench.rs                 # Step 2-3: Differential SQL check & Speedtest1/YCSB/TPC-B-style benchmarks
```

---

## Quick Start: Running Tests & Benchmarks

### 1. Run Unit, Integration, & Crash-Consistency Regression Suites
`aydb` includes comprehensive unit, crash-recovery, torn-write, compaction, LRU eviction, SQLite Virtual Table integration tests, and a deterministic fault-injection / audit regression benchmark suite (`reliability_bench`):

```powershell
cargo test
cargo run --release --example reliability_bench
```

### 2. Run Comparative SQL Benchmarks (Phase 2)
Compare `aydb` against Vanilla SQLite (`WAL` mode) on your machine:

```powershell
# Differential SQL Verification + Selected Speedtest1-Style, YCSB-Style (A-F), and TPC-B-Style Workloads
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
