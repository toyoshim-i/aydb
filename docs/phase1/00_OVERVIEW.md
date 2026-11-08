# Chapter 1.0: Phase 1 Architecture Overview — Anatomy of the `aydb` Storage Engine

This document serves as the high-level architectural textbook for **Phase 1** of `aydb` (*All Your DataBase*). Before diving into byte-level specifications of individual subsystems, this chapter explains **why** the storage engine is structured the way it is, **how** its components interact across memory and disk tiers, and **how** data flows through the system during reads, writes, background compaction, and crash recovery.

---

## 1. The Storage Engine Dilemma: Why Not Just Use a B-Tree or Standard LSM?

Every storage engine is fundamentally a trade-off between three competing hardware realities: **Read Amplification** (how many bytes must be read from storage to answer a query), **Write Amplification** (how many bytes are written to storage per byte of logical application data), and **Space Amplification** (how much extra disk/memory space is consumed by fragmentation and obsolete versions).

### 1.1 Why B-Trees Struggle with Hybrid Desktop Workloads

Traditional embedded databases (such as SQLite) use an in-place update **B-Tree** organized into fixed-size pages (typically 4 KB).

1. **Random I/O and Page Amplification:** Updating a 50-byte setting inside a 4 KB B-Tree page requires rewriting the entire 4 KB page (a write amplification factor of $80\times$).
2. **Overflow Page Fragmentation with Blobs:** When storing a 500 KB HTTP cache entry alongside small rows, a B-Tree splits the payload across a linked list of 4 KB overflow pages. As cache entries expire and are replaced by differently sized blobs, free pages scatter across the file. Reading a single cached file degenerates into dozens of non-contiguous random seeks, defeating the operating system's sequential prefetcher.
3. **SMB & Antivirus Fragility:** B-Tree engines rely on shared-memory index files (`-shm`), rolling WAL truncation, or atomic file renames. Over SMB/CIFS shares or under Windows Defender / enterprise minifilter antivirus scanners, frequent metadata extensions and lock transitions break SMB caching leases and trigger `ERROR_SHARING_VIOLATION`.

### 1.2 Why Classic LSM-Trees Struggle with Large Values

**Log-Structured Merge-Trees (LSM-Trees)** (such as LevelDB and RocksDB) solve the random-write problem by buffering mutations in memory (**MemTable**) and flushing them sequentially as immutable sorted files (**SSTables**).

However, a classic LSM-Tree embeds both keys and values together inside the SSTables. As background **Compaction** merges SSTables from Level $L$ to Level $L+1$ to maintain sort order and reclaim deleted keys, **every multi-megabyte blob is repeatedly read and rewritten across disk**—often 10 to 30 times over its lifetime. For a high-churn HTTP cache, this causes catastrophic write amplification, saturating disk bandwidth and wearing out SSDs.

### 1.3 The `aydb` Solution: WiscKey Separation + OS-Native Caching

`aydb` resolves this tension by combining four core architectural principles:

```mermaid
flowchart LR
    subgraph App["Application Workload"]
        W1["Small Settings (< 1 KB)<br/>Random ACID Updates"]
        W2["Large Cache Blobs (>= 1 KB)<br/>High Churn + Quota Eviction"]
    end

    subgraph Engine["aydb WiscKey Split Engine"]
        MT["In-Memory MemTable<br/>(Keys + Inline Values + 20B Blob Pointers)"]
        WAL["Fixed-Size Epoch WAL<br/>(16 MB Pre-allocated Ring)"]
        IDX["Immutable Index Segments<br/>(idx_NNNN.db — 4 KB Blocks)"]
        BLOB["Immutable Append-Only Blobs<br/>(blob_std / blob_lru_NNNN.db)"]
    end

    W1 -->|"Inline Path"| WAL
    W1 -->|"Inline Path"| MT
    W2 -->|"1. Write-Blob-Before-WAL"| BLOB
    W2 -->|"2. 20B Pointer Only"| WAL
    W2 -->|"3. 20B Pointer Only"| MT
    MT -->|"Sequential Flush"| IDX
    IDX -.->|"20B Pointer Dereference"| BLOB
```

1. **WiscKey-Style Key-Value Separation:** Small values (`< 1 KB`) are stored inline inside the sorted index so point lookups finish in a single read. Large values (`>= 1 KB`) bypass the LSM compaction tree entirely: they are written once to append-only `blob_*.db` files, while the index stores only a **20-byte pointer** (`BlobSegID`, `Offset`, `Length`, `CRC32C`). Compaction only merges keys and 20-byte pointers.
2. **Strict Heap Budget via OS Page Cache Delegation:** Instead of maintaining a multi-hundred-megabyte user-space Buffer Pool (which duplicates pages already cached by the OS kernel—known as *Double Caching*), `aydb` keeps only **Sparse Fence Pointers** and **Bloom Filters** in process heap. Actual 4 KB data blocks are read on demand from the Windows Cache Manager (Standby List) via positional I/O.
3. **Zero-Allocation, Rename-Free File Lifecycle:** WAL files are pre-allocated to a fixed 16 MB size and recycled using `WalEpoch` counters without truncating or zero-filling. Active segments are switched atomically via an 8 KB ping-pong metadata file (`boot_meta.db`), never calling `MoveFileEx` (rename).
4. **Copy-Less Per-Table LRU Eviction:** Tables with `MAX_BYTES` quotas isolate their blobs into per-table generational files (`blob_lru_<TblID>_<SeqID>.db`). Read hits coalesce timestamp updates in RAM without disk writes; once an old blob generation becomes 100% evicted, the entire file is unlinked in $O(1)$ without copying a single byte.

---

## 2. Memory Hierarchy & Physical File Layout

A critical part of understanding a storage engine is knowing **what lives where** across the three hardware/OS boundaries:
1. **Process Private Heap (User Space RAM)** — Strictly bounded (e.g., 8–16 MB total). Lost on process termination (`kill -9`).
2. **OS Page Cache / System Cache (Kernel Space RAM)** — Managed by Windows Cache Manager (in 256 KB Virtual Address Control Block [VACB] views) or Linux/macOS page cache. Survives process termination; lost only on OS kernel panic or power outage.
3. **Non-Volatile Storage (Physical Disk / Remote SMB Server)** — Survives power outages once flushed via `FILE_FLAG_WRITE_THROUGH` or `FlushFileBuffers` / `fdatasync`.

```mermaid
flowchart TB
    subgraph Heap["1. Process Private Heap (Strictly Bounded: ~8-16 MB)"]
        direction LR
        ActiveMT["Writable MemTable<br/>(BTreeMap / SkipList, <= 4 MB)"]
        ImmMT["Immutable MemTable<br/>(Awaiting Flush)"]
        SparseIdx["Segment Catalog in RAM<br/>- Sparse Fence Pointers (1st key per 4 KB block)<br/>- Bloom Filters (~10 bits/key)"]
        LruBuf["LRU AccessTick Coalescer<br/>(Bounded Ring/Hash Buffer, <= 512 KB)"]
    end

    subgraph OSCache["2. OS Page Cache (Windows Cache Manager / 256 KB VACB)"]
        direction LR
        WalCache["Buffered WAL Sectors<br/>(wal_buf.db)"]
        IdxCache["Hot 4 KB Index Blocks<br/>(RANDOM_ACCESS hint)"]
        BlobCache["Prefetched 256 KB Blob Windows<br/>(SEQUENTIAL_SCAN hint)"]
    end

    subgraph Disk["3. Non-Volatile Disk / Exclusive SMB Share (dwShareMode = 0)"]
        direction LR
        BootMeta["boot_meta.db (8 KB)<br/>Slot A (4 KB) | Slot B (4 KB)"]
        WalFile["wal_sync.db (2 MB) & wal_buf.db (16 MB)<br/>4 KB Sector Rings with WalEpoch"]
        IdxFiles["idx_<SeqID>.db<br/>Immutable Sorted 4 KB Blocks"]
        BlobFiles["blob_std_<SeqID>.db<br/>blob_lru_<TblID>_<SeqID>.db"]
    end

    ActiveMT -->|"Flush when full"| ImmMT
    ImmMT -->|"Write 4 KB blocks"| IdxFiles
    SparseIdx -->|"Binary search Fence Pointers,<br/>then pread(4 KB)"| IdxCache
    IdxCache <-->|"OS Paging"| IdxFiles
    BlobCache <-->|"256 KB Stride Read-Ahead"| BlobFiles
    WalCache <-->|"Background / Sync Flush"| WalFile
```

### Why Separate `RANDOM_ACCESS` and `SEQUENTIAL_SCAN` Files?

Inside the Windows kernel, the **Cache Manager** maps open files into kernel virtual address space using **256 KB views (VACBs)**. When a file is opened:
* **`FILE_FLAG_RANDOM_ACCESS` (`idx_<SeqID>.db`):** Instructs the Cache Manager *not* to perform speculative read-ahead when a 4 KB block is read, and discourages aggressive unmapping of views so frequently accessed index blocks stay hot in the Standby List.
* **`FILE_FLAG_SEQUENTIAL_SCAN` (`blob_*_<SeqID>.db`):** Instructs the Cache Manager (`CcCopyRead`) to prefetch the next 256 KB window asynchronously in the background as soon as sequential progress is detected, and to aggressively move already-read pages to the standby tail once read so multi-megabyte blobs do not evict hot index pages from RAM.

If index blocks and large blobs were interleaved inside a single file (as in SQLite), every random index lookup and sequential blob read would fight over the file's single read-ahead tracker and pollute the same 256 KB VACB views.

---

## 3. End-to-End Lifecycle Walkthroughs

To see how all Phase 1 subsystems cooperate, let us trace the four fundamental operations of the engine.

### 3.1 Walkthrough 1: The Write Path (`put` / `batch_write`)

When an application writes a key-value pair (or an atomic batch of mutations), the engine branches based on payload size (`INLINE_THRESHOLD = 1024 bytes`):

```mermaid
sequenceDiagram
    autonumber
    actor App as Application
    participant Eng as StorageEngine
    participant Blob as Active Blob Segment<br/>(blob_*_SeqID.db)
    participant WAL as Active WAL<br/>(wal_00.db)
    participant MT as Writable MemTable

    App->>Eng: put(key, value, DurabilityMode)
    alt Value >= 1 KB (Out-of-Line Blob)
        Note over Eng,Blob: Step 1: Write-Blob-Before-WAL Protocol
        Eng->>Blob: Append [Magic | CRC32C | Key | Value] (Buffered Write)
        Blob-->>Eng: Return BlobPtr { seg_id, offset, len, crc32c }
        Note over Eng,WAL: Step 2: Record only 20-byte BlobPtr in WAL
        Eng->>WAL: Append WAL Record [Key | Type=BlobPtr | BlobPtr]
    else Value < 1 KB (Inline)
        Eng->>WAL: Append WAL Record [Key | Type=Inline | Value]
    end

    alt DurabilityMode == Sync (Critical Settings)
        Eng->>WAL: Write 4 KB padded sector to wal_sync.db (WRITE_THROUGH)
    else DurabilityMode == Buffered (Cache / High-Freq State)
        Eng->>WAL: Write to wal_buf.db (Enters OS Page Cache immediately)
    end

    Eng->>MT: Insert (Key -> InlineValue or BlobPtr)
    Eng-->>App: Ok(())
```

#### Deep Dive: Why `Write-Blob-Before-WAL` Avoids Double `fsync`
In a naive KV-separated engine, guaranteeing that a pointer in the WAL never points to uninitialized garbage in the blob file would require:
1. Write to `blob.db` $\rightarrow$ call `fsync(blob.db)` (Disk Stall #1)
2. Write pointer to `wal.db` $\rightarrow$ call `fsync(wal.db)` (Disk Stall #2)

Two `fsync` calls per write would halve throughput. Instead, `aydb` writes to `blob.db` using a fast buffered write **before** writing to `wal.db`, and embeds the blob's expected `BlobCRC32C` inside the WAL record (and index pointer).
* In the normal case, the OS flushes dirty pages in arrival order, or both live in the OS cache.
* If a sudden power outage occurs after the WAL sector hits disk but before the `blob.db` buffered write reaches physical media, recovery (or a subsequent read) recomputes the CRC of the target byte range in `blob.db`. Seeing either a truncated file or a CRC mismatch against `BlobCRC32C`, `aydb` safely treats that specific record as an incomplete/uncommitted write rather than failing with database corruption! This applies at recovery only; a blob that fails verification during a normal read after open is reported as corruption per the table's `on_corrupt` policy (see `docs/ARCHITECTURE.md` §4.3).

---

### 3.2 Walkthrough 2: The Read Path (`get`)

Because `aydb` is an LSM-based engine, the latest version of a key may reside in the in-memory MemTable or in one of several immutable `idx_<SeqID>.db` segments on disk. Reads always search from **newest to oldest** and stop at the first match:

```mermaid
flowchart TD
    Start(["get(key)"]) --> CheckMT{"1. In Writable or<br/>Immutable MemTable?"}
    CheckMT -->|"Found Tombstone"| RetNone1(["Return Ok(None)"])
    CheckMT -->|"Found Inline Value"| TouchLRU1["Coalesce AccessTick in RAM<br/>(if LRU Table)"] --> RetVal1(["Return Ok(Some(val))"])
    CheckMT -->|"Found BlobPtr"| ReadBlob["Read Blob from blob_*_SeqID.db<br/>& Verify BlobCRC32C"]

    CheckMT -->|"Not Found"| LoopSeg["2. Iterate Active idx_SeqID.db<br/>(Newest SeqID -> Oldest SeqID)"]
    LoopSeg --> CheckBF{"Key in Segment's<br/>In-Heap Bloom Filter?"}
    CheckBF -->|"No (Definite Miss)"| NextSeg{"More Segments?"}
    NextSeg -->|"Yes"| LoopSeg
    NextSeg -->|"No"| RetNone2(["Return Ok(None)"])

    CheckBF -->|"Maybe (Probabilistic Hit)"| FenceSearch["Binary Search In-Heap<br/>Sparse Fence Pointers"]
    FenceSearch --> PRead["pread(4 KB Block) from OS Page Cache<br/>& Verify Block CRC32C"]
    PRead --> BlockSearch{"Binary Search Inside<br/>4 KB Block"}
    BlockSearch -->|"Not Found (False Positive)"| NextSeg
    BlockSearch -->|"Found Tombstone"| RetNone1
    BlockSearch -->|"Found Inline Value"| TouchLRU1
    BlockSearch -->|"Found BlobPtr"| ReadBlob

    ReadBlob --> VerifyCRC{"CRC Valid &<br/>Range Exists?"}
    VerifyCRC -->|"Yes"| TouchLRU1
    VerifyCRC -->|"No (Unflushed/Evicted)"| RetNone3(["Return Ok(None) (Safe Miss)"])
```

#### Deep Dive: How Sparse Fence Pointers Keep Heap Usage Under 1%
Suppose an `idx_<SeqID>.db` file is **64 MB** on disk, consisting of $16,384$ blocks of **4 KB** each.
* Instead of loading 64 MB into heap, `aydb` stores an array of $16,384$ entries containing only the **first key of each 4 KB block** (`Sparse Fence Pointer`).
* If keys average 24 bytes, $16,384 \times 24\text{ B} \approx 393\text{ KB}$ of heap memory indexes the entire 64 MB file!
* Adding a **Bloom Filter** at 10 bits per key (~1% false positive rate) allows point lookups (`get`) to skip any `idx_<SeqID>.db` segment that does not contain the target key without issuing a single system call.

---

### 3.3 Walkthrough 3: MemTable Flush, Ping-Pong Metadata Update, and Crash Recovery

When the active `MemTable` reaches its size cap (e.g., 4 MB) or the 16 MB `wal_00.db` fills up, the engine performs a **Checkpoint / Flush**:

```text
+-----------------------------------------------------------------------------------+
|                        boot_meta.db (Fixed 8 KB on Disk)                          |
+-----------------------------------------+-----------------------------------------+
|           Slot A (0 .. 4096)            |           Slot B (4096 .. 8192)         |
|  Magic: "AYDB"                          |  Magic: "AYDB"                          |
|  Generation: 41 (Older)                 |  Generation: 42 (Current Active)        |
|  ActiveWalEpoch: 7                      |  ActiveWalEpoch: 8                      |
|  ActiveIdxList: [101, 102]              |  ActiveIdxList: [101, 102, 103]         |
|  PendingDeleteList: []                  |  PendingDeleteList: []                  |
|  HeaderCRC32C / PayloadCRC32C: Valid    |  HeaderCRC32C / PayloadCRC32C: Valid    |
+-----------------------------------------+-----------------------------------------+
```

1. **Write New Index Segment:** The immutable MemTable is written sequentially as 4 KB blocks to a newly allocated `idx_103.db`, followed by `sync_barrier()`.
2. **Ping-Pong `boot_meta.db` Switch:** If Slot A currently holds `Generation = 41` and Slot B holds `Generation = 40`, the engine formats the new metadata state (`Generation = 42`, adding `idx_103.db` to `ActiveIdxList` and advancing `ActiveWalEpoch` to `8`) and overwrites **Slot B** at offset `4096` in a single 4 KB aligned write, followed by `sync_durable()`.
   * **Why Ping-Pong Works:** Even if power fails mid-write while updating Slot B (corrupting Slot B's CRC), Slot A (`Generation = 41`) remains completely untouched on disk! On reboot, the engine reads both slots, discards Slot B due to CRC mismatch, boots cleanly from Slot A, and replays `wal_00.db` (`Epoch 7`) to reconstruct the exact MemTable that produced `idx_103.db`.
3. **Zero-Fill-Free WAL Reuse:** Now that `ActiveWalEpoch` is `8` in `boot_meta.db`, the next write to `wal_00.db` wraps back to offset `0` and writes sectors tagged with `WalEpoch = 8`. No `SetEndOfFile` (truncation) or 16 MB zero-fill is needed: any sector past the current write cursor still has `WalEpoch = 7` from the previous cycle, which recovery immediately recognizes as the end of the log.

---

### 3.4 Walkthrough 4: Background Compaction & Copy-Less LRU Eviction

Over time, two background maintenance tasks keep read performance high and disk usage bounded:

1. **Index Compaction (Merging `idx_<SeqID>.db`):**
   * Multiple `idx` segments are merge-sorted into a new `idx_<NewSeqID>.db`, discarding overwritten key versions and tombstones (`DELETE` markers).
   * Because large values live in `blob_*.db`, compaction only copies keys, small inline values, and 20-byte blob pointers.
   * Once the new `idx` segment is registered in `boot_meta.db`, the old `idx` segments are closed (`CloseHandle`) and deleted (`DeleteFile`). If an antivirus filter temporarily blocks deletion, their IDs stay in `PendingDeleteList` inside `boot_meta.db` until a subsequent checkpoint succeeds.
2. **Copy-Less LRU Eviction (For Capped Tables):**
   * When a table exceeds its `MAX_BYTES` quota, the engine scans its system LRU index (`[TableID | 'l' | AccessTick | PK]`) from the oldest `AccessTick` forward.
   * Before evicting a candidate row, the engine checks the **In-Memory `AccessTick` Coalescer**: if the row was recently read via `SELECT` (meaning its in-memory `LatestTick` is newer than the on-disk `AccessTick`), its LRU index entry is lazily updated instead of evicting the row!
   * Otherwise, the row and its secondary index entries are deleted, and the referenced `blob_lru_<TblID>_<SeqID>.db` segment's `DeadBytes` counter is incremented. When all blobs in a `blob_lru` segment are dead, the segment file is closed and deleted in $O(1)$ with zero compaction I/O.

---

## 4. Phase 1 Crate & Module Architecture

To keep the implementation modular, testable in isolation, and educational, Phase 1 is organized into five strictly layered internal subsystems inside the `aydb` crate:

```mermaid
flowchart BT
    subgraph L1["Layer 1: OS & Platform Abstraction (src/pal/)"]
        PAL["PlatformFile Trait (Win32 OVERLAPPED / Unix pread-pwrite)<br/>Path Normalizer (\\\\?\\) & Handle Cache Manager"]
    end

    subgraph L2["Layer 2: Crash-Safe Persistence Primitives (src/meta/, src/wal/)"]
        META["boot_meta.db Ping-Pong Manager<br/>& Deferred GC Queue"]
        WAL["Fixed-Size Epoch WAL Writer/Reader<br/>(Dual-Handle Sync/Buffered + Fragmentation)"]
    end

    subgraph L3["Layer 3: Immutable Segment Storage (src/blob/, src/index/)"]
        BLOB["Append-Only Blob Segment Manager<br/>(Standard & Per-Table LRU Segments + DeadBytes Tracking)"]
        IDX["4 KB Block SSTable Builder & Reader<br/>(Sparse Fence Pointers + Bloom Filter)"]
    end

    subgraph L4["Layer 4: In-Memory State & Eviction (src/memtable/, src/lru/)"]
        MT["Bounded MemTable<br/>(MVCC Sequence / Tombstone / ValueOrBlobPtr)"]
        LRU["Memcomparable Key Codec &<br/>In-Memory AccessTick Coalescer + Quota Evictor"]
    end

    subgraph L5["Layer 5: Coordinator & Public API (src/engine/)"]
        ENG["StorageEngine Coordinator<br/>(get / put / delete / scan_prefix / batch_write / Compaction / Recovery)"]
    end

    L1 --> L2
    L1 --> L3
    L2 --> L5
    L3 --> L5
    L4 --> L5
    L3 --> L4
```

### Detailed Design Roadmap (Upcoming Chapters)

We will design, review, and implement Phase 1 one chapter at a time in bottom-up order:

| Step | Textbook Chapter | Core Topics to Design & Discuss |
| :--- | :--- | :--- |
| **Step 1-1** | `docs/phase1/01_PAL_AND_OS_CACHE.md` | Win32 `CreateFileW` flags (`dwShareMode=0`, `OVERLAPPED`), SMB RWH Lease mechanics, `hWalBuffered` vs. `hWalSync` cache coherency on Windows, `\\?\` UNC path normalization, POSIX `pread`/`pwrite`/`fcntl` equivalents, and FD LRU pooling. |
| **Step 1-2** | `docs/phase1/02_BOOT_META_AND_GC.md` | Exact 4096-byte binary layout of `boot_meta.db` slots, CRC32C placement, handling variable-length segment lists within a fixed 4 KB budget, atomic generation promotion, and rename-free deferred GC state machine. |
| **Step 1-3** | `docs/phase1/03_EPOCH_WAL_AND_RECOVERY.md` | 4 KB WAL sector header layout, spanning records larger than 4 KB (`FIRST`/`MIDDLE`/`LAST` fragments), `WalEpoch` wrap-around invariants, torn-write detection, and deterministic crash recovery replay. |
| **Step 1-4** | `docs/phase1/04_WISCKEY_AND_SEGMENTS.md` | `blob_*.db` record framing, `idx_*.db` 4 KB data block internal layout (prefix compression / restart points vs. offset array), trailer block, Bloom Filter construction, Sparse Fence Pointer index, MemTable flushing, and background compaction. |
| **Step 1-5** | `docs/phase1/05_CAPPED_LRU_AND_KV_API.md` | Memcomparable key encoding rules, `AccessTick` coalescing ring/hash buffer sizing, lazy promotion during eviction scans, copy-less `blob_lru` unlinking, and the public Phase 1 Rust API. |
