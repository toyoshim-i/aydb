# aydb — Architecture & Design Specification

*Windows/SMB-Optimized Embedded Storage Engine & Lightweight SQL*

---

## 1. Background and Target Requirements

`aydb` (*All Your DataBase*) is an embedded storage engine designed to handle both persistent application state (immediate synchronization of small settings and states) and large-scale caching (blob management with automatic eviction, such as HTTP caches) within a single unified, high-performance, crash-safe foundation for desktop applications.

### 1.1 Core Requirements

* **Hybrid Workload Support:**
  * **Settings & State Management:** Small records (tens to hundreds of bytes), random updates, durability on return for `SyncTransactional` commits.
  * **Cache Management:** Medium-to-large payloads (blobs ranging from a few KB to several MB), high churn rates, and automatic LRU purging governed by per-table logical size limits (`MAX_BYTES`).
* **Strict Heap Memory Bounds:**
  * The private working set (heap) consumed by the database engine itself must remain strictly bounded within a fixed budget of a few megabytes to tens of megabytes. Caching of index blocks and data payloads is delegated to the operating system's page cache (e.g., Windows Cache Manager).
* **Optimization for Windows FS (NTFS) and SMB (CIFS / Samba):**
  * Must operate without performance degradation or data corruption not only on local NTFS volumes, but also over SMB/CIFS shares common in enterprise folder redirection, roaming profiles, and WSL/container mounts.
* **Cross-Platform Portability:**
  * Built upon a design tailored for the strictest constraints (Windows/SMB), while isolating OS-specific primitives behind a minimal low-level Platform Abstraction Layer (PAL / VFS) so that Linux (`ext4`/`XFS`) and macOS (`APFS`) also achieve near-theoretical peak performance.

---

## 2. System Architecture and Filesystem Strategy

### 2.1 Single-Host Exclusive Handle Model

* **Core Assumption:** Even when the underlying storage resides on an SMB share, all concurrent access is assumed to originate from a **single host** (simultaneous access from other hosts is safely rejected via exclusive sharing mode `dwShareMode = 0`).
* **Preserving SMB RWH Leases:**
  * Database files are opened at startup with `dwShareMode = 0` (complete exclusivity) and kept open (never calling `CloseHandle` until a segment is retired).
  * This allows the client OS to continuously hold a `Read-Write-Handle (RWH) Lease` from the SMB server, eliminating network round-trip times (RTT) and serving reads and buffered writes entirely from the local RAM cache.
* **Future Multi-Process Extension (Gateway / Multiplexer Architecture):**
  * While primarily implemented as an in-process library, the engine is structured so that if multiple worker processes need concurrent access in the future, a single gateway process can hold the exclusive file handles and communicate zero-copy with workers over anonymous shared memory (`CreateFileMapping(INVALID_HANDLE_VALUE)`) and local IPC (Named Pipes / `WaitOnAddress`).

### 2.2 File Segregation Aligned with OS Prefetcher (Cache Manager) Boundaries

To maximize the effectiveness of the Windows Cache Manager (which maps files in 256 KB Virtual Address Control Block [VACB] views and tracks stride read-ahead per file stream), physical files are strictly segregated by access pattern and mutability.

| File Category | Naming Convention | OS Open Flags (Win / Unix) | Role & Lifecycle |
| :--- | :--- | :--- | :--- |
| **Boot Metadata** | `boot_meta.db` | Standard (`WRITE_THROUGH`, 4 KB-aligned) | Fixed 8 KB size (4 KB × 2 ping-pong buffer). Stores active segment ID lists, table statistics, and pending GC lists. |
| **Sync Journal (WAL)** | `wal_sync.db` | `OVERLAPPED` + `WRITE_THROUGH` | Fixed 2 MB size (physically pre-zeroed). Logs critical synchronous mutations without flushing buffered cache pages. |
| **Buffered Journal (WAL)** | `wal_buf.db` | `OVERLAPPED` + `SEQUENTIAL_SCAN` | Fixed 16 MB size (pre-allocated). Sequentially logs high-frequency buffered updates in OS cache without disk stalls. |
| **Index Layer** | `idx_<SeqID>.db` | `RANDOM_ACCESS` / `FADV_RANDOM` | Immutable sorted key-value file (SSTable format). Stores keys, small inline column values, and pointers to out-of-line blobs. |
| **Standard Blob Layer** | `blob_std_<SeqID>.db` | Standard (`OVERLAPPED` / positional I/O) | Append-only immutable file storing values larger than the inline threshold (e.g., 1 KB). Opened without `SEQUENTIAL_SCAN` to avoid 256 KB read-ahead amplification on random point reads. |
| **LRU Blob Layer** | `blob_lru_<TblID>_<SeqID>.db` | Standard (`OVERLAPPED` / positional I/O) | Append-only blob file dedicated to a specific LRU-capped table. Enables whole-generation file deletion (Copy-less Eviction) while optimizing random point reads. |

### 2.3 Rename-Free & Deferred GC File Management (Samba / Antivirus Resilience)

On Windows systems with antivirus minifilter drivers or on Samba shares, renaming (`MoveFileEx`) or deleting (`DeleteFile`) files that have recently been accessed or remain open frequently triggers `ERROR_SHARING_VIOLATION` or `STATUS_DELETE_PENDING`. `aydb` eliminates these failure modes completely through three rules:

1. **Monotonically Increasing Sequence IDs (No Filename Reuse):**
   Every `idx` and `blob` segment receives a monotonically increasing 64-bit `SeqID`. Once a filename is used, it is never reused.
2. **Rename-Free Pointer Switching:**
   When a flush or compaction completes, no file renaming or atomic file replacement is performed. Instead, the active segment ID list inside `boot_meta.db` is atomically updated.
3. **Close-Before-Delete and Deferred GC Queue:**
   Obsolete segments have all in-process handles explicitly closed (`CloseHandle`) before `DeleteFile` is invoked. If deletion fails (e.g., because an antivirus scanner is momentarily inspecting the file), the error is ignored and the segment ID is recorded in the `PendingDeleteList` of `boot_meta.db` to be retried at the next checkpoint or startup.

---

## 3. Core Storage Engine Design (KV Separation + Low-Memory Index)

### 3.1 WiscKey-Style Key-Value Separation

To support both fast random updates of small configuration records and zero write amplification for multi-megabyte HTTP cache payloads, values are stored differently based on size:

* **Inline Storage (`Value < 1 KB`):**
  Stored directly alongside the key inside the MemTable and `idx_<SeqID>.db`. A single index lookup retrieves both the key and the value.
* **Out-of-Line Blob Storage (`Value >= 1 KB`):**
  The payload is appended first to `blob_*_<SeqID>.db`, while the index stores only a compact 23-byte pointer structure:
  `[Kind (1B) | TableID (2B) | SegID (8B) | Offset (4B) | Length (4B) | ValueCRC32C (4B)]`.
  Consequently, index compaction never copies large blob payloads.

### 3.2 Two-Tier Index Management Under Strict Heap Limits

* **Writable MemTable (In-Memory, Fixed Size Cap: e.g., 4 MB – 8 MB):**
  An in-memory B-Tree or SkipList absorbing the latest writes. Once it reaches its memory cap, it is flushed to disk as an immutable `idx_<SeqID>.db` segment.
* **Immutable Index Segments (OS Cache-Backed):**
  On-disk `idx_<SeqID>.db` files are organized into 4 KB blocks. The database process keeps **only the first key of each 4 KB block (Sparse Fence Pointers) and a Bloom Filter in heap memory (consuming only a few hundred KB to a few MB)**, maintaining no custom in-process buffer pool for data blocks.
  During lookups, the engine locates the target block offset via the fence pointers and issues a direct `ReadFile` (`pread`) call, reading the 4 KB block straight from the OS page cache (Windows Standby List). This keeps application private memory minimal while utilizing available host RAM for caching.

---

## 4. Crash Recovery and Durability

`aydb` avoids random-I/O flushes during normal transactions and relies on sequential journal writes and ordered metadata updates for crash consistency. The contract is deliberately narrow:

* **`SyncTransactional` commits** are durable when the call returns; before one is appended, all earlier buffered commits are made durable first (prefix durability).
* **`BufferedFast` commits** survive process crashes (`kill -9` / `TerminateProcess`) because they reach the OS page cache, but an OS crash or power loss may roll back recent buffered commits that were not yet synced.
* Consistency rests on CRC-verified WAL sectors, `boot_meta.db` slots, and blobs, and **assumes the storage honours flush / write-through semantics**. It is exercised by deterministic fault-injection simulation (`FaultInjectingFs`), not by physical power-cut testing.

### 4.1 `boot_meta.db` Ping-Pong Buffer (Double Buffering)

* Fixed 8 KB file size: `Offset 0..4096` is **Slot A**, and `Offset 4096..8192` is **Slot B** (automatically extended with an auxiliary `boot_meta_ext_<Generation>.db` payload file if the catalog ever exceeds 4,028 bytes).
* Layout of each 4 KB slot (64-byte fixed header + variable catalog payload + 4-byte slot trailer):
  `[Header(64B): Magic "AYBM"(4B) | Version(2B) |HeaderFlags(2B) | Generation(8B) | CheckpointLSN(8B) | SyncWalEpoch(8B) | BufWalEpoch(8B) | NextSeqID(8B) | IdxCount(2B) | StdBlobCount(2B) | LruBlobCount(2B) | QuotaCount(2B) | PendingDelCount(2B) | PayloadLen(2B) | HeaderCRC32C(4B)] + [CatalogPayload: ActiveIdxList | ActiveStdBlobList | ActiveLruBlobList | TableQuotaList | PendingDeleteList] + [ZeroPadding] + [SlotCRC32C(4B at offset 4092..4096)]`
* When updating metadata, the engine overwrites whichever slot has the older `Generation` in a single 4 KB aligned write and invokes `sync_barrier()` / `sync_durable()`. Upon startup after a crash, both slots are read, and the slot with valid CRCs and the higher `Generation` is selected. Fallback to the other slot happens only on genuine corruption (CRC / format failure or a missing external manifest); a transient I/O error fails the open instead of silently booting from an older generation.

### 4.2 Pre-Allocated Fixed-Size WAL and Physical Two-Stream Commit

* **Zero-Fill-Free Reuse via Epoch Numbers:**
  Both WAL files (`wal_sync.db` at 2 MB fixed, `wal_buf.db` at 16 MB fixed) are treated as arrays of 4 KB sectors. Each sector begins with a 16-byte `WalSectorHeader`: `[SectorCRC32C (4B) | WalEpoch (8B) | ValidBytes (2B) | Reserved (2B)]`, followed by 7-byte fragment headers (`[FragType (1B) | FragLen (2B) | FragCRC32C (4B)]`) framing logical `WalBatch` payloads (`[LSN (8B) | BatchCRC32C (4B) | RecordCount (4B) | Records...]`). When a WAL file wraps around or is reused after a MemTable flush, its `WalEpoch` is incremented (and **both** epochs are rotated unconditionally on every `open`, even if recovery found nothing, so a stale same-epoch sector left behind by write-back reordering can never be replayed; this costs one `boot_meta.db` commit per open) and sectors are overwritten directly without zero-filling the file. During recovery, the engine scans sequentially from the beginning of each file and identifies the exact log tail at the first sector exhibiting either a **CRC mismatch (torn write)** or an **older `WalEpoch`**.
* **Two-Tier Durability via Physical Stream Separation (`wal_sync.db` vs. `wal_buf.db`):**
  To preserve `dwShareMode = 0` exclusivity, avoid flushing multi-megabyte buffered cache pages during small synchronous commits, and prevent LSN sector holes on power loss, `aydb` separates the journal into two physical streams:
  1. **Sync Mode (`wal_sync.db`, Critical configuration & catalog changes):** Opened with `FILE_FLAG_WRITE_THROUGH` and physically pre-zeroed so NTFS `ValidDataLength (VDL)` equals `EOF` from creation. Sectors are padded to 4 KB boundaries and written directly through to non-volatile storage.
  2. **Buffered Mode (`wal_buf.db`, HTTP cache insertions & high-frequency state updates):** Opened as a standard buffered sequential file. Writes enter the OS page cache immediately and return without disk stalls, so committed batches survive application crashes or `TerminateProcess` / `kill -9`. A full OS crash or power outage may roll back the most recent unflushed buffered writes. At every buffered-WAL sync point (prefix sync before a `wal_sync.db` append, transaction prepare, LRU segment reclaim) the open sector is **sealed**, so an already-synced sector is never rewritten in place by a later append (cost: at most one partly used 4 KB sector per sync point). During recovery, valid records from both `wal_sync.db` and `wal_buf.db` are merge-sorted in ascending order of their global 64-bit `LSN`.

### 4.3 Consistency Protocol for KV Separation (`Write-Blob-Before-WAL`)

To prevent dangling pointers (where an index entry survives recovery but its target blob payload is missing), the following write ordering is strictly enforced:

1. **Write Ordering:** Always write `[Magic | BlobCRC32C | Key | Value]` to `blob_<SeqID>.db` first, and only then append the pointer record `[Key | BlobSegID | Offset | Len | BlobCRC32C]` to `wal.db`.
2. **CRC Verification on Recovery and Read:**
   Expensive `FlushFileBuffers` calls on `blob` files are omitted on every write. Instead, during WAL replay after a power failure (or during normal reads), the engine verifies the `BlobCRC32C` stored in the WAL/index against the actual bytes in the `blob` file. If a power outage prevented the blob write from reaching disk (file truncated or CRC mismatch), the engine does **not** treat the database as corrupted; instead, it safely discards the uncommitted record. This validation happens only at startup (`recover_two_streams` + `validate_blob_ptr` for WAL-replayed pointers; blobs referenced from `idx` segments were synced before their checkpoint). After open, a blob that fails verification (CRC/magic/key mismatch, short read, missing segment) is genuine corruption and is **never** silently reported as absence: per-table `on_corrupt` policy either returns `InvalidData` / `SQLITE_CORRUPT` naming the table and primary key (`error`, default for uncapped tables; explicit deletes still succeed and clean up) or hides the row and queues it for automatic deletion at the next safe write (`evict`, default for capped tables). Both modes bump `StorageEngine::corrupt_blob_detections()`.
3. **WAL Failure Poisons the Engine:**
   If a WAL write or sync fails, the engine is poisoned: further mutations return errors until the database is reopened, while reads keep working. After reopen, the failed commit's outcome is unknown (it may or may not be present, but it is never partially applied).
4. **Recovery from SMB Write-Behind Failures:**
   If a background flush error is detected on an SMB handle, the engine closes and reopens the file handles to discard any poisoned local OS cache pages and reconstructs the MemTable from the last verified checkpoint generation.

---

## 5. SQL Mapping and Per-Table LRU (Capped Tables)

### 5.1 Memcomparable Key Encoding

SQL tables, primary rows, and secondary indexes are mapped into a single ordered key-value namespace separated by prefixes. Numeric types are encoded using sign-bit flipping plus big-endian byte order, and strings/byte arrays use escape-encoded null termination so that raw lexicographical byte comparison (`memcmp`) directly preserves SQL sort order and range scans.

* **Primary Row Data:**
  * Key: `[TableID (2B) | 'r' (1B) | EncodedPrimaryKey]`
  * Value: `[RowByteSize (4B) | LastAccessTick (8B) | ColBitmask | InlineColumnValues... | LargeColumnBlobPointers...]`
* **Secondary Index:**
  * Key: `[TableID (2B) | 'i' (1B) | IndexID (2B) | EncodedIndexColumns | EncodedPrimaryKey]`
  * Value: `Empty`
* **System-Managed LRU Tracking Index:**
  * Key: `[TableID (2B) | 'l' (1B) | AccessTick (8B, Big-Endian) | EncodedPrimaryKey]`
  * Value: `[RowByteSize (4B)]`

### 5.2 Per-Table Size Caps (`MAX_BYTES`) and Zero-Write-Amplification Approximate LRU

Automatic eviction mechanism for tables declared with size bounds (e.g., `CREATE TABLE http_cache (...) WITH (MAX_BYTES = 104857600, EVICTION = 'LRU')`):

1. **Logical Size Counter (`CurrentLogicalBytes`):**
   Each table maintains an atomic counter tracking the total logical bytes of all live rows (key length + inline column bytes + referenced blob lengths).
2. **Deferred Access Timestamp Updates on `SELECT` (In-Memory Access Coalescing):**
   Writing a new `AccessTick` to disk on every `SELECT` would turn reads into writes and degrade performance. Instead, a read hit updates `[TableID, PK] -> LatestTick` inside a **small bounded in-memory ring buffer / hash table (capped at a few hundred KB)** with zero disk I/O.
   Once a configurable time window elapses (e.g., several minutes) or when the MemTable flushes, accumulated access timestamp updates are batch-applied to the system LRU index (`[TableID | 'l' | ...]`).
3. **Automatic Eviction Sequence on Quota Overflow:**
   When an `INSERT` or `UPDATE` causes `CurrentLogicalBytes > MAX_BYTES`, the engine iterates from the beginning of `[TableID | 'l' | ...]` (smallest `AccessTick` = oldest accessed rows) and executes cascading eviction:
   * Fetches the primary row `[TableID | 'r' | PK]` and deletes the primary row, all associated secondary index entries `[TableID | 'i' | ...]`, and the system LRU entry.
   * If the row contains blob pointers, increments the `DeadBytes` counter of the corresponding `blob_lru_<TblID>_<SeqID>.db` segment and decrements `CurrentLogicalBytes`.
   * When the live data ratio of a `blob_lru` segment drops to 0% (all blobs in that generation have been evicted), the segment file is immediately closed and deleted without copying a single byte (Copy-less Eviction).

---

## 6. OS Abstraction Layer (PAL / VFS) Interface Definition

To port the Windows/SMB-hardened design cleanly to Linux (`ext4`/`XFS`) and macOS (`APFS`), OS-specific system call differences are encapsulated behind the following trait boundary:

```rust
use std::io;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenHint {
    /// For idx_NNNN.db
    /// - Win: FILE_FLAG_RANDOM_ACCESS
    /// - Linux: POSIX_FADV_RANDOM
    /// - macOS: F_RDAHEAD(0)
    ImmutableRandom,

    /// For blob_NNNN.db (random point reads + buffered append writes)
    /// - Win: Standard OVERLAPPED handle without FILE_FLAG_SEQUENTIAL_SCAN (avoids 256 KB read-ahead amplification on point reads)
    /// - Linux/macOS: Standard positional pread/pwrite handle
    ImmutableSequential,

    /// For Compaction / GC bulk read/write
    /// - Win: FILE_FLAG_SEQUENTIAL_SCAN + SetFileInformationByHandle(IoPriorityHint = Low)
    /// - Linux: POSIX_FADV_NOREUSE / DONTNEED
    /// - macOS: F_NOCACHE(1)
    BackgroundBulk,

    /// For wal.db / boot_meta.db
    MutableLog,
}

pub trait PlatformFile: Send + Sync {
    /// Thread-safe positional read at a given offset
    /// - Win: ReadFile with OVERLAPPED(Offset, OffsetHigh)
    /// - Unix: pread (FileExt::read_exact_at)
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()>;

    /// Thread-safe positional write at a given offset
    /// - Win: WriteFile with OVERLAPPED(Offset, OffsetHigh)
    /// - Unix: pwrite (FileExt::write_all_at)
    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()>;

    /// Write ordering barrier (guarantees ordering prior to metadata update)
    /// - Win: FlushFileBuffers (or write via WRITE_THROUGH handle)
    /// - Linux: fdatasync
    /// - macOS: fcntl(fd, F_BARRIERFSYNC) — avoids heavy F_FULLFSYNC stall
    fn sync_barrier(&self) -> io::Result<()>;

    /// Full durability flush to non-volatile physical media
    /// - Win: FlushFileBuffers / WRITE_THROUGH
    /// - Linux: fdatasync
    /// - macOS: fcntl(fd, F_FULLFSYNC)
    fn sync_durable(&self) -> io::Result<()>;

    /// Explicit eviction of unneeded OS page cache pages (e.g., after compaction reads)
    /// - Win: No-op (automatically released via SEQUENTIAL_SCAN)
    /// - Linux: posix_fadvise(POSIX_FADV_DONTNEED)
    /// - macOS: F_NOCACHE
    fn evict_os_cache(&self, offset: u64, len: u64) -> io::Result<()>;
}
```

> **Note on File Descriptor Limits (e.g., macOS `RLIMIT_NOFILE = 256`):**
> The file manager layer managing `PlatformFile` instances maintains a configurable LRU cache cap on open file descriptors (effectively unlimited on Windows to preserve SMB leases, and capped at e.g. 64–128 descriptors on POSIX). When the cap is reached on POSIX, cold `blob` segments are transparently closed and reopened on demand.

---

## 7. Phased Implementation Roadmap

### Phase 1: Core Storage Engine in Rust & Low-Level Ordered KV API
* Implement the `PlatformFile` trait (Windows / Unix) and extended-length path normalization (`\\?\` prefix on Windows to bypass `MAX_PATH` limits).
* Implement `boot_meta.db` (ping-pong buffer), pre-allocated fixed-size `wal.db` (Epoch + CRC recovery), and the KV-separated `idx` / `blob` segment engine.
* Implement per-table `MAX_BYTES` accounting, the in-memory `AccessTick` coalescing buffer, and automatic LRU eviction.
* Expose a low-level Rust-native Ordered KV API (`get`, `put`, `delete`, `scan_prefix`, `batch_write`).

### Phase 2: SQLite Virtual Table Adapter & Benchmark Verification
* Implement a SQLite `sqlite3_module` (`xBestIndex`, `xFilter`, `xUpdate`) backed by the Phase 1 storage engine.
* Enable immediate execution of full SQL queries via SQLite's battle-tested SQL frontend.
* **Benchmark Evaluation:** Run comparative benchmarks against vanilla SQLite (standard B-Tree in WAL mode) under identical hybrid workloads (random small configuration updates + high-churn multi-KB/MB HTTP cache replacements over both local NTFS and SMB), quantifying improvements in write amplification, file fragmentation, and P99 latency.

### Phase 3: Native Lightweight SQL Frontend (RBO + Iterator Execution)
* Remove the SQLite dependency to deliver a self-contained single-library SQL engine.
* Parse SQL into an AST (using `sqlparser-rs` or a lightweight PEG parser) and lower it through a **deterministic Rule-Based Optimizer (RBO)** without cost-based optimizer (CBO) overhead:
  * Select `IndexScan` whenever an equality or range predicate matches an available index; otherwise fall back to `SeqScan`.
  * Restrict joins to declaration-order `Index Nested Loop Join` or in-memory `Hash Join` for small tables.
* Implement the execution engine using Rust iterators (Volcano / Pipeline model), natively supporting the custom DDL extension `CREATE TABLE ... WITH (MAX_BYTES = ..., EVICTION = 'LRU')`.
