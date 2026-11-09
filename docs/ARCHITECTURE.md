# aydb — Architecture & Design Specification

*Windows/SMB-Optimized Embedded Storage Engine & Lightweight SQL*

---

## 1. Background and Target Requirements

`aydb` (*All Your DataBase*) is an embedded storage engine designed to handle both persistent application state (immediate synchronization of small settings and states) and large-scale caching (blob management with automatic eviction, such as HTTP caches) within a single unified, high-performance, crash-safe foundation for desktop applications.

### 1.1 Core Requirements

* **Hybrid Workload Support:**
  * **Settings & State Management:** Small records (tens to hundreds of bytes), random updates, durability on return for `SyncTransactional` commits.
  * **Cache Management:** Medium-to-large payloads (blobs ranging from a few KB to several MB), high churn rates, and automatic LRU purging governed by per-table logical size limits (`MAX_BYTES`).
* **Zero-Allocation & Bounded Memory Footprint:**
  * The data-holding components have fixed budgets: the MemTable (`memtable_capacity_bytes`), the verified block cache (`block_cache_bytes`, default 4 MB) and the verified blob cache (`blob_cache_bytes`, default 4 MB). Their sum is the deterministic part of the private heap.
  * Four further structures are bounded differently and are **not** included in that sum:
    * *Bloom filters and sparse fence pointers* are kept in memory for every `idx_*.db` segment. They are small relative to the data (about one fence pointer per 4 KB block and `bloom_bits_per_key` bits, default 10, per key) but grow in proportion to the size of the database.
    * *The transaction overlay* holds the uncommitted writes of the open transaction. It is limited by `max_transaction_bytes` (default 64 MB; `0` means unlimited), and a write that would exceed the limit fails. Commits waiting for a group flush (§4.5) are held in memory as well, at most one per connection.
    * *The `AccessTracker`* that coalesces read hits for LRU is capped by entry count (`max_in_memory_access_ticks`, default 65,536 keys), not by bytes.
    * *The SQLite adapter's rowid map* for non-integer primary keys retains at most 262,144 entries once all cursors are closed; while cursors are open it can hold more.
  * **Primary Storage Caching via OS Page Cache:** Raw file caching, OS-level write-behind, and sequential readahead are delegated to the host operating system's page cache (e.g., Windows Cache Manager VACBs).
  * **Optional Bounded Verified Block/Blob Cache:** Rather than a blanket prohibition on in-process caching, the engine supports an optional, strictly-bounded user-space cache for verified 4 KB index blocks (`block_cache_bytes`) and small verified blobs (`blob_cache_bytes` in `EngineConfig`).
    * *Rationale:* While the OS page cache eliminates physical disk I/O on warm pages, every uncached `idx_*.db` 4 KB block read still pays an OS kernel transition (`ReadFile` / `pread` via `CcCopyRead`) plus a full-block CRC32C pass. On Windows, this overhead amounts to microseconds per lookup. Caching already-verified immutable 4 KB data blocks and small verified blobs in user space eliminates both the syscall and repeated CRC verification on hot internal/leaf blocks, reducing lookup latency to tens of nanoseconds while keeping the cache and MemTable budgets fixed (`memtable_capacity_bytes` + `block_cache_bytes` + `blob_cache_bytes`; the structures listed above are accounted for separately). Setting `block_cache_bytes = 0` and `blob_cache_bytes = 0` preserves the zero-cache footprint for ultra-constrained environments.
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
| **Boot Metadata** | `boot_meta.db` | Standard (4 KB-aligned, device flush after every write) | Fixed 8 KB size (4 KB × 2 ping-pong buffer). Stores active segment ID lists, table statistics, and pending GC lists. |
| **Sync Journal (WAL)** | `wal_sync.db` | `OVERLAPPED` (buffered, device flush per commit) | Default 8 MB size (physically pre-zeroed). Logs critical synchronous mutations without flushing buffered cache pages. See §4.5 for the commit path. |
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

### 3.2 Index Storage: Immutable Offset-Indexed SSTables (`idx_*.db`) and Bounded Caching

* **Writable MemTable (In-Memory, Fixed Size Cap: e.g., 4 MB – 8 MB):**
  An in-memory B-Tree or SkipList absorbing the latest writes. Once it reaches its memory cap (`memtable_capacity_bytes`), it is flushed to disk as an immutable `idx_<SeqID>.db` segment.
* **Immutable Index Segments (Sparse Fence Pointers & In-Heap Bloom Filters):**
  On-disk `idx_<SeqID>.db` files are organized into 4 KB blocks. The database process keeps **only the first key of each 4 KB block (Sparse Fence Pointers) and a Bloom Filter in heap memory**. Both are proportional to the indexed data (a few hundred KB to a few MB for typical databases, more for larger ones) and are not covered by the fixed cache budgets below. During lookups, the engine locates the target block offset via binary search over the fence pointers and reads the 4 KB block.
* **Complementary Caching Architecture: OS Page Cache + Optional Bounded Verified Cache:**
  * **OS Page Cache as Primary Substrate:** The host OS page cache (Windows Standby List / VACBs, Linux page cache) serves as the primary data cache, handling physical file buffering, write-behind, and sequential read-ahead without custom in-process buffer pool bloat.
  * **Optional Bounded Verified Block Cache (`block_cache_bytes`):** Caches 4 KB `idx_*.db` data blocks in user-space fixed slots indexed by `(seq_id, block_idx)` **after** CRC32C verification.
  * **Optional Bounded Small Blob Cache (`blob_cache_bytes`):** Caches verified small blob payloads ($\le 16\text{ KB}$) indexed by `(BlobKind, table_id, seg_id, offset)` with a dedicated sub-budget so that large-blob churn cannot evict index blocks. For the append-only active blob segment, only records below `flushed_offset()` are cached.
  * **Rationale:** While the OS page cache eliminates physical disk I/O on warm pages, every uncached `idx_*.db` 4 KB block read still pays an OS kernel transition (`ReadFile` / `pread`) plus a full-block CRC32C pass. Caching already-verified immutable 4 KB data blocks and small verified blobs in user space eliminates both the syscall and repeated CRC verification on hot internal/leaf blocks, dropping read latency from microseconds to tens of nanoseconds while keeping the cache and MemTable budgets fixed (`memtable_capacity_bytes` + `block_cache_bytes` + `blob_cache_bytes`). Setting `block_cache_bytes = 0` and `blob_cache_bytes = 0` preserves the zero-cache footprint for ultra-constrained environments.
  * **Immutability & Zero Cache Invalidation Hazard:** Because `idx` and `blob` segments are immutable and `SeqID`s are never reused (`ARCHITECTURE.md` §2.3), cached entries never become stale. Invalidation is needed only when a segment is retired (during compaction commit or LRU segment reclamation), where all cached blocks for that `seq_id` are purged.
  * **Scan-Resistance:** Unbounded full-table scans do not insert blocks into the cache, and compaction reads `idx_*.db` inputs through a streaming reader that fetches one 4 KB block at a time without cache insertion. Batch passes therefore do not displace hot point-lookup working sets. This bypass comes from the readers, not from an open hint: `OpenHint::BackgroundBulk` (`FILE_FLAG_SEQUENTIAL_SCAN` plus a low I/O priority hint on Windows) is defined and implemented in the Windows PAL, but compaction does not currently open its files with it, so it is not used.

---

## 4. Crash Recovery and Durability

`aydb` avoids random-I/O flushes during normal transactions and relies on sequential journal writes and ordered metadata updates for crash consistency. The contract is deliberately narrow:

* **`SyncTransactional` commits** are durable when the call returns; before one is appended, all earlier buffered commits are made durable first (prefix durability).
* **`BufferedFast` commits** survive process crashes (`kill -9` / `TerminateProcess`) because they reach the OS page cache, but an OS crash or power loss may roll back recent buffered commits that were not yet synced.
* Consistency rests on CRC-verified WAL sectors, `boot_meta.db` slots, and blobs, and **assumes the storage honours flush semantics**. It is exercised by deterministic fault-injection simulation (`FaultInjectingFs`), not by physical power-cut testing.

### 4.1 `boot_meta.db` Ping-Pong Buffer (Double Buffering)

* Fixed 8 KB file size: `Offset 0..4096` is **Slot A**, and `Offset 4096..8192` is **Slot B** (if the catalog payload exceeds 4,028 bytes it is written to an immutable overflow file `manifest_<SeqID>.db`, and the slot stores a 20-byte pointer to it: `[SeqID (8B) | FileLen (8B) | PayloadCRC32C (4B)]`).
* Layout of each 4 KB slot (64-byte fixed header + variable payload + 4-byte slot trailer):
  `[Header(64B): Magic "AYDB"(4B) | Version(2B) | Flags(2B) | Generation(8B) | NextSeqID(8B) | CheckpointLSN(8B) | SyncWalEpoch(8B) | BufWalEpoch(8B) | PayloadLen(4B) | PayloadCRC32C(4B) | Reserved(4B) | HeaderCRC32C(4B)] + [Payload: CatalogPayload, or the external manifest pointer when `Flags` bit 0 is set] + [ZeroPadding] + [SlotCRC32C(4B at offset 4092..4096)]`
  The header offsets are `0..4` magic, `4..6` version, `6..8` flags, `8..16` generation, `16..24` next sequence ID, `24..32` checkpoint LSN, `32..40` sync WAL epoch, `40..48` buffered WAL epoch, `48..52` payload length, `52..56` payload CRC32C, `56..60` reserved, `60..64` header CRC32C (over bytes `0..60`). The trailer CRC32C covers bytes `0..4092`. The segment counts are not part of the header: the catalog payload is a sequence of length-prefixed lists (index segments, standard blob segments, LRU blob segments, table quotas, pending deletes). The overflow file starts with its own magic `AYMF` and carries a payload CRC32C. See [phase1/02_BOOT_META_AND_GC.md](./phase1/02_BOOT_META_AND_GC.md) for the field-level specification.
* When updating metadata, the engine overwrites whichever slot has the older `Generation` in a single 4 KB aligned write and invokes `sync_barrier()` / `sync_durable()`. Upon startup after a crash, both slots are read, and the slot with valid CRCs and the higher `Generation` is selected. Fallback to the other slot happens only on genuine corruption (CRC / format failure or a missing external manifest); a transient I/O error fails the open instead of silently booting from an older generation.

### 4.2 Pre-Allocated Fixed-Size WAL and Physical Two-Stream Commit

* **Zero-Fill-Free Reuse via Epoch Numbers:**
  Both WAL files (`wal_sync.db` at 8 MB by default, `wal_buf.db` at 16 MB fixed) are treated as arrays of 4 KB sectors. Each sector begins with a 16-byte `WalSectorHeader`: `[SectorCRC32C (4B) | WalEpoch (8B) | ValidBytes (2B) | Reserved (2B)]`, followed by 3-byte fragment headers (`[FragType (1B) | FragLen (2B)]`) framing logical `WalBatch` payloads (`[LSN (8B) | BatchCRC32C (4B) | RecordCount (4B) | Records...]`). When a WAL file wraps around or is reused after a MemTable flush, its `WalEpoch` is incremented (and **both** epochs are rotated unconditionally on every `open`, even if recovery found nothing, so a stale same-epoch sector left behind by write-back reordering can never be replayed; this costs one `boot_meta.db` commit per open) and sectors are overwritten directly without zero-filling the file. Fragments carry no checksum of their own: the sector CRC32C covers the whole sector (header, fragments and padding), and each reassembled batch is additionally verified by its `BatchCRC32C`. During recovery, the engine scans sequentially from the beginning of each file and identifies the exact log tail at the first sector exhibiting either a **CRC mismatch (torn write)** or an **older `WalEpoch`**.
* **Two-Tier Durability via Physical Stream Separation (`wal_sync.db` vs. `wal_buf.db`):**
  To preserve `dwShareMode = 0` exclusivity, avoid flushing multi-megabyte buffered cache pages during small synchronous commits, and prevent LSN sector holes on power loss, `aydb` separates the journal into two physical streams:
  1. **Sync Mode (`wal_sync.db`, Critical configuration & catalog changes):** Physically pre-zeroed so NTFS `ValidDataLength (VDL)` equals `EOF` from creation. Sectors are padded to 4 KB boundaries, written to the OS cache, and flushed to the device before the commit returns (§4.5).
  2. **Buffered Mode (`wal_buf.db`, HTTP cache insertions & high-frequency state updates):** Opened as a standard buffered sequential file. Writes enter the OS page cache immediately and return without disk stalls, so committed batches survive application crashes or `TerminateProcess` / `kill -9`. A full OS crash or power outage may roll back the most recent unflushed buffered writes. At every buffered-WAL sync point (prefix sync before a `wal_sync.db` append, transaction prepare, LRU segment reclaim) the open sector is **sealed**, so an already-synced sector is never rewritten in place by a later append (cost: at most one partly used 4 KB sector per sync point). During recovery, valid records from both `wal_sync.db` and `wal_buf.db` are merge-sorted in ascending order of their global 64-bit `LSN`.

### 4.3 Consistency Protocol for KV Separation (`Write-Blob-Before-WAL`)

To prevent dangling pointers (where an index entry survives recovery but its target blob payload is missing), the following write ordering is strictly enforced:

1. **Write Ordering:** Always write `[Magic | BlobCRC32C | Key | Value]` to `blob_<SeqID>.db` first, and only then append the pointer record `[Key | BlobSegID | Offset | Len | BlobCRC32C]` to the WAL (`wal_sync.db` or `wal_buf.db`).
2. **CRC Verification on Recovery and Read:**
   For `BufferedFast` commits, expensive `FlushFileBuffers` calls on `blob` files are omitted. (`SyncTransactional` commits sync the blob writers before the WAL record.) Instead, during WAL replay after a power failure (or during normal reads), the engine verifies the `BlobCRC32C` stored in the WAL/index against the actual bytes in the `blob` file. If a power outage prevented the blob write from reaching disk (file truncated or CRC mismatch), the engine does **not** treat the database as corrupted; instead, it safely discards the uncommitted record. This validation happens only at startup (`recover_two_streams` + `validate_blob_ptr` for WAL-replayed pointers; blobs referenced from `idx` segments were synced before their checkpoint). After open, a blob that fails verification (CRC/magic/key mismatch, short read, missing segment) is genuine corruption and is **never** silently reported as absence: per-table `on_corrupt` policy either returns `InvalidData` / `SQLITE_CORRUPT` naming the table and primary key (`error`, default for uncapped tables; explicit deletes still succeed and clean up) or hides the row and queues it for automatic deletion at the next safe write (`evict`, default for capped tables). Both modes bump `StorageEngine::corrupt_blob_detections()`.
   Recovery applies the startup validation per stream. In `wal_sync.db` a batch whose blob cannot be verified is dropped on its own, because the blob is synced before the WAL record is written. In `wal_buf.db` blobs and WAL records are flushed independently, so a power loss can persist a later commit while losing an earlier one. Recovery of the buffered stream therefore has **prefix semantics**: it stops at the first batch (or transaction group) with an unverifiable blob and discards everything after it in that stream, so that replay never exposes a hole in the commit order. Only a blob that was never durably written counts as unverifiable. A real I/O error while reading a blob during validation is not treated as absence; it aborts recovery and fails the open, so that a transient error cannot cause a committed batch to be dropped.
3. **WAL Failure Poisons the Engine:**
   If a WAL write/sync or metadata slot write/sync fails, the engine is poisoned. Further mutations return errors until the database is reopened, while reads keep working. A failed WAL commit may or may not be present after recovery, but it is never partially applied. A failed metadata update may already have made its new slot authoritative, so index outputs and both generations' files are retained and metadata updates and garbage collection stop until recovery resolves the on-disk state.
4. **Recovery from SMB Write-Behind Failures (design intent, not implemented):**
   The intended design is that, if a background flush error is detected on an SMB handle, the engine closes and reopens the file handles to discard any poisoned local OS cache pages and reconstructs the MemTable from the last verified checkpoint generation. This in-place recovery does not exist. The engine instead poisons itself on the first failed WAL append/sync or metadata slot write/sync (item 3), and the application has to drop the engine and reopen the database, which runs the normal crash recovery from the last verified state. A write-behind failure that is reported only when the OS later flushes cached pages is not detected unless a sync call returns it.

### 4.4 Checkpoints and Post-Commit Maintenance

* **`StorageEngine::checkpoint`:** Brings the persisted state up to date so that the next `open` has no replay work. A non-empty MemTable is flushed to a new `idx` segment, which also commits blob segment lengths, dead bytes and quota totals. With an empty MemTable, the lengths of blob segments that have grown since the last commit (for example through a rolled-back transaction) are committed instead. It fails with `InvalidInput` while a transaction is active. Dropping the engine does not run a checkpoint; a drop without one is treated as a crash by the next `open`.
* **Post-commit maintenance failures do not fail the commit:** Once the WAL append of a write or transaction has succeeded, the commit is durable and the call returns `Ok`, even if the follow-up maintenance (blob segment reclaim, quota persistence, quota eviction, compaction, MemTable flush) fails. Such failures are counted in `StorageEngine::maintenance_failures()` and the text of the latest one is kept in `last_maintenance_error()`. Retryable failures leave the flush condition or exceeded quota in place for later commits. WAL append/sync and metadata slot write/sync failures poison the engine and require reopen (item 3 above), including a metadata failure during post-commit maintenance.

### 4.5 Commit Path and Group Commit

A synchronous SQL commit (`xCommit`) has three steps, and only the first and last hold the engine lock:

1. Under the lock, the WAL batch is written to `wal_sync.db` without a device flush.
2. Without the lock, the commit waits for a device flush that covers its batch.
3. Under the lock, the batch moves to the MemTable, which makes it visible to readers.

* **Readers are not held up by the flush.** They keep reading the committed state, and a transaction becomes visible only after a flush has made it durable.
* **The writer slot is released after step 1.** The next writer starts at once. Its transaction overlay begins with the batches still waiting for durability, so it builds on them and no update is lost. Readers on other connections do not see those batches. These are bounded because each connection has at most one commit waiting.
* **One flush covers a group.** The first waiting commit becomes the leader and flushes the file. The flush covers every batch written before it started. Commits that arrive during the flush are covered by the next leader.
* **Waiting instead of `SQLITE_BUSY`.** `xBegin` waits up to `writer_wait_timeout_ms` (default 1 second) for another connection's transaction to end. The wait is bounded because that connection could be held open by the same thread, which could never end it. `0` restores immediate failure.
* **Paths that bypass the group flush it first.** Rotating the WAL, flushing the MemTable, checkpointing, a commit that is not part of the group, and a non-transactional write all flush and publish the waiting batches first, so the MemTable holds every earlier commit before they proceed.
* **A failed flush fails the whole group.** The engine is poisoned and every waiting commit returns an error, as with any WAL sync failure (§4.3 item 3).
* **Commits outside the group.** A synchronous commit with post-commit maintenance (capped tables, dropped tables, quota persistence, a due MemTable flush) keeps the writer slot during its own flush, still without the engine lock. A transaction split across several WAL batches (over 512 KB) flushes under the lock.
* **Why `wal_sync.db` is not write-through on Windows.** NTFS holds `FILE_FLAG_WRITE_THROUGH` writes back while the same file is being flushed, so such writes could not join a group flush in progress. Write-through alone also does not guarantee durability, because it can complete in a drive's volatile cache. Each commit relies on the device flush instead.

---

## 5. SQL Mapping and Per-Table LRU (Capped Tables)

### 5.1 Memcomparable Key Encoding

SQL tables, primary rows, and secondary indexes are mapped into a single ordered key-value namespace separated by prefixes. Numeric types are encoded using sign-bit flipping plus big-endian byte order, and strings/byte arrays use escape-encoded null termination so that raw lexicographical byte comparison (`memcmp`) directly preserves SQL sort order and range scans.

* **Primary Row Data:**
  * Key: `[TableID (2B) | 'r' (1B) | EncodedPrimaryKey]`
  * Value: The row payload (inline bytes), or a `BlobPtr` when it is stored out of line.
* **Secondary Index:**
  * Key: `[TableID (2B) | 'i' (1B) | IndexID (2B) | EncodedIndexColumns | EncodedPrimaryKey]`
  * Value: `Empty`
* **System-Managed LRU Tracking Index:**
  * Key: `[TableID (2B) | 'l' (1B) | AccessTick (8B, Big-Endian) | EncodedPrimaryKey]`
  * Value: `Empty`
* **Row Metadata (capped tables and tables with secondary indexes):**
  * Key: `[TableID (2B) | 't' (1B) | EncodedPrimaryKey]`
  * Value: `RowMetaRecord`: the recorded `AccessTick`, the logical size, and the secondary index values of the row.

### 5.2 Per-Table Size Caps (`MAX_BYTES`) and Zero-Write-Amplification Approximate LRU

Automatic eviction mechanism for tables declared with size bounds (e.g., a virtual table declared with `max_bytes = 104857600`):

1. **Logical Size Counter (`CurrentLogicalBytes`):**
   Each table maintains an atomic counter tracking the total logical bytes of all live rows (key length + inline column bytes + referenced blob lengths).
2. **Deferred Access Timestamp Updates on `SELECT` (In-Memory Access Coalescing):**
   Writing a new `AccessTick` to disk on every `SELECT` would turn reads into writes and degrade performance. Instead, a read hit updates `[TableID, PK] -> LatestTick` inside a **bounded in-memory map** (`max_in_memory_access_ticks` keys, default 65,536; the older half is dropped when full) with zero disk I/O.
   Ticks are not written back on a timer or at a flush. During quota enforcement (below), a row whose in-memory tick is newer than its queue entry is promoted in the LRU index instead of being evicted.
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
    /// For boot_meta.db and wal_sync.db
    /// - Win: no extra open flag; durability comes from sync_barrier / sync_durable
    /// - Unix: no access advice; the hint selects the sync behaviour below
    MutableSyncLog,

    /// For wal_buf.db
    /// - Win: FILE_FLAG_SEQUENTIAL_SCAN
    /// - Linux: POSIX_FADV_SEQUENTIAL; macOS: F_RDAHEAD(1)
    MutableBufferedLog,

    /// For idx_<SeqID>.db
    /// - Win: FILE_FLAG_RANDOM_ACCESS
    /// - Linux: POSIX_FADV_RANDOM; macOS: F_RDAHEAD(0)
    ImmutableRandom,

    /// For blob_*_<SeqID>.db (random point reads + buffered append writes)
    /// - Win: Standard OVERLAPPED handle without FILE_FLAG_SEQUENTIAL_SCAN (avoids 256 KB read-ahead amplification on point reads)
    /// - Unix: Standard positional pread/pwrite handle
    ImmutableSequential,

    /// For Compaction / GC bulk read/write. Defined, but not used by the
    /// engine: compaction does not open its files with this hint.
    /// - Win: FILE_FLAG_SEQUENTIAL_SCAN + SetFileInformationByHandle(IoPriorityHint = Low)
    /// - Linux: POSIX_FADV_NOREUSE; macOS: F_NOCACHE(1)
    BackgroundBulk,
}

pub trait PlatformFile: Send + Sync {
    /// Thread-safe positional read at a given offset
    /// - Win: ReadFile with OVERLAPPED(Offset, OffsetHigh)
    /// - Unix: pread (FileExt::read_exact_at)
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()>;

    /// Batched positional reads for scatter/gather prefetching (e.g., WiscKey blob dereferencing)
    /// - All platforms: requests are sorted by offset, nearby ones are
    ///   coalesced into one larger read (gap <= 16 KB, span <= 64 KB), and
    ///   the resulting reads are issued sequentially with read_exact_at.
    ///   Concurrent OVERLAPPED reads on Windows and io_uring batching on
    ///   Linux are possible extensions and are not implemented.
    fn read_many_at(&self, requests: &mut [(u64, &mut [u8])]) -> io::Result<()>;

    /// Thread-safe positional write at a given offset
    /// - Win: WriteFile with OVERLAPPED(Offset, OffsetHigh)
    /// - Unix: pwrite (FileExt::write_all_at)
    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()>;

    /// Write ordering barrier (guarantees ordering prior to metadata update)
    /// - Win: FlushFileBuffers
    /// - Linux: fdatasync
    /// - macOS: same as sync_durable (F_FULLFSYNC) for MutableSyncLog
    ///   handles, otherwise fsync. F_BARRIERFSYNC is not used.
    fn sync_barrier(&self) -> io::Result<()>;

    /// Full durability flush to non-volatile physical media
    /// - Win: FlushFileBuffers
    /// - Linux: fsync (File::sync_all)
    /// - macOS: fcntl(fd, F_FULLFSYNC), falling back to fsync if it fails
    fn sync_durable(&self) -> io::Result<()>;

    /// Intended: explicit eviction of unneeded OS page cache pages (e.g.,
    /// after compaction reads). Current state: a no-op returning Ok(())
    /// on every platform, and not called by the engine. The design intent
    /// was posix_fadvise(POSIX_FADV_DONTNEED) on Linux and F_NOCACHE on
    /// macOS.
    fn evict_os_cache(&self, offset: u64, len: u64) -> io::Result<()>;
}
```

> **Note on File Descriptor Limits & POSIX `HandlePool` (e.g., macOS `RLIMIT_NOFILE = 256`):**
> On Windows, every open segment handle holds an SMB `Read-Write-Handle (RWH) Lease`; because Windows supports millions of handles per process, segment handles remain open until segment retirement. On POSIX targets, where `RLIMIT_NOFILE` can be as low as 256, the file manager maintains a reference-aware `HandlePool` (`src/pal/unix.rs`) with a configurable descriptor cap (default: 128). The pool bounds the number of simultaneously open OS file descriptors across idle segment handles using LRU eviction, without evicting or closing handles that are currently pinned by an active `Arc` reference, an open `LazyBlob` cursor, or an in-flight `pread`. Cold unpinned handles are transparently closed and reopened on demand.

---

## 7. Phased Implementation Roadmap

### Phase 1: Core Storage Engine in Rust & Low-Level Ordered KV API
* Implement the `PlatformFile` trait (Windows / Unix) and extended-length path normalization (`\\?\` prefix on Windows to bypass `MAX_PATH` limits).
* Implement `boot_meta.db` (ping-pong buffer), pre-allocated fixed-size `wal_sync.db` / `wal_buf.db` (Epoch + CRC recovery), and the KV-separated `idx` / `blob` segment engine.
* Implement per-table `MAX_BYTES` accounting, the in-memory `AccessTick` coalescing buffer, and automatic LRU eviction.
* Expose a low-level Rust-native Ordered KV API (`get_row`, `put_row`, `delete_row`, `scan_table`, transactions).

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

---

## 8. Performance Characteristics and Open Items

Measured figures are in the phase chapters, each labelled with the commit it was measured at, so that a figure matches the implementation its chapter describes. The CI results page (<https://toyoshim-i.github.io/aydb/results.html>) shows the latest run. This section records why the engine behaves as it does.

### 8.1 Compared with SQLite in WAL Mode

* **Point reads** served from the MemTable or the verified caches avoid a system call, and are where `aydb` is strongest. Reads also continue at nearly full speed during synchronous commits (§4.5).
* **Buffered commits** are faster because each one coalesces into a single WAL batch in the OS cache.
* **Concurrent synchronous writers** share device flushes (§4.5; measured in phase 2, Chapter 2.2 §4). A single synchronous writer is about level with SQLite, because the device flush dominates the commit once both engines flush the device.
* **Bulk writes inside one transaction** are slower through the SQLite virtual table: each row crosses the `sqlite3_module` boundary and checks primary key uniqueness.
* **Updates to values stored out of line** cost at least two writes per buffered commit (blob append and WAL sector).
* **Short range scans** are slower: rows of a B-tree are clustered in leaf pages, while `aydb` needs a blob read per out-of-line row, and merges several sorted sources.
* **SSTable mode with a small MemTable** pays LSM read amplification: several segments can be probed per lookup. The block cache reduces this but does not remove it.

Windows-specific file handling is not the limit here. The remaining gaps come from the costs of system calls, CRC verification, and the virtual table boundary.

### 8.2 Cache Verification Trade-off

Only CRC-verified bytes enter the block and blob caches, so a hit skips re-verification. The cache is process-private memory, so this is safe against other processes, but a hit does not detect corruption that occurred on disk after the first read.

### 8.3 Open Items

* Issue the reads of `read_many_at` concurrently (OVERLAPPED on Windows, `io_uring` on Linux) instead of sequentially. This matters most on SMB.
* Avoid per-entry key and value clones in `MemTable::write_to_segment`, and decode-before-filter in `scan_prefix_range`.
* Read blobs directly into `Vec` spare capacity with a streaming CRC, avoiding the zeroed 16 KB stack buffer and the extra copy.
* Combine the overlay and MemTable lookups of owner reads, or skip the overlay probe for untouched key ranges.
* Use a cheaper coalescing path for buffered commits from several connections, without rewriting a synced sector in place.
