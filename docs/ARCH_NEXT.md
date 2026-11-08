# aydb — Next-Step Architecture Proposals (Draft)

*Status: Implemented (Steps 1–6 complete: 3-way interleaved CRC32C, configurable inline threshold, bounded VerifiedBlockCache & VerifiedBlobCache, PlatformFile::read_many_at with range-scan blob prefetching, zero-alloc buffered commit coalescing, and reference-aware POSIX HandlePool).*

This document collects design changes aimed at the workloads where `aydb` still trails vanilla SQLite (WAL mode), while keeping the core requirements of `ARCHITECTURE.md` §1: a bounded process heap, crash safety without random-I/O flushes, and Windows/SMB-friendly file handling.

---

## 0. Why These Changes Are Needed

`aydb` was designed from the start around the behaviour of Windows file systems. Its file layout, open flags, and durability protocol follow from NTFS, the Cache Manager, SMB leases, and antivirus filter drivers. The benchmark results in `README.md` show that this pays off exactly where those behaviours dominate:

- **Synchronous commits:** TPC-B-style with `FULL` sync runs **`26.1×`** faster than SQLite (`13,817` vs `529` TPS), and synchronous `128 B` settings updates run **`15.0×`** faster (P50 `56.6 µs` vs `978.6 µs`). SQLite's `synchronous=FULL` calls `FlushFileBuffers` on every commit. On NTFS that call flushes all dirty pages and the metadata of the file, plus the device cache. `aydb` instead writes one 4 KB sector with `FILE_FLAG_WRITE_THROUGH` into a pre-zeroed `wal_sync.db`, whose `ValidDataLength` already equals EOF, so no metadata update is needed.
- **Point reads served from the MemTable:** Speedtest1 test 130 (`2.76×`) and YCSB C with `256 B` values (`3.98×`) hit the in-process `BTreeMap` and issue no system call.

Despite this design work, several workloads still lose to SQLite, some by a wide margin:

- YCSB A and F with `2 KB` values: `0.55×` and `0.58×`.
- YCSB E (short range scans) with `2 KB` values: `0.39×`.
- Speedtest1-style suite in SSTable mode: `0.31×`.
- Read-heavy YCSB B and C with `2 KB` values: `0.83×` and `0.98×`.

These losses do not come from missing Windows tuning in `aydb` or extra Windows tuning in SQLite. SQLite's Windows layer (`os_win.c`) is mostly about robustness (antivirus retry, `LockFileEx`, mmap off by default). The gap is architectural:

- SQLite keeps hot pages in its own process (`pcache`). A cache hit costs tens of nanoseconds and no system call.
- `aydb` deliberately keeps no user-space data cache (`ARCHITECTURE.md` §3.2). Every access outside the MemTable pays a `ReadFile` (user/kernel transition plus a Cache Manager copy) and a full CRC32C verification, which costs microseconds on Windows.
- WiscKey separation adds one more random read per row for values `≥ 1 KB`. That cost is worthwhile for multi-KB to MB payloads but not for `2 KB` rows.
- The LSM layout probes several segments per lookup, and each probe repeats both costs.

In other words, the Windows-specific choices already capture the gains they can. The remaining gap comes from the "OS cache only, no user-space cache" principle combined with the comparatively high cost of system calls on Windows. Closing it requires changing that principle within the existing heap budget, and reducing per-access syscall and verification cost, rather than more file-system tuning.

The post-audit performance work on `main` confirms this direction. Moving compaction outside the engine lock cut the worst-case commit stall from `59.3 ms` to `9.0 ms`. Moving uncommitted transaction data into a private overlay reduced non-owner read latency with 10,000 uncommitted rows from `4.7 ms` to `0.19 µs`. Both changes removed structural costs; neither depended on file-system behaviour.

---

## 1. Where `aydb` Loses Today and Why

| Workload (README) | aydb vs SQLite | Root cause |
| :--- | ---: | :--- |
| YCSB B/C, `2 KB` values (WiscKey) | `0.83×` / `0.98×` | Every read issues a `ReadFile` on `blob_*.db` plus a CRC32C over the record. SQLite serves the same row from its in-process page cache with no syscall. |
| YCSB A/F, `2 KB` values | `0.55×` / `0.58×` | Each buffered commit issues at least two `WriteFile` calls (blob append + 4 KB WAL sector rewrite) versus one WAL frame append in SQLite. |
| YCSB E (short ranges), `2 KB` values | `0.39×` | One random blob read per row; B-tree rows are clustered in leaf pages. |
| Speedtest1-style, SSTable mode | `0.31×` | LSM read amplification: several segments probed per lookup, each probe a `ReadFile` + 4 KB CRC verification, no block reuse. |

The wins (sync commits `15–26×`, MemTable point reads `2–4×`) come from Windows-specific choices: pre-zeroed `WRITE_THROUGH` WAL (no `ValidDataLength` metadata updates, no `FlushFileBuffers`) and an in-process MemTable.

The losses share one cause: **`aydb` deliberately keeps no user-space data cache** (`ARCHITECTURE.md` §3.2), so every non-MemTable access pays a user/kernel transition, a Cache Manager copy (`CcCopyRead`), and a full CRC check. On Windows this costs microseconds per access, whereas a user-space cache hit costs tens of nanoseconds. SQLite's advantage here is its generic page cache (`pcache`), not Windows-specific tuning; its `os_win.c` adaptations are mostly about robustness (antivirus retry loop, `LockFileEx`, mmap disabled by default).

---

## 2. Design Principle Update

Current wording (`ARCHITECTURE.md` §3.2): *"maintaining no custom in-process buffer pool for data blocks."*

Proposed wording:

> The OS page cache remains the primary cache. The engine **may** keep an optional, strictly bounded in-process cache of *verified* hot blocks, whose budget is part of the fixed heap budget (§1.1: "a few megabytes to tens of megabytes"). With the budget set to `0`, behaviour is identical to the pure OS-cache design.

Rationale: the double-caching cost of a small cache (a few MB duplicated in both the OS cache and the heap) is negligible compared with removing syscalls on the hottest blocks, and an explicit cap preserves the heap-bound requirement.

---

## 3. Proposals

### 3.1 Bounded Verified Block Cache (highest impact)

**What is cached**
- `idx` data blocks: key `(seq_id, block_idx)`, value the 4 KB block **after** CRC verification.
- Optionally, small blob records (≤ `16 KB`): key `(BlobKind, table_id, seg_id, offset)`, value the verified payload. Separate sub-budget so large-blob churn cannot evict index blocks.

**Configuration** (`EngineConfig`)
- `block_cache_bytes: usize` — default `4 MB` (`0` disables the cache).
- `blob_cache_bytes: usize` — default `4 MB` (`0` disables the cache).
- Both count toward the documented heap budget, and memory accounting must include per-entry overhead.

**Replacement policy**
- CLOCK (or CLOCK-Pro) in fixed-size slots. Cheap hits with one reference-bit store, no list relinking, which suits concurrent readers.
- Scan resistance: compaction inputs and unbounded full-table scans bypass insertion (read-through without caching). As implemented, this comes from a streaming compaction reader and a `populate_cache` flag in the scan path; `OpenHint::BackgroundBulk` is defined but not used.

**Correctness invariants**
- Segments are immutable and `SeqID`s are never reused (`ARCHITECTURE.md` §2.3), so cached entries never become stale. Invalidation is needed only to reclaim memory: drop all entries of a `seq_id` when the segment is retired (compaction install, blob segment reclaim).
- Only CRC-verified bytes enter the cache, so a hit skips re-verification. This is safe because the cache is process-private memory, but note that it no longer detects corruption that occurs on disk after the first read. Document this, and optionally re-verify on a sampled basis.
- The active blob segment is append-only: cache only records below `flushed_offset()`.

**Concurrency** (depends on the `RwLock` read path, stage A2)
- Reads run under the engine read lock, so the cache needs interior mutability. Use a sharded design (`N` shards × `Mutex`, shard = hash(key)), or a lock-free CLOCK with atomic reference bits. Never hold a shard lock across I/O: on a miss, read and verify outside the lock, then insert.

**Measurement plan**
- Re-run `standard_sql_bench` SSTable rows and YCSB `2 KB` rows with budgets `0 / 1 / 4 / 16 MB`.
- Report hit rate, heap usage, and p50/p99 read latency.
- Success criterion: SSTable-mode Speedtest1 and YCSB C (`2 KB`) close to parity with SQLite at ≤ `4 MB`.

### 3.2 Configurable / Raised Inline Threshold

- `DEFAULT_INLINE_THRESHOLD = 1024` sends YCSB's `2 KB` values to blob files. WiscKey separation pays off when values are large (multiple KB to MB) and write amplification dominates. For `1–3 KB` values the extra indirection costs more than it saves.
- Upper bound: an inline entry must fit in one 4 KB block (`IndexEntry::inline_fits_in_block`: `13 + key + value ≤ MAX_SINGLE_BLOCK_ENTRY_BYTES`), so the practical maximum is about `3.5–4 KB` minus the key size.
- Trade-offs: fewer entries per block (more blocks per range scan, more fence pointers in heap), and compaction now rewrites these values (higher write amplification).
- Proposal: make the threshold per table (vtab DDL option, e.g. `inline_max=`). Benchmark `1 / 2 / 3.5 KB` defaults and pick by data, possibly keeping `1 KB` for capped (cache) tables where copy-less LRU segment reclamation matters most.

### 3.3 Blob Prefetch for Range Scans

- After a cursor chunk is fetched (`CURSOR_STREAM_CHUNK_ROWS`), the blob pointers of the next `k` rows are known. The PAL already opens files with `FILE_FLAG_OVERLAPPED`, so several `ReadFile` calls could be in flight at once.
- Add `PlatformFile::read_many_at(&[(offset, &mut [u8])])`. As implemented, it sorts the requests, coalesces nearby ones (gap up to 16 KB, span up to 64 KB), and issues the reads sequentially on every platform. Issuing them concurrently (OVERLAPPED on Windows, `io_uring` on Linux) is not implemented.
- Interaction with lazy dereference: prefetch only when the query projects blob columns. `colUsed` in `xBestIndex` already tells us this.
- Expected effect: fewer kernel transitions through coalescing today; with concurrent issue (not implemented), YCSB E (`2 KB`) would be bounded by device parallelism instead of per-row latency, which matters most on SMB.

### 3.4 Cheaper Verification

- CRC32C is single-stream (`_mm_crc32_u64` loop, latency-bound). A 3-way interleaved implementation with a combine step is typically 2–3× faster on 4 KB inputs. This applies to every block read, WAL sector seal, and blob read.
- With §3.1, cached hits skip verification entirely.

### 3.5 Fewer Syscalls per Buffered Commit

- `BufferedImmediateOsCache` rewrites a full 4 KB WAL sector per commit, plus a separate blob `WriteFile` when a value is out of line.
- Options:
  - Group commit: when several connections commit concurrently (possible after A2), coalesce their batches into one sector write.
  - An opt-in `BufferedUserSpaceCoalesced` default for bulk-load paths.
- Keep the post-audit invariant that a synced sector is never rewritten in place.

### 3.6 POSIX `HandlePool` (designed, not implemented)

- `ARCHITECTURE.md` §6 and PAL chapter §5 specify an LRU descriptor cache (default cap `128`) for cold blob segments on POSIX. `src/` does not implement it.
- With macOS `RLIMIT_NOFILE = 256`, a database with many blob segments can exhaust descriptors.
- The design must coexist with lazy-cursor pinning (`LazyBlob` holds an `Arc` to the file). Pinned handles must not be closed by the pool, so the pool needs reference-aware eviction.

### 3.7 Smaller Items

- Avoid per-entry key/value clones in `MemTable::write_to_segment` and decode-before-filter in `scan_prefix_range`.
- Blob reads: avoid zeroing the 16 KB stack buffer and the extra copy. Read directly into the `Vec` spare capacity with a streaming CRC.
- Release profile: `lto = "fat"`, `codegen-units = 1`. Keep `panic = "unwind"`, because the FFI layer relies on `catch_unwind`.
- Transaction overlay (stage A1) double lookup: owner reads check the overlay and then the MemTable. Consider a combined lookup, or skipping the overlay probe when it is empty or when the key range is untouched. This would recover the 15–25% regression measured on large single-transaction updates.

---

## 4. Suggested Order

| Step | Item | Depends on | Expected gain |
| :--- | :--- | :--- | :--- |
| 1 | §3.4 CRC interleave, §3.7 small items | — | Low risk, broad few-% gains |
| 2 | §3.2 inline threshold benchmarking | — | Large for 1–3 KB values |
| 3 | §3.1 block cache (idx blocks first) | A2 (`RwLock` reads) | Largest for SSTable mode / YCSB |
| 4 | §3.3 blob prefetch | — | Range scans, SMB |
| 5 | §3.5 group commit | A2 | Multi-connection write throughput |
| 6 | §3.6 HandlePool | — | Correctness on macOS at scale |

Each step should land with before/after numbers from `standard_sql_bench` and, where relevant, the latency and concurrency examples (`compaction_latency_bench`, `tx_isolation_read_bench`, `concurrent_read_bench`).

---

## 5. Documents to Update When Implemented

- `ARCHITECTURE.md` §1.1 (heap budget), §3.2 (cache principle), §6 (PAL `read_many_at`).
- `docs/phase1/01_PAL_AND_OS_CACHE.md` (double-caching discussion, `HandlePool` status).
- `docs/phase1/04_WISCKEY_AND_SEGMENTS.md` (inline threshold rationale).
- README benchmark table.
