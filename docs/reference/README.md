# API Reference Guide

This map groups the current application-facing API by task. Read [Public API Boundaries](PUBLIC_API.md) for the distinction between application, advanced integration, and implementation surfaces. Start with the [native storage](../guides/NATIVE_STORAGE.md) or [SQLite](../guides/SQLITE.md) guide for complete usage examples.

## Native storage reference map

| Task | API | Source |
| :--- | :--- | :--- |
| Open storage | `OsFs::open`, `StorageEngine::open` | [PAL](../../src/pal/mod.rs), [engine](../../src/engine/mod.rs) |
| Configure resources | `EngineConfig` | [engine](../../src/engine/mod.rs) |
| Select guarantees | `WriteDurability`, `CorruptPolicy` | [engine](../../src/engine/mod.rs) |
| Read and write | `put_row`, `put_row_owned`, `get_row`, `row_exists`, `delete_row` | [engine](../../src/engine/mod.rs) |
| Ordered scans | `scan_table`, `scan_table_range` | [scans](../../src/engine/scan.rs) |
| Secondary-key lookup | `lookup_by_secondary_index` | [scans](../../src/engine/scan.rs) |
| Transactions | `begin_transaction`, `commit_transaction`, `rollback_transaction`, `in_active_transaction` | [transactions](../../src/engine/tx.rs) |
| Savepoints | `savepoint`, `rollback_to_savepoint`, `release_savepoint` | [transactions](../../src/engine/tx.rs) |
| Capped tables | `register_capped_table`, `capped_table_state` | [eviction](../../src/engine/eviction.rs) |
| Runtime policies | `set_table_corrupt_policy`, `reset_table_corrupt_policy`, `corrupt_policy`, `set_table_inline_threshold`, `reset_table_inline_threshold`, `effective_inline_threshold` | [engine](../../src/engine/mod.rs) |
| Maintenance | `checkpoint`, `flush_memtable`, `compact`, `compact_auto_tiered` | [compaction](../../src/engine/compaction.rs) |
| Health and diagnostics | `is_poisoned`, `maintenance_failures`, `last_maintenance_error`, `corrupt_blob_detections`, active segment counts | [engine](../../src/engine/mod.rs) |

### Core signatures

```rust,ignore
StorageEngine::open(fs: Arc<FS>, config: EngineConfig) -> io::Result<Self>
put_row(&mut self, table_id: u16, pk: &[u8], value: &[u8],
        secondary_keys: &[(u16, Vec<u8>)], durability: WriteDurability) -> io::Result<()>
get_row(&mut self, table_id: u16, pk: &[u8]) -> io::Result<Option<Vec<u8>>>
delete_row(&mut self, table_id: u16, pk: &[u8], durability: WriteDurability) -> io::Result<bool>
scan_table_range(&mut self, table_id: u16,
                 lower_bound: Option<(&[u8], bool)>, upper_bound: Option<(&[u8], bool)>,
                 limit: usize) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>>
lookup_by_secondary_index(&self, table_id: u16, index_id: u16,
                          col_val: &[u8]) -> io::Result<Vec<Vec<u8>>>
```

These are signature summaries, not a compilable program. Exact signatures and per-item comments live in Rustdoc. The guides specify key encoding ownership, bounds, transaction visibility, durability, quotas, and error recovery. Internal on-disk formats belong to the textbook and are not the native row API.

### Configuration groups

`EngineConfig::default()` is the starting point. Consult its Rustdoc for types and defaults before overriding fields.

| Concern | Fields |
| :--- | :--- |
| WAL behavior | `sync_wal_size_bytes`, `buf_wal_size_bytes`, `buf_wal_mode` |
| Memory and lookup cost | `memtable_capacity_bytes`, `block_cache_bytes`, `blob_cache_bytes`, `bloom_bits_per_key` |
| Values and segments | `inline_threshold_bytes`, `max_blob_segment_bytes` |
| LRU tracking | `lru_read_tracking`, `max_in_memory_access_ticks` |
| Compaction scheduling | `auto_compact_segment_threshold`, `background_compaction`, `max_segments_before_inline_compaction` |
| Transactions and writer contention | `max_transaction_bytes`, `writer_wait_timeout_ms` |

These budgets do not bound the entire process heap: filters, fence pointers, pinned read versions, and other structures need separate accounting. See [architecture requirements](../ARCHITECTURE.md#11-core-requirements).

## SQLite wrapper reference map

| Task | API |
| :--- | :--- |
| Open the SQLite schema | `SqliteConnection::open_in_memory`, `open_file` |
| Attach the shared storage engine | `register_aydb_module`, `register_aydb_module_with_options` |
| Execute SQL without returned rows | `execute_batch` |
| Prepare statements | `prepare` returning `SqliteStatement<'_>` |
| Bind and execute | `SqliteStatement::execute` |
| Bind and read results | `SqliteStatement::query`, `SqliteConnection::query_all` |
| Values | `SqlValue::{Null, Integer, Float, Text, Blob}` |

See [connection wrappers](../../src/vtab/connection.rs), [value definitions](../../src/vtab/row_codec.rs), and the [SQLite guide](../guides/SQLITE.md) for setup and adapter options. Raw FFI and row encoding are separate integration surfaces.

## Generate item-level Rustdoc

From the repository root:

```powershell
cargo doc --no-deps --lib
```

Open `target/doc/aydb/index.html` locally. The current site generator publishes Markdown documentation and benchmark results; Rustdoc is not yet included in that publication pipeline. Source links above provide access from both the repository and the rendered documentation site. Generating Rustdoc lists currently public items, including implementation APIs; use the boundary document to choose supported application entry points. Existing comments have uneven coverage, so this map and the guides complement them.

## Documentation maintenance and future manuals

API changes should update their Rustdoc contracts (arguments, return values, errors, ownership, and examples), the relevant guide, and this reference map together. Future vector/search primitives extend the native section with dimensions, metrics, quantization, filtering, visibility, and resource contracts. Future native SQL gets a separate execution API reference and SQL language manual when implemented. Planned syntax must remain clearly distinguished from available functionality.
