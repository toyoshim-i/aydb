# Native Storage Guide

Use the native interface when an application owns the byte encoding and wants direct storage operations without SQL. For SQL-managed tables, use the [SQLite guide](SQLITE.md). Read the [API boundaries](../reference/PUBLIC_API.md) before using lower-level modules.

## Open, write, read, and reopen

The following is a complete Rust program for a crate depending on a local checkout of `aydb` (`aydb = { path = "/path/to/aydb" }` in its Cargo dependencies). Run it from a directory where creating `native-data` is appropriate.

```rust
use std::sync::Arc;
use aydb::engine::{EngineConfig, StorageEngine, WriteDurability};
use aydb::pal::OsFs;

fn main() -> std::io::Result<()> {
    let fs = Arc::new(OsFs::open("native-data", true)?);
    let mut db = StorageEngine::open(fs, EngineConfig::default())?;
    let table = 1;

    db.put_row(table, b"theme", b"dark", &[], WriteDurability::SyncTransactional)?;
    assert_eq!(db.get_row(table, b"theme")?, Some(b"dark".to_vec()));

    // A checkpoint reduces recovery work; it is not needed to make this sync write durable.
    db.checkpoint()?;
    drop(db);

    let fs = Arc::new(OsFs::open("native-data", false)?);
    let mut db = StorageEngine::open(fs, EngineConfig::default())?;
    assert_eq!(db.get_row(table, b"theme")?, Some(b"dark".to_vec()));
    Ok(())
}
```

`OsFs::open(path, true)` permits directory creation; it does not reset an existing database. Open one engine for a store and share that instance when necessary. A database is a directory of managed files. Do not edit, remove, or independently open its files while it is in use. SMB storage follows the single-host exclusive access model in the [architecture](../ARCHITECTURE.md#21-single-host-exclusive-handle-model).

## Rows, ordering, and secondary keys

* A native table is a `u16` namespace. Use nonzero IDs and keep an application-owned mapping of IDs to meanings; a normal native table needs no SQL DDL. Table `0` is reserved.
* Keys and values are byte slices. The engine does not infer a schema or serialize application types. Primary keys currently have a maximum length of 2,048 bytes.
* Primary-key scans follow byte order. Encode numbers and composite keys consistently if their logical order must match scan order; decimal text does not generally sort numerically.
* `put_row` inserts or overwrites. Pass the row's complete secondary-key set as `&[(index_id, encoded_value)]`; use `&[]` when none are needed. The engine maintains these entries with the row. Native secondary keys do not provide SQL `UNIQUE` semantics.
* `lookup_by_secondary_index(table_id, index_id, value)` returns matching primary keys, not row payloads. Fetch the rows with `get_row`.
* `get_row` returns `Ok(None)` for an absent row. `delete_row` returns whether a row was deleted.
* `scan_table` materializes all live rows. For larger tables, use `scan_table_range(table_id, lower, upper, limit)`, whose optional bounds are `(key_bytes, inclusive)`. To page forward, use the last returned key as an exclusive lower bound. Separate calls do not create a snapshot across concurrent changes.

## Transactions and durability

For a single native caller, begin with `begin_transaction()`, perform row operations, then call `commit_transaction()`. Reads through the owner see its uncommitted changes. If a mutation fails before commit, explicitly roll back with `rollback_transaction()` and handle any rollback error; Rust scope exit is not a transaction guard. Savepoints use `savepoint`, `rollback_to_savepoint`, and `release_savepoint`.

Each mutation specifies `WriteDurability`. A transaction containing a synchronous mutation is committed synchronously; all-buffered mutations use buffered durability. `commit_transaction()` itself takes no durability argument.

| Mode | Successful return means |
| :--- | :--- |
| `SyncTransactional` | The commit and all earlier buffered commits are durable, assuming the storage honors flush semantics |
| `BufferedFast` | The commit has reached the OS page cache and survives a process crash; recent buffered commits may be lost on OS crash or power loss |

An explicit transaction is committed when the outer commit succeeds, not when an individual `put_row` returns. The transaction overlay has its own memory limit (`max_transaction_bytes`, default 64 MiB). Avoid holding an application lock while waiting for a different caller that needs the same lock to finish a transaction. The connection ownership/group-commit machinery is an integration surface, not a substitute for native application synchronization.

## Bounded caches and policies

Call `register_capped_table(table_id, max_bytes)` to configure automatic approximate LRU eviction. Successful registration persists quota configuration outside a transaction; registration in a transaction is committed with it. `capped_table_state` reports quota accounting. Reads normally coalesce recency information in memory; recency is approximate and is not a durable access history.

The quota measures logical live row bytes, not the total size of files in the directory. WALs, indexes, stale data, pinned generations, and temporary maintenance outputs require additional space. Maintenance failure can leave a quota exceeded until a later retry; monitor the diagnostics below. Do not put irreplaceable settings in a table intended to evict rows.

`CorruptPolicy::Error` reports corrupt blob reads, while `CorruptPolicy::Evict` treats them as misses and queues cleanup. Defaults are `Error` for ordinary tables and `Evict` for capped tables. Overrides set with `set_table_corrupt_policy` are runtime settings and must be reapplied after reopening. Per-table inline threshold overrides are runtime settings as well.

## Maintenance, errors, and shutdown

* `checkpoint()` persists the current state and reduces next-open replay. It fails while a transaction is active. Dropping an engine does not checkpoint it.
* Automatic compaction is configured through `EngineConfig`. Native applications using inline defaults need not implement the staged compaction protocol.
* After an I/O error, check `is_poisoned()`. An uncertain WAL/metadata write or sync, or a blob sync failure, can poison the engine. Stop mutations, release the engine and its readers, and reopen for recovery. The failed commit may or may not be recovered, but must not be partially applied; do not blindly retry non-idempotent application operations.
* A committed write can return success while subsequent maintenance fails. Inspect `maintenance_failures()` and `last_maintenance_error()` separately. These diagnostics do not undo a successful commit.
* When sharing an engine, callers commonly use `Arc<RwLock<_>>`. The simple row/read methods in this guide require exclusive mutable access. Advanced shared read views have separate protocols; see their Rustdoc instead of bypassing synchronization.

See the [reference map](../reference/README.md) for configuration and method locations, and the [durability specification](../ARCHITECTURE.md#4-crash-recovery-and-durability) for exact failure contracts.
