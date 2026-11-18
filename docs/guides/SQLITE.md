# SQLite Adapter Guide

The adapter stores virtual table rows in `aydb` and uses the host SQLite library for SQL parsing and execution. This guide covers setup, aydb options, and differences relevant to callers. It does not duplicate the SQLite language manual. The [API boundary document](../reference/PUBLIC_API.md) separates the Rust wrappers from adapter internals.

## Open a persistent SQL application

The crate links to `winsqlite3` on Windows and `sqlite3` on Unix. A compatible system library and the platform's link requirements must be available. The wrapper does not bundle SQLite.

This complete program stores SQLite schema in `sql-app.sqlite` and virtual table contents in the separate `sql-data` directory. Run it from a directory where those paths are appropriate, using a crate with a local `aydb` dependency as in the [native guide](NATIVE_STORAGE.md).

```rust
use std::path::Path;
use std::sync::{Arc, RwLock};
use aydb::engine::{EngineConfig, StorageEngine};
use aydb::pal::OsFs;
use aydb::vtab::connection::SqliteConnection;
use aydb::vtab::row_codec::SqlValue;

fn main() -> std::io::Result<()> {
    let fs = Arc::new(OsFs::open("sql-data", true)?);
    let engine = Arc::new(RwLock::new(StorageEngine::open(fs, EngineConfig::default())?));
    let conn = SqliteConnection::open_file(Path::new("sql-app.sqlite"))?;
    conn.register_aydb_module(Arc::clone(&engine))?;
    conn.execute_batch(
        "CREATE VIRTUAL TABLE IF NOT EXISTS app_config USING aydb(
            durability = 'sync',
            columns = 'key TEXT PRIMARY KEY, value TEXT'
        );"
    )?;

    let mut insert = conn.prepare("INSERT OR REPLACE INTO app_config VALUES (?1, ?2)")?;
    insert.execute(&[SqlValue::Text("theme".into()), SqlValue::Text("dark".into())])?;
    drop(insert);
    let rows = conn.query_all("SELECT value FROM app_config WHERE key = ?1",
        &[SqlValue::Text("theme".into())])?;
    assert_eq!(rows, vec![vec![SqlValue::Text("dark".into())]]);

    // Finish statements and SQL transactions before closing the connection.
    drop(conn);
    engine.write().unwrap().checkpoint()?;
    Ok(())
}
```

On every open, open the same storage directory and SQLite schema file, register `aydb` on the connection, then issue SQL. SQLite persists the virtual table declaration; aydb persists the name-to-table-ID catalog and row contents. Keep the schema and storage paired. `open_in_memory()` makes the SQLite schema transient, so applications using it must recreate compatible virtual table declarations after opening a new connection. Recreating a declaration is different from `DROP TABLE`, which deletes the aydb table's data.

The default module registration starts the adapter's background compaction worker. Use `register_aydb_module_with_options(engine, false)` to avoid spawning that worker; it leaves the engine's existing compaction setting unchanged. Register every connection against the same shared engine rather than opening competing engines for one directory.

## Virtual table options

```sql
CREATE VIRTUAL TABLE http_cache USING aydb(
    max_bytes = 104857600,
    durability = 'buffered',
    on_corrupt = 'evict',
    inline_max = 1024,
    columns = 'key TEXT PRIMARY KEY, host TEXT, body BLOB',
    indexes = 'host'
);
```

| Option | Meaning and default |
| :--- | :--- |
| `columns` | Column declarations; default `key TEXT PRIMARY KEY, value BLOB`. Prefer an explicit single-column primary key. |
| `table_id` | Optional nonzero `u16`; normally omit it and let the persistent catalog assign an ID. Explicit IDs must not collide with other tables. |
| `durability` | `sync` or `buffered`; default `buffered`. Choose `sync` explicitly for critical settings. |
| `max_bytes` | Logical live row quota in bytes; default `0` disables capping. This does not bound total physical directory size. |
| `on_corrupt` | `error` or `evict`; default is `evict` for capped tables and `error` otherwise. |
| `inline_max` | Optional per-table threshold, `0` through `4096`; otherwise use the engine's effective threshold. |
| `indexes` | Comma-separated names of existing columns for single-column secondary indexes; empty by default. Declare these at table creation. |

Unknown options and invalid values are rejected. These are adapter options, not general SQLite `PRAGMA` settings. In particular, SQLite `PRAGMA synchronous` does not select the aydb WAL durability mode; the virtual table's `durability` option does.

## Damaged write-ahead log

The adapter never opens the engine: the application does, then passes the shared engine to `register_aydb_module`. WAL recovery is therefore configured and observed on the engine, not through SQL. The adapter has no option, pragma or SQL function for it.

`EngineConfig::wal_damage_policy` decides what `StorageEngine::open` does when recovery finds that acknowledged commits were lost from the WAL, for example after a bad sector, a bit flip or a log file truncated by a third party. The ordinary loss of unsynced `BufferedFast` commits after a power cut is not damage and is never reported.

* `WalDamagePolicy::Truncate` (default) replays the intact prefix of the log, discards the rest and opens. This matches SQLite, which also keeps the WAL frames before the first bad one. The tables then hold exactly the commits below the first lost one, in both durability modes, never a later commit without an earlier one.
* `WalDamagePolicy::Fail` makes `open` return `io::ErrorKind::InvalidData` whose inner error is a `WalDamage` (`err.get_ref()` and `downcast_ref::<WalDamage>()`). No file is written or changed, so the application can restore a backup or retry with salvage. A log file that does not exist at all is still created empty before the check.

```rust
use aydb::engine::{EngineConfig, StorageEngine, WalDamagePolicy};

let mut config = EngineConfig::default();
config.wal_damage_policy = WalDamagePolicy::Fail;
let engine = StorageEngine::open(fs, config)?; // before the SQLite connection exists
let report = engine.last_wal_recovery();       // also available after a Truncate open
if let Some(damage) = &report.damage {
    // log damage.reason, damage.first_lost_lsn, damage.discarded_batches
}
```

`last_wal_recovery()` describes the most recent `open`: replayed batches per stream, the last replayed LSN, a count of torn tail sectors (informational), and `damage`, which is `Some` only when acknowledged commits were lost. Under the default policy this report is the only trace of the loss, so an application should log it. Call it on the engine before or after wrapping the engine in `Arc<RwLock<..>>`.

`StorageEngine::open_with_salvage` is an engine-level API and the adapter does not expose it. It opens as `Truncate` whatever the policy says and returns the intact transactions that recovery discarded (`SalvagedTx`, in LSN order). SQLite itself salvages nothing. An application that needs the data calls `open_with_salvage` before it creates the SQLite connection, then writes the rows back through the engine API or SQL. The values are engine-encoded rows: decode them with the row codec (`aydb::vtab::row_codec::decode_all_columns`) and re-insert them through SQL, which also rebuilds the indexes, rowids and unique keys the adapter keeps for the table. Salvage returns only primary-row puts and deletes. A transaction that contains a schema change, a table the catalog does not know or an unreadable blob is dropped whole and only counted in `WalDamage::unsalvageable`. `aydb` is pre-release and reads no older WAL format; see ARCHITECTURE.md 4.6.5 for how a log in an older format is treated.

## Transactions, values, and limitations

Use ordinary SQL `BEGIN`, `COMMIT`, `ROLLBACK`, and `SAVEPOINT` statements through `execute_batch`, with parameterized statements for application data. The wrappers use `SqlValue::{Null, Integer, Float, Text, Blob}` for bindings and results. `query_all` and statement `query` materialize results; bound the result size for large datasets. Follow Rust lifetimes and drop statements before their connection.

The adapter implements column affinity and supported collations (`BINARY`, `NOCASE`, `RTRIM`), column `NOT NULL`/`UNIQUE`, and primary-key conflict handling. `UNIQUE` columns are checked through a persistent index, so the cost of a check does not grow with the table. It does not make every native SQLite table feature available: `CHECK`, `DEFAULT`, per-column `ON CONFLICT`, comments in column definitions and table-level constraints are rejected. [Differences from SQLite tables](SQLITE_DIFFERENCES.md) lists every known difference, with what SQLite and the SQL standard do. `aydb` has not been released and reads no on-disk format written by an earlier build; a table without its persistent rowid mapping is refused at connect with `SQLITE_CORRUPT`. Use `indexes` for the adapter's supported secondary indexes. The [adapter chapter](../phase2/01_SQLITE_VTAB_ADAPTER.md) contains the detailed implementation and pushdown behavior.

Read transactions retain a committed snapshot. Upgrading an old snapshot to a writer can return `SQLITE_BUSY`; restart the transaction rather than continuing from stale reads. Long read transactions/cursors can retain old files and memory. Concurrent writers wait for a bounded interval configured with `writer_wait_timeout_ms`. The Rust wrappers expose failures as `std::io::Error`; do not assume they provide a typed SQLite error-code API.

An engine I/O failure can require reopening, and post-commit maintenance failures have separate diagnostics. See [native error handling](NATIVE_STORAGE.md#maintenance-errors-and-shutdown). Finish SQL transactions and release statements/connections before checkpointing or releasing the engine.

## Planned features

The native SQL frontend, `VECTOR` types, hybrid search syntax, and compression policies in the roadmap are not current adapter options. Their documentation will be added with their implementation. General SQLite SQL behavior remains the responsibility of SQLite's own documentation; this guide records the aydb-specific integration contract.
