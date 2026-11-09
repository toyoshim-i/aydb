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

## Transactions, values, and limitations

Use ordinary SQL `BEGIN`, `COMMIT`, `ROLLBACK`, and `SAVEPOINT` statements through `execute_batch`, with parameterized statements for application data. The wrappers use `SqlValue::{Null, Integer, Float, Text, Blob}` for bindings and results. `query_all` and statement `query` materialize results; bound the result size for large datasets. Follow Rust lifetimes and drop statements before their connection.

The adapter implements column affinity and supported collations (`BINARY`, `NOCASE`, `RTRIM`), column `NOT NULL`/`UNIQUE`, and primary-key conflict handling. It does not make every native SQLite table feature available. Unsupported column constructs include `DEFAULT`, per-column `ON CONFLICT`, and comments in column definitions; table-level constraints are rejected. Use `indexes` for the adapter's supported secondary indexes. The [adapter chapter](../phase2/01_SQLITE_VTAB_ADAPTER.md) contains the detailed implementation and pushdown behavior.

Read transactions retain a committed snapshot. Upgrading an old snapshot to a writer can return `SQLITE_BUSY`; restart the transaction rather than continuing from stale reads. Long read transactions/cursors can retain old files and memory. Concurrent writers wait for a bounded interval configured with `writer_wait_timeout_ms`. The Rust wrappers expose failures as `std::io::Error`; do not assume they provide a typed SQLite error-code API.

An engine I/O failure can require reopening, and post-commit maintenance failures have separate diagnostics. See [native error handling](NATIVE_STORAGE.md#maintenance-errors-and-shutdown). Finish SQL transactions and release statements/connections before checkpointing or releasing the engine.

## Planned features

The native SQL frontend, `VECTOR` types, hybrid search syntax, and compression policies in the roadmap are not current adapter options. Their documentation will be added with their implementation. General SQLite SQL behavior remains the responsibility of SQLite's own documentation; this guide records the aydb-specific integration contract.
