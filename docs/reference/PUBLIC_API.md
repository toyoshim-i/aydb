# Public API Boundaries

This page defines the intended application-facing entry points and separates them from integration and implementation machinery. It describes the current paths; it does not change Rust visibility or introduce a new facade. The crate is currently version `0.1.0`; this classification does not promise a frozen API or on-disk format.

## Application interfaces

| Interface | Current entry points | Responsibility |
| :--- | :--- | :--- |
| Native storage | `aydb::engine::{StorageEngine, EngineConfig, WriteDurability, CorruptPolicy}` | Opaque byte rows, secondary keys, scans, transactions, quotas, maintenance, and diagnostics |
| Filesystem setup | `aydb::pal::OsFs` | Open the directory used by a native engine |
| SQLite integration | `aydb::vtab::connection::{SqliteConnection, SqliteStatement}` | Register the adapter, execute SQL, bind parameters, and read results |
| SQL values | `aydb::vtab::row_codec::SqlValue` | Parameter and result values for the SQLite Rust wrappers |

These are two ways to use the same storage foundation. Choose one owner for a table's schema and encoding. Native byte rows are not automatically SQL rows; do not write arbitrary bytes into tables managed by the adapter. Native callers manage their own table IDs and value formats; SQL callers let the adapter manage its catalog, row encoding, and indexes. Reserve table ID `0` for the engine. Prefer separate stores if an application cannot coordinate native and SQL namespaces safely.

## Native application subset

Use `StorageEngine::open`, row CRUD, ordered scans, secondary-key lookup, native transaction/savepoint methods, capped-table registration, per-table policies, checkpointing, and health/maintenance diagnostics as the normal application entry points. See the [native guide](../guides/NATIVE_STORAGE.md) and [reference map](README.md).

Owned-value and caller-buffer variants are advanced native operations. Lazy blob reads and staged compaction require explicit lifetime and synchronization handling; consult their Rustdoc and the implementation chapters before using them. Benchmark-only policy choices, such as eager LRU writes on reads, are not recommended defaults.

## Integration and implementation surfaces

Rust `pub` visibility currently exposes more than the intended application subset. A generated Rustdoc listing is therefore broader than an application API catalog.

| Surface | Intended audience |
| :--- | :--- |
| `*_for_conn`, connection read views, `register_or_resolve_vtab*`, `unregister_vtab` | SQLite adapter and other engine integrations that manage connection ownership/catalog lifecycles |
| `put_row_owned_with_hint`, compaction hooks, shared-reader retry protocols | Integrations that can uphold the relevant engine-lock and visibility invariants |
| `CompactionJob`, `CompactionOutput`, `prepare_compaction`, `install_compaction` | Custom maintenance schedulers; consume the documented prepare/run/install protocol |
| `PlatformFs`, `PlatformFile` | Custom filesystem backends and fault-injection implementations |
| `blob`, `cache`, `index`, `lru`, `memtable`, `meta`, `wal`, `util` internals | Engine development, format analysis, and benchmarks; visibility alone is not a recommendation to manipulate persisted structures |
| `vtab::ffi`, row codec internals, module callbacks | SQLite FFI and adapter implementation; `SqlValue` is the application-facing exception in the codec module |

The methods above remain callable where currently public. They are separated here to prevent accidental dependence on implementation protocols. Enforcing these boundaries with visibility changes, re-exports, or facade modules is follow-up code work and must preserve existing callers or document migration.

## Future additions

* Vector insertion, deletion, and search belong to the native backend API, with their own type, metric, consistency, and resource contracts. They are also made available through SQL frontends.
* Native SQL needs an execution API reference and a manual for supported syntax, types, functions, and aydb extensions. It is not implemented yet.
* CLI/MCP endpoints, source parsing, and embedding inference belong to application tooling; they compose the backend rather than becoming another database API inside the crate.
