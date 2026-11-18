# Differences from SQLite tables

This page lists where an `aydb` virtual table behaves differently from an ordinary SQLite table. For each difference it gives what the SQL standard says, what a native SQLite table does, what `aydb` does, whether the difference could be removed, and the test that pins the `aydb` behaviour. Anything not listed is meant to behave like a native table; report a difference that is missing here as a bug.

Checked against the code at commit `cbdda30`. "Not verified" marks a cell taken from the SQLite documentation or from reading the code, not from a test that runs it. The SQL standard column refers to ISO/IEC 9075 (SQL:2023) and was written without checking the text of the standard, so every entry in it is "Not verified". "—" in that column means the standard has no such feature.

The [SQLite guide](SQLITE.md) covers setup and adapter options.

Each `aydb` cell starts with one of two words:

* **Refused:** `aydb` returns an error where SQLite would accept the statement. A refusal never stores data that differs from what SQLite would store.
* **Differs:** `aydb` accepts the statement, but the result is not what a native table gives.

The "Avoidable?" column is a judgement from the design, not a plan:

* **Yes:** the current design allows it; it has not been done.
* **Format change:** possible, but it needs a new on-disk record or encoding.
* **Design choice:** possible, but `aydb` chose its behaviour on purpose.
* **Hard (aydb):** follows from a core design decision of `aydb`; removing it would change that decision.
* **Hard (SQLite):** the SQLite virtual table interface does not give the adapter what it would need.

## Column definitions

| Feature | SQL standard | Native SQLite | `aydb` | Avoidable? | Pinned by |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `COLLATE` | Collations are defined by the implementation | `BINARY`, `NOCASE`, `RTRIM` and application-defined collations | **Refused** for any collation other than `BINARY`, `NOCASE` and `RTRIM` | **Hard (aydb).** Keys and index entries are stored as bytes normalised by the collation, so that byte order is the collation order. An application collation is only a comparison function and gives no such bytes. | `ddl.rs` (unit tests) |
| `CHECK` | Enforced | Enforced | **Refused.** `CREATE VIRTUAL TABLE` fails with "CHECK constraints are not supported by aydb tables". A table that already has a `CHECK` connects, and `SELECT`, `DELETE`, `ALTER TABLE ... RENAME` and `DROP TABLE` work, but every `INSERT` and `UPDATE` fails. | **Yes.** The adapter could evaluate the expression through SQLite against the new row. | `check_tests.rs` |
| `DEFAULT` | Supported | Supported | **Refused.** `CREATE VIRTUAL TABLE` fails with "DEFAULT values are not supported by aydb tables". A table that already has a `DEFAULT` connects, and `SELECT`, `DELETE`, `ALTER TABLE ... RENAME` and `DROP TABLE` work, but every `INSERT` and `UPDATE` fails. | **Hard (SQLite).** An omitted column reaches the adapter as NULL, the same as an explicit NULL, so the adapter cannot tell when to apply the default. Not verified. | `check_tests.rs` |
| Generated columns (`GENERATED ALWAYS AS`, `AS`) | Supported (SQL:2003) | Supported, but not on virtual tables | **Refused** at `CREATE VIRTUAL TABLE` | **Hard (SQLite).** SQLite refuses to declare a generated column for a virtual table. | `ddl.rs` (unit tests) |
| Column `REFERENCES` | Enforced | Enforced only with `PRAGMA foreign_keys = ON`, and not for virtual tables | **Differs.** Accepted and ignored. Not verified. | **Yes,** with large effort: the adapter would have to check and cascade across tables itself. | — |
| Per-column `ON CONFLICT` | — | Sets the default conflict mode of the constraint | **Refused** at `CREATE VIRTUAL TABLE` | **Hard (SQLite).** `sqlite3_vtab_on_conflict` reports `ABORT` both for a statement without a conflict clause and for `OR ABORT`, so the adapter cannot tell when the column's mode applies. Not verified. | `ddl.rs` (unit tests) |
| Table constraints (composite `PRIMARY KEY`, table `UNIQUE`, `CHECK`, `FOREIGN KEY`) | Supported | Supported | **Refused** at `CREATE VIRTUAL TABLE`. A primary key has exactly one column. | **Format change.** Composite keys and multi-column unique indexes need a new key encoding. Table `CHECK` and `FOREIGN KEY` are as in the rows above. | `ddl.rs` (unit tests) |
| Comments inside `columns = '...'` | `--` and `/* */` are standard syntax | Allowed | **Refused** outside quoted names and strings | **Yes.** Parser work only. | `ddl.rs` (unit tests) |

## Keys and rowids

| Feature | SQL standard | Native SQLite | `aydb` | Avoidable? | Pinned by |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `AUTOINCREMENT` | Identity columns (`GENERATED ... AS IDENTITY`, SQL:2003) and sequence generators: values only move forward and are not reused after a delete, unless `CYCLE` is given; gaps are allowed | Keys are never reused, even after the largest one is deleted (`sqlite_sequence`) | **Differs.** Accepted but ignored: the next automatic key is `max + 1`, so the largest key can be handed out again after it is deleted. Not verified. | **Format change.** The largest key ever used would have to be stored per table. | — |
| NULL inserted into a primary key other than `INTEGER PRIMARY KEY` | A primary key is never NULL | Stored as NULL (a historical SQLite quirk), unless the column is `NOT NULL` | **Refused.** `INT PRIMARY KEY` and `BIGINT PRIMARY KEY` get no automatic key either, as in SQLite. | **Hard (aydb).** The primary key is the storage key and must be unique, so two NULL keys would collide. | `key_null_tests.rs` |
| `rowid` of a `TEXT` (or other non-integer) key | — | Assigned in sequence (1, 2, 3, …) | **Differs.** A stable value derived from a hash of the key, kept in a persistent mapping. Do not rely on its order or size. | **Yes.** The persistent mapping could hand out `max + 1` instead of a hash. | `rowid_tests.rs` |
| `rowid` of an `INT` / `BIGINT` key | — | Separate from the key | **Differs.** The rowid is the key value. | **Hard (aydb).** Integer keys are used as their own rowid, without a mapping. A separate rowid would need a mapping on every read path. | `rowid_write_tests.rs` |
| Writing a `rowid` that differs from an `INT` / `BIGINT` key | — | Sets the rowid | **Refused** with `SQLITE_MISMATCH` | **Hard (aydb),** as in the row above | `rowid_write_tests.rs` |
| `UPDATE` that changes an `INT` / `BIGINT` key | — | The row keeps its rowid | **Differs.** The rowid changes with the key. | **Hard (aydb),** as in the row above | `pk_update_tests.rs` |

## Conflict handling

| Feature | SQL standard | Native SQLite | `aydb` | Avoidable? | Pinned by |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `changes()` after a multi-row `UPDATE OR REPLACE` | No `OR REPLACE` (`MERGE` is the nearest statement); the row count is read with `GET DIAGNOSTICS ... ROW_COUNT` | Counts the rows updated | **Differs.** Can be one higher. SQLite still passes `aydb` a row that an earlier `REPLACE` in the same statement deleted, and counts the call. | **Hard (SQLite).** SQLite counts every update call it makes, and the adapter cannot cancel one. | `pk_update_tests.rs` |
| `UPDATE OR REPLACE` that moves an integer-key row onto a rowid the statement has not reached yet | No `OR REPLACE` and no rowid | Each row gets its own new values | **Differs.** The moved row gets the values SQLite computed for the row it replaced. | **Hard (aydb and SQLite).** SQLite queues the rowids before it updates, and an integer key is its own rowid. | `pk_update_tests.rs` |

## Schema statements

| Feature | SQL standard | Native SQLite | `aydb` | Avoidable? | Pinned by |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `DROP TABLE` inside a transaction or savepoint | Whether schema statements can run in a transaction is up to the implementation | Fully transactional | **Refused.** The table and its data stay. SQLite shows only "SQL logic error", because it does not pass on the virtual table's message. Outside a transaction `DROP TABLE` works. | **Hard (SQLite).** SQLite starts no virtual table transaction for `DROP TABLE` and never reports whether the surrounding transaction committed. | `drop_tests.rs` |
| `ALTER TABLE ... RENAME TO` as the first statement of a transaction | `ALTER TABLE` has no rename | Fully transactional | **Refused** until the transaction has written to an `aydb` table. After that the rename works and `ROLLBACK` undoes it. | **Hard (SQLite).** SQLite starts no virtual table transaction before the rename, so the adapter has no transaction to put it in. | `rename_tests.rs` |
| `ALTER TABLE ... RENAME TO` a name another `aydb` table on the same engine uses | Table names are unique in a schema | Not possible: names are unique in one schema | **Refused.** Only matters when several schema files share one engine. | **Hard (aydb).** One engine has one table-name catalog, shared by every schema file that uses it. | `rename_tests.rs` |
| `CREATE INDEX`, triggers, `ALTER TABLE ADD COLUMN` | Supported | Not allowed on any virtual table | **Refused** by SQLite. Use the `indexes` option for secondary indexes. Not verified. | **Hard (SQLite)** | — |

## Introspection

| Feature | SQL standard | Native SQLite | `aydb` | Avoidable? | Pinned by |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `PRAGMA table_info` column `pk` | No `PRAGMA`; columns and key constraints are described in `INFORMATION_SCHEMA` (`COLUMNS`, `TABLE_CONSTRAINTS`, `KEY_COLUMN_USAGE`) | Position in the primary key | **Differs.** `0` for a primary key that is not `INTEGER PRIMARY KEY`. | **Hard (SQLite).** Declaring the key to SQLite makes it build an automatic index with no pages, which `PRAGMA integrity_check` reports as damage. | `integrity_tests.rs` |
| `PRAGMA index_list` | Indexes are outside the standard | Lists the automatic indexes of `PRIMARY KEY` and `UNIQUE` | **Differs.** Lists none for `aydb` tables, which enforce both through their own storage. Not verified. | **Hard (SQLite),** as in the row above | — |

## Transactions and durability

| Feature | SQL standard | Native SQLite | `aydb` | Avoidable? | Pinned by |
| :--- | :--- | :--- | :--- | :--- | :--- |
| Durability setting | — | `PRAGMA synchronous` | **Differs.** `PRAGMA synchronous` has no effect on `aydb` data. Each table's `durability` option chooses `sync` or `buffered`. | **Design choice.** Durability is set per table, so that a cache table and a settings table can share one database. | — |
| Waiting for another writer | — | `busy_timeout` | **Differs.** The engine's `writer_wait_timeout_ms` | **Design choice.** One engine is shared by all connections, so the wait is set once for the engine. | `connect_tests.rs` |
| Order of rows without `ORDER BY` | Undefined | Usually rowid order | **Differs.** Primary-key order. Use `ORDER BY` when the order matters. | **Hard (aydb).** Rows are stored in primary-key order. | — |
