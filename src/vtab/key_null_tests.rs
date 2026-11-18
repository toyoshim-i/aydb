//! NULL and omitted keys on an aydb table, compared with a native table.
//!
//! Only `INTEGER PRIMARY KEY` is a rowid alias that turns NULL into an automatic key; any other
//! key type is an ordinary column, so `NOT NULL` on it must be enforced like on other columns.
//! The two tables live in separate connections because `OR ROLLBACK` ends the transaction.

use super::*;
use crate::engine::EngineConfig;
use crate::util::test_util::TempTestDir;

const MODES: [&str; 6] = [
    "",
    "OR ABORT",
    "OR FAIL",
    "OR IGNORE",
    "OR REPLACE",
    "OR ROLLBACK",
];

const SENTINEL: i64 = 7_000_000;

struct Side {
    conn: SqliteConnection,
    table: &'static str,
}

impl Side {
    fn int(&self, sql: &str) -> i64 {
        match self.conn.query_all(sql, &[]).unwrap()[0][0] {
            SqlValue::Integer(v) => v,
            ref other => panic!("unexpected {other:?}"),
        }
    }

    /// Everything observable after a statement.
    fn observe(&self, sql: &str, ok: bool) -> String {
        let in_tx = if self.conn.execute_batch("BEGIN").is_ok() {
            self.conn.execute_batch("ROLLBACK").unwrap();
            false
        } else {
            true
        };
        let rows = self
            .conn
            .query_all(
                &format!(
                    "SELECT k, v, typeof(k) FROM {} ORDER BY v, rowid",
                    self.table
                ),
                &[],
            )
            .unwrap();
        format!(
            "{sql}: ok={ok} changes={} last_rowid={} in_tx={in_tx} rows={rows:?}",
            self.int("SELECT changes()"),
            self.int("SELECT last_insert_rowid()"),
        )
    }
}

/// Runs the statement and the setup in a fresh pair of connections; returns (native, aydb).
fn run(columns: &str, setup: &[&str], stmt: &str) -> (String, String) {
    let dir = TempTestDir::new("key_null");
    let fs = Arc::new(OsFs::open(&dir.path, true).unwrap());
    let mut config = EngineConfig::default();
    config.sync_wal_size_bytes = 64 * 4096;
    config.buf_wal_size_bytes = 64 * 4096;
    let engine = Arc::new(RwLock::new(StorageEngine::open(fs, config).unwrap()));
    let aydb = SqliteConnection::open_in_memory().unwrap();
    aydb.register_aydb_module_with_options(engine, false)
        .unwrap();
    aydb.execute_batch(&format!(
        "CREATE VIRTUAL TABLE a USING aydb(table_id=1, columns='{columns}')"
    ))
    .unwrap();
    let native = SqliteConnection::open_in_memory().unwrap();
    native
        .execute_batch(&format!("CREATE TABLE n({columns})"))
        .unwrap();
    let mut out = Vec::new();
    for side in [
        Side {
            conn: native,
            table: "n",
        },
        Side {
            conn: aydb,
            table: "a",
        },
    ] {
        side.conn
            .execute_batch("CREATE TABLE scratch(id INTEGER PRIMARY KEY)")
            .unwrap();
        side.conn.execute_batch("BEGIN").unwrap();
        for s in setup {
            side.conn
                .execute_batch(&s.replace("{t}", side.table))
                .unwrap();
        }
        side.conn
            .execute_batch(&format!("INSERT INTO scratch VALUES({SENTINEL})"))
            .unwrap();
        let sql = stmt.replace("{t}", side.table);
        let ok = side.conn.execute_batch(&sql).is_ok();
        out.push(side.observe(stmt, ok));
    }
    let aydb = out.pop().unwrap();
    (out.pop().unwrap(), aydb)
}

/// Statements on a table whose key is `k` and whose other column is `v`.
fn key_statements(mode: &str) -> Vec<String> {
    vec![
        format!("INSERT {mode} INTO {{t}} VALUES(NULL, 'x')"),
        format!("INSERT {mode} INTO {{t}}(v) VALUES('x')"),
        format!("UPDATE {mode} {{t}} SET k = NULL WHERE v = 'a'"),
        format!("INSERT {mode} INTO {{t}} VALUES(NULL, 'a')"),
    ]
}

/// Specs whose key column the adapter must treat exactly like native SQLite.
#[test]
fn not_null_keys_match_native() {
    for columns in [
        "k INT PRIMARY KEY NOT NULL, v TEXT",
        "k INTEGER PRIMARY KEY NOT NULL, v TEXT",
        "k TEXT PRIMARY KEY NOT NULL, v TEXT",
        "k BIGINT NOT NULL PRIMARY KEY, v TEXT",
        "k INTEGER PRIMARY KEY, v TEXT",
        "k INTEGER NOT NULL PRIMARY KEY, v TEXT",
        "k INT CONSTRAINT nn NOT NULL PRIMARY KEY, v TEXT",
    ] {
        for mode in MODES {
            for stmt in key_statements(mode) {
                let (native, aydb) = run(columns, &["INSERT INTO {t} VALUES(1, 'a')"], &stmt);
                assert_eq!(aydb, native, "[{columns}] {stmt}");
            }
        }
    }
}

/// A non-key NOT NULL column, for comparison.
#[test]
fn not_null_non_key_columns_match_native() {
    for mode in MODES {
        for stmt in [
            format!("INSERT {mode} INTO {{t}} VALUES(2, NULL)"),
            format!("INSERT {mode} INTO {{t}}(k) VALUES(2)"),
            format!("UPDATE {mode} {{t}} SET v = NULL WHERE k = 1"),
        ] {
            let (native, aydb) = run(
                "k INTEGER PRIMARY KEY, v TEXT NOT NULL",
                &["INSERT INTO {t} VALUES(1, 'a')"],
                &stmt,
            );
            assert_eq!(aydb, native, "{stmt}");
        }
    }
}

/// `INT PRIMARY KEY` is not a rowid alias: an omitted key stays NULL on native SQLite. The
/// adapter cannot store a NULL key, so it rejects the row (and leaves the table unchanged)
/// instead of inventing a key. Same for a text key.
#[test]
fn nullable_non_alias_keys_are_rejected_not_invented() {
    for columns in [
        "k INT PRIMARY KEY, v TEXT",
        "k BIGINT PRIMARY KEY, v TEXT",
        "k TEXT PRIMARY KEY, v TEXT",
    ] {
        for stmt in [
            "INSERT INTO {t} VALUES(NULL, 'x')",
            "INSERT INTO {t}(v) VALUES('x')",
        ] {
            let (_, aydb) = run(columns, &["INSERT INTO {t} VALUES(1, 'a')"], stmt);
            assert!(
                aydb.contains("ok=false") && aydb.contains("rows=[[Integer(1)")
                    || aydb.contains("ok=false") && aydb.contains("rows=[[Text(\"1\")"),
                "[{columns}] {stmt}: {aydb}"
            );
            assert_eq!(aydb.matches("Text(\"x\")").count(), 0, "{aydb}");
        }
    }
}
