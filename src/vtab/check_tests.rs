//! `CHECK` constraints are not supported: `CREATE VIRTUAL TABLE` rejects them, and a table
//! whose stored definition already has one (written by an older build) still connects, so it
//! can be read and dropped, but refuses every write that it cannot check.

use std::path::PathBuf;

use super::*;
use crate::engine::EngineConfig;
use crate::util::test_util::TempTestDir;

#[test]
fn create_rejects_check_constraints_with_a_message() {
    let tmp = TempTestDir::new("check_create");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let engine = Arc::new(RwLock::new(
        StorageEngine::open(fs, EngineConfig::default()).unwrap(),
    ));
    let conn = SqliteConnection::open_in_memory().unwrap();
    conn.register_aydb_module(engine).unwrap();
    for (i, columns) in [
        "k TEXT PRIMARY KEY, x INT CHECK(x > 0)",
        "k TEXT PRIMARY KEY, x INT CHECK (x > 0)",
        "k TEXT PRIMARY KEY, x INT CONSTRAINT c CHECK(x > 0)",
        "k TEXT PRIMARY KEY, x INT NOT NULL CHECK(x > 0) UNIQUE",
        "k TEXT PRIMARY KEY CHECK(k <> ''), x INT",
    ]
    .into_iter()
    .enumerate()
    {
        let err = conn
            .execute_batch(&format!(
                "CREATE VIRTUAL TABLE c{i} USING aydb(table_id={}, columns='{columns}')",
                i + 1
            ))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("CHECK constraints are not supported"),
            "[{columns}] {err}"
        );
    }
    // Table-level constraints stay rejected, and a name that merely contains the word is fine.
    assert!(
        conn.execute_batch(
            "CREATE VIRTUAL TABLE tl USING aydb(table_id=9, columns='k TEXT PRIMARY KEY, x INT, CHECK (x > 0)')"
        )
        .is_err()
    );
    conn.execute_batch(
        "CREATE VIRTUAL TABLE ok USING aydb(table_id=10, columns='k TEXT PRIMARY KEY, \"check\" INT, check_x INT')",
    )
    .unwrap();
    assert_eq!(
        conn.query_all(
            "SELECT count(*) FROM sqlite_master WHERE name <> 'ok' AND name NOT LIKE 'ok_%'",
            &[]
        )
        .unwrap(),
        vec![vec![SqlValue::Integer(0)]]
    );
}

struct Fixture {
    schema_path: PathBuf,
    engine: Arc<RwLock<StorageEngine<OsFs>>>,
    // Declared last so the directories are removed after the engine has closed its files.
    _schema_dir: TempTestDir,
    _engine_dir: TempTestDir,
}

impl Fixture {
    fn open(&self) -> SqliteConnection {
        let conn = SqliteConnection::open_file(&self.schema_path).unwrap();
        conn.register_aydb_module_with_options(self.engine.clone(), false)
            .unwrap();
        conn
    }
}

/// A table `t` with rows (1,10), (2,20) whose stored definition got a `CHECK` afterwards, as
/// if an older build had created it.
fn legacy_check_table(name: &str) -> Fixture {
    let schema_dir = TempTestDir::new(&format!("{name}_schema"));
    let engine_dir = TempTestDir::new(&format!("{name}_engine"));
    let schema_path = schema_dir.path.join("schema.db");
    let fs = Arc::new(OsFs::open(&engine_dir.path, true).unwrap());
    let engine = Arc::new(RwLock::new(
        StorageEngine::open(fs, EngineConfig::default()).unwrap(),
    ));
    let fixture = Fixture {
        schema_path,
        engine,
        _schema_dir: schema_dir,
        _engine_dir: engine_dir,
    };
    let conn = fixture.open();
    conn.execute_batch(
        "CREATE VIRTUAL TABLE t USING aydb(table_id=1, columns='k INTEGER PRIMARY KEY, x INT UNIQUE');
         INSERT INTO t VALUES (1, 10), (2, 20);
         PRAGMA writable_schema = ON;
         UPDATE sqlite_master SET sql = replace(sql, 'x INT UNIQUE', 'x INT CHECK(x > 0) UNIQUE') WHERE name = 't';",
    )
    .unwrap();
    let sql = conn
        .query_all("SELECT sql FROM sqlite_master WHERE name = 't'", &[])
        .unwrap();
    assert!(format!("{sql:?}").contains("CHECK(x > 0)"));
    drop(conn);
    fixture
}

fn rows(conn: &SqliteConnection) -> Vec<Vec<SqlValue>> {
    conn.query_all("SELECT k, x FROM t ORDER BY k", &[])
        .unwrap()
}

fn original_rows() -> Vec<Vec<SqlValue>> {
    [(1, 10), (2, 20)]
        .map(|(k, x)| vec![SqlValue::Integer(k), SqlValue::Integer(x)])
        .to_vec()
}

#[test]
fn a_table_with_a_stored_check_connects_and_reads() {
    let fixture = legacy_check_table("check_read");
    let conn = fixture.open();
    assert_eq!(rows(&conn), original_rows());
    assert_eq!(
        conn.query_all("SELECT x FROM t WHERE k = 2", &[]).unwrap(),
        vec![vec![SqlValue::Integer(20)]]
    );
}

#[test]
fn a_table_with_a_stored_check_refuses_every_write_in_every_mode() {
    let fixture = legacy_check_table("check_write");
    for mode in [
        "",
        "OR ABORT",
        "OR FAIL",
        "OR IGNORE",
        "OR REPLACE",
        "OR ROLLBACK",
    ] {
        for stmt in [
            format!("INSERT {mode} INTO t VALUES (3, 30)"),
            format!("INSERT {mode} INTO t VALUES (1, 99)"),
            format!("INSERT {mode} INTO t(k, x) VALUES (4, -5)"),
            format!("UPDATE {mode} t SET x = x + 1 WHERE k = 1"),
            format!("UPDATE {mode} t SET x = 5"),
            format!("UPDATE {mode} t SET k = 7 WHERE k = 1"),
        ] {
            let conn = fixture.open();
            let err = conn.execute_batch(&stmt).unwrap_err().to_string();
            assert!(err.contains("CHECK"), "{stmt}: {err}");
            assert_eq!(rows(&conn), original_rows(), "{stmt}");
            // The failed statement leaves no transaction behind in the autocommit case.
            assert!(conn.execute_batch("BEGIN").is_ok(), "{stmt}");
            conn.execute_batch("ROLLBACK").unwrap();
        }
    }
    // The table is unchanged for a connection that reads it afresh.
    assert_eq!(rows(&fixture.open()), original_rows());
}

#[test]
fn a_table_with_a_stored_check_still_allows_delete_and_drop() {
    let fixture = legacy_check_table("check_manage");
    let conn = fixture.open();
    conn.execute_batch("DELETE FROM t WHERE k = 1").unwrap();
    assert_eq!(rows(&conn), original_rows()[1..].to_vec());
    drop(conn);

    let conn = fixture.open();
    conn.execute_batch("DROP TABLE t").unwrap();
    assert!(conn.query_all("SELECT k FROM t", &[]).is_err());
    drop(conn);

    // The dropped table's id is free again, so a fresh table can use it.
    let conn = fixture.open();
    conn.execute_batch(
        "CREATE VIRTUAL TABLE t USING aydb(table_id=1, columns='k INTEGER PRIMARY KEY, x INT')",
    )
    .unwrap();
    conn.execute_batch("INSERT INTO t VALUES (5, -1)").unwrap();
}
