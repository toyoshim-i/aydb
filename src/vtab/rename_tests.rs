//! `ALTER TABLE ... RENAME TO` on an aydb table: the catalog name follows the table, the data
//! stays reachable under the new name only, and the rename is part of the surrounding
//! transaction.

use std::path::PathBuf;

use super::*;
use crate::engine::EngineConfig;
use crate::util::test_util::TempTestDir;

pub(super) struct Fixture {
    schema_path: PathBuf,
    engine_dir: TempTestDir,
    engine: Option<Arc<RwLock<StorageEngine<OsFs>>>>,
    // Declared last so the directories are removed after the engine has closed its files.
    _schema_dir: TempTestDir,
}

pub(super) fn open_engine(dir: &TempTestDir) -> Arc<RwLock<StorageEngine<OsFs>>> {
    let fs = Arc::new(OsFs::open(&dir.path, true).unwrap());
    let mut config = EngineConfig::default();
    config.sync_wal_size_bytes = 64 * 4096;
    config.buf_wal_size_bytes = 64 * 4096;
    Arc::new(RwLock::new(StorageEngine::open(fs, config).unwrap()))
}

impl Fixture {
    pub(super) fn new(name: &str) -> Self {
        let schema_dir = TempTestDir::new(&format!("{name}_schema"));
        let engine_dir = TempTestDir::new(&format!("{name}_engine"));
        Self {
            schema_path: schema_dir.path.join("schema.db"),
            engine: Some(open_engine(&engine_dir)),
            engine_dir,
            _schema_dir: schema_dir,
        }
    }

    pub(super) fn engine(&self) -> Arc<RwLock<StorageEngine<OsFs>>> {
        self.engine.clone().unwrap()
    }

    pub(super) fn open(&self) -> SqliteConnection {
        let conn = SqliteConnection::open_file(&self.schema_path).unwrap();
        conn.register_aydb_module_with_options(self.engine.clone().unwrap(), false)
            .unwrap();
        conn
    }

    /// Closes and reopens the engine, as a process restart would.
    pub(super) fn restart(&mut self) {
        // No other reference to the engine may be left: the connections are dropped by now.
        drop(self.engine.take());
        self.engine = Some(open_engine(&self.engine_dir));
    }
}

pub(super) fn ints(conn: &SqliteConnection, sql: &str) -> Vec<i64> {
    conn.query_all(sql, &[])
        .unwrap()
        .iter()
        .map(|r| match r[0] {
            SqlValue::Integer(v) => v,
            ref other => panic!("{other:?}"),
        })
        .collect()
}

pub(super) const TABLE: &str =
    "CREATE VIRTUAL TABLE t USING aydb(columns='k INTEGER PRIMARY KEY, u TEXT UNIQUE, v INT')";

#[test]
fn a_renamed_table_keeps_its_rows_and_loses_its_old_name() {
    let fixture = Fixture::new("rename_basic");
    let conn = fixture.open();
    conn.execute_batch(TABLE).unwrap();
    conn.execute_batch("INSERT INTO t VALUES (1, 'a', 10), (2, 'b', 20)")
        .unwrap();
    conn.execute_batch("ALTER TABLE t RENAME TO t2").unwrap();
    assert_eq!(ints(&conn, "SELECT v FROM t2 ORDER BY k"), [10, 20]);
    assert!(conn.query_all("SELECT v FROM t", &[]).is_err());
    conn.execute_batch("INSERT INTO t2 VALUES (3, 'c', 30)")
        .unwrap();
    // UNIQUE still holds, updates and deletes work.
    assert!(
        conn.execute_batch("INSERT INTO t2 VALUES (4, 'a', 40)")
            .is_err()
    );
    conn.execute_batch("UPDATE t2 SET v = v + 1 WHERE k = 1; DELETE FROM t2 WHERE k = 2")
        .unwrap();
    assert_eq!(ints(&conn, "SELECT v FROM t2 ORDER BY k"), [11, 30]);
    // Rename back; the name t is usable again.
    conn.execute_batch("ALTER TABLE t2 RENAME TO t").unwrap();
    assert_eq!(ints(&conn, "SELECT v FROM t ORDER BY k"), [11, 30]);
    assert!(conn.query_all("SELECT v FROM t2", &[]).is_err());
}

#[test]
fn a_renamed_table_is_found_after_reconnect_and_engine_restart() {
    let mut fixture = Fixture::new("rename_restart");
    {
        let conn = fixture.open();
        conn.execute_batch(TABLE).unwrap();
        conn.execute_batch("INSERT INTO t VALUES (1, 'a', 10)")
            .unwrap();
        conn.execute_batch("ALTER TABLE t RENAME TO moved").unwrap();
    }
    {
        let conn = fixture.open();
        assert_eq!(ints(&conn, "SELECT v FROM moved"), [10]);
    }
    fixture.restart();
    let conn = fixture.open();
    assert_eq!(ints(&conn, "SELECT v FROM moved"), [10]);
    conn.execute_batch("INSERT INTO moved VALUES (2, 'b', 20)")
        .unwrap();
    // The old name is free for a new, separate table.
    conn.execute_batch(TABLE).unwrap();
    assert!(ints(&conn, "SELECT v FROM t").is_empty());
    assert_eq!(ints(&conn, "SELECT v FROM moved ORDER BY k"), [10, 20]);
}

#[test]
fn a_renamed_table_can_be_dropped() {
    let fixture = Fixture::new("rename_drop");
    let conn = fixture.open();
    conn.execute_batch(TABLE).unwrap();
    conn.execute_batch("INSERT INTO t VALUES (1, 'a', 10)")
        .unwrap();
    conn.execute_batch("ALTER TABLE t RENAME TO t2").unwrap();
    conn.execute_batch("DROP TABLE t2").unwrap();
    assert!(conn.query_all("SELECT v FROM t2", &[]).is_err());
    // Nothing of the table is left under either name.
    conn.execute_batch(TABLE).unwrap();
    assert!(ints(&conn, "SELECT v FROM t").is_empty());
    conn.execute_batch("INSERT INTO t VALUES (1, 'a', 1)")
        .unwrap();
}

#[test]
fn a_table_with_an_explicit_table_id_is_renamed_too() {
    let fixture = Fixture::new("rename_explicit_id");
    let conn = fixture.open();
    conn.execute_batch(
        "CREATE VIRTUAL TABLE t USING aydb(table_id=7, columns='k INTEGER PRIMARY KEY, v INT')",
    )
    .unwrap();
    conn.execute_batch("INSERT INTO t VALUES (1, 10)").unwrap();
    conn.execute_batch("ALTER TABLE t RENAME TO t2").unwrap();
    drop(conn);
    let conn = fixture.open();
    assert_eq!(ints(&conn, "SELECT v FROM t2"), [10]);
    // The id is still taken by the renamed table.
    assert!(
        conn.execute_batch(
            "CREATE VIRTUAL TABLE other USING aydb(table_id=7, columns='k INTEGER PRIMARY KEY, v INT')"
        )
        .is_err()
    );
}

#[test]
fn rename_to_a_name_another_aydb_table_uses_is_refused() {
    // Two schema files over one engine: SQLite sees no clash, the engine catalog does.
    let fixture = Fixture::new("rename_clash");
    let other_dir = TempTestDir::new("rename_clash_other");
    let first = fixture.open();
    first
        .execute_batch("CREATE VIRTUAL TABLE x USING aydb(columns='k INTEGER PRIMARY KEY, v INT')")
        .unwrap();
    first.execute_batch("INSERT INTO x VALUES (1, 10)").unwrap();
    let second = SqliteConnection::open_file(&other_dir.path.join("other.db")).unwrap();
    second
        .register_aydb_module_with_options(fixture.engine.clone().unwrap(), false)
        .unwrap();
    second
        .execute_batch("CREATE VIRTUAL TABLE y USING aydb(columns='k INTEGER PRIMARY KEY, v INT')")
        .unwrap();
    second
        .execute_batch("INSERT INTO y VALUES (1, 99)")
        .unwrap();
    assert!(second.execute_batch("ALTER TABLE y RENAME TO x").is_err());
    // Both tables are intact and still under their own names.
    assert_eq!(ints(&second, "SELECT v FROM y"), [99]);
    assert_eq!(ints(&first, "SELECT v FROM x"), [10]);
    drop(second);
    let second = SqliteConnection::open_file(&other_dir.path.join("other.db")).unwrap();
    second
        .register_aydb_module_with_options(fixture.engine.clone().unwrap(), false)
        .unwrap();
    assert_eq!(ints(&second, "SELECT v FROM y"), [99]);
}

#[test]
fn a_rolled_back_rename_leaves_the_old_name_working() {
    let mut fixture = Fixture::new("rename_rollback");
    let conn = fixture.open();
    conn.execute_batch(TABLE).unwrap();
    conn.execute_batch("INSERT INTO t VALUES (1, 'a', 10)")
        .unwrap();
    conn.execute_batch("BEGIN; INSERT INTO t VALUES (5, 'e', 50); ALTER TABLE t RENAME TO t2")
        .unwrap();
    assert_eq!(ints(&conn, "SELECT v FROM t2 ORDER BY k"), [10, 50]);
    conn.execute_batch("INSERT INTO t2 VALUES (2, 'b', 20)")
        .unwrap();
    conn.execute_batch("ROLLBACK").unwrap();
    assert_eq!(ints(&conn, "SELECT v FROM t"), [10]);
    assert!(conn.query_all("SELECT v FROM t2", &[]).is_err());
    conn.execute_batch("INSERT INTO t VALUES (3, 'c', 30)")
        .unwrap();
    drop(conn);
    fixture.restart();
    let conn = fixture.open();
    assert_eq!(ints(&conn, "SELECT v FROM t ORDER BY k"), [10, 30]);
}

#[test]
fn a_rename_that_cannot_join_the_transaction_is_refused() {
    // SQLite does not tell the table that a transaction began, so a rename as the first
    // statement of one could not be rolled back. It is refused, leaving the table untouched.
    let fixture = Fixture::new("rename_first_in_tx");
    let conn = fixture.open();
    conn.execute_batch(TABLE).unwrap();
    conn.execute_batch("INSERT INTO t VALUES (1, 'a', 10)")
        .unwrap();
    conn.execute_batch("BEGIN").unwrap();
    let err = conn
        .execute_batch("ALTER TABLE t RENAME TO t2")
        .unwrap_err()
        .to_string();
    assert!(err.contains("needs an earlier write"), "{err}");
    assert_eq!(ints(&conn, "SELECT v FROM t"), [10]);
    conn.execute_batch("COMMIT").unwrap();
    // Outside a transaction, and after a write inside one, the rename works.
    conn.execute_batch("ALTER TABLE t RENAME TO t2").unwrap();
    assert_eq!(ints(&conn, "SELECT v FROM t2"), [10]);
}

#[test]
fn a_committed_rename_inside_a_transaction_survives() {
    let mut fixture = Fixture::new("rename_commit");
    let conn = fixture.open();
    conn.execute_batch(TABLE).unwrap();
    conn.execute_batch("BEGIN; INSERT INTO t VALUES (1, 'a', 10); ALTER TABLE t RENAME TO t2; INSERT INTO t2 VALUES (2, 'b', 20); COMMIT")
        .unwrap();
    drop(conn);
    fixture.restart();
    let conn = fixture.open();
    assert_eq!(ints(&conn, "SELECT v FROM t2 ORDER BY k"), [10, 20]);
}

#[test]
fn a_renamed_capped_table_still_evicts() {
    let fixture = Fixture::new("rename_capped");
    let conn = fixture.open();
    conn.execute_batch(
        "CREATE VIRTUAL TABLE t USING aydb(max_bytes=4096, columns='k INTEGER PRIMARY KEY, v TEXT')",
    )
    .unwrap();
    conn.execute_batch("ALTER TABLE t RENAME TO capped")
        .unwrap();
    drop(conn);
    let conn = fixture.open();
    for k in 0..200 {
        conn.execute_batch(&format!(
            "INSERT INTO capped VALUES ({k}, '{}')",
            "x".repeat(200)
        ))
        .unwrap();
    }
    let count = ints(&conn, "SELECT count(*) FROM capped")[0];
    assert!(count > 0 && count < 201, "{count}");
    // 201 rows of 200 bytes do not fit the 4096-byte quota that the table carried over.
}

#[test]
fn a_second_connection_follows_a_rename() {
    let fixture = Fixture::new("rename_two_conns");
    let first = fixture.open();
    first.execute_batch(TABLE).unwrap();
    first
        .execute_batch("INSERT INTO t VALUES (1, 'a', 10)")
        .unwrap();
    let second = fixture.open();
    assert_eq!(ints(&second, "SELECT v FROM t"), [10]);
    first.execute_batch("ALTER TABLE t RENAME TO t2").unwrap();
    // The second connection has the old definition cached; its next statement sees the new schema.
    assert_eq!(ints(&second, "SELECT v FROM t2"), [10]);
    assert!(second.query_all("SELECT v FROM t", &[]).is_err());
    second
        .execute_batch("INSERT INTO t2 VALUES (2, 'b', 20)")
        .unwrap();
    assert_eq!(ints(&first, "SELECT v FROM t2 ORDER BY k"), [10, 20]);
}

#[test]
fn a_table_with_a_stored_check_can_be_renamed() {
    let fixture = Fixture::new("rename_check");
    {
        let conn = fixture.open();
        conn.execute_batch(
            "CREATE VIRTUAL TABLE t USING aydb(table_id=1, columns='k INTEGER PRIMARY KEY, x INT');
             INSERT INTO t VALUES (1, 10);
             PRAGMA writable_schema = ON;
             UPDATE sqlite_master SET sql = replace(sql, 'x INT', 'x INT CHECK(x > 0)') WHERE name = 't';",
        )
        .unwrap();
    }
    let conn = fixture.open();
    conn.execute_batch("ALTER TABLE t RENAME TO t2").unwrap();
    drop(conn);
    let conn = fixture.open();
    assert_eq!(ints(&conn, "SELECT x FROM t2"), [10]);
    assert!(conn.execute_batch("INSERT INTO t2 VALUES (2, 5)").is_err());
    conn.execute_batch("DROP TABLE t2").unwrap();
}
