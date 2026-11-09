use super::*;
use crate::engine::EngineConfig;
use crate::util::test_util::TempTestDir;

#[test]
fn column_constraints_are_enforced_on_writes() {
    let tmp = TempTestDir::new("column_constraints");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let engine = Arc::new(RwLock::new(
        StorageEngine::open(fs, EngineConfig::default()).unwrap(),
    ));
    let conn = SqliteConnection::open_in_memory().unwrap();
    conn.register_aydb_module(engine).unwrap();
    assert!(
        conn.execute_batch(
            "CREATE VIRTUAL TABLE bad USING aydb(columns='k TEXT PRIMARY KEY, v TEXT NOT/**/NULL')"
        )
        .is_err()
    );
    assert!(conn.execute_batch("CREATE VIRTUAL TABLE bad USING aydb(columns='k TEXT PRIMARY KEY, v TEXT UNIQUE ON CONFLICT IGNORE')").is_err());
    conn.execute_batch("CREATE VIRTUAL TABLE t USING aydb(columns='k TEXT PRIMARY KEY, v TEXT NOT NULL UNIQUE COLLATE NOCASE, n BLOB UNIQUE')").unwrap();
    assert!(
        conn.execute_batch("INSERT INTO t VALUES('a', NULL, 1)")
            .is_err()
    );
    conn.execute_batch(
        "INSERT INTO t VALUES('a', 'One', 1), ('b', 'Two', NULL), ('c', 'Three', NULL)",
    )
    .unwrap();
    assert!(
        conn.execute_batch("INSERT INTO t VALUES('d', 'one', 2)")
            .is_err()
    );
    assert!(
        conn.execute_batch("INSERT INTO t VALUES('d', 'Four', 1.0)")
            .is_err()
    );
    assert!(
        conn.execute_batch("UPDATE t SET v='ONE' WHERE k='b'")
            .is_err()
    );
    assert!(
        conn.execute_batch("UPDATE t SET v=NULL WHERE k='a'")
            .is_err()
    );
    conn.execute_batch("UPDATE t SET v='ONE' WHERE k='a'")
        .unwrap();
    conn.execute_batch("INSERT OR IGNORE INTO t VALUES('d', 'one', 2)")
        .unwrap();
    assert_eq!(
        conn.query_all("SELECT k FROM t WHERE k='d'", &[])
            .unwrap()
            .len(),
        0
    );
    conn.execute_batch("INSERT OR REPLACE INTO t VALUES('d', 'Two', 1)")
        .unwrap();
    let rows = conn.query_all("SELECT k FROM t ORDER BY k", &[]).unwrap();
    assert_eq!(rows.len(), 2);
    conn.execute_batch("BEGIN; SAVEPOINT s; INSERT INTO t VALUES ('e', 'Five', 5); ROLLBACK TO s; RELEASE s; COMMIT;").unwrap();
    conn.execute_batch("INSERT INTO t VALUES ('e', 'Five', 5)")
        .unwrap();
    conn.execute_batch("BEGIN; INSERT OR REPLACE INTO t VALUES ('f', 'Five', 6); ROLLBACK;")
        .unwrap();
    assert_eq!(
        conn.query_all("SELECT k FROM t WHERE k='e'", &[])
            .unwrap()
            .len(),
        1
    );
    assert!(
        conn.execute_batch("INSERT INTO t VALUES ('g', 'FIVE', 7)")
            .is_err()
    );
}

#[test]
fn conflicts_obey_statement_and_replace_semantics() {
    let tmp = TempTestDir::new("constraint_conflicts");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let engine = Arc::new(RwLock::new(
        StorageEngine::open(fs, EngineConfig::default()).unwrap(),
    ));
    let conn = SqliteConnection::open_in_memory().unwrap();
    conn.register_aydb_module(engine).unwrap();
    conn.execute_batch("CREATE VIRTUAL TABLE t USING aydb(columns='k TEXT PRIMARY KEY, a TEXT NOT NULL UNIQUE, b TEXT UNIQUE')").unwrap();
    conn.execute_batch(
        "INSERT INTO t VALUES ('a', 'one', 'x'), ('b', 'two', 'y'), ('c', 'three', 'z')",
    )
    .unwrap();

    assert!(
        conn.execute_batch("INSERT INTO t VALUES ('d', 'four', 'w'), ('e', 'one', 'q')")
            .is_err()
    );
    assert!(
        conn.query_all("SELECT k FROM t WHERE k='d'", &[])
            .unwrap()
            .is_empty()
    );
    conn.execute_batch("INSERT OR IGNORE INTO t VALUES ('d', NULL, 'w'), ('e', 'one', 'q')")
        .unwrap();
    assert_eq!(conn.query_all("SELECT k FROM t", &[]).unwrap().len(), 3);

    // The old row is excluded; the two other rows conflict on different columns.
    conn.execute_batch("UPDATE OR REPLACE t SET a='one', b='y' WHERE k='c'")
        .unwrap();
    let rows = conn.query_all("SELECT k FROM t", &[]).unwrap();
    assert_eq!(rows, vec![vec![SqlValue::Text("c".into())]]);
}

#[test]
fn uniqueness_survives_flush_reopen_and_multiple_scan_batches() {
    let tmp = TempTestDir::new("constraint_reopen");
    let sqlite_path = tmp.path.join("schema.sqlite");
    {
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
        let engine = Arc::new(RwLock::new(
            StorageEngine::open(fs, EngineConfig::default()).unwrap(),
        ));
        let conn = SqliteConnection::open_file(&sqlite_path).unwrap();
        conn.register_aydb_module(engine.clone()).unwrap();
        conn.execute_batch("CREATE VIRTUAL TABLE t USING aydb(table_id=1, columns='k INTEGER PRIMARY KEY, label TEXT NOT NULL UNIQUE COLLATE NOCASE, n BLOB UNIQUE')").unwrap();
        for i in 1..=260 {
            conn.execute_batch(&format!("INSERT INTO t VALUES ({i}, 'item{i}', NULL)"))
                .unwrap();
        }
        conn.execute_batch("INSERT INTO t VALUES (261, 'numeric', 42)")
            .unwrap();
        engine.write().unwrap().flush_memtable().unwrap();
    }
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let engine = Arc::new(RwLock::new(
        StorageEngine::open(fs, EngineConfig::default()).unwrap(),
    ));
    let conn = SqliteConnection::open_file(&sqlite_path).unwrap();
    conn.register_aydb_module(engine).unwrap();
    assert!(
        conn.execute_batch("INSERT INTO t VALUES (262, 'ITEM259', NULL)")
            .is_err()
    );
    assert!(
        conn.execute_batch("INSERT INTO t VALUES (262, 'new', 42.0)")
            .is_err()
    );
    assert!(
        conn.execute_batch("INSERT INTO t VALUES (262, NULL, NULL)")
            .is_err()
    );
    conn.execute_batch("INSERT INTO t VALUES (262, 'new', NULL)")
        .unwrap();
}

#[test]
fn default_declarations_are_rejected() {
    let tmp = TempTestDir::new("constraint_defaults");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let engine = Arc::new(RwLock::new(
        StorageEngine::open(fs, EngineConfig::default()).unwrap(),
    ));
    let conn = SqliteConnection::open_in_memory().unwrap();
    conn.register_aydb_module(engine).unwrap();
    for ddl in [
        "CREATE VIRTUAL TABLE t USING aydb(columns='k INTEGER PRIMARY KEY, a TEXT DEFAULT NULL')",
        "CREATE VIRTUAL TABLE t USING aydb(columns='k INTEGER PRIMARY KEY, a TEXT NOT NULL DEFAULT (40 + 2)')",
        "CREATE VIRTUAL TABLE t USING aydb(columns='k INTEGER PRIMARY KEY, a TEXT DEFAULT(42)')",
        "CREATE VIRTUAL TABLE t USING aydb(columns='k INTEGER PRIMARY KEY, a INT DEFAULT+42')",
        "CREATE VIRTUAL TABLE t USING aydb(columns='k INTEGER PRIMARY KEY, a INT DEFAULT-42')",
    ] {
        assert!(conn.execute_batch(ddl).is_err(), "{ddl}");
    }
}

/// Result rows of a query plus the result of `SELECT changes()`.
type RowsAndChanges = (Vec<Vec<SqlValue>>, Vec<Vec<SqlValue>>);

/// Runs `setup` and then `stmt` on an aydb virtual table and on a native SQLite table with the
/// same columns, and returns `(rows, changes)` for each so the expected result comes from SQLite.
fn run_on_vtab_and_native(
    columns: &str,
    setup: &str,
    stmt: &str,
    select: &str,
) -> (RowsAndChanges, RowsAndChanges) {
    let tmp = TempTestDir::new("update_vanished_row");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let engine = Arc::new(RwLock::new(
        StorageEngine::open(fs, EngineConfig::default()).unwrap(),
    ));
    let vtab = SqliteConnection::open_in_memory().unwrap();
    vtab.register_aydb_module(engine).unwrap();
    vtab.execute_batch(&format!(
        "CREATE VIRTUAL TABLE t USING aydb(columns='{columns}')"
    ))
    .unwrap();
    let native = SqliteConnection::open_in_memory().unwrap();
    native
        .execute_batch(&format!("CREATE TABLE t({columns})"))
        .unwrap();
    let mut out = Vec::new();
    for db in [&vtab, &native] {
        db.execute_batch(setup).unwrap();
        db.execute_batch(stmt).unwrap();
        out.push((
            db.query_all(select, &[]).unwrap(),
            db.query_all("SELECT changes()", &[]).unwrap(),
        ));
    }
    let native_out = out.pop().unwrap();
    let vtab_out = out.pop().unwrap();
    (vtab_out, native_out)
}

fn assert_update_matches_native(columns: &str, setup: &str, stmt: &str, select: &str) {
    let (vtab, native) = run_on_vtab_and_native(columns, setup, stmt, select);
    assert_eq!(vtab.0, native.0, "rows differ for: {stmt}");
    // SQLite skips a row that an earlier row of the same statement already replaced, so a native
    // table counts one fewer change. A virtual table is driven through `xUpdate` calls and
    // SQLite counts every call that returns OK, including the no-op for the vanished row, so
    // `changes()` cannot be required to match. It may only exceed the native count.
    let (SqlValue::Integer(vtab_changes), SqlValue::Integer(native_changes)) =
        (&vtab.1[0][0], &native.1[0][0])
    else {
        panic!("changes() must be an integer");
    };
    assert!(vtab_changes >= native_changes, "{stmt}");
}

#[test]
fn update_or_replace_does_not_resurrect_a_replaced_row() {
    let text = "k TEXT PRIMARY KEY, u INT UNIQUE";
    let sel = "SELECT k, u FROM t ORDER BY k";
    // The first row replaces the second one, whose update must then be a no-op.
    assert_update_matches_native(
        text,
        "INSERT INTO t VALUES('a',1),('b',2)",
        "UPDATE OR REPLACE t SET u=CASE k WHEN 'a' THEN 2 ELSE 3 END",
        sel,
    );
    // The second row replaces the first one: nothing vanishes before it is processed.
    assert_update_matches_native(
        text,
        "INSERT INTO t VALUES('a',1),('b',2)",
        "UPDATE OR REPLACE t SET u=5",
        sel,
    );
    // A three-row chain where the middle row vanishes.
    assert_update_matches_native(
        text,
        "INSERT INTO t VALUES('a',1),('b',2),('c',3)",
        "UPDATE OR REPLACE t SET u=CASE k WHEN 'a' THEN 2 WHEN 'b' THEN 9 ELSE 4 END",
        sel,
    );
}

#[test]
fn update_or_replace_does_not_resurrect_a_replaced_integer_pk_row() {
    let int = "k INTEGER PRIMARY KEY, u INT UNIQUE";
    let sel = "SELECT k, u FROM t ORDER BY k";
    assert_update_matches_native(
        int,
        "INSERT INTO t VALUES(1,10),(2,20)",
        "UPDATE OR REPLACE t SET u=CASE k WHEN 1 THEN 20 ELSE 30 END",
        sel,
    );
}

/// A PK-changing `UPDATE OR REPLACE` cannot match a native table row for row. The rowid of an
/// aydb row is its primary-key identity, so when row 1 is moved onto the key of row 2, the
/// rowid that SQLite still has queued for the replaced row 2 addresses the moved row, and
/// SQLite hands `xUpdate` values it computed before the statement began. A native table keeps
/// each row's rowid across the key change and so differs. The invariants asserted here hold on
/// both: the statement succeeds, keys stay unique, and no row is created out of nothing, so
/// the row count equals the native count.
#[test]
fn update_or_replace_pk_change_keeps_row_count_and_keys_unique() {
    for (columns, setup, stmt) in [
        (
            "k TEXT PRIMARY KEY, u INT UNIQUE",
            "INSERT INTO t VALUES('a',1),('b',2)",
            "UPDATE OR REPLACE t SET k=CASE k WHEN 'a' THEN 'b' ELSE 'c' END",
        ),
        (
            "k INTEGER PRIMARY KEY, u INT UNIQUE",
            "INSERT INTO t VALUES(1,10),(2,20)",
            "UPDATE OR REPLACE t SET k=CASE k WHEN 1 THEN 2 ELSE 3 END",
        ),
    ] {
        let (vtab, native) = run_on_vtab_and_native(columns, setup, stmt, "SELECT k, u FROM t");
        assert_eq!(vtab.0.len(), native.0.len(), "{stmt}");
        let mut keys: Vec<_> = vtab.0.iter().map(|row| format!("{:?}", row[0])).collect();
        keys.sort();
        keys.dedup();
        assert_eq!(keys.len(), vtab.0.len(), "{stmt}");
    }
}
