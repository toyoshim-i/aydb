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
