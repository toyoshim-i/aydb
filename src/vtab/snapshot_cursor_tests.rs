use super::*;
use crate::engine::EngineConfig;
use crate::util::test_util::TempTestDir;

#[test]
fn implicit_read_snapshot_survives_cursor_refills_and_other_statements() {
    let directory = TempTestDir::new("implicit_cursor_snapshot");
    let filesystem = Arc::new(OsFs::open(&directory.path, true).unwrap());
    let engine = Arc::new(RwLock::new(
        StorageEngine::open(filesystem, EngineConfig::default()).unwrap(),
    ));
    let reader = SqliteConnection::open_in_memory().unwrap();
    let writer = SqliteConnection::open_in_memory().unwrap();
    for connection in [&reader, &writer] {
        connection
            .register_aydb_module_with_options(Arc::clone(&engine), false)
            .unwrap();
        connection.execute_batch(
            "CREATE VIRTUAL TABLE t USING aydb(table_id=1, columns='k INT PRIMARY KEY, value TEXT')",
        ).unwrap();
    }
    writer.execute_batch("BEGIN").unwrap();
    for key in 1..=300 {
        writer
            .execute_batch(&format!("INSERT INTO t VALUES ({key}, 'before')"))
            .unwrap();
    }
    writer.execute_batch("COMMIT").unwrap();

    let statement = reader.prepare("SELECT k,value FROM t ORDER BY k").unwrap();
    assert_eq!(unsafe { sqlite3_step(statement.stmt) }, SQLITE_ROW);
    writer.execute_batch(
        "UPDATE t SET value='after'; DELETE FROM t WHERE k=300; INSERT INTO t VALUES (301,'after')",
    ).unwrap();
    engine.write().unwrap().compact().unwrap();
    assert_eq!(
        reader
            .query_all("SELECT value FROM t WHERE k=300", &[])
            .unwrap(),
        vec![vec![SqlValue::Text("before".into())]]
    );

    for key in 1..=300 {
        if key > 1 {
            assert_eq!(unsafe { sqlite3_step(statement.stmt) }, SQLITE_ROW);
        }
        assert_eq!(unsafe { sqlite3_column_int64(statement.stmt, 0) }, key);
        assert_eq!(
            unsafe { statement.read_column(1) },
            SqlValue::Text("before".into())
        );
    }
    assert_eq!(unsafe { sqlite3_step(statement.stmt) }, SQLITE_DONE);
    drop(statement);
    assert!(
        reader
            .query_all("SELECT k FROM t WHERE k=300", &[])
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        reader
            .query_all("SELECT value FROM t WHERE k=301", &[])
            .unwrap(),
        vec![vec![SqlValue::Text("after".into())]]
    );
}

#[test]
fn ending_a_transaction_releases_its_session_without_invalidating_open_cursors() {
    let directory = TempTestDir::new("commit_with_open_snapshot_cursor");
    let filesystem = Arc::new(OsFs::open(&directory.path, true).unwrap());
    let engine = Arc::new(RwLock::new(
        StorageEngine::open(filesystem, EngineConfig::default()).unwrap(),
    ));
    let reader = SqliteConnection::open_in_memory().unwrap();
    let writer = SqliteConnection::open_in_memory().unwrap();
    for connection in [&reader, &writer] {
        connection
            .register_aydb_module_with_options(Arc::clone(&engine), false)
            .unwrap();
        connection.execute_batch(
            "CREATE VIRTUAL TABLE t USING aydb(table_id=1, columns='k INT PRIMARY KEY, value TEXT')",
        ).unwrap();
    }
    writer
        .execute_batch("INSERT INTO t VALUES (1,'before'),(2,'before')")
        .unwrap();
    reader.execute_batch("BEGIN").unwrap();
    let statement = reader.prepare("SELECT value FROM t ORDER BY k").unwrap();
    assert_eq!(unsafe { sqlite3_step(statement.stmt) }, SQLITE_ROW);
    writer.execute_batch("UPDATE t SET value='after'").unwrap();
    reader.execute_batch("COMMIT; BEGIN").unwrap();
    assert_eq!(
        reader
            .query_all("SELECT value FROM t WHERE k=1", &[])
            .unwrap(),
        vec![vec![SqlValue::Text("after".into())]]
    );
    assert_eq!(unsafe { sqlite3_step(statement.stmt) }, SQLITE_ROW);
    assert_eq!(
        unsafe { statement.read_column(0) },
        SqlValue::Text("before".into())
    );
    drop(statement);
    reader.execute_batch("ROLLBACK").unwrap();
}
