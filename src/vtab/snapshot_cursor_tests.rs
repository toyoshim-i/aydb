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

fn open_counter_connection(engine: &Arc<RwLock<StorageEngine<OsFs>>>) -> SqliteConnection {
    const DDL: &str = "CREATE VIRTUAL TABLE c USING aydb(table_id=1, durability='sync', columns='k INT PRIMARY KEY, n INT')";
    loop {
        let connection = SqliteConnection::open_in_memory().unwrap();
        connection
            .register_aydb_module_with_options(Arc::clone(engine), false)
            .unwrap();
        if connection.execute_batch(DDL).is_ok() {
            return connection;
        }
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
}

#[test]
fn concurrent_read_then_increment_never_loses_a_committed_update() {
    use std::sync::atomic::{AtomicU64, Ordering};
    let directory = TempTestDir::new("snapshot_no_lost_update");
    let mut config = EngineConfig::default();
    // A tiny WAL forces rotation (and memtable flush) inside commits.
    config.sync_wal_size_bytes = 32 * 1024;
    let engine = Arc::new(RwLock::new(
        StorageEngine::open(Arc::new(OsFs::open(&directory.path, true).unwrap()), config).unwrap(),
    ));
    let setup = open_counter_connection(&engine);
    setup.execute_batch("INSERT INTO c VALUES (1, 0)").unwrap();
    let committed = Arc::new(AtomicU64::new(0));
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(4);
    let workers: Vec<_> = (0..4)
        .map(|_| {
            let (engine, committed) = (Arc::clone(&engine), Arc::clone(&committed));
            std::thread::spawn(move || {
                let connection = open_counter_connection(&engine);
                while std::time::Instant::now() < deadline {
                    connection.execute_batch("BEGIN").unwrap();
                    let n = match connection
                        .query_all("SELECT n FROM c WHERE k=1", &[])
                        .unwrap()[0][0]
                    {
                        SqlValue::Integer(value) => value,
                        ref other => panic!("unexpected counter value {other:?}"),
                    };
                    let outcome = connection
                        .execute_batch(&format!("UPDATE c SET n={} WHERE k=1", n + 1))
                        .and_then(|()| connection.execute_batch("COMMIT"));
                    match outcome {
                        Ok(()) => {
                            committed.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(_) => {
                            let _ = connection.execute_batch("ROLLBACK");
                        }
                    }
                }
            })
        })
        .collect();
    for worker in workers {
        worker.join().unwrap();
    }
    let committed = committed.load(Ordering::Relaxed);
    assert!(committed > 0);
    assert_eq!(
        setup.query_all("SELECT n FROM c WHERE k=1", &[]).unwrap(),
        vec![vec![SqlValue::Integer(committed as i64)]],
        "a committed increment was overwritten by a stale writer"
    );
}

#[test]
fn flush_between_read_and_write_does_not_conflict_the_upgrade() {
    let directory = TempTestDir::new("snapshot_flush_no_conflict");
    let filesystem = Arc::new(OsFs::open(&directory.path, true).unwrap());
    let engine = Arc::new(RwLock::new(
        StorageEngine::open(filesystem, EngineConfig::default()).unwrap(),
    ));
    let connection = SqliteConnection::open_in_memory().unwrap();
    connection
        .register_aydb_module_with_options(Arc::clone(&engine), false)
        .unwrap();
    connection
        .execute_batch(
            "CREATE VIRTUAL TABLE t USING aydb(table_id=1, columns='k INT PRIMARY KEY, value TEXT')",
        )
        .unwrap();
    connection
        .execute_batch("INSERT INTO t VALUES (1,'a')")
        .unwrap();
    connection.execute_batch("BEGIN").unwrap();
    connection
        .query_all("SELECT value FROM t WHERE k=1", &[])
        .unwrap();
    engine.write().unwrap().flush_memtable().unwrap();
    connection
        .execute_batch("UPDATE t SET value='b' WHERE k=1")
        .unwrap();
    connection.execute_batch("COMMIT").unwrap();
}
