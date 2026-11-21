use super::*;
use crate::engine::EngineConfig;
use crate::util::test_util::TempTestDir;

fn connections(engine: Arc<RwLock<StorageEngine<OsFs>>>) -> (SqliteConnection, SqliteConnection) {
    let a = SqliteConnection::open_in_memory().unwrap();
    let b = SqliteConnection::open_in_memory().unwrap();
    for conn in [&a, &b] {
        conn.register_aydb_module_with_options(engine.clone(), false)
            .unwrap();
        conn.execute_batch(
            "CREATE VIRTUAL TABLE t USING aydb(table_id=1, columns='k TEXT PRIMARY KEY, s TEXT, data BLOB', indexes='s');
             CREATE VIRTUAL TABLE u USING aydb(table_id=2, columns='k TEXT PRIMARY KEY, v TEXT');",
        ).unwrap();
    }
    (a, b)
}

fn query(conn: &SqliteConnection, sql: &str) -> Vec<Vec<SqlValue>> {
    conn.query_all(sql, &[]).unwrap()
}

#[test]
fn read_transaction_keeps_all_access_paths_across_commits_and_compaction() {
    let tmp = TempTestDir::new("vtab_read_snapshot");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut config = EngineConfig::default();
    config.inline_threshold_bytes = 32;
    let engine = Arc::new(RwLock::new(StorageEngine::open(fs, config).unwrap()));
    let (a, b) = connections(engine.clone());
    a.execute_batch("INSERT INTO t VALUES ('a', 'one', zeroblob(16384)), ('b', 'two', X'02'); INSERT INTO u VALUES ('x', 'before')").unwrap();
    engine.write().unwrap().flush_memtable().unwrap();
    a.execute_batch("BEGIN").unwrap();

    let old_point = query(&a, "SELECT k,s,data FROM t WHERE k='a'");
    assert_eq!(old_point[0][2], SqlValue::Blob(vec![0; 16384]));
    let old_pk = query(
        &a,
        "SELECT k,s FROM t WHERE k >= 'a' AND k <= 'z' ORDER BY k",
    );
    let old_pk_reverse = query(
        &a,
        "SELECT k,s FROM t WHERE k >= 'a' AND k <= 'z' ORDER BY k DESC",
    );
    let old_sec = query(
        &a,
        "SELECT k,s FROM t WHERE s >= 'a' AND s <= 'z' ORDER BY s",
    );
    let old_sec_reverse = query(
        &a,
        "SELECT k,s FROM t WHERE s >= 'a' AND s <= 'z' ORDER BY s DESC",
    );
    let old_rowid = match query(&a, "SELECT rowid FROM t WHERE k='b'")[0][0] {
        SqlValue::Integer(value) => value,
        _ => panic!("expected an integer rowid"),
    };

    b.execute_batch("BEGIN; UPDATE t SET s='changed', data=X'CAFE' WHERE k='a'; DELETE FROM t WHERE k='b'; INSERT INTO t VALUES ('c', 'three', X'03'); UPDATE u SET v='after' WHERE k='x'; COMMIT").unwrap();
    {
        let mut storage = engine.write().unwrap();
        storage.flush_memtable().unwrap();
        storage.compact().unwrap();
    }

    assert_eq!(query(&a, "SELECT k,s,data FROM t WHERE k='a'"), old_point);
    assert_eq!(
        query(
            &a,
            "SELECT k,s FROM t WHERE k >= 'a' AND k <= 'z' ORDER BY k"
        ),
        old_pk
    );
    assert_eq!(
        query(
            &a,
            "SELECT k,s FROM t WHERE k >= 'a' AND k <= 'z' ORDER BY k DESC"
        ),
        old_pk_reverse
    );
    assert_eq!(
        query(
            &a,
            "SELECT k,s FROM t WHERE s >= 'a' AND s <= 'z' ORDER BY s"
        ),
        old_sec
    );
    assert_eq!(
        query(
            &a,
            "SELECT k,s FROM t WHERE s >= 'a' AND s <= 'z' ORDER BY s DESC"
        ),
        old_sec_reverse
    );
    assert_eq!(
        query(&a, &format!("SELECT k FROM t WHERE rowid={old_rowid}")),
        vec![vec![SqlValue::Text("b".into())]]
    );
    assert_eq!(
        query(&a, "SELECT v FROM u WHERE k='x'"),
        vec![vec![SqlValue::Text("before".into())]]
    );
    a.execute_batch("COMMIT").unwrap();
    assert_eq!(
        query(&a, "SELECT v FROM u WHERE k='x'"),
        vec![vec![SqlValue::Text("after".into())]]
    );
    assert_eq!(
        query(&a, "SELECT k FROM t ORDER BY k"),
        vec![
            vec![SqlValue::Text("a".into())],
            vec![SqlValue::Text("c".into())]
        ]
    );
    assert!(query(&a, &format!("SELECT k FROM t WHERE rowid={old_rowid}")).is_empty());
}

#[test]
fn stale_read_transaction_cannot_upgrade_to_writer() {
    let tmp = TempTestDir::new("vtab_snapshot_upgrade");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let engine = Arc::new(RwLock::new(
        StorageEngine::open(fs, EngineConfig::default()).unwrap(),
    ));
    let (a, b) = connections(engine);
    a.execute_batch("INSERT INTO t VALUES ('a', 'old', NULL)")
        .unwrap();
    a.execute_batch("BEGIN").unwrap();
    assert_eq!(
        query(&a, "SELECT s FROM t WHERE k='a'"),
        vec![vec![SqlValue::Text("old".into())]]
    );
    b.execute_batch("UPDATE t SET s='new' WHERE k='a'").unwrap();
    let error = a
        .execute_batch("UPDATE t SET s='lost' WHERE k='a'")
        .unwrap_err();
    assert!(
        error.to_string().contains("locked") || error.to_string().contains("busy"),
        "{error}"
    );
    assert_eq!(
        query(&a, "SELECT s FROM t WHERE k='a'"),
        vec![vec![SqlValue::Text("old".into())]]
    );
    a.execute_batch("ROLLBACK").unwrap();
    assert_eq!(
        query(&b, "SELECT s FROM t WHERE k='a'"),
        vec![vec![SqlValue::Text("new".into())]]
    );
    a.execute_batch("BEGIN").unwrap();
    query(&a, "SELECT s FROM t WHERE k='a'");
    a.execute_batch("UPDATE t SET s='fresh' WHERE k='a'")
        .unwrap();
    assert_eq!(
        query(&a, "SELECT s FROM t WHERE k='a'"),
        vec![vec![SqlValue::Text("fresh".into())]]
    );
    a.execute_batch("COMMIT").unwrap();
    assert_eq!(
        query(&b, "SELECT s FROM t WHERE k='a'"),
        vec![vec![SqlValue::Text("fresh".into())]]
    );
}

#[test]
fn outer_savepoint_release_and_rollback_end_the_snapshot() {
    let tmp = TempTestDir::new("vtab_snapshot_savepoints");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let engine = Arc::new(RwLock::new(
        StorageEngine::open(fs, EngineConfig::default()).unwrap(),
    ));
    let (a, b) = connections(engine);
    a.execute_batch("INSERT INTO t VALUES ('a', 'one', NULL)")
        .unwrap();
    a.execute_batch("SAVEPOINT outer").unwrap();
    assert_eq!(
        query(&a, "SELECT s FROM t WHERE k='a'"),
        vec![vec![SqlValue::Text("one".into())]]
    );
    b.execute_batch("UPDATE t SET s='two' WHERE k='a'").unwrap();
    assert_eq!(
        query(&a, "SELECT s FROM t WHERE k='a'"),
        vec![vec![SqlValue::Text("one".into())]]
    );
    a.execute_batch("RELEASE outer").unwrap();
    assert_eq!(
        query(&a, "SELECT s FROM t WHERE k='a'"),
        vec![vec![SqlValue::Text("two".into())]]
    );
    a.execute_batch("BEGIN").unwrap();
    assert_eq!(
        query(&a, "SELECT s FROM t WHERE k='a'"),
        vec![vec![SqlValue::Text("two".into())]]
    );
    b.execute_batch("UPDATE t SET s='three' WHERE k='a'")
        .unwrap();
    a.execute_batch("ROLLBACK").unwrap();
    assert_eq!(
        query(&a, "SELECT s FROM t WHERE k='a'"),
        vec![vec![SqlValue::Text("three".into())]]
    );
}

#[test]
fn stale_read_transaction_cannot_upgrade_via_create_virtual_table() {
    let tmp = TempTestDir::new("vtab_snapshot_upgrade_create");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let engine = Arc::new(RwLock::new(
        StorageEngine::open(fs, EngineConfig::default()).unwrap(),
    ));
    let (a, b) = connections(engine);
    a.execute_batch("INSERT INTO t VALUES ('a', 'old', NULL)")
        .unwrap();
    a.execute_batch("BEGIN").unwrap();
    assert_eq!(
        query(&a, "SELECT s FROM t WHERE k='a'"),
        vec![vec![SqlValue::Text("old".into())]]
    );
    b.execute_batch("UPDATE t SET s='new' WHERE k='a'").unwrap();
    let error = a
        .execute_batch(
            "CREATE VIRTUAL TABLE extra USING aydb(table_id=3, columns='k TEXT PRIMARY KEY, v TEXT')",
        )
        .unwrap_err();
    assert!(
        error.to_string().contains("snapshot")
            || error.to_string().contains("locked")
            || error.to_string().contains("busy"),
        "{error}"
    );
    assert_eq!(
        query(&a, "SELECT s FROM t WHERE k='a'"),
        vec![vec![SqlValue::Text("old".into())]]
    );
    a.execute_batch("ROLLBACK").unwrap();
    assert_eq!(
        query(&b, "SELECT s FROM t WHERE k='a'"),
        vec![vec![SqlValue::Text("new".into())]]
    );

    // A fresh read transaction whose snapshot is still current can upgrade via CREATE VIRTUAL TABLE.
    a.execute_batch("BEGIN").unwrap();
    assert_eq!(
        query(&a, "SELECT s FROM t WHERE k='a'"),
        vec![vec![SqlValue::Text("new".into())]]
    );
    a.execute_batch(
        "CREATE VIRTUAL TABLE extra USING aydb(table_id=3, columns='k TEXT PRIMARY KEY, v TEXT');
         INSERT INTO extra VALUES ('k1', 'v1');
         COMMIT;",
    )
    .unwrap();
    b.execute_batch(
        "CREATE VIRTUAL TABLE extra USING aydb(table_id=3, columns='k TEXT PRIMARY KEY, v TEXT')",
    )
    .unwrap();
    assert_eq!(
        query(&b, "SELECT v FROM extra WHERE k='k1'"),
        vec![vec![SqlValue::Text("v1".into())]]
    );
}

#[test]
fn snapshot_write_upgrade_waits_and_succeeds_after_other_writer_rolls_back() {
    let tmp = TempTestDir::new("vtab_snap_upgrade_wait_rollback");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut cfg = EngineConfig::default();
    cfg.writer_wait_timeout_ms = 5_000;
    let engine = Arc::new(RwLock::new(StorageEngine::open(fs, cfg).unwrap()));

    let a = SqliteConnection::open_in_memory().unwrap();
    a.register_aydb_module_with_options(engine.clone(), false)
        .unwrap();
    a.execute_batch(
        "CREATE VIRTUAL TABLE t USING aydb(table_id=1, columns='k TEXT PRIMARY KEY, s TEXT');
         INSERT INTO t VALUES ('seed', 'v0');",
    )
    .unwrap();

    let (b_connected_tx, b_connected_rx) = std::sync::mpsc::channel();
    let (a_tx_started_tx, a_tx_started_rx) = std::sync::mpsc::channel();
    let (b_upgrading_tx, b_upgrading_rx) = std::sync::mpsc::channel();
    let engine_b = engine.clone();
    let worker = std::thread::spawn(move || {
        let b = SqliteConnection::open_in_memory().unwrap();
        b.register_aydb_module_with_options(engine_b, false)
            .unwrap();
        b.execute_batch(
            "CREATE VIRTUAL TABLE t USING aydb(table_id=1, columns='k TEXT PRIMARY KEY, s TEXT');",
        )
        .unwrap();
        b_connected_tx.send(()).unwrap();
        a_tx_started_rx.recv().unwrap();

        b.execute_batch("BEGIN;").unwrap();
        assert_eq!(
            query(&b, "SELECT s FROM t WHERE k='seed'"),
            vec![vec![SqlValue::Text("v0".into())]]
        );
        b_upgrading_tx.send(()).unwrap();
        // Must wait for A's uncommitted transaction, then succeed once A rolls back.
        b.execute_batch("INSERT INTO t VALUES ('from_b', 'ok'); COMMIT;")
            .unwrap();
    });

    b_connected_rx.recv().unwrap();
    a.execute_batch("BEGIN; INSERT INTO t VALUES ('uncommitted_a', 'temp');")
        .unwrap();
    a_tx_started_tx.send(()).unwrap();

    b_upgrading_rx.recv().unwrap();
    std::thread::sleep(std::time::Duration::from_millis(50));
    a.execute_batch("ROLLBACK;").unwrap();
    worker.join().unwrap();

    assert_eq!(
        query(&a, "SELECT s FROM t WHERE k='from_b'"),
        vec![vec![SqlValue::Text("ok".into())]]
    );
    assert!(query(&a, "SELECT s FROM t WHERE k='uncommitted_a'").is_empty());
}

#[test]
fn snapshot_write_upgrade_waits_then_fails_busy_when_other_writer_commits() {
    let tmp = TempTestDir::new("vtab_snap_upgrade_wait_commit");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut cfg = EngineConfig::default();
    cfg.writer_wait_timeout_ms = 5_000;
    let engine = Arc::new(RwLock::new(StorageEngine::open(fs, cfg).unwrap()));

    let a = SqliteConnection::open_in_memory().unwrap();
    a.register_aydb_module_with_options(engine.clone(), false)
        .unwrap();
    a.execute_batch(
        "CREATE VIRTUAL TABLE t USING aydb(table_id=1, columns='k TEXT PRIMARY KEY, s TEXT');
         INSERT INTO t VALUES ('seed', 'v0');",
    )
    .unwrap();

    let (b_connected_tx, b_connected_rx) = std::sync::mpsc::channel();
    let (a_tx_started_tx, a_tx_started_rx) = std::sync::mpsc::channel();
    let (b_upgrading_tx, b_upgrading_rx) = std::sync::mpsc::channel();
    let engine_b = engine.clone();
    let worker = std::thread::spawn(move || {
        let b = SqliteConnection::open_in_memory().unwrap();
        b.register_aydb_module_with_options(engine_b, false)
            .unwrap();
        b.execute_batch(
            "CREATE VIRTUAL TABLE t USING aydb(table_id=1, columns='k TEXT PRIMARY KEY, s TEXT');",
        )
        .unwrap();
        b_connected_tx.send(()).unwrap();
        a_tx_started_rx.recv().unwrap();

        b.execute_batch("BEGIN;").unwrap();
        assert_eq!(
            query(&b, "SELECT s FROM t WHERE k='seed'"),
            vec![vec![SqlValue::Text("v0".into())]]
        );
        b_upgrading_tx.send(()).unwrap();
        let err = b
            .execute_batch("INSERT INTO t VALUES ('from_b', 'stale');")
            .unwrap_err();
        assert!(
            err.to_string().contains("snapshot")
                || err.to_string().contains("locked")
                || err.to_string().contains("busy"),
            "expected busy/snapshot failure after peer commit, got: {err}"
        );
        b.execute_batch("ROLLBACK;").unwrap();
    });

    b_connected_rx.recv().unwrap();
    a.execute_batch("BEGIN; INSERT INTO t VALUES ('from_a', 'committed');")
        .unwrap();
    a_tx_started_tx.send(()).unwrap();

    b_upgrading_rx.recv().unwrap();
    std::thread::sleep(std::time::Duration::from_millis(50));
    a.execute_batch("COMMIT;").unwrap();
    worker.join().unwrap();

    assert_eq!(
        query(&a, "SELECT s FROM t WHERE k='from_a'"),
        vec![vec![SqlValue::Text("committed".into())]]
    );
    assert!(query(&a, "SELECT s FROM t WHERE k='from_b'").is_empty());
}

#[test]
fn snapshot_write_upgrade_releases_pre_upgrade_snapshot() {
    let tmp = TempTestDir::new("vtab_snap_upgrade_releases_old");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let engine = Arc::new(RwLock::new(
        StorageEngine::open(fs, EngineConfig::default()).unwrap(),
    ));
    let conn = SqliteConnection::open_in_memory().unwrap();
    conn.register_aydb_module_with_options(engine.clone(), false)
        .unwrap();
    conn.execute_batch(
        "CREATE VIRTUAL TABLE t USING aydb(table_id=1, columns='k TEXT PRIMARY KEY, s TEXT');
         INSERT INTO t VALUES ('seed', 'v0');
         BEGIN;",
    )
    .unwrap();
    assert_eq!(
        query(&conn, "SELECT s FROM t WHERE k='seed'"),
        vec![vec![SqlValue::Text("v0".into())]]
    );

    // 1 ref in `SnapshotCache`, 1 ref in `conn`'s `ReadSession`, 1 ref in `pre_snap`.
    let pre_snap = engine.read().unwrap().capture_read_snapshot();
    assert_eq!(pre_snap.strong_count(), 3);

    // Upgrading the read transaction to a write transaction releases `ReadSession`'s reference
    // immediately instead of pinning pre-upgrade state until `COMMIT`/`ROLLBACK`.
    conn.execute_batch("INSERT INTO t VALUES ('k2', 'v2');")
        .unwrap();
    assert_eq!(pre_snap.strong_count(), 2);

    conn.execute_batch("COMMIT;").unwrap();
    assert_eq!(pre_snap.strong_count(), 1);
}
