//! SQL reads on a poisoned engine, with the read mode that writes LRU promotions on reads.

use super::*;
use crate::engine::{EngineConfig, LruReadTrackingMode};
use crate::util::test_util::TempTestDir;

#[test]
fn selects_keep_working_on_a_poisoned_engine_with_eager_promotion() {
    let dir = TempTestDir::new("sql_read_poison");
    let fs = Arc::new(OsFs::open(&dir.path, true).unwrap());
    let config = EngineConfig {
        lru_read_tracking: LruReadTrackingMode::EagerWriteOnRead,
        ..EngineConfig::default()
    };
    let engine = Arc::new(RwLock::new(StorageEngine::open(fs, config).unwrap()));
    let conn = SqliteConnection::open_in_memory().unwrap();
    conn.register_aydb_module_with_options(engine.clone(), false)
        .unwrap();
    conn.execute_batch(
        "CREATE VIRTUAL TABLE capped USING aydb(max_bytes=100000, columns='k INTEGER PRIMARY KEY, u TEXT UNIQUE, v TEXT');
         CREATE VIRTUAL TABLE plain USING aydb(columns='k INTEGER PRIMARY KEY, v TEXT');
         INSERT INTO capped VALUES (1, 'a', 'x'), (2, 'b', 'y'), (3, 'c', 'z');
         INSERT INTO plain VALUES (1, 'x'), (2, 'y');
         INSERT INTO capped VALUES (4, 'd', hex(zeroblob(6000)));",
    )
    .unwrap();
    engine
        .write()
        .unwrap()
        .debug_poison("simulated WAL failure");
    assert!(
        conn.execute_batch("INSERT INTO plain VALUES (3, 'z')")
            .is_err()
    );
    for _ in 0..3 {
        for sql in [
            "SELECT count(*) FROM capped",
            "SELECT v FROM capped WHERE k = 2",
            "SELECT v FROM capped WHERE u = 'c'",
            "SELECT v FROM capped ORDER BY k DESC",
            "SELECT rowid, v FROM capped WHERE rowid = 1",
            "SELECT length(v) FROM capped WHERE k = 4",
            "SELECT length(v) FROM capped ORDER BY k",
            "SELECT v FROM plain ORDER BY k",
            "SELECT v FROM plain WHERE k = 2",
        ] {
            let rows = conn
                .query_all(sql, &[])
                .unwrap_or_else(|e| panic!("{sql}: {e}"));
            assert!(!rows.is_empty(), "{sql}");
        }
    }
    assert_eq!(
        conn.query_all("SELECT v FROM capped ORDER BY k", &[])
            .unwrap()
            .len(),
        4
    );
}

#[test]
fn sql_select_promotes_eagerly_without_breaking_read_to_write_upgrade_or_unique_checks() {
    let dir = TempTestDir::new("sql_read_eager_snapshot");
    let fs = Arc::new(OsFs::open(&dir.path, true).unwrap());
    let config = EngineConfig {
        lru_read_tracking: LruReadTrackingMode::EagerWriteOnRead,
        max_transaction_bytes: 2048,
        ..EngineConfig::default()
    };
    let engine = Arc::new(RwLock::new(StorageEngine::open(fs, config).unwrap()));
    let conn = SqliteConnection::open_in_memory().unwrap();
    conn.register_aydb_module_with_options(engine.clone(), false)
        .unwrap();
    let long_u1 = "a".repeat(300);
    let long_u2 = "b".repeat(300);
    conn.execute_batch(&format!(
        "CREATE VIRTUAL TABLE capped USING aydb(max_bytes=100000, columns='k INTEGER PRIMARY KEY, u TEXT UNIQUE, v TEXT');
         INSERT INTO capped VALUES (1, '{long_u1}', 'v1');
         INSERT INTO capped VALUES (2, '{long_u2}', 'v2');"
    ))
    .unwrap();

    // 1. Autocommit SELECT runs EagerWriteOnRead promotions (advancing next_lsn).
    let lsn_before = engine.read().unwrap().next_lsn();
    assert_eq!(
        conn.query_all("SELECT v FROM capped WHERE k = 1", &[])
            .unwrap(),
        vec![vec![SqlValue::Text("v1".into())]]
    );
    assert!(
        engine.read().unwrap().next_lsn() > lsn_before,
        "SELECT under EagerWriteOnRead must record an eager promotion"
    );

    // 2. Same-connection BEGIN; SELECT; UPDATE; COMMIT keeps snapshot write-upgrade intact.
    conn.execute_batch(
        "BEGIN;
         SELECT v FROM capped WHERE k = 1;
         UPDATE capped SET v = 'upgraded' WHERE k = 1;
         COMMIT;",
    )
    .unwrap();
    assert_eq!(
        conn.query_all("SELECT v FROM capped WHERE k = 1", &[])
            .unwrap(),
        vec![vec![SqlValue::Text("upgraded".into())]]
    );

    // 3. Reads and digest UNIQUE conflict probes inside an active transaction must neither log
    //    eager promotions into the transaction nor consume max_transaction_bytes.
    conn.execute_batch(
        "BEGIN;
         INSERT INTO capped VALUES (3, 'small_u', 'ok');",
    )
    .unwrap();
    let lsn_after_insert = engine.read().unwrap().next_lsn();
    for _ in 0..50 {
        assert_eq!(
            conn.query_all("SELECT v FROM capped WHERE k = 1", &[])
                .unwrap(),
            vec![vec![SqlValue::Text("upgraded".into())]]
        );
        assert!(
            conn.execute_batch(&format!(
                "INSERT INTO capped VALUES (4, '{long_u2}', 'dup')"
            ))
            .is_err()
        );
    }
    assert_eq!(engine.read().unwrap().next_lsn(), lsn_after_insert);
    conn.execute_batch("COMMIT").unwrap();
}

fn open_mixed_file_conn(
    db_path: &std::path::Path,
    engine: &Arc<RwLock<StorageEngine<OsFs>>>,
) -> SqliteConnection {
    let conn = SqliteConnection::open_file(db_path).unwrap();
    conn.register_aydb_module_with_options(Arc::clone(engine), false)
        .unwrap();
    conn
}

fn assert_mixed_tx_rolled_back(conn: &SqliteConnection, reader: &SqliteConnection) {
    // Native SQLite table must not be committed on another connection, and SQLite's own
    // failed-COMMIT rollback must leave `conn` in autocommit with both tables unchanged.
    assert!(
        reader
            .query_all("SELECT id FROM native_t", &[])
            .unwrap()
            .is_empty()
    );
    assert!(
        conn.query_all("SELECT id FROM native_t", &[])
            .unwrap()
            .is_empty()
    );
    assert!(
        conn.query_all("SELECT id FROM aydb_t", &[])
            .unwrap()
            .is_empty()
    );
    conn.execute_batch("BEGIN; ROLLBACK;")
        .expect("conn must be back in autocommit mode after failed COMMIT");
}

#[test]
fn commit_reports_wal_append_failure_and_rolls_back_native_table() {
    for durability in ["sync", "buffered"] {
        let dir = TempTestDir::new(&format!("vtab_wal_append_fail_{durability}"));
        let db_path = dir.path.join("mixed.sqlite");
        let fs = Arc::new(OsFs::open(&dir.path, true).unwrap());
        let engine = Arc::new(RwLock::new(
            StorageEngine::open(fs, EngineConfig::default()).unwrap(),
        ));
        let conn = open_mixed_file_conn(&db_path, &engine);
        let reader = SqliteConnection::open_file(&db_path).unwrap();

        conn.execute_batch(&format!(
            "CREATE TABLE native_t(id INTEGER PRIMARY KEY, v TEXT);
             CREATE VIRTUAL TABLE aydb_t USING aydb(table_id=1, durability='{durability}', columns='id INTEGER PRIMARY KEY, v TEXT');"
        ))
        .unwrap();

        // 1. Explicit mixed transaction: COMMIT must fail, poison engine, and roll back native_t.
        conn.execute_batch(
            "BEGIN;
             INSERT INTO native_t VALUES (1, 'from_tx');
             INSERT INTO aydb_t VALUES (1, 'from_tx');",
        )
        .unwrap();
        engine.write().unwrap().debug_fail_next_wal_append();
        let err = conn
            .execute_batch("COMMIT")
            .expect_err("COMMIT must report WAL append failure");
        assert!(!err.to_string().is_empty());
        assert!(engine.read().unwrap().is_poisoned());
        assert_mixed_tx_rolled_back(&conn, &reader);

        // 2. Autocommit on a fresh engine also reports the WAL append failure and leaves 0 rows.
        drop(reader);
        drop(conn);
        drop(engine);
        let dir2 = TempTestDir::new(&format!("vtab_wal_append_fail_auto_{durability}"));
        let fs2 = Arc::new(OsFs::open(&dir2.path, true).unwrap());
        let engine2 = Arc::new(RwLock::new(
            StorageEngine::open(fs2, EngineConfig::default()).unwrap(),
        ));
        let conn2 = SqliteConnection::open_in_memory().unwrap();
        conn2
            .register_aydb_module_with_options(Arc::clone(&engine2), false)
            .unwrap();
        conn2
            .execute_batch(&format!(
                "CREATE VIRTUAL TABLE aydb_t USING aydb(table_id=1, durability='{durability}', columns='id INTEGER PRIMARY KEY, v TEXT');"
            ))
            .unwrap();
        engine2.write().unwrap().debug_fail_next_wal_append();
        assert!(
            conn2
                .execute_batch("INSERT INTO aydb_t VALUES (1, 'auto')")
                .is_err()
        );
        assert!(engine2.read().unwrap().is_poisoned());
        assert!(
            conn2
                .query_all("SELECT id FROM aydb_t", &[])
                .unwrap()
                .is_empty()
        );
    }
}

#[test]
fn commit_reports_wal_sync_failure_for_group_and_non_group_commits() {
    // (a) Plain sync table (AwaitingGroup) + native table
    {
        let dir = TempTestDir::new("vtab_wal_sync_fail_group_single");
        let db_path = dir.path.join("mixed.sqlite");
        let fs = Arc::new(OsFs::open(&dir.path, true).unwrap());
        let engine = Arc::new(RwLock::new(
            StorageEngine::open(fs, EngineConfig::default()).unwrap(),
        ));
        let conn = open_mixed_file_conn(&db_path, &engine);
        let reader = SqliteConnection::open_file(&db_path).unwrap();
        conn.execute_batch(
            "CREATE TABLE native_t(id INTEGER PRIMARY KEY, v TEXT);
             CREATE VIRTUAL TABLE aydb_t USING aydb(table_id=1, durability='sync', columns='id INTEGER PRIMARY KEY, v TEXT');",
        )
        .unwrap();
        conn.execute_batch(
            "BEGIN;
             INSERT INTO native_t VALUES (1, 'n');
             INSERT INTO aydb_t VALUES (1, 'a');",
        )
        .unwrap();
        engine.read().unwrap().debug_fail_next_wal_sync();
        assert!(
            conn.execute_batch("COMMIT").is_err(),
            "COMMIT must report WAL sync failure on AwaitingGroup"
        );
        assert!(engine.read().unwrap().is_poisoned());
        assert_mixed_tx_rolled_back(&conn, &reader);
    }

    // (b) Capped sync table (AwaitingSync) + native table
    {
        let dir = TempTestDir::new("vtab_wal_sync_fail_capped");
        let db_path = dir.path.join("mixed.sqlite");
        let fs = Arc::new(OsFs::open(&dir.path, true).unwrap());
        let engine = Arc::new(RwLock::new(
            StorageEngine::open(fs, EngineConfig::default()).unwrap(),
        ));
        let conn = open_mixed_file_conn(&db_path, &engine);
        let reader = SqliteConnection::open_file(&db_path).unwrap();
        conn.execute_batch(
            "CREATE TABLE native_t(id INTEGER PRIMARY KEY, v TEXT);
             CREATE VIRTUAL TABLE aydb_t USING aydb(table_id=1, durability='sync', max_bytes=65536, columns='id INTEGER PRIMARY KEY, v TEXT');",
        )
        .unwrap();
        conn.execute_batch(
            "BEGIN;
             INSERT INTO native_t VALUES (1, 'n');
             INSERT INTO aydb_t VALUES (1, 'a');",
        )
        .unwrap();
        engine.read().unwrap().debug_fail_next_wal_sync();
        assert!(
            conn.execute_batch("COMMIT").is_err(),
            "COMMIT must report WAL sync failure on AwaitingSync"
        );
        assert!(engine.read().unwrap().is_poisoned());
        assert_mixed_tx_rolled_back(&conn, &reader);
    }

    // (c) Concurrent writer threads sharing a group commit where the leader hits sync failure
    {
        let dir = TempTestDir::new("vtab_wal_sync_fail_two_writers");
        let fs = Arc::new(OsFs::open(&dir.path, true).unwrap());
        let mut cfg = EngineConfig::default();
        cfg.writer_wait_timeout_ms = 5_000;
        let engine = Arc::new(RwLock::new(StorageEngine::open(fs, cfg).unwrap()));

        let ddl = "CREATE VIRTUAL TABLE aydb_t USING aydb(table_id=1, durability='sync', columns='id INTEGER PRIMARY KEY, v TEXT')";
        let conn1 = SqliteConnection::open_in_memory().unwrap();
        conn1
            .register_aydb_module_with_options(Arc::clone(&engine), false)
            .unwrap();
        conn1.execute_batch(ddl).unwrap();

        conn1
            .execute_batch("BEGIN; INSERT INTO aydb_t VALUES (1, 'w1');")
            .unwrap();

        let ready = Arc::new(std::sync::Barrier::new(2));
        let t2 = {
            let engine = Arc::clone(&engine);
            let ready = Arc::clone(&ready);
            std::thread::spawn(move || {
                let conn2 = SqliteConnection::open_in_memory().unwrap();
                conn2
                    .register_aydb_module_with_options(engine, false)
                    .unwrap();
                conn2.execute_batch(ddl).unwrap();
                ready.wait();
                conn2.execute_batch("BEGIN; INSERT INTO aydb_t VALUES (2, 'w2'); COMMIT;")
            })
        };
        ready.wait();
        // Give thread 2 time to enter `xBegin`'s wait before conn1 commits and wakes it.
        std::thread::sleep(std::time::Duration::from_millis(25));
        engine.read().unwrap().debug_fail_next_wal_sync();
        let res1 = conn1.execute_batch("COMMIT");
        let res2 = t2.join().unwrap();

        assert!(res1.is_err(), "conn1 COMMIT must fail on sync failure");
        assert!(
            res2.is_err(),
            "conn2 write/COMMIT must also fail when group sync fails"
        );
        assert!(engine.read().unwrap().is_poisoned());
        conn1
            .execute_batch("BEGIN; ROLLBACK;")
            .expect("conn1 must be back in autocommit mode after failed COMMIT");
        assert!(
            conn1
                .query_all("SELECT id FROM aydb_t", &[])
                .unwrap()
                .is_empty()
        );
    }
}

#[test]
fn commit_reports_pre_existing_poison_and_rolls_back_native_table() {
    let dir = TempTestDir::new("vtab_preexisting_poison_commit");
    let db_path = dir.path.join("mixed.sqlite");
    let fs = Arc::new(OsFs::open(&dir.path, true).unwrap());
    let engine = Arc::new(RwLock::new(
        StorageEngine::open(fs, EngineConfig::default()).unwrap(),
    ));
    let conn = open_mixed_file_conn(&db_path, &engine);
    let reader = SqliteConnection::open_file(&db_path).unwrap();
    conn.execute_batch(
        "CREATE TABLE native_t(id INTEGER PRIMARY KEY, v TEXT);
         CREATE VIRTUAL TABLE aydb_t USING aydb(table_id=1, durability='sync', columns='id INTEGER PRIMARY KEY, v TEXT');",
    )
    .unwrap();
    conn.execute_batch(
        "BEGIN;
         INSERT INTO native_t VALUES (1, 'n');
         INSERT INTO aydb_t VALUES (1, 'a');",
    )
    .unwrap();
    engine
        .write()
        .unwrap()
        .debug_poison("prior I/O failure before commit");
    assert!(
        conn.execute_batch("COMMIT").is_err(),
        "COMMIT on a poisoned engine must fail"
    );
    assert_mixed_tx_rolled_back(&conn, &reader);
}

#[test]
fn multi_table_mixed_durability_tx_commits_once_in_xsync_and_reports_errors() {
    let dir = TempTestDir::new("vtab_multi_table_mixed_durability");
    let fs = Arc::new(OsFs::open(&dir.path, true).unwrap());
    let engine = Arc::new(RwLock::new(
        StorageEngine::open(fs, EngineConfig::default()).unwrap(),
    ));
    let conn = SqliteConnection::open_in_memory().unwrap();
    conn.register_aydb_module_with_options(Arc::clone(&engine), false)
        .unwrap();
    conn.execute_batch(
        "CREATE VIRTUAL TABLE t_buf USING aydb(table_id=1, durability='buffered', columns='id INTEGER PRIMARY KEY, v TEXT');
         CREATE VIRTUAL TABLE t_sync USING aydb(table_id=2, durability='sync', columns='id INTEGER PRIMARY KEY, v TEXT');",
    )
    .unwrap();

    // 1. Write to t_buf, touch t_sync with a no-op UPDATE, COMMIT: succeeds cleanly.
    conn.execute_batch(
        "BEGIN;
         INSERT INTO t_buf VALUES (1, 'buf1');
         UPDATE t_sync SET v = 'noop' WHERE 0 = 1;
         COMMIT;",
    )
    .unwrap();
    assert_eq!(
        conn.query_all("SELECT id, v FROM t_buf", &[]).unwrap(),
        vec![vec![SqlValue::Integer(1), SqlValue::Text("buf1".into())]]
    );

    // 2. Write to both t_buf and t_sync in one transaction, then COMMIT: succeeds cleanly.
    conn.execute_batch(
        "BEGIN;
         INSERT INTO t_buf VALUES (2, 'buf2');
         INSERT INTO t_sync VALUES (10, 'sync10');
         COMMIT;",
    )
    .unwrap();
    assert_eq!(
        conn.query_all("SELECT id FROM t_buf ORDER BY id", &[])
            .unwrap(),
        vec![vec![SqlValue::Integer(1)], vec![SqlValue::Integer(2)]]
    );
    assert_eq!(
        conn.query_all("SELECT id, v FROM t_sync", &[]).unwrap(),
        vec![vec![SqlValue::Integer(10), SqlValue::Text("sync10".into())]]
    );

    // 3. Inject WAL sync failure on a mixed buffered+sync multi-table transaction.
    conn.execute_batch(
        "BEGIN;
         INSERT INTO t_buf VALUES (3, 'buf3');
         INSERT INTO t_sync VALUES (20, 'sync20');",
    )
    .unwrap();
    engine.read().unwrap().debug_fail_next_wal_sync();
    assert!(
        conn.execute_batch("COMMIT").is_err(),
        "COMMIT must report WAL sync failure across mixed-durability tables"
    );
    assert!(engine.read().unwrap().is_poisoned());
    conn.execute_batch("BEGIN; ROLLBACK;")
        .expect("conn must be back in autocommit mode after failed COMMIT");
    assert_eq!(
        conn.query_all("SELECT id FROM t_buf ORDER BY id", &[])
            .unwrap(),
        vec![vec![SqlValue::Integer(1)], vec![SqlValue::Integer(2)]]
    );
    assert_eq!(
        conn.query_all("SELECT id FROM t_sync ORDER BY id", &[])
            .unwrap(),
        vec![vec![SqlValue::Integer(10)]]
    );
}
