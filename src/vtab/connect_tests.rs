//! Connecting and creating tables while another connection has a transaction open.
//!
//! `xConnect` of an existing table changes no durable state, so it must succeed (and must not
//! wait) while another connection writes. `xCreate` of a new table does write durable state; it
//! waits, bounded by `writer_wait_timeout_ms`, for the other transaction to end.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use crate::engine::{EngineConfig, StorageEngine};
use crate::pal::OsFs;
use crate::util::test_util::TempTestDir;
use crate::vtab::connection::SqliteConnection;
use crate::vtab::row_codec::SqlValue;

const T_DDL: &str =
    "CREATE VIRTUAL TABLE t USING aydb(table_id=1, columns='id INTEGER PRIMARY KEY, v TEXT')";
const C_DDL: &str = "CREATE VIRTUAL TABLE c USING aydb(table_id=3, max_bytes=1048576, \
                     columns='id INTEGER PRIMARY KEY, v TEXT')";
const NEW_DDL: &str =
    "CREATE VIRTUAL TABLE n USING aydb(table_id=4, columns='id INTEGER PRIMARY KEY, v TEXT')";

/// A schema file that already declares `t` (id 1, three committed rows) and the capped table `c`
/// (id 3), plus the engine that holds their data. Connections opened on the schema file connect
/// both tables lazily through `xConnect`.
struct Fixture {
    schema_path: PathBuf,
    engine: Arc<RwLock<StorageEngine<OsFs>>>,
    // Declared last so the directories are removed after the engine has closed its files.
    _schema_dir: TempTestDir,
    _engine_dir: TempTestDir,
}

fn fixture(name: &str, writer_wait_timeout_ms: u32) -> Fixture {
    let schema_dir = TempTestDir::new(&format!("{name}_schema"));
    let engine_dir = TempTestDir::new(&format!("{name}_engine"));
    let schema_path = schema_dir.path.join("schema.db");
    let fs = Arc::new(OsFs::open(&engine_dir.path, true).unwrap());
    let config = EngineConfig {
        writer_wait_timeout_ms,
        ..EngineConfig::default()
    };
    let engine = Arc::new(RwLock::new(StorageEngine::open(fs, config).unwrap()));
    {
        let setup = SqliteConnection::open_file(&schema_path).unwrap();
        setup
            .register_aydb_module_with_options(engine.clone(), false)
            .unwrap();
        setup.execute_batch(T_DDL).unwrap();
        setup.execute_batch(C_DDL).unwrap();
        setup
            .execute_batch("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')")
            .unwrap();
    }
    Fixture {
        _schema_dir: schema_dir,
        _engine_dir: engine_dir,
        schema_path,
        engine,
    }
}

impl Fixture {
    /// A connection on the schema file: its first use of `t` or `c` runs `xConnect`.
    fn open(&self) -> SqliteConnection {
        let conn = SqliteConnection::open_file(&self.schema_path).unwrap();
        conn.register_aydb_module_with_options(self.engine.clone(), false)
            .unwrap();
        conn
    }

    /// An in-memory connection that creates (or, if it exists, connects) `t`.
    fn writer(&self) -> SqliteConnection {
        memory_connection(&self.engine, T_DDL)
    }

    fn is_registered(&self, table_id: u16, name: &str) -> bool {
        let eng = self.engine.read().unwrap();
        eng.active_vtab_ids.get(&table_id).map(|n| &**n) == Some(name)
    }
}

fn memory_connection(engine: &Arc<RwLock<StorageEngine<OsFs>>>, ddl: &str) -> SqliteConnection {
    let conn = SqliteConnection::open_in_memory().unwrap();
    conn.register_aydb_module_with_options(engine.clone(), false)
        .unwrap();
    conn.execute_batch(ddl).unwrap();
    conn
}

fn ids(conn: &SqliteConnection, table: &str) -> Vec<i64> {
    conn.query_all(&format!("SELECT id FROM {table} ORDER BY id"), &[])
        .unwrap()
        .into_iter()
        .map(|row| match row[0] {
            SqlValue::Integer(id) => id,
            ref other => panic!("integer id expected, got {other:?}"),
        })
        .collect()
}

#[test]
fn connect_to_an_existing_table_succeeds_while_another_connection_writes() {
    let fx = fixture("connect_during_write", 30_000);
    let writer = fx.writer();
    writer
        .execute_batch("BEGIN; UPDATE t SET v='dirty' WHERE id=1; INSERT INTO t VALUES (100,'x')")
        .unwrap();

    let started = Instant::now();
    let reader = fx.open();
    assert_eq!(ids(&reader, "t"), vec![1, 2, 3], "only committed rows");
    assert_eq!(
        reader.query_all("SELECT v FROM t WHERE id=1", &[]).unwrap(),
        vec![vec![SqlValue::Text("a".into())]]
    );
    assert!(ids(&reader, "c").is_empty());
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "a connect that needs no write must not wait for the writer"
    );

    // The writer's rollback must not remove what the connect registered.
    writer.execute_batch("ROLLBACK").unwrap();
    assert!(fx.is_registered(1, "t"));
    assert!(fx.is_registered(3, "c"));
    assert!(
        fx.engine.read().unwrap().capped_table_state(3).is_some(),
        "the capped table registration survives the rollback"
    );
    assert_eq!(ids(&reader, "t"), vec![1, 2, 3]);
}

#[test]
fn connect_sizes_the_auto_rowid_from_committed_rows_only() {
    let fx = fixture("connect_auto_rowid", 30_000);
    let writer = fx.writer();
    writer
        .execute_batch("BEGIN; INSERT INTO t VALUES (100,'uncommitted')")
        .unwrap();
    let reader = fx.open();
    assert_eq!(ids(&reader, "t"), vec![1, 2, 3]);
    writer.execute_batch("ROLLBACK").unwrap();
    reader
        .execute_batch("INSERT INTO t(v) VALUES ('auto')")
        .unwrap();
    assert_eq!(
        ids(&reader, "t"),
        vec![1, 2, 3, 4],
        "an uncommitted row that rolled back must not leave a gap in the rowid sequence"
    );
}

#[test]
fn create_waits_for_the_other_transaction_and_then_succeeds() {
    let fx = fixture("create_waits", 20_000);
    let writer = fx.writer();
    writer
        .execute_batch("BEGIN; INSERT INTO t VALUES (100,'pending')")
        .unwrap();

    let (done_tx, done_rx) = mpsc::channel();
    let engine = fx.engine.clone();
    let creator = std::thread::spawn(move || {
        let conn = SqliteConnection::open_in_memory().unwrap();
        conn.register_aydb_module_with_options(engine, false)
            .unwrap();
        let created = conn.execute_batch(NEW_DDL).map_err(|e| e.to_string());
        let inserted = created.clone().and_then(|()| {
            conn.execute_batch("INSERT INTO n VALUES (1,'one')")
                .map_err(|e| e.to_string())
        });
        done_tx.send(inserted).unwrap();
    });

    assert!(
        done_rx.recv_timeout(Duration::from_millis(400)).is_err(),
        "xCreate must wait while the other transaction is open"
    );
    writer.execute_batch("COMMIT").unwrap();
    let outcome = done_rx.recv_timeout(Duration::from_secs(30)).unwrap();
    assert_eq!(outcome, Ok(()));
    creator.join().unwrap();

    let check = memory_connection(&fx.engine, NEW_DDL);
    check.execute_batch(T_DDL).unwrap();
    assert_eq!(ids(&check, "n"), vec![1]);
    assert_eq!(ids(&check, "t"), vec![1, 2, 3, 100]);
}

#[test]
fn create_times_out_with_busy_and_leaves_nothing_behind() {
    let fx = fixture("create_times_out", 150);
    let writer = fx.writer();
    writer
        .execute_batch("BEGIN; INSERT INTO t VALUES (100,'pending')")
        .unwrap();

    let started = Instant::now();
    let conn = SqliteConnection::open_in_memory().unwrap();
    conn.register_aydb_module_with_options(fx.engine.clone(), false)
        .unwrap();
    assert!(conn.execute_batch(NEW_DDL).is_err());
    let waited = started.elapsed();
    assert!(
        waited >= Duration::from_millis(100),
        "the create returned after {waited:?}, before the bounded wait elapsed"
    );
    assert!(waited < Duration::from_secs(10), "the wait is bounded");
    assert!(
        !fx.is_registered(4, "n"),
        "a refused create is not registered"
    );

    writer.execute_batch("COMMIT").unwrap();
    conn.execute_batch(NEW_DDL).unwrap();
    conn.execute_batch("INSERT INTO n VALUES (1,'one')")
        .unwrap();
    assert_eq!(ids(&conn, "n"), vec![1]);
}

#[test]
fn the_transaction_owner_creates_a_table_without_waiting() {
    let fx = fixture("owner_creates", 30_000);
    let conn = fx.writer();
    conn.execute_batch("BEGIN; INSERT INTO t VALUES (100,'pending')")
        .unwrap();
    let started = Instant::now();
    conn.execute_batch(NEW_DDL).unwrap();
    conn.execute_batch("INSERT INTO n VALUES (1,'one'); COMMIT")
        .unwrap();
    assert!(started.elapsed() < Duration::from_secs(10));
    assert_eq!(ids(&conn, "n"), vec![1]);
}

#[test]
fn a_failed_connect_keeps_the_registration_another_connection_uses() {
    let fx = fixture("connect_error_path", 30_000);
    let first = fx.open();
    assert_eq!(ids(&first, "t"), vec![1, 2, 3]);
    assert!(fx.is_registered(1, "t"));

    let second = fx.open();
    super::FAIL_CONNECT_AFTER_REGISTRATION.with(|flag| flag.set(true));
    assert!(second.query_all("SELECT id FROM t", &[]).is_err());
    assert!(
        fx.is_registered(1, "t"),
        "the failed connect removed a registration that the first connection still uses"
    );
    assert_eq!(ids(&first, "t"), vec![1, 2, 3]);
}

#[test]
fn repeated_connects_to_an_existing_table_never_fail_against_a_busy_writer() {
    const ATTEMPTS: usize = 300;
    let fx = fixture("connect_stress", 30_000);
    let writer = fx.writer();
    let stop = Arc::new(AtomicBool::new(false));
    let (ready_tx, ready_rx) = mpsc::channel();
    let engine = fx.engine.clone();
    let stop_flag = stop.clone();
    let busy = std::thread::spawn(move || {
        let writer_conn = memory_connection(&engine, T_DDL);
        ready_tx.send(()).unwrap();
        let mut transactions = 0u64;
        while !stop_flag.load(Ordering::Relaxed) {
            writer_conn
                .execute_batch("BEGIN; UPDATE t SET v='w' WHERE id=1")
                .unwrap();
            std::thread::sleep(Duration::from_millis(2));
            writer_conn.execute_batch("COMMIT").unwrap();
            transactions += 1;
        }
        transactions
    });
    drop(writer);
    ready_rx.recv().unwrap();

    let mut failures = 0usize;
    for _ in 0..ATTEMPTS {
        let conn = fx.open();
        match conn.query_all("SELECT count(*) FROM t", &[]) {
            Ok(rows) if rows == vec![vec![SqlValue::Integer(3)]] => {}
            _ => failures += 1,
        }
    }
    stop.store(true, Ordering::Relaxed);
    let transactions = busy.join().unwrap();
    eprintln!(
        "connect stress: {failures} failures in {ATTEMPTS} attempts ({transactions} writer transactions)"
    );
    assert_eq!(failures, 0);
}

#[test]
fn connecting_uncommitted_created_table_waits_instead_of_reporting_corrupt() {
    for ddl in [
        "CREATE VIRTUAL TABLE n USING aydb(columns='id INTEGER PRIMARY KEY, v TEXT')",
        NEW_DDL,
    ] {
        let fx = fixture("connect_uncommitted_create", 150);
        let conn1 = SqliteConnection::open_in_memory().unwrap();
        conn1
            .register_aydb_module_with_options(fx.engine.clone(), false)
            .unwrap();
        conn1.execute_batch("BEGIN").unwrap();
        conn1.execute_batch(ddl).unwrap();

        let conn2 = SqliteConnection::open_in_memory().unwrap();
        conn2
            .register_aydb_module_with_options(fx.engine.clone(), false)
            .unwrap();
        let started = Instant::now();
        let err = conn2.execute_batch(ddl).unwrap_err().to_string();
        assert!(started.elapsed() >= Duration::from_millis(100));
        assert!(!err.contains("unsupported older format"), "{err}");
        assert!(!err.contains("malformed"), "{err}");

        conn1.execute_batch("ROLLBACK").unwrap();

        conn2.execute_batch(ddl).unwrap();
        conn2
            .execute_batch("INSERT INTO n VALUES (1, 'hello')")
            .unwrap();
        assert_eq!(ids(&conn2, "n"), vec![1]);
    }
}

#[test]
fn connecting_during_uncommitted_rename_does_not_poison_rollback() {
    let fx = fixture("connect_uncommitted_rename", 150);
    let conn1 = fx.writer();
    conn1
        .execute_batch("BEGIN; INSERT INTO t VALUES (100, 'dirty'); ALTER TABLE t RENAME TO t2;")
        .unwrap();

    // Connecting under the uncommitted new name must wait and fail busy rather than poison
    // `tx_start_meta.active_vtab_ids`.
    let conn2 = SqliteConnection::open_in_memory().unwrap();
    conn2
        .register_aydb_module_with_options(fx.engine.clone(), false)
        .unwrap();
    let started = Instant::now();
    assert!(
        conn2
            .execute_batch(
                "CREATE VIRTUAL TABLE t2 USING aydb(columns='id INTEGER PRIMARY KEY, v TEXT')"
            )
            .is_err()
    );
    assert!(started.elapsed() >= Duration::from_millis(100));

    // A connection against the committed schema still reads committed `t` without waiting or
    // clobbering Conn 1's live `"t2"`.
    let conn3 = fx.open();
    assert_eq!(ids(&conn3, "t"), vec![1, 2, 3]);
    assert!(fx.is_registered(1, "t2"));

    conn1.execute_batch("ROLLBACK").unwrap();
    let conn4 = fx.open();
    assert_eq!(ids(&conn4, "t"), vec![1, 2, 3]);
    drop(conn4);

    // Commit path after a concurrent read of committed `t`.
    conn1
        .execute_batch(
            "BEGIN; INSERT INTO t VALUES (100, 'committed'); ALTER TABLE t RENAME TO t2;",
        )
        .unwrap();
    assert_eq!(ids(&conn3, "t"), vec![1, 2, 3]);
    conn1.execute_batch("COMMIT").unwrap();

    let conn5 = memory_connection(
        &fx.engine,
        "CREATE VIRTUAL TABLE t2 USING aydb(columns='id INTEGER PRIMARY KEY, v TEXT')",
    );
    assert_eq!(ids(&conn5, "t2"), vec![1, 2, 3, 100]);
}

#[test]
fn disconnecting_one_connection_keeps_shared_table_registered_until_last_disconnects() {
    let fx = fixture("refcount_shared", 30_000);
    let conn1 = fx.open();
    let conn2 = fx.open();
    assert_eq!(ids(&conn1, "t"), vec![1, 2, 3]);
    assert_eq!(ids(&conn2, "t"), vec![1, 2, 3]);
    assert!(fx.is_registered(1, "t"));

    // Closing conn1 must leave table_id 1 registered while conn2 still uses it.
    drop(conn1);
    assert!(fx.is_registered(1, "t"));
    conn2
        .execute_batch("INSERT INTO t VALUES (4, 'd')")
        .unwrap();
    conn2
        .execute_batch("ALTER TABLE t RENAME TO t_renamed")
        .unwrap();
    assert_eq!(ids(&conn2, "t_renamed"), vec![1, 2, 3, 4]);
    assert!(fx.is_registered(1, "t_renamed"));

    // Closing the final connection removes the live registration even after rename.
    drop(conn2);
    assert!(!fx.engine.read().unwrap().active_vtab_ids.contains_key(&1));
}

#[test]
fn stale_disconnect_after_drop_and_recreate_does_not_unregister_new_table() {
    let fx = fixture("stale_disconnect", 30_000);
    let conn_live = fx.open();
    let conn_stale = fx.open();
    assert_eq!(ids(&conn_live, "t"), vec![1, 2, 3]);
    assert_eq!(ids(&conn_stale, "t"), vec![1, 2, 3]);

    // Drop and recreate table_id 1 on `conn_live` while `conn_stale` still holds the old handle.
    conn_live.execute_batch("DROP TABLE t").unwrap();
    conn_live.execute_batch(T_DDL).unwrap();
    conn_live
        .execute_batch("INSERT INTO t VALUES (10, 'fresh')")
        .unwrap();
    assert!(fx.is_registered(1, "t"));

    // Dropping conn_stale disconnects its stale table_id=1 instance with an old reg_token;
    // the newly created table's live registration must stay intact.
    drop(conn_stale);
    assert!(fx.is_registered(1, "t"));
    assert_eq!(ids(&conn_live, "t"), vec![10]);

    drop(conn_live);
    assert!(!fx.engine.read().unwrap().active_vtab_ids.contains_key(&1));
}

#[test]
fn concurrent_create_rename_and_connect_across_connections_stay_consistent() {
    let fx = fixture("concurrent_ddl_connect", 2_000);
    let (writer_ready_tx, writer_ready_rx) = mpsc::channel();
    let (reader_done_tx, reader_done_rx) = mpsc::channel();

    let fx_engine = fx.engine.clone();
    let fx_schema = fx.schema_path.clone();
    let writer_thread = std::thread::spawn(move || {
        let conn = SqliteConnection::open_file(&fx_schema).unwrap();
        conn.register_aydb_module_with_options(fx_engine, false)
            .unwrap();
        conn.execute_batch("PRAGMA busy_timeout = 5000;").unwrap();
        conn.execute_batch("BEGIN; INSERT INTO t VALUES (4, 'd');")
            .unwrap();
        writer_ready_tx.send(()).unwrap();
        reader_done_rx.recv().unwrap();
        conn.execute_batch("COMMIT;").unwrap();

        // Now run rename + create concurrently with another thread creating a different table.
        conn.execute_batch(
            "ALTER TABLE t RENAME TO t_renamed;
             CREATE VIRTUAL TABLE m USING aydb(table_id=5, columns='id INTEGER PRIMARY KEY, v TEXT');
             INSERT INTO m VALUES (100, 'm1');",
        )
        .unwrap();
    });

    writer_ready_rx.recv().unwrap();
    let fx_engine_r = fx.engine.clone();
    let fx_schema_r = fx.schema_path.clone();
    let reader_thread = std::thread::spawn(move || {
        let reader = SqliteConnection::open_file(&fx_schema_r).unwrap();
        reader
            .register_aydb_module_with_options(fx_engine_r, false)
            .unwrap();
        reader.execute_batch("PRAGMA busy_timeout = 5000;").unwrap();
        let started = Instant::now();
        assert_eq!(ids(&reader, "t"), vec![1, 2, 3]);
        assert!(ids(&reader, "c").is_empty());
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "existing table xConnect must not wait on active writer"
        );
        reader_done_tx.send(()).unwrap();
    });
    reader_thread.join().unwrap();

    // Spawn a concurrent DDL connection alongside the writer's rename/create phase.
    let fx_engine_c = fx.engine.clone();
    let fx_schema_c = fx.schema_path.clone();
    let creator_thread = std::thread::spawn(move || {
        let creator = SqliteConnection::open_file(&fx_schema_c).unwrap();
        creator
            .register_aydb_module_with_options(fx_engine_c, false)
            .unwrap();
        creator
            .execute_batch("PRAGMA busy_timeout = 5000;")
            .unwrap();
        creator
            .execute_batch(
                "CREATE VIRTUAL TABLE n USING aydb(table_id=4, columns='id INTEGER PRIMARY KEY, v TEXT');
                 INSERT INTO n VALUES (200, 'n1');",
            )
            .unwrap();
    });

    writer_thread.join().unwrap();
    creator_thread.join().unwrap();

    let verify = fx.open();
    assert_eq!(ids(&verify, "t_renamed"), vec![1, 2, 3, 4]);
    assert!(verify.query_all("SELECT id FROM t", &[]).is_err());
    assert_eq!(ids(&verify, "m"), vec![100]);
    assert_eq!(ids(&verify, "n"), vec![200]);
    assert!(fx.is_registered(1, "t_renamed"));
    assert!(fx.is_registered(4, "n"));
    assert!(fx.is_registered(5, "m"));

    drop(verify);
    assert!(fx.engine.read().unwrap().active_vtab_ids.is_empty());
}

#[test]
fn lazy_xconnect_by_tx_owner_and_non_owner_preserve_registration_and_settings_on_rollback() {
    // R2-H6: connecting an existing persisted table lazily from the active transaction owner (or
    // from a non-owner connection) must keep its live registration token and runtime DDL options
    // (`on_corrupt`, `inline_max`) intact across ROLLBACK and ROLLBACK TO SAVEPOINT.
    let fx = fixture("r2_h6_lazy_xconnect_rollback", 2_000);
    {
        let conn1 = fx.open();
        conn1
            .execute_batch(
                "CREATE VIRTUAL TABLE t2 USING aydb(table_id=2, on_corrupt='evict', inline_max=128, columns='id INTEGER PRIMARY KEY, v TEXT');
                 INSERT INTO t2 VALUES (1, 'one');",
            )
            .unwrap();
    }

    // 1. Reopen connection so SQLite has not called xConnect on t2 yet; owner starts a tx on t,
    //    lazily connects t2 via SELECT, then ROLLBACKs.
    let conn2 = fx.open();
    conn2
        .execute_batch("BEGIN; INSERT INTO t VALUES (99, 'tmp');")
        .unwrap();
    assert_eq!(ids(&conn2, "t2"), vec![1]);
    assert_eq!(
        fx.engine.read().unwrap().corrupt_policy(2),
        crate::engine::CorruptPolicy::Evict
    );
    assert_eq!(fx.engine.read().unwrap().effective_inline_threshold(2), 128);
    conn2.execute_batch("ROLLBACK;").unwrap();
    // t2 handle must still be active (not STALE_VTAB_ERR) and keep its runtime settings.
    conn2
        .execute_batch("INSERT INTO t2 VALUES (2, 'two');")
        .unwrap();
    assert_eq!(ids(&conn2, "t2"), vec![1, 2]);
    assert_eq!(
        fx.engine.read().unwrap().corrupt_policy(2),
        crate::engine::CorruptPolicy::Evict
    );
    assert_eq!(fx.engine.read().unwrap().effective_inline_threshold(2), 128);

    // 2. Also verify ROLLBACK TO SAVEPOINT preserves lazy xConnect on a fresh handle.
    let conn3 = fx.open();
    conn3
        .execute_batch("BEGIN; SAVEPOINT sp1; INSERT INTO t VALUES (100, 'sp');")
        .unwrap();
    let conn3_lazy_reader = fx.open();
    // Test both owner lazy xConnect inside savepoint and non-owner xConnect before rollback.
    assert_eq!(ids(&conn3, "t2"), vec![1, 2]);
    assert_eq!(ids(&conn3_lazy_reader, "t2"), vec![1, 2]);
    conn3
        .execute_batch("ROLLBACK TO sp1; RELEASE sp1; ROLLBACK;")
        .unwrap();
    conn3
        .execute_batch("INSERT INTO t2 VALUES (3, 'three');")
        .unwrap();
    conn3_lazy_reader
        .execute_batch("INSERT INTO t2 VALUES (4, 'four');")
        .unwrap();
    assert_eq!(ids(&conn3, "t2"), vec![1, 2, 3, 4]);
    assert_eq!(
        fx.engine.read().unwrap().corrupt_policy(2),
        crate::engine::CorruptPolicy::Evict
    );
    assert_eq!(fx.engine.read().unwrap().effective_inline_threshold(2), 128);
}
