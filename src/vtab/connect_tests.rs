//! Connecting and creating tables while another connection has a transaction open.
//!
//! `xConnect` of an existing, fully migrated table changes no durable state, so it must succeed
//! (and must not wait) while another connection writes. `xCreate` of a new table and the
//! migration of a legacy table do write durable state; they wait, bounded by
//! `writer_wait_timeout_ms`, for the other transaction to end.

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
    _schema_dir: TempTestDir,
    _engine_dir: TempTestDir,
    schema_path: PathBuf,
    engine: Arc<RwLock<StorageEngine<OsFs>>>,
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
fn connect_to_a_migrated_table_succeeds_while_another_connection_writes() {
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
fn repeated_connects_to_a_migrated_table_never_fail_against_a_busy_writer() {
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
