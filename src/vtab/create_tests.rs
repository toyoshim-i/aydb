//! `CREATE VIRTUAL TABLE` inside a transaction: a rolled-back create must leave nothing that a
//! later table of the same name or id would pick up, and a committed one must persist.

use super::rename_tests::{Fixture, ints};
use super::*;

const T: &str =
    "CREATE VIRTUAL TABLE t USING aydb(columns='k INTEGER PRIMARY KEY, u TEXT UNIQUE, v INT')";
const T_ID: &str = "CREATE VIRTUAL TABLE t USING aydb(table_id=5, columns='k INTEGER PRIMARY KEY, u TEXT UNIQUE, v INT')";
/// A different layout under the same name.
const T_OTHER: &str =
    "CREATE VIRTUAL TABLE t USING aydb(columns='k TEXT PRIMARY KEY, w TEXT, v INT')";
const T_OTHER_ID: &str =
    "CREATE VIRTUAL TABLE t USING aydb(table_id=5, columns='k TEXT PRIMARY KEY, w TEXT, v INT')";
/// Another name that wants id 5 or the first free id.
const OTHER_ID: &str =
    "CREATE VIRTUAL TABLE o USING aydb(table_id=5, columns='k INTEGER PRIMARY KEY, v INT')";
const OTHER: &str = "CREATE VIRTUAL TABLE o USING aydb(columns='k INTEGER PRIMARY KEY, v INT')";

fn tables(conn: &SqliteConnection) -> Vec<String> {
    conn.query_all(
        "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name",
        &[],
    )
    .unwrap()
    .iter()
    .map(|r| format!("{:?}", r[0]))
    .collect()
}

/// `(create sql, matching re-create sql)` pairs: with and without an explicit id, and with the
/// same or a different column layout the second time.
const PAIRS: [(&str, &str); 4] = [(T, T), (T, T_OTHER), (T_ID, T_ID), (T_ID, T_OTHER_ID)];

#[test]
fn a_rolled_back_create_can_be_repeated_with_any_layout() {
    for (first, second) in PAIRS {
        let mut fixture = Fixture::new("create_rollback");
        let conn = fixture.open();
        conn.execute_batch(&format!("BEGIN; {first}")).unwrap();
        conn.execute_batch("ROLLBACK").unwrap();
        assert!(tables(&conn).is_empty(), "{first}");
        assert!(conn.query_all("SELECT * FROM t", &[]).is_err());
        conn.execute_batch(second).unwrap();
        assert!(
            ints(&conn, "SELECT v FROM t").is_empty(),
            "{first} / {second}"
        );
        conn.execute_batch("INSERT INTO t(k, v) VALUES (1, 10)")
            .unwrap();
        drop(conn);
        fixture.restart();
        let conn = fixture.open();
        assert_eq!(ints(&conn, "SELECT v FROM t"), [10], "{first} / {second}");
    }
}

#[test]
fn a_rolled_back_create_leaves_no_id_or_name_behind() {
    for (create, other) in [(T_ID, OTHER_ID), (T, OTHER)] {
        let mut fixture = Fixture::new("create_rollback_id");
        let conn = fixture.open();
        conn.execute_batch(&format!("BEGIN; {create}")).unwrap();
        conn.execute_batch("ROLLBACK").unwrap();
        // The id of the rolled-back table is free for another name.
        conn.execute_batch(other).unwrap();
        conn.execute_batch("INSERT INTO o VALUES (1, 1)").unwrap();
        drop(conn);
        fixture.restart();
        let conn = fixture.open();
        assert_eq!(ints(&conn, "SELECT v FROM o"), [1]);
        // The name of the rolled-back table is free again, with a table of its own.
        conn.execute_batch(T).unwrap();
        assert!(ints(&conn, "SELECT v FROM t").is_empty());
    }
}

#[test]
fn rows_of_a_rolled_back_create_do_not_reappear() {
    for (first, second) in PAIRS {
        let mut fixture = Fixture::new("create_insert_rollback");
        let conn = fixture.open();
        conn.execute_batch(&format!("BEGIN; {first}")).unwrap();
        if first == T || first == T_ID {
            conn.execute_batch("INSERT INTO t VALUES (1, 'a', 10), (2, 'b', 20)")
                .unwrap();
        }
        conn.execute_batch("ROLLBACK").unwrap();
        conn.execute_batch(second).unwrap();
        assert!(
            ints(&conn, "SELECT v FROM t").is_empty(),
            "{first} / {second}"
        );
        // No UNIQUE or key entry of the rolled-back rows is left to collide.
        if second == T || second == T_ID {
            conn.execute_batch("INSERT INTO t VALUES (1, 'a', 11)")
                .unwrap();
            assert_eq!(ints(&conn, "SELECT v FROM t"), [11]);
        } else {
            conn.execute_batch("INSERT INTO t VALUES ('k1', 'a', 11)")
                .unwrap();
        }
        drop(conn);
        fixture.restart();
        let conn = fixture.open();
        assert_eq!(ints(&conn, "SELECT v FROM t"), [11], "{first} / {second}");
    }
}

#[test]
fn a_create_inside_a_rolled_back_savepoint_is_undone() {
    for (first, second) in PAIRS {
        let mut fixture = Fixture::new("create_savepoint");
        let conn = fixture.open();
        conn.execute_batch("BEGIN; SAVEPOINT s").unwrap();
        conn.execute_batch(first).unwrap();
        if first == T || first == T_ID {
            conn.execute_batch("INSERT INTO t VALUES (1, 'a', 10)")
                .unwrap();
        }
        conn.execute_batch("ROLLBACK TO s").unwrap();
        assert!(conn.query_all("SELECT * FROM t", &[]).is_err());
        conn.execute_batch(second).unwrap();
        assert!(
            ints(&conn, "SELECT v FROM t").is_empty(),
            "{first} / {second}"
        );
        conn.execute_batch("COMMIT").unwrap();
        drop(conn);
        fixture.restart();
        let conn = fixture.open();
        assert!(
            ints(&conn, "SELECT v FROM t").is_empty(),
            "{first} / {second}"
        );
    }
}

#[test]
fn a_create_and_insert_committed_together_persist() {
    for create in [T, T_ID] {
        let mut fixture = Fixture::new("create_commit");
        let conn = fixture.open();
        conn.execute_batch(&format!(
            "BEGIN; {create}; INSERT INTO t VALUES (1, 'a', 10); INSERT INTO t VALUES (2, 'b', 20); COMMIT"
        ))
        .unwrap();
        drop(conn);
        fixture.restart();
        let conn = fixture.open();
        assert_eq!(ints(&conn, "SELECT v FROM t ORDER BY k"), [10, 20]);
        assert!(
            conn.execute_batch("INSERT INTO t VALUES (3, 'a', 30)")
                .is_err()
        );
    }
}

#[test]
fn a_create_after_a_write_to_another_table_rolls_back_with_it() {
    for create in [T, T_ID] {
        let mut fixture = Fixture::new("create_after_write");
        let conn = fixture.open();
        conn.execute_batch(
            "CREATE VIRTUAL TABLE o USING aydb(table_id=9, columns='k INTEGER PRIMARY KEY, v INT')",
        )
        .unwrap();
        conn.execute_batch(&format!(
            "BEGIN; INSERT INTO o VALUES (1, 1); {create}; INSERT INTO t VALUES (1, 'a', 10)"
        ))
        .unwrap();
        conn.execute_batch("ROLLBACK").unwrap();
        assert!(ints(&conn, "SELECT v FROM o").is_empty());
        conn.execute_batch(create).unwrap();
        assert!(ints(&conn, "SELECT v FROM t").is_empty());
        drop(conn);
        fixture.restart();
        let conn = fixture.open();
        assert!(ints(&conn, "SELECT v FROM t").is_empty());
        assert!(ints(&conn, "SELECT v FROM o").is_empty());
    }
}

/// SQLite never tells a table that was created inside a savepoint about that savepoint, so
/// nothing ends the engine's part of a create that `ROLLBACK TO` undid. The commit finds out.
#[test]
fn a_create_undone_by_rollback_to_does_not_survive_the_commit() {
    for (create, insert) in [
        (T, "INSERT INTO t VALUES (1, 'a', 10)"),
        (T_ID, "INSERT INTO t VALUES (1, 'a', 10)"),
    ] {
        let mut fixture = Fixture::new("create_rollback_to_commit");
        let conn = fixture.open();
        conn.execute_batch("BEGIN; SAVEPOINT s").unwrap();
        conn.execute_batch(create).unwrap();
        conn.execute_batch(insert).unwrap();
        conn.execute_batch("ROLLBACK TO s; COMMIT").unwrap();
        assert!(tables(&conn).is_empty());
        drop(conn);
        fixture.restart();
        let conn = fixture.open();
        // Neither the name, the id nor the rows are left to adopt.
        conn.execute_batch(create).unwrap();
        assert!(ints(&conn, "SELECT v FROM t").is_empty(), "{create}");
    }
}

/// A table created in a savepoint that is released, not rolled back, is kept.
#[test]
fn a_create_in_a_released_savepoint_is_kept() {
    let mut fixture = Fixture::new("create_release");
    let conn = fixture.open();
    conn.execute_batch("BEGIN; SAVEPOINT s").unwrap();
    conn.execute_batch(T).unwrap();
    conn.execute_batch("INSERT INTO t VALUES (1, 'a', 10); RELEASE s; COMMIT")
        .unwrap();
    drop(conn);
    fixture.restart();
    assert_eq!(ints(&fixture.open(), "SELECT v FROM t"), [10]);
}

const OTHER_9: &str =
    "CREATE VIRTUAL TABLE o USING aydb(table_id=9, columns='k INTEGER PRIMARY KEY, v INT')";

/// `o` is another aydb table of the transaction, so the engine is told about the savepoint and
/// its rollback also undoes `t`, which was created after it. Nothing may then try to drop or
/// keep `t` a second time.
#[test]
fn a_create_undone_by_an_announced_savepoint_rollback_leaves_no_trace() {
    for create in [T, T_ID] {
        for recreate_before_commit in [false, true] {
            let mut fixture = Fixture::new("create_announced_savepoint");
            let conn = fixture.open();
            conn.execute_batch(OTHER_9).unwrap();
            conn.execute_batch(
                "BEGIN; INSERT INTO o VALUES (1, 1); SAVEPOINT s; INSERT INTO o VALUES (2, 2)",
            )
            .unwrap();
            conn.execute_batch(create).unwrap();
            conn.execute_batch("INSERT INTO t VALUES (1, 'a', 10)")
                .unwrap();
            conn.execute_batch("ROLLBACK TO s").unwrap();
            if recreate_before_commit {
                conn.execute_batch(create).unwrap();
                assert!(ints(&conn, "SELECT v FROM t").is_empty());
                conn.execute_batch("INSERT INTO t VALUES (2, 'b', 20)")
                    .unwrap();
            }
            conn.execute_batch("COMMIT").unwrap();
            drop(conn);
            fixture.restart();
            let conn = fixture.open();
            assert_eq!(ints(&conn, "SELECT v FROM o"), [1]);
            if recreate_before_commit {
                assert_eq!(ints(&conn, "SELECT v FROM t"), [20], "{create}");
            } else {
                conn.execute_batch(create).unwrap();
                assert!(ints(&conn, "SELECT v FROM t").is_empty(), "{create}");
            }
        }
    }
}

#[test]
fn a_create_in_a_released_announced_savepoint_is_kept() {
    for create in [T, T_ID] {
        let mut fixture = Fixture::new("create_announced_release");
        let conn = fixture.open();
        conn.execute_batch(OTHER_9).unwrap();
        conn.execute_batch(
            "BEGIN; INSERT INTO o VALUES (1, 1); SAVEPOINT s; INSERT INTO o VALUES (2, 2)",
        )
        .unwrap();
        conn.execute_batch(create).unwrap();
        conn.execute_batch("INSERT INTO t VALUES (1, 'a', 10); RELEASE s; COMMIT")
            .unwrap();
        drop(conn);
        fixture.restart();
        let conn = fixture.open();
        assert_eq!(ints(&conn, "SELECT v FROM o ORDER BY k"), [1, 2]);
        assert_eq!(ints(&conn, "SELECT v FROM t"), [10], "{create}");
    }
}

/// The id of a table that an announced savepoint rollback removed is free again within the same
/// transaction; the table that takes it must not be dropped for the sake of the old one.
#[test]
fn an_id_freed_by_an_announced_savepoint_rollback_can_be_taken_in_the_same_transaction() {
    for (create, next) in [
        (
            T_ID,
            "CREATE VIRTUAL TABLE x USING aydb(table_id=5, columns='k INTEGER PRIMARY KEY, v INT')",
        ),
        (
            T,
            "CREATE VIRTUAL TABLE x USING aydb(columns='k INTEGER PRIMARY KEY, v INT')",
        ),
    ] {
        let mut fixture = Fixture::new("create_freed_id");
        let conn = fixture.open();
        conn.execute_batch(OTHER_9).unwrap();
        conn.execute_batch(
            "BEGIN; INSERT INTO o VALUES (1, 1); SAVEPOINT s; INSERT INTO o VALUES (2, 2)",
        )
        .unwrap();
        conn.execute_batch(create).unwrap();
        conn.execute_batch("INSERT INTO t VALUES (1, 'a', 10)")
            .unwrap();
        conn.execute_batch("ROLLBACK TO s").unwrap();
        conn.execute_batch(next).unwrap();
        conn.execute_batch("INSERT INTO x VALUES (1, 77)").unwrap();
        conn.execute_batch("COMMIT").unwrap();
        assert_eq!(ints(&conn, "SELECT v FROM x"), [77], "{create}");
        drop(conn);
        fixture.restart();
        let conn = fixture.open();
        assert_eq!(ints(&conn, "SELECT v FROM x"), [77], "{create}");
        assert_eq!(ints(&conn, "SELECT v FROM o"), [1]);
    }
}

#[test]
fn other_connection_create_does_not_drop_uncommitted_vtab() {
    use crate::engine::EngineConfig;
    use crate::util::test_util::TempTestDir;

    for conn2_in_tx in [false, true] {
        let dir = TempTestDir::new("create_other_conn_guard");
        let fs = Arc::new(OsFs::open(&dir.path, true).unwrap());
        let mut config = EngineConfig::default();
        config.writer_wait_timeout_ms = 20;
        let engine = Arc::new(RwLock::new(
            StorageEngine::open(Arc::clone(&fs), config.clone()).unwrap(),
        ));

        let conn1 = SqliteConnection::open_in_memory().unwrap();
        conn1
            .register_aydb_module_with_options(Arc::clone(&engine), false)
            .unwrap();
        let conn2 = SqliteConnection::open_in_memory().unwrap();
        conn2
            .register_aydb_module_with_options(Arc::clone(&engine), false)
            .unwrap();

        conn1
            .execute_batch(&format!(
                "BEGIN; {T_ID}; INSERT INTO t VALUES (1, 'a', 100)"
            ))
            .unwrap();

        if conn2_in_tx {
            conn2.execute_batch("BEGIN").unwrap();
        }
        assert!(
            conn2.execute_batch(T_ID).is_err(),
            "concurrent CREATE must fail with busy instead of touching conn1's table"
        );
        if conn2_in_tx {
            let _ = conn2.execute_batch("ROLLBACK");
        }

        conn1.execute_batch("COMMIT").unwrap();
        assert_eq!(ints(&conn1, "SELECT v FROM t"), [100]);

        drop(conn2);
        drop(conn1);
        drop(engine);

        let reopened = Arc::new(RwLock::new(StorageEngine::open(fs, config).unwrap()));
        let check = SqliteConnection::open_in_memory().unwrap();
        check
            .register_aydb_module_with_options(reopened, false)
            .unwrap();
        check.execute_batch(T_ID).unwrap();
        assert_eq!(ints(&check, "SELECT v FROM t"), [100]);
    }
}

#[test]
fn duplicate_table_id_in_same_tx_does_not_drop_live_table() {
    let mut fixture = Fixture::new("create_dup_id_same_tx");
    let conn = fixture.open();
    conn.execute_batch(
        "BEGIN;
         CREATE VIRTUAL TABLE t1 USING aydb(table_id=5, columns='k INTEGER PRIMARY KEY, u TEXT UNIQUE, v INT');
         INSERT INTO t1 VALUES (1, 'a', 100);",
    )
    .unwrap();
    assert!(
        conn.execute_batch(
            "CREATE VIRTUAL TABLE t2 USING aydb(table_id=5, columns='k INTEGER PRIMARY KEY, u TEXT UNIQUE, v INT')",
        )
        .is_err(),
        "duplicate explicit table_id in active tx must be rejected"
    );
    conn.execute_batch("COMMIT").unwrap();

    assert_eq!(ints(&conn, "SELECT v FROM t1"), [100]);
    assert!(conn.query_all("SELECT * FROM t2", &[]).is_err());

    drop(conn);
    fixture.restart();
    let conn = fixture.open();
    assert_eq!(ints(&conn, "SELECT v FROM t1"), [100]);
    assert!(conn.query_all("SELECT * FROM t2", &[]).is_err());
}

#[test]
fn savepoint_rollback_allows_reusing_explicit_table_id_with_new_name() {
    let mut fixture = Fixture::new("create_sp_reuse_id");
    let conn = fixture.open();
    conn.execute_batch(
        "BEGIN;
         SAVEPOINT s;
         CREATE VIRTUAL TABLE t1 USING aydb(table_id=5, columns='k INTEGER PRIMARY KEY, v INT');
         INSERT INTO t1 VALUES (1, 100);
         ROLLBACK TO s;
         CREATE VIRTUAL TABLE t2 USING aydb(table_id=5, columns='k INTEGER PRIMARY KEY, v INT');
         COMMIT;",
    )
    .unwrap();

    assert!(ints(&conn, "SELECT v FROM t2").is_empty());
    assert!(conn.query_all("SELECT * FROM t1", &[]).is_err());

    drop(conn);
    fixture.restart();
    let conn = fixture.open();
    assert!(ints(&conn, "SELECT v FROM t2").is_empty());
    assert!(conn.query_all("SELECT * FROM t1", &[]).is_err());
}

#[test]
fn attached_db_create_with_same_name_does_not_drop_main_table() {
    let mut fixture = Fixture::new("create_attached_same_name");
    let conn = fixture.open();
    conn.execute_batch(
        "ATTACH DATABASE ':memory:' AS aux;
         BEGIN;
         CREATE VIRTUAL TABLE main.t USING aydb(table_id=5, columns='k INTEGER PRIMARY KEY, v INT');
         INSERT INTO main.t VALUES (1, 42);",
    )
    .unwrap();
    assert!(
        conn.execute_batch(
            "CREATE VIRTUAL TABLE aux.t USING aydb(table_id=9, columns='k INTEGER PRIMARY KEY, v INT')",
        )
        .is_err(),
        "creating aux.t when main.t holds catalog name 't' must fail instead of dropping main.t"
    );
    conn.execute_batch("COMMIT").unwrap();

    assert_eq!(ints(&conn, "SELECT v FROM main.t"), [42]);

    drop(conn);
    fixture.restart();
    let conn = fixture.open();
    assert_eq!(ints(&conn, "SELECT v FROM main.t"), [42]);
}

#[test]
fn a_create_and_rename_inside_a_rolled_back_savepoint_is_undone_on_commit() {
    for with_other_table in [false, true] {
        let mut fixture = Fixture::new("create_rename_sp_rollback");
        let conn = fixture.open();
        if with_other_table {
            conn.execute_batch(
                "CREATE VIRTUAL TABLE u USING aydb(columns='k INTEGER PRIMARY KEY, v INT');
                 BEGIN;
                 INSERT INTO u VALUES (1, 100);",
            )
            .unwrap();
        } else {
            conn.execute_batch("BEGIN;").unwrap();
        }
        conn.execute_batch(
            "SAVEPOINT s;
             CREATE VIRTUAL TABLE t USING aydb(columns='k INTEGER PRIMARY KEY, u TEXT UNIQUE, v INT');
             INSERT INTO t VALUES (1, 'a', 10);
             ALTER TABLE t RENAME TO t2;
             ROLLBACK TO s;
             COMMIT;",
        )
        .unwrap();
        drop(conn);
        fixture.restart();
        let conn = fixture.open();
        conn.execute_batch(T).unwrap();
        assert!(ints(&conn, "SELECT v FROM t").is_empty());
        conn.execute_batch("INSERT INTO t VALUES (1, 'a', 20)")
            .unwrap();
        assert_eq!(ints(&conn, "SELECT v FROM t"), [20]);
        conn.execute_batch("DROP TABLE t").unwrap();
        conn.execute_batch(
            "CREATE VIRTUAL TABLE t2 USING aydb(columns='k INTEGER PRIMARY KEY, u TEXT UNIQUE, v INT')",
        )
        .unwrap();
        assert!(ints(&conn, "SELECT v FROM t2").is_empty());
    }
}

#[test]
fn a_create_before_savepoint_and_rename_inside_rolled_back_savepoint_keeps_original_name() {
    let mut fixture = Fixture::new("create_then_rename_sp_rollback");
    let conn = fixture.open();
    conn.execute_batch(
        "BEGIN;
         CREATE VIRTUAL TABLE t USING aydb(columns='k INTEGER PRIMARY KEY, u TEXT UNIQUE, v INT');
         INSERT INTO t VALUES (1, 'a', 10);
         SAVEPOINT s;
         ALTER TABLE t RENAME TO t2;
         ROLLBACK TO s;
         COMMIT;",
    )
    .unwrap();
    drop(conn);
    fixture.restart();
    let conn = fixture.open();
    assert_eq!(ints(&conn, "SELECT v FROM t"), [10]);
    assert!(conn.query_all("SELECT * FROM t2", &[]).is_err());
}

#[test]
fn concurrent_writer_does_not_cause_busy_on_multi_table_commit_or_rollback() {
    use crate::engine::EngineConfig;
    use crate::util::test_util::TempTestDir;
    use std::sync::Barrier;

    let tmp = TempTestDir::new("multi_table_tx_race");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut cfg = EngineConfig::default();
    cfg.sync_wal_size_bytes = 64 * 4096;
    cfg.buf_wal_size_bytes = 64 * 4096;
    cfg.writer_wait_timeout_ms = 5_000;
    let engine = Arc::new(RwLock::new(StorageEngine::open(fs, cfg).unwrap()));

    let conn1 = SqliteConnection::open_in_memory().unwrap();
    conn1
        .register_aydb_module_with_options(engine.clone(), false)
        .unwrap();
    conn1
        .execute_batch(
            "CREATE VIRTUAL TABLE t1 USING aydb(table_id=1, columns='k INTEGER PRIMARY KEY, v INT');
             CREATE VIRTUAL TABLE t2 USING aydb(table_id=2, columns='k INTEGER PRIMARY KEY, v INT');
             CREATE VIRTUAL TABLE u  USING aydb(table_id=3, columns='k INTEGER PRIMARY KEY, v INT');",
        )
        .unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let ready = Arc::new(Barrier::new(2));
    let competitor = {
        let engine = engine.clone();
        let stop = stop.clone();
        let ready = ready.clone();
        std::thread::spawn(move || {
            let conn2 = SqliteConnection::open_in_memory().unwrap();
            conn2
                .register_aydb_module_with_options(engine, false)
                .unwrap();
            conn2
                .execute_batch(
                    "CREATE VIRTUAL TABLE u USING aydb(table_id=3, columns='k INTEGER PRIMARY KEY, v INT');",
                )
                .unwrap();
            ready.wait();
            let mut n = 0i64;
            while !stop.load(Ordering::Relaxed) {
                n += 1;
                conn2
                    .execute_batch(&format!("BEGIN; INSERT INTO u VALUES ({n}, {n}); COMMIT;"))
                    .unwrap_or_else(|e| panic!("competitor commit {n} failed: {e}"));
            }
            n
        })
    };
    ready.wait();

    // A: Multi-table ROLLBACK under continuous writer contention.
    for i in 0..40i64 {
        conn1
            .execute_batch(&format!(
                "BEGIN; INSERT INTO t1 VALUES ({i}, {i}); INSERT INTO t2 VALUES ({i}, {i}); ROLLBACK;"
            ))
            .unwrap_or_else(|e| panic!("multi-table rollback {i} failed: {e}"));
    }

    // B: Multi-table transactional CREATE VIRTUAL TABLE + COMMIT under contention.
    for i in 0..20i64 {
        let a = format!("a_{i}");
        let b = format!("b_{i}");
        conn1
            .execute_batch(&format!(
                "BEGIN;
                 CREATE VIRTUAL TABLE {a} USING aydb(columns='k INTEGER PRIMARY KEY, v INT');
                 CREATE VIRTUAL TABLE {b} USING aydb(columns='k INTEGER PRIMARY KEY, v INT');
                 COMMIT;"
            ))
            .unwrap_or_else(|e| panic!("multi-table create commit {i} failed: {e}"));
    }

    // C: Multi-table implicit SAVEPOINT + RELEASE under contention.
    for i in 0..40i64 {
        let k = 1000 + i;
        conn1
            .execute_batch(&format!(
                "SAVEPOINT sp; INSERT INTO t1 VALUES ({k}, {k}); INSERT INTO t2 VALUES ({k}, {k}); RELEASE sp;"
            ))
            .unwrap_or_else(|e| panic!("multi-table release {i} failed: {e}"));
    }

    stop.store(true, Ordering::Relaxed);
    let competitor_rows = competitor.join().unwrap();
    assert!(competitor_rows > 0);
    assert_eq!(ints(&conn1, "SELECT count(*) FROM t1"), [40]);
    assert_eq!(ints(&conn1, "SELECT count(*) FROM t2"), [40]);
    assert_eq!(ints(&conn1, "SELECT count(*) FROM u"), [competitor_rows]);
}

#[test]
fn whitespace_padded_table_names_are_distinct() {
    let mut fixture = Fixture::new("whitespace_padded_names");
    let conn = fixture.open();

    // Exact audit scenario: create `x`, insert row, then create `" x "` in the same SQLite schema.
    conn.execute_batch(
        "CREATE VIRTUAL TABLE x USING aydb(columns='k INTEGER PRIMARY KEY, v INT');
         INSERT INTO x VALUES (1, 100);
         CREATE VIRTUAL TABLE \" x \" USING aydb(columns='k INTEGER PRIMARY KEY, v INT');",
    )
    .unwrap();
    assert!(
        ints(&conn, "SELECT v FROM \" x \"").is_empty(),
        "\" x \" must not read rows belonging to x"
    );
    conn.execute_batch("INSERT INTO \" x \" VALUES (1, 200)")
        .unwrap();

    // Also test leading-only, trailing-only, and single-space identifiers.
    conn.execute_batch(
        "CREATE VIRTUAL TABLE \" x\" USING aydb(columns='k INTEGER PRIMARY KEY, v INT');
         CREATE VIRTUAL TABLE \"x \" USING aydb(columns='k INTEGER PRIMARY KEY, v INT');
         CREATE VIRTUAL TABLE \" \" USING aydb(columns='k INTEGER PRIMARY KEY, v INT');
         INSERT INTO \" x\" VALUES (1, 300);
         INSERT INTO \"x \" VALUES (1, 400);
         INSERT INTO \" \" VALUES (1, 500);",
    )
    .unwrap();

    assert_eq!(ints(&conn, "SELECT v FROM x"), [100]);
    assert_eq!(ints(&conn, "SELECT v FROM \" x \""), [200]);
    assert_eq!(ints(&conn, "SELECT v FROM \" x\""), [300]);
    assert_eq!(ints(&conn, "SELECT v FROM \"x \""), [400]);
    assert_eq!(ints(&conn, "SELECT v FROM \" \""), [500]);

    // Case-folding still preserves whitespace (`" X "` maps to `" x "`, not `x`).
    let id_trimmed = fixture
        .engine()
        .write()
        .unwrap()
        .register_or_resolve_vtab("x", None)
        .unwrap();
    let id_padded_lower = fixture
        .engine()
        .write()
        .unwrap()
        .register_or_resolve_vtab(" x ", None)
        .unwrap();
    let id_padded_upper = fixture
        .engine()
        .write()
        .unwrap()
        .register_or_resolve_vtab(" X ", None)
        .unwrap();
    assert_ne!(id_trimmed, id_padded_lower);
    assert_eq!(id_padded_lower, id_padded_upper);

    // Rename `" x "` -> `" y "` and DROP `x`; other whitespace-named tables stay intact across restart.
    conn.execute_batch("ALTER TABLE \" x \" RENAME TO \" y \"; DROP TABLE x;")
        .unwrap();
    drop(conn);
    fixture.restart();
    let conn = fixture.open();
    assert!(conn.query_all("SELECT v FROM x", &[]).is_err());
    assert!(conn.query_all("SELECT v FROM \" x \"", &[]).is_err());
    assert_eq!(ints(&conn, "SELECT v FROM \" y \""), [200]);
    assert_eq!(ints(&conn, "SELECT v FROM \" x\""), [300]);
    assert_eq!(ints(&conn, "SELECT v FROM \"x \""), [400]);
    assert_eq!(ints(&conn, "SELECT v FROM \" \""), [500]);
}

#[test]
fn in_tx_drop_and_reused_table_id_preserves_live_capped_lru_blobs_on_commit() {
    for explicit_id in [false, true] {
        for on_corrupt in ["error", "evict"] {
            let mut fixture = Fixture::new(&format!("reuse_capped_lru_{explicit_id}_{on_corrupt}"));
            let id_opt = if explicit_id { "table_id=5, " } else { "" };
            let ddl = format!(
                "CREATE VIRTUAL TABLE t1 USING aydb({id_opt}max_bytes=1048576, on_corrupt='{on_corrupt}', columns='k INT PRIMARY KEY, v BLOB')"
            );
            {
                let conn = fixture.open();
                conn.execute_batch(&format!(
                    "BEGIN;
                     SAVEPOINT s1;
                     {ddl};
                     INSERT INTO t1 VALUES (1, zeroblob(4096));
                     ROLLBACK TO s1;
                     {ddl};
                     INSERT INTO t1 VALUES (2, zeroblob(4096));
                     COMMIT;"
                ))
                .unwrap();

                // Recreated table's row 2 blob must remain intact right after commit (not reclaimed).
                assert_eq!(
                    conn.query_all("SELECT k, length(v) FROM t1 ORDER BY k", &[])
                        .unwrap(),
                    vec![vec![SqlValue::Integer(2), SqlValue::Integer(4096)]]
                );
            }
            fixture.restart();
            let conn = fixture.open();
            assert_eq!(
                conn.query_all("SELECT k, length(v) FROM t1 ORDER BY k", &[])
                    .unwrap(),
                vec![vec![SqlValue::Integer(2), SqlValue::Integer(4096)]]
            );
        }
    }
}

#[test]
fn autocommit_create_under_read_lock_and_wal_sync_failure_rolls_back_atomically() {
    let create_sql = "CREATE VIRTUAL TABLE t USING aydb(table_id=7, max_bytes=4096, durability='sync', columns='k INTEGER PRIMARY KEY, v BLOB')";

    // (a) SHARED read lock (SQLITE_BUSY) must roll back both SQLite schema and aydb catalog/quota.
    {
        let mut fixture = Fixture::new("autocommit_create_lock");
        let conn1 = fixture.open();
        conn1
            .execute_batch("CREATE TABLE n(a INT); INSERT INTO n VALUES (1);")
            .unwrap();
        let conn2 = fixture.open();
        conn2.execute_batch("BEGIN; SELECT * FROM n;").unwrap();

        let err = conn1
            .execute_batch(create_sql)
            .expect_err("autocommit CREATE under SHARED lock must fail with SQLITE_BUSY")
            .to_string();
        assert!(
            err.to_ascii_lowercase().contains("locked"),
            "expected locked error, got: {err}"
        );
        conn2.execute_batch("COMMIT").unwrap();

        assert!(
            conn1
                .query_all(
                    "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 't'",
                    &[],
                )
                .unwrap()
                .is_empty()
        );
        {
            let engine_arc = fixture.engine();
            let eng = engine_arc.read().unwrap();
            assert!(
                eng.capped_table_state_view(crate::engine::ReadView::Committed, 7)
                    .is_none()
            );
            assert!(eng.active_vtab_name(7).is_none());
        }

        // Retrying the same CREATE after releasing the lock succeeds cleanly and persists.
        conn1.execute_batch(create_sql).unwrap();
        conn1
            .execute_batch("INSERT INTO t VALUES (1, zeroblob(64))")
            .unwrap();
        drop(conn1);
        drop(conn2);
        fixture.restart();
        let conn = fixture.open();
        assert_eq!(
            conn.query_all("SELECT k, length(v) FROM t", &[]).unwrap(),
            vec![vec![SqlValue::Integer(1), SqlValue::Integer(64)]]
        );
    }

    // (b) WAL sync failure must poison the engine and roll back SQLite schema and aydb catalog/quota.
    {
        let mut fixture = Fixture::new("autocommit_create_wal_sync_fail");
        let conn = fixture.open();
        fixture.engine().read().unwrap().debug_fail_next_wal_sync();
        let err = conn
            .execute_batch(create_sql)
            .expect_err("autocommit CREATE must report WAL sync failure")
            .to_string();
        assert!(
            err.contains("injected WAL sync failure"),
            "unexpected error: {err}"
        );
        assert!(fixture.engine().read().unwrap().is_poisoned());
        assert!(tables(&conn).is_empty());

        drop(conn);
        fixture.restart();
        assert!(
            fixture
                .engine()
                .read()
                .unwrap()
                .capped_table_state_view(crate::engine::ReadView::Committed, 7)
                .is_none()
        );
        let conn = fixture.open();
        conn.execute_batch(create_sql).unwrap();
        conn.execute_batch("INSERT INTO t VALUES (2, zeroblob(32))")
            .unwrap();
        assert_eq!(
            conn.query_all("SELECT k, length(v) FROM t", &[]).unwrap(),
            vec![vec![SqlValue::Integer(2), SqlValue::Integer(32)]]
        );
    }
}

#[test]
fn failed_create_inside_read_tx_preserves_read_snapshot_and_upgrade_check() {
    let tmp = crate::util::test_util::TempTestDir::new("failed_create_read_snapshot");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let engine = Arc::new(RwLock::new(
        StorageEngine::open(fs, crate::engine::EngineConfig::default()).unwrap(),
    ));
    let conn1 = SqliteConnection::open_in_memory().unwrap();
    let conn2 = SqliteConnection::open_in_memory().unwrap();
    let ddl =
        "CREATE VIRTUAL TABLE t1 USING aydb(table_id=1, columns='k INTEGER PRIMARY KEY, v INT')";
    for conn in [&conn1, &conn2] {
        conn.register_aydb_module_with_options(Arc::clone(&engine), false)
            .unwrap();
        conn.execute_batch(ddl).unwrap();
    }
    conn1
        .execute_batch("INSERT INTO t1 VALUES (1, 10)")
        .unwrap();

    conn1.execute_batch("BEGIN;").unwrap();
    assert_eq!(ints(&conn1, "SELECT v FROM t1 WHERE k = 1"), [10]);

    assert!(
        conn1
            .execute_batch(
                "CREATE VIRTUAL TABLE bad USING aydb(table_id=1, columns='k INTEGER PRIMARY KEY, v INT')",
            )
            .is_err(),
        "creating a second table bound to occupied table_id=1 must fail"
    );

    conn2
        .execute_batch("UPDATE t1 SET v = 20 WHERE k = 1")
        .unwrap();

    // Failed CREATE must not discard conn1's active read snapshot or bypass stale write checks.
    assert_eq!(ints(&conn1, "SELECT v FROM t1 WHERE k = 1"), [10]);
    assert!(
        conn1
            .execute_batch("INSERT INTO t1 VALUES (2, 200)")
            .is_err(),
        "stale read snapshot must reject write upgrade after failed CREATE"
    );

    conn1.execute_batch("ROLLBACK;").unwrap();
    assert_eq!(ints(&conn1, "SELECT v FROM t1 WHERE k = 1"), [20]);
}
