//! `DROP TABLE` of an aydb table and transactions: whatever SQLite keeps in its schema after a
//! ROLLBACK must match what the engine still holds, so a table is never left restored in the
//! schema but emptied in the engine.

use super::rename_tests::{Fixture, ints};
use super::*;

struct Variant {
    name: &'static str,
    create: &'static str,
    /// Key literal of row `n`.
    int_key: bool,
}

const VARIANTS: [Variant; 2] = [
    Variant {
        name: "integer_pk_unique",
        create: "CREATE VIRTUAL TABLE t USING aydb(columns='k INTEGER PRIMARY KEY, u TEXT UNIQUE, v INT')",
        int_key: true,
    },
    Variant {
        name: "text_pk_capped",
        create: "CREATE VIRTUAL TABLE t USING aydb(max_bytes=1000000, columns='k TEXT PRIMARY KEY, u TEXT UNIQUE, v INT')",
        int_key: false,
    },
];

const OTHER: &str = "CREATE VIRTUAL TABLE o USING aydb(columns='k INTEGER PRIMARY KEY, v INT')";

impl Variant {
    fn insert(&self, table: &str, n: i64) -> String {
        let key = if self.int_key {
            n.to_string()
        } else {
            format!("'k{n}'")
        };
        format!("INSERT INTO {table} VALUES ({key}, 'u{n}', {})", n * 10)
    }

    /// The table holds exactly the rows `1..=n` and behaves as a whole table: its UNIQUE
    /// index, its rowid mapping and its capped quota are still there.
    fn assert_intact(&self, conn: &SqliteConnection, rows: &[i64], what: &str) {
        let values: Vec<i64> = rows.iter().map(|n| n * 10).collect();
        assert_eq!(
            ints(conn, "SELECT v FROM t ORDER BY v"),
            values,
            "{} {what}: rows",
            self.name
        );
        for n in rows {
            assert_eq!(
                ints(
                    conn,
                    &format!(
                        "SELECT count(*) FROM t WHERE rowid = (SELECT rowid FROM t WHERE v = {})",
                        n * 10
                    )
                ),
                [1],
                "{} {what}: rowid of {n}",
                self.name
            );
        }
        if let Some(n) = rows.first() {
            assert!(
                conn.execute_batch(&format!(
                    "INSERT INTO t VALUES ({}, 'u{n}', 0)",
                    if self.int_key {
                        "1000".to_string()
                    } else {
                        "'dup'".to_string()
                    }
                ))
                .is_err(),
                "{} {what}: UNIQUE index still enforced",
                self.name
            );
        }
    }
}

/// `BEGIN; DROP TABLE t; ROLLBACK`: the table must come back whole, whether or not the DROP
/// was allowed to run.
#[test]
fn a_dropped_table_comes_back_whole_after_rollback_of_the_first_statement() {
    for variant in &VARIANTS {
        let mut fixture = Fixture::new("drop_first");
        let conn = fixture.open();
        conn.execute_batch(variant.create).unwrap();
        conn.execute_batch(&variant.insert("t", 1)).unwrap();
        conn.execute_batch(&variant.insert("t", 2)).unwrap();
        conn.execute_batch("BEGIN").unwrap();
        let _ = conn.execute_batch("DROP TABLE t");
        conn.execute_batch("ROLLBACK").unwrap();
        variant.assert_intact(&conn, &[1, 2], "after ROLLBACK");
        drop(conn);
        fixture.restart();
        variant.assert_intact(&fixture.open(), &[1, 2], "after restart");
    }
}

#[test]
fn a_drop_after_a_write_to_the_same_table_is_rolled_back() {
    for variant in &VARIANTS {
        let mut fixture = Fixture::new("drop_after_write");
        let conn = fixture.open();
        conn.execute_batch(variant.create).unwrap();
        conn.execute_batch(&variant.insert("t", 1)).unwrap();
        conn.execute_batch("BEGIN").unwrap();
        conn.execute_batch(&variant.insert("t", 2)).unwrap();
        let _ = conn.execute_batch("DROP TABLE t");
        conn.execute_batch("ROLLBACK").unwrap();
        variant.assert_intact(&conn, &[1], "after ROLLBACK");
        drop(conn);
        fixture.restart();
        variant.assert_intact(&fixture.open(), &[1], "after restart");
    }
}

#[test]
fn a_drop_after_a_write_to_another_table_is_rolled_back() {
    for variant in &VARIANTS {
        let mut fixture = Fixture::new("drop_after_other");
        let conn = fixture.open();
        conn.execute_batch(variant.create).unwrap();
        conn.execute_batch(OTHER).unwrap();
        conn.execute_batch(&variant.insert("t", 1)).unwrap();
        conn.execute_batch("BEGIN; INSERT INTO o VALUES (1, 1)")
            .unwrap();
        let _ = conn.execute_batch("DROP TABLE t");
        conn.execute_batch("ROLLBACK").unwrap();
        variant.assert_intact(&conn, &[1], "after ROLLBACK");
        assert!(ints(&conn, "SELECT v FROM o").is_empty());
        drop(conn);
        fixture.restart();
        variant.assert_intact(&fixture.open(), &[1], "after restart");
    }
}

#[test]
fn a_drop_inside_a_rolled_back_savepoint_is_undone() {
    for variant in &VARIANTS {
        let fixture = Fixture::new("drop_savepoint");
        let conn = fixture.open();
        conn.execute_batch(variant.create).unwrap();
        conn.execute_batch(&variant.insert("t", 1)).unwrap();
        conn.execute_batch("BEGIN").unwrap();
        conn.execute_batch(&variant.insert("t", 2)).unwrap();
        conn.execute_batch("SAVEPOINT s").unwrap();
        let _ = conn.execute_batch("DROP TABLE t");
        conn.execute_batch("ROLLBACK TO s").unwrap();
        variant.assert_intact(&conn, &[1, 2], "after ROLLBACK TO");
        conn.execute_batch("COMMIT").unwrap();
        variant.assert_intact(&conn, &[1, 2], "after COMMIT");
    }
}

/// SQLite stops calling a table's `xCommit`/`xRollback` once it is destroyed, so a DROP could
/// not take part in the transaction: it is refused, and the table is untouched.
#[test]
fn a_drop_inside_a_transaction_is_refused_and_changes_nothing() {
    for variant in &VARIANTS {
        let mut fixture = Fixture::new("drop_refused");
        let conn = fixture.open();
        conn.execute_batch(variant.create).unwrap();
        conn.execute_batch(OTHER).unwrap();
        conn.execute_batch(&variant.insert("t", 1)).unwrap();
        for setup in ["BEGIN", "BEGIN; INSERT INTO o VALUES (1, 1)", "SAVEPOINT s"] {
            conn.execute_batch(setup).unwrap();
            assert!(conn.execute_batch("DROP TABLE t").is_err());
            variant.assert_intact(&conn, &[1], "after the refused DROP");
            conn.execute_batch("COMMIT").unwrap();
            conn.execute_batch("DELETE FROM o").unwrap();
        }
        variant.assert_intact(&conn, &[1], "after COMMIT");
        drop(conn);
        fixture.restart();
        variant.assert_intact(&fixture.open(), &[1], "after restart");
    }
}

#[test]
fn a_drop_outside_a_transaction_still_works() {
    for variant in &VARIANTS {
        let fixture = Fixture::new("drop_autocommit");
        let conn = fixture.open();
        conn.execute_batch(variant.create).unwrap();
        conn.execute_batch(&variant.insert("t", 1)).unwrap();
        conn.execute_batch("DROP TABLE t").unwrap();
        conn.execute_batch(variant.create).unwrap();
        assert!(ints(&conn, "SELECT v FROM t").is_empty());
    }
}

#[test]
fn cross_connection_drop_after_rename_and_recreate_does_not_delete_new_table_catalog() {
    let mut fixture = Fixture::new("cross_conn_rename_drop");
    let conn1 = fixture.open();
    let conn2 = fixture.open();

    conn1
        .execute_batch("CREATE VIRTUAL TABLE t USING aydb(columns='k INTEGER PRIMARY KEY, v INT')")
        .unwrap();
    conn1
        .execute_batch("INSERT INTO t VALUES (1, 100)")
        .unwrap();
    let id1 = fixture
        .engine()
        .write()
        .unwrap()
        .register_or_resolve_vtab("t", None)
        .unwrap();

    // Hold a second live vtab handle under the original name `t` on conn2 before renaming.
    assert_eq!(ints(&conn2, "SELECT v FROM t"), [100]);

    conn1
        .execute_batch(
            "ALTER TABLE t RENAME TO t_old;
             CREATE VIRTUAL TABLE t USING aydb(columns='k INTEGER PRIMARY KEY, v INT');
             INSERT INTO t VALUES (2, 200);",
        )
        .unwrap();
    let id2 = fixture
        .engine()
        .write()
        .unwrap()
        .register_or_resolve_vtab("t", None)
        .unwrap();
    assert_ne!(id1, id2);

    conn1.execute_batch("DROP TABLE t_old").unwrap();
    drop(conn2);
    // Also exercise the direct `drop_table(id1, Some("t"))` stale-name guard against recreated `t`.
    fixture
        .engine()
        .write()
        .unwrap()
        .drop_table(id1, Some("t"))
        .unwrap();

    assert_eq!(ints(&conn1, "SELECT v FROM t"), [200]);
    drop(conn1);

    fixture.restart();
    let conn = fixture.open();
    assert_eq!(ints(&conn, "SELECT v FROM t"), [200]);
    assert_eq!(
        fixture
            .engine()
            .write()
            .unwrap()
            .register_or_resolve_vtab("t", None)
            .unwrap(),
        id2
    );
}

#[test]
fn failed_drop_under_read_lock_preserves_rows_and_catalog() {
    let cases = [
        ("integer_pk_unique", VARIANTS[0].create, true),
        ("text_pk_capped", VARIANTS[1].create, false),
        (
            "sync_integer_pk",
            "CREATE VIRTUAL TABLE t USING aydb(durability='sync', columns='k INTEGER PRIMARY KEY, u TEXT UNIQUE, v INT')",
            true,
        ),
    ];
    for (label, create_sql, int_key) in cases {
        let variant = Variant {
            name: label,
            create: create_sql,
            int_key,
        };
        let mut fixture = Fixture::new(&format!("drop_read_lock_{label}"));
        let conn1 = fixture.open();
        conn1
            .execute_batch("CREATE TABLE n(a INT); INSERT INTO n VALUES (1);")
            .unwrap();
        conn1.execute_batch(variant.create).unwrap();
        conn1.execute_batch(&variant.insert("t", 1)).unwrap();
        conn1.execute_batch(&variant.insert("t", 2)).unwrap();

        let conn2 = fixture.open();
        variant.assert_intact(&conn2, &[1, 2], "before locked DROP");

        // Hold a shared SQLite file read lock on `conn2` while `conn1` tries to DROP `t`.
        conn2.execute_batch("BEGIN; SELECT * FROM n;").unwrap();
        let err = conn1
            .execute_batch("DROP TABLE t")
            .expect_err("DROP TABLE must fail with database is locked")
            .to_string();
        assert!(
            err.to_ascii_lowercase().contains("locked"),
            "{label}: expected locked error, got: {err}"
        );
        conn2.execute_batch("COMMIT").unwrap();

        // After releasing the lock, SQLite still has `t` and every row/index must remain intact.
        variant.assert_intact(&conn1, &[1, 2], "conn1 after failed DROP");
        variant.assert_intact(&conn2, &[1, 2], "conn2 after failed DROP");

        drop(conn1);
        drop(conn2);
        fixture.restart();
        let conn = fixture.open();
        variant.assert_intact(&conn, &[1, 2], "after restart");

        // Unlocked DROP TABLE succeeds cleanly and recreates empty.
        conn.execute_batch("DROP TABLE t").unwrap();
        assert!(conn.query_all("SELECT v FROM t", &[]).is_err());
        conn.execute_batch(variant.create).unwrap();
        assert!(ints(&conn, "SELECT v FROM t").is_empty());
    }
}

#[test]
fn drop_table_wal_failure_rolls_back_sqlite_schema() {
    // (a) WAL append failure: both live reader and restarted engine retain `t` and all rows.
    {
        let mut fixture = Fixture::new("drop_wal_append_fail");
        let conn = fixture.open();
        conn.execute_batch(
            "CREATE VIRTUAL TABLE t USING aydb(durability='sync', columns='k INTEGER PRIMARY KEY, u TEXT UNIQUE, v INT');
             INSERT INTO t VALUES (1, 'u1', 10), (2, 'u2', 20);",
        )
        .unwrap();
        let reader = fixture.open();
        assert_eq!(ints(&reader, "SELECT v FROM t ORDER BY k"), [10, 20]);

        fixture
            .engine()
            .write()
            .unwrap()
            .debug_fail_next_wal_append();
        let err = conn
            .execute_batch("DROP TABLE t")
            .expect_err("DROP TABLE must report WAL append failure")
            .to_string();
        assert!(!err.is_empty());
        assert!(fixture.engine().read().unwrap().is_poisoned());
        assert_eq!(
            conn.query_all(
                "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name",
                &[]
            )
            .unwrap(),
            vec![vec![SqlValue::Text("t".into())]]
        );
        assert_eq!(ints(&reader, "SELECT v FROM t ORDER BY k"), [10, 20]);

        drop(reader);
        drop(conn);
        fixture.restart();
        let conn = fixture.open();
        assert_eq!(ints(&conn, "SELECT v FROM t ORDER BY k"), [10, 20]);
    }

    // (b) WAL sync failure: commit hook rejects commit so SQLite schema and live reader retain `t`.
    {
        let fixture = Fixture::new("drop_wal_sync_fail");
        let conn = fixture.open();
        conn.execute_batch(
            "CREATE VIRTUAL TABLE t USING aydb(durability='sync', columns='k INTEGER PRIMARY KEY, u TEXT UNIQUE, v INT');
             INSERT INTO t VALUES (1, 'u1', 10), (2, 'u2', 20);",
        )
        .unwrap();
        let reader = fixture.open();
        assert_eq!(ints(&reader, "SELECT v FROM t ORDER BY k"), [10, 20]);

        fixture.engine().read().unwrap().debug_fail_next_wal_sync();
        let err = conn
            .execute_batch("DROP TABLE t")
            .expect_err("DROP TABLE must report WAL sync failure")
            .to_string();
        assert!(
            err.contains("injected WAL sync failure"),
            "unexpected error: {err}"
        );
        assert!(fixture.engine().read().unwrap().is_poisoned());
        assert_eq!(
            conn.query_all(
                "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name",
                &[]
            )
            .unwrap(),
            vec![vec![SqlValue::Text("t".into())]]
        );
        assert_eq!(ints(&reader, "SELECT v FROM t ORDER BY k"), [10, 20]);
    }
}

#[test]
fn reused_table_id_invalidates_stale_connection_handles() {
    // Case 1: Two independent :memory: SQLite schemas sharing one StorageEngine with explicit table_id=7.
    {
        let tmp = crate::util::test_util::TempTestDir::new("reused_id_explicit");
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
        let engine = Arc::new(RwLock::new(
            StorageEngine::open(fs, crate::engine::EngineConfig::default()).unwrap(),
        ));

        let conn1 = SqliteConnection::open_in_memory().unwrap();
        let conn2 = SqliteConnection::open_in_memory().unwrap();
        for conn in [&conn1, &conn2] {
            conn.register_aydb_module_with_options(Arc::clone(&engine), false)
                .unwrap();
            conn.execute_batch(
                "CREATE VIRTUAL TABLE t USING aydb(table_id=7, columns='k INTEGER PRIMARY KEY, v TEXT')",
            )
            .unwrap();
        }
        conn1
            .execute_batch("INSERT INTO t VALUES (1, 'from_t')")
            .unwrap();
        assert_eq!(
            conn2.query_all("SELECT v FROM t WHERE k = 1", &[]).unwrap(),
            vec![vec![SqlValue::Text("from_t".to_string())]]
        );

        // Conn 1 drops `t` and recreates `u` reusing table_id=7.
        conn1
            .execute_batch(
                "DROP TABLE t;
                 CREATE VIRTUAL TABLE u USING aydb(table_id=7, columns='k INTEGER PRIMARY KEY, v TEXT');
                 INSERT INTO u VALUES (1, 'from_u');",
            )
            .unwrap();

        // Stale handle on Conn 2 must fail every operation without touching `u`.
        for (op, res) in [
            (
                "SELECT",
                conn2.query_all("SELECT * FROM t", &[]).map(|_| ()),
            ),
            (
                "INSERT",
                conn2.execute_batch("INSERT INTO t VALUES (2, 'bad')"),
            ),
            (
                "UPDATE",
                conn2.execute_batch("UPDATE t SET v = 'mutated' WHERE k = 1"),
            ),
            ("DELETE", conn2.execute_batch("DELETE FROM t WHERE k = 1")),
            ("RENAME", conn2.execute_batch("ALTER TABLE t RENAME TO z")),
        ] {
            let err = res
                .expect_err(&format!("{op} on stale handle must fail"))
                .to_string();
            assert!(err.contains("stale"), "{op}: unexpected error: {err}");
        }
        // SQLite omits `xDestroy` error messages, so `DROP` reports a generic error code.
        assert!(
            conn2.execute_batch("DROP TABLE t").is_err(),
            "DROP on stale handle must fail"
        );

        // `u` on Conn 1 is completely untouched.
        assert_eq!(
            conn1
                .query_all("SELECT k, v FROM u ORDER BY k", &[])
                .unwrap(),
            vec![vec![
                SqlValue::Integer(1),
                SqlValue::Text("from_u".to_string()),
            ]]
        );

        drop(conn2);
        assert_eq!(
            conn1
                .query_all("SELECT k, v FROM u ORDER BY k", &[])
                .unwrap(),
            vec![vec![
                SqlValue::Integer(1),
                SqlValue::Text("from_u".to_string()),
            ]]
        );
    }

    // Case 2: Auto-assigned table_id reuse + `activate_persisted_vtab` during an active tx + rollback.
    {
        let tmp = crate::util::test_util::TempTestDir::new("reused_id_auto_and_activate");
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
        let engine = Arc::new(RwLock::new(
            StorageEngine::open(fs, crate::engine::EngineConfig::default()).unwrap(),
        ));

        let conn1 = SqliteConnection::open_in_memory().unwrap();
        let conn2 = SqliteConnection::open_in_memory().unwrap();
        for conn in [&conn1, &conn2] {
            conn.register_aydb_module_with_options(Arc::clone(&engine), false)
                .unwrap();
        }
        conn1
            .execute_batch(
                "CREATE VIRTUAL TABLE other USING aydb(table_id=10, columns='k INTEGER PRIMARY KEY, v INT');
                 CREATE VIRTUAL TABLE t USING aydb(columns='k INTEGER PRIMARY KEY, v TEXT');
                 INSERT INTO t VALUES (1, 'original');",
            )
            .unwrap();

        // While conn1 holds an open transaction on `other`, conn2 connects to `t`
        // (taking the `activate_persisted_vtab` path), and then conn1 rolls back.
        conn1
            .execute_batch("BEGIN; INSERT INTO other VALUES (1, 1);")
            .unwrap();
        conn2
            .execute_batch(
                "CREATE VIRTUAL TABLE t USING aydb(columns='k INTEGER PRIMARY KEY, v TEXT')",
            )
            .unwrap();
        conn1.execute_batch("ROLLBACK").unwrap();
        assert_eq!(
            conn2.query_all("SELECT v FROM t WHERE k = 1", &[]).unwrap(),
            vec![vec![SqlValue::Text("original".to_string())]]
        );

        // Now conn1 drops `t` and auto-creates `u` (which reuses table_id=1).
        conn1
            .execute_batch(
                "DROP TABLE t;
                 CREATE VIRTUAL TABLE u USING aydb(columns='k INTEGER PRIMARY KEY, v TEXT');
                 INSERT INTO u VALUES (1, 'fresh_u');",
            )
            .unwrap();

        let err = conn2
            .execute_batch("UPDATE t SET v = 'hijacked' WHERE k = 1")
            .expect_err("stale auto-id handle must fail UPDATE")
            .to_string();
        assert!(err.contains("stale"), "unexpected error: {err}");
        let err = conn2
            .query_all("SELECT v FROM t", &[])
            .expect_err("stale auto-id handle must fail SELECT")
            .to_string();
        assert!(err.contains("stale"), "unexpected error: {err}");

        assert_eq!(
            conn1.query_all("SELECT v FROM u", &[]).unwrap(),
            vec![vec![SqlValue::Text("fresh_u".to_string())]]
        );
        drop(conn2);
        conn1
            .execute_batch("INSERT INTO u VALUES (2, 'still_alive')")
            .unwrap();
        assert_eq!(conn1.query_all("SELECT v FROM u", &[]).unwrap().len(), 2);
    }
}
