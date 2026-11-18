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
