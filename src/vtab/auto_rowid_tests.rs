//! Automatic keys of an `INTEGER PRIMARY KEY` table: without AUTOINCREMENT a native table gives
//! the next insert `max(rowid) + 1` at the time of the insert. Failed statements, rolled back
//! transactions and savepoints, deletes and other connections must not change that.

use super::rename_tests::{Fixture, ints};
use super::*;

const COLUMNS: &str = "k INTEGER PRIMARY KEY, u TEXT UNIQUE, v INT NOT NULL";

fn setup(conn: &SqliteConnection) {
    conn.execute_batch(&format!(
        "CREATE VIRTUAL TABLE a USING aydb(columns='{COLUMNS}');
         CREATE TABLE n({COLUMNS});
         INSERT INTO a VALUES (1, 'a', 1), (2, 'b', 2);
         INSERT INTO n VALUES (1, 'a', 1), (2, 'b', 2);"
    ))
    .unwrap();
}

/// Runs `stmts` (`{t}` is the table) on both tables of `conn` and compares the keys that remain
/// with the keys that follow from one more automatic insert on each.
fn compare(conn: &SqliteConnection, stmts: &[&str], what: &str) {
    for t in ["n", "a"] {
        for stmt in stmts {
            let _ = conn.execute_batch(&stmt.replace("{t}", t));
        }
        // Whatever state the statements left (even an open transaction) the next insert works.
        conn.execute_batch(&format!("INSERT INTO {t} VALUES (NULL, 'next', 0)"))
            .unwrap();
    }
    let keys = |t: &str| ints(conn, &format!("SELECT k FROM {t} ORDER BY k"));
    assert_eq!(keys("a"), keys("n"), "{what}");
    for t in ["n", "a"] {
        conn.execute_batch(&format!("DELETE FROM {t} WHERE u = 'next'"))
            .unwrap();
    }
}

#[test]
fn failed_and_rolled_back_inserts_do_not_use_up_automatic_keys() {
    let fixture = Fixture::new("auto_rowid");
    let conn = fixture.open();
    setup(&conn);
    compare(
        &conn,
        &["INSERT INTO {t} VALUES (NULL, 'a', 3)"],
        "unique violation",
    );
    compare(
        &conn,
        &["INSERT INTO {t} VALUES (NULL, 'z', NULL)"],
        "not null violation",
    );
    for mode in ["", "OR ABORT", "OR FAIL", "OR IGNORE"] {
        let stmt = format!(
            "INSERT {mode} INTO {{t}} VALUES (NULL, 'c', 1), (NULL, 'a', 1), (NULL, 'd', 1)"
        );
        compare(&conn, &[&stmt], &format!("multi-row {mode}"));
        for t in ["n", "a"] {
            conn.execute_batch(&format!("DELETE FROM {t} WHERE u IN ('c', 'd')"))
                .unwrap();
        }
    }
    compare(
        &conn,
        &[
            "BEGIN",
            "INSERT INTO {t} VALUES (NULL, 'x', 1)",
            "INSERT INTO {t} VALUES (NULL, 'y', 1)",
            "ROLLBACK",
        ],
        "rolled back transaction",
    );
    compare(
        &conn,
        &[
            "BEGIN",
            "INSERT INTO {t} VALUES (NULL, 'x', 1)",
            "SAVEPOINT s",
            "INSERT INTO {t} VALUES (NULL, 'y', 1)",
            "INSERT INTO {t} VALUES (NULL, 'z', 1)",
            "ROLLBACK TO s",
            "COMMIT",
        ],
        "rolled back savepoint",
    );
    for t in ["n", "a"] {
        conn.execute_batch(&format!("DELETE FROM {t} WHERE u = 'x'"))
            .unwrap();
    }
    // The highest keys go away: native reuses them.
    compare(
        &conn,
        &[
            "INSERT INTO {t} VALUES (NULL, 'top', 1)",
            "DELETE FROM {t} WHERE u = 'top'",
        ],
        "delete of the highest key",
    );
    compare(
        &conn,
        &["DELETE FROM {t} WHERE k = 2"],
        "delete of an old highest key",
    );
}

#[test]
fn explicit_keys_and_updates_move_the_next_automatic_key_like_native() {
    let fixture = Fixture::new("auto_rowid_explicit");
    let conn = fixture.open();
    setup(&conn);
    compare(
        &conn,
        &["INSERT INTO {t} VALUES (100, 'big', 1)"],
        "explicit high key",
    );
    compare(
        &conn,
        &["UPDATE {t} SET k = 7 WHERE k = 100"],
        "key moved down",
    );
    // A failed explicit insert must not raise the next key either.
    compare(
        &conn,
        &["INSERT INTO {t} VALUES (500, 'a', 1)"],
        "failed explicit high key",
    );
}

#[test]
fn two_connections_and_a_reconnect_hand_out_the_same_keys_as_native() {
    let fixture = Fixture::new("auto_rowid_two_conns");
    let first = fixture.open();
    setup(&first);
    let second = fixture.open();
    ints(&second, "SELECT k FROM a");
    for (i, conn) in [&first, &second, &first, &second, &second, &first]
        .iter()
        .enumerate()
    {
        let u = format!("u{i}");
        // The same statement on a failing and a succeeding variant.
        let _ = conn.execute_batch("INSERT INTO a VALUES (NULL, 'a', 1)");
        conn.execute_batch(&format!("INSERT INTO a VALUES (NULL, '{u}', 1)"))
            .unwrap();
        first
            .execute_batch(&format!("INSERT INTO n VALUES (NULL, '{u}', 1)"))
            .unwrap();
    }
    let keys = |t: &str| ints(&first, &format!("SELECT k FROM {t} ORDER BY k"));
    assert_eq!(keys("a"), keys("n"));
    drop(second);
    drop(first);
    let again = fixture.open();
    again
        .execute_batch(
            "INSERT INTO a VALUES (NULL, 'after', 1); INSERT INTO n VALUES (NULL, 'after', 1)",
        )
        .unwrap();
    let keys = |t: &str| ints(&again, &format!("SELECT k FROM {t} ORDER BY k"));
    assert_eq!(keys("a"), keys("n"));
}

/// The key remembered by one connection is only good while nothing else wrote: another
/// connection that deletes the key it left behind, or inserts above it, must be noticed.
#[test]
fn a_remembered_key_is_dropped_when_another_connection_writes() {
    let fixture = Fixture::new("auto_rowid_stale_cache");
    let first = fixture.open();
    setup(&first);
    let second = fixture.open();
    ints(&second, "SELECT k FROM a");
    // (connection, statement); the same statements run on the native table through `first`.
    let steps: [(usize, &str); 7] = [
        (0, "INSERT INTO {t} VALUES (NULL, 'f1', 1)"),
        (1, "DELETE FROM {t} WHERE u = 'f1'"),
        (0, "INSERT INTO {t} VALUES (NULL, 'f2', 1)"),
        (1, "INSERT INTO {t} VALUES (10, 's1', 1)"),
        (0, "INSERT INTO {t} VALUES (NULL, 'f3', 1)"),
        (1, "INSERT INTO {t} VALUES (NULL, 's2', 1)"),
        (0, "INSERT INTO {t} VALUES (NULL, 'f4', 1)"),
    ];
    for (who, stmt) in steps {
        let conn = [&first, &second][who];
        conn.execute_batch(&stmt.replace("{t}", "a")).unwrap();
        first.execute_batch(&stmt.replace("{t}", "n")).unwrap();
        let keys = |t: &str| ints(&first, &format!("SELECT k FROM {t} ORDER BY k"));
        assert_eq!(keys("a"), keys("n"), "{stmt}");
    }
}

/// Automatic inserts in one transaction (the cached path) give the keys a native table gives,
/// also when a failing insert, a delete and a rollback to a savepoint sit between them.
#[test]
fn cached_automatic_keys_match_native_inside_a_transaction() {
    let fixture = Fixture::new("auto_rowid_cached_tx");
    let conn = fixture.open();
    setup(&conn);
    let script = [
        "BEGIN",
        "INSERT INTO {t} VALUES (NULL, 'p1', 1)",
        "INSERT INTO {t} VALUES (NULL, 'p2', 1)",
        "INSERT INTO {t} VALUES (NULL, 'p1', 1)",
        "INSERT INTO {t} VALUES (NULL, 'p3', 1)",
        "DELETE FROM {t} WHERE u = 'p3'",
        "INSERT INTO {t} VALUES (NULL, 'p4', 1)",
        "SAVEPOINT s",
        "INSERT INTO {t} VALUES (NULL, 'p5', 1)",
        "INSERT INTO {t} VALUES (NULL, 'p6', 1)",
        "ROLLBACK TO s",
        "INSERT INTO {t} VALUES (NULL, 'p7', 1)",
        "UPDATE {t} SET k = 50 WHERE u = 'p7'",
        "INSERT INTO {t} VALUES (NULL, 'p8', 1)",
        "UPDATE {t} SET k = 3 WHERE u = 'p8'",
        "INSERT INTO {t} VALUES (NULL, 'p9', 1)",
        "COMMIT",
        "INSERT INTO {t} VALUES (NULL, 'p10', 1)",
    ];
    for t in ["n", "a"] {
        for stmt in script {
            let _ = conn.execute_batch(&stmt.replace("{t}", t));
        }
    }
    let rows = |t: &str| {
        conn.query_all(&format!("SELECT k, u FROM {t} ORDER BY k"), &[])
            .unwrap()
    };
    assert_eq!(rows("a"), rows("n"));
}

#[test]
fn float_keys_above_integers_do_not_reset_automatic_rowids() {
    let fixture = Fixture::new("auto_rowid_float_keys");
    let conn = fixture.open();
    conn.execute_batch(
        "CREATE VIRTUAL TABLE a USING aydb(columns='k INTEGER PRIMARY KEY, v TEXT');
         INSERT INTO a VALUES (1, 'one'), (10, 'ten'), (10.5, 'half'), (10.75, 'three_quarters'), (100.25, 'big_float');",
    )
    .unwrap();

    // Gaps (2..9) must not be reused just because float keys sit above integer 10.
    conn.execute_batch("INSERT INTO a VALUES (NULL, 'auto1'), (NULL, 'auto2')")
        .unwrap();
    assert_eq!(
        conn.query_all(
            "SELECT k FROM a WHERE v IN ('auto1', 'auto2') ORDER BY k",
            &[]
        )
        .unwrap(),
        vec![vec![SqlValue::Integer(11)], vec![SqlValue::Integer(12)],]
    );

    // Clear auto_key_cache via an explicit float batch spanning >64 distinct floors above 12.
    conn.execute_batch("BEGIN").unwrap();
    for i in 20..120i64 {
        conn.execute_batch(&format!("INSERT INTO a VALUES ({i}.5, 'f{i}')"))
            .unwrap();
    }
    conn.execute_batch("COMMIT").unwrap();
    conn.execute_batch("INSERT INTO a VALUES (NULL, 'after_many_floats')")
        .unwrap();
    assert_eq!(
        conn.query_all("SELECT k FROM a WHERE v = 'after_many_floats'", &[])
            .unwrap(),
        vec![vec![SqlValue::Integer(13)]]
    );

    // Deleting the highest integer while floats remain above still picks the new max integer + 1.
    conn.execute_batch(
        "DELETE FROM a WHERE k = 13;
         INSERT INTO a VALUES (NULL, 'reuse_top');",
    )
    .unwrap();
    assert_eq!(
        conn.query_all("SELECT k FROM a WHERE v = 'reuse_top'", &[])
            .unwrap(),
        vec![vec![SqlValue::Integer(13)]]
    );

    // A table containing only float keys starts automatic integer keys at 1.
    conn.execute_batch(
        "CREATE VIRTUAL TABLE b USING aydb(columns='k INTEGER PRIMARY KEY, v TEXT');
         INSERT INTO b VALUES (5.5, 'f1'), (10.5, 'f2');
         INSERT INTO b VALUES (NULL, 'first_int');",
    )
    .unwrap();
    assert_eq!(
        conn.query_all("SELECT k FROM b WHERE v = 'first_int'", &[])
            .unwrap(),
        vec![vec![SqlValue::Integer(1)]]
    );
}

#[test]
fn integer_pk_with_mixed_floats_text_and_blob_keys_allocates_monotonic_rowids() {
    let mut fixture = Fixture::new("auto_rowid_mixed_types");
    let conn = fixture.open();
    conn.execute_batch(
        "CREATE VIRTUAL TABLE a USING aydb(columns='k INTEGER PRIMARY KEY, v TEXT');
         INSERT INTO a VALUES (1, 'i1'), (5, 'i5'),
                              (-1e30, 'neg_huge'), (-2.5, 'neg_f'),
                              (5.25, 'f1'), (5.75, 'f2'), (1e30, 'pos_huge'),
                              ('alpha', 't1'), (X'0102', 'b1');
         INSERT INTO a(rowid, k, v) VALUES (6, 'beta', 't_rowid6');
         INSERT INTO a VALUES (NULL, 'auto1'), (NULL, 'auto2');",
    )
    .unwrap();

    assert_eq!(
        conn.query_all(
            "SELECT k FROM a WHERE v IN ('auto1', 'auto2') ORDER BY v",
            &[]
        )
        .unwrap(),
        vec![vec![SqlValue::Integer(7)], vec![SqlValue::Integer(8)]]
    );
    let counts = conn
        .query_all("SELECT count(*), count(DISTINCT rowid) FROM a", &[])
        .unwrap();
    assert_eq!(counts[0][0], counts[0][1]);
    assert_eq!(counts[0][0], SqlValue::Integer(12));

    drop(conn);
    fixture.restart();
    let conn = fixture.open();
    conn.execute_batch("INSERT INTO a VALUES (NULL, 'auto3')")
        .unwrap();
    assert_eq!(
        conn.query_all("SELECT k FROM a WHERE v = 'auto3'", &[])
            .unwrap(),
        vec![vec![SqlValue::Integer(9)]]
    );
    let counts_after = conn
        .query_all("SELECT count(*), count(DISTINCT rowid) FROM a", &[])
        .unwrap();
    assert_eq!(counts_after[0][0], counts_after[0][1]);
    assert_eq!(counts_after[0][0], SqlValue::Integer(13));

    assert_eq!(
        conn.query_all(
            "SELECT k, v FROM a WHERE v IN ('neg_huge', 'f1', 'pos_huge', 't1', 'b1', 't_rowid6', 'auto1', 'auto2', 'auto3') ORDER BY v",
            &[]
        )
        .unwrap(),
        vec![
            vec![SqlValue::Integer(7), SqlValue::Text("auto1".into())],
            vec![SqlValue::Integer(8), SqlValue::Text("auto2".into())],
            vec![SqlValue::Integer(9), SqlValue::Text("auto3".into())],
            vec![SqlValue::Blob(vec![1, 2]), SqlValue::Text("b1".into())],
            vec![SqlValue::Float(5.25), SqlValue::Text("f1".into())],
            vec![SqlValue::Float(-1e30), SqlValue::Text("neg_huge".into())],
            vec![SqlValue::Float(1e30), SqlValue::Text("pos_huge".into())],
            vec![SqlValue::Text("alpha".into()), SqlValue::Text("t1".into())],
            vec![SqlValue::Text("beta".into()), SqlValue::Text("t_rowid6".into())],
        ]
    );
}
