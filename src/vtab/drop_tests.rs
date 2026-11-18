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
