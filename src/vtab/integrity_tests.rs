//! `PRAGMA integrity_check` / `quick_check` on a schema with aydb tables, and what SQLite still
//! knows about their declaration.

use super::rename_tests::Fixture;
use super::*;

const SPECS: [&str; 7] = [
    "k TEXT PRIMARY KEY, v INT",
    "k INTEGER PRIMARY KEY, v INT",
    "k TEXT PRIMARY KEY, u TEXT UNIQUE, v INT",
    "k INTEGER PRIMARY KEY, u TEXT UNIQUE NOT NULL, v INT COLLATE NOCASE",
    "\"a b\" TEXT PRIMARY KEY, `u--x` INT UNIQUE",
    "k TEXT PRIMARY KEY, v INT",
    "k TEXT, v INT",
];

fn texts(conn: &SqliteConnection, sql: &str) -> Vec<String> {
    conn.query_all(sql, &[])
        .unwrap()
        .iter()
        .map(|r| match &r[0] {
            SqlValue::Text(s) => s.clone(),
            other => format!("{other:?}"),
        })
        .collect()
}

fn assert_ok(conn: &SqliteConnection, what: &str) {
    for pragma in ["PRAGMA integrity_check", "PRAGMA quick_check"] {
        assert_eq!(texts(conn, pragma), ["ok"], "{pragma} {what}");
    }
}

#[test]
fn integrity_check_is_clean_with_aydb_tables() {
    let fixture = Fixture::new("integrity");
    {
        let conn = fixture.open();
        conn.execute_batch("CREATE TABLE native(a INTEGER PRIMARY KEY, b TEXT UNIQUE)")
            .unwrap();
        conn.execute_batch("INSERT INTO native VALUES (1, 'x')")
            .unwrap();
        assert_ok(&conn, "before any aydb table");
        for (i, spec) in SPECS.iter().enumerate() {
            let options = if i == 5 { "max_bytes=100000, " } else { "" };
            conn.execute_batch(&format!(
                "CREATE VIRTUAL TABLE t{i} USING aydb({options}columns='{spec}')"
            ))
            .unwrap();
            assert_ok(&conn, &format!("right after creating [{spec}]"));
        }
        for i in 0..SPECS.len() {
            conn.query_all(&format!("SELECT count(*) FROM t{i}"), &[])
                .unwrap();
        }
        assert_ok(&conn, "after reading every table");
    }
    // A fresh connection connects the tables lazily.
    let conn = fixture.open();
    assert_ok(&conn, "on a fresh connection, tables not yet connected");
    for i in 0..SPECS.len() {
        conn.query_all(&format!("SELECT count(*) FROM t{i}"), &[])
            .unwrap();
    }
    assert_ok(&conn, "on a fresh connection, tables connected");
}

/// What the declaration given to SQLite still provides to queries.
#[test]
fn the_declared_columns_still_behave_like_a_native_table() {
    let fixture = Fixture::new("integrity_declared");
    let conn = fixture.open();
    let spec = "k TEXT PRIMARY KEY, u TEXT UNIQUE NOT NULL, v INT COLLATE NOCASE, w VARCHAR(10), x";
    conn.execute_batch(&format!(
        "CREATE VIRTUAL TABLE a USING aydb(columns='{spec}'); CREATE TABLE n({spec})"
    ))
    .unwrap();
    // The stored definition is what the user wrote.
    assert_eq!(
        texts(&conn, "SELECT sql FROM sqlite_master WHERE name = 'a'"),
        [format!(
            "CREATE VIRTUAL TABLE a USING aydb(columns='{spec}')"
        )]
    );
    // Column names and declared types.
    let info = |t: &str| {
        conn.query_all(
            &format!("SELECT name, type FROM pragma_table_info('{t}')"),
            &[],
        )
        .unwrap()
    };
    assert_eq!(info("a"), info("n"));
    // Declared type decides affinity; the collation of a column applies in comparisons.
    for stmt in [
        "INSERT INTO {t} VALUES ('p', 'q', 'Abc', 5, 5)",
        "INSERT INTO {t} VALUES ('r', 's', 'abc2', '7', '7')",
    ] {
        for t in ["a", "n"] {
            conn.execute_batch(&stmt.replace("{t}", t)).unwrap();
        }
    }
    for query in [
        "SELECT typeof(v), typeof(w), typeof(x) FROM {t} ORDER BY k",
        "SELECT k FROM {t} WHERE v = 'ABC'",
        "SELECT k FROM {t} ORDER BY v",
        "SELECT k FROM {t} ORDER BY v COLLATE BINARY, k",
        "SELECT count(*) FROM {t} WHERE v < 'abd'",
    ] {
        let run = |t: &str| conn.query_all(&query.replace("{t}", t), &[]).unwrap();
        assert_eq!(run("a"), run("n"), "{query}");
    }
}

/// The one visible change: a key that is not a rowid alias, and UNIQUE, are no longer declared
/// to SQLite, so `table_info.pk` is 0 for them. `INTEGER PRIMARY KEY` is still declared.
#[test]
fn table_info_pk_only_for_the_rowid_alias() {
    let fixture = Fixture::new("integrity_pk");
    let conn = fixture.open();
    conn.execute_batch(
        "CREATE VIRTUAL TABLE a USING aydb(columns='k INTEGER PRIMARY KEY, v INT');
         CREATE VIRTUAL TABLE b USING aydb(columns='k TEXT PRIMARY KEY, v INT')",
    )
    .unwrap();
    let pk = |t: &str| {
        conn.query_all(
            &format!("SELECT pk FROM pragma_table_info('{t}') ORDER BY cid"),
            &[],
        )
        .unwrap()
    };
    assert_eq!(pk("a"), [[SqlValue::Integer(1)], [SqlValue::Integer(0)]]);
    assert_eq!(pk("b"), [[SqlValue::Integer(0)], [SqlValue::Integer(0)]]);
}
