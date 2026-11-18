//! Column definitions of the `columns=` option, compared with a native table declared with the
//! same column list in the same connection: affinity of the stored values, constraint behaviour
//! and quoted identifiers.

use super::*;
use crate::engine::EngineConfig;
use crate::util::test_util::TempTestDir;

struct Pair {
    conn: SqliteConnection,
    /// Declared last so that it is removed after the engine has closed its files.
    _dir: TempTestDir,
}

/// One connection holding aydb table `a` and native table `n` with the same `spec`.
fn pair(spec: &str, options: &str) -> Pair {
    let dir = TempTestDir::new("column_def");
    let fs = Arc::new(OsFs::open(&dir.path, true).unwrap());
    let mut config = EngineConfig::default();
    config.sync_wal_size_bytes = 64 * 4096;
    config.buf_wal_size_bytes = 64 * 4096;
    let engine = Arc::new(RwLock::new(StorageEngine::open(fs, config).unwrap()));
    let conn = SqliteConnection::open_in_memory().unwrap();
    conn.register_aydb_module_with_options(engine, false)
        .unwrap();
    conn.execute_batch(&format!(
        "CREATE VIRTUAL TABLE a USING aydb(table_id=1, {options}columns='{}')",
        spec.replace('\'', "''")
    ))
    .unwrap_or_else(|e| panic!("aydb rejected [{spec}]: {e}"));
    conn.execute_batch(&format!("CREATE TABLE n({spec})"))
        .unwrap();
    Pair { conn, _dir: dir }
}

/// Runs `stmts` (with `{t}` replaced by the table) on both tables and requires the same outcome
/// per statement and the same result of `query`.
fn check(spec: &str, options: &str, stmts: &[&str], query: &str) {
    let p = pair(spec, options);
    let mut outcomes: Vec<Vec<String>> = Vec::new();
    for table in ["n", "a"] {
        let mut out = Vec::new();
        for stmt in stmts {
            let sql = stmt.replace("{t}", table);
            let ok = p.conn.execute_batch(&sql).is_ok();
            out.push(format!("{stmt} -> {}", if ok { "ok" } else { "error" }));
        }
        let rows = p.conn.query_all(&query.replace("{t}", table), &[]);
        out.push(format!("{:?}", rows.map_err(|e| e.to_string())));
        outcomes.push(out);
    }
    assert_eq!(
        outcomes[1], outcomes[0],
        "aydb (left) differs from native (right) for [{spec}]"
    );
}

const AFFINITY_STMTS: [&str; 3] = [
    "INSERT INTO {t} VALUES('a', '1')",
    "INSERT INTO {t} VALUES('b', '01')",
    "INSERT INTO {t} VALUES('c', 2.50)",
];

#[test]
fn text_column_keeps_text_affinity_with_constraint_clauses() {
    for spec in [
        "k TEXT PRIMARY KEY, v TEXT CONSTRAINT nn NOT NULL",
        "k TEXT PRIMARY KEY, v TEXT NOT NULL CONSTRAINT u UNIQUE",
        "k TEXT PRIMARY KEY, v TEXT REFERENCES pointers(id)",
        "k TEXT PRIMARY KEY, v TEXT REFERENCES pointers (id) ON DELETE CASCADE",
        "k TEXT PRIMARY KEY, v TEXT NOT NULL REFERENCES t2(id) ON UPDATE SET NULL NOT DEFERRABLE",
        "k TEXT PRIMARY KEY, v TEXT CHECK(v<>'zzz')",
        "k TEXT PRIMARY KEY, v TEXT CHECK (v<>'zzz') COLLATE NOCASE",
        "k TEXT PRIMARY KEY, v TEXT CONSTRAINT c CHECK(length(v) < 10) NOT NULL",
        "k TEXT PRIMARY KEY, v TEXT NULL",
    ] {
        check(
            spec,
            "",
            &AFFINITY_STMTS,
            "SELECT k, v, typeof(v) FROM {t} ORDER BY k",
        );
    }
}

#[test]
fn named_primary_key_and_unique_constraints_are_recognised() {
    // 'k' stays text, and the named constraints still reject duplicates.
    for spec in [
        "k TEXT CONSTRAINT pk PRIMARY KEY, v INT",
        "k TEXT PRIMARY KEY CONSTRAINT nn NOT NULL, v INT",
        "k TEXT PRIMARY KEY, v INT CONSTRAINT u UNIQUE",
    ] {
        check(
            spec,
            "",
            &[
                "INSERT INTO {t} VALUES('1', 1)",
                "INSERT INTO {t} VALUES('01', 2)",
                "INSERT INTO {t} VALUES('1', 3)",
                "INSERT INTO {t} VALUES('x', 1)",
                "INSERT INTO {t} VALUES('y', 4)",
            ],
            "SELECT k, v, typeof(k), typeof(v) FROM {t} ORDER BY v, k",
        );
    }
}

#[test]
fn text_unique_column_with_constraint_name_has_no_false_conflict() {
    // With INT affinity '1' and '01' would both become 1 and conflict.
    check(
        "k INTEGER PRIMARY KEY, b TEXT CONSTRAINT u UNIQUE",
        "",
        &[
            "INSERT INTO {t} VALUES(1, '1')",
            "INSERT INTO {t} VALUES(2, '01')",
            "INSERT INTO {t} VALUES(3, '1')",
        ],
        "SELECT k, b, typeof(b) FROM {t} ORDER BY k",
    );
}

#[test]
fn type_names_with_several_words_and_sizes_give_sqlite_affinity() {
    for spec in [
        "k TEXT PRIMARY KEY, v VARCHAR(10) NOT NULL",
        "k TEXT PRIMARY KEY, v DECIMAL(10, 2)",
        "k TEXT PRIMARY KEY, v DOUBLE PRECISION",
        "k TEXT PRIMARY KEY, v UNSIGNED BIG INT",
        "k TEXT PRIMARY KEY, v VARYING CHARACTER (20) UNIQUE",
        "k TEXT PRIMARY KEY, v \"TEXT\"",
        "k TEXT PRIMARY KEY, v POINT",
        "k TEXT PRIMARY KEY, v",
    ] {
        check(
            spec,
            "",
            &AFFINITY_STMTS,
            "SELECT k, v, typeof(v) FROM {t} ORDER BY k",
        );
    }
}

#[test]
fn quoted_column_names_work_end_to_end() {
    for spec in [
        "\"a--b\" TEXT PRIMARY KEY, \"v/*x\" INT UNIQUE",
        "`a--b` TEXT PRIMARY KEY, [v/*x] INT UNIQUE",
        "\"primary\" TEXT PRIMARY KEY, \"unique\" INT UNIQUE",
        "\"k\" INTEGER PRIMARY KEY, v TEXT",
        "'k' TEXT PRIMARY KEY, \"v w\" INT",
        "\"a\"\"b\" TEXT PRIMARY KEY, \"not\" TEXT NOT NULL",
    ] {
        let cols: Vec<String> = {
            let p = pair(spec, "");
            p.conn
                .query_all("SELECT name FROM pragma_table_info('n')", &[])
                .unwrap()
                .iter()
                .map(|r| match &r[0] {
                    SqlValue::Text(s) => format!("\"{}\"", s.replace('"', "\"\"")),
                    other => panic!("{other:?}"),
                })
                .collect()
        };
        let (c0, c1) = (&cols[0], &cols[1]);
        let int_pk = spec.contains("INTEGER PRIMARY KEY");
        let k = |n: i32| {
            if int_pk {
                n.to_string()
            } else {
                format!("'{n}'")
            }
        };
        let v = |n: i32| {
            if spec.contains("\"not\"") || spec.contains("v TEXT") {
                format!("'{n}'")
            } else {
                n.to_string()
            }
        };
        let insert = |a: i32, b: i32| format!("INSERT INTO {{t}} VALUES({}, {})", k(a), v(b));
        let stmts = [
            insert(1, 10),
            insert(2, 20),
            insert(1, 30),
            insert(3, 20),
            format!("UPDATE {{t}} SET {c1} = {} WHERE {c0} = {}", v(40), k(2)),
            format!("DELETE FROM {{t}} WHERE {c0} = {}", k(1)),
        ];
        let refs: Vec<&str> = stmts.iter().map(String::as_str).collect();
        let query = format!(
            "SELECT {c0}, {c1}, typeof({c0}), typeof({c1}) FROM {{t}} WHERE {c0} <> {} ORDER BY {c0}",
            k(99)
        );
        check(spec, "", &refs, &query);
    }
}

#[test]
fn quoted_column_names_can_be_indexed() {
    let spec = "\"a--b\" TEXT PRIMARY KEY, \"v w\" TEXT, `x` INT";
    for indexes in [
        "indexes='\"v w\"', ",
        "indexes='\"v w\", x', ",
        "indexes='[v w]', ",
    ] {
        check(
            spec,
            indexes,
            &[
                "INSERT INTO {t} VALUES('1', 'p', 1)",
                "INSERT INTO {t} VALUES('2', 'q', 2)",
                "INSERT INTO {t} VALUES('3', 'p', 3)",
            ],
            "SELECT \"a--b\", x FROM {t} WHERE \"v w\" = 'p' ORDER BY x",
        );
    }
}

#[test]
fn columns_option_keeps_quotes_of_the_first_and_last_identifier() {
    check(
        "\"k\" INTEGER PRIMARY KEY, \"v\"",
        "",
        &[
            "INSERT INTO {t} VALUES(1, 'x')",
            "INSERT INTO {t} VALUES(1, 'y')",
        ],
        "SELECT k, v FROM {t}",
    );
}
