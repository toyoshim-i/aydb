//! Writes to the `rowid` column (explicit rowid on INSERT, `UPDATE SET rowid`) on an aydb
//! table, compared with a native table. Separate connections: `OR ROLLBACK` ends the
//! transaction.

use super::*;
use crate::engine::EngineConfig;
use crate::util::test_util::TempTestDir;

const MODES: [&str; 6] = [
    "",
    "OR ABORT",
    "OR FAIL",
    "OR IGNORE",
    "OR REPLACE",
    "OR ROLLBACK",
];

const SENTINEL: i64 = 7_000_000;

struct Spec {
    name: &'static str,
    columns: &'static str,
    options: &'static str,
    /// The key is an integer: `k` is written as a number.
    int_key: bool,
    /// `INTEGER PRIMARY KEY`: `rowid` and `k` are the same column.
    alias: bool,
}

const SPECS: [Spec; 4] = [
    Spec {
        name: "alias",
        columns: "k INTEGER PRIMARY KEY, v TEXT",
        options: "",
        int_key: true,
        alias: true,
    },
    Spec {
        name: "int_key",
        columns: "k INT PRIMARY KEY NOT NULL, v TEXT",
        options: "",
        int_key: true,
        alias: false,
    },
    Spec {
        name: "text_key",
        columns: "k TEXT PRIMARY KEY NOT NULL, v TEXT",
        options: "",
        int_key: false,
        alias: false,
    },
    Spec {
        name: "capped",
        columns: "k TEXT PRIMARY KEY NOT NULL, v TEXT",
        options: "max_bytes=100000, ",
        int_key: false,
        alias: false,
    },
];

fn key(spec: &Spec, n: i64) -> String {
    if spec.int_key {
        n.to_string()
    } else {
        format!("'k{n}'")
    }
}

struct Side {
    conn: SqliteConnection,
    table: &'static str,
}

impl Side {
    fn int(&self, sql: &str) -> i64 {
        match self.conn.query_all(sql, &[]).unwrap()[0][0] {
            SqlValue::Integer(v) => v,
            ref other => panic!("unexpected {other:?}"),
        }
    }

    fn rows(&self) -> String {
        format!(
            "{:?}",
            self.conn
                .query_all(
                    &format!("SELECT rowid, k, v FROM {} ORDER BY v", self.table),
                    &[]
                )
                .unwrap()
        )
    }

    fn observe(&self, ok: bool) -> String {
        let in_tx = if self.conn.execute_batch("BEGIN").is_ok() {
            self.conn.execute_batch("ROLLBACK").unwrap();
            false
        } else {
            true
        };
        format!(
            "ok={ok} changes={} last_rowid={} in_tx={in_tx} rows={}",
            self.int("SELECT changes()"),
            self.int("SELECT last_insert_rowid()"),
            self.rows(),
        )
    }
}

struct Lab {
    spec: &'static Spec,
    engine: Arc<RwLock<StorageEngine<OsFs>>>,
    native: Side,
    aydb: Side,
    _dir: TempTestDir,
}

impl Lab {
    fn new(spec: &'static Spec) -> Self {
        let dir = TempTestDir::new("rowid_write");
        let fs = Arc::new(OsFs::open(&dir.path, true).unwrap());
        let mut config = EngineConfig::default();
        config.sync_wal_size_bytes = 64 * 4096;
        config.buf_wal_size_bytes = 64 * 4096;
        let engine = Arc::new(RwLock::new(StorageEngine::open(fs, config).unwrap()));
        let aydb = SqliteConnection::open_in_memory().unwrap();
        aydb.register_aydb_module_with_options(engine.clone(), false)
            .unwrap();
        aydb.execute_batch(&format!(
            "CREATE VIRTUAL TABLE a USING aydb(table_id=1, {}columns='{}')",
            spec.options, spec.columns
        ))
        .unwrap();
        let native = SqliteConnection::open_in_memory().unwrap();
        native
            .execute_batch(&format!("CREATE TABLE n({})", spec.columns))
            .unwrap();
        Self {
            spec,
            engine,
            native: Side {
                conn: native,
                table: "n",
            },
            aydb: Side {
                conn: aydb,
                table: "a",
            },
            _dir: dir,
        }
    }

    /// Two rows with explicit rowids 10 and 20, then the sentinel `last_insert_rowid()`.
    fn seed(&self) {
        for side in [&self.native, &self.aydb] {
            side.conn
                .execute_batch("CREATE TABLE scratch(id INTEGER PRIMARY KEY)")
                .unwrap();
            side.conn.execute_batch("BEGIN").unwrap();
            for (rowid, v) in [(10, "a"), (20, "b")] {
                let k = key(self.spec, rowid);
                side.conn
                    .execute_batch(&format!(
                        "INSERT INTO {}(rowid, k, v) VALUES({rowid}, {k}, '{v}')",
                        side.table
                    ))
                    .unwrap();
            }
            side.conn
                .execute_batch(&format!("INSERT INTO scratch VALUES({SENTINEL})"))
                .unwrap();
        }
    }

    /// Runs `stmt` (`{t}` is the table) on both sides; returns (native, aydb) observations.
    fn run(&self, stmt: &str) -> (String, String) {
        let mut out = Vec::new();
        for side in [&self.native, &self.aydb] {
            let ok = side
                .conn
                .execute_batch(&stmt.replace("{t}", side.table))
                .is_ok();
            out.push(side.observe(ok));
        }
        let aydb = out.pop().unwrap();
        (out.pop().unwrap(), aydb)
    }
}

/// Statements that write a rowid, for key literals `KEY30`, `KEY32` and `KEY62`.
fn statements(spec: &Spec, with_null_rowid: bool) -> Vec<String> {
    let mut stmts = Vec::new();
    for mode in MODES {
        for body in [
            "INSERT {m} INTO {t}(rowid, k, v) VALUES(30, KEY30, 'x')",
            "INSERT {m} INTO {t}(rowid, k, v) VALUES(10, KEY30, 'x')",
            "INSERT {m} INTO {t}(rowid, k, v) VALUES(31, KEY32, 'x')",
            "INSERT {m} INTO {t}(rowid, k, v) VALUES(30, KEY10, 'x')",
            "UPDATE {m} {t} SET rowid = 60 WHERE v = 'a'",
            "UPDATE {m} {t} SET rowid = 20 WHERE v = 'a'",
            "UPDATE {m} {t} SET rowid = 20, v = 'z' WHERE v = 'a'",
            "UPDATE {m} {t} SET rowid = 61, k = KEY62 WHERE v = 'a'",
            "UPDATE {m} {t} SET rowid = 10 WHERE v = 'a'",
            "UPDATE {m} {t} SET rowid = 70 WHERE v = 'zz'",
            "UPDATE {m} {t} SET rowid = rowid + 1 WHERE v = 'a'",
            "UPDATE {m} {t} SET rowid = 21 WHERE v <> ''",
        ] {
            stmts.push(body.replace("{m}", mode));
        }
        if with_null_rowid {
            stmts.push(format!(
                "INSERT {mode} INTO {{t}}(rowid, k, v) VALUES(NULL, KEY30, 'x')"
            ));
        }
        if spec.alias {
            stmts.push(format!(
                "INSERT {mode} INTO {{t}}(rowid, v) VALUES(40, 'x')"
            ));
            stmts.push(format!(
                "INSERT {mode} INTO {{t}}(rowid, v) VALUES(10, 'x')"
            ));
            stmts.push(format!(
                "INSERT {mode} INTO {{t}}(rowid, v) VALUES(NULL, 'x')"
            ));
        }
    }
    stmts
        .into_iter()
        .map(|st| {
            st.replace("KEY30", &key(spec, 30))
                .replace("KEY32", &key(spec, 32))
                .replace("KEY62", &key(spec, 62))
                .replace("KEY10", &key(spec, 10))
        })
        .collect()
}

fn differential(spec: &'static Spec, stmts: &[String]) {
    let mut diffs = Vec::new();
    for stmt in stmts {
        let lab = Lab::new(spec);
        lab.seed();
        let (native, aydb) = lab.run(stmt);
        if native != aydb {
            diffs.push(format!(
                "[{}] {stmt}
  native: {native}
  aydb:   {aydb}",
                spec.name
            ));
        }
    }
    assert!(
        diffs.is_empty(),
        "{} diffs:
{}",
        diffs.len(),
        diffs.join(
            "
"
        )
    );
}

/// `INTEGER PRIMARY KEY`: the rowid is the key.
#[test]
fn integer_primary_key_rowid_writes_match_native() {
    differential(&SPECS[0], &statements(&SPECS[0], true));
}

/// Keys that keep their rowid in the persistent mapping: an explicit rowid is stored as given,
/// a taken rowid conflicts. (An omitted rowid gets an arbitrary one, so it is not compared.)
#[test]
fn mapped_key_rowid_writes_match_native() {
    for spec in &SPECS[2..] {
        differential(spec, &statements(spec, false));
    }
}

/// A key that is itself the rowid (`INT PRIMARY KEY`, not an alias on a native table) cannot
/// take a rowid of its own: such a request fails instead of being dropped.
#[test]
fn integer_key_rejects_a_rowid_that_differs_from_the_key() {
    for stmt in [
        "INSERT INTO {t}(rowid, k, v) VALUES(31, 32, 'x')",
        "INSERT OR IGNORE INTO {t}(rowid, k, v) VALUES(31, 32, 'x')",
        "INSERT OR REPLACE INTO {t}(rowid, k, v) VALUES(10, 32, 'x')",
        "UPDATE {t} SET rowid = 60 WHERE v = 'a'",
        "UPDATE OR IGNORE {t} SET rowid = 20 WHERE v = 'a'",
        "UPDATE {t} SET rowid = 61, k = 62 WHERE v = 'a'",
    ] {
        let lab = Lab::new(&SPECS[1]);
        lab.seed();
        let before = lab.aydb.rows();
        let (_, aydb) = lab.run(stmt);
        assert!(aydb.contains("ok=false"), "{stmt}: {aydb}");
        assert_eq!(lab.aydb.rows(), before, "{stmt}");
    }
    // Equal rowid and key is fine, and so is leaving the rowid alone.
    let lab = Lab::new(&SPECS[1]);
    lab.seed();
    for stmt in [
        "INSERT INTO a(rowid, k, v) VALUES(30, 30, 'x')",
        "UPDATE a SET v = 'y' WHERE v = 'a'",
        "UPDATE a SET k = 40 WHERE v = 'y'",
    ] {
        lab.aydb.conn.execute_batch(stmt).unwrap();
    }
    assert_eq!(
        lab.aydb
            .conn
            .query_all("SELECT rowid, k FROM a ORDER BY k", &[])
            .unwrap(),
        [(20, 20), (30, 30), (40, 40)]
            .map(|(r, k)| vec![SqlValue::Integer(r), SqlValue::Integer(k)])
    );
}

/// Explicit and changed rowids survive a reconnect and are found by `WHERE rowid = ?`.
#[test]
fn written_rowids_persist_across_reconnect() {
    for spec in &SPECS[2..] {
        let lab = Lab::new(spec);
        lab.aydb
            .conn
            .execute_batch(&format!(
                "INSERT INTO a(rowid, k, v) VALUES(30, {}, 'x');
                 INSERT INTO a(rowid, k, v) VALUES(31, {}, 'y');
                 UPDATE a SET rowid = 61 WHERE v = 'y';
                 UPDATE a SET rowid = 62, v = 'z' WHERE v = 'x';
                 INSERT INTO a(rowid, k, v) VALUES(31, {}, 'w');",
                key(spec, 1),
                key(spec, 2),
                key(spec, 3)
            ))
            .unwrap();
        let expect = |conn: &SqliteConnection| {
            let rows = conn
                .query_all("SELECT rowid, v FROM a ORDER BY rowid", &[])
                .unwrap();
            assert_eq!(
                rows,
                [(31, "w"), (61, "y"), (62, "z")]
                    .map(|(r, v)| vec![SqlValue::Integer(r), SqlValue::Text(v.into())]),
                "{}",
                spec.name
            );
            for (rowid, v) in [(31, "w"), (61, "y"), (62, "z")] {
                assert_eq!(
                    conn.query_all(&format!("SELECT v FROM a WHERE rowid = {rowid}"), &[])
                        .unwrap(),
                    vec![vec![SqlValue::Text(v.into())]],
                    "{} rowid {rowid}",
                    spec.name
                );
            }
            assert!(
                conn.query_all("SELECT v FROM a WHERE rowid = 30", &[])
                    .unwrap()
                    .is_empty(),
                "{} rowid 30 is gone",
                spec.name
            );
        };
        expect(&lab.aydb.conn);
        let again = SqliteConnection::open_in_memory().unwrap();
        again
            .register_aydb_module_with_options(lab.engine.clone(), false)
            .unwrap();
        again
            .execute_batch(&format!(
                "CREATE VIRTUAL TABLE a USING aydb(table_id=1, {}columns='{}')",
                spec.options, spec.columns
            ))
            .unwrap();
        expect(&again);
    }
}
