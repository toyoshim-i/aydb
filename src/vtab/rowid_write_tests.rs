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

/// Keys that keep their rowid in the persistent mapping (`INT PRIMARY KEY`, `TEXT`, capped):
/// an explicit rowid is stored as given and a taken rowid conflicts.
#[test]
fn mapped_key_rowid_writes_match_native() {
    for spec in &SPECS[1..] {
        differential(spec, &statements(spec, false));
    }
}

/// Explicit and changed rowids survive a reconnect and are found by `WHERE rowid = ?`.
#[test]
fn written_rowids_persist_across_reconnect() {
    for spec in &SPECS[1..] {
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

#[test]
fn explicit_rowid_colliding_with_integer_pk_row_conflicts_and_replaces() {
    let alias_key = &SPECS[0];
    assert!(alias_key.alias && alias_key.int_key);
    let lab = Lab::new(alias_key);

    lab.aydb
        .conn
        .execute_batch("INSERT INTO a VALUES (1, 'keep_me')")
        .unwrap();

    // 1. Explicit rowid 1 on a non-integer PK collides with row k=1.
    assert!(
        lab.aydb
            .conn
            .execute_batch("INSERT INTO a(rowid, k, v) VALUES (1, 'abc', 'other')")
            .is_err()
    );
    assert_eq!(
        lab.aydb
            .conn
            .query_all("SELECT rowid, k, v FROM a", &[])
            .unwrap(),
        vec![vec![
            SqlValue::Integer(1),
            SqlValue::Integer(1),
            SqlValue::Text("keep_me".into()),
        ]]
    );

    // 2. Updating another row's rowid to 1 must fail and DELETE must target the right row.
    lab.aydb
        .conn
        .execute_batch("INSERT INTO a(k, v) VALUES ('abc', 'other')")
        .unwrap();
    assert!(
        lab.aydb
            .conn
            .execute_batch("UPDATE a SET rowid = 1 WHERE k = 'abc'")
            .is_err()
    );
    lab.aydb
        .conn
        .execute_batch("DELETE FROM a WHERE k = 'abc'")
        .unwrap();
    assert_eq!(
        lab.aydb
            .conn
            .query_all("SELECT rowid, k, v FROM a", &[])
            .unwrap(),
        vec![vec![
            SqlValue::Integer(1),
            SqlValue::Integer(1),
            SqlValue::Text("keep_me".into()),
        ]]
    );

    // 3. INSERT OR REPLACE replaces (1, 'keep_me') rather than leaving duplicate rowid 1.
    lab.aydb
        .conn
        .execute_batch("INSERT OR REPLACE INTO a(rowid, k, v) VALUES (1, 'abc', 'replaced')")
        .unwrap();
    assert_eq!(
        lab.aydb
            .conn
            .query_all("SELECT rowid, k, v FROM a", &[])
            .unwrap(),
        vec![vec![
            SqlValue::Integer(1),
            SqlValue::Text("abc".into()),
            SqlValue::Text("replaced".into()),
        ]]
    );
    lab.aydb
        .conn
        .execute_batch("DELETE FROM a WHERE k = 'abc'")
        .unwrap();
    assert!(
        lab.aydb
            .conn
            .query_all("SELECT rowid, k, v FROM a", &[])
            .unwrap()
            .is_empty()
    );

    // 4. UPDATE OR REPLACE to rowid 1 also replaces (1, 'keep_me').
    lab.aydb
        .conn
        .execute_batch(
            "INSERT INTO a VALUES (1, 'keep_me'); INSERT INTO a(k, v) VALUES ('abc', 'other');",
        )
        .unwrap();
    lab.aydb
        .conn
        .execute_batch("UPDATE OR REPLACE a SET rowid = 1 WHERE k = 'abc'")
        .unwrap();
    assert_eq!(
        lab.aydb
            .conn
            .query_all("SELECT rowid, k, v FROM a", &[])
            .unwrap(),
        vec![vec![
            SqlValue::Integer(1),
            SqlValue::Text("abc".into()),
            SqlValue::Text("other".into()),
        ]]
    );
}

#[test]
fn failed_rowid_write_rolls_back_primary_row_atomically() {
    let dir = TempTestDir::new("rowid_write_atomicity");
    let fs = Arc::new(OsFs::open(&dir.path, true).unwrap());
    let mut cfg = EngineConfig::default();
    cfg.sync_wal_size_bytes = 64 * 4096;
    cfg.buf_wal_size_bytes = 64 * 4096;
    cfg.max_transaction_bytes = 512;
    let engine = Arc::new(RwLock::new(StorageEngine::open(fs, cfg).unwrap()));
    let conn = SqliteConnection::open_in_memory().unwrap();
    conn.register_aydb_module_with_options(engine, false)
        .unwrap();
    conn.execute_batch(
        "CREATE VIRTUAL TABLE t USING aydb(table_id=1, columns='k TEXT PRIMARY KEY, v TEXT');
         INSERT INTO t VALUES ('seed', 'ok');",
    )
    .unwrap();
    let seed_rows = conn
        .query_all("SELECT rowid, k, v FROM t ORDER BY k", &[])
        .unwrap();
    assert_eq!(seed_rows.len(), 1);

    // 1. Non-integer PK INSERT whose secondary rowid allocation fails `max_transaction_bytes`
    //    after `put_row_owned_with_hint` has already succeeded must not leave an unmapped row.
    let mut saw_insert_ok = false;
    let mut saw_insert_err = false;
    for len in (380..=520).step_by(2) {
        let val = "x".repeat(len);
        conn.execute_batch("BEGIN").unwrap();
        let res = conn.execute_batch(&format!("INSERT INTO t VALUES ('k{len}', '{val}')"));
        if res.is_ok() {
            saw_insert_ok = true;
            conn.execute_batch("ROLLBACK").unwrap();
        } else {
            saw_insert_err = true;
            assert_eq!(
                conn.query_all("SELECT rowid, k, v FROM t ORDER BY k", &[])
                    .unwrap(),
                seed_rows
            );
            conn.execute_batch("COMMIT").unwrap();
            assert_eq!(
                conn.query_all("SELECT rowid, k, v FROM t ORDER BY k", &[])
                    .unwrap(),
                seed_rows
            );
            assert_eq!(
                conn.query_all("PRAGMA integrity_check", &[]).unwrap(),
                vec![vec![SqlValue::Text("ok".into())]]
            );
            break;
        }
    }
    assert!(saw_insert_ok && saw_insert_err);

    // 2. Explicit `UPDATE SET rowid` whose `set_rowid` fails `max_transaction_bytes` after
    //    `put_row_owned_with_hint` updated the primary row must roll back atomically too.
    let mut saw_update_ok = false;
    let mut saw_update_err = false;
    for len in (380..=520).step_by(2) {
        let val = "y".repeat(len);
        conn.execute_batch("BEGIN").unwrap();
        let res = conn.execute_batch(&format!(
            "UPDATE t SET rowid = 999, v = '{val}' WHERE k = 'seed'"
        ));
        if res.is_ok() {
            saw_update_ok = true;
            conn.execute_batch("ROLLBACK").unwrap();
        } else {
            saw_update_err = true;
            assert_eq!(
                conn.query_all("SELECT rowid, k, v FROM t ORDER BY k", &[])
                    .unwrap(),
                seed_rows
            );
            conn.execute_batch("COMMIT").unwrap();
            assert_eq!(
                conn.query_all("SELECT rowid, k, v FROM t ORDER BY k", &[])
                    .unwrap(),
                seed_rows
            );
            assert_eq!(
                conn.query_all("PRAGMA integrity_check", &[]).unwrap(),
                vec![vec![SqlValue::Text("ok".into())]]
            );
            break;
        }
    }
    assert!(saw_update_ok && saw_update_err);
}

#[test]
fn non_integer_pk_exceeding_max_transaction_bytes_leaves_row_and_rowid_intact() {
    let dir = TempTestDir::new("non_int_pk_max_tx_bytes");
    let fs = Arc::new(OsFs::open(&dir.path, true).unwrap());
    let mut cfg = EngineConfig::default();
    cfg.sync_wal_size_bytes = 64 * 4096;
    cfg.buf_wal_size_bytes = 64 * 4096;
    cfg.max_transaction_bytes = 640;
    let engine = Arc::new(RwLock::new(StorageEngine::open(fs, cfg).unwrap()));
    let conn = SqliteConnection::open_in_memory().unwrap();
    conn.register_aydb_module_with_options(engine, false)
        .unwrap();

    conn.execute_batch(
        "CREATE VIRTUAL TABLE t USING aydb(table_id=1, columns='k TEXT PRIMARY KEY NOT NULL, u TEXT UNIQUE, v TEXT');
         CREATE VIRTUAL TABLE b USING aydb(table_id=2, columns='k BLOB PRIMARY KEY NOT NULL, v TEXT');
         INSERT INTO t VALUES ('k1', 'u1', 'v1'), ('k2', 'u2', 'v2');
         INSERT INTO b VALUES (X'0102', 'bv1');",
    )
    .unwrap();

    let big_val = "z".repeat(1024);

    // Subcase A: multi-stmt + SAVEPOINT + oversized TEXT PK INSERT must fail atomically and leave
    // no orphan UNIQUE index entry.
    conn.execute_batch("BEGIN; INSERT INTO t VALUES ('k3', 'u3', 'small'); SAVEPOINT s;")
        .unwrap();
    assert!(
        conn.execute_batch(&format!(
            "INSERT INTO t VALUES ('k_big', 'u_big', '{big_val}');"
        ))
        .is_err()
    );
    conn.execute_batch("ROLLBACK TO s; RELEASE s; COMMIT;")
        .unwrap();

    let t_rows = conn
        .query_all("SELECT rowid, k, u, v FROM t ORDER BY k", &[])
        .unwrap();
    assert_eq!(t_rows.len(), 3);
    assert_eq!(t_rows[0][1], SqlValue::Text("k1".into()));
    assert_eq!(t_rows[1][1], SqlValue::Text("k2".into()));
    assert_eq!(t_rows[2][1], SqlValue::Text("k3".into()));
    conn.execute_batch("INSERT INTO t VALUES ('k_big_ok', 'u_big', 'tiny');")
        .unwrap();
    conn.execute_batch("DELETE FROM t WHERE k = 'k_big_ok';")
        .unwrap();
    assert_eq!(
        conn.query_all("PRAGMA integrity_check", &[]).unwrap(),
        vec![vec![SqlValue::Text("ok".into())]]
    );

    // Subcase B: PK-changing UPDATE exceeding max_transaction_bytes leaves primary row, rowid, and UNIQUE intact.
    let snap_t = conn
        .query_all("SELECT rowid, k, u, v FROM t ORDER BY k", &[])
        .unwrap();
    conn.execute_batch("BEGIN;").unwrap();
    assert!(
        conn.execute_batch(&format!(
            "UPDATE t SET k = 'k1_moved', u = 'u1_moved', v = '{big_val}' WHERE k = 'k1';"
        ))
        .is_err()
    );
    conn.execute_batch("COMMIT;").unwrap();
    assert_eq!(
        conn.query_all("SELECT rowid, k, u, v FROM t ORDER BY k", &[])
            .unwrap(),
        snap_t
    );
    assert_eq!(
        conn.query_all("PRAGMA integrity_check", &[]).unwrap(),
        vec![vec![SqlValue::Text("ok".into())]]
    );

    // Subcase C: BLOB PRIMARY KEY INSERT exceeding max_transaction_bytes rolls back cleanly.
    let snap_b = conn
        .query_all("SELECT rowid, k, v FROM b ORDER BY k", &[])
        .unwrap();
    conn.execute_batch("BEGIN;").unwrap();
    assert!(
        conn.execute_batch(&format!("INSERT INTO b VALUES (X'aabb', '{big_val}');"))
            .is_err()
    );
    conn.execute_batch("COMMIT;").unwrap();
    assert_eq!(
        conn.query_all("SELECT rowid, k, v FROM b ORDER BY k", &[])
            .unwrap(),
        snap_b
    );
    assert_eq!(
        conn.query_all("PRAGMA integrity_check", &[]).unwrap(),
        vec![vec![SqlValue::Text("ok".into())]]
    );
}

#[test]
fn non_alias_pk_insert_and_in_place_update_use_single_wal_batch_without_savepoint() {
    let dir = TempTestDir::new("atomic_row_and_rowid_batch");
    let fs = Arc::new(OsFs::open(&dir.path, true).unwrap());
    let mut cfg = EngineConfig::default();
    cfg.sync_wal_size_bytes = 64 * 4096;
    cfg.buf_wal_size_bytes = 64 * 4096;
    let engine = Arc::new(RwLock::new(StorageEngine::open(fs, cfg).unwrap()));
    let conn = SqliteConnection::open_in_memory().unwrap();
    conn.register_aydb_module_with_options(Arc::clone(&engine), false)
        .unwrap();

    conn.execute_batch(
        "CREATE VIRTUAL TABLE t USING aydb(table_id=1, columns='k TEXT PRIMARY KEY NOT NULL, v INT');
         CREATE VIRTUAL TABLE i USING aydb(table_id=2, columns='k INT PRIMARY KEY NOT NULL, v INT');",
    )
    .unwrap();

    conn.execute_batch("BEGIN;").unwrap();
    let lsn0 = engine.read().unwrap().next_lsn();
    conn.execute_batch("INSERT INTO t VALUES ('a', 1);")
        .unwrap();
    let lsn1 = engine.read().unwrap().next_lsn();
    assert_eq!(
        lsn1 - lsn0,
        1,
        "TEXT PK INSERT must emit row + rowid in a single WAL batch"
    );

    conn.execute_batch("INSERT INTO i VALUES (10, 100);")
        .unwrap();
    let lsn2 = engine.read().unwrap().next_lsn();
    assert_eq!(
        lsn2 - lsn1,
        1,
        "INT PK INSERT must emit row + rowid in a single WAL batch"
    );

    let t_rid = match conn
        .query_all("SELECT rowid FROM t WHERE k = 'a'", &[])
        .unwrap()[0][0]
    {
        SqlValue::Integer(r) => r,
        ref other => panic!("unexpected rowid {other:?}"),
    };
    let i_rid = match conn
        .query_all("SELECT rowid FROM i WHERE k = 10", &[])
        .unwrap()[0][0]
    {
        SqlValue::Integer(r) => r,
        ref other => panic!("unexpected rowid {other:?}"),
    };

    let lsn_before_upd_t = engine.read().unwrap().next_lsn();
    conn.execute_batch("UPDATE t SET v = 2 WHERE k = 'a';")
        .unwrap();
    assert_eq!(engine.read().unwrap().next_lsn() - lsn_before_upd_t, 1);

    let lsn_before_upd_i = engine.read().unwrap().next_lsn();
    conn.execute_batch("UPDATE i SET v = 200 WHERE k = 10;")
        .unwrap();
    assert_eq!(engine.read().unwrap().next_lsn() - lsn_before_upd_i, 1);

    conn.execute_batch("COMMIT;").unwrap();
    assert_eq!(
        conn.query_all("SELECT rowid, v FROM t WHERE k = 'a'", &[])
            .unwrap(),
        vec![vec![SqlValue::Integer(t_rid), SqlValue::Integer(2)]]
    );
    assert_eq!(
        conn.query_all("SELECT rowid, v FROM i WHERE k = 10", &[])
            .unwrap(),
        vec![vec![SqlValue::Integer(i_rid), SqlValue::Integer(200)]]
    );
}
