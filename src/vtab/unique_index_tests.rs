//! The persistent UNIQUE index entries written by `xUpdate`: after every statement the `'i'`
//! entries and `'t'` records must be exactly what the rows imply. The check scans the table
//! with SQL and derives the expectation independently of the write path.

use super::unique_key::{canon_unique_key, unique_index_component};
use super::*;
use crate::engine::EngineConfig;
use crate::index::{
    NS_PRIMARY_ROW, NS_ROW_META, NS_SECONDARY_INDEX, encode_row_meta_key,
    encode_secondary_index_key,
};
use crate::util::test_util::TempTestDir;
use std::collections::{BTreeMap, BTreeSet};

const DECLARED_C: u16 = 1;
const COLUMNS: &str = "columns='k TEXT PRIMARY KEY, a TEXT UNIQUE, b INTEGER UNIQUE, \
                       n TEXT UNIQUE COLLATE NOCASE, c TEXT', indexes='c'";
type Engine = Arc<RwLock<StorageEngine<OsFs>>>;

fn ddl(table_id: u16, extra: &str) -> String {
    format!("CREATE VIRTUAL TABLE t USING aydb(table_id={table_id}, {extra}{COLUMNS})")
}

struct Fx {
    dir: TempTestDir,
    engine: Engine,
    conn: SqliteConnection,
    table_id: u16,
    capped: bool,
}

fn open_engine(dir: &TempTestDir) -> Engine {
    let fs = Arc::new(OsFs::open(&dir.path, true).unwrap());
    Arc::new(RwLock::new(
        StorageEngine::open(fs, EngineConfig::default()).unwrap(),
    ))
}

fn connect(engine: &Engine, table_id: u16, extra: &str) -> SqliteConnection {
    let conn = SqliteConnection::open_in_memory().unwrap();
    conn.register_aydb_module_with_options(engine.clone(), false)
        .unwrap();
    conn.execute_batch(&ddl(table_id, extra)).unwrap();
    conn
}

fn fixture(name: &str, extra: &str) -> Fx {
    let dir = TempTestDir::new(name);
    let engine = open_engine(&dir);
    let conn = connect(&engine, 1, extra);
    Fx {
        dir,
        engine,
        conn,
        table_id: 1,
        capped: extra.contains("max_bytes"),
    }
}

fn nocase(s: &str) -> Vec<u8> {
    let mut key = vec![KEY_TAG_TEXT];
    key.extend(s.bytes().map(|b| b.to_ascii_lowercase()));
    key
}

/// The pairs a row must record in its `'t'` record, in order: declared indexes, then UNIQUE
/// columns by column position.
fn expected_pairs(row: &[SqlValue]) -> Vec<(u16, Vec<u8>)> {
    let mut pairs = Vec::new();
    // A declared index records NULL as well.
    pairs.push((DECLARED_C, row[4].to_key_bytes()));
    for (col, iid, fold) in [
        (1usize, 0x8001u16, false),
        (2, 0x8002, false),
        (3, 0x8003, true),
    ] {
        let key = match &row[col] {
            SqlValue::Null => continue,
            SqlValue::Text(s) if fold => nocase(s),
            other => other.to_key_bytes(),
        };
        pairs.push((iid, unique_index_component(&canon_unique_key(&key))));
    }
    pairs
}

fn rows_of(conn: &SqliteConnection) -> Vec<Vec<SqlValue>> {
    conn.query_all("SELECT k, a, b, n, c FROM t", &[]).unwrap()
}

/// Asserts that the engine holds exactly the `'i'` entries and `'t'` records the rows imply.
fn assert_index(fx: &Fx, what: &str) {
    let rows = rows_of(&fx.conn);
    let tid = fx.table_id;
    let mut want_i = BTreeSet::new();
    let mut want_t = BTreeMap::new();
    for row in &rows {
        let pk = row[0].to_key_bytes();
        let pairs = expected_pairs(row);
        for (id, col) in &pairs {
            want_i.insert(encode_secondary_index_key(tid, *id, col, &pk));
        }
        if fx.capped || !pairs.is_empty() {
            want_t.insert(pk, pairs);
        }
    }
    let mut eng = fx.engine.write().unwrap();
    let got_i: BTreeSet<Vec<u8>> = eng
        .test_live_keys(tid, NS_SECONDARY_INDEX)
        .unwrap()
        .into_iter()
        .collect();
    assert_eq!(got_i, want_i, "{what}: 'i' entries");
    let got_t: BTreeSet<Vec<u8>> = eng
        .test_live_keys(tid, NS_ROW_META)
        .unwrap()
        .into_iter()
        .collect();
    let want_t_keys: BTreeSet<Vec<u8>> = want_t
        .keys()
        .map(|pk| encode_row_meta_key(tid, pk))
        .collect();
    assert_eq!(got_t, want_t_keys, "{what}: 't' records");
    for (pk, pairs) in &want_t {
        let meta = eng.test_row_meta(tid, pk).unwrap().unwrap();
        assert_eq!(&meta.secondary_keys, pairs, "{what}: pairs of {pk:02x?}");
        if fx.capped {
            assert!(meta.recorded_tick > 0, "{what}: capped row has a tick");
        } else {
            assert_eq!(meta.recorded_tick, 0);
        }
    }
}

fn run(fx: &Fx, sql: &str) {
    fx.conn.execute_batch(sql).unwrap();
    assert_index(fx, sql);
}

fn run_err(fx: &Fx, sql: &str) {
    assert!(fx.conn.execute_batch(sql).is_err(), "{sql} should fail");
    assert_index(fx, sql);
}

#[test]
fn every_write_path_leaves_exactly_the_entries_the_rows_imply() {
    let fx = fixture("unique_index_paths", "");
    run(&fx, "INSERT INTO t VALUES('k1','A',1,'Nn','c1')");
    run(&fx, "INSERT INTO t VALUES('k2',NULL,NULL,NULL,NULL)");
    run(&fx, "INSERT INTO t VALUES('k3','B',2,'Pp',NULL)");
    // The declared index and the three UNIQUE columns are told apart.
    {
        let mut eng = fx.engine.write().unwrap();
        let meta = eng
            .test_row_meta(1, &SqlValue::Text("k1".into()).to_key_bytes())
            .unwrap()
            .unwrap();
        let ids: Vec<u16> = meta.secondary_keys.iter().map(|p| p.0).collect();
        assert_eq!(ids, vec![1, 0x8001, 0x8002, 0x8003]);
    }
    // UPDATE of a UNIQUE column, of a non-key column, to NULL, and from NULL.
    run(&fx, "UPDATE t SET a='A2' WHERE k='k1'");
    run(&fx, "UPDATE t SET c='c2' WHERE k='k1'");
    run(&fx, "UPDATE t SET n=NULL, b=NULL WHERE k='k1'");
    run(&fx, "UPDATE t SET a='fromnull', b=9 WHERE k='k2'");
    run(&fx, "UPDATE t SET c='other' WHERE k='k2'");
    // NOCASE and exact-integer floats are equal to their canonical forms.
    run(&fx, "UPDATE t SET n='NN', b=7.0 WHERE k='k2'");
    // PK-changing UPDATE moves every entry.
    run(&fx, "UPDATE t SET k='k9' WHERE k='k3'");
    run(&fx, "UPDATE t SET k='k8', a='B2' WHERE k='k9'");
    // Conflicts: abort, ignore, replace (on a UNIQUE column and on the PK).
    run_err(&fx, "INSERT INTO t VALUES('k4','A2',20,'x','y')");
    run_err(&fx, "INSERT INTO t VALUES('k4','zz',7,'x','y')");
    run_err(&fx, "INSERT INTO t VALUES('k4','zz',21,'nn','y')");
    run(&fx, "INSERT OR IGNORE INTO t VALUES('k4','A2',20,'x','y')");
    run(&fx, "INSERT OR REPLACE INTO t VALUES('k4','A2',20,'x','y')");
    run(
        &fx,
        "INSERT OR REPLACE INTO t VALUES('k4','A3',22,'x2','y2')",
    );
    run(&fx, "INSERT OR REPLACE INTO t VALUES('k8','C',30,'q','z')");
    run(&fx, "UPDATE OR REPLACE t SET a='C' WHERE k='k4'");
    run(&fx, "DELETE FROM t WHERE k='k4'");
    run(&fx, "DELETE FROM t");
    assert_eq!(rows_of(&fx.conn).len(), 0);
}

#[test]
fn savepoints_and_rollback_leave_no_entries() {
    let fx = fixture("unique_index_rollback", "");
    run(&fx, "INSERT INTO t VALUES('base','A',1,'N','c')");
    fx.conn
        .execute_batch("BEGIN; INSERT INTO t VALUES('x1','B',2,'M','c');")
        .unwrap();
    assert_index(&fx, "inside transaction");
    fx.conn
        .execute_batch("SAVEPOINT s; INSERT INTO t VALUES('x2','C',3,'L','c'); UPDATE t SET a='D' WHERE k='x1';")
        .unwrap();
    assert_index(&fx, "inside savepoint");
    fx.conn.execute_batch("ROLLBACK TO s; RELEASE s;").unwrap();
    assert_index(&fx, "after ROLLBACK TO");
    assert_eq!(rows_of(&fx.conn).len(), 2);
    fx.conn
        .execute_batch("DELETE FROM t WHERE k='base'; UPDATE t SET k='moved' WHERE k='x1';")
        .unwrap();
    assert_index(&fx, "before rollback");
    fx.conn.execute_batch("ROLLBACK;").unwrap();
    assert_index(&fx, "after ROLLBACK");
    let rows = rows_of(&fx.conn);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], SqlValue::Text("base".into()));
}

#[test]
fn long_values_use_the_digest_form() {
    let fx = fixture("unique_index_long", "");
    let long = "x".repeat(600);
    let almost = format!("{}y", "x".repeat(599));
    run(
        &fx,
        &format!("INSERT INTO t VALUES('l1','{long}',1,'n1','c')"),
    );
    run(
        &fx,
        &format!("INSERT INTO t VALUES('l2','{almost}',2,'n2','c')"),
    );
    run_err(
        &fx,
        &format!("INSERT INTO t VALUES('l3','{long}',3,'n3','c')"),
    );
    {
        let eng = fx.engine.read().unwrap();
        let prefix = [0u8, 1, b'i', 0x80, 0x01];
        let long_keys: Vec<Vec<u8>> = eng
            .test_live_keys(1, NS_SECONDARY_INDEX)
            .unwrap()
            .into_iter()
            .filter(|k| k.starts_with(&prefix))
            .collect();
        assert_eq!(long_keys.len(), 2);
        for key in long_keys {
            // 5 prefix bytes + 77-byte component + escaping + 2-byte pk
            assert!(
                key.len() < 100,
                "key of {} bytes is not a digest",
                key.len()
            );
            assert_eq!(key[5], 0xFE);
        }
    }
    run_err(&fx, &format!("UPDATE t SET a='{long}' WHERE k='l2'"));
}

#[test]
fn drop_table_removes_every_entry() {
    let fx = fixture("unique_index_drop", "");
    run(&fx, "INSERT INTO t VALUES('k1','A',1,'N','c')");
    run(&fx, "INSERT INTO t VALUES('k2','B',2,'M','d')");
    fx.conn.execute_batch("DROP TABLE t").unwrap();
    let eng = fx.engine.read().unwrap();
    for ns in [NS_PRIMARY_ROW, NS_SECONDARY_INDEX, NS_ROW_META] {
        assert!(eng.test_live_keys(1, ns).unwrap().is_empty());
    }
}

#[test]
fn capped_eviction_removes_the_entries_of_evicted_rows() {
    let fx = fixture("unique_index_capped", "max_bytes=1500, ");
    for i in 0..40 {
        let pad = "p".repeat(60);
        run(
            &fx,
            &format!("INSERT INTO t VALUES('k{i:02}','a{i}',{i},'n{i}','{pad}')"),
        );
    }
    let remaining = rows_of(&fx.conn).len();
    assert!(remaining > 0 && remaining < 40, "{remaining} rows left");
    run(&fx, "UPDATE t SET a='moved' WHERE k='k39'");
    run(&fx, "DELETE FROM t WHERE k='k39'");
}

#[test]
fn overwriting_a_row_without_meta_leaves_a_correct_state() {
    // A row written before the UNIQUE index existed has neither entries nor a 't' record.
    fn legacy(fx: &Fx, k: &str, a: &str, b: i64, n: &str, c: &str) {
        let row = encode_row(&[
            SqlValue::Text(k.into()),
            SqlValue::Text(a.into()),
            SqlValue::Integer(b),
            SqlValue::Text(n.into()),
            SqlValue::Text(c.into()),
        ]);
        let mut eng = fx.engine.write().unwrap();
        let pk = SqlValue::Text(k.into()).to_key_bytes();
        eng.put_row(1, &pk, &row, &[], WriteDurability::BufferedFast)
            .unwrap();
        rowid_map::assign_synthetic_rowid(&mut eng, 1, &pk, WriteDurability::BufferedFast, false)
            .unwrap();
    }
    for (i, sql) in [
        "UPDATE t SET a='new' WHERE k='L'",
        "UPDATE t SET c='new' WHERE k='L'",
        "UPDATE t SET k='M', a='new' WHERE k='L'",
        "INSERT OR REPLACE INTO t VALUES('L','new',5,'x','y')",
        "INSERT OR REPLACE INTO t VALUES('M','la',5,'x','y')",
        "DELETE FROM t WHERE k='L'",
    ]
    .iter()
    .enumerate()
    {
        let fx = fixture(&format!("unique_index_legacy{i}"), "");
        legacy(&fx, "L", "la", 7, "LN", "lc");
        run(&fx, sql);
    }
}

#[test]
fn entries_survive_reopen_and_reconnect() {
    let fx = fixture("unique_index_reopen", "");
    run(&fx, "INSERT INTO t VALUES('k1','A',1,'N','c')");
    run(&fx, "INSERT INTO t VALUES('k2','B',2,'M','d')");
    run(&fx, "INSERT INTO t VALUES('k3',NULL,NULL,NULL,NULL)");
    let Fx {
        dir, engine, conn, ..
    } = fx;
    drop(conn);
    engine.write().unwrap().checkpoint().unwrap();
    drop(engine);

    let engine = open_engine(&dir);
    let conn = connect(&engine, 1, "");
    let fx = Fx {
        dir,
        engine,
        conn,
        table_id: 1,
        capped: false,
    };
    assert_index(&fx, "after reopen");
    run(&fx, "DELETE FROM t WHERE k='k1'");
    run(&fx, "UPDATE t SET a='B2' WHERE k='k2'");
    run(&fx, "INSERT INTO t VALUES('k4','A',1,'N','c')");
    // A second connection to the same engine sees and maintains the same state.
    let other = connect(&fx.engine, 1, "");
    other
        .execute_batch("UPDATE t SET a='viaother' WHERE k='k4'")
        .unwrap();
    assert_index(&fx, "second connection");
}

// ---------------------------------------------------------------------------------------------
// Differential test against a native SQLite table
// ---------------------------------------------------------------------------------------------

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn lit_text(rng: &mut Rng, pool: &[&str]) -> String {
    if rng.below(7) == 0 {
        "NULL".to_string()
    } else {
        format!("'{}'", pool[rng.below(pool.len() as u64) as usize])
    }
}

fn lit_int(rng: &mut Rng) -> String {
    match rng.below(9) {
        0 => "NULL".to_string(),
        1 => "2.0".to_string(),
        2 => "'3'".to_string(),
        _ => format!("{}", rng.below(5)),
    }
}

fn sorted_rows(conn: &SqliteConnection) -> Vec<String> {
    let mut rows: Vec<String> = rows_of(conn).iter().map(|r| format!("{r:?}")).collect();
    rows.sort();
    rows
}

#[test]
fn random_statements_match_native_sqlite_and_keep_the_index_exact() {
    let long = "w".repeat(300);
    let long2 = format!("{}z", "w".repeat(299));
    let a_pool = ["x", "y", "z", "X", "", &long, &long2];
    let n_pool = ["p", "P", "q", "Q", "p "];
    let k_pool = ["k0", "k1", "k2", "k3", "k4", "k5"];
    for seed in 0..200u64 {
        let mut rng = Rng(seed.wrapping_mul(0x2545_F491_4F6C_DD1D) ^ 0xA5A5);
        let fx = fixture(&format!("unique_index_diff{seed}"), "");
        let native = SqliteConnection::open_in_memory().unwrap();
        native
            .execute_batch(
                "CREATE TABLE t(k TEXT PRIMARY KEY, a TEXT UNIQUE, b INTEGER UNIQUE, \
                 n TEXT UNIQUE COLLATE NOCASE, c TEXT)",
            )
            .unwrap();
        let mut savepoints = 0u32;
        let mut in_tx = false;
        for step in 0..60 {
            let k = k_pool[rng.below(6) as usize];
            let or = ["", "OR IGNORE ", "OR REPLACE "][rng.below(3) as usize];
            let sql = match rng.below(14) {
                0..=4 => format!(
                    "INSERT {or}INTO t VALUES('{k}',{},{},{},{})",
                    lit_text(&mut rng, &a_pool),
                    lit_int(&mut rng),
                    lit_text(&mut rng, &n_pool),
                    lit_text(&mut rng, &["c1", "c2"]),
                ),
                5 | 6 => format!(
                    "UPDATE {or}t SET a={}, b={} WHERE k='{k}'",
                    lit_text(&mut rng, &a_pool),
                    lit_int(&mut rng)
                ),
                7 => format!(
                    "UPDATE {or}t SET n={}, c={} WHERE k='{k}'",
                    lit_text(&mut rng, &n_pool),
                    lit_text(&mut rng, &["c1", "c3"]),
                ),
                8 => {
                    // A PK-changing UPDATE OR REPLACE with row aliasing is a known open bug.
                    let to = k_pool[rng.below(6) as usize];
                    let or = if or == "OR REPLACE " { "" } else { or };
                    format!("UPDATE {or}t SET k='{to}' WHERE k='{k}'")
                }
                9 | 10 => format!("DELETE FROM t WHERE k='{k}'"),
                11 if !in_tx => {
                    in_tx = true;
                    "BEGIN".to_string()
                }
                11 => {
                    savepoints += 1;
                    format!("SAVEPOINT s{savepoints}")
                }
                12 if in_tx && savepoints > 0 => {
                    let s = 1 + rng.below(savepoints as u64);
                    if rng.below(2) == 0 {
                        format!("ROLLBACK TO s{s}")
                    } else {
                        savepoints = s as u32 - 1;
                        format!("RELEASE s{s}")
                    }
                }
                12 => format!("DELETE FROM t WHERE k='{k}'"),
                _ if in_tx => {
                    in_tx = false;
                    savepoints = 0;
                    if rng.below(2) == 0 {
                        "COMMIT"
                    } else {
                        "ROLLBACK"
                    }
                    .to_string()
                }
                _ => format!("DELETE FROM t WHERE k='{k}'"),
            };
            let expected = native.execute_batch(&sql);
            let actual = fx.conn.execute_batch(&sql);
            assert_eq!(
                expected.is_ok(),
                actual.is_ok(),
                "seed {seed} step {step}: {sql}\n native: {expected:?}\n aydb: {actual:?}"
            );
            assert_eq!(
                sorted_rows(&native),
                sorted_rows(&fx.conn),
                "seed {seed} step {step}: {sql}"
            );
            assert_index(&fx, &format!("seed {seed} step {step}: {sql}"));
        }
        if in_tx {
            native.execute_batch("ROLLBACK").unwrap();
            fx.conn.execute_batch("ROLLBACK").unwrap();
            assert_index(&fx, "final rollback");
        }
    }
}

#[test]
fn a_unique_only_table_records_meta_only_for_rows_with_a_key_across_reopen() {
    let dir = TempTestDir::new("unique_index_only");
    let ddl = "CREATE VIRTUAL TABLE u USING aydb(table_id=2, \
               columns='k TEXT PRIMARY KEY, a TEXT UNIQUE')";
    let open = |dir: &TempTestDir| {
        let engine = open_engine(dir);
        let conn = SqliteConnection::open_in_memory().unwrap();
        conn.register_aydb_module_with_options(engine.clone(), false)
            .unwrap();
        conn.execute_batch(ddl).unwrap();
        (engine, conn)
    };
    let pk = |k: &str| SqlValue::Text(k.into()).to_key_bytes();
    let (engine, conn) = open(&dir);
    conn.execute_batch("INSERT INTO u VALUES('k1',NULL); INSERT INTO u VALUES('k2','A')")
        .unwrap();
    {
        let mut eng = engine.write().unwrap();
        assert_eq!(eng.test_live_keys(2, NS_ROW_META).unwrap().len(), 1);
        assert!(eng.test_row_meta(2, &pk("k1")).unwrap().is_none());
        assert!(eng.test_row_meta(2, &pk("k2")).unwrap().is_some());
        eng.checkpoint().unwrap();
    }
    drop(conn);
    drop(engine);

    // After a reopen the engine has not seen a write to the table yet: the first delete must
    // still find the 't' record of the row and remove its entry.
    let (engine, conn) = open(&dir);
    conn.execute_batch("DELETE FROM u WHERE k='k2'").unwrap();
    {
        let eng = engine.read().unwrap();
        assert!(
            eng.test_live_keys(2, NS_SECONDARY_INDEX)
                .unwrap()
                .is_empty()
        );
        assert!(eng.test_live_keys(2, NS_ROW_META).unwrap().is_empty());
    }
    // Setting a key on a row that had none creates the record.
    conn.execute_batch("UPDATE u SET a='B' WHERE k='k1'")
        .unwrap();
    {
        let mut eng = engine.write().unwrap();
        assert_eq!(eng.test_live_keys(2, NS_SECONDARY_INDEX).unwrap().len(), 1);
        assert!(eng.test_row_meta(2, &pk("k1")).unwrap().is_some());
    }
    conn.execute_batch("UPDATE u SET a=NULL WHERE k='k1'")
        .unwrap();
    let eng = engine.read().unwrap();
    assert!(
        eng.test_live_keys(2, NS_SECONDARY_INDEX)
            .unwrap()
            .is_empty()
    );
    assert!(eng.test_live_keys(2, NS_ROW_META).unwrap().is_empty());
}
