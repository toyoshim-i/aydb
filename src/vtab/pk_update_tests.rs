//! Multi-row `UPDATE` statements that change the primary key, compared with a native table.
//!
//! SQLite collects the rows of such a statement first and then calls `xUpdate` once per row
//! with the row's old rowid. An earlier row's REPLACE resolution can delete a row that is still
//! queued, and renumbering rows would make a queued rowid address the wrong row. After every
//! statement both sides must hold the same rows with the same rowids.

use super::*;
use crate::engine::EngineConfig;
use crate::util::test_util::TempTestDir;

struct Spec {
    name: &'static str,
    columns: &'static str,
    options: &'static str,
    /// The key is an integer: `k` is written as a number.
    int_key: bool,
    /// The rowid of an aydb row is the key, whatever the native table keeps (`INT PRIMARY KEY`
    /// is not a rowid alias on a native table): rowids are left out of the comparison.
    rowid_is_key: bool,
    /// The key is its own rowid (both flavours above): a row whose key moves onto the key of
    /// a row that is still queued makes SQLite visit the moved row, with values computed for
    /// the replaced one. That cannot be reproduced, so such statements are not generated.
    key_is_rowid: bool,
}

const SPECS: [Spec; 5] = [
    Spec {
        name: "text",
        columns: "k TEXT PRIMARY KEY, u TEXT, v INT",
        options: "",
        int_key: false,
        rowid_is_key: false,
        key_is_rowid: false,
    },
    Spec {
        name: "text_unique",
        columns: "k TEXT PRIMARY KEY, u TEXT UNIQUE, v INT",
        options: "",
        int_key: false,
        rowid_is_key: false,
        key_is_rowid: false,
    },
    Spec {
        name: "integer_pk",
        columns: "k INTEGER PRIMARY KEY, u TEXT UNIQUE, v INT",
        options: "",
        int_key: true,
        rowid_is_key: false,
        key_is_rowid: true,
    },
    Spec {
        name: "int_pk",
        columns: "k INT PRIMARY KEY, u TEXT UNIQUE, v INT",
        options: "",
        int_key: true,
        rowid_is_key: true,
        key_is_rowid: true,
    },
    Spec {
        name: "capped",
        columns: "k TEXT PRIMARY KEY, u TEXT UNIQUE, v INT",
        options: "max_bytes=100000, ",
        int_key: false,
        rowid_is_key: false,
        key_is_rowid: false,
    },
];

const MODES: [&str; 5] = ["", "OR ABORT", "OR FAIL", "OR IGNORE", "OR REPLACE"];

const SENTINEL: i64 = 7_000_000;

/// SplitMix64: a small seeded generator, so a failure names the seed that reproduces it.
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
    /// Whether the rows are compared with their rowids.
    with_rowid: bool,
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
                    &format!(
                        "SELECT {}k, u, v FROM {} ORDER BY v",
                        if self.with_rowid { "rowid, " } else { "" },
                        self.table
                    ),
                    &[]
                )
                .unwrap()
        )
    }
}

struct Outcome {
    ok: bool,
    changes: i64,
    last_rowid: i64,
    in_tx: bool,
    rows: String,
}

impl Outcome {
    fn new(side: &Side, ok: bool) -> Self {
        let in_tx = if side.conn.execute_batch("BEGIN").is_ok() {
            side.conn.execute_batch("ROLLBACK").unwrap();
            false
        } else {
            true
        };
        Self {
            ok,
            changes: side.int("SELECT changes()"),
            last_rowid: side.int("SELECT last_insert_rowid()"),
            in_tx,
            rows: side.rows(),
        }
    }

    fn describe(&self) -> String {
        format!(
            "ok={} changes={} last_rowid={} in_tx={} rows={}",
            self.ok, self.changes, self.last_rowid, self.in_tx, self.rows
        )
    }
}

struct Lab {
    native: Side,
    aydb: Side,
    _dir: TempTestDir,
}

type Row = (i64, String, i64);

impl Lab {
    /// Both tables hold `rows` as `(key number, u, v)` with the same rowids: 1, 2, ... for
    /// mapped keys, the key itself for integer keys.
    fn new(spec: &Spec, rows: &[Row]) -> Self {
        let dir = TempTestDir::new("pk_update");
        let fs = Arc::new(OsFs::open(&dir.path, true).unwrap());
        let mut config = EngineConfig::default();
        config.sync_wal_size_bytes = 64 * 4096;
        config.buf_wal_size_bytes = 64 * 4096;
        let engine = Arc::new(RwLock::new(StorageEngine::open(fs, config).unwrap()));
        let aydb = SqliteConnection::open_in_memory().unwrap();
        aydb.register_aydb_module_with_options(engine, false)
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
        for (conn, table) in [(&native, "n"), (&aydb, "a")] {
            conn.execute_batch("CREATE TABLE scratch(id INTEGER PRIMARY KEY)")
                .unwrap();
            conn.execute_batch("BEGIN").unwrap();
            for (i, (k, u, v)) in rows.iter().enumerate() {
                let rowid = if spec.int_key { *k } else { i as i64 + 1 };
                conn.execute_batch(&format!(
                    "INSERT INTO {table}(rowid, k, u, v) VALUES({rowid}, {}, {u}, {v})",
                    key(spec, *k)
                ))
                .unwrap();
            }
            conn.execute_batch(&format!("INSERT INTO scratch VALUES({SENTINEL})"))
                .unwrap();
        }
        Self {
            native: Side {
                conn: native,
                table: "n",
                with_rowid: !spec.rowid_is_key,
            },
            aydb: Side {
                conn: aydb,
                table: "a",
                with_rowid: !spec.rowid_is_key,
            },
            _dir: dir,
        }
    }

    fn run(&self, stmt: &str) -> (Outcome, Outcome) {
        let mut out = Vec::new();
        for side in [&self.native, &self.aydb] {
            let ok = side
                .conn
                .execute_batch(&stmt.replace("{t}", side.table))
                .is_ok();
            out.push(Outcome::new(side, ok));
        }
        let aydb = out.pop().unwrap();
        (out.pop().unwrap(), aydb)
    }
}

/// A random table of 2..=6 rows and a random PK-changing UPDATE over it.
fn scenario(spec: &Spec, mode: &str, seed: u64) -> (Vec<Row>, String) {
    let mut rng = Rng(seed.wrapping_mul(0x1234_5678_9ABC_DEF1) ^ 0xA5A5);
    let n = 2 + rng.below(5) as i64;
    let rows: Vec<Row> = (1..=n).map(|i| (i, format!("'u{i}'"), i * 10)).collect();
    let (filter, queued): (String, Vec<i64>) = match rng.below(3) {
        0 => {
            let limit = 1 + rng.below(n as u64) as i64;
            (format!(" WHERE v <= {}", limit * 10), (1..=limit).collect())
        }
        1 => (
            " WHERE v % 20 = 0".to_string(),
            (1..=n).filter(|i| i % 2 == 0).collect(),
        ),
        _ => (String::new(), (1..=n).collect()),
    };
    // Target key of each row: keep, another existing key, or a key not yet used.
    let mut key_cases = String::new();
    for (k, _, _) in &rows {
        let mut target = match rng.below(4) {
            0 => *k,
            1 | 2 => 1 + rng.below(n as u64) as i64,
            _ => n + 1 + rng.below(3) as i64,
        };
        if spec.key_is_rowid && target != *k && queued.contains(&target) {
            target = n + 1 + rng.below(3) as i64;
        }
        key_cases.push_str(&format!(
            " WHEN {} THEN {}",
            key(spec, *k),
            key(spec, target)
        ));
    }
    let mut sets = format!("k = CASE k{key_cases} ELSE k END");
    match rng.below(4) {
        0 => {
            // Collide on the UNIQUE column too.
            sets.push_str(&format!(", u = 'u' || ((v / 10) % {})", 1 + rng.below(3)));
        }
        1 => sets.push_str(", v = v + 1"),
        _ => {}
    }
    (rows, format!("UPDATE {mode} {{t}} SET {sets}{filter}"))
}

/// Compares the two outcomes. `changes()` of the adapter may be higher by the rows that an
/// earlier REPLACE removed while they were still queued: SQLite counts every `xUpdate` call
/// that returns OK, a native table skips those rows. Returns whether the counts differ.
fn same_outcome(native: &Outcome, aydb: &Outcome) -> Result<bool, String> {
    if native.ok != aydb.ok
        || native.last_rowid != aydb.last_rowid
        || native.in_tx != aydb.in_tx
        || native.rows != aydb.rows
    {
        return Err("outcome".to_string());
    }
    if aydb.changes < native.changes {
        return Err("changes".to_string());
    }
    Ok(aydb.changes != native.changes)
}

#[test]
fn random_pk_changing_updates_match_native() {
    let mut failures = Vec::new();
    let mut counted_higher = 0;
    for spec in &SPECS {
        for mode in MODES {
            for seed in 0..24u64 {
                let (rows, stmt) = scenario(spec, mode, seed);
                let lab = Lab::new(spec, &rows);
                let (native, aydb) = lab.run(&stmt);
                match same_outcome(&native, &aydb) {
                    Ok(higher) => counted_higher += usize::from(higher),
                    Err(what) => failures.push(format!(
                        "[{} seed {seed}] {what}: {stmt}\n  native: {}\n  aydb:   {}",
                        spec.name,
                        native.describe(),
                        aydb.describe()
                    )),
                }
            }
        }
    }
    println!("statements with a higher aydb changes(): {counted_higher}");
    assert!(
        failures.is_empty(),
        "{} failures:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

fn plain_rows(n: i64) -> Vec<Row> {
    (1..=n).map(|i| (i, format!("'u{i}'"), i * 10)).collect()
}

/// A row that an earlier REPLACE removed is still queued: SQLite calls `xUpdate` for it anyway
/// and counts the call in `changes()`, while a native table skips the row. This is the one
/// accepted difference; rows and rowids still agree.
#[test]
fn a_queued_row_removed_by_replace_is_skipped_but_counted() {
    for spec in [&SPECS[0], &SPECS[1], &SPECS[4]] {
        let lab = Lab::new(spec, &plain_rows(2));
        let stmt = format!(
            "UPDATE OR REPLACE {{t}} SET k = CASE k WHEN {} THEN {} ELSE {} END",
            key(spec, 1),
            key(spec, 2),
            key(spec, 3)
        );
        let (native, aydb) = lab.run(&stmt);
        assert!(native.ok && aydb.ok, "{}", spec.name);
        // The first row takes the key of the second and keeps its own rowid 1.
        assert_eq!(native.rows, aydb.rows, "{}", spec.name);
        assert!(
            native
                .rows
                .contains("Integer(1), Text(\"k2\"), Text(\"u1\")")
        );
        assert_eq!(native.changes, 1, "{}", spec.name);
        assert_eq!(aydb.changes, 2, "{}", spec.name);
    }
}

/// Integer keys: a collision with a row that is not queued matches native (the example of the
/// original report), also when several queued rows collide with each other on a fresh key.
#[test]
fn integer_key_replace_collisions_match_native() {
    for spec in [&SPECS[2], &SPECS[3]] {
        for stmt in [
            "UPDATE OR REPLACE {t} SET k = 3 WHERE k = 1",
            "UPDATE OR REPLACE {t} SET k = 9 WHERE k IN (1, 2)",
            "UPDATE OR IGNORE {t} SET k = 3 WHERE k = 1",
            "UPDATE OR FAIL {t} SET k = k + 10 WHERE k >= 2",
        ] {
            let rows: Vec<Row> = [(1, 10), (3, 20)]
                .map(|(k, v)| (k, format!("'u{k}'"), v))
                .to_vec();
            let lab = Lab::new(spec, &rows);
            let (native, aydb) = lab.run(stmt);
            assert!(same_outcome(&native, &aydb).is_ok(), "{} {stmt}", spec.name);
            assert_eq!(native.changes, aydb.changes, "{} {stmt}", spec.name);
        }
    }
}

/// Divergence that cannot be removed: on a key that is its own rowid, an earlier REPLACE moves
/// a row onto the rowid of a row that SQLite still has queued. A native table then visits the
/// moved row; a virtual table gets the new values SQLite computed for the replaced row.
#[test]
fn key_that_is_its_rowid_moves_onto_a_queued_rowid() {
    let spec = &SPECS[2];
    let lab = Lab::new(spec, &plain_rows(3));
    let (native, aydb) = lab.run(
        "UPDATE OR REPLACE {t} SET k = CASE k WHEN 1 THEN 2 WHEN 2 THEN 5 WHEN 3 THEN 1 ELSE k END \
         WHERE v <= 20",
    );
    assert!(native.ok && aydb.ok);
    // Native: row 1 moves to 2 (replacing row 2), then the moved row is visited again and
    // moves on to 5. Virtual table: the values computed for the replaced row 2 land on rowid 2.
    assert!(native.rows.contains("Integer(5), Integer(5), Text(\"u1\")"));
    assert!(aydb.rows.contains("Integer(5), Integer(5), Text(\"u2\")"));
}

/// `INT PRIMARY KEY` is not a rowid alias on a native table, but the adapter derives the rowid
/// from the key: after a key change the rowid is the new key where native keeps the old one.
#[test]
fn integer_key_is_its_rowid_after_an_update() {
    let spec = &SPECS[3];
    let lab = Lab::new(spec, &plain_rows(2));
    let stmt = "UPDATE {t} SET k = 7 WHERE k = 1";
    for side in [&lab.native, &lab.aydb] {
        side.conn
            .execute_batch(&stmt.replace("{t}", side.table))
            .unwrap();
    }
    let rowid = |side: &Side| side.int(&format!("SELECT rowid FROM {} WHERE k = 7", side.table));
    assert_eq!(rowid(&lab.native), 1);
    assert_eq!(rowid(&lab.aydb), 7);
}
