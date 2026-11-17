//! Statement conflict modes (`OR ABORT`, `OR FAIL`, `OR IGNORE`, `OR ROLLBACK`, `OR REPLACE`)
//! on an aydb table, compared with a native table.
//!
//! The aydb table lives in one connection and the native table in another, both holding the
//! same schema. `OR ROLLBACK` ends the whole transaction, so the two tables cannot share a
//! connection: a rollback triggered by the native statement would also end the transaction
//! the aydb statement is meant to run in. After every statement the two connections must agree
//! on the outcome, the rows, `changes()`, `total_changes()`, `last_insert_rowid()` and whether
//! a transaction is still open.

use super::*;
use crate::engine::EngineConfig;
use crate::util::test_util::TempTestDir;

struct Variant {
    name: &'static str,
    /// Column definitions shared by the native and the aydb table.
    columns: &'static str,
    /// Extra module options of the aydb table.
    options: &'static str,
    /// The primary key is `INTEGER PRIMARY KEY`, so rowids are comparable exactly.
    int_pk: bool,
    /// The primary key holds integers (otherwise text).
    int_keys: bool,
}

const VARIANTS: [Variant; 4] = [
    Variant {
        name: "plain",
        columns: "k TEXT PRIMARY KEY, u TEXT, v INT NOT NULL",
        options: "",
        int_pk: false,
        int_keys: false,
    },
    Variant {
        name: "unique",
        columns: "k TEXT PRIMARY KEY, u TEXT UNIQUE, v INT NOT NULL",
        options: "",
        int_pk: false,
        int_keys: false,
    },
    Variant {
        name: "capped",
        columns: "k TEXT PRIMARY KEY, u TEXT UNIQUE, v INT NOT NULL",
        options: "max_bytes=100000, ",
        int_pk: false,
        int_keys: false,
    },
    Variant {
        name: "integer_pk",
        columns: "k INTEGER PRIMARY KEY, u TEXT UNIQUE, v INT NOT NULL",
        options: "",
        int_pk: true,
        int_keys: true,
    },
];

const MODES: [&str; 6] = [
    "",
    "OR ABORT",
    "OR FAIL",
    "OR IGNORE",
    "OR ROLLBACK",
    "OR REPLACE",
];

/// A key literal of the variant.
fn key(variant: &Variant, n: u32) -> String {
    if variant.int_keys {
        n.to_string()
    } else {
        format!("'k{n}'")
    }
}

/// The `last_insert_rowid()` that a statement which inserts nothing leaves in place.
const SENTINEL: i64 = 7_000_000;

struct Side {
    conn: SqliteConnection,
    table: String,
}

impl Side {
    fn counters(&self) -> (i64, i64, i64) {
        let row = &self
            .conn
            .query_all(
                "SELECT changes(), total_changes(), last_insert_rowid()",
                &[],
            )
            .unwrap()[0];
        let int = |i: usize| match row[i] {
            SqlValue::Integer(v) => v,
            ref other => panic!("unexpected counter {other:?}"),
        };
        (int(0), int(1), int(2))
    }

    /// Whether a transaction is open: `BEGIN` fails inside one.
    fn in_transaction(&self) -> bool {
        if self.conn.execute_batch("BEGIN").is_ok() {
            self.conn.execute_batch("ROLLBACK").unwrap();
            false
        } else {
            true
        }
    }
}

struct Session {
    label: String,
    int_pk: bool,
    native: Side,
    aydb: Side,
    history: Vec<String>,
    diffs: Vec<String>,
    /// Print every statement with both sides' rows (`AYDB_CONFLICT_TRACE`).
    trace: bool,
}

/// One engine shared by the sessions of a test; each session gets a table id of its own.
struct Lab {
    engine: Arc<RwLock<StorageEngine<OsFs>>>,
    next_table_id: u16,
    /// Declared last so that it is removed after the engine has closed its files.
    _dir: TempTestDir,
}

impl Lab {
    fn new(variant: &Variant) -> Self {
        let dir = TempTestDir::new(&format!("conflict_{}", variant.name));
        let fs = Arc::new(OsFs::open(&dir.path, true).unwrap());
        // Small logs: the default ones pre-allocate 24 MB.
        let mut config = EngineConfig::default();
        config.sync_wal_size_bytes = 64 * 4096;
        config.buf_wal_size_bytes = 64 * 4096;
        Self {
            engine: Arc::new(RwLock::new(StorageEngine::open(fs, config).unwrap())),
            next_table_id: 1,
            _dir: dir,
        }
    }
}

impl Session {
    fn new(lab: &mut Lab, variant: &Variant, label: String) -> Self {
        let table_id = lab.next_table_id;
        lab.next_table_id += 1;
        let engine = lab.engine.clone();
        let aydb = SqliteConnection::open_in_memory().unwrap();
        aydb.register_aydb_module_with_options(engine, false)
            .unwrap();
        aydb.execute_batch(&format!(
            "CREATE VIRTUAL TABLE a{table_id} USING aydb(table_id={table_id}, {}columns='{}')",
            variant.options, variant.columns
        ))
        .unwrap();
        let native = SqliteConnection::open_in_memory().unwrap();
        native
            .execute_batch(&format!("CREATE TABLE n({})", variant.columns))
            .unwrap();
        // Source rows of INSERT ... SELECT, each with one violation in the middle.
        for conn in [&native, &aydb] {
            conn.execute_batch("CREATE TABLE scratch(id INTEGER PRIMARY KEY)")
                .unwrap();
            for (name, rows) in [
                (
                    "src_pk",
                    [(10, "u10", "1"), (1, "u11", "2"), (12, "u12", "3")],
                ),
                (
                    "src_uq",
                    [(10, "u10", "1"), (21, "u2", "2"), (12, "u12", "3")],
                ),
                (
                    "src_nn",
                    [(10, "u10", "1"), (31, "u31", "NULL"), (12, "u12", "3")],
                ),
            ] {
                conn.execute_batch(&format!("CREATE TABLE {name}(k, u, v)"))
                    .unwrap();
                for (k, u, v) in rows {
                    conn.execute_batch(&format!(
                        "INSERT INTO {name} VALUES({}, '{u}', {v})",
                        key(variant, k)
                    ))
                    .unwrap();
                }
            }
        }
        let mut session = Self {
            label,
            int_pk: variant.int_pk,
            native: Side {
                conn: native,
                table: "n".to_string(),
            },
            aydb: Side {
                conn: aydb,
                table: format!("a{table_id}"),
            },
            history: Vec::new(),
            diffs: Vec::new(),
            trace: std::env::var_os("AYDB_CONFLICT_TRACE").is_some(),
        };
        for n in 1..=3 {
            session.run(&format!(
                "INSERT INTO {{T}} VALUES({}, 'u{n}', {n})",
                key(variant, n)
            ));
        }
        session
    }

    fn rows(&self, side: &Side) -> Vec<Vec<SqlValue>> {
        let rowid = if self.int_pk { ", rowid" } else { "" };
        side.conn
            .query_all(
                &format!("SELECT k, u, v{rowid} FROM {} ORDER BY k", side.table),
                &[],
            )
            .unwrap()
    }

    /// Runs a statement (`{T}` is the table) on both sides and records every disagreement.
    ///
    /// A write is preceded by one insert into a native scratch table, which sets
    /// `last_insert_rowid()` to a known value on both sides; whether the statement left it
    /// alone is part of what is compared.
    fn run(&mut self, template: &str) {
        self.history.push(template.to_string());
        let is_write = ["INSERT", "UPDATE", "DELETE"]
            .iter()
            .any(|w| template.starts_with(w));
        if is_write {
            for side in [&self.native, &self.aydb] {
                side.conn
                    .execute_batch(&format!(
                        "INSERT OR REPLACE INTO scratch VALUES({SENTINEL})"
                    ))
                    .unwrap();
            }
        }
        let total_before = [self.native.counters().1, self.aydb.counters().1];
        let outcome = |side: &Side| {
            let sql = template.replace("{T}", &side.table);
            side.conn.execute_batch(&sql).map_err(|e| e.to_string())
        };
        let n_out = outcome(&self.native);
        let a_out = outcome(&self.aydb);
        let mut found = Vec::new();
        if n_out.is_ok() != a_out.is_ok() {
            found.push(format!("outcome native={n_out:?} aydb={a_out:?}"));
        }
        let (n_ch, n_total, n_rowid) = self.native.counters();
        let (a_ch, a_total, a_rowid) = self.aydb.counters();
        if n_ch != a_ch {
            found.push(format!("changes() native={n_ch} aydb={a_ch}"));
        }
        if n_total - total_before[0] != a_total - total_before[1] {
            found.push(format!(
                "total_changes() grew by native={} aydb={}",
                n_total - total_before[0],
                a_total - total_before[1]
            ));
        }
        if is_write && (n_rowid == SENTINEL) != (a_rowid == SENTINEL) {
            found.push(format!(
                "last_insert_rowid() native={n_rowid} aydb={a_rowid} (untouched is {SENTINEL})"
            ));
        } else if self.int_pk && n_rowid != a_rowid {
            found.push(format!(
                "last_insert_rowid() native={n_rowid} aydb={a_rowid}"
            ));
        }
        let (n_tx, a_tx) = (self.native.in_transaction(), self.aydb.in_transaction());
        if n_tx != a_tx {
            found.push(format!("transaction open native={n_tx} aydb={a_tx}"));
        }
        let (n_rows, a_rows) = (self.rows(&self.native), self.rows(&self.aydb));
        if n_rows != a_rows {
            found.push(format!("rows native={n_rows:?} aydb={a_rows:?}"));
        }
        if self.trace {
            println!(
                "{} | {template}
  native {n_out:?} {n_rows:?}
  aydb   {a_out:?} {a_rows:?}",
                self.label
            );
        }
        for f in found {
            self.diffs.push(format!(
                "{} | after `{template}` | {f} | history {:?}",
                self.label, self.history
            ));
        }
    }
}

/// Statements that violate one constraint, with the conflict clause `{M}` to fill in. The
/// table starts with the keys 1 to 3 (`u1` to `u3`, `v` equal to the key).
fn violations(v: &Variant) -> Vec<(&'static str, String)> {
    let k = |n| key(v, n);
    vec![
        (
            "insert pk",
            format!("INSERT {{M}} INTO {{T}} VALUES({}, 'ux', 9)", k(1)),
        ),
        (
            "insert pk multi",
            format!(
                "INSERT {{M}} INTO {{T}} VALUES({}, 'u10', 1), ({}, 'u11', 2), ({}, 'u12', 3)",
                k(10),
                k(1),
                k(12)
            ),
        ),
        (
            "insert pk select",
            "INSERT {M} INTO {T} SELECT k, u, v FROM src_pk".to_string(),
        ),
        (
            "insert unique",
            format!("INSERT {{M}} INTO {{T}} VALUES({}, 'u1', 9)", k(20)),
        ),
        (
            "insert unique multi",
            format!(
                "INSERT {{M}} INTO {{T}} VALUES({}, 'u10', 1), ({}, 'u2', 2), ({}, 'u12', 3)",
                k(10),
                k(21),
                k(12)
            ),
        ),
        (
            "insert unique select",
            "INSERT {M} INTO {T} SELECT k, u, v FROM src_uq".to_string(),
        ),
        (
            "insert notnull",
            format!("INSERT {{M}} INTO {{T}} VALUES({}, 'u30', NULL)", k(30)),
        ),
        (
            "insert notnull multi",
            format!(
                "INSERT {{M}} INTO {{T}} VALUES({}, 'u10', 1), ({}, 'u31', NULL), ({}, 'u12', 3)",
                k(10),
                k(31),
                k(12)
            ),
        ),
        (
            "insert notnull select",
            "INSERT {M} INTO {T} SELECT k, u, v FROM src_nn".to_string(),
        ),
        (
            "update pk",
            format!("UPDATE {{M}} {{T}} SET k = {} WHERE k = {}", k(1), k(2)),
        ),
        (
            "update pk multi",
            format!("UPDATE {{M}} {{T}} SET k = {}", k(9)),
        ),
        (
            "update unique",
            format!("UPDATE {{M}} {{T}} SET u = 'u1' WHERE k = {}", k(2)),
        ),
        (
            "update unique multi",
            "UPDATE {M} {T} SET u = 'ux'".to_string(),
        ),
        (
            "update notnull",
            format!("UPDATE {{M}} {{T}} SET v = NULL WHERE k = {}", k(2)),
        ),
        (
            "update notnull multi",
            format!(
                "UPDATE {{M}} {{T}} SET v = CASE k WHEN {} THEN NULL ELSE 7 END",
                k(2)
            ),
        ),
    ]
}

/// Transaction shapes around the statement under test (`{S}`).
fn contexts(v: &Variant) -> Vec<(&'static str, Vec<String>)> {
    let prior = |n: u32| format!("INSERT INTO {{T}} VALUES({}, 'u{n}', {n})", key(v, n));
    let s = |list: &[&str]| list.iter().map(|x| x.to_string()).collect::<Vec<_>>();
    vec![
        ("autocommit", s(&["{S}"])),
        ("first in tx", s(&["BEGIN", "{S}", "COMMIT"])),
        (
            "after a write in tx",
            vec![
                "BEGIN".into(),
                prior(50),
                "{S}".into(),
                prior(51),
                "COMMIT".into(),
            ],
        ),
        (
            "nested, rollback to inner",
            vec![
                "BEGIN".into(),
                "SAVEPOINT s1".into(),
                prior(50),
                "SAVEPOINT s2".into(),
                prior(51),
                "{S}".into(),
                "ROLLBACK TO s2".into(),
                prior(52),
                "RELEASE s1".into(),
                "COMMIT".into(),
            ],
        ),
        (
            "nested, release then rollback to outer",
            vec![
                "BEGIN".into(),
                "SAVEPOINT s1".into(),
                prior(50),
                "SAVEPOINT s2".into(),
                prior(51),
                "{S}".into(),
                "RELEASE s2".into(),
                "ROLLBACK TO s1".into(),
                prior(52),
                "COMMIT".into(),
            ],
        ),
        (
            "savepoint without BEGIN",
            s(&[
                "SAVEPOINT s1",
                "SAVEPOINT s2",
                "{S}",
                "ROLLBACK TO s1",
                "RELEASE s1",
            ]),
        ),
    ]
}

fn check_variant(variant: &Variant) {
    let mut lab = Lab::new(variant);
    let mut diffs = Vec::new();
    let mut cases = 0usize;
    for mode in MODES {
        for (what, stmt) in violations(variant) {
            let stmt = stmt.replace("{M}", mode);
            for (ctx_name, ctx) in contexts(variant) {
                cases += 1;
                let mut session = Session::new(
                    &mut lab,
                    variant,
                    format!("{}/{mode:?}/{what}/{ctx_name}", variant.name),
                );
                for step in &ctx {
                    session.run(&step.replace("{S}", &stmt));
                }
                diffs.append(&mut session.diffs);
            }
        }
    }
    assert!(
        diffs.is_empty(),
        "{} differences in {cases} scenarios of `{}` from native SQLite; first {}:\n{}",
        diffs.len(),
        variant.name,
        diffs.len().min(25),
        diffs
            .iter()
            .take(25)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n")
    );
}

#[test]
fn conflict_modes_match_native_on_a_plain_table() {
    check_variant(&VARIANTS[0]);
}

#[test]
fn conflict_modes_match_native_on_a_unique_table() {
    check_variant(&VARIANTS[1]);
}

#[test]
fn conflict_modes_match_native_on_a_capped_table() {
    check_variant(&VARIANTS[2]);
}

#[test]
fn conflict_modes_match_native_on_an_integer_primary_key_table() {
    check_variant(&VARIANTS[3]);
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn all_or_nothing(rng: &mut Rng) -> &'static str {
    ["", "OR ABORT", "OR ROLLBACK"][rng.below(3) as usize]
}

/// A random statement over a small key domain, so that conflicts are common.
fn random_statement(v: &Variant, rng: &mut Rng, depth: &mut u32, in_tx: &mut bool) -> String {
    let mode = MODES[rng.below(MODES.len() as u64) as usize];
    let k = |rng: &mut Rng| key(v, 1 + rng.below(6) as u32);
    let u = |rng: &mut Rng| match rng.below(7) {
        0 => "NULL".to_string(),
        n => format!("'u{n}'"),
    };
    let val = |rng: &mut Rng| match rng.below(7) {
        0 => "NULL".to_string(),
        n => n.to_string(),
    };
    match rng.below(14) {
        0..=3 => {
            let rows = 1 + rng.below(3);
            let list: Vec<String> = (0..rows)
                .map(|_| format!("({}, {}, {})", k(rng), u(rng), val(rng)))
                .collect();
            format!("INSERT {mode} INTO {{T}} VALUES {}", list.join(", "))
        }
        4 => {
            let src = ["src_pk", "src_uq", "src_nn"][rng.below(3) as usize];
            format!("INSERT {mode} INTO {{T}} SELECT k, u, v FROM {src}")
        }
        5 => format!(
            "UPDATE {mode} {{T}} SET k = {} WHERE k = {}",
            k(rng),
            k(rng)
        ),
        6 => format!(
            "UPDATE {mode} {{T}} SET u = {} WHERE k = {}",
            u(rng),
            k(rng)
        ),
        // These may touch several rows. A native table visits them in rowid order and aydb in
        // key order, which changes what a partial statement leaves behind, so only the modes
        // that undo the whole statement are used.
        7 => format!(
            "UPDATE {} {{T}} SET v = {} WHERE v = {}",
            all_or_nothing(rng),
            val(rng),
            val(rng)
        ),
        8 => format!("UPDATE {} {{T}} SET u = {}", all_or_nothing(rng), u(rng)),
        9 => format!("DELETE FROM {{T}} WHERE k = {}", k(rng)),
        10 => {
            if *in_tx {
                *in_tx = false;
                *depth = 0;
                if rng.below(2) == 0 {
                    "COMMIT"
                } else {
                    "ROLLBACK"
                }
                .to_string()
            } else {
                *in_tx = true;
                "BEGIN".to_string()
            }
        }
        11 => {
            *depth += 1;
            format!("SAVEPOINT s{}", *depth)
        }
        12 if *depth > 0 => {
            let target = 1 + rng.below(*depth as u64) as u32;
            if rng.below(2) == 0 {
                *depth = target - 1;
                format!("RELEASE s{target}")
            } else {
                *depth = target;
                format!("ROLLBACK TO s{target}")
            }
        }
        _ => format!(
            "INSERT {mode} INTO {{T}} VALUES ({}, {}, {})",
            k(rng),
            u(rng),
            val(rng)
        ),
    }
}

fn random_differential(variant: &Variant, seeds: std::ops::Range<u64>) {
    // `AYDB_CONFLICT_SEED=<n>` replays one seed.
    let seeds = match std::env::var("AYDB_CONFLICT_SEED")
        .ok()
        .and_then(|v| v.parse().ok())
    {
        Some(seed) => seed..seed + 1,
        None => seeds,
    };
    let mut lab = Lab::new(variant);
    let mut diffs = Vec::new();
    for seed in seeds {
        let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
        let mut session = Session::new(&mut lab, variant, format!("{}/seed {seed}", variant.name));
        let (mut depth, mut in_tx) = (0u32, false);
        for _ in 0..40 {
            let sql = random_statement(variant, &mut rng, &mut depth, &mut in_tx);
            session.run(&sql);
            // The generator's view of the transaction may be stale after OR ROLLBACK or an
            // error; resynchronize it from the native side.
            in_tx = session.native.in_transaction();
            if !in_tx {
                depth = 0;
            }
            if !session.diffs.is_empty() {
                break;
            }
        }
        diffs.append(&mut session.diffs);
    }
    assert!(
        diffs.is_empty(),
        "{} differences in the random runs of `{}` from native SQLite; first {}:\n{}",
        diffs.len(),
        variant.name,
        diffs.len().min(15),
        diffs
            .iter()
            .take(15)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n")
    );
}

#[test]
fn random_statements_match_native_on_a_plain_table() {
    random_differential(&VARIANTS[0], 0..200);
}

#[test]
fn random_statements_match_native_on_a_unique_table() {
    random_differential(&VARIANTS[1], 0..200);
}

#[test]
fn random_statements_match_native_on_a_capped_table() {
    random_differential(&VARIANTS[2], 0..200);
}

#[test]
fn random_statements_match_native_on_an_integer_primary_key_table() {
    random_differential(&VARIANTS[3], 0..200);
}
