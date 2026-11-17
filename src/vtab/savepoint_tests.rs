//! Nested savepoints through SQLite, compared with a native table in the same connection.
//!
//! SQLite tells a virtual table about a savepoint only once the table has joined the
//! transaction (`xBegin` is followed by one `xSavepoint` for the innermost open savepoint), and
//! it numbers savepoints by their depth. A `ROLLBACK TO` an outer savepoint therefore names an
//! id the table has never been told about. Every statement of every scenario runs on a native
//! table `n` and on the aydb table `a` of the same connection, and the rows must agree after
//! each one.

use super::*;
use crate::engine::EngineConfig;
use crate::util::test_util::TempTestDir;

struct Variant {
    name: &'static str,
    native_ddl: &'static str,
    aydb_ddl: &'static str,
    /// A value that makes any write of it fail in the aydb table only.
    big: usize,
}

const VARIANTS: [Variant; 3] = [
    Variant {
        name: "plain",
        native_ddl: "CREATE TABLE n(k TEXT PRIMARY KEY, u TEXT, v TEXT)",
        aydb_ddl: "CREATE VIRTUAL TABLE a USING aydb(table_id=1, \
                   columns='k TEXT PRIMARY KEY, u TEXT, v TEXT')",
        big: 6000,
    },
    Variant {
        name: "unique",
        native_ddl: "CREATE TABLE n(k TEXT PRIMARY KEY, u TEXT UNIQUE, v TEXT)",
        aydb_ddl: "CREATE VIRTUAL TABLE a USING aydb(table_id=1, \
                   columns='k TEXT PRIMARY KEY, u TEXT UNIQUE, v TEXT')",
        big: 6000,
    },
    Variant {
        name: "capped",
        native_ddl: "CREATE TABLE n(k TEXT PRIMARY KEY, u TEXT UNIQUE, v TEXT)",
        aydb_ddl: "CREATE VIRTUAL TABLE a USING aydb(table_id=1, max_bytes=3000, \
                   columns='k TEXT PRIMARY KEY, u TEXT UNIQUE, v TEXT')",
        big: 3500,
    },
];

struct Session {
    conn: SqliteConnection,
    big: String,
    label: String,
    history: Vec<String>,
    // Declared last so the directory is removed after the connection has closed its files.
    _dir: TempTestDir,
}

impl Session {
    fn new(variant: &Variant, label: &str) -> Self {
        let dir = TempTestDir::new(&format!("savepoints_{}_{label}", variant.name));
        let fs = Arc::new(OsFs::open(&dir.path, true).unwrap());
        let mut cfg = EngineConfig::default();
        cfg.max_transaction_bytes = 4096;
        let engine = Arc::new(RwLock::new(StorageEngine::open(fs, cfg).unwrap()));
        let conn = SqliteConnection::open_in_memory().unwrap();
        conn.register_aydb_module_with_options(engine, false)
            .unwrap();
        conn.execute_batch(variant.native_ddl).unwrap();
        conn.execute_batch(variant.aydb_ddl).unwrap();
        Self {
            _dir: dir,
            conn,
            big: "b".repeat(variant.big),
            label: format!("{}/{label}", variant.name),
            history: Vec::new(),
        }
    }

    fn rows(&self, table: &str) -> Vec<Vec<SqlValue>> {
        self.conn
            .query_all(&format!("SELECT k, u, v FROM {table} ORDER BY k"), &[])
            .unwrap()
    }

    /// Runs one statement and checks that both tables agree afterwards.
    ///
    /// A statement with `{T}` runs on the native table and on the aydb table. A statement that
    /// starts with `!` is a write that must fail in the aydb table only (it exceeds a limit the
    /// native table does not have); it is not run on the native table. Anything else is a
    /// transaction control statement and runs once. Returns whether it succeeded.
    fn run(&mut self, sql: &str) -> bool {
        let sql = sql.replace("{BIG}", &self.big);
        self.history.push(sql.clone());
        let ok = if let Some(failing) = sql.strip_prefix('!') {
            let result = self.conn.execute_batch(&failing.replace("{T}", "a"));
            assert!(result.is_err(), "{}: expected a failure: {sql}", self.label);
            false
        } else if sql.contains("{T}") {
            let native = self.conn.execute_batch(&sql.replace("{T}", "n"));
            let aydb = self.conn.execute_batch(&sql.replace("{T}", "a"));
            assert_eq!(
                native.is_ok(),
                aydb.is_ok(),
                "{}: {sql}\n native: {native:?}\n aydb: {aydb:?}\n history: {:?}",
                self.label,
                self.history
            );
            native.is_ok()
        } else {
            self.conn.execute_batch(&sql).is_ok()
        };
        self.compare(&sql);
        ok
    }

    fn compare(&self, after: &str) {
        assert_eq!(
            self.rows("n"),
            self.rows("a"),
            "{}: rows differ after: {after}\n history: {:?}",
            self.label,
            self.history
        );
    }
}

fn ins(n: u32) -> String {
    format!("INSERT INTO {{T}} VALUES('k{n}','u{n}','v{n}')")
}

fn scenarios() -> Vec<(&'static str, Vec<String>)> {
    let big_replace = "!INSERT OR REPLACE INTO {T} VALUES('k1','u2','{BIG}')".to_string();
    let to_s = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    let join = |parts: Vec<Vec<String>>| parts.concat();
    vec![
        (
            "rollback to the outer savepoint",
            join(vec![
                to_s(&["BEGIN", "SAVEPOINT s1", "SAVEPOINT s2"]),
                vec![ins(1)],
                to_s(&["ROLLBACK TO s1"]),
                vec![ins(2)],
            ]),
        ),
        (
            "inner then outer",
            join(vec![
                to_s(&["BEGIN", "SAVEPOINT s1"]),
                vec![ins(1)],
                to_s(&["SAVEPOINT s2"]),
                vec![ins(2)],
                to_s(&["ROLLBACK TO s2"]),
                vec![ins(3)],
                to_s(&["ROLLBACK TO s1"]),
            ]),
        ),
        (
            "release inner then rollback outer",
            join(vec![
                to_s(&["BEGIN", "SAVEPOINT s1", "SAVEPOINT s2"]),
                vec![ins(1)],
                to_s(&["RELEASE s2"]),
                vec![ins(2)],
                to_s(&["ROLLBACK TO s1"]),
            ]),
        ),
        (
            "same name opened again",
            join(vec![
                to_s(&["BEGIN", "SAVEPOINT s"]),
                vec![ins(1)],
                to_s(&["SAVEPOINT s"]),
                vec![ins(2)],
                to_s(&["ROLLBACK TO s"]),
                vec![ins(3)],
                to_s(&["RELEASE s", "ROLLBACK TO s"]),
                vec![ins(4)],
                to_s(&["RELEASE s"]),
            ]),
        ),
        (
            "three levels, first write at the innermost",
            join(vec![
                to_s(&["BEGIN", "SAVEPOINT a", "SAVEPOINT b", "SAVEPOINT c"]),
                vec![ins(1)],
                to_s(&["ROLLBACK TO b"]),
                vec![ins(2)],
                to_s(&["SAVEPOINT c"]),
                vec![ins(3)],
                to_s(&["ROLLBACK TO a"]),
                vec![ins(4)],
                to_s(&["RELEASE a"]),
            ]),
        ),
        (
            "three levels, middle then outer",
            join(vec![
                to_s(&["BEGIN", "SAVEPOINT a"]),
                vec![ins(1)],
                to_s(&["SAVEPOINT b", "SAVEPOINT c"]),
                vec![ins(2)],
                to_s(&["ROLLBACK TO b"]),
                vec![ins(3)],
                to_s(&["ROLLBACK TO a"]),
            ]),
        ),
        (
            "three levels, rollback to the middle first written at the innermost",
            join(vec![
                to_s(&["BEGIN", "SAVEPOINT a", "SAVEPOINT b", "SAVEPOINT c"]),
                vec![ins(1)],
                to_s(&["ROLLBACK TO c"]),
                vec![ins(2)],
                to_s(&["ROLLBACK TO b"]),
                vec![ins(3)],
                to_s(&["RELEASE b", "ROLLBACK TO a"]),
            ]),
        ),
        (
            "outermost savepoint without BEGIN",
            join(vec![
                to_s(&["SAVEPOINT s1", "SAVEPOINT s2"]),
                vec![ins(1)],
                to_s(&["ROLLBACK TO s1"]),
                vec![ins(2)],
                to_s(&["SAVEPOINT s3"]),
                vec![ins(3)],
                to_s(&["ROLLBACK TO s1"]),
            ]),
        ),
        (
            "write before the first savepoint, rollback to the outer",
            join(vec![
                to_s(&["BEGIN"]),
                vec![ins(1)],
                to_s(&["SAVEPOINT s1", "SAVEPOINT s2"]),
                vec![ins(2)],
                to_s(&["ROLLBACK TO s1"]),
                vec![ins(3)],
            ]),
        ),
        (
            "write before the first savepoint, savepoints opened around a write",
            join(vec![
                to_s(&["BEGIN"]),
                vec![ins(1)],
                to_s(&["SAVEPOINT s1"]),
                vec![ins(2)],
                to_s(&["SAVEPOINT s2"]),
                vec![ins(3)],
                to_s(&["ROLLBACK TO s1"]),
            ]),
        ),
        (
            "a failing multi-row statement inside nested savepoints",
            join(vec![
                to_s(&["BEGIN", "SAVEPOINT s1", "SAVEPOINT s2"]),
                vec![ins(1)],
                vec![
                    "INSERT INTO {T} VALUES('k7','u7','v7'),('k1','u8','v8')".to_string(),
                    "UPDATE {T} SET v='changed'".to_string(),
                ],
                to_s(&["ROLLBACK TO s1"]),
            ]),
        ),
        (
            "REPLACE inside nested savepoints",
            join(vec![
                to_s(&["BEGIN"]),
                vec![ins(1), ins(2)],
                to_s(&["SAVEPOINT s1", "SAVEPOINT s2"]),
                vec![
                    "INSERT OR REPLACE INTO {T} VALUES('k1','u2','r')".to_string(),
                    "INSERT OR REPLACE INTO {T} VALUES('k10','u11','r')".to_string(),
                ],
                to_s(&["ROLLBACK TO s1"]),
                vec!["INSERT OR REPLACE INTO {T} VALUES('k2','u1','r2')".to_string()],
                to_s(&["SAVEPOINT s2"]),
                vec!["UPDATE OR REPLACE {T} SET k='k10' WHERE k='k2'".to_string()],
                to_s(&["RELEASE s2", "ROLLBACK TO s1"]),
            ]),
        ),
        (
            "a failing REPLACE inside nested savepoints",
            join(vec![
                to_s(&["BEGIN"]),
                vec![ins(1), ins(2)],
                to_s(&["SAVEPOINT s1", "SAVEPOINT s2"]),
                vec![
                    big_replace.clone(),
                    "INSERT OR REPLACE INTO {T} VALUES('k10','u11','r')".to_string(),
                    big_replace.clone(),
                ],
                to_s(&["RELEASE s2"]),
                vec![big_replace.clone(), ins(5)],
                to_s(&["ROLLBACK TO s1"]),
                vec![big_replace],
            ]),
        ),
    ]
}

#[test]
fn nested_savepoints_match_a_native_table() {
    for variant in &VARIANTS {
        for (idx, (name, script)) in scenarios().iter().enumerate() {
            for end in ["COMMIT", "ROLLBACK"] {
                let mut s = Session::new(variant, &format!("{idx}_{end}"));
                s.run(&ins(10));
                s.run("INSERT INTO {T} VALUES('k11','u11','v11')");
                for sql in script {
                    let ok = s.run(sql);
                    let control = !sql.contains("{T}") && !sql.starts_with('!');
                    assert!(
                        !control || ok,
                        "{}: {name}: control statement failed: {sql}",
                        s.label
                    );
                }
                s.run(end);
                s.run("INSERT INTO {T} VALUES('kz','uz','vz')");
            }
        }
    }
}

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

/// Random savepoint, write and transaction statements.
struct SavepointGen {
    rng: Rng,
    begun: bool,
    stack: Vec<String>,
    created: u32,
}

impl SavepointGen {
    fn new(seed: u64) -> Self {
        Self {
            rng: Rng(seed.wrapping_mul(0x2545_F491_4F6C_DD1D) ^ 0x5A5A),
            begun: false,
            stack: Vec::new(),
            created: 0,
        }
    }

    fn in_tx(&self) -> bool {
        self.begun || !self.stack.is_empty()
    }

    fn value(&mut self, prefix: &str) -> String {
        if self.rng.below(8) == 0 {
            "NULL".to_string()
        } else {
            format!("'{prefix}{}'", self.rng.below(5))
        }
    }

    fn pick_savepoint(&mut self) -> usize {
        self.rng.below(self.stack.len() as u64) as usize
    }

    fn next(&mut self) -> String {
        let k = self.rng.below(6);
        match self.rng.below(20) {
            0..=4 => {
                let or = ["", "OR IGNORE ", "OR REPLACE "][self.rng.below(3) as usize];
                let (u, v) = (self.value("u"), self.value("v"));
                format!("INSERT {or}INTO {{T}} VALUES('k{k}',{u},{v})")
            }
            5 => {
                let (u, v) = (self.value("u"), self.value("v"));
                format!("UPDATE {{T}} SET u={u}, v={v} WHERE k='k{k}'")
            }
            6 => {
                let or = ["", "OR REPLACE "][self.rng.below(2) as usize];
                let to = self.rng.below(6);
                format!("UPDATE {or}{{T}} SET k='k{to}' WHERE k='k{k}'")
            }
            7 => format!("DELETE FROM {{T}} WHERE k='k{k}'"),
            8 => {
                let k2 = (k + 1 + self.rng.below(5)) % 6;
                let (u1, u2) = (self.value("u"), self.value("u"));
                format!("INSERT INTO {{T}} VALUES('k{k}',{u1},'m'),('k{k2}',{u2},'m')")
            }
            9 => format!(
                "!INSERT OR REPLACE INTO {{T}} VALUES('k{k}','u{}','{{BIG}}')",
                self.rng.below(5)
            ),
            10 if !self.begun && self.stack.is_empty() => {
                self.begun = true;
                "BEGIN".to_string()
            }
            11..=14 => {
                // Savepoint names repeat so that the same name is opened more than once.
                self.created += 1;
                let name = format!("s{}", self.rng.below(3));
                self.stack.push(name.clone());
                format!("SAVEPOINT {name}")
            }
            15..=17 if !self.stack.is_empty() => {
                let at = self.pick_savepoint();
                let name = self.stack[at].clone();
                // SQLite looks a name up from the innermost savepoint outwards.
                let at = self.stack.iter().rposition(|n| *n == name).unwrap();
                if self.rng.below(2) == 0 {
                    self.stack.truncate(at + 1);
                    format!("ROLLBACK TO {name}")
                } else {
                    self.stack.truncate(at);
                    format!("RELEASE {name}")
                }
            }
            18 if self.in_tx() => {
                self.begun = false;
                self.stack.clear();
                if self.rng.below(2) == 0 {
                    "COMMIT"
                } else {
                    "ROLLBACK"
                }
                .to_string()
            }
            _ => format!("DELETE FROM {{T}} WHERE k='k{k}'"),
        }
    }
}

#[test]
fn random_savepoint_statements_match_a_native_table() {
    for variant in &VARIANTS {
        for seed in 0..120u64 {
            let mut gen_ = SavepointGen::new(seed);
            let mut s = Session::new(variant, &format!("seed{seed}"));
            for _ in 0..70 {
                let sql = gen_.next();
                s.run(&sql);
            }
            if gen_.in_tx() {
                s.run(if seed % 2 == 0 { "COMMIT" } else { "ROLLBACK" });
            }
            s.run("INSERT OR REPLACE INTO {T} VALUES('kz','uz','vz')");
        }
    }
}

/// Two aydb tables share one engine transaction. SQLite announces the innermost savepoint to
/// each of them when it joins, so the engine sees the same id again after the first table has
/// already written.
#[test]
fn tables_that_join_at_different_times_share_the_savepoints() {
    let scripts: [&[&str]; 4] = [
        &[
            "BEGIN",
            "SAVEPOINT s1",
            "INSERT INTO {A} VALUES('x','1')",
            "INSERT INTO {B} VALUES('y','2')",
            "ROLLBACK TO s1",
        ],
        &[
            "BEGIN",
            "SAVEPOINT s1",
            "SAVEPOINT s2",
            "INSERT INTO {A} VALUES('x','1')",
            "INSERT INTO {B} VALUES('y','2')",
            "ROLLBACK TO s2",
            "INSERT INTO {B} VALUES('z','3')",
            "ROLLBACK TO s1",
        ],
        &[
            "BEGIN",
            "SAVEPOINT s1",
            "INSERT INTO {A} VALUES('x','1')",
            "SAVEPOINT s2",
            "INSERT INTO {A} VALUES('w','0')",
            "INSERT INTO {B} VALUES('y','2')",
            "ROLLBACK TO s2",
            "RELEASE s2",
            "ROLLBACK TO s1",
        ],
        &[
            "SAVEPOINT s1",
            "SAVEPOINT s2",
            "INSERT INTO {B} VALUES('y','2')",
            "INSERT INTO {A} VALUES('x','1')",
            "ROLLBACK TO s1",
        ],
    ];
    for (idx, script) in scripts.iter().enumerate() {
        for end in ["COMMIT", "ROLLBACK"] {
            let dir = TempTestDir::new(&format!("savepoints_two_tables_{idx}_{end}"));
            let fs = Arc::new(OsFs::open(&dir.path, true).unwrap());
            let engine = Arc::new(RwLock::new(
                StorageEngine::open(fs, EngineConfig::default()).unwrap(),
            ));
            let conn = SqliteConnection::open_in_memory().unwrap();
            conn.register_aydb_module_with_options(engine, false)
                .unwrap();
            conn.execute_batch(
                "CREATE VIRTUAL TABLE a1 USING aydb(table_id=1, columns='k TEXT PRIMARY KEY, v TEXT');
                 CREATE VIRTUAL TABLE b1 USING aydb(table_id=2, columns='k TEXT PRIMARY KEY, v TEXT');
                 CREATE TABLE a0(k TEXT PRIMARY KEY, v TEXT);
                 CREATE TABLE b0(k TEXT PRIMARY KEY, v TEXT);",
            )
            .unwrap();
            let rows = |t: &str| {
                conn.query_all(&format!("SELECT k, v FROM {t} ORDER BY k"), &[])
                    .unwrap()
            };
            let run = |sql: &str| {
                for side in ["0", "1"] {
                    let sql = sql
                        .replace("{A}", &format!("a{side}"))
                        .replace("{B}", &format!("b{side}"));
                    // Control statements have no table: run them once.
                    if side == "1" && !sql.contains(" INTO ") {
                        continue;
                    }
                    conn.execute_batch(&sql).unwrap();
                }
                assert_eq!(rows("a0"), rows("a1"), "script {idx}/{end}: a after {sql}");
                assert_eq!(rows("b0"), rows("b1"), "script {idx}/{end}: b after {sql}");
            };
            for sql in *script {
                run(sql);
            }
            run(end);
        }
    }
}
