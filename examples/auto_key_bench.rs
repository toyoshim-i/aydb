//! Automatic-key insert benchmark (`examples/auto_key_bench.rs`).
//!
//! Inserts 100,000 rows with a NULL `INTEGER PRIMARY KEY` into an `aydb` virtual table and into
//! a native SQLite table, in one transaction, and reports the best of three rounds. The automatic
//! key is `max + 1` at the time of each insert (`src/vtab/auto_rowid_tests.rs`), so this tracks
//! the cost of finding that key, with and without a `UNIQUE` column to maintain.

use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use aydb::engine::{EngineConfig, StorageEngine};
use aydb::pal::OsFs;
use aydb::vtab::{SqlValue, SqliteConnection};

const ROWS: usize = 100_000;
const ROUNDS: usize = 3;

/// Best of [`ROUNDS`] rounds of [`ROWS`] automatic-key inserts into `t` in one transaction.
fn time_inserts(conn: &SqliteConnection, unique: bool) -> Duration {
    let sql = if unique {
        "INSERT INTO t VALUES (NULL, ?1, 1)"
    } else {
        "INSERT INTO t VALUES (NULL, 1)"
    };
    let mut best = Duration::MAX;
    for round in 0..ROUNDS {
        conn.execute_batch("BEGIN").unwrap();
        let start = Instant::now();
        let mut stmt = conn.prepare(sql).unwrap();
        for i in 0..ROWS {
            if unique {
                stmt.execute(&[SqlValue::Text(format!("u{round}-{i}"))])
                    .unwrap();
            } else {
                stmt.execute(&[]).unwrap();
            }
        }
        drop(stmt);
        conn.execute_batch("COMMIT").unwrap();
        best = best.min(start.elapsed());
    }
    best
}

fn aydb_conn(dir: &Path, columns: &str) -> SqliteConnection {
    let fs = Arc::new(OsFs::open(dir.join("aydb"), true).unwrap());
    let engine = Arc::new(RwLock::new(
        StorageEngine::open(fs, EngineConfig::default()).unwrap(),
    ));
    let conn = SqliteConnection::open_file(&dir.join("schema.db")).unwrap();
    conn.register_aydb_module_with_options(engine, false)
        .unwrap();
    conn.execute_batch(&format!(
        "CREATE VIRTUAL TABLE t USING aydb(columns='{columns}')"
    ))
    .unwrap();
    conn
}

fn native_conn(dir: &Path, columns: &str) -> SqliteConnection {
    let conn = SqliteConnection::open_file(&dir.join("native.sqlite")).unwrap();
    conn.execute_batch("PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;")
        .unwrap();
    conn.execute_batch(&format!("CREATE TABLE t({columns})"))
        .unwrap();
    conn
}

fn main() {
    let root = std::env::temp_dir().join(format!("aydb_auto_key_bench_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("create bench dir");

    println!("{}", "=".repeat(78));
    println!(
        "BENCHMARK 1: Automatic-Key INSERT ({ROWS} rows in one transaction, best of {ROUNDS})"
    );
    println!("{}", "=".repeat(78));
    println!(
        "{:<26} | {:>12} | {:>14} | {:>14}",
        "Table", "Time(ms)", "Rows/sec", "Native(ms)"
    );
    println!("{}", "-".repeat(78));

    for (label, columns, unique) in [
        ("plain", "k INTEGER PRIMARY KEY, v INT", false),
        (
            "with UNIQUE",
            "k INTEGER PRIMARY KEY, u TEXT UNIQUE, v INT",
            true,
        ),
    ] {
        let dir = root.join(label.replace(' ', "_"));
        std::fs::create_dir_all(&dir).unwrap();
        let aydb = time_inserts(&aydb_conn(&dir, columns), unique);
        let native = time_inserts(&native_conn(&dir, columns), unique);
        println!(
            "{:<26} | {:>12.1} | {:>12.0}/s | {:>14.1}",
            label,
            aydb.as_secs_f64() * 1e3,
            ROWS as f64 / aydb.as_secs_f64(),
            native.as_secs_f64() * 1e3,
        );
    }

    let _ = std::fs::remove_dir_all(&root);
}
