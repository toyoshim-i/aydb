//! Concurrency of durable SQL commits, aydb against SQLite with `synchronous = FULL`.
//!
//! Benchmark 1: throughput of 1/2/4/8 connections issuing autocommit `UPDATE`s. Concurrent aydb
//! commits share device flushes (group commit), while SQLite flushes once per commit.
//! Benchmark 2: point reads from 4 connections with and without one synchronous writer.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier, RwLock};
use std::time::{Duration, Instant};

use aydb::engine::{EngineConfig, StorageEngine};
use aydb::pal::OsFs;
use aydb::vtab::connection::SqliteConnection;
use aydb::vtab::row_codec::SqlValue;

const ROWS: i64 = 20_000;
const VALUE_BYTES: usize = 100;
const RUN_FOR: Duration = Duration::from_secs(2);
const READERS: usize = 4;
const AYDB_DDL: &str = "CREATE VIRTUAL TABLE t USING aydb(table_id=1, durability='sync', columns='id INTEGER PRIMARY KEY, v BLOB')";

type Engine = Arc<RwLock<StorageEngine<OsFs>>>;

#[derive(Clone)]
enum Target {
    Sqlite(PathBuf),
    Aydb(Engine),
}

impl Target {
    fn connect(&self) -> SqliteConnection {
        match self {
            Target::Sqlite(dir) => {
                let conn = SqliteConnection::open_file(&dir.join("bench.sqlite")).unwrap();
                conn.execute_batch(
                    "PRAGMA busy_timeout = 10000;
                     PRAGMA journal_mode = WAL;
                     PRAGMA synchronous = FULL;
                     PRAGMA fullfsync = ON;",
                )
                .unwrap();
                conn
            }
            Target::Aydb(engine) => {
                let conn = SqliteConnection::open_in_memory().unwrap();
                conn.register_aydb_module_with_options(engine.clone(), false)
                    .unwrap();
                conn.execute_batch(AYDB_DDL).unwrap();
                conn
            }
        }
    }
}

/// A freshly loaded database, and the connection that keeps it registered.
struct Database {
    target: Target,
    _owner: SqliteConnection,
    dir: PathBuf,
}

impl Drop for Database {
    fn drop(&mut self) {
        // Handles close when the fields drop; the removal is retried by the next run's setup.
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn fresh_dir(root: &Path, name: &str) -> PathBuf {
    let dir = root.join(name);
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn load(conn: &SqliteConnection) {
    conn.execute_batch("BEGIN").unwrap();
    {
        let mut insert = conn.prepare("INSERT INTO t VALUES (?, ?)").unwrap();
        for id in 0..ROWS {
            insert
                .execute(&[SqlValue::Integer(id), SqlValue::Blob(vec![7; VALUE_BYTES])])
                .unwrap();
        }
    }
    conn.execute_batch("COMMIT").unwrap();
}

fn open_sqlite(root: &Path) -> Database {
    let dir = fresh_dir(root, "sqlite");
    let target = Target::Sqlite(dir.clone());
    let owner = target.connect();
    owner
        .execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB)")
        .unwrap();
    load(&owner);
    Database {
        target,
        _owner: owner,
        dir,
    }
}

fn open_aydb(root: &Path) -> Database {
    let dir = fresh_dir(root, "aydb");
    let fs = Arc::new(OsFs::open(&dir, true).unwrap());
    let engine = Arc::new(RwLock::new(
        StorageEngine::open(fs, EngineConfig::default()).unwrap(),
    ));
    let owner = SqliteConnection::open_in_memory().unwrap();
    owner.register_aydb_module(engine.clone()).unwrap();
    owner.execute_batch(AYDB_DDL).unwrap();
    load(&owner);
    Database {
        target: Target::Aydb(engine),
        _owner: owner,
        dir,
    }
}

fn xorshift(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

fn spawn_writer(
    target: Target,
    seed: u64,
    ready: Arc<Barrier>,
    stop: Arc<AtomicBool>,
    commits: Arc<AtomicU64>,
    busy: Arc<AtomicU64>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let conn = target.connect();
        let mut update = conn.prepare("UPDATE t SET v = ? WHERE id = ?").unwrap();
        ready.wait();
        let mut state = 0x9E37_79B9_7F4A_7C15 ^ ((seed + 1) * 0x0123_4567);
        while !stop.load(Ordering::Relaxed) {
            let id = (xorshift(&mut state) % ROWS as u64) as i64;
            let params = [SqlValue::Blob(vec![9; VALUE_BYTES]), SqlValue::Integer(id)];
            match update.execute(&params) {
                Ok(()) => commits.fetch_add(1, Ordering::Relaxed),
                Err(e) if is_busy(&e) => busy.fetch_add(1, Ordering::Relaxed),
                Err(e) => panic!("write failed: {e}"),
            };
        }
    })
}

fn is_busy(error: &std::io::Error) -> bool {
    let text = error.to_string().to_ascii_lowercase();
    text.contains("locked") || text.contains("busy")
}

/// Returns (commits per second, failed-as-busy per second).
fn measure_writers(target: &Target, writers: usize) -> (f64, f64) {
    let ready = Arc::new(Barrier::new(writers + 1));
    let stop = Arc::new(AtomicBool::new(false));
    let commits = Arc::new(AtomicU64::new(0));
    let busy = Arc::new(AtomicU64::new(0));
    let threads: Vec<_> = (0..writers)
        .map(|w| {
            spawn_writer(
                target.clone(),
                w as u64,
                ready.clone(),
                stop.clone(),
                commits.clone(),
                busy.clone(),
            )
        })
        .collect();
    ready.wait();
    let start = Instant::now();
    std::thread::sleep(RUN_FOR);
    stop.store(true, Ordering::Relaxed);
    for thread in threads {
        thread.join().unwrap();
    }
    let secs = start.elapsed().as_secs_f64();
    (
        commits.load(Ordering::Relaxed) as f64 / secs,
        busy.load(Ordering::Relaxed) as f64 / secs,
    )
}

struct ReadResult {
    commits_per_sec: f64,
    reads_per_sec: f64,
    p50_us: f64,
    p99_us: f64,
    p999_us: f64,
}

fn measure_reads(target: &Target, with_writer: bool) -> ReadResult {
    let ready = Arc::new(Barrier::new(READERS + 1 + usize::from(with_writer)));
    let stop = Arc::new(AtomicBool::new(false));
    let commits = Arc::new(AtomicU64::new(0));
    let busy = Arc::new(AtomicU64::new(0));
    let readers: Vec<_> = (0..READERS)
        .map(|r| {
            let (target, ready, stop) = (target.clone(), ready.clone(), stop.clone());
            std::thread::spawn(move || {
                let conn = target.connect();
                let mut select = conn.prepare("SELECT v FROM t WHERE id = ?").unwrap();
                ready.wait();
                let mut state = 0xDEAD_BEEF_CAFE_F00D ^ ((r as u64 + 1) * 0x0765_4321);
                let mut latencies = Vec::with_capacity(4_000_000);
                while !stop.load(Ordering::Relaxed) {
                    let id = (xorshift(&mut state) % ROWS as u64) as i64;
                    let start = Instant::now();
                    let rows = select.query(&[SqlValue::Integer(id)]).unwrap();
                    latencies.push(start.elapsed().as_nanos() as u64);
                    assert_eq!(rows.len(), 1);
                }
                latencies
            })
        })
        .collect();
    let writer = with_writer.then(|| {
        spawn_writer(
            target.clone(),
            99,
            ready.clone(),
            stop.clone(),
            commits.clone(),
            busy.clone(),
        )
    });
    ready.wait();
    let start = Instant::now();
    std::thread::sleep(RUN_FOR);
    stop.store(true, Ordering::Relaxed);
    let mut latencies: Vec<u64> = readers
        .into_iter()
        .flat_map(|thread| thread.join().unwrap())
        .collect();
    if let Some(thread) = writer {
        thread.join().unwrap();
    }
    let secs = start.elapsed().as_secs_f64();
    latencies.sort_unstable();
    let percentile =
        |q: f64| latencies[((latencies.len() - 1) as f64 * q) as usize] as f64 / 1_000.0;
    ReadResult {
        commits_per_sec: commits.load(Ordering::Relaxed) as f64 / secs,
        reads_per_sec: latencies.len() as f64 / secs,
        p50_us: percentile(0.5),
        p99_us: percentile(0.99),
        p999_us: percentile(0.999),
    }
}

fn title(text: &str) {
    println!("{}", "=".repeat(78));
    println!("{text}");
    println!("{}", "=".repeat(78));
}

fn print_read_row(label: &str, r: &ReadResult) {
    println!(
        "{:<26} | {:>9.0} | {:>10.0} | {:>8.1} us | {:>8.1} us | {:>9.1} us",
        label, r.commits_per_sec, r.reads_per_sec, r.p50_us, r.p99_us, r.p999_us
    );
}

fn main() {
    let root = std::env::temp_dir().join(format!("aydb_cwrite_bench_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();

    title("BENCHMARK 1: Concurrent Synchronous Writers (Autocommit UPDATE, sync = FULL)");
    println!(
        "{:<8} | {:>14} | {:>12} | {:>11} | {:>8}",
        "Writers", "SQLite (tx/s)", "aydb (tx/s)", "aydb BUSY/s", "Ratio"
    );
    println!("{}", "-".repeat(66));
    for writers in [1usize, 2, 4, 8] {
        let (sqlite, _) = {
            let db = open_sqlite(&root);
            measure_writers(&db.target, writers)
        };
        let (aydb, busy) = {
            let db = open_aydb(&root);
            measure_writers(&db.target, writers)
        };
        println!(
            "{:<8} | {:>14.0} | {:>12.0} | {:>11.0} | {:>7.2}x",
            writers,
            sqlite,
            aydb,
            busy,
            aydb / sqlite
        );
    }

    title("BENCHMARK 2: Point Reads From 4 Connections During Synchronous Commits");
    println!(
        "{:<26} | {:>9} | {:>10} | {:>11} | {:>11} | {:>12}",
        "Engine & Writer", "Commits/s", "Reads/s", "P50", "P99", "P99.9"
    );
    println!("{}", "-".repeat(94));
    for (label, with_writer) in [("no writer", false), ("1 sync writer", true)] {
        let sqlite = {
            let db = open_sqlite(&root);
            measure_reads(&db.target, with_writer)
        };
        print_read_row(&format!("SQLite, {label}"), &sqlite);
        let aydb = {
            let db = open_aydb(&root);
            measure_reads(&db.target, with_writer)
        };
        print_read_row(&format!("aydb, {label}"), &aydb);
    }

    let _ = std::fs::remove_dir_all(&root);
}
