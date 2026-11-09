//! Aggregate point-read throughput of a shared engine with 1/2/4/8 reader threads.
//!
//! Part 1: engine-level committed-view reads (`get_row_into`) on a shared engine.
//! Part 2: SQL `SELECT v FROM t WHERE id = ?` through one SQLite connection per thread.
//! Preload: 100k rows, three quarters inline (100 B) and one quarter out-of-line blobs (2 KB).

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use aydb::engine::{EngineConfig, StorageEngine, WriteDurability};
use aydb::pal::OsFs;
use aydb::vtab::connection::SqliteConnection;
use aydb::vtab::row_codec::SqlValue;

const ROWS: u64 = 100_000;
const RUN_FOR: Duration = Duration::from_millis(1500);

fn value_for(i: u64) -> Vec<u8> {
    if i % 4 == 3 {
        vec![(i & 0xFF) as u8; 2048]
    } else {
        vec![(i & 0xFF) as u8; 100]
    }
}

fn xorshift(s: &mut u64) -> u64 {
    *s ^= *s << 13;
    *s ^= *s >> 7;
    *s ^= *s << 17;
    *s
}

fn run_threads<F>(threads: usize, work: F) -> f64
where
    F: Fn(usize, &AtomicBool) -> u64 + Send + Sync + 'static,
{
    let work = Arc::new(work);
    let stop = Arc::new(AtomicBool::new(false));
    let total = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    let hs: Vec<_> = (0..threads)
        .map(|t| {
            let (work, stop, total) = (work.clone(), stop.clone(), total.clone());
            std::thread::spawn(move || {
                let n = work(t, &stop);
                total.fetch_add(n, Ordering::Relaxed);
            })
        })
        .collect();
    std::thread::sleep(RUN_FOR);
    stop.store(true, Ordering::Relaxed);
    for h in hs {
        h.join().unwrap();
    }
    total.load(Ordering::Relaxed) as f64 / start.elapsed().as_secs_f64()
}

fn main() {
    let dir = std::env::temp_dir().join(format!("aydb_cread_bench_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let fs = Arc::new(OsFs::open(&dir, true).unwrap());
    let engine = Arc::new(RwLock::new(
        StorageEngine::open(fs, EngineConfig::default()).unwrap(),
    ));

    // Preload raw engine table 2 and (via SQL) table 1.
    {
        let mut e = engine.write().unwrap();
        e.begin_transaction();
        for i in 0..ROWS {
            let k = format!("k{i:08}");
            e.put_row(
                2,
                k.as_bytes(),
                &value_for(i),
                &[],
                WriteDurability::BufferedFast,
            )
            .unwrap();
        }
        e.commit_transaction().unwrap();
        e.flush_memtable().unwrap();
        // leave a tail in the memtable
        for i in ROWS..ROWS + 1000 {
            let k = format!("k{i:08}");
            e.put_row(
                2,
                k.as_bytes(),
                &value_for(i),
                &[],
                WriteDurability::BufferedFast,
            )
            .unwrap();
        }
    }
    let setup = SqliteConnection::open_in_memory().unwrap();
    setup.register_aydb_module(engine.clone()).unwrap();
    setup
        .execute_batch("CREATE VIRTUAL TABLE t USING aydb(table_id=1, columns='id INTEGER PRIMARY KEY, v BLOB');")
        .unwrap();
    {
        setup.execute_batch("BEGIN;").unwrap();
        let mut ins = setup.prepare("INSERT INTO t VALUES (?, ?)").unwrap();
        for i in 0..ROWS {
            ins.execute(&[SqlValue::Integer(i as i64), SqlValue::Blob(value_for(i))])
                .unwrap();
        }
        drop(ins);
        setup.execute_batch("COMMIT;").unwrap();
    }

    println!(
        "{:<28} | {:>8} | {:>14} | {:>8}",
        "workload", "threads", "reads/s", "speedup"
    );
    println!("{}", "-".repeat(68));

    let mut base = 0.0;
    for threads in [1usize, 2, 4, 8] {
        let eng = engine.clone();
        let rate = run_threads(threads, move |t, stop| {
            let mut s = 0x9E3779B97F4A7C15u64 ^ ((t as u64 + 1) * 0x1234567);
            let (mut slab, mut spans) = (Vec::new(), Vec::new());
            let mut n = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let k = format!("k{:08}", xorshift(&mut s) % ROWS);
                slab.clear();
                spans.clear();
                let hit = {
                    let g = eng.read().unwrap();
                    assert!(!g.read_needs_exclusive(100 + t, 2));
                    g.reader_for_conn(100 + t)
                        .get_row_into(2, k.as_bytes(), &mut slab, &mut spans)
                        .unwrap()
                };
                assert!(hit);
                n += 1;
            }
            n
        });
        if threads == 1 {
            base = rate;
        }
        println!(
            "{:<28} | {:>8} | {:>14.0} | {:>7.2}x",
            "engine get_row_into",
            threads,
            rate,
            rate / base
        );
    }

    let mut base = 0.0;
    for threads in [1usize, 2, 4, 8] {
        let eng = engine.clone();
        let rate = run_threads(threads, move |t, stop| {
            let conn = SqliteConnection::open_in_memory().unwrap();
            conn.register_aydb_module_with_options(eng.clone(), false)
                .unwrap();
            conn.execute_batch("CREATE VIRTUAL TABLE t USING aydb(table_id=1, columns='id INTEGER PRIMARY KEY, v BLOB');")
                .unwrap();
            let mut st = conn.prepare("SELECT v FROM t WHERE id = ?").unwrap();
            let mut s = 0xDEADBEEFCAFEF00Du64 ^ ((t as u64 + 1) * 0x7654321);
            let mut n = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let id = (xorshift(&mut s) % ROWS) as i64;
                let rows = st.query(&[SqlValue::Integer(id)]).unwrap();
                assert_eq!(rows.len(), 1);
                n += 1;
            }
            n
        });
        if threads == 1 {
            base = rate;
        }
        println!(
            "{:<28} | {:>8} | {:>14.0} | {:>7.2}x",
            "sql SELECT by pk",
            threads,
            rate,
            rate / base
        );
    }

    drop(setup);
    drop(engine);
    let _ = std::fs::remove_dir_all(&dir);
}
