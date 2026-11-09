//! Commit-latency benchmark: inline vs background index compaction.
//!
//! Sustained inserts into a small-MemTable engine shared behind `Arc<Mutex<_>>` (as the vtab
//! layer does). Reports per-commit latency percentiles; the max reflects the stall an unlucky
//! commit pays when it has to run a whole merge while holding the engine mutex.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use aydb::engine::{EngineConfig, StorageEngine, WriteDurability};
use aydb::pal::OsFs;

const ROWS: usize = 150_000;

fn run(background: bool) -> (Vec<Duration>, usize) {
    let dir = std::env::temp_dir().join(format!(
        "aydb_lat_bench_{}_{}",
        std::process::id(),
        if background { "bg" } else { "inline" }
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let fs = Arc::new(OsFs::open(&dir, true).unwrap());
    let cfg = EngineConfig {
        memtable_capacity_bytes: 128 * 1024,
        auto_compact_segment_threshold: 8,
        background_compaction: background,
        ..Default::default()
    };
    let engine = Arc::new(RwLock::new(StorageEngine::open(fs, cfg).unwrap()));

    let stop = Arc::new(AtomicBool::new(false));
    let worker = background.then(|| {
        let engine = Arc::clone(&engine);
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let job = {
                    let mut e = engine.write().unwrap();
                    if e.compaction_pending() {
                        e.prepare_compaction().unwrap()
                    } else {
                        None
                    }
                };
                match job {
                    Some(job) => {
                        let out = job.run().unwrap();
                        engine.write().unwrap().install_compaction(out).unwrap();
                    }
                    None => std::thread::sleep(Duration::from_millis(1)),
                }
            }
        })
    });

    let payload = vec![0x5Au8; 100];
    let mut lat = Vec::with_capacity(ROWS);
    for i in 0..ROWS {
        let key = format!("key{i:09}");
        let t = Instant::now();
        engine
            .write()
            .unwrap()
            .put_row(
                1,
                key.as_bytes(),
                &payload,
                &[],
                WriteDurability::BufferedFast,
            )
            .unwrap();
        lat.push(t.elapsed());
    }
    stop.store(true, Ordering::Relaxed);
    if let Some(w) = worker {
        w.join().unwrap();
    }
    let segs = engine.write().unwrap().active_idx_segment_count();
    drop(engine);
    let _ = std::fs::remove_dir_all(&dir);
    (lat, segs)
}

fn pct(sorted: &[Duration], p: f64) -> Duration {
    sorted[((sorted.len() as f64 * p) as usize).min(sorted.len() - 1)]
}

fn main() {
    println!("{ROWS} sustained inserts, 128 KB MemTable, threshold 8");
    println!(
        "{:<12} {:>10} {:>10} {:>10} {:>12} {:>5}",
        "mode", "p50", "p99", "p99.9", "max", "segs"
    );
    for (name, bg) in [("inline", false), ("background", true)] {
        let (mut lat, segs) = run(bg);
        lat.sort();
        println!(
            "{:<12} {:>10.1?} {:>10.1?} {:>10.1?} {:>12.2?} {:>5}",
            name,
            pct(&lat, 0.50),
            pct(&lat, 0.99),
            pct(&lat, 0.999),
            lat[lat.len() - 1],
            segs
        );
    }
}
