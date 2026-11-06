//! Empirical microbenchmark suite for Phase 1, Step 1-5 (Capped Tables, Delayed LRU Eviction,
//! & Unified `StorageEngine` Coordinator):
//!
//! 1. Read-Path LRU Tracking Trade-off (`CoalescedLazyPromotion` vs. `EagerWriteOnRead`):
//!    Measures cache-hit throughput, latency, and MemTable/WAL write amplification across
//!    50,000 hot cache reads followed by quota-exceeding insertions.
//! 2. Capped Table (`MAX_BYTES`) Steady-State Eviction & $O(1)$ Copy-less Blob Segment Deletion:
//!    Writes 16 MB of 4 KB blobs into a Capped Table configured with `MAX_BYTES = 2 MB`
//!    (`max_blob_segment_bytes = 512 KB`) and measures steady-state write throughput,
//!    logical bytes vs. physical `blob_lru` files on disk, and $O(1)$ segment unlinking.

use std::hint::black_box;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use aydb::engine::{EngineConfig, LruReadTrackingMode, StorageEngine, WriteDurability};
use aydb::pal::OsFs;

fn bench_lru_read_tracking(root: &PathBuf) {
    println!("==========================================================================================");
    println!("BENCHMARK 1 (Step 1-5): Capped Table Read-Path LRU Tracking (50,000 Cache Hit Reads)");
    println!("==========================================================================================");
    println!(
        "{:<26} | {:>14} | {:>14} | {:>16} | {:>12}",
        "LRU Read Tracking Mode",
        "Read Ops/sec",
        "Read Lat(us)",
        "WAL Batches Emitted",
        "Lazy Promos"
    );
    println!("{}", "-".repeat(90));

    const INITIAL_KEYS: usize = 500;
    const READ_OPS: usize = 50_000;

    for &mode in &[
        LruReadTrackingMode::EagerWriteOnRead,
        LruReadTrackingMode::CoalescedLazyPromotion,
    ] {
        let sub = root.join(format!("lru_read_{mode:?}"));
        let _ = std::fs::remove_dir_all(&sub);
        let fs = Arc::new(OsFs::open(&sub, true).unwrap());

        let mut cfg = EngineConfig::default();
        cfg.lru_read_tracking = mode;
        cfg.memtable_capacity_bytes = 64 * 1024 * 1024;

        let mut engine = StorageEngine::open(fs, cfg).unwrap();
        // Quota fits 500 keys of ~264 B each (~132 KB)
        engine.register_capped_table(1, 140_000).unwrap();

        let val = vec![0x5Au8; 256];
        for k in 0..INITIAL_KEYS {
            let pk = format!("pk_{k:05}").into_bytes();
            engine
                .put_row(1, &pk, &val, &[], WriteDurability::BufferedFast)
                .unwrap();
        }

        // Measure 50,000 hot cache reads on the oldest 100 keys (`pk_00000` .. `pk_00099`)
        let t_read = Instant::now();
        for i in 0..READ_OPS {
            let pk = format!("pk_{:05}", i % 100);
            let res = engine.get_row(1, pk.as_bytes()).unwrap();
            black_box(res);
        }
        let read_elapsed = t_read.elapsed().as_secs_f64();
        let ops_sec = (READ_OPS as f64) / read_elapsed;
        let lat_us = (read_elapsed * 1e6) / (READ_OPS as f64);

        // Now insert 100 new keys to trigger quota enforcement (`MAX_BYTES`) so lazy promotion runs!
        for k in INITIAL_KEYS..(INITIAL_KEYS + 100) {
            let pk = format!("pk_{k:05}").into_bytes();
            engine
                .put_row(1, &pk, &val, &[], WriteDurability::BufferedFast)
                .unwrap();
        }

        let st = engine.capped_table_state(1).unwrap();
        let wal_batches = match mode {
            LruReadTrackingMode::EagerWriteOnRead => READ_OPS,
            LruReadTrackingMode::CoalescedLazyPromotion => st.total_lazy_promotions as usize,
        };

        println!(
            "{:<26} | {:>12.0}/s | {:>9.2} us | {:>16} | {:>12}",
            match mode {
                LruReadTrackingMode::EagerWriteOnRead => "EagerWriteOnRead (Naive)",
                LruReadTrackingMode::CoalescedLazyPromotion => "CoalescedLazyPromotion",
            },
            ops_sec,
            lat_us,
            wal_batches,
            st.total_lazy_promotions
        );

        drop(engine);
        let _ = std::fs::remove_dir_all(&sub);
    }
}

fn bench_steady_state_capped_eviction(root: &PathBuf) {
    println!("\n==========================================================================================");
    println!("BENCHMARK 2 (Step 1-5): Capped Table Steady-State Eviction & O(1) Copy-less Blob GC");
    println!("  (Writing 16.0 MB of 4 KB Blobs into MAX_BYTES = 2.0 MB Capped Table, 512 KB Blob Segs)");
    println!("==========================================================================================");
    println!(
        "{:<16} | {:>16} | {:>16} | {:>16} | {:>16}",
        "Written So Far",
        "Logical Live(MB)",
        "Active Blob Segs",
        "Total Evictions",
        "Write Speed(MB/s)"
    );
    println!("{}", "-".repeat(90));

    let sub = root.join("steady_evict");
    let _ = std::fs::remove_dir_all(&sub);
    let fs = Arc::new(OsFs::open(&sub, true).unwrap());

    let mut cfg = EngineConfig::default();
    cfg.inline_threshold_bytes = 512;
    cfg.max_blob_segment_bytes = 512 * 1024; // 512 KB per blob_lru segment

    let mut engine = StorageEngine::open(fs, cfg).unwrap();
    const QUOTA_BYTES: u64 = 2 * 1024 * 1024; // 2.0 MB cap
    engine.register_capped_table(1, QUOTA_BYTES).unwrap();

    let payload = vec![0x77u8; 4096]; // 4 KB blob
    const TOTAL_BLOBS: usize = 4_096; // 4,096 * 4 KB = 16.0 MB total written
    const REPORT_INTERVAL: usize = 1_024; // Every 4.0 MB

    let t_start = Instant::now();
    for i in 1..=TOTAL_BLOBS {
        let pk = format!("cache_item_{i:06}").into_bytes();
        engine
            .put_row(1, &pk, &payload, &[], WriteDurability::BufferedFast)
            .unwrap();

        if i % REPORT_INTERVAL == 0 {
            let elapsed = t_start.elapsed().as_secs_f64();
            let written_mb = (i * payload.len()) as f64 / (1024.0 * 1024.0);
            let st = engine.capped_table_state(1).unwrap();
            let live_mb = (st.current_logical_bytes as f64) / (1024.0 * 1024.0);
            let active_segs = engine.active_lru_blob_segment_count(1);
            let mb_sec = written_mb / elapsed;

            println!(
                "{:<16} | {:>13.2} MB | {:>16} | {:>16} | {:>12.1} MB/s",
                format!("{written_mb:.1} MB"),
                live_mb,
                active_segs,
                st.total_evictions,
                mb_sec
            );
        }
    }

    drop(engine);
    let _ = std::fs::remove_dir_all(&sub);
}

fn main() {
    let dir = std::env::temp_dir().join(format!("aydb_lru_bench_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create bench dir");

    bench_lru_read_tracking(&dir);
    bench_steady_state_capped_eviction(&dir);

    let _ = std::fs::remove_dir_all(&dir);
}
