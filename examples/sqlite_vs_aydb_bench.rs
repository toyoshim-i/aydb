//! Phase 2 Head-to-Head Benchmark Suite:
//! Vanilla SQLite (`PRAGMA journal_mode = WAL`) vs. `aydb` Virtual Table (`USING aydb(...)`).
//!
//! All benchmarks execute identical parameterized SQL statements through the exact same
//! native SQLite C API (`sqlite3_prepare_v2` / `sqlite3_step`) to isolate storage-engine
//! behavior:
//!
//! 1. Small Synchronous Configuration Updates (`128 B` rows, Full Durability per transaction):
//!    Vanilla SQLite (`synchronous = FULL`) vs. `aydb` Virtual Table (`durability = 'sync'`).
//! 2. High-Churn Blob Cache with `MAX_BYTES` LRU Eviction (`16 KB` HTTP response blobs,
//!    writing `32.0 MB` total into a `4.0 MB` quota):
//!    Vanilla SQLite B-Tree + SQL LRU Eviction vs. `aydb` Virtual Table (WiscKey + Copy-less GC).
//! 3. Read-Heavy Cache Hit Workload with LRU Timestamp Tracking (`20,000` `SELECT` hits on
//!    cached blobs):
//!    Vanilla SQLite (`SELECT` + `UPDATE last_access`) vs. `aydb` Virtual Table (`SELECT` with
//!    automatic zero-I/O in-memory `AccessTracker` + Lazy Promotion).

#![allow(
    clippy::collapsible_if,
    clippy::field_reassign_with_default,
    clippy::needless_range_loop,
    clippy::ptr_arg,
    clippy::useless_vec,
    clippy::doc_overindented_list_items,
    clippy::new_without_default
)]

use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::Instant;

use aydb::engine::{EngineConfig, StorageEngine};
use aydb::pal::OsFs;
use aydb::vtab::{SqlValue, SqliteConnection};

/// Computes the total physical bytes of all files in `dir` (or matching a prefix).
fn sum_dir_bytes(dir: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .filter_map(|e| e.ok())
        .filter_map(|e| e.metadata().ok())
        .filter(|m| m.is_file())
        .map(|m| m.len())
        .sum()
}

/// Computes the physical bytes of data/index/blob files in `aydb` (excluding fixed pre-allocated WALs).
fn sum_aydb_data_segment_bytes(dir: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .filter_map(|e| e.ok())
        .filter(|e| {
            let name = e.file_name().to_string_lossy().into_owned();
            name.starts_with("idx_") || name.starts_with("blob_") || name == "boot_meta.db"
        })
        .filter_map(|e| e.metadata().ok())
        .map(|m| m.len())
        .sum()
}

fn percentile_us(sorted_ns: &[u64], pct: f64) -> f64 {
    if sorted_ns.is_empty() {
        return 0.0;
    }
    let idx = ((sorted_ns.len() - 1) as f64 * pct).round() as usize;
    (sorted_ns[idx] as f64) / 1000.0
}

fn bench_small_sync_config_updates(root: &PathBuf) {
    println!("==========================================================================================");
    println!("BENCHMARK 1 (Phase 2): Small Synchronous Config Updates via SQL (300 Commits, 128B Value)");
    println!("==========================================================================================");
    println!(
        "{:<34} | {:>12} | {:>12} | {:>12} | {:>12}",
        "SQL Engine & Mode",
        "Throughput",
        "P50 Lat(us)",
        "P99 Lat(us)",
        "Disk Writes"
    );
    println!("{}", "-".repeat(90));

    const N_COMMITS: usize = 300;
    let val_str = "v".repeat(128);

    // 1A. Vanilla SQLite (journal_mode = WAL, synchronous = FULL)
    {
        let sub = root.join("sqlite_sync_cfg");
        let _ = std::fs::remove_dir_all(&sub);
        std::fs::create_dir_all(&sub).unwrap();
        let db_path = sub.join("vanilla.sqlite");

        let conn = SqliteConnection::open_file(&db_path).unwrap();
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = FULL;
             PRAGMA fullfsync = ON;
             CREATE TABLE app_config (key TEXT PRIMARY KEY, val TEXT);",
        )
        .unwrap();

        let mut stmt = conn
            .prepare("INSERT OR REPLACE INTO app_config VALUES (?1, ?2)")
            .unwrap();

        let mut lats = Vec::with_capacity(N_COMMITS);
        let t_all = Instant::now();
        for i in 0..N_COMMITS {
            let k = SqlValue::Text(format!("setting_key_{:03}", i % 50));
            let v = SqlValue::Text(val_str.clone());
            let t0 = Instant::now();
            stmt.execute(&[k, v]).unwrap();
            lats.push(t0.elapsed().as_nanos() as u64);
        }
        let elapsed = t_all.elapsed().as_secs_f64();
        lats.sort_unstable();
        let disk_kb = (sum_dir_bytes(&sub) as f64) / 1024.0;

        println!(
            "{:<34} | {:>9.0}/s | {:>9.1} us | {:>9.1} us | {:>9.1} KB",
            "Vanilla SQLite (WAL, sync=FULL)",
            (N_COMMITS as f64) / elapsed,
            percentile_us(&lats, 0.50),
            percentile_us(&lats, 0.99),
            disk_kb
        );
        drop(stmt);
        drop(conn);
        let _ = std::fs::remove_dir_all(&sub);
    }

    // 1B. aydb Virtual Table (durability = 'sync' -> 4 KB isolated sector in wal_sync.db)
    {
        let sub = root.join("aydb_sync_cfg");
        let _ = std::fs::remove_dir_all(&sub);
        let fs = Arc::new(OsFs::open(&sub, true).unwrap());
        let engine = Arc::new(RwLock::new(
            StorageEngine::open(fs, EngineConfig::default()).unwrap(),
        ));

        let conn = SqliteConnection::open_in_memory().unwrap();
        conn.register_aydb_module(engine).unwrap();
        conn.execute_batch(
            "CREATE VIRTUAL TABLE app_config USING aydb(
                table_id = 1,
                durability = 'sync',
                columns = 'key TEXT PRIMARY KEY, val TEXT'
            );",
        )
        .unwrap();

        let mut stmt = conn
            .prepare("INSERT OR REPLACE INTO app_config VALUES (?1, ?2)")
            .unwrap();

        let mut lats = Vec::with_capacity(N_COMMITS);
        let t_all = Instant::now();
        for i in 0..N_COMMITS {
            let k = SqlValue::Text(format!("setting_key_{:03}", i % 50));
            let v = SqlValue::Text(val_str.clone());
            let t0 = Instant::now();
            stmt.execute(&[k, v]).unwrap();
            lats.push(t0.elapsed().as_nanos() as u64);
        }
        let elapsed = t_all.elapsed().as_secs_f64();
        lats.sort_unstable();
        let written_kb = ((N_COMMITS * 4096) as f64) / 1024.0;

        println!(
            "{:<34} | {:>9.0}/s | {:>9.1} us | {:>9.1} us | {:>9.1} KB",
            "aydb VTab (wal_sync)",
            (N_COMMITS as f64) / elapsed,
            percentile_us(&lats, 0.50),
            percentile_us(&lats, 0.99),
            written_kb
        );
        drop(stmt);
        drop(conn);
        let _ = std::fs::remove_dir_all(&sub);
    }
}

fn bench_blob_cache_churn_and_eviction(root: &PathBuf) {
    println!("\n==========================================================================================");
    println!("BENCHMARK 2 (Phase 2): High-Churn HTTP Blob Cache via SQL (16 KB Blobs, 4.0 MB Quota)");
    println!("  Writing 2,000 x 16 KB Blobs (32.0 MB Total) with Continuous LRU Eviction at 4.0 MB Cap");
    println!("==========================================================================================");
    println!(
        "{:<34} | {:>12} | {:>12} | {:>12} | {:>14}",
        "SQL Engine & Mode",
        "Write Speed",
        "P50 Lat(us)",
        "P99 Lat(us)",
        "Data Footprint"
    );
    println!("{}", "-".repeat(90));

    const BLOB_SIZE: usize = 16 * 1024; // 16 KB
    const QUOTA_BYTES: u64 = 4 * 1024 * 1024; // 4.0 MB cap (~250 live blobs)
    const MAX_LIVE_ROWS: i64 = (QUOTA_BYTES / (BLOB_SIZE as u64)) as i64; // 256 rows
    const TOTAL_INSERTS: usize = 2_000; // 32.0 MB written (8x the 4.0 MB quota)

    let blob_payload = vec![0x68u8; BLOB_SIZE];

    // 2A. Vanilla SQLite (journal_mode = WAL, synchronous = NORMAL) + SQL LRU Eviction
    {
        let sub = root.join("sqlite_blob_cache");
        let _ = std::fs::remove_dir_all(&sub);
        std::fs::create_dir_all(&sub).unwrap();
        let db_path = sub.join("cache.sqlite");

        let conn = SqliteConnection::open_file(&db_path).unwrap();
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             CREATE TABLE http_cache (
                 url TEXT PRIMARY KEY,
                 access_tick INTEGER NOT NULL,
                 body BLOB NOT NULL
             );",
        )
        .unwrap();

        let mut ins_stmt = conn
            .prepare("INSERT OR REPLACE INTO http_cache VALUES (?1, ?2, ?3)")
            .unwrap();
        let mut evict_stmt = conn
            .prepare("DELETE FROM http_cache WHERE access_tick <= ?1")
            .unwrap();

        let mut lats = Vec::with_capacity(TOTAL_INSERTS);
        let t_start = Instant::now();
        for i in 1..=TOTAL_INSERTS {
            let url = SqlValue::Text(format!("https://cdn.example.com/asset_{i:06}.bin"));
            let tick = SqlValue::Integer(i as i64);
            let body = SqlValue::Blob(blob_payload.clone());

            let t0 = Instant::now();
            conn.execute_batch("BEGIN").unwrap();
            ins_stmt.execute(&[url, tick, body]).unwrap();
            if (i as i64) > MAX_LIVE_ROWS {
                evict_stmt
                    .execute(&[SqlValue::Integer((i as i64) - MAX_LIVE_ROWS)])
                    .unwrap();
            }
            conn.execute_batch("COMMIT").unwrap();
            lats.push(t0.elapsed().as_nanos() as u64);
        }
        let elapsed = t_start.elapsed().as_secs_f64();
        lats.sort_unstable();
        let written_mb = (TOTAL_INSERTS * BLOB_SIZE) as f64 / (1024.0 * 1024.0);
        let disk_mb = (sum_dir_bytes(&sub) as f64) / (1024.0 * 1024.0);

        println!(
            "{:<34} | {:>8.1} MB/s | {:>9.1} us | {:>9.1} us | {:>11.2} MB",
            "Vanilla SQLite (WAL + SQL LRU)",
            written_mb / elapsed,
            percentile_us(&lats, 0.50),
            percentile_us(&lats, 0.99),
            disk_mb
        );
        drop(ins_stmt);
        drop(evict_stmt);
        drop(conn);
        let _ = std::fs::remove_dir_all(&sub);
    }

    // 2B. aydb Virtual Table (MAX_BYTES = 4 MB, WiscKey Separation + O(1) Copy-less Blob GC)
    {
        let sub = root.join("aydb_blob_cache");
        let _ = std::fs::remove_dir_all(&sub);
        let fs = Arc::new(OsFs::open(&sub, true).unwrap());
        let mut cfg = EngineConfig::default();
        cfg.max_blob_segment_bytes = 1024 * 1024; // 1.0 MB blob_lru segments
        let engine = Arc::new(RwLock::new(StorageEngine::open(fs, cfg).unwrap()));

        let conn = SqliteConnection::open_in_memory().unwrap();
        conn.register_aydb_module(engine.clone()).unwrap();
        conn.execute_batch(&format!(
            "CREATE VIRTUAL TABLE http_cache USING aydb(
                table_id = 1,
                max_bytes = {QUOTA_BYTES},
                durability = 'buffered',
                columns = 'url TEXT PRIMARY KEY, access_tick INT, body BLOB'
            );"
        ))
        .unwrap();

        let mut ins_stmt = conn
            .prepare("INSERT OR REPLACE INTO http_cache VALUES (?1, ?2, ?3)")
            .unwrap();

        let mut lats = Vec::with_capacity(TOTAL_INSERTS);
        let t_start = Instant::now();
        for i in 1..=TOTAL_INSERTS {
            let url = SqlValue::Text(format!("https://cdn.example.com/asset_{i:06}.bin"));
            let tick = SqlValue::Integer(i as i64);
            let body = SqlValue::Blob(blob_payload.clone());

            let t0 = Instant::now();
            ins_stmt.execute(&[url, tick, body]).unwrap();
            lats.push(t0.elapsed().as_nanos() as u64);
        }
        let elapsed = t_start.elapsed().as_secs_f64();
        lats.sort_unstable();
        let written_mb = (TOTAL_INSERTS * BLOB_SIZE) as f64 / (1024.0 * 1024.0);
        let data_mb = (sum_aydb_data_segment_bytes(&sub) as f64) / (1024.0 * 1024.0);

        println!(
            "{:<34} | {:>8.1} MB/s | {:>9.1} us | {:>9.1} us | {:>11.2} MB",
            "aydb VTab (WiscKey + Copy-less GC)",
            written_mb / elapsed,
            percentile_us(&lats, 0.50),
            percentile_us(&lats, 0.99),
            data_mb
        );
        drop(ins_stmt);
        drop(conn);
        drop(engine);
        let _ = std::fs::remove_dir_all(&sub);
    }
}

fn bench_read_heavy_cache_lru_tracking(root: &PathBuf) {
    println!("\n==========================================================================================");
    println!("BENCHMARK 3 (Phase 2): Read-Heavy Cache Hits with LRU Timestamp Tracking (20,000 SELECTs)");
    println!("==========================================================================================");
    println!(
        "{:<38} | {:>14} | {:>14} | {:>16}",
        "SQL Read + LRU Tracking Strategy",
        "Read Ops/sec",
        "Avg Latency",
        "Disk Write I/O"
    );
    println!("{}", "-".repeat(90));

    const NUM_ITEMS: usize = 200;
    const READ_QUERIES: usize = 20_000;
    let payload = vec![0x55u8; 4096]; // 4 KB cached blob

    // 3A. Vanilla SQLite: SELECT body + UPDATE access_tick on cache hit
    {
        let sub = root.join("sqlite_read_lru");
        let _ = std::fs::remove_dir_all(&sub);
        std::fs::create_dir_all(&sub).unwrap();
        let db_path = sub.join("read_cache.sqlite");

        let conn = SqliteConnection::open_file(&db_path).unwrap();
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             CREATE TABLE http_cache (
                 url TEXT PRIMARY KEY,
                 access_tick INTEGER NOT NULL,
                 body BLOB NOT NULL
             );
             CREATE INDEX idx_http_cache_tick ON http_cache(access_tick);",
        )
        .unwrap();

        let mut ins_stmt = conn
            .prepare("INSERT INTO http_cache VALUES (?1, ?2, ?3)")
            .unwrap();
        for i in 0..NUM_ITEMS {
            ins_stmt
                .execute(&[
                    SqlValue::Text(format!("https://cache.local/item_{i:04}")),
                    SqlValue::Integer(i as i64),
                    SqlValue::Blob(payload.clone()),
                ])
                .unwrap();
        }
        let before_bytes = sum_dir_bytes(&sub);

        let mut sel_stmt = conn
            .prepare("SELECT body FROM http_cache WHERE url = ?1")
            .unwrap();
        let mut upd_stmt = conn
            .prepare("UPDATE http_cache SET access_tick = ?1 WHERE url = ?2")
            .unwrap();

        let t0 = Instant::now();
        for q in 0..READ_QUERIES {
            let url = SqlValue::Text(format!("https://cache.local/item_{:04}", q % NUM_ITEMS));
            let rows = sel_stmt.query(std::slice::from_ref(&url)).unwrap();
            black_box(rows);
            upd_stmt
                .execute(&[SqlValue::Integer((NUM_ITEMS + q) as i64), url])
                .unwrap();
        }
        let elapsed = t0.elapsed().as_secs_f64();
        let after_bytes = sum_dir_bytes(&sub);
        let wal_growth_mb = (after_bytes.saturating_sub(before_bytes) as f64) / (1024.0 * 1024.0);

        println!(
            "{:<38} | {:>11.0}/s | {:>11.2} us | {:>11.2} MB WAL",
            "Vanilla SQLite (SELECT + UPDATE tick)",
            (READ_QUERIES as f64) / elapsed,
            (elapsed * 1e6) / (READ_QUERIES as f64),
            wal_growth_mb
        );

        drop(sel_stmt);
        drop(upd_stmt);
        drop(ins_stmt);
        drop(conn);
        let _ = std::fs::remove_dir_all(&sub);
    }

    // 3B. aydb Virtual Table: SELECT body (automatic in-memory AccessTracker + Lazy Promotion)
    {
        let sub = root.join("aydb_read_lru");
        let _ = std::fs::remove_dir_all(&sub);
        let fs = Arc::new(OsFs::open(&sub, true).unwrap());
        let engine = Arc::new(RwLock::new(
            StorageEngine::open(fs, EngineConfig::default()).unwrap(),
        ));

        let conn = SqliteConnection::open_in_memory().unwrap();
        conn.register_aydb_module(engine.clone()).unwrap();
        conn.execute_batch(
            "CREATE VIRTUAL TABLE http_cache USING aydb(
                table_id = 1,
                max_bytes = 2097152,
                durability = 'buffered',
                columns = 'url TEXT PRIMARY KEY, access_tick INTEGER, body BLOB'
            );",
        )
        .unwrap();

        let mut ins_stmt = conn
            .prepare("INSERT INTO http_cache VALUES (?1, ?2, ?3)")
            .unwrap();
        for i in 0..NUM_ITEMS {
            ins_stmt
                .execute(&[
                    SqlValue::Text(format!("https://cache.local/item_{i:04}")),
                    SqlValue::Integer(i as i64),
                    SqlValue::Blob(payload.clone()),
                ])
                .unwrap();
        }

        let mut sel_stmt = conn
            .prepare("SELECT body FROM http_cache WHERE url = ?1")
            .unwrap();

        let t0 = Instant::now();
        for q in 0..READ_QUERIES {
            let url = SqlValue::Text(format!("https://cache.local/item_{:04}", q % NUM_ITEMS));
            let rows = sel_stmt.query(&[url]).unwrap();
            black_box(rows);
        }
        let elapsed = t0.elapsed().as_secs_f64();

        println!(
            "{:<38} | {:>11.0}/s | {:>11.2} us | {:>14}",
            "aydb VTab (SELECT w/ Lazy AccessTick)",
            (READ_QUERIES as f64) / elapsed,
            (elapsed * 1e6) / (READ_QUERIES as f64),
            "0.00 MB (RAM)"
        );

        drop(sel_stmt);
        drop(ins_stmt);
        drop(conn);
        drop(engine);
        let _ = std::fs::remove_dir_all(&sub);
    }
}

fn main() {
    let dir = std::env::temp_dir().join(format!("aydb_sqlite_vs_bench_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create bench dir");

    bench_small_sync_config_updates(&dir);
    bench_blob_cache_churn_and_eviction(&dir);
    bench_read_heavy_cache_lru_tracking(&dir);

    let _ = std::fs::remove_dir_all(&dir);
}
