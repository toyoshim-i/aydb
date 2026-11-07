//! Industry-Standard SQL Benchmark Suite (`examples/standard_sql_bench.rs`):
//!
//! Evaluates Vanilla SQLite (`PRAGMA journal_mode = WAL`) vs. `aydb` Virtual Table (`USING aydb(...)`)
//! across three standard database benchmark suites:
//!
//! 1. **SQLite Official "Speedtest1" Standard Suite (Tests 100..320):**
//!    - Test 100: Bulk `INSERT` of 5,000 rows (`BEGIN .. COMMIT`)
//!    - Test 130: 5,000 Random Primary-Key Point `SELECT`s (`WHERE id = ?`)
//!    - Test 140: 5,000 Random Secondary-Index Equality `SELECT`s (`WHERE uval = ?`)
//!    - Test 160: 1,000 Primary-Key Range `SELECT`s (`WHERE id >= ? AND id < ?`)
//!    - Test 210: Full-Table Aggregation & `GROUP BY` (`SELECT category, COUNT(*), SUM(uval) ...`)
//!    - Test 230: 2,500 Random `UPDATE`s on Indexed Table
//!    - Test 240: 2,500 Random `DELETE`s + Re-`INSERT`s (Churn)
//!    - Evaluated with `WalStreamMode::BufferedImmediateOsCache` on `aydb` vs. `PRAGMA synchronous = NORMAL` on SQLite.
//!
//! 2. **YCSB (Yahoo! Cloud Serving Benchmark) Workloads A through F (Zipfian $\theta = 0.99$):**
//!    - Workload A (Update Heavy): 50% Read / 50% Update
//!    - Workload B (Read Mostly): 95% Read / 5% Update
//!    - Workload C (Read Only): 100% Point Read
//!    - Workload D (Read Latest): 95% Read Latest Inserted / 5% Insert
//!    - Workload E (Short Ranges): 95% Range Scan (`WHERE k >= ? ORDER BY k ASC LIMIT 10`) / 5% Insert
//!      — Evaluated at both `256 B` (Inline) and `2048 B` (WiscKey Separated Blob) to expose the
//!        fundamental B-Tree vs. WiscKey range-scan trade-off!
//!    - Workload F (Read-Modify-Write): 50% Point Read / 50% Read-Modify-Write
//!
//! 3. **TPC-B (`pgbench`-style) Multi-Table OLTP Benchmark:**
//!    - 4 tables (`branches`, `tellers`, `accounts`, `history`), 5 SQL statements per transaction
//!      (`UPDATE accounts` -> `SELECT abalance` -> `UPDATE tellers` -> `UPDATE branches` -> `INSERT history`),
//!      measured under both Buffered (`synchronous = NORMAL` / `wal_buf.db`) and Full Durable Sync
//!      (`synchronous = FULL` / `wal_sync.db`) modes.

use std::hint::black_box;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use aydb::engine::{EngineConfig, StorageEngine};
use aydb::pal::OsFs;
use aydb::vtab::{SqlValue, SqliteConnection};
use aydb::wal::WalStreamMode;

/// Deterministic Xorshift64* PRNG + precomputed Zipfian inverse-CDF sampler ($\theta = 0.99$).
struct ZipfRng {
    state: u64,
    cdf: Vec<f64>,
}

impl ZipfRng {
    fn new(seed: u64, num_items: usize, theta: f64) -> Self {
        let mut cdf = Vec::with_capacity(num_items);
        let mut sum = 0.0f64;
        for i in 1..=num_items {
            sum += 1.0 / (i as f64).powf(theta);
            cdf.push(sum);
        }
        for x in &mut cdf {
            *x /= sum;
        }
        Self {
            state: seed.max(1),
            cdf,
        }
    }

    #[inline]
    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.state = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    #[inline]
    fn next_usize(&mut self, bound: usize) -> usize {
        (self.next_u64() as usize) % bound
    }

    #[inline]
    fn next_f64(&mut self) -> f64 {
        ((self.next_u64() >> 11) as f64) * (1.0 / ((1u64 << 53) as f64))
    }

    /// Samples a 0-based item index `0..num_items` according to the Zipfian distribution.
    #[inline]
    fn next_zipf(&mut self) -> usize {
        let u = self.next_f64();
        self.cdf
            .partition_point(|&p| p < u)
            .min(self.cdf.len().saturating_sub(1))
    }
}

// =========================================================================
// SUITE 1: SQLite Official "Speedtest1" Standard Workload Suite
// =========================================================================

struct SpeedtestTimings {
    t100_bulk_insert_ms: f64,
    t130_pk_point_select_ms: f64,
    t140_sec_idx_select_ms: f64,
    t160_pk_range_select_ms: f64,
    t210_full_agg_group_ms: f64,
    t230_random_update_ms: f64,
    t240_delete_reinsert_ms: f64,
}

fn run_speedtest1_on_connection(conn: &SqliteConnection) -> SpeedtestTimings {
    const N_ROWS: usize = 5_000;
    const N_POINT: usize = 5_000;
    const N_RANGE: usize = 1_000;
    const N_MUT: usize = 2_500;

    let filler = "x".repeat(120);
    let mut rng = ZipfRng::new(0x1234_5678, N_ROWS, 0.99);

    // Test 100: Bulk INSERT of 5,000 indexed rows
    let t100 = Instant::now();
    conn.execute_batch("BEGIN").unwrap();
    {
        let mut ins = conn
            .prepare("INSERT INTO t1 VALUES (?1, ?2, ?3, ?4)")
            .unwrap();
        for i in 1..=N_ROWS {
            let cat = format!("cat_{:02}", i % 20);
            ins.execute(&[
                SqlValue::Integer(i as i64),
                SqlValue::Integer(((i * 73) % N_ROWS) as i64),
                SqlValue::Text(cat),
                SqlValue::Text(filler.clone()),
            ])
            .unwrap();
        }
    }
    conn.execute_batch("COMMIT").unwrap();
    let t100_bulk_insert_ms = t100.elapsed().as_secs_f64() * 1e3;

    // Test 130: 5,000 Random Primary-Key Point SELECTs
    let t130 = Instant::now();
    {
        let mut sel_pk = conn
            .prepare("SELECT uval, category, payload FROM t1 WHERE id = ?1")
            .unwrap();
        for _ in 0..N_POINT {
            let target_id = (rng.next_usize(N_ROWS) + 1) as i64;
            let rows = sel_pk.query(&[SqlValue::Integer(target_id)]).unwrap();
            black_box(rows);
        }
    }
    let t130_pk_point_select_ms = t130.elapsed().as_secs_f64() * 1e3;

    // Test 140: 5,000 Random Secondary-Index Equality SELECTs (`WHERE uval = ?`)
    let t140 = Instant::now();
    {
        let mut sel_sec = conn
            .prepare("SELECT id, payload FROM t1 WHERE uval = ?1")
            .unwrap();
        for _ in 0..N_POINT {
            let target_uval = rng.next_usize(N_ROWS) as i64;
            let rows = sel_sec.query(&[SqlValue::Integer(target_uval)]).unwrap();
            black_box(rows);
        }
    }
    let t140_sec_idx_select_ms = t140.elapsed().as_secs_f64() * 1e3;

    // Test 160: 1,000 Primary-Key Range Scans (`WHERE id >= ? AND id < ?`, 20 rows per scan)
    let t160 = Instant::now();
    {
        let mut sel_rng = conn
            .prepare("SELECT id, uval FROM t1 WHERE id >= ?1 AND id < ?2")
            .unwrap();
        for _ in 0..N_RANGE {
            let start_id = (rng.next_usize(N_ROWS - 25) + 1) as i64;
            let rows = sel_rng
                .query(&[SqlValue::Integer(start_id), SqlValue::Integer(start_id + 20)])
                .unwrap();
            black_box(rows);
        }
    }
    let t160_pk_range_select_ms = t160.elapsed().as_secs_f64() * 1e3;

    // Test 210: Full-Table Scan + GROUP BY Aggregation (10 iterations over 5,000 rows)
    let t210 = Instant::now();
    {
        let mut agg = conn
            .prepare("SELECT category, COUNT(*), SUM(uval) FROM t1 GROUP BY category")
            .unwrap();
        for _ in 0..10 {
            let rows = agg.query(&[]).unwrap();
            black_box(rows);
        }
    }
    let t210_full_agg_group_ms = t210.elapsed().as_secs_f64() * 1e3;

    // Test 230: 2,500 Random UPDATEs
    let t230 = Instant::now();
    conn.execute_batch("BEGIN").unwrap();
    {
        let mut upd = conn
            .prepare("UPDATE t1 SET uval = ?1, payload = ?2 WHERE id = ?3")
            .unwrap();
        for i in 0..N_MUT {
            let target_id = (rng.next_usize(N_ROWS) + 1) as i64;
            upd.execute(&[
                SqlValue::Integer((i * 17) as i64),
                SqlValue::Text(filler.clone()),
                SqlValue::Integer(target_id),
            ])
            .unwrap();
        }
    }
    conn.execute_batch("COMMIT").unwrap();
    let t230_random_update_ms = t230.elapsed().as_secs_f64() * 1e3;

    // Test 240: 2,500 Random DELETEs + Re-INSERTs
    let t240 = Instant::now();
    conn.execute_batch("BEGIN").unwrap();
    {
        let mut del = conn.prepare("DELETE FROM t1 WHERE id = ?1").unwrap();
        let mut ins = conn
            .prepare("INSERT OR REPLACE INTO t1 VALUES (?1, ?2, ?3, ?4)")
            .unwrap();
        for i in 1..=N_MUT {
            let id = (rng.next_usize(N_ROWS) + 1) as i64;
            del.execute(&[SqlValue::Integer(id)]).unwrap();
            ins.execute(&[
                SqlValue::Integer(id),
                SqlValue::Integer(id * 3),
                SqlValue::Text(format!("cat_{:02}", i % 20)),
                SqlValue::Text(filler.clone()),
            ])
            .unwrap();
        }
    }
    conn.execute_batch("COMMIT").unwrap();
    let t240_delete_reinsert_ms = t240.elapsed().as_secs_f64() * 1e3;

    SpeedtestTimings {
        t100_bulk_insert_ms,
        t130_pk_point_select_ms,
        t140_sec_idx_select_ms,
        t160_pk_range_select_ms,
        t210_full_agg_group_ms,
        t230_random_update_ms,
        t240_delete_reinsert_ms,
    }
}

fn bench_speedtest1_suite(root: &PathBuf) {
    println!("==========================================================================================");
    println!("SUITE 1: SQLite Official \"Speedtest1\" Standard Workload (5,000 Indexed Rows, 120B Payload)");
    println!("==========================================================================================");
    println!(
        "{:<40} | {:>16} | {:>16} | {:>12}",
        "Speedtest1 Sub-Benchmark",
        "Vanilla SQLite",
        "aydb VTab",
        "Speedup"
    );
    println!("{}", "-".repeat(90));

    // Run on Vanilla SQLite (WAL, NORMAL)
    let sq_timings = {
        let sub = root.join("speedtest_sqlite");
        let _ = std::fs::remove_dir_all(&sub);
        std::fs::create_dir_all(&sub).unwrap();
        let conn = SqliteConnection::open_file(&sub.join("speedtest.sqlite")).unwrap();
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             CREATE TABLE t1 (
                 id INTEGER PRIMARY KEY,
                 uval INTEGER,
                 category TEXT,
                 payload TEXT
             );
             CREATE INDEX idx_t1_uval ON t1(uval);",
        )
        .unwrap();
        let res = run_speedtest1_on_connection(&conn);
        drop(conn);
        let _ = std::fs::remove_dir_all(&sub);
        res
    };

    // Run on aydb Virtual Table (BufferedImmediateOsCache — OS page cache parity with SQLite synchronous = NORMAL)
    let ay_timings = {
        let sub = root.join("speedtest_aydb");
        let _ = std::fs::remove_dir_all(&sub);
        let fs = Arc::new(OsFs::open(&sub, true).unwrap());
        let mut cfg = EngineConfig::default();
        cfg.buf_wal_mode = WalStreamMode::BufferedImmediateOsCache;
        let engine = Arc::new(Mutex::new(StorageEngine::open(fs, cfg).unwrap()));

        let conn = SqliteConnection::open_in_memory().unwrap();
        conn.register_aydb_module(engine.clone()).unwrap();
        conn.execute_batch(
            "CREATE VIRTUAL TABLE t1 USING aydb(
                table_id = 1,
                durability = 'buffered',
                columns = 'id INT PRIMARY KEY, uval INT, category TEXT, payload TEXT',
                indexes = 'uval'
            );",
        )
        .unwrap();
        let res = run_speedtest1_on_connection(&conn);
        drop(conn);
        drop(engine);
        let _ = std::fs::remove_dir_all(&sub);
        res
    };

    let rows = [
        (
            "Test 100: 5,000 Bulk INSERTs (Indexed)",
            sq_timings.t100_bulk_insert_ms,
            ay_timings.t100_bulk_insert_ms,
        ),
        (
            "Test 130: 5,000 PK Point SELECTs",
            sq_timings.t130_pk_point_select_ms,
            ay_timings.t130_pk_point_select_ms,
        ),
        (
            "Test 140: 5,000 Secondary Index SELECTs",
            sq_timings.t140_sec_idx_select_ms,
            ay_timings.t140_sec_idx_select_ms,
        ),
        (
            "Test 160: 1,000 PK Range Scans (20 rows)",
            sq_timings.t160_pk_range_select_ms,
            ay_timings.t160_pk_range_select_ms,
        ),
        (
            "Test 210: 10x Full-Table GROUP BY Agg",
            sq_timings.t210_full_agg_group_ms,
            ay_timings.t210_full_agg_group_ms,
        ),
        (
            "Test 230: 2,500 Random Indexed UPDATEs",
            sq_timings.t230_random_update_ms,
            ay_timings.t230_random_update_ms,
        ),
        (
            "Test 240: 2,500 Random DELETE + INSERTs",
            sq_timings.t240_delete_reinsert_ms,
            ay_timings.t240_delete_reinsert_ms,
        ),
    ];

    let mut sq_total = 0.0;
    let mut ay_total = 0.0;
    for (name, sq, ay) in rows {
        sq_total += sq;
        ay_total += ay;
        println!(
            "{:<40} | {:>13.2} ms | {:>13.2} ms | {:>10.2}x",
            name,
            sq,
            ay,
            sq / ay
        );
    }
    println!("{}", "-".repeat(90));
    println!(
        "{:<40} | {:>13.2} ms | {:>13.2} ms | {:>10.2}x",
        "TOTAL SPEEDTEST1 SUITE TIME",
        sq_total,
        ay_total,
        sq_total / ay_total
    );
}

// =========================================================================
// SUITE 2: YCSB (Yahoo! Cloud Serving Benchmark) Workloads A through F
// =========================================================================

fn run_ycsb_on_connection(
    conn: &SqliteConnection,
    val_size: usize,
) -> [(String, f64); 6] {
    const INITIAL_RECORDS: usize = 4_000;
    const OPS_PER_WORKLOAD: usize = 5_000;

    let payload = vec![0x42u8; val_size];
    let mut rng = ZipfRng::new(0x9E37_79B9, INITIAL_RECORDS, 0.99);

    // Load phase: 4,000 initial records
    conn.execute_batch("BEGIN").unwrap();
    {
        let mut ins = conn
            .prepare("INSERT OR REPLACE INTO usertable VALUES (?1, ?2)")
            .unwrap();
        for i in 0..INITIAL_RECORDS {
            ins.execute(&[
                SqlValue::Text(format!("user{i:08}")),
                SqlValue::Blob(payload.clone()),
            ])
            .unwrap();
        }
    }
    conn.execute_batch("COMMIT").unwrap();

    let mut sel_stmt = conn
        .prepare("SELECT field0 FROM usertable WHERE ycsb_key = ?1")
        .unwrap();
    let mut upd_stmt = conn
        .prepare("UPDATE usertable SET field0 = ?1 WHERE ycsb_key = ?2")
        .unwrap();
    let mut ins_stmt = conn
        .prepare("INSERT OR REPLACE INTO usertable VALUES (?1, ?2)")
        .unwrap();
    let mut scan_stmt = conn
        .prepare(
            "SELECT ycsb_key, field0 FROM usertable WHERE ycsb_key >= ?1 ORDER BY ycsb_key ASC LIMIT 10",
        )
        .unwrap();

    // Workload A: 50% Read, 50% Update (Zipfian)
    let ta = Instant::now();
    for i in 0..OPS_PER_WORKLOAD {
        let k_idx = rng.next_zipf();
        let key = SqlValue::Text(format!("user{k_idx:08}"));
        if i % 2 == 0 {
            black_box(sel_stmt.query(&[key]).unwrap());
        } else {
            upd_stmt
                .execute(&[SqlValue::Blob(payload.clone()), key])
                .unwrap();
        }
    }
    let ops_a = (OPS_PER_WORKLOAD as f64) / ta.elapsed().as_secs_f64();

    // Workload B: 95% Read, 5% Update (Zipfian)
    let tb = Instant::now();
    for i in 0..OPS_PER_WORKLOAD {
        let k_idx = rng.next_zipf();
        let key = SqlValue::Text(format!("user{k_idx:08}"));
        if i % 20 == 0 {
            upd_stmt
                .execute(&[SqlValue::Blob(payload.clone()), key])
                .unwrap();
        } else {
            black_box(sel_stmt.query(&[key]).unwrap());
        }
    }
    let ops_b = (OPS_PER_WORKLOAD as f64) / tb.elapsed().as_secs_f64();

    // Workload C: 100% Read (Zipfian)
    let tc = Instant::now();
    for _ in 0..OPS_PER_WORKLOAD {
        let k_idx = rng.next_zipf();
        let key = SqlValue::Text(format!("user{k_idx:08}"));
        black_box(sel_stmt.query(&[key]).unwrap());
    }
    let ops_c = (OPS_PER_WORKLOAD as f64) / tc.elapsed().as_secs_f64();

    // Workload D: Read Latest (95% Read skewed to newest records, 5% Insert new record)
    let mut latest_count = INITIAL_RECORDS;
    let td = Instant::now();
    for i in 0..OPS_PER_WORKLOAD {
        if i % 20 == 0 {
            latest_count += 1;
            let key = SqlValue::Text(format!("user{latest_count:08}"));
            ins_stmt
                .execute(&[key, SqlValue::Blob(payload.clone())])
                .unwrap();
        } else {
            let offset = rng.next_zipf().min(latest_count - 1);
            let k_idx = latest_count - 1 - offset;
            let key = SqlValue::Text(format!("user{k_idx:08}"));
            black_box(sel_stmt.query(&[key]).unwrap());
        }
    }
    let ops_d = (OPS_PER_WORKLOAD as f64) / td.elapsed().as_secs_f64();

    // Workload E: Short Ranges (95% Range Scan of 10 rows, 5% Insert)
    let te = Instant::now();
    for i in 0..OPS_PER_WORKLOAD {
        if i % 20 == 0 {
            latest_count += 1;
            let key = SqlValue::Text(format!("user{latest_count:08}"));
            ins_stmt
                .execute(&[key, SqlValue::Blob(payload.clone())])
                .unwrap();
        } else {
            let k_idx = rng.next_zipf();
            let key = SqlValue::Text(format!("user{k_idx:08}"));
            black_box(scan_stmt.query(&[key]).unwrap());
        }
    }
    let ops_e = (OPS_PER_WORKLOAD as f64) / te.elapsed().as_secs_f64();

    // Workload F: Read-Modify-Write (50% Point Read, 50% Read + Update)
    let tf = Instant::now();
    for i in 0..OPS_PER_WORKLOAD {
        let k_idx = rng.next_zipf();
        let key = SqlValue::Text(format!("user{k_idx:08}"));
        black_box(sel_stmt.query(std::slice::from_ref(&key)).unwrap());
        if i % 2 == 0 {
            upd_stmt
                .execute(&[SqlValue::Blob(payload.clone()), key])
                .unwrap();
        }
    }
    let ops_f = (OPS_PER_WORKLOAD as f64) / tf.elapsed().as_secs_f64();

    [
        ("Workload A (50% R / 50% U)".to_string(), ops_a),
        ("Workload B (95% R /  5% U)".to_string(), ops_b),
        ("Workload C (100% Point Read)".to_string(), ops_c),
        ("Workload D (95% Read Latest)".to_string(), ops_d),
        ("Workload E (95% Short Range)".to_string(), ops_e),
        ("Workload F (50% R / 50% RMW)".to_string(), ops_f),
    ]
}

fn bench_ycsb_suite(root: &PathBuf) {
    for &val_size in &[256usize, 2048] {
        println!("\n==========================================================================================");
        println!(
            "SUITE 2: YCSB Workloads A-F (Zipfian theta=0.99, Value Size = {} B — {})",
            val_size,
            if val_size < 1024 {
                "Inline Storage"
            } else {
                "WiscKey Separated Blob Storage"
            }
        );
        println!("==========================================================================================");
        println!(
            "{:<34} | {:>18} | {:>18} | {:>12}",
            "YCSB Workload",
            "Vanilla SQLite(WAL)",
            "aydb VTab",
            "Ratio"
        );
        println!("{}", "-".repeat(90));

        let sq_res = {
            let sub = root.join(format!("ycsb_sq_{val_size}"));
            let _ = std::fs::remove_dir_all(&sub);
            std::fs::create_dir_all(&sub).unwrap();
            let conn = SqliteConnection::open_file(&sub.join("ycsb.sqlite")).unwrap();
            conn.execute_batch(
                "PRAGMA journal_mode = WAL;
                 PRAGMA synchronous = NORMAL;
                 CREATE TABLE usertable (
                     ycsb_key TEXT PRIMARY KEY,
                     field0 BLOB
                 );",
            )
            .unwrap();
            let r = run_ycsb_on_connection(&conn, val_size);
            drop(conn);
            let _ = std::fs::remove_dir_all(&sub);
            r
        };

        let ay_res = {
            let sub = root.join(format!("ycsb_ay_{val_size}"));
            let _ = std::fs::remove_dir_all(&sub);
            let fs = Arc::new(OsFs::open(&sub, true).unwrap());
            let engine = Arc::new(Mutex::new(
                StorageEngine::open(fs, EngineConfig::default()).unwrap(),
            ));
            let conn = SqliteConnection::open_in_memory().unwrap();
            conn.register_aydb_module(engine.clone()).unwrap();
            conn.execute_batch(
                "CREATE VIRTUAL TABLE usertable USING aydb(
                    table_id = 1,
                    durability = 'buffered',
                    columns = 'ycsb_key TEXT PRIMARY KEY, field0 BLOB'
                );",
            )
            .unwrap();
            let r = run_ycsb_on_connection(&conn, val_size);
            drop(conn);
            drop(engine);
            let _ = std::fs::remove_dir_all(&sub);
            r
        };

        for ((name, sq_ops), (_, ay_ops)) in sq_res.iter().zip(ay_res.iter()) {
            println!(
                "{:<34} | {:>14.0} ops/s | {:>14.0} ops/s | {:>10.2}x",
                name,
                sq_ops,
                ay_ops,
                ay_ops / sq_ops
            );
        }
    }
}

// =========================================================================
// SUITE 3: TPC-B (pgbench-style) 4-Table OLTP Benchmark
// =========================================================================

fn run_tpcb_transactions(conn: &SqliteConnection, n_tx: usize) -> f64 {
    const N_BRANCHES: i64 = 10;
    const N_TELLERS: i64 = 100;
    const N_ACCOUNTS: i64 = 2_000;

    conn.execute_batch("BEGIN").unwrap();
    {
        let mut ins_b = conn.prepare("INSERT INTO branches VALUES (?1, 0)").unwrap();
        for b in 1..=N_BRANCHES {
            ins_b.execute(&[SqlValue::Integer(b)]).unwrap();
        }
        let mut ins_t = conn
            .prepare("INSERT INTO tellers VALUES (?1, ?2, 0)")
            .unwrap();
        for t in 1..=N_TELLERS {
            ins_t
                .execute(&[
                    SqlValue::Integer(t),
                    SqlValue::Integer((t % N_BRANCHES) + 1),
                ])
                .unwrap();
        }
        let mut ins_a = conn
            .prepare("INSERT INTO accounts VALUES (?1, ?2, 0)")
            .unwrap();
        for a in 1..=N_ACCOUNTS {
            ins_a
                .execute(&[
                    SqlValue::Integer(a),
                    SqlValue::Integer((a % N_BRANCHES) + 1),
                ])
                .unwrap();
        }
    }
    conn.execute_batch("COMMIT").unwrap();

    let mut upd_acc = conn
        .prepare("UPDATE accounts SET abalance = abalance + ?1 WHERE aid = ?2")
        .unwrap();
    let mut sel_acc = conn
        .prepare("SELECT abalance FROM accounts WHERE aid = ?1")
        .unwrap();
    let mut upd_tel = conn
        .prepare("UPDATE tellers SET tbalance = tbalance + ?1 WHERE tid = ?2")
        .unwrap();
    let mut upd_br = conn
        .prepare("UPDATE branches SET bbalance = bbalance + ?1 WHERE bid = ?2")
        .unwrap();
    let mut ins_hist = conn
        .prepare("INSERT INTO history VALUES (?1, ?2, ?3, ?4, ?5)")
        .unwrap();

    let mut rng = ZipfRng::new(0x55AA_1234, N_ACCOUNTS as usize, 0.99);
    let t0 = Instant::now();
    for tx_id in 1..=n_tx {
        let aid = (rng.next_usize(N_ACCOUNTS as usize) + 1) as i64;
        let tid = (rng.next_usize(N_TELLERS as usize) + 1) as i64;
        let bid = (aid % N_BRANCHES) + 1;
        let delta = ((tx_id as i64) % 200) - 100;

        conn.execute_batch("BEGIN").unwrap();
        upd_acc
            .execute(&[SqlValue::Integer(delta), SqlValue::Integer(aid)])
            .unwrap();
        let acc_rows = sel_acc.query(&[SqlValue::Integer(aid)]).unwrap();
        black_box(acc_rows);
        upd_tel
            .execute(&[SqlValue::Integer(delta), SqlValue::Integer(tid)])
            .unwrap();
        upd_br
            .execute(&[SqlValue::Integer(delta), SqlValue::Integer(bid)])
            .unwrap();
        ins_hist
            .execute(&[
                SqlValue::Integer(tx_id as i64),
                SqlValue::Integer(tid),
                SqlValue::Integer(bid),
                SqlValue::Integer(aid),
                SqlValue::Integer(delta),
            ])
            .unwrap();
        conn.execute_batch("COMMIT").unwrap();
    }

    (n_tx as f64) / t0.elapsed().as_secs_f64()
}

fn bench_tpcb_suite(root: &PathBuf) {
    println!("\n==========================================================================================");
    println!("SUITE 3: TPC-B (pgbench) 4-Table OLTP Benchmark (5 SQL Statements per Transaction)");
    println!("==========================================================================================");
    println!(
        "{:<34} | {:>18} | {:>18} | {:>12}",
        "Durability Mode",
        "Vanilla SQLite(WAL)",
        "aydb VTab",
        "Ratio"
    );
    println!("{}", "-".repeat(90));

    // Mode 1: Buffered / NORMAL (1,000 multi-table transactions)
    {
        let sq_tps = {
            let sub = root.join("tpcb_sq_norm");
            let _ = std::fs::remove_dir_all(&sub);
            std::fs::create_dir_all(&sub).unwrap();
            let conn = SqliteConnection::open_file(&sub.join("tpcb.sqlite")).unwrap();
            conn.execute_batch(
                "PRAGMA journal_mode = WAL;
                 PRAGMA synchronous = NORMAL;
                 CREATE TABLE branches (bid INTEGER PRIMARY KEY, bbalance INTEGER);
                 CREATE TABLE tellers (tid INTEGER PRIMARY KEY, bid INTEGER, tbalance INTEGER);
                 CREATE TABLE accounts (aid INTEGER PRIMARY KEY, bid INTEGER, abalance INTEGER);
                 CREATE TABLE history (hid INTEGER PRIMARY KEY, tid INTEGER, bid INTEGER, aid INTEGER, delta INTEGER);",
            )
            .unwrap();
            let tps = run_tpcb_transactions(&conn, 1_000);
            drop(conn);
            let _ = std::fs::remove_dir_all(&sub);
            tps
        };

        let ay_tps = {
            let sub = root.join("tpcb_ay_norm");
            let _ = std::fs::remove_dir_all(&sub);
            let fs = Arc::new(OsFs::open(&sub, true).unwrap());
            let engine = Arc::new(Mutex::new(
                StorageEngine::open(fs, EngineConfig::default()).unwrap(),
            ));
            let conn = SqliteConnection::open_in_memory().unwrap();
            conn.register_aydb_module(engine.clone()).unwrap();
            conn.execute_batch(
                "CREATE VIRTUAL TABLE branches USING aydb(table_id=1, durability='buffered', columns='bid INT PRIMARY KEY, bbalance INT');
                 CREATE VIRTUAL TABLE tellers USING aydb(table_id=2, durability='buffered', columns='tid INT PRIMARY KEY, bid INT, tbalance INT');
                 CREATE VIRTUAL TABLE accounts USING aydb(table_id=3, durability='buffered', columns='aid INT PRIMARY KEY, bid INT, abalance INT');
                 CREATE VIRTUAL TABLE history USING aydb(table_id=4, durability='buffered', columns='hid INT PRIMARY KEY, tid INT, bid INT, aid INT, delta INT');",
            )
            .unwrap();
            let tps = run_tpcb_transactions(&conn, 1_000);
            drop(conn);
            drop(engine);
            let _ = std::fs::remove_dir_all(&sub);
            tps
        };

        println!(
            "{:<34} | {:>14.0} TPS | {:>14.0} TPS | {:>10.2}x",
            "Buffered (sync=NORMAL / wal_buf)",
            sq_tps,
            ay_tps,
            ay_tps / sq_tps
        );
    }

    // Mode 2: Full Sync Durability (150 multi-table transactions)
    {
        let sq_tps = {
            let sub = root.join("tpcb_sq_full");
            let _ = std::fs::remove_dir_all(&sub);
            std::fs::create_dir_all(&sub).unwrap();
            let conn = SqliteConnection::open_file(&sub.join("tpcb_full.sqlite")).unwrap();
            conn.execute_batch(
                "PRAGMA journal_mode = WAL;
                 PRAGMA synchronous = FULL;
                 CREATE TABLE branches (bid INTEGER PRIMARY KEY, bbalance INTEGER);
                 CREATE TABLE tellers (tid INTEGER PRIMARY KEY, bid INTEGER, tbalance INTEGER);
                 CREATE TABLE accounts (aid INTEGER PRIMARY KEY, bid INTEGER, abalance INTEGER);
                 CREATE TABLE history (hid INTEGER PRIMARY KEY, tid INTEGER, bid INTEGER, aid INTEGER, delta INTEGER);",
            )
            .unwrap();
            let tps = run_tpcb_transactions(&conn, 150);
            drop(conn);
            let _ = std::fs::remove_dir_all(&sub);
            tps
        };

        let ay_tps = {
            let sub = root.join("tpcb_ay_full");
            let _ = std::fs::remove_dir_all(&sub);
            let fs = Arc::new(OsFs::open(&sub, true).unwrap());
            let engine = Arc::new(Mutex::new(
                StorageEngine::open(fs, EngineConfig::default()).unwrap(),
            ));
            let conn = SqliteConnection::open_in_memory().unwrap();
            conn.register_aydb_module(engine.clone()).unwrap();
            conn.execute_batch(
                "CREATE VIRTUAL TABLE branches USING aydb(table_id=1, durability='sync', columns='bid INT PRIMARY KEY, bbalance INT');
                 CREATE VIRTUAL TABLE tellers USING aydb(table_id=2, durability='sync', columns='tid INT PRIMARY KEY, bid INT, tbalance INT');
                 CREATE VIRTUAL TABLE accounts USING aydb(table_id=3, durability='sync', columns='aid INT PRIMARY KEY, bid INT, abalance INT');
                 CREATE VIRTUAL TABLE history USING aydb(table_id=4, durability='sync', columns='hid INT PRIMARY KEY, tid INT, bid INT, aid INT, delta INT');",
            )
            .unwrap();
            let tps = run_tpcb_transactions(&conn, 150);
            drop(conn);
            drop(engine);
            let _ = std::fs::remove_dir_all(&sub);
            tps
        };

        println!(
            "{:<34} | {:>14.0} TPS | {:>14.0} TPS | {:>10.2}x",
            "Full Sync (sync=FULL / wal_sync)",
            sq_tps,
            ay_tps,
            ay_tps / sq_tps
        );
    }
}

// =========================================================================
// SUITE 4: On-Disk SSTable Evaluation (`memtable_capacity_bytes = 128 KB`, BENCH-01 / BENCH-02)
// =========================================================================

fn bench_on_disk_sstable_suite(root: &PathBuf) {
    println!("\n==========================================================================================");
    println!("SUITE 4: On-Disk SSTable Evaluation (memtable_capacity_bytes = 128 KB, idx_*.db SSTables)");
    println!("==========================================================================================");
    println!(
        "{:<44} | {:>16} | {:>16} | {:>10}",
        "On-Disk SSTable Workload (128 KB MemTable)",
        "Vanilla SQLite",
        "aydb (SSTables)",
        "Ratio"
    );
    println!("{}", "-".repeat(94));

    // 4A. Speedtest1 Total with 128 KB MemTable
    let sq_st = {
        let sub = root.join("sstable_st_sq");
        let _ = std::fs::remove_dir_all(&sub);
        std::fs::create_dir_all(&sub).unwrap();
        let conn = SqliteConnection::open_file(&sub.join("speedtest.sqlite")).unwrap();
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             CREATE TABLE t1 (id INTEGER PRIMARY KEY, uval INTEGER, category TEXT, payload TEXT);
             CREATE INDEX idx_t1_uval ON t1(uval);",
        )
        .unwrap();
        let r = run_speedtest1_on_connection(&conn);
        drop(conn);
        let _ = std::fs::remove_dir_all(&sub);
        r.t100_bulk_insert_ms
            + r.t130_pk_point_select_ms
            + r.t140_sec_idx_select_ms
            + r.t160_pk_range_select_ms
            + r.t210_full_agg_group_ms
            + r.t230_random_update_ms
            + r.t240_delete_reinsert_ms
    };
    let ay_st = {
        let sub = root.join("sstable_st_ay");
        let _ = std::fs::remove_dir_all(&sub);
        let fs = Arc::new(OsFs::open(&sub, true).unwrap());
        let mut cfg = EngineConfig::default();
        cfg.memtable_capacity_bytes = 128 * 1024;
        cfg.buf_wal_mode = WalStreamMode::BufferedImmediateOsCache;
        let engine = Arc::new(Mutex::new(StorageEngine::open(fs, cfg).unwrap()));
        let conn = SqliteConnection::open_in_memory().unwrap();
        conn.register_aydb_module(engine.clone()).unwrap();
        conn.execute_batch(
            "CREATE VIRTUAL TABLE t1 USING aydb(
                table_id = 1,
                durability = 'buffered',
                columns = 'id INT PRIMARY KEY, uval INT, category TEXT, payload TEXT',
                indexes = 'uval'
            );",
        )
        .unwrap();
        let r = run_speedtest1_on_connection(&conn);
        drop(conn);
        drop(engine);
        let _ = std::fs::remove_dir_all(&sub);
        r.t100_bulk_insert_ms
            + r.t130_pk_point_select_ms
            + r.t140_sec_idx_select_ms
            + r.t160_pk_range_select_ms
            + r.t210_full_agg_group_ms
            + r.t230_random_update_ms
            + r.t240_delete_reinsert_ms
    };
    println!(
        "{:<44} | {:>13.2} ms | {:>13.2} ms | {:>9.2}x",
        "Speedtest1 Total (Tests 100-240, 128 KB MT)",
        sq_st,
        ay_st,
        sq_st / ay_st
    );

    // 4B. YCSB 256 B with 128 KB MemTable (Workloads B & C)
    let sq_ycsb = {
        let sub = root.join("sstable_ycsb_sq");
        let _ = std::fs::remove_dir_all(&sub);
        std::fs::create_dir_all(&sub).unwrap();
        let conn = SqliteConnection::open_file(&sub.join("ycsb.sqlite")).unwrap();
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             CREATE TABLE usertable (ycsb_key TEXT PRIMARY KEY, field0 BLOB);",
        )
        .unwrap();
        let r = run_ycsb_on_connection(&conn, 256);
        drop(conn);
        let _ = std::fs::remove_dir_all(&sub);
        r
    };
    let ay_ycsb = {
        let sub = root.join("sstable_ycsb_ay");
        let _ = std::fs::remove_dir_all(&sub);
        let fs = Arc::new(OsFs::open(&sub, true).unwrap());
        let mut cfg = EngineConfig::default();
        cfg.memtable_capacity_bytes = 128 * 1024;
        let engine = Arc::new(Mutex::new(StorageEngine::open(fs, cfg).unwrap()));
        let conn = SqliteConnection::open_in_memory().unwrap();
        conn.register_aydb_module(engine.clone()).unwrap();
        conn.execute_batch(
            "CREATE VIRTUAL TABLE usertable USING aydb(
                table_id = 1,
                durability = 'buffered',
                columns = 'ycsb_key TEXT PRIMARY KEY, field0 BLOB'
            );",
        )
        .unwrap();
        let r = run_ycsb_on_connection(&conn, 256);
        drop(conn);
        drop(engine);
        let _ = std::fs::remove_dir_all(&sub);
        r
    };
    println!(
        "{:<44} | {:>11.0} op/s | {:>11.0} op/s | {:>9.2}x",
        "YCSB 256B Workload C (100% Point Read)",
        sq_ycsb[2].1,
        ay_ycsb[2].1,
        ay_ycsb[2].1 / sq_ycsb[2].1
    );
    println!(
        "{:<44} | {:>11.0} op/s | {:>11.0} op/s | {:>9.2}x",
        "YCSB 256B Workload B (95% Read / 5% Update)",
        sq_ycsb[1].1,
        ay_ycsb[1].1,
        ay_ycsb[1].1 / sq_ycsb[1].1
    );

    // 4C. TPC-B Buffered & Full Sync with 128 KB MemTable
    let sq_tpcb_buf = {
        let sub = root.join("sstable_tpcb_sq_buf");
        let _ = std::fs::remove_dir_all(&sub);
        std::fs::create_dir_all(&sub).unwrap();
        let conn = SqliteConnection::open_file(&sub.join("tpcb.sqlite")).unwrap();
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             CREATE TABLE branches (bid INTEGER PRIMARY KEY, bbalance INTEGER);
             CREATE TABLE tellers (tid INTEGER PRIMARY KEY, bid INTEGER, tbalance INTEGER);
             CREATE TABLE accounts (aid INTEGER PRIMARY KEY, bid INTEGER, abalance INTEGER);
             CREATE TABLE history (hid INTEGER PRIMARY KEY, tid INTEGER, bid INTEGER, aid INTEGER, delta INTEGER);",
        )
        .unwrap();
        let tps = run_tpcb_transactions(&conn, 1_000);
        drop(conn);
        let _ = std::fs::remove_dir_all(&sub);
        tps
    };
    let ay_tpcb_buf = {
        let sub = root.join("sstable_tpcb_ay_buf");
        let _ = std::fs::remove_dir_all(&sub);
        let fs = Arc::new(OsFs::open(&sub, true).unwrap());
        let mut cfg = EngineConfig::default();
        cfg.memtable_capacity_bytes = 128 * 1024;
        let engine = Arc::new(Mutex::new(StorageEngine::open(fs, cfg).unwrap()));
        let conn = SqliteConnection::open_in_memory().unwrap();
        conn.register_aydb_module(engine.clone()).unwrap();
        conn.execute_batch(
            "CREATE VIRTUAL TABLE branches USING aydb(table_id=1, durability='buffered', columns='bid INT PRIMARY KEY, bbalance INT');
             CREATE VIRTUAL TABLE tellers USING aydb(table_id=2, durability='buffered', columns='tid INT PRIMARY KEY, bid INT, tbalance INT');
             CREATE VIRTUAL TABLE accounts USING aydb(table_id=3, durability='buffered', columns='aid INT PRIMARY KEY, bid INT, abalance INT');
             CREATE VIRTUAL TABLE history USING aydb(table_id=4, durability='buffered', columns='hid INT PRIMARY KEY, tid INT, bid INT, aid INT, delta INT');",
        )
        .unwrap();
        let tps = run_tpcb_transactions(&conn, 1_000);
        drop(conn);
        drop(engine);
        let _ = std::fs::remove_dir_all(&sub);
        tps
    };
    println!(
        "{:<44} | {:>12.0} TPS | {:>12.0} TPS | {:>9.2}x",
        "TPC-B Buffered (128 KB MT, wal_buf vs NORMAL)",
        sq_tpcb_buf,
        ay_tpcb_buf,
        ay_tpcb_buf / sq_tpcb_buf
    );

    let sq_tpcb_full = {
        let sub = root.join("sstable_tpcb_sq_full");
        let _ = std::fs::remove_dir_all(&sub);
        std::fs::create_dir_all(&sub).unwrap();
        let conn = SqliteConnection::open_file(&sub.join("tpcb_full.sqlite")).unwrap();
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = FULL;
             CREATE TABLE branches (bid INTEGER PRIMARY KEY, bbalance INTEGER);
             CREATE TABLE tellers (tid INTEGER PRIMARY KEY, bid INTEGER, tbalance INTEGER);
             CREATE TABLE accounts (aid INTEGER PRIMARY KEY, bid INTEGER, abalance INTEGER);
             CREATE TABLE history (hid INTEGER PRIMARY KEY, tid INTEGER, bid INTEGER, aid INTEGER, delta INTEGER);",
        )
        .unwrap();
        let tps = run_tpcb_transactions(&conn, 150);
        drop(conn);
        let _ = std::fs::remove_dir_all(&sub);
        tps
    };
    let ay_tpcb_full = {
        let sub = root.join("sstable_tpcb_ay_full");
        let _ = std::fs::remove_dir_all(&sub);
        let fs = Arc::new(OsFs::open(&sub, true).unwrap());
        let mut cfg = EngineConfig::default();
        cfg.memtable_capacity_bytes = 128 * 1024;
        let engine = Arc::new(Mutex::new(StorageEngine::open(fs, cfg).unwrap()));
        let conn = SqliteConnection::open_in_memory().unwrap();
        conn.register_aydb_module(engine.clone()).unwrap();
        conn.execute_batch(
            "CREATE VIRTUAL TABLE branches USING aydb(table_id=1, durability='sync', columns='bid INT PRIMARY KEY, bbalance INT');
             CREATE VIRTUAL TABLE tellers USING aydb(table_id=2, durability='sync', columns='tid INT PRIMARY KEY, bid INT, tbalance INT');
             CREATE VIRTUAL TABLE accounts USING aydb(table_id=3, durability='sync', columns='aid INT PRIMARY KEY, bid INT, abalance INT');
             CREATE VIRTUAL TABLE history USING aydb(table_id=4, durability='sync', columns='hid INT PRIMARY KEY, tid INT, bid INT, aid INT, delta INT');",
        )
        .unwrap();
        let tps = run_tpcb_transactions(&conn, 150);
        drop(conn);
        drop(engine);
        let _ = std::fs::remove_dir_all(&sub);
        tps
    };
    println!(
        "{:<44} | {:>12.0} TPS | {:>12.0} TPS | {:>9.2}x",
        "TPC-B Full Sync (128 KB MT, wal_sync vs FULL)",
        sq_tpcb_full,
        ay_tpcb_full,
        ay_tpcb_full / sq_tpcb_full
    );
}

fn main() {
    let dir = std::env::temp_dir().join(format!("aydb_standard_sql_bench_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create bench dir");

    bench_speedtest1_suite(&dir);
    bench_ycsb_suite(&dir);
    bench_tpcb_suite(&dir);
    bench_on_disk_sstable_suite(&dir);

    let _ = std::fs::remove_dir_all(&dir);
}
