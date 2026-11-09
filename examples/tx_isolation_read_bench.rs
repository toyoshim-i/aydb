//! Measures non-owner read latency while another connection holds a large uncommitted
//! transaction (engine level). Conn A begins a transaction and writes N rows; conn B then
//! performs 10,000 `with_committed_read_for_conn(B, get_row)` calls.

use std::sync::Arc;
use std::time::Instant;

use aydb::engine::{EngineConfig, StorageEngine, WriteDurability};
use aydb::pal::OsFs;

const READS: usize = 10_000;

fn main() {
    let dir = std::env::temp_dir().join(format!("aydb_txiso_bench_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create bench dir");

    println!(
        "{:<14} | {:>16} | {:>16}",
        "uncommitted", "non-owner us/read", "owner us/read"
    );
    for n in [0usize, 10_000, 50_000] {
        let sub = dir.join(format!("n{n}"));
        std::fs::create_dir_all(&sub).unwrap();
        let fs = Arc::new(OsFs::open(&sub, true).unwrap());
        let mut eng = StorageEngine::open(Arc::clone(&fs), EngineConfig::default()).unwrap();
        for i in 0..100u32 {
            let k = format!("c{i:06}");
            eng.put_row(
                1,
                k.as_bytes(),
                b"committed",
                &[],
                WriteDurability::BufferedFast,
            )
            .unwrap();
        }
        eng.begin_transaction_for_conn(1).unwrap();
        for i in 0..n {
            let k = format!("u{i:08}");
            eng.put_row(
                1,
                k.as_bytes(),
                b"uncommitted",
                &[],
                WriteDurability::BufferedFast,
            )
            .unwrap();
        }
        let t = Instant::now();
        for i in 0..READS {
            let k = format!("c{:06}", i % 100);
            let v = eng
                .with_committed_read_for_conn(2, |e| e.get_row(1, k.as_bytes()))
                .unwrap();
            assert!(v.is_some());
        }
        let non_owner = t.elapsed().as_secs_f64() * 1e6 / READS as f64;
        let t = Instant::now();
        for i in 0..READS {
            let k = format!("c{:06}", i % 100);
            let v = eng
                .with_committed_read_for_conn(1, |e| e.get_row(1, k.as_bytes()))
                .unwrap();
            assert!(v.is_some());
        }
        let owner = t.elapsed().as_secs_f64() * 1e6 / READS as f64;
        println!("{n:<14} | {non_owner:>16.3} | {owner:>16.3}");
        eng.rollback_transaction_for_conn(1).unwrap();
    }
    let _ = std::fs::remove_dir_all(&dir);
}
