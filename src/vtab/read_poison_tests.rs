//! SQL reads on a poisoned engine, with the read mode that writes LRU promotions on reads.

use super::*;
use crate::engine::{EngineConfig, LruReadTrackingMode};
use crate::util::test_util::TempTestDir;

#[test]
fn selects_keep_working_on_a_poisoned_engine_with_eager_promotion() {
    let dir = TempTestDir::new("sql_read_poison");
    let fs = Arc::new(OsFs::open(&dir.path, true).unwrap());
    let config = EngineConfig {
        lru_read_tracking: LruReadTrackingMode::EagerWriteOnRead,
        ..EngineConfig::default()
    };
    let engine = Arc::new(RwLock::new(StorageEngine::open(fs, config).unwrap()));
    let conn = SqliteConnection::open_in_memory().unwrap();
    conn.register_aydb_module_with_options(engine.clone(), false)
        .unwrap();
    conn.execute_batch(
        "CREATE VIRTUAL TABLE capped USING aydb(max_bytes=100000, columns='k INTEGER PRIMARY KEY, u TEXT UNIQUE, v TEXT');
         CREATE VIRTUAL TABLE plain USING aydb(columns='k INTEGER PRIMARY KEY, v TEXT');
         INSERT INTO capped VALUES (1, 'a', 'x'), (2, 'b', 'y'), (3, 'c', 'z');
         INSERT INTO plain VALUES (1, 'x'), (2, 'y');
         INSERT INTO capped VALUES (4, 'd', hex(zeroblob(6000)));",
    )
    .unwrap();
    engine
        .write()
        .unwrap()
        .debug_poison("simulated WAL failure");
    assert!(
        conn.execute_batch("INSERT INTO plain VALUES (3, 'z')")
            .is_err()
    );
    for _ in 0..3 {
        for sql in [
            "SELECT count(*) FROM capped",
            "SELECT v FROM capped WHERE k = 2",
            "SELECT v FROM capped WHERE u = 'c'",
            "SELECT v FROM capped ORDER BY k DESC",
            "SELECT rowid, v FROM capped WHERE rowid = 1",
            "SELECT length(v) FROM capped WHERE k = 4",
            "SELECT length(v) FROM capped ORDER BY k",
            "SELECT v FROM plain ORDER BY k",
            "SELECT v FROM plain WHERE k = 2",
        ] {
            let rows = conn
                .query_all(sql, &[])
                .unwrap_or_else(|e| panic!("{sql}: {e}"));
            assert!(!rows.is_empty(), "{sql}");
        }
    }
    assert_eq!(
        conn.query_all("SELECT v FROM capped ORDER BY k", &[])
            .unwrap()
            .len(),
        4
    );
}
