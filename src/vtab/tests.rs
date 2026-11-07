use super::*;
use crate::engine::EngineConfig;
use crate::util::test_util::TempTestDir;
use crate::vtab::rowid_map::MAX_ROWID_MAP_ENTRIES;
use std::collections::HashMap;

#[test]
fn sqlite_vtab_point_lookup_secondary_index_range_and_capped_lru() {
    let tmp = TempTestDir::new("vtab_full");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let mut cfg = EngineConfig::default();
    cfg.inline_threshold_bytes = 256;
    let engine = Arc::new(Mutex::new(StorageEngine::open(fs, cfg).unwrap()));

    let conn = SqliteConnection::open_in_memory().unwrap();
    conn.register_aydb_module(engine.clone()).unwrap();

    // Create a Capped Virtual Table (`max_bytes = 3500`, fits at most 3 rows of ~1000 B each)
    conn.execute_batch(
        "CREATE VIRTUAL TABLE http_cache USING aydb(
            table_id = 1,
            max_bytes = 3500,
            durability = 'buffered',
            columns = 'url TEXT PRIMARY KEY, status INT, body BLOB, content_type TEXT',
            indexes = 'content_type, status'
        );",
    )
    .unwrap();

    // Verify EXPLAIN QUERY PLAN uses Plan 1 (`idxNum = 1`) for `WHERE url = ?`
    let eqp_pk = conn
        .query_all(
            "EXPLAIN QUERY PLAN SELECT body FROM http_cache WHERE url = 'https://a.com'",
            &[],
        )
        .unwrap();
    assert!(!eqp_pk.is_empty());

    let mut insert_stmt = conn
        .prepare("INSERT OR REPLACE INTO http_cache VALUES (?1, ?2, ?3, ?4)")
        .unwrap();

    let blob_1k = vec![0xAAu8; 1000];
    insert_stmt
        .execute(&[
            SqlValue::Text("https://site.example/1".into()),
            SqlValue::Integer(200),
            SqlValue::Blob(blob_1k.clone()),
            SqlValue::Text("text/html".into()),
        ])
        .unwrap();
    insert_stmt
        .execute(&[
            SqlValue::Text("https://site.example/2".into()),
            SqlValue::Integer(200),
            SqlValue::Blob(blob_1k.clone()),
            SqlValue::Text("application/json".into()),
        ])
        .unwrap();
    insert_stmt
        .execute(&[
            SqlValue::Text("https://site.example/3".into()),
            SqlValue::Integer(404),
            SqlValue::Blob(blob_1k.clone()),
            SqlValue::Text("text/html".into()),
        ])
        .unwrap();

    // 1. Point Lookup on PK (`WHERE url = ?`) — also touches `https://site.example/1` in AccessTracker!
    let hit1 = conn
        .query_all(
            "SELECT status, length(body) FROM http_cache WHERE url = ?1",
            &[SqlValue::Text("https://site.example/1".into())],
        )
        .unwrap();
    assert_eq!(
        hit1,
        vec![vec![SqlValue::Integer(200), SqlValue::Integer(1000)]]
    );

    // 2. Secondary Index Equality Lookup (`WHERE content_type = 'text/html'`)
    let html_urls = conn
        .query_all(
            "SELECT url FROM http_cache WHERE content_type = ?1 ORDER BY url",
            &[SqlValue::Text("text/html".into())],
        )
        .unwrap();
    assert_eq!(
        html_urls,
        vec![
            vec![SqlValue::Text("https://site.example/1".into())],
            vec![SqlValue::Text("https://site.example/3".into())],
        ]
    );

    // 3. Insert 4th row (~1000 B) -> exceeds `max_bytes = 3500`!
    // Because `/1` and `/3` were just read above, `/2` is the coldest un-accessed entry and must be evicted!
    insert_stmt
        .execute(&[
            SqlValue::Text("https://site.example/4".into()),
            SqlValue::Integer(201),
            SqlValue::Blob(blob_1k.clone()),
            SqlValue::Text("image/png".into()),
        ])
        .unwrap();

    let remaining = conn
        .query_all("SELECT url FROM http_cache ORDER BY url", &[])
        .unwrap();
    assert_eq!(
        remaining,
        vec![
            vec![SqlValue::Text("https://site.example/1".into())],
            vec![SqlValue::Text("https://site.example/3".into())],
            vec![SqlValue::Text("https://site.example/4".into())],
        ]
    );

    // 4. Primary Key Range Query (`WHERE url >= .../3 AND url <= .../4`)
    let range_rows = conn
        .query_all(
            "SELECT url, status FROM http_cache WHERE url >= ?1 AND url <= ?2 ORDER BY url",
            &[
                SqlValue::Text("https://site.example/3".into()),
                SqlValue::Text("https://site.example/4".into()),
            ],
        )
        .unwrap();
    assert_eq!(
        range_rows,
        vec![
            vec![
                SqlValue::Text("https://site.example/3".into()),
                SqlValue::Integer(404)
            ],
            vec![
                SqlValue::Text("https://site.example/4".into()),
                SqlValue::Integer(201)
            ],
        ]
    );

    // 5. SQL UPDATE & DELETE via Virtual Table
    conn.execute_batch(
        "UPDATE http_cache SET status = 500 WHERE url = 'https://site.example/3';
         DELETE FROM http_cache WHERE url = 'https://site.example/4';",
    )
    .unwrap();

    let after_mut = conn
        .query_all("SELECT url, status FROM http_cache ORDER BY url", &[])
        .unwrap();
    assert_eq!(
        after_mut,
        vec![
            vec![
                SqlValue::Text("https://site.example/1".into()),
                SqlValue::Integer(200)
            ],
            vec![
                SqlValue::Text("https://site.example/3".into()),
                SqlValue::Integer(500)
            ],
        ]
    );

    // 6. Verify ACID ROLLBACK restores pre-transaction state cleanly
    conn.execute_batch(
        "BEGIN;
         DELETE FROM http_cache WHERE url = 'https://site.example/1';
         UPDATE http_cache SET status = 999 WHERE url = 'https://site.example/3';
         ROLLBACK;",
    )
    .unwrap();

    let after_rollback = conn
        .query_all("SELECT url, status FROM http_cache ORDER BY url", &[])
        .unwrap();
    assert_eq!(after_rollback, after_mut);
}

#[test]
fn sqlite_vtab_real_pk_8byte_text_pk_crc_collision_and_limit_pushdown() {
    let tmp = TempTestDir::new("vtab_regressions");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let engine = Arc::new(Mutex::new(
        StorageEngine::open(fs, EngineConfig::default()).unwrap(),
    ));
    let conn = SqliteConnection::open_in_memory().unwrap();
    conn.register_aydb_module(engine).unwrap();

    // 1. REAL (f64) primary key ordering & range scans across negative, -0.0/+0.0, and positive floats
    conn.execute_batch(
        "CREATE VIRTUAL TABLE real_t USING aydb(
            table_id = 10,
            columns = 'k REAL PRIMARY KEY, v TEXT'
        );
        INSERT INTO real_t VALUES (-10.0, 'a'), (-2.0, 'b'), (-1.0, 'c'), (-0.0, 'z0'), (1.0, 'e'), (10.0, 'f');
        INSERT OR REPLACE INTO real_t VALUES (0.0, 'd');",
    )
    .unwrap();

    let all_real = conn.query_all("SELECT k, v FROM real_t", &[]).unwrap();
    assert_eq!(
        all_real,
        vec![
            vec![SqlValue::Float(-10.0), SqlValue::Text("a".into())],
            vec![SqlValue::Float(-2.0), SqlValue::Text("b".into())],
            vec![SqlValue::Float(-1.0), SqlValue::Text("c".into())],
            vec![SqlValue::Float(0.0), SqlValue::Text("d".into())],
            vec![SqlValue::Float(1.0), SqlValue::Text("e".into())],
            vec![SqlValue::Float(10.0), SqlValue::Text("f".into())],
        ]
    );

    let range_real = conn
        .query_all(
            "SELECT k FROM real_t WHERE k >= ?1 AND k <= ?2",
            &[SqlValue::Float(-2.0), SqlValue::Float(1.0)],
        )
        .unwrap();
    assert_eq!(
        range_real,
        vec![
            vec![SqlValue::Float(-2.0)],
            vec![SqlValue::Float(-1.0)],
            vec![SqlValue::Float(0.0)],
            vec![SqlValue::Float(1.0)],
        ]
    );

    // 2. 8-byte TEXT primary keys ("user0001") UPDATE and DELETE via SQLite vtab_update
    conn.execute_batch(
        "CREATE VIRTUAL TABLE text8_t USING aydb(
            table_id = 11,
            columns = 'id TEXT PRIMARY KEY, val INT, tag TEXT',
            indexes = 'tag'
        );
        INSERT INTO text8_t VALUES ('user0001', 10, 'alpha');
        INSERT INTO text8_t VALUES ('user0002', 20, ' (empty) ');
        INSERT INTO text8_t VALUES ('user0003', 30, NULL);
        INSERT INTO text8_t VALUES ('user0004', 40, '');",
    )
    .unwrap();

    conn.execute_batch(
        "UPDATE text8_t SET val = 111 WHERE id = 'user0001';
         DELETE FROM text8_t WHERE id = 'user0002';",
    )
    .unwrap();

    let text8_rows = conn
        .query_all("SELECT id, val FROM text8_t ORDER BY id", &[])
        .unwrap();
    assert_eq!(
        text8_rows,
        vec![
            vec![SqlValue::Text("user0001".into()), SqlValue::Integer(111)],
            vec![SqlValue::Text("user0003".into()), SqlValue::Integer(30)],
            vec![SqlValue::Text("user0004".into()), SqlValue::Integer(40)],
        ]
    );

    // Also check NULL vs '' on secondary index `tag`
    let empty_tag_rows = conn
        .query_all("SELECT id FROM text8_t WHERE tag = ''", &[])
        .unwrap();
    assert_eq!(
        empty_tag_rows,
        vec![vec![SqlValue::Text("user0004".into())]]
    );

    // 3. 32-bit CRC32C-colliding TEXT primary keys UPDATE and DELETE
    let mut seen_crc: HashMap<u32, String> = HashMap::with_capacity(100_000);
    let mut pair: Option<(String, String)> = None;
    let mut rng = 0x9E37_79B9_7F4A_7C15u64;
    for _ in 0..500_000u32 {
        rng ^= rng >> 12;
        rng ^= rng << 25;
        rng ^= rng >> 27;
        let val = rng.wrapping_mul(0x2545_F491_4F6C_DD1D);
        let candidate = format!("user_{val:016x}");
        let h = crate::util::crc32c::crc32c(candidate.as_bytes());
        if let Some(prev) = seen_crc.insert(h, candidate.clone()) {
            if prev != candidate {
                pair = Some((prev, candidate));
                break;
            }
        }
    }
    let (k1, k2) = pair.expect("found 32-bit CRC32C collision");
    conn.execute_batch(
        "CREATE VIRTUAL TABLE crc_users USING aydb(
            table_id = 12,
            columns = 'id TEXT PRIMARY KEY, name TEXT'
        );",
    )
    .unwrap();
    conn.execute_batch(&format!(
        "INSERT INTO crc_users VALUES ('{k1}', 'Alice'), ('{k2}', 'Bob');"
    ))
    .unwrap();
    let r1 = conn
        .query_all(
            &format!("SELECT rowid FROM crc_users WHERE id = '{k1}'"),
            &[],
        )
        .unwrap();
    let rid1 = match r1[0][0] {
        SqlValue::Integer(v) => v,
        _ => unreachable!(),
    };
    conn.execute_batch(&format!(
        "UPDATE crc_users SET name = 'BobUpdated' WHERE id = '{k2}';
         DELETE FROM crc_users WHERE rowid = {rid1};"
    ))
    .unwrap();
    let k1_rows = conn
        .query_all(&format!("SELECT name FROM crc_users WHERE id = '{k1}'"), &[])
        .unwrap();
    let k2_rows = conn
        .query_all(&format!("SELECT name FROM crc_users WHERE id = '{k2}'"), &[])
        .unwrap();
    assert!(k1_rows.is_empty());
    assert_eq!(k2_rows, vec![vec![SqlValue::Text("BobUpdated".into())]]);

    // 4. SELECT ... WHERE id >= ? AND other_col = ? LIMIT ? with non-matching rows preceding matching rows
    conn.execute_batch(
        "CREATE VIRTUAL TABLE items_t USING aydb(
            table_id = 13,
            columns = 'id INT PRIMARY KEY, other_col TEXT'
        );",
    )
    .unwrap();
    let mut ins_item = conn.prepare("INSERT INTO items_t VALUES (?1, ?2)").unwrap();
    for i in 1..=20 {
        let cat = if i <= 10 { "skip" } else { "match" };
        ins_item
            .execute(&[SqlValue::Integer(i), SqlValue::Text(cat.into())])
            .unwrap();
    }
    let matched = conn
        .query_all(
            "SELECT id FROM items_t WHERE id >= ?1 AND other_col = ?2 LIMIT ?3",
            &[
                SqlValue::Integer(1),
                SqlValue::Text("match".into()),
                SqlValue::Integer(5),
            ],
        )
        .unwrap();
    assert_eq!(
        matched,
        vec![
            vec![SqlValue::Integer(11)],
            vec![SqlValue::Integer(12)],
            vec![SqlValue::Integer(13)],
            vec![SqlValue::Integer(14)],
            vec![SqlValue::Integer(15)],
        ]
    );
}

#[test]
fn pk_uniqueness_on_conflict_and_table_id_registry() {
    let tmp = TempTestDir::new("vtab_pk_uniqueness");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let engine = Arc::new(Mutex::new(
        StorageEngine::open(fs, EngineConfig::default()).unwrap(),
    ));
    let conn = SqliteConnection::open_in_memory().unwrap();
    conn.register_aydb_module(engine.clone()).unwrap();

    // Part A: Primary key uniqueness and ON CONFLICT resolution modes
    conn.execute_batch(
        "CREATE VIRTUAL TABLE users USING aydb(
            table_id = 20,
            columns = 'id INT PRIMARY KEY, name TEXT, role TEXT',
            indexes = 'role'
        );
        INSERT INTO users VALUES (1, 'Alice', 'admin'), (2, 'Bob', 'user');",
    )
    .unwrap();

    // Plain INSERT with duplicate PK must fail with SQLITE_CONSTRAINT
    let dup_err = conn.execute_batch("INSERT INTO users VALUES (1, 'Imposter', 'hacker');");
    assert!(dup_err.is_err(), "duplicate PK on plain INSERT must fail");

    // Verify original row is intact
    let rows = conn
        .query_all("SELECT id, name, role FROM users ORDER BY id", &[])
        .unwrap();
    assert_eq!(
        rows,
        vec![
            vec![
                SqlValue::Integer(1),
                SqlValue::Text("Alice".into()),
                SqlValue::Text("admin".into())
            ],
            vec![
                SqlValue::Integer(2),
                SqlValue::Text("Bob".into()),
                SqlValue::Text("user".into())
            ],
        ]
    );

    // INSERT OR IGNORE skips conflicting PK 1 and inserts non-conflicting PK 3
    conn.execute_batch(
        "INSERT OR IGNORE INTO users VALUES (1, 'Ignored', 'guest'), (3, 'Charlie', 'mod');",
    )
    .unwrap();
    let rows = conn
        .query_all("SELECT id, name FROM users ORDER BY id", &[])
        .unwrap();
    assert_eq!(
        rows,
        vec![
            vec![SqlValue::Integer(1), SqlValue::Text("Alice".into())],
            vec![SqlValue::Integer(2), SqlValue::Text("Bob".into())],
            vec![SqlValue::Integer(3), SqlValue::Text("Charlie".into())],
        ]
    );

    // INSERT OR REPLACE overwrites PK 1 and updates secondary index `role`
    conn.execute_batch("INSERT OR REPLACE INTO users VALUES (1, 'AliceV2', 'superadmin');")
        .unwrap();
    let old_role = conn
        .query_all("SELECT id FROM users WHERE role = 'admin'", &[])
        .unwrap();
    assert!(old_role.is_empty());
    let new_role = conn
        .query_all("SELECT name FROM users WHERE role = 'superadmin'", &[])
        .unwrap();
    assert_eq!(new_role, vec![vec![SqlValue::Text("AliceV2".into())]]);

    // UPDATE changing PK 3 -> 2 (collision): plain UPDATE must fail and keep PK 3 intact
    let upd_err = conn.execute_batch("UPDATE users SET id = 2 WHERE id = 3;");
    assert!(upd_err.is_err(), "UPDATE to existing PK must fail");
    let check3 = conn
        .query_all("SELECT name FROM users WHERE id = 3", &[])
        .unwrap();
    assert_eq!(check3, vec![vec![SqlValue::Text("Charlie".into())]]);

    // UPDATE OR IGNORE changing PK 3 -> 2 must leave both PK 2 and PK 3 untouched
    conn.execute_batch("UPDATE OR IGNORE users SET id = 2, name = 'NoOp' WHERE id = 3;")
        .unwrap();
    let rows = conn
        .query_all("SELECT id, name FROM users ORDER BY id", &[])
        .unwrap();
    assert_eq!(
        rows,
        vec![
            vec![SqlValue::Integer(1), SqlValue::Text("AliceV2".into())],
            vec![SqlValue::Integer(2), SqlValue::Text("Bob".into())],
            vec![SqlValue::Integer(3), SqlValue::Text("Charlie".into())],
        ]
    );

    // UPDATE OR REPLACE changing PK 3 -> 2 deletes PK 3 and overwrites PK 2
    conn.execute_batch(
        "UPDATE OR REPLACE users SET id = 2, name = 'CharlieReplacedBob' WHERE id = 3;",
    )
    .unwrap();
    let rows = conn
        .query_all("SELECT id, name FROM users ORDER BY id", &[])
        .unwrap();
    assert_eq!(
        rows,
        vec![
            vec![SqlValue::Integer(1), SqlValue::Text("AliceV2".into())],
            vec![
                SqlValue::Integer(2),
                SqlValue::Text("CharlieReplacedBob".into())
            ],
        ]
    );

    // Part B: Auto-assigned table_id isolation and duplicate explicit table_id rejection
    conn.execute_batch(
        "CREATE VIRTUAL TABLE auto_a USING aydb(columns = 'k INT PRIMARY KEY, v TEXT');
         CREATE VIRTUAL TABLE auto_b USING aydb(columns = 'k INT PRIMARY KEY, v TEXT');
         INSERT INTO auto_a VALUES (1, 'from_a');
         INSERT INTO auto_b VALUES (1, 'from_b');",
    )
    .unwrap();
    let rows_a = conn.query_all("SELECT v FROM auto_a WHERE k = 1", &[]).unwrap();
    let rows_b = conn.query_all("SELECT v FROM auto_b WHERE k = 1", &[]).unwrap();
    assert_eq!(rows_a, vec![vec![SqlValue::Text("from_a".into())]]);
    assert_eq!(rows_b, vec![vec![SqlValue::Text("from_b".into())]]);

    // Explicit duplicate table_id on a second live table must fail
    let dup_tid = conn.execute_batch(
        "CREATE VIRTUAL TABLE clash_users USING aydb(
            table_id = 20,
            columns = 'id INT PRIMARY KEY, name TEXT'
        );",
    );
    assert!(dup_tid.is_err(), "duplicate live table_id must be rejected");
}

#[test]
fn storage_class_key_tags_auto_rowid_and_mixed_range() {
    let tmp = TempTestDir::new("vtab_storage_class_tags");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let engine = Arc::new(Mutex::new(
        StorageEngine::open(fs, EngineConfig::default()).unwrap(),
    ));
    let conn = SqliteConnection::open_in_memory().unwrap();
    conn.register_aydb_module(engine).unwrap();

    // 1. Storage-class key tags prevent collisions between INTEGER, REAL, TEXT, and BLOB,
    // while exact integer/float equality (`42 == 42.0`) enforces uniqueness.
    conn.execute_batch(
        "CREATE VIRTUAL TABLE poly_t USING aydb(
            table_id = 30,
            columns = 'pk BLOB PRIMARY KEY, sec BLOB, tag TEXT',
            indexes = 'sec'
        );
        INSERT INTO poly_t VALUES (42, 100, 'int_42');
        INSERT INTO poly_t VALUES (42.5, 100.5, 'real_42_5');
        INSERT INTO poly_t VALUES ('', '', 'empty_text');
        INSERT INTO poly_t VALUES (X'', X'', 'empty_blob');
        INSERT INTO poly_t VALUES ('abc', 'abc', 'text_abc');
        INSERT INTO poly_t VALUES (X'616263', X'616263', 'blob_abc');
        INSERT INTO poly_t VALUES ('01234567', NULL, 'text_8byte');",
    )
    .unwrap();

    // Inserting 42.0 when 42 is already present must fail with SQLITE_CONSTRAINT
    assert!(
        conn.execute_batch("INSERT INTO poly_t VALUES (42.0, 100.0, 'dup_real_42');")
            .is_err(),
        "42.0 must conflict with existing integer 42 on BLOB PRIMARY KEY"
    );

    // All 7 distinct values must coexist without overwriting each other, ordered by storage class
    let ordered_tags = conn.query_all("SELECT tag FROM poly_t", &[]).unwrap();
    assert_eq!(
        ordered_tags,
        vec![
            vec![SqlValue::Text("int_42".into())],
            vec![SqlValue::Text("real_42_5".into())],
            vec![SqlValue::Text("empty_text".into())],
            vec![SqlValue::Text("text_8byte".into())],
            vec![SqlValue::Text("text_abc".into())],
            vec![SqlValue::Text("empty_blob".into())],
            vec![SqlValue::Text("blob_abc".into())],
        ]
    );

    // Point lookups on PK distinguish 'abc' (TEXT) from X'616263' (BLOB) and 42 from 42.0
    let hit_text = conn
        .query_all("SELECT tag FROM poly_t WHERE pk = 'abc'", &[])
        .unwrap();
    let hit_blob = conn
        .query_all("SELECT tag FROM poly_t WHERE pk = X'616263'", &[])
        .unwrap();
    assert_eq!(hit_text, vec![vec![SqlValue::Text("text_abc".into())]]);
    assert_eq!(hit_blob, vec![vec![SqlValue::Text("blob_abc".into())]]);

    // Secondary index lookups also distinguish 'abc' vs X'616263' and '' vs X''
    let sec_text = conn
        .query_all("SELECT tag FROM poly_t WHERE sec = 'abc'", &[])
        .unwrap();
    let sec_blob = conn
        .query_all("SELECT tag FROM poly_t WHERE sec = X'616263'", &[])
        .unwrap();
    let sec_empty_str = conn
        .query_all("SELECT tag FROM poly_t WHERE sec = ''", &[])
        .unwrap();
    let sec_empty_blob = conn
        .query_all("SELECT tag FROM poly_t WHERE sec = X''", &[])
        .unwrap();
    assert_eq!(sec_text, vec![vec![SqlValue::Text("text_abc".into())]]);
    assert_eq!(sec_blob, vec![vec![SqlValue::Text("blob_abc".into())]]);
    assert_eq!(
        sec_empty_str,
        vec![vec![SqlValue::Text("empty_text".into())]]
    );
    assert_eq!(
        sec_empty_blob,
        vec![vec![SqlValue::Text("empty_blob".into())]]
    );

    // 2. NULL into INTEGER PRIMARY KEY auto-generates monotonic rowid; NULL into TEXT PK fails
    conn.execute_batch(
        "CREATE VIRTUAL TABLE auto_pk_t USING aydb(
            table_id = 31,
            columns = 'id INT PRIMARY KEY, label TEXT'
        );
        INSERT INTO auto_pk_t VALUES (NULL, 'first');
        INSERT INTO auto_pk_t VALUES (NULL, 'second');
        INSERT INTO auto_pk_t VALUES (10, 'ten');
        INSERT INTO auto_pk_t VALUES (NULL, 'eleven');",
    )
    .unwrap();

    let auto_rows = conn
        .query_all("SELECT id, label FROM auto_pk_t ORDER BY id", &[])
        .unwrap();
    assert_eq!(
        auto_rows,
        vec![
            vec![SqlValue::Integer(1), SqlValue::Text("first".into())],
            vec![SqlValue::Integer(2), SqlValue::Text("second".into())],
            vec![SqlValue::Integer(10), SqlValue::Text("ten".into())],
            vec![SqlValue::Integer(11), SqlValue::Text("eleven".into())],
        ]
    );

    // Mixed-type numeric range scan (`id > 1.5 AND id < 10.5` on INTEGER PRIMARY KEY)
    let mixed_range = conn
        .query_all(
            "SELECT id FROM auto_pk_t WHERE id > 1.5 AND id < 10.5 ORDER BY id",
            &[],
        )
        .unwrap();
    assert_eq!(
        mixed_range,
        vec![vec![SqlValue::Integer(2)], vec![SqlValue::Integer(10)]]
    );

    // Reject NULL on UPDATE of PK or INSERT into non-integer PK
    assert!(
        conn.execute_batch("UPDATE auto_pk_t SET id = NULL WHERE id = 1;")
            .is_err()
    );
    conn.execute_batch(
        "CREATE VIRTUAL TABLE str_pk_t USING aydb(
            table_id = 32,
            columns = 'k TEXT PRIMARY KEY, v INT'
        );",
    )
    .unwrap();
    assert!(
        conn.execute_batch("INSERT INTO str_pk_t VALUES (NULL, 1);")
            .is_err()
    );
}

#[test]
fn io_and_corrupt_errors_propagate_from_filter_and_update() {
    let tmp = TempTestDir::new("vtab_corrupt_err_propagation");
    let cfg = EngineConfig::default();
    {
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
        let engine = Arc::new(Mutex::new(StorageEngine::open(fs, cfg.clone()).unwrap()));
        let conn = SqliteConnection::open_in_memory().unwrap();
        conn.register_aydb_module(engine.clone()).unwrap();

        conn.execute_batch(
            "CREATE VIRTUAL TABLE docs USING aydb(
                table_id = 40,
                columns = 'id INT PRIMARY KEY, payload BLOB'
            );
            BEGIN;",
        )
        .unwrap();
        let mut ins = conn.prepare("INSERT INTO docs VALUES (?1, ?2)").unwrap();
        for i in 1..=60 {
            ins.execute(&[SqlValue::Integer(i), SqlValue::Blob(vec![0x55u8; 128])])
                .unwrap();
        }
        conn.execute_batch("COMMIT;").unwrap();

        // Flush to disk so `idx_*.db` has multiple 4 KB blocks (block 0 has catalog, block 1 has id=60)
        engine.lock().unwrap().flush_memtable().unwrap();
    }

    // Corrupt the second 4 KB data block (offset 4096 + 16) of `idx_*.db` while handles are closed
    let idx_file = std::fs::read_dir(&tmp.path)
        .unwrap()
        .filter_map(|e| e.ok())
        .find(|e| {
            let name = e.file_name().to_string_lossy().into_owned();
            name.starts_with("idx_") && name.ends_with(".db")
        })
        .expect("flushed idx segment file")
        .path();
    let mut idx_bytes = std::fs::read(&idx_file).unwrap();
    let mut blk_off = 4096usize;
    while blk_off + 4096 <= idx_bytes.len() {
        idx_bytes[blk_off + 16] ^= 0xFF;
        blk_off += 4096;
    }
    std::fs::write(&idx_file, &idx_bytes).unwrap();

    // Reopen engine and VTab; point query, full scan, and DELETE/UPDATE on block 1 must return Err
    let fs = Arc::new(OsFs::open(&tmp.path, false).unwrap());
    let engine = Arc::new(Mutex::new(StorageEngine::open(fs, cfg).unwrap()));
    let conn = SqliteConnection::open_in_memory().unwrap();
    conn.register_aydb_module(engine).unwrap();
    conn.execute_batch(
        "CREATE VIRTUAL TABLE docs USING aydb(
            table_id = 40,
            columns = 'id INT PRIMARY KEY, payload BLOB'
        );",
    )
    .unwrap();

    let point_res = conn.query_all("SELECT payload FROM docs WHERE id = 60", &[]);
    assert!(
        point_res.is_err(),
        "corrupted idx block read in xFilter point lookup must propagate error"
    );
    let scan_res = conn.query_all("SELECT payload FROM docs", &[]);
    assert!(
        scan_res.is_err(),
        "corrupted idx block read in xFilter scan must propagate error"
    );
    let del_res = conn.execute_batch("DELETE FROM docs WHERE id = 60;");
    assert!(
        del_res.is_err(),
        "corrupted idx block read during DELETE/xUpdate must propagate error"
    );
}

#[test]
fn full_scan_limit_pushdown_and_rowid_map_compaction() {
    let tmp = TempTestDir::new("vtab_limit_and_rowid_map");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let engine = Arc::new(Mutex::new(
        StorageEngine::open(fs, EngineConfig::default()).unwrap(),
    ));
    let conn = SqliteConnection::open_in_memory().unwrap();
    conn.register_aydb_module(engine).unwrap();

    conn.execute_batch(
        "CREATE VIRTUAL TABLE kv_text USING aydb(
            table_id = 50,
            columns = 'k TEXT PRIMARY KEY, v INT'
        );
        BEGIN;",
    )
    .unwrap();
    {
        let mut ins = conn.prepare("INSERT INTO kv_text VALUES (?1, ?2)").unwrap();
        for i in 0..17_000i64 {
            ins.execute(&[SqlValue::Text(format!("key_{i:06}")), SqlValue::Integer(i)])
                .unwrap();
        }
    }
    conn.execute_batch("COMMIT;").unwrap();

    // Full scan with LIMIT 5 (without WHERE clause) streams only the first chunk
    let top5 = conn
        .query_all("SELECT k, v FROM kv_text LIMIT 5", &[])
        .unwrap();
    assert_eq!(top5.len(), 5);
    assert_eq!(
        top5[0],
        vec![SqlValue::Text("key_000000".into()), SqlValue::Integer(0)]
    );
    assert_eq!(
        top5[4],
        vec![SqlValue::Text("key_000004".into()), SqlValue::Integer(4)]
    );

    // Scanning all 17,000 non-integer PK rows with rowid projection and closing cursor
    // compacts `RowidMap` when cursor closes, and subsequent UPDATE/DELETE still works!
    let all_rowids = conn.query_all("SELECT rowid FROM kv_text", &[]).unwrap();
    assert_eq!(all_rowids.len(), 17_000);
    conn.execute_batch(
        "UPDATE kv_text SET v = 999999 WHERE k = 'key_000000';
         DELETE FROM kv_text WHERE k = 'key_000001';",
    )
    .unwrap();
    let check0 = conn
        .query_all("SELECT v FROM kv_text WHERE k = 'key_000000'", &[])
        .unwrap();
    let check1 = conn
        .query_all("SELECT v FROM kv_text WHERE k = 'key_000001'", &[])
        .unwrap();
    assert_eq!(check0, vec![vec![SqlValue::Integer(999999)]]);
    assert!(check1.is_empty());
}

#[test]
fn savepoints_drop_table_paren_columns_and_panic_safety() {
    let tmp = TempTestDir::new("vtab_savepoints_drop_table");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let engine = Arc::new(Mutex::new(
        StorageEngine::open(fs, EngineConfig::default()).unwrap(),
    ));
    let conn = SqliteConnection::open_in_memory().unwrap();
    conn.register_aydb_module(engine.clone()).unwrap();

    // Columns containing commas inside parentheses (`DECIMAL(10, 2)`, `NUMERIC(5, 3)`)
    conn.execute_batch(
        "CREATE VIRTUAL TABLE products USING aydb(
            table_id = 60,
            columns = 'id INT PRIMARY KEY, price DECIMAL(10, 2), weight NUMERIC(5, 3), name VARCHAR(64)',
            indexes = 'name'
        );
        INSERT INTO products VALUES (1, 19.99, 1.25, 'Widget');",
    )
    .unwrap();
    let prod_rows = conn
        .query_all("SELECT id, price, weight, name FROM products WHERE name = 'Widget'", &[])
        .unwrap();
    assert_eq!(
        prod_rows,
        vec![vec![
            SqlValue::Integer(1),
            SqlValue::Float(19.99),
            SqlValue::Float(1.25),
            SqlValue::Text("Widget".into()),
        ]]
    );

    // Nested SAVEPOINT, ROLLBACK TO, and RELEASE
    conn.execute_batch(
        "BEGIN;
         INSERT INTO products VALUES (2, 29.99, 2.50, 'Gadget');
         SAVEPOINT sp1;
         INSERT INTO products VALUES (3, 39.99, 3.75, 'Sprocket');
         UPDATE products SET price = 99.99 WHERE id = 1;
         SAVEPOINT sp2;
         DELETE FROM products WHERE id = 2;
         ROLLBACK TO sp2;
         RELEASE sp2;
         ROLLBACK TO sp1;
         RELEASE sp1;
         COMMIT;",
    )
    .unwrap();

    // After rolling back to sp1 and committing: id=1 still has 19.99, id=2 exists, id=3 was rolled back
    let after_sp = conn
        .query_all("SELECT id, price, name FROM products ORDER BY id", &[])
        .unwrap();
    assert_eq!(
        after_sp,
        vec![
            vec![
                SqlValue::Integer(1),
                SqlValue::Float(19.99),
                SqlValue::Text("Widget".into())
            ],
            vec![
                SqlValue::Integer(2),
                SqlValue::Float(29.99),
                SqlValue::Text("Gadget".into())
            ],
        ]
    );
    // Secondary index for rolled-back 'Sprocket' must also be clean
    let sprocket = conn
        .query_all("SELECT id FROM products WHERE name = 'Sprocket'", &[])
        .unwrap();
    assert!(sprocket.is_empty());

    // DROP TABLE (xDestroy) removes all rows, secondary indexes, and catalog entry
    conn.execute_batch("DROP TABLE products;").unwrap();
    conn.execute_batch(
        "CREATE VIRTUAL TABLE products USING aydb(
            table_id = 60,
            columns = 'id INT PRIMARY KEY, price DECIMAL(10, 2), weight NUMERIC(5, 3), name VARCHAR(64)',
            indexes = 'name'
        );",
    )
    .unwrap();
    let after_drop = conn.query_all("SELECT * FROM products", &[]).unwrap();
    assert!(
        after_drop.is_empty(),
        "recreated table after DROP TABLE must be empty"
    );
}

#[test]
fn cross_connection_transaction_isolation_and_two_phase_sync_rollback() {
    let tmp = TempTestDir::new("vtab_conn_isolation");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let shared = Arc::new(Mutex::new(
        StorageEngine::open(fs, EngineConfig::default()).unwrap(),
    ));

    let conn1 = SqliteConnection::open_in_memory().unwrap();
    conn1.register_aydb_module(shared.clone()).unwrap();
    conn1
        .execute_batch(
            "CREATE VIRTUAL TABLE t USING aydb(
                table_id = 1,
                columns = 'id INTEGER PRIMARY KEY, v TEXT'
            );
            INSERT INTO t VALUES (1, 'one'), (2, 'two');",
        )
        .unwrap();

    let conn2 = SqliteConnection::open_in_memory().unwrap();
    conn2.register_aydb_module(shared.clone()).unwrap();
    conn2
        .execute_batch(
            "CREATE VIRTUAL TABLE t USING aydb(
                table_id = 1,
                columns = 'id INTEGER PRIMARY KEY, v TEXT'
            );",
        )
        .unwrap();

    // 1. Connection 1 opens a transaction and inserts id=3 ('uncommitted'), updates id=1, deletes id=2
    conn1
        .execute_batch(
            "BEGIN;
             INSERT INTO t VALUES (3, 'uncommitted');
             UPDATE t SET v = 'one_modified' WHERE id = 1;
             DELETE FROM t WHERE id = 2;",
        )
        .unwrap();

    // Connection 1 sees its own uncommitted changes (read-your-own-writes)
    assert_eq!(
        conn1
            .query_all("SELECT id, v FROM t WHERE id = 3", &[])
            .unwrap(),
        vec![vec![
            SqlValue::Integer(3),
            SqlValue::Text("uncommitted".into())
        ]]
    );

    // 2. Connection 2 MUST NOT see Connection 1's uncommitted changes!
    assert!(
        conn2
            .query_all("SELECT * FROM t WHERE id = 3", &[])
            .unwrap()
            .is_empty(),
        "second connection must not see uncommitted row from first connection"
    );
    assert_eq!(
        conn2
            .query_all("SELECT id, v FROM t ORDER BY id", &[])
            .unwrap(),
        vec![
            vec![SqlValue::Integer(1), SqlValue::Text("one".into())],
            vec![SqlValue::Integer(2), SqlValue::Text("two".into())],
        ],
        "second connection must read the exact pre-transaction committed snapshot"
    );

    // 3. Connection 2 attempting an autocommit write while Connection 1 holds the transaction
    //    must fail with SQLITE_BUSY rather than silently joining Connection 1's transaction!
    assert!(
        conn2
            .execute_batch("INSERT INTO t VALUES (4, 'other committed');")
            .is_err(),
        "concurrent write on second connection while first connection is in a transaction must fail with busy error"
    );

    // 4. Connection 1 rolls back
    conn1.execute_batch("ROLLBACK;").unwrap();

    // 5. Now Connection 2's write succeeds and persists cleanly!
    conn2
        .execute_batch("INSERT INTO t VALUES (4, 'other committed');")
        .unwrap();
    assert_eq!(
        conn1
            .query_all("SELECT id, v FROM t WHERE id = 4", &[])
            .unwrap(),
        vec![vec![
            SqlValue::Integer(4),
            SqlValue::Text("other committed".into())
        ]]
    );
}

#[test]
fn affinity_collation_unified_numeric_sorting_and_max_rowid() {
    let tmp = TempTestDir::new("vtab_affinity_collation");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let shared = Arc::new(Mutex::new(
        StorageEngine::open(fs, EngineConfig::default()).unwrap(),
    ));
    let conn = SqliteConnection::open_in_memory().unwrap();
    conn.register_aydb_module(shared).unwrap();

    // 1. INTEGER PRIMARY KEY affinity coercion on lookup (`WHERE id=1.0`, `WHERE id='1'`, `WHERE +id=1.0`)
    conn.execute_batch(
        "CREATE VIRTUAL TABLE t_int USING aydb(
            table_id = 70,
            columns = 'id INTEGER PRIMARY KEY, score INT, label TEXT',
            indexes = 'score'
        );
        INSERT INTO t_int VALUES (1, 100, 'one'), (2, 200, 'two');",
    )
    .unwrap();

    for sql in [
        "SELECT id FROM t_int WHERE id = 1.0",
        "SELECT id FROM t_int WHERE id = '1'",
        "SELECT id FROM t_int WHERE +id = 1.0",
        "SELECT id FROM t_int WHERE score = 100.0",
        "SELECT id FROM t_int WHERE score = '100'",
    ] {
        assert_eq!(
            conn.query_all(sql, &[]).unwrap(),
            vec![vec![SqlValue::Integer(1)]],
            "affinity query failed: {sql}"
        );
    }

    // Inserting '3' or 4.0 into INTEGER PRIMARY KEY stores integer values
    conn.execute_batch("INSERT INTO t_int VALUES ('3', '300', 'three'), (4.0, 400.0, 'four');")
        .unwrap();
    assert_eq!(
        conn.query_all("SELECT id, score FROM t_int WHERE id >= 3 ORDER BY id", &[])
            .unwrap(),
        vec![
            vec![SqlValue::Integer(3), SqlValue::Integer(300)],
            vec![SqlValue::Integer(4), SqlValue::Integer(400)],
        ]
    );

    // 2. COLLATE NOCASE and COLLATE RTRIM on primary key and secondary index, plus rejecting unknown collations
    conn.execute_batch(
        "CREATE VIRTUAL TABLE t_coll USING aydb(
            table_id = 71,
            columns = 'k TEXT PRIMARY KEY COLLATE NOCASE, tag TEXT COLLATE RTRIM',
            indexes = 'tag'
        );
        INSERT INTO t_coll VALUES ('Alice', 'admin   ');",
    )
    .unwrap();

    assert_eq!(
        conn.query_all("SELECT k FROM t_coll WHERE k = 'alice'", &[])
            .unwrap(),
        vec![vec![SqlValue::Text("Alice".into())]]
    );
    assert_eq!(
        conn.query_all("SELECT k FROM t_coll WHERE +k = 'ALICE'", &[])
            .unwrap(),
        vec![vec![SqlValue::Text("Alice".into())]]
    );
    assert_eq!(
        conn.query_all("SELECT k, tag FROM t_coll WHERE tag = 'admin'", &[])
            .unwrap(),
        vec![vec![
            SqlValue::Text("Alice".into()),
            SqlValue::Text("admin   ".into())
        ]]
    );
    // Duplicate under NOCASE must fail with SQLITE_CONSTRAINT
    assert!(
        conn.execute_batch("INSERT INTO t_coll VALUES ('ALICE', 'other');")
            .is_err(),
        "duplicate COLLATE NOCASE key must fail uniqueness check"
    );
    // Unsupported collation in CREATE VIRTUAL TABLE must be rejected
    assert!(
        conn.execute_batch(
            "CREATE VIRTUAL TABLE bad_coll USING aydb(
                table_id = 72,
                columns = 'k TEXT PRIMARY KEY COLLATE NOSUCHCOLLATION'
            );"
        )
        .is_err(),
        "unknown collation must be rejected at table creation"
    );

    // 3. Unified exact numeric sorting across mixed INTEGER and REAL values (including 2^53 + 1)
    conn.execute_batch(
        "CREATE VIRTUAL TABLE m_num USING aydb(
            table_id = 73,
            columns = 'k BLOB PRIMARY KEY, v TEXT'
        );
        INSERT INTO m_num VALUES
            (10, 'ten'),
            (1.5, 'one_half'),
            (-1.5, 'neg_one_half'),
            (-10, 'neg_ten'),
            (0, 'zero'),
            (9007199254740993, 'two53_plus_1'),
            (9007199254740992, 'two53'),
            (-9007199254740992, 'neg_two53'),
            (-9007199254740993, 'neg_two53_minus_1');",
    )
    .unwrap();

    let sorted_mixed = conn
        .query_all("SELECT k FROM m_num ORDER BY k", &[])
        .unwrap();
    assert_eq!(
        sorted_mixed,
        vec![
            vec![SqlValue::Integer(-9_007_199_254_740_993)],
            vec![SqlValue::Integer(-9_007_199_254_740_992)],
            vec![SqlValue::Integer(-10)],
            vec![SqlValue::Float(-1.5)],
            vec![SqlValue::Integer(0)],
            vec![SqlValue::Float(1.5)],
            vec![SqlValue::Integer(10)],
            vec![SqlValue::Integer(9_007_199_254_740_992)],
            vec![SqlValue::Integer(9_007_199_254_740_993)],
        ]
    );

    // Range scan with LIMIT pushdown on mixed numeric BLOB PK
    let top3_gt_neg2 = conn
        .query_all("SELECT k FROM m_num WHERE k > -2 ORDER BY k LIMIT 3", &[])
        .unwrap();
    assert_eq!(
        top3_gt_neg2,
        vec![
            vec![SqlValue::Float(-1.5)],
            vec![SqlValue::Integer(0)],
            vec![SqlValue::Float(1.5)],
        ]
    );

    // Point lookup of integer 10 using 10.0 on BLOB primary key
    assert_eq!(
        conn.query_all("SELECT v FROM m_num WHERE k = 10.0", &[])
            .unwrap(),
        vec![vec![SqlValue::Text("ten".into())]]
    );

    // 4. Explicit i64::MAX insertion followed by NULL auto-ID insertion must not hang!
    conn.execute_batch(
        "CREATE VIRTUAL TABLE max_id_t USING aydb(
            table_id = 74,
            columns = 'id INTEGER PRIMARY KEY, label TEXT'
        );
        INSERT INTO max_id_t VALUES (9223372036854775807, 'max_int');
        INSERT INTO max_id_t VALUES (NULL, 'after_max_1');
        INSERT INTO max_id_t VALUES (NULL, 'after_max_2');",
    )
    .unwrap();

    let rows_after_max = conn
        .query_all("SELECT id, label FROM max_id_t ORDER BY id", &[])
        .unwrap();
    assert_eq!(rows_after_max.len(), 3);
    assert_eq!(
        rows_after_max[2],
        vec![SqlValue::Integer(i64::MAX), SqlValue::Text("max_int".into())]
    );
    // Both auto-generated IDs must be valid positive integers < i64::MAX and distinct
    let id1 = match rows_after_max[0][0] {
        SqlValue::Integer(v) => v,
        _ => panic!("expected integer PK"),
    };
    let id2 = match rows_after_max[1][0] {
        SqlValue::Integer(v) => v,
        _ => panic!("expected integer PK"),
    };
    assert!(id1 > 0 && id1 < id2 && id2 < i64::MAX);
}

#[test]
fn vtab_keys_only_projection_and_tx_budget() {
    let tmp = TempTestDir::new("vtab_keys_only");
    let mut cfg = EngineConfig::default();
    cfg.max_transaction_bytes = 32 * 1024; // 32 KB transaction budget

    {
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
        let engine = Arc::new(Mutex::new(
            StorageEngine::open(fs.clone(), cfg.clone()).unwrap(),
        ));
        let conn = SqliteConnection::open_in_memory().unwrap();
        conn.register_aydb_module(engine.clone()).unwrap();

        conn.execute_batch(
            "CREATE VIRTUAL TABLE docs USING aydb(
                table_id = 80,
                columns = 'id INTEGER PRIMARY KEY, body BLOB'
            );",
        )
        .unwrap();

        let big_blob = vec![0x5Au8; 4096];
        for id in 1..=5i64 {
            conn.query_all(
                "INSERT INTO docs VALUES (?1, ?2)",
                &[SqlValue::Integer(id), SqlValue::Blob(big_blob.clone())],
            )
            .unwrap();
        }
        engine.lock().unwrap().flush_memtable().unwrap();
    }

    // Corrupt the out-of-line `blob_std_*.db` payload bytes on disk while handles are closed
    // to prove that `SELECT COUNT(*)`, `SELECT id`, and `SELECT rowid, id` do NOT read any blob files!
    let mut found_blob = false;
    for entry in std::fs::read_dir(&tmp.path).unwrap().flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with("blob_std_") && name.ends_with(".db") {
            found_blob = true;
            let p = entry.path();
            let blen = std::fs::metadata(&p).unwrap().len() as usize;
            std::fs::write(&p, vec![0xFFu8; blen]).unwrap();
        }
    }
    assert!(found_blob);

    let fs = Arc::new(OsFs::open(&tmp.path, false).unwrap());
    let engine = Arc::new(Mutex::new(StorageEngine::open(fs, cfg).unwrap()));
    let conn = SqliteConnection::open_in_memory().unwrap();
    conn.register_aydb_module(engine).unwrap();
    conn.execute_batch(
        "CREATE VIRTUAL TABLE docs USING aydb(
            table_id = 80,
            columns = 'id INTEGER PRIMARY KEY, body BLOB'
        );",
    )
    .unwrap();

    // `SELECT COUNT(*) FROM docs` (`colUsed == 0`) must succeed without touching `blob_std_*.db`!
    assert_eq!(
        conn.query_all("SELECT COUNT(*) FROM docs", &[]).unwrap(),
        vec![vec![SqlValue::Integer(5)]]
    );

    // `SELECT id FROM docs WHERE id >= 2 AND id <= 4` (`colUsed == (1 << pk_col_idx)`)
    // must also succeed via `RANGE_FLAG_KEYS_ONLY` without touching `blob_std_*.db`!
    assert_eq!(
        conn.query_all("SELECT id FROM docs WHERE id >= 2 AND id <= 4 ORDER BY id", &[])
            .unwrap(),
        vec![
            vec![SqlValue::Integer(2)],
            vec![SqlValue::Integer(3)],
            vec![SqlValue::Integer(4)],
        ]
    );

    // `SELECT rowid, id FROM docs` must also succeed via `RANGE_FLAG_KEYS_ONLY`!
    assert_eq!(
        conn.query_all("SELECT rowid, id FROM docs WHERE id <= 2 ORDER BY id", &[])
            .unwrap(),
        vec![
            vec![SqlValue::Integer(1), SqlValue::Integer(1)],
            vec![SqlValue::Integer(2), SqlValue::Integer(2)],
        ]
    );

    // Whereas projecting `body` (`SELECT body FROM docs`) DOES attempt to dereference the
    // corrupted blob and skips all 5 rows (`Ok(false)` safe blob miss), proving that the
    // keys-only queries above never touched `blob_std_*.db`!
    assert_eq!(
        conn.query_all("SELECT body FROM docs", &[]).unwrap(),
        Vec::<Vec<SqlValue>>::new(),
        "reading projected BLOB column must dereference blob_std and skip corrupted blobs"
    );

    // Verify `max_transaction_bytes` rejects an oversized SQL transaction cleanly
    conn.execute_batch("BEGIN;").unwrap();
    let huge_blob = vec![0x77u8; 40 * 1024]; // 40 KB > 32 KB max_transaction_bytes
    let res = conn.query_all(
        "INSERT INTO docs VALUES (?1, ?2)",
        &[SqlValue::Integer(99), SqlValue::Blob(huge_blob)],
    );
    assert!(
        res.is_err(),
        "SQL INSERT exceeding max_transaction_bytes must fail"
    );
    conn.execute_batch("ROLLBACK;").unwrap();
    assert_eq!(
        conn.query_all("SELECT COUNT(*) FROM docs", &[]).unwrap(),
        vec![vec![SqlValue::Integer(5)]]
    );
}

#[test]
fn sub_ulp_float_keys_numeric_duals_like_limit_and_statement_rebind() {
    let tmp = TempTestDir::new("vtab_float_duals_and_rebind");
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let engine = Arc::new(Mutex::new(
        StorageEngine::open(fs, EngineConfig::default()).unwrap(),
    ));
    let conn = SqliteConnection::open_in_memory().unwrap();
    conn.register_aydb_module(engine).unwrap();

    // 1. Negative sub-ULP floats (-1e-17, -1e-20, -1e-300, -f64::MIN_POSITIVE, -0.001, -0.5, -1.0, -1.5)
    //    on REAL PRIMARY KEY and BLOB PRIMARY KEY must not collide and must round-trip via
    //    keys-only projection (`SELECT pk FROM t`) without collapsing to 0.0!
    conn.execute_batch(
        "CREATE VIRTUAL TABLE c01_real USING aydb(
            table_id = 90,
            columns = 'pk REAL PRIMARY KEY, label TEXT'
        );
        CREATE VIRTUAL TABLE c01_blob USING aydb(
            table_id = 91,
            columns = 'pk BLOB PRIMARY KEY, label TEXT'
        );",
    )
    .unwrap();

    let neg_floats = [
        -1.5_f64,
        -1.0,
        -0.5,
        -0.001,
        -1e-17,
        -1e-20,
        -1e-300,
        -f64::MIN_POSITIVE,
        0.0,
    ];
    {
        let mut ins_real = conn
            .prepare("INSERT INTO c01_real VALUES (?1, ?2)")
            .unwrap();
        let mut ins_blob = conn
            .prepare("INSERT INTO c01_blob VALUES (?1, ?2)")
            .unwrap();
        for (idx, &v) in neg_floats.iter().enumerate() {
            ins_real
                .execute(&[SqlValue::Float(v), SqlValue::Text(format!("r_{idx}"))])
                .unwrap();
            ins_blob
                .execute(&[SqlValue::Float(v), SqlValue::Text(format!("b_{idx}"))])
                .unwrap();
        }
    }

    // Keys-only projection (`SELECT pk FROM ...`) uses `SqlValue::from_key_bytes` directly!
    let real_pks = conn.query_all("SELECT pk FROM c01_real", &[]).unwrap();
    let blob_pks = conn.query_all("SELECT pk FROM c01_blob", &[]).unwrap();
    assert_eq!(real_pks.len(), neg_floats.len());
    assert_eq!(blob_pks.len(), neg_floats.len());
    for (i, &expected) in neg_floats.iter().enumerate() {
        match (&real_pks[i][0], &blob_pks[i][0]) {
            (SqlValue::Float(r), SqlValue::Float(b)) => {
                assert_eq!(r.to_bits(), expected.to_bits());
                assert_eq!(b.to_bits(), expected.to_bits());
            }
            other => panic!("expected Float PKs, got {other:?}"),
        }
    }

    // 2. Inserting 10 and 10.0 into a BLOB PRIMARY KEY or REAL PRIMARY KEY
    //    must raise SQLITE_CONSTRAINT on plain INSERT, skip on INSERT OR IGNORE,
    //    and replace the existing row on INSERT OR REPLACE.
    conn.execute_batch(
        "CREATE VIRTUAL TABLE c02_blob USING aydb(
            table_id = 92,
            columns = 'pk BLOB PRIMARY KEY, v TEXT'
        );
        CREATE VIRTUAL TABLE c02_real USING aydb(
            table_id = 93,
            columns = 'pk REAL PRIMARY KEY, v TEXT'
        );
        INSERT INTO c02_blob VALUES (10, 'int_ten');
        INSERT INTO c02_real VALUES (10, 'int_ten_as_real');",
    )
    .unwrap();

    // Plain INSERT of 10.0 must fail with SQLITE_CONSTRAINT on both tables
    assert!(
        conn.execute_batch("INSERT INTO c02_blob VALUES (10.0, 'float_ten');")
            .is_err(),
        "plain INSERT of 10.0 when 10 exists on BLOB PK must fail"
    );
    assert!(
        conn.execute_batch("INSERT INTO c02_real VALUES (10.0, 'float_ten');")
            .is_err(),
        "plain INSERT of 10.0 when 10 exists on REAL PK must fail"
    );

    // INSERT OR IGNORE of 10.0 must skip without modifying the existing row
    conn.execute_batch(
        "INSERT OR IGNORE INTO c02_blob VALUES (10.0, 'ignored');
         INSERT OR IGNORE INTO c02_real VALUES (10.0, 'ignored');",
    )
    .unwrap();
    assert_eq!(
        conn.query_all("SELECT pk, v FROM c02_blob", &[]).unwrap(),
        vec![vec![SqlValue::Integer(10), SqlValue::Text("int_ten".into())]]
    );

    // INSERT OR REPLACE of 10.0 must replace the existing 10 row so only 1 row remains!
    conn.execute_batch(
        "INSERT OR REPLACE INTO c02_blob VALUES (10.0, 'replaced_by_float');
         INSERT OR REPLACE INTO c02_real VALUES (10.0, 'replaced_by_float');",
    )
    .unwrap();
    assert_eq!(
        conn.query_all("SELECT pk, v FROM c02_blob", &[]).unwrap(),
        vec![vec![
            SqlValue::Float(10.0),
            SqlValue::Text("replaced_by_float".into())
        ]]
    );
    assert_eq!(
        conn.query_all("SELECT pk, v FROM c02_real", &[]).unwrap(),
        vec![vec![
            SqlValue::Float(10.0),
            SqlValue::Text("replaced_by_float".into())
        ]]
    );

    // Replacing back with integer 10 on BLOB PK must also replace 10.0 cleanly!
    conn.execute_batch("INSERT OR REPLACE INTO c02_blob VALUES (10, 'back_to_int');")
        .unwrap();
    assert_eq!(
        conn.query_all("SELECT pk, v FROM c02_blob", &[]).unwrap(),
        vec![vec![
            SqlValue::Integer(10),
            SqlValue::Text("back_to_int".into())
        ]]
    );

    // 3. `SELECT * FROM t WHERE v LIKE '%match%' LIMIT 3` must return all 3 matching
    //    rows even when the first 50 rows in the table do not match '%match%'.
    conn.execute_batch(
        "CREATE VIRTUAL TABLE c03_like USING aydb(
            table_id = 94,
            columns = 'id INT PRIMARY KEY, v TEXT'
        );
        BEGIN;",
    )
    .unwrap();
    {
        let mut ins = conn
            .prepare("INSERT INTO c03_like VALUES (?1, ?2)")
            .unwrap();
        for i in 1..=60i64 {
            let text = if i <= 50 {
                format!("prefix_skip_{i}")
            } else {
                format!("prefix_match_{i}_suffix")
            };
            ins.execute(&[SqlValue::Integer(i), SqlValue::Text(text)])
                .unwrap();
        }
    }
    conn.execute_batch("COMMIT;").unwrap();

    let like_full_scan = conn
        .query_all(
            "SELECT id, v FROM c03_like WHERE v LIKE '%match%' LIMIT 3",
            &[],
        )
        .unwrap();
    assert_eq!(
        like_full_scan,
        vec![
            vec![
                SqlValue::Integer(51),
                SqlValue::Text("prefix_match_51_suffix".into())
            ],
            vec![
                SqlValue::Integer(52),
                SqlValue::Text("prefix_match_52_suffix".into())
            ],
            vec![
                SqlValue::Integer(53),
                SqlValue::Text("prefix_match_53_suffix".into())
            ],
        ]
    );

    // Also test PK range scan combined with LIKE and LIMIT
    let like_range_scan = conn
        .query_all(
            "SELECT id FROM c03_like WHERE id >= 10 AND v LIKE '%match%' LIMIT 3",
            &[],
        )
        .unwrap();
    assert_eq!(
        like_range_scan,
        vec![
            vec![SqlValue::Integer(51)],
            vec![SqlValue::Integer(52)],
            vec![SqlValue::Integer(53)],
        ]
    );

    // 4. `SELECT rowid FROM t` followed by `UPDATE` / `DELETE WHERE rowid = ?` on a
    //    `TEXT PRIMARY KEY` table with > 20,000 rows must succeed!
    conn.execute_batch(
        "CREATE VIRTUAL TABLE c04_text USING aydb(
            table_id = 95,
            columns = 'k TEXT PRIMARY KEY, val INT'
        );
        BEGIN;",
    )
    .unwrap();
    {
        let mut ins = conn
            .prepare("INSERT INTO c04_text VALUES (?1, ?2)")
            .unwrap();
        for i in 0..21_000i64 {
            ins.execute(&[SqlValue::Text(format!("row_{i:06}")), SqlValue::Integer(i)])
                .unwrap();
        }
    }
    conn.execute_batch("COMMIT;").unwrap();

    let all_rids = conn
        .query_all("SELECT rowid, k FROM c04_text ORDER BY k", &[])
        .unwrap();
    assert_eq!(all_rids.len(), 21_000);
    let rid_first = match all_rids[0][0] {
        SqlValue::Integer(r) => r,
        _ => unreachable!(),
    };
    let rid_last = match all_rids[20_999][0] {
        SqlValue::Integer(r) => r,
        _ => unreachable!(),
    };

    conn.query_all(
        "UPDATE c04_text SET val = 777777 WHERE rowid = ?1",
        &[SqlValue::Integer(rid_first)],
    )
    .unwrap();
    conn.query_all(
        "DELETE FROM c04_text WHERE rowid = ?1",
        &[SqlValue::Integer(rid_last)],
    )
    .unwrap();

    assert_eq!(
        conn.query_all("SELECT val FROM c04_text WHERE k = 'row_000000'", &[])
            .unwrap(),
        vec![vec![SqlValue::Integer(777777)]]
    );
    assert!(
        conn.query_all("SELECT val FROM c04_text WHERE k = 'row_020999'", &[])
            .unwrap()
            .is_empty()
    );

    // 5. SqliteStatement re-binding (`sqlite3_clear_bindings`) and float-to-text formatting
    conn.execute_batch(
        "CREATE VIRTUAL TABLE c05_text USING aydb(
            table_id = 96,
            columns = 'k TEXT PRIMARY KEY, v TEXT'
        );",
    )
    .unwrap();
    let mut stmt = conn
        .prepare("INSERT INTO c05_text VALUES (?1, ?2)")
        .unwrap();
    stmt.execute(&[SqlValue::Float(1e20), SqlValue::Text("first".into())])
        .unwrap();
    // Execute again with only 1 parameter bound; ?2 must be cleared to NULL by sqlite3_clear_bindings!
    stmt.execute(&[SqlValue::Float(-1e-20)]).unwrap();

    let c05_rows = conn
        .query_all("SELECT k, v FROM c05_text ORDER BY k", &[])
        .unwrap();
    assert_eq!(
        c05_rows,
        vec![
            vec![SqlValue::Text("-1.0e-20".into()), SqlValue::Null],
            vec![
                SqlValue::Text("1.0e+20".into()),
                SqlValue::Text("first".into())
            ],
        ]
    );
}

#[test]
fn chunked_streaming_cursor_and_order_by_pk_desc_pushdown() {
    let dir = TempTestDir::new("vtab_chunked_streaming");
    let fs = Arc::new(OsFs::open(&dir.path, true).unwrap());
    let engine = Arc::new(Mutex::new(
        StorageEngine::open(fs, EngineConfig::default()).unwrap(),
    ));

    let conn = SqliteConnection::open_in_memory().unwrap();
    conn.register_aydb_module(engine.clone()).unwrap();
    conn.execute_batch(
        "CREATE VIRTUAL TABLE p01_stream USING aydb(
            table_id = 97,
            columns = 'id INT PRIMARY KEY, payload BLOB'
        );
        BEGIN;",
    )
    .unwrap();

    // Insert 1,000 rows (> 3x `CURSOR_STREAM_CHUNK_ROWS = 256`) across SSTable + MemTable
    let blob_2k = vec![0x3Cu8; 2048];
    {
        let mut ins = conn
            .prepare("INSERT INTO p01_stream VALUES (?1, ?2)")
            .unwrap();
        for i in 1..=600i64 {
            ins.execute(&[SqlValue::Integer(i), SqlValue::Blob(blob_2k.clone())])
                .unwrap();
        }
    }
    conn.execute_batch("COMMIT;").unwrap();
    engine.lock().unwrap().flush_memtable().unwrap();

    conn.execute_batch("BEGIN;").unwrap();
    {
        let mut ins = conn
            .prepare("INSERT INTO p01_stream VALUES (?1, ?2)")
            .unwrap();
        for i in 601..=1_000i64 {
            ins.execute(&[SqlValue::Integer(i), SqlValue::Blob(blob_2k.clone())])
                .unwrap();
        }
    }
    conn.execute_batch("COMMIT;").unwrap();

    // 1. Verify full streaming scan in ASC order across all 4 chunks (`1000` rows)
    let count_rows = conn
        .query_all("SELECT count(*), sum(length(payload)) FROM p01_stream", &[])
        .unwrap();
    assert_eq!(
        count_rows,
        vec![vec![
            SqlValue::Integer(1_000),
            SqlValue::Integer(1_000 * 2048)
        ]]
    );

    // 2. Verify `ORDER BY id DESC` pushdown (`orderByConsumed = 1`, zero temp B-tree in EXPLAIN QUERY PLAN)
    let eqp = conn
        .query_all(
            "EXPLAIN QUERY PLAN SELECT id, length(payload) FROM p01_stream ORDER BY id DESC LIMIT 5",
            &[],
        )
        .unwrap();
    let eqp_text = format!("{eqp:?}");
    assert!(
        !eqp_text.contains("USE TEMP B-TREE FOR ORDER BY"),
        "ORDER BY id DESC must be consumed by vtab_best_index without temp B-tree sort: {eqp_text}"
    );

    let top5_desc = conn
        .query_all(
            "SELECT id FROM p01_stream ORDER BY id DESC LIMIT 5",
            &[],
        )
        .unwrap();
    assert_eq!(
        top5_desc,
        vec![
            vec![SqlValue::Integer(1_000)],
            vec![SqlValue::Integer(999)],
            vec![SqlValue::Integer(998)],
            vec![SqlValue::Integer(997)],
            vec![SqlValue::Integer(996)],
        ]
    );

    // 3. Verify multi-chunk `ORDER BY id DESC` over a range (`WHERE id >= 100 AND id <= 750 ORDER BY id DESC`)
    let desc_range = conn
        .query_all(
            "SELECT id, length(payload) FROM p01_stream WHERE id >= 100 AND id <= 750 ORDER BY id DESC",
            &[],
        )
        .unwrap();
    assert_eq!(desc_range.len(), 651);
    assert_eq!(
        desc_range[0],
        vec![SqlValue::Integer(750), SqlValue::Integer(2048)]
    );
    assert_eq!(
        desc_range[256],
        vec![SqlValue::Integer(750 - 256), SqlValue::Integer(2048)]
    );
    assert_eq!(
        desc_range[650],
        vec![SqlValue::Integer(100), SqlValue::Integer(2048)]
    );
}

#[test]
fn rowid_map_rollback_and_sync_catalog_durability() {
    let dir = TempTestDir::new("vtab_rowid_rollback_sync_cat");
    let fs = Arc::new(OsFs::open(&dir.path, true).unwrap());
    let engine = Arc::new(Mutex::new(
        StorageEngine::open(fs.clone(), EngineConfig::default()).unwrap(),
    ));

    let conn = SqliteConnection::open_in_memory().unwrap();
    conn.register_aydb_module(engine.clone()).unwrap();

    // 1. Virtual table with `durability = 'sync'` and omitted `table_id` must persist
    //    its system catalog (`Table 0`) mapping via `WriteDurability::SyncTransactional` (`wal_sync.db`)!
    conn.execute_batch(
        "CREATE VIRTUAL TABLE n01_text USING aydb(
            durability = 'sync',
            columns = 'k TEXT PRIMARY KEY, v TEXT'
        );",
    )
    .unwrap();
    let sync_wal_size = std::fs::metadata(dir.path.join("wal_sync.db"))
        .unwrap()
        .len();
    assert!(
        sync_wal_size > 0,
        "auto-assigned catalog registration for durability='sync' must write to wal_sync.db"
    );

    // 2. Populate rows with TEXT primary keys and capture their synthetic rowids
    conn.execute_batch(
        "INSERT INTO n01_text VALUES ('alpha', 'v1');
         INSERT INTO n01_text VALUES ('beta', 'v2');
         INSERT INTO n01_text VALUES ('gamma', 'v3');",
    )
    .unwrap();

    let initial_rows = conn
        .query_all("SELECT rowid, k, v FROM n01_text ORDER BY k", &[])
        .unwrap();
    let SqlValue::Integer(rid_alpha) = initial_rows[0][0] else {
        panic!("expected integer rowid for alpha");
    };
    let SqlValue::Integer(rid_beta) = initial_rows[1][0] else {
        panic!("expected integer rowid for beta");
    };

    // 2a. Explicit transaction that deletes `alpha` and updates `beta` -> `'beta_renamed'`, then rolls back:
    //     `rid_alpha` and `rid_beta` must still resolve in `RowidMap` after `ROLLBACK`!
    conn.execute_batch(
        "BEGIN;
         DELETE FROM n01_text WHERE k = 'alpha';
         UPDATE n01_text SET k = 'beta_renamed', v = 'v2_mod' WHERE k = 'beta';
         ROLLBACK;",
    )
    .unwrap();

    assert_eq!(
        conn.query_all(
            "SELECT k, v FROM n01_text WHERE rowid = ?1",
            &[SqlValue::Integer(rid_alpha)],
        )
        .unwrap(),
        vec![vec![
            SqlValue::Text("alpha".into()),
            SqlValue::Text("v1".into()),
        ]],
        "rowid lookup for 'alpha' must succeed after ROLLBACK of DELETE"
    );
    assert_eq!(
        conn.query_all(
            "SELECT k, v FROM n01_text WHERE rowid = ?1",
            &[SqlValue::Integer(rid_beta)],
        )
        .unwrap(),
        vec![vec![
            SqlValue::Text("beta".into()),
            SqlValue::Text("v2".into()),
        ]],
        "rowid lookup for 'beta' must succeed after ROLLBACK of PK UPDATE"
    );

    // 2b. Savepoint rollback (`ROLLBACK TO sp1`) and statement-level constraint abort:
    conn.execute_batch(
        "BEGIN;
         SAVEPOINT sp1;
         DELETE FROM n01_text WHERE rowid = (SELECT rowid FROM n01_text WHERE k = 'alpha');
         ROLLBACK TO sp1;
         RELEASE sp1;
         COMMIT;",
    )
    .unwrap();
    assert_eq!(
        conn.query_all(
            "SELECT k, v FROM n01_text WHERE rowid = ?1",
            &[SqlValue::Integer(rid_alpha)],
        )
        .unwrap(),
        vec![vec![
            SqlValue::Text("alpha".into()),
            SqlValue::Text("v1".into()),
        ]],
        "rowid lookup for 'alpha' must succeed after ROLLBACK TO SAVEPOINT"
    );

    // Multi-row UPDATE that changes 'alpha' PK first and then hits UNIQUE constraint on 'gamma':
    let err = conn.execute_batch(
        "UPDATE n01_text SET k = CASE WHEN k = 'alpha' THEN 'alpha_tmp' ELSE 'gamma' END;",
    );
    assert!(
        err.is_err(),
        "multi-row duplicate PK update must fail with SQLITE_CONSTRAINT"
    );

    // After statement-level rollback, updating 'alpha' by its original `rid_alpha` must work!
    conn.query_all(
        "UPDATE n01_text SET v = 'v1_after_stmt_abort' WHERE rowid = ?1",
        &[SqlValue::Integer(rid_alpha)],
    )
    .unwrap();
    assert_eq!(
        conn.query_all("SELECT v FROM n01_text WHERE k = 'alpha'", &[])
            .unwrap(),
        vec![vec![SqlValue::Text("v1_after_stmt_abort".into())]],
        "UPDATE WHERE rowid = ? must resolve 'alpha' after statement-level rollback"
    );
}

#[test]
fn secondary_index_streaming_range_scan_and_rowid_compaction() {
    let dir = TempTestDir::new("vtab_sec_idx_stream_range");
    let fs = Arc::new(OsFs::open(&dir.path, true).unwrap());
    let engine = Arc::new(Mutex::new(
        StorageEngine::open(fs, EngineConfig::default()).unwrap(),
    ));
    let conn = SqliteConnection::open_in_memory().unwrap();
    conn.register_aydb_module(engine.clone()).unwrap();

    conn.execute_batch(
        "CREATE VIRTUAL TABLE sec_items USING aydb(
             columns = 'id INTEGER PRIMARY KEY, category TEXT, score REAL, tag TEXT COLLATE NOCASE',
             indexes = 'category, score, tag'
         );",
    )
    .unwrap();

    // 1. Insert 650 rows (> 2 * CURSOR_STREAM_CHUNK_ROWS = 512) sharing `category = 'hot'`,
    //    flushing halfway so entries span both SSTable segments and MemTable.
    conn.execute_batch("BEGIN;").unwrap();
    for i in 1..=350i64 {
        conn.query_all(
            "INSERT INTO sec_items VALUES (?1, 'hot', ?2, ?3)",
            &[
                SqlValue::Integer(i),
                SqlValue::Float(i as f64 * 0.5),
                SqlValue::Text(format!("Tag_{i:04}")),
            ],
        )
        .unwrap();
    }
    conn.execute_batch("COMMIT;").unwrap();
    engine.lock().unwrap().flush_memtable().unwrap();

    conn.execute_batch("BEGIN;").unwrap();
    for i in 351..=650i64 {
        conn.query_all(
            "INSERT INTO sec_items VALUES (?1, 'hot', ?2, ?3)",
            &[
                SqlValue::Integer(i),
                SqlValue::Float(i as f64 * 0.5),
                SqlValue::Text(format!("Tag_{i:04}")),
            ],
        )
        .unwrap();
    }
    // Also insert rows with NULL score/category and negative scores to test boundary semantics
    conn.execute_batch(
        "INSERT INTO sec_items VALUES (1001, NULL, NULL, NULL);
         INSERT INTO sec_items VALUES (1002, 'cold', -20.5, 'alpha');
         INSERT INTO sec_items VALUES (1003, 'cold', -10.0, 'beta');
         INSERT INTO sec_items VALUES (1004, 'cold', -9.5, 'gamma');
         COMMIT;",
    )
    .unwrap();

    // Verify chunked streaming on `PLAN_SECONDARY_EQ` (`WHERE category = 'hot'`) with `LIMIT 10`
    let top10 = conn
        .query_all(
            "SELECT id FROM sec_items WHERE category = 'hot' LIMIT 10",
            &[],
        )
        .unwrap();
    assert_eq!(top10.len(), 10);

    // Verify full multi-chunk `PLAN_SECONDARY_EQ` scan across all 650 rows (3 chunks of 256)
    let all_hot = conn
        .query_all("SELECT count(*), sum(id) FROM sec_items WHERE category = 'hot'", &[])
        .unwrap();
    assert_eq!(
        all_hot,
        vec![vec![
            SqlValue::Integer(650),
            SqlValue::Integer(650 * 651 / 2)
        ]]
    );

    // 2. Verify `PLAN_SECONDARY_RANGE` (`>`, `>=`, `<`, `<=`, `BETWEEN`) on `score` and `tag`
    //    - Negative float & integer range: `score > -20.5 AND score <= -9.5` -> ids 1003 (-10.0) and 1004 (-9.5)
    let neg_range = conn
        .query_all(
            "SELECT id FROM sec_items WHERE score > -20.5 AND score <= -9.5 ORDER BY id",
            &[],
        )
        .unwrap();
    assert_eq!(
        neg_range,
        vec![
            vec![SqlValue::Integer(1003)],
            vec![SqlValue::Integer(1004)],
        ]
    );

    //    - Upper-only bound `< 2.0` must exclude `NULL` (id 1001) and include -20.5, -10.0, -9.5, 0.5, 1.0, 1.5
    let lt_two = conn
        .query_all(
            "SELECT id FROM sec_items WHERE score < 2.0 ORDER BY id",
            &[],
        )
        .unwrap();
    assert_eq!(
        lt_two,
        vec![
            vec![SqlValue::Integer(1)],
            vec![SqlValue::Integer(2)],
            vec![SqlValue::Integer(3)],
            vec![SqlValue::Integer(1002)],
            vec![SqlValue::Integer(1003)],
            vec![SqlValue::Integer(1004)],
        ]
    );

    //    - Multi-chunk `PLAN_SECONDARY_RANGE` returning > 600 rows (`score >= 10.0` -> ids 20..=650 = 631 rows)
    let ge_ten = conn
        .query_all("SELECT count(*) FROM sec_items WHERE score >= 10.0", &[])
        .unwrap();
    assert_eq!(ge_ten, vec![vec![SqlValue::Integer(631)]]);

    //    - NOCASE secondary index range (`WHERE tag >= 'tag_0648' AND tag <= 'TAG_0650'`)
    let nocase_range = conn
        .query_all(
            "SELECT id FROM sec_items WHERE tag >= 'tag_0648' AND tag <= 'TAG_0650' ORDER BY id",
            &[],
        )
        .unwrap();
    assert_eq!(
        nocase_range,
        vec![
            vec![SqlValue::Integer(648)],
            vec![SqlValue::Integer(649)],
            vec![SqlValue::Integer(650)],
        ]
    );

    // 3. Verify `ON CONFLICT REPLACE` and numeric dual PK resolution across `BLOB` and `INTEGER` PKs
    conn.execute_batch(
        "CREATE VIRTUAL TABLE p4_blob_pk USING aydb(columns = 'k BLOB PRIMARY KEY, v TEXT');
         INSERT INTO p4_blob_pk VALUES (10, 'int_ten');
         INSERT OR REPLACE INTO p4_blob_pk VALUES (10.0, 'float_ten');",
    )
    .unwrap();
    assert_eq!(
        conn.query_all("SELECT v FROM p4_blob_pk WHERE k = 10", &[])
            .unwrap(),
        vec![vec![SqlValue::Text("float_ten".into())]]
    );

    // 4. Verify `RowidMap::evict_oldest_half_if_needed` compacts `fifo_order`
    //    when `fifo_order.len() >= MAX_ROWID_MAP_ENTRIES` while `by_rowid.len() <= MAX_ROWID_MAP_ENTRIES`.
    let mut rmap = RowidMap::default();
    let rid_live = rmap.get_or_assign_rowid(b"\x20live_key");
    for i in 0..MAX_ROWID_MAP_ENTRIES {
        let key = format!("\x20temp_{i}").into_bytes();
        let rid = rmap.get_or_assign_rowid(&key);
        rmap.remove_by_rowid(rid);
    }
    assert_eq!(rmap.by_rowid.len(), 1);
    assert!(rmap.fifo_order.len() >= MAX_ROWID_MAP_ENTRIES);
    rmap.evict_oldest_half_if_needed();
    assert_eq!(
        rmap.fifo_order.len(),
        1,
        "stale deleted rowids in fifo_order must be compacted even when by_rowid.len() <= MAX_ROWID_MAP_ENTRIES"
    );
    assert_eq!(
        rmap.get_pk(rid_live).map(|v| v.as_slice()),
        Some(&b"\x20live_key"[..])
    );
}
