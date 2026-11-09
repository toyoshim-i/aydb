//! Building the UNIQUE index for rows that were written before it existed.
//!
//! Legacy rows are written straight into the engine the way the previous version did: with their
//! declared-index entries, but without UNIQUE entries.

use super::rowid_tests::sync_counting::CountFs;
use super::unique_index_tests::{Engine, Fx, assert_index, connect, fixture, open_engine, rows_of};
use super::unique_key::UniqueDef;
use super::unique_migration::{self, Outcome, Params};
use super::*;
use crate::engine::{EngineConfig, ReadView};
use crate::index::{NS_ROW_META, encode_row_meta_key, encode_secondary_index_key};
use crate::util::test_util::TempTestDir;
use std::sync::atomic::AtomicU64;

fn std_def() -> UniqueDef {
    UniqueDef::new(vec![
        (1, ColCollation::Binary, ColumnAffinity::Text),
        (2, ColCollation::Binary, ColumnAffinity::Integer),
        (3, ColCollation::NoCase, ColumnAffinity::Text),
    ])
}

/// A legacy row with its declared-index entry and `'t'` record but no UNIQUE entries.
fn put_indexed_legacy_row(fx: &Fx, k: &str, a: &str, b: i64, n: &str, c: &str) {
    let row = encode_row(&[
        SqlValue::Text(k.into()),
        SqlValue::Text(a.into()),
        SqlValue::Integer(b),
        SqlValue::Text(n.into()),
        SqlValue::Text(c.into()),
    ]);
    let pk = SqlValue::Text(k.into()).to_key_bytes();
    let declared = [(1u16, SqlValue::Text(c.into()).to_key_bytes())];
    let mut eng = fx.engine.write().unwrap();
    eng.put_row(1, &pk, &row, &declared, WriteDurability::BufferedFast)
        .unwrap();
    rowid_map::assign_synthetic_rowid(&mut eng, 1, &pk, WriteDurability::BufferedFast, false)
        .unwrap();
}

fn legacy_table(fx: &Fx, rows: usize) {
    for i in 0..rows {
        put_indexed_legacy_row(
            fx,
            &format!("k{i:05}"),
            &format!("a{i}"),
            i as i64,
            &format!("N{i}"),
            "c",
        );
    }
}

fn marker(fx: &Fx) -> Option<Vec<u8>> {
    marker_of(&fx.engine, 1)
}

fn marker_of(engine: &Engine, table_id: u16) -> Option<Vec<u8>> {
    engine
        .read()
        .unwrap()
        .unique_marker_view(ReadView::Committed, table_id)
        .unwrap()
}

/// Runs write statements that change nothing until the migration is complete. Returns how many
/// transactions it took.
fn drive(fx: &Fx) -> usize {
    for i in 1..=50 {
        fx.conn
            .execute_batch("DELETE FROM t WHERE k='-no-such-row-'")
            .unwrap();
        if marker(fx).is_some() {
            return i;
        }
    }
    panic!("the migration did not finish");
}

#[test]
fn migration_builds_the_index_for_tables_of_every_size_and_durability() {
    for (rows, extra, expected_begins) in [
        (0, "durability='sync', ", 1),
        (1, "durability='sync', ", 1),
        (10_000, "durability='sync', ", 3),
        (10_000, "", 3),
    ] {
        let fx = fixture(&format!("unique_mig_size{rows}"), extra);
        legacy_table(&fx, rows);
        assert!(marker(&fx).is_none());
        // The work is bounded per transaction: 4096 rows each.
        assert_eq!(drive(&fx), expected_begins, "{rows} rows");
        assert_eq!(marker(&fx).unwrap(), std_def().marker_value());
        assert_index(&fx, &format!("{rows} rows {extra}"));
        // Nothing is left to do: further writes do not touch the marker again.
        fx.conn
            .execute_batch("INSERT INTO t VALUES('zz','zza',99999,'zzn','c')")
            .unwrap();
        assert_index(&fx, "after the first write");
    }
}

#[test]
fn a_synchronous_migration_syncs_once_regardless_of_row_count() {
    fn syncs(name: &str, rows: usize, durability: WriteDurability) -> u64 {
        let dir = TempTestDir::new(name);
        let counter = Arc::new(AtomicU64::new(0));
        let fs = Arc::new(CountFs {
            inner: OsFs::open(&dir.path, true).unwrap(),
            syncs: Arc::clone(&counter),
        });
        let mut engine = StorageEngine::open(fs, EngineConfig::default()).unwrap();
        for i in 0..rows {
            let key = SqlValue::Text(format!("key{i:05}"));
            let row = encode_row(&[key.clone(), SqlValue::Text(format!("v{i}"))]);
            engine
                .put_row(
                    1,
                    &key.to_key_bytes(),
                    &row,
                    &[],
                    WriteDurability::BufferedFast,
                )
                .unwrap();
        }
        engine.checkpoint().unwrap();
        let engine = RwLock::new(engine);
        let def = UniqueDef::new(vec![(1, ColCollation::Binary, ColumnAffinity::Text)]);
        let before = counter.load(Ordering::SeqCst);
        let params = Params {
            table_id: 1,
            durability,
            def: &def,
            chunk_rows: 8,
            budget_rows: usize::MAX,
        };
        let outcome = unique_migration::migrate(&engine, &params, &mut None);
        assert!(matches!(outcome, Outcome::Complete), "{outcome:?}");
        counter.load(Ordering::SeqCst) - before
    }
    let sync = WriteDurability::SyncTransactional;
    let one = syncs("unique_mig_sync_one", 1, sync);
    let many = syncs("unique_mig_sync_many", 300, sync);
    assert!(one > 0, "a synchronous migration must reach the device");
    assert_eq!(many, one, "syncs must not grow with the number of rows");
    assert_eq!(
        syncs(
            "unique_mig_sync_buffered",
            300,
            WriteDurability::BufferedFast
        ),
        0
    );
}

#[test]
fn a_transaction_already_open_skips_the_migration_and_keeps_the_scan() {
    let fx = fixture("unique_mig_open_tx", "");
    fx.conn
        .execute_batch(
            "CREATE VIRTUAL TABLE u USING aydb(table_id=2, \
             columns='k TEXT PRIMARY KEY, a TEXT UNIQUE')",
        )
        .unwrap();
    legacy_table(&fx, 3);
    let row = encode_row(&[SqlValue::Text("u1".into()), SqlValue::Text("ua".into())]);
    {
        let mut eng = fx.engine.write().unwrap();
        let pk = SqlValue::Text("u1".into()).to_key_bytes();
        eng.put_row(2, &pk, &row, &[], WriteDurability::BufferedFast)
            .unwrap();
        rowid_map::assign_synthetic_rowid(&mut eng, 2, &pk, WriteDurability::BufferedFast, false)
            .unwrap();
    }
    // The first write of the transaction migrates `t`; the engine transaction then covers `u`.
    fx.conn
        .execute_batch(
            "BEGIN; INSERT INTO t VALUES('n1','na',500,'nn','c'); \
             INSERT INTO u VALUES('u2','ub');",
        )
        .unwrap();
    assert!(marker(&fx).is_some());
    assert!(
        marker_of(&fx.engine, 2).is_none(),
        "skipped inside a transaction"
    );
    // The conflict check still works through the scan.
    assert!(
        fx.conn
            .execute_batch("INSERT INTO u VALUES('u3','ua')")
            .is_err()
    );
    fx.conn.execute_batch("COMMIT").unwrap();
    assert!(
        marker_of(&fx.engine, 2).is_none(),
        "commit does not migrate"
    );
    fx.conn
        .execute_batch("INSERT INTO u VALUES('u4','uc')")
        .unwrap();
    assert!(
        marker_of(&fx.engine, 2).is_some(),
        "the next transaction does"
    );
    let eng = fx.engine.read().unwrap();
    assert_eq!(
        eng.lookup_by_secondary_index(2, 0x8001, &[0x20, b'u', b'a'])
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn a_connection_with_a_read_snapshot_is_not_made_stale_by_the_migration() {
    let fx = fixture("unique_mig_snapshot", "");
    legacy_table(&fx, 4);
    fx.conn
        .execute_batch("BEGIN; SELECT count(*) FROM t;")
        .unwrap();
    // The write upgrades the open read; a migration commit before it would make it stale.
    fx.conn
        .execute_batch("INSERT INTO t VALUES('n1','na',500,'nn','c')")
        .unwrap();
    fx.conn.execute_batch("COMMIT").unwrap();
    drive(&fx);
    assert_index(&fx, "after the snapshot transaction");
}

#[test]
fn rolling_back_the_triggering_transaction_keeps_the_marker_and_the_index() {
    let fx = fixture("unique_mig_rollback", "");
    legacy_table(&fx, 5);
    fx.conn
        .execute_batch("BEGIN; INSERT INTO t VALUES('n1','na',500,'nn','c'); ROLLBACK;")
        .unwrap();
    assert!(marker(&fx).is_some());
    assert_eq!(rows_of(&fx.conn).len(), 5);
    assert_index(&fx, "after rollback");
}

#[test]
fn legacy_duplicates_get_an_entry_for_each_row() {
    let fx = fixture("unique_mig_duplicates", "");
    put_indexed_legacy_row(&fx, "k1", "dup", 1, "n1", "c");
    put_indexed_legacy_row(&fx, "k2", "dup", 2, "n2", "c");
    put_indexed_legacy_row(&fx, "k3", "other", 3, "n3", "c");
    drive(&fx);
    assert_index(&fx, "duplicates");
    let eng = fx.engine.read().unwrap();
    let mut pks = eng
        .lookup_by_secondary_index(1, 0x8001, &[0x20, b'd', b'u', b'p'])
        .unwrap();
    pks.sort();
    assert_eq!(pks.len(), 2);
}

fn corrupt_big_row_fixture(name: &str, policy: &str) -> (Fx, usize) {
    let dir = TempTestDir::new(name);
    {
        let engine = open_engine(&dir);
        let conn = connect(&engine, 1, "");
        let fx = Fx {
            dir: TempTestDir::new(&format!("{name}_unused")),
            engine: engine.clone(),
            conn,
            table_id: 1,
            capped: false,
        };
        for i in 0..5 {
            let a = if i == 2 {
                "x".repeat(8000)
            } else {
                format!("a{i}")
            };
            put_indexed_legacy_row(&fx, &format!("k{i}"), &a, i, &format!("n{i}"), "c");
        }
        engine.write().unwrap().checkpoint().unwrap();
        drop(fx);
    }
    // Damage the out-of-line value of the big row.
    let blob = std::fs::read_dir(&dir.path)
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("blob_std_"))
        })
        .expect("the big row is stored out of line");
    let mut bytes = std::fs::read(&blob).unwrap();
    let mid = bytes.len() / 2;
    for b in &mut bytes[mid..mid + 64] {
        *b ^= 0xFF;
    }
    std::fs::write(&blob, bytes).unwrap();

    let engine = open_engine(&dir);
    let conn = connect(&engine, 1, policy);
    (
        Fx {
            dir,
            engine,
            conn,
            table_id: 1,
            capped: false,
        },
        5,
    )
}

#[test]
fn an_unreadable_row_stops_the_migration_without_failing_the_statement() {
    let (fx, _) = corrupt_big_row_fixture("unique_mig_corrupt_error", "on_corrupt=error, ");
    let before = fx.engine.read().unwrap().maintenance_failures();
    fx.conn
        .execute_batch("DELETE FROM t WHERE k='-no-such-row-'")
        .unwrap();
    assert!(marker(&fx).is_none(), "no marker over an unreadable row");
    {
        let eng = fx.engine.read().unwrap();
        assert_eq!(eng.maintenance_failures(), before + 1);
        assert!(eng.last_maintenance_error().is_some());
    }
    // The statement works (through the scan, which skips what it cannot conflict with) and the
    // table is not read again at every transaction.
    for _ in 0..3 {
        fx.conn
            .execute_batch("DELETE FROM t WHERE k='-no-such-row-'")
            .unwrap();
    }
    assert_eq!(
        fx.engine.read().unwrap().maintenance_failures(),
        before + 1,
        "one attempt per connection"
    );
    // A reconnect tries again.
    let again = connect(&fx.engine, 1, "on_corrupt=error, ");
    again
        .execute_batch("DELETE FROM t WHERE k='-no-such-row-'")
        .unwrap();
    assert_eq!(fx.engine.read().unwrap().maintenance_failures(), before + 2);
}

#[test]
fn an_unreadable_row_under_evict_is_skipped_and_the_rest_is_indexed() {
    let (fx, _) = corrupt_big_row_fixture("unique_mig_corrupt_evict", "on_corrupt=evict, ");
    drive(&fx);
    assert!(marker(&fx).is_some());
    // The corrupt row is evicted as soon as it is noticed; everything else is exact.
    fx.conn
        .execute_batch("DELETE FROM t WHERE k='-no-such-row-'")
        .unwrap();
    let rows = rows_of(&fx.conn);
    assert!(rows.len() >= 4);
    assert_index(&fx, "after evict");
}

#[test]
fn a_changed_definition_rebuilds_the_index_and_removes_the_stale_entries() {
    let dir = TempTestDir::new("unique_mig_definition");
    let engine = open_engine(&dir);
    let connect_u = |columns: &str| {
        let conn = SqliteConnection::open_in_memory().unwrap();
        conn.register_aydb_module_with_options(engine.clone(), false)
            .unwrap();
        conn.execute_batch(&format!(
            "CREATE VIRTUAL TABLE u USING aydb(table_id=2, columns='{columns}')"
        ))
        .unwrap();
        conn
    };
    let first = connect_u("k TEXT PRIMARY KEY, a TEXT UNIQUE, b TEXT");
    first
        .execute_batch("INSERT INTO u VALUES('k1','Ab','B1'); INSERT INTO u VALUES('k2','cd','B2')")
        .unwrap();
    let entry = |col: u16, key: &[u8]| {
        engine
            .read()
            .unwrap()
            .lookup_by_secondary_index(2, 0x8000 | col, key)
            .unwrap()
            .len()
    };
    assert_eq!(entry(1, &[0x20, b'A', b'b']), 1);
    let old_marker = marker_of(&engine, 2).unwrap();

    // Same column, different collation: the keys change.
    let nocase = connect_u("k TEXT PRIMARY KEY, a TEXT UNIQUE COLLATE NOCASE, b TEXT");
    nocase
        .execute_batch("DELETE FROM u WHERE k='-no-such-row-'")
        .unwrap();
    assert_ne!(marker_of(&engine, 2).unwrap(), old_marker);
    assert_eq!(entry(1, &[0x20, b'A', b'b']), 0, "stale entry removed");
    assert_eq!(entry(1, &[0x20, b'a', b'b']), 1);
    assert_eq!(entry(1, &[0x20, b'c', b'd']), 1);

    // Another column becomes the UNIQUE one: the old column's entries go away.
    let other = connect_u("k TEXT PRIMARY KEY, a TEXT, b TEXT UNIQUE");
    other
        .execute_batch("DELETE FROM u WHERE k='-no-such-row-'")
        .unwrap();
    assert_eq!(entry(1, &[0x20, b'a', b'b']), 0);
    assert_eq!(entry(2, &[0x20, b'B', b'1']), 1);
    let keys: Vec<Vec<u8>> = engine.read().unwrap().test_live_entry_keys(2).unwrap();
    assert_eq!(keys.len(), 2, "only the two entries of the new definition");
    let mut eng = engine.write().unwrap();
    let meta = eng
        .test_row_meta(2, &SqlValue::Text("k1".into()).to_key_bytes())
        .unwrap()
        .unwrap();
    assert_eq!(meta.secondary_keys.len(), 1);
    assert_eq!(meta.secondary_keys[0].0, 0x8002);
}

/// Index state of the small `u(k, a UNIQUE)` table used below.
fn assert_u_index(engine: &Engine, conn: &SqliteConnection, what: &str) {
    let rows = conn.query_all("SELECT k, a FROM u", &[]).unwrap();
    let mut want = std::collections::BTreeSet::new();
    let mut want_meta = std::collections::BTreeSet::new();
    for row in &rows {
        let pk = row[0].to_key_bytes();
        if row[1] != SqlValue::Null {
            want.insert(encode_secondary_index_key(
                2,
                0x8001,
                &row[1].to_key_bytes(),
                &pk,
            ));
            want_meta.insert(encode_row_meta_key(2, &pk));
        }
    }
    let eng = engine.read().unwrap();
    let got: std::collections::BTreeSet<Vec<u8>> =
        eng.test_live_entry_keys(2).unwrap().into_iter().collect();
    assert_eq!(got, want, "{what}: entries");
    let got_meta: std::collections::BTreeSet<Vec<u8>> = eng
        .test_live_keys(2, NS_ROW_META)
        .unwrap()
        .into_iter()
        .collect();
    assert_eq!(got_meta, want_meta, "{what}: 't' records");
}

#[test]
fn writers_between_chunks_keep_the_index_exact() {
    for seed in 0..30u64 {
        let dir = TempTestDir::new(&format!("unique_mig_interleave{seed}"));
        let engine = open_engine(&dir);
        let conn = SqliteConnection::open_in_memory().unwrap();
        conn.register_aydb_module_with_options(engine.clone(), false)
            .unwrap();
        conn.execute_batch(
            "CREATE VIRTUAL TABLE u USING aydb(table_id=2, \
             columns='k TEXT PRIMARY KEY, a TEXT UNIQUE')",
        )
        .unwrap();
        for i in 0..40 {
            let row = encode_row(&[
                SqlValue::Text(format!("k{i:02}")),
                SqlValue::Text(format!("a{i}")),
            ]);
            let mut eng = engine.write().unwrap();
            let pk = SqlValue::Text(format!("k{i:02}")).to_key_bytes();
            eng.put_row(2, &pk, &row, &[], WriteDurability::BufferedFast)
                .unwrap();
            rowid_map::assign_synthetic_rowid(
                &mut eng,
                2,
                &pk,
                WriteDurability::BufferedFast,
                false,
            )
            .unwrap();
        }
        let def = UniqueDef::new(vec![(1, ColCollation::Binary, ColumnAffinity::Text)]);
        let params = Params {
            table_id: 2,
            durability: WriteDurability::BufferedFast,
            def: &def,
            chunk_rows: 4,
            budget_rows: 4,
        };
        // Keeps `xBegin` from migrating on its own: this test is the only migrator.
        UNIQUE_MIGRATION_DISABLED.with(|flag| flag.set(true));
        let mut rng = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        let mut next = move |n: u64| {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng % n
        };
        let mut cursor = None;
        let mut chunks = 0;
        loop {
            match unique_migration::migrate(&engine, &params, &mut cursor) {
                Outcome::Complete => break,
                Outcome::Incomplete => {}
                other => panic!("{other:?}"),
            }
            chunks += 1;
            // Writers run while the lock is free: rows ahead of and behind the cursor change
            // value, disappear, appear and replace each other.
            for _ in 0..3 {
                let k = format!("k{:02}", next(44));
                let v = format!("a{}", next(50));
                let sql = match next(5) {
                    0 => format!("UPDATE u SET a='{v}' WHERE k='{k}'"),
                    1 => format!("DELETE FROM u WHERE k='{k}'"),
                    2 => format!("INSERT OR REPLACE INTO u VALUES('{k}','{v}')"),
                    3 => format!("UPDATE u SET a=NULL WHERE k='{k}'"),
                    _ => format!("INSERT OR IGNORE INTO u VALUES('{k}','{v}')"),
                };
                let _ = conn.execute_batch(&sql);
            }
        }
        UNIQUE_MIGRATION_DISABLED.with(|flag| flag.set(false));
        assert!(chunks >= 8, "the migration really ran in chunks");
        assert_u_index(&engine, &conn, &format!("seed {seed}"));
        // Idempotent: another full run changes nothing.
        let outcome = unique_migration::migrate(&engine, &params, &mut None);
        assert!(matches!(outcome, Outcome::Complete));
        assert_u_index(&engine, &conn, &format!("seed {seed} rerun"));
    }
}

#[test]
fn another_connections_transaction_makes_the_migration_stand_aside() {
    let fx = fixture("unique_mig_other_tx", "");
    legacy_table(&fx, 20);
    let other = connect(&fx.engine, 1, "");
    let def = std_def();
    let params = Params {
        table_id: 1,
        durability: WriteDurability::BufferedFast,
        def: &def,
        chunk_rows: 5,
        budget_rows: 5,
    };
    UNIQUE_MIGRATION_DISABLED.with(|flag| flag.set(true));
    let mut cursor = None;
    assert!(matches!(
        unique_migration::migrate(&fx.engine, &params, &mut cursor),
        Outcome::Incomplete
    ));
    other
        .execute_batch("BEGIN; INSERT INTO t VALUES('n1','na',500,'nn','c')")
        .unwrap();
    assert!(matches!(
        unique_migration::migrate(&fx.engine, &params, &mut cursor),
        Outcome::Skipped
    ));
    // The writer was not made to wait or fail for the migration's sake.
    other.execute_batch("COMMIT").unwrap();
    loop {
        match unique_migration::migrate(&fx.engine, &params, &mut cursor) {
            Outcome::Complete => break,
            Outcome::Incomplete => {}
            o => panic!("{o:?}"),
        }
    }
    UNIQUE_MIGRATION_DISABLED.with(|flag| flag.set(false));
    assert_index(&fx, "after the other transaction");
}

#[test]
fn the_latch_stops_all_further_work() {
    let fx = fixture("unique_mig_latch", "");
    legacy_table(&fx, 3);
    drive(&fx);
    // Remove the marker behind the connection's back: it has latched and does not look again.
    fx.engine
        .write()
        .unwrap()
        .remove_unique_marker(1, WriteDurability::BufferedFast)
        .unwrap();
    fx.conn
        .execute_batch("DELETE FROM t WHERE k='-no-such-row-'")
        .unwrap();
    assert!(marker(&fx).is_none());
    // A new connection does not trust the missing marker and builds it again.
    let fresh = connect(&fx.engine, 1, "");
    fresh
        .execute_batch("DELETE FROM t WHERE k='-no-such-row-'")
        .unwrap();
    assert!(marker(&fx).is_some());
    assert_index(&fx, "rebuilt");
}

#[test]
fn a_table_without_unique_columns_never_gets_a_marker() {
    let dir = TempTestDir::new("unique_mig_none");
    let engine = open_engine(&dir);
    let conn = SqliteConnection::open_in_memory().unwrap();
    conn.register_aydb_module_with_options(engine.clone(), false)
        .unwrap();
    conn.execute_batch(
        "CREATE VIRTUAL TABLE p USING aydb(table_id=3, columns='k TEXT PRIMARY KEY, v TEXT')",
    )
    .unwrap();
    conn.execute_batch("INSERT INTO p VALUES('a','1')").unwrap();
    assert!(marker_of(&engine, 3).is_none());
}
