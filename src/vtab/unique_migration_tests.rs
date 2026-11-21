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

fn with_disabled_migration<R>(f: impl FnOnce() -> R) -> R {
    let prev = UNIQUE_MIGRATION_DISABLED.with(|flag| flag.replace(true));
    let res = f();
    UNIQUE_MIGRATION_DISABLED.with(|flag| flag.set(prev));
    res
}

fn legacy_fixture(name: &str, extra: &str) -> Fx {
    with_disabled_migration(|| fixture(name, extra))
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
        let fx = legacy_fixture(&format!("unique_mig_size{rows}"), extra);
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
    // A buffered table never waits for the device for its rows or its marker.
    let buffered_one = syncs(
        "unique_mig_sync_buffered1",
        1,
        WriteDurability::BufferedFast,
    );
    let buffered_many = syncs(
        "unique_mig_sync_buffered",
        300,
        WriteDurability::BufferedFast,
    );
    assert_eq!(buffered_many, buffered_one);
    assert!(buffered_many < one, "{buffered_many} vs {one}");
}

#[test]
fn a_transaction_already_open_skips_the_migration_and_keeps_the_scan() {
    let fx = legacy_fixture("unique_mig_open_tx", "");
    with_disabled_migration(|| {
        fx.conn
            .execute_batch(
                "CREATE VIRTUAL TABLE u USING aydb(table_id=2, \
                 columns='k TEXT PRIMARY KEY, a TEXT UNIQUE')",
            )
            .unwrap();
    });
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
    let fx = legacy_fixture("unique_mig_snapshot", "");
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
    let fx = legacy_fixture("unique_mig_rollback", "");
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
    let fx = legacy_fixture("unique_mig_duplicates", "");
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
        let conn = with_disabled_migration(|| connect(&engine, 1, ""));
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
        with_disabled_migration(|| {
            conn.execute_batch(
                "CREATE VIRTUAL TABLE u USING aydb(table_id=2, \
                 columns='k TEXT PRIMARY KEY, a TEXT UNIQUE')",
            )
            .unwrap();
        });
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
    let fx = legacy_fixture("unique_mig_other_tx", "");
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
    let fx = legacy_fixture("unique_mig_latch", "");
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

#[test]
fn migrate_after_pending_group_commit_preserves_new_unique_entry() {
    use crate::engine::CommitStep;
    use crate::vtab::unique_key::UniqueDef;
    use crate::vtab::unique_migration::{Outcome, Params, migrate};

    let dir = TempTestDir::new("unique_mig_pending_group");
    let engine = open_engine(&dir);
    let pk = SqlValue::Integer(1).to_key_bytes();
    let def = UniqueDef::new(vec![(1, ColCollation::Binary, ColumnAffinity::Text)]);
    let row_old = encode_row(&[SqlValue::Integer(1), SqlValue::Text("old".into())]);
    let row_new = encode_row(&[SqlValue::Integer(1), SqlValue::Text("new".into())]);
    let old_keys = def.entries(&row_old).unwrap();
    let new_keys = def.entries(&row_new).unwrap();

    {
        let mut eng = engine.write().unwrap();
        eng.put_row(
            1,
            &pk,
            &row_old,
            &old_keys,
            WriteDurability::SyncTransactional,
        )
        .unwrap();

        eng.begin_transaction_for_conn(1).unwrap();
        eng.put_row(
            1,
            &pk,
            &row_new,
            &new_keys,
            WriteDurability::SyncTransactional,
        )
        .unwrap();
        eng.prepare_commit_transaction(WriteDurability::SyncTransactional)
            .unwrap();
        assert!(matches!(
            eng.begin_commit_for_conn(1).unwrap(),
            CommitStep::AwaitingGroup { .. }
        ));
        assert_eq!(
            eng.get_row(1, &pk).unwrap().as_deref(),
            Some(row_old.as_slice())
        );
    }

    let params = Params {
        table_id: 1,
        durability: WriteDurability::SyncTransactional,
        def: &def,
        chunk_rows: 16,
        budget_rows: 100,
    };
    let mut cursor = None;
    assert!(matches!(
        migrate(&engine, &params, &mut cursor),
        Outcome::Complete
    ));

    let eng = engine.read().unwrap();
    assert_eq!(
        eng.lookup_by_secondary_index(1, 0x8001, &SqlValue::Text("new".into()).to_key_bytes())
            .unwrap(),
        vec![pk.clone()]
    );
    assert!(
        eng.lookup_by_secondary_index(1, 0x8001, &SqlValue::Text("old".into()).to_key_bytes())
            .unwrap()
            .is_empty()
    );
    assert!(
        eng.unique_marker_view(ReadView::Committed, 1)
            .unwrap()
            .is_some()
    );
}

#[test]
fn new_empty_unique_table_writes_marker_atomically_on_create_and_avoids_scan_in_transaction() {
    use crate::engine::TABLE_SCAN_ENTRIES_COPIED;

    let fx = fixture("unique_atomic_create", "");
    assert_eq!(marker(&fx), Some(std_def().marker_value()));

    // 1. Creating a brand-new UNIQUE table inside BEGIN and bulk-inserting avoids O(N^2) scans.
    fx.conn
        .execute_batch(
            "BEGIN;
             CREATE VIRTUAL TABLE t1 USING aydb(table_id=2,
                 columns='k TEXT PRIMARY KEY, a TEXT UNIQUE');",
        )
        .unwrap();
    assert_eq!(
        fx.engine
            .read()
            .unwrap()
            .unique_marker_view(ReadView::Owner, 2)
            .unwrap(),
        Some(UniqueDef::new(vec![(1, ColCollation::Binary, ColumnAffinity::Text)]).marker_value())
    );
    TABLE_SCAN_ENTRIES_COPIED.with(|c| c.set(0));
    for i in 0..200 {
        fx.conn
            .execute_batch(&format!("INSERT INTO t1 VALUES('k{i:03}','a{i:03}')"))
            .unwrap();
    }
    assert!(
        fx.conn
            .execute_batch("INSERT INTO t1 VALUES('dup','a050')")
            .is_err()
    );
    assert_eq!(TABLE_SCAN_ENTRIES_COPIED.with(|c| c.get()), 0);
    fx.conn.execute_batch("COMMIT").unwrap();
    assert!(marker_of(&fx.engine, 2).is_some());

    // 2. Same when another write opened the transaction BEFORE CREATE VIRTUAL TABLE, plus ROLLBACK.
    fx.conn
        .execute_batch(
            "BEGIN;
             INSERT INTO t VALUES('seed','sa',1,'sn','c');
             CREATE VIRTUAL TABLE t2 USING aydb(table_id=3,
                 columns='k TEXT PRIMARY KEY, a TEXT UNIQUE');",
        )
        .unwrap();
    TABLE_SCAN_ENTRIES_COPIED.with(|c| c.set(0));
    for i in 0..200 {
        fx.conn
            .execute_batch(&format!("INSERT INTO t2 VALUES('k{i:03}','b{i:03}')"))
            .unwrap();
    }
    assert_eq!(TABLE_SCAN_ENTRIES_COPIED.with(|c| c.get()), 0);
    fx.conn.execute_batch("COMMIT").unwrap();
    assert!(marker_of(&fx.engine, 3).is_some());

    fx.conn
        .execute_batch(
            "BEGIN;
             INSERT INTO t VALUES('seed2','sa2',2,'sn2','c');
             CREATE VIRTUAL TABLE t3 USING aydb(table_id=4,
                 columns='k TEXT PRIMARY KEY, a TEXT UNIQUE');
             INSERT INTO t3 VALUES('k0','v0');
             ROLLBACK;",
        )
        .unwrap();
    assert!(marker_of(&fx.engine, 4).is_none());
}

#[test]
fn attaching_unique_vtab_to_nonempty_explicit_table_id_does_not_write_marker() {
    let dir = TempTestDir::new("unique_nonempty_explicit_id");
    let engine = open_engine(&dir);
    let pk1 = SqlValue::Text("k1".into()).to_key_bytes();
    let pk2 = SqlValue::Text("k2".into()).to_key_bytes();
    let pk3 = SqlValue::Text("k3".into()).to_key_bytes();
    {
        let mut eng = engine.write().unwrap();
        for (pk, a) in [(&pk1, "dup_a"), (&pk2, "b2"), (&pk3, "c3")] {
            let row = encode_row(&[
                SqlValue::Text(String::from_utf8(pk[1..].to_vec()).unwrap()),
                SqlValue::Text(a.into()),
            ]);
            eng.put_row(9, pk, &row, &[], WriteDurability::BufferedFast)
                .unwrap();
            rowid_map::assign_synthetic_rowid(
                &mut eng,
                9,
                pk,
                WriteDurability::BufferedFast,
                false,
            )
            .unwrap();
        }
    }

    let conn = SqliteConnection::open_in_memory().unwrap();
    conn.register_aydb_module_with_options(engine.clone(), false)
        .unwrap();
    conn.execute_batch(
        "CREATE VIRTUAL TABLE legacy9 USING aydb(table_id=9,
             columns='k TEXT PRIMARY KEY, a TEXT UNIQUE')",
    )
    .unwrap();
    // Attaching existing raw rows must NOT forge a UNIQUE completion marker before migration.
    assert!(marker_of(&engine, 9).is_none());

    // Duplicate insert fails (via migration or scan) and triggers migration so marker appears.
    assert!(
        conn.execute_batch("INSERT INTO legacy9 VALUES('k4','dup_a')")
            .is_err()
    );
    assert!(marker_of(&engine, 9).is_some());
    let eng = engine.read().unwrap();
    assert_eq!(
        eng.lookup_by_secondary_index(9, 0x8001, &[0x20, b'd', b'u', b'p', b'_', b'a'])
            .unwrap(),
        vec![pk1]
    );
}

#[test]
fn unique_migration_chunk_commits_in_single_wal_batch_and_recovers() {
    let dir = TempTestDir::new("unique_mig_single_batch");
    let def = UniqueDef::new(vec![
        (1, ColCollation::Binary, ColumnAffinity::Text),
        (2, ColCollation::NoCase, ColumnAffinity::Text),
    ]);
    {
        let engine = open_engine(&dir);
        {
            let mut eng = engine.write().unwrap();
            for i in 0..200 {
                let k = format!("k{i:03}");
                let pk = SqlValue::Text(k.clone()).to_key_bytes();
                let a = if i % 10 == 0 {
                    SqlValue::Null
                } else {
                    SqlValue::Text(format!("A{i:03}"))
                };
                let n = SqlValue::Text(format!("N{i:03}"));
                let c = SqlValue::Text("declared".into());
                let row = encode_row(&[SqlValue::Text(k), a, n, c.clone()]);
                // Mix legacy rows with and without declared-index `'t'` metadata.
                let declared = if i % 2 == 0 {
                    vec![(1u16, c.to_key_bytes())]
                } else {
                    Vec::new()
                };
                eng.put_row(1, &pk, &row, &declared, WriteDurability::BufferedFast)
                    .unwrap();
            }
            eng.checkpoint().unwrap();
        }

        let lsn_before = engine.read().unwrap().next_lsn();
        let params = Params {
            table_id: 1,
            durability: WriteDurability::SyncTransactional,
            def: &def,
            chunk_rows: 512,
            budget_rows: 512,
        };
        let outcome = unique_migration::migrate(&engine, &params, &mut None);
        assert!(matches!(outcome, Outcome::Complete), "{outcome:?}");

        let lsn_after = engine.read().unwrap().next_lsn();
        // 1 WAL batch for the entire 200-row chunk + 1 WAL batch for the completion marker.
        assert_eq!(
            lsn_after - lsn_before,
            2,
            "expected single chunk batch + single marker batch, consumed {} LSNs",
            lsn_after - lsn_before
        );
    }

    // Reopen from disk and verify every row's UNIQUE entries and `'t'` record survived.
    let reopened = open_engine(&dir);
    let mut eng = reopened.write().unwrap();
    assert_eq!(
        eng.unique_marker_view(ReadView::Committed, 1).unwrap(),
        Some(def.marker_value())
    );
    for i in 0..200 {
        let pk = SqlValue::Text(format!("k{i:03}")).to_key_bytes();
        let n_key = SqlValue::Text(format!("n{i:03}")).to_key_bytes();
        assert_eq!(
            eng.lookup_by_secondary_index(1, 0x8002, &n_key).unwrap(),
            vec![pk.clone()]
        );
        if i % 10 != 0 {
            let a_key = SqlValue::Text(format!("A{i:03}")).to_key_bytes();
            assert_eq!(
                eng.lookup_by_secondary_index(1, 0x8001, &a_key).unwrap(),
                vec![pk.clone()]
            );
        }
        let meta = eng.test_row_meta(1, &pk).unwrap().unwrap();
        let expected_ids: Vec<u16> = match (i % 2 == 0, i % 10 != 0) {
            (true, true) => vec![1, 0x8001, 0x8002],
            (true, false) => vec![1, 0x8002],
            (false, true) => vec![0x8001, 0x8002],
            (false, false) => vec![0x8002],
        };
        let got_ids: Vec<u16> = meta.secondary_keys.iter().map(|p| p.0).collect();
        assert_eq!(got_ids, expected_ids, "row {i}");
    }
}

#[test]
fn reconcile_unique_keys_batch_splits_sub_batches_cleanly_on_small_wal_ring() {
    let dir = TempTestDir::new("unique_mig_small_wal_split");
    let fs = Arc::new(OsFs::open(&dir.path, true).unwrap());
    let mut cfg = EngineConfig::default();
    cfg.sync_wal_size_bytes = 32 * 1024;
    cfg.buf_wal_size_bytes = 32 * 1024;
    let mut eng = StorageEngine::open(fs, cfg).unwrap();

    let mut batch_rows = Vec::new();
    for i in 0..160u32 {
        let pk = format!("pk{i:04}_pad_to_trigger_sub_batch_split").into_bytes();
        let row = vec![b'r'; 200];
        eng.put_row(1, &pk, &row, &[], WriteDurability::BufferedFast)
            .unwrap();
        let desired = vec![(0x8001u16, format!("val_{i:04}").into_bytes())];
        batch_rows.push((pk, row.len(), desired));
    }

    let lsn_before = eng.next_lsn();
    let updated = eng
        .reconcile_unique_keys_batch(1, &batch_rows, WriteDurability::BufferedFast)
        .unwrap();
    assert_eq!(updated, 160);
    let batches = eng.next_lsn() - lsn_before;
    assert!(
        (2..10).contains(&batches),
        "expected a few bounded sub-batches on 32 KB WAL ring, got {batches}"
    );
}
