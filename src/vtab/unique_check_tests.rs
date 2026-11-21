//! The UNIQUE conflict check through the index: it must agree with the scan and with native
//! SQLite, read no row for ordinary hits, and survive corrupt rows and stale entries.

use super::unique_index_tests::{
    NATIVE_DDL, StatementGen, assert_index, fixture, open_engine, sorted_rows,
};
use super::unique_key::unique_index_component;
use super::*;
use crate::index::encode_secondary_index_key;
use crate::util::test_util::TempTestDir;

fn with_scan<R>(scan: bool, f: impl FnOnce() -> R) -> R {
    UNIQUE_MIGRATION_DISABLED.with(|flag| flag.set(scan));
    let r = f();
    UNIQUE_MIGRATION_DISABLED.with(|flag| flag.set(false));
    r
}

#[test]
fn scan_index_and_native_sqlite_agree_on_random_statements() {
    for seed in 0..120u64 {
        let mut gen_ = StatementGen::new(seed ^ 0x5555);
        let scanned = with_scan(true, || fixture(&format!("unique_check_scan{seed}"), ""));
        let indexed = fixture(&format!("unique_check_index{seed}"), "");
        let native = SqliteConnection::open_in_memory().unwrap();
        native.execute_batch(NATIVE_DDL).unwrap();
        for step in 0..60 {
            let sql = gen_.next();
            let expected = native.execute_batch(&sql).is_ok();
            let by_scan = with_scan(true, || scanned.conn.execute_batch(&sql).is_ok());
            let by_index = with_scan(false, || indexed.conn.execute_batch(&sql).is_ok());
            let at = format!("seed {seed} step {step}: {sql}");
            assert_eq!(expected, by_scan, "scan: {at}");
            assert_eq!(expected, by_index, "index: {at}");
            let rows = sorted_rows(&native);
            assert_eq!(rows, sorted_rows(&scanned.conn), "scan rows: {at}");
            assert_eq!(rows, sorted_rows(&indexed.conn), "index rows: {at}");
            assert_index(&scanned, &at);
            assert_index(&indexed, &at);
        }
        // The indexed run really used the index from its first write on.
        assert!(
            indexed
                .engine
                .read()
                .unwrap()
                .unique_marker_view(crate::engine::ReadView::Committed, 1)
                .unwrap()
                .is_some()
        );
        assert!(
            scanned
                .engine
                .read()
                .unwrap()
                .unique_marker_view(crate::engine::ReadView::Committed, 1)
                .unwrap()
                .is_none()
        );
        if gen_.in_tx {
            for conn in [&native, &scanned.conn, &indexed.conn] {
                conn.execute_batch("ROLLBACK").unwrap();
            }
        }
    }
}

#[test]
fn an_insert_on_a_migrated_table_reads_no_row_whatever_its_size() {
    use crate::engine::TABLE_SCAN_ENTRIES_COPIED;
    let copied = |f: &dyn Fn()| {
        TABLE_SCAN_ENTRIES_COPIED.with(|c| c.set(0));
        f();
        TABLE_SCAN_ENTRIES_COPIED.with(|c| c.get())
    };
    let mut by_size = Vec::new();
    for rows in [50usize, 2000] {
        let fx = fixture(&format!("unique_check_cost{rows}"), "");
        let values: Vec<String> = (0..rows)
            .map(|i| format!("('k{i:05}','a{i}',{i},'n{i}','c')"))
            .collect();
        fx.conn
            .execute_batch(&format!("INSERT INTO t VALUES {}", values.join(",")))
            .unwrap();
        let read = copied(&|| {
            fx.conn
                .execute_batch("INSERT INTO t VALUES('new','anew',900000,'nnew','c')")
                .unwrap();
            assert!(
                fx.conn
                    .execute_batch("INSERT INTO t VALUES('new2','a7',900001,'nnew2','c')")
                    .is_err()
            );
            fx.conn
                .execute_batch("UPDATE t SET a='moved' WHERE k='k00007'")
                .unwrap();
        });
        by_size.push(read);
        // The same statements through the scan do read the table.
        let scanned = with_scan(true, || {
            fixture(&format!("unique_check_cost_scan{rows}"), "")
        });
        with_scan(true, || {
            scanned
                .conn
                .execute_batch(&format!("INSERT INTO t VALUES {}", values.join(",")))
                .unwrap();
            let read = copied(&|| {
                scanned
                    .conn
                    .execute_batch("INSERT INTO t VALUES('new','anew',900000,'nnew','c')")
                    .unwrap();
            });
            assert!(read >= rows, "the scan read {read} of {rows} rows");
        });
    }
    assert_eq!(by_size, vec![0, 0], "the index path copies no row");
}

/// A table `u(k, a UNIQUE, body)` with a big `body` in row `k2`, whose out-of-line value is then
/// damaged on disk. Returns the reopened connection and engine.
fn corrupt_row_table(
    name: &str,
    policy: &str,
    a_of_corrupt: &str,
) -> (
    TempTestDir,
    super::unique_index_tests::Engine,
    SqliteConnection,
) {
    let dir = TempTestDir::new(name);
    let ddl = |policy: &str| {
        format!(
            "CREATE VIRTUAL TABLE u USING aydb(table_id=2, {policy}\
             columns='k TEXT PRIMARY KEY, a TEXT UNIQUE, body TEXT')"
        )
    };
    {
        let engine = open_engine(&dir);
        let conn = SqliteConnection::open_in_memory().unwrap();
        conn.register_aydb_module_with_options(engine.clone(), false)
            .unwrap();
        conn.execute_batch(&ddl("")).unwrap();
        conn.execute_batch("INSERT INTO u VALUES('k1','a1','small')")
            .unwrap();
        conn.execute_batch(&format!(
            "INSERT INTO u VALUES('k2','{a_of_corrupt}','{}')",
            "x".repeat(8000)
        ))
        .unwrap();
        conn.execute_batch("INSERT INTO u VALUES('k3','a3','small')")
            .unwrap();
        engine.write().unwrap().checkpoint().unwrap();
    }
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
    let conn = SqliteConnection::open_in_memory().unwrap();
    conn.register_aydb_module_with_options(engine.clone(), false)
        .unwrap();
    conn.execute_batch(&ddl(policy)).unwrap();
    (dir, engine, conn)
}

#[test]
fn a_corrupt_row_no_longer_fails_unrelated_inserts_but_real_conflicts_are_found() {
    for policy in ["on_corrupt=error, ", "on_corrupt=evict, "] {
        let (_dir, engine, conn) = corrupt_row_table("unique_check_corrupt", policy, "a2");
        // Unrelated writes work: the row is never read.
        conn.execute_batch("INSERT INTO u VALUES('k4','a4','small')")
            .unwrap();
        conn.execute_batch("UPDATE u SET a='a5' WHERE k='k1'")
            .unwrap();
        assert_eq!(
            engine.read().unwrap().corrupt_blob_detections(),
            0,
            "{policy}"
        );
        // A real conflict on the corrupt row's value is still reported.
        assert!(
            conn.execute_batch("INSERT INTO u VALUES('k5','a2','small')")
                .is_err(),
            "{policy}"
        );
        assert_eq!(
            engine.read().unwrap().corrupt_blob_detections(),
            0,
            "{policy}"
        );
        // REPLACE removes the corrupt row without reading its value.
        conn.execute_batch("INSERT OR REPLACE INTO u VALUES('k5','a2','small')")
            .unwrap();
        let rows = conn.query_all("SELECT k FROM u ORDER BY k", &[]).unwrap();
        assert_eq!(
            rows.iter().map(|r| r[0].clone()).collect::<Vec<_>>(),
            ["k1", "k3", "k4", "k5"].map(|k| SqlValue::Text(k.into())),
            "{policy}"
        );
        // No entry of the replaced row is left behind.
        let entries = engine.read().unwrap().test_live_entry_keys(2).unwrap();
        assert_eq!(entries.len(), 4, "{policy}");
    }
}

#[test]
fn a_digest_candidate_on_a_corrupt_row_follows_the_corruption_policy() {
    let long = "q".repeat(300);
    // Under error the candidate cannot be confirmed: the statement fails.
    let (_dir, _engine, conn) =
        corrupt_row_table("unique_check_digest_error", "on_corrupt=error, ", &long);
    assert!(
        conn.execute_batch(&format!("INSERT INTO u VALUES('k5','{long}','small')"))
            .is_err()
    );
    // A different value, even a long one, is not a candidate and is unaffected.
    conn.execute_batch(&format!(
        "INSERT INTO u VALUES('k6','{}','small')",
        "r".repeat(300)
    ))
    .unwrap();

    // Under evict an unreadable candidate is no conflict.
    let (_dir, _engine, conn) =
        corrupt_row_table("unique_check_digest_evict", "on_corrupt=evict, ", &long);
    conn.execute_batch(&format!("INSERT INTO u VALUES('k5','{long}','small')"))
        .unwrap();
}

#[test]
fn stale_entries_and_digest_collisions_are_not_conflicts() {
    let fx = fixture("unique_check_stale", "");
    fx.conn
        .execute_batch("INSERT INTO t VALUES('k1','a1',1,'n1','c')")
        .unwrap();
    let long = "z".repeat(300);
    let component_of_long = {
        let mut key = vec![KEY_TAG_TEXT];
        key.extend_from_slice(long.as_bytes());
        unique_index_component(&super::unique_key::canon_unique_key(&key))
    };
    {
        let mut eng = fx.engine.write().unwrap();
        // An entry whose row does not exist.
        eng.test_put_raw_entry(encode_secondary_index_key(
            1,
            0x8001,
            &[KEY_TAG_TEXT, b's', b't'],
            &SqlValue::Text("ghost".into()).to_key_bytes(),
        ))
        .unwrap();
        // A digest that points at a row whose real value differs (a hash collision).
        eng.test_put_raw_entry(encode_secondary_index_key(
            1,
            0x8001,
            &component_of_long,
            &SqlValue::Text("k1".into()).to_key_bytes(),
        ))
        .unwrap();
    }
    fx.conn
        .execute_batch("INSERT INTO t VALUES('k2','st',2,'n2','c')")
        .unwrap();
    fx.conn
        .execute_batch(&format!("INSERT INTO t VALUES('k3','{long}',3,'n3','c')"))
        .unwrap();
    // The real conflicts remain.
    assert!(
        fx.conn
            .execute_batch("INSERT INTO t VALUES('k4','a1',4,'n4','c')")
            .is_err()
    );
    assert!(
        fx.conn
            .execute_batch(&format!("INSERT INTO t VALUES('k5','{long}',5,'n5','c')"))
            .is_err()
    );
    assert_eq!(fx.conn.query_all("SELECT k FROM t", &[]).unwrap().len(), 3);
}

#[test]
fn a_connection_that_did_not_migrate_trusts_a_marker_another_one_wrote() {
    let fx = with_scan(true, || fixture("unique_check_marker_elsewhere", ""));
    // This connection never migrates; the marker is written by someone else.
    let writer = super::unique_index_tests::connect(&fx.engine, 1, "");
    writer
        .execute_batch("INSERT INTO t VALUES('k1','a1',1,'n1','c')")
        .unwrap();
    assert!(
        fx.engine
            .read()
            .unwrap()
            .unique_marker_view(crate::engine::ReadView::Committed, 1)
            .unwrap()
            .is_some()
    );
    // `fx.conn` connected before the marker existed and never migrates here; it still finds
    // the committed marker and uses the index (the scan would copy the table's rows).
    crate::engine::TABLE_SCAN_ENTRIES_COPIED.with(|c| c.set(0));
    with_scan(true, || {
        assert!(
            fx.conn
                .execute_batch("INSERT INTO t VALUES('k2','a1',2,'n2','c')")
                .is_err()
        );
    });
    assert_eq!(
        crate::engine::TABLE_SCAN_ENTRIES_COPIED.with(|c| c.get()),
        0
    );
}
