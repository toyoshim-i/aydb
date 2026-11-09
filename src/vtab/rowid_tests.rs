use super::*;
use crate::engine::EngineConfig;
use crate::util::test_util::TempTestDir;

const COLLISION_LOW: &[u8] = &[45, 171, 210, 136, 153, 167, 140, 71];
const COLLISION_HIGH: &[u8] = &[215, 86, 151, 114, 215, 213, 49, 45];
const DDL: &str = "CREATE VIRTUAL TABLE t USING aydb(
    table_id=1, durability='sync', columns='k BLOB PRIMARY KEY, v TEXT')";

fn rowid_for(conn: &SqliteConnection, pk: &[u8]) -> i64 {
    let rows = conn
        .query_all(
            "SELECT rowid FROM t WHERE k=?1",
            &[SqlValue::Blob(pk.to_vec())],
        )
        .unwrap();
    let SqlValue::Integer(rowid) = rows[0][0] else {
        panic!("integer rowid expected")
    };
    rowid
}

fn connect(engine: Arc<RwLock<StorageEngine<OsFs>>>) -> SqliteConnection {
    let conn = SqliteConnection::open_in_memory().unwrap();
    conn.register_aydb_module_with_options(engine, false)
        .unwrap();
    conn.execute_batch(DDL).unwrap();
    conn
}

#[test]
fn colliding_rowids_keep_the_same_owner_across_connections_rollback_and_reopen() {
    let dir = TempTestDir::new("persistent_rowid_collision");
    let fs = Arc::new(OsFs::open(&dir.path, true).unwrap());
    let engine = Arc::new(RwLock::new(
        StorageEngine::open(fs.clone(), EngineConfig::default()).unwrap(),
    ));
    let conn = connect(engine.clone());
    let low = SqlValue::Blob(COLLISION_LOW.to_vec()).to_key_bytes();
    let high = SqlValue::Blob(COLLISION_HIGH.to_vec()).to_key_bytes();
    assert_eq!(hash_pk_to_rowid(&low), hash_pk_to_rowid(&high));
    let mut insert = conn.prepare("INSERT INTO t VALUES (?1,?2)").unwrap();
    insert
        .execute(&[
            SqlValue::Blob(COLLISION_HIGH.to_vec()),
            SqlValue::Text("high".into()),
        ])
        .unwrap();
    insert
        .execute(&[
            SqlValue::Blob(COLLISION_LOW.to_vec()),
            SqlValue::Text("low".into()),
        ])
        .unwrap();
    drop(insert);
    let high_id = rowid_for(&conn, COLLISION_HIGH);
    let low_id = rowid_for(&conn, COLLISION_LOW);
    assert_ne!(high_id, low_id);

    let other = connect(engine.clone());
    // Prime the second connection in the opposite order from insertion.
    assert_eq!(rowid_for(&other, COLLISION_LOW), low_id);
    assert_eq!(rowid_for(&other, COLLISION_HIGH), high_id);
    assert_eq!(
        other
            .query_all(
                "SELECT v FROM t WHERE rowid=?1",
                &[SqlValue::Integer(high_id)]
            )
            .unwrap(),
        vec![vec![SqlValue::Text("high".into())]]
    );

    other.execute_batch("BEGIN; SAVEPOINT s").unwrap();
    other
        .prepare("DELETE FROM t WHERE rowid=?1")
        .unwrap()
        .execute(&[SqlValue::Integer(high_id)])
        .unwrap();
    other
        .execute_batch("ROLLBACK TO s; RELEASE s; COMMIT")
        .unwrap();
    assert_eq!(rowid_for(&other, COLLISION_HIGH), high_id);

    drop(other);
    drop(conn);
    drop(engine);
    let reopened = Arc::new(RwLock::new(
        StorageEngine::open(fs, EngineConfig::default()).unwrap(),
    ));
    let conn = connect(reopened.clone());
    assert_eq!(rowid_for(&conn, COLLISION_LOW), low_id);
    assert_eq!(rowid_for(&conn, COLLISION_HIGH), high_id);
    let mut delete = conn.prepare("DELETE FROM t WHERE rowid=?1").unwrap();
    delete.execute(&[SqlValue::Integer(high_id)]).unwrap();
    drop(delete);
    assert_eq!(
        conn.query_all("SELECT v FROM t", &[]).unwrap(),
        vec![vec![SqlValue::Text("low".into())]]
    );
    let engine = reopened.read().unwrap();
    assert_eq!(
        engine
            .rowid_for_pk_view(crate::engine::ReadView::Owner, 1, &high)
            .unwrap(),
        None
    );
    assert_eq!(
        engine
            .pk_for_rowid_view(crate::engine::ReadView::Owner, 1, high_id)
            .unwrap(),
        None
    );
}

#[test]
fn legacy_rows_receive_persistent_identities_before_the_first_query() {
    let dir = TempTestDir::new("legacy_rowid_migration");
    let fs = Arc::new(OsFs::open(&dir.path, true).unwrap());
    let mut engine = StorageEngine::open(fs, EngineConfig::default()).unwrap();
    for (pk, value) in [(COLLISION_HIGH, "high"), (COLLISION_LOW, "low")] {
        let key = SqlValue::Blob(pk.to_vec()).to_key_bytes();
        let row = encode_row(&[SqlValue::Blob(pk.to_vec()), SqlValue::Text(value.into())]);
        engine
            .put_row(1, &key, &row, &[], WriteDurability::SyncTransactional)
            .unwrap();
    }
    let engine = Arc::new(RwLock::new(engine));
    let conn = connect(engine.clone());
    let low_id = rowid_for(&conn, COLLISION_LOW);
    let high_id = rowid_for(&conn, COLLISION_HIGH);
    assert_ne!(low_id, high_id);
    let other = connect(engine);
    assert_eq!(rowid_for(&other, COLLISION_HIGH), high_id);
    assert_eq!(rowid_for(&other, COLLISION_LOW), low_id);
}

fn connect_integer_table(engine: Arc<RwLock<StorageEngine<OsFs>>>) -> SqliteConnection {
    let conn = SqliteConnection::open_in_memory().unwrap();
    conn.register_aydb_module_with_options(engine, false)
        .unwrap();
    conn.execute_batch(
        "CREATE VIRTUAL TABLE t USING aydb(table_id=1, columns='k INT PRIMARY KEY, v TEXT')",
    )
    .unwrap();
    conn
}

#[test]
fn integer_affinity_rows_resolve_actual_rowids_for_lookup_update_delete_and_sort() {
    let dir = TempTestDir::new("mixed_integer_rowids");
    let fs = Arc::new(OsFs::open(&dir.path, true).unwrap());
    let engine = Arc::new(RwLock::new(
        StorageEngine::open(fs, EngineConfig::default()).unwrap(),
    ));
    let conn = connect_integer_table(engine.clone());
    conn.execute_batch("INSERT INTO t VALUES (1,'one'),(3,'three')")
        .unwrap();
    let mut ordered = conn
        .prepare("SELECT rowid,k FROM t ORDER BY rowid")
        .unwrap();
    let other = connect_integer_table(engine.clone());
    other
        .execute_batch("INSERT INTO t VALUES (2.5,'float'),('text','text'),(x'0102','blob')")
        .unwrap();
    assert_eq!(
        ordered.query(&[]).unwrap(),
        conn.query_all("SELECT rowid,k FROM t ORDER BY +rowid", &[])
            .unwrap()
    );
    drop(ordered);

    for key in [
        SqlValue::Float(2.5),
        SqlValue::Text("text".into()),
        SqlValue::Blob(vec![1, 2]),
    ] {
        let rows = conn
            .query_all("SELECT rowid FROM t WHERE k=?1", std::slice::from_ref(&key))
            .unwrap();
        let rowid = rows[0][0].clone();
        assert_eq!(
            other
                .query_all(
                    "SELECT k FROM t WHERE rowid=?1",
                    std::slice::from_ref(&rowid)
                )
                .unwrap(),
            vec![vec![key.clone()]]
        );
        other
            .prepare("UPDATE t SET v='updated' WHERE k=?1")
            .unwrap()
            .execute(std::slice::from_ref(&key))
            .unwrap();
        assert_eq!(
            conn.query_all(
                "SELECT v FROM t WHERE rowid=?1",
                std::slice::from_ref(&rowid)
            )
            .unwrap(),
            vec![vec![SqlValue::Text("updated".into())]]
        );
        other
            .prepare("DELETE FROM t WHERE k=?1")
            .unwrap()
            .execute(&[key])
            .unwrap();
        assert!(
            conn.query_all("SELECT k FROM t WHERE rowid=?1", &[rowid])
                .unwrap()
                .is_empty()
        );
    }
    assert_eq!(
        conn.query_all("SELECT k FROM t ORDER BY k", &[]).unwrap(),
        vec![vec![SqlValue::Integer(1)], vec![SqlValue::Integer(3)]]
    );
}

#[test]
fn synthetic_rowids_and_integer_values_share_one_identity_space() {
    let dir = TempTestDir::new("integer_rowid_reservations");
    let fs = Arc::new(OsFs::open(&dir.path, true).unwrap());
    let engine = Arc::new(RwLock::new(
        StorageEngine::open(fs, EngineConfig::default()).unwrap(),
    ));
    let conn = connect_integer_table(engine);
    let key = SqlValue::Blob(COLLISION_HIGH.to_vec());
    let rowid = hash_pk_to_rowid(&key.to_key_bytes());
    assert!(rowid > 1 && rowid < i64::MAX - 2);
    conn.prepare("INSERT INTO t VALUES (?1,'synthetic')")
        .unwrap()
        .execute(std::slice::from_ref(&key))
        .unwrap();
    let mut insert = conn.prepare("INSERT INTO t VALUES (?1,'integer')").unwrap();
    assert!(insert.execute(&[SqlValue::Integer(rowid)]).is_err());
    conn.prepare("INSERT OR IGNORE INTO t VALUES (?1,'ignored')")
        .unwrap()
        .execute(&[SqlValue::Integer(rowid)])
        .unwrap();
    insert.execute(&[SqlValue::Integer(rowid - 1)]).unwrap();
    conn.execute_batch("INSERT INTO t VALUES (NULL,'automatic')")
        .unwrap();
    assert_eq!(
        conn.query_all("SELECT k FROM t WHERE v='automatic'", &[])
            .unwrap(),
        vec![vec![SqlValue::Integer(rowid + 1)]]
    );
    assert_eq!(
        conn.query_all(
            "SELECT v FROM t WHERE rowid=?1",
            &[SqlValue::Integer(rowid)]
        )
        .unwrap(),
        vec![vec![SqlValue::Text("synthetic".into())]]
    );
    conn.prepare("INSERT OR REPLACE INTO t VALUES (?1,'replacement')")
        .unwrap()
        .execute(&[SqlValue::Integer(rowid)])
        .unwrap();
    assert_eq!(
        conn.query_all(
            "SELECT k,v FROM t WHERE rowid=?1",
            &[SqlValue::Integer(rowid)]
        )
        .unwrap(),
        vec![vec![
            SqlValue::Integer(rowid),
            SqlValue::Text("replacement".into())
        ]]
    );
    assert!(
        conn.query_all("SELECT k FROM t WHERE k=?1", std::slice::from_ref(&key))
            .unwrap()
            .is_empty()
    );

    // When an integer already owns the first candidate, allocate another identity for the blob.
    conn.prepare("INSERT INTO t VALUES (?1,'synthetic-again')")
        .unwrap()
        .execute(std::slice::from_ref(&key))
        .unwrap();
    let actual = conn
        .query_all("SELECT rowid FROM t WHERE k=?1", &[key])
        .unwrap();
    assert_ne!(actual, vec![vec![SqlValue::Integer(rowid)]]);
    assert_ne!(actual, vec![vec![SqlValue::Integer(rowid + 1)]]);
}

#[test]
fn legacy_mixed_integer_table_migrates_nonintegers_without_reusing_integer_rowids() {
    let dir = TempTestDir::new("legacy_mixed_rowid_migration");
    let fs = Arc::new(OsFs::open(&dir.path, true).unwrap());
    let mut engine = StorageEngine::open(fs, EngineConfig::default()).unwrap();
    let text = SqlValue::Text("legacy".into());
    let rowid = hash_pk_to_rowid(&text.to_key_bytes());
    for key in [SqlValue::Integer(rowid), text.clone()] {
        let pk = key.to_key_bytes();
        let row = encode_row(&[key, SqlValue::Text("legacy".into())]);
        engine
            .put_row(1, &pk, &row, &[], WriteDurability::BufferedFast)
            .unwrap();
    }
    let conn = connect_integer_table(Arc::new(RwLock::new(engine)));
    assert_eq!(
        conn.query_all(
            "SELECT k FROM t WHERE rowid=?1",
            &[SqlValue::Integer(rowid)]
        )
        .unwrap(),
        vec![vec![SqlValue::Integer(rowid)]]
    );
    let text_id = conn
        .query_all("SELECT rowid FROM t WHERE k=?1", &[text])
        .unwrap();
    assert_ne!(text_id, vec![vec![SqlValue::Integer(rowid)]]);
}
