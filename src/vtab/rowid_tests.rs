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
