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

fn connect_integer_table(engine: Arc<RwLock<StorageEngine<OsFs>>>) -> SqliteConnection {
    let conn = SqliteConnection::open_in_memory().unwrap();
    conn.register_aydb_module_with_options(engine, false)
        .unwrap();
    conn.execute_batch(
        "CREATE VIRTUAL TABLE t USING aydb(table_id=1, columns='k INTEGER PRIMARY KEY, v TEXT')",
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

/// Counts every device sync (`sync_barrier` and `sync_durable`) issued through the filesystem.
pub(super) mod sync_counting {
    use crate::pal::{OpenHint, PlatformFile, PlatformFs, PreallocStrategy};
    use std::io;
    use std::path::Path;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    pub struct CountFile<F> {
        inner: Arc<F>,
        syncs: Arc<AtomicU64>,
    }
    impl<F: PlatformFile> PlatformFile for CountFile<F> {
        fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
            self.inner.read_at(buf, offset)
        }
        fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
            self.inner.read_exact_at(buf, offset)
        }
        fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
            self.inner.write_all_at(buf, offset)
        }
        fn file_len(&self) -> io::Result<u64> {
            self.inner.file_len()
        }
        fn preallocate(&self, target_len: u64, strategy: PreallocStrategy) -> io::Result<()> {
            self.inner.preallocate(target_len, strategy)
        }
        fn sync_barrier(&self) -> io::Result<()> {
            self.syncs.fetch_add(1, Ordering::SeqCst);
            self.inner.sync_barrier()
        }
        fn sync_durable(&self) -> io::Result<()> {
            self.syncs.fetch_add(1, Ordering::SeqCst);
            self.inner.sync_durable()
        }
        fn evict_os_cache(&self, offset: u64, len: u64) -> io::Result<()> {
            self.inner.evict_os_cache(offset, len)
        }
    }
    pub struct CountFs<S: PlatformFs> {
        pub inner: S,
        pub syncs: Arc<AtomicU64>,
    }
    impl<S: PlatformFs> PlatformFs for CountFs<S> {
        type File = CountFile<S::File>;
        fn open_file(
            &self,
            rel_name: &str,
            create: bool,
            writable: bool,
            hint: OpenHint,
        ) -> io::Result<Arc<Self::File>> {
            Ok(Arc::new(CountFile {
                inner: self.inner.open_file(rel_name, create, writable, hint)?,
                syncs: Arc::clone(&self.syncs),
            }))
        }
        fn remove_file(&self, rel_name: &str) -> io::Result<()> {
            self.inner.remove_file(rel_name)
        }
        fn list_files(&self) -> io::Result<Vec<String>> {
            self.inner.list_files()
        }
        fn root_path(&self) -> &Path {
            self.inner.root_path()
        }
    }
}

/// A table written before persistent rowid mappings existed has no marker. It is not migrated:
/// connecting it fails with a defined error, whatever else the connection is doing.
fn table_without_rowid_marker(name: &str) -> (super::rename_tests::Fixture, Vec<i64>) {
    let fixture = super::rename_tests::Fixture::new(name);
    let conn = fixture.open();
    conn.execute_batch(
        "CREATE VIRTUAL TABLE good USING aydb(table_id=1, columns='k TEXT PRIMARY KEY, v TEXT');
         CREATE VIRTUAL TABLE old USING aydb(table_id=2, columns='k TEXT PRIMARY KEY, v TEXT');
         INSERT INTO good VALUES ('g', 'x');
         INSERT INTO old VALUES ('a', 'x'), ('b', 'y')",
    )
    .unwrap();
    drop(conn);
    fixture
        .engine()
        .write()
        .unwrap()
        .debug_remove_rowid_marker(2)
        .unwrap();
    (fixture, vec![1, 2])
}

#[test]
fn a_table_without_a_rowid_marker_is_refused_at_connect_with_a_message() {
    let (fixture, _) = table_without_rowid_marker("rowid_marker_missing");
    let conn = fixture.open();
    // The other tables of the schema are unaffected.
    assert_eq!(
        conn.query_all("SELECT k FROM good", &[]).unwrap(),
        vec![vec![SqlValue::Text("g".into())]]
    );
    for sql in [
        "SELECT * FROM old",
        "SELECT rowid FROM old",
        "INSERT INTO old VALUES ('c', 'z')",
        "UPDATE old SET v = 'n'",
        "DELETE FROM old",
    ] {
        let err = conn
            .query_all(sql, &[])
            .map(|_| ())
            .or_else(|_| conn.execute_batch(sql));
        let message = err.unwrap_err().to_string();
        assert!(
            message.contains("no persistent rowid mapping")
                || message.contains("constructor failed"),
            "{sql}: {message}"
        );
    }
    // Nothing was repaired behind the user's back: the marker is still absent.
    assert!(
        !fixture
            .engine()
            .read()
            .unwrap()
            .rowids_initialized(2)
            .unwrap()
    );
}

/// A refused table inside a read snapshot neither corrupts the snapshot nor fails the commit.
#[test]
fn a_refused_table_does_not_break_a_read_snapshot_or_its_commit() {
    let (fixture, _) = table_without_rowid_marker("rowid_marker_missing_snapshot");
    let conn = fixture.open();
    conn.execute_batch("BEGIN").unwrap();
    assert_eq!(conn.query_all("SELECT k FROM good", &[]).unwrap().len(), 1);
    assert!(conn.query_all("SELECT * FROM old", &[]).is_err());
    assert_eq!(conn.query_all("SELECT k FROM good", &[]).unwrap().len(), 1);
    conn.execute_batch("COMMIT").unwrap();
}

/// On a poisoned engine a scan of a current table needs no write: there is no migration left
/// that a read could trigger.
#[test]
fn scans_of_a_current_table_on_a_poisoned_engine_need_no_write() {
    let fixture = super::rename_tests::Fixture::new("rowid_scan_poisoned");
    let conn = fixture.open();
    conn.execute_batch(
        "CREATE VIRTUAL TABLE t USING aydb(columns='k TEXT PRIMARY KEY, v TEXT');
         INSERT INTO t VALUES ('a', 'x'), ('b', 'y')",
    )
    .unwrap();
    drop(conn);
    let conn = fixture.open();
    // Connected before the engine is poisoned (a connect needs a healthy engine).
    conn.query_all("SELECT count(*) FROM t", &[]).unwrap();
    fixture.engine().write().unwrap().debug_poison("simulated");
    assert_eq!(
        conn.query_all("SELECT rowid, k FROM t ORDER BY k", &[])
            .unwrap()
            .len(),
        2
    );
}
