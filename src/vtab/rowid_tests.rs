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

/// A schema file that already declares `t1` and `legacy`, plus an engine whose `legacy` table
/// (id 2) holds rows written before persistent rowid mappings existed. Connections opened on the
/// schema file connect `legacy` lazily, so the connect (and its migration) can land inside a
/// user transaction.
struct LegacyFixture {
    _schema_dir: TempTestDir,
    _engine_dir: TempTestDir,
    schema_path: std::path::PathBuf,
    engine: Arc<RwLock<StorageEngine<OsFs>>>,
}

fn legacy_fixture(name: &str) -> LegacyFixture {
    let schema_dir = TempTestDir::new(&format!("{name}_schema"));
    let schema_path = schema_dir.path.join("schema.db");
    let columns = "columns='k TEXT PRIMARY KEY, v TEXT'";
    {
        let scratch_dir = TempTestDir::new(&format!("{name}_scratch"));
        let fs = Arc::new(OsFs::open(&scratch_dir.path, true).unwrap());
        let scratch = Arc::new(RwLock::new(
            StorageEngine::open(fs, EngineConfig::default()).unwrap(),
        ));
        let conn = SqliteConnection::open_file(&schema_path).unwrap();
        conn.register_aydb_module_with_options(scratch, false)
            .unwrap();
        conn.execute_batch(&format!(
            "CREATE VIRTUAL TABLE t1 USING aydb(table_id=1, {columns});
             CREATE VIRTUAL TABLE legacy USING aydb(table_id=2, {columns})"
        ))
        .unwrap();
    }
    let engine_dir = TempTestDir::new(&format!("{name}_engine"));
    let fs = Arc::new(OsFs::open(&engine_dir.path, true).unwrap());
    let mut engine = StorageEngine::open(fs, EngineConfig::default()).unwrap();
    for key in ["a", "b"] {
        let key = SqlValue::Text(key.into());
        let pk = key.to_key_bytes();
        let row = encode_row(&[key, SqlValue::Text("v".into())]);
        engine
            .put_row(2, &pk, &row, &[], WriteDurability::BufferedFast)
            .unwrap();
    }
    LegacyFixture {
        _schema_dir: schema_dir,
        _engine_dir: engine_dir,
        schema_path,
        engine: Arc::new(RwLock::new(engine)),
    }
}

impl LegacyFixture {
    fn open(&self) -> SqliteConnection {
        let conn = SqliteConnection::open_file(&self.schema_path).unwrap();
        conn.register_aydb_module_with_options(self.engine.clone(), false)
            .unwrap();
        conn
    }

    fn migrated(&self) -> bool {
        self.engine.read().unwrap().rowids_initialized(2).unwrap()
    }
}

fn text_rows(conn: &SqliteConnection, sql: &str) -> Vec<Vec<SqlValue>> {
    conn.query_all(sql, &[]).unwrap()
}

fn assert_two_distinct_rowids(conn: &SqliteConnection) {
    let rows = text_rows(conn, "SELECT rowid,k FROM legacy ORDER BY k");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][1], SqlValue::Text("a".into()));
    assert_eq!(rows[1][1], SqlValue::Text("b".into()));
    assert_ne!(rows[0][0], rows[1][0]);
}

/// Connects `legacy` for the first time inside a user transaction (migrating it there), then
/// rolls the transaction back.
fn connect_legacy_in_rolled_back_tx(fixture: &LegacyFixture) -> SqliteConnection {
    let conn = fixture.open();
    conn.execute_batch("BEGIN; INSERT INTO t1 VALUES('x','y')")
        .unwrap();
    assert_eq!(text_rows(&conn, "SELECT k FROM legacy").len(), 2);
    assert!(fixture.migrated(), "migration is visible to its own tx");
    conn.execute_batch("ROLLBACK").unwrap();
    assert!(!fixture.migrated(), "rollback must undo the migration");
    conn
}

#[test]
fn legacy_migration_rolled_back_with_the_user_tx_is_redone_for_rowid_select() {
    let fixture = legacy_fixture("migration_rollback_select");
    let conn = connect_legacy_in_rolled_back_tx(&fixture);
    assert_two_distinct_rowids(&conn);
    assert!(fixture.migrated());
    let other = fixture.open();
    assert_two_distinct_rowids(&other);
}

#[test]
fn legacy_migration_rolled_back_with_the_user_tx_is_redone_for_update() {
    let fixture = legacy_fixture("migration_rollback_update");
    let conn = connect_legacy_in_rolled_back_tx(&fixture);
    conn.execute_batch("UPDATE legacy SET v='n'").unwrap();
    assert_eq!(
        text_rows(&conn, "SELECT k,v FROM legacy ORDER BY k"),
        vec![
            vec![SqlValue::Text("a".into()), SqlValue::Text("n".into())],
            vec![SqlValue::Text("b".into()), SqlValue::Text("n".into())],
        ]
    );
    assert_two_distinct_rowids(&conn);
}

#[test]
fn legacy_migration_rolled_back_with_the_user_tx_is_redone_for_delete() {
    let fixture = legacy_fixture("migration_rollback_delete");
    let conn = connect_legacy_in_rolled_back_tx(&fixture);
    conn.execute_batch("DELETE FROM legacy WHERE k='a'")
        .unwrap();
    assert_eq!(
        text_rows(&conn, "SELECT k FROM legacy"),
        vec![vec![SqlValue::Text("b".into())]]
    );
    assert!(fixture.migrated());
}

#[test]
fn legacy_migration_rolled_back_to_a_savepoint_is_redone() {
    let fixture = legacy_fixture("migration_rollback_savepoint");
    let conn = fixture.open();
    conn.execute_batch("BEGIN; INSERT INTO t1 VALUES('x','y'); SAVEPOINT s")
        .unwrap();
    assert_eq!(text_rows(&conn, "SELECT k FROM legacy").len(), 2);
    assert!(fixture.migrated());
    conn.execute_batch("ROLLBACK TO s").unwrap();
    assert!(!fixture.migrated(), "rollback to s must undo the migration");
    assert_two_distinct_rowids(&conn);
    conn.execute_batch("UPDATE legacy SET v='n'; DELETE FROM legacy WHERE k='a'")
        .unwrap();
    conn.execute_batch("RELEASE s; COMMIT").unwrap();
    assert!(fixture.migrated());
    assert_eq!(
        text_rows(&conn, "SELECT k,v FROM legacy"),
        vec![vec![SqlValue::Text("b".into()), SqlValue::Text("n".into())]]
    );
    assert_eq!(
        text_rows(&fixture.open(), "SELECT k,v FROM legacy"),
        vec![vec![SqlValue::Text("b".into()), SqlValue::Text("n".into())]]
    );
}

#[test]
fn legacy_migration_rolled_back_by_one_connection_is_redone_for_a_second_connection() {
    let fixture = legacy_fixture("migration_rollback_two_connections");
    let first = fixture.open();
    let second = fixture.open();
    first
        .execute_batch("BEGIN; INSERT INTO t1 VALUES('x','y')")
        .unwrap();
    assert_eq!(text_rows(&first, "SELECT k FROM legacy").len(), 2);
    // The second connection cannot join the first one's transaction, and must not be told that
    // the uncommitted migration is complete.
    assert!(second.query_all("SELECT rowid,k FROM legacy", &[]).is_err());
    first.execute_batch("ROLLBACK").unwrap();
    assert!(!fixture.migrated());
    assert_two_distinct_rowids(&second);
    assert_two_distinct_rowids(&first);
    assert_eq!(
        text_rows(&first, "SELECT rowid FROM legacy ORDER BY k"),
        text_rows(&second, "SELECT rowid FROM legacy ORDER BY k"),
    );
}

#[test]
fn legacy_migration_committed_with_the_user_tx_stays_complete() {
    let fixture = legacy_fixture("migration_commit_control");
    let conn = fixture.open();
    conn.execute_batch("BEGIN; INSERT INTO t1 VALUES('x','y')")
        .unwrap();
    assert_eq!(text_rows(&conn, "SELECT k FROM legacy").len(), 2);
    conn.execute_batch("COMMIT").unwrap();
    assert!(fixture.migrated());
    assert_two_distinct_rowids(&conn);
    conn.execute_batch("UPDATE legacy SET v='n'; DELETE FROM legacy WHERE k='a'")
        .unwrap();
    assert_eq!(
        text_rows(&fixture.open(), "SELECT k,v FROM legacy"),
        vec![vec![SqlValue::Text("b".into()), SqlValue::Text("n".into())]]
    );
}

#[test]
fn committed_marker_latches_the_vtab_and_later_scans_do_not_recheck_it() {
    let fixture = legacy_fixture("migration_latch");
    let conn = fixture.open();
    // Connecting outside a transaction migrates and commits, which latches the vtab.
    assert_two_distinct_rowids(&conn);
    assert!(fixture.migrated());
    // Losing the marker behind the latched vtab's back is not noticed: no scan or write looks
    // at the marker again, nor re-runs the migration.
    fixture
        .engine
        .write()
        .unwrap()
        .debug_remove_rowid_marker(2)
        .unwrap();
    assert!(!fixture.migrated());
    assert_two_distinct_rowids(&conn);
    conn.execute_batch("UPDATE legacy SET v='n'").unwrap();
    assert!(!fixture.migrated(), "a latched vtab must not re-check");
    // A vtab that has not seen a committed marker does repair it.
    let fresh = fixture.open();
    assert_two_distinct_rowids(&fresh);
    assert!(fixture.migrated());
}

#[test]
fn marker_visible_only_in_the_open_tx_does_not_latch_a_vtab_that_scanned_it() {
    let fixture = legacy_fixture("migration_no_latch_in_tx");
    let conn = fixture.open();
    conn.execute_batch("BEGIN; INSERT INTO t1 VALUES('x','y')")
        .unwrap();
    // Connects and scans inside the transaction, so the marker exists only in its overlay.
    assert_two_distinct_rowids(&conn);
    assert!(fixture.migrated());
    conn.execute_batch("ROLLBACK").unwrap();
    assert!(!fixture.migrated());
    // The same vtab instance scanned before the rollback and must still repair the mappings.
    assert_two_distinct_rowids(&conn);
    assert!(fixture.migrated());
}

/// Counts every device sync (`sync_barrier` and `sync_durable`) issued through the filesystem.
mod sync_counting {
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

fn put_legacy_text_rows<FS: crate::pal::PlatformFs>(
    engine: &mut StorageEngine<FS>,
    count: usize,
    durability: WriteDurability,
) {
    for i in 0..count {
        let key = SqlValue::Text(format!("key{i:05}"));
        let pk = key.to_key_bytes();
        let row = encode_row(&[key, SqlValue::Text("v".into())]);
        engine.put_row(1, &pk, &row, &[], durability).unwrap();
    }
}

/// Number of device syncs one `ensure_persistent_rowids` run issues on a legacy table of `rows`
/// rows.
fn migration_syncs(name: &str, rows: usize, durability: WriteDurability) -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    let dir = TempTestDir::new(name);
    let syncs = Arc::new(AtomicU64::new(0));
    let fs = Arc::new(sync_counting::CountFs {
        inner: OsFs::open(&dir.path, true).unwrap(),
        syncs: Arc::clone(&syncs),
    });
    let mut engine = StorageEngine::open(fs, EngineConfig::default()).unwrap();
    put_legacy_text_rows(&mut engine, rows, WriteDurability::BufferedFast);
    engine.checkpoint().unwrap();
    let before = syncs.load(Ordering::SeqCst);
    rowid_map::ensure_persistent_rowids(&mut engine, 1, durability, false).unwrap();
    let used = syncs.load(Ordering::SeqCst) - before;
    assert!(engine.rowids_initialized(1).unwrap());
    used
}

#[test]
fn synchronous_migration_syncs_once_regardless_of_row_count() {
    let sync = WriteDurability::SyncTransactional;
    let one = migration_syncs("migration_syncs_one", 1, sync);
    let many = migration_syncs("migration_syncs_many", 300, sync);
    assert!(one > 0, "a synchronous migration must reach the device");
    assert_eq!(
        many, one,
        "device syncs must not grow with the number of migrated rows"
    );
    // Buffered tables never wait for the device.
    assert_eq!(
        migration_syncs(
            "migration_syncs_buffered",
            300,
            WriteDurability::BufferedFast
        ),
        0
    );
}
