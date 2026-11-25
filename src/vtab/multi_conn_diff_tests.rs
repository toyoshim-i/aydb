//! Seeded multi-connection differential oracle comparing `aydb` virtual tables against
//! native SQLite tables inside the same database file across interleaved transactions,
//! savepoints, `ALTER TABLE RENAME TO`, temporary virtual-table savepoints, cross-connection
//! `UNIQUE` marker invalidation (`R3-H3`), and engine flush/checkpoint/compaction.
//!
//! Environment variables (matching [`crate::engine::crash_tests`]):
//! - `AYDB_MULTI_CONN_SEEDS=<n>`: number of seeds to run (default `20`).
//! - `AYDB_MULTI_CONN_SEED_START=<n>`: first seed (default `0`).
//! - `AYDB_MULTI_CONN_SEED=<n>`: replay one specific seed with verbose history on failure.
//! - `AYDB_EXPORT_TRACES_DIR=<dir>`: when set and non-empty during `export_formal_traces*`,
//!   writes `tx_fsm_multi_conn_diff.ndjson` verified by Lean 4 `aydb-trace-check`.

use std::collections::{BTreeMap, BTreeSet};

use super::unique_key::{canon_unique_key, unique_index_component};
use super::*;
use crate::engine::{EngineConfig, ReadView};
use crate::index::{NS_ROW_META, encode_row_meta_key, encode_secondary_index_key};
use crate::util::test_util::TempTestDir;

/// SplitMix64: deterministic across all platforms without external crates.
#[derive(Clone)]
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xD1B5_4A32_D192_ED03)
    }

    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn chance(&mut self, num: u64, den: u64) -> bool {
        self.below(den) < num
    }
}

fn read_env_u64(name: &str) -> Option<u64> {
    std::env::var(name).ok().and_then(|s| s.parse().ok())
}

fn seed_range() -> (u64, u64) {
    if let Some(one) = read_env_u64("AYDB_MULTI_CONN_SEED") {
        return (one, 1);
    }
    let start = read_env_u64("AYDB_MULTI_CONN_SEED_START").unwrap_or(0);
    let count = read_env_u64("AYDB_MULTI_CONN_SEEDS").unwrap_or(20);
    (start, count)
}

#[derive(Clone, Copy)]
enum TableSlot {
    T1,
    T2,
    UniqueU,
}

#[derive(Clone, Debug)]
struct TxTracker {
    conn_idx: usize,
    savepoints: Vec<(String, String)>,
    has_aydb_write: bool,
}

struct MultiConnHarness {
    engine: Arc<RwLock<StorageEngine<OsFs>>>,
    conns: [SqliteConnection; 2],
    t1_committed: String,
    t1_cur: String,
    tx: Option<TxTracker>,
    sp_counter: u32,
    ext_counter: u64,
    history: Vec<String>,
    label: String,
    // Drop TempTestDir last so connections and the engine close their files first.
    _dir: TempTestDir,
}

impl MultiConnHarness {
    fn new(seed: u64, label: &str) -> Self {
        let dir = TempTestDir::new(&format!("m5_multi_{label}_{seed}"));
        let eng_dir = dir.path.join("engine");
        std::fs::create_dir_all(&eng_dir).unwrap();
        let fs = Arc::new(OsFs::open(&eng_dir, true).unwrap());
        let mut cfg = EngineConfig::default();
        cfg.sync_wal_size_bytes = 64 * 4096;
        cfg.buf_wal_size_bytes = 64 * 4096;
        cfg.max_transaction_bytes = 64 * 1024;
        cfg.auto_compact_segment_threshold = 2;
        let bg_compaction = seed % 2 == 1;
        let engine = Arc::new(RwLock::new(StorageEngine::open(fs, cfg).unwrap()));

        let db_path = dir.path.join("sqlite.db");
        let open_conn = || {
            let conn = SqliteConnection::open_file(&db_path).unwrap();
            conn.execute_batch("PRAGMA busy_timeout = 0;").unwrap();
            conn.register_aydb_module_with_options(Arc::clone(&engine), bg_compaction)
                .unwrap();
            conn
        };
        let conn0 = open_conn();
        let conn1 = open_conn();

        conn0
            .execute_batch(
                "CREATE TABLE n_t1(k TEXT PRIMARY KEY, v TEXT);
                 CREATE VIRTUAL TABLE a_t1 USING aydb(table_id=1, durability='buffered', columns='k TEXT PRIMARY KEY, v TEXT');
                 CREATE TABLE n_t2(k TEXT PRIMARY KEY, v TEXT);
                 CREATE VIRTUAL TABLE a_t2 USING aydb(table_id=2, durability='sync', columns='k TEXT PRIMARY KEY, v TEXT');
                 CREATE TABLE n_u(k TEXT PRIMARY KEY, u TEXT UNIQUE, n TEXT UNIQUE COLLATE NOCASE, c TEXT);
                 CREATE VIRTUAL TABLE a_u USING aydb(table_id=3, durability='buffered', columns='k TEXT PRIMARY KEY, u TEXT UNIQUE, n TEXT UNIQUE COLLATE NOCASE, c TEXT', indexes='c');",
            )
            .unwrap();

        Self {
            engine,
            conns: [conn0, conn1],
            t1_committed: String::from("t1"),
            t1_cur: String::from("t1"),
            tx: None,
            sp_counter: 0,
            ext_counter: 0,
            history: Vec::new(),
            label: format!("{label} seed={seed}"),
            _dir: dir,
        }
    }

    fn t1_active_name(&self) -> &str {
        if self.tx.is_some() {
            &self.t1_cur
        } else {
            &self.t1_committed
        }
    }

    fn active_conn(&self, rng: &mut Rng) -> usize {
        match &self.tx {
            Some(tx) => tx.conn_idx,
            None => rng.below(2) as usize,
        }
    }

    fn mirror_dml(&mut self, conn_idx: usize, slot: TableSlot, sql_template: &str) {
        let (native_name, aydb_name) = match slot {
            TableSlot::T1 => {
                let s = self.t1_active_name().to_string();
                (format!("n_{s}"), format!("a_{s}"))
            }
            TableSlot::T2 => (String::from("n_t2"), String::from("a_t2")),
            TableSlot::UniqueU => (String::from("n_u"), String::from("a_u")),
        };
        let n_sql = sql_template.replace("{T}", &native_name);
        let a_sql = sql_template.replace("{T}", &aydb_name);
        self.history.push(format!("[c{conn_idx}] {a_sql}"));

        let n_res = self.conns[conn_idx].execute_batch(&n_sql);
        let a_res = self.conns[conn_idx].execute_batch(&a_sql);
        assert_eq!(
            n_res.is_ok(),
            a_res.is_ok(),
            "{}: native vs aydb outcome mismatch on `{a_sql}`\n native={n_res:?}\n aydb={a_res:?}\n history={:?}",
            self.label,
            self.history
        );
        if a_res.is_ok() {
            if let Some(tx) = &mut self.tx {
                tx.has_aydb_write = true;
            }
        }
    }

    fn assert_tables_match_on_conn(&self, conn_idx: usize, context: &str) {
        let c = &self.conns[conn_idx];
        let t1 = if self.tx.as_ref().is_some_and(|t| t.conn_idx == conn_idx) {
            self.t1_cur.as_str()
        } else {
            self.t1_committed.as_str()
        };
        for (n_tbl, a_tbl, cols) in [
            (format!("n_{t1}"), format!("a_{t1}"), "k, v"),
            (String::from("n_t2"), String::from("a_t2"), "k, v"),
            (String::from("n_u"), String::from("a_u"), "k, u, n, c"),
        ] {
            let n_rows = c
                .query_all(&format!("SELECT {cols} FROM {n_tbl} ORDER BY k"), &[])
                .unwrap();
            let a_rows = c
                .query_all(&format!("SELECT {cols} FROM {a_tbl} ORDER BY k"), &[])
                .unwrap();
            assert_eq!(
                n_rows, a_rows,
                "{}: conn{conn_idx} {a_tbl} vs {n_tbl} diverged ({context})\n history={:?}",
                self.label, self.history
            );
        }
    }

    fn assert_unique_engine_index_exact(&self, context: &str) {
        let marker_present = self
            .engine
            .read()
            .unwrap()
            .unique_marker_view(ReadView::Committed, 3)
            .unwrap()
            .is_some();
        if !marker_present {
            return;
        }
        let rows = self.conns[0]
            .query_all("SELECT k, u, n, c FROM a_u ORDER BY k", &[])
            .unwrap();
        let mut want_i = BTreeSet::new();
        let mut want_t = BTreeMap::new();
        for row in &rows {
            let pk = row[0].to_key_bytes();
            let mut pairs = Vec::new();
            pairs.push((1u16, row[3].to_key_bytes()));
            for (col_idx, iid, fold) in [(1usize, 0x8001u16, false), (2usize, 0x8002u16, true)] {
                let raw = match &row[col_idx] {
                    SqlValue::Null => continue,
                    SqlValue::Text(s) if fold => {
                        let mut b = vec![row_codec::KEY_TAG_TEXT];
                        b.extend(s.bytes().map(|ch| ch.to_ascii_lowercase()));
                        b
                    }
                    other => other.to_key_bytes(),
                };
                pairs.push((iid, unique_index_component(&canon_unique_key(&raw))));
            }
            for (iid, col_key) in &pairs {
                want_i.insert(encode_secondary_index_key(3, *iid, col_key, &pk));
            }
            want_t.insert(pk, pairs);
        }

        let mut eng = self.engine.write().unwrap();
        let got_i: BTreeSet<Vec<u8>> = eng.test_live_entry_keys(3).unwrap().into_iter().collect();
        assert_eq!(
            got_i, want_i,
            "{}: secondary 'i' entries mismatch ({context})\n history={:?}",
            self.label, self.history
        );
        let got_t: BTreeSet<Vec<u8>> = eng
            .test_live_keys(3, NS_ROW_META)
            .unwrap()
            .into_iter()
            .collect();
        let want_t_keys: BTreeSet<Vec<u8>> =
            want_t.keys().map(|pk| encode_row_meta_key(3, pk)).collect();
        assert_eq!(
            got_t, want_t_keys,
            "{}: 't' row-meta keys mismatch ({context})\n history={:?}",
            self.label, self.history
        );
        for (pk, pairs) in &want_t {
            let meta = eng.test_row_meta(3, pk).unwrap().unwrap();
            assert_eq!(
                &meta.secondary_keys, pairs,
                "{}: 't' secondary pairs mismatch ({context})",
                self.label
            );
        }
    }

    fn verify_settled(&self, context: &str) {
        assert!(self.tx.is_none());
        assert!(
            !self.engine.read().unwrap().is_poisoned(),
            "{}: engine unexpectedly poisoned ({context})\n history={:?}",
            self.label,
            self.history
        );
        self.assert_tables_match_on_conn(0, context);
        self.assert_tables_match_on_conn(1, context);
        self.assert_unique_engine_index_exact(context);
    }

    fn random_dml_sql(rng: &mut Rng, slot: TableSlot) -> String {
        let k_idx = rng.below(8);
        let val_tag = rng.next() & 0xFFFF;
        match slot {
            TableSlot::T1 | TableSlot::T2 => match rng.below(5) {
                0 => format!("INSERT INTO {{T}} VALUES ('k{k_idx}', 'v{val_tag}')"),
                1 => format!("INSERT OR REPLACE INTO {{T}} VALUES ('k{k_idx}', 'rep{val_tag}')"),
                2 => format!("INSERT OR IGNORE INTO {{T}} VALUES ('k{k_idx}', 'ign{val_tag}')"),
                3 => {
                    let nk = rng.below(8);
                    format!("UPDATE {{T}} SET k = 'k{nk}', v = 'upd{val_tag}' WHERE k = 'k{k_idx}'")
                }
                _ => format!("DELETE FROM {{T}} WHERE k = 'k{k_idx}'"),
            },
            TableSlot::UniqueU => {
                let u_sql = if rng.chance(1, 5) {
                    String::from("NULL")
                } else {
                    format!("'u{}'", rng.below(7))
                };
                let n_sql = if rng.chance(1, 5) {
                    String::from("NULL")
                } else if rng.chance(1, 2) {
                    format!("'n{}'", rng.below(7))
                } else {
                    format!("'N{}'", rng.below(7))
                };
                let c_sql = if rng.chance(1, 4) {
                    String::from("NULL")
                } else {
                    format!("'c{}'", rng.below(4))
                };
                match rng.below(5) {
                    0 => {
                        format!("INSERT INTO {{T}} VALUES ('k{k_idx}', {u_sql}, {n_sql}, {c_sql})")
                    }
                    1 => format!(
                        "INSERT OR REPLACE INTO {{T}} VALUES ('k{k_idx}', {u_sql}, {n_sql}, {c_sql})"
                    ),
                    2 => format!(
                        "INSERT OR IGNORE INTO {{T}} VALUES ('k{k_idx}', {u_sql}, {n_sql}, {c_sql})"
                    ),
                    3 => format!("UPDATE {{T}} SET u = {u_sql}, c = {c_sql} WHERE k = 'k{k_idx}'"),
                    _ => format!("DELETE FROM {{T}} WHERE k = 'k{k_idx}'"),
                }
            }
        }
    }

    fn begin_tx(&mut self, conn_idx: usize) {
        assert!(self.tx.is_none());
        self.history.push(format!("[c{conn_idx}] BEGIN"));
        self.conns[conn_idx].execute_batch("BEGIN").unwrap();
        self.t1_cur = self.t1_committed.clone();
        self.tx = Some(TxTracker {
            conn_idx,
            savepoints: Vec::new(),
            has_aydb_write: false,
        });
    }

    fn commit_or_rollback(&mut self, commit: bool) {
        let Some(tx) = self.tx.take() else {
            return;
        };
        let cmd = if commit { "COMMIT" } else { "ROLLBACK" };
        self.history.push(format!("[c{}] {cmd}", tx.conn_idx));
        self.conns[tx.conn_idx].execute_batch(cmd).unwrap();
        if commit {
            self.t1_committed = self.t1_cur.clone();
        } else {
            self.t1_cur = self.t1_committed.clone();
        }
        self.verify_settled(cmd);
    }

    fn run_cross_conn_contention_probe(&mut self, rng: &mut Rng) {
        let Some(tx) = self.tx.as_ref() else {
            return;
        };
        let owner = tx.conn_idx;
        let has_aydb_write = tx.has_aydb_write;
        let other = 1 - owner;

        // Idle connection always sees the committed snapshot of both native and aydb tables.
        self.history.push(format!("[c{other}] snapshot read check"));
        self.assert_tables_match_on_conn(other, "concurrent reader snapshot");

        // Once the active tx holds the engine write slot, another connection attempting to write
        // must fail with Busy while leaving the owner's transaction intact.
        if has_aydb_write && rng.chance(1, 2) {
            let probe = "INSERT INTO a_t2 VALUES ('busy_probe', 'never_committed')";
            self.history.push(format!("[c{other}] BUSY probe: {probe}"));
            assert!(
                self.conns[other].execute_batch(probe).is_err(),
                "{}: foreign write must fail with Busy while conn{owner} owns tx\n history={:?}",
                self.label,
                self.history
            );
        }
    }

    fn run_rename_step(&mut self) {
        let cur = self.t1_active_name().to_string();
        let next = if cur == "t1" { "t1_alt" } else { "t1" };

        if let Some(tx) = self.tx.as_mut() {
            // Ensure an aydb table write opened the vtab tx before issuing in-tx RENAME.
            if !tx.has_aydb_write {
                let c = tx.conn_idx;
                self.mirror_dml(
                    c,
                    TableSlot::T1,
                    "INSERT OR IGNORE INTO {T} VALUES ('k_r', 'anchor')",
                );
            }
            let c = self.tx.as_ref().unwrap().conn_idx;
            let sql = format!(
                "ALTER TABLE n_{cur} RENAME TO n_{next}; ALTER TABLE a_{cur} RENAME TO a_{next};"
            );
            self.history.push(format!("[c{c}] {sql}"));
            self.conns[c].execute_batch(&sql).unwrap();
            self.t1_cur = next.to_string();
            return;
        }

        let sql = format!(
            "ALTER TABLE n_{cur} RENAME TO n_{next}; ALTER TABLE a_{cur} RENAME TO a_{next};"
        );
        self.history.push(format!("[c0] {sql}"));
        self.conns[0].execute_batch(&sql).unwrap();
        self.t1_committed = next.to_string();
        self.t1_cur = self.t1_committed.clone();
        // Both connections must reject the old name and see matching rows under the new name.
        for conn in &self.conns {
            assert!(
                conn.query_all(&format!("SELECT k FROM a_{cur}"), &[])
                    .is_err()
            );
        }
        self.verify_settled("autocommit RENAME");
    }

    fn run_temp_vtab_savepoint_step(&mut self) {
        let (started_local, c) = match &self.tx {
            Some(tx) => (false, tx.conn_idx),
            None => {
                self.begin_tx(0);
                (true, 0)
            }
        };
        if !self.tx.as_ref().unwrap().has_aydb_write {
            self.mirror_dml(
                c,
                TableSlot::T1,
                "INSERT OR IGNORE INTO {T} VALUES ('k_tmp', 'base')",
            );
        }
        let sql = "SAVEPOINT tmp_ddl;
                   CREATE VIRTUAL TABLE tmp_sp USING aydb(table_id=4, columns='k INTEGER PRIMARY KEY, v TEXT');
                   INSERT INTO tmp_sp VALUES (1, 'ephemeral');
                   ROLLBACK TO tmp_ddl;
                   RELEASE tmp_ddl;";
        self.history
            .push(format!("[c{c}] temp vtab savepoint rollback"));
        self.conns[c].execute_batch(sql).unwrap();
        if started_local {
            self.commit_or_rollback(true);
        }
    }

    fn run_cross_conn_unique_invalidation(&mut self) {
        assert!(self.tx.is_none());
        self.ext_counter += 1;
        let seq = self.ext_counter;
        let ext_k = format!("ext_k_{seq}");
        let ext_u = format!("ext_u_{seq}");
        let ext_n = format!("ext_n_{seq}");

        // Prepare a query on conn0 first so it latches the current unique_ready_gen when present.
        let mut pre_stmt = self.conns[0]
            .prepare("SELECT k FROM a_u WHERE u = ?1 ORDER BY k")
            .unwrap();
        let before_pre = pre_stmt.query(&[SqlValue::Text(ext_u.clone())]).unwrap();
        assert!(before_pre.is_empty());

        let gen_before = self.engine.read().unwrap().unique_marker_gen();
        let conn3 = SqliteConnection::open_in_memory().unwrap();
        conn3
            .register_aydb_module_with_options(Arc::clone(&self.engine), false)
            .unwrap();
        let ddl3 = format!(
            "CREATE VIRTUAL TABLE a_u USING aydb(table_id=3, columns='k TEXT PRIMARY KEY, u TEXT, n TEXT, c TEXT', indexes='c');
             INSERT INTO a_u VALUES ('{ext_k}', '{ext_u}', '{ext_n}', 'c_ext');"
        );
        self.history
            .push(format!("[conn3 non-UNIQUE] INSERT {ext_k}/{ext_u}"));
        conn3.execute_batch(&ddl3).unwrap();
        drop(conn3);

        // Mirror the same disjoint row into native n_u so n_u stays identical to a_u.
        self.conns[0]
            .execute_batch(&format!(
                "INSERT INTO n_u VALUES ('{ext_k}', '{ext_u}', '{ext_n}', 'c_ext');"
            ))
            .unwrap();

        let (gen_after, marker_after) = {
            let eng = self.engine.read().unwrap();
            (
                eng.unique_marker_gen(),
                eng.unique_marker_view(ReadView::Committed, 3).unwrap(),
            )
        };
        assert!(gen_after > gen_before || marker_after.is_none());
        assert!(
            marker_after.is_none(),
            "{}: connecting without UNIQUE must clear persisted unique marker",
            self.label
        );

        // Pre-prepared query on conn0 must see conn3's row instead of trusting a stale latch.
        let after_pre = pre_stmt.query(&[SqlValue::Text(ext_u.clone())]).unwrap();
        assert_eq!(after_pre, vec![vec![SqlValue::Text(ext_k.clone())]]);
        drop(pre_stmt);

        // Duplicate UNIQUE insert on conn1 must still be rejected cleanly on both n_u and a_u.
        let dup_sql =
            format!("INSERT INTO {{T}} VALUES ('dup_{seq}', '{ext_u}', 'other_{seq}', 'c0')");
        self.mirror_dml(1, TableSlot::UniqueU, &dup_sql);
        self.verify_settled("cross-conn UNIQUE invalidation");
    }

    fn run_maintenance_step(&mut self, rng: &mut Rng) {
        if self.tx.is_none() && rng.chance(1, 2) {
            // Hold a read transaction on conn1 while flushing & compacting, verifying snapshot
            // isolation across maintenance work.
            self.history.push(String::from(
                "[c1 + maint] read snapshot across flush/compact",
            ));
            let before_n = self.conns[1]
                .query_all("SELECT k, v FROM n_t2 ORDER BY k", &[])
                .unwrap();
            self.conns[1].execute_batch("BEGIN").unwrap();
            let snap_a = self.conns[1]
                .query_all("SELECT k, v FROM a_t2 ORDER BY k", &[])
                .unwrap();
            assert_eq!(before_n, snap_a);
            {
                let mut eng = self.engine.write().unwrap();
                eng.flush_memtable().unwrap();
                eng.compact().unwrap();
            }
            let after_a = self.conns[1]
                .query_all("SELECT k, v FROM a_t2 ORDER BY k", &[])
                .unwrap();
            assert_eq!(snap_a, after_a);
            self.conns[1].execute_batch("COMMIT").unwrap();
            {
                let mut eng = self.engine.write().unwrap();
                eng.checkpoint().unwrap();
            }
            self.verify_settled("maintenance under reader snapshot");
            return;
        }

        let mut eng = self.engine.write().unwrap();
        match rng.below(3) {
            0 => {
                self.history.push(String::from("[maint] flush_memtable"));
                eng.flush_memtable().unwrap();
            }
            1 => {
                self.history.push(String::from("[maint] compact"));
                eng.compact().unwrap();
            }
            _ if !eng.in_active_transaction() => {
                self.history.push(String::from("[maint] checkpoint"));
                eng.checkpoint().unwrap();
            }
            _ => {
                self.history
                    .push(String::from("[maint] flush_memtable (in-tx defer)"));
                eng.flush_memtable().unwrap();
            }
        }
    }

    fn step(&mut self, rng: &mut Rng) {
        let roll = rng.below(100);
        if self.tx.is_none() {
            if roll < 22 {
                let c = rng.below(2) as usize;
                self.begin_tx(c);
            } else if roll < 72 {
                let c = rng.below(2) as usize;
                let slot = match rng.below(3) {
                    0 => TableSlot::T1,
                    1 => TableSlot::T2,
                    _ => TableSlot::UniqueU,
                };
                let sql = Self::random_dml_sql(rng, slot);
                self.mirror_dml(c, slot, &sql);
                self.verify_settled("autocommit DML");
            } else if roll < 82 {
                self.run_rename_step();
            } else if roll < 90 {
                self.run_temp_vtab_savepoint_step();
            } else if roll < 95 {
                self.run_cross_conn_unique_invalidation();
            } else {
                self.run_maintenance_step(rng);
            }
            return;
        }

        // Inside an active transaction on `tx.conn_idx`.
        let owner = self.tx.as_ref().unwrap().conn_idx;
        let sp_len = self.tx.as_ref().unwrap().savepoints.len();
        if roll < 50 {
            let slot = match rng.below(3) {
                0 => TableSlot::T1,
                1 => TableSlot::T2,
                _ => TableSlot::UniqueU,
            };
            let sql = Self::random_dml_sql(rng, slot);
            self.mirror_dml(owner, slot, &sql);
            self.assert_tables_match_on_conn(owner, "in-tx DML");
        } else if roll < 62 && sp_len < 3 {
            self.sp_counter += 1;
            let sp = format!("sp_{}", self.sp_counter);
            self.history.push(format!("[c{owner}] SAVEPOINT {sp}"));
            self.conns[owner]
                .execute_batch(&format!("SAVEPOINT {sp}"))
                .unwrap();
            let cur_name = self.t1_cur.clone();
            self.tx.as_mut().unwrap().savepoints.push((sp, cur_name));
        } else if roll < 70 && sp_len > 0 {
            let idx = rng.below(sp_len as u64) as usize;
            let (sp, saved_t1) = self.tx.as_ref().unwrap().savepoints[idx].clone();
            self.history.push(format!("[c{owner}] ROLLBACK TO {sp}"));
            self.conns[owner]
                .execute_batch(&format!("ROLLBACK TO {sp}"))
                .unwrap();
            self.t1_cur = saved_t1;
            self.tx.as_mut().unwrap().savepoints.truncate(idx + 1);
            self.assert_tables_match_on_conn(owner, "in-tx ROLLBACK TO");
        } else if roll < 76 && sp_len > 0 {
            let idx = rng.below(sp_len as u64) as usize;
            let sp = self.tx.as_ref().unwrap().savepoints[idx].0.clone();
            self.history.push(format!("[c{owner}] RELEASE {sp}"));
            self.conns[owner]
                .execute_batch(&format!("RELEASE {sp}"))
                .unwrap();
            self.tx.as_mut().unwrap().savepoints.truncate(idx);
        } else if roll < 82 {
            self.run_cross_conn_contention_probe(rng);
        } else if roll < 87 {
            self.run_rename_step();
        } else if roll < 91 {
            self.run_temp_vtab_savepoint_step();
        } else if roll < 94 {
            self.run_maintenance_step(rng);
        } else {
            let commit = rng.chance(3, 4);
            self.commit_or_rollback(commit);
        }
    }

    fn finish_pending_tx(&mut self, commit: bool) {
        if self.tx.is_some() {
            self.commit_or_rollback(commit);
        }
    }
}

fn run_seed(seed: u64, steps: usize) {
    let mut rng = Rng::new(seed);
    let mut harness = MultiConnHarness::new(seed, "diff");
    for _ in 0..steps {
        harness.step(&mut rng);
    }
    harness.finish_pending_tx(true);
    let c = harness.active_conn(&mut rng);
    harness.assert_tables_match_on_conn(c, "final");
    harness.verify_settled("end of seed");
}

#[test]
fn multi_conn_mixed_native_ddl_compaction_matches_oracle() {
    let (start, count) = seed_range();
    for offset in 0..count {
        run_seed(start.wrapping_add(offset), 55);
    }
}

#[test]
fn export_formal_traces_multi_conn_diff() {
    let mut rng = Rng::new(42);
    let mut harness = MultiConnHarness::new(42, "trace_export");
    harness.engine.write().unwrap().enable_tx_trace();

    // Scripted phases guarantee all multi-conn, DDL, temp-vtab prune, UNIQUE invalidation, and
    // maintenance paths appear deterministically in the trace alongside randomized steps.
    harness.begin_tx(0);
    harness.mirror_dml(0, TableSlot::T1, "INSERT INTO {T} VALUES ('k0', 'init')");
    harness.mirror_dml(0, TableSlot::T2, "INSERT INTO {T} VALUES ('k0', 'sync0')");
    harness.mirror_dml(
        0,
        TableSlot::UniqueU,
        "INSERT INTO {T} VALUES ('k0', 'u0', 'N0', 'c0')",
    );
    harness.run_cross_conn_contention_probe(&mut rng);
    harness.run_temp_vtab_savepoint_step();
    harness.run_rename_step();
    harness.commit_or_rollback(true);

    harness.run_cross_conn_unique_invalidation();

    for _ in 0..24 {
        harness.step(&mut rng);
    }
    harness.finish_pending_tx(true);
    harness.run_maintenance_step(&mut rng);
    harness.verify_settled("trace export final");

    let trace = harness.engine.write().unwrap().take_tx_trace().unwrap();
    assert!(!trace.is_empty());
    trace
        .replay_verify()
        .expect("multi-conn differential trace must replay identically");
    let normalized = trace.normalize_ids();
    normalized
        .replay_verify()
        .expect("normalized multi-conn differential trace must replay cleanly");
    let ndjson = normalized.to_ndjson();

    if let Some(dir) = std::env::var_os("AYDB_EXPORT_TRACES_DIR") {
        if !dir.is_empty() {
            let dir_path = std::path::Path::new(&dir);
            std::fs::create_dir_all(dir_path).expect("create AYDB_EXPORT_TRACES_DIR");
            std::fs::write(dir_path.join("tx_fsm_multi_conn_diff.ndjson"), &ndjson)
                .expect("write tx_fsm_multi_conn_diff.ndjson");
        }
    }
}
