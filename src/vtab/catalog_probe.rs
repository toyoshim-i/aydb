//! SQLite `sqlite_schema` inspection and stale in-transaction virtual table pruning.
//!
//! Releases the engine write lock around all SQLite catalog queries so schema re-parsing (`xConnect`)
//! can never deadlock against `engine.write()`.

use core::ffi::c_char;
use std::io;
use std::sync::{Arc, RwLock};

use crate::engine::StorageEngine;
use crate::pal::OsFs;
use crate::vtab::ffi::{
    SQLITE_DONE, SQLITE_OK, SQLITE_ROW, SqliteStmtGuard, sqlite_column_text_bytes,
    sqlite_transient, sqlite3, sqlite3_bind_text, sqlite3_prepare_v2, sqlite3_step, sqlite3_stmt,
};
use crate::vtab::snapshot;

/// Target descriptor for [`prune_matching_created_vtabs_unlocked`].
pub(in crate::vtab) struct CreatedVtabMatch<'a> {
    pub(in crate::vtab) table_id: Option<u16>,
    pub(in crate::vtab) table_name: &'a str,
    pub(in crate::vtab) cur_norm_db: &'a str,
    pub(in crate::vtab) exclude_table_id: Option<u16>,
}

/// Drops any tables created in the active transaction that are no longer `aydb` virtual tables
/// in SQLite's schema (for instance after a `ROLLBACK TO` that preceded the table's creation).
/// Releases `eng` before running internal `sqlite_schema` queries because `sqlite3_prepare_v2`
/// can trigger schema re-parsing (`xConnect`) that acquires `engine.write()`.
pub(in crate::vtab) unsafe fn prune_stale_created_vtabs_unlocked<'a>(
    db: *mut sqlite3,
    engine: &'a Arc<RwLock<StorageEngine<OsFs>>>,
    conn_id: usize,
    mut eng: std::sync::RwLockWriteGuard<'a, StorageEngine<OsFs>>,
) -> io::Result<std::sync::RwLockWriteGuard<'a, StorageEngine<OsFs>>> {
    let candidates = eng.vtabs_created_in_tx_snapshot();
    if candidates.is_empty() {
        return Ok(eng);
    }
    drop(eng);
    let mut missing = Vec::new();
    for (_, name, prev_db) in candidates {
        if unsafe { !schema_has_aydb_table(db, &prev_db, &name) } {
            missing.push((prev_db, name));
        }
    }
    eng = engine
        .write()
        .map_err(|_| io::Error::other("engine lock poisoned"))?;
    if !missing.is_empty() && eng.conn_has_open_transaction(conn_id) {
        let stale = eng.take_stale_vtabs_created_in_tx(|prev_db, prev_name| {
            missing.iter().any(|(d, n)| d == prev_db && n == prev_name)
        });
        for (id, stale_name) in stale {
            eng.drop_stale_created_vtab(id, &stale_name)?;
        }
    }
    Ok(eng)
}

/// Drops entries in `vtabs_created_in_tx` matching `target.table_name` or `target.table_id` that
/// collide with `(target.cur_norm_db, target.table_name)` (excluding `target.exclude_table_id`) or
/// are absent from SQLite's `aydb` schema, releasing `eng` around any `sqlite_schema` queries.
pub(in crate::vtab) unsafe fn prune_matching_created_vtabs_unlocked<'a>(
    db: *mut sqlite3,
    engine: &'a Arc<RwLock<StorageEngine<OsFs>>>,
    conn_id: usize,
    mut eng: std::sync::RwLockWriteGuard<'a, StorageEngine<OsFs>>,
    target: CreatedVtabMatch<'_>,
) -> io::Result<std::sync::RwLockWriteGuard<'a, StorageEngine<OsFs>>> {
    let cur_norm_name = crate::engine::normalize_vtab_ident(target.table_name);
    let matching: Vec<_> = eng
        .vtabs_created_in_tx_snapshot()
        .into_iter()
        .filter(|(id, name, _)| *name == cur_norm_name || Some(*id) == target.table_id)
        .collect();
    if matching.is_empty() {
        return Ok(eng);
    }
    let to_probe: Vec<_> = matching
        .into_iter()
        .filter(|(id, name, prev_db)| {
            !(Some(*id) != target.exclude_table_id
                && prev_db == target.cur_norm_db
                && *name == cur_norm_name)
        })
        .collect();
    let mut probed_missing = Vec::new();
    if !to_probe.is_empty() {
        drop(eng);
        for (_, name, prev_db) in to_probe {
            if unsafe { !schema_has_aydb_table(db, &prev_db, &name) } {
                probed_missing.push((prev_db, name));
            }
        }
        eng = engine
            .write()
            .map_err(|_| io::Error::other("engine lock poisoned"))?;
    }
    if eng.conn_has_open_transaction(conn_id) {
        let stale = eng.take_vtabs_created_in_tx(
            target.table_id,
            target.table_name,
            |prev_id, prev_db, prev_name| {
                (Some(prev_id) != target.exclude_table_id
                    && prev_db == target.cur_norm_db
                    && prev_name == cur_norm_name)
                    || probed_missing
                        .iter()
                        .any(|(d, n)| d == prev_db && n == prev_name)
            },
        );
        for (id, stale_name) in stale {
            eng.drop_stale_created_vtab(id, &stale_name)?;
        }
    }
    Ok(eng)
}

struct SqlScanner<'a> {
    rest: &'a str,
}

impl<'a> SqlScanner<'a> {
    fn skip_ws_and_comments(&mut self) {
        loop {
            self.rest = self
                .rest
                .trim_start_matches(|c: char| c.is_ascii_whitespace());
            if let Some(after) = self.rest.strip_prefix("--") {
                self.rest = match after.find('\n') {
                    Some(idx) => &after[idx + 1..],
                    None => "",
                };
            } else if let Some(after) = self.rest.strip_prefix("/*") {
                self.rest = match after.find("*/") {
                    Some(idx) => &after[idx + 2..],
                    None => "",
                };
            } else {
                break;
            }
        }
    }

    fn scan_bare_token(&mut self) -> Option<&'a str> {
        self.skip_ws_and_comments();
        let len = self
            .rest
            .char_indices()
            .find(|&(_, c)| !(c.is_ascii_alphanumeric() || c == '_' || c == '$' || !c.is_ascii()))
            .map_or(self.rest.len(), |(i, _)| i);
        if len == 0 {
            return None;
        }
        let tok = &self.rest[..len];
        self.rest = &self.rest[len..];
        Some(tok)
    }

    fn next_keyword(&mut self) -> Option<&'a str> {
        self.scan_bare_token()
    }

    fn try_skip_if_not_exists(&mut self) {
        let saved = self.rest;
        if self
            .next_keyword()
            .is_some_and(|w| w.eq_ignore_ascii_case("IF"))
            && self
                .next_keyword()
                .is_some_and(|w| w.eq_ignore_ascii_case("NOT"))
            && self
                .next_keyword()
                .is_some_and(|w| w.eq_ignore_ascii_case("EXISTS"))
        {
            return;
        }
        self.rest = saved;
    }

    fn next_ident(&mut self) -> Option<String> {
        self.skip_ws_and_comments();
        let mut chars = self.rest.char_indices().peekable();
        let (_, first) = chars.next()?;
        match first {
            '"' | '`' | '\'' => {
                let quote = first;
                let mut out = String::new();
                while let Some((i, ch)) = chars.next() {
                    if ch == quote {
                        if let Some((_, next_ch)) = chars.peek().copied() {
                            if next_ch == quote {
                                chars.next();
                                out.push(quote);
                                continue;
                            }
                        }
                        self.rest = &self.rest[i + ch.len_utf8()..];
                        return Some(out);
                    }
                    out.push(ch);
                }
                None
            }
            '[' => {
                let mut out = String::new();
                for (i, ch) in chars {
                    if ch == ']' {
                        self.rest = &self.rest[i + 1..];
                        return Some(out);
                    }
                    out.push(ch);
                }
                None
            }
            _ => self.scan_bare_token().map(str::to_string),
        }
    }

    fn consume_dot(&mut self) -> bool {
        self.skip_ws_and_comments();
        if let Some(after) = self.rest.strip_prefix('.') {
            self.rest = after;
            true
        } else {
            false
        }
    }
}

/// Returns `true` iff `sql` is a `CREATE VIRTUAL TABLE [IF NOT EXISTS] [schema.]table USING aydb`
/// DDL statement stored in `sqlite_schema`.
pub(in crate::vtab) fn is_aydb_vtab_sql(sql: &str) -> bool {
    let mut s = SqlScanner { rest: sql };
    if !s
        .next_keyword()
        .is_some_and(|w| w.eq_ignore_ascii_case("CREATE"))
    {
        return false;
    }
    if !s
        .next_keyword()
        .is_some_and(|w| w.eq_ignore_ascii_case("VIRTUAL"))
    {
        return false;
    }
    if !s
        .next_keyword()
        .is_some_and(|w| w.eq_ignore_ascii_case("TABLE"))
    {
        return false;
    }
    s.try_skip_if_not_exists();
    if s.next_ident().is_none() {
        return false;
    }
    if s.consume_dot() && s.next_ident().is_none() {
        return false;
    }
    if !s
        .next_keyword()
        .is_some_and(|w| w.eq_ignore_ascii_case("USING"))
    {
        return false;
    }
    let Some(module) = s.next_ident() else {
        return false;
    };
    if !module.eq_ignore_ascii_case("aydb") {
        return false;
    }
    s.skip_ws_and_comments();
    s.rest.is_empty() || s.rest.starts_with('(') || s.rest.starts_with(';')
}

/// Queries one schema's `sqlite_master` (`sqlite_temp_master` for `"temp"`) for `name` and checks
/// whether it is an `aydb` virtual table. Returns `None` if the query could not be completed.
unsafe fn query_schema_for_aydb_table(db: *mut sqlite3, catalog: &str, name: &str) -> Option<bool> {
    let Ok(name_len) = i32::try_from(name.len()) else {
        return None;
    };
    let schema_table = if catalog.eq_ignore_ascii_case("temp") {
        "temp.sqlite_temp_master".to_string()
    } else if catalog.eq_ignore_ascii_case("main") {
        "main.sqlite_master".to_string()
    } else {
        let escaped = catalog.replace('"', "\"\"");
        format!("\"{escaped}\".sqlite_master")
    };
    let sql = format!(
        "SELECT sql FROM {schema_table} WHERE type = 'table' AND name = ?1 COLLATE NOCASE LIMIT 1"
    );
    let Ok(sql_len) = i32::try_from(sql.len()) else {
        return None;
    };
    let mut raw_stmt: *mut sqlite3_stmt = core::ptr::null_mut();
    let prep_rc = unsafe {
        sqlite3_prepare_v2(
            db,
            sql.as_ptr() as *const c_char,
            sql_len,
            &mut raw_stmt,
            core::ptr::null_mut(),
        )
    };
    let stmt = SqliteStmtGuard::new(raw_stmt);
    if prep_rc != SQLITE_OK || stmt.as_ptr().is_null() {
        return None;
    }
    let bind_rc = unsafe {
        sqlite3_bind_text(
            stmt.as_ptr(),
            1,
            name.as_ptr() as *const c_char,
            name_len,
            sqlite_transient(),
        )
    };
    if bind_rc != SQLITE_OK {
        None
    } else {
        match unsafe { sqlite3_step(stmt.as_ptr()) } {
            SQLITE_ROW => {
                let bytes = unsafe { sqlite_column_text_bytes(stmt.as_ptr(), 0) };
                core::str::from_utf8(bytes).ok().map(is_aydb_vtab_sql)
            }
            SQLITE_DONE => Some(false),
            _ => None,
        }
    }
}

/// Whether SQLite's schema (`db_name`, or `temp`/`main` if empty) still has an `aydb` virtual
/// table of this name. Returns `None` if the schema catalog could not be queried.
pub(in crate::vtab) unsafe fn schema_has_aydb_table_exact(
    db: *mut sqlite3,
    db_name: &str,
    name: &str,
) -> Option<bool> {
    let _guard = snapshot::enter_internal_schema_query();
    if !db_name.is_empty() {
        return unsafe { query_schema_for_aydb_table(db, db_name, name) };
    }
    match unsafe { query_schema_for_aydb_table(db, "temp", name) } {
        Some(true) => Some(true),
        Some(false) => unsafe { query_schema_for_aydb_table(db, "main", name) },
        None => None,
    }
}

/// Whether SQLite's schema (`db_name`, or `temp`/`main` if empty) still has an `aydb` virtual
/// table of this name. Any doubt counts as "yes": a table is only removed when the catalog query
/// definitively finds no `USING aydb` virtual table with this name.
unsafe fn schema_has_aydb_table(db: *mut sqlite3, db_name: &str, name: &str) -> bool {
    unsafe { schema_has_aydb_table_exact(db, db_name, name) }.unwrap_or(true)
}

#[cfg(test)]
mod schema_sql_tests {
    use super::is_aydb_vtab_sql;

    #[test]
    fn recognizes_aydb_virtual_table_ddl_and_rejects_other_tables() {
        assert!(is_aydb_vtab_sql(
            "CREATE VIRTUAL TABLE t USING aydb(columns='k INT PRIMARY KEY')"
        ));
        assert!(is_aydb_vtab_sql(
            "  create /* c1 */ virtual -- line comment\n table if not exists main.[my.t] using \"AYDB\" (columns='k INT')"
        ));
        assert!(is_aydb_vtab_sql(
            "CREATE VIRTUAL TABLE \"t\"\"1\" USING `aydb`"
        ));
        assert!(is_aydb_vtab_sql(
            "CREATE VIRTUAL TABLE aux.`if` USING [AyDb];"
        ));

        assert!(!is_aydb_vtab_sql(""));
        assert!(!is_aydb_vtab_sql("CREATE TABLE t(a INT)"));
        assert!(!is_aydb_vtab_sql(
            "CREATE TABLE t(a TEXT DEFAULT 'USING aydb')"
        ));
        assert!(!is_aydb_vtab_sql("CREATE VIRTUAL TABLE t USING fts5(a)"));
        assert!(!is_aydb_vtab_sql(
            "CREATE VIRTUAL TABLE t USING aydb_other(columns='k INT')"
        ));
        assert!(!is_aydb_vtab_sql(
            "CREATE VIRTUAL TABLE t USING \"aydb\"extra(columns='k INT')"
        ));
    }
}
