//! Virtual table write path (`xUpdate`), numeric key duals, auto-rowid allocation,
//! and conflict resolution (`ABORT`/`IGNORE`/`REPLACE`).

use core::ffi::c_int;
use std::io;
use std::panic::{AssertUnwindSafe, catch_unwind};

use crate::engine::{
    PutRowOptions, RangeScanOptions, STATEMENT_SAVEPOINT, SlabScanOutput, StorageEngine,
};
use crate::pal::OsFs;
use crate::vtab::constraints;
use crate::vtab::ffi::{
    SQLITE_CONSTRAINT, SQLITE_ERROR, SQLITE_FULL, SQLITE_INTEGER, SQLITE_MISMATCH, SQLITE_NOMEM,
    SQLITE_NULL, SQLITE_OK, SQLITE_REPLACE, SQLITE_TOOBIG, SqliteValueRef, sqlite3_int64,
    sqlite3_value, sqlite3_vtab,
};
use crate::vtab::lifecycle::STALE_VTAB_ERR;
use crate::vtab::row_codec::{
    ColumnAffinity, KEY_TAG_INTEGER, decode_i64_tagged_key, encode_i64_tagged_key,
    f64_to_exact_i64, parse_sqlite_numeric_str, sqlite_value_to_key_bytes_with_schema,
    try_encode_sqlite_values_direct,
};
use crate::vtab::{AydbVTab, io_err_to_sqlite_rc, rowid_map};

/// Returns the alternate exact-integer numeric key representation (`9`-byte `Integer(q)` vs
/// `17`-byte `Float(q.0)`) so SQLite's `10 == 10.0` numeric key equality holds across `BLOB`
/// and `REAL` affinities.
#[inline]
pub(in crate::vtab) fn build_numeric_alt_key(pk: &[u8]) -> Option<Vec<u8>> {
    if pk.first().copied() != Some(KEY_TAG_INTEGER) {
        return None;
    }
    if pk.len() == 9 {
        let mut alt = Vec::with_capacity(17);
        alt.extend_from_slice(pk);
        alt.extend_from_slice(&0u64.to_be_bytes());
        Some(alt)
    } else if pk.len() == 17 && pk[9..17] == [0u8; 8] {
        Some(pk[0..9].to_vec())
    } else {
        None
    }
}

#[inline]
pub(in crate::vtab) fn needs_numeric_alt_probe(pk: &[u8], affinity: ColumnAffinity) -> bool {
    if !matches!(affinity, ColumnAffinity::Integer | ColumnAffinity::Numeric) {
        return true;
    }
    // Under SQLite's strict open coercion interval (-2^63, 2^63), `-9223372036854775808.0` stays
    // REAL (17-byte key) in INTEGER/NUMERIC columns while comparing equal to `i64::MIN` (9-byte
    // key). Check the leading 9-byte integer prefix directly via `decode_i64_tagged_key`.
    pk.len() >= 9 && decode_i64_tagged_key(&pk[..9]) == Some(i64::MIN)
}

/// Checks if `pk` or its exact-integer numeric dual (`build_numeric_alt_key(pk)`) exists in
/// `table_id`, returning the existing key bytes if found.
///
/// When `pk_affinity` is `ColumnAffinity::Integer` or `ColumnAffinity::Numeric`, all exact-integer
/// floats in `(-2^63, 2^63)` are coerced to 9-byte `Integer(q)` before storage, so only
/// `i64::MIN` / `-9223372036854775808.0` can have an uncoerced numeric dual in the table.
#[inline]
fn find_existing_pk_or_numeric_alt(
    eng: &mut StorageEngine<OsFs>,
    table_id: u16,
    pk: &[u8],
    pk_affinity: ColumnAffinity,
) -> io::Result<Option<Vec<u8>>> {
    if eng.row_exists(table_id, pk)? {
        return Ok(Some(pk.to_vec()));
    }
    if needs_numeric_alt_probe(pk, pk_affinity) {
        if let Some(alt) = build_numeric_alt_key(pk) {
            if eng.row_exists(table_id, &alt)? {
                return Ok(Some(alt));
            }
        }
    }
    Ok(None)
}

pub(in crate::vtab) unsafe fn extract_lossless_rowid_arg(val: *mut sqlite3_value) -> Option<i64> {
    if val.is_null() {
        return None;
    }
    let val_ref = unsafe { SqliteValueRef::from_raw(val) };
    extract_lossless_rowid(val_ref)
}

fn extract_lossless_rowid(val: SqliteValueRef<'_>) -> Option<i64> {
    if val.as_ptr().is_null() {
        return None;
    }
    match val.value_type() {
        SQLITE_INTEGER => Some(val.as_int64()),
        crate::vtab::ffi::SQLITE_FLOAT => f64_to_exact_i64(val.as_double()),
        crate::vtab::ffi::SQLITE_TEXT => {
            let slice = val.as_text_bytes();
            if slice.is_empty() {
                return None;
            }
            let s = core::str::from_utf8(slice).ok()?;
            match parse_sqlite_numeric_str(s) {
                Some(Ok(i)) => Some(i),
                _ => None,
            }
        }
        _ => None,
    }
}

pub(in crate::vtab) fn resolve_pk_for_rowid(
    vtab: &AydbVTab,
    target_rid: i64,
    row_exists: impl FnOnce(u16, &[u8]) -> io::Result<bool>,
    lookup: impl FnOnce(u16, i64) -> io::Result<Option<Vec<u8>>>,
) -> io::Result<Option<Vec<u8>>> {
    if vtab.pk_rowid_alias {
        let integer_pk = encode_i64_tagged_key(target_rid);
        if row_exists(vtab.table_id, &integer_pk)? {
            return Ok(Some(integer_pk.to_vec()));
        }
    }
    // A local cache may contain a deleted or reused identity from another connection.
    // Resolve against the same persisted read view used for the row itself.
    lookup(vtab.table_id, target_rid)
}

/// Returns the largest integer-tagged primary key of `table_id` visible to `conn_id`, or `None`
/// if it has none. A connection that does not own the open transaction sees committed rows only.
fn max_integer_rowid(
    eng: &mut StorageEngine<OsFs>,
    conn_id: usize,
    table_id: u16,
) -> io::Result<Option<i64>> {
    let lower = [KEY_TAG_INTEGER];
    eng.with_committed_read_for_conn(conn_id, |reader| {
        let mut upper = vec![KEY_TAG_INTEGER + 1];
        let mut upper_inclusive = false;
        let mut limit = 1usize;
        let (mut pks, mut slab, mut spans) = (Vec::new(), Vec::new(), Vec::new());
        loop {
            pks.clear();
            slab.clear();
            spans.clear();
            reader.scan_range_into_slab_with_cache(
                table_id,
                RangeScanOptions {
                    lower: Some((&lower, true)),
                    upper: Some((&upper, upper_inclusive)),
                    reverse: true,
                    limit,
                    keys_only: true,
                },
                true,
                SlabScanOutput {
                    pks: &mut pks,
                    slab: &mut slab,
                    spans: &mut spans,
                    lazy_blobs: None,
                },
            )?;
            for k in &pks {
                if let Some(id) = decode_i64_tagged_key(k) {
                    return Ok(Some(id));
                }
            }
            if pks.len() < limit {
                return Ok(None);
            }
            let last = pks.last().expect("limit >= 1 and pks.len() >= limit");
            // Floats in [-2^63, 2^63) share `KEY_TAG_INTEGER` as 17-byte keys sorting strictly
            // after their 9-byte `floor(f)` integer key with no integer keys in between; jumping to
            // `last[..9]` inclusive skips every float in that bucket in one step.
            if last.len() > 9 && last[0] == KEY_TAG_INTEGER {
                upper = last[..9].to_vec();
                upper_inclusive = true;
            } else {
                upper = last.clone();
                upper_inclusive = false;
            }
            limit = 64;
        }
    })
}

fn integer_rowid_occupied(
    eng: &StorageEngine<OsFs>,
    table_id: u16,
    rowid: i64,
) -> io::Result<bool> {
    Ok(eng.row_exists(table_id, &encode_i64_tagged_key(rowid))?
        || eng
            .pk_for_rowid_view(crate::engine::ReadView::Owner, table_id, rowid)?
            .is_some())
}

fn has_stale_unique_marker(eng: &StorageEngine<OsFs>, vtab: &AydbVTab) -> io::Result<bool> {
    let marker_gen = eng.unique_marker_gen();
    if !eng.unique_marker_modified_in_tx() && vtab.unique_ready.is_ready_for(marker_gen) {
        return Ok(false);
    }
    let existing = eng.unique_marker_view(crate::engine::ReadView::Owner, vtab.table_id)?;
    if vtab.unique_def.is_empty() {
        Ok(existing.is_some())
    } else {
        Ok(existing.is_some_and(|found| found.as_slice() != vtab.unique_def.marker_bytes()))
    }
}

fn invalidate_stale_unique_marker_if_needed(
    eng: &mut StorageEngine<OsFs>,
    vtab: &AydbVTab,
) -> io::Result<()> {
    let marker_gen = eng.unique_marker_gen();
    if !eng.unique_marker_modified_in_tx() && vtab.unique_ready.is_ready_for(marker_gen) {
        return Ok(());
    }
    let existing = eng.unique_marker_view(crate::engine::ReadView::Owner, vtab.table_id)?;
    if vtab.unique_def.is_empty() {
        if existing.is_some() {
            eng.remove_unique_marker(vtab.table_id, vtab.durability)?;
        }
        vtab.unique_ready.latch_if_committed(
            eng.unique_marker_gen(),
            !eng.in_active_transaction() && !eng.unique_marker_modified_in_tx(),
        );
    } else if existing.is_some_and(|found| found.as_slice() != vtab.unique_def.marker_bytes()) {
        eng.remove_unique_marker(vtab.table_id, vtab.durability)?;
    }
    Ok(())
}

#[inline]
fn needs_synthetic_delete_check(vtab: &AydbVTab, pk: &[u8]) -> bool {
    !(vtab.pk_rowid_alias && decode_i64_tagged_key(pk).is_some())
}

/// Runs `f` inside `STATEMENT_SAVEPOINT` when `guarded` is true, rolling back and releasing the
/// savepoint on failure so multi-step mutations leave no partial deletes or stale markers behind.
fn with_statement_savepoint<R>(
    eng: &mut StorageEngine<OsFs>,
    conn_id: usize,
    table_id: u16,
    guarded: bool,
    f: impl FnOnce(&mut StorageEngine<OsFs>) -> io::Result<R>,
) -> io::Result<R> {
    if guarded {
        eng.statement_savepoint_for_conn(conn_id, table_id)?;
    }
    let res = f(eng);
    if guarded {
        if res.is_err() {
            let _ = eng.rollback_to_savepoint_for_conn(conn_id, STATEMENT_SAVEPOINT);
            let _ = eng.release_savepoint_for_conn(conn_id, STATEMENT_SAVEPOINT);
        } else {
            eng.release_savepoint_for_conn(conn_id, STATEMENT_SAVEPOINT)?;
        }
    }
    res
}

fn execute_delete(
    eng: &mut StorageEngine<OsFs>,
    vtab: &AydbVTab,
    arg0: SqliteValueRef<'_>,
) -> c_int {
    let old_pk_opt = if let Some(rid) = extract_lossless_rowid(arg0) {
        match resolve_pk_for_rowid(
            vtab,
            rid,
            |t, pk| eng.row_exists(t, pk),
            |t, rid| eng.pk_for_rowid_view(crate::engine::ReadView::Owner, t, rid),
        ) {
            Ok(opt) => opt,
            Err(e) => return io_err_to_sqlite_rc(&e),
        }
    } else {
        Some(unsafe {
            sqlite_value_to_key_bytes_with_schema(
                arg0.as_ptr(),
                vtab.pk_affinity,
                vtab.pk_collation,
                false,
            )
        })
    };
    if let Some(old_pk) = old_pk_opt {
        let stale_marker = match has_stale_unique_marker(eng, vtab) {
            Ok(b) => b,
            Err(e) => return io_err_to_sqlite_rc(&e),
        };
        let check_syn = needs_synthetic_delete_check(vtab, &old_pk);
        if let Err(e) =
            with_statement_savepoint(eng, vtab.db as usize, vtab.table_id, stale_marker, |eng| {
                let deleted = eng.delete_row_with_rowid_hint(
                    vtab.table_id,
                    &old_pk,
                    vtab.durability,
                    check_syn,
                )?;
                if deleted {
                    invalidate_stale_unique_marker_if_needed(eng, vtab)?;
                }
                Ok(())
            })
        {
            return io_err_to_sqlite_rc(&e);
        }
    }
    SQLITE_OK
}

fn allocate_auto_rowid(
    eng: &mut StorageEngine<OsFs>,
    vtab: &AydbVTab,
) -> Result<(i64, Option<i64>), c_int> {
    // Like a native table: the largest key at this moment plus one (1 for an empty table).
    // Nothing is remembered between inserts, so a failed statement, a rolled back transaction or
    // savepoint, a delete, or another connection's insert never leave a gap or a duplicate. At
    // `i64::MAX` the random search below takes over.
    //
    // The previous automatic insert of this table left `max + 1` behind together with the table's
    // mutation epoch. While the epoch is unchanged nothing modified this table's rows or rowids
    // since, so that value still is `max + 1`.
    let table_epoch = eng.table_mutation_epoch(vtab.table_id);
    let cached = vtab
        .auto_key_cache
        .lock()
        .ok()
        .and_then(|guard| *guard)
        .filter(|(epoch, _)| *epoch == table_epoch)
        .map(|(_, next)| next);
    let mut candidate = match cached {
        Some(next) => next,
        None => match max_integer_rowid(eng, vtab.db as usize, vtab.table_id) {
            Ok(None) => 1,
            Ok(Some(max)) => max.saturating_add(1),
            Err(e) => return Err(io_err_to_sqlite_rc(&e)),
        },
    };
    let first_candidate = candidate;
    let mut found_free = None;
    // 1. Bounded sequential probe using checked arithmetic. It also steps over a
    //    number that a synthetic rowid already owns.
    for _ in 0..1024 {
        match integer_rowid_occupied(eng, vtab.table_id, candidate) {
            Ok(false) => {
                found_free = Some(candidate);
                break;
            }
            Ok(true) => match candidate.checked_add(1) {
                Some(next) => candidate = next,
                None => break,
            },
            Err(e) => return Err(io_err_to_sqlite_rc(&e)),
        }
    }
    // 2. If `i64::MAX` was reached (or 1024 consecutive IDs were occupied), probe up to 64
    //    deterministic pseudo-random positive `i64` candidates (`1..=i64::MAX`), matching SQLite's
    //    fallback rowid generator, and return `SQLITE_FULL` if exhausted.
    if found_free.is_none() {
        let mut state = (eng.next_lsn() ^ 0x9E37_79B9_7F4A_7C15).wrapping_add(vtab.table_id as u64);
        for attempt in 0..64u64 {
            state = state
                .wrapping_add(0x9E37_79B9_7F4A_7C15)
                .wrapping_add(attempt);
            let mut z = state;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^= z >> 31;
            let rand_pos = ((z & 0x7FFF_FFFF_FFFF_FFFF) as i64).max(1);
            match integer_rowid_occupied(eng, vtab.table_id, rand_pos) {
                Ok(false) => {
                    found_free = Some(rand_pos);
                    break;
                }
                Ok(true) => {}
                Err(e) => return Err(io_err_to_sqlite_rc(&e)),
            }
        }
    }
    let Some(chosen) = found_free else {
        return Err(SQLITE_FULL);
    };
    // Only a key that is `max + 1` can be remembered as such: not a number found by
    // stepping over synthetic rowids, and not one from the random search.
    let cache_next = (chosen == first_candidate)
        .then(|| chosen.checked_add(1))
        .flatten();
    Ok((chosen, cache_next))
}

struct RowidConflictQuery<'a> {
    old_pk_opt: Option<&'a [u8]>,
    new_pk: &'a [u8],
    coerced_int_pk: Option<i64>,
    mapped_rowid: Option<i64>,
}

fn find_rowid_conflict(
    eng: &StorageEngine<OsFs>,
    vtab: &AydbVTab,
    query: RowidConflictQuery<'_>,
) -> io::Result<Option<Vec<u8>>> {
    let Some(rowid) = query.coerced_int_pk.or(query.mapped_rowid) else {
        return Ok(None);
    };
    if vtab.pk_rowid_alias && query.coerced_int_pk.is_none() {
        let int_pk = encode_i64_tagged_key(rowid);
        if query.old_pk_opt != Some(int_pk.as_slice()) && query.new_pk != int_pk.as_slice() {
            if eng.row_exists(vtab.table_id, &int_pk)? {
                return Ok(Some(int_pk.to_vec()));
            }
        }
    }
    match eng.pk_for_rowid_view(crate::engine::ReadView::Owner, vtab.table_id, rowid)? {
        // The row that already owns the rowid is no conflict when it is the row being written or
        // the row being updated.
        Some(pk)
            if query.old_pk_opt.is_none_or(|old| old != pk.as_slice())
                && (query.mapped_rowid.is_none() || pk.as_slice() != query.new_pk) =>
        {
            Ok(Some(pk))
        }
        _ => Ok(None),
    }
}

struct UpsertRowidPlan {
    is_insert: bool,
    old_rowid_arg: Option<i64>,
    new_rowid_req: Option<i64>,
    pk_override: Option<i64>,
    auto_assigned: bool,
    cache_next_key: Option<i64>,
}

struct UpsertWritePlan {
    new_pk: Vec<u8>,
    row_bytes: Vec<u8>,
    sec_keys: Vec<(u16, Vec<u8>)>,
    old_pk_opt: Option<Vec<u8>>,
    same_pk: bool,
    known_absent: bool,
    pk_conflict_to_delete: Option<Vec<u8>>,
    unique_conflicts: Vec<Vec<u8>>,
    rowid_conflict_pk: Option<Vec<u8>>,
    pk_override: Option<i64>,
    coerced_int_pk: Option<i64>,
    mapped_rowid: Option<i64>,
    kept_rowid: Option<i64>,
    is_insert: bool,
    old_rowid_arg: Option<i64>,
    cache_next_key: Option<i64>,
}

fn validate_upsert_constraints(
    vtab: &AydbVTab,
    args: &[SqliteValueRef<'_>],
    is_insert: bool,
) -> Result<(), c_int> {
    if let Some(reason) = vtab.unsupported {
        // A row cannot be checked (CHECK) or completed (DEFAULT), so none may be stored.
        // SQLITE_CONSTRAINT would be resolved by the statement's conflict mode (OR IGNORE
        // would skip the row and report success, OR REPLACE would delete conflicting rows
        // first), so this is a plain error that every mode reports as such.
        vtab.set_err(reason);
        return Err(SQLITE_ERROR);
    }
    let num_cols = vtab.col_names.len();
    if args.len() < 2 + num_cols {
        return Err(SQLITE_ERROR);
    }
    // A native rowid alias refuses to become NULL in an UPDATE ("datatype mismatch"), a hard
    // error that no conflict clause turns into a skipped row.
    if !is_insert && vtab.pk_rowid_alias && args[2 + vtab.pk_col_idx].value_type() == SQLITE_NULL {
        return Err(SQLITE_MISMATCH);
    }
    for (idx, &not_null) in vtab.col_not_null.iter().enumerate() {
        if not_null
            && !(idx == vtab.pk_col_idx && is_insert && vtab.pk_rowid_alias)
            && args[2 + idx].value_type() == SQLITE_NULL
        {
            // NOT NULL has no REPLACE resolution without a DEFAULT, which this adapter does not
            // support, so SQLite handles every mode as the statement requires.
            return Err(SQLITE_CONSTRAINT);
        }
    }
    Ok(())
}

fn resolve_upsert_rowid_plan(
    eng: &mut StorageEngine<OsFs>,
    vtab: &AydbVTab,
    args: &[SqliteValueRef<'_>],
    is_insert: bool,
) -> Result<UpsertRowidPlan, c_int> {
    let arg0 = args[0];
    let pk_val = args[2 + vtab.pk_col_idx];
    let pk_val_type = pk_val.value_type();

    // `args[1]` is the rowid the statement asks for: the explicit rowid of an INSERT, or the new
    // rowid of an UPDATE, which equals the old one unless the statement changed it. `new_rowid_req`
    // is set only when the statement really asks for a rowid.
    let new_rowid_arg = args[1];
    if !is_insert && new_rowid_arg.value_type() == SQLITE_NULL {
        return Err(SQLITE_MISMATCH);
    }
    let old_rowid_arg = if is_insert {
        None
    } else {
        extract_lossless_rowid(arg0)
    };
    let new_rowid_req = if new_rowid_arg.value_type() == SQLITE_NULL {
        None
    } else {
        match extract_lossless_rowid(new_rowid_arg) {
            Some(rowid) if is_insert || old_rowid_arg != Some(rowid) => Some(rowid),
            Some(_) => None,
            None => return Err(SQLITE_MISMATCH),
        }
    };
    // `INTEGER PRIMARY KEY` is the rowid: a rowid without a key of its own sets the key
    // (INSERT with a NULL key), and an UPDATE that leaves the key alone moves it. An explicit key
    // wins over the rowid, as on a native table.
    let alias_rowid = match new_rowid_req {
        Some(rowid) if vtab.pk_rowid_alias => {
            if is_insert {
                (pk_val_type == SQLITE_NULL).then_some(rowid)
            } else {
                (pk_val_type == SQLITE_INTEGER && Some(pk_val.as_int64()) == old_rowid_arg)
                    .then_some(rowid)
            }
        }
        _ => None,
    };
    let mut auto_assigned = false;
    let mut cache_next_key: Option<i64> = None;

    // Handle `SQLITE_NULL` primary key.
    let pk_override = if alias_rowid.is_some() {
        alias_rowid
    } else if pk_val_type == SQLITE_NULL {
        if is_insert && vtab.pk_rowid_alias {
            let (chosen, next_cache) = allocate_auto_rowid(eng, vtab)?;
            auto_assigned = true;
            cache_next_key = next_cache;
            Some(chosen)
        } else {
            return Err(SQLITE_CONSTRAINT);
        }
    } else {
        None
    };

    Ok(UpsertRowidPlan {
        is_insert,
        old_rowid_arg,
        new_rowid_req,
        pk_override,
        auto_assigned,
        cache_next_key,
    })
}

fn apply_upsert_write(
    eng: &mut StorageEngine<OsFs>,
    vtab: &AydbVTab,
    mut plan: UpsertWritePlan,
) -> io::Result<i64> {
    let in_place_same_pk =
        !plan.is_insert && plan.old_pk_opt.as_deref() == Some(plan.new_pk.as_slice());
    let stale_marker = has_stale_unique_marker(eng, vtab)?;
    let removes_rows = stale_marker
        || plan.pk_conflict_to_delete.is_some()
        || !plan.unique_conflicts.is_empty()
        || plan.rowid_conflict_pk.is_some()
        || plan
            .old_pk_opt
            .as_ref()
            .is_some_and(|old_pk| old_pk.as_slice() != plan.new_pk.as_slice());

    let rid = with_statement_savepoint(eng, vtab.conn_id(), vtab.table_id, removes_rows, |eng| {
        for conflict_pk in plan
            .pk_conflict_to_delete
            .into_iter()
            .chain(plan.unique_conflicts)
            .chain(plan.rowid_conflict_pk)
        {
            let check_syn = needs_synthetic_delete_check(vtab, &conflict_pk);
            eng.delete_row_with_rowid_hint(
                vtab.table_id,
                &conflict_pk,
                vtab.durability,
                check_syn,
            )?;
        }

        // If UPDATE changed the physical PK bytes, delete the old PK row.
        if let Some(old_pk) = plan.old_pk_opt {
            if old_pk.as_slice() != plan.new_pk.as_slice() {
                let check_syn = needs_synthetic_delete_check(vtab, &old_pk);
                eng.delete_row_with_rowid_hint(vtab.table_id, &old_pk, vtab.durability, check_syn)?;
                if plan.same_pk {
                    plan.known_absent = true;
                }
            }
        }

        let (rid, extra_records) = if let Some(auto_id) = plan.pk_override {
            (auto_id, Vec::new())
        } else if let Some(int_id) = plan.coerced_int_pk {
            (int_id, Vec::new())
        } else if let Some(rowid) = plan.mapped_rowid.or(plan.kept_rowid) {
            let extra = eng.plan_set_rowid(vtab.table_id, &plan.new_pk, rowid)?;
            (rowid, extra)
        } else if in_place_same_pk {
            if let Some(old_rid) = plan.old_rowid_arg {
                (old_rid, Vec::new())
            } else {
                rowid_map::plan_synthetic_rowid(
                    eng,
                    vtab.table_id,
                    &plan.new_pk,
                    vtab.pk_rowid_alias,
                )?
            }
        } else {
            rowid_map::plan_synthetic_rowid(eng, vtab.table_id, &plan.new_pk, vtab.pk_rowid_alias)?
        };

        eng.put_row_owned_with_hint_and_extra(
            vtab.table_id,
            &plan.new_pk,
            plan.row_bytes,
            PutRowOptions {
                secondary_keys: &plan.sec_keys,
                extra_records,
                durability: vtab.durability,
                known_absent: plan.known_absent,
            },
        )?;
        invalidate_stale_unique_marker_if_needed(eng, vtab)?;
        Ok(rid)
    })?;

    if let (Some(next), Ok(mut cache)) = (plan.cache_next_key, vtab.auto_key_cache.lock()) {
        // After this insert's own writes: anything touching this table later moves its epoch.
        *cache = Some((eng.table_mutation_epoch(vtab.table_id), next));
    }
    Ok(rid)
}

fn execute_upsert(
    eng: &mut StorageEngine<OsFs>,
    vtab: &AydbVTab,
    args: &[SqliteValueRef<'_>],
    p_rowid: *mut sqlite3_int64,
) -> c_int {
    let is_insert = args.first().is_some_and(|a| a.value_type() == SQLITE_NULL);
    if let Err(rc) = validate_upsert_constraints(vtab, args, is_insert) {
        return rc;
    }
    let rowid_plan = match resolve_upsert_rowid_plan(eng, vtab, args, is_insert) {
        Ok(p) => p,
        Err(rc) => return rc,
    };

    let num_cols = vtab.col_names.len();
    let (new_pk, row_bytes, mut sec_keys) = match unsafe {
        try_encode_sqlite_values_direct(
            args[2..].as_ptr() as *mut *mut sqlite3_value,
            num_cols,
            vtab.pk_col_idx,
            rowid_plan.pk_override,
            &vtab.col_affinities,
            &vtab.col_collations,
            &vtab.secondary_indexes,
        )
    } {
        Ok(row) => row,
        Err(e) if e.kind() == io::ErrorKind::FileTooLarge => return SQLITE_TOOBIG,
        Err(e) if e.kind() == io::ErrorKind::OutOfMemory => return SQLITE_NOMEM,
        Err(e) => return io_err_to_sqlite_rc(&e),
    };

    let coerced_int_pk = if vtab.pk_rowid_alias {
        decode_i64_tagged_key(&new_pk)
    } else {
        None
    };
    // Only a true `INTEGER PRIMARY KEY` rowid alias derives its rowid from an integer key; every
    // other key (including non-alias `INT PRIMARY KEY`) keeps an independent persistent rowid,
    // where an explicit rowid is stored as requested.
    let mapped_rowid = if coerced_int_pk.is_none() {
        rowid_plan.new_rowid_req
    } else {
        None
    };

    // Resolve `old_pk` if this is an UPDATE (`!is_insert`).
    let mut old_pk_opt: Option<Vec<u8>> = None;
    if !rowid_plan.is_insert {
        if let Some(rid) = rowid_plan.old_rowid_arg {
            let mapped = match resolve_pk_for_rowid(
                vtab,
                rid,
                |t, pk| eng.row_exists(t, pk),
                |t, rid| eng.pk_for_rowid_view(crate::engine::ReadView::Owner, t, rid),
            ) {
                Ok(opt) => opt,
                Err(e) => return io_err_to_sqlite_rc(&e),
            };
            // SQLite collects the rows of a multi-row UPDATE before it applies them, so an earlier
            // row's REPLACE resolution can delete a row that is still queued. A native table skips
            // such a row, and so does this one. The rowid has no row and no identity mapping left,
            // so there is nothing to update; inserting here would resurrect the replaced row.
            let Some(old_pk) = mapped else {
                return SQLITE_OK;
            };
            old_pk_opt = Some(old_pk);
        } else {
            old_pk_opt = Some(unsafe {
                sqlite_value_to_key_bytes_with_schema(
                    args[0].as_ptr(),
                    vtab.pk_affinity,
                    vtab.pk_collation,
                    false,
                )
            });
        }
    }

    // Integer rowid aliases own their exact integer as a rowid; a previously allocated persistent
    // identity owns its number until its row is deleted.
    let rowid_conflict_pk = match find_rowid_conflict(
        eng,
        vtab,
        RowidConflictQuery {
            old_pk_opt: old_pk_opt.as_deref(),
            new_pk: &new_pk,
            coerced_int_pk,
            mapped_rowid,
        },
    ) {
        Ok(conflict) => conflict,
        Err(e) => return io_err_to_sqlite_rc(&e),
    };
    let conflict_mode = vtab.on_conflict_mode();
    if rowid_conflict_pk.is_some() && conflict_mode != SQLITE_REPLACE {
        return SQLITE_CONSTRAINT;
    }

    // Enforce primary key uniqueness & SQLite `ON CONFLICT` resolution.
    let same_pk = match &old_pk_opt {
        Some(old_pk) => {
            old_pk.as_slice() == new_pk.as_slice()
                || build_numeric_alt_key(old_pk.as_slice()).as_deref() == Some(new_pk.as_slice())
        }
        None => false,
    };
    let mut known_absent = rowid_plan.auto_assigned;
    let mut pk_conflict_to_delete: Option<Vec<u8>> = None;
    let unique_probes = if vtab.unique_def.is_empty() {
        Vec::new()
    } else {
        match vtab.unique_def.probes(&row_bytes) {
            Ok(p) => p,
            Err(e) => return io_err_to_sqlite_rc(&e),
        }
    };
    let unique_conflicts = match constraints::find_unique_conflicts(
        eng,
        vtab,
        &unique_probes,
        old_pk_opt.as_deref(),
    ) {
        Ok(conflicts) => conflicts,
        Err(e) => return io_err_to_sqlite_rc(&e),
    };
    if !unique_conflicts.is_empty() && conflict_mode != SQLITE_REPLACE {
        return SQLITE_CONSTRAINT;
    }
    if (rowid_plan.is_insert || !same_pk) && !rowid_plan.auto_assigned {
        let existing_conflict =
            match find_existing_pk_or_numeric_alt(eng, vtab.table_id, &new_pk, vtab.pk_affinity) {
                Ok(opt) => opt,
                Err(e) => return io_err_to_sqlite_rc(&e),
            };
        if let Some(conflict_pk) = existing_conflict {
            if conflict_mode != SQLITE_REPLACE {
                return SQLITE_CONSTRAINT;
            }
            if conflict_pk.as_slice() != new_pk.as_slice() {
                pk_conflict_to_delete = Some(conflict_pk);
                known_absent = true;
            }
        } else {
            known_absent = true;
        }
    }
    if !unique_probes.is_empty() {
        sec_keys.extend(unique_probes.into_iter().map(|p| (p.index_id, p.component)));
    }

    // A row whose key changes keeps its rowid, as on a native table. SQLite addresses the rows it
    // still has queued by their old rowids, so a rowid that moved to another row (or a new one
    // derived from the new key) would make them hit the wrong row.
    let kept_rowid = rowid_plan.old_rowid_arg.filter(|_| {
        old_pk_opt
            .as_ref()
            .is_some_and(|old| old.as_slice() != new_pk.as_slice())
    });

    let rid = match apply_upsert_write(
        eng,
        vtab,
        UpsertWritePlan {
            new_pk,
            row_bytes,
            sec_keys,
            old_pk_opt,
            same_pk,
            known_absent,
            pk_conflict_to_delete,
            unique_conflicts,
            rowid_conflict_pk,
            pk_override: rowid_plan.pk_override,
            coerced_int_pk,
            mapped_rowid,
            kept_rowid,
            is_insert: rowid_plan.is_insert,
            old_rowid_arg: rowid_plan.old_rowid_arg,
            cache_next_key: rowid_plan.cache_next_key,
        },
    ) {
        Ok(rid) => rid,
        Err(e) => return io_err_to_sqlite_rc(&e),
    };
    if !p_rowid.is_null() {
        unsafe { *p_rowid = rid };
    }
    SQLITE_OK
}

pub(in crate::vtab) unsafe extern "system" fn vtab_update(
    p_vtab: *mut sqlite3_vtab,
    argc: c_int,
    argv: *mut *mut sqlite3_value,
    p_rowid: *mut sqlite3_int64,
) -> c_int {
    catch_unwind(AssertUnwindSafe(|| {
        let Some(vtab) = (unsafe { AydbVTab::from_raw(p_vtab) }) else {
            return SQLITE_ERROR;
        };
        let args = unsafe { SqliteValueRef::slice_from_argv(argv, argc) };
        if args.is_empty() {
            return SQLITE_ERROR;
        }
        let mut eng = match vtab.engine.write() {
            Ok(g) => g,
            Err(_) => return SQLITE_ERROR,
        };
        if let Err(e) = eng.check_write_allowed_for_conn(vtab.conn_id()) {
            return io_err_to_sqlite_rc(&e);
        }
        if !eng.is_vtab_token_active(vtab.table_id, vtab.reg_token, vtab.conn_id()) {
            vtab.set_err(STALE_VTAB_ERR);
            return SQLITE_ERROR;
        }
        vtab.pending_ddl.disarm_tx_commit();

        if args.len() == 1 {
            execute_delete(&mut eng, vtab, args[0])
        } else {
            execute_upsert(&mut eng, vtab, args, p_rowid)
        }
    }))
    .unwrap_or(SQLITE_ERROR)
}
