//! Column uniqueness checks for virtual table writes.

use super::unique_key::{is_digest_component, unique_key_of_row};
use super::*;

/// Find all existing rows which conflict with the proposed row. Checks precede every
/// mutation so ABORT and IGNORE never leave partially replaced rows behind.
///
/// Once the UNIQUE index is trusted (see [`index_is_trusted`]) the check is one index probe per
/// non-NULL UNIQUE column of the new row. Before that, and for a connection whose migration
/// failed, it scans the table, which is correct at any stage. Conflicts are returned in
/// primary-key order either way.
pub(super) fn find_unique_conflicts(
    engine: &mut StorageEngine<OsFs>,
    vtab: &AydbVTab,
    row: &[u8],
    old_pk: Option<&[u8]>,
) -> io::Result<Vec<Vec<u8>>> {
    if vtab.unique_def.is_empty() {
        return Ok(Vec::new());
    }
    if index_is_trusted(engine, vtab) {
        find_conflicts_by_index(engine, vtab, row, old_pk)
    } else {
        find_conflicts_by_scan(engine, vtab, row, old_pk)
    }
}

/// True when every row's UNIQUE entries are complete: the connection latched on the marker, or
/// the marker for this definition is committed (another connection finished the migration).
/// The marker is written outside any transaction, so the committed view is the writer's view
/// too, and a rollback can never remove it. A connection whose own migration failed stays on
/// the scan.
fn index_is_trusted(engine: &StorageEngine<OsFs>, vtab: &AydbVTab) -> bool {
    if vtab.unique_ready.load(Ordering::Acquire) {
        return true;
    }
    if vtab.unique_migration_failed.load(Ordering::Relaxed) {
        return false;
    }
    let marked = engine
        .unique_marker_view(crate::engine::ReadView::Committed, vtab.table_id)
        .ok()
        .flatten()
        .is_some_and(|found| found == vtab.unique_def.marker_value());
    if marked {
        vtab.unique_ready.store(true, Ordering::Release);
    }
    marked
}

/// One probe per non-NULL UNIQUE column in the owner view, which includes the writer's own
/// uncommitted entries and tombstones. A hit whose row no longer exists is ignored. A hit on a
/// digest component is only a candidate: its row is read and the real key compared, and an
/// unreadable row is an error under `on_corrupt=error` and no conflict under `evict`. Hits on
/// full keys are exact and never read the row.
fn find_conflicts_by_index(
    engine: &mut StorageEngine<OsFs>,
    vtab: &AydbVTab,
    row: &[u8],
    old_pk: Option<&[u8]>,
) -> io::Result<Vec<Vec<u8>>> {
    let mut conflicts = std::collections::BTreeSet::new();
    for probe in vtab.unique_def.probes(row)? {
        let digest = is_digest_component(&probe.component);
        for pk in
            engine.lookup_by_secondary_index(vtab.table_id, probe.index_id, &probe.component)?
        {
            if old_pk == Some(pk.as_slice()) || conflicts.contains(&pk) {
                continue;
            }
            if !engine.row_exists(vtab.table_id, &pk)? {
                continue;
            }
            if digest {
                let Some(existing) = engine.get_row(vtab.table_id, &pk)? else {
                    continue;
                };
                let existing_key = unique_key_of_row(&existing, probe.col, probe.collation)?;
                if existing_key.as_deref() != Some(probe.key.as_slice()) {
                    continue;
                }
            }
            conflicts.insert(pk);
        }
    }
    Ok(conflicts.into_iter().collect())
}

fn find_conflicts_by_scan(
    engine: &mut StorageEngine<OsFs>,
    vtab: &AydbVTab,
    row: &[u8],
    old_pk: Option<&[u8]>,
) -> io::Result<Vec<Vec<u8>>> {
    const BATCH_SIZE: usize = 128;
    let mut keys = Vec::new();
    for (index, &unique) in vtab.col_unique.iter().enumerate() {
        if unique {
            let key =
                extract_column_key_bytes_with_collation(row, index, vtab.col_collations[index])?;
            if key.first() != Some(&KEY_TAG_NULL) {
                keys.push((index, key));
            }
        }
    }
    if keys.is_empty() {
        return Ok(Vec::new());
    }

    let mut conflicts = Vec::new();
    let mut cursor: Option<Vec<u8>> = None;
    loop {
        // Continue after the last key the engine examined; a batch with fewer rows than
        // `BATCH_SIZE` (corrupt rows skipped under the evict policy) does not end the scan.
        let (rows, next) = engine.scan_table_batch(
            vtab.table_id,
            cursor.as_deref().map(|pk| (pk, false)),
            None,
            BATCH_SIZE,
        )?;
        for (pk, existing_row) in rows {
            if old_pk == Some(pk.as_slice()) {
                continue;
            }
            for (index, key) in &keys {
                let existing = extract_column_key_bytes_with_collation(
                    &existing_row,
                    *index,
                    vtab.col_collations[*index],
                )?;
                if existing == *key
                    || build_numeric_alt_key(&existing).as_ref() == Some(key)
                    || build_numeric_alt_key(key).as_ref() == Some(&existing)
                {
                    conflicts.push(pk);
                    break;
                }
            }
        }
        match next {
            Some(pk) => cursor = Some(pk),
            None => break,
        }
    }
    Ok(conflicts)
}

/// Adds the UNIQUE index entry of every `UNIQUE` column to `sec_keys`, derived from the finished
/// row bytes (`UniqueDef::entries`).
pub(super) fn push_unique_entries(
    vtab: &AydbVTab,
    row: &[u8],
    sec_keys: &mut Vec<(u16, Vec<u8>)>,
) -> io::Result<()> {
    sec_keys.extend(vtab.unique_def.entries(row)?);
    Ok(())
}
