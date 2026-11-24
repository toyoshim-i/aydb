//! Column uniqueness checks for virtual table writes.

use super::unique_key::{is_digest_component, unique_key_of_row};
use super::*;

/// Find all existing rows which conflict with the proposed row. Checks precede every
/// mutation so ABORT and IGNORE never leave partially replaced rows behind.
///
/// Once the UNIQUE index is trusted (see [`index_is_trusted`]) the check is one index probe per
/// non-NULL UNIQUE column of the new row (`probes`). Before that, and for a connection whose
/// migration failed, it scans the table, which is correct at any stage. Conflicts are returned in
/// primary-key order either way.
pub(super) fn find_unique_conflicts(
    engine: &mut StorageEngine<OsFs>,
    vtab: &AydbVTab,
    probes: &[unique_key::UniqueProbe],
    old_pk: Option<&[u8]>,
) -> io::Result<Vec<Vec<u8>>> {
    if probes.is_empty() {
        return Ok(Vec::new());
    }
    if index_is_trusted(engine, vtab) {
        find_conflicts_by_index(engine, vtab, probes, old_pk)
    } else {
        find_conflicts_by_scan(engine, vtab, probes, old_pk)
    }
}

/// True when every row's UNIQUE entries are complete: the connection latched on the current
/// committed marker generation, or the marker for this definition is committed (another connection
/// finished the migration). When an uncommitted table is created inside an active transaction, its
/// empty-table marker is visible in `ReadView::Owner` and trusted for that transaction without
/// latching `unique_ready_gen` prematurely (so a `ROLLBACK` never leaves a stale latch). A
/// connection whose own migration failed stays on the scan.
fn index_is_trusted(engine: &StorageEngine<OsFs>, vtab: &AydbVTab) -> bool {
    let want = vtab.unique_def.marker_value();
    if engine.conn_has_open_transaction(vtab.db as usize) && engine.unique_marker_modified_in_tx() {
        return engine
            .unique_marker_view(crate::engine::ReadView::Owner, vtab.table_id)
            .ok()
            .flatten()
            .is_some_and(|found| found == want);
    }
    let marker_gen = engine.unique_marker_gen();
    if marker_gen != 0 && vtab.unique_ready_gen.load(Ordering::Acquire) == marker_gen {
        return true;
    }
    if vtab.unique_migration_failed.load(Ordering::Relaxed) {
        return false;
    }
    let marked = engine
        .unique_marker_view(crate::engine::ReadView::Committed, vtab.table_id)
        .ok()
        .flatten()
        .is_some_and(|found| found == want);
    if marked {
        if !engine.unique_marker_modified_in_tx() && marker_gen != 0 {
            vtab.unique_ready_gen.store(marker_gen, Ordering::Release);
        }
        return true;
    }
    engine.in_active_transaction()
        && engine
            .unique_marker_view(crate::engine::ReadView::Owner, vtab.table_id)
            .ok()
            .flatten()
            .is_some_and(|found| found == want)
}

/// One probe per non-NULL UNIQUE column in the owner view, which includes the writer's own
/// uncommitted entries and tombstones. A hit whose row no longer exists is ignored. A hit on a
/// digest component is only a candidate: its row is read and the real key compared, and an
/// unreadable row is an error under `on_corrupt=error` and no conflict under `evict`. Hits on
/// full keys are exact and never read the row.
fn find_conflicts_by_index(
    engine: &mut StorageEngine<OsFs>,
    vtab: &AydbVTab,
    probes: &[unique_key::UniqueProbe],
    old_pk: Option<&[u8]>,
) -> io::Result<Vec<Vec<u8>>> {
    let mut conflicts = std::collections::BTreeSet::new();
    for probe in probes {
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
                let Some(existing) = engine.get_row_untracked(vtab.table_id, &pk)? else {
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
    probes: &[unique_key::UniqueProbe],
    old_pk: Option<&[u8]>,
) -> io::Result<Vec<Vec<u8>>> {
    const BATCH_SIZE: usize = 128;

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
            for probe in probes {
                let existing = unique_key_of_row(&existing_row, probe.col, probe.collation)?;
                if existing.as_deref() == Some(probe.key.as_slice()) {
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
