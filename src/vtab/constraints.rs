//! Column uniqueness checks for virtual table writes.

use super::unique_key::{unique_index_component, unique_index_id, unique_key_of_row};
use super::*;

/// Find all existing rows which conflict with the proposed row. Checks precede every
/// mutation so ABORT and IGNORE never leave partially replaced rows behind.
pub(super) fn find_unique_conflicts(
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
/// row bytes. NULL has no entry. Columns past `0x7FFF` cannot be given an index id and are
/// rejected (SQLite's own column limit is far lower).
pub(super) fn push_unique_entries(
    vtab: &AydbVTab,
    row: &[u8],
    sec_keys: &mut Vec<(u16, Vec<u8>)>,
) -> io::Result<()> {
    for (index, &unique) in vtab.col_unique.iter().enumerate() {
        if !unique {
            continue;
        }
        if index >= 0x8000 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "too many columns for a UNIQUE index",
            ));
        }
        if let Some(key) = unique_key_of_row(row, index, vtab.col_collations[index])? {
            sec_keys.push((unique_index_id(index), unique_index_component(&key)));
        }
    }
    Ok(())
}
