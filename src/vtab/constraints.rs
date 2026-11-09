//! Column uniqueness checks for virtual table writes.

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
    let mut last_pk: Option<Vec<u8>> = None;
    loop {
        let rows = engine.scan_table_range(
            vtab.table_id,
            last_pk.as_deref().map(|pk| (pk, false)),
            None,
            BATCH_SIZE,
        )?;
        if rows.is_empty() {
            break;
        }
        last_pk = rows.last().map(|(pk, _)| pk.clone());
        let last_batch = rows.len() < BATCH_SIZE;
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
        if last_batch {
            break;
        }
    }
    Ok(conflicts)
}
