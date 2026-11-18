//! (Re)building the persistent UNIQUE index of a table whose index does not match its UNIQUE
//! definition: a table that is declared with UNIQUE columns over rows stored without them (or
//! with other collations or affinities), for example when a new schema file attaches existing
//! data.
//!
//! The migration runs in `xBegin`, before the engine transaction of the triggering statement
//! starts, never at connect and never in `xUpdate`. It works in chunks, each under one hold of
//! the engine lock: the rows are read and their entries reconciled in the same hold, so no entry
//! is ever derived from a value that another connection could have replaced meanwhile. Between
//! chunks the lock is free. A writer keeps maintaining its own entries; the migration is
//! idempotent and resumes from a key cursor.
//!
//! A hold lasts for one chunk of buffered writes. None of them waits for the device: the only
//! synchronous write is the completion marker, once, after the last chunk, and the WAL prefix
//! rule makes every earlier chunk durable no later than the marker.

use std::io;
use std::sync::RwLock;

use crate::engine::{ReadView, StorageEngine, WriteDurability};
use crate::pal::PlatformFs;

use super::unique_key::UniqueDef;

/// Rows read and reconciled under one hold of the engine lock.
pub(crate) const CHUNK_ROWS: usize = 512;

/// Rows one `xBegin` migrates at most (eight chunks). A table larger than that is migrated by
/// the following transactions, so the first write after an upgrade waits for a bounded amount of
/// work, a few tens of milliseconds, instead of for the whole table. Until the marker exists the
/// check uses the scan, which is correct at any stage.
pub(crate) const ROWS_PER_BEGIN: usize = 8 * CHUNK_ROWS;

pub(crate) struct Params<'a> {
    pub table_id: u16,
    /// Durability of the completion marker. Chunks are always buffered.
    pub durability: WriteDurability,
    pub def: &'a UniqueDef,
    pub chunk_rows: usize,
    /// Rows to process before returning `Incomplete`.
    pub budget_rows: usize,
}

#[derive(Debug)]
pub(crate) enum Outcome {
    /// The marker for the current definition is committed.
    Complete,
    /// The budget ran out; call again with the same cursor.
    Incomplete,
    /// A transaction is open (this connection's or another's); nothing was changed for it.
    Skipped,
    /// A row or the engine failed. No marker was written.
    Failed(io::Error),
}

/// Runs the migration until the budget is spent. `cursor` is the last primary key done.
pub(crate) fn migrate<FS: PlatformFs>(
    engine: &RwLock<StorageEngine<FS>>,
    p: &Params<'_>,
    cursor: &mut Option<Vec<u8>>,
) -> Outcome {
    let want = p.def.marker_value();
    let mut done = 0usize;
    loop {
        let Ok(mut eng) = engine.write() else {
            return Outcome::Failed(io::Error::other("engine lock poisoned"));
        };
        // Never join a transaction, and never make one wait: with any transaction open the
        // migration stands aside.
        if eng.in_active_transaction() {
            return Outcome::Skipped;
        }
        match eng.unique_marker_view(ReadView::Committed, p.table_id) {
            Ok(Some(found)) if found == want => return Outcome::Complete,
            Ok(Some(_)) => {
                // Built for another definition: stop trusting it before touching the entries.
                if let Err(e) = eng.remove_unique_marker(p.table_id, WriteDurability::BufferedFast)
                {
                    return Outcome::Failed(e);
                }
                *cursor = None;
            }
            Ok(None) => {}
            Err(e) => return Outcome::Failed(e),
        }
        if done >= p.budget_rows {
            return Outcome::Incomplete;
        }
        let batch = eng.scan_table_batch(
            p.table_id,
            cursor.as_deref().map(|pk| (pk, false)),
            None,
            p.chunk_rows,
        );
        let (rows, next) = match batch {
            Ok(batch) => batch,
            Err(e) => return Outcome::Failed(e),
        };
        for (pk, row) in rows {
            let desired = match p.def.entries(&row) {
                Ok(desired) => desired,
                Err(e) => return Outcome::Failed(e),
            };
            if let Err(e) =
                eng.reconcile_unique_keys(p.table_id, &pk, &desired, WriteDurability::BufferedFast)
            {
                return Outcome::Failed(e);
            }
        }
        done += p.chunk_rows;
        match next {
            Some(pk) => *cursor = Some(pk),
            None => {
                *cursor = None;
                if let Err(e) = eng.write_unique_marker(p.table_id, want.clone(), p.durability) {
                    return Outcome::Failed(e);
                }
                return match eng.unique_marker_view(ReadView::Committed, p.table_id) {
                    Ok(Some(found)) if found == want => Outcome::Complete,
                    Ok(_) => Outcome::Failed(io::Error::other("UNIQUE index marker not visible")),
                    Err(e) => Outcome::Failed(e),
                };
            }
        }
        // The lock is released here, at the end of the iteration.
    }
}
