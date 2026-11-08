//! Transaction & savepoint state machine, undo logs, and cross-connection snapshot isolation.

use std::collections::{BTreeMap, HashMap};
use std::io;
use std::sync::Arc;

use crate::lru::CappedTableState;
use crate::pal::PlatformFs;
use crate::wal::WalBatch;

use super::{StorageEngine, WriteDurability};

#[derive(Clone, Default)]
pub(crate) struct TxMetaSnapshot {
    pub(super) next_lsn: u64,
    pub(super) memtable_highest_lsn: u64,
    pub(super) std_blobs: BTreeMap<u64, (u64, u64)>,
    pub(super) lru_blobs: BTreeMap<(u16, u64), (u64, u64)>,
    pub(super) capped_tables: HashMap<u16, CappedTableState>,
    pub(super) active_vtab_ids: HashMap<u16, Arc<str>>,
    pub(super) table_has_row_meta: BTreeMap<u16, bool>,
    pub(super) dropped_tables_len: usize,
    pub(super) quotas_modified_in_tx: bool,
}

#[derive(Clone)]
pub(crate) struct SavepointMarker {
    pub(super) sp_id: i32,
    pub(super) buf_offset: usize,
    pub(super) record_count: u32,
    pub(super) pending_tx_bytes: usize,
    pub(super) pending_tx_sync: bool,
    pub(super) implicit_tx: bool,
    pub(super) undo_len: usize,
    pub(super) meta_snapshot: TxMetaSnapshot,
}

impl<FS: PlatformFs> StorageEngine<FS> {
    /// Returns `true` if an explicit or savepoint-backed transaction is currently active.
    #[inline]
    pub fn in_active_transaction(&self) -> bool {
        self.tx_depth > 0
    }

    /// Begins an explicit batch transaction (`BEGIN`). Subsequent mutations apply immediately to
    /// the in-memory `MemTable` (for read-your-own-writes visibility) while coalescing their
    /// [`crate::wal::WalRecord`] entries into a single atomic [`WalBatch`] flushed on [`Self::commit_transaction`].
    pub fn begin_transaction(&mut self) {
        if self.tx_depth == 0 {
            self.tx_memtable_undo.clear();
            self.dropped_tables_in_tx.clear();
            self.quotas_modified_in_tx = false;
            self.tx_start_meta = self.capture_tx_meta_snapshot();
            self.tx_owner_conn = None;
            self.deferred_flush_on_commit = false;
            self.deferred_compact_on_commit = false;
            self.tx_prepared = false;
            self.tx_preparing = false;
            self.pending_tx_bytes = 0;
        }
        self.tx_depth += 1;
    }

    /// Begins an explicit transaction owned by SQLite connection `conn_id`.
    /// Returns `io::ErrorKind::WouldBlock` (`SQLITE_BUSY`) if another connection already holds
    /// an active transaction on this `StorageEngine`.
    pub fn begin_transaction_for_conn(&mut self, conn_id: usize) -> io::Result<()> {
        self.check_not_poisoned()?;
        if self.tx_depth > 0 && self.tx_owner_conn != Some(conn_id) {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "database is locked by another connection's active transaction",
            ));
        }
        if self.tx_depth == 0 {
            self.tx_memtable_undo.clear();
            self.dropped_tables_in_tx.clear();
            self.quotas_modified_in_tx = false;
            self.tx_start_meta = self.capture_tx_meta_snapshot();
            self.tx_owner_conn = Some(conn_id);
            self.deferred_flush_on_commit = false;
            self.deferred_compact_on_commit = false;
            self.tx_prepared = false;
            self.tx_preparing = false;
            self.pending_tx_bytes = 0;
        }
        self.tx_depth += 1;
        Ok(())
    }

    /// Checks whether `conn_id` is permitted to mutate this `StorageEngine`.
    /// Returns `io::ErrorKind::WouldBlock` (`SQLITE_BUSY`) if another connection holds an active transaction.
    #[inline]
    pub fn check_write_allowed_for_conn(&self, conn_id: usize) -> io::Result<()> {
        if self.tx_depth > 0 && self.tx_owner_conn != Some(conn_id) {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "database is locked by another connection's active transaction",
            ));
        }
        Ok(())
    }

    /// Executes a read closure `f` for `conn_id` against the committed snapshot.
    /// If `conn_id` owns the active transaction (or no transaction is active), `f` sees the live
    /// `MemTable` (read-your-own-writes). If another connection owns an active transaction,
    /// uncommitted `MemTable` mutations are temporarily hidden so `conn_id` sees only committed data.
    pub fn with_committed_read_for_conn<F, R>(&mut self, conn_id: usize, f: F) -> R
    where
        F: FnOnce(&mut Self) -> R,
    {
        if self.tx_depth == 0 || self.tx_owner_conn == Some(conn_id) {
            return f(self);
        }

        struct CommittedReadGuard<'a, FS: PlatformFs> {
            eng: &'a mut StorageEngine<FS>,
            uncommitted_redo: Vec<(Vec<u8>, Option<crate::memtable::MemEntry>)>,
            uncommitted_highest_lsn: u64,
            prev_in_view: bool,
        }

        impl<'a, FS: PlatformFs> Drop for CommittedReadGuard<'a, FS> {
            fn drop(&mut self) {
                while let Some((key, entry)) = self.uncommitted_redo.pop() {
                    self.eng.memtable.restore_entry(key, entry);
                }
                self.eng
                    .memtable
                    .set_highest_lsn(self.uncommitted_highest_lsn);
                self.eng.in_committed_read_view = self.prev_in_view;
            }
        }

        let prev_in_view = self.in_committed_read_view;
        self.in_committed_read_view = true;
        let uncommitted_highest_lsn = self.memtable.highest_lsn();
        let committed_highest_lsn = self.tx_start_meta.memtable_highest_lsn;
        let mut uncommitted_redo = Vec::with_capacity(self.tx_memtable_undo.len());
        for i in (0..self.tx_memtable_undo.len()).rev() {
            let key = self.tx_memtable_undo[i].0.clone();
            let prev_entry = self.tx_memtable_undo[i].1.clone();
            let cur_entry = self.memtable.get(&key).cloned();
            self.memtable.restore_entry(key.clone(), prev_entry);
            uncommitted_redo.push((key, cur_entry));
        }
        self.memtable.set_highest_lsn(committed_highest_lsn);

        let guard = CommittedReadGuard {
            eng: self,
            uncommitted_redo,
            uncommitted_highest_lsn,
            prev_in_view,
        };
        f(guard.eng)
    }

    /// Prepares the active transaction for commit (`xSync` two-phase commit phase 1).
    /// Runs deferred capped-table quota evictions inside the undoable transaction context,
    /// flushes/syncs out-of-line blob segments, and pre-checks WAL capacity so any fallible I/O
    /// or space errors surface during `xSync` while `xRollback` can still cleanly undo all state.
    pub fn prepare_commit_transaction(&mut self, durability: WriteDurability) -> io::Result<()> {
        if self.tx_depth == 0 {
            return Ok(());
        }
        if durability == WriteDurability::SyncTransactional {
            self.pending_tx_sync = true;
        }
        let effective_durability = if self.pending_tx_sync {
            WriteDurability::SyncTransactional
        } else {
            WriteDurability::BufferedFast
        };

        // 1. Run capped-table quota evictions while `tx_depth > 0` so eviction mutations are
        //    recorded in `pending_tx_buf` and `tx_memtable_undo` (fully undoable on `xRollback`!).
        self.tx_preparing = true;
        let capped_table_ids: Vec<u16> = self.capped_tables.keys().copied().collect();
        for tid in capped_table_ids {
            if let Err(e) = self.evict_until_within_quota(tid, effective_durability) {
                self.tx_preparing = false;
                return Err(e);
            }
        }
        self.tx_preparing = false;

        // 2. Flush/sync active blob writers (`Write-Blob-Before-WAL` & prefix durability).
        self.flush_active_blob_buffers()?;
        if effective_durability == WriteDurability::SyncTransactional {
            self.sync_active_blob_writers()?;
            if self.last_buf_append_lsn > self.last_buf_synced_lsn {
                self.make_buf_wal_durable(false)?;
            }
        }

        // 3. Pre-verify/grow WAL capacity if this is a multi-chunk transaction (> 512 KB).
        const MAX_WAL_CHUNK_BYTES: usize = 512 * 1024;
        if self.pending_tx_buf.len() > MAX_WAL_CHUNK_BYTES {
            let required_sectors = (self.pending_tx_buf.len() / 4070) + 8;
            match effective_durability {
                WriteDurability::SyncTransactional => {
                    let target_sectors = self.sync_wal.current_sector_idx() + required_sectors;
                    if target_sectors > self.sync_wal.total_sectors() {
                        self.sync_wal.ensure_min_size(
                            (target_sectors * crate::wal::WAL_SECTOR_SIZE) as u64,
                        )?;
                    }
                }
                WriteDurability::BufferedFast => {
                    let target_sectors = self.buf_wal.current_sector_idx() + required_sectors;
                    if target_sectors > self.buf_wal.total_sectors() {
                        self.buf_wal.ensure_min_size(
                            (target_sectors * crate::wal::WAL_SECTOR_SIZE) as u64,
                        )?;
                    }
                }
            }
        }

        self.tx_prepared = true;
        Ok(())
    }

    /// Commits an explicit batch transaction (`COMMIT`), writing all coalesced [`crate::wal::WalRecord`]
    /// entries in a single [`WalBatch`] syscall (or chunked batches if exceeding 512 KB)!
    pub fn commit_transaction(&mut self) -> io::Result<()> {
        if self.tx_depth == 1 && !self.tx_prepared {
            let durability = if self.pending_tx_sync {
                WriteDurability::SyncTransactional
            } else {
                WriteDurability::BufferedFast
            };
            if let Err(e) = self.prepare_commit_transaction(durability) {
                let _ = self.rollback_transaction();
                return Err(e);
            }
        }
        if self.tx_depth > 0 {
            self.tx_depth -= 1;
        }
        if self.tx_depth == 0 {
            if let Err(e) = self.flush_pending_tx_records() {
                self.tx_depth = 1;
                let _ = self.rollback_transaction();
                return Err(e);
            }
            self.tx_owner_conn = None;
            self.tx_prepared = false;
            self.tx_preparing = false;
            self.pending_tx_bytes = 0;
            self.savepoints.clear();
            self.tx_memtable_undo.clear();
            self.tx_start_meta = TxMetaSnapshot::default();

            let capped_table_ids: Vec<u16> = self.capped_tables.keys().copied().collect();
            for tid in capped_table_ids {
                self.reclaim_fully_dead_lru_blob_segments(tid)?;
            }

            let dropped_tables = core::mem::take(&mut self.dropped_tables_in_tx);
            let quotas_modified = core::mem::replace(&mut self.quotas_modified_in_tx, false);
            let mut dropped_any_capped = false;
            for (tid, was_capped) in dropped_tables {
                self.reclaim_fully_dead_lru_blob_segments(tid)?;
                if was_capped {
                    dropped_any_capped = true;
                }
            }
            let do_compact = core::mem::replace(&mut self.deferred_compact_on_commit, false);
            let do_flush = core::mem::replace(&mut self.deferred_flush_on_commit, false);
            let will_flush_or_compact = do_compact || do_flush || self.memtable.should_flush();
            if (dropped_any_capped || quotas_modified) && !will_flush_or_compact {
                let quota_metas = self.build_quota_metas();
                self.meta_mgr.commit_update(|st| {
                    st.table_quotas = quota_metas;
                })?;
            }

            if do_compact {
                self.compact()?;
            } else if do_flush || self.memtable.should_flush() {
                self.flush_memtable()?;
            }
        }
        Ok(())
    }

    /// Commits an explicit transaction for SQLite connection `conn_id`.
    pub fn commit_transaction_for_conn(&mut self, conn_id: usize) -> io::Result<()> {
        self.check_write_allowed_for_conn(conn_id)?;
        self.commit_transaction()
    }

    /// Aborts an explicit batch transaction (`ROLLBACK`), discarding all buffered `pending_tx_buf`
    /// mutations and restoring `MemTable`, `capped_tables`, and blob `dead_bytes` in
    /// $O(\text{tx mutations})$ time via the in-memory undo log.
    pub fn rollback_transaction(&mut self) -> io::Result<()> {
        self.tx_depth = 0;
        self.tx_owner_conn = None;
        self.deferred_flush_on_commit = false;
        self.deferred_compact_on_commit = false;
        self.tx_prepared = false;
        self.tx_preparing = false;
        self.savepoints.clear();
        self.pending_tx_record_count = 0;
        self.pending_tx_bytes = 0;
        self.pending_tx_buf.clear();
        self.pending_tx_sync = false;
        let snap = core::mem::take(&mut self.tx_start_meta);
        self.restore_tx_state(0, &snap);
        Ok(())
    }

    /// Aborts an explicit transaction for SQLite connection `conn_id`.
    pub fn rollback_transaction_for_conn(&mut self, conn_id: usize) -> io::Result<()> {
        self.check_write_allowed_for_conn(conn_id)?;
        self.rollback_transaction()
    }

    /// Opens or updates a transaction savepoint (`xSavepoint`).
    pub fn savepoint(&mut self, sp_id: i32) {
        self.tx_prepared = false;
        let mut implicit_tx = false;
        if self.tx_depth == 0 {
            self.begin_transaction();
            implicit_tx = true;
        }
        if let Some(pos) = self.savepoints.iter().position(|s| s.sp_id == sp_id) {
            implicit_tx = self.savepoints[pos].implicit_tx;
            self.savepoints.truncate(pos);
        }
        let buf_offset = if self.pending_tx_record_count == 0 {
            0
        } else {
            self.pending_tx_buf.len()
        };
        let meta_snapshot = self.capture_tx_meta_snapshot();
        self.savepoints.push(SavepointMarker {
            sp_id,
            buf_offset,
            record_count: self.pending_tx_record_count,
            pending_tx_bytes: self.pending_tx_bytes,
            pending_tx_sync: self.pending_tx_sync,
            implicit_tx,
            undo_len: self.tx_memtable_undo.len(),
            meta_snapshot,
        });
    }

    /// Opens or updates a transaction savepoint for SQLite connection `conn_id`.
    pub fn savepoint_for_conn(&mut self, conn_id: usize, sp_id: i32) -> io::Result<()> {
        self.check_write_allowed_for_conn(conn_id)?;
        let was_zero = self.tx_depth == 0;
        self.savepoint(sp_id);
        if was_zero {
            self.tx_owner_conn = Some(conn_id);
        }
        Ok(())
    }

    /// Releases savepoints `>= sp_id` (`xRelease`).
    pub fn release_savepoint(&mut self, sp_id: i32) -> io::Result<()> {
        if let Some(pos) = self.savepoints.iter().position(|s| s.sp_id == sp_id) {
            let implicit_tx = self.savepoints[pos].implicit_tx;
            self.savepoints.truncate(pos);
            if implicit_tx && self.tx_depth == 1 {
                self.commit_transaction()?;
            }
        }
        Ok(())
    }

    /// Releases savepoints `>= sp_id` for SQLite connection `conn_id`.
    pub fn release_savepoint_for_conn(&mut self, conn_id: usize, sp_id: i32) -> io::Result<()> {
        self.check_write_allowed_for_conn(conn_id)?;
        self.release_savepoint(sp_id)
    }

    /// Rolls back all mutations since savepoint `sp_id` (`xRollbackTo`) in
    /// $O(\text{mutations since savepoint})$ time via the undo log.
    pub fn rollback_to_savepoint(&mut self, sp_id: i32) -> io::Result<()> {
        self.tx_prepared = false;
        let Some(pos) = self.savepoints.iter().position(|s| s.sp_id == sp_id) else {
            return Ok(());
        };
        let marker = self.savepoints[pos].clone();
        self.savepoints.truncate(pos + 1);
        if marker.record_count == 0 {
            self.pending_tx_buf.clear();
        } else {
            self.pending_tx_buf.truncate(marker.buf_offset);
        }
        self.pending_tx_record_count = marker.record_count;
        self.pending_tx_bytes = marker.pending_tx_bytes;
        self.pending_tx_sync = marker.pending_tx_sync;
        self.restore_tx_state(marker.undo_len, &marker.meta_snapshot);
        Ok(())
    }

    /// Rolls back to savepoint `sp_id` for SQLite connection `conn_id`.
    pub fn rollback_to_savepoint_for_conn(&mut self, conn_id: usize, sp_id: i32) -> io::Result<()> {
        self.check_write_allowed_for_conn(conn_id)?;
        self.rollback_to_savepoint(sp_id)
    }

    pub(super) fn capture_tx_meta_snapshot(&self) -> TxMetaSnapshot {
        TxMetaSnapshot {
            next_lsn: self.next_lsn,
            memtable_highest_lsn: self.memtable.highest_lsn(),
            std_blobs: self
                .std_blobs
                .iter()
                .map(|(&seq, b)| (seq, (b.file_len, b.dead_bytes)))
                .collect(),
            lru_blobs: self
                .lru_blobs
                .iter()
                .map(|(&k, b)| (k, (b.file_len, b.dead_bytes)))
                .collect(),
            capped_tables: self.capped_tables.clone(),
            active_vtab_ids: self.active_vtab_ids.clone(),
            table_has_row_meta: self.table_has_row_meta.clone(),
            dropped_tables_len: self.dropped_tables_in_tx.len(),
            quotas_modified_in_tx: self.quotas_modified_in_tx,
        }
    }

    pub(super) fn restore_tx_state(&mut self, undo_target_len: usize, snap: &TxMetaSnapshot) {
        while self.tx_memtable_undo.len() > undo_target_len {
            let (key, prev_entry) = self.tx_memtable_undo.pop().unwrap();
            self.memtable.restore_entry(key, prev_entry);
        }
        self.memtable.set_highest_lsn(snap.memtable_highest_lsn);
        self.next_lsn = snap.next_lsn;
        for (&seq, b) in &mut self.std_blobs {
            let (snap_len, snap_dead) = snap.std_blobs.get(&seq).copied().unwrap_or((0, 0));
            let aborted = b.file_len.saturating_sub(snap_len);
            b.dead_bytes = snap_dead.saturating_add(aborted);
        }
        for (&k, b) in &mut self.lru_blobs {
            let (snap_len, snap_dead) = snap.lru_blobs.get(&k).copied().unwrap_or((0, 0));
            let aborted = b.file_len.saturating_sub(snap_len);
            b.dead_bytes = snap_dead.saturating_add(aborted);
        }
        self.capped_tables = snap.capped_tables.clone();
        self.active_vtab_ids = snap.active_vtab_ids.clone();
        self.table_has_row_meta = snap.table_has_row_meta.clone();
        self.dropped_tables_in_tx.truncate(snap.dropped_tables_len);
        self.quotas_modified_in_tx = snap.quotas_modified_in_tx;
    }

    pub(super) fn flush_pending_tx_records(&mut self) -> io::Result<()> {
        if self.pending_tx_record_count == 0 {
            return Ok(());
        }
        self.check_not_poisoned()?;
        let record_count = self.pending_tx_record_count;
        self.pending_tx_record_count = 0;
        let durability = if self.pending_tx_sync {
            WriteDurability::SyncTransactional
        } else {
            WriteDurability::BufferedFast
        };
        self.pending_tx_sync = false;

        if durability == WriteDurability::SyncTransactional {
            self.sync_active_blob_writers()?;
        } else {
            self.flush_active_blob_buffers()?;
        }

        const MAX_WAL_CHUNK_BYTES: usize = 512 * 1024;
        if self.pending_tx_buf.len() <= MAX_WAL_CHUNK_BYTES {
            let lsn = self.next_lsn;
            self.next_lsn += 1;
            WalBatch::finalize_encoded_buf(lsn, record_count, &mut self.pending_tx_buf);
            let mut raw_buf = core::mem::take(&mut self.pending_tx_buf);
            let res = self.append_raw_wal_batch_with_rotation(&raw_buf, durability);
            raw_buf.clear();
            self.pending_tx_buf = raw_buf;
            return res;
        }

        // Large transaction buffer (> 512 KB): chunk serialized records into <= 512 KB WalBatches
        // framed by `WalRecord::TxBegin` in the first chunk and `WalRecord::TxCommit` in the last
        // chunk. Ensure the target WAL file has enough capacity so all chunks of the
        // transaction remain in the same WAL epoch without flushing uncommitted `MemTable` state.
        let mut raw_buf = core::mem::take(&mut self.pending_tx_buf);
        let mut cursor = 16usize;
        let total_len = raw_buf.len();
        let mut chunks: Vec<(Vec<u8>, u32)> = Vec::new();
        let mut chunk_buf = Vec::with_capacity(MAX_WAL_CHUNK_BYTES + 4096);
        let mut chunk_records = 0u32;

        while cursor < total_len {
            if chunk_records == 0 {
                chunk_buf.clear();
                chunk_buf.extend_from_slice(&[0u8; 16]);
                if chunks.is_empty() {
                    // Prepend `WalRecord::TxBegin` (`4u8`) to the first chunk.
                    chunk_buf.push(4u8);
                    chunk_records += 1;
                }
            }
            let rec_start = cursor;
            let op = raw_buf[cursor];
            cursor += 1;
            match op {
                1 => {
                    let klen =
                        u32::from_le_bytes(raw_buf[cursor..cursor + 4].try_into().unwrap())
                            as usize;
                    cursor += 4 + klen;
                    let vlen =
                        u32::from_le_bytes(raw_buf[cursor..cursor + 4].try_into().unwrap())
                            as usize;
                    cursor += 4 + vlen;
                }
                2 => {
                    let klen =
                        u32::from_le_bytes(raw_buf[cursor..cursor + 4].try_into().unwrap())
                            as usize;
                    cursor += 4 + klen + 23;
                }
                3 => {
                    let klen =
                        u32::from_le_bytes(raw_buf[cursor..cursor + 4].try_into().unwrap())
                            as usize;
                    cursor += 4 + klen;
                }
                4 | 5 => {}
                _ => unreachable!("invalid op in pending_tx_buf"),
            }
            let rec_slice = &raw_buf[rec_start..cursor];
            let min_records_before_split = if chunks.is_empty() { 2 } else { 1 };
            if chunk_records >= min_records_before_split
                && chunk_buf.len() + rec_slice.len() > MAX_WAL_CHUNK_BYTES
            {
                chunks.push((core::mem::take(&mut chunk_buf), chunk_records));
                chunk_buf = Vec::with_capacity(MAX_WAL_CHUNK_BYTES + 4096);
                chunk_buf.extend_from_slice(&[0u8; 16]);
                chunk_records = 0;
            }
            chunk_buf.extend_from_slice(rec_slice);
            chunk_records += 1;
        }

        if chunk_records > 0 {
            chunks.push((chunk_buf, chunk_records));
        }

        raw_buf.clear();
        self.pending_tx_buf = raw_buf;

        if chunks.len() == 1 {
            let (mut single_buf, mut single_records) = chunks.pop().unwrap();
            // Strip the leading `TxBegin` (`4u8` at index 16) since it fit in a single chunk
            single_buf.remove(16);
            single_records -= 1;
            let lsn = self.next_lsn;
            self.next_lsn += 1;
            WalBatch::finalize_encoded_buf(lsn, single_records, &mut single_buf);
            return self.append_raw_wal_batch_with_rotation(&single_buf, durability);
        }

        if let Some((last_buf, last_records)) = chunks.last_mut() {
            // Append `WalRecord::TxCommit` (`5u8`) to the final chunk.
            last_buf.push(5u8);
            *last_records += 1;
        }

        // Ensure the target WAL stream has sufficient sectors to fit all chunks in the current epoch
        let mut required_sectors = 0usize;
        for (cbuf, _) in &chunks {
            required_sectors += (cbuf.len() / 4070) + 2;
        }
        match durability {
            WriteDurability::SyncTransactional => {
                let target_sectors = self.sync_wal.current_sector_idx() + required_sectors;
                if target_sectors > self.sync_wal.total_sectors() {
                    self.sync_wal
                        .ensure_min_size((target_sectors * crate::wal::WAL_SECTOR_SIZE) as u64)?;
                }
            }
            WriteDurability::BufferedFast => {
                let target_sectors = self.buf_wal.current_sector_idx() + required_sectors;
                if target_sectors > self.buf_wal.total_sectors() {
                    self.buf_wal
                        .ensure_min_size((target_sectors * crate::wal::WAL_SECTOR_SIZE) as u64)?;
                }
            }
        }

        for (mut cbuf, crec) in chunks {
            let lsn = self.next_lsn;
            self.next_lsn += 1;
            WalBatch::finalize_encoded_buf(lsn, crec, &mut cbuf);
            self.append_raw_wal_batch_with_rotation(&cbuf, durability)?;
        }

        Ok(())
    }
}
