//! Capped Table (`MAX_BYTES`) quota management, Coalesced Lazy Promotion, cascading LRU eviction,
//! and post-recovery byte/tick reconciliation.

use std::collections::HashMap;
use std::io;

use crate::blob::BLOB_RECORD_HEADER_LEN;
use crate::index::{IndexEntry, IndexValue};
use crate::lru::{
    CappedTableState, NS_LRU_QUEUE, NS_PRIMARY_ROW, NS_ROW_META, RowMetaRecord,
    decode_lru_queue_key, decode_primary_row_key, encode_lru_queue_key, encode_namespace_prefix,
    encode_primary_row_key, encode_row_meta_key,
};
use crate::meta::{EvictionPolicy, TableQuotaMeta};
use crate::pal::PlatformFs;
use crate::wal::{BlobKind, BlobPtr, WalRecord};

use super::reader::ReadView;
use super::scan::{MergeScanSpec, MergedValueRef};
use super::{ExistingRowStatus, StorageEngine, WriteDurability};

/// Computes a safe sub-batch WAL byte cap that fits within `total_sectors` without overflowing a
/// 1-sector (`4096 B`) WAL, capped at `max_cap_bytes`.
#[inline]
pub(super) fn max_wal_sub_batch_bytes(total_sectors: usize, max_cap_bytes: usize) -> usize {
    let single_sector_cap =
        crate::wal::MAX_FRAGMENT_DATA_LEN.saturating_sub(crate::wal::WAL_BATCH_HEADER_SIZE);
    if total_sectors <= 1 {
        return single_sector_cap.min(max_cap_bytes);
    }
    let wal_bytes = (total_sectors as u64).saturating_mul(crate::wal::WAL_SECTOR_SIZE as u64);
    let max_usable = ((total_sectors as u64)
        .saturating_mul(crate::wal::MAX_FRAGMENT_DATA_LEN as u64))
    .saturating_sub(crate::wal::WAL_BATCH_HEADER_SIZE as u64);
    (wal_bytes / 2)
        .clamp(single_sector_cap as u64, max_cap_bytes as u64)
        .min(max_usable) as usize
}

impl<FS: PlatformFs> StorageEngine<FS> {
    /// Registers or updates a Capped Table with a strict `max_bytes` quota and automatic LRU eviction.
    pub fn register_capped_table(&mut self, table_id: u16, max_bytes: u64) -> io::Result<()> {
        self.check_not_poisoned()?;
        self.drain_group_for_autocommit()?;
        let prev_has_row_meta = self.table_has_row_meta.get(&table_id).copied();
        self.table_has_row_meta.insert(table_id, true);
        let prev_max = self.capped_tables.get(&table_id).map(|ct| ct.max_bytes);
        let entry = self
            .capped_tables
            .entry(table_id)
            .or_insert_with(|| CappedTableState::new(table_id, max_bytes));
        entry.max_bytes = max_bytes;
        let restore_on_err = |this: &mut Self| {
            match prev_max {
                Some(old_max) => {
                    if let Some(ct) = this.capped_tables.get_mut(&table_id) {
                        ct.max_bytes = old_max;
                    }
                }
                None => {
                    this.capped_tables.remove(&table_id);
                }
            }
            match prev_has_row_meta {
                Some(v) => {
                    this.table_has_row_meta.insert(table_id, v);
                }
                None => {
                    this.table_has_row_meta.remove(&table_id);
                }
            }
        };
        if prev_max.is_none() {
            let res = {
                let mut guard = super::ReconcilingCappedGuard::enter(self, table_id);
                guard.reconcile_single_capped_table(table_id)
            };
            if let Err(e) = res {
                restore_on_err(self);
                return Err(e);
            }
        }
        if !self.tx_state.is_active() {
            let bytes_before = self
                .capped_tables
                .get(&table_id)
                .map_or(0, |ct| ct.current_logical_bytes);
            if let Err(e) = self.evict_until_within_quota(table_id, WriteDurability::BufferedFast) {
                if !self.is_poisoned() {
                    restore_on_err(self);
                }
                return Err(e);
            }
            let evicted = self
                .capped_tables
                .get(&table_id)
                .is_some_and(|ct| ct.current_logical_bytes != bytes_before);
            // A connect of a table that is already registered as declared changes nothing, and
            // must not cost a metadata commit and a sync. The stored quotas are compared by
            // registration and `max_bytes`, so a change that never reached the disk (a lost
            // commit) is still written here. The stored byte count is not compared: it is
            // brought up to date by every checkpoint and rebuilt from the rows when the log is
            // replayed at open, so it may lag behind committed writes; only an eviction made
            // here changes it and needs a commit of its own.
            let quota_metas = self.build_quota_metas();
            let mut stored = self.meta_mgr().state().table_quotas.clone();
            stored.sort_by_key(|q| q.table_id);
            let same_registrations = stored.len() == quota_metas.len()
                && stored.iter().zip(&quota_metas).all(|(old, new)| {
                    old.table_id == new.table_id
                        && old.eviction_policy == new.eviction_policy
                        && old.max_bytes == new.max_bytes
                });
            if evicted || !same_registrations {
                // Ensure buffered deletions from the eviction (or earlier buffered writes) are
                // durable in `wal_buf.db` before persisting `table_quotas`, so a power cut cannot
                // drop the WAL while leaving a stale `current_logical_bytes` that `replayed_wal ==
                // false` would trust on reopen.
                let commit_res = if self.last_buf_append_lsn > self.last_buf_synced_lsn {
                    self.make_buf_wal_durable(true)
                } else {
                    Ok(())
                }
                .and_then(|()| {
                    self.healthy()?.commit_meta_update(|st| {
                        st.table_quotas = quota_metas;
                    })
                });
                if let Err(e) = commit_res {
                    if !self.is_poisoned() {
                        restore_on_err(self);
                    }
                    return Err(e);
                }
            }
        } else if prev_max != Some(max_bytes) {
            if let Some(tx) = self.tx_state.active_tx_mut() {
                tx.quotas_modified = true;
                let _ = self.apply_tx_event(super::tx_fsm::TxEvent::InvalidatePrepare);
            }
        }
        Ok(())
    }

    /// Returns a snapshot of the [`CappedTableState`] for `table_id`, if registered.
    pub fn capped_table_state(&self, table_id: u16) -> Option<&CappedTableState> {
        self.capped_tables.get(&table_id)
    }

    /// Returns the [`CappedTableState`] visible in `view`.
    pub(crate) fn capped_table_state_view(
        &self,
        view: ReadView,
        table_id: u16,
    ) -> Option<&CappedTableState> {
        match (view, self.tx_state.start_meta()) {
            (ReadView::Committed, Some(snap)) => snap.capped_tables.get(&table_id),
            _ => self.capped_tables.get(&table_id),
        }
    }

    pub(super) fn eager_promote_lru_on_read(&mut self, table_id: u16, pk: &[u8]) -> io::Result<()> {
        if self.tx_state.is_active() {
            self.access_tracker.record_read_hit(table_id, pk);
            return Ok(());
        }
        self.drain_group_for_autocommit()?;
        let meta_key = encode_row_meta_key(table_id, pk);
        let Some((mut meta, old_m_ptr)) = self.get_row_meta_with_ptr(&meta_key)? else {
            return Ok(());
        };
        let old_tick = meta.recorded_tick;
        let new_tick = self.access_tracker.next_tick();
        meta.recorded_tick = new_tick;

        let mut records = Vec::with_capacity(3);
        if old_tick > 0 {
            records.push(WalRecord::Delete {
                key: encode_lru_queue_key(table_id, old_tick, pk),
            });
        }
        records.push(WalRecord::PutInline {
            key: encode_lru_queue_key(table_id, new_tick, pk),
            value: Vec::new(),
        });
        let meta_bytes = meta.to_bytes()?;
        let mut new_m_ptr = None;
        if !IndexEntry::inline_fits_in_block(meta_key.len(), meta_bytes.len()) {
            let ptr = self.append_to_blob_segment(BlobKind::Standard, 0, &meta_key, &meta_bytes)?;
            new_m_ptr = Some(ptr);
            records.push(WalRecord::PutBlobPtr {
                key: meta_key,
                blob_ptr: ptr,
            });
        } else {
            records.push(WalRecord::PutInline {
                key: meta_key,
                value: meta_bytes,
            });
        }
        if let Err(e) = self.commit_wal_and_memtable_core(
            records,
            WriteDurability::BufferedFast,
            false,
            false,
            super::tx::WriteScope::Normal,
        ) {
            if let Some(p) = new_m_ptr {
                self.record_dead_blob_bytes(&p);
            }
            return Err(e);
        }
        if let Some(m_ptr) = old_m_ptr {
            self.record_dead_blob_bytes(&m_ptr);
        }
        self.maybe_auto_flush_and_compact();
        Ok(())
    }

    pub(super) fn evict_until_within_quota(
        &mut self,
        table_id: u16,
        durability: WriteDurability,
    ) -> io::Result<()> {
        const EVICT_SCAN_WINDOW: usize = 64;
        self.check_not_poisoned()?;
        self.drain_group_for_autocommit()?;

        let max_window_wal_bytes = if !self.tx_state.is_active() {
            let sectors = match durability {
                WriteDurability::SyncTransactional => self.sync_wal().total_sectors(),
                WriteDurability::BufferedFast => self.buf_wal().total_sectors(),
            };
            max_wal_sub_batch_bytes(sectors, 64 * 1024)
        } else {
            usize::MAX
        };

        while let Some((cursor_tick, max_bytes, projected_logical_bytes)) = self
            .capped_tables
            .get(&table_id)
            .filter(|ct| ct.is_over_quota())
            .map(|ct| {
                (
                    ct.eviction_cursor_tick,
                    ct.max_bytes,
                    ct.current_logical_bytes,
                )
            })
        {
            self.check_not_poisoned()?;
            let lru_prefix = encode_namespace_prefix(table_id, NS_LRU_QUEUE);
            let seek_key = encode_lru_queue_key(table_id, cursor_tick, &[]);
            let candidates =
                self.scan_merged_prefix_from(&lru_prefix, &seek_key, EVICT_SCAN_WINDOW)?;
            if candidates.is_empty() {
                if cursor_tick > 0 {
                    if let Some(ct) = self.capped_tables.get_mut(&table_id) {
                        ct.eviction_cursor_tick = 0;
                    }
                    continue;
                }
                break;
            }

            let mut win = EvictionWindowBatch {
                max_window_wal_bytes,
                last_scanned_tick: cursor_tick,
                batch_records: Vec::new(),
                batch_est_bytes: 0,
                new_meta_ptrs: Vec::new(),
                old_dead_ptrs: Vec::new(),
                consumed_hot_ticks: Vec::new(),
                window_evictions: 0,
                window_promotions: 0,
                evicted_logical_bytes: 0,
                projected_logical_bytes,
                window_next_cursor: None,
                handled_pks: HashMap::new(),
            };

            if let Err(e) = self.plan_eviction_window(table_id, max_bytes, candidates, &mut win) {
                self.abort_uncommitted_blobs(&win.new_meta_ptrs);
                return Err(e);
            }

            let progress = if !win.batch_records.is_empty() {
                self.commit_eviction_window(durability, &mut win)?;

                for p in &win.old_dead_ptrs {
                    self.record_dead_blob_bytes(p);
                }
                if let Some(tx) = self.tx_state.active_tx_mut() {
                    for (pk, max_tick) in win.consumed_hot_ticks {
                        tx.pending_lru_clears.push((table_id, pk, max_tick));
                    }
                } else {
                    for (pk, max_tick) in win.consumed_hot_ticks {
                        self.access_tracker
                            .clear_if_tick_le(table_id, &pk, max_tick);
                    }
                }
                let advanced_cursor = win
                    .window_next_cursor
                    .map(|c| c.max(win.last_scanned_tick.saturating_add(1)));
                if let Some(ct) = self.capped_tables.get_mut(&table_id) {
                    ct.current_logical_bytes = ct
                        .current_logical_bytes
                        .saturating_sub(win.evicted_logical_bytes);
                    ct.total_evictions += win.window_evictions;
                    ct.total_lazy_promotions += win.window_promotions;
                    if let Some(next_c) = advanced_cursor {
                        ct.eviction_cursor_tick = next_c;
                    }
                }
                self.maybe_auto_flush_and_compact();
                true
            } else {
                false
            };

            if !progress {
                if let Some(next_cursor) = win
                    .last_scanned_tick
                    .checked_add(1)
                    .filter(|_| win.last_scanned_tick >= cursor_tick)
                {
                    if let Some(ct) = self.capped_tables.get_mut(&table_id) {
                        ct.eviction_cursor_tick = next_cursor;
                    }
                } else {
                    break;
                }
            }
        }

        if !self.tx_state.is_active() {
            self.reclaim_fully_dead_lru_blob_segments(table_id)?;
        }
        Ok(())
    }

    fn plan_eviction_window(
        &mut self,
        table_id: u16,
        max_bytes: u64,
        candidates: Vec<IndexEntry>,
        win: &mut EvictionWindowBatch,
    ) -> io::Result<()> {
        for entry in candidates {
            let (old_tick, pk_slice) =
                decode_lru_queue_key(table_id, &entry.key).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "invalid LRU queue key")
                })?;

            if matches!(entry.value, IndexValue::Tombstone) {
                win.last_scanned_tick = old_tick;
                continue;
            }
            let pk = pk_slice.to_vec();

            if let Some(&prev_action) = win.handled_pks.get(&pk) {
                if prev_action == Some(old_tick) {
                    win.last_scanned_tick = old_tick;
                    continue;
                }
                // Duplicate or stale `'l'` entry for a PK already handled in this window.
                let del = WalRecord::Delete { key: entry.key };
                let del_est = del.est_wire_bytes();
                if !win.batch_records.is_empty()
                    && win.batch_est_bytes.saturating_add(del_est) > win.max_window_wal_bytes
                {
                    break;
                }
                win.batch_est_bytes = win.batch_est_bytes.saturating_add(del_est);
                win.batch_records.push(del);
                win.last_scanned_tick = old_tick;
                win.window_next_cursor = Some(old_tick.saturating_add(1));
                if win.batch_est_bytes >= win.max_window_wal_bytes {
                    break;
                }
                continue;
            }

            // Check row existence BEFORE lazy promotion so an orphan `'l'` key whose row is
            // already absent is cleaned up immediately rather than promoted.
            let row_key = encode_primary_row_key(table_id, &pk);
            let existing = self.check_existing_row(&row_key)?;
            if existing == ExistingRowStatus::NotFound {
                let del = WalRecord::Delete { key: entry.key };
                let del_est = del.est_wire_bytes();
                if !win.batch_records.is_empty()
                    && win.batch_est_bytes.saturating_add(del_est) > win.max_window_wal_bytes
                {
                    break;
                }
                win.batch_est_bytes = win.batch_est_bytes.saturating_add(del_est);
                win.batch_records.push(del);
                win.last_scanned_tick = old_tick;
                win.window_next_cursor = Some(old_tick.saturating_add(1));
                win.consumed_hot_ticks.push((pk.clone(), u64::MAX));
                win.handled_pks.insert(pk, None);
                if win.batch_est_bytes >= win.max_window_wal_bytes {
                    break;
                }
                continue;
            }

            // Check if this key was accessed in RAM since `old_tick` was indexed!
            if let Some(hot_tick) = self.access_tracker.peek_hot_tick(table_id, &pk) {
                if hot_tick > old_tick {
                    let cand = EvictionCandidate {
                        old_tick,
                        pk: &pk,
                        lru_key: entry.key,
                    };
                    if !self.plan_lazy_promotion(table_id, cand, hot_tick, win)? {
                        break;
                    }
                    win.handled_pks.insert(pk, Some(hot_tick));
                    if win.batch_est_bytes >= win.max_window_wal_bytes {
                        break;
                    }
                    continue;
                }
            }

            // Coldest candidate confirmed -> Cascade Evict (planned into the window batch)!
            let cand = EvictionCandidate {
                old_tick,
                pk: &pk,
                lru_key: entry.key,
            };
            if !self.plan_cascade_eviction(table_id, cand, &row_key, existing, win)? {
                break;
            }
            win.handled_pks.insert(pk, None);
            if win.projected_logical_bytes <= max_bytes
                || win.batch_est_bytes >= win.max_window_wal_bytes
            {
                break;
            }
        }
        Ok(())
    }

    fn plan_lazy_promotion(
        &mut self,
        table_id: u16,
        cand: EvictionCandidate<'_>,
        hot_tick: u64,
        win: &mut EvictionWindowBatch,
    ) -> io::Result<bool> {
        let meta_key = encode_row_meta_key(table_id, cand.pk);
        let promo_del_lru = WalRecord::Delete { key: cand.lru_key };
        let promo_put_lru = WalRecord::PutInline {
            key: encode_lru_queue_key(table_id, hot_tick, cand.pk),
            value: Vec::new(),
        };
        let mut cand_est = promo_del_lru
            .est_wire_bytes()
            .saturating_add(promo_put_lru.est_wire_bytes());
        let meta_info = if let Some((mut mrec, m_ptr)) = self.get_row_meta_with_ptr(&meta_key)? {
            mrec.recorded_tick = hot_tick;
            let meta_bytes = mrec.to_bytes()?;
            let inline = IndexEntry::inline_fits_in_block(meta_key.len(), meta_bytes.len());
            let meta_est = if inline {
                9 + meta_key.len() + meta_bytes.len()
            } else {
                28 + meta_key.len()
            };
            cand_est = cand_est.saturating_add(meta_est);
            Some((m_ptr, meta_bytes, inline))
        } else {
            None
        };
        if !win.batch_records.is_empty()
            && win.batch_est_bytes.saturating_add(cand_est) > win.max_window_wal_bytes
        {
            return Ok(false);
        }
        win.consumed_hot_ticks.push((cand.pk.to_vec(), hot_tick));
        win.batch_records.push(promo_del_lru);
        win.batch_records.push(promo_put_lru);
        if let Some((m_ptr, meta_bytes, inline)) = meta_info {
            if let Some(p) = m_ptr {
                win.old_dead_ptrs.push(p);
            }
            if inline {
                win.batch_records.push(WalRecord::PutInline {
                    key: meta_key,
                    value: meta_bytes,
                });
            } else {
                let ptr =
                    self.append_to_blob_segment(BlobKind::Standard, 0, &meta_key, &meta_bytes)?;
                win.new_meta_ptrs.push(ptr);
                win.batch_records.push(WalRecord::PutBlobPtr {
                    key: meta_key,
                    blob_ptr: ptr,
                });
            }
        }
        win.batch_est_bytes = win.batch_est_bytes.saturating_add(cand_est);
        win.window_promotions += 1;
        win.last_scanned_tick = cand.old_tick;
        win.window_next_cursor = Some(cand.old_tick.saturating_add(1));
        Ok(true)
    }

    fn plan_cascade_eviction(
        &mut self,
        table_id: u16,
        cand: EvictionCandidate<'_>,
        row_key: &[u8],
        existing: ExistingRowStatus,
        win: &mut EvictionWindowBatch,
    ) -> io::Result<bool> {
        let mut cand_records = Vec::new();
        let mut cand_dead_ptrs = Vec::new();
        if let ExistingRowStatus::Blob(old_ptr) = existing {
            cand_dead_ptrs.push(old_ptr);
        }
        cand_records.push(WalRecord::Delete {
            key: row_key.to_vec(),
        });
        self.append_rowid_deletes(table_id, cand.pk, &mut cand_records)?;
        let mut old_logical_size = 0u64;
        let mut deleted_entry_key = false;
        if self.may_have_row_meta(table_id)? {
            let old_meta = self.collect_old_meta_deletes(
                table_id,
                cand.pk,
                row_key,
                super::OldMetaDeleteOpts {
                    require_capped_for_lru: false,
                    delete_meta_key: true,
                    match_lru_key: Some(&cand.lru_key),
                },
                &mut cand_records,
            )?;
            old_logical_size = old_meta.old_logical_size;
            deleted_entry_key = old_meta.matched_lru_key;
            if let Some(p) = old_meta.old_meta_blob_ptr {
                cand_dead_ptrs.push(p);
            }
        }
        if !deleted_entry_key {
            cand_records.push(WalRecord::Delete { key: cand.lru_key });
        }
        let cand_est = cand_records
            .iter()
            .fold(0usize, |acc, r| acc.saturating_add(r.est_wire_bytes()));
        if !win.batch_records.is_empty()
            && win.batch_est_bytes.saturating_add(cand_est) > win.max_window_wal_bytes
        {
            return Ok(false);
        }
        win.batch_records.extend(cand_records);
        win.old_dead_ptrs.extend(cand_dead_ptrs);
        win.batch_est_bytes = win.batch_est_bytes.saturating_add(cand_est);
        win.evicted_logical_bytes = win.evicted_logical_bytes.saturating_add(old_logical_size);
        win.projected_logical_bytes = win.projected_logical_bytes.saturating_sub(old_logical_size);
        win.window_evictions += 1;
        win.last_scanned_tick = cand.old_tick;
        win.window_next_cursor = Some(cand.old_tick.saturating_add(1));
        win.consumed_hot_ticks.push((cand.pk.to_vec(), u64::MAX));
        Ok(true)
    }

    fn commit_eviction_window(
        &mut self,
        durability: WriteDurability,
        win: &mut EvictionWindowBatch,
    ) -> io::Result<()> {
        // `Write-Blob-Before-WAL`: ensure any out-of-line metadata blobs created by lazy
        // promotions in this window are synced before committing the single WAL batch.
        if !win.new_meta_ptrs.is_empty()
            && durability == WriteDurability::SyncTransactional
            && !self.tx_state.is_active()
        {
            if let Err(e) = self.sync_active_std_writer() {
                self.abort_uncommitted_blobs(&win.new_meta_ptrs);
                return Err(e);
            }
        }
        let batch_records = core::mem::take(&mut win.batch_records);
        if let Err(e) = self.commit_wal_and_memtable_core(
            batch_records,
            durability,
            true,
            false,
            super::tx::WriteScope::Normal,
        ) {
            self.abort_uncommitted_blobs(&win.new_meta_ptrs);
            return Err(e);
        }
        Ok(())
    }

    fn flush_reconcile_repairs(&mut self, batch: &mut ReconcileRepairBatch) -> io::Result<()> {
        if batch.missing_records.is_empty() {
            return Ok(());
        }
        let records = core::mem::take(&mut batch.missing_records);
        if let Err(e) = self.commit_wal_and_memtable_core(
            records,
            WriteDurability::BufferedFast,
            true,
            false,
            super::tx::WriteScope::Normal,
        ) {
            let aborted = core::mem::take(&mut batch.new_meta_ptrs);
            self.abort_uncommitted_blobs(&aborted);
            return Err(e);
        }
        batch.new_meta_ptrs.clear();
        batch.batch_est_bytes = 0;
        for m_ptr in batch.dead_meta_ptrs.drain(..) {
            self.record_dead_blob_bytes(&m_ptr);
        }
        Ok(())
    }

    pub(super) fn reconcile_single_capped_table(&mut self, tid: u16) -> io::Result<()> {
        self.reconcile_single_capped_table_inner(tid, true)
            .map(|_| ())
    }

    fn load_row_meta_batch(
        &mut self,
        tid: u16,
        row_batch: &[(Vec<u8>, usize)],
    ) -> io::Result<LoadedRowMetaBatch> {
        let meta_prefix = encode_namespace_prefix(tid, NS_ROW_META);
        let first_pk = decode_primary_row_key(tid, &row_batch.first().unwrap().0).unwrap_or(&[]);
        let last_pk = decode_primary_row_key(tid, &row_batch.last().unwrap().0).unwrap_or(&[]);
        let meta_lo = encode_row_meta_key(tid, first_pk);
        let meta_hi = encode_row_meta_key(tid, last_pk);

        let meta_entries = self.scan_merged_prefix_range(
            &meta_prefix,
            &meta_lo,
            true,
            Some((&meta_hi, true)),
            usize::MAX,
            false,
        )?;
        let mut meta_map: HashMap<Vec<u8>, (RowMetaRecord, Option<BlobPtr>)> =
            HashMap::with_capacity(meta_entries.len());
        // Preserve unreadable metadata so deletion can clean up indexes by primary key.
        let mut corrupt_meta = HashMap::new();
        for entry in meta_entries {
            if entry.key.len() >= 3 {
                let (decoded, ptr) = match &entry.value {
                    IndexValue::Inline(bytes) => (Some(RowMetaRecord::from_bytes(bytes)), None),
                    IndexValue::Blob(ptr) => (
                        self.dereference_blob_raw(ptr, &entry.key)?
                            .map(|bytes| RowMetaRecord::from_bytes(&bytes)),
                        Some(*ptr),
                    ),
                    IndexValue::Tombstone => continue,
                };
                let pk = entry.key[3..].to_vec();
                match decoded {
                    Some(Ok(rec)) => {
                        self.access_tracker.observe_tick(rec.recorded_tick);
                        meta_map.insert(pk, (rec, ptr));
                    }
                    Some(Err(e)) if e.kind() != io::ErrorKind::InvalidData => return Err(e),
                    _ => {
                        corrupt_meta.insert(pk, (entry.key, entry.value));
                    }
                }
            }
        }
        Ok((meta_map, corrupt_meta))
    }

    fn queue_row_meta_repair(
        &mut self,
        tid: u16,
        pk: &[u8],
        existing_meta: Option<(&RowMetaRecord, Option<BlobPtr>)>,
        fallback_logical_size: u32,
        batch: &mut ReconcileRepairBatch,
    ) -> io::Result<()> {
        let tick = self.access_tracker.next_tick();
        let (meta_logical_size, secondary_keys, old_m_ptr) = match existing_meta {
            Some((m, ptr)) => (m.logical_size, m.secondary_keys.as_slice(), ptr),
            None => (fallback_logical_size, [].as_slice(), None),
        };
        let meta_key = encode_row_meta_key(tid, pk);
        let meta_bytes = match RowMetaRecord::encode_slice(tick, meta_logical_size, secondary_keys)
        {
            Ok(bytes) => bytes,
            Err(e) => {
                let aborted = core::mem::take(&mut batch.new_meta_ptrs);
                self.abort_uncommitted_blobs(&aborted);
                return Err(e);
            }
        };
        let meta_est = if IndexEntry::inline_fits_in_block(meta_key.len(), meta_bytes.len()) {
            9 + meta_key.len() + meta_bytes.len()
        } else {
            28 + meta_key.len()
        };
        let lru_rec = WalRecord::PutInline {
            key: encode_lru_queue_key(tid, tick, pk),
            value: Vec::new(),
        };
        let row_est = lru_rec.est_wire_bytes().saturating_add(meta_est);
        if !batch.missing_records.is_empty()
            && batch.batch_est_bytes.saturating_add(row_est) > batch.max_batch_wal_bytes
        {
            self.flush_reconcile_repairs(batch)?;
        }
        let rec_start = batch.missing_records.len();
        batch.missing_records.push(lru_rec);
        match self.append_std_meta_record(
            meta_key,
            meta_bytes,
            WriteDurability::BufferedFast,
            &mut batch.missing_records,
        ) {
            Ok(Some(ptr)) => batch.new_meta_ptrs.push(ptr),
            Ok(None) => {}
            Err(e) => {
                let aborted = core::mem::take(&mut batch.new_meta_ptrs);
                self.abort_uncommitted_blobs(&aborted);
                return Err(e);
            }
        }
        for r in &batch.missing_records[rec_start..] {
            batch.batch_est_bytes = batch.batch_est_bytes.saturating_add(r.est_wire_bytes());
        }
        if let Some(m_ptr) = old_m_ptr {
            batch.dead_meta_ptrs.push(m_ptr);
        }
        Ok(())
    }

    fn reconcile_single_capped_table_inner(
        &mut self,
        tid: u16,
        apply_repairs: bool,
    ) -> io::Result<bool> {
        const RECONCILE_BATCH_SIZE: usize = 512;
        let max_batch_wal_bytes =
            max_wal_sub_batch_bytes(self.buf_wal().total_sectors(), 512 * 1024);
        let user_prefix = encode_namespace_prefix(tid, NS_PRIMARY_ROW);

        let mut r_seek_key = user_prefix.to_vec();
        let mut r_lower_inc = true;
        let mut total_bytes = 0u64;
        let mut needs_repair = false;

        loop {
            let mut row_batch: Vec<(Vec<u8>, usize)> = Vec::new();
            self.visit_merged_prefix_range(
                MergeScanSpec {
                    prefix: &user_prefix,
                    seek_key: &r_seek_key,
                    lower_inclusive: r_lower_inc,
                    upper_key_bound: None,
                    reverse: false,
                    limit: RECONCILE_BATCH_SIZE,
                    populate_cache: false,
                    need_values: true,
                },
                |key, _lsn, val| {
                    let val_len = match val {
                        MergedValueRef::Inline(v) => v.len(),
                        MergedValueRef::Blob(ptr) => {
                            (ptr.len as usize).saturating_sub(BLOB_RECORD_HEADER_LEN + key.len())
                        }
                    };
                    row_batch.push((key.to_vec(), val_len));
                    Ok(true)
                },
            )?;
            if row_batch.is_empty() {
                break;
            }
            r_seek_key = row_batch.last().unwrap().0.clone();
            r_lower_inc = false;

            let (meta_map, mut corrupt_meta) = self.load_row_meta_batch(tid, &row_batch)?;
            let mut batch = ReconcileRepairBatch {
                missing_records: Vec::new(),
                new_meta_ptrs: Vec::new(),
                dead_meta_ptrs: Vec::new(),
                batch_est_bytes: 0,
                max_batch_wal_bytes,
            };

            for (key, val_len) in row_batch {
                let Some(pk) = decode_primary_row_key(tid, &key) else {
                    continue;
                };
                if let Some((meta, old_m_ptr)) = meta_map.get(pk) {
                    total_bytes = total_bytes.saturating_add(meta.logical_size as u64);
                    if meta.recorded_tick == 0 {
                        needs_repair = true;
                        if apply_repairs {
                            self.queue_row_meta_repair(
                                tid,
                                pk,
                                Some((meta, *old_m_ptr)),
                                meta.logical_size,
                                &mut batch,
                            )?;
                        }
                    }
                } else {
                    let logical_size = (pk.len() + val_len) as u32;
                    if let Some((mkey, observed)) = corrupt_meta.remove(pk) {
                        // Count the row at its estimated size; under `Evict` it is queued for
                        // deletion (which cleans 'i'/'l' entries by scanning), under `Error` it
                        // stays until the user deletes it. Never fail reconciliation/open.
                        total_bytes = total_bytes.saturating_add(logical_size as u64);
                        needs_repair = true;
                        if apply_repairs {
                            self.count_corrupt_detection();
                            if self.corrupt_policy(tid) == super::CorruptPolicy::Evict {
                                self.queue_corrupt_eviction(tid, pk, &mkey, observed);
                            }
                        }
                        continue;
                    }
                    total_bytes = total_bytes.saturating_add(logical_size as u64);
                    needs_repair = true;
                    if apply_repairs {
                        self.queue_row_meta_repair(tid, pk, None, logical_size, &mut batch)?;
                    }
                }
            }

            if apply_repairs {
                self.flush_reconcile_repairs(&mut batch)?;
            }
        }

        if let Some(ct) = self.capped_tables.get_mut(&tid) {
            ct.current_logical_bytes = total_bytes;
        }
        if apply_repairs {
            self.maybe_auto_flush_and_compact();
        }
        Ok(needs_repair)
    }

    /// Recomputes `current_logical_bytes` and advances `access_tracker` for every capped table
    /// without mutating the WAL or MemTable, returning whether any metadata repairs are needed.
    pub(super) fn reconcile_capped_tables_readonly(&mut self) -> io::Result<bool> {
        let table_ids: Vec<u16> = self.capped_tables.keys().copied().collect();
        let mut any_needs_repair = false;
        for tid in table_ids {
            if self.reconcile_single_capped_table_inner(tid, false)? {
                any_needs_repair = true;
            }
        }
        Ok(any_needs_repair)
    }

    /// Rebuilds the byte totals and access ticks of every capped table after recovery.
    ///
    /// When `replayed_wal` is false, recovery applied nothing, so the memtable is empty and the
    /// persisted `current_logical_bytes` is authoritative. The row scan is then skipped and only
    /// the access tick counter is advanced past the ticks already present in each LRU queue.
    pub(super) fn reconcile_capped_tables_after_recovery(
        &mut self,
        replayed_wal: bool,
    ) -> io::Result<()> {
        let table_ids: Vec<u16> = self.capped_tables.keys().copied().collect();
        for tid in table_ids {
            if replayed_wal {
                self.reconcile_single_capped_table(tid)?;
            } else {
                self.observe_lru_queue_ticks(tid)?;
            }
        }
        Ok(())
    }

    /// Advances `access_tracker` past the newest tick in `table_id`'s LRU queue. Queue keys sort
    /// by tick, so a reverse scan limited to one entry yields the maximum.
    fn observe_lru_queue_ticks(&mut self, table_id: u16) -> io::Result<()> {
        let lru_prefix = encode_namespace_prefix(table_id, NS_LRU_QUEUE);
        let mut newest_tick = None;
        self.visit_merged_prefix_range_view(
            ReadView::Owner,
            super::scan::MergeScanSpec {
                prefix: &lru_prefix,
                seek_key: &lru_prefix,
                lower_inclusive: true,
                upper_key_bound: None,
                reverse: true,
                limit: 1,
                populate_cache: false,
                need_values: false,
            },
            |key, _lsn, _value| {
                let (tick, _) = decode_lru_queue_key(table_id, key).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "invalid LRU queue key")
                })?;
                newest_tick = Some(tick);
                Ok(true)
            },
        )?;
        if let Some(tick) = newest_tick {
            self.access_tracker.observe_tick(tick);
        }
        Ok(())
    }

    pub(super) fn build_quota_metas(&self) -> Vec<TableQuotaMeta> {
        Self::quota_metas_from_map(&self.capped_tables)
    }

    /// Quotas safe to persist on a MemTable flush: during a `RingFull` rotation inside an
    /// uncommitted transaction commit (`tx_state.start_meta().is_some()`), only the pre-transaction
    /// settled quotas match the flushed MemTable.
    pub(super) fn settled_quota_metas(&self) -> Vec<TableQuotaMeta> {
        if let Some(snap) = self.tx_state.start_meta() {
            Self::quota_metas_from_map(&snap.capped_tables)
        } else {
            let mut metas = Self::quota_metas_from_map(&self.capped_tables);
            if let Some(tid) = self.reconciling_capped_table() {
                metas.retain(|q| q.table_id != tid);
            }
            metas
        }
    }

    fn quota_metas_from_map(capped_tables: &HashMap<u16, CappedTableState>) -> Vec<TableQuotaMeta> {
        let mut metas: Vec<TableQuotaMeta> = capped_tables
            .values()
            .map(|ct| TableQuotaMeta {
                table_id: ct.table_id,
                eviction_policy: EvictionPolicy::Lru,
                max_bytes: ct.max_bytes,
                current_logical_bytes: ct.current_logical_bytes,
            })
            .collect();
        metas.sort_by_key(|q| q.table_id);
        metas
    }
}

struct EvictionWindowBatch {
    max_window_wal_bytes: usize,
    last_scanned_tick: u64,
    batch_records: Vec<WalRecord>,
    batch_est_bytes: usize,
    new_meta_ptrs: Vec<BlobPtr>,
    old_dead_ptrs: Vec<BlobPtr>,
    consumed_hot_ticks: Vec<(Vec<u8>, u64)>,
    window_evictions: u64,
    window_promotions: u64,
    evicted_logical_bytes: u64,
    projected_logical_bytes: u64,
    window_next_cursor: Option<u64>,
    handled_pks: HashMap<Vec<u8>, Option<u64>>,
}

struct EvictionCandidate<'a> {
    old_tick: u64,
    pk: &'a [u8],
    lru_key: Vec<u8>,
}

type LoadedRowMetaBatch = (
    HashMap<Vec<u8>, (RowMetaRecord, Option<BlobPtr>)>,
    HashMap<Vec<u8>, (Vec<u8>, IndexValue)>,
);

struct ReconcileRepairBatch {
    missing_records: Vec<WalRecord>,
    new_meta_ptrs: Vec<BlobPtr>,
    dead_meta_ptrs: Vec<BlobPtr>,
    batch_est_bytes: usize,
    max_batch_wal_bytes: usize,
}
