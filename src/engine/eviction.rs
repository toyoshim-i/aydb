//! Capped Table (`MAX_BYTES`) quota management, Coalesced Lazy Promotion, cascading LRU eviction,
//! and post-recovery byte/tick reconciliation.

use std::collections::HashMap;
use std::io;

use crate::blob::BLOB_RECORD_HEADER_LEN;
use crate::index::{IndexEntry, IndexValue};
use crate::lru::{
    CappedTableState, NS_LRU_QUEUE, NS_PRIMARY_ROW, NS_ROW_META, RowMetaRecord,
    decode_lru_queue_key, decode_primary_row_key, encode_lru_queue_key, encode_namespace_prefix,
    encode_row_meta_key,
};
use crate::meta::{EvictionPolicy, TableQuotaMeta};
use crate::pal::PlatformFs;
use crate::wal::{BlobKind, BlobPtr, WalRecord};

use super::reader::ReadView;
use super::{StorageEngine, WriteDurability};

impl<FS: PlatformFs> StorageEngine<FS> {
    /// Registers or updates a Capped Table with a strict `max_bytes` quota and automatic LRU eviction.
    pub fn register_capped_table(&mut self, table_id: u16, max_bytes: u64) -> io::Result<()> {
        self.check_not_poisoned()?;
        self.drain_group_for_autocommit()?;
        let prev_has_row_meta = self.table_has_row_meta.get(&table_id).copied();
        self.table_has_row_meta.insert(table_id, true);
        let was_present = self.capped_tables.contains_key(&table_id);
        let entry = self
            .capped_tables
            .entry(table_id)
            .or_insert_with(|| CappedTableState::new(table_id, max_bytes));
        entry.max_bytes = max_bytes;
        if !was_present {
            if let Err(e) = self.reconcile_single_capped_table(table_id) {
                self.capped_tables.remove(&table_id);
                match prev_has_row_meta {
                    Some(v) => {
                        self.table_has_row_meta.insert(table_id, v);
                    }
                    None => {
                        self.table_has_row_meta.remove(&table_id);
                    }
                }
                return Err(e);
            }
        }
        if self.tx_depth == 0 {
            let bytes_before = self
                .capped_tables
                .get(&table_id)
                .map_or(0, |ct| ct.current_logical_bytes);
            self.evict_until_within_quota(table_id, WriteDurability::BufferedFast)?;
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
            let mut quota_metas = self.build_quota_metas();
            quota_metas.sort_by_key(|q| q.table_id);
            let mut stored = self.meta_mgr.state().table_quotas.clone();
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
                if self.last_buf_append_lsn > self.last_buf_synced_lsn {
                    self.make_buf_wal_durable(true)?;
                }
                self.meta_mgr.commit_update(|st| {
                    st.table_quotas = quota_metas;
                })?;
            }
        } else {
            self.quotas_modified_in_tx = true;
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
        if view == ReadView::Committed && self.tx_start_meta.active {
            self.tx_start_meta.capped_tables.get(&table_id)
        } else {
            self.capped_tables.get(&table_id)
        }
    }

    pub(super) fn eager_promote_lru_on_read(&mut self, table_id: u16, pk: &[u8]) -> io::Result<()> {
        if self.tx_depth > 0 {
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
        let meta_bytes = meta.to_bytes();
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
        if let Err(e) = self.commit_wal_and_memtable_with_data_flag(
            records,
            WriteDurability::BufferedFast,
            false,
        ) {
            if let Some(p) = new_m_ptr {
                self.record_dead_blob_bytes(&p);
            }
            return Err(e);
        }
        if let Some(m_ptr) = old_m_ptr {
            self.record_dead_blob_bytes(&m_ptr);
        }
        Ok(())
    }

    pub(super) fn evict_until_within_quota(
        &mut self,
        table_id: u16,
        durability: WriteDurability,
    ) -> io::Result<()> {
        const EVICT_SCAN_WINDOW: usize = 64;
        loop {
            let (over, cursor_tick) = self
                .capped_tables
                .get(&table_id)
                .map(|ct| (ct.is_over_quota(), ct.eviction_cursor_tick))
                .unwrap_or((false, 0));
            if !over {
                break;
            }

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

            let mut progress = false;
            let mut last_scanned_tick = cursor_tick;

            for entry in candidates {
                let Some((old_tick, pk_slice)) = decode_lru_queue_key(table_id, &entry.key) else {
                    continue;
                };
                last_scanned_tick = old_tick;

                if matches!(entry.value, IndexValue::Tombstone) {
                    continue;
                }
                let pk = pk_slice.to_vec();

                // Check if this key was accessed in RAM since `old_tick` was indexed!
                if let Some(hot_tick) = self.access_tracker.take_hot_tick(table_id, &pk) {
                    if hot_tick > old_tick {
                        // LAZY PROMOTION: Move key to `hot_tick` in the LRU queue without evicting!
                        let meta_key = encode_row_meta_key(table_id, &pk);
                        let mut records = vec![
                            WalRecord::Delete {
                                key: entry.key.clone(),
                            },
                            WalRecord::PutInline {
                                key: encode_lru_queue_key(table_id, hot_tick, &pk),
                                value: Vec::new(),
                            },
                        ];
                        let mut old_m_ptr = None;
                        let mut new_m_ptr = None;
                        if let Some((mut mrec, m_ptr)) = self.get_row_meta_with_ptr(&meta_key)? {
                            old_m_ptr = m_ptr;
                            mrec.recorded_tick = hot_tick;
                            let meta_bytes = mrec.to_bytes();
                            new_m_ptr = self.append_std_meta_record(
                                meta_key,
                                meta_bytes,
                                durability,
                                &mut records,
                            )?;
                        }
                        if let Err(e) = self.commit_wal_and_memtable(records, durability) {
                            if let Some(ref p) = new_m_ptr {
                                self.record_dead_blob_bytes(p);
                            }
                            return Err(e);
                        }
                        if let Some(m_ptr) = old_m_ptr {
                            self.record_dead_blob_bytes(&m_ptr);
                        }
                        if let Some(ct) = self.capped_tables.get_mut(&table_id) {
                            ct.total_lazy_promotions += 1;
                            ct.eviction_cursor_tick = old_tick.saturating_add(1);
                        }
                        progress = true;
                        continue;
                    }
                }

                // Coldest candidate confirmed -> Cascade Evict!
                if self.delete_row(table_id, &pk, durability)? {
                    if let Some(ct) = self.capped_tables.get_mut(&table_id) {
                        ct.total_evictions += 1;
                        ct.eviction_cursor_tick = old_tick.saturating_add(1);
                    }
                    progress = true;
                } else {
                    // Stale LRU queue entry whose row was already gone; clean up the orphan `'l'` key
                    self.commit_wal_and_memtable(
                        vec![WalRecord::Delete { key: entry.key }],
                        durability,
                    )?;
                    if let Some(ct) = self.capped_tables.get_mut(&table_id) {
                        ct.eviction_cursor_tick = old_tick.saturating_add(1);
                    }
                    progress = true;
                }

                let still_over = self
                    .capped_tables
                    .get(&table_id)
                    .map(|ct| ct.is_over_quota())
                    .unwrap_or(false);
                if !still_over {
                    break;
                }
            }

            if !progress {
                if let Some(next_cursor) = last_scanned_tick
                    .checked_add(1)
                    .filter(|_| last_scanned_tick >= cursor_tick)
                {
                    if let Some(ct) = self.capped_tables.get_mut(&table_id) {
                        ct.eviction_cursor_tick = next_cursor;
                    }
                } else {
                    break;
                }
            }
        }

        if self.tx_depth == 0 {
            self.reclaim_fully_dead_lru_blob_segments(table_id)?;
        }
        Ok(())
    }

    pub(super) fn reconcile_single_capped_table(&mut self, tid: u16) -> io::Result<()> {
        self.reconcile_single_capped_table_inner(tid, true)
            .map(|_| ())
    }

    fn reconcile_single_capped_table_inner(
        &mut self,
        tid: u16,
        apply_repairs: bool,
    ) -> io::Result<bool> {
        const RECONCILE_BATCH_SIZE: usize = 512;
        let meta_prefix = encode_namespace_prefix(tid, NS_ROW_META);
        let user_prefix = encode_namespace_prefix(tid, NS_PRIMARY_ROW);

        let mut r_seek_key = user_prefix.to_vec();
        let mut r_lower_inc = true;
        let mut total_bytes = 0u64;
        let mut needs_repair = false;

        loop {
            let row_entries = self.scan_merged_prefix_range(
                &user_prefix,
                &r_seek_key,
                r_lower_inc,
                None,
                RECONCILE_BATCH_SIZE,
            )?;
            if row_entries.is_empty() {
                break;
            }
            r_seek_key = row_entries.last().unwrap().key.clone();
            r_lower_inc = false;

            let first_pk =
                decode_primary_row_key(tid, &row_entries.first().unwrap().key).unwrap_or(&[]);
            let last_pk =
                decode_primary_row_key(tid, &row_entries.last().unwrap().key).unwrap_or(&[]);
            let meta_lo = encode_row_meta_key(tid, first_pk);
            let meta_hi = encode_row_meta_key(tid, last_pk);

            let meta_entries = self.scan_merged_prefix_range(
                &meta_prefix,
                &meta_lo,
                true,
                Some((&meta_hi, true)),
                usize::MAX,
            )?;
            let mut meta_map: HashMap<Vec<u8>, (RowMetaRecord, Option<BlobPtr>)> =
                HashMap::with_capacity(meta_entries.len());
            // `'t'` records stored as blobs that failed verification (pk -> (meta key, ptr)).
            // They must NOT be regenerated as empty metadata (that would orphan the row's
            // secondary-index entries); instead the row is reported to the corruption policy.
            let mut corrupt_meta: HashMap<Vec<u8>, (Vec<u8>, BlobPtr)> = HashMap::new();
            for entry in meta_entries {
                if entry.key.len() >= 3 {
                    let decoded = match entry.value {
                        IndexValue::Inline(bytes) => {
                            RowMetaRecord::from_bytes(&bytes).ok().map(|r| (r, None))
                        }
                        IndexValue::Blob(ptr) => {
                            match self.dereference_blob_lenient(&ptr, &entry.key)? {
                                Some(bytes) => RowMetaRecord::from_bytes(&bytes)
                                    .ok()
                                    .map(|r| (r, Some(ptr))),
                                None => {
                                    corrupt_meta
                                        .insert(entry.key[3..].to_vec(), (entry.key.clone(), ptr));
                                    None
                                }
                            }
                        }
                        IndexValue::Tombstone => None,
                    };
                    if let Some((rec, m_ptr)) = decoded {
                        self.access_tracker.observe_tick(rec.recorded_tick);
                        meta_map.insert(entry.key[3..].to_vec(), (rec, m_ptr));
                    }
                }
            }

            let mut missing_records = Vec::new();
            let mut new_meta_ptrs = Vec::new();
            let mut dead_meta_ptrs = Vec::new();

            for entry in row_entries {
                let Some(pk) = decode_primary_row_key(tid, &entry.key) else {
                    continue;
                };
                if let Some((meta, old_m_ptr)) = meta_map.get(pk) {
                    total_bytes = total_bytes.saturating_add(meta.logical_size as u64);
                    if meta.recorded_tick == 0 {
                        needs_repair = true;
                        if apply_repairs {
                            let tick = self.access_tracker.next_tick();
                            missing_records.push(WalRecord::PutInline {
                                key: encode_lru_queue_key(tid, tick, pk),
                                value: Vec::new(),
                            });
                            let meta_key = encode_row_meta_key(tid, pk);
                            let meta_bytes = RowMetaRecord::encode_slice(
                                tick,
                                meta.logical_size,
                                &meta.secondary_keys,
                            );
                            match self.append_std_meta_record(
                                meta_key,
                                meta_bytes,
                                WriteDurability::BufferedFast,
                                &mut missing_records,
                            ) {
                                Ok(Some(ptr)) => new_meta_ptrs.push(ptr),
                                Ok(None) => {}
                                Err(e) => {
                                    for p in &new_meta_ptrs {
                                        self.record_dead_blob_bytes(p);
                                    }
                                    return Err(e);
                                }
                            }
                            if let Some(m_ptr) = *old_m_ptr {
                                dead_meta_ptrs.push(m_ptr);
                            }
                        }
                    }
                } else {
                    let val_len = match &entry.value {
                        IndexValue::Inline(v) => v.len(),
                        IndexValue::Blob(ptr) => (ptr.len as usize)
                            .saturating_sub(BLOB_RECORD_HEADER_LEN + entry.key.len()),
                        IndexValue::Tombstone => continue,
                    };
                    let logical_size = (pk.len() + val_len) as u32;
                    if let Some((mkey, mptr)) = corrupt_meta.get(pk) {
                        // Count the row at its estimated size; under `Evict` it is queued for
                        // deletion (which cleans 'i'/'l' entries by scanning), under `Error` it
                        // stays until the user deletes it. Never fail reconciliation/open.
                        total_bytes = total_bytes.saturating_add(logical_size as u64);
                        needs_repair = true;
                        if apply_repairs {
                            let _ = self.note_corrupt_blob(mkey, mptr);
                        }
                        continue;
                    }
                    total_bytes = total_bytes.saturating_add(logical_size as u64);
                    needs_repair = true;
                    if apply_repairs {
                        let tick = self.access_tracker.next_tick();
                        missing_records.push(WalRecord::PutInline {
                            key: encode_lru_queue_key(tid, tick, pk),
                            value: Vec::new(),
                        });
                        missing_records.push(WalRecord::PutInline {
                            key: encode_row_meta_key(tid, pk),
                            value: RowMetaRecord::encode_slice(tick, logical_size, &[]),
                        });
                    }
                }
            }

            if apply_repairs && !missing_records.is_empty() {
                if let Err(e) =
                    self.commit_wal_and_memtable(missing_records, WriteDurability::BufferedFast)
                {
                    for p in &new_meta_ptrs {
                        self.record_dead_blob_bytes(p);
                    }
                    return Err(e);
                }
                for m_ptr in dead_meta_ptrs {
                    self.record_dead_blob_bytes(&m_ptr);
                }
            }
        }

        if let Some(ct) = self.capped_tables.get_mut(&tid) {
            ct.current_logical_bytes = total_bytes;
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
        self.visit_merged_prefix_range_reverse_view(
            ReadView::Owner,
            &lru_prefix,
            &lru_prefix,
            true,
            None,
            1,
            |key, _lsn, _value| {
                newest_tick = decode_lru_queue_key(table_id, key).map(|(tick, _)| tick);
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
    /// uncommitted transaction commit (`tx_start_meta.active == true`), only the pre-transaction
    /// settled quotas match the flushed MemTable.
    pub(super) fn settled_quota_metas(&self) -> Vec<TableQuotaMeta> {
        if self.tx_start_meta.active {
            Self::quota_metas_from_map(&self.tx_start_meta.capped_tables)
        } else {
            Self::quota_metas_from_map(&self.capped_tables)
        }
    }

    fn quota_metas_from_map(capped_tables: &HashMap<u16, CappedTableState>) -> Vec<TableQuotaMeta> {
        capped_tables
            .values()
            .map(|ct| TableQuotaMeta {
                table_id: ct.table_id,
                eviction_policy: EvictionPolicy::Lru,
                max_bytes: ct.max_bytes,
                current_logical_bytes: ct.current_logical_bytes,
            })
            .collect()
    }
}
