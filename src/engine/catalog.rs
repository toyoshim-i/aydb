//! System catalog management (`[0, 0, 'c', name] -> table_id`), virtual-table registration,
//! rename, table-id allocation, mutation-epoch tracking, and `DROP TABLE`.

use std::collections::{HashMap, HashSet};
use std::io;
use std::sync::Arc;

use crate::index::{IndexEntry, IndexValue};
use crate::lru::{
    NS_LRU_QUEUE, NS_PRIMARY_ROW, NS_ROW_META, NS_SECONDARY_INDEX, encode_namespace_prefix,
};
use crate::pal::PlatformFs;
use crate::wal::WalRecord;

use super::reader::ReadView;
use super::rowid;
use super::tx::{self, WriteScope};
use super::unique_index;
use super::{CorruptPolicy, ExistingRowStatus, StorageEngine, WriteDurability};

/// Key prefix for virtual-table catalog entries: `[0, 0, 'c', norm_name] -> u16 BE table_id`.
pub(super) const CATALOG_PREFIX: [u8; 3] = [0, 0, b'c'];

#[inline]
pub(super) fn encode_catalog_key(norm_name: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(CATALOG_PREFIX.len() + norm_name.len());
    key.extend_from_slice(&CATALOG_PREFIX);
    key.extend_from_slice(norm_name.as_bytes());
    key
}

/// The outcome of resolving a virtual table name to a `table_id` without registering it.
pub(crate) struct VtabPlan {
    /// The lower-cased, trimmed table name.
    pub(crate) norm_name: String,
    /// The `table_id` the table resolves to (an unused one for a table that is not persisted).
    pub(crate) table_id: u16,
    /// Whether the committed catalog already maps `norm_name` to `table_id`.
    pub(crate) persisted: bool,
}

/// Normalizes a virtual-table or schema identifier according to SQLite's ASCII case-folding
/// while preserving any leading, trailing, or internal whitespace.
#[inline]
pub(crate) fn normalize_vtab_ident(name: &str) -> String {
    name.to_ascii_lowercase()
}

pub(super) fn find_free_table_id(mut is_used: impl FnMut(u16) -> bool) -> io::Result<u16> {
    let mut candidate = 1u16;
    loop {
        if !is_used(candidate) {
            return Ok(candidate);
        }
        match candidate.checked_add(1) {
            Some(next) => candidate = next,
            None => {
                return Err(io::Error::other(
                    "all 65535 virtual table_ids are exhausted",
                ));
            }
        }
    }
}

impl<FS: PlatformFs> StorageEngine<FS> {
    /// Registers or resolves a virtual table `(table_name, explicit_table_id)` in the engine's
    /// active registry and persistent catalog (`table_id = 0` under `'c'`) using
    /// [`WriteDurability::BufferedFast`].
    pub fn register_or_resolve_vtab(
        &mut self,
        table_name: &str,
        explicit_id: Option<u16>,
    ) -> io::Result<u16> {
        self.register_or_resolve_vtab_with_durability(
            table_name,
            explicit_id,
            WriteDurability::BufferedFast,
        )
    }

    /// Registers or resolves a virtual table `(table_name, explicit_table_id)` in the engine's
    /// active registry and persistent catalog (`table_id = 0` under `'c'`) with the specified
    /// [`WriteDurability`].
    pub fn register_or_resolve_vtab_with_durability(
        &mut self,
        table_name: &str,
        explicit_id: Option<u16>,
        durability: WriteDurability,
    ) -> io::Result<u16> {
        let plan = self.plan_vtab_registration(table_name, explicit_id)?;
        self.register_vtab_plan(plan, durability, None)
            .map(|(id, _)| id)
    }

    fn alloc_vtab_token(&mut self) -> u64 {
        let tok = self.next_vtab_token;
        self.next_vtab_token = self.next_vtab_token.wrapping_add(1).max(1);
        tok
    }

    fn add_vtab_holder(&mut self, table_id: u16) -> u64 {
        let tok = self.alloc_vtab_token();
        self.active_vtab_holders
            .entry(table_id)
            .or_default()
            .push(tok);
        tok
    }

    /// Returns `true` if `table_id` currently has at least one live primary row visible in
    /// [`ReadView::Owner`].
    fn table_has_any_primary_row(&self, table_id: u16) -> io::Result<bool> {
        let prefix = encode_namespace_prefix(table_id, NS_PRIMARY_ROW);
        Ok(!self
            .scan_merged_prefix_range(&prefix, &prefix, true, None, 1, false)?
            .is_empty())
    }

    /// Registers a virtual table resolved by [`Self::plan_vtab_registration`] as live and, if the
    /// catalog does not hold it yet, persists its catalog entry. Returns `(table_id, reg_token)`.
    ///
    /// When creating a new entry (`!plan.persisted`) and `initial_unique_marker` is `Some`,
    /// writes the `UNIQUE` completion marker in the same atomic batch provided `chosen_id`
    /// currently has no primary rows (an attached non-empty `explicit_table_id` still requires
    /// background/incremental migration).
    pub(crate) fn register_vtab_plan(
        &mut self,
        plan: VtabPlan,
        durability: WriteDurability,
        initial_unique_marker: Option<Vec<u8>>,
    ) -> io::Result<(u16, u64)> {
        let VtabPlan {
            norm_name,
            table_id: chosen_id,
            persisted,
        } = plan;
        if !persisted {
            let cat_key = encode_catalog_key(&norm_name);
            if !IndexEntry::inline_fits_in_block(cat_key.len(), 2) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "virtual table name exceeds maximum catalog key length",
                ));
            }
            // The rowid mapping marker (and initial UNIQUE completion marker for an empty new
            // table) goes in the same batch: a table is either absent, or in the catalog with
            // its markers, never half created.
            let mut records = vec![
                WalRecord::PutInline {
                    key: cat_key,
                    value: chosen_id.to_be_bytes().to_vec(),
                },
                rowid::rowid_marker_record(chosen_id),
            ];
            let wrote_unique_marker = if let Some(marker_val) = initial_unique_marker {
                if !self.table_has_any_primary_row(chosen_id)? {
                    records.push(unique_index::unique_marker_record(chosen_id, marker_val));
                    true
                } else {
                    false
                }
            } else {
                false
            };
            self.commit_wal_and_memtable(records, durability)?;
            if wrote_unique_marker {
                self.note_unique_marker_changed();
            }
        }
        self.active_vtab_ids
            .insert(chosen_id, Arc::from(norm_name.as_str()));
        let token = self.add_vtab_holder(chosen_id);
        Ok((chosen_id, token))
    }

    /// Decides which `table_id` a virtual table `(table_name, explicit_id)` resolves to, and
    /// whether the catalog visible in [`ReadView::Owner`] holds it, without changing anything
    /// except draining any pending group commit first.
    pub(crate) fn plan_vtab_registration(
        &mut self,
        table_name: &str,
        explicit_id: Option<u16>,
    ) -> io::Result<VtabPlan> {
        self.plan_vtab_registration_view(ReadView::Owner, table_name, explicit_id)
    }

    /// View-scoped variant of [`Self::plan_vtab_registration`]. A connection that does not own
    /// the active transaction uses [`ReadView::Committed`] so uncommitted `CREATE`/`RENAME`
    /// mutations are hidden and live registrations come from `tx_state.start_meta()`.
    pub(crate) fn plan_vtab_registration_view(
        &mut self,
        view: ReadView,
        table_name: &str,
        explicit_id: Option<u16>,
    ) -> io::Result<VtabPlan> {
        self.check_not_poisoned()?;
        self.drain_group_for_autocommit()?;
        let norm_name = normalize_vtab_ident(table_name);
        if norm_name.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "virtual table name must not be empty",
            ));
        }
        let entries = self.scan_merged_prefix_view(view, &CATALOG_PREFIX)?;
        let mut persisted_name_to_id: HashMap<String, u16> = HashMap::new();
        let mut persisted_id_to_name: HashMap<u16, String> = HashMap::new();
        for entry in entries {
            if entry.key.len() > CATALOG_PREFIX.len() {
                if let IndexValue::Inline(val) = entry.value {
                    if val.len() == 2 {
                        let name = String::from_utf8_lossy(&entry.key[CATALOG_PREFIX.len()..])
                            .into_owned();
                        let id = u16::from_be_bytes([val[0], val[1]]);
                        persisted_name_to_id.insert(name.clone(), id);
                        persisted_id_to_name.insert(id, name);
                    }
                }
            }
        }

        let active_vtab_ids = if view == ReadView::Committed {
            self.tx_state
                .start_meta()
                .map_or(&self.active_vtab_ids, |s| &s.active_vtab_ids)
        } else {
            &self.active_vtab_ids
        };

        let chosen_id = match explicit_id {
            Some(id) => {
                if id == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "table_id 0 is reserved for engine system catalog",
                    ));
                }
                if let Some(existing_name) = active_vtab_ids.get(&id) {
                    if existing_name.as_ref() != norm_name.as_str() {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!(
                                "table_id {id} is already registered to live virtual table '{existing_name}'"
                            ),
                        ));
                    }
                }
                if let Some(owner) = persisted_id_to_name.get(&id) {
                    if owner != &norm_name {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!(
                                "table_id {id} is already persisted for virtual table '{owner}'"
                            ),
                        ));
                    }
                }
                if let Some(&persisted_id) = persisted_name_to_id.get(&norm_name) {
                    if persisted_id != id {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!(
                                "virtual table '{norm_name}' is already persisted with table_id {persisted_id}"
                            ),
                        ));
                    }
                }
                id
            }
            None => {
                if let Some(&id) = persisted_name_to_id.get(&norm_name) {
                    if let Some(existing_name) = active_vtab_ids.get(&id) {
                        if existing_name.as_ref() != norm_name.as_str() {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidInput,
                                format!(
                                    "table_id {id} is already registered to live virtual table '{existing_name}'"
                                ),
                            ));
                        }
                    }
                    id
                } else {
                    find_free_table_id(|id| {
                        active_vtab_ids.contains_key(&id) || persisted_id_to_name.contains_key(&id)
                    })?
                }
            }
        };

        Ok(VtabPlan {
            persisted: persisted_name_to_id.get(&norm_name).copied() == Some(chosen_id),
            norm_name,
            table_id: chosen_id,
        })
    }

    /// Registers an already persisted virtual table as live for a connection that does not own
    /// the active transaction (or for the owner lazily reconnecting an existing persisted table).
    /// Unlike a registration made for a newly created table, it must survive transaction and
    /// savepoint rollbacks, so it is recorded in `start_meta` and savepoint snapshots without
    /// clobbering any existing committed or renamed table name already captured there.
    pub(crate) fn activate_persisted_vtab(&mut self, table_id: u16, table_name: &str) -> u64 {
        let name: Arc<str> = Arc::from(normalize_vtab_ident(table_name).as_str());
        let tok = self.alloc_vtab_token();
        if let Some(tx) = self.tx_state.any_tx_mut() {
            let dropped_in_tx = tx.dropped_tables.iter().any(|&(tid, _)| tid == table_id);
            if !dropped_in_tx {
                self.active_vtab_ids
                    .entry(table_id)
                    .or_insert_with(|| name.clone());
                self.active_vtab_holders
                    .entry(table_id)
                    .or_default()
                    .push(tok);
            }
            for marker in &mut tx.savepoint_payloads {
                if let tx::SavepointMeta::Full(snap) = &mut marker.meta {
                    let prefix_len = snap.dropped_tables_len.min(tx.dropped_tables.len());
                    let dropped_before_sp = tx.dropped_tables[..prefix_len]
                        .iter()
                        .any(|&(tid, _)| tid == table_id);
                    if !dropped_before_sp {
                        snap.active_vtab_ids
                            .entry(table_id)
                            .or_insert_with(|| name.clone());
                        snap.active_vtab_holders
                            .entry(table_id)
                            .or_default()
                            .push(tok);
                    }
                }
            }
            tx.start_meta
                .active_vtab_ids
                .entry(table_id)
                .or_insert(name);
            tx.start_meta
                .active_vtab_holders
                .entry(table_id)
                .or_default()
                .push(tok);
        } else {
            self.active_vtab_ids.insert(table_id, name);
            self.active_vtab_holders
                .entry(table_id)
                .or_default()
                .push(tok);
        }
        tok
    }

    /// Applies runtime-only DDL options (`on_corrupt`, `inline_max`) for an existing persisted
    /// table without letting a transaction rollback wipe them from `start_meta` or savepoints.
    pub(crate) fn apply_persisted_vtab_runtime_settings(
        &mut self,
        table_id: u16,
        on_corrupt: Option<CorruptPolicy>,
        inline_max: Option<usize>,
    ) {
        let apply_to =
            |policies: &mut HashMap<u16, CorruptPolicy>,
             thresholds: &mut std::collections::BTreeMap<u16, usize>| {
                match on_corrupt {
                    Some(policy) => {
                        policies.insert(table_id, policy);
                    }
                    None => {
                        policies.remove(&table_id);
                    }
                }
                match inline_max {
                    Some(max_bytes) => {
                        thresholds.insert(table_id, max_bytes);
                    }
                    None => {
                        thresholds.remove(&table_id);
                    }
                }
            };

        if let Some(tx) = self.tx_state.any_tx_mut() {
            let dropped_in_tx = tx.dropped_tables.iter().any(|&(tid, _)| tid == table_id);
            if !dropped_in_tx {
                apply_to(
                    &mut self.corrupt_policies,
                    &mut self.table_inline_thresholds,
                );
            }
            for marker in &mut tx.savepoint_payloads {
                if let tx::SavepointMeta::Full(snap) = &mut marker.meta {
                    let prefix_len = snap.dropped_tables_len.min(tx.dropped_tables.len());
                    let dropped_before_sp = tx.dropped_tables[..prefix_len]
                        .iter()
                        .any(|&(tid, _)| tid == table_id);
                    if !dropped_before_sp {
                        apply_to(
                            &mut snap.corrupt_policies,
                            &mut snap.table_inline_thresholds,
                        );
                    }
                }
            }
            apply_to(
                &mut tx.start_meta.corrupt_policies,
                &mut tx.start_meta.table_inline_thresholds,
            );
        } else {
            apply_to(
                &mut self.corrupt_policies,
                &mut self.table_inline_thresholds,
            );
        }
    }

    /// Returns `true` if `table_id` was created (`xCreate`) inside the active transaction.
    pub(crate) fn is_vtab_created_in_tx(&self, table_id: u16) -> bool {
        self.tx_state
            .active_tx()
            .is_some_and(|tx| tx.vtabs_created.iter().any(|(id, _, _)| *id == table_id))
    }

    /// Notes that the active transaction created virtual table `(table_id, table_name)` in `db_name`.
    pub(crate) fn note_vtab_created_in_tx(
        &mut self,
        table_id: u16,
        table_name: &str,
        db_name: &str,
    ) {
        if let Some(tx) = self.tx_state.active_tx_mut() {
            tx.vtabs_created.push((
                table_id,
                normalize_vtab_ident(table_name),
                normalize_vtab_ident(db_name),
            ));
        }
    }

    /// Drops a table that the active transaction created and that SQLite no longer has, but only
    /// if the catalog still maps `table_name` to `table_id`. A savepoint rollback the engine was
    /// told about has already removed the table, and its id (or name) may belong to another
    /// table by now.
    pub(crate) fn drop_stale_created_vtab(
        &mut self,
        table_id: u16,
        table_name: &str,
    ) -> io::Result<()> {
        let norm = normalize_vtab_ident(table_name);
        let key = encode_catalog_key(&norm);
        let mapped = match self.lookup_raw_entry_view(ReadView::Owner, &key)? {
            Some((_, IndexValue::Inline(val))) if val.len() == 2 => {
                Some(u16::from_be_bytes([val[0], val[1]]))
            }
            _ => None,
        };
        if mapped == Some(table_id) {
            self.drop_table_with_durability(table_id, Some(&norm), WriteDurability::BufferedFast)?;
        }
        Ok(())
    }

    /// Removes the tables that the active transaction created under `table_name` or with
    /// `table_id` for which `is_stale(prev_id, prev_db, prev_name)` holds, and returns them. SQLite
    /// only runs `xCreate` for a name its schema does not have, so one of these is a creation that
    /// a `ROLLBACK TO` undid in the schema but, because SQLite never announces such a savepoint to a
    /// table made inside it, not in the engine.
    pub(crate) fn take_vtabs_created_in_tx(
        &mut self,
        table_id: Option<u16>,
        table_name: &str,
        mut is_stale: impl FnMut(u16, &str, &str) -> bool,
    ) -> Vec<(u16, String)> {
        let Some(tx) = self.tx_state.active_tx_mut() else {
            return Vec::new();
        };
        let norm = normalize_vtab_ident(table_name);
        let (stale, kept): (Vec<_>, Vec<_>) = core::mem::take(&mut tx.vtabs_created)
            .into_iter()
            .partition(|(id, name, db)| {
                (*name == norm || Some(*id) == table_id) && is_stale(*id, db, name)
            });
        tx.vtabs_created = kept;
        stale.into_iter().map(|(id, name, _)| (id, name)).collect()
    }

    /// Returns a snapshot of `(table_id, table_name, db_name)` entries created in the active
    /// transaction so callers can probe SQLite's schema without holding the engine lock.
    pub(crate) fn vtabs_created_in_tx_snapshot(&self) -> Vec<(u16, String, String)> {
        self.tx_state
            .active_tx()
            .map_or_else(Vec::new, |tx| tx.vtabs_created.clone())
    }

    /// Removes any tables created in the active transaction for which `is_stale(db, name)` holds,
    /// returning them so `vtab_sync_tx` can drop creations undone by SQLite savepoint rollbacks
    /// (including tables that were subsequently renamed).
    pub(crate) fn take_stale_vtabs_created_in_tx(
        &mut self,
        mut is_stale: impl FnMut(&str, &str) -> bool,
    ) -> Vec<(u16, String)> {
        let Some(tx) = self.tx_state.active_tx_mut() else {
            return Vec::new();
        };
        let (stale, kept): (Vec<_>, Vec<_>) = core::mem::take(&mut tx.vtabs_created)
            .into_iter()
            .partition(|(_, name, db)| is_stale(db, name));
        tx.vtabs_created = kept;
        stale.into_iter().map(|(id, name, _)| (id, name)).collect()
    }

    /// Returns the live normalized table name registered for `table_id`, if any.
    pub(crate) fn active_vtab_name(&self, table_id: u16) -> Option<String> {
        self.active_vtab_ids.get(&table_id).map(|s| s.to_string())
    }

    /// Per-table mutation epoch for `table_id` (0 if the table has not mutated since open).
    pub(crate) fn table_mutation_epoch(&self, table_id: u16) -> u64 {
        self.table_mutation_epochs
            .get(&table_id)
            .copied()
            .unwrap_or(0)
    }

    #[cfg(test)]
    pub(crate) fn test_table_has_synthetic_rowids(&self, table_id: u16) -> Option<bool> {
        self.table_has_synthetic_rowids.get(&table_id).copied()
    }

    pub(super) fn bump_table_mutation_epoch(&mut self, table_id: u16) {
        let e = self.table_mutation_epochs.entry(table_id).or_insert(0);
        *e = e.wrapping_add(1);
    }

    /// Updates per-table mutation epochs and synthetic-rowid tracking for committed or overlayed WAL
    /// records whose keys belong to user-row or rowid namespaces.
    pub(super) fn note_records_table_mutations(&mut self, records: &[WalRecord]) {
        let mut last_tid: Option<u16> = None;
        for rec in records {
            let (key, is_put) = match rec {
                WalRecord::PutInline { key, .. } => (key.as_slice(), true),
                WalRecord::PutBlobPtr { key, .. } | WalRecord::Delete { key } => {
                    (key.as_slice(), false)
                }
                WalRecord::TxBegin | WalRecord::TxCommit | WalRecord::TxContinue => continue,
            };
            if key.len() < 3 {
                continue;
            }
            let tid = u16::from_be_bytes([key[0], key[1]]);
            if tid == 0 {
                continue;
            }
            let ns = key[2];
            if is_put
                && (ns == rowid::NS_ROWID_BY_PK || (ns == rowid::NS_PK_BY_ROWID && key.len() > 3))
            {
                self.table_has_synthetic_rowids.insert(tid, true);
            }
            if matches!(
                ns,
                NS_PRIMARY_ROW | rowid::NS_ROWID_BY_PK | rowid::NS_PK_BY_ROWID
            ) && last_tid != Some(tid)
            {
                last_tid = Some(tid);
                self.bump_table_mutation_epoch(tid);
            }
        }
    }

    /// Returns `true` if `(table_id, token)` still refers to a live virtual table incarnation in
    /// the view visible to `conn_id`.
    pub(crate) fn is_vtab_token_active(&self, table_id: u16, token: u64, conn_id: usize) -> bool {
        let holders = if self.read_view_for_conn(conn_id) == ReadView::Committed {
            self.tx_state
                .start_meta()
                .map_or(&self.active_vtab_holders, |s| &s.active_vtab_holders)
        } else {
            &self.active_vtab_holders
        };
        holders
            .get(&table_id)
            .is_some_and(|list| list.contains(&token))
    }

    /// Unregisters a live virtual table handle identified by `(table_id, token)`, removing the
    /// catalog entry from `active_vtab_ids` only when the last live holder disconnects.
    pub(crate) fn unregister_vtab_token(&mut self, table_id: u16, token: u64) {
        let remove_from = |holders_map: &mut HashMap<u16, Vec<u64>>,
                           ids_map: &mut HashMap<u16, Arc<str>>| {
            let Some(holders) = holders_map.get_mut(&table_id) else {
                return;
            };
            let Some(pos) = holders.iter().position(|&t| t == token) else {
                return;
            };
            holders.swap_remove(pos);
            if holders.is_empty() {
                holders_map.remove(&table_id);
                ids_map.remove(&table_id);
            }
        };
        remove_from(&mut self.active_vtab_holders, &mut self.active_vtab_ids);
        if let Some(tx) = self.tx_state.any_tx_mut() {
            remove_from(
                &mut tx.start_meta.active_vtab_holders,
                &mut tx.start_meta.active_vtab_ids,
            );
            for marker in &mut tx.savepoint_payloads {
                if let tx::SavepointMeta::Full(snap) = &mut marker.meta {
                    remove_from(&mut snap.active_vtab_holders, &mut snap.active_vtab_ids);
                }
            }
        }
    }

    /// Renames a live virtual table in the catalog (`xRename` / `ALTER TABLE ... RENAME TO`): the
    /// entry of `old_name` moves to `new_name` and keeps its `table_id`. Both catalog writes and
    /// the live registration follow the enclosing transaction, so a rollback restores the old
    /// name. Fails if another table already holds `new_name`.
    pub(crate) fn rename_vtab(
        &mut self,
        table_id: u16,
        old_name: &str,
        new_name: &str,
        durability: WriteDurability,
    ) -> io::Result<()> {
        self.rename_vtab_with_scope(table_id, old_name, new_name, durability, WriteScope::Normal)
    }

    fn rename_vtab_with_scope(
        &mut self,
        table_id: u16,
        old_name: &str,
        new_name: &str,
        durability: WriteDurability,
        scope: WriteScope,
    ) -> io::Result<()> {
        self.check_not_poisoned()?;
        self.drain_group_for_autocommit()?;
        let old_norm = normalize_vtab_ident(old_name);
        let new_norm = normalize_vtab_ident(new_name);
        if new_norm.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "virtual table name must not be empty",
            ));
        }
        let new_key = encode_catalog_key(&new_norm);
        if !IndexEntry::inline_fits_in_block(new_key.len(), 2) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "virtual table name exceeds maximum catalog key length",
            ));
        }
        let mut catalog_old_name: Option<String> = None;
        for entry in self.scan_merged_prefix(&CATALOG_PREFIX)? {
            let IndexValue::Inline(val) = entry.value else {
                continue;
            };
            if val.len() == 2 && entry.key.len() > CATALOG_PREFIX.len() {
                let id = u16::from_be_bytes([val[0], val[1]]);
                let entry_name =
                    String::from_utf8_lossy(&entry.key[CATALOG_PREFIX.len()..]).into_owned();
                if entry_name == new_norm && id != table_id {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("a virtual table named '{new_norm}' already exists"),
                    ));
                }
                if id == table_id && (catalog_old_name.is_none() || entry_name == old_norm) {
                    catalog_old_name = Some(entry_name);
                }
            }
        }
        if let Some(live) = self.active_vtab_ids.iter().find_map(|(id, name)| {
            (name.as_ref() == new_norm.as_str() && *id != table_id).then_some(*id)
        }) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("a virtual table named '{new_norm}' is live as table_id {live}"),
            ));
        }
        let effective_old = catalog_old_name.as_deref().unwrap_or(&old_norm);
        if effective_old == new_norm {
            self.active_vtab_ids
                .insert(table_id, Arc::from(new_norm.as_str()));
            if let Some(tx) = self.tx_state.active_tx_mut() {
                for (id, name, _) in &mut tx.vtabs_created {
                    if *id == table_id {
                        *name = new_norm.clone();
                    }
                }
            }
            return Ok(());
        }
        let mut records = Vec::with_capacity(2);
        let old_key = encode_catalog_key(effective_old);
        if catalog_old_name.is_some()
            || !matches!(
                self.check_existing_row(&old_key)?,
                ExistingRowStatus::NotFound
            )
        {
            records.push(WalRecord::Delete { key: old_key });
        }
        records.push(WalRecord::PutInline {
            key: new_key,
            value: table_id.to_be_bytes().to_vec(),
        });
        self.commit_wal_and_memtable_scoped(records, durability, scope)?;
        self.active_vtab_ids
            .insert(table_id, Arc::from(new_norm.as_str()));
        if let Some(tx) = self.tx_state.active_tx_mut() {
            for (id, name, _) in &mut tx.vtabs_created {
                if *id == table_id {
                    *name = new_norm.clone();
                }
            }
        }
        Ok(())
    }

    /// Reverts a failed in-transaction virtual table rename without being blocked by the
    /// transaction size budget.
    pub(crate) fn revert_in_tx_rename_vtab(
        &mut self,
        table_id: u16,
        new_name: &str,
        old_name: &str,
        durability: WriteDurability,
    ) -> io::Result<()> {
        self.rename_vtab_with_scope(
            table_id,
            new_name,
            old_name,
            durability,
            WriteScope::InternalBypass,
        )
    }

    /// Drops a table (`xDestroy` / `DROP TABLE`), deleting all primary rows (`'r'`), secondary
    /// index entries (`'i'`), row metadata (`'t'`), LRU queue entries (`'l'`), capped table
    /// quota registrations, and system catalog entries synchronously
    /// ([`WriteDurability::SyncTransactional`]).
    pub fn drop_table(&mut self, table_id: u16, table_name: Option<&str>) -> io::Result<()> {
        self.drop_table_with_durability(table_id, table_name, WriteDurability::SyncTransactional)
    }

    /// Drops a table using the specified [`WriteDurability`] for its WAL tombstones and catalog
    /// deletion records.
    pub fn drop_table_with_durability(
        &mut self,
        table_id: u16,
        table_name: Option<&str>,
        durability: WriteDurability,
    ) -> io::Result<()> {
        self.check_not_poisoned()?;
        self.drain_group_for_autocommit()?;
        let mut records = Vec::new();
        let mut dead_ptrs = Vec::new();

        for ns in [
            NS_PRIMARY_ROW,
            NS_SECONDARY_INDEX,
            NS_ROW_META,
            NS_LRU_QUEUE,
            rowid::NS_ROWID_BY_PK,
            rowid::NS_PK_BY_ROWID,
        ] {
            let prefix = encode_namespace_prefix(table_id, ns);
            let entries = self.scan_merged_prefix(&prefix)?;
            for entry in entries {
                if let IndexValue::Blob(ptr) = entry.value {
                    dead_ptrs.push(ptr);
                }
                records.push(WalRecord::Delete { key: entry.key });
            }
        }

        let mut catalog_keys_for_id = HashSet::new();
        for entry in self.scan_merged_prefix(&CATALOG_PREFIX)? {
            if let IndexValue::Inline(val) = entry.value {
                if val.len() == 2 && u16::from_be_bytes([val[0], val[1]]) == table_id {
                    catalog_keys_for_id.insert(entry.key.clone());
                    records.push(WalRecord::Delete { key: entry.key });
                }
            }
        }

        let candidate_name = table_name
            .map(normalize_vtab_ident)
            .or_else(|| self.active_vtab_ids.get(&table_id).map(|s| s.to_string()));
        if let Some(norm_name) = candidate_name {
            let cat_key = encode_catalog_key(&norm_name);
            if !catalog_keys_for_id.contains(&cat_key) {
                if let Some((_, IndexValue::Inline(val))) = self.lookup_raw_entry(&cat_key)? {
                    if val.len() == 2 && u16::from_be_bytes([val[0], val[1]]) == table_id {
                        records.push(WalRecord::Delete { key: cat_key });
                    }
                }
            }
        }

        // Without an enclosing transaction the deletes run in an internal one: its chunked
        // TxBegin/TxCommit framing and WAL growth let a table larger than the WAL be dropped
        // atomically. Dropping a table is exempt from `max_transaction_bytes` both standalone and
        // inside a staged DDL transaction.
        let internal_tx = !self.tx_state.is_active();
        if internal_tx {
            self.begin_transaction();
        }
        if !records.is_empty() {
            if let Err(e) =
                self.commit_wal_and_memtable_scoped(records, durability, WriteScope::InternalBypass)
            {
                if internal_tx {
                    let _ = self.rollback_transaction();
                }
                return Err(e);
            }
            self.note_unique_marker_changed();
        }
        for ptr in dead_ptrs {
            self.record_dead_blob_bytes(&ptr);
        }
        // Mark pre-drop `blob_lru` bytes dead now (rather than waiting until commit) so if this
        // `table_id` is reused later in the same transaction, only bytes from the dropped
        // incarnation are marked dead and live blobs written by the replacement table stay intact.
        for (&(t, _), b) in &mut self.lru_blobs {
            if t == table_id {
                b.dead_bytes = b.file_len;
            }
        }

        if let Some(tx) = self.tx_state.active_tx_mut() {
            tx.vtabs_created.retain(|(id, _, _)| *id != table_id);
        }
        self.active_vtab_ids.remove(&table_id);
        self.active_vtab_holders.remove(&table_id);
        self.corrupt_policies.remove(&table_id);
        self.table_inline_thresholds.remove(&table_id);
        let was_capped = self.capped_tables.remove(&table_id).is_some();
        self.table_has_row_meta.insert(table_id, false);
        self.bump_table_mutation_epoch(table_id);
        self.blob_cache.invalidate_table_blobs(table_id);
        if let Ok(mut q) = self.pending_corrupt_evictions.lock() {
            q.retain(|(tid, _), _| *tid != table_id);
        }

        // Blob reclaim, LRU writer/segment disposal, and quota commit happen once on commit in
        // `publish_committed_transaction` via `ActiveTx::dropped_tables` so rolling back an
        // enclosing transaction leaves `active_lru_writers` and `lru_blobs` intact.
        if let Some(tx) = self.tx_state.active_tx_mut() {
            tx.dropped_tables.push((table_id, was_capped));
        }
        if internal_tx {
            self.commit_transaction()?;
            self.table_has_synthetic_rowids.insert(table_id, false);
        } else {
            self.table_has_synthetic_rowids.remove(&table_id);
        }
        Ok(())
    }
}
