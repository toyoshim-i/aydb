//! Forward and reverse K-way streaming merge cursors, range boundary helpers, and secondary-index scans.

use std::io;
use std::ops::Bound;

use crate::index::{IndexEntry, IndexValue};
use crate::lru::{
    NS_PRIMARY_ROW, decode_primary_row_key, decode_secondary_index_pk_slice,
    encode_namespace_prefix, encode_primary_row_key, encode_secondary_index_exact_prefix,
};
use crate::memtable::MemEntry;
use crate::pal::PlatformFs;
use crate::wal::BlobPtr;

use super::{CorruptPolicy, LazyBlob, ReadCtx, ReadView, StorageEngine};

#[derive(Debug, Clone, Copy)]
pub(super) enum MergedValueRef<'a> {
    Inline(&'a [u8]),
    Blob(BlobPtr),
}

/// Precomputed range and prefix bounds shared by forward and reverse K-way merge scans.
struct PreparedPrefixRange<'a> {
    prefix: &'a [u8],
    eff_lo_key: &'a [u8],
    eff_lo_inc: bool,
    upper_key_bound: Option<(&'a [u8], bool)>,
    prefix_succ_opt: Option<Vec<u8>>,
}

impl<'a> PreparedPrefixRange<'a> {
    fn prepare(
        prefix: &'a [u8],
        seek_key: &'a [u8],
        lower_inclusive: bool,
        upper_key_bound: Option<(&'a [u8], bool)>,
    ) -> Option<Self> {
        let (eff_lo_key, eff_lo_inc) = if seek_key >= prefix {
            (seek_key, lower_inclusive)
        } else {
            (prefix, true)
        };

        if let Some((ub, ub_inc)) = upper_key_bound {
            if eff_lo_key > ub || (eff_lo_key == ub && (!eff_lo_inc || !ub_inc)) {
                return None;
            }
        }

        let prefix_succ_opt = if upper_key_bound.is_none() && !prefix.is_empty() {
            let mut s = prefix.to_vec();
            while s.last() == Some(&0xFF) {
                s.pop();
            }
            if let Some(last) = s.last_mut() {
                *last += 1;
                Some(s)
            } else {
                None
            }
        } else {
            None
        };

        Some(Self {
            prefix,
            eff_lo_key,
            eff_lo_inc,
            upper_key_bound,
            prefix_succ_opt,
        })
    }

    #[inline]
    fn lo_bound(&self) -> Bound<&'a [u8]> {
        if self.eff_lo_inc {
            Bound::Included(self.eff_lo_key)
        } else {
            Bound::Excluded(self.eff_lo_key)
        }
    }

    #[inline]
    fn hi_bound(&'a self) -> Bound<&'a [u8]> {
        match self.upper_key_bound {
            Some((ub, true)) => Bound::Included(ub),
            Some((ub, false)) => Bound::Excluded(ub),
            None => match &self.prefix_succ_opt {
                Some(succ) if succ.as_slice() > self.eff_lo_key => Bound::Excluded(succ.as_slice()),
                _ => Bound::Unbounded,
            },
        }
    }

    #[inline]
    fn seg_upper_arg(&'a self) -> Option<(&'a [u8], bool)> {
        match self.upper_key_bound {
            Some(u) => Some(u),
            None => self.prefix_succ_opt.as_deref().map(|s| (s, false)),
        }
    }

    #[inline]
    fn upper_for_overlap(&'a self) -> Option<&'a [u8]> {
        self.upper_key_bound
            .map(|(ub, _)| ub)
            .or(self.prefix_succ_opt.as_deref())
    }

    #[inline]
    fn in_upper_and_prefix(&self, key: &[u8]) -> bool {
        if !key.starts_with(self.prefix) {
            return false;
        }
        if let Some((ub, ub_inc)) = self.upper_key_bound {
            if ub_inc {
                if key > ub {
                    return false;
                }
            } else if key >= ub {
                return false;
            }
        }
        true
    }

    #[inline]
    fn in_lower_and_prefix(&self, key: &[u8]) -> bool {
        if !key.starts_with(self.prefix) {
            return false;
        }
        if self.eff_lo_inc {
            key >= self.eff_lo_key
        } else {
            key > self.eff_lo_key
        }
    }
}

impl<FS: PlatformFs> StorageEngine<FS> {
    /// Scans all live rows `(pk, value)` in `table_id` in ascending memcomparable primary-key order.
    pub fn scan_table(&mut self, table_id: u16) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan_table_range(table_id, None, None, usize::MAX)
    }

    /// Scans up to `limit` live primary keys (`pk`) in `table_id` within inclusive `[lower, upper]`
    /// without reading or dereferencing any out-of-line blob files.
    pub fn scan_keys_range(
        &mut self,
        table_id: u16,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        limit: usize,
    ) -> io::Result<Vec<Vec<u8>>> {
        self.owner_read(|eng, cx| eng.scan_keys_range_cx(cx, table_id, lower, upper, limit))
    }

    pub(super) fn scan_keys_range_cx(
        &self,
        cx: &ReadCtx,
        table_id: u16,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        limit: usize,
    ) -> io::Result<Vec<Vec<u8>>> {
        let mut pks = Vec::new();
        let mut slab = Vec::new();
        let mut spans = Vec::new();
        self.scan_range_into_slab_cx(
            cx,
            table_id,
            lower.map(|b| (b, true)),
            upper.map(|b| (b, true)),
            false,
            limit,
            true,
            &mut pks,
            &mut slab,
            &mut spans,
            None,
        )?;
        Ok(pks)
    }

    /// Streams live rows in `table_id` within `lower` and `upper` directly into `slab_out` and
    /// `spans_out` (or into `pks_out` when `keys_only` is `true` without dereferencing blobs)
    /// without intermediate per-row `Vec<u8>` allocations.
    ///
    /// When `lazy_blobs_out` is `Some`, out-of-line blobs are NOT read from disk during the scan;
    /// instead, a placeholder span `(0, 0)` is recorded in `spans_out` and `Some(blob_ptr)` is pushed
    /// to `lazy_blobs_out`, deferring disk I/O and CRC32C checks until `vtab_column` actually visits the row.
    #[allow(clippy::too_many_arguments)]
    pub fn scan_range_into_slab(
        &mut self,
        table_id: u16,
        lower: Option<(&[u8], bool)>,
        upper: Option<(&[u8], bool)>,
        reverse: bool,
        limit: usize,
        keys_only: bool,
        pks_out: &mut Vec<Vec<u8>>,
        slab_out: &mut Vec<u8>,
        spans_out: &mut Vec<(usize, usize)>,
        mut lazy_blobs_out: Option<&mut Vec<Option<LazyBlob<FS::File>>>>,
    ) -> io::Result<()> {
        self.owner_read(|eng, cx| {
            eng.scan_range_into_slab_cx(
                cx,
                table_id,
                lower,
                upper,
                reverse,
                limit,
                keys_only,
                pks_out,
                slab_out,
                spans_out,
                lazy_blobs_out.as_deref_mut(),
            )
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn scan_range_into_slab_cx(
        &self,
        cx: &ReadCtx,
        table_id: u16,
        lower: Option<(&[u8], bool)>,
        upper: Option<(&[u8], bool)>,
        reverse: bool,
        limit: usize,
        keys_only: bool,
        pks_out: &mut Vec<Vec<u8>>,
        slab_out: &mut Vec<u8>,
        spans_out: &mut Vec<(usize, usize)>,
        mut lazy_blobs_out: Option<&mut Vec<Option<LazyBlob<FS::File>>>>,
    ) -> io::Result<()> {
        if limit == 0 {
            return Ok(());
        }
        let prefix = encode_namespace_prefix(table_id, NS_PRIMARY_ROW);
        let mut lb_stack = [0u8; 64];
        let lb_heap;
        let seek_key: &[u8] = match lower {
            Some((lb_pk, _)) if 3 + lb_pk.len() <= lb_stack.len() => {
                lb_stack[0..3].copy_from_slice(&prefix);
                lb_stack[3..3 + lb_pk.len()].copy_from_slice(lb_pk);
                &lb_stack[..3 + lb_pk.len()]
            }
            Some((lb_pk, _)) => {
                lb_heap = encode_primary_row_key(table_id, lb_pk);
                &lb_heap
            }
            None => &prefix,
        };

        let mut ub_stack = [0u8; 64];
        let ub_heap;
        let upper_key_slice: Option<(&[u8], bool)> = match upper {
            Some((ub_pk, inc)) if 3 + ub_pk.len() <= ub_stack.len() => {
                ub_stack[0..3].copy_from_slice(&prefix);
                ub_stack[3..3 + ub_pk.len()].copy_from_slice(ub_pk);
                Some((&ub_stack[..3 + ub_pk.len()], inc))
            }
            Some((ub_pk, inc)) => {
                ub_heap = encode_primary_row_key(table_id, ub_pk);
                Some((&ub_heap, inc))
            }
            None => None,
        };

        let lo_inc = lower.map(|(_, inc)| inc).unwrap_or(true);
        if let Some((ub, ub_inc)) = upper_key_slice {
            if seek_key > ub || (seek_key == ub && (!lo_inc || !ub_inc)) {
                return Ok(());
            }
        }

        if !keys_only {
            self.ensure_all_flushed_cx(cx)?;
        }
        // Lazy dereference cannot skip a row once the cursor has emitted it, so tables whose
        // corrupt rows must vanish (`CorruptPolicy::Evict`) verify blobs eagerly during the scan.
        let lazy_ok = self.corrupt_policy(table_id) == CorruptPolicy::Error;

        if self.idx_segments_for_view(cx.view).is_empty() && !reverse {
            let lo = match lower {
                Some((_, false)) => Bound::Excluded(seek_key),
                _ => Bound::Included(seek_key),
            };
            let hi = match upper_key_slice {
                Some((ub, true)) => Bound::Included(ub),
                Some((ub, false)) => Bound::Excluded(ub),
                None => Bound::Unbounded,
            };
            let mut last_pk_buf: Vec<u8> = Vec::new();
            for (k, entry, _) in self.mem_view_for(cx.view, lo, hi) {
                if !k.starts_with(&prefix) {
                    break;
                }
                if keys_only {
                    if !matches!(entry.value, IndexValue::Tombstone) {
                        pks_out.push(k[3..].to_vec());
                        if pks_out.len() >= limit {
                            break;
                        }
                    }
                    continue;
                }
                match &entry.value {
                    IndexValue::Inline(v) => {
                        let start = slab_out.len();
                        slab_out.extend_from_slice(v);
                        spans_out.push((start, slab_out.len()));
                        if let Some(lazy_blobs) = &mut lazy_blobs_out {
                            lazy_blobs.push(None);
                            pks_out.push(k[3..].to_vec());
                        } else {
                            last_pk_buf.clear();
                            last_pk_buf.extend_from_slice(&k[3..]);
                        }
                        if spans_out.len() >= limit {
                            break;
                        }
                    }
                    IndexValue::Blob(ptr) => {
                        if let (true, Some(lazy_blobs)) = (lazy_ok, &mut lazy_blobs_out) {
                            spans_out.push((0, 0));
                            lazy_blobs.push(Some(self.pin_lazy_blob_view(cx.view, ptr)));
                            pks_out.push(k[3..].to_vec());
                            if spans_out.len() >= limit {
                                break;
                            }
                        } else {
                            let start = slab_out.len();
                            if self.read_flushed_blob_append_into_view(cx.view, ptr, k, slab_out)? {
                                spans_out.push((start, slab_out.len()));
                                if let Some(lazy_blobs) = &mut lazy_blobs_out {
                                    lazy_blobs.push(None);
                                    pks_out.push(k[3..].to_vec());
                                } else {
                                    last_pk_buf.clear();
                                    last_pk_buf.extend_from_slice(&k[3..]);
                                }
                                if spans_out.len() >= limit {
                                    break;
                                }
                            }
                        }
                    }
                    IndexValue::Tombstone => {}
                }
            }
            if !keys_only && lazy_blobs_out.is_none() && !last_pk_buf.is_empty() {
                pks_out.push(last_pk_buf);
            }
            return Ok(());
        }

        let mut last_pk_buf: Vec<u8> = Vec::new();
        let mut emit_entry = |key: &[u8], _lsn: u64, val: MergedValueRef<'_>| -> io::Result<bool> {
            if key.len() < 3 || !key.starts_with(&prefix) {
                return Ok(false);
            }
            if keys_only {
                pks_out.push(key[3..].to_vec());
                return Ok(true);
            }
            match val {
                MergedValueRef::Inline(v) => {
                    let start = slab_out.len();
                    slab_out.extend_from_slice(v);
                    spans_out.push((start, slab_out.len()));
                    if let Some(lazy_blobs) = &mut lazy_blobs_out {
                        lazy_blobs.push(None);
                        pks_out.push(key[3..].to_vec());
                    } else {
                        last_pk_buf.clear();
                        last_pk_buf.extend_from_slice(&key[3..]);
                    }
                    Ok(true)
                }
                MergedValueRef::Blob(ptr) => {
                    if let (true, Some(lazy_blobs)) = (lazy_ok, &mut lazy_blobs_out) {
                        spans_out.push((0, 0));
                        lazy_blobs.push(Some(self.pin_lazy_blob_view(cx.view, &ptr)));
                        pks_out.push(key[3..].to_vec());
                        Ok(true)
                    } else {
                        let start = slab_out.len();
                        if self.read_flushed_blob_append_into_view(cx.view, &ptr, key, slab_out)? {
                            spans_out.push((start, slab_out.len()));
                            if let Some(lazy_blobs) = &mut lazy_blobs_out {
                                lazy_blobs.push(None);
                                pks_out.push(key[3..].to_vec());
                            } else {
                                last_pk_buf.clear();
                                last_pk_buf.extend_from_slice(&key[3..]);
                            }
                            Ok(true)
                        } else {
                            Ok(false)
                        }
                    }
                }
            }
        };

        if reverse {
            self.visit_merged_prefix_range_reverse_view(
                cx.view,
                &prefix,
                seek_key,
                lo_inc,
                upper_key_slice,
                limit,
                &mut emit_entry,
            )?;
        } else {
            self.visit_merged_prefix_range_view(
                cx.view,
                &prefix,
                seek_key,
                lo_inc,
                upper_key_slice,
                limit,
                &mut emit_entry,
            )?;
        }

        if !keys_only && lazy_blobs_out.is_none() && !last_pk_buf.is_empty() {
            pks_out.push(last_pk_buf);
        }
        Ok(())
    }

    /// Scans up to `limit` live row payloads in `table_id` within `lower_bound` and `upper_bound`
    /// directly into `slab` and `spans`, avoiding per-row `Vec<u8>` heap allocations!
    pub fn scan_table_values_into(
        &mut self,
        table_id: u16,
        lower_bound: Option<(&[u8], bool)>,
        upper_bound: Option<(&[u8], bool)>,
        limit: usize,
        slab: &mut Vec<u8>,
        spans: &mut Vec<(usize, usize)>,
    ) -> io::Result<()> {
        let mut unused_pks = Vec::new();
        self.scan_range_into_slab(
            table_id,
            lower_bound,
            upper_bound,
            false,
            limit,
            false,
            &mut unused_pks,
            slab,
            spans,
            None,
        )
    }

    /// Scans up to `limit` live rows `(pk, value)` in `table_id` within `lower_bound` and `upper_bound`
    /// (`(bound_pk_bytes, inclusive)`), seeking directly via Sparse Fence Pointers!
    pub fn scan_table_range(
        &mut self,
        table_id: u16,
        lower_bound: Option<(&[u8], bool)>,
        upper_bound: Option<(&[u8], bool)>,
        limit: usize,
    ) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let prefix = encode_namespace_prefix(table_id, NS_PRIMARY_ROW);
        let seek_key = match lower_bound {
            Some((lb_pk, _)) => encode_primary_row_key(table_id, lb_pk),
            None => prefix.to_vec(),
        };
        let lo_inc = lower_bound.map(|(_, inc)| inc).unwrap_or(true);
        let upper_full =
            upper_bound.map(|(ub_pk, inc)| (encode_primary_row_key(table_id, ub_pk), inc));

        if let Some((ref ub, ub_inc)) = upper_full {
            if seek_key.as_slice() > ub.as_slice()
                || (seek_key.as_slice() == ub.as_slice() && (!lo_inc || !ub_inc))
            {
                return Ok(Vec::new());
            }
        }

        if self.idx_segments.is_empty() {
            let lo = match lower_bound {
                Some((_, false)) => Bound::Excluded(seek_key.as_slice()),
                _ => Bound::Included(seek_key.as_slice()),
            };
            let hi = match &upper_full {
                Some((ub, true)) => Bound::Included(ub.as_slice()),
                Some((ub, false)) => Bound::Excluded(ub.as_slice()),
                None => Bound::Unbounded,
            };
            let entries: Vec<(Vec<u8>, IndexValue)> = self
                .mem_view(lo, hi)
                .take_while(|(k, _, _)| k.starts_with(&prefix))
                .filter(|(_, e, _)| !matches!(e.value, IndexValue::Tombstone))
                .map(|(k, e, _)| (k.clone(), e.value.clone()))
                .collect();
            let mut rows = Vec::new();
            for (k, value) in entries {
                let val_opt = match value {
                    IndexValue::Inline(v) => Some(v),
                    IndexValue::Blob(ptr) => self.dereference_blob(&ptr, &k)?,
                    IndexValue::Tombstone => None,
                };
                if let Some(val) = val_opt {
                    rows.push((k[3..].to_vec(), val));
                    if rows.len() >= limit {
                        break;
                    }
                }
            }
            return Ok(rows);
        }

        let merged = self.scan_merged_prefix_range(
            &prefix,
            &seek_key,
            lo_inc,
            upper_full.as_ref().map(|(v, inc)| (v.as_slice(), *inc)),
            limit,
        )?;
        let mut rows = Vec::with_capacity(merged.len().min(limit));

        for entry in merged {
            let Some(pk) = decode_primary_row_key(table_id, &entry.key) else {
                continue;
            };
            let val_opt = match entry.value {
                IndexValue::Inline(v) => Some(v),
                IndexValue::Blob(ptr) => self.dereference_blob(&ptr, &entry.key)?,
                IndexValue::Tombstone => None,
            };
            if let Some(val) = val_opt {
                rows.push((pk.to_vec(), val));
                if rows.len() >= limit {
                    break;
                }
            }
        }
        Ok(rows)
    }

    /// Scans primary keys matching an exact secondary index value `(table_id, index_id, col_val)`.
    pub fn lookup_by_secondary_index(
        &self,
        table_id: u16,
        index_id: u16,
        col_val: &[u8],
    ) -> io::Result<Vec<Vec<u8>>> {
        let prefix = encode_secondary_index_exact_prefix(table_id, index_id, col_val);
        if self.idx_segments.is_empty() {
            let mut pks = Vec::new();
            for (k, entry, _) in self.mem_view(Bound::Included(prefix.as_slice()), Bound::Unbounded)
            {
                if !k.starts_with(&prefix) {
                    break;
                }
                if !matches!(entry.value, IndexValue::Tombstone) {
                    pks.push(k[prefix.len()..].to_vec());
                }
            }
            return Ok(pks);
        }

        let mut pks = Vec::new();
        self.visit_merged_prefix_range(
            &prefix,
            &prefix,
            true,
            None,
            usize::MAX,
            |key, _lsn, _val| {
                if key.starts_with(&prefix) {
                    pks.push(key[prefix.len()..].to_vec());
                    Ok(true)
                } else {
                    Ok(false)
                }
            },
        )?;
        Ok(pks)
    }

    /// Streams up to `limit` live rows matching a secondary index range `[lower_sec_key, upper_sec_key]`
    /// under `prefix` into `slab_out` and `spans_out` (or into `pks_out` when `keys_only` is `true`),
    /// returning `Ok(Some(last_sec_key))` when `limit` rows were emitted so the caller can resume
    /// the next chunk in `vtab_next`.
    #[allow(clippy::too_many_arguments)]
    pub fn scan_secondary_index_into_slab(
        &mut self,
        table_id: u16,
        index_id: u16,
        prefix: &[u8],
        lower_sec_key: (&[u8], bool),
        upper_sec_key: Option<(&[u8], bool)>,
        limit: usize,
        keys_only: bool,
        pks_out: &mut Vec<Vec<u8>>,
        slab_out: &mut Vec<u8>,
        spans_out: &mut Vec<(usize, usize)>,
        mut lazy_blobs_out: Option<&mut Vec<Option<LazyBlob<FS::File>>>>,
    ) -> io::Result<Option<Vec<u8>>> {
        self.owner_read(|eng, cx| {
            eng.scan_secondary_index_into_slab_cx(
                cx,
                table_id,
                index_id,
                prefix,
                lower_sec_key,
                upper_sec_key,
                limit,
                keys_only,
                pks_out,
                slab_out,
                spans_out,
                lazy_blobs_out.as_deref_mut(),
            )
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn scan_secondary_index_into_slab_cx(
        &self,
        cx: &ReadCtx,
        table_id: u16,
        index_id: u16,
        prefix: &[u8],
        lower_sec_key: (&[u8], bool),
        upper_sec_key: Option<(&[u8], bool)>,
        limit: usize,
        keys_only: bool,
        pks_out: &mut Vec<Vec<u8>>,
        slab_out: &mut Vec<u8>,
        spans_out: &mut Vec<(usize, usize)>,
        mut lazy_blobs_out: Option<&mut Vec<Option<LazyBlob<FS::File>>>>,
    ) -> io::Result<Option<Vec<u8>>> {
        if limit == 0 {
            return Ok(None);
        }
        let (seek_slice, mut cur_inc) = lower_sec_key;
        if let Some((ub, ub_inc)) = upper_sec_key {
            if seek_slice > ub || (seek_slice == ub && (!cur_inc || !ub_inc)) {
                return Ok(None);
            }
        }

        if !keys_only {
            self.ensure_all_flushed_cx(cx)?;
        }

        let mut last_sec_key_buf: Vec<u8> = Vec::new();

        if keys_only {
            if self.idx_segments_for_view(cx.view).is_empty() {
                let lo = if cur_inc {
                    Bound::Included(seek_slice)
                } else {
                    Bound::Excluded(seek_slice)
                };
                let hi = match upper_sec_key {
                    Some((ub, true)) => Bound::Included(ub),
                    Some((ub, false)) => Bound::Excluded(ub),
                    None => Bound::Unbounded,
                };
                for (k, entry, _) in self.mem_view_for(cx.view, lo, hi) {
                    if !k.starts_with(prefix) {
                        break;
                    }
                    if matches!(entry.value, IndexValue::Tombstone) {
                        continue;
                    }
                    if let Some(pk) = decode_secondary_index_pk_slice(table_id, index_id, k) {
                        pks_out.push(pk.to_vec());
                        last_sec_key_buf.clear();
                        last_sec_key_buf.extend_from_slice(k);
                        if pks_out.len() >= limit {
                            break;
                        }
                    }
                }
            } else {
                self.visit_merged_prefix_range_view(
                    cx.view,
                    prefix,
                    seek_slice,
                    cur_inc,
                    upper_sec_key,
                    limit,
                    |k, _lsn, _val| {
                        if let Some(pk) = decode_secondary_index_pk_slice(table_id, index_id, k) {
                            pks_out.push(pk.to_vec());
                            last_sec_key_buf.clear();
                            last_sec_key_buf.extend_from_slice(k);
                            Ok(true)
                        } else {
                            Ok(false)
                        }
                    },
                )?;
            }
            if pks_out.len() >= limit && !last_sec_key_buf.is_empty() {
                return Ok(Some(last_sec_key_buf));
            }
            return Ok(None);
        }

        let mut cur_seek_owned: Option<Vec<u8>> = None;
        let mut batch_pks: Vec<Vec<u8>> = Vec::with_capacity(limit.min(256));

        while spans_out.len() < limit {
            let batch_need = limit - spans_out.len();
            batch_pks.clear();
            let active_seek: &[u8] = cur_seek_owned.as_deref().unwrap_or(seek_slice);

            if self.idx_segments_for_view(cx.view).is_empty() {
                let lo = if cur_inc {
                    Bound::Included(active_seek)
                } else {
                    Bound::Excluded(active_seek)
                };
                let hi = match upper_sec_key {
                    Some((ub, true)) => Bound::Included(ub),
                    Some((ub, false)) => Bound::Excluded(ub),
                    None => Bound::Unbounded,
                };
                for (k, entry, _) in self.mem_view_for(cx.view, lo, hi) {
                    if !k.starts_with(prefix) {
                        break;
                    }
                    if matches!(entry.value, IndexValue::Tombstone) {
                        continue;
                    }
                    if let Some(pk) = decode_secondary_index_pk_slice(table_id, index_id, k) {
                        batch_pks.push(pk.to_vec());
                        last_sec_key_buf.clear();
                        last_sec_key_buf.extend_from_slice(k);
                        if batch_pks.len() >= batch_need {
                            break;
                        }
                    }
                }
            } else {
                self.visit_merged_prefix_range_view(
                    cx.view,
                    prefix,
                    active_seek,
                    cur_inc,
                    upper_sec_key,
                    batch_need,
                    |k, _lsn, _val| {
                        if let Some(pk) = decode_secondary_index_pk_slice(table_id, index_id, k) {
                            batch_pks.push(pk.to_vec());
                            last_sec_key_buf.clear();
                            last_sec_key_buf.extend_from_slice(k);
                            Ok(true)
                        } else {
                            Ok(false)
                        }
                    },
                )?;
            }

            if batch_pks.is_empty() {
                return Ok(None);
            }
            let scanned_count = batch_pks.len();
            for pk in &batch_pks {
                if let Some(lazy_blobs) = &mut lazy_blobs_out {
                    if self
                        .get_row_into_lazy_cx(cx, table_id, pk, slab_out, spans_out, lazy_blobs)?
                    {
                        pks_out.push(pk.clone());
                    }
                } else {
                    self.get_row_into_cx(cx, table_id, pk, slab_out, spans_out)?;
                }
            }
            if scanned_count < batch_need {
                return Ok(None);
            }
            if spans_out.len() >= limit {
                return Ok(Some(last_sec_key_buf));
            }
            cur_seek_owned = Some(last_sec_key_buf.clone());
            cur_inc = false;
        }

        if !last_sec_key_buf.is_empty() {
            Ok(Some(last_sec_key_buf))
        } else {
            Ok(None)
        }
    }

    pub(super) fn scan_merged_prefix(&self, prefix: &[u8]) -> io::Result<Vec<IndexEntry>> {
        self.scan_merged_prefix_from(prefix, prefix, usize::MAX)
    }

    pub(super) fn scan_merged_prefix_from(
        &self,
        prefix: &[u8],
        seek_key: &[u8],
        limit: usize,
    ) -> io::Result<Vec<IndexEntry>> {
        self.scan_merged_prefix_range(prefix, seek_key, true, None, limit)
    }

    /// Performs a bounded-memory streaming K-way merge across `self.memtable` and all
    /// `self.idx_segments` within `[seek_key, upper_key_bound]` under `prefix`.
    ///
    /// Resolves the highest-`lsn` version of each key on the fly, discards
    /// `IndexValue::Tombstone` entries before counting toward `limit`, and returns up to
    /// `limit` live entries in ascending key order.
    pub(super) fn scan_merged_prefix_range(
        &self,
        prefix: &[u8],
        seek_key: &[u8],
        lower_inclusive: bool,
        upper_key_bound: Option<(&[u8], bool)>,
        limit: usize,
    ) -> io::Result<Vec<IndexEntry>> {
        let mut out = Vec::new();
        self.visit_merged_prefix_range(
            prefix,
            seek_key,
            lower_inclusive,
            upper_key_bound,
            limit,
            |key, lsn, val| {
                let value = match val {
                    MergedValueRef::Inline(v) => IndexValue::Inline(v.to_vec()),
                    MergedValueRef::Blob(ptr) => IndexValue::Blob(ptr),
                };
                out.push(IndexEntry {
                    key: key.to_vec(),
                    lsn,
                    value,
                });
                Ok(true)
            },
        )?;
        Ok(out)
    }

    /// Streams up to `limit` live entries from the ascending K-way merge across `self.memtable`
    /// and `self.idx_segments` into `on_live` without allocating intermediate `IndexEntry`
    /// structs or per-entry `Vec<u8>` buffers.
    pub(super) fn visit_merged_prefix_range<F>(
        &self,
        prefix: &[u8],
        seek_key: &[u8],
        lower_inclusive: bool,
        upper_key_bound: Option<(&[u8], bool)>,
        limit: usize,
        on_live: F,
    ) -> io::Result<()>
    where
        F: FnMut(&[u8], u64, MergedValueRef<'_>) -> io::Result<bool>,
    {
        self.visit_merged_prefix_range_view(
            ReadView::Owner,
            prefix,
            seek_key,
            lower_inclusive,
            upper_key_bound,
            limit,
            on_live,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn visit_merged_prefix_range_view<F>(
        &self,
        view: ReadView,
        prefix: &[u8],
        seek_key: &[u8],
        lower_inclusive: bool,
        upper_key_bound: Option<(&[u8], bool)>,
        limit: usize,
        mut on_live: F,
    ) -> io::Result<()>
    where
        F: FnMut(&[u8], u64, MergedValueRef<'_>) -> io::Result<bool>,
    {
        if limit == 0 {
            return Ok(());
        }
        let Some(range) =
            PreparedPrefixRange::prepare(prefix, seek_key, lower_inclusive, upper_key_bound)
        else {
            return Ok(());
        };

        let mut mem_iter = MemView::new(
            self.memtable_for_view(view)
                .iter_range(range.lo_bound(), range.hi_bound()),
            self.visible_overlay(view)
                .range::<[u8], _>((range.lo_bound(), range.hi_bound())),
            false,
        );
        let mut mem_exhausted = false;

        let is_unbounded_full_scan =
            seek_key == prefix && upper_key_bound.is_none() && limit == usize::MAX;
        let populate_cache = !is_unbounded_full_scan;

        let mut seg_cursors = Vec::with_capacity(self.idx_segments_for_view(view).len());
        let upper_for_overlap = range.upper_for_overlap();
        for seg in self.idx_segments_for_view(view) {
            if !seg.overlaps_range(range.eff_lo_key, upper_for_overlap) {
                continue;
            }
            if !prefix.is_empty() && seg.min_key() > prefix && !seg.min_key().starts_with(prefix) {
                continue;
            }
            seg_cursors.push(seg.cursor_from_with_cache(
                range.eff_lo_key,
                range.eff_lo_inc,
                populate_cache,
            )?);
        }

        let mut emitted = 0usize;
        let mut target_key_buf: Vec<u8> = Vec::with_capacity(128);
        let mut best_val_buf: Vec<u8> = Vec::with_capacity(256);

        while emitted < limit {
            let mut min_key: Option<&[u8]> = None;

            if !mem_exhausted {
                if let Some(k) = mem_iter.peek_key() {
                    if range.in_upper_and_prefix(k) {
                        min_key = Some(k);
                    } else {
                        mem_exhausted = true;
                    }
                } else {
                    mem_exhausted = true;
                }
            }

            for cur in &seg_cursors {
                if let Some(k) = cur.current_key() {
                    if range.in_upper_and_prefix(k) {
                        match min_key {
                            None => min_key = Some(k),
                            Some(mk) if k < mk => {
                                min_key = Some(k);
                            }
                            _ => {}
                        }
                    }
                }
            }

            let Some(target_key_slice) = min_key else {
                break;
            };
            target_key_buf.clear();
            target_key_buf.extend_from_slice(target_key_slice);

            // `(lsn, value, from_overlay)`: overlay entries always win over segments by source
            // precedence; memtable entries are compared against segments by LSN.
            let mut best_mem_entry: Option<(u64, &IndexValue, bool)> = None;
            if !mem_exhausted && mem_iter.peek_key() == Some(target_key_buf.as_slice()) {
                let (_, mem_entry, from_overlay) = mem_iter.advance().unwrap();
                best_mem_entry = Some((mem_entry.lsn, &mem_entry.value, from_overlay));
            }

            let mut best_seg_meta: Option<(u64, u8)> = None;
            for cur in &mut seg_cursors {
                while cur.current_key() == Some(target_key_buf.as_slice()) {
                    let raw = cur.peek_raw().unwrap();
                    let beats_mem = best_mem_entry
                        .is_none_or(|(m_lsn, _, from_ov)| !from_ov && raw.lsn > m_lsn);
                    if beats_mem && best_seg_meta.is_none_or(|(s_lsn, _)| raw.lsn > s_lsn) {
                        best_seg_meta = Some((raw.lsn, raw.val_type));
                        best_val_buf.clear();
                        best_val_buf.extend_from_slice(raw.val_bytes);
                    }
                    cur.advance()?;
                }
            }

            if let Some((seg_lsn, val_type)) = best_seg_meta {
                match val_type {
                    1 => {
                        if on_live(
                            &target_key_buf,
                            seg_lsn,
                            MergedValueRef::Inline(&best_val_buf),
                        )? {
                            emitted += 1;
                        }
                    }
                    2 => {
                        let ptr = crate::index::RawIdxEntry::decode_blob_ptr(&best_val_buf)?;
                        if on_live(&target_key_buf, seg_lsn, MergedValueRef::Blob(ptr))? {
                            emitted += 1;
                        }
                    }
                    3 => {}
                    other => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("unknown idx entry val_type {other}"),
                        ));
                    }
                }
            } else if let Some((mem_lsn, mem_val, _)) = best_mem_entry {
                match mem_val {
                    IndexValue::Inline(v) => {
                        if on_live(
                            &target_key_buf,
                            mem_lsn,
                            MergedValueRef::Inline(v.as_slice()),
                        )? {
                            emitted += 1;
                        }
                    }
                    IndexValue::Blob(ptr) => {
                        if on_live(&target_key_buf, mem_lsn, MergedValueRef::Blob(*ptr))? {
                            emitted += 1;
                        }
                    }
                    IndexValue::Tombstone => {}
                }
            }
        }

        Ok(())
    }

    /// Streams up to `limit` live entries from the descending K-way merge across `self.memtable`
    /// and `self.idx_segments` into `on_live` without allocating intermediate `IndexEntry`
    /// structs or per-entry `Vec<u8>` buffers.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn visit_merged_prefix_range_reverse_view<F>(
        &self,
        view: ReadView,
        prefix: &[u8],
        seek_key: &[u8],
        lower_inclusive: bool,
        upper_key_bound: Option<(&[u8], bool)>,
        limit: usize,
        mut on_live: F,
    ) -> io::Result<()>
    where
        F: FnMut(&[u8], u64, MergedValueRef<'_>) -> io::Result<bool>,
    {
        if limit == 0 {
            return Ok(());
        }
        let Some(range) =
            PreparedPrefixRange::prepare(prefix, seek_key, lower_inclusive, upper_key_bound)
        else {
            return Ok(());
        };

        let mut mem_iter = MemView::new(
            self.memtable_for_view(view)
                .iter_range(range.lo_bound(), range.hi_bound()),
            self.visible_overlay(view)
                .range::<[u8], _>((range.lo_bound(), range.hi_bound())),
            true,
        );
        let mut mem_exhausted = false;

        let is_unbounded_full_scan =
            seek_key == prefix && upper_key_bound.is_none() && limit == usize::MAX;
        let populate_cache = !is_unbounded_full_scan;

        let seg_upper_arg = range.seg_upper_arg();
        let upper_for_overlap = range.upper_for_overlap();
        let mut seg_cursors = Vec::with_capacity(self.idx_segments_for_view(view).len());
        for seg in self.idx_segments_for_view(view) {
            if !seg.overlaps_range(range.eff_lo_key, upper_for_overlap) {
                continue;
            }
            if !prefix.is_empty() && seg.min_key() > prefix && !seg.min_key().starts_with(prefix) {
                continue;
            }
            seg_cursors.push(seg.cursor_reverse_from_with_cache(seg_upper_arg, populate_cache)?);
        }

        let mut emitted = 0usize;
        let mut target_key_buf: Vec<u8> = Vec::with_capacity(128);
        let mut best_val_buf: Vec<u8> = Vec::with_capacity(256);

        while emitted < limit {
            let mut max_key: Option<&[u8]> = None;

            if !mem_exhausted {
                if let Some(k) = mem_iter.peek_key() {
                    if range.in_lower_and_prefix(k) {
                        max_key = Some(k);
                    } else {
                        mem_exhausted = true;
                    }
                } else {
                    mem_exhausted = true;
                }
            }

            for cur in &seg_cursors {
                if let Some(k) = cur.current_key() {
                    if range.in_lower_and_prefix(k) {
                        match max_key {
                            None => max_key = Some(k),
                            Some(mk) if k > mk => {
                                max_key = Some(k);
                            }
                            _ => {}
                        }
                    }
                }
            }

            let Some(target_key_slice) = max_key else {
                break;
            };
            target_key_buf.clear();
            target_key_buf.extend_from_slice(target_key_slice);

            // `(lsn, value, from_overlay)`: overlay entries always win over segments by source
            // precedence; memtable entries are compared against segments by LSN.
            let mut best_mem_entry: Option<(u64, &IndexValue, bool)> = None;
            if !mem_exhausted && mem_iter.peek_key() == Some(target_key_buf.as_slice()) {
                let (_, mem_entry, from_overlay) = mem_iter.advance().unwrap();
                best_mem_entry = Some((mem_entry.lsn, &mem_entry.value, from_overlay));
            }

            let mut best_seg_meta: Option<(u64, u8)> = None;
            for cur in &mut seg_cursors {
                while cur.current_key() == Some(target_key_buf.as_slice()) {
                    let raw = cur.peek_raw().unwrap();
                    let beats_mem = best_mem_entry
                        .is_none_or(|(m_lsn, _, from_ov)| !from_ov && raw.lsn > m_lsn);
                    if beats_mem && best_seg_meta.is_none_or(|(s_lsn, _)| raw.lsn > s_lsn) {
                        best_seg_meta = Some((raw.lsn, raw.val_type));
                        best_val_buf.clear();
                        best_val_buf.extend_from_slice(raw.val_bytes);
                    }
                    cur.advance()?;
                }
            }

            if let Some((seg_lsn, val_type)) = best_seg_meta {
                match val_type {
                    1 => {
                        if on_live(
                            &target_key_buf,
                            seg_lsn,
                            MergedValueRef::Inline(&best_val_buf),
                        )? {
                            emitted += 1;
                        }
                    }
                    2 => {
                        let ptr = crate::index::RawIdxEntry::decode_blob_ptr(&best_val_buf)?;
                        if on_live(&target_key_buf, seg_lsn, MergedValueRef::Blob(ptr))? {
                            emitted += 1;
                        }
                    }
                    3 => {}
                    other => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("unknown idx entry val_type {other}"),
                        ));
                    }
                }
            } else if let Some((mem_lsn, mem_val, _)) = best_mem_entry {
                match mem_val {
                    IndexValue::Inline(v) => {
                        if on_live(
                            &target_key_buf,
                            mem_lsn,
                            MergedValueRef::Inline(v.as_slice()),
                        )? {
                            emitted += 1;
                        }
                    }
                    IndexValue::Blob(ptr) => {
                        if on_live(&target_key_buf, mem_lsn, MergedValueRef::Blob(*ptr))? {
                            emitted += 1;
                        }
                    }
                    IndexValue::Tombstone => {}
                }
            }
        }

        Ok(())
    }
}

type MemRange<'a> = std::collections::btree_map::Range<'a, Vec<u8>, MemEntry>;

/// Single ordered source made of the committed `MemTable` and the visible transaction overlay.
///
/// Yields `(key, entry, from_overlay)`; when both hold a key the overlay entry is returned and the
/// `MemTable` entry is skipped (source precedence, independent of LSN values). `reverse` selects
/// descending order (both ranges are consumed from the back).
pub(super) struct MemView<'a> {
    mem: crate::memtable::MemTableRange<'a>,
    ov: MemRange<'a>,
    mem_head: Option<(&'a Vec<u8>, &'a MemEntry)>,
    ov_head: Option<(&'a Vec<u8>, &'a MemEntry)>,
    reverse: bool,
}

impl<'a> MemView<'a> {
    fn new(
        mut mem: crate::memtable::MemTableRange<'a>,
        mut ov: MemRange<'a>,
        reverse: bool,
    ) -> Self {
        let mem_head = if reverse { mem.next_back() } else { mem.next() };
        let ov_head = if reverse { ov.next_back() } else { ov.next() };
        Self {
            mem,
            ov,
            mem_head,
            ov_head,
            reverse,
        }
    }

    /// True when the overlay head is emitted before (or instead of) the memtable head.
    #[inline]
    fn overlay_first(&self) -> Option<bool> {
        match (self.mem_head, self.ov_head) {
            (None, None) => None,
            (Some(_), None) => Some(false),
            (None, Some(_)) => Some(true),
            (Some((mk, _)), Some((ok, _))) => {
                let ord = if self.reverse { ok.cmp(mk) } else { mk.cmp(ok) };
                // Equal keys: the overlay shadows the memtable entry.
                Some(ord != std::cmp::Ordering::Less)
            }
        }
    }

    #[inline]
    fn peek_key(&self) -> Option<&'a [u8]> {
        match self.overlay_first()? {
            true => self.ov_head.map(|(k, _)| k.as_slice()),
            false => self.mem_head.map(|(k, _)| k.as_slice()),
        }
    }

    fn advance(&mut self) -> Option<(&'a Vec<u8>, &'a MemEntry, bool)> {
        let from_overlay = self.overlay_first()?;
        if from_overlay {
            let (k, e) = self.ov_head.take()?;
            self.ov_head = if self.reverse {
                self.ov.next_back()
            } else {
                self.ov.next()
            };
            if self.mem_head.is_some_and(|(mk, _)| mk == k) {
                self.mem_head = if self.reverse {
                    self.mem.next_back()
                } else {
                    self.mem.next()
                };
            }
            Some((k, e, true))
        } else {
            let (k, e) = self.mem_head.take()?;
            self.mem_head = if self.reverse {
                self.mem.next_back()
            } else {
                self.mem.next()
            };
            Some((k, e, false))
        }
    }
}

impl<'a> Iterator for MemView<'a> {
    type Item = (&'a Vec<u8>, &'a MemEntry, bool);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.advance()
    }
}

impl<FS: PlatformFs> StorageEngine<FS> {
    /// Ascending view over the committed `MemTable` plus the transaction overlay visible to the
    /// current reader.
    #[inline]
    pub(super) fn mem_view<'a>(&'a self, lo: Bound<&'a [u8]>, hi: Bound<&'a [u8]>) -> MemView<'a> {
        self.mem_view_for(ReadView::Owner, lo, hi)
    }

    #[inline]
    pub(super) fn mem_view_for<'a>(
        &'a self,
        view: ReadView,
        lo: Bound<&'a [u8]>,
        hi: Bound<&'a [u8]>,
    ) -> MemView<'a> {
        static EMPTY: std::collections::BTreeMap<Vec<u8>, crate::memtable::MemEntry> =
            std::collections::BTreeMap::new();
        let ov_range = if view != ReadView::Owner || !self.tx_overlay.overlaps_range(lo, hi) {
            EMPTY.range::<[u8], _>((lo, hi))
        } else {
            self.tx_overlay.map.range::<[u8], _>((lo, hi))
        };
        MemView::new(
            self.memtable_for_view(view).iter_range(lo, hi),
            ov_range,
            false,
        )
    }
}
