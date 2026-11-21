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

use super::reader::ReadSource;
use super::{CorruptPolicy, LazyBlob, ReadCtx, ReadView, StorageEngine};

/// Result of [`StorageEngine::scan_table_batch`]: the readable rows and the resume cursor.
pub type TableBatch = (Vec<(Vec<u8>, Vec<u8>)>, Option<Vec<u8>>);

#[cfg(test)]
thread_local! {
    /// Number of MemTable entries `scan_table_range` copied out of the table, for tests.
    pub(crate) static TABLE_SCAN_ENTRIES_COPIED: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}

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

#[inline]
pub(super) fn should_populate_range_cache(
    lower: Option<(&[u8], bool)>,
    upper: Option<(&[u8], bool)>,
    limit: usize,
) -> bool {
    lower.is_some() && upper.is_some() && limit <= 256
}

#[inline]
pub(super) fn should_populate_secondary_cache(
    prefix: &[u8],
    upper_sec_key: Option<(&[u8], bool)>,
    limit: usize,
) -> bool {
    let is_exact_eq = prefix.len() > 5;
    is_exact_eq || (upper_sec_key.is_some() && limit <= 256)
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
        cx: &ReadCtx<'_, FS::File>,
        table_id: u16,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        limit: usize,
    ) -> io::Result<Vec<Vec<u8>>> {
        let mut pks = Vec::new();
        let mut slab = Vec::new();
        let mut spans = Vec::new();
        let lower_b = lower.map(|b| (b, true));
        let upper_b = upper.map(|b| (b, true));
        let populate_cache = should_populate_range_cache(lower_b, upper_b, limit);
        self.scan_range_into_slab_cx(
            cx,
            table_id,
            lower_b,
            upper_b,
            false,
            limit,
            true,
            populate_cache,
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
        let populate_cache = should_populate_range_cache(lower, upper, limit);
        self.owner_read(|eng, cx| {
            eng.scan_range_into_slab_cx(
                cx,
                table_id,
                lower,
                upper,
                reverse,
                limit,
                keys_only,
                populate_cache,
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
        cx: &ReadCtx<'_, FS::File>,
        table_id: u16,
        lower: Option<(&[u8], bool)>,
        upper: Option<(&[u8], bool)>,
        reverse: bool,
        limit: usize,
        keys_only: bool,
        populate_cache: bool,
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

        if self.idx_segments_for(cx.src).is_empty() && !reverse {
            let Some(range) =
                PreparedPrefixRange::prepare(&prefix, seek_key, lo_inc, upper_key_slice)
            else {
                return Ok(());
            };
            let mut last_pk_buf: Vec<u8> = Vec::new();
            for (k, entry, _) in self.mem_view_for(cx.src, range.lo_bound(), range.hi_bound()) {
                if !range.in_upper_and_prefix(k) {
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
                            lazy_blobs.push(Some(self.pin_lazy_blob_src(cx.src, ptr)));
                            pks_out.push(k[3..].to_vec());
                            if spans_out.len() >= limit {
                                break;
                            }
                        } else {
                            let start = slab_out.len();
                            if self.read_flushed_blob_append_into_src(cx.src, ptr, k, slab_out)? {
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
                        lazy_blobs.push(Some(self.pin_lazy_blob_src(cx.src, &ptr)));
                        pks_out.push(key[3..].to_vec());
                        Ok(true)
                    } else {
                        let start = slab_out.len();
                        if self.read_flushed_blob_append_into_src(cx.src, &ptr, key, slab_out)? {
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
            self.visit_merged_prefix_range_reverse_src(
                cx.src,
                &prefix,
                seek_key,
                lo_inc,
                upper_key_slice,
                limit,
                populate_cache,
                &mut emit_entry,
            )?;
        } else {
            self.visit_merged_prefix_range_src(
                cx.src,
                &prefix,
                seek_key,
                lo_inc,
                upper_key_slice,
                limit,
                populate_cache,
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
    ///
    /// Rows whose blob is corrupt under the `Evict` policy are skipped and do not count toward
    /// `limit`, so the result holds `limit` rows whenever that many readable rows exist.
    pub fn scan_table_range(
        &mut self,
        table_id: u16,
        lower_bound: Option<(&[u8], bool)>,
        upper_bound: Option<(&[u8], bool)>,
        limit: usize,
    ) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut rows = Vec::new();
        let mut lower = lower_bound.map(|(pk, inc)| (pk.to_vec(), inc));
        while rows.len() < limit {
            let want = limit - rows.len();
            let (batch, next) = self.scan_table_batch(
                table_id,
                lower.as_ref().map(|(pk, inc)| (pk.as_slice(), *inc)),
                upper_bound,
                want,
            )?;
            rows.extend(batch);
            match next {
                Some(pk) => lower = Some((pk, false)),
                None => break,
            }
        }
        Ok(rows)
    }

    /// Examines up to `limit` live entries of `table_id` within the bounds (tombstones are not
    /// entries) and returns the readable rows among them plus a resume cursor.
    ///
    /// A row whose blob is corrupt under the `Evict` policy is examined but not returned, so the
    /// row count says nothing about whether more rows follow. The cursor does: it is
    /// `Some(pk of the last examined entry)` exactly when `limit` entries were examined, and
    /// the next batch starts after it (`(cursor, false)` as lower bound). `None` means the range
    /// is exhausted. The work and the memory copied are `O(limit)`, independent of the table size.
    /// Under the `Error` policy a corrupt blob fails the call.
    pub fn scan_table_batch(
        &mut self,
        table_id: u16,
        lower_bound: Option<(&[u8], bool)>,
        upper_bound: Option<(&[u8], bool)>,
        limit: usize,
    ) -> io::Result<TableBatch> {
        if limit == 0 {
            return Ok((Vec::new(), None));
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
                return Ok((Vec::new(), None));
            }
        }

        if self.idx_segments.is_empty() {
            let Some(range) = PreparedPrefixRange::prepare(
                &prefix,
                &seek_key,
                lo_inc,
                upper_full.as_ref().map(|(v, inc)| (v.as_slice(), *inc)),
            ) else {
                return Ok((Vec::new(), None));
            };
            let entries: Vec<(Vec<u8>, IndexValue)> = self
                .mem_view(range.lo_bound(), range.hi_bound())
                .take_while(|(k, _, _)| range.in_upper_and_prefix(k))
                .filter(|(_, e, _)| !matches!(e.value, IndexValue::Tombstone))
                .take(limit)
                .map(|(k, e, _)| {
                    #[cfg(test)]
                    TABLE_SCAN_ENTRIES_COPIED.with(|c| c.set(c.get() + 1));
                    (k.clone(), e.value.clone())
                })
                .collect();
            let next = (entries.len() == limit)
                .then(|| entries.last().map(|(k, _)| k[3..].to_vec()))
                .flatten();
            let mut rows = Vec::with_capacity(entries.len());
            for (k, value) in entries {
                let val_opt = match value {
                    IndexValue::Inline(v) => Some(v),
                    IndexValue::Blob(ptr) => self.dereference_blob(&ptr, &k)?,
                    IndexValue::Tombstone => None,
                };
                if let Some(val) = val_opt {
                    rows.push((k[3..].to_vec(), val));
                }
            }
            return Ok((rows, next));
        }

        let populate_cache = lower_bound.is_some() && upper_bound.is_some() && limit <= 256;
        let merged = self.scan_merged_prefix_range(
            &prefix,
            &seek_key,
            lo_inc,
            upper_full.as_ref().map(|(v, inc)| (v.as_slice(), *inc)),
            limit,
            populate_cache,
        )?;
        let mut rows = Vec::with_capacity(merged.len());
        let mut next = None;
        let examined = merged.len();
        for (i, entry) in merged.into_iter().enumerate() {
            let Some(pk) = decode_primary_row_key(table_id, &entry.key) else {
                continue;
            };
            if i + 1 == examined && examined == limit {
                next = Some(pk.to_vec());
            }
            let val_opt = match entry.value {
                IndexValue::Inline(v) => Some(v),
                IndexValue::Blob(ptr) => self.dereference_blob(&ptr, &entry.key)?,
                IndexValue::Tombstone => None,
            };
            if let Some(val) = val_opt {
                rows.push((pk.to_vec(), val));
            }
        }
        Ok((rows, next))
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
            let Some(range) = PreparedPrefixRange::prepare(&prefix, &prefix, true, None) else {
                return Ok(Vec::new());
            };
            let mut pks = Vec::new();
            for (k, entry, _) in self.mem_view(range.lo_bound(), range.hi_bound()) {
                if !range.in_upper_and_prefix(k) {
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
            true,
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
        let populate_cache = should_populate_secondary_cache(prefix, upper_sec_key, limit);
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
                populate_cache,
                pks_out,
                slab_out,
                spans_out,
                lazy_blobs_out.as_deref_mut(),
            )
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn collect_mem_secondary_pks_cx(
        &self,
        cx: &ReadCtx<'_, FS::File>,
        table_id: u16,
        index_id: u16,
        prefix: &[u8],
        seek_slice: &[u8],
        lower_inclusive: bool,
        upper_sec_key: Option<(&[u8], bool)>,
        limit: usize,
        pks_out: &mut Vec<Vec<u8>>,
        last_sec_key_buf: &mut Vec<u8>,
    ) {
        let Some(range) =
            PreparedPrefixRange::prepare(prefix, seek_slice, lower_inclusive, upper_sec_key)
        else {
            return;
        };
        for (k, entry, _) in self.mem_view_for(cx.src, range.lo_bound(), range.hi_bound()) {
            if !range.in_upper_and_prefix(k) {
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
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn scan_secondary_index_into_slab_cx(
        &self,
        cx: &ReadCtx<'_, FS::File>,
        table_id: u16,
        index_id: u16,
        prefix: &[u8],
        lower_sec_key: (&[u8], bool),
        upper_sec_key: Option<(&[u8], bool)>,
        limit: usize,
        keys_only: bool,
        populate_cache: bool,
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
            if self.idx_segments_for(cx.src).is_empty() {
                self.collect_mem_secondary_pks_cx(
                    cx,
                    table_id,
                    index_id,
                    prefix,
                    seek_slice,
                    cur_inc,
                    upper_sec_key,
                    limit,
                    pks_out,
                    &mut last_sec_key_buf,
                );
            } else {
                self.visit_merged_prefix_range_src(
                    cx.src,
                    prefix,
                    seek_slice,
                    cur_inc,
                    upper_sec_key,
                    limit,
                    populate_cache,
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

            if self.idx_segments_for(cx.src).is_empty() {
                self.collect_mem_secondary_pks_cx(
                    cx,
                    table_id,
                    index_id,
                    prefix,
                    active_seek,
                    cur_inc,
                    upper_sec_key,
                    batch_need,
                    &mut batch_pks,
                    &mut last_sec_key_buf,
                );
            } else {
                self.visit_merged_prefix_range_src(
                    cx.src,
                    prefix,
                    active_seek,
                    cur_inc,
                    upper_sec_key,
                    batch_need,
                    populate_cache,
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

    pub(super) fn scan_merged_prefix_view(
        &self,
        view: ReadView,
        prefix: &[u8],
    ) -> io::Result<Vec<IndexEntry>> {
        let mut out = Vec::new();
        self.visit_merged_prefix_range_view(
            view,
            prefix,
            prefix,
            true,
            None,
            usize::MAX,
            false,
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

    pub(super) fn scan_merged_prefix_from(
        &self,
        prefix: &[u8],
        seek_key: &[u8],
        limit: usize,
    ) -> io::Result<Vec<IndexEntry>> {
        self.scan_merged_prefix_range(prefix, seek_key, true, None, limit, false)
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
        populate_cache: bool,
    ) -> io::Result<Vec<IndexEntry>> {
        let mut out = Vec::new();
        self.visit_merged_prefix_range(
            prefix,
            seek_key,
            lower_inclusive,
            upper_key_bound,
            limit,
            populate_cache,
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
    #[allow(clippy::too_many_arguments)]
    pub(super) fn visit_merged_prefix_range<F>(
        &self,
        prefix: &[u8],
        seek_key: &[u8],
        lower_inclusive: bool,
        upper_key_bound: Option<(&[u8], bool)>,
        limit: usize,
        populate_cache: bool,
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
            populate_cache,
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
        populate_cache: bool,
        on_live: F,
    ) -> io::Result<()>
    where
        F: FnMut(&[u8], u64, MergedValueRef<'_>) -> io::Result<bool>,
    {
        self.visit_merged_prefix_range_src(
            ReadSource::from_view(view),
            prefix,
            seek_key,
            lower_inclusive,
            upper_key_bound,
            limit,
            populate_cache,
            on_live,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn visit_merged_prefix_range_src<F>(
        &self,
        src: ReadSource<'_, FS::File>,
        prefix: &[u8],
        seek_key: &[u8],
        lower_inclusive: bool,
        upper_key_bound: Option<(&[u8], bool)>,
        limit: usize,
        populate_cache: bool,
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

        let mut mem_iter = self.mem_view_for_dir(src, range.lo_bound(), range.hi_bound(), false);
        let mut mem_exhausted = false;

        let mut seg_cursors = Vec::with_capacity(self.idx_segments_for(src).len());
        let upper_for_overlap = range.upper_for_overlap();
        for seg in self.idx_segments_for(src) {
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
        populate_cache: bool,
        on_live: F,
    ) -> io::Result<()>
    where
        F: FnMut(&[u8], u64, MergedValueRef<'_>) -> io::Result<bool>,
    {
        self.visit_merged_prefix_range_reverse_src(
            ReadSource::from_view(view),
            prefix,
            seek_key,
            lower_inclusive,
            upper_key_bound,
            limit,
            populate_cache,
            on_live,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn visit_merged_prefix_range_reverse_src<F>(
        &self,
        src: ReadSource<'_, FS::File>,
        prefix: &[u8],
        seek_key: &[u8],
        lower_inclusive: bool,
        upper_key_bound: Option<(&[u8], bool)>,
        limit: usize,
        populate_cache: bool,
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

        let mut mem_iter = self.mem_view_for_dir(src, range.lo_bound(), range.hi_bound(), true);
        let mut mem_exhausted = false;

        let seg_upper_arg = range.seg_upper_arg();
        let upper_for_overlap = range.upper_for_overlap();
        let mut seg_cursors = Vec::with_capacity(self.idx_segments_for(src).len());
        for seg in self.idx_segments_for(src) {
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
type MemHead<'a> = Option<(&'a Vec<u8>, &'a MemEntry)>;

#[derive(Clone, Copy)]
enum MemSource {
    Overlay,
    Pending(usize),
    Mem,
}

/// Single ordered source made of the active transaction overlay, any unflushed `pending_group`
/// commits (visible only to the owner of an active transaction), and the committed `MemTable`.
///
/// Yields `(key, entry, from_overlay)` in strict source-precedence order (`overlay > newest pending
/// > ... > oldest pending > MemTable`, independent of LSN values). `reverse` selects descending order.
pub(super) struct MemView<'a> {
    mem: crate::memtable::MemTableRange<'a>,
    ov: MemRange<'a>,
    pending: Vec<(MemRange<'a>, MemHead<'a>)>,
    mem_head: MemHead<'a>,
    ov_head: MemHead<'a>,
    reverse: bool,
}

impl<'a> MemView<'a> {
    fn new(
        mut mem: crate::memtable::MemTableRange<'a>,
        mut ov: MemRange<'a>,
        pending_ranges: Vec<MemRange<'a>>,
        reverse: bool,
    ) -> Self {
        let mem_head = Self::step_mem(&mut mem, reverse);
        let ov_head = Self::step_range(&mut ov, reverse);
        let pending = pending_ranges
            .into_iter()
            .map(|mut r| {
                let head = Self::step_range(&mut r, reverse);
                (r, head)
            })
            .collect();
        Self {
            mem,
            ov,
            pending,
            mem_head,
            ov_head,
            reverse,
        }
    }

    #[inline]
    fn step_mem(
        iter: &mut crate::memtable::MemTableRange<'a>,
        reverse: bool,
    ) -> Option<(&'a Vec<u8>, &'a MemEntry)> {
        if reverse {
            iter.next_back()
        } else {
            iter.next()
        }
    }

    #[inline]
    fn step_range(iter: &mut MemRange<'a>, reverse: bool) -> Option<(&'a Vec<u8>, &'a MemEntry)> {
        if reverse {
            iter.next_back()
        } else {
            iter.next()
        }
    }

    /// True when the overlay head is emitted before (or instead of) the memtable head (fast path
    /// when no `pending_group` batches are layered).
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
    fn key_beats(&self, cand: &[u8], cur: &[u8]) -> bool {
        if self.reverse { cand > cur } else { cand < cur }
    }

    fn best_source(&self) -> Option<(MemSource, &'a [u8])> {
        let mut best: Option<(MemSource, &'a [u8])> = self
            .ov_head
            .map(|(k, _)| (MemSource::Overlay, k.as_slice()));
        for (idx, (_, head)) in self.pending.iter().enumerate() {
            if let Some((pk, _)) = head {
                let pks = pk.as_slice();
                if best.is_none_or(|(_, bk)| self.key_beats(pks, bk)) {
                    best = Some((MemSource::Pending(idx), pks));
                }
            }
        }
        if let Some((mk, _)) = self.mem_head {
            let mks = mk.as_slice();
            if best.is_none_or(|(_, bk)| self.key_beats(mks, bk)) {
                best = Some((MemSource::Mem, mks));
            }
        }
        best
    }

    #[inline]
    fn peek_key(&self) -> Option<&'a [u8]> {
        if self.pending.is_empty() {
            match self.overlay_first()? {
                true => self.ov_head.map(|(k, _)| k.as_slice()),
                false => self.mem_head.map(|(k, _)| k.as_slice()),
            }
        } else {
            self.best_source().map(|(_, k)| k)
        }
    }

    fn advance(&mut self) -> Option<(&'a Vec<u8>, &'a MemEntry, bool)> {
        if self.pending.is_empty() {
            let from_overlay = self.overlay_first()?;
            if from_overlay {
                let (k, e) = self.ov_head.take()?;
                self.ov_head = Self::step_range(&mut self.ov, self.reverse);
                if self.mem_head.is_some_and(|(mk, _)| mk == k) {
                    self.mem_head = Self::step_mem(&mut self.mem, self.reverse);
                }
                return Some((k, e, true));
            } else {
                let (k, e) = self.mem_head.take()?;
                self.mem_head = Self::step_mem(&mut self.mem, self.reverse);
                return Some((k, e, false));
            }
        }

        let (winner, _) = self.best_source()?;
        let (k, e, from_overlay) = match winner {
            MemSource::Overlay => {
                let (k, e) = self.ov_head.take()?;
                self.ov_head = Self::step_range(&mut self.ov, self.reverse);
                (k, e, true)
            }
            MemSource::Pending(win_idx) => {
                let (ref mut r, ref mut head) = self.pending[win_idx];
                let (k, e) = head.take()?;
                *head = Self::step_range(r, self.reverse);
                (k, e, true)
            }
            MemSource::Mem => {
                let (k, e) = self.mem_head.take()?;
                self.mem_head = Self::step_mem(&mut self.mem, self.reverse);
                (k, e, false)
            }
        };
        let start_pending = match winner {
            MemSource::Overlay => 0,
            MemSource::Pending(win_idx) => win_idx + 1,
            MemSource::Mem => self.pending.len(),
        };
        for (r, head) in &mut self.pending[start_pending..] {
            if head.is_some_and(|(pk, _)| pk == k) {
                *head = Self::step_range(r, self.reverse);
            }
        }
        if !matches!(winner, MemSource::Mem) && self.mem_head.is_some_and(|(mk, _)| mk == k) {
            self.mem_head = Self::step_mem(&mut self.mem, self.reverse);
        }
        Some((k, e, from_overlay))
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
    /// Ascending view over the committed `MemTable` plus any transaction overlay / pending group
    /// batches visible to the current reader.
    #[inline]
    pub(super) fn mem_view<'a>(&'a self, lo: Bound<&'a [u8]>, hi: Bound<&'a [u8]>) -> MemView<'a> {
        self.mem_view_for_dir(ReadSource::Owner, lo, hi, false)
    }

    #[inline]
    pub(super) fn mem_view_for<'a>(
        &'a self,
        src: ReadSource<'a, FS::File>,
        lo: Bound<&'a [u8]>,
        hi: Bound<&'a [u8]>,
    ) -> MemView<'a> {
        self.mem_view_for_dir(src, lo, hi, false)
    }

    #[inline]
    pub(super) fn mem_view_for_dir<'a>(
        &'a self,
        src: ReadSource<'a, FS::File>,
        lo: Bound<&'a [u8]>,
        hi: Bound<&'a [u8]>,
        reverse: bool,
    ) -> MemView<'a> {
        static EMPTY: std::collections::BTreeMap<Vec<u8>, crate::memtable::MemEntry> =
            std::collections::BTreeMap::new();
        let ov_range = if !src.is_owner() || !self.tx_overlay.overlaps_range(lo, hi) {
            EMPTY.range::<[u8], _>((lo, hi))
        } else {
            self.tx_overlay.map.range::<[u8], _>((lo, hi))
        };
        let pending_ranges =
            if src.is_owner() && self.tx_depth > 0 && !self.pending_group.is_empty() {
                let mut ranges = Vec::with_capacity(self.pending_group.len());
                for (_, entries) in self.pending_group.iter().rev() {
                    ranges.push(entries.range::<[u8], _>((lo, hi)));
                }
                ranges
            } else {
                Vec::new()
            };
        MemView::new(
            self.memtable_for(src).iter_range(lo, hi),
            ov_range,
            pending_ranges,
            reverse,
        )
    }
}
