//! Forward and reverse K-way streaming merge cursors, range boundary helpers, and secondary-index scans.

use std::io;
use std::ops::Bound;

use crate::index::{IdxSegmentCursor, IdxSegmentRevCursor, IndexEntry, IndexValue, RawIdxEntry};
use crate::lru::{
    NS_PRIMARY_ROW, decode_primary_row_key, decode_secondary_index_pk_slice,
    encode_namespace_prefix, encode_primary_row_key, encode_secondary_index_exact_prefix,
};
use crate::memtable::MemEntry;
use crate::pal::{PlatformFile, PlatformFs};
use crate::wal::BlobPtr;

use super::reader::ReadSource;
use super::{CorruptPolicy, LazyBlob, ReadCtx, ReadView, StorageEngine};

/// Result of [`StorageEngine::scan_table_batch`]: the readable rows and the resume cursor.
pub type TableBatch = (Vec<(Vec<u8>, Vec<u8>)>, Option<Vec<u8>>);

/// Caller-owned reusable buffers populated by streaming slab range scans.
pub struct SlabScanOutput<'a, F: PlatformFile> {
    pub pks: &'a mut Vec<Vec<u8>>,
    pub slab: &'a mut Vec<u8>,
    pub spans: &'a mut Vec<(usize, usize)>,
    pub lazy_blobs: Option<&'a mut Vec<Option<LazyBlob<F>>>>,
}

impl<F: PlatformFile> SlabScanOutput<'_, F> {
    #[inline]
    pub(crate) fn reborrow(&mut self) -> SlabScanOutput<'_, F> {
        SlabScanOutput {
            pks: self.pks,
            slab: self.slab,
            spans: self.spans,
            lazy_blobs: self.lazy_blobs.as_deref_mut(),
        }
    }
}

/// Bounds and projection options for [`StorageEngine::scan_range_into_slab`].
#[derive(Debug, Clone, Copy)]
pub struct RangeScanOptions<'a> {
    pub lower: Option<(&'a [u8], bool)>,
    pub upper: Option<(&'a [u8], bool)>,
    pub reverse: bool,
    pub limit: usize,
    pub keys_only: bool,
}

/// Bounds and projection options for [`StorageEngine::scan_secondary_index_into_slab`].
#[derive(Debug, Clone, Copy)]
pub struct SecondaryScanOptions<'a> {
    pub prefix: &'a [u8],
    pub lower_sec_key: (&'a [u8], bool),
    pub upper_sec_key: Option<(&'a [u8], bool)>,
    pub limit: usize,
    pub keys_only: bool,
}

/// Internal bounds and options for [`StorageEngine::visit_merged_prefix_range`].
#[derive(Debug, Clone, Copy)]
pub(super) struct MergeScanSpec<'a> {
    pub prefix: &'a [u8],
    pub seek_key: &'a [u8],
    pub lower_inclusive: bool,
    pub upper_key_bound: Option<(&'a [u8], bool)>,
    pub reverse: bool,
    pub limit: usize,
    pub populate_cache: bool,
    pub need_values: bool,
}

/// Stack-allocated primary-row key encoder avoiding heap allocation for `pk.len() <= 61`.
pub(super) enum SmallPrimaryKey {
    Stack([u8; 64], usize),
    Heap(Vec<u8>),
}

impl SmallPrimaryKey {
    #[inline]
    pub(super) fn encode(table_id: u16, pk: &[u8]) -> Self {
        let total = 3usize.saturating_add(pk.len());
        if total <= 64 {
            let mut buf = [0u8; 64];
            let prefix = encode_namespace_prefix(table_id, NS_PRIMARY_ROW);
            buf[0..3].copy_from_slice(&prefix);
            buf[3..total].copy_from_slice(pk);
            Self::Stack(buf, total)
        } else {
            Self::Heap(encode_primary_row_key(table_id, pk))
        }
    }

    #[inline]
    pub(super) fn as_slice(&self) -> &[u8] {
        match self {
            Self::Stack(buf, len) => &buf[..*len],
            Self::Heap(vec) => vec.as_slice(),
        }
    }
}

trait SegCursor {
    fn current_key(&self) -> Option<&[u8]>;
    fn peek_raw(&self) -> Option<RawIdxEntry<'_>>;
    fn advance(&mut self) -> io::Result<()>;
}

impl<F: PlatformFile> SegCursor for IdxSegmentCursor<'_, F> {
    #[inline]
    fn current_key(&self) -> Option<&[u8]> {
        Self::current_key(self)
    }

    #[inline]
    fn peek_raw(&self) -> Option<RawIdxEntry<'_>> {
        Self::peek_raw(self)
    }

    #[inline]
    fn advance(&mut self) -> io::Result<()> {
        Self::advance(self)
    }
}

impl<F: PlatformFile> SegCursor for IdxSegmentRevCursor<'_, F> {
    #[inline]
    fn current_key(&self) -> Option<&[u8]> {
        Self::current_key(self)
    }

    #[inline]
    fn peek_raw(&self) -> Option<RawIdxEntry<'_>> {
        Self::peek_raw(self)
    }

    #[inline]
    fn advance(&mut self) -> io::Result<()> {
        Self::advance(self)
    }
}

struct SlabRowEmitter<'a, FS: PlatformFs> {
    eng: &'a StorageEngine<FS>,
    cx: &'a ReadCtx<'a, FS::File>,
    out: SlabScanOutput<'a, FS::File>,
    keys_only: bool,
    lazy_ok: bool,
    start_pks: usize,
    start_spans: usize,
    limit: usize,
    last_pk_buf: Vec<u8>,
    have_last_pk: bool,
}

impl<'a, FS: PlatformFs> SlabRowEmitter<'a, FS> {
    fn new(
        eng: &'a StorageEngine<FS>,
        cx: &'a ReadCtx<'a, FS::File>,
        out: SlabScanOutput<'a, FS::File>,
        lazy_ok: bool,
        opts: RangeScanOptions<'_>,
    ) -> Self {
        let start_pks = out.pks.len();
        let start_spans = out.spans.len();
        Self {
            eng,
            cx,
            out,
            keys_only: opts.keys_only,
            lazy_ok,
            start_pks,
            start_spans,
            limit: opts.limit,
            last_pk_buf: Vec::new(),
            have_last_pk: false,
        }
    }

    #[inline]
    fn reached_limit(&self) -> bool {
        if self.keys_only {
            self.out.pks.len() - self.start_pks >= self.limit
        } else {
            self.out.spans.len() - self.start_spans >= self.limit
        }
    }

    fn emit_pk_or_resume(&mut self, pk: &[u8]) {
        if self.out.lazy_blobs.is_some() {
            self.out.pks.push(pk.to_vec());
        } else {
            self.last_pk_buf.clear();
            self.last_pk_buf.extend_from_slice(pk);
            self.have_last_pk = true;
        }
    }

    fn emit(&mut self, pk: &[u8], full_key: &[u8], val: MergedValueRef<'_>) -> io::Result<bool> {
        if self.keys_only {
            self.out.pks.push(pk.to_vec());
            return Ok(true);
        }
        match val {
            MergedValueRef::Inline(v) => {
                let start = self.out.slab.len();
                self.out.slab.extend_from_slice(v);
                self.out.spans.push((start, self.out.slab.len()));
                if let Some(lazy_blobs) = &mut self.out.lazy_blobs {
                    lazy_blobs.push(None);
                }
                self.emit_pk_or_resume(pk);
                Ok(true)
            }
            MergedValueRef::Blob(ptr) => {
                if self.lazy_ok {
                    if let Some(lazy_blobs) = &mut self.out.lazy_blobs {
                        self.out.spans.push((0, 0));
                        lazy_blobs.push(Some(self.eng.pin_lazy_blob_src(self.cx.src, &ptr)));
                        self.out.pks.push(pk.to_vec());
                        return Ok(true);
                    }
                }
                let start = self.out.slab.len();
                if self.eng.read_flushed_blob_append_into_src(
                    self.cx.src,
                    &ptr,
                    full_key,
                    self.out.slab,
                )? {
                    self.out.spans.push((start, self.out.slab.len()));
                    if let Some(lazy_blobs) = &mut self.out.lazy_blobs {
                        lazy_blobs.push(None);
                    }
                    self.emit_pk_or_resume(pk);
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
        }
    }

    fn finish(self) {
        if !self.keys_only && self.out.lazy_blobs.is_none() && self.have_last_pk {
            self.out.pks.push(self.last_pk_buf);
        }
    }
}

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
            RangeScanOptions {
                lower: lower_b,
                upper: upper_b,
                reverse: false,
                limit,
                keys_only: true,
            },
            populate_cache,
            SlabScanOutput {
                pks: &mut pks,
                slab: &mut slab,
                spans: &mut spans,
                lazy_blobs: None,
            },
        )?;
        Ok(pks)
    }

    /// Streams live rows in `table_id` according to `opts` directly into `out` (`out.slab` and
    /// `out.spans`, or `out.pks` when `opts.keys_only` is `true` without dereferencing blobs),
    /// avoiding intermediate per-row `Vec<u8>` allocations.
    ///
    /// When `out.lazy_blobs` is `Some`, out-of-line blobs are NOT read from disk during the scan;
    /// instead, a placeholder span `(0, 0)` is recorded in `out.spans` and `Some(blob_ptr)` is
    /// pushed to `out.lazy_blobs`, deferring disk I/O and CRC32C checks until visited.
    pub fn scan_range_into_slab(
        &mut self,
        table_id: u16,
        opts: RangeScanOptions<'_>,
        mut out: SlabScanOutput<'_, FS::File>,
    ) -> io::Result<()> {
        let populate_cache = should_populate_range_cache(opts.lower, opts.upper, opts.limit);
        self.owner_read(|eng, cx| {
            eng.scan_range_into_slab_cx(cx, table_id, opts, populate_cache, out.reborrow())
        })
    }

    pub(super) fn scan_range_into_slab_cx(
        &self,
        cx: &ReadCtx<'_, FS::File>,
        table_id: u16,
        opts: RangeScanOptions<'_>,
        populate_cache: bool,
        out: SlabScanOutput<'_, FS::File>,
    ) -> io::Result<()> {
        if opts.limit == 0 {
            return Ok(());
        }
        let prefix = encode_namespace_prefix(table_id, NS_PRIMARY_ROW);
        let seek_buf = opts
            .lower
            .map(|(pk, _)| SmallPrimaryKey::encode(table_id, pk));
        let seek_key: &[u8] = seek_buf
            .as_ref()
            .map(SmallPrimaryKey::as_slice)
            .unwrap_or(&prefix);

        let upper_buf = opts
            .upper
            .map(|(pk, inc)| (SmallPrimaryKey::encode(table_id, pk), inc));
        let upper_key_slice: Option<(&[u8], bool)> =
            upper_buf.as_ref().map(|(k, inc)| (k.as_slice(), *inc));

        let lo_inc = opts.lower.map(|(_, inc)| inc).unwrap_or(true);
        if let Some((ub, ub_inc)) = upper_key_slice {
            if seek_key > ub || (seek_key == ub && (!lo_inc || !ub_inc)) {
                return Ok(());
            }
        }

        if !opts.keys_only {
            self.ensure_all_flushed_cx(cx)?;
        }
        // Lazy dereference cannot skip a row once the cursor has emitted it, so tables whose
        // corrupt rows must vanish (`CorruptPolicy::Evict`) verify blobs eagerly during the scan.
        let lazy_ok = self.corrupt_policy(table_id) == CorruptPolicy::Error;
        let mut emitter = SlabRowEmitter::new(self, cx, out, lazy_ok, opts);

        if self.idx_segments_for(cx.src).is_empty() && !opts.reverse {
            let Some(range) =
                PreparedPrefixRange::prepare(&prefix, seek_key, lo_inc, upper_key_slice)
            else {
                return Ok(());
            };
            for (k, entry, _) in self.mem_view_for(cx.src, range.lo_bound(), range.hi_bound()) {
                if !range.in_upper_and_prefix(k) {
                    break;
                }
                let val_ref = match &entry.value {
                    IndexValue::Inline(v) => MergedValueRef::Inline(v.as_slice()),
                    IndexValue::Blob(ptr) => MergedValueRef::Blob(*ptr),
                    IndexValue::Tombstone => continue,
                };
                if emitter.emit(&k[3..], k, val_ref)? && emitter.reached_limit() {
                    break;
                }
            }
            emitter.finish();
            return Ok(());
        }

        self.visit_merged_prefix_range_src(
            cx.src,
            MergeScanSpec {
                prefix: &prefix,
                seek_key,
                lower_inclusive: lo_inc,
                upper_key_bound: upper_key_slice,
                reverse: opts.reverse,
                limit: opts.limit,
                populate_cache,
                need_values: !opts.keys_only,
            },
            |key, _lsn, val| {
                if key.len() < 3 || !key.starts_with(&prefix) {
                    return Ok(false);
                }
                emitter.emit(&key[3..], key, val)
            },
        )?;

        emitter.finish();
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
            RangeScanOptions {
                lower: lower_bound,
                upper: upper_bound,
                reverse: false,
                limit,
                keys_only: false,
            },
            SlabScanOutput {
                pks: &mut unused_pks,
                slab,
                spans,
                lazy_blobs: None,
            },
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
        let seek_buf = lower_bound.map(|(pk, _)| SmallPrimaryKey::encode(table_id, pk));
        let seek_key: &[u8] = seek_buf
            .as_ref()
            .map(SmallPrimaryKey::as_slice)
            .unwrap_or(&prefix);
        let lo_inc = lower_bound.map(|(_, inc)| inc).unwrap_or(true);
        let upper_buf = upper_bound.map(|(pk, inc)| (SmallPrimaryKey::encode(table_id, pk), inc));
        let upper_slice = upper_buf.as_ref().map(|(k, inc)| (k.as_slice(), *inc));

        if let Some((ub, ub_inc)) = upper_slice {
            if seek_key > ub || (seek_key == ub && (!lo_inc || !ub_inc)) {
                return Ok((Vec::new(), None));
            }
        }

        if self.idx_segments.is_empty() {
            let Some(range) = PreparedPrefixRange::prepare(&prefix, seek_key, lo_inc, upper_slice)
            else {
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

        let populate_cache = should_populate_range_cache(lower_bound, upper_bound, limit);
        let merged = self.scan_merged_prefix_range(
            &prefix,
            seek_key,
            lo_inc,
            upper_slice,
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
            MergeScanSpec {
                prefix: &prefix,
                seek_key: &prefix,
                lower_inclusive: true,
                upper_key_bound: None,
                reverse: false,
                limit: usize::MAX,
                populate_cache: true,
                need_values: false,
            },
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

    /// Streams up to `opts.limit` live rows matching a secondary index range under `opts.prefix`
    /// into `out` (`out.slab` and `out.spans`, or `out.pks` when `opts.keys_only` is `true`),
    /// returning `Ok(Some(last_sec_key))` when `opts.limit` rows were emitted so the caller can
    /// resume the next chunk in `vtab_next`.
    pub fn scan_secondary_index_into_slab(
        &mut self,
        table_id: u16,
        index_id: u16,
        opts: SecondaryScanOptions<'_>,
        mut out: SlabScanOutput<'_, FS::File>,
    ) -> io::Result<Option<Vec<u8>>> {
        let populate_cache =
            should_populate_secondary_cache(opts.prefix, opts.upper_sec_key, opts.limit);
        self.owner_read(|eng, cx| {
            eng.scan_secondary_index_into_slab_cx(
                cx,
                table_id,
                index_id,
                opts,
                populate_cache,
                out.reborrow(),
            )
        })
    }

    fn collect_secondary_pks_cx(
        &self,
        cx: &ReadCtx<'_, FS::File>,
        table_id: u16,
        index_id: u16,
        spec: MergeScanSpec<'_>,
        pks_out: &mut Vec<Vec<u8>>,
        last_sec_key_buf: &mut Vec<u8>,
    ) -> io::Result<()> {
        if self.idx_segments_for(cx.src).is_empty() {
            let Some(range) = PreparedPrefixRange::prepare(
                spec.prefix,
                spec.seek_key,
                spec.lower_inclusive,
                spec.upper_key_bound,
            ) else {
                return Ok(());
            };
            let mut emitted = 0usize;
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
                    emitted += 1;
                    if emitted >= spec.limit {
                        break;
                    }
                }
            }
            return Ok(());
        }

        self.visit_merged_prefix_range_src(cx.src, spec, |k, _lsn, _val| {
            if let Some(pk) = decode_secondary_index_pk_slice(table_id, index_id, k) {
                pks_out.push(pk.to_vec());
                last_sec_key_buf.clear();
                last_sec_key_buf.extend_from_slice(k);
                Ok(true)
            } else {
                Ok(false)
            }
        })
    }

    pub(super) fn scan_secondary_index_into_slab_cx(
        &self,
        cx: &ReadCtx<'_, FS::File>,
        table_id: u16,
        index_id: u16,
        opts: SecondaryScanOptions<'_>,
        populate_cache: bool,
        mut out: SlabScanOutput<'_, FS::File>,
    ) -> io::Result<Option<Vec<u8>>> {
        if opts.limit == 0 {
            return Ok(None);
        }
        let (seek_slice, mut cur_inc) = opts.lower_sec_key;
        if let Some((ub, ub_inc)) = opts.upper_sec_key {
            if seek_slice > ub || (seek_slice == ub && (!cur_inc || !ub_inc)) {
                return Ok(None);
            }
        }

        if !opts.keys_only {
            self.ensure_all_flushed_cx(cx)?;
        }

        let start_pks = out.pks.len();
        let start_spans = out.spans.len();
        let mut last_sec_key_buf: Vec<u8> = Vec::new();

        if opts.keys_only {
            self.collect_secondary_pks_cx(
                cx,
                table_id,
                index_id,
                MergeScanSpec {
                    prefix: opts.prefix,
                    seek_key: seek_slice,
                    lower_inclusive: cur_inc,
                    upper_key_bound: opts.upper_sec_key,
                    reverse: false,
                    limit: opts.limit,
                    populate_cache,
                    need_values: false,
                },
                out.pks,
                &mut last_sec_key_buf,
            )?;
            if out.pks.len() - start_pks >= opts.limit && !last_sec_key_buf.is_empty() {
                return Ok(Some(last_sec_key_buf));
            }
            return Ok(None);
        }

        let mut cur_seek_owned: Option<Vec<u8>> = None;
        let mut batch_pks: Vec<Vec<u8>> = Vec::with_capacity(opts.limit.min(256));

        while out.spans.len() - start_spans < opts.limit {
            let batch_need = opts.limit - (out.spans.len() - start_spans);
            batch_pks.clear();
            let active_seek: &[u8] = cur_seek_owned.as_deref().unwrap_or(seek_slice);

            self.collect_secondary_pks_cx(
                cx,
                table_id,
                index_id,
                MergeScanSpec {
                    prefix: opts.prefix,
                    seek_key: active_seek,
                    lower_inclusive: cur_inc,
                    upper_key_bound: opts.upper_sec_key,
                    reverse: false,
                    limit: batch_need,
                    populate_cache,
                    need_values: false,
                },
                &mut batch_pks,
                &mut last_sec_key_buf,
            )?;

            if batch_pks.is_empty() {
                return Ok(None);
            }
            let scanned_count = batch_pks.len();
            for pk in batch_pks.drain(..) {
                if let Some(lazy_blobs) = &mut out.lazy_blobs {
                    if self
                        .get_row_into_lazy_cx(cx, table_id, &pk, out.slab, out.spans, lazy_blobs)?
                    {
                        out.pks.push(pk);
                    }
                } else {
                    self.get_row_into_cx(cx, table_id, &pk, out.slab, out.spans)?;
                }
            }
            if scanned_count < batch_need {
                return Ok(None);
            }
            if out.spans.len() - start_spans >= opts.limit {
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

    fn collect_merged_prefix_range_src(
        &self,
        src: ReadSource<'_, FS::File>,
        spec: MergeScanSpec<'_>,
    ) -> io::Result<Vec<IndexEntry>> {
        let mut out = Vec::new();
        self.visit_merged_prefix_range_src(src, spec, |key, lsn, val| {
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
        })?;
        Ok(out)
    }

    pub(super) fn scan_merged_prefix_view(
        &self,
        view: ReadView,
        prefix: &[u8],
    ) -> io::Result<Vec<IndexEntry>> {
        self.collect_merged_prefix_range_src(
            ReadSource::from_view(view),
            MergeScanSpec {
                prefix,
                seek_key: prefix,
                lower_inclusive: true,
                upper_key_bound: None,
                reverse: false,
                limit: usize::MAX,
                populate_cache: false,
                need_values: true,
            },
        )
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
        self.collect_merged_prefix_range_src(
            ReadSource::Owner,
            MergeScanSpec {
                prefix,
                seek_key,
                lower_inclusive,
                upper_key_bound,
                reverse: false,
                limit,
                populate_cache,
                need_values: true,
            },
        )
    }

    /// Streams up to `spec.limit` live entries from the ascending or descending K-way merge across
    /// `self.memtable` and `self.idx_segments` into `on_live` without allocating intermediate
    /// `IndexEntry` structs or per-entry `Vec<u8>` buffers.
    pub(super) fn visit_merged_prefix_range<F>(
        &self,
        spec: MergeScanSpec<'_>,
        on_live: F,
    ) -> io::Result<()>
    where
        F: FnMut(&[u8], u64, MergedValueRef<'_>) -> io::Result<bool>,
    {
        self.visit_merged_prefix_range_view(ReadView::Owner, spec, on_live)
    }

    pub(super) fn visit_merged_prefix_range_view<F>(
        &self,
        view: ReadView,
        spec: MergeScanSpec<'_>,
        on_live: F,
    ) -> io::Result<()>
    where
        F: FnMut(&[u8], u64, MergedValueRef<'_>) -> io::Result<bool>,
    {
        self.visit_merged_prefix_range_src(ReadSource::from_view(view), spec, on_live)
    }

    pub(super) fn visit_merged_prefix_range_src<F>(
        &self,
        src: ReadSource<'_, FS::File>,
        spec: MergeScanSpec<'_>,
        on_live: F,
    ) -> io::Result<()>
    where
        F: FnMut(&[u8], u64, MergedValueRef<'_>) -> io::Result<bool>,
    {
        if spec.limit == 0 {
            return Ok(());
        }
        let Some(range) = PreparedPrefixRange::prepare(
            spec.prefix,
            spec.seek_key,
            spec.lower_inclusive,
            spec.upper_key_bound,
        ) else {
            return Ok(());
        };

        let upper_for_overlap = range.upper_for_overlap();
        let prefix = spec.prefix;
        let segments = self.idx_segments_for(src);

        if spec.reverse {
            let seg_upper_arg = range.seg_upper_arg();
            let mut seg_cursors = Vec::with_capacity(segments.len());
            for seg in segments {
                if !seg.overlaps_range(range.eff_lo_key, upper_for_overlap) {
                    continue;
                }
                if !prefix.is_empty()
                    && seg.min_key() > prefix
                    && !seg.min_key().starts_with(prefix)
                {
                    continue;
                }
                seg_cursors
                    .push(seg.cursor_reverse_from_with_cache(seg_upper_arg, spec.populate_cache)?);
            }
            self.visit_merged_prefix_range_impl::<true, _, _>(
                src,
                &range,
                spec,
                seg_cursors,
                on_live,
            )
        } else {
            let mut seg_cursors = Vec::with_capacity(segments.len());
            for seg in segments {
                if !seg.overlaps_range(range.eff_lo_key, upper_for_overlap) {
                    continue;
                }
                if !prefix.is_empty()
                    && seg.min_key() > prefix
                    && !seg.min_key().starts_with(prefix)
                {
                    continue;
                }
                seg_cursors.push(seg.cursor_from_with_cache(
                    range.eff_lo_key,
                    range.eff_lo_inc,
                    spec.populate_cache,
                )?);
            }
            self.visit_merged_prefix_range_impl::<false, _, _>(
                src,
                &range,
                spec,
                seg_cursors,
                on_live,
            )
        }
    }

    fn visit_merged_prefix_range_impl<const REVERSE: bool, C, F>(
        &self,
        src: ReadSource<'_, FS::File>,
        range: &PreparedPrefixRange<'_>,
        spec: MergeScanSpec<'_>,
        mut seg_cursors: Vec<C>,
        mut on_live: F,
    ) -> io::Result<()>
    where
        C: SegCursor,
        F: FnMut(&[u8], u64, MergedValueRef<'_>) -> io::Result<bool>,
    {
        let mut mem_iter = self.mem_view_for_dir(src, range.lo_bound(), range.hi_bound(), REVERSE);
        let mut mem_exhausted = false;

        let mut emitted = 0usize;
        let mut target_key_buf: Vec<u8> = Vec::with_capacity(128);
        let mut best_val_buf: Vec<u8> = Vec::with_capacity(256);

        while emitted < spec.limit {
            let mut chosen_key: Option<&[u8]> = None;

            if !mem_exhausted {
                if let Some(k) = mem_iter.peek_key() {
                    let in_range = if REVERSE {
                        range.in_lower_and_prefix(k)
                    } else {
                        range.in_upper_and_prefix(k)
                    };
                    if in_range {
                        chosen_key = Some(k);
                    } else {
                        mem_exhausted = true;
                    }
                } else {
                    mem_exhausted = true;
                }
            }

            for cur in &seg_cursors {
                if let Some(k) = cur.current_key() {
                    let in_range = if REVERSE {
                        range.in_lower_and_prefix(k)
                    } else {
                        range.in_upper_and_prefix(k)
                    };
                    if in_range {
                        match chosen_key {
                            None => chosen_key = Some(k),
                            Some(ck) if (if REVERSE { k > ck } else { k < ck }) => {
                                chosen_key = Some(k);
                            }
                            _ => {}
                        }
                    }
                }
            }

            let Some(target_key_slice) = chosen_key else {
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
                        if spec.need_values || raw.val_type == 2 {
                            best_val_buf.clear();
                            best_val_buf.extend_from_slice(raw.val_bytes);
                        }
                    }
                    cur.advance()?;
                }
            }

            if let Some((seg_lsn, val_type)) = best_seg_meta {
                match val_type {
                    1 => {
                        let inline = if spec.need_values {
                            best_val_buf.as_slice()
                        } else {
                            &[]
                        };
                        if on_live(&target_key_buf, seg_lsn, MergedValueRef::Inline(inline))? {
                            emitted += 1;
                        }
                    }
                    2 => {
                        let ptr = RawIdxEntry::decode_blob_ptr(&best_val_buf)?;
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
                        let inline = if spec.need_values { v.as_slice() } else { &[] };
                        if on_live(&target_key_buf, mem_lsn, MergedValueRef::Inline(inline))? {
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
        let owner_tx = if src.is_owner() {
            self.tx_state.active_tx()
        } else {
            None
        };
        let ov_range = match owner_tx {
            Some(tx) if tx.overlay.overlaps_range(lo, hi) => {
                tx.overlay.map.range::<[u8], _>((lo, hi))
            }
            _ => EMPTY.range::<[u8], _>((lo, hi)),
        };
        let pending_ranges =
            if owner_tx.is_some() && !self.pending_group.is_empty() && !self.is_poisoned() {
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
