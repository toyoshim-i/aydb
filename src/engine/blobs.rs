//! Out-of-line blob segment management (`blob_std` / `blob_lru`), dead-byte accounting,
//! statement undo recording, corrupt-blob policy enforcement, lazy dereferencing, and prefetching.

use std::io;
use std::sync::atomic::Ordering;

use crate::blob::{
    BLOB_RECORD_HEADER_LEN, BlobSegmentReader, BlobSegmentWriter, DEFAULT_INLINE_THRESHOLD,
};
use crate::cache::BlobCacheKey;
use crate::index::{IndexEntry, IndexValue};
use crate::lru::{NS_PRIMARY_ROW, encode_namespace_prefix, encode_primary_row_key};
use crate::meta::{
    LruBlobSegmentMeta, StdBlobSegmentMeta, format_lru_blob_file_name, format_std_blob_file_name,
};
use crate::pal::{PlatformFile, PlatformFs};
use crate::wal::{BlobKind, BlobPtr, WalRecord};

use super::reader::{ReadCtx, ReadSource};
use super::tx::BlobUndo;
use super::{
    CorruptPolicy, LazyBlob, StorageEngine, TrackedLruBlob, TrackedStdBlob, WriteDurability,
};

/// A queued automatic deletion of a row with a corrupt blob or malformed metadata.
pub(super) struct PendingCorruptEviction {
    /// Key of the corrupt record (`'r'` row or `'t'` row-meta).
    key: Vec<u8>,
    /// The record observed as corrupt. A subsequent overwrite cancels the deletion.
    observed: IndexValue,
}

impl<FS: PlatformFs> StorageEngine<FS> {
    /// Sets the runtime blob-corruption policy for `table_id` (see [`CorruptPolicy`]).
    pub fn set_table_corrupt_policy(&mut self, table_id: u16, policy: CorruptPolicy) {
        self.corrupt_policies.insert(table_id, policy);
    }

    /// Removes any explicit policy for `table_id`, restoring the derived default.
    pub fn reset_table_corrupt_policy(&mut self, table_id: u16) {
        self.corrupt_policies.remove(&table_id);
    }

    /// Effective policy for `table_id`: the explicit setting, else `Evict` for capped tables and
    /// `Error` for all others.
    pub fn corrupt_policy(&self, table_id: u16) -> CorruptPolicy {
        match self.corrupt_policies.get(&table_id) {
            Some(p) => *p,
            None if self.capped_tables.contains_key(&table_id) => CorruptPolicy::Evict,
            None => CorruptPolicy::Error,
        }
    }

    /// Sets the per-table inline threshold override in bytes.
    pub fn set_table_inline_threshold(&mut self, table_id: u16, bytes: usize) {
        self.table_inline_thresholds.insert(table_id, bytes);
    }

    /// Removes any per-table inline threshold override for `table_id`, restoring default.
    pub fn reset_table_inline_threshold(&mut self, table_id: u16) {
        self.table_inline_thresholds.remove(&table_id);
    }

    /// Effective inline payload threshold for `table_id`.
    /// - If explicitly configured via `inline_max` / `set_table_inline_threshold`, returns that value.
    /// - Else if `table_id` is a capped table, returns `self.config.inline_threshold_bytes.min(DEFAULT_INLINE_THRESHOLD)` (1024),
    ///   preserving O(1) copyless LRU segment eviction.
    /// - Else returns `self.config.inline_threshold_bytes`.
    pub fn effective_inline_threshold(&self, table_id: u16) -> usize {
        if let Some(&thresh) = self.table_inline_thresholds.get(&table_id) {
            return thresh;
        }
        if self.capped_tables.contains_key(&table_id) {
            self.config
                .inline_threshold_bytes
                .min(DEFAULT_INLINE_THRESHOLD)
        } else {
            self.config.inline_threshold_bytes
        }
    }

    /// Number of blob verification failures (or dangling blob pointers) detected since open.
    pub fn corrupt_blob_detections(&self) -> u64 {
        self.corrupt_blob_detections.load(Ordering::Relaxed)
    }

    /// Records a detection only (used by cleanup paths that must tolerate corruption).
    pub(super) fn count_corrupt_detection(&self) {
        self.corrupt_blob_detections.fetch_add(1, Ordering::Relaxed);
    }

    /// Applies the table's policy to a failed blob read of the record at `key`
    /// (`[TableID | ns | pk]`), whose pointer is `ptr`. `Error` returns an `InvalidData` error
    /// naming the table and primary key; `Evict` queues the row for deletion and returns `Ok`.
    pub(super) fn note_corrupt_blob(&self, key: &[u8], ptr: &BlobPtr) -> io::Result<()> {
        self.count_corrupt_detection();
        let (table_id, pk) = if key.len() >= 3 {
            (u16::from_be_bytes([key[0], key[1]]), &key[3..])
        } else {
            (ptr.table_id, &key[..0])
        };
        match self.corrupt_policy(table_id) {
            CorruptPolicy::Error => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "corrupt blob (verification failed or segment missing) for table {table_id} pk 0x{} (segment {}, offset {})",
                    pk.iter().map(|b| format!("{b:02x}")).collect::<String>(),
                    ptr.seg_id,
                    ptr.offset
                ),
            )),
            CorruptPolicy::Evict => {
                self.queue_corrupt_eviction(table_id, pk, key, IndexValue::Blob(*ptr));
                Ok(())
            }
        }
    }

    pub(super) fn queue_corrupt_eviction(
        &self,
        table_id: u16,
        pk: &[u8],
        key: &[u8],
        observed: IndexValue,
    ) {
        if let Ok(mut q) = self.pending_corrupt_evictions.lock() {
            q.insert(
                (table_id, pk.to_vec()),
                PendingCorruptEviction {
                    key: key.to_vec(),
                    observed,
                },
            );
        }
    }

    pub(crate) fn has_pending_corrupt_evictions(&self) -> bool {
        self.pending_corrupt_evictions
            .lock()
            .map(|pending| !pending.is_empty())
            .unwrap_or(true)
    }

    pub(super) fn has_pending_corrupt_evictions_for_table(&self, table_id: u16) -> bool {
        self.pending_corrupt_evictions
            .lock()
            .map(|pending| pending.keys().any(|&(tid, _)| tid == table_id))
            .unwrap_or(true)
    }

    /// Deletes rows queued by [`CorruptPolicy::Evict`] reads.
    ///
    /// Funnel points: the start of `put_row_owned_with_hint`, `delete_row` and `flush_memtable`,
    /// plus the end of the outermost `commit_transaction` (SQL statements write inside a
    /// transaction, where draining is deferred). Never called from read paths, so a
    /// connection that merely reads never writes. Does nothing while a transaction is active
    /// or the engine is poisoned (entries stay queued). Each entry is re-checked: the row is
    /// only deleted if its record still has the observed bytes or blob pointer (it may have been
    /// overwritten or deleted since). Deletes use `BufferedFast` durability.
    pub(crate) fn drain_corrupt_evictions(&mut self) -> io::Result<()> {
        if self.tx_state.is_active() || self.is_poisoned() || !self.has_pending_corrupt_evictions()
        {
            return Ok(());
        }
        self.drain_pending_durable()?;
        let pending: Vec<((u16, Vec<u8>), PendingCorruptEviction)> =
            match self.pending_corrupt_evictions.lock() {
                Ok(mut q) if !q.is_empty() => q.drain().collect(),
                _ => return Ok(()),
            };
        let mut iter = pending.into_iter();
        while let Some(((table_id, pk), ev)) = iter.next() {
            let step = match self.lookup_raw_entry(&ev.key) {
                Ok(Some((_, current))) if current == ev.observed => self
                    .delete_row(table_id, &pk, WriteDurability::BufferedFast)
                    .map(|_| ()),
                Ok(_) => Ok(()),
                Err(e) => Err(e),
            };
            if let Err(e) = step {
                if let Ok(mut q) = self.pending_corrupt_evictions.lock() {
                    q.entry((table_id, pk)).or_insert(ev);
                    for (k, v) in iter {
                        q.entry(k).or_insert(v);
                    }
                }
                return Err(e);
            }
        }
        Ok(())
    }

    /// Captures `ptr` together with a pinned handle to its segment file (see [`LazyBlob`]).
    pub(super) fn pin_lazy_blob_src(
        &self,
        src: ReadSource<'_, FS::File>,
        ptr: &BlobPtr,
    ) -> LazyBlob<FS::File> {
        let reader = self.blob_reader_for(src, ptr).cloned();
        LazyBlob { ptr: *ptr, reader }
    }

    /// Dereferences an out-of-line blob pointer on demand for a cursor row, appending the verified
    /// blob payload to `slab` and returning its `(start_offset, end_offset)`.
    /// Also updates LRU read tracking if `table_id` is a capped table.
    pub fn dereference_lazy_blob_into(
        &mut self,
        table_id: u16,
        pk: &[u8],
        lazy: &LazyBlob<FS::File>,
        slab: &mut Vec<u8>,
    ) -> io::Result<Option<(usize, usize)>> {
        self.owner_read(|eng, cx| eng.dereference_lazy_blob_into_cx(cx, table_id, pk, lazy, slab))
    }

    pub(super) fn dereference_lazy_blob_into_cx(
        &self,
        cx: &ReadCtx<'_, FS::File>,
        table_id: u16,
        pk: &[u8],
        lazy: &LazyBlob<FS::File>,
        slab: &mut Vec<u8>,
    ) -> io::Result<Option<(usize, usize)>> {
        let ptr = &lazy.ptr;
        let mut stack_buf = [0u8; 64];
        let heap_buf;
        let row_key: &[u8] = if 3 + pk.len() <= stack_buf.len() {
            let tb = table_id.to_be_bytes();
            stack_buf[0] = tb[0];
            stack_buf[1] = tb[1];
            stack_buf[2] = NS_PRIMARY_ROW;
            stack_buf[3..3 + pk.len()].copy_from_slice(pk);
            &stack_buf[..3 + pk.len()]
        } else {
            heap_buf = encode_primary_row_key(table_id, pk);
            &heap_buf
        };

        // The pinned handle must see the blob bytes: committed blobs are always flushed; the
        // owner view may need its writer buffer flushed first (exclusive caller retries).
        self.ensure_flushed_cx(cx, ptr)?;

        let cache_key = BlobCacheKey::from_ptr(ptr, row_key);
        let start = slab.len();
        if self.blob_cache.get_append_into(&cache_key, slab) {
            let end = slab.len();
            self.track_read_hit(cx, table_id, pk);
            return Ok(Some((start, end)));
        }

        let Some(reader) = &lazy.reader else {
            // Segment was already absent when the pointer was captured: dangling pointer.
            self.note_corrupt_blob(row_key, ptr)?;
            return Ok(None);
        };
        if !reader.append_blob_into(ptr, Some(row_key), slab)? {
            slab.truncate(start);
            self.note_corrupt_blob(row_key, ptr)?;
            return Ok(None);
        }
        let end = slab.len();

        if self.is_blob_eligible_for_cache(ptr) {
            self.blob_cache.insert(cache_key, &slab[start..end]);
        }

        self.track_read_hit(cx, table_id, pk);

        Ok(Some((start, end)))
    }

    /// Prefetches and verifies a batch of upcoming out-of-line lazy blobs using [`PlatformFile::read_many_at`]
    /// into [`crate::cache::VerifiedBlobCache`], avoiding per-row system calls on subsequent cursor accesses.
    pub fn prefetch_lazy_blobs(
        &mut self,
        table_id: u16,
        candidates: &[(&[u8], &LazyBlob<FS::File>)],
    ) -> io::Result<()> {
        self.owner_read(|eng, cx| eng.prefetch_lazy_blobs_cx(cx, table_id, candidates))
    }

    pub(super) fn prefetch_lazy_blobs_cx(
        &self,
        _cx: &ReadCtx<'_, FS::File>,
        table_id: u16,
        candidates: &[(&[u8], &LazyBlob<FS::File>)],
    ) -> io::Result<()> {
        if self.config.blob_cache_bytes == 0 || candidates.is_empty() {
            return Ok(());
        }

        struct PrefetchItem<'a, F: PlatformFile> {
            cache_key: BlobCacheKey,
            key_range: core::ops::Range<usize>,
            ptr: BlobPtr,
            reader: &'a BlobSegmentReader<F>,
        }

        let mut key_slab: Vec<u8> = Vec::new();
        let mut to_fetch: Vec<PrefetchItem<'_, FS::File>> = Vec::with_capacity(candidates.len());
        let mut stack_key = [0u8; 64];
        let mut heap_key = Vec::new();
        for (pk, lazy) in candidates {
            let ptr = &lazy.ptr;
            if !self.is_blob_eligible_for_cache(ptr)
                || (ptr.len as usize) < BLOB_RECORD_HEADER_LEN
                || (ptr.offset as u64).checked_add(ptr.len as u64).is_none()
            {
                continue;
            }
            if let Some(ref reader) = lazy.reader {
                let row_key: &[u8] = if 3 + pk.len() <= stack_key.len() {
                    let tb = table_id.to_be_bytes();
                    stack_key[0] = tb[0];
                    stack_key[1] = tb[1];
                    stack_key[2] = NS_PRIMARY_ROW;
                    stack_key[3..3 + pk.len()].copy_from_slice(pk);
                    &stack_key[..3 + pk.len()]
                } else {
                    heap_key.clear();
                    heap_key.extend_from_slice(&encode_namespace_prefix(table_id, NS_PRIMARY_ROW));
                    heap_key.extend_from_slice(pk);
                    heap_key.as_slice()
                };
                let cache_key = BlobCacheKey::from_ptr(ptr, row_key);
                if self.blob_cache.contains(&cache_key) {
                    continue;
                }
                let start = key_slab.len();
                key_slab.extend_from_slice(row_key);
                to_fetch.push(PrefetchItem {
                    cache_key,
                    key_range: start..key_slab.len(),
                    ptr: *ptr,
                    reader,
                });
            }
        }

        if to_fetch.is_empty() {
            return Ok(());
        }

        to_fetch.sort_unstable_by_key(|c| {
            (c.ptr.kind as u8, c.ptr.table_id, c.ptr.seg_id, c.ptr.offset)
        });

        let verify_and_cache = |cand: &PrefetchItem<'_, FS::File>, record_buf: &[u8]| {
            if let Some(val) = BlobSegmentReader::<FS::File>::verify_blob_record(
                record_buf,
                cand.ptr.crc32c,
                Some(&key_slab[cand.key_range.clone()]),
            ) {
                self.blob_cache.insert(cand.cache_key, val);
            }
        };

        let mut scratch: Vec<u8> = Vec::new();
        let mut group_start = 0;
        while group_start < to_fetch.len() {
            let cur_kind = to_fetch[group_start].ptr.kind;
            let cur_tbl = to_fetch[group_start].ptr.table_id;
            let cur_seg = to_fetch[group_start].ptr.seg_id;

            let mut group_end = group_start + 1;
            while group_end < to_fetch.len()
                && to_fetch[group_end].ptr.kind == cur_kind
                && to_fetch[group_end].ptr.table_id == cur_tbl
                && to_fetch[group_end].ptr.seg_id == cur_seg
            {
                group_end += 1;
            }

            let group = &to_fetch[group_start..group_end];
            let reader = group[0].reader;

            let mut i = 0;
            while i < group.len() {
                let cluster_start = group[i].ptr.offset as u64;
                let mut cluster_end = cluster_start + group[i].ptr.len as u64;
                let mut j = i + 1;
                while j < group.len() {
                    let next_start = group[j].ptr.offset as u64;
                    let next_end = next_start + group[j].ptr.len as u64;
                    let gap = next_start.saturating_sub(cluster_end);
                    let new_span = next_end.max(cluster_end) - cluster_start;
                    if gap <= 16 * 1024 && new_span <= 64 * 1024 {
                        cluster_end = cluster_end.max(next_end);
                        j += 1;
                    } else {
                        break;
                    }
                }

                if j == i + 1 {
                    let cand = &group[i];
                    let len = cand.ptr.len as usize;
                    scratch.resize(len, 0);
                    if reader
                        .file()
                        .read_exact_at(&mut scratch[..len], cluster_start)
                        .is_ok()
                    {
                        verify_and_cache(cand, &scratch[..len]);
                    }
                } else {
                    let span = (cluster_end - cluster_start) as usize;
                    scratch.resize(span, 0);
                    if reader
                        .file()
                        .read_exact_at(&mut scratch[..span], cluster_start)
                        .is_ok()
                    {
                        for cand in &group[i..j] {
                            let rel = (cand.ptr.offset as u64 - cluster_start) as usize;
                            let len = cand.ptr.len as usize;
                            verify_and_cache(cand, &scratch[rel..rel + len]);
                        }
                    } else {
                        // Fault isolation: fall back to per-item reads so a single corrupt or
                        // out-of-bounds pointer never discards valid sibling blobs.
                        for cand in &group[i..j] {
                            let len = cand.ptr.len as usize;
                            scratch.resize(len, 0);
                            if reader
                                .file()
                                .read_exact_at(&mut scratch[..len], cand.ptr.offset as u64)
                                .is_ok()
                            {
                                verify_and_cache(cand, &scratch[..len]);
                            }
                        }
                    }
                }

                i = j;
            }

            group_start = group_end;
        }

        Ok(())
    }

    pub(super) fn flush_active_blob_buffers(&mut self) -> io::Result<()> {
        self.storage
            .try_healthy(&self.commit_gate)?
            .flush_active_blob_buffers(&mut self.std_blobs, &mut self.lru_blobs)
    }

    /// Syncs every active blob writer. A failure poisons the engine, see
    /// [`Self::sync_active_std_writer`].
    pub(super) fn sync_active_blob_writers(&mut self) -> io::Result<()> {
        self.sync_active_std_writer()?;
        let table_ids = self.storage.active_lru_table_ids();
        for table_id in table_ids {
            self.sync_active_lru_writer(table_id)?;
        }
        Ok(())
    }

    /// Syncs the active standard blob writer (if any) and records its length.
    ///
    /// A failed sync poisons the engine: the kernel may have dropped the dirty pages and marked
    /// them clean, so a retried sync can report success for bytes that never reached the disk.
    /// Continuing would let a later checkpoint or WAL record cover a lost blob. Once poisoned,
    /// no further blob sync is attempted.
    pub(super) fn sync_active_std_writer(&mut self) -> io::Result<()> {
        self.storage
            .try_healthy(&self.commit_gate)?
            .sync_active_std_writer(&mut self.std_blobs, false)
    }

    /// Per-table LRU counterpart of [`Self::sync_active_std_writer`].
    pub(super) fn sync_active_lru_writer(&mut self, table_id: u16) -> io::Result<()> {
        self.storage
            .try_healthy(&self.commit_gate)?
            .sync_active_lru_writer(table_id, &mut self.lru_blobs, false)
    }

    #[inline]
    fn note_in_flight_autocommit_blob(
        &mut self,
        kind: BlobKind,
        tid_key: u16,
        seg_id: u64,
        prev_len: u64,
    ) {
        if !self.tx_state.is_active()
            && !self
                .in_flight_autocommit_blob_starts
                .iter()
                .any(|&(k, t, s, _)| k == kind && t == tid_key && s == seg_id)
        {
            self.in_flight_autocommit_blob_starts
                .push((kind, tid_key, seg_id, prev_len));
        }
    }

    #[inline]
    fn push_statement_blob_undo(&mut self, undo: BlobUndo) {
        if self.tx_state.statement_savepoint_active() {
            if let Some(tx) = self.tx_state.active_tx_mut() {
                tx.statement_blob_undo.push(undo);
            }
        }
    }

    fn record_blob_append_accounting(
        &mut self,
        kind: BlobKind,
        tid_key: u16,
        seg_id: u64,
        prev_len: u64,
        new_len: u64,
    ) {
        if !self.tx_state.is_active() {
            self.note_in_flight_autocommit_blob(kind, tid_key, seg_id, prev_len);
        }
        let delta = new_len.saturating_sub(prev_len);
        if delta > 0 {
            self.push_statement_blob_undo(BlobUndo::Appended {
                kind,
                table_id: tid_key,
                seg_id,
                len: delta,
            });
        }
    }

    pub(super) fn append_to_blob_segment(
        &mut self,
        kind: BlobKind,
        table_id: u16,
        key: &[u8],
        value: &[u8],
    ) -> io::Result<BlobPtr> {
        self.check_not_poisoned()?;
        let needed = (16 + key.len() + value.len()) as u64;
        match kind {
            BlobKind::Standard => {
                if self
                    .storage
                    .active_std_needs_roll(needed, self.config.max_blob_segment_bytes)
                {
                    if self.active_std_seq_id().is_some() {
                        self.storage
                            .try_healthy(&self.commit_gate)?
                            .sync_active_std_writer(&mut self.std_blobs, true)?;
                    }
                    let seq_id = self.healthy()?.reserve_seq_id()?;
                    let writer = BlobSegmentWriter::open(&*self.fs, BlobKind::Standard, 0, seq_id)?;
                    let reader = writer.reader();
                    let mut std_metas = self.build_checkpointed_std_blob_metas();
                    std_metas.push(StdBlobSegmentMeta {
                        seq_id,
                        file_len: 0,
                        dead_bytes: 0,
                    });
                    std_metas.sort_by_key(|m| m.seq_id);
                    if let Err(e) = self.healthy()?.commit_and_install_std_writer(writer, |st| {
                        st.std_blob_segments = std_metas;
                    }) {
                        drop(reader);
                        if !self.meta_mgr().is_poisoned() {
                            let _ = self.fs.remove_file(&format_std_blob_file_name(seq_id));
                        }
                        return Err(e);
                    }
                    self.invalidate_snapshot_std_blobs();
                    self.std_blobs.insert(
                        seq_id,
                        TrackedStdBlob {
                            reader,
                            file_len: 0,
                            dead_bytes: 0,
                        },
                    );
                }
                let (seg_id, ptr, new_len) = self.healthy()?.append_std_blob(key, value)?;
                if let Some(prev_len) = self.std_blobs.get_mut(&seg_id).map(|b| {
                    let prev = b.file_len;
                    b.file_len = new_len;
                    prev
                }) {
                    self.record_blob_append_accounting(
                        BlobKind::Standard,
                        0,
                        seg_id,
                        prev_len,
                        new_len,
                    );
                }
                Ok(ptr)
            }
            BlobKind::Lru => {
                if self.storage.active_lru_needs_roll(
                    table_id,
                    needed,
                    self.config.max_blob_segment_bytes,
                ) {
                    if self.active_lru_seq_id(table_id).is_some() {
                        self.storage
                            .try_healthy(&self.commit_gate)?
                            .sync_active_lru_writer(table_id, &mut self.lru_blobs, true)?;
                    }
                    let seq_id = self.healthy()?.reserve_seq_id()?;
                    let writer =
                        BlobSegmentWriter::open(&*self.fs, BlobKind::Lru, table_id, seq_id)?;
                    let reader = writer.reader();
                    let mut lru_metas = self.build_checkpointed_lru_blob_metas();
                    lru_metas.push(LruBlobSegmentMeta {
                        seq_id,
                        file_len: 0,
                        dead_bytes: 0,
                        table_id,
                    });
                    lru_metas.sort_by_key(|m| (m.table_id, m.seq_id));
                    if let Err(e) =
                        self.healthy()?
                            .commit_and_install_lru_writer(table_id, writer, |st| {
                                st.lru_blob_segments = lru_metas;
                            })
                    {
                        drop(reader);
                        if !self.meta_mgr().is_poisoned() {
                            let _ = self
                                .fs
                                .remove_file(&format_lru_blob_file_name(table_id, seq_id));
                        }
                        return Err(e);
                    }
                    self.invalidate_snapshot_lru_blobs();
                    self.lru_blobs.insert(
                        (table_id, seq_id),
                        TrackedLruBlob {
                            table_id,
                            reader,
                            file_len: 0,
                            dead_bytes: 0,
                        },
                    );
                }
                let (seg_id, ptr, new_len) =
                    self.healthy()?.append_lru_blob(table_id, key, value)?;
                if let Some(prev_len) = self.lru_blobs.get_mut(&(table_id, seg_id)).map(|b| {
                    let prev = b.file_len;
                    b.file_len = new_len;
                    prev
                }) {
                    self.record_blob_append_accounting(
                        BlobKind::Lru,
                        table_id,
                        seg_id,
                        prev_len,
                        new_len,
                    );
                }
                Ok(ptr)
            }
        }
    }

    pub(super) fn record_dead_blob_bytes(&mut self, ptr: &BlobPtr) {
        let (tid_key, delta) = match ptr.kind {
            BlobKind::Standard => {
                let delta = self.std_blobs.get_mut(&ptr.seg_id).map_or(0, |b| {
                    let prev_dead = b.dead_bytes;
                    b.dead_bytes = b.dead_bytes.saturating_add(ptr.len as u64);
                    if b.dead_bytes > prev_dead {
                        self.unrelocatable_std_blobs.remove(&ptr.seg_id);
                    }
                    b.dead_bytes.saturating_sub(prev_dead)
                });
                (0, delta)
            }
            BlobKind::Lru => {
                let delta = self
                    .lru_blobs
                    .get_mut(&(ptr.table_id, ptr.seg_id))
                    .map_or(0, |b| {
                        let prev_dead = b.dead_bytes;
                        b.dead_bytes = b.dead_bytes.saturating_add(ptr.len as u64);
                        b.dead_bytes.saturating_sub(prev_dead)
                    });
                (ptr.table_id, delta)
            }
        };
        if delta > 0 {
            self.push_statement_blob_undo(BlobUndo::MarkedDead {
                kind: ptr.kind,
                table_id: tid_key,
                seg_id: ptr.seg_id,
                len: delta,
            });
        }
    }

    /// Cleans up uncommitted blob pointers for a batch whose WAL commit did not happen or failed.
    ///
    /// Clears autocommit blob undo tracking when no transaction is active and records every
    /// pointer in `ptrs` as dead bytes so compaction or eviction can reclaim them later.
    pub(super) fn abort_uncommitted_blobs(&mut self, ptrs: &[BlobPtr]) {
        if !self.tx_state.is_active() {
            self.in_flight_autocommit_blob_starts.clear();
        }
        for p in ptrs {
            self.record_dead_blob_bytes(p);
        }
    }

    /// Encodes `meta_key` -> `meta_bytes` into `records`, spilling oversized metadata to
    /// `blob_std` and syncing `active_std_writer` before autocommit `SyncTransactional` WAL
    /// appends (`Write-Blob-Before-WAL`).
    pub(super) fn append_std_meta_record(
        &mut self,
        meta_key: Vec<u8>,
        meta_bytes: Vec<u8>,
        durability: WriteDurability,
        records: &mut Vec<WalRecord>,
    ) -> io::Result<Option<BlobPtr>> {
        if !IndexEntry::inline_fits_in_block(meta_key.len(), meta_bytes.len()) {
            let ptr = self.append_to_blob_segment(BlobKind::Standard, 0, &meta_key, &meta_bytes)?;
            if durability == WriteDurability::SyncTransactional && !self.tx_state.is_active() {
                if let Err(e) = self.sync_active_std_writer() {
                    self.abort_uncommitted_blobs(std::slice::from_ref(&ptr));
                    return Err(e);
                }
            }
            records.push(WalRecord::PutBlobPtr {
                key: meta_key,
                blob_ptr: ptr,
            });
            Ok(Some(ptr))
        } else {
            records.push(WalRecord::PutInline {
                key: meta_key,
                value: meta_bytes,
            });
            Ok(None)
        }
    }

    pub(super) fn ensure_blob_flushed(&mut self, ptr: &BlobPtr) -> io::Result<()> {
        self.storage
            .ensure_blob_flushed_for_read(ptr, &mut self.std_blobs, &mut self.lru_blobs)
    }

    /// Reads and verifies a blob; on failure applies the table's [`CorruptPolicy`]
    /// (`Err(InvalidData)` for `Error`, `Ok(None)` + queued eviction for `Evict`).
    pub(super) fn dereference_blob(
        &mut self,
        ptr: &BlobPtr,
        expected_key: &[u8],
    ) -> io::Result<Option<Vec<u8>>> {
        let r = self.dereference_blob_raw(ptr, expected_key)?;
        if r.is_none() {
            self.note_corrupt_blob(expected_key, ptr)?;
        }
        Ok(r)
    }

    /// Like [`Self::dereference_blob`] but never applies the policy: a failure is only counted and
    /// reported as `Ok(None)`. For cleanup/maintenance paths (delete, overwrite, compaction
    /// relocation, capped-table reconciliation) that must tolerate corrupt blobs.
    pub(super) fn dereference_blob_lenient(
        &mut self,
        ptr: &BlobPtr,
        expected_key: &[u8],
    ) -> io::Result<Option<Vec<u8>>> {
        let r = self.dereference_blob_raw(ptr, expected_key)?;
        if r.is_none() {
            self.count_corrupt_detection();
        }
        Ok(r)
    }

    pub(super) fn dereference_blob_raw(
        &mut self,
        ptr: &BlobPtr,
        expected_key: &[u8],
    ) -> io::Result<Option<Vec<u8>>> {
        self.ensure_blob_flushed(ptr)?;
        self.read_blob_ref(ptr, expected_key)
    }

    fn is_blob_eligible_for_cache(&self, ptr: &BlobPtr) -> bool {
        if self.config.blob_cache_bytes == 0 || (ptr.len as usize) > 16 * 1024 {
            return false;
        }
        self.storage.is_blob_flushed_for_cache(ptr)
    }

    /// Reads and verifies a blob that is already flushed (no policy applied).
    pub(super) fn read_blob_ref(
        &self,
        ptr: &BlobPtr,
        expected_key: &[u8],
    ) -> io::Result<Option<Vec<u8>>> {
        self.read_blob_ref_src(ReadSource::Owner, ptr, expected_key)
    }

    pub(super) fn read_blob_ref_src(
        &self,
        src: ReadSource<'_, FS::File>,
        ptr: &BlobPtr,
        expected_key: &[u8],
    ) -> io::Result<Option<Vec<u8>>> {
        let mut out = Vec::new();
        let cache_key = BlobCacheKey::from_ptr(ptr, expected_key);
        if self.blob_cache.get_append_into(&cache_key, &mut out) {
            return Ok(Some(out));
        }

        let res = match self.blob_reader_for(src, ptr) {
            Some(reader) => reader.read_blob(ptr, Some(expected_key))?,
            None => return Ok(None),
        };

        if let Some(payload) = &res {
            if self.is_blob_eligible_for_cache(ptr) {
                self.blob_cache.insert(cache_key, payload);
            }
        }
        Ok(res)
    }

    pub(super) fn dereference_blob_append_into_cx(
        &self,
        cx: &ReadCtx<'_, FS::File>,
        ptr: &BlobPtr,
        expected_key: &[u8],
        out: &mut Vec<u8>,
    ) -> io::Result<bool> {
        self.ensure_flushed_cx(cx, ptr)?;
        self.read_flushed_blob_append_into_src(cx.src, ptr, expected_key, out)
    }

    pub(super) fn read_flushed_blob_append_into_src(
        &self,
        src: ReadSource<'_, FS::File>,
        ptr: &BlobPtr,
        expected_key: &[u8],
        out: &mut Vec<u8>,
    ) -> io::Result<bool> {
        let cache_key = BlobCacheKey::from_ptr(ptr, expected_key);

        let orig_len = out.len();
        if self.blob_cache.get_append_into(&cache_key, out) {
            return Ok(true);
        }

        let reader = self.blob_reader_for(src, ptr);
        let ok = match reader {
            Some(reader) if out.is_empty() => {
                reader.read_blob_into(ptr, Some(expected_key), out)?
            }
            Some(reader) => reader.append_blob_into(ptr, Some(expected_key), out)?,
            None => false,
        };
        if !ok {
            // Corruption or dangling pointer: apply the table's policy (Err for `Error`).
            self.note_corrupt_blob(expected_key, ptr)?;
        } else if self.is_blob_eligible_for_cache(ptr) {
            self.blob_cache.insert(cache_key, &out[orig_len..]);
        }
        Ok(ok)
    }
}
