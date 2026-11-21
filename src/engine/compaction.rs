//! MemTable flushing, size-tiered SSTable auto-compaction, fragmented blob relocation,
//! and copy-less dead-blob segment reclamation.

use std::collections::HashSet;
use std::io;
use std::sync::Arc;

use crate::index::{IdxSegmentReader, IndexValue, compact_idx_segments};
use crate::meta::{
    IdxSegmentMeta, LruBlobSegmentMeta, StdBlobSegmentMeta, format_idx_file_name,
    format_lru_blob_file_name, format_std_blob_file_name,
};
use crate::pal::PlatformFs;
use crate::wal::{BlobKind, WalRecord};

use super::{StorageEngine, WriteDurability};

struct SettledBlobCheckpoint {
    dead_std_seqs: Vec<u64>,
    dead_lru_keys: Vec<(u16, u64)>,
    dead_blob_files: Vec<String>,
    std_metas: Vec<StdBlobSegmentMeta>,
    lru_metas: Vec<LruBlobSegmentMeta>,
}

/// A prepared compaction: owns everything the merge needs, independent of the engine, so it can
/// run on another thread while the engine keeps serving reads and writes.
pub struct CompactionJob<FS: PlatformFs> {
    fs: Arc<FS>,
    /// Input segments, newest first.
    inputs: Vec<Arc<IdxSegmentReader<FS::File>>>,
    out_seq_id: u64,
    level: u16,
    bloom_bits: usize,
    drop_tombstones: bool,
}

/// Result of [`CompactionJob::run`], handed to [`StorageEngine::install_compaction`].
pub struct CompactionOutput<FS: PlatformFs> {
    new_segment: Arc<IdxSegmentReader<FS::File>>,
    /// Input segments, newest first.
    inputs: Vec<Arc<IdxSegmentReader<FS::File>>>,
    drop_tombstones: bool,
}

impl<FS: PlatformFs> CompactionJob<FS> {
    /// `SeqID` reserved for the output segment.
    pub fn output_seq_id(&self) -> u64 {
        self.out_seq_id
    }

    /// Number of input segments being merged.
    pub fn input_count(&self) -> usize {
        self.inputs.len()
    }

    /// Whether the merge drops tombstones (only true for runs starting at the oldest segment).
    pub fn drops_tombstones(&self) -> bool {
        self.drop_tombstones
    }

    /// Performs the K-way merge and writes `idx_<out_seq_id>.db`. Needs no engine access. On
    /// failure the partial output file is removed.
    pub fn run(self) -> io::Result<CompactionOutput<FS>> {
        let refs: Vec<&IdxSegmentReader<FS::File>> = self.inputs.iter().map(|a| &**a).collect();
        let res = compact_idx_segments(
            &*self.fs,
            &refs,
            self.out_seq_id,
            self.level,
            self.bloom_bits,
            self.drop_tombstones,
        );
        drop(refs);
        match res {
            Ok(res) => Ok(CompactionOutput {
                new_segment: Arc::new(res.new_segment),
                inputs: self.inputs,
                drop_tombstones: self.drop_tombstones,
            }),
            Err(e) => {
                let _ = self.fs.remove_file(&format_idx_file_name(self.out_seq_id));
                Err(e)
            }
        }
    }
}

impl<FS: PlatformFs> CompactionOutput<FS> {
    /// `SeqID` of the produced segment.
    pub fn output_seq_id(&self) -> u64 {
        self.new_segment.seq_id()
    }
}

impl<FS: PlatformFs> StorageEngine<FS> {
    /// Flushes the current `MemTable` to a new immutable `idx_<SeqID>.db` segment, rotates both
    /// WAL streams to `epoch + 1` in $O(1)$ without zero-filling, atomically commits `boot_meta.db`,
    /// and triggers automatic compaction if `auto_compact_segment_threshold` is reached.
    /// If called while an explicit transaction is active (`tx_depth > 0`), defers checkpointing
    /// until `commit_transaction` so provisional state is never published to SSTables.
    pub fn flush_memtable(&mut self) -> io::Result<()> {
        self.check_not_poisoned()?;
        if self.tx_depth > 0 {
            self.deferred_flush_on_commit = true;
            return Ok(());
        }
        self.drain_pending_durable()?;
        self.drain_corrupt_evictions()?;
        self.flush_memtable_no_auto_compact()?;
        // This flush also covers any compaction owed by an earlier full-WAL rotation.
        self.auto_compaction_due = false;
        self.run_auto_compaction()
    }

    /// Runs or schedules automatic compaction when the segment count calls for it (see
    /// [`Self::flush_memtable`]). Callers guarantee `tx_depth == 0` and that no batch holds an
    /// unappended LSN, because compaction may relocate blobs and so write to the WAL.
    fn run_auto_compaction(&mut self) -> io::Result<()> {
        if self.tx_depth == 0 && self.compaction_pending() {
            // Background mode only marks compaction as pending (observable through
            // `compaction_pending()`); the merge runs outside the engine. The inline fallback
            // bounds segment growth (and read amplification) if the worker falls behind.
            let limit = if self.config.max_segments_before_inline_compaction > 0 {
                self.config.max_segments_before_inline_compaction
            } else {
                self.config.auto_compact_segment_threshold.saturating_mul(2)
            };
            if !self.config.background_compaction || self.idx_segments.len() >= limit {
                self.compact_auto_tiered()?;
            } else if let Some(hook) = &self.compaction_hook {
                hook();
            }
        } else if self.tx_depth == 0
            && self.config.background_compaction
            && !self.relocating_std_blobs
            && self.has_fragmented_std_blobs(0.5)
        {
            if let Some(hook) = &self.compaction_hook {
                hook();
            }
        }
        Ok(())
    }

    /// Carries out the automatic compaction that a full-WAL rotation had to skip (it flushes with
    /// [`Self::flush_memtable_no_auto_compact`] because its batch already holds an LSN). Call it
    /// only at points where no batch holds an unappended LSN. While a transaction is active or a
    /// group commit is still unpublished the flag stays set for the next safe point. A failure
    /// is recorded as maintenance failure and never fails the write that triggered it.
    pub(super) fn run_due_auto_compaction(&mut self) {
        if !self.auto_compaction_due
            || self.tx_depth > 0
            || !self.pending_group.is_empty()
            || self.poisoned.is_some()
        {
            return;
        }
        self.auto_compaction_due = false;
        if let Err(e) = self.run_auto_compaction() {
            self.record_maintenance_failure(&e);
        }
    }

    /// Brings the persisted state up to date so that the next `open` has no replay work.
    ///
    /// A non-empty `MemTable` is flushed to a new `idx` segment, which also commits the blob
    /// segment lengths, dead bytes, and quota totals. With an empty `MemTable` there is nothing
    /// to flush, but blob segments may have grown since the last commit (for example by a rolled
    /// back transaction); their lengths are committed so that `open` does not walk every `idx`
    /// segment to find the end of the valid data. Dead bytes can be persisted in full here
    /// because an empty `MemTable` means no logged mutation is still waiting for a checkpoint.
    ///
    /// Fails with `InvalidInput` while a transaction is active. Dropping the engine does not
    /// call this; a drop without `checkpoint` is treated as a crash by the next `open`.
    pub fn checkpoint(&mut self) -> io::Result<()> {
        self.check_not_poisoned()?;
        if self.tx_depth > 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "checkpoint is not allowed while a transaction is active",
            ));
        }
        self.drain_pending_durable()?;
        self.drain_corrupt_evictions()?;
        if !self.memtable.is_empty() {
            return self.flush_memtable();
        }
        self.buf_wal.flush_buffered_to_os()?;
        self.sync_active_blob_writers()?;
        self.release_unlinkable_blob_files()?;

        let std_metas: Vec<StdBlobSegmentMeta> = self
            .std_blobs
            .iter()
            .map(|(&seq_id, b)| StdBlobSegmentMeta {
                seq_id,
                file_len: b.file_len,
                dead_bytes: b.dead_bytes,
            })
            .collect();
        let lru_metas: Vec<LruBlobSegmentMeta> = self
            .lru_blobs
            .iter()
            .map(|(&(_tid, seq_id), b)| LruBlobSegmentMeta {
                seq_id,
                file_len: b.file_len,
                dead_bytes: b.dead_bytes,
                table_id: b.table_id,
            })
            .collect();
        let quota_metas = self.build_quota_metas();
        let st = self.meta_mgr.state();
        // `build_quota_metas` iterates a `HashMap`, so compare the quotas in table order.
        let sorted_quotas = |v: &[crate::meta::TableQuotaMeta]| {
            let mut v = v.to_vec();
            v.sort_by_key(|q| q.table_id);
            v
        };
        if st.std_blob_segments == std_metas
            && st.lru_blob_segments == lru_metas
            && sorted_quotas(&st.table_quotas) == sorted_quotas(&quota_metas)
        {
            return Ok(());
        }
        self.meta_mgr.commit_update(|st| {
            st.std_blob_segments = std_metas;
            st.lru_blob_segments = lru_metas;
            st.table_quotas = quota_metas;
        })
    }

    pub(super) fn flush_memtable_no_auto_compact(&mut self) -> io::Result<()> {
        self.check_not_poisoned()?;
        if self.tx_depth > 0 {
            self.deferred_flush_on_commit = true;
            return Ok(());
        }
        self.drain_pending_durable()?;
        self.flush_pending_tx_records()?;
        self.buf_wal.flush_buffered_to_os()?;
        if self.memtable.is_empty() {
            self.release_unlinkable_blob_files()?;
            if !self.meta_mgr.state().pending_delete_files.is_empty() {
                self.meta_mgr.try_process_pending_deletes();
            }
            return Ok(());
        }

        // Ensure all active blob segments are durably flushed before checkpointing `boot_meta.db`
        self.sync_active_blob_writers()?;

        // Consumed in memory before the file exists, so a failed write whose cleanup also fails
        // never leaves a file that a retry would reuse (ARCHITECTURE.md section 2.3). The commit
        // below persists the advanced counter.
        let new_seq_id = self.meta_mgr.reserve_seq_id();

        let mut new_reader = match self.memtable.write_to_segment(
            &*self.fs,
            new_seq_id,
            self.config.bloom_bits_per_key,
        ) {
            Ok(r) => r,
            Err(e) => {
                let _ = self.fs.remove_file(&format_idx_file_name(new_seq_id));
                return Err(e);
            }
        };
        new_reader.set_block_cache(Some(Arc::clone(&self.block_cache)));
        let idx_meta = IdxSegmentMeta {
            seq_id: new_reader.seq_id(),
            file_len: new_reader.file_len(),
            level: new_reader.level(),
            record_count: new_reader.record_count(),
        };

        let new_sync_epoch = self.sync_wal.active_epoch() + 1;
        let new_buf_epoch = self.buf_wal.active_epoch() + 1;
        let new_checkpoint_lsn = self.next_lsn.saturating_sub(1);

        let settled = self.prepare_settled_blob_checkpoint();
        // During a `RingFull` flush inside an uncommitted transaction, only pre-transaction quotas
        // reflect the flushed `MemTable`.
        let quota_metas = self.settled_quota_metas();
        // The commit rotates the WAL epochs, so no batch can reference these segments any more.
        let lingering = self.unlink_after_wal_rotation.clone();

        if let Err(e) = self.meta_mgr.commit_update(|st| {
            st.idx_segments.push(idx_meta);
            st.std_blob_segments = settled.std_metas;
            st.lru_blob_segments = settled.lru_metas;
            st.table_quotas = quota_metas;
            st.checkpoint_lsn = new_checkpoint_lsn;
            st.sync_wal_epoch = new_sync_epoch;
            st.buf_wal_epoch = new_buf_epoch;
            for fname in settled.dead_blob_files.iter().chain(&lingering) {
                st.queue_pending_delete(fname.clone());
            }
        }) {
            drop(new_reader);
            if !self.meta_mgr.is_poisoned() {
                let _ = self.fs.remove_file(&format_idx_file_name(new_seq_id));
            }
            return Err(e);
        }

        self.apply_settled_blob_reclaim(
            settled.dead_std_seqs,
            settled.dead_lru_keys,
            !settled.dead_blob_files.is_empty() || !lingering.is_empty(),
        );

        self.invalidate_snapshot_idx_segments();
        self.memtable.clear();
        self.idx_segments.push(Arc::new(new_reader));
        self.sync_wal.reset_for_new_epoch(new_sync_epoch)?;
        self.buf_wal.reset_for_new_epoch(new_buf_epoch)?;
        self.last_buf_synced_lsn = self.last_buf_append_lsn;
        // `drain_pending_durable` ran first, so no group-committed batch is lost to the rotation.
        self.wal_links.rotated();
        Ok(())
    }

    /// Queues the segments in `unlink_after_wal_rotation` for deletion when no logged batch can
    /// still reference them: the `MemTable` is empty, so every logged batch is at or below
    /// `checkpoint_lsn` and recovery skips it before verifying its blobs. A non-empty `MemTable`
    /// leaves the list for the flush that rotates the WAL.
    pub(super) fn release_unlinkable_blob_files(&mut self) -> io::Result<()> {
        if self.unlink_after_wal_rotation.is_empty()
            || !self.memtable.is_empty()
            || !self.pending_group.is_empty()
            || self.tx_depth > 0
            || self.pending_tx_record_count > 0
        {
            return Ok(());
        }
        let lingering = self.unlink_after_wal_rotation.clone();
        self.meta_mgr.commit_update(|st| {
            for fname in &lingering {
                st.queue_pending_delete(fname.clone());
            }
        })?;
        self.unlink_after_wal_rotation.clear();
        self.meta_mgr.try_process_pending_deletes();
        Ok(())
    }

    pub(super) fn rotate_wal_epochs_only(&mut self) -> io::Result<()> {
        self.check_not_poisoned()?;
        self.buf_wal.flush_buffered_to_os()?;
        self.sync_active_blob_writers()?;
        let new_sync_epoch = self.sync_wal.active_epoch() + 1;
        let new_buf_epoch = self.buf_wal.active_epoch() + 1;
        let new_checkpoint_lsn = self.next_lsn.saturating_sub(1);
        let settled = self.prepare_settled_blob_checkpoint();
        let lingering = self.unlink_after_wal_rotation.clone();
        self.meta_mgr.commit_update(|st| {
            st.std_blob_segments = settled.std_metas;
            st.lru_blob_segments = settled.lru_metas;
            st.checkpoint_lsn = new_checkpoint_lsn;
            st.sync_wal_epoch = new_sync_epoch;
            st.buf_wal_epoch = new_buf_epoch;
            for fname in settled.dead_blob_files.iter().chain(&lingering) {
                st.queue_pending_delete(fname.clone());
            }
        })?;
        self.apply_settled_blob_reclaim(
            settled.dead_std_seqs,
            settled.dead_lru_keys,
            !settled.dead_blob_files.is_empty() || !lingering.is_empty(),
        );
        self.sync_wal.reset_for_new_epoch(new_sync_epoch)?;
        self.buf_wal.reset_for_new_epoch(new_buf_epoch)?;
        self.last_buf_synced_lsn = self.last_buf_append_lsn;
        // Callers drain the group commit first (or have nothing pending, as `open` does).
        self.wal_links.rotated();
        Ok(())
    }

    fn fragmented_std_seqs<'a>(
        &'a self,
        dead_ratio_threshold: f64,
    ) -> impl Iterator<Item = u64> + 'a {
        let active_std_seq = self.active_std_writer.as_ref().map(|w| w.seq_id());
        self.std_blobs.iter().filter_map(move |(&seq, b)| {
            (Some(seq) != active_std_seq
                && b.file_len > 0
                && b.dead_bytes < b.file_len
                && (b.dead_bytes as f64) / (b.file_len as f64) >= dead_ratio_threshold)
                .then_some(seq)
        })
    }

    /// Returns `true` when at least one sealed `blob_std` segment has `dead_bytes / file_len >=
    /// dead_ratio_threshold` and can be relocated to reclaim dead space.
    pub fn has_fragmented_std_blobs(&self, dead_ratio_threshold: f64) -> bool {
        !self.is_poisoned()
            && self
                .fragmented_std_seqs(dead_ratio_threshold)
                .next()
                .is_some()
    }

    /// Relocates live blobs out of fragmented sealed `blob_std_<SeqID>.db` segments whose
    /// `dead_bytes / file_len >= dead_ratio_threshold`, appending them to the active `blob_std`
    /// segment and marking the old `BlobPtr`s dead so the fragmented segments reach 100% dead
    /// and can be reclaimed in $O(1)$.
    pub fn relocate_fragmented_std_blobs(
        &mut self,
        dead_ratio_threshold: f64,
    ) -> io::Result<usize> {
        self.check_not_poisoned()?;
        if self.tx_depth > 0 || self.relocating_std_blobs {
            return Ok(0);
        }
        self.relocating_std_blobs = true;
        let res = self.relocate_fragmented_std_blobs_inner(dead_ratio_threshold);
        self.relocating_std_blobs = false;
        res
    }

    fn relocate_fragmented_std_blobs_inner(
        &mut self,
        dead_ratio_threshold: f64,
    ) -> io::Result<usize> {
        self.drain_pending_durable()?;
        let fragmented_seqs: HashSet<u64> =
            self.fragmented_std_seqs(dead_ratio_threshold).collect();

        if fragmented_seqs.is_empty() {
            return Ok(0);
        }

        const RELOCATE_BATCH_SIZE: usize = 512;
        let mut seek_key: Vec<u8> = Vec::new();
        let mut lower_inclusive = true;
        let mut relocated = 0usize;

        loop {
            let entries = self.scan_merged_prefix_range(
                &[],
                &seek_key,
                lower_inclusive,
                None,
                RELOCATE_BATCH_SIZE,
                false,
            )?;
            if entries.is_empty() {
                break;
            }
            seek_key = entries.last().unwrap().key.clone();
            lower_inclusive = false;

            let mut batch_wal_records = Vec::new();
            let mut batch_dead_ptrs = Vec::new();
            for entry in entries {
                if let IndexValue::Blob(old_ptr) = entry.value {
                    if old_ptr.kind == BlobKind::Standard
                        && fragmented_seqs.contains(&old_ptr.seg_id)
                    {
                        if let Some(payload) =
                            self.dereference_blob_lenient(&old_ptr, &entry.key)?
                        {
                            let new_ptr = self.append_to_blob_segment(
                                BlobKind::Standard,
                                0,
                                &entry.key,
                                &payload,
                            )?;
                            batch_dead_ptrs.push(old_ptr);
                            batch_wal_records.push(WalRecord::PutBlobPtr {
                                key: entry.key,
                                blob_ptr: new_ptr,
                            });
                            relocated += 1;
                        }
                    }
                }
            }
            if !batch_wal_records.is_empty() {
                self.commit_wal_and_memtable_core(
                    batch_wal_records,
                    WriteDurability::BufferedFast,
                    false,
                    false,
                )?;
                for old_ptr in batch_dead_ptrs {
                    self.record_dead_blob_bytes(&old_ptr);
                }
                if self.memtable.should_flush() {
                    if let Err(e) = self.flush_memtable_no_auto_compact() {
                        self.record_maintenance_failure(&e);
                    }
                }
            }
        }
        Ok(relocated)
    }

    /// Background-worker pass that relocates fragmented sealed `blob_std` segments and flushes or
    /// reclaims them so dead disk space is freed even under background compaction mode.
    pub(crate) fn background_relocate_fragmented_std_blobs(
        &mut self,
        dead_ratio_threshold: f64,
    ) -> io::Result<usize> {
        self.check_not_poisoned()?;
        if self.tx_depth > 0 || !self.has_fragmented_std_blobs(dead_ratio_threshold) {
            return Ok(0);
        }
        let relocated = self.relocate_fragmented_std_blobs(dead_ratio_threshold)?;
        if relocated > 0 {
            if !self.memtable.is_empty() {
                self.flush_memtable_no_auto_compact()?;
            } else {
                self.reclaim_fully_dead_blob_segments()?;
            }
        }
        Ok(relocated)
    }

    /// Runs multi-way merge compaction across all active `idx_<SeqID>.db` segments, reclaims
    /// dead blob bytes from overwritten keys, relocates fragmented standard blob segments,
    /// commits the compacted segment to `boot_meta.db`, and deletes superseded `idx` and
    /// 100%-dead blob segments via Deferred GC.
    /// If called while an explicit transaction is active (`tx_depth > 0`), defers compaction
    /// until `commit_transaction`.
    pub fn compact(&mut self) -> io::Result<()> {
        self.check_not_poisoned()?;
        if self.tx_depth > 0 {
            self.deferred_compact_on_commit = true;
            return Ok(());
        }
        self.drain_pending_durable()?;
        if !self.memtable.is_empty() {
            self.flush_memtable_no_auto_compact()?;
        }
        let relocated = self.relocate_fragmented_std_blobs(0.5)?;
        if relocated > 0 && !self.memtable.is_empty() {
            self.flush_memtable_no_auto_compact()?;
        }
        if self.idx_segments.len() <= 1 {
            self.reclaim_fully_dead_blob_segments()?;
            return Ok(());
        }

        self.run_compaction_sync(0)
    }

    /// Runs size-tiered suffix automatic compaction.
    ///
    /// Selects a contiguous suffix `self.idx_segments[start_idx..]` of newer/smaller segments
    /// such that no older segment is merged when its size exceeds `2x` the combined size of all
    /// newer segments after it:
    /// - When `start_idx == 0` (all segments are comparable in size), delegates to full
    ///   [`Self::compact()`] (`drop_tombstones = true` + blob relocation).
    /// - When `start_idx > 0` (partial suffix compaction), merges only `idx_segments[start_idx..]`
    ///   with `drop_tombstones = false` (preserving tombstones that may shadow keys in older
    ///   segments `0..start_idx`) and skips whole-database blob relocation.
    pub fn compact_auto_tiered(&mut self) -> io::Result<()> {
        self.check_not_poisoned()?;
        if self.tx_depth > 0 {
            self.deferred_compact_on_commit = true;
            return Ok(());
        }
        self.drain_pending_durable()?;
        if !self.memtable.is_empty() {
            self.flush_memtable_no_auto_compact()?;
        }
        if self.idx_segments.len() <= 1 {
            self.reclaim_fully_dead_blob_segments()?;
            return Ok(());
        }

        let Some(start_idx) = self.select_tiered_start() else {
            return Ok(());
        };
        if start_idx == 0 {
            return self.compact();
        }
        self.run_compaction_sync(start_idx)
    }

    /// Size-tiered selection: returns the start of the contiguous suffix to merge, or `None`
    /// when fewer than two segments exist. `Some(0)` means a full compaction.
    fn select_tiered_start(&self) -> Option<usize> {
        let n = self.idx_segments.len();
        if n <= 1 {
            return None;
        }
        let mut start_idx = (n / 2).min(n - 2);
        for i in 0..(n - 1) {
            let curr_bytes = self.idx_segments[i].file_len();
            let newer_bytes: u64 = self.idx_segments[i + 1..]
                .iter()
                .map(|s| s.file_len())
                .sum();
            if curr_bytes <= newer_bytes.saturating_mul(2) {
                start_idx = i;
                break;
            }
        }
        Some(start_idx)
    }

    /// `true` when the segment count has reached `auto_compact_segment_threshold` (and the
    /// engine is healthy), i.e. a worker should run `prepare_compaction`.
    pub fn compaction_pending(&self) -> bool {
        !self.is_poisoned()
            && self.config.auto_compact_segment_threshold > 1
            && self.idx_segments.len() >= self.config.auto_compact_segment_threshold
    }

    /// Phase 1 of background compaction (cheap, under `&mut self`): picks the same size-tiered
    /// suffix as [`Self::compact_auto_tiered`] (a full compaction when it starts at index 0),
    /// reserves the output `SeqID` in memory (unique across concurrent jobs, and recovered cleanly
    /// on crash by `reserve_seq_ids_above_existing_files`), and snapshots the input readers as
    /// shared handles. Returns `None` if there is nothing to merge. Does not touch `boot_meta.db`,
    /// flush the MemTable, or touch blobs/WAL.
    ///
    /// Crash safety: until [`Self::install_compaction`] commits, the output file is an
    /// unreferenced orphan that `open()` sweeps.
    pub fn prepare_compaction(&mut self) -> io::Result<Option<CompactionJob<FS>>> {
        self.check_not_poisoned()?;
        match self.select_tiered_start() {
            Some(start) => self.prepare_compaction_at(start),
            None => Ok(None),
        }
    }

    pub(super) fn prepare_compaction_at(
        &mut self,
        start_idx: usize,
    ) -> io::Result<Option<CompactionJob<FS>>> {
        self.check_not_poisoned()?;
        if start_idx + 2 > self.idx_segments.len() {
            return Ok(None);
        }
        let out_seq_id = self.meta_mgr.reserve_seq_id();
        let full = start_idx == 0;
        let level = if full {
            1
        } else {
            self.idx_segments[start_idx..]
                .iter()
                .map(|s| s.level())
                .max()
                .unwrap_or(1)
                .max(1)
        };
        Ok(Some(CompactionJob {
            fs: Arc::clone(&self.fs),
            inputs: self.idx_segments[start_idx..]
                .iter()
                .rev()
                .cloned()
                .collect(),
            out_seq_id,
            level,
            bloom_bits: self.config.bloom_bits_per_key,
            // Tombstones may only be dropped when nothing older than the inputs exists.
            drop_tombstones: full,
        }))
    }

    fn run_compaction_sync(&mut self, start_idx: usize) -> io::Result<()> {
        if let Some(job) = self.prepare_compaction_at(start_idx)? {
            let out = job.run()?;
            self.install_compaction(out)?;
        }
        Ok(())
    }

    /// Phase 3 of background compaction (cheap, under `&mut self`). Verifies that the job's input
    /// segments are still exactly one contiguous run of `idx_segments` (newer segments appended
    /// by flushes since `prepare_compaction` are fine), commits the new segment list (plus dead
    /// blob segments, quotas and pending deletes) to `boot_meta.db`, and swaps the readers in
    /// place of the inputs, preserving oldest-to-newest order.
    ///
    /// Returns `Ok(false)` after deleting the output file if the inputs vanished (another
    /// compaction won), or if the job dropped tombstones but its inputs no longer start at index
    /// 0. While a transaction is active or the memtable is not empty, only the segment list and
    /// the pending deletes of the replaced files are committed: blob/quota metadata would include
    /// effects of WAL records beyond the checkpoint, and fully-dead blob segments are reclaimed at
    /// the next flush instead.
    ///
    /// The superseded `idx_<SeqID>.db` files are queued for deferred deletion. Their readers are
    /// dropped here, but a file still held open elsewhere (e.g. by another in-flight job's `Arc`)
    /// cannot be unlinked on Windows (share mode 0); it stays in `pending_delete_files` and is
    /// retried on later commits / at startup.
    pub fn install_compaction(&mut self, out: CompactionOutput<FS>) -> io::Result<bool> {
        if let Err(e) = self.check_not_poisoned() {
            self.discard_compaction_output(out);
            return Err(e);
        }
        let k = out.inputs.len();
        // `inputs` is newest-first; the run's oldest member is the last element.
        let first_seq = out.inputs[k - 1].seq_id();
        let start = self
            .idx_segments
            .iter()
            .position(|s| s.seq_id() == first_seq);
        let valid = match start {
            Some(st) if st + k <= self.idx_segments.len() => {
                (0..k).all(|i| Arc::ptr_eq(&self.idx_segments[st + i], &out.inputs[k - 1 - i]))
                    && (!out.drop_tombstones || st == 0)
            }
            _ => false,
        };
        if !valid {
            self.discard_compaction_output(out);
            return Ok(false);
        }
        let start = start.unwrap();

        let meta_of = |r: &Arc<IdxSegmentReader<FS::File>>| IdxSegmentMeta {
            seq_id: r.seq_id(),
            file_len: r.file_len(),
            level: r.level(),
            record_count: r.record_count(),
        };
        let mut new_idx_metas: Vec<IdxSegmentMeta> = Vec::with_capacity(self.idx_segments.len());
        new_idx_metas.extend(self.idx_segments[..start].iter().map(meta_of));
        new_idx_metas.push(meta_of(&out.new_segment));
        new_idx_metas.extend(self.idx_segments[start + k..].iter().map(meta_of));
        let old_idx_names: Vec<String> = out
            .inputs
            .iter()
            .map(|r| format_idx_file_name(r.seq_id()))
            .collect();

        // Blob accounting (dead bytes, quotas, reclaim) may only be persisted when it reflects
        // checkpointed state: WAL replay re-applies the increments of any record newer than the
        // checkpoint, so persisting them here would count them twice after a crash. The memtable
        // holds exactly the post-checkpoint mutations.
        // Commits still waiting in `pending_group` are logged but maybe not durable, and their
        // overlay is not in the memtable, so they count as well.
        let persist_blob_state =
            self.tx_depth == 0 && self.memtable.is_empty() && self.pending_group.is_empty();
        if persist_blob_state {
            // The catalog records each segment's length. Bytes beyond the durable length (for
            // example from a rolled-back transaction) would otherwise be cataloged as present and
            // counted dead, and a power cut would shorten the file below its cataloged length.
            if let Err(e) = self.sync_active_blob_writers() {
                self.discard_compaction_output(out);
                return Err(e);
            }
        }
        let (dead_std_seqs, dead_lru_keys, dead_blob_files) = if persist_blob_state {
            self.collect_fully_dead_blob_segments()
        } else {
            (Vec::new(), Vec::new(), Vec::new())
        };
        let blob_quota = if !persist_blob_state {
            None
        } else {
            let std_metas: Vec<StdBlobSegmentMeta> = self
                .std_blobs
                .iter()
                .filter(|(seq, _)| !dead_std_seqs.contains(seq))
                .map(|(&seq_id, b)| StdBlobSegmentMeta {
                    seq_id,
                    file_len: b.file_len,
                    dead_bytes: b.dead_bytes,
                })
                .collect();
            let lru_metas: Vec<LruBlobSegmentMeta> = self
                .lru_blobs
                .iter()
                .filter(|(k, _)| !dead_lru_keys.contains(k))
                .map(|(&(_tid, seq_id), b)| LruBlobSegmentMeta {
                    seq_id,
                    file_len: b.file_len,
                    dead_bytes: b.dead_bytes,
                    table_id: b.table_id,
                })
                .collect();
            Some((std_metas, lru_metas, self.build_quota_metas()))
        };

        if let Err(e) = self.meta_mgr.commit_update(|st| {
            st.idx_segments = new_idx_metas;
            if let Some((std_metas, lru_metas, quota_metas)) = blob_quota {
                st.std_blob_segments = std_metas;
                st.lru_blob_segments = lru_metas;
                st.table_quotas = quota_metas;
            }
            for name in old_idx_names {
                st.queue_pending_delete(name);
            }
            for fname in &dead_blob_files {
                st.queue_pending_delete(fname.clone());
            }
        }) {
            if !self.meta_mgr.is_poisoned() {
                self.discard_compaction_output(out);
            }
            return Err(e);
        }

        if !dead_std_seqs.is_empty() {
            self.invalidate_snapshot_std_blobs();
        }
        if !dead_lru_keys.is_empty() {
            self.invalidate_snapshot_lru_blobs();
        }
        self.invalidate_snapshot_idx_segments();

        for seq in dead_std_seqs {
            self.blob_cache
                .invalidate_blob_segment(BlobKind::Standard as u8, 0, seq);
            self.std_blobs.remove(&seq);
        }
        for key in dead_lru_keys {
            self.blob_cache
                .invalidate_blob_segment(BlobKind::Lru as u8, key.0, key.1);
            self.lru_blobs.remove(&key);
        }

        let CompactionOutput {
            mut new_segment,
            inputs,
            ..
        } = out;
        if let Some(r) = Arc::get_mut(&mut new_segment) {
            r.set_block_cache(Some(Arc::clone(&self.block_cache)));
        }
        for inp in &inputs {
            self.block_cache.invalidate_segment(inp.seq_id());
        }
        let removed: Vec<_> = self
            .idx_segments
            .splice(start..start + k, std::iter::once(new_segment))
            .collect();
        // Release our handles before unlinking so the files can actually be deleted.
        drop(removed);
        drop(inputs);
        self.meta_mgr.try_process_pending_deletes();
        if self.config.background_compaction
            && !self.relocating_std_blobs
            && self.has_fragmented_std_blobs(0.5)
        {
            if let Some(hook) = &self.compaction_hook {
                hook();
            }
        }
        Ok(true)
    }

    /// Drops an uninstalled output and unlinks its (unreferenced) file.
    fn discard_compaction_output(&self, out: CompactionOutput<FS>) {
        let name = format_idx_file_name(out.new_segment.seq_id());
        drop(out);
        let _ = self.fs.remove_file(&name);
    }

    /// Dead bytes of a `blob_std` segment that may be persisted or acted on (reclaim).
    ///
    /// While a transaction is being built or appended (its start snapshot is live) its overlay is
    /// not published and its WAL batch may not be durable, yet its overwrites and deletes are
    /// already counted in the in-memory `dead_bytes`. Persisting or reclaiming on those would
    /// lose a row that is still durable if the transaction is lost, so only the bytes counted at
    /// the start of the transaction are reported. They are counted again in memory and become
    /// effective at the first flush after the transaction has been published.
    pub(super) fn settled_std_dead_bytes(&self, seq: u64, dead_bytes: u64) -> u64 {
        if self.tx_start_meta.active {
            let at_start = self.tx_start_meta.std_blobs.get(&seq).map_or(0, |s| s.1);
            at_start.min(dead_bytes)
        } else {
            dead_bytes
        }
    }

    /// `blob_lru` counterpart of [`Self::settled_std_dead_bytes`].
    pub(super) fn settled_lru_dead_bytes(&self, key: (u16, u64), dead_bytes: u64) -> u64 {
        if self.tx_start_meta.active {
            let at_start = self.tx_start_meta.lru_blobs.get(&key).map_or(0, |s| s.1);
            at_start.min(dead_bytes)
        } else {
            dead_bytes
        }
    }

    /// Transaction-start counterpart for `blob_std` segment lengths so uncommitted appends inside an
    /// active transaction are not cataloged ahead of commit/checkpoint.
    pub(super) fn settled_std_file_len(&self, seq: u64, file_len: u64) -> u64 {
        if self.tx_start_meta.active {
            let at_start = self.tx_start_meta.std_blobs.get(&seq).map_or(0, |s| s.0);
            at_start.min(file_len)
        } else {
            file_len
        }
    }

    /// `blob_lru` counterpart of [`Self::settled_std_file_len`].
    pub(super) fn settled_lru_file_len(&self, key: (u16, u64), file_len: u64) -> u64 {
        if self.tx_start_meta.active {
            let at_start = self.tx_start_meta.lru_blobs.get(&key).map_or(0, |s| s.0);
            at_start.min(file_len)
        } else {
            file_len
        }
    }

    fn prepare_settled_blob_checkpoint(&self) -> SettledBlobCheckpoint {
        let (dead_std_seqs, dead_lru_keys, dead_blob_files) =
            self.collect_fully_dead_blob_segments();
        let std_metas: Vec<StdBlobSegmentMeta> = self
            .std_blobs
            .iter()
            .filter(|(seq, _)| !dead_std_seqs.contains(seq))
            .map(|(&seq_id, b)| StdBlobSegmentMeta {
                seq_id,
                file_len: self.settled_std_file_len(seq_id, b.file_len),
                dead_bytes: self.settled_std_dead_bytes(seq_id, b.dead_bytes),
            })
            .collect();
        let lru_metas: Vec<LruBlobSegmentMeta> = self
            .lru_blobs
            .iter()
            .filter(|(k, _)| !dead_lru_keys.contains(k))
            .map(|(&(tid, seq_id), b)| LruBlobSegmentMeta {
                seq_id,
                file_len: self.settled_lru_file_len((tid, seq_id), b.file_len),
                dead_bytes: self.settled_lru_dead_bytes((tid, seq_id), b.dead_bytes),
                table_id: b.table_id,
            })
            .collect();
        SettledBlobCheckpoint {
            dead_std_seqs,
            dead_lru_keys,
            dead_blob_files,
            std_metas,
            lru_metas,
        }
    }

    fn apply_settled_blob_reclaim(
        &mut self,
        dead_std_seqs: Vec<u64>,
        dead_lru_keys: Vec<(u16, u64)>,
        delete_attempted: bool,
    ) {
        if !dead_std_seqs.is_empty() {
            self.invalidate_snapshot_std_blobs();
        }
        if !dead_lru_keys.is_empty() {
            self.invalidate_snapshot_lru_blobs();
        }

        for seq in dead_std_seqs {
            self.blob_cache
                .invalidate_blob_segment(BlobKind::Standard as u8, 0, seq);
            self.std_blobs.remove(&seq);
        }
        for key in dead_lru_keys {
            self.blob_cache
                .invalidate_blob_segment(BlobKind::Lru as u8, key.0, key.1);
            self.lru_blobs.remove(&key);
        }
        self.unlink_after_wal_rotation.clear();
        if delete_attempted {
            self.meta_mgr.try_process_pending_deletes();
        }
    }

    pub(super) fn collect_fully_dead_blob_segments(
        &self,
    ) -> (Vec<u64>, Vec<(u16, u64)>, Vec<String>) {
        let active_std_seq = self.active_std_writer.as_ref().map(|w| w.seq_id());
        let dead_std: Vec<u64> = self
            .std_blobs
            .iter()
            .filter(|&(&seq, b)| {
                Some(seq) != active_std_seq
                    && b.file_len > 0
                    && self.settled_std_dead_bytes(seq, b.dead_bytes) >= b.file_len
            })
            .map(|(&seq, _)| seq)
            .collect();

        let mut deleted_files = Vec::new();
        for &seq in &dead_std {
            deleted_files.push(format_std_blob_file_name(seq));
        }

        let dead_lru: Vec<(u16, u64)> = self
            .lru_blobs
            .iter()
            .filter(|&(&(tid, seq), b)| {
                let active_seq = self.active_lru_writers.get(&tid).map(|w| w.seq_id());
                Some(seq) != active_seq
                    && b.file_len > 0
                    && self.settled_lru_dead_bytes((tid, seq), b.dead_bytes) >= b.file_len
            })
            .map(|(&k, _)| k)
            .collect();

        for &(tid, seq) in &dead_lru {
            deleted_files.push(format_lru_blob_file_name(tid, seq));
        }

        (dead_std, dead_lru, deleted_files)
    }

    /// Reclaims any sealed `blob_lru` segment belonging to `table_id` where `dead_bytes >= file_len`,
    /// syncing `buf_wal` first so the eviction WAL records are durable before the segments leave
    /// the catalog. The files themselves are unlinked only after the WAL epochs that may still
    /// mention them have been rotated away (see `unlink_after_wal_rotation`).
    pub(super) fn reclaim_fully_dead_lru_blob_segments(&mut self, table_id: u16) -> io::Result<()> {
        let active_seq = self.active_lru_writers.get(&table_id).map(|w| w.seq_id());
        let dead_lru: Vec<(u16, u64)> = self
            .lru_blobs
            .iter()
            .filter(|&(&(tid, seq), b)| {
                tid == table_id
                    && Some(seq) != active_seq
                    && b.file_len > 0
                    && self.settled_lru_dead_bytes((tid, seq), b.dead_bytes) >= b.file_len
            })
            .map(|(&k, _)| k)
            .collect();

        if dead_lru.is_empty() {
            return Ok(());
        }

        // Ensure any unsynced buffered eviction `Delete` WAL records that killed these LRU blob
        // segments are durable on disk before removing the segments from `boot_meta.db` and
        // unlinking them. Skip when all buffered WAL writes are already synced.
        if self.last_buf_append_lsn > self.last_buf_synced_lsn {
            self.make_buf_wal_durable(true)?;
        }

        let deleted_files: Vec<String> = dead_lru
            .iter()
            .map(|&(tid, seq)| format_lru_blob_file_name(tid, seq))
            .collect();

        let mut lru_metas = self.build_checkpointed_lru_blob_metas();
        lru_metas.retain(|m| !dead_lru.contains(&(m.table_id, m.seq_id)));
        self.meta_mgr.commit_update(|st| {
            st.lru_blob_segments = lru_metas;
        })?;

        self.invalidate_snapshot_lru_blobs();
        for &(tid, seq) in &dead_lru {
            self.blob_cache
                .invalidate_blob_segment(BlobKind::Lru as u8, tid, seq);
            self.lru_blobs.remove(&(tid, seq));
        }
        // The files are not queued for deletion yet: the evicted rows' `Put` batches may still be
        // in the WAL, and recovery must be able to verify their blobs. If the unlink happened
        // here, it would drop each such batch (and in `wal_buf.db` every later one) although a
        // later synchronous commit made it durable. The next WAL rotation queues them.
        self.unlink_after_wal_rotation.extend(deleted_files);
        self.release_unlinkable_blob_files()
    }

    /// Reclaims any sealed `blob_std` or `blob_lru` segment where `dead_bytes >= file_len`
    /// ($O(1)$ Copy-less Segment Deletion!).
    pub(super) fn reclaim_fully_dead_blob_segments(&mut self) -> io::Result<()> {
        self.drain_group_for_autocommit()?;
        let (dead_std, dead_lru, deleted_files) = self.collect_fully_dead_blob_segments();
        if deleted_files.is_empty() {
            return Ok(());
        }
        // The catalog records segment lengths: make them durable first (see `install_compaction`).
        self.sync_active_blob_writers()?;

        let mut std_metas = self.build_checkpointed_std_blob_metas();
        std_metas.retain(|m| !dead_std.contains(&m.seq_id));
        let mut lru_metas = self.build_checkpointed_lru_blob_metas();
        lru_metas.retain(|m| !dead_lru.contains(&(m.table_id, m.seq_id)));
        self.meta_mgr.commit_update(|st| {
            st.std_blob_segments = std_metas;
            st.lru_blob_segments = lru_metas;
            for fname in &deleted_files {
                st.queue_pending_delete(fname.clone());
            }
        })?;

        if !dead_std.is_empty() {
            self.invalidate_snapshot_std_blobs();
        }
        if !dead_lru.is_empty() {
            self.invalidate_snapshot_lru_blobs();
        }
        for seq in dead_std {
            self.blob_cache
                .invalidate_blob_segment(BlobKind::Standard as u8, 0, seq);
            self.std_blobs.remove(&seq);
        }
        for key in dead_lru {
            self.blob_cache
                .invalidate_blob_segment(BlobKind::Lru as u8, key.0, key.1);
            self.lru_blobs.remove(&key);
        }
        self.meta_mgr.try_process_pending_deletes();
        Ok(())
    }

    pub(super) fn build_std_blob_metas(&self) -> Vec<StdBlobSegmentMeta> {
        self.std_blobs
            .iter()
            .map(|(&seq_id, b)| StdBlobSegmentMeta {
                seq_id,
                file_len: b.file_len,
                dead_bytes: b.dead_bytes,
            })
            .collect()
    }

    pub(super) fn build_lru_blob_metas(&self) -> Vec<LruBlobSegmentMeta> {
        self.lru_blobs
            .iter()
            .map(|(&(_tid, seq_id), b)| LruBlobSegmentMeta {
                seq_id,
                file_len: b.file_len,
                dead_bytes: b.dead_bytes,
                table_id: b.table_id,
            })
            .collect()
    }

    pub(super) fn build_checkpointed_std_blob_metas(&self) -> Vec<StdBlobSegmentMeta> {
        let prev = &self.meta_mgr.state().std_blob_segments;
        let mut metas = self.build_std_blob_metas();
        for m in &mut metas {
            let prev_meta = prev.iter().find(|p| p.seq_id == m.seq_id);
            m.dead_bytes = prev_meta.map(|p| p.dead_bytes).unwrap_or(0);
            m.file_len = prev_meta.map(|p| p.file_len).unwrap_or(0);
        }
        metas
    }

    pub(super) fn build_checkpointed_lru_blob_metas(&self) -> Vec<LruBlobSegmentMeta> {
        let prev = &self.meta_mgr.state().lru_blob_segments;
        let mut metas = self.build_lru_blob_metas();
        for m in &mut metas {
            let prev_meta = prev
                .iter()
                .find(|p| p.table_id == m.table_id && p.seq_id == m.seq_id);
            m.dead_bytes = prev_meta.map(|p| p.dead_bytes).unwrap_or(0);
            m.file_len = prev_meta.map(|p| p.file_len).unwrap_or(0);
        }
        metas
    }
}
