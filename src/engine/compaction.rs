//! MemTable flushing, size-tiered SSTable auto-compaction, fragmented blob relocation,
//! and copy-less dead-blob segment reclamation.

use std::collections::HashSet;
use std::io;

use crate::index::{IdxSegmentReader, IndexValue, compact_idx_segments};
use crate::meta::{
    IdxSegmentMeta, LruBlobSegmentMeta, StdBlobSegmentMeta, format_idx_file_name,
    format_lru_blob_file_name, format_std_blob_file_name,
};
use crate::pal::{PlatformFile, PlatformFs};
use crate::wal::{BlobKind, WalRecord};

use super::{StorageEngine, WriteDurability};

impl<FS: PlatformFs> StorageEngine<FS> {
    /// Flushes the current `MemTable` to a new immutable `idx_<SeqID>.db` segment, rotates both
    /// WAL streams to `epoch + 1` in $O(1)$ without zero-filling, atomically commits `boot_meta.db`,
    /// and triggers automatic compaction if `auto_compact_segment_threshold` is reached.
    /// If called while an explicit transaction is active (`tx_depth > 0`), defers checkpointing
    /// until `commit_transaction` so provisional state is never published to SSTables.
    pub fn flush_memtable(&mut self) -> io::Result<()> {
        if self.tx_depth > 0 {
            self.deferred_flush_on_commit = true;
            return Ok(());
        }
        self.flush_memtable_no_auto_compact()?;
        if self.tx_depth == 0
            && self.config.auto_compact_segment_threshold > 1
            && self.idx_segments.len() >= self.config.auto_compact_segment_threshold
        {
            self.compact_auto_tiered()?;
        }
        Ok(())
    }

    pub(super) fn flush_memtable_no_auto_compact(&mut self) -> io::Result<()> {
        if self.tx_depth > 0 {
            self.deferred_flush_on_commit = true;
            return Ok(());
        }
        self.flush_pending_tx_records()?;
        self.buf_wal.flush_buffered_to_os()?;
        if self.memtable.is_empty() {
            return Ok(());
        }

        // Ensure all active blob segments are durably flushed before checkpointing `boot_meta.db`
        self.sync_active_blob_writers()?;

        let new_seq_id = self.meta_mgr.state().next_seq_id;

        let new_reader = match self.memtable.write_to_segment(
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
        let idx_meta = IdxSegmentMeta {
            seq_id: new_reader.seq_id(),
            file_len: new_reader.file_len(),
            level: new_reader.level(),
            record_count: new_reader.record_count(),
        };

        let new_sync_epoch = self.sync_wal.active_epoch() + 1;
        let new_buf_epoch = self.buf_wal.active_epoch() + 1;
        let new_checkpoint_lsn = self.next_lsn.saturating_sub(1);

        let (dead_std_seqs, dead_lru_keys, dead_blob_files) =
            self.collect_fully_dead_blob_segments();
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
        let quota_metas = self.build_quota_metas();

        if let Err(e) = self.meta_mgr.commit_update(|st| {
            let allocated = st.allocate_seq_id();
            debug_assert_eq!(allocated, new_seq_id);
            st.idx_segments.push(idx_meta);
            st.std_blob_segments = std_metas;
            st.lru_blob_segments = lru_metas;
            st.table_quotas = quota_metas;
            st.checkpoint_lsn = new_checkpoint_lsn;
            st.sync_wal_epoch = new_sync_epoch;
            st.buf_wal_epoch = new_buf_epoch;
            for fname in &dead_blob_files {
                st.queue_pending_delete(fname.clone());
            }
        }) {
            drop(new_reader);
            let _ = self.fs.remove_file(&format_idx_file_name(new_seq_id));
            return Err(e);
        }

        for seq in dead_std_seqs {
            self.std_blobs.remove(&seq);
        }
        for key in dead_lru_keys {
            self.lru_blobs.remove(&key);
        }
        if !dead_blob_files.is_empty() {
            self.meta_mgr.try_process_pending_deletes();
        }

        self.memtable.clear();
        self.idx_segments.push(new_reader);
        self.sync_wal.reset_for_new_epoch(new_sync_epoch);
        self.buf_wal.reset_for_new_epoch(new_buf_epoch);
        Ok(())
    }

    pub(super) fn rotate_wal_epochs_only(&mut self) -> io::Result<()> {
        self.buf_wal.flush_buffered_to_os()?;
        let new_sync_epoch = self.sync_wal.active_epoch() + 1;
        let new_buf_epoch = self.buf_wal.active_epoch() + 1;
        let new_checkpoint_lsn = self.next_lsn.saturating_sub(1);
        self.meta_mgr.commit_update(|st| {
            st.checkpoint_lsn = new_checkpoint_lsn;
            st.sync_wal_epoch = new_sync_epoch;
            st.buf_wal_epoch = new_buf_epoch;
        })?;
        self.sync_wal.reset_for_new_epoch(new_sync_epoch);
        self.buf_wal.reset_for_new_epoch(new_buf_epoch);
        Ok(())
    }

    /// Relocates live blobs out of fragmented sealed `blob_std_<SeqID>.db` segments whose
    /// `dead_bytes / file_len >= dead_ratio_threshold`, appending them to the active `blob_std`
    /// segment and marking the old `BlobPtr`s dead so the fragmented segments reach 100% dead
    /// and can be reclaimed in $O(1)$.
    pub fn relocate_fragmented_std_blobs(&mut self, dead_ratio_threshold: f64) -> io::Result<usize> {
        if self.tx_depth > 0 {
            return Ok(0);
        }
        let active_std_seq = self.active_std_writer.as_ref().map(|w| w.seq_id());
        let fragmented_seqs: HashSet<u64> = self
            .std_blobs
            .iter()
            .filter(|&(&seq, b)| {
                Some(seq) != active_std_seq
                    && b.file_len > 0
                    && b.dead_bytes < b.file_len
                    && (b.dead_bytes as f64) / (b.file_len as f64) >= dead_ratio_threshold
            })
            .map(|(&seq, _)| seq)
            .collect();

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
                        if let Some(payload) = self.dereference_blob(&old_ptr, &entry.key)? {
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
                self.commit_wal_and_memtable(batch_wal_records, WriteDurability::BufferedFast)?;
                for old_ptr in batch_dead_ptrs {
                    self.record_dead_blob_bytes(&old_ptr);
                }
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
        if self.tx_depth > 0 {
            self.deferred_compact_on_commit = true;
            return Ok(());
        }
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

        let comp_seq_id = self.meta_mgr.state().next_seq_id;

        // Pass segments ordered newest-first (`rev()`) to `compact_idx_segments`
        let refs_newest_first: Vec<&IdxSegmentReader<FS::File>> =
            self.idx_segments.iter().rev().collect();
        let outcome = match compact_idx_segments(
            &*self.fs,
            &refs_newest_first,
            comp_seq_id,
            1,
            self.config.bloom_bits_per_key,
            true,
        ) {
            Ok(o) => o,
            Err(e) => {
                let _ = self.fs.remove_file(&format_idx_file_name(comp_seq_id));
                return Err(e);
            }
        };

        let old_idx_names: Vec<String> = self
            .idx_segments
            .iter()
            .map(|r| format_idx_file_name(r.seq_id()))
            .collect();

        let new_meta = IdxSegmentMeta {
            seq_id: outcome.new_segment.seq_id(),
            file_len: outcome.new_segment.file_len(),
            level: outcome.new_segment.level(),
            record_count: outcome.new_segment.record_count(),
        };

        let (dead_std_seqs, dead_lru_keys, dead_blob_files) =
            self.collect_fully_dead_blob_segments();
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
        let quota_metas = self.build_quota_metas();

        if let Err(e) = self.meta_mgr.commit_update(|st| {
            let allocated = st.allocate_seq_id();
            debug_assert_eq!(allocated, comp_seq_id);
            st.idx_segments = vec![new_meta];
            st.std_blob_segments = std_metas;
            st.lru_blob_segments = lru_metas;
            st.table_quotas = quota_metas;
            for name in old_idx_names {
                st.queue_pending_delete(name);
            }
            for fname in &dead_blob_files {
                st.queue_pending_delete(fname.clone());
            }
        }) {
            drop(outcome);
            let _ = self.fs.remove_file(&format_idx_file_name(comp_seq_id));
            return Err(e);
        }

        for seq in dead_std_seqs {
            self.std_blobs.remove(&seq);
        }
        for key in dead_lru_keys {
            self.lru_blobs.remove(&key);
        }

        // Drop old segment readers and run pending deletes so superseded `idx_<SeqID>.db`
        // and 100%-dead blob files are unlinked immediately now that `boot_meta.db` is committed!
        self.idx_segments.clear();
        self.idx_segments.push(outcome.new_segment);
        self.meta_mgr.try_process_pending_deletes();
        Ok(())
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
        if self.tx_depth > 0 {
            self.deferred_compact_on_commit = true;
            return Ok(());
        }
        if !self.memtable.is_empty() {
            self.flush_memtable_no_auto_compact()?;
        }
        if self.idx_segments.len() <= 1 {
            self.reclaim_fully_dead_blob_segments()?;
            return Ok(());
        }

        let n = self.idx_segments.len();
        let mut start_idx = (n / 2).min(n - 2);
        for i in 0..(n - 1) {
            let curr_bytes = self.idx_segments[i].file_len();
            let newer_bytes: u64 = self.idx_segments[i + 1..].iter().map(|s| s.file_len()).sum();
            if curr_bytes <= newer_bytes.saturating_mul(2) {
                start_idx = i;
                break;
            }
        }

        if start_idx == 0 {
            return self.compact();
        }

        let comp_seq_id = self.meta_mgr.state().next_seq_id;
        let new_level = self.idx_segments[start_idx..]
            .iter()
            .map(|s| s.level())
            .max()
            .unwrap_or(1)
            .max(1);

        // Pass suffix segments ordered newest-first (`rev()`) with `drop_tombstones = false`
        let refs_newest_first: Vec<&IdxSegmentReader<FS::File>> =
            self.idx_segments[start_idx..].iter().rev().collect();
        let outcome = match compact_idx_segments(
            &*self.fs,
            &refs_newest_first,
            comp_seq_id,
            new_level,
            self.config.bloom_bits_per_key,
            false,
        ) {
            Ok(o) => o,
            Err(e) => {
                let _ = self.fs.remove_file(&format_idx_file_name(comp_seq_id));
                return Err(e);
            }
        };

        let old_idx_names: Vec<String> = self.idx_segments[start_idx..]
            .iter()
            .map(|r| format_idx_file_name(r.seq_id()))
            .collect();

        let new_meta = IdxSegmentMeta {
            seq_id: outcome.new_segment.seq_id(),
            file_len: outcome.new_segment.file_len(),
            level: outcome.new_segment.level(),
            record_count: outcome.new_segment.record_count(),
        };

        let mut new_idx_metas: Vec<IdxSegmentMeta> = Vec::with_capacity(start_idx + 1);
        for seg in &self.idx_segments[..start_idx] {
            new_idx_metas.push(IdxSegmentMeta {
                seq_id: seg.seq_id(),
                file_len: seg.file_len(),
                level: seg.level(),
                record_count: seg.record_count(),
            });
        }
        new_idx_metas.push(new_meta);

        let (dead_std_seqs, dead_lru_keys, dead_blob_files) =
            self.collect_fully_dead_blob_segments();
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
        let quota_metas = self.build_quota_metas();

        if let Err(e) = self.meta_mgr.commit_update(|st| {
            let allocated = st.allocate_seq_id();
            debug_assert_eq!(allocated, comp_seq_id);
            st.idx_segments = new_idx_metas;
            st.std_blob_segments = std_metas;
            st.lru_blob_segments = lru_metas;
            st.table_quotas = quota_metas;
            for name in old_idx_names {
                st.queue_pending_delete(name);
            }
            for fname in &dead_blob_files {
                st.queue_pending_delete(fname.clone());
            }
        }) {
            drop(outcome);
            let _ = self.fs.remove_file(&format_idx_file_name(comp_seq_id));
            return Err(e);
        }

        for seq in dead_std_seqs {
            self.std_blobs.remove(&seq);
        }
        for key in dead_lru_keys {
            self.lru_blobs.remove(&key);
        }

        self.idx_segments.truncate(start_idx);
        self.idx_segments.push(outcome.new_segment);
        self.meta_mgr.try_process_pending_deletes();
        Ok(())
    }

    pub(super) fn collect_fully_dead_blob_segments(
        &self,
    ) -> (Vec<u64>, Vec<(u16, u64)>, Vec<String>) {
        let active_std_seq = self.active_std_writer.as_ref().map(|w| w.seq_id());
        let dead_std: Vec<u64> = self
            .std_blobs
            .iter()
            .filter(|&(&seq, b)| {
                Some(seq) != active_std_seq && b.file_len > 0 && b.dead_bytes >= b.file_len
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
                Some(seq) != active_seq && b.file_len > 0 && b.dead_bytes >= b.file_len
            })
            .map(|(&k, _)| k)
            .collect();

        for &(tid, seq) in &dead_lru {
            deleted_files.push(format_lru_blob_file_name(tid, seq));
        }

        (dead_std, dead_lru, deleted_files)
    }

    /// Reclaims any sealed `blob_lru` segment belonging to `table_id` where `dead_bytes >= file_len`,
    /// syncing `buf_wal` first so the eviction WAL records are durable before unlinking.
    pub(super) fn reclaim_fully_dead_lru_blob_segments(&mut self, table_id: u16) -> io::Result<()> {
        let active_seq = self.active_lru_writers.get(&table_id).map(|w| w.seq_id());
        let dead_lru: Vec<(u16, u64)> = self
            .lru_blobs
            .iter()
            .filter(|&(&(tid, seq), b)| {
                tid == table_id
                    && Some(seq) != active_seq
                    && b.file_len > 0
                    && b.dead_bytes >= b.file_len
            })
            .map(|(&k, _)| k)
            .collect();

        if dead_lru.is_empty() {
            return Ok(());
        }

        // Ensure all buffered eviction `Delete` WAL records that killed these LRU blob segments
        // are durable on disk before removing the segments from `boot_meta.db` and unlinking them.
        self.buf_wal.flush_buffered_to_os()?;
        self.buf_wal.file().sync_durable()?;
        self.last_buf_synced_lsn = self.last_buf_append_lsn;

        let mut deleted_files = Vec::with_capacity(dead_lru.len());
        for &(tid, seq) in &dead_lru {
            deleted_files.push(format_lru_blob_file_name(tid, seq));
            self.lru_blobs.remove(&(tid, seq));
        }

        let lru_metas = self.build_checkpointed_lru_blob_metas();
        self.meta_mgr.commit_update(|st| {
            st.lru_blob_segments = lru_metas;
            for fname in &deleted_files {
                st.queue_pending_delete(fname.clone());
            }
        })?;
        self.meta_mgr.try_process_pending_deletes();
        Ok(())
    }

    /// Reclaims any sealed `blob_std` or `blob_lru` segment where `dead_bytes >= file_len`
    /// ($O(1)$ Copy-less Segment Deletion!).
    pub(super) fn reclaim_fully_dead_blob_segments(&mut self) -> io::Result<()> {
        let (dead_std, dead_lru, deleted_files) = self.collect_fully_dead_blob_segments();
        if deleted_files.is_empty() {
            return Ok(());
        }

        for seq in dead_std {
            self.std_blobs.remove(&seq);
        }
        for key in dead_lru {
            self.lru_blobs.remove(&key);
        }

        let std_metas = self.build_checkpointed_std_blob_metas();
        let lru_metas = self.build_checkpointed_lru_blob_metas();
        self.meta_mgr.commit_update(|st| {
            st.std_blob_segments = std_metas;
            st.lru_blob_segments = lru_metas;
            for fname in &deleted_files {
                st.queue_pending_delete(fname.clone());
            }
        })?;
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
            m.dead_bytes = prev
                .iter()
                .find(|p| p.seq_id == m.seq_id)
                .map(|p| p.dead_bytes)
                .unwrap_or(0);
        }
        metas
    }

    pub(super) fn build_checkpointed_lru_blob_metas(&self) -> Vec<LruBlobSegmentMeta> {
        let prev = &self.meta_mgr.state().lru_blob_segments;
        let mut metas = self.build_lru_blob_metas();
        for m in &mut metas {
            m.dead_bytes = prev
                .iter()
                .find(|p| p.table_id == m.table_id && p.seq_id == m.seq_id)
                .map(|p| p.dead_bytes)
                .unwrap_or(0);
        }
        metas
    }
}
