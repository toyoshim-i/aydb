//! Multi-way merge compaction across immutable 4 KB index segments.

use std::collections::BTreeMap;
use std::io;

use crate::pal::{PlatformFile, PlatformFs};
use crate::wal::{BlobKind, BlobPtr};

use super::{
    IdxSegmentBuilder, IdxSegmentCursor, IdxSegmentReader, VAL_TYPE_BLOB, VAL_TYPE_TOMBSTONE,
};

/// Sequential read-ahead chunk size (`64 * 4 KB = 256 KB`) for compaction merge cursors.
const COMPACTION_READAHEAD_BLOCKS: u32 = 64;

/// Summary returned after multi-way merge compaction of `idx` segments.
pub struct CompactionResult<F: PlatformFile> {
    /// Newly created merged segment reader.
    pub new_segment: IdxSegmentReader<F>,
    /// Dead bytes accumulated per `(BlobKind, table_id, blob_seq_id)` from overwritten or
    /// tombstoned `BlobPtr` entries during compaction.
    pub newly_dead_blob_bytes: Vec<(BlobKind, u16, u64, u64)>,
}

/// Merges multiple `IdxSegmentReader` instances into a single sorted `idx_<new_seq_id>.db`
/// segment using a bounded-memory streaming K-way merge ($O(K \times 4\text{ KB})$ RAM), keeping
/// only the highest-`lsn` version of each key and accumulating dead blob bytes for any overwritten
/// or deleted `BlobPtr` without copying live blobs!
pub fn compact_idx_segments<F: PlatformFile>(
    fs: &impl PlatformFs<File = F>,
    inputs: &[&IdxSegmentReader<F>],
    new_seq_id: u64,
    target_level: u16,
    bloom_bits_per_key: usize,
    drop_tombstones: bool,
) -> io::Result<CompactionResult<F>> {
    let mut cursors: Vec<IdxSegmentCursor<'_, F>> = Vec::with_capacity(inputs.len());
    for &seg in inputs {
        cursors.push(seg.cursor_all_batched(COMPACTION_READAHEAD_BLOCKS)?);
    }

    let mut builder = IdxSegmentBuilder::create(fs, new_seq_id, target_level, bloom_bits_per_key)?;
    builder.set_cap_bloom_to_meta_budget(true);
    let mut dead_blob_map: BTreeMap<(u8, u16, u64), u64> = BTreeMap::new();
    let mut target_key_buf: Vec<u8> = Vec::with_capacity(128);
    let mut winning_val_buf: Vec<u8> = Vec::with_capacity(256);

    loop {
        let mut min_key: Option<&[u8]> = None;
        for cur in &cursors {
            if let Some(k) = cur.current_key() {
                match min_key {
                    None => min_key = Some(k),
                    Some(mk) if k < mk => {
                        min_key = Some(k);
                    }
                    _ => {}
                }
            }
        }

        let Some(target_key_slice) = min_key else {
            break;
        };
        target_key_buf.clear();
        target_key_buf.extend_from_slice(target_key_slice);

        let mut winning_meta: Option<(u64, u8)> = None;
        for cur in &mut cursors {
            while cur.current_key() == Some(target_key_buf.as_slice()) {
                let raw = cur.peek_raw().unwrap();
                let mut record_dead_blob = |vtype: u8, bytes: &[u8]| {
                    if vtype == VAL_TYPE_BLOB {
                        if let Ok(ptr) = BlobPtr::from_bytes(bytes) {
                            *dead_blob_map
                                .entry((ptr.kind as u8, ptr.table_id, ptr.seg_id))
                                .or_insert(0) += ptr.len as u64;
                        }
                    }
                };
                match &mut winning_meta {
                    None => {
                        winning_meta = Some((raw.lsn, raw.val_type));
                        winning_val_buf.clear();
                        winning_val_buf.extend_from_slice(raw.val_bytes);
                    }
                    Some((win_lsn, win_vtype)) => {
                        if raw.lsn > *win_lsn {
                            record_dead_blob(*win_vtype, &winning_val_buf);
                            *win_lsn = raw.lsn;
                            *win_vtype = raw.val_type;
                            winning_val_buf.clear();
                            winning_val_buf.extend_from_slice(raw.val_bytes);
                        } else {
                            record_dead_blob(raw.val_type, raw.val_bytes);
                        }
                    }
                }
                cur.advance()?;
            }
        }

        if let Some((lsn, val_type)) = winning_meta {
            if drop_tombstones && val_type == VAL_TYPE_TOMBSTONE {
                continue;
            }
            builder.add_raw_entry(&target_key_buf, lsn, val_type, &winning_val_buf)?;
        }
    }

    let new_segment = builder.finish()?;
    let newly_dead_blob_bytes = dead_blob_map
        .into_iter()
        .map(|((k, tid, sid), bytes)| {
            let kind = if k == 0 {
                BlobKind::Standard
            } else {
                BlobKind::Lru
            };
            (kind, tid, sid, bytes)
        })
        .collect();

    Ok(CompactionResult {
        new_segment,
        newly_dead_blob_bytes,
    })
}
