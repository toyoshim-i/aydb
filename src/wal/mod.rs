//! Layer 2 (Part 2): Pre-Allocated Fixed-Size Epoch WAL (`wal_sync.db` & `wal_buf.db`)
//! and Two-Stream Crash Recovery.
//!
//! See `docs/phase1/03_EPOCH_WAL_AND_RECOVERY.md` for the detailed sector layout,
//! LevelDB-style record fragmentation, and recovery state machine.

use std::io;
use std::sync::Arc;

use crate::pal::{OpenHint, PlatformFile, PlatformFs, PreallocStrategy};
use crate::util::byte_reader::ByteReader;
use crate::util::crc32c::crc32c;

/// Fixed filename for the synchronous write-through WAL stream.
pub const WAL_SYNC_FILE_NAME: &str = "wal_sync.db";

/// Fixed filename for the buffered high-throughput WAL stream.
pub const WAL_BUF_FILE_NAME: &str = "wal_buf.db";

/// Default size for `wal_sync.db` (8 MB = 2,048 sectors of 4 KB).
///
/// A synchronous commit occupies at least one sector, and a full WAL forces a MemTable flush
/// regardless of how much the MemTable holds, so this size bounds the flush interval of small
/// synchronous commits.
pub const DEFAULT_SYNC_WAL_SIZE: u64 = 8 * 1024 * 1024;

/// Default size for `wal_buf.db` (16 MB = 4,096 sectors of 4 KB).
pub const DEFAULT_BUF_WAL_SIZE: u64 = 16 * 1024 * 1024;

/// Size of a single WAL sector (4096 bytes).
pub const WAL_SECTOR_SIZE: usize = 4096;

/// Size of the WAL sector header (`0..16`).
pub const WAL_SECTOR_HEADER_SIZE: usize = 16;

/// Maximum fragment payload bytes inside a single 4 KB sector (`4080` bytes).
pub const WAL_SECTOR_PAYLOAD_CAP: usize = WAL_SECTOR_SIZE - WAL_SECTOR_HEADER_SIZE;

/// Size of a fragment header (`[frag_type: u8 | frag_len: u16 LE]` = 3 bytes).
pub const FRAGMENT_HEADER_SIZE: usize = 3;

/// Maximum data bytes in a single fragment (`4077` bytes).
pub const MAX_FRAGMENT_DATA_LEN: usize = WAL_SECTOR_PAYLOAD_CAP - FRAGMENT_HEADER_SIZE;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum FragmentType {
    Full = 1,
    First = 2,
    Middle = 3,
    Last = 4,
}

impl FragmentType {
    fn from_u8(v: u8) -> io::Result<Self> {
        match v {
            1 => Ok(Self::Full),
            2 => Ok(Self::First),
            3 => Ok(Self::Middle),
            4 => Ok(Self::Last),
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid WAL fragment type: {other}"),
            )),
        }
    }
}

/// Identifies whether an out-of-line blob resides in a standard blob file or a per-table LRU blob file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum BlobKind {
    Standard = 0,
    Lru = 1,
}

impl BlobKind {
    pub fn from_u8(v: u8) -> io::Result<Self> {
        match v {
            0 => Ok(Self::Standard),
            1 => Ok(Self::Lru),
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid BlobKind discriminant: {other}"),
            )),
        }
    }
}

/// Compact pointer referencing an out-of-line payload stored in a `blob_*.db` segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlobPtr {
    pub kind: BlobKind,
    /// `0` for `BlobKind::Standard`, or the target `TableID` for `BlobKind::Lru`.
    pub table_id: u16,
    pub seg_id: u64,
    pub offset: u32,
    pub len: u32,
    pub crc32c: u32,
}

/// A single key-value mutation logged inside a [`WalBatch`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalRecord {
    PutInline {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    PutBlobPtr {
        key: Vec<u8>,
        blob_ptr: BlobPtr,
    },
    Delete {
        key: Vec<u8>,
    },
    /// Marks the start of a multi-chunk transaction.
    TxBegin,
    /// Marks the final chunk of a multi-chunk transaction.
    TxCommit,
}

/// An atomic group of [`WalRecord`] mutations assigned a single monotonically increasing `lsn`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalBatch {
    pub lsn: u64,
    pub records: Vec<WalRecord>,
}

impl WalBatch {
    pub fn new(lsn: u64, records: Vec<WalRecord>) -> Self {
        Self { lsn, records }
    }

    /// Appends serialized record bytes for `records` into `buf`.
    pub fn append_records_to_buf(records: &[WalRecord], buf: &mut Vec<u8>) {
        let mut extra = 0usize;
        for rec in records {
            extra += match rec {
                WalRecord::PutInline { key, value } => 9 + key.len() + value.len(),
                WalRecord::PutBlobPtr { key, .. } => 28 + key.len(),
                WalRecord::Delete { key } => 5 + key.len(),
                WalRecord::TxBegin | WalRecord::TxCommit => 1,
            };
        }
        buf.reserve(extra);
        for rec in records {
            match rec {
                WalRecord::PutInline { key, value } => {
                    buf.push(1u8);
                    buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
                    buf.extend_from_slice(key);
                    buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
                    buf.extend_from_slice(value);
                }
                WalRecord::PutBlobPtr { key, blob_ptr } => {
                    buf.push(2u8);
                    buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
                    buf.extend_from_slice(key);
                    buf.push(blob_ptr.kind as u8);
                    buf.extend_from_slice(&blob_ptr.table_id.to_le_bytes());
                    buf.extend_from_slice(&blob_ptr.seg_id.to_le_bytes());
                    buf.extend_from_slice(&blob_ptr.offset.to_le_bytes());
                    buf.extend_from_slice(&blob_ptr.len.to_le_bytes());
                    buf.extend_from_slice(&blob_ptr.crc32c.to_le_bytes());
                }
                WalRecord::Delete { key } => {
                    buf.push(3u8);
                    buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
                    buf.extend_from_slice(key);
                }
                WalRecord::TxBegin => {
                    buf.push(4u8);
                }
                WalRecord::TxCommit => {
                    buf.push(5u8);
                }
            }
        }
    }

    /// Fills in the 16-byte `[lsn (8B) | batch_crc32c (4B) | record_count (4B)]` header at `buf[0..16]`.
    pub fn finalize_encoded_buf(lsn: u64, record_count: u32, buf: &mut [u8]) {
        let batch_crc = crc32c(&buf[16..]);
        buf[0..8].copy_from_slice(&lsn.to_le_bytes());
        buf[8..12].copy_from_slice(&batch_crc.to_le_bytes());
        buf[12..16].copy_from_slice(&record_count.to_le_bytes());
    }

    /// Serializes this batch into `[lsn (8B) | batch_crc32c (4B) | record_count (4B) | records...]`
    /// reusing an existing buffer `out` without intermediate heap allocations.
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        out.clear();
        out.extend_from_slice(&[0u8; 16]);
        Self::append_records_to_buf(&self.records, out);
        Self::finalize_encoded_buf(self.lsn, self.records.len() as u32, out);
    }

    /// Serializes this batch into `[lsn (8B) | batch_crc32c (4B) | record_count (4B) | records...]`
    /// in a single allocation without intermediate copying.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(64);
        self.encode_into(&mut out);
        out
    }

    /// Decodes a serialized [`WalBatch`] and verifies its `batch_crc32c`.
    pub fn decode(data: &[u8]) -> io::Result<Self> {
        let mut r = ByteReader::with_eof_msg(data, "truncated WalBatch payload");

        let lsn = r.read_u64_le()?;
        let expected_crc = r.read_u32_le()?;
        let record_count = r.read_u32_le()? as usize;

        if crc32c(r.remaining_slice()) != expected_crc {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "WalBatch CRC32C mismatch",
            ));
        }

        // Minimum serialized WalRecord is 1 byte (1B op for TxBegin / TxCommit).
        if record_count > r.remaining() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "WalBatch record_count exceeds remaining payload bytes",
            ));
        }
        let mut records = Vec::new();
        records
            .try_reserve_exact(record_count)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "WalBatch records allocation overflow"))?;
        for _ in 0..record_count {
            let op = r.read_u8()?;
            match op {
                1 => {
                    let klen = r.read_u32_le()? as usize;
                    let key = r.read_bytes(klen)?.to_vec();
                    let vlen = r.read_u32_le()? as usize;
                    let value = r.read_bytes(vlen)?.to_vec();
                    records.push(WalRecord::PutInline { key, value });
                }
                2 => {
                    let klen = r.read_u32_le()? as usize;
                    let key = r.read_bytes(klen)?.to_vec();
                    let kind = BlobKind::from_u8(r.read_u8()?)?;
                    let table_id = r.read_u16_le()?;
                    let seg_id = r.read_u64_le()?;
                    let offset = r.read_u32_le()?;
                    let len = r.read_u32_le()?;
                    let crc = r.read_u32_le()?;
                    records.push(WalRecord::PutBlobPtr {
                        key,
                        blob_ptr: BlobPtr {
                            kind,
                            table_id,
                            seg_id,
                            offset,
                            len,
                            crc32c: crc,
                        },
                    });
                }
                3 => {
                    let klen = r.read_u32_le()? as usize;
                    let key = r.read_bytes(klen)?.to_vec();
                    records.push(WalRecord::Delete { key });
                }
                4 => {
                    records.push(WalRecord::TxBegin);
                }
                5 => {
                    records.push(WalRecord::TxCommit);
                }
                other => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("unknown WalRecord op_type: {other}"),
                    ));
                }
            }
        }

        if !r.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unexpected trailing bytes in WalBatch",
            ));
        }

        Ok(Self { lsn, records })
    }
}

/// Controls how a [`WalStreamWriter`] packs and flushes 4 KB sectors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalStreamMode {
    /// For `wal_sync.db`: Every batch starts on a fresh 4 KB sector boundary and advances to
    /// the next sector upon completion so already-committed sectors are never overwritten in place.
    SyncSectorIsolated,
    /// For `wal_buf.db` (default): Packs multiple batches densely into the active 4 KB sector
    /// and writes the updated 4 KB sector into the OS Page Cache immediately so process crashes
    /// (`kill -9` / `TerminateProcess`) lose zero committed batches.
    BufferedImmediateOsCache,
    /// For `wal_buf.db` (high-throughput bulk mode): Packs multiple batches densely in user-space
    /// RAM and only issues an OS write syscall when a 4 KB sector fills up or `flush_buffered()`
    /// is called explicitly.
    BufferedUserSpaceCoalesced,
}

/// Result of attempting to append a [`WalBatch`] to a fixed-size [`WalStreamWriter`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalAppendOutcome {
    /// Batch was written starting at `first_sector_idx` and spanning `sectors_touched` sectors.
    Appended {
        first_sector_idx: usize,
        sectors_touched: usize,
    },
    /// The fixed-size WAL ring does not have enough remaining sectors in the current epoch;
    /// the caller must flush the `MemTable` and rotate to `wal_epoch + 1`.
    RingFull,
}

/// Writer for a single fixed-size epoch-based WAL stream (`wal_sync.db` or `wal_buf.db`).
pub struct WalStreamWriter<F: PlatformFile> {
    file: Arc<F>,
    mode: WalStreamMode,
    total_sectors: usize,
    active_epoch: u64,
    /// Index of the current 4 KB sector (`0..total_sectors`).
    current_sector_idx: usize,
    /// Number of valid fragment bytes currently occupying `sector_buf[16 .. 16 + current_valid_bytes]`.
    current_valid_bytes: usize,
    /// Whether `sector_buf` has un-flushed user-space bytes (used in `BufferedUserSpaceCoalesced`).
    dirty_in_user_space: bool,
    /// Scratch buffer for the currently active 4 KB sector.
    sector_buf: [u8; WAL_SECTOR_SIZE],
    /// Backup buffer for rolling back `sector_buf` on I/O error without per-append heap allocation.
    backup_sector_buf: Box<[u8; WAL_SECTOR_SIZE]>,
    /// Scratch buffer for `append_batch` serialization without per-call heap allocation.
    scratch_encode_buf: Vec<u8>,
    /// Contiguous staging buffer for coalescing multi-sector batch writes into a single `write_all_at` syscall.
    multi_sector_buf: Vec<u8>,
}

impl<F: PlatformFile> WalStreamWriter<F> {
    /// Opens or initializes a fixed-size WAL stream file.
    pub fn open(
        fs: &impl PlatformFs<File = F>,
        file_name: &str,
        target_size: u64,
        active_epoch: u64,
        mode: WalStreamMode,
    ) -> io::Result<Self> {
        assert!(
            target_size >= WAL_SECTOR_SIZE as u64 && target_size.is_multiple_of(WAL_SECTOR_SIZE as u64),
            "WAL size must be a positive multiple of 4096 bytes"
        );
        let (hint, prealloc) = match mode {
            WalStreamMode::SyncSectorIsolated => {
                (OpenHint::MutableSyncLog, PreallocStrategy::PhysicalZeroFill)
            }
            WalStreamMode::BufferedImmediateOsCache
            | WalStreamMode::BufferedUserSpaceCoalesced => (
                OpenHint::MutableBufferedLog,
                PreallocStrategy::LogicalExtendOnly,
            ),
        };

        let file = fs.open_file(file_name, true, true, hint)?;
        if file.file_len()? < target_size {
            file.preallocate(target_size, prealloc)?;
        }

        let total_sectors = (target_size / WAL_SECTOR_SIZE as u64) as usize;
        Ok(Self {
            file,
            mode,
            total_sectors,
            active_epoch,
            current_sector_idx: 0,
            current_valid_bytes: 0,
            dirty_in_user_space: false,
            sector_buf: [0u8; WAL_SECTOR_SIZE],
            backup_sector_buf: Box::new([0u8; WAL_SECTOR_SIZE]),
            scratch_encode_buf: Vec::with_capacity(512),
            multi_sector_buf: Vec::new(),
        })
    }

    /// Positions the writer cursor right after the last valid sector recovered during startup.
    pub fn set_cursor_after_recovery(&mut self, next_sector_idx: usize) {
        self.current_sector_idx = next_sector_idx.min(self.total_sectors);
        self.current_valid_bytes = 0;
        self.dirty_in_user_space = false;
        self.sector_buf.fill(0);
    }

    /// Rotates the WAL stream to a new `WalEpoch` in $O(1)$ time without truncating or zero-filling
    /// the underlying file on disk.
    pub fn reset_for_new_epoch(&mut self, new_epoch: u64) {
        assert!(
            new_epoch > self.active_epoch,
            "new WAL epoch ({new_epoch}) must be strictly greater than current ({})",
            self.active_epoch
        );
        self.active_epoch = new_epoch;
        self.current_sector_idx = 0;
        self.current_valid_bytes = 0;
        self.dirty_in_user_space = false;
        self.sector_buf.fill(0);
    }

    pub fn active_epoch(&self) -> u64 {
        self.active_epoch
    }

    pub fn file(&self) -> &Arc<F> {
        &self.file
    }

    pub fn current_sector_idx(&self) -> usize {
        self.current_sector_idx
    }

    pub fn total_sectors(&self) -> usize {
        self.total_sectors
    }

    #[cfg(test)]
    pub(crate) fn discard_user_space_buffer_for_test(&mut self) {
        self.dirty_in_user_space = false;
    }

    /// Grows the underlying WAL file if `target_size > current_size` to accommodate an oversized
    /// multi-chunk transaction in a single epoch.
    pub fn ensure_min_size(&mut self, target_size: u64) -> io::Result<()> {
        let aligned = target_size.div_ceil(WAL_SECTOR_SIZE as u64) * WAL_SECTOR_SIZE as u64;
        let new_sectors = (aligned / WAL_SECTOR_SIZE as u64) as usize;
        if new_sectors > self.total_sectors {
            let prealloc = match self.mode {
                WalStreamMode::SyncSectorIsolated => PreallocStrategy::PhysicalZeroFill,
                WalStreamMode::BufferedImmediateOsCache
                | WalStreamMode::BufferedUserSpaceCoalesced => PreallocStrategy::LogicalExtendOnly,
            };
            self.file.preallocate(aligned, prealloc)?;
            self.total_sectors = new_sectors;
        }
        Ok(())
    }

    /// Appends an atomic [`WalBatch`] to the WAL stream, fragmenting across 4 KB sectors if needed.
    pub fn append_batch(&mut self, batch: &WalBatch) -> io::Result<WalAppendOutcome> {
        let mut raw = core::mem::take(&mut self.scratch_encode_buf);
        batch.encode_into(&mut raw);
        let res = self.append_raw_batch(&raw);
        raw.clear();
        self.scratch_encode_buf = raw;
        res
    }

    /// Appends pre-encoded batch bytes.
    ///
    /// Failure-atomic with respect to writer state: if any write or sync fails, the cursor
    /// (`current_sector_idx`, `current_valid_bytes`, `dirty_in_user_space`, `sector_buf`) is restored
    /// to its pre-call value. Restoring is safe even if some bytes reached disk: the next append
    /// rewrites the same sectors starting at the same index, and any leftover trailing sectors of a
    /// failed multi-sector batch are `Middle`/`Last` fragments without a preceding `First`, which
    /// recovery rejects. The on-disk outcome of the failed batch is nevertheless *unknown*, so
    /// callers should treat the stream as poisoned (see `StorageEngine::is_poisoned`).
    pub fn append_raw_batch(&mut self, remaining: &[u8]) -> io::Result<WalAppendOutcome> {
        let saved_idx = self.current_sector_idx;
        let saved_valid = self.current_valid_bytes;
        let saved_dirty = self.dirty_in_user_space;
        if saved_valid > 0 {
            self.backup_sector_buf.copy_from_slice(&self.sector_buf);
        }
        let res = self.append_raw_batch_inner(remaining);
        if res.is_err() {
            self.current_sector_idx = saved_idx;
            self.current_valid_bytes = saved_valid;
            self.dirty_in_user_space = saved_dirty;
            if saved_valid > 0 {
                self.sector_buf.copy_from_slice(&*self.backup_sector_buf);
            } else {
                self.sector_buf.fill(0);
            }
            self.multi_sector_buf.clear();
        }
        res
    }

    fn append_raw_batch_inner(&mut self, mut remaining: &[u8]) -> io::Result<WalAppendOutcome> {
        if remaining.is_empty() {
            return Ok(WalAppendOutcome::Appended {
                first_sector_idx: self.current_sector_idx,
                sectors_touched: 0,
            });
        }

        // First, calculate accurately how many sectors this batch will require so we never
        // write half a batch before discovering that the ring is full!
        let required_final_sector = self.preview_end_sector(remaining.len());
        if required_final_sector > self.total_sectors {
            return Ok(WalAppendOutcome::RingFull);
        }

        // If the current partially-filled buffered sector has fewer than FRAGMENT_HEADER_SIZE (3B)
        // free bytes left, seal and advance to the next sector first.
        if self.current_valid_bytes > 0
            && WAL_SECTOR_PAYLOAD_CAP - self.current_valid_bytes < FRAGMENT_HEADER_SIZE
        {
            self.flush_and_advance_sector()?;
        }

        let first_sector_idx = self.current_sector_idx;
        let mut sectors_touched = 0usize;
        let mut is_first_piece = true;
        let mut staged_start_sector: Option<usize> = None;
        self.multi_sector_buf.clear();

        while !remaining.is_empty() {
            let avail_payload = WAL_SECTOR_PAYLOAD_CAP - self.current_valid_bytes;
            let max_data = avail_payload - FRAGMENT_HEADER_SIZE;
            let chunk_len = remaining.len().min(max_data);
            let is_last_piece = chunk_len == remaining.len();

            let frag_type = match (is_first_piece, is_last_piece) {
                (true, true) => FragmentType::Full,
                (true, false) => FragmentType::First,
                (false, false) => FragmentType::Middle,
                (false, true) => FragmentType::Last,
            };

            let write_pos = WAL_SECTOR_HEADER_SIZE + self.current_valid_bytes;
            self.sector_buf[write_pos] = frag_type as u8;
            self.sector_buf[write_pos + 1..write_pos + 3]
                .copy_from_slice(&(chunk_len as u16).to_le_bytes());
            self.sector_buf[write_pos + 3..write_pos + 3 + chunk_len]
                .copy_from_slice(&remaining[..chunk_len]);

            self.current_valid_bytes += FRAGMENT_HEADER_SIZE + chunk_len;
            remaining = &remaining[chunk_len..];
            is_first_piece = false;
            sectors_touched += 1;

            match self.mode {
                WalStreamMode::SyncSectorIsolated => {
                    self.seal_current_sector_in_memory();
                    if staged_start_sector.is_none() && remaining.is_empty() {
                        let offset = (self.current_sector_idx * WAL_SECTOR_SIZE) as u64;
                        self.file.write_all_at(&self.sector_buf, offset)?;
                    } else {
                        if staged_start_sector.is_none() {
                            staged_start_sector = Some(self.current_sector_idx);
                        }
                        self.multi_sector_buf.extend_from_slice(&self.sector_buf);
                    }
                    self.current_sector_idx += 1;
                    self.current_valid_bytes = 0;
                    self.dirty_in_user_space = false;
                    self.sector_buf.fill(0);
                }
                WalStreamMode::BufferedImmediateOsCache => {
                    if !remaining.is_empty()
                        || WAL_SECTOR_PAYLOAD_CAP - self.current_valid_bytes < FRAGMENT_HEADER_SIZE
                    {
                        self.seal_current_sector_in_memory();
                        if staged_start_sector.is_none() && remaining.is_empty() {
                            let offset = (self.current_sector_idx * WAL_SECTOR_SIZE) as u64;
                            self.file.write_all_at(&self.sector_buf, offset)?;
                        } else {
                            if staged_start_sector.is_none() {
                                staged_start_sector = Some(self.current_sector_idx);
                            }
                            self.multi_sector_buf.extend_from_slice(&self.sector_buf);
                        }
                        self.current_sector_idx += 1;
                        self.current_valid_bytes = 0;
                        self.dirty_in_user_space = false;
                        self.sector_buf.fill(0);
                    } else {
                        // Write the updated 4 KB sector into the OS Page Cache immediately
                        // while keeping the sector open for further dense packing.
                        self.seal_current_sector_in_memory();
                        if staged_start_sector.is_some() {
                            self.multi_sector_buf.extend_from_slice(&self.sector_buf);
                        } else {
                            let offset = (self.current_sector_idx * WAL_SECTOR_SIZE) as u64;
                            self.file.write_all_at(&self.sector_buf, offset)?;
                        }
                    }
                }
                WalStreamMode::BufferedUserSpaceCoalesced => {
                    if !remaining.is_empty()
                        || WAL_SECTOR_PAYLOAD_CAP - self.current_valid_bytes < FRAGMENT_HEADER_SIZE
                    {
                        self.seal_current_sector_in_memory();
                        if staged_start_sector.is_none() && remaining.is_empty() {
                            let offset = (self.current_sector_idx * WAL_SECTOR_SIZE) as u64;
                            self.file.write_all_at(&self.sector_buf, offset)?;
                        } else {
                            if staged_start_sector.is_none() {
                                staged_start_sector = Some(self.current_sector_idx);
                            }
                            self.multi_sector_buf.extend_from_slice(&self.sector_buf);
                        }
                        self.current_sector_idx += 1;
                        self.current_valid_bytes = 0;
                        self.dirty_in_user_space = false;
                        self.sector_buf.fill(0);
                    } else {
                        self.dirty_in_user_space = true;
                    }
                }
            }
        }

        if let Some(start_sec) = staged_start_sector {
            let offset = (start_sec * WAL_SECTOR_SIZE) as u64;
            self.file.write_all_at(&self.multi_sector_buf, offset)?;
            self.multi_sector_buf.clear();
        }

        if self.mode == WalStreamMode::SyncSectorIsolated {
            self.file.sync_barrier()?;
        }

        Ok(WalAppendOutcome::Appended {
            first_sector_idx,
            sectors_touched,
        })
    }

    /// Flushes any pending user-space buffered sector data into the OS Page Cache.
    pub fn flush_buffered_to_os(&mut self) -> io::Result<()> {
        // `seal_and_write_current_sector` does not touch the cursor, so on error the state
        // (still dirty) is unchanged and a later flush can retry.
        if self.dirty_in_user_space && self.current_valid_bytes > 0 {
            self.seal_and_write_current_sector()?;
            self.dirty_in_user_space = false;
        }
        Ok(())
    }

    /// Closes the currently open partially-filled sector (if any) so that no later append ever
    /// rewrites it in place.
    ///
    /// Buffered modes pack batches densely and rewrite the open 4 KB sector on every append. Once
    /// that sector has been (or is about to be) made durable by a sync, an in-place rewrite that is
    /// torn by power loss would invalidate the sector CRC and destroy already-synced batches. Call
    /// this *before* the sync: the sealed sector is what gets synced, and subsequent appends start
    /// in the next, empty sector. Cost: at most one partially wasted sector per sync point. If the
    /// cursor reaches `total_sectors`, the next append returns `RingFull` and the caller rotates.
    ///
    /// Failure-atomic: the cursor only advances after the sector write succeeded.
    pub fn seal_open_sector_for_durability(&mut self) -> io::Result<()> {
        if self.current_valid_bytes == 0 {
            return Ok(());
        }
        if self.dirty_in_user_space || self.mode == WalStreamMode::BufferedUserSpaceCoalesced {
            self.seal_and_write_current_sector()?;
        }
        // In `BufferedImmediateOsCache` mode the open sector was already written by the append.
        self.current_sector_idx += 1;
        self.current_valid_bytes = 0;
        self.dirty_in_user_space = false;
        self.sector_buf.fill(0);
        Ok(())
    }

    /// Flushes pending bytes to the OS, seals the open sector (see
    /// [`Self::seal_open_sector_for_durability`]) and then syncs the file: with `sync_durable`
    /// when `durable` is true, otherwise with `sync_barrier`.
    pub fn make_durable_and_seal(&mut self, durable: bool) -> io::Result<()> {
        self.flush_buffered_to_os()?;
        self.seal_open_sector_for_durability()?;
        if durable {
            self.file.sync_durable()
        } else {
            self.file.sync_barrier()
        }
    }

    fn preview_end_sector(&self, mut rem_len: usize) -> usize {
        let mut sec_idx = self.current_sector_idx;
        let mut valid = self.current_valid_bytes;

        if valid > 0 && WAL_SECTOR_PAYLOAD_CAP - valid < FRAGMENT_HEADER_SIZE {
            sec_idx += 1;
            valid = 0;
        }

        while rem_len > 0 {
            if sec_idx >= self.total_sectors {
                return self.total_sectors + 1;
            }
            let max_data = (WAL_SECTOR_PAYLOAD_CAP - valid) - FRAGMENT_HEADER_SIZE;
            let take = rem_len.min(max_data);
            rem_len -= take;
            valid += FRAGMENT_HEADER_SIZE + take;

            if self.mode == WalStreamMode::SyncSectorIsolated
                || rem_len > 0
                || WAL_SECTOR_PAYLOAD_CAP - valid < FRAGMENT_HEADER_SIZE
            {
                sec_idx += 1;
                valid = 0;
            }
        }

        // If `valid > 0` in buffered mode, the active sector at `sec_idx` must be < total_sectors.
        if valid > 0 { sec_idx + 1 } else { sec_idx }
    }

    fn seal_current_sector_in_memory(&mut self) {
        // Zero out any trailing padding bytes in `16 + current_valid_bytes .. 4096`.
        let pad_start = WAL_SECTOR_HEADER_SIZE + self.current_valid_bytes;
        self.sector_buf[pad_start..WAL_SECTOR_SIZE].fill(0);

        self.sector_buf[4..12].copy_from_slice(&self.active_epoch.to_le_bytes());
        self.sector_buf[12..14].copy_from_slice(&(self.current_valid_bytes as u16).to_le_bytes());
        self.sector_buf[14..16].copy_from_slice(&0u16.to_le_bytes());

        let sector_crc = crc32c(&self.sector_buf[4..WAL_SECTOR_SIZE]);
        self.sector_buf[0..4].copy_from_slice(&sector_crc.to_le_bytes());
    }

    fn seal_and_write_current_sector(&mut self) -> io::Result<()> {
        self.seal_current_sector_in_memory();
        let offset = (self.current_sector_idx * WAL_SECTOR_SIZE) as u64;
        self.file.write_all_at(&self.sector_buf, offset)
    }

    fn flush_and_advance_sector(&mut self) -> io::Result<()> {
        if self.current_valid_bytes > 0 {
            self.seal_and_write_current_sector()?;
            self.current_sector_idx += 1;
            self.current_valid_bytes = 0;
            self.dirty_in_user_space = false;
            self.sector_buf.fill(0);
        }
        Ok(())
    }
}

impl<F: PlatformFile> Drop for WalStreamWriter<F> {
    fn drop(&mut self) {
        let _ = self.flush_buffered_to_os();
    }
}

/// Summary returned after scanning a single WAL file during crash recovery.
#[derive(Debug, Clone)]
pub struct SingleStreamRecovery {
    /// Reassembled, CRC-verified batches in sequential order.
    pub batches: Vec<WalBatch>,
    /// Index of the first sector after the valid log prefix (where new writes may resume).
    pub next_sector_idx: usize,
}

/// Scans a single WAL file from Sector 0 and extracts all complete [`WalBatch`] items matching
/// `expected_epoch`, halting cleanly at the first sector with an older epoch or a torn-write CRC.
pub fn recover_single_wal_stream<F: PlatformFile>(
    file: &F,
    expected_epoch: u64,
) -> io::Result<SingleStreamRecovery> {
    let file_len = file.file_len()?;
    let total_sectors = (file_len / WAL_SECTOR_SIZE as u64) as usize;

    let mut batches = Vec::new();
    let mut partial_batch_buf: Option<Vec<u8>> = None;
    let mut next_sector_idx = 0usize;

    // Read in 256 KB windows (64 sectors) to maximize sequential throughput during recovery.
    const WINDOW_SECTORS: usize = 64;
    let mut window_buf = vec![0u8; WINDOW_SECTORS * WAL_SECTOR_SIZE];

    let mut sec_idx = 0usize;
    while sec_idx < total_sectors {
        let count = (total_sectors - sec_idx).min(WINDOW_SECTORS);
        let byte_len = count * WAL_SECTOR_SIZE;
        file.read_exact_at(
            &mut window_buf[..byte_len],
            (sec_idx * WAL_SECTOR_SIZE) as u64,
        )?;

        let mut halted = false;
        for i in 0..count {
            let sector = &window_buf[i * WAL_SECTOR_SIZE..(i + 1) * WAL_SECTOR_SIZE];
            let sector_epoch = u64::from_le_bytes(sector[4..12].try_into().unwrap());
            if sector_epoch != expected_epoch {
                // Reached a sector from a previous epoch (or uninitialized 0). Log tail found!
                halted = true;
                break;
            }

            let expected_crc = u32::from_le_bytes(sector[0..4].try_into().unwrap());
            let actual_crc = crc32c(&sector[4..WAL_SECTOR_SIZE]);
            if actual_crc != expected_crc {
                // Reached a torn write at the log tail!
                halted = true;
                break;
            }

            let valid_bytes = u16::from_le_bytes(sector[12..14].try_into().unwrap()) as usize;
            if valid_bytes == 0 || valid_bytes > WAL_SECTOR_PAYLOAD_CAP {
                halted = true;
                break;
            }

            // Parse fragments within `sector[16 .. 16 + valid_bytes]`.
            let mut pos = WAL_SECTOR_HEADER_SIZE;
            let limit = WAL_SECTOR_HEADER_SIZE + valid_bytes;
            let mut sector_corrupt = false;

            while pos < limit {
                if limit - pos < FRAGMENT_HEADER_SIZE {
                    sector_corrupt = true;
                    break;
                }
                let frag_type = match FragmentType::from_u8(sector[pos]) {
                    Ok(ft) => ft,
                    Err(_) => {
                        sector_corrupt = true;
                        break;
                    }
                };
                let frag_len = u16::from_le_bytes([sector[pos + 1], sector[pos + 2]]) as usize;
                pos += FRAGMENT_HEADER_SIZE;
                if limit - pos < frag_len {
                    sector_corrupt = true;
                    break;
                }
                let frag_data = &sector[pos..pos + frag_len];
                pos += frag_len;

                match frag_type {
                    FragmentType::Full => {
                        if let Some(ref mut buf) = partial_batch_buf {
                            // A previous `First`/`Middle` fragment chain was left dangling by a crash;
                            // discard the incomplete partial batch and decode the new `Full` fragment.
                            buf.clear();
                        }
                        partial_batch_buf = None;
                        match WalBatch::decode(frag_data) {
                            Ok(batch) => batches.push(batch),
                            Err(_) => {
                                sector_corrupt = true;
                                break;
                            }
                        }
                    }
                    FragmentType::First => {
                        if let Some(ref mut buf) = partial_batch_buf {
                            // Discard any prior dangling `First`/`Middle` chain.
                            buf.clear();
                            buf.extend_from_slice(frag_data);
                        } else {
                            let mut buf = Vec::with_capacity(frag_len * 2);
                            buf.extend_from_slice(frag_data);
                            partial_batch_buf = Some(buf);
                        }
                    }
                    FragmentType::Middle => {
                        if let Some(ref mut buf) = partial_batch_buf {
                            buf.extend_from_slice(frag_data);
                        } else {
                            sector_corrupt = true;
                            break;
                        }
                    }
                    FragmentType::Last => {
                        if let Some(mut buf) = partial_batch_buf.take() {
                            buf.extend_from_slice(frag_data);
                            match WalBatch::decode(&buf) {
                                Ok(batch) => batches.push(batch),
                                Err(_) => {
                                    sector_corrupt = true;
                                    break;
                                }
                            }
                        } else {
                            sector_corrupt = true;
                            break;
                        }
                    }
                }
            }

            if sector_corrupt {
                halted = true;
                break;
            }

            next_sector_idx = sec_idx + i + 1;
        }

        if halted {
            break;
        }
        sec_idx += count;
    }

    // Any incomplete `First`/`Middle` chain without a `Last` fragment is automatically discarded.
    Ok(SingleStreamRecovery {
        batches,
        next_sector_idx,
    })
}

/// Output of the two-stream (`wal_sync.db` + `wal_buf.db`) crash-recovery coordinator.
#[derive(Debug, Clone)]
pub struct TwoStreamRecoveryResult {
    /// Merged batches ordered by ascending `lsn` (with `lsn > checkpoint_lsn` and valid blobs).
    pub merged_batches: Vec<WalBatch>,
    /// Highest `lsn` observed across all recovered batches (or `checkpoint_lsn` if none).
    pub max_recovered_lsn: u64,
    /// Next sector index for `wal_sync.db`.
    pub next_sync_sector_idx: usize,
    /// Next sector index for `wal_buf.db`.
    pub next_buf_sector_idx: usize,
}

/// Returns whether every `PutBlobPtr` in `batches` passes `validate_blob`. An `Err` from the
/// validator is a real I/O failure and is propagated instead of being read as "blob absent".
fn batches_blobs_valid<V>(batches: &[WalBatch], validate_blob: &mut V) -> io::Result<bool>
where
    V: FnMut(&BlobPtr) -> io::Result<bool>,
{
    for b in batches {
        for rec in &b.records {
            if let WalRecord::PutBlobPtr { blob_ptr, .. } = rec {
                if !validate_blob(blob_ptr)? {
                    return Ok(false);
                }
            }
        }
    }
    Ok(true)
}

/// Recovers both `wal_sync.db` and `wal_buf.db`, filters out batches with `lsn <= checkpoint_lsn`,
/// merges them in $O(N)$ by ascending `lsn`, and validates `PutBlobPtr` records via `validate_blob`
/// (enforcing the `Write-Blob-Before-WAL` crash-consistency protocol). `validate_blob` returns
/// `Ok(false)` for a blob that was never durably written and `Err` for a genuine I/O error, which
/// aborts recovery so that no committed batch is dropped because of a transient failure.
pub fn recover_two_streams<F, V>(
    sync_file: &F,
    sync_epoch: u64,
    buf_file: &F,
    buf_epoch: u64,
    checkpoint_lsn: u64,
    mut validate_blob: V,
) -> io::Result<TwoStreamRecoveryResult>
where
    F: PlatformFile,
    V: FnMut(&BlobPtr) -> io::Result<bool>,
{
    let sync_rec = recover_single_wal_stream(sync_file, sync_epoch)?;
    let buf_rec = recover_single_wal_stream(buf_file, buf_epoch)?;

    // Filter out uncommitted multi-chunk transactions (`TxBegin` without a matching `TxCommit`)
    // per stream AND validate `PutBlobPtr` records across the entire transaction group
    // before stripping transaction markers, while recording the highest LSN observed across
    // all recovered batches.
    let mut max_lsn = checkpoint_lsn;
    //
    // With `prefix_only`, the stream is a commit-ordered prefix: emission stops at the first batch
    // or transaction group whose blob validation returned `Ok(false)`, so no later batch is
    // replayed past a rolled-back one. Remaining batches are still scanned for the highest LSN so
    // that `next_lsn` never reuses a logged LSN.
    let mut filter_committed_stream =
        |batches: Vec<WalBatch>,
         validate_blob: &mut V,
         prefix_only: bool|
         -> io::Result<Vec<WalBatch>> {
            let mut out = Vec::with_capacity(batches.len());
            let mut pending_tx: Option<Vec<WalBatch>> = None;
            let mut truncated = false;

            for mut b in batches {
                if b.lsn > max_lsn {
                    max_lsn = b.lsn;
                }
                if b.lsn <= checkpoint_lsn || truncated {
                    continue;
                }
                let has_begin = b.records.iter().any(|r| matches!(r, WalRecord::TxBegin));
                let has_commit = b.records.iter().any(|r| matches!(r, WalRecord::TxCommit));
                b.records
                    .retain(|r| !matches!(r, WalRecord::TxBegin | WalRecord::TxCommit));

                if has_begin {
                    // Start of a new multi-chunk transaction; discard any prior incomplete one.
                    let mut tx_vec = Vec::new();
                    if !b.records.is_empty() {
                        tx_vec.push(b);
                    }
                    if has_commit {
                        if batches_blobs_valid(&tx_vec, validate_blob)? {
                            out.extend(tx_vec);
                        } else if prefix_only {
                            truncated = true;
                        }
                        pending_tx = None;
                    } else {
                        pending_tx = Some(tx_vec);
                    }
                } else if let Some(ref mut tx_vec) = pending_tx {
                    if !b.records.is_empty() {
                        tx_vec.push(b);
                    }
                    if has_commit {
                        let completed_tx = pending_tx.take().unwrap();
                        if batches_blobs_valid(&completed_tx, validate_blob)? {
                            out.extend(completed_tx);
                        } else if prefix_only {
                            truncated = true;
                        }
                    }
                } else if !b.records.is_empty() {
                    if batches_blobs_valid(core::slice::from_ref(&b), validate_blob)? {
                        out.push(b);
                    } else if prefix_only {
                        truncated = true;
                    }
                }
            }
            Ok(out)
        };

    // Sync-stream blobs are synced before the WAL record, so an invalid blob there is isolated.
    // Buffered-stream blobs and records are flushed independently, so power loss can drop an
    // earlier commit while a later one survives; replay must not expose that hole.
    let sync_batches = filter_committed_stream(sync_rec.batches, &mut validate_blob, false)?;
    let buf_batches = filter_committed_stream(buf_rec.batches, &mut validate_blob, true)?;

    // Two-way merge sort by ascending `lsn` by value (zero-clone).
    let mut merged = Vec::with_capacity(sync_batches.len() + buf_batches.len());
    let mut sync_iter = sync_batches.into_iter().peekable();
    let mut buf_iter = buf_batches.into_iter().peekable();
    while let (Some(s_peek), Some(b_peek)) = (sync_iter.peek(), buf_iter.peek()) {
        if s_peek.lsn <= b_peek.lsn {
            merged.push(sync_iter.next().unwrap());
        } else {
            merged.push(buf_iter.next().unwrap());
        }
    }
    merged.extend(sync_iter);
    merged.extend(buf_iter);

    Ok(TwoStreamRecoveryResult {
        merged_batches: merged,
        max_recovered_lsn: max_lsn,
        next_sync_sector_idx: sync_rec.next_sector_idx,
        next_buf_sector_idx: buf_rec.next_sector_idx,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pal::OsFs;
    use crate::util::test_util::TempTestDir;

    #[test]
    fn multi_sector_fragmentation_and_truncated_tail_recovery() {
        let tmp = TempTestDir::new("frag");
        let fs = OsFs::open(&tmp.path, true).expect("open OsFs");

        // 64 KB WAL (16 sectors of 4 KB)
        let wal_size = 64 * 1024u64;
        let mut writer = WalStreamWriter::open(
            &fs,
            WAL_BUF_FILE_NAME,
            wal_size,
            1,
            WalStreamMode::BufferedImmediateOsCache,
        )
        .expect("open wal_buf");

        // Batch 1: Small batch (< 4 KB)
        let b1 = WalBatch::new(
            1,
            vec![WalRecord::PutInline {
                key: b"k1".to_vec(),
                value: b"small_val".to_vec(),
            }],
        );
        assert!(matches!(
            writer.append_batch(&b1).unwrap(),
            WalAppendOutcome::Appended { .. }
        ));

        // Batch 2: Large batch (~13 KB) spanning First -> Middle -> Middle -> Last across 4 sectors!
        let large_val = vec![0x7Au8; 13_000];
        let b2 = WalBatch::new(
            2,
            vec![
                WalRecord::PutInline {
                    key: b"k_large".to_vec(),
                    value: large_val.clone(),
                },
                WalRecord::Delete {
                    key: b"k_old".to_vec(),
                },
            ],
        );
        let outcome = writer.append_batch(&b2).unwrap();
        assert!(matches!(
            outcome,
            WalAppendOutcome::Appended {
                sectors_touched: 4,
                ..
            }
        ));

        // Batch 3: Another multi-sector batch (~9 KB, 3 sectors) whose final sector we will corrupt!
        let b3 = WalBatch::new(
            3,
            vec![WalRecord::PutInline {
                key: b"k_torn".to_vec(),
                value: vec![0x33u8; 9_000],
            }],
        );
        writer.append_batch(&b3).unwrap();
        let last_written_sector = writer.current_sector_idx() - 1;
        drop(writer);

        // Corrupt the Last sector of Batch 3 to simulate power failure before the final sector landed!
        {
            let f = fs
                .open_file(
                    WAL_BUF_FILE_NAME,
                    false,
                    true,
                    OpenHint::MutableBufferedLog,
                )
                .unwrap();
            f.write_all_at(&[0xFF, 0xFF], (last_written_sector * WAL_SECTOR_SIZE) as u64)
                .unwrap();
        }

        // Recovery must return Batch 1 and Batch 2 intact, and cleanly discard incomplete Batch 3!
        let f = fs
            .open_file(
                WAL_BUF_FILE_NAME,
                false,
                false,
                OpenHint::MutableBufferedLog,
            )
            .unwrap();
        let rec = recover_single_wal_stream(&*f, 1).expect("recover");
        assert_eq!(rec.batches.len(), 2);
        assert_eq!(rec.batches[0], b1);
        assert_eq!(rec.batches[1], b2);
    }

    #[test]
    fn zero_fill_free_epoch_rotation_and_ring_full() {
        let tmp = TempTestDir::new("epoch_rot");
        let fs = OsFs::open(&tmp.path, true).expect("open OsFs");

        // Tiny 4-sector (16 KB) Sync WAL
        let wal_size = (4 * WAL_SECTOR_SIZE) as u64;
        let mut writer = WalStreamWriter::open(
            &fs,
            WAL_SYNC_FILE_NAME,
            wal_size,
            1,
            WalStreamMode::SyncSectorIsolated,
        )
        .expect("open wal_sync");

        // In SyncSectorIsolated mode, each batch takes 1 sector. Write 4 batches to fill all 4 sectors.
        for lsn in 1..=4 {
            let b = WalBatch::new(
                lsn,
                vec![WalRecord::PutInline {
                    key: format!("k{lsn}").into_bytes(),
                    value: b"v".to_vec(),
                }],
            );
            assert_eq!(
                writer.append_batch(&b).unwrap(),
                WalAppendOutcome::Appended {
                    first_sector_idx: (lsn - 1) as usize,
                    sectors_touched: 1,
                }
            );
        }

        // The 5th batch must return RingFull without corrupting any sector!
        let b5 = WalBatch::new(
            5,
            vec![WalRecord::PutInline {
                key: b"k5".to_vec(),
                value: b"v".to_vec(),
            }],
        );
        assert_eq!(writer.append_batch(&b5).unwrap(), WalAppendOutcome::RingFull);

        // Now rotate to Epoch 2 WITHOUT zero-filling the file, and write only 2 batches (Sectors 0 and 1).
        // Sectors 2 and 3 on disk still contain valid sectors from Epoch 1!
        writer.reset_for_new_epoch(2);
        for lsn in 10..=11 {
            let b = WalBatch::new(
                lsn,
                vec![WalRecord::PutInline {
                    key: format!("epoch2_{lsn}").into_bytes(),
                    value: b"v2".to_vec(),
                }],
            );
            writer.append_batch(&b).unwrap();
        }
        drop(writer);

        // Recovery for Epoch 2 must read Sectors 0 and 1, see Epoch 1 at Sector 2, and stop cleanly!
        let f = fs
            .open_file(WAL_SYNC_FILE_NAME, false, false, OpenHint::MutableSyncLog)
            .unwrap();
        let rec = recover_single_wal_stream(&*f, 2).expect("recover epoch 2");
        assert_eq!(rec.batches.len(), 2);
        assert_eq!(rec.batches[0].lsn, 10);
        assert_eq!(rec.batches[1].lsn, 11);
        assert_eq!(rec.next_sector_idx, 2);
    }

    #[test]
    fn two_stream_lsn_merge_and_write_blob_before_wal_validation() {
        let tmp = TempTestDir::new("two_stream");
        let fs = OsFs::open(&tmp.path, true).expect("open OsFs");

        let wal_size = 64 * 1024u64;
        let mut sync_w = WalStreamWriter::open(
            &fs,
            WAL_SYNC_FILE_NAME,
            wal_size,
            5,
            WalStreamMode::SyncSectorIsolated,
        )
        .unwrap();
        let mut buf_w = WalStreamWriter::open(
            &fs,
            WAL_BUF_FILE_NAME,
            wal_size,
            3,
            WalStreamMode::BufferedImmediateOsCache,
        )
        .unwrap();

        // LSN 10 (already checkpointed, <= checkpoint_lsn=10) in buf
        buf_w
            .append_batch(&WalBatch::new(
                10,
                vec![WalRecord::PutInline {
                    key: b"old".to_vec(),
                    value: b"1".to_vec(),
                }],
            ))
            .unwrap();

        // LSN 11 in buf (valid blob seg_id=1)
        buf_w
            .append_batch(&WalBatch::new(
                11,
                vec![WalRecord::PutBlobPtr {
                    key: b"cache_ok".to_vec(),
                    blob_ptr: BlobPtr {
                        kind: BlobKind::Lru,
                        table_id: 2,
                        seg_id: 1,
                        offset: 0,
                        len: 4096,
                        crc32c: 0x1234_5678,
                    },
                }],
            ))
            .unwrap();

        // LSN 12 in sync (critical setting)
        sync_w
            .append_batch(&WalBatch::new(
                12,
                vec![WalRecord::PutInline {
                    key: b"theme".to_vec(),
                    value: b"dark".to_vec(),
                }],
            ))
            .unwrap();

        // LSN 13 in buf (dangling blob seg_id=99 that never reached disk before power loss!)
        buf_w
            .append_batch(&WalBatch::new(
                13,
                vec![WalRecord::PutBlobPtr {
                    key: b"cache_unflushed".to_vec(),
                    blob_ptr: BlobPtr {
                        kind: BlobKind::Lru,
                        table_id: 2,
                        seg_id: 99,
                        offset: 0,
                        len: 8192,
                        crc32c: 0xDEAD_BEEF,
                    },
                }],
            ))
            .unwrap();

        // LSN 14 in sync
        sync_w
            .append_batch(&WalBatch::new(
                14,
                vec![WalRecord::PutInline {
                    key: b"lang".to_vec(),
                    value: b"ja".to_vec(),
                }],
            ))
            .unwrap();

        drop(sync_w);
        drop(buf_w);

        let sync_f = fs
            .open_file(WAL_SYNC_FILE_NAME, false, false, OpenHint::MutableSyncLog)
            .unwrap();
        let buf_f = fs
            .open_file(
                WAL_BUF_FILE_NAME,
                false,
                false,
                OpenHint::MutableBufferedLog,
            )
            .unwrap();

        let res = recover_two_streams(&*sync_f, 5, &*buf_f, 3, 10, |ptr| Ok(ptr.seg_id == 1))
            .expect("recover two streams");

        // Expected LSN order: 11 (from buf), 12 (from sync), 14 (from sync).
        // LSN 10 was filtered by checkpoint_lsn=10, and LSN 13 was safely skipped by BlobValidator!
        let recovered_lsns: Vec<u64> = res.merged_batches.iter().map(|b| b.lsn).collect();
        assert_eq!(recovered_lsns, vec![11, 12, 14]);
        assert_eq!(res.max_recovered_lsn, 14);
    }

    #[test]
    fn buffered_user_space_coalesced_flushes_tail_sector_on_drop() {
        let tmp = TempTestDir::new("coalesced_drop");
        let fs = OsFs::open(&tmp.path, true).expect("open OsFs");

        let wal_size = 64 * 1024u64;
        let mut writer = WalStreamWriter::open(
            &fs,
            WAL_BUF_FILE_NAME,
            wal_size,
            1,
            WalStreamMode::BufferedUserSpaceCoalesced,
        )
        .expect("open wal_buf coalesced");

        let batch = WalBatch::new(
            1,
            vec![WalRecord::PutInline {
                key: b"tail_key".to_vec(),
                value: b"tail_value_under_4kb".to_vec(),
            }],
        );
        writer.append_batch(&batch).unwrap();
        // Drop without calling flush_buffered_to_os() explicitly
        drop(writer);

        let f = fs
            .open_file(
                WAL_BUF_FILE_NAME,
                false,
                false,
                OpenHint::MutableBufferedLog,
            )
            .unwrap();
        let rec = recover_single_wal_stream(&*f, 1).expect("recover tail sector after drop");
        assert_eq!(rec.batches.len(), 1);
        assert_eq!(rec.batches[0], batch);
    }

    #[test]
    fn atomic_wal_batch_rejection_when_any_blob_ptr_is_invalid() {
        let tmp = TempTestDir::new("atomic_batch_reject");
        let fs = OsFs::open(&tmp.path, true).expect("open OsFs");

        let wal_size = 64 * 1024u64;
        let sync_w = WalStreamWriter::open(
            &fs,
            WAL_SYNC_FILE_NAME,
            wal_size,
            1,
            WalStreamMode::SyncSectorIsolated,
        )
        .unwrap();
        let mut buf_w = WalStreamWriter::open(
            &fs,
            WAL_BUF_FILE_NAME,
            wal_size,
            1,
            WalStreamMode::BufferedImmediateOsCache,
        )
        .unwrap();

        // Batch 1 (LSN 1): valid batch
        buf_w
            .append_batch(&WalBatch::new(
                1,
                vec![WalRecord::PutInline {
                    key: b"k1".to_vec(),
                    value: b"v1".to_vec(),
                }],
            ))
            .unwrap();

        // Batch 2 (LSN 2): multi-record batch where a Delete and PutInline accompany an unverified PutBlobPtr
        buf_w
            .append_batch(&WalBatch::new(
                2,
                vec![
                    WalRecord::Delete {
                        key: b"k1".to_vec(),
                    },
                    WalRecord::PutBlobPtr {
                        key: b"k2_blob".to_vec(),
                        blob_ptr: BlobPtr {
                            kind: BlobKind::Standard,
                            table_id: 1,
                            seg_id: 42,
                            offset: 0,
                            len: 2048,
                            crc32c: 0xABCD_EF01,
                        },
                    },
                    WalRecord::PutInline {
                        key: b"sec_idx_for_k2".to_vec(),
                        value: Vec::new(),
                    },
                ],
            ))
            .unwrap();

        drop(sync_w);
        drop(buf_w);

        let sync_f = fs
            .open_file(WAL_SYNC_FILE_NAME, false, false, OpenHint::MutableSyncLog)
            .unwrap();
        let buf_f = fs
            .open_file(
                WAL_BUF_FILE_NAME,
                false,
                false,
                OpenHint::MutableBufferedLog,
            )
            .unwrap();

        let res = recover_two_streams(&*sync_f, 1, &*buf_f, 1, 0, |_ptr| Ok(false))
            .expect("recover two streams");

        // Entire Batch 2 must be discarded so companion Delete("k1") and PutInline("sec_idx_for_k2") are not applied
        assert_eq!(res.merged_batches.len(), 1);
        assert_eq!(res.merged_batches[0].lsn, 1);
    }

    #[test]
    fn multi_chunk_transaction_rejected_atomically_when_any_chunk_blob_is_invalid() {
        let tmp = TempTestDir::new("multi_chunk_atomicity");
        let fs = OsFs::open(&tmp.path, true).expect("open OsFs");

        let wal_size = 64 * 1024u64;
        let sync_w = WalStreamWriter::open(
            &fs,
            WAL_SYNC_FILE_NAME,
            wal_size,
            1,
            WalStreamMode::SyncSectorIsolated,
        )
        .unwrap();
        let mut buf_w = WalStreamWriter::open(
            &fs,
            WAL_BUF_FILE_NAME,
            wal_size,
            1,
            WalStreamMode::BufferedImmediateOsCache,
        )
        .unwrap();

        // Chunk 1 (LSN 1): TxBegin + valid inline write
        buf_w
            .append_batch(&WalBatch::new(
                1,
                vec![
                    WalRecord::TxBegin,
                    WalRecord::PutInline {
                        key: b"first".to_vec(),
                        value: b"value".to_vec(),
                    },
                ],
            ))
            .unwrap();

        // Chunk 2 (LSN 2): PutBlobPtr (invalid/missing blob) + TxCommit
        buf_w
            .append_batch(&WalBatch::new(
                2,
                vec![
                    WalRecord::PutBlobPtr {
                        key: b"second".to_vec(),
                        blob_ptr: BlobPtr {
                            kind: BlobKind::Standard,
                            table_id: 1,
                            seg_id: 42,
                            offset: 0,
                            len: 2048,
                            crc32c: 0,
                        },
                    },
                    WalRecord::TxCommit,
                ],
            ))
            .unwrap();

        // Also verify a subsequent valid 2-chunk transaction (LSNs 3 and 4) IS recovered
        buf_w
            .append_batch(&WalBatch::new(
                3,
                vec![
                    WalRecord::TxBegin,
                    WalRecord::PutInline {
                        key: b"valid_tx_part1".to_vec(),
                        value: b"ok1".to_vec(),
                    },
                ],
            ))
            .unwrap();
        buf_w
            .append_batch(&WalBatch::new(
                4,
                vec![
                    WalRecord::PutBlobPtr {
                        key: b"valid_tx_part2".to_vec(),
                        blob_ptr: BlobPtr {
                            kind: BlobKind::Standard,
                            table_id: 1,
                            seg_id: 7,
                            offset: 0,
                            len: 2048,
                            crc32c: 0,
                        },
                    },
                    WalRecord::TxCommit,
                ],
            ))
            .unwrap();

        let recovered = recover_two_streams(
            &**sync_w.file(),
            1,
            &**buf_w.file(),
            1,
            0,
            |ptr| Ok(ptr.seg_id == 7),
        )
        .unwrap();

        let recovered_lsns: Vec<u64> = recovered.merged_batches.iter().map(|b| b.lsn).collect();
        assert!(
            recovered_lsns.is_empty(),
            "multi-chunk transaction [1, 2] with invalid blob in chunk 2 must be discarded as a unit,              and the buffered stream must not replay later transaction [3, 4] past it"
        );
        assert_eq!(recovered.max_recovered_lsn, 4);
    }

    #[test]
    fn buffered_stream_stops_at_first_invalid_blob_but_sync_stream_continues() {
        let tmp = TempTestDir::new("buf_prefix");
        let fs = OsFs::open(&tmp.path, true).expect("open OsFs");
        let wal_size = 64 * 1024u64;
        let mut sync_w = WalStreamWriter::open(
            &fs,
            WAL_SYNC_FILE_NAME,
            wal_size,
            1,
            WalStreamMode::SyncSectorIsolated,
        )
        .unwrap();
        let mut buf_w = WalStreamWriter::open(
            &fs,
            WAL_BUF_FILE_NAME,
            wal_size,
            1,
            WalStreamMode::BufferedImmediateOsCache,
        )
        .unwrap();
        let blob = |lsn: u64, seg_id: u64| {
            WalBatch::new(
                lsn,
                vec![WalRecord::PutBlobPtr {
                    key: format!("k{lsn}").into_bytes(),
                    blob_ptr: BlobPtr {
                        kind: BlobKind::Standard,
                        table_id: 1,
                        seg_id,
                        offset: 0,
                        len: 128,
                        crc32c: 0,
                    },
                }],
            )
        };
        // Segment 1 is valid; segment 2 is the blob lost by the simulated power failure.
        buf_w.append_batch(&blob(1, 1)).unwrap();
        sync_w.append_batch(&blob(2, 2)).unwrap();
        buf_w.append_batch(&blob(3, 2)).unwrap();
        sync_w.append_batch(&blob(4, 1)).unwrap();
        buf_w.append_batch(&blob(5, 1)).unwrap();

        let recovered = recover_two_streams(
            &**sync_w.file(),
            1,
            &**buf_w.file(),
            1,
            0,
            |ptr| Ok(ptr.seg_id == 1),
        )
        .unwrap();
        let lsns: Vec<u64> = recovered.merged_batches.iter().map(|b| b.lsn).collect();
        assert_eq!(lsns, vec![1, 4]);
        assert_eq!(recovered.max_recovered_lsn, 5);
    }

    // ---- Fault-injection helpers ----

    use std::sync::atomic::{AtomicBool, Ordering};

    struct FailFile {
        inner: Arc<<OsFs as PlatformFs>::File>,
        fail_write: Arc<AtomicBool>,
        fail_sync: Arc<AtomicBool>,
    }

    impl PlatformFile for FailFile {
        fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
            self.inner.read_at(buf, offset)
        }
        fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
            self.inner.read_exact_at(buf, offset)
        }
        fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
            if self.fail_write.swap(false, Ordering::SeqCst) {
                // Simulate a partial write: first sector lands, then error.
                let n = buf.len().min(WAL_SECTOR_SIZE);
                let _ = self.inner.write_all_at(&buf[..n], offset);
                return Err(io::Error::other("injected write failure"));
            }
            self.inner.write_all_at(buf, offset)
        }
        fn file_len(&self) -> io::Result<u64> {
            self.inner.file_len()
        }
        fn preallocate(&self, t: u64, s: PreallocStrategy) -> io::Result<()> {
            self.inner.preallocate(t, s)
        }
        fn sync_barrier(&self) -> io::Result<()> {
            if self.fail_sync.swap(false, Ordering::SeqCst) {
                return Err(io::Error::other("injected sync failure"));
            }
            self.inner.sync_barrier()
        }
        fn sync_durable(&self) -> io::Result<()> {
            self.inner.sync_durable()
        }
        fn evict_os_cache(&self, o: u64, l: u64) -> io::Result<()> {
            self.inner.evict_os_cache(o, l)
        }
    }

    struct FailFs {
        inner: OsFs,
        fail_write: Arc<AtomicBool>,
        fail_sync: Arc<AtomicBool>,
    }

    impl PlatformFs for FailFs {
        type File = FailFile;
        fn open_file(
            &self,
            rel_name: &str,
            create: bool,
            writable: bool,
            hint: OpenHint,
        ) -> io::Result<Arc<FailFile>> {
            Ok(Arc::new(FailFile {
                inner: self.inner.open_file(rel_name, create, writable, hint)?,
                fail_write: self.fail_write.clone(),
                fail_sync: self.fail_sync.clone(),
            }))
        }
        fn remove_file(&self, n: &str) -> io::Result<()> {
            self.inner.remove_file(n)
        }
        fn list_files(&self) -> io::Result<Vec<String>> {
            self.inner.list_files()
        }
        fn root_path(&self) -> &std::path::Path {
            self.inner.root_path()
        }
    }

    fn put_batch(lsn: u64, key: &[u8], len: usize) -> WalBatch {
        WalBatch::new(
            lsn,
            vec![WalRecord::PutInline {
                key: key.to_vec(),
                value: vec![0x42u8; len],
            }],
        )
    }

    fn fail_fs(name: &str) -> (TempTestDir, FailFs, Arc<AtomicBool>, Arc<AtomicBool>) {
        let tmp = TempTestDir::new(name);
        let w = Arc::new(AtomicBool::new(false));
        let s = Arc::new(AtomicBool::new(false));
        let fs = FailFs {
            inner: OsFs::open(&tmp.path, true).expect("open OsFs"),
            fail_write: w.clone(),
            fail_sync: s.clone(),
        };
        (tmp, fs, w, s)
    }

    fn recover_lsns(fs: &FailFs, name: &str, epoch: u64) -> Vec<u64> {
        let f = fs
            .open_file(name, false, false, OpenHint::MutableSyncLog)
            .unwrap();
        recover_single_wal_stream(&*f, epoch)
            .unwrap()
            .batches
            .iter()
            .map(|b| b.lsn)
            .collect()
    }

    #[test]
    fn sync_multi_sector_write_failure_leaves_no_hole() {
        let (_tmp, fs, fail_w, _) = fail_fs("wal_fail_sync_multi");
        let mut w = WalStreamWriter::open(
            &fs,
            WAL_SYNC_FILE_NAME,
            64 * 1024,
            1,
            WalStreamMode::SyncSectorIsolated,
        )
        .unwrap();
        w.append_batch(&put_batch(1, b"a", 10)).unwrap();
        let before = w.current_sector_idx();
        fail_w.store(true, Ordering::SeqCst);
        assert!(w.append_batch(&put_batch(2, b"big", 13_000)).is_err());
        assert_eq!(w.current_sector_idx(), before, "cursor must be restored");
        w.append_batch(&put_batch(3, b"c", 10)).unwrap();
        drop(w);
        assert_eq!(recover_lsns(&fs, WAL_SYNC_FILE_NAME, 1), vec![1, 3]);
    }

    #[test]
    fn sync_barrier_failure_restores_cursor() {
        let (_tmp, fs, _, fail_s) = fail_fs("wal_fail_barrier");
        let mut w = WalStreamWriter::open(
            &fs,
            WAL_SYNC_FILE_NAME,
            64 * 1024,
            1,
            WalStreamMode::SyncSectorIsolated,
        )
        .unwrap();
        w.append_batch(&put_batch(1, b"a", 10)).unwrap();
        let before = w.current_sector_idx();
        fail_s.store(true, Ordering::SeqCst);
        assert!(w.append_batch(&put_batch(2, b"b", 10)).is_err());
        assert_eq!(w.current_sector_idx(), before);
        w.append_batch(&put_batch(3, b"c", 10)).unwrap();
        drop(w);
        assert_eq!(recover_lsns(&fs, WAL_SYNC_FILE_NAME, 1), vec![1, 3]);
    }

    #[test]
    fn sealed_sector_survives_torn_rewrite_of_following_batch() {
        for mode in [
            WalStreamMode::BufferedImmediateOsCache,
            WalStreamMode::BufferedUserSpaceCoalesced,
        ] {
            let (_tmp, fs, _, _) = fail_fs("wal_seal_torn");
            let mut w =
                WalStreamWriter::open(&fs, WAL_BUF_FILE_NAME, 64 * 1024, 1, mode).unwrap();
            w.append_batch(&put_batch(1, b"a", 10)).unwrap();
            w.make_durable_and_seal(false).unwrap();
            assert_eq!(w.current_sector_idx(), 1);
            let out = w.append_batch(&put_batch(2, b"b", 10)).unwrap();
            assert_eq!(
                out,
                WalAppendOutcome::Appended {
                    first_sector_idx: 1,
                    sectors_touched: 1
                }
            );
            w.flush_buffered_to_os().unwrap();
            drop(w);
            assert_eq!(recover_lsns(&fs, WAL_BUF_FILE_NAME, 1), vec![1, 2]);

            // Tear the sector holding B: only A (sealed in sector 0) must remain.
            let f = fs
                .open_file(WAL_BUF_FILE_NAME, false, true, OpenHint::MutableBufferedLog)
                .unwrap();
            f.write_all_at(&[0xFFu8; 64], WAL_SECTOR_SIZE as u64 + 100)
                .unwrap();
            drop(f);
            assert_eq!(recover_lsns(&fs, WAL_BUF_FILE_NAME, 1), vec![1]);
        }
    }

    #[test]
    fn unsealed_buffered_sector_is_shared_between_batches() {
        let (_tmp, fs, _, _) = fail_fs("wal_unsealed_shared");
        let mut w = WalStreamWriter::open(
            &fs,
            WAL_BUF_FILE_NAME,
            64 * 1024,
            1,
            WalStreamMode::BufferedImmediateOsCache,
        )
        .unwrap();
        w.append_batch(&put_batch(1, b"a", 10)).unwrap();
        w.append_batch(&put_batch(2, b"b", 10)).unwrap();
        assert_eq!(w.current_sector_idx(), 0);
        // Sealing an empty open sector is a no-op.
        let mut w2 = WalStreamWriter::open(
            &fs,
            "wal_buf_other.db",
            64 * 1024,
            1,
            WalStreamMode::BufferedImmediateOsCache,
        )
        .unwrap();
        w2.seal_open_sector_for_durability().unwrap();
        assert_eq!(w2.current_sector_idx(), 0);
    }

    #[test]
    fn seal_at_end_of_ring_yields_ring_full() {
        let (_tmp, fs, _, _) = fail_fs("wal_seal_ring_end");
        let mut w = WalStreamWriter::open(
            &fs,
            WAL_BUF_FILE_NAME,
            WAL_SECTOR_SIZE as u64,
            1,
            WalStreamMode::BufferedImmediateOsCache,
        )
        .unwrap();
        w.append_batch(&put_batch(1, b"a", 10)).unwrap();
        w.make_durable_and_seal(true).unwrap();
        assert_eq!(w.current_sector_idx(), w.total_sectors());
        assert_eq!(
            w.append_batch(&put_batch(2, b"b", 10)).unwrap(),
            WalAppendOutcome::RingFull
        );
        drop(w);
        assert_eq!(recover_lsns(&fs, WAL_BUF_FILE_NAME, 1), vec![1]);
    }

    #[test]
    fn buffered_write_failure_does_not_resurrect_failed_batch() {
        let (_tmp, fs, fail_w, _) = fail_fs("wal_fail_buffered");
        let mut w = WalStreamWriter::open(
            &fs,
            WAL_BUF_FILE_NAME,
            64 * 1024,
            1,
            WalStreamMode::BufferedImmediateOsCache,
        )
        .unwrap();
        w.append_batch(&put_batch(1, b"a", 10)).unwrap();
        fail_w.store(true, Ordering::SeqCst);
        assert!(w.append_batch(&put_batch(2, b"failed", 10)).is_err());
        w.append_batch(&put_batch(3, b"c", 10)).unwrap();
        drop(w);
        assert_eq!(recover_lsns(&fs, WAL_BUF_FILE_NAME, 1), vec![1, 3]);
    }
}
