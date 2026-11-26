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
use crate::util::{io_err, io_err_with_tag};

mod recovery;
pub use self::recovery::{
    DiscardedTx, StreamCounts, TwoStreamRecoveryResult, WalDamage, WalDamageReason,
    WalRecoveryReport, recover_two_streams, recover_two_streams_optional_with_mode,
    recover_two_streams_with_mode,
};
pub(crate) use recovery::recover_two_streams_keyed;

/// Fixed filename for the synchronous device-flushed WAL stream.
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

/// On-disk WAL format version, stored in bytes `14..16` of every sector header (u16 LE).
///
/// It is checked only after the sector's epoch and CRC32C match, so sectors left over from
/// earlier epochs are still ignored silently. Version `0` is the old format (the reserved value).
pub const WAL_FORMAT_VERSION: u16 = 2;

/// Size of the batch header: `[lsn u64 | batch_crc32c u32 | record_count u32 | prev_lsn u64 |
/// durable_lsn u64]`, followed by the records.
pub const WAL_BATCH_HEADER_SIZE: usize = 32;

/// Offset of `record_count` in the batch header; also where the `batch_crc32c` range starts.
///
/// `batch_crc32c` covers `buf[WAL_BATCH_CRC_START..]`: `record_count`, `prev_lsn`, `durable_lsn`
/// and the records, as one contiguous range. (Before format version 1 it covered only the
/// records, `buf[16..]`, so a flipped `record_count` was caught only indirectly.) The LSN and
/// the CRC field itself lie outside the range.
const WAL_BATCH_CRC_START: usize = 12;

/// Maximum fragment payload bytes inside a single 4 KB sector (`4080` bytes).
pub const WAL_SECTOR_PAYLOAD_CAP: usize = WAL_SECTOR_SIZE - WAL_SECTOR_HEADER_SIZE;

/// Size of a fragment header (`[frag_type: u8 | frag_len: u16 LE]` = 3 bytes).
pub const FRAGMENT_HEADER_SIZE: usize = 3;

/// Maximum data bytes in a single fragment (`4077` bytes).
pub const MAX_FRAGMENT_DATA_LEN: usize = WAL_SECTOR_PAYLOAD_CAP - FRAGMENT_HEADER_SIZE;

/// Conservative usable WAL bytes per 4 KB sector leaving fragment-header headroom.
pub const EST_WAL_SECTOR_USABLE_BYTES: usize = 4070;

/// Wire operation tags for [`WalRecord`] entries inside an encoded [`WalBatch`].
pub const OP_PUT_INLINE: u8 = 1;
pub const OP_PUT_BLOB_PTR: u8 = 2;
pub const OP_DELETE: u8 = 3;
pub const OP_TX_BEGIN: u8 = 4;
pub const OP_TX_COMMIT: u8 = 5;
pub const OP_TX_CONTINUE: u8 = 6;

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
            other => Err(io_err_with_tag(
                io::ErrorKind::InvalidData,
                "invalid BlobKind discriminant",
                other,
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

impl BlobPtr {
    /// Fixed little-endian wire size of an encoded [`BlobPtr`] in bytes.
    pub const ENCODED_LEN: usize = 23;

    /// Serializes `self` into its fixed 23-byte little-endian wire representation.
    #[inline]
    pub fn to_bytes(&self) -> [u8; Self::ENCODED_LEN] {
        let mut out = [0u8; Self::ENCODED_LEN];
        out[0] = self.kind as u8;
        out[1..3].copy_from_slice(&self.table_id.to_le_bytes());
        out[3..11].copy_from_slice(&self.seg_id.to_le_bytes());
        out[11..15].copy_from_slice(&self.offset.to_le_bytes());
        out[15..19].copy_from_slice(&self.len.to_le_bytes());
        out[19..23].copy_from_slice(&self.crc32c.to_le_bytes());
        out
    }

    /// Appends the 23-byte little-endian wire encoding of `self` into `dst`.
    #[inline]
    pub fn encode_into(&self, dst: &mut Vec<u8>) {
        dst.extend_from_slice(&self.to_bytes());
    }

    /// Decodes a 23-byte [`BlobPtr`] from `r`.
    #[inline]
    pub fn decode_from(r: &mut ByteReader<'_>) -> io::Result<Self> {
        let kind = BlobKind::from_u8(r.read_u8()?)?;
        let table_id = r.read_u16_le()?;
        let seg_id = r.read_u64_le()?;
        let offset = r.read_u32_le()?;
        let len = r.read_u32_le()?;
        let crc32c = r.read_u32_le()?;
        Ok(Self {
            kind,
            table_id,
            seg_id,
            offset,
            len,
            crc32c,
        })
    }

    /// Decodes a [`BlobPtr`] from an exact 23-byte slice.
    #[inline]
    pub fn from_bytes(buf: &[u8]) -> io::Result<Self> {
        if buf.len() != Self::ENCODED_LEN {
            return Err(io_err(
                io::ErrorKind::InvalidData,
                "corrupted BlobPtr slice length",
            ));
        }
        let mut r = ByteReader::new(buf);
        Self::decode_from(&mut r)
    }
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
    /// Marks a middle chunk of a multi-chunk transaction, so that a batch without any marker
    /// is always a complete single-batch transaction.
    TxContinue,
}

impl WalRecord {
    /// Estimated encoded size of this record inside a WAL batch buffer (excluding blob payloads).
    #[inline]
    pub fn est_wire_bytes(&self) -> usize {
        match self {
            Self::PutInline { key, value } => 9 + key.len() + value.len(),
            Self::PutBlobPtr { key, .. } => 5 + BlobPtr::ENCODED_LEN + key.len(),
            Self::Delete { key } => 5 + key.len(),
            Self::TxBegin | Self::TxCommit | Self::TxContinue => 1,
        }
    }

    /// Estimated transaction memory/WAL footprint including out-of-line blob payloads.
    #[inline]
    pub fn est_tx_budget_bytes(&self) -> usize {
        match self {
            Self::PutBlobPtr { key, blob_ptr } => {
                5 + BlobPtr::ENCODED_LEN + key.len() + (blob_ptr.len as usize)
            }
            other => other.est_wire_bytes(),
        }
    }
}

/// Zero-allocation byte-span view of one [`WalRecord`] decoded from a [`ByteReader`].
enum BorrowedWalRecord {
    PutInline {
        key: std::ops::Range<usize>,
        value: std::ops::Range<usize>,
    },
    PutBlobPtr {
        key: std::ops::Range<usize>,
        blob_ptr: BlobPtr,
    },
    Delete {
        key: std::ops::Range<usize>,
    },
    TxBegin,
    TxCommit,
    TxContinue,
}

impl BorrowedWalRecord {
    fn decode_from(r: &mut ByteReader<'_>) -> io::Result<Self> {
        let op = r.read_u8()?;
        match op {
            OP_PUT_INLINE => {
                let klen = r.read_u32_le()? as usize;
                let key = r.read_range(klen)?;
                let vlen = r.read_u32_le()? as usize;
                let value = r.read_range(vlen)?;
                Ok(Self::PutInline { key, value })
            }
            OP_PUT_BLOB_PTR => {
                let klen = r.read_u32_le()? as usize;
                let key = r.read_range(klen)?;
                let blob_ptr = BlobPtr::decode_from(r)?;
                Ok(Self::PutBlobPtr { key, blob_ptr })
            }
            OP_DELETE => {
                let klen = r.read_u32_le()? as usize;
                let key = r.read_range(klen)?;
                Ok(Self::Delete { key })
            }
            OP_TX_BEGIN => Ok(Self::TxBegin),
            OP_TX_COMMIT => Ok(Self::TxCommit),
            OP_TX_CONTINUE => Ok(Self::TxContinue),
            other => Err(io_err_with_tag(
                io::ErrorKind::InvalidData,
                "unknown WalRecord op_type",
                other,
            )),
        }
    }

    fn into_owned(self, data: &[u8]) -> WalRecord {
        match self {
            Self::PutInline { key, value } => WalRecord::PutInline {
                key: data[key].to_vec(),
                value: data[value].to_vec(),
            },
            Self::PutBlobPtr { key, blob_ptr } => WalRecord::PutBlobPtr {
                key: data[key].to_vec(),
                blob_ptr,
            },
            Self::Delete { key } => WalRecord::Delete {
                key: data[key].to_vec(),
            },
            Self::TxBegin => WalRecord::TxBegin,
            Self::TxCommit => WalRecord::TxCommit,
            Self::TxContinue => WalRecord::TxContinue,
        }
    }
}

/// An atomic group of [`WalRecord`] mutations assigned a single monotonically increasing `lsn`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalBatch {
    pub lsn: u64,
    /// LSN of the batch appended immediately before this one (`0` if none or not yet tracked).
    pub prev_lsn: u64,
    /// Highest LSN known durable when this batch was appended (`0` if none or not yet tracked).
    pub durable_lsn: u64,
    pub records: Vec<WalRecord>,
}

impl WalBatch {
    pub fn new(lsn: u64, records: Vec<WalRecord>) -> Self {
        Self {
            lsn,
            prev_lsn: 0,
            durable_lsn: 0,
            records,
        }
    }

    fn encode_single_record_into(rec: &WalRecord, buf: &mut Vec<u8>) {
        match rec {
            WalRecord::PutInline { key, value } => {
                buf.push(OP_PUT_INLINE);
                buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
                buf.extend_from_slice(key);
                buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
                buf.extend_from_slice(value);
            }
            WalRecord::PutBlobPtr { key, blob_ptr } => {
                buf.push(OP_PUT_BLOB_PTR);
                buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
                buf.extend_from_slice(key);
                blob_ptr.encode_into(buf);
            }
            WalRecord::Delete { key } => {
                buf.push(OP_DELETE);
                buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
                buf.extend_from_slice(key);
            }
            WalRecord::TxBegin => buf.push(OP_TX_BEGIN),
            WalRecord::TxCommit => buf.push(OP_TX_COMMIT),
            WalRecord::TxContinue => buf.push(OP_TX_CONTINUE),
        }
    }

    /// Appends serialized record bytes for `records` into `buf`, validating 32-bit length bounds.
    pub fn try_append_records_to_buf(records: &[WalRecord], buf: &mut Vec<u8>) -> io::Result<()> {
        let mut extra = 0usize;
        for rec in records {
            let rec_bytes = match rec {
                WalRecord::PutInline { key, value } => {
                    if u32::try_from(key.len()).is_err() || u32::try_from(value.len()).is_err() {
                        return Err(io_err(
                            io::ErrorKind::InvalidInput,
                            "WAL PutInline key or value exceeds u32::MAX bytes",
                        ));
                    }
                    9usize
                        .checked_add(key.len())
                        .and_then(|n| n.checked_add(value.len()))
                }
                WalRecord::PutBlobPtr { key, .. } => {
                    if u32::try_from(key.len()).is_err() {
                        return Err(io_err(
                            io::ErrorKind::InvalidInput,
                            "WAL PutBlobPtr key exceeds u32::MAX bytes",
                        ));
                    }
                    28usize.checked_add(key.len())
                }
                WalRecord::Delete { key } => {
                    if u32::try_from(key.len()).is_err() {
                        return Err(io_err(
                            io::ErrorKind::InvalidInput,
                            "WAL Delete key exceeds u32::MAX bytes",
                        ));
                    }
                    5usize.checked_add(key.len())
                }
                WalRecord::TxBegin | WalRecord::TxCommit | WalRecord::TxContinue => Some(1),
            }
            .ok_or_else(|| io_err(io::ErrorKind::InvalidInput, "WAL record size overflow"))?;
            extra = extra.checked_add(rec_bytes).ok_or_else(|| {
                io_err(io::ErrorKind::InvalidInput, "WAL batch byte size overflow")
            })?;
        }
        buf.try_reserve(extra).map_err(|_| {
            io_err(
                io::ErrorKind::OutOfMemory,
                "failed to reserve buffer for WAL records",
            )
        })?;
        for rec in records {
            Self::encode_single_record_into(rec, buf);
        }
        Ok(())
    }

    /// Appends serialized record bytes for `records` into `buf`.
    pub fn append_records_to_buf(records: &[WalRecord], buf: &mut Vec<u8>) {
        Self::try_append_records_to_buf(records, buf)
            .expect("WAL batch exceeds 32-bit length limits");
    }

    /// Fills in the 32-byte `[lsn (8B) | batch_crc32c (4B) | record_count (4B) | prev_lsn (8B) |
    /// durable_lsn (8B)]` header at `buf[0..WAL_BATCH_HEADER_SIZE]`.
    pub fn finalize_encoded_buf(
        lsn: u64,
        prev_lsn: u64,
        durable_lsn: u64,
        record_count: u32,
        buf: &mut [u8],
    ) {
        buf[0..8].copy_from_slice(&lsn.to_le_bytes());
        buf[12..16].copy_from_slice(&record_count.to_le_bytes());
        buf[16..24].copy_from_slice(&prev_lsn.to_le_bytes());
        buf[24..32].copy_from_slice(&durable_lsn.to_le_bytes());
        let batch_crc = crc32c(&buf[WAL_BATCH_CRC_START..]);
        buf[8..12].copy_from_slice(&batch_crc.to_le_bytes());
    }

    /// Partitions an encoded pending batch buffer (starting after the 32-byte header) into
    /// `(start, end, record_count)` byte spans each fitting within `max_chunk_bytes` once framed
    /// with a 1-byte transaction marker (`TxBegin` / `TxContinue` / `TxCommit`).
    pub fn plan_encoded_chunks(raw_buf: &[u8], max_chunk_bytes: usize) -> Vec<(usize, usize, u32)> {
        let mut cursor = WAL_BATCH_HEADER_SIZE;
        let total_len = raw_buf.len();
        let mut chunk_spans = Vec::new();
        let mut cur_start = cursor;
        let mut cur_records = 0u32;

        while cursor < total_len {
            let rec_start = cursor;
            let op = raw_buf[cursor];
            cursor += 1;
            match op {
                OP_PUT_INLINE => {
                    let klen = u32::from_le_bytes(raw_buf[cursor..cursor + 4].try_into().unwrap())
                        as usize;
                    cursor += 4 + klen;
                    let vlen = u32::from_le_bytes(raw_buf[cursor..cursor + 4].try_into().unwrap())
                        as usize;
                    cursor += 4 + vlen;
                }
                OP_PUT_BLOB_PTR => {
                    let klen = u32::from_le_bytes(raw_buf[cursor..cursor + 4].try_into().unwrap())
                        as usize;
                    cursor += 4 + klen + BlobPtr::ENCODED_LEN;
                }
                OP_DELETE => {
                    let klen = u32::from_le_bytes(raw_buf[cursor..cursor + 4].try_into().unwrap())
                        as usize;
                    cursor += 4 + klen;
                }
                OP_TX_BEGIN..=OP_TX_CONTINUE => {}
                _ => unreachable!("invalid op in pending WAL batch buffer"),
            }
            if cur_records > 0 && WAL_BATCH_HEADER_SIZE + 1 + (cursor - cur_start) > max_chunk_bytes
            {
                chunk_spans.push((cur_start, rec_start, cur_records));
                cur_start = rec_start;
                cur_records = 0;
            }
            cur_records += 1;
        }
        if cur_records > 0 {
            chunk_spans.push((cur_start, total_len, cur_records));
        }
        chunk_spans
    }

    /// Serializes this batch into the 32-byte header followed by the records, returning an error
    /// if counts or lengths exceed 32-bit wire limits or buffer allocation fails.
    pub fn try_encode_into(&self, out: &mut Vec<u8>) -> io::Result<()> {
        let record_count = u32::try_from(self.records.len()).map_err(|_| {
            io_err(
                io::ErrorKind::InvalidInput,
                "WAL batch record count exceeds u32::MAX",
            )
        })?;
        out.clear();
        out.try_reserve(WAL_BATCH_HEADER_SIZE).map_err(|_| {
            io_err(
                io::ErrorKind::OutOfMemory,
                "failed to reserve buffer for WAL batch header",
            )
        })?;
        out.extend_from_slice(&[0u8; WAL_BATCH_HEADER_SIZE]);
        Self::try_append_records_to_buf(&self.records, out)?;
        Self::finalize_encoded_buf(self.lsn, self.prev_lsn, self.durable_lsn, record_count, out);
        Ok(())
    }

    /// Serializes this batch into the 32-byte header followed by the records,
    /// reusing an existing buffer `out` without intermediate heap allocations.
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        self.try_encode_into(out)
            .expect("WAL batch exceeds 32-bit length limits");
    }

    /// Serializes this batch into the 32-byte header followed by the records
    /// in a single allocation without intermediate copying.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(64);
        self.encode_into(&mut out);
        out
    }

    fn parse_header_and_verify_crc(
        data: &[u8],
    ) -> io::Result<(u64, usize, u64, u64, ByteReader<'_>)> {
        let mut r = ByteReader::with_eof_msg(data, "truncated WalBatch payload");

        let lsn = r.read_u64_le()?;
        let expected_crc = r.read_u32_le()?;
        // The CRC range starts at `record_count`, so verify it before trusting any later field.
        if crc32c(r.remaining_slice()) != expected_crc {
            return Err(io_err(
                io::ErrorKind::InvalidData,
                "WalBatch CRC32C mismatch",
            ));
        }
        let record_count = r.read_u32_le()? as usize;
        let prev_lsn = r.read_u64_le()?;
        let durable_lsn = r.read_u64_le()?;

        // Minimum serialized WalRecord is 1 byte (1B op for a marker).
        if record_count > r.remaining() {
            return Err(io_err(
                io::ErrorKind::InvalidData,
                "WalBatch record_count exceeds remaining payload bytes",
            ));
        }
        Ok((lsn, record_count, prev_lsn, durable_lsn, r))
    }

    /// Decodes a serialized [`WalBatch`] and verifies its `batch_crc32c`.
    pub fn decode(data: &[u8]) -> io::Result<Self> {
        let (lsn, record_count, prev_lsn, durable_lsn, mut r) =
            Self::parse_header_and_verify_crc(data)?;

        let mut records = Vec::new();
        records.try_reserve_exact(record_count).map_err(|_| {
            io_err(
                io::ErrorKind::InvalidData,
                "WalBatch records allocation overflow",
            )
        })?;
        for _ in 0..record_count {
            records.push(BorrowedWalRecord::decode_from(&mut r)?.into_owned(data));
        }

        if !r.is_empty() {
            return Err(io_err(
                io::ErrorKind::InvalidData,
                "unexpected trailing bytes in WalBatch",
            ));
        }

        Ok(Self {
            lsn,
            prev_lsn,
            durable_lsn,
            records,
        })
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

#[inline]
fn sector_byte_offset(sector_idx: usize) -> io::Result<u64> {
    (sector_idx as u64)
        .checked_mul(WAL_SECTOR_SIZE as u64)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "WAL sector offset overflow"))
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
    /// High-water mark of non-zero bytes written to `sector_buf` (`sector_buf[sector_dirty_end..]`
    /// is guaranteed all zeroes).
    sector_dirty_end: usize,
    /// Scratch buffer for the currently active 4 KB sector.
    sector_buf: [u8; WAL_SECTOR_SIZE],
    /// Backup buffer for rolling back `sector_buf` on I/O error without per-append heap allocation.
    backup_sector_buf: Box<[u8; WAL_SECTOR_SIZE]>,
    /// Scratch buffer for `append_batch` serialization without per-call heap allocation.
    scratch_encode_buf: Vec<u8>,
    /// Contiguous staging buffer for coalescing multi-sector batch writes into a single `write_all_at` syscall.
    multi_sector_buf: Vec<u8>,
    /// When set, a synchronous append leaves the device barrier to the caller.
    defer_barrier: bool,
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
        let mut writer = Self::open_unallocated(fs, file_name, target_size, active_epoch, mode)?;
        writer.allocate_to_target()?;
        Ok(writer)
    }

    fn validate_wal_target_size(target_size: u64) -> io::Result<usize> {
        if target_size < WAL_SECTOR_SIZE as u64
            || target_size > i64::MAX as u64
            || !target_size.is_multiple_of(WAL_SECTOR_SIZE as u64)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "WAL size must be a positive multiple of 4096 bytes within i64::MAX",
            ));
        }
        usize::try_from(target_size / WAL_SECTOR_SIZE as u64)
            .ok()
            .filter(|&n| n <= usize::MAX / 2)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "WAL sector count exceeds addressable limit",
                )
            })
    }

    fn init_with_file(
        file: Arc<F>,
        mode: WalStreamMode,
        total_sectors: usize,
        active_epoch: u64,
    ) -> Self {
        Self {
            file,
            mode,
            total_sectors,
            active_epoch,
            current_sector_idx: 0,
            current_valid_bytes: 0,
            dirty_in_user_space: false,
            sector_dirty_end: 0,
            sector_buf: [0u8; WAL_SECTOR_SIZE],
            backup_sector_buf: Box::new([0u8; WAL_SECTOR_SIZE]),
            scratch_encode_buf: Vec::with_capacity(512),
            multi_sector_buf: Vec::new(),
            defer_barrier: false,
        }
    }

    fn open_hint(mode: WalStreamMode) -> OpenHint {
        match mode {
            WalStreamMode::SyncSectorIsolated => OpenHint::MutableSyncLog,
            WalStreamMode::BufferedImmediateOsCache | WalStreamMode::BufferedUserSpaceCoalesced => {
                OpenHint::MutableBufferedLog
            }
        }
    }

    /// Opens an existing WAL stream without creating or preallocating a missing file. Returns
    /// `Ok(None)` when `file_name` does not exist so `WalDamagePolicy::Fail` recovery leaves the
    /// directory untouched.
    pub fn open_existing_unallocated(
        fs: &impl PlatformFs<File = F>,
        file_name: &str,
        target_size: u64,
        active_epoch: u64,
        mode: WalStreamMode,
    ) -> io::Result<Option<Self>> {
        let total_sectors = Self::validate_wal_target_size(target_size)?;
        let hint = Self::open_hint(mode);
        match fs.open_file(file_name, false, true, hint) {
            Ok(file) => {
                let file_sectors = usize::try_from(file.file_len()? / WAL_SECTOR_SIZE as u64)
                    .map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "WAL file sector count exceeds usize::MAX",
                        )
                    })?;
                Ok(Some(Self::init_with_file(
                    file,
                    mode,
                    total_sectors.max(file_sectors),
                    active_epoch,
                )))
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Opens the stream like [`Self::open`] but leaves a file that is shorter than `target_size`
    /// as it is, so that recovery can read it and a refused open changes nothing. The caller must
    /// call [`Self::allocate_to_target`] before the first append.
    pub fn open_unallocated(
        fs: &impl PlatformFs<File = F>,
        file_name: &str,
        target_size: u64,
        active_epoch: u64,
        mode: WalStreamMode,
    ) -> io::Result<Self> {
        let total_sectors = Self::validate_wal_target_size(target_size)?;
        let hint = Self::open_hint(mode);
        let file = match fs.open_file(file_name, false, true, hint) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                fs.open_file(file_name, true, true, hint)?
            }
            Err(e) => return Err(e),
        };
        let file_sectors =
            usize::try_from(file.file_len()? / WAL_SECTOR_SIZE as u64).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "WAL file sector count exceeds usize::MAX",
                )
            })?;
        Ok(Self::init_with_file(
            file,
            mode,
            total_sectors.max(file_sectors),
            active_epoch,
        ))
    }

    /// Lengthens the file to the size the stream was opened with, if it is shorter.
    pub fn allocate_to_target(&mut self) -> io::Result<()> {
        let target = sector_byte_offset(self.total_sectors)?;
        if self.file.file_len()? < target {
            let prealloc = match self.mode {
                WalStreamMode::SyncSectorIsolated => PreallocStrategy::PhysicalZeroFill,
                WalStreamMode::BufferedImmediateOsCache
                | WalStreamMode::BufferedUserSpaceCoalesced => PreallocStrategy::LogicalExtendOnly,
            };
            self.file.preallocate(target, prealloc)?;
        }
        Ok(())
    }

    #[inline]
    fn clear_sector_buf(&mut self) {
        if self.sector_dirty_end > 0 {
            self.sector_buf[..self.sector_dirty_end].fill(0);
            self.sector_dirty_end = 0;
        }
    }

    #[inline]
    fn advance_and_clear_sector(&mut self) {
        self.current_sector_idx += 1;
        self.current_valid_bytes = 0;
        self.dirty_in_user_space = false;
        self.clear_sector_buf();
    }

    /// Positions the writer cursor right after the last valid sector recovered during startup.
    /// If `next_sector_idx >= self.total_sectors`, appends will report `RingFull` until grown with
    /// [`Self::ensure_min_size`] or rotated to a new epoch (never rewound onto live sectors).
    pub fn set_cursor_after_recovery(&mut self, next_sector_idx: usize) {
        self.current_sector_idx = next_sector_idx;
        self.current_valid_bytes = 0;
        self.dirty_in_user_space = false;
        self.clear_sector_buf();
    }

    /// Rotates the WAL stream to a new `WalEpoch` in $O(1)$ time without truncating or zero-filling
    /// the underlying file on disk.
    pub fn reset_for_new_epoch(&mut self, new_epoch: u64) -> io::Result<()> {
        if new_epoch <= self.active_epoch {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "new WAL epoch ({new_epoch}) must be strictly greater than current ({})",
                    self.active_epoch
                ),
            ));
        }
        self.active_epoch = new_epoch;
        self.current_sector_idx = 0;
        self.current_valid_bytes = 0;
        self.dirty_in_user_space = false;
        self.clear_sector_buf();
        Ok(())
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
        if target_size > i64::MAX as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "WAL target size exceeds i64::MAX",
            ));
        }
        let new_sectors_u64 = target_size.div_ceil(WAL_SECTOR_SIZE as u64);
        let aligned = new_sectors_u64
            .checked_mul(WAL_SECTOR_SIZE as u64)
            .filter(|&bytes| bytes <= i64::MAX as u64)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "WAL target size overflow")
            })?;
        let new_sectors = usize::try_from(new_sectors_u64)
            .ok()
            .filter(|&n| n <= usize::MAX / 2)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "WAL sector count exceeds addressable limit",
                )
            })?;
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
        let encode_res = batch.try_encode_into(&mut raw);
        let res = match encode_res {
            Ok(()) => self.append_raw_batch(&raw),
            Err(e) => Err(e),
        };
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
    ///
    /// A failed write can also tear the open buffered sector that earlier batches share, so
    /// the error path rewrites that sector's last good image (best effort).
    pub fn append_raw_batch(&mut self, remaining: &[u8]) -> io::Result<WalAppendOutcome> {
        let saved_idx = self.current_sector_idx;
        let saved_valid = self.current_valid_bytes;
        let saved_dirty = self.dirty_in_user_space;
        // Coalesced buffered appends that fit inside the current sector with room for another
        // fragment header stay strictly in RAM (zero I/O), so they cannot fail or tear.
        let infallible_coalesced = self.mode == WalStreamMode::BufferedUserSpaceCoalesced
            && !remaining.is_empty()
            && saved_idx < self.total_sectors
            && saved_valid
                .checked_add(FRAGMENT_HEADER_SIZE)
                .and_then(|v| v.checked_add(remaining.len()))
                .and_then(|v| v.checked_add(FRAGMENT_HEADER_SIZE))
                .is_some_and(|total| total <= WAL_SECTOR_PAYLOAD_CAP);
        let saved_end = WAL_SECTOR_HEADER_SIZE + saved_valid;
        if saved_valid > 0 && !infallible_coalesced {
            self.backup_sector_buf[WAL_SECTOR_HEADER_SIZE..saved_end]
                .copy_from_slice(&self.sector_buf[WAL_SECTOR_HEADER_SIZE..saved_end]);
        }
        let res = self.append_raw_batch_inner(remaining);
        if res.is_err() {
            self.current_sector_idx = saved_idx;
            self.current_valid_bytes = saved_valid;
            self.dirty_in_user_space = saved_dirty;
            if saved_valid > 0 {
                self.sector_buf[WAL_SECTOR_HEADER_SIZE..saved_end]
                    .copy_from_slice(&self.backup_sector_buf[WAL_SECTOR_HEADER_SIZE..saved_end]);
                self.sector_buf[saved_end..WAL_SECTOR_SIZE].fill(0);
                self.sector_dirty_end = saved_end;
            } else {
                self.sector_buf.fill(0);
                self.sector_dirty_end = 0;
            }
            self.multi_sector_buf.clear();
            if saved_valid > 0 {
                // The failed write may have left a torn copy of the open sector in the file,
                // which would invalidate its CRC and destroy the batches packed before this
                // one. Put the last good image back, best effort: if this write fails too the
                // caller already treats the stream as poisoned. Only the error path pays.
                let _ = self.seal_and_write_current_sector();
            }
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
        if self.preview_end_sector(remaining.len()).is_none() {
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
            let write_end = write_pos + FRAGMENT_HEADER_SIZE + chunk_len;
            self.sector_buf[write_pos] = frag_type as u8;
            self.sector_buf[write_pos + 1..write_pos + 3]
                .copy_from_slice(&(chunk_len as u16).to_le_bytes());
            self.sector_buf[write_pos + 3..write_end].copy_from_slice(&remaining[..chunk_len]);
            self.sector_dirty_end = self.sector_dirty_end.max(write_end);

            self.current_valid_bytes += FRAGMENT_HEADER_SIZE + chunk_len;
            remaining = &remaining[chunk_len..];
            is_first_piece = false;
            sectors_touched += 1;

            match self.mode {
                WalStreamMode::SyncSectorIsolated => {
                    self.seal_current_sector_in_memory();
                    if staged_start_sector.is_none() && remaining.is_empty() {
                        let offset = sector_byte_offset(self.current_sector_idx)?;
                        self.file.write_all_at(&self.sector_buf, offset)?;
                    } else {
                        if staged_start_sector.is_none() {
                            staged_start_sector = Some(self.current_sector_idx);
                        }
                        self.multi_sector_buf.extend_from_slice(&self.sector_buf);
                    }
                    self.advance_and_clear_sector();
                }
                WalStreamMode::BufferedImmediateOsCache => {
                    if !remaining.is_empty()
                        || WAL_SECTOR_PAYLOAD_CAP - self.current_valid_bytes < FRAGMENT_HEADER_SIZE
                    {
                        self.seal_current_sector_in_memory();
                        if staged_start_sector.is_none() && remaining.is_empty() {
                            let offset = sector_byte_offset(self.current_sector_idx)?;
                            self.file.write_all_at(&self.sector_buf, offset)?;
                        } else {
                            if staged_start_sector.is_none() {
                                staged_start_sector = Some(self.current_sector_idx);
                            }
                            self.multi_sector_buf.extend_from_slice(&self.sector_buf);
                        }
                        self.advance_and_clear_sector();
                    } else {
                        // Write the updated 4 KB sector into the OS Page Cache immediately
                        // while keeping the sector open for further dense packing.
                        self.seal_current_sector_in_memory();
                        if staged_start_sector.is_some() {
                            self.multi_sector_buf.extend_from_slice(&self.sector_buf);
                        } else {
                            let offset = sector_byte_offset(self.current_sector_idx)?;
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
                            let offset = sector_byte_offset(self.current_sector_idx)?;
                            self.file.write_all_at(&self.sector_buf, offset)?;
                        } else {
                            if staged_start_sector.is_none() {
                                staged_start_sector = Some(self.current_sector_idx);
                            }
                            self.multi_sector_buf.extend_from_slice(&self.sector_buf);
                        }
                        self.advance_and_clear_sector();
                    } else {
                        self.dirty_in_user_space = true;
                    }
                }
            }
        }

        if let Some(start_sec) = staged_start_sector {
            let offset = sector_byte_offset(start_sec)?;
            self.file.write_all_at(&self.multi_sector_buf, offset)?;
            self.multi_sector_buf.clear();
        }

        if self.mode == WalStreamMode::SyncSectorIsolated && !self.defer_barrier {
            self.file.sync_barrier()?;
        }

        Ok(WalAppendOutcome::Appended {
            first_sector_idx,
            sectors_touched,
        })
    }

    /// Makes synchronous appends skip the device barrier until cleared. The caller then owes
    /// a `sync_barrier` on [`Self::file_handle`] before treating the appends as durable.
    pub(crate) fn set_defer_barrier(&mut self, defer: bool) {
        self.defer_barrier = defer;
    }

    /// Whether synchronous appends currently leave the device barrier to the caller.
    pub(crate) fn barrier_deferred(&self) -> bool {
        self.defer_barrier
    }

    /// Returns a shared handle to the log file, usable without access to this writer.
    pub(crate) fn file_handle(&self) -> Arc<F> {
        Arc::clone(&self.file)
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
        if self.dirty_in_user_space {
            self.seal_and_write_current_sector()?;
        }
        // In `BufferedImmediateOsCache` (and after `flush_buffered_to_os` in
        // `BufferedUserSpaceCoalesced`) the open sector was already written to the OS page cache.
        self.advance_and_clear_sector();
        Ok(())
    }

    /// Flushes pending bytes to the OS, seals the open sector (see
    /// [`Self::seal_open_sector_for_durability`]) and then syncs the file: with `sync_durable`
    /// when `durable` is true, otherwise with `sync_barrier`.
    pub fn make_durable_and_seal(&mut self, durable: bool) -> io::Result<()> {
        // `seal_open_sector_for_durability` writes any `dirty_in_user_space` bytes once before
        // advancing, avoiding double CRC calculation and duplicate sector writes.
        self.seal_open_sector_for_durability()?;
        if durable {
            self.file.sync_durable()
        } else {
            self.file.sync_barrier()
        }
    }

    fn preview_end_sector(&self, mut rem_len: usize) -> Option<usize> {
        let mut sec_idx = self.current_sector_idx;
        let mut valid = self.current_valid_bytes;

        if valid > 0 && WAL_SECTOR_PAYLOAD_CAP - valid < FRAGMENT_HEADER_SIZE {
            sec_idx = sec_idx.checked_add(1)?;
            valid = 0;
        }

        while rem_len > 0 {
            if sec_idx >= self.total_sectors {
                return None;
            }
            let max_data = (WAL_SECTOR_PAYLOAD_CAP - valid) - FRAGMENT_HEADER_SIZE;
            let take = rem_len.min(max_data);
            rem_len -= take;
            valid += FRAGMENT_HEADER_SIZE + take;

            if self.mode == WalStreamMode::SyncSectorIsolated
                || rem_len > 0
                || WAL_SECTOR_PAYLOAD_CAP - valid < FRAGMENT_HEADER_SIZE
            {
                sec_idx = sec_idx.checked_add(1)?;
                valid = 0;
            }
        }

        // If `valid > 0` in buffered mode, the active sector at `sec_idx` must be < total_sectors.
        let end = if valid > 0 {
            sec_idx.checked_add(1)?
        } else {
            sec_idx
        };
        (end <= self.total_sectors).then_some(end)
    }

    fn seal_current_sector_in_memory(&mut self) {
        // Zero out any previously dirtied trailing padding bytes in `16 + current_valid_bytes .. 4096`.
        let pad_start = WAL_SECTOR_HEADER_SIZE + self.current_valid_bytes;
        if self.sector_dirty_end > pad_start {
            self.sector_buf[pad_start..self.sector_dirty_end].fill(0);
            self.sector_dirty_end = pad_start;
        }
        self.sector_dirty_end = self.sector_dirty_end.max(WAL_SECTOR_HEADER_SIZE);
        debug_assert!(
            self.sector_buf[pad_start..WAL_SECTOR_SIZE]
                .iter()
                .all(|&b| b == 0),
            "trailing WAL sector padding must be zero"
        );

        self.sector_buf[4..12].copy_from_slice(&self.active_epoch.to_le_bytes());
        self.sector_buf[12..14].copy_from_slice(&(self.current_valid_bytes as u16).to_le_bytes());
        self.sector_buf[14..16].copy_from_slice(&WAL_FORMAT_VERSION.to_le_bytes());

        let sector_crc = crc32c(&self.sector_buf[4..WAL_SECTOR_SIZE]);
        self.sector_buf[0..4].copy_from_slice(&sector_crc.to_le_bytes());
    }

    fn seal_and_write_current_sector(&mut self) -> io::Result<()> {
        self.seal_current_sector_in_memory();
        let offset = sector_byte_offset(self.current_sector_idx)?;
        self.file.write_all_at(&self.sector_buf, offset)
    }

    fn flush_and_advance_sector(&mut self) -> io::Result<()> {
        if self.current_valid_bytes > 0 {
            self.seal_and_write_current_sector()?;
            self.advance_and_clear_sector();
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
#[derive(Debug, Clone, Default)]
pub struct SingleStreamRecovery {
    /// Reassembled, CRC-verified batches of the valid prefix, in sequential order.
    pub batches: Vec<WalBatch>,
    /// Index of the first sector after the valid log prefix (where new writes may resume).
    pub next_sector_idx: usize,
    /// Intact batches found in current-epoch sectors after the first sector the scan could not
    /// use, up to the end of the log (a never-written or intact older-epoch sector, after which
    /// nothing is read). They are never replayed: a batch before them may be lost. Their
    /// `prev_lsn` links and `durable_lsn` values still bound the lost range (ARCHITECTURE.md
    /// 4.6.4).
    pub(crate) past_stop: Vec<WalBatch>,
    /// Current-epoch sectors that failed their CRC32C or whose contents did not parse, up to the
    /// end of the log (a never-written or intact older-epoch sector, which stops the scan).
    /// After a power cut these are torn writes of the unsynced tail.
    pub(crate) corrupt_sectors: usize,
    /// The first current-epoch sector with a valid CRC but a format version other than
    /// [`WAL_FORMAT_VERSION`].
    pub(crate) unsupported_format: Option<UnsupportedWalFormat>,
}

/// A sector that is intact (current epoch, valid CRC) but written in another WAL format version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct UnsupportedWalFormat {
    pub sector_idx: usize,
    pub version: u16,
}

/// What [`scan_sector`] made of one sector.
enum SectorScan {
    /// The sector parsed completely.
    Usable,
    /// The ordinary end of the log: a sector that was never written (all zeros), or an intact
    /// sector (valid CRC32C) of an older epoch.
    EndOfLog,
    /// Its epoch is not the expected one, and it is not an intact older sector: a newer epoch,
    /// or an older one whose CRC32C fails (a torn write).
    OtherEpoch,
    /// The CRC32C failed, or the contents did not parse (batches found before the bad spot in
    /// the same sector were still delivered).
    Corrupt,
    /// Current epoch and valid CRC32C, but another format version.
    OtherVersion(u16),
}

/// Parses one sector, appending the batches that end in it to `out`.
///
/// `partial` carries a batch whose first fragments lie in earlier sectors. With `lenient` set
/// (after the scan has already stopped once) a continuation fragment without its start is
/// skipped instead of being corruption: its batch began in a sector that was lost.
///
/// When `verify_other_epoch_crc` is `false` (used by [`WalScanMode::FullScan`], which continues
/// scanning after any non-current sector regardless of whether it was intact or torn), mismatched-
/// epoch and all-zero older-epoch sectors return [`SectorScan::OtherEpoch`] immediately without a
/// 4 KB zero scan or CRC32C calculation.
fn scan_sector(
    sector: &[u8],
    expected_epoch: u64,
    lenient: bool,
    verify_other_epoch_crc: bool,
    partial: &mut Option<Vec<u8>>,
    out: &mut Vec<WalBatch>,
) -> SectorScan {
    let sector_epoch = u64::from_le_bytes(sector[4..12].try_into().unwrap());
    let expected_crc = u32::from_le_bytes(sector[0..4].try_into().unwrap());
    if !verify_other_epoch_crc {
        if sector_epoch != expected_epoch {
            return SectorScan::OtherEpoch;
        }
        if expected_epoch == 0 && sector.iter().all(|&b| b == 0) {
            return SectorScan::EndOfLog;
        }
    } else {
        if sector_epoch == 0 && sector.iter().all(|&b| b == 0) {
            return SectorScan::EndOfLog;
        }
        if sector_epoch != expected_epoch {
            let intact = sector_epoch < expected_epoch
                && crc32c(&sector[4..WAL_SECTOR_SIZE]) == expected_crc;
            return if intact {
                SectorScan::EndOfLog
            } else {
                SectorScan::OtherEpoch
            };
        }
    }
    if crc32c(&sector[4..WAL_SECTOR_SIZE]) != expected_crc {
        return SectorScan::Corrupt;
    }
    // Checked only now: a sector from an earlier epoch or a torn one never reports it.
    let version = u16::from_le_bytes(sector[14..16].try_into().unwrap());
    if version != WAL_FORMAT_VERSION {
        return SectorScan::OtherVersion(version);
    }
    let valid_bytes = u16::from_le_bytes(sector[12..14].try_into().unwrap()) as usize;
    if valid_bytes == 0 || valid_bytes > WAL_SECTOR_PAYLOAD_CAP {
        return SectorScan::Corrupt;
    }

    // Parse fragments within `sector[16 .. 16 + valid_bytes]`.
    let mut pos = WAL_SECTOR_HEADER_SIZE;
    let limit = WAL_SECTOR_HEADER_SIZE + valid_bytes;
    while pos < limit {
        if limit - pos < FRAGMENT_HEADER_SIZE {
            return SectorScan::Corrupt;
        }
        let Ok(frag_type) = FragmentType::from_u8(sector[pos]) else {
            return SectorScan::Corrupt;
        };
        let frag_len = u16::from_le_bytes([sector[pos + 1], sector[pos + 2]]) as usize;
        pos += FRAGMENT_HEADER_SIZE;
        if limit - pos < frag_len {
            return SectorScan::Corrupt;
        }
        let frag_data = &sector[pos..pos + frag_len];
        pos += frag_len;

        match frag_type {
            FragmentType::Full => {
                // A previous `First`/`Middle` chain left dangling by a crash is discarded.
                *partial = None;
                match WalBatch::decode(frag_data) {
                    Ok(batch) => out.push(batch),
                    Err(_) => return SectorScan::Corrupt,
                }
            }
            FragmentType::First => {
                // Also discards any prior dangling `First`/`Middle` chain.
                *partial = Some(frag_data.to_vec());
            }
            FragmentType::Middle => match partial {
                Some(buf) => buf.extend_from_slice(frag_data),
                None if lenient => {}
                None => return SectorScan::Corrupt,
            },
            FragmentType::Last => match partial.take() {
                Some(mut buf) => {
                    buf.extend_from_slice(frag_data);
                    match WalBatch::decode(&buf) {
                        Ok(batch) => out.push(batch),
                        Err(_) => return SectorScan::Corrupt,
                    }
                }
                None if lenient => {}
                None => return SectorScan::Corrupt,
            },
        }
    }
    SectorScan::Usable
}

/// Controls how recovery treats an ordinary end-of-log sector (all zeros or an intact older-epoch
/// sector).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalScanMode {
    /// Stop reading at the first end-of-log sector (fast clean opens under `Truncate`).
    StopAtEndOfLog,
    /// End the valid prefix at an end-of-log sector, but continue scanning the remainder of the
    /// file into `past_stop` so later intact current-epoch batches are discovered (used by
    /// `WalDamagePolicy::Fail` and `open_with_salvage`).
    FullScan,
}

/// Scans a single WAL file from Sector 0 and extracts the [`WalBatch`] items of `expected_epoch`.
///
/// The valid prefix ends at the first sector that is not a usable current-epoch sector: an
/// older epoch, a newer one, a torn-write CRC, an unparsable body or an unsupported format
/// version. Past an unusable sector the scan collects later intact current-epoch batches in
/// `past_stop`. Under [`WalScanMode::StopAtEndOfLog`], an all-zero or intact older-epoch sector
/// ends the scan immediately so clean opens read at most one window per stream; under
/// [`WalScanMode::FullScan`], such sectors end the valid prefix (and any in-flight fragmented
/// batch) while scanning continues to the end of the file.
pub fn recover_single_wal_stream_with_mode<F: PlatformFile>(
    file: &F,
    expected_epoch: u64,
    scan_mode: WalScanMode,
) -> io::Result<SingleStreamRecovery> {
    let file_len = file.file_len()?;
    let total_sectors = usize::try_from(file_len / WAL_SECTOR_SIZE as u64).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "WAL file sector count exceeds usize::MAX",
        )
    })?;

    let mut rec = SingleStreamRecovery::default();
    if total_sectors == 0 {
        return Ok(rec);
    }

    let mut partial_batch_buf: Option<Vec<u8>> = None;
    let mut stopped = false;
    let verify_other_epoch_crc = scan_mode == WalScanMode::StopAtEndOfLog;
    let mut sec_idx = 0usize;

    // On a clean reopen under `StopAtEndOfLog`, Sector 0 is typically all zeros or from an
    // earlier epoch. Probe only that 4 KB sector on the stack first to avoid a 256 KB read and
    // heap allocation when no recovery work exists.
    if scan_mode == WalScanMode::StopAtEndOfLog {
        let mut first_sector = [0u8; WAL_SECTOR_SIZE];
        file.read_exact_at(&mut first_sector, 0)?;
        match scan_sector(
            &first_sector,
            expected_epoch,
            false,
            verify_other_epoch_crc,
            &mut partial_batch_buf,
            &mut rec.batches,
        ) {
            SectorScan::Usable => rec.next_sector_idx = 1,
            SectorScan::EndOfLog => return Ok(rec),
            SectorScan::OtherEpoch => {
                stopped = true;
                partial_batch_buf = None;
            }
            SectorScan::Corrupt => {
                rec.corrupt_sectors += 1;
                stopped = true;
                partial_batch_buf = None;
            }
            SectorScan::OtherVersion(version) => {
                rec.unsupported_format.get_or_insert(UnsupportedWalFormat {
                    sector_idx: 0,
                    version,
                });
                stopped = true;
                partial_batch_buf = None;
            }
        }
        sec_idx = 1;
    }

    if sec_idx >= total_sectors {
        return Ok(rec);
    }

    // Read remaining sectors in up to 256 KB windows (64 sectors) to maximize sequential throughput.
    const WINDOW_SECTORS: usize = 64;
    let initial_window = (total_sectors - sec_idx).min(WINDOW_SECTORS);
    let mut window_buf = vec![0u8; initial_window * WAL_SECTOR_SIZE];

    'scan: while sec_idx < total_sectors {
        let count = (total_sectors - sec_idx).min(WINDOW_SECTORS);
        let byte_len = count * WAL_SECTOR_SIZE;
        file.read_exact_at(&mut window_buf[..byte_len], sector_byte_offset(sec_idx)?)?;

        for i in 0..count {
            let sector = &window_buf[i * WAL_SECTOR_SIZE..(i + 1) * WAL_SECTOR_SIZE];
            let out = if stopped {
                &mut rec.past_stop
            } else {
                &mut rec.batches
            };
            match scan_sector(
                sector,
                expected_epoch,
                stopped,
                verify_other_epoch_crc,
                &mut partial_batch_buf,
                out,
            ) {
                SectorScan::Usable => {
                    if !stopped {
                        rec.next_sector_idx = sec_idx + i + 1;
                    }
                    continue;
                }
                SectorScan::EndOfLog => match scan_mode {
                    WalScanMode::StopAtEndOfLog => break 'scan,
                    WalScanMode::FullScan => {}
                },
                SectorScan::OtherEpoch => {}
                SectorScan::Corrupt => rec.corrupt_sectors += 1,
                SectorScan::OtherVersion(version) => {
                    rec.unsupported_format.get_or_insert(UnsupportedWalFormat {
                        sector_idx: sec_idx + i,
                        version,
                    });
                }
            }
            // An unusable sector: the batch it would have continued is lost.
            stopped = true;
            partial_batch_buf = None;
        }
        sec_idx += count;
    }

    // Any incomplete `First`/`Middle` chain without a `Last` fragment is automatically discarded.
    Ok(rec)
}

/// Scans a single WAL file from Sector 0 in [`WalScanMode::StopAtEndOfLog`] mode.
pub fn recover_single_wal_stream<F: PlatformFile>(
    file: &F,
    expected_epoch: u64,
) -> io::Result<SingleStreamRecovery> {
    recover_single_wal_stream_with_mode(file, expected_epoch, WalScanMode::StopAtEndOfLog)
}

#[cfg(test)]
mod recovery_tests;

#[cfg(test)]
mod tests;

#[cfg(kani)]
mod kani_proofs {
    use super::*;

    fn assert_bytes_eq(a: &[u8], b: &[u8]) {
        assert_eq!(a.len(), b.len());
        let mut i = 0;
        while i < a.len() {
            assert_eq!(a[i], b[i]);
            i += 1;
        }
    }

    #[kani::proof]
    #[kani::unwind(4)]
    fn verify_wal_record_decode_arbitrary_bytes_no_panic() {
        let buf: [u8; 40] = kani::any();
        let len: usize = kani::any();
        kani::assume(len <= buf.len());
        let mut r = ByteReader::with_eof_msg(&buf[..len], "truncated WalBatch payload");
        let _ = BorrowedWalRecord::decode_from(&mut r);
    }

    fn unwrap_io<T>(res: io::Result<T>) -> T {
        match res {
            Ok(v) => v,
            Err(e) => {
                std::mem::forget(e);
                panic!("unexpected io::Error in Kani proof");
            }
        }
    }

    #[inline(always)]
    fn check_wal_roundtrip(
        record: &WalRecord,
        cap: usize,
        verify: impl FnOnce(BorrowedWalRecord, &[u8]),
    ) {
        let mut buf = Vec::with_capacity(cap);
        WalBatch::encode_single_record_into(record, &mut buf);
        let mut r = ByteReader::with_eof_msg(&buf, "truncated WalBatch payload");
        let decoded = unwrap_io(BorrowedWalRecord::decode_from(&mut r));
        assert!(r.is_empty());
        verify(decoded, &buf);
    }

    #[kani::proof]
    #[kani::unwind(4)]
    fn verify_wal_record_roundtrip_inline_delete_and_markers() {
        let kbytes: [u8; 2] = kani::any();
        let vbytes: [u8; 2] = kani::any();
        let kind_tag: u8 = kani::any();
        kani::assume(kind_tag < 5);

        match kind_tag {
            0 => {
                let record = WalRecord::PutInline {
                    key: kbytes.to_vec(),
                    value: vbytes.to_vec(),
                };
                check_wal_roundtrip(&record, 24, |decoded, buf| match decoded {
                    BorrowedWalRecord::PutInline { key: dk, value: dv } => {
                        assert_bytes_eq(&buf[dk], &kbytes);
                        assert_bytes_eq(&buf[dv], &vbytes);
                    }
                    _ => panic!("decoded variant mismatch"),
                });
            }
            1 => {
                let record = WalRecord::Delete {
                    key: kbytes.to_vec(),
                };
                check_wal_roundtrip(&record, 16, |decoded, buf| match decoded {
                    BorrowedWalRecord::Delete { key: dk } => {
                        assert_bytes_eq(&buf[dk], &kbytes);
                    }
                    _ => panic!("decoded variant mismatch"),
                });
            }
            2 => {
                check_wal_roundtrip(&WalRecord::TxBegin, 8, |decoded, _| {
                    assert!(matches!(decoded, BorrowedWalRecord::TxBegin));
                });
            }
            3 => {
                check_wal_roundtrip(&WalRecord::TxCommit, 8, |decoded, _| {
                    assert!(matches!(decoded, BorrowedWalRecord::TxCommit));
                });
            }
            _ => {
                check_wal_roundtrip(&WalRecord::TxContinue, 8, |decoded, _| {
                    assert!(matches!(decoded, BorrowedWalRecord::TxContinue));
                });
            }
        }
    }

    #[kani::proof]
    #[kani::unwind(4)]
    fn verify_wal_record_roundtrip_blob_ptr() {
        let kbytes: [u8; 2] = kani::any();
        let blob_ptr = BlobPtr {
            kind: if kani::any() {
                BlobKind::Standard
            } else {
                BlobKind::Lru
            },
            table_id: kani::any(),
            seg_id: kani::any(),
            offset: kani::any(),
            len: kani::any(),
            crc32c: kani::any(),
        };
        let record = WalRecord::PutBlobPtr {
            key: kbytes.to_vec(),
            blob_ptr,
        };

        check_wal_roundtrip(&record, 48, |decoded, buf| match decoded {
            BorrowedWalRecord::PutBlobPtr {
                key: dk,
                blob_ptr: dp,
            } => {
                assert_bytes_eq(&buf[dk], &kbytes);
                assert_eq!(dp.kind, blob_ptr.kind);
                assert_eq!(dp.table_id, blob_ptr.table_id);
                assert_eq!(dp.seg_id, blob_ptr.seg_id);
                assert_eq!(dp.offset, blob_ptr.offset);
                assert_eq!(dp.len, blob_ptr.len);
                assert_eq!(dp.crc32c, blob_ptr.crc32c);
            }
            _ => panic!("decoded variant mismatch"),
        });
    }

    #[kani::proof]
    #[kani::unwind(8)]
    fn verify_wal_batch_header_and_crc_validation() {
        let buf: [u8; 36] = kani::any();
        let len: usize = kani::any();
        kani::assume(len <= 20);
        let _ = WalBatch::parse_header_and_verify_crc(&buf[..len]);

        let lsn: u64 = kani::any();
        let prev_lsn: u64 = kani::any();
        let durable_lsn: u64 = kani::any();
        let op: u8 = kani::any();

        let mut batch_buf = [0u8; WAL_BATCH_HEADER_SIZE + 1];
        batch_buf[32] = op;
        WalBatch::finalize_encoded_buf(lsn, prev_lsn, durable_lsn, 1, &mut batch_buf);

        let (d_lsn, rc, d_prev, d_dur, r) =
            WalBatch::parse_header_and_verify_crc(&batch_buf).unwrap();
        assert_eq!(d_lsn, lsn);
        assert_eq!(rc, 1);
        assert_eq!(d_prev, prev_lsn);
        assert_eq!(d_dur, durable_lsn);
        let rem = r.remaining_slice();
        assert_eq!(rem.len(), 1);
        assert_eq!(rem[0], op);

        batch_buf[32] ^= 0x01;
        assert!(WalBatch::parse_header_and_verify_crc(&batch_buf).is_err());
    }
}
