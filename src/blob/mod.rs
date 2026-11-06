//! Layer 3 (Part 1): Out-of-Line Append-Only Blob Segments (`blob_std_<SeqID>.db` and
//! `blob_lru_<TblID>_<SeqID>.db`).
//!
//! Enforces the `Write-Blob-Before-WAL` crash-consistency protocol: blobs are appended
//! before their 21-byte [`BlobPtr`] is logged to the WAL, and every read or recovery check
//! verifies the framed `BlobCrc32c` so power-loss truncated blobs safely resolve to `None`.

use std::io;
use std::sync::Arc;

use crate::meta::{format_lru_blob_file_name, format_std_blob_file_name};
use crate::pal::{OpenHint, PlatformFile, PlatformFs};
use crate::util::crc32c::crc32c;
use crate::wal::{BlobKind, BlobPtr};

/// Default value size threshold (1024 bytes) at or above which values are stored out-of-line
/// in a `blob_*.db` segment rather than inline in `idx_<SeqID>.db`.
pub const DEFAULT_INLINE_THRESHOLD: usize = 1024;

/// Default target maximum size for a single blob segment file before rolling over to a new
/// `SeqID` (64 MB).
pub const DEFAULT_MAX_BLOB_SEGMENT_SIZE: u64 = 64 * 1024 * 1024;

const BLOB_RECORD_MAGIC: &[u8; 4] = b"AYBL";
const BLOB_RECORD_HEADER_LEN: usize = 16;

/// Append-only writer for a single standard or per-table LRU blob segment file.
pub struct BlobSegmentWriter<F: PlatformFile> {
    file: Arc<F>,
    kind: BlobKind,
    table_id: u16,
    seq_id: u64,
    write_offset: u64,
}

impl<F: PlatformFile> BlobSegmentWriter<F> {
    /// Opens or creates a blob segment file (`blob_std_<SeqID>.db` or `blob_lru_<TblID>_<SeqID>.db`).
    pub fn open(
        fs: &impl PlatformFs<File = F>,
        kind: BlobKind,
        table_id: u16,
        seq_id: u64,
    ) -> io::Result<Self> {
        let file_name = match kind {
            BlobKind::Standard => format_std_blob_file_name(seq_id),
            BlobKind::Lru => format_lru_blob_file_name(table_id, seq_id),
        };
        let file = fs.open_file(&file_name, true, true, OpenHint::ImmutableSequential)?;
        let write_offset = file.file_len()?;
        Ok(Self {
            file,
            kind,
            table_id: if kind == BlobKind::Standard {
                0
            } else {
                table_id
            },
            seq_id,
            write_offset,
        })
    }

    pub fn seq_id(&self) -> u64 {
        self.seq_id
    }

    pub fn current_len(&self) -> u64 {
        self.write_offset
    }

    pub fn file(&self) -> &Arc<F> {
        &self.file
    }

    /// Appends `[Magic "AYBL" (4B) | BlobCRC32C (4B) | KeyLen (4B) | ValLen (4B) | Key | Value]`
    /// to the blob segment and returns its [`BlobPtr`].
    pub fn append_blob(&mut self, key: &[u8], value: &[u8]) -> io::Result<BlobPtr> {
        let total_len = BLOB_RECORD_HEADER_LEN
            .checked_add(key.len())
            .and_then(|n| n.checked_add(value.len()))
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "blob record size overflow"))?;

        let total_len_u32 = u32::try_from(total_len).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "blob record exceeds u32::MAX bytes",
            )
        })?;
        let offset_u32 = u32::try_from(self.write_offset).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "blob segment offset exceeds u32::MAX bytes",
            )
        })?;

        let mut buf = Vec::with_capacity(total_len);
        buf.extend_from_slice(BLOB_RECORD_MAGIC);
        buf.extend_from_slice(&0u32.to_le_bytes()); // placeholder for CRC32C at 4..8
        buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
        buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
        buf.extend_from_slice(key);
        buf.extend_from_slice(value);

        let payload_crc = crc32c(&buf[8..]);
        buf[4..8].copy_from_slice(&payload_crc.to_le_bytes());

        self.file.write_all_at(&buf, self.write_offset)?;
        self.write_offset += total_len as u64;

        Ok(BlobPtr {
            kind: self.kind,
            table_id: self.table_id,
            seg_id: self.seq_id,
            offset: offset_u32,
            len: total_len_u32,
            crc32c: payload_crc,
        })
    }

    /// Flushes buffered blob writes to physical storage (used during checkpointing).
    pub fn sync_durable(&self) -> io::Result<()> {
        self.file.sync_durable()
    }
}

/// Reader and verifier for an immutable or active blob segment file.
pub struct BlobSegmentReader<F: PlatformFile> {
    file: Arc<F>,
    seq_id: u64,
}

impl<F: PlatformFile> BlobSegmentReader<F> {
    pub fn new(file: Arc<F>, seq_id: u64) -> Self {
        Self { file, seq_id }
    }

    pub fn open(
        fs: &impl PlatformFs<File = F>,
        kind: BlobKind,
        table_id: u16,
        seq_id: u64,
    ) -> io::Result<Self> {
        let file_name = match kind {
            BlobKind::Standard => format_std_blob_file_name(seq_id),
            BlobKind::Lru => format_lru_blob_file_name(table_id, seq_id),
        };
        let file = fs.open_file(&file_name, false, false, OpenHint::ImmutableSequential)?;
        Ok(Self { file, seq_id })
    }

    pub fn seq_id(&self) -> u64 {
        self.seq_id
    }

    /// Reads and verifies the blob payload referenced by `ptr`.
    ///
    /// Following the `Write-Blob-Before-WAL` protocol:
    /// - If the file was truncated before `offset + len` reached disk during a power outage,
    ///   or if the magic/CRC32C/key check fails, returns `Ok(None)` (safe cache miss / uncommitted
    ///   record) rather than raising a fatal corruption error.
    pub fn read_blob(
        &self,
        ptr: &BlobPtr,
        expected_key: Option<&[u8]>,
    ) -> io::Result<Option<Vec<u8>>> {
        let len = ptr.len as usize;
        if len < BLOB_RECORD_HEADER_LEN {
            return Ok(None);
        }

        let mut buf = vec![0u8; len];
        match self.file.read_exact_at(&mut buf, ptr.offset as u64) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                // Power loss occurred before the buffered blob write reached physical disk.
                return Ok(None);
            }
            Err(e) => return Err(e),
        }

        if &buf[0..4] != BLOB_RECORD_MAGIC {
            return Ok(None);
        }

        let stored_crc = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]);
        if stored_crc != ptr.crc32c {
            return Ok(None);
        }

        let actual_crc = crc32c(&buf[8..]);
        if actual_crc != ptr.crc32c {
            return Ok(None);
        }

        let key_len = u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]) as usize;
        let val_len = u32::from_le_bytes([buf[12], buf[13], buf[14], buf[15]]) as usize;

        if BLOB_RECORD_HEADER_LEN
            .checked_add(key_len)
            .and_then(|n| n.checked_add(val_len))
            != Some(len)
        {
            return Ok(None);
        }

        let key_slice = &buf[BLOB_RECORD_HEADER_LEN..BLOB_RECORD_HEADER_LEN + key_len];
        if let Some(exp_k) = expected_key {
            if key_slice != exp_k {
                return Ok(None);
            }
        }

        let val_start = BLOB_RECORD_HEADER_LEN + key_len;
        Ok(Some(buf[val_start..val_start + val_len].to_vec()))
    }

    /// Validates whether `ptr` points to an intact, CRC-verified blob record on disk.
    /// Used by the WAL recovery coordinator (`recover_two_streams`) to filter out uncommitted blobs.
    pub fn validate_blob_ptr(&self, ptr: &BlobPtr) -> bool {
        matches!(self.read_blob(ptr, None), Ok(Some(_)))
    }
}
