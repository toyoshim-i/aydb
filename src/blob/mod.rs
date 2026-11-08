//! Layer 3 (Part 1): Out-of-Line Append-Only Blob Segments (`blob_std_<SeqID>.db` and
//! `blob_lru_<TblID>_<SeqID>.db`).
//!
//! Enforces the `Write-Blob-Before-WAL` crash-consistency protocol: blobs are appended
//! before their 23-byte [`BlobPtr`] is logged to the WAL, and every read or recovery check
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
pub const BLOB_RECORD_HEADER_LEN: usize = 16;
/// Maximum user-space write buffer size (`256 KB`) in [`BlobSegmentWriter`] before flushing
/// coalesced blob records to the OS file handle.
const BLOB_WRITE_BUFFER_CAPACITY: usize = 256 * 1024;
/// Maximum blob frame size (`16 KB`) read via a stack buffer in [`BlobSegmentReader::append_blob_into`]
/// to avoid redundant `Vec::resize` zero-filling and `copy_within` payload shifting.
const BLOB_STACK_READ_MAX_LEN: usize = 16 * 1024;

/// Append-only writer for a single standard or per-table LRU blob segment file.
///
/// Reuses an internal staging buffer (`write_buf`) to eliminate per-blob heap allocations and
/// coalesce multi-row blob writes into fewer OS `WriteFile` / `pwrite` syscalls.
pub struct BlobSegmentWriter<F: PlatformFile> {
    file: Arc<F>,
    kind: BlobKind,
    table_id: u16,
    seq_id: u64,
    write_offset: u64,
    buf_start_offset: u64,
    write_buf: Vec<u8>,
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
            buf_start_offset: write_offset,
            write_buf: Vec::with_capacity(64 * 1024),
        })
    }

    pub fn seq_id(&self) -> u64 {
        self.seq_id
    }

    pub fn table_id(&self) -> u16 {
        self.table_id
    }

    pub fn current_len(&self) -> u64 {
        self.write_offset
    }

    /// Returns the file offset up to which buffered blob records have been flushed to the OS handle.
    #[inline]
    pub fn flushed_offset(&self) -> u64 {
        self.buf_start_offset
    }

    pub fn file(&self) -> &Arc<F> {
        &self.file
    }

    /// Flushes any staged blob records in `write_buf` to the underlying OS file handle.
    #[inline]
    pub fn flush_buffer(&mut self) -> io::Result<()> {
        if !self.write_buf.is_empty() {
            self.file
                .write_all_at(&self.write_buf, self.buf_start_offset)?;
            self.buf_start_offset += self.write_buf.len() as u64;
            self.write_buf.clear();
        }
        Ok(())
    }

    /// Appends `[Magic "AYBL" (4B) | BlobCRC32C (4B) | KeyLen (4B) | ValLen (4B) | Key | Value]`
    /// to the blob segment and returns its [`BlobPtr`].
    ///
    /// Stages records into the reusable `write_buf` without allocating a fresh `Vec<u8>` per blob.
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

        if !self.write_buf.is_empty()
            && self.write_buf.len().saturating_add(total_len) > BLOB_WRITE_BUFFER_CAPACITY
        {
            self.flush_buffer()?;
        }
        if self.write_buf.is_empty() {
            self.buf_start_offset = self.write_offset;
        }

        let rec_start = self.write_buf.len();
        self.write_buf.reserve(total_len);
        self.write_buf.extend_from_slice(BLOB_RECORD_MAGIC);
        self.write_buf.extend_from_slice(&0u32.to_le_bytes()); // placeholder for CRC32C at +4..+8
        self.write_buf
            .extend_from_slice(&(key.len() as u32).to_le_bytes());
        self.write_buf
            .extend_from_slice(&(value.len() as u32).to_le_bytes());
        self.write_buf.extend_from_slice(key);
        self.write_buf.extend_from_slice(value);

        let payload_crc = crc32c(&self.write_buf[rec_start + 8..rec_start + total_len]);
        self.write_buf[rec_start + 4..rec_start + 8]
            .copy_from_slice(&payload_crc.to_le_bytes());

        self.write_offset += total_len as u64;
        if self.write_buf.len() >= BLOB_WRITE_BUFFER_CAPACITY {
            self.flush_buffer()?;
        }

        Ok(BlobPtr {
            kind: self.kind,
            table_id: self.table_id,
            seg_id: self.seq_id,
            offset: offset_u32,
            len: total_len_u32,
            crc32c: payload_crc,
        })
    }

    /// Flushes buffered blob writes to the OS file handle and syncs to physical storage (used during checkpointing).
    pub fn sync_durable(&mut self) -> io::Result<()> {
        self.flush_buffer()?;
        self.file.sync_durable()
    }
}

/// Reader and verifier for an immutable or active blob segment file.
pub struct BlobSegmentReader<F: PlatformFile> {
    file: Arc<F>,
    seq_id: u64,
}

impl<F: PlatformFile> Clone for BlobSegmentReader<F> {
    /// Cheap clone: bumps the `Arc` refcount of the underlying file handle (pins the open file).
    fn clone(&self) -> Self {
        Self {
            file: Arc::clone(&self.file),
            seq_id: self.seq_id,
        }
    }
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
    /// Returns `Ok(Some(payload))` only when the record was read in full and passed the
    /// magic / CRC32C / length / key checks, and `Ok(None)` when verification FAILED (short read,
    /// bad magic, CRC mismatch, key mismatch). `Err` is reserved for genuine I/O errors.
    ///
    /// This layer does not decide what a verification failure means: after the engine has
    /// opened, blobs referenced by durable index state were validated or synced beforehand, so
    /// the engine treats `Ok(None)` as corruption and applies the per-table `on_corrupt` policy
    /// (see `CorruptPolicy`) instead of reporting a plain cache miss. Only recovery
    /// ([`Self::validate_blob_ptr`]) interprets a failure as "uncommitted record".
    pub fn read_blob(
        &self,
        ptr: &BlobPtr,
        expected_key: Option<&[u8]>,
    ) -> io::Result<Option<Vec<u8>>> {
        let mut out = Vec::new();
        if self.read_blob_into(ptr, expected_key, &mut out)? {
            Ok(Some(out))
        } else {
            Ok(None)
        }
    }

    /// Reads and verifies the blob payload referenced by `ptr` directly into `out` (clearing `out`
    /// first while reusing its heap allocation) with zero intermediate `Vec<u8>` allocations.
    pub fn read_blob_into(
        &self,
        ptr: &BlobPtr,
        expected_key: Option<&[u8]>,
        out: &mut Vec<u8>,
    ) -> io::Result<bool> {
        out.clear();
        self.append_blob_into(ptr, expected_key, out)
    }

    /// Appends the verified blob payload referenced by `ptr` onto the end of `out` in place
    /// without allocating an intermediate frame buffer.
    pub fn append_blob_into(
        &self,
        ptr: &BlobPtr,
        expected_key: Option<&[u8]>,
        out: &mut Vec<u8>,
    ) -> io::Result<bool> {
        let len = ptr.len as usize;
        if len < BLOB_RECORD_HEADER_LEN {
            return Ok(false);
        }

        // Fast path: for typical blobs (`<= 16 KB`), read into a stack buffer so we
        // avoid `Vec::resize` user-space zero-filling and `copy_within` overlapping memmove!
        if len <= BLOB_STACK_READ_MAX_LEN {
            let mut stack_buf = [0u8; BLOB_STACK_READ_MAX_LEN];
            let buf = &mut stack_buf[..len];
            match self.file.read_exact_at(buf, ptr.offset as u64) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    return Ok(false);
                }
                Err(e) => {
                    return Err(e);
                }
            }

            if &buf[0..4] != BLOB_RECORD_MAGIC {
                return Ok(false);
            }
            let stored_crc = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]);
            if stored_crc != ptr.crc32c {
                return Ok(false);
            }
            if crc32c(&buf[8..]) != ptr.crc32c {
                return Ok(false);
            }

            let key_len = u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]) as usize;
            let val_len = u32::from_le_bytes([buf[12], buf[13], buf[14], buf[15]]) as usize;
            if BLOB_RECORD_HEADER_LEN
                .checked_add(key_len)
                .and_then(|n| n.checked_add(val_len))
                != Some(len)
            {
                return Ok(false);
            }

            let key_slice = &buf[BLOB_RECORD_HEADER_LEN..BLOB_RECORD_HEADER_LEN + key_len];
            if let Some(exp_k) = expected_key {
                if key_slice != exp_k {
                    return Ok(false);
                }
            }

            let val_start = BLOB_RECORD_HEADER_LEN + key_len;
            out.extend_from_slice(&buf[val_start..val_start + val_len]);
            return Ok(true);
        }

        let orig_len = out.len();
        out.resize(orig_len + len, 0);
        match self
            .file
            .read_exact_at(&mut out[orig_len..orig_len + len], ptr.offset as u64)
        {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                // Power loss occurred before the buffered blob write reached physical disk.
                out.truncate(orig_len);
                return Ok(false);
            }
            Err(e) => {
                out.truncate(orig_len);
                return Err(e);
            }
        }

        let buf = &out[orig_len..orig_len + len];
        if &buf[0..4] != BLOB_RECORD_MAGIC {
            out.truncate(orig_len);
            return Ok(false);
        }

        let stored_crc = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]);
        if stored_crc != ptr.crc32c {
            out.truncate(orig_len);
            return Ok(false);
        }

        let actual_crc = crc32c(&buf[8..]);
        if actual_crc != ptr.crc32c {
            out.truncate(orig_len);
            return Ok(false);
        }

        let key_len = u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]) as usize;
        let val_len = u32::from_le_bytes([buf[12], buf[13], buf[14], buf[15]]) as usize;

        if BLOB_RECORD_HEADER_LEN
            .checked_add(key_len)
            .and_then(|n| n.checked_add(val_len))
            != Some(len)
        {
            out.truncate(orig_len);
            return Ok(false);
        }

        let key_slice = &buf[BLOB_RECORD_HEADER_LEN..BLOB_RECORD_HEADER_LEN + key_len];
        if let Some(exp_k) = expected_key {
            if key_slice != exp_k {
                out.truncate(orig_len);
                return Ok(false);
            }
        }

        let val_start = orig_len + BLOB_RECORD_HEADER_LEN + key_len;
        out.copy_within(val_start..val_start + val_len, orig_len);
        out.truncate(orig_len + val_len);
        Ok(true)
    }

    /// Validates whether `ptr` points to an intact, CRC-verified blob record on disk.
    /// Used by the WAL recovery coordinator (`recover_two_streams`) to filter out uncommitted blobs.
    pub fn validate_blob_ptr(&self, ptr: &BlobPtr) -> bool {
        matches!(self.read_blob(ptr, None), Ok(Some(_)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pal::OsFs;
    use crate::util::test_util::TempTestDir;

    #[test]
    fn blob_write_verify_and_unflushed_truncation_safety() {
        let tmp = TempTestDir::new("blob");
        let fs = OsFs::open(&tmp.path, true).expect("open OsFs");

        let mut writer =
            BlobSegmentWriter::open(&fs, BlobKind::Standard, 0, 1).expect("open blob writer");
        let payload = vec![0xABu8; 8192];
        let ptr1 = writer
            .append_blob(b"http://example.com/a.png", &payload)
            .expect("append blob 1");
        writer.sync_durable().unwrap();
        drop(writer);

        let reader =
            BlobSegmentReader::open(&fs, BlobKind::Standard, 0, 1).expect("open blob reader");
        let fetched = reader
            .read_blob(&ptr1, Some(b"http://example.com/a.png"))
            .expect("read blob")
            .expect("blob must exist");
        assert_eq!(fetched, payload);

        // Wrong key check must return None
        assert!(
            reader
                .read_blob(&ptr1, Some(b"http://example.com/wrong.png"))
                .unwrap()
                .is_none()
        );

        // Verify `read_blob_into` reuses the caller's `out: &mut Vec<u8>` buffer
        // capacity across calls and validates CRC32C + `expected_key`!
        let mut reusable_buf = Vec::with_capacity(16384);
        let orig_cap = reusable_buf.capacity();
        assert!(
            reader
                .read_blob_into(&ptr1, Some(b"http://example.com/a.png"), &mut reusable_buf)
                .unwrap()
        );
        assert_eq!(reusable_buf, payload);
        assert_eq!(reusable_buf.capacity(), orig_cap);

        assert!(
            !reader
                .read_blob_into(
                    &ptr1,
                    Some(b"http://example.com/wrong.png"),
                    &mut reusable_buf
                )
                .unwrap()
        );
        assert!(reusable_buf.is_empty());

        let mut bad_crc_ptr = ptr1;
        bad_crc_ptr.crc32c ^= 0xDEAD_BEEF;
        assert!(
            !reader
                .read_blob_into(
                    &bad_crc_ptr,
                    Some(b"http://example.com/a.png"),
                    &mut reusable_buf
                )
                .unwrap()
        );
        assert!(reusable_buf.is_empty());

        // Unflushed / beyond-EOF pointer (simulating power loss before blob reached disk) must return Ok(None)
        let dangling_ptr = BlobPtr {
            kind: BlobKind::Standard,
            table_id: 0,
            seg_id: 1,
            offset: ptr1.len,
            len: 4096,
            crc32c: 0x1122_3344,
        };
        assert!(reader.read_blob(&dangling_ptr, None).unwrap().is_none());
        assert!(!reader.validate_blob_ptr(&dangling_ptr));
    }

    #[test]
    fn buffered_blob_writer_and_fast_stack_or_large_reads() {
        let dir = TempTestDir::new("buffered_blob");
        let fs = OsFs::open(&dir.path, true).unwrap();

        let mut writer = BlobSegmentWriter::open(&fs, BlobKind::Standard, 0, 1).unwrap();
        let val_small = vec![0xA5u8; 2048];
        let val_large = vec![0x5Au8; 24_000]; // exceeds 16 KB stack read threshold

        let ptr1 = writer.append_blob(b"k1", &val_small).unwrap();
        let ptr2 = writer.append_blob(b"k2", &val_large).unwrap();
        // Before `flush_buffer()`, staged bytes are in `write_buf` (`flushed_offset == 0`)
        assert_eq!(writer.flushed_offset(), 0);
        assert_eq!(writer.current_len(), (ptr1.len + ptr2.len) as u64);

        writer.flush_buffer().unwrap();
        assert_eq!(writer.flushed_offset(), writer.current_len());

        let reader = BlobSegmentReader::new(writer.file().clone(), 1);
        let mut slab = Vec::new();
        assert!(reader.append_blob_into(&ptr1, Some(b"k1"), &mut slab).unwrap());
        assert_eq!(slab, val_small);
        assert!(reader.append_blob_into(&ptr2, Some(b"k2"), &mut slab).unwrap());
        assert_eq!(&slab[..2048], val_small.as_slice());
        assert_eq!(&slab[2048..], val_large.as_slice());

        // Key mismatch must leave `slab` untouched and return `Ok(false)`
        let prev_len = slab.len();
        assert!(!reader.append_blob_into(&ptr1, Some(b"wrong"), &mut slab).unwrap());
        assert_eq!(slab.len(), prev_len);
        assert!(!reader.append_blob_into(&ptr2, Some(b"wrong"), &mut slab).unwrap());
        assert_eq!(slab.len(), prev_len);
    }
}
