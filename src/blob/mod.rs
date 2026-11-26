//! Layer 3 (Part 1): Out-of-Line Append-Only Blob Segments (`blob_std_<SeqID>.db` and
//! `blob_lru_<TblID>_<SeqID>.db`).
//!
//! Enforces the `Write-Blob-Before-WAL` crash-consistency protocol: blobs are appended
//! before their 23-byte [`BlobPtr`] is logged to the WAL, and every read or recovery check
//! verifies the framed `BlobCrc32c` so power-loss truncated blobs safely resolve to `None`.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::meta::{format_lru_blob_file_name, format_std_blob_file_name};
use crate::pal::{OpenHint, PlatformFile, PlatformFs};
use crate::util::crc32c::{crc32c, crc32c_append};
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
    /// Monotonically updated flushed file length shared with paired [`BlobSegmentReader`] handles.
    known_file_len: Arc<AtomicU64>,
    /// File offset up to which `sync_durable` has completed successfully. Bytes below this
    /// offset need no further OS sync; the value starts at zero so that the first sync after
    /// open also covers a tail written by a previous process.
    synced_offset: u64,
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
            known_file_len: Arc::new(AtomicU64::new(write_offset)),
            synced_offset: 0,
            write_buf: Vec::with_capacity(64 * 1024),
        })
    }

    /// Creates a [`BlobSegmentReader`] sharing this writer's handle and live flushed length.
    pub fn reader(&self) -> BlobSegmentReader<F> {
        BlobSegmentReader {
            file: Arc::clone(&self.file),
            seq_id: self.seq_id,
            known_file_len: Arc::clone(&self.known_file_len),
        }
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

    /// Returns the file offset up to which `sync_durable` has completed successfully.
    #[inline]
    pub fn synced_offset(&self) -> u64 {
        self.synced_offset
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
            self.known_file_len
                .fetch_max(self.buf_start_offset, Ordering::Release);
            self.write_buf.clear();
            if self.write_buf.capacity() > BLOB_WRITE_BUFFER_CAPACITY * 2 {
                self.write_buf.shrink_to(BLOB_WRITE_BUFFER_CAPACITY);
            }
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
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "blob record size overflow")
            })?;

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
        self.write_buf.try_reserve(total_len).map_err(|_| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                "failed to allocate blob staging buffer",
            )
        })?;
        self.write_buf.extend_from_slice(BLOB_RECORD_MAGIC);
        self.write_buf.extend_from_slice(&0u32.to_le_bytes()); // placeholder for CRC32C at +4..+8
        self.write_buf
            .extend_from_slice(&(key.len() as u32).to_le_bytes());
        self.write_buf
            .extend_from_slice(&(value.len() as u32).to_le_bytes());
        self.write_buf.extend_from_slice(key);
        self.write_buf.extend_from_slice(value);

        let payload_crc = crc32c(&self.write_buf[rec_start + 8..rec_start + total_len]);
        self.write_buf[rec_start + 4..rec_start + 8].copy_from_slice(&payload_crc.to_le_bytes());

        if self.write_buf.len() >= BLOB_WRITE_BUFFER_CAPACITY {
            if let Err(e) = self.flush_buffer() {
                self.write_buf.truncate(rec_start);
                if self.write_buf.is_empty()
                    && self.write_buf.capacity() > BLOB_WRITE_BUFFER_CAPACITY * 2
                {
                    self.write_buf.shrink_to(BLOB_WRITE_BUFFER_CAPACITY);
                }
                return Err(e);
            }
        }
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

    /// Flushes buffered blob writes to the OS file handle and syncs to physical storage (used during checkpointing).
    ///
    /// The OS sync is skipped when no bytes have been written since the last successful sync.
    /// `synced_offset` advances only after the sync returns, so a failed sync is retried by the
    /// next call.
    pub fn sync_durable(&mut self) -> io::Result<()> {
        self.flush_buffer()?;
        if self.buf_start_offset > self.synced_offset {
            self.file.sync_durable()?;
            self.synced_offset = self.buf_start_offset;
        }
        Ok(())
    }
}

/// Reader and verifier for an immutable or active blob segment file.
pub struct BlobSegmentReader<F: PlatformFile> {
    file: Arc<F>,
    seq_id: u64,
    known_file_len: Arc<AtomicU64>,
}

impl<F: PlatformFile> Clone for BlobSegmentReader<F> {
    /// Cheap clone: bumps the `Arc` refcount of the underlying file handle and shared length cache.
    fn clone(&self) -> Self {
        Self {
            file: Arc::clone(&self.file),
            seq_id: self.seq_id,
            known_file_len: Arc::clone(&self.known_file_len),
        }
    }
}

fn parse_and_check_blob_header(
    hdr: &[u8],
    expected_total_len: usize,
    expected_crc: u32,
    expected_key: Option<&[u8]>,
) -> Option<(usize, usize)> {
    if hdr.len() < BLOB_RECORD_HEADER_LEN || &hdr[0..4] != BLOB_RECORD_MAGIC {
        return None;
    }
    let stored_crc = u32::from_le_bytes([hdr[4], hdr[5], hdr[6], hdr[7]]);
    if stored_crc != expected_crc {
        return None;
    }
    let key_len = u32::from_le_bytes([hdr[8], hdr[9], hdr[10], hdr[11]]) as usize;
    let val_len = u32::from_le_bytes([hdr[12], hdr[13], hdr[14], hdr[15]]) as usize;
    if BLOB_RECORD_HEADER_LEN
        .checked_add(key_len)
        .and_then(|n| n.checked_add(val_len))
        != Some(expected_total_len)
    {
        return None;
    }
    if let Some(exp_k) = expected_key {
        if key_len != exp_k.len() {
            return None;
        }
    }
    Some((key_len, val_len))
}

fn verify_blob_body(
    buf: &[u8],
    expected_crc: u32,
    expected_key: Option<&[u8]>,
    key_len: usize,
) -> Option<usize> {
    if crc32c(&buf[8..]) != expected_crc {
        return None;
    }

    let val_start = BLOB_RECORD_HEADER_LEN + key_len;
    if let Some(exp_k) = expected_key {
        if &buf[BLOB_RECORD_HEADER_LEN..val_start] != exp_k {
            return None;
        }
    }

    Some(val_start)
}

impl<F: PlatformFile> BlobSegmentReader<F> {
    pub fn new(file: Arc<F>, seq_id: u64) -> Self {
        Self::new_with_len(file, seq_id, 0)
    }

    pub fn new_with_len(file: Arc<F>, seq_id: u64, file_len: u64) -> Self {
        Self {
            file,
            seq_id,
            known_file_len: Arc::new(AtomicU64::new(file_len)),
        }
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
        let file_len = file.file_len()?;
        Ok(Self::new_with_len(file, seq_id, file_len))
    }

    pub fn seq_id(&self) -> u64 {
        self.seq_id
    }

    pub fn file(&self) -> &Arc<F> {
        &self.file
    }

    /// Verifies the framed blob record in `buf` against `expected_crc` and optional `expected_key`.
    /// Returns a slice to the payload value on success, or `None` if verification fails.
    pub fn verify_blob_record<'a>(
        buf: &'a [u8],
        expected_crc: u32,
        expected_key: Option<&[u8]>,
    ) -> Option<&'a [u8]> {
        let (key_len, val_len) =
            parse_and_check_blob_header(buf, buf.len(), expected_crc, expected_key)?;
        let val_start = verify_blob_body(buf, expected_crc, expected_key, key_len)?;
        Some(&buf[val_start..val_start + val_len])
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

    fn checked_ptr_end_in_file(&self, ptr: &BlobPtr) -> io::Result<Option<u64>> {
        if (ptr.len as usize) < BLOB_RECORD_HEADER_LEN {
            return Ok(None);
        }
        let Some(end_offset) = (ptr.offset as u64).checked_add(ptr.len as u64) else {
            return Ok(None);
        };
        if end_offset > self.known_file_len.load(Ordering::Acquire) {
            let file_len = self.file.file_len()?;
            self.known_file_len.fetch_max(file_len, Ordering::Release);
            if end_offset > file_len {
                return Ok(None);
            }
        }
        Ok(Some(end_offset))
    }

    /// Appends the verified blob payload referenced by `ptr` onto the end of `out` in place
    /// without allocating an intermediate frame buffer.
    pub fn append_blob_into(
        &self,
        ptr: &BlobPtr,
        expected_key: Option<&[u8]>,
        out: &mut Vec<u8>,
    ) -> io::Result<bool> {
        let Some(_) = self.checked_ptr_end_in_file(ptr)? else {
            return Ok(false);
        };
        let len = ptr.len as usize;

        // Validate the 16-byte stack header before allocating `ptr.len` bytes in `out` so a
        // tampered or mismatched record cannot force a huge reservation before failing checks.
        let mut hdr = [0u8; BLOB_RECORD_HEADER_LEN];
        match self.file.read_exact_at(&mut hdr, ptr.offset as u64) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(false),
            Err(e) => return Err(e),
        }
        let Some((key_len, val_len)) =
            parse_and_check_blob_header(&hdr, len, ptr.crc32c, expected_key)
        else {
            return Ok(false);
        };

        let orig_len = out.len();
        let new_len = orig_len.checked_add(len).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "blob read buffer length overflow",
            )
        })?;
        let reserve_res = if orig_len == 0 {
            out.try_reserve_exact(len)
        } else {
            out.try_reserve(len)
        };
        reserve_res.map_err(|_| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                "failed to allocate buffer for blob record",
            )
        })?;
        out.resize(new_len, 0);
        out[orig_len..orig_len + BLOB_RECORD_HEADER_LEN].copy_from_slice(&hdr);
        if len > BLOB_RECORD_HEADER_LEN {
            match self.file.read_exact_at(
                &mut out[orig_len + BLOB_RECORD_HEADER_LEN..new_len],
                (ptr.offset as u64) + BLOB_RECORD_HEADER_LEN as u64,
            ) {
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
        }

        let Some(val_offset) =
            verify_blob_body(&out[orig_len..new_len], ptr.crc32c, expected_key, key_len)
        else {
            out.truncate(orig_len);
            return Ok(false);
        };
        let val_start = orig_len + val_offset;
        out.copy_within(val_start..val_start + val_len, orig_len);
        out.truncate(orig_len + val_len);
        Ok(true)
    }

    /// Validates whether `ptr` points to an intact, CRC-verified blob record on disk using O(1)
    /// bounded scratch memory rather than loading the entire payload into heap.
    /// Used by the WAL recovery coordinator (`recover_two_streams`) to filter out uncommitted blobs.
    ///
    /// Returns `Ok(false)` when the record is absent or fails verification (the blob was never
    /// durably written) and `Err` for genuine I/O errors, which must not be mistaken for an
    /// uncommitted record.
    pub fn validate_blob_ptr(&self, ptr: &BlobPtr) -> io::Result<bool> {
        self.validate_blob_ptr_for_key(ptr, None)
    }

    pub(crate) fn validate_blob_ptr_for_key(
        &self,
        ptr: &BlobPtr,
        expected_key: Option<&[u8]>,
    ) -> io::Result<bool> {
        const STACK_BUF_LEN: usize = 16 * 1024;
        const STREAM_CHUNK_LEN: usize = 64 * 1024;

        let Some(_) = self.checked_ptr_end_in_file(ptr)? else {
            return Ok(false);
        };
        let total_len = ptr.len as usize;

        let mut stack_buf = [0u8; STACK_BUF_LEN];
        let first_len = total_len.min(STACK_BUF_LEN);
        match self
            .file
            .read_exact_at(&mut stack_buf[..first_len], ptr.offset as u64)
        {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(false),
            Err(e) => return Err(e),
        }

        if total_len <= STACK_BUF_LEN {
            return Ok(
                Self::verify_blob_record(&stack_buf[..total_len], ptr.crc32c, expected_key)
                    .is_some(),
            );
        }

        let Some((key_len, _)) = parse_and_check_blob_header(
            &stack_buf[..BLOB_RECORD_HEADER_LEN],
            total_len,
            ptr.crc32c,
            expected_key,
        ) else {
            return Ok(false);
        };

        let mut remaining_key = match expected_key {
            Some(key) => {
                let n = key_len.min(first_len - BLOB_RECORD_HEADER_LEN);
                if key[..n] != stack_buf[BLOB_RECORD_HEADER_LEN..BLOB_RECORD_HEADER_LEN + n] {
                    return Ok(false);
                }
                &key[n..]
            }
            None => &[][..],
        };
        let mut running_crc = crc32c(&stack_buf[8..first_len]);
        let mut file_pos = (ptr.offset as u64) + first_len as u64;
        let mut remaining = total_len - first_len;

        let mut heap_buf = Vec::new();
        let chunk_buf: &mut [u8] = if remaining > STACK_BUF_LEN {
            let cap = remaining.min(STREAM_CHUNK_LEN);
            if heap_buf.try_reserve_exact(cap).is_ok() {
                heap_buf.resize(cap, 0);
                &mut heap_buf
            } else {
                &mut stack_buf
            }
        } else {
            &mut stack_buf
        };

        while remaining > 0 {
            let to_read = remaining.min(chunk_buf.len());
            match self.file.read_exact_at(&mut chunk_buf[..to_read], file_pos) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(false),
                Err(e) => return Err(e),
            }
            let n = remaining_key.len().min(to_read);
            if remaining_key[..n] != chunk_buf[..n] {
                return Ok(false);
            }
            remaining_key = &remaining_key[n..];
            running_crc = crc32c_append(running_crc, &chunk_buf[..to_read]);
            file_pos += to_read as u64;
            remaining -= to_read;
        }

        Ok(running_crc == ptr.crc32c)
    }
}

#[cfg(test)]
mod tests;
