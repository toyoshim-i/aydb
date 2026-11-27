//! Layer 3 (Part 2): Immutable 4 KB Block SSTable Index Segments (`idx_<SeqID>.db`),
//! Sparse Fence Pointers, In-Heap Bloom Filters, and Multi-Way Merge Compaction.
//!
//! See `docs/phase1/04_WISCKEY_AND_SEGMENTS.md` for the detailed binary format.

use std::cmp::Ordering;
use std::io;
use std::sync::Arc;

pub mod bloom;
mod compaction;
mod cursor;
pub mod key_codec;
pub use bloom::BloomFilter;
pub use compaction::{CompactionResult, compact_idx_segments};
#[cfg(test)]
use cursor::ActiveBlock;
pub use cursor::{IdxSegmentCursor, IdxSegmentRevCursor};
pub use key_codec::{
    NS_LRU_QUEUE, NS_PRIMARY_ROW, NS_ROW_META, NS_SECONDARY_INDEX, RowMetaRecord,
    decode_bytes_escaped, decode_i64_memcomparable, decode_lru_queue_key, decode_primary_row_key,
    decode_secondary_index_key, decode_secondary_index_pk_slice, encode_bytes_escaped,
    encode_i64_memcomparable, encode_lru_queue_key, encode_namespace_prefix,
    encode_primary_row_key, encode_row_meta_key, encode_secondary_index_exact_prefix,
    encode_secondary_index_key, encode_secondary_index_prefix,
};

use crate::cache::VerifiedBlockCache;
use crate::meta::format_idx_file_name;
use crate::pal::{OpenHint, PlatformFile, PlatformFs};
use crate::util::byte_reader::ByteReader;
use crate::util::crc32c::crc32c;
use crate::util::{io_err, io_err_with_tag};
use crate::wal::{BlobKind, BlobPtr};

/// Size of an index data block (4096 bytes).
pub const IDX_BLOCK_SIZE: usize = 4096;

/// Write coalescing buffer capacity (`256 KB` = 64 data blocks, matching the Windows Cache
/// Manager Virtual Address Control Block size) used by [`IdxSegmentBuilder`].
const IDX_WRITE_BUFFER_CAPACITY: usize = 256 * 1024;

/// Size of the fixed trailer at the end of every 4 KB index data block (`4088..4096`).
const IDX_BLOCK_TRAILER_SIZE: usize = 8;

/// Usable byte region before the 8-byte trailer (`0..4088`).
const IDX_BLOCK_USABLE_SIZE: usize = IDX_BLOCK_SIZE - IDX_BLOCK_TRAILER_SIZE;

/// Maximum encoded size of a single `(key + header + inline_value)` before the 2-byte `u16` offset slot inside a 4 KB block.
pub const MAX_SINGLE_BLOCK_ENTRY_BYTES: usize = IDX_BLOCK_USABLE_SIZE - 2;

/// Encoded byte length of a `BlobPtr` inside an `IndexEntry` (`23` bytes).
pub const BLOB_PTR_ENCODED_LEN: usize = BlobPtr::ENCODED_LEN;

/// Default Bloom filter bits per key (`10` bits/key -> ~0.9% false-positive rate).
pub const DEFAULT_BLOOM_BITS_PER_KEY: usize = 10;

/// Maximum defensive size for an index segment's filter/fence metadata trailer (64 MB).
pub const MAX_INDEX_META_BYTES: usize = 64 * 1024 * 1024;

/// Fixed 48-byte footer at the end of every `idx_<SeqID>.db` file.
const IDX_FOOTER_SIZE: usize = 48;
const IDX_FOOTER_MAGIC: &[u8; 4] = b"AYIX";
const IDX_FORMAT_VERSION: u16 = 1;

/// Encoded `val_type` tag for inline values inside `idx_<SeqID>.db` blocks.
pub(crate) const VAL_TYPE_INLINE: u8 = 1;
/// Encoded `val_type` tag for 23-byte `BlobPtr` references inside `idx_<SeqID>.db` blocks.
pub(crate) const VAL_TYPE_BLOB: u8 = 2;
/// Encoded `val_type` tag for tombstone markers inside `idx_<SeqID>.db` blocks.
pub(crate) const VAL_TYPE_TOMBSTONE: u8 = 3;

/// Value stored for a key inside a `MemTable` or `idx_<SeqID>.db` data block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexValue {
    /// Small value (`< INLINE_THRESHOLD`) stored directly inside the index block.
    Inline(Vec<u8>),
    /// 23-byte pointer to a large value (`>= INLINE_THRESHOLD`) stored in `blob_*.db`.
    Blob(BlobPtr),
    /// Deletion marker (`DELETE`) hiding older versions of this key in older segments.
    Tombstone,
}

impl IndexValue {
    #[inline]
    pub fn encoded_payload_len(&self) -> usize {
        match self {
            Self::Inline(v) => 2 + v.len(),
            Self::Blob(_) => 23,
            Self::Tombstone => 0,
        }
    }
}

/// A complete key-value entry with its `LSN` inside an index segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexEntry {
    pub key: Vec<u8>,
    pub lsn: u64,
    pub value: IndexValue,
}

impl IndexEntry {
    /// Returns the checked total byte cost of encoding this entry + its 2-byte `u16` offset slot
    /// inside a 4096-byte data block, or `None` on arithmetic overflow.
    #[inline]
    pub fn checked_block_footprint_bytes(&self) -> Option<usize> {
        11usize
            .checked_add(self.key.len())
            .and_then(|n| n.checked_add(self.value.encoded_payload_len()))
            .and_then(|n| n.checked_add(2))
    }

    /// Returns the total byte cost of encoding this entry + its 2-byte `u16` offset slot
    /// inside a 4096-byte data block.
    #[inline]
    pub fn block_footprint_bytes(&self) -> usize {
        // 11B entry header (`key_len: u16 | type: u8 | lsn: u64`) + key + value_payload + 2B offset
        11usize
            .saturating_add(self.key.len())
            .saturating_add(self.value.encoded_payload_len())
            .saturating_add(2)
    }

    /// Returns `true` if an `IndexValue::Inline` entry with `(key_len, val_len)` fits in a single
    /// 4 KB index block (`11 + key_len + 2 + val_len <= MAX_SINGLE_BLOCK_ENTRY_BYTES`).
    #[inline]
    pub const fn inline_fits_in_block(key_len: usize, val_len: usize) -> bool {
        match key_len.checked_add(val_len) {
            Some(kv) => match kv.checked_add(13) {
                Some(total) => total <= MAX_SINGLE_BLOCK_ENTRY_BYTES,
                None => false,
            },
            None => false,
        }
    }

    /// Returns `true` if an `IndexValue::Blob` entry with `key_len` fits in a single 4 KB index
    /// block (`11 + key_len + 23 <= MAX_SINGLE_BLOCK_ENTRY_BYTES`).
    #[inline]
    pub const fn blob_ptr_fits_in_block(key_len: usize) -> bool {
        match key_len.checked_add(11 + BLOB_PTR_ENCODED_LEN) {
            Some(total) => total <= MAX_SINGLE_BLOCK_ENTRY_BYTES,
            None => false,
        }
    }

    /// Returns `true` if an `IndexValue::Tombstone` entry with `key_len` fits in a single 4 KB
    /// index block (`11 + key_len <= MAX_SINGLE_BLOCK_ENTRY_BYTES`).
    #[inline]
    pub const fn tombstone_fits_in_block(key_len: usize) -> bool {
        match key_len.checked_add(11) {
            Some(total) => total <= MAX_SINGLE_BLOCK_ENTRY_BYTES,
            None => false,
        }
    }
}

/// Builder for a single 4096-byte sorted index data block.
pub struct IdxBlockBuilder {
    buf: [u8; IDX_BLOCK_SIZE],
    write_pos: usize,
    offsets: Vec<u16>,
}

impl Default for IdxBlockBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl IdxBlockBuilder {
    pub fn new() -> Self {
        Self {
            buf: [0u8; IDX_BLOCK_SIZE],
            write_pos: 0,
            offsets: Vec::with_capacity(64),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    pub fn num_entries(&self) -> usize {
        self.offsets.len()
    }

    pub fn first_key(&self) -> Option<&[u8]> {
        if self.offsets.is_empty() {
            return None;
        }
        let klen = u16::from_le_bytes([self.buf[0], self.buf[1]]) as usize;
        Some(&self.buf[11..11 + klen])
    }

    pub fn last_key(&self) -> Option<&[u8]> {
        let start = *self.offsets.last()? as usize;
        let klen = u16::from_le_bytes([self.buf[start], self.buf[start + 1]]) as usize;
        Some(&self.buf[start + 11..start + 11 + klen])
    }

    /// Returns `true` if `entry` fits in the remaining space of this 4 KB block.
    pub fn can_fit(&self, entry: &IndexEntry) -> bool {
        let Some(needed) = entry.checked_block_footprint_bytes() else {
            return false;
        };
        let used = self.write_pos + (self.offsets.len() * 2);
        used.checked_add(needed)
            .is_some_and(|total| total <= IDX_BLOCK_USABLE_SIZE)
    }

    /// Returns `true` if a raw entry with `(key_len, val_type, val_len)` fits in this 4 KB block.
    #[inline]
    pub fn can_fit_raw(&self, key_len: usize, val_type: u8, val_len: usize) -> bool {
        let payload_len = match val_type {
            VAL_TYPE_INLINE => match val_len.checked_add(2) {
                Some(n) => n,
                None => return false,
            },
            VAL_TYPE_BLOB if val_len == BLOB_PTR_ENCODED_LEN => BLOB_PTR_ENCODED_LEN,
            VAL_TYPE_TOMBSTONE if val_len == 0 => 0,
            _ => return false,
        };
        let Some(needed) = 11usize
            .checked_add(key_len)
            .and_then(|n| n.checked_add(payload_len))
            .and_then(|n| n.checked_add(2))
        else {
            return false;
        };
        let used = self.write_pos + (self.offsets.len() * 2);
        used.checked_add(needed)
            .is_some_and(|total| total <= IDX_BLOCK_USABLE_SIZE)
    }

    fn validate_raw_entry_params(
        key_len: usize,
        val_type: u8,
        val_len: usize,
    ) -> io::Result<(u16, u16)> {
        let klen = u16::try_from(key_len)
            .map_err(|_| io_err(io::ErrorKind::InvalidInput, "key length exceeds u16::MAX"))?;
        let (vlen, payload_len) = match val_type {
            VAL_TYPE_INLINE => {
                let vlen = u16::try_from(val_len).map_err(|_| {
                    io_err(
                        io::ErrorKind::InvalidInput,
                        "inline value length exceeds u16::MAX",
                    )
                })?;
                let payload_len = val_len.checked_add(2).ok_or_else(|| {
                    io_err(
                        io::ErrorKind::InvalidInput,
                        "inline value footprint overflow",
                    )
                })?;
                (vlen, payload_len)
            }
            VAL_TYPE_BLOB => {
                if val_len != BLOB_PTR_ENCODED_LEN {
                    return Err(io_err(
                        io::ErrorKind::InvalidInput,
                        "BlobPtr raw value must be 23 bytes",
                    ));
                }
                (0, BLOB_PTR_ENCODED_LEN)
            }
            VAL_TYPE_TOMBSTONE => {
                if val_len != 0 {
                    return Err(io_err(
                        io::ErrorKind::InvalidInput,
                        "Tombstone raw value must be empty",
                    ));
                }
                (0, 0)
            }
            other => {
                return Err(io_err_with_tag(
                    io::ErrorKind::InvalidInput,
                    "unknown IndexValue type_tag",
                    other,
                ));
            }
        };
        let fits_single_block = 11usize
            .checked_add(key_len)
            .and_then(|n| n.checked_add(payload_len))
            .and_then(|n| n.checked_add(2))
            .is_some_and(|total| total <= IDX_BLOCK_USABLE_SIZE);
        if !fits_single_block {
            return Err(io_err(
                io::ErrorKind::InvalidInput,
                "IndexEntry exceeds single 4 KB block capacity",
            ));
        }
        Ok((klen, vlen))
    }

    fn check_key_order_and_start(&self, key: &[u8]) -> io::Result<u16> {
        if let Some(prev) = self.last_key() {
            if key <= prev {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "idx block keys must be added in strictly ascending order",
                ));
            }
        }
        u16::try_from(self.write_pos).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "idx block write offset exceeds u16::MAX",
            )
        })
    }

    /// Appends a raw borrowed entry (`key`, `lsn`, `val_type`, `val_bytes`) directly to this block.
    pub fn add_raw_entry(
        &mut self,
        key: &[u8],
        lsn: u64,
        val_type: u8,
        val_bytes: &[u8],
    ) -> io::Result<()> {
        let (klen, vlen) = Self::validate_raw_entry_params(key.len(), val_type, val_bytes.len())?;
        if val_type == VAL_TYPE_BLOB {
            BlobKind::from_u8(val_bytes[0])?;
        }
        if !self.can_fit_raw(key.len(), val_type, val_bytes.len()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "RawIdxEntry exceeds remaining 4 KB block capacity",
            ));
        }
        let start_u16 = self.check_key_order_and_start(key)?;

        let start = self.write_pos;
        self.offsets.push(start_u16);

        self.buf[start..start + 2].copy_from_slice(&klen.to_le_bytes());
        self.buf[start + 2] = val_type;
        self.buf[start + 3..start + 11].copy_from_slice(&lsn.to_le_bytes());
        let mut pos = start + 11;

        self.buf[pos..pos + key.len()].copy_from_slice(key);
        pos += key.len();

        match val_type {
            VAL_TYPE_INLINE => {
                self.buf[pos..pos + 2].copy_from_slice(&vlen.to_le_bytes());
                pos += 2;
                self.buf[pos..pos + val_bytes.len()].copy_from_slice(val_bytes);
                pos += val_bytes.len();
            }
            VAL_TYPE_BLOB => {
                self.buf[pos..pos + BLOB_PTR_ENCODED_LEN].copy_from_slice(val_bytes);
                pos += BLOB_PTR_ENCODED_LEN;
            }
            VAL_TYPE_TOMBSTONE => {}
            _ => unreachable!("val_type validated above"),
        }

        self.write_pos = pos;
        Ok(())
    }

    /// Appends `entry` (which must be in strictly ascending key order) to this block.
    pub fn add_entry(&mut self, entry: &IndexEntry) -> io::Result<()> {
        match &entry.value {
            IndexValue::Inline(v) => self.add_raw_entry(&entry.key, entry.lsn, VAL_TYPE_INLINE, v),
            IndexValue::Blob(ptr) => {
                self.add_raw_entry(&entry.key, entry.lsn, VAL_TYPE_BLOB, &ptr.to_bytes())
            }
            IndexValue::Tombstone => {
                self.add_raw_entry(&entry.key, entry.lsn, VAL_TYPE_TOMBSTONE, &[])
            }
        }
    }

    /// Seals the 4 KB block in place without resetting `self`.
    fn seal_in_place(&mut self) {
        let count = self.offsets.len();
        let dir_start = IDX_BLOCK_USABLE_SIZE - (count * 2);

        // Zero-fill padding between last entry and offset directory
        self.buf[self.write_pos..dir_start].fill(0);

        for (i, &off) in self.offsets.iter().enumerate() {
            let slot = dir_start + (i * 2);
            self.buf[slot..slot + 2].copy_from_slice(&off.to_le_bytes());
        }

        self.buf[4088..4090].copy_from_slice(&(count as u16).to_le_bytes());
        self.buf[4090..4092].copy_from_slice(&0u16.to_le_bytes());
        let crc = crc32c(&self.buf[0..4092]);
        self.buf[4092..4096].copy_from_slice(&crc.to_le_bytes());
    }

    /// Seals the 4 KB block, appends it directly to `dst`, and resets `self` for the next block
    /// without returning a 4 KB stack array or zeroing the entire scratch buffer.
    pub fn finish_into(&mut self, dst: &mut Vec<u8>) {
        self.seal_in_place();
        dst.extend_from_slice(&self.buf);
        self.write_pos = 0;
        self.offsets.clear();
    }

    /// Seals the 4 KB block by writing the `u16` Entry Offset Directory (`4088 - 2*K .. 4088`),
    /// the entry count, and the `block_crc32c` trailer (`4092..4096`), then resets `self`.
    pub fn finish_and_reset(&mut self) -> [u8; IDX_BLOCK_SIZE] {
        self.seal_in_place();
        let finished = self.buf;
        self.write_pos = 0;
        self.offsets.clear();
        finished
    }
}

/// Zero-copy borrowed view of an entry inside a verified 4 KB index data block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RawIdxEntry<'a> {
    pub key: &'a [u8],
    pub lsn: u64,
    pub val_type: u8,
    pub val_bytes: &'a [u8],
}

impl<'a> RawIdxEntry<'a> {
    /// Decodes a 23-byte encoded [`BlobPtr`] slice without heap allocation.
    #[inline]
    pub fn decode_blob_ptr(val_bytes: &[u8]) -> io::Result<BlobPtr> {
        BlobPtr::from_bytes(val_bytes)
    }

    /// Decodes the value payload of this entry into an owned [`IndexValue`].
    pub fn decode_value(&self) -> io::Result<IndexValue> {
        match self.val_type {
            VAL_TYPE_INLINE => Ok(IndexValue::Inline(self.val_bytes.to_vec())),
            VAL_TYPE_BLOB => Ok(IndexValue::Blob(Self::decode_blob_ptr(self.val_bytes)?)),
            VAL_TYPE_TOMBSTONE => Ok(IndexValue::Tombstone),
            other => Err(io_err_with_tag(
                io::ErrorKind::InvalidData,
                "unknown IndexValue type_tag",
                other,
            )),
        }
    }

    /// Materializes a full [`IndexEntry`] from this borrowed view.
    #[inline]
    pub fn to_owned_entry(&self) -> io::Result<IndexEntry> {
        Ok(IndexEntry {
            key: self.key.to_vec(),
            lsn: self.lsn,
            value: self.decode_value()?,
        })
    }
}

#[derive(Debug, Clone)]
pub(super) struct CursorEntrySpan {
    pub(super) key_range: core::ops::Range<usize>,
    pub(super) lsn: u64,
    pub(super) val_type: u8,
    pub(super) val_range: core::ops::Range<usize>,
}

/// Minimum encoded header size of an entry in a 4 KB data block (`klen(2) + type(1) + lsn(8)`).
const MIN_IDX_ENTRY_BYTES: usize = 11;

/// Maximum number of entries that can fit in a 4 KB data block (`11B` entry + `2B` directory slot).
const MAX_IDX_BLOCK_ENTRIES: usize = IDX_BLOCK_USABLE_SIZE / (MIN_IDX_ENTRY_BYTES + 2);

/// Zero-copy reader for a single 4096-byte index data block.
pub struct IdxBlockReader<'a> {
    buf: &'a [u8],
    num_entries: usize,
    dir_start: usize,
}

fn validate_block_entry_layout(buf: &[u8], num_entries: usize, dir_start: usize) -> io::Result<()> {
    let mut prev_end = 0usize;
    let mut prev_key: Option<&[u8]> = None;
    for i in 0..num_entries {
        let dir_pos = dir_start + (i * 2);
        let off = u16::from_le_bytes([buf[dir_pos], buf[dir_pos + 1]]) as usize;
        if off < prev_end {
            return Err(io_err(
                io::ErrorKind::InvalidData,
                "overlapping or out-of-order entry offsets in 4 KB index block",
            ));
        }
        let Some(kstart) = off.checked_add(MIN_IDX_ENTRY_BYTES) else {
            return Err(io_err(
                io::ErrorKind::InvalidData,
                "corrupted entry offset in 4 KB index block",
            ));
        };
        if kstart > dir_start {
            return Err(io_err(
                io::ErrorKind::InvalidData,
                "corrupted entry offset in 4 KB index block",
            ));
        }
        let klen = u16::from_le_bytes([buf[off], buf[off + 1]]) as usize;
        let val_type = buf[off + 2];
        let Some(kend) = kstart.checked_add(klen) else {
            return Err(io_err(
                io::ErrorKind::InvalidData,
                "corrupted key length in 4 KB index block",
            ));
        };
        if kend > dir_start {
            return Err(io_err(
                io::ErrorKind::InvalidData,
                "corrupted key slice in 4 KB index block",
            ));
        }
        let key = &buf[kstart..kend];
        if prev_key.is_some_and(|prev| key <= prev) {
            return Err(io_err(
                io::ErrorKind::InvalidData,
                "unsorted or duplicate keys in 4 KB index block",
            ));
        }
        prev_key = Some(key);
        let vend = match val_type {
            VAL_TYPE_INLINE => {
                let Some(vstart) = kend.checked_add(2) else {
                    return Err(io_err(
                        io::ErrorKind::InvalidData,
                        "truncated inline value length",
                    ));
                };
                if vstart > dir_start {
                    return Err(io_err(
                        io::ErrorKind::InvalidData,
                        "truncated inline value length",
                    ));
                }
                let vlen = u16::from_le_bytes([buf[kend], buf[kend + 1]]) as usize;
                let Some(vend) = vstart.checked_add(vlen) else {
                    return Err(io_err(
                        io::ErrorKind::InvalidData,
                        "truncated inline value payload",
                    ));
                };
                vend
            }
            VAL_TYPE_BLOB => {
                let Some(vend) = kend.checked_add(BLOB_PTR_ENCODED_LEN) else {
                    return Err(io_err(
                        io::ErrorKind::InvalidData,
                        "truncated BlobPtr in 4 KB index block",
                    ));
                };
                if vend <= dir_start {
                    BlobKind::from_u8(buf[kend])?;
                }
                vend
            }
            VAL_TYPE_TOMBSTONE => kend,
            other => {
                return Err(io_err_with_tag(
                    io::ErrorKind::InvalidData,
                    "unknown IndexValue type_tag",
                    other,
                ));
            }
        };
        if vend > dir_start {
            return Err(io_err(
                io::ErrorKind::InvalidData,
                "entry extends past 4 KB index block directory",
            ));
        }
        prev_end = vend;
    }
    Ok(())
}

impl<'a> IdxBlockReader<'a> {
    /// Verifies `block_crc32c` and structural bounds of a 4096-byte data block.
    pub fn open_verified(buf: &'a [u8; IDX_BLOCK_SIZE]) -> io::Result<Self> {
        let expected_crc = u32::from_le_bytes([buf[4092], buf[4093], buf[4094], buf[4095]]);
        let actual_crc = crc32c(&buf[0..4092]);
        if actual_crc != expected_crc {
            return Err(io_err(
                io::ErrorKind::InvalidData,
                "4 KB index data block CRC32C mismatch",
            ));
        }

        let num_entries = u16::from_le_bytes([buf[4088], buf[4089]]) as usize;
        if num_entries > MAX_IDX_BLOCK_ENTRIES {
            return Err(io_err(
                io::ErrorKind::InvalidData,
                "corrupted num_entries in 4 KB index data block",
            ));
        }
        let dir_start = IDX_BLOCK_USABLE_SIZE - (num_entries * 2);
        validate_block_entry_layout(buf, num_entries, dir_start)?;
        Ok(Self {
            buf,
            num_entries,
            dir_start,
        })
    }

    /// Structural bounds verification for an already-verified 4 KB index data block (skips CRC32C recalculation).
    pub fn open_preverified(buf: &'a [u8; IDX_BLOCK_SIZE]) -> io::Result<Self> {
        let num_entries = u16::from_le_bytes([buf[4088], buf[4089]]) as usize;
        if num_entries > MAX_IDX_BLOCK_ENTRIES {
            return Err(io_err(
                io::ErrorKind::InvalidData,
                "corrupted num_entries in 4 KB index data block",
            ));
        }
        let dir_start = IDX_BLOCK_USABLE_SIZE - (num_entries * 2);
        Ok(Self {
            buf,
            num_entries,
            dir_start,
        })
    }

    #[inline]
    fn from_verified_buf(buf: &'a [u8; IDX_BLOCK_SIZE], num_entries: usize) -> Self {
        let dir_start = IDX_BLOCK_USABLE_SIZE - (num_entries * 2);
        Self {
            buf,
            num_entries,
            dir_start,
        }
    }

    #[cfg(kani)]
    fn from_slice_for_kani(buf: &'a [u8], num_entries: usize, dir_start: usize) -> Self {
        assert!(dir_start + num_entries * 2 <= buf.len());
        Self {
            buf,
            num_entries,
            dir_start,
        }
    }

    #[inline]
    pub fn num_entries(&self) -> usize {
        self.num_entries
    }

    #[inline]
    fn entry_offset(&self, index: usize) -> io::Result<usize> {
        if index >= self.num_entries {
            return Err(io_err(
                io::ErrorKind::InvalidInput,
                "entry index out of bounds in 4 KB index block",
            ));
        }
        let pos = self.dir_start + (index * 2);
        Ok(u16::from_le_bytes([self.buf[pos], self.buf[pos + 1]]) as usize)
    }

    /// Returns the raw key slice of the entry at `index` without allocating or decoding the value.
    #[inline]
    fn key_at(&self, index: usize) -> io::Result<&'a [u8]> {
        let off = self.entry_offset(index)?;
        if off + 11 > self.dir_start {
            return Err(io_err(
                io::ErrorKind::InvalidData,
                "corrupted entry offset in 4 KB index block",
            ));
        }
        let klen = u16::from_le_bytes([self.buf[off], self.buf[off + 1]]) as usize;
        let kstart = off + 11;
        if kstart + klen > self.dir_start {
            return Err(io_err(
                io::ErrorKind::InvalidData,
                "corrupted key_len in 4 KB index block",
            ));
        }
        Ok(&self.buf[kstart..kstart + klen])
    }

    /// Parses the byte ranges of the entry at `index` inside `self.buf` with zero heap allocations.
    #[inline]
    pub(super) fn parse_entry_span_at(&self, index: usize) -> io::Result<CursorEntrySpan> {
        let off = self.entry_offset(index)?;
        if off + 11 > self.dir_start {
            return Err(io_err(
                io::ErrorKind::InvalidData,
                "corrupted entry offset in 4 KB index block",
            ));
        }
        let klen = u16::from_le_bytes([self.buf[off], self.buf[off + 1]]) as usize;
        let val_type = self.buf[off + 2];
        let lsn = u64::from_le_bytes([
            self.buf[off + 3],
            self.buf[off + 4],
            self.buf[off + 5],
            self.buf[off + 6],
            self.buf[off + 7],
            self.buf[off + 8],
            self.buf[off + 9],
            self.buf[off + 10],
        ]);
        let kstart = off + 11;
        let kend = kstart + klen;
        if kend > self.dir_start {
            return Err(io_err(
                io::ErrorKind::InvalidData,
                "corrupted key slice in 4 KB index block",
            ));
        }
        let val_range = match val_type {
            VAL_TYPE_INLINE => {
                if kend + 2 > self.dir_start {
                    return Err(io_err(
                        io::ErrorKind::InvalidData,
                        "truncated inline value length",
                    ));
                }
                let vlen = u16::from_le_bytes([self.buf[kend], self.buf[kend + 1]]) as usize;
                let vstart = kend + 2;
                let vend = vstart + vlen;
                if vend > self.dir_start {
                    return Err(io_err(
                        io::ErrorKind::InvalidData,
                        "truncated inline value payload",
                    ));
                }
                vstart..vend
            }
            VAL_TYPE_BLOB => {
                let vend = kend + BLOB_PTR_ENCODED_LEN;
                if vend > self.dir_start {
                    return Err(io_err(
                        io::ErrorKind::InvalidData,
                        "truncated BlobPtr in 4 KB index block",
                    ));
                }
                BlobKind::from_u8(self.buf[kend])?;
                kend..vend
            }
            VAL_TYPE_TOMBSTONE => kend..kend,
            other => {
                return Err(io_err_with_tag(
                    io::ErrorKind::InvalidData,
                    "unknown IndexValue type_tag",
                    other,
                ));
            }
        };
        Ok(CursorEntrySpan {
            key_range: kstart..kend,
            lsn,
            val_type,
            val_range,
        })
    }

    /// Returns a zero-copy [`RawIdxEntry`] at `index` borrowed directly from `self.buf`.
    #[inline]
    pub fn raw_entry_at(&self, index: usize) -> io::Result<RawIdxEntry<'a>> {
        let span = self.parse_entry_span_at(index)?;
        Ok(RawIdxEntry {
            key: &self.buf[span.key_range],
            lsn: span.lsn,
            val_type: span.val_type,
            val_bytes: &self.buf[span.val_range],
        })
    }

    /// Decodes the full [`IndexEntry`] at `index`.
    pub fn decode_entry_at(&self, index: usize) -> io::Result<IndexEntry> {
        self.raw_entry_at(index)?.to_owned_entry()
    }

    /// Searches for `target_key` in $O(\log K)$ time using the downward-growing `u16` offset directory.
    pub fn lookup_binary_search(&self, target_key: &[u8]) -> io::Result<Option<(u64, IndexValue)>> {
        match self.find_raw(target_key)? {
            Some(raw) => Ok(Some((raw.lsn, raw.decode_value()?))),
            None => Ok(None),
        }
    }

    /// Binary-searches for `target_key` and returns the matching entry borrowed from the block,
    /// without decoding or copying its value.
    pub fn find_raw(&self, target_key: &[u8]) -> io::Result<Option<RawIdxEntry<'a>>> {
        let mut low = 0usize;
        let mut high = self.num_entries;
        while low < high {
            let mid = low + (high - low) / 2;
            let k = self.key_at(mid)?;
            match k.cmp(target_key) {
                Ordering::Less => low = mid + 1,
                Ordering::Greater => high = mid,
                Ordering::Equal => return Ok(Some(self.raw_entry_at(mid)?)),
            }
        }
        Ok(None)
    }

    /// Searches for `target_key` in $O(K)$ time by scanning entries sequentially from index `0`.
    /// Provided to empirically benchmark the speedup of the `u16` offset directory!
    pub fn lookup_linear_scan(&self, target_key: &[u8]) -> io::Result<Option<(u64, IndexValue)>> {
        for i in 0..self.num_entries {
            let k = self.key_at(i)?;
            match k.cmp(target_key) {
                Ordering::Less => continue,
                Ordering::Equal => {
                    let entry = self.decode_entry_at(i)?;
                    return Ok(Some((entry.lsn, entry.value)));
                }
                Ordering::Greater => break,
            }
        }
        Ok(None)
    }
}

/// Sparse Fence Pointer stored in heap memory for each 4 KB data block of an `idx_<SeqID>.db` file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FencePointer {
    pub first_key: Box<[u8]>,
    pub block_idx: u32,
}

/// Writes a sorted stream of [`IndexEntry`] items into a new immutable `idx_<SeqID>.db` file.
pub struct IdxSegmentBuilder<F: PlatformFile> {
    file: Arc<F>,
    seq_id: u64,
    level: u16,
    bloom_bits_per_key: usize,
    cap_bloom_to_meta_budget: bool,
    block_builder: IdxBlockBuilder,
    fence_pointers: Vec<FencePointer>,
    bloom_hashes: Vec<u64>,
    last_key: Vec<u8>,
    num_blocks: u32,
    record_count: u32,
    write_offset: u64,
    buf_start_offset: u64,
    write_buf: Vec<u8>,
}

impl<F: PlatformFile> IdxSegmentBuilder<F> {
    pub fn create(
        fs: &impl PlatformFs<File = F>,
        seq_id: u64,
        level: u16,
        bloom_bits_per_key: usize,
    ) -> io::Result<Self> {
        let fname = format_idx_file_name(seq_id);
        let file = fs.open_file(&fname, true, true, OpenHint::ImmutableRandom)?;
        Ok(Self {
            file,
            seq_id,
            level,
            bloom_bits_per_key,
            cap_bloom_to_meta_budget: false,
            block_builder: IdxBlockBuilder::new(),
            fence_pointers: Vec::new(),
            bloom_hashes: Vec::new(),
            last_key: Vec::new(),
            num_blocks: 0,
            record_count: 0,
            write_offset: 0,
            buf_start_offset: 0,
            write_buf: Vec::with_capacity(IDX_WRITE_BUFFER_CAPACITY),
        })
    }

    /// Configures whether [`Self::finish`] caps the Bloom bitset to the remaining
    /// `MAX_INDEX_META_BYTES` budget rather than failing if `bloom_bits_per_key` would overflow it.
    pub fn set_cap_bloom_to_meta_budget(&mut self, cap: bool) {
        self.cap_bloom_to_meta_budget = cap;
    }

    /// Adds a sorted [`IndexEntry`] to the segment, flushing a 4 KB data block whenever full.
    pub fn add_entry(&mut self, entry: &IndexEntry) -> io::Result<()> {
        match &entry.value {
            IndexValue::Inline(v) => self.add_raw_entry(&entry.key, entry.lsn, VAL_TYPE_INLINE, v),
            IndexValue::Blob(ptr) => {
                self.add_raw_entry(&entry.key, entry.lsn, VAL_TYPE_BLOB, &ptr.to_bytes())
            }
            IndexValue::Tombstone => {
                self.add_raw_entry(&entry.key, entry.lsn, VAL_TYPE_TOMBSTONE, &[])
            }
        }
    }

    /// Adds a sorted raw entry (`key`, `lsn`, `val_type`, `val_bytes`) directly to the segment
    /// without constructing an intermediate [`IndexEntry`].
    pub fn add_raw_entry(
        &mut self,
        key: &[u8],
        lsn: u64,
        val_type: u8,
        val_bytes: &[u8],
    ) -> io::Result<()> {
        IdxBlockBuilder::validate_raw_entry_params(key.len(), val_type, val_bytes.len())?;
        if val_type == 2 {
            BlobKind::from_u8(val_bytes[0])?;
        }
        if self.record_count > 0 && key <= self.last_key.as_slice() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "idx segment keys must be added in strictly ascending order",
            ));
        }
        let next_count = self.record_count.checked_add(1).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "idx segment record count exceeds u32::MAX",
            )
        })?;
        if !self.block_builder.is_empty()
            && !self
                .block_builder
                .can_fit_raw(key.len(), val_type, val_bytes.len())
        {
            self.flush_current_data_block()?;
        }
        self.block_builder
            .add_raw_entry(key, lsn, val_type, val_bytes)?;
        if self.bloom_bits_per_key > 0 {
            self.bloom_hashes.push(BloomFilter::hash64(key));
        }
        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.record_count = next_count;
        Ok(())
    }

    #[inline]
    fn flush_write_buf(&mut self) -> io::Result<()> {
        if !self.write_buf.is_empty() {
            self.file
                .write_all_at(&self.write_buf, self.buf_start_offset)?;
            self.buf_start_offset += self.write_buf.len() as u64;
            self.write_buf.clear();
        }
        Ok(())
    }

    fn flush_current_data_block(&mut self) -> io::Result<()> {
        if self.block_builder.is_empty() {
            return Ok(());
        }
        let next_num_blocks = self.num_blocks.checked_add(1).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "idx segment block count exceeds u32::MAX",
            )
        })?;
        let first_key = self
            .block_builder
            .first_key()
            .expect("non-empty block must have first_key")
            .to_vec()
            .into_boxed_slice();
        let block_idx = self.num_blocks;
        if self.write_buf.is_empty() {
            self.buf_start_offset = self.write_offset;
        }
        self.block_builder.finish_into(&mut self.write_buf);
        self.write_offset += IDX_BLOCK_SIZE as u64;
        self.num_blocks = next_num_blocks;
        self.fence_pointers.push(FencePointer {
            first_key,
            block_idx,
        });
        if self.write_buf.len() >= IDX_WRITE_BUFFER_CAPACITY {
            self.flush_write_buf()?;
        }
        Ok(())
    }

    /// Finalizes the segment by flushing the last 4 KB data block, writing the Bloom filter
    /// and Sparse Fence Pointer section, writing the 48-byte footer, and syncing to disk.
    pub fn finish(mut self) -> io::Result<IdxSegmentReader<F>> {
        self.flush_current_data_block()?;
        self.flush_write_buf()?;

        let fp_count_u32 = u32::try_from(self.fence_pointers.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "idx fence pointer count exceeds u32::MAX",
            )
        })?;
        let mut fence_and_last_bytes = 4usize;
        for fp in &self.fence_pointers {
            let _ = u16::try_from(fp.first_key.len()).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "idx fence pointer key exceeds u16::MAX",
                )
            })?;
            fence_and_last_bytes = fence_and_last_bytes
                .checked_add(6 + fp.first_key.len())
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "idx metadata size overflow")
                })?;
        }
        let last_klen_u16 = u16::try_from(self.last_key.len()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "idx last_key exceeds u16::MAX")
        })?;
        fence_and_last_bytes = fence_and_last_bytes
            .checked_add(2 + self.last_key.len())
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "idx metadata size overflow")
            })?;

        const BLOOM_HEADER_BYTES: usize = 8;
        let non_bloom_bytes = fence_and_last_bytes
            .checked_add(BLOOM_HEADER_BYTES)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "idx metadata size overflow")
            })?;
        let max_bloom_bytes = MAX_INDEX_META_BYTES
            .checked_sub(non_bloom_bytes)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "idx segment metadata exceeds MAX_INDEX_META_BYTES",
                )
            })?;

        // Build BloomFilter within the remaining metadata budget (or fail before allocating if
        // capping is disabled) and drop the hash vector immediately.
        let bloom_hashes = core::mem::take(&mut self.bloom_hashes);
        let bloom = if self.cap_bloom_to_meta_budget {
            BloomFilter::from_hashes64_bounded(
                &bloom_hashes,
                self.bloom_bits_per_key,
                max_bloom_bytes,
            )
        } else {
            let desired =
                BloomFilter::desired_byte_len(bloom_hashes.len(), self.bloom_bits_per_key);
            if desired > max_bloom_bytes {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "idx segment metadata exceeds MAX_INDEX_META_BYTES",
                ));
            }
            BloomFilter::from_hashes64(&bloom_hashes, self.bloom_bits_per_key)
        };
        drop(bloom_hashes);

        // Encode Filter + Sparse Fence Pointers + max_key metadata section
        let meta_offset = self.write_offset;
        let expected_meta_len =
            non_bloom_bytes
                .checked_add(bloom.heap_bytes())
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "idx metadata size overflow")
                })?;
        if expected_meta_len > MAX_INDEX_META_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "idx segment metadata exceeds MAX_INDEX_META_BYTES",
            ));
        }
        let expected_total_len =
            expected_meta_len
                .checked_add(IDX_FOOTER_SIZE)
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "idx metadata and footer size overflow",
                    )
                })?;
        let mut meta_buf = Vec::new();
        meta_buf
            .try_reserve_exact(expected_total_len)
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "failed to allocate idx segment metadata buffer",
                )
            })?;
        bloom.encode(&mut meta_buf);
        meta_buf.extend_from_slice(&fp_count_u32.to_le_bytes());
        for fp in &self.fence_pointers {
            let klen_u16 = fp.first_key.len() as u16;
            meta_buf.extend_from_slice(&klen_u16.to_le_bytes());
            meta_buf.extend_from_slice(&fp.block_idx.to_le_bytes());
            meta_buf.extend_from_slice(&fp.first_key);
        }
        meta_buf.extend_from_slice(&last_klen_u16.to_le_bytes());
        meta_buf.extend_from_slice(&self.last_key);
        if meta_buf.len() > MAX_INDEX_META_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "idx segment metadata exceeds MAX_INDEX_META_BYTES",
            ));
        }
        let meta_len = u32::try_from(meta_buf.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "idx segment metadata exceeds u32::MAX",
            )
        })?;
        let meta_crc = crc32c(&meta_buf);

        // Encode 48-byte Footer
        let mut footer = [0u8; IDX_FOOTER_SIZE];
        footer[0..4].copy_from_slice(IDX_FOOTER_MAGIC);
        footer[4..6].copy_from_slice(&IDX_FORMAT_VERSION.to_le_bytes());
        footer[6..8].copy_from_slice(&self.level.to_le_bytes());
        footer[8..16].copy_from_slice(&self.seq_id.to_le_bytes());
        footer[16..20].copy_from_slice(&self.num_blocks.to_le_bytes());
        footer[20..24].copy_from_slice(&self.record_count.to_le_bytes());
        footer[24..32].copy_from_slice(&meta_offset.to_le_bytes());
        footer[32..36].copy_from_slice(&meta_len.to_le_bytes());
        footer[36..40].copy_from_slice(&meta_crc.to_le_bytes());
        footer[40..44].copy_from_slice(&0u32.to_le_bytes()); // reserved
        let footer_crc = crc32c(&footer[0..44]);
        footer[44..48].copy_from_slice(&footer_crc.to_le_bytes());

        meta_buf.extend_from_slice(&footer);
        debug_assert_eq!(meta_buf.capacity(), expected_total_len);
        self.file.write_all_at(&meta_buf, meta_offset)?;
        self.write_offset += meta_buf.len() as u64;
        self.file.sync_durable()?;

        let min_key = self
            .fence_pointers
            .first()
            .map(|fp| fp.first_key.to_vec())
            .unwrap_or_default();

        Ok(IdxSegmentReader {
            file: self.file,
            seq_id: self.seq_id,
            level: self.level,
            file_len: self.write_offset,
            num_blocks: self.num_blocks,
            record_count: self.record_count,
            bloom,
            fence_pointers: self.fence_pointers,
            min_key,
            max_key: self.last_key,
            block_cache: None,
        })
    }
}

/// Low-memory reader for an immutable `idx_<SeqID>.db` segment.
///
/// Keeps ONLY the [`BloomFilter`] and [`FencePointer`] array in heap memory, fetching
/// 4 KB data blocks on demand from the OS Page Cache via positional `read_exact_at`.
pub struct IdxSegmentReader<F: PlatformFile> {
    pub(super) file: Arc<F>,
    pub(super) seq_id: u64,
    level: u16,
    file_len: u64,
    pub(super) num_blocks: u32,
    record_count: u32,
    bloom: BloomFilter,
    pub(super) fence_pointers: Vec<FencePointer>,
    min_key: Vec<u8>,
    max_key: Vec<u8>,
    block_cache: Option<Arc<VerifiedBlockCache>>,
}

impl<F: PlatformFile> IdxSegmentReader<F> {
    /// Opens an existing `idx_<SeqID>.db` file, loading only its 48-byte footer, Bloom filter,
    /// and Sparse Fence Pointers into heap memory.
    pub fn open(fs: &impl PlatformFs<File = F>, seq_id: u64) -> io::Result<Self> {
        let fname = format_idx_file_name(seq_id);
        let file = fs.open_file(&fname, false, false, OpenHint::ImmutableRandom)?;
        let file_len = file.file_len()?;
        if file_len < IDX_FOOTER_SIZE as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "idx segment file shorter than 48-byte footer",
            ));
        }

        let mut footer = [0u8; IDX_FOOTER_SIZE];
        file.read_exact_at(&mut footer, file_len - (IDX_FOOTER_SIZE as u64))?;

        let mut fr = ByteReader::new(&footer);
        if fr.read_bytes(4)? != IDX_FOOTER_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid magic in idx segment footer",
            ));
        }
        let expected_footer_crc = u32::from_le_bytes(footer[44..48].try_into().unwrap());
        if crc32c(&footer[0..44]) != expected_footer_crc {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "idx segment footer CRC32C mismatch",
            ));
        }

        let version = fr.read_u16_le()?;
        if version != IDX_FORMAT_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported idx format version: {version}"),
            ));
        }

        let level = fr.read_u16_le()?;
        let stored_seq_id = fr.read_u64_le()?;
        let num_blocks = fr.read_u32_le()?;
        let record_count = fr.read_u32_le()?;
        let meta_offset = fr.read_u64_le()?;
        let meta_len = fr.read_u32_le()? as usize;
        let expected_meta_crc = fr.read_u32_le()?;

        let expected_meta_offset = (num_blocks as u64).checked_mul(IDX_BLOCK_SIZE as u64);
        let expected_file_len = meta_offset
            .checked_add(meta_len as u64)
            .and_then(|v| v.checked_add(IDX_FOOTER_SIZE as u64));

        if stored_seq_id != seq_id
            || (num_blocks == 0) != (record_count == 0)
            || expected_meta_offset != Some(meta_offset)
            || expected_file_len != Some(file_len)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "corrupted offsets in idx segment footer",
            ));
        }

        if meta_len > MAX_INDEX_META_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "idx segment metadata exceeds maximum allowed size",
            ));
        }

        let mut meta_buf = Vec::new();
        meta_buf.try_reserve_exact(meta_len).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "failed to allocate idx segment metadata buffer",
            )
        })?;
        meta_buf.resize(meta_len, 0u8);
        file.read_exact_at(&mut meta_buf, meta_offset)?;
        if crc32c(&meta_buf) != expected_meta_crc {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "idx segment filter/fence metadata CRC32C mismatch",
            ));
        }

        let mut r = ByteReader::new(&meta_buf);
        let bloom = BloomFilter::decode_from(&mut r)?;
        if r.remaining() < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "truncated fence pointer count in idx segment",
            ));
        }
        let fp_count = r.read_u32_le()? as usize;

        // Validate `fp_count` against the number of 4 KB data blocks (`1` fence pointer per block)
        // and the remaining bytes in `r` (`>= 6` bytes per fence pointer) BEFORE allocating.
        let min_fp_bytes = fp_count.checked_mul(6).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "fence pointer count byte size overflow",
            )
        })?;
        if fp_count != num_blocks as usize || min_fp_bytes > r.remaining() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "corrupted or out-of-bounds fence pointer count in idx segment",
            ));
        }

        let mut fence_pointers: Vec<FencePointer> = Vec::new();
        fence_pointers.try_reserve_exact(fp_count).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "failed to reserve fence pointers",
            )
        })?;
        for expected_block_idx in 0..fp_count {
            if r.remaining() < 6 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "truncated fence pointer entry",
                ));
            }
            let klen = r.read_u16_le()? as usize;
            let block_idx = r.read_u32_le()?;
            if block_idx != expected_block_idx as u32 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "non-sequential fence pointer block_idx in idx segment",
                ));
            }
            if r.remaining() < klen {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "truncated fence pointer key",
                ));
            }
            let first_key = r.read_bytes(klen)?.to_vec().into_boxed_slice();
            if let Some(prev) = fence_pointers.last() {
                if first_key.as_ref() <= prev.first_key.as_ref() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "unsorted fence pointers in idx segment",
                    ));
                }
            }
            fence_pointers.push(FencePointer {
                first_key,
                block_idx,
            });
        }

        let min_key = fence_pointers
            .first()
            .map(|fp| fp.first_key.to_vec())
            .unwrap_or_default();
        let max_key = if r.remaining() < 2 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "missing or truncated max_key in idx segment metadata",
            ));
        } else {
            let max_klen = r.read_u16_le()? as usize;
            if r.remaining() != max_klen {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "truncated or trailing bytes in idx segment max_key",
                ));
            }
            let max_slice = r.read_bytes(max_klen)?;
            if num_blocks == 0 && !max_slice.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "empty idx segment must have empty max_key",
                ));
            }
            if let Some(last_fp) = fence_pointers.last() {
                if max_slice < last_fp.first_key.as_ref() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "idx segment max_key precedes last fence pointer",
                    ));
                }
            }
            max_slice.to_vec()
        };

        Ok(Self {
            file,
            seq_id,
            level,
            file_len,
            num_blocks,
            record_count,
            bloom,
            fence_pointers,
            min_key,
            max_key,
            block_cache: None,
        })
    }

    pub fn seq_id(&self) -> u64 {
        self.seq_id
    }

    pub fn level(&self) -> u16 {
        self.level
    }

    pub fn file_len(&self) -> u64 {
        self.file_len
    }

    pub fn num_blocks(&self) -> u32 {
        self.num_blocks
    }

    pub fn record_count(&self) -> u32 {
        self.record_count
    }

    /// Returns the smallest key stored in this segment (empty if the segment has no entries).
    #[inline]
    pub fn min_key(&self) -> &[u8] {
        &self.min_key
    }

    /// Returns the largest key stored in this segment (empty if the segment has no entries).
    #[inline]
    pub fn max_key(&self) -> &[u8] {
        &self.max_key
    }

    /// Returns `true` if `key` lies within `[self.min_key, self.max_key]`.
    #[inline]
    pub fn may_contain_key(&self, key: &[u8]) -> bool {
        !self.fence_pointers.is_empty()
            && key >= self.min_key.as_slice()
            && key <= self.max_key.as_slice()
    }

    /// Returns `true` if `[self.min_key, self.max_key]` overlaps `[lower, upper]`.
    #[inline]
    pub fn overlaps_range(&self, lower: &[u8], upper: Option<&[u8]>) -> bool {
        !self.fence_pointers.is_empty()
            && self.max_key.as_slice() >= lower
            && upper.is_none_or(|u| self.min_key.as_slice() <= u)
    }

    /// Returns the total process heap memory (in bytes) consumed by this segment's
    /// in-memory Bloom filter and Sparse Fence Pointers.
    pub fn heap_bytes_used(&self) -> usize {
        let fp_bytes: usize = self
            .fence_pointers
            .iter()
            .map(|fp| core::mem::size_of::<FencePointer>() + fp.first_key.len())
            .sum();
        self.bloom.heap_bytes() + fp_bytes
    }

    /// Sets or clears the in-process verified block cache.
    pub fn set_block_cache(&mut self, cache: Option<Arc<VerifiedBlockCache>>) {
        self.block_cache = cache;
    }

    /// Builder pattern helper to set the block cache.
    pub fn with_block_cache(mut self, cache: Option<Arc<VerifiedBlockCache>>) -> Self {
        self.block_cache = cache;
        self
    }

    /// Returns the optional block cache reference.
    pub fn block_cache(&self) -> Option<&Arc<VerifiedBlockCache>> {
        self.block_cache.as_ref()
    }

    /// Reads a 4 KB data block by `block_idx`, checking the verified block cache first on hit.
    /// On cache miss, reads from disk, verifies CRC32C, and optionally populates the cache.
    pub fn read_block<'b>(
        &self,
        block_idx: u32,
        block_buf: &'b mut [u8; IDX_BLOCK_SIZE],
        populate_cache: bool,
    ) -> io::Result<IdxBlockReader<'b>> {
        if block_idx >= self.num_blocks {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "index block_idx {block_idx} out of bounds (num_blocks = {})",
                    self.num_blocks
                ),
            ));
        }
        if let Some(cache) = &self.block_cache {
            if cache.get_into(self.seq_id, block_idx, block_buf) {
                return IdxBlockReader::open_preverified(block_buf);
            }
        }
        self.read_block_from_disk(block_idx, block_buf, populate_cache)
    }

    /// Reads and verifies block `block_idx` from the file, then optionally caches it.
    pub(super) fn read_block_from_disk<'b>(
        &self,
        block_idx: u32,
        block_buf: &'b mut [u8; IDX_BLOCK_SIZE],
        populate_cache: bool,
    ) -> io::Result<IdxBlockReader<'b>> {
        self.file
            .read_exact_at(block_buf, (block_idx as u64) * (IDX_BLOCK_SIZE as u64))?;
        let reader = IdxBlockReader::open_verified(block_buf)?;
        if populate_cache {
            if let Some(cache) = &self.block_cache {
                cache.insert(self.seq_id, block_idx, block_buf);
            }
        }
        Ok(reader)
    }

    /// Point lookup for `key`.
    /// 1. Checks in-heap `[min_key, max_key]` key-range bounds and [`BloomFilter`] in ~10 ns.
    /// 2. Binary-searches in-heap [`FencePointer`] table in $O(\log B)$.
    /// 3. Reads a single 4 KB data block (from verified block cache or OS Page Cache) and
    ///    binary-searches its `u16` offset directory in $O(\log K)$.
    pub fn get(&self, key: &[u8]) -> io::Result<Option<(u64, IndexValue)>> {
        self.get_hashed(key, BloomFilter::hash_pair(key))
    }

    /// Like [`Self::get`], with the Bloom hash pair of `key` (from [`BloomFilter::hash_pair`])
    /// supplied by the caller so that a probe across several segments hashes the key once.
    pub fn get_hashed(
        &self,
        key: &[u8],
        hash: (u32, u32),
    ) -> io::Result<Option<(u64, IndexValue)>> {
        self.visit_hashed(key, hash, |raw| Ok((raw.lsn, raw.decode_value()?)))
    }

    /// Looks up `key` and passes the matching entry, borrowed from the block, to `visit`, so the
    /// caller can copy the value straight to its destination instead of through an owned
    /// [`IndexValue`]. Returns `None` without calling `visit` when the key is absent.
    pub fn visit_hashed<R>(
        &self,
        key: &[u8],
        hash: (u32, u32),
        visit: impl FnOnce(RawIdxEntry<'_>) -> io::Result<R>,
    ) -> io::Result<Option<R>> {
        if !self.may_contain_key(key) || !self.bloom.may_contain_hashed(hash.0, hash.1) {
            return Ok(None);
        }

        let pos = self
            .fence_pointers
            .partition_point(|fp| fp.first_key.as_ref() <= key);
        if pos == 0 {
            return Ok(None);
        }

        let block_idx = self.fence_pointers[pos - 1].block_idx;
        // On a hit the search runs directly on the shared cached block: no 4 KB stack buffer
        // is zeroed and no block copy is made.
        if let Some(cache) = &self.block_cache {
            if let Some(block) = cache.get_arc(self.seq_id, block_idx) {
                let reader = IdxBlockReader::open_preverified(&block)?;
                return match reader.find_raw(key)? {
                    Some(raw) => visit(raw).map(Some),
                    None => Ok(None),
                };
            }
        }
        let mut block_buf = [0u8; IDX_BLOCK_SIZE];
        let reader = self.read_block_from_disk(block_idx, &mut block_buf, true)?;
        match reader.find_raw(key)? {
            Some(raw) => visit(raw).map(Some),
            None => Ok(None),
        }
    }

    /// Reads all entries in this segment in sorted order.
    pub fn scan_all(&self) -> io::Result<Vec<IndexEntry>> {
        let max_entries_cap = (self.num_blocks as usize).saturating_mul(IDX_BLOCK_USABLE_SIZE / 13);
        let cap = (self.record_count as usize).min(max_entries_cap);
        let mut out = Vec::new();
        out.try_reserve(cap).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "failed to reserve scan_all entry buffer",
            )
        })?;
        let mut block_buf = [0u8; IDX_BLOCK_SIZE];
        for b in 0..self.num_blocks {
            let reader = self.read_block(b, &mut block_buf, false)?;
            for i in 0..reader.num_entries() {
                out.push(reader.decode_entry_at(i)?);
            }
        }
        Ok(out)
    }

    /// Scans all entries whose keys start with `prefix`.
    pub fn scan_prefix(&self, prefix: &[u8]) -> io::Result<Vec<IndexEntry>> {
        self.scan_prefix_range(prefix, prefix, None, usize::MAX)
    }

    /// Scans up to `limit` entries starting at `seek_key` whose keys still begin with `prefix`.
    pub fn scan_prefix_from(
        &self,
        prefix: &[u8],
        seek_key: &[u8],
        limit: usize,
    ) -> io::Result<Vec<IndexEntry>> {
        self.scan_prefix_range(prefix, seek_key, None, limit)
    }

    /// Scans up to `limit` entries starting at `seek_key`, stopping as soon as an entry
    /// stops starting with `prefix` or exceeds `upper_key_bound` (`(bound_full_key, inclusive)`).
    pub fn scan_prefix_range(
        &self,
        prefix: &[u8],
        seek_key: &[u8],
        upper_key_bound: Option<(&[u8], bool)>,
        limit: usize,
    ) -> io::Result<Vec<IndexEntry>> {
        let mut out = Vec::new();
        if self.fence_pointers.is_empty() || limit == 0 {
            return Ok(out);
        }

        let effective_start = if seek_key > prefix { seek_key } else { prefix };
        if let Some((ub, inclusive)) = upper_key_bound {
            if effective_start > ub || (!inclusive && effective_start == ub) {
                return Ok(out);
            }
        }
        if !self.overlaps_range(effective_start, upper_key_bound.map(|(ub, _)| ub)) {
            return Ok(out);
        }
        if !prefix.is_empty()
            && self.min_key.as_slice() > prefix
            && !self.min_key.starts_with(prefix)
        {
            return Ok(out);
        }
        let start_pos = self
            .fence_pointers
            .partition_point(|fp| fp.first_key.as_ref() <= effective_start);
        let first_block = start_pos.saturating_sub(1) as u32;

        let mut block_buf = [0u8; IDX_BLOCK_SIZE];
        for b in first_block..self.num_blocks {
            let reader = self.read_block(b, &mut block_buf, true)?;
            for i in 0..reader.num_entries() {
                let k = reader.key_at(i)?;
                if k < effective_start {
                    continue;
                }
                if !k.starts_with(prefix) {
                    return Ok(out);
                }
                if let Some((ub, inclusive)) = upper_key_bound {
                    if inclusive {
                        if k > ub {
                            return Ok(out);
                        }
                    } else if k >= ub {
                        return Ok(out);
                    }
                }
                let entry = reader.decode_entry_at(i)?;
                out.push(entry);
                if out.len() >= limit {
                    return Ok(out);
                }
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests;

#[cfg(kani)]
mod kani_proofs {
    use super::*;

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
    fn target_eq_2(target: &[u8], expected: &[u8; 2]) -> bool {
        target.len() == 2 && target[0] == expected[0] && target[1] == expected[1]
    }

    #[inline(always)]
    fn assert_bytes_eq_2(actual: &[u8], expected: &[u8; 2]) {
        assert_eq!(actual.len(), 2);
        assert_eq!(actual[0], expected[0]);
        assert_eq!(actual[1], expected[1]);
    }

    #[kani::proof]
    #[kani::unwind(2)]
    fn verify_idx_block_open_preverified_header_bounds() {
        let mut buf = [0u8; IDX_BLOCK_SIZE];
        let num_entries: u16 = kani::any();
        buf[4088..4090].copy_from_slice(&num_entries.to_le_bytes());

        let res = IdxBlockReader::open_preverified(&buf);
        if (num_entries as usize) > MAX_IDX_BLOCK_ENTRIES {
            assert!(res.is_err());
        } else {
            let reader = res.unwrap();
            assert_eq!(reader.num_entries(), num_entries as usize);
            assert_eq!(
                reader.dir_start,
                IDX_BLOCK_USABLE_SIZE - (num_entries as usize) * 2
            );
        }
    }

    #[kani::proof]
    #[kani::unwind(4)]
    fn verify_idx_block_accessors_arbitrary_bytes_no_panic() {
        let buf: [u8; 40] = kani::any();
        let n: usize = kani::any();
        kani::assume(n <= 3);
        let dir_start = buf.len() - n * 2;
        let reader = IdxBlockReader::from_slice_for_kani(&buf, n, dir_start);

        let idx: usize = kani::any();
        kani::assume(idx < 4);
        let _ = reader.entry_offset(idx);
        let _ = reader.key_at(idx);
        let _ = reader.parse_entry_span_at(idx);
        if let Ok(raw) = reader.raw_entry_at(idx) {
            if raw.val_type == 2 {
                let _ = RawIdxEntry::decode_blob_ptr(raw.val_bytes);
            }
        }
    }

    #[kani::proof]
    #[kani::unwind(8)]
    fn verify_idx_block_layout_validator_arbitrary_bytes_no_panic() {
        let buf: [u8; 40] = kani::any();
        let n: usize = kani::any();
        kani::assume(n <= 2);
        let dir_start = buf.len() - n * 2;
        let mut i = 0usize;
        while i < n {
            let off =
                u16::from_le_bytes([buf[dir_start + i * 2], buf[dir_start + i * 2 + 1]]) as usize;
            if off + 2 <= buf.len() {
                let klen = u16::from_le_bytes([buf[off], buf[off + 1]]) as usize;
                kani::assume(klen <= 4 || off + MIN_IDX_ENTRY_BYTES + klen > dir_start);
            }
            i += 1;
        }
        let _ = validate_block_entry_layout(&buf, n, dir_start);
    }

    fn write_inline_entry(buf: &mut [u8], off: usize, key: &[u8; 2], lsn: u64, val: &[u8; 2]) {
        buf[off..off + 2].copy_from_slice(&2u16.to_le_bytes());
        buf[off + 2] = 1;
        buf[off + 3..off + 11].copy_from_slice(&lsn.to_le_bytes());
        buf[off + 11..off + 13].copy_from_slice(key);
        buf[off + 13..off + 15].copy_from_slice(&2u16.to_le_bytes());
        buf[off + 15..off + 17].copy_from_slice(val);
    }

    fn write_tombstone_entry(buf: &mut [u8], off: usize, key: &[u8; 2], lsn: u64) {
        buf[off..off + 2].copy_from_slice(&2u16.to_le_bytes());
        buf[off + 2] = 3;
        buf[off + 3..off + 11].copy_from_slice(&lsn.to_le_bytes());
        buf[off + 11..off + 13].copy_from_slice(key);
    }

    fn write_blob_ptr_entry(buf: &mut [u8], off: usize, key: &[u8; 2], lsn: u64, ptr: BlobPtr) {
        buf[off..off + 2].copy_from_slice(&2u16.to_le_bytes());
        buf[off + 2] = 2;
        buf[off + 3..off + 11].copy_from_slice(&lsn.to_le_bytes());
        buf[off + 11..off + 13].copy_from_slice(key);
        buf[off + 13] = ptr.kind as u8;
        buf[off + 14..off + 16].copy_from_slice(&ptr.table_id.to_le_bytes());
        buf[off + 16..off + 24].copy_from_slice(&ptr.seg_id.to_le_bytes());
        buf[off + 24..off + 28].copy_from_slice(&ptr.offset.to_le_bytes());
        buf[off + 28..off + 32].copy_from_slice(&ptr.len.to_le_bytes());
        buf[off + 32..off + 36].copy_from_slice(&ptr.crc32c.to_le_bytes());
    }

    #[kani::proof]
    #[kani::unwind(4)]
    fn verify_idx_block_binary_search_inline_and_tombstone() {
        let target_buf: [u8; 3] = kani::any();
        let target_len: usize = kani::any();
        kani::assume(target_len <= 3);
        let target = &target_buf[..target_len];

        let empty = IdxBlockReader::from_slice_for_kani(&[], 0, 0);
        assert!(unwrap_io(empty.find_raw(target)).is_none());

        let mut buf = [0u8; 64];
        let k0: [u8; 2] = kani::any();
        let k1: [u8; 2] = kani::any();
        let k2: [u8; 2] = kani::any();
        kani::assume(u16::from_be_bytes(k0) < u16::from_be_bytes(k1));
        kani::assume(u16::from_be_bytes(k1) < u16::from_be_bytes(k2));

        let lsn0: u64 = kani::any();
        let lsn1: u64 = kani::any();
        let lsn2: u64 = kani::any();
        let v0: [u8; 2] = kani::any();
        let v2: [u8; 2] = kani::any();

        write_inline_entry(&mut buf, 0, &k0, lsn0, &v0);
        write_tombstone_entry(&mut buf, 17, &k1, lsn1);
        write_inline_entry(&mut buf, 30, &k2, lsn2, &v2);
        buf[58..60].copy_from_slice(&0u16.to_le_bytes());
        buf[60..62].copy_from_slice(&17u16.to_le_bytes());
        buf[62..64].copy_from_slice(&30u16.to_le_bytes());

        let reader = IdxBlockReader::from_slice_for_kani(&buf, 3, 58);
        let res = unwrap_io(reader.find_raw(target));

        if target_eq_2(target, &k0) {
            let Some(raw) = res else {
                panic!("k0 must be found");
            };
            assert_eq!(raw.lsn, lsn0);
            assert_eq!(raw.val_type, 1);
            assert_bytes_eq_2(raw.key, &k0);
            assert_bytes_eq_2(raw.val_bytes, &v0);
        } else if target_eq_2(target, &k1) {
            let Some(raw) = res else {
                panic!("k1 must be found");
            };
            assert_eq!(raw.lsn, lsn1);
            assert_eq!(raw.val_type, 3);
            assert_bytes_eq_2(raw.key, &k1);
            assert!(raw.val_bytes.is_empty());
        } else if target_eq_2(target, &k2) {
            let Some(raw) = res else {
                panic!("k2 must be found");
            };
            assert_eq!(raw.lsn, lsn2);
            assert_eq!(raw.val_type, 1);
            assert_bytes_eq_2(raw.key, &k2);
            assert_bytes_eq_2(raw.val_bytes, &v2);
        } else {
            assert!(res.is_none());
        }
    }

    #[kani::proof]
    #[kani::unwind(4)]
    fn verify_idx_block_binary_search_with_blob_ptr() {
        let mut buf = [0u8; 60];
        let k0: [u8; 2] = kani::any();
        let k1: [u8; 2] = kani::any();
        kani::assume(u16::from_be_bytes(k0) < u16::from_be_bytes(k1));

        let lsn0: u64 = kani::any();
        let lsn1: u64 = kani::any();
        let expected_ptr = BlobPtr {
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
        let v1: [u8; 2] = kani::any();

        write_blob_ptr_entry(&mut buf, 0, &k0, lsn0, expected_ptr);
        write_inline_entry(&mut buf, 36, &k1, lsn1, &v1);
        buf[56..58].copy_from_slice(&0u16.to_le_bytes());
        buf[58..60].copy_from_slice(&36u16.to_le_bytes());

        let target_buf: [u8; 3] = kani::any();
        let target_len: usize = kani::any();
        kani::assume(target_len <= 3);
        let target = &target_buf[..target_len];

        let reader = IdxBlockReader::from_slice_for_kani(&buf, 2, 56);
        let res = unwrap_io(reader.find_raw(target));

        if target_eq_2(target, &k0) {
            let Some(raw) = res else {
                panic!("k0 must be found");
            };
            assert_eq!(raw.lsn, lsn0);
            assert_eq!(raw.val_type, 2);
            assert_bytes_eq_2(raw.key, &k0);
            let dp = unwrap_io(RawIdxEntry::decode_blob_ptr(raw.val_bytes));
            assert_eq!(dp.kind, expected_ptr.kind);
            assert_eq!(dp.table_id, expected_ptr.table_id);
            assert_eq!(dp.seg_id, expected_ptr.seg_id);
            assert_eq!(dp.offset, expected_ptr.offset);
            assert_eq!(dp.len, expected_ptr.len);
            assert_eq!(dp.crc32c, expected_ptr.crc32c);
        } else if target_eq_2(target, &k1) {
            let Some(raw) = res else {
                panic!("k1 must be found");
            };
            assert_eq!(raw.lsn, lsn1);
            assert_eq!(raw.val_type, 1);
            assert_bytes_eq_2(raw.key, &k1);
            assert_bytes_eq_2(raw.val_bytes, &v1);
        } else {
            assert!(res.is_none());
        }
    }
}
