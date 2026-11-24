//! Layer 3 (Part 2): Immutable 4 KB Block SSTable Index Segments (`idx_<SeqID>.db`),
//! Sparse Fence Pointers, In-Heap Bloom Filters, and Multi-Way Merge Compaction.
//!
//! See `docs/phase1/04_WISCKEY_AND_SEGMENTS.md` for the detailed binary format.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;

pub mod bloom;
pub mod key_codec;
pub use bloom::BloomFilter;
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
use crate::util::crc32c::crc32c;
use crate::wal::{BlobKind, BlobPtr};

/// Size of an index data block (4096 bytes).
pub const IDX_BLOCK_SIZE: usize = 4096;

/// Write coalescing buffer capacity (`256 KB` = 64 data blocks, matching the Windows Cache
/// Manager Virtual Address Control Block size) used by [`IdxSegmentBuilder`].
const IDX_WRITE_BUFFER_CAPACITY: usize = 256 * 1024;

/// Sequential read-ahead chunk size (`64 * 4 KB = 256 KB`) for compaction merge cursors.
const COMPACTION_READAHEAD_BLOCKS: u32 = 64;

/// Size of the fixed trailer at the end of every 4 KB index data block (`4088..4096`).
const IDX_BLOCK_TRAILER_SIZE: usize = 8;

/// Usable byte region before the 8-byte trailer (`0..4088`).
const IDX_BLOCK_USABLE_SIZE: usize = IDX_BLOCK_SIZE - IDX_BLOCK_TRAILER_SIZE;

/// Maximum encoded size of a single `(key + header + inline_value)` before the 2-byte `u16` offset slot inside a 4 KB block.
pub const MAX_SINGLE_BLOCK_ENTRY_BYTES: usize = IDX_BLOCK_USABLE_SIZE - 2;

/// Encoded byte length of a `BlobPtr` inside an `IndexEntry` (`23` bytes).
pub const BLOB_PTR_ENCODED_LEN: usize = 23;

/// Default Bloom filter bits per key (`10` bits/key -> ~0.9% false-positive rate).
pub const DEFAULT_BLOOM_BITS_PER_KEY: usize = 10;

/// Maximum defensive size for an index segment's filter/fence metadata trailer (64 MB).
pub const MAX_INDEX_META_BYTES: usize = 64 * 1024 * 1024;

/// Fixed 48-byte footer at the end of every `idx_<SeqID>.db` file.
const IDX_FOOTER_SIZE: usize = 48;
const IDX_FOOTER_MAGIC: &[u8; 4] = b"AYIX";
const IDX_FORMAT_VERSION: u16 = 1;

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
    fn type_tag(&self) -> u8 {
        match self {
            Self::Inline(_) => 1,
            Self::Blob(_) => 2,
            Self::Tombstone => 3,
        }
    }

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
            1 => match val_len.checked_add(2) {
                Some(n) => n,
                None => return false,
            },
            2 if val_len == BLOB_PTR_ENCODED_LEN => BLOB_PTR_ENCODED_LEN,
            3 if val_len == 0 => 0,
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
        let klen = u16::try_from(key_len).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "key length exceeds u16::MAX")
        })?;
        let (vlen, payload_len) = match val_type {
            1 => {
                let vlen = u16::try_from(val_len).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "inline value length exceeds u16::MAX",
                    )
                })?;
                let payload_len = val_len.checked_add(2).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "inline value footprint overflow",
                    )
                })?;
                (vlen, payload_len)
            }
            2 => {
                if val_len != BLOB_PTR_ENCODED_LEN {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "BlobPtr raw value must be 23 bytes",
                    ));
                }
                (0, BLOB_PTR_ENCODED_LEN)
            }
            3 => {
                if val_len != 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Tombstone raw value must be empty",
                    ));
                }
                (0, 0)
            }
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unknown IndexValue type_tag: {other}"),
                ));
            }
        };
        let fits_single_block = 11usize
            .checked_add(key_len)
            .and_then(|n| n.checked_add(payload_len))
            .and_then(|n| n.checked_add(2))
            .is_some_and(|total| total <= IDX_BLOCK_USABLE_SIZE);
        if !fits_single_block {
            return Err(io::Error::new(
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
        if val_type == 2 {
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
            1 => {
                self.buf[pos..pos + 2].copy_from_slice(&vlen.to_le_bytes());
                pos += 2;
                self.buf[pos..pos + val_bytes.len()].copy_from_slice(val_bytes);
                pos += val_bytes.len();
            }
            2 => {
                self.buf[pos..pos + BLOB_PTR_ENCODED_LEN].copy_from_slice(val_bytes);
                pos += BLOB_PTR_ENCODED_LEN;
            }
            3 => {}
            _ => unreachable!("val_type validated above"),
        }

        self.write_pos = pos;
        Ok(())
    }

    /// Appends `entry` (which must be in strictly ascending key order) to this block.
    pub fn add_entry(&mut self, entry: &IndexEntry) -> io::Result<()> {
        let val_len = match &entry.value {
            IndexValue::Inline(v) => v.len(),
            IndexValue::Blob(_) => BLOB_PTR_ENCODED_LEN,
            IndexValue::Tombstone => 0,
        };
        let (klen, vlen) =
            Self::validate_raw_entry_params(entry.key.len(), entry.value.type_tag(), val_len)?;
        if !self.can_fit(entry) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "IndexEntry exceeds remaining 4 KB block capacity",
            ));
        }
        let start_u16 = self.check_key_order_and_start(&entry.key)?;

        let start = self.write_pos;
        self.offsets.push(start_u16);

        self.buf[start..start + 2].copy_from_slice(&klen.to_le_bytes());
        self.buf[start + 2] = entry.value.type_tag();
        self.buf[start + 3..start + 11].copy_from_slice(&entry.lsn.to_le_bytes());
        let mut pos = start + 11;

        self.buf[pos..pos + entry.key.len()].copy_from_slice(&entry.key);
        pos += entry.key.len();

        match &entry.value {
            IndexValue::Inline(val) => {
                self.buf[pos..pos + 2].copy_from_slice(&vlen.to_le_bytes());
                pos += 2;
                self.buf[pos..pos + val.len()].copy_from_slice(val);
                pos += val.len();
            }
            IndexValue::Blob(ptr) => {
                self.buf[pos] = ptr.kind as u8;
                self.buf[pos + 1..pos + 3].copy_from_slice(&ptr.table_id.to_le_bytes());
                self.buf[pos + 3..pos + 11].copy_from_slice(&ptr.seg_id.to_le_bytes());
                self.buf[pos + 11..pos + 15].copy_from_slice(&ptr.offset.to_le_bytes());
                self.buf[pos + 15..pos + 19].copy_from_slice(&ptr.len.to_le_bytes());
                self.buf[pos + 19..pos + 23].copy_from_slice(&ptr.crc32c.to_le_bytes());
                pos += 23;
            }
            IndexValue::Tombstone => {}
        }

        self.write_pos = pos;
        Ok(())
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
        if val_bytes.len() != BLOB_PTR_ENCODED_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "corrupted BlobPtr slice length in RawIdxEntry",
            ));
        }
        let kind = BlobKind::from_u8(val_bytes[0])?;
        let table_id = u16::from_le_bytes([val_bytes[1], val_bytes[2]]);
        let seg_id = u64::from_le_bytes(val_bytes[3..11].try_into().unwrap());
        let offset = u32::from_le_bytes(val_bytes[11..15].try_into().unwrap());
        let len = u32::from_le_bytes(val_bytes[15..19].try_into().unwrap());
        let crc = u32::from_le_bytes(val_bytes[19..23].try_into().unwrap());
        Ok(BlobPtr {
            kind,
            table_id,
            seg_id,
            offset,
            len,
            crc32c: crc,
        })
    }

    /// Decodes the value payload of this entry into an owned [`IndexValue`].
    pub fn decode_value(&self) -> io::Result<IndexValue> {
        match self.val_type {
            1 => Ok(IndexValue::Inline(self.val_bytes.to_vec())),
            2 => Ok(IndexValue::Blob(Self::decode_blob_ptr(self.val_bytes)?)),
            3 => Ok(IndexValue::Tombstone),
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown IndexValue type_tag: {other}"),
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
struct CursorEntrySpan {
    key_range: core::ops::Range<usize>,
    lsn: u64,
    val_type: u8,
    val_range: core::ops::Range<usize>,
}

/// Minimum encoded header size of an entry in a 4 KB data block (`klen(2) + type(1) + lsn(8)`).
const MIN_IDX_ENTRY_BYTES: usize = 11;

/// Maximum number of entries that can fit in a 4 KB data block (`11B` entry + `2B` directory slot).
const MAX_IDX_BLOCK_ENTRIES: usize = IDX_BLOCK_USABLE_SIZE / (MIN_IDX_ENTRY_BYTES + 2);

/// Zero-copy reader for a single 4096-byte index data block.
pub struct IdxBlockReader<'a> {
    buf: &'a [u8; IDX_BLOCK_SIZE],
    num_entries: usize,
    dir_start: usize,
}

fn validate_block_entry_layout(
    buf: &[u8; IDX_BLOCK_SIZE],
    num_entries: usize,
    dir_start: usize,
) -> io::Result<()> {
    let mut prev_end = 0usize;
    let mut prev_key: Option<&[u8]> = None;
    for i in 0..num_entries {
        let dir_pos = dir_start + (i * 2);
        let off = u16::from_le_bytes([buf[dir_pos], buf[dir_pos + 1]]) as usize;
        if off < prev_end {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "overlapping or out-of-order entry offsets in 4 KB index block",
            ));
        }
        let Some(kstart) = off.checked_add(MIN_IDX_ENTRY_BYTES) else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "corrupted entry offset in 4 KB index block",
            ));
        };
        if kstart > dir_start {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "corrupted entry offset in 4 KB index block",
            ));
        }
        let klen = u16::from_le_bytes([buf[off], buf[off + 1]]) as usize;
        let val_type = buf[off + 2];
        let Some(kend) = kstart.checked_add(klen) else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "corrupted key length in 4 KB index block",
            ));
        };
        if kend > dir_start {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "corrupted key slice in 4 KB index block",
            ));
        }
        let key = &buf[kstart..kend];
        if prev_key.is_some_and(|prev| key <= prev) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unsorted or duplicate keys in 4 KB index block",
            ));
        }
        prev_key = Some(key);
        let vend = match val_type {
            1 => {
                let Some(vstart) = kend.checked_add(2) else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "truncated inline value length",
                    ));
                };
                if vstart > dir_start {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "truncated inline value length",
                    ));
                }
                let vlen = u16::from_le_bytes([buf[kend], buf[kend + 1]]) as usize;
                let Some(vend) = vstart.checked_add(vlen) else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "truncated inline value payload",
                    ));
                };
                vend
            }
            2 => {
                let Some(vend) = kend.checked_add(BLOB_PTR_ENCODED_LEN) else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "truncated BlobPtr in 4 KB index block",
                    ));
                };
                if vend <= dir_start {
                    BlobKind::from_u8(buf[kend])?;
                }
                vend
            }
            3 => kend,
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unknown IndexValue type_tag: {other}"),
                ));
            }
        };
        if vend > dir_start {
            return Err(io::Error::new(
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
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "4 KB index data block CRC32C mismatch",
            ));
        }

        let num_entries = u16::from_le_bytes([buf[4088], buf[4089]]) as usize;
        if num_entries > MAX_IDX_BLOCK_ENTRIES {
            return Err(io::Error::new(
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
            return Err(io::Error::new(
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

    #[inline]
    pub fn num_entries(&self) -> usize {
        self.num_entries
    }

    #[inline]
    fn entry_offset(&self, index: usize) -> io::Result<usize> {
        if index >= self.num_entries {
            return Err(io::Error::new(
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
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "corrupted entry offset in 4 KB index block",
            ));
        }
        let klen = u16::from_le_bytes([self.buf[off], self.buf[off + 1]]) as usize;
        let kstart = off + 11;
        if kstart + klen > self.dir_start {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "corrupted key_len in 4 KB index block",
            ));
        }
        Ok(&self.buf[kstart..kstart + klen])
    }

    /// Parses the byte ranges of the entry at `index` inside `self.buf` with zero heap allocations.
    #[inline]
    fn parse_entry_span_at(&self, index: usize) -> io::Result<CursorEntrySpan> {
        let off = self.entry_offset(index)?;
        if off + 11 > self.dir_start {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "corrupted entry offset in 4 KB index block",
            ));
        }
        let klen = u16::from_le_bytes([self.buf[off], self.buf[off + 1]]) as usize;
        let val_type = self.buf[off + 2];
        let lsn = u64::from_le_bytes(self.buf[off + 3..off + 11].try_into().unwrap());
        let kstart = off + 11;
        let kend = kstart + klen;
        if kend > self.dir_start {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "corrupted key slice in 4 KB index block",
            ));
        }
        let val_range = match val_type {
            1 => {
                if kend + 2 > self.dir_start {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "truncated inline value length",
                    ));
                }
                let vlen = u16::from_le_bytes([self.buf[kend], self.buf[kend + 1]]) as usize;
                let vstart = kend + 2;
                let vend = vstart + vlen;
                if vend > self.dir_start {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "truncated inline value payload",
                    ));
                }
                vstart..vend
            }
            2 => {
                let vend = kend + BLOB_PTR_ENCODED_LEN;
                if vend > self.dir_start {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "truncated BlobPtr in 4 KB index block",
                    ));
                }
                BlobKind::from_u8(self.buf[kend])?;
                kend..vend
            }
            3 => kend..kend,
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unknown IndexValue type_tag: {other}"),
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
        let val_len = match &entry.value {
            IndexValue::Inline(v) => v.len(),
            IndexValue::Blob(_) => BLOB_PTR_ENCODED_LEN,
            IndexValue::Tombstone => 0,
        };
        IdxBlockBuilder::validate_raw_entry_params(
            entry.key.len(),
            entry.value.type_tag(),
            val_len,
        )?;
        if self.record_count > 0 && entry.key.as_slice() <= self.last_key.as_slice() {
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
        if !self.block_builder.is_empty() && !self.block_builder.can_fit(entry) {
            self.flush_current_data_block()?;
        }
        self.block_builder.add_entry(entry)?;
        if self.bloom_bits_per_key > 0 {
            self.bloom_hashes.push(BloomFilter::hash64(&entry.key));
        }
        self.last_key.clear();
        self.last_key.extend_from_slice(&entry.key);
        self.record_count = next_count;
        Ok(())
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
    file: Arc<F>,
    seq_id: u64,
    level: u16,
    file_len: u64,
    num_blocks: u32,
    record_count: u32,
    bloom: BloomFilter,
    fence_pointers: Vec<FencePointer>,
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

        if &footer[0..4] != IDX_FOOTER_MAGIC {
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

        let version = u16::from_le_bytes([footer[4], footer[5]]);
        if version != IDX_FORMAT_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported idx format version: {version}"),
            ));
        }

        let level = u16::from_le_bytes([footer[6], footer[7]]);
        let stored_seq_id = u64::from_le_bytes(footer[8..16].try_into().unwrap());
        let num_blocks = u32::from_le_bytes(footer[16..20].try_into().unwrap());
        let record_count = u32::from_le_bytes(footer[20..24].try_into().unwrap());
        let meta_offset = u64::from_le_bytes(footer[24..32].try_into().unwrap());
        let meta_len = u32::from_le_bytes(footer[32..36].try_into().unwrap()) as usize;
        let expected_meta_crc = u32::from_le_bytes(footer[36..40].try_into().unwrap());

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

        let mut slice: &[u8] = &meta_buf;
        let bloom = BloomFilter::decode(&mut slice)?;
        if slice.len() < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "truncated fence pointer count in idx segment",
            ));
        }
        let fp_count = u32::from_le_bytes([slice[0], slice[1], slice[2], slice[3]]) as usize;
        slice = &slice[4..];

        // Validate `fp_count` against the number of 4 KB data blocks (`1` fence pointer per block)
        // and the remaining bytes in `slice` (`>= 6` bytes per fence pointer) BEFORE allocating.
        let min_fp_bytes = fp_count.checked_mul(6).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "fence pointer count byte size overflow",
            )
        })?;
        if fp_count != num_blocks as usize || min_fp_bytes > slice.len() {
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
            if slice.len() < 6 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "truncated fence pointer entry",
                ));
            }
            let klen = u16::from_le_bytes([slice[0], slice[1]]) as usize;
            let block_idx = u32::from_le_bytes([slice[2], slice[3], slice[4], slice[5]]);
            if block_idx != expected_block_idx as u32 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "non-sequential fence pointer block_idx in idx segment",
                ));
            }
            slice = &slice[6..];
            if slice.len() < klen {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "truncated fence pointer key",
                ));
            }
            let first_key = slice[..klen].to_vec().into_boxed_slice();
            slice = &slice[klen..];
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
        let max_key = if slice.len() < 2 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "missing or truncated max_key in idx segment metadata",
            ));
        } else {
            let max_klen = u16::from_le_bytes([slice[0], slice[1]]) as usize;
            slice = &slice[2..];
            if slice.len() != max_klen {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "truncated or trailing bytes in idx segment max_key",
                ));
            }
            if num_blocks == 0 && !slice.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "empty idx segment must have empty max_key",
                ));
            }
            if let Some(last_fp) = fence_pointers.last() {
                if slice < last_fp.first_key.as_ref() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "idx segment max_key precedes last fence pointer",
                    ));
                }
            }
            slice.to_vec()
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
    fn read_block_from_disk<'b>(
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

    /// Creates a bounded-memory streaming cursor over all entries in this segment, reading
    /// one 4 KB data block at a time on demand (bypasses cache insertion).
    pub fn cursor_all(&self) -> io::Result<IdxSegmentCursor<'_, F>> {
        IdxSegmentCursor::new_all(self, false, CursorBlockStorage::new())
    }

    /// Creates a streaming cursor over all entries in this segment using batched sequential
    /// read-ahead chunks of up to `max_blocks` 4 KB blocks per `read_exact_at` syscall.
    pub(crate) fn cursor_all_batched(
        &self,
        max_blocks: u32,
    ) -> io::Result<IdxSegmentCursor<'_, F>> {
        IdxSegmentCursor::new_all(self, false, CursorBlockStorage::new_batched(max_blocks))
    }

    /// Creates a bounded-memory streaming cursor over all entries in this segment with explicit cache policy.
    pub fn cursor_all_with_cache(
        &self,
        populate_cache: bool,
    ) -> io::Result<IdxSegmentCursor<'_, F>> {
        IdxSegmentCursor::new_all(self, populate_cache, CursorBlockStorage::new())
    }

    /// Creates a bounded-memory streaming cursor starting at `(lower_key, lower_inclusive)`,
    /// seeking via sparse fence pointers and reading one 4 KB data block at a time on demand.
    pub fn cursor_from(
        &self,
        lower_key: &[u8],
        lower_inclusive: bool,
    ) -> io::Result<IdxSegmentCursor<'_, F>> {
        IdxSegmentCursor::new_from(self, lower_key, lower_inclusive, true)
    }

    /// Creates a bounded-memory streaming cursor starting at `(lower_key, lower_inclusive)` with explicit cache policy.
    pub fn cursor_from_with_cache(
        &self,
        lower_key: &[u8],
        lower_inclusive: bool,
        populate_cache: bool,
    ) -> io::Result<IdxSegmentCursor<'_, F>> {
        IdxSegmentCursor::new_from(self, lower_key, lower_inclusive, populate_cache)
    }

    /// Creates a bounded-memory reverse streaming cursor starting at `(upper_key, upper_inclusive)`,
    /// seeking via sparse fence pointers and walking 4 KB data blocks backward on demand.
    pub fn cursor_reverse_from(
        &self,
        upper_key: Option<(&[u8], bool)>,
    ) -> io::Result<IdxSegmentRevCursor<'_, F>> {
        IdxSegmentRevCursor::new_from(self, upper_key, true)
    }

    /// Creates a bounded-memory reverse streaming cursor starting at `(upper_key, upper_inclusive)` with explicit cache policy.
    pub fn cursor_reverse_from_with_cache(
        &self,
        upper_key: Option<(&[u8], bool)>,
        populate_cache: bool,
    ) -> io::Result<IdxSegmentRevCursor<'_, F>> {
        IdxSegmentRevCursor::new_from(self, upper_key, populate_cache)
    }
}

struct ReadAheadBuffer {
    buf: Vec<u8>,
    start_block: u32,
    block_count: u32,
    max_blocks: u32,
}

enum ActiveBlock {
    None,
    Cached(Arc<[u8; IDX_BLOCK_SIZE]>),
    Scratch,
    ReadAhead(usize),
}

struct CursorBlockStorage {
    active: ActiveBlock,
    scratch: Option<Box<[u8; IDX_BLOCK_SIZE]>>,
    read_ahead: Option<ReadAheadBuffer>,
}

impl CursorBlockStorage {
    fn new() -> Self {
        Self {
            active: ActiveBlock::None,
            scratch: None,
            read_ahead: None,
        }
    }

    fn new_batched(max_blocks: u32) -> Self {
        let max_blocks = max_blocks.max(1);
        Self {
            active: ActiveBlock::None,
            scratch: None,
            read_ahead: Some(ReadAheadBuffer {
                buf: Vec::with_capacity((max_blocks as usize) * IDX_BLOCK_SIZE),
                start_block: 0,
                block_count: 0,
                max_blocks,
            }),
        }
    }

    #[inline]
    fn block_bytes(&self) -> &[u8; IDX_BLOCK_SIZE] {
        match &self.active {
            ActiveBlock::Cached(arc) => arc.as_ref(),
            ActiveBlock::Scratch => self
                .scratch
                .as_deref()
                .expect("Scratch active without allocated buffer"),
            ActiveBlock::ReadAhead(off) => {
                let ra = self
                    .read_ahead
                    .as_ref()
                    .expect("ReadAhead active without buffer");
                ra.buf[*off..*off + IDX_BLOCK_SIZE]
                    .try_into()
                    .expect("ReadAhead slice matches IDX_BLOCK_SIZE")
            }
            ActiveBlock::None => panic!("CursorBlockStorage::block_bytes called when empty"),
        }
    }

    fn load<F: PlatformFile>(
        &mut self,
        seg: &IdxSegmentReader<F>,
        block_idx: u32,
        populate_cache: bool,
    ) -> io::Result<IdxBlockReader<'_>> {
        let preverified = if let Some(ra) = &mut self.read_ahead {
            if block_idx < ra.start_block || block_idx >= ra.start_block + ra.block_count {
                // Invalidate window metadata before resizing so a failed read leaves no stale span.
                ra.block_count = 0;
                self.active = ActiveBlock::None;
                let count = seg
                    .num_blocks
                    .saturating_sub(block_idx)
                    .min(ra.max_blocks)
                    .max(1);
                let bytes = (count as usize) * IDX_BLOCK_SIZE;
                ra.buf.resize(bytes, 0);
                seg.file
                    .read_exact_at(&mut ra.buf, (block_idx as u64) * (IDX_BLOCK_SIZE as u64))?;
                ra.start_block = block_idx;
                ra.block_count = count;
            }
            let off = ((block_idx - ra.start_block) as usize) * IDX_BLOCK_SIZE;
            self.active = ActiveBlock::ReadAhead(off);
            false
        } else if let Some(arc) = seg
            .block_cache
            .as_ref()
            .and_then(|c| c.get_arc(seg.seq_id, block_idx))
        {
            self.active = ActiveBlock::Cached(arc);
            true
        } else {
            self.active = ActiveBlock::None;
            let scratch = self
                .scratch
                .get_or_insert_with(|| Box::new([0u8; IDX_BLOCK_SIZE]));
            seg.file
                .read_exact_at(&mut **scratch, (block_idx as u64) * (IDX_BLOCK_SIZE as u64))?;
            self.active = ActiveBlock::Scratch;
            false
        };

        let num_entries = if preverified {
            match IdxBlockReader::open_preverified(self.block_bytes()) {
                Ok(r) => r.num_entries,
                Err(e) => {
                    self.active = ActiveBlock::None;
                    return Err(e);
                }
            }
        } else {
            let num_entries = match IdxBlockReader::open_verified(self.block_bytes()) {
                Ok(r) => r.num_entries,
                Err(e) => {
                    self.active = ActiveBlock::None;
                    return Err(e);
                }
            };
            if populate_cache {
                if let Some(cache) = &seg.block_cache {
                    cache.insert(seg.seq_id, block_idx, self.block_bytes());
                }
            }
            num_entries
        };
        Ok(IdxBlockReader::from_verified_buf(
            self.block_bytes(),
            num_entries,
        ))
    }
}

/// Bounded-memory reverse block-by-block cursor over an [`IdxSegmentReader`].
pub struct IdxSegmentRevCursor<'a, F: PlatformFile> {
    seg: &'a IdxSegmentReader<F>,
    next_block_idx: Option<u32>,
    block: CursorBlockStorage,
    entry_idx: usize,
    num_entries_in_block: usize,
    current_span: Option<CursorEntrySpan>,
    populate_cache: bool,
}

impl<'a, F: PlatformFile> IdxSegmentRevCursor<'a, F> {
    fn new_from(
        seg: &'a IdxSegmentReader<F>,
        upper_key: Option<(&[u8], bool)>,
        populate_cache: bool,
    ) -> io::Result<Self> {
        let mut cursor = Self {
            seg,
            next_block_idx: None,
            block: CursorBlockStorage::new(),
            entry_idx: 0,
            num_entries_in_block: 0,
            current_span: None,
            populate_cache,
        };
        if seg.fence_pointers.is_empty() || seg.num_blocks == 0 {
            return Ok(cursor);
        }

        let Some((ub_key, ub_inclusive)) = upper_key else {
            cursor.next_block_idx = Some(seg.num_blocks - 1);
            cursor.advance_to_prev()?;
            return Ok(cursor);
        };

        let pos = seg.fence_pointers.partition_point(|fp| {
            if ub_inclusive {
                fp.first_key.as_ref() <= ub_key
            } else {
                fp.first_key.as_ref() < ub_key
            }
        });
        if pos == 0 {
            return Ok(cursor);
        }
        let mut b = seg.fence_pointers[pos - 1].block_idx;

        loop {
            let reader = cursor.block.load(seg, b, cursor.populate_cache)?;
            cursor.num_entries_in_block = reader.num_entries();

            // Binary-search for the first entry strictly after `(ub_key, ub_inclusive)`
            let mut low = 0usize;
            let mut high = cursor.num_entries_in_block;
            while low < high {
                let mid = low + (high - low) / 2;
                let k = reader.key_at(mid)?;
                let within = if ub_inclusive {
                    k <= ub_key
                } else {
                    k < ub_key
                };
                if within {
                    low = mid + 1;
                } else {
                    high = mid;
                }
            }

            if low > 0 {
                let target_idx = low - 1;
                let span = reader.parse_entry_span_at(target_idx)?;
                cursor.entry_idx = target_idx;
                cursor.next_block_idx = b.checked_sub(1);
                cursor.current_span = Some(span);
                break;
            }

            if b == 0 {
                break;
            }
            b -= 1;
        }
        Ok(cursor)
    }

    /// Returns the raw key slice of the current entry without heap allocation.
    #[inline]
    pub fn current_key(&self) -> Option<&[u8]> {
        let s = self.current_span.as_ref()?;
        let block = self.block.block_bytes();
        Some(&block[s.key_range.clone()])
    }

    /// Returns a zero-copy [`RawIdxEntry`] view of the current entry borrowed from `self.block`.
    #[inline]
    pub fn peek_raw(&self) -> Option<RawIdxEntry<'_>> {
        let s = self.current_span.as_ref()?;
        let block = self.block.block_bytes();
        Some(RawIdxEntry {
            key: &block[s.key_range.clone()],
            lsn: s.lsn,
            val_type: s.val_type,
            val_bytes: &block[s.val_range.clone()],
        })
    }

    /// Advances the cursor to the previous entry in the segment without allocating.
    #[inline]
    pub fn advance(&mut self) -> io::Result<()> {
        if self.current_span.is_some() {
            self.advance_to_prev()?;
        }
        Ok(())
    }

    /// Consumes and returns the current [`IndexEntry`], advancing the cursor to the previous entry.
    pub fn next_entry(&mut self) -> io::Result<Option<IndexEntry>> {
        let Some(raw) = self.peek_raw() else {
            return Ok(None);
        };
        let owned = raw.to_owned_entry()?;
        self.advance_to_prev()?;
        Ok(Some(owned))
    }

    fn advance_to_prev(&mut self) -> io::Result<()> {
        self.current_span = None;
        while self.entry_idx == 0 {
            let Some(b) = self.next_block_idx else {
                return Ok(());
            };
            self.next_block_idx = b.checked_sub(1);
            let reader = self.block.load(self.seg, b, self.populate_cache)?;
            self.num_entries_in_block = reader.num_entries();
            self.entry_idx = self.num_entries_in_block;
        }

        self.entry_idx -= 1;
        let reader =
            IdxBlockReader::from_verified_buf(self.block.block_bytes(), self.num_entries_in_block);
        let span = reader.parse_entry_span_at(self.entry_idx)?;
        self.current_span = Some(span);
        Ok(())
    }
}

/// Bounded-memory block-by-block cursor over an [`IdxSegmentReader`].
///
/// Borrows cached `Arc<[u8; IDX_BLOCK_SIZE]>` blocks zero-copy on cache hit and falls back to an
/// on-demand 4 KB scratch buffer (or a sequential read-ahead buffer during compaction) only on miss.
pub struct IdxSegmentCursor<'a, F: PlatformFile> {
    seg: &'a IdxSegmentReader<F>,
    next_block_idx: u32,
    block: CursorBlockStorage,
    entry_idx: usize,
    num_entries_in_block: usize,
    current_span: Option<CursorEntrySpan>,
    populate_cache: bool,
}

impl<'a, F: PlatformFile> IdxSegmentCursor<'a, F> {
    fn new_all(
        seg: &'a IdxSegmentReader<F>,
        populate_cache: bool,
        block: CursorBlockStorage,
    ) -> io::Result<Self> {
        let mut cursor = Self {
            seg,
            next_block_idx: 0,
            block,
            entry_idx: 0,
            num_entries_in_block: 0,
            current_span: None,
            populate_cache,
        };
        cursor.advance_to_next()?;
        Ok(cursor)
    }

    fn new_from(
        seg: &'a IdxSegmentReader<F>,
        lower_key: &[u8],
        lower_inclusive: bool,
        populate_cache: bool,
    ) -> io::Result<Self> {
        let mut cursor = Self {
            seg,
            next_block_idx: 0,
            block: CursorBlockStorage::new(),
            entry_idx: 0,
            num_entries_in_block: 0,
            current_span: None,
            populate_cache,
        };
        if seg.fence_pointers.is_empty() || seg.num_blocks == 0 {
            return Ok(cursor);
        }

        let start_pos = seg
            .fence_pointers
            .partition_point(|fp| fp.first_key.as_ref() <= lower_key);
        cursor.next_block_idx = start_pos.saturating_sub(1) as u32;

        while cursor.next_block_idx < seg.num_blocks {
            let b = cursor.next_block_idx;
            cursor.next_block_idx += 1;
            let reader = cursor.block.load(seg, b, cursor.populate_cache)?;
            cursor.num_entries_in_block = reader.num_entries();

            // Binary-search the u16 offset directory for the first key satisfying the lower bound
            let mut low = 0usize;
            let mut high = cursor.num_entries_in_block;
            while low < high {
                let mid = low + (high - low) / 2;
                let k = reader.key_at(mid)?;
                let before = if lower_inclusive {
                    k < lower_key
                } else {
                    k <= lower_key
                };
                if before {
                    low = mid + 1;
                } else {
                    high = mid;
                }
            }

            if low < cursor.num_entries_in_block {
                let first_span = reader.parse_entry_span_at(low)?;
                cursor.entry_idx = low + 1;
                cursor.current_span = Some(first_span);
                break;
            } else {
                cursor.entry_idx = cursor.num_entries_in_block;
            }
        }
        Ok(cursor)
    }

    /// Returns the raw key slice of the current entry without heap allocation.
    #[inline]
    pub fn current_key(&self) -> Option<&[u8]> {
        let s = self.current_span.as_ref()?;
        let block = self.block.block_bytes();
        Some(&block[s.key_range.clone()])
    }

    /// Returns a zero-copy [`RawIdxEntry`] view of the current entry borrowed from `self.block`.
    #[inline]
    pub fn peek_raw(&self) -> Option<RawIdxEntry<'_>> {
        let s = self.current_span.as_ref()?;
        let block = self.block.block_bytes();
        Some(RawIdxEntry {
            key: &block[s.key_range.clone()],
            lsn: s.lsn,
            val_type: s.val_type,
            val_bytes: &block[s.val_range.clone()],
        })
    }

    /// Advances the cursor to the next entry in the segment without allocating.
    #[inline]
    pub fn advance(&mut self) -> io::Result<()> {
        if self.current_span.is_some() {
            self.advance_to_next()?;
        }
        Ok(())
    }

    /// Consumes and returns the current [`IndexEntry`], advancing the cursor to the next entry.
    pub fn next_entry(&mut self) -> io::Result<Option<IndexEntry>> {
        let Some(raw) = self.peek_raw() else {
            return Ok(None);
        };
        let owned = raw.to_owned_entry()?;
        self.advance_to_next()?;
        Ok(Some(owned))
    }

    fn advance_to_next(&mut self) -> io::Result<()> {
        self.current_span = None;
        while self.entry_idx >= self.num_entries_in_block {
            if self.next_block_idx >= self.seg.num_blocks {
                return Ok(());
            }
            let b = self.next_block_idx;
            self.next_block_idx += 1;
            let reader = self.block.load(self.seg, b, self.populate_cache)?;
            self.num_entries_in_block = reader.num_entries();
            self.entry_idx = 0;
        }

        let reader =
            IdxBlockReader::from_verified_buf(self.block.block_bytes(), self.num_entries_in_block);
        let span = reader.parse_entry_span_at(self.entry_idx)?;
        self.entry_idx += 1;
        self.current_span = Some(span);
        Ok(())
    }
}

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
                match &mut winning_meta {
                    None => {
                        winning_meta = Some((raw.lsn, raw.val_type));
                        winning_val_buf.clear();
                        winning_val_buf.extend_from_slice(raw.val_bytes);
                    }
                    Some((win_lsn, win_vtype)) => {
                        if raw.lsn > *win_lsn {
                            if *win_vtype == 2 && winning_val_buf.len() == BLOB_PTR_ENCODED_LEN {
                                let k = winning_val_buf[0];
                                let tid =
                                    u16::from_le_bytes([winning_val_buf[1], winning_val_buf[2]]);
                                let sid =
                                    u64::from_le_bytes(winning_val_buf[3..11].try_into().unwrap());
                                let blen =
                                    u32::from_le_bytes(winning_val_buf[15..19].try_into().unwrap());
                                *dead_blob_map.entry((k, tid, sid)).or_insert(0) += blen as u64;
                            }
                            *win_lsn = raw.lsn;
                            *win_vtype = raw.val_type;
                            winning_val_buf.clear();
                            winning_val_buf.extend_from_slice(raw.val_bytes);
                        } else if raw.val_type == 2 && raw.val_bytes.len() == BLOB_PTR_ENCODED_LEN {
                            let k = raw.val_bytes[0];
                            let tid = u16::from_le_bytes([raw.val_bytes[1], raw.val_bytes[2]]);
                            let sid = u64::from_le_bytes(raw.val_bytes[3..11].try_into().unwrap());
                            let blen =
                                u32::from_le_bytes(raw.val_bytes[15..19].try_into().unwrap());
                            *dead_blob_map.entry((k, tid, sid)).or_insert(0) += blen as u64;
                        }
                    }
                }
                cur.advance()?;
            }
        }

        if let Some((lsn, val_type)) = winning_meta {
            if drop_tombstones && val_type == 3 {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::{DEFAULT_MEMTABLE_CAPACITY_BYTES, MemTable};
    use crate::pal::OsFs;
    use crate::util::test_util::TempTestDir;

    #[test]
    fn idx_segment_fence_pointers_bloom_and_compaction() {
        let tmp = TempTestDir::new("idx_compact");
        let fs = OsFs::open(&tmp.path, true).expect("open OsFs");

        // Segment 1 (Older, LSN 1..=200)
        let mut mt1 = MemTable::new(DEFAULT_MEMTABLE_CAPACITY_BYTES);
        for i in 0..200u32 {
            let key = format!("user_key_{i:05}").into_bytes();
            if i == 50 {
                // Key 50 holds a BlobPtr in Segment 1 that will be overwritten in Segment 2
                mt1.put_blob_ptr(
                    key,
                    BlobPtr {
                        kind: BlobKind::Standard,
                        table_id: 0,
                        seg_id: 10,
                        offset: 0,
                        len: 16384,
                        crc32c: 0x9999,
                    },
                    (i + 1) as u64,
                );
            } else {
                mt1.put_inline(key, format!("val_v1_{i}").into_bytes(), (i + 1) as u64);
            }
        }
        let seg1 = mt1.flush_to_segment(&fs, 1, 10).expect("flush seg 1");
        assert!(seg1.num_blocks() >= 2);

        // Reopen Segment 1 from disk to test Footer + Bloom + FencePointer deserialization
        drop(seg1);
        let seg1 = IdxSegmentReader::open(&fs, 1).expect("reopen seg 1");

        // Verify `[min_key, max_key]` key-range metadata & pruning on `seg1`
        assert_eq!(seg1.min_key(), b"user_key_00000");
        assert_eq!(seg1.max_key(), b"user_key_00199");
        assert!(!seg1.may_contain_key(b"aaa_before_min"));
        assert!(!seg1.may_contain_key(b"zzz_after_max"));
        assert!(seg1.may_contain_key(b"user_key_00042"));
        assert!(!seg1.overlaps_range(b"zzz_0", Some(b"zzz_9")));
        assert!(!seg1.overlaps_range(b"aaa_0", Some(b"aaa_9")));
        assert!(seg1.overlaps_range(b"user_key_00100", Some(b"user_key_00150")));

        // Point lookups on Segment 1
        assert_eq!(
            seg1.get(b"user_key_00042").unwrap(),
            Some((43, IndexValue::Inline(b"val_v1_42".to_vec())))
        );
        assert_eq!(seg1.get(b"nonexistent_key").unwrap(), None);

        // Segment 2 (Newer, LSN 500..): Overwrites key 50 and deletes key 100
        let mut mt2 = MemTable::new(DEFAULT_MEMTABLE_CAPACITY_BYTES);
        mt2.put_inline(
            b"user_key_00050".to_vec(),
            b"val_v2_50_inline".to_vec(),
            500,
        );
        mt2.delete(b"user_key_00100".to_vec(), 501);
        let seg2 = mt2.flush_to_segment(&fs, 2, 10).expect("flush seg 2");

        // Compact Segment 2 and Segment 1 into Segment 3 with drop_tombstones = true
        let comp = compact_idx_segments(&fs, &[&seg2, &seg1], 3, 1, 10, true).expect("compact");
        assert_eq!(comp.new_segment.record_count(), 199); // 200 - 1 deleted key (key 100)
        assert_eq!(
            comp.new_segment.get(b"user_key_00050").unwrap(),
            Some((500, IndexValue::Inline(b"val_v2_50_inline".to_vec())))
        );
        assert_eq!(comp.new_segment.get(b"user_key_00100").unwrap(), None);

        // Verify that overwriting key 50's BlobPtr reported 16384 newly dead blob bytes on seg_id 10!
        assert_eq!(
            comp.newly_dead_blob_bytes,
            vec![(BlobKind::Standard, 0, 10, 16384)]
        );

        // Verify that a crafted index segment with an inflated `fp_count` (even with a
        // valid recomputed footer_crc32c) is rejected before allocating `fence_pointers`.
        drop(comp);
        let seg_fname = crate::meta::format_idx_file_name(3);
        let seg_file = fs
            .open_file(
                &seg_fname,
                false,
                true,
                crate::pal::OpenHint::ImmutableRandom,
            )
            .unwrap();
        let flen = seg_file.file_len().unwrap() as usize;
        let mut raw_seg = vec![0u8; flen];
        seg_file.read_exact_at(&mut raw_seg, 0).unwrap();

        let footer_start = flen - 48;
        let meta_offset = u64::from_le_bytes(
            raw_seg[footer_start + 24..footer_start + 32]
                .try_into()
                .unwrap(),
        ) as usize;
        // `meta_buf` starts with `[num_probes: u8 | reserved: [u8; 3] | bit_bytes_len: u32 LE | bits...]`,
        // followed by `fp_count: u32`.
        let bloom_byte_len = u32::from_le_bytes(
            raw_seg[meta_offset + 4..meta_offset + 8]
                .try_into()
                .unwrap(),
        ) as usize;
        let fp_count_pos = meta_offset + 8 + bloom_byte_len;
        // Overwrite `fp_count` with `u32::MAX` and recompute both `meta_crc` and `footer_crc32c`
        raw_seg[fp_count_pos..fp_count_pos + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        let new_meta_crc = crate::util::crc32c::crc32c(&raw_seg[meta_offset..footer_start]);
        raw_seg[footer_start + 36..footer_start + 40].copy_from_slice(&new_meta_crc.to_le_bytes());
        let new_footer_crc = crate::util::crc32c::crc32c(&raw_seg[footer_start..footer_start + 44]);
        raw_seg[footer_start + 44..footer_start + 48]
            .copy_from_slice(&new_footer_crc.to_le_bytes());
        seg_file.write_all_at(&raw_seg, 0).unwrap();
        drop(seg_file);

        let err = match IdxSegmentReader::open(&fs, 3) {
            Ok(_) => panic!("crafted huge fp_count with valid CRC must be rejected"),
            Err(e) => e,
        };
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn point_lookup_uses_cached_block_and_counts_hits_and_misses() {
        let tmp = TempTestDir::new("idx_get_cache");
        let fs = OsFs::open(&tmp.path, true).expect("open OsFs");
        let mut mt = MemTable::new(DEFAULT_MEMTABLE_CAPACITY_BYTES);
        for i in 0..200u32 {
            mt.put_inline(
                format!("key_{i:05}").into_bytes(),
                format!("val_{i}").into_bytes(),
                (i + 1) as u64,
            );
        }
        let mut seg = mt.flush_to_segment(&fs, 1, 10).expect("flush");
        let cache = Arc::new(VerifiedBlockCache::new(1 << 20));
        seg.set_block_cache(Some(Arc::clone(&cache)));

        // The first lookup reads the block from disk and populates the cache; later lookups of
        // keys in the same block are served from it. Each lookup counts exactly one hit or miss.
        let expect = |i: u32| {
            Some((
                (i + 1) as u64,
                IndexValue::Inline(format!("val_{i}").into_bytes()),
            ))
        };
        assert_eq!(seg.get(b"key_00007").unwrap(), expect(7));
        assert_eq!((cache.hits(), cache.misses()), (0, 1));
        assert_eq!(seg.get(b"key_00007").unwrap(), expect(7));
        assert_eq!((cache.hits(), cache.misses()), (1, 1));
        for i in 0..200u32 {
            assert_eq!(
                seg.get(format!("key_{i:05}").as_bytes()).unwrap(),
                expect(i)
            );
        }
        assert_eq!(cache.hits() + cache.misses(), 202);
        assert!(cache.hits() > 190);
        assert_eq!(seg.get(b"key_99999").unwrap(), None);
    }

    #[test]
    fn hashed_lookup_and_visitor_match_get_for_every_value_type() {
        let tmp = TempTestDir::new("idx_visit_hashed");
        let fs = OsFs::open(&tmp.path, true).expect("open OsFs");
        let ptr = BlobPtr {
            kind: BlobKind::Standard,
            table_id: 3,
            seg_id: 9,
            offset: 128,
            len: 4096,
            crc32c: 0xABCD,
        };
        let mut mt = MemTable::new(DEFAULT_MEMTABLE_CAPACITY_BYTES);
        mt.put_inline(b"inline".to_vec(), b"payload".to_vec(), 1);
        mt.put_blob_ptr(b"blob".to_vec(), ptr, 2);
        mt.delete(b"gone".to_vec(), 3);
        let mut seg = mt.flush_to_segment(&fs, 1, 10).expect("flush");
        seg.set_block_cache(Some(Arc::new(VerifiedBlockCache::new(1 << 20))));

        // Run twice so that both the disk path and the cached-block path are exercised.
        for _ in 0..2 {
            for key in [&b"inline"[..], b"blob", b"gone", b"absent"] {
                let hash = BloomFilter::hash_pair(key);
                let owned = seg.get(key).unwrap();
                assert_eq!(seg.get_hashed(key, hash).unwrap(), owned);
                let visited = seg
                    .visit_hashed(key, hash, |raw| {
                        Ok((raw.lsn, raw.val_type, raw.val_bytes.to_vec()))
                    })
                    .unwrap();
                match (&owned, &visited) {
                    (None, None) => {}
                    (Some((lsn, IndexValue::Inline(v))), Some((l, 1, b))) => {
                        assert_eq!((lsn, v), (l, b));
                    }
                    (Some((lsn, IndexValue::Blob(p))), Some((l, 2, b))) => {
                        assert_eq!(lsn, l);
                        assert_eq!(*p, RawIdxEntry::decode_blob_ptr(b).unwrap());
                    }
                    (Some((lsn, IndexValue::Tombstone)), Some((l, 3, _))) => assert_eq!(lsn, l),
                    other => panic!("visitor and owned lookup disagree: {other:?}"),
                }
            }
        }
    }

    #[test]
    fn idx_builder_finish_rejects_metadata_exceeding_max_index_meta_bytes() {
        let tmp = TempTestDir::new("idx_meta_cap");
        let fs = OsFs::open(&tmp.path, true).expect("open OsFs");
        let bits_per_key = (MAX_INDEX_META_BYTES + 1024) * 8;
        let mut builder = IdxSegmentBuilder::create(&fs, 1, 0, bits_per_key).unwrap();
        builder.add_raw_entry(b"k1", 1, 1, b"v1").unwrap();
        let err = builder
            .finish()
            .err()
            .expect("oversized idx metadata must be rejected before writing footer");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn idx_open_rejects_mismatched_unsorted_or_missing_max_key_metadata() {
        let tmp = TempTestDir::new("idx_fence_validate");
        let fs = OsFs::open(&tmp.path, true).expect("open OsFs");

        let mut mt = MemTable::new(DEFAULT_MEMTABLE_CAPACITY_BYTES);
        let payload = vec![0x42u8; 256];
        for i in 0..80u32 {
            mt.put_inline(
                format!("k_{i:04}").into_bytes(),
                payload.clone(),
                (i + 1) as u64,
            );
        }
        let seg = mt.flush_to_segment(&fs, 1, 10).unwrap();
        assert!(seg.num_blocks() >= 3);
        let expected_max_key = seg.max_key().to_vec();
        drop(seg);

        let fname = format_idx_file_name(1);
        let orig_bytes = std::fs::read(tmp.path.join(&fname)).unwrap();
        let footer_start = orig_bytes.len() - IDX_FOOTER_SIZE;
        let footer: [u8; IDX_FOOTER_SIZE] = orig_bytes[footer_start..].try_into().unwrap();
        let meta_offset = u64::from_le_bytes(footer[24..32].try_into().unwrap()) as usize;
        let num_blocks = u32::from_le_bytes(footer[16..20].try_into().unwrap());
        let orig_meta = &orig_bytes[meta_offset..footer_start];

        // Parse original Bloom + fence pointers to build targeted crafted trailers.
        let mut s = orig_meta;
        let bloom = BloomFilter::decode(&mut s).unwrap();
        let mut bloom_bytes = Vec::new();
        bloom.encode(&mut bloom_bytes);
        let fp_count = u32::from_le_bytes(s[0..4].try_into().unwrap()) as usize;
        assert_eq!(fp_count, num_blocks as usize);
        s = &s[4..];
        let mut fps = Vec::with_capacity(fp_count);
        for _ in 0..fp_count {
            let klen = u16::from_le_bytes([s[0], s[1]]) as usize;
            let bidx = u32::from_le_bytes([s[2], s[3], s[4], s[5]]);
            let fk = s[6..6 + klen].to_vec();
            s = &s[6 + klen..];
            fps.push((fk, bidx));
        }

        let write_crafted = |data: &[u8], new_meta: &[u8]| {
            let mut raw = data.to_vec();
            raw.truncate(meta_offset);
            raw.extend_from_slice(new_meta);
            let mut crafted_footer = footer;
            crafted_footer[32..36].copy_from_slice(&(new_meta.len() as u32).to_le_bytes());
            crafted_footer[36..40].copy_from_slice(&crc32c(new_meta).to_le_bytes());
            let fcrc = crc32c(&crafted_footer[0..44]);
            crafted_footer[44..48].copy_from_slice(&fcrc.to_le_bytes());
            raw.extend_from_slice(&crafted_footer);
            std::fs::write(tmp.path.join(&fname), &raw).unwrap();
        };

        let encode_fps = |entries: &[(Vec<u8>, u32)], max_tail: Option<&[u8]>| -> Vec<u8> {
            let mut m = bloom_bytes.clone();
            m.extend_from_slice(&(entries.len() as u32).to_le_bytes());
            for (fk, bidx) in entries {
                m.extend_from_slice(&(fk.len() as u16).to_le_bytes());
                m.extend_from_slice(&bidx.to_le_bytes());
                m.extend_from_slice(fk);
            }
            if let Some(tail) = max_tail {
                m.extend_from_slice(tail);
            }
            m
        };

        // 1. Mismatched `fp_count < num_blocks` must fail with InvalidData.
        let short_meta = encode_fps(&fps[..1], Some(&[2, 0, b'z', b'z']));
        write_crafted(&orig_bytes, &short_meta);
        assert_eq!(
            IdxSegmentReader::open(&fs, 1).err().unwrap().kind(),
            io::ErrorKind::InvalidData
        );

        // 2. Unsorted fence pointer keys must fail with InvalidData.
        let mut swapped = fps.clone();
        let (k0, k1) = (swapped[0].0.clone(), swapped[1].0.clone());
        swapped[0].0 = k1;
        swapped[1].0 = k0;
        let mut normal_tail = Vec::new();
        normal_tail.extend_from_slice(&(expected_max_key.len() as u16).to_le_bytes());
        normal_tail.extend_from_slice(&expected_max_key);
        write_crafted(&orig_bytes, &encode_fps(&swapped, Some(&normal_tail)));
        assert_eq!(
            IdxSegmentReader::open(&fs, 1).err().unwrap().kind(),
            io::ErrorKind::InvalidData
        );

        // 3. Trailing garbage bytes after `max_key` must fail with InvalidData.
        let mut garbage_tail = normal_tail.clone();
        garbage_tail.push(0xAA);
        write_crafted(&orig_bytes, &encode_fps(&fps, Some(&garbage_tail)));
        assert_eq!(
            IdxSegmentReader::open(&fs, 1).err().unwrap().kind(),
            io::ErrorKind::InvalidData
        );

        // 4. Single truncated length byte in `max_key` tail must fail with InvalidData.
        write_crafted(&orig_bytes, &encode_fps(&fps, Some(&[1u8])));
        assert_eq!(
            IdxSegmentReader::open(&fs, 1).err().unwrap().kind(),
            io::ErrorKind::InvalidData
        );

        // 5. Segment omitting trailing `max_key` (pre-release legacy shape) must fail with
        //    InvalidData even when the last data block is intact.
        let legacy_meta = encode_fps(&fps, None);
        write_crafted(&orig_bytes, &legacy_meta);
        assert_eq!(
            IdxSegmentReader::open(&fs, 1).err().unwrap().kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn idx_segment_cursor_borrows_cached_block_zero_copy_and_shrinks_struct() {
        assert!(std::mem::size_of::<IdxSegmentCursor<'_, crate::pal::OsFile>>() < 512);
        assert!(std::mem::size_of::<IdxSegmentRevCursor<'_, crate::pal::OsFile>>() < 512);

        let tmp = TempTestDir::new("cursor_zero_copy");
        let fs = OsFs::open(&tmp.path, true).expect("open OsFs");
        let cache = Arc::new(VerifiedBlockCache::new(1024 * 1024));

        let mut builder = IdxSegmentBuilder::create(&fs, 1, 0, 10).unwrap();
        let val = vec![0x55u8; 64];
        for i in 0..200u32 {
            let k = format!("zc_key_{i:05}").into_bytes();
            builder.add_raw_entry(&k, i as u64 + 1, 1, &val).unwrap();
        }
        let mut reader = builder.finish().unwrap();
        assert!(reader.num_blocks() >= 3);
        reader.set_block_cache(Some(Arc::clone(&cache)));

        // Warm all blocks in VerifiedBlockCache.
        let mut warm = reader.cursor_from(b"zc_key_00000", true).unwrap();
        while warm.next_entry().unwrap().is_some() {}

        let fwd = reader.cursor_from(b"zc_key_00000", true).unwrap();
        assert!(fwd.block.scratch.is_none());
        assert!(matches!(fwd.block.active, ActiveBlock::Cached(_)));
        assert_eq!(fwd.current_key(), Some(b"zc_key_00000".as_slice()));

        let rev = reader.cursor_reverse_from(None).unwrap();
        assert!(rev.block.scratch.is_none());
        assert!(matches!(rev.block.active, ActiveBlock::Cached(_)));
        assert_eq!(rev.current_key(), Some(b"zc_key_00199".as_slice()));
    }

    #[test]
    fn compact_idx_segments_uses_batched_reads_instead_of_per_block_syscalls() {
        use std::path::Path;
        use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

        struct CountingReadFile {
            inner: Arc<<OsFs as PlatformFs>::File>,
            is_idx: bool,
            read_exact_calls: Arc<AtomicUsize>,
        }

        impl PlatformFile for CountingReadFile {
            fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
                self.inner.read_at(buf, offset)
            }
            fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
                if self.is_idx {
                    self.read_exact_calls.fetch_add(1, AtomicOrdering::Relaxed);
                }
                self.inner.read_exact_at(buf, offset)
            }
            fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
                self.inner.write_all_at(buf, offset)
            }
            fn file_len(&self) -> io::Result<u64> {
                self.inner.file_len()
            }
            fn preallocate(
                &self,
                target_len: u64,
                strategy: crate::pal::PreallocStrategy,
            ) -> io::Result<()> {
                self.inner.preallocate(target_len, strategy)
            }
            fn sync_barrier(&self) -> io::Result<()> {
                self.inner.sync_barrier()
            }
            fn sync_durable(&self) -> io::Result<()> {
                self.inner.sync_durable()
            }
            fn evict_os_cache(&self, offset: u64, len: u64) -> io::Result<()> {
                self.inner.evict_os_cache(offset, len)
            }
        }

        struct CountingReadFs {
            inner: OsFs,
            read_exact_calls: Arc<AtomicUsize>,
        }

        impl PlatformFs for CountingReadFs {
            type File = CountingReadFile;

            fn open_file(
                &self,
                rel_name: &str,
                create: bool,
                writable: bool,
                hint: OpenHint,
            ) -> io::Result<Arc<Self::File>> {
                Ok(Arc::new(CountingReadFile {
                    inner: self.inner.open_file(rel_name, create, writable, hint)?,
                    is_idx: rel_name.starts_with("idx_"),
                    read_exact_calls: Arc::clone(&self.read_exact_calls),
                }))
            }
            fn remove_file(&self, rel_name: &str) -> io::Result<()> {
                self.inner.remove_file(rel_name)
            }
            fn list_files(&self) -> io::Result<Vec<String>> {
                self.inner.list_files()
            }
            fn root_path(&self) -> &Path {
                self.inner.root_path()
            }
        }

        let tmp = TempTestDir::new("batched_compaction_reads");
        let calls = Arc::new(AtomicUsize::new(0));
        let fs = CountingReadFs {
            inner: OsFs::open(&tmp.path, true).expect("open OsFs"),
            read_exact_calls: Arc::clone(&calls),
        };

        let val = vec![0x3Cu8; 64];
        for (seq_id, start) in [(1u64, 0u32), (2u64, 1u32)] {
            let mut b = IdxSegmentBuilder::create(&fs, seq_id, 0, 10).unwrap();
            for i in (start..2400u32).step_by(2) {
                let k = format!("batch_key_{i:05}").into_bytes();
                b.add_raw_entry(&k, i as u64 + 1, 1, &val).unwrap();
            }
            let r = b.finish().unwrap();
            assert!(r.num_blocks() >= 16);
        }

        let s1 = IdxSegmentReader::open(&fs, 1).unwrap();
        let s2 = IdxSegmentReader::open(&fs, 2).unwrap();
        calls.store(0, AtomicOrdering::Relaxed);

        let res = compact_idx_segments(&fs, &[&s1, &s2], 3, 1, 10, true).unwrap();
        let syscalls = calls.load(AtomicOrdering::Relaxed);
        assert!(
            syscalls <= 4,
            "expected batched compaction reads (<= 4 syscalls across >= 32 blocks), got {syscalls}"
        );
        let merged = res.new_segment.scan_all().unwrap();
        assert_eq!(merged.len(), 2400);
        for (idx, entry) in merged.iter().enumerate() {
            assert_eq!(entry.key, format!("batch_key_{idx:05}").into_bytes());
        }
    }

    #[test]
    fn idx_block_and_segment_builders_reject_malformed_and_unsorted_entries_without_corrupting_state()
     {
        let mut blk = IdxBlockBuilder::new();
        blk.add_raw_entry(b"k20", 1, 1, b"val20").unwrap();
        assert_eq!(blk.num_entries(), 1);
        assert_eq!(blk.last_key(), Some(b"k20".as_slice()));

        // Invalid type tag, wrong BlobPtr length, non-empty tombstone, duplicate key, and unsorted key
        // must all fail cleanly BEFORE mutating `offsets` or `write_pos`.
        assert_eq!(
            blk.add_raw_entry(b"k30", 2, 99, b"").unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(
            blk.add_raw_entry(b"k30", 2, 2, &[0u8; 12])
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(
            blk.add_raw_entry(b"k30", 2, 3, b"x").unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(
            blk.add_raw_entry(b"k20", 2, 1, b"dup").unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(
            blk.add_raw_entry(b"k10", 2, 1, b"back").unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(blk.num_entries(), 1);

        blk.add_raw_entry(b"k30", 3, 1, b"val30").unwrap();
        let raw_block = blk.finish_and_reset();
        let reader = IdxBlockReader::open_verified(&raw_block).unwrap();
        assert_eq!(reader.num_entries, 2);
        assert_eq!(
            reader.lookup_binary_search(b"k20").unwrap(),
            Some((1, IndexValue::Inline(b"val20".to_vec())))
        );
        assert_eq!(
            reader.lookup_binary_search(b"k30").unwrap(),
            Some((3, IndexValue::Inline(b"val30".to_vec())))
        );

        // IdxSegmentBuilder must also reject out-of-order / duplicate keys across block boundaries
        // and oversized single entries without corrupting previously buffered entries.
        let tmp = TempTestDir::new("builder_validation");
        let fs = OsFs::open(&tmp.path, true).expect("open OsFs");
        let mut seg = IdxSegmentBuilder::create(&fs, 7, 0, 10).unwrap();
        let pad_val = vec![0x55u8; 512];
        for i in 0..16u32 {
            let k = format!("k_{i:04}").into_bytes();
            seg.add_raw_entry(&k, i as u64 + 1, 1, &pad_val).unwrap();
        }
        assert!(seg.fence_pointers.len() >= 2);
        assert_eq!(
            seg.add_raw_entry(b"k_0005", 100, 1, b"v")
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(
            seg.add_raw_entry(b"k_0015", 100, 1, b"v")
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(
            seg.add_entry(&IndexEntry {
                key: b"z_huge".to_vec(),
                lsn: 101,
                value: IndexValue::Inline(vec![0u8; IDX_BLOCK_SIZE]),
            })
            .unwrap_err()
            .kind(),
            io::ErrorKind::InvalidInput
        );
        let seg_reader = seg.finish().unwrap();
        assert_eq!(seg_reader.scan_all().unwrap().len(), 16);
    }

    #[test]
    fn fence_pointer_seek_exact_block_boundary_skips_prior_block() {
        use std::path::Path;
        use std::sync::Mutex;

        struct SpyOffsetFile {
            inner: Arc<<OsFs as PlatformFs>::File>,
            is_idx: bool,
            offsets: Arc<Mutex<Vec<u64>>>,
        }

        impl PlatformFile for SpyOffsetFile {
            fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
                self.inner.read_at(buf, offset)
            }
            fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
                if self.is_idx {
                    self.offsets.lock().unwrap().push(offset);
                }
                self.inner.read_exact_at(buf, offset)
            }
            fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
                self.inner.write_all_at(buf, offset)
            }
            fn file_len(&self) -> io::Result<u64> {
                self.inner.file_len()
            }
            fn preallocate(
                &self,
                target_len: u64,
                strategy: crate::pal::PreallocStrategy,
            ) -> io::Result<()> {
                self.inner.preallocate(target_len, strategy)
            }
            fn sync_barrier(&self) -> io::Result<()> {
                self.inner.sync_barrier()
            }
            fn sync_durable(&self) -> io::Result<()> {
                self.inner.sync_durable()
            }
            fn evict_os_cache(&self, offset: u64, len: u64) -> io::Result<()> {
                self.inner.evict_os_cache(offset, len)
            }
        }

        struct SpyOffsetFs {
            inner: OsFs,
            offsets: Arc<Mutex<Vec<u64>>>,
        }

        impl PlatformFs for SpyOffsetFs {
            type File = SpyOffsetFile;

            fn open_file(
                &self,
                rel_name: &str,
                create: bool,
                writable: bool,
                hint: OpenHint,
            ) -> io::Result<Arc<Self::File>> {
                Ok(Arc::new(SpyOffsetFile {
                    inner: self.inner.open_file(rel_name, create, writable, hint)?,
                    is_idx: rel_name.starts_with("idx_"),
                    offsets: Arc::clone(&self.offsets),
                }))
            }
            fn remove_file(&self, rel_name: &str) -> io::Result<()> {
                self.inner.remove_file(rel_name)
            }
            fn list_files(&self) -> io::Result<Vec<String>> {
                self.inner.list_files()
            }
            fn root_path(&self) -> &Path {
                self.inner.root_path()
            }
        }

        let tmp = TempTestDir::new("fence_exact_boundary");
        let offsets = Arc::new(Mutex::new(Vec::new()));
        let fs = SpyOffsetFs {
            inner: OsFs::open(&tmp.path, true).expect("open OsFs"),
            offsets: Arc::clone(&offsets),
        };

        let val = vec![0x7Au8; 512];
        let mut seg = IdxSegmentBuilder::create(&fs, 1, 0, 10).unwrap();
        for i in 0..32u32 {
            let k = format!("fp_{i:04}").into_bytes();
            seg.add_raw_entry(&k, i as u64 + 1, 1, &val).unwrap();
        }
        let seg_reader = seg.finish().unwrap();
        assert!(seg_reader.fence_pointers.len() >= 4);
        let target_key = seg_reader.fence_pointers[2].first_key.to_vec();
        let expected_offset = 2 * IDX_BLOCK_SIZE as u64;

        // 1. cursor_from (inclusive) seeking to block 2's exact first_key must read block 2 directly.
        offsets.lock().unwrap().clear();
        let cur = seg_reader.cursor_from(&target_key, true).unwrap();
        assert_eq!(cur.current_key(), Some(target_key.as_slice()));
        assert_eq!(*offsets.lock().unwrap(), vec![expected_offset]);

        // 2. scan_prefix_range seeking to block 2's exact first_key must also skip block 1.
        offsets.lock().unwrap().clear();
        let rows = seg_reader
            .scan_prefix_range(b"fp_", &target_key, None, 1)
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].key, target_key);
        assert_eq!(*offsets.lock().unwrap(), vec![expected_offset]);
    }

    #[test]
    fn idx_block_builder_finish_into_zeroes_padding_across_reused_blocks() {
        let mut builder = IdxBlockBuilder::new();
        let noisy = vec![0xFFu8; 512];
        for i in 0..6u32 {
            let k = format!("noisy_{i:02}").into_bytes();
            builder.add_raw_entry(&k, i as u64 + 1, 1, &noisy).unwrap();
        }
        let mut out = Vec::new();
        builder.finish_into(&mut out);
        assert_eq!(out.len(), IDX_BLOCK_SIZE);
        let b1: &[u8; IDX_BLOCK_SIZE] = (&out[..IDX_BLOCK_SIZE]).try_into().unwrap();
        assert!(IdxBlockReader::open_verified(b1).is_ok());

        // Second block contains only one short entry, leaving > 4000 bytes of padding that used
        // to hold 0xFF from the first block.
        out.clear();
        builder.add_raw_entry(b"tiny", 10, 1, b"ok").unwrap();
        builder.finish_into(&mut out);
        assert_eq!(out.len(), IDX_BLOCK_SIZE);
        let b2: &[u8; IDX_BLOCK_SIZE] = (&out[..IDX_BLOCK_SIZE]).try_into().unwrap();
        let reader2 = IdxBlockReader::open_verified(b2).unwrap();
        assert_eq!(reader2.num_entries(), 1);
        assert_eq!(
            reader2.lookup_binary_search(b"tiny").unwrap(),
            Some((10, IndexValue::Inline(b"ok".to_vec())))
        );

        let entry_end = 11 + 4 + 2 + 2;
        let dir_start = IDX_BLOCK_USABLE_SIZE - 2;
        assert!(
            b2[entry_end..dir_start].iter().all(|&b| b == 0),
            "unused gap in reused IdxBlockBuilder must be zeroed"
        );
    }

    #[test]
    fn empty_cursors_and_exclusive_min_upper_bound_return_none_without_panicking() {
        let tmp = TempTestDir::new("empty_cursor_no_panic");
        let fs = OsFs::open(&tmp.path, true).expect("open OsFs");

        // 1. Non-empty segment: forward seek past max_key and reverse seek with exclusive min_key.
        let mut builder = IdxSegmentBuilder::create(&fs, 1, 0, 10).unwrap();
        for i in 1..=10u32 {
            let k = format!("k_{i:04}").into_bytes();
            builder.add_raw_entry(&k, i as u64, 1, b"val").unwrap();
        }
        let seg = builder.finish().unwrap();

        let mut fwd_past = seg.cursor_from(b"k_0010", false).unwrap();
        assert_eq!(fwd_past.current_key(), None);
        assert_eq!(fwd_past.peek_raw(), None);
        assert_eq!(fwd_past.next_entry().unwrap(), None);
        fwd_past.advance().unwrap();

        let mut rev_below_min = seg
            .cursor_reverse_from(Some((seg.min_key(), false)))
            .unwrap();
        assert_eq!(rev_below_min.current_key(), None);
        assert_eq!(rev_below_min.peek_raw(), None);
        assert_eq!(rev_below_min.next_entry().unwrap(), None);
        rev_below_min.advance().unwrap();

        // 2. 0-block segment (e.g. produced after full compaction drops all tombstones).
        let empty_builder = IdxSegmentBuilder::create(&fs, 2, 1, 10).unwrap();
        let empty_seg = empty_builder.finish().unwrap();
        assert_eq!(empty_seg.num_blocks(), 0);

        for mut cur in [
            empty_seg.cursor_all().unwrap(),
            empty_seg.cursor_all_batched(4).unwrap(),
            empty_seg.cursor_from(b"k_0001", true).unwrap(),
        ] {
            assert_eq!(cur.current_key(), None);
            assert_eq!(cur.peek_raw(), None);
            assert_eq!(cur.next_entry().unwrap(), None);
            cur.advance().unwrap();
        }
        let mut rev_empty = empty_seg.cursor_reverse_from(None).unwrap();
        assert_eq!(rev_empty.current_key(), None);
        assert_eq!(rev_empty.peek_raw(), None);
        assert_eq!(rev_empty.next_entry().unwrap(), None);
        rev_empty.advance().unwrap();
    }

    #[test]
    fn cursor_load_error_leaves_cursor_empty_without_panicking() {
        let tmp = TempTestDir::new("cursor_load_error_state");
        let fs = OsFs::open(&tmp.path, true).expect("open OsFs");

        let mut builder = IdxSegmentBuilder::create(&fs, 1, 0, 10).unwrap();
        let val = vec![0x42u8; 512];
        for i in 0..24u32 {
            let k = format!("key_{i:04}").into_bytes();
            builder.add_raw_entry(&k, i as u64 + 1, 1, &val).unwrap();
        }
        let seg = builder.finish().unwrap();
        assert!(seg.num_blocks() >= 2);
        drop(seg);

        // Corrupt block 1's CRC while leaving block 0 and footer valid.
        let fname = crate::meta::format_idx_file_name(1);
        let file = fs
            .open_file(&fname, false, true, OpenHint::ImmutableRandom)
            .unwrap();
        let mut b1 = [0u8; IDX_BLOCK_SIZE];
        file.read_exact_at(&mut b1, IDX_BLOCK_SIZE as u64).unwrap();
        b1[0] ^= 0xFF;
        file.write_all_at(&b1, IDX_BLOCK_SIZE as u64).unwrap();
        file.sync_barrier().unwrap();
        drop(file);

        // Single-block forward cursor: block 0 works, crossing into block 1 fails cleanly.
        let seg = IdxSegmentReader::open(&fs, 1).unwrap();
        let mut cur = seg.cursor_all().unwrap();
        loop {
            match cur.advance() {
                Ok(()) if cur.current_key().is_none() => panic!("expected block 1 corruption"),
                Ok(()) => {}
                Err(e) => {
                    assert_eq!(e.kind(), io::ErrorKind::InvalidData);
                    break;
                }
            }
        }
        assert_eq!(cur.current_key(), None);
        assert_eq!(cur.peek_raw(), None);

        // Batched read-ahead cursor: failing to load block 1 resets current_span and active block.
        let mut batched = seg.cursor_all_batched(1).unwrap();
        assert!(batched.current_key().is_some());
        loop {
            match batched.advance() {
                Ok(()) if batched.current_key().is_none() => {
                    panic!("expected block 1 corruption on batched cursor")
                }
                Ok(()) => {}
                Err(e) => {
                    assert_eq!(e.kind(), io::ErrorKind::InvalidData);
                    break;
                }
            }
        }
        assert_eq!(batched.current_key(), None);
        assert_eq!(batched.peek_raw(), None);

        // Reverse cursor: starting on last block then walking backward into corrupted block 1.
        let mut rev = seg.cursor_reverse_from(None).unwrap();
        assert!(rev.current_key().is_some());
        loop {
            match rev.advance() {
                Ok(()) if rev.current_key().is_none() => {
                    panic!("expected block 1 corruption on reverse cursor")
                }
                Ok(()) => {}
                Err(e) => {
                    assert_eq!(e.kind(), io::ErrorKind::InvalidData);
                    break;
                }
            }
        }
        assert_eq!(rev.current_key(), None);
        assert_eq!(rev.peek_raw(), None);
    }

    #[test]
    fn index_public_bounds_checks_reject_invalid_blob_kind_and_oob_indices() {
        let mut bb = IdxBlockBuilder::new();
        let mut bad_ptr = [0u8; BLOB_PTR_ENCODED_LEN];
        bad_ptr[0] = 99;
        let err = bb.add_raw_entry(b"k1", 1, 2, &bad_ptr).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);

        bb.add_raw_entry(b"k1", 1, 1, b"v1").unwrap();
        let mut raw_vec = Vec::new();
        bb.finish_into(&mut raw_vec);
        let raw_blk: [u8; IDX_BLOCK_SIZE] = raw_vec.try_into().unwrap();
        let reader = IdxBlockReader::open_verified(&raw_blk).unwrap();
        assert_eq!(
            reader.raw_entry_at(1).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(
            reader.decode_entry_at(5).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );

        let tmp = TempTestDir::new("idx_oob_read_block");
        let fs = OsFs::open(&tmp.path, true).unwrap();
        let mut sb = IdxSegmentBuilder::create(&fs, 2, 0, 10).unwrap();
        sb.add_raw_entry(b"k1", 1, 1, b"v1").unwrap();
        let seg = sb.finish().unwrap();
        let mut scratch = [0u8; IDX_BLOCK_SIZE];
        let oob_idx = seg.num_blocks();
        assert_eq!(
            seg.read_block(oob_idx, &mut scratch, false)
                .err()
                .unwrap()
                .kind(),
            io::ErrorKind::InvalidInput
        );

        let mut capped_sb =
            IdxSegmentBuilder::create(&fs, 3, 0, (MAX_INDEX_META_BYTES + 1024) * 8).unwrap();
        capped_sb.set_cap_bloom_to_meta_budget(true);
        capped_sb.add_raw_entry(b"k1", 1, 1, b"v1").unwrap();
        let capped_seg = capped_sb.finish().unwrap();
        assert_eq!(
            capped_seg.get(b"k1").unwrap(),
            Some((1, IndexValue::Inline(b"v1".to_vec())))
        );
    }

    #[test]
    fn idx_block_and_segment_structural_checks_reject_crafted_corruption() {
        // 1. `open_verified` rejects overlapping directory offsets and > MAX_IDX_BLOCK_ENTRIES.
        let mut bb = IdxBlockBuilder::new();
        bb.add_raw_entry(b"k1", 1, 1, b"v1").unwrap();
        bb.add_raw_entry(b"k2", 2, 1, b"v2").unwrap();
        let mut raw_vec = Vec::new();
        bb.finish_into(&mut raw_vec);
        let base_blk: [u8; IDX_BLOCK_SIZE] = raw_vec.try_into().unwrap();

        let mut overlap_blk = base_blk;
        let dir_start = IDX_BLOCK_USABLE_SIZE - 4;
        // Force second entry offset to overlap first entry (offset 2 < prev_end).
        overlap_blk[dir_start + 2..dir_start + 4].copy_from_slice(&2u16.to_le_bytes());
        let crc = crc32c(&overlap_blk[0..4092]);
        overlap_blk[4092..4096].copy_from_slice(&crc.to_le_bytes());
        assert_eq!(
            IdxBlockReader::open_verified(&overlap_blk)
                .err()
                .unwrap()
                .kind(),
            io::ErrorKind::InvalidData
        );

        let mut too_many_blk = base_blk;
        let bad_count = (MAX_IDX_BLOCK_ENTRIES + 1) as u16;
        too_many_blk[4088..4090].copy_from_slice(&bad_count.to_le_bytes());
        let crc = crc32c(&too_many_blk[0..4092]);
        too_many_blk[4092..4096].copy_from_slice(&crc.to_le_bytes());
        assert_eq!(
            IdxBlockReader::open_verified(&too_many_blk)
                .err()
                .unwrap()
                .kind(),
            io::ErrorKind::InvalidData
        );
        assert_eq!(
            IdxBlockReader::open_preverified(&too_many_blk)
                .err()
                .unwrap()
                .kind(),
            io::ErrorKind::InvalidData
        );

        // 2. `IdxSegmentBuilder::add_raw_entry` rejects invalid BlobKind BEFORE flushing.
        let tmp = TempTestDir::new("idx_raw_blobkind_and_empty");
        let fs = OsFs::open(&tmp.path, true).unwrap();
        let mut sb = IdxSegmentBuilder::create(&fs, 10, 0, 10).unwrap();
        sb.add_raw_entry(b"a", 1, 1, b"x").unwrap();
        let mut bad_ptr = [0u8; BLOB_PTR_ENCODED_LEN];
        bad_ptr[0] = 77;
        assert_eq!(
            sb.add_raw_entry(b"b", 2, 2, &bad_ptr).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        assert_eq!(sb.num_blocks, 0);
        let valid_seg = sb.finish().unwrap();
        assert_eq!(valid_seg.record_count(), 1);
        drop(valid_seg);

        // 3. `IdxSegmentReader::open` rejects num_blocks==0 with non-zero record_count or max_key.
        let write_crafted_empty_seg = |seq_id: u64, record_count: u32, max_key: &[u8]| {
            let mut meta_buf = Vec::new();
            BloomFilter::new(0, 0).encode(&mut meta_buf);
            meta_buf.extend_from_slice(&0u32.to_le_bytes());
            meta_buf.extend_from_slice(&(max_key.len() as u16).to_le_bytes());
            meta_buf.extend_from_slice(max_key);

            let mut footer = [0u8; IDX_FOOTER_SIZE];
            footer[0..4].copy_from_slice(IDX_FOOTER_MAGIC);
            footer[4..6].copy_from_slice(&IDX_FORMAT_VERSION.to_le_bytes());
            footer[8..16].copy_from_slice(&seq_id.to_le_bytes());
            footer[16..20].copy_from_slice(&0u32.to_le_bytes()); // num_blocks == 0
            footer[20..24].copy_from_slice(&record_count.to_le_bytes());
            footer[24..32].copy_from_slice(&0u64.to_le_bytes()); // meta_offset == 0
            footer[32..36].copy_from_slice(&(meta_buf.len() as u32).to_le_bytes());
            footer[36..40].copy_from_slice(&crc32c(&meta_buf).to_le_bytes());
            let fcrc = crc32c(&footer[0..44]);
            footer[44..48].copy_from_slice(&fcrc.to_le_bytes());

            let mut bytes = Vec::with_capacity(meta_buf.len() + IDX_FOOTER_SIZE);
            bytes.extend_from_slice(&meta_buf);
            bytes.extend_from_slice(&footer);
            let f = fs
                .open_file(
                    &format_idx_file_name(seq_id),
                    true,
                    true,
                    OpenHint::ImmutableRandom,
                )
                .unwrap();
            f.write_all_at(&bytes, 0).unwrap();
        };

        write_crafted_empty_seg(11, 5, b"");
        assert_eq!(
            IdxSegmentReader::open(&fs, 11).err().unwrap().kind(),
            io::ErrorKind::InvalidData
        );

        write_crafted_empty_seg(12, 0, b"nonempty");
        assert_eq!(
            IdxSegmentReader::open(&fs, 12).err().unwrap().kind(),
            io::ErrorKind::InvalidData
        );

        // 4. `IdxBlockReader::open_verified` rejects unsorted and duplicate keys even with valid CRC.
        let craft_two_key_block = |k_first: &[u8], k_second: &[u8]| -> [u8; IDX_BLOCK_SIZE] {
            let mut blk = [0u8; IDX_BLOCK_SIZE];
            let mut write_entry = |pos: usize, key: &[u8], lsn: u64| -> usize {
                blk[pos..pos + 2].copy_from_slice(&(key.len() as u16).to_le_bytes());
                blk[pos + 2] = 1; // Inline
                blk[pos + 3..pos + 11].copy_from_slice(&lsn.to_le_bytes());
                let ks = pos + 11;
                blk[ks..ks + key.len()].copy_from_slice(key);
                let vs = ks + key.len();
                blk[vs..vs + 2].copy_from_slice(&1u16.to_le_bytes());
                blk[vs + 2] = b'v';
                vs + 3
            };
            let off0 = 0usize;
            let off1 = write_entry(off0, k_first, 1);
            let _end = write_entry(off1, k_second, 2);
            let dir_start = IDX_BLOCK_USABLE_SIZE - 4;
            blk[dir_start..dir_start + 2].copy_from_slice(&(off0 as u16).to_le_bytes());
            blk[dir_start + 2..dir_start + 4].copy_from_slice(&(off1 as u16).to_le_bytes());
            blk[4088..4090].copy_from_slice(&2u16.to_le_bytes());
            let crc = crc32c(&blk[0..4092]);
            blk[4092..4096].copy_from_slice(&crc.to_le_bytes());
            blk
        };

        let descending_blk = craft_two_key_block(b"k2", b"k1");
        assert_eq!(
            IdxBlockReader::open_verified(&descending_blk)
                .err()
                .unwrap()
                .kind(),
            io::ErrorKind::InvalidData
        );
        let duplicate_blk = craft_two_key_block(b"k1", b"k1");
        assert_eq!(
            IdxBlockReader::open_verified(&duplicate_blk)
                .err()
                .unwrap()
                .kind(),
            io::ErrorKind::InvalidData
        );

        // 5. `IdxSegmentBuilder::finish` with bounded bloom & multi-block segment completes with exact footer reservation.
        let mut sb2 = IdxSegmentBuilder::create(&fs, 13, 0, 10).unwrap();
        for i in 0..200u32 {
            let k = format!("key_{i:04}");
            let v = vec![0x33u8; 64];
            sb2.add_entry(&IndexEntry {
                key: k.into_bytes(),
                lsn: i as u64 + 1,
                value: IndexValue::Inline(v),
            })
            .unwrap();
        }
        let multi = sb2.finish().unwrap();
        assert_eq!(multi.record_count(), 200);
        assert!(multi.num_blocks() > 1);
    }
}
