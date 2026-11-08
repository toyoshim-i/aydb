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
    /// Returns the total byte cost of encoding this entry + its 2-byte `u16` offset slot
    /// inside a 4096-byte data block.
    #[inline]
    pub fn block_footprint_bytes(&self) -> usize {
        // 11B entry header (`key_len: u16 | type: u8 | lsn: u64`) + key + value_payload + 2B offset
        11 + self.key.len() + self.value.encoded_payload_len() + 2
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

    /// Returns `true` if `entry` fits in the remaining space of this 4 KB block.
    pub fn can_fit(&self, entry: &IndexEntry) -> bool {
        let needed = entry.block_footprint_bytes();
        let used = self.write_pos + (self.offsets.len() * 2);
        used + needed <= IDX_BLOCK_USABLE_SIZE
    }

    /// Returns `true` if a raw entry with `(key_len, val_type, val_len)` fits in this 4 KB block.
    #[inline]
    pub fn can_fit_raw(&self, key_len: usize, val_type: u8, val_len: usize) -> bool {
        let payload_len = match val_type {
            1 => 2 + val_len,
            2 => BLOB_PTR_ENCODED_LEN,
            _ => 0,
        };
        let needed = 11 + key_len + payload_len + 2;
        let used = self.write_pos + (self.offsets.len() * 2);
        used + needed <= IDX_BLOCK_USABLE_SIZE
    }

    /// Appends a raw borrowed entry (`key`, `lsn`, `val_type`, `val_bytes`) directly to this block.
    pub fn add_raw_entry(
        &mut self,
        key: &[u8],
        lsn: u64,
        val_type: u8,
        val_bytes: &[u8],
    ) -> io::Result<()> {
        if !self.can_fit_raw(key.len(), val_type, val_bytes.len()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "RawIdxEntry exceeds remaining 4 KB block capacity",
            ));
        }
        let klen = u16::try_from(key.len()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "key length exceeds u16::MAX")
        })?;

        let start = self.write_pos;
        self.offsets.push(start as u16);

        self.buf[start..start + 2].copy_from_slice(&klen.to_le_bytes());
        self.buf[start + 2] = val_type;
        self.buf[start + 3..start + 11].copy_from_slice(&lsn.to_le_bytes());
        let mut pos = start + 11;

        self.buf[pos..pos + key.len()].copy_from_slice(key);
        pos += key.len();

        match val_type {
            1 => {
                let vlen = u16::try_from(val_bytes.len()).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "inline value length exceeds u16::MAX",
                    )
                })?;
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
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unknown IndexValue type_tag: {other}"),
                ));
            }
        }

        self.write_pos = pos;
        Ok(())
    }

    /// Appends `entry` (which must be in strictly ascending key order) to this block.
    pub fn add_entry(&mut self, entry: &IndexEntry) -> io::Result<()> {
        if !self.can_fit(entry) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "IndexEntry exceeds remaining 4 KB block capacity",
            ));
        }
        let klen = u16::try_from(entry.key.len()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "key length exceeds u16::MAX")
        })?;

        let start = self.write_pos;
        self.offsets.push(start as u16);

        self.buf[start..start + 2].copy_from_slice(&klen.to_le_bytes());
        self.buf[start + 2] = entry.value.type_tag();
        self.buf[start + 3..start + 11].copy_from_slice(&entry.lsn.to_le_bytes());
        let mut pos = start + 11;

        self.buf[pos..pos + entry.key.len()].copy_from_slice(&entry.key);
        pos += entry.key.len();

        match &entry.value {
            IndexValue::Inline(val) => {
                let vlen = u16::try_from(val.len()).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "inline value length exceeds u16::MAX",
                    )
                })?;
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

    /// Seals the 4 KB block by writing the `u16` Entry Offset Directory (`4088 - 2*K .. 4088`),
    /// the entry count, and the `block_crc32c` trailer (`4092..4096`), then resets `self`.
    pub fn finish_and_reset(&mut self) -> [u8; IDX_BLOCK_SIZE] {
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

        let finished = self.buf;
        self.buf.fill(0);
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

/// Zero-copy reader for a single 4096-byte index data block.
pub struct IdxBlockReader<'a> {
    buf: &'a [u8; IDX_BLOCK_SIZE],
    num_entries: usize,
    dir_start: usize,
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
        if num_entries * 2 > IDX_BLOCK_USABLE_SIZE {
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

    /// Structural bounds verification for an already-verified 4 KB index data block (skips CRC32C recalculation).
    pub fn open_preverified(buf: &'a [u8; IDX_BLOCK_SIZE]) -> io::Result<Self> {
        let num_entries = u16::from_le_bytes([buf[4088], buf[4089]]) as usize;
        if num_entries * 2 > IDX_BLOCK_USABLE_SIZE {
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
    fn entry_offset(&self, index: usize) -> usize {
        let pos = self.dir_start + (index * 2);
        u16::from_le_bytes([self.buf[pos], self.buf[pos + 1]]) as usize
    }

    /// Returns the raw key slice of the entry at `index` without allocating or decoding the value.
    #[inline]
    fn key_at(&self, index: usize) -> io::Result<&'a [u8]> {
        let off = self.entry_offset(index);
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
        let off = self.entry_offset(index);
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
        let mut low = 0usize;
        let mut high = self.num_entries;
        while low < high {
            let mid = low + (high - low) / 2;
            let k = self.key_at(mid)?;
            match k.cmp(target_key) {
                Ordering::Less => low = mid + 1,
                Ordering::Greater => high = mid,
                Ordering::Equal => {
                    let entry = self.decode_entry_at(mid)?;
                    return Ok(Some((entry.lsn, entry.value)));
                }
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

    /// Adds a sorted [`IndexEntry`] to the segment, flushing a 4 KB data block whenever full.
    pub fn add_entry(&mut self, entry: &IndexEntry) -> io::Result<()> {
        if !self.block_builder.is_empty() && !self.block_builder.can_fit(entry) {
            self.flush_current_data_block()?;
        }
        self.block_builder.add_entry(entry)?;
        if self.bloom_bits_per_key > 0 {
            self.bloom_hashes.push(BloomFilter::hash64(&entry.key));
        }
        self.last_key.clear();
        self.last_key.extend_from_slice(&entry.key);
        self.record_count += 1;
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
        self.record_count += 1;
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
        let first_key = self
            .block_builder
            .first_key()
            .expect("non-empty block must have first_key")
            .to_vec()
            .into_boxed_slice();
        let block_idx = self.num_blocks;
        let block_bytes = self.block_builder.finish_and_reset();

        if self.write_buf.is_empty() {
            self.buf_start_offset = self.write_offset;
        }
        self.write_buf.extend_from_slice(&block_bytes);
        self.write_offset += IDX_BLOCK_SIZE as u64;
        self.num_blocks += 1;
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

        // Build BloomFilter from precomputed 64-bit hashes and drop the hash vector immediately
        let bloom_hashes = core::mem::take(&mut self.bloom_hashes);
        let bloom = BloomFilter::from_hashes64(&bloom_hashes, self.bloom_bits_per_key);
        drop(bloom_hashes);

        // Encode Filter + Sparse Fence Pointers + max_key metadata section
        let meta_offset = self.write_offset;
        let mut meta_buf = Vec::with_capacity(256);
        bloom.encode(&mut meta_buf);
        meta_buf.extend_from_slice(&(self.fence_pointers.len() as u32).to_le_bytes());
        for fp in &self.fence_pointers {
            meta_buf.extend_from_slice(&(fp.first_key.len() as u16).to_le_bytes());
            meta_buf.extend_from_slice(&fp.block_idx.to_le_bytes());
            meta_buf.extend_from_slice(&fp.first_key);
        }
        meta_buf.extend_from_slice(&(self.last_key.len() as u16).to_le_bytes());
        meta_buf.extend_from_slice(&self.last_key);
        let meta_len = meta_buf.len() as u32;
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

        // Validate `fp_count` against the number of 4 KB data blocks (`<= 1` fence pointer per block)
        // and the remaining bytes in `slice` (`>= 6` bytes per fence pointer) BEFORE allocating.
        let max_possible_blocks = (meta_offset / (IDX_BLOCK_SIZE as u64)) as usize;
        let min_fp_bytes = fp_count.checked_mul(6).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "fence pointer count byte size overflow",
            )
        })?;
        if fp_count > max_possible_blocks || min_fp_bytes > slice.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "corrupted or out-of-bounds fence pointer count in idx segment",
            ));
        }

        let mut fence_pointers = Vec::new();
        fence_pointers.try_reserve_exact(fp_count).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "failed to reserve fence pointers",
            )
        })?;
        for _ in 0..fp_count {
            if slice.len() < 6 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "truncated fence pointer entry",
                ));
            }
            let klen = u16::from_le_bytes([slice[0], slice[1]]) as usize;
            let block_idx = u32::from_le_bytes([slice[2], slice[3], slice[4], slice[5]]);
            if block_idx >= num_blocks {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "fence pointer block_idx out of bounds",
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
            fence_pointers.push(FencePointer {
                first_key,
                block_idx,
            });
        }

        let min_key = fence_pointers
            .first()
            .map(|fp| fp.first_key.to_vec())
            .unwrap_or_default();
        let max_key = if slice.len() >= 2 {
            let max_klen = u16::from_le_bytes([slice[0], slice[1]]) as usize;
            slice = &slice[2..];
            if slice.len() < max_klen {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "truncated max_key in idx segment metadata",
                ));
            }
            slice[..max_klen].to_vec()
        } else if num_blocks > 0 && !fence_pointers.is_empty() {
            let last_block_idx = num_blocks - 1;
            let mut block_buf = [0u8; IDX_BLOCK_SIZE];
            file.read_exact_at(
                &mut block_buf,
                (last_block_idx as u64) * (IDX_BLOCK_SIZE as u64),
            )?;
            if let Ok(reader) = IdxBlockReader::open_verified(&block_buf) {
                if reader.num_entries() > 0 {
                    reader
                        .key_at(reader.num_entries() - 1)
                        .map(|k| k.to_vec())
                        .unwrap_or_else(|_| fence_pointers.last().unwrap().first_key.to_vec())
                } else {
                    min_key.clone()
                }
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
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
        self.file.read_exact_at(
            block_buf,
            (block_idx as u64) * (IDX_BLOCK_SIZE as u64),
        )?;
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
        if !self.may_contain_key(key) || !self.bloom.may_contain(key) {
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
                return IdxBlockReader::open_preverified(&block)?.lookup_binary_search(key);
            }
        }
        let mut block_buf = [0u8; IDX_BLOCK_SIZE];
        let reader = self.read_block_from_disk(block_idx, &mut block_buf, true)?;
        reader.lookup_binary_search(key)
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
            .partition_point(|fp| fp.first_key.as_ref() < effective_start);
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
    /// one 4 KB data block at a time on demand (bypasses cache insertion for bulk compaction).
    pub fn cursor_all(&self) -> io::Result<IdxSegmentCursor<'_, F>> {
        IdxSegmentCursor::new_all(self, false)
    }

    /// Creates a bounded-memory streaming cursor over all entries in this segment with explicit cache policy.
    pub fn cursor_all_with_cache(&self, populate_cache: bool) -> io::Result<IdxSegmentCursor<'_, F>> {
        IdxSegmentCursor::new_all(self, populate_cache)
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

/// Bounded-memory reverse block-by-block cursor over an [`IdxSegmentReader`].
pub struct IdxSegmentRevCursor<'a, F: PlatformFile> {
    seg: &'a IdxSegmentReader<F>,
    next_block_idx: Option<u32>,
    block_buf: [u8; IDX_BLOCK_SIZE],
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
            block_buf: [0u8; IDX_BLOCK_SIZE],
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
            let reader = seg.read_block(b, &mut cursor.block_buf, cursor.populate_cache)?;
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
        self.current_span
            .as_ref()
            .map(|s| &self.block_buf[s.key_range.clone()])
    }

    /// Returns a zero-copy [`RawIdxEntry`] view of the current entry borrowed from `self.block_buf`.
    #[inline]
    pub fn peek_raw(&self) -> Option<RawIdxEntry<'_>> {
        self.current_span.as_ref().map(|s| RawIdxEntry {
            key: &self.block_buf[s.key_range.clone()],
            lsn: s.lsn,
            val_type: s.val_type,
            val_bytes: &self.block_buf[s.val_range.clone()],
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
        while self.entry_idx == 0 {
            let Some(b) = self.next_block_idx else {
                self.current_span = None;
                return Ok(());
            };
            self.next_block_idx = b.checked_sub(1);
            let reader = self.seg.read_block(b, &mut self.block_buf, self.populate_cache)?;
            self.num_entries_in_block = reader.num_entries();
            self.entry_idx = self.num_entries_in_block;
        }

        self.entry_idx -= 1;
        let reader = IdxBlockReader::from_verified_buf(&self.block_buf, self.num_entries_in_block);
        let span = reader.parse_entry_span_at(self.entry_idx)?;
        self.current_span = Some(span);
        Ok(())
    }
}

/// Bounded-memory block-by-block cursor over an [`IdxSegmentReader`].
///
/// Holds only a single 4 KB data block buffer (`[u8; IDX_BLOCK_SIZE]`) in stack/struct memory,
/// enabling zero-allocation key/LSN inspection and $O(K \times 4\text{ KB})$ streaming K-way merges.
pub struct IdxSegmentCursor<'a, F: PlatformFile> {
    seg: &'a IdxSegmentReader<F>,
    next_block_idx: u32,
    block_buf: [u8; IDX_BLOCK_SIZE],
    entry_idx: usize,
    num_entries_in_block: usize,
    current_span: Option<CursorEntrySpan>,
    populate_cache: bool,
}

impl<'a, F: PlatformFile> IdxSegmentCursor<'a, F> {
    fn new_all(seg: &'a IdxSegmentReader<F>, populate_cache: bool) -> io::Result<Self> {
        let mut cursor = Self {
            seg,
            next_block_idx: 0,
            block_buf: [0u8; IDX_BLOCK_SIZE],
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
            block_buf: [0u8; IDX_BLOCK_SIZE],
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
            .partition_point(|fp| fp.first_key.as_ref() < lower_key);
        cursor.next_block_idx = start_pos.saturating_sub(1) as u32;

        while cursor.next_block_idx < seg.num_blocks {
            let b = cursor.next_block_idx;
            cursor.next_block_idx += 1;
            let reader = seg.read_block(b, &mut cursor.block_buf, cursor.populate_cache)?;
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
        self.current_span
            .as_ref()
            .map(|s| &self.block_buf[s.key_range.clone()])
    }

    /// Returns a zero-copy [`RawIdxEntry`] view of the current entry borrowed from `self.block_buf`.
    #[inline]
    pub fn peek_raw(&self) -> Option<RawIdxEntry<'_>> {
        self.current_span.as_ref().map(|s| RawIdxEntry {
            key: &self.block_buf[s.key_range.clone()],
            lsn: s.lsn,
            val_type: s.val_type,
            val_bytes: &self.block_buf[s.val_range.clone()],
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
        while self.entry_idx >= self.num_entries_in_block {
            if self.next_block_idx >= self.seg.num_blocks {
                self.current_span = None;
                return Ok(());
            }
            let b = self.next_block_idx;
            self.next_block_idx += 1;
            let reader = self.seg.read_block(b, &mut self.block_buf, self.populate_cache)?;
            self.num_entries_in_block = reader.num_entries();
            self.entry_idx = 0;
        }

        let reader = IdxBlockReader::from_verified_buf(&self.block_buf, self.num_entries_in_block);
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
        cursors.push(seg.cursor_all()?);
    }

    let mut builder = IdxSegmentBuilder::create(fs, new_seq_id, target_level, bloom_bits_per_key)?;
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
                                let sid = u64::from_le_bytes(
                                    winning_val_buf[3..11].try_into().unwrap(),
                                );
                                let blen = u32::from_le_bytes(
                                    winning_val_buf[15..19].try_into().unwrap(),
                                );
                                *dead_blob_map.entry((k, tid, sid)).or_insert(0) += blen as u64;
                            }
                            *win_lsn = raw.lsn;
                            *win_vtype = raw.val_type;
                            winning_val_buf.clear();
                            winning_val_buf.extend_from_slice(raw.val_bytes);
                        } else if raw.val_type == 2 && raw.val_bytes.len() == BLOB_PTR_ENCODED_LEN {
                            let k = raw.val_bytes[0];
                            let tid = u16::from_le_bytes([raw.val_bytes[1], raw.val_bytes[2]]);
                            let sid =
                                u64::from_le_bytes(raw.val_bytes[3..11].try_into().unwrap());
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
            .open_file(&seg_fname, false, true, crate::pal::OpenHint::ImmutableRandom)
            .unwrap();
        let flen = seg_file.file_len().unwrap() as usize;
        let mut raw_seg = vec![0u8; flen];
        seg_file.read_exact_at(&mut raw_seg, 0).unwrap();

        let footer_start = flen - 48;
        let meta_offset =
            u64::from_le_bytes(raw_seg[footer_start + 24..footer_start + 32].try_into().unwrap())
                as usize;
        // `meta_buf` starts with `[num_probes: u8 | reserved: [u8; 3] | bit_bytes_len: u32 LE | bits...]`,
        // followed by `fp_count: u32`.
        let bloom_byte_len =
            u32::from_le_bytes(raw_seg[meta_offset + 4..meta_offset + 8].try_into().unwrap())
                as usize;
        let fp_count_pos = meta_offset + 8 + bloom_byte_len;
        // Overwrite `fp_count` with `u32::MAX` and recompute both `meta_crc` and `footer_crc32c`
        raw_seg[fp_count_pos..fp_count_pos + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        let new_meta_crc = crate::util::crc32c::crc32c(&raw_seg[meta_offset..footer_start]);
        raw_seg[footer_start + 36..footer_start + 40].copy_from_slice(&new_meta_crc.to_le_bytes());
        let new_footer_crc =
            crate::util::crc32c::crc32c(&raw_seg[footer_start..footer_start + 44]);
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
        let expect = |i: u32| Some(((i + 1) as u64, IndexValue::Inline(format!("val_{i}").into_bytes())));
        assert_eq!(seg.get(b"key_00007").unwrap(), expect(7));
        assert_eq!((cache.hits(), cache.misses()), (0, 1));
        assert_eq!(seg.get(b"key_00007").unwrap(), expect(7));
        assert_eq!((cache.hits(), cache.misses()), (1, 1));
        for i in 0..200u32 {
            assert_eq!(seg.get(format!("key_{i:05}").as_bytes()).unwrap(), expect(i));
        }
        assert_eq!(cache.hits() + cache.misses(), 202);
        assert!(cache.hits() > 190);
        assert_eq!(seg.get(b"key_99999").unwrap(), None);
    }
}
