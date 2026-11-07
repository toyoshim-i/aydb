//! Layer 3 (Part 2): Immutable 4 KB Block SSTable Index Segments (`idx_<SeqID>.db`),
//! Sparse Fence Pointers, In-Heap Bloom Filters, and Multi-Way Merge Compaction.
//!
//! See `docs/phase1/04_WISCKEY_AND_SEGMENTS.md` for the detailed binary format.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;

pub mod bloom;
pub use bloom::BloomFilter;

use crate::meta::format_idx_file_name;
use crate::pal::{OpenHint, PlatformFile, PlatformFs};
use crate::util::crc32c::crc32c;
use crate::wal::{BlobKind, BlobPtr};

/// Size of an index data block (4096 bytes).
pub const IDX_BLOCK_SIZE: usize = 4096;

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

/// Maximum defensive size for an index segment's filter/fence metadata trailer (64 MB, `S01`).
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
    /// 4 KB index block (`11 + key_len + 2 + val_len <= MAX_SINGLE_BLOCK_ENTRY_BYTES`, `R-03`).
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
    /// block (`11 + key_len + 23 <= MAX_SINGLE_BLOCK_ENTRY_BYTES`, `R-03`).
    #[inline]
    pub const fn blob_ptr_fits_in_block(key_len: usize) -> bool {
        match key_len.checked_add(11 + BLOB_PTR_ENCODED_LEN) {
            Some(total) => total <= MAX_SINGLE_BLOCK_ENTRY_BYTES,
            None => false,
        }
    }

    /// Returns `true` if an `IndexValue::Tombstone` entry with `key_len` fits in a single 4 KB
    /// index block (`11 + key_len <= MAX_SINGLE_BLOCK_ENTRY_BYTES`, `R-03`).
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

    /// Decodes the full [`IndexEntry`] at `index`.
    pub fn decode_entry_at(&self, index: usize) -> io::Result<IndexEntry> {
        let off = self.entry_offset(index);
        if off + 11 > self.dir_start {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "corrupted entry offset in 4 KB index block",
            ));
        }
        let klen = u16::from_le_bytes([self.buf[off], self.buf[off + 1]]) as usize;
        let type_tag = self.buf[off + 2];
        let lsn = u64::from_le_bytes(self.buf[off + 3..off + 11].try_into().unwrap());
        let mut pos = off + 11;

        if pos + klen > self.dir_start {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "corrupted key slice in 4 KB index block",
            ));
        }
        let key = self.buf[pos..pos + klen].to_vec();
        pos += klen;

        let value = match type_tag {
            1 => {
                if pos + 2 > self.dir_start {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "truncated inline value length",
                    ));
                }
                let vlen = u16::from_le_bytes([self.buf[pos], self.buf[pos + 1]]) as usize;
                pos += 2;
                if pos + vlen > self.dir_start {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "truncated inline value payload",
                    ));
                }
                IndexValue::Inline(self.buf[pos..pos + vlen].to_vec())
            }
            2 => {
                if pos + 23 > self.dir_start {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "truncated BlobPtr in 4 KB index block",
                    ));
                }
                let kind = BlobKind::from_u8(self.buf[pos])?;
                let table_id = u16::from_le_bytes([self.buf[pos + 1], self.buf[pos + 2]]);
                let seg_id = u64::from_le_bytes(self.buf[pos + 3..pos + 11].try_into().unwrap());
                let offset = u32::from_le_bytes(self.buf[pos + 11..pos + 15].try_into().unwrap());
                let len = u32::from_le_bytes(self.buf[pos + 15..pos + 19].try_into().unwrap());
                let crc = u32::from_le_bytes(self.buf[pos + 19..pos + 23].try_into().unwrap());
                IndexValue::Blob(BlobPtr {
                    kind,
                    table_id,
                    seg_id,
                    offset,
                    len,
                    crc32c: crc,
                })
            }
            3 => IndexValue::Tombstone,
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unknown IndexValue type_tag: {other}"),
                ));
            }
        };

        Ok(IndexEntry { key, lsn, value })
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

impl BloomFilter {
    /// Computes the `(h1, h2)` double-hashing probe pair for `key`.
    #[inline]
    pub fn hash_pair(key: &[u8]) -> (u32, u32) {
        const SEED_H1: u32 = 0x1357_9BDF;
        const SEED_H2: u32 = 0x2468_ACE0;
        let h1 = crate::util::crc32c::crc32c_append(SEED_H1, key);
        let h2 = crate::util::crc32c::crc32c_append(SEED_H2, key).rotate_left(17) | 1;
        (h1, h2)
    }

    /// Computes a packed 64-bit hash `((h1 as u64) << 32) | (h2 as u64)` for `key` (`P02`).
    #[inline]
    pub fn hash64(key: &[u8]) -> u64 {
        let (h1, h2) = Self::hash_pair(key);
        ((h1 as u64) << 32) | (h2 as u64)
    }

    /// Builds a [`BloomFilter`] from precomputed `(h1, h2)` hash pairs without retaining raw keys.
    pub fn from_hash_pairs(hashes: &[(u32, u32)], bits_per_key: usize) -> Self {
        if bits_per_key == 0 || hashes.is_empty() {
            return Self::new(0, 0);
        }
        let num_probes = ((bits_per_key as f64 * 0.69) as u8).clamp(1, 16);
        let raw_bits = (hashes.len().saturating_mul(bits_per_key)).max(64);
        let num_bytes = raw_bits.div_ceil(8);
        let num_bits = (num_bytes * 8) as u32;

        let mut buf = vec![0u8; 8 + num_bytes];
        buf[0] = num_probes;
        buf[4..8].copy_from_slice(&(num_bytes as u32).to_le_bytes());
        let bits = &mut buf[8..];
        for &(h1, h2) in hashes {
            let mut h = h1;
            for _ in 0..num_probes {
                let bit_idx = (h % num_bits) as usize;
                bits[bit_idx >> 3] |= 1u8 << (bit_idx & 7);
                h = h.wrapping_add(h2);
            }
        }
        let mut slice: &[u8] = &buf;
        Self::decode(&mut slice).expect("valid encoded BloomFilter")
    }

    /// Builds a [`BloomFilter`] from packed 64-bit hashes without retaining raw keys (`P02`).
    pub fn from_hashes64(hashes: &[u64], bits_per_key: usize) -> Self {
        if bits_per_key == 0 || hashes.is_empty() {
            return Self::new(0, 0);
        }
        let num_probes = ((bits_per_key as f64 * 0.69) as u8).clamp(1, 16);
        let raw_bits = (hashes.len().saturating_mul(bits_per_key)).max(64);
        let num_bytes = raw_bits.div_ceil(8);
        let num_bits = (num_bytes * 8) as u32;

        let mut buf = vec![0u8; 8 + num_bytes];
        buf[0] = num_probes;
        buf[4..8].copy_from_slice(&(num_bytes as u32).to_le_bytes());
        let bits = &mut buf[8..];
        for &packed in hashes {
            let h1 = (packed >> 32) as u32;
            let h2 = packed as u32;
            let mut h = h1;
            for _ in 0..num_probes {
                let bit_idx = (h % num_bits) as usize;
                bits[bit_idx >> 3] |= 1u8 << (bit_idx & 7);
                h = h.wrapping_add(h2);
            }
        }
        let mut slice: &[u8] = &buf;
        Self::decode(&mut slice).expect("valid encoded BloomFilter")
    }
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

        self.file.write_all_at(&block_bytes, self.write_offset)?;
        self.write_offset += IDX_BLOCK_SIZE as u64;
        self.num_blocks += 1;
        self.fence_pointers.push(FencePointer {
            first_key,
            block_idx,
        });
        Ok(())
    }

    /// Finalizes the segment by flushing the last 4 KB data block, writing the Bloom filter
    /// and Sparse Fence Pointer section, writing the 48-byte footer, and syncing to disk.
    pub fn finish(mut self) -> io::Result<IdxSegmentReader<F>> {
        self.flush_current_data_block()?;

        // Build BloomFilter from precomputed 64-bit hashes and drop the hash vector immediately (`P02`)
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
        // and the remaining bytes in `slice` (`>= 6` bytes per fence pointer) BEFORE allocating (`S01`).
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

    /// Returns `true` if `key` lies within `[self.min_key, self.max_key]` (`PERF-02`).
    #[inline]
    pub fn may_contain_key(&self, key: &[u8]) -> bool {
        !self.fence_pointers.is_empty()
            && key >= self.min_key.as_slice()
            && key <= self.max_key.as_slice()
    }

    /// Returns `true` if `[self.min_key, self.max_key]` overlaps `[lower, upper]` (`PERF-02`).
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

    /// Point lookup for `key`.
    /// 1. Checks in-heap `[min_key, max_key]` key-range bounds and [`BloomFilter`] in ~10 ns.
    /// 2. Binary-searches in-heap [`FencePointer`] table in $O(\log B)$.
    /// 3. Reads a single 4 KB data block from the OS Page Cache and binary-searches its
    ///    `u16` offset directory in $O(\log K)$.
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
        let mut block_buf = [0u8; IDX_BLOCK_SIZE];
        self.file.read_exact_at(
            &mut block_buf,
            (block_idx as u64) * (IDX_BLOCK_SIZE as u64),
        )?;

        let reader = IdxBlockReader::open_verified(&block_buf)?;
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
            self.file
                .read_exact_at(&mut block_buf, (b as u64) * (IDX_BLOCK_SIZE as u64))?;
            let reader = IdxBlockReader::open_verified(&block_buf)?;
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
            self.file
                .read_exact_at(&mut block_buf, (b as u64) * (IDX_BLOCK_SIZE as u64))?;
            let reader = IdxBlockReader::open_verified(&block_buf)?;
            for i in 0..reader.num_entries() {
                let entry = reader.decode_entry_at(i)?;
                if entry.key.as_slice() < effective_start {
                    continue;
                }
                if !entry.key.starts_with(prefix) {
                    return Ok(out);
                }
                if let Some((ub, inclusive)) = upper_key_bound {
                    if inclusive {
                        if entry.key.as_slice() > ub {
                            return Ok(out);
                        }
                    } else if entry.key.as_slice() >= ub {
                        return Ok(out);
                    }
                }
                out.push(entry);
                if out.len() >= limit {
                    return Ok(out);
                }
            }
        }
        Ok(out)
    }

    /// Creates a bounded-memory streaming cursor over all entries in this segment, reading
    /// one 4 KB data block at a time on demand.
    pub fn cursor_all(&self) -> io::Result<IdxSegmentCursor<'_, F>> {
        IdxSegmentCursor::new_all(self)
    }

    /// Creates a bounded-memory streaming cursor starting at `(lower_key, lower_inclusive)`,
    /// seeking via sparse fence pointers and reading one 4 KB data block at a time on demand.
    pub fn cursor_from(
        &self,
        lower_key: &[u8],
        lower_inclusive: bool,
    ) -> io::Result<IdxSegmentCursor<'_, F>> {
        IdxSegmentCursor::new_from(self, lower_key, lower_inclusive)
    }

    /// Creates a bounded-memory reverse streaming cursor starting at `(upper_key, upper_inclusive)`,
    /// seeking via sparse fence pointers and walking 4 KB data blocks backward on demand (`P-01`).
    pub fn cursor_reverse_from(
        &self,
        upper_key: Option<(&[u8], bool)>,
    ) -> io::Result<IdxSegmentRevCursor<'_, F>> {
        IdxSegmentRevCursor::new_from(self, upper_key)
    }
}

/// Bounded-memory reverse block-by-block cursor over an [`IdxSegmentReader`] (`P-01`).
pub struct IdxSegmentRevCursor<'a, F: PlatformFile> {
    seg: &'a IdxSegmentReader<F>,
    next_block_idx: Option<u32>,
    block_buf: [u8; IDX_BLOCK_SIZE],
    entry_idx: usize,
    num_entries_in_block: usize,
    current: Option<IndexEntry>,
}

impl<'a, F: PlatformFile> IdxSegmentRevCursor<'a, F> {
    fn new_from(
        seg: &'a IdxSegmentReader<F>,
        upper_key: Option<(&[u8], bool)>,
    ) -> io::Result<Self> {
        let mut cursor = Self {
            seg,
            next_block_idx: None,
            block_buf: [0u8; IDX_BLOCK_SIZE],
            entry_idx: 0,
            num_entries_in_block: 0,
            current: None,
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
            seg.file
                .read_exact_at(&mut cursor.block_buf, (b as u64) * (IDX_BLOCK_SIZE as u64))?;
            let reader = IdxBlockReader::open_verified(&cursor.block_buf)?;
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
                let entry = reader.decode_entry_at(target_idx)?;
                cursor.entry_idx = target_idx;
                cursor.next_block_idx = b.checked_sub(1);
                cursor.current = Some(entry);
                break;
            }

            if b == 0 {
                break;
            }
            b -= 1;
        }
        Ok(cursor)
    }

    #[inline]
    pub fn peek(&self) -> Option<&IndexEntry> {
        self.current.as_ref()
    }

    pub fn next_entry(&mut self) -> io::Result<Option<IndexEntry>> {
        let prev = self.current.take();
        if prev.is_some() {
            self.advance_to_prev()?;
        }
        Ok(prev)
    }

    fn advance_to_prev(&mut self) -> io::Result<()> {
        while self.entry_idx == 0 {
            let Some(b) = self.next_block_idx else {
                self.current = None;
                return Ok(());
            };
            self.next_block_idx = b.checked_sub(1);
            self.seg
                .file
                .read_exact_at(&mut self.block_buf, (b as u64) * (IDX_BLOCK_SIZE as u64))?;
            let reader = IdxBlockReader::open_verified(&self.block_buf)?;
            self.num_entries_in_block = reader.num_entries();
            self.entry_idx = self.num_entries_in_block;
        }

        self.entry_idx -= 1;
        let reader = IdxBlockReader::from_verified_buf(&self.block_buf, self.num_entries_in_block);
        let entry = reader.decode_entry_at(self.entry_idx)?;
        self.current = Some(entry);
        Ok(())
    }
}

/// Bounded-memory block-by-block cursor over an [`IdxSegmentReader`].
///
/// Holds only a single 4 KB data block buffer (`[u8; IDX_BLOCK_SIZE]`) in stack/struct memory,
/// enabling $O(K \times 4\text{ KB})$ streaming K-way merges for range scans and compaction.
pub struct IdxSegmentCursor<'a, F: PlatformFile> {
    seg: &'a IdxSegmentReader<F>,
    next_block_idx: u32,
    block_buf: [u8; IDX_BLOCK_SIZE],
    entry_idx: usize,
    num_entries_in_block: usize,
    current: Option<IndexEntry>,
}

impl<'a, F: PlatformFile> IdxSegmentCursor<'a, F> {
    fn new_all(seg: &'a IdxSegmentReader<F>) -> io::Result<Self> {
        let mut cursor = Self {
            seg,
            next_block_idx: 0,
            block_buf: [0u8; IDX_BLOCK_SIZE],
            entry_idx: 0,
            num_entries_in_block: 0,
            current: None,
        };
        cursor.advance_to_next()?;
        Ok(cursor)
    }

    fn new_from(
        seg: &'a IdxSegmentReader<F>,
        lower_key: &[u8],
        lower_inclusive: bool,
    ) -> io::Result<Self> {
        let mut cursor = Self {
            seg,
            next_block_idx: 0,
            block_buf: [0u8; IDX_BLOCK_SIZE],
            entry_idx: 0,
            num_entries_in_block: 0,
            current: None,
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
            seg.file
                .read_exact_at(&mut cursor.block_buf, (b as u64) * (IDX_BLOCK_SIZE as u64))?;
            let reader = IdxBlockReader::open_verified(&cursor.block_buf)?;
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
                let first_entry = reader.decode_entry_at(low)?;
                cursor.entry_idx = low + 1;
                cursor.current = Some(first_entry);
                break;
            } else {
                cursor.entry_idx = cursor.num_entries_in_block;
            }
        }
        Ok(cursor)
    }

    /// Returns a reference to the current [`IndexEntry`] without advancing the cursor.
    #[inline]
    pub fn peek(&self) -> Option<&IndexEntry> {
        self.current.as_ref()
    }

    /// Consumes and returns the current [`IndexEntry`], advancing the cursor to the next entry.
    pub fn next_entry(&mut self) -> io::Result<Option<IndexEntry>> {
        let prev = self.current.take();
        if prev.is_some() {
            self.advance_to_next()?;
        }
        Ok(prev)
    }

    fn advance_to_next(&mut self) -> io::Result<()> {
        while self.entry_idx >= self.num_entries_in_block {
            if self.next_block_idx >= self.seg.num_blocks {
                self.current = None;
                return Ok(());
            }
            let b = self.next_block_idx;
            self.next_block_idx += 1;
            self.seg
                .file
                .read_exact_at(&mut self.block_buf, (b as u64) * (IDX_BLOCK_SIZE as u64))?;
            let reader = IdxBlockReader::open_verified(&self.block_buf)?;
            self.num_entries_in_block = reader.num_entries();
            self.entry_idx = 0;
        }

        let reader = IdxBlockReader::from_verified_buf(&self.block_buf, self.num_entries_in_block);
        let entry = reader.decode_entry_at(self.entry_idx)?;
        self.entry_idx += 1;
        self.current = Some(entry);
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

    loop {
        let mut min_key: Option<&[u8]> = None;
        for cur in &cursors {
            if let Some(entry) = cur.peek() {
                match min_key {
                    None => min_key = Some(entry.key.as_slice()),
                    Some(mk) if entry.key.as_slice() < mk => {
                        min_key = Some(entry.key.as_slice());
                    }
                    _ => {}
                }
            }
        }

        let Some(target_key_slice) = min_key else {
            break;
        };
        let target_key = target_key_slice.to_vec();

        let mut winning_entry: Option<IndexEntry> = None;
        for cur in &mut cursors {
            while cur
                .peek()
                .map(|e| e.key.as_slice() == target_key.as_slice())
                .unwrap_or(false)
            {
                let entry = cur.next_entry()?.unwrap();
                match &mut winning_entry {
                    None => {
                        winning_entry = Some(entry);
                    }
                    Some(existing) => {
                        if entry.lsn > existing.lsn {
                            if let IndexValue::Blob(old_ptr) = &existing.value {
                                *dead_blob_map
                                    .entry((old_ptr.kind as u8, old_ptr.table_id, old_ptr.seg_id))
                                    .or_insert(0) += old_ptr.len as u64;
                            }
                            *existing = entry;
                        } else if let IndexValue::Blob(old_ptr) = &entry.value {
                            *dead_blob_map
                                .entry((old_ptr.kind as u8, old_ptr.table_id, old_ptr.seg_id))
                                .or_insert(0) += old_ptr.len as u64;
                        }
                    }
                }
            }
        }

        if let Some(entry) = winning_entry {
            if drop_tombstones && entry.value == IndexValue::Tombstone {
                continue;
            }
            builder.add_entry(&entry)?;
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
