//! Kirsch-Mitzenmacher Double-Hashing Bloom Filter built on hardware-accelerated CRC32C.
//!
//! Keeps a compact bitset in heap memory (~10 bits per key = 1.25 bytes per key, ~0.9%
//! false-positive rate) so point lookups (`get`) can skip immutable `idx_<SeqID>.db` segments
//! without issuing a single `pread` system call.

use std::io;

use crate::util::crc32c::crc32c_append;

const SEED_H1: u32 = 0x1357_9BDF;
const SEED_H2: u32 = 0x2468_ACE0;

/// Compact in-memory Bloom filter for an `idx_<SeqID>.db` segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BloomFilter {
    bits: Vec<u8>,
    num_bits: u32,
    num_probes: u8,
}

impl BloomFilter {
    /// Creates an empty filter disabled (`bits_per_key == 0`) or sized for `num_keys`.
    pub fn new(num_keys: usize, bits_per_key: usize) -> Self {
        if bits_per_key == 0 || num_keys == 0 {
            return Self {
                bits: Vec::new(),
                num_bits: 0,
                num_probes: 0,
            };
        }

        // Optimal number of probes k = ln(2) * bits_per_key (~0.69 * bits_per_key)
        let k = ((bits_per_key as f64 * 0.69) as u8).clamp(1, 16);
        let raw_bits = (num_keys.saturating_mul(bits_per_key)).max(64);
        let num_bytes = raw_bits.div_ceil(8);
        let num_bits = (num_bytes * 8) as u32;

        Self {
            bits: vec![0u8; num_bytes],
            num_bits,
            num_probes: k,
        }
    }

    /// Computes the `(h1, h2)` double-hashing probe pair for `key`.
    #[inline]
    pub fn hash_pair(key: &[u8]) -> (u32, u32) {
        let h1 = crc32c_append(SEED_H1, key);
        let h2 = crc32c_append(SEED_H2, key).rotate_left(17) | 1;
        (h1, h2)
    }

    /// Computes a packed 64-bit hash `((h1 as u64) << 32) | (h2 as u64)` for `key`.
    #[inline]
    pub fn hash64(key: &[u8]) -> u64 {
        let (h1, h2) = Self::hash_pair(key);
        ((h1 as u64) << 32) | (h2 as u64)
    }

    /// Builds a [`BloomFilter`] from packed 64-bit hashes without retaining raw keys.
    pub fn from_hashes64(hashes: &[u64], bits_per_key: usize) -> Self {
        if bits_per_key == 0 || hashes.is_empty() {
            return Self::new(0, 0);
        }
        let mut filter = Self::new(hashes.len(), bits_per_key);
        for &packed in hashes {
            let h1 = (packed >> 32) as u32;
            let h2 = packed as u32;
            let mut h = h1;
            for _ in 0..filter.num_probes {
                let bit_idx = (h % filter.num_bits) as usize;
                filter.bits[bit_idx >> 3] |= 1u8 << (bit_idx & 7);
                h = h.wrapping_add(h2);
            }
        }
        filter
    }

    /// Inserts `key` into the Bloom filter.
    #[inline]
    pub fn insert(&mut self, key: &[u8]) {
        if self.num_bits == 0 {
            return;
        }
        let (h1, h2) = Self::hash_pair(key);

        let mut h = h1;
        for _ in 0..self.num_probes {
            let bit_idx = (h % self.num_bits) as usize;
            self.bits[bit_idx >> 3] |= 1u8 << (bit_idx & 7);
            h = h.wrapping_add(h2);
        }
    }

    /// Returns `true` if `key` may be present in the segment, or `false` if `key` is
    /// definitely not present.
    #[inline]
    pub fn may_contain(&self, key: &[u8]) -> bool {
        if self.num_bits == 0 {
            // When the Bloom filter is disabled (0 bits/key), conservatively return true.
            return true;
        }
        let (h1, h2) = Self::hash_pair(key);

        let mut h = h1;
        for _ in 0..self.num_probes {
            let bit_idx = (h % self.num_bits) as usize;
            if (self.bits[bit_idx >> 3] & (1u8 << (bit_idx & 7))) == 0 {
                return false;
            }
            h = h.wrapping_add(h2);
        }
        true
    }

    /// Returns the heap memory consumed by this Bloom filter in bytes.
    pub fn heap_bytes(&self) -> usize {
        self.bits.len()
    }

    /// Serializes the filter into `[num_probes: u8 | reserved: [u8; 3] | bit_bytes_len: u32 LE | bits...]`.
    pub fn encode(&self, out: &mut Vec<u8>) {
        out.push(self.num_probes);
        out.extend_from_slice(&[0u8; 3]);
        out.extend_from_slice(&(self.bits.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.bits);
    }

    /// Deserializes a [`BloomFilter`] from the beginning of `input`, advancing the slice.
    pub fn decode(input: &mut &[u8]) -> io::Result<Self> {
        if input.len() < 8 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "truncated BloomFilter header",
            ));
        }
        let num_probes = input[0];
        let byte_len = u32::from_le_bytes([input[4], input[5], input[6], input[7]]) as usize;
        *input = &input[8..];

        if input.len() < byte_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "truncated BloomFilter bitset",
            ));
        }
        let num_bits = byte_len
            .checked_mul(8)
            .and_then(|b| u32::try_from(b).ok())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "BloomFilter bitset length overflows u32 bits",
                )
            })?;
        let mut bits = Vec::new();
        bits.try_reserve_exact(byte_len).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "failed to allocate BloomFilter bitset",
            )
        })?;
        bits.extend_from_slice(&input[..byte_len]);
        *input = &input[byte_len..];

        Ok(Self {
            bits,
            num_bits,
            num_probes,
        })
    }
}
