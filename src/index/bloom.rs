//! Kirsch-Mitzenmacher Double-Hashing Bloom Filter built on a single-pass non-linear 64-bit hash.
//!
//! Keeps a compact bitset in heap memory (~10 bits per key = 1.25 bytes per key, ~0.9%
//! false-positive rate) so point lookups (`get`) can skip immutable `idx_<SeqID>.db` segments
//! without issuing a single `pread` system call.
//!
//! Uses a single-pass wyhash-style 64-bit mixer (`hash64`) and splits its upper and lower 32-bit
//! halves into `(h1, h2)` so double-hashing probes have independent entropy across equal-length
//! keys (avoiding GF(2)-affine correlation between dual-seed CRC32C hashes).

use std::io;

const SECRET0: u64 = 0xa076_1d64_78bd_642f;
const SECRET1: u64 = 0xe703_7ed1_a0b4_28db;
const SECRET2: u64 = 0x8ebc_6af0_9c88_c6e3;
const SECRET3: u64 = 0x5899_65cc_7537_4cc3;

#[inline]
fn wymix(a: u64, b: u64) -> u64 {
    let r = (a as u128).wrapping_mul(b as u128);
    (r as u64) ^ ((r >> 64) as u64)
}

#[inline]
fn read_u64_le(bytes: &[u8]) -> u64 {
    u64::from_le_bytes(bytes[..8].try_into().unwrap())
}

#[inline]
fn read_u32_le(bytes: &[u8]) -> u64 {
    u32::from_le_bytes(bytes[..4].try_into().unwrap()) as u64
}

#[inline]
fn read_small(bytes: &[u8]) -> u64 {
    let len = bytes.len();
    ((bytes[0] as u64) << 16) | ((bytes[len >> 1] as u64) << 8) | (bytes[len - 1] as u64)
}

#[inline]
fn wyhash64(key: &[u8]) -> u64 {
    let len = key.len();
    let mut seed = SECRET0 ^ wymix(SECRET0, SECRET1);
    let (a, b) = if len <= 16 {
        if len >= 4 {
            let off = (len >> 3) << 2;
            let a = (read_u32_le(&key[..4]) << 32) | read_u32_le(&key[off..]);
            let b = (read_u32_le(&key[len - 4..]) << 32) | read_u32_le(&key[len - 4 - off..]);
            (a, b)
        } else if len > 0 {
            (read_small(key), 0)
        } else {
            (0, 0)
        }
    } else {
        let mut rem = key;
        if rem.len() > 48 {
            let mut see1 = seed;
            let mut see2 = seed;
            while rem.len() > 48 {
                seed = wymix(
                    read_u64_le(&rem[0..8]) ^ SECRET1,
                    read_u64_le(&rem[8..16]) ^ seed,
                );
                see1 = wymix(
                    read_u64_le(&rem[16..24]) ^ SECRET2,
                    read_u64_le(&rem[24..32]) ^ see1,
                );
                see2 = wymix(
                    read_u64_le(&rem[32..40]) ^ SECRET3,
                    read_u64_le(&rem[40..48]) ^ see2,
                );
                rem = &rem[48..];
            }
            seed ^= see1 ^ see2;
        }
        while rem.len() > 16 {
            seed = wymix(
                read_u64_le(&rem[0..8]) ^ SECRET1,
                read_u64_le(&rem[8..16]) ^ seed,
            );
            rem = &rem[16..];
        }
        (
            read_u64_le(&key[len - 16..len - 8]),
            read_u64_le(&key[len - 8..len]),
        )
    };
    wymix(SECRET1 ^ (len as u64), wymix(a ^ SECRET1, b ^ seed))
}

/// Compact in-memory Bloom filter for an `idx_<SeqID>.db` segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BloomFilter {
    bits: Vec<u8>,
    num_bits: u32,
    num_probes: u8,
}

const MAX_BLOOM_BITS: u64 = (u32::MAX - 7) as u64;
const MAX_BLOOM_PROBES: u8 = 16;

fn compute_bloom_sizing(num_keys: usize, bits_per_key: usize) -> (usize, u32) {
    let raw_bits = (num_keys as u64)
        .saturating_mul(bits_per_key as u64)
        .clamp(64, MAX_BLOOM_BITS);
    let num_bytes = raw_bits.div_ceil(8) as usize;
    let num_bits = (num_bytes as u64 * 8) as u32;
    (num_bytes, num_bits)
}

#[inline(always)]
fn fastrange32(hash: u32, range: u32) -> usize {
    (((hash as u64).wrapping_mul(range as u64)) >> 32) as usize
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
        let k = ((bits_per_key as f64 * 0.69) as u8).clamp(1, MAX_BLOOM_PROBES);
        let (num_bytes, num_bits) = compute_bloom_sizing(num_keys, bits_per_key);

        Self {
            bits: vec![0u8; num_bytes],
            num_bits,
            num_probes: k,
        }
    }

    #[inline]
    fn unpack_hash_pair(hash: u64) -> (u32, u32) {
        ((hash >> 32) as u32, (hash as u32) | 1)
    }

    #[inline]
    fn insert_pair(&mut self, h1: u32, h2: u32) {
        let mut h = h1;
        for _ in 0..self.num_probes {
            let bit_idx = fastrange32(h, self.num_bits);
            self.bits[bit_idx >> 3] |= 1u8 << (bit_idx & 7);
            h = h.wrapping_add(h2);
        }
    }

    /// Computes the `(h1, h2)` double-hashing probe pair for `key`.
    #[inline]
    pub fn hash_pair(key: &[u8]) -> (u32, u32) {
        Self::unpack_hash_pair(Self::hash64(key))
    }

    /// Computes a full-entropy 64-bit hash for `key` in a single pass.
    #[inline]
    pub fn hash64(key: &[u8]) -> u64 {
        wyhash64(key)
    }

    /// Builds a [`BloomFilter`] from 64-bit hashes without retaining raw keys.
    pub fn from_hashes64(hashes: &[u64], bits_per_key: usize) -> Self {
        Self::from_hashes64_bounded(hashes, bits_per_key, usize::MAX)
    }

    /// Returns the bitset byte length that [`Self::from_hashes64`] would allocate for
    /// `(num_keys, bits_per_key)`.
    pub fn desired_byte_len(num_keys: usize, bits_per_key: usize) -> usize {
        if bits_per_key == 0 || num_keys == 0 {
            return 0;
        }
        compute_bloom_sizing(num_keys, bits_per_key).0
    }

    /// Like [`Self::from_hashes64`], capping the allocated bitset to at most `max_bytes`
    /// (`< 8` bytes disables the filter) and recomputing the optimal probe count `k` for the
    /// resulting effective bits per key.
    pub fn from_hashes64_bounded(hashes: &[u64], bits_per_key: usize, max_bytes: usize) -> Self {
        if bits_per_key == 0 || hashes.is_empty() || max_bytes < 8 {
            return Self::new(0, 0);
        }
        let (desired_bytes, _) = compute_bloom_sizing(hashes.len(), bits_per_key);
        let num_bytes = desired_bytes.min(max_bytes);
        let clamped_bits = (num_bytes as u64).saturating_mul(8).min(MAX_BLOOM_BITS);
        let num_bytes = (clamped_bits / 8) as usize;
        if num_bytes < 8 {
            return Self::new(0, 0);
        }
        let num_bits = (num_bytes as u64 * 8) as u32;
        let effective_bpk = (bits_per_key as f64).min((num_bits as f64) / (hashes.len() as f64));
        let k = ((effective_bpk * 0.69) as u8).clamp(1, MAX_BLOOM_PROBES);
        let mut filter = Self {
            bits: vec![0u8; num_bytes],
            num_bits,
            num_probes: k,
        };
        for &packed in hashes {
            let (h1, h2) = Self::unpack_hash_pair(packed);
            filter.insert_pair(h1, h2);
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
        self.insert_pair(h1, h2);
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
        self.may_contain_hashed(h1, h2)
    }

    /// Like [`Self::may_contain`], with the pair from [`Self::hash_pair`] already computed.
    #[inline]
    pub fn may_contain_hashed(&self, h1: u32, h2: u32) -> bool {
        if self.num_bits == 0 {
            return true;
        }
        let mut h = h1;
        for _ in 0..self.num_probes {
            let bit_idx = fastrange32(h, self.num_bits);
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

        if byte_len == 0 && num_probes != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "empty BloomFilter cannot specify probes",
            ));
        }
        if byte_len > 0 && (num_probes == 0 || num_probes > MAX_BLOOM_PROBES) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid num_probes in BloomFilter header",
            ));
        }

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bloom_sizing_clamps_without_u32_truncation() {
        let (bytes_max, bits_max) = compute_bloom_sizing(usize::MAX, 10);
        assert!(bits_max > 0);
        assert_eq!(bits_max as usize, bytes_max * 8);

        let (bytes_512m, bits_512m) = compute_bloom_sizing(1 << 26, 8);
        assert!(bits_512m > 0);
        assert_eq!(bits_512m as usize, bytes_512m * 8);

        let (bytes_exact_wrap, bits_exact_wrap) = compute_bloom_sizing(1 << 29, 8);
        assert!(bits_exact_wrap > 0);
        assert_eq!(bits_exact_wrap as usize, bytes_exact_wrap * 8);
    }

    #[test]
    fn hash_pair_components_are_nonlinear_and_independent_across_equal_length_keys() {
        use std::collections::HashSet;

        let mut xors = HashSet::new();
        let mut seen_even = false;
        let mut seen_odd = false;
        for i in 0..256u32 {
            let key = format!("key_equal_{i:06}");
            assert_eq!(key.len(), 16);
            let h64 = BloomFilter::hash64(key.as_bytes());
            seen_even |= (h64 & 1) == 0;
            seen_odd |= (h64 & 1) == 1;
            let (h1, h2) = BloomFilter::hash_pair(key.as_bytes());
            assert_eq!(h2 & 1, 1);
            xors.insert(h1 ^ h2);
        }
        assert!(seen_even && seen_odd, "hash64 should use full 64-bit LSB");
        assert!(
            xors.len() >= 200,
            "h1 ^ h2 across equal-length keys must not collapse to a constant (got {} distinct)",
            xors.len()
        );

        let mut length_hashes = HashSet::new();
        for len in [0usize, 1, 2, 3, 4, 7, 8, 15, 16, 17, 48, 49, 100] {
            let base = vec![0xA5u8; len];
            let h_base = BloomFilter::hash64(&base);
            assert!(length_hashes.insert(h_base));
            if len > 0 {
                let mut flipped = base.clone();
                flipped[len / 2] ^= 1;
                let h_flip = BloomFilter::hash64(&flipped);
                assert_ne!(h_base, h_flip);
                assert_ne!(h_base >> 32, h_flip >> 32);
                assert_ne!(h_base as u32, h_flip as u32);
            }
        }
    }

    #[test]
    fn bloom_from_hashes64_matches_insert_and_has_low_false_positive_rate() {
        let mut keys = Vec::with_capacity(2000);
        for i in 0..2000usize {
            let len = i % 100;
            let mut key = vec![(i & 0xFF) as u8; len];
            if len >= 4 {
                key[..4].copy_from_slice(&(i as u32).to_le_bytes());
            } else {
                key.extend_from_slice(&(i as u32).to_le_bytes());
            }
            keys.push(key);
        }

        let mut f_insert = BloomFilter::new(keys.len(), 10);
        let mut hashes = Vec::with_capacity(keys.len());
        for key in &keys {
            f_insert.insert(key);
            hashes.push(BloomFilter::hash64(key));
        }
        let f_hashed = BloomFilter::from_hashes64(&hashes, 10);
        assert_eq!(f_insert, f_hashed);

        for key in &keys {
            assert!(f_insert.may_contain(key), "false negative for {key:?}");
        }

        let mut false_positives = 0usize;
        for j in 0..10_000u32 {
            let probe = format!("absent_probe_{j:06}");
            if f_hashed.may_contain(probe.as_bytes()) {
                false_positives += 1;
            }
        }
        assert!(
            false_positives < 250,
            "unexpectedly high false positive rate: {false_positives}/10000"
        );
    }

    #[test]
    fn fastrange32_stays_in_bounds_and_bloom_fp_rate_stays_low() {
        for range in [1u32, 64, 65, 1000, u32::MAX] {
            assert_eq!(fastrange32(0, range), 0);
            assert!(fastrange32(u32::MAX, range) < range as usize);
            for h in [1u32, 0x1234_5678, 0x7FFF_FFFF, 0x8000_0000, u32::MAX - 1] {
                assert!(fastrange32(h, range) < range as usize);
            }
        }

        let mut filter = BloomFilter::new(2000, 10);
        for i in 0..2000u32 {
            let k = format!("equal_len_{i:06}");
            filter.insert(k.as_bytes());
        }
        for i in 0..2000u32 {
            let k = format!("equal_len_{i:06}");
            assert!(filter.may_contain(k.as_bytes()));
        }
        let mut false_positives = 0usize;
        for i in 2000..12_000u32 {
            let k = format!("equal_len_{i:06}");
            if filter.may_contain(k.as_bytes()) {
                false_positives += 1;
            }
        }
        assert!(
            false_positives < 250,
            "unexpectedly high fastrange32 false positive rate: {false_positives}/10000"
        );
    }

    #[test]
    fn from_hashes64_bounded_respects_max_bytes_and_has_zero_false_negatives() {
        let hashes: Vec<u64> = (0..10_000u64)
            .map(|i| BloomFilter::hash64(&i.to_le_bytes()))
            .collect();
        // Unbounded at 10 bpk would need 12_500 bytes; cap at 2_000 bytes.
        let bounded = BloomFilter::from_hashes64_bounded(&hashes, 10, 2_000);
        assert!(bounded.heap_bytes() <= 2_000);
        assert!(bounded.heap_bytes() > 0);
        for i in 0..10_000u64 {
            assert!(bounded.may_contain(&i.to_le_bytes()));
        }

        // Tiny budget < 8 bytes disables the filter cleanly instead of failing.
        let disabled = BloomFilter::from_hashes64_bounded(&hashes, 10, 7);
        assert_eq!(disabled.heap_bytes(), 0);
        assert!(disabled.may_contain(&0u64.to_le_bytes()));
    }

    #[test]
    fn bloom_decode_rejects_invalid_probe_counts() {
        // Non-zero probes on an empty bitset.
        let mut raw_empty_with_probes = Vec::new();
        raw_empty_with_probes.push(4);
        raw_empty_with_probes.extend_from_slice(&[0u8; 3]);
        raw_empty_with_probes.extend_from_slice(&0u32.to_le_bytes());
        let mut slice = raw_empty_with_probes.as_slice();
        assert_eq!(
            BloomFilter::decode(&mut slice).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );

        // 0 probes on a non-empty bitset.
        let mut raw_zero_probes = vec![0u8; 16];
        raw_zero_probes[4..8].copy_from_slice(&8u32.to_le_bytes());
        let mut slice = raw_zero_probes.as_slice();
        assert_eq!(
            BloomFilter::decode(&mut slice).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );

        // Excessive probes > MAX_BLOOM_PROBES on a non-empty bitset.
        let mut raw_too_many_probes = vec![0u8; 16];
        raw_too_many_probes[0] = MAX_BLOOM_PROBES + 1;
        raw_too_many_probes[4..8].copy_from_slice(&8u32.to_le_bytes());
        let mut slice = raw_too_many_probes.as_slice();
        assert_eq!(
            BloomFilter::decode(&mut slice).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }
}
