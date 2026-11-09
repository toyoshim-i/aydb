//! Hardware-accelerated CRC32C (Castagnoli, polynomial `0x1EDC6F41` / reflected `0x82F63B78`).
//!
//! Uses CPU hardware instructions (`SSE4.2` on x86/x86_64, `crc` on AArch64) with runtime
//! feature detection, falling back to a `const`-generated Slice-by-8 lookup table in pure
//! safe Rust. Zero external crate dependencies.

/// Reflected Castagnoli polynomial (`0x1EDC6F41` bit-reversed).
const POLY: u32 = 0x82F6_3B78;

/// Matrix representation of advancing a reflected CRC32C state by 1 zero bit.
const MAT_1_BIT: [u32; 32] = {
    let mut mat = [0u32; 32];
    mat[0] = POLY;
    let mut i = 1;
    while i < 32 {
        mat[i] = 1 << (i - 1);
        i += 1;
    }
    mat
};

/// Multiplies a GF(2) 32x32 matrix `mat` by column vector `vec`.
#[inline(always)]
const fn gf2_matrix_times(mat: &[u32; 32], vec: u32) -> u32 {
    let mut sum = 0u32;
    let mut idx = 0;
    while idx < 32 {
        if (vec & (1 << idx)) != 0 {
            sum ^= mat[idx];
        }
        idx += 1;
    }
    sum
}

/// Multiplies two GF(2) 32x32 matrices: `res = b * a`.
const fn gf2_matrix_mul(a: &[u32; 32], b: &[u32; 32]) -> [u32; 32] {
    let mut res = [0u32; 32];
    let mut i = 0;
    while i < 32 {
        res[i] = gf2_matrix_times(b, a[i]);
        i += 1;
    }
    res
}

/// Computes `mat^(power)` using binary exponentiation in GF(2).
const fn gf2_matrix_power(mut mat: [u32; 32], mut power: usize) -> [u32; 32] {
    let mut res = [0u32; 32];
    let mut i = 0;
    while i < 32 {
        res[i] = 1 << i; // Identity matrix
        i += 1;
    }
    while power > 0 {
        if (power & 1) != 0 {
            res = gf2_matrix_mul(&res, &mat);
        }
        mat = gf2_matrix_mul(&mat, &mat);
        power >>= 1;
    }
    res
}

/// Precomputed GF(2) matrices for shifting CRC32C state by N zero bytes:
/// advancing by N zero bytes is multiplying by `MAT_1_BIT^(8 * N)`.
const MAT_64: [u32; 32] = gf2_matrix_power(MAT_1_BIT, 8 * 64);
const MAT_128: [u32; 32] = gf2_matrix_power(MAT_1_BIT, 8 * 128);
const MAT_256: [u32; 32] = gf2_matrix_power(MAT_1_BIT, 8 * 256);
const MAT_1024: [u32; 32] = gf2_matrix_power(MAT_1_BIT, 8 * 1024);
const MAT_2048: [u32; 32] = gf2_matrix_power(MAT_1_BIT, 8 * 2048);

/// Generates an 8-nibble lookup table (8 x 16 entries = 512 bytes) from a 32x32 matrix.
const fn make_nibble_table(mat: &[u32; 32]) -> [[u32; 16]; 8] {
    let mut table = [[0u32; 16]; 8];
    let mut nib = 0;
    while nib < 8 {
        let mut val = 0;
        while val < 16 {
            let mut sum = 0u32;
            let mut bit = 0;
            while bit < 4 {
                if (val & (1 << bit)) != 0 {
                    sum ^= mat[nib * 4 + bit];
                }
                bit += 1;
            }
            table[nib][val] = sum;
            val += 1;
        }
        nib += 1;
    }
    table
}

const TBL_64: [[u32; 16]; 8] = make_nibble_table(&MAT_64);
const TBL_128: [[u32; 16]; 8] = make_nibble_table(&MAT_128);
const TBL_256: [[u32; 16]; 8] = make_nibble_table(&MAT_256);
const TBL_1024: [[u32; 16]; 8] = make_nibble_table(&MAT_1024);
const TBL_2048: [[u32; 16]; 8] = make_nibble_table(&MAT_2048);

/// Fast O(1) GF(2) matrix-vector multiplication using precomputed 8-nibble lookup table.
#[inline(always)]
fn apply_nibble_table(tbl: &[[u32; 16]; 8], vec: u32) -> u32 {
    tbl[0][(vec & 0x0F) as usize]
        ^ tbl[1][((vec >> 4) & 0x0F) as usize]
        ^ tbl[2][((vec >> 8) & 0x0F) as usize]
        ^ tbl[3][((vec >> 12) & 0x0F) as usize]
        ^ tbl[4][((vec >> 16) & 0x0F) as usize]
        ^ tbl[5][((vec >> 20) & 0x0F) as usize]
        ^ tbl[6][((vec >> 24) & 0x0F) as usize]
        ^ tbl[7][((vec >> 28) & 0x0F) as usize]
}

/// Compile-time generated 8 x 256 Slice-by-8 lookup table for CRC32C.
const TABLE: [[u32; 256]; 8] = {
    let mut table = [[0u32; 256]; 8];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u32;
        let mut bit = 0;
        while bit < 8 {
            if (crc & 1) != 0 {
                crc = (crc >> 1) ^ POLY;
            } else {
                crc >>= 1;
            }
            bit += 1;
        }
        table[0][i] = crc;
        i += 1;
    }

    let mut slice = 1;
    while slice < 8 {
        let mut idx = 0;
        while idx < 256 {
            let prev = table[slice - 1][idx];
            table[slice][idx] = (prev >> 8) ^ table[0][(prev & 0xFF) as usize];
            idx += 1;
        }
        slice += 1;
    }
    table
};

/// Computes the CRC32C checksum of `data`.
#[inline]
pub fn crc32c(data: &[u8]) -> u32 {
    crc32c_append(0, data)
}

/// Updates an existing CRC32C checksum `crc` with additional `data`.
#[inline]
pub fn crc32c_append(crc: u32, data: &[u8]) -> u32 {
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        if std::arch::is_x86_feature_detected!("sse4.2") {
            // Safety: SSE4.2 support has been verified at runtime.
            return unsafe { crc32c_sse42(crc, data) };
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        if std::arch::is_aarch64_feature_detected!("crc") {
            // Safety: ARMv8 CRC32 extension support has been verified at runtime.
            return unsafe { crc32c_aarch64(crc, data) };
        }
    }
    crc32c_software(crc, data)
}

/// Pure safe Rust Slice-by-8 implementation of CRC32C.
#[inline]
pub fn crc32c_software(crc: u32, mut data: &[u8]) -> u32 {
    let mut c = !crc;

    while data.len() >= 8 {
        let low = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) ^ c;
        let high = u32::from_le_bytes([data[4], data[5], data[6], data[7]]);

        c = TABLE[7][(low & 0xFF) as usize]
            ^ TABLE[6][((low >> 8) & 0xFF) as usize]
            ^ TABLE[5][((low >> 16) & 0xFF) as usize]
            ^ TABLE[4][(low >> 24) as usize]
            ^ TABLE[3][(high & 0xFF) as usize]
            ^ TABLE[2][((high >> 8) & 0xFF) as usize]
            ^ TABLE[1][((high >> 16) & 0xFF) as usize]
            ^ TABLE[0][(high >> 24) as usize];

        data = &data[8..];
    }

    for &b in data {
        c = (c >> 8) ^ TABLE[0][((c as u8) ^ b) as usize];
    }

    !c
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "sse4.2")]
unsafe fn crc32c_sse42(crc: u32, mut data: &[u8]) -> u32 {
    #[cfg(target_arch = "x86")]
    use core::arch::x86::_mm_crc32_u8;
    #[cfg(target_arch = "x86_64")]
    use core::arch::x86_64::{_mm_crc32_u8, _mm_crc32_u64};

    let mut c = !crc;

    #[cfg(target_arch = "x86_64")]
    {
        // 3-way interleaved pipelined CRC32C over 1024-byte stripes (3072 B blocks).
        // Hides the 3-cycle latency of _mm_crc32_u64 by executing 3 independent lanes in parallel.
        while data.len() >= 3072 {
            let mut c0 = c as u64;
            let mut c1 = 0u64;
            let mut c2 = 0u64;
            let ptr = data.as_ptr();

            let mut offset = 0;
            while offset < 1024 {
                let w0 = unsafe { (ptr.add(offset) as *const u64).read_unaligned().to_le() };
                let w1 = unsafe {
                    (ptr.add(1024 + offset) as *const u64)
                        .read_unaligned()
                        .to_le()
                };
                let w2 = unsafe {
                    (ptr.add(2048 + offset) as *const u64)
                        .read_unaligned()
                        .to_le()
                };
                c0 = _mm_crc32_u64(c0, w0);
                c1 = _mm_crc32_u64(c1, w1);
                c2 = _mm_crc32_u64(c2, w2);
                offset += 8;
            }

            c = apply_nibble_table(&TBL_2048, c0 as u32)
                ^ apply_nibble_table(&TBL_1024, c1 as u32)
                ^ (c2 as u32);
            data = &data[3072..];
        }

        // 3-way interleaved pipelined CRC32C over 128-byte stripes (384 B blocks).
        while data.len() >= 384 {
            let mut c0 = c as u64;
            let mut c1 = 0u64;
            let mut c2 = 0u64;
            let ptr = data.as_ptr();

            let mut offset = 0;
            while offset < 128 {
                let w0 = unsafe { (ptr.add(offset) as *const u64).read_unaligned().to_le() };
                let w1 = unsafe {
                    (ptr.add(128 + offset) as *const u64)
                        .read_unaligned()
                        .to_le()
                };
                let w2 = unsafe {
                    (ptr.add(256 + offset) as *const u64)
                        .read_unaligned()
                        .to_le()
                };
                c0 = _mm_crc32_u64(c0, w0);
                c1 = _mm_crc32_u64(c1, w1);
                c2 = _mm_crc32_u64(c2, w2);
                offset += 8;
            }

            c = apply_nibble_table(&TBL_256, c0 as u32)
                ^ apply_nibble_table(&TBL_128, c1 as u32)
                ^ (c2 as u32);
            data = &data[384..];
        }

        // 3-way interleaved pipelined CRC32C over 64-byte stripes (192 B blocks).
        while data.len() >= 192 {
            let mut c0 = c as u64;
            let mut c1 = 0u64;
            let mut c2 = 0u64;
            let ptr = data.as_ptr();

            let mut offset = 0;
            while offset < 64 {
                let w0 = unsafe { (ptr.add(offset) as *const u64).read_unaligned().to_le() };
                let w1 = unsafe {
                    (ptr.add(64 + offset) as *const u64)
                        .read_unaligned()
                        .to_le()
                };
                let w2 = unsafe {
                    (ptr.add(128 + offset) as *const u64)
                        .read_unaligned()
                        .to_le()
                };
                c0 = _mm_crc32_u64(c0, w0);
                c1 = _mm_crc32_u64(c1, w1);
                c2 = _mm_crc32_u64(c2, w2);
                offset += 8;
            }

            c = apply_nibble_table(&TBL_128, c0 as u32)
                ^ apply_nibble_table(&TBL_64, c1 as u32)
                ^ (c2 as u32);
            data = &data[192..];
        }

        // Remaining 8-byte words
        let mut c64 = c as u64;
        while data.len() >= 8 {
            let word = unsafe { (data.as_ptr() as *const u64).read_unaligned().to_le() };
            c64 = _mm_crc32_u64(c64, word);
            data = &data[8..];
        }
        c = c64 as u32;
    }

    for &b in data {
        c = _mm_crc32_u8(c, b);
    }

    !c
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "crc")]
unsafe fn crc32c_aarch64(crc: u32, mut data: &[u8]) -> u32 {
    use core::arch::aarch64::{__crc32cb, __crc32cd};

    let mut c = !crc;

    // 3-way interleaved pipelined CRC32C over 1024-byte stripes (3072 B blocks).
    while data.len() >= 3072 {
        let mut c0 = c;
        let mut c1 = 0u32;
        let mut c2 = 0u32;
        let ptr = data.as_ptr();

        let mut offset = 0;
        while offset < 1024 {
            let w0 = unsafe { (ptr.add(offset) as *const u64).read_unaligned().to_le() };
            let w1 = unsafe {
                (ptr.add(1024 + offset) as *const u64)
                    .read_unaligned()
                    .to_le()
            };
            let w2 = unsafe {
                (ptr.add(2048 + offset) as *const u64)
                    .read_unaligned()
                    .to_le()
            };
            c0 = __crc32cd(c0, w0);
            c1 = __crc32cd(c1, w1);
            c2 = __crc32cd(c2, w2);
            offset += 8;
        }

        c = apply_nibble_table(&TBL_2048, c0) ^ apply_nibble_table(&TBL_1024, c1) ^ c2;
        data = &data[3072..];
    }

    // 3-way interleaved pipelined CRC32C over 128-byte stripes (384 B blocks).
    while data.len() >= 384 {
        let mut c0 = c;
        let mut c1 = 0u32;
        let mut c2 = 0u32;
        let ptr = data.as_ptr();

        let mut offset = 0;
        while offset < 128 {
            let w0 = unsafe { (ptr.add(offset) as *const u64).read_unaligned().to_le() };
            let w1 = unsafe {
                (ptr.add(128 + offset) as *const u64)
                    .read_unaligned()
                    .to_le()
            };
            let w2 = unsafe {
                (ptr.add(256 + offset) as *const u64)
                    .read_unaligned()
                    .to_le()
            };
            c0 = __crc32cd(c0, w0);
            c1 = __crc32cd(c1, w1);
            c2 = __crc32cd(c2, w2);
            offset += 8;
        }

        c = apply_nibble_table(&TBL_256, c0) ^ apply_nibble_table(&TBL_128, c1) ^ c2;
        data = &data[384..];
    }

    // 3-way interleaved pipelined CRC32C over 64-byte stripes (192 B blocks).
    while data.len() >= 192 {
        let mut c0 = c;
        let mut c1 = 0u32;
        let mut c2 = 0u32;
        let ptr = data.as_ptr();

        let mut offset = 0;
        while offset < 64 {
            let w0 = unsafe { (ptr.add(offset) as *const u64).read_unaligned().to_le() };
            let w1 = unsafe {
                (ptr.add(64 + offset) as *const u64)
                    .read_unaligned()
                    .to_le()
            };
            let w2 = unsafe {
                (ptr.add(128 + offset) as *const u64)
                    .read_unaligned()
                    .to_le()
            };
            c0 = __crc32cd(c0, w0);
            c1 = __crc32cd(c1, w1);
            c2 = __crc32cd(c2, w2);
            offset += 8;
        }

        c = apply_nibble_table(&TBL_128, c0) ^ apply_nibble_table(&TBL_64, c1) ^ c2;
        data = &data[192..];
    }

    while data.len() >= 8 {
        let word = unsafe { (data.as_ptr() as *const u64).read_unaligned().to_le() };
        c = __crc32cd(c, word);
        data = &data[8..];
    }
    for &b in data {
        c = __crc32cb(c, b);
    }
    !c
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc3720_test_vectors() {
        // Standard iSCSI / RFC 3720 CRC32C check value for ASCII "123456789"
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
        assert_eq!(crc32c_software(0, b"123456789"), 0xE306_9283);

        // 32 bytes of zeros -> 0x8A9136AA
        let zeros = [0u8; 32];
        assert_eq!(crc32c(&zeros), 0x8A91_36AA);
        assert_eq!(crc32c_software(0, &zeros), 0x8A91_36AA);

        // 32 bytes of 0xFF -> 0x62A8AB43
        let ones = [0xFFu8; 32];
        assert_eq!(crc32c(&ones), 0x62A8_AB43);
        assert_eq!(crc32c_software(0, &ones), 0x62A8_AB43);

        // 32 bytes incrementing 0x00..0x1F -> 0x46DD794E
        let mut inc = [0u8; 32];
        for (i, b) in inc.iter_mut().enumerate() {
            *b = i as u8;
        }
        assert_eq!(crc32c(&inc), 0x46DD_794E);
        assert_eq!(crc32c_software(0, &inc), 0x46DD_794E);
    }

    #[test]
    fn gf2_matrix_shift_matches_crc() {
        for seed in [0u32, 1, 0x1234_5678, 0xDEAD_BEEF, 0xFFFF_FFFF] {
            // Raw state in CRC is `c`. Feeding N zero bytes with initial raw state `c`:
            // `crc32c_append(!c, &[0; N])` returns `!final_state`.
            // So `final_state = !crc32c_append(!c, &[0; N])`.
            let s_64 = !crc32c_append(!seed, &[0u8; 64]);
            assert_eq!(gf2_matrix_times(&MAT_64, seed), s_64);

            let s_128 = !crc32c_append(!seed, &[0u8; 128]);
            assert_eq!(gf2_matrix_times(&MAT_128, seed), s_128);

            let s_256 = !crc32c_append(!seed, &[0u8; 256]);
            assert_eq!(gf2_matrix_times(&MAT_256, seed), s_256);

            let s_1024 = !crc32c_append(!seed, &[0u8; 1024]);
            assert_eq!(gf2_matrix_times(&MAT_1024, seed), s_1024);

            let s_2048 = !crc32c_append(!seed, &[0u8; 2048]);
            assert_eq!(gf2_matrix_times(&MAT_2048, seed), s_2048);
        }
    }

    #[test]
    fn hardware_matches_software_comprehensive() {
        let mut buf = vec![0u8; 65536 + 64];
        for (i, b) in buf.iter_mut().enumerate() {
            *b = (i.wrapping_mul(167).wrapping_add(37) & 0xFF) as u8;
        }

        // Test every length from 0 to 8192 with various seeds and alignments
        let seeds = [0u32, 0x1234_5678, 0xDEAD_BEEF, 0xFFFF_FFFF];
        for len in 0..=8192 {
            let offset = (len * 7) % 8; // varying alignments
            let seed = seeds[len % seeds.len()];
            let slice = &buf[offset..offset + len];

            let hw = crc32c_append(seed, slice);
            let sw = crc32c_software(seed, slice);
            assert_eq!(
                hw, sw,
                "mismatch at len={len}, offset={offset}, seed={seed:#x}"
            );
        }

        // Test larger block sizes up to 65536
        for &len in &[8193, 12288, 16384, 32768, 65536] {
            for &seed in &seeds {
                for offset in 0..8 {
                    let slice = &buf[offset..offset + len];
                    let hw = crc32c_append(seed, slice);
                    let sw = crc32c_software(seed, slice);
                    assert_eq!(
                        hw, sw,
                        "mismatch at large len={len}, offset={offset}, seed={seed:#x}"
                    );
                }
            }
        }
    }

    #[test]
    fn hardware_matches_software_and_incremental() {
        let mut buf = vec![0u8; 4099];
        for (i, b) in buf.iter_mut().enumerate() {
            *b = (i.wrapping_mul(131).wrapping_add(17) & 0xFF) as u8;
        }

        for len in [0, 1, 3, 7, 8, 9, 15, 16, 63, 64, 511, 512, 4092, 4096, 4099] {
            let slice = &buf[..len];
            let hw = crc32c(slice);
            let sw = crc32c_software(0, slice);
            assert_eq!(hw, sw, "mismatch at len={len}");

            if len >= 10 {
                let split = len / 2;
                let part1 = crc32c(&slice[..split]);
                let combined = crc32c_append(part1, &slice[split..]);
                assert_eq!(combined, hw, "incremental mismatch at len={len}");
            }
        }
    }
}
