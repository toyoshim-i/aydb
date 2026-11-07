//! Hardware-accelerated CRC32C (Castagnoli, polynomial `0x1EDC6F41` / reflected `0x82F63B78`).
//!
//! Uses CPU hardware instructions (`SSE4.2` on x86/x86_64, `crc` on AArch64) with runtime
//! feature detection, falling back to a `const`-generated Slice-by-8 lookup table in pure
//! safe Rust. Zero external crate dependencies.

/// Reflected Castagnoli polynomial (`0x1EDC6F41` bit-reversed).
const POLY: u32 = 0x82F6_3B78;

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
    use core::arch::x86_64::{_mm_crc32_u64, _mm_crc32_u8};

    let mut c = !crc;

    #[cfg(target_arch = "x86_64")]
    {
        let mut c64 = c as u64;
        while data.len() >= 8 {
            let word = u64::from_le_bytes([
                data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
            ]);
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
    while data.len() >= 8 {
        let word = u64::from_le_bytes([
            data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
        ]);
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
