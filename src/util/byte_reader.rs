//! Zero-copy little-endian binary slice reader used by metadata and WAL decoders.

use std::io;

use crate::util::io_err;

/// Sequential bounds-checked binary reader over a byte slice.
#[derive(Debug, Clone, Copy)]
pub struct ByteReader<'a> {
    buf: &'a [u8],
    pos: usize,
    eof_msg: &'static str,
}

impl<'a> ByteReader<'a> {
    #[inline]
    pub const fn new(buf: &'a [u8]) -> Self {
        Self {
            buf,
            pos: 0,
            eof_msg: "unexpected end of buffer",
        }
    }

    #[inline]
    pub const fn with_eof_msg(buf: &'a [u8], eof_msg: &'static str) -> Self {
        Self {
            buf,
            pos: 0,
            eof_msg,
        }
    }

    #[inline]
    pub const fn position(&self) -> usize {
        self.pos
    }

    #[inline]
    pub const fn remaining(&self) -> usize {
        self.buf.len().saturating_sub(self.pos)
    }

    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.pos >= self.buf.len()
    }

    #[inline]
    pub fn remaining_slice(&self) -> &'a [u8] {
        &self.buf[self.pos..]
    }

    #[inline]
    pub fn read_bytes(&mut self, len: usize) -> io::Result<&'a [u8]> {
        let Some(end) = self.pos.checked_add(len) else {
            return Err(io_err(io::ErrorKind::InvalidData, self.eof_msg));
        };
        if end > self.buf.len() {
            return Err(io_err(io::ErrorKind::InvalidData, self.eof_msg));
        }
        let slice = &self.buf[self.pos..end];
        self.pos = end;
        Ok(slice)
    }

    #[inline]
    pub fn read_array<const N: usize>(&mut self) -> io::Result<[u8; N]> {
        let slice = self.read_bytes(N)?;
        let mut out = [0u8; N];
        out.copy_from_slice(slice);
        Ok(out)
    }

    #[inline]
    pub fn skip(&mut self, len: usize) -> io::Result<()> {
        self.read_bytes(len).map(|_| ())
    }

    #[inline]
    pub fn read_range(&mut self, len: usize) -> io::Result<std::ops::Range<usize>> {
        let start = self.pos;
        let _ = self.read_bytes(len)?;
        Ok(start..self.pos)
    }

    #[inline]
    pub fn read_u8(&mut self) -> io::Result<u8> {
        let bytes = self.read_bytes(1)?;
        Ok(bytes[0])
    }

    #[inline]
    pub fn read_u16_le(&mut self) -> io::Result<u16> {
        let bytes = self.read_bytes(2)?;
        Ok(u16::from_le_bytes([bytes[0], bytes[1]]))
    }

    #[inline]
    pub fn read_u32_le(&mut self) -> io::Result<u32> {
        let bytes = self.read_bytes(4)?;
        Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    #[inline]
    pub fn read_u64_le(&mut self) -> io::Result<u64> {
        let bytes = self.read_bytes(8)?;
        Ok(u64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn byte_reader_reads_primitives_ranges_arrays_and_rejects_truncation() {
        let mut data = Vec::new();
        data.push(0xAA);
        data.extend_from_slice(&0x1234u16.to_le_bytes());
        data.extend_from_slice(&0xdead_beefu32.to_le_bytes());
        data.extend_from_slice(&0x0102_0304_0506_0708u64.to_le_bytes());
        data.extend_from_slice(b"skip!");
        data.extend_from_slice(b"hello");
        data.extend_from_slice(&[9, 8, 7]);

        let mut r = ByteReader::with_eof_msg(&data, "custom eof");
        assert_eq!(r.position(), 0);
        assert_eq!(r.remaining(), data.len());
        assert!(!r.is_empty());

        assert_eq!(r.read_u8().unwrap(), 0xAA);
        assert_eq!(r.read_u16_le().unwrap(), 0x1234);
        assert_eq!(r.read_u32_le().unwrap(), 0xdead_beef);
        assert_eq!(r.read_u64_le().unwrap(), 0x0102_0304_0506_0708);
        r.skip(5).unwrap();
        let range = r.read_range(5).unwrap();
        assert_eq!(&data[range], b"hello");
        assert_eq!(r.read_array::<3>().unwrap(), [9, 8, 7]);
        assert!(r.is_empty());
        assert_eq!(r.remaining(), 0);
        assert_eq!(r.remaining_slice(), &[]);

        let eof_err = r.read_u8().unwrap_err();
        assert_eq!(eof_err.kind(), io::ErrorKind::InvalidData);
        assert_eq!(eof_err.to_string(), "custom eof");

        let mut overflow = ByteReader::new(b"abc");
        overflow.skip(1).unwrap();
        let ov_err = overflow.read_bytes(usize::MAX).unwrap_err();
        assert_eq!(ov_err.kind(), io::ErrorKind::InvalidData);
    }
}
