//! Zero-copy little-endian binary slice reader used by metadata and WAL decoders.

use std::io;

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
            return Err(io::Error::new(io::ErrorKind::InvalidData, self.eof_msg));
        };
        if end > self.buf.len() {
            return Err(io::Error::new(io::ErrorKind::InvalidData, self.eof_msg));
        }
        let slice = &self.buf[self.pos..end];
        self.pos = end;
        Ok(slice)
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
