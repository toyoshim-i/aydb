//! Common low-level utilities for `aydb`.

pub mod byte_reader;
pub mod crc32c;
#[cfg(test)]
pub mod test_util;

/// Builds an [`std::io::Error`] with a static message, collapsing to zero-allocation
/// [`std::io::Error::from`] under Kani so CBMC does not unwind `Box<dyn Error>` drop glue.
#[inline]
pub(crate) fn io_err(kind: std::io::ErrorKind, msg: &'static str) -> std::io::Error {
    #[cfg(kani)]
    {
        let _ = msg;
        std::io::Error::from(kind)
    }
    #[cfg(not(kani))]
    {
        std::io::Error::new(kind, msg)
    }
}

/// Builds an [`std::io::Error`] formatted as `"{prefix}: {tag}"`, collapsing to zero-allocation
/// [`std::io::Error::from`] under Kani so CBMC does not symbolize `core::fmt` or `Box<dyn Error>`.
#[inline]
pub(crate) fn io_err_with_tag(
    kind: std::io::ErrorKind,
    prefix: &'static str,
    tag: u8,
) -> std::io::Error {
    #[cfg(kani)]
    {
        let _ = (prefix, tag);
        std::io::Error::from(kind)
    }
    #[cfg(not(kani))]
    {
        std::io::Error::new(kind, format!("{prefix}: {tag}"))
    }
}

/// 128-byte cache-line-aligned mutex wrapper for sharded concurrent tables,
/// poisoning-tolerant on lock acquisition.
#[repr(align(128))]
pub(crate) struct CachePaddedMutex<T>(std::sync::Mutex<T>);

impl<T> CachePaddedMutex<T> {
    #[inline]
    pub(crate) fn new(value: T) -> Self {
        Self(std::sync::Mutex::new(value))
    }

    #[inline]
    pub(crate) fn lock(&self) -> std::sync::MutexGuard<'_, T> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }
}
