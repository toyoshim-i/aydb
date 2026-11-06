//! Layer 1: Platform Abstraction Layer (PAL / VFS).
//!
//! Provides OS-native positional file I/O, pre-allocation strategies, cache management hints,
//! and single-host exclusive file handle management.

use std::io;
use std::path::Path;
use std::sync::Arc;

pub mod path;

#[cfg(windows)]
pub mod windows;
#[cfg(windows)]
pub use windows::{WinPlatformFile as OsFile, WinPlatformFs as OsFs};

#[cfg(unix)]
pub mod unix;
#[cfg(unix)]
pub use unix::{UnixPlatformFile as OsFile, UnixPlatformFs as OsFs};

/// Access pattern and durability hints passed to the OS kernel when opening a file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenHint {
    /// For `boot_meta.db` and `wal_sync.db` (Write-Through / Synchronous durability).
    MutableSyncLog,
    /// For `wal_buf.db` (Buffered sequential journal).
    MutableBufferedLog,
    /// For `idx_<SeqID>.db` (4 KB random block lookups).
    ImmutableRandom,
    /// For `blob_*_<SeqID>.db` (Sequential large payload reads & appends).
    ImmutableSequential,
    /// For background compaction and GC bulk scans.
    BackgroundBulk,
}

/// Strategy for pre-allocating fixed-size files (`boot_meta.db`, `wal_sync.db`, `wal_buf.db`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreallocStrategy {
    /// Write physical zeros across the entire file length so NTFS `ValidDataLength` (VDL)
    /// equals `EOF` immediately, eliminating first-epoch metadata journal stalls.
    PhysicalZeroFill,
    /// Extend logical `EOF` / cluster allocation only (`SetEndOfFile` / `posix_fallocate`),
    /// allowing `VDL` to advance lazily as sequential writes progress.
    LogicalExtendOnly,
}

/// Low-level, thread-safe positional file abstraction.
///
/// Implementations guarantee that concurrent [`PlatformFile::read_exact_at`] and
/// [`PlatformFile::write_all_at`] calls from multiple threads do not mutate shared
/// file-offset state and do not serialize on kernel synchronous-file-object locks
/// (`FO_SYNCHRONOUS_IO`).
pub trait PlatformFile: Send + Sync + 'static {
    /// Reads up to `buf.len()` bytes starting at `offset`, returning the number of bytes read.
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize>;

    /// Reads exactly `buf.len()` bytes starting at `offset`.
    /// Returns [`io::ErrorKind::UnexpectedEof`] if fewer bytes are available.
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()>;

    /// Writes all bytes in `buf` starting at `offset`.
    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()>;

    /// Returns the current logical length of the file in bytes.
    fn file_len(&self) -> io::Result<u64>;

    /// Pre-allocates the file to `target_len` bytes using the specified strategy.
    fn preallocate(&self, target_len: u64, strategy: PreallocStrategy) -> io::Result<()>;

    /// Issues an ordering barrier ensuring prior writes reach the storage controller
    /// before subsequent metadata updates.
    fn sync_barrier(&self) -> io::Result<()>;

    /// Flushes all modified data and metadata to non-volatile physical media.
    fn sync_durable(&self) -> io::Result<()>;

    /// Hints to the OS kernel that cached pages in `[offset, offset + len)` are no longer needed.
    fn evict_os_cache(&self, offset: u64, len: u64) -> io::Result<()>;
}

/// Manages the database directory, path normalization, exclusive locking, and open handle lifecycle.
pub trait PlatformFs: Send + Sync + 'static {
    type File: PlatformFile;

    /// Opens or creates a file inside the database directory with `dwShareMode = 0` on Windows.
    fn open_file(
        &self,
        rel_name: &str,
        create: bool,
        writable: bool,
        hint: OpenHint,
    ) -> io::Result<Arc<Self::File>>;

    /// Deletes a file after all handles to it have been dropped.
    fn remove_file(&self, rel_name: &str) -> io::Result<()>;

    /// Lists all file names currently present in the database directory.
    fn list_files(&self) -> io::Result<Vec<String>>;

    /// Returns the normalized root path (e.g., `\\?\...` on Windows).
    fn root_path(&self) -> &Path;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::thread;

    static TEST_COUNTER: AtomicU64 = AtomicU64::new(1);

    struct TempTestDir {
        path: PathBuf,
    }

    impl TempTestDir {
        fn new(prefix: &str) -> Self {
            let id = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
            let pid = std::process::id();
            let dir = std::env::temp_dir().join(format!("aydb_pal_test_{prefix}_{pid}_{id}"));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).expect("create temp test dir");
            Self { path: dir }
        }
    }

    impl Drop for TempTestDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    #[test]
    fn path_normalization_and_validation() {
        let tmp = TempTestDir::new("path_norm");
        let fs = OsFs::open(&tmp.path, true).expect("open OsFs");

        #[cfg(windows)]
        {
            let root_str = fs.root_path().to_string_lossy();
            assert!(
                root_str.starts_with(r"\\?\"),
                "expected extended-length prefix, got: {root_str}"
            );

            // Verify UNC path transformation: `\\server\share\dir` -> `\\?\UNC\server\share\dir`
            let unc = PathBuf::from(r"\\nano.local\share\aydb");
            let converted = path::ensure_extended_length_prefix(unc);
            assert_eq!(
                converted.to_string_lossy(),
                r"\\?\UNC\nano.local\share\aydb"
            );
        }

        // Reject path traversal and invalid names
        assert!(fs.open_file("", true, true, OpenHint::MutableBufferedLog).is_err());
        assert!(fs.open_file("../escape.db", true, true, OpenHint::MutableBufferedLog).is_err());
        assert!(fs.open_file("sub/file.db", true, true, OpenHint::MutableBufferedLog).is_err());
        assert!(fs.open_file(r"sub\file.db", true, true, OpenHint::MutableBufferedLog).is_err());
    }

    #[test]
    #[cfg(windows)]
    fn exclusive_handle_rejects_second_open_until_dropped() {
        let tmp = TempTestDir::new("exclusive");
        let fs = OsFs::open(&tmp.path, true).expect("open OsFs");

        let f1 = fs
            .open_file("wal_sync.db", true, true, OpenHint::MutableSyncLog)
            .expect("first open must succeed");

        // Attempting to open a second handle to the same file must fail with SHARING_VIOLATION (32)
        let second = fs.open_file("wal_sync.db", false, false, OpenHint::ImmutableRandom);
        assert!(second.is_err(), "second open must fail due to dwShareMode = 0");
        assert_eq!(second.unwrap_err().raw_os_error(), Some(32));

        // Dropping the first handle immediately releases the kernel lock, allowing re-open and delete.
        drop(f1);

        let f2 = fs
            .open_file("wal_sync.db", false, true, OpenHint::MutableSyncLog)
            .expect("reopen after drop must succeed");
        drop(f2);

        fs.remove_file("wal_sync.db")
            .expect("remove_file after drop must succeed");
        assert!(fs.list_files().unwrap().is_empty());
    }

    #[test]
    fn preallocation_strategies_and_positional_io() {
        let tmp = TempTestDir::new("prealloc");
        let fs = OsFs::open(&tmp.path, true).expect("open OsFs");

        for (name, hint, strategy, size) in [
            (
                "boot_meta.db",
                OpenHint::MutableSyncLog,
                PreallocStrategy::PhysicalZeroFill,
                8 * 1024u64,
            ),
            (
                "wal_buf.db",
                OpenHint::MutableBufferedLog,
                PreallocStrategy::LogicalExtendOnly,
                512 * 1024u64,
            ),
        ] {
            let file = fs.open_file(name, true, true, hint).expect("open file");
            file.preallocate(size, strategy).expect("preallocate");
            assert_eq!(file.file_len().expect("file_len"), size);

            // Verify zero-initialized read at the tail
            let mut tail = [0xFFu8; 4096];
            file.read_exact_at(&mut tail, size - 4096)
                .expect("read tail");
            assert!(tail.iter().all(|&b| b == 0));

            // Write pattern at middle offset and read back
            let pattern = [0xA5u8; 4096];
            file.write_all_at(&pattern, 4096).expect("write pattern");
            file.sync_barrier().expect("sync_barrier");

            let mut read_back = [0u8; 4096];
            file.read_exact_at(&mut read_back, 4096)
                .expect("read pattern");
            assert_eq!(read_back, pattern);

            // Reading past EOF returns UnexpectedEof
            let mut past_eof = [0u8; 16];
            let err = file.read_exact_at(&mut past_eof, size).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
        }
    }

    #[test]
    fn multithreaded_concurrent_positional_read_write() {
        let tmp = TempTestDir::new("mt_rw");
        let fs = OsFs::open(&tmp.path, true).expect("open OsFs");

        const NUM_THREADS: usize = 8;
        const BLOCKS_PER_THREAD: usize = 32;
        const BLOCK_SIZE: usize = 4096;
        let total_size = (NUM_THREADS * BLOCKS_PER_THREAD * BLOCK_SIZE) as u64;

        let file = fs
            .open_file("idx_0001.db", true, true, OpenHint::ImmutableRandom)
            .expect("open idx_0001.db");
        file.preallocate(total_size, PreallocStrategy::PhysicalZeroFill)
            .expect("preallocate");

        // Spawn 8 threads writing and reading disjoint 4 KB blocks concurrently through a single handle
        let mut handles = Vec::new();
        for tid in 0..NUM_THREADS {
            let f = Arc::clone(&file);
            handles.push(thread::spawn(move || {
                for b in 0..BLOCKS_PER_THREAD {
                    let block_idx = tid * BLOCKS_PER_THREAD + b;
                    let offset = (block_idx * BLOCK_SIZE) as u64;
                    let tag = ((tid as u8) << 4) | ((b as u8) & 0x0F);
                    let write_buf = [tag; BLOCK_SIZE];
                    f.write_all_at(&write_buf, offset).expect("concurrent write");

                    let mut read_buf = [0u8; BLOCK_SIZE];
                    f.read_exact_at(&mut read_buf, offset)
                        .expect("concurrent read");
                    assert_eq!(read_buf, write_buf);
                }
            }));
        }

        for h in handles {
            h.join().expect("thread join");
        }
    }
}
