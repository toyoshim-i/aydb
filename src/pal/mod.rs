//! Layer 1: Platform Abstraction Layer (PAL / VFS).
//!
//! Provides OS-native positional file I/O, pre-allocation strategies, cache management hints,
//! and single-host exclusive file handle management.

use std::io;
use std::path::Path;
use std::sync::Arc;

pub mod path;

/// Process-level lock file name created inside the database directory on Unix (`flock`).
pub const DB_LOCK_FILE_NAME: &str = "db.lock";

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
    /// For `blob_*_<SeqID>.db` (Random point reads & append writes without `FILE_FLAG_SEQUENTIAL_SCAN`).
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

    /// Reads multiple non-overlapping slices at specified offsets.
    ///
    /// Default implementation sequentially reads each slice via [`PlatformFile::read_exact_at`].
    /// Specialized implementations may coalesce nearby spans or issue concurrent requests.
    fn read_many_at(&self, requests: &mut [(u64, &mut [u8])]) -> io::Result<()> {
        for (offset, buf) in requests.iter_mut() {
            self.read_exact_at(buf, *offset)?;
        }
        Ok(())
    }

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

/// Writes 256 KB zero-filled chunks across `[current_len, target_len)` and syncs durably.
pub(crate) fn preallocate_physical_zero_fill<F: PlatformFile + ?Sized>(
    file: &F,
    current_len: u64,
    target_len: u64,
) -> io::Result<()> {
    if current_len >= target_len {
        return Ok(());
    }
    const CHUNK_SIZE: usize = 256 * 1024;
    let zero_buf = vec![0u8; CHUNK_SIZE.min((target_len - current_len) as usize)];
    let mut pos = current_len;
    while pos < target_len {
        let to_write = ((target_len - pos) as usize).min(zero_buf.len());
        file.write_all_at(&zero_buf[..to_write], pos)?;
        pos += to_write as u64;
    }
    file.sync_durable()
}

/// Coalesces adjacent or nearby non-overlapping positional read requests into larger contiguous
/// physical reads (`<= 64 KB` span with gaps `<= 16 KB`) to minimize kernel transitions.
pub(crate) fn coalesced_read_many_at<F: PlatformFile + ?Sized>(
    file: &F,
    requests: &mut [(u64, &mut [u8])],
) -> io::Result<()> {
    if requests.is_empty() {
        return Ok(());
    }
    if requests.len() == 1 {
        return file.read_exact_at(requests[0].1, requests[0].0);
    }

    let mut indices: Vec<usize> = (0..requests.len()).collect();
    indices.sort_unstable_by_key(|&i| requests[i].0);

    let mut i = 0;
    while i < indices.len() {
        let first_idx = indices[i];
        let cluster_start = requests[first_idx].0;
        let mut cluster_end = cluster_start.saturating_add(requests[first_idx].1.len() as u64);
        let mut j = i + 1;

        while j < indices.len() {
            let next_idx = indices[j];
            let next_start = requests[next_idx].0;
            let next_end = next_start.saturating_add(requests[next_idx].1.len() as u64);
            let gap = next_start.saturating_sub(cluster_end);
            let new_span = next_end.max(cluster_end).saturating_sub(cluster_start);

            if gap <= 16 * 1024 && new_span <= 64 * 1024 {
                cluster_end = cluster_end.max(next_end);
                j += 1;
            } else {
                break;
            }
        }

        if j == i + 1 {
            let idx = indices[i];
            let (off, buf) = &mut requests[idx];
            file.read_exact_at(buf, *off)?;
        } else {
            let span = (cluster_end - cluster_start) as usize;
            let mut scratch = vec![0u8; span];
            file.read_exact_at(&mut scratch, cluster_start)?;
            for &idx in &indices[i..j] {
                let (off, buf) = &mut requests[idx];
                let rel = (*off - cluster_start) as usize;
                buf.copy_from_slice(&scratch[rel..rel + buf.len()]);
            }
        }

        i = j;
    }

    Ok(())
}

/// Lists all regular files in `root` (excluding [`DB_LOCK_FILE_NAME`]), sorted lexicographically.
pub(crate) fn list_dir_files(root: &Path) -> io::Result<Vec<String>> {
    let mut names = Vec::new();
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        if entry.file_type()?.is_file() {
            if let Some(s) = entry.file_name().to_str() {
                if s != DB_LOCK_FILE_NAME {
                    names.push(s.to_owned());
                }
            }
        }
    }
    names.sort();
    Ok(names)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::test_util::TempTestDir;
    #[cfg(windows)]
    use std::path::PathBuf;
    use std::thread;

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
        assert!(
            fs.open_file("", true, true, OpenHint::MutableBufferedLog)
                .is_err()
        );
        assert!(
            fs.open_file("../escape.db", true, true, OpenHint::MutableBufferedLog)
                .is_err()
        );
        assert!(
            fs.open_file("sub/file.db", true, true, OpenHint::MutableBufferedLog)
                .is_err()
        );
        assert!(
            fs.open_file(r"sub\file.db", true, true, OpenHint::MutableBufferedLog)
                .is_err()
        );
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
        assert!(
            second.is_err(),
            "second open must fail due to dwShareMode = 0"
        );
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
                    f.write_all_at(&write_buf, offset)
                        .expect("concurrent write");

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

    #[test]
    fn read_many_at_coalesces_and_reads_correctly() {
        let tmp = TempTestDir::new("read_many");
        let fs = OsFs::open(&tmp.path, true).expect("open OsFs");

        let file = fs
            .open_file("test_data.db", true, true, OpenHint::ImmutableSequential)
            .expect("open file");

        // Write 64 KB of known pattern: byte at offset `i` is `(i % 251) as u8`
        let total_size = 64 * 1024usize;
        let mut data = vec![0u8; total_size];
        for (i, b) in data.iter_mut().enumerate() {
            *b = (i % 251) as u8;
        }
        file.write_all_at(&data, 0).expect("write data");

        // Request 1: 100 bytes at offset 100
        let mut buf1 = vec![0u8; 100];
        // Request 2: 200 bytes at offset 500
        let mut buf2 = vec![0u8; 200];
        // Request 3: 50 bytes at offset 1200
        let mut buf3 = vec![0u8; 50];
        // Request 4: 1000 bytes at offset 20000 (larger gap)
        let mut buf4 = vec![0u8; 1000];

        let mut reqs = [
            (100u64, buf1.as_mut_slice()),
            (500u64, buf2.as_mut_slice()),
            (1200u64, buf3.as_mut_slice()),
            (20000u64, buf4.as_mut_slice()),
        ];

        file.read_many_at(&mut reqs).expect("read_many_at");

        assert_eq!(buf1, &data[100..200]);
        assert_eq!(buf2, &data[500..700]);
        assert_eq!(buf3, &data[1200..1250]);
        assert_eq!(buf4, &data[20000..21000]);
    }
}
