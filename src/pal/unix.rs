//! Unix (Linux / macOS) Platform Abstraction Layer (`UnixPlatformFile` / `UnixPlatformFs`).

use core::ffi::c_int;
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::path::{normalize_db_root_path, validate_flat_file_name};
use super::{DB_LOCK_FILE_NAME, OpenHint, PlatformFile, PlatformFs, PreallocStrategy};

const LOCK_EX: c_int = 2;
const LOCK_NB: c_int = 4;

#[cfg(target_os = "macos")]
const F_FULLFSYNC: c_int = 51;

unsafe extern "C" {
    fn flock(fd: c_int, operation: c_int) -> c_int;
    #[cfg(target_os = "macos")]
    fn fcntl(fd: c_int, cmd: c_int, ...) -> c_int;
    #[cfg(target_os = "linux")]
    fn posix_fallocate(fd: c_int, offset: i64, len: i64) -> c_int;
}

/// Unix implementation of [`PlatformFile`].
#[derive(Debug)]
pub struct UnixPlatformFile {
    file: File,
    #[allow(dead_code)]
    hint: OpenHint,
}

impl UnixPlatformFile {
    pub(crate) fn open_internal(
        full_path: &Path,
        create: bool,
        writable: bool,
        truncate: bool,
        hint: OpenHint,
    ) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(writable)
            .create(create)
            .truncate(truncate)
            .open(full_path)?;
        Ok(Self { file, hint })
    }
}

impl PlatformFile for UnixPlatformFile {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        FileExt::read_at(&self.file, buf, offset)
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        FileExt::read_exact_at(&self.file, buf, offset)
    }

    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        FileExt::write_all_at(&self.file, buf, offset)
    }

    fn file_len(&self) -> io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    fn preallocate(&self, target_len: u64, strategy: PreallocStrategy) -> io::Result<()> {
        let current_len = self.file_len()?;
        if current_len < target_len {
            #[cfg(target_os = "linux")]
            {
                if let Ok(len_i64) = i64::try_from(target_len) {
                    let rc = unsafe { posix_fallocate(self.file.as_raw_fd(), 0, len_i64) };
                    if rc != 0 {
                        // Fall back to `set_len` if the underlying filesystem does not support fallocate
                        self.file.set_len(target_len)?;
                    }
                } else {
                    self.file.set_len(target_len)?;
                }
            }
            #[cfg(not(target_os = "linux"))]
            {
                self.file.set_len(target_len)?;
            }
        }
        if strategy == PreallocStrategy::PhysicalZeroFill && current_len < target_len {
            const CHUNK_SIZE: usize = 256 * 1024;
            let zero_buf = vec![0u8; CHUNK_SIZE.min((target_len - current_len) as usize)];
            let mut pos = current_len;
            while pos < target_len {
                let to_write = ((target_len - pos) as usize).min(zero_buf.len());
                self.write_all_at(&zero_buf[..to_write], pos)?;
                pos += to_write as u64;
            }
            self.sync_durable()?;
        }
        Ok(())
    }

    fn sync_barrier(&self) -> io::Result<()> {
        self.file.sync_data()
    }

    fn sync_durable(&self) -> io::Result<()> {
        #[cfg(target_os = "macos")]
        {
            let rc = unsafe { fcntl(self.file.as_raw_fd(), F_FULLFSYNC) };
            if rc != -1 {
                return Ok(());
            }
        }
        self.file.sync_all()
    }

    fn evict_os_cache(&self, _offset: u64, _len: u64) -> io::Result<()> {
        Ok(())
    }
}

/// Unix implementation of [`PlatformFs`].
#[derive(Debug, Clone)]
pub struct UnixPlatformFs {
    root: PathBuf,
    _lock_file: Arc<File>,
}

impl UnixPlatformFs {
    pub fn open(root: impl AsRef<Path>, create: bool) -> io::Result<Self> {
        let normalized = normalize_db_root_path(root.as_ref(), create)?;
        let lock_path = normalized.join(DB_LOCK_FILE_NAME);
        let lock_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)?;
        let rc = unsafe { flock(lock_file.as_raw_fd(), LOCK_EX | LOCK_NB) };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            root: normalized,
            _lock_file: Arc::new(lock_file),
        })
    }

    /// Flushes directory metadata to non-volatile storage so newly created or unlinked
    /// segment/manifest directory entries survive sudden power loss (`AUD-10`).
    pub fn sync_dir(&self) -> io::Result<()> {
        let dir = File::open(&self.root)?;
        dir.sync_all()
    }
}

impl PlatformFs for UnixPlatformFs {
    type File = UnixPlatformFile;

    fn open_file(
        &self,
        rel_name: &str,
        create: bool,
        writable: bool,
        hint: OpenHint,
    ) -> io::Result<Arc<Self::File>> {
        validate_flat_file_name(rel_name)?;
        let full_path = self.root.join(rel_name);
        let existed = full_path.exists();
        let file = UnixPlatformFile::open_internal(&full_path, create, writable, false, hint)?;
        if create && (!existed || hint == OpenHint::MutableSyncLog) {
            self.sync_dir()?;
        }
        Ok(Arc::new(file))
    }

    fn remove_file(&self, rel_name: &str) -> io::Result<()> {
        validate_flat_file_name(rel_name)?;
        let full_path = self.root.join(rel_name);
        match std::fs::remove_file(full_path) {
            Ok(()) => {
                self.sync_dir()?;
                Ok(())
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }

    fn list_files(&self) -> io::Result<Vec<String>> {
        let mut names = Vec::new();
        for entry in std::fs::read_dir(&self.root)? {
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

    fn root_path(&self) -> &Path {
        &self.root
    }
}
