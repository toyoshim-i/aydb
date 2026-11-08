//! Unix (Linux / macOS) Platform Abstraction Layer (`UnixPlatformFile` / `UnixPlatformFs`).

use core::ffi::c_int;
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock, Weak};

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

/// Default maximum open file descriptors across idle segment handles in [`HandlePool`].
pub const DEFAULT_HANDLE_POOL_CAP: usize = 128;

/// Reference-aware descriptor pool for POSIX systems (e.g. macOS `RLIMIT_NOFILE = 256`).
/// Bounds simultaneously open descriptors across idle segment files while never closing
/// handles actively pinned by queries, cursors, or in-flight I/O.
pub struct HandlePool {
    max_descriptors: usize,
    inner: Mutex<HandlePoolInner>,
}

impl core::fmt::Debug for HandlePool {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("HandlePool")
            .field("max_descriptors", &self.max_descriptors)
            .finish()
    }
}

struct HandlePoolInner {
    tracked: Vec<TrackedHandle>,
}

struct TrackedHandle {
    _path: PathBuf,
    file: Weak<UnixPlatformFile>,
}

impl HandlePool {
    pub fn new(max_descriptors: usize) -> Self {
        Self {
            max_descriptors,
            inner: Mutex::new(HandlePoolInner {
                tracked: Vec::new(),
            }),
        }
    }

    pub fn register(&self, path: &Path, file: &Arc<UnixPlatformFile>) {
        let mut inner = self.inner.lock().unwrap();
        self.evict_if_needed(&mut inner);
        inner.tracked.push(TrackedHandle {
            _path: path.to_path_buf(),
            file: Arc::downgrade(file),
        });
    }

    pub fn ensure_capacity_before_open(&self) {
        let mut inner = self.inner.lock().unwrap();
        self.evict_if_needed(&mut inner);
    }

    fn evict_if_needed(&self, inner: &mut HandlePoolInner) {
        inner.tracked.retain(|th| th.file.upgrade().is_some());

        if inner.tracked.len() < self.max_descriptors {
            return;
        }

        // Find the oldest unpinned handle (strong_count <= 2: 1 for engine/pool, 1 for local upgraded Arc)
        let mut evict_idx = None;
        for (idx, th) in inner.tracked.iter().enumerate() {
            if let Some(arc) = th.file.upgrade() {
                if Arc::strong_count(&arc) <= 2 {
                    if let Ok(mut guard) = arc.file.write() {
                        if guard.is_some() {
                            *guard = None;
                            evict_idx = Some(idx);
                            break;
                        }
                    }
                }
            }
        }

        if let Some(idx) = evict_idx {
            inner.tracked.remove(idx);
        }
    }
}

/// Unix implementation of [`PlatformFile`].
pub struct UnixPlatformFile {
    path: PathBuf,
    writable: bool,
    hint: OpenHint,
    file: RwLock<Option<File>>,
    pool: Option<Arc<HandlePool>>,
}

impl core::fmt::Debug for UnixPlatformFile {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("UnixPlatformFile")
            .field("path", &self.path)
            .field("writable", &self.writable)
            .field("hint", &self.hint)
            .finish()
    }
}

impl UnixPlatformFile {
    pub(crate) fn open_internal(
        full_path: &Path,
        create: bool,
        writable: bool,
        truncate: bool,
        hint: OpenHint,
        pool: Option<Arc<HandlePool>>,
    ) -> io::Result<Self> {
        if let Some(ref p) = pool {
            p.ensure_capacity_before_open();
        }
        let file = OpenOptions::new()
            .read(true)
            .write(writable)
            .create(create)
            .truncate(truncate)
            .open(full_path)?;
        Ok(Self {
            path: full_path.to_path_buf(),
            writable,
            hint,
            file: RwLock::new(Some(file)),
            pool,
        })
    }

    pub(crate) fn with_file<R>(&self, f: impl FnOnce(&File) -> io::Result<R>) -> io::Result<R> {
        {
            let guard = self.file.read().unwrap();
            if let Some(ref file) = *guard {
                return f(file);
            }
        }
        let mut guard = self.file.write().unwrap();
        if let Some(ref file) = *guard {
            return f(file);
        }
        if let Some(ref pool) = self.pool {
            pool.ensure_capacity_before_open();
        }
        let reopened = OpenOptions::new()
            .read(true)
            .write(self.writable)
            .open(&self.path)?;
        let res = f(&reopened);
        *guard = Some(reopened);
        res
    }

    /// Whether the underlying file descriptor is currently open in memory.
    pub fn is_descriptor_open(&self) -> bool {
        self.file.read().unwrap().is_some()
    }
}

impl PlatformFile for UnixPlatformFile {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        self.with_file(|f| FileExt::read_at(f, buf, offset))
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        self.with_file(|f| FileExt::read_exact_at(f, buf, offset))
    }

    fn read_many_at(&self, requests: &mut [(u64, &mut [u8])]) -> io::Result<()> {
        super::coalesced_read_many_at(self, requests)
    }

    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        self.with_file(|f| FileExt::write_all_at(f, buf, offset))
    }

    fn file_len(&self) -> io::Result<u64> {
        self.with_file(|f| Ok(f.metadata()?.len()))
    }

    fn preallocate(&self, target_len: u64, strategy: PreallocStrategy) -> io::Result<()> {
        let current_len = self.file_len()?;
        if current_len < target_len {
            #[cfg(target_os = "linux")]
            {
                if let Ok(len_i64) = i64::try_from(target_len) {
                    let rc = self.with_file(|f| Ok(unsafe { posix_fallocate(f.as_raw_fd(), 0, len_i64) }))?;
                    if rc != 0 {
                        self.with_file(|f| f.set_len(target_len))?;
                    }
                } else {
                    self.with_file(|f| f.set_len(target_len))?;
                }
            }
            #[cfg(not(target_os = "linux"))]
            {
                self.with_file(|f| f.set_len(target_len))?;
            }
        }
        if strategy == PreallocStrategy::PhysicalZeroFill && current_len < target_len {
            super::preallocate_physical_zero_fill(self, current_len, target_len)?;
        }
        Ok(())
    }

    fn sync_barrier(&self) -> io::Result<()> {
        #[cfg(target_os = "macos")]
        {
            if self.hint == OpenHint::MutableSyncLog {
                return self.sync_durable();
            }
        }
        self.with_file(|f| f.sync_data())
    }

    fn sync_durable(&self) -> io::Result<()> {
        #[cfg(target_os = "macos")]
        {
            let rc = self.with_file(|f| Ok(unsafe { fcntl(f.as_raw_fd(), F_FULLFSYNC) }))?;
            if rc != -1 {
                return Ok(());
            }
        }
        self.with_file(|f| f.sync_all())
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
    pool: Arc<HandlePool>,
}

impl UnixPlatformFs {
    pub fn open(root: impl AsRef<Path>, create: bool) -> io::Result<Self> {
        Self::open_with_pool_cap(root, create, DEFAULT_HANDLE_POOL_CAP)
    }

    pub fn open_with_pool_cap(
        root: impl AsRef<Path>,
        create: bool,
        pool_cap: usize,
    ) -> io::Result<Self> {
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
            pool: Arc::new(HandlePool::new(pool_cap)),
        })
    }

    /// Flushes directory metadata to non-volatile storage so newly created or unlinked
    /// segment/manifest directory entries survive sudden power loss.
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
        let use_pool = match hint {
            OpenHint::MutableSyncLog | OpenHint::MutableBufferedLog => false,
            OpenHint::ImmutableSequential
            | OpenHint::ImmutableRandom
            | OpenHint::BackgroundBulk => true,
        };
        let pool_ref = if use_pool {
            Some(Arc::clone(&self.pool))
        } else {
            None
        };
        let file = Arc::new(UnixPlatformFile::open_internal(
            &full_path,
            create,
            writable,
            false,
            hint,
            pool_ref,
        )?);
        if use_pool {
            self.pool.register(&full_path, &file);
        }
        if create && (!existed || hint == OpenHint::MutableSyncLog) {
            self.sync_dir()?;
        }
        Ok(file)
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
        super::list_dir_files(&self.root)
    }

    fn root_path(&self) -> &Path {
        &self.root
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::test_util::TempTestDir;

    #[test]
    fn handle_pool_caps_descriptors_and_reopens_on_demand() {
        let tmp = TempTestDir::new("handle_pool");
        let fs = UnixPlatformFs::open_with_pool_cap(&tmp.path, true, 2).expect("open fs");

        let f1 = fs
            .open_file("idx_0001.db", true, true, OpenHint::ImmutableRandom)
            .unwrap();
        f1.write_all_at(b"hello", 0).unwrap();

        let f2 = fs
            .open_file("idx_0002.db", true, true, OpenHint::ImmutableRandom)
            .unwrap();
        f2.write_all_at(b"world", 0).unwrap();

        assert!(f1.is_descriptor_open());
        assert!(f2.is_descriptor_open());

        // Opening a 3rd pooled file exceeds cap 2 -> f1 is idle (unpinned) so its fd is evicted
        let f3 = fs
            .open_file("idx_0003.db", true, true, OpenHint::ImmutableRandom)
            .unwrap();
        f3.write_all_at(b"aydb!", 0).unwrap();

        assert!(!f1.is_descriptor_open(), "f1 descriptor should have been evicted");
        assert!(f2.is_descriptor_open());
        assert!(f3.is_descriptor_open());

        // Reading from f1 transparently reopens its descriptor
        let mut buf = [0u8; 5];
        f1.read_exact_at(&mut buf, 0).unwrap();
        assert_eq!(&buf, b"hello");
        assert!(f1.is_descriptor_open(), "f1 should be reopened on access");

        // Pinning test: pin f2 by holding another Arc reference
        let f2_pinned = Arc::clone(&f2);
        // Open f4: f2 is pinned (strong_count > 2), so f3 or f1 should be evicted instead!
        let f4 = fs
            .open_file("idx_0004.db", true, true, OpenHint::ImmutableRandom)
            .unwrap();
        f4.write_all_at(b"more!", 0).unwrap();

        assert!(f2.is_descriptor_open(), "f2 was pinned and must not be evicted");
        drop(f2_pinned);
    }
}
