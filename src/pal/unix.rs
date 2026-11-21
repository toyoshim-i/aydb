//! Unix (Linux / macOS) Platform Abstraction Layer (`UnixPlatformFile` / `UnixPlatformFs`).

use core::ffi::c_int;
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};

use super::path::{normalize_db_root_path, validate_flat_file_name};
use super::{DB_LOCK_FILE_NAME, OpenHint, PlatformFile, PlatformFs, PreallocStrategy};

const LOCK_EX: c_int = 2;
const LOCK_NB: c_int = 4;

#[cfg(target_os = "macos")]
const F_FULLFSYNC: c_int = 51;
#[cfg(target_os = "macos")]
const F_RDAHEAD: c_int = 45;
#[cfg(target_os = "macos")]
const F_NOCACHE: c_int = 48;

#[cfg(target_os = "linux")]
const POSIX_FADV_RANDOM: c_int = 1;
#[cfg(target_os = "linux")]
const POSIX_FADV_SEQUENTIAL: c_int = 2;
#[cfg(target_os = "linux")]
const POSIX_FADV_NOREUSE: c_int = 5;

unsafe extern "C" {
    fn flock(fd: c_int, operation: c_int) -> c_int;
    #[cfg(target_os = "macos")]
    fn fcntl(fd: c_int, cmd: c_int, ...) -> c_int;
    #[cfg(target_os = "macos")]
    fn fsync(fd: c_int) -> c_int;
    #[cfg(target_os = "linux")]
    #[cfg_attr(
        all(target_pointer_width = "32", target_env = "gnu"),
        link_name = "posix_fallocate64"
    )]
    fn posix_fallocate(fd: c_int, offset: i64, len: i64) -> c_int;
    #[cfg(target_os = "linux")]
    #[cfg_attr(
        all(target_pointer_width = "32", target_env = "gnu"),
        link_name = "posix_fadvise64"
    )]
    fn posix_fadvise(fd: c_int, offset: i64, len: i64, advice: c_int) -> c_int;
}

/// macOS `ENOTTY` ("inappropriate ioctl for device").
#[cfg(any(target_os = "macos", test))]
const MACOS_ENOTTY: i32 = 25;
/// macOS `ENOTSUP` ("operation not supported").
#[cfg(any(target_os = "macos", test))]
const MACOS_ENOTSUP: i32 = 45;
/// macOS `EOPNOTSUPP` ("operation not supported on socket", distinct from `ENOTSUP` on Darwin).
#[cfg(any(target_os = "macos", test))]
const MACOS_EOPNOTSUPP: i32 = 102;

/// Decides whether a failed `fcntl(F_FULLFSYNC)` means "this filesystem cannot do it" (so a plain
/// `fsync` is the best available) rather than a real flush failure. Only `ENOTSUP`, `EOPNOTSUPP`
/// (SMB and some other network or FUSE mounts; the case Go's `File.Sync` handles), and `ENOTTY`
/// (the filesystem has no ioctl handler; seen with OpenJDK and Bitcoin Core) qualify. Any other
/// errno, notably `EIO`, must be returned: after a failed flush the kernel may drop the dirty
/// pages, and a later `fsync` would then report success for data that never reached the disk.
#[cfg(any(target_os = "macos", test))]
fn full_fsync_unsupported(errno: i32) -> bool {
    matches!(errno, MACOS_ENOTSUP | MACOS_EOPNOTSUPP | MACOS_ENOTTY)
}

/// Runs a full-sync primitive (`fcntl(F_FULLFSYNC)`) with `EINTR` retries, falling back to a
/// plain `fsync` (also retrying `EINTR`) only when the error code indicates the filesystem
/// does not support `F_FULLFSYNC` (`ENOTSUP`, `EOPNOTSUPP`, `ENOTTY`).
#[cfg(any(target_os = "macos", test))]
fn sync_full_or_fallback(
    mut full: impl FnMut() -> io::Result<()>,
    mut fallback: impl FnMut() -> io::Result<()>,
) -> io::Result<()> {
    loop {
        match full() {
            Ok(()) => return Ok(()),
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) if full_fsync_unsupported(e.raw_os_error().unwrap_or(0)) => break,
            Err(e) => return Err(e),
        }
    }
    loop {
        match fallback() {
            Ok(()) => return Ok(()),
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
}

#[cfg(target_os = "macos")]
fn macos_sync_file_durable(file: &File) -> io::Result<()> {
    let fd = file.as_raw_fd();
    sync_full_or_fallback(
        || {
            if unsafe { fcntl(fd, F_FULLFSYNC) } == -1 {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        },
        || {
            if unsafe { fsync(fd) } == -1 {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        },
    )
}

/// Calls `posix_fallocate` (retrying `EINTR`), falling back to `ftruncate` (`set_len`) only when
/// the filesystem/kernel reports `EOPNOTSUPP` / `EINVAL`. Real allocation or hardware errors
/// (`ENOSPC`, `EDQUOT`, `EIO`) are propagated immediately.
#[cfg(any(target_os = "linux", test))]
fn fallocate_or_fallback(
    mut falloc: impl FnMut() -> c_int,
    mut fallback: impl FnMut() -> io::Result<()>,
) -> io::Result<()> {
    loop {
        let rc = falloc();
        if rc == 0 {
            return Ok(());
        }
        let err = io::Error::from_raw_os_error(rc);
        match err.kind() {
            io::ErrorKind::Interrupted => continue,
            io::ErrorKind::Unsupported | io::ErrorKind::InvalidInput => return fallback(),
            _ => return Err(err),
        }
    }
}

/// Tells the kernel how the file will be accessed. Advisory, so a failure is ignored. Blob files
/// (`ImmutableSequential`) get no hint because they serve random point reads, as on Windows.
#[cfg(target_os = "linux")]
fn apply_open_hint(file: &File, hint: OpenHint) {
    let advice = match hint {
        OpenHint::MutableBufferedLog => POSIX_FADV_SEQUENTIAL,
        OpenHint::ImmutableRandom => POSIX_FADV_RANDOM,
        OpenHint::BackgroundBulk => POSIX_FADV_NOREUSE,
        OpenHint::MutableSyncLog | OpenHint::ImmutableSequential => return,
    };
    unsafe {
        posix_fadvise(file.as_raw_fd(), 0, 0, advice);
    }
}

/// Tells the kernel how the file will be accessed. Advisory, so a failure is ignored. Blob files
/// (`ImmutableSequential`) get no hint because they serve random point reads, as on Windows.
#[cfg(target_os = "macos")]
fn apply_open_hint(file: &File, hint: OpenHint) {
    let (command, value) = match hint {
        OpenHint::MutableBufferedLog => (F_RDAHEAD, 1),
        OpenHint::ImmutableRandom => (F_RDAHEAD, 0),
        OpenHint::BackgroundBulk => (F_NOCACHE, 1),
        OpenHint::MutableSyncLog | OpenHint::ImmutableSequential => return,
    };
    unsafe {
        fcntl(file.as_raw_fd(), command, value as c_int);
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn apply_open_hint(_file: &File, _hint: OpenHint) {}

/// Default maximum open file descriptors across idle segment handles in [`HandlePool`].
pub const DEFAULT_HANDLE_POOL_CAP: usize = 128;

/// Reference-aware descriptor pool for POSIX systems (e.g. macOS `RLIMIT_NOFILE = 256`).
/// Bounds simultaneously open descriptors across idle segment files while never closing
/// handles actively pinned by queries, cursors, or in-flight I/O.
///
/// The pool tracks every live pooled handle, open or closed, so a handle that is reopened after
/// an eviction counts against the cap again. Eviction only uses `try_write` on a handle, so the
/// pool mutex never waits for in-flight I/O or for a handle that is being reopened (the reopening
/// thread holds that handle's write lock while it calls into the pool).
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
    clock_hand: usize,
}

struct TrackedHandle {
    path: PathBuf,
    file: Weak<UnixPlatformFile>,
}

impl HandlePool {
    pub fn new(max_descriptors: usize) -> Self {
        Self {
            max_descriptors,
            inner: Mutex::new(HandlePoolInner {
                tracked: Vec::new(),
                clock_hand: 0,
            }),
        }
    }

    pub fn register(&self, path: &Path, file: &Arc<UnixPlatformFile>) {
        let mut inner = self.inner.lock().unwrap();
        inner.tracked.retain(|th| th.file.strong_count() > 0);
        if inner.tracked.is_empty() {
            inner.clock_hand = 0;
        } else {
            inner.clock_hand %= inner.tracked.len();
        }
        inner.tracked.push(TrackedHandle {
            path: path.to_path_buf(),
            file: Arc::downgrade(file),
        });
    }

    pub fn ensure_capacity_before_open(&self) {
        let mut inner = self.inner.lock().unwrap();
        self.evict_if_needed(&mut inner);
    }

    fn evict_if_needed(&self, inner: &mut HandlePoolInner) {
        inner.tracked.retain(|th| th.file.strong_count() > 0);
        let len = inner.tracked.len();
        if len == 0 {
            inner.clock_hand = 0;
            return;
        }
        inner.clock_hand %= len;

        let mut open_count = inner
            .tracked
            .iter()
            .filter_map(|th| th.file.upgrade())
            .filter(|f| f.descriptor_open.load(Ordering::Acquire))
            .count();
        if open_count < self.max_descriptors {
            return;
        }

        // CLOCK sweep across at most two full passes: the first clears `referenced` for recently
        // used handles and the second evicts unpinned, clean idle handles until
        // `open_count < self.max_descriptors` so temporarily elevated counts drain back to the
        // cap. Re-check all guards after taking the handle's write lock to eliminate the TOCTOU
        // window with `pin_open_before_unlink`.
        for _ in 0..len.saturating_mul(2) {
            if open_count < self.max_descriptors {
                break;
            }
            let idx = inner.clock_hand;
            inner.clock_hand = (idx + 1) % len;

            let Some(arc) = inner.tracked[idx].file.upgrade() else {
                continue;
            };
            if !arc.descriptor_open.load(Ordering::Acquire)
                || arc.keep_open.load(Ordering::Acquire)
                || arc.has_unsynced_writes()
                || Arc::strong_count(&arc) > 2
            {
                continue;
            }
            if arc.referenced.swap(false, Ordering::AcqRel) {
                continue;
            }
            if let Ok(mut guard) = arc.file.try_write() {
                if arc.keep_open.load(Ordering::Acquire)
                    || arc.has_unsynced_writes()
                    || Arc::strong_count(&arc) > 2
                {
                    continue;
                }
                if guard.is_some() {
                    *guard = None;
                    arc.descriptor_open.store(false, Ordering::Release);
                    open_count = open_count.saturating_sub(1);
                }
            }
        }
    }

    /// Makes every handle to `path` that is still referenced hold an open descriptor and exempts
    /// it from eviction, so the file stays readable through that descriptor after it is unlinked.
    fn pin_open_before_unlink(&self, path: &Path) -> io::Result<()> {
        let handles: Vec<Arc<UnixPlatformFile>> = {
            let inner = self.inner.lock().unwrap();
            inner
                .tracked
                .iter()
                .filter(|th| th.path == path)
                .filter_map(|th| th.file.upgrade())
                .filter(|arc| Arc::strong_count(arc) > 1)
                .collect()
        };
        // The pool mutex is released: reopening may wait for in-flight I/O on the handle.
        for arc in handles {
            arc.keep_open.store(true, Ordering::Release);
            match arc.with_file(|_| Ok(())) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
}

/// Unix implementation of [`PlatformFile`].
pub struct UnixPlatformFile {
    path: PathBuf,
    writable: bool,
    hint: OpenHint,
    file: RwLock<Option<File>>,
    /// Mirrors `file.is_some()` so the pool can count open descriptors without taking `file`.
    descriptor_open: AtomicBool,
    /// CLOCK second-chance reference bit set on every descriptor access.
    referenced: AtomicBool,
    /// Set once the file is about to be unlinked while still referenced: the descriptor must
    /// stay open because the path can no longer be reopened.
    keep_open: AtomicBool,
    /// Number of writes/preallocations currently executing against this handle.
    active_writers: AtomicUsize,
    /// Monotonic counter incremented before each write/preallocation so dirty handles with
    /// unsynced writes are never evicted (preserving Linux `errseq_t` writeback errors).
    write_seq: AtomicU64,
    synced_seq: AtomicU64,
    pool: Option<Arc<HandlePool>>,
}

struct ActiveWriteGuard<'a>(&'a AtomicUsize);

impl Drop for ActiveWriteGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
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
        apply_open_hint(&file, hint);
        Ok(Self {
            path: full_path.to_path_buf(),
            writable,
            hint,
            file: RwLock::new(Some(file)),
            descriptor_open: AtomicBool::new(true),
            referenced: AtomicBool::new(true),
            keep_open: AtomicBool::new(false),
            active_writers: AtomicUsize::new(0),
            write_seq: AtomicU64::new(0),
            synced_seq: AtomicU64::new(0),
            pool,
        })
    }

    #[inline]
    fn has_unsynced_writes(&self) -> bool {
        self.active_writers.load(Ordering::Acquire) > 0
            || self.write_seq.load(Ordering::Acquire) != self.synced_seq.load(Ordering::Acquire)
    }

    #[inline]
    fn mark_write_start(&self) -> ActiveWriteGuard<'_> {
        self.active_writers.fetch_add(1, Ordering::AcqRel);
        self.write_seq.fetch_add(1, Ordering::AcqRel);
        ActiveWriteGuard(&self.active_writers)
    }

    fn sync_with(&self, f: impl FnOnce(&File) -> io::Result<()>) -> io::Result<()> {
        let seq = self.write_seq.load(Ordering::Acquire);
        let active_before = self.active_writers.load(Ordering::Acquire);
        self.with_file(f)?;
        if active_before == 0
            && self.active_writers.load(Ordering::Acquire) == 0
            && self.write_seq.load(Ordering::Acquire) == seq
        {
            self.synced_seq.store(seq, Ordering::Release);
        }
        Ok(())
    }

    pub(crate) fn with_file<R>(&self, f: impl FnOnce(&File) -> io::Result<R>) -> io::Result<R> {
        {
            let guard = self.file.read().unwrap();
            if let Some(ref file) = *guard {
                self.referenced.store(true, Ordering::Relaxed);
                return f(file);
            }
        }
        let mut guard = self.file.write().unwrap();
        if let Some(ref file) = *guard {
            self.referenced.store(true, Ordering::Relaxed);
            return f(file);
        }
        if let Some(ref pool) = self.pool {
            pool.ensure_capacity_before_open();
        }
        let reopened = OpenOptions::new()
            .read(true)
            .write(self.writable)
            .open(&self.path)?;
        apply_open_hint(&reopened, self.hint);
        *guard = Some(reopened);
        self.descriptor_open.store(true, Ordering::Release);
        self.referenced.store(true, Ordering::Relaxed);
        f(guard.as_ref().unwrap())
    }

    /// Whether the underlying file descriptor is currently open in memory.
    pub fn is_descriptor_open(&self) -> bool {
        self.descriptor_open.load(Ordering::Acquire)
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
        let _guard = self.mark_write_start();
        self.with_file(|f| FileExt::write_all_at(f, buf, offset))
    }

    fn file_len(&self) -> io::Result<u64> {
        self.with_file(|f| Ok(f.metadata()?.len()))
    }

    fn preallocate(&self, target_len: u64, strategy: PreallocStrategy) -> io::Result<()> {
        let current_len = self.file_len()?;
        if current_len >= target_len {
            return Ok(());
        }

        match strategy {
            PreallocStrategy::LogicalExtendOnly => {
                let _guard = self.mark_write_start();
                let extend_len = target_len - current_len;
                #[cfg(target_os = "linux")]
                {
                    if let (Ok(off_i64), Ok(len_i64)) =
                        (i64::try_from(current_len), i64::try_from(extend_len))
                    {
                        self.with_file(|f| {
                            fallocate_or_fallback(
                                || unsafe { posix_fallocate(f.as_raw_fd(), off_i64, len_i64) },
                                || f.set_len(target_len),
                            )
                        })?;
                    } else {
                        self.with_file(|f| f.set_len(target_len))?;
                    }
                }
                #[cfg(not(target_os = "linux"))]
                {
                    self.with_file(|f| f.set_len(target_len))?;
                }
            }
            PreallocStrategy::PhysicalZeroFill => {
                super::preallocate_physical_zero_fill_with_rollback(
                    self,
                    current_len,
                    target_len,
                    |len| {
                        let _guard = self.mark_write_start();
                        let _ = self.with_file(|f| f.set_len(len));
                    },
                )?;
            }
        }
        Ok(())
    }

    fn sync_barrier(&self) -> io::Result<()> {
        #[cfg(target_os = "macos")]
        {
            // Rust std's `File::sync_data` calls `fcntl(F_FULLFSYNC)` on macOS without falling
            // back to `fsync`, which fails on SMB/FUSE mounts. Route all macOS barriers through
            // `sync_durable` so `EINTR` retries and `ENOTSUP`/`EOPNOTSUPP`/`ENOTTY` fallback apply.
            self.sync_durable()
        }
        #[cfg(not(target_os = "macos"))]
        {
            self.sync_with(|f| f.sync_data())
        }
    }

    fn sync_durable(&self) -> io::Result<()> {
        #[cfg(target_os = "macos")]
        {
            self.sync_with(macos_sync_file_durable)
        }
        #[cfg(not(target_os = "macos"))]
        {
            self.sync_with(|f| f.sync_all())
        }
    }

    fn evict_os_cache(&self, _offset: u64, _len: u64) -> io::Result<()> {
        Ok(())
    }
}

/// Unix implementation of [`PlatformFs`].
#[derive(Debug, Clone)]
pub struct UnixPlatformFs {
    root: PathBuf,
    dir_file: Arc<File>,
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
        let dir_file = File::open(&normalized)?;
        let lock_path = normalized.join(DB_LOCK_FILE_NAME);
        let lock_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)?;
        loop {
            let rc = unsafe { flock(lock_file.as_raw_fd(), LOCK_EX | LOCK_NB) };
            if rc == 0 {
                break;
            }
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(err);
        }
        Ok(Self {
            root: normalized,
            dir_file: Arc::new(dir_file),
            _lock_file: Arc::new(lock_file),
            pool: Arc::new(HandlePool::new(pool_cap)),
        })
    }

    /// Flushes directory metadata to non-volatile storage so newly created or unlinked
    /// segment/manifest directory entries survive sudden power loss.
    pub fn sync_dir(&self) -> io::Result<()> {
        #[cfg(target_os = "macos")]
        {
            macos_sync_file_durable(&self.dir_file)
        }
        #[cfg(not(target_os = "macos"))]
        {
            self.dir_file.sync_all()
        }
    }
    fn remove_file_raw(&self, rel_name: &str) -> io::Result<bool> {
        validate_flat_file_name(rel_name)?;
        let full_path = self.root.join(rel_name);
        self.pool.pin_open_before_unlink(&full_path)?;
        match std::fs::remove_file(full_path) {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e),
        }
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
        let existed = if create
            && !matches!(
                hint,
                OpenHint::MutableSyncLog | OpenHint::MutableBufferedLog
            ) {
            full_path.exists()
        } else {
            true
        };
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
            &full_path, create, writable, false, hint, pool_ref,
        )?);
        if use_pool {
            self.pool.register(&full_path, &file);
        }
        if create
            && (!existed
                || matches!(
                    hint,
                    OpenHint::MutableSyncLog | OpenHint::MutableBufferedLog
                ))
        {
            self.sync_dir()?;
        }
        Ok(file)
    }

    fn remove_file(&self, rel_name: &str) -> io::Result<()> {
        if self.remove_file_raw(rel_name)? {
            UnixPlatformFs::sync_dir(self)?;
        }
        Ok(())
    }

    fn remove_file_unsynced(&self, rel_name: &str) -> io::Result<()> {
        self.remove_file_raw(rel_name).map(|_| ())
    }

    fn sync_dir(&self) -> io::Result<()> {
        UnixPlatformFs::sync_dir(self)
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
    fn full_fsync_falls_back_only_for_unsupported_filesystems() {
        assert!(full_fsync_unsupported(MACOS_ENOTSUP));
        assert!(full_fsync_unsupported(MACOS_EOPNOTSUPP));
        assert!(full_fsync_unsupported(MACOS_ENOTTY));
        for errno in [5, 9, 22, 28, 0] {
            assert!(!full_fsync_unsupported(errno), "errno {errno}");
        }
    }

    #[test]
    fn sync_full_or_fallback_retries_eintr_and_falls_back_only_when_unsupported() {
        // 1. EINTR on F_FULLFSYNC retries and succeeds without calling fallback.
        let mut full_calls = 0usize;
        let mut fallback_calls = 0usize;
        sync_full_or_fallback(
            || {
                full_calls += 1;
                if full_calls == 1 {
                    Err(io::Error::from(io::ErrorKind::Interrupted))
                } else {
                    Ok(())
                }
            },
            || {
                fallback_calls += 1;
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(full_calls, 2);
        assert_eq!(fallback_calls, 0);

        // 2. ENOTSUP, EOPNOTSUPP, and ENOTTY trigger fallback `fsync`, which also retries EINTR.
        for errno in [MACOS_ENOTSUP, MACOS_EOPNOTSUPP, MACOS_ENOTTY] {
            let mut full_attempts = 0usize;
            let mut fb_attempts = 0usize;
            sync_full_or_fallback(
                || {
                    full_attempts += 1;
                    Err(io::Error::from_raw_os_error(errno))
                },
                || {
                    fb_attempts += 1;
                    if fb_attempts == 1 {
                        Err(io::Error::from(io::ErrorKind::Interrupted))
                    } else {
                        Ok(())
                    }
                },
            )
            .unwrap();
            assert_eq!(full_attempts, 1, "errno {errno}");
            assert_eq!(fb_attempts, 2, "errno {errno}");
        }

        // 3. Hardware I/O error (EIO=5) on full must fail without calling fallback.
        let mut fb_after_eio = false;
        let err = sync_full_or_fallback(
            || Err(io::Error::from_raw_os_error(5)),
            || {
                fb_after_eio = true;
                Ok(())
            },
        )
        .unwrap_err();
        assert_eq!(err.raw_os_error(), Some(5));
        assert!(!fb_after_eio);

        // 4. Error on fallback after unsupported F_FULLFSYNC is propagated.
        let err = sync_full_or_fallback(
            || Err(io::Error::from_raw_os_error(MACOS_EOPNOTSUPP)),
            || Err(io::Error::from_raw_os_error(5)),
        )
        .unwrap_err();
        assert_eq!(err.raw_os_error(), Some(5));
    }

    #[test]
    fn handle_pool_caps_descriptors_and_reopens_on_demand() {
        let tmp = TempTestDir::new("handle_pool");
        let fs = UnixPlatformFs::open_with_pool_cap(&tmp.path, true, 2).expect("open fs");

        let f1 = fs
            .open_file("idx_0001.db", true, true, OpenHint::ImmutableRandom)
            .unwrap();
        f1.write_all_at(b"hello", 0).unwrap();
        f1.sync_durable().unwrap();

        let f2 = fs
            .open_file("idx_0002.db", true, true, OpenHint::ImmutableRandom)
            .unwrap();
        f2.write_all_at(b"world", 0).unwrap();
        f2.sync_durable().unwrap();

        assert!(f1.is_descriptor_open());
        assert!(f2.is_descriptor_open());

        // Opening a 3rd pooled file exceeds cap 2 -> f1 is idle (unpinned) so its fd is evicted
        let f3 = fs
            .open_file("idx_0003.db", true, true, OpenHint::ImmutableRandom)
            .unwrap();
        f3.write_all_at(b"aydb!", 0).unwrap();
        f3.sync_durable().unwrap();

        assert!(
            !f1.is_descriptor_open(),
            "f1 descriptor should have been evicted"
        );
        assert!(f2.is_descriptor_open());
        assert!(f3.is_descriptor_open());

        // Pin f2 by holding another Arc reference
        let f2_pinned = Arc::clone(&f2);

        // Reading from f1 transparently reopens its descriptor
        let mut buf = [0u8; 5];
        f1.read_exact_at(&mut buf, 0).unwrap();
        assert_eq!(&buf, b"hello");
        assert!(f1.is_descriptor_open(), "f1 should be reopened on access");

        // Open f4: f2 is pinned (strong_count > 2), so f3 or f1 should be evicted instead!
        let f4 = fs
            .open_file("idx_0004.db", true, true, OpenHint::ImmutableRandom)
            .unwrap();
        f4.write_all_at(b"more!", 0).unwrap();
        f4.sync_durable().unwrap();

        assert!(
            f2.is_descriptor_open(),
            "f2 was pinned and must not be evicted"
        );
        drop(f2_pinned);
    }

    fn open_count(handles: &[&Arc<UnixPlatformFile>]) -> usize {
        handles.iter().filter(|h| h.is_descriptor_open()).count()
    }

    #[test]
    fn reopened_handles_count_against_the_cap() {
        let tmp = TempTestDir::new("handle_pool_reopen_cap");
        let fs = UnixPlatformFs::open_with_pool_cap(&tmp.path, true, 2).expect("open fs");
        let mk = |name: &str| {
            let f = fs
                .open_file(name, true, true, OpenHint::ImmutableRandom)
                .unwrap();
            f.write_all_at(b"data!", 0).unwrap();
            f.sync_durable().unwrap();
            f
        };
        let f1 = mk("idx_0001.db");
        let f2 = mk("idx_0002.db");
        let f3 = mk("idx_0003.db");
        assert!(!f1.is_descriptor_open());

        // Reopening f1 must evict another idle handle instead of exceeding the cap.
        let mut buf = [0u8; 5];
        f1.read_exact_at(&mut buf, 0).unwrap();
        assert!(f1.is_descriptor_open());
        assert_eq!(open_count(&[&f1, &f2, &f3]), 2);

        // Cycling through all handles keeps the open count at the cap.
        for f in [&f2, &f3, &f1, &f2] {
            f.read_exact_at(&mut buf, 0).unwrap();
            assert_eq!(&buf, b"data!");
            assert_eq!(open_count(&[&f1, &f2, &f3]), 2);
        }
    }

    #[test]
    fn referenced_handle_stays_readable_after_remove_file() {
        let tmp = TempTestDir::new("handle_pool_unlink");
        let fs = UnixPlatformFs::open_with_pool_cap(&tmp.path, true, 1).expect("open fs");

        // The only owner of f1 plays the role of a LazyBlob cursor.
        let f1 = fs
            .open_file("idx_0001.db", true, true, OpenHint::ImmutableRandom)
            .unwrap();
        f1.write_all_at(b"hello", 0).unwrap();
        f1.sync_durable().unwrap();
        let f2 = fs
            .open_file("idx_0002.db", true, true, OpenHint::ImmutableRandom)
            .unwrap();
        assert!(!f1.is_descriptor_open(), "f1 was evicted by the cap");

        fs.remove_file("idx_0001.db").unwrap();
        assert!(
            !fs.list_files()
                .unwrap()
                .contains(&"idx_0001.db".to_string())
        );
        assert!(f1.is_descriptor_open());

        // Later opens must not close the unlinked file's descriptor.
        let f3 = fs
            .open_file("idx_0003.db", true, true, OpenHint::ImmutableRandom)
            .unwrap();
        f3.write_all_at(b"x", 0).unwrap();
        f2.write_all_at(b"y", 0).unwrap();
        assert!(f1.is_descriptor_open());
        let mut buf = [0u8; 5];
        f1.read_exact_at(&mut buf, 0).unwrap();
        assert_eq!(&buf, b"hello");
    }

    #[test]
    fn dirty_writable_handle_is_not_evicted_before_sync() {
        let tmp = TempTestDir::new("handle_pool_dirty");
        let fs = UnixPlatformFs::open_with_pool_cap(&tmp.path, true, 1).expect("open fs");

        let f1 = fs
            .open_file("idx_0001.db", true, true, OpenHint::ImmutableRandom)
            .unwrap();
        f1.write_all_at(b"dirty", 0).unwrap();
        assert!(f1.is_descriptor_open());

        // While f1 holds unsynced dirty writes, opening f2 must keep f1 open so kernel
        // writeback errors (Linux `errseq_t`) are never lost across descriptor reopen.
        let f2 = fs
            .open_file("idx_0002.db", true, true, OpenHint::ImmutableRandom)
            .unwrap();
        assert!(f1.is_descriptor_open());
        assert!(f2.is_descriptor_open());

        f1.sync_durable().unwrap();
        let _f3 = fs
            .open_file("idx_0003.db", true, true, OpenHint::ImmutableRandom)
            .unwrap();
        assert!(!f1.is_descriptor_open());
    }

    #[test]
    fn handle_pool_reclaims_multiple_excess_handles_and_uses_clock_second_chance() {
        // 1. Multiple excess dirty handles that later sync must all drain back to <= cap on the
        // next open (rather than leaking above cap one-by-one).
        let tmp = TempTestDir::new("handle_pool_multi_excess");
        let fs = UnixPlatformFs::open_with_pool_cap(&tmp.path, true, 2).expect("open fs");

        let f1 = fs
            .open_file("idx_0001.db", true, true, OpenHint::ImmutableRandom)
            .unwrap();
        f1.write_all_at(b"a", 0).unwrap();
        let f2 = fs
            .open_file("idx_0002.db", true, true, OpenHint::ImmutableRandom)
            .unwrap();
        f2.write_all_at(b"b", 0).unwrap();
        let f3 = fs
            .open_file("idx_0003.db", true, true, OpenHint::ImmutableRandom)
            .unwrap();
        f3.write_all_at(b"c", 0).unwrap();
        assert_eq!(open_count(&[&f1, &f2, &f3]), 3);

        f1.sync_durable().unwrap();
        f2.sync_durable().unwrap();
        f3.sync_durable().unwrap();

        let f4 = fs
            .open_file("idx_0004.db", true, true, OpenHint::ImmutableRandom)
            .unwrap();
        f4.write_all_at(b"d", 0).unwrap();
        f4.sync_durable().unwrap();
        assert!(
            open_count(&[&f1, &f2, &f3, &f4]) <= 2,
            "open count exceeded cap after clean drain: {}",
            open_count(&[&f1, &f2, &f3, &f4])
        );

        // 2. CLOCK second-chance: touch older handle `h1` after a sweep cleared reference bits,
        // so opening `h4` grants `h1` a second chance and evicts newer untouched `h2` instead.
        let tmp2 = TempTestDir::new("handle_pool_clock_second_chance");
        let fs2 = UnixPlatformFs::open_with_pool_cap(&tmp2.path, true, 3).expect("open fs2");
        let mk_clean = |name: &str, data: &[u8]| {
            let f = fs2
                .open_file(name, true, true, OpenHint::ImmutableRandom)
                .unwrap();
            f.write_all_at(data, 0).unwrap();
            f.sync_durable().unwrap();
            f
        };
        let h0 = mk_clean("idx_0010.db", b"h0");
        let h1 = mk_clean("idx_0011.db", b"h1");
        let h2 = mk_clean("idx_0012.db", b"h2");
        // Opening h3 clears reference bits on h0..h2 and evicts h0, leaving clock_hand at h1 and
        // h1/h2 unreferenced.
        let h3 = mk_clean("idx_0013.db", b"h3");
        assert!(!h0.is_descriptor_open());
        assert!(h1.is_descriptor_open());
        assert!(h2.is_descriptor_open());
        assert!(h3.is_descriptor_open());

        // Reading h1 sets its reference bit without touching h2.
        let mut buf = [0u8; 2];
        h1.read_exact_at(&mut buf, 0).unwrap();
        assert_eq!(&buf, b"h1");

        // Opening h4 must give h1 a second chance and evict h2 instead.
        let h4 = mk_clean("idx_0014.db", b"h4");
        assert!(
            h1.is_descriptor_open(),
            "referenced h1 should receive CLOCK second chance"
        );
        assert!(
            !h2.is_descriptor_open(),
            "unreferenced h2 should be evicted before recently read h1"
        );
        assert!(h3.is_descriptor_open());
        assert!(h4.is_descriptor_open());
    }

    #[test]
    fn fallocate_or_fallback_propagates_enospc_and_falls_back_only_when_unsupported() {
        #[cfg(target_os = "linux")]
        const UNSUPPORTED_ERRNO: c_int = 95;
        #[cfg(not(target_os = "linux"))]
        const UNSUPPORTED_ERRNO: c_int = MACOS_ENOTSUP;
        const EINTR_ERRNO: c_int = 4;
        const EINVAL_ERRNO: c_int = 22;
        const ENOSPC_ERRNO: c_int = 28;
        const EIO_ERRNO: c_int = 5;

        // 1. Success or EINTR retry never invokes fallback.
        let mut attempts = 0usize;
        let mut fb_calls = 0usize;
        fallocate_or_fallback(
            || {
                attempts += 1;
                if attempts == 1 { EINTR_ERRNO } else { 0 }
            },
            || {
                fb_calls += 1;
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(attempts, 2);
        assert_eq!(fb_calls, 0);

        // 2. EOPNOTSUPP / EINVAL trigger `set_len` fallback.
        for errno in [UNSUPPORTED_ERRNO, EINVAL_ERRNO] {
            let mut fb = 0usize;
            fallocate_or_fallback(
                || errno,
                || {
                    fb += 1;
                    Ok(())
                },
            )
            .unwrap();
            assert_eq!(fb, 1, "errno {errno}");
        }

        // 3. ENOSPC and EIO propagate immediately without calling fallback.
        for errno in [ENOSPC_ERRNO, EIO_ERRNO] {
            let mut fb = 0usize;
            let err = fallocate_or_fallback(
                || errno,
                || {
                    fb += 1;
                    Ok(())
                },
            )
            .unwrap_err();
            assert_eq!(err.raw_os_error(), Some(errno));
            assert_eq!(fb, 0, "fallback must not run on errno {errno}");
        }
    }
}
