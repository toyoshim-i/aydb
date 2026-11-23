//! Windows-specific Platform Abstraction Layer (`WinPlatformFile` / `WinPlatformFs`).
//!
//! Key architectural properties:
//! 1. **Single-Host Exclusivity (`dwShareMode = 0`)**: Preserves SMB Read-Write-Handle (RWH)
//!    leases and prevents external processes from interfering with active database files.
//! 2. **Lock-Free Concurrent Positional I/O (`FILE_FLAG_OVERLAPPED`)**: Omits the kernel's
//!    `FO_SYNCHRONOUS_IO` serialization lock so multiple threads can read cached 4 KB index
//!    blocks concurrently without Head-of-Line Blocking behind a cold disk/SMB read.
//! 3. **Synchronous Cache-Hit Fast Path + Thread-Local Event**: When a read hits the Windows
//!    Cache Manager (`CcCopyRead`), `ReadFile` completes synchronously without waiting on
//!    an event. When a read misses cache (`ERROR_IO_PENDING`), the calling thread waits on a
//!    reusable manual-reset event cached in `thread_local!` storage.

use std::cell::RefCell;
use std::ffi::OsStr;
use std::io;
use std::os::windows::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::Arc;

use super::path::{normalize_db_root_path, validate_flat_file_name};
use super::{OpenHint, PlatformFile, PlatformFs, PreallocStrategy};

#[allow(clippy::upper_case_acronyms)]
type HANDLE = *mut core::ffi::c_void;
#[allow(clippy::upper_case_acronyms)]
type BOOL = i32;
#[allow(clippy::upper_case_acronyms)]
type DWORD = u32;

const INVALID_HANDLE_VALUE: HANDLE = (-1isize) as HANDLE;
const FALSE: BOOL = 0;

const GENERIC_READ: DWORD = 0x8000_0000;
const GENERIC_WRITE: DWORD = 0x4000_0000;

const FILE_SHARE_NONE: DWORD = 0;
const FILE_SHARE_READ: DWORD = 1;
const FILE_SHARE_WRITE: DWORD = 2;
const FILE_SHARE_DELETE: DWORD = 4;

const CREATE_ALWAYS: DWORD = 2;
const OPEN_EXISTING: DWORD = 3;
const OPEN_ALWAYS: DWORD = 4;

const FILE_ATTRIBUTE_NORMAL: DWORD = 0x0000_0080;
const FILE_FLAG_BACKUP_SEMANTICS: DWORD = 0x0200_0000;
const FILE_FLAG_OVERLAPPED: DWORD = 0x4000_0000;
const FILE_FLAG_SEQUENTIAL_SCAN: DWORD = 0x0800_0000;
const FILE_FLAG_RANDOM_ACCESS: DWORD = 0x1000_0000;

const ERROR_INVALID_FUNCTION: DWORD = 1;
const ERROR_ACCESS_DENIED: DWORD = 5;
const ERROR_SHARING_VIOLATION: DWORD = 32;
const ERROR_NOT_SUPPORTED: DWORD = 50;

/// Number of retries after a first `ERROR_SHARING_VIOLATION` on open.
const SHARING_VIOLATION_RETRIES: u32 = 10;

/// Delay before retry number `retry` (0-based) after `ERROR_SHARING_VIOLATION`, or `None` once the
/// budget is spent. The sequence is 1, 2, 4, 8, 16, 32, 64, 128, 256, 512 ms: ten retries, 1023 ms
/// in total. Scanners and backup agents commonly hold a handle for hundreds of milliseconds.
fn sharing_violation_backoff(retry: u32) -> Option<std::time::Duration> {
    (retry < SHARING_VIOLATION_RETRIES).then(|| std::time::Duration::from_millis(1u64 << retry))
}
const ERROR_HANDLE_EOF: DWORD = 38;
const ERROR_ALREADY_EXISTS: DWORD = 183;
const ERROR_IO_PENDING: DWORD = 997;

/// Instructs the Windows I/O Manager not to signal the file handle object on synchronous
/// or queued completion, avoiding extra kernel dispatcher work on every cached read/write.
const FILE_SKIP_SET_EVENT_ON_HANDLE: u8 = 0x02;

// FILE_INFO_BY_HANDLE_CLASS discriminants (`minwinbase.h`)
const FILE_ALLOCATION_INFO_CLASS: i32 = 5;
const FILE_END_OF_FILE_INFO_CLASS: i32 = 6;
const FILE_IO_PRIORITY_HINT_INFO_CLASS: i32 = 12;

// IO_PRIORITY_HINT discriminant for background bulk I/O
const IO_PRIORITY_HINT_LOW: i32 = 1;

#[repr(C)]
struct Overlapped {
    internal: usize,
    internal_high: usize,
    offset: u32,
    offset_high: u32,
    h_event: HANDLE,
}

#[repr(C)]
struct FileAllocationInfo {
    allocation_size: i64,
}

#[repr(C)]
struct FileEndOfFileInfo {
    end_of_file: i64,
}

#[repr(C)]
struct FileIoPriorityHintInfo {
    priority_hint: i32,
}

#[link(name = "kernel32")]
unsafe extern "system" {
    fn CreateFileW(
        lp_file_name: *const u16,
        dw_desired_access: DWORD,
        dw_share_mode: DWORD,
        lp_security_attributes: *const core::ffi::c_void,
        dw_creation_disposition: DWORD,
        dw_flags_and_attributes: DWORD,
        h_template_file: HANDLE,
    ) -> HANDLE;

    fn ReadFile(
        h_file: HANDLE,
        lp_buffer: *mut u8,
        n_number_of_bytes_to_read: DWORD,
        lp_number_of_bytes_read: *mut DWORD,
        lp_overlapped: *mut Overlapped,
    ) -> BOOL;

    fn WriteFile(
        h_file: HANDLE,
        lp_buffer: *const u8,
        n_number_of_bytes_to_write: DWORD,
        lp_number_of_bytes_written: *mut DWORD,
        lp_overlapped: *mut Overlapped,
    ) -> BOOL;

    fn GetOverlappedResult(
        h_file: HANDLE,
        lp_overlapped: *mut Overlapped,
        lp_number_of_bytes_transferred: *mut DWORD,
        b_wait: BOOL,
    ) -> BOOL;

    fn FlushFileBuffers(h_file: HANDLE) -> BOOL;

    fn GetFileSizeEx(h_file: HANDLE, lp_file_size: *mut i64) -> BOOL;

    fn SetFileInformationByHandle(
        h_file: HANDLE,
        file_information_class: i32,
        lp_file_information: *const core::ffi::c_void,
        dw_buffer_size: DWORD,
    ) -> BOOL;

    fn CreateEventW(
        lp_event_attributes: *const core::ffi::c_void,
        b_manual_reset: BOOL,
        b_initial_state: BOOL,
        lp_name: *const u16,
    ) -> HANDLE;

    fn SetFileCompletionNotificationModes(file_handle: HANDLE, flags: u8) -> BOOL;

    fn CloseHandle(h_object: HANDLE) -> BOOL;

    fn GetLastError() -> DWORD;
}

/// Per-thread manual-reset Win32 Event used when an overlapped I/O returns `ERROR_IO_PENDING`.
struct ThreadLocalEvent(HANDLE);

impl ThreadLocalEvent {
    #[inline]
    fn get_or_create(&mut self) -> io::Result<HANDLE> {
        if self.0.is_null() {
            // Create an unnamed, manual-reset event initialized to non-signaled.
            let h = unsafe { CreateEventW(ptr::null(), 1, 0, ptr::null()) };
            if h.is_null() {
                return Err(io::Error::last_os_error());
            }
            self.0 = h;
        }
        Ok(self.0)
    }
}

impl Drop for ThreadLocalEvent {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe {
                CloseHandle(self.0);
            }
        }
    }
}

thread_local! {
    static TLS_EVENT: RefCell<ThreadLocalEvent> = const { RefCell::new(ThreadLocalEvent(ptr::null_mut())) };
}

#[inline]
fn validate_io_offset(offset: u64) -> io::Result<()> {
    if i64::try_from(offset).is_err() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "file offset exceeds i64::MAX",
        ));
    }
    Ok(())
}

#[inline]
fn with_tls_event<F, R>(f: F) -> io::Result<R>
where
    F: FnOnce(HANDLE) -> io::Result<R>,
{
    let mut f = Some(f);
    match TLS_EVENT.try_with(|cell| {
        let mut guard = cell.borrow_mut();
        let h_event = guard.get_or_create()?;
        // Setting the low bit (`| 1`) instructs the Windows kernel not to queue an IOCP
        // packet if the handle is ever associated with an I/O Completion Port.
        let tagged_event = ((h_event as usize) | 1) as HANDLE;
        (f.take().unwrap())(tagged_event)
    }) {
        Ok(res) => res,
        Err(_) => {
            // During thread teardown after `TLS_EVENT` has already been destroyed, allocate a
            // one-shot manual-reset event so destructor flushes never panic.
            let mut one_shot = ThreadLocalEvent(ptr::null_mut());
            let h_event = one_shot.get_or_create()?;
            let tagged_event = ((h_event as usize) | 1) as HANDLE;
            (f.take().unwrap())(tagged_event)
        }
    }
}

/// Windows implementation of [`PlatformFile`].
#[derive(Debug)]
pub struct WinPlatformFile {
    handle: HANDLE,
}

// Safety: `WinPlatformFile` performs exclusively positional overlapped I/O with per-thread
// event handles and never relies on shared mutable kernel file-offset state.
unsafe impl Send for WinPlatformFile {}
unsafe impl Sync for WinPlatformFile {}

impl WinPlatformFile {
    pub(crate) fn open_internal(
        full_path: &Path,
        create: bool,
        writable: bool,
        truncate: bool,
        hint: OpenHint,
    ) -> io::Result<(Self, bool)> {
        let wide_path: Vec<u16> = full_path
            .as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect();

        let access = if writable {
            GENERIC_READ | GENERIC_WRITE
        } else {
            GENERIC_READ
        };

        let disposition = match (create, truncate) {
            (true, true) => CREATE_ALWAYS,
            (true, false) => OPEN_ALWAYS,
            (false, _) => OPEN_EXISTING,
        };

        let mut flags = FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OVERLAPPED;
        match hint {
            OpenHint::MutableSyncLog => {
                // Durability comes from the explicit barrier. FILE_FLAG_WRITE_THROUGH is not
                // used because NTFS holds such writes back while the file is being flushed,
                // which would stop other commits from joining a group flush.
            }
            OpenHint::MutableBufferedLog => {
                flags |= FILE_FLAG_SEQUENTIAL_SCAN;
            }
            OpenHint::ImmutableSequential => {
                // `blob_*.db` files serve random point lookups (`get_row` / `get_row_into`), so
                // omit `FILE_FLAG_SEQUENTIAL_SCAN` to avoid aggressive read-ahead and premature
                // page eviction in the Windows Cache Manager.
            }
            OpenHint::ImmutableRandom => {
                flags |= FILE_FLAG_RANDOM_ACCESS;
            }
            OpenHint::BackgroundBulk => {
                flags |= FILE_FLAG_SEQUENTIAL_SCAN;
            }
        }

        let mut retry = 0u32;
        let handle = loop {
            let handle = unsafe {
                CreateFileW(
                    wide_path.as_ptr(),
                    access,
                    FILE_SHARE_NONE, // dwShareMode = 0 (Exclusive handle & SMB RWH Lease)
                    ptr::null(),
                    disposition,
                    flags,
                    ptr::null_mut(),
                )
            };
            if handle != INVALID_HANDLE_VALUE {
                break handle;
            }
            let err = unsafe { GetLastError() };
            // If an antivirus / EDR / backup scanner holds a handle on a freshly written segment
            // or manifest file, retry with bounded exponential backoff (about 1 s in total).
            if err == ERROR_SHARING_VIOLATION {
                if let Some(delay) = sharing_violation_backoff(retry) {
                    std::thread::sleep(delay);
                    retry += 1;
                    continue;
                }
            }
            return Err(io::Error::from_raw_os_error(err as i32));
        };
        // Capture existence from `CreateFileW` immediately before any subsequent Win32 call
        // alters `GetLastError()`, avoiding an extra `GetFileAttributesW` / `exists()` call.
        let already_existed = !create || unsafe { GetLastError() } == ERROR_ALREADY_EXISTS;

        // Best-effort: skip signaling the file handle dispatcher object since overlapped I/O
        // always uses a dedicated `OVERLAPPED::hEvent`.
        unsafe {
            let _ = SetFileCompletionNotificationModes(handle, FILE_SKIP_SET_EVENT_ON_HANDLE);
        }

        if hint == OpenHint::BackgroundBulk {
            // Best-effort I/O priority lowering so compaction does not starve foreground reads.
            let prio = FileIoPriorityHintInfo {
                priority_hint: IO_PRIORITY_HINT_LOW,
            };
            unsafe {
                let _ = SetFileInformationByHandle(
                    handle,
                    FILE_IO_PRIORITY_HINT_INFO_CLASS,
                    (&prio as *const FileIoPriorityHintInfo).cast(),
                    core::mem::size_of::<FileIoPriorityHintInfo>() as DWORD,
                );
            }
        }

        Ok((Self { handle }, already_existed))
    }

    fn set_allocation_size(&self, target_len: u64) -> io::Result<()> {
        let len_i64 = i64::try_from(target_len).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "file size exceeds i64::MAX")
        })?;
        let info = FileAllocationInfo {
            allocation_size: len_i64,
        };
        let ok = unsafe {
            SetFileInformationByHandle(
                self.handle,
                FILE_ALLOCATION_INFO_CLASS,
                (&info as *const FileAllocationInfo).cast(),
                core::mem::size_of::<FileAllocationInfo>() as DWORD,
            )
        };
        if ok == FALSE {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    fn set_eof(&self, target_len: u64) -> io::Result<()> {
        let len_i64 = i64::try_from(target_len).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "file size exceeds i64::MAX")
        })?;
        let info = FileEndOfFileInfo {
            end_of_file: len_i64,
        };
        let ok = unsafe {
            SetFileInformationByHandle(
                self.handle,
                FILE_END_OF_FILE_INFO_CLASS,
                (&info as *const FileEndOfFileInfo).cast(),
                core::mem::size_of::<FileEndOfFileInfo>() as DWORD,
            )
        };
        if ok == FALSE {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
}

impl Drop for WinPlatformFile {
    fn drop(&mut self) {
        if self.handle != INVALID_HANDLE_VALUE && !self.handle.is_null() {
            unsafe {
                CloseHandle(self.handle);
            }
        }
    }
}

impl PlatformFile for WinPlatformFile {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        validate_io_offset(offset)?;
        if buf.is_empty() {
            return Ok(0);
        }
        let chunk_len = buf.len().min(u32::MAX as usize) as DWORD;

        with_tls_event(|h_event| {
            let mut overlapped = Overlapped {
                internal: 0,
                internal_high: 0,
                offset: offset as u32,
                offset_high: (offset >> 32) as u32,
                h_event,
            };
            let mut bytes_read: DWORD = 0;

            let res = unsafe {
                ReadFile(
                    self.handle,
                    buf.as_mut_ptr(),
                    chunk_len,
                    ptr::null_mut(),
                    &mut overlapped,
                )
            };

            if res != FALSE {
                // Fast path: completed synchronously in the Windows Cache Manager. Use
                // `GetOverlappedResult(..., 0)` so an overlapped call never relies on
                // `lpNumberOfBytesRead`.
                let sync_ok = unsafe {
                    GetOverlappedResult(self.handle, &mut overlapped, &mut bytes_read, 0)
                };
                if sync_ok != FALSE {
                    return Ok(bytes_read as usize);
                }
                let sync_err = unsafe { GetLastError() };
                if sync_err == ERROR_HANDLE_EOF {
                    return Ok(0);
                }
                return Err(io::Error::from_raw_os_error(sync_err as i32));
            }

            let err = unsafe { GetLastError() };
            if err == ERROR_HANDLE_EOF {
                return Ok(0);
            }
            if err != ERROR_IO_PENDING {
                return Err(io::Error::from_raw_os_error(err as i32));
            }

            // Slow path: wait on the thread-local manual-reset event (`ReadFile`/`WriteFile`
            // reset `OVERLAPPED::hEvent` automatically before starting the next I/O).
            let wait_res =
                unsafe { GetOverlappedResult(self.handle, &mut overlapped, &mut bytes_read, 1) };
            if wait_res != FALSE {
                return Ok(bytes_read as usize);
            }
            let wait_err = unsafe { GetLastError() };
            if wait_err == ERROR_HANDLE_EOF {
                return Ok(0);
            }
            Err(io::Error::from_raw_os_error(wait_err as i32))
        })
    }

    fn read_exact_at(&self, mut buf: &mut [u8], mut offset: u64) -> io::Result<()> {
        validate_io_offset(offset)?;
        while !buf.is_empty() {
            let n = self.read_at(buf, offset)?;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "failed to fill whole buffer in read_exact_at",
                ));
            }
            buf = &mut buf[n..];
            offset = offset.checked_add(n as u64).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "file offset overflow")
            })?;
            if !buf.is_empty() {
                validate_io_offset(offset)?;
            }
        }
        Ok(())
    }

    fn read_many_at(&self, requests: &mut [(u64, &mut [u8])]) -> io::Result<()> {
        super::coalesced_read_many_at(self, requests)
    }

    fn write_all_at(&self, mut buf: &[u8], mut offset: u64) -> io::Result<()> {
        validate_io_offset(offset)?;
        while !buf.is_empty() {
            validate_io_offset(offset)?;
            let chunk_len = buf.len().min(u32::MAX as usize) as DWORD;
            let written = with_tls_event(|h_event| {
                let mut overlapped = Overlapped {
                    internal: 0,
                    internal_high: 0,
                    offset: offset as u32,
                    offset_high: (offset >> 32) as u32,
                    h_event,
                };
                let mut bytes_written: DWORD = 0;

                let res = unsafe {
                    WriteFile(
                        self.handle,
                        buf.as_ptr(),
                        chunk_len,
                        ptr::null_mut(),
                        &mut overlapped,
                    )
                };

                if res != FALSE {
                    let sync_ok = unsafe {
                        GetOverlappedResult(self.handle, &mut overlapped, &mut bytes_written, 0)
                    };
                    if sync_ok != FALSE {
                        return Ok(bytes_written as usize);
                    }
                    return Err(io::Error::last_os_error());
                }

                let err = unsafe { GetLastError() };
                if err != ERROR_IO_PENDING {
                    return Err(io::Error::from_raw_os_error(err as i32));
                }

                let wait_res = unsafe {
                    GetOverlappedResult(self.handle, &mut overlapped, &mut bytes_written, 1)
                };
                if wait_res != FALSE {
                    return Ok(bytes_written as usize);
                }

                Err(io::Error::last_os_error())
            })?;

            if written == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "failed to write whole buffer in write_all_at",
                ));
            }
            buf = &buf[written..];
            offset = offset.checked_add(written as u64).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "file offset overflow")
            })?;
        }
        Ok(())
    }

    fn file_len(&self) -> io::Result<u64> {
        let mut size: i64 = 0;
        let ok = unsafe { GetFileSizeEx(self.handle, &mut size) };
        if ok == FALSE {
            return Err(io::Error::last_os_error());
        }
        Ok(size as u64)
    }

    fn preallocate(&self, target_len: u64, strategy: PreallocStrategy) -> io::Result<()> {
        let current_len = self.file_len()?;
        if current_len >= target_len {
            return Ok(());
        }
        let _ = i64::try_from(target_len).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "file size exceeds i64::MAX")
        })?;

        match strategy {
            PreallocStrategy::LogicalExtendOnly => {
                // Set logical EndOfFile (reserves clusters on NTFS lazily via VDL).
                self.set_eof(target_len)?;
            }
            PreallocStrategy::PhysicalZeroFill => {
                // Best-effort contiguous cluster reservation without advancing logical EOF yet.
                let _ = self.set_allocation_size(target_len);
                // Write physical zeros across [current_len, target_len) so NTFS ValidDataLength
                // reaches `target_len`. Roll back logical EOF to `current_len` on failure.
                super::preallocate_physical_zero_fill_with_rollback(
                    self,
                    current_len,
                    target_len,
                    |len| {
                        let _ = self.set_eof(len);
                    },
                )?;
            }
        }

        Ok(())
    }

    fn sync_barrier(&self) -> io::Result<()> {
        // Every barrier flushes the device, as on Unix.
        self.sync_durable()
    }

    fn sync_durable(&self) -> io::Result<()> {
        let ok = unsafe { FlushFileBuffers(self.handle) };
        if ok == FALSE {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    fn evict_os_cache(&self, _offset: u64, _len: u64) -> io::Result<()> {
        // On Windows, `FILE_FLAG_SEQUENTIAL_SCAN` instructs the Cache Manager to unmap
        // 256 KB VACB views automatically behind sequential readers.
        Ok(())
    }
}

/// Windows implementation of [`PlatformFs`].
#[derive(Debug, Clone)]
pub struct WinPlatformFs {
    root: PathBuf,
}

fn is_unc_extended_path(path: &Path) -> bool {
    let wide: Vec<u16> = path.as_os_str().encode_wide().collect();
    if wide.len() < 8 {
        return false;
    }
    let pfx = [b'\\' as u16, b'\\' as u16, b'?' as u16, b'\\' as u16];
    if wide[0..4] != pfx {
        return false;
    }
    let unc_eq = |a: u16, upper: u8| a == upper as u16 || a == upper.to_ascii_lowercase() as u16;
    unc_eq(wide[4], b'U')
        && unc_eq(wide[5], b'N')
        && unc_eq(wide[6], b'C')
        && wide[7] == b'\\' as u16
}

impl WinPlatformFs {
    /// Opens or initializes a database directory at `root`, normalizing the path with `\\?\`.
    pub fn open(root: impl AsRef<Path>, create: bool) -> io::Result<Self> {
        let normalized = normalize_db_root_path(root.as_ref(), create)?;
        Ok(Self { root: normalized })
    }

    /// Best-effort parent directory metadata flush for SMB/CIFS UNC shares (`\\?\UNC\...`) so
    /// newly created or unlinked directory entries survive remote server power loss.
    fn sync_dir_if_unc(&self) -> io::Result<()> {
        if !is_unc_extended_path(&self.root) {
            return Ok(());
        }
        let wide_dir: Vec<u16> = self
            .root
            .as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect();
        let handle = unsafe {
            CreateFileW(
                wide_dir.as_ptr(),
                GENERIC_WRITE,
                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                ptr::null(),
                OPEN_EXISTING,
                FILE_FLAG_BACKUP_SEMANTICS,
                ptr::null_mut(),
            )
        };
        if handle == INVALID_HANDLE_VALUE {
            let err = unsafe { GetLastError() };
            if matches!(
                err,
                ERROR_ACCESS_DENIED | ERROR_INVALID_FUNCTION | ERROR_NOT_SUPPORTED
            ) {
                return Ok(());
            }
            return Err(io::Error::from_raw_os_error(err as i32));
        }
        let ok = unsafe { FlushFileBuffers(handle) };
        let err = if ok == FALSE {
            unsafe { GetLastError() }
        } else {
            0
        };
        unsafe {
            CloseHandle(handle);
        }
        if ok != FALSE
            || matches!(
                err,
                ERROR_ACCESS_DENIED | ERROR_INVALID_FUNCTION | ERROR_NOT_SUPPORTED
            )
        {
            Ok(())
        } else {
            Err(io::Error::from_raw_os_error(err as i32))
        }
    }
    fn remove_file_raw(&self, rel_name: &str) -> io::Result<bool> {
        validate_flat_file_name(rel_name)?;
        let full_path = self.root.join(OsStr::new(rel_name));
        match std::fs::remove_file(full_path) {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e),
        }
    }
}

impl PlatformFs for WinPlatformFs {
    type File = WinPlatformFile;

    fn open_file(
        &self,
        rel_name: &str,
        create: bool,
        writable: bool,
        hint: OpenHint,
    ) -> io::Result<Arc<Self::File>> {
        validate_flat_file_name(rel_name)?;
        let full_path = self.root.join(OsStr::new(rel_name));
        let (file, existed) =
            WinPlatformFile::open_internal(&full_path, create, writable, false, hint)?;
        if create
            && (!existed
                || matches!(
                    hint,
                    OpenHint::MutableSyncLog | OpenHint::MutableBufferedLog
                ))
        {
            self.sync_dir_if_unc()?;
        }
        Ok(Arc::new(file))
    }

    fn remove_file(&self, rel_name: &str) -> io::Result<()> {
        if self.remove_file_raw(rel_name)? {
            // The unlink has already succeeded; a best-effort UNC parent-dir flush failure
            // must not make callers believe the file still exists.
            let _ = self.sync_dir_if_unc();
        }
        Ok(())
    }

    fn remove_file_unsynced(&self, rel_name: &str) -> io::Result<()> {
        self.remove_file_raw(rel_name).map(|_| ())
    }

    fn sync_dir(&self) -> io::Result<()> {
        self.sync_dir_if_unc()
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

    #[test]
    fn is_unc_extended_path_detects_only_unc_paths() {
        assert!(is_unc_extended_path(Path::new(r"\\?\UNC\server\share\dir")));
        assert!(is_unc_extended_path(Path::new(r"\\?\unc\server\share\dir")));
        assert!(!is_unc_extended_path(Path::new(r"\\?\C:\dir")));
        assert!(!is_unc_extended_path(Path::new(r"C:\dir")));
    }

    #[test]
    fn sharing_violation_backoff_is_bounded_exponential_of_about_one_second() {
        let delays: Vec<u64> = (0..)
            .map_while(sharing_violation_backoff)
            .map(|d| d.as_millis() as u64)
            .collect();
        assert_eq!(delays, [1, 2, 4, 8, 16, 32, 64, 128, 256, 512]);
        assert_eq!(delays.iter().sum::<u64>(), 1023);
        assert_eq!(sharing_violation_backoff(10), None);
        assert_eq!(sharing_violation_backoff(u32::MAX), None);
    }

    #[test]
    fn open_internal_reports_existence_and_overlapped_io_works_without_manual_reset() {
        let dir = crate::util::test_util::TempTestDir::new("win_open_overlapped");
        let fs = WinPlatformFs::open(&dir.path, true).unwrap();
        let path = fs.root_path().join("probe.dat");

        // Fresh creation reports `already_existed == false` and preserves positional R/W.
        let (file1, existed1) =
            WinPlatformFile::open_internal(&path, true, true, false, OpenHint::MutableBufferedLog)
                .unwrap();
        assert!(!existed1);
        file1.write_all_at(b"hello-world", 4096).unwrap();
        file1.write_all_at(b"SECOND", 16 * 1024).unwrap();
        let mut buf1 = [0u8; 11];
        file1.read_exact_at(&mut buf1, 4096).unwrap();
        assert_eq!(&buf1, b"hello-world");
        drop(file1);

        // Reopen reports `already_existed == true` and concurrent cross-thread reads succeed.
        let (file2, existed2) =
            WinPlatformFile::open_internal(&path, true, false, false, OpenHint::ImmutableRandom)
                .unwrap();
        assert!(existed2);
        let shared = Arc::new(file2);
        let worker = {
            let shared = Arc::clone(&shared);
            std::thread::spawn(move || {
                let mut b = [0u8; 6];
                shared.read_exact_at(&mut b, 16 * 1024).unwrap();
                assert_eq!(&b, b"SECOND");
            })
        };
        let mut b = [0u8; 11];
        shared.read_exact_at(&mut b, 4096).unwrap();
        assert_eq!(&b, b"hello-world");
        worker.join().unwrap();
    }

    #[test]
    fn set_allocation_size_preserves_eof_and_physical_zero_fill_extends_cleanly() {
        let dir = crate::util::test_util::TempTestDir::new("win_alloc_size");
        let fs = WinPlatformFs::open(&dir.path, true).unwrap();
        let file = fs
            .open_file("wal_sync.db", true, true, OpenHint::MutableSyncLog)
            .unwrap();

        // Reserving cluster allocation does not advance logical EOF.
        file.set_allocation_size(64 * 1024).unwrap();
        assert_eq!(file.file_len().unwrap(), 0);

        // Rolling back EOF after a simulated partial write restores `current_len` on disk.
        file.set_eof(8192).unwrap();
        assert_eq!(file.file_len().unwrap(), 8192);
        file.set_eof(4096).unwrap();
        assert_eq!(file.file_len().unwrap(), 4096);

        // Extending via PhysicalZeroFill reaches `target_len` with physical zero bytes.
        file.preallocate(64 * 1024, PreallocStrategy::PhysicalZeroFill)
            .unwrap();
        assert_eq!(file.file_len().unwrap(), 64 * 1024);

        let mut buf = vec![0xA5u8; 64 * 1024];
        file.read_exact_at(&mut buf, 0).unwrap();
        assert!(buf.iter().all(|&b| b == 0));

        // Oversized target length is rejected upfront without changing file_len.
        let err = file
            .preallocate(u64::MAX, PreallocStrategy::PhysicalZeroFill)
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(file.file_len().unwrap(), 64 * 1024);
    }

    #[test]
    fn background_io_priority_hint_and_offset_bounds_and_tls_teardown() {
        let dir = crate::util::test_util::TempTestDir::new("win_prio_bounds_tls");
        let fs = Arc::new(WinPlatformFs::open(&dir.path, true).unwrap());
        let file = fs
            .open_file("bulk.dat", true, true, OpenHint::BackgroundBulk)
            .unwrap();

        // `FileIoPriorityHintInfo` is class 12 in Windows SDK `minwinbase.h`.
        let prio = FileIoPriorityHintInfo {
            priority_hint: IO_PRIORITY_HINT_LOW,
        };
        let ok = unsafe {
            SetFileInformationByHandle(
                file.handle,
                FILE_IO_PRIORITY_HINT_INFO_CLASS,
                (&prio as *const FileIoPriorityHintInfo).cast(),
                core::mem::size_of::<FileIoPriorityHintInfo>() as DWORD,
            )
        };
        assert_ne!(
            ok,
            0,
            "SetFileInformationByHandle(FileIoPriorityHintInfo) failed: {}",
            io::Error::last_os_error()
        );

        // Offsets above `i64::MAX` (especially `u64::MAX` / FILE_WRITE_TO_END_OF_FILE) must fail.
        for bad_off in [i64::MAX as u64 + 1, u64::MAX] {
            let mut rbuf = [0u8; 8];
            assert_eq!(
                file.read_at(&mut rbuf, bad_off).unwrap_err().kind(),
                io::ErrorKind::InvalidInput
            );
            assert_eq!(
                file.read_exact_at(&mut rbuf, bad_off).unwrap_err().kind(),
                io::ErrorKind::InvalidInput
            );
            assert_eq!(
                file.write_all_at(b"abc", bad_off).unwrap_err().kind(),
                io::ErrorKind::InvalidInput
            );
        }
        assert_eq!(file.file_len().unwrap(), 0);

        // Overlapped I/O executed from a TLS destructor during thread teardown must not panic
        // regardless of whether `TLS_EVENT` was destroyed before or after the caller's TLS slot.
        struct FlushOnExit {
            file: Option<Arc<WinPlatformFile>>,
            offset: u64,
        }
        impl Drop for FlushOnExit {
            fn drop(&mut self) {
                if let Some(f) = self.file.take() {
                    f.write_all_at(b"teardown", self.offset).unwrap();
                }
            }
        }
        thread_local! {
            static EXIT_FLUSH: RefCell<Option<FlushOnExit>> = const { RefCell::new(None) };
        }

        for (idx, touch_tls_event_first) in [false, true].into_iter().enumerate() {
            let f = Arc::clone(&file);
            let off = (idx as u64) * 16;
            std::thread::spawn(move || {
                if touch_tls_event_first {
                    let mut tmp = [0u8; 1];
                    let _ = f.read_at(&mut tmp, 0);
                }
                EXIT_FLUSH.with(|slot| {
                    *slot.borrow_mut() = Some(FlushOnExit {
                        file: Some(Arc::clone(&f)),
                        offset: off,
                    });
                });
                if !touch_tls_event_first {
                    let mut tmp = [0u8; 1];
                    let _ = f.read_at(&mut tmp, 0);
                }
            })
            .join()
            .unwrap();
        }
        let mut check = [0u8; 8];
        file.read_exact_at(&mut check, 0).unwrap();
        assert_eq!(&check, b"teardown");
        file.read_exact_at(&mut check, 16).unwrap();
        assert_eq!(&check, b"teardown");
    }
}
