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

const CREATE_ALWAYS: DWORD = 2;
const OPEN_EXISTING: DWORD = 3;
const OPEN_ALWAYS: DWORD = 4;

const FILE_ATTRIBUTE_NORMAL: DWORD = 0x0000_0080;
const FILE_FLAG_OVERLAPPED: DWORD = 0x4000_0000;
const FILE_FLAG_SEQUENTIAL_SCAN: DWORD = 0x0800_0000;
const FILE_FLAG_RANDOM_ACCESS: DWORD = 0x1000_0000;

const ERROR_SHARING_VIOLATION: DWORD = 32;
const ERROR_HANDLE_EOF: DWORD = 38;
const ERROR_IO_PENDING: DWORD = 997;

// FILE_INFO_BY_HANDLE_CLASS discriminants
const FILE_END_OF_FILE_INFO_CLASS: i32 = 6;
const FILE_IO_PRIORITY_HINT_INFO_CLASS: i32 = 13;

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

    fn ResetEvent(h_event: HANDLE) -> BOOL;

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

    #[inline]
    fn reset(&self) {
        if !self.0.is_null() {
            unsafe {
                ResetEvent(self.0);
            }
        }
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
fn with_tls_event<F, R>(f: F) -> io::Result<R>
where
    F: FnOnce(HANDLE, &ThreadLocalEvent) -> io::Result<R>,
{
    TLS_EVENT.with(|cell| {
        let mut guard = cell.borrow_mut();
        let h_event = guard.get_or_create()?;
        // Setting the low bit (`| 1`) instructs the Windows kernel not to queue an IOCP
        // packet if the handle is ever associated with an I/O Completion Port.
        let tagged_event = ((h_event as usize) | 1) as HANDLE;
        f(tagged_event, &guard)
    })
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
    ) -> io::Result<Self> {
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

        let mut handle = INVALID_HANDLE_VALUE;
        for retry in 0..=4u32 {
            handle = unsafe {
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
                break;
            }
            let err = unsafe { GetLastError() };
            // If an antivirus / EDR minifilter briefly holds a scan handle on a freshly written
            // segment or manifest file, retry with short exponential backoff.
            if err == ERROR_SHARING_VIOLATION && retry < 4 {
                std::thread::sleep(std::time::Duration::from_millis(1u64 << retry));
                continue;
            }
            return Err(io::Error::from_raw_os_error(err as i32));
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

        Ok(Self { handle })
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
        if buf.is_empty() {
            return Ok(0);
        }
        let chunk_len = buf.len().min(u32::MAX as usize) as DWORD;

        with_tls_event(|h_event, tls_ev| {
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
                    &mut bytes_read,
                    &mut overlapped,
                )
            };

            if res != FALSE {
                // Fast path: completed synchronously in the Windows Cache Manager!
                return Ok(bytes_read as usize);
            }

            let err = unsafe { GetLastError() };
            if err == ERROR_HANDLE_EOF {
                return Ok(0);
            }
            if err != ERROR_IO_PENDING {
                return Err(io::Error::from_raw_os_error(err as i32));
            }

            // Slow path: wait on the thread-local manual-reset event, then reset it.
            let wait_res = unsafe {
                GetOverlappedResult(self.handle, &mut overlapped, &mut bytes_read, 1)
            };
            let wait_err = if wait_res == FALSE {
                unsafe { GetLastError() }
            } else {
                0
            };
            tls_ev.reset();

            if wait_res != FALSE {
                return Ok(bytes_read as usize);
            }
            if wait_err == ERROR_HANDLE_EOF {
                return Ok(0);
            }
            Err(io::Error::from_raw_os_error(wait_err as i32))
        })
    }

    fn read_exact_at(&self, mut buf: &mut [u8], mut offset: u64) -> io::Result<()> {
        while !buf.is_empty() {
            let n = self.read_at(buf, offset)?;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "failed to fill whole buffer in read_exact_at",
                ));
            }
            buf = &mut buf[n..];
            offset += n as u64;
        }
        Ok(())
    }

    fn read_many_at(&self, requests: &mut [(u64, &mut [u8])]) -> io::Result<()> {
        super::coalesced_read_many_at(self, requests)
    }

    fn write_all_at(&self, mut buf: &[u8], mut offset: u64) -> io::Result<()> {
        while !buf.is_empty() {
            let chunk_len = buf.len().min(u32::MAX as usize) as DWORD;
            let written = with_tls_event(|h_event, tls_ev| {
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
                        &mut bytes_written,
                        &mut overlapped,
                    )
                };

                if res != FALSE {
                    return Ok(bytes_written as usize);
                }

                let err = unsafe { GetLastError() };
                if err != ERROR_IO_PENDING {
                    return Err(io::Error::from_raw_os_error(err as i32));
                }

                let wait_res = unsafe {
                    GetOverlappedResult(self.handle, &mut overlapped, &mut bytes_written, 1)
                };
                let wait_err = if wait_res == FALSE {
                    unsafe { GetLastError() }
                } else {
                    0
                };
                tls_ev.reset();

                if wait_res != FALSE {
                    return Ok(bytes_written as usize);
                }

                Err(io::Error::from_raw_os_error(wait_err as i32))
            })?;

            if written == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "failed to write whole buffer in write_all_at",
                ));
            }
            buf = &buf[written..];
            offset += written as u64;
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
        if current_len >= target_len && strategy == PreallocStrategy::LogicalExtendOnly {
            return Ok(());
        }

        // 1. Set logical EndOfFile (reserves clusters on NTFS).
        self.set_eof(target_len)?;

        // 2. If PhysicalZeroFill is requested, write zeros sequentially across [current_len, target_len)
        //    so NTFS ValidDataLength (VDL) advances all the way to `target_len`.
        if strategy == PreallocStrategy::PhysicalZeroFill && current_len < target_len {
            super::preallocate_physical_zero_fill(self, current_len, target_len)?;
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

impl WinPlatformFs {
    /// Opens or initializes a database directory at `root`, normalizing the path with `\\?\`.
    pub fn open(root: impl AsRef<Path>, create: bool) -> io::Result<Self> {
        let normalized = normalize_db_root_path(root.as_ref(), create)?;
        Ok(Self { root: normalized })
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
        let file = WinPlatformFile::open_internal(&full_path, create, writable, false, hint)?;
        Ok(Arc::new(file))
    }

    fn remove_file(&self, rel_name: &str) -> io::Result<()> {
        validate_flat_file_name(rel_name)?;
        let full_path = self.root.join(OsStr::new(rel_name));
        match std::fs::remove_file(full_path) {
            Ok(()) => Ok(()),
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
