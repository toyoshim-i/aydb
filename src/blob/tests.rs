use super::*;
use crate::pal::OsFs;
use crate::util::test_util::TempTestDir;

#[test]
fn blob_write_verify_and_unflushed_truncation_safety() {
    let tmp = TempTestDir::new("blob");
    let fs = OsFs::open(&tmp.path, true).expect("open OsFs");

    let mut writer =
        BlobSegmentWriter::open(&fs, BlobKind::Standard, 0, 1).expect("open blob writer");
    let payload = vec![0xABu8; 8192];
    let ptr1 = writer
        .append_blob(b"http://example.com/a.png", &payload)
        .expect("append blob 1");
    writer.sync_durable().unwrap();
    drop(writer);

    let reader = BlobSegmentReader::open(&fs, BlobKind::Standard, 0, 1).expect("open blob reader");
    let fetched = reader
        .read_blob(&ptr1, Some(b"http://example.com/a.png"))
        .expect("read blob")
        .expect("blob must exist");
    assert_eq!(fetched, payload);

    // Wrong key check must return None
    assert!(
        reader
            .read_blob(&ptr1, Some(b"http://example.com/wrong.png"))
            .unwrap()
            .is_none()
    );

    // Verify `read_blob_into` reuses the caller's `out: &mut Vec<u8>` buffer
    // capacity across calls and validates CRC32C + `expected_key`!
    let mut reusable_buf = Vec::with_capacity(16384);
    let orig_cap = reusable_buf.capacity();
    assert!(
        reader
            .read_blob_into(&ptr1, Some(b"http://example.com/a.png"), &mut reusable_buf)
            .unwrap()
    );
    assert_eq!(reusable_buf, payload);
    assert_eq!(reusable_buf.capacity(), orig_cap);

    assert!(
        !reader
            .read_blob_into(
                &ptr1,
                Some(b"http://example.com/wrong.png"),
                &mut reusable_buf
            )
            .unwrap()
    );
    assert!(reusable_buf.is_empty());

    let mut bad_crc_ptr = ptr1;
    bad_crc_ptr.crc32c ^= 0xDEAD_BEEF;
    assert!(
        !reader
            .read_blob_into(
                &bad_crc_ptr,
                Some(b"http://example.com/a.png"),
                &mut reusable_buf
            )
            .unwrap()
    );
    assert!(reusable_buf.is_empty());

    // Unflushed / beyond-EOF pointer (simulating power loss before blob reached disk) must return Ok(None)
    let dangling_ptr = BlobPtr {
        kind: BlobKind::Standard,
        table_id: 0,
        seg_id: 1,
        offset: ptr1.len,
        len: 4096,
        crc32c: 0x1122_3344,
    };
    assert!(reader.read_blob(&dangling_ptr, None).unwrap().is_none());
    assert!(!reader.validate_blob_ptr(&dangling_ptr).unwrap());
}

#[test]
fn buffered_blob_writer_and_fast_stack_or_large_reads() {
    let dir = TempTestDir::new("buffered_blob");
    let fs = OsFs::open(&dir.path, true).unwrap();

    let mut writer = BlobSegmentWriter::open(&fs, BlobKind::Standard, 0, 1).unwrap();
    let val_small = vec![0xA5u8; 2048];
    let val_large = vec![0x5Au8; 24_000]; // exceeds 16 KB stack read threshold

    let ptr1 = writer.append_blob(b"k1", &val_small).unwrap();
    let ptr2 = writer.append_blob(b"k2", &val_large).unwrap();
    // Before `flush_buffer()`, staged bytes are in `write_buf` (`flushed_offset == 0`)
    assert_eq!(writer.flushed_offset(), 0);
    assert_eq!(writer.current_len(), (ptr1.len + ptr2.len) as u64);

    writer.flush_buffer().unwrap();
    assert_eq!(writer.flushed_offset(), writer.current_len());

    let reader = BlobSegmentReader::new(writer.file().clone(), 1);
    let mut slab = Vec::new();
    assert!(
        reader
            .append_blob_into(&ptr1, Some(b"k1"), &mut slab)
            .unwrap()
    );
    assert_eq!(slab, val_small);
    assert!(
        reader
            .append_blob_into(&ptr2, Some(b"k2"), &mut slab)
            .unwrap()
    );
    assert_eq!(&slab[..2048], val_small.as_slice());
    assert_eq!(&slab[2048..], val_large.as_slice());

    // Key mismatch must leave `slab` untouched and return `Ok(false)`
    let prev_len = slab.len();
    assert!(
        !reader
            .append_blob_into(&ptr1, Some(b"wrong"), &mut slab)
            .unwrap()
    );
    assert_eq!(slab.len(), prev_len);
    assert!(
        !reader
            .append_blob_into(&ptr2, Some(b"wrong"), &mut slab)
            .unwrap()
    );
    assert_eq!(slab.len(), prev_len);
}

#[test]
fn append_blob_into_rejects_corrupt_huge_len_without_allocating() {
    let dir = TempTestDir::new("blob_huge_len");
    let fs = OsFs::open(&dir.path, true).unwrap();

    let mut writer = BlobSegmentWriter::open(&fs, BlobKind::Standard, 0, 1).unwrap();
    let valid = writer.append_blob(b"k1", b"small-payload").unwrap();
    writer.sync_durable().unwrap();
    drop(writer);

    let reader = BlobSegmentReader::open(&fs, BlobKind::Standard, 0, 1).unwrap();
    let mut out = Vec::with_capacity(64);
    out.extend_from_slice(b"prefix:");
    let initial_cap = out.capacity();

    let corrupt_huge_len = BlobPtr {
        kind: BlobKind::Standard,
        table_id: 0,
        seg_id: 1,
        offset: 0,
        len: u32::MAX,
        crc32c: valid.crc32c,
    };
    assert!(
        !reader
            .append_blob_into(&corrupt_huge_len, Some(b"k1"), &mut out)
            .unwrap()
    );
    assert_eq!(out, b"prefix:");
    assert_eq!(out.capacity(), initial_cap);

    let corrupt_offset_overflow = BlobPtr {
        kind: BlobKind::Standard,
        table_id: 0,
        seg_id: 1,
        offset: u32::MAX,
        len: 64,
        crc32c: valid.crc32c,
    };
    assert!(
        !reader
            .append_blob_into(&corrupt_offset_overflow, Some(b"k1"), &mut out)
            .unwrap()
    );
    assert_eq!(out, b"prefix:");
    assert_eq!(out.capacity(), initial_cap);
}

#[test]
fn append_blob_flush_failure_preserves_write_offset_and_prior_buffered_records() {
    use crate::pal::test_fs::{FsHooks, HookedFs};
    use std::sync::atomic::{AtomicBool, Ordering};

    struct FaultBlobHooks {
        fail_large_write: Arc<AtomicBool>,
    }

    impl FsHooks for FaultBlobHooks {
        fn write_all_at(
            &self,
            _name: &str,
            inner: &dyn PlatformFile,
            buf: &[u8],
            offset: u64,
        ) -> io::Result<()> {
            if buf.len() >= BLOB_WRITE_BUFFER_CAPACITY
                && self.fail_large_write.swap(false, Ordering::SeqCst)
            {
                return Err(io::Error::other("simulated blob flush write failure"));
            }
            inner.write_all_at(buf, offset)
        }
    }

    let dir = TempTestDir::new("blob_flush_fail");
    let fail_flag = Arc::new(AtomicBool::new(true));
    let fs = HookedFs::new(
        OsFs::open(&dir.path, true).unwrap(),
        FaultBlobHooks {
            fail_large_write: Arc::clone(&fail_flag),
        },
    );

    let mut writer = BlobSegmentWriter::open(&fs, BlobKind::Standard, 0, 1).unwrap();
    let val1 = vec![0x11u8; 4096];
    let ptr1 = writer.append_blob(b"k1", &val1).unwrap();
    assert_eq!(writer.current_len(), ptr1.len as u64);
    assert_eq!(writer.flushed_offset(), 0);

    let big_val_len =
        BLOB_WRITE_BUFFER_CAPACITY - ptr1.len as usize - BLOB_RECORD_HEADER_LEN - b"k_big".len();
    let val_big = vec![0x22u8; big_val_len];
    let err = writer.append_blob(b"k_big", &val_big).unwrap_err();
    assert!(
        err.to_string()
            .contains("simulated blob flush write failure")
    );
    assert_eq!(writer.current_len(), ptr1.len as u64);
    assert_eq!(writer.flushed_offset(), 0);

    let val2 = vec![0x33u8; 2048];
    let ptr2 = writer.append_blob(b"k2", &val2).unwrap();
    assert_eq!(writer.current_len(), (ptr1.len + ptr2.len) as u64);
    writer.sync_durable().unwrap();
    assert_eq!(writer.flushed_offset(), writer.current_len());
    drop(writer);

    let reader = BlobSegmentReader::open(&fs, BlobKind::Standard, 0, 1).unwrap();
    assert_eq!(
        reader.read_blob(&ptr1, Some(b"k1")).unwrap().as_deref(),
        Some(val1.as_slice())
    );
    assert_eq!(
        reader.read_blob(&ptr2, Some(b"k2")).unwrap().as_deref(),
        Some(val2.as_slice())
    );
}

#[test]
fn append_blob_into_skips_file_len_syscall_on_known_range_and_still_rejects_oversized_ptr() {
    use crate::pal::test_fs::{FsHooks, HookedFs};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingLenHooks {
        file_len_calls: Arc<AtomicUsize>,
    }

    impl FsHooks for CountingLenHooks {
        fn file_len(&self, _name: &str, inner: &dyn PlatformFile) -> io::Result<u64> {
            self.file_len_calls.fetch_add(1, Ordering::SeqCst);
            inner.file_len()
        }
    }

    let dir = TempTestDir::new("blob_cached_len");
    let calls = Arc::new(AtomicUsize::new(0));
    let fs = HookedFs::new(
        OsFs::open(&dir.path, true).unwrap(),
        CountingLenHooks {
            file_len_calls: Arc::clone(&calls),
        },
    );

    let mut writer = BlobSegmentWriter::open(&fs, BlobKind::Standard, 0, 1).unwrap();
    let reader = writer.reader();
    let val = vec![0x77u8; 256];
    let ptr1 = writer.append_blob(b"k1", &val).unwrap();
    let ptr2 = writer.append_blob(b"k2", &val).unwrap();
    writer.flush_buffer().unwrap();

    // Repeated reads covered by the flushed writer length must issue 0 file_len() calls.
    calls.store(0, Ordering::SeqCst);
    let mut out = Vec::new();
    assert!(reader.read_blob_into(&ptr1, Some(b"k1"), &mut out).unwrap());
    assert_eq!(out, val);
    assert!(
        reader
            .append_blob_into(&ptr2, Some(b"k2"), &mut out)
            .unwrap()
    );
    assert_eq!(out.len(), 512);
    assert_eq!(calls.load(Ordering::SeqCst), 0);

    // A corrupt oversized ptr.len > known_file_len falls back to 1 file_len() check and returns Ok(false).
    let bad_ptr = BlobPtr {
        len: u32::MAX,
        ..ptr1
    };
    assert!(
        !reader
            .append_blob_into(&bad_ptr, Some(b"k1"), &mut out)
            .unwrap()
    );
    assert_eq!(out.len(), 512);
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    // Freshly opened reader initializes known_file_len once at open and avoids per-read calls.
    drop(reader);
    drop(writer);
    let opened = BlobSegmentReader::open(&fs, BlobKind::Standard, 0, 1).unwrap();
    calls.store(0, Ordering::SeqCst);
    assert!(opened.read_blob_into(&ptr1, Some(b"k1"), &mut out).unwrap());
    assert!(
        opened
            .append_blob_into(&ptr2, Some(b"k2"), &mut out)
            .unwrap()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[test]
fn validate_blob_ptr_streams_large_blob_in_bounded_chunks_and_detects_corruption() {
    use crate::pal::test_fs::{FsHooks, HookedFile};
    use std::sync::atomic::{AtomicU8, AtomicUsize};

    const INJECT_NONE: u8 = 0;
    const INJECT_EOF_SECOND_READ: u8 = 1;
    const INJECT_IO_ERR_SECOND_READ: u8 = 2;

    struct SpyStreamHooks {
        max_read_len: AtomicUsize,
        read_calls: AtomicUsize,
        inject_mode: AtomicU8,
    }

    impl FsHooks for SpyStreamHooks {
        fn read_exact_at(
            &self,
            _name: &str,
            inner: &dyn PlatformFile,
            buf: &mut [u8],
            offset: u64,
        ) -> io::Result<()> {
            let call = self.read_calls.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_read_len.fetch_max(buf.len(), Ordering::SeqCst);
            if call == 2 {
                match self.inject_mode.load(Ordering::SeqCst) {
                    INJECT_EOF_SECOND_READ => {
                        return Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "injected mid-stream EOF",
                        ));
                    }
                    INJECT_IO_ERR_SECOND_READ => {
                        return Err(io::Error::other("injected mid-stream I/O error"));
                    }
                    _ => {}
                }
            }
            inner.read_exact_at(buf, offset)
        }
    }

    let dir = TempTestDir::new("blob_stream_validate");
    let fs = OsFs::open(&dir.path, true).unwrap();

    let mut writer = BlobSegmentWriter::open(&fs, BlobKind::Standard, 0, 1).unwrap();
    let mut payload = vec![0u8; 256 * 1024];
    for (i, b) in payload.iter_mut().enumerate() {
        *b = ((i * 131 + 17) & 0xFF) as u8;
    }
    let ptr = writer.append_blob(b"large-key", &payload).unwrap();
    let mut long_key = vec![b'k'; 80 * 1024];
    let long_ptr = writer.append_blob(&long_key, &payload).unwrap();
    writer.sync_durable().unwrap();
    drop(writer);

    let blob_name = format_std_blob_file_name(1);
    let rw_file = fs
        .open_file(&blob_name, false, true, OpenHint::ImmutableSequential)
        .unwrap();
    let file_len = rw_file.file_len().unwrap();
    let spy = Arc::new(HookedFile::new(
        Arc::clone(&rw_file),
        blob_name,
        SpyStreamHooks {
            max_read_len: AtomicUsize::new(0),
            read_calls: AtomicUsize::new(0),
            inject_mode: AtomicU8::new(INJECT_NONE),
        },
    ));
    let reader = BlobSegmentReader::new_with_len(Arc::clone(&spy), 1, file_len);

    // Intact 256 KB blob must validate in bounded <= 64 KB chunks over multiple reads.
    assert!(reader.validate_blob_ptr(&ptr).unwrap());
    assert!(spy.hooks().max_read_len.load(Ordering::SeqCst) <= 64 * 1024);
    assert!(spy.hooks().read_calls.load(Ordering::SeqCst) >= 5);
    assert!(
        reader
            .validate_blob_ptr_for_key(&long_ptr, Some(&long_key))
            .unwrap()
    );
    *long_key.last_mut().unwrap() = b'x';
    assert!(
        !reader
            .validate_blob_ptr_for_key(&long_ptr, Some(&long_key))
            .unwrap()
    );

    // Wrong pointer CRC must fail closed cleanly.
    let bad_crc = BlobPtr {
        crc32c: ptr.crc32c ^ 1,
        ..ptr
    };
    assert!(!reader.validate_blob_ptr(&bad_crc).unwrap());

    // Mid-stream UnexpectedEof resolves to Ok(false); real I/O error propagates Err.
    spy.hooks().read_calls.store(0, Ordering::SeqCst);
    spy.hooks()
        .inject_mode
        .store(INJECT_EOF_SECOND_READ, Ordering::SeqCst);
    assert!(!reader.validate_blob_ptr(&ptr).unwrap());

    spy.hooks().read_calls.store(0, Ordering::SeqCst);
    spy.hooks()
        .inject_mode
        .store(INJECT_IO_ERR_SECOND_READ, Ordering::SeqCst);
    assert_eq!(
        reader.validate_blob_ptr(&ptr).unwrap_err().kind(),
        io::ErrorKind::Other
    );
    spy.hooks().inject_mode.store(INJECT_NONE, Ordering::SeqCst);

    // Flipping a byte near the tail of the 256 KB payload must fail streaming CRC32C check.
    let tail_off = u64::from(ptr.offset) + u64::from(ptr.len) - 7;
    let mut orig = [0u8; 1];
    rw_file.read_exact_at(&mut orig, tail_off).unwrap();
    rw_file.write_all_at(&[orig[0] ^ 0x80], tail_off).unwrap();
    assert!(!reader.validate_blob_ptr(&ptr).unwrap());

    // Restore tail byte, then corrupt magic bytes -> must also return Ok(false).
    rw_file.write_all_at(&orig, tail_off).unwrap();
    assert!(reader.validate_blob_ptr(&ptr).unwrap());
    rw_file.write_all_at(b"XXXX", ptr.offset as u64).unwrap();
    assert!(!reader.validate_blob_ptr(&ptr).unwrap());
}

#[test]
fn blob_writer_shrinks_write_buf_after_oversized_blob_flush() {
    let dir = TempTestDir::new("blob_writer_shrink");
    let fs = OsFs::open(&dir.path, true).unwrap();

    let mut writer = BlobSegmentWriter::open(&fs, BlobKind::Standard, 0, 1).unwrap();
    let huge = vec![0xCDu8; 1024 * 1024];
    let ptr_huge = writer.append_blob(b"huge", &huge).unwrap();
    assert!(writer.write_buf.is_empty());
    assert!(
        writer.write_buf.capacity() <= BLOB_WRITE_BUFFER_CAPACITY * 2,
        "oversized write_buf capacity should shrink after flush (got {})",
        writer.write_buf.capacity()
    );

    let small = vec![0x34u8; 4096];
    let ptr_small = writer.append_blob(b"small", &small).unwrap();
    writer.sync_durable().unwrap();
    drop(writer);

    let reader = BlobSegmentReader::open(&fs, BlobKind::Standard, 0, 1).unwrap();
    assert_eq!(
        reader.read_blob(&ptr_huge, Some(b"huge")).unwrap(),
        Some(huge)
    );
    assert_eq!(
        reader.read_blob(&ptr_small, Some(b"small")).unwrap(),
        Some(small)
    );
}

#[test]
fn append_blob_into_validates_header_before_reserving_payload() {
    let dir = TempTestDir::new("blob_preheader_check");
    let fs = OsFs::open(&dir.path, true).unwrap();

    let mut writer = BlobSegmentWriter::open(&fs, BlobKind::Standard, 0, 1).unwrap();
    let payload = vec![0x77u8; 512 * 1024];
    let ptr = writer.append_blob(b"good_key", &payload).unwrap();
    writer.sync_durable().unwrap();
    let rw = Arc::clone(writer.file());
    let reader = writer.reader();
    drop(writer);

    // Mismatched key length or bad CRC must reject using only the 16-byte stack header
    // without reserving `ptr.len` bytes into `out`.
    let mut out = Vec::with_capacity(64);
    assert!(!reader.append_blob_into(&ptr, Some(b"k"), &mut out).unwrap());
    assert_eq!(out.capacity(), 64);
    assert!(out.is_empty());

    let bad_crc_ptr = BlobPtr {
        crc32c: ptr.crc32c ^ 1,
        ..ptr
    };
    assert!(
        !reader
            .append_blob_into(&bad_crc_ptr, Some(b"good_key"), &mut out)
            .unwrap()
    );
    assert_eq!(out.capacity(), 64);
    assert!(out.is_empty());

    // Corrupt magic bytes on disk: pre-header validation fails without allocating 512 KB.
    rw.write_all_at(b"XXXX", ptr.offset as u64).unwrap();
    assert!(
        !reader
            .append_blob_into(&ptr, Some(b"good_key"), &mut out)
            .unwrap()
    );
    assert_eq!(out.capacity(), 64);
    assert!(out.is_empty());
}
