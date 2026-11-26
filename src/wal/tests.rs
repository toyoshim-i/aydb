use super::*;
use crate::pal::OsFs;
use crate::util::test_util::TempTestDir;

#[test]
fn multi_sector_fragmentation_and_truncated_tail_recovery() {
    let tmp = TempTestDir::new("frag");
    let fs = OsFs::open(&tmp.path, true).expect("open OsFs");

    // 64 KB WAL (16 sectors of 4 KB)
    let wal_size = 64 * 1024u64;
    let mut writer = WalStreamWriter::open(
        &fs,
        WAL_BUF_FILE_NAME,
        wal_size,
        1,
        WalStreamMode::BufferedImmediateOsCache,
    )
    .expect("open wal_buf");

    // Batch 1: Small batch (< 4 KB)
    let b1 = WalBatch::new(
        1,
        vec![WalRecord::PutInline {
            key: b"k1".to_vec(),
            value: b"small_val".to_vec(),
        }],
    );
    assert!(matches!(
        writer.append_batch(&b1).unwrap(),
        WalAppendOutcome::Appended { .. }
    ));

    // Batch 2: Large batch (~13 KB) spanning First -> Middle -> Middle -> Last across 4 sectors!
    let large_val = vec![0x7Au8; 13_000];
    let b2 = WalBatch::new(
        2,
        vec![
            WalRecord::PutInline {
                key: b"k_large".to_vec(),
                value: large_val.clone(),
            },
            WalRecord::Delete {
                key: b"k_old".to_vec(),
            },
        ],
    );
    let outcome = writer.append_batch(&b2).unwrap();
    assert!(matches!(
        outcome,
        WalAppendOutcome::Appended {
            sectors_touched: 4,
            ..
        }
    ));

    // Batch 3: Another multi-sector batch (~9 KB, 3 sectors) whose final sector we will corrupt!
    let b3 = WalBatch::new(
        3,
        vec![WalRecord::PutInline {
            key: b"k_torn".to_vec(),
            value: vec![0x33u8; 9_000],
        }],
    );
    writer.append_batch(&b3).unwrap();
    let last_written_sector = writer.current_sector_idx() - 1;
    drop(writer);

    // Corrupt the Last sector of Batch 3 to simulate power failure before the final sector landed!
    {
        let f = fs
            .open_file(WAL_BUF_FILE_NAME, false, true, OpenHint::MutableBufferedLog)
            .unwrap();
        f.write_all_at(
            &[0xFF, 0xFF],
            (last_written_sector * WAL_SECTOR_SIZE) as u64,
        )
        .unwrap();
    }

    // Recovery must return Batch 1 and Batch 2 intact, and cleanly discard incomplete Batch 3!
    let f = fs
        .open_file(
            WAL_BUF_FILE_NAME,
            false,
            false,
            OpenHint::MutableBufferedLog,
        )
        .unwrap();
    let rec = recover_single_wal_stream(&*f, 1).expect("recover");
    assert_eq!(rec.batches.len(), 2);
    assert_eq!(rec.batches[0], b1);
    assert_eq!(rec.batches[1], b2);
}

#[test]
fn zero_fill_free_epoch_rotation_and_ring_full() {
    let tmp = TempTestDir::new("epoch_rot");
    let fs = OsFs::open(&tmp.path, true).expect("open OsFs");

    // Tiny 4-sector (16 KB) Sync WAL
    let wal_size = (4 * WAL_SECTOR_SIZE) as u64;
    let mut writer = WalStreamWriter::open(
        &fs,
        WAL_SYNC_FILE_NAME,
        wal_size,
        1,
        WalStreamMode::SyncSectorIsolated,
    )
    .expect("open wal_sync");

    // In SyncSectorIsolated mode, each batch takes 1 sector. Write 4 batches to fill all 4 sectors.
    for lsn in 1..=4 {
        let b = WalBatch::new(
            lsn,
            vec![WalRecord::PutInline {
                key: format!("k{lsn}").into_bytes(),
                value: b"v".to_vec(),
            }],
        );
        assert_eq!(
            writer.append_batch(&b).unwrap(),
            WalAppendOutcome::Appended {
                first_sector_idx: (lsn - 1) as usize,
                sectors_touched: 1,
            }
        );
    }

    // The 5th batch must return RingFull without corrupting any sector!
    let b5 = WalBatch::new(
        5,
        vec![WalRecord::PutInline {
            key: b"k5".to_vec(),
            value: b"v".to_vec(),
        }],
    );
    assert_eq!(
        writer.append_batch(&b5).unwrap(),
        WalAppendOutcome::RingFull
    );

    // Now rotate to Epoch 2 WITHOUT zero-filling the file, and write only 2 batches (Sectors 0 and 1).
    // Sectors 2 and 3 on disk still contain valid sectors from Epoch 1!
    writer.reset_for_new_epoch(2).unwrap();
    for lsn in 10..=11 {
        let b = WalBatch::new(
            lsn,
            vec![WalRecord::PutInline {
                key: format!("epoch2_{lsn}").into_bytes(),
                value: b"v2".to_vec(),
            }],
        );
        writer.append_batch(&b).unwrap();
    }
    drop(writer);

    // Recovery for Epoch 2 must read Sectors 0 and 1, see Epoch 1 at Sector 2, and stop cleanly!
    let f = fs
        .open_file(WAL_SYNC_FILE_NAME, false, false, OpenHint::MutableSyncLog)
        .unwrap();
    let rec = recover_single_wal_stream(&*f, 2).expect("recover epoch 2");
    assert_eq!(rec.batches.len(), 2);
    assert_eq!(rec.batches[0].lsn, 10);
    assert_eq!(rec.batches[1].lsn, 11);
    assert_eq!(rec.next_sector_idx, 2);
}

/// A batch linked to the previous LSN, as in a log without gaps from transactions.
fn chained(lsn: u64, records: Vec<WalRecord>) -> WalBatch {
    let mut b = WalBatch::new(lsn, records);
    b.prev_lsn = lsn - 1;
    b
}

#[test]
fn two_stream_lsn_merge_and_write_blob_before_wal_validation() {
    let tmp = TempTestDir::new("two_stream");
    let fs = OsFs::open(&tmp.path, true).expect("open OsFs");

    let wal_size = 64 * 1024u64;
    let mut sync_w = WalStreamWriter::open(
        &fs,
        WAL_SYNC_FILE_NAME,
        wal_size,
        5,
        WalStreamMode::SyncSectorIsolated,
    )
    .unwrap();
    let mut buf_w = WalStreamWriter::open(
        &fs,
        WAL_BUF_FILE_NAME,
        wal_size,
        3,
        WalStreamMode::BufferedImmediateOsCache,
    )
    .unwrap();

    // LSN 10 (already checkpointed, <= checkpoint_lsn=10) in buf
    buf_w
        .append_batch(&chained(
            10,
            vec![WalRecord::PutInline {
                key: b"old".to_vec(),
                value: b"1".to_vec(),
            }],
        ))
        .unwrap();

    // LSN 11 in buf (valid blob seg_id=1)
    buf_w
        .append_batch(&chained(
            11,
            vec![WalRecord::PutBlobPtr {
                key: b"cache_ok".to_vec(),
                blob_ptr: BlobPtr {
                    kind: BlobKind::Lru,
                    table_id: 2,
                    seg_id: 1,
                    offset: 0,
                    len: 4096,
                    crc32c: 0x1234_5678,
                },
            }],
        ))
        .unwrap();

    // LSN 12 in sync (critical setting)
    sync_w
        .append_batch(&chained(
            12,
            vec![WalRecord::PutInline {
                key: b"theme".to_vec(),
                value: b"dark".to_vec(),
            }],
        ))
        .unwrap();

    // LSN 13 in buf (dangling blob seg_id=99 that never reached disk before power loss!)
    buf_w
        .append_batch(&chained(
            13,
            vec![WalRecord::PutBlobPtr {
                key: b"cache_unflushed".to_vec(),
                blob_ptr: BlobPtr {
                    kind: BlobKind::Lru,
                    table_id: 2,
                    seg_id: 99,
                    offset: 0,
                    len: 8192,
                    crc32c: 0xDEAD_BEEF,
                },
            }],
        ))
        .unwrap();

    // LSN 14 in sync
    sync_w
        .append_batch(&chained(
            14,
            vec![WalRecord::PutInline {
                key: b"lang".to_vec(),
                value: b"ja".to_vec(),
            }],
        ))
        .unwrap();

    drop(sync_w);
    drop(buf_w);

    let sync_f = fs
        .open_file(WAL_SYNC_FILE_NAME, false, false, OpenHint::MutableSyncLog)
        .unwrap();
    let buf_f = fs
        .open_file(
            WAL_BUF_FILE_NAME,
            false,
            false,
            OpenHint::MutableBufferedLog,
        )
        .unwrap();

    let res = recover_two_streams(&*sync_f, 5, &*buf_f, 3, 10, |ptr| Ok(ptr.seg_id == 1))
        .expect("recover two streams");

    // Expected LSN order: 11 (from buf), 12 (from sync). LSN 10 was filtered by
    // checkpoint_lsn=10. LSN 13 has a blob that never reached disk: it is the cut, so LSN 14
    // is not replayed although its own blob is fine (it may have read from 13).
    let recovered_lsns: Vec<u64> = res.merged_batches.iter().map(|b| b.lsn).collect();
    assert_eq!(recovered_lsns, vec![11, 12]);
    assert_eq!(res.max_recovered_lsn, 14);
}

#[test]
fn buffered_user_space_coalesced_flushes_tail_sector_on_drop() {
    let tmp = TempTestDir::new("coalesced_drop");
    let fs = OsFs::open(&tmp.path, true).expect("open OsFs");

    let wal_size = 64 * 1024u64;
    let mut writer = WalStreamWriter::open(
        &fs,
        WAL_BUF_FILE_NAME,
        wal_size,
        1,
        WalStreamMode::BufferedUserSpaceCoalesced,
    )
    .expect("open wal_buf coalesced");

    let batch = WalBatch::new(
        1,
        vec![WalRecord::PutInline {
            key: b"tail_key".to_vec(),
            value: b"tail_value_under_4kb".to_vec(),
        }],
    );
    writer.append_batch(&batch).unwrap();
    // Drop without calling flush_buffered_to_os() explicitly
    drop(writer);

    let f = fs
        .open_file(
            WAL_BUF_FILE_NAME,
            false,
            false,
            OpenHint::MutableBufferedLog,
        )
        .unwrap();
    let rec = recover_single_wal_stream(&*f, 1).expect("recover tail sector after drop");
    assert_eq!(rec.batches.len(), 1);
    assert_eq!(rec.batches[0], batch);
}

#[test]
fn atomic_wal_batch_rejection_when_any_blob_ptr_is_invalid() {
    let tmp = TempTestDir::new("atomic_batch_reject");
    let fs = OsFs::open(&tmp.path, true).expect("open OsFs");

    let wal_size = 64 * 1024u64;
    let sync_w = WalStreamWriter::open(
        &fs,
        WAL_SYNC_FILE_NAME,
        wal_size,
        1,
        WalStreamMode::SyncSectorIsolated,
    )
    .unwrap();
    let mut buf_w = WalStreamWriter::open(
        &fs,
        WAL_BUF_FILE_NAME,
        wal_size,
        1,
        WalStreamMode::BufferedImmediateOsCache,
    )
    .unwrap();

    // Batch 1 (LSN 1): valid batch
    buf_w
        .append_batch(&chained(
            1,
            vec![WalRecord::PutInline {
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
            }],
        ))
        .unwrap();

    // Batch 2 (LSN 2): multi-record batch where a Delete and PutInline accompany an unverified PutBlobPtr
    buf_w
        .append_batch(&chained(
            2,
            vec![
                WalRecord::Delete {
                    key: b"k1".to_vec(),
                },
                WalRecord::PutBlobPtr {
                    key: b"k2_blob".to_vec(),
                    blob_ptr: BlobPtr {
                        kind: BlobKind::Standard,
                        table_id: 1,
                        seg_id: 42,
                        offset: 0,
                        len: 2048,
                        crc32c: 0xABCD_EF01,
                    },
                },
                WalRecord::PutInline {
                    key: b"sec_idx_for_k2".to_vec(),
                    value: Vec::new(),
                },
            ],
        ))
        .unwrap();

    drop(sync_w);
    drop(buf_w);

    let sync_f = fs
        .open_file(WAL_SYNC_FILE_NAME, false, false, OpenHint::MutableSyncLog)
        .unwrap();
    let buf_f = fs
        .open_file(
            WAL_BUF_FILE_NAME,
            false,
            false,
            OpenHint::MutableBufferedLog,
        )
        .unwrap();

    let res = recover_two_streams(&*sync_f, 1, &*buf_f, 1, 0, |_ptr| Ok(false))
        .expect("recover two streams");

    // Entire Batch 2 must be discarded so companion Delete("k1") and PutInline("sec_idx_for_k2") are not applied
    assert_eq!(res.merged_batches.len(), 1);
    assert_eq!(res.merged_batches[0].lsn, 1);
}

#[test]
fn multi_chunk_transaction_rejected_atomically_when_any_chunk_blob_is_invalid() {
    let tmp = TempTestDir::new("multi_chunk_atomicity");
    let fs = OsFs::open(&tmp.path, true).expect("open OsFs");

    let wal_size = 64 * 1024u64;
    let sync_w = WalStreamWriter::open(
        &fs,
        WAL_SYNC_FILE_NAME,
        wal_size,
        1,
        WalStreamMode::SyncSectorIsolated,
    )
    .unwrap();
    let mut buf_w = WalStreamWriter::open(
        &fs,
        WAL_BUF_FILE_NAME,
        wal_size,
        1,
        WalStreamMode::BufferedImmediateOsCache,
    )
    .unwrap();

    // Chunk 1 (LSN 1): TxBegin + valid inline write
    buf_w
        .append_batch(&chained(
            1,
            vec![
                WalRecord::TxBegin,
                WalRecord::PutInline {
                    key: b"first".to_vec(),
                    value: b"value".to_vec(),
                },
            ],
        ))
        .unwrap();

    // Chunk 2 (LSN 2): PutBlobPtr (invalid/missing blob) + TxCommit
    buf_w
        .append_batch(&chained(
            2,
            vec![
                WalRecord::PutBlobPtr {
                    key: b"second".to_vec(),
                    blob_ptr: BlobPtr {
                        kind: BlobKind::Standard,
                        table_id: 1,
                        seg_id: 42,
                        offset: 0,
                        len: 2048,
                        crc32c: 0,
                    },
                },
                WalRecord::TxCommit,
            ],
        ))
        .unwrap();

    // Also verify a subsequent valid 2-chunk transaction (LSNs 3 and 4) IS recovered
    buf_w
        .append_batch(&chained(
            3,
            vec![
                WalRecord::TxBegin,
                WalRecord::PutInline {
                    key: b"valid_tx_part1".to_vec(),
                    value: b"ok1".to_vec(),
                },
            ],
        ))
        .unwrap();
    buf_w
        .append_batch(&chained(
            4,
            vec![
                WalRecord::PutBlobPtr {
                    key: b"valid_tx_part2".to_vec(),
                    blob_ptr: BlobPtr {
                        kind: BlobKind::Standard,
                        table_id: 1,
                        seg_id: 7,
                        offset: 0,
                        len: 2048,
                        crc32c: 0,
                    },
                },
                WalRecord::TxCommit,
            ],
        ))
        .unwrap();

    let recovered = recover_two_streams(&**sync_w.file(), 1, &**buf_w.file(), 1, 0, |ptr| {
        Ok(ptr.seg_id == 7)
    })
    .unwrap();

    let recovered_lsns: Vec<u64> = recovered.merged_batches.iter().map(|b| b.lsn).collect();
    assert!(
        recovered_lsns.is_empty(),
        "multi-chunk transaction [1, 2] with invalid blob in chunk 2 must be discarded as a unit,              and the buffered stream must not replay later transaction [3, 4] past it"
    );
    assert_eq!(recovered.max_recovered_lsn, 4);
}

#[test]
fn unverifiable_blob_cuts_both_streams() {
    let tmp = TempTestDir::new("buf_prefix");
    let fs = OsFs::open(&tmp.path, true).expect("open OsFs");
    let wal_size = 64 * 1024u64;
    let mut sync_w = WalStreamWriter::open(
        &fs,
        WAL_SYNC_FILE_NAME,
        wal_size,
        1,
        WalStreamMode::SyncSectorIsolated,
    )
    .unwrap();
    let mut buf_w = WalStreamWriter::open(
        &fs,
        WAL_BUF_FILE_NAME,
        wal_size,
        1,
        WalStreamMode::BufferedImmediateOsCache,
    )
    .unwrap();
    let blob = |lsn: u64, seg_id: u64| {
        chained(
            lsn,
            vec![WalRecord::PutBlobPtr {
                key: format!("k{lsn}").into_bytes(),
                blob_ptr: BlobPtr {
                    kind: BlobKind::Standard,
                    table_id: 1,
                    seg_id,
                    offset: 0,
                    len: 128,
                    crc32c: 0,
                },
            }],
        )
    };
    // Segment 1 is valid; segment 2 is the blob lost by the simulated power failure.
    buf_w.append_batch(&blob(1, 1)).unwrap();
    sync_w.append_batch(&blob(2, 2)).unwrap();
    buf_w.append_batch(&blob(3, 2)).unwrap();
    sync_w.append_batch(&blob(4, 1)).unwrap();
    buf_w.append_batch(&blob(5, 1)).unwrap();

    let recovered = recover_two_streams(&**sync_w.file(), 1, &**buf_w.file(), 1, 0, |ptr| {
        Ok(ptr.seg_id == 1)
    })
    .unwrap();
    let lsns: Vec<u64> = recovered.merged_batches.iter().map(|b| b.lsn).collect();
    // Batch 2 (sync) is the cut: neither the valid sync batch 4 nor the buffered 3 and 5.
    assert_eq!(lsns, vec![1]);
    assert_eq!(recovered.max_recovered_lsn, 5);
}

// ---- Fault-injection helpers ----

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

struct FailFile {
    inner: Arc<<OsFs as PlatformFs>::File>,
    fail_write: Arc<AtomicBool>,
    fail_sync: Arc<AtomicBool>,
    /// Bytes that reach the file before an injected write failure; 0 means one full sector.
    tear_len: Arc<AtomicUsize>,
    write_count: Arc<AtomicUsize>,
}

impl PlatformFile for FailFile {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        self.inner.read_at(buf, offset)
    }
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        self.inner.read_exact_at(buf, offset)
    }
    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        self.write_count.fetch_add(1, Ordering::SeqCst);
        if self.fail_write.swap(false, Ordering::SeqCst) {
            // Simulate a partial write: first sector lands, then error.
            let torn = self.tear_len.load(Ordering::SeqCst);
            let n = if torn == 0 {
                buf.len().min(WAL_SECTOR_SIZE)
            } else {
                buf.len().min(torn)
            };
            let _ = self.inner.write_all_at(&buf[..n], offset);
            return Err(io::Error::other("injected write failure"));
        }
        self.inner.write_all_at(buf, offset)
    }
    fn file_len(&self) -> io::Result<u64> {
        self.inner.file_len()
    }
    fn preallocate(&self, t: u64, s: PreallocStrategy) -> io::Result<()> {
        self.inner.preallocate(t, s)
    }
    fn sync_barrier(&self) -> io::Result<()> {
        if self.fail_sync.swap(false, Ordering::SeqCst) {
            return Err(io::Error::other("injected sync failure"));
        }
        self.inner.sync_barrier()
    }
    fn sync_durable(&self) -> io::Result<()> {
        self.inner.sync_durable()
    }
    fn evict_os_cache(&self, o: u64, l: u64) -> io::Result<()> {
        self.inner.evict_os_cache(o, l)
    }
}

struct FailFs {
    inner: OsFs,
    fail_write: Arc<AtomicBool>,
    fail_sync: Arc<AtomicBool>,
    tear_len: Arc<AtomicUsize>,
    write_count: Arc<AtomicUsize>,
}

impl PlatformFs for FailFs {
    type File = FailFile;
    fn open_file(
        &self,
        rel_name: &str,
        create: bool,
        writable: bool,
        hint: OpenHint,
    ) -> io::Result<Arc<FailFile>> {
        Ok(Arc::new(FailFile {
            inner: self.inner.open_file(rel_name, create, writable, hint)?,
            fail_write: self.fail_write.clone(),
            fail_sync: self.fail_sync.clone(),
            tear_len: self.tear_len.clone(),
            write_count: self.write_count.clone(),
        }))
    }
    fn remove_file(&self, n: &str) -> io::Result<()> {
        self.inner.remove_file(n)
    }
    fn list_files(&self) -> io::Result<Vec<String>> {
        self.inner.list_files()
    }
    fn root_path(&self) -> &std::path::Path {
        self.inner.root_path()
    }
}

fn put_batch(lsn: u64, key: &[u8], len: usize) -> WalBatch {
    WalBatch::new(
        lsn,
        vec![WalRecord::PutInline {
            key: key.to_vec(),
            value: vec![0x42u8; len],
        }],
    )
}

fn fail_fs(name: &str) -> (TempTestDir, FailFs, Arc<AtomicBool>, Arc<AtomicBool>) {
    let tmp = TempTestDir::new(name);
    let w = Arc::new(AtomicBool::new(false));
    let s = Arc::new(AtomicBool::new(false));
    let fs = FailFs {
        inner: OsFs::open(&tmp.path, true).expect("open OsFs"),
        fail_write: w.clone(),
        fail_sync: s.clone(),
        tear_len: Arc::new(AtomicUsize::new(0)),
        write_count: Arc::new(AtomicUsize::new(0)),
    };
    (tmp, fs, w, s)
}

fn recover_lsns(fs: &FailFs, name: &str, epoch: u64) -> Vec<u64> {
    let f = fs
        .open_file(name, false, false, OpenHint::MutableSyncLog)
        .unwrap();
    recover_single_wal_stream(&*f, epoch)
        .unwrap()
        .batches
        .iter()
        .map(|b| b.lsn)
        .collect()
}

#[test]
fn sync_multi_sector_write_failure_leaves_no_hole() {
    let (_tmp, fs, fail_w, _) = fail_fs("wal_fail_sync_multi");
    let mut w = WalStreamWriter::open(
        &fs,
        WAL_SYNC_FILE_NAME,
        64 * 1024,
        1,
        WalStreamMode::SyncSectorIsolated,
    )
    .unwrap();
    w.append_batch(&put_batch(1, b"a", 10)).unwrap();
    let before = w.current_sector_idx();
    fail_w.store(true, Ordering::SeqCst);
    assert!(w.append_batch(&put_batch(2, b"big", 13_000)).is_err());
    assert_eq!(w.current_sector_idx(), before, "cursor must be restored");
    w.append_batch(&put_batch(3, b"c", 10)).unwrap();
    drop(w);
    assert_eq!(recover_lsns(&fs, WAL_SYNC_FILE_NAME, 1), vec![1, 3]);
}

#[test]
fn sync_barrier_failure_restores_cursor() {
    let (_tmp, fs, _, fail_s) = fail_fs("wal_fail_barrier");
    let mut w = WalStreamWriter::open(
        &fs,
        WAL_SYNC_FILE_NAME,
        64 * 1024,
        1,
        WalStreamMode::SyncSectorIsolated,
    )
    .unwrap();
    w.append_batch(&put_batch(1, b"a", 10)).unwrap();
    let before = w.current_sector_idx();
    fail_s.store(true, Ordering::SeqCst);
    assert!(w.append_batch(&put_batch(2, b"b", 10)).is_err());
    assert_eq!(w.current_sector_idx(), before);
    w.append_batch(&put_batch(3, b"c", 10)).unwrap();
    drop(w);
    assert_eq!(recover_lsns(&fs, WAL_SYNC_FILE_NAME, 1), vec![1, 3]);
}

#[test]
fn sealed_sector_survives_torn_rewrite_of_following_batch() {
    for mode in [
        WalStreamMode::BufferedImmediateOsCache,
        WalStreamMode::BufferedUserSpaceCoalesced,
    ] {
        let (_tmp, fs, _, _) = fail_fs("wal_seal_torn");
        let mut w = WalStreamWriter::open(&fs, WAL_BUF_FILE_NAME, 64 * 1024, 1, mode).unwrap();
        w.append_batch(&put_batch(1, b"a", 10)).unwrap();
        w.make_durable_and_seal(false).unwrap();
        assert_eq!(w.current_sector_idx(), 1);
        let out = w.append_batch(&put_batch(2, b"b", 10)).unwrap();
        assert_eq!(
            out,
            WalAppendOutcome::Appended {
                first_sector_idx: 1,
                sectors_touched: 1
            }
        );
        w.flush_buffered_to_os().unwrap();
        drop(w);
        assert_eq!(recover_lsns(&fs, WAL_BUF_FILE_NAME, 1), vec![1, 2]);

        // Tear the sector holding B: only A (sealed in sector 0) must remain.
        let f = fs
            .open_file(WAL_BUF_FILE_NAME, false, true, OpenHint::MutableBufferedLog)
            .unwrap();
        f.write_all_at(&[0xFFu8; 64], WAL_SECTOR_SIZE as u64 + 100)
            .unwrap();
        drop(f);
        assert_eq!(recover_lsns(&fs, WAL_BUF_FILE_NAME, 1), vec![1]);
    }
}

#[test]
fn unsealed_buffered_sector_is_shared_between_batches() {
    let (_tmp, fs, _, _) = fail_fs("wal_unsealed_shared");
    let mut w = WalStreamWriter::open(
        &fs,
        WAL_BUF_FILE_NAME,
        64 * 1024,
        1,
        WalStreamMode::BufferedImmediateOsCache,
    )
    .unwrap();
    w.append_batch(&put_batch(1, b"a", 10)).unwrap();
    w.append_batch(&put_batch(2, b"b", 10)).unwrap();
    assert_eq!(w.current_sector_idx(), 0);
    // Sealing an empty open sector is a no-op.
    let mut w2 = WalStreamWriter::open(
        &fs,
        "wal_buf_other.db",
        64 * 1024,
        1,
        WalStreamMode::BufferedImmediateOsCache,
    )
    .unwrap();
    w2.seal_open_sector_for_durability().unwrap();
    assert_eq!(w2.current_sector_idx(), 0);
}

#[test]
fn seal_at_end_of_ring_yields_ring_full() {
    let (_tmp, fs, _, _) = fail_fs("wal_seal_ring_end");
    let mut w = WalStreamWriter::open(
        &fs,
        WAL_BUF_FILE_NAME,
        WAL_SECTOR_SIZE as u64,
        1,
        WalStreamMode::BufferedImmediateOsCache,
    )
    .unwrap();
    w.append_batch(&put_batch(1, b"a", 10)).unwrap();
    w.make_durable_and_seal(true).unwrap();
    assert_eq!(w.current_sector_idx(), w.total_sectors());
    assert_eq!(
        w.append_batch(&put_batch(2, b"b", 10)).unwrap(),
        WalAppendOutcome::RingFull
    );
    drop(w);
    assert_eq!(recover_lsns(&fs, WAL_BUF_FILE_NAME, 1), vec![1]);
}

#[test]
fn torn_buffered_append_keeps_earlier_batches_in_the_open_sector() {
    let (_tmp, fs, fail_w, _) = fail_fs("wal_fail_buffered_torn");
    let mut w = WalStreamWriter::open(
        &fs,
        WAL_BUF_FILE_NAME,
        64 * 1024,
        1,
        WalStreamMode::BufferedImmediateOsCache,
    )
    .unwrap();
    w.append_batch(&put_batch(1, b"a", 10)).unwrap();
    // The failed append rewrites the open sector and only half of it reaches the file.
    w.file()
        .tear_len
        .store(WAL_SECTOR_SIZE / 2, Ordering::SeqCst);
    fail_w.store(true, Ordering::SeqCst);
    assert!(w.append_batch(&put_batch(2, b"torn", 3000)).is_err());
    // Dropping flushes nothing: the open sector was already written by each append.
    drop(w);
    assert_eq!(recover_lsns(&fs, WAL_BUF_FILE_NAME, 1), vec![1]);
}

#[test]
fn buffered_write_failure_does_not_resurrect_failed_batch() {
    let (_tmp, fs, fail_w, _) = fail_fs("wal_fail_buffered");
    let mut w = WalStreamWriter::open(
        &fs,
        WAL_BUF_FILE_NAME,
        64 * 1024,
        1,
        WalStreamMode::BufferedImmediateOsCache,
    )
    .unwrap();
    w.append_batch(&put_batch(1, b"a", 10)).unwrap();
    fail_w.store(true, Ordering::SeqCst);
    assert!(w.append_batch(&put_batch(2, b"failed", 10)).is_err());
    w.append_batch(&put_batch(3, b"c", 10)).unwrap();
    drop(w);
    assert_eq!(recover_lsns(&fs, WAL_BUF_FILE_NAME, 1), vec![1, 3]);
}

fn linked_batch() -> WalBatch {
    let mut b = put_batch(7, b"k", 20);
    b.prev_lsn = 6;
    b.durable_lsn = 4;
    b
}

#[test]
fn batch_round_trips_prev_and_durable_lsn() {
    let b = linked_batch();
    let enc = b.encode();
    assert_eq!(&enc[16..24], &6u64.to_le_bytes());
    assert_eq!(&enc[24..32], &4u64.to_le_bytes());
    assert_eq!(WalBatch::decode(&enc).unwrap(), b);
    // `WalBatch::new` leaves both at 0 until the engine tracks them.
    let plain = put_batch(1, b"k", 1);
    assert_eq!((plain.prev_lsn, plain.durable_lsn), (0, 0));
    assert_eq!(WalBatch::decode(&plain.encode()).unwrap(), plain);
}

#[test]
fn batch_crc_covers_record_count_prev_lsn_and_durable_lsn() {
    let enc = linked_batch().encode();
    assert_eq!(enc.len(), WAL_BATCH_HEADER_SIZE + 1 + 4 + 1 + 4 + 20);
    // RecordCount (12..16), PrevLsn (16..24), DurableLsn (24..32) and the records.
    for pos in [12, 16, 23, 24, 31, 32, enc.len() - 1] {
        let mut bad = enc.clone();
        bad[pos] ^= 0x01;
        let err = WalBatch::decode(&bad).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData, "byte {pos}");
        assert!(err.to_string().contains("CRC32C"), "byte {pos}: {err}");
    }
}

/// Rewrites the format version of sector `idx` and re-seals its CRC, as an old or future
/// writer would have left it.
fn set_sector_version(fs: &FailFs, name: &str, idx: usize, version: u16) {
    let f = fs
        .open_file(name, false, true, OpenHint::MutableBufferedLog)
        .unwrap();
    let off = (idx * WAL_SECTOR_SIZE) as u64;
    let mut sec = vec![0u8; WAL_SECTOR_SIZE];
    f.read_exact_at(&mut sec, off).unwrap();
    sec[14..16].copy_from_slice(&version.to_le_bytes());
    let crc = crc32c(&sec[4..]);
    sec[0..4].copy_from_slice(&crc.to_le_bytes());
    f.write_all_at(&sec, off).unwrap();
}

fn recover_stream(fs: &FailFs, name: &str, epoch: u64) -> SingleStreamRecovery {
    let f = fs
        .open_file(name, false, false, OpenHint::MutableSyncLog)
        .unwrap();
    recover_single_wal_stream(&*f, epoch).unwrap()
}

#[test]
fn sectors_carry_the_current_format_version() {
    let (_tmp, fs, _, _) = fail_fs("wal_version_written");
    let mut w = WalStreamWriter::open(
        &fs,
        WAL_SYNC_FILE_NAME,
        64 * 1024,
        1,
        WalStreamMode::SyncSectorIsolated,
    )
    .unwrap();
    w.append_batch(&put_batch(1, b"a", 10)).unwrap();
    drop(w);
    let f = fs
        .open_file(WAL_SYNC_FILE_NAME, false, false, OpenHint::MutableSyncLog)
        .unwrap();
    let mut sec = vec![0u8; WAL_SECTOR_SIZE];
    f.read_exact_at(&mut sec, 0).unwrap();
    drop(f);
    assert_eq!(&sec[14..16], &WAL_FORMAT_VERSION.to_le_bytes());
    assert_eq!(
        recover_stream(&fs, WAL_SYNC_FILE_NAME, 1).unsupported_format,
        None
    );
}

#[test]
fn current_epoch_sector_with_other_version_is_unsupported_format() {
    for version in [0u16, 1] {
        let (_tmp, fs, _, _) = fail_fs("wal_version_current");
        let mut w = WalStreamWriter::open(
            &fs,
            WAL_SYNC_FILE_NAME,
            64 * 1024,
            1,
            WalStreamMode::SyncSectorIsolated,
        )
        .unwrap();
        for lsn in 1..=3 {
            w.append_batch(&put_batch(lsn, b"a", 10)).unwrap();
        }
        drop(w);
        set_sector_version(&fs, WAL_SYNC_FILE_NAME, 1, version);

        let rec = recover_stream(&fs, WAL_SYNC_FILE_NAME, 1);
        assert_eq!(
            rec.unsupported_format,
            Some(UnsupportedWalFormat {
                sector_idx: 1,
                version
            })
        );
        // The stream stops at the unreadable sector, as for a bad CRC.
        assert_eq!(
            rec.batches.iter().map(|b| b.lsn).collect::<Vec<_>>(),
            vec![1]
        );
        assert_eq!(rec.next_sector_idx, 1);
    }
}

#[test]
fn stale_epoch_sector_with_any_version_is_ignored_silently() {
    for version in [0u16, 1, 2, 0xFFFF] {
        let (_tmp, fs, _, _) = fail_fs("wal_version_stale");
        let mut w = WalStreamWriter::open(
            &fs,
            WAL_SYNC_FILE_NAME,
            64 * 1024,
            1,
            WalStreamMode::SyncSectorIsolated,
        )
        .unwrap();
        w.append_batch(&put_batch(1, b"a", 10)).unwrap();
        drop(w);
        set_sector_version(&fs, WAL_SYNC_FILE_NAME, 0, version);

        // Epoch 2 is current now; the epoch-1 sector is a leftover.
        let rec = recover_stream(&fs, WAL_SYNC_FILE_NAME, 2);
        assert_eq!(rec.unsupported_format, None, "version {version}");
        assert!(rec.batches.is_empty());
        assert_eq!(rec.next_sector_idx, 0);
    }
}

#[test]
fn make_durable_and_seal_coalesced_writes_sector_once() {
    let (_tmp, fs, _, _) = fail_fs("ph7_coalesced_single_write");
    let mut w = WalStreamWriter::open(
        &fs,
        WAL_BUF_FILE_NAME,
        64 * 1024,
        1,
        WalStreamMode::BufferedUserSpaceCoalesced,
    )
    .unwrap();
    fs.write_count.store(0, Ordering::SeqCst);

    for lsn in 1..=3 {
        w.append_batch(&put_batch(lsn, b"k", 32)).unwrap();
    }
    assert_eq!(
        fs.write_count.load(Ordering::SeqCst),
        0,
        "coalesced small appends must stay purely in RAM"
    );

    w.make_durable_and_seal(true).unwrap();
    assert_eq!(
        fs.write_count.load(Ordering::SeqCst),
        1,
        "make_durable_and_seal in coalesced mode must write the sector once, not twice"
    );

    for lsn in 4..=5 {
        w.append_batch(&put_batch(lsn, b"k", 32)).unwrap();
    }
    w.flush_buffered_to_os().unwrap();
    assert_eq!(fs.write_count.load(Ordering::SeqCst), 2);
    w.make_durable_and_seal(true).unwrap();
    assert_eq!(
        fs.write_count.load(Ordering::SeqCst),
        2,
        "seal after flush_buffered_to_os must not rewrite an unmodified sector"
    );
    drop(w);

    assert_eq!(recover_lsns(&fs, WAL_BUF_FILE_NAME, 1), vec![1, 2, 3, 4, 5]);
}

#[test]
fn partial_sector_backup_and_dirty_end_zeroing_preserve_torn_write_recovery() {
    for (mode, fail_payload_len) in [
        (WalStreamMode::BufferedImmediateOsCache, 500),
        (WalStreamMode::BufferedUserSpaceCoalesced, 4000),
    ] {
        let (_tmp, fs, fail_w, _) = fail_fs("ph7_partial_backup_dirty_end");
        let mut w = WalStreamWriter::open(&fs, WAL_BUF_FILE_NAME, 64 * 1024, 1, mode).unwrap();

        w.append_batch(&put_batch(1, b"k1", 32)).unwrap();
        w.append_batch(&put_batch(2, b"k2", 32)).unwrap();
        let saved_valid = w.current_valid_bytes;
        assert!(saved_valid > 0 && saved_valid < 512);

        w.file()
            .tear_len
            .store(WAL_SECTOR_SIZE / 2, Ordering::SeqCst);
        fail_w.store(true, Ordering::SeqCst);
        assert!(
            w.append_batch(&put_batch(3, b"fail", fail_payload_len))
                .is_err()
        );
        assert_eq!(w.current_valid_bytes, saved_valid);
        assert!(
            w.sector_buf[WAL_SECTOR_HEADER_SIZE + saved_valid..WAL_SECTOR_SIZE]
                .iter()
                .all(|&b| b == 0)
        );
        let mid_rec = recover_single_wal_stream(&**w.file(), 1).unwrap();
        let mid_lsns: Vec<u64> = mid_rec.batches.iter().map(|b| b.lsn).collect();
        assert_eq!(mid_lsns, vec![1, 2]);

        w.append_batch(&put_batch(4, b"k4", 20)).unwrap();
        w.make_durable_and_seal(true).unwrap();
        drop(w);
        assert_eq!(recover_lsns(&fs, WAL_BUF_FILE_NAME, 1), vec![1, 2, 4]);
    }
}

#[test]
fn wal_writer_rejects_invalid_size_and_non_monotonic_epoch_without_panicking() {
    let tmp = TempTestDir::new("wal_invalid_args");
    let fs = OsFs::open(&tmp.path, true).expect("open OsFs");

    assert!(
        WalStreamWriter::open_existing_unallocated(
            &fs,
            WAL_SYNC_FILE_NAME,
            64 * 1024,
            1,
            WalStreamMode::SyncSectorIsolated,
        )
        .unwrap()
        .is_none(),
        "open_existing_unallocated must return None without creating missing file"
    );
    assert!(fs.list_files().unwrap().is_empty());

    for bad_size in [0u64, 512, 4097] {
        let err = match WalStreamWriter::open_unallocated(
            &fs,
            WAL_SYNC_FILE_NAME,
            bad_size,
            1,
            WalStreamMode::SyncSectorIsolated,
        ) {
            Ok(_) => panic!("invalid WAL size {bad_size} must return Err"),
            Err(e) => e,
        };
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    let mut writer = WalStreamWriter::open_unallocated(
        &fs,
        WAL_SYNC_FILE_NAME,
        64 * 1024,
        5,
        WalStreamMode::SyncSectorIsolated,
    )
    .unwrap();
    for bad_epoch in [0u64, 4, 5] {
        let err = writer.reset_for_new_epoch(bad_epoch).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }
    assert_eq!(writer.active_epoch(), 5);
    writer.reset_for_new_epoch(6).unwrap();
    assert_eq!(writer.active_epoch(), 6);

    let err = writer.ensure_min_size(u64::MAX).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
}

#[test]
fn set_cursor_after_recovery_preserves_grown_wal_position() {
    let tmp = TempTestDir::new("wal_grown_cursor");
    let fs = OsFs::open(&tmp.path, true).expect("open OsFs");

    {
        let mut w = WalStreamWriter::open(
            &fs,
            WAL_SYNC_FILE_NAME,
            8 * WAL_SECTOR_SIZE as u64,
            1,
            WalStreamMode::SyncSectorIsolated,
        )
        .unwrap();
        w.ensure_min_size(16 * WAL_SECTOR_SIZE as u64).unwrap();
        for lsn in 1..=12u64 {
            w.append_batch(&put_batch(lsn, b"k", 10)).unwrap();
        }
        assert_eq!(w.current_sector_idx(), 12);
    }

    // Reopen with the original smaller target_size (8 sectors); recovery reaches sector 12.
    let mut w = WalStreamWriter::open_existing_unallocated(
        &fs,
        WAL_SYNC_FILE_NAME,
        8 * WAL_SECTOR_SIZE as u64,
        1,
        WalStreamMode::SyncSectorIsolated,
    )
    .unwrap()
    .expect("file exists");
    let rec = recover_single_wal_stream(&**w.file(), 1).unwrap();
    assert_eq!(rec.next_sector_idx, 12);
    w.set_cursor_after_recovery(rec.next_sector_idx);
    assert_eq!(w.current_sector_idx(), 12);
    assert!(w.total_sectors() >= 16);

    w.append_batch(&put_batch(13, b"k", 10)).unwrap();
    drop(w);
    let f = fs
        .open_file(WAL_SYNC_FILE_NAME, false, false, OpenHint::MutableSyncLog)
        .unwrap();
    let rec = recover_single_wal_stream(&*f, 1).unwrap();
    let lsns: Vec<u64> = rec.batches.iter().map(|b| b.lsn).collect();
    assert_eq!(lsns, (1..=13).collect::<Vec<_>>());
}

#[test]
fn wal_bounds_reject_above_i64_max_and_preview_saturates_without_overflow() {
    let tmp = TempTestDir::new("wal_bounds_overflow");
    let fs = OsFs::open(&tmp.path, true).expect("open OsFs");

    let above_i64 = ((i64::MAX as u64) / (WAL_SECTOR_SIZE as u64) + 1) * (WAL_SECTOR_SIZE as u64);
    assert_eq!(
        WalStreamWriter::<crate::pal::OsFile>::validate_wal_target_size(above_i64)
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );

    let mut w = WalStreamWriter::open_unallocated(
        &fs,
        WAL_SYNC_FILE_NAME,
        8 * WAL_SECTOR_SIZE as u64,
        1,
        WalStreamMode::SyncSectorIsolated,
    )
    .unwrap();
    assert_eq!(
        w.ensure_min_size((i64::MAX as u64) + 1).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );

    // Near-`usize::MAX` sector cursor must return `None` rather than wrapping in release mode.
    w.total_sectors = usize::MAX;
    w.current_sector_idx = usize::MAX;
    w.current_valid_bytes = WAL_SECTOR_PAYLOAD_CAP;
    assert_eq!(w.preview_end_sector(64), None);

    // Checked encoding path matches `encode()` across all WalRecord variants.
    let batch = WalBatch::new(
        7,
        vec![
            WalRecord::TxBegin,
            WalRecord::PutInline {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
            },
            WalRecord::PutBlobPtr {
                key: b"bk".to_vec(),
                blob_ptr: BlobPtr {
                    kind: BlobKind::Standard,
                    table_id: 0,
                    seg_id: 3,
                    offset: 64,
                    len: 2048,
                    crc32c: 0x1234_5678,
                },
            },
            WalRecord::Delete {
                key: b"dk".to_vec(),
            },
            WalRecord::TxCommit,
        ],
    );
    let mut checked = Vec::new();
    batch.try_encode_into(&mut checked).unwrap();
    assert_eq!(checked, batch.encode());
}
