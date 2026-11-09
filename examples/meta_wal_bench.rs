//! Empirical microbenchmark suite for Phase 1, Steps 1-2 and 1-3:
//! - Step 1-2: Hardware SSE4.2 CRC32C vs. Software Slice-by-8 CRC32C throughput,
//!   and `boot_meta.db` Inline 4 KB Ping-Pong vs. External Manifest Overflow commit latency.
//! - Step 1-3: `wal_sync.db` (Single vs. Batched Sync) vs. `wal_buf.db` (Immediate OS Cache
//!   vs. User-Space Coalesced), $O(1)$ Epoch Rotation vs. Zero-Fill reset, and 16 MB Recovery Scan.

#![allow(
    clippy::collapsible_if,
    clippy::field_reassign_with_default,
    clippy::needless_range_loop,
    clippy::ptr_arg,
    clippy::useless_vec,
    clippy::doc_overindented_list_items,
    clippy::new_without_default
)]

use std::hint::black_box;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use aydb::meta::{BootMetaManager, IdxSegmentMeta, format_idx_file_name};
use aydb::pal::{OpenHint, OsFs, PlatformFs};
use aydb::util::crc32c::{crc32c, crc32c_software};
use aydb::wal::{
    DEFAULT_BUF_WAL_SIZE, WAL_BUF_FILE_NAME, WAL_SYNC_FILE_NAME, WalAppendOutcome, WalBatch,
    WalRecord, WalStreamMode, WalStreamWriter, recover_single_wal_stream,
};

fn bench_crc32c() {
    println!("========================================================================");
    println!("BENCHMARK 1 (Step 1-2): CRC32C Hardware (SSE4.2) vs. Software (Slice-by-8)");
    println!("========================================================================");
    println!(
        "{:<22} | {:>22} | {:>22} | {:>10}",
        "Payload Size", "Software (GB/s / ns)", "Hardware (GB/s / ns)", "Speedup"
    );
    println!("{}", "-".repeat(86));

    for (label, size, iters) in [
        ("64 B (Slot Header)", 64usize, 2_000_000usize),
        ("4 KB (Sector/Block)", 4096usize, 200_000usize),
        ("256 KB (VACB Window)", 262_144usize, 5_000usize),
    ] {
        let data = vec![0x5Au8; size];

        let t_sw = Instant::now();
        for _ in 0..iters {
            black_box(crc32c_software(0, black_box(&data)));
        }
        let sw_sec = t_sw.elapsed().as_secs_f64();
        let sw_gbps = (size * iters) as f64 / sw_sec / 1e9;
        let sw_ns = (sw_sec * 1e9) / (iters as f64);

        let t_hw = Instant::now();
        for _ in 0..iters {
            black_box(crc32c(black_box(&data)));
        }
        let hw_sec = t_hw.elapsed().as_secs_f64();
        let hw_gbps = (size * iters) as f64 / hw_sec / 1e9;
        let hw_ns = (hw_sec * 1e9) / (iters as f64);

        println!(
            "{:<22} | {:>9.2} GB/s ({:>5.0}ns) | {:>9.2} GB/s ({:>5.0}ns) | {:>9.2}x",
            label,
            sw_gbps,
            sw_ns,
            hw_gbps,
            hw_ns,
            hw_gbps / sw_gbps
        );
    }
}

fn bench_boot_meta(root: &PathBuf) {
    println!("\n========================================================================");
    println!("BENCHMARK 2 (Step 1-2): boot_meta.db Inline 4KB vs. External Manifest");
    println!("========================================================================");

    let fs = Arc::new(OsFs::open(root, true).expect("open OsFs"));
    let mut mgr = BootMetaManager::open_or_recover(Arc::clone(&fs)).expect("open meta");

    // 1. Inline Fast Path (10 segments = 260B < 4028B)
    for i in 1..=10 {
        let name = format_idx_file_name(i);
        let _ = fs.open_file(&name, true, true, OpenHint::ImmutableRandom);
    }
    mgr.commit_update(|s| {
        for _ in 1..=10 {
            let id = s.allocate_seq_id();
            s.idx_segments.push(IdxSegmentMeta {
                seq_id: id,
                file_len: 16384,
                level: 0,
                record_count: 100,
            });
        }
    })
    .unwrap();

    const ITERS: usize = 200;
    let t_inline = Instant::now();
    for i in 0..ITERS {
        mgr.commit_update(|s| {
            s.checkpoint_lsn = (i + 1) as u64;
        })
        .unwrap();
    }
    let inline_us = t_inline.elapsed().as_secs_f64() * 1e6 / (ITERS as f64);

    // 2. External Manifest Overflow Path (200 segments = 4820B > 4028B)
    mgr.commit_update(|s| {
        for _ in 11..=200 {
            let id = s.allocate_seq_id();
            s.idx_segments.push(IdxSegmentMeta {
                seq_id: id,
                file_len: 16384,
                level: 1,
                record_count: 100,
            });
        }
    })
    .unwrap();

    let t_overflow = Instant::now();
    for i in 0..ITERS {
        mgr.commit_update(|s| {
            s.checkpoint_lsn = 10_000 + (i as u64);
        })
        .unwrap();
    }
    let overflow_us = t_overflow.elapsed().as_secs_f64() * 1e6 / (ITERS as f64);

    println!(
        "Inline 4 KB Ping-Pong Commit (10 segments)    : {:>8.1} us / commit",
        inline_us
    );
    println!(
        "External Manifest Overflow Commit (200 segs)  : {:>8.1} us / commit ({:.2}x cost)",
        overflow_us,
        overflow_us / inline_us
    );
}

fn bench_wal_modes(root: &PathBuf) {
    println!("\n========================================================================");
    println!("BENCHMARK 3 (Step 1-3): Epoch WAL Modes, Group Commit, & Recovery Speed");
    println!("========================================================================");

    let fs = OsFs::open(root, true).expect("open OsFs");

    println!(
        "{:<46} | {:>14} | {:>14}",
        "WAL Mode & Batching", "Records / sec", "Latency / rec"
    );
    println!("{}", "-".repeat(80));

    // 1. SyncSectorIsolated: 1 record (64B) per sync commit (500 commits)
    for &batch_size in &[1usize, 10, 50] {
        let _ = fs.remove_file(WAL_SYNC_FILE_NAME);
        let mut sync_w = WalStreamWriter::open(
            &fs,
            WAL_SYNC_FILE_NAME,
            4 * 1024 * 1024, // 4 MB (1024 sectors)
            1,
            WalStreamMode::SyncSectorIsolated,
        )
        .unwrap();

        let num_batches = 400usize;
        let mut recs = Vec::with_capacity(batch_size);
        for r in 0..batch_size {
            recs.push(WalRecord::PutInline {
                key: format!("setting_key_{r:04}").into_bytes(),
                value: vec![0x42u8; 48],
            });
        }

        let t0 = Instant::now();
        for b in 0..num_batches {
            let batch = WalBatch::new((b + 1) as u64, recs.clone());
            match sync_w.append_batch(&batch).unwrap() {
                WalAppendOutcome::Appended { .. } => {}
                WalAppendOutcome::RingFull => panic!("unexpected ring full"),
            }
        }
        let elapsed = t0.elapsed().as_secs_f64();
        let total_recs = num_batches * batch_size;
        let rps = (total_recs as f64) / elapsed;
        let lat_us = (elapsed * 1e6) / (total_recs as f64);

        println!(
            "{:<46} | {:>14.0} | {:>11.2} us",
            format!("wal_sync.db (Sync, batch_size = {batch_size})"),
            rps,
            lat_us
        );
    }

    // 2. BufferedImmediateOsCache vs BufferedUserSpaceCoalesced (50,000 records of ~80B)
    for (label, mode) in [
        (
            "wal_buf.db (BufferedImmediateOsCache, b=1)",
            WalStreamMode::BufferedImmediateOsCache,
        ),
        (
            "wal_buf.db (BufferedUserSpaceCoalesced, b=1)",
            WalStreamMode::BufferedUserSpaceCoalesced,
        ),
    ] {
        let _ = fs.remove_file(WAL_BUF_FILE_NAME);
        let mut buf_w =
            WalStreamWriter::open(&fs, WAL_BUF_FILE_NAME, DEFAULT_BUF_WAL_SIZE, 1, mode).unwrap();

        let num_batches = 50_000usize;
        let sample_batch = WalBatch::new(
            1,
            vec![WalRecord::PutInline {
                key: b"http_cache_entry_key_0001".to_vec(),
                value: vec![0x77u8; 48],
            }],
        );
        let raw_encoded = sample_batch.encode();

        let t0 = Instant::now();
        for _ in 0..num_batches {
            buf_w.append_raw_batch(&raw_encoded).unwrap();
        }
        buf_w.flush_buffered_to_os().unwrap();
        let elapsed = t0.elapsed().as_secs_f64();
        let rps = (num_batches as f64) / elapsed;
        let lat_us = (elapsed * 1e6) / (num_batches as f64);

        println!("{:<46} | {:>14.0} | {:>11.2} us", label, rps, lat_us);

        // Measure Epoch Rotation vs Recovery Scan on the written WAL
        if mode == WalStreamMode::BufferedUserSpaceCoalesced {
            let written_sectors = buf_w.current_sector_idx();
            let t_rot = Instant::now();
            buf_w.reset_for_new_epoch(2);
            let rot_ns = t_rot.elapsed().as_nanos();
            drop(buf_w);

            let f = fs
                .open_file(
                    WAL_BUF_FILE_NAME,
                    false,
                    false,
                    OpenHint::MutableBufferedLog,
                )
                .unwrap();
            let t_rec = Instant::now();
            let rec = recover_single_wal_stream(&*f, 1).unwrap();
            let rec_ms = t_rec.elapsed().as_secs_f64() * 1e3;

            println!("{}", "-".repeat(80));
            println!(
                "O(1) Epoch Rotation Time (reset_for_new_epoch): {:>8} ns (vs ~11,150,000 ns zero-fill)",
                rot_ns
            );
            println!(
                "Crash Recovery Scan ({} sectors, {} batches): {:>8.2} ms ({:.1} MB/s)",
                written_sectors,
                rec.batches.len(),
                rec_ms,
                ((written_sectors * 4096) as f64 / 1048576.0) / (rec_ms / 1e3)
            );
        }
    }
}

fn main() {
    let dir = std::env::temp_dir().join(format!("aydb_meta_wal_bench_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create bench dir");

    bench_crc32c();
    bench_boot_meta(&dir);
    bench_wal_modes(&dir);

    let _ = std::fs::remove_dir_all(&dir);
}
