//! Empirical microbenchmark suite for Phase 1, Step 1-1 (PAL / VFS).
//!
//! Quantifies:
//! 1. Multi-threaded positional 4 KB read scaling and Head-of-Line Blocking:
//!    Standard `std::fs::File` (`FO_SYNCHRONOUS_IO` via `FileExt::seek_read`)
//!    vs. `aydb::pal::OsFile` (`FILE_FLAG_OVERLAPPED` + TLS Event).
//! 2. First-epoch vs. second-epoch 4 KB WAL sector write latency:
//!    `PreallocStrategy::LogicalExtendOnly` (`VDL = 0`)
//!    vs. `PreallocStrategy::PhysicalZeroFill` (`VDL = EOF`)
//!    for both `MutableSyncLog` and `MutableBufferedLog`. `MutableSyncLog` latency includes
//!    `sync_barrier`, as the WAL does.

#![allow(
    clippy::collapsible_if,
    clippy::field_reassign_with_default,
    clippy::needless_range_loop,
    clippy::ptr_arg,
    clippy::useless_vec,
    clippy::doc_overindented_list_items,
    clippy::new_without_default
)]

use std::fs::OpenOptions;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

use aydb::pal::{OpenHint, OsFs, PlatformFile, PlatformFs, PreallocStrategy};

#[cfg(windows)]
use std::os::windows::fs::FileExt;
#[cfg(unix)]
use std::os::unix::fs::FileExt;

fn percentile_us(sorted_ns: &[u64], pct: f64) -> f64 {
    if sorted_ns.is_empty() {
        return 0.0;
    }
    let idx = ((sorted_ns.len() - 1) as f64 * pct).round() as usize;
    sorted_ns[idx] as f64 / 1_000.0
}

fn bench_wal_prealloc_and_epochs(root: &PathBuf) {
    println!("========================================================================");
    println!("BENCHMARK 1: NTFS ValidDataLength (VDL) Cliff on 4 KB WAL Sector Writes");
    println!("========================================================================");

    for (label, hint, file_size_mb) in [
        ("wal_sync.db (durable per write, 2 MB)", OpenHint::MutableSyncLog, 2usize),
        ("wal_buf.db  (Buffered,     16 MB)", OpenHint::MutableBufferedLog, 16usize),
    ] {
        let total_bytes = (file_size_mb * 1024 * 1024) as u64;
        let num_sectors = (total_bytes / 4096) as usize;

        println!("\n--- Target: {label} ({num_sectors} sectors of 4 KB) ---");
        println!(
            "{:<22} | {:>12} | {:>14} {:>10} {:>10} | {:>14} {:>10} {:>10}",
            "PreallocStrategy",
            "Prealloc(ms)",
            "Epoch1 Tot(ms)",
            "E1 P50(us)",
            "E1 P99(us)",
            "Epoch2 Tot(ms)",
            "E2 P50(us)",
            "E2 P99(us)"
        );
        println!("{}", "-".repeat(118));

        for strategy in [
            PreallocStrategy::LogicalExtendOnly,
            PreallocStrategy::PhysicalZeroFill,
        ] {
            let fs = OsFs::open(root, true).expect("open OsFs");
            let fname = format!("bench_{}_{:?}.db", file_size_mb, strategy);
            let _ = fs.remove_file(&fname);

            let t0 = Instant::now();
            let file = fs.open_file(&fname, true, true, hint).expect("open wal");
            file.preallocate(total_bytes, strategy).expect("preallocate");
            let prealloc_ms = t0.elapsed().as_secs_f64() * 1_000.0;

            let sector_buf = [0x5Au8; 4096];

            // Epoch 1: First pass from 0..EOF
            let mut e1_lat = Vec::with_capacity(num_sectors);
            let e1_start = Instant::now();
            for s in 0..num_sectors {
                let st = Instant::now();
                file.write_all_at(&sector_buf, (s * 4096) as u64)
                    .expect("write e1");
                if hint == OpenHint::MutableSyncLog {
                    file.sync_barrier().expect("sync e1");
                }
                e1_lat.push(st.elapsed().as_nanos() as u64);
            }
            let e1_total_ms = e1_start.elapsed().as_secs_f64() * 1_000.0;
            e1_lat.sort_unstable();

            // Epoch 2: Second pass overwriting 0..EOF in-place (within VDL)
            let sector_buf2 = [0xA5u8; 4096];
            let mut e2_lat = Vec::with_capacity(num_sectors);
            let e2_start = Instant::now();
            for s in 0..num_sectors {
                let st = Instant::now();
                file.write_all_at(&sector_buf2, (s * 4096) as u64)
                    .expect("write e2");
                if hint == OpenHint::MutableSyncLog {
                    file.sync_barrier().expect("sync e2");
                }
                e2_lat.push(st.elapsed().as_nanos() as u64);
            }
            let e2_total_ms = e2_start.elapsed().as_secs_f64() * 1_000.0;
            e2_lat.sort_unstable();

            drop(file);
            let _ = fs.remove_file(&fname);

            println!(
                "{:<22} | {:>12.2} | {:>14.2} {:>10.2} {:>10.2} | {:>14.2} {:>10.2} {:>10.2}",
                format!("{strategy:?}"),
                prealloc_ms,
                e1_total_ms,
                percentile_us(&e1_lat, 0.50),
                percentile_us(&e1_lat, 0.99),
                e2_total_ms,
                percentile_us(&e2_lat, 0.50),
                percentile_us(&e2_lat, 0.99),
            );
        }
    }
}

fn bench_concurrent_reads(root: &PathBuf) {
    println!("\n========================================================================");
    println!("BENCHMARK 2: Concurrent Positional 4 KB Reads (FO_SYNCHRONOUS_IO vs OVERLAPPED)");
    println!("========================================================================");

    const FILE_SIZE: u64 = 16 * 1024 * 1024; // 16 MB (4096 blocks of 4 KB)
    const NUM_BLOCKS: u64 = FILE_SIZE / 4096;
    const READS_PER_THREAD: usize = 20_000;

    let fs = OsFs::open(root, true).expect("open OsFs");
    let fname = "bench_idx_16mb.db";
    let _ = fs.remove_file(fname);

    // Prepare 16 MB file and warm OS page cache
    {
        let f = fs
            .open_file(fname, true, true, OpenHint::ImmutableRandom)
            .expect("create idx");
        f.preallocate(FILE_SIZE, PreallocStrategy::PhysicalZeroFill)
            .expect("prealloc");
        let mut warm = [0u8; 4096];
        for b in 0..NUM_BLOCKS {
            f.read_exact_at(&mut warm, b * 4096).expect("warm");
        }
    }

    println!("\n--- Part 2A: Pure Hot-Cache Concurrent Reads (20,000 reads/thread) ---");
    println!(
        "{:<10} | {:>26} | {:>26} | {:>10}",
        "Threads", "std seek_read (Mops/sec)", "aydb OVERLAPPED (Mops/sec)", "Speedup"
    );
    println!("{}", "-".repeat(82));

    let full_path = root.join(fname);

    for &threads in &[1usize, 2, 4, 8, 16] {
        // 1. Standard std::fs::File (FO_SYNCHRONOUS_IO)
        let std_mops = {
            let std_file = Arc::new(
                OpenOptions::new()
                    .read(true)
                    .open(&full_path)
                    .expect("open std file"),
            );
            let barrier = Arc::new(Barrier::new(threads));
            let mut handles = Vec::with_capacity(threads);
            let start = Instant::now();
            for tid in 0..threads {
                let f = Arc::clone(&std_file);
                let bar = Arc::clone(&barrier);
                handles.push(thread::spawn(move || {
                    let mut buf = [0u8; 4096];
                    let mut state = (tid as u64 + 1) * 0x9E37_79B9_7F4A_7C15;
                    bar.wait();
                    for _ in 0..READS_PER_THREAD {
                        state ^= state >> 12;
                        state ^= state << 25;
                        state ^= state >> 27;
                        let blk = state.wrapping_mul(0x2545_F491_4F6C_DD1D) % NUM_BLOCKS;
                        #[cfg(windows)]
                        f.seek_read(&mut buf, blk * 4096).expect("seek_read");
                        #[cfg(unix)]
                        f.read_exact_at(&mut buf, blk * 4096).expect("read_exact_at");
                    }
                }));
            }
            for h in handles {
                h.join().unwrap();
            }
            let elapsed = start.elapsed().as_secs_f64();
            (threads * READS_PER_THREAD) as f64 / elapsed / 1_000_000.0
        };

        // 2. aydb OsFile (FILE_FLAG_OVERLAPPED)
        let aydb_mops = {
            let aydb_file = fs
                .open_file(fname, false, false, OpenHint::ImmutableRandom)
                .expect("open aydb file");
            let barrier = Arc::new(Barrier::new(threads));
            let mut handles = Vec::with_capacity(threads);
            let start = Instant::now();
            for tid in 0..threads {
                let f = Arc::clone(&aydb_file);
                let bar = Arc::clone(&barrier);
                handles.push(thread::spawn(move || {
                    let mut buf = [0u8; 4096];
                    let mut state = (tid as u64 + 1) * 0x9E37_79B9_7F4A_7C15;
                    bar.wait();
                    for _ in 0..READS_PER_THREAD {
                        state ^= state >> 12;
                        state ^= state << 25;
                        state ^= state >> 27;
                        let blk = state.wrapping_mul(0x2545_F491_4F6C_DD1D) % NUM_BLOCKS;
                        f.read_exact_at(&mut buf, blk * 4096)
                            .expect("read_exact_at");
                    }
                }));
            }
            for h in handles {
                h.join().unwrap();
            }
            let elapsed = start.elapsed().as_secs_f64();
            (threads * READS_PER_THREAD) as f64 / elapsed / 1_000_000.0
        };

        println!(
            "{:<10} | {:>26.2} | {:>26.2} | {:>9.2}x",
            threads,
            std_mops,
            aydb_mops,
            aydb_mops / std_mops
        );
    }

    println!("\n--- Part 2B: Head-of-Line Blocking Under Concurrent Flush/Disk Stall (8 Readers) ---");
    println!(
        "{:<28} | {:>14} | {:>14} | {:>14}",
        "Handle Implementation", "Throughput(K/s)", "P50 Lat (us)", "P99 Lat (us)"
    );
    println!("{}", "-".repeat(78));

    const HOL_READERS: usize = 8;
    const HOL_READS_PER_THREAD: usize = 5_000;

    // 1. std::fs::File under concurrent write+flush on the same handle
    {
        let std_file = Arc::new(
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(&full_path)
                .expect("open std file rw"),
        );
        let stop = Arc::new(AtomicBool::new(false));
        let f_staller = Arc::clone(&std_file);
        let stop_staller = Arc::clone(&stop);
        let staller = thread::spawn(move || {
            let page = [0x33u8; 65536];
            let mut off = 0u64;
            while !stop_staller.load(Ordering::Relaxed) {
                #[cfg(windows)]
                let _ = f_staller.seek_write(&page, off);
                #[cfg(unix)]
                let _ = f_staller.write_all_at(&page, off);
                let _ = f_staller.sync_data();
                off = (off + 65536) % FILE_SIZE;
            }
        });

        let barrier = Arc::new(Barrier::new(HOL_READERS));
        let mut handles = Vec::with_capacity(HOL_READERS);
        let start = Instant::now();
        for tid in 0..HOL_READERS {
            let f = Arc::clone(&std_file);
            let bar = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                let mut buf = [0u8; 4096];
                let mut lats = Vec::with_capacity(HOL_READS_PER_THREAD);
                let mut state = (tid as u64 + 1) * 0x9E37_79B9_7F4A_7C15;
                bar.wait();
                for _ in 0..HOL_READS_PER_THREAD {
                    state ^= state >> 12;
                    state ^= state << 25;
                    state ^= state >> 27;
                    let blk = state.wrapping_mul(0x2545_F491_4F6C_DD1D) % NUM_BLOCKS;
                    let t0 = Instant::now();
                    #[cfg(windows)]
                    f.seek_read(&mut buf, blk * 4096).expect("seek_read");
                    #[cfg(unix)]
                    f.read_exact_at(&mut buf, blk * 4096).expect("read_exact_at");
                    lats.push(t0.elapsed().as_nanos() as u64);
                }
                lats
            }));
        }

        let mut all_lats = Vec::with_capacity(HOL_READERS * HOL_READS_PER_THREAD);
        for h in handles {
            all_lats.extend(h.join().unwrap());
        }
        let elapsed = start.elapsed().as_secs_f64();
        stop.store(true, Ordering::Relaxed);
        let _ = staller.join();
        all_lats.sort_unstable();

        println!(
            "{:<28} | {:>14.1} | {:>14.2} | {:>14.2}",
            "std::fs::File (Sync Lock)",
            (HOL_READERS * HOL_READS_PER_THREAD) as f64 / elapsed / 1_000.0,
            percentile_us(&all_lats, 0.50),
            percentile_us(&all_lats, 0.99),
        );
    }

    // 2. aydb OsFile (FILE_FLAG_OVERLAPPED) under identical concurrent write+flush on the same handle
    {
        let aydb_file = fs
            .open_file(fname, false, true, OpenHint::ImmutableRandom)
            .expect("open aydb file rw");
        let stop = Arc::new(AtomicBool::new(false));
        let f_staller = Arc::clone(&aydb_file);
        let stop_staller = Arc::clone(&stop);
        let staller = thread::spawn(move || {
            let page = [0x33u8; 65536];
            let mut off = 0u64;
            while !stop_staller.load(Ordering::Relaxed) {
                let _ = f_staller.write_all_at(&page, off);
                let _ = f_staller.sync_durable();
                off = (off + 65536) % FILE_SIZE;
            }
        });

        let barrier = Arc::new(Barrier::new(HOL_READERS));
        let mut handles = Vec::with_capacity(HOL_READERS);
        let start = Instant::now();
        for tid in 0..HOL_READERS {
            let f = Arc::clone(&aydb_file);
            let bar = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                let mut buf = [0u8; 4096];
                let mut lats = Vec::with_capacity(HOL_READS_PER_THREAD);
                let mut state = (tid as u64 + 1) * 0x9E37_79B9_7F4A_7C15;
                bar.wait();
                for _ in 0..HOL_READS_PER_THREAD {
                    state ^= state >> 12;
                    state ^= state << 25;
                    state ^= state >> 27;
                    let blk = state.wrapping_mul(0x2545_F491_4F6C_DD1D) % NUM_BLOCKS;
                    let t0 = Instant::now();
                    f.read_exact_at(&mut buf, blk * 4096)
                        .expect("read_exact_at");
                    lats.push(t0.elapsed().as_nanos() as u64);
                }
                lats
            }));
        }

        let mut all_lats = Vec::with_capacity(HOL_READERS * HOL_READS_PER_THREAD);
        for h in handles {
            all_lats.extend(h.join().unwrap());
        }
        let elapsed = start.elapsed().as_secs_f64();
        stop.store(true, Ordering::Relaxed);
        let _ = staller.join();
        all_lats.sort_unstable();

        println!(
            "{:<28} | {:>14.1} | {:>14.2} | {:>14.2}",
            "aydb OsFile (OVERLAPPED)",
            (HOL_READERS * HOL_READS_PER_THREAD) as f64 / elapsed / 1_000.0,
            percentile_us(&all_lats, 0.50),
            percentile_us(&all_lats, 0.99),
        );
    }

    let _ = fs.remove_file(fname);
}

fn main() {
    let dir = std::env::temp_dir().join(format!("aydb_pal_bench_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create bench dir");

    bench_wal_prealloc_and_epochs(&dir);
    bench_concurrent_reads(&dir);

    let _ = std::fs::remove_dir_all(&dir);
}
