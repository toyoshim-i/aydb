//! Empirical microbenchmark suite for Phase 1, Step 1-4 (WiscKey KV Separation & 4 KB SSTables):
//!
//! 1. WiscKey Separation vs. Inline Storage across value sizes (128 B, 512 B, 1 KB, 2 KB):
//!    Point Read latency (1 pread vs. 2 preads), Compaction time & Write Amplification,
//!    and In-Heap Sparse Fence Pointer + Bloom Filter memory footprint.
//! 2. Multi-Segment Lookup Scaling across Bloom Filter bits-per-key (0, 6, 10, 14 bits/key)
//!    over 10 active `idx` segments.
//! 3. 4 KB Data Block In-Place Lookup: Downward-growing `u16` Offset Directory $O(\log K)$
//!    Binary Search vs. $O(K)$ Linear Scan.

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
use std::time::Instant;

use aydb::blob::{BlobSegmentReader, BlobSegmentWriter};
use aydb::index::{
    IDX_BLOCK_SIZE, IdxBlockBuilder, IdxBlockReader, IdxSegmentReader, IndexEntry, IndexValue,
    compact_idx_segments,
};
use aydb::memtable::MemTable;
use aydb::pal::OsFs;
use aydb::wal::BlobKind;

fn bench_wisckey_vs_inline(root: &PathBuf) {
    println!(
        "=========================================================================================="
    );
    println!(
        "BENCHMARK 1 (Step 1-4): WiscKey KV Separation vs. Inline Storage (4 Segments x 1,500 Keys)"
    );
    println!(
        "=========================================================================================="
    );
    println!(
        "{:<10} | {:<10} | {:>12} | {:>14} | {:>15} | {:>12}",
        "Val Size", "Mode", "Read Lat(us)", "Compact(ms)", "Compact Write", "Heap Idx(KB)"
    );
    println!("{}", "-".repeat(90));

    const NUM_SEGS: usize = 4;
    const KEYS_PER_SEG: usize = 1_500;
    const TOTAL_KEYS: usize = NUM_SEGS * KEYS_PER_SEG;
    const READ_SAMPLES: usize = 2_000;

    for &val_size in &[128usize, 512, 1024, 2048] {
        // --- Mode A: Inline inside idx_<SeqID>.db ---
        {
            let sub = root.join(format!("inline_{val_size}"));
            let _ = std::fs::remove_dir_all(&sub);
            let fs = OsFs::open(&sub, true).unwrap();

            let payload = vec![0x61u8; val_size];
            let mut segs = Vec::with_capacity(NUM_SEGS);
            for s in 0..NUM_SEGS {
                let mut mt = MemTable::new(64 * 1024 * 1024);
                let key_start = s * KEYS_PER_SEG;
                let key_end = (s + 1) * KEYS_PER_SEG;
                for k in key_start..key_end {
                    let key = format!("key_{k:06}").into_bytes();
                    let lsn = (k + 1) as u64;
                    mt.put_inline(key, payload.clone(), lsn);
                }
                segs.push(mt.flush_to_segment(&fs, (s + 1) as u64, 10).unwrap());
            }

            // Measure point read latency across all 4 segments (newest-first search)
            let t_read = Instant::now();
            for i in 0..READ_SAMPLES {
                let key = format!("key_{:06}", (i * 37) % TOTAL_KEYS);
                for seg in segs.iter().rev() {
                    if let Some(res) = seg.get(key.as_bytes()).unwrap() {
                        black_box(res);
                        break;
                    }
                }
            }
            let read_us = t_read.elapsed().as_secs_f64() * 1e6 / (READ_SAMPLES as f64);

            // Measure 4-way merge compaction
            let refs: Vec<&IdxSegmentReader<_>> = segs.iter().rev().collect();
            let t_comp = Instant::now();
            let comp = compact_idx_segments(&fs, &refs, 100, 1, 10, true).unwrap();
            let comp_ms = t_comp.elapsed().as_secs_f64() * 1e3;
            let comp_mb = (comp.new_segment.file_len() as f64) / (1024.0 * 1024.0);
            let heap_kb = (comp.new_segment.heap_bytes_used() as f64) / 1024.0;

            println!(
                "{:<10} | {:<10} | {:>9.2} us | {:>11.2} ms | {:>12.2} MB | {:>9.2} KB",
                format!("{val_size} B"),
                "Inline",
                read_us,
                comp_ms,
                comp_mb,
                heap_kb
            );
            drop(comp);
            drop(segs);
            let _ = std::fs::remove_dir_all(&sub);
        }

        // --- Mode B: WiscKey Separated (23B BlobPtr in idx + payload in blob_std) ---
        {
            let sub = root.join(format!("wisckey_{val_size}"));
            let _ = std::fs::remove_dir_all(&sub);
            let fs = OsFs::open(&sub, true).unwrap();

            let payload = vec![0x61u8; val_size];
            let mut blob_writer = BlobSegmentWriter::open(&fs, BlobKind::Standard, 0, 1).unwrap();
            let mut segs = Vec::with_capacity(NUM_SEGS);

            for s in 0..NUM_SEGS {
                let mut mt = MemTable::new(64 * 1024 * 1024);
                let key_start = s * KEYS_PER_SEG;
                let key_end = (s + 1) * KEYS_PER_SEG;
                for k in key_start..key_end {
                    let key = format!("key_{k:06}").into_bytes();
                    let lsn = (k + 1) as u64;
                    let ptr = blob_writer.append_blob(&key, &payload).unwrap();
                    mt.put_blob_ptr(key, ptr, lsn);
                }
                segs.push(mt.flush_to_segment(&fs, (s + 1) as u64, 10).unwrap());
            }
            blob_writer.sync_durable().unwrap();
            let blob_reader =
                BlobSegmentReader::new(blob_writer.file().clone(), blob_writer.seq_id());

            // Measure point read latency across all 4 segments (idx lookup + blob dereference)
            let t_read = Instant::now();
            for i in 0..READ_SAMPLES {
                let key = format!("key_{:06}", (i * 37) % TOTAL_KEYS);
                for seg in segs.iter().rev() {
                    if let Some((_lsn, IndexValue::Blob(ptr))) = seg.get(key.as_bytes()).unwrap() {
                        let val = blob_reader
                            .read_blob(&ptr, Some(key.as_bytes()))
                            .unwrap()
                            .unwrap();
                        black_box(val);
                        break;
                    }
                }
            }
            let read_us = t_read.elapsed().as_secs_f64() * 1e6 / (READ_SAMPLES as f64);

            // Measure 4-way merge compaction (copies ONLY keys + 23B BlobPtrs!)
            let refs: Vec<&IdxSegmentReader<_>> = segs.iter().rev().collect();
            let t_comp = Instant::now();
            let comp = compact_idx_segments(&fs, &refs, 100, 1, 10, true).unwrap();
            let comp_ms = t_comp.elapsed().as_secs_f64() * 1e3;
            let comp_mb = (comp.new_segment.file_len() as f64) / (1024.0 * 1024.0);
            let heap_kb = (comp.new_segment.heap_bytes_used() as f64) / 1024.0;

            println!(
                "{:<10} | {:<10} | {:>9.2} us | {:>11.2} ms | {:>12.2} MB | {:>9.2} KB",
                format!("{val_size} B"),
                "WiscKey",
                read_us,
                comp_ms,
                comp_mb,
                heap_kb
            );
            println!("{}", "-".repeat(90));
            drop(comp);
            drop(segs);
            drop(blob_reader);
            drop(blob_writer);
            let _ = std::fs::remove_dir_all(&sub);
        }
    }
}

fn bench_bloom_filter_scaling(root: &PathBuf) {
    println!(
        "\n=========================================================================================="
    );
    println!("BENCHMARK 2 (Step 1-4): Bloom Filter Bits/Key Trade-off Across 10 Active Segments");
    println!(
        "=========================================================================================="
    );
    println!(
        "{:<14} | {:>14} | {:>16} | {:>18} | {:>18}",
        "Bits / Key", "Bloom RAM (KB)", "False Pos Rate", "Miss Latency (us)", "Oldest Hit (us)"
    );
    println!("{}", "-".repeat(90));

    const NUM_SEGS: usize = 10;
    const KEYS_PER_SEG: usize = 2_000;
    const QUERIES: usize = 2_000;

    for &bpk in &[0usize, 6, 10, 14] {
        let sub = root.join(format!("bloom_{bpk}"));
        let _ = std::fs::remove_dir_all(&sub);
        let fs = OsFs::open(&sub, true).unwrap();

        let mut segs = Vec::with_capacity(NUM_SEGS);
        for s in 0..NUM_SEGS {
            let mut mt = MemTable::new(16 * 1024 * 1024);
            for k in 0..KEYS_PER_SEG {
                let key = format!("key_{k:06}_seg_{s:02}").into_bytes();
                mt.put_inline(key, vec![0x55u8; 64], (s * KEYS_PER_SEG + k + 1) as u64);
            }
            segs.push(mt.flush_to_segment(&fs, (s + 1) as u64, bpk).unwrap());
        }

        // Measure false positive rate on 10,000 absent keys against seg[0]
        let mut fp_hits = 0usize;
        let test_bloom = {
            let mut bf = aydb::index::BloomFilter::new(KEYS_PER_SEG, bpk);
            for k in 0..KEYS_PER_SEG {
                let key = format!("key_{k:06}_seg_00").into_bytes();
                bf.insert(&key);
            }
            bf
        };
        for q in 0..10_000usize {
            let absent_key = format!("key_{:06}_miss_{q:05}", q % KEYS_PER_SEG).into_bytes();
            if test_bloom.may_contain(&absent_key) {
                fp_hits += 1;
            }
        }
        let fp_pct = (fp_hits as f64) / 100.0;
        let total_bloom_kb = (test_bloom.heap_bytes() * NUM_SEGS) as f64 / 1024.0;

        // 1. Negative lookup (key falls inside every segment's fence-pointer range, but absent from all 10)
        let t_miss = Instant::now();
        for q in 0..QUERIES {
            let absent_key = format!("key_{:06}_miss_{q:05}", q % (KEYS_PER_SEG - 1));
            for seg in segs.iter().rev() {
                if let Some(v) = seg.get(absent_key.as_bytes()).unwrap() {
                    black_box(v);
                    break;
                }
            }
        }
        let miss_us = t_miss.elapsed().as_secs_f64() * 1e6 / (QUERIES as f64);

        // 2. Oldest-segment hit (must check 9 newer segments before hitting seg[0])
        let t_old = Instant::now();
        for q in 0..QUERIES {
            let old_key = format!("key_{:06}_seg_00", q % KEYS_PER_SEG);
            for seg in segs.iter().rev() {
                if let Some(v) = seg.get(old_key.as_bytes()).unwrap() {
                    black_box(v);
                    break;
                }
            }
        }
        let old_us = t_old.elapsed().as_secs_f64() * 1e6 / (QUERIES as f64);

        println!(
            "{:<14} | {:>11.2} KB | {:>15.2}% | {:>15.2} us | {:>15.2} us",
            if bpk == 0 {
                "0 (Disabled)".to_string()
            } else {
                format!("{bpk} bits/key")
            },
            total_bloom_kb,
            fp_pct,
            miss_us,
            old_us
        );

        drop(segs);
        let _ = std::fs::remove_dir_all(&sub);
    }
}

fn bench_block_offset_directory() {
    println!(
        "\n=========================================================================================="
    );
    println!(
        "BENCHMARK 3 (Step 1-4): 4 KB Index Block Lookup — Offset Binary Search vs. Linear Scan"
    );
    println!(
        "=========================================================================================="
    );

    let mut builder = IdxBlockBuilder::new();
    let mut keys = Vec::new();
    for i in 0..1000u32 {
        let entry = IndexEntry {
            key: format!("block_key_{i:05}").into_bytes(),
            lsn: (i + 1) as u64,
            value: IndexValue::Inline(vec![0x33u8; 16]),
        };
        if !builder.can_fit(&entry) {
            break;
        }
        keys.push(entry.key.clone());
        builder.add_entry(&entry).unwrap();
    }
    let count = builder.num_entries();
    let raw_block: [u8; IDX_BLOCK_SIZE] = builder.finish_and_reset();
    let reader = IdxBlockReader::open_verified(&raw_block).unwrap();

    const ITERS: usize = 200_000;
    let t_lin = Instant::now();
    for i in 0..ITERS {
        let target = &keys[i % count];
        black_box(reader.lookup_linear_scan(target).unwrap());
    }
    let lin_ns = (t_lin.elapsed().as_secs_f64() * 1e9) / (ITERS as f64);

    let t_bin = Instant::now();
    for i in 0..ITERS {
        let target = &keys[i % count];
        black_box(reader.lookup_binary_search(target).unwrap());
    }
    let bin_ns = (t_bin.elapsed().as_secs_f64() * 1e9) / (ITERS as f64);

    println!("Entries Packed in Single 4 KB Data Block : {count} entries");
    println!(
        "O(K) Linear Scan Lookup                  : {:>7.1} ns / lookup",
        lin_ns
    );
    println!(
        "O(log K) u16 Offset Directory Search     : {:>7.1} ns / lookup ({:.2}x speedup)",
        bin_ns,
        lin_ns / bin_ns
    );
}

fn main() {
    let dir = std::env::temp_dir().join(format!("aydb_wisckey_bench_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create bench dir");

    bench_wisckey_vs_inline(&dir);
    bench_bloom_filter_scaling(&dir);
    bench_block_offset_directory();

    let _ = std::fs::remove_dir_all(&dir);
}
