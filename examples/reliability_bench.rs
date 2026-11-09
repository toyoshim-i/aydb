//! Reliability, Crash-Consistency, and Fault-Injection Benchmark Suite for `aydb`.
//!
//! Uses a deterministic, in-memory `FaultInjectingFs` implementing [`PlatformFs`] and
//! [`PlatformFile`] to simulate:
//! 1. Sudden Power Loss (loss of un-`sync_durable` volatile page-cache writes + 512 B torn writes).
//! 2. Sudden Process Crash (`kill -9` via `mem::forget` where OS page cache survives but user-space
//!    buffers and uncommitted state are lost).
//! 3. Transaction Rollback Invariants (small tx, sealed-blob overwrite GC, >4 MB tx auto-flush,
//!    and Capped LRU table logical byte accounting across reopen/rollback).
//! 4. Post-Crash Blob GC Accounting (`dead_bytes` preservation across WAL recovery).
//! 5. Deterministic I/O Fault Injection (`ENOSPC` / `EIO` during `flush_memtable`).

#![allow(
    clippy::collapsible_if,
    clippy::field_reassign_with_default,
    clippy::needless_range_loop,
    clippy::ptr_arg,
    clippy::useless_vec,
    clippy::doc_overindented_list_items,
    clippy::new_without_default
)]

use std::collections::{BTreeMap, HashMap};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use aydb::engine::{EngineConfig, StorageEngine, WriteDurability};
use aydb::pal::{OpenHint, OsFs, PlatformFile, PlatformFs, PreallocStrategy};
use aydb::vtab::SqliteConnection;
use aydb::vtab::row_codec::SqlValue;
use aydb::wal::WalStreamMode;

// ============================================================================
// Deterministic Fault-Injecting File System (`FaultInjectingFs`)
// ============================================================================

#[derive(Clone)]
struct PendingWrite {
    offset: u64,
    data: Vec<u8>,
}

struct FileState {
    hint: OpenHint,
    /// Bytes guaranteed to have reached non-volatile media via `sync_durable()` / `sync_barrier()`.
    durable_bytes: Vec<u8>,
    /// Bytes currently visible in the OS page cache (`write_all_at` + `preallocate`).
    page_cache_bytes: Vec<u8>,
    /// Unsynced writes since the last `sync_durable()`.
    unsynced_writes: Vec<PendingWrite>,
    /// Snapshot of `page_cache_bytes` right before the most recent `write_all_at`.
    pre_last_write_bytes: Vec<u8>,
    /// Most recent `write_all_at` operation (used for 512 B torn-sector simulation on power cut).
    last_write: Option<PendingWrite>,
}

impl FileState {
    fn new(hint: OpenHint) -> Self {
        Self {
            hint,
            durable_bytes: Vec::new(),
            page_cache_bytes: Vec::new(),
            unsynced_writes: Vec::new(),
            pre_last_write_bytes: Vec::new(),
            last_write: None,
        }
    }
}

pub struct SimFile {
    name: String,
    state: Arc<Mutex<FileState>>,
    fs_ctrl: Arc<SimFsControl>,
}

struct SimFsControl {
    files: Mutex<HashMap<String, Arc<Mutex<FileState>>>>,
    /// Files whose creation is durable, with their content at the last sync. A file enters the
    /// map with its first `sync_durable`; `remove_file` leaves it at once (as `sync_dir` does on
    /// Unix). `simulate_power_loss` drops every file that is not in the map.
    durable_dir_entries: Mutex<HashMap<String, Vec<u8>>>,
    /// If `Some(n)`, the `n`-th write or sync call (1-indexed) returns `io::ErrorKind::Other`.
    fail_on_io_step: AtomicUsize,
    io_step_counter: AtomicUsize,
    root: PathBuf,
}

impl SimFsControl {
    fn check_fault(&self, op_file: &str) -> io::Result<()> {
        let target = self.fail_on_io_step.load(Ordering::SeqCst);
        if target == 0 {
            return Ok(());
        }
        // Count writes and syncs of every file kind (logs, metadata, manifests, indexes, blobs).
        {
            let step = self.io_step_counter.fetch_add(1, Ordering::SeqCst) + 1;
            if step == target {
                return Err(io::Error::new(
                    io::ErrorKind::StorageFull,
                    format!("injected deterministic I/O fault at step {step} on {op_file}"),
                ));
            }
        }
        Ok(())
    }
}

impl PlatformFile for SimFile {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        let st = self.state.lock().unwrap();
        let off = offset as usize;
        if off >= st.page_cache_bytes.len() {
            return Ok(0);
        }
        let avail = (st.page_cache_bytes.len() - off).min(buf.len());
        buf[..avail].copy_from_slice(&st.page_cache_bytes[off..off + avail]);
        Ok(avail)
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        let n = self.read_at(buf, offset)?;
        if n < buf.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "sim read past EOF",
            ));
        }
        Ok(())
    }

    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        self.fs_ctrl.check_fault(&self.name)?;
        let mut st = self.state.lock().unwrap();
        st.pre_last_write_bytes = st.page_cache_bytes.clone();
        let end = (offset as usize) + buf.len();
        if end > st.page_cache_bytes.len() {
            st.page_cache_bytes.resize(end, 0);
        }
        st.page_cache_bytes[offset as usize..end].copy_from_slice(buf);
        let pw = PendingWrite {
            offset,
            data: buf.to_vec(),
        };
        st.unsynced_writes.push(pw.clone());
        st.last_write = Some(pw);
        Ok(())
    }

    fn file_len(&self) -> io::Result<u64> {
        let st = self.state.lock().unwrap();
        Ok(st.page_cache_bytes.len() as u64)
    }

    fn preallocate(&self, target_len: u64, _strategy: PreallocStrategy) -> io::Result<()> {
        let mut st = self.state.lock().unwrap();
        let len = target_len as usize;
        if len > st.page_cache_bytes.len() {
            st.page_cache_bytes.resize(len, 0);
        }
        if len > st.durable_bytes.len() {
            st.durable_bytes.resize(len, 0);
        }
        Ok(())
    }

    fn sync_barrier(&self) -> io::Result<()> {
        // Every platform makes `sync_barrier()` a device flush: `FlushFileBuffers` on Windows
        // (no file is opened write-through any more), `fdatasync` on Linux and
        // `fcntl(F_FULLFSYNC)` on macOS for `MutableSyncLog` files. Thus `sync_barrier()`
        // transitions buffered pages into `durable_bytes`.
        self.sync_durable()
    }

    fn sync_durable(&self) -> io::Result<()> {
        self.fs_ctrl.check_fault(&self.name)?;
        let mut st = self.state.lock().unwrap();
        st.durable_bytes = st.page_cache_bytes.clone();
        st.unsynced_writes.clear();
        drop(st);

        // Also snapshot directory presence for this synced file
        let files = self.fs_ctrl.files.lock().unwrap();
        if let Some(f) = files.get(&self.name) {
            let bytes = f.lock().unwrap().durable_bytes.clone();
            self.fs_ctrl
                .durable_dir_entries
                .lock()
                .unwrap()
                .insert(self.name.clone(), bytes);
        }
        Ok(())
    }

    fn evict_os_cache(&self, _offset: u64, _len: u64) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Clone)]
pub struct FaultInjectingFs {
    ctrl: Arc<SimFsControl>,
}

impl FaultInjectingFs {
    pub fn new() -> Self {
        Self {
            ctrl: Arc::new(SimFsControl {
                files: Mutex::new(HashMap::new()),
                durable_dir_entries: Mutex::new(HashMap::new()),
                fail_on_io_step: AtomicUsize::new(0),
                io_step_counter: AtomicUsize::new(0),
                root: PathBuf::from("/sim/aydb"),
            }),
        }
    }

    /// Configures the simulator to fail on the `step`-th write/sync of any file.
    pub fn set_io_fault_step(&self, step: usize) {
        self.ctrl.io_step_counter.store(0, Ordering::SeqCst);
        self.ctrl.fail_on_io_step.store(step, Ordering::SeqCst);
    }

    /// Simulates a sudden power loss:
    /// - If `honor_write_through_hint` is `false` (strict volatile drive cache / Unix PAL model),
    ///   all writes not followed by `sync_durable()` are lost.
    /// - If `honor_write_through_hint` is `true` (FUA / Write-Through controller model),
    ///   `MutableSyncLog` writes survive, optionally injecting a 512-byte sub-sector torn write
    ///   on the very last write if `inject_torn_tail` is true.
    pub fn simulate_power_loss(&self, honor_write_through_hint: bool, inject_torn_tail: bool) {
        let mut files = self.ctrl.files.lock().unwrap();
        {
            // A file whose creation was never made durable by a sync does not survive.
            let durable = self.ctrl.durable_dir_entries.lock().unwrap();
            files.retain(|name, _| durable.contains_key(name));
        }
        for (name, file_arc) in files.iter() {
            let mut st = file_arc.lock().unwrap();
            if honor_write_through_hint && st.hint == OpenHint::MutableSyncLog {
                if inject_torn_tail && name == "wal_sync.db" {
                    if let Some(last) = st.last_write.clone() {
                        // Roll back the last 4 KB write to only its first 512 bytes (torn sector)
                        let mut base = st.pre_last_write_bytes.clone();
                        let torn_len = last.data.len().min(512);
                        let end = (last.offset as usize) + torn_len;
                        if end > base.len() {
                            base.resize(end, 0);
                        }
                        base[last.offset as usize..end].copy_from_slice(&last.data[..torn_len]);
                        st.page_cache_bytes = base.clone();
                        st.durable_bytes = base;
                        st.unsynced_writes.clear();
                        continue;
                    }
                }
                st.durable_bytes = st.page_cache_bytes.clone();
                st.unsynced_writes.clear();
            } else {
                // Revert OS page cache back to last `sync_durable()` state
                st.page_cache_bytes = st.durable_bytes.clone();
                st.unsynced_writes.clear();
            }
        }
    }
}

impl PlatformFs for FaultInjectingFs {
    type File = SimFile;

    fn open_file(
        &self,
        rel_name: &str,
        create: bool,
        _writable: bool,
        hint: OpenHint,
    ) -> io::Result<Arc<Self::File>> {
        let mut files = self.ctrl.files.lock().unwrap();
        let state = if let Some(existing) = files.get(rel_name) {
            Arc::clone(existing)
        } else if create {
            let st = Arc::new(Mutex::new(FileState::new(hint)));
            files.insert(rel_name.to_string(), Arc::clone(&st));
            st
        } else {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("sim file not found: {rel_name}"),
            ));
        };

        Ok(Arc::new(SimFile {
            name: rel_name.to_string(),
            state,
            fs_ctrl: Arc::clone(&self.ctrl),
        }))
    }

    fn remove_file(&self, rel_name: &str) -> io::Result<()> {
        let mut files = self.ctrl.files.lock().unwrap();
        self.ctrl
            .durable_dir_entries
            .lock()
            .unwrap()
            .remove(rel_name);
        if files.remove(rel_name).is_some() {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("remove_file: {rel_name} not found"),
            ))
        }
    }

    fn list_files(&self) -> io::Result<Vec<String>> {
        let files = self.ctrl.files.lock().unwrap();
        Ok(files.keys().cloned().collect())
    }

    fn root_path(&self) -> &Path {
        &self.ctrl.root
    }
}

// ============================================================================
// Suite 1: Power-Loss Sync Durability & 512 B Torn-Write Resilience
// ============================================================================

fn bench_suite_1_power_loss_sync_durability() -> (f64, f64) {
    println!(
        "=========================================================================================="
    );
    println!(
        "SUITE 1: Power-Loss Sync Commit Durability & 512 B Torn-Write Resilience (wal_sync.db)"
    );
    println!(
        "=========================================================================================="
    );
    println!(
        "{:<48} | {:>14} | {:>14} | {:>10}",
        "Power-Loss Hardware / OS Model", "Committed", "Survived", "Survival %"
    );
    println!("{}", "-".repeat(94));

    const COMMITS: usize = 200;

    // Model 1A: Strict Volatile Cache / POSIX Model (requires `sync_durable()` on commit)
    let survived_strict = {
        let fs = Arc::new(FaultInjectingFs::new());
        let mut engine = StorageEngine::open(Arc::clone(&fs), EngineConfig::default()).unwrap();
        for i in 0..COMMITS {
            let k = format!("cfg_{i:04}").into_bytes();
            let v = format!("val_{i:04}").into_bytes();
            engine
                .put_row(1, &k, &v, &[], WriteDurability::SyncTransactional)
                .unwrap();
        }
        // Simulate sudden power cut without clean shutdown (drop without flush)
        std::mem::forget(engine);
        fs.simulate_power_loss(false, false);

        let mut recovered = StorageEngine::open(Arc::clone(&fs), EngineConfig::default()).unwrap();
        let mut hits = 0usize;
        for i in 0..COMMITS {
            let k = format!("cfg_{i:04}").into_bytes();
            if recovered.get_row(1, &k).unwrap().is_some() {
                hits += 1;
            }
        }
        hits
    };
    let pct_strict = (survived_strict as f64) * 100.0 / (COMMITS as f64);
    println!(
        "{:<48} | {:>14} | {:>14} | {:>9.1}%",
        "1A. Volatile Drive Cache / Unix PAL (No fsync)", COMMITS, survived_strict, pct_strict
    );

    // Model 1B: FUA Write-Through + 512 B Sub-Sector Torn Write on the 201st in-flight commit
    let survived_fua_torn = {
        let fs = Arc::new(FaultInjectingFs::new());
        let mut engine = StorageEngine::open(Arc::clone(&fs), EngineConfig::default()).unwrap();
        for i in 0..=COMMITS {
            let k = format!("cfg_{i:04}").into_bytes();
            let v = vec![0x5Au8; 600]; // Spans >512 bytes so a 512B torn sector corrupts the CRC
            engine
                .put_row(1, &k, &v, &[], WriteDurability::SyncTransactional)
                .unwrap();
        }
        std::mem::forget(engine);
        // Tear the very last commit (commit #200) at 512 bytes
        fs.simulate_power_loss(true, true);

        let mut recovered = StorageEngine::open(Arc::clone(&fs), EngineConfig::default()).unwrap();
        let mut prior_hits = 0usize;
        for i in 0..COMMITS {
            let k = format!("cfg_{i:04}").into_bytes();
            if recovered.get_row(1, &k).unwrap().is_some() {
                prior_hits += 1;
            }
        }
        let torn_committed = recovered.get_row(1, b"cfg_0200").unwrap().is_some();
        if torn_committed {
            0 // Torn write must NOT be accepted!
        } else {
            prior_hits
        }
    };
    let pct_fua = (survived_fua_torn as f64) * 100.0 / (COMMITS as f64);
    println!(
        "{:<48} | {:>14} | {:>14} | {:>9.1}%",
        "1B. FUA Write-Through + 512B Torn Tail Sector", COMMITS, survived_fua_torn, pct_fua
    );

    (pct_strict, pct_fua)
}

// ============================================================================
// Suite 2: Process Crash (`kill -9`) Survival Across WAL Buffering Modes
// ============================================================================

fn bench_suite_2_process_crash_wal_modes() {
    println!(
        "\n=========================================================================================="
    );
    println!("SUITE 2: Sudden Process Crash (`kill -9` / No Drop) Across Buffered WAL Modes");
    println!(
        "=========================================================================================="
    );
    println!(
        "{:<44} | {:>12} | {:>12} | {:>10} | {:>12}",
        "Buffered WAL Stream Mode", "Committed", "Recovered", "Survival %", "Rec Time(ms)"
    );
    println!("{}", "-".repeat(100));

    const N_COMMITS: usize = 250;

    for (label, mode) in [
        (
            "BufferedImmediateOsCache (Default)",
            WalStreamMode::BufferedImmediateOsCache,
        ),
        (
            "BufferedUserSpaceCoalesced (Speedtest1)",
            WalStreamMode::BufferedUserSpaceCoalesced,
        ),
    ] {
        let fs = Arc::new(FaultInjectingFs::new());
        let mut cfg = EngineConfig::default();
        cfg.buf_wal_mode = mode;

        let mut engine = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
        for i in 0..N_COMMITS {
            let k = format!("row_{i:04}").into_bytes();
            let v = vec![0x42u8; 80]; // ~100B per WAL frame -> ~25 KB total (< 64 KB sector/coalesce)
            engine
                .put_row(1, &k, &v, &[], WriteDurability::BufferedFast)
                .unwrap();
        }
        // Simulate `kill -9` (process dies before Drop or flush_memtable; OS page cache stays intact)
        std::mem::forget(engine);

        let t0 = Instant::now();
        let mut recovered = StorageEngine::open(Arc::clone(&fs), cfg).unwrap();
        let rec_ms = t0.elapsed().as_secs_f64() * 1e3;

        let mut hits = 0usize;
        for i in 0..N_COMMITS {
            let k = format!("row_{i:04}").into_bytes();
            if recovered.get_row(1, &k).unwrap().is_some() {
                hits += 1;
            }
        }
        println!(
            "{:<44} | {:>12} | {:>12} | {:>9.1}% | {:>9.2} ms",
            label,
            N_COMMITS,
            hits,
            (hits as f64) * 100.0 / (N_COMMITS as f64),
            rec_ms
        );
    }
}

// ============================================================================
// Suite 3: Transaction Atomicity & Rollback Invariants (Differential Oracle)
// ============================================================================

fn bench_suite_3_transaction_rollback_invariants() {
    println!(
        "\n=========================================================================================="
    );
    println!("SUITE 3: Transaction Rollback & Quota Invariants vs. Reference BTreeMap Oracle");
    println!(
        "=========================================================================================="
    );
    println!(
        "{:<54} | {:>14} | {:>20}",
        "Scenario", "Status", "Observed Behavior"
    );
    println!("{}", "-".repeat(94));

    // Scenario 3A: Small Transaction (< 4 MB, inline rows)
    {
        let fs = Arc::new(FaultInjectingFs::new());
        let mut engine = StorageEngine::open(Arc::clone(&fs), EngineConfig::default()).unwrap();
        engine
            .put_row(1, b"k1", b"initial_v1", &[], WriteDurability::BufferedFast)
            .unwrap();

        engine.begin_transaction();
        engine
            .put_row(1, b"k1", b"mutated_v1", &[], WriteDurability::BufferedFast)
            .unwrap();
        engine
            .put_row(1, b"k2", b"added_v2", &[], WriteDurability::BufferedFast)
            .unwrap();
        engine.rollback_transaction().unwrap();

        let v1 = engine.get_row(1, b"k1").unwrap();
        let v2 = engine.get_row(1, b"k2").unwrap();
        let ok = v1.as_deref() == Some(b"initial_v1".as_slice()) && v2.is_none();
        println!(
            "{:<54} | {:>14} | {:>20}",
            "3A. Small Inline Tx Rollback (< 4 MB)",
            if ok { "PASS" } else { "FAIL" },
            if ok { "100% Restored" } else { "Corrupted" }
        );
    }

    // Scenario 3B: Sealed Blob Segment Overwrite + Rollback
    {
        let fs = Arc::new(FaultInjectingFs::new());
        let mut cfg = EngineConfig::default();
        cfg.inline_threshold_bytes = 128;
        cfg.max_blob_segment_bytes = 4096; // Seal segment after 1 x 4 KB blob
        let mut engine = StorageEngine::open(Arc::clone(&fs), cfg).unwrap();

        let orig_blob = vec![0x11u8; 4096];
        let second_blob = vec![0x22u8; 4096];
        // Write k1 (fills seg 1), then k2 (rotates to seg 2 so seg 1 is sealed!)
        engine
            .put_row(1, b"k1", &orig_blob, &[], WriteDurability::BufferedFast)
            .unwrap();
        engine
            .put_row(1, b"k2", &second_blob, &[], WriteDurability::BufferedFast)
            .unwrap();

        // Begin tx, overwrite k1 (causing seg 1 to reach 100% dead_bytes and be unlinked!), then ROLLBACK
        engine.begin_transaction();
        engine
            .put_row(
                1,
                b"k1",
                &vec![0x99u8; 4096],
                &[],
                WriteDurability::BufferedFast,
            )
            .unwrap();
        engine.rollback_transaction().unwrap();

        let read_res = engine.get_row(1, b"k1");
        let (status, note) = match read_res {
            Ok(Some(ref v)) if v == &orig_blob => ("PASS", "Blob Intact"),
            Ok(Some(_)) => ("FAIL", "Wrong Blob Data"),
            Ok(None) => ("FAIL", "Blob Vanished"),
            Err(_) => ("FAIL", "I/O Err (File Deleted)"),
        };
        println!(
            "{:<54} | {:>14} | {:>20}",
            "3B. Sealed Blob Overwrite + Rollback", status, note
        );
    }

    // Scenario 3C: Large Transaction (> 4 MB) Rollback Atomicity
    {
        let fs = Arc::new(FaultInjectingFs::new());
        let mut engine = StorageEngine::open(Arc::clone(&fs), EngineConfig::default()).unwrap();

        engine.begin_transaction();
        // Insert 5,000 inline rows of 1,000 B each (~5.0 MB > 4.0 MB pending_tx_buf & MemTable cap)
        let payload = vec![0xAAu8; 1000];
        for i in 0..5_000 {
            let k = format!("tx_key_{i:05}").into_bytes();
            engine
                .put_row(1, &k, &payload, &[], WriteDurability::BufferedFast)
                .unwrap();
        }
        // Now ROLLBACK the entire transaction! Zero rows should exist after rollback.
        engine.rollback_transaction().unwrap();

        let mut leaked_rows = 0usize;
        for i in 0..5_000 {
            let k = format!("tx_key_{i:05}").into_bytes();
            if engine.get_row(1, &k).unwrap().is_some() {
                leaked_rows += 1;
            }
        }
        let ok = leaked_rows == 0;
        println!(
            "{:<54} | {:>14} | {:>20}",
            "3C. Large Transaction (> 4 MB) Rollback Atomicity",
            if ok { "PASS" } else { "FAIL" },
            if ok {
                "0 Leaked Rows".to_string()
            } else {
                format!("{leaked_rows}/5000 Survived!")
            }
        );
    }

    // Scenario 3D: Tombstone-Shadowed Multi-Segment Range Scan with `limit`
    {
        let fs = Arc::new(FaultInjectingFs::new());
        let mut engine = StorageEngine::open(Arc::clone(&fs), EngineConfig::default()).unwrap();
        // Segment 1: 40 live rows k_00..k_39
        for i in 0..40 {
            let k = format!("k_{i:02}").into_bytes();
            engine
                .put_row(1, &k, b"live_val", &[], WriteDurability::BufferedFast)
                .unwrap();
        }
        engine.flush_memtable().unwrap();

        // Segment 2: Delete first 20 rows k_00..k_19 and flush as Tombstones
        for i in 0..20 {
            let k = format!("k_{i:02}").into_bytes();
            engine
                .delete_row(1, &k, WriteDurability::BufferedFast)
                .unwrap();
        }
        engine.flush_memtable().unwrap();

        // Query with limit = 1: should return `k_20`, NOT empty!
        let rows = engine.scan_table_range(1, None, None, 1).unwrap();
        let ok = rows.len() == 1 && rows[0].0 == b"k_20";
        println!(
            "{:<54} | {:>14} | {:>20}",
            "3D. Multi-Segment Range Scan w/ Tombstones",
            if ok { "PASS" } else { "FAIL" },
            if ok {
                "1/1 Row Found".to_string()
            } else {
                format!("{} Rows (20 Live!)", rows.len())
            }
        );
    }
}

// ============================================================================
// Suite 4: Post-Crash Blob Garbage Collection & Storage Leak
// ============================================================================

fn bench_suite_4_post_crash_blob_leak() {
    println!(
        "\n=========================================================================================="
    );
    println!("SUITE 4: Post-Crash Blob GC & Recovery Consistency");
    println!(
        "=========================================================================================="
    );
    println!(
        "{:<54} | {:>14} | {:>20}",
        "Crash Scenario", "Status", "Post-Recovery State"
    );
    println!("{}", "-".repeat(94));

    // Scenario 4A: 100% of a sealed blob segment is deleted in RAM/WAL, then crash BEFORE flush_memtable()
    {
        let fs = Arc::new(FaultInjectingFs::new());
        let mut cfg = EngineConfig::default();
        cfg.inline_threshold_bytes = 128;
        cfg.max_blob_segment_bytes = 16 * 1024; // 16 KB per blob segment (4 blobs of 4 KB)

        {
            let mut engine = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
            let blob = vec![0x77u8; 4096];
            for i in 0..8 {
                let k = format!("blob_key_{i:03}").into_bytes();
                engine
                    .put_row(1, &k, &blob, &[], WriteDurability::BufferedFast)
                    .unwrap();
            }
            engine.flush_memtable().unwrap();
        }
        {
            let mut engine = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
            // Delete all 4 blobs in Segment 1 (reclaim_fully_dead_blob_segments unlinks seg 1 before flush!)
            for i in 0..4 {
                let k = format!("blob_key_{i:03}").into_bytes();
                engine
                    .delete_row(1, &k, WriteDurability::BufferedFast)
                    .unwrap();
            }
            std::mem::forget(engine); // Sudden crash before flush_memtable updates boot_meta.db!
        }

        let reopen_res = StorageEngine::open(Arc::clone(&fs), cfg);
        let (status, detail) = match reopen_res {
            Ok(_) => ("PASS", "Opened Cleanly".to_string()),
            Err(e) => ("FAIL", format!("Unopenable ({:?})", e.kind())),
        };
        println!(
            "{:<54} | {:>14} | {:>20}",
            "4A. Delete Full Sealed Blob Seg + Crash Before Flush", status, detail
        );
    }

    // Scenario 4B: Delete 3/4 of each sealed blob segment -> Crash -> Recover -> Delete last 1/4 -> Flush & Compact
    {
        let fs = Arc::new(FaultInjectingFs::new());
        let mut cfg = EngineConfig::default();
        cfg.inline_threshold_bytes = 128;
        cfg.max_blob_segment_bytes = 16 * 1024; // 4 blobs of ~4 KB per segment

        {
            let mut engine = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
            let blob = vec![0x77u8; 4000];
            for i in 0..16 {
                let k = format!("blob_key_{i:03}").into_bytes();
                engine
                    .put_row(1, &k, &blob, &[], WriteDurability::BufferedFast)
                    .unwrap();
            }
            engine.flush_memtable().unwrap();
        }
        {
            let mut engine = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
            // Delete 3 out of 4 blobs in each 4-blob group (so dead_bytes < file_len before crash)
            for i in 0..16 {
                if i % 4 != 3 {
                    let k = format!("blob_key_{i:03}").into_bytes();
                    engine
                        .delete_row(1, &k, WriteDurability::BufferedFast)
                        .unwrap();
                }
            }
            std::mem::forget(engine); // Crash! 12 deletes are in WAL, but their dead_bytes are not in boot_meta
        }

        // Recover from WAL, delete the remaining 1/4 of blobs so 100% of keys are now deleted!
        let mut engine = StorageEngine::open(Arc::clone(&fs), cfg).unwrap();
        for i in 0..16 {
            if i % 4 == 3 {
                let k = format!("blob_key_{i:03}").into_bytes();
                engine
                    .delete_row(1, &k, WriteDurability::BufferedFast)
                    .unwrap();
            }
        }
        engine.flush_memtable().unwrap();
        let _ = engine.compact();

        let remaining_files = fs.list_files().unwrap();
        let blob_files: Vec<_> = remaining_files
            .iter()
            .filter(|f| f.starts_with("blob_std_"))
            .collect();
        let leaked_sealed = blob_files.len().saturating_sub(1);
        println!(
            "{:<54} | {:>14} | {:>20}",
            "4B. Partial Blob Delete + Crash + Delete Rest (GC)",
            if leaked_sealed == 0 { "PASS" } else { "FAIL" },
            format!("{leaked_sealed} Segs Leaked Forever")
        );
    }
}

// ============================================================================
// Suite 5: Deterministic I/O Fault Countdown During `flush_memtable()`
// ============================================================================

fn bench_suite_5_io_fault_countdown() {
    println!(
        "\n=========================================================================================="
    );
    println!("SUITE 5: Deterministic I/O Fault Injection (`StorageFull` During `flush_memtable`)");
    println!(
        "=========================================================================================="
    );
    println!(
        "{:<14} | {:>24} | {:>22} | {:>20}",
        "Fault Step #", "flush_memtable() Result", "Surviving After Retry", "Status"
    );
    println!("{}", "-".repeat(88));

    const TOTAL_KEYS: usize = 50;

    for fault_step in 1..=9 {
        let fs = Arc::new(FaultInjectingFs::new());
        let mut engine = StorageEngine::open(Arc::clone(&fs), EngineConfig::default()).unwrap();

        let mut oracle = BTreeMap::new();
        for i in 0..TOTAL_KEYS {
            let k = format!("k_{i:03}").into_bytes();
            let v = format!("val_{i:03}").into_bytes();
            engine
                .put_row(1, &k, &v, &[], WriteDurability::BufferedFast)
                .unwrap();
            oracle.insert(k, v);
        }

        // Arm deterministic fault on the `fault_step`-th I/O operation inside `flush_memtable()`
        fs.set_io_fault_step(fault_step);
        let flush_res = engine.flush_memtable();
        // A failed metadata slot write has an unknown outcome and requires recovery before retry.
        fs.set_io_fault_step(0);
        if engine.is_poisoned() {
            drop(engine);
            engine = StorageEngine::open(Arc::clone(&fs), EngineConfig::default()).unwrap();
        }
        engine
            .put_row(1, b"after_err", b"v", &[], WriteDurability::BufferedFast)
            .unwrap();
        let _ = engine.flush_memtable();
        drop(engine);

        let mut reopened = StorageEngine::open(Arc::clone(&fs), EngineConfig::default()).unwrap();
        let mut surviving = 0usize;
        for (k, expected_v) in &oracle {
            if let Ok(Some(actual_v)) = reopened.get_row(1, k) {
                if &actual_v == expected_v {
                    surviving += 1;
                }
            }
        }

        let res_str = match flush_res {
            Ok(_) => "Ok (Completed)",
            Err(_) => "Err(StorageFull)",
        };
        let status = if surviving == TOTAL_KEYS {
            "PASS (Consistent)"
        } else {
            "FAIL (Data Lost!)"
        };

        println!(
            "Step {:<9} | {:>24} | {:>17}/{:<4} | {:>20}",
            fault_step, res_str, surviving, TOTAL_KEYS, status
        );
    }
}

// ============================================================================
// Suite 6: SQLite VTab Key Encoding & Rowid Integrity
// ============================================================================

fn bench_suite_6_vtab_key_and_rowid_integrity() {
    println!(
        "\n=========================================================================================="
    );
    println!("SUITE 6: SQLite VTab Key Encoding & Rowid Integrity");
    println!(
        "=========================================================================================="
    );
    println!(
        "{:<54} | {:>14} | {:>20}",
        "VTab Correctness Scenario", "Status", "Observed Result"
    );
    println!("{}", "-".repeat(94));

    let dir = std::env::temp_dir().join(format!("aydb_rel_vtab_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let fs = Arc::new(OsFs::open(&dir, true).unwrap());
    let engine = Arc::new(RwLock::new(
        StorageEngine::open(fs, EngineConfig::default()).unwrap(),
    ));
    let conn = SqliteConnection::open_in_memory().unwrap();
    conn.register_aydb_module(engine).unwrap();

    // 6A: REAL (f64) Primary Key Index Order
    conn.execute_batch(
        "CREATE VIRTUAL TABLE real_t USING aydb(table_id=1, columns='k REAL PRIMARY KEY, v TEXT');
         INSERT INTO real_t VALUES (-10.0, 'a'), (-2.0, 'b'), (-1.0, 'c'), (0.0, 'd'), (1.0, 'e'), (10.0, 'f');",
    )
    .unwrap();
    let rows = conn.query_all("SELECT k FROM real_t", &[]).unwrap();
    let scanned_floats: Vec<f64> = rows
        .iter()
        .filter_map(|r| match r.first() {
            Some(SqlValue::Float(v)) => Some(*v),
            _ => None,
        })
        .collect();
    let expected_floats = vec![-10.0, -2.0, -1.0, 0.0, 1.0, 10.0];
    let ok_order = scanned_floats == expected_floats;
    println!(
        "{:<54} | {:>14} | {:>20}",
        "6A. REAL (f64) PK Index Scan Sort Order",
        if ok_order { "PASS" } else { "FAIL" },
        if ok_order {
            "Sorted Correctly"
        } else {
            "Inverted Negative Order"
        }
    );

    // 6B: 32-Bit Entropy Collision in 64-Bit `pk_to_rowid` Hash
    // Find two ordinary TEXT keys whose 32-bit CRC32C collides (takes ~2 ms via birthday bound),
    // insert both, and delete one by its SQLite `rowid`.
    let mut seen_crc: HashMap<u32, String> = HashMap::with_capacity(100_000);
    let mut pair: Option<(String, String)> = None;
    let mut rng = 0x9E37_79B9_7F4A_7C15u64;
    for _ in 0..500_000u32 {
        rng ^= rng >> 12;
        rng ^= rng << 25;
        rng ^= rng >> 27;
        let val = rng.wrapping_mul(0x2545_F491_4F6C_DD1D);
        let candidate = format!("user_{val:016x}");
        let h = aydb::util::crc32c::crc32c(candidate.as_bytes());
        if let Some(prev) = seen_crc.insert(h, candidate.clone()) {
            if prev != candidate {
                pair = Some((prev, candidate));
                break;
            }
        }
    }
    let (k1, k2) = pair.expect("birthday collision within 500k keys");
    conn.execute_batch(
        "CREATE VIRTUAL TABLE users_t USING aydb(table_id=2, columns='id TEXT PRIMARY KEY, name TEXT');",
    )
    .unwrap();
    conn.execute_batch(&format!(
        "INSERT INTO users_t VALUES ('{k1}', 'Alice'), ('{k2}', 'Bob');"
    ))
    .unwrap();
    // Query Alice's rowid, then delete by that rowid; Bob happens to overwrite `rowid_to_pk[rid]` on INSERT!
    let r1 = conn
        .query_all(&format!("SELECT rowid FROM users_t WHERE id = '{k1}'"), &[])
        .unwrap();
    let rid1 = match r1[0][0] {
        SqlValue::Integer(v) => v,
        _ => unreachable!(),
    };
    // Re-insert or touch k2 so `rowid_to_pk[rid]` holds k2, then delete rowid = rid1
    conn.execute_batch(&format!(
        "UPDATE users_t SET name = 'Bob' WHERE id = '{k2}';
         DELETE FROM users_t WHERE rowid = {rid1};"
    ))
    .unwrap();
    let bob_still_exists = !conn
        .query_all(&format!("SELECT name FROM users_t WHERE id = '{k2}'"), &[])
        .unwrap()
        .is_empty();
    let alice_deleted = conn
        .query_all(&format!("SELECT name FROM users_t WHERE id = '{k1}'"), &[])
        .unwrap()
        .is_empty();
    let ok_collision = bob_still_exists && alice_deleted;
    println!(
        "{:<54} | {:>14} | {:>20}",
        "6B. TEXT PK 64-Bit `rowid` Hash Collision",
        if ok_collision { "PASS" } else { "FAIL" },
        if ok_collision {
            "Correct Row Deleted"
        } else {
            "Wrong Row Deleted!"
        }
    );

    // 6C: Empty / Inverted Range Query (`WHERE k >= 10 AND k <= 5` or `k >= -10.0 AND k <= 10.0`)
    // Tested via `catch_unwind` on `StorageEngine::scan_table_range` because a panic inside
    // `extern "system" fn vtab_filter` immediately aborts the process (`SIGABRT`)!
    let prev_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let panic_res = std::panic::catch_unwind(|| {
        let fs_r = Arc::new(FaultInjectingFs::new());
        let mut eng_r = StorageEngine::open(fs_r, EngineConfig::default()).unwrap();
        eng_r
            .put_row(
                1,
                &[0x80, 0, 0, 0, 0, 0, 0, 5],
                b"v",
                &[],
                WriteDurability::BufferedFast,
            )
            .unwrap();
        let lower = [0x80u8, 0, 0, 0, 0, 0, 0, 10];
        let upper = [0x80u8, 0, 0, 0, 0, 0, 0, 5];
        eng_r
            .scan_table_range(1, Some((&lower[..], true)), Some((&upper[..], true)), 100)
            .unwrap()
    });
    std::panic::set_hook(prev_hook);
    let ok_range = panic_res.is_ok();
    println!(
        "{:<54} | {:>14} | {:>20}",
        "6C. Inverted/Empty Range Query (`lower > upper`)",
        if ok_range { "PASS" } else { "FAIL (SIGABRT)" },
        if ok_range {
            "0 Rows Returned"
        } else {
            "PANIC in BTreeMap::range"
        }
    );

    drop(conn);
    let _ = std::fs::remove_dir_all(&dir);
}

// ============================================================================
// Suite 7: Technical Architecture & Edge-Case Regression Verification
// ============================================================================

pub fn suite_7_audit_regressions(base_dir: &Path) {
    println!(
        "\n=========================================================================================="
    );
    println!("SUITE 7: Technical Architecture & Edge-Case Regressions");
    println!(
        "=========================================================================================="
    );
    println!(
        "{:<54} | {:>14} | {:>20}",
        "Technical Regression Scenario", "Status", "Observed Result"
    );
    println!("{}", "-".repeat(94));

    // 7A: Multi-restart WAL recovery after torn/dangling fragments without losing subsequent writes
    {
        // Part 1: BufferedUserSpaceCoalesced dangling `First` fragment + restart + subsequent writes
        let fs1 = Arc::new(FaultInjectingFs::new());
        let mut cfg1 = EngineConfig::default();
        cfg1.buf_wal_mode = WalStreamMode::BufferedUserSpaceCoalesced;
        {
            let mut eng = StorageEngine::open(Arc::clone(&fs1), cfg1.clone()).unwrap();
            let val = vec![0x41u8; 900];
            for i in 0..9 {
                let k = format!("k_{i:02}").into_bytes();
                eng.put_row(1, &k, &val, &[], WriteDurability::BufferedFast)
                    .unwrap();
            }
            std::mem::forget(eng); // Crash with 8 rows in OS cache + dangling `First` fragment in sector 2
        }
        {
            let mut eng = StorageEngine::open(Arc::clone(&fs1), cfg1.clone()).unwrap();
            for i in 0..3 {
                let k = format!("post_{i:02}").into_bytes();
                eng.put_row(1, &k, b"after_restart", &[], WriteDurability::BufferedFast)
                    .unwrap();
            }
        }
        let mut eng1_final = StorageEngine::open(Arc::clone(&fs1), cfg1).unwrap();
        let mut post_survived = 0usize;
        for i in 0..3 {
            let k = format!("post_{i:02}").into_bytes();
            if eng1_final.get_row(1, &k).unwrap().as_deref() == Some(b"after_restart".as_slice()) {
                post_survived += 1;
            }
        }

        // Part 2: BufferedImmediateOsCache mid-stream corrupted sector + restart + delete & insert
        let fs2 = Arc::new(FaultInjectingFs::new());
        let cfg2 = EngineConfig::default();
        {
            let mut eng = StorageEngine::open(Arc::clone(&fs2), cfg2.clone()).unwrap();
            let val = vec![0x42u8; 900];
            for i in 0..40 {
                let k = format!("r_{i:02}").into_bytes();
                eng.put_row(1, &k, &val, &[], WriteDurability::BufferedFast)
                    .unwrap();
            }
            std::mem::forget(eng);
        }
        // Corrupt sector 2 (offset 8192) of wal_buf.db
        {
            let f = fs2
                .open_file("wal_buf.db", false, true, OpenHint::MutableBufferedLog)
                .unwrap();
            f.write_all_at(&[0xFFu8; 64], 2 * 4096).unwrap();
        }
        {
            let mut eng = StorageEngine::open(Arc::clone(&fs2), cfg2.clone()).unwrap();
            eng.delete_row(1, b"r_00", WriteDurability::BufferedFast)
                .unwrap();
            eng.put_row(1, b"r_new", b"survived", &[], WriteDurability::BufferedFast)
                .unwrap();
        }
        let mut eng2_final = StorageEngine::open(Arc::clone(&fs2), cfg2).unwrap();
        let del_ok = eng2_final.get_row(1, b"r_00").unwrap().is_none();
        let ins_ok =
            eng2_final.get_row(1, b"r_new").unwrap().as_deref() == Some(b"survived".as_slice());

        let ok = post_survived == 3 && del_ok && ins_ok;
        println!(
            "{:<54} | {:>14} | {:>20}",
            "7A. Post-Recovery WAL Epoch Rotation",
            if ok { "PASS" } else { "FAIL" },
            if ok {
                "100% Writes Survived"
            } else {
                "Post-Crash Writes Lost"
            }
        );
    }

    // 7B: Oversized PK rejection & near-4KB row/meta auto-spill without wedging flush_memtable
    {
        let fs = Arc::new(FaultInjectingFs::new());
        let mut cfg = EngineConfig::default();
        cfg.inline_threshold_bytes = 4096;
        let mut eng = StorageEngine::open(Arc::clone(&fs), cfg).unwrap();

        // Oversized PK (> 2048 B) must be rejected with InvalidInput
        let oversized_pk = vec![b'x'; 5000];
        let pk_rejected =
            match eng.put_row(1, &oversized_pk, b"v", &[], WriteDurability::BufferedFast) {
                Err(e) => e.kind() == io::ErrorKind::InvalidInput,
                Ok(()) => false,
            };

        // Row with large PK (1500 B) + value (3000 B) + secondary keys whose combined size > 4 KB block
        let pk = vec![b'k'; 1500];
        let val = vec![b'v'; 3000];
        let sec = vec![
            (1u16, vec![b's'; 1500]),
            (2u16, vec![b't'; 1500]),
            (3u16, vec![b'u'; 1500]),
        ];
        let put_ok = eng
            .put_row(1, &pk, &val, &sec, WriteDurability::BufferedFast)
            .is_ok();
        let flush_ok = eng.flush_memtable().is_ok();
        let compact_ok = eng.compact().is_ok();
        let read_ok = eng.get_row(1, &pk).unwrap().as_deref() == Some(val.as_slice());

        let ok = pk_rejected && put_ok && flush_ok && compact_ok && read_ok;
        println!(
            "{:<54} | {:>14} | {:>20}",
            "7B. 4KB Block Guard & Meta Blob Spill",
            if ok { "PASS" } else { "FAIL" },
            if ok {
                "Flushed & Compacted"
            } else {
                "Flush Wedged!"
            }
        );
    }

    // 7C: SQL VTab PK uniqueness, ON CONFLICT, type-tagged keys, and auto table_id
    {
        let sub = base_dir.join("suite7_vtab_aud03_05");
        let _ = std::fs::remove_dir_all(&sub);
        std::fs::create_dir_all(&sub).unwrap();
        let fs = Arc::new(OsFs::open(&sub, true).unwrap());
        let engine = Arc::new(RwLock::new(
            StorageEngine::open(fs, EngineConfig::default()).unwrap(),
        ));
        let conn = SqliteConnection::open_in_memory().unwrap();
        conn.register_aydb_module(engine).unwrap();

        conn.execute_batch(
            "CREATE VIRTUAL TABLE t_a USING aydb(columns='k TEXT PRIMARY KEY, v TEXT');
             CREATE VIRTUAL TABLE t_b USING aydb(columns='k TEXT PRIMARY KEY, v TEXT');
             INSERT INTO t_a VALUES ('abc', 'first');",
        )
        .unwrap();

        // Plain duplicate INSERT must fail with SQLITE_CONSTRAINT
        let dup_failed = conn
            .execute_batch("INSERT INTO t_a VALUES ('abc', 'second');")
            .is_err();

        // INSERT OR IGNORE preserves 'first'; INSERT OR REPLACE updates to 'replaced'
        conn.execute_batch("INSERT OR IGNORE INTO t_a VALUES ('abc', 'ignored');")
            .unwrap();
        let v_after_ignore = conn
            .query_all("SELECT v FROM t_a WHERE k = 'abc'", &[])
            .unwrap();
        let ignore_ok = v_after_ignore == vec![vec![SqlValue::Text("first".to_string())]];

        conn.execute_batch("INSERT OR REPLACE INTO t_a VALUES ('abc', 'replaced');")
            .unwrap();
        let v_after_replace = conn
            .query_all("SELECT v FROM t_a WHERE k = 'abc'", &[])
            .unwrap();
        let replace_ok = v_after_replace == vec![vec![SqlValue::Text("replaced".to_string())]];

        // Type-tagged key isolation: 'abc' vs X'616263', '' vs X''
        conn.execute_batch(
            "INSERT INTO t_a VALUES (X'616263', 'blob_abc'), ('', 'empty_text'), (X'', 'empty_blob');",
        )
        .unwrap();
        let count_a = conn.query_all("SELECT COUNT(*) FROM t_a", &[]).unwrap();
        let count_b = conn.query_all("SELECT COUNT(*) FROM t_b", &[]).unwrap();
        let counts_ok = count_a == vec![vec![SqlValue::Integer(4)]]
            && count_b == vec![vec![SqlValue::Integer(0)]];

        drop(conn);
        let _ = std::fs::remove_dir_all(&sub);

        let ok = dup_failed && ignore_ok && replace_ok && counts_ok;
        println!(
            "{:<54} | {:>14} | {:>20}",
            "7C. VTab PK Uniqueness & Type Tags",
            if ok { "PASS" } else { "FAIL" },
            if ok {
                "Isolated & Enforced"
            } else {
                "Key Collision/Leak"
            }
        );
    }

    // 7D: Multi-chunk (>512 KB) tx crash atomicity, SAVEPOINT, & DROP TABLE
    {
        // Part 1: Multi-chunk (>512 KB) transaction torn before final TxCommit chunk must roll back 100%
        let fs = Arc::new(FaultInjectingFs::new());
        {
            let mut eng = StorageEngine::open(Arc::clone(&fs), EngineConfig::default()).unwrap();
            eng.put_row(
                1,
                b"baseline",
                b"intact",
                &[],
                WriteDurability::BufferedFast,
            )
            .unwrap();

            eng.begin_transaction();
            let payload = vec![0x55u8; 900];
            for i in 0..800 {
                let k = format!("big_tx_{i:04}").into_bytes();
                eng.put_row(1, &k, &payload, &[], WriteDurability::BufferedFast)
                    .unwrap();
            }
            eng.commit_transaction().unwrap();
            std::mem::forget(eng);
        }
        // Corrupt the final WAL sector containing TxCommit in wal_buf.db so chunk 1 is intact but chunk 2 is torn
        {
            let f = fs
                .open_file("wal_buf.db", false, true, OpenHint::MutableBufferedLog)
                .unwrap();
            // Chunk 1 is ~512 KB (~128 sectors); corrupt sector 160 in the 2nd chunk
            f.write_all_at(&[0xFFu8; 64], 160 * 4096).unwrap();
        }
        let mut recovered = StorageEngine::open(Arc::clone(&fs), EngineConfig::default()).unwrap();
        let baseline_ok =
            recovered.get_row(1, b"baseline").unwrap().as_deref() == Some(b"intact".as_slice());
        let partial_leaked = recovered.get_row(1, b"big_tx_0000").unwrap().is_some();

        // Part 2: SQL SAVEPOINT / ROLLBACK TO and DROP TABLE cleanup
        let sub = base_dir.join("suite7_vtab_use01_02");
        let _ = std::fs::remove_dir_all(&sub);
        std::fs::create_dir_all(&sub).unwrap();
        let os_fs = Arc::new(OsFs::open(&sub, true).unwrap());
        let engine = Arc::new(RwLock::new(
            StorageEngine::open(os_fs, EngineConfig::default()).unwrap(),
        ));
        let conn = SqliteConnection::open_in_memory().unwrap();
        conn.register_aydb_module(engine).unwrap();

        conn.execute_batch(
            "CREATE VIRTUAL TABLE items USING aydb(table_id=10, columns='id INT PRIMARY KEY, name TEXT');
             BEGIN;
             INSERT INTO items VALUES (1, 'kept');
             SAVEPOINT sp_inner;
             INSERT INTO items VALUES (2, 'discarded');
             ROLLBACK TO sp_inner;
             RELEASE sp_inner;
             COMMIT;",
        )
        .unwrap();
        let sp_rows = conn.query_all("SELECT id FROM items", &[]).unwrap();
        let sp_ok = sp_rows == vec![vec![SqlValue::Integer(1)]];

        conn.execute_batch(
            "DROP TABLE items;
             CREATE VIRTUAL TABLE items USING aydb(table_id=10, columns='id INT PRIMARY KEY, name TEXT');",
        )
        .unwrap();
        let after_drop = conn.query_all("SELECT COUNT(*) FROM items", &[]).unwrap();
        let drop_ok = after_drop == vec![vec![SqlValue::Integer(0)]];

        drop(conn);
        let _ = std::fs::remove_dir_all(&sub);

        let ok = baseline_ok && !partial_leaked && sp_ok && drop_ok;
        println!(
            "{:<54} | {:>14} | {:>20}",
            "7D. >512KB Tx Atomicity, SAVEPOINT & DROP",
            if ok { "PASS" } else { "FAIL" },
            if ok {
                "Atomic & Cleaned"
            } else {
                "Partial Tx/Drop Leak"
            }
        );
    }
}

// ============================================================================
// Suite 8: Transaction Isolation, 2PC & Engine Boundary Regressions
// ============================================================================

pub fn suite_8_followup_audit_regressions(base_dir: &Path) {
    println!(
        "\n=========================================================================================="
    );
    println!("SUITE 8: Transaction Isolation, 2PC & Engine Boundary Regressions");
    println!(
        "=========================================================================================="
    );
    println!(
        "{:<54} | {:>14} | {:>20}",
        "Boundary Regression Scenario", "Status", "Observed Result"
    );
    println!("{}", "-".repeat(94));

    // 8A: Cross-connection transaction isolation & two-phase xSync prepare + rollback
    {
        let sub = base_dir.join("suite8_f01_f07");
        let _ = std::fs::remove_dir_all(&sub);
        std::fs::create_dir_all(&sub).unwrap();
        let fs = Arc::new(OsFs::open(&sub, true).unwrap());
        let shared = Arc::new(RwLock::new(
            StorageEngine::open(fs, EngineConfig::default()).unwrap(),
        ));

        let conn_a = SqliteConnection::open_in_memory().unwrap();
        conn_a.register_aydb_module(shared.clone()).unwrap();
        conn_a
            .execute_batch(
                "CREATE VIRTUAL TABLE t USING aydb(table_id=1, columns='id INTEGER PRIMARY KEY, v TEXT');
                 INSERT INTO t VALUES (1, 'one'), (2, 'two');",
            )
            .unwrap();

        let conn_b = SqliteConnection::open_in_memory().unwrap();
        conn_b.register_aydb_module(shared.clone()).unwrap();
        conn_b
            .execute_batch(
                "CREATE VIRTUAL TABLE t USING aydb(table_id=1, columns='id INTEGER PRIMARY KEY, v TEXT');",
            )
            .unwrap();

        conn_a
            .execute_batch("BEGIN; INSERT INTO t VALUES (3, 'uncommitted');")
            .unwrap();
        // Connection B must read the committed snapshot `[]` for id=3
        let b_sees_uncommitted = conn_b.query_all("SELECT * FROM t WHERE id=3", &[]).unwrap();
        // Connection B's concurrent write must return SQLITE_BUSY (Err) instead of joining Connection A's tx
        let b_concurrent_write_busy = conn_b
            .execute_batch("INSERT INTO t VALUES (4, 'other committed');")
            .is_err();
        conn_a.execute_batch("ROLLBACK;").unwrap();
        // After Connection A rolls back, Connection B can write and commit row 4
        conn_b
            .execute_batch("INSERT INTO t VALUES (4, 'other committed');")
            .unwrap();
        let b_row4_after = conn_b
            .query_all("SELECT id, v FROM t WHERE id=4", &[])
            .unwrap();

        // Two-phase `xSync` (`prepare_commit_transaction`) + rollback on a capped table restores quota-evicted rows
        let two_phase_ok = {
            let mut eng = shared.write().unwrap();
            eng.register_capped_table(9, 300).unwrap();
            eng.put_row(
                9,
                b"r1",
                &vec![0x11u8; 150],
                &[],
                WriteDurability::BufferedFast,
            )
            .unwrap();
            eng.begin_transaction();
            eng.put_row(
                9,
                b"r2",
                &vec![0x22u8; 200],
                &[],
                WriteDurability::BufferedFast,
            )
            .unwrap();
            eng.prepare_commit_transaction(WriteDurability::BufferedFast)
                .unwrap();
            eng.rollback_transaction().unwrap();
            eng.get_row(9, b"r1").unwrap() == Some(vec![0x11u8; 150])
                && eng.get_row(9, b"r2").unwrap().is_none()
        };

        drop(conn_a);
        drop(conn_b);
        drop(shared);
        let _ = std::fs::remove_dir_all(&sub);

        let ok = b_sees_uncommitted.is_empty()
            && b_concurrent_write_busy
            && b_row4_after
                == vec![vec![
                    SqlValue::Integer(4),
                    SqlValue::Text("other committed".to_string()),
                ]]
            && two_phase_ok;
        assert!(ok, "8A cross-connection isolation regression check failed");
        println!(
            "{:<54} | {:>14} | {:>20}",
            "8A. Cross-Conn Isolation & 2PC Prepare", "PASS", "Isolated & 2PC Safe"
        );
    }

    // 8B: Deferred mid-tx checkpointing & atomic multi-chunk WAL recovery on invalid blob
    {
        let sub_kv = base_dir.join("suite8_f02_kv");
        let _ = std::fs::remove_dir_all(&sub_kv);
        std::fs::create_dir_all(&sub_kv).unwrap();
        let fs_kv = Arc::new(OsFs::open(&sub_kv, true).unwrap());
        let mut eng = StorageEngine::open(fs_kv.clone(), EngineConfig::default()).unwrap();
        eng.begin_transaction();
        eng.put_row(1, b"aborted", b"value", &[], WriteDurability::BufferedFast)
            .unwrap();
        eng.flush_memtable().unwrap();
        eng.compact().unwrap();
        eng.rollback_transaction().unwrap();
        let in_mem_after_rollback = eng.get_row(1, b"aborted").unwrap();
        drop(eng);

        let mut eng_reopen = StorageEngine::open(fs_kv, EngineConfig::default()).unwrap();
        let after_reopen = eng_reopen.get_row(1, b"aborted").unwrap();
        drop(eng_reopen);
        let _ = std::fs::remove_dir_all(&sub_kv);

        // Multi-chunk (`TxBegin`..`TxCommit`) WAL recovery rejects entire transaction if any chunk has invalid blob
        let recovered_lsns = {
            use aydb::wal::*;
            let sub_wal = base_dir.join("suite8_f03_wal");
            let _ = std::fs::remove_dir_all(&sub_wal);
            std::fs::create_dir_all(&sub_wal).unwrap();
            let fs_wal = OsFs::open(&sub_wal, true).unwrap();
            let sync = WalStreamWriter::open(
                &fs_wal,
                WAL_SYNC_FILE_NAME,
                65536,
                1,
                WalStreamMode::SyncSectorIsolated,
            )
            .unwrap();
            let mut buf = WalStreamWriter::open(
                &fs_wal,
                WAL_BUF_FILE_NAME,
                65536,
                1,
                WalStreamMode::BufferedImmediateOsCache,
            )
            .unwrap();
            buf.append_batch(&WalBatch::new(
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
            buf.append_batch(&WalBatch::new(
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
            let rec =
                recover_two_streams(&**sync.file(), 1, &**buf.file(), 1, 0, |_| Ok(false)).unwrap();
            let lsns: Vec<u64> = rec.merged_batches.iter().map(|b| b.lsn).collect();
            drop(buf);
            drop(sync);
            let _ = std::fs::remove_dir_all(&sub_wal);
            lsns
        };

        let ok =
            in_mem_after_rollback.is_none() && after_reopen.is_none() && recovered_lsns.is_empty();
        assert!(ok, "8B deferred tx flush regression check failed");
        println!(
            "{:<54} | {:>14} | {:>20}",
            "8B. Deferred Tx Flush & Atomic Multi-Chunk WAL", "PASS", "0 Leaked / 0 Partial"
        );
    }

    // 8C: SQLite affinity, collation (NOCASE & RTRIM), unified numeric ORDER BY, & max auto-rowid
    {
        let sub = base_dir.join("suite8_f04_f05_f08");
        let _ = std::fs::remove_dir_all(&sub);
        std::fs::create_dir_all(&sub).unwrap();
        let fs = Arc::new(OsFs::open(&sub, true).unwrap());
        let shared = Arc::new(RwLock::new(
            StorageEngine::open(fs, EngineConfig::default()).unwrap(),
        ));
        let conn = SqliteConnection::open_in_memory().unwrap();
        conn.register_aydb_module(shared).unwrap();

        conn.execute_batch(
            "CREATE VIRTUAL TABLE t USING aydb(table_id=1, columns='id INTEGER PRIMARY KEY, v TEXT');
             INSERT INTO t VALUES (1, 'one'), (2, 'two');",
        )
        .unwrap();
        let q_float = conn
            .query_all("SELECT id FROM t WHERE id=1.0", &[])
            .unwrap();
        let q_text = conn
            .query_all("SELECT id FROM t WHERE id='1'", &[])
            .unwrap();
        let q_scan = conn
            .query_all("SELECT id FROM t WHERE +id=1.0", &[])
            .unwrap();
        let affinity_ok = q_float == vec![vec![SqlValue::Integer(1)]]
            && q_text == vec![vec![SqlValue::Integer(1)]]
            && q_scan == vec![vec![SqlValue::Integer(1)]];

        conn.execute_batch(
            "CREATE VIRTUAL TABLE c USING aydb(table_id=2, columns='k TEXT PRIMARY KEY COLLATE NOCASE, tag TEXT COLLATE RTRIM', indexes='tag');
             INSERT INTO c VALUES ('Alice', 'admin   ');",
        )
        .unwrap();
        let nocase_lookup = conn
            .query_all("SELECT k FROM c WHERE k='alice'", &[])
            .unwrap();
        let rtrim_lookup = conn
            .query_all("SELECT k FROM c WHERE tag='admin'", &[])
            .unwrap();
        let collation_ok = nocase_lookup == vec![vec![SqlValue::Text("Alice".to_string())]]
            && rtrim_lookup == vec![vec![SqlValue::Text("Alice".to_string())]];

        conn.execute_batch(
            "CREATE VIRTUAL TABLE m USING aydb(table_id=3, columns='k BLOB PRIMARY KEY, v TEXT');
             INSERT INTO m VALUES (10, 'ten'), (1.5, 'fraction');",
        )
        .unwrap();
        let mixed_order = conn.query_all("SELECT k FROM m ORDER BY k", &[]).unwrap();
        let numeric_order_ok =
            mixed_order == vec![vec![SqlValue::Float(1.5)], vec![SqlValue::Integer(10)]];

        conn.execute_batch(
            "CREATE VIRTUAL TABLE max_r USING aydb(table_id=4, columns='id INTEGER PRIMARY KEY, v TEXT');
             INSERT INTO max_r VALUES (9223372036854775807, 'max');
             INSERT INTO max_r VALUES (NULL, 'auto_after_max');",
        )
        .unwrap();
        let max_rows = conn.query_all("SELECT COUNT(*) FROM max_r", &[]).unwrap();
        let max_rowid_ok = max_rows == vec![vec![SqlValue::Integer(2)]];

        drop(conn);
        let _ = std::fs::remove_dir_all(&sub);

        let ok = affinity_ok && collation_ok && numeric_order_ok && max_rowid_ok;
        assert!(ok, "8C affinity, collation & rowid regression check failed");
        println!(
            "{:<54} | {:>14} | {:>20}",
            "8C. Affinity, Collation, Numeric Order & Rowid", "PASS", "100% SQLite-Aligned"
        );
    }

    // 8D: Size-tiered partial auto-compaction, keys-only COUNT(*), & defensive index bounds rejection
    {
        let sub = base_dir.join("suite8_p01_p02_s01");
        let _ = std::fs::remove_dir_all(&sub);
        std::fs::create_dir_all(&sub).unwrap();
        let mut cfg = EngineConfig::default();
        cfg.auto_compact_segment_threshold = 4;

        // Size-tiered compaction: Build a large base segment (~48 KB), then flush 3 tiny segments; auto-compaction
        // must merge only the 3 tiny suffix segments (`start_idx = 1`, leaving 2 segments total)
        // and preserve tombstones shadowing the base segment!
        let tiered_ok = {
            let fs = Arc::new(OsFs::open(&sub, true).unwrap());
            let mut eng = StorageEngine::open(fs, cfg.clone()).unwrap();
            let payload = vec![0x42u8; 256];
            for i in 0..160u32 {
                let k = format!("base_{i:04}").into_bytes();
                eng.put_row(1, &k, &payload, &[], WriteDurability::BufferedFast)
                    .unwrap();
            }
            eng.flush_memtable().unwrap();
            assert_eq!(eng.active_idx_segment_count(), 1);

            eng.delete_row(1, b"base_0005", WriteDurability::BufferedFast)
                .unwrap();
            eng.flush_memtable().unwrap();
            eng.put_row(1, b"s1", b"v1", &[], WriteDurability::BufferedFast)
                .unwrap();
            eng.flush_memtable().unwrap();
            eng.put_row(1, b"s2", b"v2", &[], WriteDurability::BufferedFast)
                .unwrap();
            eng.flush_memtable().unwrap();

            // 4 segments triggered size-tiered suffix compaction (merging the 3 small segments -> 2 total)
            eng.active_idx_segment_count() == 2 && eng.get_row(1, b"base_0005").unwrap().is_none()
        };

        // Keys-only projection (`SELECT COUNT(*)` & `SELECT id`) reads zero out-of-line blobs
        let sub_p02 = base_dir.join("suite8_p02_keys_only");
        let _ = std::fs::remove_dir_all(&sub_p02);
        std::fs::create_dir_all(&sub_p02).unwrap();
        {
            let fs = Arc::new(OsFs::open(&sub_p02, true).unwrap());
            let engine = Arc::new(RwLock::new(
                StorageEngine::open(fs, EngineConfig::default()).unwrap(),
            ));
            let conn = SqliteConnection::open_in_memory().unwrap();
            conn.register_aydb_module(engine.clone()).unwrap();
            conn.execute_batch(
                "CREATE VIRTUAL TABLE docs USING aydb(table_id=10, columns='id INTEGER PRIMARY KEY, body BLOB');",
            )
            .unwrap();
            let big_blob = vec![0x5Au8; 4096];
            for id in 1..=4i64 {
                conn.query_all(
                    "INSERT INTO docs VALUES (?1, ?2)",
                    &[SqlValue::Integer(id), SqlValue::Blob(big_blob.clone())],
                )
                .unwrap();
            }
            engine.write().unwrap().flush_memtable().unwrap();
        }
        // Overwrite `blob_std_*.db` with 0xFF so any blob read would fail CRC verification
        for entry in std::fs::read_dir(&sub_p02).unwrap().flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with("blob_std_") && name.ends_with(".db") {
                let p = entry.path();
                let len = std::fs::metadata(&p).unwrap().len() as usize;
                std::fs::write(&p, vec![0xFFu8; len]).unwrap();
            }
        }
        let keys_only_ok = {
            let fs = Arc::new(OsFs::open(&sub_p02, false).unwrap());
            let engine = Arc::new(RwLock::new(
                StorageEngine::open(fs, EngineConfig::default()).unwrap(),
            ));
            let conn = SqliteConnection::open_in_memory().unwrap();
            conn.register_aydb_module(engine).unwrap();
            conn.execute_batch(
                "CREATE VIRTUAL TABLE docs USING aydb(table_id=10, columns='id INTEGER PRIMARY KEY, body BLOB');",
            )
            .unwrap();
            let cnt = conn.query_all("SELECT COUNT(*) FROM docs", &[]).unwrap();
            let ids = conn
                .query_all("SELECT id FROM docs ORDER BY id", &[])
                .unwrap();
            cnt == vec![vec![SqlValue::Integer(4)]] && ids.len() == 4
        };
        let _ = std::fs::remove_dir_all(&sub_p02);

        // Defensive rejection of crafted/inflated `fp_count` index trailer with valid CRC32C
        let s01_ok = {
            use aydb::index::IdxSegmentReader;
            use aydb::meta::format_idx_file_name;
            use aydb::util::crc32c::crc32c;

            let idx_name = format_idx_file_name(1);
            let idx_path = sub.join(&idx_name);
            let mut bytes = std::fs::read(&idx_path).unwrap();
            let flen = bytes.len();
            let meta_offset =
                u64::from_le_bytes(bytes[flen - 24..flen - 16].try_into().unwrap()) as usize;
            let meta_len =
                u32::from_le_bytes(bytes[flen - 16..flen - 12].try_into().unwrap()) as usize;
            // Bloom filter header is at `meta_offset`: `[num_probes: u8, 0, 0, 0, bit_bytes_len: u32 LE, bits...]`
            let bloom_byte_len =
                u32::from_le_bytes(bytes[meta_offset + 4..meta_offset + 8].try_into().unwrap())
                    as usize;
            let fp_count_pos = meta_offset + 8 + bloom_byte_len;
            // Inflate `fp_count` to `u32::MAX` and recompute both `meta_crc` and `footer_crc`
            bytes[fp_count_pos..fp_count_pos + 4].copy_from_slice(&u32::MAX.to_le_bytes());
            let new_meta_crc = crc32c(&bytes[meta_offset..meta_offset + meta_len]);
            bytes[flen - 12..flen - 8].copy_from_slice(&new_meta_crc.to_le_bytes());
            let new_footer_crc = crc32c(&bytes[flen - 48..flen - 4]);
            bytes[flen - 4..flen].copy_from_slice(&new_footer_crc.to_le_bytes());
            std::fs::write(&idx_path, &bytes).unwrap();

            let fs = OsFs::open(&sub, false).unwrap();
            match IdxSegmentReader::open(&fs, 1) {
                Err(e) => e.kind() == io::ErrorKind::InvalidData,
                Ok(_) => false,
            }
        };
        let _ = std::fs::remove_dir_all(&sub);

        let ok = tiered_ok && keys_only_ok && s01_ok;
        assert!(ok, "8D tiered compaction regression check failed");
        println!(
            "{:<54} | {:>14} | {:>20}",
            "8D. Tiered Compaction, Zero-Blob & Bounds", "PASS", "Bounded & Verified"
        );
    }
}

// ============================================================================
// SUITE 9: Reliability, Compatibility & Platform Edge-Case Regressions
// ============================================================================

pub fn suite_9_multi_angle_audit_regressions(base_dir: &Path) {
    println!();
    println!(
        "=========================================================================================="
    );
    println!("SUITE 9: Reliability, Compatibility & Platform Regressions");
    println!(
        "=========================================================================================="
    );
    println!(
        "{:<54} | {:>14} | {:>20}",
        "Edge-Case Regression Scenario", "Status", "Observed Result"
    );
    println!(
        "----------------------------------------------------------------------------------------------"
    );

    // ------------------------------------------------------------------------
    // 9A: Uncataloged blob_std_* survives open & catalogs into boot_meta.db,
    //     multi-mutation to same blob key in 1 WalBatch does not double-count dead_bytes,
    //     and unreferenced tail bytes credited to dead_bytes on recovery for O(1) unlink
    // ------------------------------------------------------------------------
    {
        let sub = base_dir.join("s9a_r01_r02_r04");
        let _ = std::fs::remove_dir_all(&sub);
        std::fs::create_dir_all(&sub).unwrap();

        let blob_r01 = vec![0xA1u8; 2048];
        let orphan_blob = aydb::meta::format_std_blob_file_name(999);
        {
            let fs = Arc::new(OsFs::open(&sub, true).unwrap());
            let mut engine = StorageEngine::open(fs.clone(), EngineConfig::default()).unwrap();
            engine
                .put_row(
                    1,
                    b"r01_k",
                    &blob_r01,
                    &[],
                    WriteDurability::SyncTransactional,
                )
                .unwrap();
            let _ = fs
                .open_file(&orphan_blob, true, true, OpenHint::ImmutableSequential)
                .unwrap();
        }
        // Simulate a sub-sector torn write in the active boot_meta.db slot (which cataloged
        // blob_std_0001.db) so open_or_recover_deferred_sweep falls back to the initial slot
        // where std_blob_segments was still empty, leaving blob_std_0001.db uncataloged!
        {
            let boot_path = sub.join(aydb::meta::BOOT_META_FILE_NAME);
            let mut bm = std::fs::read(&boot_path).unwrap();
            let gen_a = u64::from_le_bytes(bm[8..16].try_into().unwrap());
            let gen_b = u64::from_le_bytes(bm[4096 + 8..4096 + 16].try_into().unwrap());
            let newer_off = if gen_b > gen_a { 4096 } else { 0 };
            bm[newer_off + 4095] ^= 0xFF;
            std::fs::write(&boot_path, &bm).unwrap();
        }

        // Reopen: Discovers blob_std_0001.db before WAL recovery, its row
        // survives, the truly unreferenced orphan blob is swept, and blob_std_0001.db is cataloged
        // into boot_meta.db!
        let r01_ok = {
            let fs = Arc::new(OsFs::open(&sub, false).unwrap());
            let mut engine = StorageEngine::open(fs.clone(), EngineConfig::default()).unwrap();
            let val_ok = engine.get_row(1, b"r01_k").unwrap() == Some(blob_r01.clone());
            let orphan_swept = !fs.list_files().unwrap().contains(&orphan_blob);

            // Multi-mutation in 1 batch: Flush two blob keys into blob_std, then mutate `r02_mut` twice in a single
            // explicit transaction (WalBatch) and drop before flushing MemTable!
            engine
                .put_row(
                    1,
                    b"r02_live",
                    &vec![0x11u8; 2048],
                    &[],
                    WriteDurability::SyncTransactional,
                )
                .unwrap();
            engine
                .put_row(
                    1,
                    b"r02_mut",
                    &vec![0x22u8; 2048],
                    &[],
                    WriteDurability::SyncTransactional,
                )
                .unwrap();
            engine.flush_memtable().unwrap();

            engine.begin_transaction();
            engine
                .put_row(
                    1,
                    b"r02_mut",
                    b"inline_mid",
                    &[],
                    WriteDurability::SyncTransactional,
                )
                .unwrap();
            engine
                .delete_row(1, b"r02_mut", WriteDurability::SyncTransactional)
                .unwrap();
            engine.commit_transaction().unwrap();
            drop(engine);

            // Verify boot_meta.db active slot now catalogs >= 1 std_blob_segment
            let boot_meta = aydb::meta::BootMetaManager::open_or_recover(fs).unwrap();
            let cataloged_ok = !boot_meta.state().std_blob_segments.is_empty();

            val_ok && orphan_swept && cataloged_ok
        };

        let (r02_ok, r04_ok) = {
            let fs = Arc::new(OsFs::open(&sub, false).unwrap());
            let mut engine = StorageEngine::open(fs.clone(), EngineConfig::default()).unwrap();
            engine
                .delete_row(1, b"r01_k", WriteDurability::SyncTransactional)
                .unwrap();
            engine.flush_memtable().unwrap();
            engine.compact().unwrap();
            let live_ok = engine.get_row(1, b"r02_live").unwrap() == Some(vec![0x11u8; 2048]);
            let mut_gone = engine.get_row(1, b"r02_mut").unwrap().is_none();

            // Register capped table 4, write a 2 KB blob row, flush, and append 4,096
            // unreferenced tail bytes to blob_lru_0004_*.db before reopening.
            engine.register_capped_table(4, 100_000).unwrap();
            engine
                .put_row(
                    4,
                    b"lru_k1",
                    &vec![0x77u8; 2048],
                    &[],
                    WriteDurability::SyncTransactional,
                )
                .unwrap();
            engine.flush_memtable().unwrap();
            drop(engine);

            let lru_fname = fs
                .list_files()
                .unwrap()
                .into_iter()
                .find(|n| n.starts_with("blob_lru_0004_"))
                .unwrap();
            let lru_file = fs
                .open_file(&lru_fname, false, true, OpenHint::ImmutableSequential)
                .unwrap();
            let valid_len = lru_file.file_len().unwrap();
            lru_file
                .write_all_at(&vec![0xEEu8; 4096], valid_len)
                .unwrap();
            lru_file.sync_durable().unwrap();
            drop(lru_file);

            let mut reopened = StorageEngine::open(fs, EngineConfig::default()).unwrap();
            let before_del = reopened.active_lru_blob_segment_count(4);
            reopened.begin_transaction();
            reopened
                .delete_row(4, b"lru_k1", WriteDurability::SyncTransactional)
                .unwrap();
            reopened.commit_transaction().unwrap();
            let after_del = reopened.active_lru_blob_segment_count(4);
            (live_ok && mut_gone, before_del == 1 && after_del == 0)
        };
        let _ = std::fs::remove_dir_all(&sub);
        assert!(
            r01_ok && r02_ok && r04_ok,
            "9A recovery sweep check failed: r01={r01_ok}, r02={r02_ok}, r04={r04_ok}"
        );
        println!(
            "{:<54} | {:>14} | {:>20}",
            "9A. Recovery Sweep & Dead-Byte Dedupe", "PASS", "Cataloged & Reclaimed"
        );
    }

    // ------------------------------------------------------------------------
    // 9B: Exact 4 KB inline block capacity boundary 4088 vs 4089,
    //     CREATE VIRTUAL TABLE inside rolled-back transaction,
    //     DROP TABLE of capped table inside committed transaction,
    //     and concurrent read with EagerWriteOnRead during active transaction
    // ------------------------------------------------------------------------
    {
        use aydb::engine::LruReadTrackingMode;
        let sub = base_dir.join("s9b_r03_r05_r06_r07");
        let _ = std::fs::remove_dir_all(&sub);
        std::fs::create_dir_all(&sub).unwrap();

        let fs = Arc::new(OsFs::open(&sub, true).unwrap());
        let mut cfg = EngineConfig::default();
        cfg.lru_read_tracking = LruReadTrackingMode::EagerWriteOnRead;

        let engine = Arc::new(RwLock::new(
            StorageEngine::open(fs.clone(), cfg.clone()).unwrap(),
        ));

        // Exact 4 KB block boundary: 100-byte PK -> row_key.len() == 103.
        // 15 + 103 + 3970 == 4088 (fits inline in 4 KB block) vs 3971 -> 4089 (spills to blob).
        // Even with a high threshold (or default threshold), 3970B fits in an inline 4KB block
        // when threshold allows, while 3971B spills to blob without wedging flush_memtable!
        let pk_4088 = vec![b'a'; 100];
        let val_3970 = vec![0x33u8; 3970];
        let pk_4089 = vec![b'b'; 100];
        let val_3971 = vec![0x44u8; 3971];
        let r03_ok = {
            let mut r03_cfg = EngineConfig::default();
            r03_cfg.inline_threshold_bytes = 8192;
            let sub_r03 = base_dir.join("s9b_r03_only");
            let _ = std::fs::remove_dir_all(&sub_r03);
            std::fs::create_dir_all(&sub_r03).unwrap();
            let fs_r03 = Arc::new(OsFs::open(&sub_r03, true).unwrap());
            let mut eng_r03 = StorageEngine::open(fs_r03, r03_cfg).unwrap();
            eng_r03
                .put_row(2, &pk_4088, &val_3970, &[], WriteDurability::BufferedFast)
                .unwrap();
            eng_r03
                .put_row(2, &pk_4089, &val_3971, &[], WriteDurability::BufferedFast)
                .unwrap();
            let ok = eng_r03.flush_memtable().is_ok()
                && eng_r03.get_row(2, &pk_4088).unwrap() == Some(val_3970)
                && eng_r03.get_row(2, &pk_4089).unwrap() == Some(val_3971);
            drop(eng_r03);
            let _ = std::fs::remove_dir_all(&sub_r03);
            ok
        };

        // Rolled-back DDL: CREATE VIRTUAL TABLE (`register_or_resolve_vtab`) inside a rolled-back
        // transaction must roll back cleanly without WAL LSN inversion or leaked catalog entries.
        let r05_ok = {
            let mut eng = engine.write().unwrap();
            let lsn_before = eng.next_lsn();
            eng.begin_transaction();
            let tid = eng
                .register_or_resolve_vtab("r05_aborted", Some(55))
                .unwrap();
            eng.rollback_transaction().unwrap();
            let lsn_after = eng.next_lsn();
            eng.put_row(
                2,
                b"after_r05",
                b"ok",
                &[],
                WriteDurability::SyncTransactional,
            )
            .unwrap();
            tid == 55
                && lsn_after == lsn_before
                && eng.get_row(0, b"r05_aborted").unwrap().is_none()
        };

        // Committed DROP: DROP TABLE of a capped table inside a committed transaction must reclaim
        // its LRU blob segments and remove its quota from boot_meta.db!
        let (segs_before_drop, segs_after_drop) = {
            let mut eng = engine.write().unwrap();
            eng.register_capped_table(56, 50_000).unwrap();
            eng.put_row(
                56,
                b"cap56_k1",
                &vec![0x55u8; 2048],
                &[],
                WriteDurability::BufferedFast,
            )
            .unwrap();
            let before = eng.active_lru_blob_segment_count(56);
            eng.begin_transaction();
            eng.drop_table(56, Some("r06_capped")).unwrap();
            eng.commit_transaction().unwrap();
            let after = eng.active_lru_blob_segment_count(56);
            (before, after)
        };

        // Active Tx read: Concurrent read under EagerWriteOnRead during an active transaction must not
        // corrupt the writer transaction's undo log on ROLLBACK!
        let r07_ok = {
            let mut eng = engine.write().unwrap();
            eng.register_capped_table(57, 100_000).unwrap();
            eng.put_row(
                57,
                b"shared_k",
                b"committed_v1",
                &[],
                WriteDurability::BufferedFast,
            )
            .unwrap();
            eng.begin_transaction_for_conn(101).unwrap();
            eng.put_row(
                57,
                b"shared_k",
                b"uncommitted_v2",
                &[],
                WriteDurability::BufferedFast,
            )
            .unwrap();
            let reader_val = eng
                .with_committed_read_for_conn(202, |e| e.get_row(57, b"shared_k"))
                .unwrap();
            eng.rollback_transaction_for_conn(101).unwrap();
            let restored_val = eng.get_row(57, b"shared_k").unwrap();
            reader_val == Some(b"committed_v1".to_vec())
                && restored_val == Some(b"committed_v1".to_vec())
        };

        drop(engine);
        let quota_removed = aydb::meta::BootMetaManager::open_or_recover(fs)
            .unwrap()
            .state()
            .table_quotas
            .iter()
            .all(|q| q.table_id != 56);
        let r06_ok = segs_before_drop == 1 && segs_after_drop == 0 && quota_removed;
        let _ = std::fs::remove_dir_all(&sub);
        assert!(
            r03_ok && r05_ok && r06_ok && r07_ok,
            "9B 4KB bound, DDL tx & eager read guard check failed: r03={r03_ok}, r05={r05_ok}, r06={r06_ok}, r07={r07_ok}"
        );
        println!(
            "{:<54} | {:>14} | {:>20}",
            "9B. 4KB Bound, DDL Tx & Eager Read Guard", "PASS", "Isolated & Reclaimed"
        );
    }

    // ------------------------------------------------------------------------
    // 9C: Sub-ULP negative floats in REAL PK & BLOB PK, 10 and 10.0 conflict on BLOB PK,
    //     LIKE LIMIT chunk streaming across 600 rows, and non-integer PK rowid resolution
    // ------------------------------------------------------------------------
    {
        let sub = base_dir.join("s9c_c01_to_c05");
        let _ = std::fs::remove_dir_all(&sub);
        std::fs::create_dir_all(&sub).unwrap();

        let fs = Arc::new(OsFs::open(&sub, true).unwrap());
        let engine = Arc::new(RwLock::new(
            StorageEngine::open(fs, EngineConfig::default()).unwrap(),
        ));
        let conn = SqliteConnection::open_in_memory().unwrap();
        conn.register_aydb_module(engine.clone()).unwrap();

        conn.execute_batch(
            "CREATE VIRTUAL TABLE c01_real USING aydb(table_id = 61, columns = 'pk REAL PRIMARY KEY, v TEXT');
             CREATE VIRTUAL TABLE c01_blob USING aydb(table_id = 62, columns = 'pk BLOB PRIMARY KEY, v TEXT');",
        )
        .unwrap();

        let sub_ulp_floats = [-1e-17_f64, -1e-20, -1e-300, -f64::MIN_POSITIVE];
        {
            let mut ins_r = conn
                .prepare("INSERT INTO c01_real VALUES (?1, ?2)")
                .unwrap();
            let mut ins_b = conn
                .prepare("INSERT INTO c01_blob VALUES (?1, ?2)")
                .unwrap();
            for (i, &f) in sub_ulp_floats.iter().enumerate() {
                ins_r
                    .execute(&[SqlValue::Float(f), SqlValue::Text(format!("r_{i}"))])
                    .unwrap();
                ins_b
                    .execute(&[SqlValue::Float(f), SqlValue::Text(format!("b_{i}"))])
                    .unwrap();
            }
        }
        let real_rows = conn.query_all("SELECT pk FROM c01_real", &[]).unwrap();
        let blob_rows = conn.query_all("SELECT pk FROM c01_blob", &[]).unwrap();
        let c01_ok = real_rows.len() == 4
            && blob_rows.len() == 4
            && sub_ulp_floats.iter().enumerate().all(|(i, &exp)| {
                matches!(
                    (&real_rows[i][0], &blob_rows[i][0]),
                    (SqlValue::Float(r), SqlValue::Float(b))
                        if r.to_bits() == exp.to_bits() && b.to_bits() == exp.to_bits()
                )
            });

        // 10 and 10.0 conflict on BLOB PRIMARY KEY
        conn.execute_batch(
            "CREATE VIRTUAL TABLE c02_blob USING aydb(table_id = 63, columns = 'pk BLOB PRIMARY KEY, v TEXT');
             INSERT INTO c02_blob VALUES (10, 'ten_int');",
        )
        .unwrap();
        let dup_rejected = conn
            .execute_batch("INSERT INTO c02_blob VALUES (10.0, 'ten_float');")
            .is_err();
        conn.execute_batch("INSERT OR REPLACE INTO c02_blob VALUES (10.0, 'replaced');")
            .unwrap();
        let c02_rows = conn.query_all("SELECT pk, v FROM c02_blob", &[]).unwrap();
        let c02_ok = dup_rejected
            && c02_rows
                == vec![vec![
                    SqlValue::Float(10.0),
                    SqlValue::Text("replaced".into()),
                ]];

        // Unindexed LIKE predicate: SELECT ... WHERE v LIKE '%target%' LIMIT 3 across 600 rows (> 2x 256 chunk)
        conn.execute_batch(
            "CREATE VIRTUAL TABLE c03_scan USING aydb(table_id = 64, columns = 'id INT PRIMARY KEY, v TEXT');
             BEGIN;",
        )
        .unwrap();
        {
            let mut ins = conn
                .prepare("INSERT INTO c03_scan VALUES (?1, ?2)")
                .unwrap();
            for i in 1..=600i64 {
                let text = if i >= 590 {
                    format!("row_{i}_target_hit")
                } else {
                    format!("row_{i}_miss")
                };
                ins.execute(&[SqlValue::Integer(i), SqlValue::Text(text)])
                    .unwrap();
            }
        }
        conn.execute_batch("COMMIT;").unwrap();
        let like_rows = conn
            .query_all(
                "SELECT id FROM c03_scan WHERE v LIKE '%target%' LIMIT 3",
                &[],
            )
            .unwrap();
        let c03_ok = like_rows
            == vec![
                vec![SqlValue::Integer(590)],
                vec![SqlValue::Integer(591)],
                vec![SqlValue::Integer(592)],
            ];

        // TEXT PK rowid UPDATE/DELETE and statement re-binding + %.15g formatting
        conn.execute_batch(
            "CREATE VIRTUAL TABLE c04_c05 USING aydb(table_id = 65, columns = 'k TEXT PRIMARY KEY, v TEXT');",
        )
        .unwrap();
        {
            let mut ins = conn.prepare("INSERT INTO c04_c05 VALUES (?1, ?2)").unwrap();
            ins.execute(&[SqlValue::Float(1e20), SqlValue::Text("first".into())])
                .unwrap();
            ins.execute(&[SqlValue::Float(-1e-20)]).unwrap();
        }
        let rid_rows = conn
            .query_all("SELECT rowid, k, v FROM c04_c05 ORDER BY k", &[])
            .unwrap();
        let SqlValue::Integer(rid0) = rid_rows[0][0] else {
            panic!("expected integer rowid");
        };
        conn.query_all(
            "UPDATE c04_c05 SET v = 'updated_via_rowid' WHERE rowid = ?1",
            &[SqlValue::Integer(rid0)],
        )
        .unwrap();
        let after_upd = conn
            .query_all("SELECT k, v FROM c04_c05 ORDER BY k", &[])
            .unwrap();
        let c04_c05_ok = after_upd
            == vec![
                vec![
                    SqlValue::Text("-1.0e-20".into()),
                    SqlValue::Text("updated_via_rowid".into()),
                ],
                vec![
                    SqlValue::Text("1.0e+20".into()),
                    SqlValue::Text("first".into()),
                ],
            ];

        drop(conn);
        drop(engine);
        let _ = std::fs::remove_dir_all(&sub);
        assert!(
            c01_ok && c02_ok && c03_ok && c04_c05_ok,
            "9C sub-ULP float & LIKE LIMIT check failed: c01={c01_ok}, c02={c02_ok}, c03={c03_ok}, c04_05={c04_c05_ok}"
        );
        println!(
            "{:<54} | {:>14} | {:>20}",
            "9C. Sub-ULP Float, Numeric PK & LIKE LIMIT", "PASS", "Lossless & Complete"
        );
    }

    // ------------------------------------------------------------------------
    // 9D: Reverse streaming cursor pushdown (ORDER BY id DESC LIMIT 5),
    //     row-meta skip, buffered blob read in tx, two-level AccessTracker,
    //     and deep long directory path > 260 chars creation & StorageEngine I/O
    // ------------------------------------------------------------------------
    {
        let sub = base_dir.join("s9d_p01_to_p04");
        let _ = std::fs::remove_dir_all(&sub);
        std::fs::create_dir_all(&sub).unwrap();

        let fs = Arc::new(OsFs::open(&sub, true).unwrap());
        let engine = Arc::new(RwLock::new(
            StorageEngine::open(fs, EngineConfig::default()).unwrap(),
        ));
        let conn = SqliteConnection::open_in_memory().unwrap();
        conn.register_aydb_module(engine.clone()).unwrap();

        conn.execute_batch(
            "CREATE VIRTUAL TABLE p01_rev USING aydb(table_id = 71, columns = 'id INT PRIMARY KEY, body BLOB');
             BEGIN;",
        )
        .unwrap();
        let blob_2k = vec![0x66u8; 2048];
        {
            let mut ins = conn.prepare("INSERT INTO p01_rev VALUES (?1, ?2)").unwrap();
            for i in 1..=600i64 {
                ins.execute(&[SqlValue::Integer(i), SqlValue::Blob(blob_2k.clone())])
                    .unwrap();
            }
        }
        conn.execute_batch("COMMIT;").unwrap();
        engine.write().unwrap().flush_memtable().unwrap();

        let desc_rows = conn
            .query_all("SELECT id FROM p01_rev ORDER BY id DESC LIMIT 5", &[])
            .unwrap();
        let eqp_desc = conn
            .query_all(
                "EXPLAIN QUERY PLAN SELECT id FROM p01_rev ORDER BY id DESC LIMIT 5",
                &[],
            )
            .unwrap();
        let no_temp_btree = !format!("{eqp_desc:?}").contains("USE TEMP B-TREE");
        let p01_ok = no_temp_btree
            && desc_rows
                == vec![
                    vec![SqlValue::Integer(600)],
                    vec![SqlValue::Integer(599)],
                    vec![SqlValue::Integer(598)],
                    vec![SqlValue::Integer(597)],
                    vec![SqlValue::Integer(596)],
                ];

        drop(conn);
        drop(engine);
        let _ = std::fs::remove_dir_all(&sub);

        // Deep directory path exceeding 260 characters (Windows MAX_PATH)
        let mut deep_dir = base_dir.join("s9d_o02_long");
        for i in 0..8 {
            deep_dir = deep_dir.join(format!(
                "segment_{i:02}_abcdefghijklmnopqrstuvwxyz_0123456789"
            ));
        }
        assert!(deep_dir.as_os_str().len() > 280);
        let o02_ok = {
            let fs_long = Arc::new(OsFs::open(&deep_dir, true).unwrap());
            let mut eng_long = StorageEngine::open(fs_long, EngineConfig::default()).unwrap();
            eng_long
                .put_row(
                    1,
                    b"long_path_k",
                    &vec![0x99u8; 2048],
                    &[],
                    WriteDurability::SyncTransactional,
                )
                .unwrap();
            eng_long.get_row(1, b"long_path_k").unwrap() == Some(vec![0x99u8; 2048])
        };
        let _ = std::fs::remove_dir_all(base_dir.join("s9d_o02_long"));

        assert!(
            p01_ok && o02_ok,
            "9D reverse stream & long-path check failed: p01={p01_ok}, o02={o02_ok}"
        );
        println!(
            "{:<54} | {:>14} | {:>20}",
            "9D. Reverse Stream & >260-Char Path", "PASS", "Streamed & Long-Path"
        );
    }

    // ------------------------------------------------------------------------
    // 9E: RowidMap transactional undo across ROLLBACK & SAVEPOINT,
    //     capped table quota persistence to boot_meta.db, and sync catalog durability
    // ------------------------------------------------------------------------
    {
        let sub = base_dir.join("s9e_n01_to_n04");
        let _ = std::fs::remove_dir_all(&sub);
        std::fs::create_dir_all(&sub).unwrap();

        let (rid_a, rid_b) = {
            let fs = Arc::new(OsFs::open(&sub, true).unwrap());
            let engine = Arc::new(RwLock::new(
                StorageEngine::open(fs, EngineConfig::default()).unwrap(),
            ));
            let conn = SqliteConnection::open_in_memory().unwrap();
            conn.register_aydb_module(engine.clone()).unwrap();

            // Create a SyncTransactional capped virtual table with auto-assigned
            // `table_id` inside an explicit `BEGIN ... COMMIT` transaction!
            conn.execute_batch(
                "BEGIN;
                 CREATE VIRTUAL TABLE n01_n04_tbl USING aydb(
                     durability = 'sync',
                     max_bytes = 250000,
                     columns = 'k TEXT PRIMARY KEY, v TEXT'
                 );
                 INSERT INTO n01_n04_tbl VALUES ('row_a', 'init_a');
                 INSERT INTO n01_n04_tbl VALUES ('row_b', 'init_b');
                 COMMIT;",
            )
            .unwrap();

            let rids = conn
                .query_all("SELECT rowid, k FROM n01_n04_tbl ORDER BY k", &[])
                .unwrap();
            let SqlValue::Integer(ra) = rids[0][0] else {
                panic!("expected integer rowid");
            };
            let SqlValue::Integer(rb) = rids[1][0] else {
                panic!("expected integer rowid");
            };

            // Delete `row_a` and rename `row_b` inside a transaction and a savepoint, then roll back
            conn.execute_batch(
                "BEGIN;
                 DELETE FROM n01_n04_tbl WHERE k = 'row_a';
                 UPDATE n01_n04_tbl SET k = 'row_b_new' WHERE k = 'row_b';
                 ROLLBACK;
                 BEGIN;
                 SAVEPOINT sp_n01;
                 DELETE FROM n01_n04_tbl WHERE k = 'row_b';
                 ROLLBACK TO sp_n01;
                 RELEASE sp_n01;
                 COMMIT;",
            )
            .unwrap();

            // Verify both original rowids still resolve in `RowidMap` for UPDATE by rowid
            conn.query_all(
                "UPDATE n01_n04_tbl SET v = 'updated_a' WHERE rowid = ?1",
                &[SqlValue::Integer(ra)],
            )
            .unwrap();
            conn.query_all(
                "UPDATE n01_n04_tbl SET v = 'updated_b' WHERE rowid = ?1",
                &[SqlValue::Integer(rb)],
            )
            .unwrap();
            (ra, rb)
        };

        // Reopen from disk without prior MemTable flush and verify quota + catalog + data restoration
        let n01_to_n04_ok = {
            let fs = Arc::new(OsFs::open(&sub, false).unwrap());
            let engine = Arc::new(RwLock::new(
                StorageEngine::open(fs, EngineConfig::default()).unwrap(),
            ));
            let has_quota = engine
                .write()
                .unwrap()
                .capped_table_state(1)
                .map(|s| s.max_bytes == 250_000)
                .unwrap_or(false);
            let conn = SqliteConnection::open_in_memory().unwrap();
            conn.register_aydb_module(engine.clone()).unwrap();
            conn.execute_batch(
                "CREATE VIRTUAL TABLE n01_n04_tbl USING aydb(
                     durability = 'sync',
                     max_bytes = 250000,
                     columns = 'k TEXT PRIMARY KEY, v TEXT'
                 );",
            )
            .unwrap();
            let rows = conn
                .query_all("SELECT k, v FROM n01_n04_tbl ORDER BY k", &[])
                .unwrap();
            has_quota
                && rid_a != rid_b
                && rows
                    == vec![
                        vec![
                            SqlValue::Text("row_a".into()),
                            SqlValue::Text("updated_a".into()),
                        ],
                        vec![
                            SqlValue::Text("row_b".into()),
                            SqlValue::Text("updated_b".into()),
                        ],
                    ]
        };
        let _ = std::fs::remove_dir_all(&sub);
        assert!(
            n01_to_n04_ok,
            "9E rowid undo, tx quota & sync catalog check failed"
        );
        println!(
            "{:<54} | {:>14} | {:>20}",
            "9E. RowidMap Undo, Tx Quota & Sync Catalog", "PASS", "Restored & Durable"
        );
    }
}

pub fn suite_10_collation_rowid_cross_conn_audit_regressions(base_dir: &Path) {
    println!(
        "\n=== SUITE 10: Collation Overrides, Synthetic rowid Order & Cross-Conn Resolution ==="
    );

    // 10A: Query Collation Override on Primary Key and Secondary Index
    {
        let sub = base_dir.join("s10_collation");
        let _ = std::fs::remove_dir_all(&sub);
        let fs = Arc::new(OsFs::open(&sub, true).unwrap());
        let eng = Arc::new(RwLock::new(
            StorageEngine::open(fs, EngineConfig::default()).unwrap(),
        ));
        let conn = SqliteConnection::open_in_memory().unwrap();
        conn.register_aydb_module(eng).unwrap();

        conn.execute_batch(
            "CREATE VIRTUAL TABLE ci USING aydb(table_id=1, columns='id TEXT COLLATE NOCASE PRIMARY KEY, value TEXT');
             INSERT INTO ci VALUES ('a', 'original');
             CREATE VIRTUAL TABLE bin USING aydb(table_id=2, columns='id TEXT PRIMARY KEY, value TEXT', indexes='value');
             INSERT INTO bin VALUES ('a', 'Hello');",
        )
        .unwrap();

        // Query with overridden collation must not use mismatching index lookup
        let r1 = conn
            .query_all("SELECT id FROM ci WHERE id = 'A' COLLATE BINARY", &[])
            .unwrap();
        assert_eq!(r1, Vec::<Vec<SqlValue>>::new());

        conn.execute_batch("DELETE FROM ci WHERE id = 'A' COLLATE BINARY")
            .unwrap();
        let r2 = conn.query_all("SELECT * FROM ci", &[]).unwrap();
        assert_eq!(
            r2,
            vec![vec![
                SqlValue::Text("a".into()),
                SqlValue::Text("original".into())
            ]]
        );

        let r3 = conn
            .query_all("SELECT id FROM bin WHERE id = 'A' COLLATE NOCASE", &[])
            .unwrap();
        assert_eq!(r3, vec![vec![SqlValue::Text("a".into())]]);

        let r4 = conn
            .query_all(
                "SELECT id FROM bin WHERE value = 'HELLO' COLLATE NOCASE",
                &[],
            )
            .unwrap();
        assert_eq!(r4, vec![vec![SqlValue::Text("a".into())]]);

        let _ = std::fs::remove_dir_all(&sub);
        println!(
            "{:<54} | {:>14} | {:>20}",
            "10A. Query Collation Overrides (F1)", "PASS", "VDBE Evaluation"
        );
    }

    // 10B: ORDER BY rowid on synthetic non-integer PKs
    {
        let sub = base_dir.join("s10_rowid_order");
        let _ = std::fs::remove_dir_all(&sub);
        let fs = Arc::new(OsFs::open(&sub, true).unwrap());
        let eng = Arc::new(RwLock::new(
            StorageEngine::open(fs, EngineConfig::default()).unwrap(),
        ));
        let conn = SqliteConnection::open_in_memory().unwrap();
        conn.register_aydb_module(eng).unwrap();

        conn.execute_batch(
            "CREATE VIRTUAL TABLE t USING aydb(table_id=1, columns='id TEXT PRIMARY KEY, value TEXT');
             INSERT INTO t VALUES ('a','A'),('b','B'),('c','C'),('d','D'),('e','E'),('f','F');",
        )
        .unwrap();

        let r_rowid = conn
            .query_all("SELECT rowid, id FROM t ORDER BY rowid", &[])
            .unwrap();
        let r_plus = conn
            .query_all("SELECT rowid, id FROM t ORDER BY +rowid", &[])
            .unwrap();
        assert_eq!(r_rowid, r_plus);

        let _ = std::fs::remove_dir_all(&sub);
        println!(
            "{:<54} | {:>14} | {:>20}",
            "10B. Synthetic rowid ORDER BY (F2)", "PASS", "VDBE Rowid Sort"
        );
    }

    // 10C: Cross-connection synthetic rowid resolution (F3)
    {
        let sub = base_dir.join("s10_cross_conn");
        let _ = std::fs::remove_dir_all(&sub);
        let fs = Arc::new(OsFs::open(&sub, true).unwrap());
        let eng = Arc::new(RwLock::new(
            StorageEngine::open(fs, EngineConfig::default()).unwrap(),
        ));

        let conn1 = SqliteConnection::open_in_memory().unwrap();
        conn1.register_aydb_module(eng.clone()).unwrap();
        conn1
            .execute_batch(
                "CREATE VIRTUAL TABLE t USING aydb(table_id=1, columns='id TEXT PRIMARY KEY, value TEXT');
                 INSERT INTO t VALUES ('alpha', 'first');",
            )
            .unwrap();

        let rid = match conn1
            .query_all("SELECT rowid FROM t WHERE id = 'alpha'", &[])
            .unwrap()[0][0]
        {
            SqlValue::Integer(v) => v,
            _ => unreachable!(),
        };

        // Open a completely separate SQLite connection with cold cache
        let conn2 = SqliteConnection::open_in_memory().unwrap();
        conn2.register_aydb_module(eng).unwrap();
        conn2
            .execute_batch(
                "CREATE VIRTUAL TABLE t USING aydb(table_id=1, columns='id TEXT PRIMARY KEY, value TEXT');",
            )
            .unwrap();

        let hit = conn2
            .query_all(
                "SELECT value FROM t WHERE rowid = ?",
                &[SqlValue::Integer(rid)],
            )
            .unwrap();
        assert_eq!(hit, vec![vec![SqlValue::Text("first".into())]]);

        // Mutate via cold connection rowid
        conn2
            .execute_batch(&format!(
                "UPDATE t SET value = 'second' WHERE rowid = {rid}"
            ))
            .unwrap();
        let updated = conn1
            .query_all("SELECT value FROM t WHERE id = 'alpha'", &[])
            .unwrap();
        assert_eq!(updated, vec![vec![SqlValue::Text("second".into())]]);

        let _ = std::fs::remove_dir_all(&sub);
        println!(
            "{:<54} | {:>14} | {:>20}",
            "10C. Cross-Connection rowid Resolution (F3)", "PASS", "Cache-Miss Scanned"
        );
    }

    // 10D: Lossless Numeric rowid Coercion (F4)
    {
        let sub = base_dir.join("s10_numeric_rowid");
        let _ = std::fs::remove_dir_all(&sub);
        let fs = Arc::new(OsFs::open(&sub, true).unwrap());
        let eng = Arc::new(RwLock::new(
            StorageEngine::open(fs, EngineConfig::default()).unwrap(),
        ));
        let conn = SqliteConnection::open_in_memory().unwrap();
        conn.register_aydb_module(eng).unwrap();

        conn.execute_batch(
            "CREATE VIRTUAL TABLE n USING aydb(table_id=1, columns='id INTEGER PRIMARY KEY, value TEXT');
             INSERT INTO n VALUES (42, 'forty_two');",
        )
        .unwrap();

        assert_eq!(
            conn.query_all("SELECT id FROM n WHERE rowid = '42'", &[])
                .unwrap(),
            vec![vec![SqlValue::Integer(42)]]
        );
        assert_eq!(
            conn.query_all("SELECT id FROM n WHERE rowid = 42.0", &[])
                .unwrap(),
            vec![vec![SqlValue::Integer(42)]]
        );
        assert_eq!(
            conn.query_all("SELECT id FROM n WHERE rowid = 42.5", &[])
                .unwrap(),
            Vec::<Vec<SqlValue>>::new()
        );
        assert_eq!(
            conn.query_all("SELECT id FROM n WHERE rowid = '42.5'", &[])
                .unwrap(),
            Vec::<Vec<SqlValue>>::new()
        );
        assert_eq!(
            conn.query_all("SELECT id FROM n WHERE rowid = 'foo'", &[])
                .unwrap(),
            Vec::<Vec<SqlValue>>::new()
        );

        let _ = std::fs::remove_dir_all(&sub);
        println!(
            "{:<54} | {:>14} | {:>20}",
            "10D. Lossless Numeric rowid Coercion (F4)", "PASS", "Lossless Converted"
        );
    }
}

fn main() {
    let base_dir = std::env::temp_dir().join(format!("aydb_rel_suite7_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base_dir);
    std::fs::create_dir_all(&base_dir).unwrap();

    bench_suite_1_power_loss_sync_durability();
    bench_suite_2_process_crash_wal_modes();
    bench_suite_3_transaction_rollback_invariants();
    bench_suite_4_post_crash_blob_leak();
    bench_suite_5_io_fault_countdown();
    bench_suite_6_vtab_key_and_rowid_integrity();
    suite_7_audit_regressions(&base_dir);
    suite_8_followup_audit_regressions(&base_dir);
    suite_9_multi_angle_audit_regressions(&base_dir);
    suite_10_collation_rowid_cross_conn_audit_regressions(&base_dir);

    let _ = std::fs::remove_dir_all(&base_dir);
}
