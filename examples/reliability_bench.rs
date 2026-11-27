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
//! 6. SQLite VTab Key Encoding & Rowid Integrity.

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

impl Default for FaultInjectingFs {
    fn default() -> Self {
        Self::new()
    }
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
                if inject_torn_tail
                    && name == "wal_sync.db"
                    && let Some(last) = st.last_write.clone()
                {
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

fn bench_suite_1_power_loss_sync_durability(failures: &mut Vec<String>) -> (f64, f64) {
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
    if survived_strict != COMMITS {
        failures.push(format!(
            "1A. Volatile Drive Cache / Unix PAL: survived {survived_strict}/{COMMITS}"
        ));
    }

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
    if survived_fua_torn != COMMITS {
        failures.push(format!(
            "1B. FUA Write-Through + 512B Torn Tail Sector: survived {survived_fua_torn}/{COMMITS}"
        ));
    }

    (pct_strict, pct_fua)
}

// ============================================================================
// Suite 2: Process Crash (`kill -9`) Survival Across WAL Buffering Modes
// ============================================================================

fn bench_suite_2_process_crash_wal_modes(failures: &mut Vec<String>) {
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
        let cfg = EngineConfig {
            buf_wal_mode: mode,
            ..Default::default()
        };

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
        if mode == WalStreamMode::BufferedImmediateOsCache && hits != N_COMMITS {
            failures.push(format!("2. {label}: recovered {hits}/{N_COMMITS}"));
        }
    }
}

// ============================================================================
// Suite 3: Transaction Atomicity & Rollback Invariants (Differential Oracle)
// ============================================================================

fn bench_suite_3_transaction_rollback_invariants(failures: &mut Vec<String>) {
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
        if !ok {
            failures.push("3A. Small Inline Tx Rollback (< 4 MB) failed".to_string());
        }
    }

    // Scenario 3B: Sealed Blob Segment Overwrite + Rollback
    {
        let fs = Arc::new(FaultInjectingFs::new());
        let cfg = EngineConfig {
            inline_threshold_bytes: 128,
            max_blob_segment_bytes: 4096,
            ..Default::default()
        }; // Seal segment after 1 x 4 KB blob
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
        if status != "PASS" {
            failures.push(format!("3B. Sealed Blob Overwrite + Rollback: {note}"));
        }
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
        if !ok {
            failures.push(format!(
                "3C. Large Transaction (> 4 MB) Rollback Atomicity: {leaked_rows}/5000 leaked"
            ));
        }
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
        if !ok {
            failures.push(format!(
                "3D. Multi-Segment Range Scan w/ Tombstones: returned {} rows",
                rows.len()
            ));
        }
    }
}

// ============================================================================
// Suite 4: Post-Crash Blob Garbage Collection & Storage Leak
// ============================================================================

fn bench_suite_4_post_crash_blob_leak(failures: &mut Vec<String>) {
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
        let cfg = EngineConfig {
            inline_threshold_bytes: 128,
            max_blob_segment_bytes: 16 * 1024,
            ..Default::default()
        }; // 16 KB per blob segment (4 blobs of 4 KB)

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
        if status != "PASS" {
            failures.push(format!(
                "4A. Delete Full Sealed Blob Seg + Crash Before Flush: {detail}"
            ));
        }
    }

    // Scenario 4B: Delete 3/4 of each sealed blob segment -> Crash -> Recover -> Delete last 1/4 -> Flush & Compact
    {
        let fs = Arc::new(FaultInjectingFs::new());
        let cfg = EngineConfig {
            inline_threshold_bytes: 128,
            max_blob_segment_bytes: 16 * 1024,
            ..Default::default()
        }; // 4 blobs of ~4 KB per segment

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
        if leaked_sealed != 0 {
            failures.push(format!(
                "4B. Partial Blob Delete + Crash + Delete Rest (GC): {leaked_sealed} leaked"
            ));
        }
    }
}

// ============================================================================
// Suite 5: Deterministic I/O Fault Countdown During `flush_memtable()`
// ============================================================================

fn bench_suite_5_io_fault_countdown(failures: &mut Vec<String>) {
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
            if let Ok(Some(actual_v)) = reopened.get_row(1, k)
                && &actual_v == expected_v
            {
                surviving += 1;
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
        if surviving != TOTAL_KEYS {
            failures.push(format!(
                "5. I/O Fault Step #{fault_step}: survived {surviving}/{TOTAL_KEYS}"
            ));
        }
    }
}

// ============================================================================
// Suite 6: SQLite VTab Key Encoding & Rowid Integrity
// ============================================================================

fn bench_suite_6_vtab_key_and_rowid_integrity(failures: &mut Vec<String>) {
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
    if !ok_order {
        failures.push("6A. REAL (f64) PK Index Scan Sort Order failed".to_string());
    }

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
        if let Some(prev) = seen_crc.insert(h, candidate.clone())
            && prev != candidate
        {
            pair = Some((prev, candidate));
            break;
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
    // Query Alice's rowid, then delete by that rowid; Bob's INSERT must not take over Alice's identity!
    let r1 = conn
        .query_all(&format!("SELECT rowid FROM users_t WHERE id = '{k1}'"), &[])
        .unwrap();
    let rid1 = match r1[0][0] {
        SqlValue::Integer(v) => v,
        _ => unreachable!(),
    };
    // Re-insert or touch k2, then delete rowid = rid1
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
    if !ok_collision {
        failures.push("6B. TEXT PK 64-Bit rowid Hash Collision failed".to_string());
    }

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
    if !ok_range {
        failures.push("6C. Inverted/Empty Range Query failed".to_string());
    }

    drop(conn);
    let _ = std::fs::remove_dir_all(&dir);
}

fn main() {
    let mut failures = Vec::new();
    bench_suite_1_power_loss_sync_durability(&mut failures);
    bench_suite_2_process_crash_wal_modes(&mut failures);
    bench_suite_3_transaction_rollback_invariants(&mut failures);
    bench_suite_4_post_crash_blob_leak(&mut failures);
    bench_suite_5_io_fault_countdown(&mut failures);
    bench_suite_6_vtab_key_and_rowid_integrity(&mut failures);
    if !failures.is_empty() {
        eprintln!("\nFAIL: {} reliability check(s) failed:", failures.len());
        for failure in &failures {
            eprintln!("  - {failure}");
        }
        std::process::exit(1);
    }
}
