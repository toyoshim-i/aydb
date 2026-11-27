//! Optional background compaction worker for the shared `Arc<RwLock<StorageEngine<FS>>>`.
//!
//! The worker thread runs the 3-phase compaction protocol so that the expensive K-way merge
//! happens with the engine mutex RELEASED:
//! `lock -> prepare_compaction -> unlock -> job.run() -> lock -> install_compaction -> unlock`.
//!
//! One worker serves each engine instance; registrations are refcounted and the last drop
//! tears it down.
//!
//! Lifecycle: created by [`super::connection::SqliteConnection::register_aydb_module`] and owned
//! by the module's aux context. Dropping it signals shutdown, joins the thread, detaches the
//! engine hook and restores inline compaction, so no thread outlives the module registration.

use std::any::Any;
use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::engine::{StdBlobRelocStep, StorageEngine};
use crate::pal::{OsFs, PlatformFs};

const BASE_BACKOFF: Duration = Duration::from_millis(50);
const MAX_BACKOFF: Duration = Duration::from_secs(30);
const MAX_CONSECUTIVE_PANICS: u32 = 3;
const MAX_INSTALL_CONTENTION_RETRIES: usize = 4;

static NEXT_WORKER_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Default)]
struct SignalState {
    signaled: bool,
    shutdown: bool,
}

struct Signal {
    state: Mutex<SignalState>,
    cv: Condvar,
}

impl Signal {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(SignalState::default()),
            cv: Condvar::new(),
        })
    }

    fn notify(&self) {
        if let Ok(mut st) = self.state.lock() {
            st.signaled = true;
        }
        self.cv.notify_one();
    }
}

struct Entry {
    refs: usize,
    worker_id: u64,
    signal: Arc<Signal>,
    handle: Option<JoinHandle<()>>,
    alive: Arc<AtomicBool>,
}

/// One worker per engine instance, keyed by the address of the engine `RwLock` (stable and
/// unique while the worker holds an `Arc` to it). Registrations are refcounted.
static REGISTRY: Mutex<Option<HashMap<usize, Entry>>> = Mutex::new(None);

fn key_of(engine: &Arc<RwLock<StorageEngine<OsFs>>>) -> usize {
    Arc::as_ptr(engine) as usize
}

fn spawn_worker_thread<FS: PlatformFs>(
    engine: Arc<RwLock<StorageEngine<FS>>>,
    signal: Arc<Signal>,
    worker_id: u64,
    alive: Arc<AtomicBool>,
) -> io::Result<JoinHandle<()>> {
    std::thread::Builder::new()
        .name("aydb-compaction".into())
        .spawn(move || {
            let _alive_guard = AliveGuard(alive);
            worker_loop(&engine, &signal, worker_id);
        })
}

struct AliveGuard(Arc<AtomicBool>);

impl Drop for AliveGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

struct SpawnedWorker {
    worker_id: u64,
    signal: Arc<Signal>,
    handle: JoinHandle<()>,
    alive: Arc<AtomicBool>,
}

fn prepare_worker_spawn(engine: &Arc<RwLock<StorageEngine<OsFs>>>) -> Option<SpawnedWorker> {
    let worker_id = NEXT_WORKER_ID.fetch_add(1, Ordering::Relaxed);
    let signal = Signal::new();
    let alive = Arc::new(AtomicBool::new(true));
    let handle = spawn_worker_thread(
        Arc::clone(engine),
        Arc::clone(&signal),
        worker_id,
        Arc::clone(&alive),
    )
    .ok()?;
    Some(SpawnedWorker {
        worker_id,
        signal,
        handle,
        alive,
    })
}

/// A registration of the shared per-engine worker. Dropping the last registration for an
/// engine shuts the worker down.
pub(in crate::vtab) struct CompactionWorker {
    key: usize,
    engine: Arc<RwLock<StorageEngine<OsFs>>>,
}

impl CompactionWorker {
    /// Registers with the engine's worker, spawning it (or restarting a dead one after panics)
    /// without ever holding the global `REGISTRY` mutex across engine locks or thread joins.
    pub(in crate::vtab) fn spawn(engine: Arc<RwLock<StorageEngine<OsFs>>>) -> Option<Self> {
        let key = key_of(&engine);
        enum SpawnAction {
            Reuse,
            Attach {
                worker_id: u64,
                signal: Arc<Signal>,
                old_handle: Option<JoinHandle<()>>,
            },
        }

        let action = {
            let mut reg = REGISTRY.lock().ok()?;
            let map = reg.get_or_insert_with(HashMap::new);
            if let Some(e) = map.get_mut(&key) {
                if e.alive.load(Ordering::Acquire) {
                    e.refs += 1;
                    SpawnAction::Reuse
                } else {
                    let spawned = prepare_worker_spawn(&engine)?;
                    let old_handle = e.handle.replace(spawned.handle);
                    e.refs += 1;
                    e.worker_id = spawned.worker_id;
                    e.signal = Arc::clone(&spawned.signal);
                    e.alive = spawned.alive;
                    SpawnAction::Attach {
                        worker_id: spawned.worker_id,
                        signal: spawned.signal,
                        old_handle,
                    }
                }
            } else {
                let spawned = prepare_worker_spawn(&engine)?;
                map.insert(
                    key,
                    Entry {
                        refs: 1,
                        worker_id: spawned.worker_id,
                        signal: Arc::clone(&spawned.signal),
                        handle: Some(spawned.handle),
                        alive: spawned.alive,
                    },
                );
                SpawnAction::Attach {
                    worker_id: spawned.worker_id,
                    signal: spawned.signal,
                    old_handle: None,
                }
            }
        };

        let worker = Self { key, engine };
        if let SpawnAction::Attach {
            worker_id,
            signal,
            old_handle,
        } = action
        {
            if let Some(h) = old_handle {
                let _ = h.join();
            }
            let Ok(mut eng) = worker.engine.write() else {
                return None;
            };
            let hook_signal = Arc::clone(&signal);
            eng.install_compaction_hook(worker_id, Arc::new(move || hook_signal.notify()));
            drop(eng);
            // A backlog may already exist at registration time.
            signal.notify();
        }
        Some(worker)
    }
}

impl Drop for CompactionWorker {
    fn drop(&mut self) {
        // Decrement under `REGISTRY`; only the last registration takes the entry out, then drops
        // `REGISTRY` BEFORE waiting on thread join or taking `engine.write()`.
        let entry = {
            let Ok(mut reg) = REGISTRY.lock() else {
                return;
            };
            let Some(map) = reg.as_mut() else {
                return;
            };
            let Some(e) = map.get_mut(&self.key) else {
                return;
            };
            e.refs -= 1;
            if e.refs > 0 {
                return;
            }
            map.remove(&self.key)
        };
        let Some(mut entry) = entry else {
            return;
        };
        if let Ok(mut st) = entry.signal.state.lock() {
            st.shutdown = true;
        }
        entry.signal.cv.notify_all();
        if let Some(h) = entry.handle.take() {
            let _ = h.join();
        }
        if let Ok(mut eng) = self.engine.write() {
            eng.clear_compaction_hook(entry.worker_id);
        }
    }
}

/// Test support: `(registrations, worker_thread_alive)` for `engine`, if a worker is registered.
#[cfg(test)]
pub(in crate::vtab) fn worker_stats(
    engine: &Arc<RwLock<StorageEngine<OsFs>>>,
) -> Option<(usize, Arc<AtomicBool>)> {
    let reg = REGISTRY.lock().ok()?;
    let e = reg.as_ref()?.get(&key_of(engine))?;
    Some((e.refs, Arc::clone(&e.alive)))
}

fn is_shutdown(signal: &Signal) -> bool {
    signal.state.lock().map(|s| s.shutdown).unwrap_or(true)
}

fn backoff_for_failure(consecutive_failures: u32) -> Duration {
    let shift = consecutive_failures.saturating_sub(1).min(10);
    BASE_BACKOFF.saturating_mul(1u32 << shift).min(MAX_BACKOFF)
}

fn panic_message(payload: &(dyn Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic payload".to_string()
    }
}

fn wait_for_signal_or_backoff(signal: &Signal, next_retry_at: Option<Instant>) -> bool {
    let Ok(mut st) = signal.state.lock() else {
        return false;
    };
    loop {
        if st.shutdown {
            return false;
        }
        if let Some(deadline) = next_retry_at {
            let now = Instant::now();
            if now >= deadline {
                st.signaled = false;
                return true;
            }
            match signal.cv.wait_timeout(st, deadline - now) {
                Ok((g, _)) => st = g,
                Err(_) => return false,
            }
        } else if st.signaled {
            st.signaled = false;
            return true;
        } else {
            match signal.cv.wait(st) {
                Ok(g) => st = g,
                Err(_) => return false,
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PassOutcome {
    IdleOrDone,
    Deferred,
    Poisoned,
    Error,
}

#[inline]
fn record_failure_outcome<FS: PlatformFs>(eng: &StorageEngine<FS>, err: &io::Error) -> PassOutcome {
    eng.record_maintenance_failure(err);
    if eng.is_poisoned() {
        PassOutcome::Poisoned
    } else {
        PassOutcome::Error
    }
}

fn run_index_compaction_step<FS: PlatformFs>(
    engine: &Arc<RwLock<StorageEngine<FS>>>,
    contention_retries: &mut usize,
) -> Option<PassOutcome> {
    let job = {
        let Ok(mut eng) = engine.write() else {
            return Some(PassOutcome::Poisoned);
        };
        if eng.is_poisoned() {
            return Some(PassOutcome::Poisoned);
        }
        if !eng.compaction_pending() {
            return None;
        }
        match eng.prepare_compaction() {
            Ok(Some(job)) => job,
            Ok(None) => return None,
            Err(e) => return Some(record_failure_outcome(&eng, &e)),
        }
    };

    let out = match job.run() {
        Ok(out) => out,
        Err(e) => {
            if let Ok(eng) = engine.read() {
                eng.record_maintenance_failure(&e);
            }
            return Some(PassOutcome::Error);
        }
    };

    let Ok(mut eng) = engine.write() else {
        return Some(PassOutcome::Poisoned);
    };
    match eng.install_compaction(out) {
        Ok(true) => {}
        Ok(false) => {
            *contention_retries += 1;
            if *contention_retries >= MAX_INSTALL_CONTENTION_RETRIES {
                return Some(PassOutcome::Deferred);
            }
        }
        Err(e) => return Some(record_failure_outcome(&eng, &e)),
    }
    None
}

fn run_blob_relocation_step<FS: PlatformFs>(
    engine: &Arc<RwLock<StorageEngine<FS>>>,
    signal: &Signal,
    blob_reloc_passes: &mut usize,
) -> Option<PassOutcome> {
    // Up to two passes: rolling over an already-fragmented active writer during pass 1
    // can seal one more fragmented segment that a bounded second pass immediately cleans.
    *blob_reloc_passes += 1;
    let (session_opt, tx_active) = {
        let Ok(mut eng) = engine.write() else {
            return Some(PassOutcome::Poisoned);
        };
        if eng.is_poisoned() {
            return Some(PassOutcome::Poisoned);
        }
        match eng.start_std_blob_relocation(0.5) {
            Ok(Some(session)) => (Some(session), false),
            Ok(None) => (None, eng.in_active_transaction()),
            Err(e) => return Some(record_failure_outcome(&eng, &e)),
        }
    };
    let Some(mut session) = session_opt else {
        if tx_active {
            return Some(PassOutcome::Deferred);
        }
        *blob_reloc_passes = 2;
        return None;
    };

    const BG_RELOCATE_SCAN_KEYS: usize = 128;
    const BG_RELOCATE_CHUNK_BYTES: usize = 256 * 1024;
    loop {
        if is_shutdown(signal) {
            return Some(PassOutcome::IdleOrDone);
        }
        let step = {
            let Ok(mut eng) = engine.write() else {
                return Some(PassOutcome::Poisoned);
            };
            if eng.is_poisoned() {
                return Some(PassOutcome::Poisoned);
            }
            match eng.step_std_blob_relocation(
                &mut session,
                BG_RELOCATE_SCAN_KEYS,
                BG_RELOCATE_CHUNK_BYTES,
            ) {
                Ok(step) => step,
                Err(e) => return Some(record_failure_outcome(&eng, &e)),
            }
        };
        match step {
            StdBlobRelocStep::Continue => std::thread::yield_now(),
            StdBlobRelocStep::Deferred => return Some(PassOutcome::Deferred),
            StdBlobRelocStep::Finished { relocated } => {
                if relocated == 0 {
                    *blob_reloc_passes = 2;
                } else if !is_shutdown(signal) {
                    let Ok(mut eng) = engine.write() else {
                        return Some(PassOutcome::Poisoned);
                    };
                    if eng.is_poisoned() {
                        return Some(PassOutcome::Poisoned);
                    }
                    let was_in_tx = eng.in_active_transaction();
                    if let Err(e) = eng.finish_std_blob_relocation(relocated) {
                        return Some(record_failure_outcome(&eng, &e));
                    }
                    if was_in_tx {
                        return Some(PassOutcome::Deferred);
                    }
                }
                return None;
            }
        }
    }
}

fn run_dead_blob_reclaim_step<FS: PlatformFs>(
    engine: &Arc<RwLock<StorageEngine<FS>>>,
) -> Option<PassOutcome> {
    let Ok(mut eng) = engine.write() else {
        return Some(PassOutcome::Poisoned);
    };
    if eng.is_poisoned() {
        return Some(PassOutcome::Poisoned);
    }
    if eng.in_active_transaction() {
        return Some(PassOutcome::Deferred);
    }
    if let Err(e) = eng.finish_std_blob_relocation(1) {
        return Some(record_failure_outcome(&eng, &e));
    }
    if eng.has_fully_dead_std_blobs() {
        return Some(PassOutcome::Deferred);
    }
    None
}

fn run_drain_cycle<FS: PlatformFs>(
    engine: &Arc<RwLock<StorageEngine<FS>>>,
    signal: &Signal,
) -> PassOutcome {
    let mut contention_retries = 0usize;
    let mut blob_reloc_passes = 0usize;

    loop {
        if is_shutdown(signal) {
            return PassOutcome::IdleOrDone;
        }

        let (poisoned, need_idx, need_blob, need_dead_std) = {
            let Ok(eng) = engine.read() else {
                return PassOutcome::Poisoned;
            };
            (
                eng.is_poisoned(),
                eng.compaction_pending(),
                blob_reloc_passes < 2 && eng.has_fragmented_std_blobs(0.5),
                eng.has_fully_dead_std_blobs(),
            )
        };
        if poisoned {
            return PassOutcome::Poisoned;
        }
        if !need_idx && !need_blob && !need_dead_std {
            return PassOutcome::IdleOrDone;
        }

        let outcome = if need_idx {
            run_index_compaction_step(engine, &mut contention_retries)
        } else if need_blob {
            run_blob_relocation_step(engine, signal, &mut blob_reloc_passes)
        } else {
            run_dead_blob_reclaim_step(engine)
        };
        if let Some(outcome) = outcome {
            return outcome;
        }
    }
}

fn worker_loop<FS: PlatformFs>(
    engine: &Arc<RwLock<StorageEngine<FS>>>,
    signal: &Signal,
    worker_id: u64,
) {
    let mut consecutive_failures: u32 = 0;
    let mut consecutive_panics: u32 = 0;
    let mut next_retry_at: Option<Instant> = None;

    while wait_for_signal_or_backoff(signal, next_retry_at) {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run_drain_cycle(engine, signal)
        })) {
            Ok(PassOutcome::IdleOrDone) => {
                consecutive_failures = 0;
                consecutive_panics = 0;
                next_retry_at = None;
            }
            Ok(PassOutcome::Deferred) => {
                consecutive_failures = 0;
                consecutive_panics = 0;
                next_retry_at = Some(Instant::now() + BASE_BACKOFF);
            }
            Ok(PassOutcome::Poisoned) => {
                next_retry_at = None;
            }
            Ok(PassOutcome::Error) => {
                consecutive_failures = consecutive_failures.saturating_add(1);
                next_retry_at = Some(Instant::now() + backoff_for_failure(consecutive_failures));
            }
            Err(payload) => {
                let msg = panic_message(&*payload);
                if let Ok(eng) = engine.read() {
                    eng.record_maintenance_failure(&io::Error::other(format!(
                        "compaction worker panic: {msg}"
                    )));
                }
                consecutive_panics = consecutive_panics.saturating_add(1);
                if consecutive_panics >= MAX_CONSECUTIVE_PANICS {
                    if let Ok(mut eng) = engine.write() {
                        eng.clear_compaction_hook(worker_id);
                    }
                    return;
                }
                consecutive_failures = consecutive_failures.saturating_add(1);
                next_retry_at = Some(Instant::now() + backoff_for_failure(consecutive_failures));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{EngineConfig, WriteDurability};
    use crate::pal::{OpenHint, PlatformFile};
    use crate::util::test_util::TempTestDir;
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn compaction_worker_backoff_scales_exponentially_and_caps() {
        assert_eq!(backoff_for_failure(1), Duration::from_millis(50));
        assert_eq!(backoff_for_failure(2), Duration::from_millis(100));
        assert_eq!(backoff_for_failure(3), Duration::from_millis(200));
        assert_eq!(backoff_for_failure(10), Duration::from_millis(25_600));
        assert_eq!(backoff_for_failure(11), Duration::from_secs(30));
        assert_eq!(backoff_for_failure(100), Duration::from_secs(30));
    }

    #[test]
    fn compaction_worker_idle_does_not_acquire_write_lock_during_reader_hold() {
        let tmp = TempTestDir::new("worker_idle_no_poll");
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
        let engine = Arc::new(RwLock::new(
            StorageEngine::open(fs, EngineConfig::default()).unwrap(),
        ));

        let worker = CompactionWorker::spawn(Arc::clone(&engine)).expect("spawn worker");
        std::thread::sleep(Duration::from_millis(30));

        let _r1 = engine.read().unwrap();
        std::thread::sleep(Duration::from_millis(350));
        assert!(
            engine.try_read().is_ok(),
            "idle worker must not block readers by polling write lock"
        );
        drop(_r1);

        drop(worker);
        assert!(worker_stats(&engine).is_none());
        assert!(!engine.read().unwrap().background_compaction_enabled());
    }

    use crate::pal::test_fs::{FsHooks, HookedFs};

    struct FaultCompactionHooks {
        fail_idx_create: AtomicBool,
        panic_idx_read: Arc<AtomicBool>,
        idx_create_attempts: AtomicUsize,
    }

    impl FsHooks for FaultCompactionHooks {
        fn before_open_file(
            &self,
            rel_name: &str,
            create: bool,
            _writable: bool,
            _hint: OpenHint,
        ) -> io::Result<()> {
            if rel_name.starts_with("idx_") && create {
                self.idx_create_attempts.fetch_add(1, Ordering::Relaxed);
                if self.fail_idx_create.load(Ordering::Acquire) {
                    return Err(io::Error::other("injected compaction idx creation failure"));
                }
            }
            Ok(())
        }

        fn read_exact_at(
            &self,
            name: &str,
            inner: &dyn PlatformFile,
            buf: &mut [u8],
            offset: u64,
        ) -> io::Result<()> {
            if name.starts_with("idx_")
                && self.panic_idx_read.load(Ordering::Acquire)
                && offset == 0
            {
                panic!("injected idx read panic during job.run");
            }
            inner.read_exact_at(buf, offset)
        }
    }

    type FaultCompactionFs = HookedFs<OsFs, FaultCompactionHooks>;

    fn setup_pending_idx_engine(
        dir_prefix: &str,
    ) -> (
        TempTestDir,
        Arc<FaultCompactionFs>,
        Arc<RwLock<StorageEngine<FaultCompactionFs>>>,
    ) {
        let tmp = TempTestDir::new(dir_prefix);
        let fs = Arc::new(HookedFs::new(
            OsFs::open(&tmp.path, true).unwrap(),
            FaultCompactionHooks {
                fail_idx_create: AtomicBool::new(false),
                panic_idx_read: Arc::new(AtomicBool::new(false)),
                idx_create_attempts: AtomicUsize::new(0),
            },
        ));
        let mut cfg = EngineConfig::default();
        cfg.memtable_capacity_bytes = 512;
        cfg.auto_compact_segment_threshold = 2;
        cfg.max_segments_before_inline_compaction = 10_000;
        cfg.background_compaction = true;
        let mut engine = StorageEngine::open(Arc::clone(&fs), cfg).unwrap();
        for round in 0..4u32 {
            for i in 0..10u32 {
                let k = format!("k_{round:02}_{i:02}").into_bytes();
                engine
                    .put_row(1, &k, b"val_padding", &[], WriteDurability::BufferedFast)
                    .unwrap();
            }
            engine.flush_memtable().unwrap();
        }
        assert!(engine.compaction_pending());
        (tmp, fs, Arc::new(RwLock::new(engine)))
    }

    #[test]
    fn compaction_worker_records_maintenance_failure_and_backs_off_on_error() {
        let (_tmp, fs, engine) = setup_pending_idx_engine("worker_err_backoff");
        fs.hooks().idx_create_attempts.store(0, Ordering::Relaxed);
        fs.hooks().fail_idx_create.store(true, Ordering::Release);

        let signal = Signal::new();
        let hook_sig = Arc::clone(&signal);
        let worker_id = 77;
        engine
            .write()
            .unwrap()
            .install_compaction_hook(worker_id, Arc::new(move || hook_sig.notify()));

        let te = Arc::clone(&engine);
        let ts = Arc::clone(&signal);
        let handle = std::thread::spawn(move || worker_loop(&te, &ts, worker_id));
        signal.notify();

        let deadline = Instant::now() + Duration::from_secs(5);
        while engine.read().unwrap().maintenance_failures() < 2 {
            assert!(
                Instant::now() < deadline,
                "expected maintenance failures to increment"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        let failures_snap = engine.read().unwrap().maintenance_failures();
        let err_msg = engine.read().unwrap().last_maintenance_error().unwrap();
        assert!(err_msg.contains("injected compaction idx creation failure"));
        // Backoff must prevent spinning thousands of failures during 120ms (no spin storm).
        std::thread::sleep(Duration::from_millis(120));
        let failures_later = engine.read().unwrap().maintenance_failures();
        assert!(
            failures_later - failures_snap <= 3,
            "expected exponential backoff to cap retry rate (delta={})",
            failures_later - failures_snap
        );

        fs.hooks().fail_idx_create.store(false, Ordering::Release);
        signal.notify();
        let finish_deadline = Instant::now() + Duration::from_secs(5);
        while engine.read().unwrap().compaction_pending() {
            assert!(
                Instant::now() < finish_deadline,
                "worker should finish compaction after fault clears"
            );
            std::thread::sleep(Duration::from_millis(15));
        }

        signal.state.lock().unwrap().shutdown = true;
        signal.cv.notify_all();
        handle.join().unwrap();
    }

    #[test]
    fn compaction_worker_recovers_from_panic_and_falls_back_to_inline() {
        let (_tmp, fs, engine) = setup_pending_idx_engine("worker_panic_fallback");
        let signal = Signal::new();
        let hook_sig = Arc::clone(&signal);
        let worker_id = 88;
        engine
            .write()
            .unwrap()
            .install_compaction_hook(worker_id, Arc::new(move || hook_sig.notify()));
        assert!(engine.read().unwrap().compaction_pending());

        fs.hooks().panic_idx_read.store(true, Ordering::Release);
        let alive = Arc::new(AtomicBool::new(true));
        let handle = spawn_worker_thread(
            Arc::clone(&engine),
            Arc::clone(&signal),
            worker_id,
            Arc::clone(&alive),
        )
        .unwrap();
        signal.notify();

        let deadline = Instant::now() + Duration::from_secs(5);
        while alive.load(Ordering::Acquire) {
            assert!(
                Instant::now() < deadline,
                "worker should exit and restore inline mode after {MAX_CONSECUTIVE_PANICS} panics"
            );
            std::thread::sleep(Duration::from_millis(15));
        }
        handle.join().unwrap();

        let eng = engine.read().unwrap();
        assert!(!eng.background_compaction_enabled());
        assert!(eng.maintenance_failures() >= MAX_CONSECUTIVE_PANICS as u64);
        assert!(
            eng.last_maintenance_error()
                .is_some_and(|m| m.contains("compaction worker panic"))
        );
    }

    #[test]
    fn compaction_worker_chunked_blob_relocation_completes_and_shuts_down_promptly() {
        let tmp = TempTestDir::new("worker_chunked_blob_reloc");
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
        let mut cfg = EngineConfig::default();
        cfg.memtable_capacity_bytes = 2048;
        cfg.inline_threshold_bytes = 128;
        cfg.max_blob_segment_bytes = 64 * 1024;
        cfg.auto_compact_segment_threshold = 64;
        cfg.max_segments_before_inline_compaction = 10_000;
        cfg.background_compaction = true;
        let engine = Arc::new(RwLock::new(StorageEngine::open(fs, cfg).unwrap()));

        let payload = vec![0x5Au8; 16 * 1024];
        {
            let mut eng = engine.write().unwrap();
            for i in 0..48u32 {
                let k = format!("wk_{i:04}").into_bytes();
                eng.put_row(1, &k, &payload, &[], WriteDurability::BufferedFast)
                    .unwrap();
            }
            eng.compact().unwrap();
            for i in 0..48u32 {
                if i % 3 < 2 {
                    let k = format!("wk_{i:04}").into_bytes();
                    eng.delete_row(1, &k, WriteDurability::BufferedFast)
                        .unwrap();
                }
            }
            eng.flush_memtable().unwrap();
            assert!(eng.has_fragmented_std_blobs(0.5));
        }

        let worker = CompactionWorker::spawn(Arc::clone(&engine)).expect("spawn worker");
        let deadline = Instant::now() + Duration::from_secs(10);
        while engine.read().unwrap().has_fragmented_std_blobs(0.5) {
            assert!(
                Instant::now() < deadline,
                "worker should finish chunked std-blob relocation"
            );
            std::thread::sleep(Duration::from_millis(15));
        }
        drop(worker);
        assert!(worker_stats(&engine).is_none());
        let mut eng = engine.write().unwrap();
        assert!(!eng.is_relocating_std_blobs());
        for i in 0..48u32 {
            let k = format!("wk_{i:04}").into_bytes();
            let expected = if i % 3 < 2 {
                None
            } else {
                Some(payload.clone())
            };
            assert_eq!(eng.get_row(1, &k).unwrap(), expected);
        }
        drop(eng);

        // Also exercise dropping the worker immediately after triggering a multi-chunk relocation.
        {
            let mut eng = engine.write().unwrap();
            for i in 0..96u32 {
                let k = format!("sh_{i:04}").into_bytes();
                eng.put_row(1, &k, &payload, &[], WriteDurability::BufferedFast)
                    .unwrap();
            }
            eng.compact().unwrap();
            for i in 0..96u32 {
                if i % 3 < 2 {
                    let k = format!("sh_{i:04}").into_bytes();
                    eng.delete_row(1, &k, WriteDurability::BufferedFast)
                        .unwrap();
                }
            }
            eng.flush_memtable().unwrap();
            assert!(eng.has_fragmented_std_blobs(0.5));
        }
        let worker2 = CompactionWorker::spawn(Arc::clone(&engine)).expect("spawn worker2");
        std::thread::sleep(Duration::from_millis(1));
        let drop_start = Instant::now();
        drop(worker2);
        assert!(
            drop_start.elapsed() < Duration::from_secs(5),
            "worker shutdown during chunked relocation took {:?}",
            drop_start.elapsed()
        );
        assert!(!engine.read().unwrap().is_relocating_std_blobs());
    }

    #[test]
    fn compaction_worker_resumes_deferred_relocation_after_tx_commits() {
        // R2-M9a: when an open transaction defers std-blob relocation, the worker must retry and
        // finish once the transaction ends rather than parking permanently in IdleOrDone.
        let tmp = TempTestDir::new("r2_m9a_worker_deferred_resume");
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
        let mut cfg = EngineConfig::default();
        cfg.memtable_capacity_bytes = 2048;
        cfg.inline_threshold_bytes = 128;
        cfg.max_blob_segment_bytes = 64 * 1024;
        cfg.auto_compact_segment_threshold = 64;
        cfg.max_segments_before_inline_compaction = 10_000;
        cfg.background_compaction = true;
        let engine = Arc::new(RwLock::new(StorageEngine::open(fs, cfg).unwrap()));

        let payload = vec![0x5Au8; 16 * 1024];
        {
            let mut eng = engine.write().unwrap();
            for i in 0..24u32 {
                let k = format!("dk_{i:04}").into_bytes();
                eng.put_row(1, &k, &payload, &[], WriteDurability::BufferedFast)
                    .unwrap();
            }
            eng.compact().unwrap();
            for i in 0..24u32 {
                if i % 3 < 2 {
                    let k = format!("dk_{i:04}").into_bytes();
                    eng.delete_row(1, &k, WriteDurability::BufferedFast)
                        .unwrap();
                }
            }
            eng.flush_memtable().unwrap();
            assert!(eng.has_fragmented_std_blobs(0.5));
            // Hold a transaction open BEFORE spawning the worker so the initial drain is deferred.
            eng.begin_transaction_for_conn(42).unwrap();
        }

        let worker = CompactionWorker::spawn(Arc::clone(&engine)).expect("spawn worker");
        std::thread::sleep(Duration::from_millis(120));
        assert!(
            engine.read().unwrap().has_fragmented_std_blobs(0.5),
            "relocation must stay deferred while tx is active"
        );

        // Commit an empty tx (emits NO compaction hook notification) and confirm the worker wakes
        // from Deferred backoff and finishes the relocation on its own.
        engine
            .write()
            .unwrap()
            .commit_transaction_for_conn(42)
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while engine.read().unwrap().has_fragmented_std_blobs(0.5) {
            assert!(
                Instant::now() < deadline,
                "worker must resume deferred relocation once tx commits"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        drop(worker);
    }

    #[test]
    fn compaction_worker_reclaims_fully_dead_std_blobs_after_rollback() {
        // R3-M9: a 100%-dead sealed `blob_std` segment must still be reclaimed once an active
        // transaction rolls back, and `rollback_transaction` must wake the hook when deferred
        // maintenance was queued.
        let tmp = TempTestDir::new("r3_m9_dead_std_after_rollback");
        let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
        let mut cfg = EngineConfig::default();
        cfg.memtable_capacity_bytes = 2048;
        cfg.inline_threshold_bytes = 64;
        cfg.max_blob_segment_bytes = 256;
        cfg.auto_compact_segment_threshold = 64;
        cfg.max_segments_before_inline_compaction = 10_000;
        cfg.background_compaction = true;
        let engine = Arc::new(RwLock::new(
            StorageEngine::open(Arc::clone(&fs), cfg).unwrap(),
        ));

        {
            let mut eng = engine.write().unwrap();
            eng.put_row(1, b"k3", &[0x33u8; 200], &[], WriteDurability::BufferedFast)
                .unwrap();
            eng.put_row(1, b"k4", &[0x44u8; 200], &[], WriteDurability::BufferedFast)
                .unwrap();
            eng.put_row(1, b"k3", &[0x55u8; 200], &[], WriteDurability::BufferedFast)
                .unwrap();
            eng.flush_memtable().unwrap();
            assert!(!eng.has_fully_dead_std_blobs());

            eng.put_row(1, b"k4", b"tiny", &[], WriteDurability::BufferedFast)
                .unwrap();
            assert!(eng.has_fully_dead_std_blobs());
            eng.begin_transaction_for_conn(7).unwrap();
            eng.finish_std_blob_relocation(1).unwrap();
        }
        let worker = CompactionWorker::spawn(Arc::clone(&engine)).expect("spawn worker");
        std::thread::sleep(Duration::from_millis(80));
        assert!(engine.read().unwrap().has_fully_dead_std_blobs());

        engine
            .write()
            .unwrap()
            .rollback_transaction_for_conn(7)
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while engine.read().unwrap().has_fully_dead_std_blobs() {
            assert!(
                Instant::now() < deadline,
                "worker must reclaim 100%-dead std blob after intervening rollback"
            );
            std::thread::sleep(Duration::from_millis(15));
        }
        drop(worker);
    }

    #[test]
    fn compaction_worker_returns_deferred_on_install_contention_exhaustion() {
        // R3-L2: MAX_INSTALL_CONTENTION_RETRIES exhaustion returns PassOutcome::Deferred, not IdleOrDone.
        type ContentionFs = HookedFs<OsFs, ContentionHooks>;
        type SharedContentionEngine = Arc<Mutex<Option<Arc<RwLock<StorageEngine<ContentionFs>>>>>>;
        struct ContentionHooks {
            engine: SharedContentionEngine,
            races_left: Arc<AtomicUsize>,
        }
        impl ContentionHooks {
            fn maybe_race(&self, name: &str) {
                if !name.starts_with("idx_") || self.races_left.load(Ordering::Acquire) == 0 {
                    return;
                }
                let eng_arc = self.engine.lock().unwrap().clone();
                if let Some(eng_arc) = eng_arc {
                    if let Ok(mut eng) = eng_arc.try_write() {
                        if let Ok(prev) =
                            self.races_left
                                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                                    n.checked_sub(1)
                                })
                        {
                            let _ = eng.compact();
                            for r in 0..3u32 {
                                let _ = eng.put_row(
                                    1,
                                    format!("race_{r}_{prev}").as_bytes(),
                                    &[0x66u8; 300],
                                    &[],
                                    WriteDurability::BufferedFast,
                                );
                                let _ = eng.flush_memtable();
                            }
                        }
                    }
                }
            }
        }
        impl FsHooks for ContentionHooks {
            fn read_at(
                &self,
                name: &str,
                inner: &dyn PlatformFile,
                buf: &mut [u8],
                offset: u64,
            ) -> io::Result<usize> {
                self.maybe_race(name);
                inner.read_at(buf, offset)
            }
            fn read_exact_at(
                &self,
                name: &str,
                inner: &dyn PlatformFile,
                buf: &mut [u8],
                offset: u64,
            ) -> io::Result<()> {
                self.maybe_race(name);
                inner.read_exact_at(buf, offset)
            }
            fn read_many_at(
                &self,
                name: &str,
                inner: &dyn PlatformFile,
                requests: &mut [(u64, &mut [u8])],
            ) -> io::Result<()> {
                self.maybe_race(name);
                inner.read_many_at(requests)
            }
        }

        let tmp2 = TempTestDir::new("r3_l2_contention_deferred");
        let shared_engine = Arc::new(Mutex::new(None));
        let races_left = Arc::new(AtomicUsize::new(0));
        let cfs = Arc::new(HookedFs::new(
            OsFs::open(&tmp2.path, true).unwrap(),
            ContentionHooks {
                engine: Arc::clone(&shared_engine),
                races_left: Arc::clone(&races_left),
            },
        ));
        let mut cfg2 = EngineConfig::default();
        cfg2.memtable_capacity_bytes = 512;
        cfg2.auto_compact_segment_threshold = 2;
        cfg2.max_segments_before_inline_compaction = 10_000;
        cfg2.background_compaction = true;
        let ceng = Arc::new(RwLock::new(
            StorageEngine::open(Arc::clone(&cfs), cfg2).unwrap(),
        ));
        *shared_engine.lock().unwrap() = Some(Arc::clone(&ceng));
        {
            let mut eng = ceng.write().unwrap();
            for r in 0..3u32 {
                eng.put_row(
                    1,
                    format!("init_{r}").as_bytes(),
                    &[0x77u8; 300],
                    &[],
                    WriteDurability::BufferedFast,
                )
                .unwrap();
                eng.flush_memtable().unwrap();
            }
            assert!(eng.compaction_pending());
        }
        races_left.store(MAX_INSTALL_CONTENTION_RETRIES * 8, Ordering::Release);
        let sig = Signal::new();
        let outcome = run_drain_cycle(&ceng, &sig);
        races_left.store(0, Ordering::Release);
        *shared_engine.lock().unwrap() = None;
        assert!(
            matches!(outcome, PassOutcome::Deferred),
            "expected PassOutcome::Deferred on contention exhaustion, got {outcome:?}"
        );
    }
}
