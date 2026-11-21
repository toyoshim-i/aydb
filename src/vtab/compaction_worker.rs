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

use crate::engine::StorageEngine;
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

/// A registration of the shared per-engine worker. Dropping the last registration for an
/// engine shuts the worker down.
pub(crate) struct CompactionWorker {
    key: usize,
    engine: Arc<RwLock<StorageEngine<OsFs>>>,
}

impl CompactionWorker {
    /// Registers with the engine's worker, spawning it (or restarting a dead one after panics)
    /// without ever holding the global `REGISTRY` mutex across engine locks or thread joins.
    pub(crate) fn spawn(engine: Arc<RwLock<StorageEngine<OsFs>>>) -> Option<Self> {
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
                    let worker_id = NEXT_WORKER_ID.fetch_add(1, Ordering::Relaxed);
                    let signal = Signal::new();
                    let alive = Arc::new(AtomicBool::new(true));
                    let handle = spawn_worker_thread(
                        Arc::clone(&engine),
                        Arc::clone(&signal),
                        worker_id,
                        Arc::clone(&alive),
                    )
                    .ok()?;
                    let old_handle = e.handle.replace(handle);
                    e.refs += 1;
                    e.worker_id = worker_id;
                    e.signal = Arc::clone(&signal);
                    e.alive = alive;
                    SpawnAction::Attach {
                        worker_id,
                        signal,
                        old_handle,
                    }
                }
            } else {
                let worker_id = NEXT_WORKER_ID.fetch_add(1, Ordering::Relaxed);
                let signal = Signal::new();
                let alive = Arc::new(AtomicBool::new(true));
                let handle = spawn_worker_thread(
                    Arc::clone(&engine),
                    Arc::clone(&signal),
                    worker_id,
                    Arc::clone(&alive),
                )
                .ok()?;
                map.insert(
                    key,
                    Entry {
                        refs: 1,
                        worker_id,
                        signal: Arc::clone(&signal),
                        handle: Some(handle),
                        alive,
                    },
                );
                SpawnAction::Attach {
                    worker_id,
                    signal,
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
pub(crate) fn worker_stats(
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
    Poisoned,
    Error,
}

fn run_drain_cycle<FS: PlatformFs>(
    engine: &Arc<RwLock<StorageEngine<FS>>>,
    signal: &Signal,
) -> PassOutcome {
    let mut contention_retries = 0usize;
    let mut attempted_blob_reloc = false;

    loop {
        if is_shutdown(signal) {
            return PassOutcome::IdleOrDone;
        }

        let (poisoned, need_idx, need_blob) = {
            let Ok(eng) = engine.read() else {
                return PassOutcome::Poisoned;
            };
            (
                eng.is_poisoned(),
                eng.compaction_pending(),
                !attempted_blob_reloc && eng.has_fragmented_std_blobs(0.5),
            )
        };
        if poisoned {
            return PassOutcome::Poisoned;
        }
        if !need_idx && !need_blob {
            return PassOutcome::IdleOrDone;
        }

        if need_idx {
            let job = {
                let Ok(mut eng) = engine.write() else {
                    return PassOutcome::Poisoned;
                };
                if eng.is_poisoned() {
                    return PassOutcome::Poisoned;
                }
                if !eng.compaction_pending() {
                    continue;
                }
                match eng.prepare_compaction() {
                    Ok(Some(job)) => job,
                    Ok(None) => continue,
                    Err(e) => {
                        eng.record_maintenance_failure(&e);
                        return if eng.is_poisoned() {
                            PassOutcome::Poisoned
                        } else {
                            PassOutcome::Error
                        };
                    }
                }
            };

            let out = match job.run() {
                Ok(out) => out,
                Err(e) => {
                    if let Ok(eng) = engine.read() {
                        eng.record_maintenance_failure(&e);
                    }
                    return PassOutcome::Error;
                }
            };

            let Ok(mut eng) = engine.write() else {
                return PassOutcome::Poisoned;
            };
            match eng.install_compaction(out) {
                Ok(true) => {}
                Ok(false) => {
                    contention_retries += 1;
                    if contention_retries >= MAX_INSTALL_CONTENTION_RETRIES {
                        return PassOutcome::IdleOrDone;
                    }
                }
                Err(e) => {
                    eng.record_maintenance_failure(&e);
                    return if eng.is_poisoned() {
                        PassOutcome::Poisoned
                    } else {
                        PassOutcome::Error
                    };
                }
            }
        } else {
            attempted_blob_reloc = true;
            let Ok(mut eng) = engine.write() else {
                return PassOutcome::Poisoned;
            };
            if eng.is_poisoned() {
                return PassOutcome::Poisoned;
            }
            if let Err(e) = eng.background_relocate_fragmented_std_blobs(0.5) {
                eng.record_maintenance_failure(&e);
                return if eng.is_poisoned() {
                    PassOutcome::Poisoned
                } else {
                    PassOutcome::Error
                };
            }
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
    use std::path::Path;
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

    struct FaultCompactionFs {
        inner: Arc<OsFs>,
        fail_idx_create: AtomicBool,
        panic_idx_read: Arc<AtomicBool>,
        idx_create_attempts: AtomicUsize,
    }

    struct FaultCompactionFile {
        inner: Arc<crate::pal::OsFile>,
        is_idx: bool,
        panic_idx_read: Arc<AtomicBool>,
    }

    impl PlatformFile for FaultCompactionFile {
        fn file_len(&self) -> io::Result<u64> {
            self.inner.file_len()
        }
        fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
            self.inner.read_at(buf, offset)
        }
        fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
            if self.is_idx && self.panic_idx_read.load(Ordering::Acquire) && offset == 0 {
                panic!("injected idx read panic during job.run");
            }
            self.inner.read_exact_at(buf, offset)
        }
        fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
            self.inner.write_all_at(buf, offset)
        }
        fn preallocate(
            &self,
            target_len: u64,
            strategy: crate::pal::PreallocStrategy,
        ) -> io::Result<()> {
            self.inner.preallocate(target_len, strategy)
        }
        fn sync_barrier(&self) -> io::Result<()> {
            self.inner.sync_barrier()
        }
        fn sync_durable(&self) -> io::Result<()> {
            self.inner.sync_durable()
        }
        fn evict_os_cache(&self, offset: u64, len: u64) -> io::Result<()> {
            self.inner.evict_os_cache(offset, len)
        }
    }

    impl PlatformFs for FaultCompactionFs {
        type File = FaultCompactionFile;

        fn open_file(
            &self,
            rel_name: &str,
            create: bool,
            writable: bool,
            hint: OpenHint,
        ) -> io::Result<Arc<Self::File>> {
            let is_idx = rel_name.starts_with("idx_");
            if is_idx && create {
                self.idx_create_attempts.fetch_add(1, Ordering::Relaxed);
                if self.fail_idx_create.load(Ordering::Acquire) {
                    return Err(io::Error::other("injected compaction idx creation failure"));
                }
            }
            let inner = self.inner.open_file(rel_name, create, writable, hint)?;
            Ok(Arc::new(FaultCompactionFile {
                inner,
                is_idx,
                panic_idx_read: Arc::clone(&self.panic_idx_read),
            }))
        }

        fn remove_file(&self, rel_name: &str) -> io::Result<()> {
            self.inner.remove_file(rel_name)
        }
        fn list_files(&self) -> io::Result<Vec<String>> {
            self.inner.list_files()
        }
        fn root_path(&self) -> &Path {
            self.inner.root_path()
        }
    }

    fn setup_pending_idx_engine(
        dir_prefix: &str,
    ) -> (
        TempTestDir,
        Arc<FaultCompactionFs>,
        Arc<RwLock<StorageEngine<FaultCompactionFs>>>,
    ) {
        let tmp = TempTestDir::new(dir_prefix);
        let fs = Arc::new(FaultCompactionFs {
            inner: Arc::new(OsFs::open(&tmp.path, true).unwrap()),
            fail_idx_create: AtomicBool::new(false),
            panic_idx_read: Arc::new(AtomicBool::new(false)),
            idx_create_attempts: AtomicUsize::new(0),
        });
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
        fs.idx_create_attempts.store(0, Ordering::Relaxed);
        fs.fail_idx_create.store(true, Ordering::Release);

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

        fs.fail_idx_create.store(false, Ordering::Release);
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

        fs.panic_idx_read.store(true, Ordering::Release);
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
}
