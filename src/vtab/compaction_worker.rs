//! Optional background compaction worker for the shared `Arc<Mutex<StorageEngine<OsFs>>>`.
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

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use crate::engine::StorageEngine;
use crate::pal::OsFs;

/// Safety-net wake-up interval in case a signal is missed.
const POLL_INTERVAL: Duration = Duration::from_millis(250);

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
    fn notify(&self) {
        if let Ok(mut st) = self.state.lock() {
            st.signaled = true;
        }
        self.cv.notify_one();
    }
}

struct Entry {
    refs: usize,
    signal: Arc<Signal>,
    handle: Option<JoinHandle<()>>,
    #[cfg_attr(not(test), allow(dead_code))]
    alive: Arc<AtomicBool>,
}

/// One worker per engine instance, keyed by the address of the engine `Mutex` (stable and
/// unique while the worker holds an `Arc` to it). Registrations are refcounted.
static REGISTRY: Mutex<Option<HashMap<usize, Entry>>> = Mutex::new(None);

fn key_of(engine: &Arc<Mutex<StorageEngine<OsFs>>>) -> usize {
    Arc::as_ptr(engine) as usize
}

/// A registration of the shared per-engine worker. Dropping the last registration for an
/// engine shuts the worker down.
pub(crate) struct CompactionWorker {
    key: usize,
    engine: Arc<Mutex<StorageEngine<OsFs>>>,
}

impl CompactionWorker {
    /// Registers with the engine's worker, spawning it (and switching the engine into
    /// background-compaction mode) if this is the first registration.
    pub(crate) fn spawn(engine: Arc<Mutex<StorageEngine<OsFs>>>) -> Option<Self> {
        let key = key_of(&engine);
        let mut reg = REGISTRY.lock().ok()?;
        let map = reg.get_or_insert_with(HashMap::new);
        if let Some(e) = map.get_mut(&key) {
            e.refs += 1;
            return Some(Self { key, engine });
        }
        let signal = Arc::new(Signal {
            state: Mutex::new(SignalState::default()),
            cv: Condvar::new(),
        });
        {
            let mut eng = engine.lock().ok()?;
            let hook_signal = Arc::clone(&signal);
            eng.set_compaction_hook(Some(Arc::new(move || hook_signal.notify())));
            eng.set_background_compaction(true);
        }
        let alive = Arc::new(AtomicBool::new(true));
        let (te, ts, ta) = (Arc::clone(&engine), Arc::clone(&signal), Arc::clone(&alive));
        let handle = std::thread::Builder::new()
            .name("aydb-compaction".into())
            .spawn(move || {
                worker_loop(te, ts);
                ta.store(false, Ordering::SeqCst);
            })
            .ok();
        if handle.is_none() {
            if let Ok(mut eng) = engine.lock() {
                eng.set_compaction_hook(None);
                eng.set_background_compaction(false);
            }
            return None;
        }
        // A backlog may already exist at registration time.
        signal.notify();
        map.insert(
            key,
            Entry {
                refs: 1,
                signal,
                handle,
                alive,
            },
        );
        Some(Self { key, engine })
    }
}

impl Drop for CompactionWorker {
    fn drop(&mut self) {
        // Decrement; only the last registration takes the entry out for teardown.
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
        // Join WITHOUT the registry lock (a concurrent re-register may start a fresh worker).
        if let Ok(mut st) = entry.signal.state.lock() {
            st.shutdown = true;
        }
        entry.signal.cv.notify_all();
        if let Some(h) = entry.handle.take() {
            let _ = h.join();
        }
        // Restore inline compaction unless a racing re-register already installed a new worker
        // (checked under the registry lock so the decision cannot go stale).
        if let Ok(reg) = REGISTRY.lock() {
            let replaced = reg.as_ref().is_some_and(|m| m.contains_key(&self.key));
            if !replaced {
                if let Ok(mut eng) = self.engine.lock() {
                    eng.set_compaction_hook(None);
                    eng.set_background_compaction(false);
                }
            }
        }
    }
}

/// Test support: `(registrations, worker_thread_alive)` for `engine`, if a worker is registered.
#[cfg(test)]
pub(crate) fn worker_stats(
    engine: &Arc<Mutex<StorageEngine<OsFs>>>,
) -> Option<(usize, Arc<AtomicBool>)> {
    let reg = REGISTRY.lock().ok()?;
    let e = reg.as_ref()?.get(&key_of(engine))?;
    Some((e.refs, Arc::clone(&e.alive)))
}

fn is_shutdown(signal: &Signal) -> bool {
    signal.state.lock().map(|s| s.shutdown).unwrap_or(true)
}

fn worker_loop(engine: Arc<Mutex<StorageEngine<OsFs>>>, signal: Arc<Signal>) {
    loop {
        {
            let Ok(mut st) = signal.state.lock() else {
                return;
            };
            while !st.signaled && !st.shutdown {
                match signal.cv.wait_timeout(st, POLL_INTERVAL) {
                    Ok((g, res)) => {
                        st = g;
                        if res.timed_out() {
                            break;
                        }
                    }
                    Err(_) => return,
                }
            }
            if st.shutdown {
                return;
            }
            st.signaled = false;
        }

        // Drain: keep compacting while the engine still reports pending work.
        loop {
            if is_shutdown(&signal) {
                return;
            }
            // Phase 1: prepare under the lock (cheap).
            let job = {
                let Ok(mut eng) = engine.lock() else {
                    return;
                };
                if !eng.compaction_pending() {
                    break;
                }
                match eng.prepare_compaction() {
                    Ok(Some(job)) => job,
                    Ok(None) | Err(_) => break,
                }
            };
            // Phase 2: merge without the engine lock.
            let Ok(out) = job.run() else {
                break;
            };
            // Phase 3: install under the lock (cheap).
            let Ok(mut eng) = engine.lock() else {
                return;
            };
            match eng.install_compaction(out) {
                Ok(true) => {}
                Ok(false) | Err(_) => break,
            }
        }
    }
}
