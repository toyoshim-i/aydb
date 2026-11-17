//! Shared temporary directory RAII fixture for unit and integration tests.
//!
//! On Windows a directory cannot be removed while any handle inside it is open, and a test can
//! easily drop its [`TempTestDir`] before the engine, connection or file system that still holds
//! files there (field order, local variable order, `Arc` clones, `std::mem::forget`). The fixture
//! must therefore not rely on declaration order:
//!
//! * Every directory of one test process lives under a per-process root,
//!   `aydb_test_run_<pid>_<nanos>`, so a leftover is one entry per process, not one per test.
//! * Removal retries for a short time, then the directory is parked on a deferred list that is
//!   retried on the next `new` and when the last live directory is dropped.
//! * On Windows the process holds `<root>.lock` open (delete-on-close) for its whole life. The
//!   file disappears when the process dies, however it dies. A root without its lock file is
//!   therefore provably abandoned and is removed by the next test process. A root whose lock file
//!   exists belongs to a live process (possibly running in parallel) and is never touched. The
//!   process id is only part of the name, never used to decide liveness, so id reuse is harmless.
//! * Directories of the legacy flat layout (`aydb_test_<prefix>_...`) are not touched.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

static TEST_DIR_COUNTER: AtomicUsize = AtomicUsize::new(0);

const ROOT_PREFIX: &str = "aydb_test_run_";
const UNLOCKED_ROOT_PREFIX: &str = "aydb_test_unlocked_";
const REMOVE_ATTEMPTS: u32 = 20;
const REMOVE_BACKOFF: Duration = Duration::from_millis(25);

struct Shared {
    root: PathBuf,
    live: usize,
    deferred: Vec<PathBuf>,
    /// Keeps the delete-on-close lock file open until the process exits.
    _lock: Option<std::fs::File>,
}

fn shared() -> &'static Mutex<Shared> {
    static SHARED: OnceLock<Mutex<Shared>> = OnceLock::new();
    SHARED.get_or_init(|| {
        let temp = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let name = format!("{ROOT_PREFIX}{}_{nanos}", std::process::id());
        let lock = create_lock(&temp.join(format!("{name}.lock")));
        // Without its lock file a live root would look abandoned to a parallel test process, so
        // it takes a name the sweep never matches (and is left behind if this process crashes).
        let root = if cfg!(windows) && lock.is_none() {
            temp.join(format!(
                "{UNLOCKED_ROOT_PREFIX}{}_{nanos}",
                std::process::id()
            ))
        } else {
            temp.join(&name)
        };
        sweep_abandoned_roots(&temp);
        Mutex::new(Shared {
            root,
            live: 0,
            deferred: Vec::new(),
            _lock: lock,
        })
    })
}

#[cfg(windows)]
fn create_lock(path: &Path) -> Option<std::fs::File> {
    use std::os::windows::fs::OpenOptionsExt;
    const SHARE_ALL: u32 = 0x7; // READ | WRITE | DELETE
    const FILE_FLAG_DELETE_ON_CLOSE: u32 = 0x0400_0000;
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .share_mode(SHARE_ALL)
        .custom_flags(FILE_FLAG_DELETE_ON_CLOSE)
        .open(path)
        .ok()
}

#[cfg(not(windows))]
fn create_lock(_path: &Path) -> Option<std::fs::File> {
    None
}

/// Removes roots whose owning process is gone. Windows only: elsewhere open files do not block
/// removal, so a root normally empties itself and there is no liveness proof to rely on.
#[cfg(windows)]
fn sweep_abandoned_roots(temp: &Path) {
    let Ok(entries) = std::fs::read_dir(temp) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if !name.starts_with(ROOT_PREFIX) || name.ends_with(".lock") {
            continue;
        }
        let lock = temp.join(format!("{name}.lock"));
        // Only a definite "not found" proves the owner is gone; any other answer (including
        // access denied on a delete-pending lock) means treat it as alive.
        if matches!(std::fs::metadata(&lock), Err(e) if e.kind() == std::io::ErrorKind::NotFound) {
            let _ = std::fs::remove_dir_all(entry.path());
        }
    }
}

#[cfg(not(windows))]
fn sweep_abandoned_roots(_temp: &Path) {}

fn remove_with_retry(path: &Path) -> bool {
    for attempt in 0..REMOVE_ATTEMPTS {
        match std::fs::remove_dir_all(path) {
            Ok(()) => return true,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return true,
            Err(_) if attempt + 1 < REMOVE_ATTEMPTS => std::thread::sleep(REMOVE_BACKOFF),
            Err(_) => {}
        }
    }
    false
}

/// RAII temporary directory that removes its contents on drop.
pub struct TempTestDir {
    pub path: PathBuf,
}

impl TempTestDir {
    pub fn new(prefix: &str) -> Self {
        let seq = TEST_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
        let (root, retry) = {
            let mut s = shared().lock().unwrap_or_else(|e| e.into_inner());
            s.live += 1;
            (s.root.clone(), std::mem::take(&mut s.deferred))
        };
        let mut still = Vec::new();
        for p in retry {
            if !remove_with_retry_once(&p) {
                still.push(p);
            }
        }
        if !still.is_empty() {
            shared()
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .deferred
                .extend(still);
        }
        let path = root.join(format!("{prefix}_{seq}"));
        std::fs::create_dir_all(&path).expect("failed to create temporary test directory");
        Self { path }
    }
}

fn remove_with_retry_once(path: &Path) -> bool {
    match std::fs::remove_dir_all(path) {
        Ok(()) => true,
        Err(e) => e.kind() == std::io::ErrorKind::NotFound,
    }
}

impl Drop for TempTestDir {
    fn drop(&mut self) {
        let removed = remove_with_retry(&self.path);
        let mut s = shared().lock().unwrap_or_else(|e| e.into_inner());
        s.live -= 1;
        if !removed {
            s.deferred.push(self.path.clone());
        }
        if s.live == 0 {
            // Last live directory: retry parked ones once, then drop the root if it is empty.
            let parked = std::mem::take(&mut s.deferred);
            s.deferred = parked
                .into_iter()
                .filter(|p| !remove_with_retry_once(p))
                .collect();
            let _ = std::fs::remove_dir(&s.root);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_directory_dropped_before_its_open_file_is_still_removed_later() {
        let dir = TempTestDir::new("test_util_probe");
        let path = dir.path.clone();
        let file = std::fs::File::create(path.join("held")).unwrap();
        // Dropped while a handle is open: on Windows the removal fails and is parked.
        drop(dir);
        drop(file);
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while path.exists() {
            assert!(
                std::time::Instant::now() < deadline,
                "parked directory was never removed"
            );
            drop(TempTestDir::new("test_util_retry"));
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    #[test]
    fn directories_live_under_the_process_root() {
        let dir = TempTestDir::new("test_util_layout");
        let parent = dir.path.parent().unwrap();
        let name = parent.file_name().unwrap().to_str().unwrap();
        assert!(name.starts_with(ROOT_PREFIX) || name.starts_with(UNLOCKED_ROOT_PREFIX));
        assert!(name.contains(&std::process::id().to_string()));
    }
}
