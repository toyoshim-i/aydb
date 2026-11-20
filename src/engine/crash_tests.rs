//! Seeded crash-consistency tests.
//!
//! [`CrashFs`] is a deterministic in-memory filesystem that remembers, per file, which writes
//! reached the device and which are still only in the (simulated) page cache. A simulated crash
//! then decides, from a seed, what survives. [`Harness`] drives a random workload against a
//! [`StorageEngine`] on top of it, crashes at a seed-chosen point, reopens, and compares the
//! recovered state with an oracle built from the guarantees of `docs/ARCHITECTURE.md` section 4.
//!
//! See `docs/DEVELOPMENT.md` ("Crash-consistency tests") for the model, the oracle and the
//! environment variables (`AYDB_CRASH_SEEDS`, `AYDB_CRASH_SEED_START`, `AYDB_CRASH_SEED`).
//!
//! # Why a separate filesystem
//!
//! `PowerLossFs` in `tests.rs` reverts every file to its last sync image and treats sync logs as
//! write-through. That is the right model for its many targeted tests, but it cannot express
//! partial persistence, reordering, vanishing directory entries, non-durable pre-allocation or
//! I/O failures at an arbitrary operation, which are the situations this file exists for.

use super::*;
use crate::pal::{PlatformFile, PlatformFs, PreallocStrategy};
use std::collections::{BTreeMap, BTreeSet};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU64;

mod media;
use media::{Mode, OpRec, Ticket, Touched};

#[derive(Clone)]
pub(super) enum ExpectDamage {
    Exact(Option<WalDamage>),
    FastPresence(bool),
}

// =============================================================================================
// Deterministic random numbers
// =============================================================================================

/// SplitMix64: tiny, fast, and identical on every platform.
#[derive(Clone)]
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xD1B5_4A32_D192_ED03)
    }

    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `0..n` (`n > 0`).
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn chance(&mut self, num: u64, den: u64) -> bool {
        self.below(den) < num
    }
}

// =============================================================================================
// Simulated filesystem
// =============================================================================================

/// Sector size at which a power loss can tear a write.
const SECTOR: usize = 512;

/// Kinds of files, as far as fault injection is concerned.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum FileKind {
    BootMeta = 0,
    WalSync = 1,
    WalBuf = 2,
    Idx = 3,
    Blob = 4,
    Manifest = 5,
}

const KIND_COUNT: usize = 6;
const ALL_KINDS: [FileKind; KIND_COUNT] = [
    FileKind::BootMeta,
    FileKind::WalSync,
    FileKind::WalBuf,
    FileKind::Idx,
    FileKind::Blob,
    FileKind::Manifest,
];

fn classify(name: &str) -> FileKind {
    if name.starts_with("boot_meta") {
        FileKind::BootMeta
    } else if name.starts_with("wal_sync") {
        FileKind::WalSync
    } else if name.starts_with("wal_buf") {
        FileKind::WalBuf
    } else if name.starts_with("idx_") {
        FileKind::Idx
    } else if name.starts_with("manifest_") {
        FileKind::Manifest
    } else {
        // `blob_std_*` and `blob_lru_*`; the engine creates no other files.
        FileKind::Blob
    }
}

/// How directory entries (file creation and removal) become durable.
///
/// * `Unix`: `UnixPlatformFs::open_file` calls `sync_dir` after creating a file (and after every
///   `MutableSyncLog` create) and `remove_file` calls it after unlinking, so an entry is durable
///   when the call returns.
/// * `Windows`: `WinPlatformFs` never flushes the directory. The model assumes that NTFS logs
///   directory operations (creations and removals) in order, and that a successful
///   `FlushFileBuffers` on any file forces the log up to that point, so every directory
///   operation issued before the flush is durable afterwards. Directory operations issued after
///   the last flush are lost as a suffix of the log: after a power loss a random prefix of them
///   took effect. A file created and not yet covered by a flush may therefore be missing, and a
///   removed file may reappear with the content it had at its last sync.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum DirMode {
    Unix,
    Windows,
}

/// What a simulated crash does to data that has not been synced.
#[derive(Clone, Copy, Debug)]
enum Crash {
    /// The process dies but the operating system survives: the page cache and the directory are
    /// intact, so everything written so far persists (`kill -9` / `TerminateProcess`).
    Process,
    /// The machine loses power. Each unsynced write independently persists completely
    /// (`keep` percent), is lost (`lose` percent) or persists only in the 512-byte sectors the
    /// seed picks (the rest).
    Power { keep: u64, lose: u64 },
}

enum Pend {
    /// `dir_seq` is the number of directory operations issued before the write.
    Write {
        off: usize,
        data: Vec<u8>,
        dir_seq: u64,
    },
    /// A length extension without data (`preallocate` with `LogicalExtendOnly`).
    Extend { len: usize, dir_seq: u64 },
}

struct Inode {
    name: String,
    /// What readers see: the page cache.
    cache: Vec<u8>,
    /// What is guaranteed on the device.
    durable: Vec<u8>,
    /// Writes and extensions since the last successful sync, in issue order.
    pending: Vec<Pend>,
    linked: bool,
    /// Position of the creation in the directory operation log.
    create_seq: u64,
    /// Position of the removal in the directory operation log, once unlinked.
    unlink_seq: Option<u64>,
}

fn apply_write(img: &mut Vec<u8>, off: usize, data: &[u8]) {
    let end = off + data.len();
    if end > img.len() {
        img.resize(end, 0);
    }
    img[off..end].copy_from_slice(data);
}

impl Inode {
    fn write(&mut self, off: usize, data: &[u8], dir_seq: u64) {
        apply_write(&mut self.cache, off, data);
        self.pending.push(Pend::Write {
            off,
            data: data.to_vec(),
            dir_seq,
        });
    }

    fn extend(&mut self, len: usize, dir_seq: u64) {
        if len > self.cache.len() {
            self.cache.resize(len, 0);
            self.pending.push(Pend::Extend { len, dir_seq });
        }
    }

    /// A successful sync: every pending write reaches the device.
    fn sync(&mut self) {
        for p in self.pending.drain(..) {
            match p {
                Pend::Write { off, data, .. } => apply_write(&mut self.durable, off, &data),
                Pend::Extend { len, .. } => {
                    if len > self.durable.len() {
                        self.durable.resize(len, 0);
                    }
                }
            }
        }
    }

    /// The content after a power loss: the durable image plus whatever subset of the pending
    /// writes the seed lets through. A write never reaches the device before the directory
    /// operations issued before it (`dir_ops_done`), as on a journaling filesystem.
    fn power_loss_image(&self, rng: &mut Rng, keep: u64, lose: u64, dir_ops_done: u64) -> Vec<u8> {
        let mut img = self.durable.clone();
        for p in &self.pending {
            match p {
                Pend::Extend { len, dir_seq } => {
                    if rng.chance(1, 2) && *len > img.len() && *dir_seq <= dir_ops_done {
                        img.resize(*len, 0);
                    }
                }
                Pend::Write { off, data, dir_seq } => {
                    let pick = rng.below(100);
                    let (keep, lose) = if *dir_seq <= dir_ops_done {
                        (keep, lose)
                    } else {
                        (0, 100)
                    };
                    let all = pick < keep;
                    let none = !all && pick < keep + lose;
                    let end = off + data.len();
                    let mut any = false;
                    for s in (off / SECTOR)..=((end - 1) / SECTOR) {
                        let cs = (*off).max(s * SECTOR);
                        let ce = end.min((s + 1) * SECTOR);
                        let persist = if all {
                            true
                        } else if none {
                            false
                        } else {
                            rng.chance(1, 2)
                        };
                        if persist {
                            apply_write(&mut img, cs, &data[cs - off..ce - off]);
                            any = true;
                        }
                    }
                    // The size update of an extending write can reach the device without the
                    // data: the new tail then reads as zeros.
                    if !any && end > img.len() && rng.chance(1, 4) {
                        img.resize(end, 0);
                    }
                }
            }
        }
        img
    }
}

/// What an injected failure leaves behind.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Effect {
    /// The operation had no effect (a failed sync leaves its writes pending and retryable).
    Nothing,
    /// The operation took effect although it reported an error (a write that reached the cache,
    /// a sync that reached the device, a file created, a file removed).
    Applied,
    /// A failed sync that dropped the dirty pages and marked them clean ("fsyncgate"): the
    /// pending writes are never persisted and later syncs succeed without writing them.
    Lost,
}

struct Fault {
    err: io::Error,
    effect: Effect,
}

struct SimState {
    inodes: Vec<Inode>,
    dir: HashMap<String, usize>,
    mode: DirMode,
    rng: Rng,
    /// Mutating operations seen since the last crash (writes, syncs, pre-allocations, creations
    /// and removals).
    ops: u64,
    kind_ops: [u64; KIND_COUNT],
    /// The power is cut at this operation: it and every later mutating operation fail.
    cut_at: Option<u64>,
    /// One-shot failure of the n-th mutating operation on a file of this kind.
    fail_at: Option<(FileKind, u64)>,
    cut: bool,
    fail_fired: bool,
    /// Directory operations issued so far, and how many of them are durable.
    dir_seq: u64,
    dir_synced: u64,
    /// Filesystem event log, kept only when replaying a single seed.
    events: Option<Vec<String>>,
    /// Every `remove_file` of a file whose name starts with this fails (repro tests).
    fail_remove_prefix: Option<&'static str>,
}

impl SimState {
    fn note(&mut self, f: impl FnOnce() -> String) {
        if let Some(events) = &mut self.events {
            events.push(f());
        }
    }

    /// Counts one mutating operation and decides whether it fails.
    fn gate(&mut self, kind: FileKind, is_sync: bool) -> Option<Fault> {
        let index = self.ops;
        self.ops += 1;
        let nth = self.kind_ops[kind as usize];
        self.kind_ops[kind as usize] += 1;
        if self.cut {
            return Some(Fault {
                err: io::Error::other("simulated power cut"),
                effect: Effect::Nothing,
            });
        }
        if self.cut_at == Some(index) {
            self.cut = true;
            let effect = if self.rng.chance(1, 2) {
                Effect::Applied
            } else {
                Effect::Nothing
            };
            return Some(Fault {
                err: io::Error::other("simulated power cut"),
                effect,
            });
        }
        if let Some((k, n)) = self.fail_at {
            if !self.fail_fired && k == kind && n == nth {
                self.fail_fired = true;
                let effect = match self.rng.below(if is_sync { 3 } else { 2 }) {
                    0 => Effect::Nothing,
                    1 => Effect::Applied,
                    _ => Effect::Lost,
                };
                self.note(|| {
                    format!(
                        "      fs: INJECTED FAILURE of a {kind:?} {} (op #{nth}), effect {}",
                        if is_sync {
                            "sync/prealloc"
                        } else {
                            "write/create/remove"
                        },
                        match effect {
                            Effect::Nothing => "none",
                            Effect::Applied => "applied",
                            Effect::Lost => "pages dropped (fsyncgate)",
                        }
                    )
                });
                return Some(Fault {
                    err: io::Error::other("injected I/O failure"),
                    effect,
                });
            }
        }
        None
    }

    /// Logs a directory operation; on Unix it is flushed at once (`sync_dir`), unless `flushed`
    /// is false because the operation failed.
    fn dir_op(&mut self, flushed: bool) -> u64 {
        self.dir_seq += 1;
        if flushed && self.mode == DirMode::Unix {
            self.dir_synced = self.dir_seq;
        }
        self.dir_seq
    }

    /// A successful sync of a file: on Windows it also flushes the directory log.
    fn sync_file(&mut self, id: usize) {
        let name = self.inodes[id].name.clone();
        self.note(|| format!("      fs: sync {name}"));
        self.inodes[id].sync();
        if self.mode == DirMode::Windows {
            self.dir_synced = self.dir_seq;
        }
    }

    fn create(&mut self, name: &str, flushed: bool) -> usize {
        let create_seq = self.dir_op(flushed);
        self.note(|| format!("      fs: create {name} (dir op {create_seq})"));
        let id = self.inodes.len();
        self.inodes.push(Inode {
            name: name.to_string(),
            cache: Vec::new(),
            durable: Vec::new(),
            pending: Vec::new(),
            linked: true,
            create_seq,
            unlink_seq: None,
        });
        self.dir.insert(name.to_string(), id);
        id
    }

    fn crash(&mut self, how: Crash) {
        if matches!(how, Crash::Process) {
            self.note(|| {
                "      fs: crash Process (keeping OS cache, pending writes, and dir state)".into()
            });
            self.ops = 0;
            self.kind_ops = [0; KIND_COUNT];
            self.cut_at = None;
            self.fail_at = None;
            self.cut = false;
            self.fail_fired = false;
            return;
        }
        let old = std::mem::take(&mut self.inodes);
        self.dir.clear();
        // The directory operations that took effect: the flushed ones plus a random prefix of
        // the rest.
        let persisted_upto = self.dir_synced + self.rng.below(self.dir_seq - self.dir_synced + 1);
        let (lo, hi) = (self.dir_synced + 1, self.dir_seq);
        self.note(|| {
            format!("      fs: crash {how:?}; directory ops {lo}..={hi} unflushed, {persisted_upto} took effect")
        });
        // Linked inodes first, so that a ghost never shadows a live file of the same name.
        let mut order: Vec<usize> = (0..old.len()).collect();
        order.sort_by_key(|&i| !old[i].linked);
        for i in order {
            let ino = &old[i];
            let exists = match how {
                Crash::Process => ino.linked,
                Crash::Power { .. } => {
                    ino.create_seq <= persisted_upto
                        && ino.unlink_seq.is_none_or(|u| u > persisted_upto)
                }
            };
            if !exists || self.dir.contains_key(&ino.name) {
                continue;
            }
            let img = match how {
                Crash::Process => ino.cache.clone(),
                Crash::Power { keep, lose } => {
                    ino.power_loss_image(&mut self.rng, keep, lose, persisted_upto)
                }
            };
            let id = self.inodes.len();
            self.dir.insert(ino.name.clone(), id);
            self.inodes.push(Inode {
                name: ino.name.clone(),
                cache: img.clone(),
                durable: img,
                pending: Vec::new(),
                linked: true,
                create_seq: 0,
                unlink_seq: None,
            });
        }
        self.dir_seq = 0;
        self.dir_synced = 0;
        self.ops = 0;
        self.kind_ops = [0; KIND_COUNT];
        self.cut_at = None;
        self.fail_at = None;
        self.cut = false;
        self.fail_fired = false;
    }
}

struct CrashFs {
    state: Arc<Mutex<SimState>>,
    root: PathBuf,
}

struct CrashFile {
    state: Arc<Mutex<SimState>>,
    id: usize,
    kind: FileKind,
}

impl CrashFs {
    fn new(mode: DirMode, seed: u64) -> Self {
        Self {
            state: Arc::new(Mutex::new(SimState {
                inodes: Vec::new(),
                dir: HashMap::new(),
                mode,
                rng: Rng::new(seed),
                ops: 0,
                kind_ops: [0; KIND_COUNT],
                cut_at: None,
                fail_at: None,
                cut: false,
                fail_fired: false,
                dir_seq: 0,
                dir_synced: 0,
                events: env_u64("AYDB_CRASH_SEED").map(|_| Vec::new()),
                fail_remove_prefix: None,
            })),
            root: PathBuf::from("/sim/crash"),
        }
    }

    fn st(&self) -> std::sync::MutexGuard<'_, SimState> {
        self.state.lock().unwrap()
    }

    /// Arms the faults of the coming run (cleared by [`Self::crash`]).
    fn arm(&self, cut_at: Option<u64>, fail_at: Option<(FileKind, u64)>) {
        let mut st = self.st();
        st.cut_at = cut_at;
        st.fail_at = fail_at;
    }

    /// Kills I/O: every later mutating operation fails. Used right before the engine is dropped
    /// so that its destructors cannot write after the "crash".
    fn cut_now(&self) {
        self.st().cut = true;
    }

    fn cut_tripped(&self) -> bool {
        self.st().cut
    }

    /// An injected failure or a power cut has happened since the last crash.
    fn fault_happened(&self) -> bool {
        let st = self.st();
        st.cut || st.fail_fired
    }

    fn op_stats(&self) -> (u64, [u64; KIND_COUNT]) {
        let st = self.st();
        (st.ops, st.kind_ops)
    }

    fn crash(&self, how: Crash) {
        self.st().crash(how);
    }

    /// One line per log file: epoch and valid bytes of each used sector (`!` = bad CRC).
    fn wal_summary(&self) -> String {
        let st = self.st();
        let mut out = String::new();
        for name in ["wal_sync.db", "wal_buf.db"] {
            if let Some(&id) = st.dir.get(name) {
                let c = &st.inodes[id].cache;
                out += &format!("      {name} sectors:");
                for (i, sec) in c.chunks(4096).enumerate() {
                    if sec.len() < 4096 {
                        break;
                    }
                    let crc = u32::from_le_bytes(sec[0..4].try_into().unwrap());
                    let ep = u64::from_le_bytes(sec[4..12].try_into().unwrap());
                    let valid = u16::from_le_bytes(sec[12..14].try_into().unwrap());
                    if ep == 0 && valid == 0 {
                        continue;
                    }
                    let ok = crate::util::crc32c::crc32c(&sec[4..]) == crc;
                    out += &format!(" #{i}:{ep}/{valid}{}", if ok { "" } else { "!" });
                }
                out.push('\n');
            }
        }
        out
    }
}

impl PlatformFile for CrashFile {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        let st = self.state.lock().unwrap();
        let c = &st.inodes[self.id].cache;
        let off = offset as usize;
        if off >= c.len() {
            return Ok(0);
        }
        let n = (c.len() - off).min(buf.len());
        buf[..n].copy_from_slice(&c[off..off + n]);
        Ok(n)
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        if self.read_at(buf, offset)? < buf.len() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "eof"));
        }
        Ok(())
    }

    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        let mut st = self.state.lock().unwrap();
        let fault = st.gate(self.kind, false);
        if let Some(f) = fault {
            if f.effect == Effect::Applied {
                // A killed process completes or loses a `write` as a whole; an I/O error such as
                // `ENOSPC` can leave a short write in the cache.
                let n = if st.cut {
                    buf.len()
                } else {
                    st.rng.below(buf.len() as u64 + 1) as usize
                };
                if n > 0 {
                    let seq = st.dir_seq;
                    st.inodes[self.id].write(offset as usize, &buf[..n], seq);
                }
            }
            return Err(f.err);
        }
        let seq = st.dir_seq;
        st.inodes[self.id].write(offset as usize, buf, seq);
        Ok(())
    }

    fn file_len(&self) -> io::Result<u64> {
        Ok(self.state.lock().unwrap().inodes[self.id].cache.len() as u64)
    }

    fn preallocate(&self, target_len: u64, strategy: PreallocStrategy) -> io::Result<()> {
        let mut st = self.state.lock().unwrap();
        let len = target_len as usize;
        if len <= st.inodes[self.id].cache.len() {
            return Ok(());
        }
        let fault = st.gate(self.kind, strategy == PreallocStrategy::PhysicalZeroFill);
        if let Some(f) = fault {
            if f.effect == Effect::Applied {
                {
                    let seq = st.dir_seq;
                    st.inodes[self.id].extend(len, seq);
                }
            }
            return Err(f.err);
        }
        {
            let seq = st.dir_seq;
            st.inodes[self.id].extend(len, seq);
        }
        if strategy == PreallocStrategy::PhysicalZeroFill {
            // `preallocate_physical_zero_fill` ends with `sync_durable`: the zero-filled length
            // (and every earlier pending write) is on the device when the call returns.
            st.sync_file(self.id);
        }
        // `LogicalExtendOnly` (`set_len` / `SetEndOfFile`) stays pending until a sync.
        Ok(())
    }

    fn sync_barrier(&self) -> io::Result<()> {
        self.sync_durable()
    }

    fn sync_durable(&self) -> io::Result<()> {
        let mut st = self.state.lock().unwrap();
        let fault = st.gate(self.kind, true);
        if let Some(f) = fault {
            match f.effect {
                Effect::Nothing => {}
                Effect::Applied => st.sync_file(self.id),
                Effect::Lost => st.inodes[self.id].pending.clear(),
            }
            return Err(f.err);
        }
        st.sync_file(self.id);
        Ok(())
    }

    fn evict_os_cache(&self, _offset: u64, _len: u64) -> io::Result<()> {
        Ok(())
    }
}

impl PlatformFs for CrashFs {
    type File = CrashFile;

    fn open_file(
        &self,
        rel_name: &str,
        create: bool,
        _writable: bool,
        _hint: OpenHint,
    ) -> io::Result<Arc<Self::File>> {
        let kind = classify(rel_name);
        let mut st = self.st();
        if let Some(&id) = st.dir.get(rel_name) {
            return Ok(Arc::new(CrashFile {
                state: Arc::clone(&self.state),
                id,
                kind,
            }));
        }
        if !create {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("{rel_name} not found"),
            ));
        }
        let fault = st.gate(kind, false);
        if let Some(f) = fault {
            if f.effect == Effect::Applied {
                st.create(rel_name, false);
            }
            return Err(f.err);
        }
        let id = st.create(rel_name, true);
        Ok(Arc::new(CrashFile {
            state: Arc::clone(&self.state),
            id,
            kind,
        }))
    }

    fn remove_file(&self, rel_name: &str) -> io::Result<()> {
        let kind = classify(rel_name);
        let mut st = self.st();
        let Some(&id) = st.dir.get(rel_name) else {
            // Both real implementations treat a missing file as success.
            return Ok(());
        };
        if st
            .fail_remove_prefix
            .is_some_and(|p| rel_name.starts_with(p))
        {
            return Err(io::Error::other("injected removal failure"));
        }
        let fault = st.gate(kind, false);
        if let Some(f) = fault {
            if f.effect == Effect::Applied {
                let seq = st.dir_op(false);
                st.dir.remove(rel_name);
                let ino = &mut st.inodes[id];
                ino.linked = false;
                ino.unlink_seq = Some(seq);
            }
            return Err(f.err);
        }
        let seq = st.dir_op(true);
        st.note(|| format!("      fs: remove {rel_name} (dir op {seq})"));
        st.dir.remove(rel_name);
        let ino = &mut st.inodes[id];
        ino.linked = false;
        ino.unlink_seq = Some(seq);
        Ok(())
    }

    fn list_files(&self) -> io::Result<Vec<String>> {
        let mut names: Vec<String> = self.st().dir.keys().cloned().collect();
        names.sort();
        Ok(names)
    }

    fn root_path(&self) -> &Path {
        &self.root
    }
}

// =============================================================================================
// Model and oracle
// =============================================================================================

/// `(table id, key index)` -> value.
type Table = BTreeMap<(u16, u8), Vec<u8>>;

/// Tables 1 and 2 are plain: the oracle compares them exactly. Tables 3 and 4 are capped and
/// evict on their own, which the model cannot predict, so rows may disappear from them but a
/// row that is present must carry exactly the value the model has for the recovered state.
const TABLES: [u16; 4] = [1, 2, 3, 4];
const KEYS: u8 = 12;
/// First table id of the extra capped tables that make the catalog overflow into a manifest.
const WIDE_BASE: u16 = 100;
/// A wide catalog registers 160..=185 extra tables, so that its payload is close to the 4028
/// bytes that still fit inline and a few segments more or fewer decide whether a manifest exists.
const WIDE_TABLES_MIN: u16 = 160;

fn is_capped(t: u16) -> bool {
    t >= 3
}

/// Index id of the backfilled "unique" entries (the persistent UNIQUE index uses `0x8000 | col`).
const UNIQUE_IID: u16 = 0x8001;

/// Tables whose secondary entries the oracle checks. Table 1 is plain and starts every row
/// without entries (as a legacy table does): only `backfill_secondary_keys` adds them, so a
/// row is either complete or has no entry and no `'t'` record. Tables 2 (plain) and 3 (capped)
/// write their entries with the row and must always be complete. Table 4 has none.
const INDEXED_TABLES: [u16; 3] = [1, 2, 3];

/// The complete, deterministic secondary keys of a row, a pure function of its value.
fn full_keys(t: u16, v: &[u8]) -> Vec<(u16, Vec<u8>)> {
    match t {
        1 => vec![(UNIQUE_IID, v[2..4].to_vec())],
        2 => vec![(1, vec![v[2]]), (UNIQUE_IID, v[2..4].to_vec())],
        3 => {
            let mut keys = vec![(1, vec![v[2] % 3]), (UNIQUE_IID, v[2..4].to_vec())];
            if v[3].is_multiple_of(4) {
                // Long component, as a digest of an oversized unique key would be.
                keys.push((0x8002, vec![v[3]; 77]));
            }
            keys
        }
        _ => Vec::new(),
    }
}

/// What a plain `put_row` passes: table 1 deliberately omits its keys.
fn put_keys(t: u16, v: &[u8]) -> Vec<(u16, Vec<u8>)> {
    if t == 1 { Vec::new() } else { full_keys(t, v) }
}

fn key_bytes(k: u8) -> Vec<u8> {
    format!("key{k:02}").into_bytes()
}

fn describe(d: WriteDurability) -> &'static str {
    match d {
        WriteDurability::SyncTransactional => "sync",
        WriteDurability::BufferedFast => "buf",
    }
}

fn pick_durability(rng: &mut Rng) -> WriteDurability {
    if rng.chance(1, 2) {
        WriteDurability::SyncTransactional
    } else {
        WriteDurability::BufferedFast
    }
}

fn small_value_len(rng: &mut Rng) -> usize {
    4 + rng.below(180) as usize
}

fn make_value(rng: &mut Rng, t: u16, k: u8) -> Vec<u8> {
    let blob_pct = match t {
        1 => 50,
        2 => 8,
        3 => 15,
        _ => 70,
    };
    let len = if rng.chance(blob_pct, 100) {
        // Above the 1024-byte inline threshold: stored out of line.
        1100 + rng.below(1500) as usize
    } else {
        small_value_len(rng)
    };
    let ver = rng.next();
    let mut v = Vec::with_capacity(len);
    v.push(t as u8);
    v.push(k);
    v.extend_from_slice(&ver.to_le_bytes()[..2]);
    while v.len() < len {
        let i = v.len() as u64;
        v.push((ver.wrapping_mul(0x2545_F491_4F6C_DD1D) >> (i % 40)) as u8 ^ (i as u8));
    }
    v
}

fn short(v: &[u8]) -> String {
    format!(
        "{}B/{:02x}{:02x}{:02x}{:02x}",
        v.len(),
        v[0],
        v[1],
        v[2],
        v[3]
    )
}

fn diff(want: &Table, got: &Table) -> Vec<String> {
    let mut out = Vec::new();
    for t in TABLES {
        for k in 0..KEYS {
            let w = want.get(&(t, k));
            let g = got.get(&(t, k));
            if w != g {
                out.push(format!(
                    "t{t}/key{k:02}: model {} / engine {}",
                    w.map_or("absent".to_string(), |v| short(v)),
                    g.map_or("absent".to_string(), |v| short(v)),
                ));
            }
        }
    }
    out
}

/// Whether the engine state `got` is acceptable for the model state `want`.
fn matches(want: &Table, got: &Table) -> bool {
    for t in TABLES {
        for k in 0..KEYS {
            let w = want.get(&(t, k));
            let g = got.get(&(t, k));
            let ok = if is_capped(t) {
                g.is_none() || g == w
            } else {
                g == w
            };
            if !ok {
                return false;
            }
        }
    }
    true
}

/// Coverage counters, summed over all cases of a run.
#[derive(Default)]
struct Coverage {
    cases: AtomicU64,
    crashes: AtomicU64,
    process_crashes: AtomicU64,
    cuts_tripped: AtomicU64,
    failures_fired: AtomicU64,
    recovery_crashes: AtomicU64,
    /// Recoveries that returned fewer commits than were acknowledged to the caller (allowed for
    /// buffered commits after a power loss).
    lost_buffered_tail: AtomicU64,
    /// Recoveries that dropped an unacknowledged commit that had been issued.
    dropped_inflight: AtomicU64,
    wal_rotations: AtomicU64,
    /// Open engines that reported at least one swallowed maintenance failure.
    maintenance_failures: AtomicU64,
    poisoned: AtomicU64,
    /// Recoveries whose result changed at the next, immediate crash.
    unstable_recovery: AtomicU64,
    /// Recoveries checked by the LSN-order oracle, and those that replayed fewer batches than
    /// were logged (the oracle is only meaningful if it sees real cuts).
    lsn_checks: AtomicU64,
    lsn_cuts: AtomicU64,
    /// Recoveries whose report was checked to show no damage (power cuts and `kill -9` cannot
    /// damage synced data, whatever unsynced tail they lose).
    no_damage_checks: AtomicU64,
    /// Recoveries checked by the durable-watermark oracle, those whose logged batches carried a
    /// watermark above the checkpoint, and the batches below it that were verified present.
    durable_checks: AtomicU64,
    durable_marks: AtomicU64,
    durable_batches: AtomicU64,
    manifests_seen: AtomicU64,
    blob_segments_seen: AtomicU64,
    fail_by_kind: [AtomicU64; KIND_COUNT],
    /// Counters of the media-damage and group-commit modes.
    media: media::MediaCov,
}

impl Coverage {
    fn bump(c: &AtomicU64) {
        c.fetch_add(1, Ordering::Relaxed);
    }

    fn report(&self) -> String {
        let g = |c: &AtomicU64| c.load(Ordering::Relaxed);
        let kinds: Vec<String> = ALL_KINDS
            .iter()
            .map(|k| format!("{k:?}={}", g(&self.fail_by_kind[*k as usize])))
            .collect();
        format!(
            "cases={} crashes={} (process={}) cut_tripped={} failure_fired={} [{}] \
             recovery_crashes={} lost_buffered_tail={} dropped_inflight={} wal_rotations={} \
             maintenance_failures={} poisoned={} unstable_recovery={} \
             lsn_checks={} lsn_cuts={} no_damage_checks={} durable_checks={} durable_marks={} \
             durable_batches={} \
             manifests_seen={} blob_segments_seen={}
  media/group: {}",
            g(&self.cases),
            g(&self.crashes),
            g(&self.process_crashes),
            g(&self.cuts_tripped),
            g(&self.failures_fired),
            kinds.join(" "),
            g(&self.recovery_crashes),
            g(&self.lost_buffered_tail),
            g(&self.dropped_inflight),
            g(&self.wal_rotations),
            g(&self.maintenance_failures),
            g(&self.poisoned),
            g(&self.unstable_recovery),
            g(&self.lsn_checks),
            g(&self.lsn_cuts),
            g(&self.no_damage_checks),
            g(&self.durable_checks),
            g(&self.durable_marks),
            g(&self.durable_batches),
            g(&self.manifests_seen),
            g(&self.blob_segments_seen),
            self.media.report(),
        )
    }
}

#[derive(Clone, Default)]
struct EpochStat {
    ops: u64,
    kind_ops: [u64; KIND_COUNT],
}

#[derive(Clone, Copy)]
enum FaultChoice {
    None,
    Cut(u64),
    Fail(FileKind, u64),
}

type Eng = StorageEngine<CrashFs>;

/// A throwaway filesystem holding the given files, durable and cached alike. The oracle reads
/// recovery inputs through it without touching the filesystem under test or its random stream.
fn scratch_fs(files: Vec<(String, Vec<u8>)>) -> Arc<CrashFs> {
    let fs = Arc::new(CrashFs::new(DirMode::Unix, 0));
    {
        let mut st = fs.st();
        for (name, img) in files {
            let id = st.inodes.len();
            st.inodes.push(Inode {
                name: name.clone(),
                cache: img.clone(),
                durable: img,
                pending: Vec::new(),
                linked: true,
                create_seq: 0,
                unlink_seq: None,
            });
            st.dir.insert(name, id);
        }
    }
    fs
}

struct Harness<'a> {
    seed: u64,
    fs: Arc<CrashFs>,
    cfg: EngineConfig,
    cov: &'a Coverage,
    /// Workload randomness, independent of the plan and filesystem randomness so that a run with
    /// faults issues the same operations as the clean run up to the fault.
    rng: Rng,
    /// `states[i]` is the model after the i-th commit that was issued (`states[0]` is empty).
    /// The last entry may belong to a commit that was not acknowledged.
    states: Vec<Table>,
    /// Index of the last acknowledged `SyncTransactional` commit (or checkpoint): it and
    /// everything before it must survive any crash.
    acked_sync: usize,
    /// Index of the last acknowledged commit of either kind: it must survive a process crash.
    acked_any: usize,
    /// Number of extra capped tables (0 = narrow catalog).
    wide: u16,
    wide_dropped: u16,
    quota3: u64,
    quota4: u64,
    /// Backfill operations are frequent in this run.
    backfill_heavy: bool,
    trace: Vec<String>,
    /// How many trace lines a failure report keeps (all of them when replaying one seed).
    trace_cap: usize,
    mode: Mode,
    /// The logs were damaged after the last crash and no open has seen them yet: the next open
    /// runs under `Truncate` and may report damage.
    damage_pending: bool,
    /// Batches were lost to the damage, so the next recovery may show an older state than the
    /// acknowledged one.
    relaxed: bool,
    /// What the copies of the damaged directory reported; the real open must match.
    expect_damage: Option<ExpectDamage>,
    /// The LSNs and rows of the workload's operations in this epoch, for the salvage oracle.
    oplog: Vec<OpRec>,
    /// Group commits appended and not yet seen durable.
    tickets: Vec<Ticket>,
}

impl<'a> Harness<'a> {
    fn log(&mut self, s: String) {
        self.fs.st().note(|| format!("    {s}"));
        if self.trace.len() >= self.trace_cap {
            self.trace.remove(0);
        }
        self.trace.push(s);
    }

    /// Debug aid for single-seed replays: what recovery will see in each log after a crash.
    fn log_wal_contents(&mut self) {
        let newest = {
            let st = self.fs.st();
            st.dir.get("boot_meta.db").map(|&id| {
                let c = &st.inodes[id].cache;
                let mut best = (0u64, 0u64, 0u64, 0u64);
                for slot in 0..2 {
                    let b = &c[slot * 4096..(slot + 1) * 4096];
                    let g = u64::from_le_bytes(b[8..16].try_into().unwrap());
                    if g >= best.0 {
                        best = (
                            g,
                            u64::from_le_bytes(b[32..40].try_into().unwrap()),
                            u64::from_le_bytes(b[40..48].try_into().unwrap()),
                            u64::from_le_bytes(b[24..32].try_into().unwrap()),
                        );
                    }
                }
                best
            })
        };
        let Some((g, se, be, ck)) = newest else {
            return;
        };
        let mut lines = vec![format!(
            "      newest slot: generation {g}, sync epoch {se}, buf epoch {be}, checkpoint lsn {ck}"
        )];
        {
            let st = self.fs.st();
            if let Some(&id) = st.dir.get("boot_meta.db") {
                let c = &st.inodes[id].cache;
                for slot in 0..2 {
                    let b = &c[slot * 4096..(slot + 1) * 4096];
                    let flags = u16::from_le_bytes(b[6..8].try_into().unwrap());
                    let plen = u32::from_le_bytes(b[48..52].try_into().unwrap()) as usize;
                    let gen_ = u64::from_le_bytes(b[8..16].try_into().unwrap());
                    if flags != 0 || plen == 0 || gen_ != g {
                        continue;
                    }
                    let mut state = crate::meta::BootMetaState::default();
                    if state.decode_catalog_payload(&b[64..64 + plen]).is_ok() {
                        lines.push(format!(
                            "      newest catalog: idx={:?} std={:?} lru={:?} pending_delete={:?}",
                            state
                                .idx_segments
                                .iter()
                                .map(|x| x.seq_id)
                                .collect::<Vec<_>>(),
                            state
                                .std_blob_segments
                                .iter()
                                .map(|x| x.seq_id)
                                .collect::<Vec<_>>(),
                            state
                                .lru_blob_segments
                                .iter()
                                .map(|x| x.seq_id)
                                .collect::<Vec<_>>(),
                            state.pending_delete_files
                        ));
                    }
                }
            }
        }
        for (name, ep) in [("wal_sync.db", se), ("wal_buf.db", be)] {
            let id = self.fs.st().dir.get(name).copied();
            if let Some(id) = id {
                let f = CrashFile {
                    state: Arc::clone(&self.fs.state),
                    id,
                    kind: FileKind::WalSync,
                };
                if let Ok(rec) = crate::wal::recover_single_wal_stream(&f, ep) {
                    let b: Vec<(u64, usize)> = rec
                        .batches
                        .iter()
                        .map(|b| (b.lsn, b.records.len()))
                        .collect();
                    lines.push(format!(
                        "      {name} at epoch {ep} replays (lsn, records): {b:?}"
                    ));
                }
            }
        }
        for l in lines {
            self.log(l);
        }
    }

    fn fail(&self, what: String) -> String {
        if self.fs.st().events.is_some() {
            let st = self.fs.st();
            if let Some(&id) = st.dir.get("boot_meta.db") {
                let c = &st.inodes[id].cache;
                for slot in 0..2 {
                    let b = &c[slot * 4096..(slot + 1) * 4096];
                    let gen_ = u64::from_le_bytes(b[8..16].try_into().unwrap());
                    let flags = u16::from_le_bytes(b[6..8].try_into().unwrap());
                    let ckpt = u64::from_le_bytes(b[24..32].try_into().unwrap());
                    let plen = u32::from_le_bytes(b[48..52].try_into().unwrap());
                    let mut dbg = format!(
                        "      boot_meta slot {slot}: generation {gen_} flags {flags} checkpoint_lsn {ckpt} payload {plen}B"
                    );
                    if flags == 0 && plen > 0 {
                        let mut state = crate::meta::BootMetaState::default();
                        if state
                            .decode_catalog_payload(&b[64..64 + plen as usize])
                            .is_ok()
                        {
                            dbg += &format!(
                                " idx={:?} std={:?} lru={:?} pending_delete={:?}",
                                state
                                    .idx_segments
                                    .iter()
                                    .map(|x| x.seq_id)
                                    .collect::<Vec<_>>(),
                                state
                                    .std_blob_segments
                                    .iter()
                                    .map(|x| x.seq_id)
                                    .collect::<Vec<_>>(),
                                state
                                    .lru_blob_segments
                                    .iter()
                                    .map(|x| x.seq_id)
                                    .collect::<Vec<_>>(),
                                state.pending_delete_files
                            );
                        }
                    }
                    eprintln!("{dbg}");
                }
            }
        }
        let trace = match &self.fs.st().events {
            Some(events) => events.join(
                "
",
            ),
            None => format!(
                "    {}",
                self.trace.join(
                    "
    "
                )
            ),
        };
        format!(
            "seed {}: {what}
  last operations:
{trace}",
            self.seed
        )
    }

    /// Turns an engine result into "continue", "stop because a fault happened" or an error.
    fn io_ok<T>(&self, r: io::Result<T>, what: &str) -> Result<Option<T>, String> {
        match r {
            Ok(v) => Ok(Some(v)),
            Err(e) if self.fs.fault_happened() => {
                let _ = e;
                Ok(None)
            }
            Err(e) => Err(self.fail(format!(
                "unexpected error from {what} without any fault: {e}"
            ))),
        }
    }

    /// After a fault the run stops when the power is cut or the engine poisoned itself; any
    /// other failed operation is documented as not having happened, and the run goes on (a
    /// retried sync that wrongly reports success is how acknowledged data gets lost).
    fn dead(&self, e: &Eng) -> bool {
        self.fs.cut_tripped() || e.is_poisoned()
    }

    fn abort_tx(&mut self, e: &mut Eng) -> Result<bool, String> {
        let dead = self.dead(e);
        if !dead {
            let _ = e.rollback_transaction();
            self.log("  (failed, transaction rolled back, engine alive)".into());
        }
        Ok(!dead)
    }

    /// A table of the workload: the capped ones are left out when group commit is driven (the
    /// engine does not group while a capped table exists).
    fn pick_table(&mut self) -> u16 {
        let n = if self.mode.group { 2 } else { 4 };
        TABLES[self.rng.below(n) as usize]
    }

    fn last(&self) -> &Table {
        self.states.last().unwrap()
    }

    /// Records the outcome of a commit. Returns whether the workload may go on.
    /// `logged` tells whether the operation appended anything to a log. One that did not (a
    /// delete of a missing row, a transaction without records) is no commit: it makes nothing
    /// durable, so it must not advance the acknowledged-durable mark.
    fn record_commit(
        &mut self,
        r: io::Result<()>,
        ns: Table,
        sync: bool,
        logged: bool,
        alive: bool,
        what: &str,
    ) -> Result<bool, String> {
        let tripped = self.fs.cut_tripped();
        match r {
            Err(e) => {
                if !self.fs.fault_happened() {
                    return Err(self.fail(format!("{what} failed without any fault: {e}")));
                }
                if alive && !tripped {
                    self.log(format!("{what} -> Err (engine alive: not applied)"));
                    return Ok(true);
                }
                // It may or may not be durable, but it is never partially applied.
                self.states.push(ns);
                self.log(format!(
                    "{what} -> Err (candidate state {})",
                    self.states.len() - 1
                ));
                Ok(false)
            }
            Ok(()) if !logged => {
                self.log(format!("{what} -> Ok (nothing was logged)"));
                Ok(!tripped)
            }
            Ok(()) => {
                self.states.push(ns);
                let idx = self.states.len() - 1;
                if !tripped {
                    self.acked_any = idx;
                    if sync {
                        self.acked_sync = idx;
                    }
                }
                self.log(format!("{what} -> Ok (state {idx}, acked={})", !tripped));
                Ok(!tripped)
            }
        }
    }

    /// Reads the whole key universe. A read error is a failure, never "absent".
    fn read_all(&self, e: &mut Eng) -> Result<Option<Table>, String> {
        let mut t = Table::new();
        for table in TABLES {
            for k in 0..KEYS {
                match e.get_row(table, &key_bytes(k)) {
                    Ok(Some(v)) => {
                        t.insert((table, k), v);
                    }
                    Ok(None) => {}
                    Err(err) => {
                        if self.fs.fault_happened() {
                            return Ok(None);
                        }
                        return Err(
                            self.fail(format!("row t{table}/key{k:02} reads as an error: {err}"))
                        );
                    }
                }
            }
        }
        Ok(Some(t))
    }

    /// While the engine runs, it must show the newest model state.
    fn live_check(&self, e: &mut Eng) -> Result<(), String> {
        let Some(got) = self.read_all(e)? else {
            return Ok(());
        };
        if !matches(self.last(), &got) {
            let d = diff(self.last(), &got);
            return Err(self.fail(format!(
                "live state differs from the model:\n    {}",
                d.join("\n    ")
            )));
        }
        self.index_check(e, &got, false)
    }

    /// Compares the secondary entries and `'t'` records with the rows that are present.
    ///
    /// Every present row of tables 2 and 3 must have exactly the entries and the record
    /// `full_keys` derives from its value. A row of table 1 has either all of them or none (no
    /// entry, no record); with `strict` it must have all of them. No other entry or record may
    /// exist: an entry of a row that is absent (deleted, evicted, rolled back) is an orphan.
    fn index_check(&self, e: &mut Eng, got: &Table, strict: bool) -> Result<(), String> {
        for t in INDEXED_TABLES {
            match self.index_problem(e, t, got, strict) {
                Ok(None) => {}
                Ok(Some(problem)) => {
                    return Err(self.fail(format!("secondary index of t{t}: {problem}")));
                }
                Err(err) => {
                    if self.fs.fault_happened() {
                        return Ok(());
                    }
                    return Err(self.fail(format!("index check of t{t} failed to read: {err}")));
                }
            }
        }
        Ok(())
    }

    fn index_problem(
        &self,
        e: &mut Eng,
        t: u16,
        got: &Table,
        strict: bool,
    ) -> io::Result<Option<String>> {
        let live = |entries: Vec<IndexEntry>| -> BTreeSet<Vec<u8>> {
            entries
                .into_iter()
                .filter(|en| !matches!(en.value, IndexValue::Tombstone))
                .map(|en| en.key)
                .collect()
        };
        let actual_i = live(e.scan_merged_prefix(&encode_namespace_prefix(t, NS_SECONDARY_INDEX))?);
        let actual_t = live(e.scan_merged_prefix(&encode_namespace_prefix(t, NS_ROW_META))?);
        let mut want_i = BTreeSet::new();
        let mut want_t = BTreeSet::new();
        for ((tt, k), v) in got {
            if *tt != t {
                continue;
            }
            let pk = key_bytes(*k);
            let full = full_keys(t, v);
            let entries: Vec<Vec<u8>> = full
                .iter()
                .map(|(id, col)| encode_secondary_index_key(t, *id, col, &pk))
                .collect();
            let present = entries.iter().filter(|en| actual_i.contains(*en)).count();
            let meta = match e.get_row_meta_lookup(&encode_row_meta_key(t, &pk))? {
                MetaLookup::Found(m, _) => Some(m),
                MetaLookup::Absent => None,
                MetaLookup::Corrupt(_) => {
                    return Ok(Some(format!("key{k:02}: unreadable 't' record")));
                }
            };
            let complete =
                present == entries.len() && meta.as_ref().is_some_and(|m| m.secondary_keys == full);
            if complete {
                if is_capped(t) && meta.as_ref().is_some_and(|m| m.recorded_tick == 0) {
                    return Ok(Some(format!("key{k:02}: capped row has recorded_tick 0")));
                }
                want_i.extend(entries);
                want_t.insert(encode_row_meta_key(t, &pk));
            } else if t == 1 && !strict && present == 0 && meta.is_none() {
                // Not backfilled yet.
            } else {
                return Ok(Some(format!(
                    "key{k:02}: {present} of {} entries present, record {}{}",
                    entries.len(),
                    match &meta {
                        None => "absent".to_string(),
                        Some(m) if m.secondary_keys == full => "complete".to_string(),
                        Some(m) => format!("holds {} pairs", m.secondary_keys.len()),
                    },
                    if strict {
                        " (backfill had finished)"
                    } else {
                        ""
                    }
                )));
            }
        }
        for (what, want, actual) in [
            ("entry", &want_i, &actual_i),
            ("'t' record", &want_t, &actual_t),
        ] {
            if let Some(extra) = actual.difference(want).next() {
                return Ok(Some(format!(
                    "orphan {what} {extra:02x?} (no present row owns it)"
                )));
            }
            if let Some(missing) = want.difference(actual).next() {
                return Ok(Some(format!("missing {what} {missing:02x?}")));
            }
        }
        Ok(None)
    }

    /// Checks the state a reopened engine shows against the oracle and returns its commit index.
    ///
    /// The recovered state must equal the model after `k` commits for some `k` between the last
    /// guaranteed-durable commit and the newest issued one (the commits form one sequence, so
    /// this is both the prefix property and atomicity). Afterwards the model is rebased on `k`.
    fn verify(&mut self, e: &mut Eng, process_crash: bool) -> Result<usize, String> {
        let got = self
            .read_all(e)?
            .ok_or_else(|| self.fail("faults are armed during verification".into()))?;
        // After media damage that lost batches, acknowledged commits may be gone (what recovery
        // was bound to report is checked against the logs by `damage_wal`).
        let lo = if self.relaxed {
            0
        } else if process_crash {
            self.acked_any
        } else {
            self.acked_sync
        };
        let hi = self.states.len() - 1;
        let Some(k) = (lo..=hi).rev().find(|&k| matches(&self.states[k], &got)) else {
            let cause = if (0..lo).any(|k| matches(&self.states[k], &got)) {
                "an acknowledged commit was lost"
            } else {
                "the state is not the result of any prefix of the commit sequence (torn commit,                  wrong or corrupt value)"
            };
            let d_lo = diff(&self.states[lo], &got);
            let d_hi = diff(&self.states[hi], &got);
            return Err(self.fail(format!(
                "recovered state violates the oracle: {cause}\n  acked_sync={} acked_any={} \
                 (process crash: {process_crash}), candidates {lo}..={hi}\n  vs state {lo}:\n    {}\n  \
                 vs state {hi}:\n    {}",
                self.acked_sync,
                self.acked_any,
                d_lo.join("\n    "),
                d_hi.join("\n    ")
            )));
        };
        if k < self.acked_any {
            Coverage::bump(&self.cov.lost_buffered_tail);
        }
        if k < hi {
            Coverage::bump(&self.cov.dropped_inflight);
        }
        self.index_check(e, &got, false)?;
        self.states.truncate(k + 1);
        // Capped tables may have evicted rows the model still holds: rebase on what is there.
        let mut base = self.states[k].clone();
        base.retain(|(t, _), _| !is_capped(*t));
        for ((t, key), v) in got {
            if is_capped(t) {
                base.insert((t, key), v);
            }
        }
        self.states[k] = base;
        self.acked_any = k;
        self.acked_sync = self.acked_sync.min(k);
        self.relaxed = false;
        self.log(format!("recovered at state {k} (range {lo}..={hi})"));
        Ok(k)
    }

    /// Copies the log files as the page cache holds them now: every batch that was logged,
    /// including those the coming crash will lose. Call before [`CrashFs::crash`].
    fn snapshot_wal(&self) -> Vec<(String, Vec<u8>)> {
        let st = self.fs.st();
        ["wal_sync.db", "wal_buf.db"]
            .iter()
            .filter_map(|&n| {
                let id = *st.dir.get(n)?;
                Some((n.to_string(), st.inodes[id].cache.clone()))
            })
            .collect()
    }

    /// LSN-order oracle (ARCHITECTURE.md 4.6.2, 4.6.11). Runs the recovery coordinator on a copy
    /// of the crashed filesystem, before the engine opens it, and on the logs as they were before
    /// the crash. The batches recovery would replay must be a prefix of the logged batches in
    /// global LSN order: nothing may be replayed after a logged batch that is missing. The
    /// state-level oracle in [`Self::verify`] cannot see this, because a later commit may
    /// overwrite the effect of a missing one.
    ///
    /// It holds for any damage as well: the replay is always the batches below the cut. It also
    /// checks that no LSN found in the crashed logs, discarded or not, would be reused.
    fn check_lsn_prefix(&self, logged: &[(String, Vec<u8>)]) -> Result<(), String> {
        let post: Vec<(String, Vec<u8>)> = {
            let st = self.fs.st();
            st.dir
                .iter()
                .map(|(n, &id)| (n.clone(), st.inodes[id].cache.clone()))
                .collect()
        };
        let post_fs = scratch_fs(post);
        let has = |fs: &CrashFs, n: &str| fs.st().dir.contains_key(n);
        // Without a log file in either copy there is nothing to compare (the engine creates it).
        let logs = ["wal_sync.db", "wal_buf.db"];
        let logged_fs = scratch_fs(logged.to_vec());
        if !logs.iter().all(|n| has(&post_fs, n) && has(&logged_fs, n)) {
            return Ok(());
        }
        // A meta file that cannot be loaded makes the real open fail or start empty; the
        // state-level checks report that.
        let Ok(meta) =
            crate::meta::BootMetaManager::open_or_recover_deferred_sweep(Arc::clone(&post_fs))
        else {
            return Ok(());
        };
        let m = meta.state().clone();
        let sf = raw_file(&post_fs, "wal_sync.db");
        let bf = raw_file(&post_fs, "wal_buf.db");
        let mut validate = |ptr: &crate::wal::BlobPtr| -> io::Result<bool> {
            let r =
                crate::blob::BlobSegmentReader::open(&*post_fs, ptr.kind, ptr.table_id, ptr.seg_id);
            match r {
                Ok(r) => r.validate_blob_ptr(ptr),
                Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
                Err(e) => Err(e),
            }
        };
        let recovered = crate::wal::recover_two_streams(
            &*sf,
            m.sync_wal_epoch,
            &*bf,
            m.buf_wal_epoch,
            m.checkpoint_lsn,
            &mut validate,
        )
        .map_err(|e| self.fail(format!("oracle: recovery of the crashed logs failed: {e}")))?;
        // The same logs before the crash, with every blob taken as present: the reference order.
        let sf = raw_file(&logged_fs, "wal_sync.db");
        let bf = raw_file(&logged_fs, "wal_buf.db");
        let reference = crate::wal::recover_two_streams(
            &*sf,
            m.sync_wal_epoch,
            &*bf,
            m.buf_wal_epoch,
            m.checkpoint_lsn,
            |_| Ok(true),
        )
        .map_err(|e| self.fail(format!("oracle: reading the logged batches failed: {e}")))?;
        let mut want: Vec<u64> = reference.merged_batches.iter().map(|b| b.lsn).collect();
        let got: Vec<u64> = recovered.merged_batches.iter().map(|b| b.lsn).collect();
        // A failed write can leave a torn sector in the page cache while the device still holds
        // an earlier, complete version of it. Whatever batch the crashed files hold was logged,
        // so it belongs to the reference even if the cache hid it from the snapshot.
        for (name, epoch) in [
            ("wal_sync.db", m.sync_wal_epoch),
            ("wal_buf.db", m.buf_wal_epoch),
        ] {
            let scan = crate::wal::recover_single_wal_stream(&*raw_file(&post_fs, name), epoch)
                .map_err(|e| self.fail(format!("oracle: scanning {name}: {e}")))?;
            for b in scan.batches.iter().chain(&scan.past_stop) {
                if b.lsn > m.checkpoint_lsn && !want.contains(&b.lsn) {
                    want.push(b.lsn);
                }
            }
        }
        want.sort_unstable();
        Coverage::bump(&self.cov.lsn_checks);
        for (name, epoch) in [
            ("wal_sync.db", m.sync_wal_epoch),
            ("wal_buf.db", m.buf_wal_epoch),
        ] {
            let scan = crate::wal::recover_single_wal_stream(&*raw_file(&post_fs, name), epoch)
                .map_err(|e| self.fail(format!("oracle: scanning {name}: {e}")))?;
            let top = scan
                .batches
                .iter()
                .chain(&scan.past_stop)
                .map(|b| b.lsn)
                .max();
            if top.is_some_and(|t| t > recovered.max_recovered_lsn) {
                return Err(self.fail(format!(
                    "{name} holds LSN {top:?} above the highest LSN recovery saw ({})",
                    recovered.max_recovered_lsn
                )));
            }
        }
        if got.len() < want.len() {
            Coverage::bump(&self.cov.lsn_cuts);
        }
        if got.len() > want.len() || want[..got.len()] != got[..] {
            return Err(self.fail(format!(
                "recovery replays batches that are not a prefix of the logged LSN order\n  \
                 logged:    {want:?}\n  recovered: {got:?}"
            )));
        }
        Ok(())
    }

    /// Durable-watermark oracle (ARCHITECTURE.md 4.6.4). Takes the largest `durable_lsn` among the
    /// batches in the page-cache images of both logs before the crash. Every batch at or below
    /// it, and above the checkpoint, was promised to be on the device, so the crashed files must
    /// still hold it, for a power cut as for `kill -9`. The batches are read raw: whether their
    /// blobs verify is a different question.
    fn check_durable_watermark(&self, logged: &[(String, Vec<u8>)]) -> Result<(), String> {
        let post: Vec<(String, Vec<u8>)> = {
            let st = self.fs.st();
            st.dir
                .iter()
                .map(|(n, &id)| (n.clone(), st.inodes[id].cache.clone()))
                .collect()
        };
        let post_fs = scratch_fs(post);
        let logged_fs = scratch_fs(logged.to_vec());
        let logs = ["wal_sync.db", "wal_buf.db"];
        let has = |fs: &CrashFs, n: &str| fs.st().dir.contains_key(n);
        if !logs.iter().all(|n| has(&post_fs, n) && has(&logged_fs, n)) {
            return Ok(());
        }
        let Ok(meta) =
            crate::meta::BootMetaManager::open_or_recover_deferred_sweep(Arc::clone(&post_fs))
        else {
            return Ok(());
        };
        let m = meta.state().clone();
        let batches = |fs: &CrashFs, name: &str, epoch: u64| {
            crate::wal::recover_single_wal_stream(&*raw_file(fs, name), epoch).map(|r| r.batches)
        };
        let read = |fs: &CrashFs| -> io::Result<Vec<crate::wal::WalBatch>> {
            let mut all = batches(fs, "wal_sync.db", m.sync_wal_epoch)?;
            all.extend(batches(fs, "wal_buf.db", m.buf_wal_epoch)?);
            all.retain(|b| b.lsn > m.checkpoint_lsn);
            Ok(all)
        };
        let before = read(&logged_fs)
            .map_err(|e| self.fail(format!("oracle: reading the logs failed: {e}")))?;
        let after = read(&post_fs)
            .map_err(|e| self.fail(format!("oracle: reading the crashed logs: {e}")))?;
        let floor = before.iter().map(|b| b.durable_lsn).max().unwrap_or(0);
        let present: BTreeSet<u64> = after.iter().map(|b| b.lsn).collect();
        Coverage::bump(&self.cov.durable_checks);
        if floor > m.checkpoint_lsn {
            Coverage::bump(&self.cov.durable_marks);
        }
        for b in before.iter().filter(|b| b.lsn <= floor) {
            Coverage::bump(&self.cov.durable_batches);
            if !present.contains(&b.lsn) {
                let have: Vec<u64> = present.iter().copied().collect();
                let logged_lsns: Vec<u64> = before.iter().map(|b| b.lsn).collect();
                return Err(self.fail(format!(
                    "batch {} was promised durable (watermark {floor}) but is missing after \
                     the crash\n  logged lsns:    {logged_lsns:?}\n  recovered lsns: {have:?}",
                    b.lsn,
                )));
            }
        }
        Ok(())
    }

    /// The configuration of a recovery: `Fail` unless media damage was applied and not yet seen
    /// by an open (power cuts and `kill -9` must never look like damage).
    fn recovery_cfg(&self) -> EngineConfig {
        let mut cfg = self.cfg.clone();
        if self.damage_pending {
            cfg.wal_damage_policy = WalDamagePolicy::Truncate;
        }
        cfg
    }

    fn open(&mut self, what: &str) -> Result<Eng, String> {
        let e = open_engine(&self.fs, &self.recovery_cfg())
            .map_err(|e| self.fail(format!("reopen failed ({what}): {e}")))?;
        let report = e.last_wal_recovery();
        if self.damage_pending {
            // The first open of a damaged directory reports what the copies reported (unless a
            // recovery that was cut short may already have consumed the evidence).
            if let Some(want) = self.expect_damage.take() {
                match want {
                    ExpectDamage::Exact(expected) if report.damage != expected => {
                        return Err(self.fail(format!(
                            "recovery ({what}) of the damaged logs reports {:?}, the copies \
                             {expected:?}",
                            report.damage
                        )));
                    }
                    ExpectDamage::FastPresence(present) if report.damage.is_some() != present => {
                        return Err(self.fail(format!(
                            "recovery ({what}) of the damaged logs reports {:?}, fast scan expects \
                             damage={present}",
                            report.damage
                        )));
                    }
                    _ => {}
                }
            }
            self.damage_pending = false;
        } else if let Some(d) = &report.damage {
            // Power cuts and `kill -9` lose only an unsynced tail, which is not damage: the
            // report of every recovery must be clean, however many batches it had to discard.
            return Err(self.fail(format!(
                "recovery ({what}) reports WAL damage after a crash that cannot damage synced \
                 data: {d:?}"
            )));
        }
        Coverage::bump(&self.cov.no_damage_checks);
        if self.fs.st().events.is_some() {
            let st = e.meta_mgr.state();
            self.log(format!(
                "opened ({what}): generation {} idx={:?} std={:?} lru={:?}",
                st.generation,
                st.idx_segments.iter().map(|x| x.seq_id).collect::<Vec<_>>(),
                st.std_blob_segments
                    .iter()
                    .map(|x| x.seq_id)
                    .collect::<Vec<_>>(),
                st.lru_blob_segments
                    .iter()
                    .map(|x| x.seq_id)
                    .collect::<Vec<_>>(),
            ));
        }
        Ok(e)
    }

    /// Registers the capped tables (idempotent: a registration lost to a crash is redone).
    fn ensure_tables(&mut self, e: &mut Eng) -> Result<bool, String> {
        if self.mode.media {
            // Salvage names the tables through the catalog.
            for t in TABLES {
                let name = format!("t{t}");
                let r = e.register_or_resolve_vtab_with_durability(&name, Some(t), SYNC);
                if self.io_ok(r, "register_or_resolve_vtab")?.is_none() {
                    return Ok(false);
                }
            }
        }
        if self.mode.group {
            return Ok(true);
        }
        let mut want = vec![(3u16, self.quota3), (4u16, self.quota4)];
        if self.wide > 0 {
            for i in 0..self.wide {
                want.push((WIDE_BASE + i, 1 << 20));
            }
        }
        for (t, quota) in want {
            if e.capped_table_state(t).is_none() {
                let r = e.register_capped_table(t, quota);
                if self.io_ok(r, "register_capped_table")?.is_none() {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    fn run_segment(&mut self, e: &mut Eng, nops: usize) -> Result<(), String> {
        if self.mode.group {
            return self.run_group_segment(e, nops);
        }
        if !self.ensure_tables(e)? {
            return Ok(());
        }
        for _ in 0..nops {
            let r = self.rng.below(100);
            let go = if self.backfill_heavy && (46..66).contains(&r) {
                self.op_backfill(e)?
            } else if r < 36 {
                self.op_put(e)?
            } else if r < 46 {
                self.op_delete(e)?
            } else if r < 76 {
                self.op_tx(e)?
            } else if r < 83 {
                self.op_maintenance(e, 0)?
            } else if r < 89 {
                self.op_maintenance(e, 1)?
            } else if r < 94 {
                self.op_maintenance(e, 2)?
            } else if r < 97 && self.wide_dropped < self.wide {
                self.op_drop_wide(e)?
            } else {
                self.op_backfill(e)?
            };
            if !go {
                return Ok(());
            }
            if self.rng.chance(1, 3) {
                self.live_check(e)?;
            }
        }
        self.live_check(e)
    }

    fn op_put(&mut self, e: &mut Eng) -> Result<bool, String> {
        let t = self.pick_table();
        let k = self.rng.below(KEYS as u64) as u8;
        let d = pick_durability(&mut self.rng);
        let sync = d == WriteDurability::SyncTransactional;
        let v = make_value(&mut self.rng, t, k);
        let mut ns = self.last().clone();
        ns.insert((t, k), v.clone());
        let what = format!("put t{t}/key{k:02} {} {}", short(&v), describe(d));
        let lsn = e.next_lsn();
        let r = e.put_row(t, &key_bytes(k), &v, &put_keys(t, &v), d);
        let logged = e.next_lsn() != lsn;
        self.note_op(lsn, e.next_lsn(), Touched::from([((t, k), vec![Some(v)])]));
        let alive = !e.is_poisoned();
        self.record_commit(r, ns, sync, logged, alive, &what)
    }

    /// Backfills the keys of up to three consecutive present rows of table 1, one commit each
    /// (a crash between them leaves some rows complete and the rest without entries).
    fn op_backfill(&mut self, e: &mut Eng) -> Result<bool, String> {
        let start = self.rng.below(KEYS as u64) as u8;
        for i in 0..3 {
            let k = (start + i) % KEYS;
            let Some(v) = self.last().get(&(1, k)).cloned() else {
                continue;
            };
            if !self.backfill_row(e, k, &v)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn backfill_row(&mut self, e: &mut Eng, k: u8, v: &[u8]) -> Result<bool, String> {
        let d = pick_durability(&mut self.rng);
        let ns = self.last().clone();
        let what = format!("backfill t1/key{k:02} {}", describe(d));
        let lsn = e.next_lsn();
        let r = e
            .backfill_secondary_keys(1, &key_bytes(k), &full_keys(1, v), d)
            .map(|_| ());
        let logged = e.next_lsn() != lsn;
        let alive = !e.is_poisoned();
        self.record_commit(
            r,
            ns,
            d == WriteDurability::SyncTransactional,
            logged,
            alive,
            &what,
        )
    }

    /// Finishes the migration of table 1 after recovery: every present row must end up complete.
    fn complete_backfill(&mut self, e: &mut Eng) -> Result<(), String> {
        for k in 0..KEYS {
            let Some(v) = self.last().get(&(1, k)).cloned() else {
                continue;
            };
            if !self.backfill_row(e, k, &v)? {
                return Err(self.fail("backfill stopped after recovery".into()));
            }
        }
        let got = self
            .read_all(e)?
            .ok_or_else(|| self.fail("read failed after recovery".into()))?;
        self.index_check(e, &got, true)
    }

    fn op_delete(&mut self, e: &mut Eng) -> Result<bool, String> {
        let t = self.pick_table();
        let k = self.rng.below(KEYS as u64) as u8;
        let d = pick_durability(&mut self.rng);
        let mut ns = self.last().clone();
        ns.remove(&(t, k));
        let what = format!("delete t{t}/key{k:02} {}", describe(d));
        let lsn = e.next_lsn();
        let r = e.delete_row(t, &key_bytes(k), d).map(|_| ());
        let logged = e.next_lsn() != lsn;
        self.note_op(lsn, e.next_lsn(), Touched::from([((t, k), vec![None])]));
        let alive = !e.is_poisoned();
        self.record_commit(
            r,
            ns,
            d == WriteDurability::SyncTransactional,
            logged,
            alive,
            &what,
        )
    }

    /// Drops one of the extra tables inside a transaction that also flushes, which shrinks the
    /// catalog without a metadata commit of its own (a later, shorter manifest).
    fn op_drop_wide(&mut self, e: &mut Eng) -> Result<bool, String> {
        let table = WIDE_BASE + self.wide_dropped;
        self.wide_dropped += 1;
        e.begin_transaction();
        let r = e.drop_table(table, None);
        self.log(format!("drop wide table {table}"));
        if self.io_ok(r, "drop_table")?.is_none() {
            return self.abort_tx(e);
        }
        let r = e.flush_memtable();
        if self.io_ok(r, "flush_memtable in transaction")?.is_none() {
            return self.abort_tx(e);
        }
        let r = e.commit_transaction();
        Ok(self.io_ok(r, "commit of drop_table")?.is_some() || !self.dead(e))
    }

    fn op_maintenance(&mut self, e: &mut Eng, which: u8) -> Result<bool, String> {
        let (name, r) = match which {
            0 => ("flush_memtable", e.flush_memtable()),
            1 => ("checkpoint", e.checkpoint()),
            _ => ("compact", e.compact()),
        };
        self.log(format!(
            "{name} -> {}",
            if r.is_ok() { "Ok" } else { "Err" }
        ));
        Ok(self.io_ok(r, name)?.is_some() || !self.dead(e))
    }

    fn op_tx(&mut self, e: &mut Eng) -> Result<bool, String> {
        let lsn0 = e.next_lsn();
        e.begin_transaction();
        self.log("begin".into());
        let mut work = self.last().clone();
        let mut touched = Touched::new();
        let mut sps: Vec<(i32, Table, Touched)> = Vec::new();
        let mut next_sp = 1;
        let steps = 1 + self.rng.below(7);
        for _ in 0..steps {
            let r = self.rng.below(100);
            if r < 60 {
                let t = self.pick_table();
                let k = self.rng.below(KEYS as u64) as u8;
                let v = make_value(&mut self.rng, t, k);
                let d = pick_durability(&mut self.rng);
                let res = e.put_row(t, &key_bytes(k), &v, &put_keys(t, &v), d);
                self.log(format!(
                    "  tx put t{t}/key{k:02} {} {}",
                    short(&v),
                    describe(d)
                ));
                if self.io_ok(res, "put_row in transaction")?.is_none() {
                    return self.abort_tx(e);
                }
                work.insert((t, k), v.clone());
                touched.entry((t, k)).or_default().push(Some(v));
            } else if r < 70 {
                let t = self.pick_table();
                let k = self.rng.below(KEYS as u64) as u8;
                let d = pick_durability(&mut self.rng);
                let res = e.delete_row(t, &key_bytes(k), d);
                self.log(format!("  tx delete t{t}/key{k:02}"));
                if self.io_ok(res, "delete_row in transaction")?.is_none() {
                    return self.abort_tx(e);
                }
                work.remove(&(t, k));
                touched.entry((t, k)).or_default().push(None);
            } else if r < 76 {
                // Backfill inside the transaction: rolled back with it, otherwise committed.
                let k = self.rng.below(KEYS as u64) as u8;
                if let Some(v) = work.get(&(1, k)).cloned() {
                    let d = pick_durability(&mut self.rng);
                    let res = e.backfill_secondary_keys(1, &key_bytes(k), &full_keys(1, &v), d);
                    self.log(format!("  tx backfill t1/key{k:02} {}", describe(d)));
                    if self
                        .io_ok(res, "backfill_secondary_keys in transaction")?
                        .is_none()
                    {
                        return self.abort_tx(e);
                    }
                }
            } else if r < 84 {
                e.savepoint(next_sp);
                self.log(format!("  savepoint {next_sp}"));
                sps.push((next_sp, work.clone(), touched.clone()));
                next_sp += 1;
            } else if r < 92 {
                if let Some(i) = sps
                    .len()
                    .checked_sub(1)
                    .map(|n| self.rng.below(n as u64 + 1) as usize)
                {
                    let (id, snap, snap_touched) = sps[i].clone();
                    let res = e.rollback_to_savepoint(id);
                    self.log(format!("  rollback to savepoint {id}"));
                    if self.io_ok(res, "rollback_to_savepoint")?.is_none() {
                        return self.abort_tx(e);
                    }
                    work = snap;
                    touched = snap_touched;
                    sps.truncate(i + 1);
                }
            } else if let Some(i) = sps
                .len()
                .checked_sub(1)
                .map(|n| self.rng.below(n as u64 + 1) as usize)
            {
                let id = sps[i].0;
                let res = e.release_savepoint(id);
                self.log(format!("  release savepoint {id}"));
                if self.io_ok(res, "release_savepoint")?.is_none() {
                    return self.abort_tx(e);
                }
                sps.truncate(i);
            }
            // The transaction sees its own writes.
            if self.rng.chance(1, 4) {
                let t = TABLES[self.rng.below(2) as usize];
                let k = self.rng.below(KEYS as u64) as u8;
                let got = e.get_row(t, &key_bytes(k));
                match got {
                    Ok(g) => {
                        if g.as_ref() != work.get(&(t, k)) {
                            return Err(self.fail(format!(
                                "transaction does not see its own write at t{t}/key{k:02}: \
                                 model {} / engine {}",
                                work.get(&(t, k)).map_or("absent".into(), |v| short(v)),
                                g.map_or("absent".into(), |v| short(&v)),
                            )));
                        }
                    }
                    Err(err) => {
                        if !self.fs.fault_happened() {
                            return Err(self.fail(format!("read in transaction failed: {err}")));
                        }
                        return Ok(false);
                    }
                }
            }
        }
        if self.rng.chance(1, 4) {
            let res = e.rollback_transaction();
            self.log("rollback".into());
            return Ok(self.io_ok(res, "rollback_transaction")?.is_some());
        }
        let d = pick_durability(&mut self.rng);
        self.log(format!("commit {}", describe(d)));
        if let Err(err) = e.prepare_commit_transaction(d) {
            if !self.fs.fault_happened() {
                return Err(self.fail(format!("prepare_commit_transaction failed: {err}")));
            }
            let dead = self.dead(e);
            let _ = e.rollback_transaction();
            if dead {
                // Poisoned: keep the candidate, the outcome is unknown.
                self.states.push(work);
            }
            return Ok(!dead);
        }
        let logged = e.pending_tx_record_count > 0;
        let before_commit = e.next_lsn();
        let r = e.commit_transaction();
        self.note_op(lsn0, e.next_lsn().max(before_commit), touched);
        let alive = !e.is_poisoned();
        self.record_commit(
            r,
            work,
            d == WriteDurability::SyncTransactional,
            logged,
            alive,
            "tx commit",
        )
    }
}

// =============================================================================================
// One case = one seed
// =============================================================================================

/// Opens the engine, telling the filesystem that removals now happen as part of recovery.
fn open_engine(fs: &Arc<CrashFs>, cfg: &EngineConfig) -> io::Result<Eng> {
    StorageEngine::open(Arc::clone(fs), cfg.clone())
}

fn engine_config(rng: &mut Rng) -> EngineConfig {
    let mut c = EngineConfig::default();
    // Small logs so that rotation collides with commits all the time.
    c.sync_wal_size_bytes = (6 + rng.below(14)) * 4096;
    c.buf_wal_size_bytes = (6 + rng.below(14)) * 4096;
    c.memtable_capacity_bytes = 4096 * (1 + rng.below(8) as usize);
    c.max_blob_segment_bytes = 2000 + rng.below(6000);
    c.auto_compact_segment_threshold = 3 + rng.below(3) as usize;
    c.block_cache_bytes = 0;
    c.blob_cache_bytes = 0;
    // Power cuts and `kill -9` must never look like damage, so every recovery opens under `Fail`.
    c.wal_damage_policy = WalDamagePolicy::Fail;
    c
}

fn pick_crash(rng: &mut Rng) -> Crash {
    if rng.chance(1, 5) {
        return Crash::Process;
    }
    match rng.below(5) {
        0 => Crash::Power { keep: 100, lose: 0 },
        1 => Crash::Power { keep: 0, lose: 100 },
        2 => Crash::Power { keep: 50, lose: 25 },
        3 => Crash::Power { keep: 25, lose: 50 },
        _ => Crash::Power {
            keep: rng.below(60),
            lose: rng.below(40),
        },
    }
}

fn pick_fault(rng: &mut Rng, stat: Option<&EpochStat>, prefer_manifest: bool) -> FaultChoice {
    let Some(stat) = stat else {
        return FaultChoice::None;
    };
    let manifests = stat.kind_ops[FileKind::Manifest as usize];
    if prefer_manifest && manifests > 0 && rng.chance(1, 2) {
        return FaultChoice::Fail(FileKind::Manifest, rng.below(manifests));
    }
    match rng.below(100) {
        0..=14 => FaultChoice::None,
        15..=54 => FaultChoice::Cut(rng.below(stat.ops + 1)),
        _ => {
            let total: u64 = stat.kind_ops.iter().sum();
            if total == 0 {
                return FaultChoice::None;
            }
            let mut r = rng.below(total);
            for k in ALL_KINDS {
                let n = stat.kind_ops[k as usize];
                if r < n {
                    return FaultChoice::Fail(k, r);
                }
                r -= n;
            }
            FaultChoice::None
        }
    }
}

/// Runs one seed. `plan` holds the operation counts of the clean run of the same seed, from
/// which crash points and failures are drawn; without it the run is clean (it still ends with a
/// randomised power loss).
fn run_case(
    seed: u64,
    mode: Mode,
    plan: Option<&[EpochStat]>,
    cov: &Coverage,
) -> Result<Vec<EpochStat>, String> {
    run_case_inner(seed, mode, plan, cov)
}

fn run_case_inner(
    seed: u64,
    mode: Mode,
    plan: Option<&[EpochStat]>,
    cov: &Coverage,
) -> Result<Vec<EpochStat>, String> {
    let mut master = Rng::new(seed);
    let dir_mode = if master.chance(1, 2) {
        DirMode::Unix
    } else {
        DirMode::Windows
    };
    let fs = Arc::new(CrashFs::new(dir_mode, master.next()));
    let mut cfg = engine_config(&mut master);
    if mode.group && !master.chance(1, 4) {
        // Room for several commits before the MemTable has to flush, which ends the grouping
        // (a quarter of the seeds keep the small MemTable, so that the commit that waits on its
        // own flush is covered too).
        cfg.memtable_capacity_bytes = 4096 * (6 + master.below(10) as usize);
    }
    let epochs = 1 + master.below(3) as usize;
    let wide = if master.chance(1, 4) {
        WIDE_TABLES_MIN + master.below(26) as u16
    } else {
        0
    };
    // The engine does not group commits while a capped table exists.
    let wide = if mode.group { 0 } else { wide };
    let quota3 = 3200 + master.below(2500);
    let quota4 = 6000 + master.below(6000);
    let backfill_heavy = master.chance(1, 3);
    let mut h = Harness {
        seed,
        fs: Arc::clone(&fs),
        cfg,
        cov,
        rng: Rng::new(seed ^ 0x5EED),
        states: vec![Table::new()],
        acked_sync: 0,
        acked_any: 0,
        wide,
        wide_dropped: 0,
        quota3,
        quota4,
        backfill_heavy,
        trace: Vec::new(),
        trace_cap: if env_u64("AYDB_CRASH_SEED").is_some() {
            usize::MAX
        } else {
            64
        },
        mode,
        damage_pending: false,
        relaxed: false,
        expect_damage: None,
        oplog: Vec::new(),
        tickets: Vec::new(),
    };
    h.log(format!(
        "config: {dir_mode:?} {mode:?} epochs={epochs} wide={wide} sync_wal={} buf_wal={} memtable={} blob_seg={} compact_at={}",
        h.cfg.sync_wal_size_bytes,
        h.cfg.buf_wal_size_bytes,
        h.cfg.memtable_capacity_bytes,
        h.cfg.max_blob_segment_bytes,
        h.cfg.auto_compact_segment_threshold
    ));
    Coverage::bump(&cov.cases);
    let mut stats = Vec::new();
    let mut last_was_process = false;

    for ep in 0..epochs {
        let mut e = h.open(&format!("epoch {ep}"))?;
        h.verify(&mut e, last_was_process)?;
        h.oplog.clear();
        h.tickets.clear();
        let rot0 = e.sync_wal.active_epoch();
        let choice = pick_fault(
            &mut master,
            plan.and_then(|p| p.get(ep).or_else(|| p.last())),
            wide > 0,
        );
        match choice {
            FaultChoice::None => {}
            FaultChoice::Cut(n) => fs.arm(Some(n), None),
            FaultChoice::Fail(k, n) => fs.arm(None, Some((k, n))),
        }
        h.log(format!(
            "epoch {ep}: fault plan {}",
            match choice {
                FaultChoice::None => "none".to_string(),
                FaultChoice::Cut(n) => format!("power cut at op {n}"),
                FaultChoice::Fail(k, n) => format!("failure of {k:?} op #{n}"),
            }
        ));
        let nops = 25 + master.below(40) as usize;
        let seg = catch_unwind(AssertUnwindSafe(|| h.run_segment(&mut e, nops)));
        let seg = match seg {
            Ok(r) => r,
            Err(p) => Err(h.fail(format!("engine panicked: {}", panic_text(&p)))),
        };
        seg?;
        if e.is_poisoned() {
            Coverage::bump(&cov.poisoned);
        }
        if e.maintenance_failures() > 0 {
            Coverage::bump(&cov.maintenance_failures);
        }
        cov.wal_rotations
            .fetch_add(e.sync_wal.active_epoch() - rot0, Ordering::Relaxed);
        let (ops, kind_ops) = fs.op_stats();
        stats.push(EpochStat { ops, kind_ops });
        {
            let st = fs.st();
            if st.cut {
                Coverage::bump(&cov.cuts_tripped);
            }
            if st.fail_fired {
                Coverage::bump(&cov.failures_fired);
                if let FaultChoice::Fail(k, _) = choice {
                    Coverage::bump(&cov.fail_by_kind[k as usize]);
                }
            }
            cov.manifests_seen.fetch_add(
                st.inodes
                    .iter()
                    .filter(|i| i.name.starts_with("manifest_"))
                    .count() as u64,
                Ordering::Relaxed,
            );
            cov.blob_segments_seen.fetch_add(
                st.inodes
                    .iter()
                    .filter(|i| i.name.starts_with("blob_"))
                    .count() as u64,
                Ordering::Relaxed,
            );
        }
        fs.cut_now();
        drop(e);
        let crash = pick_crash(&mut master);
        Coverage::bump(&cov.crashes);
        last_was_process = matches!(crash, Crash::Process);
        if last_was_process {
            Coverage::bump(&cov.process_crashes);
        }
        h.log(format!("crash: {crash:?}"));
        let logged = h.snapshot_wal();
        h.tickets.clear();
        fs.crash(crash);
        // What the power cut or the process crash left must still hold every acknowledged
        // batch, whatever happens to the files afterwards.
        h.check_durable_watermark(&logged)?;
        if mode.media {
            h.damage_wal(&mut master, &logged)?;
        }
        h.check_lsn_prefix(&logged)?;
        if fs.st().events.is_some() {
            h.log(format!(
                "after the crash:
{}",
                fs.wal_summary()
            ));
            h.log_wal_contents();
        }

        // A second crash during recovery.
        if master.chance(1, 3) {
            Coverage::bump(&cov.recovery_crashes);
            fs.arm(Some(master.below(80)), None);
            // A recovery that is cut short may or may not have consumed the damage.
            h.expect_damage = None;
            let r = open_engine(&fs, &h.recovery_cfg());
            if let Err(err) = &r {
                if !fs.fault_happened() {
                    return Err(h.fail(format!("recovery failed without any fault: {err}")));
                }
            }
            if fs.st().events.is_some() {
                if let Ok(mut eng) = r {
                    let tripped = fs.cut_tripped();
                    fs.st().cut = false;
                    let got = h.read_all(&mut eng)?;
                    fs.st().cut = tripped;
                    if let Some(got) = got {
                        let want = h.states[h.acked_any].clone();
                        h.log(format!(
                            "state shown by the recovery that was cut (tripped={tripped}); vs state {}:
    {}",
                            h.acked_any,
                            diff(&want, &got).join("
    ")
                        ));
                    }
                    fs.cut_now();
                    drop(eng);
                } else {
                    fs.cut_now();
                }
            } else {
                fs.cut_now();
                drop(r);
            }
            let crash = pick_crash(&mut master);
            // Nothing was committed by the aborted recovery, so the process-crash claim only
            // carries over if this crash keeps the cache too.
            last_was_process = last_was_process && matches!(crash, Crash::Process);
            h.log(format!("crash during recovery: {crash:?}"));
            fs.crash(crash);
        }
    }

    // Final recovery: it must succeed, show a valid state, and leave a healthy engine.
    let mut e = h.open("final recovery")?;
    let k1 = h.verify(&mut e, last_was_process)?;
    let _ = k1;
    h.oplog.clear();
    h.tickets.clear();
    h.log("post-recovery maintenance".into());
    let r = catch_unwind(AssertUnwindSafe(|| -> Result<(), String> {
        h.run_segment(&mut e, 8)?;
        h.complete_backfill(&mut e)?;
        e.checkpoint()
            .map_err(|er| h.fail(format!("checkpoint after recovery failed: {er}")))?;
        e.compact()
            .map_err(|er| h.fail(format!("compact after recovery failed: {er}")))?;
        h.live_check(&mut e)
    }));
    match r {
        Ok(r) => r?,
        Err(p) => {
            return Err(h.fail(format!(
                "engine panicked after recovery: {}",
                panic_text(&p)
            )));
        }
    }
    // A successful checkpoint brings the persisted state up to date (docs section 4.4).
    h.acked_sync = h.states.len() - 1;
    h.acked_any = h.acked_sync;
    fs.cut_now();
    drop(e);
    fs.crash(pick_crash(&mut master));
    let mut e = h.open("reopen after checkpoint")?;
    let k2 = h.verify(&mut e, false)?;
    if k2 != h.states.len() - 1 {
        return Err(h.fail("state after checkpoint changed".into()));
    }
    // Crash again immediately: the recovered state must be stable.
    let before = h.read_all(&mut e)?.unwrap();
    fs.cut_now();
    drop(e);
    fs.crash(Crash::Power { keep: 0, lose: 100 });
    let mut e = h.open("second recovery")?;
    let after = h.read_all(&mut e)?.unwrap();
    if before != after {
        Coverage::bump(&cov.unstable_recovery);
        return Err(h.fail(format!(
            "recovered state changed at the next crash:\n    {}",
            diff(&before, &after).join("\n    ")
        )));
    }
    fs.cut_now();
    drop(e);
    Ok(stats)
}

fn panic_text(p: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = p.downcast_ref::<&str>() {
        s.to_string()
    } else if let Some(s) = p.downcast_ref::<String>() {
        s.clone()
    } else {
        "non-string panic".into()
    }
}

/// Runs a seed twice: clean (which also measures operation counts), then with faults drawn
/// from those counts.
fn run_seed(seed: u64, mode: Mode, cov: &Coverage) -> Result<(), String> {
    let stats = run_case(seed, mode, None, cov)?;
    run_case(seed, mode, Some(&stats), cov)?;
    Ok(())
}

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name).ok().and_then(|s| s.trim().parse().ok())
}

/// Runs `count` seeds from `start` on a few threads. Returns the failure messages.
fn run_seeds(start: u64, count: u64, mode: Mode, cov: &Coverage) -> Vec<String> {
    let next = AtomicU64::new(0);
    let failures = Mutex::new(Vec::new());
    let threads = std::thread::available_parallelism()
        .map_or(1, |n| n.get())
        .clamp(1, 4);
    std::thread::scope(|s| {
        for _ in 0..threads {
            s.spawn(|| {
                loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    if i >= count {
                        break;
                    }
                    let seed = start + i;
                    let res = catch_unwind(AssertUnwindSafe(|| run_seed(seed, mode, cov)));
                    let msg = match res {
                        Ok(Ok(())) => continue,
                        Ok(Err(m)) => m,
                        Err(p) => format!("seed {seed}: harness panicked: {}", panic_text(&p)),
                    };
                    failures.lock().unwrap().push(msg);
                }
            });
        }
    });
    let mut f = failures.into_inner().unwrap();
    f.sort();
    f
}

/// Default number of seeds of the regular suite (about ten seconds in a debug build).
const DEFAULT_SEEDS: u64 = 1000;
/// Default number of seeds of each of the media-damage and group-commit modes. A seed of these
/// modes costs more (copies of the damaged directory are opened three times).
const DEFAULT_MODE_SEEDS: u64 = 150;

/// Runs the seeds of one mode: `AYDB_CRASH_SEEDS` and `AYDB_CRASH_SEED_START` choose the range for
/// every mode, `AYDB_CRASH_SEED` replays one seed.
fn run_mode_test(label: &str, mode: Mode, default_count: u64) {
    let cov = Coverage::default();
    let (start, count) = match env_u64("AYDB_CRASH_SEED") {
        Some(one) => (one, 1),
        None => (
            env_u64("AYDB_CRASH_SEED_START").unwrap_or(0),
            env_u64("AYDB_CRASH_SEEDS").unwrap_or(default_count),
        ),
    };
    let t0 = std::time::Instant::now();
    let failures = run_seeds(start, count, mode, &cov);
    eprintln!(
        "{label}: seeds {start}..{} in {:.1}s, {} failure(s)
  coverage: {}",
        start + count,
        t0.elapsed().as_secs_f64(),
        failures.len(),
        cov.report()
    );
    if !failures.is_empty() {
        let shown: Vec<&String> = failures.iter().take(200).collect();
        panic!(
            "{} of {count} seeds failed (replay one with AYDB_CRASH_SEED=<seed>):
{}",
            failures.len(),
            shown.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(
                "
----
"
            )
        );
    }
}

#[test]
fn crash_consistency_seeds() {
    run_mode_test("crash-consistency", Mode::default(), DEFAULT_SEEDS);
}

/// Corrupts synced WAL data after the crash (ARCHITECTURE.md 4.6.11).
#[test]
fn crash_media_damage_seeds() {
    let mode = Mode {
        media: true,
        group: false,
    };
    run_mode_test("crash-media-damage", mode, DEFAULT_MODE_SEEDS);
}

/// Drives concurrent commits through the group commit under the power-cut harness.
#[test]
fn crash_group_commit_seeds() {
    let mode = Mode {
        media: false,
        group: true,
    };
    run_mode_test("crash-group-commit", mode, DEFAULT_MODE_SEEDS);
}

/// Group commit and media damage together.
#[test]
fn crash_group_commit_media_damage_seeds() {
    let mode = Mode {
        media: true,
        group: true,
    };
    run_mode_test("crash-group-commit-media-damage", mode, DEFAULT_MODE_SEEDS);
}

// =============================================================================================
// Tests of the simulator itself
// =============================================================================================

fn raw_file(fs: &CrashFs, name: &str) -> Arc<CrashFile> {
    fs.open_file(name, true, true, OpenHint::MutableBufferedLog)
        .unwrap()
}

fn content(fs: &CrashFs, name: &str) -> Option<Vec<u8>> {
    let f = fs
        .open_file(name, false, true, OpenHint::MutableBufferedLog)
        .ok()?;
    let mut buf = vec![0u8; f.file_len().unwrap() as usize];
    f.read_exact_at(&mut buf, 0).unwrap();
    Some(buf)
}

#[test]
fn sim_synced_data_survives_and_unsynced_data_is_decided_per_sector() {
    // Four sectors of 0xAA, synced; four of 0xBB, not synced.
    let mut full = 0;
    let mut none = 0;
    let mut partial = 0;
    for seed in 0..200 {
        let fs = CrashFs::new(DirMode::Unix, seed);
        let f = raw_file(&fs, "wal_buf.db");
        f.write_all_at(&[0xAA; 4 * SECTOR], 0).unwrap();
        f.sync_durable().unwrap();
        f.write_all_at(&[0xBB; 4 * SECTOR], 0).unwrap();
        drop(f);
        fs.crash(Crash::Power { keep: 30, lose: 30 });
        let c = content(&fs, "wal_buf.db").unwrap();
        assert_eq!(c.len(), 4 * SECTOR);
        let sectors: Vec<u8> = (0..4).map(|s| c[s * SECTOR]).collect();
        for s in 0..4 {
            assert!(
                c[s * SECTOR..(s + 1) * SECTOR]
                    .iter()
                    .all(|&b| b == sectors[s])
            );
            assert!(sectors[s] == 0xAA || sectors[s] == 0xBB);
        }
        match sectors.iter().filter(|&&b| b == 0xBB).count() {
            0 => none += 1,
            4 => full += 1,
            _ => partial += 1,
        }
    }
    assert!(
        full > 0 && none > 0 && partial > 0,
        "{full}/{none}/{partial}"
    );
}

#[test]
fn sim_process_crash_keeps_everything_and_power_loss_without_keep_drops_it() {
    let fs = CrashFs::new(DirMode::Windows, 1);
    let f = raw_file(&fs, "wal_buf.db");
    f.write_all_at(&[7; 1000], 0).unwrap();
    drop(f);
    fs.crash(Crash::Process);
    assert_eq!(content(&fs, "wal_buf.db").unwrap(), vec![7; 1000]);
    // A Windows-mode file that was never synced may vanish, but it is never half-present.
    let mut gone = 0;
    for seed in 0..40 {
        let fs = CrashFs::new(DirMode::Windows, seed);
        let f = raw_file(&fs, "blob_std_01.db");
        f.write_all_at(&[7; 1000], 0).unwrap();
        drop(f);
        fs.crash(Crash::Power { keep: 0, lose: 100 });
        match content(&fs, "blob_std_01.db") {
            None => gone += 1,
            Some(c) => assert!(c.is_empty() || c == vec![0; 1000]),
        }
    }
    assert!(gone > 0 && gone < 40);
}

#[test]
fn sim_directory_entries_follow_the_platform_mode() {
    // Unix: creation and removal are durable when the call returns.
    let fs = CrashFs::new(DirMode::Unix, 3);
    raw_file(&fs, "keep.db");
    raw_file(&fs, "gone.db");
    fs.remove_file("gone.db").unwrap();
    for _ in 0..20 {
        fs.crash(Crash::Power { keep: 0, lose: 100 });
        assert_eq!(fs.list_files().unwrap(), vec!["keep.db".to_string()]);
    }
    // Windows: a synced file keeps its entry, but its removal may be undone.
    let mut back = 0;
    let mut stayed = 0;
    for seed in 0..60 {
        let fs = CrashFs::new(DirMode::Windows, seed);
        let f = raw_file(&fs, "idx_01.db");
        f.write_all_at(&[1; 100], 0).unwrap();
        f.sync_durable().unwrap();
        drop(f);
        fs.remove_file("idx_01.db").unwrap();
        fs.crash(Crash::Power { keep: 0, lose: 100 });
        match content(&fs, "idx_01.db") {
            Some(c) => {
                assert_eq!(c, vec![1; 100]);
                back += 1;
            }
            None => stayed += 1,
        }
    }
    assert!(back > 0 && stayed > 0);
}

#[test]
fn sim_logical_preallocation_is_not_durable_until_a_sync() {
    let mut short = 0;
    let mut long = 0;
    for seed in 0..60 {
        let fs = CrashFs::new(DirMode::Unix, seed);
        let f = raw_file(&fs, "wal_buf.db");
        f.preallocate(8192, PreallocStrategy::LogicalExtendOnly)
            .unwrap();
        drop(f);
        fs.crash(Crash::Power { keep: 0, lose: 100 });
        match content(&fs, "wal_buf.db").unwrap().len() {
            0 => short += 1,
            8192 => long += 1,
            n => panic!("unexpected length {n}"),
        }
    }
    assert!(short > 0 && long > 0);
    // Physical zero fill ends with a sync.
    let fs = CrashFs::new(DirMode::Unix, 9);
    let f = raw_file(&fs, "wal_sync.db");
    f.preallocate(8192, PreallocStrategy::PhysicalZeroFill)
        .unwrap();
    drop(f);
    fs.crash(Crash::Power { keep: 0, lose: 100 });
    assert_eq!(content(&fs, "wal_sync.db").unwrap().len(), 8192);
}

#[test]
fn sim_failures_hit_the_chosen_operation_of_the_chosen_kind() {
    let fs = CrashFs::new(DirMode::Unix, 5);
    fs.arm(None, Some((FileKind::Blob, 1)));
    let wal = raw_file(&fs, "wal_sync.db");
    let blob = raw_file(&fs, "blob_std_01.db");
    // Blob operation #0 was the creation; #1 is this write.
    wal.write_all_at(&[1; 10], 0).unwrap();
    assert!(blob.write_all_at(&[2; 10], 0).is_err());
    assert!(fs.fault_happened());
    // One-shot: the next blob write works.
    blob.write_all_at(&[2; 10], 0).unwrap();
    // A power cut fails everything after it, including syncs.
    fs.arm(Some(fs.op_stats().0), None);
    assert!(wal.sync_durable().is_err());
    assert!(blob.write_all_at(&[3; 10], 0).is_err());
    assert!(fs.cut_tripped());
}

#[test]
fn sim_a_failed_sync_can_lose_its_writes_for_good() {
    let mut lost = 0;
    for seed in 0..60 {
        let fs = CrashFs::new(DirMode::Unix, seed);
        let f = raw_file(&fs, "blob_std_01.db");
        f.write_all_at(&[9; 100], 0).unwrap();
        fs.arm(None, Some((FileKind::Blob, 2)));
        if f.sync_durable().is_err() {
            // Whatever the effect, a second sync reports success.
            f.sync_durable().unwrap();
        }
        drop(f);
        fs.crash(Crash::Power { keep: 0, lose: 100 });
        let c = content(&fs, "blob_std_01.db").unwrap();
        if c != vec![9; 100] {
            lost += 1;
        }
    }
    // Effect::Lost (fsyncgate) shows as data missing after a "successful" retry.
    assert!(lost > 0);
}

#[test]
fn crash_runs_are_reproducible() {
    let a = Coverage::default();
    let b = Coverage::default();
    for seed in [3, 17, 91] {
        let s1 = run_case(seed, Mode::default(), None, &a).unwrap();
        let s2 = run_case(seed, Mode::default(), None, &b).unwrap();
        assert_eq!(s1.len(), s2.len());
        for (x, y) in s1.iter().zip(&s2) {
            assert_eq!(x.ops, y.ops, "seed {seed} is not deterministic");
            assert_eq!(x.kind_ops, y.kind_ops);
        }
        let s3 = run_case(seed, Mode::default(), Some(&s1), &a).unwrap();
        let s4 = run_case(seed, Mode::default(), Some(&s1), &b).unwrap();
        for (x, y) in s3.iter().zip(&s4) {
            assert_eq!(x.ops, y.ops, "seed {seed} is not deterministic with faults");
        }
    }
    assert_eq!(a.report(), b.report());
}

// =============================================================================================
// Minimal reproductions of engine bugs found by the seeded harness
// =============================================================================================

fn repro_config() -> EngineConfig {
    let mut c = EngineConfig::default();
    c.max_blob_segment_bytes = 2048;
    c.auto_compact_segment_threshold = 0;
    c.block_cache_bytes = 0;
    c.blob_cache_bytes = 0;
    c
}

fn row_is(e: &mut Eng, table: u16, key: &[u8], want: &[u8]) -> bool {
    e.get_row(table, key).unwrap().as_deref() == Some(want)
}

/// Regression test for a fixed bug. A fully dead standard blob segment is dropped from the
/// catalog and queued in `pending_delete_files`; its unlink fails (or the process stops before
/// it). At the next open the segment is still on disk, an older `idx` segment still holds a stale
/// pointer into it, and because another segment has a tail beyond its cataloged length, recovery
/// scans the `idx` segments. It used to count the stale pointer as live, catalog the segment
/// again and then unlink it in the same commit, so the next open failed with `NotFound` for
/// good. A file queued for deletion is now never adopted.
#[test]
fn pending_delete_of_dead_blob_segment_is_not_adopted() {
    let mut cfg = repro_config();
    cfg.max_blob_segment_bytes = 3200;
    let fs = Arc::new(CrashFs::new(DirMode::Unix, 1));
    let mut e = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
    let sync = WriteDurability::SyncTransactional;
    e.put_row(1, b"k", &[1u8; 1500], &[], sync).unwrap();
    e.put_row(1, b"g", &[2u8; 2000], &[], sync).unwrap();
    e.flush_memtable().unwrap();
    // The unlink of the dead segment fails: it stays in `pending_delete_files`.
    fs.st().fail_remove_prefix = Some("blob_std_");
    e.delete_row(1, b"k", sync).unwrap();
    e.flush_memtable().unwrap();
    assert_eq!(e.meta_mgr.state().pending_delete_files.len(), 1);
    // A buffered blob lands in the active segment, beyond its cataloged length.
    e.put_row(1, b"m", &[3u8; 1100], &[], WriteDurability::BufferedFast)
        .unwrap();
    fs.cut_now();
    drop(e);
    fs.crash(Crash::Process);
    fs.st().fail_remove_prefix = None;
    // The first reopen succeeds but catalogs the segment and unlinks it ...
    let first = StorageEngine::open(Arc::clone(&fs), cfg.clone());
    assert!(first.is_ok());
    drop(first);
    fs.crash(Crash::Process);
    // ... so the second one cannot find it.
    let second = StorageEngine::open(Arc::clone(&fs), cfg);
    assert!(second.is_ok(), "reopen failed: {:?}", second.err());
}

/// Regression test for a fixed bug. A transaction in `wal_buf.db` or `wal_sync.db` holds a row of
/// a capped table with an out-of-line value next to other rows. Later commits evict that row and
/// reclaim its LRU blob segment. The segment used to be unlinked at once, so at recovery the
/// batch no longer verified and was dropped with the other rows, although a later synchronous
/// commit made it durable. The file now stays until the WAL epoch has been rotated.
#[test]
fn reclaimed_lru_segment_keeps_batches_that_reference_it() {
    let cfg = repro_config();
    let fs = Arc::new(CrashFs::new(DirMode::Unix, 1));
    let mut e = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
    let sync = WriteDurability::SyncTransactional;
    let buf = WriteDurability::BufferedFast;
    e.register_capped_table(1, 3300).unwrap();
    e.begin_transaction();
    e.put_row(2, b"a", b"small", &[], buf).unwrap();
    e.put_row(1, b"x", &[1u8; 1500], &[], buf).unwrap();
    e.prepare_commit_transaction(buf).unwrap();
    e.commit_transaction().unwrap();
    // Two more rows evict x and reclaim its segment; the second commit is synchronous.
    e.put_row(1, b"y", &[2u8; 1500], &[], sync).unwrap();
    e.put_row(1, b"z", &[3u8; 1500], &[], sync).unwrap();
    assert!(e.get_row(1, b"x").unwrap().is_none());
    fs.cut_now();
    drop(e);
    fs.crash(Crash::Process);
    let mut e = StorageEngine::open(Arc::clone(&fs), cfg).unwrap();
    assert!(
        row_is(&mut e, 2, b"a", b"small"),
        "row a of the lost transaction"
    );
}

/// Regression test for a fixed bug. Reclaiming an LRU blob segment syncs `wal_buf.db` and records that as "buffered
/// commits are durable" without syncing the standard blob writer. A later synchronous commit
/// then skips the blob sync, so a buffered row with an out-of-line value is lost to a power cut
/// although the synchronous commit was acknowledged after it (prefix durability).
#[test]
fn lru_reclaim_syncs_blobs_before_marking_buffered_commits_durable() {
    let cfg = repro_config();
    let fs = Arc::new(CrashFs::new(DirMode::Unix, 1));
    let mut e = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
    let sync = WriteDurability::SyncTransactional;
    let buf = WriteDurability::BufferedFast;
    e.register_capped_table(1, 3300).unwrap();
    e.put_row(1, b"x", &[1u8; 1500], &[], sync).unwrap();
    e.put_row(1, b"y", &[2u8; 1500], &[], sync).unwrap();
    // A buffered blob row in a plain table, then a buffered write that evicts x.
    e.put_row(2, b"b", &[3u8; 1500], &[], buf).unwrap();
    e.put_row(1, b"z", &[4u8; 1500], &[], buf).unwrap();
    e.put_row(2, b"s", b"small", &[], sync).unwrap();
    fs.cut_now();
    drop(e);
    fs.crash(Crash::Power { keep: 0, lose: 100 });
    let mut e = StorageEngine::open(Arc::clone(&fs), cfg).unwrap();
    assert!(row_is(&mut e, 2, b"s", b"small"));
    assert!(row_is(&mut e, 2, b"b", &[3u8; 1500]), "buffered blob row b");
}

/// A reclaimed LRU segment leaves the catalog at once but is unlinked only after the WAL epoch
/// rotated, on a flush as well as on a checkpoint with an empty `MemTable`. The capped table's
/// byte accounting is unaffected by the lingering file, also across a crash and reopen.
#[test]
fn reclaimed_lru_segment_is_unlinked_after_wal_rotation() {
    let cfg = repro_config();
    let fs = Arc::new(CrashFs::new(DirMode::Unix, 1));
    let lru_files = |fs: &Arc<CrashFs>| {
        fs.list_files()
            .unwrap()
            .into_iter()
            .filter(|n| n.starts_with("blob_lru_"))
            .count()
    };
    let sync = WriteDurability::SyncTransactional;
    let mut e = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
    e.register_capped_table(1, 3300).unwrap();
    for (k, fill) in [(b"x", 1u8), (b"y", 2), (b"z", 3)] {
        e.put_row(1, k, &[fill; 1500], &[], sync).unwrap();
    }
    assert!(e.get_row(1, b"x").unwrap().is_none());
    let logical = e.capped_tables[&1].current_logical_bytes;
    assert!(logical <= 3300, "quota exceeded: {logical}");
    // x's segment left the catalog but its file lingers while the WAL may mention it.
    assert!(e.lru_blobs.keys().all(|&(_, seq)| {
        e.meta_mgr
            .state()
            .lru_blob_segments
            .iter()
            .any(|m| m.seq_id == seq)
    }));
    assert_eq!(e.unlink_after_wal_rotation.len(), 1);
    assert_eq!(lru_files(&fs), e.lru_blobs.len() + 1);
    // A flush rotates the WAL: the file is queued and unlinked.
    e.flush_memtable().unwrap();
    assert!(e.unlink_after_wal_rotation.is_empty());
    assert_eq!(lru_files(&fs), e.lru_blobs.len());
    assert!(e.meta_mgr.state().pending_delete_files.is_empty());
    assert_eq!(e.capped_tables[&1].current_logical_bytes, logical);

    // A crash while the file still lingers, then a checkpoint after the reopen.
    e.put_row(1, b"w", &[4u8; 1500], &[], sync).unwrap();
    assert!(e.get_row(1, b"y").unwrap().is_none());
    assert_eq!(e.unlink_after_wal_rotation.len(), 1);
    let logical = e.capped_tables[&1].current_logical_bytes;
    fs.cut_now();
    drop(e);
    fs.crash(Crash::Process);
    let mut e = StorageEngine::open(Arc::clone(&fs), cfg).unwrap();
    assert!(row_is(&mut e, 1, b"w", &[4u8; 1500]));
    assert!(row_is(&mut e, 1, b"z", &[3u8; 1500]));
    assert!(e.get_row(1, b"y").unwrap().is_none());
    assert_eq!(e.capped_tables[&1].current_logical_bytes, logical);
    e.checkpoint().unwrap();
    assert!(e.unlink_after_wal_rotation.is_empty());
    assert_eq!(lru_files(&fs), e.lru_blobs.len());
}

/// Regression test for [H7]: if a `blob_lru` segment accumulated pre-checkpoint dead bytes and
/// was then reclaimed (moving into `unlink_after_wal_rotation`, outside `boot_meta.db`), a crash
/// before WAL rotation must not re-adopt it as a permanent zombie by scanning stale pointers in
/// older `idx` segments.
#[test]
fn reclaimed_lru_segment_with_pre_checkpoint_dead_bytes_is_swept_after_crash() {
    let mut cfg = repro_config();
    cfg.max_blob_segment_bytes = 3500;
    let fs = Arc::new(CrashFs::new(DirMode::Unix, 1));
    let lru_files = |fs: &Arc<CrashFs>| -> Vec<String> {
        fs.list_files()
            .unwrap()
            .into_iter()
            .filter(|n| n.starts_with("blob_lru_"))
            .collect()
    };
    let sync = WriteDurability::SyncTransactional;
    let mut e = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
    e.register_capped_table(1, 4000).unwrap();

    // Two 1500-byte values share a single `blob_lru` segment.
    e.put_row(1, b"y1", &[1u8; 1500], &[], sync).unwrap();
    e.put_row(1, b"y2", &[2u8; 1500], &[], sync).unwrap();
    let files_after_y = lru_files(&fs);
    assert_eq!(files_after_y.len(), 1);
    let reclaimed_name = files_after_y[0].clone();

    // Persist pre-checkpoint dead bytes for `y1` while `y2` remains in `idx`.
    e.delete_row(1, b"y1", sync).unwrap();
    e.flush_memtable().unwrap();

    // Inserting `z1` rolls over to a new LRU segment; `z2` exceeds the 4000-byte cap and evicts
    // `y2`, making the first segment 100% dead and placing it into `unlink_after_wal_rotation`.
    e.put_row(1, b"z1", &[3u8; 1500], &[], sync).unwrap();
    e.put_row(1, b"z2", &[4u8; 1500], &[], sync).unwrap();
    assert!(e.get_row(1, b"y1").unwrap().is_none());
    assert!(e.get_row(1, b"y2").unwrap().is_none());
    assert_eq!(e.unlink_after_wal_rotation, vec![reclaimed_name.clone()]);
    assert!(lru_files(&fs).contains(&reclaimed_name));

    // Crash BEFORE WAL rotation so `reclaimed_name` lingers on disk while uncataloged.
    fs.cut_now();
    drop(e);
    fs.crash(Crash::Process);

    let mut e = StorageEngine::open(Arc::clone(&fs), cfg).unwrap();
    assert!(row_is(&mut e, 1, b"z1", &[3u8; 1500]));
    assert!(row_is(&mut e, 1, b"z2", &[4u8; 1500]));
    assert!(e.get_row(1, b"y1").unwrap().is_none());
    assert!(e.get_row(1, b"y2").unwrap().is_none());
    assert!(
        !lru_files(&fs).contains(&reclaimed_name),
        "reclaimed LRU segment {reclaimed_name} leaked as a zombie after crash"
    );
    assert_eq!(lru_files(&fs).len(), e.lru_blobs.len());
    assert!(
        !e.meta_mgr.state().lru_blob_segments.iter().any(
            |m| crate::meta::format_lru_blob_file_name(m.table_id, m.seq_id) == reclaimed_name
        )
    );
}

/// Regression test for [H8]: when table 2 rolls over its LRU blob segment during buffered
/// writes, `boot_meta.db` must only record the durable (`synced_offset`) length of table 1's
/// active LRU writer, never an unsynced tail.
#[test]
fn lru_rollover_on_one_table_does_not_persist_unsynced_len_of_another_table() {
    let mut cfg = repro_config();
    cfg.max_blob_segment_bytes = 2500;
    let fs = Arc::new(CrashFs::new(DirMode::Unix, 1));
    let mut e = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
    let buf = WriteDurability::BufferedFast;
    e.register_capped_table(1, 50_000).unwrap();
    e.register_capped_table(2, 50_000).unwrap();

    // Table 2 opens segment 2a, table 1 opens segment 1a and buffers an unsynced blob.
    e.put_row(2, b"t2_a", &[2u8; 1500], &[], buf).unwrap();
    e.put_row(1, b"t1_a", &[1u8; 1500], &[], buf).unwrap();
    // Second buffered write to table 2 forces table 2's segment to roll over and commits
    // `lru_blob_segments` to `boot_meta.db`.
    e.put_row(2, b"t2_b", &[3u8; 1500], &[], buf).unwrap();

    let t1_meta = e
        .meta_mgr
        .state()
        .lru_blob_segments
        .iter()
        .find(|m| m.table_id == 1)
        .expect("table 1 segment must be cataloged");
    assert_eq!(
        t1_meta.file_len, 0,
        "unsynced buffered bytes on table 1 leaked into boot_meta.db on table 2 rollover"
    );

    // After a power cut that drops unsynced writes, reopening must see file_len == 0 (no phantom length).
    fs.cut_now();
    drop(e);
    fs.crash(Crash::Power { keep: 0, lose: 100 });
    let e = StorageEngine::open(Arc::clone(&fs), cfg).unwrap();
    for m in e
        .meta_mgr
        .state()
        .lru_blob_segments
        .iter()
        .filter(|m| m.table_id == 1)
    {
        assert_eq!(m.file_len, 0);
    }
}

/// Regression test for [H8]: a pre-slot `commit_update` failure (when overflowing to an external
/// manifest) during blob rollover or dead-segment reclaim must neither leave 0-byte zombie entries
/// / leaked files behind nor drop still-cataloged segments from in-memory maps.
#[test]
fn pre_slot_meta_failure_on_blob_rollover_and_reclaim_keeps_maps_consistent() {
    let mut cfg = repro_config();
    cfg.max_blob_segment_bytes = 2500;
    let fs = Arc::new(CrashFs::new(DirMode::Unix, 1));
    let mut e = StorageEngine::open(Arc::clone(&fs), cfg).unwrap();
    let sync = WriteDurability::SyncTransactional;

    e.register_capped_table(7, 100_000).unwrap();

    // Force every subsequent `commit_update` to write an external `manifest_*.db` (> 4028B).
    e.meta_mgr
        .commit_update(|st| {
            for tid in 100..360u16 {
                st.table_quotas.push(crate::meta::TableQuotaMeta {
                    table_id: tid,
                    eviction_policy: crate::meta::EvictionPolicy::None,
                    max_bytes: 0,
                    current_logical_bytes: 0,
                });
            }
        })
        .unwrap();
    assert!(e.meta_mgr.state().active_manifest_seq_id.is_some());

    let arm_manifest_fail = |fs: &Arc<CrashFs>| {
        let mut st = fs.st();
        st.fail_fired = false;
        st.cut = false;
        let next = st.kind_ops[FileKind::Manifest as usize];
        st.fail_at = Some((FileKind::Manifest, next));
    };
    let blob_files = |fs: &Arc<CrashFs>, prefix: &str| -> usize {
        fs.list_files()
            .unwrap()
            .into_iter()
            .filter(|n| n.starts_with(prefix))
            .count()
    };

    // 1. Standard blob rollover pre-slot failure: no zombie entry and no leaked file.
    arm_manifest_fail(&fs);
    assert!(e.put_row(1, b"s1", &[0x11u8; 1500], &[], sync).is_err());
    assert!(!e.is_poisoned());
    assert!(e.std_blobs.is_empty());
    assert_eq!(blob_files(&fs, "blob_std_"), 0);

    // Retry succeeds; seal `s1`'s segment with `s2`, then delete `s1` so `seg1` is 100% dead.
    fs.arm(None, None);
    e.put_row(1, b"s1", &[0x11u8; 1500], &[], sync).unwrap();
    let seg1_seq = *e.std_blobs.keys().next().unwrap();
    e.put_row(1, b"s2", &[0x22u8; 1500], &[], sync).unwrap();
    e.delete_row(1, b"s1", sync).unwrap();

    // 2. reclaim_fully_dead_blob_segments pre-slot failure keeps `seg1` in `std_blobs`.
    arm_manifest_fail(&fs);
    assert!(e.reclaim_fully_dead_blob_segments().is_err());
    assert!(!e.is_poisoned());
    assert!(e.std_blobs.contains_key(&seg1_seq));

    fs.arm(None, None);
    e.reclaim_fully_dead_blob_segments().unwrap();
    assert!(!e.std_blobs.contains_key(&seg1_seq));

    // 3. LRU blob rollover & reclaim_fully_dead_lru_blob_segments pre-slot failures.
    e.put_row(7, b"l1", &[0x33u8; 1500], &[], sync).unwrap();
    let lru_seg1_seq = e.lru_blobs.keys().find(|&&(tid, _)| tid == 7).unwrap().1;

    arm_manifest_fail(&fs);
    assert!(e.put_row(7, b"l2", &[0x44u8; 1500], &[], sync).is_err());
    assert!(!e.is_poisoned());
    assert_eq!(e.active_lru_blob_segment_count(7), 1);
    assert_eq!(blob_files(&fs, "blob_lru_"), 1);

    fs.arm(None, None);
    e.put_row(7, b"l2", &[0x44u8; 1500], &[], sync).unwrap();
    assert_eq!(e.active_lru_blob_segment_count(7), 2);
    e.delete_row(7, b"l1", sync).unwrap();

    arm_manifest_fail(&fs);
    assert!(e.reclaim_fully_dead_lru_blob_segments(7).is_err());
    assert!(!e.is_poisoned());
    assert!(e.lru_blobs.contains_key(&(7, lru_seg1_seq)));
    assert!(e.unlink_after_wal_rotation.is_empty());

    fs.arm(None, None);
    e.reclaim_fully_dead_lru_blob_segments(7).unwrap();
    assert!(!e.lru_blobs.contains_key(&(7, lru_seg1_seq)));
    assert_eq!(e.unlink_after_wal_rotation.len(), 1);
}

/// A table's catalog entry and its rowid mapping marker are one atomic write. A power cut at
/// every operation of the registration must leave either no table or a table with its marker,
/// and an acknowledged registration is never lost.
#[test]
fn power_cut_during_table_registration_never_leaves_a_table_without_its_marker() {
    let sync = WriteDurability::SyncTransactional;
    let cfg = repro_config();

    // Returns the number of operations the registration issued when it ran to completion.
    let scenario = |seed: u64, cut: Option<u64>, keep: u64, lose: u64| -> u64 {
        let fs = Arc::new(CrashFs::new(DirMode::Unix, seed));
        let mut e = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
        let start = fs.op_stats().0;
        fs.arm(cut.map(|c| start + c), None);
        let acknowledged = e
            .register_or_resolve_vtab_with_durability("t", Some(1), sync)
            .is_ok();
        let span = fs.op_stats().0 - start;
        fs.cut_now();
        drop(e);
        fs.crash(Crash::Power { keep, lose });

        let mut e = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
        let persisted = e.plan_vtab_registration("t", Some(1)).unwrap().persisted;
        assert!(
            persisted || !acknowledged,
            "acknowledged registration lost (seed {seed}, cut {cut:?})"
        );
        assert_eq!(
            e.rowids_initialized(1).unwrap(),
            persisted,
            "catalog entry and marker disagree (seed {seed}, cut {cut:?})"
        );
        span
    };

    for (seed, keep, lose) in [(1, 0, 100), (2, 50, 50), (4, 20, 40)] {
        let span = scenario(seed, None, keep, lose);
        assert!(span > 0);
        for cut in 0..=span {
            scenario(seed, Some(cut), keep, lose);
        }
    }
}

/// A power cut at every operation of the UNIQUE index migration: whatever survives, a marker
/// never outlives an entry, an acknowledged migration is not lost, and resuming gives an index
/// that matches the rows exactly.
#[test]
fn power_cut_during_unique_index_migration_resumes_to_an_exact_index() {
    use crate::vtab::unique_key::UniqueDef;
    use crate::vtab::unique_migration::{Outcome, Params, migrate};
    use crate::vtab::{ColCollation, ColumnAffinity, SqlValue, encode_row};

    const ROWS: usize = 12;
    let sync = WriteDurability::SyncTransactional;
    let def = UniqueDef::new(vec![(1, ColCollation::Binary, ColumnAffinity::Text)]);
    let cfg = repro_config();
    let pk_of = |i: usize| SqlValue::Text(format!("key{i:03}")).to_key_bytes();
    // Values repeat (legacy duplicates are indexed for every row).
    let component_of = |i: usize| SqlValue::Text(format!("a{}", i % 7)).to_key_bytes();

    let exact = |e: &mut Eng, what: &str| {
        let mut want_i = std::collections::BTreeSet::new();
        let mut want_t = std::collections::BTreeSet::new();
        for i in 0..ROWS {
            want_i.insert(encode_secondary_index_key(
                1,
                0x8001,
                &component_of(i),
                &pk_of(i),
            ));
            want_t.insert(encode_row_meta_key(1, &pk_of(i)));
            let meta = e.test_row_meta(1, &pk_of(i)).unwrap().expect(what);
            assert_eq!(
                meta.secondary_keys,
                vec![(0x8001, component_of(i))],
                "{what}"
            );
        }
        let got_i: std::collections::BTreeSet<_> =
            e.test_live_entry_keys(1).unwrap().into_iter().collect();
        let got_t: std::collections::BTreeSet<_> = e
            .test_live_keys(1, NS_ROW_META)
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(got_i, want_i, "{what}: entries");
        assert_eq!(got_t, want_t, "{what}: 't' records");
    };

    // Returns the number of operations the migration issued when it ran to completion.
    let scenario = |seed: u64, cut: Option<u64>, keep: u64, lose: u64| -> u64 {
        let fs = Arc::new(CrashFs::new(DirMode::Unix, seed));
        let mut e = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
        for i in 0..ROWS {
            let row = encode_row(&[
                SqlValue::Text(format!("key{i:03}")),
                SqlValue::Text(format!("a{}", i % 7)),
            ]);
            e.put_row(1, &pk_of(i), &row, &[], sync).unwrap();
        }
        let start = fs.op_stats().0;
        fs.arm(cut.map(|c| start + c), None);
        let params = Params {
            table_id: 1,
            durability: sync,
            def: &def,
            chunk_rows: 4,
            budget_rows: usize::MAX,
        };
        let lock = std::sync::RwLock::new(e);
        let acknowledged = matches!(migrate(&lock, &params, &mut None), Outcome::Complete);
        let span = fs.op_stats().0 - start;
        fs.cut_now();
        drop(lock);
        fs.crash(Crash::Power { keep, lose });

        let mut e = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
        let marked = e.unique_marker_view(ReadView::Owner, 1).unwrap().is_some();
        assert!(
            marked || !acknowledged,
            "acknowledged migration lost (seed {seed}, cut {cut:?})"
        );
        if marked {
            exact(
                &mut e,
                &format!("marker without its entries (seed {seed}, cut {cut:?})"),
            );
        }
        // Resuming from nothing completes and leaves an exact index.
        let lock = std::sync::RwLock::new(e);
        let outcome = migrate(&lock, &params, &mut None);
        assert!(matches!(outcome, Outcome::Complete), "{outcome:?}");
        let mut e = lock.into_inner().unwrap();
        exact(&mut e, &format!("after resume (seed {seed}, cut {cut:?})"));
        span
    };

    for (seed, keep, lose) in [(1, 0, 100), (2, 50, 50), (4, 20, 40)] {
        let span = scenario(seed, None, keep, lose);
        assert!(span > 0);
        for cut in 0..=span {
            scenario(seed, Some(cut), keep, lose);
        }
    }
}

// =============================================================================================
// WAL damage: recovery must replay a prefix of the global LSN order (ARCHITECTURE.md 4.6)
// =============================================================================================
//
// These tests damage data that was already synced (a bad sector, a rotten blob), which neither a
// power cut nor `kill -9` can do: the engine orders its flushes so that those only ever lose an
// unsynced tail. The recovered state must still be the batches below the first missing or
// unverifiable one, in global LSN order across both streams.

/// Applies `f` to both images of a file, so that the damage survives either kind of crash.
fn damage_synced(fs: &CrashFs, name: &str, f: impl Fn(&mut Vec<u8>)) {
    let mut st = fs.st();
    let id = st.dir[name];
    let ino = &mut st.inodes[id];
    f(&mut ino.cache);
    f(&mut ino.durable);
    ino.pending.clear();
}

/// LSNs of the batches `recover_single_wal_stream` finds in a log file, in log order. The epoch
/// is taken from sector 0, which is how the current epoch looks after a clean run.
fn wal_lsns(fs: &CrashFs, name: &str) -> Vec<u64> {
    let img = content(fs, name).unwrap();
    let epoch = u64::from_le_bytes(img[4..12].try_into().unwrap());
    let f = raw_file(fs, name);
    crate::wal::recover_single_wal_stream(&*f, epoch)
        .unwrap()
        .batches
        .iter()
        .map(|b| b.lsn)
        .collect()
}

/// Asserts that exactly the first `cut` of `keys` (in commit order, table 1) are present.
fn assert_recovered_prefix(e: &mut Eng, keys: &[&[u8]], cut: usize) {
    let present: Vec<bool> = keys
        .iter()
        .map(|k| e.get_row(1, k).unwrap().is_some())
        .collect();
    let want: Vec<bool> = (0..keys.len()).map(|i| i < cut).collect();
    assert_eq!(
        present,
        want,
        "commit order {:?}: expected exactly the first {cut} commits, present flags are {present:?}",
        keys.iter()
            .map(|k| String::from_utf8_lossy(k).into_owned())
            .collect::<Vec<_>>()
    );
}

/// Reproduction 1 of ARCHITECTURE.md 4.6.1: `wal_sync.db` stops early at a damaged sector while
/// `wal_buf.db` holds durable batches with higher LSNs. Today the buffered batches are replayed
/// although the synchronous commit s2 below them is gone, so a commit that read s2 could survive
/// without it.
#[test]
fn damaged_sync_sector_cuts_later_buffered_batches() {
    let cfg = repro_config();
    let fs = Arc::new(CrashFs::new(DirMode::Unix, 1));
    let mut e = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
    let sync = WriteDurability::SyncTransactional;
    let buf = WriteDurability::BufferedFast;
    // Commit order (= LSN order): s1, s2 (sync), b3, b4 (buffered), s5 (sync). The last
    // synchronous commit makes b3 and b4 durable first (prefix durability).
    let keys: [&[u8]; 5] = [b"s1", b"s2", b"b3", b"b4", b"s5"];
    e.put_row(1, keys[0], b"v", &[], sync).unwrap();
    e.put_row(1, keys[1], b"v", &[], sync).unwrap();
    e.put_row(1, keys[2], b"v", &[], buf).unwrap();
    e.put_row(1, keys[3], b"v", &[], buf).unwrap();
    e.put_row(1, keys[4], b"v", &[], sync).unwrap();
    fs.cut_now();
    drop(e);
    // Everything above is durable, so a power cut that loses all unsynced data loses nothing.
    fs.crash(Crash::Power { keep: 0, lose: 100 });
    let sync_lsns = wal_lsns(&fs, "wal_sync.db");
    let buf_lsns = wal_lsns(&fs, "wal_buf.db");
    assert_eq!((sync_lsns.len(), buf_lsns.len()), (3, 2), "setup");
    assert!(
        sync_lsns[1] < buf_lsns[0] && buf_lsns[1] < sync_lsns[2],
        "setup"
    );

    // Media damage: one bit flips in the synced sector that holds s2 (sync batches are sector
    // isolated, so it is sector 1). The sector CRC no longer matches, so the stream stops there.
    damage_synced(&fs, "wal_sync.db", |img| img[4096 + 100] ^= 0x01);
    assert_eq!(
        wal_lsns(&fs, "wal_sync.db").len(),
        1,
        "setup: the damage took effect"
    );

    let mut e = StorageEngine::open(Arc::clone(&fs), cfg).unwrap();
    // The cut is at s2: only s1 may survive, nothing at or after it in either stream.
    assert_recovered_prefix(&mut e, &keys, 1);
    // The loss of an acknowledged commit is reported: b3 names s2 as its missing predecessor and
    // the sector that held it fails its CRC.
    let damage = e.last_wal_recovery().damage.clone().expect("damage");
    assert_eq!(damage.reason, WalDamageReason::CorruptSector);
    assert_eq!(damage.first_lost_lsn, sync_lsns[1]);
    assert_eq!(damage.discarded_batches.buffered, 2);
    assert_eq!(damage.discarded_batches.sync, 1);
    assert_eq!(damage.last_lsn_seen, sync_lsns[2]);
    assert_eq!(e.last_wal_recovery().replayed_batches.sync, 1);
    assert_eq!(e.last_wal_recovery().replayed_batches.buffered, 0);

    // No LSN that was ever logged is reused, discarded batches included, and the first new
    // batch links to the checkpoint, so the next recovery finds a clean chain.
    assert!(e.next_lsn() > sync_lsns[2], "next_lsn {}", e.next_lsn());
    e.put_row(1, b"n6", b"v", &[], sync).unwrap();
    fs.cut_now();
    drop(e);
    fs.crash(Crash::Power { keep: 0, lose: 100 });
    let mut e = StorageEngine::open(Arc::clone(&fs), repro_config()).unwrap();
    assert_eq!(e.last_wal_recovery().damage, None);
    assert_eq!(e.last_wal_recovery().replayed_batches.sync, 1);
    assert!(e.get_row(1, b"n6").unwrap().is_some());
    assert!(e.get_row(1, b"s1").unwrap().is_some());
    assert!(e.get_row(1, b"s2").unwrap().is_none());
}

/// A clean open reports nothing: no damage, nothing replayed, an empty torn tail.
#[test]
fn clean_open_reports_no_damage() {
    let cfg = repro_config();
    let fs = Arc::new(CrashFs::new(DirMode::Unix, 1));
    let e = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
    assert_eq!(e.last_wal_recovery(), &WalRecoveryReport::default());
    drop(e);
    let mut e = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
    e.put_row(1, b"a", b"v", &[], WriteDurability::BufferedFast)
        .unwrap();
    e.put_row(1, b"b", b"v", &[], WriteDurability::SyncTransactional)
        .unwrap();
    fs.cut_now();
    drop(e);
    fs.crash(Crash::Process);
    let e = StorageEngine::open(Arc::clone(&fs), cfg).unwrap();
    let r = e.last_wal_recovery();
    assert_eq!(r.damage, None);
    assert_eq!(
        r.replayed_batches,
        StreamCounts {
            sync: 1,
            buffered: 1
        }
    );
}

/// Reproduction 2 of ARCHITECTURE.md 4.6.1: a sync batch whose blob cannot be verified is dropped
/// on its own while the batches after it, in both streams, are kept. A later commit may have read
/// the value that the dropped commit wrote.
#[test]
fn unverifiable_sync_blob_cuts_later_batches_of_both_streams() {
    let cfg = repro_config();
    let fs = Arc::new(CrashFs::new(DirMode::Unix, 1));
    let mut e = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
    let sync = WriteDurability::SyncTransactional;
    let buf = WriteDurability::BufferedFast;
    // Commit order: s1 (inline), s2 (out-of-line value), b3 (buffered), s4 (sync, which makes
    // b3 durable). The blob of s2 was synced before its WAL record.
    let keys: [&[u8]; 4] = [b"s1", b"s2", b"b3", b"s4"];
    e.put_row(1, keys[0], b"v", &[], sync).unwrap();
    e.put_row(1, keys[1], &[0x5A; 1500], &[], sync).unwrap();
    e.put_row(1, keys[2], b"v", &[], buf).unwrap();
    e.put_row(1, keys[3], b"v", &[], sync).unwrap();
    fs.cut_now();
    drop(e);
    fs.crash(Crash::Power { keep: 0, lose: 100 });
    let sync_lsns = wal_lsns(&fs, "wal_sync.db");
    assert_eq!(sync_lsns.len(), 3, "setup");
    assert_eq!(wal_lsns(&fs, "wal_buf.db").len(), 1, "setup");

    // Media damage: a byte inside the value of s2 rots in its synced blob segment, so the blob
    // CRC no longer matches. Both WAL streams stay intact.
    let blob = fs
        .list_files()
        .unwrap()
        .into_iter()
        .find(|n| n.starts_with("blob_std_"))
        .expect("setup: the value is out of line");
    damage_synced(&fs, &blob, |img| {
        let at = img.windows(32).position(|w| w == [0x5A; 32]).unwrap();
        img[at + 100] ^= 0x01;
    });

    let mut e = StorageEngine::open(Arc::clone(&fs), cfg).unwrap();
    // The cut is at s2: only s1 may survive, neither s4 (sync) nor b3 (buffered).
    assert_recovered_prefix(&mut e, &keys, 1);
    let damage = e.last_wal_recovery().damage.clone().expect("damage");
    assert_eq!(damage.reason, WalDamageReason::UnverifiableBlob);
    assert_eq!(damage.first_lost_lsn, sync_lsns[1]);
    assert_eq!(damage.discarded_batches.sync, 2);
    assert_eq!(damage.discarded_batches.buffered, 1);
}

// =============================================================================================
// WalDamagePolicy (ARCHITECTURE.md 4.6.6)
// =============================================================================================

/// Every file of the directory with both of its images, so that a failed open is shown to have
/// written, created, synced and removed nothing.
fn dir_state(fs: &CrashFs) -> BTreeMap<String, (Vec<u8>, Vec<u8>)> {
    let st = fs.st();
    st.dir
        .iter()
        .map(|(name, &id)| {
            let ino = &st.inodes[id];
            (name.clone(), (ino.cache.clone(), ino.durable.clone()))
        })
        .collect()
}

/// Reproduction 1 (see `damaged_sync_sector_cuts_later_buffered_batches`), crashed and damaged
/// but not yet reopened. Returns the keys in commit order.
fn sync_sector_damage_fs() -> (Arc<CrashFs>, [&'static [u8]; 5], u64) {
    let fs = Arc::new(CrashFs::new(DirMode::Unix, 1));
    let mut e = StorageEngine::open(Arc::clone(&fs), repro_config()).unwrap();
    let sync = WriteDurability::SyncTransactional;
    let buf = WriteDurability::BufferedFast;
    let keys: [&[u8]; 5] = [b"s1", b"s2", b"b3", b"b4", b"s5"];
    for (i, k) in keys.iter().enumerate() {
        let d = if i == 2 || i == 3 { buf } else { sync };
        e.put_row(1, k, b"v", &[], d).unwrap();
    }
    fs.cut_now();
    drop(e);
    fs.crash(Crash::Power { keep: 0, lose: 100 });
    let s2 = wal_lsns(&fs, "wal_sync.db")[1];
    damage_synced(&fs, "wal_sync.db", |img| img[4096 + 100] ^= 0x01);
    (fs, keys, s2)
}

/// Reproduction 2 (see `unverifiable_sync_blob_cuts_later_batches_of_both_streams`).
fn rotten_blob_fs() -> (Arc<CrashFs>, [&'static [u8]; 4], u64) {
    let fs = Arc::new(CrashFs::new(DirMode::Unix, 1));
    let mut e = StorageEngine::open(Arc::clone(&fs), repro_config()).unwrap();
    let sync = WriteDurability::SyncTransactional;
    let keys: [&[u8]; 4] = [b"s1", b"s2", b"b3", b"s4"];
    e.put_row(1, keys[0], b"v", &[], sync).unwrap();
    e.put_row(1, keys[1], &[0x5A; 1500], &[], sync).unwrap();
    e.put_row(1, keys[2], b"v", &[], WriteDurability::BufferedFast)
        .unwrap();
    e.put_row(1, keys[3], b"v", &[], sync).unwrap();
    fs.cut_now();
    drop(e);
    fs.crash(Crash::Power { keep: 0, lose: 100 });
    let s2 = wal_lsns(&fs, "wal_sync.db")[1];
    let blob = fs
        .list_files()
        .unwrap()
        .into_iter()
        .find(|n| n.starts_with("blob_std_"))
        .expect("setup: the value is out of line");
    damage_synced(&fs, &blob, |img| {
        let at = img.windows(32).position(|w| w == [0x5A; 32]).unwrap();
        img[at + 100] ^= 0x01;
    });
    (fs, keys, s2)
}

fn fail_config() -> EngineConfig {
    let mut c = repro_config();
    c.wal_damage_policy = WalDamagePolicy::Fail;
    c
}

/// Opens under `Fail`, expects the damage error and checks that no file changed in any way.
fn assert_fail_open_is_untouched(fs: &Arc<CrashFs>, reason: WalDamageReason, first_lost: u64) {
    let before = dir_state(fs);
    let err = match StorageEngine::open(Arc::clone(fs), fail_config()) {
        Ok(_) => panic!("open under Fail must refuse a damaged WAL"),
        Err(e) => e,
    };
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    let damage = err
        .get_ref()
        .and_then(|e| e.downcast_ref::<WalDamage>())
        .expect("the inner error is the WalDamage");
    assert_eq!(damage.reason, reason);
    assert_eq!(damage.first_lost_lsn, first_lost);
    assert!(err.to_string().contains(&first_lost.to_string()), "{err}");
    assert!(dir_state(fs) == before, "a failed open changed a file");
}

#[test]
fn fail_policy_refuses_damaged_sync_sector_and_leaves_files_untouched() {
    let (fs, keys, s2) = sync_sector_damage_fs();
    // An orphan above `next_seq_id` makes a normal open commit the catalog to reserve its SeqID;
    // a refused open must not.
    let orphan = crate::meta::format_idx_file_name(9_999);
    fs.open_file(&orphan, true, true, OpenHint::ImmutableSequential)
        .unwrap()
        .write_all_at(b"orphan", 0)
        .unwrap();
    assert_fail_open_is_untouched(&fs, WalDamageReason::CorruptSector, s2);
    // The same directory still opens under `Truncate` and gives the prefix.
    let mut e = StorageEngine::open(Arc::clone(&fs), repro_config()).unwrap();
    assert_recovered_prefix(&mut e, &keys, 1);
    assert!(e.last_wal_recovery().damage.is_some());
}

#[test]
fn fail_policy_refuses_unverifiable_blob_and_leaves_files_untouched() {
    let (fs, keys, s2) = rotten_blob_fs();
    assert_fail_open_is_untouched(&fs, WalDamageReason::UnverifiableBlob, s2);
    let mut e = StorageEngine::open(Arc::clone(&fs), repro_config()).unwrap();
    assert_recovered_prefix(&mut e, &keys, 1);
    assert!(e.last_wal_recovery().damage.is_some());
}

/// Regression test for [W1]: on a single-stream (`SyncTransactional`-only) WAL, zeroing a
/// middle sector or replacing it with an intact older-epoch sector must still be detected by
/// `WalDamagePolicy::Fail` and salvaged by `open_with_salvage`.
#[test]
fn fail_and_salvage_detect_single_stream_zeroed_and_stale_sync_sectors() {
    for kind in ["zeroed", "stale_epoch"] {
        let fs = Arc::new(CrashFs::new(DirMode::Unix, 1));
        let mut e = StorageEngine::open(Arc::clone(&fs), salvage_config()).unwrap();
        let t1 = e
            .register_or_resolve_vtab_with_durability("orders", None, SYNC)
            .unwrap();
        e.flush_memtable().unwrap();

        e.put_row(t1, b"k1", b"v1", &[], SYNC).unwrap();
        let lsn2 = e.next_lsn();
        e.put_row(t1, b"k2", b"v2", &[], SYNC).unwrap();
        e.put_row(t1, b"k3", b"v3", &[], SYNC).unwrap();
        fs.cut_now();
        drop(e);
        fs.crash(Crash::Power { keep: 0, lose: 100 });

        damage_synced(&fs, "wal_sync.db", |img| match kind {
            "zeroed" => img[4096..8192].fill(0),
            "stale_epoch" => {
                let sec = &mut img[4096..8192];
                let epoch = u64::from_le_bytes(sec[4..12].try_into().unwrap());
                assert!(epoch >= 2);
                sec[4..12].copy_from_slice(&(epoch - 1).to_le_bytes());
                let crc = crate::util::crc32c::crc32c(&sec[4..]);
                sec[0..4].copy_from_slice(&crc.to_le_bytes());
            }
            _ => unreachable!(),
        });

        assert_fail_open_is_untouched(&fs, WalDamageReason::MissingPredecessor, lsn2);

        let (mut e, salvaged) = open_salvage(&fs);
        let d = e
            .last_wal_recovery()
            .damage
            .clone()
            .expect("open_with_salvage must report MissingPredecessor");
        assert_eq!(d.reason, WalDamageReason::MissingPredecessor, "{kind}");
        assert_eq!(d.first_lost_lsn, lsn2, "{kind}");
        assert_eq!(e.get_row(t1, b"k1").unwrap(), Some(b"v1".to_vec()));
        assert_eq!(e.get_row(t1, b"k2").unwrap(), None);
        assert_eq!(e.get_row(t1, b"k3").unwrap(), None);
        assert_eq!(salvaged.len(), 1, "{kind}");
        assert_eq!(salvaged[0].ops, vec![put_op("orders", b"k3", b"v3")]);
    }
}

/// `Truncate` is the default: a default configuration opens a damaged WAL, replays exactly the
/// prefix and reports the damage.
#[test]
fn default_policy_truncates_and_reports_damage() {
    assert_eq!(
        EngineConfig::default().wal_damage_policy,
        WalDamagePolicy::Truncate
    );
    for (fs, keys, s2) in [
        {
            let (fs, k, s2) = sync_sector_damage_fs();
            (fs, k.to_vec(), s2)
        },
        {
            let (fs, k, s2) = rotten_blob_fs();
            (fs, k.to_vec(), s2)
        },
    ] {
        let mut e = StorageEngine::open(Arc::clone(&fs), repro_config()).unwrap();
        assert_recovered_prefix(&mut e, &keys, 1);
        let damage = e.last_wal_recovery().damage.clone().expect("damage");
        assert_eq!(damage.first_lost_lsn, s2);
    }
}

/// `Fail` has no false positives: a new database, a clean reopen and every kind of ordinary
/// crash (an unsynced tail is not damage) open normally.
#[test]
fn fail_policy_opens_clean_and_crashed_databases() {
    let fs = Arc::new(CrashFs::new(DirMode::Unix, 1));
    let e = StorageEngine::open(Arc::clone(&fs), fail_config()).unwrap();
    assert_eq!(e.last_wal_recovery(), &WalRecoveryReport::default());
    drop(e);
    for crash in [
        Crash::Process,
        Crash::Power { keep: 0, lose: 100 },
        Crash::Power { keep: 50, lose: 50 },
        Crash::Power { keep: 100, lose: 0 },
    ] {
        let mut e = StorageEngine::open(Arc::clone(&fs), fail_config()).unwrap();
        assert_eq!(e.last_wal_recovery().damage, None);
        e.put_row(1, b"a", b"v", &[], WriteDurability::BufferedFast)
            .unwrap();
        e.put_row(1, b"b", b"v", &[], WriteDurability::SyncTransactional)
            .unwrap();
        e.put_row(1, b"c", b"v", &[], WriteDurability::BufferedFast)
            .unwrap();
        fs.cut_now();
        drop(e);
        fs.crash(crash);
        let e = StorageEngine::open(Arc::clone(&fs), fail_config()).unwrap();
        assert_eq!(e.last_wal_recovery().damage, None, "{crash:?}");
    }
}

// =============================================================================================
// Salvage (ARCHITECTURE.md 4.6.7)
// =============================================================================================

const SYNC: WriteDurability = WriteDurability::SyncTransactional;
const BUF: WriteDurability = WriteDurability::BufferedFast;

/// `repro_config` with a MemTable large enough that no commit of these tests triggers a flush,
/// which would checkpoint the WAL the tests want to damage.
fn salvage_config() -> EngineConfig {
    let mut c = repro_config();
    c.memtable_capacity_bytes = 64 << 20;
    c
}

/// A crashed directory whose sync log has a bad sector at batch `s2`. Written in LSN order:
/// the catalog entries of tables `orders` (1) and `indexed` (2), `s1` (`orders`/`a`, intact),
/// `s2` (`orders`/`b`, damaged: the cut), then whatever `script` does. Everything is durable.
fn damaged_fs(script: impl FnOnce(&mut Eng)) -> Arc<CrashFs> {
    let fs = Arc::new(CrashFs::new(DirMode::Unix, 1));
    let mut e = StorageEngine::open(Arc::clone(&fs), salvage_config()).unwrap();
    let orders = e.register_or_resolve_vtab_with_durability("orders", None, SYNC);
    assert_eq!(orders.unwrap(), 1);
    let indexed = e.register_or_resolve_vtab_with_durability("indexed", None, SYNC);
    assert_eq!(indexed.unwrap(), 2);
    e.put_row(1, b"a", b"va", &[], SYNC).unwrap();
    let cut = e.next_lsn();
    e.put_row(1, b"b", b"vb", &[], SYNC).unwrap();
    script(&mut e);
    fs.cut_now();
    drop(e);
    fs.crash(Crash::Power { keep: 0, lose: 100 });
    // Sync batches of this size occupy one sector each, so the position is the sector.
    let at = wal_lsns(&fs, "wal_sync.db")
        .iter()
        .position(|&l| l == cut)
        .expect("s2 is in the sync log");
    damage_synced(&fs, "wal_sync.db", |img| img[at * 4096 + 100] ^= 0x01);
    fs
}

fn open_salvage(fs: &Arc<CrashFs>) -> (Eng, Vec<SalvagedTx>) {
    StorageEngine::open_with_salvage(Arc::clone(fs), salvage_config()).unwrap()
}

fn put_op(table: &str, key: &[u8], value: &[u8]) -> SalvagedOp {
    SalvagedOp::Put {
        table: table.to_string(),
        key: key.to_vec(),
        value: value.to_vec(),
    }
}

fn del_op(table: &str, key: &[u8]) -> SalvagedOp {
    SalvagedOp::Delete {
        table: table.to_string(),
        key: key.to_vec(),
    }
}

/// The script of the first salvage tests: a buffered put, a buffered delete of an intact row and
/// a synchronous put, all after the cut. Returns their LSNs.
fn plain_script(e: &mut Eng) -> Vec<u64> {
    let mut lsns = vec![e.next_lsn()];
    e.put_row(1, b"c", b"vc", &[], BUF).unwrap();
    lsns.push(e.next_lsn());
    assert!(e.delete_row(1, b"a", BUF).unwrap());
    lsns.push(e.next_lsn());
    e.put_row(1, b"d", b"vd", &[], SYNC).unwrap();
    lsns
}

#[test]
fn salvage_returns_intact_transactions_after_the_cut_in_lsn_order() {
    let mut lsns = Vec::new();
    let fs = damaged_fs(|e| lsns = plain_script(e));
    let (mut e, salvaged) = open_salvage(&fs);
    assert_eq!(
        salvaged,
        vec![
            SalvagedTx {
                lsn: lsns[0],
                ops: vec![put_op("orders", b"c", b"vc")]
            },
            SalvagedTx {
                lsn: lsns[1],
                ops: vec![del_op("orders", b"a")]
            },
            SalvagedTx {
                lsn: lsns[2],
                ops: vec![put_op("orders", b"d", b"vd")]
            },
        ]
    );
    // The state is the prefix below the cut, as under `Truncate`.
    assert!(row_is(&mut e, 1, b"a", b"va"));
    for k in [&b"b"[..], b"c", b"d"] {
        assert!(e.get_row(1, k).unwrap().is_none());
    }
    let damage = e.last_wal_recovery().damage.clone().expect("damage");
    assert_eq!(damage.unsalvageable, 0);
}

#[test]
fn salvage_reads_a_blob_value_that_the_open_reclaims() {
    let big = vec![0x5Au8; 1500];
    let fs = damaged_fs(|e| {
        e.put_row(1, b"big", &big, &[], SYNC).unwrap();
        e.put_row(1, b"e", b"ve", &[], SYNC).unwrap();
    });
    let has_blob = |fs: &Arc<CrashFs>| {
        fs.list_files()
            .unwrap()
            .iter()
            .any(|n| n.starts_with("blob_std_"))
    };
    assert!(has_blob(&fs), "setup");
    let (mut e, salvaged) = open_salvage(&fs);
    assert_eq!(salvaged.len(), 2);
    assert_eq!(salvaged[0].ops, vec![put_op("orders", b"big", &big)]);
    assert_eq!(salvaged[1].ops, vec![put_op("orders", b"e", b"ve")]);
    assert!(e.get_row(1, b"big").unwrap().is_none());
    // Only the discarded batch referenced the segment: it is reclaimed as usual.
    assert!(!has_blob(&fs), "{:?}", fs.list_files());
}

/// Writes `n` rows `<prefix>000..` of 3000 inline bytes in one synchronous transaction.
fn write_big_tx(e: &mut Eng, prefix: &str, n: usize) {
    e.begin_transaction();
    for i in 0..n {
        let key = format!("{prefix}{i:03}");
        e.put_row(1, key.as_bytes(), &[0x11; 3000], &[], SYNC)
            .unwrap();
    }
    e.commit_transaction().unwrap();
}

/// Two multi-chunk transactions after the cut, the second with the chunk `lost` (0 = begin,
/// 1 = a middle one, 2 = the last, which carries the commit marker) damaged. Only the first one
/// and the single-batch commit after both may be salvaged.
fn multi_chunk_case(lost: usize) {
    const N: usize = 400;
    let fs = damaged_fs(|e| {
        e.set_table_inline_threshold(1, 4000);
        write_big_tx(e, "k", N);
        write_big_tx(e, "x", N);
        e.put_row(1, b"z", b"vz", &[], SYNC).unwrap();
    });
    let epoch = u64::from_le_bytes(
        content(&fs, "wal_sync.db").unwrap()[4..12]
            .try_into()
            .unwrap(),
    );
    let rec = crate::wal::recover_single_wal_stream(&*raw_file(&fs, "wal_sync.db"), epoch).unwrap();
    // The scan stops at the damaged s2; everything after it is found past the stop.
    let lsns: Vec<u64> = (rec.batches.iter().chain(&rec.past_stop))
        .map(|b| b.lsn)
        .collect();
    // Catalog x2, s1, (s2 is lost), chunks of "k", chunks of "x", z.
    assert!(
        (lsns.len() - 4).is_multiple_of(2) && (lsns.len() - 4) / 2 >= 3,
        "{lsns:?}"
    );
    let chunks = (lsns.len() - 4) / 2;
    let (k_last, x_lost) = (
        lsns[3 + chunks - 1],
        lsns[3 + chunks + lost.min(chunks - 1)],
    );
    let img = content(&fs, "wal_sync.db").unwrap();
    let used = img
        .chunks(4096)
        .take_while(|s| s.iter().any(|&b| b != 0))
        .count();
    let span = (used - 5) / 2;
    // A sector inside the chunk: chunks hold 512 KB, about 128 sectors, the last one the rest.
    let sector = 4 + span + [10, span / 2, span - 2][lost];
    damage_synced(&fs, "wal_sync.db", |img| img[sector * 4096 + 100] ^= 0x01);
    let rec = crate::wal::recover_single_wal_stream(&*raw_file(&fs, "wal_sync.db"), epoch).unwrap();
    let found: Vec<u64> = rec.past_stop.iter().map(|b| b.lsn).collect();
    assert!(
        found.contains(&k_last) && !found.contains(&x_lost),
        "setup: {found:?}"
    );

    let (mut e, salvaged) = open_salvage(&fs);
    let want_k: Vec<SalvagedOp> = (0..N)
        .map(|i| put_op("orders", format!("k{i:03}").as_bytes(), &[0x11; 3000]))
        .collect();
    let got: Vec<u64> = salvaged.iter().map(|t| t.lsn).collect();
    assert_eq!(
        salvaged.len(),
        2,
        "{got:?} lost {lost} lsns {lsns:?} found {found:?}"
    );
    assert_eq!(
        salvaged[0],
        SalvagedTx {
            lsn: k_last,
            ops: want_k
        }
    );
    assert_eq!(salvaged[1].ops, vec![put_op("orders", b"z", b"vz")]);
    assert!(e.get_row(1, b"k000").unwrap().is_none());
    let damage = e.last_wal_recovery().damage.clone().unwrap();
    assert_eq!(damage.unsalvageable, 0);
}

#[test]
fn salvage_returns_a_multi_chunk_transaction_only_if_every_chunk_is_intact() {
    for lost in 0..3 {
        multi_chunk_case(lost);
    }
}

/// A transaction of at least four chunks loses its begin and its commit chunk. Its middle chunks
/// are intact but are not a transaction: none of them may be returned, while an unrelated
/// single-batch transaction after it still is.
#[test]
fn salvage_never_returns_the_middle_chunks_of_a_transaction_that_lost_both_ends() {
    let fs = damaged_fs(|e| {
        e.set_table_inline_threshold(1, 4000);
        write_big_tx(e, "m", 600);
        e.put_row(1, b"z", b"vz", &[], SYNC).unwrap();
    });
    let epoch = u64::from_le_bytes(
        content(&fs, "wal_sync.db").unwrap()[4..12]
            .try_into()
            .unwrap(),
    );
    let scan = || crate::wal::recover_single_wal_stream(&*raw_file(&fs, "wal_sync.db"), epoch);
    let rec = scan().unwrap();
    let lsns: Vec<u64> = (rec.batches.iter().chain(&rec.past_stop))
        .map(|b| b.lsn)
        .collect();
    // Catalog x2, s1, (s2 is lost), the chunks, z.
    let chunks = lsns.len() - 4;
    assert!(chunks >= 4, "{lsns:?}");
    let used = content(&fs, "wal_sync.db")
        .unwrap()
        .chunks(4096)
        .take_while(|s| s.iter().any(|&b| b != 0))
        .count();
    let span = used - 5;
    for sector in [4 + 10, 4 + span - 2] {
        damage_synced(&fs, "wal_sync.db", |img| img[sector * 4096 + 100] ^= 0x01);
    }
    let found: Vec<u64> = scan().unwrap().past_stop.iter().map(|b| b.lsn).collect();
    let middles = &lsns[4..3 + chunks - 1];
    assert!(
        !found.contains(&lsns[3]) && !found.contains(&lsns[2 + chunks]),
        "setup: {found:?}"
    );
    assert!(
        middles.iter().all(|l| found.contains(l)),
        "setup: {found:?}"
    );

    let (_e, salvaged) = open_salvage(&fs);
    let got: Vec<u64> = salvaged.iter().map(|t| t.lsn).collect();
    assert_eq!(got, vec![lsns[lsns.len() - 1]], "{got:?}");
    assert_eq!(salvaged[0].ops, vec![put_op("orders", b"z", b"vz")]);
}

#[test]
fn salvage_skips_derived_records() {
    let fs = damaged_fs(|e| {
        e.put_row(2, b"p1", b"v1", &[(1, b"col1".to_vec())], SYNC)
            .unwrap();
        // An overwrite also deletes the old index entry and rewrites the row metadata.
        e.put_row(2, b"p1", b"v2", &[(1, b"col2".to_vec())], SYNC)
            .unwrap();
    });
    let (_e, salvaged) = open_salvage(&fs);
    let ops: Vec<Vec<SalvagedOp>> = salvaged.into_iter().map(|t| t.ops).collect();
    assert_eq!(
        ops,
        vec![
            vec![put_op("indexed", b"p1", b"v1")],
            vec![put_op("indexed", b"p1", b"v2")]
        ]
    );
}

/// After the cut: a good put, a new table (schema change), a put into it (unknown to recovery),
/// a transaction mixing a known and an unknown table, and another good put.
fn schema_script(e: &mut Eng) {
    e.put_row(1, b"c", b"vc", &[], SYNC).unwrap();
    let late = e
        .register_or_resolve_vtab_with_durability("late", None, SYNC)
        .unwrap();
    e.put_row(late, b"x", b"vx", &[], SYNC).unwrap();
    e.begin_transaction();
    e.put_row(1, b"m", b"vm", &[], SYNC).unwrap();
    e.put_row(late, b"y", b"vy", &[], SYNC).unwrap();
    e.commit_transaction().unwrap();
    e.put_row(1, b"d", b"vd", &[], SYNC).unwrap();
}

#[test]
fn salvage_counts_schema_changes_and_unknown_tables_instead_of_returning_them() {
    let fs = damaged_fs(schema_script);
    let (e, salvaged) = open_salvage(&fs);
    let ops: Vec<Vec<SalvagedOp>> = salvaged.into_iter().map(|t| t.ops).collect();
    assert_eq!(
        ops,
        vec![
            vec![put_op("orders", b"c", b"vc")],
            vec![put_op("orders", b"d", b"vd")]
        ]
    );
    // The catalog entry, the row of the unknown table, and the transaction's unknown-table row;
    // the transaction's known row is lost with it (whole transactions only).
    let damage = e.last_wal_recovery().damage.clone().unwrap();
    assert_eq!(damage.unsalvageable, 3);
    // A plain open reports the same count.
    let fs = damaged_fs(schema_script);
    let e = StorageEngine::open(Arc::clone(&fs), salvage_config()).unwrap();
    let damage = e.last_wal_recovery().damage.clone().unwrap();
    assert_eq!(damage.unsalvageable, 3);
}

#[test]
fn salvage_counts_a_value_whose_blob_fails_verification() {
    let fs = damaged_fs(|e| {
        e.put_row(1, b"big", &[0x5A; 1500], &[], SYNC).unwrap();
        e.put_row(1, b"e", b"ve", &[], SYNC).unwrap();
    });
    let blob = fs
        .list_files()
        .unwrap()
        .into_iter()
        .find(|n| n.starts_with("blob_std_"))
        .expect("setup");
    damage_synced(&fs, &blob, |img| {
        let at = img.windows(32).position(|w| w == [0x5A; 32]).unwrap();
        img[at + 100] ^= 0x01;
    });
    let (e, salvaged) = open_salvage(&fs);
    assert_eq!(salvaged.len(), 1);
    assert_eq!(salvaged[0].ops, vec![put_op("orders", b"e", b"ve")]);
    let damage = e.last_wal_recovery().damage.clone().unwrap();
    assert_eq!(damage.unsalvageable, 1);
}

#[test]
fn salvage_without_damage_is_an_empty_list_and_a_plain_open() {
    let build = || {
        let fs = Arc::new(CrashFs::new(DirMode::Unix, 1));
        let mut e = StorageEngine::open(Arc::clone(&fs), salvage_config()).unwrap();
        e.register_or_resolve_vtab_with_durability("orders", None, SYNC)
            .unwrap();
        e.put_row(1, b"a", b"va", &[], BUF).unwrap();
        e.put_row(1, b"b", &[0x77; 2000], &[], SYNC).unwrap();
        e.put_row(1, b"c", b"vc", &[], BUF).unwrap();
        fs.cut_now();
        drop(e);
        fs.crash(Crash::Power { keep: 0, lose: 100 });
        fs
    };
    let (fs_a, fs_b) = (build(), build());
    let (mut a, salvaged) = open_salvage(&fs_a);
    let mut b = StorageEngine::open(Arc::clone(&fs_b), salvage_config()).unwrap();
    assert!(salvaged.is_empty());
    assert_eq!(a.last_wal_recovery().damage, None);
    assert_eq!(a.last_wal_recovery(), b.last_wal_recovery());
    for k in [&b"a"[..], b"b", b"c"] {
        assert_eq!(a.get_row(1, k).unwrap(), b.get_row(1, k).unwrap());
    }
    // `c` was buffered after the last synchronous commit: lost like any unsynced tail.
    assert!(a.get_row(1, b"a").unwrap().is_some() && a.get_row(1, b"b").unwrap().is_some());
    assert!(dir_state(&fs_a) == dir_state(&fs_b));
}

#[test]
fn salvaged_operations_written_back_restore_the_rows() {
    let big = vec![0x5Au8; 1500];
    let fs = damaged_fs(|e| {
        plain_script(e);
        e.put_row(1, b"big", &big, &[], SYNC).unwrap();
    });
    let (mut e, salvaged) = open_salvage(&fs);
    for tx in &salvaged {
        for op in &tx.ops {
            match op {
                SalvagedOp::Put { table, key, value } => {
                    let id = e
                        .register_or_resolve_vtab_with_durability(table, None, SYNC)
                        .unwrap();
                    e.put_row(id, key, value, &[], SYNC).unwrap();
                }
                SalvagedOp::Delete { table, key } => {
                    let id = e
                        .register_or_resolve_vtab_with_durability(table, None, SYNC)
                        .unwrap();
                    e.delete_row(id, key, SYNC).unwrap();
                }
            }
        }
    }
    // `a` was deleted by a salvaged transaction; `b` was lost with the cut itself.
    assert!(e.get_row(1, b"a").unwrap().is_none());
    assert!(e.get_row(1, b"b").unwrap().is_none());
    assert!(row_is(&mut e, 1, b"c", b"vc"));
    assert!(row_is(&mut e, 1, b"d", b"vd"));
    assert!(row_is(&mut e, 1, b"big", &big));
}

#[test]
fn salvage_open_after_fail_open_returns_the_list() {
    let fs = damaged_fs(|e| {
        plain_script(e);
    });
    let mut cfg = salvage_config();
    cfg.wal_damage_policy = WalDamagePolicy::Fail;
    let before = dir_state(&fs);
    let err = match StorageEngine::open(Arc::clone(&fs), cfg.clone()) {
        Ok(_) => panic!("open under Fail must refuse a damaged WAL"),
        Err(e) => e,
    };
    let damage = err
        .get_ref()
        .and_then(|e| e.downcast_ref::<WalDamage>())
        .expect("the inner error is the WalDamage")
        .clone();
    assert!(dir_state(&fs) == before, "a failed open changed a file");
    // Salvage opens as `Truncate` whatever the configured policy is.
    let (e, salvaged) = StorageEngine::open_with_salvage(Arc::clone(&fs), cfg).unwrap();
    assert_eq!(salvaged.len(), 3);
    assert_eq!(e.last_wal_recovery().damage.as_ref(), Some(&damage));
}

#[test]
fn crash_during_a_salvage_open_reopens_to_the_same_list_or_cleanly() {
    let build = || {
        damaged_fs(|e| {
            plain_script(e);
            e.put_row(1, b"big", &[0x5A; 1500], &[], SYNC).unwrap();
        })
    };
    let rows = |e: &mut Eng| -> Vec<Option<Vec<u8>>> {
        [&b"a"[..], b"b", b"c", b"d", b"big"]
            .iter()
            .map(|k| e.get_row(1, k).unwrap())
            .collect()
    };
    let fs = build();
    let start = fs.op_stats().0;
    let (mut e, want) = open_salvage(&fs);
    let total = fs.op_stats().0 - start;
    let (want_damage, want_rows) = (e.last_wal_recovery().damage.clone(), rows(&mut e));
    assert!(
        want.len() == 4 && want_damage.is_some() && total > 4,
        "setup"
    );
    drop(e);

    let crashes = [
        Crash::Process,
        Crash::Power { keep: 0, lose: 100 },
        Crash::Power { keep: 50, lose: 50 },
    ];
    let (mut same, mut clean) = (0, 0);
    for crash in crashes {
        for k in 0..=total {
            let fs = build();
            let base = fs.op_stats().0;
            fs.arm(Some(base + k), None);
            let opened = StorageEngine::open_with_salvage(Arc::clone(&fs), salvage_config());
            fs.cut_now();
            drop(opened);
            fs.crash(crash);
            let (mut e, got) = open_salvage(&fs);
            if got.is_empty() {
                // The epochs had rotated: nothing is left to salvage, the prefix state remains.
                assert_eq!(e.last_wal_recovery().damage, None, "{crash:?} cut {k}");
                clean += 1;
            } else {
                assert_eq!(got, want, "{crash:?} cut {k}");
                assert_eq!(
                    e.last_wal_recovery().damage,
                    want_damage,
                    "{crash:?} cut {k}"
                );
                same += 1;
            }
            assert_eq!(rows(&mut e), want_rows, "{crash:?} cut {k}");
        }
    }
    assert!(same > 0 && clean > 0, "same {same}, clean {clean}");
}

// =============================================================================================
// Minimal reproductions of bugs found by the group-commit and media-damage modes
// =============================================================================================

/// The live secondary entries of `table`.
fn live_index_entries(e: &mut Eng, table: u16) -> Vec<Vec<u8>> {
    e.scan_merged_prefix(&encode_namespace_prefix(table, NS_SECONDARY_INDEX))
        .unwrap()
        .into_iter()
        .filter(|en| !matches!(en.value, IndexValue::Tombstone))
        .map(|en| en.key)
        .collect()
}

/// Appends a group commit of one row with a secondary entry, leaves its flush pending and
/// returns the ticket.
fn append_group_put(e: &mut Eng, key: &[u8], value: &[u8], sec: &[(u16, Vec<u8>)]) -> u64 {
    e.begin_transaction_for_conn(1).unwrap();
    e.put_row(2, key, value, sec, SYNC).unwrap();
    e.prepare_commit_transaction(SYNC).unwrap();
    match e.begin_commit_for_conn(1).unwrap() {
        CommitStep::AwaitingGroup { lsn, .. } => lsn,
        _ => panic!("a plain synchronous transaction must join the group commit"),
    }
}

/// A log file that a third party truncated is shorter than configured. A refused open under `Fail`
/// must leave it as it is: the file is lengthened only after the damage decision.
#[test]
fn fail_policy_leaves_a_truncated_wal_file_untouched() {
    let (fs, _keys, _s2) = sync_sector_damage_fs();
    // Only the first sector of the sync log is left: s2 and s5 are gone, the buffered batches
    // still carry the watermark that covers s2.
    damage_synced(&fs, "wal_sync.db", |img| img.truncate(4096));
    let before = dir_state(&fs);
    let err = match StorageEngine::open(Arc::clone(&fs), fail_config()) {
        Ok(_) => panic!("open under Fail must refuse a log that lost an acknowledged commit"),
        Err(e) => e,
    };
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    let after = dir_state(&fs);
    for name in ["wal_sync.db", "wal_buf.db", "boot_meta.db"] {
        let (b, a) = (&before[name], &after[name]);
        assert!(
            a == b,
            "{name} changed: {} -> {} bytes in the cache, {} -> {} on the device",
            b.0.len(),
            a.0.len(),
            b.1.len(),
            a.1.len()
        );
    }
}

/// A log file that is missing altogether is the one exception to "a refused `Fail` open changes
/// no file" (ARCHITECTURE.md 4.6.6): the streams are opened before the damage decision, and
/// opening creates the file, empty. Everything else stays as it was, and the open still refuses,
/// because the buffered batches carry a watermark that covers the lost sync batches.
#[test]
fn fail_policy_with_a_missing_wal_file_refuses_and_creates_it_empty() {
    let (fs, _keys, _s2) = sync_sector_damage_fs();
    fs.st().dir.remove("wal_sync.db");
    let before = dir_state(&fs);
    assert!(!before.contains_key("wal_sync.db"));
    let err = match StorageEngine::open(Arc::clone(&fs), fail_config()) {
        Ok(_) => panic!("open under Fail must refuse a log that lost an acknowledged commit"),
        Err(e) => e,
    };
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    assert!(
        err.get_ref()
            .and_then(|e| e.downcast_ref::<WalDamage>())
            .is_some(),
        "{err}"
    );
    let mut after = dir_state(&fs);
    let created = after
        .remove("wal_sync.db")
        .expect("the missing log file is created");
    assert!(
        created.0.is_empty() && created.1.is_empty(),
        "created with {} bytes in the cache, {} on the device",
        created.0.len(),
        created.1.len()
    );
    assert!(after == before, "a file other than the missing log changed");
}

/// A write outside a transaction must see the row that a pending group commit wrote: it reads
/// the old row's `'t'` record to maintain the secondary entries, and a stale read leaves the
/// entries of the group-committed version behind as orphans.
#[test]
fn delete_after_a_pending_group_commit_removes_its_secondary_entries() {
    let fs = Arc::new(CrashFs::new(DirMode::Unix, 1));
    let mut e = StorageEngine::open(Arc::clone(&fs), repro_config()).unwrap();
    let mut rng = Rng::new(7);
    let old = make_value(&mut rng, 2, 8);
    e.put_row(2, b"k", &old, &full_keys(2, &old), SYNC).unwrap();
    let value = make_value(&mut rng, 2, 8);
    let sec = full_keys(2, &value);
    append_group_put(&mut e, b"k", &value, &sec);
    assert!(
        !e.pending_group.is_empty(),
        "the flush must still be pending"
    );
    // An autocommit write goes on top of the pending group commit.
    let deleted = e.delete_row(2, b"k", BUF).unwrap();
    assert!(e.get_row(2, b"k").unwrap().is_none());
    let entries = live_index_entries(&mut e, 2);
    assert!(
        entries.is_empty(),
        "delete_row returned {deleted}, the row is gone, but its secondary entries remain: \
         {entries:02x?}"
    );
}

/// An old-format log (version 0 or 1 at the current epoch) is damage that `Fail` refuses and the
/// default `Truncate` discards, while sectors of earlier epochs are never looked at.
#[test]
fn old_format_wal_is_unsupported_format_and_stale_sectors_are_ignored() {
    for version in [0u16, 1] {
        let (fs, keys, s2) = sync_sector_damage_fs();
        // Undo the bit flip of that fixture and make sector 1 (s2) an old-format sector with a
        // valid CRC32C instead.
        damage_synced(&fs, "wal_sync.db", |img| {
            img[4096 + 100] ^= 0x01;
            let sec = &mut img[4096..2 * 4096];
            sec[14..16].copy_from_slice(&version.to_le_bytes());
            let crc = crate::util::crc32c::crc32c(&sec[4..]);
            sec[0..4].copy_from_slice(&crc.to_le_bytes());
        });
        assert_fail_open_is_untouched(&fs, WalDamageReason::UnsupportedFormat, s2);
        // The default policy discards the log from the old sector on and reports it.
        let mut e = StorageEngine::open(Arc::clone(&fs), repro_config()).unwrap();
        assert_recovered_prefix(&mut e, &keys, 1);
        let damage = e.last_wal_recovery().damage.clone().expect("damage");
        assert_eq!(
            damage.reason,
            WalDamageReason::UnsupportedFormat,
            "version {version}"
        );
        assert_eq!(damage.first_lost_lsn, s2);
        drop(e);

        // The open rotated both epochs: whatever is left of the old log is now stale, and a
        // clean reopen neither reports nor refuses anything, whatever versions the stale sectors
        // carry.
        let img = content(&fs, "wal_sync.db").unwrap();
        assert!(
            img.len() >= 3 * 4096,
            "setup: stale sectors are still in the file"
        );
        damage_synced(&fs, "wal_sync.db", |img| {
            for i in 0..img.len() / 4096 {
                let sec = &mut img[i * 4096..(i + 1) * 4096];
                if sec.iter().any(|&b| b != 0) {
                    sec[14..16].copy_from_slice(&version.to_le_bytes());
                    let crc = crate::util::crc32c::crc32c(&sec[4..]);
                    sec[0..4].copy_from_slice(&crc.to_le_bytes());
                }
            }
        });
        let e = StorageEngine::open(Arc::clone(&fs), fail_config()).unwrap();
        assert_eq!(e.last_wal_recovery().damage, None, "version {version}");
    }
}

/// A pending group commit replaced a row; an autocommit `put_row` replaces it again and must
/// remove the secondary entries of the group-committed version.
#[test]
fn put_after_a_pending_group_commit_replaces_its_secondary_entries() {
    let fs = Arc::new(CrashFs::new(DirMode::Unix, 1));
    let mut e = StorageEngine::open(Arc::clone(&fs), repro_config()).unwrap();
    let mut rng = Rng::new(7);
    let mut value = || make_value(&mut rng, 2, 8);
    let (old, grouped, newest) = (value(), value(), value());
    e.put_row(2, b"k", &old, &full_keys(2, &old), SYNC).unwrap();
    append_group_put(&mut e, b"k", &grouped, &full_keys(2, &grouped));
    assert!(
        !e.pending_group.is_empty(),
        "the flush must still be pending"
    );
    e.put_row(2, b"k", &newest, &full_keys(2, &newest), BUF)
        .unwrap();
    let want: BTreeSet<Vec<u8>> = full_keys(2, &newest)
        .iter()
        .map(|(id, col)| encode_secondary_index_key(2, *id, col, b"k"))
        .collect();
    let got: BTreeSet<Vec<u8>> = live_index_entries(&mut e, 2).into_iter().collect();
    assert_eq!(got, want, "entries of a superseded version remain");
}

/// `backfill_secondary_keys` must find a row that a pending group commit wrote.
#[test]
fn backfill_after_a_pending_group_commit_finds_its_row() {
    let fs = Arc::new(CrashFs::new(DirMode::Unix, 1));
    let mut e = StorageEngine::open(Arc::clone(&fs), repro_config()).unwrap();
    let mut rng = Rng::new(7);
    let value = make_value(&mut rng, 2, 8);
    append_group_put(&mut e, b"k", &value, &[]);
    assert!(
        !e.pending_group.is_empty(),
        "the flush must still be pending"
    );
    let keys = full_keys(2, &value);
    assert!(
        e.backfill_secondary_keys(2, b"k", &keys, BUF).unwrap(),
        "the row exists"
    );
    assert_eq!(live_index_entries(&mut e, 2).len(), keys.len());
}

/// `compact` and `relocate_fragmented_std_blobs` must drain any pending group commit before
/// scanning `blob_std` pointers, otherwise a relocated stale blob overwrites a group-committed
/// update or resurrects a group-committed delete at a higher LSN.
#[test]
fn compact_after_pending_group_commit_does_not_resurrect_or_overwrite() {
    for is_delete in [false, true] {
        let fs = Arc::new(CrashFs::new(DirMode::Unix, 1));
        let mut cfg = repro_config();
        cfg.inline_threshold_bytes = 64;
        cfg.max_blob_segment_bytes = 2048;
        let mut e = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
        let v1 = vec![0x11; 500];
        let v2 = vec![0x22; 500];
        let v3_old = vec![0x33; 500];
        let v4 = vec![0x44; 700];
        let v3_new = vec![0x77; 500];

        e.put_row(2, b"k1", &v1, &[], SYNC).unwrap();
        e.put_row(2, b"k2", &v2, &[], SYNC).unwrap();
        e.put_row(2, b"k3", &v3_old, &[], SYNC).unwrap();
        // Roll the blob segment so the segment holding k1, k2, k3 is sealed.
        e.put_row(2, b"k4", &v4, &[], SYNC).unwrap();
        // Delete k1 and k2 so the sealed segment has >= 50% dead bytes while k3 remains live.
        e.delete_row(2, b"k1", SYNC).unwrap();
        e.delete_row(2, b"k2", SYNC).unwrap();
        e.flush_memtable().unwrap();
        assert!(e.memtable.is_empty());

        e.begin_transaction_for_conn(1).unwrap();
        if is_delete {
            e.delete_row(2, b"k3", SYNC).unwrap();
        } else {
            e.put_row(2, b"k3", &v3_new, &[], SYNC).unwrap();
        }
        e.prepare_commit_transaction(SYNC).unwrap();
        assert!(matches!(
            e.begin_commit_for_conn(1).unwrap(),
            CommitStep::AwaitingGroup { .. }
        ));
        assert!(!e.pending_group.is_empty());
        assert!(e.memtable.is_empty());

        e.compact().unwrap();

        let want = if is_delete {
            None
        } else {
            Some(v3_new.as_slice())
        };
        assert_eq!(e.get_row(2, b"k3").unwrap().as_deref(), want);
        drop(e);
        let mut e = StorageEngine::open(Arc::clone(&fs), cfg).unwrap();
        assert_eq!(e.get_row(2, b"k3").unwrap().as_deref(), want);
    }
}

/// An autocommit `rename_vtab` must drain any pending group commit before scanning the catalog so
/// an old name entry written by the pending group commit is removed cleanly.
#[test]
fn rename_vtab_after_pending_group_commit_removes_old_catalog_entry() {
    let fs = Arc::new(CrashFs::new(DirMode::Unix, 1));
    let mut e = StorageEngine::open(Arc::clone(&fs), repro_config()).unwrap();

    e.begin_transaction_for_conn(1).unwrap();
    e.register_or_resolve_vtab_with_durability("a", Some(5), SYNC)
        .unwrap();
    e.prepare_commit_transaction(SYNC).unwrap();
    assert!(matches!(
        e.begin_commit_for_conn(1).unwrap(),
        CommitStep::AwaitingGroup { .. }
    ));
    assert!(!e.pending_group.is_empty());

    e.rename_vtab(5, "a", "b", BUF).unwrap();

    let entries: Vec<String> = e
        .scan_merged_prefix(&[0u8, 0u8, b'c'])
        .unwrap()
        .into_iter()
        .filter(|en| !matches!(en.value, IndexValue::Tombstone))
        .map(|en| String::from_utf8_lossy(&en.key[3..]).into_owned())
        .collect();
    assert_eq!(entries, vec!["b".to_string()]);
}

/// After a process crash (`kill -9`), buffered `blob_std` and `blob_lru` tails exist only in
/// the OS page cache. Reopen must sync them before cataloging lengths and checkpointing them into
/// `idx` segments, so a subsequent power cut never leaves `idx` pointing at lost blob bytes.
#[test]
fn process_crash_recovery_syncs_buffered_blobs_before_checkpoint() {
    let fs = Arc::new(CrashFs::new(DirMode::Unix, 1));
    let mut cfg = repro_config();
    cfg.inline_threshold_bytes = 64;
    let mut e = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
    e.register_capped_table(3, 64 * 1024).unwrap();
    let v_std = vec![0xA1; 512];
    let v_lru = vec![0xB2; 512];
    e.put_row(2, b"std_k", &v_std, &[], BUF).unwrap();
    e.put_row(3, b"lru_k", &v_lru, &[], BUF).unwrap();
    drop(e);

    // Process crash preserves OS cache without making unsynced blob writes durable.
    fs.crash(Crash::Process);

    // Recovery replays the buffered WAL, syncs dirty blobs, flushes `idx`, and rotates WALs.
    let mut e = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
    assert_eq!(
        e.get_row(2, b"std_k").unwrap().as_deref(),
        Some(v_std.as_slice())
    );
    assert_eq!(
        e.get_row(3, b"lru_k").unwrap().as_deref(),
        Some(v_lru.as_slice())
    );
    drop(e);

    // A subsequent power cut with zero keep wipes any never-synced OS page-cache bytes.
    fs.crash(Crash::Power { keep: 0, lose: 100 });

    let mut e = StorageEngine::open(Arc::clone(&fs), cfg).unwrap();
    assert_eq!(
        e.get_row(2, b"std_k").unwrap().as_deref(),
        Some(v_std.as_slice())
    );
    assert_eq!(
        e.get_row(3, b"lru_k").unwrap().as_deref(),
        Some(v_lru.as_slice())
    );
}

/// `checkpoint` and `flush_memtable` must drain any pending group commit before draining
/// `pending_corrupt_evictions`, so a queued eviction captured against an older corrupt pointer
/// does not overwrite/delete the healthy value written by the pending group commit.
#[test]
fn checkpoint_and_flush_do_not_evict_row_replaced_by_pending_group_commit() {
    for use_flush in [false, true] {
        let fs = Arc::new(CrashFs::new(DirMode::Unix, 1));
        let mut cfg = repro_config();
        cfg.inline_threshold_bytes = 64;
        let mut e = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
        e.set_table_corrupt_policy(2, CorruptPolicy::Evict);

        let v_bad = vec![0xAA; 512];
        let v_good = vec![0xBB; 512];
        e.put_row(2, b"k", &v_bad, &[], SYNC).unwrap();
        e.flush_memtable().unwrap();

        let blob_name = fs
            .list_files()
            .unwrap()
            .into_iter()
            .find(|f| f.starts_with("blob_std_"))
            .unwrap();
        damage_synced(&fs, &blob_name, |img| img[64] ^= 0xFF);

        // Replace `k` with a healthy value in a pending group commit.
        append_group_put(&mut e, b"k", &v_good, &[]);
        assert!(!e.pending_group.is_empty());

        // A committed reader still sees the old corrupt pointer and queues corrupt eviction.
        assert_eq!(e.reader_for_conn(99).get_row(2, b"k").unwrap(), None);
        assert!(e.has_pending_corrupt_evictions());

        if use_flush {
            e.flush_memtable().unwrap();
        } else {
            e.checkpoint().unwrap();
        }

        assert_eq!(
            e.get_row(2, b"k").unwrap().as_deref(),
            Some(v_good.as_slice())
        );
        drop(e);
        let mut e = StorageEngine::open(Arc::clone(&fs), cfg).unwrap();
        assert_eq!(
            e.get_row(2, b"k").unwrap().as_deref(),
            Some(v_good.as_slice())
        );
    }
}

/// `register_capped_table` committing `table_quotas` to `boot_meta.db` while `wal_buf.db` holds
/// buffered writes/evictions must sync `wal_buf.db` first; otherwise a power cut drops `wal_buf.db`
/// (`replayed_wal == false`), leaving phantom `current_logical_bytes` that bricks future inserts.
#[test]
fn buffered_writes_and_register_capped_table_survive_power_cut_without_phantom_quota() {
    let fs = Arc::new(CrashFs::new(DirMode::Unix, 1));
    let mut cfg = repro_config();
    cfg.inline_threshold_bytes = 4096;
    let mut e = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
    e.register_capped_table(1, 2500).unwrap();
    e.checkpoint().unwrap();

    // Fill table 1 with buffered writes right below quota, then trigger a `boot_meta.db` quota
    // commit via registering table 2 (before `wal_buf.db` was synced).
    let payload = vec![0x42u8; 400];
    for i in 0..5u8 {
        e.put_row(1, &[b'k', i], &payload, &[], BUF).unwrap();
    }
    let buffered_bytes = e.capped_table_state(1).unwrap().current_logical_bytes;
    assert!(buffered_bytes > 1500 && buffered_bytes <= 2500);
    e.register_capped_table(2, 4096).unwrap();

    // Cut I/O and lose all unsynced OS page cache on power loss.
    fs.cut_now();
    drop(e);
    fs.crash(Crash::Power { keep: 0, lose: 100 });

    let mut e = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
    let live_rows_before_evict = e.scan_keys_range(1, None, None, 100).unwrap().len();
    assert_eq!(live_rows_before_evict, 5);
    assert_eq!(
        e.capped_table_state(1).unwrap().current_logical_bytes,
        buffered_bytes
    );

    // Now lower table 1's quota to force BufferedFast eviction + `table_quotas` commit, then crash.
    e.register_capped_table(1, 1200).unwrap();
    let kept_rows = e.scan_keys_range(1, None, None, 100).unwrap().len();
    let kept_bytes = e.capped_table_state(1).unwrap().current_logical_bytes;
    assert!(kept_rows > 0 && kept_rows < 5);
    assert!(kept_bytes <= 1200);

    fs.cut_now();
    drop(e);
    fs.crash(Crash::Power { keep: 0, lose: 100 });

    let mut e = StorageEngine::open(Arc::clone(&fs), cfg).unwrap();
    assert_eq!(
        e.scan_keys_range(1, None, None, 100).unwrap().len(),
        kept_rows
    );
    assert_eq!(
        e.capped_table_state(1).unwrap().current_logical_bytes,
        kept_bytes
    );
    // Inserting a new row must succeed normally rather than being immediately wiped out.
    e.put_row(1, b"fresh", &[0x55u8; 200], &[], SYNC).unwrap();
    assert_eq!(e.get_row(1, b"fresh").unwrap(), Some(vec![0x55u8; 200]));
}

/// If an uncommitted transaction triggers a `RingFull` `flush_memtable_no_auto_compact()` during
/// commit and then fails before its own WAL batch is durable, `boot_meta.db` must persist only the
/// pre-transaction settled `table_quotas`, not the uncommitted transaction's bytes or tables.
#[test]
fn tx_ring_full_flush_persists_settled_quotas_not_uncommitted_tx() {
    let fs = Arc::new(CrashFs::new(DirMode::Unix, 7));
    let mut cfg = repro_config();
    cfg.sync_wal_size_bytes = 6 * 4096;
    cfg.buf_wal_size_bytes = 6 * 4096;
    cfg.memtable_capacity_bytes = 16 * 1024 * 1024;
    cfg.inline_threshold_bytes = 4096;

    let mut e = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
    e.register_capped_table(1, 100_000).unwrap();
    e.set_table_inline_threshold(1, 4096);
    // Consume most of `sync_wal` with small committed rows while keeping MemTable un-flushed.
    for i in 0..4u8 {
        e.put_row(1, &[b'c', i], &[0x11u8; 3000], &[], SYNC)
            .unwrap();
    }
    let settled_bytes = e.capped_table_state(1).unwrap().current_logical_bytes;
    assert!(settled_bytes >= 12_000);
    assert_eq!(e.meta_mgr.state().idx_segments.len(), 0);
    assert_eq!(e.sync_wal.current_sector_idx(), 4);

    // Inside a transaction, register table 2 and add enough rows to overflow `sync_wal` on commit
    // (`RingFull`) without tripping `prepare_commit_transaction`'s `exceeds_wal` pre-grow.
    e.begin_transaction();
    e.register_capped_table(2, 50_000).unwrap();
    for i in 0..4u8 {
        e.put_row(1, &[b'u', i], &[0x22u8; 3000], &[], SYNC)
            .unwrap();
    }
    assert!(e.capped_table_state(1).unwrap().current_logical_bytes > settled_bytes);

    let meta_gen_before = e.meta_mgr.state().generation;
    let wal_sync_nth = fs.op_stats().1[FileKind::WalSync as usize];
    // Fail the retry `WalSync` write immediately after `RingFull` flushes `idx` + `boot_meta.db`.
    fs.arm(None, Some((FileKind::WalSync, wal_sync_nth)));
    assert!(e.commit_transaction().is_err());
    assert!(
        e.meta_mgr.state().generation > meta_gen_before,
        "RingFull must have committed boot_meta.db"
    );
    let quotas = &e.meta_mgr.state().table_quotas;
    assert_eq!(quotas.len(), 1, "uncommitted table 2 leaked into quotas");
    assert_eq!(quotas[0].table_id, 1);
    assert_eq!(quotas[0].current_logical_bytes, settled_bytes);

    fs.cut_now();
    drop(e);
    fs.crash(Crash::Power { keep: 0, lose: 100 });

    let e = StorageEngine::open(Arc::clone(&fs), cfg).unwrap();
    assert_eq!(
        e.capped_table_state(1).unwrap().current_logical_bytes,
        settled_bytes
    );
    assert!(e.capped_table_state(2).is_none());
}
