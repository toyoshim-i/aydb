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
use std::collections::BTreeMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU64;

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
    manifests_seen: AtomicU64,
    blob_segments_seen: AtomicU64,
    fail_by_kind: [AtomicU64; KIND_COUNT],
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
             manifests_seen={} blob_segments_seen={}",
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
            g(&self.manifests_seen),
            g(&self.blob_segments_seen),
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
    trace: Vec<String>,
    /// How many trace lines a failure report keeps (all of them when replaying one seed).
    trace_cap: usize,
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
        Ok(())
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
        let lo = if process_crash {
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
        self.log(format!("recovered at state {k} (range {lo}..={hi})"));
        Ok(k)
    }

    fn open(&mut self, what: &str) -> Result<Eng, String> {
        let e = open_engine(&self.fs, &self.cfg)
            .map_err(|e| self.fail(format!("reopen failed ({what}): {e}")))?;
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
        if !self.ensure_tables(e)? {
            return Ok(());
        }
        for _ in 0..nops {
            let r = self.rng.below(100);
            let go = if r < 36 {
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
                true
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
        let t = TABLES[self.rng.below(4) as usize];
        let k = self.rng.below(KEYS as u64) as u8;
        let d = pick_durability(&mut self.rng);
        let sync = d == WriteDurability::SyncTransactional;
        let v = make_value(&mut self.rng, t, k);
        let mut ns = self.last().clone();
        ns.insert((t, k), v.clone());
        let what = format!("put t{t}/key{k:02} {} {}", short(&v), describe(d));
        let lsn = e.next_lsn();
        let r = e.put_row(t, &key_bytes(k), &v, &[], d);
        let logged = e.next_lsn() != lsn;
        let alive = !e.is_poisoned();
        self.record_commit(r, ns, sync, logged, alive, &what)
    }

    fn op_delete(&mut self, e: &mut Eng) -> Result<bool, String> {
        let t = TABLES[self.rng.below(4) as usize];
        let k = self.rng.below(KEYS as u64) as u8;
        let d = pick_durability(&mut self.rng);
        let mut ns = self.last().clone();
        ns.remove(&(t, k));
        let what = format!("delete t{t}/key{k:02} {}", describe(d));
        let lsn = e.next_lsn();
        let r = e.delete_row(t, &key_bytes(k), d).map(|_| ());
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
        e.begin_transaction();
        self.log("begin".into());
        let mut work = self.last().clone();
        let mut sps: Vec<(i32, Table)> = Vec::new();
        let mut next_sp = 1;
        let steps = 1 + self.rng.below(7);
        for _ in 0..steps {
            let r = self.rng.below(100);
            if r < 60 {
                let t = TABLES[self.rng.below(4) as usize];
                let k = self.rng.below(KEYS as u64) as u8;
                let v = make_value(&mut self.rng, t, k);
                let d = pick_durability(&mut self.rng);
                let res = e.put_row(t, &key_bytes(k), &v, &[], d);
                self.log(format!(
                    "  tx put t{t}/key{k:02} {} {}",
                    short(&v),
                    describe(d)
                ));
                if self.io_ok(res, "put_row in transaction")?.is_none() {
                    return self.abort_tx(e);
                }
                work.insert((t, k), v);
            } else if r < 76 {
                let t = TABLES[self.rng.below(4) as usize];
                let k = self.rng.below(KEYS as u64) as u8;
                let d = pick_durability(&mut self.rng);
                let res = e.delete_row(t, &key_bytes(k), d);
                self.log(format!("  tx delete t{t}/key{k:02}"));
                if self.io_ok(res, "delete_row in transaction")?.is_none() {
                    return self.abort_tx(e);
                }
                work.remove(&(t, k));
            } else if r < 84 {
                e.savepoint(next_sp);
                self.log(format!("  savepoint {next_sp}"));
                sps.push((next_sp, work.clone()));
                next_sp += 1;
            } else if r < 92 {
                if let Some(i) = sps
                    .len()
                    .checked_sub(1)
                    .map(|n| self.rng.below(n as u64 + 1) as usize)
                {
                    let (id, snap) = sps[i].clone();
                    let res = e.rollback_to_savepoint(id);
                    self.log(format!("  rollback to savepoint {id}"));
                    if self.io_ok(res, "rollback_to_savepoint")?.is_none() {
                        return self.abort_tx(e);
                    }
                    work = snap;
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
        let r = e.commit_transaction();
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
    plan: Option<&[EpochStat]>,
    cov: &Coverage,
) -> Result<Vec<EpochStat>, String> {
    run_case_inner(seed, plan, cov)
}

fn run_case_inner(
    seed: u64,
    plan: Option<&[EpochStat]>,
    cov: &Coverage,
) -> Result<Vec<EpochStat>, String> {
    let mut master = Rng::new(seed);
    let mode = if master.chance(1, 2) {
        DirMode::Unix
    } else {
        DirMode::Windows
    };
    let fs = Arc::new(CrashFs::new(mode, master.next()));
    let cfg = engine_config(&mut master);
    let epochs = 1 + master.below(3) as usize;
    let wide = if master.chance(1, 4) {
        WIDE_TABLES_MIN + master.below(26) as u16
    } else {
        0
    };
    let quota3 = 3200 + master.below(2500);
    let quota4 = 6000 + master.below(6000);
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
        trace: Vec::new(),
        trace_cap: if env_u64("AYDB_CRASH_SEED").is_some() {
            usize::MAX
        } else {
            64
        },
    };
    h.log(format!(
        "config: {mode:?} epochs={epochs} wide={wide} sync_wal={} buf_wal={} memtable={} blob_seg={} compact_at={}",
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
        fs.crash(crash);
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
            let r = open_engine(&fs, &h.cfg);
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
    h.log("post-recovery maintenance".into());
    let r = catch_unwind(AssertUnwindSafe(|| -> Result<(), String> {
        h.run_segment(&mut e, 8)?;
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
fn run_seed(seed: u64, cov: &Coverage) -> Result<(), String> {
    let stats = run_case(seed, None, cov)?;
    run_case(seed, Some(&stats), cov)?;
    Ok(())
}

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name).ok().and_then(|s| s.trim().parse().ok())
}

/// Runs `count` seeds from `start` on a few threads. Returns the failure messages.
fn run_seeds(start: u64, count: u64, cov: &Coverage) -> Vec<String> {
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
                    let res = catch_unwind(AssertUnwindSafe(|| run_seed(seed, cov)));
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

#[test]
fn crash_consistency_seeds() {
    let cov = Coverage::default();
    let (start, count) = match env_u64("AYDB_CRASH_SEED") {
        Some(one) => (one, 1),
        None => (
            env_u64("AYDB_CRASH_SEED_START").unwrap_or(0),
            env_u64("AYDB_CRASH_SEEDS").unwrap_or(DEFAULT_SEEDS),
        ),
    };
    let t0 = std::time::Instant::now();
    let failures = run_seeds(start, count, &cov);
    eprintln!(
        "crash-consistency: seeds {start}..{} in {:.1}s, {} failure(s)\n  coverage: {}",
        start + count,
        t0.elapsed().as_secs_f64(),
        failures.len(),
        cov.report()
    );
    if !failures.is_empty() {
        let shown: Vec<&String> = failures.iter().take(200).collect();
        panic!(
            "{} of {count} seeds failed (replay one with AYDB_CRASH_SEED=<seed>):\n{}",
            failures.len(),
            shown
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
                .join("\n----\n")
        );
    }
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
        let s1 = run_case(seed, None, &a).unwrap();
        let s2 = run_case(seed, None, &b).unwrap();
        assert_eq!(s1.len(), s2.len());
        for (x, y) in s1.iter().zip(&s2) {
            assert_eq!(x.ops, y.ops, "seed {seed} is not deterministic");
            assert_eq!(x.kind_ops, y.kind_ops);
        }
        let s3 = run_case(seed, Some(&s1), &a).unwrap();
        let s4 = run_case(seed, Some(&s1), &b).unwrap();
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

/// The rowid migration of a legacy table writes its mappings buffered and only the completion
/// marker with the table's durability. A power cut at every operation of a synchronous migration
/// must leave either a marker with all of its mappings or an unfinished migration that resumes;
/// mappings that survived keep their rowids, and an acknowledged migration is never lost.
#[test]
fn power_cut_during_rowid_migration_resumes_with_stable_rowids() {
    use crate::vtab::rowid_map::ensure_persistent_rowids;
    use crate::vtab::{SqlValue, encode_row};

    const ROWS: usize = 12;
    let sync = WriteDurability::SyncTransactional;
    let pks: Vec<Vec<u8>> = (0..ROWS)
        .map(|i| SqlValue::Text(format!("key{i:03}")).to_key_bytes())
        .collect();
    let cfg = repro_config();

    // Returns the number of operations the migration issued when it ran to completion.
    let scenario = |seed: u64, cut: Option<u64>, keep: u64, lose: u64| -> u64 {
        let fs = Arc::new(CrashFs::new(DirMode::Unix, seed));
        let mut e = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
        for (i, pk) in pks.iter().enumerate() {
            let row = encode_row(&[SqlValue::Text(format!("key{i:03}")), SqlValue::Integer(0)]);
            e.put_row(1, pk, &row, &[], sync).unwrap();
        }
        let start = fs.op_stats().0;
        fs.arm(cut.map(|c| start + c), None);
        let acknowledged = ensure_persistent_rowids(&mut e, 1, sync, false).is_ok();
        let span = fs.op_stats().0 - start;
        fs.cut_now();
        drop(e);
        fs.crash(Crash::Power { keep, lose });

        let mut e = StorageEngine::open(Arc::clone(&fs), cfg.clone()).unwrap();
        let initialized = e.rowids_initialized(1).unwrap();
        assert!(
            initialized || !acknowledged,
            "acknowledged migration lost (seed {seed}, cut {cut:?})"
        );
        let mut kept = Vec::new();
        for pk in &pks {
            let rowid = e.rowid_for_pk_view(ReadView::Owner, 1, pk).unwrap();
            assert!(
                rowid.is_some() || !initialized,
                "marker without a mapping (seed {seed}, cut {cut:?})"
            );
            kept.push(rowid);
        }
        // The migration resumes; surviving identities do not change.
        ensure_persistent_rowids(&mut e, 1, sync, false).unwrap();
        assert!(e.rowids_initialized(1).unwrap());
        let mut seen = std::collections::BTreeSet::new();
        for (pk, before) in pks.iter().zip(&kept) {
            let rowid = e
                .rowid_for_pk_view(ReadView::Owner, 1, pk)
                .unwrap()
                .expect("every row has a rowid after the resumed migration");
            assert!(before.is_none_or(|b| b == rowid), "rowid changed");
            assert_eq!(
                e.pk_for_rowid_view(ReadView::Owner, 1, rowid).unwrap(),
                Some(pk.clone())
            );
            assert!(seen.insert(rowid), "duplicate rowid");
        }
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
