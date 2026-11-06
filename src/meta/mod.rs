//! Layer 2 (Part 1): Atomic Boot Metadata (`boot_meta.db`), Hybrid External Manifests,
//! and Rename-Free Deferred Garbage Collection.
//!
//! See `docs/phase1/02_BOOT_META_AND_GC.md` for the detailed binary specification and
//! crash-recovery state machine.

use std::collections::BTreeSet;
use std::io;
use std::sync::Arc;

use crate::pal::{OpenHint, PlatformFile, PlatformFs, PreallocStrategy};
use crate::util::crc32c::crc32c;

/// Fixed filename for the 8 KB ping-pong boot metadata file.
pub const BOOT_META_FILE_NAME: &str = "boot_meta.db";

/// Size of a single ping-pong slot (4 KB).
pub const SLOT_SIZE: usize = 4096;

/// Total size of `boot_meta.db` (Slot A + Slot B = 8 KB).
pub const BOOT_META_FILE_SIZE: u64 = (SLOT_SIZE * 2) as u64;

/// Fixed header size inside a 4 KB slot (`0..64`).
const SLOT_HEADER_SIZE: usize = 64;

/// Size of the trailing slot checksum (`4092..4096`).
const SLOT_TRAILER_SIZE: usize = 4;

/// Maximum inline catalog payload capacity inside a single 4 KB slot (`4028` bytes).
pub const MAX_INLINE_PAYLOAD_LEN: usize = SLOT_SIZE - SLOT_HEADER_SIZE - SLOT_TRAILER_SIZE;

const BOOT_META_MAGIC: &[u8; 4] = b"AYDB";
const MANIFEST_MAGIC: &[u8; 4] = b"AYMF";
const FORMAT_VERSION: u16 = 1;

/// Flag indicating that the catalog payload is stored in an external `manifest_<SeqID>.db` file.
const FLAG_EXTERNAL_MANIFEST: u16 = 0x0001;

const EXTERNAL_POINTER_LEN: usize = 20;
const MANIFEST_HEADER_LEN: usize = 16;

/// Formats an immutable index segment filename (`idx_<16-hex-digit SeqID>.db`).
pub fn format_idx_file_name(seq_id: u64) -> String {
    format!("idx_{seq_id:016x}.db")
}

/// Formats a standard append-only blob segment filename (`blob_std_<16-hex-digit SeqID>.db`).
pub fn format_std_blob_file_name(seq_id: u64) -> String {
    format!("blob_std_{seq_id:016x}.db")
}

/// Formats a per-table LRU blob segment filename (`blob_lru_<4-hex-digit TblID>_<16-hex-digit SeqID>.db`).
pub fn format_lru_blob_file_name(table_id: u16, seq_id: u64) -> String {
    format!("blob_lru_{table_id:04x}_{seq_id:016x}.db")
}

/// Formats an overflow manifest filename (`manifest_<16-hex-digit SeqID>.db`).
pub fn format_manifest_file_name(seq_id: u64) -> String {
    format!("manifest_{seq_id:016x}.db")
}

/// Returns `true` if `file_name` is an immutable segment or manifest managed by `BootMetaState`.
fn is_managed_segment_or_manifest(file_name: &str) -> bool {
    file_name.ends_with(".db")
        && (file_name.starts_with("idx_")
            || file_name.starts_with("blob_std_")
            || file_name.starts_with("blob_lru_")
            || file_name.starts_with("manifest_"))
}

/// Metadata descriptor for an active immutable index segment (`idx_<SeqID>.db`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdxSegmentMeta {
    pub seq_id: u64,
    pub file_len: u64,
    pub level: u16,
    pub record_count: u32,
}

/// Metadata descriptor for an active standard blob segment (`blob_std_<SeqID>.db`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StdBlobSegmentMeta {
    pub seq_id: u64,
    pub file_len: u64,
    pub dead_bytes: u64,
}

/// Metadata descriptor for an active per-table LRU blob segment (`blob_lru_<TblID>_<SeqID>.db`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LruBlobSegmentMeta {
    pub seq_id: u64,
    pub file_len: u64,
    pub dead_bytes: u64,
    pub table_id: u16,
}

/// Eviction policy for a table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum EvictionPolicy {
    None = 0,
    Lru = 1,
}

impl EvictionPolicy {
    pub fn from_u8(v: u8) -> io::Result<Self> {
        match v {
            0 => Ok(Self::None),
            1 => Ok(Self::Lru),
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid EvictionPolicy discriminant: {other}"),
            )),
        }
    }
}

/// Per-table logical size quota and live byte accounting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableQuotaMeta {
    pub table_id: u16,
    pub eviction_policy: EvictionPolicy,
    pub max_bytes: u64,
    pub current_logical_bytes: u64,
}

/// Authoritative persistent state stored in `boot_meta.db` (and optional overflow manifest).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootMetaState {
    /// Strictly monotonic commit generation (`1, 2, 3, ...`).
    pub generation: u64,
    /// Next available monotonically increasing sequence ID for segment/manifest files.
    pub next_seq_id: u64,
    /// Highest `LSN` already flushed into immutable segments.
    pub checkpoint_lsn: u64,
    /// Active epoch number for `wal_sync.db`.
    pub sync_wal_epoch: u64,
    /// Active epoch number for `wal_buf.db`.
    pub buf_wal_epoch: u64,
    /// Active immutable index segments (`idx_<SeqID>.db`), ordered newest-first or by level.
    pub idx_segments: Vec<IdxSegmentMeta>,
    /// Active standard blob segments (`blob_std_<SeqID>.db`).
    pub std_blob_segments: Vec<StdBlobSegmentMeta>,
    /// Active per-table LRU blob segments (`blob_lru_<TblID>_<SeqID>.db`).
    pub lru_blob_segments: Vec<LruBlobSegmentMeta>,
    /// Table quota configurations and logical byte counters.
    pub table_quotas: Vec<TableQuotaMeta>,
    /// Filenames of retired segments/manifests awaiting deletion (deferred GC queue).
    pub pending_delete_files: Vec<String>,
    /// Sequence ID of the external manifest file currently backing this state, if overflowed.
    pub active_manifest_seq_id: Option<u64>,
}

impl Default for BootMetaState {
    fn default() -> Self {
        Self {
            generation: 1,
            next_seq_id: 1,
            checkpoint_lsn: 0,
            sync_wal_epoch: 1,
            buf_wal_epoch: 1,
            idx_segments: Vec::new(),
            std_blob_segments: Vec::new(),
            lru_blob_segments: Vec::new(),
            table_quotas: Vec::new(),
            pending_delete_files: Vec::new(),
            active_manifest_seq_id: None,
        }
    }
}

impl BootMetaState {
    /// Allocates and returns a fresh, monotonically increasing `SeqID`.
    pub fn allocate_seq_id(&mut self) -> u64 {
        let id = self.next_seq_id;
        self.next_seq_id = self
            .next_seq_id
            .checked_add(1)
            .expect("64-bit SeqID overflow");
        id
    }

    /// Queues a filename for deferred deletion if not already queued.
    pub fn queue_pending_delete(&mut self, file_name: String) {
        if !self.pending_delete_files.contains(&file_name) {
            self.pending_delete_files.push(file_name);
        }
    }

    /// Returns the set of all segment and manifest filenames actively referenced by this state.
    pub fn referenced_file_names(&self) -> BTreeSet<String> {
        let mut set = BTreeSet::new();
        for seg in &self.idx_segments {
            set.insert(format_idx_file_name(seg.seq_id));
        }
        for seg in &self.std_blob_segments {
            set.insert(format_std_blob_file_name(seg.seq_id));
        }
        for seg in &self.lru_blob_segments {
            set.insert(format_lru_blob_file_name(seg.table_id, seg.seq_id));
        }
        if let Some(mf_id) = self.active_manifest_seq_id {
            set.insert(format_manifest_file_name(mf_id));
        }
        set
    }

    /// Serializes the variable-length catalog lists into a deterministic byte vector.
    pub(crate) fn encode_catalog_payload(&self) -> io::Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(256);

        // 1. Index segments (24B each)
        buf.extend_from_slice(&(self.idx_segments.len() as u32).to_le_bytes());
        for seg in &self.idx_segments {
            buf.extend_from_slice(&seg.seq_id.to_le_bytes());
            buf.extend_from_slice(&seg.file_len.to_le_bytes());
            buf.extend_from_slice(&seg.level.to_le_bytes());
            buf.extend_from_slice(&0u16.to_le_bytes()); // reserved
            buf.extend_from_slice(&seg.record_count.to_le_bytes());
        }

        // 2. Standard blob segments (24B each)
        buf.extend_from_slice(&(self.std_blob_segments.len() as u32).to_le_bytes());
        for seg in &self.std_blob_segments {
            buf.extend_from_slice(&seg.seq_id.to_le_bytes());
            buf.extend_from_slice(&seg.file_len.to_le_bytes());
            buf.extend_from_slice(&seg.dead_bytes.to_le_bytes());
        }

        // 3. LRU blob segments (32B each)
        buf.extend_from_slice(&(self.lru_blob_segments.len() as u32).to_le_bytes());
        for seg in &self.lru_blob_segments {
            buf.extend_from_slice(&seg.seq_id.to_le_bytes());
            buf.extend_from_slice(&seg.file_len.to_le_bytes());
            buf.extend_from_slice(&seg.dead_bytes.to_le_bytes());
            buf.extend_from_slice(&seg.table_id.to_le_bytes());
            buf.extend_from_slice(&[0u8; 6]); // reserved
        }

        // 4. Table quotas (24B each)
        buf.extend_from_slice(&(self.table_quotas.len() as u32).to_le_bytes());
        for q in &self.table_quotas {
            buf.extend_from_slice(&q.table_id.to_le_bytes());
            buf.push(q.eviction_policy as u8);
            buf.extend_from_slice(&[0u8; 5]); // reserved
            buf.extend_from_slice(&q.max_bytes.to_le_bytes());
            buf.extend_from_slice(&q.current_logical_bytes.to_le_bytes());
        }

        // 5. Pending delete filenames
        buf.extend_from_slice(&(self.pending_delete_files.len() as u32).to_le_bytes());
        for name in &self.pending_delete_files {
            let bytes = name.as_bytes();
            let len_u16 = u16::try_from(bytes.len()).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "pending delete filename exceeds u16::MAX",
                )
            })?;
            buf.extend_from_slice(&len_u16.to_le_bytes());
            buf.extend_from_slice(bytes);
        }

        Ok(buf)
    }

    /// Deserializes the catalog lists from `payload` into `self`.
    pub(crate) fn decode_catalog_payload(&mut self, mut payload: &[u8]) -> io::Result<()> {
        fn read_bytes<'a>(input: &mut &'a [u8], n: usize) -> io::Result<&'a [u8]> {
            if input.len() < n {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "truncated catalog payload in boot_meta",
                ));
            }
            let (head, tail) = input.split_at(n);
            *input = tail;
            Ok(head)
        }

        fn read_u16(input: &mut &[u8]) -> io::Result<u16> {
            let b = read_bytes(input, 2)?;
            Ok(u16::from_le_bytes([b[0], b[1]]))
        }

        fn read_u32(input: &mut &[u8]) -> io::Result<u32> {
            let b = read_bytes(input, 4)?;
            Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
        }

        fn read_u64(input: &mut &[u8]) -> io::Result<u64> {
            let b = read_bytes(input, 8)?;
            Ok(u64::from_le_bytes([
                b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
            ]))
        }

        // 1. Index segments
        let num_idx = read_u32(&mut payload)? as usize;
        if num_idx.saturating_mul(24) > payload.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "corrupted idx_segments count",
            ));
        }
        let mut idx_segments = Vec::with_capacity(num_idx);
        for _ in 0..num_idx {
            let seq_id = read_u64(&mut payload)?;
            let file_len = read_u64(&mut payload)?;
            let level = read_u16(&mut payload)?;
            let _reserved = read_u16(&mut payload)?;
            let record_count = read_u32(&mut payload)?;
            idx_segments.push(IdxSegmentMeta {
                seq_id,
                file_len,
                level,
                record_count,
            });
        }

        // 2. Standard blob segments
        let num_std_blob = read_u32(&mut payload)? as usize;
        if num_std_blob.saturating_mul(24) > payload.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "corrupted std_blob_segments count",
            ));
        }
        let mut std_blob_segments = Vec::with_capacity(num_std_blob);
        for _ in 0..num_std_blob {
            let seq_id = read_u64(&mut payload)?;
            let file_len = read_u64(&mut payload)?;
            let dead_bytes = read_u64(&mut payload)?;
            std_blob_segments.push(StdBlobSegmentMeta {
                seq_id,
                file_len,
                dead_bytes,
            });
        }

        // 3. LRU blob segments
        let num_lru_blob = read_u32(&mut payload)? as usize;
        if num_lru_blob.saturating_mul(32) > payload.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "corrupted lru_blob_segments count",
            ));
        }
        let mut lru_blob_segments = Vec::with_capacity(num_lru_blob);
        for _ in 0..num_lru_blob {
            let seq_id = read_u64(&mut payload)?;
            let file_len = read_u64(&mut payload)?;
            let dead_bytes = read_u64(&mut payload)?;
            let table_id = read_u16(&mut payload)?;
            let _reserved = read_bytes(&mut payload, 6)?;
            lru_blob_segments.push(LruBlobSegmentMeta {
                seq_id,
                file_len,
                dead_bytes,
                table_id,
            });
        }

        // 4. Table quotas
        let num_quotas = read_u32(&mut payload)? as usize;
        if num_quotas.saturating_mul(24) > payload.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "corrupted table_quotas count",
            ));
        }
        let mut table_quotas = Vec::with_capacity(num_quotas);
        for _ in 0..num_quotas {
            let table_id = read_u16(&mut payload)?;
            let policy_byte = read_bytes(&mut payload, 1)?[0];
            let eviction_policy = EvictionPolicy::from_u8(policy_byte)?;
            let _reserved = read_bytes(&mut payload, 5)?;
            let max_bytes = read_u64(&mut payload)?;
            let current_logical_bytes = read_u64(&mut payload)?;
            table_quotas.push(TableQuotaMeta {
                table_id,
                eviction_policy,
                max_bytes,
                current_logical_bytes,
            });
        }

        // 5. Pending deletes
        let num_deletes = read_u32(&mut payload)? as usize;
        let mut pending_delete_files = Vec::with_capacity(num_deletes.min(1024));
        for _ in 0..num_deletes {
            let name_len = read_u16(&mut payload)? as usize;
            let name_bytes = read_bytes(&mut payload, name_len)?;
            let name = std::str::from_utf8(name_bytes).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid UTF-8 in pending_delete_files",
                )
            })?;
            pending_delete_files.push(name.to_owned());
        }

        if !payload.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unexpected trailing bytes in boot_meta catalog payload",
            ));
        }

        self.idx_segments = idx_segments;
        self.std_blob_segments = std_blob_segments;
        self.lru_blob_segments = lru_blob_segments;
        self.table_quotas = table_quotas;
        self.pending_delete_files = pending_delete_files;

        Ok(())
    }
}

/// Identifies which of the two 4 KB slots in `boot_meta.db` is being addressed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotIndex {
    SlotA = 0,
    SlotB = 1,
}

impl SlotIndex {
    #[inline]
    pub fn byte_offset(self) -> u64 {
        match self {
            Self::SlotA => 0,
            Self::SlotB => SLOT_SIZE as u64,
        }
    }

    #[inline]
    pub fn opposite(self) -> Self {
        match self {
            Self::SlotA => Self::SlotB,
            Self::SlotB => Self::SlotA,
        }
    }
}

/// Coordinates atomic ping-pong updates to `boot_meta.db`, external manifest overflow,
/// and rename-free deferred garbage collection.
pub struct BootMetaManager<F: PlatformFs> {
    fs: Arc<F>,
    file: Arc<F::File>,
    active_slot: SlotIndex,
    state: BootMetaState,
}

impl<F: PlatformFs> BootMetaManager<F> {
    /// Opens `boot_meta.db` (creating and pre-allocating it if missing), runs the 4-state
    /// crash-recovery truth table across Slot A and Slot B, and sweeps orphaned or pending-delete
    /// segment files.
    pub fn open_or_recover(fs: Arc<F>) -> io::Result<Self> {
        let file = fs.open_file(BOOT_META_FILE_NAME, true, true, OpenHint::MutableSyncLog)?;
        if file.file_len()? < BOOT_META_FILE_SIZE {
            file.preallocate(BOOT_META_FILE_SIZE, PreallocStrategy::PhysicalZeroFill)?;
        }

        let mut raw = [0u8; SLOT_SIZE * 2];
        file.read_exact_at(&mut raw, 0)?;

        let slot_a_raw: &[u8; SLOT_SIZE] = raw[..SLOT_SIZE].try_into().unwrap();
        let slot_b_raw: &[u8; SLOT_SIZE] = raw[SLOT_SIZE..].try_into().unwrap();

        let a_all_zero = slot_a_raw.iter().all(|&b| b == 0);
        let b_all_zero = slot_b_raw.iter().all(|&b| b == 0);

        if a_all_zero && b_all_zero {
            // Case 3: Newly created database (or crash prior to first metadata commit).
            let initial_state = BootMetaState::default();
            let mut mgr = Self {
                fs,
                file,
                // Set active_slot to SlotB so the initial write below lands in SlotA (offset 0).
                active_slot: SlotIndex::SlotB,
                state: initial_state,
            };
            mgr.write_current_state_to_slot(SlotIndex::SlotA)?;
            mgr.active_slot = SlotIndex::SlotA;
            mgr.sweep_orphans_and_pending_deletes(None)?;
            return Ok(mgr);
        }

        let parsed_a = Self::parse_and_load_slot(&fs, slot_a_raw);
        let parsed_b = Self::parse_and_load_slot(&fs, slot_b_raw);

        let (winning_slot, winning_state, standby_manifest_id) = match (parsed_a, parsed_b) {
            (Ok(state_a), Ok(state_b)) => {
                // Case 1: Both slots are valid; select the higher generation.
                if state_b.generation > state_a.generation {
                    let standby_mf = state_a.active_manifest_seq_id;
                    (SlotIndex::SlotB, state_b, standby_mf)
                } else {
                    let standby_mf = state_b.active_manifest_seq_id;
                    (SlotIndex::SlotA, state_a, standby_mf)
                }
            }
            (Ok(state_a), Err(_)) => {
                // Case 2a: Slot A valid, Slot B torn/uninitialized.
                (SlotIndex::SlotA, state_a, None)
            }
            (Err(_), Ok(state_b)) => {
                // Case 2b: Slot B valid, Slot A torn/uninitialized.
                (SlotIndex::SlotB, state_b, None)
            }
            (Err(err_a), Err(_err_b)) => {
                // Case 4: Both slots contain non-zero bytes and both failed validation.
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("boot_meta.db corruption: both Slot A and Slot B are invalid ({err_a})"),
                ));
            }
        };

        let mut mgr = Self {
            fs,
            file,
            active_slot: winning_slot,
            state: winning_state,
        };

        mgr.sweep_orphans_and_pending_deletes(standby_manifest_id)?;
        Ok(mgr)
    }

    /// Returns a shared reference to the current authoritative [`BootMetaState`].
    pub fn state(&self) -> &BootMetaState {
        &self.state
    }

    /// Returns which ping-pong slot currently holds the latest committed state.
    pub fn active_slot(&self) -> SlotIndex {
        self.active_slot
    }

    /// Atomically commits a mutated [`BootMetaState`] by incrementing `generation`,
    /// writing to the opposite ping-pong slot (and an external `manifest_<SeqID>.db` if
    /// the catalog payload exceeds 4028 bytes), flushing to non-volatile storage, and
    /// executing a deferred GC pass over `pending_delete_files`.
    pub fn commit_update<M>(&mut self, mutate: M) -> io::Result<()>
    where
        M: FnOnce(&mut BootMetaState),
    {
        let mut next_state = self.state.clone();
        let prev_manifest_id = next_state.active_manifest_seq_id;

        mutate(&mut next_state);
        next_state.generation = self
            .state
            .generation
            .checked_add(1)
            .expect("64-bit generation overflow");

        let target_slot = self.active_slot.opposite();
        Self::write_state_to_slot_internal(&self.fs, &self.file, target_slot, &mut next_state)?;

        // If the previous generation used an external manifest that is no longer the active one,
        // queue the old manifest file for deferred deletion.
        if let Some(old_mf_id) = prev_manifest_id {
            if next_state.active_manifest_seq_id != Some(old_mf_id) {
                next_state.queue_pending_delete(format_manifest_file_name(old_mf_id));
            }
        }

        self.active_slot = target_slot;
        self.state = next_state;

        // Best-effort deletion pass over pending_delete_files.
        self.try_process_pending_deletes();
        Ok(())
    }

    /// Attempts to delete all files currently listed in `state.pending_delete_files`.
    /// Any file that fails deletion (e.g., due to `ERROR_SHARING_VIOLATION` from antivirus
    /// scanning or an open handle) remains in `pending_delete_files` for future retry.
    pub fn try_process_pending_deletes(&mut self) {
        let mut remaining = Vec::new();
        for file_name in self.state.pending_delete_files.drain(..) {
            if self.fs.remove_file(&file_name).is_err() {
                remaining.push(file_name);
            }
        }
        self.state.pending_delete_files = remaining;
    }

    fn write_current_state_to_slot(&mut self, slot: SlotIndex) -> io::Result<()> {
        Self::write_state_to_slot_internal(&self.fs, &self.file, slot, &mut self.state)
    }

    fn write_state_to_slot_internal(
        fs: &Arc<F>,
        file: &Arc<F::File>,
        slot: SlotIndex,
        state: &mut BootMetaState,
    ) -> io::Result<()> {
        let catalog_bytes = state.encode_catalog_payload()?;

        let (flags, slot_payload_bytes) = if catalog_bytes.len() <= MAX_INLINE_PAYLOAD_LEN {
            state.active_manifest_seq_id = None;
            (0u16, catalog_bytes)
        } else {
            // Overflow to an immutable external manifest file FIRST before updating boot_meta.db!
            let mf_seq_id = state.allocate_seq_id();
            let mf_name = format_manifest_file_name(mf_seq_id);
            let mf_payload_crc = crc32c(&catalog_bytes);
            let mf_total_len = (MANIFEST_HEADER_LEN + catalog_bytes.len()) as u64;

            let mut mf_buf = Vec::with_capacity(mf_total_len as usize);
            mf_buf.extend_from_slice(MANIFEST_MAGIC);
            mf_buf.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
            mf_buf.extend_from_slice(&0u16.to_le_bytes()); // reserved
            mf_buf.extend_from_slice(&(catalog_bytes.len() as u32).to_le_bytes());
            mf_buf.extend_from_slice(&mf_payload_crc.to_le_bytes());
            mf_buf.extend_from_slice(&catalog_bytes);

            let mf_file = fs.open_file(&mf_name, true, true, OpenHint::ImmutableSequential)?;
            mf_file.write_all_at(&mf_buf, 0)?;
            mf_file.sync_durable()?;
            drop(mf_file);

            state.active_manifest_seq_id = Some(mf_seq_id);

            let mut ptr_buf = Vec::with_capacity(EXTERNAL_POINTER_LEN);
            ptr_buf.extend_from_slice(&mf_seq_id.to_le_bytes());
            ptr_buf.extend_from_slice(&mf_total_len.to_le_bytes());
            ptr_buf.extend_from_slice(&mf_payload_crc.to_le_bytes());

            (FLAG_EXTERNAL_MANIFEST, ptr_buf)
        };

        let payload_len = slot_payload_bytes.len() as u32;
        let payload_crc = crc32c(&slot_payload_bytes);

        let mut slot_buf = [0u8; SLOT_SIZE];
        slot_buf[0..4].copy_from_slice(BOOT_META_MAGIC);
        slot_buf[4..6].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
        slot_buf[6..8].copy_from_slice(&flags.to_le_bytes());
        slot_buf[8..16].copy_from_slice(&state.generation.to_le_bytes());
        slot_buf[16..24].copy_from_slice(&state.next_seq_id.to_le_bytes());
        slot_buf[24..32].copy_from_slice(&state.checkpoint_lsn.to_le_bytes());
        slot_buf[32..40].copy_from_slice(&state.sync_wal_epoch.to_le_bytes());
        slot_buf[40..48].copy_from_slice(&state.buf_wal_epoch.to_le_bytes());
        slot_buf[48..52].copy_from_slice(&payload_len.to_le_bytes());
        slot_buf[52..56].copy_from_slice(&payload_crc.to_le_bytes());
        slot_buf[56..60].copy_from_slice(&0u32.to_le_bytes()); // reserved

        let header_crc = crc32c(&slot_buf[0..60]);
        slot_buf[60..64].copy_from_slice(&header_crc.to_le_bytes());

        slot_buf[64..64 + slot_payload_bytes.len()].copy_from_slice(&slot_payload_bytes);

        let trailer_crc = crc32c(&slot_buf[0..4092]);
        slot_buf[4092..4096].copy_from_slice(&trailer_crc.to_le_bytes());

        file.write_all_at(&slot_buf, slot.byte_offset())?;
        file.sync_durable()?;
        Ok(())
    }

    fn parse_and_load_slot(
        fs: &Arc<F>,
        slot_buf: &[u8; SLOT_SIZE],
    ) -> io::Result<BootMetaState> {
        if &slot_buf[0..4] != BOOT_META_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid magic in boot_meta slot",
            ));
        }

        let expected_header_crc = u32::from_le_bytes([
            slot_buf[60],
            slot_buf[61],
            slot_buf[62],
            slot_buf[63],
        ]);
        let actual_header_crc = crc32c(&slot_buf[0..60]);
        if actual_header_crc != expected_header_crc {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "header CRC32C mismatch in boot_meta slot",
            ));
        }

        let expected_trailer_crc = u32::from_le_bytes([
            slot_buf[4092],
            slot_buf[4093],
            slot_buf[4094],
            slot_buf[4095],
        ]);
        let actual_trailer_crc = crc32c(&slot_buf[0..4092]);
        if actual_trailer_crc != expected_trailer_crc {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "trailer CRC32C mismatch in boot_meta slot (sub-sector torn write)",
            ));
        }

        let version = u16::from_le_bytes([slot_buf[4], slot_buf[5]]);
        if version != FORMAT_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported boot_meta format version: {version}"),
            ));
        }

        let flags = u16::from_le_bytes([slot_buf[6], slot_buf[7]]);
        if (flags & !FLAG_EXTERNAL_MANIFEST) != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown boot_meta flags: {flags:#x}"),
            ));
        }

        let generation = u64::from_le_bytes(slot_buf[8..16].try_into().unwrap());
        let next_seq_id = u64::from_le_bytes(slot_buf[16..24].try_into().unwrap());
        let checkpoint_lsn = u64::from_le_bytes(slot_buf[24..32].try_into().unwrap());
        let sync_wal_epoch = u64::from_le_bytes(slot_buf[32..40].try_into().unwrap());
        let buf_wal_epoch = u64::from_le_bytes(slot_buf[40..48].try_into().unwrap());
        let payload_len = u32::from_le_bytes(slot_buf[48..52].try_into().unwrap()) as usize;
        let expected_payload_crc = u32::from_le_bytes(slot_buf[52..56].try_into().unwrap());

        if generation == 0 || payload_len > MAX_INLINE_PAYLOAD_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid generation or payload_len in boot_meta slot",
            ));
        }

        let slot_payload = &slot_buf[64..64 + payload_len];
        if crc32c(slot_payload) != expected_payload_crc {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "payload CRC32C mismatch in boot_meta slot",
            ));
        }

        let mut state = BootMetaState {
            generation,
            next_seq_id,
            checkpoint_lsn,
            sync_wal_epoch,
            buf_wal_epoch,
            ..BootMetaState::default()
        };

        if (flags & FLAG_EXTERNAL_MANIFEST) == 0 {
            state.active_manifest_seq_id = None;
            state.decode_catalog_payload(slot_payload)?;
        } else {
            if slot_payload.len() != EXTERNAL_POINTER_LEN {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid external manifest pointer length in boot_meta slot",
                ));
            }
            let mf_seq_id = u64::from_le_bytes(slot_payload[0..8].try_into().unwrap());
            let mf_file_len = u64::from_le_bytes(slot_payload[8..16].try_into().unwrap());
            let mf_expected_crc = u32::from_le_bytes(slot_payload[16..20].try_into().unwrap());

            let mf_name = format_manifest_file_name(mf_seq_id);
            let mf_file = fs.open_file(&mf_name, false, false, OpenHint::ImmutableSequential)?;
            if mf_file.file_len()? != mf_file_len || mf_file_len < MANIFEST_HEADER_LEN as u64 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "external manifest file length mismatch",
                ));
            }

            let mut mf_buf = vec![0u8; mf_file_len as usize];
            mf_file.read_exact_at(&mut mf_buf, 0)?;

            if &mf_buf[0..4] != MANIFEST_MAGIC {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid magic in external manifest file",
                ));
            }
            let mf_ver = u16::from_le_bytes([mf_buf[4], mf_buf[5]]);
            let mf_payload_len =
                u32::from_le_bytes([mf_buf[8], mf_buf[9], mf_buf[10], mf_buf[11]]) as usize;
            let mf_header_crc_field =
                u32::from_le_bytes([mf_buf[12], mf_buf[13], mf_buf[14], mf_buf[15]]);

            if mf_ver != FORMAT_VERSION
                || MANIFEST_HEADER_LEN + mf_payload_len != mf_buf.len()
                || mf_header_crc_field != mf_expected_crc
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "external manifest header mismatch",
                ));
            }

            let catalog_slice = &mf_buf[MANIFEST_HEADER_LEN..];
            if crc32c(catalog_slice) != mf_expected_crc {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "external manifest payload CRC32C mismatch",
                ));
            }

            state.active_manifest_seq_id = Some(mf_seq_id);
            state.decode_catalog_payload(catalog_slice)?;
        }

        Ok(state)
    }

    fn sweep_orphans_and_pending_deletes(
        &mut self,
        standby_manifest_id: Option<u64>,
    ) -> io::Result<()> {
        let mut referenced = self.state.referenced_file_names();
        if let Some(standby_mf) = standby_manifest_id {
            referenced.insert(format_manifest_file_name(standby_mf));
        }

        let files_on_disk = self.fs.list_files()?;
        for fname in files_on_disk {
            if is_managed_segment_or_manifest(&fname) && !referenced.contains(&fname) {
                self.state.queue_pending_delete(fname);
            }
        }

        self.try_process_pending_deletes();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pal::OsFs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static META_TEST_COUNTER: AtomicU64 = AtomicU64::new(1);

    struct TempTestDir {
        path: PathBuf,
    }

    impl TempTestDir {
        fn new(prefix: &str) -> Self {
            let id = META_TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
            let pid = std::process::id();
            let dir = std::env::temp_dir().join(format!("aydb_meta_test_{prefix}_{pid}_{id}"));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).expect("create temp test dir");
            Self { path: dir }
        }
    }

    impl Drop for TempTestDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    #[test]
    fn fresh_init_and_ping_pong_commits() {
        let tmp = TempTestDir::new("ping_pong");
        let fs = Arc::new(OsFs::open(&tmp.path, true).expect("open OsFs"));

        {
            let mut mgr = BootMetaManager::open_or_recover(Arc::clone(&fs)).expect("open fresh");
            assert_eq!(mgr.active_slot(), SlotIndex::SlotA);
            assert_eq!(mgr.state().generation, 1);

            // Commit Gen 2 -> lands in Slot B
            mgr.commit_update(|s| {
                let id = s.allocate_seq_id();
                s.idx_segments.push(IdxSegmentMeta {
                    seq_id: id,
                    file_len: 16384,
                    level: 0,
                    record_count: 42,
                });
                s.checkpoint_lsn = 100;
            })
            .expect("commit gen 2");
            assert_eq!(mgr.active_slot(), SlotIndex::SlotB);
            assert_eq!(mgr.state().generation, 2);

            // Create the referenced idx file so orphan sweep on reopen sees it as live
            let idx_name = format_idx_file_name(1);
            let _f = fs
                .open_file(&idx_name, true, true, OpenHint::ImmutableRandom)
                .expect("create idx_1");

            // Commit Gen 3 -> lands in Slot A
            mgr.commit_update(|s| {
                s.table_quotas.push(TableQuotaMeta {
                    table_id: 7,
                    eviction_policy: EvictionPolicy::Lru,
                    max_bytes: 100 * 1024 * 1024,
                    current_logical_bytes: 4096,
                });
                s.checkpoint_lsn = 250;
            })
            .expect("commit gen 3");
            assert_eq!(mgr.active_slot(), SlotIndex::SlotA);
            assert_eq!(mgr.state().generation, 3);
        }

        // Reopen and verify Generation 3 is selected from Slot A
        let mgr = BootMetaManager::open_or_recover(Arc::clone(&fs)).expect("reopen");
        assert_eq!(mgr.active_slot(), SlotIndex::SlotA);
        assert_eq!(mgr.state().generation, 3);
        assert_eq!(mgr.state().checkpoint_lsn, 250);
        assert_eq!(mgr.state().idx_segments.len(), 1);
        assert_eq!(mgr.state().table_quotas.len(), 1);
        assert_eq!(mgr.state().table_quotas[0].table_id, 7);
    }

    #[test]
    fn subsector_torn_write_and_dual_corruption_detection() {
        let tmp = TempTestDir::new("torn_write");
        let fs = Arc::new(OsFs::open(&tmp.path, true).expect("open OsFs"));

        {
            let mut mgr = BootMetaManager::open_or_recover(Arc::clone(&fs)).expect("open fresh");
            // Gen 2 in Slot B
            mgr.commit_update(|s| {
                s.checkpoint_lsn = 500;
            })
            .expect("commit gen 2");
            // Gen 3 in Slot A
            mgr.commit_update(|s| {
                s.checkpoint_lsn = 900;
            })
            .expect("commit gen 3");
            assert_eq!(mgr.active_slot(), SlotIndex::SlotA);
        }

        // Simulate a 512-byte sub-sector torn write at the very end of Slot A (byte 4095,
        // corrupting SlotTrailerCrc32c while leaving HeaderCrc32c intact!).
        {
            let f = fs
                .open_file(BOOT_META_FILE_NAME, false, true, OpenHint::MutableSyncLog)
                .expect("open boot_meta raw");
            f.write_all_at(&[0xFF], 4095).expect("corrupt trailer byte");
        }

        // Recovery must detect the torn trailer on Slot A (Gen 3) and fall back cleanly to Slot B (Gen 2)!
        {
            let mgr = BootMetaManager::open_or_recover(Arc::clone(&fs))
                .expect("recover from Slot B after Slot A torn write");
            assert_eq!(mgr.active_slot(), SlotIndex::SlotB);
            assert_eq!(mgr.state().generation, 2);
            assert_eq!(mgr.state().checkpoint_lsn, 500);
        }

        // Now corrupt Slot B as well; recovery must refuse to open and return InvalidData!
        {
            let f = fs
                .open_file(BOOT_META_FILE_NAME, false, true, OpenHint::MutableSyncLog)
                .expect("open boot_meta raw");
            f.write_all_at(&[0xEE], 4096 + 10)
                .expect("corrupt Slot B header");
        }

        let res = BootMetaManager::open_or_recover(Arc::clone(&fs));
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn external_manifest_overflow_and_gc() {
        let tmp = TempTestDir::new("ext_manifest");
        let fs = Arc::new(OsFs::open(&tmp.path, true).expect("open OsFs"));

        let first_mf_id;
        let second_mf_id;
        {
            let mut mgr = BootMetaManager::open_or_recover(Arc::clone(&fs)).expect("open fresh");

            // Add 200 index segments (200 * 24B = 4800B > 4028B inline limit)
            mgr.commit_update(|s| {
                for i in 0..200 {
                    let seq_id = s.allocate_seq_id();
                    let fname = format_idx_file_name(seq_id);
                    let _ = fs
                        .open_file(&fname, true, true, OpenHint::ImmutableRandom)
                        .unwrap();
                    s.idx_segments.push(IdxSegmentMeta {
                        seq_id,
                        file_len: 4096 * (i + 1),
                        level: 1,
                        record_count: 100,
                    });
                }
            })
            .expect("commit overflow gen 2");

            first_mf_id = mgr
                .state()
                .active_manifest_seq_id
                .expect("must overflow to external manifest");

            // Commit another overflow update; the first manifest must be garbage collected
            mgr.commit_update(|s| {
                s.checkpoint_lsn = 7777;
            })
            .expect("commit overflow gen 3");

            second_mf_id = mgr
                .state()
                .active_manifest_seq_id
                .expect("must overflow to second external manifest");
            assert_ne!(first_mf_id, second_mf_id);

            let files = fs.list_files().unwrap();
            assert!(!files.contains(&format_manifest_file_name(first_mf_id)));
            assert!(files.contains(&format_manifest_file_name(second_mf_id)));
        }

        // Reopen and verify all 200 segments are recovered from the external manifest
        let mgr = BootMetaManager::open_or_recover(Arc::clone(&fs)).expect("reopen overflow");
        assert_eq!(mgr.state().generation, 3);
        assert_eq!(mgr.state().checkpoint_lsn, 7777);
        assert_eq!(mgr.state().idx_segments.len(), 200);
        assert_eq!(mgr.state().active_manifest_seq_id, Some(second_mf_id));
    }

    #[test]
    fn deferred_gc_on_sharing_violation_and_orphan_sweep() {
        let tmp = TempTestDir::new("deferred_gc");
        let fs = Arc::new(OsFs::open(&tmp.path, true).expect("open OsFs"));

        let locked_idx_name = format_idx_file_name(10);
        let orphan_idx_name = format_idx_file_name(99);

        // Hold an open handle to `idx_000000000000000a.db` to simulate an antivirus filter
        // blocking DeleteFile while the segment is retired.
        let held_handle = fs
            .open_file(&locked_idx_name, true, true, OpenHint::ImmutableRandom)
            .expect("create and hold idx 10");

        // Also create an unreferenced orphan segment file (simulating a crash mid-flush)
        let _orphan = fs
            .open_file(&orphan_idx_name, true, true, OpenHint::ImmutableRandom)
            .expect("create orphan idx 99");
        drop(_orphan);

        {
            let mut mgr = BootMetaManager::open_or_recover(Arc::clone(&fs)).expect("open");
            // Orphan `idx_99` should have been swept immediately on startup!
            assert!(!fs.list_files().unwrap().contains(&orphan_idx_name));

            // On Windows, `locked_idx_name` could not be deleted because `held_handle` is open;
            // verify it remains queued in `pending_delete_files` and persisted on the next commit.
            #[cfg(windows)]
            {
                assert!(mgr.state().pending_delete_files.contains(&locked_idx_name));
                mgr.commit_update(|s| {
                    s.checkpoint_lsn = 42;
                })
                .expect("commit with pending delete");
                assert!(mgr.state().pending_delete_files.contains(&locked_idx_name));
            }
        }

        // Now drop the held handle (antivirus scan finished) and reopen `BootMetaManager`;
        // startup GC sweep must delete `locked_idx_name` cleanly!
        drop(held_handle);

        let mgr = BootMetaManager::open_or_recover(Arc::clone(&fs)).expect("reopen after unlock");
        assert!(mgr.state().pending_delete_files.is_empty());
        assert!(!fs.list_files().unwrap().contains(&locked_idx_name));
    }
}
