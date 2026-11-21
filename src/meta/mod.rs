//! Layer 2 (Part 1): Atomic Boot Metadata (`boot_meta.db`), Hybrid External Manifests,
//! and Rename-Free Deferred Garbage Collection.
//!
//! See `docs/phase1/02_BOOT_META_AND_GC.md` for the detailed binary specification and
//! crash-recovery state machine.

use std::collections::{BTreeSet, HashSet};
use std::io;
use std::sync::Arc;

use crate::pal::{OpenHint, PlatformFile, PlatformFs, PreallocStrategy};
use crate::util::byte_reader::ByteReader;
use crate::util::crc32c::{crc32c, crc32c_append};

/// Computes the CRC32C checksum covering the first 12 header bytes (`Magic || Version ||
/// Reserved || PayloadLen`) followed immediately by `catalog_bytes`.
///
/// Covering the header bytes ensures any torn write or bit-flip in `Magic`, `FormatVersion`,
/// `Reserved`, or `PayloadLen` is classified as `SlotLoadError::Corrupt` rather than a
/// CRC-valid forward-incompatibility abort.
#[inline]
fn compute_manifest_crc(header_12: &[u8], catalog_bytes: &[u8]) -> u32 {
    debug_assert_eq!(header_12.len(), 12);
    crc32c_append(crc32c(header_12), catalog_bytes)
}

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

/// Defensive upper bound on external manifest file size (16 MB).
pub const MAX_EXTERNAL_MANIFEST_BYTES: u64 = 16 * 1024 * 1024;

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

fn is_hex_n(s: &str, expected_len: usize) -> bool {
    s.len() == expected_len && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

/// Returns `true` if `file_name` is a canonical immutable segment or manifest managed by
/// `BootMetaState`.
fn is_managed_segment_or_manifest(file_name: &str) -> bool {
    if crate::pal::path::validate_flat_file_name(file_name).is_err() {
        return false;
    }
    let Some(stem) = file_name.strip_suffix(".db") else {
        return false;
    };
    if let Some(hex) = stem
        .strip_prefix("idx_")
        .or_else(|| stem.strip_prefix("blob_std_"))
        .or_else(|| stem.strip_prefix("manifest_"))
    {
        return is_hex_n(hex, 16);
    }
    if let Some(rest) = stem.strip_prefix("blob_lru_") {
        return rest
            .split_once('_')
            .is_some_and(|(tbl, seq)| is_hex_n(tbl, 4) && is_hex_n(seq, 16));
    }
    false
}

/// Extracts the SeqID encoded in a managed segment or manifest file name
/// (`idx_`, `blob_std_`, `blob_lru_<TblID>_` or `manifest_`).
fn parse_managed_seq_id(file_name: &str) -> Option<u64> {
    let stem = file_name.strip_suffix(".db")?;
    let hex = if let Some(h) = stem.strip_prefix("idx_") {
        h
    } else if let Some(h) = stem.strip_prefix("blob_std_") {
        h
    } else if let Some(h) = stem.strip_prefix("blob_lru_") {
        h.split_once('_')?.1
    } else {
        stem.strip_prefix("manifest_")?
    };
    u64::from_str_radix(hex, 16).ok()
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

    /// Queues a managed segment or manifest filename for deferred deletion if not already queued.
    pub fn queue_pending_delete(&mut self, file_name: String) {
        if is_managed_segment_or_manifest(&file_name)
            && !self.pending_delete_files.contains(&file_name)
        {
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
        let checked_count = |len: usize, what: &str| {
            u32::try_from(len).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("{what} count exceeds u32::MAX"),
                )
            })
        };

        // 1. Index segments (24B each)
        buf.extend_from_slice(
            &checked_count(self.idx_segments.len(), "idx_segments")?.to_le_bytes(),
        );
        for seg in &self.idx_segments {
            buf.extend_from_slice(&seg.seq_id.to_le_bytes());
            buf.extend_from_slice(&seg.file_len.to_le_bytes());
            buf.extend_from_slice(&seg.level.to_le_bytes());
            buf.extend_from_slice(&0u16.to_le_bytes()); // reserved
            buf.extend_from_slice(&seg.record_count.to_le_bytes());
        }

        // 2. Standard blob segments (24B each)
        buf.extend_from_slice(
            &checked_count(self.std_blob_segments.len(), "std_blob_segments")?.to_le_bytes(),
        );
        for seg in &self.std_blob_segments {
            buf.extend_from_slice(&seg.seq_id.to_le_bytes());
            buf.extend_from_slice(&seg.file_len.to_le_bytes());
            buf.extend_from_slice(&seg.dead_bytes.to_le_bytes());
        }

        // 3. LRU blob segments (32B each)
        buf.extend_from_slice(
            &checked_count(self.lru_blob_segments.len(), "lru_blob_segments")?.to_le_bytes(),
        );
        for seg in &self.lru_blob_segments {
            buf.extend_from_slice(&seg.seq_id.to_le_bytes());
            buf.extend_from_slice(&seg.file_len.to_le_bytes());
            buf.extend_from_slice(&seg.dead_bytes.to_le_bytes());
            buf.extend_from_slice(&seg.table_id.to_le_bytes());
            buf.extend_from_slice(&[0u8; 6]); // reserved
        }

        // 4. Table quotas (24B each)
        buf.extend_from_slice(
            &checked_count(self.table_quotas.len(), "table_quotas")?.to_le_bytes(),
        );
        for q in &self.table_quotas {
            buf.extend_from_slice(&q.table_id.to_le_bytes());
            buf.push(q.eviction_policy as u8);
            buf.extend_from_slice(&[0u8; 5]); // reserved
            buf.extend_from_slice(&q.max_bytes.to_le_bytes());
            buf.extend_from_slice(&q.current_logical_bytes.to_le_bytes());
        }

        // 5. Pending delete filenames
        buf.extend_from_slice(
            &checked_count(self.pending_delete_files.len(), "pending_delete_files")?.to_le_bytes(),
        );
        for name in &self.pending_delete_files {
            if !is_managed_segment_or_manifest(name) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("invalid managed filename in pending_delete_files: {name}"),
                ));
            }
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
    pub(crate) fn decode_catalog_payload(&mut self, payload: &[u8]) -> io::Result<()> {
        let mut r = ByteReader::with_eof_msg(payload, "truncated catalog payload in boot_meta");

        // 1. Index segments
        let num_idx = r.read_u32_le()? as usize;
        if num_idx.checked_mul(24).is_none_or(|b| b > r.remaining()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "corrupted idx_segments count",
            ));
        }
        let mut idx_segments = Vec::new();
        idx_segments.try_reserve_exact(num_idx).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "idx_segments allocation overflow",
            )
        })?;
        let mut seen_idx = HashSet::new();
        seen_idx.try_reserve(num_idx).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "idx_segments seen-set allocation overflow",
            )
        })?;
        for _ in 0..num_idx {
            let seq_id = r.read_u64_le()?;
            let file_len = r.read_u64_le()?;
            let level = r.read_u16_le()?;
            let _reserved = r.read_u16_le()?;
            let record_count = r.read_u32_le()?;
            if !seen_idx.insert(seq_id) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "duplicate seq_id in idx_segments",
                ));
            }
            idx_segments.push(IdxSegmentMeta {
                seq_id,
                file_len,
                level,
                record_count,
            });
        }

        // 2. Standard blob segments
        let num_std_blob = r.read_u32_le()? as usize;
        if num_std_blob
            .checked_mul(24)
            .is_none_or(|b| b > r.remaining())
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "corrupted std_blob_segments count",
            ));
        }
        let mut std_blob_segments = Vec::new();
        std_blob_segments
            .try_reserve_exact(num_std_blob)
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "std_blob_segments allocation overflow",
                )
            })?;
        let mut seen_std = HashSet::new();
        seen_std.try_reserve(num_std_blob).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "std_blob_segments seen-set allocation overflow",
            )
        })?;
        for _ in 0..num_std_blob {
            let seq_id = r.read_u64_le()?;
            let file_len = r.read_u64_le()?;
            let dead_bytes = r.read_u64_le()?;
            if !seen_std.insert(seq_id) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "duplicate seq_id in std_blob_segments",
                ));
            }
            std_blob_segments.push(StdBlobSegmentMeta {
                seq_id,
                file_len,
                dead_bytes,
            });
        }

        // 3. LRU blob segments
        let num_lru_blob = r.read_u32_le()? as usize;
        if num_lru_blob
            .checked_mul(32)
            .is_none_or(|b| b > r.remaining())
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "corrupted lru_blob_segments count",
            ));
        }
        let mut lru_blob_segments = Vec::new();
        lru_blob_segments
            .try_reserve_exact(num_lru_blob)
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "lru_blob_segments allocation overflow",
                )
            })?;
        let mut seen_lru = HashSet::new();
        seen_lru.try_reserve(num_lru_blob).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "lru_blob_segments seen-set allocation overflow",
            )
        })?;
        for _ in 0..num_lru_blob {
            let seq_id = r.read_u64_le()?;
            let file_len = r.read_u64_le()?;
            let dead_bytes = r.read_u64_le()?;
            let table_id = r.read_u16_le()?;
            let _reserved = r.read_bytes(6)?;
            if !seen_lru.insert((table_id, seq_id)) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "duplicate (table_id, seq_id) in lru_blob_segments",
                ));
            }
            lru_blob_segments.push(LruBlobSegmentMeta {
                seq_id,
                file_len,
                dead_bytes,
                table_id,
            });
        }

        // 4. Table quotas
        let num_quotas = r.read_u32_le()? as usize;
        if num_quotas.checked_mul(24).is_none_or(|b| b > r.remaining()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "corrupted table_quotas count",
            ));
        }
        let mut table_quotas = Vec::new();
        table_quotas.try_reserve_exact(num_quotas).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "table_quotas allocation overflow",
            )
        })?;
        let mut seen_quotas = HashSet::new();
        seen_quotas.try_reserve(num_quotas).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "table_quotas seen-set allocation overflow",
            )
        })?;
        for _ in 0..num_quotas {
            let table_id = r.read_u16_le()?;
            let policy_byte = r.read_u8()?;
            let eviction_policy = EvictionPolicy::from_u8(policy_byte)?;
            let _reserved = r.read_bytes(5)?;
            let max_bytes = r.read_u64_le()?;
            let current_logical_bytes = r.read_u64_le()?;
            if !seen_quotas.insert(table_id) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "duplicate table_id in table_quotas",
                ));
            }
            table_quotas.push(TableQuotaMeta {
                table_id,
                eviction_policy,
                max_bytes,
                current_logical_bytes,
            });
        }

        // 5. Pending deletes
        let num_deletes = r.read_u32_le()? as usize;
        if num_deletes.checked_mul(2).is_none_or(|b| b > r.remaining()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "corrupted pending_delete_files count",
            ));
        }
        let mut pending_delete_files = Vec::new();
        pending_delete_files
            .try_reserve_exact(num_deletes)
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "pending_delete_files allocation overflow",
                )
            })?;
        for _ in 0..num_deletes {
            let name_len = r.read_u16_le()? as usize;
            let name_bytes = r.read_bytes(name_len)?;
            let name = std::str::from_utf8(name_bytes).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid UTF-8 in pending_delete_files",
                )
            })?;
            if !is_managed_segment_or_manifest(name) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "non-managed filename in pending_delete_files",
                ));
            }
            pending_delete_files.push(name.to_owned());
        }

        if !r.is_empty() {
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

/// Classifies why a `boot_meta.db` slot could not be loaded.
enum SlotLoadError {
    /// Torn write, uninitialized slot, magic/CRC mismatch, or missing/truncated/corrupt external
    /// manifest: safe to demote and fall back to the opposite valid slot.
    Corrupt(io::Error),
    /// CRC-verified slot or external manifest carrying an unsupported format version, unknown flag
    /// bits, non-zero reserved header bytes, invalid header bounds, invalid catalog payload, or
    /// self-contradictory `pending_delete_files`: must abort open without falling back.
    Incompatible(io::Error),
    /// Potentially transient I/O error: must abort open so nothing is swept.
    Io(io::Error),
}

enum SlotPayload {
    Inline(BootMetaState),
    External {
        base_state: BootMetaState,
        mf_seq_id: u64,
        mf_file_len: u64,
        mf_expected_crc: u32,
    },
}

struct ParsedSlotEnvelope {
    generation: u64,
    payload: SlotPayload,
}

impl ParsedSlotEnvelope {
    #[inline]
    fn external_manifest_seq_id(&self) -> Option<u64> {
        match &self.payload {
            SlotPayload::Inline(_) => None,
            SlotPayload::External { mf_seq_id, .. } => Some(*mf_seq_id),
        }
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
    standby_manifest_id: Option<u64>,
    uncertain_write: Option<String>,
    needs_preallocate: bool,
    needs_initial_sync: bool,
}

impl<F: PlatformFs> BootMetaManager<F> {
    /// Opens `boot_meta.db` (creating and pre-allocating it if missing), runs the 4-state
    /// crash-recovery truth table across Slot A and Slot B, and sweeps orphaned or pending-delete
    /// segment files immediately.
    pub fn open_or_recover(fs: Arc<F>) -> io::Result<Self> {
        let mut mgr = Self::open_or_recover_deferred_sweep(fs)?;
        mgr.sweep_orphans()?;
        Ok(mgr)
    }

    fn ensure_no_existing_db_files(fs: &Arc<F>) -> io::Result<()> {
        for fname in fs.list_files()? {
            if fname == crate::pal::DB_LOCK_FILE_NAME {
                continue;
            }
            if is_managed_segment_or_manifest(&fname) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "boot_meta.db is missing or zeroed while existing database files are present ({fname})"
                    ),
                ));
            }
            if fname == crate::wal::WAL_SYNC_FILE_NAME || fname == crate::wal::WAL_BUF_FILE_NAME {
                let wf = match fs.open_file(&fname, false, false, OpenHint::ImmutableRandom) {
                    Ok(f) => f,
                    Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                    Err(e) => return Err(e),
                };
                if wf.file_len()? > 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "boot_meta.db is missing or zeroed while existing database files are present ({fname})"
                        ),
                    ));
                }
            }
        }
        Ok(())
    }

    fn init_fresh_boot_meta(fs: Arc<F>, file: Arc<F::File>) -> io::Result<Self> {
        if file.file_len()? < BOOT_META_FILE_SIZE {
            file.preallocate(BOOT_META_FILE_SIZE, PreallocStrategy::PhysicalZeroFill)?;
        }
        // Case 3: Newly created database (or crash prior to first metadata commit).
        let mut mgr = Self {
            fs,
            file,
            // Set active_slot to SlotB so the initial write below lands in SlotA (offset 0).
            active_slot: SlotIndex::SlotB,
            state: BootMetaState::default(),
            standby_manifest_id: None,
            uncertain_write: None,
            needs_preallocate: false,
            needs_initial_sync: false,
        };
        mgr.write_current_state_to_slot(SlotIndex::SlotA)?;
        mgr.active_slot = SlotIndex::SlotA;
        Ok(mgr)
    }

    /// Opens `boot_meta.db` and runs the 4-state crash-recovery truth table across Slot A and
    /// Slot B **without** sweeping orphan segment files yet.
    ///
    /// For an existing non-zero `boot_meta.db`, preallocation (if short) and the initial
    /// `sync_durable` are deferred until [`Self::ensure_durable`], [`Self::commit_update`], or
    /// [`Self::sweep_orphans`] so `StorageEngine::open` under `WalDamagePolicy::Fail` can inspect
    /// the WAL and refuse damaged databases without altering `boot_meta.db` or directory state.
    pub fn open_or_recover_deferred_sweep(fs: Arc<F>) -> io::Result<Self> {
        let file = match fs.open_file(BOOT_META_FILE_NAME, false, true, OpenHint::MutableSyncLog) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                // Guard against a missing `boot_meta.db` destroying an existing database before
                // creating the file or calling `sync_dir`.
                Self::ensure_no_existing_db_files(&fs)?;
                let created =
                    fs.open_file(BOOT_META_FILE_NAME, true, true, OpenHint::MutableSyncLog)?;
                return Self::init_fresh_boot_meta(fs, created);
            }
            Err(e) => return Err(e),
        };

        let file_len = file.file_len()?;
        let mut raw = [0u8; SLOT_SIZE * 2];
        let to_read = usize::try_from(file_len)
            .unwrap_or(SLOT_SIZE * 2)
            .min(SLOT_SIZE * 2);
        if to_read > 0 {
            file.read_exact_at(&mut raw[..to_read], 0)?;
        }

        let slot_a_raw: &[u8; SLOT_SIZE] = raw[..SLOT_SIZE].try_into().unwrap();
        let slot_b_raw: &[u8; SLOT_SIZE] = raw[SLOT_SIZE..].try_into().unwrap();

        let a_all_zero = slot_a_raw.iter().all(|&b| b == 0);
        let b_all_zero = slot_b_raw.iter().all(|&b| b == 0);

        if a_all_zero && b_all_zero {
            // Guard against a zeroed `boot_meta.db` destroying an existing database before
            // preallocating or writing Slot A.
            Self::ensure_no_existing_db_files(&fs)?;
            // Drop the existing handle (Windows exclusive lock) and reopen with create=true so
            // Unix `sync_dir` covers a directory entry created before an interrupted first open.
            drop(file);
            let created =
                fs.open_file(BOOT_META_FILE_NAME, true, true, OpenHint::MutableSyncLog)?;
            return Self::init_fresh_boot_meta(fs, created);
        }

        // Parse both 4 KB slot envelopes first; only torn/corrupt headers or missing/corrupt
        // manifests may demote a slot, while CRC-valid incompatible formats/flags/reserved bytes
        // and transient I/O errors abort open. External manifests are materialized lazily so
        // opening a healthy primary slot never reads the standby slot's manifest file.
        let env_a = Self::classify_slot_result(Self::parse_slot_envelope(slot_a_raw))?;
        let env_b = Self::classify_slot_result(Self::parse_slot_envelope(slot_b_raw))?;

        let (winning_slot, winning_state, standby_manifest_id) = match (env_a, env_b) {
            (Ok(ea), Ok(eb)) => {
                // Case 1: Both 4 KB envelopes are valid; select the higher generation and only
                // fall back to the standby external manifest if the primary manifest is corrupt.
                let (primary_slot, primary_env, standby_slot, standby_env) =
                    if eb.generation > ea.generation {
                        (SlotIndex::SlotB, eb, SlotIndex::SlotA, ea)
                    } else {
                        (SlotIndex::SlotA, ea, SlotIndex::SlotB, eb)
                    };
                let standby_mf = standby_env.external_manifest_seq_id();
                match Self::materialize_slot(&fs, primary_env) {
                    Ok(state) => (primary_slot, state, standby_mf),
                    Err(SlotLoadError::Corrupt(err_primary)) => {
                        match Self::classify_slot_result(Self::materialize_slot(&fs, standby_env))?
                        {
                            Ok(state) => (standby_slot, state, None),
                            Err(err_standby) => {
                                return Err(io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    format!(
                                        "boot_meta.db corruption: both Slot A and Slot B are invalid ({err_primary}; standby: {err_standby})"
                                    ),
                                ));
                            }
                        }
                    }
                    Err(SlotLoadError::Incompatible(e) | SlotLoadError::Io(e)) => return Err(e),
                }
            }
            (Ok(ea), Err(_)) => {
                // Case 2a: Slot A valid, Slot B torn/uninitialized.
                match Self::classify_slot_result(Self::materialize_slot(&fs, ea))? {
                    Ok(state_a) => (SlotIndex::SlotA, state_a, None),
                    Err(err_a) => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "boot_meta.db corruption: both Slot A and Slot B are invalid ({err_a})"
                            ),
                        ));
                    }
                }
            }
            (Err(_), Ok(eb)) => {
                // Case 2b: Slot B valid, Slot A torn/uninitialized.
                match Self::classify_slot_result(Self::materialize_slot(&fs, eb))? {
                    Ok(state_b) => (SlotIndex::SlotB, state_b, None),
                    Err(err_b) => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "boot_meta.db corruption: both Slot A and Slot B are invalid ({err_b})"
                            ),
                        ));
                    }
                }
            }
            (Err(err_a), Err(_err_b)) => {
                // Case 4: Both slots contain non-zero bytes and both failed validation.
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "boot_meta.db corruption: both Slot A and Slot B are invalid ({err_a})"
                    ),
                ));
            }
        };

        Ok(Self {
            fs,
            file,
            active_slot: winning_slot,
            state: winning_state,
            standby_manifest_id,
            uncertain_write: None,
            needs_preallocate: file_len < BOOT_META_FILE_SIZE,
            needs_initial_sync: true,
        })
    }

    /// Ensures any deferred preallocation (if `boot_meta.db` was shorter than 8 KB) and initial
    /// `sync_durable` of the recovered winning slot are complete on disk.
    ///
    /// After a process crash the winning slot may exist only in the OS cache while the opposite
    /// slot is the only one known to be durable; making the winner durable before the first
    /// overwrite or orphan sweep prevents a power cut from tearing both slots.
    pub(crate) fn ensure_durable(&mut self) -> io::Result<()> {
        self.check_healthy()?;
        if self.needs_preallocate {
            self.file
                .preallocate(BOOT_META_FILE_SIZE, PreallocStrategy::PhysicalZeroFill)?;
            self.needs_preallocate = false;
        }
        if self.needs_initial_sync {
            self.file.sync_durable()?;
            self.needs_initial_sync = false;
        }
        Ok(())
    }

    /// Durably advances `next_seq_id` past the SeqID of every managed segment or manifest file on
    /// disk, cataloged or not. A crashed flush or compaction can leave an orphan whose SeqID was
    /// never persisted; allocating that SeqID again would reuse the file name, and the existing
    /// longer file would keep its stale tail (file names are never reused, see ARCHITECTURE.md
    /// section 2.3). Must run before anything allocates a SeqID.
    pub fn reserve_seq_ids_above_existing_files(&mut self) -> io::Result<()> {
        let max_seen = self
            .fs
            .list_files()?
            .iter()
            .filter_map(|f| parse_managed_seq_id(f))
            .max();
        match max_seen {
            Some(u64::MAX) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "managed segment SeqID overflow",
            )),
            Some(m) if m >= self.state.next_seq_id => {
                self.commit_update(|st| st.next_seq_id = m + 1)
            }
            _ => Ok(()),
        }
    }

    /// Sweeps any unreferenced `idx_*`, `blob_std_*`, `blob_lru_*`, or `manifest_*` files on disk
    /// and processes `pending_delete_files`.
    pub fn sweep_orphans(&mut self) -> io::Result<()> {
        self.ensure_durable()?;
        let standby_mf = self.standby_manifest_id;
        self.sweep_orphans_and_pending_deletes(standby_mf)
    }

    /// Returns the underlying `boot_meta.db` file handle.
    pub fn file(&self) -> &Arc<F::File> {
        &self.file
    }

    /// Returns a shared reference to the current authoritative [`BootMetaState`].
    pub fn state(&self) -> &BootMetaState {
        &self.state
    }

    /// Takes the next `SeqID` in memory, before the file that will carry it is created. The id
    /// stays consumed whatever happens to that file, so a failed creation (which can leave the
    /// file behind with a directory entry that is not durable) is never retried under the same
    /// name. The next commit persists the advanced counter; a crash before that is covered by
    /// `reserve_seq_ids_above_existing_files` at the next open.
    pub fn reserve_seq_id(&mut self) -> u64 {
        self.state.allocate_seq_id()
    }

    /// Returns which ping-pong slot currently holds the latest committed state.
    pub fn active_slot(&self) -> SlotIndex {
        self.active_slot
    }

    /// True after a slot write or flush failed with an unknown on-disk outcome.
    pub fn is_poisoned(&self) -> bool {
        self.uncertain_write.is_some()
    }

    pub(crate) fn check_healthy(&self) -> io::Result<()> {
        match &self.uncertain_write {
            Some(message) => Err(io::Error::other(format!(
                "metadata is poisoned by an uncertain slot write and must be reopened: {message}"
            ))),
            None => Ok(()),
        }
    }

    /// Atomically commits a mutated [`BootMetaState`] by incrementing `generation`,
    /// writing to the opposite ping-pong slot (and an external `manifest_<SeqID>.db` if
    /// the catalog payload exceeds 4028 bytes), flushing to non-volatile storage, and
    /// executing a deferred GC pass over `pending_delete_files`.
    /// A slot write/flush error leaves its outcome unknown and disables updates and GC until reopen.
    pub fn commit_update<M>(&mut self, mutate: M) -> io::Result<()>
    where
        M: FnOnce(&mut BootMetaState),
    {
        self.ensure_durable()?;
        let mut next_state = self.state.clone();
        let prev_manifest_id = next_state.active_manifest_seq_id;

        mutate(&mut next_state);
        next_state.generation = self.state.generation.checked_add(1).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "64-bit generation overflow")
        })?;

        let target_slot = self.active_slot.opposite();
        let mut slot_write_attempted = false;
        let mut orphan_manifest = None;
        if let Err(error) = Self::write_state_to_slot_internal(
            &self.fs,
            &self.file,
            target_slot,
            &mut next_state,
            &mut slot_write_attempted,
            &mut orphan_manifest,
        ) {
            if slot_write_attempted {
                // A failed write or flush may already have made the new slot authoritative.
                // Keep both generations' files until reopen determines which one survived.
                self.uncertain_write = Some(error.to_string());
            } else {
                // The failed attempt may have left a partially or fully written manifest file
                // under the SeqID it allocated. Consume that SeqID so a retry never reuses the
                // name: a shorter retry would otherwise keep the stale tail, and a slot would
                // reference a manifest with more bytes than it describes. A crash instead of an
                // error is covered by `reserve_seq_ids_above_existing_files` at the next open.
                self.state.next_seq_id = self.state.next_seq_id.max(next_state.next_seq_id);
                if let Some(mf_name) = orphan_manifest {
                    self.state.queue_pending_delete(mf_name);
                }
            }
            return Err(error);
        }

        // The previous generation becomes the standby slot and may be the fallback at the next
        // open, so its external manifest is retained. The manifest that stops being referenced by
        // either slot (the previous standby's) is queued for deferred deletion.
        if let Some(old_standby_mf) = self.standby_manifest_id {
            if next_state.active_manifest_seq_id != Some(old_standby_mf)
                && prev_manifest_id != Some(old_standby_mf)
            {
                next_state.queue_pending_delete(format_manifest_file_name(old_standby_mf));
            }
        }

        self.active_slot = target_slot;
        self.state = next_state;
        self.standby_manifest_id = prev_manifest_id;

        // Best-effort deletion pass over pending_delete_files.
        self.try_process_pending_deletes();
        Ok(())
    }

    /// Attempts to delete all files currently listed in `state.pending_delete_files`.
    /// Any file that fails deletion (e.g., due to `ERROR_SHARING_VIOLATION` from antivirus
    /// scanning or an open handle) remains in `pending_delete_files` for future retry.
    pub fn try_process_pending_deletes(&mut self) {
        if self.is_poisoned() || self.needs_initial_sync {
            return;
        }
        let referenced = self.state.referenced_file_names();
        let standby_mf = self.standby_manifest_id.map(format_manifest_file_name);
        let mut remaining = Vec::new();
        for file_name in self.state.pending_delete_files.drain(..) {
            if !is_managed_segment_or_manifest(&file_name)
                || referenced.contains(&file_name)
                || standby_mf.as_deref() == Some(file_name.as_str())
            {
                continue;
            }
            if self.fs.remove_file(&file_name).is_err() {
                remaining.push(file_name);
            }
        }
        self.state.pending_delete_files = remaining;
    }

    fn write_current_state_to_slot(&mut self, slot: SlotIndex) -> io::Result<()> {
        let mut orphan_manifest = None;
        Self::write_state_to_slot_internal(
            &self.fs,
            &self.file,
            slot,
            &mut self.state,
            &mut false,
            &mut orphan_manifest,
        )
    }

    fn write_state_to_slot_internal(
        fs: &Arc<F>,
        file: &Arc<F::File>,
        slot: SlotIndex,
        state: &mut BootMetaState,
        slot_write_attempted: &mut bool,
        orphan_manifest: &mut Option<String>,
    ) -> io::Result<()> {
        let catalog_bytes = state.encode_catalog_payload()?;

        let (flags, slot_payload_bytes) = if catalog_bytes.len() <= MAX_INLINE_PAYLOAD_LEN {
            state.active_manifest_seq_id = None;
            (0u16, catalog_bytes)
        } else {
            let mf_total_len = (MANIFEST_HEADER_LEN as u64)
                .checked_add(catalog_bytes.len() as u64)
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "external manifest length overflow",
                    )
                })?;
            let catalog_len_u32 = u32::try_from(catalog_bytes.len()).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "external manifest exceeds MAX_EXTERNAL_MANIFEST_BYTES",
                )
            })?;
            if mf_total_len > MAX_EXTERNAL_MANIFEST_BYTES {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "external manifest exceeds MAX_EXTERNAL_MANIFEST_BYTES",
                ));
            }
            if state.next_seq_id == u64::MAX {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "64-bit SeqID overflow",
                ));
            }
            // Overflow to an immutable external manifest file FIRST before updating boot_meta.db!
            let mf_seq_id = state.allocate_seq_id();
            let mf_name = format_manifest_file_name(mf_seq_id);

            let mut mf_buf = Vec::with_capacity(mf_total_len as usize);
            mf_buf.extend_from_slice(MANIFEST_MAGIC);
            mf_buf.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
            mf_buf.extend_from_slice(&0u16.to_le_bytes()); // reserved
            mf_buf.extend_from_slice(&catalog_len_u32.to_le_bytes());
            let mf_crc = compute_manifest_crc(&mf_buf[0..12], &catalog_bytes);
            mf_buf.extend_from_slice(&mf_crc.to_le_bytes());
            mf_buf.extend_from_slice(&catalog_bytes);

            let write_res = fs
                .open_file(&mf_name, true, true, OpenHint::ImmutableSequential)
                .and_then(|mf_file| {
                    mf_file.write_all_at(&mf_buf, 0)?;
                    mf_file.sync_durable()?;
                    drop(mf_file);
                    Ok(())
                });
            if let Err(e) = write_res {
                if fs.remove_file(&mf_name).is_err() {
                    *orphan_manifest = Some(mf_name);
                }
                return Err(e);
            }

            state.active_manifest_seq_id = Some(mf_seq_id);

            let mut ptr_buf = Vec::with_capacity(EXTERNAL_POINTER_LEN);
            ptr_buf.extend_from_slice(&mf_seq_id.to_le_bytes());
            ptr_buf.extend_from_slice(&mf_total_len.to_le_bytes());
            ptr_buf.extend_from_slice(&mf_crc.to_le_bytes());

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

        *slot_write_attempted = true;
        file.write_all_at(&slot_buf, slot.byte_offset())?;
        file.sync_durable()?;
        Ok(())
    }

    /// Separates "torn/corrupt slot that may fall back to the opposite slot" from both
    /// "CRC-verified incompatible format" and "potentially transient I/O error".
    fn classify_slot_result<T>(res: Result<T, SlotLoadError>) -> io::Result<io::Result<T>> {
        match res {
            Ok(val) => Ok(Ok(val)),
            Err(SlotLoadError::Corrupt(e)) => Ok(Err(e)),
            Err(SlotLoadError::Incompatible(e) | SlotLoadError::Io(e)) => Err(e),
        }
    }

    fn parse_slot_envelope(
        slot_buf: &[u8; SLOT_SIZE],
    ) -> Result<ParsedSlotEnvelope, SlotLoadError> {
        if &slot_buf[0..4] != BOOT_META_MAGIC {
            return Err(SlotLoadError::Corrupt(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid magic in boot_meta slot",
            )));
        }

        let expected_header_crc =
            u32::from_le_bytes([slot_buf[60], slot_buf[61], slot_buf[62], slot_buf[63]]);
        let actual_header_crc = crc32c(&slot_buf[0..60]);
        if actual_header_crc != expected_header_crc {
            return Err(SlotLoadError::Corrupt(io::Error::new(
                io::ErrorKind::InvalidData,
                "header CRC32C mismatch in boot_meta slot",
            )));
        }

        let expected_trailer_crc = u32::from_le_bytes([
            slot_buf[4092],
            slot_buf[4093],
            slot_buf[4094],
            slot_buf[4095],
        ]);
        let actual_trailer_crc = crc32c(&slot_buf[0..4092]);
        if actual_trailer_crc != expected_trailer_crc {
            return Err(SlotLoadError::Corrupt(io::Error::new(
                io::ErrorKind::InvalidData,
                "trailer CRC32C mismatch in boot_meta slot (sub-sector torn write)",
            )));
        }

        // Both slot CRCs matched: an unsupported version, unknown flag bit, or non-zero reserved
        // field here is an intentional newer/foreign format, never a torn write.
        let version = u16::from_le_bytes([slot_buf[4], slot_buf[5]]);
        if version != FORMAT_VERSION {
            return Err(SlotLoadError::Incompatible(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported boot_meta format version: {version}"),
            )));
        }

        let flags = u16::from_le_bytes([slot_buf[6], slot_buf[7]]);
        if (flags & !FLAG_EXTERNAL_MANIFEST) != 0 {
            return Err(SlotLoadError::Incompatible(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown boot_meta flags: {flags:#x}"),
            )));
        }

        let reserved = u32::from_le_bytes(slot_buf[56..60].try_into().unwrap());
        if reserved != 0 {
            return Err(SlotLoadError::Incompatible(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("non-zero reserved bytes in boot_meta header: {reserved:#x}"),
            )));
        }

        let generation = u64::from_le_bytes(slot_buf[8..16].try_into().unwrap());
        let next_seq_id = u64::from_le_bytes(slot_buf[16..24].try_into().unwrap());
        let checkpoint_lsn = u64::from_le_bytes(slot_buf[24..32].try_into().unwrap());
        let sync_wal_epoch = u64::from_le_bytes(slot_buf[32..40].try_into().unwrap());
        let buf_wal_epoch = u64::from_le_bytes(slot_buf[40..48].try_into().unwrap());
        let payload_len = u32::from_le_bytes(slot_buf[48..52].try_into().unwrap()) as usize;
        let expected_payload_crc = u32::from_le_bytes(slot_buf[52..56].try_into().unwrap());

        if generation == 0
            || generation == u64::MAX
            || next_seq_id == 0
            || next_seq_id == u64::MAX
            || checkpoint_lsn == u64::MAX
            || sync_wal_epoch == 0
            || sync_wal_epoch == u64::MAX
            || buf_wal_epoch == 0
            || buf_wal_epoch == u64::MAX
            || payload_len > MAX_INLINE_PAYLOAD_LEN
        {
            return Err(SlotLoadError::Incompatible(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid generation, bounds, or payload_len in boot_meta slot",
            )));
        }

        let slot_payload = &slot_buf[64..64 + payload_len];
        if crc32c(slot_payload) != expected_payload_crc {
            return Err(SlotLoadError::Incompatible(io::Error::new(
                io::ErrorKind::InvalidData,
                "payload CRC32C mismatch in boot_meta slot",
            )));
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
            state
                .decode_catalog_payload(slot_payload)
                .map_err(SlotLoadError::Incompatible)?;
            Ok(ParsedSlotEnvelope {
                generation,
                payload: SlotPayload::Inline(state),
            })
        } else {
            if slot_payload.len() != EXTERNAL_POINTER_LEN {
                return Err(SlotLoadError::Incompatible(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid external manifest pointer length in boot_meta slot",
                )));
            }
            let mf_seq_id = u64::from_le_bytes(slot_payload[0..8].try_into().unwrap());
            let mf_file_len = u64::from_le_bytes(slot_payload[8..16].try_into().unwrap());
            let mf_expected_crc = u32::from_le_bytes(slot_payload[16..20].try_into().unwrap());

            if mf_file_len < MANIFEST_HEADER_LEN as u64 || mf_file_len > MAX_EXTERNAL_MANIFEST_BYTES
            {
                return Err(SlotLoadError::Incompatible(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "external manifest length out of supported bounds",
                )));
            }
            let _ = usize::try_from(mf_file_len).map_err(|_| {
                SlotLoadError::Incompatible(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "external manifest length overflow",
                ))
            })?;

            Ok(ParsedSlotEnvelope {
                generation,
                payload: SlotPayload::External {
                    base_state: state,
                    mf_seq_id,
                    mf_file_len,
                    mf_expected_crc,
                },
            })
        }
    }

    fn materialize_slot(
        fs: &Arc<F>,
        envelope: ParsedSlotEnvelope,
    ) -> Result<BootMetaState, SlotLoadError> {
        let state = match envelope.payload {
            SlotPayload::Inline(state) => state,
            SlotPayload::External {
                mut base_state,
                mf_seq_id,
                mf_file_len,
                mf_expected_crc,
            } => {
                let mf_len_usize = usize::try_from(mf_file_len).map_err(|_| {
                    SlotLoadError::Incompatible(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "external manifest length overflow",
                    ))
                })?;

                let map_mf_io = |e: io::Error| match e.kind() {
                    io::ErrorKind::NotFound | io::ErrorKind::UnexpectedEof => {
                        SlotLoadError::Corrupt(e)
                    }
                    _ => SlotLoadError::Io(e),
                };

                let mf_name = format_manifest_file_name(mf_seq_id);
                let mf_file = fs
                    .open_file(&mf_name, false, false, OpenHint::ImmutableSequential)
                    .map_err(map_mf_io)?;
                if mf_file.file_len().map_err(map_mf_io)? != mf_file_len {
                    return Err(SlotLoadError::Corrupt(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "external manifest file length mismatch",
                    )));
                }
                let mut mf_buf = Vec::new();
                mf_buf.try_reserve_exact(mf_len_usize).map_err(|_| {
                    SlotLoadError::Io(io::Error::new(
                        io::ErrorKind::OutOfMemory,
                        "external manifest allocation overflow",
                    ))
                })?;
                mf_buf.resize(mf_len_usize, 0u8);
                mf_file.read_exact_at(&mut mf_buf, 0).map_err(map_mf_io)?;

                if &mf_buf[0..4] != MANIFEST_MAGIC {
                    return Err(SlotLoadError::Corrupt(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "invalid magic in external manifest file",
                    )));
                }
                let mf_ver = u16::from_le_bytes([mf_buf[4], mf_buf[5]]);
                let mf_reserved = u16::from_le_bytes([mf_buf[6], mf_buf[7]]);
                let mf_payload_len =
                    u32::from_le_bytes([mf_buf[8], mf_buf[9], mf_buf[10], mf_buf[11]]) as usize;
                let mf_header_crc_field =
                    u32::from_le_bytes([mf_buf[12], mf_buf[13], mf_buf[14], mf_buf[15]]);

                if MANIFEST_HEADER_LEN.checked_add(mf_payload_len) != Some(mf_buf.len())
                    || mf_header_crc_field != mf_expected_crc
                {
                    return Err(SlotLoadError::Corrupt(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "external manifest header mismatch",
                    )));
                }

                let catalog_slice = &mf_buf[MANIFEST_HEADER_LEN..];
                if compute_manifest_crc(&mf_buf[0..12], catalog_slice) != mf_expected_crc {
                    return Err(SlotLoadError::Corrupt(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "external manifest CRC32C mismatch",
                    )));
                }

                if mf_ver != FORMAT_VERSION {
                    return Err(SlotLoadError::Incompatible(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("unsupported external manifest format version: {mf_ver}"),
                    )));
                }
                if mf_reserved != 0 {
                    return Err(SlotLoadError::Incompatible(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "non-zero reserved bytes in external manifest header: {mf_reserved:#x}"
                        ),
                    )));
                }

                base_state.active_manifest_seq_id = Some(mf_seq_id);
                base_state
                    .decode_catalog_payload(catalog_slice)
                    .map_err(SlotLoadError::Incompatible)?;
                base_state
            }
        };

        let referenced = state.referenced_file_names();
        if state
            .pending_delete_files
            .iter()
            .any(|fname| referenced.contains(fname))
        {
            return Err(SlotLoadError::Incompatible(io::Error::new(
                io::ErrorKind::InvalidData,
                "pending_delete_files references an active segment or manifest",
            )));
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
            if fname == crate::pal::DB_LOCK_FILE_NAME {
                continue;
            }
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
    use crate::util::test_util::TempTestDir;

    #[test]
    fn reserve_seq_ids_skips_every_existing_managed_file_name() {
        let tmp = TempTestDir::new("reserve_seq");
        let fs = Arc::new(OsFs::open(&tmp.path, true).expect("open OsFs"));
        let mut mgr = BootMetaManager::open_or_recover(Arc::clone(&fs)).expect("open fresh");
        assert_eq!(mgr.state().next_seq_id, 1);

        for (name, seq) in [
            (format_idx_file_name(4), 4),
            (format_std_blob_file_name(9), 9),
            (format_lru_blob_file_name(3, 17), 17),
            (format_manifest_file_name(12), 12),
        ] {
            std::fs::write(tmp.path.join(&name), b"orphan").unwrap();
            mgr.reserve_seq_ids_above_existing_files().unwrap();
            assert!(mgr.state().next_seq_id > seq, "{name}");
        }
        assert_eq!(mgr.state().next_seq_id, 18);

        // The reservation is durable and idempotent.
        let generation = mgr.state().generation;
        mgr.reserve_seq_ids_above_existing_files().unwrap();
        assert_eq!(mgr.state().generation, generation);
        drop(mgr);
        let reopened = BootMetaManager::open_or_recover_deferred_sweep(fs).unwrap();
        assert_eq!(reopened.state().next_seq_id, 18);
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

            // The first manifest is still referenced by the standby slot and must be retained.
            let files = fs.list_files().unwrap();
            assert!(files.contains(&format_manifest_file_name(first_mf_id)));
            assert!(files.contains(&format_manifest_file_name(second_mf_id)));

            // Once a third generation is committed the first manifest is referenced by neither slot.
            mgr.commit_update(|s| {
                s.checkpoint_lsn = 8888;
            })
            .expect("commit overflow gen 4");
            let files = fs.list_files().unwrap();
            assert!(!files.contains(&format_manifest_file_name(first_mf_id)));
            assert!(files.contains(&format_manifest_file_name(second_mf_id)));
        }

        // Reopen and verify all 200 segments are recovered from the external manifest
        let mgr = BootMetaManager::open_or_recover(Arc::clone(&fs)).expect("reopen overflow");
        assert_eq!(mgr.state().generation, 4);
        assert_eq!(mgr.state().checkpoint_lsn, 8888);
        assert_eq!(mgr.state().idx_segments.len(), 200);
        assert!(mgr.state().active_manifest_seq_id.is_some());
    }

    #[test]
    fn standby_manifest_survives_commits_and_restart_sweep_and_serves_fallback() {
        let tmp = TempTestDir::new("standby_manifest");
        let fs = Arc::new(OsFs::open(&tmp.path, true).expect("open OsFs"));
        let manifests = |fs: &Arc<OsFs>| {
            let mut v: Vec<String> = fs
                .list_files()
                .unwrap()
                .into_iter()
                .filter(|f| f.starts_with("manifest_"))
                .collect();
            v.sort();
            v
        };

        let (active_mf, standby_mf, active_slot);
        {
            let mut mgr = BootMetaManager::open_or_recover(Arc::clone(&fs)).expect("open fresh");
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
            .unwrap();
            for lsn in 1..=4u64 {
                let prev = mgr.state().active_manifest_seq_id.unwrap();
                mgr.commit_update(|s| s.checkpoint_lsn = lsn).unwrap();
                let cur = mgr.state().active_manifest_seq_id.unwrap();
                assert_ne!(prev, cur);
                // Exactly the active and the standby manifests exist after every commit.
                assert_eq!(manifests(&fs), {
                    let mut v = vec![
                        format_manifest_file_name(prev),
                        format_manifest_file_name(cur),
                    ];
                    v.sort();
                    v
                });
            }
            active_mf = mgr.state().active_manifest_seq_id.unwrap();
            standby_mf = mgr.standby_manifest_id.unwrap();
            active_slot = mgr.active_slot();
        }

        // A restart sweep keeps both manifests.
        drop(BootMetaManager::open_or_recover(Arc::clone(&fs)).expect("reopen with sweep"));
        assert!(manifests(&fs).contains(&format_manifest_file_name(active_mf)));
        assert!(manifests(&fs).contains(&format_manifest_file_name(standby_mf)));

        // Corrupt the active slot; the open must fall back to the standby generation.
        let f = fs
            .open_file(BOOT_META_FILE_NAME, false, true, OpenHint::MutableSyncLog)
            .unwrap();
        let off = match active_slot {
            SlotIndex::SlotA => 0,
            SlotIndex::SlotB => SLOT_SIZE as u64,
        };
        f.write_all_at(&[0xFF], off + SLOT_SIZE as u64 - 1).unwrap();
        drop(f);

        let mgr = BootMetaManager::open_or_recover(Arc::clone(&fs)).expect("fallback open");
        assert_eq!(mgr.state().active_manifest_seq_id, Some(standby_mf));
        assert_eq!(mgr.state().idx_segments.len(), 200);
        assert_eq!(mgr.state().checkpoint_lsn, 3);
    }

    /// Test-only `PlatformFs` that fails manifest opens with a non-corruption error on demand.
    struct FlakyManifestFs {
        inner: OsFs,
        fail_manifest_open: std::sync::atomic::AtomicBool,
        fail_target: Option<String>,
        blocked_open_attempts: std::sync::atomic::AtomicUsize,
    }

    impl PlatformFs for FlakyManifestFs {
        type File = <OsFs as PlatformFs>::File;

        fn open_file(
            &self,
            rel_name: &str,
            create: bool,
            writable: bool,
            hint: OpenHint,
        ) -> io::Result<Arc<Self::File>> {
            let matches_target = match &self.fail_target {
                Some(name) => rel_name == name,
                None => rel_name.starts_with("manifest_"),
            };
            if matches_target
                && !create
                && self
                    .fail_manifest_open
                    .load(std::sync::atomic::Ordering::SeqCst)
            {
                self.blocked_open_attempts
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                return Err(io::Error::from(io::ErrorKind::PermissionDenied));
            }
            self.inner.open_file(rel_name, create, writable, hint)
        }

        fn remove_file(&self, rel_name: &str) -> io::Result<()> {
            self.inner.remove_file(rel_name)
        }

        fn list_files(&self) -> io::Result<Vec<String>> {
            self.inner.list_files()
        }

        fn root_path(&self) -> &std::path::Path {
            self.inner.root_path()
        }
    }

    #[test]
    fn transient_manifest_io_error_does_not_roll_back_or_sweep() {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

        let tmp = TempTestDir::new("flaky_manifest");
        let fs = Arc::new(FlakyManifestFs {
            inner: OsFs::open(&tmp.path, true).expect("open OsFs"),
            fail_manifest_open: AtomicBool::new(false),
            fail_target: None,
            blocked_open_attempts: AtomicUsize::new(0),
        });

        {
            let mut mgr = BootMetaManager::open_or_recover(Arc::clone(&fs)).expect("open fresh");
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
            .expect("commit gen 2");
            mgr.commit_update(|s| s.checkpoint_lsn = 4242)
                .expect("commit gen 3");
        }

        let files_before = fs.list_files().unwrap();

        fs.fail_manifest_open.store(true, Ordering::SeqCst);
        let res = BootMetaManager::open_or_recover(Arc::clone(&fs));
        assert_eq!(
            res.err().expect("transient error must fail open").kind(),
            io::ErrorKind::PermissionDenied
        );
        let res = BootMetaManager::open_or_recover_deferred_sweep(Arc::clone(&fs));
        assert_eq!(
            res.err().expect("transient error must fail open").kind(),
            io::ErrorKind::PermissionDenied
        );
        assert_eq!(
            fs.list_files().unwrap(),
            files_before,
            "nothing may be swept"
        );

        fs.fail_manifest_open.store(false, Ordering::SeqCst);
        let mgr = BootMetaManager::open_or_recover(Arc::clone(&fs)).expect("reopen after clear");
        assert_eq!(mgr.state().generation, 3);
        assert_eq!(mgr.state().checkpoint_lsn, 4242);
        assert_eq!(mgr.state().idx_segments.len(), 200);
    }

    #[test]
    fn healthy_active_manifest_opens_without_reading_corrupt_or_failing_standby_manifest() {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

        let tmp = TempTestDir::new("lazy_standby_mf");
        let standby_mf_id;
        let active_mf_id;
        {
            let fs = Arc::new(OsFs::open(&tmp.path, true).expect("open OsFs"));
            let mut mgr = BootMetaManager::open_or_recover(Arc::clone(&fs)).expect("open fresh");
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
                s.checkpoint_lsn = 111;
            })
            .expect("commit gen 2");
            standby_mf_id = mgr.state().active_manifest_seq_id.expect("gen 2 manifest");

            mgr.commit_update(|s| {
                s.checkpoint_lsn = 222;
            })
            .expect("commit gen 3");
            active_mf_id = mgr.state().active_manifest_seq_id.expect("gen 3 manifest");
            assert_ne!(standby_mf_id, active_mf_id);
        }

        // Corrupt the standby manifest on disk AND block any attempt to open it.
        let standby_name = format_manifest_file_name(standby_mf_id);
        std::fs::write(tmp.path.join(&standby_name), b"corrupt standby manifest").unwrap();
        let fs = Arc::new(FlakyManifestFs {
            inner: OsFs::open(&tmp.path, true).expect("open OsFs"),
            fail_manifest_open: AtomicBool::new(true),
            fail_target: Some(standby_name.clone()),
            blocked_open_attempts: AtomicUsize::new(0),
        });

        let mut mgr = BootMetaManager::open_or_recover(Arc::clone(&fs))
            .expect("healthy active manifest must open without touching standby manifest");
        assert_eq!(fs.blocked_open_attempts.load(Ordering::SeqCst), 0);
        assert_eq!(mgr.state().generation, 3);
        assert_eq!(mgr.state().checkpoint_lsn, 222);
        assert_eq!(mgr.state().active_manifest_seq_id, Some(active_mf_id));
        // Standby manifest must still be retained until the standby slot is overwritten.
        assert!(fs.list_files().unwrap().contains(&standby_name));

        // Advancing one more generation overwrites the old standby slot and sweeps standby_name.
        mgr.commit_update(|s| {
            s.checkpoint_lsn = 333;
        })
        .expect("commit gen 4");
        assert!(!fs.list_files().unwrap().contains(&standby_name));
    }

    #[test]
    fn deferred_gc_on_sharing_violation_and_orphan_sweep() {
        let tmp = TempTestDir::new("deferred_gc");
        let fs = Arc::new(OsFs::open(&tmp.path, true).expect("open OsFs"));

        let locked_idx_name = format_idx_file_name(10);
        let orphan_idx_name = format_idx_file_name(99);

        {
            let mut mgr = BootMetaManager::open_or_recover(Arc::clone(&fs)).expect("open");
            // Commit Gen 2 referencing `locked_idx_name`
            mgr.commit_update(|s| {
                s.idx_segments.push(IdxSegmentMeta {
                    seq_id: 10,
                    file_len: 4096,
                    level: 0,
                    record_count: 1,
                });
            })
            .expect("commit gen 2");
        }

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
            let mut mgr =
                BootMetaManager::open_or_recover(Arc::clone(&fs)).expect("reopen with orphan");
            // Orphan `idx_99` should have been swept immediately on startup!
            assert!(!fs.list_files().unwrap().contains(&orphan_idx_name));

            // Retire `locked_idx_name` and queue it for deletion while `held_handle` is open
            mgr.commit_update(|s| {
                s.idx_segments.clear();
                s.queue_pending_delete(locked_idx_name.clone());
            })
            .expect("retire idx 10");

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

    #[test]
    fn missing_or_zeroed_boot_meta_refuses_to_destroy_existing_segments() {
        let tmp = TempTestDir::new("missing_boot_meta");
        let fs = Arc::new(OsFs::open(&tmp.path, true).expect("open OsFs"));

        let idx_name = format_idx_file_name(1);
        {
            let mut mgr = BootMetaManager::open_or_recover(Arc::clone(&fs)).expect("open fresh");
            mgr.commit_update(|s| {
                let id = s.allocate_seq_id();
                s.idx_segments.push(IdxSegmentMeta {
                    seq_id: id,
                    file_len: 4096,
                    level: 0,
                    record_count: 10,
                });
            })
            .expect("commit idx_1");
            let _f = fs
                .open_file(&idx_name, true, true, OpenHint::ImmutableRandom)
                .expect("create idx_1");
        }

        // Delete `boot_meta.db` while `idx_0000000000000001.db` exists on disk
        fs.remove_file(BOOT_META_FILE_NAME)
            .expect("remove boot_meta.db");

        let err = match BootMetaManager::open_or_recover(Arc::clone(&fs)) {
            Ok(_) => panic!("open_or_recover must fail when boot_meta.db is missing"),
            Err(e) => e,
        };
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(
            fs.list_files().unwrap().contains(&idx_name),
            "existing segment file must NOT be deleted when boot_meta.db is missing or zeroed"
        );
    }

    #[test]
    fn corrupted_catalog_counts_with_valid_crc_rejected() {
        let tmp = TempTestDir::new("corrupted_catalog_counts");
        let fs = Arc::new(OsFs::open(&tmp.path, true).expect("open OsFs"));
        {
            let _mgr = BootMetaManager::open_or_recover(Arc::clone(&fs)).expect("open fresh");
        }

        // Craft a slot payload claiming `num_deletes = u32::MAX` while recomputing a valid slot CRC32C
        let file = fs
            .open_file(BOOT_META_FILE_NAME, false, true, OpenHint::ImmutableRandom)
            .unwrap();
        let mut buf = vec![0u8; BOOT_META_FILE_SIZE as usize];
        file.read_exact_at(&mut buf, 0).unwrap();

        // Slot A starts at 0; payload starts at 64 (`num_idx=0`, `num_std=0`, `num_lru=0`, `num_quotas=0`, `num_deletes=u32::MAX`)
        buf[64..68].copy_from_slice(&0u32.to_le_bytes());
        buf[68..72].copy_from_slice(&0u32.to_le_bytes());
        buf[72..76].copy_from_slice(&0u32.to_le_bytes());
        buf[76..80].copy_from_slice(&0u32.to_le_bytes());
        buf[80..84].copy_from_slice(&u32::MAX.to_le_bytes());
        // Bump Slot A generation to 99 so it wins over Slot B, and recompute Slot A CRC32C
        buf[8..16].copy_from_slice(&99u64.to_le_bytes());
        let crc_a = crc32c(&buf[0..4092]);
        buf[4092..4096].copy_from_slice(&crc_a.to_le_bytes());
        // Corrupt Slot B so Slot A is the sole valid-CRC candidate
        buf[4096..4100].copy_from_slice(b"XXXX");
        file.write_all_at(&buf, 0).unwrap();
        file.sync_durable().unwrap();
        drop(file);

        let err = match BootMetaManager::open_or_recover(Arc::clone(&fs)) {
            Ok(_) => panic!("crafted huge num_deletes with valid CRC must be rejected"),
            Err(e) => e,
        };
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    /// Event recorded by [`LoggingFs`] for `boot_meta.db`.
    #[derive(Debug, Clone, PartialEq, Eq)]
    enum BootMetaEvent {
        Write(u64),
        Preallocate(u64),
        Sync,
    }

    type EventLog = Arc<std::sync::Mutex<Vec<BootMetaEvent>>>;

    /// Test-only `PlatformFs` that records the writes and syncs issued on `boot_meta.db`.
    struct LoggingFs {
        inner: OsFs,
        log: EventLog,
    }

    struct LoggingFile {
        inner: Arc<<OsFs as PlatformFs>::File>,
        log: Option<EventLog>,
    }

    impl LoggingFile {
        fn record(&self, event: BootMetaEvent) {
            if let Some(log) = &self.log {
                log.lock().unwrap().push(event);
            }
        }
    }

    impl PlatformFile for LoggingFile {
        fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
            self.inner.read_at(buf, offset)
        }
        fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
            self.inner.read_exact_at(buf, offset)
        }
        fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
            self.record(BootMetaEvent::Write(offset));
            self.inner.write_all_at(buf, offset)
        }
        fn file_len(&self) -> io::Result<u64> {
            self.inner.file_len()
        }
        fn preallocate(&self, target_len: u64, strategy: PreallocStrategy) -> io::Result<()> {
            self.record(BootMetaEvent::Preallocate(target_len));
            self.inner.preallocate(target_len, strategy)
        }
        fn sync_barrier(&self) -> io::Result<()> {
            self.inner.sync_barrier()
        }
        fn sync_durable(&self) -> io::Result<()> {
            self.record(BootMetaEvent::Sync);
            self.inner.sync_durable()
        }
        fn evict_os_cache(&self, offset: u64, len: u64) -> io::Result<()> {
            self.inner.evict_os_cache(offset, len)
        }
    }

    impl PlatformFs for LoggingFs {
        type File = LoggingFile;

        fn open_file(
            &self,
            rel_name: &str,
            create: bool,
            writable: bool,
            hint: OpenHint,
        ) -> io::Result<Arc<Self::File>> {
            Ok(Arc::new(LoggingFile {
                inner: self.inner.open_file(rel_name, create, writable, hint)?,
                log: (rel_name == BOOT_META_FILE_NAME).then(|| Arc::clone(&self.log)),
            }))
        }

        fn remove_file(&self, rel_name: &str) -> io::Result<()> {
            self.inner.remove_file(rel_name)
        }

        fn list_files(&self) -> io::Result<Vec<String>> {
            self.inner.list_files()
        }

        fn root_path(&self) -> &std::path::Path {
            self.inner.root_path()
        }
    }

    #[test]
    fn first_slot_write_after_open_is_preceded_by_a_sync_of_boot_meta() {
        let tmp = TempTestDir::new("sync_before_first_slot_write");
        {
            // Two commits make both slots valid, so the next open picks a winner.
            let fs = Arc::new(OsFs::open(&tmp.path, true).expect("open OsFs"));
            let mut mgr = BootMetaManager::open_or_recover(fs).unwrap();
            mgr.commit_update(|st| st.checkpoint_lsn = 1).unwrap();
            mgr.commit_update(|st| st.checkpoint_lsn = 2).unwrap();
        }
        let log: EventLog = Arc::default();
        let fs = Arc::new(LoggingFs {
            inner: OsFs::open(&tmp.path, true).expect("reopen OsFs"),
            log: Arc::clone(&log),
        });
        let mut mgr = BootMetaManager::open_or_recover(fs).unwrap();
        mgr.commit_update(|st| st.checkpoint_lsn = 3).unwrap();
        mgr.commit_update(|st| st.checkpoint_lsn = 4).unwrap();

        // The winning slot may exist only in the OS cache after a process crash. It must be
        // durable before the commit overwrites the opposite slot, the only known-durable one.
        let events = log.lock().unwrap().clone();
        let first_write = events
            .iter()
            .position(|e| matches!(e, BootMetaEvent::Write(_)))
            .expect("a slot write");
        assert!(
            events[..first_write].contains(&BootMetaEvent::Sync),
            "no boot_meta.db sync before the first slot write: {events:?}"
        );
        // Only the first commit pays for it: each commit adds exactly write + sync.
        let syncs = events.iter().filter(|e| **e == BootMetaEvent::Sync).count();
        assert_eq!(syncs, 3, "{events:?}");
    }

    /// Flag bits other than the external-manifest flag are refused, including `0x0002`, which an
    /// earlier format used for the UNIQUE index guard.
    #[test]
    fn unknown_flag_bits_are_rejected() {
        for bit in [0x0002u16, 0x0004] {
            let tmp = TempTestDir::new("unknown_flag");
            let fs = Arc::new(OsFs::open(&tmp.path, true).expect("open OsFs"));
            {
                let mut mgr = BootMetaManager::open_or_recover(Arc::clone(&fs)).unwrap();
                mgr.commit_update(|s| s.checkpoint_lsn = 1).unwrap();
                mgr.commit_update(|s| s.checkpoint_lsn = 2).unwrap();
            }
            let path = tmp.path.join(BOOT_META_FILE_NAME);
            let mut bytes = std::fs::read(&path).unwrap();
            for slot in 0..2 {
                let base = slot * SLOT_SIZE;
                let flags = u16::from_le_bytes([bytes[base + 6], bytes[base + 7]]) | bit;
                bytes[base + 6..base + 8].copy_from_slice(&flags.to_le_bytes());
                let header_crc = crc32c(&bytes[base..base + 60]);
                bytes[base + 60..base + 64].copy_from_slice(&header_crc.to_le_bytes());
                let trailer = crc32c(&bytes[base..base + 4092]);
                bytes[base + 4092..base + 4096].copy_from_slice(&trailer.to_le_bytes());
            }
            std::fs::write(&path, &bytes).unwrap();
            let err = BootMetaManager::open_or_recover(fs)
                .err()
                .expect("must refuse");
            assert!(
                err.to_string().contains("unknown boot_meta flags"),
                "{bit:#x}: {err}"
            );
        }
    }

    #[test]
    fn single_incompatible_slot_aborts_open_without_falling_back_to_older_slot() {
        for case in ["version", "flags", "reserved"] {
            let tmp = TempTestDir::new("single_incompat_slot");
            let fs = Arc::new(OsFs::open(&tmp.path, true).expect("open OsFs"));
            let idx1 = format_idx_file_name(1);
            let idx2 = format_idx_file_name(2);
            {
                let mut mgr = BootMetaManager::open_or_recover(Arc::clone(&fs)).unwrap();
                // Gen 2 in Slot B references idx_1.
                mgr.commit_update(|s| {
                    let id = s.allocate_seq_id();
                    s.idx_segments.push(IdxSegmentMeta {
                        seq_id: id,
                        file_len: 4096,
                        level: 0,
                        record_count: 1,
                    });
                })
                .unwrap();
                let _ = fs
                    .open_file(&idx1, true, true, OpenHint::ImmutableRandom)
                    .unwrap();
                assert_eq!(mgr.active_slot(), SlotIndex::SlotB);

                // Gen 3 in Slot A references idx_1 and idx_2.
                mgr.commit_update(|s| {
                    let id = s.allocate_seq_id();
                    s.idx_segments.push(IdxSegmentMeta {
                        seq_id: id,
                        file_len: 4096,
                        level: 0,
                        record_count: 1,
                    });
                })
                .unwrap();
                let _ = fs
                    .open_file(&idx2, true, true, OpenHint::ImmutableRandom)
                    .unwrap();
                assert_eq!(mgr.active_slot(), SlotIndex::SlotA);
            }

            // Modify ONLY Slot A (leaving Slot B intact valid v1) with recomputed slot CRCs.
            let path = tmp.path.join(BOOT_META_FILE_NAME);
            let mut bytes = std::fs::read(&path).unwrap();
            match case {
                "version" => {
                    bytes[4..6].copy_from_slice(&(FORMAT_VERSION + 1).to_le_bytes());
                }
                "flags" => {
                    let flags = u16::from_le_bytes([bytes[6], bytes[7]]) | 0x0004;
                    bytes[6..8].copy_from_slice(&flags.to_le_bytes());
                }
                "reserved" => {
                    bytes[56..60].copy_from_slice(&1u32.to_le_bytes());
                }
                _ => unreachable!(),
            }
            let header_crc = crc32c(&bytes[0..60]);
            bytes[60..64].copy_from_slice(&header_crc.to_le_bytes());
            let trailer_crc = crc32c(&bytes[0..4092]);
            bytes[4092..4096].copy_from_slice(&trailer_crc.to_le_bytes());
            std::fs::write(&path, &bytes).unwrap();

            let err = BootMetaManager::open_or_recover(Arc::clone(&fs))
                .err()
                .expect("CRC-valid incompatible Slot A must abort open");
            assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{case}: {err}");
            assert!(
                fs.list_files().unwrap().contains(&idx2),
                "{case}: newer-generation segment {idx2} was swept on fallback"
            );
        }
    }

    #[test]
    fn single_incompatible_external_manifest_aborts_open_without_fallback() {
        for case in ["version", "reserved"] {
            // Part 1: Unchecksummed header bit-flips must be caught as Corrupt (CRC mismatch) and
            // fall back cleanly to the standby Slot B generation.
            {
                let tmp = TempTestDir::new("bitflip_incompat_mf");
                let fs = Arc::new(OsFs::open(&tmp.path, true).expect("open OsFs"));
                let mf_seq;
                {
                    let mut mgr = BootMetaManager::open_or_recover(Arc::clone(&fs)).unwrap();
                    mgr.commit_update(|s| {
                        s.checkpoint_lsn = 10;
                    })
                    .unwrap();
                    assert_eq!(mgr.active_slot(), SlotIndex::SlotB);

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
                    .unwrap();
                    assert_eq!(mgr.active_slot(), SlotIndex::SlotA);
                    mf_seq = mgr
                        .state()
                        .active_manifest_seq_id
                        .expect("gen 3 must use external manifest");
                }

                let mf_path = tmp.path.join(format_manifest_file_name(mf_seq));
                let mut mf_bytes = std::fs::read(&mf_path).unwrap();
                match case {
                    "version" => {
                        mf_bytes[4..6].copy_from_slice(&(FORMAT_VERSION + 1).to_le_bytes());
                    }
                    "reserved" => {
                        mf_bytes[6..8].copy_from_slice(&1u16.to_le_bytes());
                    }
                    _ => unreachable!(),
                }
                std::fs::write(&mf_path, &mf_bytes).unwrap();

                let mgr = BootMetaManager::open_or_recover(Arc::clone(&fs)).expect(
                    "unchecksummed manifest header corruption must fall back to standby slot",
                );
                assert_eq!(mgr.active_slot(), SlotIndex::SlotB, "{case}");
                assert_eq!(mgr.state().checkpoint_lsn, 10, "{case}");
            }

            // Part 2: CRC-valid incompatible header fields (both manifest CRC and slot pointer CRC
            // updated) must abort open without falling back or sweeping Gen 3 files.
            let tmp = TempTestDir::new("single_incompat_mf");
            let fs = Arc::new(OsFs::open(&tmp.path, true).expect("open OsFs"));
            let extra_idx_seq;
            let mf_seq;
            {
                let mut mgr = BootMetaManager::open_or_recover(Arc::clone(&fs)).unwrap();
                // Gen 2 in Slot B is inline (empty catalog).
                mgr.commit_update(|s| {
                    s.checkpoint_lsn = 10;
                })
                .unwrap();
                assert_eq!(mgr.active_slot(), SlotIndex::SlotB);

                // Gen 3 in Slot A overflows into an external manifest referencing new idx files.
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
                .unwrap();
                assert_eq!(mgr.active_slot(), SlotIndex::SlotA);
                extra_idx_seq = mgr.state().idx_segments[0].seq_id;
                mf_seq = mgr
                    .state()
                    .active_manifest_seq_id
                    .expect("gen 3 must use external manifest");
            }

            let mf_name = format_manifest_file_name(mf_seq);
            let mf_path = tmp.path.join(&mf_name);
            let mut mf_bytes = std::fs::read(&mf_path).unwrap();
            match case {
                "version" => {
                    mf_bytes[4..6].copy_from_slice(&(FORMAT_VERSION + 1).to_le_bytes());
                }
                "reserved" => {
                    mf_bytes[6..8].copy_from_slice(&1u16.to_le_bytes());
                }
                _ => unreachable!(),
            }
            let updated_crc =
                compute_manifest_crc(&mf_bytes[0..12], &mf_bytes[MANIFEST_HEADER_LEN..]);
            mf_bytes[12..16].copy_from_slice(&updated_crc.to_le_bytes());
            std::fs::write(&mf_path, &mf_bytes).unwrap();

            let meta_path = tmp.path.join(BOOT_META_FILE_NAME);
            let mut raw = std::fs::read(&meta_path).unwrap();
            raw[80..84].copy_from_slice(&updated_crc.to_le_bytes());
            let ptr_crc = crc32c(&raw[64..64 + EXTERNAL_POINTER_LEN]);
            raw[52..56].copy_from_slice(&ptr_crc.to_le_bytes());
            let h_crc = crc32c(&raw[0..60]);
            raw[60..64].copy_from_slice(&h_crc.to_le_bytes());
            let t_crc = crc32c(&raw[0..4092]);
            raw[4092..4096].copy_from_slice(&t_crc.to_le_bytes());
            std::fs::write(&meta_path, &raw).unwrap();

            let err = BootMetaManager::open_or_recover(Arc::clone(&fs))
                .err()
                .expect("CRC-valid incompatible external manifest must abort open");
            assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{case}: {err}");
            let files = fs.list_files().unwrap();
            assert!(files.contains(&mf_name), "{case}: manifest was deleted");
            assert!(
                files.contains(&format_idx_file_name(extra_idx_seq)),
                "{case}: newer-generation idx file was swept on fallback"
            );
        }
    }

    #[test]
    fn open_deferred_sweep_defers_preallocate_and_sync_until_mutation() {
        let tmp = TempTestDir::new("defer_boot_meta");
        let path = tmp.path.join(BOOT_META_FILE_NAME);
        {
            let fs = Arc::new(OsFs::open(&tmp.path, true).expect("open OsFs"));
            let _mgr = BootMetaManager::open_or_recover(fs).unwrap();
        }
        // Truncate after Slot A (keeping valid Slot A in the first 4 KB).
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(SLOT_SIZE as u64)
            .unwrap();

        let log: EventLog = Arc::default();
        let fs = Arc::new(LoggingFs {
            inner: OsFs::open(&tmp.path, true).expect("reopen OsFs"),
            log: Arc::clone(&log),
        });
        let mut mgr = BootMetaManager::open_or_recover_deferred_sweep(fs).unwrap();
        assert!(
            log.lock().unwrap().is_empty(),
            "deferred open must neither preallocate nor sync boot_meta.db: {:?}",
            *log.lock().unwrap()
        );
        assert_eq!(std::fs::metadata(&path).unwrap().len(), SLOT_SIZE as u64);

        mgr.commit_update(|s| s.checkpoint_lsn = 7).unwrap();
        let events = log.lock().unwrap().clone();
        assert_eq!(
            events,
            vec![
                BootMetaEvent::Preallocate(BOOT_META_FILE_SIZE),
                BootMetaEvent::Sync,
                BootMetaEvent::Write(SLOT_SIZE as u64),
                BootMetaEvent::Sync,
            ]
        );
        assert_eq!(std::fs::metadata(&path).unwrap().len(), BOOT_META_FILE_SIZE);
    }

    #[test]
    fn missing_boot_meta_propagates_wal_io_error_without_creating_boot_meta() {
        struct InjectWalIoErrorFs {
            inner: OsFs,
            fail_open: bool,
            fail_len: bool,
        }
        struct InjectWalLenFile {
            inner: Arc<<OsFs as PlatformFs>::File>,
            fail_len: bool,
        }
        impl PlatformFile for InjectWalLenFile {
            fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
                self.inner.read_at(buf, offset)
            }
            fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
                self.inner.read_exact_at(buf, offset)
            }
            fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
                self.inner.write_all_at(buf, offset)
            }
            fn file_len(&self) -> io::Result<u64> {
                if self.fail_len {
                    return Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        "injected wal_sync file_len failure",
                    ));
                }
                self.inner.file_len()
            }
            fn preallocate(&self, target_len: u64, strategy: PreallocStrategy) -> io::Result<()> {
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
        impl PlatformFs for InjectWalIoErrorFs {
            type File = InjectWalLenFile;
            fn open_file(
                &self,
                rel_name: &str,
                create: bool,
                writable: bool,
                hint: OpenHint,
            ) -> io::Result<Arc<Self::File>> {
                if self.fail_open && rel_name == crate::wal::WAL_SYNC_FILE_NAME {
                    return Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        "injected wal_sync open failure",
                    ));
                }
                Ok(Arc::new(InjectWalLenFile {
                    inner: self.inner.open_file(rel_name, create, writable, hint)?,
                    fail_len: self.fail_len && rel_name == crate::wal::WAL_SYNC_FILE_NAME,
                }))
            }
            fn remove_file(&self, rel_name: &str) -> io::Result<()> {
                self.inner.remove_file(rel_name)
            }
            fn list_files(&self) -> io::Result<Vec<String>> {
                self.inner.list_files()
            }
            fn root_path(&self) -> &std::path::Path {
                self.inner.root_path()
            }
        }

        for (fail_open, fail_len) in [(true, false), (false, true)] {
            let tmp = TempTestDir::new("missing_boot_meta_wal_err");
            std::fs::write(tmp.path.join(crate::wal::WAL_SYNC_FILE_NAME), b"wal").unwrap();
            let fs = Arc::new(InjectWalIoErrorFs {
                inner: OsFs::open(&tmp.path, true).expect("open OsFs"),
                fail_open,
                fail_len,
            });
            let err = BootMetaManager::open_or_recover(fs)
                .err()
                .expect("WAL I/O error must propagate instead of creating blank boot_meta");
            assert_eq!(err.kind(), io::ErrorKind::PermissionDenied);
            assert!(
                !tmp.path.join(BOOT_META_FILE_NAME).exists(),
                "boot_meta.db must not be created when missing-meta guard fails on WAL I/O"
            );
        }
    }

    fn craft_raw_catalog_with_pending_delete(name: &str) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&0u32.to_le_bytes()); // idx_segments
        buf.extend_from_slice(&0u32.to_le_bytes()); // std_blob_segments
        buf.extend_from_slice(&0u32.to_le_bytes()); // lru_blob_segments
        buf.extend_from_slice(&0u32.to_le_bytes()); // table_quotas
        buf.extend_from_slice(&1u32.to_le_bytes()); // pending_delete_files = 1
        buf.extend_from_slice(&(name.len() as u16).to_le_bytes());
        buf.extend_from_slice(name.as_bytes());
        buf
    }

    #[test]
    fn decode_rejects_unmanaged_and_referenced_pending_deletes() {
        for bad_name in [
            "boot_meta.db",
            "wal_sync.db",
            "wal_buf.db",
            "db.lock",
            "../escape.db",
            "idx_bad.db",
            "IDX_0000000000000001.db",
            "",
        ] {
            let mut state = BootMetaState::default();
            state.queue_pending_delete(bad_name.to_string());
            assert!(
                state.pending_delete_files.is_empty(),
                "queue_pending_delete must ignore {bad_name:?}"
            );
            let raw = craft_raw_catalog_with_pending_delete(bad_name);
            let err = state
                .decode_catalog_payload(&raw)
                .expect_err("decode must reject non-managed pending_delete filename");
            assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{bad_name:?}");
        }

        // Duplicate segment / quota IDs must also be rejected cleanly.
        let mut dup_idx_payload = Vec::new();
        dup_idx_payload.extend_from_slice(&2u32.to_le_bytes());
        for _ in 0..2 {
            dup_idx_payload.extend_from_slice(&1u64.to_le_bytes());
            dup_idx_payload.extend_from_slice(&4096u64.to_le_bytes());
            dup_idx_payload.extend_from_slice(&0u16.to_le_bytes());
            dup_idx_payload.extend_from_slice(&0u16.to_le_bytes());
            dup_idx_payload.extend_from_slice(&1u32.to_le_bytes());
        }
        dup_idx_payload.extend_from_slice(&[0u8; 16]);
        let mut st = BootMetaState::default();
        assert_eq!(
            st.decode_catalog_payload(&dup_idx_payload)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );

        // Active referenced segment listed in pending_delete_files must demote the slot, and
        // runtime `try_process_pending_deletes` must refuse to unlink non-managed/referenced files.
        let tmp = TempTestDir::new("pending_delete_guard");
        let fs = Arc::new(OsFs::open(&tmp.path, true).expect("open OsFs"));
        let active_idx = format_idx_file_name(1);
        let mut mgr = BootMetaManager::open_or_recover(Arc::clone(&fs)).unwrap();
        mgr.commit_update(|s| {
            s.idx_segments.push(IdxSegmentMeta {
                seq_id: 1,
                file_len: 4096,
                level: 0,
                record_count: 1,
            });
        })
        .unwrap();
        let _active_f = fs
            .open_file(&active_idx, true, true, OpenHint::ImmutableRandom)
            .unwrap();
        drop(_active_f);
        // Inject an unmanaged file + the live segment directly into in-memory state and run GC.
        std::fs::write(tmp.path.join("user_keep.txt"), b"keep").unwrap();
        mgr.state.pending_delete_files.push("boot_meta.db".into());
        mgr.state.pending_delete_files.push("user_keep.txt".into());
        mgr.state.pending_delete_files.push(active_idx.clone());
        mgr.try_process_pending_deletes();
        assert!(mgr.state().pending_delete_files.is_empty());
        assert!(tmp.path.join("boot_meta.db").exists());
        assert!(tmp.path.join("user_keep.txt").exists());
        assert!(tmp.path.join(&active_idx).exists());

        // Now craft Slot A (Gen 99) claiming `idx_segments=[1, 2]` and `pending_delete=[idx_1]` on
        // disk while `idx_2` exists only in Gen 99; reopen must abort with `InvalidData` rather
        // than falling back to Gen 2 Slot B and sweeping `idx_2`.
        drop(mgr);
        let newer_idx = format_idx_file_name(2);
        let _newer_f = fs
            .open_file(&newer_idx, true, true, OpenHint::ImmutableRandom)
            .unwrap();
        drop(_newer_f);
        let mut state = BootMetaState {
            idx_segments: vec![
                IdxSegmentMeta {
                    seq_id: 1,
                    file_len: 4096,
                    level: 0,
                    record_count: 1,
                },
                IdxSegmentMeta {
                    seq_id: 2,
                    file_len: 4096,
                    level: 0,
                    record_count: 1,
                },
            ],
            pending_delete_files: vec![active_idx.clone()],
            ..BootMetaState::default()
        };
        let payload = state.encode_catalog_payload().unwrap();
        state.pending_delete_files.clear();
        let meta_path = tmp.path.join(BOOT_META_FILE_NAME);
        let mut raw_slots = std::fs::read(&meta_path).unwrap();
        // Target Slot A (Gen 1) -> bump to Gen 99 with conflicting active + pending delete.
        raw_slots[8..16].copy_from_slice(&99u64.to_le_bytes());
        raw_slots[48..52].copy_from_slice(&(payload.len() as u32).to_le_bytes());
        raw_slots[52..56].copy_from_slice(&crc32c(&payload).to_le_bytes());
        let h_crc = crc32c(&raw_slots[0..60]);
        raw_slots[60..64].copy_from_slice(&h_crc.to_le_bytes());
        raw_slots[64..64 + payload.len()].copy_from_slice(&payload);
        let t_crc = crc32c(&raw_slots[0..4092]);
        raw_slots[4092..4096].copy_from_slice(&t_crc.to_le_bytes());
        std::fs::write(&meta_path, &raw_slots).unwrap();

        let err = match BootMetaManager::open_or_recover(Arc::clone(&fs)) {
            Ok(_) => panic!("CRC-valid self-contradictory slot must abort open without fallback"),
            Err(e) => e,
        };
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(tmp.path.join(&active_idx).exists());
        assert!(tmp.path.join(&newer_idx).exists());
    }

    #[test]
    fn corrupt_slot_with_u64_max_or_zero_epoch_is_rejected_without_panic() {
        let cases: [(usize, u64); 8] = [
            (8, 0),
            (8, u64::MAX),
            (16, 0),
            (16, u64::MAX),
            (24, u64::MAX),
            (32, 0),
            (32, u64::MAX),
            (40, u64::MAX),
        ];
        for (offset, val) in cases {
            let tmp = TempTestDir::new("u64_max_slot");
            let fs = Arc::new(OsFs::open(&tmp.path, true).expect("open OsFs"));
            {
                let mut mgr = BootMetaManager::open_or_recover(Arc::clone(&fs)).unwrap();
                mgr.commit_update(|s| s.checkpoint_lsn = 42).unwrap();
                assert_eq!(mgr.active_slot(), SlotIndex::SlotB);
            }
            let extra_idx = format_idx_file_name(99);
            std::fs::write(tmp.path.join(&extra_idx), b"gen99").unwrap();
            let meta_path = tmp.path.join(BOOT_META_FILE_NAME);
            let mut raw = std::fs::read(&meta_path).unwrap();
            // Rewrite Slot A (generation=99) with the extreme header value and valid CRCs.
            raw[8..16].copy_from_slice(&99u64.to_le_bytes());
            raw[offset..offset + 8].copy_from_slice(&val.to_le_bytes());
            let h_crc = crc32c(&raw[0..60]);
            raw[60..64].copy_from_slice(&h_crc.to_le_bytes());
            let t_crc = crc32c(&raw[0..4092]);
            raw[4092..4096].copy_from_slice(&t_crc.to_le_bytes());
            std::fs::write(&meta_path, &raw).unwrap();

            let err = match BootMetaManager::open_or_recover(Arc::clone(&fs)) {
                Ok(_) => panic!("offset={offset} val={val:#x}: CRC-valid slot must abort open"),
                Err(e) => e,
            };
            assert_eq!(
                err.kind(),
                io::ErrorKind::InvalidData,
                "offset={offset} val={val:#x}: {err}"
            );
            assert!(
                tmp.path.join(&extra_idx).exists(),
                "offset={offset} val={val:#x}: fallback must not sweep {extra_idx}"
            );
        }

        // An orphan file at SeqID u64::MAX must fail cleanly instead of panicking on next allocate.
        let tmp = TempTestDir::new("seq_u64_max");
        let fs = Arc::new(OsFs::open(&tmp.path, true).expect("open OsFs"));
        let mut mgr = BootMetaManager::open_or_recover(Arc::clone(&fs)).unwrap();
        std::fs::write(tmp.path.join(format_idx_file_name(u64::MAX)), b"alien").unwrap();
        let err = mgr.reserve_seq_ids_above_existing_files().unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn write_rejects_oversized_manifest_and_cleans_up_failed_manifest_file() {
        use std::sync::atomic::{AtomicBool, Ordering};

        // Part 1: catalog exceeding MAX_EXTERNAL_MANIFEST_BYTES (16 MB) fails before allocation or poisoning.
        let tmp = TempTestDir::new("oversized_manifest");
        let fs = Arc::new(OsFs::open(&tmp.path, true).expect("open OsFs"));
        let mut mgr = BootMetaManager::open_or_recover(Arc::clone(&fs)).unwrap();
        let err = mgr
            .commit_update(|s| {
                for seq in 1..=700_000u64 {
                    s.idx_segments.push(IdxSegmentMeta {
                        seq_id: seq,
                        file_len: 4096,
                        level: 0,
                        record_count: 1,
                    });
                }
            })
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(!mgr.is_poisoned());
        assert_eq!(mgr.state().next_seq_id, 1);
        assert!(
            !fs.list_files()
                .unwrap()
                .iter()
                .any(|f| f.starts_with("manifest_"))
        );
        mgr.commit_update(|s| s.checkpoint_lsn = 55).unwrap();
        assert_eq!(mgr.state().checkpoint_lsn, 55);

        // Part 2: failure writing/syncing an external manifest cleans up the file while consuming SeqID.
        struct FailManifestSyncFs {
            inner: OsFs,
            fail_sync: Arc<AtomicBool>,
        }
        struct FailManifestSyncFile {
            inner: Arc<<OsFs as PlatformFs>::File>,
            is_manifest: bool,
            fail_sync: Arc<AtomicBool>,
        }
        impl PlatformFile for FailManifestSyncFile {
            fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
                self.inner.read_at(buf, offset)
            }
            fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
                self.inner.read_exact_at(buf, offset)
            }
            fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
                self.inner.write_all_at(buf, offset)
            }
            fn file_len(&self) -> io::Result<u64> {
                self.inner.file_len()
            }
            fn preallocate(&self, target_len: u64, strategy: PreallocStrategy) -> io::Result<()> {
                self.inner.preallocate(target_len, strategy)
            }
            fn sync_barrier(&self) -> io::Result<()> {
                self.inner.sync_barrier()
            }
            fn sync_durable(&self) -> io::Result<()> {
                if self.is_manifest && self.fail_sync.swap(false, Ordering::SeqCst) {
                    return Err(io::Error::other("simulated manifest sync failure"));
                }
                self.inner.sync_durable()
            }
            fn evict_os_cache(&self, offset: u64, len: u64) -> io::Result<()> {
                self.inner.evict_os_cache(offset, len)
            }
        }
        impl PlatformFs for FailManifestSyncFs {
            type File = FailManifestSyncFile;
            fn open_file(
                &self,
                rel_name: &str,
                create: bool,
                writable: bool,
                hint: OpenHint,
            ) -> io::Result<Arc<Self::File>> {
                Ok(Arc::new(FailManifestSyncFile {
                    inner: self.inner.open_file(rel_name, create, writable, hint)?,
                    is_manifest: rel_name.starts_with("manifest_"),
                    fail_sync: Arc::clone(&self.fail_sync),
                }))
            }
            fn remove_file(&self, rel_name: &str) -> io::Result<()> {
                self.inner.remove_file(rel_name)
            }
            fn list_files(&self) -> io::Result<Vec<String>> {
                self.inner.list_files()
            }
            fn root_path(&self) -> &std::path::Path {
                self.inner.root_path()
            }
        }

        let tmp2 = TempTestDir::new("manifest_cleanup");
        let fail_sync = Arc::new(AtomicBool::new(true));
        let fs2 = Arc::new(FailManifestSyncFs {
            inner: OsFs::open(&tmp2.path, true).unwrap(),
            fail_sync: Arc::clone(&fail_sync),
        });
        let mut mgr2 = BootMetaManager::open_or_recover(Arc::clone(&fs2)).unwrap();
        let err = mgr2
            .commit_update(|s| {
                for i in 1..=200u64 {
                    s.idx_segments.push(IdxSegmentMeta {
                        seq_id: i,
                        file_len: 4096,
                        level: 0,
                        record_count: 1,
                    });
                }
                s.next_seq_id = 201;
            })
            .unwrap_err();
        assert!(err.to_string().contains("simulated manifest sync failure"));
        assert!(!mgr2.is_poisoned());
        // Failed SeqID 201 stays consumed so retry gets 202, and orphan `manifest_*` is gone!
        assert_eq!(mgr2.state().next_seq_id, 202);
        assert!(
            !fs2.list_files()
                .unwrap()
                .iter()
                .any(|f| f.starts_with("manifest_"))
        );
    }

    #[test]
    fn crc_valid_duplicate_seq_id_slot_and_manifest_abort_without_fallback() {
        // Part 1: inline Slot A (Gen 99) with valid CRCs and duplicate idx seq_id must abort
        // open without falling back to Slot B (Gen 2) or sweeping newer-generation segments.
        let tmp1 = TempTestDir::new("dup_seq_inline");
        let fs1 = Arc::new(OsFs::open(&tmp1.path, true).expect("open OsFs"));
        {
            let mut mgr = BootMetaManager::open_or_recover(Arc::clone(&fs1)).unwrap();
            mgr.commit_update(|s| s.checkpoint_lsn = 10).unwrap();
            assert_eq!(mgr.active_slot(), SlotIndex::SlotB);
        }
        let newer_idx1 = format_idx_file_name(50);
        std::fs::write(tmp1.path.join(&newer_idx1), b"gen99").unwrap();

        let mut dup_payload = Vec::new();
        dup_payload.extend_from_slice(&2u32.to_le_bytes());
        for _ in 0..2 {
            dup_payload.extend_from_slice(&50u64.to_le_bytes());
            dup_payload.extend_from_slice(&4096u64.to_le_bytes());
            dup_payload.extend_from_slice(&0u16.to_le_bytes());
            dup_payload.extend_from_slice(&0u16.to_le_bytes());
            dup_payload.extend_from_slice(&1u32.to_le_bytes());
        }
        dup_payload.extend_from_slice(&[0u8; 16]);

        let meta_path1 = tmp1.path.join(BOOT_META_FILE_NAME);
        let mut raw1 = std::fs::read(&meta_path1).unwrap();
        raw1[6..8].copy_from_slice(&0u16.to_le_bytes());
        raw1[8..16].copy_from_slice(&99u64.to_le_bytes());
        raw1[48..52].copy_from_slice(&(dup_payload.len() as u32).to_le_bytes());
        raw1[52..56].copy_from_slice(&crc32c(&dup_payload).to_le_bytes());
        let h_crc1 = crc32c(&raw1[0..60]);
        raw1[60..64].copy_from_slice(&h_crc1.to_le_bytes());
        raw1[64..64 + dup_payload.len()].copy_from_slice(&dup_payload);
        let t_crc1 = crc32c(&raw1[0..4092]);
        raw1[4092..4096].copy_from_slice(&t_crc1.to_le_bytes());
        std::fs::write(&meta_path1, &raw1).unwrap();

        let err1 = match BootMetaManager::open_or_recover(Arc::clone(&fs1)) {
            Ok(_) => panic!("CRC-valid duplicate seq_id inline slot must abort open"),
            Err(e) => e,
        };
        assert_eq!(err1.kind(), io::ErrorKind::InvalidData);
        assert!(tmp1.path.join(&newer_idx1).exists());

        // Part 2: external manifest (Gen 99) with valid CRCs and duplicate idx seq_id must also
        // abort open without falling back to Slot B or sweeping newer-generation segments.
        let tmp2 = TempTestDir::new("dup_seq_manifest");
        let fs2 = Arc::new(OsFs::open(&tmp2.path, true).expect("open OsFs"));
        {
            let mut mgr = BootMetaManager::open_or_recover(Arc::clone(&fs2)).unwrap();
            mgr.commit_update(|s| s.checkpoint_lsn = 10).unwrap();
            assert_eq!(mgr.active_slot(), SlotIndex::SlotB);
        }
        let newer_idx2 = format_idx_file_name(60);
        std::fs::write(tmp2.path.join(&newer_idx2), b"gen99").unwrap();
        let mf_seq = 61u64;
        let mf_name = format_manifest_file_name(mf_seq);
        let mut mf_cat = Vec::new();
        mf_cat.extend_from_slice(&2u32.to_le_bytes());
        for _ in 0..2 {
            mf_cat.extend_from_slice(&60u64.to_le_bytes());
            mf_cat.extend_from_slice(&4096u64.to_le_bytes());
            mf_cat.extend_from_slice(&0u16.to_le_bytes());
            mf_cat.extend_from_slice(&0u16.to_le_bytes());
            mf_cat.extend_from_slice(&1u32.to_le_bytes());
        }
        mf_cat.extend_from_slice(&[0u8; 16]);
        let mut mf_bytes = Vec::with_capacity(MANIFEST_HEADER_LEN + mf_cat.len());
        mf_bytes.extend_from_slice(MANIFEST_MAGIC);
        mf_bytes.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        mf_bytes.extend_from_slice(&0u16.to_le_bytes());
        mf_bytes.extend_from_slice(&(mf_cat.len() as u32).to_le_bytes());
        let mf_crc = compute_manifest_crc(&mf_bytes[0..12], &mf_cat);
        mf_bytes.extend_from_slice(&mf_crc.to_le_bytes());
        mf_bytes.extend_from_slice(&mf_cat);
        std::fs::write(tmp2.path.join(&mf_name), &mf_bytes).unwrap();

        let mut ptr_payload = [0u8; EXTERNAL_POINTER_LEN];
        ptr_payload[0..8].copy_from_slice(&mf_seq.to_le_bytes());
        ptr_payload[8..16].copy_from_slice(&(mf_bytes.len() as u64).to_le_bytes());
        ptr_payload[16..20].copy_from_slice(&mf_crc.to_le_bytes());

        let meta_path2 = tmp2.path.join(BOOT_META_FILE_NAME);
        let mut raw2 = std::fs::read(&meta_path2).unwrap();
        raw2[6..8].copy_from_slice(&FLAG_EXTERNAL_MANIFEST.to_le_bytes());
        raw2[8..16].copy_from_slice(&99u64.to_le_bytes());
        raw2[48..52].copy_from_slice(&(EXTERNAL_POINTER_LEN as u32).to_le_bytes());
        raw2[52..56].copy_from_slice(&crc32c(&ptr_payload).to_le_bytes());
        let h_crc2 = crc32c(&raw2[0..60]);
        raw2[60..64].copy_from_slice(&h_crc2.to_le_bytes());
        raw2[64..64 + EXTERNAL_POINTER_LEN].copy_from_slice(&ptr_payload);
        let t_crc2 = crc32c(&raw2[0..4092]);
        raw2[4092..4096].copy_from_slice(&t_crc2.to_le_bytes());
        std::fs::write(&meta_path2, &raw2).unwrap();

        let err2 = match BootMetaManager::open_or_recover(Arc::clone(&fs2)) {
            Ok(_) => panic!("CRC-valid duplicate seq_id external manifest must abort open"),
            Err(e) => e,
        };
        assert_eq!(err2.kind(), io::ErrorKind::InvalidData);
        assert!(tmp2.path.join(&mf_name).exists());
        assert!(tmp2.path.join(&newer_idx2).exists());
    }
}
