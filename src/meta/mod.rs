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
    if !is_managed_segment_or_manifest(file_name) {
        return None;
    }
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
    fn check_header_bounds(&self) -> io::Result<()> {
        if self.generation == 0
            || self.generation == u64::MAX
            || self.next_seq_id == 0
            || self.next_seq_id == u64::MAX
            || self.checkpoint_lsn == u64::MAX
            || self.sync_wal_epoch == 0
            || self.sync_wal_epoch == u64::MAX
            || self.buf_wal_epoch == 0
            || self.buf_wal_epoch == u64::MAX
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "BootMetaState header counter or epoch out of valid bounds",
            ));
        }
        Ok(())
    }

    /// Allocates and returns a fresh, monotonically increasing `SeqID`, failing cleanly if
    /// allocating would reach the reserved `u64::MAX` sentinel.
    pub fn try_allocate_seq_id(&mut self) -> io::Result<u64> {
        if self.next_seq_id >= u64::MAX - 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "64-bit SeqID exhausted",
            ));
        }
        let id = self.next_seq_id;
        self.next_seq_id += 1;
        Ok(id)
    }

    /// Allocates and returns a fresh, monotonically increasing `SeqID`.
    pub fn allocate_seq_id(&mut self) -> u64 {
        self.try_allocate_seq_id().expect("64-bit SeqID overflow")
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
        const MIN_MANAGED_FILE_NAME_LEN: usize = 23; // "idx_" + 16 hex + ".db"
        let num_deletes = r.read_u32_le()? as usize;
        if num_deletes
            .checked_mul(2 + MIN_MANAGED_FILE_NAME_LEN)
            .is_none_or(|b| b > r.remaining())
        {
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
        let mut seen_deletes = HashSet::new();
        seen_deletes.try_reserve(num_deletes).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "pending_delete_files seen-set allocation overflow",
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
            if !seen_deletes.insert(name) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "duplicate filename in pending_delete_files",
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
    /// `sync_durable` are deferred until `ensure_durable`, [`Self::commit_update`], or
    /// [`Self::sweep_orphans`] so `StorageEngine::open` under `WalDamagePolicy::Fail` can inspect
    /// the WAL and refuse damaged databases without altering `boot_meta.db` or directory state.
    pub fn open_or_recover_deferred_sweep(fs: Arc<F>) -> io::Result<Self> {
        let mut created_via_not_found = false;
        let file = match fs.open_file(BOOT_META_FILE_NAME, false, true, OpenHint::MutableSyncLog) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                // Guard against a missing `boot_meta.db` destroying an existing database before
                // creating the file or calling `sync_dir`.
                Self::ensure_no_existing_db_files(&fs)?;
                created_via_not_found = true;
                fs.open_file(BOOT_META_FILE_NAME, true, true, OpenHint::MutableSyncLog)?
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
            // Keep the exclusive handle held while syncing the parent directory so another opener
            // cannot initialize or populate the database between close and reopen.
            if !created_via_not_found {
                fs.sync_dir()?;
            }
            return Self::init_fresh_boot_meta(fs, file);
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
            Some(m) if m >= u64::MAX - 1 => Err(io::Error::new(
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

    /// Takes the next `SeqID` in memory, before the file that will carry it is created, returning
    /// an error instead of panicking if 64-bit sequence IDs are exhausted.
    pub fn try_reserve_seq_id(&mut self) -> io::Result<u64> {
        self.state.try_allocate_seq_id()
    }

    /// Takes the next `SeqID` in memory, before the file that will carry it is created. The id
    /// stays consumed whatever happens to that file, so a failed creation (which can leave the
    /// file behind with a directory entry that is not durable) is never retried under the same
    /// name. The next commit persists the advanced counter; a crash before that is covered by
    /// `reserve_seq_ids_above_existing_files` at the next open.
    pub fn reserve_seq_id(&mut self) -> u64 {
        self.try_reserve_seq_id().expect("64-bit SeqID overflow")
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
        next_state.generation = self
            .state
            .generation
            .checked_add(1)
            .filter(|&g| g < u64::MAX)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "64-bit generation overflow")
            })?;
        next_state.check_header_bounds()?;

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
        let mut unlinked = Vec::new();
        for file_name in self.state.pending_delete_files.drain(..) {
            if !is_managed_segment_or_manifest(&file_name)
                || referenced.contains(&file_name)
                || standby_mf.as_deref() == Some(file_name.as_str())
            {
                continue;
            }
            if self.fs.remove_file_unsynced(&file_name).is_ok() {
                unlinked.push(file_name);
            } else {
                remaining.push(file_name);
            }
        }
        if !unlinked.is_empty() && self.fs.sync_dir().is_err() {
            // A crash can restore these names until the directory flush succeeds.
            remaining.append(&mut unlinked);
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
        state.check_header_bounds()?;
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
            if state.next_seq_id >= u64::MAX - 1 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "64-bit SeqID overflow",
                ));
            }
            // Overflow to an immutable external manifest file FIRST before updating boot_meta.db!
            let mf_seq_id = state.try_allocate_seq_id()?;
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

        state.check_header_bounds()?;
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
            // A partial overwrite can replace a header together with its own CRC while
            // leaving the old payload and trailer. The payload CRC detects that mixture.
            return Err(SlotLoadError::Corrupt(io::Error::new(
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
mod tests;
