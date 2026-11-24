//! `aydb` (*All Your DataBase*) — Windows/SMB-optimized embedded storage engine.
//!
//! Phase 1 & Phase 2 modules:
//! - PAL: Platform Abstraction Layer (`pal`)
//! - Boot Metadata & Utilities: Atomic Boot Metadata, Hardware CRC32C, and Deferred GC (`meta`, `util`)
//! - Epoch WAL: Pre-Allocated Two-Stream WAL and Crash Recovery (`wal`)
//! - Storage & Caching: WiscKey Blob Segments, 4 KB Block SSTables, Bloom Filters,
//!   Bounded Verified Block/Blob Caches, Bounded MemTable, and Compaction (`blob`, `cache`, `index`, `memtable`)
//! - Coordinator Engine & Capped Tables: `MAX_BYTES` LRU Eviction, Memcomparable Keys,
//!   and Unified Engine (`lru`, `engine`)
//! - SQLite Adapter: `sqlite3_module` Virtual Table & Connection Wrapper (`vtab`)

#![allow(
    clippy::collapsible_if,
    clippy::field_reassign_with_default,
    clippy::needless_range_loop,
    clippy::map_entry
)]

pub mod blob;
pub mod cache;
pub mod engine;
pub mod index;
pub mod lru;
pub mod memtable;
pub mod meta;
pub mod pal;
pub mod util;
pub mod vtab;
pub mod wal;
