//! `aydb` (*All Your DataBase*) — Windows/SMB-optimized embedded storage engine.
//!
//! Currently implements Phase 1:
//! - Step 1-1: Platform Abstraction Layer (`pal`)
//! - Step 1-2: Atomic Boot Metadata, Hardware CRC32C, and Deferred GC (`meta`, `util`)
//! - Step 1-3: Pre-Allocated Epoch WAL and Two-Stream Crash Recovery (`wal`)
//! - Step 1-4: WiscKey KV Separation, 4 KB Block SSTables, Sparse Fence Pointers,
//!   Bloom Filters, Bounded MemTable, and Merge Compaction (`blob`, `index`, `memtable`)
//! - Step 1-5: Capped Tables (`MAX_BYTES`), Delayed LRU Eviction, Memcomparable Keys,
//!   and Unified Coordinator Engine (`lru`, `engine`)
//! - Phase 2: SQLite Virtual Table (`sqlite3_module`) Adapter (`vtab`)

pub mod blob;
pub mod engine;
pub mod index;
pub mod lru;
pub mod memtable;
pub mod meta;
pub mod pal;
pub mod util;
pub mod vtab;
pub mod wal;
