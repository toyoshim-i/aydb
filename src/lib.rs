//! `aydb` (*All Your DataBase*) — Windows/SMB-optimized embedded storage engine.
//!
//! Currently implements Phase 1:
//! - Step 1-1: Platform Abstraction Layer (`pal`)
//! - Step 1-2: Atomic Boot Metadata, Hardware CRC32C, and Deferred GC (`meta`, `util`)
//! - Step 1-3: Pre-Allocated Epoch WAL and Two-Stream Crash Recovery (`wal`)

pub mod meta;
pub mod pal;
pub mod util;
pub mod wal;
