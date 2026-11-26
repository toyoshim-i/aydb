//! Predecessor link and durable watermark of the WAL (ARCHITECTURE.md 4.6.3 and 4.6.4).
//!
//! Every batch records `prev_lsn` (the batch appended just before it, across both streams) and
//! `durable_lsn` (the highest LSN known durable at append time). Both are kept here, under the
//! engine lock that already serializes appends, and cost no extra sync: the watermark only
//! records what the existing flush ordering has already established.

/// The oldest batch of one stream that no successful device sync is known to cover.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Unsynced {
    /// Every batch of the stream with a smaller LSN is durable.
    lsn: u64,
    /// A real batch LSN (or at most the checkpoint LSN) below `lsn`. Once every batch below
    /// `lsn` is durable in both streams, this is a durable LSN that names a real batch.
    prev: u64,
}

/// The two log streams.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum LogStream {
    Sync,
    Buffered,
}

#[derive(Debug)]
pub(super) struct WalLinks {
    /// LSN of the batch appended last, in either stream (or the checkpoint LSN after open).
    last_appended: u64,
    /// Every batch with an LSN at or below this value has reached the device in its stream.
    durable: u64,
    /// LSN of the last batch appended to `wal_sync.db`.
    last_sync_append: u64,
    sync_head: Option<Unsynced>,
    buf_head: Option<Unsynced>,
}

impl WalLinks {
    /// `base` is the LSN below which nothing is left in the WAL: after `open` everything that was
    /// replayed is checkpointed before the epochs rotate, so it is the highest recovered LSN.
    pub(super) fn new(base: u64) -> Self {
        Self {
            last_appended: base,
            durable: base,
            last_sync_append: 0,
            sync_head: None,
            buf_head: None,
        }
    }

    /// The `(prev_lsn, durable_lsn)` header fields of the next batch.
    pub(super) fn stamp(&self) -> (u64, u64) {
        (self.last_appended, self.durable)
    }

    /// Records a batch that was appended successfully. `synced` tells that the append itself ran
    /// a device sync of its stream (a synchronous append without a deferred barrier), which also
    /// covers every earlier write to that file.
    pub(super) fn appended(&mut self, stream: LogStream, lsn: u64, prev: u64, synced: bool) {
        self.last_appended = lsn;
        let head = match stream {
            LogStream::Sync => {
                self.last_sync_append = lsn;
                &mut self.sync_head
            }
            LogStream::Buffered => &mut self.buf_head,
        };
        if synced {
            *head = None;
        } else if head.is_none() {
            *head = Some(Unsynced { lsn, prev });
        }
        self.advance();
    }

    /// A sync of `wal_buf.db` returned success: every buffered batch appended so far is durable.
    pub(super) fn buf_synced(&mut self) {
        self.buf_head = None;
        self.advance();
    }

    /// A sync of `wal_sync.db` returned success and covers every batch up to `through`.
    pub(super) fn sync_flushed(&mut self, through: u64) {
        if let Some(head) = self.sync_head {
            if head.lsn <= through {
                // Batches appended after the flush started are not covered. `through` is a real
                // batch LSN, so it can name the new boundary.
                self.sync_head = (self.last_sync_append > through).then_some(Unsynced {
                    lsn: through + 1,
                    prev: through,
                });
            }
        }
        self.advance();
    }

    /// A sync of `wal_sync.db` returned success after every append to it so far.
    pub(super) fn sync_flushed_all(&mut self) {
        self.sync_flushed(self.last_sync_append);
    }

    /// The WAL epochs rotated: every batch appended so far is covered by the checkpoint.
    pub(super) fn rotated(&mut self) {
        self.sync_head = None;
        self.buf_head = None;
        self.advance();
    }

    /// Raises `durable` to the last LSN below the globally oldest batch without a covering sync.
    fn advance(&mut self) {
        let bound = match (self.sync_head, self.buf_head) {
            (None, None) => self.last_appended,
            (Some(h), None) | (None, Some(h)) => h.prev,
            (Some(s), Some(b)) => {
                if s.lsn <= b.lsn {
                    s.prev
                } else {
                    b.prev
                }
            }
        };
        self.durable = self.durable.max(bound);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn starts_at_the_base_with_everything_durable() {
        let l = WalLinks::new(7);
        assert_eq!(l.stamp(), (7, 7));
    }

    #[test]
    fn synchronous_append_with_its_own_sync_is_durable_at_once() {
        let mut l = WalLinks::new(0);
        l.appended(LogStream::Sync, 1, 0, true);
        assert_eq!(l.stamp(), (1, 1));
    }

    #[test]
    fn buffered_batches_stay_below_the_watermark_until_a_sync() {
        let mut l = WalLinks::new(0);
        l.appended(LogStream::Sync, 1, 0, true);
        l.appended(LogStream::Buffered, 2, 1, false);
        l.appended(LogStream::Buffered, 3, 2, false);
        assert_eq!(l.stamp(), (3, 1));
        l.buf_synced();
        assert_eq!(l.stamp(), (3, 3));
    }

    #[test]
    fn a_stream_cannot_advance_the_watermark_past_the_other_streams_gap() {
        let mut l = WalLinks::new(0);
        l.appended(LogStream::Buffered, 1, 0, false);
        l.appended(LogStream::Sync, 2, 1, true);
        // The sync of wal_sync.db says nothing about batch 1.
        assert_eq!(l.stamp(), (2, 0));
        l.buf_synced();
        assert_eq!(l.stamp(), (2, 2));
    }

    #[test]
    fn group_flush_covers_only_batches_up_to_its_target() {
        let mut l = WalLinks::new(0);
        l.appended(LogStream::Sync, 1, 0, false);
        l.appended(LogStream::Sync, 2, 1, false);
        l.appended(LogStream::Sync, 3, 2, false);
        assert_eq!(l.stamp(), (3, 0));
        l.sync_flushed(2);
        assert_eq!(l.stamp(), (3, 2));
        l.sync_flushed(3);
        assert_eq!(l.stamp(), (3, 3));
    }

    #[test]
    fn a_batch_between_group_batches_holds_the_watermark_back() {
        let mut l = WalLinks::new(0);
        l.appended(LogStream::Sync, 1, 0, false);
        l.appended(LogStream::Buffered, 2, 1, false);
        l.appended(LogStream::Sync, 3, 2, false);
        l.sync_flushed(3);
        // Batch 2 is not durable: only batch 1 is known to be.
        assert_eq!(l.stamp(), (3, 1));
    }

    #[test]
    fn rotation_makes_everything_appended_so_far_durable() {
        let mut l = WalLinks::new(0);
        l.appended(LogStream::Sync, 1, 0, false);
        l.appended(LogStream::Buffered, 2, 1, false);
        l.rotated();
        assert_eq!(l.stamp(), (2, 2));
    }

    #[test]
    fn export_formal_traces_wal_links() {
        use core::fmt::Write as _;

        fn emit_step(
            out: &mut String,
            seq: &mut u64,
            links: &mut WalLinks,
            event_json: &str,
            apply: impl FnOnce(&mut WalLinks),
        ) {
            let prev_durable = links.stamp().1;
            apply(links);
            let stamp = links.stamp();
            assert!(stamp.1 >= prev_durable);
            writeln!(
                out,
                "{{\"kind\":\"step\",\"seq\":{},\"event\":{event_json},\"stamp\":[{},{}]}}",
                *seq, stamp.0, stamp.1
            )
            .unwrap();
            *seq += 1;
        }

        let mut out = String::from("{\"kind\":\"header\",\"schema\":1,\"fsm\":\"wal_links\"}\n");
        let mut seq = 0u64;

        // Scenario 1: Base 0, synchronous & buffered interleaving, cross-stream gap, partial & full flush.
        writeln!(out, "{{\"kind\":\"init\",\"base\":0}}").unwrap();
        let mut links = WalLinks::new(0);
        emit_step(
            &mut out,
            &mut seq,
            &mut links,
            "{\"tag\":\"Appended\",\"stream\":\"Sync\",\"lsn\":1,\"prev\":0,\"synced\":true}",
            |l| l.appended(LogStream::Sync, 1, 0, true),
        );
        emit_step(
            &mut out,
            &mut seq,
            &mut links,
            "{\"tag\":\"Appended\",\"stream\":\"Buffered\",\"lsn\":2,\"prev\":1,\"synced\":false}",
            |l| l.appended(LogStream::Buffered, 2, 1, false),
        );
        emit_step(
            &mut out,
            &mut seq,
            &mut links,
            "{\"tag\":\"Appended\",\"stream\":\"Buffered\",\"lsn\":3,\"prev\":2,\"synced\":false}",
            |l| l.appended(LogStream::Buffered, 3, 2, false),
        );
        emit_step(
            &mut out,
            &mut seq,
            &mut links,
            "{\"tag\":\"BufSynced\"}",
            |l| l.buf_synced(),
        );
        emit_step(
            &mut out,
            &mut seq,
            &mut links,
            "{\"tag\":\"Appended\",\"stream\":\"Buffered\",\"lsn\":4,\"prev\":3,\"synced\":false}",
            |l| l.appended(LogStream::Buffered, 4, 3, false),
        );
        emit_step(
            &mut out,
            &mut seq,
            &mut links,
            "{\"tag\":\"Appended\",\"stream\":\"Sync\",\"lsn\":5,\"prev\":4,\"synced\":true}",
            |l| l.appended(LogStream::Sync, 5, 4, true),
        );
        emit_step(
            &mut out,
            &mut seq,
            &mut links,
            "{\"tag\":\"BufSynced\"}",
            |l| l.buf_synced(),
        );
        emit_step(
            &mut out,
            &mut seq,
            &mut links,
            "{\"tag\":\"Appended\",\"stream\":\"Sync\",\"lsn\":6,\"prev\":5,\"synced\":false}",
            |l| l.appended(LogStream::Sync, 6, 5, false),
        );
        emit_step(
            &mut out,
            &mut seq,
            &mut links,
            "{\"tag\":\"Appended\",\"stream\":\"Sync\",\"lsn\":7,\"prev\":6,\"synced\":false}",
            |l| l.appended(LogStream::Sync, 7, 6, false),
        );
        emit_step(
            &mut out,
            &mut seq,
            &mut links,
            "{\"tag\":\"Appended\",\"stream\":\"Sync\",\"lsn\":8,\"prev\":7,\"synced\":false}",
            |l| l.appended(LogStream::Sync, 8, 7, false),
        );
        emit_step(
            &mut out,
            &mut seq,
            &mut links,
            "{\"tag\":\"SyncFlushed\",\"through\":7}",
            |l| l.sync_flushed(7),
        );
        emit_step(
            &mut out,
            &mut seq,
            &mut links,
            "{\"tag\":\"SyncFlushedAll\"}",
            |l| l.sync_flushed_all(),
        );

        // Scenario 2: Non-zero base after checkpoint, mixed gap held back across sync flush, then rotated.
        writeln!(out, "{{\"kind\":\"init\",\"base\":7}}").unwrap();
        let mut links2 = WalLinks::new(7);
        emit_step(
            &mut out,
            &mut seq,
            &mut links2,
            "{\"tag\":\"Appended\",\"stream\":\"Sync\",\"lsn\":8,\"prev\":7,\"synced\":false}",
            |l| l.appended(LogStream::Sync, 8, 7, false),
        );
        emit_step(
            &mut out,
            &mut seq,
            &mut links2,
            "{\"tag\":\"Appended\",\"stream\":\"Buffered\",\"lsn\":9,\"prev\":8,\"synced\":false}",
            |l| l.appended(LogStream::Buffered, 9, 8, false),
        );
        emit_step(
            &mut out,
            &mut seq,
            &mut links2,
            "{\"tag\":\"Appended\",\"stream\":\"Sync\",\"lsn\":10,\"prev\":9,\"synced\":false}",
            |l| l.appended(LogStream::Sync, 10, 9, false),
        );
        emit_step(
            &mut out,
            &mut seq,
            &mut links2,
            "{\"tag\":\"SyncFlushed\",\"through\":10}",
            |l| l.sync_flushed(10),
        );
        emit_step(
            &mut out,
            &mut seq,
            &mut links2,
            "{\"tag\":\"Rotated\"}",
            |l| l.rotated(),
        );

        assert!(seq >= 17);
        if let Some(dir) = std::env::var_os("AYDB_EXPORT_TRACES_DIR") {
            if !dir.is_empty() {
                let dir_path = std::path::Path::new(&dir);
                std::fs::create_dir_all(dir_path).expect("create AYDB_EXPORT_TRACES_DIR");
                std::fs::write(dir_path.join("wal_links_scenarios.ndjson"), &out)
                    .expect("write wal_links_scenarios.ndjson");
            }
        }
    }
}
