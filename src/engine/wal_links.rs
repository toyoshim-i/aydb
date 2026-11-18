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
pub(crate) enum LogStream {
    Sync,
    Buffered,
}

#[derive(Debug)]
pub(crate) struct WalLinks {
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
}
