//! Predecessor link and durable watermark written into every WAL batch (ARCHITECTURE.md 4.6.3
//! and 4.6.4). The tests read both log files back and check the chain of the merged batches.

use super::*;
use crate::pal::OsFs;
use crate::util::test_util::TempTestDir;
use crate::wal::recover_single_wal_stream;
use std::time::Duration;

type Eng = StorageEngine<OsFs>;

const SYNC: WriteDurability = WriteDurability::SyncTransactional;
const BUF: WriteDurability = WriteDurability::BufferedFast;

/// A batch read back from a log file, with the stream it was found in.
struct Logged {
    sync_stream: bool,
    batch: WalBatch,
}

fn open(fs: &Arc<OsFs>, cfg: &EngineConfig) -> Eng {
    StorageEngine::open(Arc::clone(fs), cfg.clone()).unwrap()
}

fn fresh(name: &str, cfg: EngineConfig) -> (TempTestDir, Arc<OsFs>, Eng, EngineConfig) {
    let tmp = TempTestDir::new(name);
    let fs = Arc::new(OsFs::open(&tmp.path, true).unwrap());
    let e = open(&fs, &cfg);
    (tmp, fs, e, cfg)
}

/// Every batch of the current epoch of both log files, merged by LSN.
fn read_log(e: &mut Eng) -> Vec<Logged> {
    e.buf_wal.flush_buffered_to_os().unwrap();
    let mut log = Vec::new();
    for (sync_stream, w) in [(true, &e.sync_wal), (false, &e.buf_wal)] {
        let rec = recover_single_wal_stream(&**w.file(), w.active_epoch()).unwrap();
        log.extend(
            rec.batches
                .into_iter()
                .map(|batch| Logged { sync_stream, batch }),
        );
    }
    log.sort_by_key(|l| l.batch.lsn);
    log
}

/// Checks the chain and the watermark of every batch in the logs and returns them.
///
/// * The first batch links to an LSN at or below the checkpoint, every other one to the batch
///   before it in the merged order, whatever LSN gaps lie between them.
/// * `durable_lsn` never decreases, is below the batch's own LSN, and names a batch of the log or
///   lies at or below the checkpoint.
fn check_links(e: &mut Eng) -> Vec<Logged> {
    let checkpoint = e.meta_mgr.state().checkpoint_lsn;
    let log = read_log(e);
    let lsns: Vec<u64> = log.iter().map(|l| l.batch.lsn).collect();
    for (i, l) in log.iter().enumerate() {
        let b = &l.batch;
        if i == 0 {
            assert!(
                b.prev_lsn <= checkpoint,
                "first batch {} links to {} above the checkpoint {checkpoint}",
                b.lsn,
                b.prev_lsn
            );
        } else {
            assert_eq!(
                b.prev_lsn,
                lsns[i - 1],
                "batch {} breaks the chain: {lsns:?}",
                b.lsn
            );
            assert!(
                b.durable_lsn >= log[i - 1].batch.durable_lsn,
                "durable_lsn decreases at batch {}",
                b.lsn
            );
        }
        assert!(
            b.durable_lsn < b.lsn,
            "durable_lsn {} >= lsn {}",
            b.durable_lsn,
            b.lsn
        );
        assert!(
            b.durable_lsn <= checkpoint || lsns.contains(&b.durable_lsn),
            "durable_lsn {} of batch {} is no batch of {lsns:?} and above checkpoint {checkpoint}",
            b.durable_lsn,
            b.lsn
        );
    }
    log
}

fn put(e: &mut Eng, key: &[u8], durability: WriteDurability) {
    e.put_row(1, key, b"value", &[], durability).unwrap();
}

#[test]
fn interleaved_sync_and_buffered_commits_chain_and_carry_the_watermark() {
    let (_tmp, _fs, mut e, _) = fresh("links_interleaved", EngineConfig::default());
    let base = e.meta_mgr.state().checkpoint_lsn;
    let order = [SYNC, BUF, BUF, SYNC, BUF, SYNC, SYNC, BUF];
    for (i, d) in order.iter().enumerate() {
        put(&mut e, format!("k{i}").as_bytes(), *d);
    }
    let log = check_links(&mut e);
    assert_eq!(log.len(), order.len());

    // A synchronous batch is appended after the buffered stream and its own barrier made
    // everything before it durable. A buffered batch carries the last synchronous LSN.
    let mut last_sync = base;
    for l in &log {
        if l.sync_stream {
            assert_eq!(
                l.batch.durable_lsn, l.batch.prev_lsn,
                "sync batch {}",
                l.batch.lsn
            );
            last_sync = l.batch.lsn;
        } else {
            assert_eq!(
                l.batch.durable_lsn, last_sync,
                "buffered batch {}",
                l.batch.lsn
            );
        }
    }
    assert_eq!(
        log.iter().filter(|l| l.sync_stream).count(),
        order.iter().filter(|d| **d == SYNC).count()
    );
}

#[test]
fn lsn_gaps_inside_a_transaction_do_not_break_the_chain() {
    let (_tmp, _fs, mut e, _) = fresh("links_tx_gaps", EngineConfig::default());
    put(&mut e, b"before", SYNC);
    e.begin_transaction_for_conn(1).unwrap();
    for k in [b"t1", b"t2", b"t3"] {
        e.put_row(1, k, b"value", &[], SYNC).unwrap();
    }
    e.commit_transaction_for_conn(1).unwrap();
    put(&mut e, b"after", BUF);

    let log = check_links(&mut e);
    assert_eq!(log.len(), 3);
    assert!(
        log[1].batch.lsn - log[0].batch.lsn >= 3,
        "the statements of the transaction leave unused LSNs"
    );
    assert_eq!(log[1].batch.prev_lsn, log[0].batch.lsn);
    assert_eq!(log[2].batch.prev_lsn, log[1].batch.lsn);
}

#[test]
fn chunks_of_a_large_transaction_are_ordinary_batches_of_the_chain() {
    for durability in [SYNC, BUF] {
        let mut cfg = EngineConfig::default();
        cfg.inline_threshold_bytes = 1 << 20;
        let (_tmp, _fs, mut e, _) = fresh("links_chunks", cfg);
        put(&mut e, b"before", BUF);
        e.begin_transaction_for_conn(1).unwrap();
        // About 750 KB of inline rows (an inline row must fit a 4 KB index block).
        for i in 0..250u32 {
            let value = vec![i as u8; 3000];
            e.put_row(1, &i.to_le_bytes(), &value, &[], durability)
                .unwrap();
        }
        e.commit_transaction_for_conn(1).unwrap();
        put(&mut e, b"after", durability);

        let log = check_links(&mut e);
        let chunks = log
            .iter()
            .filter(|l| {
                l.batch
                    .records
                    .iter()
                    .any(|r| matches!(r, WalRecord::TxBegin))
            })
            .count();
        assert_eq!(chunks, 1);
        assert!(
            log.len() >= 4,
            "1 + at least 2 chunks + 1, got {}",
            log.len()
        );
        let chunk_batches = log.iter().filter(|l| l.batch.records.len() > 1).count();
        assert!(
            chunk_batches >= 2,
            "the transaction must span several batches"
        );
    }
}

/// A commit that joins the group commit, as the SQL layer drives it.
type Ticket = (u64, Arc<<OsFs as PlatformFs>::File>, Arc<tx::CommitGate>);

fn group_commit(e: &mut Eng, conn: usize, key: &[u8]) -> Ticket {
    e.begin_transaction_for_conn(conn).unwrap();
    e.put_row(1, key, b"value", &[], SYNC).unwrap();
    e.prepare_commit_transaction(SYNC).unwrap();
    match e.begin_commit_for_conn(conn).unwrap() {
        CommitStep::AwaitingGroup { lsn, file, gate } => (lsn, file, gate),
        _ => panic!("a plain synchronous transaction must join the group commit"),
    }
}

fn lead_flush(e: &mut Eng, t: &Ticket) -> u64 {
    let GroupRole::Lead(target) = t.2.join_group(t.0).unwrap() else {
        panic!("nothing has been flushed, so this commit leads");
    };
    e.finish_group_flush(target, t.1.sync_barrier()).unwrap();
    target
}

#[test]
fn group_committed_batches_chain_in_append_order_and_claim_only_flushed_data() {
    let (_tmp, _fs, mut e, _) = fresh("links_group", EngineConfig::default());
    let base = e.meta_mgr.state().checkpoint_lsn;
    let g1 = group_commit(&mut e, 1, b"g1");
    let g2 = group_commit(&mut e, 2, b"g2");
    // Neither batch has been flushed yet, so a batch appended now cannot claim them.
    assert_eq!(lead_flush(&mut e, &g1), g2.0);
    let g3 = group_commit(&mut e, 3, b"g3");
    // A buffered commit first makes the waiting group commit durable.
    e.begin_transaction_for_conn(4).unwrap();
    e.put_row(1, b"b4", b"value", &[], BUF).unwrap();
    e.prepare_commit_transaction(BUF).unwrap();
    assert!(matches!(
        e.begin_commit_for_conn(4).unwrap(),
        CommitStep::Done
    ));
    put(&mut e, b"s5", SYNC);

    let log = check_links(&mut e);
    assert_eq!(log.len(), 5);
    let lsns: Vec<u64> = log.iter().map(|l| l.batch.lsn).collect();
    assert_eq!(lsns[..3], [g1.0, g2.0, g3.0]);
    let durable: Vec<u64> = log.iter().map(|l| l.batch.durable_lsn).collect();
    assert_eq!(durable[0], base);
    assert_eq!(durable[1], base, "g1 was not flushed when g2 was appended");
    assert_eq!(durable[2], g2.0, "the flush covered g1 and g2");
    assert_eq!(durable[3], g3.0, "the buffered commit drained g3 first");
    assert_eq!(durable[4], lsns[3], "everything before s5 is durable");
}

#[test]
fn concurrent_connections_share_group_flushes_and_keep_one_chain() {
    let (_tmp, _fs, e, _) = fresh("links_threads", EngineConfig::default());
    let engine = Arc::new(std::sync::Mutex::new(e));
    let mut workers = Vec::new();
    for conn in 1..=4usize {
        let engine = Arc::clone(&engine);
        workers.push(std::thread::spawn(move || {
            for i in 0..25 {
                let key = format!("c{conn}-{i}");
                let step = loop {
                    let mut eng = engine.lock().unwrap();
                    if eng.begin_transaction_for_conn(conn).is_ok() {
                        eng.put_row(1, key.as_bytes(), b"value", &[], SYNC).unwrap();
                        eng.prepare_commit_transaction(SYNC).unwrap();
                        break eng.begin_commit_for_conn(conn).unwrap();
                    }
                    let gate = eng.commit_gate();
                    let seen = gate.tx_end_count();
                    drop(eng);
                    gate.wait_tx_end(seen, Duration::from_millis(20));
                };
                match step {
                    CommitStep::Done => {}
                    CommitStep::AwaitingSync(file) => {
                        let flushed = file.sync_barrier();
                        engine
                            .lock()
                            .unwrap()
                            .finish_commit_for_conn(flushed)
                            .unwrap();
                    }
                    CommitStep::AwaitingGroup { lsn, file, gate } => loop {
                        match gate.join_group(lsn).unwrap() {
                            GroupRole::Durable => break,
                            GroupRole::Lead(target) => {
                                let flushed = file.sync_barrier();
                                let mut eng = engine.lock().unwrap();
                                eng.finish_group_flush(target, flushed).unwrap();
                            }
                        }
                    },
                }
            }
        }));
    }
    for w in workers {
        w.join().unwrap();
    }
    let mut e = engine.lock().unwrap();
    let log = check_links(&mut e);
    assert_eq!(log.len(), 100);
    assert!(
        log.iter().any(|l| l.batch.durable_lsn > 0),
        "later batches must carry the watermark of earlier flushes"
    );
}

#[test]
fn chain_survives_wal_rotation_in_the_middle() {
    let mut cfg = EngineConfig::default();
    cfg.sync_wal_size_bytes = 8 * 4096;
    cfg.buf_wal_size_bytes = 8 * 4096;
    let (_tmp, _fs, mut e, _) = fresh("links_rotation", cfg);
    let epoch0 = e.sync_wal.active_epoch();
    let value = vec![0x33u8; 600];
    for i in 0..300u32 {
        let d = if i % 3 == 0 { SYNC } else { BUF };
        e.put_row(1, &i.to_le_bytes(), &value, &[], d).unwrap();
        // Checked after every commit, so the state right after a rotation is covered too.
        check_links(&mut e);
    }
    assert!(
        e.sync_wal.active_epoch() > epoch0 + 2,
        "the logs must have wrapped"
    );

    // An explicit flush rotates with an empty MemTable afterwards.
    e.flush_memtable().unwrap();
    put(&mut e, b"after-flush", BUF);
    let log = check_links(&mut e);
    assert_eq!(log.len(), 1);
    let checkpoint = e.meta_mgr.state().checkpoint_lsn;
    assert_eq!(log[0].batch.prev_lsn, log[0].batch.durable_lsn);
    assert!(log[0].batch.prev_lsn <= checkpoint);
}

#[test]
fn group_commit_is_drained_before_a_ring_full_rotation() {
    let mut cfg = EngineConfig::default();
    cfg.sync_wal_size_bytes = 4 * 4096;
    let (_tmp, _fs, mut e, _) = fresh("links_group_rotation", cfg);
    let epoch0 = e.sync_wal.active_epoch();
    let mut tickets = Vec::new();
    for i in 0..12u32 {
        tickets.push(group_commit(&mut e, 1 + (i % 3) as usize, &i.to_le_bytes()));
        check_links(&mut e);
    }
    assert!(
        e.sync_wal.active_epoch() > epoch0,
        "the sync log must have wrapped"
    );
    // The first commit was appended before the first rotation, which flushed it.
    let t = &tickets[0];
    assert!(matches!(t.2.join_group(t.0).unwrap(), GroupRole::Durable));
}

#[test]
fn first_batch_after_reopen_links_to_the_checkpoint() {
    let cfg = EngineConfig::default();
    let (_tmp, fs, mut e, cfg) = fresh("links_reopen", cfg);
    put(&mut e, b"a", SYNC);
    put(&mut e, b"b", BUF);
    put(&mut e, b"c", BUF);
    let last = e.next_lsn() - 1;
    drop(e);

    // Unclean: the replayed batches are checkpointed before the epochs rotate.
    let mut e = open(&fs, &cfg);
    assert_eq!(e.meta_mgr.state().checkpoint_lsn, last);
    put(&mut e, b"d", BUF);
    let log = check_links(&mut e);
    assert_eq!(log.len(), 1);
    assert_eq!(log[0].batch.prev_lsn, last);
    assert_eq!(log[0].batch.durable_lsn, last);
    put(&mut e, b"e", SYNC);
    check_links(&mut e);

    // Clean: a checkpoint, then reopen with nothing to replay.
    e.checkpoint().unwrap();
    let last = e.next_lsn() - 1;
    drop(e);
    let mut e = open(&fs, &cfg);
    put(&mut e, b"f", SYNC);
    let log = check_links(&mut e);
    assert_eq!(log.len(), 1);
    assert_eq!(log[0].batch.prev_lsn, last);
    assert_eq!(log[0].batch.durable_lsn, last);
}

#[test]
fn failed_append_does_not_advance_the_chain() {
    // A single batch larger than the whole log fails without being appended and without
    // poisoning, so the next batch must still link to the last one that was appended.
    let mut cfg = EngineConfig::default();
    cfg.sync_wal_size_bytes = 2 * 4096;
    let (_tmp, _fs, mut e, _) = fresh("links_failed_append", cfg);
    put(&mut e, b"a", SYNC);
    // Twelve secondary index entries of 1 KB each make one batch of about 12 KB.
    let sec: Vec<(u16, Vec<u8>)> = (10..22).map(|i| (i, vec![1u8; 1000])).collect();
    assert!(e.put_row(1, b"big", b"v", &sec, SYNC).is_err());
    assert!(!e.is_poisoned());
    put(&mut e, b"b", SYNC);
    check_links(&mut e);
}
