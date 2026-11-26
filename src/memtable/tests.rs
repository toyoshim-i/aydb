use super::*;
use crate::pal::OsFs;
use crate::util::test_util::TempTestDir;

#[test]
fn cloned_memtables_preserve_entries_and_accounting_after_mutations() {
    let mut original = MemTable::new(4096);
    original.put_inline(b"a".to_vec(), b"original".to_vec(), 1);
    original.put_inline(b"b".to_vec(), b"retained".to_vec(), 2);
    let snapshot = original.clone();
    assert!(Arc::ptr_eq(&original.chunks, &snapshot.chunks));
    let snapshot_bytes = snapshot.approximate_bytes();

    original.put_inline(b"a".to_vec(), b"changed".to_vec(), 3);
    original.delete(b"b".to_vec(), 4);
    let after_upsert = original.clone();
    let after_upsert_bytes = original.approximate_bytes();
    original.absorb(BTreeMap::from([
        (
            b"a".to_vec(),
            MemEntry {
                lsn: 5,
                value: IndexValue::Inline(b"merged".to_vec()),
            },
        ),
        (
            b"c".to_vec(),
            MemEntry {
                lsn: 6,
                value: IndexValue::Inline(b"added".to_vec()),
            },
        ),
    ]));
    let before_clear = original.clone();
    original.clear();

    assert!(original.is_empty());
    assert_eq!(original.approximate_bytes(), 0);
    assert_eq!(
        snapshot.get(b"a").unwrap().value,
        IndexValue::Inline(b"original".to_vec())
    );
    assert_eq!(
        snapshot.get(b"b").unwrap().value,
        IndexValue::Inline(b"retained".to_vec())
    );
    assert_eq!(snapshot.approximate_bytes(), snapshot_bytes);
    assert_eq!(snapshot.highest_lsn(), 2);
    assert_eq!(
        after_upsert.get(b"a").unwrap().value,
        IndexValue::Inline(b"changed".to_vec())
    );
    assert_eq!(after_upsert.get(b"b").unwrap().value, IndexValue::Tombstone);
    assert_eq!(after_upsert.approximate_bytes(), after_upsert_bytes);
    assert_eq!(
        before_clear.get(b"a").unwrap().value,
        IndexValue::Inline(b"merged".to_vec())
    );
    assert_eq!(before_clear.len(), 3);
    assert_eq!(before_clear.highest_lsn(), 6);
}

struct TestRng(u64);

impl TestRng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// Plain-`BTreeMap` model of a `MemTable`.
#[derive(Clone, Default)]
struct Model {
    map: BTreeMap<Vec<u8>, MemEntry>,
    bytes: usize,
    highest_lsn: u64,
    generation: u64,
}

impl Model {
    fn put(&mut self, key: Vec<u8>, entry: MemEntry) -> Option<MemEntry> {
        self.generation += 1;
        self.highest_lsn = self.highest_lsn.max(entry.lsn);
        let new_cost = entry.approx_heap_bytes(key.len());
        let prev = self.map.insert(key.clone(), entry);
        if let Some(old) = &prev {
            self.bytes -= old.approx_heap_bytes(key.len());
        }
        self.bytes += new_cost;
        prev
    }
}

fn model_key(rng: &mut TestRng) -> Vec<u8> {
    match rng.below(40) {
        0 => Vec::new(),
        1 => vec![0xFF, 0xFF],
        2..=5 => format!("k{:03}", rng.below(300)).into_bytes(),
        _ => format!("k{:05}", rng.below(5000)).into_bytes(),
    }
}

fn model_entry(rng: &mut TestRng, lsn: u64) -> MemEntry {
    let value = match rng.below(4) {
        0 => IndexValue::Tombstone,
        1 => IndexValue::Blob(BlobPtr {
            kind: crate::wal::BlobKind::Standard,
            table_id: 0,
            seg_id: rng.below(100),
            offset: 0,
            len: 5000,
            crc32c: 0,
        }),
        _ => IndexValue::Inline(vec![b'v'; rng.below(40) as usize]),
    };
    MemEntry { lsn, value }
}

fn assert_matches_model(mt: &MemTable, model: &Model, rng: &mut TestRng) {
    assert_eq!(mt.len(), model.map.len());
    assert_eq!(mt.is_empty(), model.map.is_empty());
    assert_eq!(mt.approximate_bytes(), model.bytes);
    assert_eq!(mt.highest_lsn(), model.highest_lsn);
    assert_eq!(mt.generation(), model.generation);

    let all = Bound::<&[u8]>::Unbounded;
    let forward: Vec<_> = mt.iter_range(all, all).collect();
    assert!(forward.iter().map(|(k, e)| (*k, *e)).eq(model.map.iter()));
    let backward: Vec<_> = mt.iter_range(all, all).rev().collect();
    assert!(
        backward
            .iter()
            .map(|(k, e)| (*k, *e))
            .eq(model.map.iter().rev())
    );

    // Point reads, hits and misses.
    for _ in 0..40 {
        let key = model_key(rng);
        assert_eq!(mt.get(&key), model.map.get(&key));
    }
    for (k, e) in model.map.iter().step_by(97) {
        assert_eq!(mt.get(k), Some(e));
    }

    // Bounded ranges, consumed from both ends in a random order.
    for _ in 0..30 {
        let (a, b) = (model_key(rng), model_key(rng));
        let (lo_key, hi_key) = if a <= b { (a, b) } else { (b, a) };
        let lo = match rng.below(3) {
            0 => Bound::Unbounded,
            1 => Bound::Included(lo_key.as_slice()),
            _ => Bound::Excluded(lo_key.as_slice()),
        };
        let hi = match rng.below(3) {
            0 => Bound::Unbounded,
            1 => Bound::Included(hi_key.as_slice()),
            _ if lo_key == hi_key && matches!(lo, Bound::Excluded(_)) => Bound::Unbounded,
            _ => Bound::Excluded(hi_key.as_slice()),
        };
        if lo_key == hi_key && matches!((lo, hi), (Bound::Excluded(_), Bound::Excluded(_))) {
            continue;
        }
        let mut got = mt.iter_range(lo, hi);
        let mut want = model.map.range::<[u8], _>((lo, hi));
        loop {
            let (g, w) = if rng.below(2) == 0 {
                (got.next(), want.next())
            } else {
                (got.next_back(), want.next_back())
            };
            assert_eq!(g, w);
            if w.is_none() {
                break;
            }
        }
    }

    // Prefix scans go through the same range machinery.
    let prefix = b"k00";
    let want: Vec<_> = model
        .map
        .iter()
        .filter(|(k, _)| k.starts_with(prefix))
        .map(|(k, e)| (k.clone(), e.lsn))
        .collect();
    let got: Vec<_> = mt
        .scan_prefix(prefix)
        .into_iter()
        .map(|e| (e.key, e.lsn))
        .collect();
    assert_eq!(got, want);
}

#[test]
fn matches_btreemap_model_across_random_operations() {
    for seed in 1..=4u64 {
        let mut rng = TestRng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
        let mut mt = MemTable::new(usize::MAX);
        let mut model = Model::default();
        let mut snapshots: Vec<(MemTable, Model)> = Vec::new();
        let mut lsn = 0u64;
        for step in 0..6000 {
            match rng.below(100) {
                0..=59 => {
                    lsn += 1;
                    let key = model_key(&mut rng);
                    let entry = model_entry(&mut rng, lsn);
                    let prev_model = model.put(key.clone(), entry.clone());
                    let prev = match entry.value {
                        IndexValue::Inline(v) => mt.put_inline(key, v, lsn),
                        IndexValue::Blob(p) => mt.put_blob_ptr(key, p, lsn),
                        IndexValue::Tombstone => mt.delete(key, lsn),
                    };
                    assert_eq!(prev, prev_model);
                }
                60..=79 => {
                    // Alternate between tiny overlays (per-key path) and large ones
                    // (bulk path, possibly spanning and splitting many chunks).
                    let count = if rng.below(4) == 0 {
                        rng.below(1500)
                    } else {
                        rng.below(4)
                    };
                    let mut overlay = BTreeMap::new();
                    for _ in 0..count {
                        lsn += 1;
                        overlay.insert(model_key(&mut rng), model_entry(&mut rng, lsn));
                    }
                    let generation_before = mt.generation();
                    let published = !overlay.is_empty();
                    for (k, e) in &overlay {
                        model.put(k.clone(), e.clone());
                    }
                    mt.absorb(overlay);
                    // Every non-empty publication bumps the generation (the per-key path
                    // may bump it more than once); an empty one must not.
                    assert_eq!(mt.generation() > generation_before, published);
                    model.generation = mt.generation();
                }
                80..=87 => {
                    if snapshots.len() < 8 {
                        snapshots.push((mt.clone(), model.clone()));
                    } else {
                        snapshots.remove(rng.below(8) as usize);
                    }
                }
                88..=89 => {
                    // Clearing keeps the generation, like a flush that moved the content.
                    mt.clear();
                    model.map.clear();
                    model.bytes = 0;
                }
                _ => {}
            }
            if step % 500 == 499 {
                assert_matches_model(&mt, &model, &mut rng);
                for (snap, snap_model) in &snapshots {
                    assert_matches_model(snap, snap_model, &mut rng);
                }
            }
        }
        assert_matches_model(&mt, &model, &mut rng);
        for (snap, snap_model) in &snapshots {
            assert_matches_model(snap, snap_model, &mut rng);
        }
    }
}

#[test]
fn chunks_stay_bounded_and_ordered_for_sequential_and_bulk_loads() {
    let key = |i: usize| format!("key{i:08}").into_bytes();
    let entry = |i: usize| MemEntry {
        lsn: i as u64 + 1,
        value: IndexValue::Inline(vec![1; 8]),
    };
    let mut incremental = MemTable::new(usize::MAX);
    let mut bulk = MemTable::new(usize::MAX);
    let total = 10_000;
    for i in 0..total {
        incremental.put_inline(key(i), vec![1; 8], i as u64 + 1);
    }
    bulk.absorb((0..total).map(|i| (key(i), entry(i))).collect());
    for mt in [&incremental, &bulk] {
        assert_eq!(mt.len(), total);
        assert!(mt.chunks.len() >= total / MAX_CHUNK_ENTRIES);
        assert!(mt.chunks.iter().all(|c| c.map.len() <= MAX_CHUNK_ENTRIES));
        assert!(mt.chunks.windows(2).all(|w| w[0].min < w[1].min));
        assert!(mt.chunks[0].min.is_empty());
        for c in mt.chunks.iter() {
            assert!(c.map.keys().all(|k| k.as_slice() >= &*c.min));
        }
        assert_eq!(mt.get(&key(4321)), Some(&entry(4321)));
    }
}

fn entry_clones() -> usize {
    MEM_ENTRY_CLONES.with(|c| c.get())
}

#[test]
fn write_after_snapshot_does_not_copy_the_whole_table() {
    let total = 20_000usize;
    let mut mt = MemTable::new(usize::MAX);
    for i in 0..total {
        mt.put_inline(
            format!("key{i:08}").into_bytes(),
            vec![7u8; 16],
            i as u64 + 1,
        );
    }
    let snapshot = mt.clone();

    // Updates, inserts, deletes and a small absorb while the snapshot is alive.
    let before = entry_clones();
    mt.put_inline(b"key00010000".to_vec(), b"changed".to_vec(), 50_000);
    let first_write = entry_clones() - before;
    mt.put_inline(b"zzz-new".to_vec(), b"new".to_vec(), 50_001);
    mt.delete(b"key00000007".to_vec(), 50_002);
    mt.absorb(BTreeMap::from([(
        b"key00015000".to_vec(),
        MemEntry {
            lsn: 50_003,
            value: IndexValue::Inline(b"abs".to_vec()),
        },
    )]));
    let copied = entry_clones() - before;

    assert!(
        first_write <= 1024,
        "first write after a snapshot copied {first_write} of {total} entries"
    );
    assert!(
        copied <= 4 * 1024,
        "writes after a snapshot copied {copied} of {total} entries"
    );
    // The snapshot is untouched.
    assert_eq!(
        snapshot.get(b"key00010000").unwrap().value,
        IndexValue::Inline(vec![7u8; 16])
    );
    assert!(snapshot.get(b"zzz-new").is_none());
    assert_eq!(snapshot.len(), total);
    assert_eq!(mt.len(), total + 1);
}

#[test]
fn single_pass_upsert_zero_copy_cursors_and_256kb_coalesced_segment_builder() {
    let tmp = TempTestDir::new("memtable_idx");
    let fs = OsFs::open(&tmp.path, true).expect("open OsFs");

    // Verify single-pass `put_inline`, `delete`, and `apply_single_wal_record`
    // return the displaced `Option<MemEntry>` directly without a separate lookup.
    let mut mt = MemTable::new(16 * 1024 * 1024);
    assert_eq!(mt.put_inline(b"k1".to_vec(), b"v1".to_vec(), 10), None);
    assert_eq!(
        mt.put_inline(b"k1".to_vec(), b"v2".to_vec(), 11),
        Some(MemEntry {
            lsn: 10,
            value: IndexValue::Inline(b"v1".to_vec()),
        })
    );
    assert_eq!(
        mt.delete(b"k1".to_vec(), 12),
        Some(MemEntry {
            lsn: 11,
            value: IndexValue::Inline(b"v2".to_vec()),
        })
    );
    assert_eq!(
        mt.apply_single_wal_record(
            13,
            WalRecord::PutInline {
                key: b"k1".to_vec(),
                value: b"v3".to_vec(),
            },
        ),
        Some(MemEntry {
            lsn: 12,
            value: IndexValue::Tombstone,
        })
    );

    // Populate > 80 blocks (> 320 KB, crossing the 256 KB VACB write coalescing boundary)
    // and flush to an `idx_<SeqID>.db` segment.
    let mut big_mt = MemTable::new(16 * 1024 * 1024);
    let payload = vec![0x5Au8; 256];
    let total_keys = 1200u32; // 1200 * ~280 B > 336 KB (> 82 blocks of 4 KB)
    for i in 0..total_keys {
        let key = format!("coalesce_key_{i:06}").into_bytes();
        big_mt.put_inline(key, payload.clone(), (i + 1) as u64);
    }
    let seg = big_mt
        .flush_to_segment(&fs, 10, 10)
        .expect("flush > 256 KB segment");
    let num_blocks = seg.num_blocks();
    assert!(
        num_blocks > 64,
        "segment must exceed 64 blocks (256 KB) to exercise multi-chunk write coalescing"
    );
    assert_eq!(seg.record_count(), total_keys);
    drop(seg);

    // Reopen from disk to verify all coalesced 256 KB + tail blocks and metadata trailer
    let reopened = IdxSegmentReader::open(&fs, 10).expect("reopen coalesced segment");
    assert_eq!(reopened.num_blocks(), num_blocks);
    assert_eq!(reopened.record_count(), total_keys);

    // Verify zero-copy forward and reverse cursors (`current_key()` and `peek_raw()`)
    let mut fwd = reopened.cursor_all().expect("forward cursor");
    let mut count_fwd = 0u32;
    while let Some(raw) = fwd.peek_raw() {
        let expected_key = format!("coalesce_key_{count_fwd:06}");
        assert_eq!(raw.key, expected_key.as_bytes());
        assert_eq!(fwd.current_key(), Some(expected_key.as_bytes()));
        assert_eq!(raw.lsn, (count_fwd + 1) as u64);
        assert_eq!(raw.val_type, 1);
        assert_eq!(raw.val_bytes, payload.as_slice());
        count_fwd += 1;
        fwd.advance().unwrap();
    }
    assert_eq!(count_fwd, total_keys);

    let mut rev = reopened.cursor_reverse_from(None).expect("reverse cursor");
    let mut count_rev = total_keys;
    while let Some(raw) = rev.peek_raw() {
        count_rev -= 1;
        let expected_key = format!("coalesce_key_{count_rev:06}");
        assert_eq!(raw.key, expected_key.as_bytes());
        assert_eq!(rev.current_key(), Some(expected_key.as_bytes()));
        assert_eq!(raw.lsn, (count_rev + 1) as u64);
        assert_eq!(raw.val_type, 1);
        assert_eq!(raw.val_bytes, payload.as_slice());
        rev.advance().unwrap();
    }
    assert_eq!(count_rev, 0);
}

#[test]
fn iter_range_with_inverted_or_empty_excluded_bounds_returns_empty_without_panicking() {
    for total in [10u32, 300] {
        // Small cap triggers chunking on 300 entries so both single-chunk and multi-chunk paths run.
        let mut mt = MemTable::new(1024);
        for i in 0..total {
            mt.put_inline(
                format!("k_{i:04}").into_bytes(),
                b"payload_bytes".to_vec(),
                (i + 1) as u64,
            );
        }
        assert_eq!(
            mt.iter_range(Bound::Included(b"k_0009"), Bound::Included(b"k_0001"))
                .count(),
            0
        );
        assert_eq!(
            mt.iter_range(Bound::Excluded(b"k_0005"), Bound::Excluded(b"k_0005"))
                .count(),
            0
        );
        assert_eq!(
            mt.iter_range(Bound::Excluded(b"k_0005"), Bound::Included(b"k_0005"))
                .count(),
            0
        );
        assert_eq!(
            mt.iter_range(Bound::Included(b"k_0005"), Bound::Excluded(b"k_0005"))
                .count(),
            0
        );
        assert_eq!(
            mt.iter_range(Bound::Excluded(b"k_0200"), Bound::Excluded(b"k_0010"))
                .count(),
            0
        );
    }
}

#[test]
fn absorb_bulk_split_is_balanced_zero_clone_on_empty_and_tracks_bytes_accurately() {
    let mut entries = BTreeMap::new();
    let val_a = vec![0xAAu8; 20];
    for i in 0..3000u32 {
        let k = format!("bulk_{i:05}").into_bytes();
        entries.insert(
            k,
            MemEntry {
                lsn: (i + 1) as u64,
                value: IndexValue::Inline(val_a.clone()),
            },
        );
    }

    let mut mt = MemTable::new(16 * 1024 * 1024);
    let clones_before = mem_entry_clones();
    mt.absorb(entries);
    assert_eq!(
        mem_entry_clones() - clones_before,
        0,
        "bulk absorb into empty chunk must not clone MemEntry"
    );
    assert_eq!(mt.len(), 3000);
    assert_eq!(mt.highest_lsn(), 3000);
    assert!(mt.chunks.len() >= 6);
    for chunk in mt.chunks.iter() {
        assert!(
            chunk.map.len() <= MAX_CHUNK_ENTRIES,
            "chunk length {} exceeded MAX_CHUNK_ENTRIES",
            chunk.map.len()
        );
    }

    // Second overlapping bulk absorb updates replaced entries accurately.
    let mut updates = BTreeMap::new();
    let val_b = vec![0xBBu8; 40];
    for i in (0..3000u32).step_by(2) {
        let k = format!("bulk_{i:05}").into_bytes();
        updates.insert(
            k,
            MemEntry {
                lsn: 5000 + i as u64,
                value: IndexValue::Inline(val_b.clone()),
            },
        );
    }
    // Also insert 500 new keys.
    for i in 3000..3500u32 {
        let k = format!("bulk_{i:05}").into_bytes();
        updates.insert(
            k,
            MemEntry {
                lsn: 8000 + i as u64,
                value: IndexValue::Inline(val_b.clone()),
            },
        );
    }
    let expected_clones = 0usize;
    let clones_before_second = mem_entry_clones();
    mt.absorb(updates);
    assert_eq!(mem_entry_clones() - clones_before_second, expected_clones);
    assert_eq!(mt.len(), 3500);
    assert_eq!(mt.highest_lsn(), 8000 + 3499);

    let expected_bytes: usize = mt
        .iter_range(Bound::Unbounded, Bound::Unbounded)
        .map(|(k, e)| e.approx_heap_bytes(k.len()))
        .sum();
    assert_eq!(mt.approximate_bytes(), expected_bytes);
    for i in 0..3500u32 {
        let k = format!("bulk_{i:05}").into_bytes();
        let got = mt.get(&k).unwrap();
        let expected_val = if i >= 3000 || i % 2 == 0 {
            &val_b
        } else {
            &val_a
        };
        assert_eq!(got.value, IndexValue::Inline(expected_val.clone()));
    }
}

#[test]
fn empty_memtable_does_not_trigger_should_flush_even_with_zero_capacity() {
    let mut mt = MemTable::new(0);
    assert!(mt.is_empty());
    assert!(!mt.should_flush());

    mt.put_inline(b"k".to_vec(), b"v".to_vec(), 1);
    assert!(!mt.is_empty());
    assert!(mt.should_flush());
}
