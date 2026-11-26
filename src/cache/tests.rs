use super::*;

#[test]
fn block_cache_hit_miss_clock_eviction_and_segment_invalidation() {
    // Budget for 16 shards * 2 slots = 32 slots * 4096 = 131,072 bytes
    let cache = VerifiedBlockCache::new(32 * IDX_BLOCK_SIZE);
    assert!(cache.is_enabled());

    let mut block1 = [0x11u8; IDX_BLOCK_SIZE];
    block1[0] = 42;
    cache.insert(1, 0, &block1);

    let mut out = [0u8; IDX_BLOCK_SIZE];
    assert!(cache.get_into(1, 0, &mut out));
    assert_eq!(out[0], 42);
    assert_eq!(cache.hits(), 1);

    assert!(!cache.get_into(1, 1, &mut out));
    assert_eq!(cache.misses(), 1);

    // Invalidate segment 1 wholesale
    cache.invalidate_segment(1);
    assert!(!cache.get_into(1, 0, &mut out));
    assert_eq!(cache.misses(), 2);
}

#[test]
fn block_cache_get_arc_counts_hits_and_keeps_held_blocks_stable() {
    // One slot per shard, so a second block hashed to the same shard replaces the first.
    let cache = VerifiedBlockCache::new(NUM_SHARDS * IDX_BLOCK_SIZE);
    assert!(cache.get_arc(1, 0).is_none());
    assert_eq!((cache.hits(), cache.misses()), (0, 1));

    cache.insert(1, 0, &[0x11u8; IDX_BLOCK_SIZE]);
    let held = cache.get_arc(1, 0).expect("resident block");
    assert_eq!(held[0], 0x11);
    assert_eq!((cache.hits(), cache.misses()), (1, 1));

    // Re-inserting the same key and evicting it by other keys must not change the block that
    // a reader already holds.
    cache.insert(1, 0, &[0x22u8; IDX_BLOCK_SIZE]);
    assert_eq!(held[0], 0x11);
    assert_eq!(cache.get_arc(1, 0).unwrap()[0], 0x22);
    for blk in 1..(NUM_SHARDS as u32 * 8) {
        cache.insert(1, blk, &[0x33u8; IDX_BLOCK_SIZE]);
    }
    assert!(cache.get_arc(1, 0).is_none());
    assert_eq!(held[0], 0x11);
    assert!(held.iter().all(|&b| b == 0x11));

    let mut out = [0u8; IDX_BLOCK_SIZE];
    let hits = cache.hits();
    assert!(!cache.get_into(1, 0, &mut out));
    assert_eq!(cache.hits(), hits);
}

#[test]
fn block_cache_disabled_when_zero_bytes() {
    let cache = VerifiedBlockCache::new(0);
    assert!(!cache.is_enabled());

    let block = [0x55u8; IDX_BLOCK_SIZE];
    cache.insert(1, 0, &block);
    let mut out = [0u8; IDX_BLOCK_SIZE];
    assert!(!cache.get_into(1, 0, &mut out));
}

#[test]
fn blob_cache_hit_miss_and_segment_invalidation() {
    // Budget 64 KB
    let cache = VerifiedBlobCache::new(64 * 1024);
    assert!(cache.is_enabled());

    let key1 = BlobCacheKey {
        kind: crate::wal::BlobKind::Lru as u8,
        table_id: 10,
        seg_id: 100,
        offset: 4096,
        len: 32,
        crc32c: 0x1111_2222,
        key_hash: 0xAAAA_BBBB,
    };
    let payload1 = b"hello blob cache";
    cache.insert(key1, payload1);

    let mut out = Vec::new();
    assert!(cache.get_append_into(&key1, &mut out));
    assert_eq!(out, payload1);
    assert_eq!(cache.hits(), 1);

    let key_absent = BlobCacheKey {
        kind: crate::wal::BlobKind::Lru as u8,
        table_id: 10,
        seg_id: 100,
        offset: 9999,
        len: 32,
        crc32c: 0x1111_2222,
        key_hash: 0xAAAA_BBBB,
    };
    assert!(!cache.get_append_into(&key_absent, &mut out));
    assert_eq!(cache.misses(), 1);

    // Invalidate segment 100
    cache.invalidate_blob_segment(crate::wal::BlobKind::Lru as u8, 10, 100);
    out.clear();
    assert!(!cache.get_append_into(&key1, &mut out));
}

#[test]
fn blob_cache_rejects_payload_larger_than_16kb() {
    let cache = VerifiedBlobCache::new(64 * 1024);
    let key = BlobCacheKey {
        kind: crate::wal::BlobKind::Lru as u8,
        table_id: 1,
        seg_id: 1,
        offset: 0,
        len: (17 * 1024) as u32,
        crc32c: 0x5555_6666,
        key_hash: 0xCCCC_DDDD,
    };
    let big_payload = vec![0xAAu8; 17 * 1024];
    cache.insert(key, &big_payload);

    let mut out = Vec::new();
    assert!(!cache.get_append_into(&key, &mut out));
}

#[test]
fn block_cache_invalidate_releases_arc_and_blob_cache_key_distinguishes_crc_len_key() {
    let block_cache = VerifiedBlockCache::new(1024 * 1024);
    let block = [0x3Cu8; IDX_BLOCK_SIZE];
    block_cache.insert(7, 0, &block);
    let held = block_cache.get_arc(7, 0).unwrap();
    // `held` + cache slot hold two strong references.
    assert_eq!(Arc::strong_count(&held), 2);
    block_cache.invalidate_segment(7);
    assert_eq!(Arc::strong_count(&held), 1);

    let blob_cache = VerifiedBlobCache::new(64 * 1024);
    let base_ptr = crate::wal::BlobPtr {
        kind: crate::wal::BlobKind::Standard,
        table_id: 0,
        seg_id: 5,
        offset: 128,
        len: 64,
        crc32c: 0x1234_5678,
    };
    let std_key = BlobCacheKey::from_ptr(&base_ptr, b"row_a");
    blob_cache.insert(std_key, b"payload_a");

    // Standard blobs (`table_id == 0`) must not be wiped by `invalidate_table_blobs(0)`.
    blob_cache.invalidate_table_blobs(0);
    let mut out = Vec::new();
    assert!(blob_cache.get_append_into(&std_key, &mut out));
    assert_eq!(out, b"payload_a");

    // Same `(kind, table_id, seg_id, offset)` with different len / crc32c / key must miss.
    let mut diff_len = base_ptr;
    diff_len.len = 65;
    assert!(!blob_cache.contains(&BlobCacheKey::from_ptr(&diff_len, b"row_a")));

    let mut diff_crc = base_ptr;
    diff_crc.crc32c ^= 1;
    assert!(!blob_cache.contains(&BlobCacheKey::from_ptr(&diff_crc, b"row_a")));

    assert!(!blob_cache.contains(&BlobCacheKey::from_ptr(&base_ptr, b"row_b")));
}

#[test]
fn cache_shards_are_cache_line_aligned() {
    assert!(std::mem::align_of::<CachePaddedMutex<BlockClockShard>>() >= 128);
    assert!(std::mem::align_of::<CachePaddedMutex<BlobClockShard>>() >= 128);
}

#[test]
fn blob_cache_reuses_free_slots_and_compacts_after_shrink_and_invalidation() {
    let cache = VerifiedBlobCache::new(256 * 1024);
    let lru_kind = crate::wal::BlobKind::Lru as u8;

    let small_payload = vec![0x42u8; 64];
    for i in 0..3200u32 {
        let key = BlobCacheKey {
            kind: lru_kind,
            table_id: 1,
            seg_id: 1,
            offset: i * 128,
            len: 64,
            crc32c: i,
            key_hash: i as u64 + 1,
        };
        cache.insert(key, &small_payload);
    }

    let mut large_entries = Vec::new();
    for i in 0..160u32 {
        let key = BlobCacheKey {
            kind: lru_kind,
            table_id: 1,
            seg_id: 1,
            offset: 1_000_000 + i * 4096,
            len: 2048,
            crc32c: 10_000 + i,
            key_hash: 100_000 + i as u64,
        };
        let payload = vec![(i & 0xFF) as u8; 2048];
        cache.insert(key, &payload);
        large_entries.push((key, payload));
    }

    let mut buf = Vec::new();
    for (key, expected) in &large_entries {
        if cache.contains(key) {
            buf.clear();
            assert!(cache.get_append_into(key, &mut buf));
            assert_eq!(&buf, expected);
        }
    }

    for shard_wrapper in &cache.shards {
        let shard = shard_wrapper.lock();
        assert_eq!(shard.entries.iter().flatten().count(), shard.index.len());
        assert!(
            shard.entries.len() <= 64.max(shard.index.len().saturating_mul(2)),
            "shard accumulated too many holes: entries={} live={}",
            shard.entries.len(),
            shard.index.len()
        );
    }

    cache.invalidate_blob_segment(lru_kind, 1, 1);
    for shard_wrapper in &cache.shards {
        let shard = shard_wrapper.lock();
        assert!(shard.entries.is_empty());
        assert!(shard.free_slots.is_empty());
        assert!(shard.index.is_empty());
        assert_eq!(shard.current_bytes, 0);
    }
}

#[test]
fn caches_track_hits_and_misses_accurately_under_concurrent_access() {
    let block_cache = Arc::new(VerifiedBlockCache::new(32 * IDX_BLOCK_SIZE));
    let blob_cache = Arc::new(VerifiedBlobCache::new(64 * 1024));
    let lru_kind = crate::wal::BlobKind::Lru as u8;

    std::thread::scope(|s| {
        for t in 0..4u32 {
            let bc = Arc::clone(&block_cache);
            let blc = Arc::clone(&blob_cache);
            s.spawn(move || {
                let mut out_blk = [0u8; IDX_BLOCK_SIZE];
                let mut out_blob = Vec::new();
                for i in 0..500u32 {
                    let blk_idx = (i + t * 7) % 48;
                    if !bc.get_into(1, blk_idx, &mut out_blk) {
                        let fill = [((blk_idx ^ t) & 0xFF) as u8; IDX_BLOCK_SIZE];
                        bc.insert(1, blk_idx, &fill);
                    }

                    let blob_id = (i + t * 5) % 64;
                    let key = BlobCacheKey {
                        kind: lru_kind,
                        table_id: 2,
                        seg_id: 3,
                        offset: blob_id * 512,
                        len: 128,
                        crc32c: blob_id,
                        key_hash: blob_id as u64 + 99,
                    };
                    out_blob.clear();
                    if !blc.get_append_into(&key, &mut out_blob) {
                        let payload = vec![(blob_id & 0xFF) as u8; 128];
                        blc.insert(key, &payload);
                    }
                }
            });
        }
    });

    assert_eq!(block_cache.hits() + block_cache.misses(), 2000);
    assert_eq!(blob_cache.hits() + blob_cache.misses(), 2000);
}

#[test]
fn tiny_non_zero_cache_budgets_never_exceed_configured_capacity() {
    let disabled_blk = VerifiedBlockCache::new(IDX_BLOCK_SIZE - 1);
    assert!(!disabled_blk.is_enabled());
    assert_eq!(
        disabled_blk
            .shards
            .iter()
            .map(|s| s.lock().slots.len())
            .sum::<usize>(),
        0
    );

    let one_blk = VerifiedBlockCache::new(IDX_BLOCK_SIZE);
    assert!(one_blk.is_enabled());
    assert_eq!(
        one_blk
            .shards
            .iter()
            .map(|s| s.lock().slots.len())
            .sum::<usize>(),
        1
    );
    one_blk.insert(1, 0, &[0xAB; IDX_BLOCK_SIZE]);

    let tiny_blob = VerifiedBlobCache::new(63);
    assert!(!tiny_blob.is_enabled());
    assert_eq!(
        tiny_blob
            .shards
            .iter()
            .map(|s| s.lock().capacity_bytes)
            .sum::<usize>(),
        0
    );

    let exact_blob = VerifiedBlobCache::new(100);
    assert!(exact_blob.is_enabled());
    assert_eq!(
        exact_blob
            .shards
            .iter()
            .map(|s| s.lock().capacity_bytes)
            .sum::<usize>(),
        100
    );
    let k = BlobCacheKey {
        kind: crate::wal::BlobKind::Standard as u8,
        table_id: 1,
        seg_id: 1,
        offset: 0,
        len: 50,
        crc32c: 1,
        key_hash: 1,
    };
    // Overhead (64) + payload (50) = 114 > 100 must be rejected.
    exact_blob.insert(k, &[0x11; 50]);
    assert!(!exact_blob.contains(&k));
    // Overhead (64) + payload (32) = 96 <= 100 may admit if hashed to active shard.
    let k2 = BlobCacheKey { len: 32, ..k };
    exact_blob.insert(k2, &[0x22; 32]);
    for shard in &exact_blob.shards {
        let g = shard.lock();
        assert!(g.current_bytes <= g.capacity_bytes);
    }
}

#[test]
fn small_cache_budgets_avoid_zero_slot_shards_and_fit_full_16kb_blobs() {
    for blocks in [1usize, 2, 4, 8] {
        let cache = VerifiedBlockCache::new(blocks * IDX_BLOCK_SIZE);
        assert_eq!(cache.shards.len(), blocks);
        for s in &cache.shards {
            assert!(!s.lock().slots.is_empty());
        }
        for blk_idx in 0..8u32 {
            let block = [(blk_idx as u8).wrapping_add(1); IDX_BLOCK_SIZE];
            cache.insert(9, blk_idx, &block);
            let got = cache
                .get_arc(9, blk_idx)
                .expect("no zero-slot shard may drop inserted block");
            assert_eq!(got[0], block[0]);
        }
    }

    // Small blob cache budgets >= 16 KB + overhead keep few enough shards that max-sized
    // 16 KB cached payloads still fit instead of being split into tiny unusable shards.
    for budget in [
        MAX_CACHED_BLOB_PAYLOAD_BYTES + BLOB_CACHE_ENTRY_OVERHEAD,
        32 * 1024,
        64 * 1024,
    ] {
        let blob_cache = VerifiedBlobCache::new(budget);
        let max_payload = vec![0x5Eu8; MAX_CACHED_BLOB_PAYLOAD_BYTES];
        let key = BlobCacheKey {
            kind: crate::wal::BlobKind::Standard as u8,
            table_id: 0,
            seg_id: 1,
            offset: 0,
            len: (MAX_CACHED_BLOB_PAYLOAD_BYTES + 20) as u32,
            crc32c: 0xDEAD_BEEF,
            key_hash: 0x1234_5678,
        };
        blob_cache.insert(key, &max_payload);
        let mut out = Vec::new();
        assert!(
            blob_cache.get_append_into(&key, &mut out),
            "16 KB blob must fit in {budget}-byte cache"
        );
        assert_eq!(out, max_payload);
    }
}

#[test]
fn blob_shard_compaction_shrinks_over_allocated_containers() {
    let cache = VerifiedBlobCache::new(16 * 1024 * 1024);
    for i in 0..4096u32 {
        let k = BlobCacheKey {
            kind: crate::wal::BlobKind::Standard as u8,
            table_id: 1,
            seg_id: (i % 4) as u64 + 1,
            offset: i * 16,
            len: 8,
            crc32c: i,
            key_hash: i as u64,
        };
        cache.insert(k, &[0x42u8; 8]);
    }
    let total_cap_before: usize = cache
        .shards
        .iter()
        .map(|s| {
            let g = s.lock();
            g.entries.capacity() + g.free_slots.capacity() + g.index.capacity()
        })
        .sum();
    assert!(total_cap_before >= 4096);

    // Remove 3/4 of segments to trigger partial shard compaction, then remaining segment to clear.
    for seg in 1..=3u64 {
        cache.invalidate_blob_segment(crate::wal::BlobKind::Standard as u8, 1, seg);
    }
    let total_cap_mid: usize = cache
        .shards
        .iter()
        .map(|s| {
            let g = s.lock();
            g.entries.capacity() + g.free_slots.capacity() + g.index.capacity()
        })
        .sum();
    assert!(
        total_cap_mid < total_cap_before,
        "mid capacity {total_cap_mid} should shrink below {total_cap_before}"
    );

    cache.invalidate_blob_segment(crate::wal::BlobKind::Standard as u8, 1, 4);
    for s in &cache.shards {
        let g = s.lock();
        assert!(g.index.is_empty());
        assert!(g.entries.capacity() <= 64);
        assert!(g.free_slots.capacity() <= 64);
        assert!(g.index.capacity() <= 64);
    }
}
