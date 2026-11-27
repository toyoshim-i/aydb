//! Streaming forward and reverse cursors (with optional sequential read-ahead) for index segments.

use std::io;
use std::sync::Arc;

use crate::pal::PlatformFile;

use super::{
    CursorEntrySpan, IDX_BLOCK_SIZE, IdxBlockReader, IdxSegmentReader, IndexEntry, RawIdxEntry,
};

impl<F: PlatformFile> IdxSegmentReader<F> {
    /// Creates a bounded-memory streaming cursor over all entries in this segment, reading
    /// one 4 KB data block at a time on demand (bypasses cache insertion).
    pub fn cursor_all(&self) -> io::Result<IdxSegmentCursor<'_, F>> {
        IdxSegmentCursor::new_all(self, false, CursorBlockStorage::new())
    }

    /// Creates a streaming cursor over all entries in this segment using batched sequential
    /// read-ahead chunks of up to `max_blocks` 4 KB blocks per `read_exact_at` syscall.
    pub(crate) fn cursor_all_batched(
        &self,
        max_blocks: u32,
    ) -> io::Result<IdxSegmentCursor<'_, F>> {
        IdxSegmentCursor::new_all(self, false, CursorBlockStorage::new_batched(max_blocks))
    }

    /// Creates a bounded-memory streaming cursor over all entries in this segment with explicit cache policy.
    pub fn cursor_all_with_cache(
        &self,
        populate_cache: bool,
    ) -> io::Result<IdxSegmentCursor<'_, F>> {
        IdxSegmentCursor::new_all(self, populate_cache, CursorBlockStorage::new())
    }

    /// Creates a bounded-memory streaming cursor starting at `(lower_key, lower_inclusive)`,
    /// seeking via sparse fence pointers and reading one 4 KB data block at a time on demand.
    pub fn cursor_from(
        &self,
        lower_key: &[u8],
        lower_inclusive: bool,
    ) -> io::Result<IdxSegmentCursor<'_, F>> {
        IdxSegmentCursor::new_from(self, lower_key, lower_inclusive, true)
    }

    /// Creates a bounded-memory streaming cursor starting at `(lower_key, lower_inclusive)` with explicit cache policy.
    pub fn cursor_from_with_cache(
        &self,
        lower_key: &[u8],
        lower_inclusive: bool,
        populate_cache: bool,
    ) -> io::Result<IdxSegmentCursor<'_, F>> {
        IdxSegmentCursor::new_from(self, lower_key, lower_inclusive, populate_cache)
    }

    /// Creates a bounded-memory reverse streaming cursor starting at `(upper_key, upper_inclusive)`,
    /// seeking via sparse fence pointers and walking 4 KB data blocks backward on demand.
    pub fn cursor_reverse_from(
        &self,
        upper_key: Option<(&[u8], bool)>,
    ) -> io::Result<IdxSegmentRevCursor<'_, F>> {
        IdxSegmentRevCursor::new_from(self, upper_key, true)
    }

    /// Creates a bounded-memory reverse streaming cursor starting at `(upper_key, upper_inclusive)` with explicit cache policy.
    pub fn cursor_reverse_from_with_cache(
        &self,
        upper_key: Option<(&[u8], bool)>,
        populate_cache: bool,
    ) -> io::Result<IdxSegmentRevCursor<'_, F>> {
        IdxSegmentRevCursor::new_from(self, upper_key, populate_cache)
    }
}

struct ReadAheadBuffer {
    buf: Vec<u8>,
    start_block: u32,
    block_count: u32,
    max_blocks: u32,
}

pub(super) enum ActiveBlock {
    None,
    Cached(Arc<[u8; IDX_BLOCK_SIZE]>),
    Scratch,
    ReadAhead(usize),
}

pub(super) struct CursorBlockStorage {
    pub(super) active: ActiveBlock,
    pub(super) scratch: Option<Box<[u8; IDX_BLOCK_SIZE]>>,
    read_ahead: Option<ReadAheadBuffer>,
}

impl CursorBlockStorage {
    fn new() -> Self {
        Self {
            active: ActiveBlock::None,
            scratch: None,
            read_ahead: None,
        }
    }

    fn new_batched(max_blocks: u32) -> Self {
        let max_blocks = max_blocks.max(1);
        Self {
            active: ActiveBlock::None,
            scratch: None,
            read_ahead: Some(ReadAheadBuffer {
                buf: Vec::with_capacity((max_blocks as usize) * IDX_BLOCK_SIZE),
                start_block: 0,
                block_count: 0,
                max_blocks,
            }),
        }
    }

    #[inline]
    fn block_bytes(&self) -> &[u8; IDX_BLOCK_SIZE] {
        match &self.active {
            ActiveBlock::Cached(arc) => arc.as_ref(),
            ActiveBlock::Scratch => self
                .scratch
                .as_deref()
                .expect("Scratch active without allocated buffer"),
            ActiveBlock::ReadAhead(off) => {
                let ra = self
                    .read_ahead
                    .as_ref()
                    .expect("ReadAhead active without buffer");
                ra.buf[*off..*off + IDX_BLOCK_SIZE]
                    .try_into()
                    .expect("ReadAhead slice matches IDX_BLOCK_SIZE")
            }
            ActiveBlock::None => panic!("CursorBlockStorage::block_bytes called when empty"),
        }
    }

    fn load<F: PlatformFile>(
        &mut self,
        seg: &IdxSegmentReader<F>,
        block_idx: u32,
        populate_cache: bool,
    ) -> io::Result<IdxBlockReader<'_>> {
        let preverified = if let Some(ra) = &mut self.read_ahead {
            if block_idx < ra.start_block || block_idx >= ra.start_block + ra.block_count {
                // Invalidate window metadata before resizing so a failed read leaves no stale span.
                ra.block_count = 0;
                self.active = ActiveBlock::None;
                let count = seg
                    .num_blocks
                    .saturating_sub(block_idx)
                    .min(ra.max_blocks)
                    .max(1);
                let bytes = (count as usize) * IDX_BLOCK_SIZE;
                ra.buf.resize(bytes, 0);
                seg.file
                    .read_exact_at(&mut ra.buf, (block_idx as u64) * (IDX_BLOCK_SIZE as u64))?;
                ra.start_block = block_idx;
                ra.block_count = count;
            }
            let off = ((block_idx - ra.start_block) as usize) * IDX_BLOCK_SIZE;
            self.active = ActiveBlock::ReadAhead(off);
            false
        } else if let Some(arc) = seg
            .block_cache
            .as_ref()
            .and_then(|c| c.get_arc(seg.seq_id, block_idx))
        {
            self.active = ActiveBlock::Cached(arc);
            true
        } else {
            self.active = ActiveBlock::None;
            let scratch = self
                .scratch
                .get_or_insert_with(|| Box::new([0u8; IDX_BLOCK_SIZE]));
            seg.file
                .read_exact_at(&mut **scratch, (block_idx as u64) * (IDX_BLOCK_SIZE as u64))?;
            self.active = ActiveBlock::Scratch;
            false
        };

        let num_entries = if preverified {
            match IdxBlockReader::open_preverified(self.block_bytes()) {
                Ok(r) => r.num_entries,
                Err(e) => {
                    self.active = ActiveBlock::None;
                    return Err(e);
                }
            }
        } else {
            let num_entries = match IdxBlockReader::open_verified(self.block_bytes()) {
                Ok(r) => r.num_entries,
                Err(e) => {
                    self.active = ActiveBlock::None;
                    return Err(e);
                }
            };
            if populate_cache {
                if let Some(cache) = &seg.block_cache {
                    cache.insert(seg.seq_id, block_idx, self.block_bytes());
                }
            }
            num_entries
        };
        Ok(IdxBlockReader::from_verified_buf(
            self.block_bytes(),
            num_entries,
        ))
    }
}

/// Bounded-memory reverse block-by-block cursor over an [`IdxSegmentReader`].
pub struct IdxSegmentRevCursor<'a, F: PlatformFile> {
    seg: &'a IdxSegmentReader<F>,
    next_block_idx: Option<u32>,
    pub(super) block: CursorBlockStorage,
    entry_idx: usize,
    num_entries_in_block: usize,
    current_span: Option<CursorEntrySpan>,
    populate_cache: bool,
}

impl<'a, F: PlatformFile> IdxSegmentRevCursor<'a, F> {
    fn new_from(
        seg: &'a IdxSegmentReader<F>,
        upper_key: Option<(&[u8], bool)>,
        populate_cache: bool,
    ) -> io::Result<Self> {
        let mut cursor = Self {
            seg,
            next_block_idx: None,
            block: CursorBlockStorage::new(),
            entry_idx: 0,
            num_entries_in_block: 0,
            current_span: None,
            populate_cache,
        };
        if seg.fence_pointers.is_empty() || seg.num_blocks == 0 {
            return Ok(cursor);
        }

        let Some((ub_key, ub_inclusive)) = upper_key else {
            cursor.next_block_idx = Some(seg.num_blocks - 1);
            cursor.advance_to_prev()?;
            return Ok(cursor);
        };

        let pos = seg.fence_pointers.partition_point(|fp| {
            if ub_inclusive {
                fp.first_key.as_ref() <= ub_key
            } else {
                fp.first_key.as_ref() < ub_key
            }
        });
        if pos == 0 {
            return Ok(cursor);
        }
        let mut b = seg.fence_pointers[pos - 1].block_idx;

        loop {
            let reader = cursor.block.load(seg, b, cursor.populate_cache)?;
            cursor.num_entries_in_block = reader.num_entries();

            // Binary-search for the first entry strictly after `(ub_key, ub_inclusive)`
            let mut low = 0usize;
            let mut high = cursor.num_entries_in_block;
            while low < high {
                let mid = low + (high - low) / 2;
                let k = reader.key_at(mid)?;
                let within = if ub_inclusive {
                    k <= ub_key
                } else {
                    k < ub_key
                };
                if within {
                    low = mid + 1;
                } else {
                    high = mid;
                }
            }

            if low > 0 {
                let target_idx = low - 1;
                let span = reader.parse_entry_span_at(target_idx)?;
                cursor.entry_idx = target_idx;
                cursor.next_block_idx = b.checked_sub(1);
                cursor.current_span = Some(span);
                break;
            }

            if b == 0 {
                break;
            }
            b -= 1;
        }
        Ok(cursor)
    }

    /// Returns the raw key slice of the current entry without heap allocation.
    #[inline]
    pub fn current_key(&self) -> Option<&[u8]> {
        let s = self.current_span.as_ref()?;
        let block = self.block.block_bytes();
        Some(&block[s.key_range.clone()])
    }

    /// Returns a zero-copy [`RawIdxEntry`] view of the current entry borrowed from `self.block`.
    #[inline]
    pub fn peek_raw(&self) -> Option<RawIdxEntry<'_>> {
        let s = self.current_span.as_ref()?;
        let block = self.block.block_bytes();
        Some(RawIdxEntry {
            key: &block[s.key_range.clone()],
            lsn: s.lsn,
            val_type: s.val_type,
            val_bytes: &block[s.val_range.clone()],
        })
    }

    /// Advances the cursor to the previous entry in the segment without allocating.
    #[inline]
    pub fn advance(&mut self) -> io::Result<()> {
        if self.current_span.is_some() {
            self.advance_to_prev()?;
        }
        Ok(())
    }

    /// Consumes and returns the current [`IndexEntry`], advancing the cursor to the previous entry.
    pub fn next_entry(&mut self) -> io::Result<Option<IndexEntry>> {
        let Some(raw) = self.peek_raw() else {
            return Ok(None);
        };
        let owned = raw.to_owned_entry()?;
        self.advance_to_prev()?;
        Ok(Some(owned))
    }

    fn advance_to_prev(&mut self) -> io::Result<()> {
        self.current_span = None;
        while self.entry_idx == 0 {
            let Some(b) = self.next_block_idx else {
                return Ok(());
            };
            self.next_block_idx = b.checked_sub(1);
            let reader = self.block.load(self.seg, b, self.populate_cache)?;
            self.num_entries_in_block = reader.num_entries();
            self.entry_idx = self.num_entries_in_block;
        }

        self.entry_idx -= 1;
        let reader =
            IdxBlockReader::from_verified_buf(self.block.block_bytes(), self.num_entries_in_block);
        let span = reader.parse_entry_span_at(self.entry_idx)?;
        self.current_span = Some(span);
        Ok(())
    }
}

/// Bounded-memory block-by-block cursor over an [`IdxSegmentReader`].
///
/// Borrows cached `Arc<[u8; IDX_BLOCK_SIZE]>` blocks zero-copy on cache hit and falls back to an
/// on-demand 4 KB scratch buffer (or a sequential read-ahead buffer during compaction) only on miss.
pub struct IdxSegmentCursor<'a, F: PlatformFile> {
    seg: &'a IdxSegmentReader<F>,
    next_block_idx: u32,
    pub(super) block: CursorBlockStorage,
    entry_idx: usize,
    num_entries_in_block: usize,
    current_span: Option<CursorEntrySpan>,
    populate_cache: bool,
}

impl<'a, F: PlatformFile> IdxSegmentCursor<'a, F> {
    fn new_all(
        seg: &'a IdxSegmentReader<F>,
        populate_cache: bool,
        block: CursorBlockStorage,
    ) -> io::Result<Self> {
        let mut cursor = Self {
            seg,
            next_block_idx: 0,
            block,
            entry_idx: 0,
            num_entries_in_block: 0,
            current_span: None,
            populate_cache,
        };
        cursor.advance_to_next()?;
        Ok(cursor)
    }

    fn new_from(
        seg: &'a IdxSegmentReader<F>,
        lower_key: &[u8],
        lower_inclusive: bool,
        populate_cache: bool,
    ) -> io::Result<Self> {
        let mut cursor = Self {
            seg,
            next_block_idx: 0,
            block: CursorBlockStorage::new(),
            entry_idx: 0,
            num_entries_in_block: 0,
            current_span: None,
            populate_cache,
        };
        if seg.fence_pointers.is_empty() || seg.num_blocks == 0 {
            return Ok(cursor);
        }

        let start_pos = seg
            .fence_pointers
            .partition_point(|fp| fp.first_key.as_ref() <= lower_key);
        cursor.next_block_idx = start_pos.saturating_sub(1) as u32;

        while cursor.next_block_idx < seg.num_blocks {
            let b = cursor.next_block_idx;
            cursor.next_block_idx += 1;
            let reader = cursor.block.load(seg, b, cursor.populate_cache)?;
            cursor.num_entries_in_block = reader.num_entries();

            // Binary-search the u16 offset directory for the first key satisfying the lower bound
            let mut low = 0usize;
            let mut high = cursor.num_entries_in_block;
            while low < high {
                let mid = low + (high - low) / 2;
                let k = reader.key_at(mid)?;
                let before = if lower_inclusive {
                    k < lower_key
                } else {
                    k <= lower_key
                };
                if before {
                    low = mid + 1;
                } else {
                    high = mid;
                }
            }

            if low < cursor.num_entries_in_block {
                let first_span = reader.parse_entry_span_at(low)?;
                cursor.entry_idx = low + 1;
                cursor.current_span = Some(first_span);
                break;
            } else {
                cursor.entry_idx = cursor.num_entries_in_block;
            }
        }
        Ok(cursor)
    }

    /// Returns the raw key slice of the current entry without heap allocation.
    #[inline]
    pub fn current_key(&self) -> Option<&[u8]> {
        let s = self.current_span.as_ref()?;
        let block = self.block.block_bytes();
        Some(&block[s.key_range.clone()])
    }

    /// Returns a zero-copy [`RawIdxEntry`] view of the current entry borrowed from `self.block`.
    #[inline]
    pub fn peek_raw(&self) -> Option<RawIdxEntry<'_>> {
        let s = self.current_span.as_ref()?;
        let block = self.block.block_bytes();
        Some(RawIdxEntry {
            key: &block[s.key_range.clone()],
            lsn: s.lsn,
            val_type: s.val_type,
            val_bytes: &block[s.val_range.clone()],
        })
    }

    /// Advances the cursor to the next entry in the segment without allocating.
    #[inline]
    pub fn advance(&mut self) -> io::Result<()> {
        if self.current_span.is_some() {
            self.advance_to_next()?;
        }
        Ok(())
    }

    /// Consumes and returns the current [`IndexEntry`], advancing the cursor to the next entry.
    pub fn next_entry(&mut self) -> io::Result<Option<IndexEntry>> {
        let Some(raw) = self.peek_raw() else {
            return Ok(None);
        };
        let owned = raw.to_owned_entry()?;
        self.advance_to_next()?;
        Ok(Some(owned))
    }

    fn advance_to_next(&mut self) -> io::Result<()> {
        self.current_span = None;
        while self.entry_idx >= self.num_entries_in_block {
            if self.next_block_idx >= self.seg.num_blocks {
                return Ok(());
            }
            let b = self.next_block_idx;
            self.next_block_idx += 1;
            let reader = self.block.load(self.seg, b, self.populate_cache)?;
            self.num_entries_in_block = reader.num_entries();
            self.entry_idx = 0;
        }

        let reader =
            IdxBlockReader::from_verified_buf(self.block.block_bytes(), self.num_entries_in_block);
        let span = reader.parse_entry_span_at(self.entry_idx)?;
        self.entry_idx += 1;
        self.current_span = Some(span);
        Ok(())
    }
}
