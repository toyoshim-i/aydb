//! Explicit read views and the shared-access [`EngineReader`].
//!
//! A [`ReadView`] replaces the former "committed read" engine flag: it is resolved per
//! connection ([`StorageEngine::read_view_for_conn`]) and threaded through the read paths, so
//! reads that need no exclusive state run through `&StorageEngine` and can proceed in parallel
//! under a shared lock.

use std::cell::{Cell, RefCell};
use std::io;
use std::ops::Deref;

use crate::pal::{PlatformFile, PlatformFs};

use super::snapshot::ReadSnapshot;
use super::{LazyBlob, StorageEngine};

/// Which version of the data a read observes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadView {
    /// Committed data plus the active transaction's private overlay (read-your-own-writes).
    /// Used by the transaction owner, and by every connection while no transaction is active
    /// (the overlay is then empty).
    Owner,
    /// Committed data only: another connection's uncommitted writes are invisible.
    Committed,
    /// Committed sources captured before subsequent transactions published their changes.
    Snapshot,
}

/// Internal zero-cost read source threaded through `&self` engine paths.
pub(super) enum ReadSource<'a, F: PlatformFile> {
    Owner,
    Committed,
    Snapshot(&'a ReadSnapshot<F>),
}

impl<F: PlatformFile> Clone for ReadSource<'_, F> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<F: PlatformFile> Copy for ReadSource<'_, F> {}

impl<F: PlatformFile> ReadSource<'_, F> {
    #[inline]
    pub(super) fn from_view(view: ReadView) -> Self {
        match view {
            ReadView::Owner => Self::Owner,
            ReadView::Committed => Self::Committed,
            ReadView::Snapshot => {
                unreachable!(
                    "ReadView::Snapshot cannot resolve sources without a ReadSnapshotToken"
                )
            }
        }
    }

    #[inline]
    pub(super) fn is_owner(self) -> bool {
        matches!(self, Self::Owner)
    }

    #[inline]
    pub(super) fn read_view(self) -> ReadView {
        match self {
            Self::Owner => ReadView::Owner,
            Self::Committed => ReadView::Committed,
            Self::Snapshot(_) => ReadView::Snapshot,
        }
    }
}

pub(super) const NEED_FLUSH_MSG: &str = "unflushed blob requires exclusive access";

/// Per-read state threaded through the `*_cx` read paths.
pub(crate) struct ReadCtx<'a, F: PlatformFile> {
    pub(super) src: ReadSource<'a, F>,
    /// True when the caller holds exclusive access (so eager LRU promotions can be queued and
    /// buffered blobs flushed on request).
    pub(super) eager_ok: bool,
    /// False for internal constraint checks that must not mutate LRU tracking state.
    pub(super) track_hits: bool,
    /// Set when the read hit a blob still in an active writer's buffer (owner view only).
    pub(super) need_flush: Cell<bool>,
    pub(super) eager_hits: RefCell<Vec<(u16, Vec<u8>)>>,
}

impl<'a, F: PlatformFile> ReadCtx<'a, F> {
    pub(super) fn new(src: ReadSource<'a, F>, eager_ok: bool) -> Self {
        Self::new_with_tracking(src, eager_ok, true)
    }

    pub(super) fn new_with_tracking(
        src: ReadSource<'a, F>,
        eager_ok: bool,
        track_hits: bool,
    ) -> Self {
        Self {
            src,
            eager_ok,
            track_hits,
            need_flush: Cell::new(false),
            eager_hits: RefCell::new(Vec::new()),
        }
    }
}

pub(crate) enum CxRef<'a, F: PlatformFile> {
    Owned(ReadCtx<'a, F>),
    Borrowed(&'a ReadCtx<'a, F>),
}

impl<'a, F: PlatformFile> Deref for CxRef<'a, F> {
    type Target = ReadCtx<'a, F>;
    fn deref(&self) -> &ReadCtx<'a, F> {
        match self {
            CxRef::Owned(c) => c,
            CxRef::Borrowed(c) => c,
        }
    }
}

/// Read access to a [`StorageEngine`] through `&self` for one connection's [`ReadView`].
///
/// Obtained from [`StorageEngine::reader_for_conn`] (shared lock) or handed to the closure of
/// [`StorageEngine::with_committed_read_for_conn`] (exclusive access, where buffered blobs are
/// flushed and `EagerWriteOnRead` promotions applied on the caller's behalf).
pub struct EngineReader<'a, FS: PlatformFs> {
    pub(super) eng: &'a StorageEngine<FS>,
    pub(super) cx: CxRef<'a, FS::File>,
}

impl<FS: PlatformFs> EngineReader<'_, FS> {
    /// The view this reader observes.
    pub fn view(&self) -> ReadView {
        self.cx.src.read_view()
    }

    /// True if a read through this reader failed because it needed exclusive access (an
    /// unflushed blob of the owner view); the caller must retry under an exclusive lock.
    pub fn needs_exclusive_retry(&self) -> bool {
        self.cx.need_flush.get()
    }

    /// See [`StorageEngine::get_row`].
    pub fn get_row(&self, table_id: u16, pk: &[u8]) -> io::Result<Option<Vec<u8>>> {
        self.eng.get_row_cx(&self.cx, table_id, pk)
    }

    /// See [`StorageEngine::get_row_into`].
    pub fn get_row_into(
        &self,
        table_id: u16,
        pk: &[u8],
        slab: &mut Vec<u8>,
        spans: &mut Vec<(usize, usize)>,
    ) -> io::Result<bool> {
        self.eng
            .get_row_into_cx(&self.cx, table_id, pk, slab, spans)
    }

    /// See [`StorageEngine::get_row_into_lazy`].
    pub fn get_row_into_lazy(
        &self,
        table_id: u16,
        pk: &[u8],
        slab: &mut Vec<u8>,
        spans: &mut Vec<(usize, usize)>,
        lazy_blobs: &mut Vec<Option<LazyBlob<FS::File>>>,
    ) -> io::Result<bool> {
        self.eng
            .get_row_into_lazy_cx(&self.cx, table_id, pk, slab, spans, lazy_blobs)
    }

    /// See [`StorageEngine::dereference_lazy_blob_into`].
    pub fn dereference_lazy_blob_into(
        &self,
        table_id: u16,
        pk: &[u8],
        lazy: &LazyBlob<FS::File>,
        slab: &mut Vec<u8>,
    ) -> io::Result<Option<(usize, usize)>> {
        self.eng
            .dereference_lazy_blob_into_cx(&self.cx, table_id, pk, lazy, slab)
    }

    /// See [`StorageEngine::prefetch_lazy_blobs`].
    pub fn prefetch_lazy_blobs(
        &self,
        table_id: u16,
        candidates: &[(&[u8], &LazyBlob<FS::File>)],
    ) -> io::Result<()> {
        self.eng
            .prefetch_lazy_blobs_cx(&self.cx, table_id, candidates)
    }

    /// Whether a live row with primary key `pk` exists in this view.
    pub fn row_exists(&self, table_id: u16, pk: &[u8]) -> io::Result<bool> {
        let row_key = crate::lru::encode_primary_row_key(table_id, pk);
        Ok(self.eng.check_existing_row_src(self.cx.src, &row_key)?
            != super::ExistingRowStatus::NotFound)
    }

    /// See [`StorageEngine::scan_keys_range`].
    pub fn scan_keys_range(
        &self,
        table_id: u16,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        limit: usize,
    ) -> io::Result<Vec<Vec<u8>>> {
        self.eng
            .scan_keys_range_cx(&self.cx, table_id, lower, upper, limit)
    }

    /// See [`StorageEngine::scan_range_into_slab`].
    #[allow(clippy::too_many_arguments)]
    pub fn scan_range_into_slab(
        &self,
        table_id: u16,
        lower: Option<(&[u8], bool)>,
        upper: Option<(&[u8], bool)>,
        reverse: bool,
        limit: usize,
        keys_only: bool,
        pks_out: &mut Vec<Vec<u8>>,
        slab_out: &mut Vec<u8>,
        spans_out: &mut Vec<(usize, usize)>,
        lazy_blobs_out: Option<&mut Vec<Option<LazyBlob<FS::File>>>>,
    ) -> io::Result<()> {
        let populate_cache = super::scan::should_populate_range_cache(lower, upper, limit);
        self.scan_range_into_slab_with_cache(
            table_id,
            lower,
            upper,
            reverse,
            limit,
            keys_only,
            populate_cache,
            pks_out,
            slab_out,
            spans_out,
            lazy_blobs_out,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn scan_range_into_slab_with_cache(
        &self,
        table_id: u16,
        lower: Option<(&[u8], bool)>,
        upper: Option<(&[u8], bool)>,
        reverse: bool,
        limit: usize,
        keys_only: bool,
        populate_cache: bool,
        pks_out: &mut Vec<Vec<u8>>,
        slab_out: &mut Vec<u8>,
        spans_out: &mut Vec<(usize, usize)>,
        lazy_blobs_out: Option<&mut Vec<Option<LazyBlob<FS::File>>>>,
    ) -> io::Result<()> {
        self.eng.scan_range_into_slab_cx(
            &self.cx,
            table_id,
            lower,
            upper,
            reverse,
            limit,
            keys_only,
            populate_cache,
            pks_out,
            slab_out,
            spans_out,
            lazy_blobs_out,
        )
    }

    /// See [`StorageEngine::scan_secondary_index_into_slab`].
    #[allow(clippy::too_many_arguments)]
    pub fn scan_secondary_index_into_slab(
        &self,
        table_id: u16,
        index_id: u16,
        prefix: &[u8],
        lower_sec_key: (&[u8], bool),
        upper_sec_key: Option<(&[u8], bool)>,
        limit: usize,
        keys_only: bool,
        pks_out: &mut Vec<Vec<u8>>,
        slab_out: &mut Vec<u8>,
        spans_out: &mut Vec<(usize, usize)>,
        lazy_blobs_out: Option<&mut Vec<Option<LazyBlob<FS::File>>>>,
    ) -> io::Result<Option<Vec<u8>>> {
        let populate_cache =
            super::scan::should_populate_secondary_cache(prefix, upper_sec_key, limit);
        self.scan_secondary_index_into_slab_with_cache(
            table_id,
            index_id,
            prefix,
            lower_sec_key,
            upper_sec_key,
            limit,
            keys_only,
            populate_cache,
            pks_out,
            slab_out,
            spans_out,
            lazy_blobs_out,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn scan_secondary_index_into_slab_with_cache(
        &self,
        table_id: u16,
        index_id: u16,
        prefix: &[u8],
        lower_sec_key: (&[u8], bool),
        upper_sec_key: Option<(&[u8], bool)>,
        limit: usize,
        keys_only: bool,
        populate_cache: bool,
        pks_out: &mut Vec<Vec<u8>>,
        slab_out: &mut Vec<u8>,
        spans_out: &mut Vec<(usize, usize)>,
        lazy_blobs_out: Option<&mut Vec<Option<LazyBlob<FS::File>>>>,
    ) -> io::Result<Option<Vec<u8>>> {
        self.eng.scan_secondary_index_into_slab_cx(
            &self.cx,
            table_id,
            index_id,
            prefix,
            lower_sec_key,
            upper_sec_key,
            limit,
            keys_only,
            populate_cache,
            pks_out,
            slab_out,
            spans_out,
            lazy_blobs_out,
        )
    }
}

// The vtab shares one engine across threads behind an `RwLock`.
const _: () = {
    fn assert_send_sync<T: Send + Sync>() {}
    #[allow(dead_code)]
    fn engine_is_send_sync() {
        assert_send_sync::<StorageEngine<crate::pal::OsFs>>();
    }
};
