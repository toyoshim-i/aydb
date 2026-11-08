//! Explicit read views and the shared-access [`EngineReader`].
//!
//! A [`ReadView`] replaces the former "committed read" engine flag: it is resolved per
//! connection ([`StorageEngine::read_view_for_conn`]) and threaded through the read paths, so
//! reads that need no exclusive state run through `&StorageEngine` and can proceed in parallel
//! under a shared lock.

use std::cell::{Cell, RefCell};
use std::io;
use std::ops::Deref;

use crate::pal::PlatformFs;

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
}

pub(super) const NEED_FLUSH_MSG: &str = "unflushed blob requires exclusive access";

/// Per-read state threaded through the `*_cx` read paths.
pub(crate) struct ReadCtx {
    pub(super) view: ReadView,
    /// True when the caller holds exclusive access (so eager LRU promotions can be queued and
    /// buffered blobs flushed on request).
    pub(super) eager_ok: bool,
    /// Set when the read hit a blob still in an active writer's buffer (owner view only).
    pub(super) need_flush: Cell<bool>,
    pub(super) eager_hits: RefCell<Vec<(u16, Vec<u8>)>>,
}

impl ReadCtx {
    pub(super) fn new(view: ReadView, eager_ok: bool) -> Self {
        Self {
            view,
            eager_ok,
            need_flush: Cell::new(false),
            eager_hits: RefCell::new(Vec::new()),
        }
    }
}

pub(crate) enum CxRef<'a> {
    Owned(ReadCtx),
    Borrowed(&'a ReadCtx),
}

impl Deref for CxRef<'_> {
    type Target = ReadCtx;
    fn deref(&self) -> &ReadCtx {
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
    pub(super) cx: CxRef<'a>,
}

impl<FS: PlatformFs> EngineReader<'_, FS> {
    /// The view this reader observes.
    pub fn view(&self) -> ReadView {
        self.cx.view
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
        self.eng.get_row_into_cx(&self.cx, table_id, pk, slab, spans)
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

    /// Whether a live row with primary key `pk` exists in this view.
    pub fn row_exists(&self, table_id: u16, pk: &[u8]) -> io::Result<bool> {
        let row_key = crate::lru::encode_primary_row_key(table_id, pk);
        Ok(self.eng.check_existing_row_view(self.cx.view, &row_key)?
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
        self.eng.scan_range_into_slab_cx(
            &self.cx,
            table_id,
            lower,
            upper,
            reverse,
            limit,
            keys_only,
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
        self.eng.scan_secondary_index_into_slab_cx(
            &self.cx,
            table_id,
            index_id,
            prefix,
            lower_sec_key,
            upper_sec_key,
            limit,
            keys_only,
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
