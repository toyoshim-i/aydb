//! Test-only hookable [`PlatformFs`] / [`PlatformFile`] wrapper shared across unit tests.

use std::io;
use std::path::Path;
use std::sync::Arc;

use crate::pal::{OpenHint, PlatformFile, PlatformFs, PreallocStrategy};

impl<F: PlatformFile> PlatformFile for Arc<F> {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        (**self).read_at(buf, offset)
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        (**self).read_exact_at(buf, offset)
    }

    fn read_many_at(&self, requests: &mut [(u64, &mut [u8])]) -> io::Result<()> {
        (**self).read_many_at(requests)
    }

    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        (**self).write_all_at(buf, offset)
    }

    fn file_len(&self) -> io::Result<u64> {
        (**self).file_len()
    }

    fn preallocate(&self, target_len: u64, strategy: PreallocStrategy) -> io::Result<()> {
        (**self).preallocate(target_len, strategy)
    }

    fn sync_barrier(&self) -> io::Result<()> {
        (**self).sync_barrier()
    }

    fn sync_durable(&self) -> io::Result<()> {
        (**self).sync_durable()
    }

    fn evict_os_cache(&self, offset: u64, len: u64) -> io::Result<()> {
        (**self).evict_os_cache(offset, len)
    }
}

impl<FS: PlatformFs> PlatformFs for Arc<FS> {
    type File = FS::File;

    fn open_file(
        &self,
        rel_name: &str,
        create: bool,
        writable: bool,
        hint: OpenHint,
    ) -> io::Result<Arc<Self::File>> {
        (**self).open_file(rel_name, create, writable, hint)
    }

    fn remove_file(&self, rel_name: &str) -> io::Result<()> {
        (**self).remove_file(rel_name)
    }

    fn remove_file_unsynced(&self, rel_name: &str) -> io::Result<()> {
        (**self).remove_file_unsynced(rel_name)
    }

    fn sync_dir(&self) -> io::Result<()> {
        (**self).sync_dir()
    }

    fn list_files(&self) -> io::Result<Vec<String>> {
        (**self).list_files()
    }

    fn root_path(&self) -> &Path {
        (**self).root_path()
    }
}

/// Event hooks for [`HookedFs`] and [`HookedFile`]. Every method delegates to `inner` or `default`
/// unless overridden.
pub(crate) trait FsHooks: Send + Sync + 'static {
    fn before_open_file(
        &self,
        _rel_name: &str,
        _create: bool,
        _writable: bool,
        _hint: OpenHint,
    ) -> io::Result<()> {
        Ok(())
    }

    fn after_open_file(
        &self,
        _rel_name: &str,
        _create: bool,
        _writable: bool,
        _hint: OpenHint,
        _ok: bool,
    ) {
    }

    fn remove_file(
        &self,
        _rel_name: &str,
        default: &mut dyn FnMut() -> io::Result<()>,
    ) -> io::Result<()> {
        default()
    }

    fn remove_file_unsynced(
        &self,
        _rel_name: &str,
        default: &mut dyn FnMut() -> io::Result<()>,
    ) -> io::Result<()> {
        default()
    }

    fn sync_dir(&self, default: &mut dyn FnMut() -> io::Result<()>) -> io::Result<()> {
        default()
    }

    fn list_files(
        &self,
        default: &mut dyn FnMut() -> io::Result<Vec<String>>,
    ) -> io::Result<Vec<String>> {
        default()
    }

    fn read_at(
        &self,
        _name: &str,
        inner: &dyn PlatformFile,
        buf: &mut [u8],
        offset: u64,
    ) -> io::Result<usize> {
        inner.read_at(buf, offset)
    }

    fn read_exact_at(
        &self,
        _name: &str,
        inner: &dyn PlatformFile,
        buf: &mut [u8],
        offset: u64,
    ) -> io::Result<()> {
        inner.read_exact_at(buf, offset)
    }

    fn read_many_at(
        &self,
        _name: &str,
        inner: &dyn PlatformFile,
        requests: &mut [(u64, &mut [u8])],
    ) -> io::Result<()> {
        inner.read_many_at(requests)
    }

    fn write_all_at(
        &self,
        _name: &str,
        inner: &dyn PlatformFile,
        buf: &[u8],
        offset: u64,
    ) -> io::Result<()> {
        inner.write_all_at(buf, offset)
    }

    fn file_len(&self, _name: &str, inner: &dyn PlatformFile) -> io::Result<u64> {
        inner.file_len()
    }

    fn preallocate(
        &self,
        _name: &str,
        inner: &dyn PlatformFile,
        target_len: u64,
        strategy: PreallocStrategy,
    ) -> io::Result<()> {
        inner.preallocate(target_len, strategy)
    }

    fn sync_barrier(&self, _name: &str, inner: &dyn PlatformFile) -> io::Result<()> {
        inner.sync_barrier()
    }

    fn sync_durable(&self, _name: &str, inner: &dyn PlatformFile) -> io::Result<()> {
        inner.sync_durable()
    }

    fn evict_os_cache(
        &self,
        _name: &str,
        inner: &dyn PlatformFile,
        offset: u64,
        len: u64,
    ) -> io::Result<()> {
        inner.evict_os_cache(offset, len)
    }
}

/// [`PlatformFile`] wrapper forwarding operations through an [`FsHooks`] implementation.
pub(crate) struct HookedFile<F: PlatformFile, H: FsHooks> {
    inner: Arc<F>,
    name: String,
    hooks: Arc<H>,
}

impl<F: PlatformFile, H: FsHooks> HookedFile<F, H> {
    pub(crate) fn new(inner: Arc<F>, name: impl Into<String>, hooks: H) -> Self {
        Self {
            inner,
            name: name.into(),
            hooks: Arc::new(hooks),
        }
    }

    pub(crate) fn hooks(&self) -> &H {
        &self.hooks
    }
}

impl<F: PlatformFile, H: FsHooks> PlatformFile for HookedFile<F, H> {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        self.hooks.read_at(&self.name, &*self.inner, buf, offset)
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        self.hooks
            .read_exact_at(&self.name, &*self.inner, buf, offset)
    }

    fn read_many_at(&self, requests: &mut [(u64, &mut [u8])]) -> io::Result<()> {
        self.hooks.read_many_at(&self.name, &*self.inner, requests)
    }

    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        self.hooks
            .write_all_at(&self.name, &*self.inner, buf, offset)
    }

    fn file_len(&self) -> io::Result<u64> {
        self.hooks.file_len(&self.name, &*self.inner)
    }

    fn preallocate(&self, target_len: u64, strategy: PreallocStrategy) -> io::Result<()> {
        self.hooks
            .preallocate(&self.name, &*self.inner, target_len, strategy)
    }

    fn sync_barrier(&self) -> io::Result<()> {
        self.hooks.sync_barrier(&self.name, &*self.inner)
    }

    fn sync_durable(&self) -> io::Result<()> {
        self.hooks.sync_durable(&self.name, &*self.inner)
    }

    fn evict_os_cache(&self, offset: u64, len: u64) -> io::Result<()> {
        self.hooks
            .evict_os_cache(&self.name, &*self.inner, offset, len)
    }
}

/// [`PlatformFs`] wrapper forwarding file and directory operations through an [`FsHooks`]
/// implementation.
pub(crate) struct HookedFs<FS: PlatformFs, H: FsHooks> {
    inner: FS,
    hooks: Arc<H>,
}

impl<FS: PlatformFs, H: FsHooks> HookedFs<FS, H> {
    pub(crate) fn new(inner: FS, hooks: H) -> Self {
        Self {
            inner,
            hooks: Arc::new(hooks),
        }
    }

    pub(crate) fn hooks(&self) -> &H {
        &self.hooks
    }
}

impl<FS: PlatformFs, H: FsHooks> PlatformFs for HookedFs<FS, H> {
    type File = HookedFile<FS::File, H>;

    fn open_file(
        &self,
        rel_name: &str,
        create: bool,
        writable: bool,
        hint: OpenHint,
    ) -> io::Result<Arc<Self::File>> {
        self.hooks
            .before_open_file(rel_name, create, writable, hint)?;
        let res = self.inner.open_file(rel_name, create, writable, hint);
        self.hooks
            .after_open_file(rel_name, create, writable, hint, res.is_ok());
        Ok(Arc::new(HookedFile {
            inner: res?,
            name: rel_name.to_owned(),
            hooks: Arc::clone(&self.hooks),
        }))
    }

    fn remove_file(&self, rel_name: &str) -> io::Result<()> {
        let mut f = || self.inner.remove_file(rel_name);
        self.hooks.remove_file(rel_name, &mut f)
    }

    fn remove_file_unsynced(&self, rel_name: &str) -> io::Result<()> {
        let mut f = || self.inner.remove_file_unsynced(rel_name);
        self.hooks.remove_file_unsynced(rel_name, &mut f)
    }

    fn sync_dir(&self) -> io::Result<()> {
        let mut f = || self.inner.sync_dir();
        self.hooks.sync_dir(&mut f)
    }

    fn list_files(&self) -> io::Result<Vec<String>> {
        let mut f = || self.inner.list_files();
        self.hooks.list_files(&mut f)
    }

    fn root_path(&self) -> &Path {
        self.inner.root_path()
    }
}
