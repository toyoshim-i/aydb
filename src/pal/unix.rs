//! Unix (Linux / macOS) Platform Abstraction Layer (`UnixPlatformFile` / `UnixPlatformFs`).

use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::path::{normalize_db_root_path, validate_flat_file_name};
use super::{OpenHint, PlatformFile, PlatformFs, PreallocStrategy};

/// Unix implementation of [`PlatformFile`].
#[derive(Debug)]
pub struct UnixPlatformFile {
    file: File,
    #[allow(dead_code)]
    hint: OpenHint,
}

impl UnixPlatformFile {
    pub(crate) fn open_internal(
        full_path: &Path,
        create: bool,
        writable: bool,
        truncate: bool,
        hint: OpenHint,
    ) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(writable)
            .create(create)
            .truncate(truncate)
            .open(full_path)?;
        Ok(Self { file, hint })
    }
}

impl PlatformFile for UnixPlatformFile {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        FileExt::read_at(&self.file, buf, offset)
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        FileExt::read_exact_at(&self.file, buf, offset)
    }

    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        FileExt::write_all_at(&self.file, buf, offset)
    }

    fn file_len(&self) -> io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    fn preallocate(&self, target_len: u64, strategy: PreallocStrategy) -> io::Result<()> {
        let current_len = self.file_len()?;
        if current_len < target_len {
            self.file.set_len(target_len)?;
        }
        if strategy == PreallocStrategy::PhysicalZeroFill && current_len < target_len {
            const CHUNK_SIZE: usize = 256 * 1024;
            let zero_buf = vec![0u8; CHUNK_SIZE.min((target_len - current_len) as usize)];
            let mut pos = current_len;
            while pos < target_len {
                let to_write = ((target_len - pos) as usize).min(zero_buf.len());
                self.write_all_at(&zero_buf[..to_write], pos)?;
                pos += to_write as u64;
            }
            self.sync_durable()?;
        }
        Ok(())
    }

    fn sync_barrier(&self) -> io::Result<()> {
        self.file.sync_data()
    }

    fn sync_durable(&self) -> io::Result<()> {
        self.file.sync_all()
    }

    fn evict_os_cache(&self, _offset: u64, _len: u64) -> io::Result<()> {
        Ok(())
    }
}

/// Unix implementation of [`PlatformFs`].
#[derive(Debug, Clone)]
pub struct UnixPlatformFs {
    root: PathBuf,
}

impl UnixPlatformFs {
    pub fn open(root: impl AsRef<Path>, create: bool) -> io::Result<Self> {
        let normalized = normalize_db_root_path(root.as_ref(), create)?;
        Ok(Self { root: normalized })
    }
}

impl PlatformFs for UnixPlatformFs {
    type File = UnixPlatformFile;

    fn open_file(
        &self,
        rel_name: &str,
        create: bool,
        writable: bool,
        hint: OpenHint,
    ) -> io::Result<Arc<Self::File>> {
        validate_flat_file_name(rel_name)?;
        let full_path = self.root.join(rel_name);
        let file = UnixPlatformFile::open_internal(&full_path, create, writable, false, hint)?;
        Ok(Arc::new(file))
    }

    fn remove_file(&self, rel_name: &str) -> io::Result<()> {
        validate_flat_file_name(rel_name)?;
        let full_path = self.root.join(rel_name);
        match std::fs::remove_file(full_path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }

    fn list_files(&self) -> io::Result<Vec<String>> {
        let mut names = Vec::new();
        for entry in std::fs::read_dir(&self.root)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                if let Some(s) = entry.file_name().to_str() {
                    names.push(s.to_owned());
                }
            }
        }
        names.sort();
        Ok(names)
    }

    fn root_path(&self) -> &Path {
        &self.root
    }
}
