//! Path normalization and validation utilities for `aydb`.
//!
//! On Windows, standard Win32 paths are subject to the legacy `MAX_PATH` (260 character)
//! limit unless prefixed with `\\?\` (for local volumes) or `\\?\UNC\` (for SMB shares).

use std::io;
use std::path::{Component, Path, PathBuf};

/// Normalizes a database root directory path into an absolute, extended-length path
/// on Windows (or canonical path on Unix), creating the directory if `create` is true.
///
/// Resolves relative paths and applies the `\\?\` extended-length prefix *before* calling
/// `std::fs::create_dir_all` so directories deeper than `MAX_PATH` (260 characters) can be
/// created on Windows even when `LongPathsEnabled` is not set in the registry.
pub fn normalize_db_root_path(path: &Path, create: bool) -> io::Result<PathBuf> {
    let abs_path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    let clean_abs = clean_lexical_path(&abs_path);
    let prefixed = ensure_extended_length_prefix(clean_abs);
    if create {
        std::fs::create_dir_all(&prefixed)?;
    }
    let canonical = std::fs::canonicalize(&prefixed)?;
    Ok(ensure_extended_length_prefix(canonical))
}

fn clean_lexical_path(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for comp in path.components() {
        match comp {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            other => out.push(other.as_os_str()),
        }
    }
    out
}

/// Ensures that a Windows path uses the `\\?\` or `\\?\UNC\` extended-length prefix.
/// On non-Windows platforms, returns the path unchanged.
#[cfg(windows)]
pub fn ensure_extended_length_prefix(path: PathBuf) -> PathBuf {
    use std::ffi::OsString;
    use std::os::windows::ffi::{OsStrExt, OsStringExt};

    let wide: Vec<u16> = path.as_os_str().encode_wide().collect();
    // Already starts with `\\?\` or `\\.\`
    if wide.len() >= 4
        && wide[0] == b'\\' as u16
        && wide[1] == b'\\' as u16
        && (wide[2] == b'?' as u16 || wide[2] == b'.' as u16)
        && wide[3] == b'\\' as u16
    {
        return path;
    }

    // UNC path `\\server\share\...` -> `\\?\UNC\server\share\...`
    if wide.len() >= 2 && wide[0] == b'\\' as u16 && wide[1] == b'\\' as u16 {
        let mut out: Vec<u16> = "\\\\?\\UNC\\".encode_utf16().collect();
        out.extend_from_slice(&wide[2..]);
        return PathBuf::from(OsString::from_wide(&out));
    }

    // Standard drive-letter path `C:\...` -> `\\?\C:\...`
    let mut out: Vec<u16> = "\\\\?\\".encode_utf16().collect();
    out.extend_from_slice(&wide);
    PathBuf::from(OsString::from_wide(&out))
}

#[cfg(not(windows))]
pub fn ensure_extended_length_prefix(path: PathBuf) -> PathBuf {
    path
}

/// Validates that `rel_name` is a single, flat file name inside the database directory
/// (rejecting empty names, parent directory `..` traversal, or path separators).
pub fn validate_flat_file_name(rel_name: &str) -> io::Result<()> {
    if rel_name.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "file name must not be empty",
        ));
    }
    if rel_name.contains('/') || rel_name.contains('\\') || rel_name.contains(':') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("file name must not contain path separators: {rel_name}"),
        ));
    }
    let path = Path::new(rel_name);
    let mut components = path.components();
    match (components.next(), components.next()) {
        (Some(Component::Normal(_)), None) => Ok(()),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid flat file name: {rel_name}"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_db_root_path_creates_deep_long_paths_exceeding_max_path() {
        let pid = std::process::id();
        let base = std::env::temp_dir().join(format!("aydb_long_path_{pid}"));
        let base_prefixed = ensure_extended_length_prefix(clean_lexical_path(&base));
        let _ = std::fs::remove_dir_all(&base_prefixed);

        let mut deep = base.clone();
        for i in 0..8 {
            deep.push(format!("segment_{i:02}_{}", "a".repeat(32)));
        }
        assert!(
            deep.as_os_str().len() > 280,
            "test path must exceed legacy MAX_PATH (260 chars)"
        );

        let normalized = normalize_db_root_path(&deep, true)
            .expect("normalize_db_root_path must create paths > 260 chars");
        assert!(normalized.exists());
        let _ = std::fs::remove_dir_all(&base_prefixed);
    }
}
