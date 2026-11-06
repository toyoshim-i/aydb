//! Path normalization and validation utilities for `aydb`.
//!
//! On Windows, standard Win32 paths are subject to the legacy `MAX_PATH` (260 character)
//! limit unless prefixed with `\\?\` (for local volumes) or `\\?\UNC\` (for SMB shares).

use std::io;
use std::path::{Component, Path, PathBuf};

/// Normalizes a database root directory path into an absolute, extended-length path
/// on Windows (or canonical path on Unix), creating the directory if `create` is true.
pub fn normalize_db_root_path(path: &Path, create: bool) -> io::Result<PathBuf> {
    if create {
        std::fs::create_dir_all(path)?;
    }
    let canonical = std::fs::canonicalize(path)?;
    Ok(ensure_extended_length_prefix(canonical))
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
