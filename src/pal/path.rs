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
    } else if matches!(path.components().next(), Some(Component::Prefix(_))) {
        std::path::absolute(path)?
    } else {
        std::env::current_dir()?.join(path)
    };
    #[cfg(windows)]
    let prefixed = ensure_extended_length_prefix(clean_lexical_path(&abs_path)?);
    // On Unix a symlink followed by ".." names the parent of the link target.
    #[cfg(not(windows))]
    let prefixed = abs_path;
    if create {
        std::fs::create_dir_all(&prefixed)?;
    }
    let canonical = std::fs::canonicalize(&prefixed)?;
    Ok(ensure_extended_length_prefix(canonical))
}

#[cfg(windows)]
fn clean_lexical_path(path: &Path) -> io::Result<PathBuf> {
    use std::ffi::OsString;
    use std::os::windows::ffi::{OsStrExt, OsStringExt};
    use std::path::Prefix;

    // Preserve verbatim `\\?\` / device `\\.\` trailing dots/spaces untouched; however, if a
    // verbatim path contains `/` (never valid inside a Windows filename), convert `/` -> `\` and
    // lexically normalize `.` and `..` on post-prefix segments so `..` never escapes into the prefix.
    let mut wide_raw: Vec<u16> = path.as_os_str().encode_wide().collect();
    let has_slash = wide_raw.contains(&(b'/' as u16));
    let is_verbatim_or_device = matches!(
        path.components().next(),
        Some(Component::Prefix(p))
            if p.kind().is_verbatim() || matches!(p.kind(), Prefix::DeviceNS(_))
    ) || (wide_raw.len() >= 4
        && (wide_raw[0] == b'\\' as u16 || wide_raw[0] == b'/' as u16)
        && (wide_raw[1] == b'\\' as u16 || wide_raw[1] == b'/' as u16)
        && (wide_raw[2] == b'?' as u16 || wide_raw[2] == b'.' as u16)
        && (wide_raw[3] == b'\\' as u16 || wide_raw[3] == b'/' as u16));

    let converted_backing: Option<PathBuf> = if is_verbatim_or_device && has_slash {
        for u in &mut wide_raw {
            if *u == b'/' as u16 {
                *u = b'\\' as u16;
            }
        }
        Some(PathBuf::from(OsString::from_wide(&wide_raw)))
    } else {
        None
    };
    let effective_path = converted_backing.as_deref().unwrap_or(path);
    let is_verbatim = matches!(
        effective_path.components().next(),
        Some(Component::Prefix(p))
            if p.kind().is_verbatim() || matches!(p.kind(), Prefix::DeviceNS(_))
    );
    let collapse_verbatim_dots = is_verbatim && has_slash;

    let mut out = PathBuf::new();
    let mut normal_depth = 0usize;
    for comp in effective_path.components() {
        match comp {
            Component::Prefix(_) | Component::RootDir => out.push(comp.as_os_str()),
            Component::CurDir => {}
            Component::Normal(s) if collapse_verbatim_dots && s == "." => {}
            Component::ParentDir => {
                if normal_depth > 0 {
                    out.pop();
                    normal_depth -= 1;
                }
            }
            Component::Normal(s) if collapse_verbatim_dots && s == ".." => {
                if normal_depth > 0 {
                    out.pop();
                    normal_depth -= 1;
                }
            }
            Component::Normal(s) if !is_verbatim => {
                let mut wide: Vec<u16> = s.encode_wide().collect();
                while wide
                    .last()
                    .copied()
                    .is_some_and(|u| u == b' ' as u16 || u == b'.' as u16)
                {
                    wide.pop();
                }
                if wide.is_empty() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Windows path component consists only of dots or spaces",
                    ));
                }
                out.push(OsString::from_wide(&wide));
                normal_depth += 1;
            }
            other => {
                out.push(other.as_os_str());
                if matches!(other, Component::Normal(_)) {
                    normal_depth += 1;
                }
            }
        }
    }
    Ok(out)
}

#[cfg(windows)]
fn push_collapsed_backslashes(out: &mut Vec<u16>, rest: &[u16]) {
    let sep = b'\\' as u16;
    for &unit in rest {
        if unit == sep && out.last().copied() == Some(sep) {
            continue;
        }
        out.push(unit);
    }
}

/// Ensures that a Windows path uses the `\\?\` or `\\?\UNC\` extended-length prefix.
/// Normalizes any `/` separators to `\` before constructing or preserving the prefix because
/// Win32 extended-length paths disable automatic slash translation.
/// On non-Windows platforms, returns the path unchanged.
#[cfg(windows)]
pub fn ensure_extended_length_prefix(path: PathBuf) -> PathBuf {
    use std::ffi::OsString;
    use std::os::windows::ffi::{OsStrExt, OsStringExt};

    let mut wide: Vec<u16> = path.as_os_str().encode_wide().collect();
    for unit in &mut wide {
        if *unit == b'/' as u16 {
            *unit = b'\\' as u16;
        }
    }

    let sep = b'\\' as u16;
    // Already starts with `\\?\` or `\\.\`
    if wide.len() >= 4
        && wide[0] == sep
        && wide[1] == sep
        && (wide[2] == b'?' as u16 || wide[2] == b'.' as u16)
        && wide[3] == sep
    {
        let mut out = Vec::with_capacity(wide.len());
        out.extend_from_slice(&wide[..4]);
        push_collapsed_backslashes(&mut out, &wide[4..]);
        return PathBuf::from(OsString::from_wide(&out));
    }

    // UNC path `\\server\share\...` -> `\\?\UNC\server\share\...`
    if wide.len() >= 2 && wide[0] == sep && wide[1] == sep {
        let mut out: Vec<u16> = "\\\\?\\UNC\\".encode_utf16().collect();
        push_collapsed_backslashes(&mut out, &wide[2..]);
        return PathBuf::from(OsString::from_wide(&out));
    }

    // Standard drive-letter path `C:\...` -> `\\?\C:\...`
    let mut out: Vec<u16> = "\\\\?\\".encode_utf16().collect();
    push_collapsed_backslashes(&mut out, &wide);
    PathBuf::from(OsString::from_wide(&out))
}

#[cfg(not(windows))]
pub fn ensure_extended_length_prefix(path: PathBuf) -> PathBuf {
    path
}

/// Validates that `rel_name` is a single, flat file name inside the database directory
/// (rejecting empty names, interior NUL bytes, parent directory `..` traversal, or path separators).
pub fn validate_flat_file_name(rel_name: &str) -> io::Result<()> {
    if rel_name.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "file name must not be empty",
        ));
    }
    if rel_name.contains('\0') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "file name must not contain NUL bytes",
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
    fn validate_flat_file_name_rejects_interior_nul() {
        let err = validate_flat_file_name("a\0b.db").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    #[cfg(windows)]
    fn normalize_db_root_path_resolves_drive_relative_paths() {
        let cwd = std::env::current_dir().unwrap();
        let Some(Component::Prefix(pfx)) = cwd.components().next() else {
            return;
        };
        let prefix_str = pfx.as_os_str().to_string_lossy();
        if let Some(drive) = prefix_str.strip_suffix(':') {
            if drive.len() == 1 {
                let drive_rel = PathBuf::from(format!("{drive}:."));
                let normalized = normalize_db_root_path(&drive_rel, false).unwrap();
                assert!(normalized.exists());
                let norm_str = normalized.to_string_lossy();
                assert!(norm_str.starts_with(r"\\?\"));
                assert_ne!(norm_str, format!(r"\\?\{drive}:."));
            }
        }
    }

    #[test]
    #[cfg(unix)]
    fn database_root_resolves_parent_components_after_symlinks() {
        let tmp = crate::util::test_util::TempTestDir::new("root_symlink_parent");
        let target = tmp.path.join("target");
        std::fs::create_dir_all(target.join("sub")).unwrap();
        std::os::unix::fs::symlink(target.join("sub"), tmp.path.join("link")).unwrap();
        let path = tmp.path.join("link/../database");
        let resolved = normalize_db_root_path(&path, true).unwrap();
        assert_eq!(
            resolved,
            std::fs::canonicalize(target.join("database")).unwrap()
        );
        assert_eq!(normalize_db_root_path(&path, false).unwrap(), resolved);
        assert!(!tmp.path.join("database").exists());
    }

    #[test]
    fn normalize_db_root_path_creates_deep_long_paths_exceeding_max_path() {
        let pid = std::process::id();
        let base = std::env::temp_dir().join(format!("aydb_long_path_{pid}"));
        let base_prefixed = ensure_extended_length_prefix(base.clone());
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

    #[test]
    #[cfg(windows)]
    fn ensure_extended_length_prefix_normalizes_mixed_slash_unc_and_drive_paths() {
        assert_eq!(
            ensure_extended_length_prefix(PathBuf::from("//server/share/aydb")),
            PathBuf::from(r"\\?\UNC\server\share\aydb")
        );
        assert_eq!(
            ensure_extended_length_prefix(PathBuf::from(r"\\server/share/dir//sub")),
            PathBuf::from(r"\\?\UNC\server\share\dir\sub")
        );
        let cleaned = clean_lexical_path(Path::new("//server/share/a/../b")).unwrap();
        assert_eq!(
            ensure_extended_length_prefix(cleaned),
            PathBuf::from(r"\\?\UNC\server\share\b")
        );
        assert_eq!(
            ensure_extended_length_prefix(PathBuf::from("//?/UNC/server/share/aydb")),
            PathBuf::from(r"\\?\UNC\server\share\aydb")
        );
        assert_eq!(
            ensure_extended_length_prefix(PathBuf::from(r"\\?\C:/foo//bar")),
            PathBuf::from(r"\\?\C:\foo\bar")
        );
        assert_eq!(
            ensure_extended_length_prefix(PathBuf::from("C:/foo/bar")),
            PathBuf::from(r"\\?\C:\foo\bar")
        );
        assert_eq!(
            ensure_extended_length_prefix(PathBuf::from(r"\\.\C:/foo")),
            PathBuf::from(r"\\.\C:\foo")
        );
    }

    #[test]
    #[cfg(windows)]
    fn clean_lexical_path_strips_trailing_dots_and_spaces_before_verbatim_prefix() {
        let tmp = crate::util::test_util::TempTestDir::new("win_trailing_dots");
        let dirty = tmp.path.join("mydb. . ");
        let resolved = normalize_db_root_path(&dirty, true).unwrap();
        assert!(resolved.ends_with("mydb"));
        assert!(tmp.path.join("mydb").exists());
        assert_eq!(
            normalize_db_root_path(&tmp.path.join("mydb."), false).unwrap(),
            resolved
        );

        let err = clean_lexical_path(Path::new(r"C:\data\...")).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    #[cfg(windows)]
    fn clean_lexical_path_normalizes_verbatim_mixed_slashes_without_stripping_trailing_spaces() {
        assert_eq!(
            clean_lexical_path(Path::new(r"\\?\C:\foo/bar/../baz")).unwrap(),
            PathBuf::from(r"\\?\C:\foo\baz")
        );
        assert_eq!(
            clean_lexical_path(Path::new("//?/C:/foo/./bar/../../qux")).unwrap(),
            PathBuf::from(r"\\?\C:\qux")
        );
        assert_eq!(
            clean_lexical_path(Path::new(r"\\?\C:\../..")).unwrap(),
            PathBuf::from(r"\\?\C:\")
        );
        // Trailing space on verbatim component must remain intact while `/` + `..` collapse.
        assert_eq!(
            clean_lexical_path(Path::new(r"\\?\C:\dir /sub/../end ")).unwrap(),
            PathBuf::from(r"\\?\C:\dir \end ")
        );
        assert_eq!(
            clean_lexical_path(Path::new(r"\\?\UNC\server\share/a/./b/../c")).unwrap(),
            PathBuf::from(r"\\?\UNC\server\share\a\c")
        );
    }
}
