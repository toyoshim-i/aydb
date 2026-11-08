//! `CREATE VIRTUAL TABLE` argument and column/affinity/collation specification parser.

use core::ffi::{c_char, c_int};
use std::ffi::CStr;

use crate::engine::{CorruptPolicy, WriteDurability};
use crate::vtab::ffi::SQLITE_ERROR;
use crate::vtab::row_codec::{ColCollation, ColumnAffinity};

/// Parsed schema and configuration options from `CREATE VIRTUAL TABLE ... USING aydb(...)`.
pub(super) struct ParsedVtabDdl {
    pub(super) table_name: String,
    pub(super) explicit_table_id: Option<u16>,
    pub(super) max_bytes: u64,
    pub(super) durability: WriteDurability,
    /// Explicit `on_corrupt=error|evict`; `None` keeps the engine default (capped -> evict).
    pub(super) on_corrupt: Option<CorruptPolicy>,
    /// Explicit per-table inline threshold override in bytes; `None` keeps engine default.
    pub(super) inline_max: Option<usize>,
    pub(super) columns_spec: String,
    pub(super) col_names: Vec<String>,
    pub(super) col_affinities: Vec<ColumnAffinity>,
    pub(super) col_collations: Vec<ColCollation>,
    pub(super) pk_col_idx: usize,
    pub(super) pk_affinity: ColumnAffinity,
    pub(super) pk_collation: ColCollation,
    pub(super) secondary_indexes: Vec<(usize, u16)>,
}

/// Splits a comma-separated SQL column list at top-level commas only, ignoring commas inside
/// parenthesized type parameters such as `DECIMAL(10, 2)`.
pub(super) fn split_top_level_commas(input: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0usize;
    let mut start = 0usize;
    for (idx, ch) in input.char_indices() {
        match ch {
            '(' => depth = depth.saturating_add(1),
            ')' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                parts.push(&input[start..idx]);
                start = idx + 1;
            }
            _ => {}
        }
    }
    if start <= input.len() {
        let tail = &input[start..];
        if !tail.trim().is_empty() {
            parts.push(tail);
        }
    }
    parts
}

/// Parses `CREATE VIRTUAL TABLE` module arguments (`argc`, `argv`) into a [`ParsedVtabDdl`].
///
/// # Safety
/// `argv` must point to `argc` valid C string pointers as passed by SQLite to `xCreate`/`xConnect`.
pub(super) unsafe fn parse_vtab_ddl(
    argc: c_int,
    argv: *const *const c_char,
) -> Result<ParsedVtabDdl, c_int> {
    unsafe {
        // Extract virtual table name from argv[2]
        let table_name = if argc > 2 && !(*argv.offset(2)).is_null() {
            CStr::from_ptr(*argv.offset(2))
                .to_string_lossy()
                .trim()
                .to_string()
        } else {
            String::from("vtab")
        };

        let mut explicit_table_id: Option<u16> = None;
        let mut max_bytes: u64 = 0;
        let mut durability = WriteDurability::BufferedFast;
        let mut on_corrupt: Option<CorruptPolicy> = None;
        let mut inline_max: Option<usize> = None;
        let mut columns_spec = String::from("key TEXT PRIMARY KEY, value BLOB");
        let mut indexes_spec = String::new();

        for i in 3..argc {
            let arg_ptr = *argv.offset(i as isize);
            if arg_ptr.is_null() {
                continue;
            }
            let raw_arg = CStr::from_ptr(arg_ptr).to_string_lossy();
            if let Some((k, v)) = raw_arg.split_once('=') {
                let key = k.trim().to_ascii_lowercase();
                let val = v.trim().trim_matches('\'').trim_matches('"').trim();
                match key.as_str() {
                    "table_id" => match val.parse::<u16>() {
                        Ok(id) if id != 0 => explicit_table_id = Some(id),
                        _ => return Err(SQLITE_ERROR),
                    },
                    "max_bytes" => {
                        if let Ok(mb) = val.parse::<u64>() {
                            max_bytes = mb;
                        }
                    }
                    "durability" => {
                        if val.eq_ignore_ascii_case("sync") {
                            durability = WriteDurability::SyncTransactional;
                        } else {
                            durability = WriteDurability::BufferedFast;
                        }
                    }
                    "on_corrupt" => {
                        if val.eq_ignore_ascii_case("error") {
                            on_corrupt = Some(CorruptPolicy::Error);
                        } else if val.eq_ignore_ascii_case("evict") {
                            on_corrupt = Some(CorruptPolicy::Evict);
                        } else {
                            return Err(SQLITE_ERROR);
                        }
                    }
                    "inline_max" => match val.parse::<usize>() {
                        Ok(thresh) if thresh <= 4096 => inline_max = Some(thresh),
                        _ => return Err(SQLITE_ERROR),
                    },
                    "columns" => {
                        columns_spec = val.to_string();
                    }
                    "indexes" => {
                        indexes_spec = val.to_string();
                    }
                    _ => {}
                }
            }
        }

        // Parse column names, identify PRIMARY KEY column index, and determine affinity & collation
        let mut col_names = Vec::new();
        let mut col_affinities = Vec::new();
        let mut col_collations = Vec::new();
        let mut pk_col_idx = 0usize;
        for (idx, col_def) in split_top_level_commas(&columns_spec).into_iter().enumerate() {
            let trimmed = col_def.trim();
            let raw_tokens: Vec<&str> = trimmed.split_whitespace().collect();
            if raw_tokens.is_empty() {
                continue;
            }
            col_names.push(raw_tokens[0].to_string());

            let mut collation = ColCollation::Binary;
            let mut type_tokens = Vec::new();
            let mut t_idx = 1usize;
            while t_idx < raw_tokens.len() {
                let tok_upper = raw_tokens[t_idx].to_ascii_uppercase();
                if tok_upper == "COLLATE" {
                    if t_idx + 1 >= raw_tokens.len() {
                        return Err(SQLITE_ERROR);
                    }
                    let coll_name = raw_tokens[t_idx + 1]
                        .trim_matches(|c: char| !c.is_ascii_alphanumeric() && c != '_');
                    let Some(parsed_coll) = ColCollation::from_name(coll_name) else {
                        return Err(SQLITE_ERROR);
                    };
                    collation = parsed_coll;
                    t_idx += 2;
                    continue;
                }
                if tok_upper == "PRIMARY"
                    && t_idx + 1 < raw_tokens.len()
                    && raw_tokens[t_idx + 1].eq_ignore_ascii_case("KEY")
                {
                    pk_col_idx = idx;
                    t_idx += 2;
                    continue;
                }
                if tok_upper == "NOT"
                    && t_idx + 1 < raw_tokens.len()
                    && raw_tokens[t_idx + 1].eq_ignore_ascii_case("NULL")
                {
                    t_idx += 2;
                    continue;
                }
                if tok_upper == "UNIQUE" {
                    t_idx += 1;
                    continue;
                }
                type_tokens.push(tok_upper);
                t_idx += 1;
            }
            let type_part = type_tokens.join(" ");
            col_affinities.push(ColumnAffinity::from_type_str(&type_part));
            col_collations.push(collation);
        }
        if col_names.is_empty() {
            return Err(SQLITE_ERROR);
        }
        let pk_affinity = col_affinities
            .get(pk_col_idx)
            .copied()
            .unwrap_or(ColumnAffinity::Blob);
        let pk_collation = col_collations
            .get(pk_col_idx)
            .copied()
            .unwrap_or(ColCollation::Binary);

        // Parse secondary index column names
        let mut secondary_indexes = Vec::new();
        let mut next_idx_id = 1u16;
        for idx_col in indexes_spec.split(',') {
            let target = idx_col.trim();
            if target.is_empty() {
                continue;
            }
            if let Some(pos) = col_names
                .iter()
                .position(|c| c.eq_ignore_ascii_case(target))
            {
                secondary_indexes.push((pos, next_idx_id));
                next_idx_id += 1;
            }
        }

        Ok(ParsedVtabDdl {
            table_name,
            explicit_table_id,
            max_bytes,
            durability,
            on_corrupt,
            inline_max,
            columns_spec,
            col_names,
            col_affinities,
            col_collations,
            pk_col_idx,
            pk_affinity,
            pk_collation,
            secondary_indexes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;

    fn parse(opts: &[&str]) -> Result<ParsedVtabDdl, c_int> {
        let mut args: Vec<CString> = ["aydb", "main", "t"]
            .iter()
            .map(|s| CString::new(*s).unwrap())
            .collect();
        args.extend(opts.iter().map(|s| CString::new(*s).unwrap()));
        let ptrs: Vec<*const c_char> = args.iter().map(|c| c.as_ptr()).collect();
        unsafe { parse_vtab_ddl(ptrs.len() as c_int, ptrs.as_ptr()) }
    }

    #[test]
    fn on_corrupt_option_parsing() {
        assert_eq!(parse(&[]).unwrap().on_corrupt, None);
        assert_eq!(
            parse(&["on_corrupt=error"]).unwrap().on_corrupt,
            Some(CorruptPolicy::Error)
        );
        assert_eq!(
            parse(&["on_corrupt = 'EVICT'"]).unwrap().on_corrupt,
            Some(CorruptPolicy::Evict)
        );
        assert_eq!(
            parse(&["max_bytes=10", "ON_CORRUPT=Evict"]).unwrap().on_corrupt,
            Some(CorruptPolicy::Evict)
        );
        assert_eq!(parse(&["on_corrupt=ignore"]).err(), Some(SQLITE_ERROR));
        assert_eq!(parse(&["on_corrupt="]).err(), Some(SQLITE_ERROR));
    }

    #[test]
    fn inline_max_option_parsing() {
        assert_eq!(parse(&[]).unwrap().inline_max, None);
        assert_eq!(
            parse(&["inline_max=2048"]).unwrap().inline_max,
            Some(2048)
        );
        assert_eq!(
            parse(&["inline_max=0"]).unwrap().inline_max,
            Some(0)
        );
        assert_eq!(
            parse(&["INLINE_MAX=4096"]).unwrap().inline_max,
            Some(4096)
        );
        assert_eq!(
            parse(&["inline_max = '3500'"]).unwrap().inline_max,
            Some(3500)
        );
        assert_eq!(parse(&["inline_max=4097"]).err(), Some(SQLITE_ERROR));
        assert_eq!(parse(&["inline_max=-1"]).err(), Some(SQLITE_ERROR));
        assert_eq!(parse(&["inline_max=abc"]).err(), Some(SQLITE_ERROR));
        assert_eq!(parse(&["inline_max="]).err(), Some(SQLITE_ERROR));
    }
}
