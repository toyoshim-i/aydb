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
    /// The column list handed to `sqlite3_declare_vtab`: the definition without the constraints
    /// that make SQLite build an index object for the table (see [`ColumnDef::declared`]).
    pub(super) declared_columns: String,
    pub(super) col_names: Vec<String>,
    pub(super) col_affinities: Vec<ColumnAffinity>,
    pub(super) col_collations: Vec<ColCollation>,
    pub(super) col_not_null: Vec<bool>,
    pub(super) col_unique: Vec<bool>,
    pub(super) pk_col_idx: usize,
    pub(super) pk_affinity: ColumnAffinity,
    /// The key column is a rowid alias: declared exactly `INTEGER PRIMARY KEY` (not `DESC`).
    /// Only then does a NULL key mean "assign the next key".
    pub(super) pk_rowid_alias: bool,
    /// Some column has a `CHECK` constraint, which this adapter cannot enforce.
    pub(super) has_check: bool,
    pub(super) pk_collation: ColCollation,
    pub(super) secondary_indexes: Vec<(usize, u16)>,
}

/// Splits a comma-separated SQL column list at top-level commas only, ignoring commas inside
/// parenthesized type parameters such as `DECIMAL(10, 2)`.
pub(super) fn split_top_level_commas(input: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0usize;
    let mut start = 0usize;
    let mut quote = None;
    let mut chars = input.char_indices().peekable();
    while let Some((idx, ch)) = chars.next() {
        if let Some(end) = quote {
            if ch == end {
                if end != ']' && chars.peek().is_some_and(|(_, next)| *next == end) {
                    chars.next();
                } else {
                    quote = None;
                }
            }
            continue;
        }
        match ch {
            '\'' | '"' | '`' => quote = Some(ch),
            '[' => quote = Some(']'),
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

/// Returns true if a comma-separated `columns` entry is a table-level constraint
/// (`PRIMARY KEY(..)`, `UNIQUE(..)`, `CHECK(..)`, `FOREIGN KEY(..)`, `CONSTRAINT name ..`)
/// rather than a column definition. These are not supported and must not be mistaken for a column.
fn is_table_constraint(tokens: &[&str]) -> bool {
    let first = tokens[0];
    let word = first
        .split('(')
        .next()
        .unwrap_or(first)
        .to_ascii_uppercase();
    let has_paren = first.contains('(') || tokens.get(1).is_some_and(|t| t.starts_with('('));
    let second_is_key = tokens
        .get(1)
        .is_some_and(|t| t.to_ascii_uppercase().starts_with("KEY"));
    match word.as_str() {
        "PRIMARY" | "FOREIGN" => second_is_key,
        "UNIQUE" | "CHECK" => has_paren,
        "CONSTRAINT" => tokens.len() > 2,
        _ => false,
    }
}

/// Split a column definition without splitting quoted values or parenthesized expressions.
fn column_tokens(input: &str) -> Vec<&str> {
    let bytes = input.as_bytes();
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        let start = i;
        let mut depth = 0usize;
        while i < bytes.len() {
            match bytes[i] {
                b'\'' | b'"' | b'`' | b'[' => {
                    let end = if bytes[i] == b'[' { b']' } else { bytes[i] };
                    i += 1;
                    while i < bytes.len() {
                        if bytes[i] == end {
                            i += 1;
                            if end != b']' && bytes.get(i) == Some(&end) {
                                i += 1;
                            } else {
                                break;
                            }
                        } else {
                            i += 1;
                        }
                    }
                }
                b'(' => {
                    depth += 1;
                    i += 1;
                }
                b')' => {
                    depth = depth.saturating_sub(1);
                    i += 1;
                }
                b if b.is_ascii_whitespace() && depth == 0 => break,
                _ => i += 1,
            }
        }
        if start < i {
            tokens.push(&input[start..i]);
        }
    }
    tokens
}

/// Removes one pair of matching `'...'` or `"..."` wrapping from an option value and undoes the
/// doubled quote character inside it. Identifier quotes inside the value are left alone.
fn unwrap_option_value(raw: &str) -> String {
    let value = raw.trim();
    for quote in ['\'', '"'] {
        if value.len() >= 2 && value.starts_with(quote) && value.ends_with(quote) {
            let inner = &value[1..value.len() - 1];
            let doubled = format!("{quote}{quote}");
            return inner
                .replace(&doubled, &quote.to_string())
                .trim()
                .to_string();
        }
    }
    value.to_string()
}

/// Returns the name a SQL identifier token denotes: `"a""b"`, `` `a` ``, `[a]` and `'a'` lose
/// their quotes (a doubled quote character collapses); a bare name is returned unchanged.
pub(super) fn unquote_identifier(token: &str) -> String {
    let close = match token.chars().next() {
        Some(first @ ('"' | '`' | '\'')) => first,
        Some('[') => ']',
        _ => return token.to_string(),
    };
    if token.len() < 2 || !token.ends_with(close) {
        return token.to_string();
    }
    let inner = &token[1..token.len() - 1];
    if close == ']' {
        inner.to_string()
    } else {
        inner.replace(&format!("{close}{close}"), &close.to_string())
    }
}

/// True if `input` contains `--` or `/*` outside quoted identifiers and string literals.
fn has_comment_outside_quotes(input: &str) -> bool {
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            quote @ (b'\'' | b'"' | b'`' | b'[') => {
                let end = if quote == b'[' { b']' } else { quote };
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == end {
                        i += 1;
                        if end != b']' && bytes.get(i) == Some(&end) {
                            i += 1;
                        } else {
                            break;
                        }
                    } else {
                        i += 1;
                    }
                }
            }
            b'-' if bytes.get(i + 1) == Some(&b'-') => return true,
            b'/' if bytes.get(i + 1) == Some(&b'*') => return true,
            _ => i += 1,
        }
    }
    false
}

/// The word a column-definition token starts with, uppercased: the leading run of word
/// characters. `CHECK(a>0)` gives `CHECK`; a quoted token gives `None`.
fn token_keyword(token: &str) -> Option<String> {
    let word: String = token
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
        .collect();
    if word.is_empty() {
        None
    } else {
        Some(word.to_ascii_uppercase())
    }
}

/// Words that start a column constraint and therefore end the type name.
const CONSTRAINT_KEYWORDS: [&str; 11] = [
    "CONSTRAINT",
    "PRIMARY",
    "NOT",
    "NULL",
    "UNIQUE",
    "CHECK",
    "DEFAULT",
    "COLLATE",
    "REFERENCES",
    "GENERATED",
    "AS",
];

fn is_word(token: Option<&&str>, word: &str) -> bool {
    token.is_some_and(|t| t.eq_ignore_ascii_case(word))
}

/// One parsed column definition.
struct ColumnDef {
    name: String,
    affinity: ColumnAffinity,
    collation: ColCollation,
    not_null: bool,
    unique: bool,
    primary_key: bool,
    /// `INTEGER PRIMARY KEY` that SQLite treats as an alias of the rowid.
    rowid_alias: bool,
    check: bool,
    /// The definition as declared to SQLite. A `PRIMARY KEY` that is not a rowid alias and
    /// `UNIQUE` are left out: SQLite would build an automatic index for each, and it walks those
    /// when it checks the database, finding pages that a virtual table does not have
    /// ("invalid page number"). The adapter enforces both itself. Everything SQLite does use for
    /// a virtual table stays: name, declared type, `NOT NULL`, `COLLATE`, and `INTEGER PRIMARY
    /// KEY` (the rowid alias, which has no index).
    declared: String,
}

/// Consumes the parenthesised group that follows keyword token `tokens[i]`, either attached to
/// it (`CHECK(a>0)`) or as the next token (`CHECK (a>0)`). Returns the index after the group.
fn skip_paren_group(tokens: &[&str], i: usize) -> Option<usize> {
    let word_len = tokens[i]
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
        .count();
    if tokens[i][word_len..].starts_with('(') {
        Some(i + 1)
    } else if tokens.get(i + 1).is_some_and(|t| t.starts_with('(')) {
        Some(i + 2)
    } else {
        None
    }
}

/// Parses one column definition (`column-def` of the SQLite grammar) from its tokens: the
/// column name, the type name (identifiers up to the first constraint keyword, with an optional
/// parenthesised size), then column constraints. The affinity comes from the type name only.
fn parse_column_def(tokens: &[&str]) -> Result<ColumnDef, c_int> {
    let mut def = ColumnDef {
        name: unquote_identifier(tokens[0]),
        affinity: ColumnAffinity::Blob,
        collation: ColCollation::Binary,
        not_null: false,
        unique: false,
        primary_key: false,
        rowid_alias: false,
        check: false,
        declared: String::new(),
    };
    let mut i = 1usize;
    let mut type_words: Vec<String> = Vec::new();
    let mut has_size = false;
    while i < tokens.len() {
        let tok = tokens[i];
        let quoted = matches!(tok.as_bytes()[0], b'"' | b'\'' | b'`' | b'[');
        if !quoted && token_keyword(tok).is_some_and(|k| CONSTRAINT_KEYWORDS.contains(&k.as_str()))
        {
            break;
        }
        if tok.contains('(') {
            has_size = true;
        }
        if quoted {
            type_words.push(unquote_identifier(tok).to_ascii_uppercase());
        } else if !tok.starts_with('(') {
            // `VARCHAR(10)`: the size takes no part in the affinity.
            let word = tok.split('(').next().unwrap_or(tok);
            type_words.push(word.to_ascii_uppercase());
        }
        i += 1;
    }
    def.affinity = ColumnAffinity::from_type_str(&type_words.join(" "));
    let integer_typed = type_words.len() == 1 && type_words[0] == "INTEGER" && !has_size;
    let mut declared: Vec<&str> = tokens[..i].to_vec();
    // `CONSTRAINT name`, kept only together with the constraint that follows it.
    let mut pending_name: Option<&[&str]> = None;

    while i < tokens.len() {
        let Some(keyword) = token_keyword(tokens[i]) else {
            return Err(SQLITE_ERROR);
        };
        let start = i;
        match keyword.as_str() {
            "CONSTRAINT" => {
                if i + 1 >= tokens.len() {
                    return Err(SQLITE_ERROR);
                }
                i += 2;
                pending_name = Some(&tokens[start..i]);
                continue;
            }
            "PRIMARY" => {
                if !is_word(tokens.get(i + 1), "KEY") {
                    return Err(SQLITE_ERROR);
                }
                def.primary_key = true;
                def.rowid_alias = integer_typed;
                i += 2;
                if is_word(tokens.get(i), "DESC") {
                    def.rowid_alias = false;
                    i += 1;
                } else if is_word(tokens.get(i), "ASC") {
                    i += 1;
                }
                if is_word(tokens.get(i), "AUTOINCREMENT") {
                    i += 1;
                }
            }
            "NOT" => {
                if !is_word(tokens.get(i + 1), "NULL") {
                    return Err(SQLITE_ERROR);
                }
                def.not_null = true;
                i += 2;
            }
            "NULL" => i += 1,
            "UNIQUE" => {
                def.unique = true;
                i += 1;
            }
            // Parsed so that the rest of the definition is read correctly. The adapter cannot
            // evaluate it, so `xCreate` rejects it and a table that has one refuses writes.
            "CHECK" => {
                def.check = true;
                i = skip_paren_group(tokens, i).ok_or(SQLITE_ERROR)?;
            }
            "COLLATE" => {
                let Some(name) = tokens.get(i + 1) else {
                    return Err(SQLITE_ERROR);
                };
                let Some(collation) = ColCollation::from_name(&unquote_identifier(name)) else {
                    return Err(SQLITE_ERROR);
                };
                def.collation = collation;
                i += 2;
            }
            "REFERENCES" => {
                // SQLite does not enforce foreign keys on virtual tables; skip the clause.
                i += 1;
                if i >= tokens.len() {
                    return Err(SQLITE_ERROR);
                }
                i += 1; // parent table, possibly with its column list attached
                if tokens.get(i).is_some_and(|t| t.starts_with('(')) {
                    i += 1;
                }
                i = skip_foreign_key_actions(tokens, i)?;
            }
            // DEFAULT values and generated columns are not supported.
            _ => return Err(SQLITE_ERROR),
        }
        let leaves_index = keyword == "UNIQUE" || (keyword == "PRIMARY" && !def.rowid_alias);
        let name = pending_name.take();
        if !leaves_index {
            declared.extend_from_slice(name.unwrap_or(&[]));
            declared.extend_from_slice(&tokens[start..i]);
        }
    }
    def.declared = declared.join(" ");
    Ok(def)
}

/// Skips the optional tail of a `REFERENCES` clause: `ON DELETE|UPDATE action`, `MATCH name` and
/// `[NOT] DEFERRABLE [INITIALLY DEFERRED|IMMEDIATE]`.
fn skip_foreign_key_actions(tokens: &[&str], mut i: usize) -> Result<usize, c_int> {
    while i < tokens.len() {
        match tokens[i].to_ascii_uppercase().as_str() {
            "ON" => {
                if !(is_word(tokens.get(i + 1), "DELETE") || is_word(tokens.get(i + 1), "UPDATE")) {
                    return Err(SQLITE_ERROR);
                }
                let action = tokens
                    .get(i + 2)
                    .map(|t| t.to_ascii_uppercase())
                    .ok_or(SQLITE_ERROR)?;
                i += match action.as_str() {
                    "SET" | "NO" => 4,
                    "CASCADE" | "RESTRICT" => 3,
                    _ => return Err(SQLITE_ERROR),
                };
            }
            "MATCH" | "INITIALLY" => i += 2,
            "NOT" if is_word(tokens.get(i + 1), "DEFERRABLE") => i += 2,
            "DEFERRABLE" => i += 1,
            _ => break,
        }
    }
    Ok(i.min(tokens.len()))
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
                let val = unwrap_option_value(v);
                match key.as_str() {
                    "table_id" => match val.parse::<u16>() {
                        Ok(id) if id != 0 => explicit_table_id = Some(id),
                        _ => return Err(SQLITE_ERROR),
                    },
                    "max_bytes" => match val.parse::<u64>() {
                        Ok(mb) => max_bytes = mb,
                        Err(_) => return Err(SQLITE_ERROR),
                    },
                    "durability" => {
                        if val.eq_ignore_ascii_case("sync") {
                            durability = WriteDurability::SyncTransactional;
                        } else if val.eq_ignore_ascii_case("buffered") {
                            durability = WriteDurability::BufferedFast;
                        } else {
                            return Err(SQLITE_ERROR);
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
                    _ => return Err(SQLITE_ERROR),
                }
            } else if !raw_arg.trim().is_empty() {
                return Err(SQLITE_ERROR);
            }
        }

        // Parse column names, identify PRIMARY KEY column index, and determine affinity & collation
        let mut col_names = Vec::new();
        let mut col_affinities = Vec::new();
        let mut col_collations = Vec::new();
        let mut col_not_null = Vec::new();
        let mut col_unique = Vec::new();
        let mut pk_col_idx = 0usize;
        let mut pk_rowid_alias = false;
        let mut has_check = false;
        let mut declared_columns: Vec<String> = Vec::new();
        for col_def in split_top_level_commas(&columns_spec) {
            let trimmed = col_def.trim();
            // The parser does not support comments or per-constraint conflict policies.
            // Reject them rather than silently misinterpret a constraint.
            if has_comment_outside_quotes(trimmed) {
                return Err(SQLITE_ERROR);
            }
            let raw_tokens = column_tokens(trimmed);
            if raw_tokens.windows(2).any(|pair| {
                pair[0].eq_ignore_ascii_case("ON") && pair[1].eq_ignore_ascii_case("CONFLICT")
            }) {
                return Err(SQLITE_ERROR);
            }
            if raw_tokens.is_empty() {
                continue;
            }
            if is_table_constraint(&raw_tokens) {
                return Err(SQLITE_ERROR);
            }
            let def = parse_column_def(&raw_tokens)?;
            if def.primary_key {
                pk_col_idx = col_names.len();
                pk_rowid_alias = def.rowid_alias;
            }
            has_check |= def.check;
            declared_columns.push(def.declared);
            col_names.push(def.name);
            col_affinities.push(def.affinity);
            col_collations.push(def.collation);
            col_not_null.push(def.not_null);
            col_unique.push(def.unique);
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
        for idx_col in split_top_level_commas(&indexes_spec) {
            if idx_col.trim().is_empty() {
                continue;
            }
            let target = unquote_identifier(idx_col.trim());
            let Some(pos) = col_names
                .iter()
                .position(|c| c.eq_ignore_ascii_case(&target))
            else {
                return Err(SQLITE_ERROR);
            };
            secondary_indexes.push((pos, next_idx_id));
            next_idx_id += 1;
        }

        Ok(ParsedVtabDdl {
            table_name,
            explicit_table_id,
            max_bytes,
            durability,
            on_corrupt,
            inline_max,
            declared_columns: declared_columns.join(", "),
            col_names,
            col_affinities,
            col_collations,
            col_not_null,
            col_unique,
            pk_col_idx,
            pk_affinity,
            pk_rowid_alias,
            has_check,
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
            parse(&["max_bytes=10", "ON_CORRUPT=Evict"])
                .unwrap()
                .on_corrupt,
            Some(CorruptPolicy::Evict)
        );
        assert_eq!(parse(&["on_corrupt=ignore"]).err(), Some(SQLITE_ERROR));
        assert_eq!(parse(&["on_corrupt="]).err(), Some(SQLITE_ERROR));
    }

    #[test]
    fn inline_max_option_parsing() {
        assert_eq!(parse(&[]).unwrap().inline_max, None);
        assert_eq!(parse(&["inline_max=2048"]).unwrap().inline_max, Some(2048));
        assert_eq!(parse(&["inline_max=0"]).unwrap().inline_max, Some(0));
        assert_eq!(parse(&["INLINE_MAX=4096"]).unwrap().inline_max, Some(4096));
        assert_eq!(
            parse(&["inline_max = '3500'"]).unwrap().inline_max,
            Some(3500)
        );
        assert_eq!(parse(&["inline_max=4097"]).err(), Some(SQLITE_ERROR));
        assert_eq!(parse(&["inline_max=-1"]).err(), Some(SQLITE_ERROR));
        assert_eq!(parse(&["inline_max=abc"]).err(), Some(SQLITE_ERROR));
        assert_eq!(parse(&["inline_max="]).err(), Some(SQLITE_ERROR));
    }

    #[test]
    fn durability_option_parsing() {
        assert!(matches!(
            parse(&[]).unwrap().durability,
            WriteDurability::BufferedFast
        ));
        assert!(matches!(
            parse(&["durability='SYNC'"]).unwrap().durability,
            WriteDurability::SyncTransactional
        ));
        assert!(matches!(
            parse(&["durability = 'buffered'"]).unwrap().durability,
            WriteDurability::BufferedFast
        ));
        assert_eq!(parse(&["durability='snyc'"]).err(), Some(SQLITE_ERROR));
        assert_eq!(parse(&["durability="]).err(), Some(SQLITE_ERROR));
    }

    #[test]
    fn max_bytes_option_parsing() {
        assert_eq!(parse(&["max_bytes=4096"]).unwrap().max_bytes, 4096);
        assert_eq!(parse(&["max_bytes='10'"]).unwrap().max_bytes, 10);
        assert_eq!(parse(&["max_bytes=10MB"]).err(), Some(SQLITE_ERROR));
        assert_eq!(parse(&["max_bytes=-1"]).err(), Some(SQLITE_ERROR));
        assert_eq!(parse(&["max_bytes="]).err(), Some(SQLITE_ERROR));
    }

    #[test]
    fn unknown_option_keys_are_rejected() {
        assert_eq!(parse(&["max_byte=10"]).err(), Some(SQLITE_ERROR));
        assert_eq!(parse(&["bogus"]).err(), Some(SQLITE_ERROR));
        assert!(parse(&["table_id=3", "indexes=", "on_corrupt=error"]).is_ok());
    }

    #[test]
    fn indexes_must_name_existing_columns() {
        let cols = "columns='id INTEGER PRIMARY KEY, name TEXT, tag TEXT'";
        let ok = parse(&[cols, "indexes='name, TAG'"]).unwrap();
        assert_eq!(ok.secondary_indexes, vec![(1, 1), (2, 2)]);
        assert_eq!(parse(&[cols, "indexes='nme'"]).err(), Some(SQLITE_ERROR));
        assert_eq!(
            parse(&[cols, "indexes='name,missing'"]).err(),
            Some(SQLITE_ERROR)
        );
    }

    #[test]
    fn table_level_constraints_are_rejected() {
        for spec in [
            "columns='a TEXT, b TEXT, PRIMARY KEY(a)'",
            "columns='a TEXT, b TEXT, PRIMARY KEY (a, b)'",
            "columns='a TEXT PRIMARY KEY, b TEXT, UNIQUE(b)'",
            "columns='a TEXT PRIMARY KEY, b INT, CHECK (b > 0)'",
            "columns='a TEXT PRIMARY KEY, b INT, FOREIGN KEY(b) REFERENCES x(y)'",
            "columns='a TEXT PRIMARY KEY, CONSTRAINT c UNIQUE(a)'",
        ] {
            assert_eq!(parse(&[spec]).err(), Some(SQLITE_ERROR), "{spec}");
        }
        let ok =
            parse(&["columns='a TEXT PRIMARY KEY, check_v INT UNIQUE, primary_x TEXT'"]).unwrap();
        assert_eq!(ok.col_names, vec!["a", "check_v", "primary_x"]);
    }

    fn cols(spec: &str) -> ParsedVtabDdl {
        parse(&[&format!("columns='{spec}'")]).unwrap_or_else(|_| panic!("rejected [{spec}]"))
    }

    #[test]
    fn type_name_ends_at_the_first_constraint_keyword() {
        use ColumnAffinity::*;
        for (spec, affinity) in [
            ("k TEXT PRIMARY KEY, b TEXT CONSTRAINT u UNIQUE", Text),
            ("k TEXT PRIMARY KEY, b TEXT CONSTRAINT nn NOT NULL", Text),
            ("k TEXT PRIMARY KEY, b TEXT REFERENCES p(id)", Text),
            (
                "k TEXT PRIMARY KEY, b TEXT REFERENCES p (id) ON DELETE CASCADE",
                Text,
            ),
            ("k TEXT PRIMARY KEY, b TEXT CHECK(b<>'')", Text),
            ("k TEXT PRIMARY KEY, b TEXT CHECK (b <> 'x y')", Text),
            ("k TEXT PRIMARY KEY, b VARCHAR(10) NOT NULL", Text),
            ("k TEXT PRIMARY KEY, b DECIMAL(10, 2)", Numeric),
            ("k TEXT PRIMARY KEY, b FANCY", Numeric),
            ("k TEXT PRIMARY KEY, b DOUBLE PRECISION", Real),
            ("k TEXT PRIMARY KEY, b UNSIGNED BIG INT", Integer),
            ("k TEXT PRIMARY KEY, b", Blob),
            ("k TEXT PRIMARY KEY, b NOT NULL", Blob),
            ("k TEXT PRIMARY KEY, b CONSTRAINT c NOT NULL", Blob),
            ("k TEXT PRIMARY KEY, b \"INT\"", Integer),
            ("k TEXT PRIMARY KEY, b POINT", Integer),
        ] {
            assert_eq!(cols(spec).col_affinities[1], affinity, "{spec}");
        }
    }

    #[test]
    fn column_constraints_are_recognised_with_names() {
        let d = cols(
            "a TEXT CONSTRAINT p PRIMARY KEY, b INT CONSTRAINT u UNIQUE, c TEXT CONSTRAINT n NOT NULL COLLATE NOCASE",
        );
        assert_eq!(d.pk_col_idx, 0);
        assert_eq!(d.col_unique, vec![false, true, false]);
        assert_eq!(d.col_not_null, vec![false, false, true]);
        assert_eq!(d.col_collations[2], ColCollation::NoCase);
        let d = cols("a INT, b TEXT PRIMARY KEY CONSTRAINT n NOT NULL");
        assert_eq!(d.pk_col_idx, 1);
        assert_eq!(d.col_not_null, vec![false, true]);
    }

    #[test]
    fn quoted_names_are_unquoted_and_never_keywords() {
        let d = cols(
            "\"a--b\" TEXT PRIMARY KEY, [v/*x] INT, `p q` TEXT, \"primary\" TEXT UNIQUE, \"unique\" INT, 'n' TEXT, \"x\"\"y\" TEXT",
        );
        assert_eq!(
            d.col_names,
            vec!["a--b", "v/*x", "p q", "primary", "unique", "n", "x\"y"]
        );
        assert!(d.col_unique[3]);
        assert!(!d.col_unique[4]);
        assert_eq!(d.col_affinities[4], ColumnAffinity::Integer);
        assert_eq!(d.pk_col_idx, 0);
    }

    #[test]
    fn columns_option_unwrap_keeps_identifier_quotes() {
        let d = parse(&["columns='\"k\" INTEGER PRIMARY KEY, \"v\"'"]).unwrap();
        assert_eq!(d.col_names, vec!["k", "v"]);
        assert_eq!(d.col_affinities[0], ColumnAffinity::Integer);
        let d = parse(&["columns=\"\"\"k\"\" TEXT PRIMARY KEY, v INT\""]).unwrap();
        assert_eq!(d.col_names, vec!["k", "v"]);
        let d = parse(&["columns=\"k TEXT PRIMARY KEY, v INT\""]).unwrap();
        assert_eq!(d.col_names, vec!["k", "v"]);
        assert_eq!(unwrap_option_value("  'a''b'  "), "a'b");
        assert_eq!(unwrap_option_value("\"'a'\""), "'a'");
    }

    #[test]
    fn comments_are_rejected_only_outside_quotes() {
        for spec in [
            "k TEXT PRIMARY KEY, v TEXT -- c",
            "k TEXT PRIMARY KEY, v TEXT /* c */",
            "k TEXT PRIMARY KEY, v TEXT NOT/**/NULL",
            "k TEXT PRIMARY KEY, v TEXT CHECK(v > 0 -- c\n)",
        ] {
            assert_eq!(
                parse(&[&format!("columns='{spec}'")]).err(),
                Some(SQLITE_ERROR),
                "{spec}"
            );
        }
        let d = cols("\"a--b\" TEXT PRIMARY KEY, v TEXT CHECK(v <> '--'), w TEXT CHECK(w <> '/*')");
        assert_eq!(d.col_names, vec!["a--b", "v", "w"]);
    }

    #[test]
    fn unsupported_column_clauses_stay_rejected() {
        for spec in [
            "k TEXT PRIMARY KEY, v INT DEFAULT 1",
            "k TEXT PRIMARY KEY, v INT DEFAULT(1)",
            "k TEXT PRIMARY KEY, v INT UNIQUE ON CONFLICT IGNORE",
            "k TEXT PRIMARY KEY, v INT GENERATED ALWAYS AS (1)",
            "k TEXT PRIMARY KEY, v INT AS (1)",
            "k TEXT PRIMARY KEY, v INT COLLATE UNKNOWN",
            "k TEXT PRIMARY KEY, v INT CHECK",
        ] {
            assert_eq!(
                parse(&[&format!("columns='{spec}'")]).err(),
                Some(SQLITE_ERROR),
                "{spec}"
            );
        }
    }

    #[test]
    fn indexes_match_quoted_column_names() {
        let cols = "columns='\"a b\" INTEGER PRIMARY KEY, \"x,y\" TEXT, `z` TEXT'";
        let ok = parse(&[cols, "indexes='\"x,y\", [Z]'"]).unwrap();
        assert_eq!(ok.secondary_indexes, vec![(1, 1), (2, 2)]);
        assert_eq!(parse(&[cols, "indexes='\"x\"'"]).err(), Some(SQLITE_ERROR));
    }

    #[test]
    fn only_integer_primary_key_is_a_rowid_alias() {
        for (spec, alias) in [
            ("k INTEGER PRIMARY KEY, v TEXT", true),
            ("k integer primary key not null, v TEXT", true),
            ("k INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, v TEXT", true),
            ("k INTEGER CONSTRAINT p PRIMARY KEY ASC, v TEXT", true),
            ("k INT PRIMARY KEY, v TEXT", false),
            ("k BIGINT PRIMARY KEY, v TEXT", false),
            ("k TEXT PRIMARY KEY, v TEXT", false),
            ("k INTEGER(4) PRIMARY KEY, v TEXT", false),
            ("k UNSIGNED INTEGER PRIMARY KEY, v TEXT", false),
            ("k INTEGER PRIMARY KEY DESC, v TEXT", false),
            ("k INTEGER, v TEXT", false),
        ] {
            assert_eq!(cols(spec).pk_rowid_alias, alias, "{spec}");
        }
    }

    #[test]
    fn declaration_for_sqlite_drops_what_creates_an_index() {
        for (spec, declared) in [
            ("k TEXT PRIMARY KEY, v INT", "k TEXT, v INT"),
            (
                "k INTEGER PRIMARY KEY, v INT",
                "k INTEGER PRIMARY KEY, v INT",
            ),
            (
                "k INTEGER PRIMARY KEY NOT NULL, v INT",
                "k INTEGER PRIMARY KEY NOT NULL, v INT",
            ),
            ("k INT PRIMARY KEY NOT NULL, v INT", "k INT NOT NULL, v INT"),
            ("k INTEGER PRIMARY KEY DESC, v INT", "k INTEGER, v INT"),
            ("k TEXT CONSTRAINT p PRIMARY KEY, v INT", "k TEXT, v INT"),
            (
                "k TEXT PRIMARY KEY, u TEXT NOT NULL CONSTRAINT x UNIQUE COLLATE NOCASE",
                "k TEXT, u TEXT NOT NULL COLLATE NOCASE",
            ),
            (
                "\"a b\" TEXT PRIMARY KEY, `u--x` VARCHAR(10) UNIQUE, w",
                "\"a b\" TEXT, `u--x` VARCHAR(10), w",
            ),
            (
                "k TEXT PRIMARY KEY, v INT CONSTRAINT c NOT NULL REFERENCES p(id) ON DELETE CASCADE",
                "k TEXT, v INT CONSTRAINT c NOT NULL REFERENCES p(id) ON DELETE CASCADE",
            ),
        ] {
            assert_eq!(cols(spec).declared_columns, declared, "{spec}");
        }
    }
}
