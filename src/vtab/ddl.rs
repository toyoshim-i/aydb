//! `CREATE VIRTUAL TABLE` argument and column/affinity/collation specification parser.

use core::ffi::{c_char, c_int};
use std::ffi::CStr;

use crate::engine::{CorruptPolicy, WriteDurability};
use crate::vtab::ffi::SQLITE_ERROR;
use crate::vtab::row_codec::{ColCollation, ColumnAffinity};

/// Parsed schema and configuration options from `CREATE VIRTUAL TABLE ... USING aydb(...)`.
pub(super) struct ParsedVtabDdl {
    pub(super) db_name: String,
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
    /// Why some column cannot be honoured (`CHECK`, `DEFAULT`), as the error message for
    /// `xCreate` and for every write.
    pub(super) unsupported: Option<&'static str>,
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
        if !tail.trim().is_empty() || !parts.is_empty() {
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

/// Advances past a quoted literal or identifier starting at `bytes[i]` (`'...'`, `"..."`,
/// `` `...` ``, or `[...]`), returning an error if unclosed.
fn scan_quoted(bytes: &[u8], mut i: usize, quote: u8) -> Result<usize, c_int> {
    let close = if quote == b'[' { b']' } else { quote };
    i += 1;
    while i < bytes.len() {
        if bytes[i] == close {
            i += 1;
            if close != b']' && bytes.get(i) == Some(&close) {
                i += 1;
            } else {
                return Ok(i);
            }
        } else {
            i += 1;
        }
    }
    Err(SQLITE_ERROR)
}

/// Splits a column definition at SQL token boundaries (`(...)` groups, quotes, `+`/`-`, and
/// whitespace) while keeping SQLite `x'...'` blob literals and scientific notation intact.
fn column_tokens(input: &str) -> Result<Vec<&str>, c_int> {
    let bytes = input.as_bytes();
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= bytes.len() {
            break;
        }
        let start = i;
        match bytes[i] {
            q @ (b'\'' | b'"' | b'`' | b'[') => {
                i = scan_quoted(bytes, i, q)?;
            }
            b'(' => {
                let mut depth = 1usize;
                i += 1;
                while i < bytes.len() && depth > 0 {
                    match bytes[i] {
                        q @ (b'\'' | b'"' | b'`' | b'[') => {
                            i = scan_quoted(bytes, i, q)?;
                        }
                        b'(' => {
                            depth += 1;
                            i += 1;
                        }
                        b')' => {
                            depth -= 1;
                            i += 1;
                        }
                        _ => i += 1,
                    }
                }
                if depth != 0 {
                    return Err(SQLITE_ERROR);
                }
            }
            b')' => return Err(SQLITE_ERROR),
            b'+' | b'-' => {
                i += 1;
            }
            _ => {
                let starts_numeric = bytes[start].is_ascii_digit() || bytes[start] == b'.';
                while i < bytes.len() {
                    let b = bytes[i];
                    if b.is_ascii_whitespace()
                        || matches!(b, b'\'' | b'"' | b'`' | b'[' | b'(' | b')')
                    {
                        break;
                    }
                    if matches!(b, b'+' | b'-')
                        && !(starts_numeric && i > start && matches!(bytes[i - 1], b'e' | b'E'))
                    {
                        break;
                    }
                    i += 1;
                }
                // SQLite hex blob literal `x'...'` / `X'...'` stays a single token.
                if i - start == 1
                    && matches!(bytes[start], b'x' | b'X')
                    && bytes.get(i) == Some(&b'\'')
                {
                    i = scan_quoted(bytes, i, b'\'')?;
                }
            }
        }
        tokens.push(&input[start..i]);
    }
    Ok(tokens)
}

/// Removes one pair of matching `'...'` or `"..."` wrapping from an option value and undoes the
/// doubled quote character inside it. Identifier quotes inside the value are left alone.
fn unwrap_option_value(raw: &str) -> String {
    let value = raw.trim();
    let bytes = value.as_bytes();
    if bytes.len() >= 2 && matches!(bytes[0], b'\'' | b'"') {
        let quote = bytes[0];
        let mut i = 1;
        let mut unescaped = Vec::with_capacity(bytes.len() - 2);
        while i < bytes.len() {
            if bytes[i] == quote {
                if i + 1 < bytes.len() && bytes[i + 1] == quote {
                    unescaped.push(quote);
                    i += 2;
                } else if i + 1 == bytes.len() {
                    let s = String::from_utf8(unescaped)
                        .expect("stripping paired ASCII quotes preserves UTF-8");
                    return s.trim().to_string();
                } else {
                    return value.to_string();
                }
            } else {
                unescaped.push(bytes[i]);
                i += 1;
            }
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

/// Uppercased identifier keyword if `token` is a bare ASCII identifier word (`[A-Za-z0-9_]+`).
fn token_keyword(token: &str) -> Option<String> {
    if !token.is_empty()
        && token
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_')
    {
        Some(token.to_ascii_uppercase())
    } else {
        None
    }
}

/// Why a clause that an older build accepted cannot be honoured: creating such a table is
/// refused, and a table that has one refuses INSERT and UPDATE.
pub(super) const CHECK_UNSUPPORTED: &str = "CHECK constraints are not supported by aydb tables";
pub(super) const DEFAULT_UNSUPPORTED: &str = "DEFAULT values are not supported by aydb tables";
pub(super) const AUTOINCREMENT_UNSUPPORTED: &str = "AUTOINCREMENT is not supported by aydb tables";

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
    unsupported: Option<&'static str>,
    /// The definition as declared to SQLite. A `PRIMARY KEY` that is not a rowid alias and
    /// `UNIQUE` are left out: SQLite would build an automatic index for each, and it walks those
    /// when it checks the database, finding pages that a virtual table does not have
    /// ("invalid page number"). The adapter enforces both itself. Everything SQLite does use for
    /// a virtual table stays: name, declared type, `NOT NULL`, `COLLATE`, and `INTEGER PRIMARY
    /// KEY` (the rowid alias, which has no index).
    declared: String,
}

/// Consumes the parenthesised group token that follows keyword token `tokens[i]`. Returns the
/// index after the group.
fn skip_paren_group(tokens: &[&str], i: usize) -> Option<usize> {
    tokens
        .get(i + 1)
        .is_some_and(|t| t.starts_with('('))
        .then_some(i + 2)
}

/// Parses one column definition (`column-def` of the SQLite grammar) from its tokens: the
/// column name, the type name (identifiers up to the first constraint keyword, with an optional
/// parenthesised size), then column constraints. The affinity comes from the type name only.
fn parse_column_def(tokens: &[&str]) -> Result<ColumnDef, c_int> {
    if tokens[0].starts_with('(') {
        return Err(SQLITE_ERROR);
    }
    let mut def = ColumnDef {
        name: unquote_identifier(tokens[0]),
        affinity: ColumnAffinity::Blob,
        collation: ColCollation::Binary,
        not_null: false,
        unique: false,
        primary_key: false,
        rowid_alias: false,
        unsupported: None,
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
            type_words.push(tok.to_ascii_uppercase());
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
        let mut has_autoincrement = false;
        match keyword.as_str() {
            "CONSTRAINT" => {
                if pending_name.is_some() || i + 1 >= tokens.len() {
                    return Err(SQLITE_ERROR);
                }
                let name = tokens[i + 1];
                if name.starts_with('(')
                    || matches!(name, "+" | "-")
                    || unquote_identifier(name).is_empty()
                {
                    return Err(SQLITE_ERROR);
                }
                i += 2;
                pending_name = Some(&tokens[start..i]);
                continue;
            }
            "PRIMARY" => {
                if def.primary_key || !is_word(tokens.get(i + 1), "KEY") {
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
                    if !def.rowid_alias {
                        return Err(SQLITE_ERROR);
                    }
                    def.unsupported.get_or_insert(AUTOINCREMENT_UNSUPPORTED);
                    has_autoincrement = true;
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
                def.unsupported.get_or_insert(CHECK_UNSUPPORTED);
                i = skip_paren_group(tokens, i).ok_or(SQLITE_ERROR)?;
            }
            // `DEFAULT value`, `DEFAULT(expr)` or `DEFAULT 'text'`: skipped like CHECK. A write
            // that leaves the column out would store NULL, not the default, so a table that has
            // one is read-only for inserts and updates (see `unsupported`).
            "DEFAULT" => {
                def.unsupported.get_or_insert(DEFAULT_UNSUPPORTED);
                i += 1;
                if matches!(tokens.get(i), Some(&"+") | Some(&"-")) {
                    i += 1;
                }
                if i >= tokens.len() {
                    return Err(SQLITE_ERROR);
                }
                i += 1;
            }
            "COLLATE" => {
                let Some(name) = tokens.get(i + 1) else {
                    return Err(SQLITE_ERROR);
                };
                if name.starts_with('(') {
                    return Err(SQLITE_ERROR);
                }
                let unquoted = unquote_identifier(name);
                if unquoted.is_empty() || unquoted != unquoted.trim() {
                    return Err(SQLITE_ERROR);
                }
                let Some(collation) = ColCollation::from_name(&unquoted) else {
                    return Err(SQLITE_ERROR);
                };
                def.collation = collation;
                i += 2;
            }
            "REFERENCES" => {
                // SQLite does not enforce foreign keys on virtual tables; skip the clause.
                i += 1;
                if i >= tokens.len() || tokens[i].starts_with('(') {
                    return Err(SQLITE_ERROR);
                }
                i += 1; // parent table name
                if tokens.get(i).is_some_and(|t| t.starts_with('(')) {
                    i += 1;
                }
                i = skip_foreign_key_actions(tokens, i)?;
            }
            // Generated columns cannot even be declared to SQLite for a virtual table, so no
            // table with one exists.
            _ => return Err(SQLITE_ERROR),
        }
        let leaves_index = keyword == "UNIQUE" || (keyword == "PRIMARY" && !def.rowid_alias);
        let name = pending_name.take();
        if !leaves_index {
            declared.extend_from_slice(name.unwrap_or(&[]));
            if keyword == "PRIMARY" && has_autoincrement {
                declared.extend_from_slice(&tokens[start..i - 1]);
            } else {
                declared.extend_from_slice(&tokens[start..i]);
            }
        }
    }
    if pending_name.is_some() {
        return Err(SQLITE_ERROR);
    }
    let mut out = String::new();
    for tok in declared {
        if !out.is_empty() && !tok.starts_with('(') {
            out.push(' ');
        }
        out.push_str(tok);
    }
    def.declared = out;
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
    unsafe fn argv_lossy_at(argv: *const *const c_char, idx: isize) -> Option<String> {
        let ptr = unsafe { *argv.offset(idx) };
        (!ptr.is_null()).then(|| {
            unsafe { CStr::from_ptr(ptr) }
                .to_string_lossy()
                .into_owned()
        })
    }

    if argc < 0 || (argc > 0 && argv.is_null()) {
        return Err(SQLITE_ERROR);
    }
    // Extract SQLite schema name from argv[1] and virtual table name from argv[2],
    // preserving any leading/trailing whitespace so distinct quoted names do not collide.
    let db_name = if argc > 1 {
        unsafe { argv_lossy_at(argv, 1) }
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| String::from("main"))
    } else {
        String::from("main")
    };
    let table_name = if argc > 2 {
        unsafe { argv_lossy_at(argv, 2) }.unwrap_or_else(|| String::from("vtab"))
    } else {
        String::from("vtab")
    };
    let mut raw_args = Vec::with_capacity(argc.saturating_sub(3) as usize);
    for i in 3..argc {
        if let Some(arg) = unsafe { argv_lossy_at(argv, i as isize) } {
            raw_args.push(arg);
        }
    }
    parse_vtab_ddl_args(db_name, table_name, &raw_args)
}

fn parse_vtab_ddl_args(
    db_name: String,
    table_name: String,
    raw_args: &[String],
) -> Result<ParsedVtabDdl, c_int> {
    let mut explicit_table_id: Option<u16> = None;
    let mut max_bytes: u64 = 0;
    let mut durability = WriteDurability::BufferedFast;
    let mut on_corrupt: Option<CorruptPolicy> = None;
    let mut inline_max: Option<usize> = None;
    let mut columns_spec = String::from("key TEXT PRIMARY KEY, value BLOB");
    let mut indexes_spec = String::new();

    for raw_arg in raw_args {
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
    let mut has_primary_key = false;
    let mut unsupported: Option<&'static str> = None;
    let mut declared_columns: Vec<String> = Vec::new();
    for col_def in split_top_level_commas(&columns_spec) {
        let trimmed = col_def.trim();
        // The parser does not support comments or per-constraint conflict policies.
        // Reject them rather than silently misinterpret a constraint.
        if has_comment_outside_quotes(trimmed) {
            return Err(SQLITE_ERROR);
        }
        let raw_tokens = column_tokens(trimmed)?;
        if raw_tokens.windows(2).any(|pair| {
            pair[0].eq_ignore_ascii_case("ON") && pair[1].eq_ignore_ascii_case("CONFLICT")
        }) {
            return Err(SQLITE_ERROR);
        }
        if raw_tokens.is_empty() {
            return Err(SQLITE_ERROR);
        }
        if is_table_constraint(&raw_tokens) {
            return Err(SQLITE_ERROR);
        }
        let def = parse_column_def(&raw_tokens)?;
        if def.primary_key {
            if has_primary_key {
                return Err(SQLITE_ERROR);
            }
            has_primary_key = true;
            pk_col_idx = col_names.len();
            pk_rowid_alias = def.rowid_alias;
        }
        if unsupported.is_none() {
            unsupported = def.unsupported;
        }
        declared_columns.push(def.declared);
        col_names.push(def.name);
        col_affinities.push(def.affinity);
        col_collations.push(def.collation);
        col_not_null.push(def.not_null);
        col_unique.push(def.unique);
    }
    if col_names.is_empty() || col_names.len() >= 0x8000 || !has_primary_key {
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
    if !indexes_spec.trim().is_empty() {
        for idx_col in split_top_level_commas(&indexes_spec) {
            if idx_col.trim().is_empty() {
                return Err(SQLITE_ERROR);
            }
            if next_idx_id >= 0x8000 {
                return Err(SQLITE_ERROR);
            }
            let target = unquote_identifier(idx_col.trim());
            let Some(pos) = col_names
                .iter()
                .position(|c| c.eq_ignore_ascii_case(&target))
            else {
                return Err(SQLITE_ERROR);
            };
            if secondary_indexes.iter().any(|&(p, _)| p == pos) {
                return Err(SQLITE_ERROR);
            }
            secondary_indexes.push((pos, next_idx_id));
            next_idx_id += 1;
        }
    }

    Ok(ParsedVtabDdl {
        db_name,
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
        unsupported,
        pk_collation,
        secondary_indexes,
    })
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
        parse(&[&format!("columns='{}'", spec.replace('\'', "''"))])
            .unwrap_or_else(|_| panic!("rejected [{spec}]"))
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
            "k TEXT PRIMARY KEY, v INT UNIQUE ON CONFLICT IGNORE",
            "k TEXT PRIMARY KEY, v INT GENERATED ALWAYS AS (1)",
            "k TEXT PRIMARY KEY, v INT AS (1)",
            "k TEXT PRIMARY KEY, v INT COLLATE UNKNOWN",
            "k TEXT PRIMARY KEY, v INT CHECK",
            "k TEXT PRIMARY KEY, v INT DEFAULT",
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
        ] {
            assert_eq!(cols(spec).pk_rowid_alias, alias, "{spec}");
        }
    }

    #[test]
    fn missing_primary_key_is_rejected() {
        for spec in [
            "k INTEGER, v TEXT",
            "a TEXT UNIQUE, b INT NOT NULL",
            "a BLOB",
        ] {
            assert_eq!(
                parse(&[&format!("columns='{spec}'")]).err(),
                Some(SQLITE_ERROR),
                "{spec}"
            );
        }

        let dir = crate::util::test_util::TempTestDir::new("ddl_missing_pk");
        let fs = std::sync::Arc::new(crate::pal::OsFs::open(&dir.path, true).unwrap());
        let engine = std::sync::Arc::new(std::sync::RwLock::new(
            crate::engine::StorageEngine::open(fs, crate::engine::EngineConfig::default()).unwrap(),
        ));
        let conn = crate::vtab::SqliteConnection::open_in_memory().unwrap();
        conn.register_aydb_module_with_options(engine, false)
            .unwrap();
        assert!(
            conn.execute_batch("CREATE VIRTUAL TABLE no_pk USING aydb(columns='a INT, b TEXT')")
                .is_err()
        );
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

    #[test]
    fn default_clauses_are_skipped_and_flagged() {
        for (spec, reason) in [
            (
                "k INTEGER PRIMARY KEY, x INT DEFAULT 5 UNIQUE",
                DEFAULT_UNSUPPORTED,
            ),
            (
                "k INTEGER PRIMARY KEY, x INT DEFAULT -1",
                DEFAULT_UNSUPPORTED,
            ),
            (
                "k INTEGER PRIMARY KEY, x INT DEFAULT - 1 NOT NULL",
                DEFAULT_UNSUPPORTED,
            ),
            (
                "k INTEGER PRIMARY KEY, x INT DEFAULT (1 + 1)",
                DEFAULT_UNSUPPORTED,
            ),
            (
                "k INTEGER PRIMARY KEY, x TEXT DEFAULT 'a b' COLLATE NOCASE",
                DEFAULT_UNSUPPORTED,
            ),
            (
                "k INTEGER PRIMARY KEY, x INT DEFAULT(3) UNIQUE",
                DEFAULT_UNSUPPORTED,
            ),
            (
                "k INTEGER PRIMARY KEY, x INT DEFAULT CURRENT_TIMESTAMP",
                DEFAULT_UNSUPPORTED,
            ),
            (
                "k INTEGER PRIMARY KEY, x INT CHECK (x > 0)",
                CHECK_UNSUPPORTED,
            ),
        ] {
            let d = cols(spec);
            assert_eq!(d.unsupported, Some(reason), "{spec}");
            assert_eq!(d.col_names, vec!["k", "x"], "{spec}");
        }
        // The columns after the clause are still read.
        let d = cols("k INTEGER PRIMARY KEY, x INT DEFAULT 5 UNIQUE NOT NULL, y TEXT");
        assert_eq!(d.col_unique, vec![false, true, false]);
        assert_eq!(d.col_not_null, vec![false, true, false]);
        assert_eq!(d.col_names.len(), 3);
        assert_eq!(cols("k TEXT PRIMARY KEY, v INT").unsupported, None);
    }

    #[test]
    fn adjacent_parens_and_quotes_split_into_separate_tokens() {
        let d = cols("k TEXT PRIMARY KEY, v VARCHAR(10)UNIQUE");
        assert_eq!(d.col_affinities[1], ColumnAffinity::Text);
        assert_eq!(d.col_unique, vec![false, true]);
        assert_eq!(d.declared_columns, "k TEXT, v VARCHAR(10)");

        let d = cols("k TEXT PRIMARY KEY, v VARCHAR(10)NOT NULL");
        assert_eq!(d.col_not_null, vec![false, true]);
        assert_eq!(d.declared_columns, "k TEXT, v VARCHAR(10) NOT NULL");

        let d = cols("k TEXT PRIMARY KEY, v VARCHAR(10)CHECK(v<>'')");
        assert_eq!(d.unsupported, Some(CHECK_UNSUPPORTED));
        assert_eq!(d.col_affinities[1], ColumnAffinity::Text);

        let d = cols("k TEXT PRIMARY KEY, v INT DEFAULT(0)UNIQUE");
        assert_eq!(d.unsupported, Some(DEFAULT_UNSUPPORTED));
        assert_eq!(d.col_unique, vec![false, true]);

        let d = cols("k TEXT PRIMARY KEY, v TEXT DEFAULT'abc'UNIQUE");
        assert_eq!(d.unsupported, Some(DEFAULT_UNSUPPORTED));
        assert_eq!(d.col_unique, vec![false, true]);

        let d = cols("k TEXT PRIMARY KEY, v INT DEFAULT-1 UNIQUE");
        assert_eq!(d.unsupported, Some(DEFAULT_UNSUPPORTED));
        assert_eq!(d.col_unique, vec![false, true]);

        let d = cols("k TEXT PRIMARY KEY, v REAL DEFAULT 1e-5 UNIQUE");
        assert_eq!(d.unsupported, Some(DEFAULT_UNSUPPORTED));
        assert_eq!(d.col_unique, vec![false, true]);

        let d = cols("k TEXT PRIMARY KEY, v BLOB DEFAULT x'0102'UNIQUE");
        assert_eq!(d.unsupported, Some(DEFAULT_UNSUPPORTED));
        assert_eq!(d.col_unique, vec![false, true]);

        let d = cols("k TEXT PRIMARY KEY, v TEXT CHECK(v <> ')')NOT NULL");
        assert_eq!(d.unsupported, Some(CHECK_UNSUPPORTED));
        assert_eq!(d.col_not_null, vec![false, true]);

        let d = cols("\"k\"INTEGER PRIMARY KEY, v TEXT CONSTRAINT\"u\"UNIQUE");
        assert_eq!(d.col_names, vec!["k", "v"]);
        assert!(d.pk_rowid_alias);
        assert_eq!(d.col_unique, vec![false, true]);
        assert_eq!(d.declared_columns, "\"k\" INTEGER PRIMARY KEY, v TEXT");

        let d = cols("k TEXT PRIMARY KEY, v TEXT REFERENCES p(id)ON DELETE CASCADE");
        assert_eq!(
            d.declared_columns,
            "k TEXT, v TEXT REFERENCES p(id) ON DELETE CASCADE"
        );

        for malformed in [
            "k TEXT PRIMARY KEY, CONSTRAINT \"c\"UNIQUE(k)",
            "k TEXT PRIMARY KEY, v VARCHAR(10",
            "k TEXT PRIMARY KEY, v INT ) UNIQUE",
            "\"k TEXT PRIMARY KEY",
            "k TEXT PRIMARY KEY, v INT CONSTRAINT c",
        ] {
            assert_eq!(
                parse(&[&format!("columns='{malformed}'")]).err(),
                Some(SQLITE_ERROR),
                "{malformed}"
            );
        }
    }

    #[test]
    fn multiple_primary_keys_are_rejected() {
        for spec in [
            "a INTEGER PRIMARY KEY, b TEXT PRIMARY KEY",
            "a TEXT PRIMARY KEY, b TEXT PRIMARY KEY",
            "a TEXT PRIMARY KEY PRIMARY KEY, b INT",
        ] {
            assert_eq!(
                parse(&[&format!("columns='{spec}'")]).err(),
                Some(SQLITE_ERROR),
                "{spec}"
            );
        }
    }

    #[test]
    fn autoincrement_is_flagged_or_rejected() {
        let d = cols("k INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT");
        assert!(d.pk_rowid_alias);
        assert_eq!(d.unsupported, Some(AUTOINCREMENT_UNSUPPORTED));
        assert_eq!(d.declared_columns, "k INTEGER PRIMARY KEY, v TEXT");

        for invalid in [
            "k TEXT PRIMARY KEY AUTOINCREMENT, v TEXT",
            "k INT PRIMARY KEY AUTOINCREMENT, v TEXT",
            "k INTEGER PRIMARY KEY DESC AUTOINCREMENT, v TEXT",
            "k INTEGER(4) PRIMARY KEY AUTOINCREMENT, v TEXT",
        ] {
            assert_eq!(
                parse(&[&format!("columns='{invalid}'")]).err(),
                Some(SQLITE_ERROR),
                "{invalid}"
            );
        }
    }

    #[test]
    fn indexes_reject_duplicates_and_high_bit_overflow() {
        let cols_opt = "columns='id INTEGER PRIMARY KEY, name TEXT, tag TEXT'";
        for dup in ["indexes='name, NAME'", "indexes='name, \"name\"'"] {
            assert_eq!(parse(&[cols_opt, dup]).err(), Some(SQLITE_ERROR), "{dup}");
        }

        // 0x8000 columns (32768) exceeds the non-UNIQUE column limit so `unique_index_id` high
        // bit (`0x8000`) and secondary index IDs (`1..0x8000`) remain disjoint.
        let mut many_cols = String::from("c0 INTEGER PRIMARY KEY");
        for i in 1..0x8000usize {
            many_cols.push_str(&format!(", c{i} INT"));
        }
        assert_eq!(
            parse(&[&format!("columns='{many_cols}'")]).err(),
            Some(SQLITE_ERROR)
        );
    }

    #[test]
    fn option_unwrapping_constraints_collations_and_commas_harden_edge_cases() {
        // Unquoted columns option whose first and last identifiers happen to be quoted must keep
        // all outer quotes intact.
        let d = parse(&[r#"columns="k" INTEGER PRIMARY KEY, "v""#]).unwrap();
        assert_eq!(d.col_names, vec!["k", "v"]);
        assert!(d.pk_rowid_alias);

        // Quoted value with escaped inner quotes still unwraps cleanly.
        let d = parse(&["columns='\"k\" INTEGER PRIMARY KEY, ''v'' TEXT'"]).unwrap();
        assert_eq!(d.col_names, vec!["k", "v"]);

        // Quoted identifiers around a block comment must NOT be unwrapped to hide the comment.
        assert_eq!(
            parse(&["columns='k' /* comment */ 'v'"]).err(),
            Some(SQLITE_ERROR)
        );

        // Consecutive, empty-name, and punctuation-named CONSTRAINT clauses are rejected.
        for bad_constraint in [
            "k TEXT PRIMARY KEY, v INT CONSTRAINT a CONSTRAINT b UNIQUE",
            "k TEXT PRIMARY KEY, v INT CONSTRAINT ( UNIQUE",
            "k TEXT PRIMARY KEY, v INT CONSTRAINT \"\" UNIQUE",
            "k TEXT PRIMARY KEY, v INT CONSTRAINT + UNIQUE",
        ] {
            assert_eq!(
                parse(&[&format!("columns='{bad_constraint}'")]).err(),
                Some(SQLITE_ERROR),
                "{bad_constraint}"
            );
        }

        // COLLATE allows properly quoted standard names without internal whitespace padding.
        let d = cols("k TEXT PRIMARY KEY, a TEXT COLLATE \"NOCASE\", b TEXT COLLATE [RTRIM]");
        assert_eq!(
            d.col_collations,
            vec![
                ColCollation::Binary,
                ColCollation::NoCase,
                ColCollation::RTrim
            ]
        );
        for bad_collate in [
            "k TEXT PRIMARY KEY, a TEXT COLLATE \" NOCASE \"",
            "k TEXT PRIMARY KEY, a TEXT COLLATE (NOCASE)",
            "k TEXT PRIMARY KEY, a TEXT COLLATE \"\"",
        ] {
            assert_eq!(
                parse(&[&format!("columns='{bad_collate}'")]).err(),
                Some(SQLITE_ERROR),
                "{bad_collate}"
            );
        }

        // Empty comma slots in columns and non-empty indexes are rejected, while indexes='' works.
        assert_eq!(
            parse(&["columns='k INT PRIMARY KEY, v TEXT'", "indexes=''"])
                .unwrap()
                .secondary_indexes,
            Vec::new()
        );
        for bad_commas in [
            vec!["columns='k INT PRIMARY KEY,'"],
            vec!["columns=', k INT PRIMARY KEY'"],
            vec!["columns='k INT PRIMARY KEY, , v TEXT'"],
            vec!["columns='k INT PRIMARY KEY, v TEXT'", "indexes='v,'"],
            vec!["columns='k INT PRIMARY KEY, v TEXT'", "indexes=', v'"],
            vec![
                "columns='k INT PRIMARY KEY, v TEXT, w INT'",
                "indexes='v,,w'",
            ],
        ] {
            assert_eq!(
                parse(&bad_commas).err(),
                Some(SQLITE_ERROR),
                "{bad_commas:?}"
            );
        }
    }
}
