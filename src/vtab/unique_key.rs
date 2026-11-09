//! Key forms for the persistent UNIQUE index.
//!
//! A unique entry is stored as `[tid]['i'][iid][Escaped(component)][pk]`. The component is the
//! canonical column key (`canon_unique_key`), or, for keys longer than
//! `UNIQUE_KEY_INLINE_MAX`, a fixed-size digest (`unique_index_component`). No value is ever
//! rejected for its length; a digest match is only a candidate that the caller must confirm
//! against the real row.

use std::io;

use super::extract_column_key_bytes_with_collation;
use super::row_codec::{ColCollation, KEY_TAG_INTEGER, KEY_TAG_NULL};
use crate::index::BloomFilter;

/// Longest canonical key stored verbatim in the index. Longer keys use the digest form.
#[cfg_attr(not(test), allow(dead_code))]
pub(super) const UNIQUE_KEY_INLINE_MAX: usize = 256;

/// First byte of the digest form. Every canonical key starts with a key tag (`0x05..=0x30`), so
/// the two forms can never be confused.
#[cfg_attr(not(test), allow(dead_code))]
pub(super) const UNIQUE_KEY_DIGEST_TAG: u8 = 0xFE;

/// Number of leading key bytes kept in the digest form.
#[cfg_attr(not(test), allow(dead_code))]
pub(super) const UNIQUE_KEY_DIGEST_PREFIX: usize = 64;

/// Size of the digest form: tag, length (`u32` BE), key prefix, 64-bit hash.
#[cfg_attr(not(test), allow(dead_code))]
pub(super) const UNIQUE_KEY_DIGEST_LEN: usize = 1 + 4 + UNIQUE_KEY_DIGEST_PREFIX + 8;

/// Maps a column key to the byte string whose equality is SQLite's equality for that column.
///
/// Column keys encode the same number differently depending on its storage class: an `INTEGER`
/// is 9 bytes (`[0x10, i64]`) while an exact-integer `REAL` is 17 bytes with an all-zero
/// remainder (`[0x10, i64, 0 * 8]`), yet SQLite treats `1` and `1.0` as equal. The canonical form
/// maps the 17-byte shape to the 9-byte one; every other key is returned unchanged.
#[cfg_attr(not(test), allow(dead_code))]
pub(super) fn canon_unique_key(key: &[u8]) -> Vec<u8> {
    if key.len() == 17 && key[0] == KEY_TAG_INTEGER && key[9..17] == [0u8; 8] {
        key[0..9].to_vec()
    } else {
        key.to_vec()
    }
}

/// Returns the component stored in the index for a canonical key: the key itself when it is at
/// most `UNIQUE_KEY_INLINE_MAX` bytes, otherwise
/// `[0xFE][len u32 BE][first 64 bytes][64-bit hash of the whole key]`.
#[cfg_attr(not(test), allow(dead_code))]
pub(super) fn unique_index_component(canon_key: &[u8]) -> Vec<u8> {
    if canon_key.len() <= UNIQUE_KEY_INLINE_MAX {
        return canon_key.to_vec();
    }
    let mut out = Vec::with_capacity(UNIQUE_KEY_DIGEST_LEN);
    out.push(UNIQUE_KEY_DIGEST_TAG);
    out.extend_from_slice(&(canon_key.len() as u32).to_be_bytes());
    out.extend_from_slice(&canon_key[..UNIQUE_KEY_DIGEST_PREFIX]);
    out.extend_from_slice(&BloomFilter::hash64(canon_key).to_be_bytes());
    out
}

/// True when `component` is the digest form, whose equality is only a candidate match.
#[cfg_attr(not(test), allow(dead_code))]
pub(super) fn is_digest_component(component: &[u8]) -> bool {
    component.first() == Some(&UNIQUE_KEY_DIGEST_TAG)
}

/// Computes the canonical key of column `col_idx` in the finished row bytes, or `None` for NULL
/// (a NULL never conflicts and has no index entry). INSERT, UPDATE and backfill all derive their
/// entries from the row bytes through this function.
#[cfg_attr(not(test), allow(dead_code))]
pub(super) fn unique_key_of_row(
    row: &[u8],
    col_idx: usize,
    collation: ColCollation,
) -> io::Result<Option<Vec<u8>>> {
    let key = extract_column_key_bytes_with_collation(row, col_idx, collation)?;
    if key.first() == Some(&KEY_TAG_NULL) {
        return Ok(None);
    }
    Ok(Some(canon_unique_key(&key)))
}

#[cfg(test)]
mod tests {
    use super::super::ffi::*;
    use super::super::row_codec::{
        ColumnAffinity, SqlValue, encode_row, encode_sqlite_values_direct,
    };
    use super::super::{build_numeric_alt_key, encode_f64_unified_key, encode_i64_tagged_key};
    use super::*;
    use core::ffi::{c_char, c_int, c_void};
    use std::ffi::CString;

    #[test]
    fn canon_maps_exact_integer_floats_to_the_integer_key() {
        for v in [0i64, 1, -1, 1 << 53, i64::MIN, i64::MAX] {
            let int_key = encode_i64_tagged_key(v).to_vec();
            assert_eq!(canon_unique_key(&int_key), int_key);
        }
        for f in [
            0.0f64,
            -0.0,
            1.0,
            -1.0,
            9007199254740992.0,
            -9223372036854775808.0,
        ] {
            let key = encode_f64_unified_key(f);
            assert_eq!(key.len(), 17);
            let canon = canon_unique_key(&key);
            assert_eq!(canon.len(), 9);
            assert_eq!(canon, encode_i64_tagged_key(f as i64).to_vec());
        }
    }

    #[test]
    fn canon_keeps_every_other_key_unchanged() {
        let others: Vec<Vec<u8>> = vec![
            vec![KEY_TAG_NULL],
            encode_f64_unified_key(1.5),
            encode_f64_unified_key(-0.25),
            encode_f64_unified_key(1e300),
            encode_f64_unified_key(f64::INFINITY),
            encode_f64_unified_key(f64::NEG_INFINITY),
            encode_f64_unified_key(9223372036854775808.0),
            [&[0x20u8][..], b"a"].concat(),
            [&[0x30u8][..], &[0u8; 16]].concat(),
            // A 17-byte blob key must not be mistaken for the integer shape.
            [&[0x30u8][..], &[0u8; 16]].concat(),
            Vec::new(),
        ];
        for key in others {
            assert_eq!(canon_unique_key(&key), key);
        }
        // Integer tag with a non-zero remainder is a genuine fraction.
        let mut fraction = encode_i64_tagged_key(3).to_vec();
        fraction.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 1]);
        assert_eq!(canon_unique_key(&fraction), fraction);
    }

    #[test]
    fn short_keys_are_stored_verbatim_up_to_the_threshold() {
        let at_limit = [&[0x20u8][..], &vec![b'x'; UNIQUE_KEY_INLINE_MAX - 1]].concat();
        assert_eq!(at_limit.len(), UNIQUE_KEY_INLINE_MAX);
        assert_eq!(unique_index_component(&at_limit), at_limit);
        assert!(!is_digest_component(&at_limit));
        assert!(!is_digest_component(&[]));
    }

    #[test]
    fn long_keys_use_the_fixed_size_digest_form() {
        let key = [&[0x20u8][..], &vec![b'x'; UNIQUE_KEY_INLINE_MAX]].concat();
        assert_eq!(key.len(), UNIQUE_KEY_INLINE_MAX + 1);
        let component = unique_index_component(&key);
        assert_eq!(component.len(), UNIQUE_KEY_DIGEST_LEN);
        assert!(is_digest_component(&component));
        assert_eq!(component[0], 0xFE);
        assert_eq!(&component[1..5], &(key.len() as u32).to_be_bytes());
        assert_eq!(&component[5..69], &key[..64]);
        assert_eq!(&component[69..], &BloomFilter::hash64(&key).to_be_bytes());
        assert_eq!(unique_index_component(&key), component);

        let huge = [&[0x30u8][..], &vec![7u8; 1 << 20]].concat();
        let huge_component = unique_index_component(&huge);
        assert_eq!(huge_component.len(), UNIQUE_KEY_DIGEST_LEN);
        assert_eq!(&huge_component[1..5], &(huge.len() as u32).to_be_bytes());
    }

    #[test]
    fn digest_distinguishes_keys_that_differ_after_the_prefix() {
        let base = [&[0x20u8][..], &vec![b'x'; 400]].concat();
        let mut seen = std::collections::HashSet::new();
        seen.insert(unique_index_component(&base));
        for pos in [64usize, 65, 100, 255, 256, 399, 400] {
            let mut other = base.clone();
            other[pos] ^= 1;
            assert!(
                seen.insert(unique_index_component(&other)),
                "flipping byte {pos} left the component unchanged"
            );
        }
        // A different length with identical prefix and tail also differs.
        let longer = [&base[..], b"x"].concat();
        assert!(seen.insert(unique_index_component(&longer)));
    }

    #[test]
    fn digest_form_never_collides_with_an_inline_key() {
        // Every key tag the column encoders emit is below the digest tag.
        for tag in [KEY_TAG_NULL, 0x0E, KEY_TAG_INTEGER, 0x12, 0x20, 0x30] {
            assert!(tag < UNIQUE_KEY_DIGEST_TAG);
        }
    }

    #[test]
    fn long_form_is_computed_from_the_canonical_key() {
        // Numeric keys are far shorter than the threshold, so canonicalization is the only step.
        let float_key = encode_f64_unified_key(42.0);
        let int_key = encode_i64_tagged_key(42).to_vec();
        assert_eq!(
            unique_index_component(&canon_unique_key(&float_key)),
            unique_index_component(&canon_unique_key(&int_key))
        );
    }

    #[test]
    fn row_key_is_none_for_null_and_canonical_otherwise() {
        let row = encode_row(&[
            SqlValue::Null,
            SqlValue::Float(7.0),
            SqlValue::Integer(7),
            SqlValue::Text("Ab ".into()),
        ]);
        assert_eq!(
            unique_key_of_row(&row, 0, ColCollation::Binary).unwrap(),
            None
        );
        // Columns past the end of the row read as NULL.
        assert_eq!(
            unique_key_of_row(&row, 9, ColCollation::Binary).unwrap(),
            None
        );
        let f = unique_key_of_row(&row, 1, ColCollation::Binary).unwrap();
        let i = unique_key_of_row(&row, 2, ColCollation::Binary).unwrap();
        assert_eq!(f, i);
        assert_eq!(f.unwrap(), encode_i64_tagged_key(7).to_vec());
        assert_eq!(
            unique_key_of_row(&row, 3, ColCollation::NoCase)
                .unwrap()
                .unwrap(),
            b"\x20ab ".to_vec()
        );
        assert_eq!(
            unique_key_of_row(&row, 3, ColCollation::RTrim)
                .unwrap()
                .unwrap(),
            b"\x20Ab".to_vec()
        );
    }

    // ---------------------------------------------------------------------------------------
    // Differential test against SQLite
    // ---------------------------------------------------------------------------------------

    /// SplitMix64, as in the crash tests.
    struct Rng(u64);

    impl Rng {
        fn new(seed: u64) -> Self {
            Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xD1B5_4A32_D192_ED03)
        }

        fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }

        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    /// Minimal raw SQLite handle so the test can obtain `sqlite3_value` pointers.
    struct Db {
        db: *mut sqlite3,
    }

    impl Db {
        fn open() -> Self {
            let mut db = core::ptr::null_mut();
            let name = CString::new(":memory:").unwrap();
            assert_eq!(unsafe { sqlite3_open(name.as_ptr(), &mut db) }, SQLITE_OK);
            Self { db }
        }

        fn exec(&self, sql: &str) -> bool {
            let sql = CString::new(sql).unwrap();
            unsafe {
                sqlite3_exec(
                    self.db,
                    sql.as_ptr(),
                    None,
                    core::ptr::null_mut(),
                    core::ptr::null_mut(),
                ) == SQLITE_OK
            }
        }

        fn prepare(&self, sql: &str) -> *mut sqlite3_stmt {
            let mut stmt = core::ptr::null_mut();
            let rc = unsafe {
                sqlite3_prepare_v2(
                    self.db,
                    sql.as_ptr() as *const c_char,
                    sql.len() as c_int,
                    &mut stmt,
                    core::ptr::null_mut(),
                )
            };
            assert_eq!(rc, SQLITE_OK, "prepare failed: {sql}");
            stmt
        }
    }

    impl Drop for Db {
        fn drop(&mut self) {
            unsafe {
                sqlite3_close(self.db);
            }
        }
    }

    fn bind(stmt: *mut sqlite3_stmt, value: &SqlValue) {
        unsafe {
            sqlite3_reset(stmt);
            sqlite3_clear_bindings(stmt);
            let rc = match value {
                SqlValue::Null => sqlite3_bind_null(stmt, 1),
                SqlValue::Integer(v) => sqlite3_bind_int64(stmt, 1, *v),
                SqlValue::Float(v) => sqlite3_bind_double(stmt, 1, *v),
                SqlValue::Text(s) => sqlite3_bind_text(
                    stmt,
                    1,
                    s.as_ptr() as *const c_char,
                    s.len() as c_int,
                    sqlite_transient(),
                ),
                SqlValue::Blob(b) => sqlite3_bind_blob(
                    stmt,
                    1,
                    b.as_ptr() as *const c_void,
                    b.len() as c_int,
                    sqlite_transient(),
                ),
            };
            assert_eq!(rc, SQLITE_OK);
        }
    }

    /// Canonical unique key of `value` as the virtual table would store it: the value goes through
    /// the real row encoder (affinity applied), then through `unique_key_of_row`.
    fn aydb_key(
        select: *mut sqlite3_stmt,
        value: &SqlValue,
        affinity: ColumnAffinity,
        collation: ColCollation,
    ) -> Vec<u8> {
        bind(select, value);
        unsafe {
            assert_eq!(sqlite3_step(select), SQLITE_ROW);
            let mut values = [sqlite3_column_value(select, 0)];
            let (_pk, row, _sec) = encode_sqlite_values_direct(
                values.as_mut_ptr(),
                1,
                0,
                None,
                &[affinity],
                &[collation],
                &[],
            );
            sqlite3_reset(select);
            unique_key_of_row(&row, 0, collation)
                .unwrap()
                .expect("pool contains no NULL")
        }
    }

    /// SQLite's verdict: two values are equal for a UNIQUE column when the second INSERT fails.
    fn sqlite_equal(insert: *mut sqlite3_stmt, db: &Db, a: &SqlValue, b: &SqlValue) -> bool {
        assert!(db.exec("BEGIN"));
        bind(insert, a);
        assert_eq!(unsafe { sqlite3_step(insert) }, SQLITE_DONE);
        bind(insert, b);
        let rc = unsafe { sqlite3_step(insert) };
        unsafe {
            sqlite3_reset(insert);
        }
        assert!(db.exec("ROLLBACK"));
        match rc {
            SQLITE_DONE => false,
            SQLITE_CONSTRAINT => true,
            other => panic!("unexpected step result {other}"),
        }
    }

    /// The three-way comparison `find_unique_conflicts` uses today.
    fn legacy_conflict(existing: &[u8], key: &[u8]) -> bool {
        existing == key
            || build_numeric_alt_key(existing).as_deref() == Some(key)
            || build_numeric_alt_key(key).as_deref() == Some(existing)
    }

    fn text(s: &str) -> SqlValue {
        SqlValue::Text(s.to_string())
    }

    fn value_pool() -> Vec<SqlValue> {
        let long_a = "a".repeat(300);
        let long_upper = "A".repeat(300);
        let long_padded = format!("{}{}", "a".repeat(300), "   ");
        let long_other_tail = format!("{}b", "a".repeat(299));
        vec![
            SqlValue::Integer(0),
            SqlValue::Integer(1),
            SqlValue::Integer(-1),
            SqlValue::Integer(1 << 53),
            SqlValue::Integer((1 << 53) + 1),
            SqlValue::Integer(i64::MIN),
            SqlValue::Integer(i64::MAX),
            SqlValue::Float(0.0),
            SqlValue::Float(-0.0),
            SqlValue::Float(1.0),
            SqlValue::Float(-1.0),
            SqlValue::Float(1.5),
            SqlValue::Float(9007199254740992.0),
            SqlValue::Float(9007199254740994.0),
            SqlValue::Float(-9223372036854775808.0),
            SqlValue::Float(9223372036854775808.0),
            SqlValue::Float(1e300),
            SqlValue::Float(-1e300),
            SqlValue::Float(f64::INFINITY),
            SqlValue::Float(f64::NEG_INFINITY),
            SqlValue::Float(10.0),
            text("a"),
            text("A"),
            text("a "),
            text("a  "),
            text("10"),
            text("10.0"),
            text(" 10"),
            text("1e1"),
            text("1"),
            text("1.0"),
            text("0"),
            text("-0.0"),
            text(""),
            text(" "),
            text("abc"),
            text("9223372036854775807"),
            text("9223372036854775808"),
            text("9007199254740993"),
            text(&long_a),
            text(&long_upper),
            text(&long_padded),
            text(&long_other_tail),
            SqlValue::Blob(Vec::new()),
            SqlValue::Blob(b"a".to_vec()),
            SqlValue::Blob(b"A".to_vec()),
            SqlValue::Blob(b"a ".to_vec()),
            SqlValue::Blob(vec![0x31]),
            SqlValue::Blob(vec![0x20; 300]),
            SqlValue::Blob(vec![0; 300]),
            SqlValue::Blob([vec![0x61; 299], vec![0x62]].concat()),
        ]
    }

    fn random_value(rng: &mut Rng) -> SqlValue {
        const EDGES: [i64; 9] = [
            0,
            1,
            -1,
            1 << 53,
            (1 << 53) + 1,
            i64::MIN,
            i64::MAX,
            255,
            -256,
        ];
        let edge = EDGES[rng.below(EDGES.len() as u64) as usize];
        let delta = rng.below(5) as i64 - 2;
        match rng.below(8) {
            0 => SqlValue::Integer(edge.wrapping_add(delta)),
            1 => SqlValue::Float(edge as f64 + delta as f64),
            2 => SqlValue::Float((rng.below(2000) as f64 - 1000.0) / 8.0),
            3 | 4 => {
                const ALPHABET: &[u8] = b"aAbB 01.e-+9";
                let len = rng.below(8) as usize;
                let s: String = (0..len)
                    .map(|_| ALPHABET[rng.below(ALPHABET.len() as u64) as usize] as char)
                    .collect();
                SqlValue::Text(s)
            }
            5 => {
                // Numeric-looking text, including forms SQLite normalizes under numeric affinity.
                let n = edge.wrapping_add(delta);
                match rng.below(4) {
                    0 => SqlValue::Text(n.to_string()),
                    1 => SqlValue::Text(format!("{n}.0")),
                    2 => SqlValue::Text(format!(" {n} ")),
                    _ => SqlValue::Text(format!("{}e0", n)),
                }
            }
            6 => {
                let len = rng.below(6) as usize;
                SqlValue::Blob((0..len).map(|_| b"aA 1"[rng.below(4) as usize]).collect())
            }
            _ => {
                let len = 250 + rng.below(12) as usize;
                let mut s = "a".repeat(len);
                if rng.below(2) == 0 {
                    s.push_str(&" ".repeat(rng.below(4) as usize));
                }
                SqlValue::Text(s)
            }
        }
    }

    const AFFINITIES: [(&str, ColumnAffinity); 5] = [
        ("INTEGER", ColumnAffinity::Integer),
        ("REAL", ColumnAffinity::Real),
        ("TEXT", ColumnAffinity::Text),
        ("NUMERIC", ColumnAffinity::Numeric),
        ("BLOB", ColumnAffinity::Blob),
    ];
    const COLLATIONS: [(&str, ColCollation); 3] = [
        ("BINARY", ColCollation::Binary),
        ("NOCASE", ColCollation::NoCase),
        ("RTRIM", ColCollation::RTrim),
    ];

    /// Compares `canon(a) == canon(b)` with SQLite over all pairs of `values`, for every
    /// affinity and collation. Returns the number of pairs checked.
    fn check_against_sqlite(values: &[SqlValue], pairs: &[(usize, usize)]) -> usize {
        let mut checked = 0;
        let mut failures: Vec<String> = Vec::new();
        for (aff_name, affinity) in AFFINITIES {
            assert_eq!(ColumnAffinity::from_type_str(aff_name), affinity);
            for (coll_name, collation) in COLLATIONS {
                let db = Db::open();
                assert!(db.exec(&format!(
                    "CREATE TABLE n(c {aff_name} COLLATE {coll_name} UNIQUE)"
                )));
                let insert = db.prepare("INSERT INTO n VALUES(?1)");
                let select = db.prepare("SELECT ?1");
                let keys: Vec<Vec<u8>> = values
                    .iter()
                    .map(|v| aydb_key(select, v, affinity, collation))
                    .collect();
                for &(i, j) in pairs {
                    let expected = sqlite_equal(insert, &db, &values[i], &values[j]);
                    let actual = keys[i] == keys[j];
                    let legacy = legacy_conflict(&keys[i], &keys[j]);
                    // The legacy predicate runs on non-canonical keys in production, so compare
                    // it on the raw keys too.
                    let raw_i = aydb_raw_key(select, &values[i], affinity, collation);
                    let raw_j = aydb_raw_key(select, &values[j], affinity, collation);
                    let legacy_raw = legacy_conflict(&raw_i, &raw_j);
                    assert_eq!(legacy, actual);
                    assert_eq!(legacy_raw, actual);
                    checked += 1;
                    if expected != actual && failures.len() < 20 {
                        failures.push(format!(
                            "{aff_name}/{coll_name}: {:?} vs {:?}: sqlite equal={expected}, canon equal={actual}",
                            values[i], values[j]
                        ));
                    }
                }
                unsafe {
                    sqlite3_finalize(insert);
                    sqlite3_finalize(select);
                }
            }
        }
        assert!(
            failures.is_empty(),
            "disagreements:\n{}",
            failures.join("\n")
        );
        checked
    }

    fn aydb_raw_key(
        select: *mut sqlite3_stmt,
        value: &SqlValue,
        affinity: ColumnAffinity,
        collation: ColCollation,
    ) -> Vec<u8> {
        bind(select, value);
        unsafe {
            assert_eq!(sqlite3_step(select), SQLITE_ROW);
            let mut values = [sqlite3_column_value(select, 0)];
            let (_pk, row, _sec) = encode_sqlite_values_direct(
                values.as_mut_ptr(),
                1,
                0,
                None,
                &[affinity],
                &[collation],
                &[],
            );
            sqlite3_reset(select);
            extract_column_key_bytes_with_collation(&row, 0, collation).unwrap()
        }
    }

    #[test]
    fn canonical_key_equality_matches_sqlite_for_the_value_pool() {
        let values = value_pool();
        let mut pairs = Vec::new();
        for i in 0..values.len() {
            for j in 0..values.len() {
                pairs.push((i, j));
            }
        }
        let checked = check_against_sqlite(&values, &pairs);
        assert_eq!(checked, values.len() * values.len() * 15);
    }

    #[test]
    fn canonical_key_equality_matches_sqlite_for_seeded_random_values() {
        for seed in 0..4u64 {
            let mut rng = Rng::new(seed);
            let values: Vec<SqlValue> = (0..60).map(|_| random_value(&mut rng)).collect();
            let mut pairs = Vec::new();
            for i in 0..values.len() {
                for j in 0..values.len() {
                    pairs.push((i, j));
                }
            }
            check_against_sqlite(&values, &pairs);
        }
    }

    #[test]
    fn digest_equality_agrees_with_key_equality_for_the_value_pool() {
        // The digest form may collide (the caller confirms), but it must never separate equal keys.
        let values = value_pool();
        for (_, affinity) in AFFINITIES {
            for (_, collation) in COLLATIONS {
                let db = Db::open();
                let select = db.prepare("SELECT ?1");
                let keys: Vec<Vec<u8>> = values
                    .iter()
                    .map(|v| aydb_key(select, v, affinity, collation))
                    .collect();
                for a in &keys {
                    for b in &keys {
                        if a == b {
                            assert_eq!(unique_index_component(a), unique_index_component(b));
                        } else if a.len() > UNIQUE_KEY_INLINE_MAX || b.len() > UNIQUE_KEY_INLINE_MAX
                        {
                            // Different keys of which one is long must not be stored identically
                            // unless the hash really collides, which the pool must not hit.
                            assert_ne!(unique_index_component(a), unique_index_component(b));
                        }
                    }
                }
                unsafe {
                    sqlite3_finalize(select);
                }
            }
        }
    }
}
