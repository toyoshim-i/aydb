//! SQLite virtual table query planner (`xBestIndex`) and `idxNum` plan encoding.

use core::ffi::c_int;
use std::panic::{AssertUnwindSafe, catch_unwind};

use crate::vtab::AydbVTab;
use crate::vtab::ffi::{
    SQLITE_ERROR, SQLITE_INDEX_CONSTRAINT_EQ, SQLITE_INDEX_CONSTRAINT_GE,
    SQLITE_INDEX_CONSTRAINT_GT, SQLITE_INDEX_CONSTRAINT_LE, SQLITE_INDEX_CONSTRAINT_LT,
    SQLITE_INDEX_SCAN_UNIQUE, SQLITE_OK, sqlite3_index_info, sqlite3_vtab, sqlite3_vtab_collation,
};
use crate::vtab::row_codec::{ColCollation, ColumnAffinity};

pub(in crate::vtab) const PLAN_FULL_SCAN: c_int = 0;
const PLAN_PK_POINT: c_int = 1;
const PLAN_SECONDARY_EQ: c_int = 2;
pub(in crate::vtab) const PLAN_PK_RANGE: c_int = 3;
pub(in crate::vtab) const PLAN_SECONDARY_RANGE: c_int = 4;
const PLAN_UNIQUE_EQ: c_int = 5;

const POINT_FLAG_BY_ROWID: c_int = 0x01;
/// Point lookup only needs existence + primary key (no value read), so a corrupt out-of-line
/// blob never fails `DELETE ... WHERE pk = ?` / `SELECT pk ... WHERE pk = ?`.
pub(in crate::vtab) const POINT_FLAG_KEYS_ONLY: c_int = 0x02;

const RANGE_FLAG_LOWER_INCLUSIVE: c_int = 0x01;
const RANGE_FLAG_LOWER_EXCLUSIVE: c_int = 0x02;
const RANGE_FLAG_UPPER_INCLUSIVE: c_int = 0x04;
const RANGE_FLAG_UPPER_EXCLUSIVE: c_int = 0x08;
pub(in crate::vtab) const RANGE_FLAG_KEYS_ONLY: c_int = 0x20;
const RANGE_FLAG_REVERSE: c_int = 0x40;

#[cfg(test)]
thread_local! {
    /// Test observability: last `(idxNum, orderByConsumed, argv_indexes)` chosen by `vtab_best_index`.
    pub(in crate::vtab) static LAST_BEST_INDEX_PLAN: std::cell::RefCell<Option<(c_int, c_int, Vec<c_int>)>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
fn record_best_index_plan(
    info: &sqlite3_index_info,
    usages: &[crate::vtab::ffi::sqlite3_index_constraint_usage],
) {
    let args = usages.iter().map(|u| u.argvIndex).collect();
    LAST_BEST_INDEX_PLAN.with(|cell| {
        *cell.borrow_mut() = Some((info.idxNum, info.orderByConsumed, args));
    });
}

/// Lower/upper range bounds and scan modifiers shared between PK and secondary range plans.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::vtab) struct RangePlan {
    pub(in crate::vtab) lower_inclusive: Option<bool>,
    pub(in crate::vtab) upper_inclusive: Option<bool>,
    pub(in crate::vtab) keys_only: bool,
    pub(in crate::vtab) reverse: bool,
}

/// Structured representation of the query plan bit-packed into `sqlite3_index_info::idxNum`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::vtab) enum DecodedPlan {
    FullScan { keys_only: bool, reverse: bool },
    PkPoint { by_rowid: bool, keys_only: bool },
    SecondaryEq { index_id: u16, keys_only: bool },
    UniqueEq { col_idx: u16, keys_only: bool },
    PkRange(RangePlan),
    SecondaryRange { index_id: u16, range: RangePlan },
}

#[inline]
const fn range_flags_bits(range: RangePlan) -> c_int {
    let lo = match range.lower_inclusive {
        Some(true) => RANGE_FLAG_LOWER_INCLUSIVE,
        Some(false) => RANGE_FLAG_LOWER_EXCLUSIVE,
        None => 0,
    };
    let hi = match range.upper_inclusive {
        Some(true) => RANGE_FLAG_UPPER_INCLUSIVE,
        Some(false) => RANGE_FLAG_UPPER_EXCLUSIVE,
        None => 0,
    };
    let ko = if range.keys_only {
        RANGE_FLAG_KEYS_ONLY
    } else {
        0
    };
    let rev = if range.reverse { RANGE_FLAG_REVERSE } else { 0 };
    lo | hi | ko | rev
}

#[inline]
const fn decode_range_flags(flags: c_int) -> RangePlan {
    let lower_inclusive = if (flags & RANGE_FLAG_LOWER_INCLUSIVE) != 0 {
        Some(true)
    } else if (flags & RANGE_FLAG_LOWER_EXCLUSIVE) != 0 {
        Some(false)
    } else {
        None
    };
    let upper_inclusive = if (flags & RANGE_FLAG_UPPER_INCLUSIVE) != 0 {
        Some(true)
    } else if (flags & RANGE_FLAG_UPPER_EXCLUSIVE) != 0 {
        Some(false)
    } else {
        None
    };
    RangePlan {
        lower_inclusive,
        upper_inclusive,
        keys_only: (flags & RANGE_FLAG_KEYS_ONLY) != 0,
        reverse: (flags & RANGE_FLAG_REVERSE) != 0,
    }
}

impl DecodedPlan {
    #[inline]
    pub(in crate::vtab) const fn encode(self) -> c_int {
        match self {
            Self::FullScan { keys_only, reverse } => {
                let flags = range_flags_bits(RangePlan {
                    lower_inclusive: None,
                    upper_inclusive: None,
                    keys_only,
                    reverse,
                });
                PLAN_FULL_SCAN | (flags << 8)
            }
            Self::PkPoint {
                by_rowid,
                keys_only,
            } => {
                let by = if by_rowid { POINT_FLAG_BY_ROWID } else { 0 };
                let ko = if keys_only { POINT_FLAG_KEYS_ONLY } else { 0 };
                PLAN_PK_POINT | ((by | ko) << 8)
            }
            Self::SecondaryEq {
                index_id,
                keys_only,
            } => {
                let eq_flags = if keys_only { RANGE_FLAG_KEYS_ONLY } else { 0 };
                PLAN_SECONDARY_EQ
                    | (((index_id as c_int) & 0xFFFF) << 8)
                    | ((eq_flags & 0x7F) << 24)
            }
            Self::UniqueEq { col_idx, keys_only } => {
                let eq_flags = if keys_only { RANGE_FLAG_KEYS_ONLY } else { 0 };
                PLAN_UNIQUE_EQ | (((col_idx as c_int) & 0xFFFF) << 8) | ((eq_flags & 0x7F) << 24)
            }
            Self::PkRange(range) => PLAN_PK_RANGE | (range_flags_bits(range) << 8),
            Self::SecondaryRange { index_id, range } => {
                let flags = range_flags_bits(RangePlan {
                    reverse: false,
                    ..range
                });
                PLAN_SECONDARY_RANGE
                    | (((index_id as c_int) & 0xFFFF) << 8)
                    | ((flags & 0x7F) << 24)
            }
        }
    }

    #[inline]
    pub(in crate::vtab) const fn decode(idx_num: c_int) -> Self {
        match idx_num & 0xFF {
            PLAN_PK_POINT => {
                let pt_flags = (idx_num >> 8) & 0xFF;
                Self::PkPoint {
                    by_rowid: (pt_flags & POINT_FLAG_BY_ROWID) != 0,
                    keys_only: (pt_flags & POINT_FLAG_KEYS_ONLY) != 0,
                }
            }
            PLAN_SECONDARY_EQ => {
                let index_id = ((idx_num >> 8) & 0xFFFF) as u16;
                let flags = (idx_num >> 24) & 0x7F;
                Self::SecondaryEq {
                    index_id,
                    keys_only: (flags & RANGE_FLAG_KEYS_ONLY) != 0,
                }
            }
            PLAN_UNIQUE_EQ => {
                let col_idx = ((idx_num >> 8) & 0xFFFF) as u16;
                let flags = (idx_num >> 24) & 0x7F;
                Self::UniqueEq {
                    col_idx,
                    keys_only: (flags & RANGE_FLAG_KEYS_ONLY) != 0,
                }
            }
            PLAN_PK_RANGE => Self::PkRange(decode_range_flags(idx_num >> 8)),
            PLAN_SECONDARY_RANGE => {
                let index_id = ((idx_num >> 8) & 0xFFFF) as u16;
                Self::SecondaryRange {
                    index_id,
                    range: decode_range_flags((idx_num >> 24) & 0x7F),
                }
            }
            _ => {
                let flags = idx_num >> 8;
                Self::FullScan {
                    keys_only: (flags & RANGE_FLAG_KEYS_ONLY) != 0,
                    reverse: (flags & RANGE_FLAG_REVERSE) != 0,
                }
            }
        }
    }
}

unsafe fn constraint_matches_col_collation(
    p_info: *mut sqlite3_index_info,
    constraint_idx: usize,
    col_collation: ColCollation,
) -> bool {
    let ptr = unsafe { sqlite3_vtab_collation(p_info, constraint_idx as c_int) };
    if ptr.is_null() {
        return true;
    }
    let Ok(cstr) = (unsafe { core::ffi::CStr::from_ptr(ptr) }).to_str() else {
        return false;
    };
    ColCollation::from_name(cstr) == Some(col_collation)
}

#[inline]
unsafe fn order_by_slice<'a>(
    n_order_by: c_int,
    a_order_by: *const crate::vtab::ffi::sqlite3_index_orderby,
) -> &'a [crate::vtab::ffi::sqlite3_index_orderby] {
    let n = usize::try_from(n_order_by).unwrap_or(0);
    if n == 0 || a_order_by.is_null() {
        &[]
    } else {
        unsafe { core::slice::from_raw_parts(a_order_by, n) }
    }
}

/// Returns `Some(reverse)` if a primary-key scan satisfies `order_by` directly. Because primary
/// keys are unique under their column collation, any trailing tie-breaker columns after `pk`
/// never re-order equal rows.
#[inline]
fn pk_order_by(
    order_by: &[crate::vtab::ffi::sqlite3_index_orderby],
    pk_col_idx: usize,
) -> Option<bool> {
    // Integer affinity also accepts REAL/TEXT/BLOB keys with synthetic rowids; only `pk_col_idx`
    // order (never `iColumn == -1`) is guaranteed to match the LSM key sort order.
    let first = order_by.first()?;
    if first.iColumn == pk_col_idx as i32 {
        Some(first.desc != 0)
    } else {
        None
    }
}

/// Returns `true` when an equality probe on `col_idx` satisfies `order_by`.
///
/// An equality scan yields matching rows in ascending `pk` order (including any cross-affinity
/// fallback via `start_pk_full_scan`), and for numeric/real/integer affinities every match has an
/// equal key so `ORDER BY col_idx[, pk ASC]` is also satisfied.
#[inline]
fn eq_index_consumes_order_by(
    order_by: &[crate::vtab::ffi::sqlite3_index_orderby],
    col_idx: usize,
    col_aff: ColumnAffinity,
    pk_col_idx: usize,
) -> bool {
    let Some(first) = order_by.first() else {
        return false;
    };
    if first.iColumn == pk_col_idx as i32 && first.desc == 0 {
        return true;
    }
    if matches!(col_aff, ColumnAffinity::Text | ColumnAffinity::Blob) {
        return false;
    }
    if first.iColumn == col_idx as i32 {
        return order_by.len() == 1
            || (order_by[1].iColumn == pk_col_idx as i32 && order_by[1].desc == 0);
    }
    false
}

/// Returns `true` when an ascending secondary-index range scan (`Escaped(col_val) | pk`)
/// satisfies `ORDER BY sec_col_idx ASC` or `ORDER BY sec_col_idx ASC, pk ASC`.
#[inline]
fn sec_range_consumes_order_by(
    order_by: &[crate::vtab::ffi::sqlite3_index_orderby],
    sec_col_idx: usize,
    pk_col_idx: usize,
) -> bool {
    let Some(first) = order_by.first() else {
        return false;
    };
    if first.iColumn != sec_col_idx as i32 || first.desc != 0 {
        return false;
    }
    order_by.len() == 1 || (order_by[1].iColumn == pk_col_idx as i32 && order_by[1].desc == 0)
}

struct SelectedBestIndexPlan {
    idx_num: c_int,
    idx_flags: Option<c_int>,
    order_by_consumed: bool,
    estimated_cost: f64,
    estimated_rows: i64,
    used_constraints: [usize; 2],
    num_used: usize,
}

impl AydbVTab {
    pub(in crate::vtab) fn is_unique_index_ready_for_plan(&self) -> bool {
        let want = self.unique_def.marker_bytes();
        let Ok(eng) = self.engine.read() else {
            return false;
        };
        if eng.conn_has_open_transaction(self.conn_id()) && eng.unique_marker_modified_in_tx() {
            return eng
                .unique_marker_view(crate::engine::ReadView::Owner, self.table_id)
                .ok()
                .flatten()
                .is_some_and(|found| found.as_slice() == want);
        }
        let marker_gen = eng.unique_marker_gen();
        if self.unique_ready.is_ready_for(marker_gen) {
            return true;
        }
        let marked = eng
            .unique_marker_view(crate::engine::ReadView::Committed, self.table_id)
            .ok()
            .flatten()
            .is_some_and(|found| found.as_slice() == want);
        if marked && marker_gen != 0 {
            self.unique_ready
                .latch_if_committed(marker_gen, !eng.unique_marker_modified_in_tx());
            true
        } else {
            self.unique_ready.invalidate();
            false
        }
    }
}

pub(in crate::vtab) unsafe extern "system" fn vtab_best_index(
    p_vtab: *mut sqlite3_vtab,
    p_info: *mut sqlite3_index_info,
) -> c_int {
    catch_unwind(AssertUnwindSafe(|| {
        let Some(vtab) = (unsafe { AydbVTab::from_raw(p_vtab) }) else {
            return SQLITE_ERROR;
        };
        if p_info.is_null() {
            return SQLITE_ERROR;
        }
        // Read input fields via raw pointer without holding a live `&mut sqlite3_index_info`
        // across FFI calls to `sqlite3_vtab_collation(p_info, ...)`, which inspects `p_info`
        // and SQLite's trailing `HiddenIndexInfo`.
        let (n_constraint, a_constraint, a_constraint_usage, n_order_by, a_order_by, col_used) = unsafe {
            (
                (*p_info).nConstraint,
                (*p_info).aConstraint,
                (*p_info).aConstraintUsage,
                (*p_info).nOrderBy,
                (*p_info).aOrderBy,
                (*p_info).colUsed,
            )
        };

        let n = usize::try_from(n_constraint).unwrap_or(0);
        let constraints: &[crate::vtab::ffi::sqlite3_index_constraint] = if n == 0 {
            &[]
        } else if a_constraint.is_null() || a_constraint_usage.is_null() {
            return SQLITE_ERROR;
        } else {
            unsafe { core::slice::from_raw_parts(a_constraint, n) }
        };
        let order_by = unsafe { order_by_slice(n_order_by, a_order_by) };
        let matches_collation = |idx: usize, coll: ColCollation| unsafe {
            constraint_matches_col_collation(p_info, idx, coll)
        };
        let plan = select_best_index(vtab, &matches_collation, constraints, order_by, col_used);

        // No more calls into `sqlite3_vtab_collation`: safe to materialize unique references.
        let info = unsafe { &mut *p_info };
        let usages: &mut [crate::vtab::ffi::sqlite3_index_constraint_usage] = if n == 0 {
            &mut []
        } else {
            unsafe { core::slice::from_raw_parts_mut(a_constraint_usage, n) }
        };
        for slot in 0..plan.num_used {
            let idx = plan.used_constraints[slot];
            usages[idx].argvIndex = (slot + 1) as c_int;
            usages[idx].omit = 0;
        }
        info.idxNum = plan.idx_num;
        if let Some(flags) = plan.idx_flags {
            info.idxFlags = flags;
        }
        if plan.order_by_consumed {
            info.orderByConsumed = 1;
        }
        info.estimatedCost = plan.estimated_cost;
        info.estimatedRows = plan.estimated_rows;

        #[cfg(test)]
        record_best_index_plan(info, usages);
        SQLITE_OK
    }))
    .unwrap_or(SQLITE_ERROR)
}

fn plan_pk_point(
    vtab: &AydbVTab,
    matches_collation: &impl Fn(usize, ColCollation) -> bool,
    constraints: &[crate::vtab::ffi::sqlite3_index_constraint],
    order_by: &[crate::vtab::ffi::sqlite3_index_orderby],
    can_use_keys_only: bool,
) -> Option<SelectedBestIndexPlan> {
    for (i, c) in constraints.iter().enumerate() {
        if c.usable == 0 {
            continue;
        }
        let col = c.iColumn;
        if c.op == SQLITE_INDEX_CONSTRAINT_EQ && (col == vtab.pk_col_idx as i32 || col == -1) {
            if col >= 0 && !matches_collation(i, vtab.pk_collation) {
                continue;
            }
            // Do not promise at most one row when a TEXT or BLOB PK may fall back to a
            // cross-affinity full scan where `'1'`, `'01'`, and `'1.0'` all match `1`.
            let unique_scan = col == -1
                || matches!(
                    vtab.pk_affinity,
                    ColumnAffinity::Integer | ColumnAffinity::Numeric | ColumnAffinity::Real
                );
            let idx_flags = unique_scan.then_some(SQLITE_INDEX_SCAN_UNIQUE);
            let order_by_consumed = (unique_scan && !order_by.is_empty())
                || order_by
                    .first()
                    .is_some_and(|ob| ob.iColumn == vtab.pk_col_idx as i32 && ob.desc == 0);
            return Some(SelectedBestIndexPlan {
                idx_num: DecodedPlan::PkPoint {
                    by_rowid: col == -1,
                    keys_only: can_use_keys_only,
                }
                .encode(),
                idx_flags,
                order_by_consumed,
                estimated_cost: 1.0,
                estimated_rows: 1,
                used_constraints: [i, 0],
                num_used: 1,
            });
        }
    }
    None
}

fn find_best_eq_candidate<T: Copy>(
    vtab: &AydbVTab,
    matches_collation: &impl Fn(usize, ColCollation) -> bool,
    constraints: &[crate::vtab::ffi::sqlite3_index_constraint],
    order_by: &[crate::vtab::ffi::sqlite3_index_orderby],
    mut resolve_col: impl FnMut(usize) -> Option<T>,
) -> Option<(usize, T, bool)> {
    let mut best: Option<(usize, T, bool)> = None;
    for (i, c) in constraints.iter().enumerate() {
        if c.usable == 0 || c.op != SQLITE_INDEX_CONSTRAINT_EQ || c.iColumn < 0 {
            continue;
        }
        let col_idx = c.iColumn as usize;
        let Some(tag) = resolve_col(col_idx) else {
            continue;
        };
        if !matches_collation(i, vtab.col_collations[col_idx]) {
            continue;
        }
        let ob = eq_index_consumes_order_by(
            order_by,
            col_idx,
            vtab.col_affinities[col_idx],
            vtab.pk_col_idx,
        );
        if best.is_none_or(|(_, _, prev_ob)| !prev_ob && ob) {
            best = Some((i, tag, ob));
            if ob {
                break;
            }
        }
    }
    best
}

struct RangeBounds {
    lower: Option<(usize, bool)>,
    upper: Option<(usize, bool)>,
}

fn scan_range_bounds(
    constraints: &[crate::vtab::ffi::sqlite3_index_constraint],
    target_col: i32,
    col_collation: ColCollation,
    matches_collation: &impl Fn(usize, ColCollation) -> bool,
) -> RangeBounds {
    let mut lower: Option<(usize, bool)> = None;
    let mut upper: Option<(usize, bool)> = None;
    for (i, c) in constraints.iter().enumerate() {
        if c.usable == 0 || c.iColumn != target_col {
            continue;
        }
        if !matches_collation(i, col_collation) {
            continue;
        }
        match c.op {
            SQLITE_INDEX_CONSTRAINT_GE if lower.is_none() => lower = Some((i, true)),
            SQLITE_INDEX_CONSTRAINT_GT if lower.is_none() => lower = Some((i, false)),
            SQLITE_INDEX_CONSTRAINT_LE if upper.is_none() => upper = Some((i, true)),
            SQLITE_INDEX_CONSTRAINT_LT if upper.is_none() => upper = Some((i, false)),
            _ => {}
        }
    }
    RangeBounds { lower, upper }
}

fn plan_range_scan(
    vtab: &AydbVTab,
    matches_collation: &impl Fn(usize, ColCollation) -> bool,
    constraints: &[crate::vtab::ffi::sqlite3_index_constraint],
    order_by: &[crate::vtab::ffi::sqlite3_index_orderby],
    can_use_keys_only: bool,
    pk_ob: Option<bool>,
) -> Option<SelectedBestIndexPlan> {
    struct RangeCandidate {
        sec_index_id: Option<u16>,
        lower: Option<(usize, bool)>,
        upper: Option<(usize, bool)>,
        ob_consumed: bool,
        reverse: bool,
        cost: f64,
        rows: i64,
    }

    let mut best_range: Option<RangeCandidate> = None;
    let pk_bounds = scan_range_bounds(
        constraints,
        vtab.pk_col_idx as i32,
        vtab.pk_collation,
        matches_collation,
    );
    if pk_bounds.lower.is_some() || pk_bounds.upper.is_some() {
        let bounded = pk_bounds.lower.is_some() && pk_bounds.upper.is_some();
        let cost = match (bounded, pk_ob.is_some()) {
            (true, true) => 14.0,
            (true, false) => 15.0,
            (false, true) => 75.0,
            (false, false) => 80.0,
        };
        best_range = Some(RangeCandidate {
            sec_index_id: None,
            lower: pk_bounds.lower,
            upper: pk_bounds.upper,
            ob_consumed: pk_ob.is_some(),
            reverse: pk_ob.unwrap_or(false),
            cost,
            rows: if bounded { 16 } else { 128 },
        });
    }

    for &(sec_col_idx, index_id) in &vtab.secondary_indexes {
        let sec_bounds = scan_range_bounds(
            constraints,
            sec_col_idx as i32,
            vtab.col_collations[sec_col_idx],
            matches_collation,
        );
        if sec_bounds.lower.is_some() || sec_bounds.upper.is_some() {
            let bounded = sec_bounds.lower.is_some() && sec_bounds.upper.is_some();
            let ob_consumed = sec_range_consumes_order_by(order_by, sec_col_idx, vtab.pk_col_idx);
            let cost = match (bounded, ob_consumed) {
                (true, true) => 25.0,
                (true, false) => 30.0,
                (false, true) => 100.0,
                (false, false) => 120.0,
            };
            if best_range.as_ref().is_none_or(|prev| cost < prev.cost) {
                best_range = Some(RangeCandidate {
                    sec_index_id: Some(index_id),
                    lower: sec_bounds.lower,
                    upper: sec_bounds.upper,
                    ob_consumed,
                    reverse: false,
                    cost,
                    rows: if bounded { 16 } else { 128 },
                });
            }
        }
    }

    let winner = best_range?;
    let mut used_constraints = [0usize; 2];
    let mut num_used = 0usize;
    if let Some((i, _)) = winner.lower {
        used_constraints[num_used] = i;
        num_used += 1;
    }
    if let Some((i, _)) = winner.upper {
        used_constraints[num_used] = i;
        num_used += 1;
    }
    let lower_inclusive = winner.lower.map(|(_, inc)| inc);
    let upper_inclusive = winner.upper.map(|(_, inc)| inc);
    let idx_num = match winner.sec_index_id {
        None => DecodedPlan::PkRange(RangePlan {
            lower_inclusive,
            upper_inclusive,
            keys_only: can_use_keys_only,
            reverse: winner.ob_consumed && winner.reverse,
        })
        .encode(),
        Some(index_id) => DecodedPlan::SecondaryRange {
            index_id,
            range: RangePlan {
                lower_inclusive,
                upper_inclusive,
                keys_only: can_use_keys_only,
                reverse: false,
            },
        }
        .encode(),
    };
    Some(SelectedBestIndexPlan {
        idx_num,
        idx_flags: None,
        order_by_consumed: winner.ob_consumed,
        estimated_cost: winner.cost,
        estimated_rows: winner.rows,
        used_constraints,
        num_used,
    })
}

fn select_best_index(
    vtab: &AydbVTab,
    matches_collation: &impl Fn(usize, ColCollation) -> bool,
    constraints: &[crate::vtab::ffi::sqlite3_index_constraint],
    order_by: &[crate::vtab::ffi::sqlite3_index_orderby],
    col_used: u64,
) -> SelectedBestIndexPlan {
    // Check if only `rowid` (`colUsed == 0`) or only a binary-collated PK column is projected
    let can_use_keys_only = col_used == 0
        || (vtab.pk_collation == ColCollation::Binary
            && vtab.pk_col_idx < 63
            && col_used == (1u64 << vtab.pk_col_idx));

    // 1. Check for usable equality constraint (`=`) on Primary Key (or ROWID `-1`)
    if let Some(plan) = plan_pk_point(
        vtab,
        matches_collation,
        constraints,
        order_by,
        can_use_keys_only,
    ) {
        return plan;
    }

    // 2. Check for usable equality constraint (`=`) on a trusted persistent UNIQUE column
    if !vtab.unique_def.is_empty() {
        if let Some((i, col_idx, ob_consumed)) =
            find_best_eq_candidate(vtab, matches_collation, constraints, order_by, |col_idx| {
                (col_idx < 0x7FFF && vtab.col_unique.get(col_idx) == Some(&true))
                    .then_some(col_idx as u16)
            })
        {
            if vtab.is_unique_index_ready_for_plan() {
                // Keep `omit = 0` so digest collisions and cross-affinity fallbacks filter exact values.
                return SelectedBestIndexPlan {
                    idx_num: DecodedPlan::UniqueEq {
                        col_idx,
                        keys_only: can_use_keys_only,
                    }
                    .encode(),
                    idx_flags: None,
                    order_by_consumed: ob_consumed,
                    estimated_cost: if ob_consumed { 1.8 } else { 2.0 },
                    estimated_rows: 1,
                    used_constraints: [i, 0],
                    num_used: 1,
                };
            }
        }
    }

    // 3. Check for usable equality constraint (`=`) on a Secondary Index column
    if let Some((i, index_id, ob_consumed)) =
        find_best_eq_candidate(vtab, matches_collation, constraints, order_by, |col_idx| {
            vtab.secondary_indexes
                .iter()
                .find(|&&(c, _)| c == col_idx)
                .map(|&(_, id)| id)
        })
    {
        // Do not set `omit = 1` on secondary index lookups so SQLite verifies the
        // exact column type and value (`NULL` vs `""` vs `X''`).
        return SelectedBestIndexPlan {
            idx_num: DecodedPlan::SecondaryEq {
                index_id,
                keys_only: can_use_keys_only,
            }
            .encode(),
            idx_flags: None,
            order_by_consumed: ob_consumed,
            estimated_cost: if ob_consumed { 4.5 } else { 5.0 },
            estimated_rows: 4,
            used_constraints: [i, 0],
            num_used: 1,
        };
    }

    // 4. Compare usable range constraints (`>`, `>=`, `<`, `<=`) across PK and Secondary
    //    Indexes so bounded ranges beat one-sided ranges (`bounded PK < bounded secondary <
    //    one-sided PK < one-sided secondary`).
    let pk_ob = pk_order_by(order_by, vtab.pk_col_idx);
    if let Some(plan) = plan_range_scan(
        vtab,
        matches_collation,
        constraints,
        order_by,
        can_use_keys_only,
        pk_ob,
    ) {
        return plan;
    }

    // 5. Fallback: Full Table Scan (let SQLite's VDBE enforce LIMIT/OFFSET after any
    //    residual WHERE predicates like `LIKE`, `OR`, or expressions not passed in `aConstraint`)
    SelectedBestIndexPlan {
        idx_num: DecodedPlan::FullScan {
            keys_only: can_use_keys_only,
            reverse: pk_ob.unwrap_or(false),
        }
        .encode(),
        idx_flags: None,
        order_by_consumed: pk_ob.is_some(),
        estimated_cost: 10_000.0,
        estimated_rows: 1_000,
        used_constraints: [0; 2],
        num_used: 0,
    }
}
