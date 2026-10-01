//! Column-at-a-time folds for the packed aggregate lanes.
//!
//! A packed lane (COUNT(*), exact DECIMAL SUM and AVG, DECIMAL MIN/MAX)
//! reduces every group to an `i128` total and a row count, which is what
//! [`PackedCell`] commits to a group's state. The per-row fold resolved
//! each lane's reader, matched the lane kind and returned a `Result` for
//! every row and lane; here a batch's slots are computed once and each lane
//! then runs one monomorphized loop over its packed column, so the loop body
//! is a load, an add and a store.
//!
//! The accumulator is struct-of-arrays: a row count per slot, a total per
//! lane and slot, and a NULL count per lane and slot that is only allocated
//! once a lane meets a NULL. A lane's row count is the slot's rows minus its
//! NULLs, which is the count [`PackedCell::add`] keeps by incrementing.

use std::ops::Range;

use super::aggregate::{AggregateState, CompiledAggregate, written_zeros};
use super::two_pass::{LaneReader, PackedCell, PackedLane, TwoPassLane};
use super::{ExecError, MemoryTracker};
use crate::RecordBatch;
use crate::array::ValidityMask;
use crate::batch::{DecimalUnits, TypedValues};

/// The physical rows a fold reads, in the order their slots are listed.
#[derive(Clone, Debug)]
pub(super) enum FoldRows<'a> {
    /// A contiguous run of rows, every one selected.
    Span(Range<usize>),
    /// Selected rows, ascending.
    Picked(&'a [u32]),
}

impl FoldRows<'_> {
    pub(super) fn len(&self) -> usize {
        match self {
            Self::Span(rows) => rows.len(),
            Self::Picked(rows) => rows.len(),
        }
    }

    /// The listed rows, in order.
    pub(super) fn iter(&self) -> impl Iterator<Item = usize> + '_ {
        (0..self.len()).map(move |index| match self {
            Self::Span(rows) => rows.start + index,
            Self::Picked(rows) => rows[index] as usize,
        })
    }
}

/// The rows of `rows` the batch selects: a span when every one is selected,
/// otherwise the selected rows gathered into `buffer`.
pub(super) fn fold_rows<'a>(
    batch: &RecordBatch,
    rows: Range<usize>,
    buffer: &'a mut Vec<u32>,
) -> FoldRows<'a> {
    let selection = batch.selection();
    if selection.count_in(rows.clone()) == rows.len() {
        return FoldRows::Span(rows);
    }
    buffer.clear();
    buffer.extend(
        selection
            .selected_rows_in(rows)
            .map(|row| u32::try_from(row).expect("batch row fits u32")),
    );
    FoldRows::Picked(buffer)
}

/// One lane's input for a batch, resolved before any row is read.
pub(super) enum LaneInput<'a> {
    /// The lane is not packed; the caller applies it some other way.
    Skip,
    /// COUNT(*): the slot's row count is the lane's.
    Count,
    /// Scaled decimal units that fit 64 bits.
    Units(&'a [i64], &'a ValidityMask),
}

const SUM: u8 = 0;
const MINIMUM: u8 = 1;
const MAXIMUM: u8 = 2;

#[inline]
fn combine<const OP: u8>(total: &mut i128, value: i64) {
    let value = i128::from(value);
    match OP {
        // A total of `i64` units cannot leave `i128` before 2^64 rows, so
        // the wrapping add never wraps; it only spares the overflow check.
        SUM => *total = total.wrapping_add(value),
        MINIMUM => *total = (*total).min(value),
        _ => *total = (*total).max(value),
    }
}

/// One column's accumulators: the totals of its SUM, MIN and MAX roles
/// (empty for a role no lane takes) and its NULL rows per slot.
struct ColumnTotals<'a> {
    sum: &'a mut [i128],
    minimum: &'a mut [i128],
    maximum: &'a mut [i128],
}

impl ColumnTotals<'_> {
    /// Folds one value into `slot` for every role the column has.
    #[inline]
    fn add<const S: bool, const MN: bool, const MX: bool>(&mut self, slot: usize, value: i64) {
        if S {
            combine::<SUM>(&mut self.sum[slot], value);
        }
        if MN {
            combine::<MINIMUM>(&mut self.minimum[slot], value);
        }
        if MX {
            combine::<MAXIMUM>(&mut self.maximum[slot], value);
        }
    }
}

/// Folds one column's units into every role it has in a single pass,
/// counting NULL rows per slot.
#[inline]
fn fold_column<const S: bool, const MN: bool, const MX: bool>(
    mut totals: ColumnTotals<'_>,
    nulls: &mut Vec<u64>,
    slot_count: usize,
    slots: &[u32],
    rows: &FoldRows<'_>,
    values: &[i64],
    validity: &ValidityMask,
) {
    match rows {
        FoldRows::Span(span) => {
            let values = &values[span.clone()];
            if validity.no_nulls() {
                for (&slot, &value) in slots.iter().zip(values) {
                    totals.add::<S, MN, MX>(slot as usize, value);
                }
            } else {
                let nulls = lane_nulls(nulls, slot_count);
                for ((&slot, &value), row) in slots.iter().zip(values).zip(span.clone()) {
                    if validity.is_valid(row) {
                        totals.add::<S, MN, MX>(slot as usize, value);
                    } else {
                        nulls[slot as usize] += 1;
                    }
                }
            }
        }
        FoldRows::Picked(picked) => {
            if validity.no_nulls() {
                for (&slot, &row) in slots.iter().zip(*picked) {
                    totals.add::<S, MN, MX>(slot as usize, values[row as usize]);
                }
            } else {
                let nulls = lane_nulls(nulls, slot_count);
                for (&slot, &row) in slots.iter().zip(*picked) {
                    let row = row as usize;
                    if validity.is_valid(row) {
                        totals.add::<S, MN, MX>(slot as usize, values[row]);
                    } else {
                        nulls[slot as usize] += 1;
                    }
                }
            }
        }
    }
}

/// The packed lanes that read one column: the lane holding each role's
/// totals (SUM and AVG share the sum role), and the lane holding the
/// column's NULL counts.
#[derive(Clone, Copy, Debug, Default)]
struct ColumnRoles {
    sum: Option<usize>,
    minimum: Option<usize>,
    maximum: Option<usize>,
    nulls: usize,
}

fn lane_nulls(nulls: &mut Vec<u64>, slot_count: usize) -> &mut [u64] {
    if nulls.is_empty() {
        *nulls = written_zeros(slot_count);
    }
    nulls
}

/// The starting total of a lane: the identity of its combine.
fn identity(lane: Option<PackedLane>) -> i128 {
    match lane {
        Some(PackedLane::Minimum { .. }) => i128::MAX,
        Some(PackedLane::Maximum { .. }) => i128::MIN,
        _ => 0,
    }
}

/// Whether a lane keeps a total (COUNT(*) needs only the slot's rows).
fn keeps_total(lane: Option<PackedLane>) -> bool {
    matches!(
        lane,
        Some(
            PackedLane::Sum { .. }
                | PackedLane::Average { .. }
                | PackedLane::Minimum { .. }
                | PackedLane::Maximum { .. }
        )
    )
}

/// Per-slot packed totals for every packed lane of an aggregate.
pub(super) struct PackedFold {
    slot_count: usize,
    lanes: Vec<Option<PackedLane>>,
    /// Selected rows per slot.
    counts: Vec<u64>,
    /// Per lane: one total per slot, or empty for a lane with no total.
    totals: Vec<Vec<i128>>,
    /// Per lane: NULL rows per slot, empty until the lane meets a NULL.
    nulls: Vec<Vec<u64>>,
    /// Per lane: the lane whose `totals` hold its result - itself, or the
    /// first lane of the same role over the same column (SUM beside AVG).
    total_of: Vec<usize>,
    /// Per lane: the lane whose `nulls` count its column's NULL rows.
    nulls_of: Vec<usize>,
    /// The lanes that keep totals, grouped by the column they read.
    columns: Vec<ColumnRoles>,
}

impl PackedFold {
    #[cfg(test)]
    pub(super) fn new(slot_count: usize, lanes: &[Option<PackedLane>]) -> Self {
        Self::over_columns(slot_count, lanes, &(0..lanes.len()).collect::<Vec<_>>())
    }

    /// A fold whose lanes read `columns` (one per lane): the lanes over one
    /// column fold in a single pass, and a second sum of a column - SUM
    /// beside AVG - shares the first one's totals instead of repeating it.
    pub(super) fn sharing(
        slot_count: usize,
        packed: &[Option<PackedLane>],
        lanes: &[TwoPassLane],
    ) -> Self {
        let columns = lanes
            .iter()
            .enumerate()
            .map(|(index, lane)| match lane {
                TwoPassLane::DecimalUnits { column, .. }
                | TwoPassLane::ExtremeDecimal { column, .. } => *column,
                // Never shared: a key past any real column index.
                _ => usize::MAX - index,
            })
            .collect::<Vec<_>>();
        Self::over_columns(slot_count, packed, &columns)
    }

    fn over_columns(slot_count: usize, lanes: &[Option<PackedLane>], sources: &[usize]) -> Self {
        let mut total_of = (0..lanes.len()).collect::<Vec<_>>();
        let mut nulls_of = total_of.clone();
        let mut columns: Vec<(usize, ColumnRoles)> = Vec::new();
        for (index, lane) in lanes.iter().enumerate() {
            if !keeps_total(*lane) {
                continue;
            }
            let source = sources[index];
            let position = columns
                .iter()
                .position(|(column, _)| *column == source)
                .unwrap_or_else(|| {
                    columns.push((
                        source,
                        ColumnRoles {
                            nulls: index,
                            ..ColumnRoles::default()
                        },
                    ));
                    columns.len() - 1
                });
            let roles = &mut columns[position].1;
            nulls_of[index] = roles.nulls;
            let role = match lane {
                Some(PackedLane::Minimum { .. }) => &mut roles.minimum,
                Some(PackedLane::Maximum { .. }) => &mut roles.maximum,
                _ => &mut roles.sum,
            };
            total_of[index] = *role.get_or_insert(index);
        }
        Self {
            slot_count,
            lanes: lanes.to_vec(),
            counts: written_zeros(slot_count),
            totals: lanes
                .iter()
                .enumerate()
                .map(|(index, lane)| {
                    if keeps_total(*lane) && total_of[index] == index {
                        filled(slot_count, identity(*lane))
                    } else {
                        Vec::new()
                    }
                })
                .collect(),
            nulls: lanes.iter().map(|_| Vec::new()).collect(),
            total_of,
            nulls_of,
            columns: columns.into_iter().map(|(_, roles)| roles).collect(),
        }
    }

    /// The most a fold over `slot_count` slots holds, NULL counts included.
    pub(super) fn bytes(slot_count: usize, lanes: &[Option<PackedLane>]) -> usize {
        let per_slot = lanes.iter().fold(size_of::<u64>(), |bytes, lane| {
            let total = if keeps_total(*lane) {
                size_of::<i128>()
            } else {
                0
            };
            bytes.saturating_add(total + size_of::<u64>())
        });
        slot_count.saturating_mul(per_slot)
    }

    /// Whether any row reached `slot`.
    pub(super) fn occupied(&self, slot: usize) -> bool {
        self.counts[slot] > 0
    }

    /// Each lane's packed input in `batch`, or `None` when a packed lane's
    /// column carries no 64-bit units (the caller folds such a batch row by
    /// row through [`Self::add_row`]).
    pub(super) fn resolve<'a>(
        &self,
        batch: &'a RecordBatch,
        lanes: &[TwoPassLane],
    ) -> Option<Vec<LaneInput<'a>>> {
        lanes
            .iter()
            .zip(&self.lanes)
            .map(|(lane, packed)| match (packed, lane) {
                (None, _) => Some(LaneInput::Skip),
                (Some(PackedLane::Count), _) => Some(LaneInput::Count),
                (
                    Some(_),
                    TwoPassLane::DecimalUnits { column, .. }
                    | TwoPassLane::ExtremeDecimal { column, .. },
                ) => match batch.column(*column).and_then(crate::ColumnVector::typed) {
                    Some((
                        TypedValues::Decimal128 {
                            values: DecimalUnits::Narrow(values),
                            ..
                        },
                        validity,
                    )) if values.len() >= batch.row_count() => {
                        Some(LaneInput::Units(values, validity))
                    }
                    _ => None,
                },
                (Some(_), _) => None,
            })
            .collect()
    }

    /// Folds the rows `rows` lists, the i-th into `slots[i]`.
    pub(super) fn fold(&mut self, inputs: &[LaneInput<'_>], slots: &[u32], rows: &FoldRows<'_>) {
        debug_assert_eq!(slots.len(), rows.len());
        for &slot in slots {
            self.counts[slot as usize] += 1;
        }
        for roles in &self.columns {
            let LaneInput::Units(values, validity) = &inputs[roles.nulls] else {
                continue;
            };
            let mut take = |lane: Option<usize>| {
                lane.map_or_else(Vec::new, |lane| std::mem::take(&mut self.totals[lane]))
            };
            let (mut sum, mut minimum, mut maximum) =
                (take(roles.sum), take(roles.minimum), take(roles.maximum));
            let mut nulls = std::mem::take(&mut self.nulls[roles.nulls]);
            let totals = ColumnTotals {
                sum: &mut sum,
                minimum: &mut minimum,
                maximum: &mut maximum,
            };
            let slot_count = self.slot_count;
            macro_rules! fold {
                ($s:literal, $mn:literal, $mx:literal) => {
                    fold_column::<$s, $mn, $mx>(
                        totals, &mut nulls, slot_count, slots, rows, values, validity,
                    )
                };
            }
            match (
                roles.sum.is_some(),
                roles.minimum.is_some(),
                roles.maximum.is_some(),
            ) {
                (true, true, true) => fold!(true, true, true),
                (true, true, false) => fold!(true, true, false),
                (true, false, true) => fold!(true, false, true),
                (true, false, false) => fold!(true, false, false),
                (false, true, true) => fold!(false, true, true),
                (false, true, false) => fold!(false, true, false),
                (false, false, true) => fold!(false, false, true),
                (false, false, false) => {}
            }
            for (lane, totals) in [roles.sum, roles.minimum, roles.maximum]
                .into_iter()
                .zip([sum, minimum, maximum])
            {
                if let Some(lane) = lane {
                    self.totals[lane] = totals;
                }
            }
            self.nulls[roles.nulls] = nulls;
        }
    }

    /// Folds one row through the per-row readers: the same arithmetic as
    /// [`Self::fold`], for a batch whose columns carry no packed units.
    pub(super) fn add_row(&mut self, slot: usize, readers: &[LaneReader<'_>], row: usize) {
        self.counts[slot] += 1;
        for (index, reader) in readers.iter().enumerate() {
            let lane = self.lanes[index];
            if !keeps_total(lane) || self.total_of[index] != index {
                continue;
            }
            // A lane sharing another's totals is skipped above; one that
            // owns its totals but not its column's NULL counts skips NULLs.
            if let Some(bits) = reader.bits(row) {
                let value = i64::from_ne_bytes(bits.to_ne_bytes());
                let total = &mut self.totals[index][slot];
                match lane {
                    Some(PackedLane::Minimum { .. }) => combine::<MINIMUM>(total, value),
                    Some(PackedLane::Maximum { .. }) => combine::<MAXIMUM>(total, value),
                    _ => combine::<SUM>(total, value),
                }
            } else if self.nulls_of[index] == index {
                let slot_count = self.slot_count;
                lane_nulls(&mut self.nulls[index], slot_count)[slot] += 1;
            }
        }
    }

    /// Adds `other` in, its slot `s` landing on `place(s)`.
    pub(super) fn merge_from(&mut self, other: &Self, place: impl Fn(usize) -> usize) {
        for (slot, &count) in other.counts.iter().enumerate() {
            if count == 0 {
                continue;
            }
            let target = place(slot);
            self.counts[target] += count;
            for index in 0..self.lanes.len() {
                if let Some(&value) = other.totals[index].get(slot) {
                    let total = &mut self.totals[index][target];
                    match self.lanes[index] {
                        Some(PackedLane::Minimum { .. }) => *total = (*total).min(value),
                        Some(PackedLane::Maximum { .. }) => *total = (*total).max(value),
                        _ => *total = total.wrapping_add(value),
                    }
                }
                if let Some(&nulls) = other.nulls[index].get(slot)
                    && nulls > 0
                {
                    let slot_count = self.slot_count;
                    lane_nulls(&mut self.nulls[index], slot_count)[target] += nulls;
                }
            }
        }
    }

    /// Applies `slot`'s totals to its group's states, once per lane.
    pub(super) fn commit_slot(
        &self,
        slot: usize,
        states: &mut [AggregateState],
        aggregates: &[CompiledAggregate],
        memory: &MemoryTracker,
    ) -> Result<(), ExecError> {
        commit_merged(std::slice::from_ref(self), slot, states, aggregates, memory)
    }
}

/// Whether any of `folds`, all over the same slots, reached `slot`.
pub(super) fn occupied_in(folds: &[PackedFold], slot: usize) -> bool {
    folds.iter().any(|fold| fold.occupied(slot))
}

/// Applies `slot`'s totals across `folds` - worker partials over the same
/// slots and lanes - to its group's states, once per lane: the partials
/// combine per slot here instead of being merged whole first.
pub(super) fn commit_merged(
    folds: &[PackedFold],
    slot: usize,
    states: &mut [AggregateState],
    aggregates: &[CompiledAggregate],
    memory: &MemoryTracker,
) -> Result<(), ExecError> {
    for (index, (state, aggregate)) in states.iter_mut().zip(aggregates).enumerate() {
        if let Some((lane, cell)) = merged_cell(folds, index, slot) {
            cell.commit(lane, state, aggregate, memory)?;
        }
    }
    Ok(())
}

/// Lane `index`'s total and row count for `slot` across `folds`, or `None`
/// for a lane that is not packed.
pub(super) fn merged_cell(
    folds: &[PackedFold],
    index: usize,
    slot: usize,
) -> Option<(PackedLane, PackedCell)> {
    let lane = folds.first()?.lanes[index]?;
    let count: u64 = folds.iter().map(|fold| fold.counts[slot]).sum();
    let mut total = identity(Some(lane));
    let mut nulls = 0_u64;
    for fold in folds {
        if let Some(&value) = fold.totals[fold.total_of[index]].get(slot) {
            match lane {
                PackedLane::Minimum { .. } => total = total.min(value),
                PackedLane::Maximum { .. } => total = total.max(value),
                _ => total = total.wrapping_add(value),
            }
        }
        nulls += fold.nulls[fold.nulls_of[index]]
            .get(slot)
            .copied()
            .unwrap_or(0);
    }
    Some((
        lane,
        PackedCell {
            total,
            rows: count - nulls,
        },
    ))
}

/// `len` copies of `value`, written as they are made (see [`written_zeros`]:
/// a zero identity would otherwise come back as untouched zero pages).
fn filled(len: usize, value: i128) -> Vec<i128> {
    let mut values = written_zeros(len);
    if value != 0 {
        values.fill(value);
    }
    values
}

#[cfg(test)]
mod tests {
    use super::{FoldRows, LaneInput, PackedFold, PackedLane, merged_cell};
    use crate::array::ValidityMask;

    const LANES: [Option<PackedLane>; 4] = [
        Some(PackedLane::Count),
        Some(PackedLane::Sum {
            scale: 2,
            float_output: false,
        }),
        Some(PackedLane::Minimum { scale: 2 }),
        Some(PackedLane::Maximum { scale: 2 }),
    ];

    /// Invented column values: signed units, every seventh row NULL.
    fn column(rows: usize) -> (Vec<i64>, ValidityMask) {
        let values = (0..rows)
            .map(|row| {
                let row = i64::try_from(row).expect("small");
                (row * 7_919) % 20_011 - 10_000
            })
            .collect::<Vec<_>>();
        let valid = (0..rows).map(|row| row % 7 != 3).collect::<Vec<_>>();
        (values, ValidityMask::from_bools(&valid))
    }

    fn slots_of(rows: impl Iterator<Item = usize>, slot_count: usize) -> Vec<u32> {
        rows.map(|row| u32::try_from((row * 31) % slot_count).expect("small"))
            .collect()
    }

    /// Per slot: rows, then (total, non-NULL rows) for SUM, MIN and MAX.
    fn reference(
        rows: &[usize],
        slots: &[u32],
        values: &[i64],
        valid: &ValidityMask,
        slot_count: usize,
    ) -> Vec<(u64, [(i128, u64); 3])> {
        let mut expected = vec![(0, [(0, 0), (i128::MAX, 0), (i128::MIN, 0)]); slot_count];
        for (&row, &slot) in rows.iter().zip(slots) {
            let entry = &mut expected[slot as usize];
            entry.0 += 1;
            if valid.is_valid(row) {
                let value = i128::from(values[row]);
                entry.1[0] = (entry.1[0].0 + value, entry.1[0].1 + 1);
                entry.1[1] = (entry.1[1].0.min(value), entry.1[1].1 + 1);
                entry.1[2] = (entry.1[2].0.max(value), entry.1[2].1 + 1);
            }
        }
        expected
    }

    fn check(folds: &[PackedFold], expected: &[(u64, [(i128, u64); 3])]) {
        for (slot, (rows, lanes)) in expected.iter().enumerate() {
            let (_, count) = merged_cell(folds, 0, slot).expect("count lane");
            assert_eq!(count.rows, *rows, "slot {slot} rows");
            for (index, (total, non_null)) in lanes.iter().enumerate() {
                let (_, cell) = merged_cell(folds, index + 1, slot).expect("packed lane");
                assert_eq!(cell.rows, *non_null, "slot {slot} lane {index} rows");
                if *non_null > 0 {
                    assert_eq!(cell.total, *total, "slot {slot} lane {index} total");
                }
            }
        }
    }

    #[test]
    fn spans_picked_rows_and_partials_agree_with_a_row_by_row_reference() {
        let rows = 10_000;
        let slot_count = 13;
        let (values, valid) = column(rows);
        let inputs = [
            LaneInput::Count,
            LaneInput::Units(&values, &valid),
            LaneInput::Units(&values, &valid),
            LaneInput::Units(&values, &valid),
        ];
        // A whole span into one fold.
        let all = (0..rows).collect::<Vec<_>>();
        let slots = slots_of(all.iter().copied(), slot_count);
        let mut fold = PackedFold::new(slot_count, &LANES);
        fold.fold(&inputs, &slots, &FoldRows::Span(0..rows));
        check(
            std::slice::from_ref(&fold),
            &reference(&all, &slots, &values, &valid, slot_count),
        );
        // Every third row picked, split across two partials.
        let picked = (0..rows).filter(|row| row % 3 == 0).collect::<Vec<_>>();
        let half = picked.len() / 2;
        let mut partials = Vec::new();
        for part in [&picked[..half], &picked[half..]] {
            let indices = part
                .iter()
                .map(|row| u32::try_from(*row).expect("small"))
                .collect::<Vec<_>>();
            let part_slots = slots_of(part.iter().copied(), slot_count);
            let mut fold = PackedFold::new(slot_count, &LANES);
            fold.fold(&inputs, &part_slots, &FoldRows::Picked(&indices));
            partials.push(fold);
        }
        let picked_slots = slots_of(picked.iter().copied(), slot_count);
        check(
            &partials,
            &reference(&picked, &picked_slots, &values, &valid, slot_count),
        );
    }

    #[test]
    fn lanes_over_one_column_fold_once_and_agree_with_separate_lanes() {
        use super::super::two_pass::TwoPassLane;
        let rows = 9_000;
        let slot_count = 11;
        let (values, valid) = column(rows);
        // COUNT(*), SUM, AVG, MIN, MAX of one column.
        let packed = [
            Some(PackedLane::Count),
            LANES[1],
            Some(PackedLane::Average {
                digits: 4,
                result_scale: 6,
            }),
            LANES[2],
            LANES[3],
        ];
        let sum = TwoPassLane::DecimalUnits {
            column: 3,
            scale: 2,
            float_output: false,
        };
        let extreme = TwoPassLane::ExtremeDecimal {
            column: 3,
            scale: 2,
        };
        let lanes = [TwoPassLane::CountStar, sum, sum, extreme, extreme];
        let inputs = [
            LaneInput::Count,
            LaneInput::Units(&values, &valid),
            LaneInput::Units(&values, &valid),
            LaneInput::Units(&values, &valid),
            LaneInput::Units(&values, &valid),
        ];
        let listed = (0..rows)
            .filter(|row| row % 5 != 1)
            .map(|row| u32::try_from(row).expect("small"))
            .collect::<Vec<_>>();
        let listed_slots = slots_of(listed.iter().map(|row| *row as usize), slot_count);
        let span_slots = slots_of(0..rows, slot_count);
        let mut shared = PackedFold::sharing(slot_count, &packed, &lanes);
        let mut separate = PackedFold::new(slot_count, &packed);
        for fold in [&mut shared, &mut separate] {
            fold.fold(&inputs, &listed_slots, &FoldRows::Picked(&listed));
            fold.fold(&inputs, &span_slots, &FoldRows::Span(0..rows));
        }
        let mut merged = PackedFold::sharing(slot_count, &packed, &lanes);
        merged.merge_from(&shared, |slot| slot);
        for slot in 0..slot_count {
            for index in 0..packed.len() {
                let (_, expected) =
                    merged_cell(std::slice::from_ref(&separate), index, slot).expect("packed");
                for folds in [std::slice::from_ref(&shared), std::slice::from_ref(&merged)] {
                    let (_, cell) = merged_cell(folds, index, slot).expect("packed");
                    assert_eq!(cell.rows, expected.rows, "slot {slot} lane {index} rows");
                    assert_eq!(cell.total, expected.total, "slot {slot} lane {index} total");
                }
            }
        }
    }

    /// Kernel measurement, ignored by default:
    /// `cargo test --release -p pintail-exec --lib packed_fold::tests::kernel -- --ignored --nocapture`.
    #[test]
    #[ignore = "measurement"]
    fn kernel_against_row_at_a_time() {
        let rows = 1 << 22;
        let (values, _) = column(rows);
        let valid = ValidityMask::all_valid(rows);
        let lanes = [LANES[0], LANES[1]];
        for slot_count in [5, 1_025, 100_001] {
            let slots = slots_of(0..rows, slot_count);
            let inputs = [LaneInput::Count, LaneInput::Units(&values, &valid)];
            let mut best_fold = f64::MAX;
            let mut best_rows = f64::MAX;
            for _ in 0..5 {
                let mut fold = PackedFold::new(slot_count, &lanes);
                let started = std::time::Instant::now();
                for start in (0..rows).step_by(4_096) {
                    let end = (start + 4_096).min(rows);
                    fold.fold(&inputs, &slots[start..end], &FoldRows::Span(start..end));
                }
                best_fold = best_fold.min(started.elapsed().as_secs_f64());
                std::hint::black_box(&fold);
                // The shape this replaced: per row and lane, a reader match,
                // a lane match and a checked add returning a Result.
                let mut cells = vec![(0_i128, 0_u64); slot_count * 2];
                let started = std::time::Instant::now();
                for (row, slot) in slots.iter().enumerate() {
                    for (index, lane) in lanes.iter().enumerate() {
                        let bits = match index {
                            0 => Some(0),
                            _ => valid.is_valid(row).then_some(values[row]),
                        };
                        let cell = &mut cells[*slot as usize * 2 + index];
                        if let Some(bits) = std::hint::black_box(bits) {
                            let added: Result<(), ()> = match lane {
                                Some(PackedLane::Count) => Ok(()),
                                _ => cell
                                    .0
                                    .checked_add(i128::from(bits))
                                    .map(|total| cell.0 = total)
                                    .ok_or(()),
                            };
                            added.expect("no overflow");
                            cell.1 += 1;
                        }
                    }
                }
                best_rows = best_rows.min(started.elapsed().as_secs_f64());
                std::hint::black_box(&cells);
            }
            #[allow(clippy::cast_precision_loss)]
            let per_row = |seconds: f64| seconds * 1e9 / rows as f64;
            eprintln!(
                "{slot_count} slots: fold {:.2} ns/row, row-at-a-time {:.2} ns/row",
                per_row(best_fold),
                per_row(best_rows)
            );
        }
    }
}
