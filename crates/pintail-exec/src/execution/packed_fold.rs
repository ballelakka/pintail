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

/// Folds one lane's units into `totals`, counting NULL rows per slot.
#[inline]
fn fold_units<const OP: u8>(
    totals: &mut [i128],
    nulls: &mut Vec<u64>,
    slots: &[u32],
    rows: &FoldRows<'_>,
    values: &[i64],
    validity: &ValidityMask,
) {
    let slot_count = totals.len();
    match rows {
        FoldRows::Span(span) => {
            let values = &values[span.clone()];
            if validity.no_nulls() {
                for (&slot, &value) in slots.iter().zip(values) {
                    combine::<OP>(&mut totals[slot as usize], value);
                }
            } else {
                let nulls = lane_nulls(nulls, slot_count);
                for ((&slot, &value), row) in slots.iter().zip(values).zip(span.clone()) {
                    if validity.is_valid(row) {
                        combine::<OP>(&mut totals[slot as usize], value);
                    } else {
                        nulls[slot as usize] += 1;
                    }
                }
            }
        }
        FoldRows::Picked(picked) => {
            if validity.no_nulls() {
                for (&slot, &row) in slots.iter().zip(*picked) {
                    combine::<OP>(&mut totals[slot as usize], values[row as usize]);
                }
            } else {
                let nulls = lane_nulls(nulls, slot_count);
                for (&slot, &row) in slots.iter().zip(*picked) {
                    let row = row as usize;
                    if validity.is_valid(row) {
                        combine::<OP>(&mut totals[slot as usize], values[row]);
                    } else {
                        nulls[slot as usize] += 1;
                    }
                }
            }
        }
    }
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
}

impl PackedFold {
    pub(super) fn new(slot_count: usize, lanes: &[Option<PackedLane>]) -> Self {
        Self {
            slot_count,
            lanes: lanes.to_vec(),
            counts: written_zeros(slot_count),
            totals: lanes
                .iter()
                .map(|lane| {
                    if keeps_total(*lane) {
                        filled(slot_count, identity(*lane))
                    } else {
                        Vec::new()
                    }
                })
                .collect(),
            nulls: lanes.iter().map(|_| Vec::new()).collect(),
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
        for (index, input) in inputs.iter().enumerate() {
            let LaneInput::Units(values, validity) = input else {
                continue;
            };
            let totals = &mut self.totals[index];
            let nulls = &mut self.nulls[index];
            match self.lanes[index] {
                Some(PackedLane::Sum { .. } | PackedLane::Average { .. }) => {
                    fold_units::<SUM>(totals, nulls, slots, rows, values, validity);
                }
                Some(PackedLane::Minimum { .. }) => {
                    fold_units::<MINIMUM>(totals, nulls, slots, rows, values, validity);
                }
                Some(PackedLane::Maximum { .. }) => {
                    fold_units::<MAXIMUM>(totals, nulls, slots, rows, values, validity);
                }
                Some(PackedLane::Count) | None => {}
            }
        }
    }

    /// Folds one row through the per-row readers: the same arithmetic as
    /// [`Self::fold`], for a batch whose columns carry no packed units.
    pub(super) fn add_row(&mut self, slot: usize, readers: &[LaneReader<'_>], row: usize) {
        self.counts[slot] += 1;
        for (index, reader) in readers.iter().enumerate() {
            let lane = self.lanes[index];
            if !keeps_total(lane) {
                continue;
            }
            if let Some(bits) = reader.bits(row) {
                let value = i64::from_ne_bytes(bits.to_ne_bytes());
                let total = &mut self.totals[index][slot];
                match lane {
                    Some(PackedLane::Minimum { .. }) => combine::<MINIMUM>(total, value),
                    Some(PackedLane::Maximum { .. }) => combine::<MAXIMUM>(total, value),
                    _ => combine::<SUM>(total, value),
                }
            } else {
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
        if let Some(&value) = fold.totals[index].get(slot) {
            match lane {
                PackedLane::Minimum { .. } => total = total.min(value),
                PackedLane::Maximum { .. } => total = total.max(value),
                _ => total = total.wrapping_add(value),
            }
        }
        nulls += fold.nulls[index].get(slot).copied().unwrap_or(0);
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
