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
    let Some(first) = folds.first() else {
        return Ok(());
    };
    let count: u64 = folds.iter().map(|fold| fold.counts[slot]).sum();
    for (index, (state, aggregate)) in states.iter_mut().zip(aggregates).enumerate() {
        let Some(lane) = first.lanes[index] else {
            continue;
        };
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
        let cell = PackedCell {
            total,
            rows: count - nulls,
        };
        cell.commit(lane, state, aggregate, memory)?;
    }
    Ok(())
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
