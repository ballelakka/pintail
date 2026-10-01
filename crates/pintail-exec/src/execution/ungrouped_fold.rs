//! Column-at-a-time folds for an aggregate with no GROUP BY.
//!
//! An ungrouped aggregate over a filtered scan - a COUNT and a SUM over the
//! last seven days of an event table, say - went through the grouped row
//! loop with an empty key: per selected row it hashed the empty key, probed
//! the one-entry map, charged the memory tracker and dispatched every
//! aggregate through the general update, and an integer SUM or a temporal
//! MIN materialized its column as `Value` cells to read one number. That
//! measured 80 to 270 ns a row, single-threaded, where the scan beneath it
//! spent 1 to 3 ns. Block skipping had already cut the scan to the window;
//! the aggregate was the whole query.
//!
//! Here each aggregate folds its column over the batch's selected rows in
//! one typed loop and then updates its state once per batch. Every fold is
//! one the per-row update performs exactly: counts, integer and scaled
//! decimal unit totals, and the first row holding a MIN or MAX in row order.
//! An aggregate with no column fold here keeps the per-row update, for its
//! own state only.

use pintail_sql::AggregateFunction;
use pintail_types::{DataType, Value};

use super::aggregate::{
    AggregateState, CompiledAggregate, aggregate_uses_float, decimal_average_scale,
    update_aggregate_states,
};
use super::packed_fold::{FoldRows, fold_rows};
use super::{ExecError, MemoryTracker};
use crate::RecordBatch;
use crate::array::ValidityMask;
use crate::batch::{DecimalUnits, TypedValues};

/// Widest decimal-average widening folded as one partial total: an `i64`
/// unit times `10^19` still fits `i128`.
const AVERAGE_MAX_DIGITS: u8 = 19;

/// Whether every aggregate can take this fold at all. The ones that collect
/// values (`GROUP_CONCAT`, the JSON aggregates) keep the general path, which
/// can spill them.
pub(super) fn eligible(aggregates: &[CompiledAggregate]) -> bool {
    !aggregates.is_empty()
        && aggregates.iter().all(|aggregate| {
            !matches!(
                aggregate.function,
                AggregateFunction::GroupConcat
                    | AggregateFunction::JsonArrayAgg
                    | AggregateFunction::JsonObjectAgg
            )
        })
}

/// Per query: how many aggregate-batches folded by column, and how many
/// fell back to the per-row update. Recorded on the profile.
#[derive(Default)]
pub(super) struct FoldTally {
    pub(super) folded: usize,
    pub(super) per_row: usize,
}

/// Folds `batch`'s selected rows into `states`, one per aggregate.
pub(super) fn fold_batch(
    batch: &RecordBatch,
    aggregates: &[CompiledAggregate],
    states: &mut [AggregateState],
    rows_buffer: &mut Vec<u32>,
    tally: &mut FoldTally,
    memory: &MemoryTracker,
) -> Result<(), ExecError> {
    let rows = fold_rows(batch, 0..batch.row_count(), rows_buffer);
    if rows.len() == 0 {
        return Ok(());
    }
    let batch_bytes = batch.estimated_bytes();
    for (aggregate, state) in aggregates.iter().zip(states.iter_mut()) {
        if fold_column(batch, &rows, aggregate, state, memory)? {
            tally.folded += 1;
            continue;
        }
        tally.per_row += 1;
        for row in batch.selection().selected_rows() {
            update_aggregate_states(
                batch,
                row,
                batch_bytes,
                std::slice::from_ref(aggregate),
                std::slice::from_mut(state),
                memory,
            )?;
        }
    }
    Ok(())
}

/// The rows of `rows` whose `validity` bit is set.
fn valid_count(rows: &FoldRows<'_>, validity: &ValidityMask) -> u64 {
    let count = if validity.no_nulls() {
        rows.len()
    } else {
        match rows {
            FoldRows::Span(span) => span.clone().filter(|row| validity.is_valid(*row)).count(),
            FoldRows::Picked(picked) => picked
                .iter()
                .filter(|row| validity.is_valid(**row as usize))
                .count(),
        }
    };
    count as u64
}

/// Calls `each` with every valid row of `rows` and its value, in row order.
#[inline]
fn for_valid<T: Copy>(
    rows: &FoldRows<'_>,
    values: &[T],
    validity: &ValidityMask,
    mut each: impl FnMut(usize, T),
) {
    match rows {
        FoldRows::Span(span) => {
            if validity.no_nulls() {
                for (row, value) in span.clone().zip(&values[span.clone()]) {
                    each(row, *value);
                }
            } else {
                for row in span.clone() {
                    if validity.is_valid(row) {
                        each(row, values[row]);
                    }
                }
            }
        }
        FoldRows::Picked(picked) => {
            if validity.no_nulls() {
                for &row in *picked {
                    let row = row as usize;
                    each(row, values[row]);
                }
            } else {
                for &row in *picked {
                    let row = row as usize;
                    if validity.is_valid(row) {
                        each(row, values[row]);
                    }
                }
            }
        }
    }
}

/// The sum of the valid rows' units and how many there were.
fn unit_total(rows: &FoldRows<'_>, units: &[i64], validity: &ValidityMask) -> (i128, u64) {
    let mut total = 0_i128;
    let mut count = 0_u64;
    // `i64` units cannot carry an `i128` total out of range before 2^64
    // rows, so the add needs no check.
    for_valid(rows, units, validity, |_, value| {
        total = total.wrapping_add(i128::from(value));
        count += 1;
    });
    (total, count)
}

/// The first row, in row order, holding the least (`least`) or greatest
/// value: the row the per-row update would keep, since it replaces only on
/// a strictly better value.
fn extreme_row<T: Copy + Ord>(
    rows: &FoldRows<'_>,
    values: &[T],
    validity: &ValidityMask,
    least: bool,
) -> Option<(usize, T)> {
    let mut best: Option<(usize, T)> = None;
    for_valid(rows, values, validity, |row, value| {
        let better = match best {
            None => true,
            Some((_, current)) => {
                if least {
                    value < current
                } else {
                    value > current
                }
            }
        };
        if better {
            best = Some((row, value));
        }
    });
    best
}

/// Folds one aggregate's column over `rows`, or `false` with nothing
/// applied when the column or the function has no fold here.
#[allow(clippy::too_many_lines)]
fn fold_column(
    batch: &RecordBatch,
    rows: &FoldRows<'_>,
    aggregate: &CompiledAggregate,
    state: &mut AggregateState,
    memory: &MemoryTracker,
) -> Result<bool, ExecError> {
    if aggregate.distinct {
        return Ok(false);
    }
    let Some(expression) = &aggregate.expr else {
        if aggregate.function == AggregateFunction::Count {
            state.add_dense_count(rows.len() as u64)?;
            return Ok(true);
        }
        return Ok(false);
    };
    let Some(column) = expression
        .column_index()
        .and_then(|column| batch.column(column))
    else {
        return Ok(false);
    };
    // Only a plain integer column folds as an integer: a YEAR or a BIT
    // stored as one keeps the per-row update's own reading of it.
    let plain_integer = matches!(
        column.data_type(),
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
    );
    let Some((typed, validity)) = column.typed() else {
        return Ok(false);
    };
    match (aggregate.function, typed) {
        (AggregateFunction::Count, _) => {
            state.add_dense_count(valid_count(rows, validity))?;
            Ok(true)
        }
        (
            AggregateFunction::Sum,
            TypedValues::Decimal128 {
                values: DecimalUnits::Narrow(units),
                scale,
                ..
            },
        ) => {
            let (total, count) = unit_total(rows, units, validity);
            if count > 0 {
                state.update_decimal_sum_units(total, *scale, aggregate_uses_float(aggregate))?;
            }
            Ok(true)
        }
        (
            AggregateFunction::Average,
            TypedValues::Decimal128 {
                values: DecimalUnits::Narrow(units),
                scale,
                ..
            },
        ) => {
            let Some(result_scale) = decimal_average_scale(aggregate) else {
                return Ok(false);
            };
            let Some(digits) = result_scale
                .checked_sub(*scale)
                .filter(|digits| *digits <= AVERAGE_MAX_DIGITS)
            else {
                return Ok(false);
            };
            let (total, count) = unit_total(rows, units, validity);
            if count > 0 {
                state.add_decimal_average_partial(total, digits, result_scale, count)?;
            }
            Ok(true)
        }
        // An integer SUM typed as its own column type keeps the per-row
        // update's checked integer total. A batch whose own total leaves
        // the type is handed back, so the per-row update decides it.
        (AggregateFunction::Sum, TypedValues::Int64(values))
            if plain_integer && aggregate.data_type == Some(DataType::Int64) =>
        {
            let mut total = 0_i64;
            let mut count = 0_u64;
            let mut overflow = false;
            for_valid(rows, values, validity, |_, value| {
                match total.checked_add(value) {
                    Some(sum) => total = sum,
                    None => overflow = true,
                }
                count += 1;
            });
            if overflow {
                return Ok(false);
            }
            if count > 0 {
                state.add_dense_signed(total)?;
            }
            Ok(true)
        }
        (AggregateFunction::Sum, TypedValues::UInt64(values))
            if plain_integer && aggregate.data_type == Some(DataType::UInt64) =>
        {
            let mut total = 0_u64;
            let mut count = 0_u64;
            let mut overflow = false;
            for_valid(rows, values, validity, |_, value| {
                match total.checked_add(value) {
                    Some(sum) => total = sum,
                    None => overflow = true,
                }
                count += 1;
            });
            if overflow {
                return Ok(false);
            }
            if count > 0 {
                state.add_dense_unsigned(total)?;
            }
            Ok(true)
        }
        // MIN/MAX over scaled decimal or temporal units: the per-row update
        // compares the same units and formats the winning row's text.
        (
            AggregateFunction::Minimum | AggregateFunction::Maximum,
            TypedValues::Decimal128 {
                values: DecimalUnits::Narrow(units),
                ..
            }
            | TypedValues::Temporal { units, .. },
        ) => {
            let least = aggregate.function == AggregateFunction::Minimum;
            if let Some((row, units)) = extreme_row(rows, units, validity, least) {
                state.update_extreme_units(
                    aggregate,
                    i128::from(units),
                    || typed.format_unit(row),
                    memory,
                )?;
            }
            Ok(true)
        }
        // MIN/MAX over integers: the per-row update retains the integer
        // value with its f64 as the comparison hint.
        (AggregateFunction::Minimum | AggregateFunction::Maximum, TypedValues::Int64(values))
            if plain_integer =>
        {
            let least = aggregate.function == AggregateFunction::Minimum;
            if let Some((_, value)) = extreme_row(rows, values, validity, least) {
                #[allow(clippy::cast_precision_loss)]
                let number = value as f64;
                state.update_with_number(aggregate, &Value::Int64(value), Some(number), memory)?;
            }
            Ok(true)
        }
        (AggregateFunction::Minimum | AggregateFunction::Maximum, TypedValues::UInt64(values))
            if plain_integer =>
        {
            let least = aggregate.function == AggregateFunction::Minimum;
            if let Some((_, value)) = extreme_row(rows, values, validity, least) {
                #[allow(clippy::cast_precision_loss)]
                let number = value as f64;
                state.update_with_number(aggregate, &Value::UInt64(value), Some(number), memory)?;
            }
            Ok(true)
        }
        _ => Ok(false),
    }
}
