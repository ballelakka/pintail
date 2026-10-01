//! A columnar fold for the fused inner join-aggregate's commonest shape: a
//! build key that is dense and unique (an auto-increment primary key), and
//! aggregates that are counts and exact decimal sums or averages of probe
//! columns.
//!
//! The row fold resolved each probe row's group through two indirections -
//! the dense slot, then that bucket's list of group indexes - and updated
//! every aggregate through the general typed-column path, which looks the
//! column up, matches its representation and dispatches on the aggregate
//! function once per row per aggregate. That update was a quarter of the
//! whole profile of a star join grouped by a dimension column. Here a key
//! resolves to its group with one array index, each aggregate folds into
//! plain per-group integers, and the states see one update per group per
//! morsel. Every fold here is exact integer addition, so the states end
//! where the row fold leaves them.

use pintail_sql::AggregateFunction;

use super::aggregate::{
    AggregateGroup, CompiledAggregate, aggregate_uses_float, decimal_average_scale,
};
use super::join::PartitionedBuild;
use super::morsel::Morsel;
use super::{ExecError, MemoryTracker};
use crate::array::ValidityMask;
use crate::batch::{DecimalUnits, TypedValues};
use crate::expression::CompiledExpr;

/// Probe rows resolved before the aggregates fold them: small enough that
/// the row and group buffers stay in L1.
const CHUNK_ROWS: usize = 2_048;

/// Widest decimal-average widening folded as one partial total; the same
/// bound the packed aggregate lanes use.
const AVERAGE_MAX_DIGITS: u8 = 19;

/// Each dense key slot's one group, for a build whose every key names
/// exactly one row.
///
/// A slot without a key - and any probe row that matches nothing - resolves
/// to the discard group one past the real ones, so the fold adds every row
/// somewhere and needs no branch to skip the misses.
pub(super) struct UniqueKeyGroups {
    minimum: i128,
    groups: Vec<u32>,
    discard: u32,
}

impl UniqueKeyGroups {
    /// `None` unless the build finalized to a dense table and every bucket
    /// holds one row: a key with several rows folds each of them, which is
    /// the row fold's job.
    pub(super) fn resolve(
        build: &PartitionedBuild,
        dense_group_indexes: &[Option<&[usize]>],
        group_count: usize,
    ) -> Option<Self> {
        let (minimum, index) = build.dense_layout()?;
        let discard = u32::try_from(group_count)
            .ok()
            .filter(|discard| *discard < u32::MAX)?;
        let mut groups = Vec::with_capacity(index.len());
        for slot in index {
            let group = match slot {
                None => discard,
                Some(flat) => match dense_group_indexes.get(*flat).copied().flatten() {
                    None | Some([]) => discard,
                    Some([group]) => u32::try_from(*group).ok().filter(|g| *g < discard)?,
                    Some(_) => return None,
                },
            };
            groups.push(group);
        }
        Some(Self {
            minimum,
            groups,
            discard,
        })
    }

    /// Bytes the slot array holds.
    pub(super) fn bytes(&self) -> usize {
        self.groups.len().saturating_mul(size_of::<u32>())
    }

    #[inline]
    fn group(&self, key: i128) -> u32 {
        usize::try_from(key.wrapping_sub(self.minimum))
            .ok()
            .and_then(|offset| self.groups.get(offset).copied())
            .unwrap_or(self.discard)
    }

    /// Each value's group into `out`, NULL keys to the discard group.
    #[inline]
    fn resolve_into<T: Copy>(&self, values: &[T], valid: Option<&[bool]>, out: &mut Vec<u32>)
    where
        i128: From<T>,
    {
        out.clear();
        out.extend(values.iter().map(|value| self.group(i128::from(*value))));
        if let Some(valid) = valid {
            for (group, valid) in out.iter_mut().zip(valid) {
                if !valid {
                    *group = self.discard;
                }
            }
        }
    }
}

/// How one aggregate folds, decided once per query.
#[derive(Clone, Copy)]
pub(super) enum Lane {
    /// `COUNT(*)`: the group's matched rows.
    CountRows,
    /// `COUNT(column)`: the matched rows where the column is not NULL.
    CountValid { column: usize },
    /// `SUM(column)` of a decimal column, on its scaled units.
    DecimalSum { column: usize, float_output: bool },
    /// Exact `AVG(column)` of a decimal column at `result_scale`.
    DecimalAverage { column: usize, result_scale: u8 },
}

/// The lane of every aggregate, or `None` when any of them needs the row
/// fold: a DISTINCT, a build-side argument, or a function without a lane.
pub(super) fn plan_lanes(aggregates: &[CompiledAggregate], left_width: usize) -> Option<Vec<Lane>> {
    aggregates
        .iter()
        .map(|aggregate| {
            if aggregate.distinct {
                return None;
            }
            let column = match &aggregate.expr {
                None => None,
                Some(expression) => Some(expression.column_index().filter(|c| *c < left_width)?),
            };
            match (aggregate.function, column) {
                (AggregateFunction::Count, None) => Some(Lane::CountRows),
                (AggregateFunction::Count, Some(column)) => Some(Lane::CountValid { column }),
                (AggregateFunction::Sum, Some(column)) => Some(Lane::DecimalSum {
                    column,
                    float_output: aggregate_uses_float(aggregate),
                }),
                (AggregateFunction::Average, Some(column)) => {
                    decimal_average_scale(aggregate).map(|result_scale| Lane::DecimalAverage {
                        column,
                        result_scale,
                    })
                }
                _ => None,
            }
        })
        .collect()
}

/// One lane's column in this morsel's batch.
enum LaneInput<'a> {
    Rows,
    Valid(&'a ValidityMask),
    Units {
        units: &'a [i64],
        validity: &'a ValidityMask,
        scale: u8,
    },
}

/// The probe key column as integers, when it is one.
enum Keys<'a> {
    Signed(&'a [i64]),
    Unsigned(&'a [u64]),
}

impl Keys<'_> {
    /// The groups of `rows` - a contiguous range or picked rows - into `out`.
    fn resolve(
        &self,
        table: &UniqueKeyGroups,
        rows: &Rows<'_>,
        valid: Option<&[bool]>,
        scratch: &mut Vec<i128>,
        out: &mut Vec<u32>,
    ) {
        match (self, rows) {
            (Self::Signed(values), Rows::Range(range)) => {
                table.resolve_into(&values[range.clone()], valid, out);
            }
            (Self::Unsigned(values), Rows::Range(range)) => {
                table.resolve_into(&values[range.clone()], valid, out);
            }
            (Self::Signed(values), Rows::Picked(rows)) => {
                scratch.clear();
                scratch.extend(rows.iter().map(|row| i128::from(values[*row as usize])));
                table.resolve_into(scratch, valid, out);
            }
            (Self::Unsigned(values), Rows::Picked(rows)) => {
                scratch.clear();
                scratch.extend(rows.iter().map(|row| i128::from(values[*row as usize])));
                table.resolve_into(scratch, valid, out);
            }
        }
    }
}

/// The rows of one chunk.
enum Rows<'a> {
    Range(std::ops::Range<usize>),
    Picked(&'a [u32]),
}

impl Rows<'_> {
    /// Each row's validity under `mask`, or `None` when every row is valid.
    fn validity(&self, mask: &ValidityMask, out: &mut Vec<bool>) -> bool {
        if mask.no_nulls() {
            return false;
        }
        out.clear();
        match self {
            Self::Range(range) => out.extend(range.clone().map(|row| mask.is_valid(row))),
            Self::Picked(rows) => out.extend(rows.iter().map(|row| mask.is_valid(*row as usize))),
        }
        true
    }
}

/// Adds each row's units into its group's total.
#[inline]
fn add_units(totals: &mut [i128], groups: &[u32], units: &[i64], rows: &Rows<'_>) {
    match rows {
        Rows::Range(range) => {
            for (group, units) in groups.iter().zip(&units[range.clone()]) {
                totals[*group as usize] += i128::from(*units);
            }
        }
        Rows::Picked(rows) => {
            for (group, row) in groups.iter().zip(*rows) {
                totals[*group as usize] += i128::from(units[*row as usize]);
            }
        }
    }
}

/// Folds `morsel` into `groups` through the unique-key table, marking each
/// group a probe row reached in `touched`. `false`, with nothing folded,
/// when a column of this batch is not in a representation a lane reads -
/// the row fold then takes the morsel.
#[allow(clippy::too_many_lines)]
pub(super) fn fold_morsel(
    morsel: &Morsel<'_>,
    left_key: &CompiledExpr,
    keys: &UniqueKeyGroups,
    lanes: &[Lane],
    groups: &mut [AggregateGroup],
    touched: &mut [bool],
    memory: &MemoryTracker,
) -> Result<bool, ExecError> {
    let batch = morsel.batch;
    let Some((key_values, key_validity)) = left_key
        .column_index()
        .and_then(|column| batch.column(column))
        .and_then(crate::ColumnVector::typed)
    else {
        return Ok(false);
    };
    let key_values = match key_values {
        TypedValues::Int64(values) => Keys::Signed(values),
        TypedValues::UInt64(values) => Keys::Unsigned(values),
        _ => return Ok(false),
    };
    let mut inputs = Vec::with_capacity(lanes.len());
    for lane in lanes {
        let input = match *lane {
            Lane::CountRows => LaneInput::Rows,
            Lane::CountValid { column } => {
                let Some((_, validity)) = batch.column(column).and_then(crate::ColumnVector::typed)
                else {
                    return Ok(false);
                };
                LaneInput::Valid(validity)
            }
            Lane::DecimalSum { column, .. } | Lane::DecimalAverage { column, .. } => {
                match batch.column(column).and_then(crate::ColumnVector::typed) {
                    Some((
                        TypedValues::Decimal128 {
                            values: DecimalUnits::Narrow(units),
                            scale,
                            ..
                        },
                        validity,
                    )) => LaneInput::Units {
                        units,
                        validity,
                        scale: *scale,
                    },
                    _ => return Ok(false),
                }
            }
        };
        if let (Lane::DecimalAverage { result_scale, .. }, LaneInput::Units { scale, .. }) =
            (lane, &input)
            && result_scale
                .checked_sub(*scale)
                .is_none_or(|digits| digits > AVERAGE_MAX_DIGITS)
        {
            return Ok(false);
        }
        inputs.push(input);
    }

    let group_count = groups.len();
    // One slot more than the groups: the discard group the misses land in.
    let slots = group_count + 1;
    let mut hits = vec![0_u64; slots];
    let mut totals = vec![0_i128; slots.saturating_mul(lanes.len())];
    let mut null_rows = vec![0_u64; slots.saturating_mul(lanes.len())];
    let mut chunk_groups = Vec::<u32>::with_capacity(CHUNK_ROWS);
    let mut lane_groups = Vec::<u32>::with_capacity(CHUNK_ROWS);
    let mut picked = Vec::<u32>::with_capacity(CHUNK_ROWS);
    let mut valid = Vec::<bool>::with_capacity(CHUNK_ROWS);
    let mut scratch = Vec::<i128>::new();
    let contiguous = morsel.selected_count() == morsel.rows.len();
    let mut selected = morsel.selected_rows();
    let mut next = morsel.rows.start;
    loop {
        memory.check_interruption()?;
        let rows = if contiguous {
            let end = next.saturating_add(CHUNK_ROWS).min(morsel.rows.end);
            if next >= end {
                break;
            }
            let range = next..end;
            next = end;
            Rows::Range(range)
        } else {
            picked.clear();
            for row in selected.by_ref().take(CHUNK_ROWS) {
                picked.push(u32::try_from(row).map_err(|_| {
                    ExecError::InvalidBatch("a probe batch holds more rows than a join can address")
                })?);
            }
            if picked.is_empty() {
                break;
            }
            Rows::Picked(&picked)
        };
        let key_valid = rows.validity(key_validity, &mut valid);
        key_values.resolve(
            keys,
            &rows,
            key_valid.then_some(valid.as_slice()),
            &mut scratch,
            &mut chunk_groups,
        );
        for group in &chunk_groups {
            hits[*group as usize] += 1;
        }
        for (lane, input) in inputs.iter().enumerate() {
            let lane_slots = lane * slots..(lane + 1) * slots;
            let (validity, units) = match input {
                LaneInput::Rows => continue,
                LaneInput::Valid(validity) => (*validity, None),
                LaneInput::Units {
                    units, validity, ..
                } => (*validity, Some(*units)),
            };
            // A NULL argument's row moves to the discard group for this
            // lane alone, and is counted against its group's valid rows;
            // the other lanes still see it.
            let groups = if rows.validity(validity, &mut valid) {
                let nulls = &mut null_rows[lane_slots.clone()];
                lane_groups.clear();
                for (group, valid) in chunk_groups.iter().zip(&valid) {
                    if *valid {
                        lane_groups.push(*group);
                    } else {
                        nulls[*group as usize] += 1;
                        lane_groups.push(keys.discard);
                    }
                }
                &lane_groups
            } else {
                &chunk_groups
            };
            if let Some(units) = units {
                add_units(&mut totals[lane_slots], groups, units, &rows);
            }
        }
    }

    for (group_index, rows) in hits.iter().take(group_count).enumerate() {
        if *rows == 0 {
            continue;
        }
        touched[group_index] = true;
        let states = &mut groups[group_index].states;
        for (lane_index, ((lane, input), state)) in
            lanes.iter().zip(&inputs).zip(states.iter_mut()).enumerate()
        {
            let total = totals[lane_index * slots + group_index];
            let valid = *rows - null_rows[lane_index * slots + group_index];
            match (*lane, input) {
                (Lane::CountRows, _) => state.add_dense_count(*rows)?,
                (Lane::CountValid { .. }, _) => state.add_dense_count(valid)?,
                (Lane::DecimalSum { float_output, .. }, LaneInput::Units { scale, .. }) => {
                    if valid > 0 {
                        state.update_decimal_sum_units(total, *scale, float_output)?;
                    }
                }
                (Lane::DecimalAverage { result_scale, .. }, LaneInput::Units { scale, .. }) => {
                    if valid > 0 {
                        state.add_decimal_average_partial(
                            total,
                            result_scale - *scale,
                            result_scale,
                            valid,
                        )?;
                    }
                }
                _ => {
                    return Err(ExecError::InvalidPhysicalPlan(
                        "a fused join lane lost its column",
                    ));
                }
            }
        }
    }
    Ok(true)
}
