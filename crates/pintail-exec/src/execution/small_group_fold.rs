//! A one-key GROUP BY with few groups, folded a column at a time per group.
//!
//! A grouped aggregate whose aggregates have no two-pass lane - a MIN or MAX
//! of a DATETIME is one - or whose key is an expression such as
//! `DATE(occurred_at)` ran the general row loop: per selected row it built
//! the key as a `Value`, normalized it, probed the group map and updated
//! every aggregate through the general update. Over a recent time window
//! that was 75 to 120 ns a row, and the window's few groups made all of it
//! overhead.
//!
//! Here each batch's selected rows are resolved to their group - through
//! the dictionary codes of a text key, or the units of a temporal or
//! integer key, resolving each distinct key once per batch - then ordered
//! by group, and every group's rows fold column-at-a-time exactly as an
//! ungrouped aggregate's do. Groups are keyed by the same normalized value
//! the general path uses, and the first row of a group in row order supplies
//! its key value, so the groups and their keys are the general path's.

use std::collections::HashMap;
use std::hash::BuildHasherDefault;

use pintail_types::Value;

use super::aggregate::{AggregateGroup, AggregateState, CompiledAggregate, GroupKeyHasher};
use super::join::normalized_group_hash_key;
use super::packed_fold::FoldRows;
use super::ungrouped_fold::{FoldTally, eligible, fold_rows_into};
use super::{
    ExecError, HASH_ENTRY_OVERHEAD, MaterializedRows, MemoryTracker, PullOperator,
    estimated_row_payload_bytes,
};
use crate::ColumnVector;
use crate::RecordBatch;
use crate::array::ValidityMask;
use crate::batch::TypedValues;
use crate::collation::Collation;
use crate::expression::CompiledExpr;

/// Most distinct keys the first batch may show for the fold to be chosen.
/// Every group pays a fold call per aggregate per batch, so the fold earns
/// its keep only while groups hold many rows each.
const MAX_FIRST_BATCH_GROUPS: usize = 256;
/// Rows the first batch must hold per distinct key.
const MIN_ROWS_PER_GROUP: usize = 64;

/// A key column for one batch: borrowed when the key is a column, computed
/// when it is an expression with a column kernel.
enum KeyColumn<'a> {
    Borrowed(&'a ColumnVector),
    Owned(ColumnVector),
}

impl KeyColumn<'_> {
    fn get(&self) -> &ColumnVector {
        match self {
            Self::Borrowed(column) => column,
            Self::Owned(column) => column,
        }
    }
}

/// The type an expression key evaluates to, which its column kernels
/// require; `None` for a node that declares none.
fn declared_type(key: &CompiledExpr) -> Option<pintail_types::DataType> {
    match key {
        CompiledExpr::Unary { data_type, .. }
        | CompiledExpr::Binary { data_type, .. }
        | CompiledExpr::Scalar { data_type, .. } => *data_type,
        _ => None,
    }
}

fn key_column<'a>(key: &CompiledExpr, batch: &'a RecordBatch) -> Option<KeyColumn<'a>> {
    match key.column_index() {
        Some(index) => batch.column(index).map(KeyColumn::Borrowed),
        None => key
            .evaluate_column(batch, declared_type(key))
            .map(KeyColumn::Owned),
    }
}

/// Whether the fold suits this query, judged on its first batch: one key
/// with a column form, aggregates the fold takes, and few distinct keys
/// over many rows.
pub(super) fn suits(
    group_by: &[CompiledExpr],
    aggregates: &[CompiledAggregate],
    first: &RecordBatch,
) -> bool {
    let [key] = group_by else {
        return false;
    };
    if !eligible(aggregates) || key.has_variable_effects() {
        return false;
    }
    // An expression key must have a packed kernel: one read row by row
    // builds its column from values and then parses them back, which costs
    // more than the general path it would replace.
    let column = match key.column_index() {
        Some(index) => first.column(index).map(KeyColumn::Borrowed),
        None => key
            .evaluate_vector_column_quietly(first, declared_type(key))
            .map(KeyColumn::Owned),
    };
    let Some(column) = column else {
        return false;
    };
    let column = column.get();
    let rows = first.visible_row_count();
    let limit = MAX_FIRST_BATCH_GROUPS.min(rows / MIN_ROWS_PER_GROUP);
    if limit == 0 {
        return false;
    }
    let Some((typed, validity)) = column.typed() else {
        return false;
    };
    let mut seen = std::collections::HashSet::<u64>::new();
    let mut distinct = |bits: u64| {
        seen.insert(bits);
        seen.len() <= limit
    };
    match typed {
        TypedValues::Utf8(strings) => match strings.dictionary() {
            Some((codes, _)) => first
                .selection()
                .selected_rows()
                .all(|row| !validity.is_valid(row) || distinct(u64::from(codes[row]))),
            None => false,
        },
        TypedValues::Temporal { units, text } if text.derived() => first
            .selection()
            .selected_rows()
            .all(|row| !validity.is_valid(row) || distinct(units[row].cast_unsigned())),
        TypedValues::Int64(values) => first
            .selection()
            .selected_rows()
            .all(|row| !validity.is_valid(row) || distinct(values[row].cast_unsigned())),
        TypedValues::UInt64(values) => first
            .selection()
            .selected_rows()
            .all(|row| !validity.is_valid(row) || distinct(values[row])),
        _ => false,
    }
}

/// The groups so far, keyed as the general path keys them.
struct Groups {
    groups: Vec<AggregateGroup>,
    index: HashMap<Value, u32>,
}

impl Groups {
    /// The group of a row whose key value is `value`, made when new.
    fn resolve(
        &mut self,
        value: Value,
        collation: Collation,
        aggregates: &[CompiledAggregate],
        memory: &MemoryTracker,
    ) -> Result<u32, ExecError> {
        let normalized = normalized_group_hash_key(value.clone(), collation).unwrap_or(Value::Null);
        if let Some(group) = self.index.get(&normalized) {
            return Ok(*group);
        }
        let values = vec![value];
        let bytes = estimated_row_payload_bytes(&values)
            .saturating_add(normalized.heap_bytes())
            .saturating_add(size_of::<Value>().saturating_mul(2))
            .saturating_add(size_of::<AggregateGroup>())
            .saturating_add(aggregates.len().saturating_mul(size_of::<AggregateState>()))
            .saturating_add(HASH_ENTRY_OVERHEAD);
        memory.reserve(bytes)?;
        let group = u32::try_from(self.groups.len())
            .map_err(|_| ExecError::InvalidBatch("too many groups for a small-group fold"))?;
        self.groups.push(AggregateGroup {
            values,
            states: aggregates.iter().map(AggregateState::new).collect(),
        });
        self.index.insert(normalized, group);
        Ok(group)
    }
}

type UnitGroups = HashMap<u64, u32, BuildHasherDefault<GroupKeyHasher>>;

/// Each selected row's group, in row order, into `out`.
#[allow(clippy::too_many_arguments)]
fn resolve_batch(
    key: &CompiledExpr,
    batch: &RecordBatch,
    groups: &mut Groups,
    collation: Collation,
    aggregates: &[CompiledAggregate],
    rows: &[u32],
    out: &mut Vec<u32>,
    memory: &MemoryTracker,
) -> Result<(), ExecError> {
    out.clear();
    let column = key_column(key, batch);
    let typed = column
        .as_ref()
        .and_then(|column| column.get().typed().map(|typed| (column.get(), typed)));
    // Resolves `row` once per distinct key: `cached` finds it again.
    let mut by_value = |column: &ColumnVector, row: usize| -> Result<u32, ExecError> {
        let value = column
            .value_owned(row)
            .ok_or(ExecError::InvalidBatch("group key row outside its column"))?;
        groups.resolve(value, collation, aggregates, memory)
    };
    match typed {
        Some((column, (TypedValues::Utf8(strings), validity)))
            if strings.dictionary().is_some() =>
        {
            let (codes, dictionary) = strings.dictionary().expect("checked above");
            let mut by_code = vec![u32::MAX; dictionary.len()];
            let mut null = None;
            for &row in rows {
                let row = row as usize;
                let group = if validity.is_valid(row) {
                    let code = codes[row] as usize;
                    if by_code[code] == u32::MAX {
                        by_code[code] = by_value(column, row)?;
                    }
                    by_code[code]
                } else {
                    resolve_null(&mut null, column, row, &mut by_value)?
                };
                out.push(group);
            }
        }
        Some((column, (TypedValues::Temporal { units, text }, validity))) if text.derived() => {
            by_bits(
                column,
                validity,
                rows,
                out,
                |row| units[row].cast_unsigned(),
                &mut by_value,
            )?;
        }
        Some((column, (TypedValues::Int64(values), validity))) => {
            by_bits(
                column,
                validity,
                rows,
                out,
                |row| values[row].cast_unsigned(),
                &mut by_value,
            )?;
        }
        Some((column, (TypedValues::UInt64(values), validity))) => {
            by_bits(
                column,
                validity,
                rows,
                out,
                |row| values[row],
                &mut by_value,
            )?;
        }
        Some((column, _)) => {
            for &row in rows {
                out.push(by_value(column, row as usize)?);
            }
        }
        None => {
            // No column form for this batch: the key row by row, as the
            // general path evaluates it.
            for &row in rows {
                let value = key.evaluate(batch, row as usize)?;
                out.push(groups.resolve(value, collation, aggregates, memory)?);
            }
        }
    }
    Ok(())
}

fn resolve_null(
    null: &mut Option<u32>,
    column: &ColumnVector,
    row: usize,
    by_value: &mut impl FnMut(&ColumnVector, usize) -> Result<u32, ExecError>,
) -> Result<u32, ExecError> {
    if let Some(group) = *null {
        return Ok(group);
    }
    let group = by_value(column, row)?;
    *null = Some(group);
    Ok(group)
}

/// Groups for a key whose packed bits identify its value: the previous
/// row's key first (a window ordered by time repeats it), then a map.
fn by_bits(
    column: &ColumnVector,
    validity: &ValidityMask,
    rows: &[u32],
    out: &mut Vec<u32>,
    bits: impl Fn(usize) -> u64,
    by_value: &mut impl FnMut(&ColumnVector, usize) -> Result<u32, ExecError>,
) -> Result<(), ExecError> {
    let mut map = UnitGroups::default();
    let mut last: Option<(u64, u32)> = None;
    let mut null = None;
    for &row in rows {
        let row = row as usize;
        if !validity.is_valid(row) {
            out.push(resolve_null(&mut null, column, row, by_value)?);
            continue;
        }
        let key = bits(row);
        let group = match last {
            Some((previous, group)) if previous == key => group,
            _ => {
                let group = if let Some(group) = map.get(&key) {
                    *group
                } else {
                    let group = by_value(column, row)?;
                    map.insert(key, group);
                    group
                };
                last = Some((key, group));
                group
            }
        };
        out.push(group);
    }
    Ok(())
}

/// Folds every batch of `input`, `first` included, by group.
#[allow(clippy::too_many_arguments)]
pub(super) fn build_small_group_fold(
    input: &mut PullOperator,
    first: RecordBatch,
    key: &CompiledExpr,
    aggregates: &[CompiledAggregate],
    memory: &MemoryTracker,
    key_collation: Collation,
) -> Result<MaterializedRows, ExecError> {
    let mut groups = Groups {
        groups: Vec::new(),
        index: HashMap::new(),
    };
    let mut tally = FoldTally::default();
    let mut selected = Vec::<u32>::new();
    let mut row_groups = Vec::<u32>::new();
    let mut ordered = Vec::<u32>::new();
    let mut offsets = Vec::<usize>::new();
    let mut next = Some(first);
    while let Some(batch) = match next.take() {
        Some(batch) => Some(batch),
        None => input.next_batch(memory)?,
    } {
        memory.check_interruption()?;
        selected.clear();
        for row in batch.selection().selected_rows() {
            selected.push(
                u32::try_from(row)
                    .map_err(|_| ExecError::InvalidBatch("a batch row past u32 for a fold"))?,
            );
        }
        if selected.is_empty() {
            continue;
        }
        resolve_batch(
            key,
            &batch,
            &mut groups,
            key_collation,
            aggregates,
            &selected,
            &mut row_groups,
            memory,
        )?;
        // A counting sort by group keeps each group's rows in row order.
        let group_count = groups.groups.len();
        offsets.clear();
        offsets.resize(group_count + 1, 0);
        for group in &row_groups {
            offsets[*group as usize + 1] += 1;
        }
        for group in 0..group_count {
            offsets[group + 1] += offsets[group];
        }
        ordered.clear();
        ordered.resize(selected.len(), 0);
        let mut cursor = offsets.clone();
        for (row, group) in selected.iter().zip(&row_groups) {
            let slot = &mut cursor[*group as usize];
            ordered[*slot] = *row;
            *slot += 1;
        }
        for group in 0..group_count {
            let range = offsets[group]..offsets[group + 1];
            if range.is_empty() {
                continue;
            }
            fold_rows_into(
                &batch,
                &FoldRows::Picked(&ordered[range]),
                aggregates,
                &mut groups.groups[group].states,
                &mut tally,
                memory,
            )?;
        }
    }
    super::ProfileNote::of(input).set(&format!(
        "small-group column fold: {} groups, {} aggregate-batches by column, {} per row",
        groups.groups.len(),
        tally.folded,
        tally.per_row
    ));
    let mut rows = Vec::with_capacity(groups.groups.len());
    for group in groups.groups {
        let mut row = group.values;
        row.reserve(group.states.len());
        for state in group.states {
            row.push(state.finish(memory)?);
        }
        memory.reserve(estimated_row_payload_bytes(&row))?;
        rows.push(row);
    }
    Ok(MaterializedRows {
        rows,
        position: 0,
        spilled: None,
        ready: None,
    })
}
