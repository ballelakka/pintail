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
//!
//! When every aggregate merges exactly, batches are held in a window and
//! folded on the pool: each row range builds its own groups, and the
//! partials merge into the query's groups in row order. Row order is what
//! keeps the answer the serial one: a group's key comes from its first row,
//! and a MIN or MAX merge keeps the earlier of two equal values, as the
//! serial fold keeps the first row holding it. A float sum or average, a
//! variance and a DISTINCT aggregate stay serial, because their partials
//! would add in another order or merge through another path.

use std::collections::HashMap;
use std::hash::BuildHasherDefault;

use pintail_sql::AggregateFunction;
use pintail_types::Value;
use rayon::prelude::*;

use super::aggregate::{
    AggregateGroup, AggregateState, CompiledAggregate, GroupKeyHasher, aggregate_uses_float,
};
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
/// Batches a parallel window holds per pool thread before it folds.
const WINDOW_BATCHES_PER_THREAD: usize = 2;

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

/// [`key_column`] on a pool thread: an expression key that raised a
/// warning, or has no packed kernel, answers `None` and is evaluated again
/// on the query's thread, where what it raises is recorded.
fn key_column_quietly<'a>(key: &CompiledExpr, batch: &'a RecordBatch) -> Option<KeyColumn<'a>> {
    match key.column_index() {
        Some(index) => batch.column(index).map(KeyColumn::Borrowed),
        None => key
            .evaluate_vector_column_quietly(batch, declared_type(key))
            .map(KeyColumn::Owned),
    }
}

/// Whether the fold suits this query, judged on its first batch: one key
/// with a column form, aggregates the fold takes, and few distinct keys
/// over many rows. A refusal names its reason for the profile.
pub(super) fn suits(
    group_by: &[CompiledExpr],
    aggregates: &[CompiledAggregate],
    first: &RecordBatch,
) -> Result<(), &'static str> {
    let [key] = group_by else {
        return Err("more than one key");
    };
    if !eligible(aggregates) {
        return Err("an aggregate that collects values");
    }
    if key.has_variable_effects() {
        return Err("a key that assigns a variable");
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
        return Err("the key has no packed kernel");
    };
    let column = column.get();
    let rows = first.visible_row_count();
    let limit = MAX_FIRST_BATCH_GROUPS.min(rows / MIN_ROWS_PER_GROUP);
    if limit == 0 {
        return Err("too few rows in the first batch");
    }
    let Some((typed, validity)) = column.typed() else {
        return Err("the key column is not typed");
    };
    let mut seen = std::collections::HashSet::<u64>::new();
    let mut distinct = |bits: u64| {
        seen.insert(bits);
        seen.len() <= limit
    };
    let few = match typed {
        TypedValues::Utf8(strings) => match strings.dictionary() {
            Some((codes, _)) => first
                .selection()
                .selected_rows()
                .all(|row| !validity.is_valid(row) || distinct(u64::from(codes[row]))),
            None => return Err("a text key without dictionary codes"),
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
        _ => return Err("a key type without packed bits"),
    };
    if few {
        Ok(())
    } else {
        Err("too many distinct keys in the first batch")
    }
}

/// Whether partial states of every aggregate merge into the serial answer
/// when merged in row order.
fn merges_exactly(aggregates: &[CompiledAggregate]) -> bool {
    aggregates.iter().all(|aggregate| {
        !aggregate.distinct
            && !aggregate_uses_float(aggregate)
            && !matches!(
                aggregate.function,
                AggregateFunction::StdDev { .. } | AggregateFunction::Variance { .. }
            )
    })
}

/// The groups so far, keyed as the general path keys them.
#[allow(clippy::struct_field_names)] // `groups.groups` reads as the list it is
struct Groups {
    groups: Vec<AggregateGroup>,
    index: HashMap<Value, u32>,
    /// Bytes reserved for the groups, handed back when a partial's groups
    /// have merged into the query's.
    reserved: usize,
}

impl Groups {
    fn new() -> Self {
        Self {
            groups: Vec::new(),
            index: HashMap::new(),
            reserved: 0,
        }
    }

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
        self.reserved = self.reserved.saturating_add(bytes);
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

/// Each of `rows`' group, in row order, into `out`. `column` is the key's
/// column form for the batch, `None` to evaluate the key row by row.
#[allow(clippy::too_many_arguments)]
fn resolve_rows(
    key: &CompiledExpr,
    column: Option<&ColumnVector>,
    batch: &RecordBatch,
    groups: &mut Groups,
    collation: Collation,
    aggregates: &[CompiledAggregate],
    rows: &[u32],
    out: &mut Vec<u32>,
    memory: &MemoryTracker,
) -> Result<(), ExecError> {
    out.clear();
    let typed = column.and_then(|column| column.typed().map(|typed| (column, typed)));
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

/// Buffers one fold of a row set reuses.
#[derive(Default)]
struct Scratch {
    selected: Vec<u32>,
    row_groups: Vec<u32>,
    ordered: Vec<u32>,
    offsets: Vec<usize>,
}

/// Folds the selected rows of `batch` within `range` into `groups`.
#[allow(clippy::too_many_arguments)]
fn fold_range(
    key: &CompiledExpr,
    column: Option<&ColumnVector>,
    batch: &RecordBatch,
    range: std::ops::Range<usize>,
    groups: &mut Groups,
    collation: Collation,
    aggregates: &[CompiledAggregate],
    scratch: &mut Scratch,
    tally: &mut FoldTally,
    memory: &MemoryTracker,
) -> Result<(), ExecError> {
    let Scratch {
        selected,
        row_groups,
        ordered,
        offsets,
    } = scratch;
    selected.clear();
    for row in batch.selection().selected_rows_in(range) {
        selected.push(
            u32::try_from(row)
                .map_err(|_| ExecError::InvalidBatch("a batch row past u32 for a fold"))?,
        );
    }
    if selected.is_empty() {
        return Ok(());
    }
    resolve_rows(
        key, column, batch, groups, collation, aggregates, selected, row_groups, memory,
    )?;
    // A counting sort by group keeps each group's rows in row order.
    let group_count = groups.groups.len();
    offsets.clear();
    offsets.resize(group_count + 1, 0);
    for group in row_groups.iter() {
        offsets[*group as usize + 1] += 1;
    }
    for group in 0..group_count {
        offsets[group + 1] += offsets[group];
    }
    ordered.clear();
    ordered.resize(selected.len(), 0);
    let mut cursor = offsets.clone();
    for (row, group) in selected.iter().zip(row_groups.iter()) {
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
            batch,
            &FoldRows::Picked(&ordered[range]),
            aggregates,
            &mut groups.groups[group].states,
            tally,
            memory,
        )?;
    }
    Ok(())
}

/// What a parallel fold has done, for the profile.
#[derive(Default)]
struct Parallelism {
    windows: usize,
    ranges: usize,
    serial_windows: usize,
}

/// Folds a window of batches on the pool and merges the partials into
/// `groups` in row order. A window where some batch's key has no quiet
/// column form folds serially, so what the key raises is recorded once on
/// the query's thread.
#[allow(clippy::too_many_arguments)]
fn fold_window(
    key: &CompiledExpr,
    window: &[RecordBatch],
    groups: &mut Groups,
    collation: Collation,
    aggregates: &[CompiledAggregate],
    scratch: &mut Scratch,
    tally: &mut FoldTally,
    parallelism: &mut Parallelism,
    memory: &MemoryTracker,
) -> Result<(), ExecError> {
    memory.check_interruption()?;
    let columns: Vec<Option<KeyColumn<'_>>> = window
        .par_iter()
        .map(|batch| key_column_quietly(key, batch))
        .collect();
    if columns.iter().any(Option::is_none) {
        parallelism.serial_windows += 1;
        for batch in window {
            let column = key_column(key, batch);
            fold_range(
                key,
                column.as_ref().map(KeyColumn::get),
                batch,
                0..batch.row_count(),
                groups,
                collation,
                aggregates,
                scratch,
                tally,
                memory,
            )?;
        }
        return Ok(());
    }
    let plan = super::morsel::morsel_plan(
        window.iter().map(RecordBatch::row_count),
        super::morsel::default_morsel_limit(),
    );
    parallelism.windows += 1;
    parallelism.ranges += plan.len();
    let partials = plan
        .par_iter()
        .map(|(index, range)| {
            let mut local = Groups::new();
            let mut local_tally = FoldTally::default();
            let result = fold_range(
                key,
                columns[*index].as_ref().map(KeyColumn::get),
                &window[*index],
                range.clone(),
                &mut local,
                collation,
                aggregates,
                &mut Scratch::default(),
                &mut local_tally,
                memory,
            );
            // A failed partial hands back what it took before the error
            // goes up.
            if result.is_err() {
                memory.release(local.reserved);
            }
            result.map(|()| (local, local_tally))
        })
        .collect::<Vec<_>>();
    let mut failure = None;
    for partial in partials {
        let (local, local_tally) = match partial {
            Ok(partial) => partial,
            Err(error) => {
                failure.get_or_insert(error);
                continue;
            }
        };
        if failure.is_some() {
            memory.release(local.reserved);
            continue;
        }
        tally.folded += local_tally.folded;
        tally.per_row += local_tally.per_row;
        let reserved = local.reserved;
        let merged = (|| {
            for group in local.groups {
                let mut values = group.values;
                let value = values.pop().unwrap_or(Value::Null);
                let target = groups.resolve(value, collation, aggregates, memory)?;
                let states = &mut groups.groups[target as usize].states;
                for ((state, partial), aggregate) in
                    states.iter_mut().zip(group.states).zip(aggregates)
                {
                    state.merge(aggregate, partial, memory)?;
                }
            }
            Ok(())
        })();
        memory.release(reserved);
        if let Err(error) = merged {
            failure.get_or_insert(error);
        }
    }
    failure.map_or(Ok(()), Err)
}

/// Folds every batch of `input`, `first` included, by group.
#[allow(clippy::too_many_lines)] // one pull loop: hold, fold, then finish
#[allow(clippy::too_many_arguments)]
pub(super) fn build_small_group_fold(
    input: &mut PullOperator,
    first: RecordBatch,
    key: &CompiledExpr,
    aggregates: &[CompiledAggregate],
    memory: &MemoryTracker,
    key_collation: Collation,
) -> Result<MaterializedRows, ExecError> {
    let mut groups = Groups::new();
    let mut tally = FoldTally::default();
    let mut scratch = Scratch::default();
    let mut parallelism = Parallelism::default();
    let threads = rayon::current_num_threads();
    let parallel = threads > 1 && merges_exactly(aggregates);
    let window_cap = threads.saturating_mul(WINDOW_BATCHES_PER_THREAD);
    let mut window = Vec::<RecordBatch>::new();
    let mut window_reserved = 0_usize;
    let mut next = Some(first);
    let flush = |window: &mut Vec<RecordBatch>,
                 window_reserved: &mut usize,
                 groups: &mut Groups,
                 scratch: &mut Scratch,
                 tally: &mut FoldTally,
                 parallelism: &mut Parallelism|
     -> Result<(), ExecError> {
        if window.is_empty() {
            return Ok(());
        }
        let result = fold_window(
            key,
            window,
            groups,
            key_collation,
            aggregates,
            scratch,
            tally,
            parallelism,
            memory,
        );
        window.clear();
        memory.release(*window_reserved);
        *window_reserved = 0;
        result
    };
    while let Some(batch) = match next.take() {
        Some(batch) => Some(batch),
        None => input.next_batch(memory)?,
    } {
        memory.check_interruption()?;
        if batch.visible_row_count() == 0 {
            continue;
        }
        if parallel {
            // A held batch is charged until its window folds; with no
            // room, the window folds now, and a batch that still does not
            // fit folds alone.
            let bytes = batch.estimated_bytes();
            let mut held = memory.reserve(bytes).is_ok();
            if !held {
                flush(
                    &mut window,
                    &mut window_reserved,
                    &mut groups,
                    &mut scratch,
                    &mut tally,
                    &mut parallelism,
                )?;
                held = memory.reserve(bytes).is_ok();
            }
            if held {
                window_reserved = window_reserved.saturating_add(bytes);
                window.push(batch);
                if window.len() >= window_cap {
                    flush(
                        &mut window,
                        &mut window_reserved,
                        &mut groups,
                        &mut scratch,
                        &mut tally,
                        &mut parallelism,
                    )?;
                }
                continue;
            }
        }
        let column = key_column(key, &batch);
        fold_range(
            key,
            column.as_ref().map(KeyColumn::get),
            &batch,
            0..batch.row_count(),
            &mut groups,
            key_collation,
            aggregates,
            &mut scratch,
            &mut tally,
            memory,
        )?;
    }
    flush(
        &mut window,
        &mut window_reserved,
        &mut groups,
        &mut scratch,
        &mut tally,
        &mut parallelism,
    )?;
    let mode = if parallel {
        format!(
            "{} parallel windows over {} row ranges, {} serial windows",
            parallelism.windows, parallelism.ranges, parallelism.serial_windows
        )
    } else {
        "serial: an aggregate whose partials do not merge exactly".to_owned()
    };
    super::ProfileNote::of(input).set(&format!(
        "small-group column fold: {} groups, {} aggregate-batches by column, {} per row, {mode}",
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
