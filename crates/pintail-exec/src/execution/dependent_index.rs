//! A hash index that answers a correlated `EXISTS` without re-running it.
//!
//! The dependent path answers `EXISTS (SELECT .. FROM t WHERE ..)` once per
//! outer row by cloning, planning and executing the inner query. The memo
//! shares answers between rows that carry the same correlation tuple, but
//! when the tuples are mostly distinct every row still pays a full plan and
//! execution - most of a millisecond each, which a few tens of thousands of
//! outer rows turn into tens of seconds.
//!
//! When the inner query is one base table under a conjunctive `WHERE`, the
//! question each row asks is always the same one with different constants:
//! "is there a row of `t` with these integer keys that also satisfies the
//! rest?". So the operator reads `t` once, filtered by the conjuncts that do
//! not mention the outer row, and indexes the surviving rows by the integer
//! columns the outer row is equated with. Each outer row then looks up its
//! key and evaluates only the remaining correlated conjuncts over the
//! candidates, stopping at the first one that is true.
//!
//! What it must never change, and how each is kept:
//!
//! - **Comparison semantics**: only `inner_column = outer_expression` with
//!   both sides integer-typed becomes a key. Integers compare by value, so
//!   hashing them widened to `i128` is exactly `=`, signed against unsigned
//!   included; no collation or coercion is ever involved. Everything else
//!   stays in the residual and is evaluated by the ordinary expression code.
//! - **NULL**: a NULL key on either side is never equal to anything, so an
//!   inner row with one is not indexed and an outer row with one finds
//!   nothing. A residual that evaluates to NULL is not a match, as in
//!   `WHERE`.
//! - **Anything unexpected** - a key value that is not an integer, a refused
//!   memory charge, too many rows, a failure while reading the table - falls
//!   back to the per-row path, which answers as it always has. The index can
//!   only remove work, never fail a query that runs without it.
//! - **Snapshot**: the provider is pinned for the statement, so the table
//!   read once is the table every per-row execution would have read.

use std::collections::HashMap;
use std::mem::size_of;

use pintail_sql::{BinaryOp, BoundColumn, BoundExpr, BoundExprKind, BoundProjection, BoundQuery};
use pintail_types::{DataType, Value};

use super::{
    DependentRow, Execution, MemoryTracker, dependent_subquery_memory_limit, substitute_outer_expr,
};
use crate::expression::{CompiledExpr, predicate_truth};
use crate::{LogicalPlanner, Optimizer, PhysicalPlanner, RecordBatch};

/// Inner rows one index holds at most. A table larger than this is read by
/// the per-row path, whose key filter can prune what a full read cannot.
const MAX_INDEX_ROWS: usize = 4 << 20;

/// Inner rows per per-row execution the index waits out before building. A
/// build reads the whole filtered table once; an outer input of a handful
/// of rows is cheaper answered one execution at a time, so a large table
/// lets the per-row path answer the first rows and builds only once the
/// outer input has shown it is not tiny.
const ROWS_PER_WAITED_EXECUTION: u64 = 8_192;
const MAX_WAITED_EXECUTIONS: u64 = 32;

/// How often, in candidates evaluated, a probe checks for cancellation.
const INTERRUPTION_STRIDE: usize = 1_024;

/// The share of the query's remaining memory one index may hold. An
/// operator that resets its memo under memory pressure would otherwise
/// drop and rebuild an index that fills half the ceiling on every row.
const MEMORY_SHARE_DIVISOR: usize = 4;

/// What one subquery slot's index is doing.
pub(super) enum IndexState {
    /// The shape qualifies; the per-row path answers `remaining` more rows
    /// before the index is built.
    Pending {
        plan: Box<IndexPlan>,
        remaining: u64,
    },
    /// Built and answering every row it can.
    Built(Box<ExistsIndex>),
    /// The shape does not qualify, or the build gave up. Never retried.
    Declined,
}

/// The parts of an inner query the index is built from, found once per
/// operator.
pub(super) struct IndexPlan {
    /// The inner table filtered by the uncorrelated conjuncts, projecting
    /// only `layout`.
    materialize: BoundQuery,
    /// Inner columns in the order `materialize` projects them.
    layout: Vec<BoundColumn>,
    /// Per key, the position in `layout` of its inner column.
    inner_keys: Vec<usize>,
    /// Per key, the outer-only expression the inner column is equated with.
    outer_keys: Vec<BoundExpr>,
    /// The correlated conjuncts that are not keys, joined by AND.
    residual: Option<BoundExpr>,
}

/// A built index: the filtered inner rows and their integer keys.
pub(super) struct ExistsIndex {
    batches: Vec<RecordBatch>,
    /// Key tuple to `(batch, row)` of every inner row carrying it.
    rows: HashMap<Vec<i128>, Vec<(u32, u32)>>,
    layout: Vec<BoundColumn>,
    /// Outer key expressions compiled against the operator's input.
    outer_keys: Vec<CompiledExpr>,
    residual: Option<BoundExpr>,
    /// Bytes charged to the query's tracker, returned by `release`.
    reserved: usize,
    /// Reused probe key.
    probe: Vec<i128>,
}

impl ExistsIndex {
    pub(super) fn release(&mut self, memory: &MemoryTracker) {
        memory.release(self.reserved);
        self.reserved = 0;
        self.batches.clear();
        self.rows.clear();
    }
}

/// Counts the index moved, for the process counters.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct IndexStats {
    pub(super) builds: u64,
    pub(super) probes: u64,
    pub(super) declines: u64,
}

/// Decides, once per slot, whether `query` has a shape the index answers.
pub(super) fn plan(query: &BoundQuery) -> IndexState {
    analyse(query).map_or(IndexState::Declined, |(plan, estimated_rows)| {
        IndexState::Pending {
            plan: Box::new(plan),
            remaining: estimated_rows.map_or(MAX_WAITED_EXECUTIONS, |rows| {
                (rows / ROWS_PER_WAITED_EXECUTION).min(MAX_WAITED_EXECUTIONS)
            }),
        }
    })
}

fn analyse(query: &BoundQuery) -> Option<(IndexPlan, Option<u64>)> {
    let [source] = query.from.as_slice() else {
        return None;
    };
    // EXISTS ignores what a row carries and any LIMIT that keeps one row.
    let limit_keeps_a_row = query
        .limit
        .is_none_or(|limit| limit.offset == 0 && limit.count >= 1);
    if !source.joins.is_empty()
        || source.base.input.is_some()
        || !query.group_by.is_empty()
        || !query.aggregates.is_empty()
        || !query.windows.is_empty()
        || query.having.is_some()
        || !query.union_all.is_empty()
        || !query.set_ops.is_empty()
        || query.recursive.is_some()
        || !limit_keeps_a_row
        || !query.projection.iter().all(|projection| {
            matches!(
                projection.expr.kind,
                BoundExprKind::Literal(_) | BoundExprKind::Column(_)
            )
        })
    {
        return None;
    }
    let base = &source.base;
    if base
        .estimated_rows
        .or(base.row_count)
        .is_some_and(|rows| rows > MAX_INDEX_ROWS as u64)
    {
        return None;
    }
    let filter = query.filter.as_ref()?;
    let mut conjuncts = Vec::new();
    flatten_and(filter, &mut conjuncts);

    let mut uncorrelated = Vec::new();
    let mut keys = Vec::new();
    let mut residual = Vec::new();
    for conjunct in conjuncts {
        if !plain_expression(conjunct) {
            return None;
        }
        let mut inner_columns = Vec::new();
        let mut has_outer = false;
        columns_of(conjunct, &mut inner_columns, &mut has_outer);
        if inner_columns.iter().any(|column| !belongs_to(column, base)) {
            return None;
        }
        if !has_outer {
            uncorrelated.push(conjunct.clone());
        } else if let Some((inner, outer)) = integer_key(conjunct) {
            keys.push((inner.clone(), outer.clone()));
        } else {
            residual.push(conjunct.clone());
        }
    }
    if keys.is_empty() {
        return None;
    }

    let mut layout: Vec<BoundColumn> = Vec::new();
    let mut add_column = |column: &BoundColumn| {
        if let Some(position) = layout.iter().position(|seen| same_column(seen, column)) {
            position
        } else {
            layout.push(column.clone());
            layout.len() - 1
        }
    };
    let inner_keys = keys
        .iter()
        .map(|(inner, _)| add_column(inner))
        .collect::<Vec<_>>();
    for conjunct in &residual {
        let mut inner_columns = Vec::new();
        let mut has_outer = false;
        columns_of(conjunct, &mut inner_columns, &mut has_outer);
        for column in inner_columns {
            add_column(column);
        }
    }

    let materialize = materialize_query(query, &layout, uncorrelated);
    Some((
        IndexPlan {
            materialize,
            layout,
            inner_keys,
            outer_keys: keys.into_iter().map(|(_, outer)| outer).collect(),
            residual: conjoin(residual),
        },
        base.estimated_rows.or(base.row_count),
    ))
}

/// The inner table filtered by `uncorrelated`, projecting `layout`.
fn materialize_query(
    query: &BoundQuery,
    layout: &[BoundColumn],
    uncorrelated: Vec<BoundExpr>,
) -> BoundQuery {
    BoundQuery {
        from: query.from.clone(),
        tables: query.tables.clone(),
        projection: layout
            .iter()
            .map(|column| BoundProjection {
                name: column.name.clone(),
                expr: BoundExpr {
                    data_type: Some(column.data_type),
                    nullable: column.nullable,
                    kind: BoundExprKind::Column(column.clone()),
                },
            })
            .collect(),
        filter: conjoin(uncorrelated),
        group_by: Vec::new(),
        aggregates: Vec::new(),
        windows: Vec::new(),
        having: None,
        distinct: false,
        order_by: Vec::new(),
        hidden_sort_columns: 0,
        union_all: Vec::new(),
        union_distinct: false,
        set_ops: Vec::new(),
        limit: None,
        recursive: None,
        text_collation: query.text_collation,
    }
}

/// Reads the filtered inner table and indexes it. `None` when anything
/// about it says the per-row path should keep answering.
pub(super) fn build(plan: &IndexPlan, context: &DependentRow<'_>) -> Option<ExistsIndex> {
    let outer_keys = plan
        .outer_keys
        .iter()
        .map(|expression| {
            outer_columns_resolve(expression, context.columns)
                .then(|| CompiledExpr::compile(expression, context.columns, context.collation).ok())
                .flatten()
        })
        .collect::<Option<Vec<_>>>()?;
    if plan
        .residual
        .as_ref()
        .is_some_and(|residual| !outer_columns_resolve(residual, context.columns))
    {
        return None;
    }
    let physical = PhysicalPlanner::plan(
        Optimizer::optimize(LogicalPlanner::plan(plan.materialize.clone())),
        context.collation,
    )
    .ok()?;
    let limit = dependent_subquery_memory_limit(context.memory, context.batch).ok()?;
    let mut execution = Execution::start_with_deadline(
        physical,
        context.provider,
        limit,
        context.memory.deadline,
        context.collation,
    )
    .ok()?;

    let mut index = ExistsIndex {
        batches: Vec::new(),
        rows: HashMap::new(),
        layout: plan.layout.clone(),
        outer_keys,
        residual: plan.residual.clone(),
        reserved: 0,
        probe: Vec::with_capacity(plan.inner_keys.len()),
    };
    let key_bytes = size_of::<Vec<i128>>()
        + size_of::<Vec<(u32, u32)>>()
        + plan.inner_keys.len() * size_of::<i128>()
        + super::HASH_ENTRY_OVERHEAD;
    let mut indexed = 0_usize;
    let budget = context.memory.remaining() / MEMORY_SHARE_DIVISOR;
    let outcome = (|| -> Option<()> {
        while let Some(batch) = execution.next_batch().ok()? {
            let batch_number = u32::try_from(index.batches.len()).ok()?;
            let mut charge = batch.estimated_bytes();
            let mut key = Vec::with_capacity(plan.inner_keys.len());
            'rows: for row in batch.selection().selected_rows() {
                key.clear();
                for &position in &plan.inner_keys {
                    match integer_key_value(batch.column(position)?.value(row)?) {
                        KeyValue::Integer(value) => key.push(value),
                        KeyValue::Null => continue 'rows,
                        KeyValue::Other => return None,
                    }
                }
                indexed += 1;
                if indexed > MAX_INDEX_ROWS {
                    return None;
                }
                let row = u32::try_from(row).ok()?;
                charge += size_of::<(u32, u32)>();
                if let Some(rows) = index.rows.get_mut(key.as_slice()) {
                    rows.push((batch_number, row));
                } else {
                    charge += key_bytes;
                    index.rows.insert(key.clone(), vec![(batch_number, row)]);
                }
            }
            if index.reserved.saturating_add(charge) > budget {
                return None;
            }
            context.memory.reserve(charge).ok()?;
            index.reserved += charge;
            index.batches.push(batch);
        }
        Some(())
    })();
    if outcome.is_none() {
        index.release(context.memory);
        return None;
    }
    Some(index)
}

/// Answers `EXISTS` for the current outer row, or `None` when this row's
/// key is not an integer and the per-row path must answer it.
pub(super) fn probe(
    index: &mut ExistsIndex,
    context: &DependentRow<'_>,
) -> Result<Option<bool>, super::ExecError> {
    index.probe.clear();
    for key in &index.outer_keys {
        match integer_key_value(&key.evaluate(context.batch, context.row)?) {
            KeyValue::Integer(value) => index.probe.push(value),
            KeyValue::Null => return Ok(Some(false)),
            KeyValue::Other => return Ok(None),
        }
    }
    let Some(candidates) = index.rows.get(index.probe.as_slice()) else {
        return Ok(Some(false));
    };
    let Some(residual) = &index.residual else {
        return Ok(Some(true));
    };
    let mut residual = residual.clone();
    let mut substituted = Vec::new();
    substitute_outer_expr(
        &mut residual,
        context.batch,
        context.row,
        context.columns,
        &mut substituted,
    )?;
    let residual = CompiledExpr::compile(&residual, &index.layout, context.collation)?;
    for (evaluated, &(batch, row)) in candidates.iter().enumerate() {
        if evaluated % INTERRUPTION_STRIDE == INTERRUPTION_STRIDE - 1 {
            context.memory.check_interruption()?;
        }
        let batch = &index.batches[batch as usize];
        if predicate_truth(&residual.evaluate(batch, row as usize)?)? {
            return Ok(Some(true));
        }
    }
    Ok(Some(false))
}

enum KeyValue {
    Integer(i128),
    Null,
    Other,
}

fn integer_key_value(value: &Value) -> KeyValue {
    match value {
        Value::Int64(value) => KeyValue::Integer(i128::from(*value)),
        Value::UInt64(value) => KeyValue::Integer(i128::from(*value)),
        Value::Null => KeyValue::Null,
        _ => KeyValue::Other,
    }
}

const fn is_integer(data_type: DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
    )
}

/// `inner_column = outer_expression`, either way round, both integers.
fn integer_key(conjunct: &BoundExpr) -> Option<(&BoundColumn, &BoundExpr)> {
    let BoundExprKind::Binary {
        op: BinaryOp::Equal,
        left,
        right,
    } = &conjunct.kind
    else {
        return None;
    };
    let side = |column: &BoundExpr, other: &'_ BoundExpr| {
        let BoundExprKind::Column(inner) = &column.kind else {
            return false;
        };
        if inner.outer
            || !is_integer(inner.data_type)
            || column.data_type.is_some_and(|t| !is_integer(t))
        {
            return false;
        }
        let mut inner_columns = Vec::new();
        let mut has_outer = false;
        columns_of(other, &mut inner_columns, &mut has_outer);
        inner_columns.is_empty() && has_outer && other.data_type.is_some_and(is_integer)
    };
    if side(left, right) {
        let BoundExprKind::Column(inner) = &left.kind else {
            return None;
        };
        Some((inner, right))
    } else if side(right, left) {
        let BoundExprKind::Column(inner) = &right.kind else {
            return None;
        };
        Some((inner, left))
    } else {
        None
    }
}

fn flatten_and<'a>(expression: &'a BoundExpr, conjuncts: &mut Vec<&'a BoundExpr>) {
    if let BoundExprKind::Binary {
        op: BinaryOp::And,
        left,
        right,
    } = &expression.kind
    {
        flatten_and(left, conjuncts);
        flatten_and(right, conjuncts);
    } else {
        conjuncts.push(expression);
    }
}

fn conjoin(conjuncts: Vec<BoundExpr>) -> Option<BoundExpr> {
    conjuncts.into_iter().reduce(|left, right| BoundExpr {
        data_type: Some(DataType::Boolean),
        nullable: left.nullable || right.nullable,
        kind: BoundExprKind::Binary {
            op: BinaryOp::And,
            left: Box::new(left),
            right: Box::new(right),
        },
    })
}

/// Whether an expression is built only from the node kinds the index
/// evaluates row by row: no subqueries, no aggregate or window slots.
fn plain_expression(expression: &BoundExpr) -> bool {
    match &expression.kind {
        BoundExprKind::Column(_) | BoundExprKind::Literal(_) => true,
        BoundExprKind::Unary { expr, .. } | BoundExprKind::IsNull { expr, .. } => {
            plain_expression(expr)
        }
        BoundExprKind::Binary { left, right, .. } => {
            plain_expression(left) && plain_expression(right)
        }
        BoundExprKind::Scalar { args, .. } => args.iter().all(plain_expression),
        BoundExprKind::PreparedIn { .. }
        | BoundExprKind::ScalarSubquery(_)
        | BoundExprKind::ExistsSubquery { .. }
        | BoundExprKind::InSubquery { .. }
        | BoundExprKind::GroupKey(_)
        | BoundExprKind::Aggregate(_)
        | BoundExprKind::Window(_) => false,
    }
}

/// Collects the inner columns an expression reads and whether it reads any
/// outer one. Only called on `plain_expression`s.
fn columns_of<'a>(
    expression: &'a BoundExpr,
    inner: &mut Vec<&'a BoundColumn>,
    has_outer: &mut bool,
) {
    match &expression.kind {
        BoundExprKind::Column(column) if column.outer => *has_outer = true,
        BoundExprKind::Column(column) => inner.push(column),
        BoundExprKind::Unary { expr, .. } | BoundExprKind::IsNull { expr, .. } => {
            columns_of(expr, inner, has_outer);
        }
        BoundExprKind::Binary { left, right, .. } => {
            columns_of(left, inner, has_outer);
            columns_of(right, inner, has_outer);
        }
        BoundExprKind::Scalar { args, .. } => {
            for argument in args {
                columns_of(argument, inner, has_outer);
            }
        }
        _ => {}
    }
}

/// Whether every outer column `expression` reads is one of the operator's
/// input columns. One that is not belongs to a scope further out, which
/// only the per-row path substitutes.
fn outer_columns_resolve(expression: &BoundExpr, columns: &[BoundColumn]) -> bool {
    match &expression.kind {
        BoundExprKind::Column(column) if column.outer => columns
            .iter()
            .any(|candidate| same_column(candidate, column)),
        BoundExprKind::Unary { expr, .. } | BoundExprKind::IsNull { expr, .. } => {
            outer_columns_resolve(expr, columns)
        }
        BoundExprKind::Binary { left, right, .. } => {
            outer_columns_resolve(left, columns) && outer_columns_resolve(right, columns)
        }
        BoundExprKind::Scalar { args, .. } => args
            .iter()
            .all(|argument| outer_columns_resolve(argument, columns)),
        _ => true,
    }
}

fn same_column(left: &BoundColumn, right: &BoundColumn) -> bool {
    left.database_id == right.database_id
        && left.table_id == right.table_id
        && left.column_id == right.column_id
        && left
            .relation_name
            .eq_ignore_ascii_case(&right.relation_name)
}

fn belongs_to(column: &BoundColumn, table: &pintail_sql::BoundTable) -> bool {
    column.database_id == table.database_id
        && column.table_id == table.table_id
        && column
            .relation_name
            .eq_ignore_ascii_case(&table.relation_name)
}
