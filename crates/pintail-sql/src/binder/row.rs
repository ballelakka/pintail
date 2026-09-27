//! Row constructors in comparisons: `(a, b) = (1, 2)`, `ROW(a, b) < (x, y)`,
//! `(a, b) IN ((1, 2), (3, 4))`.
//!
//! A row comparison is a fixed combination of its columns' comparisons, so
//! it is rewritten into them before binding and every later stage sees
//! ordinary scalar predicates:
//!
//! - `=` is every pair equal, `<=>` every pair null-safe equal, and `<>` any
//!   pair different. Three-valued logic carries `MySQL`'s NULL answer through:
//!   `(1, NULL) = (1, 2)` is NULL, `(1, NULL) = (2, 2)` is false.
//! - `<`, `<=`, `>`, `>=` compare lexicographically: the first pair decides
//!   unless it is equal, in which case the rest of the row does.
//!   `(a1, a2) < (b1, b2)` is `a1 < b1 OR (a1 = b1 AND a2 < b2)`, which is
//!   NULL exactly where `MySQL` answers NULL - an undecided pair whose
//!   successors cannot settle the comparison.
//! - `IN` is equality with any listed row, and `NOT IN` its negation.
//!
//! Rows nest: a column that is itself a row compares by the same rules when
//! the rewritten pair is bound.

use sqlparser::ast::{BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments};

use super::BindError;

/// The columns of a row constructor, or `None` for any other expression.
pub(super) fn columns(expr: &Expr) -> Option<Vec<Expr>> {
    match expr {
        Expr::Tuple(items) => Some(items.clone()),
        Expr::Nested(inner) => columns(inner),
        Expr::Function(function)
            if function.over.is_none() && function.name.to_string().eq_ignore_ascii_case("ROW") =>
        {
            let FunctionArguments::List(list) = &function.args else {
                return None;
            };
            if list.args.len() < 2 {
                return None;
            }
            list.args
                .iter()
                .map(|argument| match argument {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => Some(expr.clone()),
                    _ => None,
                })
                .collect()
        }
        _ => None,
    }
}

fn arity_error(expected: usize) -> BindError {
    BindError::InvalidScalarFunction(format!("Operand should contain {expected} column(s)"))
}

fn binary(left: Expr, op: BinaryOperator, right: Expr) -> Expr {
    Expr::Nested(Box::new(Expr::BinaryOp {
        left: Box::new(left),
        op,
        right: Box::new(right),
    }))
}

/// `left op right` over two rows, rewritten into column comparisons; `None`
/// when either side is not a row constructor or `op` is not a comparison.
///
/// # Errors
///
/// A row compared with a row of another width, or with a scalar.
pub(super) fn comparison(
    left: &Expr,
    op: &BinaryOperator,
    right: &Expr,
) -> Result<Option<Expr>, BindError> {
    if !matches!(
        op,
        BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Spaceship
            | BinaryOperator::Lt
            | BinaryOperator::LtEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq
    ) {
        return Ok(None);
    }
    let (left_columns, right_columns) = match (columns(left), columns(right)) {
        (Some(left), Some(right)) => (left, right),
        (None, None) => return Ok(None),
        // A row against a subquery is the subquery binder's to answer.
        (Some(_), None) if is_subquery(right) => return Ok(None),
        (None, Some(_)) if is_subquery(left) => return Ok(None),
        (Some(row), None) | (None, Some(row)) => return Err(arity_error(row.len())),
    };
    if left_columns.len() != right_columns.len() {
        return Err(arity_error(left_columns.len()));
    }
    let pairs = left_columns
        .into_iter()
        .zip(right_columns)
        .collect::<Vec<_>>();
    Ok(Some(match op {
        BinaryOperator::Eq | BinaryOperator::Spaceship => all(pairs, op),
        BinaryOperator::NotEq => pairs
            .into_iter()
            .map(|(left, right)| binary(left, BinaryOperator::NotEq, right))
            .reduce(|left, right| binary(left, BinaryOperator::Or, right))
            .ok_or_else(|| arity_error(0))?,
        _ => lexicographic(pairs, op),
    }))
}

fn is_subquery(expr: &Expr) -> bool {
    match expr {
        Expr::Subquery(_) => true,
        Expr::Nested(inner) => is_subquery(inner),
        _ => false,
    }
}

fn all(pairs: Vec<(Expr, Expr)>, op: &BinaryOperator) -> Expr {
    pairs
        .into_iter()
        .map(|(left, right)| binary(left, op.clone(), right))
        .reduce(|left, right| binary(left, BinaryOperator::And, right))
        .unwrap_or_else(|| Expr::value(sqlparser::ast::Value::Boolean(true)))
}

/// `(a1, ..., an) op (b1, ..., bn)` for an ordering `op`.
fn lexicographic(mut pairs: Vec<(Expr, Expr)>, op: &BinaryOperator) -> Expr {
    let strict = match op {
        BinaryOperator::LtEq => BinaryOperator::Lt,
        BinaryOperator::GtEq => BinaryOperator::Gt,
        other => other.clone(),
    };
    // Built from the last pair outward: the last pair uses `op` itself, so
    // `<=` admits equal rows; every earlier pair either decides strictly or
    // is equal and defers.
    let Some((left, right)) = pairs.pop() else {
        return Expr::value(sqlparser::ast::Value::Boolean(false));
    };
    let mut tail = binary(left, op.clone(), right);
    while let Some((left, right)) = pairs.pop() {
        let decides = binary(left.clone(), strict.clone(), right.clone());
        let defers = binary(
            binary(left, BinaryOperator::Eq, right),
            BinaryOperator::And,
            tail,
        );
        tail = binary(decides, BinaryOperator::Or, defers);
    }
    tail
}

/// `row [NOT] IN (row, ...)`, rewritten into row equalities; `None` when
/// `expr` is not a row constructor.
///
/// # Errors
///
/// A listed item that is not a row of the same width.
pub(super) fn in_list(
    expr: &Expr,
    list: &[Expr],
    negated: bool,
) -> Result<Option<Expr>, BindError> {
    let Some(width) = columns(expr).map(|columns| columns.len()) else {
        return Ok(None);
    };
    let mut any: Option<Expr> = None;
    for item in list {
        if columns(item).is_none_or(|columns| columns.len() != width) {
            return Err(arity_error(width));
        }
        let equal = binary(expr.clone(), BinaryOperator::Eq, item.clone());
        any = Some(match any {
            None => equal,
            Some(previous) => binary(previous, BinaryOperator::Or, equal),
        });
    }
    let Some(any) = any else {
        return Err(arity_error(width));
    };
    Ok(Some(if negated {
        Expr::UnaryOp {
            op: sqlparser::ast::UnaryOperator::Not,
            expr: Box::new(Expr::Nested(Box::new(any))),
        }
    } else {
        any
    }))
}
