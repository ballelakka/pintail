//! Column-at-a-time selection kernels.
//!
//! A predicate over a packed column becomes a bitmask one 64-row word at a
//! time: the test runs over a fixed run of 64 values with no branch and no
//! early exit, so the compiler turns it into vector compares, and validity
//! joins the answer as one AND per word instead of one test per row. Words
//! are filled by spans in parallel; each word has exactly one writer.

use rayon::prelude::*;

use super::CompiledExpr;
use crate::array::ValidityMask;
use crate::batch::TypedValues;
use crate::{RecordBatch, SelectionMask};
use pintail_sql::BinaryOp;
use pintail_types::{DataType, Value};

/// Words one parallel task fills. Below this many words the mask fills on
/// the calling thread: a task costs more than the comparisons it would run.
const WORDS_PER_SPAN: usize = 1024;

/// Bit `i` set where `keep(values[i])`, for at most 64 values.
#[inline]
fn word_bits<T: Copy>(values: &[T], keep: &impl Fn(T) -> bool) -> u64 {
    let mut bits = 0_u64;
    if let Ok(full) = <&[T; 64]>::try_from(values) {
        // A fixed trip count is what lets the loop vectorize.
        for (offset, value) in full.iter().enumerate() {
            bits |= u64::from(keep(*value)) << offset;
        }
    } else {
        for (offset, value) in values.iter().enumerate() {
            bits |= u64::from(keep(*value)) << offset;
        }
    }
    bits
}

/// The rows of `values` that are valid and pass `keep`.
pub(super) fn select_words<T: Copy + Sync>(
    values: &[T],
    validity: &ValidityMask,
    keep: impl Fn(T) -> bool + Sync,
) -> SelectionMask {
    let word_count = values.len().div_ceil(64);
    let word = |index: usize| {
        let start = index * 64;
        let end = (start + 64).min(values.len());
        word_bits(&values[start..end], &keep) & validity.word(index)
    };
    let words: Vec<u64> = if word_count <= WORDS_PER_SPAN {
        (0..word_count).map(word).collect()
    } else {
        (0..word_count.div_ceil(WORDS_PER_SPAN))
            .into_par_iter()
            .flat_map_iter(|span| {
                let first = span * WORDS_PER_SPAN;
                (first..(first + WORDS_PER_SPAN).min(word_count)).map(word)
            })
            .collect()
    };
    SelectionMask::from_words(values.len(), words)
}

/// The rows of a dictionary-coded column whose entry `matching` accepts.
/// A single accepted entry - the shape of `column = 'literal'` - compares
/// codes directly; otherwise each code looks its entry up.
pub(super) fn select_codes(
    codes: &[u32],
    validity: &ValidityMask,
    matching: &[bool],
) -> SelectionMask {
    let mut accepted = matching
        .iter()
        .enumerate()
        .filter(|(_, keep)| **keep)
        .map(|(code, _)| code);
    match (accepted.next(), accepted.next()) {
        (None, _) => SelectionMask::none(codes.len()),
        (Some(code), None) => match u32::try_from(code) {
            Ok(code) => select_words(codes, validity, move |value| value == code),
            Err(_) => SelectionMask::none(codes.len()),
        },
        _ => select_words(codes, validity, |value| {
            usize::try_from(value)
                .ok()
                .and_then(|value| matching.get(value))
                .copied()
                .unwrap_or(false)
        }),
    }
}

/// One side of a range on a packed column, in the column's own units.
#[derive(Clone, Copy)]
enum PackedBound {
    Signed(i64),
    Unsigned(u64),
}

/// The literal of `column op literal` in the packed column's units, where
/// the packed comparison would compare those same units; `None` for every
/// shape the single-comparison kernel answers some other way.
fn packed_bound(typed: &TypedValues, logical: DataType, literal: &Value) -> Option<PackedBound> {
    match (typed, literal) {
        (TypedValues::Int64(_), Value::Int64(value)) => Some(PackedBound::Signed(*value)),
        (TypedValues::UInt64(_), Value::UInt64(value)) => Some(PackedBound::Unsigned(*value)),
        (TypedValues::UInt64(_), Value::Int64(value)) => {
            u64::try_from(*value).ok().map(PackedBound::Unsigned)
        }
        (TypedValues::Temporal { .. }, Value::Utf8(text)) => match logical {
            DataType::Date32 => crate::batch::parse_date_days(text).map(PackedBound::Signed),
            DataType::DateTime64 { fsp } => {
                let expected_len = if fsp == 0 { 19 } else { 20 + usize::from(fsp) };
                (text.len() == expected_len)
                    .then(|| crate::batch::parse_datetime_micros(text))
                    .flatten()
                    .map(PackedBound::Signed)
            }
            _ => None,
        },
        _ => None,
    }
}

/// The rows of a packed column inside a two-sided range, in one pass.
///
/// `lower` is `(op, literal)` with `op` one of `>`/`>=`, `upper` the same
/// with `<`/`<=`. Two separate comparisons read the column twice and
/// intersect two masks; this reads it once. `None` where either side is a
/// shape the packed comparison does not answer in the column's own units.
fn packed_range_mask(
    typed: &TypedValues,
    validity: &ValidityMask,
    logical: DataType,
    lower: (BinaryOp, &Value),
    upper: (BinaryOp, &Value),
) -> Option<SelectionMask> {
    let rows = validity.len();
    let low = packed_bound(typed, logical, lower.1)?;
    let high = packed_bound(typed, logical, upper.1)?;
    let lower_strict = lower.0 == BinaryOp::Greater;
    let upper_strict = upper.0 == BinaryOp::Less;
    match (typed, low, high) {
        (
            TypedValues::Int64(values) | TypedValues::Temporal { units: values, .. },
            PackedBound::Signed(low),
            PackedBound::Signed(high),
        ) => {
            let low = if lower_strict {
                low.checked_add(1)
            } else {
                Some(low)
            };
            let high = if upper_strict {
                high.checked_sub(1)
            } else {
                Some(high)
            };
            let (Some(low), Some(high)) = (low, high) else {
                return Some(SelectionMask::none(rows));
            };
            if low > high {
                return Some(SelectionMask::none(rows));
            }
            // `low <= value <= high` as one unsigned comparison: a value below
            // `low` wraps past the span.
            let span = high.wrapping_sub(low).cast_unsigned();
            Some(select_words(values, validity, move |value: i64| {
                value.wrapping_sub(low).cast_unsigned() <= span
            }))
        }
        (TypedValues::UInt64(values), PackedBound::Unsigned(low), PackedBound::Unsigned(high)) => {
            let low = if lower_strict {
                low.checked_add(1)
            } else {
                Some(low)
            };
            let high = if upper_strict {
                high.checked_sub(1)
            } else {
                Some(high)
            };
            let (Some(low), Some(high)) = (low, high) else {
                return Some(SelectionMask::none(rows));
            };
            if low > high {
                return Some(SelectionMask::none(rows));
            }
            let span = high - low;
            Some(select_words(values, validity, move |value: u64| {
                value.wrapping_sub(low) <= span
            }))
        }
        _ => None,
    }
}

/// `column op literal` with the column on the left, mirrored when the
/// literal came first.
fn column_comparison(expr: &CompiledExpr) -> Option<(usize, BinaryOp, &Value)> {
    let CompiledExpr::Binary {
        op, left, right, ..
    } = expr
    else {
        return None;
    };
    match (left.as_ref(), right.as_ref()) {
        (CompiledExpr::Column(column), CompiledExpr::Literal(value)) => Some((*column, *op, value)),
        (CompiledExpr::Literal(value), CompiledExpr::Column(column)) => {
            Some((*column, super::mirror_comparison(*op), value))
        }
        _ => None,
    }
}

/// `column > a AND column < b` (any mix of strict and inclusive bounds, in
/// either order) over one packed column, answered in a single pass. `None`
/// when the conjunction is not that shape; the caller then intersects the
/// two sides' own masks, which is the same answer.
pub(super) fn conjunction_range_mask(
    batch: &RecordBatch,
    left: &CompiledExpr,
    right: &CompiledExpr,
) -> Option<SelectionMask> {
    let (left_column, left_op, left_value) = column_comparison(left)?;
    let (right_column, right_op, right_value) = column_comparison(right)?;
    if left_column != right_column {
        return None;
    }
    let is_lower = |op| matches!(op, BinaryOp::Greater | BinaryOp::GreaterOrEqual);
    let is_upper = |op| matches!(op, BinaryOp::Less | BinaryOp::LessOrEqual);
    let (lower, upper) = if is_lower(left_op) && is_upper(right_op) {
        ((left_op, left_value), (right_op, right_value))
    } else if is_upper(left_op) && is_lower(right_op) {
        ((right_op, right_value), (left_op, left_value))
    } else {
        return None;
    };
    let vector = batch.column(left_column)?;
    let (typed, validity) = vector.typed()?;
    packed_range_mask(typed, validity, vector.data_type(), lower, upper)
}

/// `column BETWEEN lower AND upper` over one packed column in a single
/// pass; `None` for shapes the caller answers as two comparisons.
pub(super) fn between_range_mask(
    batch: &RecordBatch,
    column: usize,
    lower: &Value,
    upper: &Value,
) -> Option<SelectionMask> {
    let vector = batch.column(column)?;
    let (typed, validity) = vector.typed()?;
    packed_range_mask(
        typed,
        validity,
        vector.data_type(),
        (BinaryOp::GreaterOrEqual, lower),
        (BinaryOp::LessOrEqual, upper),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reference<T: Copy>(values: &[T], valid: &[bool], keep: impl Fn(T) -> bool) -> Vec<bool> {
        values
            .iter()
            .zip(valid)
            .map(|(value, valid)| *valid && keep(*value))
            .collect()
    }

    fn bits(mask: &SelectionMask) -> Vec<bool> {
        (0..mask.len()).map(|row| mask.is_selected(row)).collect()
    }

    #[test]
    fn word_kernels_match_a_row_loop_across_spans_tails_and_nulls() {
        for rows in [
            0,
            1,
            63,
            64,
            65,
            200,
            64 * WORDS_PER_SPAN + 7,
            3 * 64 * WORDS_PER_SPAN,
        ] {
            let values: Vec<i64> = (0..rows)
                .map(|row| i64::try_from(row * 7 % 101).expect("small") - 50)
                .collect();
            let valid: Vec<bool> = (0..rows).map(|row| row % 13 != 5).collect();
            for validity in [
                ValidityMask::all_valid(rows),
                ValidityMask::from_bools(&valid),
            ] {
                let valid: Vec<bool> = (0..rows).map(|row| validity.is_valid(row)).collect();
                let mask = select_words(&values, &validity, |value| value >= -3);
                assert_eq!(bits(&mask), reference(&values, &valid, |value| value >= -3));
                let codes: Vec<u32> = values
                    .iter()
                    .map(|value| u32::try_from(value.rem_euclid(5)).expect("small"))
                    .collect();
                for matching in [
                    vec![false; 5],
                    vec![false, false, true, false, false],
                    vec![true, false, true, false, true],
                    vec![true; 5],
                ] {
                    let mask = select_codes(&codes, &validity, &matching);
                    assert_eq!(
                        bits(&mask),
                        reference(&codes, &valid, |code| matching[code as usize]),
                    );
                }
            }
        }
    }

    #[test]
    fn packed_ranges_match_two_comparisons_at_every_edge() {
        let values: Vec<i64> = vec![i64::MIN, -5, -1, 0, 1, 4, 5, 6, i64::MAX];
        let valid = vec![true, true, false, true, true, true, true, true, true];
        let validity = ValidityMask::from_bools(&valid);
        let typed = TypedValues::Int64(values.clone());
        let ops = [
            (BinaryOp::Greater, BinaryOp::Less),
            (BinaryOp::Greater, BinaryOp::LessOrEqual),
            (BinaryOp::GreaterOrEqual, BinaryOp::Less),
            (BinaryOp::GreaterOrEqual, BinaryOp::LessOrEqual),
        ];
        let edges = [i64::MIN, -5, -1, 0, 1, 5, i64::MAX];
        for (lower_op, upper_op) in ops {
            for low in edges {
                for high in edges {
                    let mask = packed_range_mask(
                        &typed,
                        &validity,
                        DataType::Int64,
                        (lower_op, &Value::Int64(low)),
                        (upper_op, &Value::Int64(high)),
                    )
                    .expect("packed");
                    let expected = reference(&values, &valid, |value| {
                        let above = if lower_op == BinaryOp::Greater {
                            value > low
                        } else {
                            value >= low
                        };
                        let below = if upper_op == BinaryOp::Less {
                            value < high
                        } else {
                            value <= high
                        };
                        above && below
                    });
                    assert_eq!(
                        bits(&mask),
                        expected,
                        "{lower_op:?} {low} {upper_op:?} {high}"
                    );
                }
            }
        }
        let unsigned: Vec<u64> = vec![0, 1, 2, 9, 10, 11, u64::MAX];
        let all = ValidityMask::all_valid(unsigned.len());
        let typed = TypedValues::UInt64(unsigned.clone());
        for (low, high) in [
            (0_u64, 0_u64),
            (1, 10),
            (10, 1),
            (0, u64::MAX),
            (u64::MAX, u64::MAX),
        ] {
            for (lower_op, upper_op) in ops {
                let mask = packed_range_mask(
                    &typed,
                    &all,
                    DataType::UInt64,
                    (lower_op, &Value::UInt64(low)),
                    (upper_op, &Value::UInt64(high)),
                )
                .expect("packed");
                let expected: Vec<bool> = unsigned
                    .iter()
                    .map(|value| {
                        let above = if lower_op == BinaryOp::Greater {
                            *value > low
                        } else {
                            *value >= low
                        };
                        let below = if upper_op == BinaryOp::Less {
                            *value < high
                        } else {
                            *value <= high
                        };
                        above && below
                    })
                    .collect();
                assert_eq!(bits(&mask), expected);
            }
        }
        // A negative bound on an unsigned column is not in the column's
        // units; the caller's two comparisons answer it.
        assert!(
            packed_range_mask(
                &typed,
                &all,
                DataType::UInt64,
                (BinaryOp::GreaterOrEqual, &Value::Int64(-1)),
                (BinaryOp::Less, &Value::UInt64(5)),
            )
            .is_none()
        );
    }
}
