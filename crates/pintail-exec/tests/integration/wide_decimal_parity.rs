//! DECIMAL columns wider than 38 digits compute exactly, as `MySQL` does up to
//! its 65-digit ceiling.
//!
//! Every answer in `wide_decimal_parity.pairs` was read from `MySQL` 8.4 over
//! the same rows.

use pintail_types::{Column, DataType, KeyPart, PrimaryKey, StoredRow, TableSchema, Value};

use crate::pairs_fixture::assert_pairs;

/// (id, a DECIMAL(65,30), b DECIMAL(50,0), c DECIMAL(40,5)) as the source
/// sends them.
const ROWS: [(u64, Option<[&str; 3]>); 4] = [
    (
        1,
        Some([
            "12345678901234567890123456789012345.123456789012345678901234567890",
            "99999999999999999999999999999999999999999999999999",
            "12345678901234567890123456789012345.12345",
        ]),
    ),
    (
        2,
        Some([
            "-0.000000000000000000000000000001",
            "-12345678901234567890123456789012345678901234567890",
            "-1.50000",
        ]),
    ),
    (
        3,
        Some(["1.000000000000000000000000000000", "1", "0.00000"]),
    ),
    (4, None),
];

fn schema() -> TableSchema {
    let decimal = |id, name, precision, scale| {
        Column::new(id, name, DataType::Decimal { precision, scale }, true)
    };
    TableSchema::new(
        1,
        vec![
            Column::new(1, "id", DataType::UInt64, false),
            decimal(2, "a", 65, 30),
            decimal(3, "b", 50, 0),
            decimal(4, "c", 40, 5),
        ],
    )
    .expect("schema")
}

#[test]
fn wide_decimals_answer_as_mysql_does() {
    let rows = ROWS
        .iter()
        .map(|(id, texts)| {
            let mut values = vec![Value::UInt64(*id)];
            let texts = texts.map(|texts| texts.map(|text| Value::Utf8(text.to_owned())));
            values.extend(texts.unwrap_or([Value::Null, Value::Null, Value::Null]));
            StoredRow::new(
                PrimaryKey::new(vec![KeyPart::UInt64(*id)]).expect("key"),
                values,
                *id,
                false,
            )
        })
        .collect();
    assert_pairs(
        include_str!("wide_decimal_parity.pairs"),
        "w",
        &schema(),
        rows,
        26,
    );
}
