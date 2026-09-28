//! Values a function has to truncate or refuse leave `MySQL`'s warnings in
//! the statement's diagnostics area: text read as a number past its numeric
//! prefix (1292 "Truncated incorrect ..."), a date that does not parse
//! (1292 "Incorrect datetime value"), and `STR_TO_DATE` input that does not
//! match its format (1411).
//!
//! Every answer and warning in `statement_warnings.pairs` was read from
//! `MySQL` 8.4 over the same rows.

use pintail_types::{Column, DataType, KeyPart, PrimaryKey, StoredRow, TableSchema, Value};

use crate::pairs_fixture::assert_pairs;

/// (id, s, d) as the source holds them.
const ROWS: [(u64, Option<[&str; 2]>); 4] = [
    (1, Some(["12abc", "2024-13-01"])),
    (2, Some(["7", "2024-01-05"])),
    (3, Some(["x", "bad"])),
    (4, None),
];

fn schema() -> TableSchema {
    TableSchema::new(
        1,
        vec![
            Column::new(1, "id", DataType::UInt64, false),
            Column::new(2, "s", DataType::Utf8, true),
            Column::new(3, "d", DataType::Utf8, true),
        ],
    )
    .expect("schema")
}

#[test]
fn truncations_and_invalid_dates_warn_as_mysql_does() {
    let rows = ROWS
        .iter()
        .map(|(id, texts)| {
            let mut values = vec![Value::UInt64(*id)];
            let texts = texts.map(|texts| texts.map(|text| Value::Utf8(text.to_owned())));
            values.extend(texts.unwrap_or([Value::Null, Value::Null]));
            StoredRow::new(
                PrimaryKey::new(vec![KeyPart::UInt64(*id)]).expect("key"),
                values,
                *id,
                false,
            )
        })
        .collect();
    assert_pairs(
        include_str!("statement_warnings.pairs"),
        "wn",
        &schema(),
        rows,
        17,
    );
}
