//! Dates only a lenient source holds - February 30th, April 31st - replicate
//! as the values `MySQL` returns for them rather than as NULL.
//!
//! Every answer in `lenient_date_parity.pairs` was read from `MySQL` 8.4 over
//! the same rows, stored under `ALLOW_INVALID_DATES`.

use pintail_types::{Column, DataType, KeyPart, PrimaryKey, StoredRow, TableSchema, Value};

use crate::pairs_fixture::assert_pairs;

/// (id, d DATE, dt DATETIME) as the source sends them.
const ROWS: [(u64, Option<[&str; 2]>); 5] = [
    (1, Some(["2024-02-30", "2024-02-30 10:00:00"])),
    (2, Some(["2024-02-28", "2024-02-28 10:00:00"])),
    (3, Some(["2024-04-31", "2024-04-31 23:59:59"])),
    (4, Some(["2024-03-01", "2024-03-01 00:00:00"])),
    (5, None),
];

fn schema() -> TableSchema {
    TableSchema::new(
        1,
        vec![
            Column::new(1, "id", DataType::UInt64, false),
            Column::new(2, "d", DataType::Date32, true),
            Column::new(3, "dt", DataType::DateTime64 { fsp: 0 }, true),
        ],
    )
    .expect("schema")
}

#[test]
fn lenient_dates_answer_as_mysql_does() {
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
        include_str!("lenient_date_parity.pairs"),
        "v",
        &schema(),
        rows,
        16,
    );
}
