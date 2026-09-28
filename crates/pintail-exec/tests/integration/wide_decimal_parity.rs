//! DECIMAL columns wider than 38 digits compute exactly, as `MySQL` does up to
//! its 65-digit ceiling.
//!
//! Every answer in `wide_decimal_parity.pairs` was read from `MySQL` 8.4 over
//! the same rows: a `Q:` line is a query, the `R:` lines after it are its
//! rows, tab-separated, as the command-line client prints them.

use pintail_catalog::{
    CatalogSnapshot, DatabaseEntry, DatabaseId, TableEntry, TableId, TableStatistics,
};
use pintail_exec::collation::Collation;
use pintail_exec::{Execution, LogicalPlanner, Optimizer, PhysicalPlanner, SnapshotScanProvider};
use pintail_sql::{Binder, parse_statement};
use pintail_store::{StoreOptions, TableStore};
use pintail_types::{Column, DataType, KeyPart, PrimaryKey, StoredRow, TableSchema, Value};

const PAIRS: &str = include_str!("wide_decimal_parity.pairs");

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
    TableSchema::new(
        1,
        vec![
            Column::new(1, "id", DataType::UInt64, false),
            Column::new(
                2,
                "a",
                DataType::Decimal {
                    precision: 65,
                    scale: 30,
                },
                true,
            ),
            Column::new(
                3,
                "b",
                DataType::Decimal {
                    precision: 50,
                    scale: 0,
                },
                true,
            ),
            Column::new(
                4,
                "c",
                DataType::Decimal {
                    precision: 40,
                    scale: 5,
                },
                true,
            ),
        ],
    )
    .expect("schema")
}

fn render(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_owned(),
        Value::Utf8(text) => text.clone(),
        Value::Int64(number) => number.to_string(),
        Value::UInt64(number) => number.to_string(),
        Value::Boolean(flag) => u8::from(*flag).to_string(),
        Value::Float64(number) => number.mysql_text(),
        Value::DecimalAverage(average) => average.label.clone(),
        other => format!("{other:?}"),
    }
}

fn run(sql: &str, catalog: &CatalogSnapshot, provider: &SnapshotScanProvider<'_>) -> String {
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let statement = parse_statement(sql).expect("parse");
        let bound = Binder::new(catalog, Some("app"))
            .bind(&statement)
            .map_err(|error| format!("bind error: {error}"))?;
        let physical = PhysicalPlanner::plan(
            Optimizer::optimize(LogicalPlanner::plan(bound)),
            Collation::default(),
        )
        .map_err(|error| format!("plan error: {error}"))?;
        let mut execution =
            Execution::start(physical, provider, 64 * 1024 * 1024, Collation::default())
                .map_err(|error| format!("start error: {error}"))?;
        let mut lines = Vec::new();
        while let Some(batch) = execution
            .next_batch()
            .map_err(|error| format!("execution error: {error}"))?
        {
            for row in batch.selection().selected_rows() {
                let values: Vec<String> = (0..batch.columns().len())
                    .map(|column| {
                        batch
                            .column(column)
                            .and_then(|column| column.value(row))
                            .map_or_else(|| "NULL".to_owned(), render)
                    })
                    .collect();
                lines.push(values.join("\t"));
            }
        }
        Ok::<_, String>(lines.join("\n"))
    }));
    match outcome {
        Ok(Ok(text) | Err(text)) => text,
        Err(_) => "panic".to_owned(),
    }
}

#[test]
fn wide_decimals_answer_as_mysql_does() {
    let directory = tempfile::tempdir().expect("tempdir");
    let mut table =
        TableStore::open(directory.path(), schema(), StoreOptions::default()).expect("open");
    table
        .bulk_ingest_snapshot(
            ROWS.iter()
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
                .collect(),
        )
        .expect("ingest");
    let snapshot = table.snapshot();
    let database_id = DatabaseId::new(1);
    let table_id = TableId::new(1);
    let entry = TableEntry::new(
        table_id,
        "w",
        schema(),
        TableStatistics::with_row_count(ROWS.len() as u64),
    )
    .expect("entry");
    let database = DatabaseEntry::new(database_id, "app", [entry]).expect("database");
    let catalog = CatalogSnapshot::new([database]).expect("catalog");
    let provider =
        SnapshotScanProvider::new([(database_id, table_id, &snapshot)]).expect("provider");

    let mut failures = Vec::new();
    let mut cases = 0;
    for block in PAIRS.split("Q: ").filter(|block| !block.trim().is_empty()) {
        let mut lines = block.lines();
        let sql = lines.next().expect("query line").trim_end_matches(';');
        let expected = lines
            .filter_map(|line| line.strip_prefix("R: "))
            .collect::<Vec<_>>()
            .join("\n");
        let actual = run(sql, &catalog, &provider);
        cases += 1;
        if actual != expected {
            failures.push(format!(
                "{sql}\n  pintail: {actual:?}\n  mysql:   {expected:?}"
            ));
        }
    }
    assert!(cases >= 26, "the fixture lost its cases");
    assert!(
        failures.is_empty(),
        "{} of {cases} differ:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
