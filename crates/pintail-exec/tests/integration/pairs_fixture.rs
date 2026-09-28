//! Replays a `.pairs` fixture - answers read from `MySQL` 8.4 - against one
//! in-process table.
//!
//! A `Q:` line is a query; the `R:` lines after it are its rows,
//! tab-separated, as the command-line client prints them. Every mismatch is
//! reported at once, so one run shows the whole distance to `MySQL`.
//!
//! A fixture that carries `W:` lines - `SHOW WARNINGS` rows, deduplicated
//! and sorted - has every query's warnings checked too, a query with none
//! expecting none. They are compared as a set: how often `MySQL` repeats a
//! warning follows how often it re-evaluates an expression (an `ORDER BY`
//! doubles some), which is not a contract.

use pintail_catalog::{
    CatalogSnapshot, DatabaseEntry, DatabaseId, TableEntry, TableId, TableStatistics,
};
use pintail_exec::collation::Collation;
use pintail_exec::{Execution, LogicalPlanner, Optimizer, PhysicalPlanner, SnapshotScanProvider};
use pintail_sql::{Binder, parse_statement};
use pintail_store::{StoreOptions, TableStore};
use pintail_types::{StoredRow, TableSchema, Value};

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

/// Loads `rows` into a table named `table` and asserts every query in
/// `pairs` answers exactly as recorded.
pub fn assert_pairs(
    pairs: &str,
    table: &str,
    schema: &TableSchema,
    rows: Vec<StoredRow>,
    min_cases: usize,
) {
    let directory = tempfile::tempdir().expect("tempdir");
    let row_count = rows.len() as u64;
    let mut store =
        TableStore::open(directory.path(), schema.clone(), StoreOptions::default()).expect("open");
    store.bulk_ingest_snapshot(rows).expect("ingest");
    let snapshot = store.snapshot();
    let database_id = DatabaseId::new(1);
    let table_id = TableId::new(1);
    let entry = TableEntry::new(
        table_id,
        table,
        schema.clone(),
        TableStatistics::with_row_count(row_count),
    )
    .expect("entry");
    let database = DatabaseEntry::new(database_id, "app", [entry]).expect("database");
    let catalog = CatalogSnapshot::new([database]).expect("catalog");
    let provider =
        SnapshotScanProvider::new([(database_id, table_id, &snapshot)]).expect("provider");

    let check_warnings = pairs.lines().any(|line| line.starts_with("W: "));
    let mut failures = Vec::new();
    let mut cases = 0;
    for block in pairs.split("Q: ").filter(|block| !block.trim().is_empty()) {
        let mut lines = block.lines();
        let sql = lines.next().expect("query line").trim_end_matches(';');
        let lines: Vec<&str> = lines.collect();
        let expected = lines
            .iter()
            .filter_map(|line| line.strip_prefix("R: "))
            .collect::<Vec<_>>()
            .join("\n");
        let _ = pintail_exec::take_session_conversion_warnings();
        let actual = run(sql, &catalog, &provider);
        let mut warnings: Vec<String> = pintail_exec::take_session_conversion_warnings()
            .0
            .into_iter()
            .map(|warning| format!("Warning\t{}\t{}", warning.code, warning.message))
            .collect();
        warnings.sort();
        warnings.dedup();
        cases += 1;
        if actual != expected {
            failures.push(format!(
                "{sql}\n  pintail: {actual:?}\n  mysql:   {expected:?}"
            ));
        }
        let expected_warnings: Vec<&str> = lines
            .iter()
            .filter_map(|line| line.strip_prefix("W: "))
            .collect();
        if check_warnings && warnings != expected_warnings {
            failures.push(format!(
                "{sql}\n  pintail warnings: {warnings:?}\n  mysql warnings:   {expected_warnings:?}"
            ));
        }
    }
    assert!(cases >= min_cases, "the fixture lost its cases");
    assert!(
        failures.is_empty(),
        "{} of {cases} differ:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
