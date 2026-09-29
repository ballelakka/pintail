//! GROUP BY over date parts of a packed temporal column. Year, month, day,
//! hour, minute and second read straight off the packed units; a calendar
//! part such as DAYOFWEEK has no such reading and must take the general
//! path. Admitting it to the packed-units key failed every non-empty input
//! with "date-part group key column lost its packed units" - an empty
//! input hid it, because nothing reached the scatter.

use std::collections::BTreeMap;

use pintail_catalog::{
    CatalogSnapshot, DatabaseEntry, DatabaseId, TableEntry, TableId, TableStatistics,
};
use pintail_exec::collation::Collation;
use pintail_exec::{Execution, LogicalPlanner, Optimizer, PhysicalPlanner, SnapshotScanProvider};
use pintail_sql::{Binder, parse_statement};
use pintail_store::{StoreOptions, TableStore};
use pintail_types::{Column, DataType, KeyPart, PrimaryKey, StoredRow, TableSchema, Value};

const ROWS: u64 = 20_000;

fn schema() -> TableSchema {
    TableSchema::new(
        1,
        vec![
            Column::new(1, "id", DataType::UInt64, false),
            Column::new(2, "at", DataType::DateTime64 { fsp: 0 }, true),
        ],
    )
    .expect("schema")
}

/// Row `id` falls on day `id % 9` of September 2026 (the 1st is a Tuesday)
/// at hour `id % 24`; every 97th row is NULL.
fn stamp(id: u64) -> Option<(u64, u64)> {
    (!id.is_multiple_of(97)).then_some((1 + id % 9, id % 24))
}

fn run(sql: &str) -> Vec<String> {
    let directory = tempfile::tempdir().expect("directory");
    let mut table =
        TableStore::open(directory.path(), schema(), StoreOptions::default()).expect("table");
    let stored = (0..ROWS)
        .map(|id| {
            let at = stamp(id).map_or(Value::Null, |(day, hour)| {
                Value::Utf8(format!("2026-09-{day:02} {hour:02}:30:00"))
            });
            StoredRow::new(
                PrimaryKey::new(vec![KeyPart::UInt64(id)]).expect("key"),
                vec![Value::UInt64(id), at],
                id + 1,
                false,
            )
        })
        .collect::<Vec<_>>();
    table.bulk_ingest_snapshot(stored).expect("rows");
    let entry = TableEntry::new(
        TableId::new(1),
        "events",
        schema(),
        TableStatistics::with_row_count(ROWS),
    )
    .expect("entry")
    .with_key_columns([1])
    .expect("key");
    let catalog = CatalogSnapshot::new([
        DatabaseEntry::new(DatabaseId::new(1), "app", [entry]).expect("database")
    ])
    .expect("catalog");
    let snapshot = table.snapshot();
    let provider = SnapshotScanProvider::new([(DatabaseId::new(1), TableId::new(1), &snapshot)])
        .expect("provider");
    let bound = Binder::new(&catalog, Some("app"))
        .bind(&parse_statement(sql).expect("parse"))
        .unwrap_or_else(|error| panic!("bind {sql}: {error}"));
    let physical = PhysicalPlanner::plan(
        Optimizer::optimize(LogicalPlanner::plan(bound)),
        Collation::default(),
    )
    .expect("plan");
    let mut execution =
        Execution::start(physical, &provider, 1 << 31, Collation::default()).expect("start");
    let mut rows = Vec::new();
    while let Some(batch) = execution
        .next_batch()
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
    {
        for row in batch.selection().selected_rows() {
            rows.push(
                (0..batch.columns().len())
                    .map(|column| {
                        let value = batch
                            .column(column)
                            .and_then(|column| column.value_owned(row))
                            .expect("value");
                        match value {
                            Value::Int64(value) => value.to_string(),
                            Value::UInt64(value) => value.to_string(),
                            Value::Null => "Null".to_owned(),
                            other => format!("{other:?}"),
                        }
                    })
                    .collect::<Vec<_>>()
                    .join("|"),
            );
        }
    }
    rows
}

/// The expected answer, keyed by `(first part, second part)` with NULL
/// sorting first as the source database orders it.
fn expected(parts: impl Fn(u64, u64) -> (u64, u64)) -> Vec<String> {
    let mut groups: BTreeMap<Option<(u64, u64)>, u64> = BTreeMap::new();
    for id in 0..ROWS {
        *groups
            .entry(stamp(id).map(|(day, hour)| parts(day, hour)))
            .or_default() += 1;
    }
    groups
        .into_iter()
        .map(|(key, count)| match key {
            Some((first, second)) => format!("{first}|{second}|{count}"),
            None => format!("Null|Null|{count}"),
        })
        .collect()
}

#[test]
fn a_calendar_part_beside_an_hour_groups_on_the_general_path() {
    // September 1st 2026 is a Tuesday: DAYOFWEEK counts Sunday as 1.
    let day_of_week = |day: u64| (day + 1) % 7 + 1;
    assert_eq!(
        run(
            "SELECT DAYOFWEEK(at) AS dw, HOUR(at) AS h, COUNT(*) FROM events \
             GROUP BY dw, h ORDER BY dw, h"
        ),
        expected(|day, hour| (day_of_week(day), hour)),
    );
}

#[test]
fn packed_parts_keep_their_answer() {
    assert_eq!(
        run("SELECT DAY(at) AS d, HOUR(at) AS h, COUNT(*) FROM events \
             GROUP BY d, h ORDER BY d, h"),
        expected(|day, hour| (day, hour)),
    );
}
