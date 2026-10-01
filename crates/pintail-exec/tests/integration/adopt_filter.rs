//! A scan tests its own predicates on the worker that adopts each chunk
//! and hands the Filters above a narrowed, prefiltered batch. Whatever the
//! predicate - one the packed kernels answer, one they decline, NULLs on
//! either side, a range written as two conjuncts - the rows that come out
//! must be exactly the rows the predicate keeps, row by row.

use pintail_catalog::{
    CatalogSnapshot, DatabaseEntry, DatabaseId, TableEntry, TableId, TableStatistics,
};
use pintail_exec::collation::Collation;
use pintail_exec::{Execution, LogicalPlanner, Optimizer, PhysicalPlanner, SnapshotScanProvider};
use pintail_sql::{Binder, parse_statement};
use pintail_store::{StoreOptions, TableStore};
use pintail_types::{Column, DataType, KeyPart, PrimaryKey, StoredRow, TableSchema, Value};

const ROWS: u64 = 300_000;
const LABELS: [&str; 4] = ["amber", "Basil", "basil", "cedar"];

fn schema() -> TableSchema {
    TableSchema::new(
        1,
        vec![
            Column::new(1, "id", DataType::UInt64, false),
            Column::new(2, "label", DataType::Utf8, true),
            Column::new(3, "n", DataType::Int64, true),
            Column::new(4, "day", DataType::Date32, true),
        ],
    )
    .expect("schema")
}

fn label(id: u64) -> Option<&'static str> {
    (id % 11 != 3).then(|| LABELS[usize::try_from(id % 4).expect("small")])
}

fn n(id: u64) -> Option<i64> {
    (id % 13 != 5).then(|| i64::try_from(id % 9).expect("small"))
}

fn day(id: u64) -> Option<u32> {
    (id % 17 != 2).then(|| u32::try_from(1 + id % 28).expect("small"))
}

fn fixture() -> (tempfile::TempDir, TableStore, CatalogSnapshot) {
    let directory = tempfile::tempdir().expect("directory");
    let mut table =
        TableStore::open(directory.path(), schema(), StoreOptions::default()).expect("table");
    let mut start = 0;
    while start < ROWS {
        let end = (start + 50_000).min(ROWS);
        table
            .bulk_ingest_snapshot(
                (start..end)
                    .map(|id| {
                        StoredRow::new(
                            PrimaryKey::new(vec![KeyPart::UInt64(id)]).expect("key"),
                            vec![
                                Value::UInt64(id),
                                label(id).map_or(Value::Null, |text| Value::Utf8(text.to_owned())),
                                n(id).map_or(Value::Null, Value::Int64),
                                day(id).map_or(Value::Null, |day| {
                                    Value::Utf8(format!("2024-02-{day:02}"))
                                }),
                            ],
                            id + 1,
                            false,
                        )
                    })
                    .collect(),
            )
            .expect("rows");
        start = end;
    }
    let entry = TableEntry::new(
        TableId::new(1),
        "items",
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
    (directory, table, catalog)
}

fn first_column(table: &TableStore, catalog: &CatalogSnapshot, sql: &str) -> Vec<Value> {
    let snapshot = table.snapshot();
    let provider = SnapshotScanProvider::new([(DatabaseId::new(1), TableId::new(1), &snapshot)])
        .expect("provider");
    let bound = Binder::new(catalog, Some("app"))
        .bind(&parse_statement(sql).expect("parse"))
        .expect("bind");
    let physical = PhysicalPlanner::plan(
        Optimizer::optimize(LogicalPlanner::plan(bound)),
        Collation::default(),
    )
    .expect("plan");
    let mut execution =
        Execution::start(physical, &provider, 1 << 30, Collation::default()).expect("start");
    let mut rows = Vec::new();
    while let Some(batch) = execution.next_batch().expect("batch") {
        for row in batch.selection().selected_rows() {
            rows.push(batch.columns()[0].value_owned(row).expect("value"));
        }
    }
    rows
}

fn ids(table: &TableStore, catalog: &CatalogSnapshot, sql: &str) -> Vec<u64> {
    let mut ids: Vec<u64> = first_column(table, catalog, sql)
        .into_iter()
        .map(|value| match value {
            Value::UInt64(id) => id,
            other => panic!("unexpected id {other:?}"),
        })
        .collect();
    ids.sort_unstable();
    ids
}

/// Which ids a case's predicate keeps, computed from the generator.
type Keep = Box<dyn Fn(u64) -> bool>;

#[test]
fn scan_side_selection_keeps_exactly_the_rows_each_predicate_keeps() {
    let (_directory, table, catalog) = fixture();
    // The default collation is case- and accent-insensitive, so 'basil'
    // matches both spellings.
    let basil = |id: u64| label(id).is_some_and(|text| text.eq_ignore_ascii_case("basil"));
    let cases: Vec<(&str, Keep)> = vec![
        (
            "SELECT id, label FROM items WHERE label = 'basil'",
            Box::new(basil),
        ),
        (
            "SELECT id, label FROM items WHERE label <> 'basil'",
            Box::new(move |id| label(id).is_some() && !basil(id)),
        ),
        (
            "SELECT id, n FROM items WHERE n >= 2 AND n < 5",
            Box::new(|id| n(id).is_some_and(|n| (2..5).contains(&n))),
        ),
        (
            "SELECT id, n FROM items WHERE 5 > n AND n > 2",
            Box::new(|id| n(id).is_some_and(|n| n > 2 && n < 5)),
        ),
        (
            "SELECT id, day FROM items WHERE day BETWEEN '2024-02-03' AND '2024-02-09'",
            Box::new(|id| day(id).is_some_and(|day| (3..=9).contains(&day))),
        ),
        (
            "SELECT id, label FROM items WHERE label = 'basil' AND n >= 4 AND n <= 4",
            Box::new(move |id| basil(id) && n(id) == Some(4)),
        ),
        // The kernels do not answer a function of the column; the Filter
        // above still decides every row.
        (
            "SELECT id, label FROM items WHERE UPPER(label) = 'BASIL' AND n > 6",
            Box::new(move |id| basil(id) && n(id).is_some_and(|n| n > 6)),
        ),
        (
            "SELECT id, n FROM items WHERE n IS NULL OR n = 0",
            Box::new(|id| n(id).is_none_or(|n| n == 0)),
        ),
    ];
    for (sql, keep) in cases {
        let expected: Vec<u64> = (0..ROWS).filter(|id| keep(*id)).collect();
        assert_eq!(ids(&table, &catalog, sql), expected, "{sql}");
    }
    // A projection of the predicate column alone builds no filter-first
    // decode; the adopted batches are narrowed all the same.
    let expected = (0..ROWS).filter(|id| basil(*id)).count();
    assert_eq!(
        first_column(
            &table,
            &catalog,
            "SELECT COUNT(*) FROM items WHERE label = 'basil'"
        ),
        [Value::UInt64(u64::try_from(expected).expect("small"))]
    );
}
