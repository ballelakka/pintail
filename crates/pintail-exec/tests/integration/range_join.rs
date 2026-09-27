//! Joins whose only link between the inputs is an inequality - `<`, `<=`,
//! `>`, `>=` or `BETWEEN` - sort the right input by it and search it per
//! left row, so a band join reads the rows its bounds reach instead of
//! testing every pair; the full condition still decides each pair.

use pintail_catalog::{
    CatalogSnapshot, DatabaseEntry, DatabaseId, TableEntry, TableId, TableStatistics,
};
use pintail_exec::collation::Collation;
use pintail_exec::{Execution, LogicalPlanner, Optimizer, PhysicalPlanner, SnapshotScanProvider};
use pintail_sql::{Binder, parse_statement};
use pintail_store::{StoreOptions, TableStore};
use pintail_types::{Column, DataType, KeyPart, PrimaryKey, StoredRow, TableSchema, Value};

fn schema() -> TableSchema {
    TableSchema::new(
        1,
        vec![
            Column::new(1, "id", DataType::UInt64, false),
            Column::new(2, "a", DataType::Int64, true),
            Column::new(3, "d", DataType::Date32, true),
        ],
    )
    .expect("schema")
}

fn render(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_owned(),
        Value::UInt64(number) => number.to_string(),
        Value::Int64(number) => number.to_string(),
        Value::Utf8(text) => text.clone(),
        other => format!("{other:?}"),
    }
}

/// Runs `sql` over `rows` of (id, a, d).
fn run(rows: &[(u64, Option<i64>, Option<&str>)], sql: &str) -> Vec<String> {
    let catalog = CatalogSnapshot::new([DatabaseEntry::new(
        DatabaseId::new(41),
        "app",
        [TableEntry::new(
            TableId::new(42),
            "t",
            schema(),
            TableStatistics::with_row_count(rows.len() as u64),
        )
        .expect("table entry")],
    )
    .expect("database entry")])
    .expect("catalog");
    let dir = tempfile::tempdir().expect("temporary table");
    let mut table = TableStore::open(dir.path(), schema(), StoreOptions::default()).expect("open");
    table
        .bulk_ingest_snapshot(
            rows.iter()
                .map(|(id, a, d)| {
                    StoredRow::new(
                        PrimaryKey::new(vec![KeyPart::UInt64(*id)]).expect("key"),
                        vec![
                            Value::UInt64(*id),
                            a.map_or(Value::Null, Value::Int64),
                            d.map_or(Value::Null, |d| Value::Utf8(d.to_owned())),
                        ],
                        *id,
                        false,
                    )
                })
                .collect(),
        )
        .expect("bulk rows");
    let snapshot = table.snapshot();
    let provider = SnapshotScanProvider::new([(DatabaseId::new(41), TableId::new(42), &snapshot)])
        .expect("provider");
    let statement = parse_statement(sql).expect("parse");
    let bound = Binder::new(&catalog, Some("app"))
        .bind(&statement)
        .unwrap_or_else(|error| panic!("bind {sql}: {error}"));
    let physical = PhysicalPlanner::plan(
        Optimizer::optimize(LogicalPlanner::plan(bound)),
        Collation::default(),
    )
    .expect("physical plan");
    let mut execution =
        Execution::start(physical, &provider, 512 * 1024 * 1024, Collation::default())
            .expect("start execution");
    let mut out = Vec::new();
    while let Some(batch) = execution
        .next_batch()
        .unwrap_or_else(|error| panic!("pull batch for {sql}: {error}"))
    {
        let columns = batch.columns().len();
        for row in batch.selection().selected_rows() {
            out.push(
                (0..columns)
                    .map(|column| {
                        render(
                            batch
                                .column(column)
                                .and_then(|column| column.value(row))
                                .expect("selected value"),
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(","),
            );
        }
    }
    out
}

const SMALL: [(u64, Option<i64>, Option<&str>); 4] = [
    (1, Some(1), Some("2024-01-01")),
    (2, Some(1), Some("2024-01-05")),
    (3, Some(2), None),
    (4, None, Some("2024-01-03")),
];

#[test]
fn inequality_joins_answer_every_kind() {
    assert_eq!(
        run(
            &SMALL,
            "SELECT x.id, y.id FROM t AS x JOIN t AS y ON x.a < y.a ORDER BY x.id, y.id"
        ),
        ["1,3", "2,3"]
    );
    assert_eq!(
        run(
            &SMALL,
            "SELECT x.id, y.id FROM t AS x JOIN t AS y ON y.id BETWEEN x.id AND x.id + 1 \
             ORDER BY x.id, y.id"
        ),
        ["1,1", "1,2", "2,2", "2,3", "3,3", "3,4", "4,4"]
    );
    assert_eq!(
        run(
            &SMALL,
            "SELECT x.id, y.id FROM t AS x LEFT JOIN t AS y ON x.a > y.a ORDER BY x.id, y.id"
        ),
        ["1,NULL", "2,NULL", "3,1", "3,2", "4,NULL"]
    );
    assert_eq!(
        run(
            &SMALL,
            "SELECT x.id, y.id FROM t AS x JOIN t AS y ON y.d >= x.d AND y.d < '2024-01-05' \
             AND x.id <> y.id ORDER BY x.id, y.id"
        ),
        ["1,4"]
    );
}

#[test]
fn a_band_join_reads_only_the_rows_its_bounds_reach() {
    // 30,000 rows against themselves is 900 million pairs; the band keeps
    // three per row. Testing every pair would not finish in any test budget.
    let rows = (1..=30_000_u64)
        .map(|id| (id, Some(i64::try_from(id).expect("small")), None))
        .collect::<Vec<_>>();
    let started = std::time::Instant::now();
    assert_eq!(
        run(
            &rows,
            "SELECT COUNT(*) FROM t AS x JOIN t AS y ON y.a BETWEEN x.a AND x.a + 2"
        ),
        ["89997"]
    );
    assert!(
        started.elapsed() < std::time::Duration::from_secs(60),
        "the band join took {:?}",
        started.elapsed()
    );
}
