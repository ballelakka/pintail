//! Row-by-row text work inside a grouped query: `general_ci` comparisons
//! in CASE arms and a time-zone conversion in the grouping key. Each is
//! evaluated once per row, so its per-call cost is the query's cost.
//!
//! The measurement is `#[ignore]`d:
//! `cargo test --profile recovery -p pintail-exec --test integration row_path_text_cost::
//! -- --ignored --nocapture`.

use pintail_catalog::{
    CatalogSnapshot, DatabaseEntry, DatabaseId, TableEntry, TableId, TableStatistics,
};
use pintail_exec::collation::Collation;
use pintail_exec::{Execution, LogicalPlanner, Optimizer, PhysicalPlanner, SnapshotScanProvider};
use pintail_sql::{Binder, parse_statement};
use pintail_store::{StoreOptions, TableStore};
use pintail_types::{Column, DataType, KeyPart, PrimaryKey, StoredRow, TableSchema, Value};

const ROWS: u64 = 100_000;
const KINDS: [&str; 4] = ["alpha", "Beta", "gamma", "delta"];

fn schema() -> TableSchema {
    TableSchema::new(
        1,
        vec![
            Column::new(1, "id", DataType::UInt64, false),
            Column::new(2, "owner", DataType::Int64, false),
            Column::new(3, "kind", DataType::Utf8, false)
                .with_collation(Some("utf8mb4_general_ci".to_owned())),
            Column::new(4, "at", DataType::DateTime64 { fsp: 0 }, false),
        ],
    )
    .expect("schema")
}

fn row(id: u64) -> StoredRow {
    let day = id % 400;
    StoredRow::new(
        PrimaryKey::new(vec![KeyPart::UInt64(id)]).expect("key"),
        vec![
            Value::UInt64(id),
            Value::Int64(i64::try_from(id % 50).expect("owner")),
            Value::Utf8(KINDS[usize::try_from(id % 4).expect("kind")].to_owned()),
            Value::Utf8(format!(
                "2025-{:02}-{:02} {:02}:{:02}:00",
                day / 31 % 12 + 1,
                day % 28 + 1,
                id % 24,
                id % 60
            )),
        ],
        id,
        false,
    )
}

const QUERY: &str = "SELECT owner, YEAR(CONVERT_TZ(at, '+00:00', '+05:30')) AS y, \
     MONTH(CONVERT_TZ(at, '+00:00', '+05:30')) AS m, \
     COUNT(DISTINCT CASE WHEN kind IN ('ALPHA', 'beta') THEN id END), \
     COUNT(DISTINCT CASE WHEN kind = 'Gamma' THEN id END), \
     COUNT(DISTINCT CASE WHEN kind = 'DELTA ' THEN id END) \
     FROM events GROUP BY owner, y, m";

fn run(table: &TableStore, catalog: &CatalogSnapshot) -> Vec<Vec<Value>> {
    let snapshot = table.snapshot();
    let provider = SnapshotScanProvider::new([(DatabaseId::new(1), TableId::new(1), &snapshot)])
        .expect("provider");
    let bound = Binder::new(catalog, Some("app"))
        .bind(&parse_statement(QUERY).expect("parse"))
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
        for index in batch.selection().selected_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|column| column.value(index).cloned().expect("value"))
                    .collect(),
            );
        }
    }
    rows
}

#[test]
#[ignore = "measurement"]
fn measure_text_work_per_row_in_a_grouped_query() {
    let directory = tempfile::tempdir().expect("directory");
    let mut table =
        TableStore::open(directory.path(), schema(), StoreOptions::default()).expect("table");
    table
        .bulk_ingest_snapshot((1..=ROWS).map(row).collect())
        .expect("rows");
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
    for _ in 0..5 {
        let started = std::time::Instant::now();
        let rows = run(&table, &catalog);
        eprintln!(
            "grouped text work: {} groups in {:.1} ms",
            rows.len(),
            started.elapsed().as_secs_f64() * 1000.0
        );
    }
}
