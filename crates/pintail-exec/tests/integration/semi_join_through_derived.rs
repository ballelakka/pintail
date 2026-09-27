//! Correlated `EXISTS`, `NOT EXISTS` and `IN` whose subquery joins tables or
//! groups (under `EXISTS`) run as a semi or anti join against a derived
//! table instead of once per outer row, with the same answers.

use pintail_catalog::{
    CatalogSnapshot, DatabaseEntry, DatabaseId, TableEntry, TableId, TableStatistics,
};
use pintail_exec::collation::Collation;
use pintail_exec::{Execution, LogicalPlanner, Optimizer, PhysicalPlanner, SnapshotScanProvider};
use pintail_sql::{Binder, parse_statement};
use pintail_store::{StoreOptions, TableStore};
use pintail_types::{Column, DataType, KeyPart, PrimaryKey, StoredRow, TableSchema, Value};

/// (id, a, b), with one NULL `a`.
const ROWS: [(u64, Option<i64>, i64); 4] = [
    (1, Some(1), 1),
    (2, Some(1), 2),
    (3, Some(2), 1),
    (4, None, 1),
];

fn schema() -> TableSchema {
    TableSchema::new(
        1,
        vec![
            Column::new(1, "id", DataType::UInt64, false),
            Column::new(2, "a", DataType::Int64, true),
            Column::new(3, "b", DataType::Int64, false),
        ],
    )
    .expect("schema")
}

fn catalog() -> CatalogSnapshot {
    let database = DatabaseEntry::new(
        DatabaseId::new(31),
        "app",
        [TableEntry::new(
            TableId::new(32),
            "pairs",
            schema(),
            TableStatistics::with_row_count(ROWS.len() as u64),
        )
        .expect("pairs entry")],
    )
    .expect("database entry");
    CatalogSnapshot::new([database]).expect("catalog")
}

fn bind(sql: &str) -> Result<pintail_sql::BoundQuery, String> {
    let statement = parse_statement(sql).map_err(|error| error.to_string())?;
    Binder::new(&catalog(), Some("app"))
        .bind(&statement)
        .map_err(|error| error.to_string())
}

fn render(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_owned(),
        Value::UInt64(number) => number.to_string(),
        Value::Int64(number) => number.to_string(),
        Value::Boolean(flag) => u8::from(*flag).to_string(),
        other => format!("{other:?}"),
    }
}

fn run(sql: &str) -> Vec<String> {
    let dir = tempfile::tempdir().expect("temporary table");
    let mut pairs =
        TableStore::open(dir.path(), schema(), StoreOptions::default()).expect("open pairs");
    pairs
        .bulk_ingest_snapshot(
            ROWS.iter()
                .map(|(id, a, b)| {
                    StoredRow::new(
                        PrimaryKey::new(vec![KeyPart::UInt64(*id)]).expect("key"),
                        vec![
                            Value::UInt64(*id),
                            a.map_or(Value::Null, Value::Int64),
                            Value::Int64(*b),
                        ],
                        *id,
                        false,
                    )
                })
                .collect(),
        )
        .expect("bulk pairs");
    let snapshot = pairs.snapshot();
    let provider = SnapshotScanProvider::new([(DatabaseId::new(31), TableId::new(32), &snapshot)])
        .expect("provider");
    let bound = bind(sql).unwrap_or_else(|error| panic!("bind {sql}: {error}"));
    let physical = PhysicalPlanner::plan(
        Optimizer::optimize(LogicalPlanner::plan(bound)),
        Collation::default(),
    )
    .expect("physical plan");
    let mut execution =
        Execution::start(physical, &provider, 64 * 1024 * 1024, Collation::default())
            .expect("start execution");
    let mut rows = Vec::new();
    while let Some(batch) = execution
        .next_batch()
        .unwrap_or_else(|error| panic!("pull batch for {sql}: {error}"))
    {
        let columns = batch.columns().len();
        for row in batch.selection().selected_rows() {
            rows.push(
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
    rows
}

fn ids(predicate: &str) -> Vec<String> {
    run(&format!(
        "SELECT id FROM pairs AS o WHERE {predicate} ORDER BY id"
    ))
}

fn joins_through_derived(predicate: &str) -> bool {
    let bound = bind(&format!("SELECT id FROM pairs AS o WHERE {predicate}"))
        .unwrap_or_else(|error| panic!("bind {predicate}: {error}"));
    format!("{bound:?}").contains("<semi-join-")
}

#[test]
fn exists_over_a_join_is_a_semi_join() {
    let exists = "EXISTS (SELECT 1 FROM pairs AS p JOIN pairs AS q ON q.id = p.id \
                  WHERE p.a = o.b AND q.b > 1)";
    assert!(joins_through_derived(exists));
    assert_eq!(ids(exists), ["1", "3", "4"]);
    let not_exists = format!("NOT {exists}");
    assert!(joins_through_derived(&not_exists));
    assert_eq!(ids(&not_exists), ["2"]);
}

#[test]
fn exists_over_a_grouping_is_a_semi_join() {
    let grouped = "EXISTS (SELECT p.a FROM pairs AS p WHERE p.b = o.a GROUP BY p.a)";
    assert!(joins_through_derived(grouped));
    assert_eq!(ids(grouped), ["1", "2", "3"]);
}

#[test]
fn in_over_a_join_is_a_semi_join() {
    let membership = "o.a IN (SELECT p.b FROM pairs AS p JOIN pairs AS q ON q.id = p.id \
                      WHERE p.id <> o.id)";
    assert!(joins_through_derived(membership));
    assert_eq!(ids(membership), ["1", "2", "3"]);
}

#[test]
fn shapes_the_rewrite_cannot_read_keep_their_answers() {
    // Correlated inside the join's ON, an ungrouped aggregate, and a
    // LIMIT: none is read as a semi join, and each answers as MySQL does.
    let on_clause =
        "EXISTS (SELECT 1 FROM pairs AS p JOIN pairs AS q ON q.id = p.id AND q.a = o.b)";
    assert!(!joins_through_derived(on_clause));
    assert_eq!(ids(on_clause), ["1", "2", "3", "4"]);
    let aggregate = "EXISTS (SELECT MAX(p.a) FROM pairs AS p JOIN pairs AS q ON q.id = p.id \
                     WHERE p.a = o.b + 10)";
    assert!(!joins_through_derived(aggregate));
    assert_eq!(ids(aggregate), ["1", "2", "3", "4"]);
}
