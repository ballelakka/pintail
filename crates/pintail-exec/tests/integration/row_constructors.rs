//! Row constructors in comparisons and IN lists, with `MySQL`'s NULL answers:
//! a pair that cannot be decided leaves the row comparison NULL unless
//! another pair settles it.

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
        "SELECT id FROM pairs WHERE {predicate} ORDER BY id"
    ))
}

#[test]
fn equality_and_inequality_follow_mysql_null_answers() {
    assert_eq!(ids("(a, b) = (1, 2)"), ["2"]);
    // (NULL, 1) <> (1, 2) is true: the second pair already differs.
    assert_eq!(ids("(a, b) <> (1, 2)"), ["1", "3", "4"]);
    assert_eq!(ids("ROW(a, b) <=> ROW(NULL, 1)"), ["4"]);
}

#[test]
fn ordering_compares_lexicographically() {
    assert_eq!(ids("(a, b) < (2, 1)"), ["1", "2"]);
    assert_eq!(ids("(a, b) >= (1, 2)"), ["2", "3"]);
    assert_eq!(ids("(a, b) <= (1, 2)"), ["1", "2"]);
    assert_eq!(ids("(a, b) > (1, 1)"), ["2", "3"]);
}

#[test]
fn in_lists_of_rows() {
    assert_eq!(ids("(a, b) IN ((1, 1), (2, 1))"), ["1", "3"]);
    // Row 4 is NULL, not true, under NOT IN: its first pair is undecided.
    assert_eq!(ids("(a, b) NOT IN ((1, 1), (2, 1))"), ["2"]);
}

#[test]
fn constant_rows_answer_null_where_mysql_does() {
    assert_eq!(
        run(
            "SELECT (1, NULL) = (1, 2), (1, NULL) = (2, 2), (1, 2) <=> (1, NULL), \
             (2, NULL) > (1, 5), (1, NULL) NOT IN ((1, 2), (3, 4))"
        ),
        ["NULL,0,0,1,NULL"]
    );
}

#[test]
fn rows_of_different_widths_are_refused() {
    for sql in [
        "SELECT (1, 2) = (1, 2, 3)",
        "SELECT (1, 2) = 1",
        "SELECT id FROM pairs WHERE (a, b) IN ((1, 2), (3, 4, 5))",
    ] {
        assert!(bind(sql).is_err(), "{sql} must be refused");
    }
}

#[test]
fn rows_in_subqueries() {
    // Members (1, 1), (2, 1), (1, 2), (1, NULL): row 4's (NULL, 1) is
    // undecided against each, so it is NULL rather than true.
    assert_eq!(ids("(a, b) IN (SELECT b, a FROM pairs)"), ["1", "2", "3"]);
    assert_eq!(ids("(a, b) IN (SELECT 1, 2 UNION SELECT 2, 1)"), ["2", "3"]);
    assert_eq!(
        ids("(a, b) NOT IN (SELECT b, a FROM pairs WHERE id IN (1, 2))"),
        ["2"]
    );
    assert_eq!(
        run(
            "SELECT id, (a, b) IN (SELECT b, a FROM pairs WHERE id IN (1, 2)), \
             (a, b) NOT IN (SELECT b, a FROM pairs WHERE id IN (1, 2)) FROM pairs ORDER BY id"
        ),
        ["1,1,0", "2,0,1", "3,1,0", "4,NULL,NULL"]
    );
    assert_eq!(
        ids("(a, b) IN (SELECT p.b, p.a FROM pairs AS p WHERE p.id = pairs.id)"),
        ["1"]
    );
    assert!(bind("SELECT id FROM pairs WHERE (a, b) IN (SELECT id FROM pairs)").is_err());
}
