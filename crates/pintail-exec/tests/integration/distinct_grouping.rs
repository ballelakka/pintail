//! DISTINCT over many rows with few distinct values groups the rows before
//! sorting them, so the sort orders a few hundred rows rather than every
//! input row. The answer must be the one the sort alone gave: each set of
//! equal rows answered by the first of them to arrive, folded under each
//! column's own collation, in sorted order.
//!
//! The measurement is `#[ignore]`d:
//! `cargo test --profile recovery -p pintail-exec --test integration distinct_grouping::
//! -- --ignored --nocapture`.

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
            Column::new(2, "name", DataType::Utf8, true)
                .with_collation(Some("utf8mb4_0900_ai_ci".to_owned())),
            Column::new(3, "code", DataType::Utf8, true)
                .with_collation(Some("utf8mb4_bin".to_owned())),
            Column::new(4, "grp", DataType::Int64, true),
            Column::new(5, "at", DataType::DateTime64 { fsp: 0 }, true),
            Column::new(6, "ratio", DataType::Float64, true),
        ],
    )
    .expect("schema")
}

type Row = (
    Option<&'static str>,
    Option<&'static str>,
    Option<i64>,
    Option<&'static str>,
    Option<f64>,
);

struct Fixture {
    _directory: tempfile::TempDir,
    table: TableStore,
    catalog: CatalogSnapshot,
}

fn text(value: Option<&str>) -> Value {
    value.map_or(Value::Null, |value| Value::Utf8(value.to_owned()))
}

impl Fixture {
    fn new(rows: impl IntoIterator<Item = Row>) -> Self {
        let directory = tempfile::tempdir().expect("directory");
        let mut table =
            TableStore::open(directory.path(), schema(), StoreOptions::default()).expect("table");
        let stored = rows
            .into_iter()
            .zip(0_u64..)
            .map(|((name, code, grp, at, ratio), id)| {
                StoredRow::new(
                    PrimaryKey::new(vec![KeyPart::UInt64(id)]).expect("key"),
                    vec![
                        Value::UInt64(id),
                        text(name),
                        text(code),
                        grp.map_or(Value::Null, Value::Int64),
                        text(at),
                        ratio.map_or(Value::Null, |ratio| {
                            Value::Float64(pintail_types::Float64::new(ratio))
                        }),
                    ],
                    id + 1,
                    false,
                )
            })
            .collect::<Vec<_>>();
        let count = u64::try_from(stored.len()).expect("rows");
        table.bulk_ingest_snapshot(stored).expect("rows");
        let entry = TableEntry::new(
            TableId::new(1),
            "items",
            schema(),
            TableStatistics::with_row_count(count),
        )
        .expect("entry")
        .with_key_columns([1])
        .expect("key");
        Self {
            _directory: directory,
            table,
            catalog: CatalogSnapshot::new([
                DatabaseEntry::new(DatabaseId::new(1), "app", [entry]).expect("database")
            ])
            .expect("catalog"),
        }
    }

    fn run(&self, sql: &str) -> (Vec<String>, f64) {
        let snapshot = self.table.snapshot();
        let provider =
            SnapshotScanProvider::new([(DatabaseId::new(1), TableId::new(1), &snapshot)])
                .expect("provider");
        let bound = Binder::new(&self.catalog, Some("app"))
            .bind(&parse_statement(sql).expect("parse"))
            .unwrap_or_else(|error| panic!("bind {sql}: {error}"));
        let physical = PhysicalPlanner::plan(
            Optimizer::optimize(LogicalPlanner::plan(bound)),
            Collation::default(),
        )
        .expect("plan");
        let started = std::time::Instant::now();
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
                            value
                                .text()
                                .map_or_else(|| format!("{value:?}"), str::to_owned)
                        })
                        .collect::<Vec<_>>()
                        .join("|"),
                );
            }
        }
        (rows, started.elapsed().as_secs_f64() * 1000.0)
    }
}

fn fixture() -> Fixture {
    Fixture::new([
        (
            Some("Apple"),
            Some("x"),
            Some(2),
            Some("2026-01-02 10:00:00"),
            Some(0.5),
        ),
        (
            Some("apple"),
            Some("X"),
            Some(1),
            Some("2026-01-01 10:00:00"),
            Some(-0.0),
        ),
        (
            Some("Äpple"),
            Some("x"),
            Some(2),
            Some("2026-01-02 10:00:00"),
            Some(0.0),
        ),
        (None, None, None, None, None),
        (
            Some("banana"),
            Some("y"),
            Some(1),
            Some("2026-01-01 10:00:00"),
            Some(0.5),
        ),
        (Some("BANANA"), Some("y"), Some(3), None, None),
        (None, Some("x"), None, None, None),
        (
            Some("apple "),
            Some("x "),
            Some(2),
            Some("2026-01-02 10:00:00"),
            Some(0.5),
        ),
    ])
}

#[test]
fn equal_rows_answer_with_the_first_to_arrive_in_sorted_order() {
    let fixture = fixture();
    // ai_ci folds case and accent and is NO PAD: 'Apple', 'apple' and
    // 'Äpple' are one value answered by 'Apple'; 'apple ' is another.
    assert_eq!(
        fixture.run("SELECT DISTINCT name FROM items").0,
        ["Null", "Apple", "apple ", "banana"]
    );
    // utf8mb4_bin keeps case apart and, being PAD SPACE, not trailing space.
    assert_eq!(
        fixture.run("SELECT DISTINCT code FROM items").0,
        ["Null", "X", "x", "y"]
    );
    assert_eq!(
        fixture.run("SELECT DISTINCT grp, name FROM items").0,
        [
            "Null|Null",
            "Int64(1)|apple",
            "Int64(1)|banana",
            "Int64(2)|Apple",
            "Int64(2)|apple ",
            "Int64(3)|BANANA",
        ]
    );
    assert_eq!(
        fixture.run("SELECT DISTINCT at FROM items").0,
        ["Null", "2026-01-01 10:00:00", "2026-01-02 10:00:00"]
    );
    assert_eq!(
        fixture
            .run("SELECT DISTINCT name FROM items ORDER BY name DESC LIMIT 2")
            .0,
        ["banana", "apple "]
    );
}

#[test]
#[ignore = "measurement, not an assertion"]
fn distinct_cost_over_few_values() {
    const NAMES: [&str; 8] = [
        "page_view",
        "click",
        "api_error",
        "video_play",
        "login",
        "logout",
        "scroll",
        "search",
    ];
    let fixture = Fixture::new((0..1_500_000_u32).map(|id| {
        (
            Some(NAMES[usize::try_from(id).expect("id") % NAMES.len()]),
            Some(NAMES[usize::try_from(id / 7).expect("id") % NAMES.len()]),
            Some(i64::from(id % 700)),
            None,
            None,
        )
    }));
    for sql in [
        "SELECT DISTINCT name FROM items ORDER BY name",
        "SELECT DISTINCT grp, code FROM items ORDER BY grp LIMIT 500",
    ] {
        let mut timings = (0..3).map(|_| fixture.run(sql).1).collect::<Vec<_>>();
        timings.sort_by(f64::total_cmp);
        println!(
            "median {:>9.1} ms  min {:>9.1} ms  {sql}",
            timings[1], timings[0]
        );
    }
}
