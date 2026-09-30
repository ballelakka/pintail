//! A build side that is itself a join - a large table joined to a small
//! grouped or distinct one, which is how a correlated `IN (SELECT ...)` in
//! an ON clause is planned - takes the probe side's keys down to the input
//! each key column comes from. The large table is then read for the keys
//! that can match rather than joined whole and filtered after, and the
//! answers stay those computed here from the rows.
//!
//! The measurement is `#[ignore]`d:
//! `cargo test --profile recovery -p pintail-exec --test integration build_keys_through_joins::
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
            Column::new(2, "parent", DataType::Int64, true),
            Column::new(3, "val", DataType::Int64, true),
        ],
    )
    .expect("schema")
}

struct Fixture {
    _directory: tempfile::TempDir,
    table: TableStore,
    catalog: CatalogSnapshot,
}

impl Fixture {
    fn new(rows: impl IntoIterator<Item = (Option<i64>, Option<i64>)>) -> Self {
        let directory = tempfile::tempdir().expect("directory");
        let mut table =
            TableStore::open(directory.path(), schema(), StoreOptions::default()).expect("table");
        let stored = rows
            .into_iter()
            .zip(0_u64..)
            .map(|((parent, val), id)| {
                StoredRow::new(
                    PrimaryKey::new(vec![KeyPart::UInt64(id)]).expect("key"),
                    vec![
                        Value::UInt64(id),
                        parent.map_or(Value::Null, Value::Int64),
                        val.map_or(Value::Null, Value::Int64),
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
            "nodes",
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

    fn plan(&self, sql: &str) -> pintail_exec::LogicalPlan {
        let bound = Binder::new(&self.catalog, Some("app"))
            .bind(&parse_statement(sql).expect("parse"))
            .unwrap_or_else(|error| panic!("bind {sql}: {error}"));
        Optimizer::optimize(LogicalPlanner::plan(bound))
    }

    fn run(&self, sql: &str) -> (Vec<String>, f64) {
        let snapshot = self.table.snapshot();
        let provider =
            SnapshotScanProvider::new([(DatabaseId::new(1), TableId::new(1), &snapshot)])
                .expect("provider");
        let physical = PhysicalPlanner::plan(self.plan(sql), Collation::default()).expect("plan");
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

fn render(rows: Vec<String>) -> Vec<String> {
    let mut rows = rows
        .into_iter()
        .map(|row| {
            row.split('|')
                .map(|cell| {
                    let cell = cell.trim_start_matches("UInt64(").trim_end_matches(')');
                    if cell == "Null" { "NULL" } else { cell }.to_owned()
                })
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

type Rows = Vec<(Option<i64>, Option<i64>)>;

fn rows_of(size: i64, parents: i64, values: i64) -> Rows {
    (0..size)
        .map(|id| {
            let parent = match id % 9 {
                0 => None,
                _ => Some(id % parents),
            };
            let val = (id % 11 != 4).then_some(id % values);
            (parent, val)
        })
        .collect()
}

/// `a` filtered to `a.val = value`, left-joined to its children `c` whose
/// `val` is among the values of the rows whose parent is `a.val`.
fn correlated_in(value: i64) -> String {
    format!(
        "SELECT a.id, c.id FROM nodes a \
         LEFT JOIN nodes c ON c.parent = a.id \
           AND c.val IN (SELECT g.val FROM nodes g WHERE g.parent = a.val) \
         WHERE a.val = {value}"
    )
}

fn expected_correlated_in(data: &Rows, value: i64) -> Vec<String> {
    let mut expected = Vec::new();
    for (a, (_, a_val)) in data.iter().enumerate() {
        if *a_val != Some(value) {
            continue;
        }
        let a = i64::try_from(a).expect("id");
        let members = data
            .iter()
            .filter(|(parent, _)| *parent == Some(value))
            .filter_map(|(_, val)| *val)
            .collect::<Vec<_>>();
        let children = data
            .iter()
            .enumerate()
            .filter(|(_, (parent, val))| {
                *parent == Some(a) && val.is_some_and(|val| members.contains(&val))
            })
            .map(|(c, _)| format!("{a}|{c}"))
            .collect::<Vec<_>>();
        if children.is_empty() {
            expected.push(format!("{a}|NULL"));
        } else {
            expected.extend(children);
        }
    }
    expected.sort();
    expected
}

/// `a` filtered to `a.val = value`, joined to the pairs of a child `c` and
/// the row `g` its `val` names.
fn inner_pairs(kind: &str, value: i64) -> String {
    format!(
        "SELECT a.id, d.cid, d.gid FROM nodes a \
         {kind} JOIN (SELECT c.id AS cid, c.parent AS cp, g.id AS gid FROM nodes c \
           JOIN nodes g ON g.id = c.val) d ON d.cp = a.id \
         WHERE a.val = {value}"
    )
}

fn expected_inner_pairs(data: &Rows, value: i64, left: bool) -> Vec<String> {
    let len = i64::try_from(data.len()).expect("len");
    let mut expected = Vec::new();
    for (a, (_, a_val)) in data.iter().enumerate() {
        if *a_val != Some(value) {
            continue;
        }
        let a = i64::try_from(a).expect("id");
        let pairs = data
            .iter()
            .enumerate()
            .filter(|(_, (parent, val))| {
                *parent == Some(a) && val.is_some_and(|val| (0..len).contains(&val))
            })
            .map(|(c, (_, val))| format!("{a}|{c}|{}", val.expect("val")))
            .collect::<Vec<_>>();
        if pairs.is_empty() && left {
            expected.push(format!("{a}|NULL|NULL"));
        } else {
            expected.extend(pairs);
        }
    }
    expected.sort();
    expected
}

#[test]
fn correlated_membership_in_a_left_join_keeps_its_answer() {
    let data = rows_of(90, 30, 12);
    let fixture = Fixture::new(data.clone());
    for value in [0, 3, 7, 11, 40] {
        assert_eq!(
            render(fixture.run(&correlated_in(value)).0),
            expected_correlated_in(&data, value),
            "{value}"
        );
    }
}

#[test]
fn a_joined_build_side_keeps_its_answer() {
    let data = rows_of(90, 30, 100);
    let fixture = Fixture::new(data.clone());
    for value in [0, 3, 7, 50, 99, 400] {
        assert_eq!(
            render(fixture.run(&inner_pairs("LEFT", value)).0),
            expected_inner_pairs(&data, value, true),
            "left {value}"
        );
        assert_eq!(
            render(fixture.run(&inner_pairs("", value)).0),
            expected_inner_pairs(&data, value, false),
            "inner {value}"
        );
    }
}

#[test]
#[ignore = "measurement, not an assertion"]
fn joined_build_side_cost() {
    // 400,000 rows under 40,000 parents with 2,000 values: each filter keeps
    // about two hundred probe rows against a build side joining the whole
    // table to a distinct projection of it.
    let fixture = Fixture::new(rows_of(400_000, 40_000, 2_000));
    for sql in [correlated_in(7), inner_pairs("LEFT", 7)] {
        let mut timings = (0..5).map(|_| fixture.run(&sql)).collect::<Vec<_>>();
        timings.sort_by(|left, right| left.1.total_cmp(&right.1));
        println!(
            "median {:>9.1} ms  rows {}",
            timings[2].1,
            timings[2].0.len()
        );
    }
}
