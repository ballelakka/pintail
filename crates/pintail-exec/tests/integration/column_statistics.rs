//! Column statistics kept by storage (distinct-value sketches and value
//! ranges) steer two join decisions, and neither changes an answer:
//!
//! - an inner join run with no pinned key, written large-relation-first,
//!   is rebuilt around the relation a non-key filter leaves few rows of;
//! - a join on two keys whose probe side a filter leaves a few rows of
//!   reads that side first, and its keys filter the build side, instead of
//!   hashing the build side whole for them.
//!
//! Each fixture runs with and without statistics attached, so the
//! measurement compares the two plans over the same data:
//! `cargo test --profile recovery -p pintail-exec --test integration column_statistics::
//! -- --ignored --nocapture`.

use std::sync::Arc;

use pintail_catalog::{
    CatalogSnapshot, DatabaseEntry, DatabaseId, TableEntry, TableId, TableStatistics,
};
use pintail_exec::collation::Collation;
use pintail_exec::{Execution, LogicalPlanner, Optimizer, PhysicalPlanner, SnapshotScanProvider};
use pintail_sql::{Binder, parse_statement};
use pintail_store::{StoreOptions, TableStore};
use pintail_types::{Column, DataType, KeyPart, PrimaryKey, StoredRow, TableSchema, Value};

/// `id`, a `parent` among the first `parents` ids, a `val` in thirteen
/// values, a `grp` in two thousand and a `band` in twenty-six.
fn schema() -> TableSchema {
    TableSchema::new(
        1,
        vec![
            Column::new(1, "id", DataType::UInt64, false),
            Column::new(2, "parent", DataType::Int64, true),
            Column::new(3, "val", DataType::Int64, true),
            Column::new(4, "grp", DataType::Int64, false),
            Column::new(5, "band", DataType::Int64, false),
        ],
    )
    .expect("schema")
}

#[derive(Clone, Copy)]
struct Row {
    parent: Option<i64>,
    val: Option<i64>,
    grp: i64,
    band: i64,
}

fn rows_of(size: i64, parents: i64) -> Vec<Row> {
    (0..size)
        .map(|id| Row {
            parent: (id % 9 != 0).then_some((id * 7919) % parents),
            val: (id % 11 != 4).then_some(id % 13),
            grp: (id * 31) % 2_000,
            band: (id / 3) % 26,
        })
        .collect()
}

struct Fixture {
    _directory: tempfile::TempDir,
    table: TableStore,
    with: CatalogSnapshot,
    without: CatalogSnapshot,
}

impl Fixture {
    fn new(data: &[Row]) -> Self {
        let directory = tempfile::tempdir().expect("directory");
        let mut table =
            TableStore::open(directory.path(), schema(), StoreOptions::default()).expect("table");
        let stored = data
            .iter()
            .zip(0_u64..)
            .map(|(row, id)| {
                StoredRow::new(
                    PrimaryKey::new(vec![KeyPart::UInt64(id)]).expect("key"),
                    vec![
                        Value::UInt64(id),
                        row.parent.map_or(Value::Null, Value::Int64),
                        row.val.map_or(Value::Null, Value::Int64),
                        Value::Int64(row.grp),
                        Value::Int64(row.band),
                    ],
                    id + 1,
                    false,
                )
            })
            .collect::<Vec<_>>();
        let count = u64::try_from(stored.len()).expect("rows");
        table.bulk_ingest_snapshot(stored).expect("rows");
        let statistics = Arc::new(table.snapshot().column_statistics());
        let catalog = |statistics: Option<Arc<pintail_catalog::ColumnStatistics>>| {
            let mut entry = TableEntry::new(
                TableId::new(1),
                "nodes",
                schema(),
                TableStatistics::with_estimated_row_count(count),
            )
            .expect("entry")
            .with_key_columns([1])
            .expect("key");
            if let Some(statistics) = statistics {
                entry = entry.with_column_statistics(statistics);
            }
            CatalogSnapshot::new([
                DatabaseEntry::new(DatabaseId::new(1), "app", [entry]).expect("database")
            ])
            .expect("catalog")
        };
        Self {
            _directory: directory,
            table,
            with: catalog(Some(statistics)),
            without: catalog(None),
        }
    }

    fn plan(&self, sql: &str, statistics: bool) -> pintail_exec::LogicalPlan {
        let catalog = if statistics {
            &self.with
        } else {
            &self.without
        };
        let bound = Binder::new(catalog, Some("app"))
            .bind(&parse_statement(sql).expect("parse"))
            .unwrap_or_else(|error| panic!("bind {sql}: {error}"));
        Optimizer::optimize(LogicalPlanner::plan(bound))
    }

    fn run(&self, sql: &str, statistics: bool) -> (Vec<String>, f64) {
        let snapshot = self.table.snapshot();
        let provider =
            SnapshotScanProvider::new([(DatabaseId::new(1), TableId::new(1), &snapshot)])
                .expect("provider");
        let physical =
            PhysicalPlanner::plan(self.plan(sql, statistics), Collation::default()).expect("plan");
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
                            match value {
                                Value::UInt64(value) => value.to_string(),
                                Value::Int64(value) => value.to_string(),
                                other => format!("{other:?}"),
                            }
                        })
                        .collect::<Vec<_>>()
                        .join("|"),
                );
            }
        }
        rows.sort();
        (rows, started.elapsed().as_secs_f64() * 1000.0)
    }
}

/// Rows `a` whose parent `b` has a parent `c` in group `grp`.
fn chain(grp: i64) -> String {
    format!(
        "SELECT a.id, b.id, c.id FROM nodes a \
         JOIN nodes b ON b.id = a.parent \
         JOIN nodes c ON c.id = b.parent \
         WHERE c.grp = {grp}"
    )
}

fn expected_chain(data: &[Row], grp: i64) -> Vec<String> {
    let at = |id: i64| usize::try_from(id).ok().and_then(|index| data.get(index));
    let mut expected = Vec::new();
    for (a, row) in data.iter().enumerate() {
        let Some(b) = row.parent else { continue };
        let Some(Row {
            parent: Some(c), ..
        }) = at(b)
        else {
            continue;
        };
        if at(*c).is_some_and(|row| row.grp == grp) {
            expected.push(format!("{a}|{b}|{c}"));
        }
    }
    expected.sort();
    expected
}

/// Pairs sharing both a parent and a value, the probe side cut to one
/// group and the build side to one band.
fn two_keys(grp: i64, band: i64) -> String {
    format!(
        "SELECT a.id, b.id FROM nodes a \
         JOIN nodes b ON b.parent = a.parent AND b.val = a.val \
         WHERE a.grp = {grp} AND b.band = {band}"
    )
}

fn expected_two_keys(data: &[Row], grp: i64, band: i64) -> Vec<String> {
    let mut expected = Vec::new();
    for (a, left) in data.iter().enumerate() {
        if left.grp != grp {
            continue;
        }
        let (Some(parent), Some(val)) = (left.parent, left.val) else {
            continue;
        };
        for (b, right) in data.iter().enumerate() {
            if right.band == band && right.parent == Some(parent) && right.val == Some(val) {
                expected.push(format!("{a}|{b}"));
            }
        }
    }
    expected.sort();
    expected
}

#[test]
fn statistics_describe_the_stored_columns() {
    let data = rows_of(20_000, 2_000);
    let fixture = Fixture::new(&data);
    let statistics = fixture.table.snapshot().column_statistics();
    assert_eq!(statistics.rows, 20_000);
    let distinct = |column: u32| {
        statistics
            .column(column)
            .and_then(|facts| facts.distinct)
            .expect("sketch")
    };
    assert!((12..=14).contains(&distinct(3)), "{}", distinct(3));
    assert!((1_400..=2_600).contains(&distinct(4)), "{}", distinct(4));
    assert!((20..=32).contains(&distinct(5)), "{}", distinct(5));
    let parent = statistics.column(2).expect("parent");
    assert_eq!(parent.non_null, 20_000 - 20_000_u64.div_ceil(9));
    let range = parent.range.expect("range");
    assert_eq!((range.low, range.high), (0, 1_999));
}

#[test]
fn statistics_keep_the_answers() {
    let data = rows_of(3_000, 300);
    let fixture = Fixture::new(&data);
    for grp in [7, 31, 1_999, 5_000] {
        let expected = expected_chain(&data, grp);
        for statistics in [false, true] {
            assert_eq!(fixture.run(&chain(grp), statistics).0, expected, "{grp}");
        }
    }
    for (grp, band) in [(7, 3), (62, 0), (0, 25), (3_000, 1)] {
        let expected = expected_two_keys(&data, grp, band);
        for statistics in [false, true] {
            assert_eq!(
                fixture.run(&two_keys(grp, band), statistics).0,
                expected,
                "{grp} {band}"
            );
        }
    }
}

#[test]
fn a_filtered_later_relation_is_joined_first() {
    use pintail_exec::LogicalPlan;
    fn top_join(plan: &LogicalPlan) -> Option<&LogicalPlan> {
        match plan {
            LogicalPlan::Join { .. } => Some(plan),
            LogicalPlan::Project { input, .. } | LogicalPlan::Filter { input, .. } => {
                top_join(input)
            }
            _ => None,
        }
    }
    let fixture = Fixture::new(&rows_of(40_000, 4_000));
    let named = |plan: &LogicalPlan, name: &str| {
        format!("{plan:?}").contains(&format!("relation_name: \"{name}\""))
    };
    // Without statistics the run stays in written order, `a` and `b` first.
    let Some(LogicalPlan::Join { right, .. }) = top_join(&fixture.plan(&chain(7), false)).cloned()
    else {
        panic!("no join");
    };
    assert!(named(&right, "c") && !named(&right, "a"), "{right:?}");
    // With them, `c` - one group of two thousand - is joined first.
    let plan = fixture.plan(&chain(7), true);
    let Some(LogicalPlan::Join { left, right, .. }) = top_join(&plan) else {
        panic!("{plan:?}");
    };
    assert!(
        named(left, "c") && named(right, "a") && !named(right, "c"),
        "{plan:?}"
    );
}

#[test]
#[ignore = "measurement, not an assertion"]
fn column_statistics_cost() {
    let data = rows_of(400_000, 40_000);
    let fixture = Fixture::new(&data);
    for (name, sql) in [
        ("filtered chain", chain(7)),
        ("two-key join", two_keys(7, 3)),
    ] {
        for statistics in [false, true] {
            let mut timings = (0..7)
                .map(|_| fixture.run(&sql, statistics))
                .collect::<Vec<_>>();
            timings.sort_by(|left, right| left.1.total_cmp(&right.1));
            println!(
                "{name:<16} statistics={statistics:<5} median {:>9.2} ms  min {:>9.2} ms  rows {}",
                timings[3].1,
                timings[0].1,
                timings[3].0.len()
            );
        }
    }
}
