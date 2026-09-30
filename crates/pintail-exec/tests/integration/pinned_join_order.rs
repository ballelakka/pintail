//! A run of inner joins written with the large relation first and a later
//! relation pinned by its whole primary key joins the pinned relation, and
//! what it links to, first: in written order every relation is joined to
//! the large one before the filter keeping a row or two is reached. The
//! answers stay those computed here from the rows.
//!
//! The measurement is `#[ignore]`d:
//! `cargo test --profile recovery -p pintail-exec --test integration pinned_join_order::
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

type Rows = Vec<(Option<i64>, Option<i64>)>;

/// Rows under `parents` parents, one in nine without one, and a `val`
/// missing on one in eleven.
fn rows_of(size: i64, parents: i64) -> Rows {
    (0..size)
        .map(|id| {
            let parent = (id % 9 != 0).then_some(id % parents);
            let val = (id % 11 != 4).then_some(id % 13);
            (parent, val)
        })
        .collect()
}

/// Rows `a` whose parent `b` has the parent `c`, `c` pinned to `pins`.
fn chain(pins: &str) -> String {
    format!(
        "SELECT a.id, b.id, c.val FROM nodes a \
         JOIN nodes b ON b.id = a.parent \
         JOIN nodes c ON c.id = b.parent AND c.val IS NOT NULL \
         WHERE c.id IN ({pins})"
    )
}

fn expected_chain(data: &Rows, pins: &[i64]) -> Vec<String> {
    let at = |id: i64| usize::try_from(id).ok().and_then(|index| data.get(index));
    let mut expected = Vec::new();
    for (a, (a_parent, _)) in data.iter().enumerate() {
        let Some(b) = *a_parent else { continue };
        let Some((Some(c), _)) = at(b) else { continue };
        let Some((_, Some(c_val))) = at(*c) else {
            continue;
        };
        if pins.contains(c) {
            expected.push(format!("{a}|{b}|Int64({c_val})"));
        }
    }
    expected.sort();
    expected
}

fn render(rows: Vec<String>) -> Vec<String> {
    let mut rows = rows
        .into_iter()
        .map(|row| {
            row.split('|')
                .enumerate()
                .map(|(index, cell)| {
                    if index < 2 {
                        cell.trim_start_matches("UInt64(")
                            .trim_end_matches(')')
                            .to_owned()
                    } else {
                        cell.to_owned()
                    }
                })
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

#[test]
fn a_pinned_later_relation_keeps_the_answer() {
    let data = rows_of(300, 40);
    let fixture = Fixture::new(data.clone());
    for pins in [vec![3], vec![0, 7, 11], vec![5, 500], vec![4]] {
        let list = pins
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        assert_eq!(
            render(fixture.run(&chain(&list)).0),
            expected_chain(&data, &pins),
            "{list}"
        );
    }
}

#[test]
fn a_pinned_later_relation_is_joined_first() {
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
    let fixture = Fixture::new(rows_of(300, 40));
    let plan = fixture.plan(&chain("3"));
    // Written order is `(a JOIN b) JOIN c`. Reordered, `b` joins the pinned
    // `c` first, on the probe side, and `a` is joined last.
    let Some(LogicalPlan::Join { left, right, .. }) = top_join(&plan) else {
        panic!("{plan:?}");
    };
    let named = |plan: &LogicalPlan, name: &str| {
        format!("{plan:?}").contains(&format!("relation_name: \"{name}\""))
    };
    assert!(
        named(left, "c") && named(right, "a") && !named(right, "c"),
        "{plan:?}"
    );
}

#[test]
#[ignore = "measurement, not an assertion"]
fn pinned_relation_cost() {
    let fixture = Fixture::new(rows_of(400_000, 40_000));
    let sql = chain("3, 17");
    let mut timings = (0..5).map(|_| fixture.run(&sql)).collect::<Vec<_>>();
    timings.sort_by(|left, right| left.1.total_cmp(&right.1));
    println!(
        "median {:>9.1} ms  rows {}",
        timings[2].1,
        timings[2].0.len()
    );
}
