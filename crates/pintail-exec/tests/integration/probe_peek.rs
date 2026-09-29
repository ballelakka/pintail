//! A hash join whose probe side is too large by estimate to read ahead of the
//! build still peeks at it. Estimates of a join chain multiply, so a chain
//! filtered to a few rows, or none, can claim millions; when the peek finds
//! the probe complete and small, its keys - every part of a composite key -
//! filter the build side, and an empty probe skips the build. The answers
//! stay those of the unfiltered build.
//!
//! The measurement is `#[ignore]`d:
//! `cargo test --profile recovery -p pintail-exec --test integration probe_peek::
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

/// Every row's parent is a row with a `val`, a NULL `val`, a missing row, or
/// NULL, so each join below meets matched, NULL-valued and unmatched rows.
fn small_rows() -> Vec<(Option<i64>, Option<i64>)> {
    (0..60_i64)
        .map(|id| {
            let parent = match id % 5 {
                0 => None,
                1 => Some(1_000 + id),
                _ => Some(id % 12),
            };
            let val = (id % 7 != 3).then_some(id % 4);
            (parent, val)
        })
        .collect()
}

fn small() -> Fixture {
    Fixture::new(small_rows())
}

#[test]
#[ignore = "measurement, not an assertion"]
fn small_probe_cost() {
    // 2,000 parents with fifty children each. Unfiltered, each chain fans
    // out to five million rows; every filter below keeps about a hundred
    // probe rows, or none, which no scan estimate knows.
    let fixture = Fixture::new((0..100_000_i64).map(|id| (Some(id % 2_000), Some(id % 1_000))));
    let filtered = "SELECT COUNT(*), SUM(c.val) FROM nodes a \
                    LEFT JOIN nodes b ON b.id = a.parent \
                    LEFT JOIN nodes c ON c.parent = b.id \
                    WHERE b.val = 7";
    // An inner join whose inputs filter to nothing, ahead of a LEFT JOIN
    // whose estimate-sized probe side never produces a row.
    let emptied = "SELECT COUNT(*), SUM(c.val) FROM nodes a \
                   JOIN nodes b ON b.id = a.parent \
                   LEFT JOIN nodes c ON c.parent = b.id \
                   WHERE a.val = -1";
    // The same with a hundred probe rows left against the whole build side.
    let narrowed = "SELECT COUNT(*), SUM(c.val) FROM nodes a \
                    JOIN nodes b ON b.id = a.parent \
                    LEFT JOIN nodes c ON c.parent = b.id \
                    WHERE a.val = 7";
    // A two-part key whose first part is shared by fifty build rows.
    let paired = "SELECT COUNT(*), SUM(c.val) FROM nodes a \
                  JOIN nodes b ON b.id = a.parent \
                  LEFT JOIN nodes c ON c.parent = b.id AND c.val = a.val \
                  WHERE a.val = 7";
    for sql in [filtered, emptied, narrowed, paired] {
        let mut timings = (0..3).map(|_| fixture.run(sql)).collect::<Vec<_>>();
        timings.sort_by(|left, right| left.1.total_cmp(&right.1));
        println!("median {:>9.1} ms  {:?}", timings[1].1, timings[1].0);
    }
}

#[test]
fn left_join_over_an_empty_probe_side_is_empty() {
    let fixture = small();
    let (rows, _) = fixture.run(
        "SELECT a.id, c.id FROM nodes a JOIN nodes b ON b.id = a.parent \
         LEFT JOIN nodes c ON c.parent = b.id WHERE a.val = -1",
    );
    assert!(rows.is_empty(), "{rows:?}");
    let (rows, _) = fixture.run(
        "SELECT COUNT(*) FROM nodes a JOIN nodes b ON b.id = a.parent \
         LEFT JOIN nodes c ON c.parent = b.id WHERE a.val = -1",
    );
    assert!(rows.len() == 1 && rows[0].ends_with("(0)"), "{rows:?}");
}

#[test]
fn left_join_over_a_small_probe_side_keeps_its_answer() {
    let fixture = small();
    for value in 0..4 {
        let mut rows = fixture
            .run(&format!(
                "SELECT a.id, b.id, c.id FROM nodes a JOIN nodes b ON b.id = a.parent \
                 LEFT JOIN nodes c ON c.parent = b.id WHERE a.val = {value}"
            ))
            .0;
        let mut expected = fixture
            .run(&format!(
                "SELECT d.aid, d.bid, c.id FROM (SELECT a.id AS aid, b.id AS bid FROM nodes a \
                 JOIN nodes b ON b.id = a.parent) d LEFT JOIN nodes c ON c.parent = d.bid \
                 WHERE d.aid IN (SELECT id FROM nodes WHERE val = {value})"
            ))
            .0;
        rows.sort();
        expected.sort();
        assert_eq!(rows, expected, "{value}");
        // A two-part key, each part of which filters the build side.
        let mut rows = fixture
            .run(&format!(
                "SELECT a.id, b.id, c.id FROM nodes a JOIN nodes b ON b.id = a.parent \
                 LEFT JOIN nodes c ON c.parent = b.id AND c.val = a.val WHERE a.val = {value}"
            ))
            .0;
        let mut expected = fixture
            .run(&format!(
                "SELECT d.aid, d.bid, c.id FROM (SELECT a.id AS aid, b.id AS bid, a.val AS aval \
                 FROM nodes a JOIN nodes b ON b.id = a.parent) d \
                 LEFT JOIN nodes c ON c.parent = d.bid AND c.val = d.aval \
                 WHERE d.aid IN (SELECT id FROM nodes WHERE val = {value})"
            ))
            .0;
        rows.sort();
        expected.sort();
        assert_eq!(rows, expected, "two-part {value}");
    }
}

/// Inner and semi joins whose probe side is a join chain, which a peek at
/// the probe can find small, against answers computed here from the rows.
#[test]
fn inner_and_semi_joins_over_a_small_probe_chain_keep_their_answers() {
    let fixture = small();
    let data = small_rows();
    let id_of = |index: usize| i64::try_from(index).expect("id");
    let exists = |id: i64| data.iter().any(|(parent, _)| *parent == Some(id));
    for value in 0..4 {
        // a LEFT JOIN b, then b's children inner-joined.
        let mut expected = Vec::new();
        let mut semi = Vec::new();
        for (index, (parent, val)) in data.iter().enumerate() {
            if *val != Some(value) {
                continue;
            }
            let Some(parent) =
                parent.filter(|parent| usize::try_from(*parent).is_ok_and(|p| p < data.len()))
            else {
                continue;
            };
            for (child, (child_parent, _)) in data.iter().enumerate() {
                if *child_parent == Some(parent) {
                    expected.push(format!("{}|{}", id_of(index), id_of(child)));
                }
            }
            if exists(parent) {
                semi.push(id_of(index).to_string());
            }
        }
        let render = |rows: Vec<String>| {
            let mut rows = rows
                .into_iter()
                .map(|row| {
                    row.split('|')
                        .map(|cell| {
                            cell.trim_start_matches("UInt64(")
                                .trim_end_matches(')')
                                .to_owned()
                        })
                        .collect::<Vec<_>>()
                        .join("|")
                })
                .collect::<Vec<_>>();
            rows.sort();
            rows
        };
        expected.sort();
        semi.sort();
        let inner = fixture
            .run(&format!(
                "SELECT a.id, c.id FROM nodes a LEFT JOIN nodes b ON b.id = a.parent \
                 JOIN nodes c ON c.parent = b.id WHERE a.val = {value}"
            ))
            .0;
        assert_eq!(render(inner), expected, "inner {value}");
        let exists_rows = fixture
            .run(&format!(
                "SELECT a.id FROM nodes a LEFT JOIN nodes b ON b.id = a.parent \
                 WHERE a.val = {value} AND EXISTS (SELECT 1 FROM nodes c WHERE c.parent = b.id)"
            ))
            .0;
        assert_eq!(render(exists_rows), semi, "semi {value}");
    }
}
