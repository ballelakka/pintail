//! What a filtered scan pays once its table has taken scattered updates.
//!
//! `#[ignore]`: measurement, not assertion. Run with
//! `PINTAIL_DISABLE_SETTLED_MEMO=1 cargo test --release -p pintail-exec
//! --test integration mutated_scan_cost:: -- --ignored --nocapture`;
//! `MUTATED_SCAN_PROFILE=1` beside `PINTAIL_PROFILE=1` prints each run's
//! operator profile.
//!
//! The table is loaded as a snapshot, then one row in a hundred is updated,
//! one in a thousand deleted and a few thousand appended through the change
//! path, flushed as a replica flushes - the state a replica answers from
//! while its source is written to.

use pintail_catalog::{
    CatalogSnapshot, DatabaseEntry, DatabaseId, TableEntry, TableId, TableStatistics,
};
use pintail_exec::collation::Collation;
use pintail_exec::{Execution, LogicalPlanner, Optimizer, PhysicalPlanner, SnapshotScanProvider};
use pintail_sql::{Binder, parse_statement};
use pintail_store::{StoreOptions, TableStore};
use pintail_types::{Column, DataType, KeyPart, PrimaryKey, StoredRow, TableSchema, Value};

const ROWS: u64 = 2_000_000;
const RUNS: usize = 5;

fn schema() -> TableSchema {
    TableSchema::new(
        1,
        vec![
            Column::new(1, "id", DataType::UInt64, false),
            Column::new(2, "made", DataType::Int64, false),
            Column::new(3, "planned", DataType::Int64, false),
            Column::new(4, "owner", DataType::Int64, false),
            Column::new(5, "amount", DataType::Int64, false),
            Column::new(6, "stage", DataType::Int64, false),
        ],
    )
    .expect("schema")
}

fn row(id: u64, version: u64, revision: i64, deleted: bool) -> StoredRow {
    let signed = i64::try_from(id).expect("small");
    StoredRow::new(
        PrimaryKey::new(vec![KeyPart::UInt64(id)]).expect("key"),
        vec![
            Value::UInt64(id),
            Value::Int64(signed),
            Value::Int64(signed.wrapping_mul(7919) % 2_000_000),
            Value::Int64(signed % 50_000),
            Value::Int64((signed.wrapping_mul(31) + revision) % 100_000),
            Value::Int64((signed + revision) % 6),
        ],
        version,
        deleted,
    )
}

struct Fixture {
    _directory: tempfile::TempDir,
    table: TableStore,
    catalog: CatalogSnapshot,
    /// Change versions only rise, as a source's log positions do.
    version: u64,
    revision: i64,
}

impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().expect("directory");
        let mut table =
            TableStore::open(directory.path(), schema(), StoreOptions::default()).expect("table");
        table
            .bulk_ingest_snapshot((0..ROWS).map(|id| row(id, id + 1, 0, false)).collect())
            .expect("rows");
        let entry = TableEntry::new(
            TableId::new(1),
            "items",
            schema(),
            TableStatistics::with_row_count(ROWS),
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
            version: ROWS + 1,
            revision: 0,
        }
    }

    /// One row in a hundred updated, one in a thousand deleted, a few
    /// thousand appended, in change batches flushed as a replica flushes.
    fn mutate(&mut self, compact: bool) {
        let mut version = self.version;
        self.revision += 1;
        let appended = ROWS + u64::try_from(self.revision).expect("small") * 10_000;
        let mut changes = Vec::new();
        for id in (0..ROWS).filter(|id| id % 100 == 37) {
            version += 1;
            changes.push(row(id, version, self.revision, false));
        }
        for id in (0..ROWS).filter(|id| id % 1000 == 501) {
            version += 1;
            changes.push(row(id, version, 0, true));
        }
        for id in appended - 10_000..appended {
            version += 1;
            changes.push(row(id, version, 0, false));
        }
        self.version = version;
        for batch in changes.chunks(2_000) {
            let outcome = self.table.ingest_cdc(batch.to_vec()).expect("change batch");
            if outcome.should_flush() {
                self.table.flush().expect("flush");
            }
        }
        self.table.flush().expect("flush");
        if compact {
            for _ in 0..8 {
                if self
                    .table
                    .compact()
                    .expect("compact")
                    .output_path()
                    .is_none()
                {
                    break;
                }
            }
        }
    }

    fn run(&self, sql: &str) -> (Vec<Vec<Value>>, f64) {
        let snapshot = self.table.snapshot();
        let provider =
            SnapshotScanProvider::new([(DatabaseId::new(1), TableId::new(1), &snapshot)])
                .expect("provider");
        let bound = Binder::new(&self.catalog, Some("app"))
            .bind(&parse_statement(sql).expect("parse"))
            .expect("bind");
        let physical = PhysicalPlanner::plan(
            Optimizer::optimize(LogicalPlanner::plan(bound)),
            Collation::default(),
        )
        .expect("plan");
        let started = std::time::Instant::now();
        let mut execution =
            Execution::start(physical, &provider, 1 << 30, Collation::default()).expect("start");
        let mut rows = Vec::new();
        while let Some(batch) = execution.next_batch().expect("batch") {
            for row in batch.selection().selected_rows() {
                rows.push(
                    batch
                        .columns()
                        .iter()
                        .map(|column| column.value_owned(row).expect("value"))
                        .collect(),
                );
            }
        }
        if std::env::var_os("MUTATED_SCAN_PROFILE").is_some()
            && let Some(profile) = execution.profile()
        {
            println!("{}", profile.render());
        }
        (rows, started.elapsed().as_secs_f64() * 1000.0)
    }

    fn report(&self, state: &str) {
        println!(
            "-- {state}: {} segments",
            self.table.metrics().expect("metrics").segment_count()
        );
        for (label, sql) in CASES {
            let (answer, _) = self.run(sql);
            let mut times = (0..RUNS).map(|_| self.run(sql).1).collect::<Vec<_>>();
            times.sort_by(f64::total_cmp);
            println!(
                "{label:<28} median {:>9.1} ms  min {:>9.1} ms  ({} rows)",
                times[RUNS / 2],
                times[0],
                answer.len()
            );
        }
    }
}

const CASES: [(&str, &str); 7] = [
    ("count all", "SELECT COUNT(*) FROM items"),
    (
        "amount range count",
        "SELECT COUNT(*) FROM items WHERE amount BETWEEN 1000 AND 2000",
    ),
    (
        "made range count",
        "SELECT COUNT(*) FROM items WHERE made BETWEEN 500000 AND 520000",
    ),
    (
        "made range newest 50",
        "SELECT id FROM items WHERE made BETWEEN 500000 AND 520000 ORDER BY made DESC LIMIT 50",
    ),
    (
        "planned range count",
        "SELECT COUNT(*) FROM items WHERE planned BETWEEN 500000 AND 520000",
    ),
    ("owner point", "SELECT id FROM items WHERE owner = 4242"),
    ("stage count", "SELECT COUNT(*) FROM items WHERE stage = 3"),
];

#[test]
#[ignore = "measurement, not an assertion"]
fn mutated_scan_cost() {
    let mut fixture = Fixture::new();
    fixture.report("pristine");
    fixture.mutate(false);
    fixture.report("mutated, flushed");
    fixture.mutate(true);
    fixture.report("mutated twice, compacted");
    fixture.mutate(false);
    fixture.report("compacted, then mutated again");
}
