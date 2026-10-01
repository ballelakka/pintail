//! In-process timing of a star join folded into a grouped aggregate: a fact
//! table of `PINTAIL_BENCH_ROWS` rows (default 20M) joined on an unsigned
//! integer key to a 100K-row dimension whose primary key is dense, grouped
//! by a dimension text column with eight values, summing a DECIMAL(12,2)
//! fact column. Ignored: it is a measurement, not a gate. Run with
//! `PINTAIL_DISABLE_SETTLED_MEMO=1 cargo test --release -p pintail-exec
//!  --test integration join_aggregate_bench:: -- --ignored --nocapture`.
//! `PINTAIL_BENCH_CASE` narrows the cases by label substring.
use std::time::{Duration, Instant};

use pintail_catalog::{
    CatalogSnapshot, DatabaseEntry, DatabaseId, TableEntry, TableId, TableStatistics,
};
use pintail_exec::collation::Collation;
use pintail_exec::{Execution, LogicalPlanner, Optimizer, PhysicalPlanner, SnapshotScanProvider};
use pintail_sql::{Binder, parse_statement};
use pintail_store::{StoreOptions, TableStore};
use pintail_types::{Column, DataType, KeyPart, PrimaryKey, StoredRow, TableSchema, Value};

const CHUNK: u64 = 1_000_000;
const DIM_ROWS: u64 = 100_000;
const REGIONS: [&str; 8] = [
    "north", "south", "east", "west", "inner", "outer", "upper", "lower",
];

fn fact_schema() -> TableSchema {
    TableSchema::new(
        1,
        vec![
            Column::new(1, "id", DataType::UInt64, false),
            Column::new(2, "dim_id", DataType::UInt32, false),
            Column::new(
                3,
                "amount",
                DataType::Decimal {
                    precision: 12,
                    scale: 2,
                },
                false,
            ),
        ],
    )
    .expect("schema")
}

fn dim_schema() -> TableSchema {
    TableSchema::new(
        1,
        vec![
            Column::new(1, "id", DataType::UInt32, false),
            Column::new(2, "zone", DataType::Utf8, false),
        ],
    )
    .expect("schema")
}

fn amount(id: u64) -> String {
    let cents = 1_000 + (id * 7_919) % 99_000;
    format!("{}.{:02}", cents / 100, cents % 100)
}

struct Fixture {
    _directory: tempfile::TempDir,
    facts: TableStore,
    dims: TableStore,
    catalog: CatalogSnapshot,
}

impl Fixture {
    fn new(rows: u64) -> Self {
        let directory = tempfile::tempdir().expect("directory");
        let mut facts = TableStore::open(
            directory.path().join("facts"),
            fact_schema(),
            StoreOptions::default(),
        )
        .expect("facts");
        let mut next = 1;
        while next <= rows {
            let end = (next + CHUNK - 1).min(rows);
            facts
                .bulk_ingest_snapshot(
                    (next..=end)
                        .map(|id| {
                            StoredRow::new(
                                PrimaryKey::new(vec![KeyPart::UInt64(id)]).expect("key"),
                                vec![
                                    Value::UInt64(id),
                                    Value::UInt64(1 + (id * 17) % DIM_ROWS),
                                    Value::Utf8(amount(id)),
                                ],
                                1,
                                false,
                            )
                        })
                        .collect(),
                )
                .expect("ingest");
            next = end + 1;
        }
        let mut dims = TableStore::open(
            directory.path().join("dims"),
            dim_schema(),
            StoreOptions::default(),
        )
        .expect("dims");
        dims.bulk_ingest_snapshot(
            (1..=DIM_ROWS)
                .map(|id| {
                    StoredRow::new(
                        PrimaryKey::new(vec![KeyPart::UInt64(id)]).expect("key"),
                        vec![
                            Value::UInt64(id),
                            Value::Utf8(
                                REGIONS[usize::try_from(id % 8).expect("small")].to_owned(),
                            ),
                        ],
                        1,
                        false,
                    )
                })
                .collect(),
        )
        .expect("ingest dims");
        let facts_entry = TableEntry::new(
            TableId::new(1),
            "facts",
            fact_schema(),
            TableStatistics::with_row_count(rows),
        )
        .expect("entry")
        .with_key_columns([1])
        .expect("key");
        let dims_entry = TableEntry::new(
            TableId::new(2),
            "dims",
            dim_schema(),
            TableStatistics::with_row_count(DIM_ROWS),
        )
        .expect("entry")
        .with_key_columns([1])
        .expect("key");
        let database = DatabaseEntry::new(DatabaseId::new(1), "app", [facts_entry, dims_entry])
            .expect("database");
        Self {
            _directory: directory,
            facts,
            dims,
            catalog: CatalogSnapshot::new([database]).expect("catalog"),
        }
    }

    fn run(&self, sql: &str) -> Result<(Vec<String>, Duration), String> {
        let facts = self.facts.snapshot();
        let dims = self.dims.snapshot();
        let provider = SnapshotScanProvider::new([
            (DatabaseId::new(1), TableId::new(1), &facts),
            (DatabaseId::new(1), TableId::new(2), &dims),
        ])
        .expect("provider");
        let bound = Binder::new(&self.catalog, Some("app"))
            .bind(&parse_statement(sql).expect("parse"))
            .expect("bind");
        let physical = PhysicalPlanner::plan(
            Optimizer::optimize(LogicalPlanner::plan(bound)),
            Collation::default(),
        )
        .expect("plan");
        let limit = 4 << 30;
        let clock = Instant::now();
        let mut execution = if std::env::var_os("PINTAIL_BENCH_PROFILE").is_some() {
            Execution::start_profiled(physical, &provider, limit, None, Collation::default())
        } else {
            Execution::start(physical, &provider, limit, Collation::default())
        }
        .map_err(|error| error.to_string())?;
        let mut rows = Vec::new();
        loop {
            match execution.next_batch() {
                Ok(Some(batch)) => {
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
                Ok(None) => break,
                Err(error) => return Err(error.to_string()),
            }
        }
        let elapsed = clock.elapsed();
        if let Some(profile) = execution.profile() {
            eprintln!("{}", profile.render());
        }
        Ok((rows, elapsed))
    }
}

const CASES: &[(&str, &str)] = &[
    (
        "inner join + group by dim text",
        "SELECT d.zone, COUNT(*) AS cnt, ROUND(SUM(f.amount), 2) AS total FROM facts f \
         JOIN dims d ON f.dim_id = d.id GROUP BY d.zone ORDER BY total DESC",
    ),
    (
        "left join + group by dim text",
        "SELECT d.zone, COUNT(*) AS cnt, ROUND(SUM(f.amount), 2) AS total FROM facts f \
         LEFT JOIN dims d ON f.dim_id = d.id GROUP BY d.zone ORDER BY total DESC",
    ),
    (
        "inner join + count only",
        "SELECT d.zone, COUNT(*) AS cnt FROM facts f JOIN dims d ON f.dim_id = d.id \
         GROUP BY d.zone ORDER BY cnt DESC",
    ),
];

#[test]
#[ignore = "a measurement over a large in-process table, not a gate"]
fn star_join_aggregate_over_a_large_table() {
    let rows = std::env::var("PINTAIL_BENCH_ROWS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(20_000_000_u64);
    let runs = std::env::var("PINTAIL_BENCH_RUNS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(9_usize);
    let only = std::env::var("PINTAIL_BENCH_CASE").ok();
    let clock = Instant::now();
    let fixture = Fixture::new(rows);
    eprintln!(
        "ingested {rows} rows in {:.1}s; {} rayon threads",
        clock.elapsed().as_secs_f64(),
        rayon::current_num_threads(),
    );
    let _ = fixture.run("SELECT COUNT(*) FROM facts");
    for (label, sql) in CASES {
        if only
            .as_ref()
            .is_some_and(|only| !label.contains(only.as_str()))
        {
            continue;
        }
        let mut times = Vec::with_capacity(runs);
        let mut answer = Vec::new();
        for _ in 0..runs {
            match fixture.run(sql) {
                Ok((rows, elapsed)) => {
                    answer = rows;
                    times.push(elapsed);
                }
                Err(error) => panic!("{label}: {error}"),
            }
        }
        times.sort();
        let min = times[0].as_secs_f64() * 1e3;
        let median = times[times.len() / 2].as_secs_f64() * 1e3;
        eprintln!("{label:<34} min={min:>8.1}ms  median={median:>8.1}ms");
        if std::env::var_os("PINTAIL_BENCH_ANSWERS").is_some() {
            for row in answer {
                eprintln!("    {row}");
            }
        }
    }
}
