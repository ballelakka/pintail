//! In-process timing of scan-bound aggregates over a large order-shaped
//! table, with the settled memo off so every run decodes its columns.
//! Ignored: it is a measurement, not a gate. Run with
//! `PINTAIL_DISABLE_SETTLED_MEMO=1 cargo test --release -p pintail-exec
//!  --test integration scan_decode_bench:: -- --ignored --nocapture`.
//! `PINTAIL_BENCH_ROWS` overrides the table size (default 20M) and
//! `PINTAIL_BENCH_RUNS` the timed runs per query (default 9).
use std::time::{Duration, Instant};

use pintail_catalog::{
    CatalogSnapshot, DatabaseEntry, DatabaseId, TableEntry, TableId, TableStatistics,
};
use pintail_exec::collation::Collation;
use pintail_exec::{Execution, LogicalPlanner, Optimizer, PhysicalPlanner, SnapshotScanProvider};
use pintail_sql::{Binder, parse_statement};
use pintail_store::{StoreOptions, TableStore};
use pintail_types::{Column, DataType, KeyPart, PrimaryKey, StoredRow, TableSchema, Value};

const PHASES: [&str; 5] = ["queued", "picking", "sent", "arrived", "voided"];
const ZONES: [&str; 8] = [
    "north", "south", "east", "west", "inner", "outer", "upper", "lower",
];
const CHUNK: u64 = 1_000_000;

fn schema() -> TableSchema {
    TableSchema::new(
        1,
        vec![
            Column::new(1, "id", DataType::UInt64, false),
            Column::new(2, "buyer", DataType::UInt32, false),
            Column::new(
                3,
                "amount",
                DataType::Decimal {
                    precision: 12,
                    scale: 2,
                },
                false,
            ),
            Column::new(4, "phase", DataType::Utf8, false),
            Column::new(5, "zone", DataType::Utf8, false),
            Column::new(6, "placed", DataType::Date32, false),
        ],
    )
    .expect("schema")
}

fn civil(days: i64) -> String {
    // Days since 2020-01-01 to a calendar date.
    let z = days + 18_262 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!("{year:04}-{month:02}-{day:02}")
}

fn row(id: u64) -> StoredRow {
    let units = (1 + id % 20) * (1000 + (id * 7919) % 99_000);
    StoredRow::new(
        PrimaryKey::new(vec![KeyPart::UInt64(id)]).expect("key"),
        vec![
            Value::UInt64(id),
            Value::UInt64(1 + (id * 17) % 100_000),
            Value::Utf8(format!("{}.{:02}", units / 100, units % 100)),
            Value::Utf8(PHASES[usize::try_from(id % 5).expect("small")].to_owned()),
            Value::Utf8(ZONES[usize::try_from(id % 8).expect("small")].to_owned()),
            Value::Utf8(civil(i64::try_from((id * 7) % 1825).expect("small"))),
        ],
        1,
        false,
    )
}

struct Fixture {
    _directory: tempfile::TempDir,
    table: TableStore,
    catalog: CatalogSnapshot,
}

impl Fixture {
    fn new(rows: u64) -> Self {
        let directory = tempfile::tempdir().expect("directory");
        let mut table = TableStore::open(
            directory.path().join("t"),
            schema(),
            StoreOptions::default(),
        )
        .expect("table");
        let mut next = 1;
        while next <= rows {
            let end = (next + CHUNK - 1).min(rows);
            table
                .bulk_ingest_snapshot((next..=end).map(row).collect())
                .expect("ingest");
            next = end + 1;
        }
        let entry = TableEntry::new(
            TableId::new(1),
            "orders_like",
            schema(),
            TableStatistics::with_row_count(rows),
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

    fn run(&self, sql: &str) -> (Vec<String>, Duration) {
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
        let clock = Instant::now();
        let mut execution = if std::env::var_os("PINTAIL_BENCH_PROFILE").is_some() {
            Execution::start_profiled(physical, &provider, 4 << 30, None, Collation::default())
        } else {
            Execution::start(physical, &provider, 4 << 30, Collation::default())
        }
        .expect("start");
        let mut rows = Vec::new();
        while let Some(batch) = execution.next_batch().expect("batch") {
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
        let elapsed = clock.elapsed();
        if let Some(profile) = execution.profile() {
            eprintln!("{}", profile.render());
        }
        (rows, elapsed)
    }
}

const CASES: &[(&str, &str)] = &[
    (
        "Q2",
        "SELECT COUNT(*) AS cnt FROM orders_like WHERE phase = 'sent'",
    ),
    (
        "Q3",
        "SELECT phase, COUNT(*) AS cnt, ROUND(AVG(amount), 2) AS avg_amt FROM orders_like \
         GROUP BY phase ORDER BY cnt DESC",
    ),
    (
        "Q4",
        "SELECT zone, phase, COUNT(*) AS cnt, ROUND(SUM(amount), 2) AS total FROM orders_like \
         GROUP BY zone, phase ORDER BY total DESC, zone, phase LIMIT 20",
    ),
    (
        "Q5",
        "SELECT YEAR(placed) AS yr, MONTH(placed) AS mo, COUNT(*) AS cnt, \
         ROUND(SUM(amount), 2) AS revenue FROM orders_like \
         WHERE placed >= '2023-01-01' AND placed < '2024-01-01' GROUP BY yr, mo ORDER BY yr, mo",
    ),
    (
        "Q6",
        "SELECT buyer, COUNT(*) AS n, ROUND(SUM(amount), 2) AS spent FROM orders_like \
         GROUP BY buyer ORDER BY spent DESC, buyer LIMIT 10",
    ),
    (
        "Q7",
        "SELECT zone, COUNT(*) AS cnt, ROUND(SUM(amount), 2) AS total, \
         ROUND(AVG(amount), 2) AS avg_amt, ROUND(MIN(amount), 2) AS min_amt, \
         ROUND(MAX(amount), 2) AS max_amt, COUNT(DISTINCT buyer) AS buyers FROM orders_like \
         WHERE placed BETWEEN '2022-01-01' AND '2023-12-31' GROUP BY zone ORDER BY total DESC",
    ),
];

#[test]
#[ignore = "measurement: run explicitly with --ignored --nocapture"]
fn scan_decode_timings() {
    let rows = std::env::var("PINTAIL_BENCH_ROWS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(20_000_000_u64);
    let runs = std::env::var("PINTAIL_BENCH_RUNS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(9_usize);
    let wanted = std::env::var("PINTAIL_BENCH_QUERIES").ok();
    let clock = Instant::now();
    let fixture = Fixture::new(rows);
    eprintln!("ingested {rows} rows in {:?}", clock.elapsed());
    for (label, sql) in CASES {
        if wanted
            .as_deref()
            .is_some_and(|wanted| !wanted.split(',').any(|name| name == *label))
        {
            continue;
        }
        let (answer, _) = fixture.run(sql);
        let mut times = (0..runs)
            .map(|_| {
                let (again, elapsed) = fixture.run(sql);
                assert_eq!(again, answer, "{label} answer changed between runs");
                elapsed.as_secs_f64() * 1000.0
            })
            .collect::<Vec<_>>();
        times.sort_by(f64::total_cmp);
        let digest = answer.join(";");
        eprintln!(
            "{label}: median {:.2} ms, min {:.2} ms, answer {} rows: {}",
            times[times.len() / 2],
            times[0],
            answer.len(),
            if digest.len() > 300 {
                &digest[..300]
            } else {
                &digest
            }
        );
    }
}
