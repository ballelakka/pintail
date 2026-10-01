//! What filtered analytical scans pay to choose their rows.
//!
//! `#[ignore]`: measurement, not assertion. Run with
//! `PINTAIL_DISABLE_SETTLED_MEMO=1 cargo test --release -p pintail-exec
//! --test integration selection_cost:: -- --ignored --nocapture`.
//! `SELECTION_ROWS` sets the table size (default 20,000,000),
//! `SELECTION_RUNS` the timed repetitions, `SELECTION_ONLY` a label
//! substring, and `SELECTION_PROFILE=1` prints each case's operator profile.
//!
//! The table is invented: a wide fact table whose label, zone and day
//! columns are spread uniformly and periodically over the key, so no block
//! is prunable by its statistics and every predicate must test each row.
//! Each case's answer is checked against a direct computation over the
//! generator, so a faster run that changed an answer fails.

use std::collections::BTreeMap;

use pintail_catalog::{
    CatalogSnapshot, DatabaseEntry, DatabaseId, TableEntry, TableId, TableStatistics,
};
use pintail_exec::collation::Collation;
use pintail_exec::{Execution, LogicalPlanner, Optimizer, PhysicalPlanner, SnapshotScanProvider};
use pintail_sql::{Binder, parse_statement};
use pintail_store::{StoreOptions, TableStore};
use pintail_types::{Column, DataType, KeyPart, PrimaryKey, StoredRow, TableSchema, Value};

const LABELS: [&str; 5] = ["new", "open", "sent", "done", "void"];
const ZONES: [&str; 8] = [
    "north", "south", "east", "west", "upper", "lower", "inner", "outer",
];
const CHUNK: u64 = 100_000;

fn schema() -> TableSchema {
    TableSchema::new(
        1,
        vec![
            Column::new(1, "id", DataType::UInt64, false),
            Column::new(2, "account", DataType::UInt32, false),
            Column::new(
                3,
                "amount",
                DataType::Decimal {
                    precision: 12,
                    scale: 2,
                },
                false,
            ),
            Column::new(4, "label", DataType::Utf8, false),
            Column::new(5, "zone", DataType::Utf8, false),
            Column::new(6, "day", DataType::Date32, false),
        ],
    )
    .expect("schema")
}

/// Days since 2020-01-01 for row `id`, spread over five years.
fn day_offset(id: u64) -> u64 {
    (id * 7) % 1825
}

/// Amount in cents for row `id`.
fn cents(id: u64) -> u64 {
    (1 + id % 20) * (1000 + (id * 7919) % 99_000)
}

fn civil(offset: u64) -> (i64, u32, u32) {
    // Days from 1970-01-01 to 2020-01-01 is 18262.
    let days = 18_262 + i64::try_from(offset).expect("small");
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = u32::try_from(doy - (153 * mp + 2) / 5 + 1).expect("day");
    let m = u32::try_from(if mp < 10 { mp + 3 } else { mp - 9 }).expect("month");
    (if m <= 2 { y + 1 } else { y }, m, d)
}

fn row(id: u64) -> StoredRow {
    let (year, month, day) = civil(day_offset(id));
    let amount = cents(id);
    StoredRow::new(
        PrimaryKey::new(vec![KeyPart::UInt64(id)]).expect("key"),
        vec![
            Value::UInt64(id),
            Value::UInt64(1 + (id * 17) % 100_000),
            Value::Utf8(format!("{}.{:02}", amount / 100, amount % 100)),
            Value::Utf8(LABELS[usize::try_from(id % 5).expect("small")].to_owned()),
            Value::Utf8(ZONES[usize::try_from(id % 8).expect("small")].to_owned()),
            Value::Utf8(format!("{year:04}-{month:02}-{day:02}")),
        ],
        id + 1,
        false,
    )
}

struct Fixture {
    _directory: tempfile::TempDir,
    table: TableStore,
    catalog: CatalogSnapshot,
    rows: u64,
}

impl Fixture {
    fn new(rows: u64) -> Self {
        let directory = tempfile::tempdir().expect("directory");
        let mut table =
            TableStore::open(directory.path(), schema(), StoreOptions::default()).expect("table");
        let mut start = 1;
        while start <= rows {
            let end = (start + CHUNK).min(rows + 1);
            table
                .bulk_ingest_snapshot((start..end).map(row).collect())
                .expect("rows");
            start = end;
        }
        let entry = TableEntry::new(
            TableId::new(1),
            "facts",
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
            rows,
        }
    }

    fn run(&self, sql: &str, profile: bool) -> (Vec<String>, f64) {
        let snapshot = self.table.snapshot();
        let provider =
            SnapshotScanProvider::new([(DatabaseId::new(1), TableId::new(1), &snapshot)])
                .expect("provider");
        let started = std::time::Instant::now();
        let bound = Binder::new(&self.catalog, Some("app"))
            .bind(&parse_statement(sql).expect("parse"))
            .expect("bind");
        let physical = PhysicalPlanner::plan(
            Optimizer::optimize(LogicalPlanner::plan(bound)),
            Collation::default(),
        )
        .expect("plan");
        let mut execution =
            Execution::start(physical, &provider, 4 << 30, Collation::default()).expect("start");
        let mut rows = Vec::new();
        while let Some(batch) = execution.next_batch().expect("batch") {
            for row in batch.selection().selected_rows() {
                let values: Vec<String> = batch
                    .columns()
                    .iter()
                    .map(|column| match column.value_owned(row).expect("value") {
                        Value::Int64(value) => value.to_string(),
                        Value::UInt64(value) => value.to_string(),
                        Value::Utf8(text) => text,
                        other => format!("{other:?}"),
                    })
                    .collect();
                rows.push(values.join("|"));
            }
        }
        let elapsed = started.elapsed().as_secs_f64() * 1000.0;
        if profile && let Some(profile) = execution.profile() {
            println!("{}", profile.render());
        }
        (rows, elapsed)
    }

    /// Rows of the five-label count, by direct computation.
    fn expected_label_count(&self, label: u64, first_id: u64) -> Vec<String> {
        let count = (first_id..=self.rows).filter(|id| id % 5 == label).count();
        vec![count.to_string()]
    }

    /// `(year, month) -> (count, cents)` for days in `[from, to)` offsets.
    fn expected_monthly(&self, from: u64, to: u64) -> Vec<String> {
        let mut groups: BTreeMap<(i64, u32), (u64, u64)> = BTreeMap::new();
        for id in 1..=self.rows {
            let offset = day_offset(id);
            if offset >= from && offset < to {
                let (year, month, _) = civil(offset);
                let entry = groups.entry((year, month)).or_default();
                entry.0 += 1;
                entry.1 += cents(id);
            }
        }
        groups
            .into_iter()
            .map(|((year, month), (count, sum))| {
                format!("{year}|{month}|{count}|{}.{:02}", sum / 100, sum % 100)
            })
            .collect()
    }

    /// Per zone: count and sum for days in `[from, to)` offsets, ordered
    /// by sum descending.
    fn expected_zones(&self, from: u64, to: u64) -> Vec<String> {
        let mut groups: BTreeMap<usize, (u64, u64)> = BTreeMap::new();
        for id in 1..=self.rows {
            let offset = day_offset(id);
            if offset >= from && offset < to {
                let entry = groups
                    .entry(usize::try_from(id % 8).expect("small"))
                    .or_default();
                entry.0 += 1;
                entry.1 += cents(id);
            }
        }
        let mut rows: Vec<(u64, String)> = groups
            .into_iter()
            .map(|(zone, (count, sum))| {
                (
                    sum,
                    format!("{}|{count}|{}.{:02}", ZONES[zone], sum / 100, sum % 100),
                )
            })
            .collect();
        rows.sort_by_key(|row| std::cmp::Reverse(row.0));
        rows.into_iter().map(|(_, row)| row).collect()
    }
}

#[test]
#[ignore = "measurement, not an assertion"]
fn selection_cost() {
    let rows = std::env::var("SELECTION_ROWS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(20_000_000_u64);
    let runs = std::env::var("SELECTION_RUNS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(11_usize);
    let only = std::env::var("SELECTION_ONLY").ok();
    let profile = std::env::var_os("SELECTION_PROFILE").is_some();
    let built = std::time::Instant::now();
    let fixture = Fixture::new(rows);
    println!(
        "fixture: {rows} rows in {:.1}s",
        built.elapsed().as_secs_f64()
    );
    // Offsets: 2023-01-01 is day 1096, 2024-01-01 is 1461, 2022-01-01 is
    // 731 and 2024-01-01 again closes the two-year range.
    let cases: Vec<(&str, String, Vec<String>)> = vec![
        (
            "label count",
            "SELECT COUNT(*) FROM facts WHERE label = 'sent'".to_owned(),
            fixture.expected_label_count(2, 1),
        ),
        (
            "label count + key",
            "SELECT COUNT(*) FROM facts WHERE label = 'sent' AND id >= 3".to_owned(),
            fixture.expected_label_count(2, 3),
        ),
        (
            "monthly range",
            "SELECT YEAR(day) AS yr, MONTH(day) AS mo, COUNT(*), SUM(amount) FROM facts \
             WHERE day >= '2023-01-01' AND day < '2024-01-01' GROUP BY yr, mo ORDER BY yr, mo"
                .to_owned(),
            fixture.expected_monthly(1096, 1461),
        ),
        (
            "zone between",
            "SELECT zone, COUNT(*), SUM(amount) FROM facts \
             WHERE day BETWEEN '2022-01-01' AND '2023-12-31' GROUP BY zone ORDER BY SUM(amount) DESC"
                .to_owned(),
            fixture.expected_zones(731, 1461),
        ),
        (
            "zone between + distinct",
            "SELECT zone, COUNT(*), SUM(amount), COUNT(DISTINCT account) FROM facts \
             WHERE day BETWEEN '2022-01-01' AND '2023-12-31' GROUP BY zone ORDER BY SUM(amount) DESC"
                .to_owned(),
            Vec::new(),
        ),
    ];
    for (label, sql, expected) in &cases {
        if only
            .as_ref()
            .is_some_and(|only| !label.contains(only.as_str()))
        {
            continue;
        }
        let (answer, _) = fixture.run(sql, profile);
        if expected.is_empty() {
            assert!(!answer.is_empty(), "{label}: empty answer");
        } else {
            let answer: Vec<String> = if label.starts_with("zone") {
                answer
                    .iter()
                    .map(|row| row.splitn(4, '|').take(3).collect::<Vec<_>>().join("|"))
                    .collect()
            } else {
                answer
            };
            assert_eq!(&answer, expected, "{label}: answer differs");
        }
        let mut times: Vec<f64> = (0..runs).map(|_| fixture.run(sql, false).1).collect();
        times.sort_by(f64::total_cmp);
        println!(
            "{label:<24} median {:>8.2}ms  min {:>8.2}ms",
            times[runs / 2],
            times[0]
        );
    }
}
