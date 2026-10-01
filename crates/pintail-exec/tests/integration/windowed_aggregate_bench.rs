//! In-process timing of aggregates over a recent time window of an event
//! table whose event time rises with its key: the window selects a few
//! hundred thousand to a little over a million rows out of twenty million,
//! so the scan skips most blocks and the aggregate is what is left to pay
//! for. Ungrouped, grouped by a low-cardinality text column and grouped by
//! the day, over a seven-day and a thirty-day window. Every answer is
//! checked against a direct computation over the generator.
//!
//! Ignored: a measurement, not a gate. Run with
//! `PINTAIL_DISABLE_SETTLED_MEMO=1 cargo test --profile recovery -p
//! pintail-exec --test integration windowed_aggregate_bench:: -- --ignored
//! --nocapture`. `WINDOW_ROWS` sets the table size, `WINDOW_ONLY` a label
//! substring, and `WINDOW_PROFILE=1` beside `PINTAIL_PROFILE=1` prints the
//! first run's profile of each case.
use std::collections::BTreeMap;
use std::time::Instant;

use pintail_catalog::{
    CatalogSnapshot, DatabaseEntry, DatabaseId, TableEntry, TableId, TableStatistics,
};
use pintail_exec::collation::Collation;
use pintail_exec::{Execution, LogicalPlanner, Optimizer, PhysicalPlanner, SnapshotScanProvider};
use pintail_sql::{Binder, parse_statement};
use pintail_store::{StoreOptions, TableStore};
use pintail_types::{Column, DataType, KeyPart, PrimaryKey, StoredRow, TableSchema, Value};

/// Seconds between consecutive events.
const STEP_SECONDS: u64 = 2;
/// 2024-01-01 00:00:00 UTC.
const EPOCH: u64 = 1_704_067_200;
const DAY: u64 = 86_400;
const SEGMENT_ROWS: u64 = 4_000_000;
const CHANNELS: [&str; 5] = ["email", "kiosk", "phone", "store", "web"];

fn schema() -> TableSchema {
    TableSchema::new(
        1,
        vec![
            Column::new(1, "id", DataType::UInt64, false),
            Column::new(2, "occurred_at", DataType::DateTime64 { fsp: 0 }, false),
            Column::new(
                3,
                "price",
                DataType::Decimal {
                    precision: 10,
                    scale: 2,
                },
                false,
            ),
            Column::new(4, "channel", DataType::Utf8, false),
            Column::new(5, "amount", DataType::Int64, false),
        ],
    )
    .expect("schema")
}

fn civil(days: u64) -> (u64, u64, u64) {
    let z = days + 719_468;
    let era = z / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + u64::from(month <= 2);
    (year, month, day)
}

/// The epoch plus `seconds`, as the column's text.
fn timestamp(seconds: u64) -> String {
    let total = EPOCH + seconds;
    let (year, month, day) = civil(total / DAY);
    let within = total % DAY;
    format!(
        "{year:04}-{month:02}-{day:02} {:02}:{:02}:{:02}",
        within / 3600,
        within / 60 % 60,
        within % 60
    )
}

fn price(id: u64) -> u64 {
    1 + (id * 7919) % 99_999
}

fn channel(id: u64) -> usize {
    usize::try_from((id.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 40) % 5).expect("small")
}

fn amount(id: u64) -> i64 {
    i64::try_from(id % 1_000).expect("small") - 300
}

fn cents(value: u64) -> String {
    format!("{}.{:02}", value / 100, value % 100)
}

fn row(id: u64) -> StoredRow {
    StoredRow::new(
        PrimaryKey::new(vec![KeyPart::UInt64(id)]).expect("key"),
        vec![
            Value::UInt64(id),
            Value::Utf8(timestamp(id * STEP_SECONDS)),
            Value::Utf8(cents(price(id))),
            Value::Utf8(CHANNELS[channel(id)].to_owned()),
            Value::Int64(amount(id)),
        ],
        1,
        false,
    )
}

#[derive(Default, Clone)]
struct Totals {
    count: u64,
    price: u64,
    amount: i64,
    first: u64,
    last: u64,
}

impl Totals {
    fn add(&mut self, id: u64) {
        if self.count == 0 {
            self.first = id;
        }
        self.count += 1;
        self.price += price(id);
        self.amount += amount(id);
        self.last = id;
    }

    /// AVG of a DECIMAL(10,2): six places, rounded half away from zero.
    fn average(&self) -> String {
        let scaled = u128::from(self.price) * 10_000;
        let count = u128::from(self.count);
        let units = (scaled * 2 + count) / (count * 2);
        format!("{}.{:06}", units / 1_000_000, units % 1_000_000)
    }

    fn row(&self, shape: Shape) -> String {
        match shape {
            Shape::CountSum => format!("{}|{}|{}", self.count, cents(self.price), self.amount),
            Shape::Full => format!(
                "{}|{}|{}|{}|{}",
                self.count,
                cents(self.price),
                self.average(),
                timestamp(self.first * STEP_SECONDS),
                timestamp(self.last * STEP_SECONDS)
            ),
            Shape::Daily => format!("{}|{}", self.count, cents(self.price)),
        }
    }
}

#[derive(Clone, Copy)]
enum Shape {
    CountSum,
    Full,
    Daily,
}

#[derive(Clone, Copy)]
enum Grouping {
    None,
    Channel,
    Day,
}

/// The window's ids: `occurred_at` between `from` and `to` seconds, both
/// inclusive.
fn ids(rows: u64, from: u64, to: u64) -> std::ops::RangeInclusive<u64> {
    from.div_ceil(STEP_SECONDS)..=(to / STEP_SECONDS).min(rows - 1)
}

fn expected(rows: u64, from: u64, to: u64, grouping: Grouping, shape: Shape) -> Vec<String> {
    let mut groups = BTreeMap::<String, Totals>::new();
    for id in ids(rows, from, to) {
        let key = match grouping {
            Grouping::None => String::new(),
            Grouping::Channel => CHANNELS[channel(id)].to_owned(),
            Grouping::Day => timestamp(id * STEP_SECONDS)[..10].to_owned(),
        };
        groups.entry(key).or_default().add(id);
    }
    if matches!(grouping, Grouping::None) && groups.is_empty() {
        return vec![match shape {
            Shape::CountSum => "0|NULL|NULL".to_owned(),
            _ => "0|NULL|NULL|NULL|NULL".to_owned(),
        }];
    }
    groups
        .iter()
        .map(|(key, totals)| match grouping {
            Grouping::None => totals.row(shape),
            _ => format!("{key}|{}", totals.row(shape)),
        })
        .collect()
}

fn run(
    catalog: &CatalogSnapshot,
    store: &TableStore,
    sql: &str,
    profile: bool,
) -> (Vec<String>, f64) {
    let snapshot = store.snapshot();
    let provider = SnapshotScanProvider::new([(DatabaseId::new(1), TableId::new(1), &snapshot)])
        .expect("provider");
    let clock = Instant::now();
    let bound = Binder::new(catalog, Some("app"))
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
                    Value::Null => "NULL".to_owned(),
                    Value::DecimalAverage(average) => average.label.clone(),
                    other => format!("{other:?}"),
                })
                .collect();
            rows.push(values.join("|"));
        }
    }
    let elapsed = clock.elapsed().as_secs_f64() * 1e3;
    if profile && let Some(profile) = execution.profile() {
        eprintln!("{sql}\n{}", profile.render());
    }
    (rows, elapsed)
}

#[test]
#[ignore = "measurement: run with --ignored --nocapture"]
#[allow(clippy::too_many_lines)] // load, then one table of shapes
fn aggregates_over_a_recent_window() {
    let rows = std::env::var("WINDOW_ROWS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(20_000_000_u64);
    let only = std::env::var("WINDOW_ONLY").ok();
    let profile = std::env::var_os("WINDOW_PROFILE").is_some();
    let directory = tempfile::tempdir().expect("directory");
    let options = StoreOptions {
        background_compaction: false,
        ..StoreOptions::default()
    };
    let mut store = TableStore::open(directory.path(), schema(), options).expect("store");
    let load = Instant::now();
    let mut next = 0;
    while next < rows {
        let end = (next + SEGMENT_ROWS).min(rows);
        store
            .bulk_ingest_snapshot((next..end).map(row).collect())
            .expect("ingest");
        next = end;
    }
    eprintln!("{rows} rows loaded in {:.1}s", load.elapsed().as_secs_f64());
    let entry = TableEntry::new(
        TableId::new(1),
        "events",
        schema(),
        TableStatistics::with_row_count(rows),
    )
    .expect("entry")
    .with_key_columns([1])
    .expect("key");
    let catalog = CatalogSnapshot::new([
        DatabaseEntry::new(DatabaseId::new(1), "app", [entry]).expect("database")
    ])
    .expect("catalog");

    let span = (rows - 1) * STEP_SECONDS;
    let full = "COUNT(*), SUM(price), AVG(price), MIN(occurred_at), MAX(occurred_at)";
    let cases: [(&str, Grouping, Shape, String); 6] = [
        (
            "count+sum",
            Grouping::None,
            Shape::CountSum,
            "SELECT COUNT(*), SUM(price), SUM(amount) FROM events WHERE {window}".to_owned(),
        ),
        (
            "count/sum/avg/min/max",
            Grouping::None,
            Shape::Full,
            format!("SELECT {full} FROM events WHERE {{window}}"),
        ),
        (
            "by channel",
            Grouping::Channel,
            Shape::Full,
            format!(
                "SELECT channel, {full} FROM events WHERE {{window}} \
                 GROUP BY channel ORDER BY channel"
            ),
        ),
        (
            "by day",
            Grouping::Day,
            Shape::Daily,
            "SELECT DATE(occurred_at) AS d, COUNT(*), SUM(price) FROM events \
             WHERE {window} GROUP BY d ORDER BY d"
                .to_owned(),
        ),
        (
            "by day, full",
            Grouping::Day,
            Shape::Full,
            format!(
                "SELECT DATE(occurred_at) AS d, {full} FROM events WHERE {{window}} \
                 GROUP BY d ORDER BY d"
            ),
        ),
        (
            "by formatted day",
            Grouping::Day,
            Shape::Daily,
            "SELECT DATE_FORMAT(occurred_at, '%Y-%m-%d') AS d, COUNT(*), SUM(price) \
             FROM events WHERE {window} GROUP BY d ORDER BY d"
                .to_owned(),
        ),
    ];
    let mut mismatches = 0;
    for (window_name, window) in [("7 days", 7 * DAY), ("30 days", 30 * DAY)] {
        for (name, grouping, shape, template) in &cases {
            let label = format!("{window_name}, {name}");
            if only
                .as_ref()
                .is_some_and(|only| !label.contains(only.as_str()))
            {
                continue;
            }
            let mut timings = Vec::new();
            for run_index in 0..9_u64 {
                // The window ends a few hours earlier each run, so nothing
                // remembered answers a repeat. The seven-day window is
                // open-ended ("since"), the month is a closed BETWEEN.
                let (from, to, predicate) = if window == 7 * DAY {
                    let from = span - window - run_index * 3_600;
                    (from, span, format!("occurred_at >= '{}'", timestamp(from)))
                } else {
                    let from = span / 2 + run_index * 3_600;
                    let to = from + window;
                    (
                        from,
                        to,
                        format!(
                            "occurred_at BETWEEN '{}' AND '{}'",
                            timestamp(from),
                            timestamp(to)
                        ),
                    )
                };
                let sql = template.replace("{window}", &predicate);
                let (answer, elapsed) = run(&catalog, &store, &sql, profile && run_index == 0);
                if run_index == 0 || run_index == 8 {
                    let want = expected(rows, from, to, *grouping, *shape);
                    if answer != want {
                        mismatches += 1;
                        eprintln!(
                            "MISMATCH {label}: {sql}\n  got  {:?}\n  want {:?}",
                            answer.iter().take(3).collect::<Vec<_>>(),
                            want.iter().take(3).collect::<Vec<_>>()
                        );
                    }
                }
                if run_index > 0 {
                    timings.push(elapsed);
                }
            }
            timings.sort_by(f64::total_cmp);
            eprintln!(
                "{label:<32} min {:>8.2} ms  median {:>8.2} ms",
                timings[0],
                timings[timings.len() / 2],
            );
        }
    }
    assert_eq!(mismatches, 0, "answers differ from the generator");
}
