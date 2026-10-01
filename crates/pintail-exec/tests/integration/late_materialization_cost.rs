//! What a selective filter over a wide table pays for the columns it does
//! not test.
//!
//! `#[ignore]`: measurement, not assertion. Run with
//! `PINTAIL_DISABLE_SETTLED_MEMO=1 cargo test --profile recovery -p
//! pintail-exec --test integration late_materialization_cost:: -- --ignored
//! --nocapture`. `LATE_ROWS` sets the table size (default 20,000,000),
//! `LATE_RUNS` the timed repetitions, `LATE_ONLY` a label substring, and
//! `LATE_PROFILE=1` beside `PINTAIL_PROFILE=1` prints each case's profile.
//!
//! The table is invented: twelve columns of integers, decimals, temporals
//! and text, with a selector column spread pseudo-randomly over the key so
//! `sel < n` keeps `n` rows in ten thousand, scattered through every block.
//! No block statistic can skip anything; only the selection can. Every
//! answer is checked against a direct computation over the generator.

use std::collections::BTreeMap;

use pintail_catalog::{
    CatalogSnapshot, DatabaseEntry, DatabaseId, TableEntry, TableId, TableStatistics,
};
use pintail_exec::collation::Collation;
use pintail_exec::{Execution, LogicalPlanner, Optimizer, PhysicalPlanner, SnapshotScanProvider};
use pintail_sql::{Binder, parse_statement};
use pintail_store::{StoreOptions, TableStore};
use pintail_types::{Column, DataType, KeyPart, PrimaryKey, StoredRow, TableSchema, Value};

const LABELS: [&str; 5] = ["amber", "beige", "coral", "denim", "ebony"];
const CHUNK: u64 = 100_000;
const THRESHOLDS: [u64; 4] = [1, 100, 1000, 5000];

fn schema() -> TableSchema {
    let decimal = |precision, scale| DataType::Decimal { precision, scale };
    TableSchema::new(
        1,
        vec![
            Column::new(1, "id", DataType::UInt64, false),
            Column::new(2, "sel", DataType::Int64, false),
            Column::new(3, "grp", DataType::UInt32, false),
            Column::new(4, "qty", DataType::Int64, false),
            Column::new(5, "amount", decimal(12, 2), false),
            Column::new(6, "price", decimal(10, 2), false),
            Column::new(7, "created", DataType::DateTime64 { fsp: 0 }, false),
            Column::new(8, "shipped", DataType::Date32, true),
            Column::new(9, "label", DataType::Utf8, false),
            Column::new(10, "note", DataType::Utf8, false),
            Column::new(11, "city", DataType::Utf8, false),
            Column::new(12, "score", DataType::Int64, true),
        ],
    )
    .expect("schema")
}

fn mix(id: u64) -> u64 {
    let mut z = id.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// One generated row, in plain Rust terms.
struct Gen {
    sel: u64,
    qty: u64,
    amount: u64,
    price: u64,
    created: String,
    shipped: Option<String>,
    label: usize,
    note: String,
    city: String,
    score: Option<u64>,
}

fn civil(days_since_2020: u64) -> (u64, u64, u64) {
    let days = 18_262 + i64::try_from(days_since_2020).expect("small");
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (
        u64::try_from(y).expect("year"),
        u64::try_from(m).expect("month"),
        u64::try_from(d).expect("day"),
    )
}

fn generate(id: u64) -> Gen {
    let h = mix(id);
    let day = (id * 13) % 1825;
    let (y, m, d) = civil(day);
    let second = (h >> 20) % 86_400;
    let (sy, sm, sd) = civil((day + 3) % 1825);
    Gen {
        sel: h % 10_000,
        qty: 1 + (h >> 14) % 50,
        amount: 100 + (h >> 24) % 9_999_900,
        price: 1 + (id * 7919) % 99_999,
        created: format!(
            "{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02}",
            second / 3600,
            second / 60 % 60,
            second % 60
        ),
        shipped: (!id.is_multiple_of(7)).then(|| format!("{sy:04}-{sm:02}-{sd:02}")),
        label: usize::try_from(id % 5).expect("small"),
        note: format!("n{h:016x}{id:08x}"),
        city: format!("c{:03}", (h >> 40) % 211),
        score: (!id.is_multiple_of(11)).then_some((h >> 33) % 1000),
    }
}

fn cents(value: u64) -> String {
    format!("{}.{:02}", value / 100, value % 100)
}

fn row(id: u64) -> StoredRow {
    let g = generate(id);
    StoredRow::new(
        PrimaryKey::new(vec![KeyPart::UInt64(id)]).expect("key"),
        vec![
            Value::UInt64(id),
            Value::Int64(i64::try_from(g.sel).expect("small")),
            Value::UInt64(id % 1000),
            Value::Int64(i64::try_from(g.qty).expect("small")),
            Value::Utf8(cents(g.amount)),
            Value::Utf8(cents(g.price)),
            Value::Utf8(g.created),
            g.shipped.map_or(Value::Null, Value::Utf8),
            Value::Utf8(LABELS[g.label].to_owned()),
            Value::Utf8(g.note),
            Value::Utf8(g.city),
            g.score.map_or(Value::Null, |score| {
                Value::Int64(i64::try_from(score).expect("small"))
            }),
        ],
        id + 1,
        false,
    )
}

#[derive(Default)]
struct Totals {
    count: u64,
    amount: u64,
    qty: u64,
    created: Option<String>,
    note: Option<String>,
    city: Option<String>,
    score: Option<u64>,
}

#[derive(Default)]
struct Grouped {
    count: u64,
    price: u64,
    shipped: Option<String>,
    note: Option<String>,
}

fn keep_max(slot: &mut Option<String>, value: &str) {
    if slot.as_deref().is_none_or(|current| value > current) {
        *slot = Some(value.to_owned());
    }
}

fn keep_min(slot: &mut Option<String>, value: &str) {
    if slot.as_deref().is_none_or(|current| value < current) {
        *slot = Some(value.to_owned());
    }
}

struct Fixture {
    _directory: tempfile::TempDir,
    table: TableStore,
    catalog: CatalogSnapshot,
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
            "wide",
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

    fn run(&self, sql: &str, profile: bool) -> (Vec<String>, f64, String) {
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
                        Value::Null => "NULL".to_owned(),
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
        let stats = provider
            .scan_stats(DatabaseId::new(1), TableId::new(1))
            .map(|stats| {
                format!(
                    "blocks {}/{} decoded {} bytes_decompressed {} values_decoded {}",
                    stats.blocks_read,
                    stats.blocks_total(),
                    stats.blocks_decoded,
                    stats.bytes_decompressed,
                    stats.values_decoded
                )
            })
            .unwrap_or_default();
        (rows, elapsed, stats)
    }
}

/// Per threshold: the totals row, the grouped rows and the projected rows.
type Answers = (Vec<String>, Vec<String>, Vec<String>);

/// Expected answers for every threshold, from one pass over the generator.
fn expected(rows: u64) -> BTreeMap<u64, Answers> {
    let mut totals: Vec<Totals> = THRESHOLDS.iter().map(|_| Totals::default()).collect();
    let mut groups: Vec<BTreeMap<usize, Grouped>> =
        THRESHOLDS.iter().map(|_| BTreeMap::new()).collect();
    let mut points = Vec::new();
    for id in 1..=rows {
        let h = mix(id) % 10_000;
        if h >= THRESHOLDS[THRESHOLDS.len() - 1] {
            continue;
        }
        let g = generate(id);
        if g.sel < THRESHOLDS[0] {
            points.push(format!("{id}|{}|{}|{}", g.note, cents(g.amount), g.created));
        }
        for (index, threshold) in THRESHOLDS.iter().enumerate() {
            if g.sel >= *threshold {
                continue;
            }
            let t = &mut totals[index];
            t.count += 1;
            t.amount += g.amount;
            t.qty += g.qty;
            keep_max(&mut t.created, &g.created);
            keep_min(&mut t.note, &g.note);
            keep_max(&mut t.city, &g.city);
            if let Some(score) = g.score {
                *t.score.get_or_insert(0) += score;
            }
            let entry = groups[index].entry(g.label).or_default();
            entry.count += 1;
            entry.price += g.price;
            if let Some(shipped) = &g.shipped {
                keep_max(&mut entry.shipped, shipped);
            }
            keep_max(&mut entry.note, &g.note);
        }
    }
    let text = |value: &Option<String>| value.clone().unwrap_or_else(|| "NULL".to_owned());
    THRESHOLDS
        .iter()
        .enumerate()
        .map(|(index, threshold)| {
            let t = &totals[index];
            let total = vec![format!(
                "{}|{}|{}|{}|{}|{}|{}",
                t.count,
                cents(t.amount),
                t.qty,
                text(&t.created),
                text(&t.note),
                text(&t.city),
                t.score
                    .map_or_else(|| "NULL".to_owned(), |score| score.to_string())
            )];
            let grouped = groups[index]
                .iter()
                .map(|(label, g)| {
                    format!(
                        "{}|{}|{}|{}|{}",
                        LABELS[*label],
                        g.count,
                        cents(g.price),
                        text(&g.shipped),
                        text(&g.note)
                    )
                })
                .collect();
            let points = if index == 0 {
                points.clone()
            } else {
                Vec::new()
            };
            (*threshold, (total, grouped, points))
        })
        .collect()
}

#[test]
#[ignore = "measurement, not an assertion"]
fn late_materialization_cost() {
    let rows = std::env::var("LATE_ROWS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(20_000_000_u64);
    let runs = std::env::var("LATE_RUNS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(11_usize);
    let only = std::env::var("LATE_ONLY").ok();
    let profile = std::env::var_os("LATE_PROFILE").is_some();
    let built = std::time::Instant::now();
    let fixture = Fixture::new(rows);
    let expected = expected(rows);
    println!(
        "fixture: {rows} rows in {:.1}s",
        built.elapsed().as_secs_f64()
    );
    let mut cases: Vec<(String, String, Vec<String>)> = Vec::new();
    for (threshold, (total, grouped, points)) in &expected {
        #[allow(clippy::cast_precision_loss)]
        let percent = *threshold as f64 / 100.0;
        cases.push((
            format!("totals {percent}%"),
            format!(
                "SELECT COUNT(*), SUM(amount), SUM(qty), MAX(created), MIN(note), MAX(city), \
                 SUM(score) FROM wide WHERE sel < {threshold}"
            ),
            total.clone(),
        ));
        cases.push((
            format!("grouped {percent}%"),
            format!(
                "SELECT label, COUNT(*), SUM(price), MAX(shipped), MAX(note) FROM wide \
                 WHERE sel < {threshold} GROUP BY label ORDER BY label"
            ),
            grouped.clone(),
        ));
        if !points.is_empty() {
            cases.push((
                format!("rows {percent}%"),
                format!(
                    "SELECT id, note, amount, created FROM wide WHERE sel < {threshold} \
                     ORDER BY id"
                ),
                points.clone(),
            ));
        }
    }
    for (label, sql, expected) in &cases {
        if only
            .as_ref()
            .is_some_and(|only| !label.contains(only.as_str()))
        {
            continue;
        }
        let (answer, _, stats) = fixture.run(sql, profile);
        assert_eq!(&answer, expected, "{label}: answer differs");
        let mut times: Vec<f64> = (0..runs).map(|_| fixture.run(sql, false).1).collect();
        times.sort_by(f64::total_cmp);
        println!(
            "{label:<16} median {:>8.2}ms  min {:>8.2}ms  {stats}",
            times[runs / 2],
            times[0]
        );
    }
}
