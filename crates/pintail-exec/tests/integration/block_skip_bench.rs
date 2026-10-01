//! In-process timing of recent-window filters on an event table whose
//! event time rises with its key, laid out as a compacted replica holds it:
//! segments of four million rows. Segment statistics narrow a window to one
//! or two segments; the blocks inside them are what block value skipping
//! leaves out. A filter on a column that follows no order is the control:
//! nothing can be skipped, so its time should not move. Ignored: a
//! measurement, not a gate. Run with
//! `cargo test --profile recovery -p pintail-exec --test integration block_skip_bench:: -- --ignored --nocapture`.
//! `PINTAIL_BENCH_ROWS` overrides the table size, `PINTAIL_BENCH_SEGMENT_ROWS`
//! the rows per segment.
use std::time::{Duration, Instant};

use pintail_catalog::{
    CatalogSnapshot, DatabaseEntry, DatabaseId, TableEntry, TableId, TableStatistics,
};
use pintail_exec::collation::Collation;
use pintail_exec::{
    Execution, LogicalPlanner, Optimizer, PhysicalPlanner, PhysicalScanStats, SnapshotScanProvider,
};
use pintail_sql::{Binder, parse_statement};
use pintail_store::{StoreOptions, TableStore};
use pintail_types::{Column, DataType, KeyPart, PrimaryKey, StoredRow, TableSchema, Value};

/// Seconds between consecutive events: 20 million rows span about
/// fifteen months.
const STEP_SECONDS: u64 = 2;
/// 2024-01-01 00:00:00 UTC.
const EPOCH: u64 = 1_704_067_200;

fn schema() -> TableSchema {
    TableSchema::new(
        1,
        vec![
            Column::new(1, "id", DataType::UInt64, false),
            Column::new(2, "occurred_at", DataType::DateTime64 { fsp: 0 }, false),
            Column::new(3, "score", DataType::Int64, false),
            Column::new(4, "amount", DataType::Int64, false),
            Column::new(5, "cycle_day", DataType::Date32, false),
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
    let (year, month, day) = civil(total / 86_400);
    let within = total % 86_400;
    format!(
        "{year:04}-{month:02}-{day:02} {:02}:{:02}:{:02}",
        within / 3600,
        within / 60 % 60,
        within % 60
    )
}

fn row(id: u64) -> StoredRow {
    // A multiplicative hash: a score with no relation to insertion order.
    let score = (id.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 40) % 10_000;
    StoredRow::new(
        PrimaryKey::new(vec![KeyPart::UInt64(id)]).expect("key"),
        vec![
            Value::UInt64(id),
            Value::Utf8(timestamp(id * STEP_SECONDS)),
            Value::Int64(i64::try_from(score).expect("small") - 5_000),
            Value::Int64(i64::try_from(id % 1_000).expect("small")),
            // A date that recurs every five years of rows: no block can be
            // skipped for a range on it, so it costs only the check.
            Value::Utf8(timestamp((id % 1_825) * 86_400)[..10].to_owned()),
        ],
        1,
        false,
    )
}

fn run(catalog: &CatalogSnapshot, store: &TableStore, sql: &str) -> (Duration, PhysicalScanStats) {
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
    let mut execution = Execution::start(
        physical,
        &provider,
        4 * 1024 * 1024 * 1024,
        Collation::default(),
    )
    .expect("start");
    while execution.next_batch().expect("batch").is_some() {}
    let elapsed = clock.elapsed();
    if std::env::var_os("PINTAIL_BENCH_PROFILE").is_some()
        && let Some(profile) = execution.profile()
    {
        eprintln!("{sql}\n{}", profile.render());
    }
    let stats = provider
        .scan_stats(DatabaseId::new(1), TableId::new(1))
        .unwrap_or_default();
    (elapsed, stats)
}

#[test]
#[ignore = "measurement: run with --ignored --nocapture"]
#[allow(clippy::too_many_lines, clippy::cast_precision_loss)] // load, then one table of shapes; display only
fn recent_windows_over_an_event_table() {
    let env = |name: &str, default: u64| {
        std::env::var(name)
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(default)
    };
    let rows = env("PINTAIL_BENCH_ROWS", 20_000_000);
    let segment_rows = env("PINTAIL_BENCH_SEGMENT_ROWS", 4_000_000);
    let directory = tempfile::tempdir().expect("directory");
    let options = StoreOptions {
        background_compaction: false,
        ..StoreOptions::default()
    };
    let mut store = TableStore::open(directory.path(), schema(), options).expect("store");
    let load = Instant::now();
    let mut next = 0;
    while next < rows {
        let end = (next + segment_rows).min(rows);
        store
            .bulk_ingest_snapshot((next..end).map(row).collect())
            .expect("ingest");
        next = end;
    }
    eprintln!(
        "{rows} rows in segments of {segment_rows}, loaded in {:.1}s",
        load.elapsed().as_secs_f64()
    );
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

    let span = rows * STEP_SECONDS;
    let day = 86_400;
    // (name, window length in seconds, template). `{from}`/`{to}` move each
    // run so nothing remembered answers.
    let shapes: [(&str, u64, &str); 7] = [
        (
            "fixed cost: one key",
            0,
            "SELECT COUNT(*), SUM(amount) FROM events WHERE id = {score}",
        ),
        (
            "last 7 days, count+sum",
            7 * day,
            "SELECT COUNT(*), SUM(amount) FROM events WHERE occurred_at >= '{from}'",
        ),
        (
            "one month, count+sum",
            30 * day,
            "SELECT COUNT(*), SUM(amount) FROM events \
             WHERE occurred_at BETWEEN '{from}' AND '{to}'",
        ),
        (
            "one month, top score",
            30 * day,
            "SELECT id, score FROM events \
             WHERE occurred_at >= '{from}' AND occurred_at < '{to}' \
             ORDER BY score DESC LIMIT 10",
        ),
        (
            "control: recurring date >=<",
            0,
            "SELECT COUNT(*), SUM(amount) FROM events \
             WHERE cycle_day >= '2025-01-01' AND cycle_day < '2026-01-0{run}'",
        ),
        (
            "control: recurring BETWEEN",
            0,
            "SELECT COUNT(*), SUM(amount) FROM events \
             WHERE cycle_day BETWEEN '2024-01-01' AND '2025-12-2{run}'",
        ),
        (
            "control: unordered column",
            0,
            "SELECT COUNT(*), SUM(amount) FROM events WHERE score > {score}",
        ),
    ];
    for (name, window, template) in shapes {
        let mut timings = Vec::new();
        let mut last_stats = PhysicalScanStats::default();
        for run_index in 0..8_u64 {
            // "Last N days" ends at the newest row, minus a few hours per run.
            let to = span - run_index * 3_600;
            let from = to.saturating_sub(window);
            let from = if template.contains("BETWEEN") || template.contains("{to}") {
                // A month somewhere in the table.
                (run_index * 37 * day) % span.saturating_sub(window).max(1)
            } else {
                from
            };
            let sql = template
                .replace("{from}", &timestamp(from))
                .replace("{to}", &timestamp(from + window))
                .replace("{score}", &(4_000 + run_index).to_string())
                .replace("{run}", &(run_index + 1).to_string());
            let (elapsed, stats) = run(&catalog, &store, &sql);
            if run_index > 0 {
                timings.push(elapsed);
            }
            last_stats = stats;
        }
        timings.sort();
        eprintln!(
            "{name:<28} min {:>8.2} ms  median {:>8.2} ms  segments {}/{} blocks decoded {} \
             value-skipped {} decompressed {:.1} MB",
            timings[0].as_secs_f64() * 1e3,
            timings[timings.len() / 2].as_secs_f64() * 1e3,
            last_stats.segments_read,
            last_stats.segments_total(),
            last_stats.blocks_decoded,
            last_stats.blocks_value_skipped,
            last_stats.bytes_decompressed as f64 / 1e6,
        );
    }
}
