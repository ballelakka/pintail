//! A map of which path every aggregate kind takes under every key shape,
//! and what it costs a row.
//!
//! An invented ledger of integers, decimals, doubles, datetimes, a
//! low-cardinality text column the store dictionary-codes, a plain text
//! column with mostly distinct values, and NULLs in all of them. Each query
//! runs one aggregate, so its time is that aggregate's path plus the scan;
//! the `COUNT(*)` row of each shape is the floor to read the others
//! against. The profile notes say which fold ran or why one declined.
//!
//! `cargo test --profile recovery -p pintail-exec --test integration
//! aggregate_path_survey -- --ignored --nocapture`; `SURVEY_ROWS` sets the
//! table size (2M by default), `SURVEY_ONLY` keeps the queries whose text
//! contains it, `SURVEY_RUNS` the repeats per query (3).

use pintail_catalog::{
    CatalogSnapshot, DatabaseEntry, DatabaseId, TableEntry, TableId, TableStatistics,
};
use pintail_exec::collation::Collation;
use pintail_exec::{Execution, LogicalPlanner, Optimizer, PhysicalPlanner, SnapshotScanProvider};
use pintail_sql::{Binder, parse_statement};
use pintail_store::{StoreOptions, TableStore};
use pintail_types::{Column, DataType, KeyPart, PrimaryKey, StoredRow, TableSchema, Value};

/// Low-cardinality text with case, accents and trailing spaces, so a
/// collation-aware MIN/MAX has ties and near-ties to get right.
const TAGS: [&str; 12] = [
    "amber", "Amber", "ámber ", "birch", "BIRCH", "cedar", "Cédar", "delta", "Echo", "écho",
    "fern ", "zinc",
];
const REGIONS: [&str; 8] = [
    "north", "south", "east", "west", "upper", "lower", "inner", "outer",
];

struct Fixture {
    _directory: tempfile::TempDir,
    table: TableStore,
    catalog: CatalogSnapshot,
}

fn mix(id: u64) -> u64 {
    let mut x = id.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

fn row(id: u64) -> StoredRow {
    let h = mix(id);
    let small = |modulus: u64| usize::try_from(h % modulus).expect("small");
    let null_if = |every: u64, value: Value| {
        if mix(id ^ every).is_multiple_of(every) {
            Value::Null
        } else {
            value
        }
    };
    let qty = i64::try_from(h % 100_000).expect("small") - 20_000;
    let cents = i64::try_from((h >> 20) % 10_000_000).expect("small") - 2_000_000;
    let price = format!(
        "{}{}.{:02}",
        if cents < 0 { "-" } else { "" },
        cents.abs() / 100,
        cents.abs() % 100
    );
    #[allow(clippy::cast_precision_loss)]
    let ratio = ((h >> 8) % 1_000_000) as f64 / 7.0 - 50_000.0;
    let seconds = (h >> 12) % (3 * 365 * 86_400);
    let day = seconds / 86_400;
    let ts = format!(
        "{}-{:02}-{:02} {:02}:{:02}:{:02}",
        2022 + day / 365,
        1 + (day % 365) / 31,
        1 + (day % 365) % 28,
        (seconds / 3_600) % 24,
        (seconds / 60) % 60,
        seconds % 60
    );
    let label = format!("Item-{:08x}", (h >> 16) % 50_000_000);
    StoredRow::new(
        PrimaryKey::new(vec![KeyPart::UInt64(id)]).expect("key"),
        vec![
            Value::UInt64(id),
            null_if(17, Value::Int64(qty)),
            null_if(19, Value::Utf8(price)),
            null_if(23, Value::float64(ratio)),
            null_if(29, Value::Utf8(ts)),
            null_if(31, Value::Utf8(TAGS[small(12)].to_owned())),
            null_if(37, Value::Utf8(label)),
            Value::Int64(i64::try_from(id % 200_000).expect("small")),
            Value::Utf8(REGIONS[usize::try_from((h >> 40) % 8).expect("small")].to_owned()),
        ],
        1,
        false,
    )
}

fn fixture(rows: u64) -> Fixture {
    let schema = TableSchema::new(
        1,
        vec![
            Column::new(1, "id", DataType::UInt64, false),
            Column::new(2, "qty", DataType::Int64, true),
            Column::new(
                3,
                "price",
                DataType::Decimal {
                    precision: 12,
                    scale: 2,
                },
                true,
            ),
            Column::new(4, "ratio", DataType::Float64, true),
            Column::new(5, "ts", DataType::DateTime64 { fsp: 0 }, true),
            Column::new(6, "tag", DataType::Utf8, true),
            Column::new(7, "label", DataType::Utf8, true),
            Column::new(8, "grp", DataType::Int64, false),
            Column::new(9, "region", DataType::Utf8, false),
        ],
    )
    .expect("schema");
    let directory = tempfile::tempdir().expect("directory");
    let mut table =
        TableStore::open(directory.path(), schema.clone(), StoreOptions::default()).expect("table");
    let mut start = 0;
    while start < rows {
        let end = (start + 100_000).min(rows);
        table
            .bulk_ingest_snapshot((start..end).map(row).collect())
            .expect("ingest");
        start = end;
    }
    let entry = TableEntry::new(
        TableId::new(1),
        "ledger",
        schema,
        TableStatistics::with_row_count(rows),
    )
    .expect("entry");
    Fixture {
        _directory: directory,
        table,
        catalog: CatalogSnapshot::new([
            DatabaseEntry::new(DatabaseId::new(1), "app", [entry]).expect("database")
        ])
        .expect("catalog"),
    }
}

fn run(fixture: &Fixture, sql: &str) -> (usize, f64, String) {
    let snapshot = fixture.table.snapshot();
    let provider = SnapshotScanProvider::new([(DatabaseId::new(1), TableId::new(1), &snapshot)])
        .expect("provider");
    let started = std::time::Instant::now();
    let bound = Binder::new(&fixture.catalog, Some("app"))
        .bind(&parse_statement(sql).expect("parse"))
        .expect("bind");
    let plan = PhysicalPlanner::plan(
        Optimizer::optimize(LogicalPlanner::plan(bound)),
        Collation::default(),
    )
    .expect("plan");
    let mut execution =
        Execution::start_profiled(plan, &provider, 8 << 30, None, Collation::default())
            .expect("execution");
    let mut rows = 0;
    while let Some(batch) = execution.next_batch().expect("batch") {
        rows += batch.selection().selected_rows().count();
    }
    let elapsed = started.elapsed().as_secs_f64() * 1000.0;
    let notes = execution
        .profile()
        .map(|profile| {
            profile
                .operators
                .iter()
                .filter_map(|operator| operator.note.clone())
                .collect::<Vec<_>>()
                .join("; ")
        })
        .unwrap_or_default();
    (rows, elapsed, notes)
}

const AGGREGATES: &[&str] = &[
    "COUNT(*)",
    "COUNT(qty)",
    "SUM(qty)",
    "AVG(qty)",
    "MIN(qty)",
    "MAX(qty)",
    "BIT_AND(qty)",
    "BIT_OR(qty)",
    "BIT_XOR(qty)",
    "STDDEV(qty)",
    "VARIANCE(qty)",
    "COUNT(DISTINCT qty)",
    "SUM(price)",
    "AVG(price)",
    "MIN(price)",
    "MAX(price)",
    "STDDEV(price)",
    "COUNT(DISTINCT price)",
    "SUM(ratio)",
    "AVG(ratio)",
    "MIN(ratio)",
    "MAX(ratio)",
    "STDDEV(ratio)",
    "MIN(ts)",
    "MAX(ts)",
    "COUNT(DISTINCT ts)",
    "COUNT(tag)",
    "MIN(tag)",
    "MAX(tag)",
    "COUNT(DISTINCT tag)",
    "GROUP_CONCAT(DISTINCT tag)",
    "MIN(label)",
    "MAX(label)",
    "COUNT(DISTINCT label)",
];

const SHAPES: &[(&str, &str)] = &[
    ("ungrouped", ""),
    ("few-text", " GROUP BY region"),
    ("many-int", " GROUP BY grp"),
    ("date", " GROUP BY DATE(ts)"),
];

#[test]
#[ignore = "survey: prints the path and cost of every aggregate shape"]
fn aggregate_path_survey() {
    let rows: u64 = std::env::var("SURVEY_ROWS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(2_000_000);
    let runs: usize = std::env::var("SURVEY_RUNS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(3);
    let only = std::env::var("SURVEY_ONLY").ok();
    let built = std::time::Instant::now();
    let fixture = fixture(rows);
    println!(
        "# {rows} rows built in {:.1}s",
        built.elapsed().as_secs_f64()
    );
    println!("shape\taggregate\tmedian_ms\tmin_ms\tns_per_row\tgroups\tpath");
    for (shape, suffix) in SHAPES {
        for aggregate in AGGREGATES {
            let key = match *shape {
                "few-text" => "region, ",
                "many-int" => "grp, ",
                "date" => "DATE(ts), ",
                _ => "",
            };
            let sql = format!("SELECT {key}{aggregate} FROM ledger{suffix}");
            if only.as_deref().is_some_and(|only| !sql.contains(only)) {
                continue;
            }
            let mut times = Vec::new();
            let mut last = (0, String::new());
            for _ in 0..runs {
                let (groups, elapsed, notes) = run(&fixture, &sql);
                times.push(elapsed);
                last = (groups, notes);
            }
            times.sort_by(f64::total_cmp);
            let median = times[times.len() / 2];
            #[allow(clippy::cast_precision_loss)]
            let per_row = median * 1e6 / rows as f64;
            println!(
                "{shape}\t{aggregate}\t{median:.1}\t{:.1}\t{per_row:.1}\t{}\t{}",
                times[0], last.0, last.1
            );
        }
    }
}
