//! Conditional counts over date windows measured from `NOW()`: every row
//! compares a datetime against a window edge that is the same for the
//! whole statement.
//!
//! The measurement is `#[ignore]`d:
//! `cargo test --profile recovery -p pintail-exec --test integration conditional_count_windows::
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
            Column::new(2, "state", DataType::Utf8, true)
                .with_collation(Some("utf8mb4_0900_ai_ci".to_owned())),
            Column::new(3, "stage", DataType::Utf8, true)
                .with_collation(Some("utf8mb4_0900_ai_ci".to_owned())),
            Column::new(4, "score", DataType::Int64, true),
            Column::new(5, "sent_at", DataType::DateTime64 { fsp: 0 }, true),
            Column::new(6, "resent_at", DataType::DateTime64 { fsp: 0 }, true),
        ],
    )
    .expect("schema")
}

type Row = (
    Option<&'static str>,
    Option<&'static str>,
    Option<i64>,
    Option<String>,
    Option<String>,
);

struct Fixture {
    _directory: tempfile::TempDir,
    table: TableStore,
    catalog: CatalogSnapshot,
}

fn text(value: Option<&str>) -> Value {
    value.map_or(Value::Null, |value| Value::Utf8(value.to_owned()))
}

impl Fixture {
    fn new(rows: impl IntoIterator<Item = Row>) -> Self {
        let directory = tempfile::tempdir().expect("directory");
        let mut table =
            TableStore::open(directory.path(), schema(), StoreOptions::default()).expect("table");
        let stored = rows
            .into_iter()
            .zip(0_u64..)
            .map(|((state, stage, score, sent, resent), id)| {
                StoredRow::new(
                    PrimaryKey::new(vec![KeyPart::UInt64(id)]).expect("key"),
                    vec![
                        Value::UInt64(id),
                        text(state),
                        text(stage),
                        score.map_or(Value::Null, Value::Int64),
                        text(sent.as_deref()),
                        text(resent.as_deref()),
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
            "items",
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

    fn run(&self, sql: &str) -> (Vec<String>, f64) {
        let snapshot = self.table.snapshot();
        let provider =
            SnapshotScanProvider::new([(DatabaseId::new(1), TableId::new(1), &snapshot)])
                .expect("provider");
        let bound = Binder::new(&self.catalog, Some("app"))
            .bind(&parse_statement(sql).expect("parse"))
            .unwrap_or_else(|error| panic!("bind {sql}: {error}"));
        let physical = PhysicalPlanner::plan(
            Optimizer::optimize(LogicalPlanner::plan(bound)),
            Collation::default(),
        )
        .expect("plan");
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

const STATES: [&str; 5] = ["open", "waiting", "done", "returned", "closed"];

fn stamp(day: u32, second: u32) -> String {
    let date = chrono::NaiveDate::from_ymd_opt(2026, 1, 1).expect("date")
        + chrono::Duration::days(i64::from(day % 270));
    format!(
        "{} {:02}:{:02}:{:02}",
        date,
        second / 3600 % 24,
        second / 60 % 60,
        second % 60
    )
}

fn bulk() -> Fixture {
    Fixture::new((0..200_000_u32).map(|id| {
        let index = usize::try_from(id).expect("id");
        (
            Some(STATES[index % STATES.len()]),
            Some(if id % 13 == 0 { "failed" } else { "ok" }),
            Some(i64::from(id % 11) - 1),
            Some(stamp(id / 700, id * 37)),
            (id % 4 == 0).then(|| stamp(id / 600, id * 53)),
        )
    }))
}

#[test]
#[ignore = "measurement, not an assertion"]
fn conditional_window_count_cost() {
    let fixture = bulk();
    let window = |low: u32, high: u32| {
        format!(
            "COUNT(CASE WHEN state = 'waiting' \
             AND COALESCE(resent_at, sent_at) <= DATE_SUB(NOW(), INTERVAL {low} DAY) \
             AND COALESCE(resent_at, sent_at) > DATE_SUB(NOW(), INTERVAL {high} DAY) THEN 1 END)"
        )
    };
    let queries = [
        "SELECT COUNT(CASE WHEN state = 'waiting' THEN 1 END) FROM items".to_owned(),
        "SELECT COUNT(CASE WHEN sent_at <= '2026-05-01 00:00:00' THEN 1 END) FROM items".to_owned(),
        "SELECT COUNT(CASE WHEN state = 'waiting' AND sent_at <= '2026-05-01 00:00:00' THEN 1 END) FROM items".to_owned(),
        "SELECT COUNT(CASE WHEN sent_at <= '2026-05-01 00:00:00' AND sent_at > '2026-02-01 00:00:00' THEN 1 END) FROM items".to_owned(),
        "SELECT COUNT(CASE WHEN COALESCE(resent_at, sent_at) <= '2026-05-01 00:00:00' AND COALESCE(resent_at, sent_at) > '2026-02-01 00:00:00' THEN 1 END) FROM items".to_owned(),
        "SELECT COUNT(CASE WHEN state = 'waiting' AND COALESCE(resent_at, sent_at) <= '2026-05-01 00:00:00' AND COALESCE(resent_at, sent_at) > '2026-02-01 00:00:00' THEN 1 END) FROM items".to_owned(),
        "SELECT COUNT(CASE WHEN sent_at <= DATE_SUB(NOW(), INTERVAL 7 DAY) THEN 1 END) FROM items".to_owned(),
        "SELECT COUNT(CASE WHEN COALESCE(resent_at, sent_at) <= '2026-05-01 00:00:00' THEN 1 END) FROM items".to_owned(),
        format!("SELECT {} FROM items", window(2, 7)),
        format!(
            "SELECT {}, {}, {}, {}, \
             COUNT(CASE WHEN state IN ('waiting', 'done') AND stage = 'failed' THEN 1 END), \
             COUNT(CASE WHEN state = 'done' AND score <= 0 THEN 1 END) FROM items",
            window(0, 2),
            window(2, 7),
            window(7, 30),
            window(30, 90)
        ),
    ];
    let joined = |select: &str| {
        format!(
            "SELECT {select} FROM items a JOIN items b ON b.id = a.id \
             LEFT JOIN (SELECT id, MAX(resent_at) AS last_at FROM items GROUP BY id) x ON x.id = a.id"
        )
    };
    let joined_window = |low: u32, high: u32| {
        format!(
            "COUNT(CASE WHEN a.state = 'waiting' \
             AND COALESCE(x.last_at, a.sent_at) <= DATE_SUB(NOW(), INTERVAL {low} DAY) \
             AND COALESCE(x.last_at, a.sent_at) > DATE_SUB(NOW(), INTERVAL {high} DAY) THEN 1 END)"
        )
    };
    let queries = queries
        .into_iter()
        .chain([
            joined("COUNT(*)"),
            joined("COUNT(CASE WHEN x.last_at <= '2026-05-01 00:00:00' THEN 1 END)"),
            joined("COUNT(CASE WHEN COALESCE(a.resent_at, a.sent_at) <= '2026-05-01 00:00:00' THEN 1 END)"),
            joined("COUNT(CASE WHEN COALESCE(x.last_at, a.sent_at) <= '2026-05-01 00:00:00' THEN 1 END)"),
            joined("COUNT(CASE WHEN COALESCE(x.last_at, a.sent_at) <= DATE_SUB(NOW(), INTERVAL 2 DAY) THEN 1 END)"),
            joined("COUNT(CASE WHEN a.state = 'waiting' THEN 1 END)"),
            joined("COUNT(CASE WHEN a.sent_at <= DATE_SUB(NOW(), INTERVAL 7 DAY) THEN 1 END)"),
            joined(&joined_window(2, 7)),
            joined(&[joined_window(0, 2), joined_window(2, 7), joined_window(7, 30), joined_window(30, 90)].join(", ")),
        ])
        .collect::<Vec<_>>();
    for sql in &queries {
        let _ = pintail_exec::take_exec_counters();
        let mut timings = (0..3).map(|_| fixture.run(sql).1).collect::<Vec<_>>();
        let counters = pintail_exec::take_exec_counters();
        timings.sort_by(f64::total_cmp);
        println!(
            "median {:>9.1} ms  min {:>9.1} ms  scalar {:>8} {}",
            timings[1],
            timings[0],
            counters.rows_projected_scalar,
            &sql[..sql.len().min(160)]
        );
    }
}
