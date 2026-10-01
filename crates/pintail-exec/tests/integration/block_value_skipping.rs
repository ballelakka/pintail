//! A range filter on a column that rises with insertion order - an event
//! time, an increasing counter - selects a few blocks of a large segment.
//! Each block stores its column's least and greatest value, so the scan
//! skips the blocks that cannot hold a match instead of decoding them all.
//! These cases hold the answers to a direct computation over the same rows,
//! at block boundaries, through NULL-only blocks, with rows still in the
//! memtable and after a column is added, and check the skipping happened.
use std::collections::BTreeMap;

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

/// Rows of the base segment: twelve blocks and a short one at the default
/// block size.
const ROWS: u64 = 200_000;
const BLOCK: u64 = 16 * 1024;
/// 2024-01-01 00:00:00 UTC.
const EPOCH: i64 = 1_704_067_200;

fn schema(with_late: bool) -> TableSchema {
    let mut columns = vec![
        Column::new(1, "id", DataType::UInt64, false),
        Column::new(2, "seen", DataType::DateTime64 { fsp: 0 }, true),
        Column::new(3, "score", DataType::Int64, true),
        Column::new(4, "day", DataType::Date32, true),
        Column::new(5, "seq", DataType::UInt64, true),
    ];
    if with_late {
        columns.push(Column::new(6, "late", DataType::Int64, true));
    }
    TableSchema::new(if with_late { 2 } else { 1 }, columns).expect("schema")
}

/// Civil date from days since 1970-01-01.
fn civil(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

fn datetime(seconds: i64) -> String {
    let (year, month, day) = civil(seconds.div_euclid(86_400));
    let within = seconds.rem_euclid(86_400);
    format!(
        "{year:04}-{month:02}-{day:02} {:02}:{:02}:{:02}",
        within / 3600,
        within / 60 % 60,
        within % 60
    )
}

fn date(seconds: i64) -> String {
    datetime(seconds)[..10].to_owned()
}

/// One logical row: event time in epoch seconds, a score that does not
/// follow insertion order, the event's day, a counter and the late column.
#[derive(Clone, Copy, Debug)]
struct Model {
    seen: Option<i64>,
    score: Option<i64>,
    seq: Option<u64>,
    late: Option<i64>,
}

impl Model {
    fn base(id: u64) -> Self {
        let signed = i64::try_from(id).expect("small");
        // A whole block of NULL event times, and scattered NULLs elsewhere.
        let null_seen = (3 * BLOCK..4 * BLOCK).contains(&id) || id.is_multiple_of(97);
        Self {
            seen: (!null_seen).then_some(EPOCH + signed * 60),
            score: (!id.is_multiple_of(113)).then_some((signed * 7_919) % 1_000 - 500),
            seq: (!id.is_multiple_of(89)).then_some(id * 3),
            late: None,
        }
    }

    fn day(self) -> Option<i64> {
        self.seen.map(|seconds| seconds.div_euclid(86_400))
    }

    fn stored(self, id: u64, version: u64, with_late: bool) -> StoredRow {
        let mut values = vec![
            Value::UInt64(id),
            self.seen
                .map_or(Value::Null, |seconds| Value::Utf8(datetime(seconds))),
            self.score.map_or(Value::Null, Value::Int64),
            self.seen
                .map_or(Value::Null, |seconds| Value::Utf8(date(seconds))),
            self.seq.map_or(Value::Null, Value::UInt64),
        ];
        if with_late {
            values.push(self.late.map_or(Value::Null, Value::Int64));
        }
        StoredRow::new(
            PrimaryKey::new(vec![KeyPart::UInt64(id)]).expect("key"),
            values,
            version,
            false,
        )
    }
}

fn tombstone(id: u64, version: u64, with_late: bool) -> StoredRow {
    let width = if with_late { 6 } else { 5 };
    let mut values = vec![Value::Null; width];
    values[0] = Value::UInt64(id);
    StoredRow::new(
        PrimaryKey::new(vec![KeyPart::UInt64(id)]).expect("key"),
        values,
        version,
        true,
    )
}

struct Fixture {
    _directory: tempfile::TempDir,
    table: TableStore,
    catalog: CatalogSnapshot,
    model: BTreeMap<u64, Model>,
    with_late: bool,
}

impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().expect("directory");
        let mut table = TableStore::open(
            directory.path(),
            schema(false),
            StoreOptions {
                background_compaction: false,
                ..StoreOptions::default()
            },
        )
        .expect("table");
        table
            .bulk_ingest_snapshot(
                (0..ROWS)
                    .map(|id| Model::base(id).stored(id, 1, false))
                    .collect(),
            )
            .expect("ingest");
        let model = (0..ROWS).map(|id| (id, Model::base(id))).collect();
        Self {
            _directory: directory,
            table,
            catalog: catalog(false),
            model,
            with_late: false,
        }
    }

    /// Writes that stay in the memtable: rows of skipped blocks moved into
    /// the windows the queries ask for, rows of matching blocks moved out
    /// or deleted, and new keys past the segment.
    fn write_memtable(&mut self) {
        let mut writes = Vec::new();
        let mut put = |model: &mut BTreeMap<u64, Model>, id: u64, row: Model| {
            writes.push(row.stored(id, 2, self.with_late));
            model.insert(id, row);
        };
        for (index, id) in [5_u64, 20_000, 3 * BLOCK + 7, 150_001]
            .into_iter()
            .enumerate()
        {
            let mut row = self.model[&id];
            row.seen = Some(EPOCH + 100_000 * 60 + i64::try_from(index).expect("small"));
            row.seq = Some(300_000);
            put(&mut self.model, id, row);
        }
        for id in [100_001_u64, 100_002] {
            let mut row = self.model[&id];
            row.seen = Some(EPOCH);
            put(&mut self.model, id, row);
        }
        put(
            &mut self.model,
            ROWS + 10,
            Model {
                seen: Some(EPOCH + 100_000 * 60 + 30),
                score: Some(7),
                seq: Some(1),
                // Written before the column exists.
                late: None,
            },
        );
        for id in [100_005_u64, 100_006] {
            writes.push(tombstone(id, 2, self.with_late));
            self.model.remove(&id);
        }
        self.table.ingest(writes).expect("memtable writes");
    }

    /// Adds a column: the base segment predates it, the rows written next
    /// carry it.
    fn add_late_column(&mut self) {
        self.table.evolve_schema(schema(true)).expect("evolve");
        self.with_late = true;
        self.catalog = catalog(true);
        let mut writes = Vec::new();
        for id in (ROWS + 100..ROWS + 40_000).step_by(3) {
            let signed = i64::try_from(id).expect("small");
            let row = Model {
                seen: Some(EPOCH + signed * 60),
                score: Some(signed % 50),
                seq: Some(id * 3),
                late: Some(signed % 11),
            };
            writes.push(row.stored(id, 3, true));
            self.model.insert(id, row);
        }
        self.table.ingest(writes).expect("late writes");
        self.table.flush().expect("flush");
    }

    fn run(&self, sql: &str) -> (Vec<Vec<Value>>, PhysicalScanStats) {
        let snapshot = self.table.snapshot();
        let provider =
            SnapshotScanProvider::new([(DatabaseId::new(1), TableId::new(1), &snapshot)])
                .expect("provider");
        let bound = Binder::new(&self.catalog, Some("app"))
            .bind(&parse_statement(sql).unwrap_or_else(|error| panic!("{sql}: {error}")))
            .unwrap_or_else(|error| panic!("{sql}: {error}"));
        let physical = PhysicalPlanner::plan(
            Optimizer::optimize(LogicalPlanner::plan(bound)),
            Collation::default(),
        )
        .expect("plan");
        let mut execution =
            Execution::start(physical, &provider, 1 << 30, Collation::default()).expect("start");
        let mut rows = Vec::new();
        while let Some(batch) = execution
            .next_batch()
            .unwrap_or_else(|error| panic!("{sql}: {error}"))
        {
            for row in batch.selection().selected_rows() {
                rows.push(
                    batch
                        .columns()
                        .iter()
                        .map(|column| column.value(row).cloned().unwrap_or(Value::Null))
                        .collect::<Vec<_>>(),
                );
            }
        }
        let stats = provider
            .scan_stats(DatabaseId::new(1), TableId::new(1))
            .unwrap_or_default();
        (rows, stats)
    }
}

fn catalog(with_late: bool) -> CatalogSnapshot {
    let entry = TableEntry::new(
        TableId::new(1),
        "events",
        schema(with_late),
        TableStatistics::with_row_count(ROWS),
    )
    .expect("entry")
    .with_key_columns([1])
    .expect("key");
    CatalogSnapshot::new(
        [DatabaseEntry::new(DatabaseId::new(1), "app", [entry]).expect("database")],
    )
    .expect("catalog")
}

type Filter = Box<dyn Fn(&Model) -> bool>;

/// The event time of base row `id`.
fn at(id: u64) -> i64 {
    EPOCH + i64::try_from(id).expect("small") * 60
}

/// The filters checked, each as SQL and as the same test over the model.
/// `skips` marks those whose matches sit in a few blocks of the base.
#[allow(clippy::too_many_lines)] // one table of cases
fn cases() -> Vec<(String, Filter, bool)> {
    let boundary = 5 * BLOCK;
    let mut cases: Vec<(String, Filter, bool)> = Vec::new();
    let mut add = |sql: String, filter: Filter, skips: bool| cases.push((sql, filter, skips));
    let (lo, hi) = (at(boundary), at(boundary + 5_000));
    add(
        format!("seen >= '{}' AND seen < '{}'", datetime(lo), datetime(hi)),
        Box::new(move |row| row.seen.is_some_and(|seen| seen >= lo && seen < hi)),
        true,
    );
    let last = at(boundary - 1);
    add(
        format!("seen = '{}'", datetime(last)),
        Box::new(move |row| row.seen == Some(last)),
        true,
    );
    let first = at(boundary);
    add(
        format!(
            "seen BETWEEN '{}' AND '{}'",
            datetime(last),
            datetime(first)
        ),
        Box::new(move |row| row.seen.is_some_and(|seen| seen >= last && seen <= first)),
        true,
    );
    add(
        format!("seen > '{}'", datetime(last)),
        Box::new(move |row| row.seen.is_some_and(|seen| seen > last)),
        true,
    );
    add(
        format!("seen < '{}'", datetime(first)),
        Box::new(move |row| row.seen.is_some_and(|seen| seen < first)),
        true,
    );
    add(
        format!("seen <= '{}'", datetime(first)),
        Box::new(move |row| row.seen.is_some_and(|seen| seen <= first)),
        true,
    );
    // A fraction the column cannot hold, on either side of a whole second.
    add(
        format!("seen > '{}.5'", datetime(last)),
        Box::new(move |row| row.seen.is_some_and(|seen| seen > last)),
        true,
    );
    add(
        format!("seen < '{}.5'", datetime(first)),
        Box::new(move |row| row.seen.is_some_and(|seen| seen <= first)),
        true,
    );
    let (one, two) = (at(1_000), at(190_000));
    add(
        format!("seen IN ('{}', '{}', NULL)", datetime(one), datetime(two)),
        Box::new(move |row| row.seen == Some(one) || row.seen == Some(two)),
        false,
    );
    // Inside the NULL-only block: nothing can match.
    let inside = at(3 * BLOCK + 10);
    add(
        format!("seen = '{}'", datetime(inside)),
        Box::new(move |row| row.seen == Some(inside)),
        true,
    );
    // A bare day against a DATETIME compares as that day's midnight.
    let day = at(100_000).div_euclid(86_400) * 86_400;
    add(
        format!(
            "seen >= '{}' AND seen < '{}'",
            date(day),
            date(day + 86_400)
        ),
        Box::new(move |row| {
            row.seen
                .is_some_and(|seen| seen >= day && seen < day + 86_400)
        }),
        true,
    );
    add(
        format!("seen > '{}'", date(day)),
        Box::new(move |row| row.seen.is_some_and(|seen| seen > day)),
        true,
    );
    let day_number = day / 86_400;
    add(
        format!("day = '{}'", date(day)),
        Box::new(move |row| row.day() == Some(day_number)),
        true,
    );
    add(
        format!(
            "day BETWEEN '{}' AND '{}'",
            date(day),
            date(day + 3 * 86_400)
        ),
        Box::new(move |row| {
            row.day()
                .is_some_and(|d| d >= day_number && d <= day_number + 3)
        }),
        true,
    );
    // An unsigned counter, and a signed column that follows no order.
    add(
        format!("seq >= {} AND seq <= {}", 3 * boundary, 3 * (boundary + 99)),
        Box::new(move |row| {
            row.seq
                .is_some_and(|seq| seq >= 3 * boundary && seq <= 3 * (boundary + 99))
        }),
        true,
    );
    add(
        "seq > 18446744073709551000".to_owned(),
        Box::new(|row| row.seq.is_some_and(|seq| seq > 18_446_744_073_709_551_000)),
        true,
    );
    add(
        format!("score > 400 AND seen < '{}'", datetime(at(20_000))),
        Box::new(move |row| {
            row.score.is_some_and(|score| score > 400)
                && row.seen.is_some_and(|seen| seen < at(20_000))
        }),
        true,
    );
    add(
        "score >= 499".to_owned(),
        Box::new(|row| row.score.is_some_and(|score| score >= 499)),
        false,
    );
    // Shapes that bound nothing and must read everything.
    add(
        format!("seen < '{}' OR score = 3", datetime(at(10))),
        Box::new(move |row| row.seen.is_some_and(|seen| seen < at(10)) || row.score == Some(3)),
        false,
    );
    add(
        format!("NOT (seen >= '{}')", datetime(at(10))),
        Box::new(move |row| row.seen.is_some_and(|seen| seen < at(10))),
        false,
    );
    cases
}

fn expected(fixture: &Fixture, filter: &Filter) -> (Vec<Vec<Value>>, Vec<Vec<Value>>) {
    let matched = fixture
        .model
        .iter()
        .filter(|(_, row)| filter(row))
        .collect::<Vec<_>>();
    let count = Value::Int64(i64::try_from(matched.len()).expect("small"));
    let scores = matched
        .iter()
        .filter_map(|(_, row)| row.score)
        .collect::<Vec<_>>();
    let sum = if scores.is_empty() {
        Value::Null
    } else {
        Value::Int64(scores.iter().sum())
    };
    let ids = matched
        .iter()
        .map(|(id, row)| {
            vec![
                Value::UInt64(**id),
                row.score.map_or(Value::Null, Value::Int64),
            ]
        })
        .collect();
    (vec![vec![count, sum]], ids)
}

fn normalize(rows: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
    rows.into_iter()
        .map(|row| {
            row.into_iter()
                .map(|value| match value {
                    Value::UInt64(count) => Value::Int64(i64::try_from(count).expect("count")),
                    Value::Utf8(text) => {
                        text.parse::<i64>().map_or(Value::Utf8(text), Value::Int64)
                    }
                    other => other,
                })
                .collect()
        })
        .collect()
}

fn check_all(fixture: &Fixture, stage: &str) {
    for (filter_sql, filter, skips) in cases() {
        let (aggregate, ids) = expected(fixture, &filter);
        let sql = format!("SELECT COUNT(*), SUM(score) FROM events WHERE {filter_sql}");
        let (rows, stats) = fixture.run(&sql);
        assert_eq!(normalize(rows), aggregate, "{stage}: {sql}");
        if skips {
            // A bound past every value of a segment prunes it whole instead.
            assert!(
                stats.blocks_value_skipped > 0 || stats.segments_pruned > 0,
                "{stage}: {sql} skipped no block: {stats:?}"
            );
        }
        let sql = format!("SELECT id, score FROM events WHERE {filter_sql} ORDER BY id");
        let (rows, _) = fixture.run(&sql);
        assert_eq!(rows, ids, "{stage}: {sql}");
    }
}

#[test]
fn block_value_skipping_matches_a_direct_computation() {
    let mut fixture = Fixture::new();
    check_all(&fixture, "segments only");
    fixture.write_memtable();
    check_all(&fixture, "with memtable rows");
    fixture.add_late_column();
    check_all(&fixture, "after adding a column");
    // The added column: the base segment predates it, so its rows read as
    // NULL there and fail the bound; only the newer rows can match.
    let (rows, _) = fixture.run("SELECT COUNT(*) FROM events WHERE late >= 9");
    let expected = fixture
        .model
        .values()
        .filter(|row| row.late.is_some_and(|late| late >= 9))
        .count();
    assert_eq!(
        normalize(rows),
        vec![vec![Value::Int64(i64::try_from(expected).expect("small"))]]
    );
}

#[test]
fn a_narrow_window_decodes_one_block() {
    let fixture = Fixture::new();
    let (_, stats) = fixture.run(&format!(
        "SELECT COUNT(*) FROM events WHERE seen >= '{}' AND seen < '{}'",
        datetime(at(5 * BLOCK)),
        datetime(at(5 * BLOCK + 100))
    ));
    // One block can hold the window; the other twelve are skipped.
    assert_eq!(stats.blocks_value_skipped, 12, "{stats:?}");
}
