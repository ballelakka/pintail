//! An aggregate with no GROUP BY folds each column a batch at a time; the
//! general grouped path, which a constant expression key forces, folds the
//! same rows one at a time. Both answers must agree exactly: counts, exact
//! decimal and integer totals, decimal averages, and the MIN/MAX of
//! decimal, temporal and integer columns - over NULLs, over a filter that
//! leaves scattered rows, over segments and memtable rows together, and
//! over a window that selects nothing.

use pintail_catalog::{
    CatalogSnapshot, DatabaseEntry, DatabaseId, TableEntry, TableId, TableStatistics,
};
use pintail_exec::collation::Collation;
use pintail_exec::{Execution, LogicalPlanner, Optimizer, PhysicalPlanner, SnapshotScanProvider};
use pintail_sql::{Binder, parse_statement};
use pintail_store::{StoreOptions, TableStore};
use pintail_types::{Column, DataType, KeyPart, PrimaryKey, StoredRow, TableSchema, Value};

const ROWS: u64 = 120_000;

fn schema() -> TableSchema {
    TableSchema::new(
        1,
        vec![
            Column::new(1, "id", DataType::UInt64, false),
            Column::new(
                2,
                "fee",
                DataType::Decimal {
                    precision: 12,
                    scale: 2,
                },
                true,
            ),
            Column::new(3, "seen_at", DataType::DateTime64 { fsp: 0 }, true),
            Column::new(4, "seen_on", DataType::Date32, true),
            Column::new(5, "delta", DataType::Int64, true),
            Column::new(6, "units", DataType::UInt32, true),
            Column::new(7, "tag", DataType::Utf8, true),
        ],
    )
    .expect("schema")
}

fn row(id: u64, version: u64) -> StoredRow {
    let signed = i64::try_from(id).expect("small");
    let fee = if id.is_multiple_of(13) {
        Value::Null
    } else {
        let cents = (signed * 7_919) % 2_000_000 - 700_000;
        Value::Utf8(format!(
            "{}{}.{:02}",
            if cents < 0 { "-" } else { "" },
            cents.abs() / 100,
            cents.abs() % 100
        ))
    };
    // Repeating timestamps, so a MIN or MAX is held by several rows.
    let seen_at = if id.is_multiple_of(17) {
        Value::Null
    } else {
        let minute = (id * 37) % 5_000;
        Value::Utf8(format!(
            "2025-0{}-{:02} {:02}:{:02}:00",
            1 + minute / 1_000,
            1 + (minute / 40) % 28,
            (minute / 60) % 24,
            minute % 60
        ))
    };
    let seen_on = if id.is_multiple_of(19) {
        Value::Null
    } else {
        Value::Utf8(format!("2024-{:02}-{:02}", 1 + id % 12, 1 + id % 28))
    };
    let delta = if id.is_multiple_of(23) {
        Value::Null
    } else {
        Value::Int64((signed * 104_729) % 1_000_003 - 500_000)
    };
    let units = if id.is_multiple_of(29) {
        Value::Null
    } else {
        Value::UInt64((id * 31) % 70_000)
    };
    let tag = if id.is_multiple_of(7) {
        Value::Null
    } else {
        Value::Utf8(format!("t{}", id % 11))
    };
    StoredRow::new(
        PrimaryKey::new(vec![KeyPart::UInt64(id)]).expect("key"),
        vec![Value::UInt64(id), fee, seen_at, seen_on, delta, units, tag],
        version,
        false,
    )
}

struct Fixture {
    _directory: tempfile::TempDir,
    table: TableStore,
    catalog: CatalogSnapshot,
}

fn fixture() -> Fixture {
    let directory = tempfile::tempdir().expect("directory");
    let options = StoreOptions {
        background_compaction: false,
        ..StoreOptions::default()
    };
    let mut table = TableStore::open(directory.path(), schema(), options).expect("table");
    let mut start = 0;
    while start < ROWS {
        let end = (start + 30_000).min(ROWS);
        table
            .bulk_ingest_snapshot((start..end).map(|id| row(id, 1)).collect())
            .expect("ingest");
        start = end;
    }
    // Memtable rows: new keys past the segments, and rewrites of keys
    // inside them (a NULL-heavy rewrite and a deletion among them).
    let mut writes: Vec<StoredRow> = (ROWS..ROWS + 3_000).map(|id| row(id, 2)).collect();
    writes.extend((0..400_u64).map(|k| row(k * 211, 2)));
    writes.push(StoredRow::new(
        PrimaryKey::new(vec![KeyPart::UInt64(5)]).expect("key"),
        vec![
            Value::UInt64(5),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
        ],
        3,
        true,
    ));
    table.ingest(writes).expect("memtable writes");
    let entry = TableEntry::new(
        TableId::new(1),
        "visits",
        schema(),
        TableStatistics::with_row_count(ROWS),
    )
    .expect("entry")
    .with_key_columns([1])
    .expect("key");
    let catalog = CatalogSnapshot::new([
        DatabaseEntry::new(DatabaseId::new(1), "app", [entry]).expect("database")
    ])
    .expect("catalog");
    Fixture {
        _directory: directory,
        table,
        catalog,
    }
}

fn run(fixture: &Fixture, sql: &str) -> Vec<Vec<Value>> {
    let snapshot = fixture.table.snapshot();
    let provider = SnapshotScanProvider::new([(DatabaseId::new(1), TableId::new(1), &snapshot)])
        .expect("provider");
    let bound = Binder::new(&fixture.catalog, Some("app"))
        .bind(&parse_statement(sql).expect("parse"))
        .expect("bind");
    let plan = PhysicalPlanner::plan(
        Optimizer::optimize(LogicalPlanner::plan(bound)),
        Collation::default(),
    )
    .expect("plan");
    let mut execution =
        Execution::start(plan, &provider, 256 << 20, Collation::default()).expect("execution");
    let mut rows = Vec::new();
    while let Some(batch) = execution.next_batch().expect("batch") {
        for row in batch.selection().selected_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|column| column.value_owned(row).expect("value"))
                    .collect::<Vec<_>>(),
            );
        }
    }
    rows
}

const AGGREGATES: &str = "COUNT(*), COUNT(fee), SUM(fee), AVG(fee), MIN(fee), MAX(fee), \
     COUNT(seen_at), MIN(seen_at), MAX(seen_at), MIN(seen_on), MAX(seen_on), \
     SUM(delta), MIN(delta), MAX(delta), SUM(units), MIN(units), MAX(units), \
     AVG(delta), COUNT(tag), MIN(tag), MAX(tag)";

#[test]
fn ungrouped_column_fold_matches_the_row_fold() {
    let fixture = fixture();
    for filter in [
        "",
        // Scattered rows: every batch is a picked selection.
        "WHERE id % 3 <> 1",
        // A range: mostly whole spans, and the memtable rows.
        "WHERE id >= 100000",
        "WHERE seen_at BETWEEN '2025-02-01 00:00:00' AND '2025-03-15 12:00:00'",
        // Nothing selected: COUNT is zero and everything else NULL.
        "WHERE id > 99999999",
    ] {
        let folded = run(
            &fixture,
            &format!("SELECT {AGGREGATES} FROM visits {filter}"),
        );
        // A constant expression key: one group, folded row by row.
        let general = run(
            &fixture,
            &format!("SELECT {AGGREGATES} FROM visits {filter} GROUP BY id * 0"),
        );
        assert_eq!(folded.len(), 1, "{filter}");
        if general.is_empty() {
            // No rows: the grouped form has no group at all.
            assert_eq!(folded[0][0], Value::UInt64(0), "{filter}");
            assert!(
                folded[0][2..]
                    .iter()
                    .all(|value| matches!(value, Value::Null | Value::UInt64(0))),
                "{filter}: {:?}",
                folded[0]
            );
            continue;
        }
        assert_eq!(folded, general, "{filter}");
    }
}
