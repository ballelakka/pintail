//! Packed aggregate lanes folded a column at a time - over an integer key's
//! range, and over dense text and text-pair slots - agree with the general
//! path, which an expression key forces.
//!
//! The table is built so one query crosses every transition the range fold
//! has: a first window of narrow keys, later windows that widen the range
//! (the fold re-bases), a stretch of keys too sparse for any array (the fold
//! hands its groups to the partition maps mid-stream), and a return to the
//! first keys. NULL keys, NULL amounts and a group whose amounts are all
//! NULL ride along throughout, and a filter makes the folds read selected
//! rows rather than whole spans.

use pintail_catalog::{
    CatalogSnapshot, DatabaseEntry, DatabaseId, TableEntry, TableId, TableStatistics,
};
use pintail_exec::collation::Collation;
use pintail_exec::{Execution, LogicalPlanner, Optimizer, PhysicalPlanner, SnapshotScanProvider};
use pintail_sql::{Binder, parse_statement};
use pintail_store::{StoreOptions, TableStore};
use pintail_types::{Column, DataType, KeyPart, PrimaryKey, StoredRow, TableSchema, Value};

const SHELVES: [&str; 6] = ["oak", "pine", "birch", "ash", "elm", "fir"];
const AISLES: [&str; 3] = ["north", "south", "east"];

/// The key every row of one stretch carries; `None` is NULL.
fn key_of(id: u64) -> Option<i64> {
    let id_signed = i64::try_from(id).expect("small id");
    if id.is_multiple_of(29) {
        return None;
    }
    Some(match id {
        // Wider on both sides: the range re-bases.
        200_000..400_000 => id_signed % 9_000 - 4_500,
        // Too sparse for an array of any useful width.
        400_000..480_000 => (id_signed * 7_919) % 1_000_000_007,
        // Narrow, 600 keys around zero: first in the range fold, and after
        // the sparse stretch in the partition maps.
        _ => id_signed % 600 - 300,
    })
}

struct Fixture {
    _directory: tempfile::TempDir,
    table: TableStore,
    catalog: CatalogSnapshot,
}

fn fixture(key_type: DataType) -> Fixture {
    let schema = TableSchema::new(
        1,
        vec![
            Column::new(1, "id", DataType::UInt64, false),
            Column::new(2, "owner", key_type, true),
            Column::new(
                3,
                "amount",
                DataType::Decimal {
                    precision: 12,
                    scale: 2,
                },
                true,
            ),
            Column::new(4, "shelf", DataType::Utf8, true),
            Column::new(5, "aisle", DataType::Utf8, true),
        ],
    )
    .expect("schema");
    let directory = tempfile::tempdir().expect("directory");
    let mut table =
        TableStore::open(directory.path(), schema.clone(), StoreOptions::default()).expect("table");
    let rows = 560_000_u64;
    let mut start = 0;
    while start < rows {
        let end = (start + 40_000).min(rows);
        table
            .bulk_ingest_snapshot(
                (start..end)
                    .map(|id| {
                        let key = key_of(id);
                        let owner = match (key, key_type) {
                            (None, _) => Value::Null,
                            // Unsigned keys sit above an offset, so they
                            // stay positive and the range does not start
                            // at zero.
                            (Some(key), DataType::UInt64) => Value::UInt64(
                                u64::try_from(key + 2_000_000_000)
                                    .expect("offset keeps it positive"),
                            ),
                            (Some(key), _) => Value::Int64(key),
                        };
                        // Key 17 never has an amount, so its SUM, AVG, MIN
                        // and MAX stay NULL while its COUNT does not.
                        let amount = if id.is_multiple_of(13) || key == Some(17) {
                            Value::Null
                        } else {
                            let cents =
                                i64::try_from((id * 7_919) % 2_000_000).expect("small") - 400_000;
                            Value::Utf8(format!(
                                "{}{}.{:02}",
                                if cents < 0 { "-" } else { "" },
                                cents.abs() / 100,
                                cents.abs() % 100
                            ))
                        };
                        let shelf = if id.is_multiple_of(31) {
                            Value::Null
                        } else {
                            Value::Utf8(SHELVES[usize::try_from(id % 6).expect("small")].to_owned())
                        };
                        let aisle = if id.is_multiple_of(37) {
                            Value::Null
                        } else {
                            Value::Utf8(AISLES[usize::try_from(id % 3).expect("small")].to_owned())
                        };
                        StoredRow::new(
                            PrimaryKey::new(vec![KeyPart::UInt64(id)]).expect("key"),
                            vec![Value::UInt64(id), owner, amount, shelf, aisle],
                            1,
                            false,
                        )
                    })
                    .collect(),
            )
            .expect("ingest");
        start = end;
    }
    let entry = TableEntry::new(
        TableId::new(1),
        "stock",
        schema,
        TableStatistics::with_row_count(rows),
    )
    .expect("entry");
    let catalog = CatalogSnapshot::new([
        DatabaseEntry::new(DatabaseId::new(1), "test", [entry]).expect("database")
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
    let bound = Binder::new(&fixture.catalog, Some("test"))
        .bind(&parse_statement(sql).expect("parse"))
        .expect("bind");
    let plan = PhysicalPlanner::plan(
        Optimizer::optimize(LogicalPlanner::plan(bound)),
        Collation::default(),
    )
    .expect("plan");
    // A modest ceiling keeps the scatter windows small, so the stream
    // crosses many of them and every transition happens between two.
    let mut execution =
        Execution::start(plan, &provider, 96 << 20, Collation::default()).expect("execution");
    let mut rows = Vec::new();
    while let Some(batch) = execution.next_batch().expect("batch") {
        for row in batch.selection().selected_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|column| column.value(row).expect("value").clone())
                    .collect::<Vec<_>>(),
            );
        }
    }
    rows
}

const LANES: &str = "COUNT(*), SUM(amount), AVG(amount), MIN(amount), MAX(amount)";

fn assert_same(fixture: &Fixture, direct: &str, general: &str) {
    let left = run(fixture, direct);
    let right = run(fixture, general);
    assert!(!left.is_empty(), "{direct} returned no groups");
    assert_eq!(left.len(), right.len(), "{direct}");
    for (left, right) in left.iter().zip(&right) {
        assert_eq!(left, right, "{direct}");
    }
}

fn integer_range_matches_general(key_type: DataType) {
    let fixture = fixture(key_type);
    for filter in [
        // Every transition: narrow, re-based, sparse, revisited.
        "",
        // Only the range fold: finished straight from its slots.
        "WHERE id < 400000",
        // Selected rows rather than spans.
        "WHERE id % 3 <> 1",
    ] {
        assert_same(
            &fixture,
            &format!("SELECT owner AS k, {LANES} FROM stock {filter} GROUP BY k ORDER BY k"),
            &format!("SELECT owner + 0 AS k, {LANES} FROM stock {filter} GROUP BY k ORDER BY k"),
        );
    }
    // The all-NULL group keeps NULL totals and its row count.
    let key = match key_type {
        DataType::UInt64 => "2000000017",
        _ => "17",
    };
    let rows = run(
        &fixture,
        &format!(
            "SELECT owner, {LANES} FROM stock WHERE id < 400000 GROUP BY owner \
             HAVING owner = {key}"
        ),
    );
    assert_eq!(rows.len(), 1);
    assert!(
        matches!(rows[0][1], Value::Int64(count) if count > 0)
            || matches!(rows[0][1], Value::UInt64(count) if count > 0)
    );
    assert!(
        rows[0][2..]
            .iter()
            .all(|value| matches!(value, Value::Null))
    );
}

#[test]
fn signed_integer_range_fold_matches_general() {
    integer_range_matches_general(DataType::Int64);
}

#[test]
fn unsigned_integer_range_fold_matches_general() {
    integer_range_matches_general(DataType::UInt64);
}

#[test]
fn dense_text_column_folds_match_general() {
    let fixture = fixture(DataType::Int64);
    for filter in ["", "WHERE id % 3 <> 1"] {
        assert_same(
            &fixture,
            &format!("SELECT shelf AS k, {LANES} FROM stock {filter} GROUP BY k ORDER BY k"),
            &format!(
                "SELECT CONCAT(shelf, '') AS k, {LANES} FROM stock {filter} GROUP BY k ORDER BY k"
            ),
        );
        assert_same(
            &fixture,
            &format!(
                "SELECT shelf AS a, aisle AS b, {LANES} FROM stock {filter} \
                 GROUP BY a, b ORDER BY a, b"
            ),
            &format!(
                "SELECT CONCAT(shelf, '') AS a, CONCAT(aisle, '') AS b, {LANES} FROM stock \
                 {filter} GROUP BY a, b ORDER BY a, b"
            ),
        );
    }
}
