//! A memoized aggregate must be keyed on every predicate that shaped its
//! input, not only on the ones its scans evaluate.
//!
//! A WHERE conjunct on the null-extended side of a LEFT join cannot run in
//! that side's scan; it stays a Filter above the join. The settled memo
//! used to walk through such a Filter as if its predicate were already in
//! a scan signature, so `e.tier IS NULL` and `COALESCE(e.tier, 0) = 0` over
//! one join shared a key and the second was served the first's answer.
//! Each case here runs after its neighbours have filled the memo, and is
//! compared with the same rows read through a derived table the memo never
//! keys (its LIMIT makes the input no settled plan).

use pintail_catalog::{
    CatalogSnapshot, DatabaseEntry, DatabaseId, TableEntry, TableId, TableStatistics,
};
use pintail_exec::collation::Collation;
use pintail_exec::{Execution, LogicalPlanner, Optimizer, PhysicalPlanner, SnapshotScanProvider};
use pintail_sql::{Binder, parse_statement};
use pintail_store::{StoreOptions, TableStore};
use pintail_types::{Column, DataType, KeyPart, PrimaryKey, StoredRow, TableSchema, Value};

const DATABASE: DatabaseId = DatabaseId::new(4);

fn stored(id: u64, values: Vec<Value>) -> StoredRow {
    StoredRow::new(
        PrimaryKey::new(vec![KeyPart::UInt64(id)]).expect("key"),
        values,
        id,
        false,
    )
}

struct Fixture {
    _directories: Vec<tempfile::TempDir>,
    tables: Vec<(TableId, TableStore)>,
    catalog: CatalogSnapshot,
}

/// `items(id, bucket)` each with at most one `extras(id, item, tier)`: a
/// third of the items have none, and the extras' tiers cycle through NULL,
/// 0, 1 and 2, so every predicate below selects a different row set.
fn fixture() -> Fixture {
    let items = 120_u64;
    let schemas = [
        (
            "items",
            vec![
                Column::new(1, "id", DataType::UInt64, false),
                Column::new(2, "bucket", DataType::Int64, false),
            ],
        ),
        (
            "extras",
            vec![
                Column::new(1, "id", DataType::UInt64, false),
                Column::new(2, "item", DataType::Int64, false),
                Column::new(3, "tier", DataType::Int64, true),
            ],
        ),
    ];
    let signed = |value: u64| i64::try_from(value).expect("small");
    let rows: [Vec<StoredRow>; 2] = [
        (1..=items)
            .map(|id| stored(id, vec![Value::UInt64(id), Value::Int64(signed(id % 5))]))
            .collect(),
        (1..=items)
            .filter(|id| !id.is_multiple_of(3))
            .map(|id| {
                let tier = match id % 4 {
                    0 => Value::Null,
                    tier => Value::Int64(signed(tier - 1)),
                };
                stored(id, vec![Value::UInt64(id), Value::Int64(signed(id)), tier])
            })
            .collect(),
    ];
    let mut directories = Vec::new();
    let mut tables = Vec::new();
    let mut entries = Vec::new();
    for (index, ((name, columns), rows)) in schemas.into_iter().zip(rows).enumerate() {
        let id = index as u64 + 1;
        let schema = TableSchema::new(1, columns).expect("schema");
        let directory = tempfile::tempdir().expect("directory");
        let count = rows.len() as u64;
        let mut store = TableStore::open(directory.path(), schema.clone(), StoreOptions::default())
            .expect("open table");
        store.bulk_ingest_snapshot(rows).expect("rows");
        directories.push(directory);
        tables.push((TableId::new(id), store));
        entries.push(
            TableEntry::new(
                TableId::new(id),
                name,
                schema,
                TableStatistics::with_row_count(count),
            )
            .expect("entry")
            .with_key_columns([1])
            .expect("key"),
        );
    }
    Fixture {
        _directories: directories,
        tables,
        catalog: CatalogSnapshot::new([
            DatabaseEntry::new(DATABASE, "app", entries).expect("database")
        ])
        .expect("catalog"),
    }
}

impl Fixture {
    fn run(&self, sql: &str) -> Vec<Vec<String>> {
        let snapshots = self
            .tables
            .iter()
            .map(|(id, table)| (*id, table.snapshot()))
            .collect::<Vec<_>>();
        let provider = SnapshotScanProvider::new(
            snapshots
                .iter()
                .map(|(id, snapshot)| (DATABASE, *id, snapshot)),
        )
        .expect("provider");
        let bound = Binder::new(&self.catalog, Some("app"))
            .bind(&parse_statement(sql).expect("parse"))
            .expect("bind");
        let physical = PhysicalPlanner::plan(
            Optimizer::optimize(LogicalPlanner::plan(bound)),
            Collation::default(),
        )
        .expect("plan");
        let mut execution =
            Execution::start(physical, &provider, 64 * 1024 * 1024, Collation::default())
                .expect("start");
        let mut rows = Vec::new();
        while let Some(batch) = execution.next_batch().expect("batch") {
            for row in batch.selection().selected_rows() {
                rows.push(
                    (0..batch.columns().len())
                        .map(|column| {
                            match batch.column(column).and_then(|column| column.value(row)) {
                                Some(Value::Null) | None => "NULL".to_owned(),
                                Some(other) => format!("{other:?}"),
                            }
                        })
                        .collect(),
                );
            }
        }
        rows
    }
}

const JOIN: &str = "FROM items i LEFT JOIN extras e ON e.item = i.id";

/// Near-identical predicates on the null-extended side, each selecting a
/// different set of rows from the fixture (or the same set by another
/// spelling, which must still be answered on its own).
const PREDICATES: &[&str] = &[
    "e.tier IS NULL",
    "COALESCE(e.tier, 0) = 0",
    "e.tier = 0",
    "COALESCE(e.tier, 1) = 1",
    "COALESCE(e.tier, 0) = 1",
    "IFNULL(e.tier, 0) = 0",
    "IFNULL(e.tier, 2) = 2",
    "e.tier <=> NULL",
    "e.tier <=> 0",
    "NOT (e.tier IS NOT NULL)",
    "e.tier IS NOT NULL",
    "e.id IS NULL",
    "e.tier IS NULL AND e.id IS NOT NULL",
];

fn reference(fixture: &Fixture, predicate: &str, grouped: bool) -> Vec<Vec<String>> {
    let predicate = predicate.replace("e.", "x.");
    let (select, group) = if grouped {
        (
            "x.bucket, COUNT(*), SUM(x.iid)",
            "GROUP BY x.bucket ORDER BY x.bucket",
        )
    } else {
        ("COUNT(*), SUM(x.iid)", "")
    };
    fixture.run(&format!(
        "SELECT {select} FROM (SELECT i.id iid, i.bucket, e.id, e.tier {JOIN} \
         LIMIT 1000000000) x WHERE {predicate} {group}"
    ))
}

fn sweep(grouped: bool) {
    let fixture = fixture();
    let (select, group) = if grouped {
        (
            "i.bucket, COUNT(*), SUM(i.id)",
            "GROUP BY i.bucket ORDER BY i.bucket",
        )
    } else {
        ("COUNT(*), SUM(i.id)", "")
    };
    // Twice over: the second pass answers every case with the memo full of
    // its neighbours' entries.
    for pass in 0..2 {
        for predicate in PREDICATES {
            let got = fixture.run(&format!("SELECT {select} {JOIN} WHERE {predicate} {group}"));
            assert_eq!(
                got,
                reference(&fixture, predicate, grouped),
                "pass {pass}: {predicate}"
            );
        }
    }
}

#[test]
fn predicates_above_an_outer_join_key_the_memo_ungrouped() {
    sweep(false);
}

#[test]
fn predicates_above_an_outer_join_key_the_memo_grouped() {
    sweep(true);
}

/// The reported pair, in the reported order: sixty rows have no tier and
/// twenty more a zero one, so the second must not repeat the first.
#[test]
fn is_null_then_coalesce_zero_answer_apart() {
    let fixture = fixture();
    let count = |predicate: &str| {
        fixture.run(&format!(
            "SELECT COUNT(*), SUM(i.id) {JOIN} WHERE {predicate}"
        ))
    };
    let is_null = count("e.tier IS NULL");
    let coalesce = count("COALESCE(e.tier, 0) = 0");
    assert_eq!(is_null, reference(&fixture, "e.tier IS NULL", false));
    assert_eq!(
        coalesce,
        reference(&fixture, "COALESCE(e.tier, 0) = 0", false)
    );
    assert_ne!(is_null, coalesce);
}

/// Joins that differ only past their first key column, or only in the
/// ON condition's residual, pair different rows and must not share an
/// entry either. Each is compared with the same join read through a
/// derived table on each side.
#[test]
fn join_conditions_past_the_first_key_key_the_memo() {
    let fixture = fixture();
    let conditions = [
        "e.item = i.id",
        "e.item = i.id AND e.tier = i.bucket",
        "e.item = i.id AND e.tier + 1 = i.bucket",
        "e.item = i.id AND e.tier < i.bucket",
        "e.item = i.id AND e.tier > i.bucket",
        "e.item = i.id AND COALESCE(e.tier, 0) < i.bucket",
    ];
    for pass in 0..2 {
        for condition in conditions {
            for kind in ["LEFT", "INNER"] {
                let got = fixture.run(&format!(
                    "SELECT i.bucket, COUNT(*), COUNT(e.id), SUM(i.id) FROM items i \
                     {kind} JOIN extras e ON {condition} GROUP BY i.bucket ORDER BY i.bucket"
                ));
                let expected = fixture.run(&format!(
                    "SELECT i.bucket, COUNT(*), COUNT(e.id), SUM(i.id) FROM \
                     (SELECT id, bucket FROM items LIMIT 1000000000) i \
                     {kind} JOIN (SELECT id, item, tier FROM extras LIMIT 1000000000) e \
                     ON {condition} GROUP BY i.bucket ORDER BY i.bucket"
                ));
                assert_eq!(got, expected, "pass {pass}: {kind} JOIN ON {condition}");
            }
        }
    }
}
