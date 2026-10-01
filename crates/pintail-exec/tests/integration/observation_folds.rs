//! Double SUM and AVG, STDDEV/VARIANCE over integers and doubles, and the
//! BIT_* folds, folded a column at a time, give the bits the per-row
//! update gives. An expression argument (`col + 0`) keeps the per-row
//! update, so each pair is the same rows through both. The doubles are
//! built to make the summation order visible: large and small magnitudes
//! interleaved, so any reordering moves the last bits.

use pintail_catalog::{
    CatalogSnapshot, DatabaseEntry, DatabaseId, TableEntry, TableId, TableStatistics,
};
use pintail_exec::collation::Collation;
use pintail_exec::{Execution, LogicalPlanner, Optimizer, PhysicalPlanner, SnapshotScanProvider};
use pintail_sql::{Binder, parse_statement};
use pintail_store::{StoreOptions, TableStore};
use pintail_types::{Column, DataType, KeyPart, PrimaryKey, StoredRow, TableSchema, Value};

const ROWS: u64 = 150_000;

struct Fixture {
    _directory: tempfile::TempDir,
    table: TableStore,
    catalog: CatalogSnapshot,
}

fn fixture() -> Fixture {
    let schema = TableSchema::new(
        1,
        vec![
            Column::new(1, "id", DataType::UInt64, false),
            Column::new(2, "shelf", DataType::Utf8, false),
            Column::new(3, "weight", DataType::Int64, true),
            Column::new(4, "mass", DataType::Float64, true),
            Column::new(
                5,
                "cost",
                DataType::Decimal {
                    precision: 14,
                    scale: 3,
                },
                true,
            ),
            Column::new(6, "bits", DataType::UInt64, true),
        ],
    )
    .expect("schema");
    let directory = tempfile::tempdir().expect("directory");
    let mut table =
        TableStore::open(directory.path(), schema.clone(), StoreOptions::default()).expect("table");
    let row = |id: u64| {
        let signed = i64::try_from(id).expect("small");
        let weight = if id.is_multiple_of(7) {
            Value::Null
        } else {
            Value::Int64((signed * 7_919) % 1_000_003 - 500_000)
        };
        #[allow(clippy::cast_precision_loss)]
        let mass = if id.is_multiple_of(9) {
            Value::Null
        } else if id.is_multiple_of(2) {
            Value::float64(1e15 + (id as f64) / 3.0)
        } else {
            Value::float64(-(id as f64) * 0.1)
        };
        let cost = if id.is_multiple_of(11) {
            Value::Null
        } else {
            let units = (signed * 104_729) % 90_000_000_000 - 40_000_000_000;
            Value::Utf8(format!(
                "{}{}.{:03}",
                if units < 0 { "-" } else { "" },
                units.abs() / 1_000,
                units.abs() % 1_000
            ))
        };
        let bits = if id.is_multiple_of(13) {
            Value::Null
        } else {
            Value::UInt64(id.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1)
        };
        StoredRow::new(
            PrimaryKey::new(vec![KeyPart::UInt64(id)]).expect("key"),
            vec![
                Value::UInt64(id),
                Value::Utf8(["oak", "pine", "ash"][usize::try_from(id % 3).expect("small")].into()),
                weight,
                mass,
                cost,
                bits,
            ],
            1,
            false,
        )
    };
    let mut start = 0;
    while start < ROWS {
        let end = (start + 50_000).min(ROWS);
        table
            .bulk_ingest_snapshot((start..end).map(row).collect())
            .expect("ingest");
        start = end;
    }
    let entry = TableEntry::new(
        TableId::new(1),
        "crates",
        schema,
        TableStatistics::with_row_count(ROWS),
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
                    .collect(),
            );
        }
    }
    rows
}

#[test]
fn folded_observations_match_the_per_row_update_bit_for_bit() {
    let fixture = fixture();
    // (folded, per row, whether the grouped form is compared too). A
    // grouped double or integer STDDEV rides a scatter lane whose partials
    // merge, on either side of this change, so only the ungrouped form is
    // the serial fold.
    let pairs = [
        ("SUM(mass)", "SUM(mass + 0)", false),
        ("AVG(mass)", "AVG(mass + 0)", false),
        ("STDDEV(mass)", "STDDEV(mass + 0)", false),
        ("VAR_SAMP(mass)", "VAR_SAMP(mass + 0)", false),
        ("STDDEV(weight)", "STDDEV(weight + 0)", false),
        ("VARIANCE(weight)", "VARIANCE(weight + 0)", false),
        ("BIT_AND(weight)", "BIT_AND(weight + 0)", true),
        ("BIT_OR(weight)", "BIT_OR(weight + 0)", true),
        ("BIT_XOR(weight)", "BIT_XOR(weight + 0)", true),
        ("BIT_AND(bits)", "BIT_AND(bits + 0)", true),
        ("BIT_XOR(bits)", "BIT_XOR(bits + 0)", true),
    ];
    let mut failures = Vec::new();
    // Every filter keeps the scan from answering out of block statistics,
    // which total a double column block by block.
    for filter in ["WHERE id % 5 <> 2", "WHERE id % 2 = 0", "WHERE id < 0"] {
        for (folded, per_row, grouped) in pairs {
            for (key, group) in [("", ""), ("shelf, ", " GROUP BY shelf ORDER BY shelf")] {
                if !group.is_empty() && !grouped {
                    continue;
                }
                let left = run(
                    &fixture,
                    &format!("SELECT {key}{folded} FROM crates {filter}{group}"),
                );
                let right = run(
                    &fixture,
                    &format!("SELECT {key}{per_row} FROM crates {filter}{group}"),
                );
                if format!("{left:?}") != format!("{right:?}") {
                    failures.push(format!("{folded} {filter}{group}: {left:?} != {right:?}"));
                }
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
