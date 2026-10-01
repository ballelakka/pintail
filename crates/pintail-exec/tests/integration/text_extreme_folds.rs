//! Text MIN, MAX and COUNT(DISTINCT) folded a column at a time answer as
//! `MySQL` does under the column's collation: accent- and case-insensitive
//! with NO PAD (`utf8mb4_0900_ai_ci`), code points with PAD SPACE
//! (`utf8mb4_bin`), and single-byte weights with PAD SPACE
//! (`latin1_swedish_ci`). Values that tie under a collation (`amber` and
//! `Amber`, `zinc ` and `ZINC`) keep the spelling of the first row holding
//! one, which is the row `MySQL` keeps reading in key order.
//!
//! A low-cardinality column the store dictionary-codes and a column of
//! mostly distinct values cross both of the fold's paths; a filter makes it
//! read selected rows rather than whole spans, and a few-groups key sends
//! the same folds through the small-group path. The expected answers were
//! read from a `MySQL` 8.4 server holding the same rows.

use pintail_catalog::{
    CatalogSnapshot, DatabaseEntry, DatabaseId, TableEntry, TableId, TableStatistics,
};
use pintail_exec::collation::Collation;
use pintail_exec::{Execution, LogicalPlanner, Optimizer, PhysicalPlanner, SnapshotScanProvider};
use pintail_sql::{Binder, parse_statement};
use pintail_store::{StoreOptions, TableStore};
use pintail_types::{Column, DataType, KeyPart, PrimaryKey, StoredRow, TableSchema, Value};

const ROWS: u64 = 120_000;
const TAGS: [&str; 8] = [
    "ámber ", "amber", "Amber", "birch", "BIRCH", "zinc ", "Zínc", "ZINC",
];
const KITES: [&str; 4] = ["kite", "Kite", "kíte", "KITE "];
const REGIONS: [&str; 3] = ["north", "south", "east"];
const COLLATIONS: [&str; 3] = ["utf8mb4_0900_ai_ci", "utf8mb4_bin", "latin1_swedish_ci"];

struct Fixture {
    _directory: tempfile::TempDir,
    table: TableStore,
    catalog: CatalogSnapshot,
}

fn index(id: u64, modulus: u64) -> usize {
    usize::try_from(id % modulus).expect("small")
}

fn fixture() -> Fixture {
    let mut columns = vec![
        Column::new(1, "id", DataType::UInt64, false),
        Column::new(2, "region", DataType::Utf8, false),
    ];
    let mut next = 3;
    for (suffix, collation) in ["ai", "bin", "latin"].iter().zip(COLLATIONS) {
        for name in ["tag", "label"] {
            columns.push(
                Column::new(next, format!("{name}_{suffix}"), DataType::Utf8, true)
                    .with_collation(Some(collation.to_owned())),
            );
            next += 1;
        }
    }
    let schema = TableSchema::new(1, columns).expect("schema");
    let directory = tempfile::tempdir().expect("directory");
    let mut table =
        TableStore::open(directory.path(), schema.clone(), StoreOptions::default()).expect("table");
    let row = |id: u64| {
        let tag = if id.is_multiple_of(11) {
            Value::Null
        } else {
            Value::Utf8(TAGS[index(id, 8)].to_owned())
        };
        let label = if id.is_multiple_of(13) {
            Value::Null
        } else {
            Value::Utf8(format!("{}-{:05}", KITES[index(id, 4)], id % 50_000))
        };
        let mut values = vec![
            Value::UInt64(id),
            Value::Utf8(REGIONS[index(id, 3)].to_owned()),
        ];
        for _ in 0..3 {
            values.push(tag.clone());
            values.push(label.clone());
        }
        StoredRow::new(
            PrimaryKey::new(vec![KeyPart::UInt64(id)]).expect("key"),
            values,
            1,
            false,
        )
    };
    let mut start = 0;
    while start < ROWS {
        let end = (start + 30_000).min(ROWS);
        table
            .bulk_ingest_snapshot((start..end).map(row).collect())
            .expect("ingest");
        start = end;
    }
    let entry = TableEntry::new(
        TableId::new(1),
        "kites",
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

fn run(fixture: &Fixture, sql: &str) -> Vec<String> {
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
            let cells: Vec<String> = batch
                .columns()
                .iter()
                .map(|column| match column.value_owned(row).expect("value") {
                    Value::Null => "NULL".to_owned(),
                    Value::Utf8(text) => format!("[{text}]"),
                    Value::Int64(number) => number.to_string(),
                    Value::UInt64(number) => number.to_string(),
                    other => format!("{other:?}"),
                })
                .collect();
            rows.push(cells.join("|"));
        }
    }
    rows
}

/// `MySQL` 8.4's answers over the same rows, one line per column and
/// filter: `MIN|MAX|COUNT(DISTINCT)`.
const EXPECTED: &[(&str, &str, &str)] = &[
    ("tag_ai", "", "[amber]|[zinc ]|5"),
    ("tag_ai", "WHERE id % 3 <> 1", "[Amber]|[zinc ]|5"),
    ("label_ai", "", "[KITE -00003]|[kíte-49998]|50000"),
    (
        "label_ai",
        "WHERE id % 3 <> 1",
        "[KITE -00003]|[kíte-49998]|48461",
    ),
    ("tag_bin", "", "[Amber]|[ámber ]|8"),
    ("tag_bin", "WHERE id % 3 <> 1", "[Amber]|[ámber ]|8"),
    ("label_bin", "", "[KITE -00003]|[kíte-49998]|50000"),
    (
        "label_bin",
        "WHERE id % 3 <> 1",
        "[KITE -00003]|[kíte-49998]|48461",
    ),
    ("tag_latin", "", "[amber]|[zinc ]|3"),
    ("tag_latin", "WHERE id % 3 <> 1", "[Amber]|[zinc ]|3"),
    ("label_latin", "", "[KITE -00003]|[kíte-49998]|50000"),
    (
        "label_latin",
        "WHERE id % 3 <> 1",
        "[KITE -00003]|[kíte-49998]|48461",
    ),
];

#[test]
fn text_extremes_and_distinct_counts_follow_the_collation() {
    let fixture = fixture();
    let print = std::env::var_os("TEXT_FOLD_PRINT").is_some();
    let mut failures = Vec::new();
    for (column, filter, expected) in EXPECTED {
        let sql = format!(
            "SELECT MIN({column}), MAX({column}), COUNT(DISTINCT {column}) FROM kites {filter}"
        );
        let answer = run(&fixture, &sql).join("\n");
        if print {
            println!("{column}\t{filter}\t{answer}");
        }
        if answer != *expected {
            failures.push(format!("{sql}: {answer} != {expected}"));
        }
        // The few-groups path folds the same columns per group; each
        // group's answer must be the one its own filter gives ungrouped.
        let grouped = run(
            &fixture,
            &format!(
                "SELECT region, MIN({column}), MAX({column}), COUNT(DISTINCT {column}) \
                 FROM kites {filter} GROUP BY region ORDER BY region"
            ),
        );
        // The filter drops every row of one region.
        let regions = if filter.is_empty() { 3 } else { 2 };
        if grouped.len() != regions {
            failures.push(format!("{column} {filter}: {} groups", grouped.len()));
        }
        for line in &grouped {
            let region = line.split('|').next().expect("region cell");
            let region = region.trim_matches(|c| c == '[' || c == ']');
            let joiner = if filter.is_empty() { "WHERE" } else { "AND" };
            let alone = run(
                &fixture,
                &format!(
                    "SELECT MIN({column}), MAX({column}), COUNT(DISTINCT {column}) \
                     FROM kites {filter} {joiner} region = '{region}'"
                ),
            )
            .join("\n");
            let grouped_answer = line.split_once('|').expect("cells").1;
            if grouped_answer != alone {
                failures.push(format!(
                    "{column} {filter} region {region}: grouped {grouped_answer} != {alone}"
                ));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
