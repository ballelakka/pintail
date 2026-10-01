//! A filter-first scan decodes its predicate columns, chooses the rows the
//! filter keeps, and reads the other columns for those rows alone. These
//! tests hold the answers to a direct computation over a model of the
//! table at every selectivity from nothing to everything, across NULLs in
//! the tested and the read columns, live memtable rows over the segments,
//! and a column added by a schema change that older segments lack.
//!
//! The table is invented: a key, a selector spread pseudo-randomly over
//! the key, a decimal, a nullable text, a dictionary label and a nullable
//! integer, later joined by a nullable integer added with the schema.

use std::collections::BTreeMap;

use pintail_catalog::{
    CatalogSnapshot, DatabaseEntry, DatabaseId, TableEntry, TableId, TableStatistics,
};
use pintail_exec::collation::Collation;
use pintail_exec::{Execution, LogicalPlanner, Optimizer, PhysicalPlanner, SnapshotScanProvider};
use pintail_sql::{Binder, parse_statement};
use pintail_store::{StoreOptions, TableStore};
use pintail_types::{Column, DataType, KeyPart, PrimaryKey, StoredRow, TableSchema, Value};

const LABELS: [&str; 4] = ["alpha", "bravo", "delta", "omega"];
const ROWS: u64 = 60_000;
/// Out of 1000: none, one in a thousand, one in a hundred, a tenth, half,
/// and every row with a selector.
const THRESHOLDS: [i64; 6] = [0, 1, 10, 100, 500, 1000];

fn schema(with_extra: bool) -> TableSchema {
    let mut columns = vec![
        Column::new(1, "id", DataType::UInt64, false),
        Column::new(2, "sel", DataType::Int64, true),
        Column::new(
            3,
            "amount",
            DataType::Decimal {
                precision: 12,
                scale: 2,
            },
            false,
        ),
        Column::new(4, "note", DataType::Utf8, true),
        Column::new(5, "label", DataType::Utf8, false),
        Column::new(6, "score", DataType::Int64, true),
    ];
    if with_extra {
        columns.push(Column::new(7, "extra", DataType::Int64, true));
    }
    TableSchema::new(if with_extra { 2 } else { 1 }, columns).expect("schema")
}

fn mix(value: u64) -> u64 {
    let mut z = value.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

#[derive(Clone)]
struct Model {
    sel: Option<i64>,
    cents: u64,
    note: Option<String>,
    label: usize,
    score: Option<i64>,
    extra: Option<i64>,
}

fn generate(id: u64, salt: u64) -> Model {
    let h = mix(id ^ (salt << 40));
    Model {
        sel: (!id.is_multiple_of(13)).then(|| i64::try_from(h % 1000).expect("small")),
        cents: 1 + (h >> 12) % 100_000,
        note: (!id.is_multiple_of(17)).then(|| format!("n{:012x}", (h >> 16) & 0xFFFF_FFFF_FFFF)),
        label: usize::try_from((h >> 50) % 4).expect("small"),
        score: (!id.is_multiple_of(11)).then(|| i64::try_from((h >> 30) % 500).expect("small")),
        extra: None,
    }
}

fn stored(id: u64, model: &Model, version: u64, with_extra: bool, deleted: bool) -> StoredRow {
    let mut values = vec![
        Value::UInt64(id),
        model.sel.map_or(Value::Null, Value::Int64),
        Value::Utf8(cents(model.cents)),
        model.note.clone().map_or(Value::Null, Value::Utf8),
        Value::Utf8(LABELS[model.label].to_owned()),
        model.score.map_or(Value::Null, Value::Int64),
    ];
    if with_extra {
        values.push(model.extra.map_or(Value::Null, Value::Int64));
    }
    StoredRow::new(
        PrimaryKey::new(vec![KeyPart::UInt64(id)]).expect("key"),
        values,
        version,
        deleted,
    )
}

fn cents(value: u64) -> String {
    format!("{}.{:02}", value / 100, value % 100)
}

fn run(table: &TableStore, schema: &TableSchema, sql: &str) -> Vec<String> {
    let snapshot = table.snapshot();
    let entry = TableEntry::new(
        TableId::new(1),
        "facts",
        schema.clone(),
        TableStatistics::with_row_count(ROWS),
    )
    .expect("entry")
    .with_key_columns([1])
    .expect("key");
    let catalog = CatalogSnapshot::new([
        DatabaseEntry::new(DatabaseId::new(1), "app", [entry]).expect("database")
    ])
    .expect("catalog");
    let provider = SnapshotScanProvider::new([(DatabaseId::new(1), TableId::new(1), &snapshot)])
        .expect("provider");
    let bound = Binder::new(&catalog, Some("app"))
        .bind(&parse_statement(sql).expect("parse"))
        .expect("bind");
    let physical = PhysicalPlanner::plan(
        Optimizer::optimize(LogicalPlanner::plan(bound)),
        Collation::default(),
    )
    .expect("plan");
    let mut execution =
        Execution::start(physical, &provider, 1 << 30, Collation::default()).expect("start");
    let mut rows = Vec::new();
    while let Some(batch) = execution.next_batch().expect("batch") {
        for row in batch.selection().selected_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|column| match column.value_owned(row).expect("value") {
                        Value::Int64(value) => value.to_string(),
                        Value::UInt64(value) => value.to_string(),
                        Value::Utf8(text) => text,
                        Value::Null => "NULL".to_owned(),
                        other => format!("{other:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join("|"),
            );
        }
    }
    rows
}

fn text(value: Option<&str>) -> String {
    value.map_or_else(|| "NULL".to_owned(), str::to_owned)
}

fn number(value: Option<i64>) -> String {
    value.map_or_else(|| "NULL".to_owned(), |value| value.to_string())
}

/// Every query shape at every threshold, against the model.
fn check(table: &TableStore, schema: &TableSchema, model: &BTreeMap<u64, Model>, stage: &str) {
    let with_extra = schema.columns().len() == 7;
    for threshold in THRESHOLDS {
        let kept = model
            .iter()
            .filter(|(_, row)| row.sel.is_some_and(|sel| sel < threshold))
            .collect::<Vec<_>>();
        let count = kept.len();
        let amount: u64 = kept.iter().map(|(_, row)| row.cents).sum();
        let notes = kept.iter().filter_map(|(_, row)| row.note.as_deref());
        let min_note = notes.clone().min();
        let max_note = notes.max();
        let scores = kept.iter().filter_map(|(_, row)| row.score);
        let score_sum = (scores.clone().count() > 0).then(|| scores.clone().sum::<i64>());
        let expected = vec![format!(
            "{count}|{}|{}|{}|{}|{}",
            if count == 0 {
                "NULL".to_owned()
            } else {
                cents(amount)
            },
            text(min_note),
            text(max_note),
            number(score_sum),
            scores.count()
        )];
        let sql = format!(
            "SELECT COUNT(*), SUM(amount), MIN(note), MAX(note), SUM(score), COUNT(score) \
             FROM facts WHERE sel < {threshold}"
        );
        assert_eq!(run(table, schema, &sql), expected, "{stage}: {sql}");

        let mut labels: BTreeMap<usize, (usize, u64)> = BTreeMap::new();
        for (_, row) in &kept {
            let entry = labels.entry(row.label).or_default();
            entry.0 += 1;
            entry.1 += row.cents;
        }
        let expected = labels
            .iter()
            .map(|(label, (count, amount))| {
                format!("{}|{count}|{}", LABELS[*label], cents(*amount))
            })
            .collect::<Vec<_>>();
        let sql = format!(
            "SELECT label, COUNT(*), SUM(amount) FROM facts WHERE sel < {threshold} \
             GROUP BY label ORDER BY label"
        );
        assert_eq!(run(table, schema, &sql), expected, "{stage}: {sql}");

        let expected = kept
            .iter()
            .map(|(id, row)| {
                let mut line = format!(
                    "{id}|{}|{}|{}",
                    text(row.note.as_deref()),
                    cents(row.cents),
                    number(row.score)
                );
                if with_extra {
                    line.push('|');
                    line.push_str(&number(row.extra));
                }
                line
            })
            .collect::<Vec<_>>();
        let sql = format!(
            "SELECT id, note, amount, score{} FROM facts WHERE sel < {threshold} ORDER BY id",
            if with_extra { ", extra" } else { "" }
        );
        assert_eq!(run(table, schema, &sql), expected, "{stage}: {sql}");
    }
    if with_extra {
        for bound in [0, 5, 50, 1000] {
            let kept = model
                .iter()
                .filter(|(_, row)| row.extra.is_some_and(|extra| extra < bound))
                .collect::<Vec<_>>();
            let expected = kept
                .iter()
                .map(|(id, row)| format!("{id}|{}|{}", number(row.sel), text(row.note.as_deref())))
                .collect::<Vec<_>>();
            let sql = format!("SELECT id, sel, note FROM facts WHERE extra < {bound} ORDER BY id");
            assert_eq!(run(table, schema, &sql), expected, "{stage}: {sql}");
        }
    }
}

#[test]
fn filter_first_scans_match_the_model_through_mutations_and_a_schema_change() {
    let directory = tempfile::tempdir().expect("directory");
    let v1 = schema(false);
    let mut table =
        TableStore::open(directory.path(), v1.clone(), StoreOptions::default()).expect("table");
    let mut model: BTreeMap<u64, Model> = BTreeMap::new();
    // Two segments of two full blocks and a short one each.
    for (start, end) in [(1, ROWS / 2), (ROWS / 2 + 1, ROWS)] {
        let rows = (start..=end)
            .map(|id| {
                let row = generate(id, 0);
                let stored = stored(id, &row, id, false, false);
                model.insert(id, row);
                stored
            })
            .collect();
        table.bulk_ingest_snapshot(rows).expect("snapshot");
    }
    check(&table, &v1, &model, "segments");

    // Live rows over the segments: updates, deletes and appends.
    let mut version = ROWS + 1;
    let mut live = Vec::new();
    for id in (1..=ROWS).step_by(97) {
        let row = generate(id, 1);
        live.push(stored(id, &row, version, false, false));
        model.insert(id, row);
        version += 1;
    }
    for id in (5..=ROWS).step_by(101) {
        let row = model.remove(&id).unwrap_or_else(|| generate(id, 0));
        live.push(stored(id, &row, version, false, true));
        version += 1;
    }
    for id in ROWS + 1..=ROWS + 500 {
        let row = generate(id, 2);
        live.push(stored(id, &row, version, false, false));
        model.insert(id, row);
        version += 1;
    }
    table.ingest_cdc(live).expect("live rows");
    check(&table, &v1, &model, "memtable");

    // A nullable column added: every row so far reads it as NULL.
    let v2 = schema(true);
    table.evolve_schema(v2.clone()).expect("evolve");
    check(&table, &v2, &model, "after the schema change");

    // Rows written after the change carry it, in the memtable and then in
    // a segment of their own beside the older ones.
    let mut live = Vec::new();
    for id in (3..=ROWS + 500).step_by(89) {
        let Some(row) = model.get_mut(&id) else {
            continue;
        };
        row.extra = Some(i64::try_from(mix(id) % 100).expect("small"));
        live.push(stored(id, row, version, true, false));
        version += 1;
    }
    table.ingest_cdc(live).expect("live rows");
    check(&table, &v2, &model, "memtable after the schema change");
    table.flush().expect("flush");
    check(&table, &v2, &model, "flushed after the schema change");
}
