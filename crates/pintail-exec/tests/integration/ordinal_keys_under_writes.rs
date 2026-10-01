//! An ENUM sorts by its declared ordinal and a SET by its member bitmask,
//! and that has to hold wherever the rows live: in a settled segment, in the
//! memtable only (a table written since its last flush), or split between
//! the two with updates, deletes and inserts on top of a segment. Each
//! layout reaches the sort through a different scan path, and a path that
//! hands the key over as bare text sorts it alphabetically.
//!
//! Labels are declared so that alphabetical and declared order disagree
//! everywhere; the expectations are computed from the declaration.

use std::collections::BTreeMap;

use pintail_catalog::{
    CatalogSnapshot, DatabaseEntry, DatabaseId, TableEntry, TableId, TableStatistics,
};
use pintail_exec::collation::Collation;
use pintail_exec::{Execution, LogicalPlanner, Optimizer, PhysicalPlanner, SnapshotScanProvider};
use pintail_sql::{Binder, parse_statement};
use pintail_store::{StoreOptions, TableStore};
use pintail_types::{Column, DataType, KeyPart, PrimaryKey, StoredRow, TableSchema, Value};

/// Ordinals 1..=5; alphabetically: cancelled, delivered, pending,
/// processing, shipped.
const LABELS: [&str; 5] = ["pending", "processing", "shipped", "delivered", "cancelled"];

/// Bits 1, 2, 4, 8; alphabetically: alpha, beta, new, vip.
const MEMBERS: [&str; 4] = ["vip", "new", "beta", "alpha"];

/// Subsets written in declared member order, as `MySQL` stores them.
const TAGS: [&str; 7] = [
    "",
    "alpha",
    "vip",
    "new,alpha",
    "vip,beta",
    "beta",
    "vip,new,beta,alpha",
];

fn mask(tags: &str) -> u64 {
    tags.split(',')
        .filter(|member| !member.is_empty())
        .map(|member| {
            1_u64
                << MEMBERS
                    .iter()
                    .position(|declared| *declared == member)
                    .expect("declared member")
        })
        .sum()
}

fn ordinal(status: &str) -> usize {
    LABELS
        .iter()
        .position(|declared| *declared == status)
        .expect("declared label")
}

fn schema() -> TableSchema {
    TableSchema::new(
        1,
        vec![
            Column::new(1, "id", DataType::UInt64, false),
            Column::new(2, "status", DataType::Utf8, true)
                .with_enum_labels(Some(LABELS.iter().map(ToString::to_string).collect())),
            Column::new(3, "tags", DataType::Utf8, true)
                .with_set_members(Some(MEMBERS.iter().map(ToString::to_string).collect())),
        ],
    )
    .expect("schema")
}

fn row(id: u64, status: &str, tags: &str, version: u64, deleted: bool) -> StoredRow {
    StoredRow::new(
        PrimaryKey::new(vec![KeyPart::UInt64(id)]).expect("key"),
        vec![
            Value::UInt64(id),
            Value::Utf8(status.to_owned()),
            Value::Utf8(tags.to_owned()),
        ],
        version,
        deleted,
    )
}

/// Where the table's rows live when the query runs.
#[derive(Clone, Copy, Debug)]
enum Layout {
    /// One settled segment, nothing in the memtable.
    Settled,
    /// Every row still in the memtable.
    MemtableOnly,
    /// A settled segment with updates, deletes and inserts in the memtable.
    SettledWithWrites,
    /// Two flushed segments whose keys interleave, plus memtable writes.
    TwoFlushesWithWrites,
}

const LAYOUTS: [Layout; 4] = [
    Layout::Settled,
    Layout::MemtableOnly,
    Layout::SettledWithWrites,
    Layout::TwoFlushesWithWrites,
];

/// id -> (status, tags) the table answers with.
type Model = BTreeMap<u64, (&'static str, &'static str)>;

fn base(id: u64) -> (&'static str, &'static str) {
    let status = LABELS[usize::try_from(id.wrapping_mul(7) % 5).expect("label")];
    let tags = TAGS[usize::try_from(id.wrapping_mul(11) % 7).expect("tags")];
    (status, tags)
}

fn changed(id: u64) -> (&'static str, &'static str) {
    let status = LABELS[usize::try_from((id + 3) % 5).expect("label")];
    let tags = TAGS[usize::try_from((id + 2) % 7).expect("tags")];
    (status, tags)
}

fn build(layout: Layout, rows: u64) -> (tempfile::TempDir, TableStore, Model) {
    let directory = tempfile::tempdir().expect("directory");
    let mut table = TableStore::open(
        directory.path(),
        schema(),
        StoreOptions {
            background_compaction: false,
            ..StoreOptions::default()
        },
    )
    .expect("table");
    let mut model = Model::new();
    let all = |model: &mut Model| {
        (1..=rows)
            .map(|id| {
                let (status, tags) = base(id);
                model.insert(id, (status, tags));
                row(id, status, tags, 1, false)
            })
            .collect::<Vec<_>>()
    };
    let writes = |model: &mut Model| {
        let mut writes = Vec::new();
        let step = (rows / 9).max(1);
        for id in (1..=rows).step_by(usize::try_from(step).expect("step")) {
            let (status, tags) = changed(id);
            model.insert(id, (status, tags));
            writes.push(row(id, status, tags, 3, false));
        }
        for id in (2..=rows).step_by(usize::try_from(step * 2).expect("step")) {
            model.remove(&id);
            writes.push(row(id, "pending", "", 3, true));
        }
        for id in [rows + 1, rows + 2, rows + 50] {
            let (status, tags) = changed(id);
            model.insert(id, (status, tags));
            writes.push(row(id, status, tags, 3, false));
        }
        writes
    };
    match layout {
        Layout::Settled => {
            let rows = all(&mut model);
            table.bulk_ingest_snapshot(rows).expect("ingest");
        }
        Layout::MemtableOnly => {
            let rows = all(&mut model);
            table.ingest(rows).expect("memtable rows");
        }
        Layout::SettledWithWrites => {
            let rows = all(&mut model);
            table.bulk_ingest_snapshot(rows).expect("ingest");
            let writes = writes(&mut model);
            table.ingest(writes).expect("memtable writes");
        }
        Layout::TwoFlushesWithWrites => {
            let rows = all(&mut model);
            let (even, odd): (Vec<_>, Vec<_>) = rows
                .into_iter()
                .enumerate()
                .partition(|(position, _)| position % 2 == 0);
            table
                .ingest(even.into_iter().map(|(_, row)| row).collect())
                .expect("first half");
            table.flush().expect("first flush");
            table
                .ingest(odd.into_iter().map(|(_, row)| row).collect())
                .expect("second half");
            table.flush().expect("second flush");
            let writes = writes(&mut model);
            table.ingest(writes).expect("memtable writes");
        }
    }
    (directory, table, model)
}

fn run(table: &TableStore, rows: u64, sql: &str) -> Vec<Vec<String>> {
    let snapshot = table.snapshot();
    let database_id = DatabaseId::new(1);
    let table_id = TableId::new(1);
    let entry = TableEntry::new(
        table_id,
        "parcels",
        schema(),
        TableStatistics::with_row_count(rows),
    )
    .expect("entry")
    .with_key_columns([1])
    .expect("key");
    let database = DatabaseEntry::new(database_id, "app", [entry]).expect("database");
    let catalog = CatalogSnapshot::new([database]).expect("catalog");
    let provider =
        SnapshotScanProvider::new([(database_id, table_id, &snapshot)]).expect("provider");
    let statement = parse_statement(sql).expect("parse");
    let bound = Binder::new(&catalog, Some("app"))
        .bind(&statement)
        .expect("bind");
    let physical = PhysicalPlanner::plan(
        Optimizer::optimize(LogicalPlanner::plan(bound)),
        Collation::default(),
    )
    .expect("plan");
    let mut execution =
        Execution::start(physical, &provider, 256 * 1024 * 1024, Collation::default())
            .expect("start");
    let mut out = Vec::new();
    while let Some(batch) = execution
        .next_batch()
        .unwrap_or_else(|error| panic!("pull batch for {sql}: {error}"))
    {
        for row in batch.selection().selected_rows() {
            out.push(
                batch
                    .columns()
                    .iter()
                    .map(|column| match column.value(row) {
                        Some(Value::Null) | None => "NULL".to_owned(),
                        Some(Value::Utf8(text) | Value::Enum { label: text, .. }) => text.clone(),
                        Some(Value::UInt64(number)) => number.to_string(),
                        Some(Value::Int64(number)) => number.to_string(),
                        Some(other) => format!("{other:?}"),
                    })
                    .collect::<Vec<_>>(),
            );
        }
    }
    out
}

/// Every (layout, size) the shapes below run over: a handful of rows, a
/// table of a few blocks, and one past the size a scan once materialized.
fn each(mut check: impl FnMut(&str, &TableStore, u64, &Model)) {
    for rows in [12_u64, 5_000, 70_000] {
        for layout in LAYOUTS {
            let (_directory, table, model) = build(layout, rows);
            check(&format!("{layout:?} at {rows} rows"), &table, rows, &model);
        }
    }
}

fn status_counts(model: &Model) -> Vec<Vec<String>> {
    let mut counts = BTreeMap::<usize, u64>::new();
    for (status, _) in model.values() {
        *counts.entry(ordinal(status)).or_default() += 1;
    }
    counts
        .into_iter()
        .map(|(ordinal, count)| vec![LABELS[ordinal].to_owned(), count.to_string()])
        .collect()
}

fn tag_counts(model: &Model) -> Vec<Vec<String>> {
    let mut counts = BTreeMap::<u64, (&str, u64)>::new();
    for (_, tags) in model.values() {
        counts.entry(mask(tags)).or_insert((tags, 0)).1 += 1;
    }
    counts
        .into_values()
        .map(|(tags, count)| vec![tags.to_owned(), count.to_string()])
        .collect()
}

fn first_column(rows: Vec<Vec<String>>) -> Vec<Vec<String>> {
    rows.into_iter().map(|row| vec![row[0].clone()]).collect()
}

#[test]
fn distinct_enum_orders_by_ordinal_in_every_layout() {
    each(|case, table, rows, model| {
        assert_eq!(
            run(
                table,
                rows,
                "SELECT DISTINCT status FROM parcels ORDER BY status"
            ),
            first_column(status_counts(model)),
            "{case}"
        );
        assert_eq!(
            run(
                table,
                rows,
                "SELECT DISTINCT status FROM parcels ORDER BY status DESC"
            ),
            first_column(status_counts(model).into_iter().rev().collect()),
            "{case}, descending"
        );
    });
}

#[test]
fn grouped_enum_orders_by_ordinal_in_every_layout() {
    each(|case, table, rows, model| {
        assert_eq!(
            run(
                table,
                rows,
                "SELECT status, COUNT(*) AS n FROM parcels GROUP BY status ORDER BY status"
            ),
            status_counts(model),
            "{case}"
        );
    });
}

#[test]
fn distinct_set_orders_by_bitmask_in_every_layout() {
    each(|case, table, rows, model| {
        assert_eq!(
            run(
                table,
                rows,
                "SELECT DISTINCT tags FROM parcels ORDER BY tags"
            ),
            first_column(tag_counts(model)),
            "{case}"
        );
    });
}

#[test]
fn grouped_set_orders_by_bitmask_in_every_layout() {
    each(|case, table, rows, model| {
        assert_eq!(
            run(
                table,
                rows,
                "SELECT tags, COUNT(*) AS n FROM parcels GROUP BY tags ORDER BY tags"
            ),
            tag_counts(model),
            "{case}"
        );
        assert_eq!(
            run(
                table,
                rows,
                "SELECT tags, COUNT(*) AS n FROM parcels GROUP BY tags ORDER BY tags DESC"
            ),
            tag_counts(model).into_iter().rev().collect::<Vec<_>>(),
            "{case}, descending"
        );
    });
}

#[test]
fn a_two_key_group_orders_by_both_ordinals_in_every_layout() {
    each(|case, table, rows, model| {
        let mut counts = BTreeMap::<(usize, u64), (&str, &str, u64)>::new();
        for (status, tags) in model.values() {
            counts
                .entry((ordinal(status), mask(tags)))
                .or_insert((status, tags, 0))
                .2 += 1;
        }
        let expected = counts
            .into_values()
            .map(|(status, tags, count)| {
                vec![status.to_owned(), tags.to_owned(), count.to_string()]
            })
            .collect::<Vec<_>>();
        assert_eq!(
            run(
                table,
                rows,
                "SELECT status, tags, COUNT(*) AS n FROM parcels \
                 GROUP BY status, tags ORDER BY status, tags"
            ),
            expected,
            "{case}"
        );
    });
}

#[test]
fn a_limited_sort_keeps_the_lowest_ordinals_in_every_layout() {
    each(|case, table, rows, model| {
        let mut by_status = model
            .iter()
            .map(|(id, (status, _))| (ordinal(status), *id))
            .collect::<Vec<_>>();
        by_status.sort_unstable();
        let expected = by_status
            .iter()
            .take(7)
            .map(|(ordinal, id)| vec![id.to_string(), LABELS[*ordinal].to_owned()])
            .collect::<Vec<_>>();
        assert_eq!(
            run(
                table,
                rows,
                "SELECT id, status FROM parcels ORDER BY status, id LIMIT 7"
            ),
            expected,
            "{case}"
        );
        let mut by_tags = model
            .iter()
            .map(|(id, (_, tags))| (mask(tags), *id, *tags))
            .collect::<Vec<_>>();
        by_tags.sort_unstable();
        let expected = by_tags
            .iter()
            .take(7)
            .map(|(_, id, tags)| vec![id.to_string(), (*tags).to_owned()])
            .collect::<Vec<_>>();
        assert_eq!(
            run(
                table,
                rows,
                "SELECT id, tags FROM parcels ORDER BY tags, id LIMIT 7"
            ),
            expected,
            "{case}"
        );
    });
}
