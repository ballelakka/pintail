//! A GROUP BY over an inner join whose aggregates all read one large
//! relation folds that relation first. The answers here are computed
//! independently from the fixture's rows, including a join key repeated on
//! the other side, where each partial group must count once per match.

use std::collections::BTreeMap;

use pintail_catalog::{
    CatalogSnapshot, DatabaseEntry, DatabaseId, TableEntry, TableId, TableStatistics,
};
use pintail_exec::collation::Collation;
use pintail_exec::{Execution, LogicalPlanner, Optimizer, PhysicalPlanner, SnapshotScanProvider};
use pintail_sql::{Binder, BoundQuery, parse_statement};
use pintail_store::{StoreOptions, TableStore};
use pintail_types::{Column, DataType, KeyPart, PrimaryKey, StoredRow, TableSchema, Value};

const FACTS: u64 = 60_000;
const DIMS: u64 = 100;
const TAGS: u64 = 150;

fn dim_of(id: u64) -> u64 {
    id % DIMS + 1
}

fn qty_of(id: u64) -> i64 {
    i64::try_from(id * 7 % 13).expect("small")
}

fn bonus_of(id: u64) -> Option<i64> {
    (!id.is_multiple_of(5)).then(|| i64::try_from(id % 3).expect("small"))
}

fn stored(id: u64, values: Vec<Value>) -> StoredRow {
    StoredRow::new(
        PrimaryKey::new(vec![KeyPart::UInt64(id)]).expect("key"),
        values,
        id,
        false,
    )
}

fn table(directory: &std::path::Path, schema: &TableSchema, rows: Vec<StoredRow>) -> TableStore {
    let mut table =
        TableStore::open(directory, schema.clone(), StoreOptions::default()).expect("open table");
    table.bulk_ingest_snapshot(rows).expect("rows");
    table
}

struct Fixture {
    _directories: Vec<tempfile::TempDir>,
    tables: Vec<(TableId, TableStore)>,
    catalog: CatalogSnapshot,
}

const DATABASE: DatabaseId = DatabaseId::new(3);

#[allow(clippy::too_many_lines)] // three tables of fixture data read best inline
fn fixture() -> Fixture {
    let facts = TableSchema::new(
        1,
        vec![
            Column::new(1, "id", DataType::UInt64, false),
            Column::new(2, "dim", DataType::Int64, false),
            Column::new(3, "qty", DataType::Int64, false),
            Column::new(4, "bonus", DataType::Int64, true),
        ],
    )
    .expect("facts");
    let dims = TableSchema::new(
        1,
        vec![
            Column::new(1, "id", DataType::Int64, false),
            Column::new(2, "region", DataType::Utf8, false),
        ],
    )
    .expect("dims");
    let tags = TableSchema::new(
        1,
        vec![
            Column::new(1, "id", DataType::UInt64, false),
            Column::new(2, "dim", DataType::Int64, false),
            Column::new(3, "tag", DataType::Utf8, false),
        ],
    )
    .expect("tags");
    let mut directories = Vec::new();
    let mut tables = Vec::new();
    let mut entries = Vec::new();
    let specs: [(u64, &str, &TableSchema, Vec<StoredRow>); 3] = [
        (
            1,
            "facts",
            &facts,
            (1..=FACTS)
                .map(|id| {
                    stored(
                        id,
                        vec![
                            Value::UInt64(id),
                            Value::Int64(i64::try_from(dim_of(id)).expect("small")),
                            Value::Int64(qty_of(id)),
                            bonus_of(id).map_or(Value::Null, Value::Int64),
                        ],
                    )
                })
                .collect(),
        ),
        (
            2,
            "dims",
            &dims,
            (1..=DIMS)
                .map(|id| {
                    stored(
                        id,
                        vec![
                            Value::Int64(i64::try_from(id).expect("small")),
                            Value::Utf8(format!("r{}", id % 3)),
                        ],
                    )
                })
                .collect(),
        ),
        (
            3,
            "tags",
            &tags,
            (1..=TAGS)
                .map(|id| {
                    stored(
                        id,
                        vec![
                            Value::UInt64(id),
                            Value::Int64(i64::try_from((id - 1) % DIMS + 1).expect("small")),
                            Value::Utf8(format!("t{}", id % 4)),
                        ],
                    )
                })
                .collect(),
        ),
    ];
    for (id, name, schema, rows) in specs {
        let directory = tempfile::tempdir().expect("directory");
        let count = rows.len() as u64;
        let store = table(directory.path(), schema, rows);
        directories.push(directory);
        tables.push((TableId::new(id), store));
        entries.push(
            TableEntry::new(
                TableId::new(id),
                name,
                schema.clone(),
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
    fn bind(&self, sql: &str) -> BoundQuery {
        Binder::new(&self.catalog, Some("app"))
            .bind(&parse_statement(sql).expect("parse"))
            .expect("bind")
    }

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
        let physical = PhysicalPlanner::plan(
            Optimizer::optimize(LogicalPlanner::plan(self.bind(sql))),
            Collation::default(),
        )
        .expect("plan");
        let mut execution =
            Execution::start(physical, &provider, 256 * 1024 * 1024, Collation::default())
                .expect("start");
        let mut rows = Vec::new();
        while let Some(batch) = execution.next_batch().expect("batch") {
            for row in batch.selection().selected_rows() {
                rows.push(
                    (0..batch.columns().len())
                        .map(|column| {
                            match batch.column(column).and_then(|column| column.value(row)) {
                                Some(Value::Null) | None => "NULL".to_owned(),
                                Some(Value::Utf8(text)) => text.clone(),
                                Some(Value::Int64(number)) => number.to_string(),
                                Some(Value::UInt64(number)) => number.to_string(),
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

fn folded(query: &BoundQuery) -> bool {
    let source = &query.from[0];
    std::iter::once(&source.base)
        .chain(source.joins.iter().map(|join| &join.table))
        .any(|table| table.table_name == "facts" && table.input.is_some())
}

#[test]
fn a_join_to_a_dimension_folds_the_facts_first() {
    let fixture = fixture();
    let sql = "SELECT d.region, SUM(f.qty), COUNT(*), COUNT(f.bonus), MIN(f.qty), MAX(f.qty) \
               FROM facts f JOIN dims d ON f.dim = d.id WHERE f.qty > 3 \
               GROUP BY d.region ORDER BY d.region";
    assert!(folded(&fixture.bind(sql)), "the facts fold below the join");
    let mut expected = BTreeMap::<String, (i64, u64, u64, i64, i64)>::new();
    for id in (1..=FACTS).filter(|id| qty_of(*id) > 3) {
        let entry =
            expected
                .entry(format!("r{}", dim_of(id) % 3))
                .or_insert((0, 0, 0, i64::MAX, i64::MIN));
        entry.0 += qty_of(id);
        entry.1 += 1;
        entry.2 += u64::from(bonus_of(id).is_some());
        entry.3 = entry.3.min(qty_of(id));
        entry.4 = entry.4.max(qty_of(id));
    }
    let expected = expected
        .into_iter()
        .map(|(region, (sum, count, bonus, low, high))| {
            vec![
                region,
                sum.to_string(),
                count.to_string(),
                bonus.to_string(),
                low.to_string(),
                high.to_string(),
            ]
        })
        .collect::<Vec<_>>();
    assert_eq!(fixture.run(sql), expected);
}

#[test]
fn a_repeated_key_on_the_other_side_counts_each_match() {
    let fixture = fixture();
    let sql = "SELECT t.tag, SUM(f.qty), COUNT(*) FROM facts f JOIN tags t ON t.dim = f.dim \
               GROUP BY t.tag ORDER BY t.tag";
    assert!(folded(&fixture.bind(sql)), "the facts fold below the join");
    let mut expected = BTreeMap::<String, (i64, u64)>::new();
    for tag in 1..=TAGS {
        let dim = (tag - 1) % DIMS + 1;
        for id in (1..=FACTS).filter(|id| dim_of(*id) == dim) {
            let entry = expected.entry(format!("t{}", tag % 4)).or_default();
            entry.0 += qty_of(id);
            entry.1 += 1;
        }
    }
    let expected = expected
        .into_iter()
        .map(|(tag, (sum, count))| vec![tag, sum.to_string(), count.to_string()])
        .collect::<Vec<_>>();
    assert_eq!(fixture.run(sql), expected);
}

#[test]
fn shapes_the_fold_cannot_answer_stay_as_written() {
    let fixture = fixture();
    for sql in [
        // An outer join null-extends rows the partial counts cannot see.
        "SELECT d.region, COUNT(*) FROM dims d LEFT JOIN facts f ON f.dim = d.id GROUP BY d.region",
        // DISTINCT needs every value.
        "SELECT d.region, COUNT(DISTINCT f.qty) FROM facts f JOIN dims d ON f.dim = d.id \
         GROUP BY d.region",
        // Aggregates over both sides.
        "SELECT d.region, SUM(f.qty + d.id) FROM facts f JOIN dims d ON f.dim = d.id \
         GROUP BY d.region",
        // No GROUP BY: COUNT over nothing is 0, a re-folded SUM NULL.
        "SELECT COUNT(*) FROM facts f JOIN dims d ON f.dim = d.id",
        // A predicate across both sides needs the rows.
        "SELECT d.region, SUM(f.qty) FROM facts f JOIN dims d ON f.dim = d.id \
         WHERE f.qty > d.id GROUP BY d.region",
    ] {
        assert!(!folded(&fixture.bind(sql)), "{sql}");
    }
}
