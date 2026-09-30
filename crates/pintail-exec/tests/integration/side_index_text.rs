//! The side index over text columns must answer exactly what a full scan
//! answers under the comparison's collation: a case- and accent-insensitive
//! lookup returns every row equal to its literal under that collation, a
//! PAD SPACE collation ignores trailing spaces, a binary one does neither,
//! and NULLs never match. Checked with the index on and off, against a row
//! model judged by the engine's own comparator, from a fresh snapshot, with
//! changes in the memtable, after flushes wrote postings sections, after
//! compaction, and after a schema change.
//!
//! The measurement is `#[ignore]`d and runs with the memo off:
//! `PINTAIL_DISABLE_SETTLED_MEMO=1 cargo test --profile recovery -p pintail-exec
//! --test integration side_index_text:: -- --ignored --nocapture`.

use std::collections::BTreeMap;

use pintail_catalog::{
    CatalogSnapshot, DatabaseEntry, DatabaseId, TableEntry, TableId, TableStatistics,
};
use pintail_exec::collation::Collation;
use pintail_exec::{
    Execution, LogicalPlanner, Optimizer, PhysicalPlanner, SnapshotScanProvider,
    compare_collated_text,
};
use pintail_sql::{Binder, parse_statement};
use pintail_store::{StoreOptions, TableStore, override_side_index};
use pintail_types::{Column, DataType, KeyPart, PrimaryKey, StoredRow, TableSchema, Value};

const DATABASE_ID: DatabaseId = DatabaseId::new(1);
const TABLE_ID: TableId = TableId::new(1);
const ROWS: u64 = 120_000;
const NAMES: u64 = 997;

fn schema(version: u32, evolved: bool) -> TableSchema {
    let mut columns = vec![
        Column::new(1, "id", DataType::UInt64, false),
        Column::new(2, "label", DataType::Utf8, true)
            .with_collation(Some("utf8mb4_general_ci".into())),
        Column::new(3, "code", DataType::Utf8, true),
        Column::new(4, "tag", DataType::Utf8, true).with_collation(Some("utf8mb4_bin".into())),
    ];
    if evolved {
        columns.push(Column::new(6, "extra", DataType::Int64, true));
    } else {
        columns.push(Column::new(5, "amount", DataType::Int64, false));
    }
    TableSchema::new(version, columns).expect("schema")
}

/// One base name in several spellings that some collations fold together:
/// case, an accent, trailing spaces.
fn spelling(id: u64, salt: u64) -> Option<String> {
    if (id + salt).is_multiple_of(29) {
        return None;
    }
    let base = format!("name{}", (id * 31 + salt) % NAMES);
    Some(match (id + salt) % 6 {
        0 => base,
        1 => base.to_uppercase(),
        2 => format!("{base}  "),
        3 => base.replacen('a', "\u{e1}", 1),
        4 => format!("N{}", &base[1..]),
        _ => base.replacen('e', "\u{c9}", 1),
    })
}

/// Which of an item's text columns a probe reads.
type Pick = fn(&Item) -> Option<&String>;

#[derive(Clone, Debug)]
struct Item {
    label: Option<String>,
    code: Option<String>,
    tag: Option<String>,
}

fn initial(id: u64) -> Item {
    Item {
        label: spelling(id, 0),
        code: spelling(id, 1),
        tag: spelling(id, 2),
    }
}

fn text(value: Option<&String>) -> Value {
    value.map_or(Value::Null, |text| Value::Utf8(text.clone()))
}

fn row(id: u64, item: &Item, version: u64, deleted: bool, evolved: bool) -> StoredRow {
    StoredRow::new(
        PrimaryKey::new(vec![KeyPart::UInt64(id)]).expect("key"),
        vec![
            Value::UInt64(id),
            text(item.label.as_ref()),
            text(item.code.as_ref()),
            text(item.tag.as_ref()),
            if evolved {
                Value::Null
            } else {
                Value::Int64(i64::try_from(id % 13).expect("small"))
            },
        ],
        version,
        deleted,
    )
}

struct Fixture {
    _dir: tempfile::TempDir,
    store: TableStore,
    catalog: CatalogSnapshot,
    model: BTreeMap<u64, Item>,
    version: u64,
    evolved: bool,
}

impl Fixture {
    fn new() -> Self {
        let options = StoreOptions {
            background_compaction: false,
            ..StoreOptions::default()
        };
        let dir = tempfile::tempdir().expect("dir");
        let mut store = TableStore::open(dir.path(), schema(1, false), options).expect("open");
        let model = (1..=ROWS)
            .map(|id| (id, initial(id)))
            .collect::<BTreeMap<_, _>>();
        store
            .bulk_ingest_snapshot(
                model
                    .iter()
                    .map(|(id, item)| row(*id, item, 1, false, false))
                    .collect(),
            )
            .expect("ingest");
        Self {
            _dir: dir,
            store,
            catalog: catalog(schema(1, false)),
            model,
            version: 2,
            evolved: false,
        }
    }

    fn evolve(&mut self) {
        self.store.evolve_schema(schema(2, true)).expect("evolve");
        self.catalog = catalog(schema(2, true));
        self.evolved = true;
    }

    /// Moves rows into and out of the probed names (in other spellings),
    /// to NULL, deletes some, and appends new ones.
    fn change(&mut self, round: u64) {
        let mut changes = Vec::new();
        let ids = self.model.keys().copied().collect::<Vec<_>>();
        for id in ids {
            if id % 97 != round % 97 && id % 89 != 3 {
                continue;
            }
            self.version += 1;
            if id % 5 == 0 {
                let old = self.model.remove(&id).expect("row");
                changes.push(row(id, &old, self.version, true, self.evolved));
                continue;
            }
            let item = Item {
                label: (id % 7 != 0)
                    .then(|| ["NAME17", "n\u{c1}me17 ", "name18"][(id % 3) as usize].to_owned()),
                code: Some(["NAME5", "name5  ", "n\u{e1}me5"][(id % 3) as usize].to_owned()),
                tag: (id % 2 == 0).then(|| "name17".to_owned()),
            };
            changes.push(row(id, &item, self.version, false, self.evolved));
            self.model.insert(id, item);
        }
        for id in ROWS + round * 1_000..ROWS + round * 1_000 + 300 {
            self.version += 1;
            let item = initial(id + round);
            changes.push(row(id, &item, self.version, false, self.evolved));
            self.model.insert(id, item);
        }
        for batch in changes.chunks(2_000) {
            self.store.ingest_cdc(batch.to_vec()).expect("change batch");
        }
    }

    fn run(&self, sql: &str, index: bool) -> Vec<Vec<Value>> {
        override_side_index(Some(index));
        let snapshot = self.store.snapshot();
        let provider =
            SnapshotScanProvider::new([(DATABASE_ID, TABLE_ID, &snapshot)]).expect("provider");
        let statement = parse_statement(sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
        let bound = Binder::new(&self.catalog, Some("app"))
            .bind(&statement)
            .unwrap_or_else(|error| panic!("{sql}: {error}"));
        let logical = Optimizer::optimize(LogicalPlanner::plan(bound));
        let physical = PhysicalPlanner::plan(logical, Collation::default()).expect("plan");
        let mut execution =
            Execution::start(physical, &provider, 512 * 1024 * 1024, Collation::default())
                .expect("start");
        let mut rows = Vec::new();
        while let Some(batch) = execution
            .next_batch()
            .unwrap_or_else(|error| panic!("{sql}: {error}"))
        {
            for index in batch.selection().selected_rows() {
                rows.push(
                    batch
                        .columns()
                        .iter()
                        .map(|column| column.value(index).cloned().expect("value"))
                        .collect::<Vec<_>>(),
                );
            }
        }
        override_side_index(None);
        rows
    }

    fn expect(&self, pick: Pick, literals: &[&str], collation: Collation) -> Vec<u64> {
        self.model
            .iter()
            .filter(|(_, item)| {
                pick(item).is_some_and(|value| {
                    literals.iter().any(|literal| {
                        compare_collated_text(value, literal, collation)
                            == std::cmp::Ordering::Equal
                    })
                })
            })
            .map(|(id, _)| *id)
            .collect()
    }

    fn check(&self, stage: &str) {
        let ids = |rows: Vec<Vec<Value>>| {
            rows.iter()
                .map(|row| match &row[0] {
                    Value::UInt64(id) => *id,
                    other => panic!("not an id: {other:?}"),
                })
                .collect::<Vec<_>>()
        };
        let cases: [(&str, Pick, &[&str], Collation); 5] = [
            (
                "SELECT id, code FROM items WHERE label = 'NAME17' ORDER BY id",
                |item| item.label.as_ref(),
                &["NAME17"],
                Collation::Utf8mb4GeneralCi,
            ),
            (
                "SELECT id, tag FROM items WHERE label IN ('n\u{e1}me300', 'name18  ', 'nope') ORDER BY id",
                |item| item.label.as_ref(),
                &["n\u{e1}me300", "name18  ", "nope"],
                Collation::Utf8mb4GeneralCi,
            ),
            (
                "SELECT id, label FROM items WHERE code = 'N\u{c1}ME5' ORDER BY id",
                |item| item.code.as_ref(),
                &["N\u{c1}ME5"],
                Collation::Utf8mb40900AiCi,
            ),
            (
                "SELECT id, label FROM items WHERE code IN ('name5', 'name6  ') ORDER BY id",
                |item| item.code.as_ref(),
                &["name5", "name6  "],
                Collation::Utf8mb40900AiCi,
            ),
            (
                "SELECT id, code FROM items WHERE tag = 'name17' ORDER BY id",
                |item| item.tag.as_ref(),
                &["name17"],
                Collation::Utf8mb4Bin,
            ),
        ];
        for (sql, pick, literals, collation) in cases {
            let expected = self.expect(pick, literals, collation);
            assert!(
                !expected.is_empty() || literals.len() > 1,
                "{stage}: {sql} matches nothing"
            );
            for index in [false, true] {
                assert_eq!(
                    ids(self.run(sql, index)),
                    expected,
                    "{stage}, index {index}: {sql}"
                );
            }
        }
        // An explicit COLLATE decides the comparison, whatever the column's.
        let sql =
            "SELECT id, code FROM items WHERE label = 'name17' COLLATE utf8mb4_bin ORDER BY id";
        let expected = self.expect(
            |item| item.label.as_ref(),
            &["name17"],
            Collation::Utf8mb4Bin,
        );
        for index in [false, true] {
            assert_eq!(
                ids(self.run(sql, index)),
                expected,
                "{stage}, index {index}: {sql}"
            );
        }
    }
}

fn catalog(schema: TableSchema) -> CatalogSnapshot {
    let entry = TableEntry::new(
        TABLE_ID,
        "items",
        schema,
        TableStatistics::with_row_count(ROWS),
    )
    .expect("entry")
    .with_key_columns([1])
    .expect("key");
    let database = DatabaseEntry::new(DATABASE_ID, "app", [entry]).expect("database");
    CatalogSnapshot::new([database]).expect("catalog")
}

#[test]
fn a_text_side_index_lookup_answers_under_the_comparison_collation() {
    let mut fixture = Fixture::new();
    fixture.check("snapshot");
    fixture.change(1);
    fixture.check("changes in the memtable");
    fixture.store.flush().expect("flush");
    fixture.check("flushed over the snapshot");
    fixture.change(2);
    fixture.store.flush().expect("flush");
    fixture.store.compact().expect("compact");
    fixture.check("compacted");
    fixture.evolve();
    fixture.check("schema changed");
    fixture.change(3);
    fixture.store.flush().expect("flush");
    fixture.store.compact().expect("compact");
    fixture.check("compacted under the changed schema");
}

#[test]
#[ignore = "measurement: run with --ignored --nocapture"]
fn measure_text_side_index_lookups() {
    let mut fixture = Fixture::new();
    for sql in [
        "SELECT id, code FROM items WHERE label = 'NAME17' ORDER BY id",
        "SELECT id, label FROM items WHERE code IN ('name5', 'name6', 'NAME7')",
        "SELECT COUNT(*), SUM(amount) FROM items WHERE code = 'n\u{e1}me9' AND label IS NOT NULL",
    ] {
        for round in ["built", "persisted"] {
            for index in [false, true] {
                fixture.run(sql, index);
                let mut samples = (0..9)
                    .map(|_| {
                        let started = std::time::Instant::now();
                        fixture.run(sql, index);
                        started.elapsed().as_secs_f64() * 1_000.0
                    })
                    .collect::<Vec<_>>();
                samples.sort_by(f64::total_cmp);
                println!(
                    "{:>8.2} ms median {:>8.2} ms min  {round} index={index}  {sql}",
                    samples[samples.len() / 2],
                    samples[0]
                );
            }
            // A compaction writes the used columns' postings sections.
            fixture.store.compact().expect("compact");
        }
    }
    let (entries, bytes, build_us) = pintail_store::side_index_totals();
    println!("side index: {entries} entries, {bytes} bytes, built in {build_us} us");
}
