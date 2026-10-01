//! A small dimension table that is being written to: its rows sit in
//! segments and its newest changes - updates, deletes and inserts - in the
//! memtable, the state every replica of a live source answers from.
//!
//! Each answer is checked against the same final rows loaded whole into a
//! second store with no memtable, so the scan's visibility (newest version
//! wins, deletes hide, inserts appear, a column added after the segments
//! were written reads NULL there) is asserted, not assumed.
//!
//! `memtable_dimension_scan_cost` is `#[ignore]`d: measurement, not
//! assertion. Run with `PINTAIL_DISABLE_SETTLED_MEMO=1 cargo test --profile
//! recovery -p pintail-exec --test integration memtable_dimension_scan:: --
//! --ignored --nocapture`; `DIMENSION_SIZES`, `DIMENSION_PERCENTS`,
//! `DIMENSION_QUERY` and `DIMENSION_RUNS` narrow it, and `DIMENSION_PROFILE=1`
//! prints each run's operator profile.

use std::collections::BTreeMap;

use pintail_catalog::{
    CatalogSnapshot, DatabaseEntry, DatabaseId, TableEntry, TableId, TableStatistics,
};
use pintail_exec::collation::Collation;
use pintail_exec::{Execution, LogicalPlanner, Optimizer, PhysicalPlanner, SnapshotScanProvider};
use pintail_sql::{Binder, parse_statement};
use pintail_store::{StoreOptions, TableStore};
use pintail_types::{Column, DataType, KeyPart, PrimaryKey, StoredRow, TableSchema, Value};

const ORDERS: u64 = 400_000;
const REGIONS: [&str; 6] = ["north", "south", "east", "west", "coast", "inland"];

fn shop_schema(altered: bool) -> TableSchema {
    let mut columns = vec![
        Column::new(1, "id", DataType::UInt64, false),
        Column::new(2, "region", DataType::Utf8, false),
        Column::new(3, "label", DataType::Utf8, false),
        Column::new(4, "rate", DataType::Int64, false),
    ];
    if altered {
        columns.push(Column::new(5, "grade", DataType::Int64, true));
    }
    TableSchema::new(if altered { 2 } else { 1 }, columns).expect("schema")
}

fn order_schema() -> TableSchema {
    TableSchema::new(
        1,
        vec![
            Column::new(1, "id", DataType::UInt64, false),
            Column::new(2, "shop_id", DataType::UInt64, false),
            Column::new(3, "cents", DataType::Int64, false),
        ],
    )
    .expect("schema")
}

#[derive(Clone)]
struct Shop {
    region: &'static str,
    label: String,
    rate: i64,
    grade: Option<i64>,
}

impl Shop {
    fn original(id: u64) -> Self {
        Self {
            region: REGIONS[usize::try_from(id % 6).expect("small")],
            label: format!("shop {id:08}"),
            rate: i64::try_from(id % 1_000).expect("small"),
            grade: None,
        }
    }

    fn revised(id: u64) -> Self {
        Self {
            region: REGIONS[usize::try_from((id + 1) % 6).expect("small")],
            label: format!("shop {id:08} revised"),
            rate: i64::try_from(id % 997).expect("small") + 5_000,
            grade: (!id.is_multiple_of(3)).then(|| i64::try_from(id % 11).expect("small")),
        }
    }

    fn row(&self, id: u64, altered: bool, version: u64, deleted: bool) -> StoredRow {
        let mut values = vec![
            Value::UInt64(id),
            Value::Utf8(self.region.to_owned()),
            Value::Utf8(self.label.clone()),
            Value::Int64(self.rate),
        ];
        if altered {
            values.push(self.grade.map_or(Value::Null, Value::Int64));
        }
        StoredRow::new(
            PrimaryKey::new(vec![KeyPart::UInt64(id)]).expect("key"),
            values,
            version,
            deleted,
        )
    }
}

fn order_row(id: u64, shops: u64) -> StoredRow {
    let signed = i64::try_from(id).expect("small");
    StoredRow::new(
        PrimaryKey::new(vec![KeyPart::UInt64(id)]).expect("key"),
        vec![
            Value::UInt64(id),
            Value::UInt64(id.wrapping_mul(7_919) % shops + 1),
            Value::Int64(signed.wrapping_mul(31) % 10_000),
        ],
        1,
        false,
    )
}

struct Fixture {
    _directory: tempfile::TempDir,
    /// Shops in segments with changes in the memtable, then orders.
    live: Vec<TableStore>,
    /// The same final shops loaded whole, then the same orders.
    settled: Vec<TableStore>,
    catalog: CatalogSnapshot,
    updated: u64,
    /// The highest key, inserted through the change path when any was.
    last: u64,
}

impl Fixture {
    /// `shops` rows loaded as a snapshot, then `percent` of them changed
    /// through the change path - a third updated, a third deleted, a third
    /// inserted past the last key - and left in the memtable. `altered`
    /// adds a nullable column between the snapshot and the changes.
    fn new(shops: u64, percent: u64, altered: bool) -> Self {
        let directory = tempfile::tempdir().expect("directory");
        let open = |name: &str, schema: TableSchema| {
            TableStore::open(directory.path().join(name), schema, StoreOptions::default())
                .expect("store")
        };
        let mut model = (1..=shops)
            .map(|id| (id, Shop::original(id)))
            .collect::<BTreeMap<_, _>>();
        let mut live = open("live-shops", shop_schema(false));
        live.bulk_ingest_snapshot(
            model
                .iter()
                .map(|(id, shop)| shop.row(*id, false, 1, false))
                .collect(),
        )
        .expect("snapshot");
        if altered {
            live.evolve_schema(shop_schema(true)).expect("evolve");
        }
        let each = shops * percent / 300;
        let mut changes = Vec::new();
        let mut version = 2;
        let mut updated = 1;
        let mut last = shops;
        if let Some(step) = shops.checked_div(each) {
            for index in 0..each {
                let id = 1 + index * step;
                updated = id;
                let shop = Shop::revised(id);
                changes.push(shop.row(id, altered, version, false));
                model.insert(id, shop);
                version += 1;
                let gone = 2 + index * step;
                changes.push(Shop::original(gone).row(gone, altered, version, true));
                model.remove(&gone);
                version += 1;
                let fresh = shops + 1 + index;
                last = fresh;
                let shop = Shop::revised(fresh);
                changes.push(shop.row(fresh, altered, version, false));
                model.insert(fresh, shop);
                version += 1;
            }
        }
        for batch in changes.chunks(2_000) {
            live.ingest_cdc(batch.to_vec()).expect("changes");
        }
        let mut settled = open("settled-shops", shop_schema(altered));
        settled
            .bulk_ingest_snapshot(
                model
                    .iter()
                    .map(|(id, shop)| shop.row(*id, altered, 1, false))
                    .collect(),
            )
            .expect("settled");
        let key_space = shops + each;
        let orders = |name: &str| {
            let mut store = open(name, order_schema());
            store
                .bulk_ingest_snapshot((1..=ORDERS).map(|id| order_row(id, key_space)).collect())
                .expect("orders");
            store
        };
        let live = vec![live, orders("live-orders")];
        let settled = vec![settled, orders("settled-orders")];
        let entries = [
            ("shops", shop_schema(altered), shops),
            ("orders", order_schema(), ORDERS),
        ]
        .into_iter()
        .enumerate()
        .map(|(index, (name, schema, rows))| {
            TableEntry::new(
                TableId::new(u64::try_from(index + 1).expect("table")),
                name,
                schema,
                TableStatistics::with_row_count(rows),
            )
            .expect("entry")
            .with_key_columns([1])
            .expect("key")
        })
        .collect::<Vec<_>>();
        Self {
            _directory: directory,
            live,
            settled,
            catalog: CatalogSnapshot::new([
                DatabaseEntry::new(DatabaseId::new(1), "app", entries).expect("database")
            ])
            .expect("catalog"),
            updated,
            last,
        }
    }

    fn run(&self, stores: &[TableStore], sql: &str) -> (Vec<Vec<Value>>, f64) {
        let snapshots = stores.iter().map(TableStore::snapshot).collect::<Vec<_>>();
        let provider = SnapshotScanProvider::new(snapshots.iter().enumerate().map(|(i, s)| {
            (
                DatabaseId::new(1),
                TableId::new(u64::try_from(i + 1).expect("table")),
                s,
            )
        }))
        .expect("provider");
        let bound = Binder::new(&self.catalog, Some("app"))
            .bind(&parse_statement(sql).expect("parse"))
            .unwrap_or_else(|error| panic!("bind {sql}: {error}"));
        let physical = PhysicalPlanner::plan(
            Optimizer::optimize(LogicalPlanner::plan(bound)),
            Collation::default(),
        )
        .expect("plan");
        let started = std::time::Instant::now();
        let profiled = std::env::var_os("DIMENSION_PROFILE").is_some();
        let mut execution = if profiled {
            Execution::start_profiled(physical, &provider, 1 << 30, None, Collation::default())
        } else {
            Execution::start(physical, &provider, 1 << 30, Collation::default())
        }
        .expect("start");
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
                        .map(|column| column.value_owned(row).expect("value"))
                        .collect(),
                );
            }
        }
        if profiled && let Some(profile) = execution.profile() {
            println!("{}", profile.render());
        }
        (rows, started.elapsed().as_secs_f64() * 1000.0)
    }

    fn queries(&self, altered: bool) -> Vec<(&'static str, String)> {
        let mut queries = vec![
            (
                "full scan",
                "SELECT COUNT(*), SUM(rate), MAX(label), MIN(region) FROM shops".to_owned(),
            ),
            (
                "filtered scan",
                "SELECT COUNT(*), SUM(rate), MAX(label) FROM shops WHERE region = 'north'"
                    .to_owned(),
            ),
            (
                "rows out",
                "SELECT id, label FROM shops WHERE rate BETWEEN 998 AND 5010 ORDER BY id"
                    .to_owned(),
            ),
            (
                "star join",
                "SELECT s.region, COUNT(*), SUM(o.cents) FROM orders o JOIN shops s \
                 ON o.shop_id = s.id GROUP BY s.region ORDER BY s.region"
                    .to_owned(),
            ),
            (
                "point",
                format!("SELECT label, rate FROM shops WHERE id = {}", self.updated),
            ),
        ];
        if altered {
            queries.push((
                "added column",
                "SELECT COUNT(grade), SUM(grade), COUNT(*) FROM shops WHERE grade IS NULL OR grade > 3"
                    .to_owned(),
            ));
        }
        queries
    }

    fn assert_exact(&self, altered: bool) {
        for (label, sql) in self.queries(altered) {
            let (live, _) = self.run(&self.live, &sql);
            let (settled, _) = self.run(&self.settled, &sql);
            assert_eq!(live, settled, "{label}: {sql}");
            assert!(!settled.is_empty(), "{label}: {sql}");
        }
        // Point lookups of an updated key, a deleted one, one the snapshot
        // left alone, the last inserted one and one past every key.
        for id in [
            self.updated,
            self.updated + 1,
            self.updated + 2,
            self.last,
            self.last + 1,
        ] {
            let sql = format!("SELECT id, label, rate FROM shops WHERE id = {id}");
            assert_eq!(
                self.run(&self.live, &sql).0,
                self.run(&self.settled, &sql).0,
                "{sql}"
            );
        }
    }
}

#[test]
fn a_written_dimension_answers_exactly() {
    for shops in [5_000, 70_000] {
        for percent in [0, 1, 10] {
            for altered in [false, true] {
                Fixture::new(shops, percent, altered).assert_exact(altered);
            }
        }
    }
}

#[test]
#[ignore = "measurement, not an assertion"]
fn memtable_dimension_scan_cost() {
    let list = |name: &str, default: &[u64]| {
        std::env::var(name).map_or_else(
            |_| default.to_vec(),
            |list| {
                list.split(',')
                    .map(|item| item.parse().expect("number"))
                    .collect::<Vec<u64>>()
            },
        )
    };
    let sizes = list(
        "DIMENSION_SIZES",
        &[5_000, 20_000, 60_000, 200_000, 2_000_000],
    );
    let percents = list("DIMENSION_PERCENTS", &[0, 1, 10]);
    let runs = usize::try_from(list("DIMENSION_RUNS", &[9])[0]).expect("runs");
    let only = std::env::var("DIMENSION_QUERY").ok();
    for shops in sizes {
        for percent in percents.iter().copied() {
            let fixture = Fixture::new(shops, percent, false);
            fixture.assert_exact(false);
            for (label, sql) in fixture.queries(false) {
                if only.as_deref().is_some_and(|only| only != label) {
                    continue;
                }
                let mut times = (0..runs)
                    .map(|_| fixture.run(&fixture.live, &sql).1)
                    .collect::<Vec<_>>();
                times.sort_by(f64::total_cmp);
                println!(
                    "shops {shops:>9} changed {percent:>2}% {label:<14} median {:>8.2} ms  min {:>8.2} ms",
                    times[runs / 2],
                    times[0],
                );
            }
        }
    }
}
