//! `GROUP BY k ORDER BY k LIMIT n`, grouping by nothing but a leading key
//! column of the table that drives a chain of joins, each reading its
//! other table by that table's primary key. The driving scan arrives in
//! key order and the joins keep it, so each group is a run of equal keys
//! and the first n runs are the answer: the scan and the joins stop there
//! instead of grouping every joined row and sorting the groups. The answer,
//! with offsets, keys whose every row the joins drop, and inner and outer
//! joins, must be the one grouping and sorting everything gives.

use pintail_catalog::{
    CatalogSnapshot, DatabaseEntry, DatabaseId, TableEntry, TableId, TableStatistics,
};
use pintail_exec::collation::Collation;
use pintail_exec::{Execution, LogicalPlanner, Optimizer, PhysicalPlanner, SnapshotScanProvider};
use pintail_sql::{Binder, parse_statement};
use pintail_store::{StoreOptions, TableStore};
use pintail_types::{Column, DataType, KeyPart, PrimaryKey, StoredRow, TableSchema, Value};

const OWNERS: u64 = 4_000;
const GROUPS: u64 = 300;
const SITES: u64 = 40;

fn schema(columns: Vec<Column>) -> TableSchema {
    TableSchema::new(1, columns).expect("schema")
}

fn memberships_schema() -> TableSchema {
    schema(vec![
        Column::new(1, "owner", DataType::UInt64, false),
        Column::new(2, "grp", DataType::UInt64, false),
        Column::new(3, "state", DataType::Utf8, false),
    ])
}

fn owners_schema() -> TableSchema {
    schema(vec![
        Column::new(1, "id", DataType::UInt64, false),
        Column::new(2, "label", DataType::Utf8, false),
    ])
}

fn groups_schema() -> TableSchema {
    schema(vec![
        Column::new(1, "id", DataType::UInt64, false),
        Column::new(2, "site", DataType::UInt64, false),
    ])
}

fn sites_schema() -> TableSchema {
    schema(vec![
        Column::new(1, "id", DataType::UInt64, false),
        Column::new(2, "kind", DataType::Utf8, true),
    ])
}

struct Fixture {
    _directory: tempfile::TempDir,
    tables: Vec<TableStore>,
    catalog: CatalogSnapshot,
}

fn key(parts: &[u64]) -> PrimaryKey {
    PrimaryKey::new(parts.iter().map(|part| KeyPart::UInt64(*part)).collect()).expect("key")
}

impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().expect("directory");
        let open = |name: &str, schema: TableSchema, rows: Vec<StoredRow>| {
            let mut table =
                TableStore::open(directory.path().join(name), schema, StoreOptions::default())
                    .expect("table");
            table.bulk_ingest_snapshot(rows).expect("rows");
            table
        };
        let mut memberships = Vec::new();
        for owner in 0..OWNERS {
            // Some owners hold no row at all, some several.
            for slot in 0..(owner % 4) {
                let grp = (owner * 7 + slot * 13) % GROUPS;
                let state =
                    ["open", "held", "closed"][usize::try_from((owner + slot) % 3).expect("slot")];
                memberships.push(StoredRow::new(
                    key(&[owner, grp]),
                    vec![
                        Value::UInt64(owner),
                        Value::UInt64(grp),
                        Value::Utf8(state.to_owned()),
                    ],
                    1,
                    false,
                ));
            }
        }
        memberships.sort_by(|left, right| left.key().cmp(right.key()));
        memberships.dedup_by(|left, right| left.key() == right.key());
        let count = u64::try_from(memberships.len()).expect("rows");
        // Every tenth owner is missing, so its rows fall out of an inner join.
        let owners = (0..OWNERS)
            .filter(|owner| owner % 10 != 3)
            .map(|owner| {
                StoredRow::new(
                    key(&[owner]),
                    vec![Value::UInt64(owner), Value::Utf8(format!("o{owner}"))],
                    1,
                    false,
                )
            })
            .collect::<Vec<_>>();
        let groups = (0..GROUPS)
            .map(|grp| {
                StoredRow::new(
                    key(&[grp]),
                    vec![Value::UInt64(grp), Value::UInt64(grp % SITES)],
                    1,
                    false,
                )
            })
            .collect::<Vec<_>>();
        let sites = (0..SITES)
            .map(|site| {
                let kind = match site % 3 {
                    0 => Value::Null,
                    1 => Value::Utf8("far".to_owned()),
                    _ => Value::Utf8("near".to_owned()),
                };
                StoredRow::new(key(&[site]), vec![Value::UInt64(site), kind], 1, false)
            })
            .collect::<Vec<_>>();
        let entry = |id: u64, name: &str, schema: TableSchema, rows: u64, keys: &[u32]| {
            TableEntry::new(
                TableId::new(id),
                name,
                schema,
                TableStatistics::with_row_count(rows),
            )
            .expect("entry")
            .with_key_columns(keys.iter().copied())
            .expect("key")
        };
        let entries = [
            entry(1, "memberships", memberships_schema(), count, &[1, 2]),
            entry(2, "owners", owners_schema(), OWNERS, &[1]),
            entry(3, "groups", groups_schema(), GROUPS, &[1]),
            entry(4, "sites", sites_schema(), SITES, &[1]),
        ];
        let tables = vec![
            open("memberships", memberships_schema(), memberships),
            open("owners", owners_schema(), owners),
            open("groups", groups_schema(), groups),
            open("sites", sites_schema(), sites),
        ];
        Self {
            _directory: directory,
            tables,
            catalog: CatalogSnapshot::new([
                DatabaseEntry::new(DatabaseId::new(1), "app", entries).expect("database")
            ])
            .expect("catalog"),
        }
    }

    /// Rows, the most rows any join emitted, and every operator's label.
    fn run(&self, sql: &str) -> (Vec<String>, u64, Vec<String>) {
        let snapshots = self
            .tables
            .iter()
            .map(TableStore::snapshot)
            .collect::<Vec<_>>();
        let provider = SnapshotScanProvider::new(
            snapshots
                .iter()
                .zip(1_u64..)
                .map(|(snapshot, id)| (DatabaseId::new(1), TableId::new(id), snapshot)),
        )
        .expect("provider");
        let bound = Binder::new(&self.catalog, Some("app"))
            .bind(&parse_statement(sql).expect("parse"))
            .unwrap_or_else(|error| panic!("bind {sql}: {error}"));
        let physical = PhysicalPlanner::plan(
            Optimizer::optimize(LogicalPlanner::plan(bound)),
            Collation::default(),
        )
        .expect("plan");
        let mut execution =
            Execution::start_profiled(physical, &provider, 1 << 30, None, Collation::default())
                .expect("start");
        let mut rows = Vec::new();
        while let Some(batch) = execution
            .next_batch()
            .unwrap_or_else(|error| panic!("{sql}: {error}"))
        {
            for row in batch.selection().selected_rows() {
                rows.push(format!(
                    "{:?}",
                    (0..batch.columns().len())
                        .map(|column| batch
                            .column(column)
                            .and_then(|column| column.value(row))
                            .cloned())
                        .collect::<Vec<_>>()
                ));
            }
        }
        let profile = execution.profile().expect("a profile");
        let joined = profile
            .operators
            .iter()
            .filter(|node| node.label.contains("Join"))
            .map(|node| node.rows)
            .max()
            .unwrap_or(0);
        let labels = profile
            .operators
            .iter()
            .map(|node| node.label.clone())
            .collect();
        (rows, joined, labels)
    }
}

/// `$KEY` is the sort key: `m.owner` is the grouping column, `m.owner + 0`
/// the same order with nothing to stream.
const QUERY: &str = "SELECT m.owner FROM memberships m \
     $JOIN owners o ON o.id = m.owner \
     INNER JOIN `groups` g ON g.id = m.grp \
     INNER JOIN sites s ON s.id = g.site \
     WHERE m.state IN ('open', 'held') AND (s.kind IS NULL OR s.kind <> 'far') \
     GROUP BY m.owner ORDER BY $KEY LIMIT 25 OFFSET $OFFSET";

#[test]
fn groups_in_key_order_stop_at_the_limit_and_the_answer_holds() {
    let fixture = Fixture::new();
    for join in ["INNER JOIN", "LEFT JOIN"] {
        for offset in ["0", "25", "900", "3000"] {
            let query = QUERY.replace("$JOIN", join).replace("$OFFSET", offset);
            let (runs, joined, labels) = fixture.run(&query.replace("$KEY", "m.owner"));
            let (whole, whole_joined, _) = fixture.run(&query.replace("$KEY", "m.owner + 0"));
            assert_eq!(runs, whole, "{join} offset {offset}");
            assert!(
                labels.iter().any(|label| label == "KeyRuns"),
                "{join} offset {offset}: {labels:?}"
            );
            if offset == "0" {
                assert_eq!(runs.len(), 25);
                assert!(
                    joined * 10 < whole_joined,
                    "{join}: {joined} rows joined against {whole_joined}"
                );
            }
        }
    }
}

#[test]
fn an_aggregate_or_a_descending_order_keeps_the_grouping() {
    let fixture = Fixture::new();
    for query in [
        "SELECT m.owner, COUNT(*) FROM memberships m INNER JOIN owners o ON o.id = m.owner \
         GROUP BY m.owner ORDER BY m.owner LIMIT 5",
        "SELECT m.owner FROM memberships m INNER JOIN owners o ON o.id = m.owner \
         GROUP BY m.owner ORDER BY m.owner DESC LIMIT 5",
    ] {
        let (rows, _, labels) = fixture.run(query);
        assert_eq!(rows.len(), 5);
        assert!(!labels.iter().any(|label| label == "KeyRuns"), "{labels:?}");
    }
}
