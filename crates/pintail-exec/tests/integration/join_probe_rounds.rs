//! A chain of joins that fans out below an aggregate probes a round of
//! probe batches on the pool at once, each batch on its own worker. The
//! answer must be the one the serial probe gives, row for row and in the
//! same order: a LEFT join with a residual ON predicate, a composite key,
//! probe rows that find nothing, and a fan-out wide enough that one probe
//! batch yields several output batches.
//!
//! The measurement is `#[ignore]`d:
//! `cargo test --profile recovery -p pintail-exec --test integration join_probe_rounds::
//! -- --ignored --nocapture`.

use pintail_catalog::{
    CatalogSnapshot, DatabaseEntry, DatabaseId, TableEntry, TableId, TableStatistics,
};
use pintail_exec::collation::Collation;
use pintail_exec::{Execution, LogicalPlanner, Optimizer, PhysicalPlanner, SnapshotScanProvider};
use pintail_sql::{Binder, parse_statement};
use pintail_store::{StoreOptions, TableStore};
use pintail_types::{Column, DataType, KeyPart, PrimaryKey, StoredRow, TableSchema, Value};

fn schema(columns: &[(&str, DataType)]) -> TableSchema {
    TableSchema::new(
        1,
        columns
            .iter()
            .zip(1_u32..)
            .map(|((name, data_type), id)| Column::new(id, *name, *data_type, id != 1))
            .collect(),
    )
    .expect("schema")
}

fn members_schema() -> TableSchema {
    schema(&[
        ("id", DataType::UInt64),
        ("team", DataType::UInt64),
        ("since", DataType::DateTime64 { fsp: 0 }),
    ])
}

fn steps_schema() -> TableSchema {
    schema(&[("id", DataType::UInt64), ("team", DataType::UInt64)])
}

fn sessions_schema() -> TableSchema {
    schema(&[
        ("id", DataType::UInt64),
        ("step", DataType::UInt64),
        ("held", DataType::DateTime64 { fsp: 0 }),
    ])
}

fn marks_schema() -> TableSchema {
    schema(&[
        ("id", DataType::UInt64),
        ("session", DataType::UInt64),
        ("member", DataType::UInt64),
        ("present", DataType::Int64),
    ])
}

fn stamp(day: u64) -> Value {
    Value::Utf8(format!(
        "2026-{:02}-{:02} 10:00:00",
        1 + day / 28 % 12,
        1 + day % 28
    ))
}

struct Fixture {
    _directory: tempfile::TempDir,
    tables: Vec<TableStore>,
    catalog: CatalogSnapshot,
}

impl Fixture {
    #[allow(clippy::too_many_lines)]
    fn new(members: u64) -> Self {
        let teams = (members / 40).max(1);
        let steps = teams * 6;
        let sessions = steps * 5;
        let directory = tempfile::tempdir().expect("directory");
        let data: [(&str, TableSchema, Vec<Vec<Value>>); 4] = [
            (
                "members",
                members_schema(),
                (0..members)
                    .map(|id| {
                        vec![
                            Value::UInt64(id),
                            if id % 29 == 0 {
                                Value::Null
                            } else {
                                Value::UInt64(id % teams)
                            },
                            if id % 3 == 0 {
                                Value::Null
                            } else {
                                stamp(id % 200)
                            },
                        ]
                    })
                    .collect(),
            ),
            (
                "steps",
                steps_schema(),
                (0..steps)
                    .map(|id| vec![Value::UInt64(id), Value::UInt64(id % teams)])
                    .collect(),
            ),
            (
                "sessions",
                sessions_schema(),
                (0..sessions)
                    .map(|id| {
                        vec![
                            Value::UInt64(id),
                            Value::UInt64(id % steps),
                            stamp(id * 7 % 330),
                        ]
                    })
                    .collect(),
            ),
            (
                "marks",
                marks_schema(),
                (0..members * 4)
                    .map(|id| {
                        vec![
                            Value::UInt64(id),
                            Value::UInt64(id * 13 % sessions),
                            Value::UInt64(id / 4),
                            Value::Int64(i64::from(id % 5 != 0)),
                        ]
                    })
                    .collect(),
            ),
        ];
        let mut tables = Vec::new();
        let mut entries = Vec::new();
        for ((name, schema, rows), id) in data.into_iter().zip(1_u64..) {
            let count = rows.len() as u64;
            let mut table = TableStore::open(
                directory.path().join(name),
                schema.clone(),
                StoreOptions::default(),
            )
            .expect("table");
            table
                .bulk_ingest_snapshot(
                    rows.into_iter()
                        .zip(0_u64..)
                        .map(|(values, key)| {
                            StoredRow::new(
                                PrimaryKey::new(vec![KeyPart::UInt64(key)]).expect("key"),
                                values,
                                key + 1,
                                false,
                            )
                        })
                        .collect(),
                )
                .expect("rows");
            tables.push(table);
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
        Self {
            _directory: directory,
            tables,
            catalog: CatalogSnapshot::new([
                DatabaseEntry::new(DatabaseId::new(1), "app", entries).expect("database")
            ])
            .expect("catalog"),
        }
    }

    fn run(&self, sql: &str) -> Vec<String> {
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
            Execution::start(physical, &provider, 1 << 31, Collation::default()).expect("start");
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
        rows
    }
}

/// `work` on a pool of one thread, where every probe is serial. The fixture
/// is built there too: a table store cannot be shared across threads.
fn on_one_thread<T: Send>(work: impl FnOnce() -> T + Send) -> T {
    rayon::ThreadPoolBuilder::new()
        .num_threads(1)
        .build()
        .expect("pool")
        .install(work)
}

/// Rows straight out of the join chain, in the order it emits them.
const ROWS: &str = "SELECT m.id, st.id, se.id, mk.id, mk.present FROM members m \
     LEFT JOIN steps st ON st.team = m.team \
     LEFT JOIN sessions se ON se.step = st.id AND se.held >= COALESCE(m.since, '1900-01-01') \
     LEFT JOIN marks mk ON mk.session = se.id AND mk.member = m.id";

/// The chain under an aggregate that counts across the fan-out.
const GROUPED: &str = "SELECT m.team, m.id, COUNT(DISTINCT st.id), COUNT(DISTINCT se.id), \
     SUM(CASE WHEN mk.id IS NOT NULL AND mk.present = 1 THEN 1 ELSE 0 END), COUNT(*) \
     FROM members m \
     LEFT JOIN steps st ON st.team = m.team \
     LEFT JOIN sessions se ON se.step = st.id AND se.held >= COALESCE(m.since, '1900-01-01') \
     LEFT JOIN marks mk ON mk.session = se.id AND mk.member = m.id \
     GROUP BY m.team, m.id ORDER BY m.id";

#[test]
fn a_round_on_the_pool_answers_as_the_serial_probe_does() {
    let fixture = Fixture::new(24_000);
    for sql in [ROWS, GROUPED] {
        let serial = on_one_thread(|| Fixture::new(24_000).run(sql));
        let pooled = fixture.run(sql);
        assert!(serial.len() >= 24_000, "{}", serial.len());
        assert_eq!(serial.len(), pooled.len(), "{sql}");
        assert!(serial == pooled, "{sql}: the rows or their order moved");
    }
}

#[test]
#[ignore = "measurement"]
fn measure_a_fanning_join_chain_under_an_aggregate() {
    const MEMBERS: u64 = 200_000;
    let timed = |fixture: &Fixture| {
        let started = std::time::Instant::now();
        let rows = fixture.run(GROUPED).len();
        (rows, started.elapsed())
    };
    let fixture = Fixture::new(MEMBERS);
    for _ in 0..2 {
        let (rows, serial) = on_one_thread(|| timed(&Fixture::new(MEMBERS)));
        let (_, pooled) = timed(&fixture);
        println!(
            "{rows} rows: serial {serial:?}, pool of {} {pooled:?}",
            rayon::current_num_threads()
        );
    }
}

/// The chain with the residual spelled without COALESCE: a probe row with no
/// start date keeps the sessions on or after the literal, any other the
/// sessions on or after its date.
const GROUPED_EXPANDED: &str = "SELECT m.team, m.id, COUNT(DISTINCT st.id), \
     COUNT(DISTINCT se.id), \
     SUM(CASE WHEN mk.id IS NOT NULL AND mk.present = 1 THEN 1 ELSE 0 END), COUNT(*) \
     FROM members m \
     LEFT JOIN steps st ON st.team = m.team \
     LEFT JOIN sessions se ON se.step = st.id \
       AND (se.held >= m.since OR (m.since IS NULL AND se.held >= '1900-01-01 00:00:00')) \
     LEFT JOIN marks mk ON mk.session = se.id AND mk.member = m.id \
     GROUP BY m.team, m.id ORDER BY m.id";

/// A DATETIME compared with `COALESCE(datetime, 'literal')` compares as a
/// DATETIME, whether the literal is a date or a datetime, and a residual
/// reading two of the joined columns answers as one reading them all.
#[test]
fn a_datetime_coalesce_residual_answers_as_its_expanded_form() {
    let fixture = Fixture::new(6_000);
    let expanded = fixture.run(GROUPED_EXPANDED);
    assert!(expanded.len() >= 6_000, "{}", expanded.len());
    assert!(fixture.run(GROUPED) == expanded, "a date literal");
    let datetime_literal = GROUPED.replace("'1900-01-01'", "'1900-01-01 00:00:00'");
    assert!(
        fixture.run(&datetime_literal) == expanded,
        "a datetime literal"
    );
    // A literal after every session keeps only the probe rows with a date.
    let late = fixture.run(
        "SELECT COUNT(*) FROM members m JOIN sessions se ON se.step = m.team \
         AND se.held >= COALESCE(m.since, '2099-01-01')",
    );
    let dated = fixture.run(
        "SELECT COUNT(*) FROM members m JOIN sessions se ON se.step = m.team \
         AND se.held >= m.since",
    );
    assert_eq!(late, dated);
}

#[test]
#[ignore = "measurement"]
fn measure_a_datetime_coalesce_residual() {
    let fixture = Fixture::new(200_000);
    for _ in 0..3 {
        let started = std::time::Instant::now();
        let rows = fixture.run(GROUPED).len();
        println!("{rows} rows in {:?}", started.elapsed());
    }
}
