//! `ORDER BY` a left table's columns `LIMIT n` over a chain of LEFT JOINs
//! reads only that table's first n rows through the joins: every left row
//! survives a LEFT JOIN with at least one output row, so the first n output
//! rows come from the first n left rows. The answer - ties, offsets and a
//! right side that matches a row twice included - must be the one the
//! whole join sorted would give.

use pintail_catalog::{
    CatalogSnapshot, DatabaseEntry, DatabaseId, TableEntry, TableId, TableStatistics,
};
use pintail_exec::collation::Collation;
use pintail_exec::{Execution, LogicalPlanner, Optimizer, PhysicalPlanner, SnapshotScanProvider};
use pintail_sql::{Binder, parse_statement};
use pintail_store::{StoreOptions, TableStore};
use pintail_types::{Column, DataType, KeyPart, PrimaryKey, StoredRow, TableSchema, Value};

const EVENTS: u64 = 60_000;
const PEOPLE: u64 = 500;

fn events_schema() -> TableSchema {
    TableSchema::new(
        1,
        vec![
            Column::new(1, "id", DataType::UInt64, false),
            Column::new(2, "actor", DataType::UInt64, true),
            Column::new(3, "subject", DataType::UInt64, true),
            Column::new(4, "at", DataType::DateTime64 { fsp: 0 }, false),
            Column::new(5, "note", DataType::Utf8, true),
        ],
    )
    .expect("events schema")
}

fn people_schema() -> TableSchema {
    TableSchema::new(
        1,
        vec![
            Column::new(1, "id", DataType::UInt64, false),
            Column::new(2, "person", DataType::UInt64, false),
            Column::new(3, "label", DataType::Utf8, false),
        ],
    )
    .expect("people schema")
}

struct Fixture {
    _directory: tempfile::TempDir,
    events: TableStore,
    people: TableStore,
    catalog: CatalogSnapshot,
}

impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().expect("directory");
        let mut events = TableStore::open(
            directory.path().join("events"),
            events_schema(),
            StoreOptions::default(),
        )
        .expect("events");
        events
            .bulk_ingest_snapshot(
                (0..EVENTS)
                    .map(|id| {
                        StoredRow::new(
                            PrimaryKey::new(vec![KeyPart::UInt64(id)]).expect("key"),
                            vec![
                                Value::UInt64(id),
                                if id % 17 == 0 {
                                    Value::Null
                                } else {
                                    Value::UInt64(id % PEOPLE)
                                },
                                Value::UInt64((id * 7) % (PEOPLE + 50)),
                                // Many events share a second, so the cut falls among ties.
                                Value::Utf8(format!(
                                    "2026-02-{:02} {:02}:{:02}:00",
                                    1 + (id / 3_000) % 28,
                                    (id / 60) % 24,
                                    (id / 7) % 60
                                )),
                                Value::Utf8(format!("n{id}")),
                            ],
                            id + 1,
                            false,
                        )
                    })
                    .collect(),
            )
            .expect("event rows");
        let mut people = TableStore::open(
            directory.path().join("people"),
            people_schema(),
            StoreOptions::default(),
        )
        .expect("people");
        // Person 3 has two rows, so an event naming it joins twice.
        people
            .bulk_ingest_snapshot(
                (0..=PEOPLE)
                    .map(|id| {
                        let person = if id == PEOPLE { 3 } else { id };
                        StoredRow::new(
                            PrimaryKey::new(vec![KeyPart::UInt64(id)]).expect("key"),
                            vec![
                                Value::UInt64(id),
                                Value::UInt64(person),
                                Value::Utf8(format!("p{id}")),
                            ],
                            id + 1,
                            false,
                        )
                    })
                    .collect(),
            )
            .expect("people rows");
        let events_entry = TableEntry::new(
            TableId::new(1),
            "events",
            events_schema(),
            TableStatistics::with_row_count(EVENTS),
        )
        .expect("events entry")
        .with_key_columns([1])
        .expect("events key");
        let people_entry = TableEntry::new(
            TableId::new(2),
            "people",
            people_schema(),
            TableStatistics::with_row_count(PEOPLE + 1),
        )
        .expect("people entry")
        .with_key_columns([1])
        .expect("people key");
        Self {
            _directory: directory,
            events,
            people,
            catalog: CatalogSnapshot::new([DatabaseEntry::new(
                DatabaseId::new(1),
                "app",
                [events_entry, people_entry],
            )
            .expect("database")])
            .expect("catalog"),
        }
    }

    /// Ordered rows and the rows the lowest join was fed, from a profiled run.
    fn run(&self, sql: &str) -> (Vec<String>, u64) {
        let (rows, joined, _) = self.run_labelled(sql);
        (rows, joined)
    }

    /// `run`, with the label of every operator the plan ran.
    fn run_labelled(&self, sql: &str) -> (Vec<String>, u64, Vec<String>) {
        let events = self.events.snapshot();
        let people = self.people.snapshot();
        let provider = SnapshotScanProvider::new([
            (DatabaseId::new(1), TableId::new(1), &events),
            (DatabaseId::new(1), TableId::new(2), &people),
        ])
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

/// `$AT` is the sort key: `e.at` lets the limit reach the events table,
/// `e.at + INTERVAL 0 SECOND` is the same order with nothing to push.
const QUERY: &str = "SELECT e.id, e.at, a.label AS actor, s.label AS subject FROM events e \
     LEFT JOIN people a ON a.person = e.actor \
     LEFT JOIN people s ON s.person = e.subject \
     ORDER BY $AT DESC LIMIT 20 OFFSET $OFFSET";

#[test]
fn the_limit_reaches_the_preserved_table_and_the_answer_holds() {
    let fixture = Fixture::new();
    for offset in ["0", "40", "3000"] {
        let query = QUERY.replace("$OFFSET", offset);
        let (pushed, joined) = fixture.run(&query.replace("$AT", "e.at"));
        let (whole, whole_joined) = fixture.run(&query.replace("$AT", "e.at + INTERVAL 0 SECOND"));
        assert_eq!(pushed.len(), 20, "offset {offset}");
        assert_eq!(pushed, whole, "offset {offset}");
        assert!(
            whole_joined >= EVENTS,
            "the unpushed form joins every event"
        );
        assert!(
            joined <= 2 * (20 + offset.parse::<u64>().expect("offset")),
            "offset {offset}: {joined} rows went through the joins"
        );
    }
}

#[test]
fn a_filter_above_the_joins_keeps_the_limit_where_it_was() {
    let fixture = Fixture::new();
    // A WHERE on the joined side can drop left rows; the first n events are
    // then not enough, so nothing may move.
    let query = "SELECT e.id FROM events e LEFT JOIN people a ON a.person = e.actor \
         WHERE a.label IS NOT NULL ORDER BY e.at DESC, e.id LIMIT 20";
    let (rows, joined) = fixture.run(query);
    assert_eq!(rows.len(), 20);
    assert!(joined >= EVENTS - EVENTS / 17 - 1, "{joined}");
}

#[test]
fn a_short_prefix_reads_the_rest_of_its_rows_by_key() {
    let fixture = Fixture::new();
    // Every column of the table, and a predicate on it: the sort reads the
    // key and the sort column, and the full rows of the survivors are read
    // by key after the cut.
    let query = "SELECT e.*, a.label AS actor FROM events e \
         LEFT JOIN people a ON a.person = e.actor \
         WHERE e.note <> 'n5' AND e.subject IS NOT NULL \
         ORDER BY $AT DESC LIMIT 20 OFFSET $OFFSET";
    for offset in ["0", "40"] {
        let query = query.replace("$OFFSET", offset);
        let (deferred, _, labels) = fixture.run_labelled(&query.replace("$AT", "e.at"));
        let (whole, _) = fixture.run(&query.replace("$AT", "e.at + INTERVAL 0 SECOND"));
        assert_eq!(deferred.len(), 20, "offset {offset}");
        assert_eq!(deferred, whole, "offset {offset}");
        assert!(
            labels
                .iter()
                .any(|label| label.starts_with("KeyLookupJoin")),
            "offset {offset}: {labels:?}"
        );
    }
    // Ascending ties resolve by arrival as well.
    let query = "SELECT e.* FROM events e LEFT JOIN people a ON a.person = e.actor \
         ORDER BY $AT LIMIT 30 OFFSET 7";
    let (deferred, _) = fixture.run(&query.replace("$AT", "e.at"));
    let (whole, _) = fixture.run(&query.replace("$AT", "e.at + INTERVAL 0 SECOND"));
    assert_eq!(deferred, whole);
}
