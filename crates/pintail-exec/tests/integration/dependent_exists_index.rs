//! A correlated `EXISTS` outside the top-level `WHERE` - inside a `CASE`,
//! in the select list, under an `OR` - is answered by the dependent path,
//! one inner execution per outer row. When the inner query is one table
//! under a conjunctive `WHERE` with an integer equality to the outer row,
//! the operator indexes that table once and each outer row looks its key
//! up instead.
//!
//! The expectations are derived by hand from `MySQL` semantics for the data
//! below: NULL keys never match, a residual that is NULL is not a match,
//! signed and unsigned integers compare by value, and duplicate candidates
//! change nothing. The scale case asserts the index engaged and answers
//! twenty thousand outer rows quickly.

use std::time::Instant;

use pintail_catalog::{
    CatalogSnapshot, DatabaseEntry, DatabaseId, TableEntry, TableId, TableStatistics,
};
use pintail_exec::collation::Collation;
use pintail_exec::{
    Execution, LogicalPlanner, Optimizer, PhysicalPlanner, SnapshotScanProvider, take_exec_counters,
};
use pintail_sql::{Binder, parse_statement};
use pintail_store::{StoreOptions, TableStore};
use pintail_types::{Column, DataType, KeyPart, PrimaryKey, StoredRow, TableSchema, Value};

fn visits_schema() -> TableSchema {
    TableSchema::new(
        1,
        vec![
            Column::new(1, "id", DataType::UInt64, false),
            Column::new(2, "person_id", DataType::Int64, true),
            Column::new(3, "team_id", DataType::UInt64, false),
            Column::new(4, "starts_at", DataType::DateTime64 { fsp: 0 }, false),
            Column::new(5, "tag", DataType::Utf8, true),
        ],
    )
    .expect("visits schema")
}

fn teams_schema() -> TableSchema {
    TableSchema::new(
        1,
        vec![
            Column::new(1, "id", DataType::UInt64, false),
            Column::new(2, "name", DataType::Utf8, false),
        ],
    )
    .expect("teams schema")
}

fn leaves_schema() -> TableSchema {
    TableSchema::new(
        1,
        vec![
            Column::new(1, "id", DataType::UInt64, false),
            Column::new(2, "person_id", DataType::UInt64, true),
            Column::new(3, "team_id", DataType::Int64, false),
            Column::new(4, "state", DataType::Utf8, false),
            Column::new(5, "day_from", DataType::Date32, true),
            Column::new(6, "day_to", DataType::Date32, true),
            Column::new(7, "tag", DataType::Utf8, true),
        ],
    )
    .expect("leaves schema")
}

fn stored(id: u64, values: Vec<Value>) -> StoredRow {
    StoredRow::new(
        PrimaryKey::new(vec![KeyPart::UInt64(id)]).expect("key"),
        values,
        id,
        false,
    )
}

fn text(value: &str) -> Value {
    Value::Utf8(value.to_owned())
}

fn optional_text(value: Option<&str>) -> Value {
    value.map_or(Value::Null, text)
}

struct Tables {
    visits: Vec<StoredRow>,
    teams: Vec<StoredRow>,
    leaves: Vec<StoredRow>,
}

type Visit<'a> = (u64, Option<i64>, u64, &'a str, Option<&'a str>);
type Leave<'a> = (
    u64,
    Option<u64>,
    i64,
    &'a str,
    Option<&'a str>,
    Option<&'a str>,
    Option<&'a str>,
);

fn visit(row: Visit<'_>) -> StoredRow {
    let (id, person, team, starts_at, tag) = row;
    stored(
        id,
        vec![
            Value::UInt64(id),
            person.map_or(Value::Null, Value::Int64),
            Value::UInt64(team),
            text(starts_at),
            optional_text(tag),
        ],
    )
}

fn leave(row: Leave<'_>) -> StoredRow {
    let (id, person, team, state, from, to, tag) = row;
    stored(
        id,
        vec![
            Value::UInt64(id),
            person.map_or(Value::Null, Value::UInt64),
            Value::Int64(team),
            text(state),
            optional_text(from),
            optional_text(to),
            optional_text(tag),
        ],
    )
}

fn team(id: u64, name: &str) -> StoredRow {
    stored(id, vec![Value::UInt64(id), text(name)])
}

/// Hand-checked data. Per visit, whether an approved leave of the same
/// person and team covers its day:
///
/// 1. person 10 team 1, Jan 11: leaves 1 and 8 both cover it - yes.
/// 2. person 10 team 1, Jan 15: leave 1 ends Jan 12, leave 2 is Jan 20,
///    leave 3 covers it but is pending - no.
/// 3. person 10 team 1, Jan 20 evening: leave 2 has no end, so it is the
///    single day Jan 20 - yes.
/// 4. person 10 team 2, Jan 11: leave 7 - yes.
/// 5. person 11 team 2, Jan 5 23:59: leave 4 ends Jan 5, compared as dates -
///    yes.
/// 6. person 11 team 1: no leave for that team - no.
/// 7. NULL person: never equal, and leave 5's NULL person matches nothing -
///    no.
/// 8. person 12 team 1: leave 6 has no dates, the residual is NULL - no.
/// 9. person 13: no leave at all - no.
/// 10. team 3 does not exist, so the join drops it.
fn small_tables() -> Tables {
    let visits: [Visit<'_>; 10] = [
        (1, Some(10), 1, "2024-01-11 09:00:00", Some("a")),
        (2, Some(10), 1, "2024-01-15 09:00:00", Some("b")),
        (3, Some(10), 1, "2024-01-20 18:00:00", None),
        (4, Some(10), 2, "2024-01-11 09:00:00", None),
        (5, Some(11), 2, "2024-01-05 23:59:00", None),
        (6, Some(11), 1, "2024-01-03 09:00:00", None),
        (7, None, 1, "2024-01-03 09:00:00", Some("a")),
        (8, Some(12), 1, "2024-01-03 09:00:00", None),
        (9, Some(13), 1, "2024-01-03 09:00:00", None),
        (10, Some(10), 3, "2024-01-11 09:00:00", None),
    ];
    let leaves: [Leave<'_>; 8] = [
        (
            1,
            Some(10),
            1,
            "approved",
            Some("2024-01-10"),
            Some("2024-01-12"),
            Some("a"),
        ),
        (2, Some(10), 1, "approved", Some("2024-01-20"), None, None),
        (
            3,
            Some(10),
            1,
            "pending",
            Some("2024-01-01"),
            Some("2024-01-31"),
            Some("b"),
        ),
        (
            4,
            Some(11),
            2,
            "approved",
            Some("2024-01-01"),
            Some("2024-01-05"),
            None,
        ),
        (
            5,
            None,
            1,
            "approved",
            Some("2024-01-01"),
            Some("2024-12-31"),
            Some("a"),
        ),
        (6, Some(12), 1, "approved", None, None, None),
        (
            7,
            Some(10),
            2,
            "approved",
            Some("2024-01-10"),
            Some("2024-01-12"),
            None,
        ),
        (
            8,
            Some(10),
            1,
            "approved",
            Some("2024-01-11"),
            Some("2024-01-11"),
            None,
        ),
    ];
    Tables {
        visits: visits.into_iter().map(visit).collect(),
        teams: vec![team(1, "red"), team(2, "blue")],
        leaves: leaves.into_iter().map(leave).collect(),
    }
}

fn run(tables: Tables, sql: &str) -> Vec<Vec<String>> {
    let directories = [(); 3].map(|()| tempfile::tempdir().expect("table dir"));
    let counts = [
        tables.visits.len() as u64,
        tables.teams.len() as u64,
        tables.leaves.len() as u64,
    ];
    let open = |index: usize, schema: TableSchema, rows: Vec<StoredRow>| {
        let mut store =
            TableStore::open(directories[index].path(), schema, StoreOptions::default())
                .expect("open table");
        store.bulk_ingest_snapshot(rows).expect("snapshot");
        store
    };
    let visits = open(0, visits_schema(), tables.visits);
    let teams = open(1, teams_schema(), tables.teams);
    let leaves = open(2, leaves_schema(), tables.leaves);
    let snapshots = [visits.snapshot(), teams.snapshot(), leaves.snapshot()];
    let database_id = DatabaseId::new(7);
    let ids = [TableId::new(71), TableId::new(72), TableId::new(73)];
    let database = DatabaseEntry::new(
        database_id,
        "app",
        [
            TableEntry::new(
                ids[0],
                "visits",
                visits_schema(),
                TableStatistics::with_row_count(counts[0]),
            )
            .expect("visits entry"),
            TableEntry::new(
                ids[1],
                "teams",
                teams_schema(),
                TableStatistics::with_row_count(counts[1]),
            )
            .expect("teams entry"),
            TableEntry::new(
                ids[2],
                "leaves",
                leaves_schema(),
                TableStatistics::with_row_count(counts[2]),
            )
            .expect("leaves entry"),
        ],
    )
    .expect("database");
    let catalog = CatalogSnapshot::new([database]).expect("catalog");
    let provider = SnapshotScanProvider::new([
        (database_id, ids[0], &snapshots[0]),
        (database_id, ids[1], &snapshots[1]),
        (database_id, ids[2], &snapshots[2]),
    ])
    .expect("provider");
    let statement = parse_statement(sql).expect("parse");
    let bound = Binder::new(&catalog, Some("app"))
        .bind(&statement)
        .unwrap_or_else(|error| panic!("bind {sql}: {error}"));
    let physical = PhysicalPlanner::plan(
        Optimizer::optimize(LogicalPlanner::plan(bound)),
        Collation::default(),
    )
    .expect("plan");
    let mut execution =
        Execution::start(physical, &provider, 256 * 1024 * 1024, Collation::default())
            .expect("start");
    let mut rows = Vec::new();
    while let Some(batch) = execution
        .next_batch()
        .unwrap_or_else(|error| panic!("pull batch for {sql}: {error}"))
    {
        for row in batch.selection().selected_rows() {
            rows.push(
                (0..batch.columns().len())
                    .map(|column| {
                        render(
                            batch
                                .column(column)
                                .and_then(|values| values.value(row))
                                .expect("selected value"),
                        )
                    })
                    .collect(),
            );
        }
    }
    rows
}

fn render(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_owned(),
        Value::Boolean(value) => u8::from(*value).to_string(),
        Value::Int64(value) => value.to_string(),
        Value::UInt64(value) => value.to_string(),
        Value::Utf8(value) => value.clone(),
        other => format!("{other:?}"),
    }
}

fn pairs(rows: &[Vec<String>]) -> Vec<(String, String)> {
    rows.iter()
        .map(|row| (row[0].clone(), row[1].clone()))
        .collect()
}

fn expected(values: &[(u64, u8)]) -> Vec<(String, String)> {
    values
        .iter()
        .map(|(id, value)| (id.to_string(), value.to_string()))
        .collect()
}

const ON_LEAVE: &str = "EXISTS (SELECT 1 FROM leaves l \
     WHERE l.person_id = v.person_id AND l.team_id = t.id AND l.state = 'approved' \
       AND CAST(v.starts_at AS DATE) >= CAST(l.day_from AS DATE) \
       AND CAST(v.starts_at AS DATE) <= CAST(COALESCE(l.day_to, l.day_from) AS DATE))";

const ANSWERS: [(u64, u8); 9] = [
    (1, 1),
    (2, 0),
    (3, 1),
    (4, 1),
    (5, 1),
    (6, 0),
    (7, 0),
    (8, 0),
    (9, 0),
];

/// Runs `sql` and returns its rows with the index counters it moved. The
/// counters are thread-local; the dependent operator runs on this thread.
fn run_counted(tables: Tables, sql: &str) -> (Vec<Vec<String>>, u64, u64) {
    let _ = take_exec_counters();
    let rows = run(tables, sql);
    let counters = take_exec_counters();
    (
        rows,
        counters.dependent_index_builds,
        counters.dependent_index_probes,
    )
}

#[test]
fn a_case_exists_answers_from_the_index() {
    let sql = format!(
        "SELECT v.id, CASE WHEN {ON_LEAVE} THEN 1 ELSE 0 END AS on_leave \
         FROM visits v JOIN teams t ON t.id = v.team_id ORDER BY v.id"
    );
    let (rows, builds, probes) = run_counted(small_tables(), &sql);
    assert_eq!(pairs(&rows), expected(&ANSWERS));
    assert_eq!(builds, 1, "the index engaged once for the operator");
    assert_eq!(probes, 9, "every joined outer row was answered by it");
}

#[test]
fn not_exists_is_the_exact_complement() {
    let sql = format!(
        "SELECT v.id, CASE WHEN NOT {ON_LEAVE} THEN 1 ELSE 0 END AS off \
         FROM visits v JOIN teams t ON t.id = v.team_id ORDER BY v.id"
    );
    let (rows, builds, _) = run_counted(small_tables(), &sql);
    let complement = ANSWERS.map(|(id, value)| (id, 1 - value));
    assert_eq!(pairs(&rows), expected(&complement));
    assert_eq!(builds, 1);
}

#[test]
fn a_bare_select_list_exists_answers_from_the_index() {
    let sql = format!(
        "SELECT v.id, {ON_LEAVE} AS on_leave \
         FROM visits v JOIN teams t ON t.id = v.team_id ORDER BY v.id"
    );
    let (rows, builds, _) = run_counted(small_tables(), &sql);
    assert_eq!(pairs(&rows), expected(&ANSWERS));
    assert_eq!(builds, 1);
}

#[test]
fn an_exists_under_or_in_where_filters_by_the_index() {
    let sql = format!(
        "SELECT v.id, t.name FROM visits v JOIN teams t ON t.id = v.team_id \
         WHERE v.id = 2 OR {ON_LEAVE} ORDER BY v.id"
    );
    let rows = run(small_tables(), &sql);
    let ids = rows.iter().map(|row| row[0].clone()).collect::<Vec<_>>();
    assert_eq!(ids, ["1", "2", "3", "4", "5"]);
}

#[test]
fn an_exists_with_limit_one_and_no_residual_uses_the_index() {
    // Every conjunct but the keys is uncorrelated: the index answers from
    // the key lookup alone. Visit 7's NULL person matches nothing even
    // though leave 5 has a NULL person too.
    let sql = "SELECT v.id, CASE WHEN EXISTS (SELECT * FROM leaves l \
               WHERE v.person_id = l.person_id AND l.state = 'approved' LIMIT 1) \
               THEN 1 ELSE 0 END FROM visits v ORDER BY v.id";
    let (rows, builds, _) = run_counted(small_tables(), sql);
    assert_eq!(
        pairs(&rows),
        expected(&[
            (1, 1),
            (2, 1),
            (3, 1),
            (4, 1),
            (5, 1),
            (6, 1),
            (7, 0),
            (8, 1),
            (9, 0),
            (10, 1),
        ])
    );
    assert_eq!(builds, 1);
}

#[test]
fn a_residual_text_comparison_keeps_its_semantics() {
    // Key on the person, residual on the tag: person 10 has leave 1 tagged
    // 'a' and leave 3 tagged 'b', which visits 1 and 2 carry. Visit 7 has
    // tag 'a' but a NULL person.
    let sql = "SELECT v.id, CASE WHEN EXISTS (SELECT 1 FROM leaves l \
               WHERE l.person_id = v.person_id AND l.tag = v.tag) \
               THEN 1 ELSE 0 END FROM visits v ORDER BY v.id";
    let (rows, builds, _) = run_counted(small_tables(), sql);
    let answers = (1..=10)
        .map(|id| (id, u8::from(matches!(id, 1 | 2))))
        .collect::<Vec<_>>();
    assert_eq!(pairs(&rows), expected(&answers));
    assert_eq!(builds, 1);
}

#[test]
fn a_text_only_correlation_falls_back_to_the_per_row_path() {
    // No integer equality: the index declines and the per-row path answers.
    // Visits 1 and 7 carry tag 'a', which leaves 1 and 5 carry; visit 2's
    // 'b' is on leave 3.
    let sql = "SELECT v.id, CASE WHEN EXISTS (SELECT 1 FROM leaves l WHERE l.tag = v.tag) \
               THEN 1 ELSE 0 END FROM visits v ORDER BY v.id";
    let (rows, builds, probes) = run_counted(small_tables(), sql);
    let answers = (1..=10)
        .map(|id| (id, u8::from(matches!(id, 1 | 2 | 7))))
        .collect::<Vec<_>>();
    assert_eq!(pairs(&rows), expected(&answers));
    assert_eq!((builds, probes), (0, 0), "text keys never build an index");
}

#[test]
fn an_empty_inner_table_answers_false_everywhere() {
    let mut tables = small_tables();
    tables.leaves.clear();
    let sql = format!(
        "SELECT v.id, CASE WHEN {ON_LEAVE} THEN 1 ELSE 0 END \
         FROM visits v JOIN teams t ON t.id = v.team_id ORDER BY v.id"
    );
    let rows = run(tables, &sql);
    assert_eq!(pairs(&rows), expected(&ANSWERS.map(|(id, _)| (id, 0))));
}

/// Twenty thousand outer rows with mostly distinct correlation tuples over
/// five thousand inner rows: one inner execution per row took most of a
/// millisecond each; the index answers them all from one read.
#[test]
fn twenty_thousand_distinct_outer_rows_answer_quickly() {
    const VISITS: u64 = 20_000;
    const LEAVES: u64 = 5_000;
    let day = |id: u64| 5 + id % 20;
    let visits = (1..=VISITS)
        .map(|id| {
            visit((
                id,
                Some(i64::try_from(id % LEAVES + 1).expect("small")),
                id % 3,
                &format!("2024-01-{:02} 08:00:00", day(id)),
                None,
            ))
        })
        .collect();
    let leaves = (1..=LEAVES)
        .map(|id| {
            leave((
                id,
                Some(id),
                i64::try_from(id % 3).expect("small"),
                if id % 2 == 0 { "approved" } else { "pending" },
                Some("2024-01-10"),
                Some("2024-01-20"),
                None,
            ))
        })
        .collect();
    let tables = Tables {
        visits,
        teams: vec![team(0, "zero"), team(1, "one"), team(2, "two")],
        leaves,
    };
    let expected_on_leave = (1..=VISITS)
        .filter(|id| {
            let person = id % LEAVES + 1;
            person.is_multiple_of(2) && person % 3 == id % 3 && (10..=20).contains(&day(*id))
        })
        .count();
    let sql = format!(
        "SELECT v.id, CASE WHEN {ON_LEAVE} THEN 1 ELSE 0 END AS on_leave \
         FROM visits v JOIN teams t ON t.id = v.team_id"
    );
    let started = Instant::now();
    let (rows, builds, probes) = run_counted(tables, &sql);
    let elapsed = started.elapsed();
    assert_eq!(rows.len(), usize::try_from(VISITS).expect("small"));
    println!("{VISITS} outer rows over {LEAVES} inner rows: {elapsed:?}");
    assert_eq!(
        rows.iter().filter(|row| row[1] == "1").count(),
        expected_on_leave
    );
    assert_eq!(builds, 1);
    assert_eq!(probes, VISITS);
    assert!(
        elapsed.as_secs() < 8,
        "20k outer rows took {elapsed:?}; the per-row path takes about 17 s in a debug build"
    );
}
