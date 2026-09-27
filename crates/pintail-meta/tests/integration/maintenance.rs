//! The control-plane file's upkeep: checking it, copying it, and bounding
//! the history that grows with every replication cycle.

use pintail_meta::MetaStore;

fn store_with_database(dir: &std::path::Path) -> MetaStore {
    let store = MetaStore::open(&dir.join("pintail-meta.db")).expect("metadata opens");
    store
        .upsert_database("db_1", "source", b"dsn", "2026-01-01T00:00:00+00:00")
        .expect("database row");
    store
}

fn run(store: &MetaStore, id: &str, kind: &str, status: &str, started_at: &str) {
    store
        .start_sync_run(id, "db_1", None, kind, started_at)
        .expect("run starts");
    if status != "running" {
        store
            .finish_sync_run(id, status, 0, 0, 1, None)
            .expect("run finishes");
    }
}

#[test]
fn a_healthy_file_reports_no_problems() {
    let dir = tempfile::tempdir().expect("temporary directory");
    let store = store_with_database(dir.path());
    assert!(
        store
            .integrity_problems(false)
            .expect("quick check")
            .is_empty()
    );
    assert!(
        store
            .integrity_problems(true)
            .expect("full check")
            .is_empty()
    );
}

#[test]
fn a_damaged_page_is_reported_rather_than_found_by_a_read() {
    let dir = tempfile::tempdir().expect("temporary directory");
    let path = dir.path().join("pintail-meta.db");
    {
        let store = store_with_database(dir.path());
        for index in 0..2_000 {
            run(
                &store,
                &format!("run_{index:05}"),
                "cdc",
                "completed",
                "2026-01-01T00:00:00+00:00",
            );
        }
    }
    // Everything into the main file, so the damage below is not shadowed
    // by a newer page image in the WAL.
    let checkpoint = rusqlite::Connection::open(&path).expect("raw open");
    checkpoint
        .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()))
        .expect("checkpoint");
    drop(checkpoint);
    // Overwrite a stretch of pages in the middle of the file: what a torn
    // write or a second writer without the lock leaves behind.
    let mut bytes = std::fs::read(&path).expect("read file");
    let middle = bytes.len() / 2;
    for byte in &mut bytes[middle..middle + 8_192] {
        *byte = 0x5a;
    }
    std::fs::write(&path, bytes).expect("damage file");

    // A file too damaged to check is reported by the failure itself; what
    // must never happen is a clean bill of health.
    let store = MetaStore::open(&path).expect("header still readable");
    if let Ok(problems) = store.integrity_problems(false) {
        assert!(!problems.is_empty(), "the damage must be reported");
    }
}

#[test]
fn a_backup_is_a_complete_readable_copy_and_replaces_the_previous_one() {
    let dir = tempfile::tempdir().expect("temporary directory");
    let store = store_with_database(dir.path());
    run(
        &store,
        "run_a",
        "snapshot",
        "completed",
        "2026-01-02T00:00:00+00:00",
    );
    let target = dir.path().join("backup.db");
    store.backup_into(&target).expect("first copy");
    run(
        &store,
        "run_b",
        "snapshot",
        "completed",
        "2026-01-03T00:00:00+00:00",
    );
    // A copy a crash cut short must not block the next one.
    std::fs::write(dir.path().join("backup.db.partial"), b"torn").expect("stale partial");
    store
        .backup_into(&target)
        .expect("second copy replaces the first");
    assert!(!dir.path().join("backup.db.partial").exists());

    let copy = MetaStore::open(&target).expect("copy opens");
    assert!(copy.integrity_problems(true).expect("check").is_empty());
    let runs = copy.sync_runs(Some("db_1"), 10).expect("runs");
    assert_eq!(runs.len(), 2, "the copy holds everything written before it");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let mode = std::fs::metadata(&target)
            .expect("stat")
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600, "the copy holds encrypted DSNs");
    }
}

#[test]
fn pruning_keeps_copies_failures_and_live_runs_longer_than_routine_cycles() {
    let dir = tempfile::tempdir().expect("temporary directory");
    let store = store_with_database(dir.path());
    let old = "2026-01-01T00:00:00+00:00";
    let recent = "2026-01-20T00:00:00+00:00";
    run(&store, "old_cycle", "cdc", "completed", old);
    run(&store, "old_poll", "polling", "completed", old);
    run(&store, "old_failure", "cdc", "error", old);
    run(&store, "old_copy", "resnapshot", "completed", old);
    run(&store, "old_running", "snapshot", "running", old);
    run(&store, "new_cycle", "cdc", "completed", recent);

    let removed = store
        .prune_sync_runs("2026-01-10T00:00:00+00:00", "2025-12-01T00:00:00+00:00")
        .expect("prune");
    assert_eq!(removed, 2);
    let mut kept = store
        .sync_runs(Some("db_1"), 100)
        .expect("runs")
        .into_iter()
        .map(|run| run.id)
        .collect::<Vec<_>>();
    kept.sort();
    assert_eq!(
        kept,
        ["new_cycle", "old_copy", "old_failure", "old_running"]
    );

    let removed = store
        .prune_sync_runs("2026-01-10T00:00:00+00:00", "2026-01-10T00:00:00+00:00")
        .expect("prune");
    assert_eq!(removed, 2, "past the history bound only the live run stays");
    let mut kept = store
        .sync_runs(Some("db_1"), 100)
        .expect("runs")
        .into_iter()
        .map(|run| run.id)
        .collect::<Vec<_>>();
    kept.sort();
    assert_eq!(kept, ["new_cycle", "old_running"]);
}
