use std::{
    path::{Path, PathBuf},
    process::Command,
    thread,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
use pintail_meta::MetaStore;

const DATABASES: [&str; 2] = ["db-a", "db-b"];
const TABLES: [&str; 4] = ["item-a", "item-b", "item-c", "item-d"];

fn duration() -> Duration {
    Duration::from_secs(
        std::env::var("PINTAIL_META_STRESS_SECONDS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(120),
    )
}

fn seed(path: &Path) -> Result<()> {
    let store = MetaStore::open(path)?;
    for database in DATABASES {
        store.upsert_database(database, database, b"opaque", "2026-01-01T00:00:00Z")?;
        for table in TABLES {
            store.upsert_snapshot_table(database, table, None, None)?;
        }
    }
    Ok(())
}

fn write_loop(path: PathBuf, worker: usize, until: Instant) -> Result<()> {
    let mut sequence = 0_u64;
    while Instant::now() < until {
        let database = DATABASES[worker % DATABASES.len()];
        let table = TABLES[worker % TABLES.len()];
        let id = format!("worker-{worker}-{sequence}");
        MetaStore::open(&path)?.upsert_snapshot_table(database, table, None, None)?;
        MetaStore::open(&path)?.start_snapshot_chunk(database, table, &id, None, None)?;
        MetaStore::open(&path)?.complete_snapshot_chunk(database, table, &id, 1)?;
        MetaStore::open(&path)?.start_sync_run(&id, database, Some(table), "snapshot", "now")?;
        MetaStore::open(&path)?.finish_sync_run(&id, "completed", 1, 1, 1, None)?;
        sequence += 1;
        thread::sleep(Duration::from_millis(2));
    }
    Ok(())
}

fn integrity_check(path: &Path) -> Result<()> {
    let connection = rusqlite::Connection::open(path)?;
    let mut statement = connection.prepare("PRAGMA integrity_check")?;
    let findings = statement
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    ensure!(
        findings.len() == 1 && findings[0] == "ok",
        "integrity_check: {findings:?}"
    );
    Ok(())
}

fn run_case(foreign_fd: bool, second_process: bool) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("meta.db");
    seed(&path)?;
    let duration = duration();
    let until = Instant::now() + duration;

    let child = if second_process {
        Some(
            Command::new(std::env::current_exe()?)
                .arg("--exact")
                .arg("second_process_worker")
                .arg("--nocapture")
                .env("PINTAIL_META_STRESS_CHILD_PATH", &path)
                .env(
                    "PINTAIL_META_STRESS_SECONDS",
                    duration.as_secs().to_string(),
                )
                .spawn()
                .context("start second process")?,
        )
    } else {
        None
    };

    let mut workers = Vec::new();
    for worker in 0..8 {
        let path = path.clone();
        workers.push(thread::spawn(move || write_loop(path, worker, until)));
    }
    let reader_path = path.clone();
    workers.push(thread::spawn(move || {
        while Instant::now() < until {
            let store = MetaStore::open(&reader_path)?;
            store.tables(DATABASES[0])?;
            store.sync_runs(Some(DATABASES[1]), 10)?;
            thread::sleep(Duration::from_millis(2));
        }
        Ok(())
    }));
    let checkpoint_path = path.clone();
    workers.push(thread::spawn(move || {
        let connection = rusqlite::Connection::open(&checkpoint_path)?;
        connection.busy_timeout(Duration::from_secs(5))?;
        while Instant::now() < until {
            connection.query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |_| Ok(()))?;
            thread::sleep(Duration::from_millis(50));
        }
        Ok(())
    }));
    if foreign_fd {
        let fd_path = path.clone();
        workers.push(thread::spawn(move || {
            while Instant::now() < until {
                drop(std::fs::File::open(&fd_path)?);
                thread::sleep(Duration::from_millis(1));
            }
            Ok(())
        }));
    }

    let mut worker_error = None;
    for worker in workers {
        if let Err(error) = worker.join().expect("stress worker panicked") {
            worker_error.get_or_insert(error);
        }
    }
    if let Some(mut child) = child {
        if !child.wait()?.success() {
            worker_error.get_or_insert_with(|| anyhow::anyhow!("second process failed"));
        }
    }
    integrity_check(&path)?;
    if let Some(error) = worker_error {
        return Err(error);
    }
    Ok(())
}

#[test]
fn second_process_worker() -> Result<()> {
    let Ok(path) = std::env::var("PINTAIL_META_STRESS_CHILD_PATH") else {
        return Ok(());
    };
    let until = Instant::now() + duration();
    let path = PathBuf::from(path);
    let a = thread::spawn({
        let path = path.clone();
        move || write_loop(path, 100, until)
    });
    let b = thread::spawn(move || write_loop(path, 101, until));
    a.join().expect("child writer panicked")?;
    b.join().expect("child writer panicked")?;
    Ok(())
}

#[test]
fn replaced_database_is_initialized_again() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("meta.db");
    let first = MetaStore::open(&path)?;
    let version = first.schema_version()?;
    drop(first);
    std::fs::remove_file(&path)?;
    let second = MetaStore::open(&path)?;
    ensure!(second.schema_version()? == version);
    Ok(())
}

#[test]
#[ignore = "two-minute metadata contention run"]
fn baseline_concurrent_load() -> Result<()> {
    run_case(false, false)
}

#[test]
#[ignore = "two-minute metadata contention run"]
fn foreign_fd_open_close_enabled() -> Result<()> {
    run_case(true, false)
}

#[test]
#[ignore = "two-minute metadata contention run"]
fn foreign_fd_open_close_disabled() -> Result<()> {
    run_case(false, false)
}

#[test]
#[ignore = "two-minute metadata contention run"]
fn second_process_opening_file() -> Result<()> {
    run_case(false, true)
}
