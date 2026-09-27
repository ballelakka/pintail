//! Upkeep of the control-plane file, on a timer of its own.
//!
//! Damage to the metadata file used to surface as whatever read touched the
//! bad page first - a dashboard that could not decode its table list, hours
//! after the fact, with no good copy to go back to. This task looks for
//! damage on purpose (a full check at startup, a quick one every hour),
//! reports it as an error so it reaches Sentry and the dashboard, keeps a
//! short rotation of consistent copies beside the data, and prunes the run
//! history that otherwise grows by one row per replication cycle forever.
//!
//! A copy is only taken while the last check was clean: rotating damaged
//! copies in would push out the good ones the moment they are needed.

use std::{
    path::{Path, PathBuf},
    sync::{Mutex, OnceLock},
    time::{Duration, SystemTime},
};

use chrono::Utc;
use serde::Serialize;

use crate::{ApiState, events::ApiEvent};

/// How often the file is checked and the history pruned.
const CHECK_INTERVAL: Duration = Duration::from_secs(60 * 60);
/// Default age at which a new copy is taken.
const DEFAULT_BACKUP_HOURS: u64 = 6;
/// Default number of copies kept: two days at the default cadence.
const DEFAULT_BACKUP_KEEP: usize = 8;
/// Successful replication cycles are kept this long; they are one row per
/// cycle and only the recent ones are ever read.
const CYCLE_RETENTION: chrono::Duration = chrono::Duration::days(1);
/// Copies, repairs and failures are kept this long: they are what an
/// operator reads after an incident.
const HISTORY_RETENTION: chrono::Duration = chrono::Duration::days(30);

const BACKUP_DIRECTORY: &str = "meta-backups";
const BACKUP_PREFIX: &str = "pintail-meta-";

/// What the last check of the metadata file found.
#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct MetadataHealth {
    /// `unchecked` until the first check completes, then `ok` or `damaged`.
    pub(crate) state: &'static str,
    pub(crate) checked_at: Option<String>,
    /// What the check reported, capped; empty when healthy.
    pub(crate) problems: Vec<String>,
    /// When the newest copy on disk was written.
    pub(crate) last_backup_at: Option<String>,
    /// Why the last attempt to write a copy failed, if it did.
    pub(crate) backup_error: Option<String>,
}

fn health_cell() -> &'static Mutex<MetadataHealth> {
    static HEALTH: OnceLock<Mutex<MetadataHealth>> = OnceLock::new();
    HEALTH.get_or_init(|| {
        Mutex::new(MetadataHealth {
            state: "unchecked",
            ..MetadataHealth::default()
        })
    })
}

/// The metadata file's health as of the last check.
pub(crate) fn current() -> MetadataHealth {
    health_cell()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
}

fn update(change: impl FnOnce(&mut MetadataHealth)) {
    change(
        &mut health_cell()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
    );
}

fn env_number<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// Runs the upkeep until `shutdown` fires. The first pass runs at once and
/// checks thoroughly; later passes use the quick check.
pub(crate) async fn run(state: ApiState, mut shutdown: tokio::sync::broadcast::Receiver<()>) {
    let mut interval = tokio::time::interval(CHECK_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut thorough = true;
    loop {
        tokio::select! {
            _ = interval.tick() => {
                let pass_state = state.clone();
                // Every step is synchronous SQLite and file work, and a full
                // check of a large file takes seconds: keep it off the
                // runtime's worker threads.
                let _ = tokio::task::spawn_blocking(move || upkeep(&pass_state, thorough)).await;
                thorough = false;
            }
            _ = shutdown.recv() => break,
        }
    }
}

fn upkeep(state: &ApiState, thorough: bool) {
    let healthy = check(state, thorough);
    prune(state);
    if healthy {
        backup_if_due(state);
    }
}

/// Checks the file and records the outcome; true when it is healthy.
fn check(state: &ApiState, thorough: bool) -> bool {
    let outcome = state
        .metadata()
        .map_err(|error| error.to_string())
        .and_then(|metadata| {
            metadata
                .integrity_problems(thorough)
                .map_err(|error| format!("{error:#}"))
        });
    let problems = match outcome {
        Ok(problems) => problems,
        // A check that cannot run is not a clean check. The file may simply
        // be busy, so this is reported but not called damage.
        Err(error) => {
            pintail_log::log_error!("metadata integrity check could not run: {error}");
            return false;
        }
    };
    let now = Utc::now().to_rfc3339();
    let damaged = !problems.is_empty();
    let newly_damaged = damaged && current().state != "damaged";
    if damaged {
        // Error level, so it reaches Sentry. Once per transition, with the
        // first findings; the dashboard carries the rest.
        if newly_damaged {
            pintail_log::log_error!(
                "metadata damaged: {} problem(s) found, first: {}",
                problems.len(),
                problems.first().map_or("", String::as_str)
            );
            state.publish(ApiEvent::database(
                "metadata.damaged",
                "control-plane",
                format!(
                    "the control-plane metadata file failed its integrity check \
                     ({} problem(s)); copies in {BACKUP_DIRECTORY}/ predate the damage",
                    problems.len()
                ),
            ));
        }
    } else if current().state == "damaged" {
        pintail_log::log_info!("metadata integrity check is clean again");
    }
    update(|health| {
        health.state = if damaged { "damaged" } else { "ok" };
        health.checked_at = Some(now);
        health.problems = problems.into_iter().take(20).collect();
    });
    !damaged
}

fn prune(state: &ApiState) {
    let now = Utc::now();
    let cycles_since = (now - CYCLE_RETENTION).to_rfc3339();
    let history_since = (now - HISTORY_RETENTION).to_rfc3339();
    match state
        .metadata()
        .map_err(|error| error.to_string())
        .and_then(|metadata| {
            metadata
                .prune_sync_runs(&cycles_since, &history_since)
                .map_err(|error| format!("{error:#}"))
        }) {
        Ok(0) => {}
        Ok(removed) => pintail_log::log_info!("pruned {removed} sync run(s) past retention"),
        Err(error) => pintail_log::log_error!("sync run pruning failed: {error}"),
    }
}

fn backup_if_due(state: &ApiState) {
    let Ok(data_dir) = state.data_dir() else {
        return;
    };
    let directory = data_dir.join(BACKUP_DIRECTORY);
    let period = Duration::from_secs(
        env_number("PINTAIL_META_BACKUP_HOURS", DEFAULT_BACKUP_HOURS).saturating_mul(3600),
    );
    let keep = env_number("PINTAIL_META_BACKUP_KEEP", DEFAULT_BACKUP_KEEP);
    if period.is_zero() || keep == 0 {
        return;
    }
    let existing = backups(&directory);
    let newest = existing
        .last()
        .and_then(|path| path.metadata().ok())
        .and_then(|metadata| metadata.modified().ok());
    let due = newest.is_none_or(|written| {
        SystemTime::now()
            .duration_since(written)
            .is_ok_and(|age| age >= period)
    });
    if !due {
        update(|health| health.last_backup_at = newest.map(rfc3339));
        return;
    }
    match write_backup(state, &directory) {
        Ok(path) => {
            pintail_log::log_info!("metadata backup written to {}", path.display());
            for stale in backups(&directory).iter().rev().skip(keep) {
                if let Err(error) = std::fs::remove_file(stale) {
                    pintail_log::log_error!(
                        "could not remove old metadata backup {}: {error}",
                        stale.display()
                    );
                }
            }
            update(|health| {
                health.last_backup_at = Some(Utc::now().to_rfc3339());
                health.backup_error = None;
            });
        }
        Err(error) => {
            pintail_log::log_error!("metadata backup failed: {error}");
            update(|health| {
                health.last_backup_at = newest.map(rfc3339);
                health.backup_error = Some(error);
            });
        }
    }
}

fn write_backup(state: &ApiState, directory: &Path) -> Result<PathBuf, String> {
    std::fs::create_dir_all(directory)
        .map_err(|error| format!("could not create {}: {error}", directory.display()))?;
    let target = directory.join(format!(
        "{BACKUP_PREFIX}{}.db",
        Utc::now().format("%Y%m%dT%H%M%SZ")
    ));
    state
        .metadata()
        .map_err(|error| error.to_string())?
        .backup_into(&target)
        .map_err(|error| format!("{error:#}"))?;
    Ok(target)
}

/// Completed copies in `directory`, oldest first. The timestamped names sort
/// chronologically; partial copies are not counted.
fn backups(directory: &Path) -> Vec<PathBuf> {
    let Ok(entries) = std::fs::read_dir(directory) else {
        return Vec::new();
    };
    let mut found = entries
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| {
                    name.starts_with(BACKUP_PREFIX)
                        && Path::new(name)
                            .extension()
                            .is_some_and(|extension| extension.eq_ignore_ascii_case("db"))
                })
        })
        .collect::<Vec<_>>();
    found.sort();
    found
}

fn rfc3339(time: SystemTime) -> String {
    chrono::DateTime::<Utc>::from(time).to_rfc3339()
}

#[cfg(test)]
mod tests {
    use super::backups;

    #[test]
    fn only_completed_copies_count_and_they_sort_oldest_first() {
        let dir = tempfile::tempdir().expect("temporary directory");
        for name in [
            "pintail-meta-20260102T000000Z.db",
            "pintail-meta-20260101T000000Z.db",
            "pintail-meta-20260103T000000Z.db.partial",
            "unrelated.db",
        ] {
            std::fs::write(dir.path().join(name), b"").expect("write");
        }
        let names = backups(dir.path())
            .iter()
            .map(|path| {
                path.file_name()
                    .expect("named")
                    .to_string_lossy()
                    .into_owned()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            [
                "pintail-meta-20260101T000000Z.db",
                "pintail-meta-20260102T000000Z.db"
            ]
        );
    }
}
