//! Exclusive ownership of the data directory.
//!
//! Two servers writing one data directory corrupt it quietly: each keeps its
//! own view of the metadata database and the table stores, and whichever
//! writes last wins row by row. A redeploy that starts the new process
//! before the old one has exited is the usual way to get there. The first
//! thing a server does is take an OS lock on a file in the directory and
//! hold it for its whole life; a second server waits for it, then refuses.
//!
//! The lock cannot outlive its owner and cannot wedge a boot:
//! - It is an advisory lock on an open file, which the kernel drops when
//!   the owning process ends however it ends, SIGKILL and OOM kill
//!   included. There is no pid file to go stale and nothing to clean up.
//! - The file is opened close-on-exec, so no child process inherits it.
//! - Nothing else in the server takes this lock, and it is taken before
//!   any other, so there is no ordering to get wrong.
//! - The wait is bounded. A server that cannot get the directory exits
//!   and says who holds it; it never blocks forever.
//! - A filesystem that does not implement locks gets a loud warning and
//!   an unlocked boot rather than a server that refuses to start.

use std::{
    ffi::OsString,
    fs::{File, OpenOptions},
    io::{ErrorKind, Seek, SeekFrom, Write},
    path::Path,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use fs2::FileExt;

const LOCK_FILE: &str = ".pintail.lock";
const RETRY_STEP: Duration = Duration::from_millis(250);
const WAIT_VARIABLE: &str = "PINTAIL_DATA_DIR_LOCK_WAIT_SECONDS";

/// How long a starting server waits for a previous one to let go. A
/// redeploy's old process is normally gone within its stop grace period.
pub const DEFAULT_WAIT: Duration = Duration::from_secs(120);

/// Held for the life of the process; dropping it releases the directory.
#[derive(Debug)]
pub struct DataDirLock {
    file: Option<File>,
}

impl DataDirLock {
    /// Whether the directory is actually locked; false only on a filesystem
    /// without lock support.
    #[must_use]
    pub fn is_held(&self) -> bool {
        self.file.is_some()
    }
}

/// The wait from `PINTAIL_DATA_DIR_LOCK_WAIT_SECONDS`, or [`DEFAULT_WAIT`].
///
/// # Errors
///
/// Fails when the variable is set to something other than a non-negative
/// number of seconds.
pub fn wait_from_environment() -> Result<Duration> {
    wait_from(std::env::var_os(WAIT_VARIABLE))
}

fn wait_from(value: Option<OsString>) -> Result<Duration> {
    let Some(value) = value.filter(|value| !value.is_empty()) else {
        return Ok(DEFAULT_WAIT);
    };
    let seconds = value
        .to_str()
        .and_then(|value| value.trim().parse::<f64>().ok())
        .filter(|seconds| seconds.is_finite() && *seconds >= 0.0)
        .with_context(|| format!("{WAIT_VARIABLE} must be a non-negative number of seconds"))?;
    Ok(Duration::from_secs_f64(seconds))
}

/// Takes the data directory, waiting up to `wait` for a current owner to
/// exit. The lock file records the owner's process id for whoever finds
/// the directory busy.
///
/// # Errors
///
/// Fails when the lock file cannot be opened, or another process still
/// owns the directory once `wait` has passed.
pub fn acquire(data_dir: &Path, wait: Duration) -> Result<DataDirLock> {
    std::fs::create_dir_all(data_dir)
        .with_context(|| format!("failed to create data directory {}", data_dir.display()))?;
    let path = data_dir.join(LOCK_FILE);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .with_context(|| format!("failed to open data directory lock {}", path.display()))?;
    let started = Instant::now();
    let mut announced = false;
    loop {
        match FileExt::try_lock_exclusive(&file) {
            Ok(()) => break,
            Err(error) if error.kind() == fs2::lock_contended_error().kind() => {
                let owner = std::fs::read_to_string(&path).unwrap_or_default();
                let owner = owner.trim();
                let waited = started.elapsed();
                if waited >= wait {
                    bail!(
                        "data directory {} is in use by another pintail process ({owner}) and \
                         was not released within {:.0}s; two servers must never share one data \
                         directory, so stop the other one (or raise {WAIT_VARIABLE} if it is \
                         still shutting down)",
                        data_dir.display(),
                        wait.as_secs_f64()
                    );
                }
                if !announced {
                    pintail_log::log_info!(
                        "data directory {} is held by another process ({owner}); waiting up to {:.0}s for it to exit",
                        data_dir.display(),
                        wait.as_secs_f64()
                    );
                    announced = true;
                }
                std::thread::sleep(RETRY_STEP.min(wait.saturating_sub(waited)));
            }
            Err(error) if lock_unsupported(&error) => {
                pintail_log::log_error!(
                    "the filesystem holding {} does not support file locks ({error}); starting \
                     without data directory ownership, so make sure only one server uses it",
                    data_dir.display()
                );
                return Ok(DataDirLock { file: None });
            }
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("failed to lock data directory {}", path.display()));
            }
        }
    }
    // Diagnostic only: failing to record the owner never costs the lock.
    let recorded = file
        .set_len(0)
        .and_then(|()| file.seek(SeekFrom::Start(0)))
        .and_then(|_| writeln!(file, "pid {}", std::process::id()));
    if let Err(error) = recorded {
        pintail_log::log_info!("could not record the owner in {}: {error}", path.display());
    }
    Ok(DataDirLock { file: Some(file) })
}

/// ENOLCK and EOPNOTSUPP mean the filesystem has no lock support, not that
/// someone else holds the lock.
fn lock_unsupported(error: &std::io::Error) -> bool {
    #[cfg(target_os = "linux")]
    const CODES: &[i32] = &[37, 95];
    #[cfg(target_os = "macos")]
    const CODES: &[i32] = &[77, 45, 102];
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    const CODES: &[i32] = &[];
    error.kind() == ErrorKind::Unsupported
        || error
            .raw_os_error()
            .is_some_and(|code| CODES.contains(&code))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_second_owner_is_refused_until_the_first_lets_go() {
        let dir = tempfile::tempdir().expect("temporary data directory");
        let first = acquire(dir.path(), Duration::ZERO).expect("first owner");
        assert!(first.is_held());
        let started = Instant::now();
        let refused = acquire(dir.path(), Duration::from_millis(300))
            .expect_err("second owner must be refused");
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the wait is bounded"
        );
        assert!(
            refused
                .to_string()
                .contains("in use by another pintail process")
        );
        assert!(
            refused
                .to_string()
                .contains(&format!("pid {}", std::process::id()))
        );
        drop(first);
        acquire(dir.path(), Duration::ZERO).expect("free once released");
    }

    #[test]
    fn a_waiting_owner_takes_the_directory_as_soon_as_it_is_released() {
        let dir = tempfile::tempdir().expect("temporary data directory");
        let first = acquire(dir.path(), Duration::ZERO).expect("first owner");
        let release = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(200));
            drop(first);
        });
        acquire(dir.path(), Duration::from_secs(30)).expect("taken after release");
        release.join().expect("release thread");
    }

    #[test]
    fn the_wait_reads_seconds_and_rejects_nonsense() {
        assert_eq!(wait_from(None).unwrap(), DEFAULT_WAIT);
        assert_eq!(wait_from(Some("".into())).unwrap(), DEFAULT_WAIT);
        assert_eq!(
            wait_from(Some("2.5".into())).unwrap(),
            Duration::from_millis(2500)
        );
        assert_eq!(wait_from(Some("0".into())).unwrap(), Duration::ZERO);
        assert!(wait_from(Some("-1".into())).is_err());
        assert!(wait_from(Some("soon".into())).is_err());
    }
}
