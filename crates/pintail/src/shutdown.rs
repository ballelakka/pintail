//! A shutdown that always finishes.
//!
//! Graceful shutdown waits for every open connection to end, and some never
//! do on their own: an event stream a dashboard holds open answers forever,
//! so one browser tab kept a server alive through SIGTERM for over eleven
//! minutes until it was killed. The signal starts a grace period; whatever
//! is still open when it runs out is dropped.

use std::{future::Future, time::Duration};

/// How long open work may take to finish after the signal. Short of the ten
/// seconds a container runtime waits before it kills the process, so the
/// server leaves on its own terms.
pub const GRACE: Duration = Duration::from_secs(5);

/// How long blocking tasks (a statement mid-execution, a metadata check)
/// get once serving has stopped. Together with [`GRACE`] this stays inside
/// the runtime's ten seconds.
pub const BLOCKING_GRACE: Duration = Duration::from_secs(2);

/// Runs `serve` to completion, or until `grace` has passed since `signalled`
/// resolved. `None` means the grace period ran out and `serve` was dropped
/// with work still open.
pub async fn finish_within<T>(
    serve: impl Future<Output = T>,
    signalled: impl Future<Output = ()>,
    grace: Duration,
) -> Option<T> {
    tokio::pin!(serve);
    tokio::select! {
        biased;
        output = &mut serve => return Some(output),
        () = signalled => {}
    }
    tokio::time::timeout(grace, serve).await.ok()
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::finish_within;

    #[tokio::test]
    async fn work_that_never_ends_is_dropped_after_the_grace_period() {
        let started = Instant::now();
        let finished = finish_within(
            std::future::pending::<()>(),
            tokio::time::sleep(Duration::from_millis(50)),
            Duration::from_millis(100),
        )
        .await;
        assert_eq!(finished, None);
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[tokio::test]
    async fn work_that_ends_within_the_grace_period_keeps_its_result() {
        let finished = finish_within(
            async {
                tokio::time::sleep(Duration::from_millis(50)).await;
                7
            },
            std::future::ready(()),
            Duration::from_secs(5),
        )
        .await;
        assert_eq!(finished, Some(7));
    }

    #[tokio::test]
    async fn work_finished_before_any_signal_returns_at_once() {
        let finished = finish_within(
            std::future::ready(3),
            std::future::pending::<()>(),
            Duration::ZERO,
        )
        .await;
        assert_eq!(finished, Some(3));
    }
}
