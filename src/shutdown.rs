//! Cross-platform helper for waiting on a graceful-shutdown signal.
//!
//! Docker, Kubernetes and systemd stop a service by sending SIGTERM, while a terminal sends
//! SIGINT on Ctrl+C. `tokio::signal::unix` (needed to observe SIGTERM) does not exist on
//! Windows, so this waits on whichever of SIGINT or SIGTERM arrives first on Unix, and falls
//! back to SIGINT alone everywhere else.

/// Waits for a shutdown signal: SIGINT, and on Unix platforms also SIGTERM. Returns as soon
/// as either arrives.
pub async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};

        let mut sigterm = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(
                    "Failed to install SIGTERM handler ({}); shutdown will only respond to Ctrl+C.",
                    e
                );
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };

        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = sigterm.recv() => {}
        }
    }

    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
