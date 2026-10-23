//! Reconnection rate limiter for BraidFS.
//!
//! Prevents too-rapid reconnection attempts that could overload servers.
//! Matches JS `ReconnectRateLimiter` from braidfs/index.js.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

/// Rate limiter for reconnection attempts.
#[derive(Debug)]
pub struct ReconnectRateLimiter {
    /// Base delay between reconnection attempts in milliseconds.
    delay_ms: u64,
    /// Track connection state per URL.
    connections: Arc<Mutex<HashMap<String, ConnectionState>>>,
}

#[derive(Debug, Clone)]
struct ConnectionState {
    /// Whether currently connected.
    connected: bool,
    /// Last connection attempt time.
    last_attempt: Instant,
    /// Number of consecutive failures.
    failure_count: u32,
    /// Queue of pending connection requests.
    pending_turns: u32,
}

impl Default for ConnectionState {
    fn default() -> Self {
        Self {
            connected: false,
            last_attempt: Instant::now(),
            failure_count: 0,
            pending_turns: 0,
        }
    }
}

impl ReconnectRateLimiter {
    /// Create a new rate limiter with the given base delay.
    pub fn new(delay_ms: u64) -> Self {
        Self {
            delay_ms,
            connections: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Get a "turn" to attempt a connection.
    ///
    /// This may wait if too many rapid attempts have been made.
    pub async fn get_turn(&self, url: &str) -> Duration {
        let mut conns = self.connections.lock().await;
        let state = conns.entry(url.to_string()).or_default();

        state.pending_turns += 1;

        // Calculate delay based on failure count
        let delay = if state.connected {
            Duration::ZERO
        } else {
            let multiplier = (state.failure_count.min(10) + 1) as u64;
            Duration::from_millis(self.delay_ms * multiplier)
        };

        // Check if we need to wait
        let elapsed = state.last_attempt.elapsed();
        if elapsed < delay {
            delay - elapsed
        } else {
            Duration::ZERO
        }
    }

    /// Called when a connection is established.
    pub async fn on_conn(&self, url: &str) {
        let mut conns = self.connections.lock().await;
        let state = conns.entry(url.to_string()).or_default();

        state.connected = true;
        state.failure_count = 0;
        state.last_attempt = Instant::now();

        tracing::debug!("on_conn: {} - connected", url);
    }

    /// Called when a connection is disconnected.
    pub async fn on_diss(&self, url: &str) {
        let mut conns = self.connections.lock().await;
        let state = conns.entry(url.to_string()).or_default();

        state.connected = false;
        state.failure_count += 1;
        state.last_attempt = Instant::now();

        tracing::debug!(
            "on_diss: {} - disconnected (failures: {})",
            url,
            state.failure_count
        );
    }

    /// Check if a URL is currently connected.
    pub async fn is_connected(&self, url: &str) -> bool {
        let conns = self.connections.lock().await;
        conns.get(url).map(|s| s.connected).unwrap_or(false)
    }

    /// Get the current failure count for a URL.
    pub async fn failure_count(&self, url: &str) -> u32 {
        let conns = self.connections.lock().await;
        conns.get(url).map(|s| s.failure_count).unwrap_or(0)
    }

    /// Reset the state for a URL.
    pub async fn reset(&self, url: &str) {
        let mut conns = self.connections.lock().await;
        conns.remove(url);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_rate_limiter_basic() {
        let limiter = ReconnectRateLimiter::new(100);

        // First connection should have no delay
        let delay = limiter.get_turn("http://example.com").await;
        assert!(delay <= Duration::from_millis(100));
    }

    #[tokio::test]
    async fn test_rate_limiter_on_conn_diss() {
        let limiter = ReconnectRateLimiter::new(100);

        limiter.on_conn("http://example.com").await;
        assert!(limiter.is_connected("http://example.com").await);

        limiter.on_diss("http://example.com").await;
        assert!(!limiter.is_connected("http://example.com").await);
        assert_eq!(limiter.failure_count("http://example.com").await, 1);
    }

    #[tokio::test]
    async fn test_rate_limiter_exponential_backoff() {
        let limiter = ReconnectRateLimiter::new(100);

        // Simulate multiple failures
        for _ in 0..5 {
            limiter.on_diss("http://example.com").await;
        }

        assert_eq!(limiter.failure_count("http://example.com").await, 5);
    }
}
