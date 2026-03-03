//! Retry configuration and logic for Braid HTTP client.

use std::time::Duration;

/// Configuration for retry behavior.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts (None = infinite)
    pub max_retries: Option<u32>,
    /// Initial backoff duration
    pub initial_backoff: Duration,
    /// Maximum backoff duration
    pub max_backoff: Duration,
    /// HTTP status codes that trigger a retry
    pub retry_on_status: Vec<u16>,
    /// Whether to respect the `Retry-After` header
    pub respect_retry_after: bool,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: None,
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(3),
            retry_on_status: vec![408, 425, 429, 502, 503, 504],
            respect_retry_after: true,
        }
    }
}

impl RetryConfig {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn no_retry() -> Self {
        Self {
            max_retries: Some(0),
            ..Default::default()
        }
    }

    #[must_use]
    pub fn with_max_retries(mut self, max: u32) -> Self {
        self.max_retries = Some(max);
        self
    }

    #[must_use]
    pub fn with_initial_backoff(mut self, duration: Duration) -> Self {
        self.initial_backoff = duration;
        self
    }

    #[must_use]
    pub fn with_max_backoff(mut self, duration: Duration) -> Self {
        self.max_backoff = duration;
        self
    }

    #[must_use]
    pub fn with_retry_on_status(mut self, status: u16) -> Self {
        if !self.retry_on_status.contains(&status) {
            self.retry_on_status.push(status);
        }
        self
    }

    #[must_use]
    pub fn with_respect_retry_after(mut self, respect: bool) -> Self {
        self.respect_retry_after = respect;
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum RetryDecision {
    Retry(Duration),
    DontRetry,
}

#[derive(Debug, Clone)]
pub struct RetryState {
    pub attempts: u32,
    pub current_backoff: Duration,
    config: RetryConfig,
}

impl RetryState {
    pub fn new(config: RetryConfig) -> Self {
        Self {
            attempts: 0,
            current_backoff: config.initial_backoff,
            config,
        }
    }

    pub fn should_retry_error(&mut self, is_abort: bool) -> RetryDecision {
        if is_abort {
            return RetryDecision::DontRetry;
        }
        self.decide_retry(None)
    }

    pub fn should_retry_status(
        &mut self,
        status: u16,
        retry_after: Option<Duration>,
    ) -> RetryDecision {
        if !self.config.retry_on_status.contains(&status) {
            return RetryDecision::DontRetry;
        }
        self.decide_retry(retry_after)
    }

    pub fn should_retry_status_with_text(
        &mut self,
        status: u16,
        status_text: Option<&str>,
        retry_after: Option<Duration>,
    ) -> RetryDecision {
        if let Some(text) = status_text {
            if text.to_lowercase().contains("missing parents") {
                return self.decide_retry(retry_after);
            }
        }
        self.should_retry_status(status, retry_after)
    }

    fn decide_retry(&mut self, retry_after: Option<Duration>) -> RetryDecision {
        self.attempts += 1;
        if let Some(max) = self.config.max_retries {
            if self.attempts > max {
                return RetryDecision::DontRetry;
            }
        }

        let wait = if self.config.respect_retry_after {
            retry_after.unwrap_or(self.current_backoff)
        } else {
            self.current_backoff
        };

        self.current_backoff = std::cmp::min(
            self.current_backoff + Duration::from_secs(1),
            self.config.max_backoff,
        );

        RetryDecision::Retry(wait)
    }

    pub fn reset(&mut self) {
        self.attempts = 0;
        self.current_backoff = self.config.initial_backoff;
    }
}

pub fn parse_retry_after(value: &str) -> Option<Duration> {
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = RetryConfig::default();
        assert_eq!(config.max_retries, None);
        assert!(config.retry_on_status.contains(&503));
    }

    #[test]
    fn test_retry_state_basic() {
        let config = RetryConfig::default().with_max_retries(1);
        let mut state = RetryState::new(config);
        assert!(matches!(
            state.should_retry_error(false),
            RetryDecision::Retry(_)
        ));
        assert_eq!(state.should_retry_error(false), RetryDecision::DontRetry);
    }
}
