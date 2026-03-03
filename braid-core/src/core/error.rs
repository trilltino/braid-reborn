//! Error types for Braid HTTP operations.

use std::io;
use thiserror::Error;

/// Result type for Braid HTTP operations.
pub type Result<T> = std::result::Result<T, BraidError>;

/// Errors that can occur during Braid HTTP operations.
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum BraidError {
    #[error("HTTP error: {0}")]
    Http(String),

    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("Header parse error: {0}")]
    HeaderParse(String),

    #[error("Body parse error: {0}")]
    BodyParse(String),

    #[error("Invalid version: {0}")]
    InvalidVersion(String),

    #[error("Subscription error: {0}")]
    Subscription(String),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Subscription closed")]
    SubscriptionClosed,

    #[error("Expected status 209 for subscription, got {0}")]
    InvalidSubscriptionStatus(u16),

    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("Operation timed out")]
    Timeout,

    #[error("Request aborted")]
    Aborted,

    #[error("BraidFS Error: {0}")]
    Fs(String),

    #[error("Invalid UTF-8: {0}")]
    InvalidUtf8(#[from] std::string::FromUtf8Error),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Server has dropped history")]
    HistoryDropped,

    #[error("Conflicting versions in merge: {0}")]
    MergeConflict(String),

    #[cfg(all(feature = "native", not(target_arch = "wasm32")))]
    #[error("Notify error: {0}")]
    Notify(#[from] notify::Error),

    #[error("Anyhow error: {0}")]
    Anyhow(String),

    #[error("Client error: {0}")]
    Client(#[from] braid_http::BraidError),
}

impl From<anyhow::Error> for BraidError {
    fn from(err: anyhow::Error) -> Self {
        BraidError::Anyhow(err.to_string())
    }
}

impl BraidError {
    /// Check if this error is retryable.
    #[inline]
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        match self {
            BraidError::Http(msg) => {
                msg.contains("408")
                    || msg.contains("425")
                    || msg.contains("429")
                    || msg.contains("502")
                    || msg.contains("503")
                    || msg.contains("504")
            }
            BraidError::Timeout | BraidError::Io(_) => true,
            BraidError::HistoryDropped => false,
            _ => false,
        }
    }

    /// Check if this is an access denied error.
    #[inline]
    #[must_use]
    pub fn is_access_denied(&self) -> bool {
        match self {
            BraidError::Http(msg) => msg.contains("401") || msg.contains("403"),
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timeout_is_retryable() {
        assert!(BraidError::Timeout.is_retryable());
    }

    #[test]
    fn test_history_dropped_not_retryable() {
        assert!(!BraidError::HistoryDropped.is_retryable());
    }

    #[test]
    fn test_http_503_is_retryable() {
        let err = BraidError::Http("503 Service Unavailable".into());
        assert!(err.is_retryable());
    }

    #[test]
    fn test_access_denied_401() {
        let err = BraidError::Http("401 Unauthorized".into());
        assert!(err.is_access_denied());
    }
}
