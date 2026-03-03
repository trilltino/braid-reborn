//! Server configuration for Braid-HTTP support.
//!
//! This module defines the [`ServerConfig`] struct that controls the behavior
//! of the Braid-HTTP server middleware, including subscription support,
//! heartbeat intervals, and multiplexing capabilities.
//!
//! # Configuration Options
//!
//! | Option | Default | Description |
//! |--------|---------|-------------|
//! | `enable_subscriptions` | true | Enable HTTP 209 subscriptions |
//! | `max_subscriptions` | 1000 | Max concurrent subscriptions |
//! | `max_subscription_duration_secs` | 3600 | Max subscription lifetime |
//! | `heartbeat_interval` | 30 | Heartbeat interval (seconds) |
//! | `enable_multiplex` | false | Enable request multiplexing |
//!
//! # Examples
//!
//! ## Default Configuration
//!
//! ```
//! use crate::core::server::ServerConfig;
//!
//! let config = ServerConfig::default();
//! assert!(config.enable_subscriptions);
//! assert_eq!(config.max_subscriptions, 1000);
//! ```
//!
//! ## Custom Configuration
//!
//! ```
//! use crate::core::server::ServerConfig;
//!
//! let config = ServerConfig {
//!     enable_subscriptions: true,
//!     max_subscriptions: 5000,
//!     max_subscription_duration_secs: 7200,
//!     heartbeat_interval: 60,
//!     enable_multiplex: true,
//! };
//! ```
//!
//! ## Partial Override
//!
//! ```
//! use crate::core::server::ServerConfig;
//!
//! let config = ServerConfig {
//!     max_subscriptions: 5000,
//!     ..Default::default()
//! };
//! assert_eq!(config.max_subscriptions, 5000);
//! assert_eq!(config.heartbeat_interval, 30); // Default
//! ```

/// Server configuration for Braid-HTTP support.
///
/// Controls subscription behavior, heartbeat intervals, and multiplexing
/// for the Braid server middleware.
///
/// # Example
///
/// ```
/// use crate::core::server::ServerConfig;
///
/// let config = ServerConfig {
///     enable_subscriptions: true,
///     max_subscriptions: 5000,
///     ..Default::default()
/// };
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerConfig {
    /// Enable subscription support (HTTP 209).
    ///
    /// When enabled, clients can request subscriptions using the
    /// `Subscribe: true` header and receive streaming updates.
    pub enable_subscriptions: bool,

    /// Maximum concurrent subscriptions.
    ///
    /// Limits the number of active subscription connections to prevent
    /// resource exhaustion.
    pub max_subscriptions: usize,

    /// Maximum subscription duration in seconds.
    ///
    /// Subscriptions will be automatically closed after this duration.
    /// Set to 0 for unlimited duration.
    pub max_subscription_duration_secs: u64,

    /// Heartbeat interval in seconds.
    ///
    /// The server sends heartbeat messages at this interval to keep
    /// subscription connections alive.
    pub heartbeat_interval: u64,

    /// Enable request multiplexing.
    ///
    /// When enabled, multiple requests can be multiplexed over a single
    /// connection using HTTP 293 status code.
    pub enable_multiplex: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            enable_subscriptions: true,
            max_subscriptions: 1000,
            max_subscription_duration_secs: 3600,
            heartbeat_interval: 30,
            enable_multiplex: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ServerConfig::default();
        assert!(config.enable_subscriptions);
        assert_eq!(config.max_subscriptions, 1000);
        assert_eq!(config.max_subscription_duration_secs, 3600);
        assert_eq!(config.heartbeat_interval, 30);
        assert!(!config.enable_multiplex);
    }

    #[test]
    fn test_custom_config() {
        let config = ServerConfig {
            enable_subscriptions: false,
            max_subscriptions: 5000,
            max_subscription_duration_secs: 7200,
            heartbeat_interval: 60,
            enable_multiplex: true,
        };
        assert!(!config.enable_subscriptions);
        assert_eq!(config.max_subscriptions, 5000);
        assert!(config.enable_multiplex);
    }

    #[test]
    fn test_partial_override() {
        let config = ServerConfig {
            max_subscriptions: 5000,
            ..Default::default()
        };
        assert_eq!(config.max_subscriptions, 5000);
        assert!(config.enable_subscriptions);
        assert_eq!(config.heartbeat_interval, 30);
    }

    #[test]
    fn test_clone() {
        let config = ServerConfig::default();
        let cloned = config.clone();
        assert_eq!(config, cloned);
    }

    #[test]
    fn test_debug() {
        let config = ServerConfig::default();
        let debug = format!("{:?}", config);
        assert!(debug.contains("ServerConfig"));
        assert!(debug.contains("enable_subscriptions"));
    }
}
