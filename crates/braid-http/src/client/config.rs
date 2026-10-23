//! Configuration for the Braid HTTP client.

/// Configuration for the Braid HTTP client.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientConfig {
    /// Maximum retries for failed requests.
    pub max_retries: u32,
    /// Base retry delay in milliseconds.
    pub retry_delay_ms: u64,
    /// Connection timeout in seconds.
    pub connection_timeout_secs: u64,
    /// Enable request logging.
    pub enable_logging: bool,
    /// Maximum concurrent subscriptions.
    pub max_subscriptions: usize,
    /// Threshold for auto-multiplexing.
    pub auto_multiplex_threshold: usize,
    /// Enable multiplexing for subscription requests.
    pub enable_multiplex: bool,
    /// Proxy URL (optional).
    pub proxy_url: String,
    /// Request timeout in milliseconds.
    pub request_timeout_ms: u64,
    /// Maximum total connections in the pool.
    pub max_total_connections: u32,
}

impl Default for ClientConfig {
    fn default() -> Self {
        ClientConfig {
            max_retries: 3,
            retry_delay_ms: 1000,
            connection_timeout_secs: 30,
            enable_logging: false,
            max_subscriptions: 100,
            auto_multiplex_threshold: 3,
            enable_multiplex: true,
            proxy_url: String::new(),
            request_timeout_ms: 30000,
            max_total_connections: 100,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ClientConfig::default();
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.retry_delay_ms, 1000);
        assert_eq!(config.connection_timeout_secs, 30);
        assert!(!config.enable_logging);
        assert_eq!(config.max_subscriptions, 100);
        assert_eq!(config.auto_multiplex_threshold, 3);
        assert!(config.enable_multiplex);
        assert_eq!(config.proxy_url, "");
        assert_eq!(config.request_timeout_ms, 30000);
        assert_eq!(config.max_total_connections, 100);
    }

    #[test]
    fn test_custom_config() {
        let config = ClientConfig {
            max_retries: 5,
            retry_delay_ms: 2000,
            connection_timeout_secs: 60,
            enable_logging: true,
            max_subscriptions: 50,
            auto_multiplex_threshold: 5,
            enable_multiplex: false,
            proxy_url: "http://proxy".to_string(),
            request_timeout_ms: 1000,
            max_total_connections: 40,
        };
        assert_eq!(config.max_retries, 5);
        assert_eq!(config.retry_delay_ms, 2000);
        assert_eq!(config.connection_timeout_secs, 60);
        assert!(config.enable_logging);
        assert_eq!(config.max_subscriptions, 50);
        assert_eq!(config.auto_multiplex_threshold, 5);
        assert!(!config.enable_multiplex);
        assert_eq!(config.proxy_url, "http://proxy");
        assert_eq!(config.request_timeout_ms, 1000);
        assert_eq!(config.max_total_connections, 40);
    }

    #[test]
    fn test_partial_override() {
        let config = ClientConfig {
            max_retries: 10,
            ..Default::default()
        };
        assert_eq!(config.max_retries, 10);
        assert_eq!(config.retry_delay_ms, 1000);
    }

    #[test]
    fn test_clone() {
        let config = ClientConfig::default();
        let cloned = config.clone();
        assert_eq!(config, cloned);
    }

    #[test]
    fn test_debug() {
        let config = ClientConfig::default();
        let debug = format!("{:?}", config);
        assert!(debug.contains("ClientConfig"));
        assert!(debug.contains("max_retries"));
    }
}
