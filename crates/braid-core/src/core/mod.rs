//! Braid HTTP Protocol Implementation for Rust (Core + Server)

pub mod error;
pub mod merge;
#[cfg(feature = "server")]
pub mod server;
pub mod traits;

// Re-export from braid-http
pub use braid_http::client::{
    BraidClient, ClientConfig, HeartbeatConfig, Message, MessageParser, ParseState, RetryConfig,
    RetryDecision, RetryState, Subscription, SubscriptionStream,
};
pub use braid_http::error::{BraidError as ClientError, Result as ClientResult};
pub use braid_http::protocol as protocol_mod;
pub use braid_http::types::{BraidRequest, BraidResponse, ContentRange, Patch, Update, Version};

// Re-export local error/types if needed, or unify.
pub use error::{BraidError, Result};
