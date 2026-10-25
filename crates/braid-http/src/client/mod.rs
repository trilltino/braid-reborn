//! Braid HTTP client implementation.

#[cfg(not(target_arch = "wasm32"))]
mod channel;
mod config;
mod credentials;
#[cfg(not(target_arch = "wasm32"))]
pub use channel::{
    ChannelCheckpoint, ChannelConfig, ChannelJournal, ChannelState, ChannelStatus, ReliableChannel,
};
pub use credentials::{http_origin, validate_resource_url, OriginCredentials, ScopedCredentials};
mod fetch;
#[cfg(test)]
mod fuzzer;
mod headers;
mod multiplex;
#[cfg(not(target_arch = "wasm32"))]
pub mod native_network;
mod parser;
#[cfg(not(target_arch = "wasm32"))]
mod reliable;
pub mod retry;
#[cfg(not(target_arch = "wasm32"))]
pub use reliable::ReliableSubscription;
mod subscription;
pub(crate) mod utils;
#[cfg(target_arch = "wasm32")]
pub mod wasm_network;

pub use config::ClientConfig;
pub use fetch::BraidClient;
pub use headers::{BraidHeaders, HeaderParser};
pub use parser::{parse_status_line, Message, MessageParser, ParseState, ParserLimits};
pub use retry::{parse_retry_after, RetryConfig, RetryDecision, RetryState};
pub use subscription::{HeartbeatConfig, Subscription, SubscriptionStream};
pub use utils::*;
