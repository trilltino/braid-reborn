//! Braid HTTP client implementation.

mod channel;
mod config;
mod credentials;
mod journal;
mod payload;
mod runtime;
pub use channel::{
    ChannelConfig, ChannelDiagnostics, ChannelEvent, ChannelState, ChannelStatus, ReliableChannel,
    RepairCause, RetryReason,
};
pub use credentials::{http_origin, validate_resource_url, OriginCredentials, ScopedCredentials};
#[cfg(not(target_arch = "wasm32"))]
pub use journal::FileJournal;
pub use journal::{ChannelCheckpoint, ChannelJournal, JournalBackend, MAX_JOURNAL_BYTES};
pub(crate) use payload::VerifiedPayload;
pub use payload::{collect_payload, PayloadRef, PayloadSource, PayloadStream};
pub use runtime::OwnedTask;
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
