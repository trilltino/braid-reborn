//! Braid HTTP server implementation.

mod config;
mod middleware;
mod parse_update;
mod send_update;
pub mod utils;

pub mod conflict_resolver;

pub mod multiplex;
pub mod resource_state;
pub mod subscription;

pub use config::ServerConfig;
pub use conflict_resolver::ConflictResolver;
pub use middleware::{BraidLayer, BraidState, IsFirefox};
pub use parse_update::ParseUpdateExt;
pub use resource_state::ResourceStateManager;
pub use send_update::{SendUpdateExt, UpdateResponse};

use crate::core::Update;
use std::sync::Arc;
use tokio::sync::broadcast;

/// Broadcast channel for sending updates to multiple subscribers.
pub type UpdateBroadcast = broadcast::Sender<Arc<Update>>;
pub use send_update::BraidUpdate;
