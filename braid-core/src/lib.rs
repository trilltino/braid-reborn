//! braid-core: Unified Braid Protocol implementation in Rust.
//!
//! This crate consolidates several Braid-related components into a single library:
//!
//! - **core**: The core Braid-HTTP protocol implementation (types, parser, client, server).
//! - **antimatter**: Conflict resolution and state management.
//! - **blob**: Braid-Blob storage and synchronization service.
//! - **fs**: Filesystem synchronization client logic.

pub use smallvec;
pub mod core;
pub mod vendor;

// Antimatter module removed in favor of Diamond Types

#[cfg(feature = "blob")]
pub use braid_blob as blob;

#[cfg(feature = "fs")]
pub mod fs;

// Top-level re-exports for common usage
pub use crate::core::error::{BraidError, Result};
pub use crate::core::{BraidRequest, BraidResponse, Patch, Update, Version};
pub use braid_http::types;

#[cfg(feature = "client")]
pub use crate::core::{BraidClient, ClientConfig, Subscription};
#[cfg(feature = "client")]
pub use braid_http::client;

#[cfg(feature = "server")]
pub use crate::core::server;
#[cfg(feature = "server")]
pub use crate::core::server::{BraidLayer, BraidState, ConflictResolver, ServerConfig};

#[cfg(not(target_arch = "wasm32"))]
pub use crate::core::merge;

#[cfg(feature = "blob")]
pub use braid_blob::BlobStore;

