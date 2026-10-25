#![cfg_attr(
    not(test),
    deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)
)]
#![deny(unsafe_code)]
#![cfg_attr(test, deny(warnings))]
//! braid-core: Unified Braid Protocol implementation in Rust.
//!
//! This crate consolidates several Braid-related components into a single library:
//!
//! - **core**: The core Braid-HTTP protocol implementation (types, parser, client, server).
//! - **replica** and **simpleton**: Causal storage and coordinated text clients.
//! - **blob**: Braid-Blob storage and synchronization service.
//! - **fs**: Filesystem synchronization client logic.

pub use smallvec;
pub mod core;
#[cfg(feature = "native")]
pub mod replica;
#[cfg(feature = "native")]
pub mod simpleton;
// Copied algorithms have their own upstream assertions, lints and formatting.
#[cfg(all(feature = "serde", feature = "smallvec"))]
#[allow(warnings, clippy::all, unsafe_code, clippy::unwrap_used, clippy::expect_used, clippy::panic)]
#[rustfmt::skip]
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

#[cfg(all(feature = "serde", feature = "smallvec", not(target_arch = "wasm32")))]
pub use crate::core::merge;

#[cfg(feature = "blob")]
pub use braid_blob::BlobStore;
