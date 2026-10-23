//! Core Service Layer
//!
//! Provides shared infrastructure for the Braid server, including
//! authentication, data models, configuration, and storage.

pub mod auth;
pub mod blobs;
pub mod config;
pub mod ctx;
pub mod daemon;
pub mod error;
pub mod models;
pub mod pages;
pub mod protocol;
pub mod router;
pub mod store;

// Re-exports for convenience
pub use config::{AppState, ChatServerConfig};
pub use ctx::Ctx;
pub use error::{Error, Result};
pub use router::router;
