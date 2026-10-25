#![cfg_attr(test, deny(warnings))]
#![forbid(unsafe_code)]
#![cfg_attr(
    not(test),
    deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)
)]
pub mod client;
pub mod error;
pub mod traits;
pub mod types;

pub use client::BraidClient;
pub use types::{BraidRequest, BraidResponse};
// Version might be in a submodule of types or directly in types/mod.rs
pub use error::{BraidError, Result};
#[cfg(not(target_arch = "wasm32"))]
pub mod durable;
pub mod protocol;
