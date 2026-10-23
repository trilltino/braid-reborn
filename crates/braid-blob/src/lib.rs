#![cfg_attr(
    not(test),
    deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)
)]
#![forbid(unsafe_code)]
#[cfg(feature = "native")]
mod objects;
pub mod store;
pub use store::{atomic_write, encode_filename, BlobMetadata};
#[cfg(feature = "native")]
pub use store::{BlobStore, BlobUpload};
