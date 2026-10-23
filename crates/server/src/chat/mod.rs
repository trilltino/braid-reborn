//! Chat Service Layer
//!
//! Implements messaging, presence, and collaborative features
//! using the Braid protocol and Diamond-type CRDTs.

pub mod ai;
pub mod crdt;
pub mod friends;
pub mod handlers;
pub mod mail;

pub use handlers::router;
