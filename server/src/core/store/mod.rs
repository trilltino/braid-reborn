//! Chat storage module
//!
//! Provides JSON-based storage with CRDT support for conflict-free
//! distributed chat synchronization.

pub mod json_store;

pub use json_store::{JsonChatStore, UpdateChannel, RoomUpdate, UpdateType, RoomData};
