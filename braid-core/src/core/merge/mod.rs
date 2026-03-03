//! Merge algorithms and CRDT implementations for conflict resolution.
//!
//! This module provides merge strategies for resolving concurrent edits to the same resource
//! when multiple clients make simultaneous mutations. The module includes built-in support
//! for CRDT-based merge algorithms.
//!
//! # Merge-Types in Braid-HTTP
//!
//! Per Section 2.2 of [draft-toomim-httpbis-braid-http-04], resources can declare a merge type
//! to specify how concurrent edits should be reconciled:
//!
//! | Merge Type | Description |
//! |------------|-------------|
//! //! | `"diamond"` | Diamond-types CRDT for text documents |
//! | `"antimatter"` | Antimatter CRDT with pruning |
//! | Custom | Application-defined merge algorithms |
//!
//! # Key Types
//!
//! | Type | Description |
//! |------|-------------|
//! | [`MergeType`] | Trait for pluggable merge algorithms |
//! //! | [`AntimatterMergeType`] | Antimatter CRDT with pruning |
//! | [`DiamondCRDT`] | High-performance text CRDT |
//! | [`MergeTypeRegistry`] | Factory for creating merge type instances |
//!
//! [draft-toomim-httpbis-braid-http-04]: https://datatracker.ietf.org/doc/html/draft-toomim-httpbis-braid-http

pub mod merge_type;
pub mod simpleton;



#[cfg(not(target_arch = "wasm32"))]
pub mod diamond;

// Re-exports
pub use merge_type::{MergePatch, MergeResult, MergeType, MergeTypeRegistry};
pub use simpleton::SimpletonMergeType;

#[cfg(not(target_arch = "wasm32"))]
pub use diamond::{DiamondCRDT, DiamondMergeType};


