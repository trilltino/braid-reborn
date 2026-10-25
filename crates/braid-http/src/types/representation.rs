//! A materialized representation at one complete causal frontier.
use super::Version;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Media type is declared by the resource, never inferred by inspecting bytes.
/// A graph query keeps one such frontier per resource rather than inventing a
/// single version for independently changing resources.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RepresentationAt {
    pub bytes: Bytes,
    pub media_type: String,
    pub frontier: Vec<Version>,
}
