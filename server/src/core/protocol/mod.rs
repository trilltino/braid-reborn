//! Braid Protocol Implementation
//! 
//! This module re-exports Braid protocol types from the braid-http crate
//! and provides chat-specific protocol extensions.

pub use braid_http::protocol::{
    constants::{headers},
    parse_version_header,
    format_version_header,
};
pub use braid_http::types::{Version};

use axum::http::HeaderMap;

/// Extension trait for HeaderMap to extract Braid headers
pub trait BraidHeaderExt {
    fn get_braid_version(&self) -> Option<Vec<Version>>;
    fn get_braid_parents(&self) -> Option<Vec<Version>>;
    fn get_braid_subscribe(&self) -> bool;
}

impl BraidHeaderExt for HeaderMap {
    fn get_braid_version(&self) -> Option<Vec<Version>> {
        self.get(&headers::VERSION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| parse_version_header(v).ok())
    }

    fn get_braid_parents(&self) -> Option<Vec<Version>> {
        self.get(&headers::PARENTS)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| parse_version_header(v).ok())
    }

    fn get_braid_subscribe(&self) -> bool {
        self.get(&headers::SUBSCRIBE).is_some()
    }
}

/// Format version for response header (quoted string)
pub fn format_version(version: u64) -> String {
    format!("\"{}\"", version)
}

/// Format parents for response header
pub fn format_parents(parents: &[String]) -> String {
    parents
        .iter()
        .map(|p| format!("\"{}\"", p))
        .collect::<Vec<_>>()
        .join(", ")
}
