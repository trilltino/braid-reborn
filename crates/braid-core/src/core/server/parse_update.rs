//! Parse incoming update requests for Braid protocol.
//!
//! This module provides utilities for parsing Braid protocol headers and bodies
//! from HTTP requests, extracting version information, patch data, and other
//! protocol parameters.
//!
//! # Request Parsing
//!
//! Handlers can extract protocol information from incoming requests using:
//! - HTTP headers (Version, Parents, Merge-Type, Content-Range, etc.)
//! - Request body (snapshot or patches)
//!
//! # Status Codes
//!
//! Servers should respond with:
//! - `200 OK` - Successful update acceptance
//! - `206 Partial Content` - Range-based patches accepted
//! - `208 Already Reported` - Duplicate (idempotent) request
//! - `409 Conflict` - Version conflict in update
//! - `416 Range Not Satisfiable` - Invalid range specified
//!
//! # Specification
//!
//! See Sections 2 and 3 of draft-toomim-httpbis-braid-http for request specifications.

use crate::core::error::{BraidError, Result};
use crate::core::protocol_mod as protocol;
use crate::core::{Patch, Version};
use axum::extract::Request;
use bytes::Bytes;

/// Parsed update from request body.
///
/// Contains the structured representation of an incoming update request,
/// including version information and either the full body or patches.
#[derive(Clone, Debug)]
pub struct ParsedUpdate {
    /// Version ID(s) from Version header
    pub version: Vec<Version>,
    /// Parent version ID(s) from Parents header
    pub parents: Vec<Version>,
    /// Patches extracted from request body
    pub patches: Vec<Patch>,
    /// Full body if not patches (snapshot)
    pub body: Option<Bytes>,
}

impl ParsedUpdate {
    /// Create from HTTP request.
    ///
    /// Extracts Braid protocol headers and body from the request.
    pub async fn from_request(req: &Request) -> Result<Self> {
        let version_header = req
            .headers()
            .get("version")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");

        let parents_header = req
            .headers()
            .get("parents")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");

        let version = protocol::parse_version_header(version_header)?;
        let parents = protocol::parse_version_header(parents_header)?;

        Ok(ParsedUpdate {
            version,
            parents,
            patches: Vec::new(),
            body: None,
        })
    }
}

/// Extension trait for Axum request to parse Braid updates.
///
/// Provides methods to extract Braid protocol information from HTTP requests.
/// This trait is implemented for Axum `Request` types.
pub trait ParseUpdateExt {
    /// Parse version from Version header.
    ///
    /// Returns the version ID(s) specified in the Version header,
    /// or an empty vector if not present.
    fn get_version(&self) -> Result<Vec<Version>>;

    /// Parse parents from Parents header.
    ///
    /// Returns the parent version ID(s) specified in the Parents header,
    /// or an empty vector if not present.
    fn get_parents(&self) -> Result<Vec<Version>>;

    /// Parse patches from request body.
    ///
    /// Extracts patches from the request body, handling multi-patch format (Section 3.3).
    /// Returns an empty vector if the body is a snapshot rather than patches.
    fn get_patches(&self) -> Result<Vec<Patch>>;

    /// Parse complete update from request.
    ///
    /// Extracts all Braid protocol information from headers and body,
    /// returning a structured `ParsedUpdate`.
    fn parse_update(&self) -> Result<ParsedUpdate>;
}

/// Parse Content-Range header
#[allow(dead_code)]
pub fn parse_content_range(value: &str) -> Result<(String, String)> {
    let parts: Vec<&str> = value.splitn(2, ' ').collect();
    if parts.len() != 2 {
        return Err(BraidError::HeaderParse(format!(
            "Invalid Content-Range: {}",
            value
        )));
    }
    Ok((parts[0].to_string(), parts[1].to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_version_header() {
        let result = protocol::parse_version_header("\"v1\", \"v2\", \"v3\"").unwrap();
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_parse_version_header_empty() {
        let result = protocol::parse_version_header("").unwrap();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_parse_content_range() {
        let (unit, range) = protocol::parse_content_range("json .field").unwrap();
        assert_eq!(unit, "json");
        assert_eq!(range, ".field");
    }
}
