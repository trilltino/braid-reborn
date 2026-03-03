//! Send update response implementation for Braid protocol.
//!
//! This module provides utilities for sending Braid protocol updates to clients
//! over HTTP responses, handling version headers, merge types, patches, and
//! subscription status codes.
//!
//! # Response Format
//!
//! Braid updates are sent as HTTP responses with specific headers:
//! - **Version**: The version ID(s) of the update
//! - **Parents**: The parent version ID(s) in the DAG
//! - **Current-Version**: The latest version (for catch-up signaling)
//! - **Merge-Type**: The conflict resolution strategy
//! - **Content-Range**: Range specification for patches
//! - **Content-Length**: Length of response body
//!
//! # Status Codes
//!
//! - `200 OK` - Standard update response
//! - `206 Partial Content` - Range-based patches
//! - `209 Subscription` - Subscription update (Section 4)
//! - `293 Merge Conflict` - Conflict detected
//! - `410 Gone` - History dropped
//! - `416 Range Not Satisfiable` - Invalid range
//!
//! # Specification
//!
//! See Sections 2, 3, and 4 of draft-toomim-httpbis-braid-http.

use crate::core::error::Result;
use crate::core::protocol_mod as protocol;
use crate::core::protocol_mod::constants::{headers, media_types};
use crate::core::{Update, Version};
use axum::{
    body::Body,
    http::{header, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
};
use bytes::Bytes;

use std::collections::BTreeMap;

/// Extension trait for Axum responses to send Braid updates.
///
/// Provides methods to encode and send Braid protocol updates as HTTP responses.
/// This trait is implemented for types that can produce HTTP responses.
pub trait SendUpdateExt {
    /// Send a Braid update to the client.
    ///
    /// Encodes the update with appropriate headers and status code,
    /// returning it as an HTTP response.
    fn send_update(&mut self, update: &Update) -> Result<()>;

    /// Send raw bytes as response body.
    ///
    /// Sends raw bytes directly in the response body.
    fn send_body(&mut self, body: &[u8]) -> Result<()>;
}

/// Builder for creating update responses.
///
/// Provides a fluent API for constructing Braid protocol responses with
/// appropriate headers and status codes.
///
/// # Examples
///
/// ```ignore
/// use crate::core::server::UpdateResponse;
/// use crate::core::Version;
///
/// let response = UpdateResponse::new(200)
///     .with_version(vec![Version::new("v2")])
///     .with_parents(vec![Version::new("v1")])
///     .with_header("Merge-Type".to_string(), "diamond".to_string())
///     .with_body("{\"data\": \"updated\"}")
///     .build();
/// ```
pub struct UpdateResponse {
    /// HTTP status code
    status: u16,
    /// Response headers
    headers: BTreeMap<String, String>,
    /// Response body
    body: Option<Bytes>,
}

impl UpdateResponse {
    /// Create a new update response builder with the given status code.
    ///
    /// Use HTTP 209 for subscription updates (Section 4).
    pub fn new(status: u16) -> Self {
        UpdateResponse {
            status,
            headers: BTreeMap::new(),
            body: None,
        }
    }

    /// Set version header(s).
    ///
    /// Specifies the version ID(s) of this update (Section 2).
    pub fn with_version(mut self, versions: Vec<Version>) -> Self {
        let version_str = protocol::format_version_header(&versions);
        self.headers
            .insert(headers::VERSION.as_str().to_string(), version_str);
        self
    }

    /// Set parents header
    pub fn with_parents(mut self, parents: Vec<Version>) -> Self {
        let parents_str = protocol::format_version_header(&parents);
        self.headers
            .insert(headers::PARENTS.as_str().to_string(), parents_str);
        self
    }

    /// Set current-version header
    pub fn with_current_version(mut self, versions: Vec<Version>) -> Self {
        let current_version_str = protocol::format_version_header(&versions);
        self.headers.insert(
            headers::CURRENT_VERSION.as_str().to_string(),
            current_version_str,
        );
        self
    }

    /// Set body
    pub fn with_body(mut self, body: impl Into<Bytes>) -> Self {
        self.body = Some(body.into());
        self
    }

    /// Set custom header
    pub fn with_header(mut self, key: String, value: String) -> Self {
        self.headers.insert(key, value);
        self
    }

    /// Build the response
    pub fn build(self) -> Response {
        let mut response = match self.status {
            200 => Response::builder().status(StatusCode::OK),
            209 => Response::builder().status(StatusCode::from_u16(209).unwrap()),
            404 => Response::builder().status(StatusCode::NOT_FOUND),
            500 => Response::builder().status(StatusCode::INTERNAL_SERVER_ERROR),
            _ => Response::builder().status(StatusCode::from_u16(self.status).unwrap()),
        };

        for (key, value) in &self.headers {
            if let Ok(header_value) = value.parse::<HeaderValue>() {
                response = response.header(key, header_value);
            }
        }

        if let Some(body) = self.body {
            response
                .header(header::CONTENT_LENGTH, body.len())
                .body(Body::from(body))
                .unwrap_or_else(|_| Response::default())
        } else {
            response
                .body(Body::empty())
                .unwrap_or_else(|_| Response::default())
        }
    }
}

/// Wrapper around Update for IntoResponse implementation
pub struct BraidUpdate(pub Update);

/// Convert Update to written HTTP response
impl IntoResponse for BraidUpdate {
    fn into_response(self) -> Response {
        let update = self.0;
        let mut response_builder = UpdateResponse::new(update.status);

        if !update.version.is_empty() {
            response_builder = response_builder.with_version(update.version);
        }

        if !update.parents.is_empty() {
            response_builder = response_builder.with_parents(update.parents);
        }

        if let Some(current_version) = update.current_version {
            response_builder = response_builder.with_current_version(current_version);
        }

        if let Some(content_type) = update.content_type {
            response_builder = response_builder
                .with_header(header::CONTENT_TYPE.as_str().to_string(), content_type);
        } else if update.patches.is_some() {
            // GAP-S03: Patch updates MUST use application/braid-patch Content-Type
            response_builder = response_builder.with_header(
                header::CONTENT_TYPE.as_str().to_string(),
                media_types::BRAID_PATCH.to_string(),
            );
        }

        for (key, value) in update.extra_headers {
            response_builder = response_builder.with_header(key, value);
        }

        if let Some(body) = update.body {
            response_builder = response_builder.with_body(body);
        } else if let Some(patches) = update.patches {
            let patches_str = patches.len().to_string();
            response_builder =
                response_builder.with_header(headers::PATCHES.as_str().to_string(), patches_str);

            if patches.len() == 1 {
                let patch = &patches[0];
                let content_range = format!("{} {}", patch.unit, patch.range);
                response_builder = response_builder
                    .with_header(headers::CONTENT_RANGE.as_str().to_string(), content_range);
                response_builder = response_builder.with_body(patch.content.clone());
            } else if patches.len() > 1 {
                // Multi-patch serialization (Section 3.3)
                let mut multi_body = bytes::BytesMut::new();
                for patch in patches {
                    use bytes::BufMut;
                    let patch_headers = format!(
                        "Content-Length: {}\r\nContent-Range: {} {}\r\n\r\n",
                        patch.len(),
                        patch.unit,
                        patch.range
                    );
                    multi_body.put_slice(patch_headers.as_bytes());
                    multi_body.put_slice(&patch.content);
                    multi_body.put_slice(b"\r\n");
                }
                response_builder = response_builder.with_body(multi_body.freeze());
            }
        }

        response_builder.build()
    }
}

/// HTTP response status codes
pub mod status {
    use axum::http::StatusCode;

    /// 209 Subscription
    #[allow(dead_code)]
    pub const SUBSCRIPTION: u16 = 209;

    /// 293 Responded via Multiplexer
    #[allow(dead_code)]
    pub const RESPONDED_VIA_MULTIPLEX: u16 = 293;

    #[allow(dead_code)]
    pub fn subscription_response() -> StatusCode {
        StatusCode::from_u16(SUBSCRIPTION).unwrap()
    }

    #[allow(dead_code)]
    pub fn multiplex_response() -> StatusCode {
        StatusCode::from_u16(RESPONDED_VIA_MULTIPLEX).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_update_response_builder() {
        let response = UpdateResponse::new(200)
            .with_version(vec![Version::from("v1")])
            .with_header("Custom".to_string(), "value".to_string())
            .build();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[test]
    fn test_version_in_response() {
        // GAP-S16: Ensure Version header is included in PUT responses
        let response = UpdateResponse::new(200)
            .with_version(vec![Version::from("v42")])
            .build();

        let version_header = response
            .headers()
            .get("version")
            .and_then(|v| v.to_str().ok());

        assert!(version_header.is_some());
        assert!(version_header.unwrap().contains("v42"));
    }

    #[test]
    fn test_version_with_parents_in_response() {
        // GAP-S16: Ensure both Version and Parents headers included
        let response = UpdateResponse::new(200)
            .with_version(vec![Version::from("v2")])
            .with_parents(vec![Version::from("v1")])
            .build();

        assert!(response.headers().contains_key("version"));
        assert!(response.headers().contains_key("parents"));
    }

    #[test]
    fn test_patch_content_type() {
        // GAP-S03: Patch updates MUST use application/braid-patch Content-Type
        use crate::core::Patch;
        let update = Update::patched(Version::from("v1"), vec![Patch::json(".a", "1")]);
        let response: Response = BraidUpdate(update).into_response();

        let ct = response
            .headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap();
        assert_eq!(ct, "application/braid-patch");
    }

    #[test]
    fn test_subscription_status() {
        // GAP-S02: Subscription updates MUST use status 209
        let update = Update::subscription_snapshot(Version::from("v1"), "data");
        let response: Response = BraidUpdate(update).into_response();

        assert_eq!(response.status().as_u16(), 209);
    }
}
