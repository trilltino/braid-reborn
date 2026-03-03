//! Protocol constants for Braid-HTTP.
//!
//! This module defines standard header names, status codes, merge type identifiers,
//! and other protocol constants used throughout the Braid-HTTP implementation.
//!
//! # Organization
//!
//! ```text
//! constants/
//! ├── Top-level    - Common status codes (STATUS_SUBSCRIPTION, etc.)
//! ├── status       - All HTTP status codes used by Braid
//! ├── headers      - All Braid protocol header names (typed)
//! └── merge_types  - Merge type identifiers
//! ```
//!
//! # Status Codes
//!
//! | Code | Constant | Description |
//! |------|----------|-------------|
//! | 200 | `status::OK` | Standard response |
//! | 206 | `status::PARTIAL_CONTENT` | Range-based patches |
//! | 209 | `STATUS_SUBSCRIPTION` | Subscription update |
//! | 293 | `STATUS_MERGE_CONFLICT` | Version conflicts |
//! | 410 | `STATUS_GONE` | History dropped |
//! | 416 | `STATUS_RANGE_NOT_SATISFIABLE` | Invalid range |
//!
//! # Examples
//!
//! ```
//! use crate::protocol;
//! use crate::protocol::constants::{headers, merge_types};
//!
//! // Use top-level constants
//! let status = 209u16;
//! if status == protocol::STATUS_SUBSCRIPTION {
//!     println!("Subscription response");
//! }
//!
//! // Use module constants
//! if status == protocol::status::SUBSCRIPTION {
//!     println!("Same as above");
//! }
//!
//! // Use header name constants
//! let header_name = headers::VERSION;
//! assert_eq!(header_name, "Version");
//!
//! // Use merge type constants
//! let merge_type = merge_types::DIAMOND;
//! assert_eq!(merge_type, "diamond");
//! ```
//!
//! # Specification
//!
//! See [draft-toomim-httpbis-braid-http-04]:
//!
//! - **Section 2**: Versioning and merge types
//! - **Section 3**: Patches and Content-Range
//! - **Section 4**: Subscriptions and status codes
//!
//! [draft-toomim-httpbis-braid-http-04]: https://datatracker.ietf.org/doc/html/draft-toomim-httpbis-braid-http

// =============================================================================
// Top-Level Status Code Constants
// =============================================================================

/// HTTP status code for subscription updates (Section 4).
///
/// The server sends this status code (209) when responding to a subscription
/// request. The connection remains open and updates are streamed as they occur.
///
/// # Example
///
/// ```
/// use crate::protocol::STATUS_SUBSCRIPTION;
///
/// assert_eq!(STATUS_SUBSCRIPTION, 209);
/// ```
pub const STATUS_SUBSCRIPTION: u16 = 209;

/// HTTP status code for merge conflicts (Section 2.2).
///
/// The server sends this status code (293) when it detects conflicting versions
/// during a merge operation. The client should apply the merge strategy specified
/// by the Merge-Type header to resolve the conflict.
///
/// # Example
///
/// ```
/// use crate::protocol::STATUS_MERGE_CONFLICT;
///
/// assert_eq!(STATUS_MERGE_CONFLICT, 293);
/// ```
pub const STATUS_MERGE_CONFLICT: u16 = 293;

/// HTTP status code for history dropped (Section 4.5).
///
/// The server sends this status code (410 Gone) when it has discarded version
/// history before the requested version. The client must restart synchronization
/// from scratch by clearing its local state.
pub const STATUS_GONE: u16 = 410;

/// HTTP status code for range not satisfiable.
///
/// The server sends this status code (416) when the requested Content-Range
/// is invalid or cannot be satisfied (e.g., range exceeds resource size).
pub const STATUS_RANGE_NOT_SATISFIABLE: u16 = 416;

// =============================================================================
// Status Code Module
// =============================================================================

/// Standard HTTP status codes used by Braid-HTTP.
///
/// # Example
///
/// ```
/// use crate::protocol::status;
///
/// assert_eq!(status::OK, 200);
/// assert_eq!(status::SUBSCRIPTION, 209);
/// ```
pub mod status {
    /// 200 OK - Standard response
    pub const OK: u16 = 200;

    /// 206 Partial Content - Range-based patches (RFC 7233)
    pub const PARTIAL_CONTENT: u16 = 206;

    /// 209 Subscription - Subscription update (Braid-HTTP)
    pub const SUBSCRIPTION: u16 = 209;

    /// 293 Merge Conflict - Version conflicts detected (Braid-HTTP)
    pub const MERGE_CONFLICT: u16 = 293;

    /// 410 Gone - History dropped, client must restart
    pub const GONE: u16 = 410;

    /// 416 Range Not Satisfiable - Invalid range request (RFC 7233)
    pub const RANGE_NOT_SATISFIABLE: u16 = 416;
}

// =============================================================================
// Header Names Module
// =============================================================================

/// Braid protocol header names.
///
/// Use these constants when setting or reading Braid-specific headers.
/// These are typed `axum::http::HeaderName` constants for zero-copy usage.
///
/// # Example
///
/// ```
/// use crate::protocol::constants::headers;
///
/// assert_eq!(headers::VERSION, "version");
/// assert_eq!(headers::SUBSCRIBE, "subscribe");
/// ```
pub mod headers {
    use http::HeaderName;

    /// Version header - identifies the version of the resource.
    pub const VERSION: HeaderName = HeaderName::from_static("version");

    /// Parents header - identifies parent version(s) in the DAG.
    pub const PARENTS: HeaderName = HeaderName::from_static("parents");

    /// Current-Version header - latest version for catch-up signaling.
    pub const CURRENT_VERSION: HeaderName = HeaderName::from_static("current-version");

    /// Subscribe header - requests subscription mode.
    pub const SUBSCRIBE: HeaderName = HeaderName::from_static("subscribe");

    /// Heartbeats header - keep-alive interval for subscriptions.
    pub const HEARTBEATS: HeaderName = HeaderName::from_static("heartbeats");

    /// Peer header - identifies the client peer.
    pub const PEER: HeaderName = HeaderName::from_static("peer");

    /// Merge-Type header - conflict resolution strategy.
    pub const MERGE_TYPE: HeaderName = HeaderName::from_static("merge-type");

    /// Content-Range header - range specification for patches.
    pub const CONTENT_RANGE: HeaderName = http::header::CONTENT_RANGE;

    /// Patches header - number of patches in multi-patch format.
    pub const PATCHES: HeaderName = HeaderName::from_static("patches");

    /// Multiplex-Version header - version of the multiplexing protocol.
    pub const MULTIPLEX_VERSION: HeaderName = HeaderName::from_static("multiplex-version");

    /// Multiplex-Through header - path for multiplexing.
    pub const MULTIPLEX_THROUGH: HeaderName = HeaderName::from_static("multiplex-through");

    /// Retry-After header - suggested retry delay.
    pub const RETRY_AFTER: HeaderName = http::header::RETRY_AFTER;

    /// Content-Length header - body length.
    pub const CONTENT_LENGTH: HeaderName = http::header::CONTENT_LENGTH;

    /// Content-Type header - body media type.
    pub const CONTENT_TYPE: HeaderName = http::header::CONTENT_TYPE;
}

// =============================================================================
// Merge Types Module
// =============================================================================

/// Merge type identifiers for conflict resolution.
///
/// # Example
///
/// ```
/// use crate::protocol::merge_types;
///
/// assert_eq!(merge_types::DIAMOND, "diamond");
/// ```
pub mod merge_types {

    /// Diamond-types CRDT merge type for collaborative text editing.
    pub const DIAMOND: &str = "diamond";

    /// Simpleton merge type for basic file editing.
    pub const SIMPLETON: &str = "simpleton";
}

// =============================================================================
// Media Types Module
// =============================================================================

/// Braid protocol media types.
pub mod media_types {
    /// application/braid-patch media type for patch updates (Section 3).
    pub const BRAID_PATCH: &str = "application/braid-patch";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_status_codes() {
        assert_eq!(STATUS_SUBSCRIPTION, 209);
        assert_eq!(STATUS_MERGE_CONFLICT, 293);
        assert_eq!(STATUS_GONE, 410);
        assert_eq!(STATUS_RANGE_NOT_SATISFIABLE, 416);
    }

    #[test]
    fn test_status_module() {
        assert_eq!(status::OK, 200);
        assert_eq!(status::PARTIAL_CONTENT, 206);
        assert_eq!(status::SUBSCRIPTION, 209);
        assert_eq!(status::MERGE_CONFLICT, 293);
    }

    #[test]
    fn test_header_names() {
        assert_eq!(headers::VERSION.as_str(), "version");
        assert_eq!(headers::PARENTS.as_str(), "parents");
        assert_eq!(headers::SUBSCRIBE.as_str(), "subscribe");
        assert_eq!(headers::MERGE_TYPE.as_str(), "merge-type");
    }

    #[test]
    fn test_merge_types() {
        assert_eq!(merge_types::DIAMOND, "diamond");
    }
}
