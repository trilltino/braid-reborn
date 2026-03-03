//! Braid-specific HTTP header handling.

use crate::error::{BraidError, Result};
use crate::protocol;
use crate::types::Version;
use http::header::{HeaderMap, HeaderValue};

/// Braid-specific HTTP headers for requests and responses.
#[derive(Clone, Debug, Default)]
pub struct BraidHeaders {
    /// Version identifier(s) from `Version` header
    pub version: Option<Vec<Version>>,
    /// Parent version(s) from `Parents` header
    pub parents: Option<Vec<Version>>,
    /// Current version(s) from `Current-Version` header
    pub current_version: Option<Vec<Version>>,
    /// Subscribe header indicating subscription mode
    pub subscribe: bool,
    /// Peer identifier from `Peer` header
    pub peer: Option<String>,
    /// Heartbeat interval from `Heartbeats` header
    pub heartbeat: Option<String>,
    /// Merge type from `Merge-Type` header
    pub merge_type: Option<String>,
    /// Number of patches from `Patches` header
    pub patches_count: Option<usize>,
    /// Content range from `Content-Range` header
    pub content_range: Option<String>,
    /// Retry-After header for backoff guidance
    pub retry_after: Option<String>,
    /// Additional non-Braid headers
    pub extra: std::collections::BTreeMap<String, String>,
}

impl BraidHeaders {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_version(mut self, version: Version) -> Self {
        let mut versions = self.version.unwrap_or_default();
        versions.push(version);
        self.version = Some(versions);
        self
    }

    pub fn with_versions(mut self, versions: Vec<Version>) -> Self {
        self.version = Some(versions);
        self
    }

    pub fn with_parent(mut self, parent: Version) -> Self {
        let mut parents = self.parents.unwrap_or_default();
        parents.push(parent);
        self.parents = Some(parents);
        self
    }

    pub fn with_parents(mut self, parents: Vec<Version>) -> Self {
        self.parents = Some(parents);
        self
    }

    pub fn with_current_version(mut self, version: Version) -> Self {
        let mut versions = self.current_version.unwrap_or_default();
        versions.push(version);
        self.current_version = Some(versions);
        self
    }

    pub fn with_current_versions(mut self, versions: Vec<Version>) -> Self {
        self.current_version = Some(versions);
        self
    }

    pub fn with_subscribe(mut self) -> Self {
        self.subscribe = true;
        self
    }

    pub fn with_merge_type(mut self, merge_type: impl Into<String>) -> Self {
        self.merge_type = Some(merge_type.into());
        self
    }

    pub fn with_content_range(mut self, content_range: impl Into<String>) -> Self {
        self.content_range = Some(content_range.into());
        self
    }

    pub fn with_heartbeat(mut self, interval: String) -> Self {
        self.heartbeat = Some(interval);
        self
    }

    pub fn with_peer(mut self, peer: String) -> Self {
        self.peer = Some(peer);
        self
    }

    /// Convert to HTTP HeaderMap.
    pub fn to_header_map(&self) -> Result<HeaderMap> {
        let mut headers = HeaderMap::new();

        if let Some(ref versions) = self.version {
            let version_str = protocol::format_version_header(versions);
            headers.insert(
                "Version",
                HeaderValue::from_str(&version_str)
                    .map_err(|e| BraidError::Config(e.to_string()))?,
            );
        }

        if let Some(ref parents) = self.parents {
            let parents_str = protocol::format_version_header(parents);
            headers.insert(
                "Parents",
                HeaderValue::from_str(&parents_str)
                    .map_err(|e| BraidError::Config(e.to_string()))?,
            );
        }

        if let Some(ref current_versions) = self.current_version {
            let current_version_str = protocol::format_version_header(current_versions);
            headers.insert(
                "Current-Version",
                HeaderValue::from_str(&current_version_str)
                    .map_err(|e| BraidError::Config(e.to_string()))?,
            );
        }

        if self.subscribe {
            headers.insert("Subscribe", HeaderValue::from_static("true"));
        }

        if let Some(ref peer) = self.peer {
            headers.insert(
                "Peer",
                HeaderValue::from_str(peer).map_err(|e| BraidError::Config(e.to_string()))?,
            );
        }

        if let Some(ref heartbeat) = self.heartbeat {
            headers.insert(
                "Heartbeats",
                HeaderValue::from_str(heartbeat).map_err(|e| BraidError::Config(e.to_string()))?,
            );
        }

        if let Some(ref merge_type) = self.merge_type {
            headers.insert(
                "Merge-Type",
                HeaderValue::from_str(merge_type).map_err(|e| BraidError::Config(e.to_string()))?,
            );
        }

        if let Some(count) = self.patches_count {
            headers.insert(
                "Patches",
                HeaderValue::from_str(&count.to_string())
                    .map_err(|e| BraidError::Config(e.to_string()))?,
            );
        }

        if let Some(ref content_range) = self.content_range {
            headers.insert(
                "Content-Range",
                HeaderValue::from_str(content_range)
                    .map_err(|e| BraidError::Config(e.to_string()))?,
            );
        }

        Ok(headers)
    }

    /// Parse from HTTP HeaderMap.
    pub fn from_header_map(headers: &HeaderMap) -> Result<Self> {
        let mut braid_headers = BraidHeaders::new();
        for (name, value) in headers.iter() {
            let name_lower = name.as_str().to_lowercase();
            let value_str = value
                .to_str()
                .map_err(|_| BraidError::HeaderParse("Invalid header value".to_string()))?;

            match name_lower.as_str() {
                "version" => {
                    braid_headers.version = Some(protocol::parse_version_header(value_str)?);
                }
                "parents" => {
                    braid_headers.parents = Some(protocol::parse_version_header(value_str)?);
                }
                "current-version" => {
                    braid_headers.current_version =
                        Some(protocol::parse_version_header(value_str)?);
                }
                "subscribe" => {
                    braid_headers.subscribe = value_str.to_lowercase() == "true";
                }
                "peer" => {
                    braid_headers.peer = Some(value_str.to_string());
                }
                "heartbeats" => {
                    braid_headers.heartbeat = Some(value_str.to_string());
                }
                "merge-type" => {
                    braid_headers.merge_type = Some(value_str.to_string());
                }
                "patches" => {
                    braid_headers.patches_count = value_str.parse().ok();
                }
                "content-range" => {
                    braid_headers.content_range = Some(value_str.to_string());
                }
                "retry-after" => {
                    braid_headers.retry_after = Some(value_str.to_string());
                }
                _ => {
                    braid_headers
                        .extra
                        .insert(name_lower, value_str.to_string());
                }
            }
        }

        Ok(braid_headers)
    }
}

/// Utility for parsing Braid protocol headers.
pub struct HeaderParser;

impl HeaderParser {
    pub fn parse_version(value: &str) -> Result<Vec<Version>> {
        protocol::parse_version_header(value)
    }

    pub fn parse_content_range(value: &str) -> Result<(String, String)> {
        protocol::parse_content_range(value)
    }

    pub fn format_version(versions: &[Version]) -> String {
        protocol::format_version_header(versions)
    }

    pub fn format_content_range(unit: &str, range: &str) -> String {
        protocol::format_content_range(unit, range)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_braid_headers_to_map() {
        let headers = BraidHeaders::new()
            .with_version(Version::String("v1".to_string()))
            .with_subscribe();

        let map = headers.to_header_map().unwrap();
        assert!(map.contains_key("Version"));
        assert!(map.contains_key("Subscribe"));
    }

    #[test]
    fn test_parse_version_header() {
        let result = protocol::parse_version_header("\"v1\", \"v2\", \"v3\"").unwrap();
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_parse_content_range() {
        let (unit, range) = protocol::parse_content_range("json .field").unwrap();
        assert_eq!(unit, "json");
        assert_eq!(range, ".field");
    }
}
