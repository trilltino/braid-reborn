//! Complete update in the Braid protocol.

use crate::types::{ContentRange, Patch, Version};
use bytes::Bytes;
use std::collections::BTreeMap;

/// A complete update in the Braid protocol.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Update {
    /// Version ID(s)
    pub version: Vec<Version>,
    /// Parent version(s)
    pub parents: Vec<Version>,
    /// Catch-up signaling
    pub current_version: Option<Vec<Version>>,
    /// Conflict resolution strategy
    pub merge_type: Option<String>,
    /// Incremental updates
    pub patches: Option<Vec<Patch>>,
    /// Complete state content
    pub body: Option<Bytes>,
    /// Content range for single patch
    pub content_range: Option<ContentRange>,
    /// Media type
    pub content_type: Option<String>,
    /// HTTP status code
    pub status: u16,
    /// Additional headers
    pub extra_headers: BTreeMap<String, String>,
    /// Target URL
    pub url: Option<String>,
}

#[cfg(feature = "fuzzing")]
impl<'a> arbitrary::Arbitrary<'a> for Update {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(Update {
            version: u.arbitrary()?,
            parents: u.arbitrary()?,
            current_version: u.arbitrary()?,
            merge_type: u.arbitrary()?,
            patches: u.arbitrary()?,
            body: {
                let v: Option<Vec<u8>> = u.arbitrary()?;
                v.map(bytes::Bytes::from)
            },
            content_range: u.arbitrary()?,
            content_type: u.arbitrary()?,
            status: u.arbitrary()?,
            extra_headers: u.arbitrary()?,
            url: u.arbitrary()?,
        })
    }
}

impl Update {
    /// Create a snapshot update with complete state.
    #[must_use]
    pub fn snapshot(version: Version, body: impl Into<Bytes>) -> Self {
        Update {
            version: vec![version],
            parents: vec![],
            current_version: None,
            merge_type: None,
            patches: None,
            body: Some(body.into()),
            content_range: None,
            content_type: None,
            status: 200,
            extra_headers: BTreeMap::new(),
            url: None,
        }
    }

    /// Create a patch update with incremental changes.
    #[must_use]
    pub fn patched(version: Version, patches: Vec<Patch>) -> Self {
        Update {
            version: vec![version],
            parents: vec![],
            current_version: None,
            merge_type: None,
            patches: Some(patches),
            body: None,
            content_range: None,
            content_type: None,
            status: 200,
            url: None,
            extra_headers: BTreeMap::new(),
        }
    }

    #[inline]
    #[must_use]
    pub fn is_snapshot(&self) -> bool {
        self.body.is_some()
    }

    #[inline]
    #[must_use]
    pub fn is_patched(&self) -> bool {
        self.patches.is_some()
    }

    #[inline]
    #[must_use]
    pub fn primary_version(&self) -> Option<&Version> {
        self.version.first()
    }

    #[inline]
    #[must_use]
    pub fn body_str(&self) -> Option<&str> {
        self.body.as_ref().and_then(|b| std::str::from_utf8(b).ok())
    }

    #[must_use]
    pub fn subscription_snapshot(version: Version, body: impl Into<Bytes>) -> Self {
        Update::snapshot(version, body).with_status(209)
    }

    #[must_use]
    pub fn subscription_patched(version: Version, patches: Vec<Patch>) -> Self {
        Update::patched(version, patches).with_status(209)
    }

    #[must_use]
    pub fn with_parent(mut self, parent: Version) -> Self {
        self.parents.push(parent);
        self
    }

    #[must_use]
    pub fn with_parents(mut self, parents: Vec<Version>) -> Self {
        self.parents.extend(parents);
        self
    }

    #[must_use]
    pub fn with_current_version(mut self, version: Version) -> Self {
        if self.current_version.is_none() {
            self.current_version = Some(Vec::new());
        }
        if let Some(ref mut versions) = self.current_version {
            versions.push(version);
        }
        self
    }

    #[must_use]
    pub fn with_merge_type(mut self, merge_type: impl Into<String>) -> Self {
        self.merge_type = Some(merge_type.into());
        self
    }

    #[must_use]
    pub fn with_content_range(mut self, content_range: ContentRange) -> Self {
        self.content_range = Some(content_range);
        self
    }

    #[must_use]
    pub fn with_content_type(mut self, content_type: impl Into<String>) -> Self {
        self.content_type = Some(content_type.into());
        self
    }

    #[must_use]
    pub fn with_status(mut self, status: u16) -> Self {
        self.status = status;
        self
    }

    #[must_use]
    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.extra_headers.insert(name.into(), value.into());
        self
    }

    #[must_use]
    pub fn to_json(&self) -> serde_json::Value {
        let mut obj = serde_json::Map::new();

        obj.insert(
            "version".to_string(),
            serde_json::Value::Array(self.version.iter().map(|v| v.to_json()).collect()),
        );

        obj.insert(
            "parents".to_string(),
            serde_json::Value::Array(self.parents.iter().map(|v| v.to_json()).collect()),
        );

        if let Some(body) = &self.body {
            obj.insert(
                "body".to_string(),
                serde_json::Value::String(String::from_utf8_lossy(body).into_owned()),
            );
        }

        if let Some(merge_type) = &self.merge_type {
            obj.insert(
                "merge_type".to_string(),
                serde_json::Value::String(merge_type.clone()),
            );
        }

        serde_json::Value::Object(obj)
    }
}

impl Default for Update {
    fn default() -> Self {
        Update {
            version: vec![],
            parents: vec![],
            current_version: None,
            merge_type: None,
            patches: None,
            body: None,
            content_range: None,
            content_type: None,
            status: 200,
            extra_headers: BTreeMap::new(),
            url: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_update_snapshot() {
        let update = Update::snapshot(Version::new("v1"), "body");
        assert_eq!(update.version.len(), 1);
        assert!(update.body.is_some());
        assert!(update.patches.is_none());
        assert!(update.is_snapshot());
        assert!(!update.is_patched());
    }

    #[test]
    fn test_update_patched() {
        let update = Update::patched(Version::new("v1"), vec![Patch::json(".field", "value")]);
        assert_eq!(update.version.len(), 1);
        assert!(update.patches.is_some());
        assert!(update.body.is_none());
        assert!(update.is_patched());
        assert!(!update.is_snapshot());
    }

    #[test]
    fn test_update_builder() {
        let update = Update::snapshot(Version::new("v1"), "body")
            .with_parent(Version::new("v0"))
            .with_merge_type("diamond");
        assert_eq!(update.parents.len(), 1);
        assert_eq!(update.merge_type, Some("diamond".to_string()));
    }

    #[test]
    fn test_primary_version() {
        let update = Update::snapshot(Version::new("v1"), "body");
        assert_eq!(update.primary_version(), Some(&Version::new("v1")));
    }

    #[test]
    fn test_body_str() {
        let update = Update::snapshot(Version::new("v1"), "hello");
        assert_eq!(update.body_str(), Some("hello"));
    }

    #[test]
    fn test_with_parents() {
        let update = Update::snapshot(Version::new("v3"), "merged")
            .with_parents(vec![Version::new("v1"), Version::new("v2")]);
        assert_eq!(update.parents.len(), 2);
    }

    #[test]
    fn test_with_header() {
        let update = Update::snapshot(Version::new("v1"), "data").with_header("X-Custom", "value");
        assert_eq!(
            update.extra_headers.get("X-Custom"),
            Some(&"value".to_string())
        );
    }

    #[test]
    fn test_to_json() {
        let update = Update::snapshot(Version::new("v1"), "data")
            .with_parent(Version::new("v0"))
            .with_merge_type("diamond");
        let json = update.to_json();
        assert!(json.get("version").is_some());
        assert!(json.get("parents").is_some());
        assert!(json.get("body").is_some());
        assert!(json.get("merge_type").is_some());
    }

    #[test]
    fn test_default() {
        let update = Update::default();
        assert!(update.version.is_empty());
        assert!(update.parents.is_empty());
        assert_eq!(update.status, 200);
    }

    #[test]
    fn test_subscription_snapshot() {
        let update = Update::subscription_snapshot(Version::new("v1"), "data");
        assert_eq!(update.status, 209);
        assert!(update.is_snapshot());
        assert!(!update.is_patched());
    }

    #[test]
    fn test_subscription_patched() {
        let update =
            Update::subscription_patched(Version::new("v2"), vec![Patch::json(".field", "value")]);
        assert_eq!(update.status, 209);
        assert!(update.is_patched());
        assert!(!update.is_snapshot());
    }

    #[test]
    fn test_subscription_with_parents() {
        let update = Update::subscription_snapshot(Version::new("v2"), "data")
            .with_parent(Version::new("v1"));
        assert_eq!(update.status, 209);
        assert_eq!(update.parents.len(), 1);
    }

    #[test]
    fn test_zero_length_body() {
        // Zero-length bodies are valid per spec Section 4
        let update = Update::snapshot(Version::new("v1"), "");
        assert!(update.body.is_some());
        assert_eq!(update.body.as_ref().unwrap().len(), 0);
    }

    #[test]
    fn test_zero_length_subscription() {
        // Zero-length subscription updates are valid (e.g., deletion marker)
        let update = Update::subscription_snapshot(Version::new("v1"), Bytes::new());
        assert_eq!(update.status, 209);
        assert!(update.body.is_some());
        assert_eq!(update.body.as_ref().unwrap().len(), 0);
    }
}
