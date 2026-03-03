//! Braid-specific request parameters.

use crate::types::{Patch, Version};

/// Braid-specific request parameters.
#[derive(Clone, Debug, Default)]
pub struct BraidRequest {
    pub version: Option<Vec<Version>>,
    pub parents: Option<Vec<Version>>,
    pub subscribe: bool,
    pub patches: Option<Vec<Patch>>,
    pub heartbeat_interval: Option<u64>,
    pub peer: Option<String>,
    pub ack: Option<Vec<Version>>,
    pub enable_multiplex: bool,
    pub merge_type: Option<String>,
    pub content_type: Option<String>,
    pub method: String,
    pub body: bytes::Bytes,
    pub extra_headers: std::collections::BTreeMap<String, String>,
    pub retry: Option<crate::client::retry::RetryConfig>,
}

impl BraidRequest {
    #[inline]
    pub fn new() -> Self {
        Self {
            method: "GET".to_string(),
            body: bytes::Bytes::new(),
            extra_headers: std::collections::BTreeMap::new(),
            ..Default::default()
        }
    }

    pub fn subscribe(mut self) -> Self {
        self.subscribe = true;
        self
    }

    #[inline]
    pub fn is_subscription(&self) -> bool {
        self.subscribe
    }

    pub fn with_version(mut self, version: Version) -> Self {
        self.version.get_or_insert_with(Vec::new).push(version);
        self
    }

    pub fn with_versions(mut self, versions: Vec<Version>) -> Self {
        self.version = Some(versions);
        self
    }

    pub fn with_parent(mut self, version: Version) -> Self {
        self.parents.get_or_insert_with(Vec::new).push(version);
        self
    }

    pub fn with_peer(mut self, peer: impl Into<String>) -> Self {
        self.peer = Some(peer.into());
        self
    }

    pub fn with_ack(mut self, version: Version) -> Self {
        self.ack.get_or_insert_with(Vec::new).push(version);
        self
    }

    pub fn with_parents(mut self, parents: Vec<Version>) -> Self {
        self.parents = Some(parents);
        self
    }

    pub fn with_patches(mut self, patches: Vec<Patch>) -> Self {
        self.patches = Some(patches);
        self
    }

    #[inline]
    pub fn has_patches(&self) -> bool {
        self.patches.as_ref().is_some_and(|p| !p.is_empty())
    }

    pub fn with_heartbeat(mut self, seconds: u64) -> Self {
        self.heartbeat_interval = Some(seconds);
        self
    }

    pub fn with_multiplex(mut self, enable: bool) -> Self {
        self.enable_multiplex = enable;
        self
    }

    pub fn with_merge_type(mut self, merge_type: impl Into<String>) -> Self {
        self.merge_type = Some(merge_type.into());
        self
    }

    pub fn with_content_type(mut self, content_type: impl Into<String>) -> Self {
        self.content_type = Some(content_type.into());
        self
    }

    pub fn with_method(mut self, method: impl Into<String>) -> Self {
        self.method = method.into();
        self
    }

    pub fn with_body(mut self, body: impl Into<bytes::Bytes>) -> Self {
        self.body = body.into();
        self
    }

    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.extra_headers.insert(key.into(), value.into());
        self
    }

    pub fn with_retry(mut self, config: crate::client::retry::RetryConfig) -> Self {
        self.retry = Some(config);
        self
    }

    pub fn retry(self) -> Self {
        self.with_retry(crate::client::retry::RetryConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_braid_request_builder() {
        let req = BraidRequest::new()
            .subscribe()
            .with_version(Version::new("v1"))
            .with_heartbeat(5);

        assert!(req.subscribe);
        assert_eq!(req.version.unwrap().len(), 1);
        assert_eq!(req.heartbeat_interval, Some(5));
    }
}
