//! HTTP response with Braid protocol information.

use crate::protocol;
use crate::types::{ContentRange, Version};
use bytes::Bytes;
use std::collections::BTreeMap;

/// HTTP response with Braid protocol information.
#[derive(Clone, Debug)]
pub struct BraidResponse {
    pub status: u16,
    pub headers: BTreeMap<String, String>,
    pub body: Bytes,
    pub is_subscription: bool,
}

impl BraidResponse {
    pub fn new(status: u16, body: impl Into<Bytes>) -> Self {
        BraidResponse {
            status,
            headers: BTreeMap::new(),
            body: body.into(),
            is_subscription: status == 209,
        }
    }

    pub fn subscription(body: impl Into<Bytes>) -> Self {
        Self::new(209, body)
    }

    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(name.into(), value.into());
        self
    }

    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    pub fn get_version(&self) -> Option<Vec<Version>> {
        self.header("version")
            .and_then(|v| protocol::parse_version_header(v).ok())
    }

    pub fn get_parents(&self) -> Option<Vec<Version>> {
        self.header("parents")
            .and_then(|v| protocol::parse_version_header(v).ok())
    }

    pub fn get_current_version(&self) -> Option<Vec<Version>> {
        self.header("current-version")
            .and_then(|v| protocol::parse_version_header(v).ok())
    }

    pub fn get_merge_type(&self) -> Option<String> {
        self.header("merge-type").map(|s| s.to_string())
    }

    pub fn get_content_range(&self) -> Option<ContentRange> {
        self.header("content-range")
            .and_then(|v| ContentRange::from_header_value(v).ok())
    }

    pub fn body_str(&self) -> Option<&str> {
        std::str::from_utf8(&self.body).ok()
    }

    #[inline]
    pub fn is_success(&self) -> bool {
        (200..300).contains(&self.status)
    }
    #[inline]
    pub fn is_partial(&self) -> bool {
        self.status == 206
    }
}

impl Default for BraidResponse {
    fn default() -> Self {
        BraidResponse {
            status: 200,
            headers: BTreeMap::new(),
            body: Bytes::new(),
            is_subscription: false,
        }
    }
}

#[cfg(feature = "fuzzing")]
impl<'a> arbitrary::Arbitrary<'a> for BraidResponse {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let status: u16 = u.arbitrary()?;
        Ok(BraidResponse {
            status,
            headers: u.arbitrary()?,
            body: bytes::Bytes::from(u.arbitrary::<Vec<u8>>()?),
            is_subscription: status == 209,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_braid_response_basic() {
        let res = BraidResponse::new(200, "hello").with_header("Version", "\"v1\"");
        assert_eq!(res.body_str(), Some("hello"));
        assert_eq!(res.header("version"), Some("\"v1\""));
    }
}
