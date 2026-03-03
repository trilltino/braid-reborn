//! Patch representing a partial update to a resource.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// A patch representing a partial update to a resource.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Patch {
    /// The addressing unit type (e.g., `"json"`, `"bytes"`)
    pub unit: String,
    /// The range specification
    pub range: String,
    /// The patch content
    pub content: Bytes,
    /// Content length in bytes
    pub content_length: Option<usize>,
}

#[cfg(feature = "fuzzing")]
impl<'a> arbitrary::Arbitrary<'a> for Patch {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(Patch {
            unit: u.arbitrary()?,
            range: u.arbitrary()?,
            content: bytes::Bytes::from(u.arbitrary::<Vec<u8>>()?),
            content_length: u.arbitrary()?,
        })
    }
}

impl Patch {
    /// Create a new patch.
    #[must_use]
    pub fn new(
        unit: impl Into<String>,
        range: impl Into<String>,
        content: impl Into<Bytes>,
    ) -> Self {
        let content_bytes = content.into();
        let content_length = content_bytes.len();
        Patch {
            unit: unit.into(),
            range: range.into(),
            content: content_bytes,
            content_length: Some(content_length),
        }
    }

    #[inline]
    #[must_use]
    pub fn json(range: impl Into<String>, content: impl Into<Bytes>) -> Self {
        Self::new("json", range, content)
    }

    #[inline]
    #[must_use]
    pub fn bytes(range: impl Into<String>, content: impl Into<Bytes>) -> Self {
        Self::new("bytes", range, content)
    }

    #[inline]
    #[must_use]
    pub fn text(range: impl Into<String>, content: impl Into<String>) -> Self {
        let content_str = content.into();
        Self::new("text", range, Bytes::from(content_str))
    }

    #[inline]
    #[must_use]
    pub fn lines(range: impl Into<String>, content: impl Into<String>) -> Self {
        let content_str = content.into();
        Self::new("lines", range, Bytes::from(content_str))
    }

    #[must_use]
    pub fn with_length(
        unit: impl Into<String>,
        range: impl Into<String>,
        content: impl Into<Bytes>,
        length: usize,
    ) -> Self {
        Patch {
            unit: unit.into(),
            range: range.into(),
            content: content.into(),
            content_length: Some(length),
        }
    }

    #[inline]
    #[must_use]
    pub fn is_json(&self) -> bool {
        self.unit == "json"
    }

    #[inline]
    #[must_use]
    pub fn is_bytes(&self) -> bool {
        self.unit == "bytes"
    }

    #[inline]
    #[must_use]
    pub fn is_text(&self) -> bool {
        self.unit == "text"
    }

    #[inline]
    #[must_use]
    pub fn is_lines(&self) -> bool {
        self.unit == "lines"
    }

    #[inline]
    #[must_use]
    pub fn content_str(&self) -> Option<&str> {
        std::str::from_utf8(&self.content).ok()
    }

    #[inline]
    #[must_use]
    pub fn content_text(&self) -> Option<&str> {
        self.content_str()
    }

    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        self.content_length.unwrap_or_else(|| self.content.len())
    }

    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[must_use]
    pub fn content_range_header(&self) -> String {
        format!("{} {}", self.unit, self.range)
    }

    pub fn validate(&self) -> crate::error::Result<()> {
        if self.unit.is_empty() {
            return Err(crate::error::BraidError::Protocol(
                "Patch unit cannot be empty".into(),
            ));
        }
        if self.range.is_empty() {
            return Err(crate::error::BraidError::Protocol(
                "Patch range cannot be empty".into(),
            ));
        }
        Ok(())
    }
}

impl Default for Patch {
    fn default() -> Self {
        Patch {
            unit: "bytes".to_string(),
            range: String::new(),
            content: Bytes::new(),
            content_length: Some(0),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_patch_new() {
        let patch = Patch::new("custom", "range", "content");
        assert_eq!(patch.unit, "custom");
        assert_eq!(patch.range, "range");
        assert_eq!(patch.content_length, Some(7));
    }

    #[test]
    fn test_patch_json() {
        let patch = Patch::json(".field", "value");
        assert_eq!(patch.unit, "json");
        assert_eq!(patch.range, ".field");
        assert!(patch.is_json());
    }

    #[test]
    fn test_patch_bytes() {
        let patch = Patch::bytes("0:100", &b"content"[..]);
        assert_eq!(patch.unit, "bytes");
        assert_eq!(patch.range, "0:100");
        assert!(patch.is_bytes());
    }

    #[test]
    fn test_patch_text() {
        let patch = Patch::text(".title", "New Title");
        assert_eq!(patch.unit, "text");
        assert_eq!(patch.range, ".title");
        assert!(patch.is_text());
    }

    #[test]
    fn test_patch_lines() {
        let patch = Patch::lines("10:20", "new lines\n");
        assert_eq!(patch.unit, "lines");
        assert_eq!(patch.range, "10:20");
        assert!(patch.is_lines());
    }

    #[test]
    fn test_content_str() {
        let patch = Patch::json(".field", "value");
        assert_eq!(patch.content_str(), Some("value"));
    }

    #[test]
    fn test_len_and_is_empty() {
        let patch = Patch::json(".field", "value");
        assert_eq!(patch.len(), 5);
        assert!(!patch.is_empty());

        let empty = Patch::json(".field", "");
        assert_eq!(empty.len(), 0);
        assert!(empty.is_empty());
    }

    #[test]
    fn test_content_range_header() {
        let patch = Patch::json(".users[0]", "{}");
        assert_eq!(patch.content_range_header(), "json .users[0]");
    }

    #[test]
    fn test_default() {
        let patch = Patch::default();
        assert_eq!(patch.unit, "bytes");
        assert!(patch.range.is_empty());
        assert!(patch.is_empty());
    }

    #[test]
    fn test_validate_valid_patch() {
        let patch = Patch::json(".field", "value");
        assert!(patch.validate().is_ok());
    }

    #[test]
    fn test_validate_empty_unit() {
        let patch = Patch {
            unit: String::new(),
            range: ".field".to_string(),
            content: Bytes::from("value"),
            content_length: Some(5),
        };
        assert!(patch.validate().is_err());
    }

    #[test]
    fn test_validate_empty_range() {
        let patch = Patch {
            unit: "json".to_string(),
            range: String::new(),
            content: Bytes::from("value"),
            content_length: Some(5),
        };
        assert!(patch.validate().is_err());
    }

    #[test]
    fn test_validate_all_types() {
        assert!(Patch::json(".f", "v").validate().is_ok());
        assert!(Patch::bytes("0:10", &b"data"[..]).validate().is_ok());
        assert!(Patch::text(".t", "text").validate().is_ok());
        assert!(Patch::lines("1:5", "lines").validate().is_ok());
    }
}
