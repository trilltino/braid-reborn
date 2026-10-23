//! Content-Range specification for patches.

use std::fmt;
use std::str::FromStr;

/// Content-Range specification for patches.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct ContentRange {
    /// The addressing unit type (e.g., "json", "bytes").
    pub unit: String,
    /// The range specification within the resource.
    pub range: String,
}

impl ContentRange {
    #[inline]
    #[must_use]
    pub fn new(unit: impl Into<String>, range: impl Into<String>) -> Self {
        ContentRange {
            unit: unit.into(),
            range: range.into(),
        }
    }

    #[inline]
    #[must_use]
    pub fn json(range: impl Into<String>) -> Self {
        Self::new("json", range)
    }
    #[inline]
    #[must_use]
    pub fn bytes(range: impl Into<String>) -> Self {
        Self::new("bytes", range)
    }
    #[inline]
    #[must_use]
    pub fn text(range: impl Into<String>) -> Self {
        Self::new("text", range)
    }
    #[inline]
    #[must_use]
    pub fn lines(range: impl Into<String>) -> Self {
        Self::new("lines", range)
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

    #[must_use]
    pub fn to_header_value(&self) -> String {
        format!("{} {}", self.unit, self.range)
    }

    pub fn from_header_value(value: &str) -> Result<Self, String> {
        let parts: Vec<&str> = value.splitn(2, ' ').collect();
        if parts.len() != 2 {
            return Err(format!(
                "Invalid Content-Range: expected 'unit range', got '{}'",
                value
            ));
        }
        Ok(ContentRange {
            unit: parts[0].to_string(),
            range: parts[1].to_string(),
        })
    }
}

impl Default for ContentRange {
    fn default() -> Self {
        ContentRange {
            unit: "bytes".to_string(),
            range: "0:0".to_string(),
        }
    }
}

impl fmt::Display for ContentRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.unit, self.range)
    }
}

impl FromStr for ContentRange {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_header_value(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_content_range_basic() {
        let range = ContentRange::new("json", ".field");
        assert_eq!(range.to_header_value(), "json .field");
        let parsed = ContentRange::from_header_value("json .field").unwrap();
        assert_eq!(parsed, range);
    }
}
