//! Version identifier for the Braid-HTTP protocol.

use std::hash::Hash;

/// A version identifier in the Braid protocol.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum Version {
    /// String-based version ID.
    String(String),
    /// Integer-based version ID.
    Integer(i64),
}

impl Version {
    /// Create a new string-based version.
    #[inline]
    #[must_use]
    pub fn new(s: impl Into<String>) -> Self {
        Version::String(s.into())
    }

    /// Create a new integer-based version.
    #[inline]
    #[must_use]
    pub fn integer(n: i64) -> Self {
        Version::Integer(n)
    }

    #[inline]
    #[must_use]
    pub fn is_string(&self) -> bool {
        matches!(self, Version::String(_))
    }

    #[inline]
    #[must_use]
    pub fn is_integer(&self) -> bool {
        matches!(self, Version::Integer(_))
    }

    #[inline]
    #[must_use]
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Version::String(s) => Some(s),
            Version::Integer(_) => None,
        }
    }

    #[inline]
    #[must_use]
    pub fn as_integer(&self) -> Option<i64> {
        match self {
            Version::Integer(i) => Some(*i),
            Version::String(_) => None,
        }
    }

    #[must_use]
    pub fn to_json(&self) -> serde_json::Value {
        match self {
            Version::String(s) => serde_json::json!(s),
            Version::Integer(i) => serde_json::json!(i),
        }
    }

    #[must_use]
    pub fn from_json(value: serde_json::Value) -> Self {
        match value {
            serde_json::Value::String(s) => Version::String(s),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Version::Integer(i)
                } else {
                    Version::String(n.to_string())
                }
            }
            v => Version::String(v.to_string()),
        }
    }
}

impl std::fmt::Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Version::String(s) => write!(f, "{}", s),
            Version::Integer(i) => write!(f, "{}", i),
        }
    }
}

impl From<String> for Version {
    #[inline]
    fn from(s: String) -> Self {
        Version::String(s)
    }
}

impl From<&str> for Version {
    #[inline]
    fn from(s: &str) -> Self {
        Version::String(s.to_string())
    }
}

impl From<i64> for Version {
    #[inline]
    fn from(n: i64) -> Self {
        Version::Integer(n)
    }
}

impl From<i32> for Version {
    #[inline]
    fn from(n: i32) -> Self {
        Version::Integer(n as i64)
    }
}

impl Default for Version {
    fn default() -> Self {
        Version::String(String::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_new() {
        let v = Version::new("abc123");
        assert_eq!(v, Version::String("abc123".to_string()));
    }

    #[test]
    fn test_version_integer() {
        let v = Version::integer(42);
        assert_eq!(v, Version::Integer(42));
    }

    #[test]
    fn test_version_display_string() {
        let v = Version::String("v1".to_string());
        assert_eq!(v.to_string(), "v1");
    }

    #[test]
    fn test_version_display_integer() {
        let v = Version::Integer(42);
        assert_eq!(v.to_string(), "42");
    }

    #[test]
    fn test_version_from_str() {
        let v: Version = "v1".into();
        assert_eq!(v, Version::String("v1".to_string()));
    }

    #[test]
    fn test_version_from_string() {
        let v: Version = String::from("v1").into();
        assert_eq!(v, Version::String("v1".to_string()));
    }

    #[test]
    fn test_version_from_i64() {
        let v: Version = 42i64.into();
        assert_eq!(v, Version::Integer(42));
    }

    #[test]
    fn test_version_from_i32() {
        let v: Version = 42i32.into();
        assert_eq!(v, Version::Integer(42));
    }

    #[test]
    fn test_is_string() {
        assert!(Version::new("abc").is_string());
        assert!(!Version::Integer(42).is_string());
    }

    #[test]
    fn test_is_integer() {
        assert!(Version::Integer(42).is_integer());
        assert!(!Version::new("abc").is_integer());
    }

    #[test]
    fn test_as_str() {
        let v = Version::new("abc");
        assert_eq!(v.as_str(), Some("abc"));

        let v = Version::Integer(42);
        assert_eq!(v.as_str(), None);
    }

    #[test]
    fn test_as_integer() {
        let v = Version::Integer(42);
        assert_eq!(v.as_integer(), Some(42));

        let v = Version::new("abc");
        assert_eq!(v.as_integer(), None);
    }

    #[test]
    fn test_to_json_string() {
        let v = Version::new("abc");
        assert_eq!(v.to_json(), serde_json::json!("abc"));
    }

    #[test]
    fn test_to_json_integer() {
        let v = Version::Integer(42);
        assert_eq!(v.to_json(), serde_json::json!(42));
    }

    #[test]
    fn test_from_json_string() {
        let v = Version::from_json(serde_json::json!("abc"));
        assert_eq!(v, Version::String("abc".to_string()));
    }

    #[test]
    fn test_from_json_integer() {
        let v = Version::from_json(serde_json::json!(42));
        assert_eq!(v, Version::Integer(42));
    }

    #[test]
    fn test_version_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(Version::new("v1"));
        set.insert(Version::new("v2"));
        set.insert(Version::Integer(1));
        assert_eq!(set.len(), 3);
    }

    #[test]
    fn test_version_default() {
        let v = Version::default();
        assert_eq!(v, Version::String(String::new()));
    }
}
