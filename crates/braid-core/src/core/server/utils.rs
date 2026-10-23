//! Server utilities for Braid protocol.
//!
//! Provides helper functions matching the JavaScript reference implementation.

use axum::response::Response;

/// Escape non-ASCII characters in headers for structured header format.
///
/// Matches JS reference `ascii_ify()` function:
/// ```javascript
/// function ascii_ify(s) {
///     return s.replace(/[^\x20-\x7E]/g,
///         c => '\\u' + c.charCodeAt(0).toString(16).padStart(4, '0'))
/// }
/// ```
///
/// # Arguments
///
/// * `s` - The string to escape
///
/// # Returns
///
/// A string with non-printable ASCII characters escaped as `\uXXXX`.
pub fn ascii_ify(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    for c in s.chars() {
        let code = c as u32;
        if (0x20..=0x7E).contains(&code) {
            result.push(c);
        } else {
            result.push_str(&format!("\\u{:04x}", code));
        }
    }
    result
}

/// Set permissive CORS headers on a response.
///
/// Matches JS reference `free_cors()` function:
/// ```javascript
/// function free_cors(res) {
///     res.setHeader("Access-Control-Allow-Origin", "*")
///     res.setHeader("Access-Control-Allow-Methods", "*")
///     res.setHeader("Access-Control-Allow-Headers", "*")
///     res.setHeader("Access-Control-Expose-Headers", "*")
/// }
/// ```
pub fn free_cors_headers() -> Vec<(&'static str, &'static str)> {
    vec![
        ("access-control-allow-origin", "*"),
        ("access-control-allow-methods", "*"),
        ("access-control-allow-headers", "*"),
        ("access-control-expose-headers", "*"),
    ]
}

/// Apply permissive CORS headers to a response builder.
pub fn apply_free_cors(response: &mut Response) {
    let headers = response.headers_mut();
    for (name, value) in free_cors_headers() {
        headers.insert(
            axum::http::header::HeaderName::from_static(name),
            axum::http::header::HeaderValue::from_static(value),
        );
    }
}

/// Number of extra newlines to add for Firefox workaround.
///
/// Firefox has a network buffering bug that requires extra newlines
/// in streaming responses to flush properly.
pub const FIREFOX_EXTRA_NEWLINES: usize = 240;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ascii_ify_simple() {
        assert_eq!(ascii_ify("hello"), "hello");
        assert_eq!(ascii_ify("test 123"), "test 123");
    }

    #[test]
    fn test_ascii_ify_unicode() {
        // Non-ASCII characters should be escaped
        assert_eq!(ascii_ify("héllo"), "h\\u00e9llo");
        assert_eq!(ascii_ify("日本語"), "\\u65e5\\u672c\\u8a9e");
    }

    #[test]
    fn test_ascii_ify_control_chars() {
        // Control characters should be escaped
        assert_eq!(ascii_ify("hello\nworld"), "hello\\u000aworld");
        assert_eq!(ascii_ify("tab\there"), "tab\\u0009here");
    }

    #[test]
    fn test_free_cors_headers() {
        let headers = free_cors_headers();
        assert_eq!(headers.len(), 4);
        assert!(headers
            .iter()
            .any(|(k, _)| *k == "access-control-allow-origin"));
    }
}
