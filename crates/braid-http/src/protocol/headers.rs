//! Shared header parsing and formatting for Braid-HTTP.

use crate::error::{BraidError, Result};
use crate::types::Version;

/// HTTP list-valued frontiers may occupy several field lines.
pub fn headers_from_http(
    headers: &http::HeaderMap,
) -> Result<std::collections::BTreeMap<String, String>> {
    let mut result = std::collections::BTreeMap::new();
    for key in headers.keys() {
        if matches!(key.as_str(), "version" | "parents" | "current-version") {
            let values = headers
                .get_all(key)
                .iter()
                .map(|v| {
                    v.to_str()
                        .map_err(|e| BraidError::HeaderParse(e.to_string()))
                })
                .collect::<Result<Vec<_>>>()?;
            result.insert(key.as_str().to_owned(), values.join(", "));
        } else if let Some(value) = headers
            .get_all(key)
            .iter()
            .filter_map(|v| v.to_str().ok())
            .last()
        {
            result.insert(key.as_str().to_owned(), value.to_owned());
        }
    }
    Ok(result)
}

/// Parse version header value.
pub fn parse_version_header(value: &str) -> Result<Vec<Version>> {
    let value = value.trim();
    if value.is_empty() {
        return Ok(vec![]);
    }
    // Explicit compatibility for older JSON-array frontiers; never selected by hostname.
    if value.starts_with('[') {
        let versions = serde_json::from_str::<Vec<String>>(value)
            .map_err(|e| BraidError::HeaderParse(e.to_string()))?;
        return Ok(versions.into_iter().map(Version::String).collect());
    }
    use sfv::{BareItem, List, ListEntry, Parser};
    let list = Parser::new(value)
        .parse::<List>()
        .map_err(|e| BraidError::HeaderParse(format!("Invalid version frontier: {e}")))?;
    let mut versions = vec![];
    for entry in list {
        let version = match entry {
            ListEntry::Item(item) => match item.bare_item {
                BareItem::String(s) => Version::String(s.into()),
                BareItem::Integer(i) => Version::Integer(i.into()),
                BareItem::Token(t) => Version::String(t.into()),
                _ => return Err(BraidError::HeaderParse("Invalid version type".into())),
            },
            _ => return Err(BraidError::HeaderParse("Nested version frontier".into())),
        };
        if !versions.contains(&version) {
            versions.push(version);
        }
    }
    Ok(versions)
}

/// Format version header value.
pub fn format_version_header(versions: &[Version]) -> String {
    versions
        .iter()
        .map(|v| match v {
            Version::String(s) => format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\"")),
            Version::Integer(i) => i.to_string(),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

pub fn format_version_header_json(versions: &[Version]) -> String {
    // Braid.org expects JSON array of STRINGS.
    // Ensure all versions (even Integers) are serialized as strings.
    let strings: Vec<String> = versions.iter().map(|v| v.to_string()).collect();
    let json = serde_json::to_string(&strings).unwrap_or_else(|_| "[]".to_string());
    tracing::info!("[Protocol] Formatted headers as JSON: {}", json);
    json
}

pub fn parse_current_version_header(value: &str) -> Result<Vec<Version>> {
    parse_version_header(value)
}

pub fn parse_content_range(value: &str) -> Result<(String, String)> {
    let parts: Vec<&str> = value.splitn(2, ' ').collect();
    if parts.len() != 2 {
        return Err(BraidError::HeaderParse(format!(
            "Invalid Content-Range: expected 'unit range', got '{}'",
            value
        )));
    }
    Ok((parts[0].to_string(), parts[1].to_string()))
}

#[inline]
pub fn format_content_range(unit: &str, range: &str) -> String {
    format!("{} {}", unit, range)
}

pub fn parse_heartbeat(value: &str) -> Result<u64> {
    let trimmed = value.trim();
    if let Some(ms_str) = trimmed.strip_suffix("ms") {
        return ms_str
            .parse::<u64>()
            .map(|n| n / 1000)
            .map_err(|_| BraidError::HeaderParse(format!("Invalid heartbeat: {}", value)));
    }
    if let Some(s_str) = trimmed.strip_suffix('s') {
        return s_str
            .parse()
            .map_err(|_| BraidError::HeaderParse(format!("Invalid heartbeat: {}", value)));
    }
    trimmed
        .parse()
        .map_err(|_| BraidError::HeaderParse(format!("Invalid heartbeat: {}", value)))
}

pub fn parse_merge_type(value: &str) -> Result<String> {
    let trimmed = value.trim();
    match trimmed {
        crate::protocol::constants::merge_types::DIAMOND => Ok(trimmed.to_string()),
        _ => Err(BraidError::HeaderParse(format!(
            "Unsupported merge-type: {}",
            value
        ))),
    }
}

pub fn parse_tunneled_response(
    bytes: &[u8],
) -> Result<(u16, std::collections::BTreeMap<String, String>, usize)> {
    let s = String::from_utf8_lossy(bytes);
    if let Some(end_idx) = s.find("\r\n\r\n") {
        let headers_part = &s[..end_idx];
        let mut status = 200;
        let mut headers = std::collections::BTreeMap::new();
        for line in headers_part.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            if let Some(val) = line.strip_prefix(":status:") {
                status = val.trim().parse().unwrap_or(200);
                continue;
            }
            if let Some((name, value)) = line.split_once(':') {
                headers.insert(name.trim().to_lowercase(), value.trim().to_string());
            }
        }
        Ok((status, headers, end_idx + 4))
    } else {
        Err(BraidError::HeaderParse("Incomplete headers".to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_headers() {
        assert_eq!(parse_heartbeat("5s").unwrap(), 5);
        assert_eq!(format_content_range("json", ".f"), "json .f");
    }
}
