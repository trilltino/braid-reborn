//! Shared header parsing and formatting for Braid-HTTP.

use crate::error::{BraidError, Result};
use crate::types::Version;

/// Parse version header value.
pub fn parse_version_header(value: &str) -> Result<Vec<Version>> {
    tracing::info!("[BraidHTTP] Parsing version header: '{}'", value);
    // 1. Try Structured Field Values (Strict Standard)
    use sfv::{BareItem, List, ListEntry, Parser};
    match Parser::new(value).parse::<List>() {
        Ok(list) => {
            let mut versions = Vec::new();
            for member in list {
                match member {
                    ListEntry::Item(item) => match item.bare_item {
                        BareItem::String(s) => versions.push(Version::String(s.into())),
                        BareItem::Integer(i) => versions.push(Version::Integer(i.into())),
                        BareItem::Token(t) => versions.push(Version::String(t.into())),
                        _ => {}
                    },
                    _ => {}
                }
            }
            if !versions.is_empty() {
                return Ok(versions);
            }
        }
        Err(_) => {}
    }

    // 2. Fallback: Try JSON Array (Braid.org often uses ["id"])
    if let Ok(json_arr) = serde_json::from_str::<Vec<String>>(value) {
        return Ok(json_arr.into_iter().map(Version::String).collect());
    }

    // 3. Fallback: Try JSON String (Quoted "id")
    if let Ok(json_str) = serde_json::from_str::<String>(value) {
        return Ok(vec![Version::String(json_str)]);
    }

    // 4. Fallback: Raw String (treat as single version ID)
    let trimmed = value.trim();
    if !trimmed.is_empty() {
        // Strip quotes and escapes recursively (handles "\"id\"", '"id"', etc.)
        let mut clean = trimmed.to_string();
        loop {
            let next = clean
                .trim_matches(|c| c == '"' || c == '\'' || c == '\\')
                .to_string();
            if next == clean || next.is_empty() {
                break;
            }
            clean = next;
        }

        if !clean.is_empty() {
            return Ok(vec![Version::String(clean)]);
        }
    }

    Ok(Vec::new())
}

/// Format version header value.
pub fn format_version_header(versions: &[Version]) -> String {
    versions
        .iter()
        .map(|v| match v {
            Version::String(s) => format!("\"{}\"", s.replace("\"", "\\\"")),
            Version::Integer(i) => format!("\"{}\"", i), // Force quotes around integers
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
