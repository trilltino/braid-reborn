//! Shared header parsing and formatting for Braid-HTTP.

use crate::error::{BraidError, Result};
use crate::types::Version;

/// HTTP list-valued frontiers may occupy several field lines.
pub fn headers_from_http(
    headers: &http::HeaderMap,
) -> Result<std::collections::BTreeMap<String, String>> {
    let mut result = std::collections::BTreeMap::new();
    for key in headers.keys() {
        let values = headers
            .get_all(key)
            .iter()
            .map(|v| {
                v.to_str()
                    .map_err(|e| BraidError::HeaderParse(e.to_string()))
            })
            .collect::<Result<Vec<_>>>()?;
        if values.len() > 1
            && matches!(
                key.as_str(),
                "content-length"
                    | "content-range"
                    | "patches"
                    | "merge-type"
                    | "subscribe"
                    | "heartbeats"
                    | "repr-digest"
                    | "repr-type"
            )
        {
            return Err(BraidError::HeaderParse(format!("Duplicate header: {key}")));
        }
        result.insert(key.as_str().to_owned(), values.join(", "));
    }
    Ok(result)
}

/// Parse version header value.
pub fn parse_version_header(value: &str) -> Result<Vec<Version>> {
    let value = value.trim();
    if value.is_empty() {
        return Ok(vec![]);
    }
    use sfv::{BareItem, List, ListEntry, Parser};
    let list = Parser::new(value)
        .parse::<List>()
        .map_err(|e| BraidError::HeaderParse(format!("Invalid version frontier: {e}")))?;
    let mut versions = vec![];
    for entry in list {
        let version = match entry {
            ListEntry::Item(item) if item.params.is_empty() => match item.bare_item {
                BareItem::String(s) => Version::String(s.into()),
                BareItem::DisplayString(s) => Version::String(s),
                _ => return Err(BraidError::HeaderParse("Invalid version type".into())),
            },
            _ => return Err(BraidError::HeaderParse("Nested version frontier".into())),
        };
        if version.to_string().is_empty() || version.to_string().chars().any(char::is_control) {
            return Err(BraidError::HeaderParse(
                "Empty or control-containing event ID".into(),
            ));
        }
        if !versions.contains(&version) {
            versions.push(version);
        }
    }
    versions.sort_by_key(ToString::to_string);
    Ok(versions)
}

/// Format version header value.
pub fn format_version_header(versions: &[Version]) -> String {
    let mut identifiers: Vec<String> = versions.iter().map(ToString::to_string).collect();
    identifiers.sort();
    identifiers.dedup();
    identifiers
        .iter()
        .map(|s| {
            if s.bytes().all(|b| (0x20..=0x7e).contains(&b)) {
                format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))
            } else {
                sfv::ItemSerializer::new()
                    .bare_item(sfv::RefBareItem::DisplayString(s))
                    .finish()
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

pub fn format_version_header_json(versions: &[Version]) -> String {
    // Braid.org expects JSON array of STRINGS.
    // Ensure all versions (even Integers) are serialized as strings.
    let strings: Vec<String> = versions.iter().map(|v| v.to_string()).collect();
    serde_json::Value::from(strings).to_string()
}

/// The drafts use a Structured Fields boolean; `true`/`false` are retained for JS peers.
pub fn parse_subscribe(value: &str) -> Result<bool> {
    match value.trim() {
        "?1" | "true" => Ok(true),
        "?0" | "false" => Ok(false),
        _ => Err(BraidError::HeaderParse("Invalid Subscribe boolean".into())),
    }
}

pub fn parse_current_version_header(value: &str) -> Result<Vec<Version>> {
    parse_version_header(value)
}

/// Structured string identity, with bounded legacy token compatibility.
pub fn parse_peer(value: &str) -> Result<String> {
    let value = value.trim();
    let peer = if value.starts_with('"') || value.starts_with('%') {
        use sfv::{BareItem, Item, Parser};
        let item = Parser::new(value)
            .parse::<Item>()
            .map_err(|e| BraidError::HeaderParse(format!("Invalid Peer string: {e}")))?;
        if !item.params.is_empty() {
            return Err(BraidError::HeaderParse(
                "Peer parameters are unsupported".into(),
            ));
        }
        match item.bare_item {
            BareItem::String(peer) => peer.into(),
            BareItem::DisplayString(peer) => peer,
            _ => return Err(BraidError::HeaderParse("Peer must be a string".into())),
        }
    } else if value
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
    {
        value.to_owned()
    } else {
        return Err(BraidError::HeaderParse("Invalid peer identity".into()));
    };
    if peer.is_empty() || peer.len() > 512 || peer.chars().any(char::is_control) {
        return Err(BraidError::HeaderParse("Invalid peer length".into()));
    }
    Ok(peer)
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

/// Checked wire duration. Bare values are seconds; the legacy `s`/`ms` suffixes
/// are accepted without losing precision. There is no data-dependent fallback.
pub fn parse_heartbeat_duration(value: &str) -> Result<std::time::Duration> {
    let trimmed = value.trim();
    let (number, scale) = if let Some(number) = trimmed.strip_suffix("ms") {
        (number, 0.001)
    } else {
        (trimmed.strip_suffix('s').unwrap_or(trimmed), 1.0)
    };
    let seconds = number.parse::<f64>().ok().map(|n| n * scale);
    let seconds = seconds
        .filter(|n| n.is_finite() && *n > 0.0 && *n <= 86400.0)
        .ok_or_else(|| BraidError::HeaderParse(format!("Invalid heartbeat: {value}")))?;
    let duration = std::time::Duration::try_from_secs_f64(seconds)
        .map_err(|_| BraidError::HeaderParse(format!("Invalid heartbeat: {value}")))?;
    if duration.is_zero() {
        return Err(BraidError::HeaderParse("Heartbeat rounds to zero".into()));
    }
    Ok(duration)
}

/// Compatibility API: positive fractional seconds round upward, never to zero.
#[deprecated(note = "use parse_heartbeat_duration to retain subsecond precision")]
pub fn parse_heartbeat(value: &str) -> Result<u64> {
    let duration = parse_heartbeat_duration(value)?;
    Ok(duration.as_secs() + u64::from(duration.subsec_nanos() != 0))
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

/// Remove HTTP credentials, hop-by-hop fields and request transport metadata
/// before turning an HTTP entity into a persisted/replicated event. Unknown
/// application metadata is retained unless Connection explicitly nominates it.
pub fn strip_transport_headers(headers: &mut std::collections::BTreeMap<String, String>) {
    let nominated: Vec<_> = headers
        .iter()
        .filter(|(name, _)| name.eq_ignore_ascii_case("connection"))
        .flat_map(|(_, value)| {
            value
                .split(',')
                .map(|name| name.trim().to_ascii_lowercase())
        })
        .collect();
    headers.retain(|name, _| {
        let name = name.to_ascii_lowercase();
        !is_credential_header(&name)
            && !matches!(
                name.as_str(),
                "authorization"
                    | "proxy-authorization"
                    | "proxy-authenticate"
                    | "www-authenticate"
                    | "cookie"
                    | "cookie2"
                    | "set-cookie"
                    | "connection"
                    | "keep-alive"
                    | "te"
                    | "trailer"
                    | "transfer-encoding"
                    | "upgrade"
                    | "host"
                    | "user-agent"
                    | "accept"
                    | "accept-encoding"
                    | "origin"
                    | "referer"
            )
            && !name.starts_with("sec-fetch-")
            && !nominated.contains(&name)
    });
}

pub(crate) fn is_credential_header(name: &str) -> bool {
    [
        "authorization",
        "proxy-authorization",
        "cookie",
        "cookie2",
        "set-cookie",
        "set-cookie2",
        "authentication-info",
        "proxy-authentication-info",
        "www-authenticate",
        "proxy-authenticate",
    ]
    .iter()
    .any(|header| name.eq_ignore_ascii_case(header))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_headers() {
        assert_eq!(
            parse_heartbeat_duration("500ms").unwrap(),
            std::time::Duration::from_millis(500)
        );
        assert_eq!(format_content_range("json", ".f"), "json .f");
    }
}
