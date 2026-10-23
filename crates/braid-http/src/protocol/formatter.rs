//! Canonical byte-preserving Braid framing for every transport.
use crate::error::{BraidError, Result};
use crate::types::{BraidRequest, Update};
use bytes::{Bytes, BytesMut};
use std::collections::BTreeMap;

pub struct WireCodec;
impl WireCodec {
    pub fn encode(update: &Update) -> Result<Bytes> {
        format_update(update)
    }
    pub fn decode_entity(mut headers: BTreeMap<String, String>, body: Bytes) -> Result<Update> {
        // HTTP Content-Length describes the entire entity, not an individual patch.
        headers.remove("transfer-encoding");
        if headers.contains_key("patches") {
            headers.remove("content-length");
        } else {
            headers.insert("content-length".into(), body.len().to_string());
        }
        let mut frame = BytesMut::new();
        for (k, v) in &headers {
            write_header(&mut frame, k, v)?;
        }
        frame.extend_from_slice(b"\r\n");
        frame.extend_from_slice(&body);
        let mut parser = crate::client::MessageParser::new();
        let mut messages = parser.feed(&frame)?;
        parser.finish()?;
        if messages.len() != 1 {
            return Err(BraidError::Protocol("Expected exactly one update".into()));
        }
        crate::client::utils::message_to_update(messages.remove(0))
    }
}

pub fn format_update(update: &Update) -> Result<Bytes> {
    let (headers, body) = encode_parts(update)?;
    let mut frame = BytesMut::new();
    for (k, v) in headers {
        write_header(&mut frame, &k, &v)?;
    }
    frame.extend_from_slice(b"\r\n");
    frame.extend_from_slice(&body);
    Ok(frame.freeze())
}

pub fn encode_update_entity(update: &Update) -> Result<(BTreeMap<String, String>, Bytes)> {
    encode_parts(update)
}

pub fn encode_request(request: &BraidRequest) -> Result<(BTreeMap<String, String>, Bytes)> {
    if request.patches.is_some() && !request.body.is_empty() {
        return Err(BraidError::Protocol(
            "Request contains both snapshot and patches".into(),
        ));
    }
    let mut extra = request.extra_headers.clone();
    let mut version = request.version.clone();
    let mut parents = request.parents.clone();
    let mut merge_type = request.merge_type.clone();
    let mut content_type = request.content_type.clone();
    for (key, value) in &request.extra_headers {
        match key.to_ascii_lowercase().as_str() {
            "version" => {
                if version.is_some() {
                    return Err(BraidError::Protocol("Duplicate Version source".into()));
                }
                version = Some(crate::protocol::parse_version_header(value)?);
                extra.remove(key);
            }
            "parents" => {
                if parents.is_some() {
                    return Err(BraidError::Protocol("Duplicate Parents source".into()));
                }
                parents = Some(crate::protocol::parse_version_header(value)?);
                extra.remove(key);
            }
            "merge-type" => {
                merge_type = Some(value.clone());
                extra.remove(key);
            }
            "content-type" => {
                content_type = Some(value.clone());
                extra.remove(key);
            }
            _ => {}
        }
    }
    let has_parents = parents.is_some();
    let update = Update {
        version: version.unwrap_or_default(),
        parents: parents.unwrap_or_default(),
        patches: request.patches.clone(),
        body: if request.patches.is_some() {
            None
        } else {
            Some(request.body.clone())
        },
        merge_type,
        content_type,
        extra_headers: extra,
        ..Update::default()
    };
    let (mut headers, body) = encode_parts(&update)?;
    if !has_parents {
        headers.remove("parents");
    }
    if request.subscribe {
        headers.insert("subscribe".into(), "true".into());
    }
    if let Some(seconds) = request.heartbeat_interval {
        headers.insert("heartbeats".into(), seconds.to_string());
    }
    if let Some(peer) = &request.peer {
        headers.insert("peer".into(), serde_json::to_string(peer)?);
    }
    if let Some(ack) = &request.ack {
        headers.insert("ack".into(), crate::protocol::format_version_header(ack));
    }
    // The HTTP stack supplies the entity length, including all patch framing bytes.
    headers.remove("content-length");
    Ok((headers, body))
}

fn encode_parts(update: &Update) -> Result<(BTreeMap<String, String>, Bytes)> {
    if update.body.is_some() && update.patches.is_some() {
        return Err(BraidError::Protocol(
            "Update contains both snapshot and patches".into(),
        ));
    }
    let mut headers = BTreeMap::new();
    for (k, v) in &update.extra_headers {
        let name = k.to_ascii_lowercase();
        if matches!(
            name.as_str(),
            "content-length"
                | "patches"
                | "version"
                | "parents"
                | "current-version"
                | "content-range"
                | "transfer-encoding"
        ) {
            return Err(BraidError::Protocol(format!(
                "Reserved framing header: {k}"
            )));
        }
        headers.insert(name, v.clone());
    }
    if !update.version.is_empty() {
        headers.insert(
            "version".into(),
            crate::protocol::format_version_header(&update.version),
        );
    }
    headers.insert(
        "parents".into(),
        crate::protocol::format_version_header(&update.parents),
    );
    if let Some(current) = &update.current_version {
        headers.insert(
            "current-version".into(),
            crate::protocol::format_version_header(current),
        );
    }
    if let Some(mt) = &update.merge_type {
        headers.insert("merge-type".into(), mt.clone());
    }
    if let Some(ct) = &update.content_type {
        headers.insert("content-type".into(), ct.clone());
    }
    let body = if let Some(patches) = &update.patches {
        headers.insert("patches".into(), patches.len().to_string());
        let mut body = BytesMut::new();
        for patch in patches {
            write_header(
                &mut body,
                "content-length",
                &patch.content.len().to_string(),
            )?;
            write_header(
                &mut body,
                "content-range",
                &format!("{} {}", patch.unit, patch.range),
            )?;
            body.extend_from_slice(b"\r\n");
            body.extend_from_slice(&patch.content);
            body.extend_from_slice(b"\r\n");
        }
        body.freeze()
    } else {
        let body = update.body.clone().unwrap_or_default();
        headers.insert("content-length".into(), body.len().to_string());
        if let Some(range) = &update.content_range {
            headers.insert("content-range".into(), range.to_string());
        }
        body
    };
    for (k, v) in &headers {
        validate_header(k, v)?;
    }
    Ok((headers, body))
}
fn validate_header(key: &str, value: &str) -> Result<()> {
    http::header::HeaderName::from_bytes(key.as_bytes())
        .map_err(|e| BraidError::Protocol(e.to_string()))?;
    http::header::HeaderValue::from_str(value).map_err(|e| BraidError::Protocol(e.to_string()))?;
    Ok(())
}
fn write_header(buffer: &mut BytesMut, key: &str, value: &str) -> Result<()> {
    validate_header(key, value)?;
    buffer.extend_from_slice(key.as_bytes());
    buffer.extend_from_slice(b": ");
    buffer.extend_from_slice(value.as_bytes());
    buffer.extend_from_slice(b"\r\n");
    Ok(())
}
