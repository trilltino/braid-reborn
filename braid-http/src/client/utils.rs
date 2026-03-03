//! Utility functions for the Braid HTTP client.

use crate::client::parser::Message;
use crate::error::{BraidError, Result};
use crate::protocol;
use crate::types::{Update, Version};
use bytes::{Bytes, BytesMut};
use std::time::Duration;

pub fn parse_content_range(header: &str) -> Result<(String, String)> {
    protocol::parse_content_range(header)
}

pub fn format_content_range(unit: &str, range: &str) -> String {
    protocol::format_content_range(unit, range)
}

pub fn parse_heartbeat(value: &str) -> Result<Duration> {
    let trimmed = value.trim();
    let num_str = if let Some(s) = trimmed.strip_suffix("ms") {
        s
    } else if let Some(s) = trimmed.strip_suffix('s') {
        s
    } else {
        trimmed
    };

    let num: f64 = num_str
        .parse()
        .map_err(|_| BraidError::HeaderParse(format!("Invalid heartbeat: {}", value)))?;
    Ok(Duration::from_secs_f64(num))
}

pub fn version_to_json_string(version: &str) -> String {
    format!("\"{}\"", version)
}

pub fn is_retryable_status(status: u16) -> bool {
    matches!(status, 408 | 425 | 429 | 502 | 503 | 504)
}

pub fn is_access_denied_status(status: u16) -> bool {
    matches!(status, 401 | 403)
}

pub fn exponential_backoff(attempt: u32, base_ms: u64) -> Duration {
    let delay_ms = base_ms * 2_u64.pow(attempt.min(10));
    Duration::from_millis(delay_ms)
}

pub fn merge_bodies(body1: &Bytes, body2: &Bytes) -> Bytes {
    let mut result = BytesMut::with_capacity(body1.len() + body2.len());
    result.extend_from_slice(body1);
    result.extend_from_slice(body2);
    result.freeze()
}

pub fn message_to_update(msg: Message) -> Update {
    let version = extract_version(&msg.headers).unwrap_or_else(|| {
        // Fallback: If version is missing, we use a unique temporary version for this update
        // to avoid colliding with other "missing" states, but ideally we should skip.
        // For braid.org/diamond-types, we should use a valid-looking id if we MUST have one.
        let temp_id = "temp-0".to_string();
        tracing::warn!(
            "[BraidHTTP] Version header missing in message from {}. Using temporary ID: {}",
            msg.url.as_deref().unwrap_or("unknown"),
            temp_id
        );
        Version::new(&temp_id)
    });

    let mut builder = if !msg.patches.is_empty() {
        Update::patched(version, msg.patches)
    } else {
        let body = String::from_utf8_lossy(&msg.body).to_string();
        Update::snapshot(version, body)
    };

    if let Some(parents) = extract_parents(&msg.headers) {
        for parent in parents {
            builder = builder.with_parent(parent);
        }
    }

    if let Some(merge_type) = msg.headers.get("merge-type") {
        builder = builder.with_merge_type(merge_type.clone());
    }

    builder.url = msg.url;
    builder
}

fn extract_version(headers: &std::collections::BTreeMap<String, String>) -> Option<Version> {
    let version = headers
        .get("current-version")
        .or_else(|| headers.get("version"))
        .or_else(|| headers.get("Version"))
        .or_else(|| headers.get("Current-Version"))
        .and_then(|v| {
            let trimmed = v.trim();
            if trimmed.is_empty() || trimmed == "\"\"" {
                return None;
            }
            protocol::parse_version_header(v).ok()
        })
        .and_then(|mut v| v.pop());

    if version.is_none() {
        tracing::info!(
            "[BraidHTTP] extract_version failed. Headers were: {:?}",
            headers
        );
    } else {
        tracing::debug!("[BraidHTTP] Parsed version: {:?}", version);
    }
    version
}

fn extract_parents(headers: &std::collections::BTreeMap<String, String>) -> Option<Vec<Version>> {
    let parents = headers
        .get("parents")
        .and_then(|v| protocol::parse_version_header(v).ok());

    if parents.is_none() {
        tracing::debug!(
            "[BraidHTTP] Parents header missing or failed to parse. Headers: {:?}",
            headers
        );
    } else {
        tracing::debug!("[BraidHTTP] Parsed parents: {:?}", parents);
    }
    parents
}

#[cfg(not(target_arch = "wasm32"))]
pub fn spawn_task<F>(future: F)
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    tokio::spawn(future);
}

#[cfg(target_arch = "wasm32")]
pub fn spawn_task<F>(future: F)
where
    F: std::future::Future<Output = ()> + 'static,
{
    wasm_bindgen_futures::spawn_local(future);
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn sleep(duration: Duration) {
    tokio::time::sleep(duration).await;
}

#[cfg(target_arch = "wasm32")]
pub async fn sleep(duration: Duration) {
    gloo_timers::future::sleep(duration).await;
}
