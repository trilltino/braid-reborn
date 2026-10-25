//! Utility functions for the Braid HTTP client.

use crate::client::parser::Message;
use crate::error::Result;
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
    protocol::parse_heartbeat_duration(value)
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

pub fn message_to_update(msg: Message) -> Result<Update> {
    let parse = |name: &str| -> Result<Vec<Version>> {
        msg.headers
            .get(name)
            .map(|v| protocol::parse_version_header(v))
            .transpose()
            .map(|v| v.unwrap_or_default())
    };
    let version = parse("version")?;
    let parents = parse("parents")?;
    let current_version = msg
        .headers
        .get("current-version")
        .map(|v| protocol::parse_version_header(v))
        .transpose()?;
    let content_range = msg
        .headers
        .get("content-range")
        .map(|v| protocol::parse_content_range(v))
        .transpose()?;
    let extra_headers = msg.extra_headers();
    let status = msg.status().unwrap_or(200);
    let mut patches = msg.patches;
    let is_patched = msg.headers.contains_key("patches") || content_range.is_some();
    if let Some((unit, range)) = content_range {
        patches.push(crate::types::Patch::new(unit, range, msg.body.clone()));
    }
    Ok(Update {
        version,
        parents,
        current_version,
        merge_type: msg.headers.get("merge-type").cloned(),
        content_type: msg.headers.get("content-type").cloned(),
        body: if is_patched { None } else { Some(msg.body) },
        patches: if is_patched { Some(patches) } else { None },
        status,
        extra_headers,
        url: msg.url,
        ..Update::default()
    })
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
    // Browser timers use signed 32-bit milliseconds. A long configured retry
    // must remain a long wait instead of overflowing into a busy reconnect loop.
    // Round a fractional millisecond up; never shorten a requested deadline.
    let mut remaining = duration;
    while !remaining.is_zero() {
        let millis = remaining.as_millis().min(i32::MAX as u128) as u32;
        let millis = millis.max(1);
        gloo_timers::future::TimeoutFuture::new(millis).await;
        remaining = remaining.saturating_sub(Duration::from_millis(u64::from(millis)));
    }
}
