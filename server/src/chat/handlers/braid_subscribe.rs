//! Pure Braid Protocol Subscription Handler
//!
//! Uses braid-http and braid-core for all subscription handling.
//! NO SSE - pure Braid protocol throughout.
//!
//! # Braid Protocol Format (xfmail-style)
//!
//! Per draft-toomim-httpbis-braid-http-04, subscriptions use:
//! - HTTP 209 status code for subscription responses
//! - Multipart message boundaries for updates
//! - Version and Parents headers for each update
//! - Merge-Type header for conflict resolution strategy
//!
//! # Wire Format
//!
//! ```text
//! HTTP/1.1 209 Subscription
//! Content-Type: application/json
//! Subscribe: true
//! Merge-Type: diamond
//!
//! Version: "v1"
//! Content-Length: 42
//!
//! {"id": "...", "content": "Hello"}
//!
//! Version: "v2"
//! Parents: "v1"
//! Content-Length: 45
//!
//! {"id": "...", "content": "Hello World"}
//! ```

use crate::core::config::AppState;
use crate::core::models::Message;
use crate::core::store::json_store::UpdateType;
use axum::{
    body::Body,
    extract::{Path, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use braid_http::protocol::{
    constants::headers,
    headers::{format_version_header, parse_heartbeat, parse_version_header},
};
use bytes::Bytes;
use std::convert::Infallible;
use std::time::Duration;
use tracing::{debug, error, info};

/// Format a Braid update for the wire (multipart format).
/// Based on xfmail's implementation for spec compliance.
fn format_braid_update(message: &Message, crdt_version: Option<&str>) -> Bytes {
    let body = serde_json::to_string(message).unwrap_or_default();
    let mut update = String::new();

    // Version header (required)
    let version = crdt_version.unwrap_or(&message.version);
    update.push_str(&format!("Version: \"{}\"\r\n", version));

    // Parents header (if any)
    if !message.parents.is_empty() {
        update.push_str(&format!(
            "Parents: {}\r\n",
            format_version_header(&message.parents)
        ));
    }

    // Content-Length header (required for multipart)
    update.push_str(&format!("Content-Length: {}\r\n", body.len()));
    update.push_str("\r\n");
    update.push_str(&body);
    update.push_str("\r\n\r\n");

    // Log for Inspector in formal Braid-HTTP format
    let patch = format!(
        "[{{\"unit\": \"json\", \"range\": \"[0:0]\", \"content\": {}}}]",
        body
    );
    info!(
        target: "braid_inspector",
        "[BRAID-CHAT] Update:\n\n\
         HTTP/1.1 209 Subscription\n\
         Host: braid.org\n\
         Version: \"{}\"\n\
         Parents: {}\n\
         Author: {}\n\
         Merge-Type: diamond\n\
         Patches: 1\n\
         Content-Type: application/json\n\
         Content-Length: {}\n\
         \n\
         {}",
        version,
        if !message.parents.is_empty() {
            format_version_header(&message.parents)
        } else {
            "[]".to_string()
        },
        message.sender,
        patch.len(),
        patch
    );

    Bytes::from(update)
}

/// Handle pure Braid subscription for conversation messages.
///
/// GET /chat/{room_id}/subscribe
///
/// This handler implements true Braid-HTTP subscriptions (xfmail-style):
/// - Returns HTTP 209 status for subscriptions
/// - Uses multipart format for streaming updates
/// - Includes Version and Parents headers per update
/// - Specifies Merge-Type: diamond for CRDT conflict resolution
///
/// Headers:
///   - Subscribe: true (required)
///   - Heartbeats: 30s (optional)
///   - Version: "10@server" (optional - for resuming)
///   - Parents: "v1", "v2" (optional - for catch-up sync)
pub async fn braid_subscribe(
    Path(room_id): Path<String>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> std::result::Result<Response<Body>, StatusCode> {
    info!("[BraidSubscribe] /chat/{}", room_id);

    // Check for Subscribe header (Braid protocol requirement)
    if headers.get(&headers::SUBSCRIBE).is_none() {
        error!("[BraidSubscribe] Missing Subscribe header");
        return Err(StatusCode::BAD_REQUEST);
    }

    // Parse Parents header for catch-up sync (xfmail feature)
    let client_parents: Vec<braid_http::types::Version> = headers
        .get(&headers::PARENTS)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| parse_version_header(v).ok())
        .unwrap_or_default();

    // Get heartbeat interval from header
    let heartbeat = headers
        .get(&headers::HEARTBEATS)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| parse_heartbeat(v).ok())
        .unwrap_or(30);

    // Parse Version header for resuming subscription
    let since_version: Option<String> = headers
        .get(&headers::VERSION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| parse_version_header(v).ok())
        .and_then(|v| v.first().map(|v| v.to_string()));

    debug!(
        "[BraidSubscribe] heartbeat={}s, since_version={:?}, parents={:?}",
        heartbeat, since_version, client_parents
    );

    // Get current room state and load initial messages
    let (current_version, initial_messages) = {
        if let Some(room_lock) = state.store.get_room(&room_id).await.ok().flatten() {
            let version = {
                let room_data = room_lock.read().await;
                room_data
                    .crdt
                    .get_frontier()
                    .first()
                    .cloned()
                    .unwrap_or_else(|| braid_http::types::Version::String("0@server".to_string()))
                    .to_string()
            };

            // Get messages - use catch-up sync if parents provided - NO LOCK HELD
            let messages = if client_parents.is_empty() {
                // No parents = get recent messages
                state
                    .store
                    .get_messages(&room_id, since_version.as_deref())
                    .await
                    .unwrap_or_default()
            } else {
                // Catch-up sync: get messages since parents
                state
                    .store
                    .get_messages_since_parents(&room_id, &client_parents, 100)
                    .await
                    .unwrap_or_default()
            };

            (version, messages)
        } else {
            ("0@server".to_string(), Vec::new())
        }
    };

    info!(
        "[BRAID-STREAM] Established for room {} with {} initial messages",
        room_id,
        initial_messages.len()
    );

    // Get the broadcast channel for this room
    let channel = state.store.get_channel(&room_id).await;
    let mut rx = channel.tx.subscribe();

    // Create the Braid subscription stream
    let stream = async_stream::stream! {
        // Send initial messages using multipart format
        for msg in initial_messages {
            let crdt_version = msg.version.clone();
            yield Ok::<_, Infallible>(format_braid_update(&msg, Some(&crdt_version)));
        }

        // Stream updates with Braid protocol
        let mut heartbeat_interval = tokio::time::interval(Duration::from_secs(heartbeat));

        loop {
            tokio::select! {
                // Wait for room updates
                Ok(update) = rx.recv() => {
                    match update.update_type {
                        UpdateType::Message => {
                            // Parse the message from update data
                            if let Ok(msg) = serde_json::from_value::<Message>(update.data.clone()) {
                                yield Ok::<_, Infallible>(format_braid_update(&msg, update.crdt_version.as_deref()));
                            }
                        }
                        _ => {
                            // For non-message updates, use simple format
                            let data = serde_json::to_string(&update.data).unwrap_or_default();
                            let event_type = match update.update_type {
                                UpdateType::Presence => "presence",
                                UpdateType::Typing => "typing",
                                UpdateType::RoomUpdate => "room",
                                UpdateType::Sync => "sync",
                                _ => "unknown",
                            };

                            let mut output = String::new();
                            if let Some(version) = &update.crdt_version {
                                output.push_str(&format!("Version: \"{}\"\r\n", version));
                            }
                            output.push_str(&format!("type: {}\r\n", event_type));
                            output.push_str(&format!("Content-Length: {}\r\n", data.len()));
                            output.push_str("\r\n");
                            output.push_str(&data);
                            output.push_str("\r\n\r\n");
                            yield Ok::<_, Infallible>(Bytes::from(output));
                        }
                    }
                }

                // Send heartbeat (blank line = Braid keepalive)
                _ = heartbeat_interval.tick() => {
                    yield Ok::<_, Infallible>(Bytes::from("\r\n".to_string()));
                }
            }
        }
    };

    // Build HTTP 209 response with Braid headers (xfmail-style)
    let response = Response::builder()
        .status(StatusCode::from_u16(209).unwrap()) // HTTP 209 Subscription
        .header(header::CONTENT_TYPE, "application/json")
        .header(headers::SUBSCRIBE.as_str(), "true")
        .header("Merge-Type", "diamond")
        .header("Cache-Control", "no-cache")
        .header("Connection", "keep-alive")
        .header(headers::HEARTBEATS.as_str(), format!("{}s", heartbeat))
        .header(
            headers::VERSION.as_str(),
            format!("\"{}\"", current_version),
        )
        .body(Body::from_stream(stream))
        .map_err(|e| {
            error!("[BraidSubscribe] Failed to build response: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_braid_header_format() {
        // Test that we use proper Braid header formatting
        let version = "42@server";
        let formatted = format!("{}: \"{}\"\r\n", headers::VERSION.as_str(), version);
        assert!(formatted.contains("version:"));
        assert!(formatted.contains("42@server"));
    }
}
