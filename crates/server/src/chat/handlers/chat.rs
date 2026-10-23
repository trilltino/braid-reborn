//! Chat handlers using pure Braid protocol
//!
//! All endpoints use braid-http protocol headers and braid-core CRDT.
//! NO SSE - subscriptions are handled by braid_subscribe.rs

use crate::core::{
    config::AppState,
    models::{
        BlobRef, ChatRoom, ChatSnapshot, CreateMessageInput, MessageType, MessageTypeInput,
        RoomSyncStatus, SyncStatus,
    },
};
use axum::{
    extract::{Multipart, Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Json, Response},
};
use braid_http::protocol::{
    constants::headers,
    headers::{format_version_header, parse_version_header},
};
use tracing::{error, info, warn};

/// GET /chat/:room_id
///
/// Braid protocol endpoint for fetching chat room state.
/// Returns current messages as JSON with Braid version headers.
pub async fn get_chat_room(
    Path(room_id): Path<String>,
    headers: HeaderMap,
    State(state): State<AppState>,
) -> Result<Response, Response> {
    info!("GET /chat/{}", room_id);

    // Parse Braid version header if present
    let since_version = headers
        .get(&headers::VERSION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| parse_version_header(v).ok())
        .and_then(|versions| versions.first().map(|v| v.to_string()));

    // Get or create room
    let room_lock = state
        .store
        .get_or_create_room(&room_id, Some("anonymous"))
        .await
        .map_err(|e| {
            error!("Failed to get/create room: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        })?;

    // Get room info and version while holding lock
    let (room_info, frontier) = {
        let room = room_lock.read().await;
        (room.room.clone(), room.crdt.get_frontier())
    };

    let current_version = frontier
        .first()
        .cloned()
        .unwrap_or_else(|| braid_http::types::Version::String("0@server".to_string()));

    // Get messages (optionally since a specific version) - NO LOCK HELD
    let messages = state
        .store
        .get_messages(&room_id, since_version.as_deref())
        .await
        .map_err(|e| {
            error!("Failed to get/messages: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        })?;

    // Build response headers using braid-http
    let mut response_headers = HeaderMap::new();

    response_headers.insert(
        http::header::CONTENT_TYPE,
        "application/json".parse().unwrap(),
    );
    response_headers.insert(
        headers::VERSION.clone(),
        format_version_header(&[current_version.clone()])
            .parse()
            .unwrap(),
    );
    response_headers.insert(
        headers::CURRENT_VERSION.clone(),
        format_version_header(&[current_version]).parse().unwrap(),
    );

    // Add Braid protocol support headers
    response_headers.insert(
        http::HeaderName::from_static("range-request-allow-methods"),
        "PATCH, PUT".parse().unwrap(),
    );
    response_headers.insert(
        http::HeaderName::from_static("range-request-allow-units"),
        "json".parse().unwrap(),
    );

    let snapshot = ChatSnapshot {
        room: room_info,
        messages,
    };

    Ok((response_headers, Json(snapshot)).into_response())
}

/// PUT /chat/:room_id
///
/// Braid protocol endpoint for adding a message.
/// Uses antimatter merge type for CRDT consistency.
pub async fn put_message(
    Path(room_id): Path<String>,
    headers: HeaderMap,
    State(state): State<AppState>,
    Json(input): Json<CreateMessageInput>,
) -> std::result::Result<(HeaderMap, StatusCode), StatusCode> {
    info!("PUT /chat/{}", room_id);

    // Extract sender from header (in real app, use auth)
    let sender = headers
        .get("x-user")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("anonymous")
        .to_string();

    // Convert message type
    let msg_type = match input.message_type {
        MessageTypeInput::Text => MessageType::Text,
        MessageTypeInput::Image { width, height } => MessageType::Image { width, height },
        MessageTypeInput::File { filename, size } => MessageType::File { filename, size },
    };

    // Convert blob refs
    let blob_refs = input
        .blob_refs
        .map(|refs| {
            refs.into_iter()
                .map(|r| BlobRef {
                    hash: r.hash,
                    content_type: r.content_type,
                    filename: r.filename,
                    size: r.size,
                    inline_data: None,
                })
                .collect()
        })
        .unwrap_or_default();

    // Create message using CRDT
    let message = state
        .store
        .add_message(
            &room_id,
            &sender,
            &input.content,
            msg_type,
            input.reply_to,
            blob_refs,
        )
        .await
        .map_err(|e| {
            error!("Failed to create message: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    // Check for AI trigger
    if let Some(ref ai_manager) = state.ai_manager {
        match ai_manager.process_message(&room_id, &message).await {
            Ok(Some(_bot_msg)) => {
                info!("[@BraidBot] Responded in room {}", room_id);
                // Bot response is already added to store
            }
            Ok(None) => {}
            Err(e) => {
                warn!("[@BraidBot] Failed to process message: {}", e);
            }
        }
    }

    // Sync to daemon if available (braidfs-daemon)
    if let Some(ref daemon) = state.daemon {
        if let Err(e) = daemon.sync_room_to_daemon(&room_id).await {
            warn!("Failed to sync room {} to daemon: {}", room_id, e);
        }
    }

    info!(
        "Created message {} in room {} (version {})",
        message.id, room_id, message.version
    );

    // Return updated version headers using braid-http
    let mut response_headers = HeaderMap::new();
    response_headers.insert(
        headers::VERSION.clone(),
        format_version_header(&[braid_http::types::Version::String(message.version.clone())])
            .parse()
            .unwrap(),
    );

    Ok((response_headers, StatusCode::OK))
}

/// GET /chat/:room_id/status
///
/// Get sync status for a room from daemon.
pub async fn get_room_status(
    Path(room_id): Path<String>,
    State(state): State<AppState>,
) -> std::result::Result<Json<RoomSyncStatus>, StatusCode> {
    let status = if let Some(ref daemon) = state.daemon {
        daemon.get_sync_status(&room_id).await
    } else {
        RoomSyncStatus {
            room_id: room_id.clone(),
            status: SyncStatus::Offline,
            last_sync: None,
            pending_changes: 0,
        }
    };

    Ok(Json(status))
}

/// GET /chat/rooms
///
/// List all chat rooms.
pub async fn list_rooms(
    State(state): State<AppState>,
) -> std::result::Result<Json<Vec<ChatRoom>>, StatusCode> {
    let rooms = state.store.list_rooms().await;
    Ok(Json(rooms))
}

/// POST /chat/:room_id/drafts
///
/// Save a draft message (offline support)
pub async fn save_draft(
    Path(room_id): Path<String>,
    State(state): State<AppState>,
    Json(input): Json<CreateMessageInput>,
) -> std::result::Result<StatusCode, StatusCode> {
    let msg_type = match input.message_type {
        MessageTypeInput::Text => MessageType::Text,
        MessageTypeInput::Image { width, height } => MessageType::Image { width, height },
        MessageTypeInput::File { filename, size } => MessageType::File { filename, size },
    };

    state
        .store
        .save_draft(&room_id, &input.content, msg_type)
        .await
        .map_err(|e| {
            error!("Failed to save draft: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(StatusCode::CREATED)
}

/// GET /chat/:room_id/drafts
///
/// Get draft messages for a room
pub async fn get_drafts(
    Path(room_id): Path<String>,
    State(state): State<AppState>,
) -> std::result::Result<Json<Vec<crate::core::models::DraftMessage>>, StatusCode> {
    let drafts = state.store.get_drafts(&room_id).await;
    Ok(Json(drafts))
}

/// DELETE /chat/:room_id/drafts
///
/// Clear drafts for a room (after successful sync)
pub async fn clear_drafts(
    Path(room_id): Path<String>,
    State(state): State<AppState>,
) -> std::result::Result<StatusCode, StatusCode> {
    state.store.clear_drafts(&room_id).await.map_err(|e| {
        error!("Failed to clear drafts: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok(StatusCode::OK)
}
