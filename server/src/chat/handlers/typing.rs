use crate::core::config::AppState;
use crate::core::models::TypingIndicator;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use chrono::Utc;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn};

/// In-memory typing indicators store
static TYPING: RwLock<Option<Arc<RwLock<HashMap<String, TypingIndicator>>>>> =
    RwLock::const_new(None);

async fn get_typing_store() -> Arc<RwLock<HashMap<String, TypingIndicator>>> {
    let guard = TYPING.read().await;
    if let Some(store) = guard.as_ref() {
        return store.clone();
    }
    drop(guard);

    let mut guard = TYPING.write().await;
    let store = Arc::new(RwLock::new(HashMap::new()));
    *guard = Some(store.clone());
    store
}

/// GET /chat/:room_id/typing
pub async fn get_typing(
    Path(room_id): Path<String>,
    State(_state): State<AppState>,
) -> std::result::Result<Json<Vec<TypingIndicator>>, StatusCode> {
    info!("GET /chat/{}/typing", room_id);

    let store = get_typing_store().await;
    let typing_map = store.read().await;

    // Filter by room and recent activity (within last 5 seconds)
    let now = Utc::now();
    let typing_list: Vec<TypingIndicator> = typing_map
        .values()
        .filter(|t| t.room_id == room_id && t.is_typing && (now - t.timestamp).num_seconds() < 5)
        .cloned()
        .collect();

    Ok(Json(typing_list))
}

/// PUT /chat/:room_id/typing
pub async fn update_typing(
    Path(room_id): Path<String>,
    State(state): State<AppState>,
    Json(typing): Json<TypingIndicator>,
) -> std::result::Result<StatusCode, StatusCode> {
    info!(
        "PUT /chat/{}/typing - {} is_typing={}",
        room_id, typing.user, typing.is_typing
    );

    let store = get_typing_store().await;
    let mut typing_map = store.write().await;

    let indicator = TypingIndicator {
        room_id: room_id.clone(),
        timestamp: Utc::now(),
        ..typing
    };

    typing_map.insert(format!("{}:{}", room_id, indicator.user), indicator.clone());

    // Broadcast typing update
    let update = crate::core::store::json_store::RoomUpdate {
        room_id: room_id.clone(),
        update_type: crate::core::store::json_store::UpdateType::Typing,
        data: serde_json::to_value(&indicator).unwrap_or_default(),
        crdt_version: None,
    };

    if let Err(e) = state.store.broadcast(&room_id, update).await {
        warn!("Failed to broadcast typing: {}", e);
    }

    Ok(StatusCode::OK)
}
