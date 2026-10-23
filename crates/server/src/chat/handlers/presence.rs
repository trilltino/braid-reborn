use crate::core::config::AppState;
use crate::core::models::{Presence, PresenceStatus};
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

/// In-memory presence store (could be moved to JsonChatStore later)
static PRESENCE: RwLock<Option<Arc<RwLock<HashMap<String, Presence>>>>> = RwLock::const_new(None);

async fn get_presence_store() -> Arc<RwLock<HashMap<String, Presence>>> {
    let guard = PRESENCE.read().await;
    if let Some(store) = guard.as_ref() {
        return store.clone();
    }
    drop(guard);

    let mut guard = PRESENCE.write().await;
    let store = Arc::new(RwLock::new(HashMap::new()));
    *guard = Some(store.clone());
    store
}

/// GET /chat/:room_id/presence
pub async fn get_presence(
    Path(room_id): Path<String>,
    State(_state): State<AppState>,
) -> std::result::Result<Json<Vec<Presence>>, StatusCode> {
    info!("GET /chat/{}/presence", room_id);

    let store = get_presence_store().await;
    let presence_map = store.read().await;

    // Filter by users who might be in this room
    // In a real implementation, we'd track room membership
    let presence_list: Vec<Presence> = presence_map
        .values()
        .filter(|p| matches!(p.status, PresenceStatus::Online | PresenceStatus::Away))
        .cloned()
        .collect();

    Ok(Json(presence_list))
}

/// PUT /chat/:room_id/presence
pub async fn update_presence(
    Path(room_id): Path<String>,
    State(state): State<AppState>,
    Json(presence): Json<Presence>,
) -> std::result::Result<StatusCode, StatusCode> {
    info!(
        "PUT /chat/{}/presence - {} is {:?}",
        room_id, presence.user, presence.status
    );

    let store = get_presence_store().await;
    let mut presence_map = store.write().await;

    // Create updated presence with new timestamp
    let updated_presence = Presence {
        user: presence.user.clone(),
        status: presence.status,
        last_seen: Utc::now(),
        current_room: presence.current_room.clone(),
    };

    presence_map.insert(presence.user.clone(), updated_presence.clone());

    // Broadcast presence update
    let update = crate::core::store::json_store::RoomUpdate {
        room_id: room_id.clone(),
        update_type: crate::core::store::json_store::UpdateType::Presence,
        data: serde_json::to_value(&updated_presence).unwrap_or_default(),
        crdt_version: None,
    };

    if let Err(e) = state.store.broadcast(&room_id, update).await {
        warn!("Failed to broadcast presence: {}", e);
    }

    Ok(StatusCode::OK)
}
