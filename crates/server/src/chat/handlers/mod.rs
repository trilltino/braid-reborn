//! Chat Handlers and Router
//!
//! Orchestrates messaging, presence, and friend system routes.

use crate::core::AppState;
use axum::{
    routing::{delete, get, post},
    Router,
};

pub mod braid_subscribe;
pub mod chat;
pub mod friends;
pub mod handler_config;
pub mod presence;
pub mod typing;

pub fn router() -> Router<AppState> {
    Router::new()
        // Core Braid protocol endpoints (NO SSE)
        .route("/chat/rooms", get(chat::list_rooms))
        .route(
            "/chat/{room_id}",
            get(chat::get_chat_room).put(chat::put_message),
        )
        // Pure Braid subscription - NO SSE, uses braid-http protocol
        .route(
            "/chat/{room_id}/subscribe",
            get(braid_subscribe::braid_subscribe),
        )
        // Room status and offline support
        .route("/chat/{room_id}/status", get(chat::get_room_status))
        .route(
            "/chat/{room_id}/drafts",
            get(chat::get_drafts)
                .post(chat::save_draft)
                .delete(chat::clear_drafts),
        )
        // Friends system
        .route("/friends", get(friends::list_friends))
        .route(
            "/friends/requests",
            get(friends::list_pending_requests).post(friends::send_friend_request),
        )
        .route(
            "/friends/requests/{request_id}",
            axum::routing::put(friends::respond_friend_request),
        )
        // Chat-specific extensions
        .route(
            "/chat/{room_id}/presence",
            get(presence::get_presence).put(presence::update_presence),
        )
        .route(
            "/chat/{room_id}/typing",
            get(typing::get_typing).put(typing::update_typing),
        )
        // Config (Daemon cookie)
        .route("/config/cookie", post(handler_config::set_daemon_cookie))
}
