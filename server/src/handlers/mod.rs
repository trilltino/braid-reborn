//! Handlers for server
//!
//! All handlers use pure Braid protocol (braid-core + braid-http).

pub mod auth;
pub mod braid_subscribe;
pub mod chat;
pub mod friends;
pub mod presence;
pub mod typing;
pub mod config;
// Note: Pages functionality is in core::pages, not here

// Re-export AppState from config
pub use crate::core::config::AppState;

// Auth handlers
pub use auth::{list_users, login, logout, me, signup, update_profile};

// Chat handlers using pure Braid protocol
pub use chat::{
    clear_drafts, get_blob, get_chat_room, get_drafts, get_room_status, put_message, save_draft,
    upload_blob, list_rooms,
};

// Braid-native subscription (NOT SSE)
pub use braid_subscribe::braid_subscribe;

// Friend request handlers
pub use friends::{
    list_friends, list_pending_requests, respond_friend_request, send_friend_request,
};

// Presence handlers
pub use presence::{get_presence, update_presence};

// Typing indicators
pub use typing::{get_typing, update_typing as send_typing};

// Config
pub use config::set_daemon_cookie;
