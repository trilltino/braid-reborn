//! Core Router
//!
//! Handles shared infrastructure routes like Auth and Blobs.

use crate::core::auth::handlers as auth_handlers;
use crate::core::blobs;
use crate::core::AppState;
use axum::{
    routing::{get, post},
    Router,
};

pub fn router() -> Router<AppState> {
    Router::new()
        // Auth routes
        .route("/auth/signup", post(auth_handlers::signup))
        .route("/auth/login", post(auth_handlers::login))
        .route("/auth/logout", post(auth_handlers::logout))
        .route("/auth/me", get(auth_handlers::me))
        .route(
            "/auth/profile/{user_id}",
            axum::routing::put(auth_handlers::update_profile),
        )
        .route("/users", get(auth_handlers::list_users))
        // Blob routes
        .route("/blobs", post(blobs::upload_blob))
        .route("/blobs/{hash}", get(blobs::get_blob))
}
