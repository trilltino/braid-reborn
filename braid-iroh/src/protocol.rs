//! Braid protocol handler for iroh connections.
//!
//! Wraps an Axum router in `IrohAxum` so that Braid-HTTP routes (GET, PUT
//! with Version/Parents headers) are served over HTTP/3 on iroh QUIC
//! connections. This is the bridge between the P2P transport and the
//! existing braid_http_rs server middleware.

use axum::{
    extract::{Query, State},
    response::IntoResponse,
    routing::{get, put},
    Router,
};
use braid_http_rs::{Update, Version};
use bytes::Bytes;
use http::StatusCode;
use iroh_h3_axum::IrohAxum;
use std::collections::HashMap;
use std::sync::Arc;

use crate::subscription::SubscriptionManager;

/// Shared state accessible from Axum route handlers.
#[derive(Clone)]
pub struct BraidAppState {
    /// Subscription manager for gossip-backed pub/sub.
    pub subscriptions: Arc<SubscriptionManager>,
    /// In-memory resource store: URL → List of Updates (History).
    pub resources: Arc<tokio::sync::RwLock<std::collections::HashMap<String, Vec<Update>>>>,
}

/// Build the Axum router with Braid-HTTP routes, then wrap it in
/// `IrohAxum` so it can be mounted on an iroh endpoint.
///
/// Routes:
/// - `GET /:resource`  → returns the latest snapshot
/// - `PUT /:resource`  → accepts a new update, broadcasts via gossip
pub fn build_protocol_handler(state: BraidAppState) -> IrohAxum {
    let router = Router::new()
        .route("/{resource}", get(handle_get))
        .route("/{resource}", put(handle_put))
        .with_state(state);

    IrohAxum::new(router)
}

use http::HeaderMap;

/// GET handler — returns the current state of a resource.
/// If the resource doesn't exist yet, returns 404.
async fn handle_get(
    State(state): State<BraidAppState>,
    axum::extract::Path(resource): axum::extract::Path<String>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let url = format!("/{}", resource);

    // Check for Subscribe header (Subscription 209 Demo)
    if let Some(val) = headers.get("subscribe") {
        if val == "true" {
            println!("[PROTOCOL] Received Subscribe request for {}", url);

            // Try to get current content to send as initial state (latest version)
            let resources = state.resources.read().await;
            println!("[PROTOCOL] Looking for resource in store, available keys: {:?}", resources.keys().collect::<Vec<_>>());
            let initial_content = if let Some(history) = resources.get(&url) {
                println!("[PROTOCOL] Found history with {} entries for {}", history.len(), url);
                if let Some(latest) = history.last() {
                    let content = serde_json::to_string(latest).unwrap_or_default();
                    println!("[PROTOCOL] Sending initial content (len: {})", content.len());
                    content
                } else {
                    println!("[PROTOCOL] History empty for {}", url);
                    String::new()
                }
            } else {
                // Empty initial state
                println!("[PROTOCOL] Resource not found: {}", url);
                String::new()
            };

            return (
                StatusCode::from_u16(209).unwrap(),
                [("subscribe", "true")],
                initial_content,
            )
                .into_response();
        }
    }

    let resources = state.resources.read().await;

    if let Some(history) = resources.get(&url) {
        // 1. Check for ?version=...
        if let Some(ver) = params.get("version") {
            // Basic implementation: check if version string contains the requested ID
            // Optimally we'd use strict equality but Version enum makes it tricky strictly
            if let Some(update) = history
                .iter()
                .find(|u| u.version.iter().any(|v| v.to_string() == *ver))
            {
                return (
                    StatusCode::OK,
                    serde_json::to_string(update).unwrap_or_default(),
                )
                    .into_response();
            }
            return StatusCode::NOT_FOUND.into_response();
        }

        // 2. Check for ?history=true
        if params.get("history").map(|v| v == "true").unwrap_or(false) {
            // Return list of version strings
            let versions: Vec<String> = history
                .iter()
                .map(|u| {
                    u.version
                        .iter()
                        .map(|v| v.to_string())
                        .collect::<Vec<_>>()
                        .join(",")
                })
                .collect();
            return (
                StatusCode::OK,
                serde_json::to_string(&versions).unwrap_or_default(),
            )
                .into_response();
        }

        // 3. Default: Return latest
        if let Some(latest) = history.last() {
            return (
                StatusCode::OK,
                serde_json::to_string(latest).unwrap_or_default(),
            )
                .into_response();
        }
    }

    StatusCode::NOT_FOUND.into_response()
}

/// PUT handler — stores a new update and broadcasts it via gossip.
async fn handle_put(
    State(state): State<BraidAppState>,
    axum::extract::Path(resource): axum::extract::Path<String>,
    headers: HeaderMap,
    body: String,
) -> impl IntoResponse {
    let url = format!("/{}", resource);

    // Debug logging for Braid format
    println!("\nINCOMING BRAID PUT:");
    println!("PUT {} HTTP/3", url);
    for (name, value) in &headers {
        if name.as_str().starts_with("version")
            || name.as_str().starts_with("parents")
            || name.as_str().starts_with("merge-type")
            || name.as_str().starts_with("content-type")
            || name.as_str().starts_with("content-range")
            || name.as_str().starts_with("braid-")
        {
            println!("{}: {}", name, value.to_str().unwrap_or("???"));
        }
    }
    println!();
    println!("{}", body);
    println!("----------------------------------------\n");

    // Parse the incoming update from headers
    let version = headers
        .get("version")
        .and_then(|v| v.to_str().ok())
        .map(|v| Version::String(v.to_string()))
        .unwrap_or_else(|| Version::String(uuid::Uuid::new_v4().to_string()));
    let update = Update::snapshot(version, Bytes::from(body.clone().into_bytes()));

    // Store locally
    // Store locally (append to history)
    let mut resources = state.resources.write().await;
    resources
        .entry(url.clone())
        .or_insert_with(Vec::new)
        .push(update.clone());

    // Broadcast to gossip subscribers
    if let Err(e) = state.subscriptions.broadcast(&url, &update).await {
        tracing::warn!("gossip broadcast failed for {}: {}", url, e);
    }

    StatusCode::OK
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_braid_app_state_clone() {
        // This test verifies BraidAppState can be cloned (required for Axum)
        // We can't easily create a real SubscriptionManager without a gossip instance,
        // but we can verify the derive macro works
        fn assert_clone<T: Clone>() {}
        assert_clone::<BraidAppState>();
    }

    #[test]
    fn test_build_protocol_handler() {
        // Just verify the function signature compiles
        // Real testing requires a gossip instance
        fn assert_handler_fn<F>()
        where
            F: Fn(BraidAppState) -> IrohAxum,
        {
        }
        assert_handler_fn::<fn(BraidAppState) -> IrohAxum>();
    }
}
