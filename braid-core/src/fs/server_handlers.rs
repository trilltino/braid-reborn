use super::mapping;
use crate::core::server::{BraidState, BraidUpdate};
use crate::core::Update;
use crate::fs::state::DaemonState;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension,
};
use serde::Deserialize;
use std::sync::Arc;

#[derive(Deserialize)]
pub struct GetParams {
    pub url: String,
}

pub async fn handle_get_file(
    Path(path): Path<String>,
    State(state): State<DaemonState>,
    Extension(braid_state): Extension<Arc<BraidState>>,
) -> Response {
    tracing::info!("GET /{} (subscribe={})", path, braid_state.subscribe);

    // If it's a Braid URL, we allow it for on-demand sync
    let is_braid_url = path.starts_with("http://") || path.starts_with("https://");

    if !is_braid_url && path != ".braidfs/config" && path != ".braidfs/errors" {
        tracing::debug!("Path not found: {}", path);
        return (
            StatusCode::NOT_FOUND,
            axum::response::Html(
                r#"Nothing to see here. Use a Braid URL or check <a href=".braidfs/config">.braidfs/config</a>"#
            )
        ).into_response();
    }

    // Map URL path to filesystem path
    let file_path = match mapping::url_to_path(&path) {
        Ok(p) => p,
        Err(e) => {
            tracing::error!("Path mapping error: {:?}", e);
            return (StatusCode::BAD_REQUEST, format!("Invalid path: {}", e)).into_response();
        }
    };

    // 4. Read file content (or fetch if missing and it's a URL)
    let content = match tokio::fs::read_to_string(&file_path).await {
        Ok(c) => c,
        Err(_) => {
            if is_braid_url {
                tracing::info!("[On-Demand] Fetching {} from remote...", path);
                let fetch_req = crate::core::BraidRequest::new().with_method("GET");
                match state.client.fetch(&path, fetch_req).await {
                    Ok(res) if (200..300).contains(&res.status) => {
                        let body = String::from_utf8_lossy(&res.body).to_string();
                        // Lazy persist
                        if let Some(p) = file_path.parent() {
                            let _ = tokio::fs::create_dir_all(p).await;
                        }
                        let _ = tokio::fs::write(&file_path, &body).await;

                        // Initialize merge state if not exists
                        let peer_id = crate::fs::PEER_ID.read().await.clone();
                        let mut merges = state.active_merges.write().await;
                        let _merge = merges.entry(path.clone()).or_insert_with(|| {
                            let mut m = state
                                .merge_registry
                                .create("simpleton", &peer_id)
                                .expect("Failed to create simpleton merge");
                            m.initialize(&body);
                            m
                        });

                        body
                    }
                    _ => {
                        return (StatusCode::NOT_FOUND, "Resource not found on remote")
                            .into_response();
                    }
                }
            } else {
                if let Some(parent) = file_path.parent() {
                    let _ = tokio::fs::create_dir_all(parent).await;
                }
                let empty_content = if path == ".braidfs/config" {
                    r#"{"sync":{},"cookies":{},"port":45678}"#
                } else {
                    ""
                };
                let _ = tokio::fs::write(&file_path, empty_content).await;
                empty_content.to_string()
            }
        }
    };

    // Get current version
    let version = {
        let store = state.version_store.read().await;
        store
            .get(&path)
            .map(|v| v.current_version.clone())
            .unwrap_or_else(|| vec![braid_http::types::Version::String("initial".to_string())])
    };

    // Send snapshot response
    let update = Update::snapshot(version[0].clone(), content.clone());
    BraidUpdate(update).into_response()
}

pub async fn handle_get_file_api(
    State(state): State<DaemonState>,
    Extension(braid_state): Extension<Arc<BraidState>>,
    axum::extract::Query(params): axum::extract::Query<GetParams>,
) -> Response {
    handle_get_file(Path(params.url), State(state), Extension(braid_state)).await
}

pub async fn handle_put_file(
    Path(path): Path<String>,
    State(state): State<DaemonState>,
    Extension(braid_state): Extension<Arc<BraidState>>,
    _headers: axum::http::HeaderMap,
    body: String,
) -> Response {
    tracing::info!("PUT /{}", path);

    // allow PUT to urls and .braidfs/config/errors
    let is_braid_url = path.starts_with("http://") || path.starts_with("https://");

    if !is_braid_url && path != ".braidfs/config" && path != ".braidfs/errors" {
        tracing::warn!("PUT not allowed for path: {}", path);
        return (StatusCode::NOT_FOUND, "Not found").into_response();
    }

    // Map URL path to filesystem path
    let file_path = match mapping::url_to_path(&path) {
        Ok(p) => p,
        Err(e) => {
            tracing::error!("Path mapping error: {:?}", e);
            return (StatusCode::BAD_REQUEST, format!("Invalid path: {}", e)).into_response();
        }
    };

    // Write content to file
    if let Some(parent) = file_path.parent() {
        if let Err(e) = tokio::fs::create_dir_all(parent).await {
            tracing::error!("Failed to create directory: {:?}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Directory creation failed",
            )
                .into_response();
        }
    }

    if let Err(e) = tokio::fs::write(&file_path, &body).await {
        tracing::error!("Failed to write file: {:?}", e);
        return (StatusCode::INTERNAL_SERVER_ERROR, "File write failed").into_response();
    }

    // Update content cache
    {
        let mut cache = state.content_cache.write().await;
        cache.insert(path.clone().to_string(), body);
    }

    // Update version store if version was provided
    if let Some(version) = &braid_state.version {
        let mut store = state.version_store.write().await;
        store.update(
            &path,
            version.clone(),
            braid_state.parents.clone().unwrap_or_default(),
        );
        let _ = store.save().await;
    }

    tracing::info!("File written: {}", path);
    (StatusCode::OK, "File updated").into_response()
}
