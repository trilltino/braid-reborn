//! Pages Handler
//!
//! Handles GET/PUT for file-based pages using Simpleton merge type.
//! Persists version state and manages Braid subscriptions.

use crate::core::config::AppState;
use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use braid_core::core::merge::{
    merge_type::{MergePatch, MergeType},
    simpleton::SimpletonMergeType,
    MergeResult,
};
use braid_http::protocol::constants::headers::{PATCHES, VERSION};
use braid_http::protocol::headers as header_utils;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::{Component, PathBuf};
use tokio::fs;
use tracing::{error, info, warn};

#[derive(Debug, Serialize, Deserialize, Default)]
struct PageMeta {
    peer_id: String,
    char_counter: i64,
    version: Vec<braid_http::types::Version>,
}

/// GET /{path}
/// Reads file content and returns with Version header.
/// Also handles Braid subscriptions (Subscribe: true) with 209 Subscription.
/// GET /{path}
/// Reads file content and returns with Version header.
/// Also handles Braid subscriptions (Subscribe: true) with 209 Subscription.
pub async fn get_wiki_page(
    Path(path_str): Path<String>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Response {
    info!("GET Wiki: {}", path_str);

    // 1. Map path to file
    let file_path = match resolve_path(&state.pages_manager.storage_dir, &path_str) {
        Ok(p) => p,
        Err(e) => return (StatusCode::BAD_REQUEST, e).into_response(),
    };

    // 2. Read content & meta
    let content = match fs::read_to_string(&file_path).await {
        Ok(c) => c,
        Err(_) => return StatusCode::NOT_FOUND.into_response(),
    };
    let meta_path = get_meta_path(&file_path);
    let meta = load_meta(&meta_path).await.unwrap_or_default();
    let current_version = meta
        .version
        .first()
        .cloned()
        .unwrap_or_else(|| braid_http::types::Version::String("0@server".to_string()));

    // 3. Check for Subscription
    if let Some(_) = headers.get(braid_http::protocol::constants::headers::SUBSCRIBE) {
        info!("Handling Wiki Subscription for {}", path_str);

        let _path_clone = path_str.clone();
        let (mut rx, last_update) = state.pages_manager.subscribe(&path_str).await;

        // If there's a newer update than what we read from disk, use that
        let (start_version, start_content) = if let Some(ref update) = last_update {
            info!("[Wiki Subscription] Using cached update for {}", path_str);
            (update.version.clone(), update.content.clone().unwrap_or(content.clone()))
        } else {
            (vec![current_version.clone()], content.clone())
        };

        // Initial snapshot
        let initial_update = format_wiki_update(
            &path_str,
            start_version,
            vec![],
            None,
            Some(start_content),
        );

        let stream = async_stream::stream! {
            yield Ok::<Bytes, std::convert::Infallible>(initial_update);

            // If we had a cached update, send it as a patch notification too
            if let Some(update) = last_update {
                yield Ok::<Bytes, std::convert::Infallible>(format_wiki_update(
                    &update.path,
                    update.version,
                    update.parents,
                    update.patches,
                    update.content
                ));
            }

            loop {
                match rx.recv().await {
                    Ok(update) => {
                         yield Ok::<Bytes, std::convert::Infallible>(format_wiki_update(
                             &update.path,
                             update.version,
                             update.parents,
                             update.patches,
                             update.content
                        ));
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        // Handle lag? For now just ignore
                        info!("[Wiki Subscription] Receiver lagged for {}", path_str);
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        info!("[Wiki Subscription] Channel closed for {}", path_str);
                        break;
                    }
                }
            }
        };

        return Response::builder()
            .status(StatusCode::from_u16(209).unwrap())
            .header(axum::http::header::CONTENT_TYPE, "application/json")
            .header(braid_http::protocol::constants::headers::SUBSCRIBE, "true")
            .header("Merge-Type", "simpleton")
            .header("Cache-Control", "no-cache")
            .header("Connection", "keep-alive")
            .header(
                VERSION.clone(),
                braid_http::protocol::headers::format_version_header(&[current_version]),
            )
            .body(Body::from_stream(stream))
            .unwrap();
    }

    // 4. Standard GET Response
    let mut headers = HeaderMap::new();
    headers.insert(
        VERSION.clone(),
        braid_http::protocol::headers::format_version_header(&[current_version])
            .parse()
            .unwrap(),
    );
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        "text/plain".parse().unwrap(),
    );
    headers.insert(
        axum::http::header::CACHE_CONTROL,
        "no-cache".parse().unwrap(),
    );

    (headers, content).into_response()
}

// Braid Wire Protocol Formatter
fn format_wiki_update(
    _path: &str,
    version: Vec<braid_http::types::Version>,
    parents: Vec<braid_http::types::Version>,
    patches: Option<Vec<MergePatch>>,
    content: Option<String>,
) -> bytes::Bytes {
    use std::fmt::Write;
    let mut update = String::new();

    // Version
    if let Some(v) = version.first() {
        let _ = write!(update, "Version: \"{}\"\r\n", v);
    }

    // Parents
    // Always send Parents header, even if empty, to satisfy simpleton-client.js
    if !parents.is_empty() {
        let _ = write!(
            update,
            "Parents: {}\r\n",
            braid_http::protocol::headers::format_version_header(&parents)
        );
    } else {
        // Send empty version list representation
        let _ = write!(update, "Parents: \r\n");
    }

    // Body/Patches
    let body = if let Some(patches) = patches {
        // Send aspatches
        serde_json::to_string(&patches).unwrap_or_default()
    } else if let Some(content) = content {
        // Send value (snapshot)
        // For simpleton client, if value is sent, it treats as "patches: [{range:[0,0], content: value}]" essentially?
        // Actually simpleton-client checks `if (update.patches)` or `update.state = update.body_text`.
        content
    } else {
        String::new()
    };

    let _ = write!(update, "Content-Length: {}\r\n", body.len());
    update.push_str("\r\n");
    update.push_str(&body);
    update.push_str("\r\n\r\n");

    bytes::Bytes::from(update)
}

/// PUT /{path}
/// Applies Simpleton patch to file.
pub async fn put_wiki_page(
    Path(path_str): Path<String>,
    State(state): State<AppState>,
    headers: HeaderMap,
    body: String,
) -> Response {
    info!("PUT Wiki: {}", path_str);

    // 1. Map path
    let file_path = match resolve_path(&state.pages_manager.storage_dir, &path_str) {
        Ok(p) => p,
        Err(e) => return (StatusCode::BAD_REQUEST, e).into_response(),
    };

    // 2. Load current state (or init)
    let meta_path = get_meta_path(&file_path);
    let mut simpleton = match load_meta(&meta_path).await {
        Some(s) => s,
        None => {
            // New file or no meta
            let content = fs::read_to_string(&file_path).await.unwrap_or_default();
            let mut s = SimpletonMergeType::new("server");
            s.initialize(&content);
            s
        }
    };

    // 3. Parse Patch (if provided) or use body as Full Replacement
    // Check if Patches header exists
    let patch_json = headers.get(&PATCHES).and_then(|h| h.to_str().ok());
    let merge_result = if let Some(json_str) = patch_json {
        // Parse Braid patches
        match serde_json::from_str::<Vec<MergePatch>>(json_str) {
            Ok(patches) => {
                let mut result = None;
                for patch in patches {
                    // Apply each patch
                    // Note: Simpleton local_edit/apply_patch expect ONE patch at a time updating state
                    // logic here might need refinement for multiple patches transaction
                    let res = simpleton.apply_patch(patch); // apply_patch for remote? or local_edit?
                                                            // If we are server receiving PUT, we are "applying peer's patch".
                                                            // But if peer sends specific range, we use apply_patch.
                    result = Some(res);
                }
                result
                    .unwrap_or_else(|| braid_core::core::merge::MergeResult::failure("No patches"))
            }
            Err(_) => braid_core::core::merge::MergeResult::failure("Invalid Patches JSON"),
        }
    } else {
        // No Patches header -> Treat body as "everything" replacement (Snapshot)
        let patch = MergePatch::new("everything", Value::String(body));
        simpleton.local_edit(patch)
    };

    if !merge_result.success {
        error!("Merge failed: {:?}", merge_result.error);
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }

    // 4. Trace patch headers (Parents)
    // We should validate parents? For now accept blind.

    // 5. Write to disk
    if let Some(parent) = file_path.parent() {
        let _ = fs::create_dir_all(parent).await;
    }
    if let Err(e) = fs::write(&file_path, &simpleton.content).await {
        error!("Write failed: {}", e);
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }

    // 6. Write meta (Serialize Simpleton state)
    if let Err(e) = save_meta(&meta_path, &simpleton).await {
        error!("Meta write failed: {}", e);
    }

    // 7. Notify Subscribers
    info!("[PUT Wiki] Notifying subscribers for path: {}", path_str);
    
    // Get parent versions (the versions before this update)
    // For now, we use the current simpleton version as parent reference
    let parent_versions = simpleton
        .version
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    
    // For full replacement (no patches header), create a patch that represents the entire change
    let broadcast_patches = if let Some(json_str) = patch_json {
        serde_json::from_str::<Vec<MergePatch>>(json_str).ok()
    } else {
        // Full replacement - send as a patch that replaces everything
        // This ensures clients can apply the update incrementally
        None
    };

    let subscriber_count = state.pages_manager.subscriber_count(&path_str).await;
    info!("[PUT Wiki] Broadcasting to {} subscribers for path: {}", subscriber_count, path_str);

    state
        .pages_manager
        .notify_update(
            &path_str,
            simpleton.version.clone(),
            parent_versions,
            broadcast_patches,
            Some(simpleton.content.clone()),
        )
        .await;
    
    info!("[PUT Wiki] Update broadcast complete for path: {}", path_str);

    // 7. Response
    let new_version = simpleton
        .version
        .first()
        .cloned()
        .unwrap_or_else(|| braid_http::types::Version::String("0@server".to_string()));
    let mut resp_headers = HeaderMap::new();
    resp_headers.insert(
        VERSION.clone(),
        braid_http::protocol::headers::format_version_header(&[new_version])
            .parse()
            .unwrap(),
    );

    (resp_headers, StatusCode::OK).into_response()
}

/// GET /wiki/index
/// Returns list of all wiki pages.
pub async fn list_wiki_pages(State(state): State<AppState>) -> Response {
    let pages = state.pages_manager.list_pages().await;
    Json(pages).into_response()
}

#[derive(Deserialize)]
pub struct SearchQuery {
    pub q: String,
}

/// GET /wiki/search?q=query
/// Searches across all wiki content.
pub async fn search_wiki_pages(
    State(state): State<AppState>,
    Query(query): Query<SearchQuery>,
) -> Response {
    let results = state.pages_manager.search_pages(&query.q).await;
    Json(results).into_response()
}

// Helpers

fn resolve_path(base: &std::path::Path, path_str: &str) -> Result<PathBuf, String> {
    // Simple sanitization
    let path = PathBuf::from(path_str);
    if path.components().any(|c| matches!(c, Component::ParentDir)) {
        return Err("Invalid path".into());
    }
    Ok(base.join(path))
}

fn get_meta_path(path: &std::path::Path) -> PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(".braid-meta");
    path.with_file_name(name)
}

async fn load_meta(path: &std::path::Path) -> Option<SimpletonMergeType> {
    match fs::read_to_string(path).await {
        Ok(json) => serde_json::from_str(&json).ok(),
        Err(_) => None,
    }
}

async fn save_meta(path: &std::path::Path, meta: &SimpletonMergeType) -> std::io::Result<()> {
    let json = serde_json::to_string(meta)?;
    fs::write(path, json).await
}

// ============== LOCAL.ORG HANDLERS ==============

use super::local_org::TextPatch;

/// GET /local.org/
/// List all pages in local.org
pub async fn list_local_pages(State(state): State<AppState>) -> Response {
    let pages = state.local_org_manager.list_pages().await;
    Json(pages).into_response()
}

/// GET /local.org/{path}
/// Get page content with Braid subscription support
pub async fn get_local_page(
    Path(path_str): Path<String>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Response {
    info!("GET Local.org: {}", path_str);

    // Try to get page content
    let (content, version) = match state.local_org_manager.get_page(&path_str).await {
        Ok(c) => c,
        Err(_) => {
            // Page doesn't exist - create it with empty content
            match state.local_org_manager.create_page(&path_str, "").await {
                Ok(v) => (String::new(), v),
                Err(e) => {
                    return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
                }
            }
        }
    };

    // Check for subscription
    if headers
        .get(braid_http::protocol::constants::headers::SUBSCRIBE)
        .is_some()
    {
        info!("Handling Local.org Subscription for {}", path_str);

        let mut rx = state.local_org_manager.subscribe(&path_str).await;

        // Initial snapshot
        let initial = format!(
            "Version: \"{}\"\r\nParents: \r\nContent-Length: {}\r\n\r\n{}\r\n\r\n",
            version,
            content.len(),
            content
        );

        let stream = async_stream::stream! {
            yield Ok::<Bytes, std::convert::Infallible>(Bytes::from(initial));

            loop {
                match rx.recv().await {
                    Ok(update) => {
                        let patches_json = serde_json::to_string(&update.patches).unwrap_or_default();
                        let msg = format!(
                            "Version: \"{}\"\r\nParents: \"{}\"\r\nPatches: {}\r\nContent-Length: 0\r\n\r\n\r\n\r\n",
                            update.version,
                            update.parent_version,
                            patches_json
                        );
                        yield Ok::<Bytes, std::convert::Infallible>(Bytes::from(msg));
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        };

        return Response::builder()
            .status(StatusCode::from_u16(209).unwrap())
            .header(axum::http::header::CONTENT_TYPE, "text/plain")
            .header(braid_http::protocol::constants::headers::SUBSCRIBE, "true")
            .header("Merge-Type", "simpleton")
            .header("Cache-Control", "no-cache")
            .header("Connection", "keep-alive")
            .header(VERSION.clone(), format!("\"{}\"", version))
            .body(Body::from_stream(stream))
            .unwrap();
    }

    // Standard GET response
    let mut resp_headers = HeaderMap::new();
    resp_headers.insert(VERSION.clone(), format!("\"{}\"", version).parse().unwrap());
    resp_headers.insert(
        axum::http::header::CONTENT_TYPE,
        "text/plain".parse().unwrap(),
    );

    (resp_headers, content).into_response()
}

/// PUT /local.org/{path}
/// Apply patches to page content
pub async fn put_local_page(
    Path(path_str): Path<String>,
    State(state): State<AppState>,
    headers: HeaderMap,
    body: String,
) -> Response {
    info!("PUT Local.org: {}", path_str);

    // Parse patches from body or Patches header
    let patches: Vec<TextPatch> = if let Some(patches_header) = headers.get(&PATCHES) {
        match patches_header
            .to_str()
            .ok()
            .and_then(|s| serde_json::from_str(s).ok())
        {
            Some(p) => p,
            None => return (StatusCode::BAD_REQUEST, "Invalid Patches header").into_response(),
        }
    } else {
        // Body is raw patches JSON
        match serde_json::from_str(&body) {
            Ok(p) => p,
            Err(_) => {
                // Treat as full content replacement
                vec![TextPatch {
                    range: (0, usize::MAX), // Replace all
                    content: body,
                }]
            }
        }
    };

    // Get parent version from header
    let parent_version = headers
        .get("Parents")
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.trim_matches('"').parse::<u64>().ok())
        .unwrap_or(0);

    // Apply patches
    match state
        .local_org_manager
        .apply_patches(&path_str, patches, parent_version)
        .await
    {
        Ok(new_version) => {
            let mut resp_headers = HeaderMap::new();
            resp_headers.insert(
                VERSION.clone(),
                format!("\"{}\"", new_version).parse().unwrap(),
            );
            (resp_headers, StatusCode::OK).into_response()
        }
        Err(e) => {
            error!("PUT Local.org failed: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}
