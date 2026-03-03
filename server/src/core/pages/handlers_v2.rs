//! Pages Handler V2 - Diamond Types CRDT Support
//!
//! Updated handlers with:
//! - Version graph storage (JSON-based)
//! - Parent validation for causal consistency
//! - Support for multiple merge types (simpleton, diamond)
//! - 409 Conflict response for unknown parents

use crate::core::config::AppState;
use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use braid_core::core::merge::merge_type::{MergePatch, MergeType};
use braid_http::protocol::constants::headers::{PATCHES, VERSION, PARENTS};
use braid_http::types::Version;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::PathBuf;
use tracing::{error, info, warn};

use super::versioned_storage::{VersionedPage, VersionedStorage};

/// Query parameters for GET/PUT requests
#[derive(Debug, Deserialize)]
pub struct PageQuery {
    /// Merge type to use (simpleton or diamond)
    #[serde(rename = "merge-type")]
    pub merge_type: Option<String>,
}

/// GET /v2/pages/{path}
/// Reads page content with version graph support.
/// Supports Braid subscriptions (Subscribe: true) with HTTP 209.
pub async fn get_page_v2(
    Path(path_str): Path<String>,
    Query(query): Query<PageQuery>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Response {
    info!("[GET v2] {} (merge-type: {:?})", path_str, query.merge_type);

    let storage = VersionedStorage::new(state.pages_manager.storage_dir.clone());
    let merge_type = query.merge_type.as_deref().unwrap_or("diamond");

    // Load or create page
    let page = storage.load_or_create(&path_str, merge_type).await;

    // Get current version from heads
    let current_version = page
        .heads
        .first()
        .cloned()
        .unwrap_or_else(|| "ROOT".to_string());

    // Check for subscription
    if headers.get(braid_http::protocol::constants::headers::SUBSCRIBE).is_some() {
        info!("[GET v2] Subscription request for {}", path_str);

        let (mut rx, last_update) = state.pages_manager.subscribe(&path_str).await;

        // Use cached update if available
        let (start_version, start_content) = if let Some(ref update) = last_update {
            info!("[GET v2] Using cached update for {}", path_str);
            let version_str = update.version.first()
                .map(|v| match v {
                    Version::String(s) => s.clone(),
                    Version::Integer(i) => i.to_string(),
                })
                .unwrap_or_else(|| current_version.clone());
            (version_str, update.content.clone().unwrap_or_else(|| page.content.clone()))
        } else {
            (current_version.clone(), page.content.clone())
        };

        // Format initial snapshot
        let initial = format_pages_update(
            &path_str,
            &start_version,
            &[],
            None,
            Some(&start_content),
        );

        let stream = async_stream::stream! {
            yield Ok::<Bytes, std::convert::Infallible>(Bytes::from(initial));

            // Listen for updates
            loop {
                match rx.recv().await {
                    Ok(update) => {
                        let version_str = update.version.first()
                            .map(|v| match v {
                                Version::String(s) => s.clone(),
                                Version::Integer(i) => i.to_string(),
                            })
                            .unwrap_or_default();
                        
                        let parents_str: Vec<String> = update.parents.iter()
                            .map(|v| match v {
                                Version::String(s) => s.clone(),
                                Version::Integer(i) => i.to_string(),
                            })
                            .collect();

                        let msg = format_pages_update(
                            &update.path,
                            &version_str,
                            &parents_str,
                            update.patches.as_ref(),
                            update.content.as_deref(),
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
            .header("Merge-Type", merge_type)
            .header("Cache-Control", "no-cache")
            .header("Connection", "keep-alive")
            .header(VERSION.clone(), format!("\"{}\"", current_version))
            .body(Body::from_stream(stream))
            .unwrap();
    }

    // Regular GET response
    let mut resp_headers = HeaderMap::new();
    resp_headers.insert(
        VERSION.clone(),
        format!("\"{}\"", current_version).parse().unwrap(),
    );
    resp_headers.insert("Merge-Type", merge_type.parse().unwrap());

    (resp_headers, page.content).into_response()
}

/// PUT /v2/pages/{path}
/// Updates page with parent validation and version graph tracking.
/// Returns 409 Conflict if parents are unknown (client needs to sync first).
pub async fn put_page_v2(
    Path(path_str): Path<String>,
    Query(query): Query<PageQuery>,
    State(state): State<AppState>,
    headers: HeaderMap,
    body: String,
) -> Response {
    info!("[PUT v2] {} (merge-type: {:?})", path_str, query.merge_type);

    let storage = VersionedStorage::new(state.pages_manager.storage_dir.clone());
    let merge_type = query.merge_type.as_deref().unwrap_or("diamond");

    // Load existing page or create new
    let mut page = storage.load_or_create(&path_str, merge_type).await;

    // Parse Version header (required)
    let new_version = headers
        .get(&VERSION)
        .and_then(|h| h.to_str().ok())
        .and_then(|s| header_utils::parse_version_list(s).ok())
        .and_then(|v| v.into_iter().next())
        .unwrap_or_else(|| {
            // Generate version if not provided
            let counter = page.version_graph.len() + 1;
            Version::String(format!("server-{}-{}", std::process::id(), counter))
        });

    // Parse Parents header (required for validation)
    let parents = headers
        .get(&PARENTS)
        .and_then(|h| h.to_str().ok())
        .and_then(|s| header_utils::parse_version_list(s).ok())
        .unwrap_or_default();

    info!("[PUT v2] Version: {:?}, Parents: {:?}", new_version, parents);

    // PARENT VALIDATION: Check that all parents exist in version graph
    if let Err(e) = VersionedStorage::validate_parents(&page, &parents) {
        warn!("[PUT v2] Parent validation failed for {}: {}", path_str, e);
        return (
            StatusCode::CONFLICT,
            format!("409 Conflict: {}. Sync required.", e),
        )
            .into_response();
    }

    // Parse Patches header or use body as full replacement
    let patches: Vec<MergePatch> = if let Some(patches_str) = headers.get(&PATCHES).and_then(|h| h.to_str().ok()) {
        match serde_json::from_str(patches_str) {
            Ok(p) => p,
            Err(e) => {
                error!("[PUT v2] Invalid Patches JSON: {}", e);
                return (StatusCode::BAD_REQUEST, "Invalid Patches header").into_response();
            }
        }
    } else {
        // No patches - treat body as full replacement
        vec![MergePatch::new("[0:]", Value::String(body))]
    };

    // Apply patches using merge type
    let mut merge_instance = match merge_type {
        "diamond" => {
            match braid_core::core::merge::MergeTypeRegistry::new().create("diamond", "server") {
                Some(m) => m,
                None => {
                    error!("[PUT v2] Diamond merge type not available");
                    return (StatusCode::INTERNAL_SERVER_ERROR, "Merge type unavailable").into_response();
                }
            }
        }
        _ => {
            match braid_core::core::merge::MergeTypeRegistry::new().create("simpleton", "server") {
                Some(m) => m,
                None => {
                    error!("[PUT v2] Simpleton merge type not available");
                    return (StatusCode::INTERNAL_SERVER_ERROR, "Merge type unavailable").into_response();
                }
            }
        }
    };

    // Initialize with current content
    merge_instance.initialize(&page.content);

    // Apply patches
    let mut rebased_patches = Vec::new();
    for patch in patches {
        let result = merge_instance.apply_patch(patch);
        if !result.success {
            error!("[PUT v2] Merge failed: {:?}", result.error);
            return (
                StatusCode::CONFLICT,
                format!("Merge failed: {:?}", result.error),
            )
                .into_response();
        }
        rebased_patches.extend(result.rebased_patches);
    }

    // Update page
    page.content = merge_instance.get_content();
    
    let version_str = match &new_version {
        Version::String(s) => s.clone(),
        Version::Integer(i) => i.to_string(),
    };
    
    let parent_strings: Vec<String> = parents
        .iter()
        .map(|p| match p {
            Version::String(s) => s.clone(),
            Version::Integer(i) => i.to_string(),
        })
        .collect();

    page.version_graph.insert(version_str.clone(), parent_strings.clone());
    page.heads = vec![version_str.clone()];
    page.modified_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    // Save to storage
    if let Err(e) = storage.save(&path_str, &page).await {
        error!("[PUT v2] Save failed: {}", e);
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }

    // Notify subscribers
    let version_for_notify = vec![new_version.clone()];
    let parents_for_notify: Vec<Version> = parents.clone();
    
    state.pages_manager.notify_update(
        &path_str,
        version_for_notify,
        parents_for_notify,
        Some(rebased_patches),
        Some(page.content.clone()),
    ).await;

    // Response
    let mut resp_headers = HeaderMap::new();
    resp_headers.insert(
        VERSION.clone(),
        format!("\"{}\"", version_str).parse().unwrap(),
    );

    info!("[PUT v2] Success for {} -> version {}", path_str, version_str);
    (resp_headers, StatusCode::OK).into_response()
}

/// GET /v2/pages
/// List all pages with metadata
pub async fn list_pages_v2(State(state): State<AppState>) -> Response {
    let storage = VersionedStorage::new(state.pages_manager.storage_dir.clone());
    let pages = storage.list_pages().await;
    Json(pages).into_response()
}

/// GET /v2/pages/{path}/versions
/// Get version graph for a page (for debugging)
pub async fn get_page_versions_v2(
    Path(path_str): Path<String>,
    State(state): State<AppState>,
) -> Response {
    let storage = VersionedStorage::new(state.pages_manager.storage_dir.clone());
    
    match storage.load(&path_str).await {
        Some(page) => {
            let response = serde_json::json!({
                "path": path_str,
                "heads": page.heads,
                "version_graph": page.version_graph,
                "version_count": page.version_graph.len(),
                "merge_type": page.merge_type,
            });
            Json(response).into_response()
        }
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

/// Format a Braid protocol update message
fn format_pages_update(
    path: &str,
    version: &str,
    parents: &[String],
    patches: Option<&Vec<MergePatch>>,
    content: Option<&str>,
) -> Bytes {
    let patches_json = patches
        .map(|p| serde_json::to_string(p).unwrap_or_default())
        .unwrap_or_else(|| "[]".to_string());
    
    let parents_str = if parents.is_empty() {
        "".to_string()
    } else {
        parents.iter().map(|p| format!("\"{}\"", p)).collect::<Vec<_>>().join(", ")
    };

    let content_str = content.unwrap_or("");

    let msg = format!(
        "Version: \"{}\"\r\n\
         Parents: {}\r\n\
         Patches: {}\r\n\
         Content-Length: {}\r\n\r\n{}\r\n\r\n",
        version,
        parents_str,
        patches_json,
        content_str.len(),
        content_str
    );

    Bytes::from(msg)
}

/// Header utilities (re-exported from braid_http)
mod header_utils {
    use super::*;

    pub fn parse_version_list(s: &str) -> Result<Vec<Version>, String> {
        // Simple parser for version lists: "v1", "v1, v2", or "[v1, v2]"
        let trimmed = s.trim();
        let trimmed = trimmed.trim_matches(|c| c == '[' || c == ']');
        
        if trimmed.is_empty() {
            return Ok(Vec::new());
        }

        let parts: Vec<&str> = trimmed.split(',').collect();
        let versions: Vec<Version> = parts
            .iter()
            .map(|p| {
                let p = p.trim().trim_matches('"').trim_matches('\'');
                if let Ok(i) = p.parse::<i64>() {
                    Version::Integer(i)
                } else {
                    Version::String(p.to_string())
                }
            })
            .collect();

        Ok(versions)
    }
}
