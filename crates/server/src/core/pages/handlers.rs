//! Pages Handler
//!
//! Handles GET/PUT for file-based pages using Simpleton merge type.
//! Persists version state and manages Braid subscriptions.

use crate::core::config::AppState;
use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use bytes::Bytes;
use serde::Deserialize;
use std::path::{Component, PathBuf};
use tokio::fs;

pub async fn get_wiki_page(
    Path(path): Path<String>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Response {
    let file = match resolve_path(&state.pages_manager.storage_dir, &path) {
        Ok(p) => p,
        Err(e) => return (StatusCode::BAD_REQUEST, e).into_response(),
    };
    let _guard = braid_http::durable::document_lock(file.clone()).await;
    let store = braid_core::replica::ReplicaStore::new(
        state.pages_manager.storage_dir.join(".braid-replicas"),
    );
    if let Err(e) = import_file(&store, &path, &file).await {
        return braid_core::server::replica_http::error_response(e);
    }
    braid_core::server::replica_http::get(store, path, headers, "simpleton").await
}

pub async fn put_wiki_page(
    Path(path): Path<String>,
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let file = match resolve_path(&state.pages_manager.storage_dir, &path) {
        Ok(p) => p,
        Err(e) => return (StatusCode::BAD_REQUEST, e).into_response(),
    };
    let _guard = braid_http::durable::document_lock(file.clone()).await;
    let store = braid_core::replica::ReplicaStore::new(
        state.pages_manager.storage_dir.join(".braid-replicas"),
    );
    if let Err(e) = import_file(&store, &path, &file).await {
        return braid_core::server::replica_http::error_response(e);
    }
    let response = braid_core::server::replica_http::put(
        store.clone(),
        path.clone(),
        headers,
        body,
        "simpleton",
    )
    .await;
    if response.status().is_success() {
        match store.load(&path).await {
            Ok(Some(doc)) => {
                if let Err(e) =
                    braid_http::durable::atomic_write(&file, doc.content.as_bytes()).await
                {
                    return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
                }
            }
            Ok(None) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
            Err(e) => return braid_core::server::replica_http::error_response(e),
        }
    }
    response
}

async fn import_file(
    store: &braid_core::replica::ReplicaStore,
    resource: &str,
    file: &std::path::Path,
) -> Result<(), braid_core::replica::ReplicaError> {
    if store.load(resource).await?.is_none() {
        let content = match fs::read(file).await {
            Ok(data) => data,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        use sha2::{Digest, Sha256};
        let version =
            braid_http::types::Version::new(format!("legacy-{:x}", Sha256::digest(&content)));
        store
            .commit(
                resource,
                &braid_http::types::Update::snapshot(version, content),
                "simpleton",
            )
            .await?;
    }
    Ok(())
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
    let path = PathBuf::from(path_str);
    if path_str.is_empty()
        || path_str.contains([':', '\\', '\0'])
        || path
            .components()
            .any(|c| !matches!(c, Component::Normal(_)))
    {
        return Err("Invalid relative page path".into());
    }
    let base = if base.is_absolute() {
        base.to_owned()
    } else {
        std::env::current_dir()
            .map_err(|e| e.to_string())?
            .join(base)
    };
    let mut target = base.clone();
    for component in path.components() {
        target.push(component);
        if std::fs::symlink_metadata(&target).is_ok_and(|m| m.file_type().is_symlink()) {
            return Err("Symlink page paths are unsupported".into());
        }
    }
    Ok(target)
}

// ============== LOCAL.ORG HANDLERS ==============

/// GET /local.org/
/// List all pages in local.org
pub async fn list_local_pages(State(state): State<AppState>) -> Response {
    let pages = state.local_org_manager.list_pages().await;
    Json(pages).into_response()
}

/// GET /local.org/{path}
/// Get page content with Braid subscription support
pub async fn get_local_page(
    Path(path): Path<String>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Response {
    let base = state.local_org_manager.storage_dir();
    let file = match resolve_path(&base, &path) {
        Ok(p) => p,
        Err(e) => return (StatusCode::BAD_REQUEST, e).into_response(),
    };
    let _guard = braid_http::durable::document_lock(file.clone()).await;
    let store = braid_core::replica::ReplicaStore::new(base.join(".braid-replicas"));
    if let Err(e) = import_file(&store, &path, &file).await {
        return braid_core::server::replica_http::error_response(e);
    }
    braid_core::server::replica_http::get(store, path, headers, "simpleton").await
}
pub async fn put_local_page(
    Path(path): Path<String>,
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let base = state.local_org_manager.storage_dir();
    let file = match resolve_path(&base, &path) {
        Ok(p) => p,
        Err(e) => return (StatusCode::BAD_REQUEST, e).into_response(),
    };
    let _guard = braid_http::durable::document_lock(file.clone()).await;
    let store = braid_core::replica::ReplicaStore::new(base.join(".braid-replicas"));
    if let Err(e) = import_file(&store, &path, &file).await {
        return braid_core::server::replica_http::error_response(e);
    }
    let response = braid_core::server::replica_http::put(
        store.clone(),
        path.clone(),
        headers,
        body,
        "simpleton",
    )
    .await;
    if response.status().is_success() {
        match store.load(&path).await {
            Ok(Some(doc)) => {
                if let Err(e) =
                    braid_http::durable::atomic_write(&file, doc.content.as_bytes()).await
                {
                    return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
                }
            }
            Err(e) => return braid_core::server::replica_http::error_response(e),
            _ => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        }
    }
    response
}
