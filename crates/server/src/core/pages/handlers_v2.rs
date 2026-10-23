//! Durable versioned pages served by the shared Braid replica adapter.
use super::versioned_storage::VersionedStorage;
use crate::core::config::AppState;
use axum::{
    extract::{Path, Query, State},
    http::HeaderMap,
    response::{IntoResponse, Response},
    Json,
};
use braid_core::{replica::ReplicaStore, server::replica_http};
use bytes::Bytes;
use serde::Deserialize;
#[derive(Debug, Deserialize)]
pub struct PageQuery {
    #[serde(rename = "merge-type")]
    pub merge_type: Option<String>,
}
pub async fn get_page_v2(
    Path(path): Path<String>,
    Query(query): Query<PageQuery>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Response {
    if let Some(resource) = path.strip_suffix("/versions") {
        return get_page_versions_v2(Path(resource.to_owned()), State(state)).await;
    }
    replica_http::get(
        ReplicaStore::new(state.pages_manager.storage_dir.clone()),
        path,
        headers,
        query.merge_type.as_deref().unwrap_or("simpleton"),
    )
    .await
}
pub async fn put_page_v2(
    Path(path): Path<String>,
    Query(query): Query<PageQuery>,
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    replica_http::put(
        ReplicaStore::new(state.pages_manager.storage_dir.clone()),
        path,
        headers,
        body,
        query.merge_type.as_deref().unwrap_or("simpleton"),
    )
    .await
}
pub async fn list_pages_v2(State(state): State<AppState>) -> Response {
    Json(
        VersionedStorage::new(state.pages_manager.storage_dir.clone())
            .list_pages()
            .await,
    )
    .into_response()
}
pub async fn get_page_versions_v2(
    Path(path): Path<String>,
    State(state): State<AppState>,
) -> Response {
    match ReplicaStore::new(state.pages_manager.storage_dir.clone()).load(&path).await {
        Ok(Some(page))=>Json(serde_json::json!({"path":path,"heads":page.heads,"version_graph":page.version_graph,"version_count":page.version_graph.len(),"merge_type":page.merge_type})).into_response(),
        Ok(None)=>axum::http::StatusCode::NOT_FOUND.into_response(),
        Err(e)=>replica_http::error_response(e),
    }
}
