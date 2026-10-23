//! Blob Service Layer
//!
//! Handles binary object storage using the braid-blob crate.
//! Used for chat attachments and wiki media.

use axum::{
    extract::{Path, State, Multipart},
    http::{HeaderMap, StatusCode},
    response::Json,
};
use crate::core::AppState;
use crate::core::models::BlobRef;
use tracing::{info, error};

/// POST /blobs
pub async fn upload_blob(
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> std::result::Result<Json<BlobRef>, StatusCode> {
    use bytes::Bytes;
    use sha2::{Digest, Sha256};

    info!("POST /blobs - uploading blob");

    let mut filename = None;
    let mut content_type = None;
    let mut data = None;

    while let Some(field) = multipart.next_field().await.map_err(|e| {
        error!("Failed to read multipart field: {}", e);
        StatusCode::BAD_REQUEST
    })? {
        let name = field.name().unwrap_or("").to_string();
        
        if name == "file" {
            filename = field.file_name().map(|s| s.to_string());
            content_type = field.content_type().map(|s| s.to_string());
            data = Some(field.bytes().await.map_err(|e| {
                error!("Failed to read file data: {}", e);
                StatusCode::BAD_REQUEST
            })?);
        }
    }

    let data = data.ok_or(StatusCode::BAD_REQUEST)?;
    let filename = filename.unwrap_or_else(|| "unnamed".to_string());
    let content_type = content_type.unwrap_or_else(|| "application/octet-stream".to_string());

    let mut hasher = Sha256::new();
    hasher.update(&data);
    let hash = format!("{:x}", hasher.finalize());

    let version = vec![braid_http::types::Version::from(hash.clone())];
    let parents = vec![];

    let data_len = data.len();
    state.store.blob_store()
        .put(&hash, Bytes::from(data), version, parents, Some(content_type.clone()))
        .await
        .map_err(|e| {
            error!("Failed to store blob: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    info!("Stored blob {} ({} bytes)", hash, data_len);

    Ok(Json(BlobRef {
        hash,
        content_type,
        filename,
        size: data_len as u64,
        inline_data: None,
    }))
}

/// GET /blobs/:hash
pub async fn get_blob(
    Path(hash): Path<String>,
    State(state): State<AppState>,
) -> std::result::Result<(HeaderMap, axum::body::Bytes), StatusCode> {
    info!("GET /blobs/{}", hash);

    let (data, meta) = state.store.blob_store()
        .get(&hash)
        .await
        .map_err(|e| {
            error!("Failed to get blob: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .ok_or(StatusCode::NOT_FOUND)?;

    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        meta.content_type.unwrap_or_else(|| "application/octet-stream".to_string())
            .parse()
            .unwrap(),
    );

    Ok((headers, data))
}
