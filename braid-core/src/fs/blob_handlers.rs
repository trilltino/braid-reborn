use crate::fs::state::DaemonState;
use axum::{extract::State, response::IntoResponse, Json};

pub async fn handle_get_blob(
    axum::extract::Path(hash): axum::extract::Path<String>,
    State(state): State<DaemonState>,
) -> impl IntoResponse {
    let key = format!("blob:{}", hash);
    if let Ok(Some((bytes, meta))) = state.binary_sync.blob_store().get(&key).await {
        let mut headers = axum::http::HeaderMap::new();
        if let Some(ct) = meta.content_type {
            headers.insert(
                axum::http::header::CONTENT_TYPE,
                ct.parse().unwrap_or(axum::http::HeaderValue::from_static(
                    "application/octet-stream",
                )),
            );
        }
        (headers, bytes).into_response()
    } else {
        axum::http::StatusCode::NOT_FOUND.into_response()
    }
}

pub async fn handle_put_blob(
    State(state): State<DaemonState>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> Json<serde_json::Value> {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(&body);
    let hash = format!("{:x}", hasher.finalize());
    let key = format!("blob:{}", hash);

    // Minimal version - just store as "local-upload"
    let uuid_str = uuid::Uuid::new_v4().to_string();
    let version = vec![crate::core::Version::new(&format!(
        "local-{}",
        &uuid_str[..8]
    ))];

    let content_type = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    match state
        .binary_sync
        .blob_store()
        .put(
            &key,
            bytes::Bytes::from(body.to_vec()),
            version,
            vec![],
            content_type,
        )
        .await
    {
        Ok(_) => Json(serde_json::json!({ "status": "ok", "hash": hash })),
        Err(e) => {
            tracing::error!("Blob put failed: {}", e);
            Json(serde_json::json!({ "status": "error", "message": e.to_string() }))
        }
    }
}
