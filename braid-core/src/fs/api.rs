use super::blob_handlers::{handle_get_blob, handle_put_blob};
use super::mapping;
use super::server_handlers::{handle_get_file, handle_get_file_api, handle_put_file};
use crate::core::server::BraidLayer;
use crate::core::Result;
use crate::fs::state::{Command, DaemonState};
use axum::{
    extract::State,
    routing::{delete, put},
    Json, Router,
};
use serde::Deserialize;
use std::net::SocketAddr;

#[derive(Deserialize)]
pub struct SyncParams {
    url: String,
}

#[derive(Deserialize)]
pub struct PushParams {
    url: String,
    content: String,
    content_type: Option<String>,
}

#[derive(Deserialize)]
pub struct CookieParams {
    pub domain: String,
    pub value: String,
}

#[derive(Deserialize)]
pub struct IdentityParams {
    pub domain: String,
    pub email: String,
}

#[derive(Deserialize)]
pub struct MountParams {
    pub port: Option<u16>,
    pub mount_point: Option<String>,
}

pub async fn run_server(port: u16, state: DaemonState) -> Result<()> {
    let app = Router::new()
        .route("/api/sync", put(handle_sync))
        .route("/api/sync", delete(handle_unsync))
        .route("/api/push", put(handle_push))
        .route("/api/get", axum::routing::get(handle_get_file_api))
        .route("/api/cookie", put(handle_cookie))
        .route("/api/identity", put(handle_identity));

    #[cfg(feature = "nfs")]
    let app = app
        .route("/api/mount", put(handle_mount))
        .route("/api/mount", delete(handle_unmount));

    let app = app
        .route(
            "/.braidfs/config",
            axum::routing::get(handle_braidfs_config),
        )
        .route(
            "/.braidfs/errors",
            axum::routing::get(handle_braidfs_errors),
        )
        .route(
            "/.braidfs/get_version/{fullpath}/{hash}",
            axum::routing::get(handle_get_version),
        )
        .route(
            "/.braidfs/set_version/{fullpath}/{parents}",
            axum::routing::put(handle_set_version),
        )
        .route("/api/blob/{hash}", axum::routing::get(handle_get_blob))
        .route("/api/blob", put(handle_put_blob))
        .route("/{*path}", axum::routing::get(handle_get_file))
        .route("/{*path}", put(handle_put_file))
        .layer(BraidLayer::new().middleware())
        .with_state(state);

    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    tracing::info!("Daemon API listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

async fn handle_sync(
    State(state): State<DaemonState>,
    Json(params): Json<SyncParams>,
) -> Json<serde_json::Value> {
    tracing::info!("IPC Command: Sync {}", params.url);

    if let Err(e) = state
        .tx_cmd
        .send(Command::Sync {
            url: params.url.clone(),
        })
        .await
    {
        tracing::error!("Failed to send sync command: {}", e);
        return Json(serde_json::json!({ "status": "error", "message": "Internal channel error" }));
    }

    Json(serde_json::json!({ "status": "ok", "url": params.url }))
}

async fn handle_unsync(
    State(state): State<DaemonState>,
    Json(params): Json<SyncParams>,
) -> Json<serde_json::Value> {
    tracing::info!("IPC Command: Unsync {}", params.url);

    if let Err(e) = state
        .tx_cmd
        .send(Command::Unsync {
            url: params.url.clone(),
        })
        .await
    {
        tracing::error!("Failed to send unsync command: {}", e);
        return Json(serde_json::json!({ "status": "error", "message": "Internal channel error" }));
    }

    Json(serde_json::json!({ "status": "ok", "url": params.url }))
}

async fn handle_push(
    State(state): State<DaemonState>,
    Json(params): Json<PushParams>,
) -> Json<serde_json::Value> {
    tracing::info!(
        "IPC Command: Push {} ({} bytes)",
        params.url,
        params.content.len()
    );

    // 1. Write content to local file
    let path = match mapping::url_to_path(&params.url) {
        Ok(p) => p,
        Err(e) => {
            tracing::error!("Failed to map URL to path: {}", e);
            return Json(
                serde_json::json!({ "status": "error", "message": format!("Path mapping failed: {}", e) }),
            );
        }
    };

    if let Some(parent) = path.parent() {
        if let Err(e) = tokio::fs::create_dir_all(parent).await {
            tracing::error!("Failed to create parent directory: {}", e);
            return Json(
                serde_json::json!({ "status": "error", "message": format!("Directory creation failed: {}", e) }),
            );
        }
    }

    // 2. Get current version (parents)
    let parents = {
        let store = state.version_store.read().await;
        store
            .get(&params.url)
            .map(|v| v.current_version.clone())
            .unwrap_or_default()
    };

    // 3. Get original content for diff
    let original_content = {
        let cache = state.content_cache.read().await;
        cache.get(&params.url).cloned()
    };

    // 4. Push to remote FIRST (Confirm)
    match crate::fs::sync::sync_local_to_remote(
        &path,
        &params.url,
        &parents,
        original_content,
        params.content.clone(),
        params.content_type,
        state.clone(),
    )
    .await
    {
        Ok(()) => {
            tracing::info!("Successfully pushed {} to remote", params.url);

            // 5. Commit to local disk atomically only after server confirmation
            let temp_folder = path
                .parent()
                .unwrap_or(std::path::Path::new("."))
                .join(".braid_tmp");
            if let Err(e) =
                crate::blob::atomic_write(&path, params.content.as_bytes(), &temp_folder).await
            {
                tracing::error!(
                    "Failed to write file atomically after successful sync: {}",
                    e
                );
                return Json(
                    serde_json::json!({ "status": "error", "message": format!("Server accepted but local atomic write failed: {}", e) }),
                );
            }
            // Add to pending to avoid loop if we had a watcher trigger
            state.pending.add(path.clone());

            Json(serde_json::json!({ "status": "ok", "url": params.url }))
        }
        Err(e) => {
            tracing::error!("Push failed for {}: {}", params.url, e);
            let err_str = e.to_string();
            let status = if err_str.contains("401") || err_str.contains("Unauthorized") {
                "unauthorized"
            } else if err_str.contains("403") || err_str.contains("Forbidden") {
                "forbidden"
            } else {
                "error"
            };

            tracing::error!("Server error detail: {}", e);

            Json(serde_json::json!({
                "status": status,
                "message": format!("Push failed: {}", e),
                "domain": url::Url::parse(&params.url).ok().and_then(|u| u.domain().map(|d| d.to_string()))
            }))
        }
    }
}

async fn handle_cookie(
    State(state): State<DaemonState>,
    Json(params): Json<CookieParams>,
) -> Json<serde_json::Value> {
    tracing::info!("IPC Command: SetCookie {}={}", params.domain, params.value);

    if let Err(e) = state
        .tx_cmd
        .send(Command::SetCookie {
            domain: params.domain.clone(),
            value: params.value.clone(),
        })
        .await
    {
        tracing::error!("Failed to send cookie command: {}", e);
        return Json(serde_json::json!({ "status": "error", "message": "Internal channel error" }));
    }

    Json(serde_json::json!({ "status": "ok", "domain": params.domain }))
}

async fn handle_identity(
    State(state): State<DaemonState>,
    Json(params): Json<IdentityParams>,
) -> Json<serde_json::Value> {
    tracing::info!(
        "IPC Command: SetIdentity {}={}",
        params.domain,
        params.email
    );

    if let Err(e) = state
        .tx_cmd
        .send(Command::SetIdentity {
            domain: params.domain.clone(),
            email: params.email.clone(),
        })
        .await
    {
        tracing::error!("Failed to send identity command: {}", e);
        return Json(serde_json::json!({ "status": "error", "message": "Internal channel error" }));
    }

    Json(serde_json::json!({ "status": "ok", "domain": params.domain }))
}

/// Handle /.braidfs/config - returns the current configuration.
async fn handle_braidfs_config(State(state): State<DaemonState>) -> Json<serde_json::Value> {
    let config = state.config.read().await;
    Json(serde_json::json!({
        "sync": config.sync,
        "cookies": config.cookies,
        "port": config.port,
        "debounce_ms": config.debounce_ms,
        "ignore_patterns": config.ignore_patterns,
    }))
}

/// Error log storage (in-memory for now).
static ERRORS: std::sync::OnceLock<std::sync::Mutex<Vec<String>>> = std::sync::OnceLock::new();

fn get_errors() -> &'static std::sync::Mutex<Vec<String>> {
    ERRORS.get_or_init(|| std::sync::Mutex::new(Vec::new()))
}

/// Log an error to the in-memory error log.
pub fn log_error(text: &str) {
    tracing::error!("LOGGING ERROR: {}", text);
    if let Ok(mut errors) = get_errors().lock() {
        errors.push(format!(
            "{}: {}",
            chrono::Utc::now().format("%Y-%m-%d %H:%M:%S"),
            text
        ));
        // Keep only last 100 errors
        if errors.len() > 100 {
            errors.remove(0);
        }
    }
}

/// Handle /.braidfs/errors - returns the error log.
async fn handle_braidfs_errors() -> String {
    if let Ok(errors) = get_errors().lock() {
        errors.join("\n")
    } else {
        "Error reading error log".to_string()
    }
}

/// Handle /.braidfs/get_version/{fullpath}/{hash} - get version by content hash.
async fn handle_get_version(
    axum::extract::Path((fullpath, hash)): axum::extract::Path<(String, String)>,
    State(state): State<DaemonState>,
) -> Json<serde_json::Value> {
    use percent_encoding::percent_decode_str;
    let fullpath = percent_decode_str(&fullpath)
        .decode_utf8_lossy()
        .to_string();
    let hash = percent_decode_str(&hash).decode_utf8_lossy().to_string();

    tracing::debug!("get_version: {} hash={}", fullpath, hash);

    // Look up version in version store
    let versions = state.version_store.read().await;
    if let Some(version) = versions.get_version_by_hash(&fullpath, &hash) {
        Json(serde_json::json!(version))
    } else {
        Json(serde_json::json!(null))
    }
}

/// Handle /.braidfs/set_version/{fullpath}/{parents} - set version by content hash.
async fn handle_set_version(
    axum::extract::Path((fullpath, parents)): axum::extract::Path<(String, String)>,
    State(state): State<DaemonState>,
    body: String,
) -> Json<serde_json::Value> {
    use percent_encoding::percent_decode_str;
    let fullpath = percent_decode_str(&fullpath)
        .decode_utf8_lossy()
        .to_string();
    let parents_json = percent_decode_str(&parents).decode_utf8_lossy().to_string();

    let parents: Vec<String> = serde_json::from_str(&parents_json).unwrap_or_default();

    tracing::info!("set_version: {} parents={:?}", fullpath, parents);

    match mapping::path_to_url(std::path::Path::new(&fullpath)) {
        Ok(url) => {
            let mut store = state.version_store.write().await;
            let my_id = crate::fs::PEER_ID.read().await.clone();

            // Generate a new version ID
            let version_id = format!(
                "{}-{}",
                my_id,
                uuid::Uuid::new_v4().to_string()[..8].to_string()
            );

            store.update(
                &url,
                vec![crate::core::Version::new(&version_id)],
                parents
                    .into_iter()
                    .map(|p| crate::core::Version::new(&p))
                    .collect(),
            );
            let _ = store.save().await;

            // Also update content cache
            let mut cache = state.content_cache.write().await;
            cache.insert(url, body);

            Json(serde_json::json!({ "status": "ok", "version": version_id }))
        }
        Err(e) => {
            tracing::error!("Failed to map path to URL: {}", e);
            Json(serde_json::json!({ "status": "error", "message": e.to_string() }))
        }
    }
}

/// Check if a file is read-only.
/// Matches JS `is_read_only()` from braidfs/index.js.
#[cfg(unix)]
pub async fn is_read_only(path: &std::path::Path) -> std::io::Result<bool> {
    use std::os::unix::fs::PermissionsExt;
    let metadata = tokio::fs::metadata(path).await?;
    let mode = metadata.permissions().mode();
    // Check if write bit is set for owner
    Ok((mode & 0o200) == 0)
}

#[cfg(windows)]
pub async fn is_read_only(path: &std::path::Path) -> std::io::Result<bool> {
    let metadata = tokio::fs::metadata(path).await?;
    Ok(metadata.permissions().readonly())
}

/// Set a file to read-only or writable.
/// Matches JS `set_read_only()` from braidfs/index.js.
#[cfg(unix)]
pub async fn set_read_only(path: &std::path::Path, read_only: bool) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let metadata = tokio::fs::metadata(path).await?;
    let mut perms = metadata.permissions();
    let mode = perms.mode();

    let new_mode = if read_only {
        mode & !0o222 // Remove write bits
    } else {
        mode | 0o200 // Add owner write bit
    };

    perms.set_mode(new_mode);
    tokio::fs::set_permissions(path, perms).await
}

#[cfg(windows)]
pub async fn set_read_only(path: &std::path::Path, read_only: bool) -> std::io::Result<()> {
    let metadata = tokio::fs::metadata(path).await?;
    let mut perms = metadata.permissions();
    perms.set_readonly(read_only);
    tokio::fs::set_permissions(path, perms).await
}

#[cfg(feature = "nfs")]
async fn handle_mount(
    State(state): State<DaemonState>,
    Json(params): Json<MountParams>,
) -> Json<serde_json::Value> {
    let port = params.port.unwrap_or(2049);
    tracing::info!("IPC Command: Mount on port {}", port);

    if let Err(e) = state
        .tx_cmd
        .send(Command::Mount {
            port,
            mount_point: params.mount_point,
        })
        .await
    {
        tracing::error!("Failed to send mount command: {}", e);
        return Json(serde_json::json!({ "status": "error", "message": "Internal channel error" }));
    }

    Json(serde_json::json!({ "status": "ok", "port": port }))
}

#[cfg(feature = "nfs")]
async fn handle_unmount(State(state): State<DaemonState>) -> Json<serde_json::Value> {
    tracing::info!("IPC Command: Unmount");

    if let Err(e) = state.tx_cmd.send(Command::Unmount).await {
        tracing::error!("Failed to send unmount command: {}", e);
        return Json(serde_json::json!({ "status": "error", "message": "Internal channel error" }));
    }

    Json(serde_json::json!({ "status": "ok" }))
}
#[allow(dead_code)]
async fn handle_push_binary(
    State(state): State<DaemonState>,
    query: axum::extract::Query<SyncParams>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> Json<serde_json::Value> {
    tracing::info!(
        "IPC Command: Push Binary {} ({} bytes)",
        query.url,
        body.len()
    );

    // 1. Map URL to path
    let path = match mapping::url_to_path(&query.url) {
        Ok(p) => p,
        Err(e) => {
            return Json(
                serde_json::json!({ "status": "error", "message": format!("Path mapping failed: {}", e) }),
            );
        }
    };

    if let Some(parent) = path.parent() {
        let _ = tokio::fs::create_dir_all(parent).await;
    }

    // 2. Parents/Version
    let parents = {
        let store = state.version_store.read().await;
        store
            .get(&query.url)
            .map(|v| v.current_version.clone())
            .unwrap_or_default()
    };

    let content_type = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    // 3. Push to remote
    match crate::fs::sync::sync_binary_to_remote(
        &path,
        &query.url,
        &parents,
        body.clone(),
        content_type,
        state.clone(),
    )
    .await
    {
        Ok(()) => {
            // 4. Commit to local disk atomically
            let temp_folder = path
                .parent()
                .unwrap_or(std::path::Path::new("."))
                .join(".braid_tmp");
            if let Err(e) = crate::blob::atomic_write(&path, &body, &temp_folder).await {
                return Json(
                    serde_json::json!({ "status": "error", "message": format!("Server accepted but local atomic write failed: {}", e) }),
                );
            }
            state.pending.add(path);

            Json(serde_json::json!({ "status": "ok", "url": query.url }))
        }
        Err(e) => Json(
            serde_json::json!({ "status": "error", "message": format!("Binary push failed: {}", e) }),
        ),
    }
}
