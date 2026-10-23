use crate::core::{protocol_mod as protocol, BraidError, Result};
use crate::fs::state::DaemonState;
use crate::fs::versions::PendingEdit;
use braid_http::types::{BraidRequest, Patch, Version as BraidVersion};
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use tracing::{error, info};

pub(crate) async fn authenticate(
    mut request: BraidRequest,
    url: &str,
    state: &DaemonState,
) -> BraidRequest {
    if let Ok(url) = url::Url::parse(url) {
        if let Some(domain) = url.domain() {
            if let Some(token) = state.config.read().await.cookies.get(domain) {
                let cookie = if token.contains('=') {
                    token.clone()
                } else if domain == "braid.org" || domain.ends_with(".braid.org") {
                    format!("client={token}")
                } else {
                    format!("token={token}")
                };
                request = request.with_header("Cookie", cookie);
            }
        }
    }
    request
}
pub(crate) fn content_hash(data: &[u8]) -> String {
    format!("{:x}", Sha256::digest(data))
}

/// Journal before sending, replay exactly the same identity after an ambiguous ACK.
pub async fn sync_local_to_remote(
    path: &PathBuf,
    url_in: &str,
    parents: &[BraidVersion],
    original_content: Option<String>,
    new_content: String,
    content_type: Option<String>,
    state: DaemonState,
) -> Result<()> {
    if new_content.len() > 16 * 1024 * 1024 {
        return Err(BraidError::Fs(
            "Local edit exceeds the 16 MiB journal limit".into(),
        ));
    }
    let url = url_in.trim_matches('"').trim().to_owned();
    let active = state.text_sessions.lock().await.get(&url).cloned();
    if let Some(session) = active {
        super::file_service::reconcile(&url, &state, &session, true, Some(&new_content)).await?;
        return wait_for_ack(&session).await;
    }

    let key = if path.is_absolute() {
        path.clone()
    } else {
        std::env::current_dir()?.join(path)
    };
    let _guard = braid_http::durable::document_lock(key).await;
    let mut base = original_content;
    if base.is_none() {
        base = state.content_cache.read().await.get(&url).cloned();
    }
    if base.is_none() {
        base = state
            .version_store
            .read()
            .await
            .get(&url)
            .and_then(|v| v.content.clone());
    }
    let mut frontier = state
        .version_store
        .read()
        .await
        .get(&url)
        .map(|v| v.current_version.clone())
        .unwrap_or_else(|| parents.to_vec());
    loop {
        let pending = state.version_store.read().await.outbox.get(&url).cloned();
        let edit = if let Some(edit) = pending {
            edit
        } else {
            if base.is_none() {
                let req = authenticate(BraidRequest::new(), &url, &state).await;
                let response = state
                    .client
                    .fetch(&url, req)
                    .await
                    .map_err(|e| BraidError::Http(e.to_string()))?;
                if response.status == 404 {
                    base = Some(String::new());
                    frontier.clear();
                } else if (200..300).contains(&response.status) {
                    let remote = std::str::from_utf8(&response.body)
                        .map_err(|e| BraidError::Protocol(e.to_string()))?;
                    if remote != new_content {
                        return Err(BraidError::Protocol("Local file has no acknowledged base; remote differs. Both copies are preserved; explicit conflict resolution is required".into()));
                    }
                    return Ok(());
                } else {
                    return Err(BraidError::Http(format!(
                        "Baseline GET failed: {}",
                        response.status
                    )));
                }
            }
            let base_text = base.clone().unwrap_or_default();
            if base_text == new_content {
                return Ok(());
            }
            let edit = PendingEdit {
                version: BraidVersion::new(format!(
                    "{}-{}",
                    uuid::Uuid::new_v4(),
                    base_text.chars().count() + new_content.chars().count()
                )),
                parents: frontier.clone(),
                base: base_text,
                content: new_content.clone(),
                content_type: content_type.clone().unwrap_or_else(|| "text/plain".into()),
            };
            let mut store = state.version_store.write().await;
            if store.outbox.len() >= 128 {
                return Err(BraidError::Fs(
                    "Durable outbox is full; local file preserved".into(),
                ));
            }
            store.outbox.insert(url.clone(), edit.clone());
            store.save().await?;
            edit
        };
        state.version_store.read().await.save().await?;
        let request = BraidRequest::new()
            .with_method("PUT")
            .with_version(edit.version.clone())
            .with_parents(edit.parents.clone())
            .with_merge_type("simpleton")
            .with_content_type(edit.content_type.clone())
            .with_patches(vec![Patch::text(
                format!("[0:{}]", edit.base.chars().count()),
                edit.content.clone(),
            )]);
        let request = authenticate(request, &url, &state).await;
        let response = state
            .client
            .fetch(&url, request)
            .await
            .map_err(|e| BraidError::Http(e.to_string()))?;
        if !(200..300).contains(&response.status) {
            state
                .failed_syncs
                .write()
                .await
                .insert(url.clone(), (response.status, std::time::Instant::now()));
            return Err(BraidError::Http(format!(
                "PUT failed: {} (edit retained in durable outbox)",
                response.status
            )));
        }
        {
            let mut store = state.version_store.write().await;
            let old = store.file_versions.get(&url).cloned();
            store.update_with_hash(
                &url,
                vec![edit.version.clone()],
                edit.parents.clone(),
                Some(content_hash(edit.content.as_bytes())),
            );
            if let Some(version) = store.file_versions.get_mut(&url) {
                version.content = Some(edit.content.clone());
            }
            store.outbox.remove(&url);
            if let Err(e) = store.save().await {
                store.outbox.insert(url.clone(), edit.clone());
                match old {
                    Some(old) => {
                        store.file_versions.insert(url.clone(), old);
                    }
                    None => {
                        store.file_versions.remove(&url);
                    }
                }
                return Err(e);
            }
        }
        state
            .content_cache
            .write()
            .await
            .insert(url.clone(), edit.content.clone());
        state.active_merges.write().await.remove(&url);
        state.failed_syncs.write().await.remove(&url);
        frontier = vec![edit.version];
        base = Some(edit.content.clone());
        if edit.content == new_content {
            return Ok(());
        }
        // A previous queued edit was delivered first. Journal this newer file as its child.
    }
}

/// Logic for syncing a local binary file to a remote Braid URL.
pub async fn sync_binary_to_remote(
    _path: &std::path::Path,
    url_in: &str,
    parents: &[BraidVersion],
    data: bytes::Bytes,
    content_type: Option<String>,
    state: DaemonState,
) -> Result<()> {
    let url_str = url_in.trim_matches('"').trim().to_string();
    info!("[BraidFS] Syncing binary {} to remote...", url_str);

    // Build native Braid PUT request
    let mut request = BraidRequest::new()
        .with_method("PUT")
        .with_body(data.to_vec());

    // Get parents if not provided
    let mut effective_parents = parents.to_vec();
    if effective_parents.is_empty() {
        let head_req = BraidRequest::new().with_method("GET");
        if let Ok(res) = state.client.fetch(&url_str, head_req).await {
            if let Some(v_header) = res.header("version") {
                if let Ok(versions) = protocol::parse_version_header(v_header) {
                    for v in versions {
                        let normalized = v.to_string().trim_matches('"').to_string();
                        if !normalized.is_empty() {
                            effective_parents.push(braid_http::types::Version::String(normalized));
                        }
                    }
                }
            }
        }
    }

    let new_version = format!("blob-{}", uuid::Uuid::new_v4());
    request = request
        .with_version(BraidVersion::new(new_version))
        .with_parents(effective_parents);

    // Content type
    let ct = content_type.unwrap_or_else(|| "application/octet-stream".to_string());
    request = request.with_content_type(ct);

    // Auth
    let mut final_request = request;
    if let Ok(u) = url::Url::parse(&url_str) {
        if let Some(domain) = u.domain() {
            let cfg = state.config.read().await;
            if let Some(token) = cfg.cookies.get(domain) {
                let cookie_str = if token.contains('=') {
                    token.clone()
                } else if domain.contains("braid.org") {
                    format!("client={}", token)
                } else {
                    format!("token={}", token)
                };
                final_request = final_request.with_header("Cookie", cookie_str);
            }
        }
    }

    // Execute PUT
    match state.client.fetch(&url_str, final_request).await {
        Ok(res) => {
            if (200..300).contains(&res.status) {
                info!(
                    "[BraidFS] Binary sync success (braid) status: {}",
                    res.status
                );
                Ok(())
            } else {
                let err_msg = format!("Binary sync failed: HTTP {}", res.status);
                error!("[BraidFS] {}", err_msg);
                Err(BraidError::Http(err_msg))
            }
        }
        Err(e) => {
            error!("[BraidFS] Binary sync error: {}", e);
            Err(BraidError::Http(format!("Binary sync error: {}", e)))
        }
    }
}

/// Confirmation refers to the durable channel queue, never just a successful GET.
pub(crate) async fn wait_for_ack(session: &crate::simpleton::SimpletonClient) -> Result<()> {
    let mut status = session.status();
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            let current = status.borrow_and_update().clone();
            if let Some(error) = current.error {
                return Err(BraidError::Protocol(error));
            }
            if current.initialized && !current.has_pending_edit && current.outstanding_puts == 0 {
                return Ok(());
            }
            status
                .changed()
                .await
                .map_err(|_| BraidError::SubscriptionClosed)?;
        }
    })
    .await
    .map_err(|_| BraidError::Timeout)?
}
