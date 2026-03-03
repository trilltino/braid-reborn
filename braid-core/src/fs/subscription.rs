use super::PEER_ID;
use crate::core::BraidRequest;
use crate::core::Result;
use crate::fs::mapping;
use crate::fs::state::DaemonState;
use std::collections::HashMap;

pub async fn spawn_subscription(
    url: String,
    subscriptions: &mut HashMap<String, tokio::task::JoinHandle<()>>,
    state: DaemonState,
) {
    tracing::info!("[DEBUG] spawn_subscription called for {}", url);
    
    if subscriptions.contains_key(&url) {
        tracing::info!("[DEBUG] Subscription for {} already exists, skipping", url);
        return;
    }

    let sync_enabled = state
        .config
        .read()
        .await
        .sync
        .get(&url)
        .cloned()
        .unwrap_or(false);
    
    tracing::info!("[DEBUG] Sync enabled for {}: {}", url, sync_enabled);
    
    if !sync_enabled {
        tracing::warn!("[DEBUG] Sync not enabled for {}, skipping subscription", url);
        return;
    }

    let url_capture = url.clone();
    let state_capture = state.clone();
    let handle = tokio::spawn(async move {
        loop {
            match subscribe_loop(url_capture.clone(), state_capture.clone()).await {
                Ok(_) => {
                    tracing::info!(
                        "Subscription for {} ended normally. Reconnecting in 1s...",
                        url_capture
                    );
                }
                Err(e) => {
                    // Stream errors are usually just idle timeouts - not real errors
                    let error_str = format!("{}", e);
                    if error_str.contains("decode") || error_str.contains("timeout") || error_str.contains("closed") {
                        tracing::info!(
                            "Subscription for {} idle timeout (normal). Reconnecting in 1s...",
                            url_capture
                        );
                    } else {
                        tracing::error!(
                            "Subscription error for {}: {}. Reconnecting in 1s...",
                            url_capture,
                            e
                        );
                    }
                }
            }
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    });

    subscriptions.insert(url, handle);
}

pub async fn subscribe_loop(url: String, state: DaemonState) -> Result<()> {
    tracing::info!("[DEBUG] === subscribe_loop START for {}", url);

    // First, fetch the current content via regular GET to ensure we have data
    // This handles servers that return HTTP 200 instead of HTTP 209 subscription stream
    tracing::info!("[DEBUG] Building fetch request for {}", url);
    
    let fetch_req = {
        let mut req = BraidRequest::new()
            .with_header("Accept", "text/plain");
        
        // Add Authentication Headers
        if let Ok(u) = url::Url::parse(&url) {
            if let Some(domain) = u.domain() {
                let cfg = state.config.read().await;
                tracing::info!("[DEBUG] Config read, cookies: {:?}", cfg.cookies.keys().collect::<Vec<_>>());
                if let Some(token) = cfg.cookies.get(domain) {
                    tracing::info!("[DEBUG] Found cookie for domain {}", domain);
                    req = req.with_header("Authorization", format!("Bearer {}", token));
                    let cookie_str = if token.contains('=') {
                        token.clone()
                    } else if domain.contains("braid.org") {
                        format!("client={}", token)
                    } else {
                        format!("token={}", token)
                    };
                    req = req.with_header("Cookie", cookie_str);
                } else {
                    tracing::warn!("[DEBUG] No cookie found for domain {}", domain);
                }
            }
        }
        tracing::info!("[DEBUG] Fetch request built with headers: {:?}", req.extra_headers);
        req
    };
    
    // Try to fetch initial content first
    tracing::info!("[DEBUG] Calling state.client.fetch for {}", url);
    match state.client.fetch(&url, fetch_req).await {
        Ok(response) => {
            tracing::info!("[DEBUG] Fetch returned status {} with {} bytes for {}", 
                response.status, response.body.len(), url);
            
            if (200..300).contains(&response.status) && !response.body.is_empty() {
                let body = String::from_utf8_lossy(&response.body);
                tracing::info!("[DEBUG] Response body preview (first 200 chars): {}", 
                    body.chars().take(200).collect::<String>());
                
                // Process and write the content
                let final_content = if body.trim().starts_with("<!DOCTYPE")
                    || body.trim().starts_with("<html")
                {
                    tracing::info!("[DEBUG] Detected HTML, extracting markdown");
                    mapping::extract_markdown(&body)
                } else {
                    tracing::info!("[DEBUG] Using body as-is (plain text)");
                    body.to_string()
                };
                
                tracing::info!("[DEBUG] Mapping URL to path for {}", url);
                match mapping::url_to_path(&url) {
                    Ok(path) => {
                        // Skip if local server is managing this URL
                        {
                            let managed = state.local_server_managed.read().await;
                            if managed.contains(&url) {
                                tracing::info!("[BraidFS-Sub] Skipping write for {} - managed by local server", url);
                                return Ok(());
                            }
                        }
                        
                        // ALWAYS write server content to file - server is source of truth
                        // Cache check removed: it was preventing braid.org updates from syncing to IDE
                        // when subscription reconnected after timeout
                        
                        tracing::info!("[DEBUG] Path resolved to: {:?}", path);
                        state.pending.add(path.clone());
                        
                        if let Some(parent) = path.parent() {
                            tracing::info!("[DEBUG] Ensuring parent directory: {:?}", parent);
                            ensure_dir_path(parent).await;
                        }
                        
                        let tmp_path = path.with_extension("tmp");
                        tracing::info!("[DEBUG] Writing to tmp file: {:?}", tmp_path);
                        
                        match tokio::fs::write(&tmp_path, &final_content).await {
                            Ok(_) => {
                                tracing::info!("[DEBUG] Tmp file written, renaming to {:?}", path);
                                match tokio::fs::rename(&tmp_path, &path).await {
                                    Ok(_) => {
                                        tracing::info!("[BraidFS-Sub] Wrote initial content for {} ({} bytes)", 
                                            url, final_content.len());
                                        
                                        // Update content cache
                                        let mut cache = state.content_cache.write().await;
                                        cache.insert(url.clone(), final_content);
                                        tracing::info!("[DEBUG] Content cache updated");
                                    }
                                    Err(e) => {
                                        tracing::error!("[DEBUG] Failed to rename tmp file: {}", e);
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::error!("[DEBUG] Failed to write tmp file: {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!("[DEBUG] Failed to map URL to path: {:?}", e);
                    }
                }
            } else {
                tracing::warn!("[DEBUG] Fetch returned empty body or non-2xx status");
            }
        }
        Err(e) => {
            tracing::error!("[DEBUG] Fetch failed with error: {}", e);
        }
    }

    // Now subscribe for real-time updates
    let mut sub_req = BraidRequest::new()
        .subscribe()
        .with_header("Accept", "text/plain")
        .with_header("Heartbeats", "30s");

    // For braid.org wiki pages, request the simpleton merge type
    // (the server will send the content as plain text with simpleton CRDT)
    if url.contains("braid.org") {
        sub_req = sub_req.with_merge_type("simpleton");
    }

    let my_id = PEER_ID.read().await.clone();
    sub_req = sub_req.with_peer(my_id);

    // Add Authentication Headers
    if let Ok(u) = url::Url::parse(&url) {
        if let Some(domain) = u.domain() {
            let cfg = state.config.read().await;
            if let Some(token) = cfg.cookies.get(domain) {
                sub_req = sub_req.with_header("Authorization", format!("Bearer {}", token));
                let cookie_str = if token.contains('=') {
                    token.clone()
                } else if domain.contains("braid.org") {
                    format!("client={}", token)
                } else {
                    format!("token={}", token)
                };
                sub_req = sub_req.with_header("Cookie", cookie_str);
            }
        }
    }

    let mut sub = state.client.subscribe(&url, sub_req).await?;
    let mut is_first = true;
    
    tracing::info!("[BraidFS-Sub] Subscription stream started for {}", url);

    while let Some(update) = sub.next().await {
        let update = update?;

        // Filter echoes (ignore updates from ourselves)
        if let Some(v) = update.primary_version() {
            let v_str = v.to_string();
            let my_id = PEER_ID.read().await;
            if !is_first && v_str.contains(&*my_id) {
                tracing::debug!(
                    "[BraidFS] Ignoring echo from {} (version matches our PEER_ID {})",
                    url,
                    *my_id
                );
                continue;
            }
        }
        is_first = false;

        tracing::debug!("Received update from {}: {:?}", url, update.version);

        // Check if we already have this version (skip write but keep listening)
        {
            let store = state.version_store.read().await;
            if let Some(file_version) = store.file_versions.get(&url) {
                let fetched_version = update.version.clone();
                if file_version.current_version == fetched_version {
                    tracing::info!("[BraidFS-Sub] Version {:?} already current for {}, continuing to listen", 
                        fetched_version, url);
                    continue;  // Keep subscription alive, don't return!
                }
            }
        }

        // Update version store
        {
            state.tracker.mark(&url);
            let mut store = state.version_store.write().await;
            store.update(&url, update.version.clone(), update.parents.clone());
            let _ = store.save().await;
        }

        let patches = match update.patches.as_ref() {
            Some(p) if !p.is_empty() => p,
            _ => {
                // Check if it is a snapshot
                if let Some(body) = update.body_str() {
                    tracing::info!(
                        "[BraidFS-Sub] Snapshot for {}, writing {} bytes",
                        url,
                        body.len()
                    );

                    // Get/Create Merge State (extracting valid content version)
                    let raw_content = {
                        let mut merges = state.active_merges.write().await;
                        let peer_id = PEER_ID.read().await.clone();
                        let requested_merge_type =
                            update.merge_type.as_deref().unwrap_or("simpleton");
                        let merge = merges.entry(url.clone()).or_insert_with(|| {
                            tracing::info!(
                                "[BraidFS] Creating merge state for {} with type: {}",
                                url,
                                requested_merge_type
                            );
                            let mut m = state
                                .merge_registry
                                .create(requested_merge_type, &peer_id)
                                .or_else(|| state.merge_registry.create("simpleton", &peer_id))
                                .expect("Failed to create merge type");
                            m.initialize(body);
                            m
                        });

                        let patch = crate::core::merge::MergePatch {
                            range: "".to_string(),
                            content: serde_json::Value::String(body.to_string()),
                            version: update.primary_version().cloned(),
                            parents: update.parents.clone(),
                        };
                        merge.apply_patch(patch);
                        merge.get_content()
                    };

                    // Filter: Auto-Extract Markdown if HTML shell
                    let final_content = if raw_content.trim().starts_with("<!DOCTYPE")
                        || raw_content.trim().starts_with("<html")
                    {
                        mapping::extract_markdown(&raw_content)
                    } else {
                        raw_content
                    };

                    // Skip if local server is managing this URL
                    {
                        let managed = state.local_server_managed.read().await;
                        if managed.contains(&url) {
                            tracing::info!("[BraidFS-Sub] Skipping snapshot write for {} - managed by local server", url);
                            return Ok(());
                        }
                    }
                    
                    // NOTE: We ALWAYS write server updates to file, even if content matches cache.
                    // The server is the source of truth for LWW. This ensures IDE refreshes.
                    // Cache check removed - was preventing braid.org updates from showing in IDE.
                    
                    // Update Content Cache
                    {
                        let mut cache = state.content_cache.write().await;
                        cache.insert(url.clone(), final_content.clone());
                    }
                    
                    if let Ok(path) = mapping::url_to_path(&url) {
                        // Add to pending BEFORE writing to avoid echo loop
                        state.pending.add(path.clone());

                        if let Some(parent) = path.parent() {
                            ensure_dir_path(parent).await;
                        }

                        // Atomic Write: Write to .tmp file then rename (Snapshot)
                        let tmp_path = path.with_extension("tmp");
                        if let Err(e) = tokio::fs::write(&tmp_path, final_content.clone()).await {
                            tracing::error!("Failed to write tmp file for snapshot {}: {}", url, e);
                        } else {
                            match tokio::fs::rename(&tmp_path, &path).await {
                                Ok(_) => {
                                    // Update Content Cache only on success
                                    let mut cache = state.content_cache.write().await;
                                    cache.insert(url.clone(), final_content.clone());
                                }
                                Err(e) => {
                                    tracing::error!("Failed to rename tmp file for snapshot {}: {} (fallback direct)", url, e);
                                    if let Err(e2) = tokio::fs::write(&path, final_content).await {
                                        tracing::error!(
                                            "Direct snapshot write failed for {}: {}",
                                            url,
                                            e2
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
                continue;
            }
        };

        let path = mapping::url_to_path(&url)?;

        let content = if path.exists() {
            tokio::fs::read_to_string(&path).await.unwrap_or_default()
        } else {
            String::new()
        };

        // Apply Patches via Merge State
        let final_content = {
            let mut merges = state.active_merges.write().await;
            let peer_id = PEER_ID.read().await.clone();
            let requested_merge_type = update.merge_type.as_deref().unwrap_or("simpleton");
            let merge = merges.entry(url.clone()).or_insert_with(|| {
                tracing::info!(
                    "[BraidFS] Creating merge state for {} with type: {}",
                    url,
                    requested_merge_type
                );
                let mut m = state
                    .merge_registry
                    .create(requested_merge_type, &peer_id)
                    .or_else(|| state.merge_registry.create("simpleton", &peer_id))
                    .expect("Failed to create merge type");
                m.initialize(&content);
                m
            });

            for patch in patches {
                let patch_content = std::str::from_utf8(&patch.content).unwrap_or("");

                let merge_patch = crate::core::merge::MergePatch {
                    range: patch.range.clone(),
                    content: serde_json::Value::String(patch_content.to_string()),
                    version: update.primary_version().cloned(),
                    parents: update.parents.clone(),
                };
                let result = merge.apply_patch(merge_patch);
                if !result.success {
                    let error_msg = result.error.unwrap_or_else(|| "Unknown merge error".to_string());
                    tracing::error!("[BraidFS] Merge failed for {}: {}. Triggering re-sync...", url, error_msg);
                    return Err(crate::core::BraidError::Internal(format!("Merge failed: {}", error_msg)));
                }
            }
            merge.get_content()
        };

        if let Ok(path) = mapping::url_to_path(&url) {
            // Add to pending BEFORE writing to avoid echo loop
            state.pending.add(path.clone());

            if let Some(parent) = path.parent() {
                ensure_dir_path(parent).await;
            }

            // Atomic Write: Write to .tmp file then rename
            // This prevents "Access Denied" if the file is open in an editor/viewer
            let tmp_path = path.with_extension("tmp");
            if let Err(e) = tokio::fs::write(&tmp_path, final_content.clone()).await {
                tracing::error!("Failed to write tmp file for {}: {}", url, e);
            } else {
                match tokio::fs::rename(&tmp_path, &path).await {
                    Ok(_) => {
                        // Update Content Cache only on success
                        let mut cache = state.content_cache.write().await;
                        cache.insert(url.clone(), final_content);
                    }
                    Err(e) => {
                        tracing::error!("Failed to rename tmp file for {}: {} (attempting direct write fallback)", url, e);
                        // Fallback: Try direct write if rename fails (e.g. cross-device, though unlikely in simple sync)
                        if let Err(e2) = tokio::fs::write(&path, final_content.clone()).await {
                            tracing::error!("Direct write fallback failed for {}: {}", url, e2);
                        }
                    }
                }
            }
        }

        tracing::debug!("Updated local file {}", url);
    }

    Ok(())
}

async fn ensure_dir_path(path: &std::path::Path) {
    let mut current = std::path::PathBuf::new();
    for component in path.components() {
        use std::path::Component;
        
        // Skip Prefix (e.g., "C:") and RootDir ("\") on Windows
        // These cannot and should not be created as directories
        match component {
            Component::Prefix(_) | Component::RootDir => {
                current.push(component);
                continue;
            }
            _ => {}
        }
        
        current.push(component);
        if current.exists() {
            if current.is_file() {
                let mut new_name = current.clone();
                new_name.set_extension("txt");
                tracing::warn!(
                    "[BraidFS] Path conflict: {:?} is a file but needs to be a directory. Renaming to {:?}",
                    current,
                    new_name
                );
                if let Err(e) = tokio::fs::rename(&current, &new_name).await {
                    tracing::error!("[BraidFS] Failed to resolve path conflict: {}", e);
                    continue;
                }
                // Now create the directory
                if let Err(e) = tokio::fs::create_dir(&current).await {
                    tracing::error!("[BraidFS] Failed to create directory after rename: {}", e);
                }
            }
        } else {
            if let Err(e) = tokio::fs::create_dir(&current).await {
                // It might have been created by another task in the meantime
                if e.kind() != std::io::ErrorKind::AlreadyExists {
                    tracing::error!("[BraidFS] Failed to create directory {:?}: {}", current, e);
                }
            }
        }
    }
}
