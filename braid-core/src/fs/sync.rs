use crate::core::{protocol_mod as protocol, BraidError, Result};
use crate::fs::state::DaemonState;
use crate::fs::PEER_ID;
use braid_http::types::{BraidRequest, Version as BraidVersion, Patch};
use std::path::PathBuf;
use tracing::{error, info};

/// Logic for syncing a local file to a remote Braid URL.
pub async fn sync_local_to_remote(
    _path: &PathBuf,
    url_in: &str,
    parents: &[BraidVersion],
    _original_content: Option<String>,
    new_content: String,
    content_type: Option<String>,
    state: DaemonState,
) -> Result<()> {
    let url_str = url_in.trim_matches('"').trim().to_string();
    info!("[BraidFS] Syncing {} to remote...", url_str);

    // All URLs now use native Braid HTTP client (removed curl workaround)
    // The native client handles Windows properly without PowerShell quote issues

    // 2. Standard Braid Protocol Path
    let mut request = BraidRequest::new().with_method("PUT");
    let mut effective_parents: Vec<BraidVersion> = parents.to_vec();

    // Fetch server content first to check for conflicts (LWW check)
    let mut server_content: Option<String> = None;
    let mut server_version: Option<BraidVersion> = None;
    
    if effective_parents.is_empty() {
        let mut head_req = BraidRequest::new()
            .with_method("GET")
            .with_header("Accept", "text/plain");

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
                    head_req = head_req.with_header("Cookie", cookie_str);
                }
            }
        }

        if let Ok(res) = state.client.fetch(&url_str, head_req).await {
            // Check server content vs local (LWW: if server differs, accept server)
            if !res.body.is_empty() {
                let server_body = String::from_utf8_lossy(&res.body).to_string();
                if server_body != new_content {
                    info!("[BraidFS-Sync] Server content differs from local - accepting server (LWW)");
                    server_content = Some(server_body);
                }
            }
            
            if let Some(v_header) = res
                .headers
                .get("version")
                .or(res.headers.get("current-version"))
            {
                if let Ok(versions) = protocol::parse_version_header(v_header) {
                    for v in versions {
                        if !v.to_string().trim_matches('"').is_empty() {
                            effective_parents.push(v.clone());
                            server_version = Some(v);
                        }
                    }
                }
            }
        }
    }
    
    // If server has different content, update local file instead of pushing
    if let Some(server_body) = server_content {
        info!("[BraidFS-Sync] Updating local file with server content (server is newer/different)");
        
        // Update content cache
        {
            let mut cache = state.content_cache.write().await;
            cache.insert(url_str.clone(), server_body.clone());
        }
        
        // Update version store
        if let Some(ref sv) = server_version {
            let mut store = state.version_store.write().await;
            store.update(&url_str, vec![sv.clone()], vec![]);
            let _ = store.save().await;
        }
        
        // Write to file
        if let Ok(path) = crate::fs::mapping::url_to_path(&url_str) {
            state.pending.add(path.clone());
            let tmp_path = path.with_extension("tmp");
            if tokio::fs::write(&tmp_path, &server_body).await.is_ok() {
                let _ = tokio::fs::rename(&tmp_path, &path).await;
                info!("[BraidFS-Sync] Local file updated with server content");
            }
        }
        
        return Ok(()); // Don't push - we accepted server content
    }

    // Generate a new Version ID for this edit
    // We utilize the SimpletonMergeType for all updates to ensure spec compliance (diffs + correct versioning)
    let (new_version_id, patches, my_id) = {
        // 1. Get cached content to allow hydration of merge state
        let cached_content = {
            let cache = state.content_cache.read().await;
            cache.get(&url_str).cloned()
        };

        let mut merges = state.active_merges.write().await;
        let peer_id = {
            let config = state.config.read().await;
            let mut id = config.peer_id.clone();
            
            // Use authenticated identity if available for this domain
            if let Ok(u) = url::Url::parse(&url_str) {
                if let Some(domain) = u.domain() {
                    if let Some(email) = config.identities.get(domain) {
                        id = email.clone();
                        // Also try to extract username if it's an email
                        if let Some((user, _)) = id.split_once('@') {
                             id = user.to_string();
                        }
                    }
                }
            }
            id
        };
        let my_id = peer_id.clone();
        
        // 2. Get or create merge type
        let merge = merges.entry(url_str.clone()).or_insert_with(|| {
            info!("[BraidFS-Sync] Initializing Simpleton merge with peer_id: {}", peer_id);
            let mut m = state
                .merge_registry
                .create("simpleton", &peer_id)
                .expect("Failed to create simpleton merge type");
            // Initialize with cached content if available, otherwise empty
            m.initialize(cached_content.as_deref().unwrap_or(""));
            m
        });

        if merge.get_content().is_empty() && cached_content.as_ref().map(|s| !s.is_empty()).unwrap_or(false) {
             merge.initialize(cached_content.as_deref().unwrap());
        }

        // Capture the *current* version (which will become the parent) BEFORE applying the edit
        let current_ver_before_edit = merge.get_version().first().cloned();

        let patch = crate::core::merge::MergePatch::new("everything", serde_json::Value::String(new_content.clone()));
        let res = merge.local_edit(patch);
        
        let ver = res.version.unwrap_or_else(|| BraidVersion::new(format!("{}-{}", peer_id, 0)));

        // Add the *previous* version to effective_parents
        if let Some(pv) = current_ver_before_edit {
            if !effective_parents.contains(&pv) {
                effective_parents.push(pv);
            }
        }
 
        (ver, res.rebased_patches, my_id)
    };

    // Guard: If no patches were generated, it means local content matches the current merge state.
    // We skip the PUT to avoid sending a duplicate version ID which would trigger a 500 on braid.org.
    if patches.is_empty() {
        info!("[BraidFS-Sync] No local changes detected for {} - skipping PUT", url_str);
        return Ok(());
    }

    info!("[BraidFS-Sync] Using version: {} (Peer: {})", new_version_id, my_id);

    if !effective_parents.is_empty() {
        let filtered_parents: Vec<BraidVersion> = effective_parents
            .iter()
            .filter(|p| !p.to_string().starts_with("temp-") && !p.to_string().starts_with("missing-"))
            .cloned()
            .collect();

        // 3. Self-Healing: Flatten self-forks.
        // If we have multiple parents from the SAME peer (us or others), it might confuse simpleton servers.
        // We pick the "latest" one per peer (lexically).
        use std::collections::HashMap;
        let mut latest_per_peer: HashMap<String, BraidVersion> = HashMap::new();
        for p in filtered_parents {
            let p_str = p.to_string();
            let parts: Vec<&str> = p_str.split('-').collect();
            if parts.len() >= 2 {
                let peer = parts[0];
                let ver_str = parts[1];
                if let Some(existing) = latest_per_peer.get(peer) {
                    let existing_str = existing.to_string();
                    let existing_ver = existing_str.split('-').last().unwrap_or("0");
                    if ver_str > existing_ver {
                        latest_per_peer.insert(peer.to_string(), p);
                    }
                } else {
                    latest_per_peer.insert(peer.to_string(), p);
                }
            } else {
                // If it doesn't follow peer-ver format, keep it anyway
                latest_per_peer.insert(p_str, p);
            }
        }
        effective_parents = latest_per_peer.into_values().collect();

        if !effective_parents.is_empty() {
            let p_strings: Vec<String> = effective_parents.iter().map(|p| p.to_string()).collect();
            info!("[BraidFS-Sync] Setting Parents (flattened): {:?}", p_strings);
            request = request.with_parents(effective_parents.clone());
        }
    }

    let ct = content_type.unwrap_or_else(|| "text/plain".to_string());
    request = request.with_content_type(ct);

    // Convert patches to Braid patches logic
    if !patches.is_empty() {
        let http_patches: Vec<Patch> = patches.into_iter().map(|mp| {
            let content_bytes = match mp.content {
                serde_json::Value::String(s) => bytes::Bytes::from(s),
                val => bytes::Bytes::from(val.to_string()),
            };
            
            Patch {
                unit: "json".to_string(), // Simpleton ranges use json unit
                range: mp.range,
                content: content_bytes,
                content_length: None, 
            }
        }).collect();
        request = request.with_patches(http_patches);
    } else {
         request = request.with_body(new_content.clone());
    }
    
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

    info!("[BraidFS-Sync] Sending PUT with {} bytes body", new_content.len());
    let status = match state.client.fetch(&url_str, final_request).await {
        Ok(res) => {
            info!("[BraidFS-Sync] PUT response status: {}", res.status);
            if (200..300).contains(&res.status) {
                state.failed_syncs.write().await.remove(&url_str);
                info!("[BraidFS] Sync success (braid) status: {}", res.status);
                state
                    .content_cache
                    .write()
                    .await
                    .insert(url_str.clone(), new_content.clone());
                
                // Update version store with the new version
                {
                    let mut store = state.version_store.write().await;
                    use braid_http::types::Version;
                    store.update(&url_str, vec![Version::from(new_version_id.clone())], effective_parents.iter().map(|v| Version::from(v.clone())).collect());
                    match store.save().await {
                        Ok(_) => info!("[BraidFS-Sync] Updated version store to: {}", new_version_id),
                        Err(e) => error!("[BraidFS-Sync] Failed to save version store: {}", e),
                    }
                }
                
                return Ok(());
            }
            res.status
        }
        Err(e) => {
            error!("[BraidFS] Sync error: {}", e);
            500
        }
    };

    let err_msg = format!("Sync failed: HTTP {}", status);
    state
        .failed_syncs
        .write()
        .await
        .insert(url_str, (status, std::time::Instant::now()));
    Err(BraidError::Http(err_msg))
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
            if let Some(v_header) = res.header("version").or(res.header("current-version")) {
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

    // Generate version
    let new_version = format!("{}-{}", 
        PEER_ID.read().await.clone(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis()
    );
    request = request.with_version(BraidVersion::new(&new_version));

    // Add parents
    request = request.with_version(BraidVersion::new(&new_version));
    
    if !effective_parents.is_empty() {
        let final_parents: Vec<BraidVersion> = effective_parents.iter()
            .filter(|p| {
                let s = p.to_string();
                !s.starts_with("temp-") && !s.starts_with("missing-")
            })
            .map(|p| p.clone())
            .collect();
        if !final_parents.is_empty() {
            request = request.with_parents(final_parents);
        }
    }

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
                info!("[BraidFS] Binary sync success (braid) status: {}", res.status);
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
