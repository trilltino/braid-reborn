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

    if subscriptions
        .get(&url)
        .is_some_and(|handle| !handle.is_finished())
    {
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
        tracing::warn!(
            "[DEBUG] Sync not enabled for {}, skipping subscription",
            url
        );
        return;
    }

    let url_capture = url.clone();
    let state_capture = state.clone();
    let handle = tokio::spawn(async move {
        if let Err(error) = subscribe_loop(url_capture.clone(), state_capture.clone()).await {
            tracing::error!(%error,url=%url_capture,"Text synchronization stopped; durable edits retained for repair");
        }
    });
    subscriptions.insert(url, handle);
}

pub(crate) async fn text_session(
    url: &str,
    state: &DaemonState,
) -> Result<std::sync::Arc<crate::simpleton::SimpletonClient>> {
    text_session_seeded(url, state, None).await
}

pub(crate) async fn text_session_for_operation(
    url: &str,
    state: &DaemonState,
    sequence: u64,
    text: &str,
) -> Result<std::sync::Arc<crate::simpleton::SimpletonClient>> {
    let session = text_session_seeded(url, state, Some((sequence, text))).await?;
    session
        .changed_for_operation(sequence, text.to_owned())
        .await?;
    Ok(session)
}

async fn text_session_seeded(
    url: &str,
    state: &DaemonState,
    operation: Option<(u64, &str)>,
) -> Result<std::sync::Arc<crate::simpleton::SimpletonClient>> {
    if let Some(configured) = mapping::configured_url(&mapping::url_to_path(url)?, state).await? {
        if configured != url {
            return Err(crate::core::BraidError::Config(
                "Requested URL collides with another configured local pathname".into(),
            ));
        }
    }
    let mut sessions = state.text_sessions.lock().await;
    if let Some(session) = sessions.get(url) {
        return Ok(session.clone());
    }
    if state.version_store.read().await.outbox.contains_key(url) {
        return Err(crate::core::BraidError::Fs(
            "Legacy durable edit must replay before starting the shared channel".into(),
        ));
    }
    let journal_path = crate::fs::config::get_root_dir()?
        .join(".braidfs/channels")
        .join(format!(
            "{}.json",
            crate::fs::sync::content_hash(url.as_bytes())
        ));
    let journal = braid_http::client::ChannelJournal::open(journal_path).await?;
    let known = state.version_store.read().await.get(url).cloned();
    let base = if journal.checkpoint().application.is_none() {
        if let Some(known) = known {
            let text = known.content.ok_or_else(|| {
                crate::core::BraidError::Fs(
                    "Saved text base is unavailable; explicit history recovery required".into(),
                )
            })?;
            let local = match tokio::fs::read_to_string(mapping::url_to_path(url)?).await {
                Ok(local) => Some(local),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => return Err(error.into()),
            };
            let pending = operation
                .map(|(_, text)| text.to_owned())
                .or_else(|| local.filter(|local| local != &text));
            Some(crate::simpleton::SimpletonBase {
                text,
                version: known.current_version,
                pending,
                initialized: true,
                last_operation: operation.map_or(0, |(sequence, _)| sequence),
            })
        } else {
            operation.map(|(sequence, text)| crate::simpleton::SimpletonBase {
                text: String::new(),
                version: vec![],
                pending: Some(text.to_owned()),
                initialized: false,
                last_operation: sequence,
            })
        }
    } else {
        None
    };
    let request = crate::fs::sync::authenticate(BraidRequest::new(), url, state).await;
    let session = std::sync::Arc::new(
        crate::simpleton::SimpletonClient::connect_seeded(
            &state.client,
            url,
            journal,
            request,
            base,
        )
        .await?,
    );
    sessions.insert(url.to_owned(), session.clone());
    Ok(session)
}

pub async fn subscribe_loop(url: String, state: DaemonState) -> Result<()> {
    let session = text_session(&url, &state).await?;
    let mut status = session.status();
    let result = async {
        loop {
            let current = status.borrow_and_update().clone();
            if let Some(error) = current.error {
                return Err(crate::core::BraidError::Protocol(error));
            }
            if current.initialized && !state.local_server_managed.read().await.contains(&url) {
                super::file_service::reconcile(&url, &state, &session, false, None).await?;
            }
            status
                .changed()
                .await
                .map_err(|_| crate::core::BraidError::SubscriptionClosed)?;
        }
    }
    .await;
    session.shutdown().await;
    let mut sessions = state.text_sessions.lock().await;
    if sessions
        .get(&url)
        .is_some_and(|stored| std::sync::Arc::ptr_eq(stored, &session))
    {
        sessions.remove(&url);
    }
    result
}

pub async fn materialize_update(
    url: &str,
    state: &DaemonState,
    update: &braid_http::types::Update,
) -> Result<()> {
    let _lease = super::file_service::lock().await?;
    let path = mapping::url_to_path(url)?;
    let key = if path.is_absolute() {
        path.clone()
    } else {
        std::env::current_dir()?.join(&path)
    };
    let _guard = braid_http::durable::document_lock(key).await;
    let known = state.version_store.read().await.get(url).cloned();
    if known
        .as_ref()
        .is_some_and(|v| same_frontier(&v.current_version, &update.version))
        && !update.version.is_empty()
    {
        return Ok(());
    }
    if update.version.is_empty()
        && known
            .as_ref()
            .is_some_and(|v| !v.current_version.is_empty())
    {
        return Err(crate::core::BraidError::Protocol(
            "Versioned file received an unversioned update".into(),
        ));
    }
    if state.version_store.read().await.outbox.contains_key(url) {
        return Err(crate::core::BraidError::Protocol(
            "Remote update deferred while a local edit is pending".into(),
        ));
    }
    let local = match tokio::fs::read(&path).await {
        Ok(data) => Some(data),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => return Err(e.into()),
    };
    let cached = state.content_cache.read().await.get(url).cloned();
    let data = if let Some(body) = &update.body {
        body.clone()
    } else if let Some(patches) = &update.patches {
        let current = known
            .as_ref()
            .map(|v| v.current_version.as_slice())
            .unwrap_or_default();
        if !same_frontier(current, &update.parents) {
            return Err(crate::core::BraidError::Protocol(
                "Patch parents do not match the materialized file; snapshot required".into(),
            ));
        }
        let disk_base = local.as_deref().unwrap_or_default();
        let saved = known
            .as_ref()
            .and_then(|v| v.content.as_deref())
            .or(cached.as_deref());
        let base = if let Some(saved) = saved {
            saved
        } else if known.is_none() {
            ""
        } else {
            if !known
                .as_ref()
                .and_then(|v| v.content_hash.as_ref())
                .is_some_and(|hash| *hash == crate::fs::sync::content_hash(disk_base))
            {
                return Err(crate::core::BraidError::Protocol(
                    "Acknowledged patch base unavailable; snapshot required".into(),
                ));
            }
            std::str::from_utf8(disk_base)
                .map_err(|e| crate::core::BraidError::Protocol(e.to_string()))?
        };
        bytes::Bytes::from(
            braid_http::protocol::text::apply_text_patches(base, patches)
                .map_err(|e| crate::core::BraidError::Protocol(e.to_string()))?,
        )
    } else {
        return Ok(());
    };
    let candidate = data.clone();
    let clean = local.as_ref().is_none_or(|data| {
        known
            .as_ref()
            .and_then(|v| v.content_hash.as_ref())
            .map(|hash| *hash == crate::fs::sync::content_hash(data))
            .unwrap_or_else(|| cached.as_ref().is_some_and(|text| text.as_bytes() == data))
            || candidate.as_ref() == data
    });
    if !clean {
        return Err(crate::core::BraidError::Protocol(
            "Dirty local file preserved; remote update requires conflict resolution".into(),
        ));
    }
    state.pending.add(path.clone());
    if let Err(e) = braid_http::durable::atomic_write(&path, &data).await {
        state.pending.remove(&path);
        return Err(e.into());
    }
    {
        let mut store = state.version_store.write().await;
        let old = store.file_versions.get(url).cloned();
        store.update_with_hash(
            url,
            update.version.clone(),
            update.parents.clone(),
            Some(crate::fs::sync::content_hash(&data)),
        );
        if let Some(version) = store.file_versions.get_mut(url) {
            version.content = std::str::from_utf8(&data).ok().map(str::to_owned);
        }
        if let Err(e) = store.save().await {
            match old {
                Some(old) => {
                    store.file_versions.insert(url.to_owned(), old);
                }
                None => {
                    store.file_versions.remove(url);
                }
            }
            return Err(e);
        }
    }
    state.tracker.mark(url);
    if let Ok(text) = std::str::from_utf8(&data) {
        state
            .content_cache
            .write()
            .await
            .insert(url.to_owned(), text.to_owned());
    }
    state.active_merges.write().await.remove(url);
    Ok(())
}

pub(crate) fn same_frontier(
    a: &[braid_http::types::Version],
    b: &[braid_http::types::Version],
) -> bool {
    a.len() == b.len() && a.iter().all(|v| b.contains(v))
}
