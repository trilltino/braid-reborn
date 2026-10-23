//! Local projection and edits share one filesystem transaction boundary with NFS.
use super::{config, mapping, state::DaemonState, sync::content_hash};
use crate::{
    core::{BraidError, Result},
    simpleton::SimpletonClient,
};
use std::path::Path;
use tokio::io::AsyncReadExt;

pub(crate) fn try_lock(root: &Path) -> std::io::Result<std::fs::File> {
    braid_http::durable::try_writer_lease(&root.join(".braidfs/files.lock"))
}

pub(crate) async fn lock() -> Result<std::fs::File> {
    let root = config::get_root_dir()?;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let root = root.clone();
        match tokio::task::spawn_blocking(move || try_lock(&root))
            .await
            .map_err(|error| BraidError::Internal(error.to_string()))?
        {
            Ok(lease) => return Ok(lease),
            Err(error)
                if error.kind() == std::io::ErrorKind::WouldBlock
                    && tokio::time::Instant::now() < deadline =>
            {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            Err(error) => return Err(error.into()),
        }
    }
}

pub(crate) async fn read_text(path: &Path) -> Result<Option<String>> {
    let file = match tokio::fs::File::open(path).await {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let mut bytes = Vec::new();
    file.take(8 * 1024 * 1024 + 1)
        .read_to_end(&mut bytes)
        .await?;
    if bytes.len() > 8 * 1024 * 1024 {
        return Err(BraidError::Fs("Filesystem text exceeds 8 MiB".into()));
    }
    Ok(Some(String::from_utf8(bytes)?))
}

/// Apply two independent scalar edits at the same saved base through the existing
/// causal engine. This is a local reconciliation; the wire merge type stays Simpleton.
pub(crate) fn merge(base: &str, local: &str, remote: &str) -> Result<String> {
    if local == remote || remote == base {
        return Ok(local.to_owned());
    }
    if local == base {
        return Ok(remote.to_owned());
    }
    use braid_http::types::{Update, Version};
    let mut document = crate::replica::ReplicaDocument::empty("diamond");
    let base_id = Version::new("projection-base");
    let result = (|| {
        document.apply(
            &Update::snapshot(base_id.clone(), base.to_owned()).with_merge_type("diamond"),
        )?;
        for (side, text) in [("local", local), ("remote", remote)] {
            let (patch, _) = crate::simpleton::scalar_diff(base, text)
                .map_err(|error| crate::replica::ReplicaError::Invalid(error.to_string()))?;
            document.apply(
                &Update::patched(
                    Version::new(format!(
                        "projection-{side}-{}",
                        content_hash(text.as_bytes())
                    )),
                    vec![patch],
                )
                .with_parents(vec![base_id.clone()])
                .with_merge_type("diamond"),
            )?;
        }
        Ok::<_, crate::replica::ReplicaError>(document.content)
    })();
    result.map_err(|error| BraidError::MergeConflict(error.to_string()))
}

/// Reconcile a live disk view and the journal, then atomically project it. The
/// revision comparison prevents a remote receive from being overwritten by a
/// decision made using an older application state. No network ACK is awaited here.
pub(crate) async fn reconcile(
    url: &str,
    state: &DaemonState,
    session: &SimpletonClient,
    local_edit: bool,
    requested: Option<&str>,
) -> Result<()> {
    let _lease = lock().await?;
    if super::operations::blocks(url, state).await? {
        return if local_edit {
            Err(BraidError::Fs(
                "NFS operation retains this edit until reconciliation".into(),
            ))
        } else {
            Ok(())
        };
    }
    let path = mapping::url_to_path(url)?;
    let local = match requested {
        Some(text) => Some(text.to_owned()),
        None => read_text(&path).await?,
    };
    let (base, deleted) = {
        let store = state.version_store.read().await;
        (
            store
                .materialized
                .get(url)
                .cloned()
                .or_else(|| store.get(url).and_then(|v| v.content.clone())),
            store.deleted.contains_key(url),
        )
    };
    if deleted && (local.is_none() || !local_edit) {
        return Ok(());
    }
    for _ in 0..8 {
        let current = session.status().borrow().clone();
        if let Some(error) = current.error {
            return Err(BraidError::Protocol(error));
        }
        if !current.initialized && !local_edit {
            return Ok(());
        }
        let mut text = current.text.clone();
        if local_edit
            || local
                .as_ref()
                .is_some_and(|local| local != &current.text && Some(local) != base.as_ref())
        {
            let local = local.as_deref().unwrap_or(""); // An observed unlink is an explicit empty replacement.
            text = match &base {
                Some(base) => merge(base, local, &current.text)?,
                None if local == current.text => local.to_owned(),
                None => {
                    return Err(BraidError::MergeConflict(
                        "Local text has no saved merge base; both copies retained".into(),
                    ))
                }
            };
        }
        let queued = text != current.text;
        if !queued && !local_edit && local.as_ref() == Some(&text) {
            let store = state.version_store.read().await;
            let confirmed =
                current.initialized && !current.has_pending_edit && current.outstanding_puts == 0;
            if store.materialized.get(url) == Some(&text)
                && (!confirmed
                    || store.get(url).is_some_and(|version| {
                        super::subscription::same_frontier(
                            &version.current_version,
                            &current.version,
                        ) && version.content.as_ref() == Some(&text)
                    }))
            {
                return Ok(());
            }
        }
        if queued {
            match session
                .changed_if_revision(current.revision, text.clone())
                .await
            {
                Err(braid_http::BraidError::MergeConflict(_)) => continue,
                other => other?,
            }
        }
        if local.as_ref() != Some(&text) && !(local_edit && local.is_none() && text.is_empty()) {
            state.pending.add(path.clone());
            braid_http::durable::atomic_write(&path, text.as_bytes()).await?;
        }
        let mut store = state.version_store.write().await;
        let old_view = store.materialized.insert(url.to_owned(), text.clone());
        let old_version = store.file_versions.get(url).cloned();
        let old_deleted = if local_edit {
            store.deleted.remove(url)
        } else {
            None
        };
        if !queued
            && current.initialized
            && !current.has_pending_edit
            && current.outstanding_puts == 0
        {
            store.update_with_hash(
                url,
                current.version,
                vec![],
                Some(content_hash(text.as_bytes())),
            );
            if let Some(version) = store.file_versions.get_mut(url) {
                version.content = Some(text.clone());
            }
        }
        if let Err(error) = store.save().await {
            match old_view {
                Some(view) => {
                    store.materialized.insert(url.to_owned(), view);
                }
                None => {
                    store.materialized.remove(url);
                }
            }
            match old_version {
                Some(version) => {
                    store.file_versions.insert(url.to_owned(), version);
                }
                None => {
                    store.file_versions.remove(url);
                }
            }
            if let Some(deleted) = old_deleted {
                store.deleted.insert(url.to_owned(), deleted);
            }
            return Err(error);
        }
        state
            .content_cache
            .write()
            .await
            .insert(url.to_owned(), text);
        return Ok(());
    }
    Err(BraidError::MergeConflict(
        "Text kept changing during projection; retry retained".into(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn disjoint_unicode_edits_keep_both_sides() {
        assert_eq!(
            merge("a\u{1f600}b", "A\u{1f600}b", "a\u{1f600}B").unwrap(),
            "A\u{1f600}B"
        );
        assert_eq!(
            merge("abc", "abc local", "remote abc").unwrap(),
            "remote abc local"
        );
        assert_eq!(merge("abc", "abc", "ab").unwrap(), "ab");
    }
}
