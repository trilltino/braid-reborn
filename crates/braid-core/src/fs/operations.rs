//! Durable NFS namespace intentions, reconciled by the daemon's existing text
//! channels. A rename is copy-then-empty over HTTP, not an atomic remote rename.
use super::{config, mapping, state::DaemonState};
use crate::core::{BraidError, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
#[cfg(feature = "nfs")]
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Entry {
    pub source: String,
    pub destination: Option<String>,
    pub snapshot: u32,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Plan {
    pub entries: Vec<Entry>,
}

pub(crate) fn initialize(db: &Connection) -> rusqlite::Result<()> {
    db.execute_batch("CREATE TABLE IF NOT EXISTS nfs_changes(id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL, source TEXT NOT NULL, destination TEXT, state TEXT NOT NULL, plan TEXT, last_error TEXT);")?;
    let columns = {
        let mut statement = db.prepare("PRAGMA table_info(nfs_changes)")?;
        let columns = statement
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        columns
    };
    for (name, sql) in [
        ("plan", "ALTER TABLE nfs_changes ADD COLUMN plan TEXT"),
        (
            "last_error",
            "ALTER TABLE nfs_changes ADD COLUMN last_error TEXT",
        ),
    ] {
        if !columns.iter().any(|column| column == name) {
            db.execute_batch(sql)?;
        }
    }
    Ok(())
}

fn snapshot_path(root: &Path, id: i64, index: u32) -> PathBuf {
    root.join(".braidfs/nfs-ops")
        .join(id.to_string())
        .join(format!("{index}.bin"))
}

/// Copy before recording `prepared`. Interrupted staging never mutates the live
/// namespace. Files/bytes/depth are bounded, and symlinks/special files fail closed.
#[cfg(feature = "nfs")]
pub(crate) fn capture(
    root: &Path,
    id: i64,
    source: &str,
    destination: Option<&str>,
) -> std::io::Result<Plan> {
    let directory = root.join(".braidfs/nfs-ops").join(id.to_string());
    std::fs::create_dir_all(&directory)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700))?;
    }
    let mut stack = vec![(source.to_owned(), destination.map(str::to_owned), 0u32)];
    let mut entries = Vec::new();
    let mut total = 0u64;
    let mut visited = 0usize;
    while let Some((source, destination, depth)) = stack.pop() {
        visited += 1;
        if visited > 256 || depth > 32 {
            return Err(std::io::Error::other(
                "NFS operation exceeds its tree limit",
            ));
        }
        let path = root.join(
            source
                .strip_prefix('/')
                .ok_or_else(|| std::io::Error::other("Invalid virtual path"))?,
        );
        let meta = std::fs::symlink_metadata(&path)?;
        if meta.is_dir() && !meta.file_type().is_symlink() {
            for child in std::fs::read_dir(path)? {
                let child = child?;
                let name = child
                    .file_name()
                    .into_string()
                    .map_err(|_| std::io::Error::other("Non UTF-8 pathname"))?;
                if name.starts_with('.') {
                    return Err(std::io::Error::other(
                        "Private files cannot be remotely renamed",
                    ));
                }
                stack.push((
                    format!("{source}/{name}"),
                    destination
                        .as_ref()
                        .map(|destination| format!("{destination}/{name}")),
                    depth + 1,
                ));
                if stack.len() > 256 {
                    return Err(std::io::Error::other(
                        "NFS operation exceeds its tree limit",
                    ));
                }
            }
        } else if meta.is_file() && !meta.file_type().is_symlink() {
            if entries.len() >= 128 || meta.len() > 64 * 1024 * 1024 {
                return Err(std::io::Error::other("NFS snapshot limit exceeded"));
            }
            let index = entries.len() as u32;
            let mut input = std::fs::File::open(path)?.take(64 * 1024 * 1024 + 1);
            let mut output = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(snapshot_path(root, id, index))?;
            let size = std::io::copy(&mut input, &mut output)?;
            total = total
                .checked_add(size)
                .ok_or_else(|| std::io::Error::other("Snapshot size overflow"))?;
            if size > 64 * 1024 * 1024 || total > 128 * 1024 * 1024 {
                return Err(std::io::Error::other("NFS snapshot byte limit exceeded"));
            }
            output.flush()?;
            output.sync_all()?;
            entries.push(Entry {
                source,
                destination,
                snapshot: index,
            });
        } else {
            return Err(std::io::Error::other(
                "Symlinks and special files cannot be renamed through NFS",
            ));
        }
    }
    #[cfg(unix)]
    {
        std::fs::File::open(&directory)?.sync_all()?;
        if let Some(parent) = directory.parent() {
            std::fs::File::open(parent)?.sync_all()?;
        }
    }
    Ok(Plan { entries })
}

fn beneath(path: &str, parent: &str) -> bool {
    path == parent
        || path
            .strip_prefix(parent)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

pub(crate) async fn blocks(url: &str, state: &DaemonState) -> Result<bool> {
    let path = mapping::url_to_path(url)?;
    let root = config::get_root_dir()?;
    let path = path
        .strip_prefix(&root)
        .map_err(|error| BraidError::Fs(error.to_string()))?;
    let path = format!("/{}", path.to_string_lossy().replace('\\', "/"));
    let db = state.inode_db.clone();
    tokio::task::spawn_blocking(move || {
        let db = db.lock();
        initialize(&db).map_err(sql_error)?;
        let mut statement = db.prepare("SELECT source,destination FROM nfs_changes WHERE state IN ('staging','prepared','remote_pending','repair_required')").map_err(sql_error)?;
        let mut rows = statement.query([]).map_err(sql_error)?;
        while let Some(row) = rows.next().map_err(sql_error)? {
            let source: String = row.get(0).map_err(sql_error)?;
            let destination: Option<String> = row.get(1).map_err(sql_error)?;
            if beneath(&path,&source) || destination.is_some_and(|destination| beneath(&path,&destination)) {return Ok(true);}
        }
        Ok(false)
    }).await.map_err(|error| BraidError::Internal(error.to_string()))?
}
fn sql_error(error: rusqlite::Error) -> BraidError {
    BraidError::Fs(format!("NFS operation journal: {error}"))
}

struct Operation {
    id: i64,
    plan: Option<String>,
}
async fn next(state: &DaemonState) -> Result<Option<Operation>> {
    let db = state.inode_db.clone();
    tokio::task::spawn_blocking(move || {
        let db = db.lock();
        initialize(&db).map_err(sql_error)?;
        db.query_row(
            "SELECT id,plan FROM nfs_changes WHERE state='remote_pending' ORDER BY id LIMIT 1",
            [],
            |row| {
                Ok(Operation {
                    id: row.get(0)?,
                    plan: row.get(1)?,
                })
            },
        )
        .optional()
        .map_err(sql_error)
    })
    .await
    .map_err(|error| BraidError::Internal(error.to_string()))?
}
async fn mark(state: &DaemonState, id: i64, status: &str, error: Option<String>) -> Result<()> {
    let db = state.inode_db.clone();
    let status = status.to_owned();
    tokio::task::spawn_blocking(move || {
        db.lock()
            .execute(
                "UPDATE nfs_changes SET state=?,last_error=? WHERE id=?",
                params![status, error, id],
            )
            .map_err(sql_error)
    })
    .await
    .map_err(|error| BraidError::Internal(error.to_string()))??;
    Ok(())
}

async fn configured_url(path: &str, state: &DaemonState) -> Result<Option<String>> {
    let root = config::get_root_dir()?;
    let target = root.join(path.trim_start_matches('/'));
    mapping::configured_url(&target, state).await
}

async fn destination_url(source: &str, destination: &str, state: &DaemonState) -> Result<String> {
    if let Some(url) = configured_url(destination, state).await? {
        return Ok(url);
    }
    let root = config::get_root_dir()?;
    let candidate = mapping::path_to_url(&root.join(destination.trim_start_matches('/')))?;
    let source = url::Url::parse(source).map_err(|error| BraidError::Fs(error.to_string()))?;
    let mut target =
        url::Url::parse(&candidate).map_err(|error| BraidError::Fs(error.to_string()))?;
    target
        .set_scheme(source.scheme())
        .map_err(|()| BraidError::Fs("Unsupported destination scheme".into()))?;
    if target.origin() != source.origin() {
        return Err(BraidError::Fs(
            "Cross-origin NFS rename needs an explicitly configured destination URL".into(),
        ));
    }
    Ok(target.to_string())
}

/// Process at most one intention. Returns true only when all affected configured
/// resources have durable ACKs (or no resource was configured for synchronization).
pub async fn reconcile_next(state: &DaemonState) -> Result<bool> {
    let Some(operation) = next(state).await? else {
        return Ok(false);
    };
    let Some(plan) = &operation.plan else {
        mark(
            state,
            operation.id,
            "repair_required",
            Some("Legacy operation has no captured content; explicit recovery required".into()),
        )
        .await?;
        return Err(BraidError::Fs(
            "Legacy NFS operation retained for repair".into(),
        ));
    };
    let plan: Plan = serde_json::from_str(plan)?;
    let result = apply(state, operation.id, &plan).await;
    match result {
        Ok(synced) => {
            mark(
                state,
                operation.id,
                if synced {
                    "remote_confirmed"
                } else {
                    "local_only"
                },
                None,
            )
            .await?;
            // Release the namespace gate before reconciling later local edits.
            // A crash here is recovered by the enabled resource's subscription.
            for entry in &plan.entries {
                if let Some(destination) = &entry.destination {
                    if let Some(url) = configured_url(destination, state).await? {
                        let session = state.text_sessions.lock().await.get(&url).cloned();
                        if let Some(session) = session {
                            super::file_service::reconcile(&url, state, &session, false, None)
                                .await?;
                        }
                    }
                }
            }
            Ok(true)
        }
        Err(error) => {
            mark(
                state,
                operation.id,
                "remote_pending",
                Some(error.to_string()),
            )
            .await?;
            Err(error)
        }
    }
}
async fn apply(state: &DaemonState, id: i64, plan: &Plan) -> Result<bool> {
    let sequence = u64::try_from(id).map_err(|error| BraidError::Fs(error.to_string()))?;
    let root = config::get_root_dir()?;
    let mut synced = false;
    for entry in &plan.entries {
        let source_url = configured_url(&entry.source, state).await?;
        let target_url = match (&entry.destination, &source_url) {
            (Some(destination), Some(source)) => {
                Some(destination_url(source, destination, state).await?)
            }
            (Some(destination), None) => configured_url(destination, state).await?,
            (None, _) => None,
        };
        if let Some(url) = target_url {
            synced = true;
            let text = super::file_service::read_text(&snapshot_path(&root, id, entry.snapshot))
                .await?
                .ok_or_else(|| BraidError::Fs("NFS operation snapshot is missing".into()))?;
            let session =
                super::subscription::text_session_for_operation(&url, state, sequence, &text)
                    .await?;
            {
                let _lease = super::file_service::lock().await?;
                let mut store = state.version_store.write().await;
                // The actual file may already contain a later local edit. The
                // captured bytes are its base, not a request to overwrite it.
                if session.status().borrow().last_operation == sequence {
                    store.materialized.insert(url.clone(), text.clone());
                }
                if store.deleted.get(&url).is_some_and(|old| *old <= sequence) {
                    store.deleted.remove(&url);
                }
                store.save().await?;
            }
            super::sync::wait_for_ack(&session).await?;
            let mut config = state.config.write().await;
            config.sync.insert(url, true);
            config.save().await?;
        }
        let Some(source_url) = source_url else {
            continue;
        };
        synced = true;
        let session =
            super::subscription::text_session_for_operation(&source_url, state, sequence, "")
                .await?;
        {
            let _lease = super::file_service::lock().await?;
            let mut store = state.version_store.write().await;
            if store
                .deleted
                .get(&source_url)
                .is_none_or(|old| *old < sequence)
            {
                store.deleted.insert(source_url.clone(), sequence);
            }
            store.save().await?;
        }
        super::sync::wait_for_ack(&session).await?;
    }
    Ok(synced)
}

pub(crate) async fn run(state: DaemonState) {
    loop {
        match reconcile_next(&state).await {
            Ok(true) => continue,
            Ok(false) => {}
            Err(error) => {
                tracing::warn!(%error,"NFS remote operation remains journaled for retry or repair")
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
}
