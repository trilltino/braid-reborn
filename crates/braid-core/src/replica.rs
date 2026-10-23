//! Durable text replica shared by HTTP and Iroh adapters.
//!
//! Positional edits require the current frontier. Concurrent/stale edits receive
//! an explicit conflict rather than being applied to the wrong base.
use crate::core::merge::diamond::DiamondCRDT;
pub use braid_http::durable::document_lock as storage_lock;
use braid_http::{
    durable::{atomic_write, document_lock},
    types::{Update, Version},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex, OnceLock, Weak},
};
use tokio::sync::broadcast;

#[derive(Debug, thiserror::Error)]
pub enum ReplicaError {
    #[error("{0}")]
    Invalid(String),
    #[error("{0}")]
    Conflict(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaDocument {
    #[serde(default)]
    pub resource: Option<String>,
    pub content: String,
    pub heads: Vec<String>,
    pub version_graph: HashMap<String, Vec<String>>,
    pub merge_type: String,
    pub merge_state: Value,
    pub created_at: u64,
    pub modified_at: u64,
    #[serde(default)]
    pub history: Vec<Update>,
    #[serde(default)]
    pub fingerprints: HashMap<String, String>,
}
fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
pub fn same_frontier(a: &[String], b: &[String]) -> bool {
    a.len() == b.len() && a.iter().all(|v| b.contains(v))
}
impl ReplicaDocument {
    pub fn empty(merge_type: &str) -> Self {
        Self {
            resource: None,
            content: String::new(),
            heads: vec![],
            version_graph: HashMap::new(),
            merge_type: merge_type.into(),
            merge_state: Value::Null,
            created_at: now(),
            modified_at: now(),
            history: vec![],
            fingerprints: HashMap::new(),
        }
    }
    pub fn snapshot(&self) -> Update {
        Update {
            version: self.heads.iter().map(Version::new).collect(),
            body: Some(self.content.clone().into()),
            merge_type: Some(self.merge_type.clone()),
            content_type: Some("text/plain; charset=utf-8".into()),
            ..Update::default()
        }
    }
    /// Validate the entire update on a candidate; callers persist before publishing it.
    pub fn apply(&mut self, update: &Update) -> Result<bool, ReplicaError> {
        if update.version.len() != 1 {
            return Err(ReplicaError::Invalid(
                "A mutation must name one event".into(),
            ));
        }
        if !update.version[0].is_string() || update.parents.iter().any(|v| !v.is_string()) {
            return Err(ReplicaError::Invalid(
                "This text replica requires string version identifiers".into(),
            ));
        }
        let id = update.version[0].to_string();
        if id.is_empty() || update.parents.contains(&update.version[0]) {
            return Err(ReplicaError::Invalid(
                "Empty or self-parented version".into(),
            ));
        }
        if !matches!(self.merge_type.as_str(), "simpleton" | "diamond")
            || update
                .merge_type
                .as_ref()
                .is_some_and(|m| *m != self.merge_type)
        {
            return Err(ReplicaError::Conflict("Merge type mismatch".into()));
        }
        let mut parents: Vec<String> = update.parents.iter().map(ToString::to_string).collect();
        if parents
            .iter()
            .enumerate()
            .any(|(i, p)| parents[..i].contains(p))
        {
            return Err(ReplicaError::Invalid("Duplicate parents".into()));
        }
        parents.sort();
        // Hash only mutation identity/semantics, independent of transport headers.
        let patch_semantics = update.patches.as_ref().map(|patches| {
            patches
                .iter()
                .map(|p| (&p.unit, &p.range, &p.content))
                .collect::<Vec<_>>()
        });
        let fingerprint = format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&(
                &update.version,
                &parents,
                &self.merge_type,
                &update.body,
                &patch_semantics,
                &update.content_type,
            ))?)
        );
        if let Some(old) = self.fingerprints.get(&id) {
            return if *old == fingerprint {
                Ok(false)
            } else {
                Err(ReplicaError::Conflict(
                    "Version reused with different payload".into(),
                ))
            };
        }
        if self.version_graph.contains_key(&id) {
            return Err(ReplicaError::Conflict(
                "Legacy version cannot be deduplicated without its original payload".into(),
            ));
        }
        for parent in &parents {
            if !self.version_graph.contains_key(parent) && !self.heads.contains(parent) {
                return Err(ReplicaError::Conflict(format!("Unknown parent: {parent}")));
            }
        }
        if !same_frontier(&self.heads, &parents) {
            return Err(ReplicaError::Conflict(
                "Stale or concurrent positional update; fetch a snapshot and resolve the conflict"
                    .into(),
            ));
        }
        let content = match (&update.body, &update.patches) {
            (Some(body), None) => std::str::from_utf8(body)
                .map_err(|e| ReplicaError::Invalid(e.to_string()))?
                .to_owned(),
            (None, Some(patches)) => {
                braid_http::protocol::text::apply_text_patches(&self.content, patches)
                    .map_err(|e| ReplicaError::Invalid(e.to_string()))?
            }
            _ => return Err(ReplicaError::Invalid("Expected snapshot or patches".into())),
        };
        if let Some(digest) = update.extra_headers.get("repr-digest") {
            use base64::Engine;
            let actual = base64::engine::general_purpose::STANDARD
                .encode(Sha256::digest(content.as_bytes()));
            if digest.trim() != format!("sha-256=:{actual}:") {
                return Err(ReplicaError::Invalid(
                    "Representation digest mismatch or unsupported digest algorithm".into(),
                ));
            }
        }
        let mut crdt = if self.merge_state.is_null() {
            // Explicit migration of snapshot-only state: retain the old graph and seed a checkpoint.
            let mut crdt = DiamondCRDT::new(format!("replica-{}", uuid::Uuid::new_v4()));
            if !self.content.is_empty() {
                crdt.add_insert(0, &self.content);
            }
            crdt
        } else {
            DiamondCRDT::restore_checkpoint(&self.merge_state).map_err(ReplicaError::Invalid)?
        };
        let len = crdt.content().chars().count();
        if len > 0 {
            crdt.add_delete(0..len);
        }
        if !content.is_empty() {
            crdt.add_insert(0, &content);
        }
        crdt.register_version_mapping(id.clone(), crdt.get_local_frontier());
        let checkpoint = crdt.durable_checkpoint().map_err(ReplicaError::Invalid)?;
        self.content = content;
        self.merge_state = checkpoint;
        self.version_graph.insert(id.clone(), parents);
        self.heads = vec![id.clone()];
        self.fingerprints.insert(id, fingerprint);
        self.history.push(update.clone());
        self.modified_at = now();
        Ok(true)
    }
    /// A snapshot is a valid replacement when old history predates migration.
    pub fn replay_since(&self, frontier: &[String]) -> Result<Vec<Update>, ReplicaError> {
        for v in frontier {
            if !self.version_graph.contains_key(v) && !self.heads.contains(v) {
                return Err(ReplicaError::Conflict(format!(
                    "Unknown replay version: {v}"
                )));
            }
        }
        if same_frontier(frontier, &self.heads) {
            return Ok(vec![]);
        }
        if frontier.len() == 1 {
            if let Some(i) = self
                .history
                .iter()
                .position(|u| u.version.iter().any(|v| v.to_string() == frontier[0]))
            {
                return Ok(self.history[i + 1..].to_vec());
            }
        }
        Ok(vec![self.snapshot()])
    }
}

#[derive(Clone)]
pub struct ReplicaStore {
    base_dir: PathBuf,
}
impl ReplicaStore {
    pub fn new(base_dir: PathBuf) -> Self {
        let base_dir = if base_dir.is_absolute() {
            base_dir
        } else {
            std::env::current_dir().unwrap_or_default().join(base_dir)
        };
        Self { base_dir }
    }
    pub fn path(&self, resource: &str) -> PathBuf {
        self.base_dir
            .join(format!("p-{:x}.json", Sha256::digest(resource.as_bytes())))
    }
    fn channel(&self, resource: &str) -> Arc<broadcast::Sender<Update>> {
        static CHANNELS: OnceLock<Mutex<HashMap<PathBuf, Weak<broadcast::Sender<Update>>>>> =
            OnceLock::new();
        let mut channels = CHANNELS
            .get_or_init(Default::default)
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        channels.retain(|_, v| v.strong_count() > 0);
        let key = self.path(resource);
        if let Some(channel) = channels.get(&key).and_then(Weak::upgrade) {
            channel
        } else {
            let channel = Arc::new(broadcast::channel(128).0);
            channels.insert(key, Arc::downgrade(&channel));
            channel
        }
    }
    pub async fn load(&self, resource: &str) -> Result<Option<ReplicaDocument>, ReplicaError> {
        let path = self.path(resource);
        let data = match tokio::fs::read(&path).await {
            Ok(data) => data,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // Read the old JSON naming convention once, then commit to the unambiguous path.
                let name = resource.replace(['/', '\\'], "_");
                if name.is_empty() || name.contains(':') || name.starts_with('.') {
                    return Ok(None);
                }
                let name = if name.ends_with(".json") {
                    name
                } else {
                    format!("{name}.json")
                };
                match tokio::fs::read(self.base_dir.join(name)).await {
                    Ok(data) => data,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                    Err(e) => return Err(e.into()),
                }
            }
            Err(e) => return Err(e.into()),
        };
        Ok(Some(serde_json::from_slice(&data)?))
    }
    pub async fn commit(
        &self,
        resource: &str,
        update: &Update,
        merge_type: &str,
    ) -> Result<ReplicaDocument, ReplicaError> {
        let _guard = document_lock(self.path(resource)).await;
        let mut document = self
            .load(resource)
            .await?
            .unwrap_or_else(|| ReplicaDocument::empty(merge_type));
        if document.apply(update)? {
            document.resource = Some(resource.to_owned());
            atomic_write(&self.path(resource), &serde_json::to_vec(&document)?).await?;
            let _ = self.channel(resource).send(document.snapshot());
        }
        Ok(document)
    }
    /// Subscribe and read under the commit lock to avoid the snapshot/subscription race.
    pub async fn subscribe(
        &self,
        resource: &str,
        merge_type: &str,
    ) -> Result<
        (
            ReplicaDocument,
            broadcast::Receiver<Update>,
            Arc<broadcast::Sender<Update>>,
        ),
        ReplicaError,
    > {
        let _guard = document_lock(self.path(resource)).await;
        let channel = self.channel(resource);
        let receiver = channel.subscribe();
        let document = self
            .load(resource)
            .await?
            .unwrap_or_else(|| ReplicaDocument::empty(merge_type));
        Ok((document, receiver, channel))
    }
}
