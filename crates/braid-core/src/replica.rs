//! Durable text replica shared by HTTP and Iroh adapters.
//!
//! Diamond edits are interpreted at their declared causal parents. Simpleton
//! retains its coordinated-client rule and rejects stale positional mutations.
use crate::core::merge::diamond::DiamondCRDT;
pub use braid_http::durable::{
    atomic_write as atomic_write_private, document_lock as storage_lock,
};
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

mod history;
mod representation;
pub use history::{
    HistoryLimits, HistoryPage, HistoryRequest, MAX_HISTORY_PAGE_BYTES, MAX_HISTORY_PAGE_EVENTS,
    MAX_HISTORY_REQUEST_BYTES, REPLICATION_HISTORY_PATH, REPLICATION_NAMESPACE,
};

#[derive(Debug, thiserror::Error)]
pub enum ReplicaError {
    #[error("{0}")]
    Invalid(String),
    #[error("{0}")]
    Conflict(String),
    #[error("Requested history is unavailable: {0:?}")]
    HistoryUnavailable(Vec<Version>),
    #[error("{0}")]
    Limit(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Access(#[from] crate::access::AccessError),
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
    a.iter().collect::<std::collections::HashSet<_>>()
        == b.iter().collect::<std::collections::HashSet<_>>()
}
impl ReplicaDocument {
    /// Original events in dependency order, with a causally closed continuation.
    pub fn history_page(
        &self,
        from: &[Version],
        target: Option<&[Version]>,
        limits: HistoryLimits,
    ) -> Result<HistoryPage, ReplicaError> {
        let current: Vec<_> = self.heads.iter().map(Version::new).collect();
        history::page(self, from, target.unwrap_or(&current), limits)
    }
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
            content_type: representation::media_type(self),
            ..Update::default()
        }
    }

    /// Materialize a known causal frontier without changing current state.
    pub fn snapshot_at(&self, frontier: &[Version]) -> Result<Update, ReplicaError> {
        if self.merge_type == representation::JSON_SNAPSHOT_PROFILE {
            return representation::snapshot_at(self, frontier);
        }
        let ids: Vec<_> = frontier.iter().map(ToString::to_string).collect();
        if same_frontier(&ids, &self.heads) {
            return Ok(self.snapshot());
        }
        self.ancestors(frontier)?;
        if self.merge_state.is_null() {
            return Err(ReplicaError::HistoryUnavailable(frontier.to_vec()));
        }
        let crdt =
            DiamondCRDT::restore_checkpoint(&self.merge_state).map_err(ReplicaError::Invalid)?;
        let content = crdt
            .content_at_parents(frontier)
            .map_err(|_| ReplicaError::HistoryUnavailable(frontier.to_vec()))?;
        Ok(Update {
            version: frontier.to_vec(),
            body: Some(content.into()),
            merge_type: Some(self.merge_type.clone()),
            content_type: representation::media_type(self),
            ..Update::default()
        })
    }

    pub(crate) fn ancestors(
        &self,
        frontier: &[Version],
    ) -> Result<std::collections::HashSet<String>, ReplicaError> {
        if frontier
            .iter()
            .any(|v| !v.is_string() || v.to_string().is_empty())
        {
            return Err(ReplicaError::Invalid("Invalid history frontier".into()));
        }
        let mut pending: Vec<_> = frontier.iter().map(|v| (v.to_string(), false)).collect();
        let mut known = std::collections::HashSet::new();
        let mut active = std::collections::HashSet::new();
        while let Some((id, expanded)) = pending.pop() {
            if expanded {
                active.remove(&id);
                known.insert(id);
                continue;
            }
            if known.contains(&id) {
                continue;
            }
            if !active.insert(id.clone()) {
                return Err(ReplicaError::Invalid(
                    "Cycle in persisted version graph".into(),
                ));
            }
            let parents = self
                .version_graph
                .get(&id)
                .ok_or_else(|| ReplicaError::HistoryUnavailable(vec![Version::new(&id)]))?;
            pending.push((id, true));
            pending.extend(parents.iter().map(|p| (p.clone(), false)));
        }
        Ok(known)
    }

    /// Original events in dependency order, never positional patches rebased by
    /// arrival order. Both boundaries must describe a reachable history interval.
    pub fn causal_history(
        &self,
        from: &[Version],
        to: &[Version],
    ) -> Result<Vec<Update>, ReplicaError> {
        let base = self.ancestors(from)?;
        let target = self.ancestors(to)?;
        if !base.is_subset(&target) {
            return Err(ReplicaError::Invalid(
                "History boundaries are not causally ordered".into(),
            ));
        }
        let mut events = Vec::new();
        let mut bytes = 0usize;
        for event in self.history.iter().filter(|u| {
            u.version
                .iter()
                .any(|v| target.contains(&v.to_string()) && !base.contains(&v.to_string()))
        }) {
            bytes = bytes.saturating_add(serde_json::to_vec(event)?.len());
            if events.len() >= 1024 || bytes > 32 * 1024 * 1024 {
                return Err(ReplicaError::Limit(
                    "History response exceeds 1024 events or 32 MiB".into(),
                ));
            }
            events.push(event.clone());
        }
        let retained: std::collections::HashSet<_> = events
            .iter()
            .flat_map(|u| u.version.iter().map(ToString::to_string))
            .collect();
        if target.difference(&base).any(|id| !retained.contains(id)) {
            return Err(ReplicaError::HistoryUnavailable(to.to_vec()));
        }
        Ok(events)
    }
    /// Validate the entire update on a candidate; callers persist before publishing it.
    pub fn apply(&mut self, update: &Update) -> Result<bool, ReplicaError> {
        self.apply_profile(update, false)
    }

    fn apply_profile(&mut self, update: &Update, transforming: bool) -> Result<bool, ReplicaError> {
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
        if !matches!(
            self.merge_type.as_str(),
            "simpleton" | "diamond" | "json-snapshot"
        ) || update
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
        if self.merge_type == representation::JSON_SNAPSHOT_PROFILE {
            representation::apply_snapshot(self, update)?;
            self.heads = vec![id.clone()];
            self.version_graph.insert(id.clone(), parents);
            self.fingerprints.insert(id, fingerprint);
            self.history.push(update.clone());
            self.modified_at = now();
            return Ok(true);
        }
        if self.merge_type == "simpleton" && !transforming && !same_frontier(&self.heads, &parents)
        {
            return Err(ReplicaError::Conflict(
                "Stale Simpleton positional update requires explicit reconciliation".into(),
            ));
        }
        let mut crdt = if self.merge_state.is_null() {
            // Snapshot-only migration has one known base; historical missing
            // checkpoints stay unavailable instead of being fabricated as root.
            let mut crdt = DiamondCRDT::new(format!(
                "legacy-{:x}",
                Sha256::digest(self.content.as_bytes())
            ));
            if !self.content.is_empty() {
                crdt.add_insert(0, &self.content);
            }
            for head in &self.heads {
                crdt.register_version_mapping(head.clone(), crdt.get_local_frontier());
            }
            crdt
        } else {
            DiamondCRDT::restore_checkpoint(&self.merge_state).map_err(ReplicaError::Invalid)?
        };
        let base = crdt
            .content_at_parents(&update.parents)
            .map_err(ReplicaError::Conflict)?;
        let content = match (&update.body, &update.patches) {
            (Some(body), None) => std::str::from_utf8(body)
                .map_err(|e| ReplicaError::Invalid(e.to_string()))?
                .to_owned(),
            (None, Some(patches)) => braid_http::protocol::text::apply_text_patches(&base, patches)
                .map_err(|e| ReplicaError::Invalid(e.to_string()))?,
            _ => return Err(ReplicaError::Invalid("Expected snapshot or patches".into())),
        };
        validate_digest(&content, update)?;
        crdt.apply_wire_event(update)
            .map_err(ReplicaError::Invalid)?;
        let checkpoint = crdt.durable_checkpoint().map_err(ReplicaError::Invalid)?;
        self.content = crdt.content();
        self.merge_state = checkpoint;
        self.heads.retain(|head| !parents.contains(head));
        self.heads.push(id.clone());
        self.heads.sort();
        self.heads.dedup();
        self.version_graph.insert(id.clone(), parents);
        self.fingerprints.insert(id, fingerprint);
        self.history.push(update.clone());
        self.modified_at = now();
        Ok(true)
    }
    /// A snapshot is a valid replacement when old history predates migration.
    pub fn replay_since(&self, frontier: &[String]) -> Result<Vec<Update>, ReplicaError> {
        self.ancestors(&frontier.iter().map(Version::new).collect::<Vec<_>>())?;
        if same_frontier(frontier, &self.heads) {
            return Ok(vec![]);
        }
        if frontier.len() == 1 {
            if let Some(i) = self
                .history
                .iter()
                .position(|u| u.version.iter().any(|v| v.to_string() == frontier[0]))
            {
                let tail = &self.history[i + 1..];
                let mut cursor = frontier.to_vec();
                for update in tail {
                    let parents = update
                        .parents
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>();
                    if !same_frontier(&parents, &cursor) {
                        return Ok(vec![self.snapshot()]);
                    }
                    cursor = update.version.iter().map(ToString::to_string).collect();
                }
                return Ok(tail.to_vec());
            }
        }
        Ok(vec![self.snapshot()])
    }
}

#[derive(Clone)]
pub struct ReplicaStore {
    base_dir: PathBuf,
    lease: Arc<tokio::sync::OnceCell<Arc<std::fs::File>>>,
}
impl ReplicaStore {
    pub async fn representation_at(
        &self,
        resource: &str,
        frontier: Option<&[Version]>,
    ) -> Result<Option<braid_http::types::RepresentationAt>, ReplicaError> {
        self.load(resource)
            .await?
            .map(|document| representation::representation_at(&document, frontier))
            .transpose()
    }

    pub async fn representation_at_authorized(
        &self,
        resource: &str,
        frontier: Option<&[Version]>,
        policies: &crate::access::PolicyStore,
        principal: Option<&crate::access::Principal>,
    ) -> Result<Option<braid_http::types::RepresentationAt>, ReplicaError> {
        policies
            .authorize(resource, principal, crate::access::Action::Read)
            .await?;
        let representation = self.representation_at(resource, frontier).await?;
        policies
            .authorize(resource, principal, crate::access::Action::Read)
            .await?;
        Ok(representation)
    }
    pub async fn history_page(
        &self,
        resource: &str,
        from: &[Version],
        target: Option<&[Version]>,
        limits: HistoryLimits,
    ) -> Result<Option<HistoryPage>, ReplicaError> {
        self.load(resource)
            .await?
            .map(|document| document.history_page(from, target, limits))
            .transpose()
    }

    /// Authorize before reading or revealing any history metadata.
    pub async fn history_page_authorized(
        &self,
        resource: &str,
        from: &[Version],
        target: Option<&[Version]>,
        limits: HistoryLimits,
        policies: &crate::access::PolicyStore,
        principal: Option<&crate::access::Principal>,
    ) -> Result<Option<HistoryPage>, ReplicaError> {
        policies
            .authorize(resource, principal, crate::access::Action::Read)
            .await?;
        let page = self.history_page(resource, from, target, limits).await?;
        policies
            .authorize(resource, principal, crate::access::Action::Read)
            .await?;
        Ok(page)
    }

    /// Authenticated causal events are not subscriber-relative projections.
    /// This profile preserves their parents and identity without manufacturing
    /// a Peer header merely to opt into historical-parent transformation.
    pub async fn commit_causal_authorized(
        &self,
        resource: &str,
        update: &Update,
        merge_type: &str,
        policies: &crate::access::PolicyStore,
        principal: &crate::access::Principal,
    ) -> Result<ReplicaDocument, ReplicaError> {
        self.commit_profile(
            resource,
            update,
            merge_type,
            true,
            Some((policies, principal)),
        )
        .await
    }
    pub async fn snapshot_at(
        &self,
        resource: &str,
        frontier: &[Version],
    ) -> Result<Option<Update>, ReplicaError> {
        self.load(resource)
            .await?
            .map(|document| document.snapshot_at(frontier))
            .transpose()
    }
    pub fn new(base_dir: PathBuf) -> Result<Self, ReplicaError> {
        Ok(Self {
            base_dir: std::path::absolute(base_dir)?,
            lease: Default::default(),
        })
    }
    /// Resource policy directory for transports owning this replica root.
    pub fn policy_store(&self) -> Result<crate::access::PolicyStore, crate::access::AccessError> {
        crate::access::PolicyStore::new(self.base_dir.join(".resource-policies"))
    }
    async fn ensure_writer(&self) -> Result<(), ReplicaError> {
        self.lease
            .get_or_try_init(|| async {
                braid_http::durable::shared_writer_lease(&self.base_dir.join(".writer.lock")).await
            })
            .await?;
        Ok(())
    }
    /// Existing resources or policies that the internal replication namespace
    /// would shadow. The endpoint stays disabled until this list is empty;
    /// nothing is renamed, deleted, or adopted implicitly.
    pub async fn replication_namespace_collisions(&self) -> Result<Vec<String>, ReplicaError> {
        #[derive(Deserialize)]
        struct Named {
            resource: Option<String>,
        }
        let mut collisions = std::collections::BTreeSet::new();
        let legacy_prefix = REPLICATION_NAMESPACE.replace(['/', '\\'], "_");
        match tokio::fs::read_dir(&self.base_dir).await {
            Ok(mut directory) => {
                while let Some(entry) = directory.next_entry().await? {
                    let name = entry.file_name().to_string_lossy().into_owned();
                    if name.starts_with(&legacy_prefix) {
                        collisions.insert(name);
                    } else if name.starts_with("p-")
                        && name.ends_with(".json")
                        && entry.file_type().await?.is_file()
                    {
                        let named: Named =
                            serde_json::from_slice(&tokio::fs::read(entry.path()).await?)?;
                        if let Some(resource) = named
                            .resource
                            .filter(|resource| resource.starts_with(REPLICATION_NAMESPACE))
                        {
                            collisions.insert(resource);
                        }
                    }
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        for policy in self.policy_store()?.inventory().await? {
            if policy.resource.starts_with(REPLICATION_NAMESPACE) {
                collisions.insert(policy.resource);
            }
        }
        Ok(collisions.into_iter().collect())
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
        self.ensure_writer().await?;
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
        self.commit_profile(resource, update, merge_type, false, None)
            .await
    }

    /// The caller must pair this profile with per-peer transformed subscriptions.
    /// The conservative positional API retains its existing conflict behavior.
    pub async fn commit_transforming_simpleton(
        &self,
        resource: &str,
        update: &Update,
    ) -> Result<ReplicaDocument, ReplicaError> {
        self.commit_profile(resource, update, "simpleton", true, None)
            .await
    }

    /// Network mutation boundary. Existing ownerless data cannot be claimed by
    /// a request; only genuinely new data receives a recoverable private policy.
    pub async fn commit_authorized(
        &self,
        resource: &str,
        update: &Update,
        merge_type: &str,
        policies: &crate::access::PolicyStore,
        principal: &crate::access::Principal,
    ) -> Result<ReplicaDocument, ReplicaError> {
        let transforming = merge_type == "simpleton"
            && update
                .extra_headers
                .get("peer")
                .map(|peer| braid_http::protocol::parse_peer(peer))
                .transpose()
                .map_err(|error| ReplicaError::Invalid(error.to_string()))?
                .is_some();
        self.commit_profile(
            resource,
            update,
            merge_type,
            transforming,
            Some((policies, principal)),
        )
        .await
    }

    async fn commit_profile(
        &self,
        resource: &str,
        update: &Update,
        merge_type: &str,
        transforming: bool,
        authority: Option<(&crate::access::PolicyStore, &crate::access::Principal)>,
    ) -> Result<ReplicaDocument, ReplicaError> {
        let _guard = document_lock(self.path(resource)).await;
        let stored = self.load(resource).await?;
        if stored.is_none() && resource.starts_with(REPLICATION_NAMESPACE) {
            return Err(ReplicaError::Invalid(
                "The replication namespace is reserved for internal protocol endpoints".into(),
            ));
        }
        let mut permit = match authority {
            Some((policies, principal)) => Some(
                policies
                    .begin_write(resource, principal, stored.is_none())
                    .await?,
            ),
            None => None,
        };
        let mut document = stored.unwrap_or_else(|| ReplicaDocument::empty(merge_type));
        if document.merge_type != merge_type {
            return Err(ReplicaError::Conflict(
                "Resource merge profile does not match the requested profile".into(),
            ));
        }
        if document.apply_profile(update, transforming)? {
            document.resource = Some(resource.to_owned());
            if let (Some(permit), Some((policies, _))) = (&mut permit, authority) {
                permit.reserve(policies).await?;
            }
            atomic_write(&self.path(resource), &serde_json::to_vec(&document)?).await?;
            let mut broadcast = update.clone();
            broadcast.current_version = Some(document.heads.iter().map(Version::new).collect());
            let _ = self.channel(resource).send(broadcast);
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

fn validate_digest(content: &str, update: &Update) -> Result<(), ReplicaError> {
    if let Some(digest) = update.extra_headers.get("repr-digest") {
        use base64::Engine;
        let actual =
            base64::engine::general_purpose::STANDARD.encode(Sha256::digest(content.as_bytes()));
        if digest.trim() != format!("sha-256=:{actual}:") {
            return Err(ReplicaError::Invalid(
                "Representation digest mismatch or unsupported digest algorithm".into(),
            ));
        }
    }
    Ok(())
}
