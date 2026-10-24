//! Per-resource state management.

use crate::core::merge::diamond::DiamondCRDT;

use parking_lot::Mutex;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::SystemTime;

/// The state of a single collaborative resource.
#[derive(Debug, Clone)]
pub struct ResourceState {
    pub crdt: DiamondCRDT,
    pub last_sync: SystemTime,
    pub seen_versions: HashSet<String>,
    pub fingerprints: HashMap<String, String>,
    pub merge_type: String,
}

/// Thread-safe registry of collaborative document resources.
pub struct ResourceStateManager {
    resources: Arc<Mutex<HashMap<String, Arc<Mutex<ResourceState>>>>>,
    new_resource_tx: broadcast::Sender<String>,
}

use tokio::sync::broadcast;

impl ResourceStateManager {
    /// Compatibility mutations stage an independent engine and publish exactly
    /// once. The callback must not mutate external state or publish notifications.
    pub(crate) fn apply_atomic(
        &self,
        resource_id: &str,
        update: &crate::Update,
        agent_id: &str,
        apply: impl FnOnce(&mut DiamondCRDT) -> Result<(), String>,
    ) -> Result<crate::Update, String> {
        let [version] = update.version.as_slice() else {
            return Err("A mutation must identify exactly one event".into());
        };
        if !version.is_string()
            || version.to_string().is_empty()
            || update.parents.contains(version)
        {
            return Err("Invalid or self-parented event".into());
        }
        let mut parents = update.parents.clone();
        parents.sort_by_key(ToString::to_string);
        if parents.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err("Duplicate parents".into());
        }
        let id = version.to_string();
        let fingerprint =
            serde_json::to_string(&(agent_id, &parents, &update.body, &update.patches))
                .map_err(|e| e.to_string())?;
        // A rejected first mutation must not leave an empty indexed resource.
        // Existing resources release the registry immediately; a new resource
        // remains private until its complete candidate has validated.
        let mut registration = Some(self.resources.lock());
        let existing = registration
            .as_ref()
            .and_then(|resources| resources.get(resource_id).cloned());
        let resource = if let Some(resource) = existing {
            registration = None;
            resource
        } else {
            Arc::new(Mutex::new(ResourceState {
                crdt: DiamondCRDT::new(agent_id),
                last_sync: SystemTime::now(),
                seen_versions: HashSet::new(),
                fingerprints: HashMap::new(),
                merge_type: "diamond".into(),
            }))
        };
        let mut state = resource.lock();
        if state.merge_type != "diamond" {
            return Err("Merge type mismatch".into());
        }
        if let Some(previous) = state.fingerprints.get(&id) {
            if previous != &fingerprint {
                return Err("Version reused with a different operation".into());
            }
        } else {
            let mut candidate = state.crdt.clone();
            apply(&mut candidate)?;
            state.crdt = candidate;
            state.fingerprints.insert(id.clone(), fingerprint);
            state.seen_versions.insert(id);
            state.last_sync = SystemTime::now();
        }
        let response = crate::Update::snapshot(version.clone(), state.crdt.content())
            .with_merge_type("diamond")
            .with_parents(update.parents.clone());
        drop(state);
        if let Some(mut resources) = registration {
            resources.insert(resource_id.into(), resource);
            drop(resources);
            let _ = self.new_resource_tx.send(resource_id.into());
        }
        Ok(response)
    }

    #[must_use]
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(1024);
        Self {
            resources: Arc::new(Mutex::new(HashMap::new())),
            new_resource_tx: tx,
        }
    }

    #[must_use]
    pub fn get_or_create_resource(
        &self,
        resource_id: &str,
        initial_agent_id: &str,
        requested_merge_type: Option<&str>,
    ) -> Arc<Mutex<ResourceState>> {
        let mut resources = self.resources.lock();
        resources
            .entry(resource_id.to_string())
            .or_insert_with(|| {
                let merge_type = requested_merge_type.unwrap_or("simpleton").to_string();

                // Notify subscribers about the NEW resource
                let _ = self.new_resource_tx.send(resource_id.to_string());

                Arc::new(Mutex::new(ResourceState {
                    crdt: DiamondCRDT::new(initial_agent_id),
                    last_sync: SystemTime::now(),
                    seen_versions: HashSet::new(),
                    fingerprints: HashMap::new(),
                    merge_type,
                }))
            })
            .clone()
    }

    pub fn subscribe_to_indices(&self) -> broadcast::Receiver<String> {
        self.new_resource_tx.subscribe()
    }

    #[inline]
    #[must_use]
    pub fn get_resource(&self, resource_id: &str) -> Option<Arc<Mutex<ResourceState>>> {
        self.resources.lock().get(resource_id).cloned()
    }

    #[must_use]
    pub fn list_resources(&self) -> Vec<String> {
        self.resources.lock().keys().cloned().collect()
    }

    #[must_use]
    pub fn has_version(&self, resource_id: &str, version_id: &str) -> bool {
        self.get_resource(resource_id)
            .is_some_and(|r| r.lock().seen_versions.contains(version_id))
    }

    pub fn apply_update(
        &self,
        resource_id: &str,
        content: &str,
        agent_id: &str,
        version_id: Option<&str>,
        requested_merge_type: Option<&str>,
    ) -> Result<Value, String> {
        let resource = self.get_or_create_resource(resource_id, agent_id, requested_merge_type);
        let mut state = resource.lock();

        if let Some(req_mt) = requested_merge_type {
            if state.merge_type != req_mt {
                return Err(format!(
                    "Merge-type mismatch: {} vs {}",
                    state.merge_type, req_mt
                ));
            }
        }

        let fingerprint = serde_json::json!(["snapshot", content]).to_string();
        if let Some(vid) = version_id {
            if let Some(old) = state.fingerprints.get(vid) {
                if *old != fingerprint {
                    return Err("Version reused with a different operation".into());
                }
                return Ok(Self::export_operations(&state.crdt));
            }
        }

        let len = state.crdt.content().chars().count();
        if len > 0 {
            state.crdt.add_delete(0..len);
        }
        state.crdt.add_insert(0, content);

        if let Some(vid) = version_id {
            if state.crdt.resolve_version(vid).is_none() {
                let frontier = state.crdt.get_local_frontier();
                state
                    .crdt
                    .register_version_mapping(vid.to_owned(), frontier);
            }
            state.fingerprints.insert(vid.to_owned(), fingerprint);
            state.seen_versions.insert(vid.to_owned());
        }
        state.last_sync = SystemTime::now();
        Ok(Self::export_operations(&state.crdt))
    }

    #[allow(clippy::too_many_arguments)] // Retained public compatibility API.
    pub fn apply_remote_insert_versioned(
        &self,
        resource_id: &str,
        agent_id: &str,
        parents: &[&str],
        pos: usize,
        text: &str,
        version_id: Option<&str>,
        requested_merge_type: Option<&str>,
    ) -> Result<Value, String> {
        let resource = self.get_or_create_resource(resource_id, agent_id, requested_merge_type);
        let mut state = resource.lock();
        let fingerprint = serde_json::json!(["insert", agent_id, parents, pos, text]).to_string();
        if let Some(vid) = version_id {
            if let Some(old) = state.fingerprints.get(vid) {
                if *old != fingerprint {
                    return Err("Version reused with a different operation".into());
                }
                return Ok(Self::export_operations(&state.crdt));
            }
        }

        // Diamond specific insert (remote versioned)
        state
            .crdt
            .add_insert_remote_versioned(agent_id, parents, pos, text, version_id)?;

        if let Some(vid) = version_id {
            if state.crdt.resolve_version(vid).is_none() {
                let frontier = state.crdt.get_local_frontier();
                state
                    .crdt
                    .register_version_mapping(vid.to_owned(), frontier);
            }
            state.fingerprints.insert(vid.to_owned(), fingerprint);
            state.seen_versions.insert(vid.to_owned());
        }
        state.last_sync = SystemTime::now();
        Ok(Self::export_operations(&state.crdt))
    }

    fn export_operations(crdt: &DiamondCRDT) -> Value {
        crdt.export_operations()
    }

    pub fn apply_remote_insert(
        &self,
        resource_id: &str,
        agent_id: &str,
        pos: usize,
        text: &str,
        version_id: Option<&str>,
        requested_merge_type: Option<&str>,
    ) -> Result<Value, String> {
        let resource = self.get_or_create_resource(resource_id, agent_id, requested_merge_type);
        let mut state = resource.lock();
        if let Some(req_mt) = requested_merge_type {
            if state.merge_type != req_mt {
                return Err(format!(
                    "Merge-type mismatch: {} vs {}",
                    state.merge_type, req_mt
                ));
            }
        }
        let fingerprint = serde_json::json!(["insert", agent_id, pos, text]).to_string();
        if let Some(vid) = version_id {
            if let Some(old) = state.fingerprints.get(vid) {
                if *old != fingerprint {
                    return Err("Version reused with a different operation".into());
                }
                return Ok(Self::export_operations(&state.crdt));
            }
        }

        if pos > state.crdt.content().chars().count() {
            return Err("Insertion outside document".into());
        }
        // Diamond insert without strict parents (uses current frontier)
        state.crdt.add_insert(pos, text);

        if let Some(vid) = version_id {
            if state.crdt.resolve_version(vid).is_none() {
                let frontier = state.crdt.get_local_frontier();
                state
                    .crdt
                    .register_version_mapping(vid.to_owned(), frontier);
            }
            state.fingerprints.insert(vid.to_owned(), fingerprint);
            state.seen_versions.insert(vid.to_owned());
        }
        state.last_sync = SystemTime::now();
        Ok(Self::export_operations(&state.crdt))
    }

    pub fn apply_remote_delete_versioned(
        &self,
        resource_id: &str,
        agent_id: &str,
        parents: &[&str],
        range: std::ops::Range<usize>,
        version_id: Option<&str>,
        requested_merge_type: Option<&str>,
    ) -> Result<Value, String> {
        let resource = self.get_or_create_resource(resource_id, agent_id, requested_merge_type);
        let mut state = resource.lock();
        let fingerprint =
            serde_json::json!(["delete", agent_id, parents, range.start, range.end]).to_string();
        if let Some(vid) = version_id {
            if let Some(old) = state.fingerprints.get(vid) {
                if *old != fingerprint {
                    return Err("Version reused with a different operation".into());
                }
                return Ok(Self::export_operations(&state.crdt));
            }
        }

        state
            .crdt
            .add_delete_remote_versioned(agent_id, parents, range, version_id)?;

        if let Some(vid) = version_id {
            if state.crdt.resolve_version(vid).is_none() {
                let frontier = state.crdt.get_local_frontier();
                state
                    .crdt
                    .register_version_mapping(vid.to_owned(), frontier);
            }
            state.fingerprints.insert(vid.to_owned(), fingerprint);
            state.seen_versions.insert(vid.to_owned());
        }
        state.last_sync = SystemTime::now();
        Ok(Self::export_operations(&state.crdt))
    }

    pub fn apply_remote_delete(
        &self,
        resource_id: &str,
        agent_id: &str,
        start: usize,
        end: usize,
        version_id: Option<&str>,
        requested_merge_type: Option<&str>,
    ) -> Result<Value, String> {
        let resource = self.get_or_create_resource(resource_id, agent_id, requested_merge_type);
        let mut state = resource.lock();
        if let Some(req_mt) = requested_merge_type {
            if state.merge_type != req_mt {
                return Err(format!(
                    "Merge-type mismatch: {} vs {}",
                    state.merge_type, req_mt
                ));
            }
        }
        let fingerprint = serde_json::json!(["delete", agent_id, start, end]).to_string();
        if let Some(vid) = version_id {
            if let Some(old) = state.fingerprints.get(vid) {
                if *old != fingerprint {
                    return Err("Version reused with a different operation".into());
                }
                return Ok(Self::export_operations(&state.crdt));
            }
        }

        if start > end || end > state.crdt.content().chars().count() {
            return Err("Deletion outside document".into());
        }
        state.crdt.add_delete(start..end);

        if let Some(vid) = version_id {
            if state.crdt.resolve_version(vid).is_none() {
                let frontier = state.crdt.get_local_frontier();
                state
                    .crdt
                    .register_version_mapping(vid.to_owned(), frontier);
            }
            state.fingerprints.insert(vid.to_owned(), fingerprint);
            state.seen_versions.insert(vid.to_owned());
        }
        state.last_sync = SystemTime::now();
        Ok(Self::export_operations(&state.crdt))
    }

    #[inline]
    #[must_use]
    pub fn get_resource_state(&self, resource_id: &str) -> Option<Value> {
        self.get_resource(resource_id)
            .map(|r| Self::export_operations(&r.lock().crdt))
    }

    #[inline]
    #[must_use]
    pub fn get_merge_quality(&self, _resource_id: &str) -> Option<u32> {
        // Delegate to Diamond CRDT
        // Diamond types handles merge quality? No, but let's assume 100
        Some(100)
    }

    pub fn register_version_mapping(
        &self,
        resource_id: &str,
        version: String,
        frontier: crate::vendor::diamond_types::Frontier,
    ) {
        if let Some(r) = self.get_resource(resource_id) {
            let mut state = r.lock();
            state.crdt.register_version_mapping(version, frontier);
        }
    }

    pub fn get_history(
        &self,
        resource_id: &str,
        since_versions: &[&str],
    ) -> Result<Vec<Value>, String> {
        let resource = self.get_resource(resource_id).ok_or("Unknown resource")?;
        let result = resource.lock().crdt.history_since(since_versions);
        result
    }
}

impl Clone for ResourceStateManager {
    fn clone(&self) -> Self {
        Self {
            resources: Arc::clone(&self.resources),
            new_resource_tx: self.new_resource_tx.clone(),
        }
    }
}

impl Default for ResourceStateManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_resource_manager() {
        let manager = ResourceStateManager::new();
        manager
            .apply_update("doc1", "hello", "alice", None, None)
            .unwrap();
        assert_eq!(
            manager.get_resource_state("doc1").unwrap()["content"],
            "hello"
        );
    }
}
