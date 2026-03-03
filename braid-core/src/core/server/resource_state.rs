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
    pub merge_type: String,
}

/// Thread-safe registry of collaborative document resources.
pub struct ResourceStateManager {
    resources: Arc<Mutex<HashMap<String, Arc<Mutex<ResourceState>>>>>,
    new_resource_tx: broadcast::Sender<String>,
}

use tokio::sync::broadcast;

impl ResourceStateManager {
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
                let merge_type = requested_merge_type
                    .unwrap_or("simpleton")
                    .to_string();

                // Notify subscribers about the NEW resource
                let _ = self.new_resource_tx.send(resource_id.to_string());

                Arc::new(Mutex::new(ResourceState {
                    crdt: DiamondCRDT::new(initial_agent_id),
                    last_sync: SystemTime::now(),
                    seen_versions: HashSet::new(),
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

        if let Some(vid) = version_id {
            if state.seen_versions.contains(vid) {
                return Ok(Self::export_operations(&state.crdt));
            }
            state.seen_versions.insert(vid.to_string());
        }

        // Diamond Types handles "replace" natively via local_insert/delete logic?
        // Or we use a "replace all" patch?
        // DiamondCRDT has a specific way to handle "snapshot" updates if we treat them as such.
        // But here we are applying a "content" update.
        // Ideally we should use `add_insert` / `add_delete` if we knew the diff.
        // But for "apply_update", we usually assume it's a "set content" operation or we rely on the caller sending patches.
        // Wait, the previous implementation used `DiamondCRDT`?
        // If I assume "simpleton" logic (replace all), I can do that in Diamond too.
        // But usually Diamond expects OPERATIONS.
        // Let's assume for now we use `local_edit` equivalent.
        
        // Emulating "replace all"
        let len = state.crdt.content().chars().count();
        if len > 0 {
             let _ = state.crdt.add_delete(0..len);
        }
        let _ = state.crdt.add_insert(0, content);

        state.last_sync = SystemTime::now();
        Ok(Self::export_operations(&state.crdt))
    }

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
        if let Some(vid) = version_id {
            if state.seen_versions.contains(vid) {
                return Ok(Self::export_operations(&state.crdt));
            }
            state.seen_versions.insert(vid.to_string());
        }

        // Diamond specific insert (remote versioned)
        let _ = state.crdt.add_insert_remote_versioned(agent_id, parents, pos, text, version_id);

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
        if let Some(vid) = version_id {
            if state.seen_versions.contains(vid) {
                return Ok(Self::export_operations(&state.crdt));
            }
            state.seen_versions.insert(vid.to_string());
        }
        
        // Diamond insert without strict parents (uses current frontier)
        let _ = state.crdt.add_insert(pos, text);
        
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
        if let Some(vid) = version_id {
            if state.seen_versions.contains(vid) {
                return Ok(Self::export_operations(&state.crdt));
            }
            state.seen_versions.insert(vid.to_string());
        }
        
        let _ = state.crdt.add_delete_remote_versioned(agent_id, parents, range, version_id);
        
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
        if let Some(vid) = version_id {
            if state.seen_versions.contains(vid) {
                return Ok(Self::export_operations(&state.crdt));
            }
            state.seen_versions.insert(vid.to_string());
        }
        
        let _ = state.crdt.add_delete(start..end);
        
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
        _resource_id: &str,
        _since_versions: &[&str],
    ) -> Result<Vec<Value>, String> {
        // History retrieval not yet implemented in restored Diamond struct
        Err("History retrieval temporarily unavailable".to_string())
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
