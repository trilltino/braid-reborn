//! Conflict resolution for Simpleton CRDT merges.
//!
//! This module handles incoming updates with the "simpleton" merge-type, applying
//! CRDT operations and returning merged results. It bridges Braid-HTTP protocol
//! updates with the underlying simpleton CRDT engine.
//!
//! # Request/Response Formats
//!
//! **Plain Text Updates:**
//! - Inserts text at position 0
//! - Body can be plain text or JSON
//!
//! **Structured JSON Updates:**
//! - `"inserts"`: Array of `{pos, text}` objects
//! - `"deletes"`: Array of `{start, end}` objects
//! - All operations are applied and merged into the document

use crate::core::server::ResourceStateManager;
use crate::core::{Update, Version};
use serde_json::{json, Value};

/// Handles conflict resolution using Diamond-Types CRDT.
///
/// The conflict resolver intercepts updates marked with `merge-type: "diamond"`,
/// applies them to the appropriate resource's CRDT, and returns the merged result.
/// This ensures deterministic convergence across all peers.
///
/// # Request/Response Formats
///
/// **Plain Text Updates:**
/// - Inserts text at position 0
/// - Body can be plain text or JSON
///
/// **Structured JSON Updates:**
/// - `"inserts"`: Array of `{pos, text}` objects
/// - `"deletes"`: Array of `{start, end}` objects
/// - All operations are applied and merged into the CRDT
#[derive(Clone)]
pub struct ConflictResolver {
    /// Manages per-resource CRDT state
    resource_manager: ResourceStateManager,
}

impl ConflictResolver {
    /// Create a new conflict resolver with the given resource manager.
    ///
    /// # Arguments
    ///
    /// * `resource_manager` - The centralized resource state registry
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use crate::core::server::{ConflictResolver, ResourceStateManager};
    ///
    /// let manager = ResourceStateManager::new();
    /// let resolver = ConflictResolver::new(manager);
    /// ```
    #[must_use]
    pub fn new(resource_manager: ResourceStateManager) -> Self {
        Self { resource_manager }
    }

    /// Resolve an update by applying CRDT semantics if needed.
    ///
    /// If the update has `merge-type: "diamond"`, it's applied to the resource's CRDT.
    /// Otherwise, the update is returned unchanged (no merge strategy applied).
    ///
    /// # Arguments
    ///
    /// * `resource_id` - The resource being updated
    /// * `update` - The incoming Braid update
    /// * `agent_id` - Origin agent identifier
    ///
    /// # Returns
    ///
    /// The resolved update with merged content and current version.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let resolver = ConflictResolver::new(manager);
    /// let update = Update::snapshot(Version::new("v1"), "hello")
    ///     .with_merge_type("diamond");
    /// let result = resolver.resolve_update("doc1", &update, "alice").await?;
    /// ```
    pub async fn resolve_update(
        &self,
        resource_id: &str,
        update: &Update,
        agent_id: &str,
    ) -> Result<Update, String> {
        match &update.merge_type {
            Some(merge_type) if merge_type == "simpleton" || merge_type == "braid-text" || merge_type == "diamond" => {
                self.resolve_text_merge(resource_id, update, agent_id)
                    .await
            }
            _ => Ok(update.clone()),
        }
    }

    /// Apply and merge a text update.
    ///
    /// Detects whether the body is:
    /// - Plain text (applies as insertion at position 0)
    /// - Structured JSON with operation arrays (applies each operation)
    ///
    /// # Arguments
    ///
    /// * `resource_id` - Resource to update
    /// * `update` - The Braid update with body
    /// * `agent_id` - Origin agent
    ///
    /// # Returns
    ///
    /// A new update with merged content and current version.
    async fn resolve_text_merge(
        &self,
        resource_id: &str,
        update: &Update,
        agent_id: &str,
    ) -> Result<Update, String> {
        if let Some(body_bytes) = &update.body {
            let body_str = String::from_utf8_lossy(body_bytes);

            if body_str.starts_with('{') && body_str.ends_with('}') {
                if let Ok(json_data) = serde_json::from_str::<Value>(&body_str) {
                    if json_data.is_object() {
                        let version_id = update.version.get(0).map(|v| v.to_string());
                        let parents_vec: Vec<String> =
                            update.parents.iter().map(|v| v.to_string()).collect();
                        let parents = if parents_vec.is_empty() {
                            None
                        } else {
                            Some(parents_vec.as_slice())
                        };
                        return self
                            .handle_text_json(
                                resource_id,
                                &json_data,
                                agent_id,
                                version_id.as_deref(),
                                parents,
                            )
                            .await;
                    }
                }
            }

            let version_id = update.version.get(0).map(|v| v.to_string());
            let version_ref = version_id.as_deref();

            let _ = self.resource_manager.apply_update(
                resource_id,
                &body_str,
                agent_id,
                version_ref,
                None, // Use existing merge type
            )?;
        }

        let update = self.build_merged_response(resource_id, agent_id).await?;

        // Register the new version mapping
        if let Some(v) = update.version.get(0) {
            let frontier = {
                let resource = self.resource_manager.get_resource(resource_id).unwrap();
                let state = resource.lock();
                state.crdt.get_local_frontier()
            };
            self.resource_manager
                .register_version_mapping(resource_id, v.to_string(), frontier);
        }

        Ok(update)
    }

    /// Retrieve history for a resource since specific versions.
    pub async fn get_history(
        &self,
        resource_id: &str,
        since_versions: &[&str],
    ) -> Result<Vec<Update>, String> {
        let serialized_ops_list = self
            .resource_manager
            .get_history(resource_id, since_versions)?;

        let mut updates = Vec::new();
        for ops in serialized_ops_list {
            // Convert internal ops to Braid updates
            // (Note: This is a simplified conversion)
            // For now, we'll return a special Update that carries the ops
            // In a real Braid system, these would be converted to application/braid-patch
            let update = Update::snapshot(
                crate::core::Version::new("history-delta"),
                serde_json::to_vec(&ops).map_err(|e| e.to_string())?,
            );
            updates.push(update);
        }

        Ok(updates)
    }

    /// Parse and apply structured JSON operations.
    ///
    /// Expected JSON format:
    /// ```json
    /// {
    ///   "inserts": [{"pos": 0, "text": "hello"}],
    ///   "deletes": [{"start": 5, "end": 6}]
    /// }
    /// ```
    ///
    /// # Arguments
    ///
    /// * `resource_id` - Resource to update
    /// * `json_data` - Parsed JSON operations
    /// * `agent_id` - Origin agent
    ///
    /// # Returns
    ///
    /// A response update with merged state.
    async fn handle_text_json(
        &self,
        resource_id: &str,
        json_data: &Value,
        agent_id: &str,
        version_id: Option<&str>,
        parents: Option<&[String]>,
    ) -> Result<Update, String> {
        let parent_strs: Option<Vec<&str>> =
            parents.map(|p| p.iter().map(|s| s.as_str()).collect());
        let parent_refs = parent_strs.as_ref().map(|v| v.as_slice());

        self.apply_insert_operations(resource_id, json_data, agent_id, version_id, parent_refs);
        self.apply_delete_operations(resource_id, json_data, agent_id, version_id, parent_refs);

        let merged_state = self
            .resource_manager
            .get_resource_state(resource_id)
            .ok_or_else(|| "Failed to retrieve resource state after merge".to_string())?;

        let merged_content = extract_string(&merged_state, "content", "");
        let quality = self
            .resource_manager
            .get_merge_quality(resource_id)
            .unwrap_or(0);
        let version_str = extract_string(&merged_state, "version", &format!("merged-{}", agent_id));

        let response_body = json!({
            "content": merged_content,
            "merge_quality": quality,
            "agents": [agent_id],
        })
        .to_string();

        Ok(Update::snapshot(Version::new(&version_str), response_body).with_merge_type("diamond"))
    }

    /// Apply insertion operations from JSON array.
    ///
    /// Silently skips malformed operations.
    fn apply_insert_operations(
        &self,
        resource_id: &str,
        json_data: &Value,
        agent_id: &str,
        version_id: Option<&str>,
        parents: Option<&[&str]>,
    ) {
        if let Some(inserts) = json_data.get("inserts").and_then(|v| v.as_array()) {
            for insert in inserts {
                if let (Some(pos), Some(text)) = (
                    insert.get("pos").and_then(|v| v.as_u64()),
                    insert.get("text").and_then(|v| v.as_str()),
                ) {
                    if let Some(p) = parents {
                        let _ = self.resource_manager.apply_remote_insert_versioned(
                            resource_id,
                            agent_id,
                            p,
                            pos as usize,
                            text,
                            version_id,
                            Some("diamond"),
                        );
                    } else {
                        let _ = self.resource_manager.apply_remote_insert(
                            resource_id,
                            agent_id,
                            pos as usize,
                            text,
                            version_id,
                            Some("diamond"),
                        );
                    }
                }
            }
        }
    }

    /// Apply deletion operations from JSON array.
    ///
    /// Silently skips malformed operations.
    fn apply_delete_operations(
        &self,
        resource_id: &str,
        json_data: &Value,
        agent_id: &str,
        version_id: Option<&str>,
        parents: Option<&[&str]>,
    ) {
        if let Some(deletes) = json_data.get("deletes").and_then(|v| v.as_array()) {
            for delete in deletes {
                if let (Some(start), Some(end)) = (
                    delete.get("start").and_then(|v| v.as_u64()),
                    delete.get("end").and_then(|v| v.as_u64()),
                ) {
                    if let Some(p) = parents {
                        let _ = self.resource_manager.apply_remote_delete_versioned(
                            resource_id,
                            agent_id,
                            p,
                            (start as usize)..(end as usize),
                            version_id,
                            Some("diamond"),
                        );
                    } else {
                        let _ = self.resource_manager.apply_remote_delete(
                            resource_id,
                            agent_id,
                            start as usize,
                            end as usize,
                            version_id,
                            Some("diamond"),
                        );
                    }
                }
            }
        }
    }

    /// Build a Braid response with merged content.
    ///
    /// Extracts content and version from the current resource state.
    async fn build_merged_response(
        &self,
        resource_id: &str,
        agent_id: &str,
    ) -> Result<Update, String> {
        let merged_state = self
            .resource_manager
            .get_resource_state(resource_id)
            .ok_or_else(|| "Failed to retrieve merged resource state".to_string())?;

        let merged_content = extract_string(&merged_state, "content", "");
        let version_str = extract_string(&merged_state, "version", &format!("merged-{}", agent_id));

        Ok(Update::snapshot(Version::new(&version_str), merged_content).with_merge_type("diamond"))
    }

    /// Get the current content of a resource.
    ///
    /// # Arguments
    ///
    /// * `resource_id` - Resource to query
    ///
    /// # Returns
    ///
    /// Current document text, or `None` if the resource doesn't exist.
    #[inline]
    #[must_use]
    pub fn get_resource_content(&self, resource_id: &str) -> Option<String> {
        self.resource_manager
            .get_resource_state(resource_id)
            .and_then(|state| state["content"].as_str().map(|s| s.to_string()))
    }

    /// Get the current version of a resource.
    ///
    /// # Arguments
    ///
    /// * `resource_id` - Resource to query
    ///
    /// # Returns
    ///
    /// Current version identifier, or `None` if the resource doesn't exist.
    #[inline]
    #[must_use]
    pub fn get_resource_version(&self, resource_id: &str) -> Option<Version> {
        self.resource_manager
            .get_resource_state(resource_id)
            .and_then(|state| state["version"].as_str().map(Version::new))
    }
}

/// Helper to safely extract string values from JSON.
///
/// Returns the specified default if the field is missing or not a string.
fn extract_string(json: &Value, key: &str, default: &str) -> String {
    json.get(key)
        .and_then(|v| v.as_str())
        .unwrap_or(default)
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_resolve_non_diamond_update() {
        let manager = ResourceStateManager::new();
        let resolver = ConflictResolver::new(manager);

        let update = Update::snapshot(Version::new("v1"), "test content");
        let result = resolver.resolve_update("doc1", &update, "alice").await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_resolve_simpleton_update() {
        let manager = ResourceStateManager::new();
        let resolver = ConflictResolver::new(manager);

        let update =
            Update::snapshot(Version::new("v1"), "test content").with_merge_type("simpleton");

        let result = resolver.resolve_update("doc1", &update, "alice").await;
        assert!(result.is_ok());

        let resolved = result.unwrap();
        assert_eq!(resolved.merge_type, Some("simpleton".to_string()));
    }

    #[tokio::test]
    async fn test_concurrent_merges() {
        let manager = ResourceStateManager::new();
        let resolver = ConflictResolver::new(manager);

        let update1 = Update::snapshot(Version::new("v1"), "hello").with_merge_type("simpleton");
        let update2 = Update::snapshot(Version::new("v2"), "world").with_merge_type("simpleton");

        let _ = resolver.resolve_update("doc1", &update1, "alice").await;
        let result = resolver.resolve_update("doc1", &update2, "bob").await;

        assert!(result.is_ok());
    }
}
