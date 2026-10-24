//! Compatibility adapter for atomic Diamond text mutations.
use crate::core::server::ResourceStateManager;
use crate::core::{Update, Version};
use serde::Deserialize;

#[derive(Clone)]
pub struct ConflictResolver {
    resource_manager: ResourceStateManager,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Insert {
    pos: usize,
    text: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Delete {
    start: usize,
    end: usize,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Batch {
    #[serde(default)]
    inserts: Vec<Insert>,
    #[serde(default)]
    deletes: Vec<Delete>,
}
impl ConflictResolver {
    pub fn new(resource_manager: ResourceStateManager) -> Self {
        Self { resource_manager }
    }

    pub async fn resolve_update(
        &self,
        resource_id: &str,
        update: &Update,
        agent_id: &str,
    ) -> Result<Update, String> {
        if update.merge_type.as_deref() != Some("diamond") {
            return Ok(update.clone());
        }
        let batch = match &update.body {
            Some(body) => {
                let text = std::str::from_utf8(body).map_err(|e| e.to_string())?;
                let value = serde_json::from_str::<serde_json::Value>(text);
                match value {
                    Ok(value)
                        if value.get("inserts").is_some() || value.get("deletes").is_some() =>
                    {
                        Some(serde_json::from_value::<Batch>(value).map_err(|e| e.to_string())?)
                    }
                    _ => None,
                }
            }
            None => None,
        };
        let structured = batch.is_some();
        let mut result =
            self.resource_manager
                .apply_atomic(resource_id, update, agent_id, |candidate| {
                    if let Some(batch) = batch {
                        let mut content = candidate.content_at_parents(&update.parents)?;
                        // Legacy JSON coordinates are sequential. Refuse a stale base;
                        // canonical wire patches use their declared historical frontier.
                        if candidate.parent_frontier(&update.parents)?
                            != candidate.get_local_frontier()
                        {
                            return Err(
                                "Stale legacy positional batch requires reconciliation".into()
                            );
                        }
                        for insert in batch.inserts {
                            content = braid_http::protocol::text::apply_text_patches(
                                &content,
                                &[crate::Patch::text(
                                    format!("[{}:{}]", insert.pos, insert.pos),
                                    insert.text,
                                )],
                            )
                            .map_err(|e| e.to_string())?;
                        }
                        for delete in batch.deletes {
                            content = braid_http::protocol::text::apply_text_patches(
                                &content,
                                &[crate::Patch::text(
                                    format!("[{}:{}]", delete.start, delete.end),
                                    "",
                                )],
                            )
                            .map_err(|e| e.to_string())?;
                        }
                        let mut normalized = update.clone();
                        normalized.body = Some(content.into());
                        normalized.patches = None;
                        candidate.apply_wire_event(&normalized)
                    } else {
                        candidate.apply_wire_event(update)
                    }
                })?;
        if structured {
            result.body = Some(
                serde_json::json!({"content": result.body_str(), "agents": [agent_id]})
                    .to_string()
                    .into(),
            );
            result.content_type = Some("application/json".into());
        }
        Ok(result)
    }

    /// Legacy engine deltas retain their distinct media type and are not
    /// advertised as replayable Braid event history. Use ReplicaStore for that.
    pub async fn get_history(
        &self,
        resource_id: &str,
        since_versions: &[&str],
    ) -> Result<Vec<Update>, String> {
        self.resource_manager
            .get_history(resource_id, since_versions)?
            .into_iter()
            .map(|ops| {
                Ok(Update {
                    body: Some(serde_json::to_vec(&ops).map_err(|e| e.to_string())?.into()),
                    content_type: Some("application/vnd.braid.legacy-operations+json".into()),
                    ..Update::default()
                })
            })
            .collect()
    }
    pub fn get_resource_content(&self, resource_id: &str) -> Option<String> {
        self.resource_manager.get_resource_state(resource_id)?["content"]
            .as_str()
            .map(str::to_owned)
    }
    pub fn get_resource_version(&self, resource_id: &str) -> Option<Version> {
        self.resource_manager.get_resource_state(resource_id)?["version"]
            .as_str()
            .map(Version::new)
    }
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
