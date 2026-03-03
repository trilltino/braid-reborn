//! MergeType trait - Pluggable merge algorithm interface for Braid-HTTP.
//!
//! This module defines the core trait for merge algorithms that can be used
//! with Braid-HTTP resources. Multiple implementations can be registered
//! and selected per-resource.
//!
//! # Supported Merge Types
//!
//! | Name | Description |
//! |------|-------------|
//! | `\"diamond\"` | Diamond-types CRDT for text |
//! | `"diamond"` | Diamond-types CRDT for text |
//! | `"antimatter"` | Antimatter CRDT with pruning |
//! | Custom | Application-defined algorithms |

use serde_json::Value;
use std::collections::HashMap;
use std::fmt::Debug;

/// A patch representing a change to a resource.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MergePatch {
    /// Range or path specifier (e.g., "0:5" or ".foo.bar")
    pub range: String,
    /// The content to insert/replace
    pub content: Value,
    /// Version ID that created this patch
    pub version: Option<braid_http::types::Version>,
    /// Parent versions this patch depends on
    pub parents: Vec<braid_http::types::Version>,
}

impl MergePatch {
    /// Create a new merge patch.
    pub fn new(range: &str, content: Value) -> Self {
        Self {
            range: range.to_string(),
            content,
            version: None,
            parents: Vec::new(),
        }
    }

    /// Create with version info.
    pub fn with_version(range: &str, content: Value, version: braid_http::types::Version, parents: Vec<braid_http::types::Version>) -> Self {
        Self {
            range: range.to_string(),
            content,
            version: Some(version),
            parents,
        }
    }
}

/// Result of a merge operation.
#[derive(Debug, Clone)]
pub struct MergeResult {
    /// Whether the merge was successful
    pub success: bool,
    /// Rebased patches that can be sent to other clients
    pub rebased_patches: Vec<MergePatch>,
    /// The new version ID created (if any)
    pub version: Option<braid_http::types::Version>,
    /// Error message if merge failed
    pub error: Option<String>,
}

impl MergeResult {
    /// Create a successful merge result.
    pub fn success(version: Option<braid_http::types::Version>, rebased_patches: Vec<MergePatch>) -> Self {
        Self {
            success: true,
            rebased_patches,
            version,
            error: None,
        }
    }

    /// Create a failed merge result.
    pub fn failure(error: &str) -> Self {
        Self {
            success: false,
            rebased_patches: Vec::new(),
            version: None,
            error: Some(error.to_string()),
        }
    }
}

/// Trait for pluggable merge algorithms.
///
/// Implementations of this trait can be registered with the Braid-HTTP server
/// to handle merge operations for resources.
pub trait MergeType: Debug + Send + Sync {
    /// Get the name of this merge type (e.g., "diamond", "antimatter").
    fn name(&self) -> &str;

    /// Initialize the merge state with initial content.
    fn initialize(&mut self, content: &str) -> MergeResult;

    /// Apply a patch from a remote client.
    ///
    /// # Arguments
    /// * `patch` - The patch to apply
    ///
    /// # Returns
    /// MergeResult with rebased patches for other clients
    fn apply_patch(&mut self, patch: MergePatch) -> MergeResult;

    /// Apply a local edit and create a new version.
    ///
    /// # Arguments
    /// * `patch` - The local edit to apply
    ///
    /// # Returns
    /// MergeResult with the new version and patches to broadcast
    fn local_edit(&mut self, patch: MergePatch) -> MergeResult;

    /// Get the current content as a string.
    fn get_content(&self) -> String;

    /// Get the current version frontier.
    fn get_version(&self) -> Vec<braid_http::types::Version>;

    /// Get all known versions (for sync).
    fn get_all_versions(&self) -> HashMap<String, Vec<braid_http::types::Version>>;

    /// Prune old versions that are no longer needed.
    ///
    /// Called when all peers have acknowledged versions.
    fn prune(&mut self) -> bool;

    /// Check if this merge type supports history compression.
    fn supports_pruning(&self) -> bool {
        false
    }

    /// Clone this merge type instance.
    fn clone_box(&self) -> Box<dyn MergeType>;
}

impl Clone for Box<dyn MergeType> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

/// Registry for available merge types.
pub struct MergeTypeRegistry {
    factories: HashMap<String, Box<dyn Fn(&str) -> Box<dyn MergeType> + Send + Sync>>,
}

impl std::fmt::Debug for MergeTypeRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MergeTypeRegistry")
            .field(
                "registered_types",
                &self.factories.keys().collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl Default for MergeTypeRegistry {
    fn default() -> Self {
        Self {
            factories: HashMap::new(),
        }
    }
}

impl MergeTypeRegistry {
    /// Create a new registry with default merge types.
    pub fn new() -> Self {
        let mut registry = Self::default();

        // Register built-in types
        registry.register("simpleton", |peer_id| {
            Box::new(super::simpleton::SimpletonMergeType::new(peer_id))
        });

        // Register Diamond Types CRDT for true collaborative editing
        #[cfg(not(target_arch = "wasm32"))]
        registry.register("diamond", |peer_id| {
            Box::new(super::diamond::DiamondMergeType::new(peer_id))
        });

        registry
    }

    /// Register a merge type factory.
    pub fn register<F>(&mut self, name: &str, factory: F)
    where
        F: Fn(&str) -> Box<dyn MergeType> + Send + Sync + 'static,
    {
        self.factories.insert(name.to_string(), Box::new(factory));
    }

    /// Create an instance of a merge type by name.
    pub fn create(&self, name: &str, peer_id: &str) -> Option<Box<dyn MergeType>> {
        self.factories.get(name).map(|f| f(peer_id))
    }

    /// List available merge types.
    pub fn list(&self) -> Vec<String> {
        self.factories.keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
}
