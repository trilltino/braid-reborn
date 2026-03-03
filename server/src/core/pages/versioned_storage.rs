//! Versioned page storage with full version graph support.
//!
//! This module provides JSON-based storage for pages with:
//! - Full version graph (version -> parents mapping)
//! - Content-addressed storage
//! - Support for multiple merge types (simpleton, diamond)
//! - Parent validation for causal consistency

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tokio::fs;
use tracing::{info, warn};

use braid_core::core::merge::merge_type::{MergePatch, MergeType};
use braid_core::core::merge::MergeTypeRegistry;
use braid_http::types::Version;

/// Version graph entry: version -> its parents
pub type VersionGraph = HashMap<String, Vec<String>>;

/// Complete page state stored on disk
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionedPage {
    /// Current merged content
    pub content: String,
    /// Current version heads (tips of the version graph)
    pub heads: Vec<String>,
    /// Full version graph: version -> parent versions
    pub version_graph: VersionGraph,
    /// Merge type used for this page
    pub merge_type: String,
    /// Serialized merge type state (for restoration)
    pub merge_state: Value,
    /// Creation timestamp
    pub created_at: u64,
    /// Last modification timestamp
    pub modified_at: u64,
}

/// Page metadata for lightweight operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageMetadata {
    pub path: String,
    pub title: String,
    pub last_modified: u64,
    pub size: u64,
    pub merge_type: String,
    pub version_count: usize,
}

/// Storage backend for versioned pages
pub struct VersionedStorage {
    base_dir: PathBuf,
    registry: MergeTypeRegistry,
}

impl VersionedStorage {
    pub fn new(base_dir: PathBuf) -> Self {
        let registry = MergeTypeRegistry::new();
        Self {
            base_dir,
            registry,
        }
    }

    /// Get the storage path for a page
    fn page_path(&self, path: &str) -> PathBuf {
        // Sanitize path: replace / with _, ensure .json extension
        let safe_name = path.replace('/', "_").replace('\\', "_");
        let file_name = if safe_name.ends_with(".json") {
            safe_name
        } else {
            format!("{}.json", safe_name)
        };
        self.base_dir.join(file_name)
    }

    /// Check if a version exists in the graph
    pub fn has_version(page: &VersionedPage, version: &str) -> bool {
        page.version_graph.contains_key(version) || page.heads.contains(&version.to_string())
    }

    /// Validate that all parents exist in the version graph
    pub fn validate_parents(page: &VersionedPage, parents: &[Version]) -> Result<(), String> {
        for parent in parents {
            let parent_str = match parent {
                Version::String(s) => s.as_str(),
                Version::Integer(i) => return Err(format!("Integer versions not supported: {}", i)),
            };

            // ROOT is always valid (empty string or special marker)
            if parent_str.is_empty() || parent_str == "ROOT" {
                continue;
            }

            // Check if parent exists in graph
            if !Self::has_version(page, parent_str) {
                return Err(format!(
                    "Parent version '{}' not found. Need to sync first.",
                    parent_str
                ));
            }
        }
        Ok(())
    }

    /// Load a page from storage
    pub async fn load(&self, path: &str) -> Option<VersionedPage> {
        let file_path = self.page_path(path);
        
        match fs::read_to_string(&file_path).await {
            Ok(json) => {
                match serde_json::from_str::<VersionedPage>(&json) {
                    Ok(page) => Some(page),
                    Err(e) => {
                        warn!("Failed to parse page {}: {}", path, e);
                        None
                    }
                }
            }
            Err(_) => None,
        }
    }

    /// Load or create a new page
    pub async fn load_or_create(&self, path: &str, merge_type: &str) -> VersionedPage {
        if let Some(page) = self.load(path).await {
            return page;
        }

        // Create new page
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        VersionedPage {
            content: String::new(),
            heads: Vec::new(),
            version_graph: HashMap::new(),
            merge_type: merge_type.to_string(),
            merge_state: Value::Null,
            created_at: now,
            modified_at: now,
        }
    }

    /// Save a page to storage
    pub async fn save(&self, path: &str, page: &VersionedPage) -> anyhow::Result<()> {
        let file_path = self.page_path(path);
        
        // Ensure directory exists
        if let Some(parent) = file_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        // Serialize with pretty printing for human readability
        let json = serde_json::to_string_pretty(page)?;
        fs::write(&file_path, json).await?;
        
        info!("Saved page {} with {} versions", path, page.version_graph.len());
        Ok(())
    }

    /// Apply a patch to a page using the appropriate merge type
    pub async fn apply_patch(
        &self,
        page: &mut VersionedPage,
        patches: Vec<MergePatch>,
        new_version: Version,
        parents: Vec<Version>,
    ) -> Result<Vec<MergePatch>, String> {
        // Validate parents first
        Self::validate_parents(page, &parents)?;

        // Create merge type instance
        let mut merge_type = self
            .registry
            .create(&page.merge_type, "server")
            .ok_or_else(|| format!("Unknown merge type: {}", page.merge_type))?;

        // Restore merge state
        merge_type.initialize(&page.content);

        // Apply patches
        let mut rebased_patches = Vec::new();
        for patch in patches {
            let result = merge_type.apply_patch(patch);
            if !result.success {
                return Err(result.error.unwrap_or_else(|| "Merge failed".to_string()));
            }
            rebased_patches.extend(result.rebased_patches);
        }

        // Update page state
        page.content = merge_type.get_content();
        
        // Update version graph
        let version_str = match &new_version {
            Version::String(s) => s.clone(),
            Version::Integer(i) => i.to_string(),
        };
        
        let parent_strings: Vec<String> = parents
            .iter()
            .map(|p| match p {
                Version::String(s) => s.clone(),
                Version::Integer(i) => i.to_string(),
            })
            .collect();
        
        page.version_graph.insert(version_str.clone(), parent_strings);
        page.heads = vec![version_str];
        
        page.modified_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Ok(rebased_patches)
    }

    /// List all pages
    pub async fn list_pages(&self) -> Vec<PageMetadata> {
        let mut pages = Vec::new();
        
        let mut entries = match fs::read_dir(&self.base_dir).await {
            Ok(e) => e,
            Err(_) => return pages,
        };

        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            if path.extension().map_or(false, |ext| ext == "json") {
                if let Ok(content) = fs::read_to_string(&path).await {
                    if let Ok(page) = serde_json::from_str::<VersionedPage>(&content) {
                        let file_name = path.file_stem().unwrap_or_default().to_string_lossy();
                        pages.push(PageMetadata {
                            path: file_name.to_string(),
                            title: Self::extract_title(&page.content),
                            last_modified: page.modified_at,
                            size: page.content.len() as u64,
                            merge_type: page.merge_type,
                            version_count: page.version_graph.len(),
                        });
                    }
                }
            }
        }
        
        pages
    }

    /// Extract title from content (first H1 or first line)
    fn extract_title(content: &str) -> String {
        for line in content.lines() {
            let trimmed = line.trim();
            if trimmed.starts_with("# ") {
                return trimmed[2..].trim().to_string();
            }
            if !trimmed.is_empty() {
                // Return first non-empty line, truncated
                let title = trimmed.to_string();
                if title.len() > 50 {
                    return format!("{}...", &title[..50]);
                }
                return title;
            }
        }
        "Untitled".to_string()
    }

    /// Search pages
    pub async fn search(&self, query: &str) -> Vec<PageMetadata> {
        let all = self.list_pages().await;
        let query_lower = query.to_lowercase();
        
        all.into_iter()
            .filter(|p| {
                p.title.to_lowercase().contains(&query_lower) ||
                p.path.to_lowercase().contains(&query_lower)
            })
            .collect()
    }

    /// Get version graph as JSON for debugging
    pub fn get_version_graph_json(page: &VersionedPage) -> String {
        serde_json::to_string_pretty(&page.version_graph).unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_parents() {
        let mut page = VersionedPage {
            content: "test".to_string(),
            heads: vec!["v2".to_string()],
            version_graph: {
                let mut g = HashMap::new();
                g.insert("v1".to_string(), vec![]);
                g.insert("v2".to_string(), vec!["v1".to_string()]);
                g
            },
            merge_type: "simpleton".to_string(),
            merge_state: Value::Null,
            created_at: 0,
            modified_at: 0,
        };

        // Valid parent
        assert!(VersionedStorage::validate_parents(&page, &[Version::String("v1".to_string())]).is_ok());
        
        // Invalid parent
        assert!(VersionedStorage::validate_parents(&page, &[Version::String("v99".to_string())]).is_err());
        
        // ROOT is always valid
        assert!(VersionedStorage::validate_parents(&page, &[Version::String("ROOT".to_string())]).is_ok());
        assert!(VersionedStorage::validate_parents(&page, &[Version::String("".to_string())]).is_ok());
    }
}
