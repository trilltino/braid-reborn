//! Metadata view over the shared durable replica documents.
pub use braid_core::replica::ReplicaDocument as VersionedPage;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageMetadata {
    pub path: String,
    pub title: String,
    pub last_modified: u64,
    pub size: u64,
    pub merge_type: String,
    pub version_count: usize,
}
pub struct VersionedStorage {
    base_dir: PathBuf,
}
impl VersionedStorage {
    pub fn new(base_dir: PathBuf) -> Self {
        Self { base_dir }
    }
    pub async fn list_pages(&self) -> Vec<PageMetadata> {
        let mut pages = vec![];
        let Ok(mut entries) = tokio::fs::read_dir(&self.base_dir).await else {
            return pages;
        };
        while let Ok(Some(entry)) = entries.next_entry().await {
            if entry.path().extension().is_some_and(|e| e == "json") {
                if let Ok(bytes) = tokio::fs::read(entry.path()).await {
                    if let Ok(page) = serde_json::from_slice::<VersionedPage>(&bytes) {
                        let title = page
                            .content
                            .lines()
                            .map(str::trim)
                            .find(|s| !s.is_empty())
                            .unwrap_or("Untitled")
                            .trim_start_matches("# ")
                            .chars()
                            .take(50)
                            .collect();
                        pages.push(PageMetadata {
                            path: page.resource.unwrap_or_else(|| {
                                entry
                                    .path()
                                    .file_stem()
                                    .unwrap_or_default()
                                    .to_string_lossy()
                                    .into_owned()
                            }),
                            title,
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
}
