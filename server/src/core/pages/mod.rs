//! Pages Service
//!
//! File-based page storage, broadcast, and sync.
//! Powers the unified Pages Editor for Web and Tauri clients.

pub mod handlers;
pub mod handlers_v2;
pub mod local_org;
pub mod manager;
pub mod versioned_storage;

pub use handlers::{
    get_wiki_page, put_wiki_page, list_wiki_pages, search_wiki_pages,
    get_local_page, put_local_page, list_local_pages,
};
pub use local_org::LocalOrgManager;
pub use manager::{PagesManager, PageInfo, PagesUpdate};

use crate::core::AppState;
use axum::{
    routing::{get, put},
    Router,
};

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/wiki/index", get(handlers::list_wiki_pages))
        .route("/wiki/search", get(handlers::search_wiki_pages))
        // Local.org routes
        .route("/local.org/", get(handlers::list_local_pages))
        .route(
            "/local.org/{*path}",
            get(handlers::get_local_page).put(handlers::put_local_page),
        )
        // V2 API with Diamond Types CRDT support
        .route("/v2/pages", get(handlers_v2::list_pages_v2))
        .route(
            "/v2/pages/{*path}",
            get(handlers_v2::get_page_v2).put(handlers_v2::put_page_v2),
        )
        .route("/v2/pages/{*path}/versions", get(handlers_v2::get_page_versions_v2))
}
