//! Braid Server
//!
//! Modular architecture separating Core infrastructure, Chat services,
//! and Pages functionality.

pub mod chat;
pub mod core;

// Re-exports from core for legacy/convenience
pub use crate::core::ctx::Ctx;
pub use crate::core::error::{Error, Result};
pub use crate::core::config::{AppState, ChatServerConfig};

use ax_auth::mw_require_auth;
use axum::{routing::get, Router, middleware, response::IntoResponse, extract::{Path, State, Request}};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{info, warn, Level};
use tracing_subscriber::FmtSubscriber;

use crate::core::auth::AuthManager;
use crate::chat::friends::FriendManager;
use crate::core::store::json_store::JsonChatStore;
use crate::chat::ai::{AiChatManager, AiConfig};
use crate::core::daemon::DaemonIntegration;
use crate::chat::mail::MailManager;
use crate::core::pages::{LocalOrgManager, PagesManager};

// Alias authentication middleware for clarity
use crate::core::auth::middleware as ax_auth;

pub async fn run() -> anyhow::Result<()> {
    // Initialize tracing
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    if tracing::subscriber::set_global_default(subscriber).is_err() {
        // Already set, ignore
    }

    info!("=== Braid Server (Modular) ===");
    info!("Services: Core | Chat | Website");

    // Get BRAID_ROOT from environment or default
    let braid_root = std::env::var("BRAID_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("braid_data"));

    // Initialize configuration
    let config = ChatServerConfig::with_base_dir(&braid_root);
    config.ensure_dirs().await?;

    info!("Storage directory: {:?}", config.storage_dir);

    // 1. Initialize Core Infrastructure
    let auth_manager = Arc::new(AuthManager::new(&braid_root).await?);
    let store = Arc::new(JsonChatStore::new(config.clone()).await?);
    
    // 2. Initialize Chat Services
    let friend_manager = Arc::new(FriendManager::new(&braid_root).await?);
    let mail_manager = Arc::new(MailManager::new(store.clone()));
    
    let ai_manager = if std::env::var("DISABLE_AI").is_err() {
        let ai_config = AiConfig::default();
        let ai = Arc::new(AiChatManager::new(ai_config, store.clone(), &config.storage_dir).await?);
        let _ = ai.start_watching().await;
        Some(ai)
    } else {
        None
    };

    // 3. Initialize Website Services
    let pages_manager = Arc::new(PagesManager::new(
        config.daemon_port,
        braid_common::braid_org_dir(),
    ));
    pages_manager.ensure_dirs().await?;
    let _ = pages_manager.start_discovery().await;

    let local_org_manager = Arc::new(LocalOrgManager::new(
        &braid_root.to_string_lossy(),
    ));
    local_org_manager.ensure_dir().await?;

    // 4. Initialize Shared Integrations
    let daemon = if config.enable_daemon {
        match DaemonIntegration::new(config.clone(), store.clone()).await {
            Ok((daemon_integration, file_events_rx)) => {
                let daemon = Arc::new(daemon_integration);
                let daemon_clone = daemon.clone();
                tokio::spawn(async move {
                    crate::core::daemon::file_watcher_task(file_events_rx, daemon_clone).await;
                });
                Some(daemon)
            }
            Err(_) => None,
        }
    } else {
        None
    };

    // Create app state
    let app_state = AppState {
        config,
        store,
        auth: auth_manager,
        friends: friend_manager,
        ai_manager,
        daemon,
        mail_manager,
        pages_manager,
        local_org_manager,
    };

    // Build the Modular Router
    
    // Services routers
    let core_router = core::router();
    let chat_router = chat::router()
        .route_layer(middleware::from_fn_with_state(app_state.clone(), mw_require_auth));
    let pages_router = core::pages::router();

    // Main App Router
    let app = Router::new()
        // Protocol-Driven Dispatcher (the entrance)
        .route("/{*path}", get(dispatch_get).put(dispatch_put))
        
        // Merge service routers
        .merge(core_router)
        .merge(chat_router)
        .merge(pages_router)
        
        // Global routes
        .route("/health", get(health_check))
        
        // State and Layers
        .with_state(app_state)
        .layer(tower_http::cors::CorsLayer::permissive())
        .layer(tower_http::trace::TraceLayer::new_for_http());

    // Start server
    let port = std::env::var("PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3001);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    
    info!("Server starting at http://localhost:{}", port);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

async fn health_check() -> &'static str {
    "OK - Braid Server (Modular Architecture)"
}

// Braid Protocol Dispatcher
// 
// Routes requests based on Merge-Type or extension to Chat or Website services.

async fn dispatch_get(
    _uri: axum::http::Uri,
    headers: axum::http::HeaderMap,
    Path(path): Path<String>,
    State(state): State<AppState>,
) -> axum::response::Response {
    let merge_type = headers.get("Merge-Type").and_then(|h| h.to_str().ok());
    let is_simpleton = merge_type == Some("simpleton");
    let has_extension = std::path::Path::new(&path).extension().is_some();
    let is_local_org = path.starts_with("local.org/");
    
    if is_local_org {
        crate::core::pages::handlers::get_local_page(Path(path.replace("local.org/", "")), State(state), headers).await
    } else if is_simpleton || has_extension {
        crate::core::pages::handlers::get_wiki_page(Path(path), State(state), headers).await
    } else {
        crate::chat::handlers::braid_subscribe::braid_subscribe(
            Path(path),
            State(state),
            headers,
        ).await.into_response()
    }
}

async fn dispatch_put(
    Path(path): Path<String>,
    State(state): State<AppState>,
    request: Request,
) -> axum::response::Response {
    let headers = request.headers().clone();
    let merge_type = headers.get("Merge-Type").and_then(|h| h.to_str().ok());
    let is_simpleton = merge_type == Some("simpleton");
    let has_extension = std::path::Path::new(&path).extension().is_some();
    let is_local_org = path.starts_with("local.org/");

    let (parts, body) = request.into_parts();
    let bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap_or_default();
    let body_str = String::from_utf8(bytes.to_vec()).unwrap_or_default();

    if is_local_org {
        return crate::core::pages::handlers::put_local_page(
            Path(path.replace("local.org/", "")),
            State(state),
            parts.headers,
            body_str
        ).await.into_response();
    }

    if is_simpleton || has_extension {

        return crate::core::pages::handlers::put_wiki_page(
            Path(path),
            State(state),
            parts.headers,
            body_str
        ).await.into_response();
    }

    // Default: Chat
    match serde_json::from_slice::<crate::core::models::CreateMessageInput>(&bytes) {
        Ok(json) => {
            match crate::chat::handlers::chat::put_message(
                Path(path),
                parts.headers,
                State(state),
                axum::Json(json)
            ).await {
                Ok((h, s)) => (s, h).into_response(),
                Err(c) => c.into_response(),
            }
        },
        Err(e) => {
            warn!("Failed to parse Chat JSON for {}: {}. Fallback to Wiki?", path, e);
            let body_str = String::from_utf8(bytes.to_vec()).unwrap_or_default();
            crate::core::pages::handlers::put_wiki_page(
               Path(path),
               State(state),
               parts.headers,
               body_str
            ).await
        }
    }
}
