//! Actual application-router CORS behavior. Rust execution is deferred by request.
use axum::{body::Body, http::Request};
use local_link_server::{
    chat::{friends::FriendManager, mail::MailManager},
    core::{auth::AuthManager, pages::{LocalOrgManager, PagesManager}, store::json_store::JsonChatStore},
    AppState, ChatServerConfig,
};
use std::sync::Arc;
use tower::ServiceExt;

async fn state(root: &std::path::Path) -> AppState {
    let mut config = ChatServerConfig::with_base_dir(root);
    config.enable_daemon = false;
    let auth = Arc::new(AuthManager::new(root).await.unwrap());
    let store = Arc::new(JsonChatStore::new(config.clone()).await.unwrap());
    AppState {
        config,
        friends: Arc::new(FriendManager::with_pool(auth.pool()).await.unwrap()),
        mail_manager: Arc::new(MailManager::new(store.clone())),
        pages_manager: Arc::new(PagesManager::new(0, root.join("pages"))),
        local_org_manager: Arc::new(LocalOrgManager::new(&root.to_string_lossy())),
        store, auth, ai_manager: None, daemon: None,
    }
}

#[tokio::test]
async fn only_configured_origins_can_read_protocol_headers_or_pass_preflight() {
    let root = tempfile::tempdir().unwrap();
    let state = state(root.path()).await;
    let origin = "http://localhost:43123";
    let app = local_link_server::router_with_cors(state.clone(), &[origin.into()]).unwrap();
    let request = Request::builder().uri("/health").header("origin", origin).body(Body::empty()).unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.headers()["access-control-allow-origin"], origin);
    assert_eq!(response.headers()["access-control-allow-credentials"], "true");
    let exposed = response.headers()["access-control-expose-headers"].to_str().unwrap();
    for name in ["version", "parents", "current-version", "repr-digest", "heartbeats", "multiplex-version"] {
        assert!(exposed.split(',').map(str::trim).any(|value| value == name), "missing exposure for {name}");
    }
    assert!(!exposed.contains("authorization"));
    for candidate in [origin, "http://localhost:43124", "https://localhost:43123"] {
        let request = Request::builder().method("OPTIONS").uri("/v2/pages/doc")
            .header("origin", candidate).header("access-control-request-method", "PUT")
            .header("access-control-request-headers", "authorization,version,parents,content-type")
            .body(Body::empty()).unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        if candidate == origin {
            assert_eq!(response.headers()["access-control-allow-origin"], origin);
        } else {
            assert!(!response.headers().contains_key("access-control-allow-origin"));
        }
    }
    for invalid in ["*", "null", "http://localhost:43123/path", "http://user@localhost:43123"] {
        assert!(local_link_server::router_with_cors(state.clone(), &[invalid.into()]).is_err());
    }
    let request = Request::builder().uri("/health").header("origin", origin).body(Body::empty()).unwrap();
    let response = local_link_server::router(state).oneshot(request).await.unwrap();
    assert!(!response.headers().contains_key("access-control-allow-origin"));
}
