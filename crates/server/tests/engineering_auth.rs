use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use local_link_server::{
    chat::{friends::FriendManager, mail::MailManager},
    core::{
        auth::AuthManager,
        config::{AppState, ChatServerConfig},
        pages::{LocalOrgManager, PagesManager},
        store::json_store::JsonChatStore,
    },
};
use std::sync::Arc;
use tower::ServiceExt;

#[tokio::test]
async fn dispatcher_and_blob_routes_require_auth_and_user_headers_cannot_impersonate() {
    let temp = tempfile::tempdir().unwrap();
    let config = ChatServerConfig::with_base_dir(temp.path());
    let store = Arc::new(JsonChatStore::new(config.clone()).await.unwrap());
    let auth = Arc::new(AuthManager::new(temp.path()).await.unwrap());
    let user = auth
        .signup(
            "alice@example.test".into(),
            "alice".into(),
            "test password 123".into(),
            None,
        )
        .await
        .unwrap();
    let (_, session) = auth
        .login("alice@example.test".into(), "test password 123".into())
        .await
        .unwrap();
    let state = AppState {
        config: config.clone(),
        store: store.clone(),
        auth,
        friends: Arc::new(FriendManager::new(temp.path()).await.unwrap()),
        ai_manager: None,
        daemon: None,
        mail_manager: Arc::new(MailManager::new(store.clone())),
        pages_manager: Arc::new(PagesManager::new(0, temp.path().join("pages"))),
        local_org_manager: Arc::new(LocalOrgManager::new(temp.path().to_str().unwrap())),
    };
    let app = local_link_server::router(state);
    for (method, path) in [
        ("GET", "/room"),
        ("PUT", "/room"),
        ("GET", "/chat/room"),
        ("GET", "/blobs/secret"),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(path)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::UNAUTHORIZED,
            "{method} {path}"
        );
    }
    let response = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/chat/room")
                .header("authorization", format!("Bearer {}", session.token))
                .header("x-user", "someone-else")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"content":"hello"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        store.get_messages("room", None).await.unwrap()[0].sender,
        user.id
    );
}
