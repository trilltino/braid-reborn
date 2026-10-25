use axum::{
    body::Body,
    http::{Request, StatusCode},
    response::Response,
};
use braid_core::access::{administration::Assignment, PolicyChange, Principal, Role};
use local_link_server::{
    chat::{friends::FriendManager, mail::MailManager},
    core::{
        auth::AuthManager,
        config::{AppState, ChatServerConfig},
        models::MessageType,
        pages::{LocalOrgManager, PagesManager},
        store::json_store::JsonChatStore,
    },
};
use std::sync::Arc;
use tower::ServiceExt;

async fn json(response: Response) -> serde_json::Value {
    let bytes = axum::body::to_bytes(response.into_body(), 32 * 1024 * 1024)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}
fn request(method: &str, uri: &str, token: Option<&str>, body: impl Into<Body>) -> Request<Body> {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(token) = token {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    builder
        .header("content-type", "application/json")
        .body(body.into())
        .unwrap()
}

#[tokio::test]
async fn real_routes_isolate_users_filter_indexes_and_guard_references_and_streams() {
    use futures::StreamExt;
    let root = tempfile::tempdir().unwrap();
    let config = ChatServerConfig::with_base_dir(root.path());
    let store = Arc::new(JsonChatStore::new(config.clone()).await.unwrap());
    let auth = Arc::new(AuthManager::new(root.path()).await.unwrap());
    let mut identities = Vec::new();
    for name in ["alice", "bob"] {
        auth.signup(
            format!("{name}@example.test"),
            name.into(),
            "test password 123".into(),
            None,
        )
        .await
        .unwrap();
        let (user, session) = auth
            .login(format!("{name}@example.test"), "test password 123".into())
            .await
            .unwrap();
        identities.push((Principal::User(user.id), session.token));
    }
    let (alice, a) = &identities[0];
    let (bob, b) = &identities[1];
    let policies = auth.policies().unwrap();
    let pages = Arc::new(PagesManager::new(0, root.path().join("pages")));
    pages.ensure_dirs().await.unwrap();
    let outside = root.path().join("outside");
    tokio::fs::create_dir(&outside).await.unwrap();
    tokio::fs::write(outside.join("secret.md"), "outside the page root")
        .await
        .unwrap();
    let link = pages.storage_dir.join("escape");
    #[cfg(unix)]
    std::os::unix::fs::symlink(&outside, &link).unwrap();
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        let output = std::process::Command::new("cmd")
            .args(["/C", "mklink", "/J"])
            .arg(&link)
            .arg(&outside)
            .creation_flags(0x08000000)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    tokio::fs::write(pages.storage_dir.join("legacy.md"), "ownerless legacy")
        .await
        .unwrap();
    store
        .add_message("legacy", "old", "original", MessageType::Text, None, vec![])
        .await
        .unwrap();
    let state = AppState {
        config,
        store: store.clone(),
        auth,
        friends: Arc::new(FriendManager::new(root.path()).await.unwrap()),
        ai_manager: None,
        daemon: None,
        mail_manager: Arc::new(MailManager::new(store.clone())),
        pages_manager: pages,
        local_org_manager: Arc::new(LocalOrgManager::new(root.path().to_str().unwrap())),
    };
    let mut control_state = state.clone();
    let app = local_link_server::router(state);
    // Even an explicitly granted owner may not traverse a junction/symlink.
    policies
        .create("wiki/escape/secret.md", alice.clone())
        .await
        .unwrap();
    assert_eq!(
        app.clone()
            .oneshot(request("GET", "/escape/secret.md", Some(a), Body::empty()))
            .await
            .unwrap()
            .status(),
        400
    );
    let mut escape = request(
        "PUT",
        "/escape/new.md",
        Some(a),
        "must stay inside the root",
    );
    escape
        .headers_mut()
        .insert("version", "\"escape\"".parse().unwrap());
    assert_eq!(app.clone().oneshot(escape).await.unwrap().status(), 400);
    assert!(!outside.join("new.md").exists());
    for (index, uri) in ["/local.org/local.md", "/v2/pages/note"]
        .into_iter()
        .enumerate()
    {
        let mut create = request("PUT", uri, Some(a), "private page");
        create
            .headers_mut()
            .insert("version", format!("\"page-{index}\"").parse().unwrap());
        assert_eq!(
            app.clone().oneshot(create).await.unwrap().status(),
            200,
            "{uri}"
        );
        assert_eq!(
            app.clone()
                .oneshot(request("GET", uri, Some(a), Body::empty()))
                .await
                .unwrap()
                .status(),
            200
        );
        assert_eq!(
            app.clone()
                .oneshot(request("GET", uri, Some(b), Body::empty()))
                .await
                .unwrap()
                .status(),
            403
        );
    }
    for (method, uri) in [
        ("GET", "/v2/pages/note/versions"),
        ("HEAD", "/v2/pages/note/versions"),
    ] {
        assert_eq!(
            app.clone()
                .oneshot(request(method, uri, Some(a), Body::empty()))
                .await
                .unwrap()
                .status(),
            200
        );
        assert_eq!(
            app.clone()
                .oneshot(request(method, uri, Some(b), Body::empty()))
                .await
                .unwrap()
                .status(),
            403
        );
    }
    for uri in ["/v2/pages", "/local.org/"] {
        assert_eq!(
            json(
                app.clone()
                    .oneshot(request("GET", uri, Some(b), Body::empty()))
                    .await
                    .unwrap()
            )
            .await,
            serde_json::json!([])
        );
    }
    let response = app
        .clone()
        .oneshot(request(
            "PUT",
            "/chat/private",
            Some(a),
            r#"{"content":"secret"}"#,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    for uri in [
        "/private",
        "/chat/private",
        "/chat/private/status",
        "/chat/private/subscribe",
        "/chat/private/drafts",
    ] {
        assert_eq!(
            app.clone()
                .oneshot(request("GET", uri, Some(b), Body::empty()))
                .await
                .unwrap()
                .status(),
            StatusCode::FORBIDDEN,
            "{uri}"
        );
    }
    assert_eq!(
        json(
            app.clone()
                .oneshot(request("GET", "/chat/rooms", Some(b), Body::empty()))
                .await
                .unwrap()
        )
        .await,
        serde_json::json!([])
    );
    assert_eq!(
        json(
            app.clone()
                .oneshot(request("GET", "/chat/rooms", Some(a), Body::empty()))
                .await
                .unwrap()
        )
        .await
        .as_array()
        .unwrap()
        .len(),
        1
    );
    assert_eq!(
        app.clone()
            .oneshot(request(
                "PUT",
                "/chat/private",
                Some(b),
                r#"{"content":"attack"}"#
            ))
            .await
            .unwrap()
            .status(),
        403
    );
    for uri in ["/chat/legacy", "/legacy.md"] {
        assert_eq!(
            app.clone()
                .oneshot(request("GET", uri, Some(a), Body::empty()))
                .await
                .unwrap()
                .status(),
            403
        );
        assert_eq!(
            app.clone()
                .oneshot(request("PUT", uri, Some(a), r#"{"content":"claim"}"#))
                .await
                .unwrap()
                .status(),
            403
        );
    }
    let plan = policies
        .plan_adoption(vec![Assignment {
            resource: "chat/legacy".into(),
            owner: alice.clone(),
        }])
        .await
        .unwrap();
    policies.apply_adoption(&plan).await.unwrap();
    assert_eq!(
        app.clone()
            .oneshot(request("GET", "/chat/legacy", Some(a), Body::empty()))
            .await
            .unwrap()
            .status(),
        200
    );
    assert_eq!(
        store.get_messages("legacy", None).await.unwrap()[0].content,
        "original"
    );
    let mut create = request("PUT", "/private.md", Some(a), "# secret page");
    create
        .headers_mut()
        .insert("version", "\"page1\"".parse().unwrap());
    assert_eq!(app.clone().oneshot(create).await.unwrap().status(), 200);
    assert_eq!(
        app.clone()
            .oneshot(request("GET", "/private.md", Some(b), Body::empty()))
            .await
            .unwrap()
            .status(),
        403
    );
    for uri in ["/wiki/index", "/wiki/search?q=secret"] {
        assert_eq!(
            json(
                app.clone()
                    .oneshot(request("GET", uri, Some(b), Body::empty()))
                    .await
                    .unwrap()
            )
            .await,
            serde_json::json!([])
        );
    }
    // Uploads create a private reference, not a public content-addressed URL.
    let multipart = "--fixture\r\nContent-Disposition: form-data; name=\"file\"; filename=\"secret.txt\"\r\nContent-Type: text/plain\r\n\r\nprivate attachment\r\n--fixture--\r\n";
    let mut upload = request("POST", "/blobs", Some(a), multipart);
    upload.headers_mut().insert(
        "content-type",
        "multipart/form-data; boundary=fixture".parse().unwrap(),
    );
    let response = app.clone().oneshot(upload).await.unwrap();
    assert_eq!(response.status(), 200);
    let reference = json(response).await;
    let blob_url = format!("/blobs/{}", reference["hash"].as_str().unwrap());
    assert_eq!(
        app.clone()
            .oneshot(request("GET", &blob_url, Some(b), Body::empty()))
            .await
            .unwrap()
            .status(),
        403
    );
    let attach = serde_json::json!({"content":"attachment", "blob_refs":[reference]}).to_string();
    assert_eq!(
        app.clone()
            .oneshot(request("PUT", "/chat/bob", Some(b), attach.clone()))
            .await
            .unwrap()
            .status(),
        403,
        "a message cannot grant its author access to a guessed hash"
    );
    assert_eq!(
        app.clone()
            .oneshot(request("PUT", "/chat/private", Some(a), attach))
            .await
            .unwrap()
            .status(),
        200
    );
    policies
        .change(
            "chat/private",
            alice,
            PolicyChange::Grant(bob.clone(), Role::Reader),
        )
        .await
        .unwrap();
    let mut unavailable = request("GET", "/chat/private/subscribe", Some(b), Body::empty());
    unavailable
        .headers_mut()
        .insert("subscribe", "?1".parse().unwrap());
    unavailable
        .headers_mut()
        .insert("parents", "\"missing\"".parse().unwrap());
    let unavailable = app.clone().oneshot(unavailable).await.unwrap();
    assert_eq!(unavailable.status().as_u16(), 432);
    assert_eq!(unavailable.headers()["parents"], "\"missing\"");
    assert!(!unavailable.headers().contains_key("version"));
    assert_eq!(
        app.clone()
            .oneshot(request("GET", &blob_url, Some(b), Body::empty()))
            .await
            .unwrap()
            .status(),
        200
    );
    assert_eq!(
        app.clone()
            .oneshot(request(
                "PUT",
                "/chat/private",
                Some(b),
                r#"{"content":"reader cannot write"}"#
            ))
            .await
            .unwrap()
            .status(),
        403
    );
    let mut subscribe = request("GET", "/chat/private/subscribe", Some(b), Body::empty());
    subscribe
        .headers_mut()
        .insert("subscribe", "?1".parse().unwrap());
    let response = app.clone().oneshot(subscribe).await.unwrap();
    assert_eq!(response.status(), 209);
    let mut stream = response.into_body().into_data_stream();
    assert!(stream.next().await.unwrap().is_ok());
    policies
        .change("chat/private", alice, PolicyChange::Revoke(bob.clone()))
        .await
        .unwrap();
    // Any already-buffered upstream message must be discarded after revocation.
    let frame = tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
        .await
        .unwrap()
        .unwrap();
    assert!(frame.is_err());
    assert_eq!(
        app.clone()
            .oneshot(request("GET", &blob_url, Some(b), Body::empty()))
            .await
            .unwrap()
            .status(),
        403
    );
    policies
        .change("chat/private", alice, PolicyChange::PublicRead(true))
        .await
        .unwrap();
    assert_eq!(
        app.clone()
            .oneshot(request("GET", "/chat/private", None, Body::empty()))
            .await
            .unwrap()
            .status(),
        200
    );
    assert_eq!(
        app.clone()
            .oneshot(request("GET", &blob_url, None, Body::empty()))
            .await
            .unwrap()
            .status(),
        200
    );
    let public = json(
        app.oneshot(request("GET", "/chat/rooms", None, Body::empty()))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(public.as_array().unwrap().len(), 1);
    assert_eq!(public[0]["id"], "private");
    // Global daemon credentials are a separate owner-managed control resource.
    // The app's login token must never be forwarded as daemon authentication.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let pending = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let reply_pending = pending.clone();
    let reply_calls = calls.clone();
    let daemon_app = axum::Router::new().route(
        "/api/credentials",
        axum::routing::put(
            move |headers: axum::http::HeaderMap,
                  axum::Json(body): axum::Json<serde_json::Value>| {
                let (pending, calls) = (reply_pending.clone(), reply_calls.clone());
                async move {
                    use axum::response::IntoResponse;
                    assert!(!headers.contains_key("authorization"));
                    assert!(!headers.contains_key("cookie"));
                    assert!(body.get("credentials").is_some());
                    calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    if pending.load(std::sync::atomic::Ordering::SeqCst) {
                        (
                            StatusCode::ACCEPTED,
                            axum::Json(serde_json::json!({"status":"pending"})),
                        )
                            .into_response()
                    } else {
                        axum::Json(serde_json::json!({"status":"ok"})).into_response()
                    }
                }
            },
        ),
    );
    control_state.config.daemon_port = listener.local_addr().unwrap().port();
    let (daemon, _receiver) = local_link_server::core::daemon::DaemonIntegration::new(
        control_state.config.clone(),
        store,
    )
    .await
    .unwrap();
    control_state.daemon = Some(Arc::new(daemon));
    let control_app = local_link_server::router(control_state);
    let task = tokio::spawn(async move {
        axum::serve(listener, daemon_app).await.unwrap();
    });
    let credential_body = serde_json::json!({"origin":"http://127.0.0.1:3001", "credentials":{"bearer_token":"explicit-control-token"}}).to_string();
    assert_eq!(
        control_app
            .clone()
            .oneshot(request(
                "POST",
                "/config/credentials",
                Some(a),
                credential_body.clone()
            ))
            .await
            .unwrap()
            .status(),
        403
    );
    policies
        .create("control/daemon", alice.clone())
        .await
        .unwrap();
    policies
        .change(
            "control/daemon",
            alice,
            PolicyChange::Grant(bob.clone(), Role::Editor),
        )
        .await
        .unwrap();
    assert_eq!(
        control_app
            .clone()
            .oneshot(request(
                "POST",
                "/config/credentials",
                Some(b),
                credential_body.clone()
            ))
            .await
            .unwrap()
            .status(),
        403
    );
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(
        control_app
            .clone()
            .oneshot(request(
                "POST",
                "/config/credentials",
                Some(a),
                credential_body.clone()
            ))
            .await
            .unwrap()
            .status(),
        200
    );
    let cookie_body =
        serde_json::json!({"domain":"example.test", "value":"explicit-cookie"}).to_string();
    let legacy = json(
        control_app
            .clone()
            .oneshot(request(
                "POST",
                "/config/cookie",
                Some(a),
                cookie_body.clone(),
            ))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(legacy["status"], "ok");
    assert_eq!(legacy["domain"], "example.test");
    pending.store(true, std::sync::atomic::Ordering::SeqCst);
    assert_eq!(
        control_app
            .clone()
            .oneshot(request(
                "POST",
                "/config/credentials",
                Some(a),
                credential_body
            ))
            .await
            .unwrap()
            .status(),
        202
    );
    assert_eq!(
        control_app
            .oneshot(request("POST", "/config/cookie", Some(a), cookie_body))
            .await
            .unwrap()
            .status(),
        202
    );
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 4);
    task.abort();
    let _ = task.await;
}
