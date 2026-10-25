//! Test-only loopback fixture around the actual application router.
//! Bootstrap tokens are generated only inside a caller-supplied empty test root.
use axum::{
    body::Body, extract::{Request, State}, http::StatusCode, middleware::{self, Next},
    response::{IntoResponse, Response}, routing::post, Json, Router,
};
use braid_core::{access::{PolicyChange, Principal, Role}, replica::ReplicaStore, types::{Update, Version}};
use local_link_server::{
    chat::{friends::FriendManager, mail::MailManager},
    core::{auth::AuthManager, config::{AppState, ChatServerConfig}, pages::{LocalOrgManager, PagesManager}, store::json_store::JsonChatStore},
};
use serde::Deserialize;
use std::{collections::BTreeSet, sync::{Arc, Mutex, atomic::{AtomicUsize, Ordering}}};

#[derive(Default)]
struct Metrics {
    active: AtomicUsize,
    starts: AtomicUsize,
    puts: Mutex<Vec<serde_json::Value>>,
    lose_ack: Mutex<BTreeSet<String>>,
}
struct ActiveSubscription(Arc<Metrics>);
impl Drop for ActiveSubscription {
    fn drop(&mut self) { self.0.active.fetch_sub(1, Ordering::SeqCst); }
}
#[derive(Clone)]
struct Fixture {
    state: AppState,
    alice: Principal,
    bob: Principal,
    metrics: Arc<Metrics>,
}
#[derive(Deserialize)]
struct Control {
    action: String,
    #[serde(default)] name: String,
    #[serde(default)] text: String,
    #[serde(default)] share: bool,
}
async fn control(State(fixture): State<Fixture>, Json(input): Json<Control>) -> Response {
    match control_result(fixture, input).await {
        Ok(value) => Json(value).into_response(),
        Err(_) => (StatusCode::BAD_REQUEST, "Invalid test fixture operation").into_response(),
    }
}
async fn control_result(fixture: Fixture, input: Control) -> anyhow::Result<serde_json::Value> {
    if input.action == "metrics" {
        return Ok(serde_json::json!({
            "active": fixture.metrics.active.load(Ordering::SeqCst),
            "starts": fixture.metrics.starts.load(Ordering::SeqCst),
            "puts": fixture.metrics.puts.lock().unwrap_or_else(|e| e.into_inner()).clone(),
        }));
    }
    anyhow::ensure!(!input.name.is_empty() && input.name.len() <= 64 && input.name.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'-' | b'_')), "Invalid fixture name");
    let policies = fixture.state.auth.policies()?;
    let resource = local_link_server::core::access::resource_key("v2", &input.name)
        .map_err(|_| anyhow::anyhow!("Invalid fixture resource"))?;
    match input.action.as_str() {
        "seed" => {
            anyhow::ensure!(input.text.len() <= 8 * 1024 * 1024, "Fixture text too large");
            let store = ReplicaStore::new(fixture.state.pages_manager.storage_dir.clone())?;
            anyhow::ensure!(store.load(&input.name).await?.is_none(), "Fixture already exists");
            policies.create(&resource, fixture.alice.clone()).await?;
            if input.share { policies.change(&resource, &fixture.alice, PolicyChange::Grant(fixture.bob.clone(), Role::Editor)).await?; }
            store.commit(&input.name, &Update::snapshot(Version::new(format!("seed-{}", input.name)), input.text).with_merge_type("simpleton").with_content_type("text/plain"), "simpleton").await?;
        }
        "revoke" => { policies.change(&resource, &fixture.alice, PolicyChange::Revoke(fixture.bob)).await?; }
        "lose-ack" => { fixture.metrics.lose_ack.lock().unwrap_or_else(|e| e.into_inner()).insert(format!("/v2/pages/{}", input.name)); }
        _ => anyhow::bail!("Unknown fixture action"),
    }
    Ok(serde_json::json!({"ok": true}))
}

async fn observe(State(metrics): State<Arc<Metrics>>, request: Request, next: Next) -> Response {
    let path = request.uri().path().to_owned();
    let is_put = request.method() == axum::http::Method::PUT;
    let version = request.headers().get("version").and_then(|value| value.to_str().ok()).map(str::to_owned);
    let response = next.run(request).await;
    if is_put && path.starts_with("/v2/pages/") {
        metrics.puts.lock().unwrap_or_else(|e| e.into_inner()).push(serde_json::json!({"path": path, "version": version, "status": response.status().as_u16()}));
        if response.status().is_success() && metrics.lose_ack.lock().unwrap_or_else(|e| e.into_inner()).remove(&path) {
            // Production already committed. Fail the actual HTTP response body,
            // so the browser sees an uncertain PUT and must replay its identity.
            let (mut parts, _) = response.into_parts();
            parts.headers.remove(axum::http::header::CONTENT_LENGTH);
            return Response::from_parts(parts, Body::from_stream(futures::stream::once(async {
                Err::<bytes::Bytes, _>(std::io::Error::new(std::io::ErrorKind::ConnectionReset, "Test-only lost acknowledgment"))
            })));
        }
    }
    if response.status().as_u16() != 209 { return response; }
    metrics.starts.fetch_add(1, Ordering::SeqCst);
    metrics.active.fetch_add(1, Ordering::SeqCst);
    let guard = ActiveSubscription(metrics);
    let (parts, body) = response.into_parts();
    Response::from_parts(parts, Body::from_stream(async_stream::stream! {
        use futures::StreamExt;
        let _guard = guard;
        let mut stream = body.into_data_stream();
        while let Some(frame) = stream.next().await { yield frame; }
    }))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    anyhow::ensure!(args.len() >= 3, "browser_fixture EMPTY_TEST_ROOT PORT ALLOWED_ORIGIN...");
    let root = std::path::absolute(&args[0])?;
    tokio::fs::create_dir_all(&root).await?;
    anyhow::ensure!(tokio::fs::read_dir(&root).await?.next_entry().await?.is_none(), "Fixture root must be empty");
    let port: u16 = args[1].parse()?;
    let mut config = ChatServerConfig::with_base_dir(&root);
    config.enable_daemon = false;
    config.ensure_dirs().await?;
    let auth = Arc::new(AuthManager::new(&root).await?);
    let mut principals = Vec::new();
    let mut tokens = Vec::new();
    let password = uuid::Uuid::new_v4().to_string();
    for name in ["alice", "bob"] {
        let email = format!("{name}@fixture.invalid");
        auth.signup(email.clone(), name.into(), password.clone(), None).await?;
        let (user, session) = auth.login(email, password.clone()).await?;
        principals.push(Principal::User(user.id)); tokens.push(session.token);
    }
    let store = Arc::new(JsonChatStore::new(config.clone()).await?);
    let pages = Arc::new(PagesManager::new(0, root.join("pages")));
    pages.ensure_dirs().await?;
    let state = AppState {
        config, store: store.clone(), auth,
        friends: Arc::new(FriendManager::new(&root).await?), ai_manager: None, daemon: None,
        mail_manager: Arc::new(MailManager::new(store)), pages_manager: pages,
        local_org_manager: Arc::new(LocalOrgManager::new(&root.to_string_lossy())),
    };
    let metrics = Arc::new(Metrics::default());
    let fixture = Fixture { state: state.clone(), alice: principals[0].clone(), bob: principals[1].clone(), metrics: metrics.clone() };
    let control = Router::new().route("/_fixture/control", post(control)).with_state(fixture);
    let app = local_link_server::router_with_cors(state, &args[2..])?
        .merge(control).layer(middleware::from_fn_with_state(metrics, observe));
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port)).await?;
    let address = listener.local_addr()?;
    // Ephemeral fixture-only bootstrap. The runner keeps it in memory and never
    // serializes tokens in its results/traces or an uploaded log.
    println!("{}", serde_json::json!({"origin": format!("http://127.0.0.1:{}", address.port()), "alice": tokens[0], "bob": tokens[1]}));
    axum::serve(listener, app).await?;
    Ok(())
}
