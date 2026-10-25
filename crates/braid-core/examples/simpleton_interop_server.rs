//! Loopback-only interoperability fixture, not a deployment server.
use axum::{extract::Path, response::IntoResponse, routing::get, Json};
use braid_core::{replica::ReplicaStore, simpleton::SimpletonClient, BraidClient, Update, Version};
use braid_http::client::ChannelJournal;
use serde_json::json;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::env::args()
        .nth(1)
        .ok_or("Pass an empty fixture data directory")?;
    let store = ReplicaStore::new(std::path::PathBuf::from(root))?;
    if store.load("doc").await?.is_some() {
        return Err("Fixture directory is not empty".into());
    }
    store
        .commit(
            "doc",
            &Update::snapshot(Version::new("seed"), "a\u{1f600}b"),
            "simpleton",
        )
        .await?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}/doc", listener.local_addr()?);
    let rust = Arc::new(
        SimpletonClient::connect(&BraidClient::new()?, &url, ChannelJournal::memory()).await?,
    );
    let (gate, _) = tokio::sync::watch::channel(false);
    let gate = Arc::new(gate);
    let pending = Arc::new(AtomicUsize::new(0));
    let drop_ack = Arc::new(AtomicBool::new(false));
    let get_store = store.clone();
    let put_store = store.clone();
    let put_gate = gate.clone();
    let put_pending = pending.clone();
    let put_drop = drop_ack.clone();
    let status_rust = rust.clone();
    let edit_rust = rust.clone();
    let state_store = store.clone();
    let state_pending = pending.clone();
    let app = axum::Router::new()
        .route("/doc", get(move |headers: axum::http::HeaderMap| {
            let store = get_store.clone();
            async move { braid_core::server::replica_http::get(store, "doc".into(), headers, "simpleton").await }
        }).put(move |headers: axum::http::HeaderMap, body: bytes::Bytes| {
            let (store, gate, pending, drop_ack) = (put_store.clone(), put_gate.clone(), put_pending.clone(), put_drop.clone());
            async move {
                let mut open = gate.subscribe();
                pending.fetch_add(1, Ordering::SeqCst);
                while *open.borrow() { if open.changed().await.is_err() { return axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response(); } }
                pending.fetch_sub(1, Ordering::SeqCst);
                let response = braid_core::server::replica_http::put(store, "doc".into(), headers, body, "simpleton").await;
                if response.status().is_success() && drop_ack.swap(false, Ordering::SeqCst) { axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response() }
                else { response }
            }
        }))
        .route("/rust", get(move || {
            let rust = status_rust.clone();
            async move {
                let status = rust.status().borrow().clone();
                Json(json!({"text":status.text,"initialized":status.initialized,"outstanding":status.outstanding_puts,"error":status.error}))
            }
        }).post(move |text: String| {
            let rust = edit_rust.clone();
            async move { match rust.changed(text).await {Ok(())=>axum::http::StatusCode::OK.into_response(),Err(error)=>(axum::http::StatusCode::INTERNAL_SERVER_ERROR,error.to_string()).into_response()} }
        }))
        .route("/gate/{mode}", axum::routing::post(move |Path(mode): Path<String>| {
            let (gate, drop_ack) = (gate.clone(), drop_ack.clone());
            async move {
                match mode.as_str() {
                    "pause" => { drop_ack.store(true, Ordering::SeqCst); gate.send_replace(true); }
                    "resume" => { gate.send_replace(false); }
                    _ => return axum::http::StatusCode::BAD_REQUEST,
                }
                axum::http::StatusCode::OK
            }
        }))
        .route("/state", get(move || {
            let (store,pending) = (state_store.clone(),state_pending.clone());
            async move {
                match store.load("doc").await {
                    Ok(Some(document))=>Json(json!({"text":document.content,"events":document.history.len(),"pending":pending.load(Ordering::SeqCst)})).into_response(),
                    _=>axum::http::StatusCode::INTERNAL_SERVER_ERROR.into_response(),
                }
            }
        }));
    println!("{}", json!({"url":url}));
    axum::serve(listener, app).await?;
    rust.shutdown().await;
    Ok(())
}
