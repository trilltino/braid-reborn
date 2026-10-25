//! Owned browser Fetch transport using the canonical byte codec.
use super::{browser::{browser_error, error_code}, HeartbeatConfig, MessageParser};
use crate::{error::{BraidError, Result}, traits::BraidNetwork, types::{BraidRequest, BraidResponse, Update}};
use async_trait::async_trait;
use futures::FutureExt;
use std::collections::HashMap;
use wasm_bindgen::prelude::*;

pub struct WasmNetwork;

#[wasm_bindgen(module = "/src/client/browser_runtime.js")]
extern "C" {
    #[wasm_bindgen(catch, js_name = browserFetchStart)]
    fn fetch_start(url: &str, method: &str, headers: &str, body: &[u8]) -> std::result::Result<JsValue, JsValue>;
    #[wasm_bindgen(catch, js_name = browserFetchReady)]
    async fn fetch_ready(state: &JsValue) -> std::result::Result<JsValue, JsValue>;
    #[wasm_bindgen(js_name = browserFetchStatus)]
    fn fetch_status(state: &JsValue) -> u16;
    #[wasm_bindgen(js_name = browserFetchHeaders)]
    fn fetch_headers(state: &JsValue) -> String;
    #[wasm_bindgen(catch, js_name = browserFetchRead)]
    async fn fetch_read(state: &JsValue) -> std::result::Result<JsValue, JsValue>;
    #[wasm_bindgen(catch, js_name = browserFetchClose)]
    fn fetch_close(state: &JsValue) -> std::result::Result<(), JsValue>;
}

struct FetchOwner { state: JsValue }
impl FetchOwner {
    async fn open(url: &str, request: &BraidRequest) -> Result<Self> {
        let parsed = super::validate_resource_url(url)?;
        if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() {
            return Err(BraidError::Config("Browser Fetch requires HTTP(S)".into()));
        }
        let method = http::Method::from_bytes(request.method.as_bytes())
            .map_err(|_| BraidError::Protocol("Invalid outgoing HTTP method".into()))?;
        if matches!(method.as_str(), "CONNECT" | "TRACE" | "TRACK") {
            return Err(BraidError::CapabilityUnavailable { capability: "requested HTTP method in browser Fetch" });
        }
        if request.extra_headers.keys().any(|name| name.eq_ignore_ascii_case("cookie") || name.eq_ignore_ascii_case("cookie2")) {
            return Err(BraidError::CapabilityUnavailable { capability: "direct Cookie headers in browser Fetch; use origin cookies or bearer authentication" });
        }
        let (headers, body) = crate::protocol::encode_request(request)?;
        let encoded = serde_json::to_string(&headers)?;
        let owner = Self { state: fetch_start(url, method.as_str(), &encoded, &body).map_err(network_error)? };
        fetch_ready(&owner.state).await.map_err(network_error)?;
        Ok(owner)
    }
    fn status(&self) -> u16 { fetch_status(&self.state) }
    fn headers(&self) -> Result<HashMap<String, String>> {
        serde_json::from_str(&fetch_headers(&self.state)).map_err(|_| BraidError::HeaderParse("Invalid browser response headers".into()))
    }
    async fn read(&self) -> Result<Option<Vec<u8>>> {
        let value = fetch_read(&self.state).await.map_err(network_error)?;
        if value.is_undefined() { return Ok(None); }
        let bytes = js_sys::Uint8Array::new(&value);
        if bytes.length() > 32 * 1024 * 1024 { return Err(BraidError::ResourceLimit {resource:"HTTP chunk", limit:32 * 1024 * 1024}); }
        Ok(Some(bytes.to_vec()))
    }
}
impl Drop for FetchOwner {
    fn drop(&mut self) { let _ = fetch_close(&self.state); }
}

fn network_error(error: JsValue) -> BraidError {
    if error_code(&error) == "failed" { BraidError::Http("Browser Fetch failed".into()) }
    else { browser_error(error) }
}

#[async_trait(?Send)]
impl BraidNetwork for WasmNetwork {
    async fn fetch(&self, url: &str, request: BraidRequest) -> Result<BraidResponse> {
        let response = FetchOwner::open(url, &request).await?;
        let status = response.status();
        let headers = response.headers()?;
        let mut body = Vec::new();
        while let Some(chunk) = response.read().await? {
            if body.len().saturating_add(chunk.len()) > 32 * 1024 * 1024 { return Err(BraidError::ResourceLimit {resource:"HTTP entity", limit:32 * 1024 * 1024}); }
            body.extend_from_slice(&chunk);
        }
        Ok(BraidResponse { status, headers, body:body.into(), is_subscription:status == 209 })
    }

    async fn subscribe(&self, url: &str, mut request: BraidRequest) -> Result<async_channel::Receiver<Result<Update>>> {
        request.subscribe = true;
        request.method = "GET".into();
        let response = FetchOwner::open(url, &request).await?;
        let headers = response.headers()?;
        if response.status() != 209 {
            return Err(BraidError::SubscriptionStatus {status:response.status(), retry_after:headers.get("retry-after").and_then(|value| super::parse_retry_after(value))});
        }
        let heartbeat = headers.get("heartbeats").map(|value| {
            HeartbeatConfig::from_header(value).ok_or_else(|| BraidError::HeaderParse("Invalid Heartbeats response".into()))
        }).transpose()?;
        let merge_type = headers.get("merge-type").cloned();
        let repr_type = headers.get("repr-type").cloned();
        let (sender, receiver) = async_channel::bounded(1);
        wasm_bindgen_futures::spawn_local(async move {
            let mut parser = MessageParser::new();
            loop {
                let next = async {
                    if let Some(config) = &heartbeat { super::runtime::timeout(config.timeout, response.read()).await? }
                    else { response.read().await }
                }.fuse();
                let closed = sender.closed().fuse();
                futures::pin_mut!(next, closed);
                let chunk = futures::select! { _ = closed => return, result = next => result };
                let bytes = match chunk {
                    Ok(Some(bytes)) => bytes,
                    Ok(None) => { if let Err(error) = parser.finish() { let _ = sender.send(Err(error)).await; } return; },
                    Err(error) => { let _ = sender.send(Err(error)).await; return; },
                };
                let messages = match parser.feed(&bytes) {
                    Ok(messages) => messages,
                    Err(error) => { let _ = sender.send(Err(error)).await; return; },
                };
                for message in messages {
                    let update = crate::client::message_to_update(message).map(|mut update| {
                        if update.merge_type.is_none() {update.merge_type = merge_type.clone();}
                        if let Some(repr) = &repr_type {update.extra_headers.entry("repr-type".into()).or_insert_with(|| repr.clone());}
                        update
                    });
                    let terminal = update.is_err();
                    if sender.send(update).await.is_err() || terminal { return; }
                }
            }
        });
        Ok(receiver)
    }
}
