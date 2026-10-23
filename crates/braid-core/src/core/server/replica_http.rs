//! The same replica and wire implementation is used by TCP HTTP and Iroh HTTP/3.
use crate::replica::{ReplicaError, ReplicaStore};
use axum::{
    body::Body,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use braid_http::protocol::{format_update, WireCodec};
use bytes::Bytes;

pub fn error_response(error: ReplicaError) -> Response {
    let status = match &error {
        ReplicaError::Invalid(_) => StatusCode::BAD_REQUEST,
        ReplicaError::Conflict(_) => StatusCode::CONFLICT,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (status, error.to_string()).into_response()
}
pub async fn get(
    store: ReplicaStore,
    resource: String,
    headers: HeaderMap,
    merge_type: &str,
) -> Response {
    if headers.get("subscribe").is_some_and(|v| v == "true") {
        let (document, mut receiver, keepalive) = match store.subscribe(&resource, merge_type).await
        {
            Ok(subscription) => subscription,
            Err(e) => return error_response(e),
        };
        let initial = if let Some(parents) = headers.get("parents") {
            let frontier = match parents
                .to_str()
                .ok()
                .and_then(|v| braid_http::protocol::parse_version_header(v).ok())
            {
                Some(v) => v.into_iter().map(|v| v.to_string()).collect::<Vec<_>>(),
                None => return (StatusCode::BAD_REQUEST, "Invalid Parents").into_response(),
            };
            match document.replay_since(&frontier) {
                Ok(updates) => updates,
                Err(e) => return error_response(e),
            }
        } else {
            vec![document.snapshot()]
        };
        let stream = async_stream::stream! {
            let _keepalive=keepalive;
            for update in initial {
                match format_update(&update) {Ok(bytes)=>yield Ok::<Bytes,std::io::Error>(bytes),Err(e)=>{yield Err(std::io::Error::other(e.to_string()));return;}}
            }
            let mut heartbeat=tokio::time::interval(std::time::Duration::from_secs(30));
            heartbeat.tick().await;
            loop {
                tokio::select! {
                    _=heartbeat.tick()=>yield Ok(Bytes::from_static(b"\r\n")),
                    received=receiver.recv()=>match received {
                        Ok(update)=>match format_update(&update) {Ok(bytes)=>yield Ok(bytes),Err(e)=>{yield Err(std::io::Error::other(e.to_string()));return;}},
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_))=>{
                            yield Err(std::io::Error::other("Subscriber lagged; reconnect from committed frontier"));return;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed)=>return,
                    }
                }
            }
        };
        return Response::builder()
            .status(209)
            .header("subscribe", "true")
            .header("heartbeats", "30")
            .header("merge-type", document.merge_type)
            .header("cache-control", "no-cache")
            .body(Body::from_stream(stream))
            .unwrap();
    }
    match store.load(&resource).await {
        Ok(Some(document)) => super::BraidUpdate(document.snapshot()).into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(e) => error_response(e),
    }
}
pub async fn put(
    store: ReplicaStore,
    resource: String,
    headers: HeaderMap,
    body: Bytes,
    merge_type: &str,
) -> Response {
    let headers = match braid_http::protocol::headers_from_http(&headers) {
        Ok(headers) => headers,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };
    let update = match WireCodec::decode_entity(headers, body) {
        Ok(update) => update,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };
    let negotiated = update.merge_type.as_deref().unwrap_or(merge_type);
    match store.commit(&resource, &update, negotiated).await {
        Ok(document) => {
            let version = braid_http::protocol::format_version_header(&update.version);
            Response::builder()
                .status(200)
                .header("version", version)
                .header(
                    "current-version",
                    braid_http::protocol::format_version_header(&document.snapshot().version),
                )
                .body(Body::empty())
                .unwrap()
        }
        Err(e) => error_response(e),
    }
}
