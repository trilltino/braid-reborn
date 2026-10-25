//! Braid over Iroh uses the same durable replica and streaming codec as TCP HTTP.
use axum::{
    extract::{Path, State},
    http::HeaderMap,
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use braid_http_rs::{replica::ReplicaStore, server::replica_http};
use bytes::Bytes;
use iroh::EndpointId;
use iroh_h3_axum::{IrohAxum, RemoteId};
use parking_lot::RwLock;
use std::{collections::HashSet, sync::Arc};

/// The authenticated QUIC endpoint identity is checked before reading a resource.
#[derive(Clone, Debug)]
pub enum AccessPolicy {
    Peers(HashSet<EndpointId>),
    /// Open the transport to all authenticated endpoints. Resource policies still
    /// apply independently; topic names never supply resource authority.
    Public,
}
impl Default for AccessPolicy {
    fn default() -> Self {
        Self::Peers(HashSet::new())
    }
}
impl AccessPolicy {
    fn permits(&self, peer: EndpointId) -> bool {
        match self {
            Self::Peers(peers) => peers.contains(&peer),
            Self::Public => true,
        }
    }
}
#[derive(Clone)]
pub struct BraidAppState {
    pub policy_changed: tokio::sync::watch::Sender<u64>,
    pub replica: ReplicaStore,
    pub access: Arc<RwLock<AccessPolicy>>,
}
pub fn build_protocol_handler(state: BraidAppState) -> IrohAxum {
    IrohAxum::new(
        Router::new()
            .route("/{*resource}", get(handle_get).put(handle_put))
            .with_state(state),
    )
}
async fn handle_get(
    State(state): State<BraidAppState>,
    Path(resource): Path<String>,
    RemoteId(peer): RemoteId,
    headers: HeaderMap,
) -> Response {
    if !state.access.read().permits(peer) {
        return axum::http::StatusCode::FORBIDDEN.into_response();
    }
    let mut changed = state.policy_changed.subscribe();
    let policies = match state.replica.policy_store() {
        Ok(policies) => policies,
        Err(error) => return replica_http::error_response(error.into()),
    };
    let response = replica_http::get_authorized(
        state.replica,
        policies,
        Some(braid_http_rs::access::Principal::Peer(peer.to_string())),
        format!("/{resource}"),
        headers,
        "simpleton",
    )
    .await;
    let (parts, body) = response.into_parts();
    let stream = async_stream::stream! {
        use futures::StreamExt;
        let mut data = body.into_data_stream();
        loop {
            if !state.access.read().permits(peer) { break; }
            tokio::select! {
                biased;
                change = changed.changed() => { if change.is_err() { break; } }
                frame = data.next() => match frame {
                    Some(frame) => {
                        if !state.access.read().permits(peer) { break; }
                        yield frame;
                    }
                    None => break,
                }
            }
        }
    };
    Response::from_parts(parts, axum::body::Body::from_stream(stream))
}
async fn handle_put(
    State(state): State<BraidAppState>,
    Path(resource): Path<String>,
    RemoteId(peer): RemoteId,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if !state.access.read().permits(peer) {
        return axum::http::StatusCode::FORBIDDEN.into_response();
    }
    let policies = match state.replica.policy_store() {
        Ok(policies) => policies,
        Err(error) => return replica_http::error_response(error.into()),
    };
    replica_http::put_authorized(
        state.replica,
        policies,
        braid_http_rs::access::Principal::Peer(peer.to_string()),
        format!("/{resource}"),
        headers,
        body,
        "simpleton",
    )
    .await
}
