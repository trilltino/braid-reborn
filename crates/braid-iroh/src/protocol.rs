//! Braid over Iroh uses the same durable replica and streaming codec as TCP HTTP.
use axum::{
    extract::{Path, State},
    http::HeaderMap,
    response::Response,
    routing::get,
    Router,
};
use braid_http_rs::{replica::ReplicaStore, server::replica_http};
use bytes::Bytes;
use iroh_h3_axum::IrohAxum;
#[derive(Clone)]
pub struct BraidAppState {
    pub replica: ReplicaStore,
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
    headers: HeaderMap,
) -> Response {
    replica_http::get(state.replica, format!("/{resource}"), headers, "simpleton").await
}
async fn handle_put(
    State(state): State<BraidAppState>,
    Path(resource): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    replica_http::put(
        state.replica,
        format!("/{resource}"),
        headers,
        body,
        "simpleton",
    )
    .await
}
