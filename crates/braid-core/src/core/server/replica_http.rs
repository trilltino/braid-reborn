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
    if let ReplicaError::HistoryUnavailable(frontier) = &error {
        return Response::builder()
            .status(432)
            .header(
                "version",
                braid_http::protocol::format_version_header(frontier),
            )
            .body(Body::from(error.to_string()))
            .unwrap_or_else(crate::core::server::response_error);
    }
    let status = match &error {
        ReplicaError::Invalid(_) => StatusCode::BAD_REQUEST,
        ReplicaError::Conflict(_) => StatusCode::CONFLICT,
        ReplicaError::Limit(_) => StatusCode::PAYLOAD_TOO_LARGE,
        ReplicaError::Access(crate::access::AccessError::Denied) => StatusCode::FORBIDDEN,
        ReplicaError::Access(crate::access::AccessError::AlreadyOwned) => StatusCode::CONFLICT,
        ReplicaError::Access(crate::access::AccessError::Invalid(_)) => StatusCode::BAD_REQUEST,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (status, error.to_string()).into_response()
}

/// Checks authority before loading data or revealing historical selectors.
pub async fn get_authorized(
    store: ReplicaStore,
    policies: crate::access::PolicyStore,
    principal: Option<crate::access::Principal>,
    resource: String,
    headers: HeaderMap,
    merge_type: &str,
) -> Response {
    if let Err(error) = policies
        .authorize(&resource, principal.as_ref(), crate::access::Action::Read)
        .await
    {
        return error_response(error.into());
    }
    let response = get(store, resource.clone(), headers, merge_type).await;
    protect_response(policies, resource, principal, response).await
}

/// Rechecks active streams on every frame and at least once per second, including
/// idle streams. Dropping the response cancels the timer and upstream receiver.
pub async fn protect_response(
    policies: crate::access::PolicyStore,
    resource: String,
    principal: Option<crate::access::Principal>,
    response: Response,
) -> Response {
    if let Err(error) = policies
        .authorize(&resource, principal.as_ref(), crate::access::Action::Read)
        .await
    {
        return error_response(error.into());
    }
    if response.status().as_u16() != 209 {
        return response;
    }
    let (parts, body) = response.into_parts();
    let stream = async_stream::stream! {
        use futures::StreamExt;
        let mut upstream = body.into_data_stream();
        let mut timer = tokio::time::interval(std::time::Duration::from_secs(1));
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            if policies.authorize(&resource, principal.as_ref(), crate::access::Action::Read).await.is_err() {
                yield Err(std::io::Error::other("Resource access revoked or policy unavailable"));
                break;
            }
            tokio::select! {
                _ = timer.tick() => {},
                frame = upstream.next() => match frame {
                    Some(frame) => {
                        if policies.authorize(&resource, principal.as_ref(), crate::access::Action::Read).await.is_err() {
                            yield Err(std::io::Error::other("Resource access revoked or policy unavailable"));
                            break;
                        }
                        yield frame.map_err(std::io::Error::other);
                    },
                    None => break,
                },
            }
        }
    };
    Response::from_parts(parts, Body::from_stream(stream))
}
pub fn selector_error_response(
    error: ReplicaError,
    fields: &std::collections::BTreeMap<String, String>,
) -> Response {
    let mut response = error_response(error);
    if response.status().as_u16() == 432 {
        response.headers_mut().remove("version");
        for name in ["version", "parents"] {
            if let Some(value) = fields.get(name) {
                match axum::http::HeaderValue::from_str(value) {
                    Ok(value) => {
                        response
                            .headers_mut()
                            .insert(axum::http::HeaderName::from_static(name), value);
                    }
                    Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
                }
            }
        }
    }
    response
}
pub async fn get(
    store: ReplicaStore,
    resource: String,
    headers: HeaderMap,
    merge_type: &str,
) -> Response {
    let fields = match braid_http::protocol::headers_from_http(&headers) {
        Ok(fields) => fields,
        Err(error) => return (StatusCode::BAD_REQUEST, error.to_string()).into_response(),
    };
    if fields.contains_key("version") && fields.contains_key("subscribe") {
        return (
            StatusCode::BAD_REQUEST,
            "Version and Subscribe cannot be combined",
        )
            .into_response();
    }
    let version = match fields
        .get("version")
        .map(|v| braid_http::protocol::parse_version_header(v))
        .transpose()
    {
        Ok(version) => version,
        Err(error) => return (StatusCode::BAD_REQUEST, error.to_string()).into_response(),
    };
    let parents = match fields
        .get("parents")
        .map(|v| braid_http::protocol::parse_version_header(v))
        .transpose()
    {
        Ok(parents) => parents,
        Err(error) => return (StatusCode::BAD_REQUEST, error.to_string()).into_response(),
    };
    let heartbeat = match fields
        .get("heartbeats")
        .map(|v| braid_http::protocol::parse_heartbeat_duration(v))
        .transpose()
    {
        Ok(Some(interval)) if interval >= std::time::Duration::from_millis(100) => interval,
        Ok(None) => std::time::Duration::from_secs(30),
        _ => return (StatusCode::BAD_REQUEST, "Unsupported heartbeat interval").into_response(),
    };
    let peer = match fields
        .get("peer")
        .map(|value| braid_http::protocol::parse_peer(value))
        .transpose()
    {
        Ok(peer) => peer,
        Err(error) => return (StatusCode::BAD_REQUEST, error.to_string()).into_response(),
    };
    let subscribe = match fields
        .get("subscribe")
        .map(|s| braid_http::protocol::parse_subscribe(s))
        .transpose()
    {
        Ok(value) => value.unwrap_or(false),
        Err(error) => return (StatusCode::BAD_REQUEST, error.to_string()).into_response(),
    };
    if subscribe {
        let (document, mut receiver, keepalive) = match store.subscribe(&resource, merge_type).await
        {
            Ok(subscription) => subscription,
            Err(e) => return error_response(e),
        };
        if document.merge_type == "simpleton" {
            return match (super::simpleton_stream::SimpletonSubscription {
                store,
                resource,
                peer,
                parents,
                document,
                receiver,
                keepalive,
                heartbeat,
            })
            .into_response()
            {
                Ok(response) => response,
                Err(error) => selector_error_response(error, &fields),
            };
        }
        let initial = if let Some(parents) = parents {
            let frontier = parents
                .into_iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>();
            match document.replay_since(&frontier) {
                Ok(updates) => updates,
                Err(e) => return selector_error_response(e, &fields),
            }
        } else {
            vec![document.snapshot()]
        };
        let owner = store.clone();
        let stream = async_stream::stream! {
            let _owner=owner;
            let _keepalive=keepalive;
            for update in initial {
                match format_update(&update) {Ok(bytes)=>yield Ok::<Bytes,std::io::Error>(bytes),Err(e)=>{yield Err(std::io::Error::other(e.to_string()));return;}}
            }
            let mut heartbeat=tokio::time::interval(heartbeat);
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
            .header("heartbeats", heartbeat.as_secs_f64().to_string())
            .header("merge-type", document.merge_type)
            .header("cache-control", "no-cache")
            .body(Body::from_stream(stream))
            .unwrap_or_else(crate::core::server::response_error);
    }
    match store.load(&resource).await {
        Ok(Some(document)) => {
            if let Some(parents) = parents {
                let target = version.unwrap_or_else(|| document.snapshot().version);
                let updates = match document.causal_history(&parents, &target) {
                    Ok(updates) => updates,
                    Err(error) => return selector_error_response(error, &fields),
                };
                let frames = updates
                    .iter()
                    .map(format_update)
                    .collect::<Result<Vec<_>, _>>();
                match frames {
                    Ok(frames) => Response::builder()
                        .status(200)
                        .header(
                            "version",
                            braid_http::protocol::format_version_header(&target),
                        )
                        .header(
                            "parents",
                            braid_http::protocol::format_version_header(&parents),
                        )
                        .body(Body::from_stream(futures::stream::iter(
                            frames.into_iter().map(Ok::<_, std::io::Error>),
                        )))
                        .unwrap_or_else(crate::core::server::response_error),
                    Err(error) => {
                        (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response()
                    }
                }
            } else {
                match version.map_or_else(
                    || Ok(document.snapshot()),
                    |frontier| document.snapshot_at(&frontier),
                ) {
                    Ok(update) => super::BraidUpdate(update).into_response(),
                    Err(error) => selector_error_response(error, &fields),
                }
            }
        }
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
    put_profile(store, resource, headers, body, merge_type, None).await
}

pub async fn put_authorized(
    store: ReplicaStore,
    policies: crate::access::PolicyStore,
    principal: crate::access::Principal,
    resource: String,
    headers: HeaderMap,
    body: Bytes,
    merge_type: &str,
) -> Response {
    put_profile(
        store,
        resource,
        headers,
        body,
        merge_type,
        Some((policies, principal)),
    )
    .await
}

async fn put_profile(
    store: ReplicaStore,
    resource: String,
    headers: HeaderMap,
    body: Bytes,
    merge_type: &str,
    authority: Option<(crate::access::PolicyStore, crate::access::Principal)>,
) -> Response {
    let headers = match braid_http::protocol::headers_from_http(&headers) {
        Ok(headers) => headers,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };
    let transforming = match headers
        .get("peer")
        .map(|value| braid_http::protocol::parse_peer(value))
        .transpose()
    {
        Ok(peer) => peer.is_some(),
        Err(error) => return (StatusCode::BAD_REQUEST, error.to_string()).into_response(),
    };
    let update = match WireCodec::decode_entity(headers, body) {
        Ok(update) => update,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };
    let negotiated = update.merge_type.as_deref().unwrap_or(merge_type);
    let committed = if let Some((policies, principal)) = authority {
        store
            .commit_authorized(&resource, &update, negotiated, &policies, &principal)
            .await
    } else if transforming && negotiated == "simpleton" {
        store
            .commit_transforming_simpleton(&resource, &update)
            .await
    } else {
        store.commit(&resource, &update, negotiated).await
    };
    match committed {
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
                .unwrap_or_else(crate::core::server::response_error)
        }
        Err(e) => error_response(e),
    }
}

/// `POST /.well-known/braid/replication/v1/history`: authenticated, bounded
/// original-event pages for ingestion by another replica. Authority is checked
/// before any history metadata is loaded, and the request cannot name the
/// reserved replication namespace itself.
pub async fn history_authorized(
    store: ReplicaStore,
    policies: crate::access::PolicyStore,
    principal: crate::access::Principal,
    body: Bytes,
) -> Response {
    use crate::replica::{HistoryRequest, MAX_HISTORY_REQUEST_BYTES, REPLICATION_NAMESPACE};
    if body.len() > MAX_HISTORY_REQUEST_BYTES {
        return error_response(ReplicaError::Limit("History request is too large".into()));
    }
    let request: HistoryRequest = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(error) => {
            return (
                StatusCode::BAD_REQUEST,
                format!(
                    "Invalid history request at line {}, column {}",
                    error.line(),
                    error.column()
                ),
            )
                .into_response()
        }
    };
    if !request.resource.starts_with('/') || request.resource.starts_with(REPLICATION_NAMESPACE) {
        return (StatusCode::BAD_REQUEST, "Invalid history resource").into_response();
    }
    match store
        .history_page_authorized(
            &request.resource,
            &request.from,
            request.target.as_deref(),
            request.limits.unwrap_or_default(),
            &policies,
            Some(&principal),
        )
        .await
    {
        Ok(Some(page)) => match serde_json::to_vec(&page) {
            Ok(bytes) => Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .header("cache-control", "no-store")
                .body(Body::from(bytes))
                .unwrap_or_else(crate::core::server::response_error),
            Err(error) => error_response(error.into()),
        },
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(error) => error_response(error),
    }
}
