//! Application resource boundary. Authentication identifies a principal; policy
//! decides which representations, histories, indexes and streams it may observe.
use super::{auth::middleware::authenticate_headers, config::AppState};
use axum::{
    extract::{FromRequestParts, MatchedPath, Path, Request, State},
    http::{HeaderMap, Method, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use braid_core::{
    access::{AccessError, Action, MutationPermit, PolicyStore, Principal},
    replica::ReplicaStore,
    server::replica_http,
};
use std::collections::HashMap;

#[derive(Clone)]
pub struct AuthorizedPublicRead;

#[derive(Clone, Copy)]
enum Kind {
    Chat,
    Wiki,
    Local,
    V2,
}
impl Kind {
    fn prefix(self) -> &'static str {
        match self {
            Self::Chat => "chat",
            Self::Wiki => "wiki",
            Self::Local => "local",
            Self::V2 => "v2",
        }
    }
}

/// Windows file names share policies across case aliases. Reject path aliases
/// and storage-internal names before deriving either a policy key or disk path.
pub fn resource_key(prefix: &str, path: &str) -> Result<String, StatusCode> {
    braid_core::access::named_resource(prefix, path).map_err(|_| StatusCode::BAD_REQUEST)
}

pub async fn principal(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<Option<Principal>, StatusCode> {
    if !headers.contains_key(axum::http::header::AUTHORIZATION) {
        return Ok(None);
    }
    let ctx = authenticate_headers(&state.auth, headers)
        .await
        .map_err(|_| StatusCode::UNAUTHORIZED)?;
    Ok(Some(Principal::User(ctx.user_id().into())))
}

fn denied(error: AccessError, anonymous: bool) -> Response {
    if matches!(error, AccessError::Denied) && anonymous {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    replica_http::error_response(error.into())
}

pub async fn enforce(State(state): State<AppState>, request: Request, next: Next) -> Response {
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map(|path| path.as_str().to_owned())
        .unwrap_or_default();
    let collection = match route.as_str() {
        "/chat/rooms" => Some((Kind::Chat, "id")),
        "/wiki/index" | "/wiki/search" => Some((Kind::Wiki, "path")),
        "/local.org/" => Some((Kind::Local, "path")),
        "/v2/pages" => Some((Kind::V2, "path")),
        _ => None,
    };
    let resource_route = route.starts_with("/chat/{room_id}")
        || matches!(
            route.as_str(),
            "/{*path}" | "/local.org/{*path}" | "/v2/pages/{*path}"
        );
    if collection.is_none() && !resource_route {
        return next.run(request).await;
    }
    let principal = match principal(&state, request.headers()).await {
        Ok(principal) => principal,
        Err(status) => return status.into_response(),
    };
    let policies = match state.auth.policies() {
        Ok(policies) => policies,
        Err(error) => return denied(error, principal.is_none()),
    };
    let (mut parts, body) = request.into_parts();
    if let Some((kind, field)) = collection {
        if principal.is_none() {
            parts.extensions.insert(AuthorizedPublicRead);
        }
        let response = next.run(Request::from_parts(parts, body)).await;
        if !response.status().is_success() {
            return response;
        }
        let bytes = match axum::body::to_bytes(response.into_body(), 32 * 1024 * 1024).await {
            Ok(bytes) => bytes,
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        };
        let values: Vec<serde_json::Value> = match serde_json::from_slice(&bytes) {
            Ok(values) => values,
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        };
        let mut visible = Vec::new();
        for value in values {
            let Some(path) = value.get(field).and_then(|value| value.as_str()) else {
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            };
            let key = match resource_key(kind.prefix(), path) {
                Ok(key) => key,
                Err(_) => continue,
            };
            match policies
                .authorize(&key, principal.as_ref(), Action::Read)
                .await
            {
                Ok(()) => visible.push(value),
                Err(AccessError::Denied) => {}
                Err(error) => return denied(error, principal.is_none()),
            }
        }
        return Json(visible).into_response();
    }
    let Path(parameters) =
        match Path::<HashMap<String, String>>::from_request_parts(&mut parts, &state).await {
            Ok(parameters) => parameters,
            Err(_) => return StatusCode::BAD_REQUEST.into_response(),
        };
    let (kind, path) = if let Some(room) = parameters.get("room_id") {
        (Kind::Chat, room.clone())
    } else {
        let Some(path) = parameters.get("path") else {
            return StatusCode::BAD_REQUEST.into_response();
        };
        if route == "/local.org/{*path}" {
            (Kind::Local, path.clone())
        } else if route == "/v2/pages/{*path}" {
            (
                Kind::V2,
                if matches!(parts.method, Method::GET | Method::HEAD) {
                    path.strip_suffix("/versions").unwrap_or(path).into()
                } else {
                    path.clone()
                },
            )
        } else if let Some(local) = path.strip_prefix("local.org/") {
            (Kind::Local, local.into())
        } else if parts
            .headers
            .get("merge-type")
            .is_some_and(|value| value == "simpleton")
            || std::path::Path::new(path).extension().is_some()
        {
            (Kind::Wiki, path.clone())
        } else {
            (Kind::Chat, path.clone())
        }
    };
    let key = match resource_key(kind.prefix(), &path) {
        Ok(key) => key,
        Err(status) => return status.into_response(),
    };
    let reading = matches!(parts.method, Method::GET | Method::HEAD);
    let permit = if reading {
        if let Err(error) = policies
            .authorize(&key, principal.as_ref(), Action::Read)
            .await
        {
            return denied(error, principal.is_none());
        }
        if principal.is_none() {
            parts.extensions.insert(AuthorizedPublicRead);
        }
        None
    } else {
        let Some(actor) = &principal else {
            return StatusCode::UNAUTHORIZED.into_response();
        };
        match reserve_write(&state, &policies, actor, kind, &path, &key).await {
            Ok(permit) => Some(permit),
            Err(response) => return response,
        }
    };
    let selectors = parts.headers.clone();
    let mut response = next.run(Request::from_parts(parts, body)).await;
    drop(permit);
    if reading && response.status().as_u16() == 432 {
        let fields = match braid_http::protocol::headers_from_http(&selectors) {
            Ok(fields) => fields,
            Err(_) => return StatusCode::BAD_REQUEST.into_response(),
        };
        response.headers_mut().remove("version");
        response.headers_mut().remove("parents");
        for name in ["version", "parents"] {
            if let Some(value) = fields.get(name) {
                let value = match axum::http::HeaderValue::from_str(value) {
                    Ok(value) => value,
                    Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
                };
                response
                    .headers_mut()
                    .insert(axum::http::HeaderName::from_static(name), value);
            }
        }
    }
    if reading {
        replica_http::protect_response(policies, key, principal, response).await
    } else {
        response
    }
}

#[allow(clippy::result_large_err)]
async fn reserve_write(
    state: &AppState,
    policies: &PolicyStore,
    actor: &Principal,
    kind: Kind,
    path: &str,
    key: &str,
) -> Result<MutationPermit, Response> {
    if matches!(kind, Kind::Chat) {
        return state
            .store
            .authorize_resource_write(path, policies, actor)
            .await
            .map_err(|error| {
                if let Some(error) = error.downcast_ref::<AccessError>() {
                    match error {
                        AccessError::Denied => StatusCode::FORBIDDEN.into_response(),
                        _ => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
                    }
                } else {
                    StatusCode::INTERNAL_SERVER_ERROR.into_response()
                }
            });
    }
    let base = if matches!(kind, Kind::Local) {
        state.local_org_manager.storage_dir()
    } else {
        state.pages_manager.storage_dir.clone()
    };
    let store = ReplicaStore::new(if matches!(kind, Kind::V2) {
        base.clone()
    } else {
        base.join(".braid-replicas")
    })
    .map_err(replica_http::error_response)?;
    let file = if matches!(kind, Kind::V2) {
        store.path(path)
    } else {
        super::pages::handlers::resolve_path(&base, path)
            .map_err(|_| StatusCode::BAD_REQUEST.into_response())?
    };
    let _guard = braid_http::durable::document_lock(file.clone()).await;
    let exists = tokio::fs::try_exists(&file)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())?
        || store
            .load(path)
            .await
            .map_err(replica_http::error_response)?
            .is_some();
    let mut permit = policies
        .begin_write(key, actor, !exists)
        .await
        .map_err(|error| denied(error, false))?;
    permit
        .reserve(policies)
        .await
        .map_err(|error| denied(error, false))?;
    Ok(permit)
}

/// Blob authority comes from an upload reference or a readable resource that
/// already refers to it. A requested new message cannot grant its own access.
pub async fn authorize_blob(
    state: &AppState,
    actor: Option<&Principal>,
    hash: &str,
) -> Result<(), AccessError> {
    if hash.len() != 64 || !hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(AccessError::Denied);
    }
    let policies = state.auth.policies()?;
    let mut references = vec![format!("blob/{hash}")];
    if let Some(Principal::User(user)) = actor {
        references.push(format!("upload/{user}/{hash}"));
    }
    for reference in references {
        match policies.authorize(&reference, actor, Action::Read).await {
            Ok(()) => return Ok(()),
            Err(AccessError::Denied) => {}
            Err(error) => return Err(error),
        }
    }
    let rooms = state.store.list_rooms().await;
    if rooms.len() > 10000 {
        return Err(AccessError::Invalid(
            "Attachment reference scan exceeds 10000 resources".into(),
        ));
    }
    for room in rooms {
        let key = resource_key("chat", &room.id)
            .map_err(|_| AccessError::Invalid("Invalid room resource".into()))?;
        match policies.authorize(&key, actor, Action::Read).await {
            Ok(()) => {}
            Err(AccessError::Denied) => continue,
            Err(error) => return Err(error),
        }
        let room = state
            .store
            .get_room(&room.id)
            .await
            .map_err(|_| AccessError::Invalid("Cannot read attachment references".into()))?
            .ok_or_else(|| {
                AccessError::Invalid("Room disappeared during attachment lookup".into())
            })?;
        let room = room.read().await;
        if room.crdt.get_messages_sorted().iter().any(|message| {
            message
                .blob_refs
                .iter()
                .any(|reference| reference.hash == hash)
        }) {
            return Ok(());
        }
    }
    Err(AccessError::Denied)
}
