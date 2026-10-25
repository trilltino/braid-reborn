use crate::core::{AppState, Error, Result};
use axum::{extract::State, Json};
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use braid_core::access::{Action, Principal};
use serde::Deserialize;

#[derive(Deserialize)]
pub struct CookiePayload {
    pub domain: String,
    pub value: String,
}

#[derive(Deserialize)]
pub struct CredentialsPayload {
    pub origin: String,
    pub credentials: braid_http::client::OriginCredentials,
}

async fn authorize_control(
    state: &AppState,
    ctx: &crate::core::Ctx,
) -> std::result::Result<braid_core::access::MutationPermit, StatusCode> {
    let policies = state
        .auth
        .policies()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let actor = Principal::User(ctx.user_id().into());
    let status = |error| match error {
        braid_core::access::AccessError::Denied => StatusCode::FORBIDDEN,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    let mut permit = policies
        .begin_write("control/daemon", &actor, false)
        .await
        .map_err(status)?;
    policies
        .authorize("control/daemon", Some(&actor), Action::Manage)
        .await
        .map_err(status)?;
    permit.reserve(&policies).await.map_err(status)?;
    Ok(permit)
}

pub async fn protected_set_daemon_cookie(
    State(state): State<AppState>,
    ctx: crate::core::Ctx,
    Json(payload): Json<CookiePayload>,
) -> Response {
    let _permit = match authorize_control(&state, &ctx).await {
        Ok(permit) => permit,
        Err(status) => return status.into_response(),
    };
    let scoped =
        match braid_http::client::ScopedCredentials::legacy_cookie(&payload.domain, &payload.value)
        {
            Ok(scoped) => scoped,
            Err(_) => return StatusCode::BAD_REQUEST.into_response(),
        };
    let (origin, credentials) = scoped.into_parts();
    let response = apply_credentials(
        &state,
        CredentialsPayload {
            origin,
            credentials,
        },
    )
    .await;
    if response.status() == StatusCode::OK {
        Json(serde_json::json!({"status":"ok", "domain":payload.domain})).into_response()
    } else {
        response
    }
}

pub async fn set_daemon_credentials(
    State(state): State<AppState>,
    ctx: crate::core::Ctx,
    Json(payload): Json<CredentialsPayload>,
) -> Response {
    let _permit = match authorize_control(&state, &ctx).await {
        Ok(permit) => permit,
        Err(status) => return status.into_response(),
    };
    apply_credentials(&state, payload).await
}
async fn apply_credentials(state: &AppState, payload: CredentialsPayload) -> Response {
    if braid_http::client::ScopedCredentials::new(&payload.origin, payload.credentials.clone())
        .is_err()
    {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let Some(daemon) = &state.daemon else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    match daemon
        .set_origin_credentials(&payload.origin, &payload.credentials)
        .await
    {
        Ok(crate::core::daemon::ConfigurationOutcome::Persisted) => {
            Json(serde_json::json!({"status":"ok"})).into_response()
        }
        Ok(crate::core::daemon::ConfigurationOutcome::Pending) => (
            StatusCode::ACCEPTED,
            Json(serde_json::json!({"status":"pending"})),
        )
            .into_response(),
        Err(error) => (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({"status":"error", "message":error.to_string()})),
        )
            .into_response(),
    }
}

pub async fn set_daemon_cookie(
    State(state): State<AppState>,
    Json(payload): Json<CookiePayload>,
) -> Result<Json<serde_json::Value>> {
    if let Some(daemon) = &state.daemon {
        daemon.set_cookie(&payload.domain, &payload.value).await?;
        Ok(Json(
            serde_json::json!({ "status": "ok", "domain": payload.domain }),
        ))
    } else {
        Err(Error::Internal("Daemon integration disabled".to_string()))
    }
}
