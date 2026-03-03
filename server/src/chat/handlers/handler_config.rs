use crate::core::{AppState, Error, Result};
use axum::{extract::State, Json};
use serde::Deserialize;

#[derive(Deserialize)]
pub struct CookiePayload {
    pub domain: String,
    pub value: String,
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
