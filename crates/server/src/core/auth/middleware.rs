use crate::core::config::AppState;
use crate::core::ctx::Ctx;
use crate::core::error::{Error, Result};
use axum::{
    extract::{Request, State},
    http::header,
    middleware::Next,
    response::Response,
};
use tracing::debug;

/// Authentication shared by routed middleware and the protocol dispatcher.
pub async fn authenticate_headers(
    auth: &super::AuthManager,
    headers: &axum::http::HeaderMap,
) -> Result<Ctx> {
    let value = headers
        .get(header::AUTHORIZATION)
        .ok_or(Error::AuthFailNoToken)?;
    let value = value
        .to_str()
        .map_err(|_| Error::AuthFailTokenWrongFormat)?;
    let token = value
        .strip_prefix("Bearer ")
        .filter(|s| !s.is_empty())
        .ok_or(Error::AuthFailTokenWrongFormat)?;
    let user = auth
        .validate_session(token)
        .await
        .map_err(|_| Error::LoginFail)?;
    Ok(Ctx::new(user.id))
}

pub async fn mw_require_auth(
    State(state): State<AppState>,
    mut req: Request,
    next: Next,
) -> Result<Response> {
    debug!("MIDDLEWARE: require_auth");

    let ctx = authenticate_headers(&state.auth, req.headers()).await?;

    // Store Ctx in request extensions
    req.extensions_mut().insert(ctx);

    Ok(next.run(req).await)
}
