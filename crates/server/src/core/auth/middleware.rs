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

pub async fn mw_require_auth(
    State(state): State<AppState>,
    mut req: Request,
    next: Next,
) -> Result<Response> {
    debug!("MIDDLEWARE: require_auth");

    let auth_header = req.headers().get(header::AUTHORIZATION);
    let auth_header = match auth_header {
        Some(h) => h.to_str().map_err(|_| Error::AuthFailTokenWrongFormat)?,
        None => return Err(Error::AuthFailNoToken),
    };

    // Format: "Bearer <token>"
    if !auth_header.starts_with("Bearer ") {
        return Err(Error::AuthFailTokenWrongFormat);
    }

    let token = &auth_header[7..];

    // Validate token
    let user_info = state
        .auth
        .validate_session(token)
        .await
        .map_err(|_| Error::LoginFail)?;

    // Create Ctx
    let ctx = Ctx::new(user_info.id);

    // Store Ctx in request extensions
    req.extensions_mut().insert(ctx);

    Ok(next.run(req).await)
}
