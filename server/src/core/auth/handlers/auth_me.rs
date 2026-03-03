use crate::core::auth::UserInfo;
use crate::core::config::AppState;
use crate::core::ctx::Ctx;
use crate::core::error::{Error, Result};
use axum::extract::State;
use axum::Json;

/// GET /auth/me
pub async fn me(State(state): State<AppState>, ctx: Ctx) -> Result<Json<UserInfo>> {
    // No need to check token manually here!
    // If we are here, 'ctx' contains a valid user_id confirmed by middleware.

    let user = state.auth.get_user(ctx.user_id()).await?;

    Ok(Json(user))
}
