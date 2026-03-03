//! Auth handlers

use crate::core::config::AppState;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

#[derive(Debug, Deserialize)]
pub struct SignupRequest {
    pub email: String,
    pub username: String,
    pub password: String,
    pub avatar_blob_hash: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct LoginRequest {
    pub email: String,
    pub password: String,
}

use super::super::UserInfo;

#[derive(Debug, Serialize)]
pub struct AuthResponse {
    pub token: String,
    pub user_id: String,
    pub username: String,
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

pub async fn signup(
    State(state): State<AppState>,
    Json(req): Json<SignupRequest>,
) -> Result<Json<AuthResponse>, (StatusCode, Json<ErrorResponse>)> {
    info!("POST /auth/signup - {}", req.email);

    match state
        .auth
        .signup(
            req.email.clone(),
            req.username.clone(),
            req.password.clone(),
            req.avatar_blob_hash,
        )
        .await
    {
        Ok(user) => {
            // Create session
            match state
                .auth
                .login(req.email.clone(), req.password.clone())
                .await
            {
                Ok((_, session)) => {
                    info!("User {} registered successfully", req.email);
                    Ok(Json(AuthResponse {
                        token: session.token,
                        user_id: user.id,
                        username: user.username,
                    }))
                }
                Err(e) => {
                    warn!("Login after signup failed: {}", e);
                    Err((
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(ErrorResponse {
                            error: "Account created but login failed".to_string(),
                        }),
                    ))
                }
            }
        }
        Err(e) => {
            warn!("Signup failed for {}: {}", req.email, e);
            Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: e.to_string(),
                }),
            ))
        }
    }
}

pub async fn login(
    State(state): State<AppState>,
    Json(req): Json<LoginRequest>,
) -> Result<Json<AuthResponse>, (StatusCode, Json<ErrorResponse>)> {
    info!("POST /auth/login - {}", req.email);

    match state
        .auth
        .login(req.email.clone(), req.password.clone())
        .await
    {
        Ok((user, session)) => {
            info!("User {} logged in successfully", req.email);
            Ok(Json(AuthResponse {
                token: session.token.clone(),
                user_id: user.id,
                username: user.username,
            }))
        }
        Err(e) => {
            warn!("Login failed for {}: {}", req.email, e);
            Err((
                StatusCode::UNAUTHORIZED,
                Json(ErrorResponse {
                    error: "Invalid credentials".to_string(),
                }),
            ))
        }
    }
}

/// POST /auth/logout
pub async fn logout(State(_state): State<AppState>) -> StatusCode {
    info!("POST /auth/logout");
    // In a real implementation, we would invalidate the session
    // For now, just return OK
    StatusCode::OK
}

/// GET /auth/me
pub async fn me(
    State(state): State<AppState>,
    ctx: crate::core::ctx::Ctx,
) -> Result<Json<UserInfo>, crate::core::error::Error> {
    let user = state.auth.get_user(ctx.user_id()).await?;

    Ok(Json(user))
}

/// GET /users
pub async fn list_users(State(state): State<AppState>) -> Result<Json<Vec<UserInfo>>, StatusCode> {
    info!("GET /users");
    match state.auth.list_users().await {
        Ok(users) => Ok(Json(users)),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

#[derive(Debug, Deserialize)]
pub struct UpdateProfileRequest {
    pub username: Option<String>,
    pub email: Option<String>,
    pub password: Option<String>,
    pub avatar_blob_hash: Option<String>,
}

/// PUT /auth/profile
pub async fn update_profile(
    State(state): State<AppState>,
    // In a real app, extract user_id from token
    Path(user_id): Path<String>,
    Json(req): Json<UpdateProfileRequest>,
) -> Result<Json<UserInfo>, (StatusCode, Json<ErrorResponse>)> {
    info!("PUT /auth/profile - {}", user_id);

    match state
        .auth
        .update_user(
            &user_id,
            req.username,
            req.email,
            req.password,
            req.avatar_blob_hash,
        )
        .await
    {
        Ok(user) => Ok(Json(user)),
        Err(e) => Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )),
    }
}
