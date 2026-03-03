//! Friend Request Handlers

use crate::chat::friends::{Contact, FriendRequest};
use crate::core::config::AppState;
use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    Json,
};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

/// Request to send friend request
#[derive(Debug, Deserialize)]
pub struct SendFriendRequest {
    pub to_email: String,
    pub message: Option<String>,
}

/// Request to respond to friend request
#[derive(Debug, Deserialize)]
pub struct RespondRequest {
    pub action: String, // "accept" or "reject"
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

fn get_token_from_headers(headers: &HeaderMap) -> Option<String> {
    headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(|s| s.to_string())
}

/// POST /friends/requests - Send a friend request
pub async fn send_friend_request(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<SendFriendRequest>,
) -> Result<Json<FriendRequest>, (StatusCode, Json<ErrorResponse>)> {
    let token = get_token_from_headers(&headers).unwrap_or_default();

    if token.is_empty() {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(ErrorResponse {
                error: "Missing authorization".to_string(),
            }),
        ));
    }

    // Validate session and get user
    let user = match state.auth.validate_session(&token).await {
        Ok(u) => u,
        Err(_) => {
            return Err((
                StatusCode::UNAUTHORIZED,
                Json(ErrorResponse {
                    error: "Invalid session".to_string(),
                }),
            ))
        }
    };

    match state
        .friends
        .send_request(user.id, req.to_email, req.message)
        .await
    {
        Ok(request) => {
            info!(
                "Friend request sent from {} to {}",
                user.username, request.to_email
            );
            Ok(Json(request))
        }
        Err(e) => {
            warn!("Failed to send friend request: {}", e);
            Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: e.to_string(),
                }),
            ))
        }
    }
}

/// GET /friends/requests - Get pending friend requests
pub async fn list_pending_requests(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Vec<FriendRequest>>, (StatusCode, Json<ErrorResponse>)> {
    let token = get_token_from_headers(&headers).unwrap_or_default();

    if token.is_empty() {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(ErrorResponse {
                error: "Missing authorization".to_string(),
            }),
        ));
    }

    let user = match state.auth.validate_session(&token).await {
        Ok(u) => u,
        Err(_) => {
            return Err((
                StatusCode::UNAUTHORIZED,
                Json(ErrorResponse {
                    error: "Invalid session".to_string(),
                }),
            ))
        }
    };

    match state.friends.get_pending_requests(&user.id).await {
        Ok(requests) => Ok(Json(requests)),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )),
    }
}

/// PUT /friends/requests/{request_id} - Accept or reject friend request
pub async fn respond_friend_request(
    Path(request_id): Path<String>,
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<RespondRequest>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    let token = get_token_from_headers(&headers).unwrap_or_default();

    if token.is_empty() {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(ErrorResponse {
                error: "Missing authorization".to_string(),
            }),
        ));
    }

    // Validate the user owns this request
    let _user = match state.auth.validate_session(&token).await {
        Ok(u) => u,
        Err(_) => {
            return Err((
                StatusCode::UNAUTHORIZED,
                Json(ErrorResponse {
                    error: "Invalid session".to_string(),
                }),
            ))
        }
    };

    let accept = req.action == "accept";

    match state.friends.respond_to_request(&request_id, accept).await {
        Ok(_) => {
            info!(
                "Friend request {} {}",
                request_id,
                if accept { "accepted" } else { "rejected" }
            );
            Ok(StatusCode::OK)
        }
        Err(e) => Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )),
    }
}

/// GET /friends - Get user's contacts (friends)
pub async fn list_friends(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Vec<Contact>>, (StatusCode, Json<ErrorResponse>)> {
    let token = get_token_from_headers(&headers).unwrap_or_default();

    if token.is_empty() {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(ErrorResponse {
                error: "Missing authorization".to_string(),
            }),
        ));
    }

    let user = match state.auth.validate_session(&token).await {
        Ok(u) => u,
        Err(_) => {
            return Err((
                StatusCode::UNAUTHORIZED,
                Json(ErrorResponse {
                    error: "Invalid session".to_string(),
                }),
            ))
        }
    };

    match state.friends.get_contacts(&user.id).await {
        Ok(contacts) => Ok(Json(contacts)),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )),
    }
}
