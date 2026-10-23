use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

#[derive(Debug)]
pub enum Error {
    // Auth Errors
    LoginFail,
    AuthFailNoToken,
    AuthFailTokenWrongFormat,
    AuthFailCtxNotInRequestExt,

    // Model Errors
    TicketDeleteFailIdNotFound { id: u64 },

    // Generic
    BadRequest(String),
    Internal(String),
    Braid(String),
}

pub type Result<T> = core::result::Result<T, Error>;

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let (status, error_message) = match self {
            Error::LoginFail => (StatusCode::UNAUTHORIZED, "Login failed".to_string()),
            Error::AuthFailNoToken => (StatusCode::UNAUTHORIZED, "No auth token found".to_string()),
            Error::AuthFailTokenWrongFormat => (
                StatusCode::UNAUTHORIZED,
                "Auth token wrong format".to_string(),
            ),
            Error::AuthFailCtxNotInRequestExt => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Auth context missing".to_string(),
            ),
            Error::TicketDeleteFailIdNotFound { .. } => {
                (StatusCode::BAD_REQUEST, "Ticket not found".to_string())
            }
            Error::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
            Error::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
            Error::Braid(msg) => (StatusCode::BAD_REQUEST, msg),
        };

        let body = Json(json!({
            "error": {
                "message": error_message
            }
        }));

        (status, body).into_response()
    }
}

// Allow conversion from other errors (e.g., anyhow, sqlx) easiest via string
impl From<anyhow::Error> for Error {
    fn from(err: anyhow::Error) -> Self {
        Error::Internal(err.to_string())
    }
}

impl From<String> for Error {
    fn from(err: String) -> Self {
        Error::Internal(err)
    }
}

impl From<braid_http::error::BraidError> for Error {
    fn from(err: braid_http::error::BraidError) -> Self {
        Error::Braid(err.to_string())
    }
}
