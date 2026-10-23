use crate::core::error::{Error, Result};
use axum::{extract::FromRequestParts, http::request::Parts};

#[derive(Clone, Debug)]
pub struct Ctx {
    user_id: String,
}

impl Ctx {
    pub fn new(user_id: String) -> Self {
        Self { user_id }
    }

    pub fn user_id(&self) -> &str {
        &self.user_id
    }
}

impl<S> FromRequestParts<S> for Ctx
where
    S: Send + Sync,
{
    type Rejection = Error;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self> {
        parts
            .extensions
            .get::<Ctx>()
            .cloned()
            .ok_or(Error::AuthFailCtxNotInRequestExt)
    }
}
