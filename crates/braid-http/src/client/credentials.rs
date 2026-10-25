//! Credentials are scoped to one HTTP origin and kept outside event journals.
use crate::{types::BraidRequest, BraidError, Result};
use serde::{Deserialize, Serialize};

#[derive(Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OriginCredentials {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bearer_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cookie: Option<String>,
}
impl std::fmt::Debug for OriginCredentials {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OriginCredentials")
            .field(
                "bearer_token",
                &self.bearer_token.as_ref().map(|_| "[redacted]"),
            )
            .field("cookie", &self.cookie.as_ref().map(|_| "[redacted]"))
            .finish()
    }
}
impl OriginCredentials {
    pub fn validate(&self) -> Result<()> {
        if self.bearer_token.is_none() && self.cookie.is_none() {
            return Err(BraidError::Config(
                "At least one credential is required".into(),
            ));
        }
        if let Some(token) = &self.bearer_token {
            if token.is_empty()
                || token.len() > 8192
                || !token.is_ascii()
                || token.chars().any(|c| c.is_control() || c.is_whitespace())
            {
                return Err(BraidError::Config("Invalid bearer credentials".into()));
            }
        }
        if let Some(cookie) = &self.cookie {
            if cookie.is_empty()
                || cookie.len() > 16384
                || !cookie.is_ascii()
                || cookie.chars().any(char::is_control)
            {
                return Err(BraidError::Config("Invalid cookie credentials".into()));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct ScopedCredentials {
    origin: String,
    credentials: OriginCredentials,
}
impl ScopedCredentials {
    /// Compatibility domain cookies have one safe default scope: HTTPS/443.
    pub fn legacy_cookie(domain: &str, value: &str) -> Result<Self> {
        let origin = format!("https://{domain}");
        let parsed = validate_resource_url(&origin)?;
        if parsed
            .host_str()
            .is_none_or(|host| host != domain.to_ascii_lowercase())
            || parsed.port().is_some()
        {
            return Err(BraidError::Config(
                "Legacy cookies require a hostname without a port or URL path".into(),
            ));
        }
        let cookie = if value.contains('=') {
            value.to_owned()
        } else if parsed
            .domain()
            .is_some_and(|host| host == "braid.org" || host.ends_with(".braid.org"))
        {
            format!("client={value}")
        } else {
            format!("token={value}")
        };
        Self::new(
            &origin,
            OriginCredentials {
                bearer_token: None,
                cookie: Some(cookie),
            },
        )
    }
    pub fn into_parts(self) -> (String, OriginCredentials) {
        (self.origin, self.credentials)
    }
    pub fn new(origin: &str, credentials: OriginCredentials) -> Result<Self> {
        credentials.validate()?;
        let parsed = validate_resource_url(origin)?;
        if parsed.path() != "/" || parsed.query().is_some() || parsed.fragment().is_some() {
            return Err(BraidError::Config(
                "Credential scope must be an origin without a path, query or fragment".into(),
            ));
        }
        Ok(Self {
            origin: http_origin(origin)?,
            credentials,
        })
    }
    pub fn origin(&self) -> &str {
        &self.origin
    }
    pub fn authenticate(&self, url: &str, mut request: BraidRequest) -> Result<BraidRequest> {
        if http_origin(url)? != self.origin {
            return Err(BraidError::Config(
                "Credentials do not authorize this origin".into(),
            ));
        }
        request.extra_headers.retain(|name, _| {
            !name.eq_ignore_ascii_case("authorization") && !name.eq_ignore_ascii_case("cookie")
        });
        if let Some(token) = &self.credentials.bearer_token {
            request = request.with_header("authorization", format!("Bearer {token}"));
        }
        if let Some(cookie) = &self.credentials.cookie {
            request = request.with_header("cookie", cookie);
        }
        Ok(request)
    }
}

/// URL userinfo can otherwise end up in errors, resource bindings and journals.
/// Authentication must be explicit, separate from the resource identity.
pub fn validate_resource_url(url: &str) -> Result<url::Url> {
    let parsed =
        url::Url::parse(url).map_err(|_| BraidError::Config("Invalid resource URL".into()))?;
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(BraidError::Config(
            "URL userinfo is not a credential store".into(),
        ));
    }
    Ok(parsed)
}
pub fn http_origin(url: &str) -> Result<String> {
    let parsed = validate_resource_url(url)?;
    if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() {
        return Err(BraidError::Config(
            "Credentials require an HTTP or HTTPS origin".into(),
        ));
    }
    Ok(parsed.origin().ascii_serialization())
}
