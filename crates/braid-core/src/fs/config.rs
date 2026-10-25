use crate::core::{BraidError, Result};
use braid_http::{
    client::{http_origin, OriginCredentials, ScopedCredentials},
    types::BraidRequest,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Serialize, Deserialize)]
struct ConfigMigration {
    schema_version: u32,
    original_sha256: String,
    migrated_sha256: String,
    rollback_allowed: bool,
}
fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
async fn read_config(path: &std::path::Path) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    fs::File::open(path)
        .await?
        .take(16 * 1024 * 1024 + 1)
        .read_to_end(&mut bytes)
        .await?;
    if bytes.len() > 16 * 1024 * 1024 {
        return Err(BraidError::Config("Config exceeds 16 MiB".into()));
    }
    Ok(bytes)
}
async fn rollback_candidate(path: &std::path::Path) -> Result<(Vec<u8>, String, bool)> {
    let record: ConfigMigration =
        parse_config(&read_config(&path.with_extension("migration.json")).await?)?;
    let original = read_config(&path.with_extension("v1.backup")).await?;
    let current = read_config(path).await?;
    if record.schema_version != 1
        || digest(&original) != record.original_sha256
        || !record.rollback_allowed
    {
        return Err(BraidError::Config(
            "Unverified or already used config migration cannot be rolled back".into(),
        ));
    }
    let current_hash = digest(&current);
    let needed = current_hash != record.original_sha256;
    if needed && current_hash != record.migrated_sha256 {
        return Err(BraidError::Config(
            "Config changed after migration; preserve it before rollback".into(),
        ));
    }
    Ok((original, current_hash, needed))
}
fn parse_config<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    serde_json::from_slice(bytes).map_err(|error| {
        BraidError::Config(format!(
            "Invalid configuration at line {}, column {}",
            error.line(),
            error.column()
        ))
    })
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Config {
    #[serde(default = "legacy_schema_version")]
    pub schema_version: u32,
    #[serde(default)]
    pub peer_id: String,
    #[serde(default)]
    pub sync: HashMap<String, bool>,
    #[serde(default)]
    pub cookies: HashMap<String, String>,
    #[serde(default)]
    pub origin_credentials: HashMap<String, OriginCredentials>,
    #[serde(default)]
    pub identities: HashMap<String, String>,
    #[serde(default = "default_port")]
    pub port: u16,
    /// Patterns to ignore (from .braidignore)
    #[serde(default)]
    pub ignore_patterns: Vec<String>,
    /// Debounce delay in milliseconds for file changes
    #[serde(default = "default_debounce_ms")]
    pub debounce_ms: u64,
    /// Preserve extension configuration without logging unrecognized values.
    #[serde(flatten)]
    pub extra: std::collections::BTreeMap<String, serde_json::Value>,
}

fn legacy_schema_version() -> u32 {
    1
}
impl std::fmt::Debug for Config {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.redacted().fmt(formatter)
    }
}

fn default_debounce_ms() -> u64 {
    10 // Live sync: 10ms debounce for near-instant collaboration
}

fn default_port() -> u16 {
    45678
}

impl Config {
    pub async fn load() -> Result<Self> {
        Self::load_from(&get_config_path()?).await
    }
    pub async fn load_from(config_path: &std::path::Path) -> Result<Self> {
        if !fs::try_exists(config_path).await? {
            return Ok(Config::default());
        }

        let content = read_config(config_path).await?;

        let mut config: Config = parse_config(&content)?;
        if !matches!(config.schema_version, 1 | 2) {
            return Err(BraidError::Config("Unsupported config schema".into()));
        }
        config.schema_version = 2;
        config.validate()?;

        if config.peer_id.is_empty() {
            config.peer_id = format!("braidfs_{}", &uuid::Uuid::new_v4().to_string()[..8]);
            config.save_to(config_path).await?;
        }

        // Validate cookies: warn if value looks like a URL (common mistake)
        for (domain, token) in &config.cookies {
            if token.starts_with("http://") || token.starts_with("https://") {
                tracing::warn!(
                    "Cookie for domain '{}' looks like a URL; expected a token string",
                    domain
                );
            }
        }

        Ok(config)
    }

    pub async fn save(&self) -> Result<()> {
        self.save_to(&get_config_path()?).await
    }
    pub async fn save_to(&self, config_path: &std::path::Path) -> Result<()> {
        self.save_profile(config_path, false, None).await
    }
    async fn save_profile(
        &self,
        config_path: &std::path::Path,
        migration_only: bool,
        expected: Option<&[u8]>,
    ) -> Result<()> {
        self.validate()?;
        if self.schema_version != 2 {
            return Err(BraidError::Config(
                "Config writes require schema version 2".into(),
            ));
        }
        let config_path = std::path::absolute(config_path)?;
        let _guard = braid_http::durable::document_lock(config_path.clone()).await;
        let _lease =
            braid_http::durable::writer_lease(&config_path.with_extension("write.lock")).await?;

        if let Some(parent) = config_path.parent() {
            fs::create_dir_all(parent).await.map_err(BraidError::Io)?;
        }

        let content = serde_json::to_vec_pretty(self).map_err(BraidError::Json)?;
        if content.len() > 16 * 1024 * 1024 {
            return Err(BraidError::Config("Config exceeds 16 MiB".into()));
        }
        match read_config(&config_path).await {
            Ok(original) => {
                if expected.is_some_and(|expected| original != expected) {
                    return Err(BraidError::Config(
                        "Config changed during migration preparation".into(),
                    ));
                }
                let old: Config = parse_config(&original)?;
                let version = old.schema_version;
                if !matches!(version, 1 | 2) {
                    return Err(BraidError::Config(
                        "Cannot overwrite a future config schema".into(),
                    ));
                }
                if version == 1 {
                    let backup = config_path.with_extension("v1.backup");
                    let mut options = fs::OpenOptions::new();
                    options.write(true).create_new(true);
                    #[cfg(unix)]
                    options.mode(0o600);
                    match options.open(&backup).await {
                        Ok(mut file) => {
                            file.write_all(&original).await?;
                            file.sync_all().await?;
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                            if read_config(&backup).await? != original {
                                return Err(BraidError::Config(
                                    "Legacy config backup differs; review before migration".into(),
                                ));
                            }
                        }
                        Err(error) => return Err(error.into()),
                    }
                    let record = ConfigMigration {
                        schema_version: 1,
                        original_sha256: digest(&original),
                        migrated_sha256: digest(&content),
                        rollback_allowed: migration_only,
                    };
                    braid_http::durable::atomic_write(
                        &config_path.with_extension("migration.json"),
                        &serde_json::to_vec_pretty(&record)?,
                    )
                    .await?;
                }
            }
            Err(BraidError::Io(error))
                if error.kind() == std::io::ErrorKind::NotFound && expected.is_none() => {}
            Err(error) => return Err(error),
        }
        braid_http::durable::atomic_write(&config_path, &content)
            .await
            .map_err(BraidError::Io)?;

        Ok(())
    }

    /// Read-only preparation/validation, suitable for a local dry-run command.
    pub async fn migration_info(path: &std::path::Path) -> Result<serde_json::Value> {
        let path = std::path::absolute(path)?;
        let bytes = read_config(&path).await?;
        let config: Self = parse_config(&bytes)?;
        if !matches!(config.schema_version, 1 | 2) {
            return Err(BraidError::Config("Unsupported config schema".into()));
        }
        config.validate()?;
        Ok(
            serde_json::json!({"source":path, "source_schema":config.schema_version, "target_schema":2,
            "sha256":digest(&bytes), "backup":path.with_extension("v1.backup"), "configuration":config.redacted()}),
        )
    }
    /// Explicit format-only migration. Repeated application is idempotent.
    pub async fn migrate_file(path: &std::path::Path) -> Result<serde_json::Value> {
        let path = std::path::absolute(path)?;
        let parent = path
            .parent()
            .ok_or_else(|| BraidError::Config("Config needs a parent directory".into()))?;
        let _offline = braid_http::durable::writer_lease(&parent.join("daemon.lock")).await?;
        let bytes = read_config(&path).await?;
        let mut config: Self = parse_config(&bytes)?;
        if config.schema_version == 2 {
            return Self::migration_info(&path).await;
        }
        if config.schema_version != 1 {
            return Err(BraidError::Config("Unsupported config schema".into()));
        }
        config.schema_version = 2;
        if config.peer_id.is_empty() {
            config.peer_id = format!("braidfs_{}", &uuid::Uuid::new_v4().to_string()[..8]);
        }
        config.save_profile(&path, true, Some(&bytes)).await?;
        Self::migration_info(&path).await
    }
    /// Restore the exact original only before subsequent configuration writes.
    pub async fn rollback_info(path: &std::path::Path) -> Result<serde_json::Value> {
        let path = std::path::absolute(path)?;
        let (original, current_hash, needed) = rollback_candidate(&path).await?;
        let config: Self = parse_config(&original)?;
        config.validate()?;
        Ok(
            serde_json::json!({"source":path, "target_schema":1, "current_sha256":current_hash,
            "original_sha256":digest(&original), "rollback_needed":needed, "configuration":config.redacted()}),
        )
    }
    /// Restore the exact original only before subsequent configuration writes.
    pub async fn rollback_file(path: &std::path::Path) -> Result<serde_json::Value> {
        let path = std::path::absolute(path)?;
        let parent = path
            .parent()
            .ok_or_else(|| BraidError::Config("Config needs a parent directory".into()))?;
        let _offline = braid_http::durable::writer_lease(&parent.join("daemon.lock")).await?;
        let _guard = braid_http::durable::document_lock(path.clone()).await;
        let _lease = braid_http::durable::writer_lease(&path.with_extension("write.lock")).await?;
        let (original, _, needed) = rollback_candidate(&path).await?;
        if needed {
            braid_http::durable::atomic_write(&path, &original).await?;
        }
        Self::rollback_info(&path).await
    }

    pub fn validate(&self) -> Result<()> {
        if self.extra.keys().any(|key| {
            matches!(
                key.as_str(),
                "schema_version"
                    | "peer_id"
                    | "sync"
                    | "cookies"
                    | "origin_credentials"
                    | "identities"
                    | "port"
                    | "ignore_patterns"
                    | "debounce_ms"
            )
        }) {
            return Err(BraidError::Config(
                "Extension configuration shadows a reserved field".into(),
            ));
        }
        for url in self.sync.keys() {
            braid_http::client::validate_resource_url(url)?;
        }
        for (origin, credentials) in &self.origin_credentials {
            let scoped = ScopedCredentials::new(origin, credentials.clone())?;
            if scoped.origin() != origin {
                return Err(BraidError::Config(
                    "Credential keys must be canonical origins".into(),
                ));
            }
        }
        Ok(())
    }

    pub fn authenticate(&self, mut request: BraidRequest, url: &str) -> Result<BraidRequest> {
        let origin = http_origin(url)?;
        request.extra_headers.retain(|name, _| {
            !name.eq_ignore_ascii_case("authorization") && !name.eq_ignore_ascii_case("cookie")
        });
        if let Some(credentials) = self.origin_credentials.get(&origin) {
            return Ok(
                ScopedCredentials::new(&origin, credentials.clone())?.authenticate(url, request)?
            );
        }
        let parsed = braid_http::client::validate_resource_url(url)?;
        // Legacy domain cookies have no scheme/port binding. Retain them only
        // for the original HTTPS/default-port scope; explicit origins cover IPC.
        if parsed.scheme() == "https" && parsed.port_or_known_default() == Some(443) {
            if let Some(domain) = parsed.domain() {
                if let Some(token) = self.cookies.get(domain) {
                    return Ok(ScopedCredentials::legacy_cookie(domain, token)?
                        .authenticate(url, request)?);
                }
            }
        }
        Ok(request)
    }

    pub fn redacted(&self) -> serde_json::Value {
        let cookies: std::collections::BTreeMap<_, _> =
            self.cookies.keys().map(|key| (key, "[redacted]")).collect();
        let credentials: std::collections::BTreeMap<_, _> = self
            .origin_credentials
            .keys()
            .map(|key| (key, "[redacted]"))
            .collect();
        serde_json::json!({"schema_version":self.schema_version, "peer_id":self.peer_id, "sync":self.sync,
            "cookies":cookies, "origin_credentials":credentials, "identities":self.identities,
            "port":self.port, "ignore_patterns":self.ignore_patterns, "debounce_ms":self.debounce_ms})
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            schema_version: 2,
            peer_id: format!("braidfs_{}", &uuid::Uuid::new_v4().to_string()[..8]),
            sync: HashMap::new(),
            cookies: HashMap::new(),
            origin_credentials: HashMap::new(),
            identities: HashMap::new(),
            port: default_port(),
            ignore_patterns: default_ignore_patterns(),
            debounce_ms: default_debounce_ms(),
            extra: Default::default(),
        }
    }
}

/// Default patterns to ignore (.git, node_modules, etc.)
fn default_ignore_patterns() -> Vec<String> {
    vec![
        ".git".to_string(),
        ".git/**".to_string(),
        "node_modules/**".to_string(),
        ".DS_Store".to_string(),
        "*.swp".to_string(),
        "*.swo".to_string(),
        "*~".to_string(),
        ".braidfs/**".to_string(),
        "*.sqlite".to_string(),
        "*.sqlite-journal".to_string(),
        "*.db".to_string(),
        "*.db-journal".to_string(),
    ]
}

pub fn get_config_path() -> Result<PathBuf> {
    let root = get_root_dir()?;
    Ok(root.join(".braidfs").join("config"))
}

pub fn get_root_dir() -> Result<PathBuf> {
    let root_str = std::env::var("BRAID_ROOT").unwrap_or_else(|_| "braid_data".to_string());

    let root = PathBuf::from(root_str);
    match std::fs::canonicalize(&root) {
        Ok(abs) => Ok(abs),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Ok(std::path::absolute(root)?)
        }
        Err(error) => Err(error.into()),
    }
}

/// Get the trash directory for deleted files.
pub fn get_trash_dir() -> Result<PathBuf> {
    let root = get_root_dir()?;
    Ok(root.join(".braidfs").join("trash"))
}

/// Check if a file is binary based on its extension.
pub fn is_binary(filename: &str) -> bool {
    let binary_extensions = [
        ".jpg", ".jpeg", ".png", ".gif", ".mp4", ".mp3", ".zip", ".tar", ".rar", ".pdf", ".doc",
        ".docx", ".xls", ".xlsx", ".ppt", ".pptx", ".exe", ".dll", ".so", ".dylib", ".bin", ".iso",
        ".img", ".bmp", ".tiff", ".svg", ".webp", ".avi", ".mov", ".wmv", ".flv", ".mkv", ".wav",
        ".flac", ".aac", ".ogg", ".wma", ".7z", ".gz", ".bz2", ".xz",
    ];

    let filename_lower = filename.to_lowercase();
    binary_extensions
        .iter()
        .any(|ext| filename_lower.ends_with(ext))
}

/// Check if a path should be skipped during sync.
pub fn skip_file(path: &str) -> bool {
    if path.contains('#') {
        return true;
    }
    if path.ends_with(".DS_Store") {
        return true;
    }
    if path == ".braidfs" || path.starts_with(".braidfs/") || path.starts_with(".braidfs\\") {
        return true;
    }
    if path.ends_with(".sqlite")
        || path.ends_with(".sqlite-journal")
        || path.ends_with(".db")
        || path.ends_with(".db-journal")
        || path.ends_with(".tmp")
    {
        return true;
    }
    false
}

/// Move a file to the trash directory instead of deleting it.
pub async fn trash_file(fullpath: &std::path::Path, path: &str) -> Result<PathBuf> {
    let trash_dir = get_trash_dir()?;
    tokio::fs::create_dir_all(&trash_dir).await?;

    let random = uuid::Uuid::new_v4().to_string()[..8].to_string();
    let filename = path.replace(['/', '\\'], "_");
    let dest = trash_dir.join(format!("{}_{}", filename, random));

    tokio::fs::rename(fullpath, &dest).await?;
    tracing::warn!("Moved unsynced file to trash: {:?} -> {:?}", fullpath, dest);

    Ok(dest)
}
