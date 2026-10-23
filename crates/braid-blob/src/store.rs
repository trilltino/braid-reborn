use async_trait::async_trait;
use braid_http::error::{BraidError, Result};
use braid_http::traits::BraidStorage;
use braid_http::types::Version;
use bytes::Bytes;
#[cfg(feature = "native")]
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
#[cfg(feature = "native")]
use std::path::PathBuf;
use std::sync::Arc;
#[cfg(feature = "native")]
use tokio::fs;
#[cfg(feature = "native")]
use tokio::sync::Mutex;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BlobMetadata {
    pub key: String,
    pub version: Vec<Version>,
    pub content_type: Option<String>,
    pub parents: Vec<Version>,
    /// SHA-256 hash of blob content for deduplication.
    #[serde(default)]
    pub content_hash: Option<String>,
    /// Size of the blob in bytes.
    #[serde(default)]
    pub size: Option<u64>,
}

/// Upload policy and immutable event identity. Limits are checked while streaming.
#[derive(Clone, Debug)]
#[cfg(feature = "native")]
pub struct BlobUpload {
    pub version: Vec<Version>,
    pub parents: Vec<Version>,
    pub content_type: Option<String>,
    pub expected_hash: Option<String>,
    pub max_bytes: u64,
}

#[cfg(feature = "native")]
impl BlobUpload {
    pub fn new(version: Vec<Version>, parents: Vec<Version>, content_type: Option<String>) -> Self {
        Self {
            version,
            parents,
            content_type,
            expected_hash: None,
            max_bytes: crate::objects::DEFAULT_BUFFER_LIMIT,
        }
    }
}

#[derive(Clone, Debug)]
#[cfg(feature = "native")]
pub struct BlobStore {
    objects: crate::objects::Objects,
    meta_conn: Arc<Mutex<Connection>>,
}

#[cfg(feature = "native")]
impl BlobStore {
    pub async fn new(db_path: PathBuf, meta_db_path: PathBuf) -> Result<Self> {
        let base = std::env::current_dir()?;
        let db_path = base.join(db_path);
        let meta_db_path = base.join(meta_db_path);
        fs::create_dir_all(&db_path).await?;
        if let Some(parent) = meta_db_path.parent() {
            fs::create_dir_all(parent).await?;
        }
        let conn = tokio::task::spawn_blocking(move || -> Result<Connection> {
            let conn = Connection::open(meta_db_path).map_err(db_error)?;
            conn.busy_timeout(std::time::Duration::from_secs(5))
                .map_err(db_error)?;
            conn.execute_batch(
                "PRAGMA synchronous=FULL;
                CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value JSON);
                CREATE TABLE IF NOT EXISTS events (key TEXT NOT NULL, version TEXT NOT NULL,
                    fingerprint TEXT NOT NULL, PRIMARY KEY(key,version));",
            )
            .map_err(db_error)?;
            Ok(conn)
        })
        .await
        .map_err(|e| BraidError::Fs(e.to_string()))??;
        Ok(Self {
            objects: crate::objects::Objects::new(db_path),
            meta_conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// SQLite work runs on the blocking pool, including transaction rollback.
    /// Once started it completes even if the awaiting HTTP request is cancelled.
    async fn database<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&mut Connection) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let mut connection = self.meta_conn.clone().lock_owned().await;
        tokio::task::spawn_blocking(move || operation(&mut connection))
            .await
            .map_err(|e| BraidError::Fs(e.to_string()))?
    }

    pub async fn get_meta(&self, key: &str) -> Result<Option<BlobMetadata>> {
        let key = key.to_owned();
        self.database(move |conn| {
            let value: Option<String> = conn
                .query_row("SELECT value FROM meta WHERE key=?", params![key], |row| {
                    row.get(0)
                })
                .optional()
                .map_err(db_error)?;
            value
                .map(|value| serde_json::from_str(&value).map_err(BraidError::from))
                .transpose()
        })
        .await
    }

    /// Verify an immutable payload with a fixed 64 KiB buffer, then rewind it.
    /// Consumers can stream large files without collecting their contents.
    pub async fn open(&self, key: &str) -> Result<Option<(fs::File, BlobMetadata)>> {
        let Some(meta) = self.get_meta(key).await? else {
            return Ok(None);
        };
        let file = self.objects.open(&meta).await?;
        Ok(Some((file, meta)))
    }

    /// Convenience collection API, capped at 64 MiB. Use `open` for larger objects.
    pub async fn get(&self, key: &str) -> Result<Option<(Bytes, BlobMetadata)>> {
        use tokio::io::AsyncReadExt;
        let Some((file, meta)) = self.open(key).await? else {
            return Ok(None);
        };
        let limit = crate::objects::DEFAULT_BUFFER_LIMIT;
        if file.metadata().await?.len() > limit {
            return Err(BraidError::ResourceLimit {
                resource: "buffered blob read",
                limit,
            });
        }
        let mut bytes = Vec::new();
        file.take(limit + 1).read_to_end(&mut bytes).await?;
        if bytes.len() as u64 > limit {
            return Err(BraidError::ResourceLimit {
                resource: "buffered blob read",
                limit,
            });
        }
        if let Some(expected) = &meta.content_hash {
            if format!("{:x}", Sha256::digest(&bytes)) != *expected {
                return Err(BraidError::Fs("Checksum mismatch for blob".into()));
            }
        }
        Ok(Some((Bytes::from(bytes), meta)))
    }

    pub async fn put(
        &self,
        key: &str,
        data: Bytes,
        version: Vec<Version>,
        parents: Vec<Version>,
        content_type: Option<String>,
    ) -> Result<Vec<Version>> {
        self.put_stream(
            key,
            std::io::Cursor::new(data),
            BlobUpload::new(version, parents, content_type),
        )
        .await
    }

    /// Stage and hash the entire bounded stream before publishing its reference.
    /// Unreferenced objects are retained after failures; committed payloads are
    /// never garbage-collected implicitly because older versions may need them.
    pub async fn put_stream<R: tokio::io::AsyncRead + Unpin>(
        &self,
        key: &str,
        reader: R,
        upload: BlobUpload,
    ) -> Result<Vec<Version>> {
        let BlobUpload {
            version,
            parents,
            content_type,
            expected_hash,
            max_bytes,
        } = upload;
        if version.len() != 1
            || version[0].to_string().is_empty()
            || parents.contains(&version[0])
            || parents
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len()
                != parents.len()
        {
            return Err(BraidError::Protocol(
                "Blob update requires a unique, non-self-parented event".into(),
            ));
        }
        let (hash, size) = self
            .objects
            .stage(reader, max_bytes, expected_hash.as_deref())
            .await?;
        let key = key.to_owned();
        self.database(move |conn| {
            let tx = conn
                .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                .map_err(db_error)?;
            let id = version[0].to_string();
            let mut canonical_parents = parents.clone();
            canonical_parents.sort_by_key(ToString::to_string);
            let fingerprint = format!(
                "{:x}",
                Sha256::digest(serde_json::to_vec(&(
                    &version,
                    &canonical_parents,
                    &hash,
                    &content_type
                ))?)
            );
            let previous: Option<String> = tx
                .query_row(
                    "SELECT fingerprint FROM events WHERE key=? AND version=?",
                    params![key, id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(db_error)?;
            if let Some(previous) = previous {
                return if previous == fingerprint {
                    Ok(version)
                } else {
                    Err(BraidError::Protocol(
                        "Blob version reused with different payload".into(),
                    ))
                };
            }
            let current: Option<String> = tx
                .query_row("SELECT value FROM meta WHERE key=?", params![key], |row| {
                    row.get(0)
                })
                .optional()
                .map_err(db_error)?;
            if let Some(current) = current {
                let current: BlobMetadata = serde_json::from_str(&current)?;
                if same_frontier(&current.version, &version) {
                    return if current.content_hash.as_deref() == Some(&hash)
                        && same_frontier(&current.parents, &parents)
                        && current.content_type == content_type
                    {
                        Ok(version)
                    } else {
                        Err(BraidError::Protocol(
                            "Blob version reused with different content".into(),
                        ))
                    };
                }
                if !same_frontier(&current.version, &parents) {
                    return Err(BraidError::Protocol(
                        "Concurrent blob update requires explicit conflict resolution".into(),
                    ));
                }
            } else if !parents.is_empty() {
                return Err(BraidError::Protocol("Unknown blob parents".into()));
            }
            let meta = BlobMetadata {
                key: key.clone(),
                version: version.clone(),
                parents,
                content_type,
                content_hash: Some(hash),
                size: Some(size),
            };
            tx.execute(
                "INSERT OR REPLACE INTO meta (key,value) VALUES (?,?)",
                params![key, serde_json::to_string(&meta)?],
            )
            .map_err(db_error)?;
            tx.execute(
                "INSERT INTO events (key,version,fingerprint) VALUES (?,?,?)",
                params![key, id, fingerprint],
            )
            .map_err(db_error)?;
            tx.commit().map_err(db_error)?;
            Ok(version)
        })
        .await
    }

    /// Remove the current reference. Historical immutable payloads/events remain retained.
    pub async fn delete(&self, key: &str) -> Result<()> {
        let key = key.to_owned();
        self.database(move |conn| {
            conn.execute("DELETE FROM meta WHERE key=?", params![key])
                .map_err(db_error)?;
            Ok(())
        })
        .await
    }
}

#[cfg(feature = "native")]
fn db_error(error: rusqlite::Error) -> BraidError {
    BraidError::Fs(error.to_string())
}

#[cfg(feature = "native")]
#[async_trait]
impl BraidStorage for BlobStore {
    async fn put(&self, key: &str, data: bytes::Bytes, meta: String) -> Result<()> {
        let metadata: BlobMetadata =
            serde_json::from_str(&meta).map_err(|e| BraidError::Config(e.to_string()))?;
        self.put(
            key,
            data,
            metadata.version,
            metadata.parents,
            metadata.content_type,
        )
        .await
        .map(|_| ())
    }

    async fn get(&self, key: &str) -> Result<Option<(bytes::Bytes, String)>> {
        if let Some((data, meta)) = self.get(key).await? {
            let meta_str =
                serde_json::to_string(&meta).map_err(|e| BraidError::Config(e.to_string()))?;
            Ok(Some((data, meta_str)))
        } else {
            Ok(None)
        }
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.delete(key).await
    }

    async fn list_keys(&self) -> Result<Vec<String>> {
        self.database(|conn| {
            let mut statement = conn
                .prepare("SELECT key FROM meta ORDER BY key")
                .map_err(db_error)?;
            let rows = statement
                .query_map([], |row| row.get(0))
                .map_err(db_error)?;
            rows.collect::<std::result::Result<Vec<String>, _>>()
                .map_err(db_error)
        })
        .await
    }
}

fn same_frontier(a: &[Version], b: &[Version]) -> bool {
    a.iter().collect::<std::collections::HashSet<_>>()
        == b.iter().collect::<std::collections::HashSet<_>>()
}

pub fn encode_filename(s: &str) -> String {
    let bits: String = s
        .chars()
        .filter(|c| c.is_alphabetic())
        .map(|c| if c.is_uppercase() { "1" } else { "0" })
        .collect();

    let postfix = if bits.is_empty() {
        "0".to_string()
    } else {
        bits_to_hex(&bits)
    };

    let s_swapped = s
        .chars()
        .map(|c| match c {
            '/' => '!',
            '!' => '/',
            _ => c,
        })
        .collect::<String>();

    let mut encoded = String::new();
    for c in s_swapped.chars() {
        if matches!(
            c,
            '<' | '>' | ':' | '"' | '/' | '|' | '\\' | '?' | '*' | '%' | '\x00'..='\x1f' | '\x7f'
        ) {
            encoded.push_str(&format!("%{:02X}", c as u8));
        } else {
            encoded.push(c);
        }
    }
    let mut s = encoded;

    let is_reserved = {
        let lower = s.to_lowercase();
        let name_part = lower.split('.').next().unwrap_or("");
        matches!(name_part, "con" | "prn" | "aux" | "nul")
            || (name_part.len() == 4
                && name_part.starts_with("com")
                && name_part
                    .chars()
                    .nth(3)
                    .is_some_and(|c| c.is_ascii_digit() && c != '0'))
            || (name_part.len() == 4
                && name_part.starts_with("lpt")
                && name_part
                    .chars()
                    .nth(3)
                    .is_some_and(|c| c.is_ascii_digit() && c != '0'))
    };

    if is_reserved {
        if let Some(char_at_2) = s.chars().nth(2) {
            let encoded_char = format!("%{:02X}", char_at_2 as u8);
            let chars: Vec<char> = s.chars().collect();
            let prefix: String = chars.iter().take(2).collect();
            let suffix: String = chars.iter().skip(3).collect();
            s = format!("{}{}{}", prefix, encoded_char, suffix);
        }
    }

    format!("{}.{}", s, postfix)
}

fn bits_to_hex(bits: &str) -> String {
    if bits.is_empty() {
        return "0".to_string();
    }

    let rem = bits.len() % 4;
    let padded = if rem == 0 {
        bits.to_string()
    } else {
        format!("{}{}", "0".repeat(4 - rem), bits)
    };

    let mut hex = String::new();
    for chunk in padded.as_bytes().chunks(4) {
        // The caller constructs this string exclusively from zero/one bits.
        let val = chunk
            .iter()
            .fold(0u8, |value, bit| (value << 1) | u8::from(*bit == b'1'));
        hex.push_str(&format!("{:x}", val));
    }

    let trimmed = hex.trim_start_matches('0');
    if trimmed.is_empty() {
        "0".to_string()
    } else {
        trimmed.to_string()
    }
}

pub async fn atomic_write(
    dest: &std::path::Path,
    data: &[u8],
    temp_folder: &std::path::Path,
) -> Result<std::fs::Metadata> {
    let _ = temp_folder;
    braid_http::durable::atomic_write(dest, data).await?;
    Ok(tokio::fs::metadata(dest).await?)
}
