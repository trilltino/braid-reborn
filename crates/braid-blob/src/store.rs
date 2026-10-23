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

#[derive(Clone, Debug)]
#[cfg(feature = "native")]
pub struct BlobStore {
    db_path: PathBuf,
    _meta_db_path: PathBuf,
    meta_conn: Arc<Mutex<Connection>>,
}

#[cfg(feature = "native")]
struct BlobTransaction {
    conn: tokio::sync::OwnedMutexGuard<Connection>,
    committed: bool,
}
#[cfg(feature = "native")]
impl Drop for BlobTransaction {
    fn drop(&mut self) {
        if !self.committed {
            let _ = self.conn.execute_batch("ROLLBACK");
        }
    }
}

#[cfg(feature = "native")]
impl BlobStore {
    pub async fn new(db_path: PathBuf, meta_db_path: PathBuf) -> Result<Self> {
        let db_path = if db_path.is_absolute() {
            db_path
        } else {
            std::env::current_dir()?.join(db_path)
        };
        let meta_db_path = if meta_db_path.is_absolute() {
            meta_db_path
        } else {
            std::env::current_dir()?.join(meta_db_path)
        };
        fs::create_dir_all(&db_path)
            .await
            .map_err(|e| BraidError::Io(e))?;
        if let Some(parent) = meta_db_path.parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(|e| BraidError::Io(e))?;
        }
        let conn = Connection::open(&meta_db_path).map_err(|e| BraidError::Fs(e.to_string()))?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value JSON
            )",
            [],
        )
        .map_err(|e| BraidError::Fs(e.to_string()))?;

        conn.busy_timeout(std::time::Duration::from_secs(5))
            .map_err(|e| BraidError::Fs(e.to_string()))?;
        conn.execute_batch("PRAGMA synchronous=FULL; CREATE TABLE IF NOT EXISTS events (key TEXT NOT NULL, version TEXT NOT NULL, fingerprint TEXT NOT NULL, PRIMARY KEY(key,version));").map_err(|e|BraidError::Fs(e.to_string()))?;
        Ok(Self {
            db_path,
            _meta_db_path: meta_db_path,
            meta_conn: Arc::new(Mutex::new(conn)),
        })
    }

    pub async fn get(&self, key: &str) -> Result<Option<(Bytes, BlobMetadata)>> {
        let meta = {
            let conn = self.meta_conn.lock().await;
            let mut stmt = conn
                .prepare("SELECT value FROM meta WHERE key = ?")
                .map_err(|e| BraidError::Fs(e.to_string()))?;
            let mut rows = stmt
                .query(params![key])
                .map_err(|e| BraidError::Fs(e.to_string()))?;

            if let Some(row) = rows.next().map_err(|e| BraidError::Fs(e.to_string()))? {
                let value_str: String = row.get(0).map_err(|e| BraidError::Fs(e.to_string()))?;
                serde_json::from_str::<BlobMetadata>(&value_str)
                    .map_err(|e| BraidError::Config(e.to_string()))?
            } else {
                return Ok(None);
            }
        };

        let object_path = meta
            .content_hash
            .as_ref()
            .map(|h| self.db_path.join("objects").join(h));
        let file_path = if let Some(path) = object_path {
            if fs::try_exists(&path).await? {
                path
            } else {
                self.get_file_path(key)
            }
        } else {
            self.get_file_path(key)
        };
        if fs::try_exists(&file_path)
            .await
            .map_err(|e| BraidError::Io(e))?
        {
            let data = fs::read(&file_path).await.map_err(|e| BraidError::Io(e))?;

            // Verify content hash if available
            if let Some(expected_hash) = &meta.content_hash {
                let mut hasher = Sha256::new();
                hasher.update(&data);
                let actual_hash = format!("{:x}", hasher.finalize());
                if &actual_hash != expected_hash {
                    return Err(BraidError::Fs(format!(
                        "Checksum mismatch for blob {}: expected {}, got {}",
                        key, expected_hash, actual_hash
                    )));
                }
            }

            Ok(Some((Bytes::from(data), meta)))
        } else {
            Err(BraidError::Fs(format!(
                "Missing committed blob data for {key}"
            )))
        }
    }

    pub async fn get_meta(&self, key: &str) -> Result<Option<BlobMetadata>> {
        let conn = self.meta_conn.lock().await;
        let mut stmt = conn
            .prepare("SELECT value FROM meta WHERE key = ?")
            .map_err(|e| BraidError::Fs(e.to_string()))?;
        let mut rows = stmt
            .query(params![key])
            .map_err(|e| BraidError::Fs(e.to_string()))?;

        if let Some(row) = rows.next().map_err(|e| BraidError::Fs(e.to_string()))? {
            let value_str: String = row.get(0).map_err(|e| BraidError::Fs(e.to_string()))?;
            Ok(Some(
                serde_json::from_str::<BlobMetadata>(&value_str)
                    .map_err(|e| BraidError::Config(e.to_string()))?,
            ))
        } else {
            Ok(None)
        }
    }

    pub async fn put(
        &self,
        key: &str,
        data: Bytes,
        version: Vec<Version>,
        parents: Vec<Version>,
        content_type: Option<String>,
    ) -> Result<Vec<Version>> {
        if version.len() != 1 || version[0].to_string().is_empty() || parents.contains(&version[0])
        {
            return Err(BraidError::Protocol(
                "Blob update requires a unique, non-self-parented event".into(),
            ));
        }
        let content_hash = format!("{:x}", Sha256::digest(&data));
        // Serialize compare/commit across every handle sharing this metadata database.
        let _guard = braid_http::durable::document_lock(self._meta_db_path.clone()).await;
        let conn = self.meta_conn.clone().lock_owned().await;
        conn.execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| BraidError::Fs(e.to_string()))?;
        let mut transaction = BlobTransaction {
            conn,
            committed: false,
        };
        let id = version[0].to_string();
        let mut canonical_parents = parents.clone();
        canonical_parents.sort_by_key(ToString::to_string);
        let fingerprint = format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&(
                &version,
                &canonical_parents,
                &content_hash,
                &content_type
            ))?)
        );
        let old_fingerprint: Option<String> = transaction
            .conn
            .query_row(
                "SELECT fingerprint FROM events WHERE key=? AND version=?",
                params![key, id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| BraidError::Fs(e.to_string()))?;
        if let Some(old) = old_fingerprint {
            return if old == fingerprint {
                Ok(version)
            } else {
                Err(BraidError::Protocol(
                    "Blob version reused with different payload".into(),
                ))
            };
        }
        let current_meta: Option<String> = transaction
            .conn
            .query_row("SELECT value FROM meta WHERE key=?", params![key], |row| {
                row.get(0)
            })
            .optional()
            .map_err(|e| BraidError::Fs(e.to_string()))?;
        let current_meta = current_meta
            .map(|json| serde_json::from_str::<BlobMetadata>(&json))
            .transpose()?;
        if let Some(meta) = &current_meta {
            if same_frontier(&meta.version, &version) {
                return if meta.content_hash.as_deref() == Some(&content_hash)
                    && same_frontier(&meta.parents, &parents)
                    && meta.content_type == content_type
                {
                    Ok(meta.version.clone())
                } else {
                    Err(BraidError::Protocol(
                        "Blob version reused with different content".into(),
                    ))
                };
            }
            if !same_frontier(&meta.version, &parents) {
                return Err(BraidError::Protocol(
                    "Concurrent blob update requires explicit conflict resolution".into(),
                ));
            }
        } else if !parents.is_empty() {
            return Err(BraidError::Protocol("Unknown blob parents".into()));
        }
        let new_meta = BlobMetadata {
            key: key.into(),
            version: version.clone(),
            content_type,
            parents,
            content_hash: Some(content_hash.clone()),
            size: Some(data.len() as u64),
        };
        // Immutable objects are durable before their SQLite reference becomes visible.
        // A crash before the reference commit leaves only an unreferenced object.
        let path = self.db_path.join("objects").join(&content_hash);
        if fs::try_exists(&path).await? {
            let stored = fs::read(&path).await?;
            if stored.as_slice() != data.as_ref() {
                return Err(BraidError::Fs("Corrupt content-addressed object".into()));
            }
        } else {
            braid_http::durable::atomic_write(&path, &data).await?;
        }
        transaction
            .conn
            .execute(
                "INSERT OR REPLACE INTO meta (key,value) VALUES (?,?)",
                params![key, serde_json::to_string(&new_meta)?],
            )
            .map_err(|e| BraidError::Fs(e.to_string()))?;
        transaction
            .conn
            .execute(
                "INSERT INTO events (key,version,fingerprint) VALUES (?,?,?)",
                params![key, id, fingerprint],
            )
            .map_err(|e| BraidError::Fs(e.to_string()))?;
        transaction
            .conn
            .execute_batch("COMMIT")
            .map_err(|e| BraidError::Fs(e.to_string()))?;
        transaction.committed = true;
        Ok(version)
    }

    pub async fn delete(&self, key: &str) -> Result<()> {
        {
            let conn = self.meta_conn.lock().await;
            conn.execute("DELETE FROM meta WHERE key = ?", params![key])
                .map_err(|e| BraidError::Fs(e.to_string()))?;
        }

        Ok(())
    }

    fn get_file_path(&self, key: &str) -> PathBuf {
        self.db_path.join(encode_filename(key))
    }
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
        let conn = self.meta_conn.lock().await;
        let mut stmt = conn
            .prepare("SELECT key FROM meta")
            .map_err(|e| BraidError::Fs(e.to_string()))?;
        let rows = stmt
            .query_map([], |row| row.get(0))
            .map_err(|e| BraidError::Fs(e.to_string()))?;

        let mut keys = Vec::new();
        for key in rows {
            keys.push(key.map_err(|e| BraidError::Fs(e.to_string()))?);
        }
        Ok(keys)
    }
}

fn same_frontier(a: &[Version], b: &[Version]) -> bool {
    a.len() == b.len() && a.iter().all(|v| b.contains(v))
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
                    .map_or(false, |c| c.is_ascii_digit() && c != '0'))
            || (name_part.len() == 4
                && name_part.starts_with("lpt")
                && name_part
                    .chars()
                    .nth(3)
                    .map_or(false, |c| c.is_ascii_digit() && c != '0'))
    };

    if is_reserved {
        if s.len() >= 3 {
            let char_at_2 = s.chars().nth(2).unwrap();
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
        let chunk_str = std::str::from_utf8(chunk).unwrap();
        let val = u8::from_str_radix(chunk_str, 2).unwrap();
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
