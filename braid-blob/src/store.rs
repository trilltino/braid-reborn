use async_trait::async_trait;
use braid_http::error::{BraidError, Result};
use braid_http::traits::BraidStorage;
use braid_http::types::Version;
use bytes::Bytes;
#[cfg(feature = "native")]
use rusqlite::{params, Connection};
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
impl BlobStore {
    pub async fn new(db_path: PathBuf, meta_db_path: PathBuf) -> Result<Self> {
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

        let file_path = self.get_file_path(key);
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
            Ok(None)
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
        let current_meta = self.get_meta(key).await?;
        let new_ver_str = version.first().map(|v| v.to_string()).unwrap_or_default();
        if let Some(meta) = &current_meta {
            let current_ver_str = meta
                .version
                .first()
                .map(|v| v.to_string())
                .unwrap_or_default();
            if compare_versions(&new_ver_str, &current_ver_str) <= 0 {
                return Ok(meta.version.clone());
            }
        }

        // Compute content hash for deduplication
        let mut hasher = Sha256::new();
        hasher.update(&data);
        let hash_bytes = hasher.finalize();
        let content_hash = format!("{:x}", hash_bytes);

        let new_meta = BlobMetadata {
            key: key.to_string(),
            version: version.clone(),
            content_type,
            parents,
            content_hash: Some(content_hash.clone()),
            size: Some(data.len() as u64),
        };

        // Write file atomically
        let file_path = self.get_file_path(key);
        let temp_folder = self.db_path.join("tmp");
        atomic_write(&file_path, &data, &temp_folder).await?;

        // Update metadata
        {
            let conn = self.meta_conn.lock().await;
            let val_str =
                serde_json::to_string(&new_meta).map_err(|e| BraidError::Config(e.to_string()))?;
            conn.execute(
                "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
                params![key, val_str],
            )
            .map_err(|e| BraidError::Fs(e.to_string()))?;
        }

        Ok(version)
    }

    pub async fn delete(&self, key: &str) -> Result<()> {
        {
            let conn = self.meta_conn.lock().await;
            conn.execute("DELETE FROM meta WHERE key = ?", params![key])
                .map_err(|e| BraidError::Fs(e.to_string()))?;
        }

        let file_path = self.get_file_path(key);
        if fs::try_exists(&file_path)
            .await
            .map_err(|e| BraidError::Io(e))?
        {
            fs::remove_file(&file_path)
                .await
                .map_err(|e| BraidError::Io(e))?;
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

fn compare_versions(a: &str, b: &str) -> i32 {
    let seq_a = get_event_seq(a);
    let seq_b = get_event_seq(b);

    let c = compare_seqs(seq_a, seq_b);
    if c != 0 {
        return c;
    }

    if a < b {
        -1
    } else if a > b {
        1
    } else {
        0
    }
}

fn get_event_seq(e: &str) -> &str {
    if e.is_empty() {
        return "";
    }
    if let Some(idx) = e.rfind('-') {
        &e[idx + 1..]
    } else {
        e
    }
}

fn compare_seqs(a: &str, b: &str) -> i32 {
    if a.len() != b.len() {
        return (a.len() as i32) - (b.len() as i32);
    }
    if a < b {
        -1
    } else if a > b {
        1
    } else {
        0
    }
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
    use tokio::fs;

    fs::create_dir_all(temp_folder)
        .await
        .map_err(|e| BraidError::Io(e))?;

    let temp_name = format!("tmp_{}", uuid::Uuid::new_v4());
    let temp_path = temp_folder.join(temp_name);

    fs::write(&temp_path, data)
        .await
        .map_err(|e| BraidError::Io(e))?;

    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent)
            .await
            .map_err(|e| BraidError::Io(e))?;
    }

    fs::rename(&temp_path, dest)
        .await
        .map_err(|e| BraidError::Io(e))?;

    let metadata = std::fs::metadata(dest).map_err(|e| BraidError::Io(e))?;
    Ok(metadata)
}
