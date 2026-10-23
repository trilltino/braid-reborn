use crate::fs::mapping::{self, extract_markdown};
use crate::fs::state::DaemonState;
use async_trait::async_trait;
use braid_http::traits::BraidStorage;
use nfsserve::nfs::{fattr3, ftype3, nfsstat3, nfsstring, nfstime3, sattr3, specdata3};
use nfsserve::vfs::{DirEntry, NFSFileSystem, ReadDirResult, VFSCapabilities};
use parking_lot::RwLock as PRwLock;
use rusqlite::params;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tracing::info;
use url::Url;

/// Braid NFS backend implementation.
pub struct BraidNfsBackend {
    state: DaemonState,
    blob_store: Arc<braid_blob::BlobStore>,
    id_to_path: Arc<PRwLock<HashMap<u64, String>>>,
    path_to_id: Arc<PRwLock<HashMap<String, u64>>>,
    next_id: Arc<PRwLock<u64>>,
}

impl BraidNfsBackend {
    pub fn new(state: DaemonState, blob_store: Arc<braid_blob::BlobStore>) -> Self {
        let mut id_to_path = HashMap::new();
        let mut path_to_id = HashMap::new();
        let mut max_id = 1;

        // Warm cache from database
        {
            let conn = state.inode_db.lock();
            let mut stmt = conn.prepare("SELECT id, path FROM inodes").unwrap();
            let rows = stmt
                .query_map([], |row| {
                    Ok((row.get::<_, u64>(0)?, row.get::<_, String>(1)?))
                })
                .unwrap();

            for row in rows {
                if let Ok((id, path)) = row {
                    id_to_path.insert(id, path.clone());
                    path_to_id.insert(path, id);
                    if id > max_id {
                        max_id = id;
                    }
                }
            }
        }

        // Ensure Root is ID 1
        if !id_to_path.contains_key(&1) {
            id_to_path.insert(1, "/".to_string());
            path_to_id.insert("/".to_string(), 1);
            let conn = state.inode_db.lock();
            let _ = conn.execute(
                "INSERT OR IGNORE INTO inodes (id, path) VALUES (1, '/')",
                [],
            );
        }

        Self {
            state,
            blob_store,
            id_to_path: Arc::new(PRwLock::new(id_to_path)),
            path_to_id: Arc::new(PRwLock::new(path_to_id)),
            next_id: Arc::new(PRwLock::new(max_id + 1)),
        }
    }

    fn get_path(&self, id: u64) -> Option<String> {
        self.id_to_path.read().get(&id).cloned()
    }

    fn get_or_create_id(&self, path: &str) -> u64 {
        let path = if path.is_empty() { "/" } else { path };
        if let Some(id) = self.path_to_id.read().get(path) {
            return *id;
        }

        let mut next_id_lock = self.next_id.write();
        let id = *next_id_lock;
        *next_id_lock += 1;

        // Persist to DB
        {
            let conn = self.state.inode_db.lock();
            if let Err(e) = conn.execute(
                "INSERT INTO inodes (id, path) VALUES (?, ?)",
                params![id, path],
            ) {
                tracing::error!("Failed to persist inode mapping: {}", e);
            }
        }

        self.path_to_id.write().insert(path.to_string(), id);
        self.id_to_path.write().insert(id, path.to_string());
        id
    }

    fn get_attr(&self, id: u64, ftype: ftype3, size: u64) -> fattr3 {
        fattr3 {
            ftype,
            mode: if matches!(ftype, ftype3::NF3DIR) {
                0o755
            } else {
                0o644
            },
            nlink: 1,
            uid: 0,
            gid: 0,
            size,
            used: size,
            rdev: specdata3 {
                specdata1: 0,
                specdata2: 0,
            },
            fsid: 0,
            fileid: id,
            atime: nfstime3 {
                seconds: 0,
                nseconds: 0,
            },
            mtime: nfstime3 {
                seconds: 0,
                nseconds: 0,
            },
            ctime: nfstime3 {
                seconds: 0,
                nseconds: 0,
            },
        }
    }

    fn url_to_vpath(&self, url_str: &str) -> std::result::Result<String, anyhow::Error> {
        let url = Url::parse(url_str)?;
        let host = url
            .host_str()
            .ok_or_else(|| anyhow::anyhow!("URL missing host"))?;
        let port = url.port();
        let mut vpath = host.to_string();
        if let Some(p) = port {
            vpath.push_str(&format!("+{}", p));
        }
        for segment in url.path_segments().unwrap_or_else(|| "".split('/')) {
            if !segment.is_empty() {
                vpath.push('/');
                vpath.push_str(segment);
            }
        }
        if url.path().ends_with('/') {
            vpath.push_str("/index");
        }
        Ok(format!("/{}", vpath))
    }
}

#[async_trait]
impl NFSFileSystem for BraidNfsBackend {
    fn capabilities(&self) -> VFSCapabilities {
        VFSCapabilities::ReadWrite
    }

    fn root_dir(&self) -> u64 {
        1
    }

    async fn lookup(&self, parent_id: u64, name: &nfsstring) -> std::result::Result<u64, nfsstat3> {
        let parent_path = self.get_path(parent_id).ok_or(nfsstat3::NFS3ERR_STALE)?;
        let name_str = String::from_utf8_lossy(&name.0);

        // Handle virtual /blobs/ path
        let full_path = if parent_path == "/" && name_str == "blobs" {
            "/blobs".to_string()
        } else {
            mapping::path_join(&parent_path, &name_str)
        };

        Ok(self.get_or_create_id(&full_path))
    }

    async fn getattr(&self, id: u64) -> std::result::Result<fattr3, nfsstat3> {
        let vpath = self.get_path(id).ok_or(nfsstat3::NFS3ERR_STALE)?;

        // Special Case: Virtual /blobs directory
        if vpath == "/blobs" {
            return Ok(self.get_attr(id, ftype3::NF3DIR, 4096));
        }

        // Special Case: Files inside /blobs/
        if vpath.starts_with("/blobs/") {
            let key = &vpath["/blobs/".len()..];
            if let Ok(Some(meta)) = self.blob_store.get_meta(key).await {
                return Ok(self.get_attr(id, ftype3::NF3REG, meta.size.unwrap_or(0)));
            }
            // Fallback to checking disk or return error
        }

        let root = crate::fs::config::get_root_dir().map_err(|_| nfsstat3::NFS3ERR_IO)?;
        let path = root.join(vpath.trim_start_matches('/'));

        let metadata = tokio::fs::metadata(&path).await.ok();
        let (ftype, size) = if let Some(meta) = metadata {
            if meta.is_dir() {
                (ftype3::NF3DIR, 4096)
            } else {
                // If it's a file, check if it's an HTML shell we should filter
                if let Ok(content) = tokio::fs::read_to_string(&path).await {
                    let filtered = extract_markdown(&content);
                    (ftype3::NF3REG, filtered.len() as u64)
                } else {
                    (ftype3::NF3REG, meta.len())
                }
            }
        } else {
            let version_store = self.state.version_store.read().await;
            let is_vdir = version_store.file_versions.keys().any(|url| {
                if let Ok(vp) = self.url_to_vpath(url) {
                    vp.starts_with(&vpath) && vp != vpath
                } else {
                    false
                }
            });

            if is_vdir || vpath == "/" {
                (ftype3::NF3DIR, 4096)
            } else {
                return Err(nfsstat3::NFS3ERR_NOENT);
            }
        };

        Ok(self.get_attr(id, ftype, size))
    }

    async fn setattr(&self, id: u64, _attr: sattr3) -> std::result::Result<fattr3, nfsstat3> {
        self.getattr(id).await
    }

    async fn read(
        &self,
        id: u64,
        offset: u64,
        count: u32,
    ) -> std::result::Result<(Vec<u8>, bool), nfsstat3> {
        let vpath = self.get_path(id).ok_or(nfsstat3::NFS3ERR_STALE)?;

        // Special Case: Read from BlobStore
        if vpath.starts_with("/blobs/") {
            let key = &vpath["/blobs/".len()..];
            if let Ok(Some((data, _meta))) = self.blob_store.get(key).await {
                let start = offset as usize;
                if start >= data.len() {
                    return Ok((vec![], true));
                }
                let end = std::cmp::min(start + count as usize, data.len());
                let slice = &data[start..end];
                let eof = end == data.len();
                return Ok((slice.to_vec(), eof));
            }
            return Err(nfsstat3::NFS3ERR_NOENT);
        }

        let root = crate::fs::config::get_root_dir().map_err(|_| nfsstat3::NFS3ERR_IO)?;
        let path = root.join(vpath.trim_start_matches('/'));

        if !path.exists() {
            return Err(nfsstat3::NFS3ERR_NOENT);
        }

        // Optimization: Try to find if this path is in content_cache
        let url = mapping::path_to_url(&path).ok();
        if let Some(url_str) = url {
            let cache = self.state.content_cache.read().await;
            if let Some(content) = cache.get(&url_str) {
                let filtered = extract_markdown(content).into_bytes();
                let start = offset as usize;
                if start >= filtered.len() {
                    return Ok((vec![], true));
                }
                let end = std::cmp::min(start + count as usize, filtered.len());
                let slice = &filtered[start..end];
                let eof = end == filtered.len();
                return Ok((slice.to_vec(), eof));
            }
        }

        // Fallback: Streaming read from disk for non-cached or larger files
        use tokio::io::AsyncReadExt;
        let mut file = tokio::fs::File::open(&path)
            .await
            .map_err(|_| nfsstat3::NFS3ERR_IO)?;

        // Check if it's a Braid Shell (only if we haven't checked cache)
        // For large files, we skip the shell-filtering logic as it's meant for Wiki pages.
        let metadata = file.metadata().await.map_err(|_| nfsstat3::NFS3ERR_IO)?;
        if metadata.len() < 1024 * 1024 {
            // Smaller than 1MB, try to treat as Wiki page
            if let Ok(content) = tokio::fs::read_to_string(&path).await {
                let filtered = extract_markdown(&content).into_bytes();
                let start = offset as usize;
                if start >= filtered.len() {
                    return Ok((vec![], true));
                }
                let end = std::cmp::min(start + count as usize, filtered.len());
                let slice = &filtered[start..end];
                let eof = end == filtered.len();
                return Ok((slice.to_vec(), eof));
            }
            // If read_to_string failed (e.g. binary), fall through to raw read
        }

        // Truly large file - read directly (streaming)
        file.seek(std::io::SeekFrom::Start(offset))
            .await
            .map_err(|_| nfsstat3::NFS3ERR_IO)?;
        let mut buffer = vec![0u8; count as usize];
        let n = file
            .read(&mut buffer)
            .await
            .map_err(|_| nfsstat3::NFS3ERR_IO)?;
        buffer.truncate(n);
        let eof = offset + n as u64 >= metadata.len();
        Ok((buffer, eof))
    }

    async fn write(
        &self,
        id: u64,
        offset: u64,
        data: &[u8],
    ) -> std::result::Result<fattr3, nfsstat3> {
        let vpath = self.get_path(id).ok_or(nfsstat3::NFS3ERR_STALE)?;
        let root = crate::fs::config::get_root_dir().map_err(|_| nfsstat3::NFS3ERR_IO)?;
        let path = root.join(vpath.trim_start_matches('/'));

        // Logic for re-wrapping Braid shells:
        // If it's a write to the start of the file (offset 0), we can check if it was a shell.
        if offset == 0 && data.len() < 1024 * 1024 {
            let new_content_str = String::from_utf8_lossy(data).to_string();
            let url = mapping::path_to_url(&path).ok();

            let mut wrapped_content = None;

            // Try cache first for the "schema" or "shell"
            if let Some(url_str) = &url {
                let cache = self.state.content_cache.read().await;
                if let Some(old_content) = cache.get(url_str) {
                    let wrapped = mapping::wrap_markdown(old_content, &new_content_str);
                    if wrapped != new_content_str {
                        wrapped_content = Some(wrapped);
                    }
                }
            }

            // If not in cache, check disk (only for smaller files)
            if wrapped_content.is_none() {
                if let Ok(old_content) = tokio::fs::read_to_string(&path).await {
                    let wrapped = mapping::wrap_markdown(&old_content, &new_content_str);
                    if wrapped != new_content_str {
                        wrapped_content = Some(wrapped);
                    }
                }
            }

            if let Some(wrapped) = wrapped_content {
                info!("NFS Write: Re-wrapping markdown for {}", vpath);
                let temp_folder = path
                    .parent()
                    .unwrap_or(std::path::Path::new("."))
                    .join(".braid_tmp");
                crate::blob::atomic_write(&path, wrapped.as_bytes(), &temp_folder)
                    .await
                    .map_err(|_| nfsstat3::NFS3ERR_IO)?;

                let metadata = tokio::fs::metadata(&path)
                    .await
                    .map_err(|_| nfsstat3::NFS3ERR_IO)?;
                return Ok(self.get_attr(id, ftype3::NF3REG, metadata.len()));
            }
        }

        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|_| nfsstat3::NFS3ERR_IO)?;
        }

        // For standard NFS writes that might be random access (via seek),
        // we can't easily use atomic_write (which replaces the whole file).
        // However, if the client is doing a full overwrite or append, we could optimize.
        // For now, let's keep the standard write for partial updates but add an info log.
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(&path)
            .await
            .map_err(|_| nfsstat3::NFS3ERR_IO)?;

        file.seek(std::io::SeekFrom::Start(offset))
            .await
            .map_err(|_| nfsstat3::NFS3ERR_IO)?;
        file.write_all(data)
            .await
            .map_err(|_| nfsstat3::NFS3ERR_IO)?;

        let metadata = tokio::fs::metadata(&path)
            .await
            .map_err(|_| nfsstat3::NFS3ERR_IO)?;
        Ok(self.get_attr(id, ftype3::NF3REG, metadata.len()))
    }

    async fn create(
        &self,
        dir_id: u64,
        name: &nfsstring,
        _attr: sattr3,
    ) -> std::result::Result<(u64, fattr3), nfsstat3> {
        let dir_path = self.get_path(dir_id).ok_or(nfsstat3::NFS3ERR_STALE)?;
        let name_str = String::from_utf8_lossy(&name.0).to_string();
        let full_path = mapping::path_join(&dir_path, &name_str);
        let root = crate::fs::config::get_root_dir().map_err(|_| nfsstat3::NFS3ERR_IO)?;
        let path = root.join(full_path.trim_start_matches('/'));

        info!("NFS Create: {} (vpath={})", path.display(), full_path);

        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|_| nfsstat3::NFS3ERR_IO)?;
        }

        tokio::fs::File::create(&path)
            .await
            .map_err(|_| nfsstat3::NFS3ERR_IO)?;

        let id = self.get_or_create_id(&full_path);
        let attr = self.get_attr(id, ftype3::NF3REG, 0);
        Ok((id, attr))
    }

    async fn create_exclusive(
        &self,
        _dir_id: u64,
        _name: &nfsstring,
    ) -> std::result::Result<u64, nfsstat3> {
        Err(nfsstat3::NFS3ERR_NOTSUPP)
    }

    async fn mkdir(
        &self,
        dir_id: u64,
        name: &nfsstring,
    ) -> std::result::Result<(u64, fattr3), nfsstat3> {
        let dir_path = self.get_path(dir_id).ok_or(nfsstat3::NFS3ERR_STALE)?;
        let name_str = String::from_utf8_lossy(&name.0).to_string();
        let full_path = mapping::path_join(&dir_path, &name_str);
        let root = crate::fs::config::get_root_dir().map_err(|_| nfsstat3::NFS3ERR_IO)?;
        let path = root.join(full_path.trim_start_matches('/'));

        info!("NFS Mkdir: {} (vpath={})", path.display(), full_path);

        tokio::fs::create_dir_all(&path)
            .await
            .map_err(|_| nfsstat3::NFS3ERR_IO)?;

        let id = self.get_or_create_id(&full_path);
        let attr = self.get_attr(id, ftype3::NF3DIR, 4096);
        Ok((id, attr))
    }

    async fn remove(&self, dir_id: u64, name: &nfsstring) -> std::result::Result<(), nfsstat3> {
        let dir_path = self.get_path(dir_id).ok_or(nfsstat3::NFS3ERR_STALE)?;
        let name_str = String::from_utf8_lossy(&name.0).to_string();
        let full_path = mapping::path_join(&dir_path, &name_str);
        let root = crate::fs::config::get_root_dir().map_err(|_| nfsstat3::NFS3ERR_IO)?;
        let path = root.join(full_path.trim_start_matches('/'));

        info!("NFS Remove: {} (vpath={})", path.display(), full_path);

        tokio::fs::remove_file(&path)
            .await
            .map_err(|_| nfsstat3::NFS3ERR_IO)?;
        Ok(())
    }

    async fn rename(
        &self,
        old_dir: u64,
        old_name: &nfsstring,
        new_dir: u64,
        new_name: &nfsstring,
    ) -> std::result::Result<(), nfsstat3> {
        let old_dir_path = self.get_path(old_dir).ok_or(nfsstat3::NFS3ERR_STALE)?;
        let old_name_str = String::from_utf8_lossy(&old_name.0).to_string();
        let old_full_path = mapping::path_join(&old_dir_path, &old_name_str);

        let new_dir_path = self.get_path(new_dir).ok_or(nfsstat3::NFS3ERR_STALE)?;
        let new_name_str = String::from_utf8_lossy(&new_name.0).to_string();
        let new_full_path = mapping::path_join(&new_dir_path, &new_name_str);

        let root = crate::fs::config::get_root_dir().map_err(|_| nfsstat3::NFS3ERR_IO)?;
        let old_path = root.join(old_full_path.trim_start_matches('/'));
        let new_path = root.join(new_full_path.trim_start_matches('/'));

        info!("NFS Rename: {:?} -> {:?}", old_path, new_path);

        if let Some(parent) = new_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|_| nfsstat3::NFS3ERR_IO)?;
        }

        tokio::fs::rename(old_path, new_path)
            .await
            .map_err(|_| nfsstat3::NFS3ERR_IO)?;

        // Update ID mapping if possible (though IDs are cached, this path for that ID is now invalid)
        // For now, BraidNfsBackend might need an ID-to-path cache refresh or similar.
        // But since we derive IDs heuristicly or cache them, let's just proceed.

        Ok(())
    }

    async fn readdir(
        &self,
        dir_id: u64,
        cookie: u64,
        _count: usize,
    ) -> std::result::Result<ReadDirResult, nfsstat3> {
        let dir_path = self.get_path(dir_id).ok_or(nfsstat3::NFS3ERR_STALE)?;
        let mut entries = Vec::new();

        if dir_path == "/" {
            // Add virtual /blobs entry
            let blob_id = self.get_or_create_id("/blobs");
            entries.push(DirEntry {
                fileid: blob_id,
                name: nfsstring("blobs".as_bytes().to_vec()),
                attr: self.get_attr(blob_id, ftype3::NF3DIR, 4096),
            });
        }

        if dir_path == "/blobs" {
            // List actual blobs from BlobStore
            if let Ok(keys) = self.blob_store.list_keys().await {
                for key in keys {
                    let full_path = format!("/blobs/{}", key);
                    let id = self.get_or_create_id(&full_path);
                    let size = self
                        .blob_store
                        .get_meta(&key)
                        .await
                        .ok()
                        .flatten()
                        .and_then(|m| m.size)
                        .unwrap_or(0);

                    entries.push(DirEntry {
                        fileid: id,
                        name: nfsstring(key.as_bytes().to_vec()),
                        attr: self.get_attr(id, ftype3::NF3REG, size),
                    });
                }
            }
        } else {
            let prefix = if dir_path == "/" {
                "/".to_string()
            } else {
                format!("{}/", dir_path)
            };

            // Query DB for immediate children
            // Efficient way: get all paths starting with prefix, but with no more slashes after the prefix
            let conn = self.state.inode_db.lock();
            let mut stmt = conn
                .prepare("SELECT id, path FROM inodes WHERE path LIKE ? AND path != ?")
                .unwrap();
            let rows = stmt
                .query_map(params![format!("{}%", prefix), dir_path], |row| {
                    Ok((row.get::<_, u64>(0)?, row.get::<_, String>(1)?))
                })
                .unwrap();

            for row in rows {
                if let Ok((id, path)) = row {
                    let relative = if dir_path == "/" {
                        &path[1..]
                    } else {
                        &path[dir_path.len() + 1..]
                    };

                    // Only immediate children (no more slashes)
                    if !relative.is_empty() && !relative.contains('/') {
                        let root =
                            crate::fs::config::get_root_dir().map_err(|_| nfsstat3::NFS3ERR_IO)?;
                        let abs_path = root.join(path.trim_start_matches('/'));

                        let (ftype, size) = if abs_path.is_file() {
                            (
                                ftype3::NF3REG,
                                std::fs::metadata(&abs_path).map(|m| m.len()).unwrap_or(0),
                            )
                        } else {
                            (ftype3::NF3DIR, 4096)
                        };

                        entries.push(DirEntry {
                            fileid: id,
                            name: nfsstring(relative.as_bytes().to_vec()),
                            attr: self.get_attr(id, ftype, size),
                        });
                    }
                }
            }
        }

        let start = cookie as usize;
        let paged_entries = if start < entries.len() {
            entries.into_iter().skip(start).collect()
        } else {
            vec![]
        };
        Ok(ReadDirResult {
            entries: paged_entries,
            end: true,
        })
    }

    async fn symlink(
        &self,
        _dir_id: u64,
        _name: &nfsstring,
        _target: &nfsstring,
        _attr: &sattr3,
    ) -> std::result::Result<(u64, fattr3), nfsstat3> {
        Err(nfsstat3::NFS3ERR_NOTSUPP)
    }

    async fn readlink(&self, _id: u64) -> std::result::Result<nfsstring, nfsstat3> {
        Err(nfsstat3::NFS3ERR_NOTSUPP)
    }
}
