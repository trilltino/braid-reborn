//! NFS protocol adapter over a serialized filesystem and inode registry.
mod backing;
use crate::fs::state::DaemonState;
use async_trait::async_trait;
use backing::{io, virtual_attr, Backing};
use braid_http::traits::BraidStorage;
use nfsserve::nfs::{fattr3, ftype3, nfsstat3, nfsstring, sattr3};
use nfsserve::vfs::{DirEntry, NFSFileSystem, ReadDirResult, VFSCapabilities};
use std::{path::PathBuf, sync::Arc};
use tokio::io::{AsyncReadExt, AsyncSeekExt};

fn valid_name(bytes: &[u8]) -> Result<&str, nfsstat3> {
    let name = std::str::from_utf8(bytes).map_err(|_| nfsstat3::NFS3ERR_INVAL)?;
    let stem = name.split('.').next().unwrap_or("").to_ascii_lowercase();
    let reserved = matches!(stem.as_str(), "con" | "prn" | "aux" | "nul")
        || (stem.len() == 4
            && (stem.starts_with("com") || stem.starts_with("lpt"))
            && matches!(stem.as_bytes().last(), Some(b'1'..=b'9')));
    if name == ".braidfs"
        || name.starts_with(".braid-")
        || name.is_empty()
        || matches!(name, "." | "..")
        || name.ends_with(['.', ' '])
        || reserved
        || name
            .chars()
            .any(|c| c.is_control() || "/\\:<>\"|?*".contains(c))
    {
        return Err(nfsstat3::NFS3ERR_INVAL);
    }
    Ok(name)
}

pub struct BraidNfsBackend {
    backing: Arc<Backing>,
    gate: Arc<tokio::sync::Semaphore>,
    blob_store: Arc<braid_blob::BlobStore>,
}
impl BraidNfsBackend {
    pub fn new(
        state: DaemonState,
        blob_store: Arc<braid_blob::BlobStore>,
    ) -> crate::core::Result<Self> {
        Self::with_root(state, blob_store, crate::fs::config::get_root_dir()?)
    }
    /// Explicit root avoids process-global environment changes in embedded services/tests.
    pub fn with_root(
        state: DaemonState,
        blob_store: Arc<braid_blob::BlobStore>,
        root: PathBuf,
    ) -> crate::core::Result<Self> {
        Self::open(root, state.inode_db, blob_store)
    }
    /// Standalone serving needs storage components, not daemon clients/configuration.
    pub fn open(
        root: PathBuf,
        inode_db: Arc<parking_lot::Mutex<rusqlite::Connection>>,
        blob_store: Arc<braid_blob::BlobStore>,
    ) -> crate::core::Result<Self> {
        let backing = Backing::new(root, inode_db).map_err(|e| {
            crate::core::BraidError::Fs(format!("NFS registry recovery failed: {e:?}"))
        })?;
        Ok(Self {
            backing: Arc::new(backing),
            gate: Arc::new(tokio::sync::Semaphore::new(1)),
            blob_store,
        })
    }
    async fn storage<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&Backing) -> Result<T, nfsstat3> + Send + 'static,
    ) -> Result<T, nfsstat3> {
        let permit = self
            .gate
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| nfsstat3::NFS3ERR_IO)?;
        let backing = self.backing.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let _lease = crate::fs::file_service::try_lock(&backing.root).map_err(|error| {
                if error.kind() == std::io::ErrorKind::WouldBlock {
                    nfsstat3::NFS3ERR_JUKEBOX
                } else {
                    io(error)
                }
            })?;
            operation(&backing)
        })
        .await
        .map_err(|_| nfsstat3::NFS3ERR_IO)?
    }
    async fn path(&self, id: u64) -> Result<String, nfsstat3> {
        self.storage(move |backing| backing.vpath(id)).await
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
    async fn lookup(&self, parent: u64, name: &nfsstring) -> Result<u64, nfsstat3> {
        let name = valid_name(&name.0)?.to_owned();
        let parent_path = self.path(parent).await?;
        let path = crate::fs::mapping::path_join(&parent_path, &name);
        if path == "/blobs" {
            return self.storage(move |backing| backing.id(&path)).await;
        }
        if let Some(key) = path.strip_prefix("/blobs/") {
            if self
                .blob_store
                .get_meta(key)
                .await
                .map_err(|_| nfsstat3::NFS3ERR_IO)?
                .is_none()
            {
                return Err(nfsstat3::NFS3ERR_NOENT);
            }
            return self.storage(move |backing| backing.id(&path)).await;
        }
        let bytes = name.into_bytes();
        self.storage(move |backing| {
            let path = backing.child(parent, &bytes)?;
            std::fs::metadata(backing.path(&path)?).map_err(io)?;
            backing.id(&path)
        })
        .await
    }
    async fn getattr(&self, id: u64) -> Result<fattr3, nfsstat3> {
        let path = self.path(id).await?;
        if path == "/blobs" {
            return Ok(virtual_attr(id, ftype3::NF3DIR, 0));
        }
        if let Some(key) = path.strip_prefix("/blobs/") {
            let meta = self
                .blob_store
                .get_meta(key)
                .await
                .map_err(|_| nfsstat3::NFS3ERR_IO)?
                .ok_or(nfsstat3::NFS3ERR_NOENT)?;
            return Ok(virtual_attr(id, ftype3::NF3REG, meta.size.unwrap_or(0)));
        }
        self.storage(move |backing| backing.attr(id)).await
    }
    async fn setattr(&self, id: u64, attr: sattr3) -> Result<fattr3, nfsstat3> {
        self.storage(move |backing| backing.setattr(id, attr)).await
    }
    async fn read(&self, id: u64, offset: u64, count: u32) -> Result<(Vec<u8>, bool), nfsstat3> {
        if count > 1024 * 1024 {
            return Err(nfsstat3::NFS3ERR_INVAL);
        }
        let path = self.path(id).await?;
        if let Some(key) = path.strip_prefix("/blobs/") {
            let (mut file, _) = self
                .blob_store
                .open(key)
                .await
                .map_err(|_| nfsstat3::NFS3ERR_IO)?
                .ok_or(nfsstat3::NFS3ERR_NOENT)?;
            let size = file.metadata().await.map_err(io)?.len();
            file.seek(std::io::SeekFrom::Start(offset))
                .await
                .map_err(io)?;
            let mut bytes = vec![0; count as usize];
            let read = file.read(&mut bytes).await.map_err(io)?;
            bytes.truncate(read);
            return Ok((bytes, offset.saturating_add(read as u64) >= size));
        }
        self.storage(move |backing| backing.read(id, offset, count))
            .await
    }
    async fn write(&self, id: u64, offset: u64, data: &[u8]) -> Result<fattr3, nfsstat3> {
        if data.len() > 1024 * 1024 {
            return Err(nfsstat3::NFS3ERR_INVAL);
        }
        let data = data.to_vec();
        self.storage(move |backing| backing.write(id, offset, &data))
            .await
    }
    async fn create(
        &self,
        parent: u64,
        name: &nfsstring,
        attr: sattr3,
    ) -> Result<(u64, fattr3), nfsstat3> {
        valid_name(&name.0)?;
        let name = name.0.clone();
        self.storage(move |backing| {
            // nfsserve checks GUARDED before this call. UNCHECKED CREATE must
            // reuse an existing regular file and apply the requested attributes.
            let path = backing.child(parent, &name)?;
            let id = match std::fs::metadata(backing.path(&path)?) {
                Ok(meta) if meta.is_file() => backing.id(&path)?,
                Ok(_) => return Err(nfsstat3::NFS3ERR_ISDIR),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    backing.create(parent, &name, false)?
                }
                Err(error) => return Err(io(error)),
            };
            let attr = backing.setattr(id, attr)?;
            Ok((id, attr))
        })
        .await
    }
    async fn create_exclusive(&self, parent: u64, name: &nfsstring) -> Result<u64, nfsstat3> {
        valid_name(&name.0)?;
        let name = name.0.clone();
        self.storage(move |backing| backing.create(parent, &name, false))
            .await
    }
    async fn mkdir(&self, parent: u64, name: &nfsstring) -> Result<(u64, fattr3), nfsstat3> {
        valid_name(&name.0)?;
        let name = name.0.clone();
        self.storage(move |backing| {
            let id = backing.create(parent, &name, true)?;
            Ok((id, backing.attr(id)?))
        })
        .await
    }
    async fn remove(&self, parent: u64, name: &nfsstring) -> Result<(), nfsstat3> {
        valid_name(&name.0)?;
        let name = name.0.clone();
        self.storage(move |backing| {
            let path = backing.child(parent, &name)?;
            backing.change("remove", &path, None)
        })
        .await
    }
    async fn rename(
        &self,
        old_dir: u64,
        old_name: &nfsstring,
        new_dir: u64,
        new_name: &nfsstring,
    ) -> Result<(), nfsstat3> {
        valid_name(&old_name.0)?;
        valid_name(&new_name.0)?;
        let old = old_name.0.clone();
        let new = new_name.0.clone();
        self.storage(move |backing| {
            let source = backing.child(old_dir, &old)?;
            let destination = backing.child(new_dir, &new)?;
            backing.change("rename", &source, Some(&destination))
        })
        .await
    }
    async fn readdir(
        &self,
        parent: u64,
        cookie: u64,
        count: usize,
    ) -> Result<ReadDirResult, nfsstat3> {
        let parent_path = self.path(parent).await?;
        let mut entries = if parent_path == "/blobs" {
            let mut result = Vec::new();
            for key in self
                .blob_store
                .list_keys()
                .await
                .map_err(|_| nfsstat3::NFS3ERR_IO)?
            {
                let path = format!("/blobs/{key}");
                let id = self.storage(move |backing| backing.id(&path)).await?;
                result.push(DirEntry {
                    fileid: id,
                    name: nfsstring(key.into_bytes()),
                    attr: self.getattr(id).await?,
                });
            }
            result
        } else {
            self.storage(move |backing| {
                let path = backing.path(&backing.vpath(parent)?)?;
                let mut entries = Vec::new();
                for entry in std::fs::read_dir(path).map_err(io)? {
                    let entry = entry.map_err(io)?;
                    let name = entry.file_name();
                    let Some(name) = name.to_str() else {
                        continue;
                    };
                    if valid_name(name.as_bytes()).is_err() || (parent == 1 && name == "blobs") {
                        continue;
                    }
                    if entry.file_type().map_err(io)?.is_symlink() {
                        continue;
                    }
                    let path = backing.child(parent, name.as_bytes())?;
                    let id = backing.id(&path)?;
                    entries.push(DirEntry {
                        fileid: id,
                        name: nfsstring(name.as_bytes().to_vec()),
                        attr: backing.attr(id)?,
                    });
                }
                if parent == 1 {
                    let id = backing.id("/blobs")?;
                    entries.push(DirEntry {
                        fileid: id,
                        name: nfsstring(b"blobs".to_vec()),
                        attr: virtual_attr(id, ftype3::NF3DIR, 0),
                    });
                }
                Ok(entries)
            })
            .await?
        };
        // nfsserve writes the fileid into each wire cookie. Cookies are identities,
        // never vector indices, and remain usable when preceding entries disappear.
        entries.sort_by_key(|entry| entry.fileid);
        entries.retain(|entry| entry.fileid > cookie);
        let end = entries.len() <= count;
        entries.truncate(count);
        Ok(ReadDirResult { entries, end })
    }
    async fn symlink(
        &self,
        _: u64,
        _: &nfsstring,
        _: &nfsstring,
        _: &sattr3,
    ) -> Result<(u64, fattr3), nfsstat3> {
        Err(nfsstat3::NFS3ERR_NOTSUPP)
    }
    async fn readlink(&self, _: u64) -> Result<nfsstring, nfsstat3> {
        Err(nfsstat3::NFS3ERR_NOTSUPP)
    }
}

#[cfg(test)]
mod engineering_tests {
    use super::valid_name;
    #[test]
    fn rejects_ambiguous_and_platform_specific_path_names() {
        for name in [
            b"..".as_slice(),
            b"../outside",
            b"a/b",
            b"a\\b",
            b"CON",
            b"LPT1.txt",
            b"x.",
            b"x ",
            b"C:drive",
            b"\xff",
            b"a\0b",
        ] {
            assert!(valid_name(name).is_err(), "{name:?}");
        }
        assert_eq!(
            valid_name("hello-\u{1f600}.txt".as_bytes()).unwrap(),
            "hello-\u{1f600}.txt"
        );
    }

    #[tokio::test]
    async fn concurrent_writes_and_inode_cookies_follow_the_nfsserve_contract() {
        use super::*;
        let root = tempfile::tempdir().unwrap();
        let private = root.path().join(".braidfs");
        let blobs = Arc::new(
            braid_blob::BlobStore::new(private.join("blobs"), private.join("blobs.sqlite"))
                .await
                .unwrap(),
        );
        let db = Arc::new(parking_lot::Mutex::new(
            rusqlite::Connection::open(private.join("inodes.sqlite")).unwrap(),
        ));
        let backend = Arc::new(BraidNfsBackend {
            backing: Arc::new(Backing::new(root.path().to_owned(), db).unwrap()),
            gate: Arc::new(tokio::sync::Semaphore::new(1)),
            blob_store: blobs,
        });
        assert!(matches!(
            backend.lookup(1, &nfsstring(b"missing".to_vec())).await,
            Err(nfsstat3::NFS3ERR_NOENT)
        ));
        let (id, _) = backend
            .create(1, &nfsstring(b"file".to_vec()), sattr3::default())
            .await
            .unwrap();
        let mut writes = tokio::task::JoinSet::new();
        for index in 0..20u8 {
            let backend = backend.clone();
            writes.spawn(async move {
                backend.write(id, u64::from(index), &[index]).await.unwrap();
            });
        }
        while let Some(result) = writes.join_next().await {
            result.unwrap();
        }
        assert_eq!(
            backend.read(id, 0, 100).await.unwrap().0,
            (0..20u8).collect::<Vec<_>>()
        );
        for name in ["a", "b", "c", "d"] {
            backend
                .create(1, &nfsstring(name.as_bytes().to_vec()), sattr3::default())
                .await
                .unwrap();
        }
        let mut seen = std::collections::HashSet::new();
        let mut cookie = 0;
        loop {
            let page = backend.readdir(1, cookie, 2).await.unwrap();
            for entry in page.entries {
                assert!(seen.insert(entry.fileid));
                cookie = entry.fileid;
                assert_ne!(&entry.name.0, b".braidfs");
            }
            if page.end {
                break;
            }
        }
        assert_eq!(seen.len(), 6); // Five files and the virtual blob directory.
        backend
            .rename(
                1,
                &nfsstring(b"file".to_vec()),
                1,
                &nfsstring(b"renamed".to_vec()),
            )
            .await
            .unwrap();
        assert_eq!(
            backend
                .lookup(1, &nfsstring(b"renamed".to_vec()))
                .await
                .unwrap(),
            id
        );
        backend
            .remove(1, &nfsstring(b"renamed".to_vec()))
            .await
            .unwrap();
        assert!(matches!(
            backend.getattr(id).await,
            Err(nfsstat3::NFS3ERR_STALE)
        ));
    }
}
