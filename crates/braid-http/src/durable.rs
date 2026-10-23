//! Atomic file replacement and process-local document serialization.
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock, Weak},
};
use tokio::io::AsyncWriteExt;

/// Kernel-managed exclusive ownership. Busy writers fail explicitly; no async
/// worker blocks waiting for another process, and process death releases it.
pub async fn writer_lease(path: &Path) -> std::io::Result<std::fs::File> {
    let path = std::path::absolute(path)?;
    tokio::task::spawn_blocking(move || try_writer_lease(&path)).await?
}

/// Blocking, non-waiting counterpart for filesystem adapters on blocking workers.
pub fn try_writer_lease(path: &Path) -> std::io::Result<std::fs::File> {
    let parent = path
        .parent()
        .ok_or_else(|| std::io::Error::other("Writer lease needs a directory"))?;
    std::fs::create_dir_all(parent)?;
    let mut options = std::fs::OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    if std::fs::symlink_metadata(path).is_ok_and(|meta| meta.file_type().is_symlink()) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "Writer lease cannot be a symlink",
        ));
    }
    let file = options.open(path)?;
    file.try_lock().map_err(|error| match error {
        std::fs::TryLockError::WouldBlock => std::io::Error::new(
            std::io::ErrorKind::WouldBlock,
            "Another writer owns this store",
        ),
        std::fs::TryLockError::Error(error) => error,
    })?;
    Ok(file)
}

/// Stateless replicas in one process share ownership and document locks. A second
/// process cannot create a stale, independent set of subscription broadcasts.
pub async fn shared_writer_lease(path: &Path) -> std::io::Result<Arc<std::fs::File>> {
    static LEASES: OnceLock<tokio::sync::Mutex<HashMap<PathBuf, Weak<std::fs::File>>>> =
        OnceLock::new();
    let path = std::path::absolute(path)?;
    let mut leases = LEASES.get_or_init(Default::default).lock().await;
    leases.retain(|_, lease| lease.strong_count() > 0);
    if let Some(lease) = leases.get(&path).and_then(Weak::upgrade) {
        return Ok(lease);
    }
    let lease = Arc::new(writer_lease(&path).await?);
    leases.insert(path, Arc::downgrade(&lease));
    Ok(lease)
}

pub async fn atomic_write(path: &Path, data: &[u8]) -> std::io::Result<()> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    tokio::fs::create_dir_all(parent).await?;
    let tmp = parent.join(format!(".braid-{}.tmp", uuid::Uuid::new_v4()));
    let result = async {
        let mut options = tokio::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o600);
        let mut file = options.open(&tmp).await?;
        file.write_all(data).await?;
        file.sync_all().await?;
        drop(file);
        tokio::fs::rename(&tmp, path).await?;
        #[cfg(unix)]
        {
            let parent = parent.to_owned();
            tokio::task::spawn_blocking(move || std::fs::File::open(parent)?.sync_all()).await??;
        }
        Ok(())
    }
    .await;
    if result.is_err() {
        let _ = tokio::fs::remove_file(&tmp).await;
    }
    result
}

/// All readers/mutators of a document use the same absolute storage key.
pub async fn document_lock(key: PathBuf) -> tokio::sync::OwnedMutexGuard<()> {
    static LOCKS: OnceLock<Mutex<HashMap<PathBuf, Weak<tokio::sync::Mutex<()>>>>> = OnceLock::new();
    let lock = {
        let mut locks = LOCKS
            .get_or_init(Default::default)
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        locks.retain(|_, lock| lock.strong_count() > 0);
        if let Some(lock) = locks.get(&key).and_then(Weak::upgrade) {
            lock
        } else {
            let lock = Arc::new(tokio::sync::Mutex::new(()));
            locks.insert(key, Arc::downgrade(&lock));
            lock
        }
    };
    lock.lock_owned().await
}
