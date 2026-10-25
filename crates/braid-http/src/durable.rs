//! Atomic file replacement and process-local document serialization.
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock, Weak},
};
use tokio::io::AsyncWriteExt;

/// Junctions and other Windows reparse points also redirect filesystem access.
pub fn is_link_or_reparse_point(metadata: &std::fs::Metadata) -> bool {
    let linked = metadata.file_type().is_symlink();
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        linked || metadata.file_attributes() & 0x400 != 0
    }
    #[cfg(not(windows))]
    {
        linked
    }
}

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
    if std::fs::symlink_metadata(path).is_ok_and(|meta| is_link_or_reparse_point(&meta)) {
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
    #[cfg(feature = "fault-injection")]
    fault::check(fault::Point::CreateDirectory, path).await?;
    tokio::fs::create_dir_all(parent).await?;
    let tmp = parent.join(format!(".braid-{}.tmp", uuid::Uuid::new_v4()));
    let result = async {
        #[cfg(feature = "fault-injection")]
        fault::check(fault::Point::CreateTemp, path).await?;
        let mut options = tokio::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o600);
        let mut file = options.open(&tmp).await?;
        #[cfg(feature = "fault-injection")]
        fault::check(fault::Point::WriteData, path).await?;
        file.write_all(data).await?;
        #[cfg(feature = "fault-injection")]
        fault::check(fault::Point::SyncFile, path).await?;
        file.sync_all().await?;
        drop(file);
        #[cfg(feature = "fault-injection")]
        fault::check(fault::Point::Rename, path).await?;
        tokio::fs::rename(&tmp, path).await?;
        #[cfg(feature = "fault-injection")]
        fault::check(fault::Point::SyncDirectory, path).await?;
        #[cfg(unix)]
        {
            let parent = parent.to_owned();
            tokio::task::spawn_blocking(move || std::fs::File::open(parent)?.sync_all()).await??;
        }
        Ok(())
    }
    .await;
    if result.is_err() {
        // A simulated crash leaves exactly what a dead process would: no cleanup.
        #[cfg(feature = "fault-injection")]
        if result.as_ref().err().is_some_and(fault::is_crash) {
            return result;
        }
        let _ = tokio::fs::remove_file(&tmp).await;
    }
    result
}

/// Deterministic persistence fault plans. A plan covers every write whose target is under
/// `root`, so tests with distinct temporary directories stay independent when run in
/// parallel. Enumerate crash points by arming plan 1, 2, 3, ... until none fires.
#[cfg(feature = "fault-injection")]
pub mod fault {
    use std::{
        path::{Path, PathBuf},
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            Arc, Mutex,
        },
    };

    /// The ordered steps inside one atomic file replacement.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum Point {
        CreateDirectory,
        CreateTemp,
        WriteData,
        SyncFile,
        Rename,
        SyncDirectory,
    }
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum Action {
        /// Report this error and let normal cleanup run (disk full, permissions, ...).
        Fail(std::io::ErrorKind),
        /// Stop dead without cleanup, as a killed process would. Not a power-loss model.
        Crash,
        /// Suspend forever, so the caller can cancel (drop) the operation exactly here.
        Pause,
    }

    struct State {
        root: PathBuf,
        nth: usize,
        action: Action,
        hits: AtomicUsize,
        fired: AtomicBool,
        last: Mutex<Option<Point>>,
    }
    static PLANS: Mutex<Vec<Arc<State>>> = Mutex::new(Vec::new());

    /// Disarms the plan when dropped.
    pub struct Guard(Arc<State>);
    impl Guard {
        /// Persistence steps observed under the root so far.
        pub fn hits(&self) -> usize {
            self.0.hits.load(Ordering::SeqCst)
        }
        pub fn fired(&self) -> bool {
            self.0.fired.load(Ordering::SeqCst)
        }
        /// The step at which the plan fired.
        pub fn point(&self) -> Option<Point> {
            *self.0.last.lock().unwrap_or_else(|e| e.into_inner())
        }
    }
    impl Drop for Guard {
        fn drop(&mut self) {
            PLANS
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .retain(|plan| !Arc::ptr_eq(plan, &self.0));
        }
    }

    /// Fire `action` at the `nth` (1-based) persistence step under `root`.
    /// `usize::MAX` never fires, which makes the guard a pure step counter.
    pub fn arm(root: &Path, nth: usize, action: Action) -> Guard {
        let root = std::path::absolute(root).unwrap_or_else(|_| root.to_owned());
        let state = Arc::new(State {
            root,
            nth,
            action,
            hits: AtomicUsize::new(0),
            fired: AtomicBool::new(false),
            last: Mutex::new(None),
        });
        PLANS
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(state.clone());
        Guard(state)
    }

    const CRASH: &str = "injected crash";
    pub(super) fn is_crash(error: &std::io::Error) -> bool {
        error.kind() == std::io::ErrorKind::Other && error.to_string() == CRASH
    }

    pub(super) async fn check(point: Point, target: &Path) -> std::io::Result<()> {
        let target = std::path::absolute(target).unwrap_or_else(|_| target.to_owned());
        let plans: Vec<Arc<State>> = PLANS.lock().unwrap_or_else(|e| e.into_inner()).clone();
        for plan in plans.iter().filter(|plan| target.starts_with(&plan.root)) {
            if plan.hits.fetch_add(1, Ordering::SeqCst) + 1 != plan.nth {
                continue;
            }
            plan.fired.store(true, Ordering::SeqCst);
            *plan.last.lock().unwrap_or_else(|e| e.into_inner()) = Some(point);
            return match plan.action {
                Action::Fail(kind) => Err(std::io::Error::new(kind, "injected fault")),
                Action::Crash => Err(std::io::Error::other(CRASH)),
                Action::Pause => std::future::pending().await,
            };
        }
        Ok(())
    }
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
