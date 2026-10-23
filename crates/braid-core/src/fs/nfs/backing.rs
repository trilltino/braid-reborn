//! Blocking filesystem/SQLite boundary. Each operation runs under the adapter's
//! single permit; cancellation cannot interrupt a started storage transaction.
use super::{nfsstat3, valid_name};
use crate::fs::mapping::{extract_markdown, wrap_markdown};
use nfsserve::nfs::{
    fattr3, ftype3, nfstime3, sattr3, set_atime, set_gid3, set_mode3, set_mtime, set_size3,
    set_uid3, specdata3,
};
use parking_lot::Mutex;
use rusqlite::{params, Connection, OptionalExtension};
use std::{
    io::{Read, Seek, SeekFrom, Write},
    path::{Component, Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

const MAX_FILE: u64 = 64 * 1024 * 1024;
const MAX_SHELL: u64 = 1024 * 1024;

pub(super) fn io(error: std::io::Error) -> nfsstat3 {
    use std::io::ErrorKind::*;
    match error.kind() {
        NotFound => nfsstat3::NFS3ERR_NOENT,
        AlreadyExists => nfsstat3::NFS3ERR_EXIST,
        PermissionDenied => nfsstat3::NFS3ERR_ACCES,
        NotADirectory => nfsstat3::NFS3ERR_NOTDIR,
        IsADirectory => nfsstat3::NFS3ERR_ISDIR,
        DirectoryNotEmpty => nfsstat3::NFS3ERR_NOTEMPTY,
        InvalidInput | InvalidData => nfsstat3::NFS3ERR_INVAL,
        _ => nfsstat3::NFS3ERR_IO,
    }
}

pub(super) struct Backing {
    pub root: PathBuf,
    pub db: Arc<Mutex<Connection>>,
    _lease: std::fs::File,
}
impl Backing {
    pub fn new(root: PathBuf, db: Arc<Mutex<Connection>>) -> Result<Self, nfsstat3> {
        std::fs::create_dir_all(&root).map_err(io)?;
        let root = root.canonicalize().map_err(io)?;
        let _files = crate::fs::file_service::try_lock(&root).map_err(io)?;
        let private = root.join(".braidfs");
        std::fs::create_dir_all(&private).map_err(io)?;
        if std::fs::symlink_metadata(&private)
            .map_err(io)?
            .file_type()
            .is_symlink()
        {
            return Err(nfsstat3::NFS3ERR_ACCES);
        }
        let lease = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(private.join("nfs.lock"))
            .map_err(io)?;
        lease.try_lock().map_err(|_| nfsstat3::NFS3ERR_ACCES)?;
        let this = Self {
            root,
            db,
            _lease: lease,
        };
        {
            let mut conn = this.db.lock();
            conn.pragma_update(None, "journal_mode", "WAL")
                .map_err(|_| nfsstat3::NFS3ERR_IO)?;
            conn.pragma_update(None, "synchronous", "FULL")
                .map_err(|_| nfsstat3::NFS3ERR_IO)?;
            conn.busy_timeout(std::time::Duration::from_secs(5))
                .map_err(|_| nfsstat3::NFS3ERR_IO)?;
            crate::fs::operations::initialize(&conn).map_err(|_| nfsstat3::NFS3ERR_IO)?;
            conn.execute("UPDATE nfs_changes SET state='failed',last_error='Interrupted before namespace mutation' WHERE state='staging'",[]).map_err(|_| nfsstat3::NFS3ERR_IO)?;
            let tx = conn.transaction().map_err(|_| nfsstat3::NFS3ERR_IO)?;
            tx.execute_batch("CREATE TABLE IF NOT EXISTS inodes(id INTEGER PRIMARY KEY, path TEXT UNIQUE NOT NULL);
                CREATE TABLE IF NOT EXISTS nfs_sequence(singleton INTEGER PRIMARY KEY CHECK(singleton=1), value INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS nfs_changes(id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL, source TEXT NOT NULL, destination TEXT, state TEXT NOT NULL);
                INSERT OR IGNORE INTO inodes(id,path) VALUES(1,'/');
                INSERT OR IGNORE INTO nfs_sequence(singleton,value) SELECT 1,MAX(id) FROM inodes;") .map_err(|_| nfsstat3::NFS3ERR_IO)?;
            let root: String = tx
                .query_row("SELECT path FROM inodes WHERE id=1", [], |row| row.get(0))
                .map_err(|_| nfsstat3::NFS3ERR_IO)?;
            if root != "/" {
                return Err(nfsstat3::NFS3ERR_IO);
            }
            tx.commit().map_err(|_| nfsstat3::NFS3ERR_IO)?;
        }
        let pending: Vec<(i64, String, String, Option<String>)> = {
            let conn = this.db.lock();
            let mut stmt = conn.prepare("SELECT id,kind,source,destination FROM nfs_changes WHERE state='prepared' ORDER BY id").map_err(|_| nfsstat3::NFS3ERR_IO)?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
                })
                .map_err(|_| nfsstat3::NFS3ERR_IO)?
                .collect::<rusqlite::Result<_>>()
                .map_err(|_| nfsstat3::NFS3ERR_IO)?;
            rows
        };
        for (id, kind, source, destination) in pending {
            this.finish_change(id, &kind, &source, destination.as_deref())?;
        }
        Ok(this)
    }
    pub fn path(&self, virtual_path: &str) -> Result<PathBuf, nfsstat3> {
        let relative = Path::new(
            virtual_path
                .strip_prefix('/')
                .ok_or(nfsstat3::NFS3ERR_INVAL)?,
        );
        for part in relative.components() {
            let Component::Normal(name) = part else {
                return Err(nfsstat3::NFS3ERR_INVAL);
            };
            valid_name(name.to_str().ok_or(nfsstat3::NFS3ERR_INVAL)?.as_bytes())?;
        }
        let path = self.root.join(relative);
        for ancestor in path.ancestors() {
            match std::fs::symlink_metadata(ancestor) {
                Ok(meta) if meta.file_type().is_symlink() => return Err(nfsstat3::NFS3ERR_ACCES),
                Err(error) if error.kind() != std::io::ErrorKind::NotFound => {
                    return Err(io(error))
                }
                _ => {}
            }
            if ancestor == self.root {
                break;
            }
        }
        Ok(path)
    }
    pub fn vpath(&self, id: u64) -> Result<String, nfsstat3> {
        self.db
            .lock()
            .query_row("SELECT path FROM inodes WHERE id=?", [id], |row| row.get(0))
            .optional()
            .map_err(|_| nfsstat3::NFS3ERR_IO)?
            .ok_or(nfsstat3::NFS3ERR_STALE)
    }
    pub fn id(&self, path: &str) -> Result<u64, nfsstat3> {
        self.path(path)?;
        let mut conn = self.db.lock();
        let tx = conn.transaction().map_err(|_| nfsstat3::NFS3ERR_IO)?;
        if let Some(id) = tx
            .query_row("SELECT id FROM inodes WHERE path=?", [path], |row| {
                row.get(0)
            })
            .optional()
            .map_err(|_| nfsstat3::NFS3ERR_IO)?
        {
            return Ok(id);
        }
        tx.execute("UPDATE nfs_sequence SET value=MAX(value,(SELECT MAX(id) FROM inodes))+1 WHERE singleton=1", []).map_err(|_| nfsstat3::NFS3ERR_IO)?;
        let id: u64 = tx
            .query_row(
                "SELECT value FROM nfs_sequence WHERE singleton=1",
                [],
                |row| row.get(0),
            )
            .map_err(|_| nfsstat3::NFS3ERR_IO)?;
        tx.execute("INSERT INTO inodes(id,path) VALUES(?,?)", params![id, path])
            .map_err(|_| nfsstat3::NFS3ERR_IO)?;
        tx.commit().map_err(|_| nfsstat3::NFS3ERR_IO)?;
        Ok(id)
    }
    pub fn child(&self, parent: u64, name: &[u8]) -> Result<String, nfsstat3> {
        let parent = self.vpath(parent)?;
        if parent == "/blobs" || parent.starts_with("/blobs/") {
            return Err(nfsstat3::NFS3ERR_ROFS);
        }
        if !self.path(&parent)?.is_dir() {
            return Err(nfsstat3::NFS3ERR_NOTDIR);
        }
        Ok(crate::fs::mapping::path_join(&parent, valid_name(name)?))
    }
    fn shell(&self, path: &Path) -> Result<Option<(String, Vec<u8>)>, nfsstat3> {
        if std::fs::metadata(path).map_err(io)?.len() > MAX_SHELL {
            return Ok(None);
        }
        let bytes = std::fs::read(path).map_err(io)?;
        let Ok(text) = String::from_utf8(bytes) else {
            return Ok(None);
        };
        let view = extract_markdown(&text);
        if view != text {
            Ok(Some((text, view.into_bytes())))
        } else {
            Ok(None)
        }
    }
    pub fn attr(&self, id: u64) -> Result<fattr3, nfsstat3> {
        let path = self.path(&self.vpath(id)?)?;
        let meta = std::fs::metadata(&path).map_err(io)?;
        let size = if meta.is_dir() {
            meta.len()
        } else {
            self.shell(&path)?
                .map_or(meta.len(), |(_, view)| view.len() as u64)
        };
        let mut attr = virtual_attr(
            id,
            if meta.is_dir() {
                ftype3::NF3DIR
            } else {
                ftype3::NF3REG
            },
            size,
        );
        attr.mode = if meta.is_dir() { 0o755 } else { 0o644 };
        attr.mtime = checked_time(meta.modified().map_err(io)?)?;
        attr.atime = checked_time(meta.accessed().map_err(io)?)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            attr.mode = meta.mode() & 0o7777;
            attr.uid = meta.uid();
            attr.gid = meta.gid();
            attr.nlink = u32::try_from(meta.nlink()).map_err(|_| nfsstat3::NFS3ERR_IO)?;
            attr.ctime = nfstime3 {
                seconds: u32::try_from(meta.ctime()).map_err(|_| nfsstat3::NFS3ERR_IO)?,
                nseconds: u32::try_from(meta.ctime_nsec()).map_err(|_| nfsstat3::NFS3ERR_IO)?,
            };
        }
        Ok(attr)
    }
    pub fn read(&self, id: u64, offset: u64, count: u32) -> Result<(Vec<u8>, bool), nfsstat3> {
        let path = self.path(&self.vpath(id)?)?;
        if let Some((_, view)) = self.shell(&path)? {
            let start = usize::try_from(offset)
                .unwrap_or(usize::MAX)
                .min(view.len());
            let end = start.saturating_add(count as usize).min(view.len());
            return Ok((view[start..end].to_vec(), end == view.len()));
        }
        let mut file = std::fs::File::open(path).map_err(io)?;
        let size = file.metadata().map_err(io)?.len();
        file.seek(SeekFrom::Start(offset)).map_err(io)?;
        let mut buffer = vec![0; count as usize];
        let n = file.read(&mut buffer).map_err(io)?;
        buffer.truncate(n);
        Ok((buffer, offset.saturating_add(n as u64) >= size))
    }
    pub fn write(&self, id: u64, offset: u64, data: &[u8]) -> Result<fattr3, nfsstat3> {
        let vpath = self.vpath(id)?;
        self.writable(&vpath)?;
        if data.is_empty() {
            return self.attr(id);
        }
        let path = self.path(&vpath)?;
        let end = offset
            .checked_add(data.len() as u64)
            .filter(|end| *end <= MAX_FILE)
            .ok_or(nfsstat3::NFS3ERR_FBIG)?;
        if let Some((shell, mut view)) = self.shell(&path)? {
            if end > MAX_SHELL {
                return Err(nfsstat3::NFS3ERR_FBIG);
            }
            view.resize(view.len().max(end as usize), 0);
            view[offset as usize..end as usize].copy_from_slice(data);
            self.replace_shell(&path, &shell, view)?;
        } else {
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .open(&path)
                .map_err(io)?;
            file.seek(SeekFrom::Start(offset)).map_err(io)?;
            file.write_all(data).map_err(io)?;
            file.sync_all().map_err(io)?;
        }
        self.attr(id)
    }
    fn replace_shell(&self, path: &Path, original: &str, view: Vec<u8>) -> Result<(), nfsstat3> {
        let text = String::from_utf8(view).map_err(|_| nfsstat3::NFS3ERR_INVAL)?;
        // An embedded delimiter changes the projection, so refuse it before committing.
        if text.contains("</script>") {
            return Err(nfsstat3::NFS3ERR_INVAL);
        }
        let wrapped = wrap_markdown(original, &text);
        if wrapped.len() as u64 > MAX_SHELL {
            return Err(nfsstat3::NFS3ERR_FBIG);
        }
        let temp = path.with_file_name(format!(".braid-{}.tmp", uuid::Uuid::new_v4()));
        let result = (|| {
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temp)?;
            file.write_all(wrapped.as_bytes())?;
            file.sync_all()?;
            drop(file);
            std::fs::rename(&temp, path)?;
            sync_parent(path)
        })();
        if result.is_err() {
            let _ = std::fs::remove_file(temp);
        }
        result.map_err(io)
    }
    pub fn setattr(&self, id: u64, attr: sattr3) -> Result<fattr3, nfsstat3> {
        let vpath = self.vpath(id)?;
        self.writable(&vpath)?;
        let path = self.path(&vpath)?;
        let metadata = std::fs::metadata(&path).map_err(io)?;
        let current = self.attr(id)?;
        if matches!(attr.uid,set_uid3::uid(uid) if uid != current.uid)
            || matches!(attr.gid,set_gid3::gid(gid) if gid != current.gid)
        {
            return Err(nfsstat3::NFS3ERR_NOTSUPP);
        }
        #[cfg(not(unix))]
        if matches!(attr.mode,set_mode3::mode(mode) if mode != current.mode) {
            return Err(nfsstat3::NFS3ERR_NOTSUPP);
        }
        let mut times = std::fs::FileTimes::new();
        let now = SystemTime::now();
        if let Ok(value) = metadata.accessed() {
            times = times.set_accessed(value);
        }
        if let Ok(value) = metadata.modified() {
            times = times.set_modified(value);
        }
        match attr.atime {
            set_atime::DONT_CHANGE => {}
            set_atime::SET_TO_SERVER_TIME => times = times.set_accessed(now),
            set_atime::SET_TO_CLIENT_TIME(value) => times = times.set_accessed(from_time(value)?),
        }
        match attr.mtime {
            set_mtime::DONT_CHANGE => {}
            set_mtime::SET_TO_SERVER_TIME => times = times.set_modified(now),
            set_mtime::SET_TO_CLIENT_TIME(value) => times = times.set_modified(from_time(value)?),
        }
        if metadata.is_dir() {
            if !matches!(attr.size, set_size3::Void) {
                return Err(nfsstat3::NFS3ERR_ISDIR);
            }
            #[cfg(unix)]
            {
                if let set_mode3::mode(mode) = attr.mode {
                    use std::os::unix::fs::PermissionsExt;
                    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode & 0o7777))
                        .map_err(io)?;
                }
                let file = std::fs::File::open(&path).map_err(io)?;
                file.set_times(times).map_err(io)?;
                file.sync_all().map_err(io)?;
            }
            #[cfg(not(unix))]
            if !matches!(attr.atime, set_atime::DONT_CHANGE)
                || !matches!(attr.mtime, set_mtime::DONT_CHANGE)
            {
                return Err(nfsstat3::NFS3ERR_NOTSUPP);
            }
            return self.attr(id);
        }
        if let set_size3::size(size) = attr.size {
            if size > MAX_FILE {
                return Err(nfsstat3::NFS3ERR_FBIG);
            }
        }
        let replacement = if let (set_size3::size(size), Some((shell, mut view))) =
            (attr.size, self.shell(&path)?)
        {
            if size > MAX_SHELL {
                return Err(nfsstat3::NFS3ERR_FBIG);
            }
            view.resize(size as usize, 0);
            let text = String::from_utf8(view).map_err(|_| nfsstat3::NFS3ERR_INVAL)?;
            if text.contains("</script>") {
                return Err(nfsstat3::NFS3ERR_INVAL);
            }
            let wrapped = wrap_markdown(&shell, &text);
            if wrapped.len() as u64 > MAX_SHELL {
                return Err(nfsstat3::NFS3ERR_FBIG);
            }
            Some(wrapped)
        } else {
            None
        };
        // Stage all file changes. A failed timestamp/permission/truncate operation
        // cannot leave half of a multi-attribute request in the live file.
        let temp = path.with_file_name(format!(".braid-{}.tmp", uuid::Uuid::new_v4()));
        let result = (|| -> std::io::Result<()> {
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .read(true)
                .create_new(true)
                .open(&temp)?;
            if let Some(text) = replacement {
                file.write_all(text.as_bytes())?;
            } else {
                let mut original = std::fs::File::open(&path)?;
                std::io::copy(&mut original, &mut file)?;
                if let set_size3::size(size) = attr.size {
                    file.set_len(size)?;
                }
            }
            file.set_permissions(metadata.permissions())?;
            #[cfg(unix)]
            if let set_mode3::mode(mode) = attr.mode {
                use std::os::unix::fs::PermissionsExt;
                file.set_permissions(std::fs::Permissions::from_mode(mode & 0o7777))?;
            }
            file.set_times(times)?;
            file.sync_all()?;
            drop(file);
            std::fs::rename(&temp, &path)?;
            sync_parent(&path)
        })();
        if result.is_err() {
            let _ = std::fs::remove_file(temp);
        }
        result.map_err(io)?;
        self.attr(id)
    }
    pub fn create(&self, parent: u64, name: &[u8], directory: bool) -> Result<u64, nfsstat3> {
        let vpath = self.child(parent, name)?;
        self.writable(&vpath)?;
        let path = self.path(&vpath)?;
        if directory {
            std::fs::create_dir(&path).map_err(io)?;
        } else {
            std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
                .map_err(io)?
                .sync_all()
                .map_err(io)?;
        }
        sync_parent(&path).map_err(io)?;
        self.id(&vpath)
    }
    fn writable(&self, path: &str) -> Result<(), nfsstat3> {
        if path == "/" || path == "/blobs" || path.starts_with("/blobs/") {
            Err(nfsstat3::NFS3ERR_ROFS)
        } else {
            Ok(())
        }
    }
    pub fn change(
        &self,
        kind: &str,
        source: &str,
        destination: Option<&str>,
    ) -> Result<(), nfsstat3> {
        self.writable(source)?;
        if let Some(destination) = destination {
            self.writable(destination)?;
            if source == destination {
                return Ok(());
            }
        }
        // Validate paths and source before recording a recoverable operation.
        std::fs::symlink_metadata(self.path(source)?).map_err(io)?;
        if kind == "remove"
            && self.path(source)?.is_dir()
            && std::fs::read_dir(self.path(source)?)
                .map_err(io)?
                .next()
                .is_some()
        {
            return Err(nfsstat3::NFS3ERR_NOTEMPTY);
        }
        if let Some(destination) = destination {
            self.path(destination)?;
        }
        let id = {
            let conn = self.db.lock();
            let pending: u64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM nfs_changes WHERE state IN ('prepared','remote_pending')",
                    [],
                    |row| row.get(0),
                )
                .map_err(|_| nfsstat3::NFS3ERR_IO)?;
            if pending >= 4096 {
                return Err(nfsstat3::NFS3ERR_NOSPC);
            }
            conn.execute(
                "INSERT INTO nfs_changes(kind,source,destination,state) VALUES(?,?,?,'staging')",
                params![kind, source, destination],
            )
            .map_err(|_| nfsstat3::NFS3ERR_IO)?;
            conn.last_insert_rowid()
        };
        let plan = match crate::fs::operations::capture(&self.root, id, source, destination) {
            Ok(plan) => plan,
            Err(error) => {
                self.db
                    .lock()
                    .execute(
                        "UPDATE nfs_changes SET state='failed',last_error=? WHERE id=?",
                        params![error.to_string(), id],
                    )
                    .map_err(|_| nfsstat3::NFS3ERR_IO)?;
                return Err(io(error));
            }
        };
        let plan = serde_json::to_string(&plan).map_err(|_| nfsstat3::NFS3ERR_IO)?;
        self.db
            .lock()
            .execute(
                "UPDATE nfs_changes SET state='prepared',plan=? WHERE id=?",
                params![plan, id],
            )
            .map_err(|_| nfsstat3::NFS3ERR_IO)?;
        self.finish_change(id, kind, source, destination)
    }
    fn finish_change(
        &self,
        id: i64,
        kind: &str,
        source: &str,
        destination: Option<&str>,
    ) -> Result<(), nfsstat3> {
        let old = self.path(source)?;
        match kind {
            "rename" => {
                let new = self.path(destination.ok_or(nfsstat3::NFS3ERR_IO)?)?;
                if old.try_exists().map_err(io)? {
                    if let Err(error) = std::fs::rename(&old, &new) {
                        self.db
                            .lock()
                            .execute("UPDATE nfs_changes SET state='failed' WHERE id=?", [id])
                            .map_err(|_| nfsstat3::NFS3ERR_IO)?;
                        return Err(io(error));
                    }
                } else if !new.try_exists().map_err(io)? {
                    return Err(nfsstat3::NFS3ERR_IO);
                }
                sync_parent(&old).map_err(io)?;
                sync_parent(&new).map_err(io)?;
            }
            "remove" => {
                if old.try_exists().map_err(io)? {
                    // Keep removed bytes in a private recovery directory until an explicit
                    // remote tombstone protocol is selected by the filesystem service.
                    let trash = self.root.join(".braidfs/nfs-trash");
                    std::fs::create_dir_all(&trash).map_err(io)?;
                    let destination = trash.join(id.to_string());
                    if old.is_dir() && std::fs::read_dir(&old).map_err(io)?.next().is_some() {
                        return Err(nfsstat3::NFS3ERR_NOTEMPTY);
                    }
                    std::fs::rename(&old, &destination).map_err(io)?;
                    sync_parent(&destination).map_err(io)?;
                }
                sync_parent(&old).map_err(io)?;
            }
            _ => return Err(nfsstat3::NFS3ERR_IO),
        }
        let mut conn = self.db.lock();
        let tx = conn.transaction().map_err(|_| nfsstat3::NFS3ERR_IO)?;
        let paths: Vec<(u64, String)> = {
            let mut stmt = tx
                .prepare("SELECT id,path FROM inodes")
                .map_err(|_| nfsstat3::NFS3ERR_IO)?;
            let rows = stmt
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                .map_err(|_| nfsstat3::NFS3ERR_IO)?
                .collect::<rusqlite::Result<_>>()
                .map_err(|_| nfsstat3::NFS3ERR_IO)?;
            rows
        };
        if let Some(destination) = destination {
            for (inode, path) in &paths {
                if beneath(path, destination) && !beneath(path, source) {
                    tx.execute("DELETE FROM inodes WHERE id=?", [inode])
                        .map_err(|_| nfsstat3::NFS3ERR_IO)?;
                }
            }
        }
        for (inode, path) in paths {
            if beneath(&path, source) {
                if let Some(destination) = destination {
                    let moved = format!("{destination}{}", &path[source.len()..]);
                    tx.execute("UPDATE inodes SET path=? WHERE id=?", params![moved, inode])
                        .map_err(|_| nfsstat3::NFS3ERR_IO)?;
                } else {
                    tx.execute("DELETE FROM inodes WHERE id=?", [inode])
                        .map_err(|_| nfsstat3::NFS3ERR_IO)?;
                }
            }
        }
        tx.execute(
            "UPDATE nfs_changes SET state='remote_pending' WHERE id=?",
            [id],
        )
        .map_err(|_| nfsstat3::NFS3ERR_IO)?;
        tx.commit().map_err(|_| nfsstat3::NFS3ERR_IO)?;
        Ok(())
    }
}
fn beneath(path: &str, root: &str) -> bool {
    path == root
        || path
            .strip_prefix(root)
            .is_some_and(|suffix| suffix.starts_with('/'))
}
fn sync_parent(path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        std::fs::File::open(
            path.parent()
                .ok_or_else(|| std::io::Error::other("No parent directory"))?,
        )?
        .sync_all()?;
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}
fn time(value: SystemTime) -> nfstime3 {
    let duration = value.duration_since(UNIX_EPOCH).unwrap_or_default();
    nfstime3 {
        seconds: u32::try_from(duration.as_secs()).unwrap_or(u32::MAX),
        nseconds: duration.subsec_nanos(),
    }
}
fn checked_time(value: SystemTime) -> Result<nfstime3, nfsstat3> {
    let duration = value
        .duration_since(UNIX_EPOCH)
        .map_err(|_| nfsstat3::NFS3ERR_IO)?;
    Ok(nfstime3 {
        seconds: u32::try_from(duration.as_secs()).map_err(|_| nfsstat3::NFS3ERR_IO)?,
        nseconds: duration.subsec_nanos(),
    })
}
fn from_time(value: nfstime3) -> Result<SystemTime, nfsstat3> {
    if value.nseconds >= 1_000_000_000 {
        return Err(nfsstat3::NFS3ERR_INVAL);
    }
    UNIX_EPOCH
        .checked_add(std::time::Duration::new(
            u64::from(value.seconds),
            value.nseconds,
        ))
        .ok_or(nfsstat3::NFS3ERR_INVAL)
}
pub(super) fn virtual_attr(id: u64, ftype: ftype3, size: u64) -> fattr3 {
    fattr3 {
        ftype,
        mode: if matches!(ftype, ftype3::NF3DIR) {
            0o755
        } else {
            0o444
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
        atime: time(UNIX_EPOCH),
        mtime: time(UNIX_EPOCH),
        ctime: time(UNIX_EPOCH),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn backing(root: &Path) -> Backing {
        Backing::new(
            root.to_owned(),
            Arc::new(Mutex::new(
                Connection::open(root.join("inodes.sqlite")).unwrap(),
            )),
        )
        .unwrap()
    }
    #[test]
    fn partial_binary_writes_truncate_and_sparse_offsets_are_durable() {
        let root = tempfile::tempdir().unwrap();
        let fs = backing(root.path());
        let id = fs.create(1, b"file.bin", false).unwrap();
        fs.write(id, 0, b"\xff\x00abc").unwrap();
        fs.write(id, 3, b"XY").unwrap();
        assert_eq!(fs.read(id, 1, 10).unwrap(), (b"\x00aXY".to_vec(), true));
        fs.setattr(
            id,
            sattr3 {
                size: set_size3::size(2),
                ..Default::default()
            },
        )
        .unwrap();
        fs.write(id, 4, b"z").unwrap();
        assert_eq!(fs.read(id, 0, 10).unwrap().0, b"\xff\x00\x00\x00z");
        assert!(matches!(
            fs.write(id, MAX_FILE, b"x"),
            Err(nfsstat3::NFS3ERR_FBIG)
        ));
        drop(fs);
        let fs = backing(root.path());
        assert_eq!(fs.id("/file.bin").unwrap(), id);
        assert_eq!(fs.read(id, 0, 10).unwrap().0, b"\xff\x00\x00\x00z");
    }
    #[test]
    fn shell_partial_writes_and_truncation_use_the_same_view_as_reads() {
        let root = tempfile::tempdir().unwrap();
        let fs = backing(root.path());
        let original = "<html><script type=\"statebus\">metadata</script><script type=\"statebus\"># hello world</script></html>";
        std::fs::write(root.path().join("page"), original).unwrap();
        let id = fs.id("/page").unwrap();
        fs.write(id, 8, b"NFS").unwrap();
        assert_eq!(fs.read(id, 0, 100).unwrap().0, b"# hello NFSld");
        fs.setattr(
            id,
            sattr3 {
                size: set_size3::size(11),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(fs.attr(id).unwrap().size, 11);
        assert_eq!(fs.read(id, 0, 100).unwrap().0, b"# hello NFS");
        let disk = std::fs::read_to_string(root.path().join("page")).unwrap();
        assert!(disk.contains(">metadata</script>"));
        assert!(disk.contains("># hello NFS</script>"));
    }
    #[test]
    fn rename_recovers_after_disk_commit_and_retains_descendant_handles() {
        let root = tempfile::tempdir().unwrap();
        let fs = backing(root.path());
        let dir = fs.create(1, b"old", true).unwrap();
        let child = fs.create(dir, b"child", false).unwrap();
        fs.write(child, 0, b"recover me").unwrap();
        fs.db.lock().execute("INSERT INTO nfs_changes(kind,source,destination,state) VALUES('rename','/old','/new','prepared')",[]).unwrap();
        std::fs::rename(root.path().join("old"), root.path().join("new")).unwrap();
        drop(fs);
        let fs = backing(root.path());
        assert_eq!(fs.vpath(dir).unwrap(), "/new");
        assert_eq!(fs.vpath(child).unwrap(), "/new/child");
        assert_eq!(fs.read(child, 0, 100).unwrap().0, b"recover me");
        let pending: u64 = fs
            .db
            .lock()
            .query_row(
                "SELECT COUNT(*) FROM nfs_changes WHERE state='remote_pending'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(pending, 1);
    }
    #[test]
    fn removal_keeps_recovery_bytes_and_never_reuses_stale_handles() {
        let root = tempfile::tempdir().unwrap();
        let fs = backing(root.path());
        let old = fs.create(1, b"old", false).unwrap();
        fs.write(old, 0, b"retained").unwrap();
        fs.change("remove", "/old", None).unwrap();
        assert!(matches!(fs.vpath(old), Err(nfsstat3::NFS3ERR_STALE)));
        let new = fs.create(1, b"new", false).unwrap();
        assert!(new > old);
        let trash = std::fs::read_dir(root.path().join(".braidfs/nfs-trash"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        assert_eq!(std::fs::read(trash).unwrap(), b"retained");
    }
    #[test]
    fn hidden_state_and_nonempty_remove_fail_before_mutation() {
        let root = tempfile::tempdir().unwrap();
        let fs = backing(root.path());
        assert!(matches!(
            fs.path("/.braidfs/config"),
            Err(nfsstat3::NFS3ERR_INVAL)
        ));
        assert!(fs.path("/../outside").is_err());
        let dir = fs.create(1, b"dir", true).unwrap();
        fs.create(dir, b"child", false).unwrap();
        assert!(matches!(
            fs.change("remove", "/dir", None),
            Err(nfsstat3::NFS3ERR_NOTEMPTY)
        ));
        drop(fs);
        let fs = backing(root.path());
        assert!(fs.id("/dir/child").is_ok());
    }
    #[cfg(unix)]
    #[test]
    fn symlinks_never_escape_the_export() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let fs = backing(root.path());
        std::os::unix::fs::symlink(outside.path(), root.path().join("escape")).unwrap();
        assert!(matches!(
            fs.path("/escape/secret"),
            Err(nfsstat3::NFS3ERR_ACCES)
        ));
    }
}
