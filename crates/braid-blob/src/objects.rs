//! Immutable payload IO. SQLite references are committed only after promotion.
use crate::store::{encode_filename, BlobMetadata};
use braid_http::{BraidError, Result};
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use tokio::{
    fs,
    io::{AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};

pub(crate) const DEFAULT_BUFFER_LIMIT: u64 = 64 * 1024 * 1024;

#[derive(Debug, Clone)]
pub(crate) struct Objects {
    root: PathBuf,
}

struct StagingFile(PathBuf);
impl Drop for StagingFile {
    fn drop(&mut self) {
        // Cancellation may leave an unreferenced temporary file on Windows if an
        // outstanding OS write still owns the handle. No metadata ever names it.
        if let Err(error) = std::fs::remove_file(&self.0) {
            if error.kind() != std::io::ErrorKind::NotFound {
                tracing::debug!(%error, "Deferred cleanup of uncommitted blob staging file");
            }
        }
    }
}

impl Objects {
    pub(crate) fn new(root: PathBuf) -> Self {
        Self { root }
    }

    pub(crate) async fn stage<R: AsyncRead + Unpin>(
        &self,
        mut reader: R,
        limit: u64,
        expected: Option<&str>,
    ) -> Result<(String, u64)> {
        let directory = self.root.join("objects");
        fs::create_dir_all(&directory).await?;
        let staging = StagingFile(directory.join(format!(".upload-{}", uuid::Uuid::new_v4())));
        let mut options = fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o600);
        let mut file = options.open(&staging.0).await?;
        let mut digest = Sha256::new();
        let mut size = 0u64;
        let mut buffer = vec![0u8; 64 * 1024].into_boxed_slice();
        loop {
            let read = reader.read(&mut buffer).await?;
            if read == 0 {
                break;
            }
            size = size
                .checked_add(read as u64)
                .filter(|n| *n <= limit)
                .ok_or(BraidError::ResourceLimit {
                    resource: "blob upload",
                    limit,
                })?;
            digest.update(&buffer[..read]);
            file.write_all(&buffer[..read]).await?;
        }
        let hash = format!("{:x}", digest.finalize());
        if expected.is_some_and(|value| value != hash) {
            return Err(BraidError::Fs("Blob upload checksum mismatch".into()));
        }
        file.sync_all().await?;
        drop(file);
        let destination = directory.join(&hash);
        match fs::symlink_metadata(&destination).await {
            Ok(metadata) => {
                if !metadata.is_file() || metadata.file_type().is_symlink() {
                    return Err(BraidError::Fs("Invalid content-addressed object".into()));
                }
                let mut existing = fs::File::open(&destination).await?;
                verify(&mut existing, Some(&hash), Some(size)).await?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fs::rename(&staging.0, &destination).await?;
                sync_directory(directory).await?;
            }
            Err(error) => return Err(error.into()),
        }
        // An unreferenced immutable object after cancellation is safe to retain.
        Ok((hash, size))
    }

    pub(crate) async fn open(&self, metadata: &BlobMetadata) -> Result<fs::File> {
        let legacy = self.root.join(encode_filename(&metadata.key));
        let path = match &metadata.content_hash {
            Some(hash) => {
                if hash.len() != 64
                    || !hash
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                {
                    return Err(BraidError::Fs("Invalid stored content hash".into()));
                }
                let object = self.root.join("objects").join(hash);
                if fs::try_exists(&object).await? {
                    object
                } else {
                    legacy
                }
            }
            None => legacy,
        };
        let info = fs::symlink_metadata(&path).await.map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                BraidError::Fs(format!("Missing committed blob data for {}", metadata.key))
            } else {
                error.into()
            }
        })?;
        if !info.is_file() || info.file_type().is_symlink() {
            return Err(BraidError::Fs("Blob payload must be a regular file".into()));
        }
        let mut file = fs::File::open(path).await?;
        verify(&mut file, metadata.content_hash.as_deref(), metadata.size).await?;
        file.rewind().await?;
        Ok(file)
    }
}

async fn verify(
    file: &mut fs::File,
    expected_hash: Option<&str>,
    expected_size: Option<u64>,
) -> Result<()> {
    let mut digest = Sha256::new();
    let mut size = 0u64;
    let mut buffer = vec![0u8; 64 * 1024].into_boxed_slice();
    loop {
        let read = file.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        size = size
            .checked_add(read as u64)
            .ok_or_else(|| BraidError::Fs("Blob length overflow".into()))?;
        digest.update(&buffer[..read]);
    }
    if expected_hash.is_some_and(|expected| expected != format!("{:x}", digest.finalize()))
        || expected_size.is_some_and(|expected| expected != size)
    {
        return Err(BraidError::Fs(
            "Checksum mismatch or incorrect length for blob".into(),
        ));
    }
    Ok(())
}

async fn sync_directory(directory: PathBuf) -> Result<()> {
    #[cfg(unix)]
    tokio::task::spawn_blocking(move || std::fs::File::open(directory)?.sync_all())
        .await
        .map_err(|error| BraidError::Fs(error.to_string()))??;
    #[cfg(not(unix))]
    let _directory = directory;
    Ok(())
}
