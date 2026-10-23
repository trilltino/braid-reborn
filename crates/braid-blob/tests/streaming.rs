use braid_blob::{BlobStore, BlobUpload};
use braid_http::types::Version;
use bytes::Bytes;
use sha2::{Digest, Sha256};
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, ReadBuf};

#[tokio::test]
async fn streamed_upload_is_verified_and_replayed_without_changing_the_head() {
    let dir = tempfile::tempdir().unwrap();
    let objects = dir.path().join("data");
    let store = BlobStore::new(objects.clone(), dir.path().join("meta.sqlite"))
        .await
        .unwrap();
    let data = vec![0xab; 1024 * 1024];
    let hash = format!("{:x}", Sha256::digest(&data));
    let mut options = BlobUpload::new(
        vec![Version::new("one")],
        vec![],
        Some("application/octet-stream".into()),
    );
    options.expected_hash = Some(hash.clone());
    options.max_bytes = data.len() as u64;
    let (mut writer, reader) = tokio::io::duplex(4096);
    let source = data.clone();
    let producer = tokio::spawn(async move {
        writer.write_all(&source).await.unwrap();
    });
    store.put_stream("file", reader, options).await.unwrap();
    producer.await.unwrap();
    let (mut file, meta) = store.open("file").await.unwrap().unwrap();
    assert_eq!(meta.content_hash.as_deref(), Some(hash.as_str()));
    assert_eq!(meta.size, Some(data.len() as u64));
    let mut received = Vec::new();
    file.read_to_end(&mut received).await.unwrap();
    assert_eq!(received, data);
    store
        .put(
            "file",
            Bytes::from_static(b"new"),
            vec![Version::new("two")],
            vec![Version::new("one")],
            None,
        )
        .await
        .unwrap();
    store
        .put(
            "file",
            Bytes::from(data),
            vec![Version::new("one")],
            vec![],
            Some("application/octet-stream".into()),
        )
        .await
        .unwrap();
    assert_eq!(store.get("file").await.unwrap().unwrap().0.as_ref(), b"new");
    assert!(
        objects.join("objects").join(hash).is_file(),
        "older content stays retained"
    );
}

struct FailingReader(bool);
impl AsyncRead for FailingReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.0 {
            Poll::Ready(Err(io::Error::other("injected read failure")))
        } else {
            self.0 = true;
            buffer.put_slice(b"partial");
            Poll::Ready(Ok(()))
        }
    }
}

#[tokio::test]
async fn partial_stream_limit_and_digest_failures_never_publish_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let store = BlobStore::new(dir.path().join("data"), dir.path().join("meta.sqlite"))
        .await
        .unwrap();
    let options = BlobUpload::new(vec![Version::new("one")], vec![], None);
    assert!(store
        .put_stream("partial", FailingReader(false), options.clone())
        .await
        .is_err());
    let mut limited = options.clone();
    limited.max_bytes = 2;
    assert!(store
        .put_stream("limit", std::io::Cursor::new(b"too big"), limited)
        .await
        .is_err());
    let mut digest = options;
    digest.expected_hash = Some("0".repeat(64));
    assert!(store
        .put_stream("digest", std::io::Cursor::new(b"wrong hash"), digest)
        .await
        .is_err());
    for key in ["partial", "limit", "digest"] {
        assert!(store.get_meta(key).await.unwrap().is_none());
    }
}

#[tokio::test]
async fn damaged_immutable_objects_are_never_replaced_by_a_successful_write() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("data");
    let store = BlobStore::new(path.clone(), dir.path().join("meta.sqlite"))
        .await
        .unwrap();
    let data = Bytes::from_static(b"original");
    store
        .put(
            "file",
            data.clone(),
            vec![Version::new("one")],
            vec![],
            None,
        )
        .await
        .unwrap();
    let meta = store.get_meta("file").await.unwrap().unwrap();
    tokio::fs::write(
        path.join("objects").join(meta.content_hash.unwrap()),
        b"corrupted",
    )
    .await
    .unwrap();
    assert!(store.open("file").await.is_err());
    assert!(store
        .put("file", data, vec![Version::new("one")], vec![], None)
        .await
        .is_err());
}
