#![cfg(feature = "nfs")]
//! Independent RPC client: adapter-only calls cannot detect a wire handler which
//! drops a continuation cookie before invoking the adapter.
use braid_core::fs::nfs::BraidNfsBackend;
use nfsserve::{
    tcp::{NFSTcp, NFSTcpListener},
    vfs::NFSFileSystem,
};
use std::{
    io::{Cursor, Read},
    sync::Arc,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

fn word(input: &mut Cursor<Vec<u8>>) -> u32 {
    let mut bytes = [0; 4];
    Read::read_exact(input, &mut bytes).unwrap();
    u32::from_be_bytes(bytes)
}
fn double(input: &mut Cursor<Vec<u8>>) -> u64 {
    (u64::from(word(input)) << 32) | u64::from(word(input))
}
async fn readdir(
    stream: &mut TcpStream,
    handle: &[u8],
    cookie: u64,
    count: u32,
    plus: bool,
) -> (u32, Vec<(u64, String)>, bool) {
    let mut call = Vec::new();
    // CALL / RPC v2 / NFS program / v3 / READDIR, AUTH_NONE credentials/verifier.
    for value in [
        42u32,
        0,
        2,
        100003,
        3,
        if plus { 17 } else { 16 },
        0,
        0,
        0,
        0,
        handle.len() as u32,
    ] {
        call.extend_from_slice(&value.to_be_bytes());
    }
    call.extend_from_slice(handle);
    while call.len() % 4 != 0 {
        call.push(0);
    }
    call.extend_from_slice(&cookie.to_be_bytes());
    call.extend_from_slice(&[0; 8]);
    call.extend_from_slice(&count.to_be_bytes());
    if plus {
        call.extend_from_slice(&count.to_be_bytes());
    }
    stream
        .write_u32(0x80000000 | call.len() as u32)
        .await
        .unwrap();
    stream.write_all(&call).await.unwrap();
    let marker = tokio::time::timeout(std::time::Duration::from_secs(5), stream.read_u32())
        .await
        .unwrap()
        .unwrap();
    assert_ne!(marker & 0x80000000, 0);
    let size = marker & 0x7fffffff;
    assert!(size <= 65536);
    let mut bytes = vec![0; size as usize];
    stream.read_exact(&mut bytes).await.unwrap();
    let mut input = Cursor::new(bytes);
    assert_eq!(
        [word(&mut input), word(&mut input), word(&mut input)],
        [42, 1, 0]
    );
    assert_eq!(word(&mut input), 0); // AUTH_NONE verifier.
    assert_eq!(word(&mut input), 0);
    assert_eq!(word(&mut input), 0); // Accepted RPC.
    let status = word(&mut input);
    if status != 0 {
        return (status, vec![], false);
    }
    if word(&mut input) != 0 {
        input.set_position(input.position() + 84);
    }
    input.set_position(input.position() + 8); // Cookie verifier.
    let mut entries = Vec::new();
    while word(&mut input) != 0 {
        let _id = double(&mut input);
        let length = word(&mut input) as usize;
        let mut name = vec![0; length];
        Read::read_exact(&mut input, &mut name).unwrap();
        input.set_position(input.position() + (4 - length % 4) as u64 % 4);
        entries.push((double(&mut input), String::from_utf8(name).unwrap()));
    }
    (status, entries, word(&mut input) != 0)
}

#[tokio::test]
async fn plain_readdir_resumes_at_the_wire_cookie_and_rejects_tiny_counts() {
    let root = tempfile::tempdir().unwrap();
    let private = root.path().join(".braidfs");
    let blobs = Arc::new(
        braid_core::BlobStore::new(private.join("blobs"), private.join("blob.sqlite"))
            .await
            .unwrap(),
    );
    let db = Arc::new(parking_lot::Mutex::new(
        rusqlite::Connection::open(private.join("inodes.sqlite")).unwrap(),
    ));
    let backend = BraidNfsBackend::open(root.path().to_owned(), db, blobs).unwrap();
    for n in 0..50 {
        backend
            .create_exclusive(1, &format!("entry-{n:03}").into_bytes().into())
            .await
            .unwrap();
    }
    let handle = backend.id_to_fh(1).data;
    let listener = NFSTcpListener::bind("127.0.0.1:0", backend).await.unwrap();
    let port = listener.get_listen_port();
    let server = tokio::spawn(async move {
        listener.handle_forever().await.unwrap();
    });
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let mut cookie = 0;
    let mut names = std::collections::HashSet::new();
    loop {
        let (status, entries, end) = readdir(&mut stream, &handle, cookie, 512, false).await;
        assert_eq!(status, 0);
        assert!(!entries.is_empty());
        for (next, name) in entries {
            assert!(next > cookie, "RPC continuation repeated its previous page");
            cookie = next;
            assert!(names.insert(name));
        }
        if end {
            break;
        }
    }
    assert_eq!(names.len(), 51); // Fifty files plus the virtual blob directory.
    for plus in [false, true] {
        let (status, _, _) = readdir(&mut stream, &handle, 0, 0, plus).await;
        assert_eq!(status, 10005); // NFS3ERR_TOOSMALL, without arithmetic underflow.
    }
    drop(stream);
    server.abort();
}
