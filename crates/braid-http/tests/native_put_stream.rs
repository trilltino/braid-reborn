//! The native transport streams an immutable payload with an exact Content-Length
//! and never presents a failed source stream as a complete request.
use braid_http::{
    client::{native_network::NativeNetwork, PayloadStream},
    traits::BraidNetwork,
    types::BraidRequest,
    BraidError, Result,
};
use bytes::Bytes;
use futures::StreamExt;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

struct Received {
    content_length: Option<usize>,
    transfer_chunked: bool,
    body: Vec<u8>,
}

/// Minimal HTTP/1.1 sink: reads the head, then up to `Content-Length` bytes.
async fn sink(listener: TcpListener) -> Received {
    let aborted = Received {
        content_length: None,
        transfer_chunked: false,
        body: Vec::new(),
    };
    let (mut socket, _) = listener.accept().await.unwrap();
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        // A client that aborts before the request head is complete sent nothing usable.
        if socket.read_exact(&mut byte).await.is_err() {
            return aborted;
        }
        head.push(byte[0]);
    }
    let head = String::from_utf8_lossy(&head).to_ascii_lowercase();
    let content_length = head.lines().find_map(|line| {
        line.strip_prefix("content-length:")
            .and_then(|value| value.trim().parse::<usize>().ok())
    });
    let transfer_chunked = head.contains("transfer-encoding: chunked");
    let mut body = Vec::new();
    let mut buffer = vec![0u8; 64 * 1024];
    while content_length.is_none_or(|length| body.len() < length) {
        match socket.read(&mut buffer).await {
            Ok(0) | Err(_) => break,
            Ok(read) => body.extend_from_slice(&buffer[..read]),
        }
    }
    let _ = socket
        .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
        .await;
    Received {
        content_length,
        transfer_chunked,
        body,
    }
}

fn request() -> BraidRequest {
    BraidRequest::new()
        .with_method("PUT")
        .with_version(braid_http::types::Version::new("blob-1"))
}

#[tokio::test]
async fn payload_streams_with_exact_length_and_without_chunked_framing() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/blob", listener.local_addr().unwrap());
    let server = tokio::spawn(sink(listener));
    let content: Vec<u8> = (0..3 * 1024 * 1024)
        .map(|index| (index % 251) as u8)
        .collect();
    let chunks: Vec<Result<Bytes>> = content
        .chunks(64 * 1024)
        .map(|chunk| Ok(Bytes::copy_from_slice(chunk)))
        .collect();
    let body: PayloadStream = futures::stream::iter(chunks).boxed();
    let network = NativeNetwork::new(reqwest::Client::new());
    let response = network
        .put_stream(&url, request(), body, content.len() as u64)
        .await
        .unwrap();
    assert_eq!(response.status, 200);
    let received = server.await.unwrap();
    assert_eq!(received.content_length, Some(content.len()));
    assert!(!received.transfer_chunked);
    assert_eq!(received.body, content);
}

#[tokio::test]
async fn a_failing_source_aborts_the_request_instead_of_completing_it() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/blob", listener.local_addr().unwrap());
    let server = tokio::spawn(sink(listener));
    let chunks: Vec<Result<Bytes>> = vec![
        Ok(Bytes::from(vec![1u8; 4096])),
        Err(BraidError::Payload("source digest changed".into())),
    ];
    let body: PayloadStream = futures::stream::iter(chunks).boxed();
    let network = NativeNetwork::new(reqwest::Client::new());
    let outcome = network.put_stream(&url, request(), body, 10_000).await;
    assert!(
        outcome.is_err(),
        "a truncated payload must not look accepted"
    );
    let received = server.await.unwrap();
    // Either nothing was sent, or the promised length was never delivered.
    assert!(
        received.body.len() < 10_000,
        "the server never saw the promised length"
    );
}
