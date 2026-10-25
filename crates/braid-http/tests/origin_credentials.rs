use braid_http::{
    client::{http_origin, OriginCredentials, ScopedCredentials},
    types::BraidRequest,
};

#[test]
fn credentials_bind_scheme_host_and_port_and_never_debug_values() {
    let credentials = OriginCredentials {
        bearer_token: Some("private-bearer-value".into()),
        cookie: Some("private=cookie-value".into()),
    };
    let scope = ScopedCredentials::new("HTTPS://Example.test:443/", credentials).unwrap();
    assert_eq!(scope.origin(), "https://example.test");
    assert_eq!(
        http_origin("https://example.test/doc?q=yes").unwrap(),
        "https://example.test"
    );
    let request = scope
        .authenticate("https://example.test/doc", BraidRequest::new())
        .unwrap();
    assert_eq!(
        request.extra_headers["authorization"],
        "Bearer private-bearer-value"
    );
    assert!(!format!("{request:?}").contains("private-bearer-value"));
    for url in [
        "http://example.test/doc",
        "https://example.test:444/doc",
        "https://other.test/doc",
        "https://user:password@example.test/doc",
    ] {
        assert!(
            scope.authenticate(url, BraidRequest::new()).is_err(),
            "{url}"
        );
    }
    let debug = format!("{scope:?}");
    assert!(!debug.contains("private-bearer-value"));
    assert!(!debug.contains("cookie-value"));
    for value in ["", "with space", "line\r\nbreak"] {
        assert!(OriginCredentials {
            bearer_token: Some(value.into()),
            cookie: None
        }
        .validate()
        .is_err());
    }
    let config = braid_http::client::ClientConfig {
        proxy_url: "http://user:private-proxy-value@proxy.test".into(),
        ..Default::default()
    };
    assert!(!format!("{config:?}").contains("private-proxy-value"));
}

#[tokio::test]
async fn native_requests_do_not_follow_redirects_or_turn_puts_into_get_acks() {
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };
    let destination = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let destination_url = format!("http://{}/stolen", destination.local_addr().unwrap());
    let source = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", source.local_addr().unwrap());
    let server = tokio::spawn(async move {
        for code in [307, 303, 307] {
            let (mut stream, _) = source.accept().await.unwrap();
            let mut buffer = vec![0; 8192];
            let mut request = Vec::new();
            let end = loop {
                if let Some(end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                    if request.len() >= end + 8 {
                        break end + 4;
                    }
                }
                let n = stream.read(&mut buffer).await.unwrap();
                assert!(n > 0);
                request.extend_from_slice(&buffer[..n]);
                assert!(request.len() <= 65536);
            };
            let headers = std::str::from_utf8(&request[..end])
                .unwrap()
                .to_ascii_lowercase();
            assert!(headers.starts_with("put /doc "));
            assert!(headers.contains("authorization: bearer private-token"));
            stream.write_all(format!("HTTP/1.1 {code} Redirect\r\nLocation: {destination_url}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
        }
    });
    let credentials = ScopedCredentials::new(
        &origin,
        OriginCredentials {
            bearer_token: Some("private-token".into()),
            cookie: None,
        },
    )
    .unwrap();
    let client = braid_http::BraidClient::new().unwrap();
    let configured = braid_http::BraidClient::with_client_builder(
        reqwest::Client::builder().redirect(reqwest::redirect::Policy::limited(3)),
    )
    .unwrap();
    for (transport, expected) in [
        (client.clone(), 307),
        (client.clone(), 303),
        (configured, 307),
    ] {
        let url = format!("{origin}/doc");
        let request = credentials
            .authenticate(
                &url,
                BraidRequest::new().with_method("PUT").with_body("data"),
            )
            .unwrap();
        assert_eq!(
            transport.fetch(&url, request).await.unwrap().status,
            expected
        );
    }
    server.await.unwrap();
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), destination.accept())
            .await
            .is_err()
    );
    assert!(matches!(
        client.get("http://user:secret@127.0.0.1/doc").await,
        Err(braid_http::BraidError::Config(_))
    ));
}
