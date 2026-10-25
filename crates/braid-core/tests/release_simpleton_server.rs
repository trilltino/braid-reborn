use braid_core::{replica::ReplicaStore, simpleton::SimpletonClient, BraidClient, Update, Version};
use braid_http::client::ChannelJournal;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

#[tokio::test]
async fn concurrent_peers_converge_through_lost_ack_and_restart() {
    let root = tempfile::tempdir().unwrap();
    let store = ReplicaStore::new(root.path().join("server")).unwrap();
    store
        .commit(
            "doc",
            &Update::snapshot(Version::new("seed"), "a\u{1f600}b"),
            "simpleton",
        )
        .await
        .unwrap();
    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let attempts = Arc::new(AtomicUsize::new(0));
    let get_store = store.clone();
    let put_store = store.clone();
    let put_attempts = attempts.clone();
    let put_barrier = barrier.clone();
    let app = axum::Router::new().route(
        "/doc",
        axum::routing::get(move |headers: axum::http::HeaderMap| {
            let store = get_store.clone();
            async move {
                braid_core::server::replica_http::get(store, "doc".into(), headers, "simpleton")
                    .await
            }
        })
        .put(move |headers: axum::http::HeaderMap, body: bytes::Bytes| {
            let store = put_store.clone();
            let attempts = put_attempts.clone();
            let barrier = put_barrier.clone();
            async move {
                use axum::response::IntoResponse;
                let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                if attempt < 2 {
                    barrier.wait().await;
                }
                let response = braid_core::server::replica_http::put(
                    store,
                    "doc".into(),
                    headers,
                    body,
                    "simpleton",
                )
                .await;
                assert!(response.status().is_success(), "{}", response.status());
                if attempt == 0 {
                    axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response()
                } else {
                    response
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/doc", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let client = BraidClient::new().unwrap();
    let mut observer = client
        .subscribe(
            &url,
            braid_core::BraidRequest::new()
                .subscribe()
                .with_merge_type("simpleton"),
        )
        .await
        .unwrap();
    let initial = observer.next().await.unwrap().unwrap();
    let observer_task = tokio::spawn(async move {
        let mut text = initial.body_str().unwrap().to_owned();
        let mut version = initial.version;
        loop {
            let update = observer.next().await.unwrap().unwrap();
            assert_eq!(
                update.parents, version,
                "observer patch must use its actual view"
            );
            text = braid_http::protocol::text::apply_text_patches(
                &text,
                update.patches.as_ref().unwrap(),
            )
            .unwrap();
            version = update.version;
            if text == "aX\u{1f600}Yb" {
                break;
            }
        }
    });
    let left_path = root.path().join("left.json");
    let left = SimpletonClient::connect(
        &client,
        url.as_str(),
        ChannelJournal::open(&left_path).await.unwrap(),
    )
    .await
    .unwrap();
    let right = SimpletonClient::connect(
        &client,
        url.as_str(),
        ChannelJournal::open(root.path().join("right.json"))
            .await
            .unwrap(),
    )
    .await
    .unwrap();
    let mut ticker = tokio::time::interval(std::time::Duration::from_millis(10));
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        while !(left.status().borrow().initialized && right.status().borrow().initialized) {
            ticker.tick().await;
        }
    })
    .await
    .unwrap();
    let (a, b) = tokio::join!(
        left.changed("aX\u{1f600}b".into()),
        right.changed("a\u{1f600}Yb".into())
    );
    a.unwrap();
    b.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(20), barrier.wait())
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            let a = left.status().borrow().clone();
            let b = right.status().borrow().clone();
            assert!(a.error.is_none(), "left: {:?}", a.error);
            assert!(b.error.is_none(), "right: {:?}", b.error);
            if a.text == "aX\u{1f600}Yb"
                && a.text == b.text
                && a.outstanding_puts == 0
                && b.outstanding_puts == 0
            {
                break;
            }
            ticker.tick().await;
        }
    })
    .await
    .unwrap();
    assert!(
        attempts.load(Ordering::SeqCst) >= 3,
        "lost ACK must replay the same event"
    );
    tokio::time::timeout(std::time::Duration::from_secs(2), observer_task)
        .await
        .unwrap()
        .unwrap();
    let document = store.load("doc").await.unwrap().unwrap();
    assert_eq!(
        document.history.len(),
        3,
        "retry must not append another event"
    );
    left.close().await;
    let reopened = SimpletonClient::connect(
        &client,
        &url,
        ChannelJournal::open(&left_path).await.unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(reopened.status().borrow().text, document.content);
    reopened.close().await;
    right.close().await;
    server.abort();
}
