use axum::{
    extract::{Path, State},
    http::{HeaderMap, HeaderValue},
};
use local_link_server::{
    chat::{friends::FriendManager, mail::MailManager},
    core::{
        auth::AuthManager,
        config::{AppState, ChatServerConfig},
        pages::{LocalOrgManager, PagesManager},
        store::json_store::JsonChatStore,
    },
};
use std::sync::Arc;

async fn application() -> (tempfile::TempDir, AppState) {
    let temp = tempfile::tempdir().unwrap();
    let config = ChatServerConfig::with_base_dir(temp.path());
    let store = Arc::new(JsonChatStore::new(config.clone()).await.unwrap());
    let state = AppState {
        config,
        store: store.clone(),
        auth: Arc::new(AuthManager::new(temp.path()).await.unwrap()),
        friends: Arc::new(FriendManager::new(temp.path()).await.unwrap()),
        ai_manager: None,
        daemon: None,
        mail_manager: Arc::new(MailManager::new(store)),
        pages_manager: Arc::new(PagesManager::new(0, temp.path().join("pages"))),
        local_org_manager: Arc::new(LocalOrgManager::new(temp.path().to_str().unwrap())),
    };
    (temp, state)
}

#[tokio::test]
async fn chat_historical_get_returns_the_requested_message_revision() {
    use local_link_server::core::models::{ChatSnapshot, MessageType};
    let (_root, state) = application().await;
    let first = state
        .store
        .add_message("room", "alice", "before", MessageType::Text, None, vec![])
        .await
        .unwrap();
    let edited = state
        .store
        .edit_message("room", &first.id, "after")
        .await
        .unwrap();
    let mut headers = HeaderMap::new();
    headers.insert(
        "version",
        HeaderValue::from_str(&format!("\"{}\"", first.version)).unwrap(),
    );
    let response = local_link_server::chat::handlers::chat::get_chat_room(
        Path("room".into()),
        headers.clone(),
        State(state.clone()),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(
        response.headers()["version"],
        format!("\"{}\"", first.version)
    );
    assert_eq!(
        response.headers()["current-version"],
        format!("\"{}\"", edited.version)
    );
    let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let snapshot: ChatSnapshot = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(snapshot.messages.len(), 1);
    assert_eq!(snapshot.messages[0].content, "before");
    assert_eq!(snapshot.messages[0].version, first.version);
    for value in ["?0", "?1"] {
        headers.insert("subscribe", HeaderValue::from_static(value));
        let response = local_link_server::chat::handlers::chat::get_chat_room(
            Path("room".into()),
            headers.clone(),
            State(state.clone()),
        )
        .await
        .unwrap_err();
        assert_eq!(response.status(), 400);
    }
}

#[test]
fn legacy_chat_snapshot_keeps_current_data_without_inventing_history() {
    use local_link_server::{chat::crdt::ChatCrdt, core::models::MessageType};
    let mut original = ChatCrdt::new("room", "legacy");
    let (first, message) = original.add_message("alice", "before", MessageType::Text, None, vec![]);
    original
        .edit_message(&message.id, "after", "alice")
        .unwrap();
    let mut legacy = original.export_state();
    legacy.events.clear();
    let restored = ChatCrdt::import_state(legacy);
    let current = restored.snapshot_at(&restored.get_frontier()).unwrap();
    assert_eq!(current.get_messages_sorted()[0].content, "after");
    assert!(matches!(
        restored.snapshot_at(&[braid_http::types::Version::new(first)]),
        Err(braid_core::replica::ReplicaError::HistoryUnavailable(_))
    ));
    assert_eq!(restored.export_state().events.len(), 0);
}

#[tokio::test]
async fn subsecond_chat_heartbeat_must_not_panic_when_the_stream_is_polled() {
    let (_root, state) = application().await;
    let mut headers = HeaderMap::new();
    headers.insert("subscribe", HeaderValue::from_static("true"));
    headers.insert("heartbeats", HeaderValue::from_static("500ms"));
    let response = local_link_server::chat::handlers::braid_subscribe::braid_subscribe(
        Path("room".into()),
        State(state),
        headers,
    )
    .await
    .unwrap();
    assert_eq!(response.headers()["heartbeats"], "0.5");
    assert!(!response.headers().contains_key("connection"));
    let task = tokio::spawn(async move {
        use futures::StreamExt;
        response.into_body().into_data_stream().next().await
    });
    let result = tokio::time::timeout(std::time::Duration::from_secs(2), task)
        .await
        .unwrap();
    assert!(result.is_ok(), "chat stream task panicked: {result:?}");
}

#[tokio::test]
async fn invalid_stream_headers_fail_before_polling() {
    let (_root, state) = application().await;
    for value in ["0", "50ms", "garbage", "NaN", "86401s"] {
        let mut headers = HeaderMap::new();
        headers.insert("subscribe", HeaderValue::from_static("true"));
        headers.insert("heartbeats", HeaderValue::from_str(value).unwrap());
        let result = local_link_server::chat::handlers::braid_subscribe::braid_subscribe(
            Path("room".into()),
            State(state.clone()),
            headers,
        )
        .await;
        assert_eq!(
            result.unwrap_err(),
            axum::http::StatusCode::BAD_REQUEST,
            "{value}"
        );
    }
    let mut headers = HeaderMap::new();
    headers.insert("subscribe", HeaderValue::from_static("true"));
    headers.insert("parents", HeaderValue::from_static("\"missing\""));
    let result = local_link_server::chat::handlers::braid_subscribe::braid_subscribe(
        Path("room".into()),
        State(state),
        headers,
    )
    .await;
    assert_eq!(result.unwrap_err().as_u16(), 432);
}

#[tokio::test]
async fn slow_subscribers_fail_for_repair_and_drop_releases_receiver() {
    use futures::StreamExt;
    use local_link_server::core::store::json_store::{RoomUpdate, UpdateType};
    let (_root, state) = application().await;
    let mut headers = HeaderMap::new();
    headers.insert("subscribe", HeaderValue::from_static("true"));
    let response = local_link_server::chat::handlers::braid_subscribe::braid_subscribe(
        Path("room".into()),
        State(state.clone()),
        headers.clone(),
    )
    .await
    .unwrap();
    let channel = state.store.get_channel("room").await;
    assert_eq!(channel.tx.receiver_count(), 1);
    for n in 0..200 {
        channel
            .tx
            .send(RoomUpdate {
                room_id: "room".into(),
                update_type: UpdateType::Typing,
                data: serde_json::json!({"n":n}),
                crdt_version: None,
            })
            .unwrap();
    }
    let mut stream = response.into_body().into_data_stream();
    let mut failed = false;
    for _ in 0..3 {
        match tokio::time::timeout(std::time::Duration::from_secs(1), stream.next())
            .await
            .unwrap()
        {
            Some(Err(_)) => {
                failed = true;
                break;
            }
            Some(Ok(bytes)) => assert_eq!(bytes.as_ref(), b"\r\n"),
            None => break,
        }
    }
    assert!(failed, "lag must not silently lose updates");
    drop(stream);
    assert_eq!(channel.tx.receiver_count(), 0);
    let response = local_link_server::chat::handlers::braid_subscribe::braid_subscribe(
        Path("room".into()),
        State(state),
        headers,
    )
    .await
    .unwrap();
    assert_eq!(channel.tx.receiver_count(), 1);
    drop(response);
    assert_eq!(channel.tx.receiver_count(), 0);
}

#[tokio::test]
async fn registering_during_a_write_delivers_each_message_once() {
    use futures::StreamExt;
    use local_link_server::core::models::MessageType;
    let (_root, state) = application().await;
    let first = state
        .store
        .add_message("room", "alice", "first", MessageType::Text, None, vec![])
        .await
        .unwrap();
    let room = state.store.get_room("room").await.unwrap().unwrap();
    let guard = room.write().await;
    let mut headers = HeaderMap::new();
    headers.insert("subscribe", HeaderValue::from_static("true"));
    let subscriber_state = state.clone();
    let subscriber = tokio::spawn(async move {
        local_link_server::chat::handlers::braid_subscribe::braid_subscribe(
            Path("room".into()),
            State(subscriber_state),
            headers,
        )
        .await
        .unwrap()
    });
    let channel = state.store.get_channel("room").await;
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while channel.tx.receiver_count() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let writer_state = state.clone();
    let writer = tokio::spawn(async move {
        writer_state
            .store
            .add_message("room", "alice", "second", MessageType::Text, None, vec![])
            .await
            .unwrap()
    });
    drop(guard);
    let response = subscriber.await.unwrap();
    let second = writer.await.unwrap();
    let mut stream = response.into_body().into_data_stream();
    let mut parser = braid_http::client::MessageParser::new();
    let mut messages = Vec::new();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while messages.len() < 2 {
            let bytes = stream.next().await.unwrap().unwrap();
            for frame in parser.feed(&bytes).unwrap() {
                let update = braid_http::client::message_to_update(frame).unwrap();
                let message: local_link_server::core::models::Message =
                    serde_json::from_slice(update.body.as_ref().unwrap()).unwrap();
                messages.push(message.id);
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(messages, vec![first.id, second.id]);
}
