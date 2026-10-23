use local_link_server::{
    chat::crdt::ChatCrdt,
    core::{config::ChatServerConfig, models::MessageType, store::json_store::JsonChatStore},
};

#[tokio::test]
async fn acknowledged_chat_messages_and_edits_survive_restart() {
    let temp = tempfile::tempdir().unwrap();
    let config = ChatServerConfig::with_base_dir(temp.path());
    let store = JsonChatStore::new(config.clone()).await.unwrap();
    let first = store
        .add_message("room", "alice", "hello", MessageType::Text, None, vec![])
        .await
        .unwrap();
    assert!(!first.parents.iter().any(|v| v.to_string() == first.version));
    let edited = store
        .edit_message("room", &first.id, "hello 😀")
        .await
        .unwrap();
    drop(store);
    let reopened = JsonChatStore::new(config).await.unwrap();
    let messages = reopened.get_messages("room", None).await.unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].content, "hello 😀");
    assert_eq!(messages[0].version, edited.version);
    assert!(!messages[0].edit_history.is_empty());
    assert_eq!(
        reopened.get_conversation_tips("room").await.unwrap(),
        vec![edited.version]
    );
}

#[test]
fn chat_versions_keep_predecessors_and_indexes_across_serialization() {
    let mut chat = ChatCrdt::new("room", "node");
    let (v1, first) = chat.add_message("alice", "first", MessageType::Text, None, vec![]);
    assert!(first.parents.is_empty());
    let (v2, second) = chat.add_message("bob", "second", MessageType::Text, None, vec![]);
    assert_eq!(
        second
            .parents
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
        vec![v1.clone()]
    );
    let state = chat.export_state();
    assert_eq!(state.version_graph.len(), 2);
    let restored = ChatCrdt::import_state(state);
    assert_eq!(restored.get_message_by_version(&v1).unwrap().id, first.id);
    assert_eq!(restored.get_message_by_version(&v2).unwrap().id, second.id);
    assert_eq!(restored.get_frontier(), chat.get_frontier());
}

#[tokio::test]
async fn room_paths_cannot_escape_storage_and_corruption_is_not_an_empty_room() {
    let temp = tempfile::tempdir().unwrap();
    let config = ChatServerConfig::with_base_dir(temp.path());
    let store = JsonChatStore::new(config.clone()).await.unwrap();
    assert!(store
        .get_or_create_room("../escaped", Some("alice"))
        .await
        .is_err());
    assert!(store
        .get_or_create_room("CON", Some("alice"))
        .await
        .is_err());
    drop(store);
    tokio::fs::write(config.storage_dir.join("corrupt.json"), b"{bad")
        .await
        .unwrap();
    assert!(JsonChatStore::new(config).await.is_err());
}

#[tokio::test]
async fn failed_disk_commit_keeps_the_previous_visible_chat_state() {
    let temp = tempfile::tempdir().unwrap();
    let config = ChatServerConfig::with_base_dir(temp.path());
    let store = JsonChatStore::new(config.clone()).await.unwrap();
    store
        .get_or_create_room("room", Some("alice"))
        .await
        .unwrap();
    let path = config.storage_dir.join("room.json");
    tokio::fs::remove_file(&path).await.unwrap();
    tokio::fs::create_dir(&path).await.unwrap();
    assert!(store
        .add_message(
            "room",
            "alice",
            "not committed",
            MessageType::Text,
            None,
            vec![]
        )
        .await
        .is_err());
    assert!(store.get_messages("room", None).await.unwrap().is_empty());
}

#[tokio::test]
async fn a_second_chat_writer_is_rejected_until_the_owner_shuts_down() {
    let root = tempfile::tempdir().unwrap();
    let config = ChatServerConfig::with_base_dir(root.path());
    let owner = JsonChatStore::new(config.clone()).await.unwrap();
    assert!(JsonChatStore::new(config.clone()).await.is_err());
    owner
        .add_message("room", "alice", "durable", MessageType::Text, None, vec![])
        .await
        .unwrap();
    drop(owner);
    let reopened = JsonChatStore::new(config).await.unwrap();
    assert_eq!(
        reopened.get_messages("room", None).await.unwrap()[0].content,
        "durable"
    );
}

#[tokio::test]
async fn remote_chat_edit_batches_persist_and_replay_after_restart() {
    let temp = tempfile::tempdir().unwrap();
    let config = ChatServerConfig::with_base_dir(temp.path());
    let store = JsonChatStore::new(config.clone()).await.unwrap();
    let mut origin = ChatCrdt::new("room", "origin");
    let (_, message) =
        origin.add_message("alice", "first", MessageType::Text, Some("thread"), vec![]);
    let first = origin.get_frontier();
    store
        .merge_updates("room", origin.generate_sync_braid(&[]).unwrap())
        .await
        .unwrap();
    origin.edit_message(&message.id, "edited", "alice").unwrap();
    store
        .merge_updates("room", origin.generate_sync_braid(&first).unwrap())
        .await
        .unwrap();
    drop(store);
    let reopened = JsonChatStore::new(config).await.unwrap();
    let recovered = reopened.get_message("room", &message.id).await.unwrap();
    assert_eq!(recovered.content, "edited");
    assert_eq!(recovered.reply_to.as_deref(), Some("thread"));
    assert_eq!(
        reopened
            .generate_sync_braid("room", &[])
            .await
            .unwrap()
            .len(),
        2
    );
    let before = origin.export_to_crdt().unwrap();
    let mut forged = origin.generate_sync_braid(&first).unwrap();
    forged[0].version = "forged".into();
    forged[0].author = "mallory".into();
    assert!(origin.merge_updates(forged).is_err());
    assert_eq!(origin.export_to_crdt().unwrap(), before);
    assert!(origin.import_from_crdt("{bad").is_err());
    assert_eq!(origin.export_to_crdt().unwrap(), before);
}
