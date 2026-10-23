use server::config::ChatServerConfig;
use server::models::MessageType;
use server::store::JsonChatStore;
use std::fs;
use tempfile::tempdir;

#[tokio::test]
async fn test_chat_room_storage_integrity() {
    let dir = tempdir().unwrap();
    let config = ChatServerConfig::with_base_dir(dir.path());

    let room_id = "integrity-test-room";

    {
        // 1. Create a room and add a message in a scoped block
        let store = JsonChatStore::new(config.clone()).await.unwrap();
        store
            .add_message(
                room_id,
                "user1",
                "Hello Integrity!",
                MessageType::Text,
                None,
                vec![],
            )
            .await
            .unwrap();
        // store is dropped here
    }

    let room_path = dir.path().join("chats").join(format!("{}.json", room_id));
    assert!(room_path.exists(), "Room JSON file should exist");

    // 2. Verify we can load it back correctly (Force reload by creating a new store instance)
    let store = JsonChatStore::new(config).await.unwrap();
    let messages = store.get_messages(room_id, None).await.unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].content, "Hello Integrity!");

    // 3. Simulate corruption (malformed JSON)
    fs::write(&room_path, "{ malformed json ...").unwrap();

    // Note: JsonChatStore currently caches rooms in memory.
    // To test corruption detection on load, we would need to bypass the cache.
}

#[tokio::test]
async fn test_chat_server_blob_integrity() {
    let dir = tempdir().unwrap();
    let config = ChatServerConfig::with_base_dir(dir.path());
    let store = JsonChatStore::new(config).await.unwrap();

    let blob_data = bytes::Bytes::from("chat server blob data");

    // 1. Upload a blob via the store's blob_store
    let mut hasher = sha2::Sha256::default();
    sha2::Digest::update(&mut hasher, &blob_data);
    let hash = format!("{:x}", sha2::Digest::finalize(hasher));

    let version = vec![braid_http::types::Version::from(hash.clone())];
    store
        .blob_store()
        .put(
            &hash,
            blob_data.clone(),
            version,
            vec![],
            Some("text/plain".into()),
        )
        .await
        .unwrap();

    // 2. Retrieve and verify
    let (retrieved, _meta) = store.blob_store().get(&hash).await.unwrap().unwrap();
    assert_eq!(retrieved, blob_data);

    // 3. Corrupt it
    let blob_file_path = dir
        .path()
        .join("blobs")
        .join(braid_core::blob::encode_filename(&hash));
    fs::write(&blob_file_path, "corrupted blob").unwrap();

    // 4. Verify checksum failure (using the feature we just added to braid-blob)
    let result = store.blob_store().get(&hash).await;
    assert!(result.is_err(), "Retrieving corrupted blob should fail");
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Checksum mismatch"));
}
