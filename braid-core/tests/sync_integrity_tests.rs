use braid_core::blob::{atomic_write, BlobStore};
use std::fs;
use std::path::PathBuf;
use tempfile::tempdir;

#[tokio::test]
async fn test_atomic_write_guarantee() {
    let dir = tempdir().unwrap();
    let dest_path = dir.path().join("test_file.txt");
    let temp_folder = dir.path().join("tmp");
    let content = b"hello atomic world";

    // 1. Successful atomic write
    atomic_write(&dest_path, content, &temp_folder).await.expect("Atomic write should succeed");
    assert_eq!(fs::read(&dest_path).unwrap(), content);

    // 2. Overwrite existing file atomically
    let new_content = b"new atomic content";
    atomic_write(&dest_path, new_content, &temp_folder).await.expect("Atomic overwrite should succeed");
    assert_eq!(fs::read(&dest_path).unwrap(), new_content);
}

#[tokio::test]
async fn test_blob_store_checksum_verification() {
    let dir = tempdir().unwrap();
    let blob_dir = dir.path().join("blobs");
    let meta_db = dir.path().join("meta.sqlite");
    
    let store = BlobStore::new(blob_dir.clone(), meta_db).await.unwrap();
    let key = "test-blob";
    let data = bytes::Bytes::from("integrity test data");
    
    // 1. Put blob (will compute hash)
    store.put(key, data.clone(), vec![], vec![], None).await.unwrap();
    
    // 2. Get blob and verify it works
    let (retrieved_data, _meta) = store.get(key).await.unwrap().expect("Blob should exist");
    assert_eq!(retrieved_data, data);
    
    // 3. Manually corrupt the blob file on disk
    let blob_path = blob_dir.join(braid_core::blob::encode_filename(key));
    fs::write(&blob_path, "corrupted data").unwrap();
    
    // 4. Get blob again and verify it fails with Checksum mismatch
    let result = store.get(key).await;
    match result {
        Err(e) => {
            let err_msg = e.to_string();
            assert!(err_msg.contains("Checksum mismatch"), "Error should be checksum mismatch, got: {}", err_msg);
        }
        Ok(_) => panic!("Retrieved corrupted blob without error!"),
    }
}
