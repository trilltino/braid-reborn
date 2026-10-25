#![cfg(feature = "fs")]
use braid_core::fs::config::Config;
use braid_http::{client::OriginCredentials, types::BraidRequest};

#[tokio::test]
async fn config_migration_is_reviewable_resumable_and_rollback_preserves_original_bytes() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("config");
    let original = br#"{"peer_id":"stable-peer","sync":{"https://example.test/doc":true},"cookies":{"example.test":"private-legacy-value"}}"#;
    tokio::fs::write(&path, original).await.unwrap();
    let preview = Config::migration_info(&path).await.unwrap();
    assert_eq!(preview["source_schema"], 1);
    assert!(!preview.to_string().contains("private-legacy-value"));
    assert!(!path.with_extension("v1.backup").exists());
    assert!(
        !root.path().join("daemon.lock").exists(),
        "dry-run must not create locks"
    );
    // Resume after backup creation but before target publication.
    tokio::fs::write(path.with_extension("v1.backup"), original)
        .await
        .unwrap();
    Config::migrate_file(&path).await.unwrap();
    let migrated = tokio::fs::read(&path).await.unwrap();
    Config::migrate_file(&path).await.unwrap();
    assert_eq!(tokio::fs::read(&path).await.unwrap(), migrated);
    assert_eq!(
        tokio::fs::read(path.with_extension("v1.backup"))
            .await
            .unwrap(),
        original
    );
    let config = Config::load_from(&path).await.unwrap();
    assert_eq!(config.schema_version, 2);
    assert_eq!(config.peer_id, "stable-peer");
    let rollback = Config::rollback_info(&path).await.unwrap();
    assert_eq!(rollback["target_schema"], 1);
    assert_eq!(rollback["rollback_needed"], true);
    assert_eq!(tokio::fs::read(&path).await.unwrap(), migrated);
    let request = config
        .authenticate(BraidRequest::new(), "https://example.test/doc")
        .unwrap();
    assert_eq!(
        request.extra_headers["cookie"],
        "token=private-legacy-value"
    );
    for url in [
        "http://example.test/doc",
        "https://example.test:444/doc",
        "https://other.test/doc",
    ] {
        assert!(!config
            .authenticate(BraidRequest::new(), url)
            .unwrap()
            .extra_headers
            .contains_key("cookie"));
    }
    Config::rollback_file(&path).await.unwrap();
    Config::rollback_file(&path).await.unwrap();
    assert_eq!(tokio::fs::read(&path).await.unwrap(), original);
}

#[tokio::test]
async fn later_credentials_block_rollback_and_debug_and_status_redact_values() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("config");
    tokio::fs::write(
        &path,
        br#"{"peer_id":"stable-peer","cookies":{"legacy.test":"private-legacy-token"}}"#,
    )
    .await
    .unwrap();
    Config::migrate_file(&path).await.unwrap();
    let mut config = Config::load_from(&path).await.unwrap();
    config.origin_credentials.insert(
        "http://localhost:4567".into(),
        OriginCredentials {
            bearer_token: Some("private-bearer-token".into()),
            cookie: None,
        },
    );
    config.save_to(&path).await.unwrap();
    assert!(Config::rollback_file(&path).await.is_err());
    let restored = Config::load_from(&path).await.unwrap();
    assert_eq!(
        restored
            .authenticate(BraidRequest::new(), "http://localhost:4567/doc")
            .unwrap()
            .extra_headers["authorization"],
        "Bearer private-bearer-token"
    );
    assert!(restored
        .authenticate(BraidRequest::new(), "http://localhost:4568/doc")
        .unwrap()
        .extra_headers
        .is_empty());
    let visible = format!("{restored:?} {}", restored.redacted());
    assert!(!visible.contains("private-legacy-token"));
    assert!(!visible.contains("private-bearer-token"));
}

#[tokio::test]
async fn unknown_configuration_survives_and_malformed_credentials_do_not_enter_errors() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("config");
    tokio::fs::write(
        &path,
        br#"{"peer_id":"stable","custom_extension":{"private":"extension-value"}}"#,
    )
    .await
    .unwrap();
    Config::migrate_file(&path).await.unwrap();
    let config = Config::load_from(&path).await.unwrap();
    assert_eq!(
        config.extra["custom_extension"]["private"],
        "extension-value"
    );
    assert!(!format!("{config:?}").contains("extension-value"));
    tokio::fs::write(&path, br#"{"peer_id":"stable","origin_credentials":{"https://example.test":"private-malformed-token"}}"#).await.unwrap();
    let error = Config::load_from(&path).await.unwrap_err().to_string();
    assert!(!error.contains("private-malformed-token"));
    assert!(error.contains("line"));
}

#[tokio::test]
async fn config_migration_refuses_live_daemons_future_formats_and_unverified_backups() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("config");
    let original = br#"{"peer_id":"stable-peer"}"#;
    tokio::fs::write(&path, original).await.unwrap();
    let owner = braid_http::durable::writer_lease(&root.path().join("daemon.lock"))
        .await
        .unwrap();
    assert!(Config::migrate_file(&path).await.is_err());
    drop(owner);
    tokio::fs::write(path.with_extension("v1.backup"), b"wrong backup")
        .await
        .unwrap();
    assert!(Config::migrate_file(&path).await.is_err());
    assert_eq!(tokio::fs::read(&path).await.unwrap(), original);
    let future = br#"{"schema_version":999,"peer_id":"stable-peer"}"#;
    tokio::fs::write(&path, future).await.unwrap();
    assert!(Config::load_from(&path).await.is_err());
    assert!(Config::migrate_file(&path).await.is_err());
    assert_eq!(tokio::fs::read(&path).await.unwrap(), future);
}

#[test]
fn internal_config_and_secret_metadata_are_never_scan_candidates() {
    for path in [
        ".braidfs",
        ".braidfs/config",
        ".braidfs/config.v1.backup",
        ".braidfs/channels/pending.json",
        ".braidfs\\config",
    ] {
        assert!(braid_core::fs::config::skip_file(path), "{path}");
    }
    assert!(!braid_core::fs::config::skip_file(".braidfs-other/content"));
    let mut config = Config::default();
    config.origin_credentials.insert(
        "http://user:secret@localhost".into(),
        OriginCredentials {
            bearer_token: Some("token".into()),
            cookie: None,
        },
    );
    assert!(config.validate().is_err());
    let mut config = Config::default();
    config
        .extra
        .insert("schema_version".into(), serde_json::json!(999));
    assert!(config.validate().is_err());
}

#[test]
fn administrative_config_commands_default_to_read_only() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("config");
    let original = br#"{"peer_id":"stable-peer","cookies":{"example.test":"private-cli-value"}}"#;
    std::fs::write(&path, original).unwrap();
    let run = |command: &str, apply: bool| {
        let mut process = std::process::Command::new(env!("CARGO_BIN_EXE_braid-admin"));
        process.arg(command).arg(&path);
        if apply {
            process.arg("--apply");
        }
        let output = process.output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(!String::from_utf8_lossy(&output.stdout).contains("private-cli-value"));
    };
    run("config-preview", false);
    run("config-migrate", false);
    assert_eq!(std::fs::read(&path).unwrap(), original);
    run("config-migrate", true);
    let migrated = std::fs::read(&path).unwrap();
    run("config-rollback", false);
    assert_eq!(std::fs::read(&path).unwrap(), migrated);
    run("config-rollback", true);
    assert_eq!(std::fs::read(&path).unwrap(), original);
}
