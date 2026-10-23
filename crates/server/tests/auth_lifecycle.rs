use local_link_server::core::{auth::AuthManager, database};

#[tokio::test]
async fn logout_and_account_disable_invalidate_sessions_across_managers() {
    let root = tempfile::tempdir().unwrap();
    let first = AuthManager::new(root.path()).await.unwrap();
    let second = AuthManager::new(root.path()).await.unwrap();
    first
        .signup(
            "alice@example.test".into(),
            "alice".into(),
            "test-password".into(),
            None,
        )
        .await
        .unwrap();
    let (_, session) = first
        .login("alice@example.test".into(), "test-password".into())
        .await
        .unwrap();
    assert!(first.validate_session(&session.token).await.is_ok());
    second.logout(&session.token).await.unwrap();
    assert!(first.validate_session(&session.token).await.is_err());
    let (_, session) = first
        .login("alice@example.test".into(), "test-password".into())
        .await
        .unwrap();
    let pool = database::open(&root.path().join("users.sqlite"))
        .await
        .unwrap();
    database::query("UPDATE users SET is_active=0 WHERE email=?")
        .bind("alice@example.test")
        .execute(&pool)
        .await
        .unwrap();
    assert!(first.validate_session(&session.token).await.is_err());
}
