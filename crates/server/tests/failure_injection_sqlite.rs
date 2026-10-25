//! SQLite contention at the account boundary: a competing writer holds the database, the
//! application must fail within its busy timeout (no hang, no panic, no partial account),
//! and succeed unchanged once the writer finishes.
use local_link_server::core::{auth::AuthManager, database};
use std::time::{Duration, Instant};

#[tokio::test]
async fn a_busy_database_fails_cleanly_and_recovers_without_a_partial_account() {
    let root = tempfile::tempdir().unwrap();
    let auth = AuthManager::new(root.path()).await.unwrap();
    let pool = database::open(&root.path().join("users.sqlite"))
        .await
        .unwrap();

    // An unrelated writer takes the write lock and keeps it.
    let mut blocker = pool.acquire().await.unwrap();
    database::query("BEGIN IMMEDIATE")
        .execute(&mut *blocker)
        .await
        .unwrap();

    let started = Instant::now();
    let outcome = auth
        .signup(
            "busy@example.test".into(),
            "busy".into(),
            "test-password".into(),
            None,
        )
        .await;
    let waited = started.elapsed();
    assert!(
        outcome.is_err(),
        "a write behind a held lock cannot succeed"
    );
    // The configured busy timeout bounds the wait: it neither returns instantly nor hangs.
    assert!(
        waited >= Duration::from_secs(4) && waited < Duration::from_secs(30),
        "busy wait was {waited:?}"
    );
    // The failed signup left nothing behind.
    database::query("ROLLBACK")
        .execute(&mut *blocker)
        .await
        .unwrap();
    drop(blocker);
    let leftovers: i64 =
        database::query_as::<_, (i64,)>("SELECT COUNT(*) FROM users WHERE email=?")
            .bind("busy@example.test")
            .fetch_one(&pool)
            .await
            .unwrap()
            .0;
    assert_eq!(leftovers, 0, "no partial account after a busy failure");

    // The same request now succeeds, and the account works.
    auth.signup(
        "busy@example.test".into(),
        "busy".into(),
        "test-password".into(),
        None,
    )
    .await
    .unwrap();
    let (_, session) = auth
        .login("busy@example.test".into(), "test-password".into())
        .await
        .unwrap();
    assert!(auth.validate_session(&session.token).await.is_ok());
}
