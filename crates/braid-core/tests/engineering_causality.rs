use braid_core::{replica::ReplicaStore, Patch, Update, Version};

fn edit(id: &str, parent: &str, offset: usize, content: &str) -> Update {
    let mut update = Update::patched(
        Version::new(id),
        vec![Patch::new(
            "text",
            format!("[{offset}:{offset}]"),
            content.to_owned(),
        )],
    );
    update.parents = vec![Version::new(parent)];
    update.merge_type = Some("diamond".into());
    update
}

#[tokio::test]
async fn concurrent_diamond_edits_converge_and_keep_both_heads_after_restart() {
    let temp = tempfile::tempdir().unwrap();
    let left = ReplicaStore::new(temp.path().join("left")).unwrap();
    let right = ReplicaStore::new(temp.path().join("right")).unwrap();
    let mut seed = Update::snapshot(Version::new("seed"), "a😀b");
    seed.merge_type = Some("diamond".into());
    for store in [&left, &right] {
        store.commit("doc", &seed, "diamond").await.unwrap();
    }
    let alice = edit("alice", "seed", 2, "A");
    let bob = edit("bob", "seed", 2, "B");
    left.commit("doc", &alice, "diamond").await.unwrap();
    left.commit("doc", &bob, "diamond").await.unwrap();
    right.commit("doc", &bob, "diamond").await.unwrap();
    right.commit("doc", &alice, "diamond").await.unwrap();
    let left = ReplicaStore::new(temp.path().join("left")).unwrap();
    let document = left.load("doc").await.unwrap().unwrap();
    assert_eq!(
        document.content,
        right.load("doc").await.unwrap().unwrap().content
    );
    assert_eq!(document.heads, vec!["alice", "bob"]);
    assert_eq!(document.history.len(), 3);
    assert!(document.content.contains('A') && document.content.contains('B'));
    // A subsequent event observes both branches, without erasing either event.
    let mut merged = Update::patched(
        Version::new("merged"),
        vec![Patch::new("text", "[0:0]", "!")],
    );
    merged.parents = vec![Version::new("bob"), Version::new("alice")];
    merged.merge_type = Some("diamond".into());
    left.commit("doc", &merged, "diamond").await.unwrap();
    let saved = left.load("doc").await.unwrap().unwrap();
    assert_eq!(saved.heads, vec!["merged"]);
    assert_eq!(saved.version_graph.len(), 4);
    assert!(saved.content.starts_with('!'));
}

#[tokio::test]
async fn concurrent_simpleton_writes_still_return_a_conflict_without_mutation() {
    let temp = tempfile::tempdir().unwrap();
    let store = ReplicaStore::new(temp.path().to_owned()).unwrap();
    let seed = Update::snapshot(Version::new("seed"), "abc");
    store.commit("doc", &seed, "simpleton").await.unwrap();
    let mut first = edit("first", "seed", 1, "X");
    first.merge_type = Some("simpleton".into());
    store.commit("doc", &first, "simpleton").await.unwrap();
    let before = tokio::fs::read(store.path("doc")).await.unwrap();
    let mut second = edit("second", "seed", 1, "Y");
    second.merge_type = Some("simpleton".into());
    assert!(store.commit("doc", &second, "simpleton").await.is_err());
    assert_eq!(before, tokio::fs::read(store.path("doc")).await.unwrap());
}

#[tokio::test]
async fn seeded_causal_schedules_converge_with_restarts_and_duplicate_delivery() {
    for seed in 0usize..24 {
        let root = tempfile::tempdir().unwrap();
        let stores = [
            ReplicaStore::new(root.path().join("a")).unwrap(),
            ReplicaStore::new(root.path().join("b")).unwrap(),
        ];
        let base =
            Update::snapshot(Version::new("root"), "a\u{1f600}bc").with_merge_type("diamond");
        for store in &stores {
            store.commit("doc", &base, "diamond").await.unwrap();
        }
        let mut branches = Vec::new();
        for branch in 0..3 {
            let first_id = format!("{seed}-{branch}-first");
            let first = if (seed + branch) % 2 == 0 {
                edit(&first_id, "root", (seed + branch) % 5, "X")
            } else {
                let mut update =
                    Update::patched(Version::new(&first_id), vec![Patch::text("[1:2]", "")])
                        .with_merge_type("diamond");
                update.parents = vec![Version::new("root")];
                update
            };
            let next = edit(&format!("{seed}-{branch}-next"), &first_id, 0, "!");
            branches.push([first, next]);
        }
        for (replica, store) in stores.iter().enumerate() {
            let order = if replica == 0 { [0, 1, 2] } else { [2, 0, 1] };
            for branch in order {
                for event in &branches[branch] {
                    // A fresh handle forces operation-log decoding between every event.
                    let reopened =
                        ReplicaStore::new(root.path().join(if replica == 0 { "a" } else { "b" }))
                            .unwrap();
                    reopened.commit("doc", event, "diamond").await.unwrap();
                    store.commit("doc", event, "diamond").await.unwrap();
                }
            }
        }
        let a = stores[0].load("doc").await.unwrap().unwrap();
        let b = stores[1].load("doc").await.unwrap().unwrap();
        assert_eq!(a.content, b.content, "schedule seed {seed}");
        assert_eq!(a.heads, b.heads, "schedule seed {seed}");
        assert_eq!(a.version_graph.len(), 7);
        assert_eq!(a.history.len(), 7);
    }
}
