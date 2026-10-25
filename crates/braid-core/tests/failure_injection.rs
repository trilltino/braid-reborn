//! Deterministic failure injection at every step of the shared atomic-write primitive,
//! and therefore at every persistence boundary built on it (replica documents, resource
//! policies, channel journals). Each scenario enumerates the steps with a counting pass,
//! then re-runs from a fresh directory injecting a fault at step 1, 2, 3, ...
//!
//! Faults: disk-full and permission errors (with normal cleanup), a simulated crash (no
//! cleanup, like a killed process; NOT a power-loss model), and a pause that lets the test
//! cancel the operation exactly at that step. After each fault the store is reopened and
//! must show a state a reader may legitimately see (complete old, complete new, or a
//! documented intermediate), no file may be torn, and a retry must complete.
//!
//! Stores embed random identities (policy creation ids, engine agent ids), so states are
//! compared through semantic observations rather than raw bytes.
use braid_core::{
    access::{PolicyChange, Principal, Role},
    replica::ReplicaStore,
    types::{Update, Version},
};
use braid_http::{
    client::ChannelJournal,
    durable::fault::{arm, Action},
};
use std::{
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    time::Duration,
};

type Task<T> = Pin<Box<dyn Future<Output = T>>>;

fn alice() -> Principal {
    Principal::Peer("alice".into())
}
fn event(index: usize) -> Update {
    let mut update = Update::snapshot(
        Version::new(format!("v{index}")),
        format!("content {index}"),
    );
    if index > 0 {
        update = update.with_parent(Version::new(format!("v{}", index - 1)));
    }
    update
}

/// Every durable file must be complete JSON; temporary files and locks are exempt.
fn assert_no_torn_files(dir: &Path, context: &str) {
    let mut stack = vec![dir.to_owned()];
    while let Some(current) = stack.pop() {
        for entry in std::fs::read_dir(&current).into_iter().flatten().flatten() {
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().into_owned();
            if path.is_dir() {
                stack.push(path);
            } else if name.ends_with(".json") && !name.starts_with(".braid-") {
                let bytes = std::fs::read(&path).unwrap();
                assert!(
                    serde_json::from_slice::<serde_json::Value>(&bytes).is_ok(),
                    "{context}: {name} is torn ({} bytes)",
                    bytes.len()
                );
            }
        }
    }
}
fn leftover_temporaries(dir: &Path) -> usize {
    let mut count = 0;
    let mut stack = vec![dir.to_owned()];
    while let Some(current) = stack.pop() {
        for entry in std::fs::read_dir(&current).into_iter().flatten().flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if entry.file_name().to_string_lossy().starts_with(".braid-") {
                count += 1;
            }
        }
    }
    count
}

const FAILURES: [Action; 3] = [
    Action::Fail(std::io::ErrorKind::StorageFull),
    Action::Fail(std::io::ErrorKind::PermissionDenied),
    Action::Crash,
];

struct Scenario {
    name: &'static str,
    /// Leaves the directory in the "before" state.
    before: fn(PathBuf) -> Task<()>,
    /// The mutation under test.
    mutate: fn(PathBuf) -> Task<Result<(), String>>,
    /// A semantic summary of what a reader sees in the directory.
    observe: fn(PathBuf) -> Task<String>,
    /// Intermediate states a crash may legitimately leave visible.
    intermediates: &'static [&'static str],
}

async fn run_scenario(scenario: &Scenario) -> usize {
    let counting = tempfile::tempdir().unwrap();
    (scenario.before)(counting.path().to_owned()).await;
    let before = (scenario.observe)(counting.path().to_owned()).await;
    let counter = arm(counting.path(), usize::MAX, Action::Crash);
    (scenario.mutate)(counting.path().to_owned()).await.unwrap();
    let steps = counter.hits();
    drop(counter);
    let after = (scenario.observe)(counting.path().to_owned()).await;
    assert!(
        steps > 0,
        "{}: the mutation never reached the persistence layer",
        scenario.name
    );
    assert_ne!(
        before, after,
        "{}: the mutation changed nothing observable",
        scenario.name
    );

    for step in 1..=steps {
        for action in FAILURES {
            let dir = tempfile::tempdir().unwrap();
            (scenario.before)(dir.path().to_owned()).await;
            let guard = arm(dir.path(), step, action);
            let outcome = (scenario.mutate)(dir.path().to_owned()).await;
            assert!(
                guard.fired(),
                "{}: step {step} was not reached",
                scenario.name
            );
            let point = guard.point();
            drop(guard);
            let context = format!("{} / step {step} {action:?} at {point:?}", scenario.name);
            // A lost write is never reported as success.
            assert!(outcome.is_err(), "{context}: reported success");
            if matches!(action, Action::Fail(_)) {
                assert_eq!(
                    leftover_temporaries(dir.path()),
                    0,
                    "{context}: leaked a temporary"
                );
            }
            assert_no_torn_files(dir.path(), &context);
            let seen = (scenario.observe)(dir.path().to_owned()).await;
            assert!(
                seen == before || seen == after || scenario.intermediates.contains(&seen.as_str()),
                "{context}: unexpected state\n  saw:    {seen}\n  before: {before}\n  after:  {after}"
            );
            // Restart recovery: a retry from the surviving state completes.
            (scenario.mutate)(dir.path().to_owned())
                .await
                .unwrap_or_else(|error| panic!("{context}: retry failed: {error}"));
            assert_eq!(
                (scenario.observe)(dir.path().to_owned()).await,
                after,
                "{context}: retry did not converge"
            );
        }
    }
    // Cancellation: pause at each step, abandon the future, and require a usable store.
    for step in 1..=steps {
        let dir = tempfile::tempdir().unwrap();
        (scenario.before)(dir.path().to_owned()).await;
        let guard = arm(dir.path(), step, Action::Pause);
        // Cancel exactly when the fault fires, however slowly the machine reaches that step.
        {
            let mutation = (scenario.mutate)(dir.path().to_owned());
            tokio::pin!(mutation);
            let reached = tokio::time::timeout(Duration::from_secs(30), async {
                tokio::select! {
                    _ = &mut mutation => false,
                    _ = async { while !guard.fired() { tokio::time::sleep(Duration::from_millis(2)).await; } } => true,
                }
            })
            .await;
            assert_eq!(
                reached,
                Ok(true),
                "{}: step {step} did not suspend",
                scenario.name
            );
            // Dropping `mutation` here abandons the future mid-step.
        }
        assert!(guard.fired());
        drop(guard);
        let context = format!("{} / cancelled at step {step}", scenario.name);
        assert_no_torn_files(dir.path(), &context);
        // Locks held by the abandoned future must have been released with it.
        tokio::time::timeout(
            Duration::from_secs(5),
            (scenario.mutate)(dir.path().to_owned()),
        )
        .await
        .unwrap_or_else(|_| panic!("{context}: left the store locked"))
        .unwrap_or_else(|error| panic!("{context}: retry failed: {error}"));
        assert_eq!(
            (scenario.observe)(dir.path().to_owned()).await,
            after,
            "{context}: diverged"
        );
    }
    steps
}

async fn observe_replica(dir: PathBuf) -> String {
    let store = ReplicaStore::new(dir).unwrap();
    let document = match store.load("/doc").await.unwrap() {
        Some(document) => format!("{}@{:?}", document.content, document.heads),
        None => "none".into(),
    };
    let policy = match store.policy_store().unwrap().get("/doc").await.unwrap() {
        Some(policy) => format!("{:?}/{}", policy.owner, policy.grants.len()),
        None => "none".into(),
    };
    format!("doc={document};policy={policy}")
}
async fn seed_first_event(dir: PathBuf) {
    let store = ReplicaStore::new(dir).unwrap();
    let policies = store.policy_store().unwrap();
    store
        .commit_authorized("/doc", &event(0), "simpleton", &policies, &alice())
        .await
        .unwrap();
}
async fn commit_event(dir: PathBuf, index: usize) -> Result<(), String> {
    let store = ReplicaStore::new(dir).map_err(|e| e.to_string())?;
    let policies = store.policy_store().map_err(|e| e.to_string())?;
    store
        .commit_authorized("/doc", &event(index), "simpleton", &policies, &alice())
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

fn replica_new_resource() -> Scenario {
    Scenario {
        name: "new authorized resource (policy then data)",
        before: |_| Box::pin(async {}),
        mutate: |dir| Box::pin(commit_event(dir, 0)),
        observe: |dir| Box::pin(observe_replica(dir)),
        // Ownership is durable before the data it protects; the reverse is never allowed.
        intermediates: &["doc=none;policy=Peer(\"alice\")/0"],
    }
}
fn replica_next_event() -> Scenario {
    Scenario {
        name: "second event on an existing resource",
        before: |dir| Box::pin(seed_first_event(dir)),
        mutate: |dir| Box::pin(commit_event(dir, 1)),
        observe: |dir| Box::pin(observe_replica(dir)),
        intermediates: &[],
    }
}
fn policy_grant() -> Scenario {
    Scenario {
        name: "resource policy grant",
        before: |dir| Box::pin(seed_first_event(dir)),
        mutate: |dir| {
            Box::pin(async move {
                let store = ReplicaStore::new(dir).map_err(|e| e.to_string())?;
                let policies = store.policy_store().map_err(|e| e.to_string())?;
                policies
                    .change(
                        "/doc",
                        &alice(),
                        PolicyChange::Grant(Principal::Peer("bob".into()), Role::Reader),
                    )
                    .await
                    .map(|_| ())
                    .map_err(|e| e.to_string())
            })
        },
        observe: |dir| Box::pin(observe_replica(dir)),
        intermediates: &[],
    }
}
fn channel_journal() -> Scenario {
    Scenario {
        name: "channel journal checkpoint",
        before: |_| Box::pin(async {}),
        mutate: |dir| {
            Box::pin(async move {
                let mut journal = ChannelJournal::open(dir.join("journal.json"))
                    .await
                    .map_err(|e| e.to_string())?;
                // A fault after the rename (the directory sync) reports an error although the
                // new state is already visible. Initialization is deliberately not idempotent,
                // so a retry that finds exactly this state has nothing left to do.
                if journal.checkpoint().application == Some(serde_json::json!({"text": "hello"})) {
                    return Ok(());
                }
                journal
                    .initialize_application(
                        vec![Version::new("base")],
                        serde_json::json!({"text": "hello"}),
                    )
                    .await
                    .map_err(|e| e.to_string())
            })
        },
        observe: |dir| {
            Box::pin(async move {
                if !dir.join("journal.json").exists() {
                    return "no journal".to_owned();
                }
                let journal = ChannelJournal::open(dir.join("journal.json"))
                    .await
                    .unwrap();
                format!("{:?}", journal.checkpoint().application)
            })
        },
        intermediates: &[],
    }
}

#[tokio::test]
async fn replica_documents_survive_a_fault_at_every_step() {
    let steps = run_scenario(&replica_new_resource()).await;
    // Policy then data: at least two atomic replacements of six steps each.
    assert!(
        steps >= 12,
        "expected at least two atomic writes, saw {steps} steps"
    );
    let steps = run_scenario(&replica_next_event()).await;
    assert!(
        steps >= 6,
        "expected an atomic document write, saw {steps} steps"
    );
}

#[tokio::test]
async fn data_never_exists_without_its_policy_at_any_crash_point() {
    let steps = {
        let dir = tempfile::tempdir().unwrap();
        let counter = arm(dir.path(), usize::MAX, Action::Crash);
        commit_event(dir.path().to_owned(), 0).await.unwrap();
        counter.hits()
    };
    for step in 1..=steps {
        let dir = tempfile::tempdir().unwrap();
        let guard = arm(dir.path(), step, Action::Crash);
        let _ = commit_event(dir.path().to_owned(), 0).await;
        drop(guard);
        let store = ReplicaStore::new(dir.path().to_owned()).unwrap();
        if store.load("/doc").await.unwrap().is_some() {
            let policy = store.policy_store().unwrap().get("/doc").await.unwrap();
            assert!(
                policy.is_some(),
                "step {step}: data was published before its ownership policy"
            );
        }
    }
}

#[tokio::test]
async fn policies_and_journals_survive_a_fault_at_every_step() {
    run_scenario(&policy_grant()).await;
    run_scenario(&channel_journal()).await;
}

// ---- Subprocess termination --------------------------------------------------------------
// This is process-kill evidence: the operating system keeps everything the dead process had
// written. It says nothing about power loss, where unsynced data may be lost.

/// Child body, started only by the test below.
#[test]
#[ignore = "child process of killed_writer_leaves_every_acknowledged_event_and_releases_ownership"]
fn child_replica_writer() {
    use std::io::Write;
    let Ok(dir) = std::env::var("BRAID_FAULT_DIR") else {
        return;
    };
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let store = ReplicaStore::new(PathBuf::from(dir)).unwrap();
        let policies = store.policy_store().unwrap();
        for index in 0.. {
            store
                .commit_authorized("/doc", &event(index), "simpleton", &policies, &alice())
                .await
                .unwrap();
            // Acknowledge only after the commit returned.
            println!("ACK {index}");
            std::io::stdout().flush().unwrap();
        }
    });
}

#[test]
fn killed_writer_leaves_every_acknowledged_event_and_releases_ownership() {
    use std::io::{BufRead, BufReader};
    use std::process::{Command, Stdio};
    let runtime = tokio::runtime::Runtime::new().unwrap();
    for seed in 0u64..12 {
        let dir = tempfile::tempdir().unwrap();
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "child_replica_writer",
                "--nocapture",
            ])
            .env("BRAID_FAULT_DIR", dir.path())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
        // Kill after a seed-dependent number of acknowledgments plus a seed-dependent delay,
        // so some kills land inside a write and some between writes.
        let target = 2 + (seed * 5) % 17;
        let mut acknowledged: Option<usize> = None;
        for line in lines.by_ref() {
            let line = line.unwrap();
            if let Some(index) = line.strip_prefix("ACK ") {
                acknowledged = Some(index.parse().unwrap());
                if acknowledged.unwrap() as u64 >= target {
                    break;
                }
            }
        }
        std::thread::sleep(Duration::from_micros((seed * 977) % 4000));
        child.kill().unwrap();
        child.wait().unwrap();
        let acknowledged = acknowledged.expect("the child acknowledged nothing");

        runtime.block_on(async {
            let store = ReplicaStore::new(dir.path().to_owned()).unwrap();
            // Ownership is released with the process, and the store opens.
            let document = store.load("/doc").await.expect("store loads after a kill").unwrap();
            let present = document.history.len();
            assert!(
                present > acknowledged,
                "seed {seed}: event {acknowledged} was acknowledged but only {present} events survived"
            );
            // The surviving history is an unbroken chain v0, v1, ... with no gap or tear.
            for (index, update) in document.history.iter().enumerate() {
                assert_eq!(update.version, vec![Version::new(format!("v{index}"))], "seed {seed}");
            }
            assert_eq!(document.heads, vec![format!("v{}", present - 1)], "seed {seed}");
            assert_no_torn_files(dir.path(), &format!("seed {seed}"));
            // A new writer can continue immediately.
            commit_event(dir.path().to_owned(), present).await.unwrap();
        });
    }
}
