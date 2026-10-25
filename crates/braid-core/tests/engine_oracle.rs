//! Differential test: the copied CRDT engine against the upstream engine at a pinned
//! revision (tests/oracle). Both run the same position-free scripts; they must agree on
//! text, operation count, cross-decoding of each other's encodings, and merge order.
//!
//! Needs the oracle binary, so it is ignored by default: run
//! `node scripts/run-engine-oracle.mjs`, which builds the oracle and sets
//! `BRAID_ORACLE_BIN`. Seeds are saved: a failure writes the exact script to
//! `target/oracle-failures/` and prints the seed; replay with `BRAID_ORACLE_SEED`.
#![cfg(all(feature = "serde", feature = "smallvec"))]

use braid_core::vendor::diamond_types as dt;
use std::{
    collections::BTreeSet,
    io::{BufRead, BufReader, Write},
    process::{Child, ChildStdin, ChildStdout, Command, Stdio},
};

include!("../../../tests/oracle/runner.rs");

struct Oracle {
    child: Child,
    input: ChildStdin,
    output: BufReader<ChildStdout>,
}
impl Oracle {
    fn start() -> Self {
        let binary = std::env::var("BRAID_ORACLE_BIN").expect(
            "BRAID_ORACLE_BIN must name the built oracle; use scripts/run-engine-oracle.mjs",
        );
        let mut child = Command::new(binary)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .expect("oracle starts");
        let input = child.stdin.take().unwrap();
        let output = BufReader::new(child.stdout.take().unwrap());
        Self {
            child,
            input,
            output,
        }
    }
    fn ask(&mut self, request: serde_json::Value) -> serde_json::Value {
        writeln!(self.input, "{request}").unwrap();
        self.input.flush().unwrap();
        let mut line = String::new();
        self.output.read_line(&mut line).expect("oracle replies");
        serde_json::from_str(&line)
            .unwrap_or_else(|_| panic!("oracle closed or sent garbage: {line:?}"))
    }
}
impl Drop for Oracle {
    fn drop(&mut self) {
        let _ = self.child.kill();
    }
}

/// SplitMix64: tiny, dependency-free and stable across platforms.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e3779b97f4a7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        z ^ (z >> 31)
    }
    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound.max(1)
    }
}

const ALPHABET: &[&str] = &[
    "a",
    "b",
    "z",
    " ",
    "\n",
    "\u{e9}",
    "\u{4e2d}",
    "\u{1f600}",
    "\u{1f1fa}\u{1f1f8}",
    "e\u{301}",
    "\u{200d}",
];
fn random_text(rng: &mut Rng) -> String {
    (0..1 + rng.below(5))
        .map(|_| ALPHABET[rng.below(ALPHABET.len() as u64) as usize])
        .collect()
}

/// Replicas edit and occasionally sync; parents are always a true frontier.
fn generate(seed: u64, agents: usize, operations: usize) -> Script {
    let mut rng = Rng(seed ^ ((agents as u64) << 48));
    let mut ancestors: Vec<BTreeSet<usize>> = Vec::new();
    let mut frontiers: Vec<Vec<usize>> = vec![vec![]; agents];
    let mut ops = Vec::new();
    let dominators = |ancestors: &[BTreeSet<usize>], set: BTreeSet<usize>| -> Vec<usize> {
        set.iter()
            .copied()
            .filter(|x| !set.iter().any(|y| y != x && ancestors[*y].contains(x)))
            .collect()
    };
    while ops.len() < operations {
        let agent = rng.below(agents as u64) as usize;
        if agents > 1 && rng.below(100) < 30 {
            let other = (agent + 1 + rng.below(agents as u64 - 1) as usize) % agents;
            let union: BTreeSet<usize> = frontiers[agent]
                .iter()
                .chain(&frontiers[other])
                .copied()
                .collect();
            frontiers[agent] = dominators(&ancestors, union);
            continue;
        }
        let index = ops.len();
        let mut known: BTreeSet<usize> = BTreeSet::from([index]);
        for parent in &frontiers[agent] {
            known.extend(&ancestors[*parent]);
        }
        ancestors.push(known);
        let insert = (rng.below(100) < 65).then(|| random_text(&mut rng));
        ops.push(ScriptOp {
            agent,
            parents: frontiers[agent].clone(),
            insert,
            position_permille: rng.below(1001) as u32,
            length_permille: rng.below(400) as u32,
        });
        frontiers[agent] = vec![index];
    }
    Script { agents, ops }
}

fn check_script(oracle: &mut Oracle, script: &Script) -> Result<(), String> {
    let local = run(script)?;
    let reply = oracle.ask(serde_json::json!({ "cmd": "run", "script": script }));
    if reply.get("panic").is_some() || reply.get("error").is_some() {
        return Err(format!("oracle failed to run the script: {reply}"));
    }
    let remote: Outcome = serde_json::from_value(reply).map_err(|e| e.to_string())?;
    if local.text != remote.text {
        return Err(format!(
            "merged text differs\n local: {:?}\noracle: {:?}",
            local.text, remote.text
        ));
    }
    if local.operations != remote.operations {
        return Err(format!(
            "operation count differs: {} vs {}",
            local.operations, remote.operations
        ));
    }
    // Each engine must read the other's encoding.
    if decode_text(&remote.encoded)? != remote.text {
        return Err("copied engine decodes the oracle's encoding to different text".into());
    }
    let decoded = oracle.ask(serde_json::json!({ "cmd": "decode", "bytes": local.encoded }));
    if decoded["text"] != serde_json::json!(local.text) {
        return Err(format!(
            "oracle decodes the copied engine's encoding differently: {decoded}"
        ));
    }
    // Merge order must not matter, for either engine, with either engine's bytes.
    let half = Script {
        agents: script.agents,
        ops: script.ops[..script.ops.len() / 2].to_vec(),
    };
    let prefix = run(&half)?;
    for chunks in [
        vec![prefix.encoded.clone(), local.encoded.clone()],
        vec![local.encoded.clone(), prefix.encoded.clone()],
        vec![remote.encoded.clone(), prefix.encoded.clone()],
    ] {
        let merged = merge_text(&chunks)?;
        if merged != local.text {
            return Err(format!(
                "merge order changed the copied engine's result: {merged:?}"
            ));
        }
        let theirs = oracle.ask(serde_json::json!({ "cmd": "merge", "chunks": chunks }));
        if theirs["text"] != serde_json::json!(local.text) {
            return Err(format!("merge order changed the oracle's result: {theirs}"));
        }
    }
    Ok(())
}

#[test]
#[ignore = "needs the upstream oracle binary; run node scripts/run-engine-oracle.mjs"]
fn copied_engine_agrees_with_upstream_on_seeded_schedules() {
    let seeds: u64 = std::env::var("BRAID_ORACLE_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(60);
    let only: Option<u64> = std::env::var("BRAID_ORACLE_SEED")
        .ok()
        .and_then(|v| v.parse().ok());
    let mut oracle = Oracle::start();
    let mut checked = 0;
    let mut operations = 0;
    let (mut merges, mut concurrent, mut deletes) = (0usize, 0usize, 0usize);
    for agents in [2usize, 3, 10] {
        for seed in only.map_or(0..seeds, |s| s..s + 1) {
            let script = generate(seed, agents, 40 + (seed as usize % 5) * 25);
            // Panics in the copied engine are failures with a saved script, not aborts.
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                check_script(&mut oracle, &script)
            }))
            .unwrap_or_else(|_| Err("copied engine panicked".into()));
            if let Err(message) = outcome {
                let directory = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../../target/oracle-failures");
                std::fs::create_dir_all(&directory).unwrap();
                let file = directory.join(format!("agents{agents}-seed{seed}.json"));
                std::fs::write(&file, serde_json::to_vec_pretty(&script).unwrap()).unwrap();
                panic!(
                    "agents={agents} seed={seed}: {message}\nscript saved to {}",
                    file.display()
                );
            }
            checked += 1;
            operations += script.ops.len();
            let mut seen = std::collections::HashSet::new();
            for op in &script.ops {
                merges += usize::from(op.parents.len() > 1);
                // A second operation on the same parents is concurrent with the first.
                concurrent += usize::from(!seen.insert(op.parents.clone()));
                deletes += usize::from(op.insert.is_none());
            }
        }
    }
    // Agreement is only meaningful if the schedules really exercise concurrency.
    assert!(
        merges > 0 && concurrent > operations / 20 && deletes > operations / 10,
        "schedules were too sequential: merges={merges} concurrent={concurrent} deletes={deletes}"
    );
    println!(
        "oracle agreement: {checked} schedules, {operations} operations ({merges} merge points, {concurrent} concurrent siblings, {deletes} deletes; 2, 3 and 10 replicas)"
    );
}

/// Always-on: the generator itself is deterministic and produces valid frontiers, so a
/// saved seed means the same script on every machine.
#[test]
fn schedule_generator_is_deterministic_and_frontiers_are_valid() {
    for agents in [2usize, 3, 10] {
        let a = generate(7, agents, 60);
        let b = generate(7, agents, 60);
        assert_eq!(
            serde_json::to_string(&a).unwrap(),
            serde_json::to_string(&b).unwrap()
        );
        assert_ne!(
            serde_json::to_string(&a).unwrap(),
            serde_json::to_string(&generate(8, agents, 60)).unwrap()
        );
        // The copied engine accepts every generated script and keeps its own invariants.
        let log = build(&a).unwrap();
        assert!(
            log.len() >= a.ops.len(),
            "every operation occupies at least one item"
        );
    }
}
