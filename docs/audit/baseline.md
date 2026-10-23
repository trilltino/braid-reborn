# Engineering baseline, 2026-10-23

Baseline branch: `master`; commit `4f5275b9ab83bc3cff1ef91ff068be6539edbfa2`.
The working tree initially contained the two supplied audit/refactor documents.
No application data was migrated. Baseline commands ran on Windows x86_64 MSVC,
Rust `1.99.0 (b940084d7 2026-09-28)`, Cargo `1.99.0 (5f94df478 2026-08-27)`.
This records observations, not an independently established minimum Rust version.

## Actual initial results

| Command | Exit/result | Seconds |
| --- | --- | ---: |
| cargo metadata --locked --format-version 1 | 0 | 2.15 |
| cargo tree --workspace --locked --edges normal,build,dev | 0 | 0.73 |
| cargo check --workspace --all-targets --locked | 101 | 1.49 |
| cargo test --workspace --locked | timeout after 360s | 360.02 |
| cargo clippy --workspace --all-targets --all-features --locked -- -D warnings | 101 | 46.97 |
| cargo fmt --all -- --check | 1 | 3.12 |
| cargo check -p braid-http --no-default-features --locked | 0 | 55.0 |
| cargo check -p braid-core --no-default-features --locked | 101 | 17.46 |

Workspace all-targets checks failed on incorrect server example/test imports and
mixed rand versions in braid-iroh. The full test baseline timed out after 360s;
it is **not** a passing test count. All-features Clippy first failed on missing
`arbitrary` support. Formatting and core minimal features also failed.
HTTP minimal features passed. Earlier audit-focused passes are historical and
do not replace these baseline failures.

Machine logs and full metadata/tree are generated under
`target/engineering/baseline/`; they are intentionally ignored. Source hashes of
all 272 first-party crate files captured before editing are preserved in
[source-inventory.json](source-inventory.json). Source ownership includes a large
copied Diamond engine inside braid-core; distinguish it from maintained adapters.

## Lexical safety inventory

These counts are search matches, not an AST proof of reachable panic sites.
The original classifier stopped at a column-zero test module and can classify
later production items as test code. Comments, strings and macro expansions can
also affect counts. No zero-panic claim follows from these numbers.

| Original category | Pattern | Matches |
| --- | --- | ---: |
| example | `\.unwrap\(` | 1 |
| example | `unwrap_or\w*\(` | 7 |
| production | `\.expect\(` | 4 |
| production | `\.unwrap\(` | 44 |
| production | `\bunsafe\b` | 2 |
| production | `let _ =` | 66 |
| production | `unwrap_or\w*\(` | 152 |
| test | `\.expect\(` | 5 |
| test | `\.unwrap\(` | 157 |
| test | `let _ =` | 2 |
| test | `panic!` | 1 |
| test | `unwrap_or\w*\(` | 1 |
| vendored | `TODO|FIXME` | 177 |
| vendored | `\.expect\(` | 8 |
| vendored | `\.unwrap\(` | 295 |
| vendored | `\bunsafe\b` | 19 |
| vendored | `panic!` | 67 |
| vendored | `todo!` | 16 |
| vendored | `unimplemented!` | 9 |
| vendored | `unwrap_or\w*\(` | 27 |

## Regressions reproduced before repairs

- Four chat durability/path/causal regressions failed before repair; all four
  passed after candidate-state publication and persisted graph changes.
- Concurrent Diamond branch test failed with stale-parent conflict before the
  historical-parent adapter; opposite-order edits and restart now pass.
- A new bounded blob implementation initially overflowed the Windows test
  stack. Heap-backed buffers fixed it; retained tests exercise the same path.

A complete function-size inventory, geiger/udeps analysis, Linux runtime baseline,
and before/after performance measurements remain open. See
[engineering status](engineering-status.md) for checkpoint evidence and gaps.
