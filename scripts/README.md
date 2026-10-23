# Workspace scripts

Run commands from the repository root.

```sh
cargo build -p braid-http --locked --example fuzz_controller
cargo build -p braid-core --locked --example simpleton_controller
node scripts/run-upstream-conformance.mjs
```

The conformance runner needs Git, Node.js and npm. It downloads a pinned upstream
harness and writes its checkout, dependencies and reports under `target/audit/`.
All four suites (HTTP, subscriptions, reliable updates and Simpleton) run by
default. Text suites drive the production Rust Simpleton client. Reports preserve
individual failures and runner source pins. Current results belong in the
[engineering status](../docs/audit/engineering-status.md).

`py scripts/dependency-inventory.py` refreshes the current direct-edge inventory
from locked Cargo metadata, source searches and official crates.io observations.
It does not upgrade dependencies. Coverage-guided targets have their own
[instructions](../tests/fuzz/README.md).

The [legacy directory](legacy/) preserves earlier repository generation and
publishing scripts. They are outside the current workspace workflow.

`upstream-checkout.mjs` verifies repository ownership before mutating any cached
checkout. Invalid or modified caches get a fresh isolated clone; they are not
deleted and Git cannot discover/mutate the parent project. The regression is
`node tests/interoperability/source-checkout.mjs`. Both protocol runners use it.
