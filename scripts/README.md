# Workspace scripts

Run commands from the repository root.

```sh
cargo build -p braid-http --locked --example fuzz_controller
node scripts/run-upstream-conformance.mjs
```

The conformance runner needs Git, Node.js and npm. It downloads a pinned upstream
harness and writes its checkout, dependencies and reports under `target/audit/`.
Use `--reliability` to include the additional reliable-update suite; see the
[audit status](../docs/audits/AUDIT_IMPLEMENTATION_STATUS.md) for known failures.

The [legacy directory](legacy/) preserves earlier repository generation and
publishing scripts. They are outside the current workspace workflow.
