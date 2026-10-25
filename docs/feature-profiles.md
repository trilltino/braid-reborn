# Supported feature checks

Run `node scripts/check-feature-profiles.mjs` from the repository root. It runs
each profile separately to prevent workspace feature unification from supplying
an undeclared dependency. Each profile runs library Clippy with warnings denied
and library tests whose crate roots deny Rust warnings. Actual route, transport,
subprocess and interoperability tests remain part of the workspace gates.

| Package | Individual profiles |
| --- | --- |
| braid-http | minimal, native |
| braid-core | minimal, client, native, server, blob, fs, nfs, engine, compressed-engine |
| braid-iroh | minimal, proxy |

`minimal` disables default features. `engine` enables `serde,smallvec` and
`compressed-engine` additionally enables `lz4`. Other names select the feature of
the same name. To repeat a subset, pass exact keys, for example
`node scripts/check-feature-profiles.mjs braid-core/fs braid-core/nfs`.
The generated per-command results are in `target/feature-profiles/report.json`.

The fs-only profile reproduced missing server/Axum/client dependencies that the
workspace build had hidden. The `fs` feature now declares its existing server
and client requirements. `nfs` enables `fs`, so enabling NFS alone actually
exposes its adapter. Native-only private Diamond wire-adapter methods are gated
with their native callers rather than leaving dead methods in the engine profile.
No registry package or lockfile version changes are required for these repairs.

Windows and Linux CI also run default and all-feature workspace tests plus
all-target/all-feature Clippy. WASM is a distinct compile check:

```text
cargo check -p braid-http --locked --target wasm32-unknown-unknown --no-default-features --features wasm
```

These checks establish only their named target/profile combinations. They do not
certify browser runtime persistence, Iroh relays, mounted NFS behavior on Windows,
or arbitrary combinations of reserved compatibility features.
