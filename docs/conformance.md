# Conformance profile and executable evidence

The normative profile is Braid HTTP draft-04, Versions draft-05, Range-Patch
 draft-01 and Merge-Types draft-00 at the pinned spec revision in
[upstream compatibility](upstream-compatibility.md). This is an implemented subset
of drafts, not a claim of complete IETF standard conformance.

| Requirement / spec area | Rust boundary | Executable evidence / status |
| --- | --- | --- |
| BHTTP updates, snapshot and patch bodies | braid-http protocol::WireCodec | audit_conformance; js_fixtures; upstream HTTP suite |
| Range-Patch numeric Patches and per-patch byte lengths | protocol::formatter; client::MessageParser | binary, empty, single/multiple patches; JS source fixtures at every split |
| Versions-05 IDs as structured strings/display strings | protocol::headers | engineering_wire strict grammar/canonical frontier tests |
| Multiple Parents header lines | headers_from_http | engineering_wire aggregate/reject duplicate framing headers |
| Unicode text addressing by scalar values; transport lengths by bytes | protocol::text; Diamond adapter | audit_replica; engineering_causality; Unicode JS payload fixture |
| Original-coordinate multipatches, validate before mutation | protocol::text; replica | invalid multi-patch and overlap regression cases |
| Real HTTP 209 subscription, multiple updates and heartbeat CRLF | native_network; replica_http | upstream subscriptions; native HTTP test; Iroh transport test |
| Subscribe SF boolean and legacy JS true | parse_subscribe; replica_http | engineering_wire and real Iroh ?1 subscription |
| Strict framing/control/header/body/patch-count limits | client::MessageParser / ParserLimits | engineering_wire, audit_conformance, 2048 seeded mutation cases |
| Explicit finish/EOF detects truncated bodies | MessageParser::finish | audit_conformance and JS fragmentation test |
| Causal identity, idempotent retry, unknown/self-parent rejection | replica::ReplicaDocument | audit_replica, engineering_causality, lost ACK filesystem test |
| Historical-parent Diamond edits retain concurrent heads | DiamondCRDT::apply_wire_event | opposite-order branches plus 24 restart/duplicate schedules |
| Simpleton coordinated current-frontier writes | ReplicaDocument::apply | stale concurrent Simpleton returns conflict with identical stored bytes |
| Persist-before-notify and durable receive/outbox | replica, fs sync | audit_filesystem; engineering_storage; streaming blob faults |
| Authenticated Iroh GET/PUT/209 and active revocation | protocol policy + RemoteId | braid-iroh/tests/transport.rs, local direct connections |
| Chat session sender, catch-all and blob authorization | server auth middleware / handlers | engineering_auth through the actual Router |
| Stable encoded URL/path mapping and collision rejection | fs::mapping / shared session registry | encoded segments round-trip without double-encoding; removed paths keep canonical prefixes; conflicting enabled URLs fail explicitly |
| Malformed room paths/state fail closed | JsonChatStore | engineering_storage; NFS component name unit test |
| JS encoder -> Rust and Rust encoder -> JS | tests/interoperability/codec.mjs | checked-in source-pinned protocol-fixtures; run script for both directions |
| Unknown historical Versions request | no complete cross-adapter implementation | unsupported; do not advertise arbitrary version GET |
| Coordinated native PUT/subscription state machine | ReliableChannel + production Simpleton client + filesystem | reliable_channel, audit_shared_channel, independent reliable-updates and Simpleton suites; browser/H3 channel ownership remains separate |
| Full browser runtime/backpressure/cancellation semantics | wasm_network | compile check only; browser behavioral tests open |
| Linked JSON, Range Query, multiplex network transport | extension modules | incomplete/unsupported; codec presence does not establish deployment support |
| NFS offsets/truncate/rename/remove/restart | fs::nfs adapter/backing + fs::operations | binary/sparse/shell/attribute/inode/cookie unit tests; partitioned rename/restart/lost-ACK/offline-edit/remove integration; privileged Linux NFSv3 mount CI passed; raw TCP READDIR cookie/tiny-budget regression passed |
| Partition/relay repair, ACL lifecycle, malicious peer isolation | Iroh/gossip | local allow/revoke tested; relay and anti-entropy tests open |
| Process death and durable replay | ChannelJournal / SQLite / FS | reliable_channel kills an actual writer process and reopens/replays; exhaustive power-loss and cancellation matrix remains open |

Run the actual tests from the repository root:

```sh
cargo test --workspace --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo check -p braid-core --no-default-features --locked
cargo check -p braid-http --locked --target wasm32-unknown-unknown --no-default-features --features wasm
cargo build -p braid-http --locked --example fuzz_controller
cargo build -p braid-core --locked --example simpleton_controller
node scripts/run-upstream-conformance.mjs
node tests/interoperability/codec.mjs
```

Coverage-guided wire/text targets and pinned nightly CI live in [tests/fuzz](../tests/fuzz/README.md).
The deterministic mutation smoke test is separate from these targets.
The model schedules establish convergence for the enumerated text cases; they
are not an independent formal correctness proof. Current pass/fail evidence and
remaining release criteria belong in audit/engineering-status.md.
