# Upstream compatibility, checked 2026-10-23

Primary source implementations were inspected, not just package READMEs. Branch
heads below are source observations and are not silently treated as equivalent
to published releases. Registry Cargo.lock checksums pin the versions actually
compiled. The differential test verifies its source checkout SHA before use.

## Braid/spec reference revisions

| Source | Exact observed SHA | Role |
| --- | --- | --- |
| [braid-spec](https://github.com/braid-org/braid-spec/tree/0541922135bea0d60ad6b539e8d1ec25c2527284) | `0541922135bea0d60ad6b539e8d1ec25c2527284` | BHTTP-04, Versions-05, Range-Patch-01, Merge-Types-00 normative profile |
| [braid-http](https://github.com/braid-org/braid-http/tree/639b811ec91e5d1aae4a4a7d162616b499f95b07) | `639b811ec91e5d1aae4a4a7d162616b499f95b07` | source-level bidirectional codec fixtures |
| [braid-text](https://github.com/braid-org/braid-text/tree/1b50ed357dcbbf59a1f4554690f69d0458bc967e) | `1b50ed357dcbbf59a1f4554690f69d0458bc967e` | Simpleton scalar/UTF-16 coordinate and channel reference |
| [braid-fuzz](https://github.com/braid-org/braid-fuzz/tree/b79c23f877d2e6807e3925bc0165a4da139a4458) | `b79c23f877d2e6807e3925bc0165a4da139a4458` | independent controller contract and HTTP/subscription tests |
| [braidfs](https://github.com/braid-org/braidfs/tree/c54447f09016f9934ead1cb4bf277a945e5093fb) | `c54447f09016f9934ead1cb4bf277a945e5093fb` | filesystem reference; not a guarantee of parity |
| [iroh](https://github.com/n0-computer/iroh/tree/cf0d2b048b625e87e9331dd3b9f921f4c0d8bf71) | `cf0d2b048b625e87e9331dd3b9f921f4c0d8bf71` | source/API compatibility investigation |
| [iroh-gossip](https://github.com/n0-computer/iroh-gossip/tree/cf859543942b017d9e7034ecfa4aa61bfcf291e7) | `cf859543942b017d9e7034ecfa4aa61bfcf291e7` | source/API compatibility investigation |
| [iroh-h3](https://github.com/Le-Maz/iroh-h3/tree/9917d5c382810396f0d1e4cebfadf0ca788db100) | `9917d5c382810396f0d1e4cebfadf0ca788db100` | source/API compatibility investigation |
| [diamond-types](https://github.com/josephg/diamond-types/tree/89ae3a0ab8d941a2885e6ec89d094bc9ce9d2922) | `89ae3a0ab8d941a2885e6ec89d094bc9ce9d2922` | current comparison source; copied engine provenance not established |
| [h3](https://github.com/hyperium/h3/tree/43745ad87e95b0c035ac9c547aad467303213425) | `43745ad87e95b0c035ac9c547aad467303213425` | source/API compatibility investigation |

## Published compatible transport stack

| Package | Selected release | Published UTC | Rust declaration |
| --- | --- | --- | --- |
| [iroh](https://crates.io/crates/iroh/1.4.1) | 1.4.1 | 2026-10-21T17:08:50.039731Z | 1.91 |
| [iroh-gossip](https://crates.io/crates/iroh-gossip/0.101.0) | 0.101.0 | 2026-06-15T13:41:18.455860Z | 1.91 |
| [iroh-h3](https://crates.io/crates/iroh-h3/0.3.0) | 0.3.0 | 2026-06-29T18:15:44.054032Z | not declared |
| [iroh-h3-axum](https://crates.io/crates/iroh-h3-axum/0.4.0) | 0.4.0 | 2026-06-29T18:15:45.730443Z | not declared |
| [iroh-h3-client](https://crates.io/crates/iroh-h3-client/0.5.0) | 0.5.0 | 2026-06-29T18:15:46.729982Z | not declared |
| [h3](https://crates.io/crates/h3/0.0.8) | 0.0.8 | 2025-05-06T15:37:32.883527Z | 1.70 |

Iroh 1.4.1 requires Rust 1.91. The validated local toolchain is 1.99.0; a 1.91
build has not been established as the complete workspace MSRV. Endpoint builder
presets and AddressLookupBuilder replace the 0.96 APIs. Gossip 0.101's spawn API
is synchronous. H3 Axum 0.4 inserts an authenticated RemoteId extracted by the
resource policy. H3 client 0.5 preserves streaming responses. Local H3 tests use
Minimal endpoints and shared in-memory address lookup, so relay/NAT traversal
remains a distinct unverified test obligation.

The replaced path dependencies were compared to available registry archives.
[Vendor provenance](audit/vendor-provenance.json) records hashes of every old
file and the baseline commit where all source remains recoverable. Gossip's
single source patch expires received_messages: 0.101.0 already contains it.
The old H3 source patches adapt read_chunk and removed error variants: 0.3.0
already implements the new Iroh API. H3 client differs only in its manifest.
The declared old H3 Axum 0.2.0 archive returned HTTP 403; its exact published
provenance remains unresolved. New Axum 0.4 was read and exercised by actual GET,
PUT, persistent subscriptions and peer revocation tests before replacement.

The copied Diamond engine remains isolated under braid-core/src/vendor. Do not
claim it is byte-for-byte upstream HEAD. Its adapter adds a narrow historical
text checkout using existing TextInfo::merge_into because generic checkout_at_version
is unfinished upstream. This local patch is covered by opposite-order concurrent
edits and restart tests; remaining generic-map/list APIs retain upstream assertions
and are not covered by the public replica guarantee. A full migration to a
maintained pinned engine and old binary checkpoint corpus is outstanding. The copied causal-storage test
now uses a temporary directory instead of leaving test.cg in the workspace;
this changes only test isolation.

## Protocol decisions and discrepancies

Versions-05 permits structured strings/display strings, not arbitrary SFV tokens,
numbers, nested lists or parameters. Rust implements that stricter grammar and
canonical frontier ordering. Current JS source still parses versions with
JSON.parse('[' + header + ']') and uses JSON escapes for non-ASCII IDs. These
behaviors differ: the shared JS golden fixture subset deliberately uses ASCII
IDs while payloads include Unicode/binary data. Rust-only display-string tests
are a normative test, not proof that JS accepts that syntax.

Numeric Patches and per-patch byte lengths are emitted, including for a single
patch. Incoming single Content-Range and numeric multi-patch forms are accepted.
Rust now labels multi-patch entities application/http-patches; representation
media type is carried as Repr-Type when supplied. HTTP transfer coding is owned
by HTTP/H3 libraries and is never parsed as application patch framing.

The older braid-fuzz runner pins braid-http 1.3.125 and braid-text 0.5.3 independently
from the new source-level codec harness at braid-http SHA 639b811. Do not collapse
these distinct tests into one version claim. Upstream reliable tests 14/15 need
Simpleton controller support; HTTP-only controller success does not establish
coordinated Simpleton convergence.

## Continuation source and dependency decisions

The nine recorded upstream HEADs were rechecked on 2026-10-23 and remained at
these pins. The complete modern Simpleton/text-client sources and reliable
GET/PUT channel implementation were read alongside their runner contract. The
new core example drives the production Rust client for all text suites. The
pinned fuzz runner installs braid-http 1.3.125 and braid-text 0.5.3 explicitly;
those published versions are distinct from the modern source pins above.
The committed independent runner npm graph uses a compatible send 0.19.0
override for its HTTP bridge's XSS advisory; runner algorithms and protocol
library versions are unchanged. All four suites pass with this locked graph.

The client retains a stable peer and scalar counter, ignores updates whose
parents are not its current view, bounds outstanding PUTs, stores offline text,
and validates representation digests. Reconnect uses the oldest queued PUT's
parents while its ACK is uncertain; resuming at its optimistic ID incorrectly
produced HTTP 409 before the server had received it. The partitioned NFS test
reproduced and guards this boundary. The Rust Simpleton server still rejects
stale positional writes instead of claiming transforming-server compatibility.

Maintained compact_str 0.10.0 replaces smartstring in thirteen copied Diamond
modules. Inline capacity/allocation behavior can differ; serialized strings
remain strings and copied algorithm regressions are run. Agent names must be
strictly shorter than 50 bytes and cannot be ROOT; SHA-256 encoded IDs bound
untrusted longer names. A name of exactly 50 bytes previously reached a copied
engine assertion and was reproduced by the shared filesystem integration test.

The narrow netlink 0.8.1 patch replaces only its macro dependency, as recorded in
[vendor/README.md](../vendor/README.md). Netlink 0.9.0's upstream source SHA is
4652ced0cd685430a3df0207a50a95445d3b9aa5, and comparison HEAD was
4b49dd4548b43ab744962cd31236aa15cb1cde20. Its incompatible removed macro API
precludes an isolated transitive upgrade. No advisory suppression is used.

The mounted Linux follow-up required an archive-pinned nfsserve 0.10.2 patch.
Its raw READDIR handler discarded the supplied cookie, despite the adapter's
correct pagination. Both directory procedures also subtracted framing overhead
from unchecked small byte counts. Registry 0.11.0 (source
 af4f1176dd709847f3dda4f1a7cd093edaadde63) was read and retains both issues.
A raw TCP RPC test reproduces duplicate pages before the patch and checks
both zero-budget responses afterward. Version 0.10.2 has no source SHA metadata
in its archive; its exact checksum and changes are recorded in vendor/nfsserve,
with a compatible num-derive 0.4 manifest update following 0.11's usage.
