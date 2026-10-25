# Copied CRDT engine: provenance investigation

**Status: partially established. The exact origin is not recoverable from this
repository; the closest upstream revision and the extent of local changes are
measured. Treat all of it as evidence to investigate, not proof of origin.**

`crates/braid-core/src/vendor/{diamond_types,rle}` (105 Rust files, about 26,900
lines) was added in commit `4f5275b` (2026-10-23) with no source revision, patch set
or license notice recorded. This investigation compares it with the upstream
[diamond-types](https://github.com/josephg/diamond-types) history (1,295 commits
cloned; 714 touch the engine). The machine-readable per-file result is
[engine-provenance.json](engine-provenance.json); the method is stored in that file.

## Findings

| Question | Result |
| --- | --- |
| Byte-identical to any upstream blob in history? | Only `storage/file.rs` and `rle/mod.rs`'s upstream `lib.rs` counterpart; 2 of 106 files |
| Same file layout as upstream? | Yes. At the closest sampled revision every local file except two local `mod.rs` entry points exists upstream at the corresponding path; upstream's two `lib.rs` entry points were not copied |
| Closest sampled upstream revision | `90227908e8271786c49d30f10ef0e41ede207109` (committer date 2024-10-01, "Added SerializedOpsOwned"). 40 evenly spaced commits and the neighbourhood of the minimum were scored, so a closer unsampled revision is possible |
| Size of local differences at that revision | 1 identical file; 102 files differ by 2,938 changed lines in total (about 11% of the code); 52 of those differ by 10 lines or fewer; 4 differ by more than 100 lines |
| Largest local changes | `listmerge/merge.rs`, `causalgraph/graph/tools.rs`, `frontier.rs`, `causalgraph/storage.rs` |
| Upstream HEAD examined | `89ae3a0ab8d941a2885e6ec89d094bc9ce9d2922` is diamond-types 2.0.0 (requires Rust 1.96); the copy is older and API-different, so a drop-in replacement is not established |

Reformatting does not explain the differences: running rustfmt over the upstream
files increased, rather than decreased, the distance, because the copy keeps
upstream's formatting. The numbers therefore reflect real edits plus any
additional upstream churn between the unknown source revision and the sampled one.

## License status (compliance gap)

Upstream is ISC (`rle`: ISC OR Apache-2.0). This repository is MIT OR Apache-2.0
and `deny.toml` allows ISC for dependencies, but the vendored directory carried no
attribution. [NOTICE.md](../../crates/braid-core/src/vendor/NOTICE.md) now records the
attribution and the missing piece: the upstream snapshot examined has no license file
or copyright line for the engine crate, and none has been invented. Obtaining that
line from the upstream author is an open action.

## What this does and does not unblock

- **INT-PROV-1** moves from "origin unestablished" to "closest revision and local
  delta measured"; it does **not** close. A reproducible patch set against a pinned
  upstream revision does not exist and cannot be reconstructed with confidence.
- **INT-PROV-2** (historical checkpoint corpus and an independently pinned oracle)
  remains open. The sensible next step is to check out the identified revision as the
  oracle, generate checkpoints with it, and compare decoding, checkout and merge
  behavior with the copied engine; until that passes, the engine storage format stays
  frozen.
- Panics and assertions inside the copied engine remain unaudited (first-party lint
  restrictions do not apply to it); see the baseline lexical inventory.

## Reproduce

```
git clone https://github.com/josephg/diamond-types.git <empty dir>
```

then compare each local file with the same path at the revision above (CRLF
normalized, whitespace-sensitive line diff), as recorded in the JSON's `method`.
