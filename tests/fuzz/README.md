# Coverage-guided boundary fuzzing

This separate cargo-fuzz workspace keeps libFuzzer and nightly instrumentation
out of shipped crates. Its lockfile is committed independently. From this directory:

```sh
cargo +nightly-2026-10-22 fuzz run --fuzz-dir . wire -- -max_total_time=60 -max_len=65536 -rss_limit_mb=512 -timeout=5
cargo +nightly-2026-10-22 fuzz run --fuzz-dir . text -- -max_total_time=60 -max_len=65536 -rss_limit_mb=512 -timeout=5
```

`wire` compares fragmented and whole-input decoding, finite EOF, typed conversion
and canonical re-encoding with bounded parser limits. `text` checks scalar range
replacement against an independent character-vector model. CI retains artifacts
and corpora, including failure seeds. Time-bounded smoke runs do not establish
exhaustive safety. Vendored binary checkpoint ingestion and multi-peer runtime
state machines still need separate campaigns.

The recorded Linux CI run at code commit 5ab2e55 passed both AddressSanitizer
campaigns: wire 1,274,253 executions and text 2,189,993 executions, 61 seconds
each. See [engineering status](../../docs/audit/engineering-status.md) for the
run link and remaining coverage boundaries. The job sets RUSTUP_TOOLCHAIN
explicitly because the root rust-toolchain.toml otherwise selects stable.
