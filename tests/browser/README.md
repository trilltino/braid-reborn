# Browser channel validation

This directory contains two different levels of evidence:

- `node --test tests/browser/runtime-source.mjs` checks the JavaScript adapter's
  ownership and error contracts using controlled IndexedDB/Web Lock doubles. It
  does not compile Rust or establish browser behavior.
- `node tests/browser/run.mjs --no-build` runs the production Rust/WASM journal,
  reliable channel and Simpleton client in Chromium, Firefox and WebKit against
  actual application routes. It requires prebuilt artifacts; `--no-build` fails
  explicitly when artifacts are missing and never invokes Cargo.

The browser runtime suites and new Rust harness/example have **not been compiled
or executed during the current source-only checkpoint**, at the user's request.
Passing JavaScript source fixtures must not close the browser release gate.

When building is authorized, `node tests/browser/build-harness.mjs` builds the
WASM harness plus the native `browser_fixture` example and runs a wasm-bindgen CLI
matching the library version in Cargo.lock. The build script never installs a
tool automatically. Set `WASM_BINDGEN` to an existing matching CLI if needed.
Install the exact Node dependency graph with `npm ci --prefix tests/browser` and
provide the browsers matching the locked Playwright runner before runtime tests.

Use `--engine chromium,firefox,webkit`, `--suite journal,network`, and `--grep`
to select runtime checks. Every omitted engine-specific scenario is counted as a
skip with its reason. In particular, injected `QuotaExceededError` fixtures do
not substitute for actual quota exhaustion in Firefox or WebKit; those cells
remain open until there is real engine evidence.

`JournalHarness` and the offline `ChannelHarness` test joint application/outbox
transactions, restart, Web Locks, quota/abort failures, byte-preserving backups,
schema upgrades and authorization-context isolation. `SimpletonHarness` uses the
same production client as native applications. `FetchHarness` uses the production
transport and codec directly for slow-reader and cancellation scenarios.

The native fixture accepts an empty temporary root, binds loopback, creates two
test-only users and calls `local_link_server::router_with_cors`. The test server
uses an explicit allowed origin; the alternate origin must fail CORS. Its outer
middleware observes subscription lifetime and can lose a response after the
production route commits a PUT. It does not implement a second protocol or
substitute a proxy for the application routes. The fixture's generated login
tokens remain in the runner's memory and must never be uploaded in traces/logs.

Results are written to ignored `tests/browser/dist/results.json`, including the
source revision, OS/architecture, Node and browser versions, and individual
scenario outcomes. `BRAID_BROWSER_FIXTURE` may point to an explicitly prebuilt
native example; it does not cause an automatic build.
