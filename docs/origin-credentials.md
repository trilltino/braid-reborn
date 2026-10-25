# Origin-scoped daemon and channel credentials

Credentials belong to an exact HTTP origin: scheme, canonical host and port.
`https://example.test`, `http://example.test`, and
`https://example.test:8443` are different scopes. Paths, query strings, fragments
and URL userinfo are rejected as credential scopes. Userinfo is also rejected
before a resource URL enters a request or channel journal.

The daemon stores schema-version-2 configuration in `.braidfs/config`. Credentials
stay in this local configuration; they are not copied into events, outboxes,
recovery application state or status responses. Unix writes use private modes;
Windows inherits the directory ACL. Debug output and configuration diagnostics
redact credential values. Internal `.braidfs` files, including backups, cannot be
scanner upload candidates even when ignore patterns are changed.

An origin entry has this shape:

```json
{
  "origin_credentials": {
    "http://127.0.0.1:3001": { "bearer_token": "your-session-token" },
    "https://example.test": { "cookie": "client=your-cookie" }
  }
}
```

The local daemon accepts `PUT /api/credentials` with
`{"origin":"http://127.0.0.1:3001","credentials":{"bearer_token":"your-session-token"}}`.
HTTP 200 means the configuration was persisted. Storage errors do not publish the
candidate to memory. HTTP 202 means persistence is still unknown; it does not
acknowledge a completed change. A successful update drains/restarts affected text
channels using their existing journals, preserving uncertain PUT identities.
Other origins keep their own credentials. Status reports configured origin names
and redacted values, never the secrets themselves.

The authenticated application bridge is `POST /config/credentials`, with the same
body. It requires Manage authority on the separate `control/daemon` policy.
This policy is assigned explicitly by a local administrator; the first incoming
request cannot claim it. Reader/editor grants do not confer settings ownership.
The bridge uses direct loopback HTTP with proxying and redirects disabled, and
never forwards the app login Authorization header as daemon authentication.

The old `/config/cookie` and daemon `/api/cookie` endpoints remain adapters. Domain
cookies map to HTTPS/default-port only. Existing legacy `cookies` configuration
is retained with that same scope; non-default ports and HTTP require explicit
origin entries. The old app response retains its status/domain fields, including
an explicit pending response when persistence is unconfirmed.

Before upgrading a populated config, stop the daemon and use the local commands:

```text
braid-admin config-preview PATH_TO_CONFIG
braid-admin config-migrate PATH_TO_CONFIG
braid-admin config-migrate PATH_TO_CONFIG --apply
braid-admin config-rollback PATH_TO_CONFIG
braid-admin config-rollback PATH_TO_CONFIG --apply
```

Without `--apply`, commands are read-only. Applying migration preserves the exact
old bytes in `config.v1.backup`, writes a versioned hash record, validates the
source before publication, and supports resume. Explicit migration/rollback
refuses a running daemon's writer lease. Rollback verifies both backup and current
hashes and refuses subsequent configuration changes. Unknown configuration fields
survive migration. Keep a full data backup for rollback after application/data
writes; config rollback is not a data rollback. Old binaries do not preserve new
credential fields on writes, so mixed-version writers remain unsupported.

Rust clients can use `ScopedCredentials`, `OriginCredentials` and
`BraidClient::reliable_channel_authenticated` without changing old channel APIs.
Default native clients do not follow redirects: a 303 must not turn a queued PUT
into an apparently successful GET. `with_client_builder` preserves custom builder
settings while applying this redirect rule. The older `with_client` constructor
retains the caller-owned client's redirect/default-header policy as an explicit
transport boundary.

Browser Fetch uses its same-origin ambient-cookie policy and reports a typed
capability error for explicit Cookie headers; use bearer authentication or the
browser's own origin cookie handling. Browser runtime redirects, persistence,
quota behavior and cancellation still require the portable-channel milestone.

Executable evidence is in `origin_credentials`, `origin_configuration`,
`reliable_channel`, `release_authorization` and the actual daemon fixture
`node tests/interoperability/credentials.mjs` (build braidfs-daemon first).
