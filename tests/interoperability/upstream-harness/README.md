# Independent runner dependency lock

The runner source stays at braid-fuzz
`b79c23f877d2e6807e3925bc0165a4da139a4458`. The conformance script copies these
two npm files into its ignored source checkout before `npm ci --ignore-scripts`.
All four direct testing dependencies are exact, with transitive integrity hashes.
The protocol implementations remain braid-http 1.3.125 and braid-text 0.5.3.

The http2-express-bridge override selects compatible send 0.19.0 to fix
[GHSA-m6fv-jmcg-4jfg](https://github.com/advisories/GHSA-m6fv-jmcg-4jfg).
This changes testing dependencies, not the independent runner's algorithms or
the shipped Rust graph. npm audit runs in conformance and security CI.
