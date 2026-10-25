// Independent pinned JS Simpleton client against the production Rust server and
// Rust client. The fixture gates two PUTs to force the same historical base.
import { spawn } from 'node:child_process';
import { createRequire } from 'node:module';
import { readFileSync, mkdirSync, mkdtempSync, writeFileSync } from 'node:fs';
import { once } from 'node:events';
import { createInterface } from 'node:readline';
import { webcrypto } from 'node:crypto';
import vm from 'node:vm';
import path from 'node:path';
import assert from 'node:assert/strict';
import { checkoutUpstream } from '../../scripts/upstream-checkout.mjs';

const root = path.resolve(import.meta.dirname, '../..');
process.chdir(root);
const httpRevision = '639b811ec91e5d1aae4a4a7d162616b499f95b07';
const textRevision = '1b50ed357dcbbf59a1f4554690f69d0458bc967e';
const http = checkoutUpstream('target/interoperability/braid-http', 'https://github.com/braid-org/braid-http.git', httpRevision);
const text = checkoutUpstream('target/interoperability/braid-text', 'https://github.com/braid-org/braid-text.git', textRevision);
const require = createRequire(import.meta.url);
const clientModule = require(path.join(http, 'braid-http-client.js'));
globalThis.reliable_update_channel = clientModule.reliable_update_channel;
if (!globalThis.crypto) globalThis.crypto = webcrypto;
vm.runInThisContext(readFileSync(path.join(text, 'client/text-client.js'), 'utf8'), { filename: 'pinned-text-client.js' });
vm.runInThisContext(readFileSync(path.join(text, 'client/simpleton.js'), 'utf8'), { filename: 'pinned-simpleton.js' });

mkdirSync('target/interoperability', { recursive: true });
const directory = mkdtempSync(path.resolve('target/interoperability/simpleton-server-'));
const executable = path.resolve('target/debug/examples/simpleton_interop_server' + (process.platform === 'win32' ? '.exe' : ''));
const server = spawn(executable, [directory], { stdio: ['ignore', 'pipe', 'pipe'], windowsHide: true });
let stderr = '';
server.stderr.on('data', bytes => { stderr += bytes; });
const lines = createInterface({ input: server.stdout });
let client;
try {
  const address = await Promise.race([
    once(lines, 'line').then(([line]) => JSON.parse(line)),
    once(server, 'error').then(([error]) => { throw error; }),
    once(server, 'exit').then(([code]) => { throw Error(`Rust server exited ${code}: ${stderr}`); }),
    new Promise((_, reject) => { const timer = setTimeout(() => reject(Error('Rust fixture startup timed out')), 20000); timer.unref(); }),
  ]);
  const origin = new URL(address.url).origin;
  async function request(route, options = {}) {
    const response = await fetch(origin + route, { ...options, signal: AbortSignal.timeout(20000) });
    assert.equal(response.status, 200, `${route}: ${await response.clone().text()}`);
    return response;
  }
  async function poll(check, label) {
    const end = Date.now() + 20000;
    while (Date.now() < end) {
      if (await check()) return;
      await new Promise(resolve => setTimeout(resolve, 10));
    }
    throw Error(`Timed out: ${label}; ${stderr}`);
  }
  let state = '', initialized = false, online = false, acked = false;
  const errors = [];
  client = globalThis.simpleton_client(address.url, {
    send_digests: true,
    on_update(update) { state = update.state; initialized = true; return state; },
    on_online(value) { online = value; },
    on_ack() { acked = true; },
    on_error(error) { errors.push(String(error)); },
  });
  await poll(async () => initialized && online && (await (await request('/rust')).json()).initialized, 'both clients initialized');
  assert.equal(state, 'a\u{1f600}b');
  await request('/gate/pause', { method: 'POST' });
  state = 'aX\u{1f600}b'; acked = false;
  client.changed(state);
  await request('/rust', { method: 'POST', body: 'a\u{1f600}Yb' });
  await poll(async () => (await (await request('/state')).json()).pending === 2, 'two concurrent PUTs captured');
  await request('/gate/resume', { method: 'POST' });
  await poll(async () => {
    const rust = await (await request('/rust')).json();
    assert.equal(rust.error, null);
    return state === 'aX\u{1f600}Yb' && rust.text === state && rust.outstanding === 0 && acked;
  }, 'JS/Rust convergence after a lost acknowledgment');
  const committed = await (await request('/state')).json();
  assert.equal(committed.events, 3, 'retry must retain its immutable identity');
  const historical = await request('/doc', { headers: { Version: '"seed"' } });
  assert.equal(historical.headers.get('version'), '"seed"');
  assert.equal(await historical.text(), 'a\u{1f600}b');
  state = ''; acked = false; client.changed(state);
  await poll(async () => acked && (await (await request('/rust')).json()).text === '', 'empty replacement propagates');
  assert.equal(errors.length, 0, JSON.stringify(errors));
  const report = { httpRevision, textRevision, passed: ['concurrent Unicode edits', 'lost acknowledgment', 'idempotent retry', 'historical snapshot', 'empty replacement'] };
  writeFileSync('target/interoperability/simpleton-server-report.json', JSON.stringify(report, null, 2) + '\n');
  console.log('Pinned JS client and production Rust client converge through the Rust Simpleton server: 5 scenarios passed');
} finally {
  client?.abort();
  lines.close();
  if (server.pid && server.exitCode === null) { const exited = once(server, 'exit'); server.kill(); await exited; }
}
