// Actual daemon IPC and HTTP channels. Fixture values are synthetic credentials.
import { spawn } from 'node:child_process';
import { createServer } from 'node:http';
import { createHash } from 'node:crypto';
import { mkdtemp, mkdir, readFile, writeFile, rm, rename, rmdir } from 'node:fs/promises';
import path from 'node:path';
import assert from 'node:assert/strict';
import { once } from 'node:events';

const base = path.resolve('target');
await mkdir(base, { recursive: true });
const root = await mkdtemp(path.join(base, 'credential-fixture-'));
const relative = path.relative(base, root);
assert(relative && !relative.startsWith('..') && !path.isAbsolute(relative));
const requests = [];
let expected = 'fixture-private-token-one';
const servers = [];
const sockets = new Set();
let daemon;
let output = '';
const pause = ms => new Promise(resolve => setTimeout(resolve, ms));
async function until(check, label) {
  const deadline = Date.now() + 30000;
  while (Date.now() < deadline) {
    if (daemon?.exitCode !== null && daemon?.exitCode !== undefined) throw new Error(`Daemon exited: ${output.slice(-2000)}`);
    try { if (await check()) return; } catch (error) { if (error.name === 'AssertionError') throw error; }
    await pause(30);
  }
  throw new Error(`Timed out: ${label}; ${output.slice(-2000)}`);
}
async function fixture(name) {
  const server = createServer((req, res) => {
    requests.push({ name, authorization: req.headers.authorization });
    if (name === 'other' || req.headers.authorization !== `Bearer ${expected}`) {
      res.writeHead(401); res.end(); return;
    }
    if (req.method === 'GET') {
      res.writeHead(209, { Subscribe: 'true', Heartbeats: '0.1', 'Merge-Type': 'simpleton' });
      res.write('Version: "seed"\r\nParents: \r\nContent-Type: text/plain\r\nContent-Length: 4\r\n\r\nbase\r\n\r\n');
      const timer = setInterval(() => res.write('\r\n'), 100);
      res.on('close', () => clearInterval(timer));
    } else { req.resume(); req.on('end', () => { res.writeHead(200); res.end(); }); }
  });
  server.on('connection', socket => { sockets.add(socket); socket.on('close', () => sockets.delete(socket)); });
  server.listen(0, '127.0.0.1'); await once(server, 'listening'); servers.push(server);
  return `http://127.0.0.1:${server.address().port}`;
}
try {
  const origin = await fixture('authorized');
  const other = await fixture('other');
  const reservation = createServer(); reservation.listen(0, '127.0.0.1'); await once(reservation, 'listening');
  const port = reservation.address().port; await new Promise(resolve => reservation.close(resolve));
  await mkdir(path.join(root, '.braidfs'));
  const configPath = path.join(root, '.braidfs', 'config');
  await writeFile(configPath, JSON.stringify({ schema_version: 2, peer_id: 'credential-fixture', sync: {}, cookies: {}, origin_credentials: {} }));
  const binary = path.resolve('target/debug', process.platform === 'win32' ? 'braidfs-daemon.exe' : 'braidfs-daemon');
  daemon = spawn(binary, ['--port', String(port)], { env: { ...process.env, BRAID_ROOT: root }, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'] });
  const collect = chunk => { output += chunk.toString(); if (output.length > 2 * 1024 * 1024) output = output.slice(-1024 * 1024); };
  daemon.stdout.on('data', collect); daemon.stderr.on('data', collect);
  daemon.on('error', error => { output += error.message; });
  const ipc = `http://127.0.0.1:${port}`;
  await until(async () => (await fetch(`${ipc}/.braidfs/config`)).ok, 'daemon ready');
  async function put(route, value) {
    const response = await fetch(`${ipc}${route}`, { method: 'PUT', headers: { 'content-type': 'application/json' }, body: JSON.stringify(value) });
    assert.equal(response.status, 200, `${route}: ${await response.clone().text()}`);
    return response.json();
  }
  assert.equal((await put('/api/credentials', { origin, credentials: { bearer_token: expected } })).status, 'ok');
  const saved = JSON.parse(await readFile(configPath, 'utf8'));
  assert.equal(saved.origin_credentials[origin].bearer_token, expected, 'IPC ACK must follow persistence');
  const legacy = await put('/api/cookie', { domain: 'legacy.example.test', value: 'fixture-cookie' });
  assert.equal(legacy.domain, 'legacy.example.test');
  const url = `${origin}/doc`;
  await put('/api/sync', { url });
  await until(() => requests.some(request => request.name === 'authorized' && request.authorization === `Bearer ${expected}`), 'authenticated GET');
  const journal = path.join(root, '.braidfs/channels', `${createHash('sha256').update(url).digest('hex')}.json`);
  await until(async () => JSON.parse(await readFile(journal, 'utf8')).application?.initialized, 'durable initial view');
  const visible = await (await fetch(`${ipc}/.braidfs/config`)).text();
  assert(!visible.includes(expected));
  assert(!(await readFile(journal, 'utf8')).includes(expected));
  await put('/api/sync', { url: `${other}/doc` });
  await until(() => requests.some(request => request.name === 'other'), 'other origin GET');
  assert(requests.filter(request => request.name === 'other').every(request => request.authorization === undefined));
  expected = 'fixture-private-token-two';
  await put('/api/credentials', { origin, credentials: { bearer_token: expected } });
  await until(() => requests.some(request => request.name === 'authorized' && request.authorization === `Bearer ${expected}`), 'rotated channel request');
  // A failed configuration publication must not update the live credential map
  // or produce a successful persistence acknowledgment.
  const savedConfig = `${configPath}.fixture-saved`;
  assert(path.dirname(savedConfig) === path.join(root, '.braidfs'));
  await rename(configPath, savedConfig); await mkdir(configPath);
  const rejected = await fetch(`${ipc}/api/credentials`, { method: 'PUT', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ origin: other, credentials: { bearer_token: 'fixture-private-token-rejected' } }) });
  assert.equal(rejected.status, 500);
  const live = await (await fetch(`${ipc}/.braidfs/config`)).json();
  assert.equal(live.origin_credentials[other], undefined);
  await rmdir(configPath); await rename(savedConfig, configPath);
  for (const secret of ['fixture-private-token-one', expected, 'fixture-private-token-rejected', 'fixture-cookie']) assert(!output.includes(secret), 'logs must not contain credential values');
  console.log('Daemon origin credentials: persistence, scoped transport, journal isolation, rotation, failed-publication atomicity and redacted status passed');
} finally {
  if (daemon?.pid && daemon.exitCode === null) { daemon.kill(); await once(daemon, 'exit'); }
  for (const socket of sockets) socket.destroy();
  await Promise.all(servers.map(server => new Promise(resolve => server.close(resolve))));
  const checked = path.relative(base, path.resolve(root));
  assert(checked && !checked.startsWith('..') && !path.isAbsolute(checked) && path.basename(root).startsWith('credential-fixture-'));
  await rm(root, { recursive: true, force: true });
}
