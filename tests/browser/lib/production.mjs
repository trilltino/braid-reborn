// Starts a prebuilt, loopback-only fixture around local_link_server::router_with_cors.
// This helper never invokes Cargo. Ephemeral tokens stay in memory, outside reports.
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { existsSync } from 'node:fs';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { isAbsolute, join, relative, resolve } from 'node:path';
import { createInterface } from 'node:readline';
import { browserRoot } from './servers.mjs';

export function fixtureExecutable() {
  return process.env.BRAID_BROWSER_FIXTURE || resolve(browserRoot, '../../target/debug/examples/browser_fixture' + (process.platform === 'win32' ? '.exe' : ''));
}

export async function startProductionFixture(origins) {
  const executable = fixtureExecutable();
  if (!existsSync(executable)) throw new Error('The native browser_fixture example is absent; build explicitly when permitted or set BRAID_BROWSER_FIXTURE.');
  const root = await mkdtemp(join(tmpdir(), 'braid-browser-routes-'));
  const server = spawn(executable, [root, '0', ...origins], { stdio: ['ignore', 'pipe', 'pipe'], windowsHide: true });
  // Do not retain or upload stderr: the fixture's bootstrap/authentication data is private.
  server.stderr.resume();
  const lines = createInterface({ input: server.stdout });
  let timer;
  async function close() {
    lines.close();
    if (server.pid && server.exitCode === null && server.signalCode === null) {
      const exited = once(server, 'exit');
      server.kill();
      await exited;
    }
    const scoped = relative(resolve(tmpdir()), resolve(root));
    if (!scoped || isAbsolute(scoped) || scoped === '..' || scoped.startsWith('../') || scoped.startsWith('..\\') || !scoped.startsWith('braid-browser-routes-')) {
      throw new Error('Refusing to remove a fixture directory outside its generated temporary scope');
    }
    await rm(root, { recursive: true, force: true, maxRetries: 5, retryDelay: 200 });
  }
  try {
    const bootstrap = await Promise.race([
      once(lines, 'line').then(([line]) => JSON.parse(line)),
      once(server, 'error').then(() => { throw new Error('Native browser fixture could not start'); }),
      once(server, 'exit').then(() => { throw new Error('Native browser fixture exited before readiness'); }),
      new Promise((_, reject) => { timer = setTimeout(() => reject(new Error('Native browser fixture startup timed out')), 30000); }),
    ]);
    let sequence = 0;
    return {
      origin: bootstrap.origin, tokens: { alice: bootstrap.alice, bob: bootstrap.bob },
      unique(label) { return `${label}-${++sequence}`; },
      async control(action, fields = {}) {
        const response = await fetch(bootstrap.origin + '/_fixture/control', {
          method: 'POST', headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ action, ...fields }), signal: AbortSignal.timeout(30000),
        });
        if (!response.ok) throw new Error(`Fixture operation ${action} failed (${response.status})`);
        return response.json();
      },
      async read(name, user = 'alice') {
        const response = await fetch(bootstrap.origin + '/v2/pages/' + name, { headers: { authorization: `Bearer ${bootstrap[user]}` }, signal: AbortSignal.timeout(30000) });
        if (!response.ok) throw new Error(`Fixture read failed (${response.status})`);
        return { text: await response.text(), version: response.headers.get('version') };
      }, close,
    };
  } catch (error) { await close(); throw error; }
  finally { clearTimeout(timer); }
}
