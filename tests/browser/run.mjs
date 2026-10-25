// Runs every browser suite in Chromium, Firefox and WebKit against local servers
// and prints per-engine pass/fail/skip counts. An engine is never skipped
// silently: a failure to launch is a failure, and a skipped test prints why.
//
//   node tests/browser/run.mjs [--engine chromium,firefox,webkit] [--grep text] [--no-build] [--suite journal,...]
import { spawnSync } from 'node:child_process';
import { existsSync, readdirSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { registry, runAll, engines } from './lib/runner.mjs';
import { makeContextFactory } from './lib/context.mjs';
import { ports, startPageServer, browserRoot } from './lib/servers.mjs';
import { startProductionFixture } from './lib/production.mjs';

const args = process.argv.slice(2);
const option = (name) => { const i = args.indexOf(`--${name}`); return i >= 0 ? args[i + 1] : undefined; };
const flag = (name) => args.includes(`--${name}`);
const selected = (option('engine') || Object.keys(engines).join(',')).split(',');
for (const engine of selected) if (!engines[engine]) { console.error(`unknown engine ${engine}`); process.exit(2); }

if (flag('no-build') && !existsSync(join(browserRoot, 'dist', 'pkg', 'harness.js'))) {
  console.error('The browser harness is absent. --no-build never invokes Cargo; build explicitly when permitted.');
  process.exit(2);
}
if (!flag('no-build')) {
  const built = spawnSync(process.execPath, [join(browserRoot, 'build-harness.mjs')], { stdio: 'inherit' });
  if (built.status !== 0) { console.error('harness build failed'); process.exit(2); }
}

const here = dirname(fileURLToPath(import.meta.url));
const wanted = option('suite')?.split(',');
for (const file of readdirSync(join(here, 'suites')).sort()) {
  if (!file.endsWith('.mjs')) continue;
  if (wanted && !wanted.includes(file.replace(/\.mjs$/, ''))) continue;
  await import(pathToFileURL(join(here, 'suites', file)).href);
}

const p = ports();
const page = await startPageServer(p.page, { host: '0.0.0.0' });
const urls = { app: `http://localhost:${p.page}`, alt: `http://127.0.0.1:${p.page}` };
let summary;
let fixture;
try {
  if (!wanted || wanted.includes('network')) fixture = await startProductionFixture([urls.app]);
  summary = await runAll({ selected, grep: option('grep'), makeContext: makeContextFactory({ urls, fixture }) });
} finally {
  if (fixture) await fixture.close();
  await page.close();
}

console.log('\n== summary ==');
let failures = 0;
for (const [engine, s] of Object.entries(summary)) {
  console.log(`${engine.padEnd(9)} passed ${s.passed}  failed ${s.failed}  skipped ${s.skipped}  total ${s.passed + s.failed + s.skipped}`);
  for (const skip of s.skips) console.log(`  skipped: ${skip.name} -- ${skip.reason}`);
  for (const failure of s.failures) console.log(`  FAILED: ${failure.name}`);
  failures += s.failed;
}
const revision = spawnSync('git', ['rev-parse', 'HEAD'], { cwd: join(browserRoot, '../..'), encoding: 'utf8', windowsHide: true });
writeFileSync(join(browserRoot, 'dist', 'results.json'), JSON.stringify({
  schema_version: 1, source_revision: revision.status === 0 ? revision.stdout.trim() : null,
  platform: process.platform, architecture: process.arch, node: process.version,
  scenarios: 'deterministic registered cases', engines: summary,
}, null, 2));
process.exit(failures ? 1 : 0);
