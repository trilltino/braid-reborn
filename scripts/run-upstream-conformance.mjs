// Run the independent, pinned upstream harness against the Rust library adapter.
import { spawnSync } from 'node:child_process';
import { copyFileSync, existsSync, mkdirSync, writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const root = fileURLToPath(new URL('../', import.meta.url));
process.chdir(root);
const harness = path.join('target', 'audit', 'braid-fuzz');
const revision = 'b79c23f877d2e6807e3925bc0165a4da139a4458';
function run(program, args, options = {}) {
  const result = spawnSync(program, args, { cwd: root, stdio: 'inherit', ...options });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`${program} exited ${result.status}`);
}
mkdirSync(path.dirname(harness), { recursive: true });
if (!existsSync(path.join(harness, '.git'))) {
  run('git', ['clone', '--no-checkout', 'https://github.com/braid-org/braid-fuzz.git', harness]);
}
run('git', ['-C', harness, 'checkout', revision]);
// Keep the independent runner source pinned while locking its full npm graph.
// The compatible send override fixes the bridge's test-only XSS advisory.
for (const file of ['package.json', 'package-lock.json']) {
  copyFileSync(path.join(root, 'tests', 'interoperability', 'upstream-harness', file),
    path.join(root, harness, file));
}
run(process.platform === 'win32' ? 'npm.cmd' : 'npm',
  ['ci', '--ignore-scripts'],
  { cwd: path.join(root, harness), shell: process.platform === 'win32' });
run(process.platform === 'win32' ? 'npm.cmd' : 'npm',
  ['audit', '--omit=dev'],
  { cwd: path.join(root, harness), shell: process.platform === 'win32' });
const suffix = process.platform === 'win32' ? '.exe' : '';
const rawExecutable = 'target/debug/examples/fuzz_controller' + suffix;
const textExecutable = 'target/debug/examples/simpleton_controller' + suffix;
for (const executable of [rawExecutable, textExecutable]) {
  if (!existsSync(executable)) throw new Error('Build braid-http --example fuzz_controller and braid-core --example simpleton_controller first');
}
let failed = false;
const suites = ['http', 'subscriptions', 'reliable-updates', 'simpleton'];
for (const suite of suites) {
  const executable = ['http', 'subscriptions'].includes(suite) ? rawExecutable : textExecutable;
  const result = spawnSync(process.execPath,
    [path.join(harness, 'test-runner.js'), '--cmd', executable, suite, '--json'],
    { encoding: 'utf8', cwd: root });
  if (result.error) throw result.error;
  writeFileSync(path.join('target', 'audit', `${suite}.json`), result.stdout);
  writeFileSync(path.join('target', 'audit', `${suite}.stderr.log`), result.stderr);
  const report = JSON.parse(result.stdout);
  console.log(`${suite}: ${report.summary.passed}/${report.summary.total} passed`);
  if (report.summary.failed) {
    failed = true;
    for (const test of report.results.filter(t => t.status === 'fail')) console.log(`${test.id}: ${test.error}`);
  }
}
process.exitCode = failed ? 1 : 0;
