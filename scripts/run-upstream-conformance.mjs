// Run the independent, pinned upstream harness against the Rust library adapter.
import { spawnSync } from 'node:child_process';
import { existsSync, mkdirSync, writeFileSync } from 'node:fs';
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
// The harness manifest uses ranges. Pin its two protocol implementations too.
run(process.platform === 'win32' ? 'npm.cmd' : 'npm',
  ['install', '--ignore-scripts', '--save-exact', 'braid-text@0.5.3', 'braid-http@1.3.125'],
  { cwd: path.join(root, harness), shell: process.platform === 'win32' });
const executable = 'target/debug/examples/fuzz_controller' + (process.platform === 'win32' ? '.exe' : '');
if (!existsSync(executable)) throw new Error('Run cargo build -p braid-http --example fuzz_controller first');
let failed = false;
const suites = ['http', 'subscriptions'];
if (process.argv.includes('--reliability')) suites.push('reliable-updates');
for (const suite of suites) {
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
