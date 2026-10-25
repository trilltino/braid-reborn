// Keep each supported profile isolated from workspace feature unification.
import { spawnSync } from 'node:child_process';
import { mkdirSync, writeFileSync } from 'node:fs';

const profiles = [
  ['braid-http', 'minimal', ''], ['braid-http', 'native', 'native'],
  ['braid-core', 'minimal', ''], ['braid-core', 'client', 'client'],
  ['braid-core', 'native', 'native'], ['braid-core', 'server', 'server'],
  ['braid-core', 'blob', 'blob'], ['braid-core', 'fs', 'fs'],
  ['braid-core', 'nfs', 'nfs'], ['braid-core', 'engine', 'serde,smallvec'],
  ['braid-core', 'compressed-engine', 'serde,smallvec,lz4'],
  ['braid-iroh', 'minimal', ''], ['braid-iroh', 'proxy', 'proxy'],
];
const selected = process.argv.slice(2);
if (selected.some(key => !profiles.some(([pkg, name]) => `${pkg}/${name}` === key))) {
  throw new Error('Unknown profile; use package/profile from this script');
}
const results = [];
mkdirSync('target/feature-profiles', { recursive: true });
for (const [pkg, name, features] of profiles) {
  if (selected.length && !selected.includes(`${pkg}/${name}`)) continue;
  const common = ['-p', pkg, '--lib', '--no-default-features', '--locked'];
  if (features) common.push('--features', features);
  const args = ['clippy', ...common, '--', '-D', 'warnings'];
  console.log(`Checking ${pkg}/${name}`);
  const result = spawnSync('cargo', args, { stdio: 'inherit', windowsHide: true });
  const record = { package: pkg, profile: name, features, args, status: result.status, error: result.error?.message };
  results.push(record);
  writeFileSync('target/feature-profiles/report.json', JSON.stringify({ platform: process.platform, results }, null, 2));
  if (result.error || result.status !== 0) process.exit(result.status || 1);
  // Library test crates deny Rust warnings themselves; Clippy separately checks
  // production code. Integration routes are exercised by the workspace gate.
  const tests = spawnSync('cargo', ['test', ...common], { stdio: 'inherit', windowsHide: true });
  record.test_status = tests.status;
  record.test_error = tests.error?.message;
  writeFileSync('target/feature-profiles/report.json', JSON.stringify({ platform: process.platform, results }, null, 2));
  if (tests.error || tests.status !== 0) process.exit(tests.status || 1);
}
