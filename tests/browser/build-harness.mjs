// Builds the wasm harness with cargo and the wasm-bindgen CLI whose version matches Cargo.lock.
import { spawnSync } from 'node:child_process';
import { existsSync, mkdirSync, readFileSync, rmSync } from 'node:fs';
import { dirname, isAbsolute, join, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
const out = join(root, 'tests', 'browser', 'dist', 'pkg');

function run(command, args, options = {}) {
  const result = spawnSync(command, args, { stdio: 'inherit', cwd: root, ...options });
  if (result.status !== 0) throw new Error(`${command} ${args.join(' ')} failed with ${result.status}`);
}

function lockedBindgenVersion() {
  const lock = readFileSync(join(root, 'Cargo.lock'), 'utf8');
  const match = lock.match(/name = "wasm-bindgen"\r?\nversion = "([^"]+)"/);
  if (!match) throw new Error('wasm-bindgen is not in Cargo.lock');
  return match[1];
}

function bindgenBinary(version) {
  const candidates = [
    process.env.WASM_BINDGEN,
    join(root, 'target', 'wasm-tools', 'bin', 'wasm-bindgen.exe'),
    join(root, 'target', 'wasm-tools', 'bin', 'wasm-bindgen'),
    'wasm-bindgen',
  ].filter(Boolean);
  for (const candidate of candidates) {
    const probe = spawnSync(candidate, ['--version'], { encoding: 'utf8' });
    if (probe.status === 0 && probe.stdout.trim() === `wasm-bindgen ${version}`) return candidate;
  }
  throw new Error(
    `wasm-bindgen CLI ${version} (the version in Cargo.lock) was not found. Install it with:\n` +
    `  cargo install wasm-bindgen-cli --version ${version} --locked --root target/wasm-tools\n` +
    `or set WASM_BINDGEN to a matching binary.`);
}

const bindgen = bindgenBinary(lockedBindgenVersion());
run('cargo', ['build', '-p', 'braid-browser-harness', '--target', 'wasm32-unknown-unknown', '--release', '--locked']);
run('cargo', ['build', '-p', 'local_link_server', '--example', 'browser_fixture', '--locked']);
const scopedOut = relative(root, out);
if (!scopedOut || isAbsolute(scopedOut) || scopedOut.startsWith('..') || resolve(out) !== resolve(root, 'tests/browser/dist/pkg')) throw new Error('Invalid generated output directory');
rmSync(out, { recursive: true, force: true });
mkdirSync(out, { recursive: true });
const wasm = join(root, 'target', 'wasm32-unknown-unknown', 'release', 'braid_browser_harness.wasm');
if (!existsSync(wasm)) throw new Error(`missing ${wasm}`);
run(bindgen, ['--target', 'web', '--out-dir', out, '--out-name', 'harness', wasm]);
console.log(`harness written to ${out}`);
