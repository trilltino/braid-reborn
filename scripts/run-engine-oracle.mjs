// Build the upstream-engine oracle (tests/oracle) and run the differential test against
// the copied engine. Usage: node scripts/run-engine-oracle.mjs [seeds]
import { spawnSync } from 'node:child_process';
import { existsSync } from 'node:fs';
import { resolve } from 'node:path';

const root = resolve(import.meta.dirname, '..');
const targetDir = resolve(root, 'target/oracle');
const run = (command, args, options = {}) => {
  const result = spawnSync(command, args, { stdio: 'inherit', cwd: root, windowsHide: true, ...options });
  if (result.error || result.status !== 0) {
    console.error(`${command} ${args.join(' ')} failed`);
    process.exit(result.status || 1);
  }
};
run('cargo', ['build', '--locked', '--manifest-path', 'tests/oracle/Cargo.toml'], {
  env: { ...process.env, CARGO_TARGET_DIR: targetDir },
});
const binary = resolve(targetDir, 'debug', process.platform === 'win32' ? 'engine-oracle.exe' : 'engine-oracle');
if (!existsSync(binary)) throw new Error(`oracle binary missing: ${binary}`);
run('cargo', ['test', '--locked', '-p', 'braid-core', '--all-features', '--test', 'engine_oracle', '--', '--include-ignored', '--nocapture'], {
  env: { ...process.env, BRAID_ORACLE_BIN: binary, ...(process.argv[2] ? { BRAID_ORACLE_SEEDS: process.argv[2] } : {}) },
});
