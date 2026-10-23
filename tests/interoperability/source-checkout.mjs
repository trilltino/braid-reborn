import assert from 'node:assert/strict';
import { mkdirSync, mkdtempSync, readFileSync, writeFileSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import path from 'node:path';
import { checkoutUpstream } from '../../scripts/upstream-checkout.mjs';

const root = path.resolve(import.meta.dirname, '../../target');
mkdirSync(root, { recursive: true });
const fixture = mkdtempSync(path.join(root, 'checkout-regression-'));
function git(args) {
  const result = spawnSync('git', args, { encoding: 'utf8' });
  assert.equal(result.status, 0, result.stderr);
  return result.stdout.trim();
}
function repository(name, file, body) {
  const directory = path.join(fixture, name);
  git(['init', directory]);
  writeFileSync(path.join(directory, file), body);
  git(['-C', directory, 'add', file]);
  git(['-C', directory, '-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid', 'commit', '-m', 'Fixture']);
  return directory;
}
const remote = repository('remote', 'source.js', 'independent source');
const parent = repository('parent', 'keep.txt', 'parent must survive');
const before = git(['-C', parent, 'rev-parse', 'HEAD']);
const revision = git(['-C', remote, 'rev-parse', 'HEAD']);
const invalid = path.join(parent, 'cache', 'upstream');
mkdirSync(path.join(invalid, '.git'), { recursive: true });
const source = checkoutUpstream(invalid, remote, revision);
assert.notEqual(source, invalid);
assert.equal(readFileSync(path.join(source, 'source.js'), 'utf8'), 'independent source');
assert.equal(git(['-C', parent, 'rev-parse', 'HEAD']), before);
assert.equal(readFileSync(path.join(parent, 'keep.txt'), 'utf8'), 'parent must survive');
assert.equal(checkoutUpstream(source, remote, revision), source);
writeFileSync(path.join(source, 'source.js'), 'preserved local modification');
const replacement = checkoutUpstream(source, remote, revision);
assert.notEqual(replacement, source);
assert.equal(readFileSync(path.join(source, 'source.js'), 'utf8'), 'preserved local modification');
assert.equal(readFileSync(path.join(replacement, 'source.js'), 'utf8'), 'independent source');
console.log('Invalid Git cache cannot mutate its parent; isolated checkout reuse passed');
