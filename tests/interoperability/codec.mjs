// Differential codec contract against actual upstream source, pinned by Git SHA.
import { createRequire } from 'node:module';
import { spawnSync } from 'node:child_process';
import { readFileSync, writeFileSync, mkdirSync, existsSync } from 'node:fs';
import http from 'node:http';
import assert from 'node:assert/strict';
import path from 'node:path';
const root = path.resolve(import.meta.dirname, '../..');
process.chdir(root);
const sha = '639b811ec91e5d1aae4a4a7d162616b499f95b07';
const source = 'target/engineering/upstream/braid-http';
function run(program, args, options = {}) {
  const result = spawnSync(program, args, { encoding: 'utf8', ...options });
  if (result.status !== 0) throw Error(result.stderr || result.error || result.stdout);
  return result.stdout;
}
if (!existsSync(source + '/.git')) {
  mkdirSync(path.dirname(source), { recursive: true });
  run('git', ['clone', '--no-checkout', 'https://github.com/braid-org/braid-http.git', source]);
}
run('git', ['-C', source, 'fetch', '--refetch', '--depth=1', 'https://github.com/braid-org/braid-http.git', sha]);
run('git', ['-C', source, 'checkout', sha]);
const require = createRequire(import.meta.url);
const { braidify } = require(path.join(root, source, 'braid-http-server.js'));
const { parse_update } = require(path.join(root, source, 'braid-http-client.js'));
const fixtures = [
  { name: 'unicode-snapshot', expected: { version: ['one'], parents: [], body: [...Buffer.from('a\u{1f600}\u00e9\r\nb')] } },
  { name: 'empty-snapshot', expected: { version: ['two'], parents: ['one'], body: [] } },
  { name: 'single-patch', expected: { version: ['three'], parents: ['two'], patches: [{ unit: 'text', range: '[0:0]', content: [...Buffer.from('\u{1f600}')] }] } },
  { name: 'numeric-multipatch-binary', expected: { version: ['four'], parents: ['three'], patches: [{ unit: 'bytes', range: '[0:0]', content: [0, 255, 13, 10] }, { unit: 'bytes', range: '[3:4]', content: [] }] } },
];
async function encodeJS(expected) {
  const server = http.createServer(async (req, res) => {
    braidify(req, res);
    res.startMultiresponse();
    const update = { version: expected.version, parents: expected.parents };
    if (expected.body) update.body = new Uint8Array(expected.body);
    else if (expected.patches.length === 1) update.patch = { ...expected.patches[0], content: new Uint8Array(expected.patches[0].content) };
    else update.patches = expected.patches.map(p => ({ ...p, content: new Uint8Array(p.content) }));
    await res.sendUpdate(update);
    res.end();
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  try {
    return await new Promise((resolve, reject) => {
      http.get(`http://127.0.0.1:${server.address().port}/doc`, res => {
        assert.equal(res.statusCode, 209);
        const chunks = [];
        res.on('data', chunk => chunks.push(chunk));
        res.on('end', () => resolve([...Buffer.concat(chunks)]));
        res.on('error', reject);
      }).on('error', reject);
    });
  } finally { await new Promise(resolve => server.close(resolve)); }
}
function decodeJS(wire) {
  const state = parse_update({ input: new Uint8Array(wire) });
  assert.equal(state.result, 'success');
  const result = { version: state.version, parents: state.parents };
  if (state.patches) result.patches = state.patches.map(({ unit, range, content }) => ({ unit, range, content: [...content] }));
  else result.body = [...state.body];
  return result;
}
for (const fixture of fixtures) {
  fixture.wire = await encodeJS(fixture.expected);
  assert.deepEqual(decodeJS(fixture.wire), fixture.expected);
}
const file = 'tests/protocol-fixtures/js-codec.json';
const record = { source: `https://github.com/braid-org/braid-http/tree/${sha}`, sha, fixtures };
if (process.argv.includes('--generate')) {
  mkdirSync(path.dirname(file), { recursive: true });
  writeFileSync(file, JSON.stringify(record, null, 2) + '\n');
} else assert.deepEqual(JSON.parse(readFileSync(file)), record, 'Upstream fixture bytes changed');
const input = fixtures.map(({ expected }) => ({ ...expected, status: 200, extra_headers: {}, body: expected.body ?? null, patches: expected.patches ?? null }));
const rust = JSON.parse(run('cargo', ['run', '--quiet', '--locked', '-p', 'braid-http', '--example', 'codec_fixtures'], { input: JSON.stringify(input) }));
rust.forEach((wire, i) => assert.deepEqual(decodeJS(wire), fixtures[i].expected));
console.log(`${fixtures.length} JS -> Rust fixtures and ${rust.length} Rust -> JS frames verified at ${sha}`);
