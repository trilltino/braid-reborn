// Real application routes and the production Rust Simpleton/Fetch adapters.
// The fixture's middleware only observes lifetime and loses one already-committed
// response. It does not implement or proxy any Braid protocol behavior.
import assert from 'node:assert/strict';
import { test } from '../lib/runner.mjs';
import { ok, simpleton } from '../lib/harness.mjs';

async function until(check, label) {
  const deadline = Date.now() + 30000;
  while (Date.now() < deadline) { if (await check()) return; await new Promise(resolve => setTimeout(resolve, 25)); }
  throw new Error(`Timed out: ${label}`);
}
async function seed(t, label, text = 'a😀b', share = false) {
  assert.ok(t.fixture, 'native fixture is required for network tests');
  const name = t.fixture.unique(label);
  await t.fixture.control('seed', { name, text, share });
  return { name, url: `${t.fixture.origin}/v2/pages/${name}`, text };
}
async function initialized(page, slot = 'text') {
  await until(async () => {
    const value = ok(await simpleton.status(page, slot));
    assert.equal(value.error, null);
    return value.online && value.initialized;
  }, 'production Simpleton initialized');
}
async function settled(page, text, slot = 'text') {
  await until(async () => {
    const value = ok(await simpleton.status(page, slot));
    assert.equal(value.error, null);
    return value.text === text && value.online && value.pending_count === 0 && !value.has_pending_edit;
  }, 'durable edit acknowledged');
}

test('network: actual application CORS exposes exact historical versions and denies unapproved origins', async t => {
  const item = await seed(t, 'cors');
  const context = await t.newContext();
  const page = await t.openPage(context);
  const result = await page.evaluate(([url, token, version]) => window.attempt(() => braid.FetchHarness.finite(url, token, version)), [item.url, t.fixture.tokens.alice, `seed-${item.name}`]);
  const response = JSON.parse(ok(result));
  assert.equal(response.status, 200);
  assert.equal(response.body, item.text);
  assert.equal(response.headers.version, `"seed-${item.name}"`);
  const denied = await t.openPage(context, t.urls.alt);
  const inaccessible = await denied.evaluate(([url, token]) => window.attempt(() => braid.FetchHarness.finite(url, token)), [item.url, t.fixture.tokens.alice]);
  assert.ok(inaccessible.error, 'an unapproved origin must fail Fetch/CORS');
});

test('network: accepted PUT with lost ACK replays exactly the same identity', async t => {
  const item = await seed(t, 'lost-ack');
  const context = await t.newContext();
  const page = await t.openPage(context);
  ok(await simpleton.open(page, item.url, 'user:alice', t.fixture.tokens.alice));
  await initialized(page);
  await t.fixture.control('lose-ack', { name: item.name });
  const edited = 'aX😀b e\u0301';
  ok(await simpleton.changed(page, edited));
  await settled(page, edited);
  const metrics = await t.fixture.control('metrics');
  const attempts = metrics.puts.filter(entry => entry.path === `/v2/pages/${item.name}`);
  assert.ok(attempts.length >= 2, 'accepted request was replayed');
  assert.equal(new Set(attempts.map(entry => entry.version)).size, 1, 'uncertain PUT identity never changes');
  assert.equal((await t.fixture.read(item.name)).text, edited);
  ok(await simpleton.close(page));
  const persisted = JSON.stringify(await page.evaluate(() => window.raw.dump()));
  assert.ok(!persisted.includes(t.fixture.tokens.alice), 'transport credentials are absent from recovery data');
});

test('network: production Fetch rejects redirects before the target is requested', async t => {
  const context = await t.newContext();
  const page = await t.openPage(context);
  const before = (await t.fixture.control('metrics')).redirect_hits;
  const response = await page.evaluate(url => window.attempt(() => braid.FetchHarness.anonymous(url)), t.fixture.origin + '/_fixture/redirect');
  assert.ok(response.error, 'redirect is an explicit transport failure');
  assert.equal((await t.fixture.control('metrics')).redirect_hits, before, 'redirect target was never requested');
});

test('network: offline text survives reload and repairs over the production route', async t => {
  const item = await seed(t, 'offline');
  const context = await t.newContext();
  let page = await t.openPage(context);
  ok(await simpleton.open(page, item.url, 'user:alice', t.fixture.tokens.alice));
  await initialized(page);
  await context.setOffline(true);
  const edited = 'offline 😀 e\u0301';
  ok(await simpleton.changed(page, edited));
  const pending = ok(await simpleton.status(page));
  assert.equal(pending.text, edited);
  assert.ok(pending.pending_count > 0 || pending.has_pending_edit);
  ok(await simpleton.close(page));
  await context.setOffline(false);
  await page.reload();
  await page.waitForFunction(() => window.__ready);
  ok(await simpleton.open(page, item.url, 'user:alice', t.fixture.tokens.alice));
  await settled(page, edited);
  assert.equal((await t.fixture.read(item.name)).text, edited);
  ok(await simpleton.close(page));
});

test('network: two browser clients converge after forced same-base Unicode edits', async t => {
  const item = await seed(t, 'concurrent', 'a😀b', true);
  const leftContext = await t.newContext();
  const rightContext = await t.newContext();
  const left = await t.openPage(leftContext);
  const right = await t.openPage(rightContext);
  ok(await simpleton.open(left, item.url, 'user:alice', t.fixture.tokens.alice));
  ok(await simpleton.open(right, item.url, 'user:bob', t.fixture.tokens.bob));
  await Promise.all([initialized(left), initialized(right)]);
  let waiting = 0, release;
  const gate = new Promise(resolve => { release = resolve; });
  const intercept = async route => {
    if (route.request().method() === 'PUT') {
      if (++waiting === 2) release();
      await gate;
    }
    await route.continue();
  };
  await leftContext.route(item.url, intercept);
  await rightContext.route(item.url, intercept);
  ok(await simpleton.changed(left, 'aX😀b'));
  ok(await simpleton.changed(right, 'a😀Yb'));
  await Promise.all([settled(left, 'aX😀Yb'), settled(right, 'aX😀Yb')]);
  assert.equal((await t.fixture.read(item.name)).text, 'aX😀Yb');
  ok(await simpleton.close(left)); ok(await simpleton.close(right));
});

test('network: a persistent browser restart retains Simpleton identity, pending edits and receive frontier', async t => {
  const item = await seed(t, 'restart');
  const profile = await t.persistentProfile();
  let context = await profile.launch();
  let page = await t.openPage(context);
  ok(await simpleton.open(page, item.url, 'user:alice', t.fixture.tokens.alice));
  await initialized(page);
  const initial = ok(await simpleton.status(page));
  assert.deepEqual(initial.receive_frontier, [`seed-${item.name}`]);
  await context.setOffline(true);
  const edited = 'survives browser restart 😀';
  ok(await simpleton.changed(page, edited));
  const before = ok(await simpleton.status(page));
  ok(await simpleton.close(page));
  const saved = (await page.evaluate(() => window.raw.dump())).stores.journals[0].value.payload;
  assert.ok(saved.bytes > 0);
  await profile.close();
  context = await profile.launch();
  page = await t.openPage(context);
  ok(await simpleton.open(page, item.url, 'user:alice', t.fixture.tokens.alice));
  await settled(page, edited);
  const after = ok(await simpleton.status(page));
  assert.equal(after.recovery, 'Verified');
  if (before.version[0] !== initial.version[0]) assert.equal(after.version[0], before.version[0], 'uncertain queued identity survives full browser restart');
  assert.equal((await t.fixture.read(item.name)).text, edited);
  ok(await simpleton.close(page));
});

test('network: dropping a full slow-reader subscription cancels the actual server body', async t => {
  const item = await seed(t, 'slow');
  const context = await t.newContext();
  const page = await t.openPage(context);
  const before = await t.fixture.control('metrics');
  ok(await page.evaluate(([url, token]) => window.attempt(async () => {
    window.slow = await braid.FetchHarness.subscribe(url, token); return true;
  }), [item.url, t.fixture.tokens.alice]));
  await until(async () => (await t.fixture.control('metrics')).active > before.active, 'server subscription active');
  for (let index = 0; index < 4; index++) {
    const current = await t.fixture.read(item.name);
    const response = await fetch(item.url, {
      method: 'PUT', headers: { authorization: `Bearer ${t.fixture.tokens.alice}`, 'content-type': 'text/plain', version: `"slow-${item.name}-${index}"`, parents: current.version },
      body: 'x'.repeat(128 * 1024) + index, signal: AbortSignal.timeout(30000),
    });
    assert.equal(response.status, 200);
  }
  const finite = await t.fixture.read(item.name);
  assert.ok(finite.text.endsWith('3'), 'slow subscription does not block unrelated finite requests');
  ok(await page.evaluate(() => window.attempt(() => { window.slow.close(); window.slow.free(); delete window.slow; return true; })));
  await until(async () => (await t.fixture.control('metrics')).active === before.active, 'server stream cancelled after receiver drop');
});

test('network: revocation terminates an idle authenticated browser subscription', async t => {
  const item = await seed(t, 'revoke', 'private', true);
  const context = await t.newContext();
  const page = await t.openPage(context);
  ok(await simpleton.open(page, item.url, 'user:bob', t.fixture.tokens.bob));
  await initialized(page);
  await t.fixture.control('revoke', { name: item.name });
  await until(async () => {
    const value = ok(await simpleton.status(page));
    return value.error !== null && !value.online;
  }, 'revoked Simpleton stopped');
  const response = await page.evaluate(([url, token]) => window.attempt(() => braid.FetchHarness.finite(url, token)), [item.url, t.fixture.tokens.bob]);
  assert.equal(JSON.parse(ok(response)).status, 403);
  ok(await simpleton.close(page));
});
