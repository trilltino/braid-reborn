// BRW-CAP-1..3 against real engines: the production ChannelJournal::open_browser
// (IndexedDB + exclusive Web Locks) driven through the wasm harness.
import assert from 'node:assert/strict';
import { test } from '../lib/runner.mjs';
import { channel, journal, ok, failed, resource } from '../lib/harness.mjs';

const ALICE = 'user:alice';
const BOB = 'user:bob';
// Astral character, combining mark, and a scalar that normalises differently.
const UNICODE = 'a\u{1F600}b é é \u{10FFFF}';

test('capabilities: missing facilities fail explicitly before identity or storage is touched', async t => {
  const context = await t.newContext();
  for (const [facility, remove, pattern] of [
    ['IndexedDB', () => Object.defineProperty(window, 'indexedDB', { value: undefined, configurable: true }), /IndexedDB/],
    ['Web Locks', () => Object.defineProperty(Navigator.prototype, 'locks', { get: () => undefined, configurable: true }), /Web Locks/],
    ['crypto.subtle', () => Object.defineProperty(window, 'crypto', { value: { getRandomValues: crypto.getRandomValues.bind(crypto) }, configurable: true }), /crypto/],
  ]) {
    const page = await t.openPage(context);
    assert.equal(await page.evaluate(() => window.raw.exists()), false, 'fresh profile has no journal database');
    await page.evaluate(remove);
    const result = await page.evaluate(() => window.attempt(() => braid.JournalHarness.open('http://localhost/doc', 'user:alice')));
    const error = failed(result, 'capability', `${facility} missing`);
    assert.match(error.message, pattern);
    await page.close();
  }
  // Identity generation checks crypto first and returns an error instead of panicking.
  const page = await t.openPage(context);
  await page.evaluate(() => Object.defineProperty(window, 'crypto', { value: undefined, configurable: true }));
  failed(await page.evaluate(() => window.attempt(() => braid.peer_id())), 'capability', 'peer id without crypto');
  failed(await page.evaluate(() => window.attempt(() => braid.check_capabilities())), 'capability', 'capability check without crypto');
  await page.close();
  const fresh = await t.openPage(context);
  assert.equal(await fresh.evaluate(() => window.raw.exists()), false, 'no database was created by any failed open');
  assert.match(ok(await fresh.evaluate(() => window.attempt(() => braid.peer_id()))), /^[0-9a-f-]{36}$/);
});

test('journal: offline edits persist through reload with unicode intact', async t => {
  const context = await t.newContext();
  const page = await t.openPage(context);
  const url = resource(t.urls);
  ok(await channel.open(page, url, ALICE), 'open');
  ok(await channel.queue(page, 'v-one', UNICODE, JSON.stringify({ text: UNICODE, n: 1 })), 'queue one');
  ok(await channel.queue(page, 'v-two', UNICODE + '!', JSON.stringify({ text: UNICODE + '!', n: 2 })), 'queue two');
  ok(await channel.close(page), 'close');
  await page.reload();
  await page.waitForFunction(() => window.__ready);
  ok(await channel.open(page, url, ALICE), 'reopen after reload');
  const pending = ok(await channel.pending(page), 'pending');
  assert.deepEqual(pending, [
    { version: ['v-one'], body: UNICODE },
    { version: ['v-two'], body: UNICODE + '!' },
  ]);
  ok(await channel.close(page));
  const dump = await page.evaluate(() => window.raw.dump());
  assert.equal(dump.version, 2);
  assert.deepEqual(Object.keys(dump.stores).sort(), ['backups', 'journals']);
  assert.equal(dump.stores.journals.length, 1);
});

test('journal: a persistent profile keeps the outbox across browser restart', async t => {
  const profile = await t.persistentProfile();
  const url = resource(t.urls, 'restart');
  let context = await profile.launch();
  let page = await t.openPage(context);
  ok(await channel.open(page, url, ALICE));
  ok(await channel.queue(page, 'before-restart', UNICODE, JSON.stringify({ n: 1 })));
  ok(await channel.close(page));
  await profile.close();
  context = await profile.launch();
  page = await t.openPage(context);
  ok(await channel.open(page, url, ALICE));
  assert.deepEqual(ok(await channel.pending(page)), [{ version: ['before-restart'], body: UNICODE }]);
  ok(await channel.close(page));
});

test('journal: a second tab is refused with the explicit competing-writer error', async t => {
  const context = await t.newContext();
  const first = await t.openPage(context);
  const second = await t.openPage(context);
  const url = resource(t.urls, 'exclusive');
  ok(await channel.open(first, url, ALICE));
  ok(await channel.queue(first, 'owned', 'x'));
  const refused = failed(await channel.open(second, url, ALICE), 'competing-writer', 'second tab');
  assert.match(refused.message, /competing writer/);
  // The refused tab reads nothing and wrote nothing.
  assert.deepEqual(ok(await channel.pending(first)), [{ version: ['owned'], body: 'x' }]);
  ok(await channel.close(first));
  // Ownership is released only after the last transaction: now the other tab may own it.
  ok(await channel.open(second, url, ALICE), 'second tab after release');
  assert.deepEqual(ok(await channel.pending(second)), [{ version: ['owned'], body: 'x' }]);
  ok(await channel.close(second));
});

test('journal: closing the owning tab releases the lock for another tab', async t => {
  const context = await t.newContext();
  const first = await t.openPage(context);
  const second = await t.openPage(context);
  const url = resource(t.urls, 'tabclose');
  ok(await channel.open(first, url, ALICE));
  failed(await channel.open(second, url, ALICE), 'competing-writer');
  await first.close();
  let result;
  for (let attempt = 0; attempt < 40; attempt++) {
    result = await channel.open(second, url, ALICE);
    if (!result.error) break;
    await new Promise(r => setTimeout(r, 100));
  }
  ok(result, 'second tab after the owner tab closed');
  ok(await channel.close(second));
});

test('journal: authorization contexts never see each other\'s journal, nor is any identity stored in clear', async t => {
  const context = await t.newContext();
  const page = await t.openPage(context);
  const url = resource(t.urls, 'isolated');
  // Both principals may own the same resource at the same time: separate locks and keys.
  ok(await channel.open(page, url, ALICE, 'alice'));
  ok(await channel.open(page, url, BOB, 'bob'));
  ok(await channel.queue(page, 'alice-edit', 'only alice', null, 'alice'));
  assert.deepEqual(ok(await channel.pending(page, 'bob')), []);
  ok(await channel.close(page, 'alice'));
  ok(await channel.close(page, 'bob'));
  // Reopening as bob still sees nothing; alice sees her own edit.
  ok(await channel.open(page, url, BOB, 'bob'));
  assert.deepEqual(ok(await channel.pending(page, 'bob')), []);
  ok(await channel.close(page, 'bob'));
  ok(await channel.open(page, url, ALICE, 'alice'));
  assert.deepEqual(ok(await channel.pending(page, 'alice')), [{ version: ['alice-edit'], body: 'only alice' }]);
  ok(await channel.close(page, 'alice'));
  const dump = await page.evaluate(() => window.raw.dump());
  const text = JSON.stringify(dump);
  assert.ok(!text.includes('user:alice') && !text.includes('user:bob'), 'authorization context is never stored in clear');
  assert.ok(!/bearer|authorization|cookie/i.test(text), 'no credential material in the database');
  const contexts = new Set(dump.stores.journals.map(item => item.value.context));
  assert.equal(dump.stores.journals.length, 2, 'one record per authorization context');
  assert.equal(contexts.size, 2);
  for (const id of contexts) assert.match(id, /^[0-9a-f]{64}$/);
  // Another browser context (a separate profile) shares nothing at all.
  const other = await t.newContext();
  const otherPage = await t.openPage(other);
  ok(await channel.open(otherPage, url, ALICE));
  assert.deepEqual(ok(await channel.pending(otherPage)), []);
  ok(await channel.close(otherPage));
});

test('journal: a different resource under the same context is a different journal', async t => {
  const context = await t.newContext();
  const page = await t.openPage(context);
  ok(await channel.open(page, resource(t.urls, 'a'), ALICE, 'a'));
  ok(await channel.open(page, resource(t.urls, 'b'), ALICE, 'b'));
  ok(await channel.queue(page, 'on-a', 'a', null, 'a'));
  assert.deepEqual(ok(await channel.pending(page, 'b')), []);
  ok(await channel.close(page, 'a'));
  ok(await channel.close(page, 'b'));
});

test('journal: a transaction abort acknowledges nothing and publishes no candidate', async t => {
  const context = await t.newContext();
  const page = await t.openPage(context);
  const url = resource(t.urls, 'abort');
  ok(await channel.open(page, url, ALICE));
  ok(await channel.save(page, JSON.stringify({ n: 1 })));
  // A real IndexedDB abort: the browser rolls the readwrite transaction back and
  // dispatches `abort`. The wrapper only decides when to call the genuine tx.abort().
  await page.evaluate(() => {
    const original = IDBDatabase.prototype.transaction;
    window.__aborted = 0;
    IDBDatabase.prototype.transaction = function (...args) {
      const tx = original.apply(this, args);
      if (args[1] === 'readwrite' && args[0] === 'journals') { window.__aborted++; queueMicrotask(() => tx.abort()); }
      return tx;
    };
    window.__restore = () => { IDBDatabase.prototype.transaction = original; };
  });
  const failure = failed(await channel.save(page, JSON.stringify({ n: 2 })), 'io', 'aborted save');
  assert.match(failure.message, /aborted/);
  failed(await channel.queue(page, 'lost', 'never stored', JSON.stringify({ n: 3 })), 'io', 'aborted queue');
  assert.equal(await page.evaluate(() => window.__aborted), 2);
  assert.deepEqual(ok(await channel.pending(page)), [], 'the aborted entry was not published in memory');
  await page.evaluate(() => window.__restore());
  ok(await channel.queue(page, 'kept', 'stored'));
  ok(await channel.close(page));
  ok(await journal.open(page, url, ALICE));
  const checkpoint = ok(await journal.checkpoint(page));
  assert.deepEqual(checkpoint.application, { n: 1 }, 'neither aborted application state survived');
  assert.deepEqual(checkpoint.outbox.map(update => update.version), [['kept']]);
  ok(await journal.free(page));
});

const bigApplication = n => JSON.stringify({ n, filler: 'x'.repeat(3 * 1024 * 1024) });

/** Occupy the origin's quota with a second database until the engine itself refuses a write. */
async function fillQuota(page) {
  return page.evaluate(async () => {
    // A quota error can invalidate the connection (Firefox), so every write opens its own.
    const put = async (key, size) => {
      const db = await new Promise((resolve, reject) => {
        const request = indexedDB.open('filler', 1);
        request.onupgradeneeded = () => request.result.createObjectStore('s');
        request.onsuccess = () => resolve(request.result);
        request.onerror = () => reject(request.error);
      });
      try {
        await new Promise((resolve, reject) => {
          const tx = db.transaction('s', 'readwrite');
          tx.objectStore('s').put(new Uint8Array(size), key);
          tx.oncomplete = resolve; tx.onabort = () => reject(tx.error);
        });
      } finally { db.close(); }
    };
    let written = 0, stoppedBy = null, i = 0;
    // Large chunks first, then ever smaller ones, so the remaining quota is below any candidate write.
    for (let size = 1024 * 1024; size >= 16 * 1024 && written < 512 * 1024 * 1024;) {
      try { await put('k' + i++, size); written += size; }
      catch (error) { stoppedBy = error.name; size /= 4; }
    }
    return { written, stoppedBy };
  });
}
const dropFiller = page => page.evaluate(() => new Promise(resolve => {
  const r = indexedDB.deleteDatabase('filler'); r.onsuccess = r.onerror = r.onblocked = () => resolve();
}));

// Real engine quota exhaustion. Chromium: CDP Storage.overrideQuotaForOrigin (1 MiB) before the
// database exists. Firefox: the browser is launched with dom.quotaManager.temporaryStorage.fixedLimit=1024
// KiB and a second database is filled until Firefox itself throws QuotaExceededError.
// WebKit offers no quota control; its real-quota test is skipped with the recorded reason below.
test('journal: a real quota failure keeps the previous record and publishes nothing', async t => {
  const browser = t.engine === 'firefox'
    ? await t.launchBrowser({ firefoxUserPrefs: { 'dom.quotaManager.temporaryStorage.fixedLimit': 1024 } })
    : t.browser;
  const context = await t.newContext({}, browser);
  const page = await t.openPage(context);
  const url = resource(t.urls, 'quota');
  let relieve;
  if (t.engine === 'chromium') {
    const session = await context.newCDPSession(page);
    await session.send('Storage.overrideQuotaForOrigin', { origin: t.urls.app, quotaSize: 1024 * 1024 });
    relieve = () => session.send('Storage.overrideQuotaForOrigin', { origin: t.urls.app });
  }
  ok(await channel.open(page, url, ALICE));
  ok(await channel.save(page, JSON.stringify({ n: 1 })));
  if (t.engine === 'firefox') {
    const filled = await fillQuota(page);
    assert.equal(filled.stoppedBy, 'QuotaExceededError', `Firefox filler: ${JSON.stringify(filled)}`);
    relieve = () => dropFiller(page);
  }
  const refused = failed(await channel.save(page, bigApplication(2)), 'quota', 'write over the exhausted quota');
  assert.match(refused.message, /quota/i);
  assert.deepEqual(ok(await channel.pending(page)), []);
  await relieve();
  ok(await channel.queue(page, 'after-quota', 'fits'), 'a small write after the failure');
  ok(await channel.close(page));
  const reloaded = await t.openPage(context);
  ok(await journal.open(reloaded, url, ALICE));
  const checkpoint = ok(await journal.checkpoint(reloaded));
  assert.equal(checkpoint.application.n, 1, 'the last acknowledged application state is exactly what is stored');
  assert.deepEqual(checkpoint.outbox.map(update => update.version), [['after-quota']]);
  ok(await journal.free(reloaded));
}, {
  timeout: 180000,
  skip: {
    webkit: 'Playwright WebKit has no quota override; 6.7 GB of real IndexedDB filler never produced QuotaExceededError (stoppedBy: null). Covered only by the injected-DOMException test below.',
  },
});

// Engine-independent injection of the browser-reported failure: put() throws the
// DOMException a full disk raises. It exercises our real error path (abort, reject,
// no publish) in every engine, but it is NOT engine quota exhaustion.
test('journal: an injected QuotaExceededError from put() maps to a storage error and publishes nothing', async t => {
  const context = await t.newContext();
  const page = await t.openPage(context);
  const url = resource(t.urls, 'injected-quota');
  ok(await channel.open(page, url, ALICE));
  ok(await channel.save(page, JSON.stringify({ n: 1 })));
  await page.evaluate(() => {
    const original = IDBObjectStore.prototype.put;
    IDBObjectStore.prototype.put = function (...args) {
      if (this.name === 'journals') throw new DOMException('injected', 'QuotaExceededError');
      return original.apply(this, args);
    };
    window.__restore = () => { IDBObjectStore.prototype.put = original; };
  });
  failed(await channel.save(page, JSON.stringify({ n: 2 })), 'quota', 'injected quota');
  failed(await channel.queue(page, 'lost', 'x', JSON.stringify({ n: 3 })), 'quota', 'injected quota on queue');
  assert.deepEqual(ok(await channel.pending(page)), []);
  await page.evaluate(() => window.__restore());
  ok(await channel.queue(page, 'kept', 'y'));
  ok(await channel.close(page));
  ok(await journal.open(page, url, ALICE));
  const checkpoint = ok(await journal.checkpoint(page));
  assert.deepEqual(checkpoint.application, { n: 1 });
  assert.deepEqual(checkpoint.outbox.map(update => update.version), [['kept']]);
  ok(await journal.free(page));
});

test('journal: the 32 MiB checkpoint limit is enforced before any transaction', async t => {
  const context = await t.newContext();
  const page = await t.openPage(context);
  const url = resource(t.urls, 'limit');
  ok(await channel.open(page, url, ALICE));
  ok(await channel.save(page, JSON.stringify({ n: 1 })));
  await page.evaluate(() => {
    const original = IDBDatabase.prototype.transaction;
    window.__transactions = 0;
    IDBDatabase.prototype.transaction = function (...args) { window.__transactions++; return original.apply(this, args); };
  });
  const over = JSON.stringify({ n: 2, filler: 'y'.repeat(33 * 1024 * 1024) });
  failed(await channel.save(page, over), 'resource-limit', '33 MiB checkpoint');
  assert.equal(await page.evaluate(() => window.__transactions), 0, 'refused without opening a transaction');
  ok(await channel.save(page, JSON.stringify({ n: 3, filler: 'z'.repeat(31 * 1024 * 1024) })), '31 MiB checkpoint');
  // Within the limit by itself, but the envelope metadata pushes the record over it.
  failed(await channel.save(page, JSON.stringify({ n: 4, filler: 'w'.repeat(32 * 1024 * 1024 - 64) })), 'resource-limit', 'checkpoint plus envelope');
  ok(await channel.close(page));
});

test('journal: backup keeps the original bytes and never replaces them', async t => {
  const context = await t.newContext();
  const page = await t.openPage(context);
  const url = resource(t.urls, 'backup');
  ok(await journal.open(page, url, ALICE));
  ok(await journal.backup(page, 'pre-migration'), 'backup of an empty journal is a no-op');
  ok(await journal.initialize(page, JSON.stringify({ text: UNICODE })));
  ok(await journal.backup(page, 'pre-migration'));
  ok(await journal.free(page));
  let dump = await page.evaluate(() => window.raw.dump());
  assert.equal(dump.stores.backups.length, 1);
  const original = dump.stores.journals[0].value.payload;
  assert.equal(dump.stores.backups[0].value.payload.sha256, original.sha256, 'backup is byte-identical to the stored journal');
  assert.equal(dump.stores.backups[0].value.label, 'pre-migration');
  // Replace the journal as a migration would, then back up again under the same label.
  const [{ key, value }] = dump.stores.journals;
  const migrated = JSON.stringify({ resource_url: url, receive_frontier: [], local_frontier: [], outbox: [], application: { text: 'migrated' } });
  await page.evaluate(args => window.raw.write(args), { store: 'journals', key, value: { schema_version: 1, resource: value.resource, context: value.context }, payloadText: migrated });
  ok(await journal.open(page, url, ALICE));
  ok(await journal.backup(page, 'pre-migration'));
  ok(await journal.backup(page, 'second'));
  ok(await journal.free(page));
  dump = await page.evaluate(() => window.raw.dump());
  const byLabel = Object.fromEntries(dump.stores.backups.map(item => [item.value.label, item.value.payload.sha256]));
  assert.equal(byLabel['pre-migration'], original.sha256, 'the first backup is still the original');
  assert.notEqual(byLabel.second, original.sha256, 'a new label captured the migrated bytes');
  ok(await journal.open(page, url, ALICE));
  failed(await journal.backup(page, 'bad label!'), 'config', 'invalid backup label');
  ok(await journal.free(page));
});

test('journal: a database written by the previous schema is upgraded without touching its records', async t => {
  const context = await t.newContext();
  const page = await t.openPage(context);
  const url = resource(t.urls, 'upgrade');
  const contextId = await page.evaluate(c => window.raw.sha256Hex('braid-http/journal-context/v1\u0000' + c), ALICE);
  const checkpoint = JSON.stringify({ resource_url: url, receive_frontier: [], local_frontier: [], outbox: [], application: { kept: UNICODE } });
  await page.evaluate(args => window.raw.write(args), {
    store: 'journals', version: 1, key: JSON.stringify([contextId, url]),
    value: { schema_version: 1, resource: url, context: contextId }, payloadText: checkpoint,
  });
  let dump = await page.evaluate(() => window.raw.dump());
  assert.equal(dump.version, 1);
  assert.deepEqual(Object.keys(dump.stores), ['journals']);
  const before = dump.stores.journals[0].value.payload.sha256;
  ok(await journal.open(page, url, ALICE), 'open over a version 1 database');
  assert.deepEqual(ok(await journal.checkpoint(page)).application, { kept: UNICODE });
  ok(await journal.backup(page, 'after-upgrade'));
  ok(await journal.free(page));
  dump = await page.evaluate(() => window.raw.dump());
  assert.equal(dump.version, 2);
  assert.deepEqual(Object.keys(dump.stores).sort(), ['backups', 'journals']);
  assert.equal(dump.stores.journals[0].value.payload.sha256, before, 'record bytes unchanged by the upgrade');
});

test('journal: a record for another resource, context or schema is rejected, never adopted', async t => {
  const context = await t.newContext();
  const page = await t.openPage(context);
  const url = resource(t.urls, 'binding');
  ok(await journal.open(page, url, ALICE));
  ok(await journal.initialize(page, JSON.stringify({ n: 1 })));
  ok(await journal.free(page));
  const [{ key, value }] = (await page.evaluate(() => window.raw.dump())).stores.journals;
  const stage = (patch) => page.evaluate(args => window.raw.write(args), { store: 'journals', key, value: { ...value, payload: undefined, ...patch }, payloadText: '{}' });
  await stage({ resource: 'https://elsewhere.example/doc' });
  failed(await journal.open(page, url, ALICE), 'config', 'resource mismatch');
  await stage({ context: '0'.repeat(64) });
  failed(await journal.open(page, url, ALICE), 'config', 'context mismatch');
  await stage({ schema_version: 99 });
  failed(await journal.open(page, url, ALICE), 'config', 'unknown schema');
  // A corrupt checkpoint is an error that leaves the stored bytes alone.
  await page.evaluate(args => window.raw.write(args), { store: 'journals', key, value: { schema_version: 1, resource: value.resource, context: value.context }, payloadText: '{not json' });
  failed(await journal.open(page, url, ALICE), 'config', 'corrupt checkpoint');
  const after = (await page.evaluate(() => window.raw.dump())).stores.journals[0].value.payload;
  assert.equal(after.head, '{not json');
  // Every failed open released the lock; a valid record is adopted again.
  const valid = JSON.stringify({ resource_url: url, receive_frontier: [], local_frontier: [], outbox: [], application: { n: 7 } });
  await page.evaluate(args => window.raw.write(args), { store: 'journals', key, value: { schema_version: 1, resource: value.resource, context: value.context }, payloadText: valid });
  ok(await journal.open(page, url, ALICE), 'open after repairing the record');
  assert.deepEqual(ok(await journal.checkpoint(page)).application, { n: 7 });
  ok(await journal.free(page));
});
