// Source adapter contracts only. This fixture does not certify a browser engine
// or Rust/WASM compilation. IDB and Web Locks below are controlled test doubles.
import assert from 'node:assert/strict';
import test from 'node:test';
import { webcrypto } from 'node:crypto';
import * as runtime from '../../crates/braid-http/src/client/browser_runtime.js';

const turn = () => new Promise(resolve => setTimeout(resolve, 0));
const resource = 'https://example.test/doc';

class Transaction {
  constructor(database, mode) { this.database = database; this.mode = mode; this.aborted = false; this.candidate = null; }
  objectStore() {
    return {
      get: key => this.request(() => structuredClone(this.database.records.get(key))),
      put: (value, key) => {
        if (this.database.quota) throw new DOMException('Untrusted storage payload', 'QuotaExceededError');
        this.candidate = [key, structuredClone(value)];
        return this.request(() => key);
      },
    };
  }
  request(read) {
    const request = {};
    queueMicrotask(() => {
      if (this.aborted) return;
      request.result = read();
      request.onsuccess?.();
      if (this.mode === 'readonly') queueMicrotask(() => this.complete());
    });
    return request;
  }
  complete() {
    if (this.aborted) return;
    if (this.candidate) this.database.records.set(...this.candidate);
    this.oncomplete?.();
  }
  abort() {
    this.aborted = true;
    queueMicrotask(() => this.onabort?.());
  }
}

function environment() {
  const held = new Map();
  const database = {
    records: new Map(), transactions: [], quota: false, closed: 0,
    objectStoreNames: { contains: () => true },
    close() { this.closed++; },
    transaction(_store, mode) { const tx = new Transaction(this, mode); this.transactions.push(tx); return tx; },
  };
  const idb = {
    delayed: false, pending: [],
    open() {
      const request = { result: database };
      const opened = () => request.onsuccess?.();
      if (this.delayed) this.pending.push(opened); else queueMicrotask(opened);
      return request;
    },
  };
  const locks = {
    request(name, options, callback) {
      assert.deepEqual(options, { mode: 'exclusive', ifAvailable: true });
      const lock = held.has(name) ? null : { name };
      if (lock) held.set(name, lock);
      return Promise.resolve().then(() => callback(lock)).finally(() => { if (lock) held.delete(name); });
    },
  };
  const values = { isSecureContext: true, crypto: webcrypto, indexedDB: idb, navigator: { locks } };
  const original = new Map();
  for (const [key, value] of Object.entries(values)) {
    original.set(key, Object.getOwnPropertyDescriptor(globalThis, key));
    Object.defineProperty(globalThis, key, { value, configurable: true, writable: true });
  }
  return { held, database, idb, restore() {
    for (const [key, descriptor] of original) {
      if (descriptor) Object.defineProperty(globalThis, key, descriptor); else delete globalThis[key];
    }
  } };
}

async function opened(context = 'user:alice') {
  const journal = runtime.browserJournalStart(resource, context);
  await runtime.browserJournalReady(journal);
  return journal;
}

test('IDB request success does not acknowledge before transaction completion', async () => {
  const env = environment(); let journal;
  try {
    journal = await opened();
    let acknowledged = false;
    const write = runtime.browserJournalStore(journal, new Uint8Array([1, 2])).then(() => { acknowledged = true; });
    await turn();
    assert.equal(acknowledged, false);
    assert.equal(env.database.records.size, 0);
    env.database.transactions.at(-1).complete();
    await write;
    assert.equal(acknowledged, true);
    assert.deepEqual(await runtime.browserJournalLoad(journal), new Uint8Array([1, 2]));
  } finally { if (journal) runtime.browserJournalClose(journal); await turn(); env.restore(); }
});

test('busy writer fails, different authenticated context has an isolated key', async () => {
  const env = environment(); let first, second, busy;
  try {
    first = await opened();
    busy = runtime.browserJournalStart(resource, 'user:alice');
    await assert.rejects(runtime.browserJournalReady(busy), error => runtime.browserErrorCode(error) === 'journal-busy');
    second = await opened('user:bob');
    const write = runtime.browserJournalStore(first, new Uint8Array([7]));
    env.database.transactions.at(-1).complete(); await write;
    assert.equal(await runtime.browserJournalLoad(second), undefined);
    runtime.browserJournalClose(first); first = null; await turn();
    first = await opened();
    assert.deepEqual(await runtime.browserJournalLoad(first), new Uint8Array([7]));
  } finally { for (const journal of [first, second, busy]) if (journal) runtime.browserJournalClose(journal); await turn(); env.restore(); }
});

test('quota failure leaves previous whole record and does not acknowledge', async () => {
  const env = environment(); let journal;
  try {
    journal = await opened();
    const original = runtime.browserJournalStore(journal, new Uint8Array([4]));
    env.database.transactions.at(-1).complete(); await original;
    env.database.quota = true;
    await assert.rejects(runtime.browserJournalStore(journal, new Uint8Array([9])), error => runtime.browserErrorCode(error) === 'quota');
    assert.deepEqual(await runtime.browserJournalLoad(journal), new Uint8Array([4]));
  } finally { if (journal) runtime.browserJournalClose(journal); await turn(); env.restore(); }
});

test('closing an active write holds writer ownership until its abort event', async () => {
  const env = environment(); let journal;
  try {
    journal = await opened();
    const write = runtime.browserJournalStore(journal, new Uint8Array([9]));
    runtime.browserJournalClose(journal);
    assert.equal(env.held.size, 1);
    await assert.rejects(write, error => runtime.browserErrorCode(error) === 'aborted');
    await turn();
    assert.equal(env.held.size, 0);
    assert.equal(env.database.records.size, 0);
  } finally { if (journal) runtime.browserJournalClose(journal); await turn(); env.restore(); }
});

test('abandoned opening releases its lock and closes a late DB result', async () => {
  const env = environment(); let journal;
  try {
    env.idb.delayed = true;
    journal = runtime.browserJournalStart(resource, 'user:alice');
    await turn();
    const ready = runtime.browserJournalReady(journal);
    runtime.browserJournalClose(journal);
    await assert.rejects(ready, error => runtime.browserErrorCode(error) === 'aborted');
    await turn();
    assert.equal(env.held.size, 0);
    for (const opened of env.idb.pending) opened();
    assert.equal(env.database.closed, 1);
  } finally { if (journal) runtime.browserJournalClose(journal); await turn(); env.restore(); }
});

test('envelope metadata counts against the 32MiB bound before a transaction', async () => {
  const env = environment(); let journal;
  try {
    journal = await opened();
    await assert.rejects(runtime.browserJournalStore(journal, new Uint8Array(32 * 1024 * 1024)), error => runtime.browserErrorCode(error) === 'journal-limit');
    assert.equal(env.database.transactions.length, 0);
  } finally { if (journal) runtime.browserJournalClose(journal); await turn(); env.restore(); }
});

test('capabilities and journal identities fail explicitly before opening', () => {
  const env = environment();
  try {
    assert.throws(() => runtime.browserJournalStart('https://name:secret@example.test/doc', 'user:alice'), error => runtime.browserErrorCode(error) === 'journal-binding');
    globalThis.crypto = undefined;
    assert.throws(() => runtime.browserRandomBytes(), error => runtime.browserErrorCode(error) === 'crypto');
    assert.equal(env.database.transactions.length, 0);
    assert.equal(runtime.browserErrorCode(new Error('secret URL/header/payload')), 'failed');
  } finally { env.restore(); }
});

test('Fetch owns abort, reader cancellation and credential-safe redirect policy', async () => {
  const env = environment(); const originalFetch = globalThis.fetch; let owner;
  try {
    let options, canceled = 0;
    const stream = new ReadableStream({ cancel() { canceled++; } });
    globalThis.fetch = async (_url, init) => {
      options = init;
      return { type: 'basic', status: 209, headers: new Headers({ version: '"one"' }), body: stream };
    };
    const payload = new Uint8Array([1]);
    owner = runtime.browserFetchStart(resource, 'PUT', JSON.stringify({ authorization: 'Bearer private-token' }), payload);
    payload[0] = 9;
    await runtime.browserFetchReady(owner);
    assert.equal(options.redirect, 'error');
    assert.equal(options.credentials, 'same-origin');
    assert.deepEqual(options.body, new Uint8Array([1]));
    assert.equal(runtime.browserFetchStatus(owner), 209);
    runtime.browserFetchClose(owner);
    assert.equal(options.signal.aborted, true);
    await turn();
    assert.equal(canceled, 1);
    assert.equal(stream.locked, false);
  } finally { if (owner) runtime.browserFetchClose(owner); globalThis.fetch = originalFetch; env.restore(); }
});

test('dropping before response headers aborts the actual Fetch promise', async () => {
  const env = environment(); const originalFetch = globalThis.fetch; let owner;
  try {
    globalThis.fetch = (_url, options) => new Promise((_resolve, reject) => {
      options.signal.addEventListener('abort', () => reject(new DOMException('cancelled', 'AbortError')), { once: true });
    });
    owner = runtime.browserFetchStart(resource, 'GET', '{}', new Uint8Array());
    const ready = runtime.browserFetchReady(owner);
    runtime.browserFetchClose(owner);
    await assert.rejects(ready, error => runtime.browserErrorCode(error) === 'aborted');
  } finally { if (owner) runtime.browserFetchClose(owner); globalThis.fetch = originalFetch; env.restore(); }
});
