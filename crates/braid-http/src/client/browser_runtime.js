// Runtime adapters used by the Rust channel. No credentials enter the journal.
const MAX_RECORD = 32 * 1024 * 1024;
const MAX_ENVELOPE_OVERHEAD = 32 * 1024;
const MAX_RESOURCE = 8192;
const MAX_CONTEXT = 512;
const DB_NAME = "braid-http.channels";
// Version 1 held only "journals"; version 2 adds the write-once "backups" store.
// The upgrade creates what is missing and never rewrites an existing record.
const DB_VERSION = 2;
const ENVELOPE_SCHEMA = 1;
const OPEN_TIMEOUT_MS = 30000;
const LABEL = /^[A-Za-z0-9_-]{1,64}$/;

function failure(code) {
    const error = new Error("Braid browser operation failed");
    error.braidCode = code;
    return error;
}

/// A stable machine-readable reason. Messages never carry URLs, headers or payloads.
export function browserErrorCode(error) {
    if (typeof error?.braidCode === "string") return error.braidCode;
    switch (error?.name) {
        case "QuotaExceededError": return "quota";
        case "AbortError": return "aborted";
        case "VersionError": return "journal-schema";
        default: return "failed";
    }
}

/// Every required facility is checked here, before any identity is generated or
/// any storage is touched. `storage` adds IndexedDB and Web Locks.
export function browserCheckCapabilities(storage) {
    const g = globalThis;
    const required = [
        ["secure-context", g.isSecureContext === true],
        ["fetch", typeof g.fetch === "function"],
        ["abort-controller", typeof g.AbortController === "function"],
        ["readable-stream", typeof g.ReadableStream === "function"],
        ["crypto", typeof g.crypto?.getRandomValues === "function"],
        ["text-encoder", typeof g.TextEncoder === "function"],
        ["promise", typeof g.Promise === "function" && typeof g.Promise.resolve === "function"],
        ["microtasks", typeof g.queueMicrotask === "function"],
        ["timers", typeof g.setTimeout === "function" && typeof g.clearTimeout === "function"],
    ];
    if (storage) {
        required.push(
            ["crypto", typeof g.crypto?.subtle?.digest === "function"],
            ["indexed-db", typeof g.indexedDB?.open === "function"],
            ["web-locks", typeof g.navigator?.locks?.request === "function"],
        );
    }
    for (const [code, available] of required) if (!available) throw failure(code);
}

export function browserRandomBytes() {
    browserCheckCapabilities(false);
    try { return globalThis.crypto.getRandomValues(new Uint8Array(16)); }
    catch { throw failure("crypto"); }
}

async function sha256Hex(text) {
    const digest = await globalThis.crypto.subtle.digest("SHA-256", new TextEncoder().encode(text));
    return Array.from(new Uint8Array(digest), byte => byte.toString(16).padStart(2, "0")).join("");
}

function openDatabase(state) {
    return new Promise((resolve, reject) => {
        let settled = false;
        const finish = (settle, value) => {
            if (settled) return;
            settled = true;
            state.cancelOpen = null;
            settle(value);
        };
        state.cancelOpen = () => finish(reject, failure("aborted"));
        let request;
        try { request = indexedDB.open(DB_NAME, DB_VERSION); }
        catch (error) { finish(reject, error); return; }
        request.onupgradeneeded = () => {
            const upgrade = request.transaction;
            // A cancelled/blocked open may still reach an upgrade later. Never
            // let that abandoned request modify the database after ownership
            // has been released.
            if (settled || state.closed) { try { upgrade.abort(); } catch {} return; }
            state.transactions.add(upgrade);
            const terminal = () => { state.transactions.delete(upgrade); releaseIfIdle(state); };
            upgrade.addEventListener("complete", terminal, { once: true });
            upgrade.addEventListener("abort", terminal, { once: true });
            for (const name of ["journals", "backups"]) {
                if (!request.result.objectStoreNames.contains(name)) request.result.createObjectStore(name);
            }
        };
        request.onblocked = () => finish(reject, failure("database-blocked"));
        request.onerror = () => finish(reject, request.error);
        request.onsuccess = () => {
            // A result that arrives after cancellation or a block must not leak a connection.
            if (settled) request.result.close();
            else finish(resolve, request.result);
        };
    });
}

/// The Rust owner receives this handle before awaiting readiness, so cancelling
/// open() releases an acquired lock and closes a late database connection.
/// Ownership (the exclusive Web Lock) is acquired before the database is opened,
/// and the database is opened before any record is read.
export function browserJournalStart(resource, context) {
    browserCheckCapabilities(true);
    let parsed;
    try { parsed = new URL(resource); } catch { throw failure("journal-binding"); }
    if (!["http:", "https:"].includes(parsed.protocol) || parsed.username || parsed.password || parsed.hash ||
        resource.length > MAX_RESOURCE || !context || context.length > MAX_CONTEXT || /[\u0000-\u001f\u007f]/.test(context)) {
        throw failure("journal-binding");
    }
    const state = {
        resource, context, contextId: null, key: null, database: null,
        transactions: new Set(), closed: false, release: null, rejectReady: null,
        cancelOpen: null, timer: null, ready: null, stale: false,
    };
    const held = new Promise(resolve => { state.release = resolve; });
    state.ready = new Promise((resolve, reject) => {
        state.rejectReady = reject;
        state.timer = setTimeout(() => { reject(failure("timeout")); browserJournalClose(state); }, OPEN_TIMEOUT_MS);
        (async () => {
            // Only a digest of the authorization context is ever used as a name or stored.
            state.contextId = await sha256Hex("braid-http/journal-context/v1\u0000" + context);
            state.key = JSON.stringify([state.contextId, resource]);
            const lockName = "braid-http/journal/v1/" + await sha256Hex(state.key);
            if (state.closed) throw failure("aborted");
            await navigator.locks.request(lockName, { mode: "exclusive", ifAvailable: true }, async lock => {
                if (!lock) throw failure("journal-busy");
                if (state.closed) throw failure("aborted");
                try {
                    const database = await openDatabase(state);
                    if (state.closed) { database.close(); throw failure("aborted"); }
                    watch(state, database);
                    state.database = database;
                    clearTimeout(state.timer);
                    resolve();
                    await held;
                } finally {
                    state.database?.close();
                    state.database = null;
                }
            });
        })().then(() => {}, error => {
            // Also reached when the lock is lost after readiness (for example stolen):
            // the journal is then closed and every later write fails.
            clearTimeout(state.timer);
            reject(error);
            browserJournalClose(state);
        });
    });
    // Avoid an unhandled rejection when Rust drops the opening owner immediately.
    state.ready.catch(() => {});
    return state;
}

function watch(state, database) {
    database.onversionchange = () => browserJournalClose(state);
    // An engine may close a connection after a storage error (Firefox does after
    // QuotaExceededError). The next transaction then reopens it while the lock is held.
    database.onclose = () => { if (state.database === database) state.stale = true; };
}

async function reopen(state) {
    state.database?.close();
    state.database = null;
    const database = await openDatabase(state);
    if (state.closed) { database.close(); throw failure("aborted"); }
    watch(state, database);
    state.database = database;
    state.stale = false;
}

export function browserJournalReady(state) { return state.ready; }

function releaseIfIdle(state) {
    if (state.closed && state.transactions.size === 0) state.release();
}

export function browserJournalClose(state) {
    if (state.closed) return;
    state.closed = true;
    clearTimeout(state.timer);
    state.rejectReady(failure("aborted"));
    state.cancelOpen?.();
    for (const transaction of state.transactions) {
        try { transaction.abort(); } catch { /* A completing transaction owns its terminal event. */ }
    }
    releaseIfIdle(state);
}

/// Resolves only on the transaction's `complete` event, never on request success:
/// a candidate is durable (to the engine's durability level) only then. `run`
/// issues requests and reports the value to return through `set`.
async function transaction(state, storeName, mode, run) {
    if (state.closed || !state.database) throw failure("aborted");
    if (state.stale) await reopen(state);
    try {
        return await attemptTransaction(state, storeName, mode, run);
    } catch (error) {
        if (error?.notStarted !== true) throw error;
        // The connection was closing before the transaction began; nothing was written.
        await reopen(state);
        return attemptTransaction(state, storeName, mode, run);
    }
}

function attemptTransaction(state, storeName, mode, run) {
    return new Promise((resolve, reject) => {
        let tx;
        try {
            tx = mode === "readwrite"
                ? state.database.transaction(storeName, mode, { durability: "strict" })
                : state.database.transaction(storeName, mode);
        } catch (error) {
            if (error?.name === "InvalidStateError") error.notStarted = true;
            reject(error);
            return;
        }
        state.transactions.add(tx);
        let result, cause;
        const finish = () => { state.transactions.delete(tx); releaseIfIdle(state); };
        tx.oncomplete = () => { finish(); resolve(result); };
        tx.onabort = () => { finish(); reject(cause || tx.error || failure(state.closed ? "aborted" : "transaction-aborted")); };
        // A request error is not terminal: IndexedDB then dispatches abort.
        tx.onerror = () => { cause = cause || tx.error; };
        try {
            run(tx.objectStore(storeName), value => { result = value; });
        } catch (error) {
            cause = error;
            try { tx.abort(); } catch { finish(); reject(error); }
        }
    });
}

function ask(request, settle) {
    request.onsuccess = () => settle(request.result);
}

function checkEnvelopeSize(state, payload, label = "") {
    const metadata = new TextEncoder().encode(JSON.stringify({
        schema_version: ENVELOPE_SCHEMA, resource: state.resource, context: state.contextId, label,
    }));
    if (metadata.byteLength > MAX_ENVELOPE_OVERHEAD || metadata.byteLength + payload.byteLength > MAX_RECORD) {
        throw failure("journal-limit");
    }
}

export async function browserJournalLoad(state) {
    const envelope = await transaction(state, "journals", "readonly", (store, set) => ask(store.get(state.key), set));
    if (envelope === undefined) return undefined;
    if (envelope?.schema_version !== ENVELOPE_SCHEMA) throw failure("journal-schema");
    if (envelope.resource !== state.resource || envelope.context !== state.contextId) throw failure("journal-binding");
    if (!(envelope.payload instanceof Uint8Array)) throw failure("journal-schema");
    checkEnvelopeSize(state, envelope.payload);
    return envelope.payload;
}

export async function browserJournalStore(state, payload) {
    if (!(payload instanceof Uint8Array)) throw failure("journal-schema");
    // wasm-bindgen lends a view of linear memory; own a copy before any await.
    const bytes = payload.slice();
    checkEnvelopeSize(state, bytes);
    const envelope = { schema_version: ENVELOPE_SCHEMA, resource: state.resource, context: state.contextId, payload: bytes };
    await transaction(state, "journals", "readwrite", store => { store.put(envelope, state.key); });
}

/// Write-once copy of bytes about to be migrated. An existing backup is kept.
export async function browserJournalBackup(state, label, payload) {
    if (!LABEL.test(label) || !(payload instanceof Uint8Array)) throw failure("journal-schema");
    const bytes = payload.slice();
    checkEnvelopeSize(state, bytes, label);
    const key = JSON.stringify([state.contextId, state.resource, label]);
    const envelope = { schema_version: ENVELOPE_SCHEMA, resource: state.resource, context: state.contextId, label, payload: bytes };
    await transaction(state, "backups", "readwrite", store => {
        const probe = store.get(key);
        probe.onsuccess = () => { if (probe.result === undefined) store.put(envelope, key); };
    });
}

export function browserFetchStart(url, method, encodedHeaders, body) {
    browserCheckCapabilities(false);
    const controller = new AbortController();
    const state = { controller, reader: null, response: null, closed: false };
    const options = {
        method, headers: JSON.parse(encodedHeaders), redirect: "error",
        credentials: "same-origin", signal: controller.signal,
    };
    if (method !== "GET" && method !== "HEAD") options.body = body.slice();
    state.ready = fetch(url, options).then(response => {
        if (state.closed) throw failure("aborted");
        if (response.type === "opaque" || response.type === "opaqueredirect") throw failure("opaque-response");
        state.response = response;
        state.reader = response.body?.getReader() || null;
    });
    state.ready.catch(() => {});
    return state;
}

export function browserFetchReady(state) { return state.ready; }
export function browserFetchStatus(state) { return state.response.status; }
export function browserFetchHeaders(state) { return JSON.stringify(Object.fromEntries(state.response.headers.entries())); }
export async function browserFetchRead(state) {
    if (state.closed) throw failure("aborted");
    if (!state.reader) return undefined;
    const { done, value } = await state.reader.read();
    return done ? undefined : value;
}
export function browserFetchClose(state) {
    if (state.closed) return;
    state.closed = true;
    try { state.controller.abort(); } catch {}
    if (state.reader) {
        const reader = state.reader;
        try { reader.cancel().catch(() => {}).finally(() => { try { reader.releaseLock(); } catch {} }); }
        catch { try { reader.releaseLock(); } catch {} }
        state.reader = null;
    }
}
