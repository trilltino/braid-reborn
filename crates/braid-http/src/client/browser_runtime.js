// Runtime adapters used by the Rust channel. No credentials enter the journal.
const MAX_RECORD = 32 * 1024 * 1024;
const MAX_ENVELOPE_OVERHEAD = 32 * 1024;

function failure(code) {
    const error = new Error("Braid browser operation failed");
    error.braidCode = code;
    return error;
}

export function browserErrorCode(error) {
    if (typeof error?.braidCode === "string") return error.braidCode;
    if (error?.name === "QuotaExceededError") return "quota";
    if (error?.name === "AbortError") return "aborted";
    return "failed";
}

export function browserCheckCapabilities(storage) {
    const required = [
        ["secure-context", globalThis.isSecureContext === true],
        ["fetch", typeof globalThis.fetch === "function"],
        ["abort-controller", typeof globalThis.AbortController === "function"],
        ["readable-stream", typeof globalThis.ReadableStream === "function"],
        ["crypto", typeof globalThis.crypto?.getRandomValues === "function"],
        ["text-encoder", typeof globalThis.TextEncoder === "function"],
        ["timers", typeof globalThis.setTimeout === "function" && typeof globalThis.clearTimeout === "function"],
    ];
    if (storage) required.push(
        ["indexed-db", typeof globalThis.indexedDB?.open === "function"],
        ["web-locks", typeof globalThis.navigator?.locks?.request === "function"],
    );
    for (const [code, available] of required) if (!available) throw failure(code);
}

export function browserRandomBytes() {
    browserCheckCapabilities(false);
    try { return globalThis.crypto.getRandomValues(new Uint8Array(16)); }
    catch { throw failure("crypto"); }
}

// The Rust owner receives this handle before awaiting readiness, so cancelling
// open() can release an acquired lock and close a late database connection.
export function browserJournalStart(resource, context) {
    browserCheckCapabilities(true);
    let parsed;
    try { parsed = new URL(resource); } catch { throw failure("journal-binding"); }
    if (!['http:', 'https:'].includes(parsed.protocol) || parsed.username || parsed.password || parsed.hash ||
        parsed.href !== resource || resource.length > 8192 || !context || context.length > 512 || /[\u0000-\u001f\u007f]/.test(context)) throw failure("journal-binding");
    const state = {
        resource, context, database: null, transactions: new Set(), closed: false,
        release: null, rejectReady: null, cancelOpen: null, timer: null,
    };
    const held = new Promise(resolve => { state.release = resolve; });
    state.ready = new Promise((resolve, reject) => {
        state.rejectReady = reject;
        const lockName = "braid-http/journal/" + JSON.stringify([context, resource]);
        state.lockTask = navigator.locks.request(lockName, { mode: "exclusive", ifAvailable: true }, async lock => {
            if (!lock) throw failure("journal-busy");
            if (state.closed) throw failure("aborted");
            const database = await new Promise((opened, failed) => {
                let request;
                state.cancelOpen = () => {
                    try { request?.transaction?.abort(); } catch {}
                    failed(failure("aborted"));
                };
                try { request = indexedDB.open("braid-http.channels", 1); }
                catch (error) { failed(error); return; }
                request.onupgradeneeded = () => {
                    if (state.closed) { request.transaction.abort(); return; }
                    if (!request.result.objectStoreNames.contains("journals")) request.result.createObjectStore("journals");
                };
                request.onblocked = () => failed(failure("database-blocked"));
                request.onerror = () => failed(request.error);
                request.onsuccess = () => {
                    if (state.closed) { request.result.close(); failed(failure("aborted")); }
                    else opened(request.result);
                };
            });
            if (state.closed) { database.close(); throw failure("aborted"); }
            state.cancelOpen = null;
            state.database = database;
            database.onversionchange = () => browserJournalClose(state);
            clearTimeout(state.timer);
            resolve();
            await held;
            database.close();
        }).catch(error => {
            clearTimeout(state.timer);
            state.closed = true;
            state.database?.close();
            reject(error);
        });
    });
    state.timer = setTimeout(() => {
        state.rejectReady(failure("timeout"));
        browserJournalClose(state);
    }, 10000);
    // Avoid an unhandled rejection when Rust drops the opening owner immediately.
    state.ready.catch(() => {});
    return state;
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

function transaction(state, mode, run) {
    if (state.closed || !state.database) return Promise.reject(failure("aborted"));
    return new Promise((resolve, reject) => {
        let tx;
        try { tx = state.database.transaction("journals", mode); }
        catch (error) { reject(error); return; }
        state.transactions.add(tx);
        let result, requestError;
        const terminal = () => { state.transactions.delete(tx); releaseIfIdle(state); };
        tx.oncomplete = () => { terminal(); resolve(result); };
        tx.onabort = () => { terminal(); reject(requestError || tx.error || failure("aborted")); };
        // onerror is not terminal: IndexedDB dispatches abort or complete later.
        tx.onerror = () => { requestError = tx.error || failure("transaction-failed"); };
        try {
            const request = run(tx.objectStore("journals"), JSON.stringify([state.context, state.resource]));
            request.onsuccess = () => { result = request.result; };
            request.onerror = () => { requestError = request.error; };
        } catch (error) {
            requestError = error;
            try { tx.abort(); } catch { terminal(); reject(error); }
        }
    });
}

export async function browserJournalLoad(state) {
    const envelope = await transaction(state, "readonly", (store, key) => store.get(key));
    if (envelope === undefined) return undefined;
    if (envelope?.schema_version !== 1) throw failure("journal-schema");
    if (envelope.resource !== state.resource || envelope.context !== state.context) throw failure("journal-binding");
    if (!(envelope.payload instanceof Uint8Array)) throw failure("journal-schema");
    checkEnvelopeSize(state, envelope.payload);
    return envelope.payload;
}

export async function browserJournalStore(state, payload) {
    if (!(payload instanceof Uint8Array)) throw failure("journal-schema");
    checkEnvelopeSize(state, payload);
    const envelope = { schema_version: 1, resource: state.resource, context: state.context, payload };
    await transaction(state, "readwrite", (store, key) => store.put(envelope, key));
}

function checkEnvelopeSize(state, payload) {
    const metadata = new TextEncoder().encode(JSON.stringify({ schema_version: 1, resource: state.resource, context: state.context }));
    if (metadata.byteLength > MAX_ENVELOPE_OVERHEAD || metadata.byteLength + payload.byteLength > MAX_RECORD) throw failure("journal-limit");
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
