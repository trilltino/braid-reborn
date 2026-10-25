// Calls into the wasm harness from Node. Results are `{ ok }` or `{ error: { code, message } }`.
import assert from 'node:assert/strict';

export const channel = {
  open: (page, resource, context, slot = 'main') => page.evaluate(([r, c, s]) => window.attempt(async () => {
    window.slots[s] = await braid.ChannelHarness.open(r, c);
    return true;
  }), [resource, context, slot]),
  queue: (page, version, body, application = null, slot = 'main') => page.evaluate(([v, b, a, s]) => window.attempt(
    () => window.slots[s].queue(v, b, a)), [version, body, application, slot]),
  save: (page, application, slot = 'main') => page.evaluate(([a, s]) => window.attempt(
    () => window.slots[s].save_application(a)), [application, slot]),
  pending: async (page, slot = 'main') => {
    const result = await page.evaluate(s => window.attempt(() => window.slots[s].pending()), slot);
    return result.error ? result : { ok: JSON.parse(result.ok) };
  },
  close: (page, slot = 'main') => page.evaluate(s => window.attempt(async () => { await window.slots[s].close(); window.slots[s].free(); delete window.slots[s]; return true; }), slot),
};

export const journal = {
  open: (page, resource, context, slot = 'journal') => page.evaluate(([r, c, s]) => window.attempt(async () => {
    window.slots[s] = await braid.JournalHarness.open(r, c);
    return true;
  }), [resource, context, slot]),
  checkpoint: async (page, slot = 'journal') => {
    const result = await page.evaluate(s => window.attempt(() => window.slots[s].checkpoint()), slot);
    return result.error ? result : { ok: JSON.parse(result.ok) };
  },
  initialize: (page, application, slot = 'journal') => page.evaluate(([a, s]) => window.attempt(
    () => window.slots[s].initialize(a)), [application, slot]),
  backup: (page, label, slot = 'journal') => page.evaluate(([l, s]) => window.attempt(
    () => window.slots[s].backup_original(l)), [label, slot]),
  /** Dropping the Rust owner is the shutdown path for a journal-only handle. */
  free: (page, slot = 'journal') => page.evaluate(s => window.attempt(async () => {
    window.slots[s].free(); delete window.slots[s];
    // The Web Lock is released on a later turn, after the last transaction ends.
    await new Promise(r => setTimeout(r, 50));
    return true;
  }), slot),
};

export function ok(result, what = 'operation') {
  assert.ok(!result.error, `${what} failed: ${JSON.stringify(result.error)}`);
  return result.ok;
}
export function failed(result, code, what = 'operation') {
  assert.ok(result.error, `${what} unexpectedly succeeded`);
  assert.equal(result.error.code, code, `${what} failed with ${JSON.stringify(result.error)}`);
  return result.error;
}

export const resource = (urls, name = 'doc') => `${urls.app}/wiki/${name}.md`;

export const simpleton = {
  open: (page, resource, context, bearer, slot = 'text') => page.evaluate(([r, c, b, s]) => window.attempt(async () => {
    window.slots[s] = await braid.SimpletonHarness.open(r, c, b); return true;
  }), [resource, context, bearer, slot]),
  changed: (page, text, slot = 'text') => page.evaluate(([text, slot]) => window.attempt(() => window.slots[slot].changed(text)), [text, slot]),
  status: async (page, slot = 'text') => {
    const result = await page.evaluate(slot => window.attempt(() => window.slots[slot].status()), slot);
    return result.error ? result : { ok: JSON.parse(result.ok) };
  },
  close: (page, slot = 'text') => page.evaluate(slot => window.attempt(async () => {
    await window.slots[slot].close(); window.slots[slot].free(); delete window.slots[slot]; return true;
  }), slot),
};
