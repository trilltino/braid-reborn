// Minimal test registry with per-engine accounting. A test is never skipped
// silently: `skip` carries a printed reason and counts separately.
import { chromium, firefox, webkit } from 'playwright-core';

export const engines = { chromium, firefox, webkit };
export const registry = [];

/** Register a test. `only` limits engines; `skip` is `{ engine: 'reason' }` and is reported. */
export function test(name, fn, options = {}) {
  registry.push({ name, fn, ...options });
}

export function engineLaunchOptions(engine) {
  return engine === 'firefox' ? {} : {};
}

export async function runAll({ selected, grep, log = console.log, makeContext }) {
  const summary = {};
  for (const engine of selected) {
    summary[engine] = { passed: 0, failed: 0, skipped: 0, failures: [], skips: [], passes: [], version: null };
    let browser;
    try { browser = await engines[engine].launch(engineLaunchOptions(engine)); }
    catch {
      summary[engine].failed++;
      summary[engine].failures.push({ name: 'engine launch', error: 'Browser engine unavailable; install the pinned runner engine before executing this gate' });
      log(`FAIL ${engine} engine launch`);
      continue;
    }
    const version = browser.version();
    summary[engine].version = version;
    log(`\n== ${engine} ${version} ==`);
    try {
      for (const entry of registry) {
        if (grep && !entry.name.includes(grep)) continue;
        if (entry.only && !entry.only.includes(engine)) {
          summary[engine].skipped++;
          summary[engine].skips.push({ name: entry.name, reason: 'Test profile explicitly targets other engines' });
          continue;
        }
        const outcome = summary[engine];
        const reason = entry.skip?.[engine];
        if (reason) {
          outcome.skipped++;
          outcome.skips.push({ name: entry.name, reason });
          log(`SKIP ${engine} ${entry.name} -- ${reason}`);
          continue;
        }
        const started = Date.now();
        const cleanup = [];
        const t = makeContext({ engine, browser, cleanup, entry });
        let timer;
        try {
          await Promise.race([
            entry.fn(t),
            new Promise((_, reject) => { timer = setTimeout(() => reject(new Error(`timed out after ${entry.timeout || 120000} ms`)), entry.timeout || 120000); }),
          ]);
          outcome.passed++;
          outcome.passes.push({ name: entry.name, elapsed_ms: Date.now() - started });
          log(`PASS ${engine} ${entry.name} (${Date.now() - started} ms)`);
        } catch (error) {
          outcome.failed++;
          outcome.failures.push({ name: entry.name, error: String(error && error.stack || error) });
          log(`FAIL ${engine} ${entry.name} (${Date.now() - started} ms)\n     ${String(error && error.message || error).split('\n').join('\n     ')}`);
        } finally {
          clearTimeout(timer);
          for (const close of cleanup.reverse()) { try { await close(); } catch { /* best effort */ } }
        }
      }
    } finally {
      await browser.close().catch(() => {});
    }
  }
  return summary;
}
