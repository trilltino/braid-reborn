// Per-test helpers: contexts, pages with the loaded wasm harness, engine switches.
import { engines } from './runner.mjs';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { isAbsolute, join, relative, resolve } from 'node:path';

export function makeContextFactory({ urls, fixture }) {
  return ({ engine, browser, cleanup }) => {
    const t = {
      engine, browser, urls, fixture,
      async launchBrowser(options = {}) {
        const launched = await engines[engine].launch(options);
        cleanup.push(() => launched.close());
        return launched;
      },
      async newContext(options = {}, from = browser) {
        const context = await from.newContext(options);
        cleanup.push(() => context.close());
        return context;
      },
      /** A page with the production wasm harness initialised; resolves once it is ready. */
      async openPage(context, origin = urls.app) {
        const page = await context.newPage();
        page.pageErrors = [];
        page.on('pageerror', error => page.pageErrors.push(String(error)));
        await page.goto(origin + '/');
        await page.waitForFunction(() => window.__ready || window.__initError, null, { timeout: 30000 });
        const failure = await page.evaluate(() => window.__initError);
        if (failure) throw new Error('harness failed to initialise: ' + failure);
        return page;
      },
      /** A real persistent profile: storage survives closing and relaunching the browser. */
      async persistentProfile(options = {}) {
        const dir = await mkdtemp(join(tmpdir(), `braid-profile-${engine}-`));
        cleanup.push(() => {
          const scoped = relative(resolve(tmpdir()), resolve(dir));
          if (!scoped || isAbsolute(scoped) || scoped === '..' || scoped.startsWith('../') || scoped.startsWith('..\\') || !scoped.startsWith(`braid-profile-${engine}-`)) throw new Error('Invalid generated profile cleanup scope');
          return rm(dir, { recursive: true, force: true, maxRetries: 5, retryDelay: 200 });
        });
        const profile = {
          dir,
          async launch() {
            const context = await engines[engine].launchPersistentContext(dir, options);
            profile.current = context;
            return context;
          },
          async close() { if (profile.current) { const c = profile.current; profile.current = null; await c.close(); } },
        };
        cleanup.push(() => profile.close());
        return profile;
      },
    };
    return t;
  };
}
