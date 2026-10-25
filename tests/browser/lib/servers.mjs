// Static server for the harness page and its wasm package. Everything else the
// suites need (the application routes, fault injection) lives in other servers.
import http from 'node:http';
import { readFile } from 'node:fs/promises';
import { dirname, extname, isAbsolute, join, normalize, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

export const browserRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const types = { '.js': 'text/javascript', '.mjs': 'text/javascript', '.wasm': 'application/wasm', '.html': 'text/html', '.json': 'application/json' };

export function ports() {
  const base = Number(process.env.BRAID_BROWSER_PORT_BASE || 47100);
  return { page: base + 1, api: base + 2, fault: base + 3, tls: base + 4, other: base + 5 };
}

export async function startPageServer(port, { host = '127.0.0.1' } = {}) {
  const server = http.createServer(async (request, response) => {
    const path = new URL(request.url, 'http://x').pathname;
    const target = path === '/' ? '/fixtures/index.html'
      : path.startsWith('/pkg/') ? '/dist' + path
      : path;
    const file = normalize(join(browserRoot, target));
    const confined = relative(browserRoot, file);
    if (isAbsolute(confined) || confined === '..' || confined.startsWith('../') || confined.startsWith('..\\') || confined.split(/[\\/]/).includes('node_modules')) { response.statusCode = 403; response.end(); return; }
    try {
      const body = await readFile(file);
      response.setHeader('content-type', types[extname(file)] || 'application/octet-stream');
      response.setHeader('cache-control', 'no-store');
      response.end(body);
    } catch { response.statusCode = 404; response.end('not found'); }
  });
  await new Promise((done, fail) => { server.once('error', fail); server.listen(port, host, done); });
  return { server, close: () => new Promise(done => { server.closeAllConnections?.(); server.close(done); }) };
}
