// A disposable cache directory must never cause Git to mutate the parent repo.
import { existsSync, mkdirSync, mkdtempSync, realpathSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import path from 'node:path';

export function checkoutUpstream(directory, remote, revision) {
  const environment = { ...process.env };
  for (const key of ['GIT_DIR', 'GIT_WORK_TREE', 'GIT_COMMON_DIR', 'GIT_INDEX_FILE',
    'GIT_OBJECT_DIRECTORY', 'GIT_ALTERNATE_OBJECT_DIRECTORIES']) delete environment[key];
  function git(args, probe = false) {
    const result = spawnSync('git', args, { env: environment, encoding: 'utf8',
      stdio: ['ignore', 'pipe', probe ? 'pipe' : 'inherit'] });
    if (!probe && result.status !== 0) throw new Error(result.error || `git ${args[0]} exited ${result.status}`);
    return result;
  }
  function ownsRepository(directory) {
    if (!existsSync(directory)) return false;
    const result = git(['-C', directory, 'rev-parse', '--show-toplevel'], true);
    if (result.status !== 0) return false;
    try {
      const normalize = value => process.platform === 'win32' ? value.toLowerCase() : value;
      return normalize(realpathSync(result.stdout.trim())) === normalize(realpathSync(directory));
    } catch (error) {
      if (error.code === 'ENOENT') return false;
      throw error;
    }
  }
  let source = path.resolve(directory);
  const clean = ownsRepository(source) && git(['-C', source, 'diff', '--quiet', 'HEAD', '--', '.',
    ':(exclude)package.json', ':(exclude)package-lock.json'], true).status === 0;
  if (!clean) {
    mkdirSync(path.dirname(source), { recursive: true });
    // Preserve an incomplete cache; a fresh isolated clone avoids both deletion
    // and accidental repository discovery through the parent working directory.
    source = mkdtempSync(path.join(path.dirname(source), path.basename(source) + '-'));
    git(['clone', '--no-checkout', remote, source]);
  }
  if (!ownsRepository(source)) throw new Error('Upstream checkout is not an isolated repository');
  git(['-C', source, 'fetch', '--refetch', '--depth=1', remote, revision]);
  git(['-C', source, 'checkout', '--detach', revision]);
  const actual = git(['-C', source, 'rev-parse', 'HEAD']).stdout.trim();
  if (actual !== revision) throw new Error(`Upstream revision mismatch: ${actual}`);
  return source;
}
