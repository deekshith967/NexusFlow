/**
 * Zero-prerequisite local demo launcher (run with: npm run demo:local).
 *
 * Boots a throwaway embedded PostgreSQL server (via the same launcher
 * the integration tests use), points DATABASE_URL at it, and runs the
 * executable end-to-end demo (`npm run demo`: role=all, in-memory
 * transport, real PostgreSQL). The database is ephemeral and deleted
 * afterwards. The first run downloads a PostgreSQL binary (~50 MB).
 *
 * The demo's own exit code is propagated; the database is stopped on
 * every path (success, demo failure, Ctrl-C).
 */
import { spawn } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const entry = path.join(ROOT, 'test', 'helpers', 'pg-server-entry.mjs');

const pg = spawn(process.execPath, [entry], {
  stdio: ['ignore', 'pipe', 'inherit'],
  cwd: ROOT,
});

let stdout = '';
let info = null;
pg.stdout.on('data', (chunk) => {
  stdout += chunk.toString();

  if (!info) {
    const line = stdout.split('\n').find((l) => l.trim().startsWith('{'));

    if (line) {
      try {
        info = JSON.parse(line);
      } catch {
        // keep waiting for the ready line
      }
    }
  }
});

const killPg = () => {
  try {
    pg.kill('SIGKILL');
  } catch {
    // already exited
  }
};
process.on('SIGINT', () => {
  killPg();
  process.exit(130);
});
process.on('SIGTERM', () => {
  killPg();
  process.exit(143);
});

const deadline = Date.now() + 120_000;

while (!info) {
  if (Date.now() > deadline) {
    killPg();
    console.error('demo:local: timed out waiting for embedded PostgreSQL');
    process.exit(1);
  }

  await new Promise((r) => setTimeout(r, 100));
}

if (!info.ok || !info.connectionString) {
  killPg();
  console.error('demo:local: embedded PostgreSQL did not report ready');
  process.exit(1);
}

console.log(
  `demo:local: throwaway PostgreSQL on 127.0.0.1:${info.port} (ephemeral, deleted afterwards)`,
);

const demo = spawn('npm', ['run', 'demo'], {
  stdio: 'inherit',
  cwd: ROOT,
  env: { ...process.env, DATABASE_URL: info.connectionString },
  shell: process.platform === 'win32',
});

const code = await new Promise((resolve) => {
  demo.on('exit', (exitCode) => resolve(exitCode ?? 1));
  demo.on('error', () => resolve(1));
});

killPg();
process.exit(code);
