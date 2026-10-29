/**
 * Real PostgreSQL test database.
 *
 * PostgreSQL binaries come from the `embedded-postgres` npm package. The
 * server runs in a short-lived child process (the package is ESM and does
 * not load under Jest's CJS runtime); this helper only manages the child
 * and a connection pool. All repository migrations are applied so
 * integration tests exercise the actual schema and concurrency behavior.
 */
import { spawn } from 'node:child_process';
import * as path from 'node:path';

import { Pool } from 'pg';

import { applyMigrations } from '../../src/database/migrations/applier';

export interface TestDb {
  readonly pool: Pool;
  readonly connectionString: string;
  readonly port: number;
  stop(): Promise<void>;
}

interface ServerInfo {
  ok: boolean;
  port: number;
  database: string;
  connectionString: string;
}

export async function startTestDb(): Promise<TestDb> {
  const entry = path.join(__dirname, 'pg-server-entry.mjs');
  const child = spawn(process.execPath, [entry], {
    stdio: ['ignore', 'pipe', 'inherit'],
  });

  let stdout = '';
  let info: ServerInfo | null = null;
  // Set once stop() is called: the exit that follows is intentional.
  // (embedded-postgres installs its own exit hook, which stops the
  // server and exits with 143 after SIGTERM; that is a clean shutdown.)
  let stopping = false;

  child.stdout.on('data', (chunk: Buffer) => {
    stdout += chunk.toString();

    if (!info) {
      const line = stdout.split('\n').find((l) => l.trim().startsWith('{'));

      if (line) {
        try {
          info = JSON.parse(line) as ServerInfo;
        } catch {
          // keep waiting for the ready line
        }
      }
    }
  });

  const exited = new Promise<number>((resolve) => {
    child.once('exit', (code) => resolve(code ?? -1));
    child.once('error', () => resolve(-1));
  });

  // The child is expected to outlive this call; track exit for diagnostics.
  void exited.then((code) => {
    if (!stopping && code !== 0) {
      process.emitWarning(`embedded postgres child exited with code ${code}`);
    }
  });

  try {
    // Wait for the ready line (bounded).
    const deadline = Date.now() + 60_000;

    while (!info) {
      if (Date.now() > deadline) {
        child.kill('SIGKILL');
        throw new Error('timed out waiting for embedded postgres to start');
      }

      await new Promise((resolve) => setTimeout(resolve, 100));
    }
  } catch (error) {
    child.kill('SIGKILL');
    throw error;
  }

  const serverInfo = info as ServerInfo;
  const pool = new Pool({ connectionString: serverInfo.connectionString, max: 16 });

  try {
    await applyMigrations(serverInfo.connectionString);
  } catch (error) {
    await pool.end();
    child.kill('SIGKILL');
    throw error;
  }

  return {
    pool,
    connectionString: serverInfo.connectionString,
    port: serverInfo.port,
    stop: async () => {
      stopping = true;
      await pool.end();
      child.kill('SIGTERM');

      await new Promise<void>((resolve) => {
        const timer = setTimeout(() => {
          child.kill('SIGKILL');
          resolve();
        }, 10_000);
        child.once('exit', () => {
          clearTimeout(timer);
          resolve();
        });
      });
    },
  };
}
