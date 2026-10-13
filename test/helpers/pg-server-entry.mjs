/**
 * Child-process entry point that boots a real embedded PostgreSQL server
 * for integration tests. Runs outside Jest (Node ESM) and prints a single
 * JSON line with the connection details once ready. SIGTERM shuts it down.
 */
import { mkdtempSync, rmSync } from 'node:fs';
import net from 'node:net';
import os from 'node:os';

import EmbeddedPostgres from 'embedded-postgres';

async function freePort() {
  const server = net.createServer();

  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => resolve());
  });

  const address = server.address();
  const port = address && typeof address === 'object' ? address.port : null;

  await new Promise((resolve) => server.close(() => resolve()));

  if (!port) {
    throw new Error('could not allocate a free port');
  }

  return port;
}

const port = await freePort();
const databaseDir = mkdtempSync(`${os.tmpdir()}/nexusflow-pg-`);
const user = 'nexusflow';
const password = 'nexusflow';
const database = 'nexusflow_test';

const embedded = new EmbeddedPostgres({
  databaseDir,
  user,
  password,
  port,
  persistent: false,
});

let stopping = false;

async function shutdown(code) {
  if (stopping) {
    return;
  }

  stopping = true;

  try {
    await embedded.stop({ force: true });
  } catch {
    // ignore
  }

  rmSync(databaseDir, { recursive: true, force: true });
  process.exit(code);
}

process.on('SIGTERM', () => {
  void shutdown(0);
});
process.on('SIGINT', () => {
  void shutdown(0);
});
process.on('uncaughtException', (error) => {
  console.error(error);
  void shutdown(1);
});

try {
  await embedded.initialise();
  await embedded.start();
  await embedded.createDatabase(database);
} catch (error) {
  console.error(error);
  await shutdown(1);
}

console.log(
  JSON.stringify({
    ok: true,
    port,
    database,
    connectionString: `postgres://${user}:${password}@127.0.0.1:${port}/${database}`,
  }),
);
