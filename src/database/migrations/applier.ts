import { existsSync, readFileSync, readdirSync } from 'node:fs';
import * as path from 'node:path';
import { Pool, type QueryResultRow } from 'pg';

import { configurePgTypeParsers } from '../parsers';

export interface MigrationResult {
  readonly filename: string;
  readonly applied: boolean;
}

/**
 * Applies numbered SQL migrations from the migrations directory in lexical
 * order, tracking applied files in `schema_migrations`. Each migration runs
 * in its own transaction; a failure aborts the migration and leaves the
 * database unchanged for that file.
 */
export async function applyMigrations(
  connectionString: string,
  migrationsDir?: string,
): Promise<MigrationResult[]> {
  const dir = resolveMigrationsDir(migrationsDir);
  const files = readdirSync(dir)
    .filter((f) => f.endsWith('.sql'))
    .sort((a, b) => a.localeCompare(b));

  configurePgTypeParsers();
  const pool = new Pool({ connectionString, max: 1 });

  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS schema_migrations (
        filename TEXT PRIMARY KEY,
        applied_at TIMESTAMPTZ NOT NULL
      )
    `);

    const results: MigrationResult[] = [];

    for (const filename of files) {
      const already = await pool.query<{ filename: string }>(
        'SELECT filename FROM schema_migrations WHERE filename = $1',
        [filename],
      );

      if (already.rows.length > 0) {
        results.push({ filename, applied: false });
        continue;
      }

      const sql = readFileSync(path.join(dir, filename), 'utf8');
      const client = await pool.connect();

      try {
        await client.query('BEGIN');
        await client.query(sql);
        await client.query(
          'INSERT INTO schema_migrations (filename, applied_at) VALUES ($1, now())',
          [filename],
        );
        await client.query('COMMIT');
        results.push({ filename, applied: true });
      } catch (error) {
        await client.query('ROLLBACK');
        throw new Error(
          `Migration ${filename} failed: ${(error as Error).message}`,
        );
      } finally {
        client.release();
      }
    }

    return results;
  } finally {
    await pool.end();
  }
}

export async function checkDatabaseReachable(
  connectionString: string,
): Promise<string> {
  const pool = new Pool({ connectionString, max: 1 });

  try {
    const result = await pool.query<QueryResultRow>('SELECT 1 AS ok');

    if (result.rows.length === 0 || result.rows[0].ok !== 1) {
      throw new Error('unexpected response from database');
    }

    const version = await pool.query<QueryResultRow>('SELECT version()');

    return version.rows[0].version as string;
  } finally {
    await pool.end();
  }
}

function resolveMigrationsDir(migrationsDir?: string): string {
  const candidates = [
    migrationsDir,
    // Running from dist/ (compiled): migrations copied next to the applier.
    path.join(__dirname, 'migrations'),
    // Running from source (ts-node/tests): repository source layout.
    path.join(__dirname, 'migrations'),
    path.join(process.cwd(), 'src', 'database', 'migrations'),
  ].filter((c): c is string => typeof c === 'string');

  for (const candidate of candidates) {
    if (existsSync(candidate) && existsSync(path.join(candidate, '001_initial_workflow_definition.sql'))) {
      return candidate;
    }
  }

  throw new Error(`Migrations directory not found (tried: ${candidates.join(', ')})`);
}
