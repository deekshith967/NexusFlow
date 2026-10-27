/**
 * Unit tests for migration-directory resolution (no database needed).
 * Actual migration application is exercised by every integration suite
 * via test/helpers/test-db.ts.
 */
import { existsSync } from 'node:fs';
import * as path from 'node:path';
import { Pool } from 'pg';

const mockQuery = jest.fn((sql: string) => {
  if (sql.includes('version()')) {
    return Promise.resolve({ rows: [{ version: 'PostgreSQL 15.1 (mock)' }] });
  }

  return Promise.resolve({ rows: [{ ok: 1 }] });
});
const mockEnd = jest.fn(() => Promise.resolve());

jest.mock('pg', () => ({
  Pool: jest.fn().mockImplementation(() => ({
    query: mockQuery,
    end: mockEnd,
  })),
}));

import {
  checkDatabaseReachable,
  resolveMigrationsDir,
} from '../../src/database/migrations/applier';

describe('resolveMigrationsDir', () => {
  it('resolves the repository migrations by default', () => {
    const dir = resolveMigrationsDir();

    expect(
      existsSync(path.join(dir, '001_initial_workflow_definition.sql')),
    ).toBe(true);
    expect(
      existsSync(path.join(dir, '002_execution_persistence.sql')),
    ).toBe(true);
  });

  it('honors an explicit migrations directory', () => {
    const explicit = path.join(
      process.cwd(),
      'src',
      'database',
      'migrations',
    );

    expect(resolveMigrationsDir(explicit)).toBe(explicit);
  });
});

describe('checkDatabaseReachable (mocked pg)', () => {
  const savedDatabaseSsl = process.env.DATABASE_SSL;

  beforeEach(() => {
    jest.clearAllMocks();
    delete process.env.DATABASE_SSL;
  });

  afterEach(() => {
    if (savedDatabaseSsl === undefined) {
      delete process.env.DATABASE_SSL;
    } else {
      process.env.DATABASE_SSL = savedDatabaseSsl;
    }
  });

  it('enables TLS when DATABASE_SSL=true', async () => {
    process.env.DATABASE_SSL = 'true';
    const url = 'postgres://user:pass@db.example.com:5432/nexusflow';

    await expect(checkDatabaseReachable(url)).resolves.toContain(
      'PostgreSQL 15.1',
    );
    expect(Pool).toHaveBeenCalledWith({
      connectionString: url,
      max: 1,
      ssl: { rejectUnauthorized: false },
    });
  });

  it('auto-enables TLS for sslmode=require URLs', async () => {
    const url =
      'postgres://user:pass@db.example.com:5432/nexusflow?sslmode=require';

    await checkDatabaseReachable(url);
    expect(Pool).toHaveBeenCalledWith({
      connectionString: url,
      max: 1,
      ssl: { rejectUnauthorized: false },
    });
  });

  it('leaves TLS off for plain local URLs', async () => {
    const url = 'postgres://user:pass@localhost:5432/nexusflow';

    await checkDatabaseReachable(url);
    expect(Pool).toHaveBeenCalledWith({
      connectionString: url,
      max: 1,
      ssl: undefined,
    });
  });
});
