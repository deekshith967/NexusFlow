/**
 * Real-PostgreSQL regression test for the deployment TLS posture.
 *
 * Render's internal connection string uses the private network, where
 * the server does not necessarily offer TLS. This suite pins the `pg`
 * client behavior that makes forcing TLS unsafe there: with `ssl`
 * configured, the client FAILS against a non-TLS server ("The server
 * does not support SSL connections") instead of falling back to
 * plaintext. That is why render.yaml deliberately omits DATABASE_SSL
 * (see README "Deploying to Render").
 */
import { Pool } from 'pg';

import { checkDatabaseReachable } from '../../src/database/migrations/applier';
import { startTestDb, type TestDb } from '../helpers/test-db';

describe('PostgreSQL TLS behavior against a non-TLS server', () => {
  let db: TestDb;
  const savedDatabaseSsl = process.env.DATABASE_SSL;

  beforeAll(async () => {
    db = await startTestDb();
  }, 120_000);

  afterAll(async () => {
    await db.stop();
  });

  afterEach(() => {
    if (savedDatabaseSsl === undefined) {
      delete process.env.DATABASE_SSL;
    } else {
      process.env.DATABASE_SSL = savedDatabaseSsl;
    }
  });

  it('connects without TLS (control: the server itself is healthy)', async () => {
    const pool = new Pool({ connectionString: db.connectionString, max: 1 });

    try {
      const result = await pool.query('SELECT 1 AS ok');

      expect(result.rows).toEqual([{ ok: 1 }]);
    } finally {
      await pool.end();
    }
  });

  it('fails closed — no plaintext fallback — when TLS is forced', async () => {
    const pool = new Pool({
      connectionString: db.connectionString,
      max: 1,
      ssl: { rejectUnauthorized: false },
    });

    try {
      await expect(pool.query('SELECT 1 AS ok')).rejects.toThrow(
        /does not support SSL/,
      );
    } finally {
      await pool.end();
    }
  });

  it('fails the boot reachability check when DATABASE_SSL=true against a non-TLS server', async () => {
    process.env.DATABASE_SSL = 'true';

    await expect(checkDatabaseReachable(db.connectionString)).rejects.toThrow(
      /does not support SSL/,
    );
  });
});
