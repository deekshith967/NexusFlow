/**
 * Unit tests for environment configuration parsing (no database needed):
 * documented defaults, required values, role/transport validation, the
 * in-memory transport guard for separate processes, UUID and tuning
 * validation, and DATABASE_SSL explicit/auto-detect resolution.
 */
import { loadEnvConfig, resolveDatabaseSsl } from '../../src/config/env';

function baseEnv(): NodeJS.ProcessEnv {
  return { DATABASE_URL: 'postgres://user:pass@localhost:5432/nexusflow' };
}

describe('loadEnvConfig', () => {
  it('applies documented defaults', () => {
    const config = loadEnvConfig(baseEnv());

    expect(config.role).toBe('api');
    expect(config.transport).toBe('kafka');
    expect(config.databaseUrl).toBe(
      'postgres://user:pass@localhost:5432/nexusflow',
    );
    expect(config.databaseSsl).toBe(false);
    expect(config.port).toBe(3000);
    expect(config.defaultTenantId).toBe(
      '00000000-0000-0000-0000-000000000000',
    );
    expect(config.serviceId).toMatch(/^svc-/);
    expect(config.kafka.bootstrapServers).toEqual(['localhost:9092']);
    expect(config.kafka.dispatchTopic).toBe('nexusflow.task.dispatch');
    expect(config.scheduler.pollIntervalMs).toBe(250);
    expect(config.relay.publishTimeoutMs).toBe(30_000);
  });

  it('requires DATABASE_URL', () => {
    expect(() => loadEnvConfig({})).toThrow(
      'DATABASE_URL is required for PostgreSQL access',
    );
    expect(() => loadEnvConfig({ DATABASE_URL: '   ' })).toThrow(
      'DATABASE_URL is required for PostgreSQL access',
    );
  });

  it('rejects invalid roles and transports', () => {
    expect(() =>
      loadEnvConfig({ ...baseEnv(), NEXUSFLOW_ROLE: 'bogus' }),
    ).toThrow('NEXUSFLOW_ROLE must be one of');
    expect(() =>
      loadEnvConfig({ ...baseEnv(), NEXUSFLOW_TRANSPORT: 'bogus' }),
    ).toThrow('NEXUSFLOW_TRANSPORT must be');
  });

  it('rejects the in-memory transport for separate worker and outbox-relay processes', () => {
    expect(() =>
      loadEnvConfig({
        ...baseEnv(),
        NEXUSFLOW_ROLE: 'worker',
        NEXUSFLOW_TRANSPORT: 'memory',
      }),
    ).toThrow('separate worker and outbox-relay processes require Kafka');
    expect(() =>
      loadEnvConfig({
        ...baseEnv(),
        NEXUSFLOW_ROLE: 'outbox-relay',
        NEXUSFLOW_TRANSPORT: 'memory',
      }),
    ).toThrow('separate worker and outbox-relay processes require Kafka');
  });

  it('allows the in-memory transport for single-process roles', () => {
    for (const role of ['api', 'all', 'scheduler']) {
      const config = loadEnvConfig({
        ...baseEnv(),
        NEXUSFLOW_ROLE: role,
        NEXUSFLOW_TRANSPORT: 'memory',
      });

      expect(config.transport).toBe('memory');
    }
  });

  it('validates the default tenant UUID', () => {
    expect(() =>
      loadEnvConfig({ ...baseEnv(), NEXUSFLOW_DEFAULT_TENANT_ID: 'nope' }),
    ).toThrow('NEXUSFLOW_DEFAULT_TENANT_ID must be a valid UUID');

    const custom = '12345678-1234-4234-8234-123456789012';
    expect(
      loadEnvConfig({ ...baseEnv(), NEXUSFLOW_DEFAULT_TENANT_ID: custom })
        .defaultTenantId,
    ).toBe(custom);
  });

  it('validates positive-integer tuning values', () => {
    expect(() => loadEnvConfig({ ...baseEnv(), PORT: 'abc' })).toThrow(
      'PORT must be a positive integer',
    );
    expect(() =>
      loadEnvConfig({ ...baseEnv(), SCHEDULER_POLL_MS: '0' }),
    ).toThrow('SCHEDULER_POLL_MS must be a positive integer');

    const config = loadEnvConfig({
      ...baseEnv(),
      PORT: '4001',
      RELAY_BATCH_LIMIT: '5',
    });
    expect(config.port).toBe(4001);
    expect(config.relay.batchLimit).toBe(5);
  });

  it('parses explicit DATABASE_SSL', () => {
    expect(
      loadEnvConfig({ ...baseEnv(), DATABASE_SSL: 'true' }).databaseSsl,
    ).toBe(true);
    expect(
      loadEnvConfig({ ...baseEnv(), DATABASE_SSL: 'false' }).databaseSsl,
    ).toBe(false);
    expect(
      loadEnvConfig({ ...baseEnv(), DATABASE_SSL: 'yes' }).databaseSsl,
    ).toBe(true);
    expect(() => loadEnvConfig({ ...baseEnv(), DATABASE_SSL: 'maybe' })).toThrow(
      "DATABASE_SSL must be 'true' or 'false'",
    );
  });
});

describe('resolveDatabaseSsl', () => {
  it('auto-detects TLS from sslmode/ssl URL parameters', () => {
    expect(
      resolveDatabaseSsl(
        'postgres://u:p@host:5432/db?sslmode=require',
        {},
      ),
    ).toBe(true);
    expect(
      resolveDatabaseSsl('postgres://u:p@host:5432/db?ssl=true', {}),
    ).toBe(true);
    expect(
      resolveDatabaseSsl(
        'postgres://u:p@host:5432/db?sslmode=disable',
        {},
      ),
    ).toBe(false);
    expect(resolveDatabaseSsl('postgres://u:p@host:5432/db', {})).toBe(false);
  });

  it('lets explicit DATABASE_SSL override URL auto-detection', () => {
    expect(
      resolveDatabaseSsl('postgres://u:p@host:5432/db', {
        DATABASE_SSL: 'true',
      }),
    ).toBe(true);
    expect(
      resolveDatabaseSsl('postgres://u:p@host:5432/db?sslmode=require', {
        DATABASE_SSL: 'false',
      }),
    ).toBe(false);
  });
});

describe('resolveDatabaseSsl Render URL shapes', () => {
  // Render's internal connection string (injected by fromDatabase)
  // carries no TLS parameters, and the private network does not
  // require TLS — so TLS must stay OFF by default. Forcing it makes
  // every service fail at boot (the pg client has no plaintext
  // fallback; see test/database/pg-tls-behavior.spec.ts). That is why
  // render.yaml deliberately omits DATABASE_SSL.
  const internalUrl =
    'postgres://nexusflow:secret@dpg-abc123-a.oregon-postgres.render.com:5432/nexusflow';

  it('leaves TLS off for Render internal URLs without an override', () => {
    expect(resolveDatabaseSsl(internalUrl, {})).toBe(false);
    expect(loadEnvConfig({ DATABASE_URL: internalUrl }).databaseSsl).toBe(
      false,
    );
  });

  it('lets an explicit override win over the internal URL shape', () => {
    expect(
      resolveDatabaseSsl(internalUrl, { DATABASE_SSL: 'true' }),
    ).toBe(true);
    expect(
      loadEnvConfig({ DATABASE_URL: internalUrl, DATABASE_SSL: 'false' })
        .databaseSsl,
    ).toBe(false);
  });

  it('resolves every documented sslmode/ssl variant', () => {
    for (const mode of ['require', 'prefer', 'verify-ca', 'verify-full']) {
      expect(
        resolveDatabaseSsl(`postgres://u:p@host:5432/db?sslmode=${mode}`, {}),
      ).toBe(true);
    }

    for (const value of ['true', '1', 'require']) {
      expect(
        resolveDatabaseSsl(`postgres://u:p@host:5432/db?ssl=${value}`, {}),
      ).toBe(true);
    }

    expect(
      resolveDatabaseSsl('postgres://u:p@host:5432/db?sslmode=disable', {}),
    ).toBe(false);
  });

  it('keeps loadEnvConfig and resolveDatabaseSsl consistent', () => {
    const cases: Array<{ url: string; ssl?: string }> = [
      { url: 'postgres://u:p@host:5432/db' },
      { url: 'postgres://u:p@host:5432/db?sslmode=require' },
      { url: internalUrl },
      { url: 'postgres://u:p@host:5432/db', ssl: 'true' },
      { url: 'postgres://u:p@host:5432/db?sslmode=require', ssl: 'false' },
    ];

    for (const { url, ssl } of cases) {
      const env: NodeJS.ProcessEnv = { DATABASE_URL: url };

      if (ssl !== undefined) {
        env.DATABASE_SSL = ssl;
      }

      // loadEnvConfig (app pool) and resolveDatabaseSsl (migrate +
      // boot reachability check) must always agree.
      expect(loadEnvConfig(env).databaseSsl).toBe(
        resolveDatabaseSsl(url, env),
      );
    }
  });
});
