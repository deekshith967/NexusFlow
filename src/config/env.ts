export type AppRole = 'api' | 'scheduler' | 'worker' | 'outbox-relay' | 'all';
export type TransportKind = 'kafka' | 'memory';

export interface KafkaSaslConfig {
  readonly mechanism: 'plain' | 'scram-sha-256' | 'scram-sha-512';
  readonly username: string;
  readonly password: string;
}

export interface KafkaConfig {
  readonly bootstrapServers: string[];
  readonly dispatchTopic: string;
  readonly lifecycleTopic: string;
  readonly consumerGroup: string;
  readonly clientId: string;
  /**
   * Whether the Kafka client uses TLS. Required (true) whenever SASL
   * credentials are configured — SASL over plaintext would expose the
   * password on the wire. Managed brokers (Confluent, Upstash,
   * Redpanda) require TLS.
   */
  readonly ssl: boolean;
  /**
   * SASL credentials for brokers that require authentication. Absent
   * for plaintext/local brokers without auth.
   */
  readonly sasl?: KafkaSaslConfig;
}

export interface ApiKeyConfig {
  readonly name: string;
  readonly secret: string;
  readonly tenantId: string;
}

export interface EnvConfig {
  readonly role: AppRole;
  readonly transport: TransportKind;
  readonly databaseUrl: string;
  /**
   * Whether the PostgreSQL connection uses TLS. Explicit
   * `DATABASE_SSL=true/false` wins; otherwise auto-detected from
   * `sslmode`/`ssl` parameters in DATABASE_URL. External managed
   * endpoints commonly mandate TLS (Render requires it on external
   * connections); Render's internal connection string uses the
   * private network where TLS is optional, so leave DATABASE_SSL
   * unset for the internal URL.
   */
  readonly databaseSsl: boolean;
  /**
   * API keys accepted by the HTTP control plane (`NEXUSFLOW_API_KEYS`
   * entries). Each key binds its bearer to exactly one tenant
   * server-side; the `X-Tenant-Id` header is never trusted as
   * identity. At least one key is required for the api/all roles.
   * Secrets must never be logged.
   */
  readonly apiKeys: readonly ApiKeyConfig[];
  readonly kafka: KafkaConfig;
  readonly port: number;
  readonly defaultTenantId: string;
  readonly serviceId: string;
  readonly scheduler: {
    readonly pollIntervalMs: number;
    readonly batchLimit: number;
    readonly leaseMs: number;
  };
  readonly worker: {
    readonly consumerGroup: string;
  };
  readonly relay: {
    readonly pollIntervalMs: number;
    readonly batchLimit: number;
    readonly claimTtlMs: number;
    readonly publishTimeoutMs: number;
  };
}

const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

function parsePositiveInt(
  value: string | undefined,
  fallback: number,
  name: string,
): number {
  if (value === undefined || value.trim() === '') {
    return fallback;
  }

  const parsed = Number(value);

  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`${name} must be a positive integer, got '${value}'`);
  }

  return parsed;
}

function parseRole(value: string | undefined): AppRole {
  const role = (value ?? 'api').trim();

  if (!['api', 'scheduler', 'worker', 'outbox-relay', 'all'].includes(role)) {
    throw new Error(
      `NEXUSFLOW_ROLE must be one of api|scheduler|worker|outbox-relay|all, got '${role}'`,
    );
  }

  return role as AppRole;
}

/**
 * Resolves whether a PostgreSQL connection string needs TLS
 * (explicit DATABASE_SSL override, else sslmode/ssl auto-detect).
 * Used by code paths that only carry the connection string (migration
 * applier, startup reachability check).
 */
export function resolveDatabaseSsl(
  databaseUrl: string,
  env: NodeJS.ProcessEnv = process.env,
): boolean {
  return parseDatabaseSsl(env.DATABASE_SSL, databaseUrl);
}

function parseDatabaseSsl(
  value: string | undefined,
  databaseUrl: string,
): boolean {
  if (value !== undefined && value.trim() !== '') {
    const normalized = value.trim().toLowerCase();

    if (normalized === 'true' || normalized === '1' || normalized === 'yes') {
      return true;
    }

    if (normalized === 'false' || normalized === '0' || normalized === 'no') {
      return false;
    }

    throw new Error(`DATABASE_SSL must be 'true' or 'false', got '${value}'`);
  }

  // Auto-detect: managed PostgreSQL URLs commonly carry ?sslmode=require
  // (or ?ssl=true). An explicit sslmode=disable keeps TLS off.
  const query = `?${databaseUrl.split('?', 2)[1] ?? ''}`;

  if (/(?:^|[?&])sslmode=(require|prefer|verify-ca|verify-full)\b/i.test(query)) {
    return true;
  }

  if (/(?:^|[?&])ssl=(true|1|require)\b/i.test(query)) {
    return true;
  }

  return false;
}

function parseTransport(value: string | undefined): TransportKind {
  const transport = (value ?? 'kafka').trim();

  if (transport !== 'kafka' && transport !== 'memory') {
    throw new Error(
      `NEXUSFLOW_TRANSPORT must be 'kafka' or 'memory', got '${transport}'`,
    );
  }

  if (transport === 'memory') {
    // The in-memory transport is a test/local-demo double. It is only valid
    // inside a single process; multi-process deployments must use Kafka.
  }

  return transport;
}

function parseStrictBoolean(
  value: string | undefined,
  name: string,
): boolean | undefined {
  if (value === undefined || value.trim() === '') {
    return undefined;
  }

  const normalized = value.trim().toLowerCase();

  if (normalized === 'true' || normalized === '1' || normalized === 'yes') {
    return true;
  }

  if (normalized === 'false' || normalized === '0' || normalized === 'no') {
    return false;
  }

  throw new Error(`${name} must be 'true' or 'false', got '${value}'`);
}

const API_KEY_SECRET_MIN_LENGTH = 16;

/**
 * Parses NEXUSFLOW_API_KEYS: comma-separated
 * `name:secret:tenant-UUID` entries. Names and secrets must be unique
 * (a shared secret would bind one bearer to two tenants); secrets
 * must be at least 16 characters. Error messages never include
 * secret values.
 */
function parseApiKeys(value: string | undefined): ApiKeyConfig[] {
  if (value === undefined || value.trim() === '') {
    return [];
  }

  const keys: ApiKeyConfig[] = [];
  const names = new Set<string>();
  const secrets = new Set<string>();

  for (const [index, raw] of value.split(',').entries()) {
    const entry = raw.trim();

    if (entry === '') {
      continue;
    }

    const parts = entry.split(':');

    if (parts.length !== 3) {
      throw new Error(
        `NEXUSFLOW_API_KEYS entry #${index + 1} must have the form name:secret:tenant-UUID`,
      );
    }

    const [name, secret, tenantId] = parts.map((p) => p.trim());

    if (!name) {
      throw new Error(
        `NEXUSFLOW_API_KEYS entry #${index + 1} must have a non-empty name`,
      );
    }

    if (names.has(name)) {
      throw new Error(`NEXUSFLOW_API_KEYS contains duplicate key name '${name}'`);
    }

    if (secret.length < API_KEY_SECRET_MIN_LENGTH) {
      throw new Error(
        `NEXUSFLOW_API_KEYS key '${name}' must have a secret of at least ${API_KEY_SECRET_MIN_LENGTH} characters`,
      );
    }

    if (secrets.has(secret)) {
      throw new Error(
        'NEXUSFLOW_API_KEYS contains the same secret twice (each key must have a unique secret)',
      );
    }

    if (!UUID_PATTERN.test(tenantId)) {
      throw new Error(
        `NEXUSFLOW_API_KEYS key '${name}' must bind a valid tenant UUID, got '${tenantId}'`,
      );
    }

    names.add(name);
    secrets.add(secret);
    keys.push({ name, secret, tenantId: tenantId.toLowerCase() });
  }

  return keys;
}

const SASL_MECHANISMS = ['plain', 'scram-sha-256', 'scram-sha-512'] as const;

/**
 * Parses Kafka transport security (KAFKA_SSL, KAFKA_SASL_MECHANISM /
 * _USERNAME / _PASSWORD). SASL requires TLS: credentials must never
 * travel over a plaintext connection. Managed brokers (Confluent
 * Cloud: plain; Upstash/Redpanda: scram-sha-256/512) all require TLS.
 */
function parseKafkaSecurity(env: NodeJS.ProcessEnv): {
  ssl: boolean;
  sasl?: KafkaSaslConfig;
} {
  const ssl = parseStrictBoolean(env.KAFKA_SSL, 'KAFKA_SSL') ?? false;

  const rawMechanism = env.KAFKA_SASL_MECHANISM?.trim();
  const username = env.KAFKA_SASL_USERNAME?.trim();
  const password = env.KAFKA_SASL_PASSWORD?.trim();

  if (!rawMechanism) {
    if (username) {
      throw new Error(
        'KAFKA_SASL_USERNAME requires KAFKA_SASL_MECHANISM to be set',
      );
    }

    if (password) {
      throw new Error(
        'KAFKA_SASL_PASSWORD requires KAFKA_SASL_MECHANISM to be set',
      );
    }

    return { ssl };
  }

  const mechanism = rawMechanism.toLowerCase();

  if (!SASL_MECHANISMS.includes(mechanism as (typeof SASL_MECHANISMS)[number])) {
    throw new Error(
      `KAFKA_SASL_MECHANISM must be one of plain|scram-sha-256|scram-sha-512, got '${rawMechanism}'`,
    );
  }

  if (!username) {
    throw new Error(
      'KAFKA_SASL_USERNAME is required when KAFKA_SASL_MECHANISM is set',
    );
  }

  if (!password) {
    throw new Error(
      'KAFKA_SASL_PASSWORD is required when KAFKA_SASL_MECHANISM is set',
    );
  }

  if (!ssl) {
    throw new Error(
      'KAFKA_SSL must be true when SASL is configured (SASL credentials must not travel over plaintext)',
    );
  }

  return {
    ssl,
    sasl: {
      mechanism: mechanism as KafkaSaslConfig['mechanism'],
      username,
      password,
    },
  };
}

export function loadEnvConfig(
  env: NodeJS.ProcessEnv = process.env,
): EnvConfig {
  const databaseUrl = env.DATABASE_URL?.trim();

  if (!databaseUrl) {
    throw new Error('DATABASE_URL is required for PostgreSQL access');
  }

  const role = parseRole(env.NEXUSFLOW_ROLE);
  const transport = parseTransport(env.NEXUSFLOW_TRANSPORT);

  if (transport === 'memory' && (role === 'worker' || role === 'outbox-relay')) {
    // The in-memory transport shares no state across processes. A
    // separate worker could never receive dispatches, and a separate
    // relay would publish into the void while marking events published
    // (silent non-delivery); both roles must use Kafka outside the
    // single-process 'all' role.
    throw new Error(
      'NEXUSFLOW_TRANSPORT=memory is only valid for single-process roles (api/all) or tests; separate worker and outbox-relay processes require Kafka',
    );
  }

  // API keys are parsed for every role (a malformed value fails fast
  // everywhere) but required only where HTTP is served: without at
  // least one key the api/all roles could not authenticate anyone.
  const apiKeys = parseApiKeys(env.NEXUSFLOW_API_KEYS);

  if ((role === 'api' || role === 'all') && apiKeys.length === 0) {
    throw new Error(
      'NEXUSFLOW_API_KEYS is required for the api/all roles (at least one name:secret:tenant-UUID entry)',
    );
  }

  const kafkaSecurity = parseKafkaSecurity(env);

  const defaultTenantId =
    env.NEXUSFLOW_DEFAULT_TENANT_ID?.trim() ??
    '00000000-0000-0000-0000-000000000000';

  if (!UUID_PATTERN.test(defaultTenantId)) {
    throw new Error('NEXUSFLOW_DEFAULT_TENANT_ID must be a valid UUID');
  }

  const bootstrapServers = (
    env.KAFKA_BOOTSTRAP_SERVERS ?? 'localhost:9092'
  )
    .split(',')
    .map((s) => s.trim())
    .filter((s) => s.length > 0);

  const serviceId =
    env.NEXUSFLOW_SERVICE_ID?.trim() || `svc-${process.pid}-${Date.now() % 100000}`;

  return {
    role,
    transport,
    databaseUrl,
    databaseSsl: parseDatabaseSsl(env.DATABASE_SSL, databaseUrl),
    apiKeys,
    port: parsePositiveInt(env.PORT, 3000, 'PORT'),
    defaultTenantId,
    serviceId,
    kafka: {
      bootstrapServers,
      dispatchTopic: env.NEXUSFLOW_DISPATCH_TOPIC?.trim() || 'nexusflow.task.dispatch',
      lifecycleTopic:
        env.NEXUSFLOW_LIFECYCLE_TOPIC?.trim() || 'nexusflow.workflow.events',
      consumerGroup:
        env.NEXUSFLOW_WORKER_GROUP?.trim() || 'nexusflow-workers',
      clientId: serviceId,
      ssl: kafkaSecurity.ssl,
      sasl: kafkaSecurity.sasl,
    },
    scheduler: {
      pollIntervalMs: parsePositiveInt(
        env.SCHEDULER_POLL_MS,
        250,
        'SCHEDULER_POLL_MS',
      ),
      batchLimit: parsePositiveInt(
        env.SCHEDULER_BATCH_LIMIT,
        20,
        'SCHEDULER_BATCH_LIMIT',
      ),
      leaseMs: parsePositiveInt(env.NEXUSFLOW_LEASE_MS, 30_000, 'NEXUSFLOW_LEASE_MS'),
    },
    worker: {
      consumerGroup:
        env.NEXUSFLOW_WORKER_GROUP?.trim() || 'nexusflow-workers',
    },
    relay: {
      pollIntervalMs: parsePositiveInt(
        env.RELAY_POLL_MS,
        250,
        'RELAY_POLL_MS',
      ),
      batchLimit: parsePositiveInt(
        env.RELAY_BATCH_LIMIT,
        20,
        'RELAY_BATCH_LIMIT',
      ),
      claimTtlMs: parsePositiveInt(
        env.RELAY_CLAIM_TTL_MS,
        30_000,
        'RELAY_CLAIM_TTL_MS',
      ),
      // Bounded wait for a single broker publish. A publish that takes
      // longer is treated as a failure (recorded + claim released,
      // retried via the outbox) so one wedged broker call can never
      // stall the relay — and shutdown drain is therefore bounded.
      publishTimeoutMs: parsePositiveInt(
        env.RELAY_PUBLISH_TIMEOUT_MS,
        30_000,
        'RELAY_PUBLISH_TIMEOUT_MS',
      ),
    },
  };
}
