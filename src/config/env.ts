export type AppRole = 'api' | 'scheduler' | 'worker' | 'outbox-relay' | 'all';
export type TransportKind = 'kafka' | 'memory';

export interface KafkaConfig {
  readonly bootstrapServers: string[];
  readonly dispatchTopic: string;
  readonly lifecycleTopic: string;
  readonly consumerGroup: string;
  readonly clientId: string;
}

export interface EnvConfig {
  readonly role: AppRole;
  readonly transport: TransportKind;
  readonly databaseUrl: string;
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

export function loadEnvConfig(
  env: NodeJS.ProcessEnv = process.env,
): EnvConfig {
  const databaseUrl = env.DATABASE_URL?.trim();

  if (!databaseUrl) {
    throw new Error('DATABASE_URL is required for PostgreSQL access');
  }

  const role = parseRole(env.NEXUSFLOW_ROLE);
  const transport = parseTransport(env.NEXUSFLOW_TRANSPORT);

  if (transport === 'memory' && role === 'worker') {
    throw new Error(
      'NEXUSFLOW_TRANSPORT=memory is only valid for single-process roles (api/all) or tests; a separate worker process requires Kafka',
    );
  }

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
    },
  };
}
