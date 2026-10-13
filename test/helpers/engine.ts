/**
 * In-process execution engine harness for integration tests.
 *
 * Wires the REAL scheduler claim transaction, outbox relay, and worker
 * against real PostgreSQL. The only non-production component is the
 * in-memory transport (no Kafka broker exists in the test environment);
 * every durable behavior (claims, leases, fencing, outbox, completion)
 * is exercised for real.
 */
import type { Pool } from 'pg';

import { OutboxRelayService } from '../../src/outbox-relay/outbox-relay.service';
import { SchedulerService } from '../../src/scheduler/scheduler.service';
import { MemoryMessageTransport } from '../../src/transport/memory-transport';
import { WorkflowService } from '../../src/workflow/application/workflow.service';
import { RunService } from '../../src/workflow/application/run.service';
import { WorkerService } from '../../src/worker/worker.service';
import { EnvConfig } from '../../src/config/env';
import { DatabaseService } from '../../src/database/database.service';
import type {
  TaskDefinition,
  WorkflowDefinition,
} from '../../src/workflow/domain/workflow-definition';
import { definition, task } from './factory';

export interface Engine {
  readonly pool: Pool;
  readonly transport: MemoryMessageTransport;
  readonly scheduler: SchedulerService;
  readonly relay: OutboxRelayService;
  readonly worker: WorkerService;
  readonly workflows: WorkflowService;
  readonly runs: RunService;
  readonly config: EnvConfig;

  /** Run scheduler + relay + worker until the given run is terminal. */
  drainRun(workflowRunId: string, timeoutMs?: number): Promise<string>;
  /** One scheduler + relay cycle. */
  tick(): Promise<void>;
  stop(): Promise<void>;
}

export interface BaseConfigOptions {
  leaseMs?: number;
  pollIntervalMs?: number;
  claimTtlMs?: number;
  publishTimeoutMs?: number;
}

export function baseConfig(options: BaseConfigOptions = {}): EnvConfig {
  return {
    role: 'all',
    transport: 'memory',
    databaseUrl: 'unused',
    port: 0,
    defaultTenantId: '00000000-0000-0000-0000-000000000000',
    serviceId: 'test',
    kafka: {
      bootstrapServers: ['localhost:9092'],
      dispatchTopic: 'nexusflow.task.dispatch',
      lifecycleTopic: 'nexusflow.workflow.events',
      consumerGroup: 'test-workers',
      clientId: 'test',
    },
    scheduler: {
      pollIntervalMs: options.pollIntervalMs ?? 20,
      batchLimit: 20,
      leaseMs: options.leaseMs ?? 30_000,
    },
    worker: { consumerGroup: 'test-workers' },
    relay: {
      pollIntervalMs: options.pollIntervalMs ?? 20,
      batchLimit: 20,
      claimTtlMs: options.claimTtlMs ?? 5_000,
      publishTimeoutMs: options.publishTimeoutMs ?? 5_000,
    },
  };
}

export async function createEngine(
  pool: Pool,
  config: EnvConfig = baseConfig(),
): Promise<Engine> {
  const transport = new MemoryMessageTransport();
  const database = new DatabaseService(pool);

  const scheduler = new SchedulerService(database, config);
  const relay = new OutboxRelayService(database, config, transport);
  const worker = new WorkerService(database, config, transport);
  const workflows = new WorkflowService(database);
  const runs = new RunService(database);

  const engine: Engine = {
    pool,
    transport,
    scheduler,
    relay,
    worker,
    workflows,
    runs,
    config,

    async tick() {
      await scheduler.scan();
      await relay.pump();
      await transport.drain(5000);
    },

    async drainRun(workflowRunId, timeoutMs = 20_000) {
      const deadline = Date.now() + timeoutMs;

      for (;;) {
        await engine.tick();

        const state = (
          await pool.query<{ state: string }>(
            'SELECT state FROM workflow_runs WHERE workflow_run_id = $1',
            [workflowRunId],
          )
        ).rows[0]?.state;

        if (state === 'SUCCEEDED' || state === 'FAILED' || state === 'CANCELLED') {
          // One final tick so outbox events for the terminal state publish.
          await engine.tick();
          return state;
        }

        if (Date.now() > deadline) {
          const tasks = await pool.query<{ task_key: string; state: string; attempt_count: number }>(
            'SELECT task_key, state, attempt_count FROM task_executions WHERE workflow_run_id = $1 ORDER BY task_key',
            [workflowRunId],
          );
          throw new Error(
            `Run ${workflowRunId} did not reach a terminal state in ${timeoutMs}ms; tasks: ${JSON.stringify(
              tasks.rows,
            )}`,
          );
        }
      }
    },

    async stop() {
      scheduler.onApplicationShutdown();
      // Await: waits for an in-flight publish cycle so the transport
      // below is not closed out from under it.
      await relay.onApplicationShutdown();
      worker.onApplicationShutdown();
      await transport.close();
    },
  };

  // Start the worker consumer (the scheduler/relay loops are driven
  // explicitly by tests via tick()/drainRun() for determinism).
  await worker.onModuleInit();

  return engine;
}

/**
 * Creates, publishes, and starts a workflow run with the given tasks.
 */
export async function setupRun(
  engine: Engine,
  tasks: readonly TaskDefinition[],
  input?: {
    name?: string;
    workflow?: WorkflowDefinition;
    runInput?: Record<string, unknown>;
    idempotencyKey?: string;
    tenantId?: string;
  },
): Promise<{
  workflowId: string;
  workflowVersionId: string;
  workflowRunId: string;
  tenantId: string;
}> {
  const tenantId = input?.tenantId ?? '11111111-1111-4111-8111-111111111111';
  const workflow = await engine.workflows.createWorkflow({
    tenantId,
    name: input?.name ?? `eng-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
  });

  const version = await engine.workflows.createDraftVersion({
    tenantId,
    workflowId: workflow.workflow_id,
    definition: input?.workflow ?? definition([...tasks]),
  });

  await engine.workflows.publish(version.workflow_version_id);

  const result = await engine.runs.startRun({
    tenantId,
    workflowId: workflow.workflow_id,
    workflowVersionId: version.workflow_version_id,
    input: input?.runInput,
    idempotencyKey: input?.idempotencyKey,
  });

  return {
    workflowId: workflow.workflow_id,
    workflowVersionId: version.workflow_version_id,
    workflowRunId: result.run.workflow_run_id,
    tenantId,
  };
}

export { definition, task };
