import { randomUUID } from 'node:crypto';
import type { PoolClient } from 'pg';

import { OutboxEventType } from '../outbox/events';
import { insertOutboxEvent } from '../outbox/outbox-repository';

export interface ClaimedTask {
  readonly taskExecutionId: string;
  readonly workflowRunId: string;
  readonly taskDefinitionId: string;
  readonly taskKey: string;
  readonly fencingToken: number;
  readonly attemptId: string;
  readonly attemptNumber: number;
  readonly dispatchId: string;
  readonly leaseOwner: string;
  readonly leaseMs: number;
  readonly timeoutMs: number;
  readonly tenantId: string;
  readonly workflowId: string;
  readonly workflowVersionId: string;
  readonly runCorrelationId: string;
}

interface ClaimRow {
  task_execution_id: string;
  workflow_run_id: string;
  task_definition_id: string;
  task_key: string;
  fencing_token: number;
  attempt_count: number;
}

const CLAIM_SQL = `
  WITH eligible AS (
    SELECT te.task_execution_id
    FROM task_executions te
    JOIN workflow_runs wr
      ON wr.workflow_run_id = te.workflow_run_id
    WHERE wr.state = 'RUNNING'
      AND (
            (te.state = 'READY')
         OR (te.state = 'RETRY_WAIT'
             AND te.next_attempt_at IS NOT NULL
             AND te.next_attempt_at <= now())
         OR (te.state IN ('CLAIMED', 'RUNNING')
             AND te.lease_expires_at IS NOT NULL
             AND te.lease_expires_at <= now())
      )
    ORDER BY te.created_at ASC, te.task_execution_id ASC
    LIMIT $1
    FOR UPDATE SKIP LOCKED
  ),
  claimed AS (
    UPDATE task_executions te
    SET
      state = 'CLAIMED',
      fencing_token = te.fencing_token + 1,
      lease_owner = $2,
      lease_expires_at = now() + make_interval(secs => $3::float8),
      claimed_at = now(),
      next_attempt_at = NULL,
      attempt_count = te.attempt_count + 1,
      updated_at = now()
    FROM eligible e
    WHERE te.task_execution_id = e.task_execution_id
    RETURNING te.*
  )
  SELECT task_execution_id, workflow_run_id, task_definition_id, task_key,
         fencing_token, attempt_count
  FROM claimed
  ORDER BY created_at ASC, task_execution_id ASC
`;

/**
 * Scheduler claim transaction (contract D.6).
 *
 * Atomically, in one PostgreSQL transaction:
 *  1. selects eligible task executions (READY, due RETRY_WAIT, or expired
 *     CLAIMED/RUNNING leases) of RUNNING runs using FOR UPDATE SKIP LOCKED
 *  2. increments the fencing token, assigns the lease, transitions to
 *     CLAIMED
 *  3. abandons any orphaned in-flight attempt and creates the next attempt
 *  4. resolves the task input (run input + upstream outputs)
 *  5. inserts task.claimed and task.dispatch.requested outbox events
 *
 * Multiple scheduler instances may call this concurrently; row locking
 * guarantees a single valid claim per task.
 */
export async function claimBatch(
  client: PoolClient,
  input: {
    batchSize: number;
    leaseOwner: string;
    schedulerId: string;
    leaseMs: number;
  },
): Promise<ClaimedTask[]> {
  const result = await client.query<ClaimRow>(CLAIM_SQL, [
    input.batchSize,
    input.leaseOwner,
    input.leaseMs / 1000,
  ]);

  const claimed: ClaimedTask[] = [];

  for (const row of result.rows) {
    claimed.push(await finalizeClaim(client, row, input));
  }

  return claimed;
}

async function finalizeClaim(
  client: PoolClient,
  row: ClaimRow,
  input: { batchSize: number; leaseOwner: string; schedulerId: string; leaseMs: number },
): Promise<ClaimedTask> {
  // Load run + definition + version context.
  const run = (
    await client.query<{
      workflow_run_id: string;
      tenant_id: string;
      workflow_id: string;
      workflow_version_id: string;
      correlation_id: string;
      input_payload: Record<string, unknown> | null;
    }>(
      `
      SELECT workflow_run_id, tenant_id, workflow_id,
             workflow_version_id, correlation_id, input_payload
      FROM workflow_runs
      WHERE workflow_run_id = $1
    `,
      [row.workflow_run_id],
    )
  ).rows[0];

  const def = (
    await client.query<{
      task_definition_id: string;
      workflow_version_id: string;
      timeout_ms: number;
    }>(
      `
      SELECT task_definition_id, workflow_version_id, timeout_ms
      FROM task_definitions
      WHERE task_definition_id = $1
    `,
      [row.task_definition_id],
    )
  ).rows[0];

  if (!run || !def) {
    // Cannot happen (FKs); fail loudly and roll back.
    throw new Error(
      `Claim context missing for task execution ${row.task_execution_id}`,
    );
  }

  // Abandon any previous attempt that lost ownership (lease expiry,
  // crash). Attempt terminal states are immutable, so only
  // CREATED/DISPATCHED/STARTED can transition to ABANDONED.
  await client.query(
    `
    UPDATE task_attempts
    SET state = 'ABANDONED', finished_at = now()
    WHERE task_execution_id = $1
      AND state IN ('CREATED', 'DISPATCHED', 'STARTED')
  `,
    [row.task_execution_id],
  );

  const attemptNumber = row.attempt_count;
  const attemptId = randomUUID();
  const dispatchId = randomUUID();

  await client.query(
    `
    INSERT INTO task_attempts (
      task_attempt_id, task_execution_id, attempt_number, state,
      scheduler_id, fencing_token, dispatch_id, timeout_at, created_at
    )
    VALUES ($1, $2, $3, 'DISPATCHED', $4, $5, $6,
            now() + make_interval(secs => $7::float8), now())
  `,
    [
      attemptId,
      row.task_execution_id,
      attemptNumber,
      input.schedulerId,
      row.fencing_token,
      dispatchId,
      def.timeout_ms / 1000,
    ],
  );

  await client.query(
    `
    UPDATE task_executions
    SET last_attempt_id = $2, updated_at = now()
    WHERE task_execution_id = $1
  `,
    [row.task_execution_id, attemptId],
  );

  // Resolve input: run input plus direct upstream outputs.
  const upstreamEdges = (
    await client.query<{ upstream: string }>(
      `
      SELECT td_up.task_definition_id AS upstream
      FROM task_dependencies dep
      JOIN task_definitions td_up
        ON td_up.task_definition_id = dep.upstream_task_definition_id
      WHERE dep.workflow_version_id = $1
        AND dep.downstream_task_definition_id = $2
    `,
      [def.workflow_version_id, def.task_definition_id],
    )
  ).rows;

  const upstreamOutputs: Record<string, unknown> = {};

  if (upstreamEdges.length > 0) {
    const parameters: unknown[] = [
      ...upstreamEdges.map((e) => e.upstream),
      row.workflow_run_id,
    ];

    const placeholders = upstreamEdges
      .map((_, i) => `$${i + 1}`)
      .join(', ');

    const upstreamRows = (
      await client.query<{
        task_key: string;
        output_payload: unknown;
        state: string;
      }>(
        `
        SELECT te.task_key, te.output_payload, te.state
        FROM task_executions te
        WHERE te.workflow_run_id = $${parameters.length}
          AND te.task_definition_id IN (${placeholders})
      `,
        parameters,
      )
    ).rows;

    for (const upstream of upstreamRows) {
      upstreamOutputs[upstream.task_key] = upstream.output_payload ?? null;
    }
  }

  const resolvedInput = {
    ...(run.input_payload ?? {}),
    upstream: upstreamOutputs,
  };

  await client.query(
    `
    UPDATE task_executions
    SET resolved_input = $2, updated_at = now()
    WHERE task_execution_id = $1
  `,
    [row.task_execution_id, JSON.stringify(resolvedInput)],
  );

  // task.claimed (lifecycle event)
  await insertOutboxEvent(client, {
    eventType: OutboxEventType.TaskClaimed,
    aggregateType: 'task_execution',
    aggregateId: row.task_execution_id,
    tenantId: run.tenant_id,
    workflowId: run.workflow_id,
    workflowVersionId: run.workflow_version_id,
    workflowRunId: run.workflow_run_id,
    taskExecutionId: row.task_execution_id,
    taskAttemptId: attemptId,
    correlationId: run.correlation_id,
    causationId: dispatchId,
    producer: `nexusflow/scheduler:${input.schedulerId}`,
    payload: {
      workflowRunId: run.workflow_run_id,
      taskExecutionId: row.task_execution_id,
      taskKey: row.task_key,
      attemptNumber,
      fencingToken: row.fencing_token,
      leaseOwner: input.leaseOwner,
      dispatchId,
    },
  });

  // task.dispatch.requested — consumed by the outbox relay to publish on
  // the Kafka dispatch topic.
  await insertOutboxEvent(client, {
    eventType: OutboxEventType.TaskDispatchRequested,
    aggregateType: 'task_execution',
    aggregateId: row.task_execution_id,
    tenantId: run.tenant_id,
    workflowId: run.workflow_id,
    workflowVersionId: run.workflow_version_id,
    workflowRunId: run.workflow_run_id,
    taskExecutionId: row.task_execution_id,
    taskAttemptId: attemptId,
    correlationId: run.correlation_id,
    causationId: dispatchId,
    producer: `nexusflow/scheduler:${input.schedulerId}`,
    payload: {
      dispatchId,
      taskExecutionId: row.task_execution_id,
      taskAttemptId: attemptId,
      workflowRunId: run.workflow_run_id,
      taskKey: row.task_key,
      attemptNumber,
      fencingToken: row.fencing_token,
      leaseOwner: input.leaseOwner,
      timeoutMs: def.timeout_ms,
    },
  });

  return {
    taskExecutionId: row.task_execution_id,
    workflowRunId: row.workflow_run_id,
    taskDefinitionId: row.task_definition_id,
    taskKey: row.task_key,
    fencingToken: row.fencing_token,
    attemptId,
    attemptNumber,
    dispatchId,
    leaseOwner: input.leaseOwner,
    leaseMs: input.leaseMs,
    timeoutMs: def.timeout_ms,
    tenantId: run.tenant_id,
    workflowId: run.workflow_id,
    workflowVersionId: run.workflow_version_id,
    runCorrelationId: run.correlation_id,
  };
}
