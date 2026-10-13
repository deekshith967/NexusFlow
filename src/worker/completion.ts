import type { PoolClient } from 'pg';

import { OutboxEventType } from '../outbox/events';
import { insertOutboxEvent } from '../outbox/outbox-repository';

export interface CompletionContext {
  readonly workflowRunId: string;
  readonly workflowVersionId: string;
  readonly workflowId: string;
  readonly tenantId: string;
  readonly correlationId: string;
  readonly producer: string;
}

const ACTIVE_STATES = ['WAITING', 'READY', 'CLAIMED', 'RUNNING', 'RETRY_WAIT'] as const;
const OK_TERMINAL_STATES = ['SUCCEEDED', 'SKIPPED'] as const;

/**
 * Exact workflow completion algorithm (contract B.5).
 *
 * MUST be called while the caller holds the workflow run row lock
 * (FOR UPDATE) and inside the same transaction that changed a task
 * execution to a terminal state. Returns the run state after evaluation.
 */
export async function evaluateWorkflowCompletion(
  client: PoolClient,
  ctx: CompletionContext,
  trigger: {
    failureCode?: string;
    failureMessage?: string;
    taskExecutionId?: string;
    taskKey?: string;
  } | null,
): Promise<string> {
  const run = (
    await client.query<{ state: string }>(
      `
      SELECT state
      FROM workflow_runs
      WHERE workflow_run_id = $1
      FOR UPDATE
    `,
      [ctx.workflowRunId],
    )
  ).rows[0];

  if (!run) {
    throw new Error(`Workflow run ${ctx.workflowRunId} not found`);
  }

  // Step 2: verify the run is currently RUNNING.
  if (run.state !== 'RUNNING') {
    return run.state;
  }

  // Lock all of the run's task rows in a deterministic order
  // (task_execution_id). Transactions that reach this point already
  // hold the run row lock, so this scan is serialized per run; the
  // explicit order keeps lock acquisition stable regardless of plan.
  const tasks = (
    await client.query<{ task_execution_id: string; state: string; task_key: string }>(
      `
      SELECT task_execution_id, state, task_key
      FROM task_executions
      WHERE workflow_run_id = $1
      ORDER BY task_execution_id ASC
      FOR UPDATE
    `,
      [ctx.workflowRunId],
    )
  ).rows;

  const states = tasks.map((t) => t.state);
  const hasFailed = states.includes('FAILED');
  const hasActive = states.some((s) =>
    (ACTIVE_STATES as readonly string[]).includes(s),
  );
  const hasBlocked = states.includes('BLOCKED');
  const allOk = states.every((s) =>
    (OK_TERMINAL_STATES as readonly string[]).includes(s),
  );

  if (hasFailed) {
    // Step 4: run -> FAILED.
    await failRun(
      client,
      ctx,
      trigger ?? {
        failureCode: 'TASK_FAILED',
        failureMessage: 'A task permanently failed',
      },
    );
    return 'FAILED';
  }

  if (hasActive) {
    // Step 5: run stays RUNNING.
    return 'RUNNING';
  }

  if (hasBlocked) {
    // Step 6: terminal but blocked -> run FAILED (defensive path).
    await failRun(
      client,
      ctx,
      {
        failureCode: 'TASK_BLOCKED',
        failureMessage:
          'All tasks are terminal and at least one task is BLOCKED',
      },
    );
    return 'FAILED';
  }

  if (allOk) {
    // Step 7: every task SUCCEEDED or SKIPPED -> run SUCCEEDED.
    await client.query(
      `
      UPDATE workflow_runs
      SET state = 'SUCCEEDED', completed_at = now()
      WHERE workflow_run_id = $1
        AND state = 'RUNNING'
    `,
      [ctx.workflowRunId],
    );

    await insertOutboxEvent(client, {
      eventType: OutboxEventType.WorkflowRunSucceeded,
      aggregateType: 'workflow_run',
      aggregateId: ctx.workflowRunId,
      tenantId: ctx.tenantId,
      workflowId: ctx.workflowId,
      workflowVersionId: ctx.workflowVersionId,
      workflowRunId: ctx.workflowRunId,
      correlationId: ctx.correlationId,
      producer: ctx.producer,
      payload: {
        workflowRunId: ctx.workflowRunId,
        workflowVersionId: ctx.workflowVersionId,
        taskCount: tasks.length,
      },
    });

    return 'SUCCEEDED';
  }

  // Cancellation (step 8) is not part of the Phase 1 core.
  return 'RUNNING';
}

async function failRun(
  client: PoolClient,
  ctx: CompletionContext,
  failure: {
    failureCode?: string;
    failureMessage?: string;
    taskExecutionId?: string;
    taskKey?: string;
  },
): Promise<void> {
  await client.query(
    `
    UPDATE workflow_runs
    SET state = 'FAILED',
        completed_at = now(),
        failure_code = $2,
        failure_message = $3
    WHERE workflow_run_id = $1
      AND state = 'RUNNING'
  `,
    [ctx.workflowRunId, failure.failureCode ?? null, failure.failureMessage ?? null],
  );

  // WAITING tasks cannot satisfy dependencies once the run is failed;
  // transition them to BLOCKED (B.3 permits WAITING -> BLOCKED).
  // READY/RETRY_WAIT/CLAIMED/RUNNING tasks become non-executable because
  // the scheduler only claims for RUNNING runs and late reports are
  // rejected (the completion transactions verify run state first).
  await client.query(
    `
    UPDATE task_executions
    SET state = 'BLOCKED', updated_at = now()
    WHERE workflow_run_id = $1
      AND state = 'WAITING'
  `,
    [ctx.workflowRunId],
  );

  // Resolve dangling attempts: once the run is terminal no further
  // authoritative report can be accepted, so in-flight attempts that
  // have not reported are ABANDONED (B.4).
  await client.query(
    `
    UPDATE task_attempts
    SET state = 'ABANDONED', finished_at = now()
    WHERE task_execution_id IN (
      SELECT task_execution_id
      FROM task_executions
      WHERE workflow_run_id = $1
    )
      AND state IN ('DISPATCHED', 'STARTED')
  `,
    [ctx.workflowRunId],
  );

  await insertOutboxEvent(client, {
    eventType: OutboxEventType.WorkflowRunFailed,
    aggregateType: 'workflow_run',
    aggregateId: ctx.workflowRunId,
    tenantId: ctx.tenantId,
    workflowId: ctx.workflowId,
    workflowVersionId: ctx.workflowVersionId,
    workflowRunId: ctx.workflowRunId,
    correlationId: ctx.correlationId,
    producer: ctx.producer,
    payload: {
      workflowRunId: ctx.workflowRunId,
      workflowVersionId: ctx.workflowVersionId,
      failureCode: failure.failureCode ?? null,
      failureMessage: failure.failureMessage ?? null,
      failedTaskExecutionId: failure.taskExecutionId ?? null,
      failedTaskKey: failure.taskKey ?? null,
    },
  });
}

/**
 * Dependency activation (contract D.13).
 *
 * Called inside the prerequisite completion transaction, after the
 * prerequisite moved to a terminal state. For each direct downstream
 * task still WAITING:
 *  - all prerequisites satisfied (SUCCEEDED or SKIPPED) -> READY
 *  - any prerequisite FAILED or BLOCKED (execution impossible) -> BLOCKED
 *  - otherwise stays WAITING
 *
 * Concurrency: all completion transactions for a run hold the run row
 * lock, so activation is serialized per run; the conditional
 * WAITING -> READY/BLOCKED updates make duplicate activation impossible.
 */
export async function activateDownstreamTasks(
  client: PoolClient,
  ctx: CompletionContext,
  completedTaskDefinitionId: string,
): Promise<void> {
  const downstream = (
    await client.query<{
      task_definition_id: string;
      task_execution_id: string;
      task_key: string;
    }>(
      `
      SELECT td.task_definition_id, te.task_execution_id, te.task_key
      FROM task_dependencies dep
      JOIN task_definitions td
        ON td.task_definition_id = dep.downstream_task_definition_id
      JOIN task_executions te
        ON te.task_definition_id = td.task_definition_id
       AND te.workflow_run_id = $1
      WHERE dep.workflow_version_id = $2
        AND dep.upstream_task_definition_id = $3
    `,
      [ctx.workflowRunId, ctx.workflowVersionId, completedTaskDefinitionId],
    )
  ).rows;

  for (const target of downstream) {
    const targetRow = (
      await client.query<{ state: string }>(
        `
        SELECT state
        FROM task_executions
        WHERE task_execution_id = $1
        FOR UPDATE
      `,
        [target.task_execution_id],
      )
    ).rows[0];

    if (!targetRow || targetRow.state !== 'WAITING') {
      continue;
    }

    // All prerequisites of the downstream task.
    const prerequisites = (
      await client.query<{ state: string }>(
        `
        SELECT te.state
        FROM task_dependencies dep
        JOIN task_executions te
          ON te.task_definition_id = dep.upstream_task_definition_id
         AND te.workflow_run_id = $1
        WHERE dep.workflow_version_id = $2
          AND dep.downstream_task_definition_id = $3
      `,
        [ctx.workflowRunId, ctx.workflowVersionId, target.task_definition_id],
      )
    ).rows;

    const prerequisiteStates = prerequisites.map((p) => p.state);
    const anyUnresolved = prerequisiteStates.some((s) => !isTerminalState(s));
    const anyBlocking = prerequisiteStates.some(
      (s) => s === 'FAILED' || s === 'BLOCKED',
    );

    if (anyUnresolved) {
      // Some prerequisite has not reached a terminal state yet.
      continue;
    }

    if (anyBlocking) {
      const blocked = await client.query(
        `
        UPDATE task_executions
        SET state = 'BLOCKED', updated_at = now()
        WHERE task_execution_id = $1
          AND state = 'WAITING'
      `,
        [target.task_execution_id],
      );

      if (blocked.rowCount === 1) {
        await insertOutboxEvent(client, {
          eventType: OutboxEventType.TaskBlocked,
          aggregateType: 'task_execution',
          aggregateId: target.task_execution_id,
          tenantId: ctx.tenantId,
          workflowId: ctx.workflowId,
          workflowVersionId: ctx.workflowVersionId,
          workflowRunId: ctx.workflowRunId,
          taskExecutionId: target.task_execution_id,
          correlationId: ctx.correlationId,
          producer: ctx.producer,
          payload: {
            workflowRunId: ctx.workflowRunId,
            taskExecutionId: target.task_execution_id,
            taskKey: target.task_key,
            reason: 'prerequisite_failed_or_blocked',
          },
        });
      }

      continue;
    }

    // All prerequisites satisfied.
    const ready = await client.query(
      `
      UPDATE task_executions
      SET state = 'READY', updated_at = now()
      WHERE task_execution_id = $1
        AND state = 'WAITING'
    `,
      [target.task_execution_id],
    );

    if (ready.rowCount === 1) {
      await insertOutboxEvent(client, {
        eventType: OutboxEventType.TaskReady,
        aggregateType: 'task_execution',
        aggregateId: target.task_execution_id,
        tenantId: ctx.tenantId,
        workflowId: ctx.workflowId,
        workflowVersionId: ctx.workflowVersionId,
        workflowRunId: ctx.workflowRunId,
        taskExecutionId: target.task_execution_id,
        correlationId: ctx.correlationId,
        producer: ctx.producer,
        payload: {
          workflowRunId: ctx.workflowRunId,
          taskExecutionId: target.task_execution_id,
          taskKey: target.task_key,
          reason: 'dependencies_satisfied',
        },
      });
    }
  }
}

function isTerminalState(state: string): boolean {
  return ['SUCCEEDED', 'FAILED', 'SKIPPED', 'BLOCKED', 'CANCELLED'].includes(
    state,
  );
}
