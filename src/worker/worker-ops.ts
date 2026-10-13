import type { PoolClient } from 'pg';

import { DatabaseService } from '../database/database.service';
import { OutboxEventType } from '../outbox/events';
import { insertOutboxEvent } from '../outbox/outbox-repository';
import {
  activateDownstreamTasks,
  evaluateWorkflowCompletion,
  type CompletionContext,
} from './completion';

export interface StartAttemptInput {
  readonly dispatchId: string;
  readonly taskExecutionId: string;
  readonly fencingToken: number;
  readonly workerId: string;
}

export interface StartAttemptResult {
  readonly ok: boolean;
  readonly reason?:
    | 'not_found'
    | 'stale'
    | 'run_not_running'
    | 'lease_expired';
}

/**
 * Worker start transaction (contract D.9).
 *
 * Validates that the dispatch attempt matches, the task is CLAIMED with
 * the current fencing token and a valid lease, and the run is RUNNING,
 * then transitions task -> RUNNING, attempt -> STARTED and inserts
 * task.started. A zero-row result means the dispatch is stale or
 * invalid and the handler must NOT execute.
 *
 * Lock ordering: workflow_runs FIRST, then task_executions, then
 * task_attempts. Completion transactions follow the same order, so
 * worker-vs-worker transactions on the same run serialize on the run
 * row and cannot deadlock. (The scheduler claim locks run+task
 * atomically with FOR UPDATE SKIP LOCKED and therefore never blocks,
 * so it cannot participate in a cycle either.)
 */
export async function startAttempt(
  database: DatabaseService,
  input: StartAttemptInput,
): Promise<StartAttemptResult> {
  return database.transaction(async (client) => {
    // 1. Unlocked lookup to identify the run. (The attempt's
    //    task_execution_id/workflow_run_id are immutable, so a stale
    //    pointer is impossible; the authoritative state is re-read
    //    under locks in step 3.)
    const located = (
      await client.query<{ workflow_run_id: string }>(
        `
        SELECT te.workflow_run_id
        FROM task_attempts a
        JOIN task_executions te
          ON te.task_execution_id = a.task_execution_id
        WHERE a.dispatch_id = $1
      `,
        [input.dispatchId],
      )
    ).rows[0];

    if (!located) {
      return { ok: false, reason: 'not_found' };
    }

    // 2. Lock the run row first (standard lock order).
    const run = (
      await client.query<{ state: string; correlation_id: string }>(
        `
        SELECT state, correlation_id
        FROM workflow_runs
        WHERE workflow_run_id = $1
        FOR UPDATE
      `,
        [located.workflow_run_id],
      )
    ).rows[0];

    if (!run) {
      return { ok: false, reason: 'stale' };
    }

    // 3. Authoritative locked read of the attempt + task.
    const attempt = (
      await client.query<{
        task_attempt_id: string;
        dispatch_id: string;
        task_execution_id: string;
        attempt_number: number;
        state: string;
        fencing_token: number;
        timeout_at: string | null;
        task_state: string;
        task_fencing_token: number;
        task_lease_expires_at: string | null;
        last_attempt_id: string | null;
        workflow_run_id: string;
        task_definition_id: string;
        task_key: string;
      }>(
        `
        SELECT a.task_attempt_id, a.dispatch_id, a.task_execution_id,
               a.attempt_number, a.state, a.fencing_token, a.timeout_at,
               te.state AS task_state,
               te.fencing_token AS task_fencing_token,
               te.lease_expires_at AS task_lease_expires_at,
               te.last_attempt_id,
               te.workflow_run_id,
               te.task_definition_id,
               te.task_key
        FROM task_attempts a
        JOIN task_executions te
          ON te.task_execution_id = a.task_execution_id
        WHERE a.dispatch_id = $1
        FOR UPDATE
      `,
        [input.dispatchId],
      )
    ).rows[0];

    if (!attempt) {
      return { ok: false, reason: 'not_found' };
    }

    if (
      attempt.state !== 'DISPATCHED' ||
      attempt.task_state !== 'CLAIMED' ||
      attempt.fencing_token !== input.fencingToken ||
      attempt.fencing_token !== attempt.task_fencing_token ||
      attempt.last_attempt_id !== attempt.task_attempt_id
    ) {
      return { ok: false, reason: 'stale' };
    }

    if (run.state !== 'RUNNING') {
      // The run is terminal: no authoritative execution is possible.
      // Abandon the attempt so the record resolves.
      await client.query(
        `
        UPDATE task_attempts
        SET state = 'ABANDONED', finished_at = now()
        WHERE task_attempt_id = $1
          AND state = 'DISPATCHED'
      `,
        [attempt.task_attempt_id],
      );
      return { ok: false, reason: 'run_not_running' };
    }

    const updated = await client.query(
      `
      UPDATE task_executions
      SET state = 'RUNNING',
          started_at = COALESCE(started_at, now()),
          updated_at = now()
      WHERE task_execution_id = $1
        AND state = 'CLAIMED'
        AND fencing_token = $2
        AND last_attempt_id = $3
        AND lease_expires_at IS NOT NULL
        AND lease_expires_at > now()
      RETURNING task_execution_id
    `,
      [
        attempt.task_execution_id,
        input.fencingToken,
        attempt.task_attempt_id,
      ],
    );

    if (updated.rowCount === 0) {
      return { ok: false, reason: 'lease_expired' };
    }

    // The attempt row is locked (step 3); the transition must affect
    // exactly one row or the transaction is inconsistent — abort so
    // task and attempt cannot commit different outcomes.
    const started = await client.query(
      `
      UPDATE task_attempts
      SET state = 'STARTED',
          worker_id = $2,
          started_at = now()
      WHERE task_attempt_id = $1
        AND state = 'DISPATCHED'
    `,
      [attempt.task_attempt_id, input.workerId],
    );

    if (started.rowCount !== 1) {
      throw new Error(
        `Attempt ${attempt.task_attempt_id} state transition to STARTED affected ${started.rowCount} rows; aborting start transaction`,
      );
    }

    // task.started correlates with the WORKFLOW RUN's correlation_id
    // (the run-level trace identifier); the dispatch that caused this
    // start is recorded as causation.
    await insertOutboxEvent(client, {
      eventType: OutboxEventType.TaskStarted,
      aggregateType: 'task_execution',
      aggregateId: attempt.task_execution_id,
      workflowRunId: attempt.workflow_run_id,
      taskExecutionId: attempt.task_execution_id,
      taskAttemptId: attempt.task_attempt_id,
      correlationId: run.correlation_id,
      causationId: attempt.dispatch_id,
      producer: `nexusflow/worker:${input.workerId}`,
      payload: {
        workflowRunId: attempt.workflow_run_id,
        taskExecutionId: attempt.task_execution_id,
        taskAttemptId: attempt.task_attempt_id,
        attemptNumber: attempt.attempt_number,
        workerId: input.workerId,
      },
    });

    return { ok: true };
  });
}

/**
 * Lease renewal (contract D.10). A zero-row update means the renewal
 * failed: the worker must treat itself as stale and stop executing.
 */
export async function renewLease(
  database: DatabaseService,
  input: {
    taskExecutionId: string;
    fencingToken: number;
    leaseMs: number;
  },
): Promise<boolean> {
  const result = await database.query(
    `
    UPDATE task_executions
    SET lease_expires_at = now() + make_interval(secs => $3::float8),
        updated_at = now()
    WHERE task_execution_id = $1
      AND fencing_token = $2
      AND state IN ('CLAIMED', 'RUNNING')
      AND lease_expires_at IS NOT NULL
      AND lease_expires_at > now()
    RETURNING task_execution_id
  `,
    [input.taskExecutionId, input.fencingToken, input.leaseMs / 1000],
  );

  return (result.rowCount ?? 0) > 0;
}

interface CommonReportInput {
  readonly taskExecutionId: string;
  readonly attemptId: string;
  readonly fencingToken: number;
  readonly workerId: string;
}

export interface CompleteSuccessInput extends CommonReportInput {
  readonly output: unknown;
}

export interface ReportResult {
  readonly applied: boolean;
  readonly runState?: string;
  readonly taskState?: string;
}

async function loadRunContext(
  client: PoolClient,
  taskExecutionId: string,
): Promise<CompletionContext & {
  taskDefinitionId: string;
  taskKey: string;
  runState: string;
} | null> {
  const row = (
    await client.query<{
      workflow_run_id: string;
      tenant_id: string;
      workflow_id: string;
      workflow_version_id: string;
      correlation_id: string;
      run_state: string;
      task_definition_id: string;
      task_key: string;
    }>(
      `
      SELECT wr.workflow_run_id,
             wr.tenant_id,
             wr.workflow_id,
             wr.workflow_version_id,
             wr.correlation_id,
             wr.state AS run_state,
             te.task_definition_id,
             te.task_key
      FROM task_executions te
      JOIN workflow_runs wr
        ON wr.workflow_run_id = te.workflow_run_id
      WHERE te.task_execution_id = $1
      FOR UPDATE OF wr
    `,
      [taskExecutionId],
    )
  ).rows[0];

  if (!row) {
    return null;
  }

  return {
    workflowRunId: row.workflow_run_id,
    workflowVersionId: row.workflow_version_id,
    workflowId: row.workflow_id,
    tenantId: row.tenant_id,
    correlationId: row.correlation_id,
    producer: 'nexusflow/worker',
    taskDefinitionId: row.task_definition_id,
    taskKey: row.task_key,
    runState: row.run_state,
  };
}

async function attemptDispatchId(
  client: PoolClient,
  attemptId: string,
): Promise<string | null> {
  const row = (
    await client.query<{ dispatch_id: string }>(
      'SELECT dispatch_id FROM task_attempts WHERE task_attempt_id = $1',
      [attemptId],
    )
  ).rows[0];

  return row ? row.dispatch_id : null;
}

/**
 * Successful completion (contract D.11).
 *
 * Conditionally validates task/attempt/fencing/lease, persists output,
 * transitions attempt -> SUCCEEDED and task -> SUCCEEDED, clears the
 * lease, activates downstream tasks, and runs the exact completion
 * algorithm — all in one transaction. A stale or duplicate report
 * affects zero rows (or the run is no longer RUNNING) and returns the
 * already-committed outcome without repeating activation.
 *
 * Lock ordering: workflow_runs first (loadRunContext holds the run row
 * lock), then task_executions, then task_attempts — matching the
 * start path so concurrent worker transactions serialize on the run.
 *
 * Invariant: after the task state update, the matching attempt
 * transition MUST affect exactly one row; otherwise the transaction is
 * aborted so a task and its attempt cannot commit inconsistent
 * outcomes.
 */
export async function completeSuccess(
  database: DatabaseService,
  input: CompleteSuccessInput,
): Promise<ReportResult> {
  return database.transaction(async (client) => {
    const context = await loadRunContext(client, input.taskExecutionId);

    if (!context) {
      return { applied: false };
    }

    if (context.runState !== 'RUNNING') {
      const task = (
        await client.query<{ state: string }>(
          'SELECT state FROM task_executions WHERE task_execution_id = $1',
          [input.taskExecutionId],
        )
      ).rows[0];

      return {
        applied: false,
        runState: context.runState,
        taskState: task?.state,
      };
    }

    const outputJson = JSON.stringify(input.output);

    const updated = await client.query(
      `
      UPDATE task_executions
      SET state = 'SUCCEEDED',
          output_payload = $2,
          failure_code = NULL,
          failure_message = NULL,
          lease_owner = NULL,
          lease_expires_at = NULL,
          completed_at = now(),
          updated_at = now()
      WHERE task_execution_id = $1
        AND state = 'RUNNING'
        AND fencing_token = $3
        AND last_attempt_id = $4
        AND lease_expires_at IS NOT NULL
        AND lease_expires_at > now()
      RETURNING task_execution_id
    `,
    [
      input.taskExecutionId,
      outputJson,
      input.fencingToken,
      input.attemptId,
    ]);

    if (updated.rowCount === 0) {
      const task = (
        await client.query<{ state: string }>(
          'SELECT state FROM task_executions WHERE task_execution_id = $1',
          [input.taskExecutionId],
        )
      ).rows[0];

      return {
        applied: false,
        runState: 'RUNNING',
        taskState: task?.state,
      };
    }

    const attemptResult = await client.query(
      `
      UPDATE task_attempts
      SET state = 'SUCCEEDED',
          result_payload = $2,
          finished_at = now()
      WHERE task_attempt_id = $1
        AND state = 'STARTED'
    `,
      [input.attemptId, outputJson],
    );

    if (attemptResult.rowCount !== 1) {
      throw new Error(
        `Attempt ${input.attemptId} transition to SUCCEEDED affected ${attemptResult.rowCount} rows; aborting completion transaction`,
      );
    }

    const dispatchId = await attemptDispatchId(client, input.attemptId);

    await insertOutboxEvent(client, {
      eventType: OutboxEventType.TaskSucceeded,
      aggregateType: 'task_execution',
      aggregateId: input.taskExecutionId,
      tenantId: context.tenantId,
      workflowId: context.workflowId,
      workflowVersionId: context.workflowVersionId,
      workflowRunId: context.workflowRunId,
      taskExecutionId: input.taskExecutionId,
      taskAttemptId: input.attemptId,
      correlationId: context.correlationId,
      causationId: dispatchId ?? undefined,
      producer: `nexusflow/worker:${input.workerId}`,
      payload: {
        workflowRunId: context.workflowRunId,
        taskExecutionId: input.taskExecutionId,
        taskKey: context.taskKey,
        taskAttemptId: input.attemptId,
        workerId: input.workerId,
      },
    });

    await activateDownstreamTasks(
      client,
      context,
      context.taskDefinitionId,
    );

    const runState = await evaluateWorkflowCompletion(client, context, null);

    return { applied: true, runState };
  });
}

export interface CompleteFailureInput extends CommonReportInput {
  readonly failure: {
    readonly code: string;
    readonly message: string;
    readonly retryable: boolean;
    readonly timedOut?: boolean;
  };
  /** attempt_number of the reporting attempt (1-based). */
  readonly attemptNumber: number;
  readonly maxAttempts: number;
  readonly retryDelayMs: number;
  readonly failurePolicy: 'FAIL_WORKFLOW' | 'SKIP_DOWNSTREAM';
}

/**
 * Failed completion (contract D.12).
 *
 * Persists the structured failure, transitions the attempt to
 * FAILED/TIMED_OUT, and either schedules a retry (RETRY_WAIT with
 * next_attempt_at) or applies the task's failure policy
 * (FAIL_WORKFLOW -> FAILED, SKIP_DOWNSTREAM -> SKIPPED), then activates
 * or blocks downstream tasks and evaluates workflow completion — all in
 * one transaction.
 *
 * Lock ordering: workflow_runs first (loadRunContext), then
 * task_executions, then task_attempts — matching the start path.
 *
 * Invariant: after the task state update, the matching attempt
 * transition MUST affect exactly one row; otherwise the transaction is
 * aborted so a task and its attempt cannot commit inconsistent
 * outcomes.
 */
export async function completeFailure(
  database: DatabaseService,
  input: CompleteFailureInput,
): Promise<ReportResult> {
  return database.transaction(async (client) => {
    const context = await loadRunContext(client, input.taskExecutionId);

    if (!context) {
      return { applied: false };
    }

    if (context.runState !== 'RUNNING') {
      const task = (
        await client.query<{ state: string }>(
          'SELECT state FROM task_executions WHERE task_execution_id = $1',
          [input.taskExecutionId],
        )
      ).rows[0];

      return {
        applied: false,
        runState: context.runState,
        taskState: task?.state,
      };
    }

    const retryable =
      input.failure.retryable &&
      input.attemptNumber < input.maxAttempts;

    const terminalState =
      input.failurePolicy === 'FAIL_WORKFLOW' ? 'FAILED' : 'SKIPPED';

    let taskState: string;

    if (retryable) {
      const updated = await client.query(
        `
        UPDATE task_executions
        SET state = 'RETRY_WAIT',
            next_attempt_at = now() + make_interval(secs => $2::float8),
            lease_owner = NULL,
            lease_expires_at = NULL,
            failure_code = $3,
            failure_message = $4,
            updated_at = now()
        WHERE task_execution_id = $1
          AND state = 'RUNNING'
          AND fencing_token = $5
          AND last_attempt_id = $6
          AND lease_expires_at IS NOT NULL
          AND lease_expires_at > now()
        RETURNING state
      `,
        [
          input.taskExecutionId,
          input.retryDelayMs / 1000,
          input.failure.code,
          input.failure.message,
          input.fencingToken,
          input.attemptId,
        ],
      );

      if (updated.rowCount === 0) {
        const task = (
          await client.query<{ state: string }>(
            'SELECT state FROM task_executions WHERE task_execution_id = $1',
            [input.taskExecutionId],
          )
        ).rows[0];

        return {
          applied: false,
          runState: 'RUNNING',
          taskState: task?.state,
        };
      }

      taskState = 'RETRY_WAIT';
    } else {
      const updated = await client.query(
        `
        UPDATE task_executions
        SET state = $2,
            failure_code = $3,
            failure_message = $4,
            lease_owner = NULL,
            lease_expires_at = NULL,
            completed_at = now(),
            updated_at = now()
        WHERE task_execution_id = $1
          AND state = 'RUNNING'
          AND fencing_token = $5
          AND last_attempt_id = $6
          AND lease_expires_at IS NOT NULL
          AND lease_expires_at > now()
        RETURNING state
      `,
        [
          input.taskExecutionId,
          terminalState,
          input.failure.code,
          input.failure.message,
          input.fencingToken,
          input.attemptId,
        ],
      );

      if (updated.rowCount === 0) {
        const task = (
          await client.query<{ state: string }>(
            'SELECT state FROM task_executions WHERE task_execution_id = $1',
            [input.taskExecutionId],
          )
        ).rows[0];

        return {
          applied: false,
          runState: 'RUNNING',
          taskState: task?.state,
        };
      }

      taskState = terminalState;
    }

    const attemptResult = await client.query(
      `
      UPDATE task_attempts
      SET state = $2,
          failure_code = $3,
          failure_message = $4,
          finished_at = now()
      WHERE task_attempt_id = $1
        AND state = 'STARTED'
    `,
      [
        input.attemptId,
        input.failure.timedOut ? 'TIMED_OUT' : 'FAILED',
        input.failure.code,
        input.failure.message,
      ],
    );

    if (attemptResult.rowCount !== 1) {
      throw new Error(
        `Attempt ${input.attemptId} transition to ${input.failure.timedOut ? 'TIMED_OUT' : 'FAILED'} affected ${attemptResult.rowCount} rows; aborting completion transaction`,
      );
    }

    const dispatchId = await attemptDispatchId(client, input.attemptId);

    if (retryable) {
      await insertOutboxEvent(client, {
        eventType: OutboxEventType.TaskRetryScheduled,
        aggregateType: 'task_execution',
        aggregateId: input.taskExecutionId,
        tenantId: context.tenantId,
        workflowId: context.workflowId,
        workflowVersionId: context.workflowVersionId,
        workflowRunId: context.workflowRunId,
        taskExecutionId: input.taskExecutionId,
        taskAttemptId: input.attemptId,
        correlationId: context.correlationId,
        causationId: dispatchId ?? undefined,
        producer: `nexusflow/worker:${input.workerId}`,
        payload: {
          workflowRunId: context.workflowRunId,
          taskExecutionId: input.taskExecutionId,
          taskKey: context.taskKey,
          attemptNumber: input.attemptNumber,
          retryDelayMs: input.retryDelayMs,
          failureCode: input.failure.code,
        },
      });
    } else {
      await insertOutboxEvent(client, {
        eventType: OutboxEventType.TaskFailed,
        aggregateType: 'task_execution',
        aggregateId: input.taskExecutionId,
        tenantId: context.tenantId,
        workflowId: context.workflowId,
        workflowVersionId: context.workflowVersionId,
        workflowRunId: context.workflowRunId,
        taskExecutionId: input.taskExecutionId,
        taskAttemptId: input.attemptId,
        correlationId: context.correlationId,
        causationId: dispatchId ?? undefined,
        producer: `nexusflow/worker:${input.workerId}`,
        payload: {
          workflowRunId: context.workflowRunId,
          taskExecutionId: input.taskExecutionId,
          taskKey: context.taskKey,
          taskAttemptId: input.attemptId,
          outcome: terminalState,
          failureCode: input.failure.code,
          failureMessage: input.failure.message,
        },
      });

      await activateDownstreamTasks(
        client,
        context,
        context.taskDefinitionId,
      );
    }

    const runState = await evaluateWorkflowCompletion(
      client,
      context,
      !retryable && input.failurePolicy === 'FAIL_WORKFLOW'
        ? {
            failureCode: input.failure.code,
            failureMessage: input.failure.message,
            taskExecutionId: input.taskExecutionId,
            taskKey: context.taskKey,
          }
        : null,
    );

    return { applied: true, runState, taskState };
  });
}
