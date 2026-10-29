import { randomUUID } from 'node:crypto';
import { Injectable, Logger } from '@nestjs/common';
import type { PoolClient } from 'pg';

import { DatabaseService } from '../../database/database.service';
import {
  RunLaunchRejectedError,
  WorkflowNotFoundError,
  WorkflowRunNotFoundError,
  WorkflowVersionNotFoundError,
} from '../../common/errors';
import { OutboxEventType } from '../../outbox/events';
import { insertOutboxEvent } from '../../outbox/outbox-repository';
import { WorkflowVersionState } from '../domain/workflow-version-state';
import {
  TaskAttemptRow,
  TaskExecutionRow,
  WorkflowRunRow,
} from '../persistence/rows';


export interface StartRunInput {
  readonly tenantId: string;
  readonly workflowId: string;
  /** Optional: defaults to the highest-numbered PUBLISHED version. */
  readonly workflowVersionId?: string;
  readonly input?: Record<string, unknown>;
  /** Optional idempotency key for tenant/workflow-scoped start calls. */
  readonly idempotencyKey?: string;
  readonly correlationId?: string;
}

export interface StartRunResult {
  readonly run: WorkflowRunRow;
  /** False when the run was returned from a previous idempotent call. */
  readonly created: boolean;
  readonly taskExecutions: TaskExecutionRow[];
}

export interface RunDetail {
  readonly run: WorkflowRunRow;
  readonly workflowVersionId: string;
  readonly taskExecutions: TaskExecutionRow[];
}

const RUN_COLUMNS = `
  workflow_run_id, tenant_id, workflow_id, workflow_version_id, state,
  idempotency_key, input_payload, output_payload, correlation_id,
  created_at, started_at, completed_at, failure_code, failure_message,
  cancellation_requested_at
`;

const TASK_EXECUTION_COLUMNS = `
  task_execution_id, workflow_run_id, task_definition_id, task_key, state,
  resolved_input, output_payload, failure_code, failure_message,
  attempt_count, next_attempt_at, lease_owner, lease_expires_at,
  fencing_token, claimed_at, started_at, completed_at, last_attempt_id,
  created_at, updated_at
`;

/**
 * Application service for workflow run creation (contract D.5) and
 * read-only inspection of runs, task executions, and attempts.
 */
@Injectable()
export class RunService {
  private readonly logger = new Logger(RunService.name);

  constructor(private readonly database: DatabaseService) {}

  async startRun(input: StartRunInput): Promise<StartRunResult> {
    if (input.idempotencyKey !== undefined) {
      if (
        typeof input.idempotencyKey !== 'string' ||
        input.idempotencyKey.trim().length === 0
      ) {
        throw new RunLaunchRejectedError(
          'idempotencyKey must be a non-empty string when provided',
        );
      }
    }

    if (input.input !== undefined && (typeof input.input !== 'object' || input.input === null || Array.isArray(input.input))) {
      throw new RunLaunchRejectedError('input must be a JSON object');
    }

    return this.database.transaction((client) =>
      this.startRunInTransaction(client, input),
    );
  }

  private async startRunInTransaction(
    client: PoolClient,
    input: StartRunInput,
  ): Promise<StartRunResult> {
    // 1. Validate the selected version is PUBLISHED (and owned by the tenant).
    const version = input.workflowVersionId
      ? await client
          .query<{
              workflow_version_id: string;
              workflow_id: string;
              tenant_id: string;
              version_number: number;
              state: WorkflowVersionState;
            }>(
            `
            SELECT workflow_version_id, workflow_id, tenant_id,
                   version_number, state
            FROM workflow_versions
            WHERE workflow_version_id = $1
            FOR UPDATE
          `,
            [input.workflowVersionId],
          )
          .then((r) => r.rows[0] ?? null)
      : await client
          .query<{
              workflow_version_id: string;
              workflow_id: string;
              tenant_id: string;
              version_number: number;
              state: WorkflowVersionState;
            }>(
            `
            SELECT workflow_version_id, workflow_id, tenant_id,
                   version_number, state
            FROM workflow_versions
            WHERE workflow_id = $1
              AND tenant_id = $2
              AND state = 'PUBLISHED'
            ORDER BY version_number DESC
            LIMIT 1
            FOR UPDATE
          `,
            [input.workflowId, input.tenantId],
          )
          .then((r) => r.rows[0] ?? null);

    // Another tenant's resources are indistinguishable from missing ones
    // (ADR 001): never reveal that a foreign workflow or version exists.
    if (version && version.tenant_id !== input.tenantId) {
      throw new WorkflowVersionNotFoundError(version.workflow_version_id);
    }

    if (!version) {
      if (input.workflowVersionId) {
        throw new WorkflowVersionNotFoundError(input.workflowVersionId);
      }

      const owned = await client.query(
        'SELECT 1 FROM workflows WHERE workflow_id = $1 AND tenant_id = $2',
        [input.workflowId, input.tenantId],
      );

      if (owned.rowCount === 0) {
        throw new WorkflowNotFoundError(input.workflowId);
      }

      throw new RunLaunchRejectedError(
        `No PUBLISHED workflow version exists for workflow '${input.workflowId}'`,
      );
    }

    if (version.workflow_id !== input.workflowId) {
      throw new RunLaunchRejectedError(
        `Workflow version '${version.workflow_version_id}' does not belong to workflow '${input.workflowId}'`,
      );
    }

    if (version.state !== WorkflowVersionState.PUBLISHED) {
      throw new RunLaunchRejectedError(
        `Workflow version '${version.workflow_version_id}' is ${version.state}; only PUBLISHED versions can start runs`,
      );
    }

    const workflowRunId = randomUUID();
    const correlationId = input.correlationId ?? randomUUID();
    const idempotencyKey = input.idempotencyKey ?? null;

    // 2./3. Resolve or create the idempotency record through the unique
    // constraint; an existing idempotent run is returned as-is.
    let existing: WorkflowRunRow | null = null;

    if (idempotencyKey !== null) {
      const prior = await client.query<WorkflowRunRow>(
        `
        SELECT ${RUN_COLUMNS}
        FROM workflow_runs
        WHERE tenant_id = $1
          AND workflow_id = $2
          AND idempotency_key = $3
      `,
        [input.tenantId, input.workflowId, idempotencyKey],
      );

      existing = prior.rows[0] ?? null;
    }

    if (!existing) {
      const inserted = await client.query<{ workflow_run_id: string }>(
        `
        INSERT INTO workflow_runs (
          workflow_run_id, tenant_id, workflow_id, workflow_version_id,
          state, idempotency_key, input_payload, correlation_id, created_at
        )
        VALUES ($1, $2, $3, $4, 'CREATED', $5, $6, $7, now())
        ON CONFLICT (tenant_id, workflow_id, idempotency_key)
          WHERE idempotency_key IS NOT NULL
        DO NOTHING
        RETURNING workflow_run_id
      `,
        [
          workflowRunId,
          input.tenantId,
          input.workflowId,
          version.workflow_version_id,
          idempotencyKey,
          input.input ? JSON.stringify(input.input) : null,
          correlationId,
        ],
      );

      if (inserted.rowCount === 0) {
        // Lost an idempotency race: another transaction created the run.
        const raced = await client.query<WorkflowRunRow>(
          `
          SELECT ${RUN_COLUMNS}
          FROM workflow_runs
          WHERE tenant_id = $1
            AND workflow_id = $2
            AND idempotency_key = $3
        `,
          [input.tenantId, input.workflowId, idempotencyKey],
        );

        existing = raced.rows[0] ?? null;

        if (existing) {
          return {
            run: existing,
            created: false,
            taskExecutions: await this.loadTaskExecutions(client, existing.workflow_run_id),
          };
        }

        throw new RunLaunchRejectedError(
          'Could not resolve existing idempotent run',
        );
      }
    } else {
      // Reuse the run identified by the idempotency key.
      return {
        run: existing,
        created: false,
        taskExecutions: await this.loadTaskExecutions(client, existing.workflow_run_id),
      };
    }

    // 4./5. Load the immutable task graph and create one task execution
    // per task definition.
    const tasks = await client.query<{
      task_definition_id: string;
      task_key: string;
    }>(
      `
      SELECT task_definition_id, task_key
      FROM task_definitions
      WHERE workflow_version_id = $1
      ORDER BY task_key ASC
    `,
      [version.workflow_version_id],
    );

    if (tasks.rows.length === 0) {
      throw new RunLaunchRejectedError(
        `Workflow version '${version.workflow_version_id}' has no task definitions`,
      );
    }

    const upstreamIds = new Set(
      (
        await client.query<{ downstream: string }>(
          `
          SELECT DISTINCT downstream_task_definition_id AS downstream
          FROM task_dependencies
          WHERE workflow_version_id = $1
        `,
          [version.workflow_version_id],
        )
      ).rows.map((r) => r.downstream),
    );

    for (const task of tasks.rows) {
      const state = upstreamIds.has(task.task_definition_id)
        ? 'WAITING'
        : 'READY';

      await client.query(
        `
        INSERT INTO task_executions (
          task_execution_id, workflow_run_id, task_definition_id, task_key,
          state, attempt_count, fencing_token, created_at, updated_at
        )
        VALUES ($1, $2, $3, $4, $5, 0, 0, now(), now())
      `,
        [
          randomUUID(),
          workflowRunId,
          task.task_definition_id,
          task.task_key,
          state,
        ],
      );
    }

    // 8. The run becomes RUNNING only inside this transaction.
    await client.query(
      `
      UPDATE workflow_runs
      SET state = 'RUNNING', started_at = now()
      WHERE workflow_run_id = $1
        AND state = 'CREATED'
    `,
      [workflowRunId],
    );

    // 9. workflow.run.started
    const rootTasks = tasks.rows.filter(
      (t) => !upstreamIds.has(t.task_definition_id),
    );

    await insertOutboxEvent(client, {
      eventType: OutboxEventType.WorkflowRunStarted,
      aggregateType: 'workflow_run',
      aggregateId: workflowRunId,
      tenantId: input.tenantId,
      workflowId: input.workflowId,
      workflowVersionId: version.workflow_version_id,
      workflowRunId,
      correlationId,
      producer: 'nexusflow/api',
      payload: {
        workflowRunId,
        workflowId: input.workflowId,
        workflowVersionId: version.workflow_version_id,
        versionNumber: version.version_number,
        rootTaskKeys: rootTasks.map((t) => t.task_key),
      },
    });

    // 10. task.ready for root tasks (dispatchable only after commit).
    const createdExecutions = await this.loadTaskExecutions(
      client,
      workflowRunId,
    );

    for (const execution of createdExecutions) {
      if (execution.state !== 'READY') {
        continue;
      }

      const definition = tasks.rows.find(
        (t) => t.task_key === execution.task_key,
      );

      if (!definition) {
        continue;
      }

      await insertOutboxEvent(client, {
        eventType: OutboxEventType.TaskReady,
        aggregateType: 'task_execution',
        aggregateId: execution.task_execution_id,
        tenantId: input.tenantId,
        workflowId: input.workflowId,
        workflowVersionId: version.workflow_version_id,
        workflowRunId,
        taskExecutionId: execution.task_execution_id,
        correlationId,
        producer: 'nexusflow/api',
        payload: {
          workflowRunId,
          taskExecutionId: execution.task_execution_id,
          taskKey: execution.task_key,
          reason: 'root',
        },
      });
    }

    const run = (
      await client.query<WorkflowRunRow>(
        `SELECT ${RUN_COLUMNS} FROM workflow_runs WHERE workflow_run_id = $1`,
        [workflowRunId],
      )
    ).rows[0];

    this.logger.log(
      `Started run ${workflowRunId} for workflow ${input.workflowId} (version ${version.version_number})`,
    );

    return {
      run,
      created: true,
      taskExecutions: createdExecutions,
    };
  }

  private async loadTaskExecutions(
    client: PoolClient,
    workflowRunId: string,
  ): Promise<TaskExecutionRow[]> {
    const result = await client.query<TaskExecutionRow>(
      `
      SELECT ${TASK_EXECUTION_COLUMNS}
      FROM task_executions
      WHERE workflow_run_id = $1
      ORDER BY task_key ASC
    `,
      [workflowRunId],
    );

    return result.rows;
  }

  async getRunDetail(
    tenantId: string,
    workflowRunId: string,
  ): Promise<RunDetail> {
    return this.database.transaction(async (client) => {
      const run = await this.findRunOrThrow(client, tenantId, workflowRunId);
      const taskExecutions = await this.loadTaskExecutions(
        client,
        run.workflow_run_id,
      );

      return {
        run,
        workflowVersionId: run.workflow_version_id,
        taskExecutions,
      };
    });
  }

  private findRunOrThrow(
    client: PoolClient,
    tenantId: string,
    workflowRunId: string,
  ): Promise<WorkflowRunRow> {
    return client
      .query<WorkflowRunRow>(
        `
        SELECT ${RUN_COLUMNS}
        FROM workflow_runs
        WHERE workflow_run_id = $1
      `,
        [workflowRunId],
      )
      .then((result) => {
        const row = result.rows[0];

        if (!row || row.tenant_id !== tenantId) {
          throw new WorkflowRunNotFoundError(workflowRunId);
        }

        return row;
      });
  }

  async getRun(
    tenantId: string,
    workflowRunId: string,
  ): Promise<WorkflowRunRow> {
    return this.database.transaction((client) =>
      this.findRunOrThrow(client, tenantId, workflowRunId),
    );
  }

  async listRuns(
    tenantId: string,
    workflowId?: string,
    limit = 50,
  ): Promise<WorkflowRunRow[]> {
    return this.database.transaction((client) => {
      const sql = workflowId
        ? `
          SELECT ${RUN_COLUMNS}
          FROM workflow_runs
          WHERE tenant_id = $1 AND workflow_id = $2
          ORDER BY created_at DESC
          LIMIT $3
        `
        : `
          SELECT ${RUN_COLUMNS}
          FROM workflow_runs
          WHERE tenant_id = $1
          ORDER BY created_at DESC
          LIMIT $2
        `;

      const values = workflowId
        ? [tenantId, workflowId, limit]
        : [tenantId, limit];

      return client
        .query<WorkflowRunRow>(sql, values)
        .then((result) => result.rows);
    });
  }

  async listTaskExecutions(
    tenantId: string,
    workflowRunId: string,
  ): Promise<TaskExecutionRow[]> {
    return this.database.transaction((client) => {
      const run = this.findRunOrThrow(client, tenantId, workflowRunId);

      return run.then(() =>
        this.loadTaskExecutions(client, workflowRunId),
      );
    });
  }

  async listAttempts(
    tenantId: string,
    workflowRunId: string,
    taskExecutionId: string,
  ): Promise<TaskAttemptRow[]> {
    return this.database.transaction((client) => {
      // Verify ownership through the run's tenant, and that the task
      // belongs to the run named in the request, before reading attempts.
      const owner = client
        .query<{ runTenant: string; runId: string }>(
          `
          SELECT wr.tenant_id AS "runTenant", wr.workflow_run_id AS "runId"
          FROM task_executions te
          JOIN workflow_runs wr
            ON wr.workflow_run_id = te.workflow_run_id
          WHERE te.task_execution_id = $1
        `,
          [taskExecutionId],
        )
        .then((r) => r.rows[0] ?? null);

      return owner.then(async (row) => {
        if (
          !row ||
          row.runTenant !== tenantId ||
          row.runId !== workflowRunId
        ) {
          throw new WorkflowRunNotFoundError(taskExecutionId);
        }

        const result = await client.query<TaskAttemptRow>(
          `
          SELECT task_attempt_id, task_execution_id, attempt_number, state,
                 worker_id, scheduler_id, fencing_token, dispatch_id,
                 started_at, finished_at, timeout_at, result_payload,
                 failure_code, failure_message, created_at
          FROM task_attempts
          WHERE task_execution_id = $1
          ORDER BY attempt_number ASC
        `,
          [taskExecutionId],
        );

        return result.rows;
      });
    });
  }
}
