import { randomUUID } from 'node:crypto';
import type { PoolClient } from 'pg';

import {
  TaskDefinitionRow,
  TaskDependencyRow,
  VersionDefinition,
  WorkflowRow,
  WorkflowVersionRow,
} from './rows';

/**
 * SQL for workflow identity, version, task definition, and dependency
 * persistence. Every method operates on the supplied transactional client
 * so callers control commit/rollback (contract section D).
 */

const WORKFLOW_COLUMNS = `
  workflow_id, tenant_id, name, description, status,
  created_at, updated_at, archived_at
`;

const VERSION_COLUMNS = `
  workflow_version_id, workflow_id, tenant_id, version_number, state,
  definition_hash, created_at, published_at, retired_at
`;

const TASK_DEFINITION_COLUMNS = `
  task_definition_id, workflow_version_id, task_key, handler_type,
  handler_version, configuration, input_mapping, output_mapping,
  retry_policy, timeout_ms, failure_policy, created_at
`;

export interface InsertedTaskRef {
  readonly taskKey: string;
  readonly taskDefinitionId: string;
}

export function createWorkflowIdentity(
  client: PoolClient,
  input: { tenantId: string; name: string; description: string | null },
): Promise<WorkflowRow> {
  return client
    .query<WorkflowRow>(
      `
      INSERT INTO workflows (
        workflow_id, tenant_id, name, description, status,
        created_at, updated_at
      )
      VALUES ($1, $2, $3, $4, 'active', now(), now())
      RETURNING ${WORKFLOW_COLUMNS}
    `,
      [
        randomUUID(),
        input.tenantId,
        // Stored exactly as provided (no trimming/re-casing); blank names are
        // rejected by the service and by workflows_name_nonempty.
        input.name,
        input.description,
      ],
    )
    .then((result) => result.rows[0]);
}

export async function findWorkflow(
  client: PoolClient,
  workflowId: string,
): Promise<WorkflowRow | null> {
  const result = await client.query<WorkflowRow>(
    `SELECT ${WORKFLOW_COLUMNS} FROM workflows WHERE workflow_id = $1`,
    [workflowId],
  );

  return result.rows[0] ?? null;
}

export async function findWorkflowForUpdate(
  client: PoolClient,
  workflowId: string,
): Promise<WorkflowRow | null> {
  const result = await client.query<WorkflowRow>(
    `SELECT ${WORKFLOW_COLUMNS} FROM workflows
     WHERE workflow_id = $1
     FOR UPDATE`,
    [workflowId],
  );

  return result.rows[0] ?? null;
}

export async function listWorkflows(
  client: PoolClient,
  tenantId: string,
): Promise<WorkflowRow[]> {
  const result = await client.query<WorkflowRow>(
    `SELECT ${WORKFLOW_COLUMNS} FROM workflows
     WHERE tenant_id = $1
     ORDER BY created_at ASC, workflow_id ASC`,
    [tenantId],
  );

  return result.rows;
}

export async function findWorkflowVersion(
  client: PoolClient,
  workflowVersionId: string,
): Promise<WorkflowVersionRow | null> {
  const result = await client.query<WorkflowVersionRow>(
    `SELECT ${VERSION_COLUMNS} FROM workflow_versions
     WHERE workflow_version_id = $1`,
    [workflowVersionId],
  );

  return result.rows[0] ?? null;
}

export async function findWorkflowVersionForUpdate(
  client: PoolClient,
  workflowVersionId: string,
): Promise<WorkflowVersionRow | null> {
  const result = await client.query<WorkflowVersionRow>(
    `SELECT ${VERSION_COLUMNS} FROM workflow_versions
     WHERE workflow_version_id = $1
     FOR UPDATE`,
    [workflowVersionId],
  );

  return result.rows[0] ?? null;
}

export async function listWorkflowVersions(
  client: PoolClient,
  workflowId: string,
): Promise<WorkflowVersionRow[]> {
  const result = await client.query<WorkflowVersionRow>(
    `SELECT ${VERSION_COLUMNS} FROM workflow_versions
     WHERE workflow_id = $1
     ORDER BY version_number ASC`,
    [workflowId],
  );

  return result.rows;
}

export async function nextVersionNumber(
  client: PoolClient,
  workflowId: string,
): Promise<number> {
  const result = await client.query<{ next: number }>(
    `SELECT COALESCE(MAX(version_number), 0) + 1 AS next
     FROM workflow_versions
     WHERE workflow_id = $1`,
    [workflowId],
  );

  return result.rows[0].next;
}

export async function insertDraftVersion(
  client: PoolClient,
  input: {
    workflowId: string;
    tenantId: string;
    versionNumber: number;
  },
): Promise<WorkflowVersionRow> {
  const result = await client.query<WorkflowVersionRow>(
    `
    INSERT INTO workflow_versions (
      workflow_version_id, workflow_id, tenant_id, version_number, state,
      created_at
    )
    VALUES ($1, $2, $3, $4, 'DRAFT', now())
    RETURNING ${VERSION_COLUMNS}
  `,
    [
      randomUUID(),
      input.workflowId,
      input.tenantId,
      input.versionNumber,
    ],
  );

  return result.rows[0];
}

export interface InsertTaskInput {
  readonly taskKey: string;
  readonly handlerType: string;
  readonly handlerVersion: string;
  readonly configuration: Record<string, unknown>;
  readonly inputMapping: Record<string, unknown>;
  readonly outputMapping: Record<string, unknown>;
  readonly retryPolicy: { maxAttempts: number; retryDelayMs?: number };
  readonly timeoutMs: number;
  readonly failurePolicy: string;
}

/**
 * Inserts task definitions and dependency edges for a version. Returns
 * task_key -> task_definition_id references for edge resolution.
 */
export async function insertTaskGraph(
  client: PoolClient,
  workflowVersionId: string,
  tasks: readonly InsertTaskInput[],
  dependencyKeys: readonly (readonly [string, string])[],
): Promise<Map<string, string>> {
  const refs = new Map<string, string>();

  for (const task of tasks) {
    const taskDefinitionId = randomUUID();
    refs.set(task.taskKey, taskDefinitionId);

    const result = await client.query<{ task_definition_id: string }>(
      `
      INSERT INTO task_definitions (
        task_definition_id, workflow_version_id, task_key, handler_type,
        handler_version, configuration, input_mapping, output_mapping,
        retry_policy, timeout_ms, failure_policy, created_at
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, now())
      RETURNING task_definition_id
    `,
      [
        taskDefinitionId,
        workflowVersionId,
        task.taskKey,
        task.handlerType,
        task.handlerVersion,
        JSON.stringify(task.configuration),
        JSON.stringify(task.inputMapping),
        JSON.stringify(task.outputMapping),
        JSON.stringify(task.retryPolicy),
        task.timeoutMs,
        task.failurePolicy,
      ],
    );

    refs.set(task.taskKey, result.rows[0].task_definition_id);
  }

  for (const [upstreamKey, downstreamKey] of dependencyKeys) {
    const upstreamId = refs.get(upstreamKey);
    const downstreamId = refs.get(downstreamKey);

    if (!upstreamId || !downstreamId) {
      throw new Error(
        `Dependency references unknown task: ${upstreamKey} -> ${downstreamKey}`,
      );
    }

    await client.query(
      `
      INSERT INTO task_dependencies (
        dependency_id, workflow_version_id,
        upstream_task_definition_id, downstream_task_definition_id, created_at
      )
      VALUES ($1, $2, $3, $4, now())
    `,
      [
        randomUUID(),
        workflowVersionId,
        upstreamId,
        downstreamId,
      ],
    );
  }

  return refs;
}

export async function deleteTaskGraph(
  client: PoolClient,
  workflowVersionId: string,
): Promise<void> {
  await client.query('DELETE FROM task_dependencies WHERE workflow_version_id = $1', [
    workflowVersionId,
  ]);
  await client.query('DELETE FROM task_definitions WHERE workflow_version_id = $1', [
    workflowVersionId,
  ]);
}

export async function loadVersionDefinition(
  client: PoolClient,
  workflowVersionId: string,
  lock: boolean,
): Promise<VersionDefinition | null> {
  const lockSuffix = lock ? ' FOR UPDATE' : '';

  const versionResult = await client.query<WorkflowVersionRow>(
    `SELECT ${VERSION_COLUMNS} FROM workflow_versions
     WHERE workflow_version_id = $1${lockSuffix}`,
    [workflowVersionId],
  );

  const version = versionResult.rows[0];

  if (!version) {
    return null;
  }

  const taskResult = await client.query<TaskDefinitionRow>(
    `SELECT ${TASK_DEFINITION_COLUMNS} FROM task_definitions
     WHERE workflow_version_id = $1
     ORDER BY task_key ASC`,
    [workflowVersionId],
  );

  const depResult = await client.query<TaskDependencyRow>(
    `SELECT dependency_id, workflow_version_id,
            upstream_task_definition_id, downstream_task_definition_id, created_at
     FROM task_dependencies
     WHERE workflow_version_id = $1
     ORDER BY upstream_task_definition_id ASC, downstream_task_definition_id ASC`,
    [workflowVersionId],
  );

  const workflowResult = await client.query<WorkflowRow>(
    `SELECT ${WORKFLOW_COLUMNS} FROM workflows WHERE workflow_id = $1`,
    [version.workflow_id],
  );

  return {
    version,
    workflow: workflowResult.rows[0] ?? null,
    tasks: taskResult.rows,
    dependencies: depResult.rows,
  };
}

/**
 * Conditional transition helpers. All return the updated row or null when
 * the preconditions no longer hold (state was changed concurrently).
 */
export async function publishWorkflowVersion(
  client: PoolClient,
  workflowVersionId: string,
  definitionHash: string,
): Promise<WorkflowVersionRow | null> {
  const result = await client.query<WorkflowVersionRow>(
    `
    UPDATE workflow_versions
    SET state = 'PUBLISHED',
        definition_hash = $2,
        published_at = now()
    WHERE workflow_version_id = $1
      AND state = 'DRAFT'
    RETURNING ${VERSION_COLUMNS}
  `,
    [workflowVersionId, definitionHash],
  );

  return result.rows[0] ?? null;
}

export async function retireWorkflowVersion(
  client: PoolClient,
  workflowVersionId: string,
): Promise<WorkflowVersionRow | null> {
  const result = await client.query<WorkflowVersionRow>(
    `
    UPDATE workflow_versions
    SET state = 'RETIRED',
        retired_at = now()
    WHERE workflow_version_id = $1
      AND state = 'PUBLISHED'
    RETURNING ${VERSION_COLUMNS}
  `,
    [workflowVersionId],
  );

  return result.rows[0] ?? null;
}

export async function discardWorkflowVersion(
  client: PoolClient,
  workflowVersionId: string,
): Promise<WorkflowVersionRow | null> {
  const result = await client.query<WorkflowVersionRow>(
    `
    UPDATE workflow_versions
    SET state = 'DISCARDED'
    WHERE workflow_version_id = $1
      AND state = 'DRAFT'
    RETURNING ${VERSION_COLUMNS}
  `,
    [workflowVersionId],
  );

  return result.rows[0] ?? null;
}
