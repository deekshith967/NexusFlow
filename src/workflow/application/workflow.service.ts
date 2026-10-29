import type { PoolClient } from 'pg';

import { Injectable, Logger } from '@nestjs/common';

import { DatabaseService } from '../../database/database.service';
import {
  DomainError,
  WorkflowNameConflictError,
  WorkflowNotFoundError,
  WorkflowVersionNotFoundError,
  isUniqueViolation,
} from '../../common/errors';
import { OutboxEventType } from '../../outbox/events';
import { insertOutboxEvent } from '../../outbox/outbox-repository';
import { createBuiltinRegistry } from '../../worker/handlers/builtins';
import { DagValidator } from '../domain/dag-validator';
import { computeDefinitionHash } from '../domain/definition-hash';
import { WorkflowDefinition } from '../domain/workflow-definition';
import { FailurePolicy } from '../domain/failure-policy';
import {
  InvalidWorkflowVersionTransitionError,
  PublishedWorkflowVersionImmutableError,
  WorkflowValidationError,
} from '../domain/workflow-errors';
import { WorkflowVersionState } from '../domain/workflow-version-state';
import {
  createWorkflowIdentity,
  discardWorkflowVersion,
  findWorkflowForUpdate,
  findWorkflowVersionForUpdate,
  insertDraftVersion,
  insertTaskGraph,
  deleteTaskGraph,
  listWorkflows,
  listWorkflowVersions,
  loadVersionDefinition,
  nextVersionNumber,
  publishWorkflowVersion,
  retireWorkflowVersion,
  type InsertTaskInput,
} from '../persistence/workflow-repository';
import type {
  VersionDefinition,
  WorkflowRow,
  WorkflowVersionRow,
} from '../persistence/rows';

const TENANT_UNIQUE_INDEX = 'workflows_tenant_name_active_unique';

export interface CreateWorkflowInput {
  readonly tenantId: string;
  readonly name: string;
  readonly description?: string;
}

export interface CreateDraftVersionInput {
  readonly tenantId: string;
  readonly workflowId: string;
  readonly definition: WorkflowDefinition;
}

export interface UpdateDraftDefinitionInput {
  readonly tenantId: string;
  readonly workflowVersionId: string;
  readonly definition: WorkflowDefinition;
}

export interface ValidationReport {
  readonly valid: boolean;
  readonly errors: readonly string[];
}

/**
 * Application service for workflow identity, draft versions, validation,
 * publication, and retirement (contract D.1, D.2, D.3, D.4, B.1).
 */
@Injectable()
export class WorkflowService {
  private readonly logger = new Logger(WorkflowService.name);
  private readonly validator = new DagValidator();

  /**
   * Handler references a worker can resolve (contract D.2 step 5,
   * D.3 step 3). Phase 1 workers execute only the built-in registry, so
   * a definition naming any other (type, version) could never run.
   */
  private readonly knownHandlers: ReadonlySet<string> = new Set(
    createBuiltinRegistry().list(),
  );

  private validateDefinition(definition: WorkflowDefinition): void {
    this.validator.validate(definition);

    for (const task of definition.tasks) {
      const key = `${task.handlerType}:${task.handlerVersion}`;

      if (!this.knownHandlers.has(key)) {
        throw new WorkflowValidationError(
          `Unknown handler '${key}' for task '${task.taskKey}'; registered handlers: ${[...this.knownHandlers].sort().join(', ')}`,
        );
      }
    }
  }

  constructor(private readonly database: DatabaseService) {}

  async createWorkflow(input: CreateWorkflowInput): Promise<WorkflowRow> {
    if (typeof input.name !== 'string' || input.name.trim().length === 0) {
      throw new WorkflowValidationError('Workflow name is required');
    }

    if (
      input.description !== undefined &&
      (typeof input.description !== 'string' || input.description.trim().length === 0)
    ) {
      throw new WorkflowValidationError(
        'Workflow description must be a non-empty string when provided',
      );
    }

    try {
      return await this.database.transaction((client) =>
        createWorkflowIdentity(client, {
          tenantId: input.tenantId,
          // Stored exactly as provided; only blank names are rejected.
          name: input.name,
          description: input.description ?? null,
        }),
      );
    } catch (error) {
      // Map ONLY the tenant/name unique violation to a domain conflict.
      // Other unique violations indicate a different (bug) condition and
      // must not be silently reported as a name conflict.
      if (isUniqueViolation(error, TENANT_UNIQUE_INDEX)) {
        throw new WorkflowNameConflictError(input.tenantId, input.name);
      }

      throw error;
    }
  }

  async getWorkflow(
    tenantId: string,
    workflowId: string,
  ): Promise<WorkflowRow> {
    const workflow = await this.database.query<WorkflowRow>(
      `
      SELECT workflow_id, tenant_id, name, description, status,
             created_at, updated_at, archived_at
      FROM workflows
      WHERE workflow_id = $1
    `,
      [workflowId],
    );
    const row = workflow.rows[0];

    if (!row || row.tenant_id !== tenantId) {
      throw new WorkflowNotFoundError(workflowId);
    }

    return row;
  }

  async listWorkflows(tenantId: string): Promise<WorkflowRow[]> {
    return this.database.transaction((client) => listWorkflows(client, tenantId));
  }

  async createDraftVersion(
    input: CreateDraftVersionInput,
  ): Promise<WorkflowVersionRow> {
    this.validateDefinition(input.definition);

    return this.database.transaction((client) => {
      const workflow = findWorkflowForUpdate(client, input.workflowId);

      return workflow.then(async (found) => {
        if (!found || found.tenant_id !== input.tenantId) {
          throw new WorkflowNotFoundError(input.workflowId);
        }

        const versionNumber = await nextVersionNumber(client, input.workflowId);
        const version = await insertDraftVersion(client, {
          workflowId: input.workflowId,
          tenantId: input.tenantId,
          versionNumber,
        });

        await insertTaskGraph(
          client,
          version.workflow_version_id,
          toInsertTasks(input.definition),
          dependencyKeyPairs(input.definition),
        );

        return version;
      });
    });
  }

  async updateDraftDefinition(
    input: UpdateDraftDefinitionInput,
  ): Promise<WorkflowVersionRow> {
    this.validateDefinition(input.definition);

    return this.database.transaction((client) =>
      this.replaceDraftDefinition(client, input),
    );
  }

  private async replaceDraftDefinition(
    client: PoolClient,
    input: UpdateDraftDefinitionInput,
  ): Promise<WorkflowVersionRow> {
    const existing = await loadVersionDefinition(
      client,
      input.workflowVersionId,
      true,
    );

    if (!existing) {
      throw new WorkflowVersionNotFoundError(input.workflowVersionId);
    }

    assertVersionTenant(existing.version, input.tenantId);

    if (existing.version.state !== WorkflowVersionState.DRAFT) {
      if (existing.version.state === WorkflowVersionState.PUBLISHED) {
        throw new PublishedWorkflowVersionImmutableError();
      }

      throw new InvalidWorkflowVersionTransitionError(
        existing.version.state,
        WorkflowVersionState.DRAFT,
      );
    }

    await deleteTaskGraph(client, input.workflowVersionId);
    await insertTaskGraph(
      client,
      input.workflowVersionId,
      toInsertTasks(input.definition),
      dependencyKeyPairs(input.definition),
    );

    return existing.version;
  }

  async getValidationReport(
    tenantId: string,
    workflowVersionId: string,
  ): Promise<ValidationReport> {
    const definition = await this.database.transaction((client) =>
      loadVersionDefinition(client, workflowVersionId, false),
    );

    if (!definition) {
      throw new WorkflowVersionNotFoundError(workflowVersionId);
    }

    assertVersionTenant(definition.version, tenantId);

    const errors = collectValidationErrors(
      (d) => this.validateDefinition(d),
      domainDefinitionFrom(definition),
    );

    return { valid: errors.length === 0, errors };
  }

  async publish(
    tenantId: string,
    workflowVersionId: string,
  ): Promise<WorkflowVersionRow> {
    const version = await this.database.transaction(async (client) => {
      const definition = await loadVersionDefinition(client, workflowVersionId, true);

      if (!definition) {
        throw new WorkflowVersionNotFoundError(workflowVersionId);
      }

      assertVersionTenant(definition.version, tenantId);

      if (definition.version.state !== WorkflowVersionState.DRAFT) {
        throw new InvalidWorkflowVersionTransitionError(
          definition.version.state,
          WorkflowVersionState.PUBLISHED,
        );
      }

      const domainDefinition = domainDefinitionFrom(definition);

      for (const error of collectValidationErrors(
        (d) => this.validateDefinition(d),
        domainDefinition,
      )) {
        throw new WorkflowValidationError(error);
      }

      const hash = computeDefinitionHash(domainDefinition);
      const published = await publishWorkflowVersion(
        client,
        workflowVersionId,
        hash,
      );

      if (!published) {
        // Lost a race with a concurrent transition.
        throw new InvalidWorkflowVersionTransitionError(
          'DRAFT',
          WorkflowVersionState.PUBLISHED,
        );
      }

      await insertOutboxEvent(client, {
        eventType: OutboxEventType.WorkflowVersionPublished,
        aggregateType: 'workflow_version',
        aggregateId: published.workflow_version_id,
        tenantId: published.tenant_id,
        workflowId: published.workflow_id,
        workflowVersionId: published.workflow_version_id,
        correlationId: published.workflow_version_id,
        producer: 'nexusflow/api',
        payload: {
          workflowVersionId: published.workflow_version_id,
          workflowId: published.workflow_id,
          versionNumber: published.version_number,
          definitionHash: hash,
        },
      });

      return published;
    });

    this.logger.log(
      `Published workflow version ${version.workflow_version_id} (v${version.version_number})`,
    );

    return version;
  }

  async retire(
    tenantId: string,
    workflowVersionId: string,
  ): Promise<WorkflowVersionRow> {
    const version = await this.database.transaction(async (client) => {
      const existing = await findWorkflowVersionForUpdate(client, workflowVersionId);

      if (!existing) {
        throw new WorkflowVersionNotFoundError(workflowVersionId);
      }

      assertVersionTenant(existing, tenantId);

      if (existing.state !== WorkflowVersionState.PUBLISHED) {
        throw new InvalidWorkflowVersionTransitionError(
          existing.state,
          WorkflowVersionState.RETIRED,
        );
      }

      const retired = await retireWorkflowVersion(client, workflowVersionId);

      if (!retired) {
        throw new InvalidWorkflowVersionTransitionError(
          existing.state,
          WorkflowVersionState.RETIRED,
        );
      }

      await insertOutboxEvent(client, {
        eventType: OutboxEventType.WorkflowVersionRetired,
        aggregateType: 'workflow_version',
        aggregateId: retired.workflow_version_id,
        tenantId: retired.tenant_id,
        workflowId: retired.workflow_id,
        workflowVersionId: retired.workflow_version_id,
        correlationId: retired.workflow_version_id,
        producer: 'nexusflow/api',
        payload: {
          workflowVersionId: retired.workflow_version_id,
          workflowId: retired.workflow_id,
          versionNumber: retired.version_number,
        },
      });

      return retired;
    });

    return version;
  }

  async discard(
    tenantId: string,
    workflowVersionId: string,
  ): Promise<WorkflowVersionRow> {
    const version = await this.database.transaction(async (client) => {
      const existing = await findWorkflowVersionForUpdate(client, workflowVersionId);

      if (!existing) {
        throw new WorkflowVersionNotFoundError(workflowVersionId);
      }

      assertVersionTenant(existing, tenantId);

      if (existing.state !== WorkflowVersionState.DRAFT) {
        throw new InvalidWorkflowVersionTransitionError(
          existing.state,
          WorkflowVersionState.DISCARDED,
        );
      }

      const discarded = await discardWorkflowVersion(client, workflowVersionId);

      if (!discarded) {
        throw new InvalidWorkflowVersionTransitionError(
          existing.state,
          WorkflowVersionState.DISCARDED,
        );
      }

      return discarded;
    });

    return version;
  }

  async getVersion(
    tenantId: string,
    workflowVersionId: string,
  ): Promise<VersionDefinition> {
    const definition = await this.database.transaction((client) =>
      loadVersionDefinition(client, workflowVersionId, false),
    );

    if (!definition) {
      throw new WorkflowVersionNotFoundError(workflowVersionId);
    }

    assertVersionTenant(definition.version, tenantId);

    return definition;
  }

  async listVersions(
    tenantId: string,
    workflowId: string,
  ): Promise<WorkflowVersionRow[]> {
    // Verify the workflow belongs to the tenant first (404 otherwise),
    // then list its versions.
    await this.getWorkflow(tenantId, workflowId);

    return this.database.transaction((client) =>
      listWorkflowVersions(client, workflowId),
    );
  }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Tenant isolation for version-scoped operations. A version owned by a
 * different tenant is reported as not found (404 at the API boundary),
 * revealing nothing about its existence or state.
 */
function assertVersionTenant(
  version: WorkflowVersionRow,
  tenantId: string,
): void {
  if (version.tenant_id !== tenantId) {
    throw new WorkflowVersionNotFoundError(version.workflow_version_id);
  }
}

function toInsertTasks(
  definition: WorkflowDefinition,
): InsertTaskInput[] {
  return definition.tasks.map((task) => ({
    taskKey: task.taskKey,
    handlerType: task.handlerType,
    handlerVersion: task.handlerVersion,
    configuration: { ...task.configuration },
    inputMapping: { ...task.inputMapping },
    outputMapping: { ...task.outputMapping },
    retryPolicy: {
      maxAttempts: task.retryPolicy.maxAttempts,
      ...(task.retryPolicy.retryDelayMs !== undefined
        ? { retryDelayMs: task.retryPolicy.retryDelayMs }
        : {}),
    },
    timeoutMs: task.timeoutMs,
    failurePolicy: task.failurePolicy,
  }));
}

function dependencyKeyPairs(
  definition: WorkflowDefinition,
): (readonly [string, string])[] {
  const pairs: (readonly [string, string])[] = [];

  for (const task of definition.tasks) {
    for (const dependency of task.dependencies) {
      pairs.push([dependency, task.taskKey]);
    }
  }

  return pairs;
}

export function domainDefinitionFrom(
  definition: VersionDefinition,
): WorkflowDefinition {
  const dependenciesByDownstream = new Map<string, string[]>();

  for (const edge of definition.dependencies) {
    const upstreamTask = definition.tasks.find(
      (t) => t.task_definition_id === edge.upstream_task_definition_id,
    );
    const downstreamTask = definition.tasks.find(
      (t) => t.task_definition_id === edge.downstream_task_definition_id,
    );

    if (!upstreamTask || !downstreamTask) {
      continue;
    }

    const list =
      dependenciesByDownstream.get(downstreamTask.task_key) ?? [];

    list.push(upstreamTask.task_key);
    dependenciesByDownstream.set(downstreamTask.task_key, list);
  }

  return {
    name: definition.workflow?.name ?? 'unknown-workflow',
    ...(definition.workflow?.description !== null &&
    definition.workflow?.description !== undefined
      ? { description: definition.workflow.description }
      : {}),
    tasks: definition.tasks.map((task) => ({
      taskKey: task.task_key,
      handlerType: task.handler_type,
      handlerVersion: task.handler_version,
      configuration: { ...task.configuration },
      inputMapping: { ...task.input_mapping },
      outputMapping: { ...task.output_mapping },
      retryPolicy: {
        maxAttempts: task.retry_policy.maxAttempts,
        ...(task.retry_policy.retryDelayMs !== undefined
          ? { retryDelayMs: task.retry_policy.retryDelayMs }
          : {}),
      },
      timeoutMs: task.timeout_ms,
      failurePolicy: task.failure_policy as FailurePolicy,
      dependencies: dependenciesByDownstream.get(task.task_key) ?? [],
    })),
  };
}

function collectValidationErrors(
  validate: (definition: WorkflowDefinition) => void,
  definition: WorkflowDefinition,
): string[] {
  const errors: string[] = [];

  // Validate the whole definition; the validator throws on the first
  // violation, so also check per-task for a friendlier report.
  try {
    validate(definition);
    return [];
  } catch (error) {
    errors.push(
      error instanceof DomainError || error instanceof Error
        ? error.message
        : 'Unknown validation error',
    );
  }

  return errors;
}
