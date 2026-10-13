import { WorkflowValidationError } from '../domain/workflow-errors';

export interface ParsedCreateWorkflow {
  readonly name: string;
  readonly description?: string;
}

export interface ParsedTaskDefinition {
  readonly taskKey: string;
  readonly handlerType: string;
  readonly handlerVersion: string;
  readonly configuration: Record<string, unknown>;
  readonly inputMapping: Record<string, unknown>;
  readonly outputMapping: Record<string, unknown>;
  readonly retryPolicy: { maxAttempts: number; retryDelayMs?: number };
  readonly timeoutMs: number;
  readonly failurePolicy: string;
  readonly dependencies: string[];
}

export interface ParsedVersionDefinition {
  readonly name?: string;
  readonly description?: string;
  readonly tasks: ParsedTaskDefinition[];
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return (
    typeof value === 'object' &&
    value !== null &&
    !Array.isArray(value)
  );
}

function fail(message: string): never {
  throw new WorkflowValidationError(message);
}

export function parseCreateWorkflowBody(
  body: unknown,
): ParsedCreateWorkflow {
  if (!isPlainObject(body)) {
    fail('Request body must be a JSON object');
  }

  if (typeof body.name !== 'string' || body.name.trim().length === 0) {
    fail('name is required and must be a non-empty string');
  }

  if (
    body.description !== undefined &&
    (typeof body.description !== 'string' ||
      body.description.trim().length === 0)
  ) {
    fail('description must be a non-empty string when provided');
  }

  return {
    name: body.name,
    ...(body.description !== undefined ? { description: body.description } : {}),
  };
}

export function parseVersionBody(body: unknown): ParsedVersionDefinition {
  if (!isPlainObject(body)) {
    fail('Request body must be a JSON object');
  }

  if (body.tasks === undefined || !Array.isArray(body.tasks)) {
    fail('tasks is required and must be a non-empty array');
  }

  if (body.tasks.length === 0) {
    fail('tasks must contain at least one task');
  }

  if (
    body.name !== undefined &&
    (typeof body.name !== 'string' || body.name.trim().length === 0)
  ) {
    fail('name must be a non-empty string when provided');
  }

  if (
    body.description !== undefined &&
    typeof body.description !== 'string'
  ) {
    fail('description must be a string when provided');
  }

  const tasks = body.tasks.map((raw, i) => parseTaskBody(raw, i));

  return {
    ...(body.name !== undefined ? { name: body.name } : {}),
    ...(body.description !== undefined
      ? { description: body.description }
      : {}),
    tasks,
  };
}

function parseTaskBody(raw: unknown, index: number): ParsedTaskDefinition {
  const where = `tasks[${index}]`;

  if (!isPlainObject(raw)) {
    fail(`${where} must be an object`);
  }

  if (typeof raw.taskKey !== 'string' || raw.taskKey.trim().length === 0) {
    fail(`${where}.taskKey is required and must be a non-empty string`);
  }

  if (
    typeof raw.handlerType !== 'string' ||
    raw.handlerType.trim().length === 0
  ) {
    fail(`${where}.handlerType is required and must be a non-empty string`);
  }

  if (
    typeof raw.handlerVersion !== 'string' ||
    raw.handlerVersion.trim().length === 0
  ) {
    fail(`${where}.handlerVersion is required and must be a non-empty string`);
  }

  if (raw.configuration !== undefined && !isPlainObject(raw.configuration)) {
    fail(`${where}.configuration must be a JSON object`);
  }

  if (raw.inputMapping !== undefined && !isPlainObject(raw.inputMapping)) {
    fail(`${where}.inputMapping must be a JSON object`);
  }

  if (raw.outputMapping !== undefined && !isPlainObject(raw.outputMapping)) {
    fail(`${where}.outputMapping must be a JSON object`);
  }

  const retryPolicy: { maxAttempts: number; retryDelayMs?: number } = {
    maxAttempts: 1,
  };

  if (raw.retryPolicy !== undefined) {
    if (!isPlainObject(raw.retryPolicy)) {
      fail(`${where}.retryPolicy must be a JSON object`);
    }

    const maxAttempts = raw.retryPolicy.maxAttempts;

    if (
      typeof maxAttempts !== 'number' ||
      !Number.isInteger(maxAttempts) ||
      maxAttempts < 1
    ) {
      fail(`${where}.retryPolicy.maxAttempts must be a positive integer`);
    }

    retryPolicy.maxAttempts = maxAttempts;

    if (raw.retryPolicy.retryDelayMs !== undefined) {
      const delay = raw.retryPolicy.retryDelayMs;

      if (
        typeof delay !== 'number' ||
        !Number.isInteger(delay) ||
        delay < 0
      ) {
        fail(`${where}.retryPolicy.retryDelayMs must be a non-negative integer`);
      }

      retryPolicy.retryDelayMs = delay;
    }
  }

  const timeoutMs = raw.timeoutMs;

  if (
    typeof timeoutMs !== 'number' ||
    !Number.isInteger(timeoutMs) ||
    timeoutMs <= 0
  ) {
    fail(`${where}.timeoutMs is required and must be a positive integer`);
  }

  if (raw.failurePolicy !== 'FAIL_WORKFLOW' && raw.failurePolicy !== 'SKIP_DOWNSTREAM') {
    fail(
      `${where}.failurePolicy must be 'FAIL_WORKFLOW' or 'SKIP_DOWNSTREAM' (IGNORE_FAILURE is not a valid Phase 1 policy)`,
    );
  }

  if (raw.dependencies !== undefined) {
    if (!Array.isArray(raw.dependencies)) {
      fail(`${where}.dependencies must be an array of task keys`);
    }

    for (const dep of raw.dependencies) {
      if (typeof dep !== 'string' || dep.trim().length === 0) {
        fail(`${where}.dependencies entries must be non-empty strings`);
      }
    }
  }

  return {
    taskKey: raw.taskKey,
    handlerType: raw.handlerType,
    handlerVersion: raw.handlerVersion,
    configuration: isPlainObject(raw.configuration) ? raw.configuration : {},
    inputMapping: isPlainObject(raw.inputMapping) ? raw.inputMapping : {},
    outputMapping: isPlainObject(raw.outputMapping)
      ? raw.outputMapping
      : {},
    retryPolicy,
    timeoutMs,
    failurePolicy: raw.failurePolicy,
    dependencies: Array.isArray(raw.dependencies)
      ? [...(raw.dependencies as string[])]
      : [],
  };
}

export interface ParsedStartRun {
  readonly workflowVersionId?: string;
  readonly input?: Record<string, unknown>;
  readonly idempotencyKey?: string;
}

const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export function parseStartRunBody(body: unknown): ParsedStartRun {
  if (!isPlainObject(body)) {
    fail('Request body must be a JSON object (or send an empty body)');
  }

  if (
    body.workflowVersionId !== undefined &&
    (typeof body.workflowVersionId !== 'string' ||
      !UUID_PATTERN.test(body.workflowVersionId))
  ) {
    fail('workflowVersionId must be a UUID when provided');
  }

  if (body.input !== undefined && !isPlainObject(body.input)) {
    fail('input must be a JSON object when provided');
  }

  if (
    body.idempotencyKey !== undefined &&
    (typeof body.idempotencyKey !== 'string' ||
      body.idempotencyKey.trim().length === 0)
  ) {
    fail('idempotencyKey must be a non-empty string when provided');
  }

  return {
    ...(body.workflowVersionId !== undefined
      ? { workflowVersionId: body.workflowVersionId }
      : {}),
    ...(body.input !== undefined ? { input: body.input } : {}),
    ...(body.idempotencyKey !== undefined
      ? { idempotencyKey: body.idempotencyKey }
      : {}),
  };
}

export function parseTenantId(
  headerValue: string | undefined,
  defaultTenantId: string,
): string {
  const value = headerValue?.trim();

  if (!value) {
    return defaultTenantId;
  }

  if (!UUID_PATTERN.test(value)) {
    fail('X-Tenant-Id must be a valid UUID');
  }

  return value.toLowerCase();
}
