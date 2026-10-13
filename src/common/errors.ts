/**
 * Base class for domain-level errors. The API layer maps these to HTTP
 * responses; persistence/scheduler/worker layers should never surface raw
 * database diagnostics through these classes.
 */
export class DomainError extends Error {
  constructor(message: string) {
    super(message);
    this.name = new.target.name;
  }
}

/** A tenant-scoped workflow with this name already exists (not archived). */
export class WorkflowNameConflictError extends DomainError {
  readonly tenantId: string;
  readonly workflowName: string;

  constructor(tenantId: string, workflowName: string) {
    super(
      `A workflow named '${workflowName}' already exists for this tenant. Workflow names are case-sensitive.`,
    );
    this.name = 'WorkflowNameConflictError';
    this.tenantId = tenantId;
    this.workflowName = workflowName;
  }
}

export class WorkflowNotFoundError extends DomainError {
  constructor(workflowId: string) {
    super(`Workflow '${workflowId}' was not found`);
    this.workflowId = workflowId;
  }

  readonly workflowId: string;
}

export class WorkflowVersionNotFoundError extends DomainError {
  constructor(workflowVersionId: string) {
    super(`Workflow version '${workflowVersionId}' was not found`);
    this.workflowVersionId = workflowVersionId;
  }

  readonly workflowVersionId: string;
}

export class WorkflowRunNotFoundError extends DomainError {
  constructor(workflowRunId: string) {
    super(`Workflow run '${workflowRunId}' was not found`);
    this.workflowRunId = workflowRunId;
  }

  readonly workflowRunId: string;
}

/** The selected version cannot start a run (not PUBLISHED, retired, ...). */
export class RunLaunchRejectedError extends DomainError {
  constructor(reason: string) {
    super(reason);
    this.reason = reason;
  }

  readonly reason: string;
}

/** True when the error is a PostgreSQL unique-violation (23505). */
export function isUniqueViolation(
  error: unknown,
  constraintName?: string,
): boolean {
  if (typeof error !== 'object' || error === null) {
    return false;
  }

  const candidate = error as { code?: unknown; constraint?: unknown };

  if (candidate.code !== '23505') {
    return false;
  }

  if (constraintName !== undefined) {
    return candidate.constraint === constraintName;
  }

  return true;
}
