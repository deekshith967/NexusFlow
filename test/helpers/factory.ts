import { DatabaseService } from '../../src/database/database.service';
import { configurePgTypeParsers } from '../../src/database/parsers';
import { RunService } from '../../src/workflow/application/run.service';
import { WorkflowService } from '../../src/workflow/application/workflow.service';
import { FailurePolicy } from '../../src/workflow/domain/failure-policy';
import type {
  TaskDefinition,
  WorkflowDefinition,
} from '../../src/workflow/domain/workflow-definition';
import type { Pool } from 'pg';

configurePgTypeParsers();

export function createWorkflowService(pool: Pool): WorkflowService {
  return new WorkflowService(new DatabaseService(pool));
}

export function createRunService(pool: Pool): RunService {
  return new RunService(new DatabaseService(pool));
}

export function task(
  taskKey: string,
  dependenciesOrOptions: readonly string[] | Partial<TaskDefinition> = [],
  options: Partial<TaskDefinition> = {},
): TaskDefinition {
  // Accept either `task(key, deps)` or `task(key, { dependencies, ...opts })`.
  const merged: Partial<TaskDefinition> = Array.isArray(dependenciesOrOptions)
    ? { dependencies: dependenciesOrOptions, ...options }
    : { ...dependenciesOrOptions, ...options };

  return {
    taskKey,
    handlerType: 'echo',
    handlerVersion: '1',
    configuration: {},
    inputMapping: {},
    outputMapping: {},
    retryPolicy: { maxAttempts: 1 },
    timeoutMs: 10_000,
    failurePolicy: FailurePolicy.FAIL_WORKFLOW,
    dependencies: [],
    ...merged,
  };
}

export function definition(
  tasks: readonly TaskDefinition[],
  overrides: Partial<WorkflowDefinition> = {},
): WorkflowDefinition {
  return {
    name: 'test-workflow',
    tasks,
    ...overrides,
  };
}
