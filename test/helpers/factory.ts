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
  options: Partial<TaskDefinition> = {},
): TaskDefinition {
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
    ...options,
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
