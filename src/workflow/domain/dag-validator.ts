import { FailurePolicy } from './failure-policy';
import {
  TaskDefinition,
  WorkflowDefinition,
} from './workflow-definition';
import { WorkflowValidationError } from './workflow-errors';

export class DagValidator {
  validate(definition: WorkflowDefinition): void {
    this.validateWorkflow(definition);

    const tasksByKey = new Map<string, TaskDefinition>();

    for (const task of definition.tasks) {
      if (tasksByKey.has(task.taskKey)) {
        throw new WorkflowValidationError(
          `Duplicate task key: ${task.taskKey}`,
        );
      }

      tasksByKey.set(task.taskKey, task);
      this.validateTask(task);
    }

    for (const task of definition.tasks) {
      for (const dependency of task.dependencies) {
        if (dependency === task.taskKey) {
          throw new WorkflowValidationError(
            `Task cannot depend on itself: ${task.taskKey}`,
          );
        }

        if (!tasksByKey.has(dependency)) {
          throw new WorkflowValidationError(
            `Unknown dependency '${dependency}' referenced by task '${task.taskKey}'`,
          );
        }
      }
    }

    this.detectCycles(definition.tasks);
  }

  private validateWorkflow(definition: WorkflowDefinition): void {
    if (!definition.name.trim()) {
      throw new WorkflowValidationError('Workflow name is required');
    }

    if (definition.tasks.length === 0) {
      throw new WorkflowValidationError(
        'Workflow must contain at least one task',
      );
    }
  }

  private validateTask(task: TaskDefinition): void {
    if (!task.taskKey.trim()) {
      throw new WorkflowValidationError('Task key is required');
    }

    if (!task.handlerType.trim()) {
      throw new WorkflowValidationError(
        `Handler type is required for task '${task.taskKey}'`,
      );
    }

    if (!task.handlerVersion.trim()) {
      throw new WorkflowValidationError(
        `Handler version is required for task '${task.taskKey}'`,
      );
    }

    if (
      !Number.isInteger(task.retryPolicy.maxAttempts) ||
      task.retryPolicy.maxAttempts < 1
    ) {
      throw new WorkflowValidationError(
        `Retry maxAttempts must be a positive integer for task '${task.taskKey}'`,
      );
    }

    if (
      task.retryPolicy.retryDelayMs !== undefined &&
      (!Number.isInteger(task.retryPolicy.retryDelayMs) ||
        task.retryPolicy.retryDelayMs < 0)
    ) {
      throw new WorkflowValidationError(
        `Retry retryDelayMs must be a non-negative integer for task '${task.taskKey}'`,
      );
    }

    if (!Number.isInteger(task.timeoutMs) || task.timeoutMs <= 0) {
      throw new WorkflowValidationError(
        `Timeout must be a positive integer for task '${task.taskKey}'`,
      );
    }

    if (
      task.failurePolicy !== FailurePolicy.FAIL_WORKFLOW &&
      task.failurePolicy !== FailurePolicy.SKIP_DOWNSTREAM
    ) {
      throw new WorkflowValidationError(
        `Failure policy must be FAIL_WORKFLOW or SKIP_DOWNSTREAM for task '${task.taskKey}' (got '${String(task.failurePolicy)}'); IGNORE_FAILURE is not a valid Phase 1 policy`,
      );
    }
  }

  private detectCycles(tasks: readonly TaskDefinition[]): void {
    const graph = new Map<string, readonly string[]>();

    for (const task of tasks) {
      graph.set(task.taskKey, task.dependencies);
    }

    const visiting = new Set<string>();
    const visited = new Set<string>();

    const visit = (taskKey: string): void => {
      if (visiting.has(taskKey)) {
        throw new WorkflowValidationError(
          `Dependency cycle detected involving task '${taskKey}'`,
        );
      }

      if (visited.has(taskKey)) {
        return;
      }

      visiting.add(taskKey);

      for (const dependency of graph.get(taskKey) ?? []) {
        visit(dependency);
      }

      visiting.delete(taskKey);
      visited.add(taskKey);
    };

    for (const task of tasks) {
      visit(task.taskKey);
    }
  }
}
