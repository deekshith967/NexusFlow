import { createHash } from 'node:crypto';

import type { WorkflowDefinition } from './workflow-definition';

/**
 * Computes the canonical SHA-256 hash of a workflow definition
 * (contract D.3 step 6). The hash is stable: tasks are sorted by
 * task_key and each task's dependency list is sorted alphabetically, so
 * equivalent definitions always produce the same hash regardless of
 * insertion order.
 */
export function computeDefinitionHash(
  definition: WorkflowDefinition,
): string {
  const canonical = {
    name: definition.name,
    ...(definition.description !== undefined
      ? { description: definition.description }
      : {}),
    tasks: [...definition.tasks]
      .sort((a, b) => a.taskKey.localeCompare(b.taskKey))
      .map((task) => ({
        taskKey: task.taskKey,
        handlerType: task.handlerType,
        handlerVersion: task.handlerVersion,
        configuration: task.configuration,
        inputMapping: task.inputMapping,
        outputMapping: task.outputMapping,
        retryPolicy: task.retryPolicy,
        timeoutMs: task.timeoutMs,
        failurePolicy: task.failurePolicy,
        dependencies: [...task.dependencies].sort(),
      })),
  };

  return createHash('sha256')
    .update(JSON.stringify(canonical))
    .digest('hex');
}
