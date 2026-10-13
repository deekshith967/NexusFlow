/**
 * Outbox event types required by the architecture contract (section D).
 * Every database state transition that requires an event inserts its row
 * into outbox_events in the same transaction (invariant 15).
 */
export const OutboxEventType = {
  WorkflowVersionPublished: 'workflow.version.published',
  WorkflowVersionRetired: 'workflow.version.retired',
  WorkflowRunStarted: 'workflow.run.started',
  WorkflowRunSucceeded: 'workflow.run.succeeded',
  WorkflowRunFailed: 'workflow.run.failed',
  TaskReady: 'task.ready',
  TaskClaimed: 'task.claimed',
  TaskDispatchRequested: 'task.dispatch.requested',
  TaskStarted: 'task.started',
  TaskSucceeded: 'task.succeeded',
  TaskFailed: 'task.failed',
  TaskRetryScheduled: 'task.retry_scheduled',
  TaskBlocked: 'task.blocked',
} as const;

export type OutboxEventTypeName =
  (typeof OutboxEventType)[keyof typeof OutboxEventType];

export const OUTBOX_SCHEMA_VERSION = '1.0.0';

export interface OutboxEventInput {
  readonly eventType: OutboxEventTypeName;
  readonly aggregateType: string;
  readonly aggregateId: string;
  readonly tenantId?: string;
  readonly workflowId?: string;
  readonly workflowVersionId?: string;
  readonly workflowRunId?: string;
  readonly taskExecutionId?: string;
  readonly taskAttemptId?: string;
  readonly correlationId: string;
  readonly causationId?: string;
  readonly producer: string;
  readonly payload: Record<string, unknown>;
  readonly headers?: Record<string, string>;
}

/**
 * Kafka routing for an outbox event (contract C.8):
 * - task dispatch events use `task_execution_id` as the key
 * - run lifecycle events use `workflow_run_id` as the key
 * - other lifecycle events use `aggregate_id` as the key
 */
export function routeOutboxEvent(
  topics: { dispatchTopic: string; lifecycleTopic: string },
  event: {
    eventType: string;
    workflowRunId?: string | null;
    taskExecutionId?: string | null;
    aggregateId: string;
  },
): { topic: string; key: string } {
  if (event.eventType === OutboxEventType.TaskDispatchRequested) {
    if (!event.taskExecutionId) {
      throw new Error('task.dispatch.requested requires task_execution_id');
    }

    return { topic: topics.dispatchTopic, key: event.taskExecutionId };
  }

  if (event.workflowRunId) {
    return { topic: topics.lifecycleTopic, key: event.workflowRunId };
  }

  return { topic: topics.lifecycleTopic, key: event.aggregateId };
}
