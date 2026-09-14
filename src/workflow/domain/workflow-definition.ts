import { FailurePolicy } from './failure-policy';

export interface RetryPolicy {
  readonly maxAttempts: number;
}

export interface TaskDefinition {
  readonly taskKey: string;
  readonly handlerType: string;
  readonly handlerVersion: string;
  readonly configuration: Readonly<Record<string, unknown>>;
  readonly inputMapping: Readonly<Record<string, unknown>>;
  readonly outputMapping: Readonly<Record<string, unknown>>;
  readonly retryPolicy: RetryPolicy;
  readonly timeoutMs: number;
  readonly failurePolicy: FailurePolicy;
  readonly dependencies: readonly string[];
}

export interface WorkflowDefinition {
  readonly name: string;
  readonly description?: string;
  readonly tasks: readonly TaskDefinition[];
}
