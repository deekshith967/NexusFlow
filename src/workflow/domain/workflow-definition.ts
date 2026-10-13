import { FailurePolicy } from './failure-policy';

export interface RetryPolicy {
  /** Maximum number of attempts, including the first one (>= 1). */
  readonly maxAttempts: number;
  /**
   * Delay before the next retry (ms). Optional; when omitted the scheduler
   * may retry immediately (default 0). This is an implementation detail
   * permitted by the contract's `retry_policy JSONB` column.
   */
  readonly retryDelayMs?: number;
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
