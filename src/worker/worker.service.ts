import {
  Inject,
  Injectable,
  Logger,
  OnApplicationShutdown,
  OnModuleInit,
} from '@nestjs/common';

import { APP_CONFIG } from '../config/config.module';
import { EnvConfig } from '../config/env';
import { DatabaseService } from '../database/database.service';
import { MESSAGE_TRANSPORT } from '../transport/transport.module';
import { MessageTransport } from '../transport/types';
import { createBuiltinRegistry } from './handlers/builtins';
import { HandlerRegistry } from './handlers/registry';
import { HandlerContext, HandlerResult } from './handlers/types';
import {
  completeFailure,
  completeSuccess,
  renewLease,
  startAttempt,
} from './worker-ops';

interface DispatchPayload {
  dispatchId: string;
  taskExecutionId: string;
  taskAttemptId: string;
  workflowRunId?: string;
  taskKey?: string;
  attemptNumber: number;
  fencingToken: number;
  leaseOwner?: string;
  timeoutMs?: number;
}

interface TaskDefinitionForExecution {
  task_definition_id: string;
  handler_type: string;
  handler_version: string;
  configuration: Record<string, unknown>;
  timeout_ms: number;
  failure_policy: string;
  retry_policy: { maxAttempts: number; retryDelayMs?: number };
  resolved_input: unknown;
}

/**
 * Worker process (contract: workers consume dispatch messages, validate
 * eligibility, execute deterministic handlers, renew leases, and report
 * outcomes conditionally).
 *
 * Ownership is NOT the worker's: the scheduler assigns leases and fencing
 * tokens. The worker only verifies them; a stale worker (old fencing
 * token or expired lease) can never commit authoritative state.
 */
@Injectable()
export class WorkerService implements OnModuleInit, OnApplicationShutdown {
  private readonly logger = new Logger(WorkerService.name);
  private readonly registry: HandlerRegistry;
  private readonly workerId: string;
  private running = false;
  private shutdownRequested = false;

  constructor(
    private readonly database: DatabaseService,
    @Inject(APP_CONFIG) private readonly config: EnvConfig,
    @Inject(MESSAGE_TRANSPORT) private readonly transport: MessageTransport,
  ) {
    this.registry = createBuiltinRegistry();
    this.workerId = `worker-${config.serviceId}-${process.pid}`;
  }

  async onModuleInit(): Promise<void> {
    if (!this.workerRoleEnabled()) {
      this.logger.log('Worker role not enabled; skipping consumer startup');
      return;
    }

    this.running = true;

    await this.transport.consumer.subscribe({
      group: this.config.worker.consumerGroup,
      topic: this.config.kafka.dispatchTopic,
      handler: (message) => this.handleDispatch(message),
    });

    this.logger.log(
      `Worker ${this.workerId} consuming ${this.config.kafka.dispatchTopic} (group ${this.config.worker.consumerGroup})`,
    );
  }

  private workerRoleEnabled(): boolean {
    return (
      this.config.role === 'worker' || this.config.role === 'all'
    );
  }

  onApplicationShutdown(): void {
    // Stop accepting new work; in-flight executions finish (or are
    // recovered via lease expiry if the process is killed).
    this.running = false;
    this.shutdownRequested = true;
    this.logger.log(`Worker ${this.workerId} shutting down`);
  }

  private async handleDispatch(message: {
    key: string;
    value: string;
  }): Promise<void> {
    if (this.shutdownRequested) {
      return;
    }

    let payload: DispatchPayload;

    try {
      const envelope = JSON.parse(message.value) as {
        eventType?: string;
        payload?: DispatchPayload;
      };

      // The relay publishes the event envelope; the dispatch payload is
      // nested under `payload`.
      payload = envelope.payload ?? (envelope as DispatchPayload);
    } catch {
      this.logger.error(
        `Dropping malformed dispatch message (key=${message.key})`,
      );
      return;
    }

    if (!payload.dispatchId || !payload.taskExecutionId || !payload.taskAttemptId) {
      this.logger.error(
        `Dropping dispatch message missing identifiers (key=${message.key})`,
      );
      return;
    }

    try {
      await this.executeDispatch(payload);
    } catch (error) {
      // Infrastructure failure (e.g. database outage): let the message
      // be redelivered (at-least-once). Duplicate dispatches are safe:
      // startAttempt validates state, fencing token, and lease.
      this.logger.error(
        `Dispatch processing failed for ${payload.taskExecutionId}: ${(error as Error).message}`,
      );
      throw error;
    }
  }

  private async executeDispatch(payload: DispatchPayload): Promise<void> {
    // D.9: validate and transition to RUNNING. Zero rows => stale.
    const start = await startAttempt(this.database, {
      dispatchId: payload.dispatchId,
      taskExecutionId: payload.taskExecutionId,
      fencingToken: payload.fencingToken,
      workerId: this.workerId,
    });

    if (!start.ok) {
      this.logger.log(
        `Dispatch for ${payload.taskExecutionId} is stale (${start.reason}); not executing`,
      );
      return;
    }

    const definition = await this.loadDefinition(payload.taskExecutionId);

    if (!definition) {
      // Should not happen; report a permanent failure.
      await this.reportFailure(payload, definition, {
        code: 'INTERNAL',
        message: 'Task definition not found during execution',
        retryable: false,
      });
      return;
    }

    // Renew the lease while the handler runs so long tasks are not
    // reclaimed. A failed renewal means we are stale: stop renewing and
    // let the report be rejected by the database.
    const leaseMs = this.config.scheduler.leaseMs;
    const renewIntervalMs = Math.max(100, Math.floor(leaseMs / 3));
    let stale = false;
    const renewTimer = setInterval(() => {
      void renewLease(this.database, {
        taskExecutionId: payload.taskExecutionId,
        fencingToken: payload.fencingToken,
        leaseMs,
      }).then((ok) => {
        if (!ok && !stale) {
          stale = true;
          this.logger.error(
            `Lease renewal failed for ${payload.taskExecutionId}; treating worker as stale`,
          );
        }
      });
    }, renewIntervalMs);
    renewTimer.unref?.();

    let result: HandlerResult;

    try {
      result = await this.runHandler(definition, payload);
    } finally {
      clearInterval(renewTimer);
    }

    if (result.status === 'succeeded') {
      await completeSuccess(this.database, {
        taskExecutionId: payload.taskExecutionId,
        attemptId: payload.taskAttemptId,
        fencingToken: payload.fencingToken,
        workerId: this.workerId,
        output: result.output,
      });
      this.logger.log(
        `Task ${payload.taskKey ?? payload.taskExecutionId} succeeded (attempt ${payload.attemptNumber})`,
      );
    } else {
      await this.reportFailure(payload, definition, {
        code: result.failureCode,
        message: result.failureMessage,
        retryable: result.retryable,
      });
    }
  }

  private async loadDefinition(
    taskExecutionId: string,
  ): Promise<TaskDefinitionForExecution | null> {
    const result = await this.database.query<TaskDefinitionForExecution>(
      `
      SELECT td.task_definition_id,
             td.handler_type,
             td.handler_version,
             td.configuration,
             td.timeout_ms,
             td.failure_policy,
             td.retry_policy,
             te.resolved_input
      FROM task_executions te
      JOIN task_definitions td
        ON td.task_definition_id = te.task_definition_id
      WHERE te.task_execution_id = $1
    `,
      [taskExecutionId],
    );

    return result.rows[0] ?? null;
  }

  private async runHandler(
    definition: TaskDefinitionForExecution,
    payload: DispatchPayload,
  ): Promise<HandlerResult> {
    const handler = this.registry.resolve(
      definition.handler_type,
      definition.handler_version,
    );

    if (!handler) {
      return {
        status: 'failed',
        failureCode: 'UNKNOWN_HANDLER',
        failureMessage: `No handler registered for ${definition.handler_type}:${definition.handler_version}`,
        retryable: false,
      };
    }

    const ctx: HandlerContext = {
      input: definition.resolved_input ?? null,
      configuration: definition.configuration ?? {},
      attemptNumber: payload.attemptNumber,
    };

    const timeoutMs = definition.timeout_ms;
    const startedAt = Date.now();

    try {
      const result = await Promise.race([
        handler(ctx),
        new Promise<HandlerResult>((resolve) => {
          const timer = setTimeout(() => {
            resolve({
              status: 'failed',
              failureCode: 'TIMEOUT',
              failureMessage: `Handler exceeded timeout of ${timeoutMs}ms`,
              retryable: true,
            });
          }, timeoutMs);
          timer.unref?.();
        }),
      ]);

      const elapsed = Date.now() - startedAt;
      if (elapsed >= timeoutMs && result.status === 'succeeded') {
        // The handler finished after the timeout fired: treat as timeout.
        return {
          status: 'failed',
          failureCode: 'TIMEOUT',
          failureMessage: `Handler exceeded timeout of ${timeoutMs}ms`,
          retryable: true,
        };
      }

      return result;
    } catch (error) {
      return {
        status: 'failed',
        failureCode: 'HANDLER_ERROR',
        failureMessage: (error as Error).message,
        // Unexpected handler exceptions are not classified retryable:
        // built-in handlers report their own retryability.
        retryable: false,
      };
    }
  }

  private async reportFailure(
    payload: DispatchPayload,
    definition: TaskDefinitionForExecution | null,
    failure: { code: string; message: string; retryable: boolean; timedOut?: boolean },
  ): Promise<void> {
    if (!definition) {
      return;
    }

    const result = await completeFailure(this.database, {
      taskExecutionId: payload.taskExecutionId,
      attemptId: payload.taskAttemptId,
      fencingToken: payload.fencingToken,
      workerId: this.workerId,
      failure: { ...failure },
      attemptNumber: payload.attemptNumber,
      maxAttempts: definition.retry_policy?.maxAttempts ?? 1,
      retryDelayMs: definition.retry_policy?.retryDelayMs ?? 0,
      failurePolicy:
        definition.failure_policy === 'SKIP_DOWNSTREAM'
          ? 'SKIP_DOWNSTREAM'
          : 'FAIL_WORKFLOW',
    });

    if (result.applied) {
      this.logger.log(
        `Task ${payload.taskKey ?? payload.taskExecutionId} failure applied: task=${result.taskState} run=${result.runState} (attempt ${payload.attemptNumber}, ${failure.code})`,
      );
    } else {
      this.logger.log(
        `Failure report for ${payload.taskExecutionId} not applied (stale or duplicate); run=${result.runState}`,
      );
    }
  }
}
