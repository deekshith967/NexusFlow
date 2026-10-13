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
import { OutboxEventType, routeOutboxEvent } from '../outbox/events';
import { OutboxEventRow } from '../outbox/outbox-repository';
import { MESSAGE_TRANSPORT } from '../transport/transport.module';
import { MessageTransport } from '../transport/types';

const CLAIM_SQL = `
  WITH claimable AS (
    SELECT outbox_event_id
    FROM outbox_events
    WHERE published_at IS NULL
      AND (claim_owner IS NULL OR claim_expires_at <= now())
    ORDER BY created_at ASC, outbox_event_id ASC
    LIMIT $1
    FOR UPDATE SKIP LOCKED
  )
  UPDATE outbox_events e
  SET claim_owner = $2,
      claim_expires_at = now() + make_interval(secs => $3::float8)
  FROM claimable c
  WHERE e.outbox_event_id = c.outbox_event_id
  RETURNING e.*
`;

/**
 * Outbox relay (contracts D.7, D.8, C.8).
 *
 * - Claims unpublished (or reclaimable) outbox rows with
 *   FOR UPDATE SKIP LOCKED and a claim TTL (claim transaction).
 * - Publishes to Kafka OUTSIDE the transaction (at-least-once).
 * - Marks published with a conditional update
 *   (WHERE published_at IS NULL AND claim_owner = self).
 * - On publication failure, increments publish_attempts, stores
 *   last_error, and releases the claim — but ONLY while this relay
 *   still owns the claim (WHERE claim_owner = self). A zero-row result
 *   means the claim expired and another relay took over; the row is
 *   left untouched for the new owner.
 *
 * Each broker publish is bounded by `publishTimeoutMs`; a wedged call
 * becomes a recorded, retryable failure instead of stalling the relay,
 * which is what makes the shutdown drain provably bounded (see
 * onApplicationShutdown / attemptPublish).
 *
 * If the relay crashes after publishing but before marking the row
 * published, the event is republished; consumers deduplicate via
 * event_id / database state.
 */
@Injectable()
export class OutboxRelayService
  implements OnModuleInit, OnApplicationShutdown
{
  private readonly logger = new Logger(OutboxRelayService.name);
  private timer: NodeJS.Timeout | null = null;
  private inFlight = false;
  private shutdownRequested = false;
  readonly claimOwner: string;

  /** Published events since startup (for health/diagnostics). */
  publishedCount = 0;
  failedCount = 0;
  lastPublishAt: Date | null = null;

  constructor(
    private readonly database: DatabaseService,
    @Inject(APP_CONFIG) private readonly config: EnvConfig,
    @Inject(MESSAGE_TRANSPORT) private readonly transport: MessageTransport,
  ) {
    this.claimOwner = `relay-${config.serviceId}-${process.pid}`;
  }

  private roleEnabled(): boolean {
    return this.config.role === 'outbox-relay' || this.config.role === 'all';
  }

  async onModuleInit(): Promise<void> {
    if (!this.roleEnabled()) {
      this.logger.log('Outbox relay role not enabled; skipping startup');
      return;
    }

    await this.transport.ensureTopics([
      this.config.kafka.dispatchTopic,
      this.config.kafka.lifecycleTopic,
    ]);

    // NOTE: intentionally NOT unref'd — in relay-role processes the poll
    // timer is what keeps the process alive; onApplicationShutdown clears
    // it during graceful shutdown.
    this.timer = setInterval(
      () => void this.pump().catch((error) => {
        this.logger.error(
          `Outbox relay pump failed: ${(error as Error).message}`,
        );
      }),
      this.config.relay.pollIntervalMs,
    );

    void this.pump().catch(() => undefined);

    this.logger.log(`Outbox relay ${this.claimOwner} started`);
  }

  /**
   * Graceful shutdown: stops future polls and FULLY drains the
   * in-flight publish cycle before returning.
   *
   * The drain is guaranteed to terminate (assuming a responsive
   * database): every transport publish inside the in-flight cycle is
   * bounded by `publishTimeoutMs` (see attemptPublish), and once
   * `shutdownRequested` is set the remaining rows of the batch fail
   * fast without initiating any transport operation. Every other step
   * of the cycle is a database query.
   *
   * Therefore, when this method returns:
   *  - the in-flight pump has completed ALL of its database and
   *    transport operations (none can outlive the drain), and
   *  - no further pump can start (`pump()` refuses while
   *    `shutdownRequested` is set).
   *
   * Documented boundary: the drain requires the database to remain
   * responsive (a hung database during shutdown stalls this hook — the
   * same cycle's claim transaction would be hung as well). A broker
   * request abandoned by the publish timeout is not cancelled inside
   * the transport client (kafkajs has no AbortSignal); the relay stops
   * waiting for it, the event stays durable in the outbox, and
   * at-least-once delivery + event_id deduplication make the possible
   * duplicate safe.
   */
  async onApplicationShutdown(): Promise<void> {
    this.shutdownRequested = true;

    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }

    // Drain the in-flight cycle (bounded: see above).
    while (this.inFlight) {
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
  }

  /** One claim + publish cycle. Exposed for tests. */
  async pump(): Promise<void> {
    if (this.shutdownRequested || this.inFlight) {
      return;
    }

    this.inFlight = true;

    try {
      const rows = await this.database.transaction<OutboxEventRow[]>(
        (client) =>
          client
            .query<OutboxEventRow>(CLAIM_SQL, [
              this.config.relay.batchLimit,
              this.claimOwner,
              this.config.relay.claimTtlMs / 1000,
            ])
            .then((result) => result.rows),
      );

      for (const row of rows) {
        await this.publishOne(row);
      }
    } finally {
      this.inFlight = false;
    }
  }

  private async publishOne(row: OutboxEventRow): Promise<void> {
    const { topic, key } = routeOutboxEvent(
      {
        dispatchTopic: this.config.kafka.dispatchTopic,
        lifecycleTopic: this.config.kafka.lifecycleTopic,
      },
      {
        eventType: row.event_type,
        workflowRunId: row.workflow_run_id,
        taskExecutionId: row.task_execution_id,
        aggregateId: row.aggregate_id,
      },
    );

    const envelope = {
      eventId: row.event_id,
      eventType: row.event_type,
      schemaVersion: row.schema_version,
      aggregateType: row.aggregate_type,
      aggregateId: row.aggregate_id,
      tenantId: row.tenant_id,
      workflowId: row.workflow_id,
      workflowVersionId: row.workflow_version_id,
      workflowRunId: row.workflow_run_id,
      taskExecutionId: row.task_execution_id,
      taskAttemptId: row.task_attempt_id,
      occurredAt: row.occurred_at,
      correlationId: row.correlation_id,
      causationId: row.causation_id,
      producer: row.producer,
      payload: row.payload,
    };

    const publish = await this.attemptPublish(
      topic,
      {
        key,
        value: JSON.stringify(envelope),
        headers: {
          'event-id': row.event_id,
          'event-type': row.event_type,
          'schema-version': row.schema_version,
        },
      },
    );

    if (publish.ok) {
      // D.8: conditional publish mark. A zero-row update means another
      // relay already marked it published (duplicate) — success either way.
      const marked = await this.database.query(
        `
        UPDATE outbox_events
        SET published_at = now(),
            publish_attempts = publish_attempts + 1,
            claim_owner = NULL,
            claim_expires_at = NULL
        WHERE outbox_event_id = $1
          AND published_at IS NULL
          AND claim_owner = $2
      `,
        [row.outbox_event_id, this.claimOwner],
      );

      if ((marked.rowCount ?? 0) > 0) {
        this.publishedCount += 1;
        this.lastPublishAt = new Date();
      } else {
        this.logger.debug(
          `Event ${row.event_id} was already published by another relay`,
        );
      }

      return;
    }

    // Publication failed (or was skipped because shutdown was requested):
    // record the failure and release the claim so the event stays durable
    // and retryable.
    const message = publish.error ?? 'unknown error';

    try {
      // Ownership guard: only THIS relay may record the failure or
      // release the claim. If the claim expired and another relay
      // already reclaimed the row, this update matches zero rows —
      // ownership was lost, and the new owner is responsible for the
      // event. We must not clear or overwrite the new owner's claim.
      const cleanup = await this.database.query(
        `
        UPDATE outbox_events
        SET publish_attempts = publish_attempts + 1,
            last_error = $3,
            claim_owner = NULL,
            claim_expires_at = NULL
        WHERE outbox_event_id = $1
          AND published_at IS NULL
          AND claim_owner = $2
      `,
        [row.outbox_event_id, this.claimOwner, message.slice(0, 2000)],
      );

      if ((cleanup.rowCount ?? 0) > 0) {
        this.failedCount += 1;
        this.logger.warn(
          `Publication failed for event ${row.event_id} (${row.event_type}): ${message}`,
        );
      } else {
        this.logger.warn(
          `Publication failed for event ${row.event_id} (${row.event_type}) but the claim is owned by another relay; leaving the row untouched for the new owner`,
        );
      }
    } catch (recordError) {
      this.logger.error(
        `Could not record publication failure for ${row.event_id}: ${(recordError as Error).message}`,
      );
    }
  }

  /**
   * Publish with a bounded wait (the only unbounded operation in a relay
   * cycle; every other step is a database query).
   *
   * - If shutdown has been requested, no new transport operation is
   *   initiated: the row fails fast so the in-flight cycle drains
   *   quickly and the event stays durable for a later relay.
   * - Otherwise the publish is raced against `publishTimeoutMs`. A
   *   wedged broker call therefore becomes a recorded, retryable
   *   failure instead of stalling the relay forever — which is what
   *   makes the shutdown drain provably bounded. At-least-once is
   *   preserved either way: the row is only marked published by the
   *   ownership-guarded conditional update, and consumers deduplicate
   *   on event_id.
   *
   * The underlying publish promise is always guarded, so a late
   * rejection after the timeout can never surface as an unhandled
   * rejection.
   */
  private async attemptPublish(
    topic: string,
    message: {
      key: string;
      value: string;
      headers?: Record<string, string>;
    },
  ): Promise<{ ok: boolean; error?: string }> {
    if (this.shutdownRequested) {
      return { ok: false, error: 'relay shutting down; publish skipped' };
    }

    const timeoutMs = this.config.relay.publishTimeoutMs;

    let raw: Promise<void>;
    try {
      raw = this.transport.publisher.publish(topic, message);
    } catch (error) {
      // A transport that throws synchronously (instead of rejecting) is
      // treated as a failed publish.
      return {
        ok: false,
        error: (error as Error).message ?? 'publish failed',
      };
    }

    let timer: NodeJS.Timeout | undefined;
    const timeout = new Promise<'timeout'>((resolve) => {
      timer = setTimeout(() => resolve('timeout'), timeoutMs);
    });

    const guarded = raw.then(
      () => 'published' as const,
      (error: unknown) => ({
        error: (error as Error).message ?? 'publish failed',
      }),
    );

    const result = await Promise.race([guarded, timeout]);

    if (timer) {
      clearTimeout(timer);
    }

    if (result === 'timeout') {
      return {
        ok: false,
        error: `publish did not complete within ${timeoutMs}ms`,
      };
    }

    if (result === 'published') {
      return { ok: true };
    }

    return { ok: false, error: result.error };
  }

  /** Is there durable unpublished work? */
  async hasBacklog(): Promise<boolean> {
    const result = await this.database.query<{ n: number }>(
      `SELECT count(*)::int AS n FROM outbox_events WHERE published_at IS NULL`,
    );

    return (result.rows[0]?.n ?? 0) > 0;
  }

  async isReady(): Promise<boolean> {
    const dbReady = await this.database
      .query<{ ok: number }>('SELECT 1 AS ok')
      .then(() => true)
      .catch(() => false);

    const transportReady = await this.transport.publisher
      .isReady()
      .catch(() => false);

    return dbReady && transportReady;
  }
}

export { OutboxEventType };
