/**
 * Outbox relay integration tests (real PostgreSQL, in-memory transport
 * as a stand-in for the Kafka broker — no broker exists in this
 * environment).
 *
 * Covers contract D.7/D.8 behavior and required failure-injection:
 * relay crash after Kafka publication, Kafka outage with durable
 * unpublished outbox rows, and duplicate publication tolerated via
 * event_id.
 */
import type { Pool } from 'pg';

import { OutboxRelayService } from '../../src/outbox-relay/outbox-relay.service';
import { SchedulerService } from '../../src/scheduler/scheduler.service';
import { baseConfig, createEngine, Engine, setupRun, task } from '../helpers/engine';
import { startTestDb, TestDb } from '../helpers/test-db';
import { DatabaseService } from '../../src/database/database.service';
import { MessageTransport } from '../../src/transport/types';

/**
 * A transport whose publish() blocks on a gate the test controls,
 * then either succeeds or throws. Lets tests hold a relay mid-publish.
 */
function gatedTransport(failAfterRelease: boolean, errorMessage: string) {
  let release: () => void;

  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });

  const transport: MessageTransport = {
    kind: 'memory',
    ensureTopics: async () => undefined,
    publisher: {
      publish: async () => {
        await gate;
        if (failAfterRelease) {
          throw new Error(errorMessage);
        }
      },
      isReady: async () => true,
      close: async () => undefined,
    },
    consumer: {
      subscribe: async () => undefined,
      close: async () => undefined,
    },
    close: async () => undefined,
  };

  return { transport, release: () => release() };
}

async function waitForRowState(
  pool: Pool,
  sql: string,
  values: unknown[],
  test: (row: Record<string, unknown>) => boolean,
  what: string,
  timeoutMs = 10_000,
): Promise<Record<string, unknown>> {
  const deadline = Date.now() + timeoutMs;

  for (;;) {
    const result = await pool.query<Record<string, unknown>>(sql, values);
    const row = result.rows[0];

    if (row && test(row)) {
      return row;
    }

    if (Date.now() > deadline) {
      throw new Error(`timed out waiting for ${what}; row=${JSON.stringify(result.rows)}`);
    }

    await new Promise((r) => setTimeout(r, 25));
  }
}

describe('outbox relay (real PostgreSQL + in-memory transport)', () => {
  let db: TestDb;
  let pool: Pool;
  let engine: Engine;
  const tenantId = '11111111-1111-4111-8111-111111111111';

  beforeAll(async () => {
    db = await startTestDb();
    pool = db.pool;
    engine = await createEngine(pool, baseConfig());
  }, 120_000);

  afterAll(async () => {
    await engine.stop();
    await db.stop();
  });

  async function unpublishedEvents() {
    const result = await pool.query<{ outbox_event_id: string; event_type: string }>(
      `
      SELECT outbox_event_id, event_type
      FROM outbox_events
      WHERE published_at IS NULL
    `,
    );
    return result.rows;
  }

  it('claims, publishes, and marks published; routes events per contract C.8', async () => {
    const { workflowVersionId, workflowRunId } = await setupRun(
      engine,
      [task('route')],
      { name: 'relay-routing' },
    );

    // Claim the task (creates task.claimed + task.dispatch.requested).
    const scheduler = new SchedulerService(
      new DatabaseService(pool),
      baseConfig(),
    );
    expect(await scheduler.scan()).toBe(1);

    // Pump until the outbox drains. The worker executes the task in
    // response to the dispatch, which inserts new outbox events
    // (task.started / task.succeeded / workflow.run.succeeded).
    for (let i = 0; i < 10; i += 1) {
      await engine.relay.pump();
      await engine.transport.drain();
      if ((await unpublishedEvents()).length === 0) {
        break;
      }
    }

    expect(await unpublishedEvents()).toEqual([]);

    const published = (
      await pool.query<{ n: number; publish_attempts: number; claim_owner: string | null }>(
        `
        SELECT count(*)::int AS n, min(publish_attempts) AS publish_attempts,
               max(claim_owner) AS claim_owner
        FROM outbox_events
        WHERE workflow_run_id = $1
      `,
        [workflowRunId],
      )
    ).rows[0];
    expect(published.n).toBeGreaterThan(0);
    expect(published.publish_attempts).toBe(1);
    expect(published.claim_owner).toBeNull();

    // Routing: dispatch event on the dispatch topic keyed by
    // task_execution_id; run events on the lifecycle topic keyed by
    // workflow_run_id.
    const dispatchMessages = engine.transport.published.filter(
      (m) => m.topic === engine.config.kafka.dispatchTopic,
    );
    const lifecycleMessages = engine.transport.published.filter(
      (m) => m.topic === engine.config.kafka.lifecycleTopic,
    );

    expect(dispatchMessages.length).toBeGreaterThan(0);
    expect(lifecycleMessages.length).toBeGreaterThan(0);

    const taskExecutionId = (
      await pool.query<{ task_execution_id: string }>(
        `SELECT task_execution_id FROM task_executions WHERE workflow_run_id = $1`,
        [workflowRunId],
      )
    ).rows[0];

    const dispatch = dispatchMessages.find(
      (m) => m.key === taskExecutionId.task_execution_id,
    );
    expect(dispatch).toBeDefined();
    const dispatchEnvelope = JSON.parse(dispatch!.value) as {
      eventType: string;
      eventId: string;
      payload: Record<string, unknown>;
    };
    expect(dispatchEnvelope.eventType).toBe('task.dispatch.requested');
    expect(dispatchEnvelope.payload.taskExecutionId).toBe(
      taskExecutionId.task_execution_id,
    );

    // The version-published event (no run) is keyed by aggregate_id.
    const versionEvent = lifecycleMessages.find((m) => m.key === workflowVersionId);
    expect(versionEvent).toBeDefined();

    // Envelope mandatory fields present.
    const envelope = JSON.parse(versionEvent!.value) as Record<string, unknown>;
    for (const field of [
      'eventId',
      'eventType',
      'schemaVersion',
      'aggregateType',
      'aggregateId',
      'occurredAt',
      'correlationId',
      'producer',
      'payload',
    ]) {
      expect(envelope[field]).toBeDefined();
    }
  });

  it('Kafka outage: events stay durable and unpublished, then publish after recovery', async () => {
    const workflow = await engine.workflows.createWorkflow({
      tenantId,
      name: `outage-${Date.now()}`,
    });
    const version = await engine.workflows.createDraftVersion({
      tenantId,
      workflowId: workflow.workflow_id,
      definition: { name: 'outage-wf', tasks: [task('x')] },
    });

    // Fault injection: the transport refuses publication.
    engine.transport.outage = true;

    await engine.workflows.publish(version.workflow_version_id);

    const before = await unpublishedEvents();
    expect(before.some((e) => e.event_type === 'workflow.version.published')).toBe(true);

    // Pump while down: publication fails, row stays durable.
    await engine.relay.pump();

    const row = (
      await pool.query<{
        published_at: string | null;
        publish_attempts: number;
        last_error: string | null;
        claim_owner: string | null;
        claim_expires_at: string | null;
      }>(
        `
        SELECT published_at, publish_attempts, last_error,
               claim_owner, claim_expires_at
        FROM outbox_events
        WHERE event_type = 'workflow.version.published'
          AND workflow_version_id = $1
      `,
        [version.workflow_version_id],
      )
    ).rows[0];

    expect(row.published_at).toBeNull();
    expect(row.publish_attempts).toBe(1);
    expect(row.last_error).toBeTruthy();
    // Claim released so another relay can retry.
    expect(row.claim_owner).toBeNull();
    expect(row.claim_expires_at).toBeNull();

    // Recover: publication succeeds on the next pump.
    engine.transport.outage = false;
    await engine.relay.pump();

    const after = (
      await pool.query<{ n: number }>(
        `SELECT count(*)::int AS n FROM outbox_events
         WHERE workflow_version_id = $1 AND published_at IS NULL`,
        [version.workflow_version_id],
      )
    ).rows[0];
    expect(after.n).toBe(0);
  });

  it('relay crash after publication: event is republished and deduplicated by event_id', async () => {
    const workflow = await engine.workflows.createWorkflow({
      tenantId,
      name: `crash-${Date.now()}`,
    });
    const version = await engine.workflows.createDraftVersion({
      tenantId,
      workflowId: workflow.workflow_id,
      definition: { name: 'crash-wf', tasks: [task('y')] },
    });
    await engine.workflows.publish(version.workflow_version_id);

    const eventRow = (
      await pool.query<{
        outbox_event_id: string;
        event_id: string;
      }>(
        `
        SELECT outbox_event_id, event_id
        FROM outbox_events
        WHERE workflow_version_id = $1 AND event_type = 'workflow.version.published'
      `,
        [version.workflow_version_id],
      )
    ).rows[0];

    // Simulate relay A: claim the row...
    await pool.query(
      `
      UPDATE outbox_events
      SET claim_owner = 'crashed-relay',
          claim_expires_at = now() + interval '30 seconds'
      WHERE outbox_event_id = $1
    `,
      [eventRow.outbox_event_id],
    );

    // ...publish to the broker...
    const envelopeValue = JSON.stringify({
      eventId: eventRow.event_id,
      eventType: 'workflow.version.published',
      schemaVersion: '1.0.0',
      aggregateType: 'workflow_version',
      aggregateId: version.workflow_version_id,
      correlationId: version.workflow_version_id,
      producer: 'crashed-relay',
      payload: { workflowVersionId: version.workflow_version_id },
    });
    await engine.transport.publisher.publish(
      engine.config.kafka.lifecycleTopic,
      { key: version.workflow_version_id, value: envelopeValue },
    );

    // ...and crash before marking published. The claim expires.
    await pool.query(
      `
      UPDATE outbox_events
      SET claim_expires_at = now() - interval '1 second'
      WHERE outbox_event_id = $1
    `,
      [eventRow.outbox_event_id],
    );

    // A second relay reclaims and republishes.
    await engine.relay.pump();
    await engine.transport.drain();

    const published = (
      await pool.query<{ n: number; publish_attempts: number }>(
        `SELECT count(*)::int AS n, max(publish_attempts) AS publish_attempts
         FROM outbox_events WHERE outbox_event_id = $1`,
        [eventRow.outbox_event_id],
      )
    ).rows[0];
    expect(published.n).toBe(1);
    expect(published.publish_attempts).toBe(1);

    // The broker received the event twice with the SAME event_id —
    // consumers deduplicate on event_id.
    const deliveries = engine.transport.published.filter((m) => {
      if (m.topic !== engine.config.kafka.lifecycleTopic) {
        return false;
      }

      const envelope = JSON.parse(m.value) as { eventId?: string };
      return envelope.eventId === eventRow.event_id;
    });
    expect(deliveries.length).toBe(2);
  });

  it('concurrent relays do not double-publish the same batch (claim exclusivity)', async () => {
    const workflow = await engine.workflows.createWorkflow({
      tenantId,
      name: `conc-relay-${Date.now()}`,
    });
    const version = await engine.workflows.createDraftVersion({
      tenantId,
      workflowId: workflow.workflow_id,
      definition: { name: 'conc-wf', tasks: [task('z')] },
    });
    await engine.workflows.publish(version.workflow_version_id);

    const [relayA, relayB] = [
      new OutboxRelayService(new DatabaseService(pool), baseConfig(), engine.transport),
      new OutboxRelayService(new DatabaseService(pool), baseConfig(), engine.transport),
    ];

    // Both relays pump in parallel; the claim transaction guarantees
    // each event is handled by exactly one relay per cycle.
    await Promise.all([relayA.pump(), relayB.pump()]);
    await engine.transport.drain();

    const row = (
      await pool.query<{ publish_attempts: number; published_at: string | null }>(
        `
        SELECT publish_attempts, published_at
        FROM outbox_events
        WHERE workflow_version_id = $1 AND event_type = 'workflow.version.published'
      `,
        [version.workflow_version_id],
      )
    ).rows[0];

    expect(row.published_at).not.toBeNull();
    // Exactly one relay published it in this cycle.
    expect(row.publish_attempts).toBe(1);

    const eventId = (
      await pool.query<{ event_id: string }>(
        `SELECT event_id FROM outbox_events
         WHERE workflow_version_id = $1 AND event_type = 'workflow.version.published'`,
        [version.workflow_version_id],
      )
    ).rows[0];
    const deliveries = engine.transport.published.filter((m) => {
      const envelope = JSON.parse(m.value) as { eventId?: string };
      return envelope.eventId === eventId.event_id;
    });
    expect(deliveries.length).toBe(1);
  });

  it('a relay that lost claim ownership must not clear the new owners claim', async () => {
    // Insert one unpublished event.
    const row = (
      await pool.query<{ outbox_event_id: string }>(
        `
        INSERT INTO outbox_events (
          outbox_event_id, event_id, event_type, schema_version,
          aggregate_type, aggregate_id, correlation_id, producer,
          payload, occurred_at, created_at
        )
        VALUES (gen_random_uuid(), gen_random_uuid(), 'test.event', '1.0.0',
                'test', gen_random_uuid(), 'corr', 'test', '{}', now(), now())
        RETURNING outbox_event_id
      `
      )
    ).rows[0];

    const sql = 'SELECT claim_owner, claim_expires_at, publish_attempts, last_error, published_at FROM outbox_events WHERE outbox_event_id = $1';

    // Relay A claims and starts publishing (blocks on its gate).
    const a = gatedTransport(true, 'simulated outage A');
    const relayA = new OutboxRelayService(
      new DatabaseService(pool),
      { ...baseConfig(), serviceId: 'relay-ownership-A' },
      a.transport,
    );
    const pumpA = relayA.pump();

    await waitForRowState(
      pool, sql, [row.outbox_event_id],
      (r) => r.claim_owner === relayA.claimOwner,
      'relay A to claim the event',
    );

    // A's claim expires, and relay B reclaims while A is still publishing.
    await pool.query(
      'UPDATE outbox_events SET claim_expires_at = now() - interval \'1 second\' WHERE outbox_event_id = $1',
      [row.outbox_event_id],
    );

    const b = gatedTransport(false, 'unused');
    const relayB = new OutboxRelayService(
      new DatabaseService(pool),
      { ...baseConfig(), serviceId: 'relay-ownership-B' },
      b.transport,
    );
    const pumpB = relayB.pump();

    await waitForRowState(
      pool, sql, [row.outbox_event_id],
      (r) => r.claim_owner === relayB.claimOwner,
      'relay B to reclaim the event',
    );

    // A's publish now fails. Its failure cleanup must detect the lost
    // ownership and leave the row untouched for B.
    a.release();
    await pumpA;

    const afterA = (
      await pool.query<{
        claim_owner: string | null;
        claim_expires_at: string | null;
        publish_attempts: number;
        last_error: string | null;
        published_at: string | null;
      }>(sql, [row.outbox_event_id])
    ).rows[0];

    expect(afterA.claim_owner).toBe(relayB.claimOwner);
    expect(afterA.claim_expires_at).not.toBeNull();
    expect(afterA.publish_attempts).toBe(0);
    expect(afterA.last_error).toBeNull();
    expect(afterA.published_at).toBeNull();

    // B finishes: publishes successfully and marks the row published
    // under its own ownership (at-least-once, exactly one mark).
    b.release();
    await pumpB;

    const afterB = (
      await pool.query<{ claim_owner: string | null; publish_attempts: number; published_at: string | null }>(sql, [row.outbox_event_id])
    ).rows[0];
    expect(afterB.published_at).not.toBeNull();
    expect(afterB.claim_owner).toBeNull();
    expect(afterB.publish_attempts).toBe(1);
  });

  it('shutdown waits for an in-flight publish cycle and stops future polls', async () => {
    const inserted = (
      await pool.query<{ outbox_event_id: string }>(
        `
        INSERT INTO outbox_events (
          outbox_event_id, event_id, event_type, schema_version,
          aggregate_type, aggregate_id, correlation_id, producer,
          payload, occurred_at, created_at
        )
        VALUES (gen_random_uuid(), gen_random_uuid(), 'test.event', '1.0.0',
                'test', gen_random_uuid(), 'corr', 'test', '{}', now(), now())
        RETURNING outbox_event_id
      `
      )
    ).rows[0];

    const sql = 'SELECT claim_owner, published_at FROM outbox_events WHERE outbox_event_id = $1';

    const g = gatedTransport(false, 'unused');
    const relay = new OutboxRelayService(
      new DatabaseService(pool),
      { ...baseConfig(), serviceId: 'relay-shutdown' },
      g.transport,
    );

    // Start a pump; its publish blocks on the gate (in flight).
    const pumpP = relay.pump();

    await waitForRowState(
      pool, sql, [inserted.outbox_event_id],
      (r) => r.claim_owner === relay.claimOwner,
      'relay to claim the event',
    );

    // Shutdown must not return while the in-flight cycle still needs
    // the pool/transport.
    const shutdownP = relay.onApplicationShutdown();
    let shutdownReturned = false;

    shutdownP.then(() => {
      shutdownReturned = true;
    });

    // The publish is still blocked: shutdown cannot have returned.
    await new Promise((r) => setTimeout(r, 100));
    expect(shutdownReturned).toBe(false);

    // Release the publish; the cycle finishes, then shutdown returns.
    g.release();
    await pumpP;
    await shutdownP;

    const row = (
      await pool.query<{ published_at: string | null }>(sql, [inserted.outbox_event_id])
    ).rows[0];
    expect(row.published_at).not.toBeNull();
    expect(relay.publishedCount).toBe(1);

    // Future polls are suppressed after shutdown.
    const second = (
      await pool.query<{ outbox_event_id: string }>(
        `
        INSERT INTO outbox_events (
          outbox_event_id, event_id, event_type, schema_version,
          aggregate_type, aggregate_id, correlation_id, producer,
          payload, occurred_at, created_at
        )
        VALUES (gen_random_uuid(), gen_random_uuid(), 'test.event', '1.0.0',
                'test', gen_random_uuid(), 'corr', 'test', '{}', now(), now())
        RETURNING outbox_event_id
      `
      )
    ).rows[0];

    await relay.pump();

    const untouched = (
      await pool.query<{ claim_owner: string | null; published_at: string | null }>(
        sql,
        [second.outbox_event_id],
      )
    ).rows[0];
    expect(untouched.claim_owner).toBeNull();
    expect(untouched.published_at).toBeNull();

    // Keep the outbox clean for the remaining tests.
    await pool.query('DELETE FROM outbox_events WHERE outbox_event_id = $1', [
      second.outbox_event_id,
    ]);
  });

  it('shutdown drains a stuck publish within the publish timeout and keeps the event durable', async () => {
    // A transport whose publish NEVER resolves (wedged broker).
    let publishCalls = 0;
    const stuckTransport: MessageTransport = {
      kind: 'memory',
      ensureTopics: async () => undefined,
      publisher: {
        publish: async () => {
          publishCalls += 1;
          await new Promise<void>(() => {
            /* never resolves */
          });
        },
        isReady: async () => true,
        close: async () => undefined,
      },
      consumer: {
        subscribe: async () => undefined,
        close: async () => undefined,
      },
      close: async () => undefined,
    };

    const relay = new OutboxRelayService(
      new DatabaseService(pool),
      {
        ...baseConfig(),
        serviceId: 'relay-drain',
        relay: { ...baseConfig().relay, publishTimeoutMs: 200 },
      },
      stuckTransport,
    );

    const row = (
      await pool.query<{ outbox_event_id: string }>(
        `
        INSERT INTO outbox_events (
          outbox_event_id, event_id, event_type, schema_version,
          aggregate_type, aggregate_id, correlation_id, producer,
          payload, occurred_at, created_at
        )
        VALUES (gen_random_uuid(), gen_random_uuid(), 'test.event', '1.0.0',
                'test', gen_random_uuid(), 'corr', 'test', '{}', now(), now())
        RETURNING outbox_event_id
      `
      )
    ).rows[0];

    const sql =
      'SELECT claim_owner, published_at, publish_attempts, last_error FROM outbox_events WHERE outbox_event_id = $1';

    // Start a pump; its publish is now stuck inside the transport.
    const pumpP = relay.pump();

    await waitForRowState(
      pool, sql, [row.outbox_event_id],
      (r) => r.claim_owner === relay.claimOwner,
      'relay to claim the event before the publish wedges',
    );

    // Request shutdown.
    const shutdownP = relay.onApplicationShutdown();
    let shutdownReturned = false;

    shutdownP.then(() => {
      shutdownReturned = true;
    });

    // While the stuck publish is still in flight, shutdown must NOT
    // have returned — and the row must still be claimed (the relay has
    // not yet finished its cycle).
    await new Promise((r) => setTimeout(r, 50));
    expect(shutdownReturned).toBe(false);

    const midShutdown = (
      await pool.query<{ claim_owner: string | null; published_at: string | null }>(
        sql,
        [row.outbox_event_id],
      )
    ).rows[0];
    expect(midShutdown.claim_owner).toBe(relay.claimOwner);
    expect(midShutdown.published_at).toBeNull();

    // Shutdown must return once the in-flight cycle drains — bounded by
    // the 200ms publish timeout, not by the wedged transport.
    const startedAt = Date.now();
    await shutdownP;
    const elapsed = Date.now() - startedAt;
    expect(elapsed).toBeLessThan(5_000);

    // The relay stopped waiting for the wedged publish, recorded the
    // failure, and released the claim: the event is durable and
    // retryable by another relay.
    const after = (
      await pool.query<{
        claim_owner: string | null;
        published_at: string | null;
        publish_attempts: number;
        last_error: string | null;
      }>(sql, [row.outbox_event_id])
    ).rows[0];
    expect(after.published_at).toBeNull();
    expect(after.claim_owner).toBeNull();
    expect(after.publish_attempts).toBe(1);
    expect(after.last_error).toMatch(/publish did not complete within 200ms/);

    // After shutdown completes, no further pump can perform transport
    // operations.
    await relay.pump();
    expect(publishCalls).toBe(1);

    const unchanged = (
      await pool.query<{ claim_owner: string | null; published_at: string | null }>(
        sql,
        [row.outbox_event_id],
      )
    ).rows[0];
    expect(unchanged.claim_owner).toBeNull();
    expect(unchanged.published_at).toBeNull();

    // Keep the outbox clean for the remaining tests.
    await pool.query('DELETE FROM outbox_events WHERE outbox_event_id = $1', [
      row.outbox_event_id,
    ]);
  });

  it('hasBacklog and isReady report correctly', async () => {
    expect(await engine.relay.hasBacklog()).toBe(false);

    // Insert an unpublished row directly.
    await pool.query(
      `
      INSERT INTO outbox_events (
        outbox_event_id, event_id, event_type, schema_version,
        aggregate_type, aggregate_id, correlation_id, producer,
        payload, occurred_at, created_at
      )
      VALUES (gen_random_uuid(), gen_random_uuid(), 'test.event', '1.0.0',
              'test', gen_random_uuid(), 'corr', 'test', '{}', now(), now())
    `,
    );

    expect(await engine.relay.hasBacklog()).toBe(true);
    await engine.relay.pump();
    expect(await engine.relay.hasBacklog()).toBe(false);
  });
});
