/**
 * Scheduler claim integration tests (real PostgreSQL).
 *
 * Covers contract G required tests 10 and 12 plus failure-injection:
 * scheduler crash before claim commit, scheduler crash after claim
 * commit with the relay down, lease expiry reclamation, and concurrent
 * schedulers.
 */
import type { Pool, PoolClient } from 'pg';

import { DatabaseService } from '../../src/database/database.service';
import { claimBatch } from '../../src/scheduler/claim';
import { SchedulerService } from '../../src/scheduler/scheduler.service';
import { baseConfig, createEngine, Engine, setupRun, task } from '../helpers/engine';
import { startTestDb, TestDb } from '../helpers/test-db';

describe('scheduler claim (real PostgreSQL)', () => {
  let db: TestDb;
  let pool: Pool;
  let engine: Engine;

  beforeAll(async () => {
    db = await startTestDb();
    pool = db.pool;
    engine = await createEngine(pool, baseConfig());
  }, 120_000);

  afterAll(async () => {
    await engine.stop();
    await db.stop();
  });

  async function manualClaim(
    client: PoolClient,
    owner: string,
  ): Promise<ReturnType<typeof claimBatch>> {
    await client.query('BEGIN');

    try {
      const rows = await claimBatch(client, {
        batchSize: 20,
        leaseOwner: owner,
        schedulerId: owner,
        leaseMs: 30_000,
      });
      await client.query('COMMIT');
      return rows;
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    }
  }

  async function expireLease(taskExecutionId: string): Promise<void> {
    await pool.query(
      `UPDATE task_executions
       SET lease_expires_at = now() - interval '1 second'
       WHERE task_execution_id = $1`,
      [taskExecutionId],
    );
  }

  async function runTasks(runId: string) {
    const rows = await pool.query<{
      task_execution_id: string;
      task_key: string;
      state: string;
      fencing_token: number;
      attempt_count: number;
    }>(
      `
      SELECT task_execution_id, task_key, state, fencing_token, attempt_count
      FROM task_executions
      WHERE workflow_run_id = $1
      ORDER BY task_key
    `,
      [runId],
    );

    return rows.rows;
  }

  it('concurrent schedulers create only one valid claim and one attempt (G-10)', async () => {
    const { workflowRunId } = await setupRun(
      engine,
      [task('a'), task('b')],
      { name: 'concurrent-claim' },
    );

    const clients = await Promise.all(
      Array.from({ length: 4 }, () => pool.connect()),
    );

    try {
      const results = await Promise.all(
        clients.map((client, i) => manualClaim(client, `sched-${i}`)),
      );

      const totalClaims = results.reduce((n, r) => n + r.length, 0);
      expect(totalClaims).toBe(2);

      const tasks = await runTasks(workflowRunId);
      expect(tasks).toHaveLength(2);

      for (const t of tasks) {
        expect(t.state).toBe('CLAIMED');
        expect(t.fencing_token).toBe(1);
        expect(t.attempt_count).toBe(1);

        const attempts = (
          await pool.query<{ n: number }>(
            `SELECT count(*)::int AS n FROM task_attempts
             WHERE task_execution_id = $1`,
            [t.task_execution_id],
          )
        ).rows[0];
        expect(attempts.n).toBe(1);

        const claimedEvents = (
          await pool.query<{ n: number }>(
            `SELECT count(*)::int AS n FROM outbox_events
             WHERE task_execution_id = $1 AND event_type = 'task.claimed'`,
            [t.task_execution_id],
          )
        ).rows[0];
        expect(claimedEvents.n).toBe(1);

        const dispatchEvents = (
          await pool.query<{ n: number }>(
            `SELECT count(*)::int AS n FROM outbox_events
             WHERE task_execution_id = $1 AND event_type = 'task.dispatch.requested'`,
            [t.task_execution_id],
          )
        ).rows[0];
        expect(dispatchEvents.n).toBe(1);
      }

    } finally {
      for (const client of clients) {
        client.release();
      }
    }

    // Re-scanning while leases are valid claims nothing.
    const scheduler2 = new SchedulerService(
      new DatabaseService(pool),
      baseConfig(),
    );
    const rescanned = await scheduler2.scan();
    expect(rescanned).toBe(0);
  });

  it('an expired lease can be reclaimed with a higher fencing token (G-12)', async () => {
    const { workflowRunId } = await setupRun(engine, [
      task('solo', [], { retryPolicy: { maxAttempts: 5 } }),
    ]);

    const scheduler = new SchedulerService(
      new DatabaseService(pool),
      baseConfig(),
    );

    // First claim.
    expect(await scheduler.scan()).toBe(1);

    let [t] = await runTasks(workflowRunId);
    expect(t.state).toBe('CLAIMED');
    expect(t.fencing_token).toBe(1);

    const attempt1 = (
      await pool.query<{ task_attempt_id: string; state: string; fencing_token: number }>(
        'SELECT task_attempt_id, state, fencing_token FROM task_attempts WHERE task_execution_id = $1 ORDER BY attempt_number',
        [t.task_execution_id],
      )
    ).rows;
    expect(attempt1).toHaveLength(1);
    expect(attempt1[0].state).toBe('DISPATCHED');

    // Simulate the worker dying: expire the lease and reclaim.
    await expireLease(t.task_execution_id);
    expect(await scheduler.scan()).toBe(1);

    [t] = await runTasks(workflowRunId);
    expect(t.state).toBe('CLAIMED');
    expect(t.fencing_token).toBe(2);
    expect(t.attempt_count).toBe(2);

    const attempts2 = (
      await pool.query<{ n: number; state: string }>(
        `SELECT count(*)::int AS n, min(state) AS state
         FROM task_attempts
         WHERE task_execution_id = $1`,
        [t.task_execution_id],
      )
    ).rows[0];
    expect(attempts2.n).toBe(2);

    // The abandoned attempt is the first one.
    const attemptStates = (
      await pool.query<{ attempt_number: number; state: string }>(
        `SELECT attempt_number, state FROM task_attempts
         WHERE task_execution_id = $1
         ORDER BY attempt_number`,
        [t.task_execution_id],
      )
    ).rows;
    expect(attemptStates[0].state).toBe('ABANDONED');
    expect(attemptStates[1].state).toBe('DISPATCHED');
  });

  it('scheduler crash before claim commit leaves the task claimable (no orphans)', async () => {
    const { workflowRunId } = await setupRun(engine, [
      task('crashy', [], { retryPolicy: { maxAttempts: 1 } }),
    ]);

    const client = await pool.connect();

    try {
      // Begin a claim transaction, then abort it (process crash before
      // commit).
      await client.query('BEGIN');
      await claimBatch(client, {
        batchSize: 10,
        leaseOwner: 'crashed-scheduler',
        schedulerId: 'crashed-scheduler',
        leaseMs: 30_000,
      });
      await client.query('ROLLBACK'); // crash
    } finally {
      client.release();
    }

    let tasks = await runTasks(workflowRunId);
    expect(tasks[0].state).toBe('READY');
    expect(tasks[0].fencing_token).toBe(0);

    const orphanAttempts = (
      await pool.query<{ n: number }>(
        `SELECT count(*)::int AS n FROM task_attempts
         WHERE task_execution_id = (
           SELECT task_execution_id FROM task_executions
           WHERE workflow_run_id = $1
         )`,
        [workflowRunId],
      )
    ).rows[0];
    expect(orphanAttempts.n).toBe(0);

    // Another scheduler claims it successfully.
    const scheduler = new SchedulerService(
      new DatabaseService(pool),
      baseConfig(),
    );
    expect(await scheduler.scan()).toBe(1);

    tasks = await runTasks(workflowRunId);
    expect(tasks[0].state).toBe('CLAIMED');
    expect(tasks[0].fencing_token).toBe(1);
  });

  it('scheduler crash after claim commit with relay down: dispatch intent stays durable and is reclaimable', async () => {
    const { workflowRunId } = await setupRun(engine, [
      task('relaydown', [], { retryPolicy: { maxAttempts: 3 } }),
    ]);

    const scheduler = new SchedulerService(
      new DatabaseService(pool),
      baseConfig(),
    );

    // Claim commits; the relay never runs (crashed / down).
    expect(await scheduler.scan()).toBe(1);

    const [t] = await runTasks(workflowRunId);
    expect(t.state).toBe('CLAIMED');

    const unpublished = (
      await pool.query<{ n: number }>(
        `SELECT count(*)::int AS n FROM outbox_events
         WHERE task_execution_id = $1
           AND event_type = 'task.dispatch.requested'
           AND published_at IS NULL`,
        [t.task_execution_id],
      )
    ).rows[0];
    expect(unpublished.n).toBe(1);

    // With a valid lease the task is not re-claimed (no duplicate claim).
    expect(await scheduler.scan()).toBe(0);

    // Once the lease expires it is reclaimed with a higher token; the
    // outbox still holds a dispatch intent the relay can publish.
    await expireLease(t.task_execution_id);
    expect(await scheduler.scan()).toBe(1);

    const [t2] = await runTasks(workflowRunId);
    expect(t2.fencing_token).toBe(2);
  });

  it('does not claim tasks of non-RUNNING runs', async () => {
    const { workflowRunId } = await setupRun(engine, [
      task('orphan', [], { retryPolicy: { maxAttempts: 1 } }),
    ]);

    // Force the run to FAILED without completing tasks (simulates an
    // external run-level failure record).
    await pool.query(
      `UPDATE workflow_runs
       SET state = 'FAILED', completed_at = now(), failure_code = 'TEST'
       WHERE workflow_run_id = $1`,
      [workflowRunId],
    );

    const scheduler = new SchedulerService(
      new DatabaseService(pool),
      baseConfig(),
    );
    expect(await scheduler.scan()).toBe(0);

    const tasks = await runTasks(workflowRunId);
    expect(tasks[0].state).toBe('READY'); // unchanged, non-executable
  });
});
