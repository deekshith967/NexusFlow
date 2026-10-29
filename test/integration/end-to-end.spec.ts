/**
 * End-to-end execution tests (real PostgreSQL; in-memory transport in
 * place of the Kafka broker, which does not exist in this environment —
 * the Kafka transport itself is covered by unit tests).
 *
 * The FULL loop runs: run start -> scheduler claim -> outbox dispatch
 * intent -> relay publication -> worker dispatch consumption -> handler
 * execution -> conditional completion -> dependency activation ->
 * workflow completion, with retries and failure policies.
 */
import type { Pool } from 'pg';

import { baseConfig, createEngine, Engine, setupRun, task } from '../helpers/engine';
import { startTestDb, TestDb } from '../helpers/test-db';
import { FailurePolicy } from '../../src/workflow/domain/failure-policy';

describe('end-to-end workflow execution', () => {
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

  it('executes a dependent DAG to completion with correct inputs and outputs', async () => {
    const { workflowRunId } = await setupRun(
      engine,
      [
        task('fetch'),
        task('enrich', ['fetch'], { handlerType: 'merge', configuration: { with: { enriched: true } } }),
        task('storeA', ['enrich']),
        task('storeB', ['enrich']),
        task('finalize', ['storeA', 'storeB'], { handlerType: 'merge', configuration: { with: { finalized: true } } }),
      ],
      { runInput: { orderId: 42 } },
    );

    const finalState = await engine.drainRun(workflowRunId);
    expect(finalState).toBe('SUCCEEDED');

    const rows = (
      await pool.query<{ task_key: string; state: string; output_payload: unknown }>(
        `SELECT task_key, state, output_payload
         FROM task_executions
         WHERE workflow_run_id = $1
         ORDER BY task_key`,
        [workflowRunId],
      )
    ).rows;

    const byKey = new Map(rows.map((r) => [r.task_key, r]));

    for (const r of rows) {
      expect(r.state).toBe('SUCCEEDED');
    }

    // The run input propagated to the root task.
    const fetchOutput = byKey.get('fetch')!.output_payload as {
      echo: { orderId?: number; upstream?: Record<string, unknown> };
    };
    expect(fetchOutput.echo.orderId).toBe(42);
    expect(fetchOutput.echo.upstream).toEqual({});

    // Upstream outputs flowed into downstream resolved inputs.
    const enrichInput = (
      await pool.query<{ resolved_input: unknown }>(
        `SELECT resolved_input FROM task_executions
         WHERE workflow_run_id = $1 AND task_key = 'enrich'`,
        [workflowRunId],
      )
    ).rows[0];
    const enrichResolved = enrichInput.resolved_input as {
      orderId?: number;
      upstream?: Record<string, unknown>;
    };
    expect(enrichResolved.orderId).toBe(42);
    expect(enrichResolved.upstream).toHaveProperty('fetch');

    // The merge handler applied its configuration.
    expect(byKey.get('enrich')!.output_payload).toEqual(
      expect.objectContaining({ orderId: 42, enriched: true }),
    );

    // All tasks completed with exactly one successful attempt each.
    const attempts = (
      await pool.query<{ n: number }>(
        `SELECT count(*)::int AS n FROM task_attempts
         WHERE state = 'SUCCEEDED'
           AND task_execution_id IN (
             SELECT task_execution_id FROM task_executions
             WHERE workflow_run_id = $1
           )`,
        [workflowRunId],
      )
    ).rows[0];
    expect(attempts.n).toBe(5);
  });

  it('recovers from a worker crash during execution via lease expiry and re-executes', async () => {
    // Long sleep handler with a short lease: the worker "crashes" (the
    // test never reports) and the lease expires while the handler would
    // still be running. Recovery reclaims with a higher fencing token.
    const { workflowRunId } = await setupRun(
      engine,
      [
        task('longrun', [], {
          handlerType: 'sleep',
          retryPolicy: { maxAttempts: 3, retryDelayMs: 0 },
          configuration: { ms: 1_500 },
          timeoutMs: 60_000,
        }),
      ],
      { name: `crash-${Date.now()}` },
    );

    // Let the scheduler claim and the relay publish the dispatch; the
    // worker starts the handler (which sleeps 3s). Do NOT drain here:
    // the handler must still be running when the lease is expired.
    await engine.scheduler.scan();
    await engine.relay.pump();
    await new Promise((resolve) => setTimeout(resolve, 500));

    const before = (
      await pool.query<{ state: string }>(
        'SELECT state FROM task_executions WHERE workflow_run_id = $1',
        [workflowRunId],
      )
    ).rows[0];
    expect(before.state).toBe('RUNNING');

    // Simulate the worker crashing: force-expire the lease while the
    // handler is still running (it will later report with a stale
    // fencing token and must be rejected).
    await pool.query(
      `
      UPDATE task_executions
      SET lease_expires_at = now() - interval '1 second'
      WHERE workflow_run_id = $1
    `,
      [workflowRunId],
    );

    // Recovery: the scheduler reclaims with a higher fencing token and
    // the replacement attempt completes the run.
    const finalState = await engine.drainRun(workflowRunId, 30_000);
    expect(finalState).toBe('SUCCEEDED');

    // The stale first worker's late report must not have applied.
    const output = (
      await pool.query<{ output_payload: unknown }>(
        'SELECT output_payload FROM task_executions WHERE workflow_run_id = $1',
        [workflowRunId],
      )
    ).rows[0];
    expect(output.output_payload).toEqual(
      expect.objectContaining({ sleptMs: 1_500 }),
    );

    const exec = (
      await pool.query<{ fencing_token: number; attempt_count: number }>(
        'SELECT fencing_token, attempt_count FROM task_executions WHERE workflow_run_id = $1',
        [workflowRunId],
      )
    ).rows[0];

    // At least one reclamation happened (fencing token > 1).
    expect(exec.fencing_token).toBeGreaterThanOrEqual(2);

    // The first attempt is ABANDONED (lost ownership), later attempt
    // SUCCEEDED.
    const attemptStates = (
      await pool.query<{ state: string }>(
        `
        SELECT state FROM task_attempts
        WHERE task_execution_id = (
          SELECT task_execution_id FROM task_executions
          WHERE workflow_run_id = $1
        )
        ORDER BY attempt_number
      `,
        [workflowRunId],
      )
    ).rows.map((r) => r.state);
    expect(attemptStates[0]).toBe('ABANDONED');
    expect(attemptStates).toContain('SUCCEEDED');
  }, 30_000);

  it('FAIL_WORKFLOW: a permanent failure fails the run and blocks the rest', async () => {
    const { workflowRunId } = await setupRun(
      engine,
      [
        task('ok1'),
        task('boom', ['ok1'], {
          handlerType: 'fail',
          retryPolicy: { maxAttempts: 2, retryDelayMs: 0 },
          configuration: { failUntilAttempt: 99, retryable: true },
        }),
        task('never', ['boom']),
      ],
    );

    const finalState = await engine.drainRun(workflowRunId);
    expect(finalState).toBe('FAILED');

    const rows = (
      await pool.query<{ task_key: string; state: string }>(
        'SELECT task_key, state FROM task_executions WHERE workflow_run_id = $1 ORDER BY task_key',
        [workflowRunId],
      )
    ).rows;
    const byKey = new Map(rows.map((r) => [r.task_key, r]));

    expect(byKey.get('ok1')!.state).toBe('SUCCEEDED');
    expect(byKey.get('boom')!.state).toBe('FAILED');
    expect(byKey.get('never')!.state).toBe('BLOCKED');

    // The run carries the failure details.
    const run = (
      await pool.query<{ state: string; failure_code: string | null }>(
        'SELECT state, failure_code FROM workflow_runs WHERE workflow_run_id = $1',
        [workflowRunId],
      )
    ).rows[0];
    expect(run.state).toBe('FAILED');
    expect(run.failure_code).toBe('HANDLER_FAILED');

    // Attempts: 2 attempts for boom (retries exhausted).
    const attempts = (
      await pool.query<{ n: number }>(
        `SELECT count(*)::int AS n FROM task_attempts
         WHERE task_execution_id = (
           SELECT task_execution_id FROM task_executions
           WHERE workflow_run_id = $1 AND task_key = 'boom'
         )`,
        [workflowRunId],
      )
    ).rows[0];
    expect(attempts.n).toBe(2);
  });

  it('SKIP_DOWNSTREAM: the failed task is SKIPPED and the rest completes', async () => {
    const { workflowRunId } = await setupRun(
      engine,
      [
        task('flaky-skip', [], {
          handlerType: 'fail',
          retryPolicy: { maxAttempts: 1 },
          failurePolicy: FailurePolicy.SKIP_DOWNSTREAM,
          configuration: { failUntilAttempt: 99, retryable: false },
        }),
        task('sibling'),
        task('combine', ['flaky-skip', 'sibling'], {
          handlerType: 'merge',
          configuration: { with: { combined: true } },
        }),
      ],
    );

    const finalState = await engine.drainRun(workflowRunId);
    expect(finalState).toBe('SUCCEEDED');

    const rows = (
      await pool.query<{ task_key: string; state: string }>(
        'SELECT task_key, state FROM task_executions WHERE workflow_run_id = $1 ORDER BY task_key',
        [workflowRunId],
      )
    ).rows;
    const byKey = new Map(rows.map((r) => [r.task_key, r]));

    expect(byKey.get('flaky-skip')!.state).toBe('SKIPPED');
    expect(byKey.get('sibling')!.state).toBe('SUCCEEDED');
    expect(byKey.get('combine')!.state).toBe('SUCCEEDED');
  });

  it('demonstrates retry-then-success with a deterministic failing handler', async () => {
    const { workflowRunId } = await setupRun(
      engine,
      [
        task('unstable', [], {
          handlerType: 'fail',
          retryPolicy: { maxAttempts: 4, retryDelayMs: 0 },
          configuration: { failUntilAttempt: 3, retryable: true },
        }),
      ],
    );

    const finalState = await engine.drainRun(workflowRunId);
    expect(finalState).toBe('SUCCEEDED');

    const attempts = (
      await pool.query<{ attempt_number: number; state: string; failure_code: string | null }>(
        `
        SELECT attempt_number, state, failure_code
        FROM task_attempts
        WHERE task_execution_id = (
          SELECT task_execution_id FROM task_executions
          WHERE workflow_run_id = $1
        )
        ORDER BY attempt_number
      `,
        [workflowRunId],
      )
    ).rows;

    expect(attempts).toHaveLength(3);
    expect(attempts.map((a) => a.state)).toEqual([
      'FAILED',
      'FAILED',
      'SUCCEEDED',
    ]);
    expect(attempts[0].failure_code).toBe('HANDLER_FAILED');
  });

  it('records a real worker handler timeout as a TIMED_OUT attempt, then fails the run', async () => {
    const { workflowRunId } = await setupRun(engine, [
      task('too-slow', [], {
        handlerType: 'sleep',
        retryPolicy: { maxAttempts: 2, retryDelayMs: 0 },
        timeoutMs: 50,
        configuration: { ms: 400 },
      }),
    ]);

    const finalState = await engine.drainRun(workflowRunId);
    expect(finalState).toBe('FAILED');

    const attempts = (
      await pool.query<{ attempt_number: number; state: string; failure_code: string | null }>(
        `
        SELECT attempt_number, state, failure_code
        FROM task_attempts
        WHERE task_execution_id = (
          SELECT task_execution_id FROM task_executions
          WHERE workflow_run_id = $1
        )
        ORDER BY attempt_number
      `,
        [workflowRunId],
      )
    ).rows;

    // Timeout is classified retryable: two attempts, both TIMED_OUT.
    expect(attempts.map((a) => a.state)).toEqual(['TIMED_OUT', 'TIMED_OUT']);
    expect(attempts.every((a) => a.failure_code === 'TIMEOUT')).toBe(true);

    const taskRow = (
      await pool.query<{ state: string; failure_code: string | null }>(
        'SELECT state, failure_code FROM task_executions WHERE workflow_run_id = $1',
        [workflowRunId],
      )
    ).rows[0];
    expect(taskRow).toEqual({ state: 'FAILED', failure_code: 'TIMEOUT' });
  });

  it('all lifecycle outbox events are published for a successful run', async () => {
    const { workflowRunId } = await setupRun(engine, [task('e2e-evt')]);
    await engine.drainRun(workflowRunId);

    const events = (
      await pool.query<{ event_type: string; published_at: string | null }>(
        'SELECT event_type, published_at FROM outbox_events WHERE workflow_run_id = $1',
        [workflowRunId],
      )
    ).rows;

    const published = events.filter((e) => e.published_at !== null);
    expect(published.length).toBe(events.length); // all published

    const types = published.map((e) => e.event_type).sort();
    expect(types).toEqual(
      expect.arrayContaining([
        'workflow.run.started',
        'task.ready',
        'task.claimed',
        'task.dispatch.requested',
        'task.started',
        'task.succeeded',
        'workflow.run.succeeded',
      ]),
    );
  });
});
