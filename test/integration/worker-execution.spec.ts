/**
 * Worker execution integration tests (real PostgreSQL).
 *
 * Covers contract G required tests 11, 13, 14, 15 plus retry
 * scheduling, terminal failures, failure policies, timeout handling,
 * and stale-worker protection.
 */
import type { Pool } from 'pg';

import { DatabaseService } from '../../src/database/database.service';
import { SchedulerService } from '../../src/scheduler/scheduler.service';
import { baseConfig, createEngine, Engine, setupRun, task } from '../helpers/engine';
import { startTestDb, TestDb } from '../helpers/test-db';
import { completeFailure, completeSuccess, renewLease, startAttempt } from '../../src/worker/worker-ops';
import { FailurePolicy } from '../../src/workflow/domain/failure-policy';

describe('worker execution (real PostgreSQL)', () => {
  let db: TestDb;
  let pool: Pool;
  let engine: Engine;

  beforeAll(async () => {
    db = await startTestDb();
    pool = db.pool;
    engine = await createEngine(pool, baseConfig({ leaseMs: 30_000 }));
  }, 120_000);

  afterAll(async () => {
    await engine.stop();
    await db.stop();
  });

  const database = () => new DatabaseService(pool);

  async function claimAndStart(workflowRunId: string) {
    const scheduler = new SchedulerService(database(), baseConfig());
    expect(await scheduler.scan()).toBeGreaterThan(0);

    const rows = (
      await pool.query<{
        task_execution_id: string;
        task_key: string;
        state: string;
        fencing_token: number;
      }>(
        `
        SELECT task_execution_id, task_key, state, fencing_token
        FROM task_executions
        WHERE workflow_run_id = $1 AND state = 'CLAIMED'
        ORDER BY task_key
      `,
        [workflowRunId],
      )
    ).rows;

    const started: {
      taskExecutionId: string;
      taskKey: string;
      fencingToken: number;
      dispatchId: string;
      attemptId: string;
      attemptNumber: number;
    }[] = [];

    for (const row of rows) {
      const attempt = (
        await pool.query<{
          task_attempt_id: string;
          dispatch_id: string;
          attempt_number: number;
        }>(
          `
          SELECT task_attempt_id, dispatch_id, attempt_number
          FROM task_attempts
          WHERE task_execution_id = $1
            AND state = 'DISPATCHED'
          ORDER BY attempt_number DESC
          LIMIT 1
        `,
          [row.task_execution_id],
        )
      ).rows[0];

      const result = await startAttempt(database(), {
        dispatchId: attempt.dispatch_id,
        taskExecutionId: row.task_execution_id,
        fencingToken: row.fencing_token,
        workerId: 'test-worker',
      });

      expect(result.ok).toBe(true);

      started.push({
        taskExecutionId: row.task_execution_id,
        taskKey: row.task_key,
        fencingToken: row.fencing_token,
        dispatchId: attempt.dispatch_id,
        attemptId: attempt.task_attempt_id,
        attemptNumber: attempt.attempt_number,
      });
    }

    return started;
  }

  it('lease renewal succeeds only with the current fencing token and valid lease (G-11)', async () => {
    const { workflowRunId } = await setupRun(engine, [
      task('renew', [], { retryPolicy: { maxAttempts: 3 } }),
    ]);

    const [s] = await claimAndStart(workflowRunId);

    // Current token, valid lease: renewal succeeds.
    expect(
      await renewLease(database(), {
        taskExecutionId: s.taskExecutionId,
        fencingToken: s.fencingToken,
        leaseMs: 30_000,
      }),
    ).toBe(true);

    // Stale token: renewal fails.
    expect(
      await renewLease(database(), {
        taskExecutionId: s.taskExecutionId,
        fencingToken: s.fencingToken - 1,
        leaseMs: 30_000,
      }),
    ).toBe(false);

    // Expired lease: renewal fails.
    await pool.query(
      `UPDATE task_executions
       SET lease_expires_at = now() - interval '1 second'
       WHERE task_execution_id = $1`,
      [s.taskExecutionId],
    );
    expect(
      await renewLease(database(), {
        taskExecutionId: s.taskExecutionId,
        fencingToken: s.fencingToken,
        leaseMs: 30_000,
      }),
    ).toBe(false);
  });

  it('stale completion with an old fencing token is rejected (G-13)', async () => {
    const { workflowRunId } = await setupRun(engine, [
      task('stale', [], { retryPolicy: { maxAttempts: 5 } }),
    ]);

    // Claim 1 -> token 1, start it.
    const first = await claimAndStart(workflowRunId);
    expect(first).toHaveLength(1);

    // The worker dies; the lease expires and the scheduler reclaims
    // with a higher fencing token (token 2).
    await pool.query(
      `UPDATE task_executions
       SET lease_expires_at = now() - interval '1 second'
       WHERE task_execution_id = $1`,
      [first[0].taskExecutionId],
    );
    const second = await claimAndStart(workflowRunId);
    expect(second).toHaveLength(1);
    expect(second[0].fencingToken).toBe(first[0].fencingToken + 1);

    // The old worker (token 1, old attempt) tries to complete: rejected.
    const stale = await completeSuccess(database(), {
      taskExecutionId: first[0].taskExecutionId,
      attemptId: first[0].attemptId,
      fencingToken: first[0].fencingToken,
      workerId: 'stale-worker',
      output: { from: 'stale-worker' },
    });
    expect(stale.applied).toBe(false);

    // The task is untouched by the stale report (it is RUNNING under
    // the new claim, started by claimAndStart above).
    const state = (
      await pool.query<{ state: string; fencing_token: number }>(
        'SELECT state, fencing_token FROM task_executions WHERE task_execution_id = $1',
        [first[0].taskExecutionId],
      )
    ).rows[0];
    expect(state.state).toBe('RUNNING');
    expect(state.fencing_token).toBe(second[0].fencingToken);

    // The current worker completes successfully.
    const ok = await completeSuccess(database(), {
      taskExecutionId: second[0].taskExecutionId,
      attemptId: second[0].attemptId,
      fencingToken: second[0].fencingToken,
      workerId: 'current-worker',
      output: { from: 'current-worker' },
    });
    expect(ok.applied).toBe(true);

    const final = (
      await pool.query<{ state: string; output_payload: unknown }>(
        'SELECT state, output_payload FROM task_executions WHERE task_execution_id = $1',
        [first[0].taskExecutionId],
      )
    ).rows[0];
    expect(final.state).toBe('SUCCEEDED');
    expect(final.output_payload).toEqual({ from: 'current-worker' });
  });

  it('concurrent fan-in prerequisite completion activates the downstream task exactly once (G-14)', async () => {
    const { workflowRunId } = await setupRun(engine, [
      task('p1'),
      task('p2'),
      task('join', ['p1', 'p2']),
    ]);

    const claimed = await claimAndStart(workflowRunId);
    expect(claimed).toHaveLength(2); // p1 and p2 (join is WAITING)

    const byKey = new Map(claimed.map((c) => [c.taskKey, c]));

    // Both prerequisites complete concurrently.
    const results = await Promise.all([
      completeSuccess(database(), {
        taskExecutionId: byKey.get('p1')!.taskExecutionId,
        attemptId: byKey.get('p1')!.attemptId,
        fencingToken: byKey.get('p1')!.fencingToken,
        workerId: 'worker-p1',
        output: { from: 'p1' },
      }),
      completeSuccess(database(), {
        taskExecutionId: byKey.get('p2')!.taskExecutionId,
        attemptId: byKey.get('p2')!.attemptId,
        fencingToken: byKey.get('p2')!.fencingToken,
        workerId: 'worker-p2',
        output: { from: 'p2' },
      }),
    ]);

    expect(results.every((r) => r.applied)).toBe(true);

    const join = (
      await pool.query<{ state: string }>(
        `
        SELECT te.state
        FROM task_executions te
        WHERE te.workflow_run_id = $1 AND te.task_key = 'join'
      `,
        [workflowRunId],
      )
    ).rows[0];
    expect(join.state).toBe('READY');

    // Exactly one task.ready event for the downstream task.
    const readyEvents = (
      await pool.query<{ n: number }>(
        `
        SELECT count(*)::int AS n
        FROM outbox_events
        WHERE event_type = 'task.ready'
          AND workflow_run_id = $1
          AND payload->>'taskKey' = 'join'
      `,
        [workflowRunId],
      )
    ).rows[0];
    expect(readyEvents.n).toBe(1);
  });

  it('successful completion, dependency activation, and workflow completion are atomic with outbox insertion (G-15a)', async () => {
    const { workflowRunId } = await setupRun(engine, [task('only')]);

    const [s] = await claimAndStart(workflowRunId);
    const result = await completeSuccess(database(), {
      taskExecutionId: s.taskExecutionId,
      attemptId: s.attemptId,
      fencingToken: s.fencingToken,
      workerId: 'worker-1',
      output: { done: true },
    });

    expect(result.applied).toBe(true);
    expect(result.runState).toBe('SUCCEEDED');

    // Task, attempt, run, and outbox events all committed together.
    const run = (
      await pool.query<{ state: string; completed_at: string | null }>(
        'SELECT state, completed_at FROM workflow_runs WHERE workflow_run_id = $1',
        [workflowRunId],
      )
    ).rows[0];
    expect(run.state).toBe('SUCCEEDED');
    expect(run.completed_at).not.toBeNull();

    const events = (
      await pool.query<{ event_type: string }>(
        `
        SELECT event_type FROM outbox_events
        WHERE workflow_run_id = $1
        ORDER BY event_type
      `,
        [workflowRunId],
      )
    ).rows.map((r: { event_type: string }) => r.event_type);
    expect(events).toEqual(
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

    // A duplicate success report is a no-op.
    const dup = await completeSuccess(database(), {
      taskExecutionId: s.taskExecutionId,
      attemptId: s.attemptId,
      fencingToken: s.fencingToken,
      workerId: 'worker-1',
      output: { done: true },
    });
    expect(dup.applied).toBe(false);
  });

  it('retryable failure schedules RETRY_WAIT and a later attempt succeeds (G-15b)', async () => {
    const { workflowRunId } = await setupRun(engine, [
      task('flaky', [], {
        retryPolicy: { maxAttempts: 3, retryDelayMs: 0 },
        configuration: { failUntilAttempt: 2 },
        handlerType: 'fail',
      }),
    ]);

    // Attempt 1: the 'fail' handler fails while attemptNumber < 2.
    const first = await claimAndStart(workflowRunId);
    const s1 = first[0];
    const fail1 = await completeFailure(database(), {
      taskExecutionId: s1.taskExecutionId,
      attemptId: s1.attemptId,
      fencingToken: s1.fencingToken,
      workerId: 'worker-flaky',
      failure: {
        code: 'HANDLER_FAILED',
        message: 'simulated transient failure',
        retryable: true,
      },
      attemptNumber: s1.attemptNumber,
      maxAttempts: 3,
      retryDelayMs: 0,
      failurePolicy: 'FAIL_WORKFLOW',
    });
    expect(fail1.applied).toBe(true);
    expect(fail1.taskState).toBe('RETRY_WAIT');
    expect(fail1.runState).toBe('RUNNING');

    const retryEvent = (
      await pool.query<{ n: number }>(
        `
        SELECT count(*)::int AS n
        FROM outbox_events
        WHERE workflow_run_id = $1 AND event_type = 'task.retry_scheduled'
      `,
        [workflowRunId],
      )
    ).rows[0];
    expect(retryEvent.n).toBe(1);

    // Attempt 2 succeeds (failUntilAttempt=2).
    const second = await claimAndStart(workflowRunId);
    expect(second).toHaveLength(1);
    expect(second[0].attemptNumber).toBe(2);

    const ok = await completeSuccess(database(), {
      taskExecutionId: second[0].taskExecutionId,
      attemptId: second[0].attemptId,
      fencingToken: second[0].fencingToken,
      workerId: 'worker-flaky',
      output: { recoveredAtAttempt: 2 },
    });
    expect(ok.applied).toBe(true);
    expect(ok.runState).toBe('SUCCEEDED');

    // Durable attempt history.
    const attempts = (
      await pool.query<{ attempt_number: number; state: string }>(
        `
        SELECT attempt_number, state FROM task_attempts
        WHERE task_execution_id = $1
        ORDER BY attempt_number
      `,
      [s1.taskExecutionId],
    )
    ).rows;
    expect(attempts.map((a) => a.state)).toEqual(['FAILED', 'SUCCEEDED']);
  });

  it('exhausted retries with FAIL_WORKFLOW fail the task and the run; downstream is BLOCKED (G-15c)', async () => {
    const { workflowRunId } = await setupRun(engine, [
      task('doomed', [], {
        handlerType: 'fail',
        retryPolicy: { maxAttempts: 1 },
      }),
      task('after', ['doomed']),
    ]);

    const [s] = await claimAndStart(workflowRunId);
    const fail = await completeFailure(database(), {
      taskExecutionId: s.taskExecutionId,
      attemptId: s.attemptId,
      fencingToken: s.fencingToken,
      workerId: 'worker-doomed',
      failure: {
        code: 'PERMANENT',
        message: 'cannot continue',
        retryable: true, // but attempts exhausted (maxAttempts=1)
      },
      attemptNumber: s.attemptNumber,
      maxAttempts: 1,
      retryDelayMs: 0,
      failurePolicy: 'FAIL_WORKFLOW',
    });

    expect(fail.applied).toBe(true);
    expect(fail.taskState).toBe('FAILED');
    expect(fail.runState).toBe('FAILED');

    const states = (
      await pool.query<{ task_key: string; state: string }>(
        'SELECT task_key, state FROM task_executions WHERE workflow_run_id = $1 ORDER BY task_key',
        [workflowRunId],
      )
    ).rows;
    expect(states).toEqual(
      expect.arrayContaining([
        { task_key: 'after', state: 'BLOCKED' },
        { task_key: 'doomed', state: 'FAILED' },
      ]),
    );

    const failedEvent = (
      await pool.query<{ n: number }>(
        `SELECT count(*)::int AS n FROM outbox_events
         WHERE workflow_run_id = $1 AND event_type = 'workflow.run.failed'`,
        [workflowRunId],
      )
    ).rows[0];
    expect(failedEvent.n).toBe(1);

    // A late report from the failed attempt cannot change the run.
    const late = await completeSuccess(database(), {
      taskExecutionId: s.taskExecutionId,
      attemptId: s.attemptId,
      fencingToken: s.fencingToken,
      workerId: 'late-worker',
      output: { late: true },
    });
    expect(late.applied).toBe(false);
  });

  it('SKIP_DOWNSTREAM: failed task becomes SKIPPED and eligible descendants continue (G-15d)', async () => {
    const { workflowRunId } = await setupRun(engine, [
      task('bad', [], {
        handlerType: 'fail',
        retryPolicy: { maxAttempts: 1 },
        failurePolicy: FailurePolicy.SKIP_DOWNSTREAM,
      }),
      task('good'),
      task('fanin', ['bad', 'good']),
    ]);

    // Claim and start bad + good (fanin is WAITING).
    const claimed = await claimAndStart(workflowRunId);
    expect(claimed).toHaveLength(2);
    const byKey = new Map(claimed.map((c) => [c.taskKey, c]));

    // bad fails permanently with SKIP_DOWNSTREAM.
    const fail = await completeFailure(database(), {
      taskExecutionId: byKey.get('bad')!.taskExecutionId,
      attemptId: byKey.get('bad')!.attemptId,
      fencingToken: byKey.get('bad')!.fencingToken,
      workerId: 'worker-bad',
      failure: {
        code: 'PERMANENT',
        message: 'skipped downstream',
        retryable: false,
      },
      attemptNumber: byKey.get('bad')!.attemptNumber,
      maxAttempts: 1,
      retryDelayMs: 0,
      failurePolicy: FailurePolicy.SKIP_DOWNSTREAM,
    });
    expect(fail.applied).toBe(true);
    expect(fail.taskState).toBe('SKIPPED');
    expect(fail.runState).toBe('RUNNING'); // good is still active

    // good succeeds -> fanin (deps bad+good) becomes READY.
    const ok = await completeSuccess(database(), {
      taskExecutionId: byKey.get('good')!.taskExecutionId,
      attemptId: byKey.get('good')!.attemptId,
      fencingToken: byKey.get('good')!.fencingToken,
      workerId: 'worker-good',
      output: { from: 'good' },
    });
    expect(ok.applied).toBe(true);

    const fanin = (
      await pool.query<{ state: string }>(
        `SELECT state FROM task_executions
         WHERE workflow_run_id = $1 AND task_key = 'fanin'`,
        [workflowRunId],
      )
    ).rows[0];
    expect(fanin.state).toBe('READY');

    // fanin succeeds -> run SUCCEEDED despite the skipped task.
    const faninClaim = await claimAndStart(workflowRunId);
    expect(faninClaim).toHaveLength(1);
    expect(faninClaim[0].taskKey).toBe('fanin');

    const done = await completeSuccess(database(), {
      taskExecutionId: faninClaim[0].taskExecutionId,
      attemptId: faninClaim[0].attemptId,
      fencingToken: faninClaim[0].fencingToken,
      workerId: 'worker-fanin',
      output: { merged: true },
    });
    expect(done.applied).toBe(true);
    expect(done.runState).toBe('SUCCEEDED');

    // The run input and upstream outputs were resolved into the input.
    const faninRow = (
      await pool.query<{ resolved_input: unknown }>(
        `SELECT resolved_input FROM task_executions
         WHERE workflow_run_id = $1 AND task_key = 'fanin'`,
        [workflowRunId],
      )
    ).rows[0];
    const faninInput = faninRow.resolved_input as {
      upstream?: Record<string, unknown>;
    };
    expect(faninInput.upstream?.bad).toBeNull();
    expect(faninInput.upstream?.good).toEqual({ from: 'good' });
  });

  it('handler timeout is recorded as TIMED_OUT and retried', async () => {
    const { workflowRunId } = await setupRun(engine, [
      task('slow', [], {
        handlerType: 'sleep',
        retryPolicy: { maxAttempts: 2, retryDelayMs: 0 },
        timeoutMs: 50,
        configuration: { ms: 5_000 },
      }),
    ]);

    // Simulate the worker timing out the handler (the real worker races
    // the handler against timeout_ms and reports TIMED_OUT).
    const [s] = await claimAndStart(workflowRunId);

    const attempt = (
      await pool.query<{ timeout_at: string | null }>(
        'SELECT timeout_at FROM task_attempts WHERE task_attempt_id = $1',
        [s.attemptId],
      )
    ).rows[0];
    expect(attempt.timeout_at).not.toBeNull();

    const timedOut = await completeFailure(database(), {
      taskExecutionId: s.taskExecutionId,
      attemptId: s.attemptId,
      fencingToken: s.fencingToken,
      workerId: 'worker-slow',
      failure: {
        code: 'TIMEOUT',
        message: 'handler exceeded timeout',
        retryable: true,
        timedOut: true,
      },
      attemptNumber: s.attemptNumber,
      maxAttempts: 2,
      retryDelayMs: 0,
      failurePolicy: 'FAIL_WORKFLOW',
    });
    expect(timedOut.applied).toBe(true);
    expect(timedOut.taskState).toBe('RETRY_WAIT');

    const attemptState = (
      await pool.query<{ state: string }>(
        'SELECT state FROM task_attempts WHERE task_attempt_id = $1',
        [s.attemptId],
      )
    ).rows[0];
    expect(attemptState.state).toBe('TIMED_OUT');
  });

  it('startAttempt rejects stale dispatches (duplicate or superseded)', async () => {
    const { workflowRunId } = await setupRun(engine, [
      task('dup', [], { retryPolicy: { maxAttempts: 3 } }),
    ]);

    const [s] = await claimAndStart(workflowRunId);

    // The real start already happened (claimAndStart). A duplicate
    // delivery of the same dispatch is rejected (attempt no longer
    // DISPATCHED).
    const dup = await startAttempt(database(), {
      dispatchId: s.dispatchId,
      taskExecutionId: s.taskExecutionId,
      fencingToken: s.fencingToken,
      workerId: 'dup-worker',
    });
    expect(dup.ok).toBe(false);

    // A dispatch that never existed is not_found.
    const missing = await startAttempt(database(), {
      dispatchId: '00000000-0000-4000-8000-000000000000',
      taskExecutionId: s.taskExecutionId,
      fencingToken: s.fencingToken,
      workerId: 'dup-worker',
    });
    expect(missing.ok).toBe(false);
    expect(missing.reason).toBe('not_found');
  });

  it('a completion report after lease expiration is rejected', async () => {
    const { workflowRunId } = await setupRun(engine, [
      task('slowworker', [], { retryPolicy: { maxAttempts: 3 } }),
    ]);

    const [s] = await claimAndStart(workflowRunId);

    // The worker is too slow: the lease expires before it reports.
    await pool.query(
      `UPDATE task_executions
       SET lease_expires_at = now() - interval '1 second'
       WHERE task_execution_id = $1`,
      [s.taskExecutionId],
    );

    const rejected = await completeSuccess(database(), {
      taskExecutionId: s.taskExecutionId,
      attemptId: s.attemptId,
      fencingToken: s.fencingToken,
      workerId: 'slow-worker',
      output: { too: 'late' },
    });
    expect(rejected.applied).toBe(false);
  });

  it('task.started uses the run correlation id, with the dispatch id as causation', async () => {
    const { workflowRunId } = await setupRun(engine, [
      task('corr', [], { retryPolicy: { maxAttempts: 1 } }),
    ]);

    const [s] = await claimAndStart(workflowRunId);

    const run = (
      await pool.query<{ correlation_id: string }>(
        'SELECT correlation_id FROM workflow_runs WHERE workflow_run_id = $1',
        [workflowRunId],
      )
    ).rows[0];

    const started = (
      await pool.query<{ correlation_id: string | null; causation_id: string | null }>(
        `
        SELECT correlation_id, causation_id
        FROM outbox_events
        WHERE event_type = 'task.started'
          AND workflow_run_id = $1
        `,
        [workflowRunId],
      )
    ).rows[0];

    expect(started).toBeDefined();
    // Correlation follows the RUN, not the dispatch.
    expect(started.correlation_id).toBe(run.correlation_id);
    expect(started.correlation_id).not.toBe(s.dispatchId);
    // The dispatch that caused the start is recorded as causation.
    expect(started.causation_id).toBe(s.dispatchId);
  });

  it('a completion whose attempt transition does not match aborts without committing the task change', async () => {
    // Scenario 1: completeSuccess.
    const run1 = await setupRun(engine, [
      task('inconsistent-success', [], { retryPolicy: { maxAttempts: 3 } }),
    ]);
    const [s1] = await claimAndStart(run1.workflowRunId);

    // Simulate a concurrent re-claim that already abandoned the attempt.
    // (Impossible under the normal locks; this exercises the invariant.)
    await pool.query(
      `UPDATE task_attempts
       SET state = 'ABANDONED', finished_at = now()
       WHERE task_attempt_id = $1`,
      [s1.attemptId],
    );

    await expect(
      completeSuccess(database(), {
        taskExecutionId: s1.taskExecutionId,
        attemptId: s1.attemptId,
        fencingToken: s1.fencingToken,
        workerId: 'inconsistent-worker',
        output: { should: 'not commit' },
      }),
    ).rejects.toThrow(/affected 0 rows/);

    // The task update rolled back with the transaction.
    const t1 = (
      await pool.query<{ state: string }>(
        'SELECT state FROM task_executions WHERE task_execution_id = $1',
        [s1.taskExecutionId],
      )
    ).rows[0];
    expect(t1.state).toBe('RUNNING');

    // No task.succeeded event was committed.
    const ev1 = (
      await pool.query<{ n: number }>(
        `SELECT count(*)::int AS n FROM outbox_events
         WHERE event_type = 'task.succeeded' AND workflow_run_id = $1`,
        [run1.workflowRunId],
      )
    ).rows[0];
    expect(ev1.n).toBe(0);

    // Scenario 2: completeFailure (retryable path).
    const run2 = await setupRun(engine, [
      task('inconsistent-failure', [], { retryPolicy: { maxAttempts: 3 } }),
    ]);
    const [s2] = await claimAndStart(run2.workflowRunId);

    await pool.query(
      `UPDATE task_attempts
       SET state = 'ABANDONED', finished_at = now()
       WHERE task_attempt_id = $1`,
      [s2.attemptId],
    );

    await expect(
      completeFailure(database(), {
        taskExecutionId: s2.taskExecutionId,
        attemptId: s2.attemptId,
        fencingToken: s2.fencingToken,
        workerId: 'inconsistent-worker',
        failure: { code: 'HANDLER_FAILED', message: 'boom', retryable: true },
        attemptNumber: s2.attemptNumber,
        maxAttempts: 3,
        retryDelayMs: 100,
        failurePolicy: FailurePolicy.FAIL_WORKFLOW,
      }),
    ).rejects.toThrow(/affected 0 rows/);

    const t2 = (
      await pool.query<{ state: string }>(
        'SELECT state FROM task_executions WHERE task_execution_id = $1',
        [s2.taskExecutionId],
      )
    ).rows[0];
    expect(t2.state).toBe('RUNNING');

    const ev2 = (
      await pool.query<{ n: number }>(
        `SELECT count(*)::int AS n FROM outbox_events
         WHERE event_type = 'task.retry_scheduled' AND workflow_run_id = $1`,
        [run2.workflowRunId],
      )
    ).rows[0];
    expect(ev2.n).toBe(0);
  });
});
