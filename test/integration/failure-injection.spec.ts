/**
 * Failure-injection and race coverage (contract E, G "Required
 * failure-injection coverage", H "Verification").
 *
 * PostgreSQL loss is injected for real: the transaction's backend is
 * terminated with pg_terminate_backend() immediately before COMMIT, so
 * the server never commits and the client sees a connection error. Each
 * test then verifies that no partial state or outbox row survived, and
 * that a retry on a healthy connection completes the transition once.
 */
import { Pool } from 'pg';
import type { PoolClient } from 'pg';

import { DatabaseService } from '../../src/database/database.service';
import { claimBatch } from '../../src/scheduler/claim';
import { RunService } from '../../src/workflow/application/run.service';
import { completeFailure, completeSuccess, startAttempt } from '../../src/worker/worker-ops';
import { FailurePolicy } from '../../src/workflow/domain/failure-policy';
import { baseConfig, createEngine, Engine, setupRun, task } from '../helpers/engine';
import { definition } from '../helpers/factory';
import { startTestDb, TestDb } from '../helpers/test-db';

/**
 * DatabaseService whose next transaction loses its PostgreSQL backend
 * after the operation ran but before COMMIT.
 */
class CrashBeforeCommitDatabase extends DatabaseService {
  constructor(
    pool: Pool,
    private readonly admin: Pool,
  ) {
    super(pool);
  }

  override transaction<T>(operation: (client: PoolClient) => Promise<T>): Promise<T> {
    return super.transaction(async (client) => {
      // A terminated backend surfaces as a client 'error' event too.
      client.on('error', () => undefined);
      const pid = (await client.query<{ pid: number }>('SELECT pg_backend_pid() AS pid')).rows[0].pid;
      const result = await operation(client);
      await this.admin.query('SELECT pg_terminate_backend($1)', [pid]);
      return result;
    });
  }
}

describe('failure injection and races (real PostgreSQL)', () => {
  let db: TestDb;
  let pool: Pool;
  let faultyPool: Pool;
  let engine: Engine;

  beforeAll(async () => {
    db = await startTestDb();
    pool = db.pool;
    faultyPool = new Pool({ connectionString: db.connectionString, max: 4 });
    faultyPool.on('error', () => undefined);
    engine = await createEngine(pool, baseConfig({ leaseMs: 30_000 }));
  }, 120_000);

  afterAll(async () => {
    await engine.stop();
    await faultyPool.end();
    await db.stop();
  });

  const healthy = () => new DatabaseService(pool);
  const crashing = () => new CrashBeforeCommitDatabase(faultyPool, pool);

  async function count(sql: string, params: unknown[]): Promise<number> {
    return (await pool.query<{ n: number }>(sql, params)).rows[0].n;
  }

  async function claimAndStartAll(workflowRunId: string) {
    const claimed = await healthy().transaction((client) =>
      claimBatch(client, {
        batchSize: 50,
        leaseOwner: 'sched-fi',
        schedulerId: 'sched-fi',
        leaseMs: 30_000,
      }),
    );
    const mine = claimed.filter((c) => c.workflowRunId === workflowRunId);

    for (const c of mine) {
      const started = await startAttempt(healthy(), {
        dispatchId: c.dispatchId,
        taskExecutionId: c.taskExecutionId,
        fencingToken: c.fencingToken,
        workerId: 'worker-fi',
      });
      expect(started.ok).toBe(true);
    }

    return new Map(mine.map((c) => [c.taskKey, c]));
  }

  it('PostgreSQL loss before the scheduler claim commits leaves no claim, attempt, lease, or outbox row', async () => {
    const { workflowRunId } = await setupRun(engine, [task('root')]);

    await expect(
      crashing().transaction((client) =>
        claimBatch(client, { batchSize: 50, leaseOwner: 'doomed', schedulerId: 'doomed', leaseMs: 30_000 }),
      ),
    ).rejects.toThrow(/terminat/i);

    const row = (
      await pool.query<{ state: string; fencing_token: number; lease_owner: string | null; attempt_count: number }>(
        'SELECT state, fencing_token, lease_owner, attempt_count FROM task_executions WHERE workflow_run_id = $1',
        [workflowRunId],
      )
    ).rows[0];
    expect(row).toEqual({ state: 'READY', fencing_token: 0, lease_owner: null, attempt_count: 0 });
    expect(
      await count(
        `SELECT count(*)::int AS n FROM task_attempts a JOIN task_executions te USING (task_execution_id)
         WHERE te.workflow_run_id = $1`,
        [workflowRunId],
      ),
    ).toBe(0);
    expect(
      await count(
        `SELECT count(*)::int AS n FROM outbox_events
         WHERE workflow_run_id = $1 AND event_type IN ('task.claimed', 'task.dispatch.requested')`,
        [workflowRunId],
      ),
    ).toBe(0);

    // Another scheduler claims it normally with the first fencing token.
    const claimed = await claimAndStartAll(workflowRunId);
    expect(claimed.get('root')?.fencingToken).toBe(1);
    expect(claimed.get('root')?.attemptNumber).toBe(1);
  });

  it('PostgreSQL loss before a success report commits changes nothing; the retried report applies exactly once', async () => {
    const { workflowRunId } = await setupRun(engine, [task('up'), task('down', ['up'])]);
    const up = (await claimAndStartAll(workflowRunId)).get('up')!;

    const report = {
      taskExecutionId: up.taskExecutionId,
      attemptId: up.attemptId,
      fencingToken: up.fencingToken,
      workerId: 'worker-fi',
      output: { ok: true },
    };

    await expect(completeSuccess(crashing(), report)).rejects.toThrow(/terminat/i);

    const states = async () =>
      Object.fromEntries(
        (
          await pool.query<{ task_key: string; state: string }>(
            'SELECT task_key, state FROM task_executions WHERE workflow_run_id = $1',
            [workflowRunId],
          )
        ).rows.map((r) => [r.task_key, r.state]),
      );

    expect(await states()).toEqual({ up: 'RUNNING', down: 'WAITING' });
    expect(
      (await pool.query<{ state: string }>('SELECT state FROM task_attempts WHERE task_attempt_id = $1', [up.attemptId]))
        .rows[0].state,
    ).toBe('STARTED');
    expect(
      await count(
        `SELECT count(*)::int AS n FROM outbox_events
         WHERE workflow_run_id = $1 AND event_type IN ('task.succeeded', 'task.ready')
           AND task_execution_id IS DISTINCT FROM NULL
           AND payload->>'reason' IS DISTINCT FROM 'root'`,
        [workflowRunId],
      ),
    ).toBe(0);

    // The worker retries after the outage: applied once, downstream activated once.
    expect((await completeSuccess(healthy(), report)).applied).toBe(true);
    // A duplicate retry is a no-op.
    expect((await completeSuccess(healthy(), report)).applied).toBe(false);

    expect(await states()).toEqual({ up: 'SUCCEEDED', down: 'READY' });
    expect(
      await count(
        `SELECT count(*)::int AS n FROM outbox_events WHERE workflow_run_id = $1 AND event_type = 'task.succeeded'`,
        [workflowRunId],
      ),
    ).toBe(1);
    expect(
      await count(
        `SELECT count(*)::int AS n FROM outbox_events
         WHERE workflow_run_id = $1 AND event_type = 'task.ready' AND payload->>'taskKey' = 'down'`,
        [workflowRunId],
      ),
    ).toBe(1);
  });

  it('PostgreSQL loss before a terminal failure commits leaves the run RUNNING and nothing BLOCKED', async () => {
    const { workflowRunId } = await setupRun(engine, [task('bad'), task('after', ['bad'])]);
    const bad = (await claimAndStartAll(workflowRunId)).get('bad')!;

    const report = {
      taskExecutionId: bad.taskExecutionId,
      attemptId: bad.attemptId,
      fencingToken: bad.fencingToken,
      workerId: 'worker-fi',
      failure: { code: 'BOOM', message: 'permanent', retryable: false },
      attemptNumber: bad.attemptNumber,
      maxAttempts: 1,
      retryDelayMs: 0,
      failurePolicy: FailurePolicy.FAIL_WORKFLOW,
    };

    await expect(completeFailure(crashing(), report)).rejects.toThrow(/terminat/i);

    const run = await pool.query<{ state: string }>('SELECT state FROM workflow_runs WHERE workflow_run_id = $1', [
      workflowRunId,
    ]);
    expect(run.rows[0].state).toBe('RUNNING');
    expect(
      await count(
        `SELECT count(*)::int AS n FROM task_executions WHERE workflow_run_id = $1 AND state IN ('FAILED', 'BLOCKED')`,
        [workflowRunId],
      ),
    ).toBe(0);
    expect(
      await count(
        `SELECT count(*)::int AS n FROM outbox_events
         WHERE workflow_run_id = $1 AND event_type IN ('task.failed', 'workflow.run.failed')`,
        [workflowRunId],
      ),
    ).toBe(0);

    const retried = await completeFailure(healthy(), report);
    expect(retried).toMatchObject({ applied: true, runState: 'FAILED', taskState: 'FAILED' });
    expect(
      await count(
        `SELECT count(*)::int AS n FROM outbox_events WHERE workflow_run_id = $1 AND event_type = 'workflow.run.failed'`,
        [workflowRunId],
      ),
    ).toBe(1);
  });

  it('PostgreSQL loss before run start commits leaves no run, task executions, or events; the idempotent retry creates one run', async () => {
    const tenantId = '11111111-1111-4111-8111-111111111111';
    const workflow = await engine.workflows.createWorkflow({ tenantId, name: `fi-start-${Date.now()}` });
    const version = await engine.workflows.createDraftVersion({
      tenantId,
      workflowId: workflow.workflow_id,
      definition: definition([task('x'), task('y', ['x'])]),
    });
    await engine.workflows.publish(tenantId, version.workflow_version_id);

    const start = {
      tenantId,
      workflowId: workflow.workflow_id,
      idempotencyKey: 'fi-start-key',
    };

    await expect(new RunService(crashing()).startRun(start)).rejects.toThrow(/terminat/i);

    expect(
      await count('SELECT count(*)::int AS n FROM workflow_runs WHERE workflow_id = $1', [workflow.workflow_id]),
    ).toBe(0);
    expect(
      await count(
        `SELECT count(*)::int AS n FROM outbox_events WHERE workflow_id = $1 AND event_type = 'workflow.run.started'`,
        [workflow.workflow_id],
      ),
    ).toBe(0);

    const runs = new RunService(healthy());
    const first = await runs.startRun(start);
    const replay = await runs.startRun(start);
    expect(first.created).toBe(true);
    expect(replay.created).toBe(false);
    expect(replay.run.workflow_run_id).toBe(first.run.workflow_run_id);
    expect(first.taskExecutions).toHaveLength(2);
    expect(
      await count('SELECT count(*)::int AS n FROM workflow_runs WHERE workflow_id = $1', [workflow.workflow_id]),
    ).toBe(1);
  });

  it('concurrent completion of every final task completes the run exactly once', async () => {
    const leaves = ['l1', 'l2', 'l3', 'l4', 'l5', 'l6'];
    const { workflowRunId } = await setupRun(engine, leaves.map((k) => task(k)));
    const claimed = await claimAndStartAll(workflowRunId);
    expect(claimed.size).toBe(leaves.length);

    const results = await Promise.all(
      [...claimed.values()].map((c) =>
        completeSuccess(healthy(), {
          taskExecutionId: c.taskExecutionId,
          attemptId: c.attemptId,
          fencingToken: c.fencingToken,
          workerId: `worker-${c.taskKey}`,
          output: { key: c.taskKey },
        }),
      ),
    );

    expect(results.every((r) => r.applied)).toBe(true);
    // Exactly one transaction observed the run as the last to finish.
    expect(results.filter((r) => r.runState === 'SUCCEEDED')).toHaveLength(1);
    expect(
      await count(
        `SELECT count(*)::int AS n FROM outbox_events WHERE workflow_run_id = $1 AND event_type = 'workflow.run.succeeded'`,
        [workflowRunId],
      ),
    ).toBe(1);
  });

  it('a concurrent sibling success and FAIL_WORKFLOW failure never yield SUCCEEDED', async () => {
    const { workflowRunId } = await setupRun(engine, [task('good'), task('bad')]);
    const claimed = await claimAndStartAll(workflowRunId);
    const good = claimed.get('good')!;
    const bad = claimed.get('bad')!;

    await Promise.all([
      completeSuccess(healthy(), {
        taskExecutionId: good.taskExecutionId,
        attemptId: good.attemptId,
        fencingToken: good.fencingToken,
        workerId: 'worker-good',
        output: {},
      }),
      completeFailure(healthy(), {
        taskExecutionId: bad.taskExecutionId,
        attemptId: bad.attemptId,
        fencingToken: bad.fencingToken,
        workerId: 'worker-bad',
        failure: { code: 'BOOM', message: 'permanent', retryable: false },
        attemptNumber: bad.attemptNumber,
        maxAttempts: 1,
        retryDelayMs: 0,
        failurePolicy: FailurePolicy.FAIL_WORKFLOW,
      }),
    ]);

    const run = await pool.query<{ state: string }>('SELECT state FROM workflow_runs WHERE workflow_run_id = $1', [
      workflowRunId,
    ]);
    expect(run.rows[0].state).toBe('FAILED');
    expect(
      await count(
        `SELECT count(*)::int AS n FROM outbox_events
         WHERE workflow_run_id = $1 AND event_type IN ('workflow.run.succeeded', 'workflow.run.failed')`,
        [workflowRunId],
      ),
    ).toBe(1);
  });

  it('duplicate concurrent success reports for one attempt apply exactly once', async () => {
    const { workflowRunId } = await setupRun(engine, [task('dup'), task('next', ['dup'])]);
    const dup = (await claimAndStartAll(workflowRunId)).get('dup')!;
    const report = {
      taskExecutionId: dup.taskExecutionId,
      attemptId: dup.attemptId,
      fencingToken: dup.fencingToken,
      workerId: 'worker-dup',
      output: { n: 1 },
    };

    const results = await Promise.all(Array.from({ length: 5 }, () => completeSuccess(healthy(), report)));
    expect(results.filter((r) => r.applied)).toHaveLength(1);
    expect(
      await count(
        `SELECT count(*)::int AS n FROM outbox_events WHERE workflow_run_id = $1 AND event_type = 'task.succeeded'`,
        [workflowRunId],
      ),
    ).toBe(1);
    expect(
      await count(
        `SELECT count(*)::int AS n FROM outbox_events
         WHERE workflow_run_id = $1 AND event_type = 'task.ready' AND payload->>'taskKey' = 'next'`,
        [workflowRunId],
      ),
    ).toBe(1);
  });
});
