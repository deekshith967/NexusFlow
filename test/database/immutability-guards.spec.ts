/**
 * Database-level immutability guards (migration 003). Each guard is
 * exercised with direct SQL (the application never issues these
 * statements; the guards exist to catch defects and manual edits).
 */
import type { Pool } from 'pg';

import { baseConfig, createEngine, Engine, setupRun, task } from '../helpers/engine';
import { startTestDb, TestDb } from '../helpers/test-db';

describe('immutability guards (real PostgreSQL, migration 003)', () => {
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

  const immutable = { code: 'NF001' };

  async function succeededRun() {
    const setup = await setupRun(engine, [task('a'), task('b', ['a'])]);
    expect(await engine.drainRun(setup.workflowRunId)).toBe('SUCCEEDED');
    return setup;
  }

  it('published versions and their task graph cannot be modified', async () => {
    const { workflowVersionId, tenantId } = await succeededRun();

    await expect(
      pool.query(
        `UPDATE workflow_versions SET state = 'DRAFT' WHERE workflow_version_id = $1`,
        [workflowVersionId],
      ),
    ).rejects.toMatchObject(immutable);
    await expect(
      pool.query(
        `UPDATE workflow_versions SET definition_hash = 'x' WHERE workflow_version_id = $1`,
        [workflowVersionId],
      ),
    ).rejects.toMatchObject(immutable);
    await expect(
      pool.query(
        `UPDATE task_definitions SET timeout_ms = 1 WHERE workflow_version_id = $1`,
        [workflowVersionId],
      ),
    ).rejects.toMatchObject(immutable);
    await expect(
      pool.query(`DELETE FROM task_dependencies WHERE workflow_version_id = $1`, [
        workflowVersionId,
      ]),
    ).rejects.toMatchObject(immutable);
    await expect(
      pool.query(
        `INSERT INTO task_definitions (
           task_definition_id, workflow_version_id, task_key, handler_type,
           handler_version, configuration, input_mapping, output_mapping,
           retry_policy, timeout_ms, failure_policy, created_at)
         VALUES (gen_random_uuid(), $1, 'smuggled', 'echo', '1', '{}', '{}', '{}',
                 '{"maxAttempts":1}', 1000, 'FAIL_WORKFLOW', now())`,
        [workflowVersionId],
      ),
    ).rejects.toMatchObject(immutable);
    await expect(
      pool.query(`DELETE FROM workflow_versions WHERE workflow_version_id = $1`, [
        workflowVersionId,
      ]),
    ).rejects.toMatchObject(immutable);

    // The one permitted change: PUBLISHED -> RETIRED, via the service.
    const retired = await engine.workflows.retire(tenantId, workflowVersionId);
    expect(retired.state).toBe('RETIRED');

    // RETIRED is terminal.
    await expect(
      pool.query(
        `UPDATE workflow_versions SET state = 'PUBLISHED', retired_at = NULL
         WHERE workflow_version_id = $1`,
        [workflowVersionId],
      ),
    ).rejects.toMatchObject(immutable);
  });

  it('rejects unknown version states', async () => {
    const tenantId = '11111111-1111-4111-8111-111111111111';
    const workflow = await engine.workflows.createWorkflow({
      tenantId,
      name: `state-check-${Date.now()}`,
    });
    const draft = await engine.workflows.createDraftVersion({
      tenantId,
      workflowId: workflow.workflow_id,
      definition: { name: 'state-check', tasks: [task('s')] },
    });
    const workflowVersionId = draft.workflow_version_id;

    // UPDATE: the lifecycle trigger rejects the invalid transition.
    await expect(
      pool.query(
        `UPDATE workflow_versions SET state = 'BOGUS' WHERE workflow_version_id = $1`,
        [workflowVersionId],
      ),
    ).rejects.toMatchObject(immutable);

    // INSERT: the state CHECK constraint rejects unknown states.
    await expect(
      pool.query(
        `INSERT INTO workflow_versions (
           workflow_version_id, workflow_id, tenant_id, version_number, state, created_at)
         VALUES (gen_random_uuid(), $1, $2, 999, 'BOGUS', now())`,
        [workflow.workflow_id, tenantId],
      ),
    ).rejects.toMatchObject({ constraint: 'workflow_versions_state_check' });
  });

  it('terminal runs, task executions, attempts, and published events are immutable', async () => {
    const { workflowRunId } = await succeededRun();

    await expect(
      pool.query(
        `UPDATE workflow_runs SET state = 'RUNNING', completed_at = NULL
         WHERE workflow_run_id = $1`,
        [workflowRunId],
      ),
    ).rejects.toMatchObject(immutable);

    await expect(
      pool.query(
        `UPDATE task_executions SET state = 'READY', completed_at = NULL
         WHERE workflow_run_id = $1 AND task_key = 'a'`,
        [workflowRunId],
      ),
    ).rejects.toMatchObject(immutable);

    await expect(
      pool.query(
        `UPDATE task_attempts SET state = 'STARTED'
         WHERE task_execution_id IN (
           SELECT task_execution_id FROM task_executions WHERE workflow_run_id = $1)`,
        [workflowRunId],
      ),
    ).rejects.toMatchObject(immutable);

    await expect(
      pool.query(
        `UPDATE outbox_events SET published_at = NULL
         WHERE workflow_run_id = $1 AND published_at IS NOT NULL`,
        [workflowRunId],
      ),
    ).rejects.toMatchObject(immutable);

    // Nothing changed.
    const run = await pool.query<{ state: string }>(
      'SELECT state FROM workflow_runs WHERE workflow_run_id = $1',
      [workflowRunId],
    );
    expect(run.rows[0].state).toBe('SUCCEEDED');
  });

  it('a nonterminal task cannot have its fencing token moved backwards or its run reassigned', async () => {
    const { workflowRunId } = await setupRun(engine, [task('t')]);
    await engine.scheduler.scan(); // claim: fencing token 1

    const row = (
      await pool.query<{ task_execution_id: string; fencing_token: number }>(
        'SELECT task_execution_id, fencing_token FROM task_executions WHERE workflow_run_id = $1',
        [workflowRunId],
      )
    ).rows[0];
    expect(row.fencing_token).toBe(1);

    await expect(
      pool.query(
        'UPDATE task_executions SET fencing_token = 0 WHERE task_execution_id = $1',
        [row.task_execution_id],
      ),
    ).rejects.toMatchObject(immutable);

    const other = await setupRun(engine, [task('u')]);
    await expect(
      pool.query(
        'UPDATE task_executions SET workflow_run_id = $2 WHERE task_execution_id = $1',
        [row.task_execution_id, other.workflowRunId],
      ),
    ).rejects.toMatchObject(immutable);

    // Normal recovery still works under the guards: drain to success.
    expect(await engine.drainRun(workflowRunId)).toBe('SUCCEEDED');
  });
});
