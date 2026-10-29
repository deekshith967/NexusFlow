/**
 * Integration tests (REAL PostgreSQL via embedded-postgres) covering
 * contract section G required tests 1-9 and Phase A/B/C behavior:
 * identity creation, unique-index conflict handling, draft versions,
 * DAG validation, publication, idempotent run start, task initialization.
 */
import { Pool } from 'pg';

import {
  WorkflowNameConflictError,
  WorkflowNotFoundError,
  WorkflowVersionNotFoundError,
} from '../../src/common/errors';
import {
  InvalidWorkflowVersionTransitionError,
  PublishedWorkflowVersionImmutableError,
  WorkflowValidationError,
} from '../../src/workflow/domain/workflow-errors';
import { RunLaunchRejectedError } from '../../src/common/errors';
import type { TaskDefinition } from '../../src/workflow/domain/workflow-definition';
import { createRunService, createWorkflowService, definition, task } from '../helpers/factory';
import { startTestDb, TestDb } from '../helpers/test-db';

describe('workflow lifecycle (real PostgreSQL)', () => {
  let db: TestDb;
  let pool: Pool;
  const tenantId = '11111111-1111-4111-8111-111111111111';

  beforeAll(async () => {
    db = await startTestDb();
    pool = db.pool;
  }, 120_000);

  afterAll(async () => {
    await db.stop();
  });

  async function newWorkflow(name = `wf-${Math.random().toString(36).slice(2)}`) {
    return createWorkflowService(pool).createWorkflow({
      tenantId,
      name,
      description: 'integration test workflow',
    });
  }

  // -------------------------------------------------------------------------
  // Phase A — workflow identity
  // -------------------------------------------------------------------------

  describe('workflow identity', () => {
    it('creates a workflow identity with a generated ID and status active', async () => {
      const workflow = await newWorkflow('identity-workflow');

      expect(workflow.workflow_id).toMatch(
        /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/,
      );
      expect(workflow.status).toBe('active');
      expect(workflow.name).toBe('identity-workflow');
      expect(workflow.tenant_id).toBe(tenantId);
    });

    it('rejects blank names without altering stored spelling or casing', async () => {
      const service = createWorkflowService(pool);

      await expect(
        service.createWorkflow({ tenantId, name: '   ' }),
      ).rejects.toThrow(WorkflowValidationError);

      await expect(
        service.createWorkflow({ tenantId, name: '' }),
      ).rejects.toThrow(WorkflowValidationError);

      // Names keep their exact spelling and casing.
      const workflow = await newWorkflow('MixedCase  Name');
      const fetched = await service.getWorkflow(tenantId, workflow.workflow_id);
      expect(fetched.name).toBe('MixedCase  Name');
    });

    it('maps the tenant/name unique violation to a domain conflict (409-mappable)', async () => {
      const service = createWorkflowService(pool);
      const first = await newWorkflow('conflict-workflow');

      await expect(
        service.createWorkflow({ tenantId, name: 'conflict-workflow' }),
      ).rejects.toBeInstanceOf(WorkflowNameConflictError);

      // Case differs -> different name, no conflict.
      const variant = await service.createWorkflow({
        tenantId,
        name: 'Conflict-Workflow',
      });
      expect(variant.workflow_id).not.toBe(first.workflow_id);
    });

    it('resolves concurrent same-name creates to exactly one winner', async () => {
      const service = createWorkflowService(pool);
      const name = `concurrent-${Date.now()}`;

      const results = await Promise.allSettled(
        Array.from({ length: 8 }, () =>
          service.createWorkflow({ tenantId, name }),
        ),
      );

      const wins = results.filter((r) => r.status === 'fulfilled');
      const conflicts = results.filter((r) => r.status === 'rejected');

      expect(wins).toHaveLength(1);
      expect(conflicts).toHaveLength(7);

      for (const conflict of conflicts) {
        if (conflict.status === 'rejected') {
          expect(conflict.reason).toBeInstanceOf(WorkflowNameConflictError);
        }
      }

      const rows = await pool.query<{ workflow_id: string }>(
        'SELECT workflow_id FROM workflows WHERE tenant_id = $1 AND name = $2',
        [tenantId, name],
      );
      expect(rows.rows).toHaveLength(1);
      expect(wins[0].status).toBe('fulfilled');
      if (wins[0].status === 'fulfilled') {
        expect(rows.rows[0].workflow_id).toBe(wins[0].value.workflow_id);
      }
    });
  });

  // -------------------------------------------------------------------------
  // Phase A/B — draft versions, DAG validation, publication
  // -------------------------------------------------------------------------

  describe('draft versions and publication', () => {
    it('persists a draft version with tasks and dependencies (G-2 setup)', async () => {
      const workflow = await newWorkflow('draft-graph');
      const service = createWorkflowService(pool);

      const version = await service.createDraftVersion({
        tenantId,
        workflowId: workflow.workflow_id,
        definition: definition([
          task('fetch'),
          task('transform', { dependencies: ['fetch'] }),
          task('persist', { dependencies: ['transform'] }),
        ]),
      });

      expect(version.state).toBe('DRAFT');
      expect(version.version_number).toBe(1);

      const loaded = await service.getVersion(tenantId, version.workflow_version_id);
      expect(loaded.tasks).toHaveLength(3);
      expect(loaded.dependencies).toHaveLength(2);
    });

    it('rejects a task definition with a self-dependency (G-1)', async () => {
      const workflow = await newWorkflow('self-dep');
      const service = createWorkflowService(pool);

      await expect(
        service.createDraftVersion({
          tenantId,
          workflowId: workflow.workflow_id,
          definition: definition([task('a', { dependencies: ['a'] })]),
        }),
      ).rejects.toThrow(WorkflowValidationError);
    });

    it('rejects a workflow version containing a dependency cycle (G-2)', async () => {
      const workflow = await newWorkflow('cycle');
      const service = createWorkflowService(pool);

      await expect(
        service.createDraftVersion({
          tenantId,
          workflowId: workflow.workflow_id,
          definition: definition([
            task('a', { dependencies: ['b'] }),
            task('b', { dependencies: ['c'] }),
            task('c', { dependencies: ['a'] }),
          ]),
        }),
      ).rejects.toThrow(WorkflowValidationError);
    });

    it('rejects a dependency referencing a task from another workflow version (G-3)', async () => {
      // The composite FK (task_definition_id, workflow_version_id) makes
      // cross-version edges impossible at the database level.
      const workflow = await newWorkflow('cross-version');
      const service = createWorkflowService(pool);

      const v1 = await service.createDraftVersion({
        tenantId,
        workflowId: workflow.workflow_id,
        definition: definition([task('a')]),
      });
      const v2 = await service.createDraftVersion({
        tenantId,
        workflowId: workflow.workflow_id,
        definition: definition([task('b')]),
      });

      const taskA = (
        await pool.query<{ task_definition_id: string }>(
          'SELECT task_definition_id FROM task_definitions WHERE workflow_version_id = $1',
          [v1.workflow_version_id],
        )
      ).rows[0];
      const taskB = (
        await pool.query<{ task_definition_id: string }>(
          'SELECT task_definition_id FROM task_definitions WHERE workflow_version_id = $1',
          [v2.workflow_version_id],
        )
      ).rows[0];

      await expect(
        pool.query(
          `INSERT INTO task_dependencies
             (dependency_id, workflow_version_id,
              upstream_task_definition_id, downstream_task_definition_id, created_at)
           VALUES (gen_random_uuid(), $1, $2, $3, now())`,
          [v2.workflow_version_id, taskA.task_definition_id, taskB.task_definition_id],
        ),
      ).rejects.toMatchObject({ code: '23503' }); // FK violation
    });

    it('rejects publication when configuration is invalid (G-4)', async () => {
      const workflow = await newWorkflow('bad-config');
      const service = createWorkflowService(pool);

      // A valid draft is created, then its stored retry policy is
      // corrupted directly (simulating an invalid persisted definition).
      const version = await service.createDraftVersion({
        tenantId,
        workflowId: workflow.workflow_id,
        definition: definition([task('a')]),
      });

      await pool.query(
        `UPDATE task_definitions
         SET retry_policy = '{"maxAttempts": 0}'::jsonb
         WHERE workflow_version_id = $1`,
        [version.workflow_version_id],
      );

      await expect(
        service.publish(tenantId, version.workflow_version_id),
      ).rejects.toThrow(WorkflowValidationError);

      // The version must still be DRAFT (publication rolled back).
      const state = (
        await pool.query<{ state: string }>(
          'SELECT state FROM workflow_versions WHERE workflow_version_id = $1',
          [version.workflow_version_id],
        )
      ).rows[0];
      expect(state.state).toBe('DRAFT');

      // Corrupted retry delay (non-integer) is rejected at publication.
      const v2 = await service.createDraftVersion({
        tenantId,
        workflowId: workflow.workflow_id,
        definition: definition([task('b')]),
      });
      await pool.query(
        `UPDATE task_definitions
         SET retry_policy = '{"maxAttempts": 1, "retryDelayMs": "x"}'::jsonb
         WHERE workflow_version_id = $1`,
        [v2.workflow_version_id],
      );
      await expect(
        service.publish(tenantId, v2.workflow_version_id),
      ).rejects.toThrow(WorkflowValidationError);

      // IGNORE_FAILURE is not a valid Phase 1 policy: the database
      // constraint rejects it for persisted rows (and the validator
      // rejects it for in-memory definitions).
      const v3 = await service.createDraftVersion({
        tenantId,
        workflowId: workflow.workflow_id,
        definition: definition([task('c')]),
      });
      await expect(
        pool.query(
          `UPDATE task_definitions
           SET failure_policy = 'IGNORE_FAILURE'
           WHERE workflow_version_id = $1`,
          [v3.workflow_version_id],
        ),
      ).rejects.toMatchObject({
        constraint: 'task_definitions_failure_policy_check',
      });
    });

    it('rejects unknown handler references at draft time and at publication (G-4, D.2/D.3)', async () => {
      const workflow = await newWorkflow('bad-handler');
      const service = createWorkflowService(pool);

      await expect(
        service.createDraftVersion({
          tenantId,
          workflowId: workflow.workflow_id,
          definition: definition([task('a', [], { handlerType: 'no-such-handler' })]),
        }),
      ).rejects.toThrow(/Unknown handler 'no-such-handler:1'/);

      await expect(
        service.createDraftVersion({
          tenantId,
          workflowId: workflow.workflow_id,
          definition: definition([task('a', [], { handlerVersion: '99' })]),
        }),
      ).rejects.toThrow(WorkflowValidationError);

      // A stored draft whose handler reference is corrupted cannot publish.
      const version = await service.createDraftVersion({
        tenantId,
        workflowId: workflow.workflow_id,
        definition: definition([task('a')]),
      });
      await pool.query(
        `UPDATE task_definitions SET handler_type = 'ghost'
         WHERE workflow_version_id = $1`,
        [version.workflow_version_id],
      );

      const report = await service.getValidationReport(tenantId, version.workflow_version_id);
      expect(report.valid).toBe(false);
      expect(report.errors.join(' ')).toMatch(/Unknown handler 'ghost:1'/);

      await expect(
        service.publish(tenantId, version.workflow_version_id),
      ).rejects.toThrow(WorkflowValidationError);

      const state = (
        await pool.query<{ state: string }>(
          'SELECT state FROM workflow_versions WHERE workflow_version_id = $1',
          [version.workflow_version_id],
        )
      ).rows[0];
      expect(state.state).toBe('DRAFT');
    });

    it('publishes a valid draft atomically with its outbox event', async () => {
      const workflow = await newWorkflow('publish-atomic');
      const service = createWorkflowService(pool);

      const version = await service.createDraftVersion({
        tenantId,
        workflowId: workflow.workflow_id,
        definition: definition([task('a'), task('b', { dependencies: ['a'] })]),
      });

      const published = await service.publish(tenantId, version.workflow_version_id);

      expect(published.state).toBe('PUBLISHED');
      expect(published.published_at).not.toBeNull();
      expect(published.definition_hash).toMatch(/^[a-f0-9]{64}$/);

      const events = await pool.query(
        `SELECT event_type FROM outbox_events
         WHERE workflow_version_id = $1 AND event_type = 'workflow.version.published'`,
        [version.workflow_version_id],
      );
      expect(events.rows).toHaveLength(1);
    });

    it('does not publish twice and rejects invalid transitions (G-5, B.1)', async () => {
      const workflow = await newWorkflow('no-double-publish');
      const service = createWorkflowService(pool);

      const version = await service.createDraftVersion({
        tenantId,
        workflowId: workflow.workflow_id,
        definition: definition([task('a')]),
      });
      await service.publish(tenantId, version.workflow_version_id);

      await expect(
        service.publish(tenantId, version.workflow_version_id),
      ).rejects.toThrow(InvalidWorkflowVersionTransitionError);
    });

    it('published workflow versions cannot be mutated (G-5)', async () => {
      const workflow = await newWorkflow('immutable');
      const service = createWorkflowService(pool);

      const version = await service.createDraftVersion({
        tenantId,
        workflowId: workflow.workflow_id,
        definition: definition([task('a'), task('b', { dependencies: ['a'] })]),
      });
      await service.publish(tenantId, version.workflow_version_id);

      await expect(
        service.updateDraftDefinition({
          tenantId,
          workflowVersionId: version.workflow_version_id,
          definition: definition([task('only-a')]),
        }),
      ).rejects.toBeInstanceOf(PublishedWorkflowVersionImmutableError);

      // The stored graph must be unchanged.
      const loaded = await service.getVersion(tenantId, version.workflow_version_id);
      expect(loaded.tasks).toHaveLength(2);
      expect(loaded.dependencies).toHaveLength(1);
    });

    it('retired versions cannot start new runs (G-6)', async () => {
      const workflow = await newWorkflow('retired');
      const service = createWorkflowService(pool);
      const runs = createRunService(pool);

      const version = await service.createDraftVersion({
        tenantId,
        workflowId: workflow.workflow_id,
        definition: definition([task('a')]),
      });
      await service.publish(tenantId, version.workflow_version_id);
      await service.retire(tenantId, version.workflow_version_id);

      await expect(
        runs.startRun({
          tenantId,
          workflowId: workflow.workflow_id,
          workflowVersionId: version.workflow_version_id,
        }),
      ).rejects.toBeInstanceOf(RunLaunchRejectedError);

      // Without an explicit version: no PUBLISHED version remains.
      await expect(
        runs.startRun({
          tenantId,
          workflowId: workflow.workflow_id,
        }),
      ).rejects.toBeInstanceOf(RunLaunchRejectedError);
    });

    it('rejects version operations from another tenant (service-level isolation)', async () => {
      const workflow = await newWorkflow('cross-tenant');
      const service = createWorkflowService(pool);
      const otherTenant = '22222222-2222-4222-8222-222222222222';

      const version = await service.createDraftVersion({
        tenantId,
        workflowId: workflow.workflow_id,
        definition: definition([task('a')]),
      });

      await expect(
        service.getVersion(otherTenant, version.workflow_version_id),
      ).rejects.toBeInstanceOf(WorkflowVersionNotFoundError);
      await expect(
        service.publish(otherTenant, version.workflow_version_id),
      ).rejects.toBeInstanceOf(WorkflowVersionNotFoundError);
      await expect(
        service.retire(otherTenant, version.workflow_version_id),
      ).rejects.toBeInstanceOf(WorkflowVersionNotFoundError);
      await expect(
        service.updateDraftDefinition({
          tenantId: otherTenant,
          workflowVersionId: version.workflow_version_id,
          definition: definition([task('b')]),
        }),
      ).rejects.toBeInstanceOf(WorkflowVersionNotFoundError);
      await expect(
        service.listVersions(otherTenant, workflow.workflow_id),
      ).rejects.toBeInstanceOf(WorkflowNotFoundError);

      // Owner access is unaffected: the version is still a draft.
      const loaded = await service.getVersion(
        tenantId,
        version.workflow_version_id,
      );
      expect(loaded.version.state).toBe('DRAFT');
    });

    it('discarded drafts cannot be published (B.1)', async () => {
      const workflow = await newWorkflow('discard');
      const service = createWorkflowService(pool);

      const version = await service.createDraftVersion({
        tenantId,
        workflowId: workflow.workflow_id,
        definition: definition([task('a')]),
      });
      await service.discard(tenantId, version.workflow_version_id);

      await expect(
        service.publish(tenantId, version.workflow_version_id),
      ).rejects.toThrow(InvalidWorkflowVersionTransitionError);
    });
  });

  // -------------------------------------------------------------------------
  // Phase C — run creation
  // -------------------------------------------------------------------------

  describe('run creation (D.5)', () => {
    async function publishedWorkflow(
      tasksArg: TaskDefinition[] = [],
    ) {
      const workflow = await newWorkflow(`run-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`);
      const service = createWorkflowService(pool);
      const version = await service.createDraftVersion({
        tenantId,
        workflowId: workflow.workflow_id,
        definition: definition(
          tasksArg.length
            ? tasksArg
            : [task('a'), task('b', { dependencies: ['a'] }), task('c', { dependencies: ['b'] })],
        ),
      });
      await service.publish(tenantId, version.workflow_version_id);

      return { workflow, version };
    }

    it('creates exactly one task execution per task definition (G-8)', async () => {
      const { workflow, version } = await publishedWorkflow([
        task('a'),
        task('b', { dependencies: ['a'] }),
        task('c'),
      ]);
      const runs = createRunService(pool);

      const result = await runs.startRun({
        tenantId,
        workflowId: workflow.workflow_id,
        workflowVersionId: version.workflow_version_id,
        input: { orderId: 42 },
      });

      expect(result.created).toBe(true);
      expect(result.run.state).toBe('RUNNING');
      expect(result.taskExecutions).toHaveLength(3);

      const count = await pool.query<{ n: number }>(
        'SELECT count(*)::int AS n FROM task_executions WHERE workflow_run_id = $1',
        [result.run.workflow_run_id],
      );
      expect(count.rows[0].n).toBe(3);
    });

    it('initializes root tasks as READY and non-root tasks as WAITING (G-9)', async () => {
      const { workflow, version } = await publishedWorkflow([
        task('root-1'),
        task('root-2'),
        task('mid', { dependencies: ['root-1', 'root-2'] }),
        task('leaf', { dependencies: ['mid'] }),
      ]);
      const runs = createRunService(pool);

      const result = await runs.startRun({
        tenantId,
        workflowId: workflow.workflow_id,
        workflowVersionId: version.workflow_version_id,
      });

      const byKey = new Map(
        result.taskExecutions.map((t) => [t.task_key, t.state]),
      );

      expect(byKey.get('root-1')).toBe('READY');
      expect(byKey.get('root-2')).toBe('READY');
      expect(byKey.get('mid')).toBe('WAITING');
      expect(byKey.get('leaf')).toBe('WAITING');

      // The start transaction also wrote the started event and root
      // task.ready events atomically.
      const events = await pool.query(
        `SELECT event_type FROM outbox_events
         WHERE workflow_run_id = $1
         ORDER BY created_at ASC, outbox_event_id ASC`,
        [result.run.workflow_run_id],
      );
      const types = events.rows.map((r: { event_type: string }) => r.event_type);
      expect(types).toContain('workflow.run.started');
      expect(types.filter((t: string) => t === 'task.ready')).toHaveLength(2);
    });

    it('workflow-start idempotency returns the original run (G-7)', async () => {
      const { workflow, version } = await publishedWorkflow();
      const runs = createRunService(pool);
      const key = 'idem-key-1';

      const first = await runs.startRun({
        tenantId,
        workflowId: workflow.workflow_id,
        workflowVersionId: version.workflow_version_id,
        idempotencyKey: key,
      });
      const second = await runs.startRun({
        tenantId,
        workflowId: workflow.workflow_id,
        workflowVersionId: version.workflow_version_id,
        idempotencyKey: key,
      });

      expect(second.created).toBe(false);
      expect(second.run.workflow_run_id).toBe(first.run.workflow_run_id);

      const count = await pool.query<{ n: number }>(
        `SELECT count(*)::int AS n FROM workflow_runs
         WHERE tenant_id = $1 AND workflow_id = $2 AND idempotency_key = $3`,
        [tenantId, workflow.workflow_id, key],
      );
      expect(count.rows[0].n).toBe(1);

      // Concurrent idempotent starts with the same key create one run.
      const [c1, c2, c3] = await Promise.all(
        [0, 1, 2].map(() =>
          runs.startRun({
            tenantId,
            workflowId: workflow.workflow_id,
            workflowVersionId: version.workflow_version_id,
            idempotencyKey: `idem-race-${Date.now()}`,
          }),
        ),
      );
      const ids = new Set([c1, c2, c3].map((r) => r.run.workflow_run_id));
      expect(ids.size).toBe(1);
    });

    it('rejects invalid launches and leaves no partial run', async () => {
      const workflow = await newWorkflow('no-version');
      const runs = createRunService(pool);

      // No published version.
      await expect(
        runs.startRun({ tenantId, workflowId: workflow.workflow_id }),
      ).rejects.toBeInstanceOf(RunLaunchRejectedError);

      const runsCount = (
        await pool.query<{ n: number }>(
          'SELECT count(*)::int AS n FROM workflow_runs WHERE workflow_id = $1',
          [workflow.workflow_id],
        )
      ).rows[0];
      expect(runsCount.n).toBe(0);

      // Unknown version id.
      await expect(
        runs.startRun({
          tenantId,
          workflowId: workflow.workflow_id,
          workflowVersionId: '99999999-9999-4999-8999-999999999999',
        }),
      ).rejects.toThrow();

      // Cross-tenant access is indistinguishable from a missing resource
      // (ADR 001): pinned version -> version not found, implicit latest
      // version -> workflow not found.
      const other = '22222222-2222-4222-8222-222222222222';
      const { workflow: wf2, version: v2 } = await publishedWorkflow();
      await expect(
        runs.startRun({
          tenantId: other,
          workflowId: wf2.workflow_id,
          workflowVersionId: v2.workflow_version_id,
        }),
      ).rejects.toBeInstanceOf(WorkflowVersionNotFoundError);
      await expect(
        runs.startRun({ tenantId: other, workflowId: wf2.workflow_id }),
      ).rejects.toBeInstanceOf(WorkflowNotFoundError);

      // A run started by this tenant is invisible to another tenant.
      const started = await runs.startRun({
        tenantId,
        workflowId: wf2.workflow_id,
        workflowVersionId: v2.workflow_version_id,
      });
      await expect(
        runs.getRunDetail(other, started.run.workflow_run_id),
      ).rejects.toThrow();
    });

    it('stores input payload and version reference for later scheduling', async () => {
      const { workflow, version } = await publishedWorkflow();
      const runs = createRunService(pool);

      const result = await runs.startRun({
        tenantId,
        workflowId: workflow.workflow_id,
        workflowVersionId: version.workflow_version_id,
        input: { a: 1, b: 'two' },
      });

      const detail = await runs.getRunDetail(tenantId, result.run.workflow_run_id);
      expect(detail.run.input_payload).toEqual({ a: 1, b: 'two' });
      expect(detail.run.workflow_version_id).toBe(version.workflow_version_id);
      expect(detail.taskExecutions.every((t) => t.state === 'WAITING' || t.state === 'READY')).toBe(true);
    });
  });
});
