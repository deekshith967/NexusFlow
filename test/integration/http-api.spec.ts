/**
 * Integration tests for the HTTP control plane (Phase F).
 *
 * REAL PostgreSQL (embedded-postgres) + in-memory message transport,
 * real HTTP via Nest's listener and Node's fetch (no test-only HTTP
 * dependency). Covers: health/readiness, workflow identity CRUD,
 * version draft/validation/publication/retirement, run start (including
 * idempotent replay), run/task/attempt inspection, tenant isolation, and
 * stable status codes without leaking database diagnostics.
 *
 * Note: JSON bodies use the persistence row shape (snake_case columns),
 * which keeps the API stable against schema naming.
 */
import { INestApplication } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import type { AddressInfo } from 'node:net';

import { AppModule } from '../../src/app.module';
import { ApiExceptionsFilter } from '../../src/workflow/api/exception-filter';
import { startTestDb, TestDb } from '../helpers/test-db';

const TENANT_A = 'aaaa1111-aaaa-4aaa-8aaa-aaaaaaaaaaaa';
const TENANT_B = 'bbbb2222-bbbb-4bbb-8bbb-bbbbbbbbbbbb';

// Test-only API secrets (>= 16 chars). Key A binds TENANT_A, key B binds
// TENANT_B; the api() helper derives the matching key per tenant.
const SECRET_A = 'test-secret-for-tenant-a1';
const SECRET_B = 'test-secret-for-tenant-b1';

interface TaskBody {
  taskKey: string;
  handlerType: string;
  handlerVersion: string;
  configuration?: Record<string, unknown>;
  inputMapping?: Record<string, unknown>;
  outputMapping?: Record<string, unknown>;
  retryPolicy?: { maxAttempts: number; retryDelayMs?: number };
  timeoutMs: number;
  failurePolicy: 'FAIL_WORKFLOW' | 'SKIP_DOWNSTREAM';
  dependencies?: string[];
}

const task = (
  taskKey: string,
  dependencies: string[] = [],
  overrides: Partial<TaskBody> = {},
): TaskBody => ({
  taskKey,
  handlerType: 'echo',
  handlerVersion: '1',
  configuration: { text: `hello ${taskKey}` },
  timeoutMs: 5000,
  failurePolicy: 'FAIL_WORKFLOW',
  dependencies,
  ...overrides,
});

describe('HTTP API (real PostgreSQL)', () => {
  let db: TestDb;
  let app: INestApplication;
  let baseUrl: string;

  const previousEnv: Record<string, string | undefined> = {};

  beforeAll(async () => {
    db = await startTestDb();

    for (const key of [
      'DATABASE_URL',
      'NEXUSFLOW_ROLE',
      'NEXUSFLOW_TRANSPORT',
      'NEXUSFLOW_DEFAULT_TENANT_ID',
      'NEXUSFLOW_API_KEYS',
    ]) {
      previousEnv[key] = process.env[key];
    }

    process.env.DATABASE_URL = db.connectionString;
    process.env.NEXUSFLOW_ROLE = 'api';
    process.env.NEXUSFLOW_TRANSPORT = 'memory';
    process.env.NEXUSFLOW_DEFAULT_TENANT_ID = TENANT_A;
    process.env.NEXUSFLOW_API_KEYS =
      `key-a:${SECRET_A}:${TENANT_A},key-b:${SECRET_B}:${TENANT_B}`;

    app = await NestFactory.create(AppModule);
    app.useGlobalFilters(new ApiExceptionsFilter());
    app.enableShutdownHooks();
    await app.listen(0, '127.0.0.1');

    const server = app.getHttpServer() as unknown as {
      address(): AddressInfo | null;
    };
    const address = server.address();
    expect(address?.port).toBeGreaterThan(0);
    baseUrl = `http://127.0.0.1:${address?.port}`;
  }, 120_000);

  afterAll(async () => {
    if (app) {
      await app.close();
    }

    if (db) {
      await db.stop();
    }

    for (const [key, value] of Object.entries(previousEnv)) {
      if (value === undefined) {
        delete process.env[key];
      } else {
        process.env[key] = value;
      }
    }
  });

  interface ApiResult {
    readonly status: number;
    readonly body: Record<string, unknown>;
  }

  async function api(
    method: string,
    path: string,
    body?: unknown,
    tenant?: string,
    // Secret to present as `Authorization: Bearer`. Defaults to the key
    // bound to `tenant` (TENANT_B -> SECRET_B, anything else -> SECRET_A);
    // pass null to omit credentials (401 tests) or an explicit secret to
    // simulate another caller (mismatch/isolation tests).
    secret?: string | null,
  ): Promise<ApiResult> {
    let resolvedSecret: string | null;

    if (secret === undefined) {
      resolvedSecret = tenant === TENANT_B ? SECRET_B : SECRET_A;
    } else {
      resolvedSecret = secret;
    }

    const response = await fetch(`${baseUrl}${path}`, {
      method,
      headers: {
        'content-type': 'application/json',
        ...(tenant ? { 'x-tenant-id': tenant } : {}),
        ...(resolvedSecret
          ? { authorization: `Bearer ${resolvedSecret}` }
          : {}),
      },
      body: body === undefined ? undefined : JSON.stringify(body),
    });

    const text = await response.text();

    let parsed: Record<string, unknown>;

    try {
      parsed = JSON.parse(text) as Record<string, unknown>;
    } catch {
      parsed = { raw: text };
    }

    return { status: response.status, body: parsed };
  }

  async function createPublishedWorkflow(
    tenant: string = TENANT_A,
    tasks?: TaskBody[],
  ) {
    const created = await api(
      'POST',
      '/workflows',
      { name: `api-${Math.random().toString(36).slice(2, 10)}` },
      tenant,
    );

    if (created.status !== 201) {
      throw new Error(`workflow creation failed: ${JSON.stringify(created)}`);
    }

    const workflowId = created.body.workflow_id as string;

    const version = await api(
      'POST',
      `/workflows/${workflowId}/versions`,
      { tasks: tasks ?? [task('only')] },
      tenant,
    );

    if (version.status !== 201) {
      throw new Error(`version creation failed: ${JSON.stringify(version)}`);
    }

    const versionId = version.body.workflow_version_id as string;
    const published = await api('POST', `/versions/${versionId}/publish`, {}, tenant);

    if (published.status !== 200) {
      throw new Error(`publish failed: ${JSON.stringify(published)}`);
    }

    return { workflowId, versionId };
  }

  // -------------------------------------------------------------------------
  // Health and readiness
  // -------------------------------------------------------------------------

  it('reports liveness and database readiness', async () => {
    const live = await api('GET', '/health');
    expect(live.status).toBe(200);
    expect(live.body.status).toBe('ok');
    expect(live.body.role).toBe('api');

    const ready = await api('GET', '/health/ready');
    expect(ready.status).toBe(200);
    expect(ready.body.status).toBe('ready');
    expect(ready.body.checks).toEqual(
      expect.objectContaining({ database: 'ok' }),
    );
  });

  // -------------------------------------------------------------------------
  // Workflow identity
  // -------------------------------------------------------------------------

  it('creates a workflow with a generated ID and rejects duplicates', async () => {
    const name = `dup-${Math.random().toString(36).slice(2, 10)}`;
    const first = await api('POST', '/workflows', { name }, TENANT_A);
    expect(first.status).toBe(201);
    expect(first.body.workflow_id).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/,
    );
    expect(first.body.name).toBe(name);
    expect(first.body.status).toBe('active');

    const duplicate = await api('POST', '/workflows', { name }, TENANT_A);
    expect(duplicate.status).toBe(409);
    expect(duplicate.body.error).toBe('Conflict');

    // The same name is allowed in another tenant.
    const otherTenant = await api('POST', '/workflows', { name }, TENANT_B);
    expect(otherTenant.status).toBe(201);
    expect(otherTenant.body.workflow_id).not.toBe(first.body.workflow_id);
  });

  it('rejects invalid workflow bodies and tenant headers', async () => {
    const missingName = await api('POST', '/workflows', {}, TENANT_A);
    expect(missingName.status).toBe(422);

    const badTenant = await api('POST', '/workflows', { name: 'x' }, 'nope');
    expect(badTenant.status).toBe(422);
    expect(badTenant.body.error).toBe('ValidationError');
  });

  it('lists and inspects workflows, isolating tenants', async () => {
    const created = await api(
      'POST',
      '/workflows',
      { name: `iso-${Math.random().toString(36).slice(2, 10)}` },
      TENANT_A,
    );
    const workflowId = created.body.workflow_id as string;

    const listed = await api('GET', '/workflows', undefined, TENANT_A);
    expect(listed.status).toBe(200);
    expect(Array.isArray(listed.body)).toBe(true);
    expect(
      (listed.body as unknown as unknown[]).some(
        (row) =>
          (row as { workflow_id: string }).workflow_id === workflowId,
      ),
    ).toBe(true);

    const fetched = await api('GET', `/workflows/${workflowId}`, undefined, TENANT_A);
    expect(fetched.status).toBe(200);

    const crossTenant = await api(
      'GET',
      `/workflows/${workflowId}`,
      undefined,
      TENANT_B,
    );
    expect(crossTenant.status).toBe(404);

    const missing = await api(
      'GET',
      '/workflows/00000000-0000-4000-8000-000000000000',
      undefined,
      TENANT_A,
    );
    expect(missing.status).toBe(404);
  });

  // -------------------------------------------------------------------------
  // Draft versions, validation, publication
  // -------------------------------------------------------------------------

  it('validates version bodies and rejects invalid DAGs at draft time', async () => {
    const { workflowId } = await createPublishedWorkflow(TENANT_A, [task('a')]);

    const missingHandler = await api(
      'POST',
      `/workflows/${workflowId}/versions`,
      {
        tasks: [
          { taskKey: 'x', timeoutMs: 1000, failurePolicy: 'FAIL_WORKFLOW' },
        ],
      },
      TENANT_A,
    );
    expect(missingHandler.status).toBe(422);

    const cycle = await api(
      'POST',
      `/workflows/${workflowId}/versions`,
      { tasks: [task('a', ['b']), task('b', ['a'])] },
      TENANT_A,
    );
    expect(cycle.status).toBe(422);

    const badPolicy = await api(
      'POST',
      `/workflows/${workflowId}/versions`,
      { tasks: [task('a', [], { failurePolicy: 'IGNORE_FAILURE' as never })] },
      TENANT_A,
    );
    expect(badPolicy.status).toBe(422);
  });

  it('creates, validates, publishes, and protects a version end to end', async () => {
    const { workflowId } = await createPublishedWorkflow(TENANT_A);
    const draft = await api(
      'POST',
      `/workflows/${workflowId}/versions`,
      {
        tasks: [
          task('fetch'),
          task('enrich', ['fetch']),
          task('storeA', ['enrich']),
          task('storeB', ['enrich']),
          task('finalize', ['storeA', 'storeB']),
        ],
      },
      TENANT_A,
    );
    expect(draft.status).toBe(201);
    const draftId = draft.body.workflow_version_id as string;
    expect(draft.body.state).toBe('DRAFT');

    const report = await api('POST', `/versions/${draftId}/validate`, {}, TENANT_A);
    expect(report.status).toBe(200);
    expect(report.body.valid).toBe(true);
    expect(report.body.errors).toEqual([]);

    const published = await api('POST', `/versions/${draftId}/publish`, {}, TENANT_A);
    expect(published.status).toBe(200);
    expect(published.body.state).toBe('PUBLISHED');
    expect(typeof published.body.definition_hash).toBe('string');

    // Re-publication is an invalid transition.
    const again = await api('POST', `/versions/${draftId}/publish`, {}, TENANT_A);
    expect(again.status).toBe(409);

    // Published definitions are immutable.
    const mutation = await api(
      'PUT',
      `/versions/${draftId}/definition`,
      { tasks: [task('other')] },
      TENANT_A,
    );
    expect(mutation.status).toBe(409);

    const inspected = await api('GET', `/versions/${draftId}`, undefined, TENANT_A);
    expect(inspected.status).toBe(200);
    expect(inspected.body.version).toEqual(
      expect.objectContaining({ state: 'PUBLISHED' }),
    );
    // The repository returns tasks ordered by task_key.
    expect(
      (inspected.body.tasks as { task_key: string }[]).map((t) => t.task_key),
    ).toEqual(['enrich', 'fetch', 'finalize', 'storeA', 'storeB']);

    const listed = await api('GET', `/workflows/${workflowId}/versions`, undefined, TENANT_A);
    expect(listed.status).toBe(200);
    expect(listed.body.length).toBe(2);

    const retired = await api('POST', `/versions/${draftId}/retire`, {}, TENANT_A);
    expect(retired.status).toBe(200);
    expect(retired.body.state).toBe('RETIRED');
  });

  it('updates a draft definition before publication', async () => {
    const { workflowId } = await createPublishedWorkflow(TENANT_A, [task('a')]);
    const draft = await api(
      'POST',
      `/workflows/${workflowId}/versions`,
      { tasks: [task('a')] },
      TENANT_A,
    );
    const draftId = draft.body.workflow_version_id as string;

    const updated = await api(
      'PUT',
      `/versions/${draftId}/definition`,
      { tasks: [task('a'), task('b', ['a'])] },
      TENANT_A,
    );
    expect(updated.status).toBe(200);

    const inspected = await api('GET', `/versions/${draftId}`, undefined, TENANT_A);
    expect(
      (inspected.body.tasks as { task_key: string }[]).map((t) => t.task_key),
    ).toEqual(['a', 'b']);
  });

  // -------------------------------------------------------------------------
  // Run start and inspection
  // -------------------------------------------------------------------------

  it('isolates versions across tenants (404, no cross-tenant mutation)', async () => {
    const created = await api(
      'POST',
      '/workflows',
      { name: `x-tenant-${Math.random().toString(36).slice(2, 10)}` },
      TENANT_A,
    );
    expect(created.status).toBe(201);
    const workflowId = created.body.workflow_id as string;

    const version = await api(
      'POST',
      `/workflows/${workflowId}/versions`,
      { tasks: [task('only')] },
      TENANT_A,
    );
    expect(version.status).toBe(201);
    const versionId = version.body.workflow_version_id as string;

    // Every version-scoped read and mutation 404s for the other tenant.
    expect(
      (await api('GET', `/versions/${versionId}`, undefined, TENANT_B)).status,
    ).toBe(404);
    expect(
      (await api('POST', `/versions/${versionId}/validate`, {}, TENANT_B))
        .status,
    ).toBe(404);
    expect(
      (await api('POST', `/versions/${versionId}/publish`, {}, TENANT_B))
        .status,
    ).toBe(404);
    expect(
      (await api('POST', `/versions/${versionId}/retire`, {}, TENANT_B)).status,
    ).toBe(404);
    expect(
      (
        await api(
          'PUT',
          `/versions/${versionId}/definition`,
          { tasks: [task('only')] },
          TENANT_B,
        )
      ).status,
    ).toBe(404);
    expect(
      (
        await api(
          'GET',
          `/workflows/${workflowId}/versions`,
          undefined,
          TENANT_B,
        )
      ).status,
    ).toBe(404);

    // Tenant B's attempts mutated nothing: A can still publish the draft.
    const published = await api(
      'POST',
      `/versions/${versionId}/publish`,
      {},
      TENANT_A,
    );
    expect(published.status).toBe(200);
    expect(published.body.state).toBe('PUBLISHED');
  });

  it('starts a run from the published version and supports idempotent replay', async () => {
    const { workflowId } = await createPublishedWorkflow(TENANT_A, [
      task('first'),
      task('second', ['first']),
    ]);
    const key = `idem-${Math.random().toString(36).slice(2, 12)}`;
    const input = { payload: 'from-api' };

    const started = await api(
      'POST',
      `/workflows/${workflowId}/runs`,
      { idempotencyKey: key, input },
      TENANT_A,
    );
    expect(started.status).toBe(201);
    expect(started.body.created).toBe(true);
    expect(started.body.state).toBe('RUNNING');
    const runId = started.body.workflow_run_id as string;

    const replay = await api(
      'POST',
      `/workflows/${workflowId}/runs`,
      { idempotencyKey: key, input },
      TENANT_A,
    );
    expect(replay.status).toBe(200);
    expect(replay.body.created).toBe(false);
    expect(replay.body.workflow_run_id).toBe(runId);

    // A different key creates a different run.
    const second = await api(
      'POST',
      `/workflows/${workflowId}/runs`,
      { idempotencyKey: `${key}-2`, input },
      TENANT_A,
    );
    expect(second.status).toBe(201);
    expect(second.body.workflow_run_id).not.toBe(runId);

    const detail = await api('GET', `/runs/${runId}`, undefined, TENANT_A);
    expect(detail.status).toBe(200);
    expect(detail.body.run).toEqual(
      expect.objectContaining({
        state: 'RUNNING',
        input_payload: input,
        idempotency_key: key,
      }),
    );
    // RunDetail wraps persistence rows in camelCase envelope fields.
    expect((detail.body.taskExecutions as unknown[]).length).toBe(2);

    const tasks = await api('GET', `/runs/${runId}/tasks`, undefined, TENANT_A);
    expect(tasks.status).toBe(200);
    expect(tasks.body.length).toBe(2);

    const firstTask = (tasks.body as unknown as { task_key: string; task_execution_id: string }[]).find(
      (t) => t.task_key === 'first',
    );
    expect(firstTask?.task_execution_id).toBeDefined();

    const attempts = await api(
      'GET',
      `/runs/${runId}/tasks/${firstTask?.task_execution_id}/attempts`,
      undefined,
      TENANT_A,
    );
    expect(attempts.status).toBe(200);
    expect(attempts.body).toEqual([]);

    const runList = await api(
      'GET',
      `/runs?workflowId=${workflowId}`,
      undefined,
      TENANT_A,
    );
    expect(runList.status).toBe(200);
    expect(runList.body.length).toBe(2);
  });

  it('rejects invalid run launches with stable codes and no DB diagnostics', async () => {
    const { workflowId, versionId } = await createPublishedWorkflow(
      TENANT_A,
      [task('only')],
    );

    // Reference a version that does not exist: a 404 on the version.
    const unknownVersion = await api(
      'POST',
      `/workflows/${workflowId}/runs`,
      { workflowVersionId: '00000000-0000-4000-8000-000000000000' },
      TENANT_A,
    );
    expect(unknownVersion.status).toBe(404);
    expect(unknownVersion.body.error).toBe('NotFound');

    // Retired versions cannot be launched.
    await api('POST', `/versions/${versionId}/retire`, {}, TENANT_A);
    const retiredVersion = await api(
      'POST',
      `/workflows/${workflowId}/runs`,
      { workflowVersionId: versionId },
      TENANT_A,
    );
    expect(retiredVersion.status).toBe(422);

    // With the only published version retired, no launchable version exists.
    const noVersion = await api(
      'POST',
      `/workflows/${workflowId}/runs`,
      {},
      TENANT_A,
    );
    expect(noVersion.status).toBe(422);

    // Unknown run IDs are 404, and messages never leak SQL or driver text.
    const missingRun = await api(
      'GET',
      '/runs/00000000-0000-4000-8000-000000000000',
      undefined,
      TENANT_A,
    );
    expect(missingRun.status).toBe(404);
    expect(JSON.stringify(missingRun.body)).not.toMatch(/SELECT|postgres|pgnative/i);

    const badLimit = await api('GET', '/runs?limit=abc', undefined, TENANT_A);
    expect(badLimit.status).toBe(400);
  });

  // -------------------------------------------------------------------------
  // API-key authentication and server-side tenant binding
  // -------------------------------------------------------------------------

  it('rejects unauthenticated and wrongly authenticated requests with 401', async () => {
    const missing = await api('GET', '/workflows', undefined, TENANT_A, null);
    expect(missing.status).toBe(401);
    expect(missing.body.statusCode).toBe(401);
    expect(missing.body.error).toBe('UnauthorizedException');
    expect(typeof missing.body.message).toBe('string');

    const wrongSecret = await api(
      'GET',
      '/workflows',
      undefined,
      TENANT_A,
      'wrong-secret-value-0000',
    );
    expect(wrongSecret.status).toBe(401);
    expect(wrongSecret.body.error).toBe('UnauthorizedException');

    // Health stays public: platform health checks carry no credentials.
    const readiness = await api(
      'GET',
      '/health/ready',
      undefined,
      undefined,
      null,
    );
    expect(readiness.status).toBe(200);
    expect(readiness.body.status).toBe('ready');
  });

  it('binds the tenant from the API key and rejects mismatched headers with 403', async () => {
    // Key A authenticates tenant A; claiming tenant B via the header is
    // forbidden — the header is never trusted as identity.
    const mismatch = await api(
      'POST',
      '/workflows',
      { name: 'mismatch-probe' },
      TENANT_B,
      SECRET_A,
    );
    expect(mismatch.status).toBe(403);
    expect(mismatch.body.error).toBe('ForbiddenException');

    // Without any tenant header, identity comes from the key alone.
    const name = `keybound-${Math.random().toString(36).slice(2, 10)}`;
    const created = await api(
      'POST',
      '/workflows',
      { name },
      undefined,
      SECRET_A,
    );
    expect(created.status).toBe(201);
    const workflowId = created.body.workflow_id as string;

    // Tenant B's key cannot see tenant A's workflow (key-bound isolation).
    const listedB = await api(
      'GET',
      '/workflows',
      undefined,
      undefined,
      SECRET_B,
    );
    expect(listedB.status).toBe(200);
    expect(
      (listedB.body as unknown as unknown[]).some(
        (row) => (row as { workflow_id: string }).workflow_id === workflowId,
      ),
    ).toBe(false);

    const fetchedB = await api(
      'GET',
      `/workflows/${workflowId}`,
      undefined,
      undefined,
      SECRET_B,
    );
    expect(fetchedB.status).toBe(404);
  });
});
