/**
 * Executable end-to-end demo (run with: npm run demo)
 *
 * Boots ONE local NexusFlow process (role=all, in-memory transport,
 * REAL PostgreSQL via DATABASE_URL), then drives the full lifecycle
 * over HTTP:
 *
 *   1. create workflow identity
 *   2. create draft version (DAG with a retrying task and a fan-in)
 *   3. validate + publish the version
 *   4. start a run
 *   5. watch the scheduler claim tasks and the worker execute them
 *      (one task fails twice and succeeds on attempt 3)
 *   6. inspect the persisted run, tasks, and attempts
 *
 * Required: DATABASE_URL pointing at a reachable PostgreSQL database.
 * Optional: PORT (default 3000), NEXUSFLOW_DEFAULT_TENANT_ID.
 *
 * Auth: the demo configures its own NEXUSFLOW_API_KEYS dev key (local
 * use only — never reuse the demo secret anywhere else) and sends it
 * as an Authorization: Bearer header on every request.
 */
import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import type { AddressInfo } from 'node:net';

import { AppModule } from '../src/app.module';
import { ApiExceptionsFilter } from '../src/workflow/api/exception-filter';
import {
  applyMigrations,
  checkDatabaseReachable,
} from '../src/database/migrations/applier';

const TENANT =
  process.env.NEXUSFLOW_DEFAULT_TENANT_ID ??
  '00000000-0000-0000-0000-000000000000';

// Local-demo credential only. Real deployments set NEXUSFLOW_API_KEYS
// from their secret store (see README "HTTP API" and .env.example).
const DEMO_API_SECRET = 'demo-local-dev-secret-0000';

async function main(): Promise<void> {
  const databaseUrl = process.env.DATABASE_URL;

  if (!databaseUrl) {
    console.error('DATABASE_URL is required (any reachable PostgreSQL).');
    process.exit(1);
  }

  process.env.NEXUSFLOW_ROLE = 'all';
  process.env.NEXUSFLOW_TRANSPORT = 'memory';
  process.env.NEXUSFLOW_DEFAULT_TENANT_ID = TENANT;
  process.env.NEXUSFLOW_API_KEYS = `demo:${DEMO_API_SECRET}:${TENANT}`;
  if (!process.env.PORT) {
    process.env.PORT = '3000';
  }

  console.log('==> Checking PostgreSQL...');
  const version = await checkDatabaseReachable(databaseUrl);
  console.log(`    ${version.split('\n')[0]}`);

  console.log('==> Applying migrations (idempotent)...');
  await applyMigrations(databaseUrl);
  console.log('    done');

  const app = await NestFactory.create(AppModule);
  app.useGlobalFilters(new ApiExceptionsFilter());
  app.enableShutdownHooks();
  await app.listen(Number(process.env.PORT), '127.0.0.1');
  const port = (app.getHttpServer() as unknown as {
    address(): AddressInfo | null;
  }).address()?.port ?? Number(process.env.PORT);
  const base = `http://127.0.0.1:${port}`;
  console.log(`==> NexusFlow (role=all) listening on ${base}`);

  const headers = {
    'content-type': 'application/json',
    'x-tenant-id': TENANT,
    authorization: `Bearer ${DEMO_API_SECRET}`,
  };

  const call = async (
    method: string,
    path: string,
    body?: unknown,
  ): Promise<{ status: number; body: Record<string, unknown> }> => {
    const res = await fetch(`${base}${path}`, {
      method,
      headers,
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    const text = await res.text();

    try {
      return { status: res.status, body: JSON.parse(text) as Record<string, unknown> };
    } catch {
      return { status: res.status, body: { raw: text } };
    }
  };

  const requireOk = (label: string, status: number, expected: number) => {
    if (status !== expected) {
      throw new Error(`${label} failed with status ${status}`);
    }
  };

  // 1. Workflow identity ----------------------------------------------------
  const stamp = Date.now().toString(36);
  console.log(`\n1) Creating workflow identity "demo-data-pipeline-${stamp}"...`);
  const wf = await call('POST', '/workflows', {
    name: `demo-data-pipeline-${stamp}`,
    description: 'NexusFlow end-to-end demo',
  });
  requireOk('workflow creation', wf.status, 201);
  const workflowId = wf.body.workflow_id as string;
  console.log(`    workflow_id=${workflowId} status=${wf.body.status}`);

  // 2. Draft version ---------------------------------------------------------
  console.log('\n2) Creating draft version with a DAG + a retrying task...');
  const ver = await call('POST', `/workflows/${workflowId}/versions`, {
    tasks: [
      {
        taskKey: 'fetch',
        handlerType: 'echo',
        handlerVersion: '1',
        configuration: {},
        timeoutMs: 10_000,
        failurePolicy: 'FAIL_WORKFLOW',
      },
      {
        taskKey: 'transform',
        handlerType: 'fail',
        handlerVersion: '1',
        // Fails on attempts 1 and 2, succeeds on attempt 3 (deterministic).
        configuration: { failUntilAttempt: 3 },
        retryPolicy: { maxAttempts: 3, retryDelayMs: 500 },
        timeoutMs: 10_000,
        failurePolicy: 'FAIL_WORKFLOW',
        dependencies: ['fetch'],
      },
      {
        taskKey: 'loadA',
        handlerType: 'sleep',
        handlerVersion: '1',
        configuration: { ms: 30 },
        timeoutMs: 10_000,
        failurePolicy: 'SKIP_DOWNSTREAM',
        dependencies: ['transform'],
      },
      {
        taskKey: 'loadB',
        handlerType: 'sleep',
        handlerVersion: '1',
        configuration: { ms: 30 },
        timeoutMs: 10_000,
        failurePolicy: 'SKIP_DOWNSTREAM',
        dependencies: ['transform'],
      },
      {
        taskKey: 'finalize',
        handlerType: 'merge',
        handlerVersion: '1',
        configuration: { with: { completed: true } },
        timeoutMs: 10_000,
        failurePolicy: 'FAIL_WORKFLOW',
        dependencies: ['loadA', 'loadB'],
      },
    ],
  });
  requireOk('version creation', ver.status, 201);
  const versionId = ver.body.workflow_version_id as string;
  console.log(`    version=${versionId} state=${ver.body.state}`);

  // 3. Validate + publish ----------------------------------------------------
  const validation = await call('POST', `/versions/${versionId}/validate`);
  requireOk('validation', validation.status, 200);
  console.log(`\n3) Validation: valid=${validation.body.valid}`);

  const published = await call('POST', `/versions/${versionId}/publish`);
  requireOk('publication', published.status, 200);
  console.log(
    `    state=${published.body.state} hash=${String(
      published.body.definition_hash,
    ).slice(0, 12)}...`,
  );

  // 4. Start run -------------------------------------------------------------
  console.log('\n4) Starting a run (idempotencyKey=demo-run-1)...');
  const run = await call('POST', `/workflows/${workflowId}/runs`, {
    idempotencyKey: 'demo-run-1',
    input: { dataset: 'customers-2026' },
  });
  requireOk('run start', run.status, 201);
  const runId = run.body.workflow_run_id as string;
  console.log(`    run=${runId} state=${run.body.state}`);

  // Idempotent replay check.
  const replay = await call('POST', `/workflows/${workflowId}/runs`, {
    idempotencyKey: 'demo-run-1',
    input: { dataset: 'customers-2026' },
  });
  console.log(
    `    idempotent replay: status=${replay.status} created=${replay.body.created} sameRun=${replay.body.workflow_run_id === runId}`,
  );

  // 5. Watch the run complete --------------------------------------------------
  console.log('\n5) Watching the run until it reaches a terminal state...');
  const deadline = Date.now() + 60_000;
  let state = 'RUNNING';

  while (Date.now() < deadline) {
    const detail = await call('GET', `/runs/${runId}`);
    state = (detail.body.run as { state: string }).state;

    if (state === 'SUCCEEDED' || state === 'FAILED' || state === 'CANCELLED') {
      break;
    }

    await new Promise((r) => setTimeout(r, 250));
  }

  console.log(`    final run state: ${state}`);
  if (state !== 'SUCCEEDED') {
    throw new Error(`demo run did not succeed (state=${state})`);
  }

  // 6. Inspect persisted state -------------------------------------------------
  console.log('\n6) Persisted task executions:');
  const tasks = await call('GET', `/runs/${runId}/tasks`);
  for (const t of tasks.body as unknown as {
    task_key: string;
    state: string;
    attempt_count: number;
  }[]) {
    console.log(
      `    ${t.task_key.padEnd(10)} state=${t.state} attempts=${t.attempt_count}`,
    );
  }

  const transformTask = (tasks.body as unknown as { task_key: string; task_execution_id: string }[]).find(
    (t) => t.task_key === 'transform',
  );
  const attempts = await call(
    'GET',
    `/runs/${runId}/tasks/${transformTask?.task_execution_id}/attempts`,
  );
  console.log('\n    attempts for "transform" (fails twice, then succeeds):');
  for (const a of attempts.body as unknown as {
    attempt_number: number;
    state: string;
    failure_message: string | null;
  }[]) {
    console.log(
      `      #${a.attempt_number} ${a.state}${
        a.failure_message ? ` — ${a.failure_message}` : ''
      }`,
    );
  }

  const finalRun = await call('GET', `/runs/${runId}`);
  console.log(
    `\nDemo complete: run ${runId} finished ${(finalRun.body.run as { state: string }).state} with ${
      (finalRun.body.taskExecutions as unknown[]).length
    } persisted task executions.`,
  );

  await app.close();
  process.exit(0);
}

main().catch((error) => {
  console.error('Demo failed:', error);
  process.exit(1);
});
