/**
 * Unit tests for the health controller's readiness semantics (no
 * database needed). The happy path against real PostgreSQL is covered
 * in test/integration/http-api.spec.ts; these tests pin the 200/503
 * status contract, including the database-down branch that the shared
 * integration app cannot exercise without breaking other tests.
 */
import type { Response } from 'express';

import type { EnvConfig } from '../../src/config/env';
import type { DatabaseService } from '../../src/database/database.service';
import type { ReadinessBody } from '../../src/workflow/api/health.controller';
import { HealthController } from '../../src/workflow/api/health.controller';

function testConfig(): EnvConfig {
  return {
    role: 'api',
    transport: 'memory',
    defaultTenantId: '00000000-0000-0000-0000-000000000000',
  } as unknown as EnvConfig;
}

function testController(checkHealth: () => Promise<void>): {
  controller: HealthController;
  res: Response;
  statusMock: jest.Mock;
} {
  const controller = new HealthController(
    { checkHealth } as unknown as DatabaseService,
    testConfig(),
  );
  const statusMock = jest.fn();
  const res = { status: statusMock } as unknown as Response;

  return { controller, res, statusMock };
}

describe('HealthController', () => {
  it('reports liveness without touching the database', () => {
    const { controller } = testController(() => {
      throw new Error('must not be called');
    });

    expect(controller.liveness()).toEqual({
      status: 'ok',
      service: 'nexusflow',
      role: 'api',
    });
  });

  it('reports 200 ready when the database is reachable', async () => {
    const { controller, res, statusMock } = testController(() =>
      Promise.resolve(),
    );

    const body: ReadinessBody = await controller.readiness(res);

    expect(statusMock).toHaveBeenCalledWith(200);
    expect(body).toEqual({
      status: 'ready',
      service: 'nexusflow',
      role: 'api',
      checks: {
        database: 'ok',
        transport: 'memory',
        defaultTenantId: '00000000-0000-0000-0000-000000000000',
      },
    });
  });

  it('reports 503 not_ready when the database is down', async () => {
    const { controller, res, statusMock } = testController(() =>
      Promise.reject(new Error('connection refused')),
    );

    const body: ReadinessBody = await controller.readiness(res);

    expect(statusMock).toHaveBeenCalledWith(503);
    expect(body).toEqual({
      status: 'not_ready',
      service: 'nexusflow',
      role: 'api',
      checks: {
        database: 'down',
        transport: 'memory',
        defaultTenantId: '00000000-0000-0000-0000-000000000000',
      },
    });
  });
});
