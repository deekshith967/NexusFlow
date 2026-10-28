import { Test } from '@nestjs/testing';
import { Pool } from 'pg';
import { AppModule } from '../src/app.module';

describe('AppModule', () => {
  const previousDatabaseUrl = process.env.DATABASE_URL;

  beforeAll(() => {
    // The Pool provider is overridden below; a placeholder URL satisfies
    // the environment configuration contract without connecting.
    process.env.DATABASE_URL =
      process.env.DATABASE_URL ??
      'postgres://nexusflow:nexusflow@127.0.0.1:5432/nexusflow';
    process.env.NEXUSFLOW_ROLE = 'api';
    process.env.NEXUSFLOW_TRANSPORT = 'memory';
    // The api role requires at least one API key (throwaway test value).
    process.env.NEXUSFLOW_API_KEYS =
      'test:test-secret-for-app-module-0:00000000-0000-0000-0000-000000000000';
  });

  afterAll(() => {
    if (previousDatabaseUrl === undefined) {
      delete process.env.DATABASE_URL;
    } else {
      process.env.DATABASE_URL = previousDatabaseUrl;
    }
    delete process.env.NEXUSFLOW_ROLE;
    delete process.env.NEXUSFLOW_TRANSPORT;
    delete process.env.NEXUSFLOW_API_KEYS;
  });

  it('compiles the root application module and shuts down the database pool', async () => {
    const pool = { end: jest.fn().mockResolvedValue(undefined) };
    const module = await Test.createTestingModule({ imports: [AppModule] })
      .overrideProvider(Pool)
      .useValue(pool)
      .compile();

    expect(module).toBeDefined();
    await module.close();

    expect(pool.end).toHaveBeenCalledTimes(1);
  });
});
