import { Logger } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import { Pool } from 'pg';
import { DatabaseModule } from '../../src/database/database.module';

describe('DatabaseModule', () => {
  let previousDatabaseUrl: string | undefined;
  let previousApiKeys: string | undefined;
  let testingModule: TestingModule | undefined;

  beforeEach(() => {
    previousDatabaseUrl = process.env.DATABASE_URL;
    previousApiKeys = process.env.NEXUSFLOW_API_KEYS;
    process.env.DATABASE_URL = 'postgresql://localhost/nexusflow_test';
    // loadEnvConfig defaults to the api role, which requires a key.
    process.env.NEXUSFLOW_API_KEYS =
      'test:test-secret-for-db-module-test:00000000-0000-0000-0000-000000000000';
  });

  afterEach(async () => {
    try {
      await testingModule?.close();
    } finally {
      testingModule = undefined;
      jest.restoreAllMocks();

      if (previousDatabaseUrl === undefined) {
        delete process.env.DATABASE_URL;
      } else {
        process.env.DATABASE_URL = previousDatabaseUrl;
      }

      if (previousApiKeys === undefined) {
        delete process.env.NEXUSFLOW_API_KEYS;
      } else {
        process.env.NEXUSFLOW_API_KEYS = previousApiKeys;
      }
    }
  });

  it('logs a generic message for unexpected idle-client pool errors', async () => {
    const genericMessage =
      'Unexpected PostgreSQL pool error from an idle client';
    const loggerError = jest
      .spyOn(Logger.prototype, 'error')
      .mockImplementation(() => undefined);

    testingModule = await Test.createTestingModule({
      imports: [DatabaseModule],
    }).compile();

    const pool = testingModule.get(Pool);
    pool.emit('error', new Error('Sensitive PostgreSQL diagnostic details'));

    expect(loggerError).toHaveBeenCalledTimes(1);
    expect(loggerError).toHaveBeenCalledWith(genericMessage);
  });
});
