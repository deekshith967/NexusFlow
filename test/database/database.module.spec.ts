import { Logger } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import { Pool } from 'pg';
import { DatabaseModule } from '../../src/database/database.module';

describe('DatabaseModule', () => {
  let previousDatabaseUrl: string | undefined;
  let testingModule: TestingModule | undefined;

  beforeEach(() => {
    previousDatabaseUrl = process.env.DATABASE_URL;
    process.env.DATABASE_URL = 'postgresql://localhost/nexusflow_test';
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
