import { Pool } from 'pg';
import { DatabaseService } from '../../src/database/database.service';

function createService() {
  const pool = {
    query: jest.fn(),
    connect: jest.fn(),
    end: jest.fn(),
  };

  return {
    pool,
    service: new DatabaseService(pool as unknown as Pool),
  };
}

function createClient() {
  return {
    query: jest.fn().mockResolvedValue({ rows: [] }),
    release: jest.fn(),
  };
}

describe('DatabaseService', () => {
  describe('query', () => {
    it('delegates parameterized SQL and returns the query result', async () => {
      const { pool, service } = createService();
      const result = { rows: [{ id: 42 }], rowCount: 1 };
      pool.query.mockResolvedValue(result);

      await expect(
        service.query<{ id: number }>(
          'SELECT id FROM items WHERE id = $1',
          [42],
        ),
      ).resolves.toBe(result);

      expect(pool.query).toHaveBeenCalledWith(
        'SELECT id FROM items WHERE id = $1',
        [42],
      );
    });
  });

  describe('transaction', () => {
    it('commits successful work and returns its result', async () => {
      const { pool, service } = createService();
      const client = createClient();
      pool.connect.mockResolvedValue(client);

      const result = await service.transaction(async (transactionClient) => {
        await transactionClient.query(
          'UPDATE accounts SET balance = $1 WHERE account_id = $2',
          [100, 'account-1'],
        );
        return 'committed';
      });

      expect(result).toBe('committed');
      expect(pool.connect).toHaveBeenCalledTimes(1);
      expect(client.query).toHaveBeenNthCalledWith(1, 'BEGIN');
      expect(client.query).toHaveBeenNthCalledWith(
        2,
        'UPDATE accounts SET balance = $1 WHERE account_id = $2',
        [100, 'account-1'],
      );
      expect(client.query).toHaveBeenNthCalledWith(3, 'COMMIT');
      expect(client.release).toHaveBeenCalledTimes(1);
    });

    it('rolls back failed work and releases its client', async () => {
      const { pool, service } = createService();
      const client = createClient();
      const operationError = new Error('operation failed');
      pool.connect.mockResolvedValue(client);

      await expect(
        service.transaction(() => Promise.reject(operationError)),
      ).rejects.toBe(operationError);

      expect(client.query).toHaveBeenNthCalledWith(1, 'BEGIN');
      expect(client.query).toHaveBeenNthCalledWith(2, 'ROLLBACK');
      expect(client.release).toHaveBeenCalledTimes(1);
    });

    it('preserves the operation error if rollback fails and discards the client', async () => {
      const { pool, service } = createService();
      const client = createClient();
      const operationError = new Error('operation failed');
      const rollbackError = new Error('rollback failed');
      pool.connect.mockResolvedValue(client);
      client.query.mockImplementation((statement: string) => {
        if (statement === 'ROLLBACK') {
          return Promise.reject(rollbackError);
        }

        return Promise.resolve({ rows: [] });
      });

      await expect(
        service.transaction(() => Promise.reject(operationError)),
      ).rejects.toBe(operationError);

      expect(client.query).toHaveBeenNthCalledWith(2, 'ROLLBACK');
      expect(client.release).toHaveBeenCalledTimes(1);
      expect(client.release).toHaveBeenCalledWith(true);
    });

    it('releases the client if COMMIT fails after attempting rollback', async () => {
      const { pool, service } = createService();
      const client = createClient();
      const commitError = new Error('commit failed');
      pool.connect.mockResolvedValue(client);
      client.query.mockImplementation((statement: string) => {
        if (statement === 'COMMIT') {
          return Promise.reject(commitError);
        }

        return Promise.resolve({ rows: [] });
      });

      await expect(service.transaction(() => Promise.resolve('result'))).rejects.toBe(
        commitError,
      );

      expect(client.query).toHaveBeenNthCalledWith(1, 'BEGIN');
      expect(client.query).toHaveBeenNthCalledWith(2, 'COMMIT');
      expect(client.query).toHaveBeenNthCalledWith(3, 'ROLLBACK');
      expect(client.release).toHaveBeenCalledTimes(1);
    });

    it('closes the pool during application shutdown', async () => {
      const { pool, service } = createService();
      pool.end.mockResolvedValue(undefined);

      await service.onApplicationShutdown();

      expect(pool.end).toHaveBeenCalledTimes(1);
    });
  });
});
