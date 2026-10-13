import {
  Inject,
  Injectable,
  OnApplicationShutdown,
} from '@nestjs/common';
import { Pool } from 'pg';
import type { PoolClient, QueryResult, QueryResultRow } from 'pg';

@Injectable()
export class DatabaseService implements OnApplicationShutdown {
  constructor(@Inject(Pool) private readonly pool: Pool) {}

  query<R extends QueryResultRow = QueryResultRow>(
    text: string,
    values: readonly unknown[] = [],
  ): Promise<QueryResult<R>> {
    return this.pool.query<R>(text, [...values]);
  }

  async transaction<T>(
    operation: (client: PoolClient) => Promise<T>,
  ): Promise<T> {
    const client = await this.pool.connect();
    let discardClient = false;

    try {
      await client.query('BEGIN');
      const result = await operation(client);
      await client.query('COMMIT');
      return result;
    } catch (error) {
      try {
        await client.query('ROLLBACK');
      } catch {
        // Discard connections whose transaction could not be rolled back.
        discardClient = true;
      }

      // Keep the initiating error, even if rollback failed as well.
      throw error;
    } finally {
      if (discardClient) {
        client.release(true);
      } else {
        client.release();
      }
    }
  }

  async onApplicationShutdown(): Promise<void> {
    await this.pool.end();
  }
}
