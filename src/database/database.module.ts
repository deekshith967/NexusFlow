import { Logger, Module } from '@nestjs/common';
import { Pool } from 'pg';
import { DatabaseService } from './database.service';

const logger = new Logger('DatabaseModule');

@Module({
  providers: [
    {
      provide: Pool,
      useFactory: (): Pool => {
        const connectionString = process.env.DATABASE_URL;
        if (!connectionString?.trim()) {
          throw new Error('DATABASE_URL is required for PostgreSQL access');
        }

        const pool = new Pool({ connectionString });
        pool.on('error', () => {
          logger.error('Unexpected PostgreSQL pool error from an idle client');
        });

        return pool;
      },
    },
    DatabaseService,
  ],
  exports: [DatabaseService],
})
export class DatabaseModule {}
