import { Logger, Module } from '@nestjs/common';
import { Pool } from 'pg';
import { loadEnvConfig } from '../config/env';
import { DatabaseService } from './database.service';
import { configurePgTypeParsers } from './parsers';

const logger = new Logger('DatabaseModule');

@Module({
  providers: [
    {
      provide: Pool,
      useFactory: (): Pool => {
        // Parsed centrally so DATABASE_URL/DATABASE_SSL validation and
        // error messages match the rest of the application.
        const config = loadEnvConfig(process.env);

        configurePgTypeParsers();

        const pool = new Pool({
          connectionString: config.databaseUrl,
          // Managed PostgreSQL (e.g. Render) requires TLS. The server
          // certificate chain is not verified because managed providers
          // rotate certificates outside the default trust store; this
          // protects against passive eavesdropping, not an active MITM.
          // The connection string itself is never logged.
          ssl: config.databaseSsl ? { rejectUnauthorized: false } : undefined,
        });
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
