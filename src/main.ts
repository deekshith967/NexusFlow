import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { Logger } from '@nestjs/common';

import { AppModule } from './app.module';
import { loadEnvConfig } from './config/env';
import { checkDatabaseReachable } from './database/migrations/applier';

const logger = new Logger('Bootstrap');

async function bootstrap(): Promise<void> {
  const config = loadEnvConfig(process.env);

  // Fail fast when PostgreSQL is unreachable.
  const pgVersion = await checkDatabaseReachable(config.databaseUrl);
  logger.log(
    `PostgreSQL reachable: ${pgVersion.split('\n')[0]} (role=${config.role}, transport=${config.transport})`,
  );

  const app = await NestFactory.create(AppModule);
  app.enableShutdownHooks();

  if (config.role === 'api' || config.role === 'all') {
    app.setGlobalPrefix('', { exclude: [] });
    await app.listen(config.port);
    logger.log(`NexusFlow API listening on port ${config.port}`);
  } else {
    // Non-HTTP process: initialize lifecycle services (scheduler loop,
    // worker consumer, relay loop) and keep running until signaled.
    await app.init();
    logger.log(
      `NexusFlow ${config.role} started (service=${config.serviceId})`,
    );
  }
}

bootstrap().catch((error) => {
  logger.error(`Bootstrap failed: ${(error as Error).message}`);
  process.exit(1);
});
