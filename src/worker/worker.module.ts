import { Module } from '@nestjs/common';

import { ConfigModule } from '../config/config.module';
import { DatabaseModule } from '../database/database.module';
import { TransportModule } from '../transport/transport.module';
import { WorkerService } from './worker.service';

@Module({
  imports: [ConfigModule, DatabaseModule, TransportModule],
  providers: [WorkerService],
  exports: [WorkerService],
})
export class WorkerModule {}
