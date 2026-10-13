import { Module } from '@nestjs/common';

import { ConfigModule } from './config/config.module';
import { DatabaseModule } from './database/database.module';
import { OutboxRelayModule } from './outbox-relay/outbox-relay.module';
import { SchedulerModule } from './scheduler/scheduler.module';
import { TransportModule } from './transport/transport.module';
import { WorkflowModule } from './workflow/workflow.module';
import { WorkerModule } from './worker/worker.module';

/**
 * Single application with role-gated behavior (NEXUSFLOW_ROLE):
 * - api:          HTTP control plane (no background loops)
 * - scheduler:    claim loop only
 * - worker:       dispatch consumer only
 * - outbox-relay: outbox publish loop only
 * - all:          everything in one process (local demo convenience;
 *                 production deployments run the roles as separate
 *                 processes, per the architecture contract)
 */
@Module({
  imports: [
    ConfigModule,
    DatabaseModule,
    TransportModule,
    WorkflowModule,
    SchedulerModule,
    WorkerModule,
    OutboxRelayModule,
  ],
})
export class AppModule {}
