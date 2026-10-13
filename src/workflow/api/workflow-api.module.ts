import { Module } from '@nestjs/common';

import { DatabaseModule } from '../../database/database.module';
import { WorkflowModule } from '../workflow.module';
import { HealthController } from './health.controller';
import { RunController } from './run.controller';
import { VersionController, WorkflowController } from './workflow.controller';

/**
 * HTTP control plane (Phase F). Controllers are registered in every role,
 * but HTTP is only enabled for the api/all roles (see main.ts).
 */
@Module({
  imports: [DatabaseModule, WorkflowModule],
  controllers: [
    WorkflowController,
    VersionController,
    RunController,
    HealthController,
  ],
})
export class WorkflowApiModule {}
