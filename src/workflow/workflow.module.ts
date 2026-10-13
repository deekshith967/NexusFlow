import { Module } from '@nestjs/common';

import { DatabaseModule } from '../database/database.module';
import { WorkflowService } from './application/workflow.service';
import { RunService } from './application/run.service';

@Module({
  imports: [DatabaseModule],
  providers: [WorkflowService, RunService],
  exports: [WorkflowService, RunService],
})
export class WorkflowModule {}
