import {
  BadRequestException,
  Controller,
  Get,
  Inject,
  Param,
  Query,
} from '@nestjs/common';

import { APP_CONFIG } from '../../config/config.module';
import { EnvConfig } from '../../config/env';
import { RunService } from '../application/run.service';
import { parseTenantId } from './dto';
import { TenantIdParam } from './tenant';

@Controller()
export class RunController {
  constructor(
    private readonly runs: RunService,
    @Inject(APP_CONFIG) private readonly config: EnvConfig,
  ) {}

  @Get('runs')
  async listRuns(
    @TenantIdParam() tenantHeader: string | undefined,
    @Query('workflowId') workflowId?: string,
    @Query('limit') limit?: string,
  ) {
    const tenantId = parseTenantId(tenantHeader, this.config.defaultTenantId);
    const parsedLimit = limit === undefined ? undefined : Number(limit);

    if (
      parsedLimit !== undefined &&
      (!Number.isInteger(parsedLimit) || parsedLimit < 1 || parsedLimit > 200)
    ) {
      throw new BadRequestException(
        'limit must be an integer between 1 and 200',
      );
    }

    return this.runs.listRuns(tenantId, workflowId, parsedLimit);
  }

  @Get('runs/:workflowRunId')
  async getRun(
    @TenantIdParam() tenantHeader: string | undefined,
    @Param('workflowRunId') workflowRunId: string,
  ) {
    const tenantId = parseTenantId(tenantHeader, this.config.defaultTenantId);
    return this.runs.getRunDetail(tenantId, workflowRunId);
  }

  @Get('runs/:workflowRunId/tasks')
  async listTasks(
    @TenantIdParam() tenantHeader: string | undefined,
    @Param('workflowRunId') workflowRunId: string,
  ) {
    const tenantId = parseTenantId(tenantHeader, this.config.defaultTenantId);
    return this.runs.listTaskExecutions(tenantId, workflowRunId);
  }

  @Get('runs/:workflowRunId/tasks/:taskExecutionId/attempts')
  async listAttempts(
    @TenantIdParam() tenantHeader: string | undefined,
    @Param('taskExecutionId') taskExecutionId: string,
  ) {
    const tenantId = parseTenantId(tenantHeader, this.config.defaultTenantId);
    return this.runs.listAttempts(tenantId, taskExecutionId);
  }
}
