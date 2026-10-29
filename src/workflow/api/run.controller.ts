import {
  BadRequestException,
  Controller,
  Get,
  Param,
  ParseUUIDPipe,
  Query,
  UseGuards,
} from '@nestjs/common';

import { RunService } from '../application/run.service';
import { ApiKeyAuthGuard } from './auth';
import { TenantIdParam } from './tenant';

@UseGuards(ApiKeyAuthGuard)
@Controller()
export class RunController {
  constructor(private readonly runs: RunService) {}

  @Get('runs')
  async listRuns(
    @TenantIdParam() tenantId: string,
    @Query('workflowId', new ParseUUIDPipe({ optional: true }))
    workflowId?: string,
    @Query('limit') limit?: string,
  ) {
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
    @TenantIdParam() tenantId: string,
    @Param('workflowRunId', ParseUUIDPipe) workflowRunId: string,
  ) {
    return this.runs.getRunDetail(tenantId, workflowRunId);
  }

  @Get('runs/:workflowRunId/tasks')
  async listTasks(
    @TenantIdParam() tenantId: string,
    @Param('workflowRunId', ParseUUIDPipe) workflowRunId: string,
  ) {
    return this.runs.listTaskExecutions(tenantId, workflowRunId);
  }

  @Get('runs/:workflowRunId/tasks/:taskExecutionId/attempts')
  async listAttempts(
    @TenantIdParam() tenantId: string,
    @Param('workflowRunId', ParseUUIDPipe) workflowRunId: string,
    @Param('taskExecutionId', ParseUUIDPipe) taskExecutionId: string,
  ) {
    return this.runs.listAttempts(tenantId, workflowRunId, taskExecutionId);
  }
}
