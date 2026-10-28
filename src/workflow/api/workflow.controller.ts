import {
  Body,
  Controller,
  Get,
  HttpCode,
  Param,
  Post,
  Put,
  Req,
  Res,
  UseGuards,
} from '@nestjs/common';
import type { Request, Response } from 'express';

import { ApiKeyAuthGuard } from './auth';
import {
  parseCreateWorkflowBody,
  parseStartRunBody,
  parseVersionBody,
} from './dto';
import { TenantIdParam } from './tenant';
import { FailurePolicy } from '../domain/failure-policy';
import { WorkflowService } from '../application/workflow.service';
import { RunService } from '../application/run.service';

@UseGuards(ApiKeyAuthGuard)
@Controller()
export class WorkflowController {
  constructor(
    private readonly workflows: WorkflowService,
    private readonly runs: RunService,
  ) {}

  @Post('workflows')
  async createWorkflow(
    @TenantIdParam() tenantId: string,
    @Body() body: unknown,
  ) {
    const parsed = parseCreateWorkflowBody(body);

    const workflow = await this.workflows.createWorkflow({
      tenantId,
      name: parsed.name,
      description: parsed.description,
    });

    return workflow;
  }

  @Get('workflows')
  async listWorkflows(@TenantIdParam() tenantId: string) {
    return this.workflows.listWorkflows(tenantId);
  }

  @Get('workflows/:workflowId')
  async getWorkflow(
    @TenantIdParam() tenantId: string,
    @Param('workflowId') workflowId: string,
  ) {
    return this.workflows.getWorkflow(tenantId, workflowId);
  }

  @Post('workflows/:workflowId/versions')
  async createVersion(
    @TenantIdParam() tenantId: string,
    @Param('workflowId') workflowId: string,
    @Body() body: unknown,
  ) {
    const parsed = parseVersionBody(body);
    const workflow = await this.workflows.getWorkflow(tenantId, workflowId);

    const version = await this.workflows.createDraftVersion({
      tenantId,
      workflowId,
      definition: {
        name: parsed.name ?? workflow.name,
        ...(parsed.description !== undefined
          ? { description: parsed.description }
          : {}),
        tasks: parsed.tasks.map((t) => ({
          taskKey: t.taskKey,
          handlerType: t.handlerType,
          handlerVersion: t.handlerVersion,
          configuration: t.configuration,
          inputMapping: t.inputMapping,
          outputMapping: t.outputMapping,
          retryPolicy: t.retryPolicy,
          timeoutMs: t.timeoutMs,
          failurePolicy: t.failurePolicy as FailurePolicy,
          dependencies: t.dependencies,
        })),
      },
    });

    return version;
  }

  @Get('workflows/:workflowId/versions')
  async listVersions(
    @TenantIdParam() tenantId: string,
    @Param('workflowId') workflowId: string,
  ) {
    return this.workflows.listVersions(tenantId, workflowId);
  }

  @Post('workflows/:workflowId/runs')
  async startRun(
    @TenantIdParam() tenantId: string,
    @Param('workflowId') workflowId: string,
    @Body() body: unknown,
    @Req() req: Request,
    @Res() res: Response,
  ) {
    const parsed = parseStartRunBody(body);

    const result = await this.runs.startRun({
      tenantId,
      workflowId,
      workflowVersionId: parsed.workflowVersionId,
      input: parsed.input,
      idempotencyKey: parsed.idempotencyKey,
      correlationId:
        (typeof req.headers['x-correlation-id'] === 'string'
          ? req.headers['x-correlation-id']
          : undefined) ?? undefined,
    });

    // 201 when a new run is created; 200 for an idempotent replay.
    res.status(result.created ? 201 : 200).json({
      workflow_run_id: result.run.workflow_run_id,
      workflow_id: result.run.workflow_id,
      workflow_version_id: result.run.workflow_version_id,
      state: result.run.state,
      created: result.created,
      correlation_id: result.run.correlation_id,
      started_at: result.run.started_at,
    });
  }
}

@UseGuards(ApiKeyAuthGuard)
@Controller('versions')
export class VersionController {
  constructor(private readonly workflows: WorkflowService) {}

  @Get(':workflowVersionId')
  async getVersion(
    @TenantIdParam() tenantId: string,
    @Param('workflowVersionId') workflowVersionId: string,
  ) {
    return this.workflows.getVersion(tenantId, workflowVersionId);
  }

  @Put(':workflowVersionId/definition')
  async updateDefinition(
    @TenantIdParam() tenantId: string,
    @Param('workflowVersionId') workflowVersionId: string,
    @Body() body: unknown,
  ) {
    const parsed = parseVersionBody(body);
    const existing = await this.workflows.getVersion(tenantId, workflowVersionId);

    return this.workflows.updateDraftDefinition({
      tenantId,
      workflowVersionId,
      definition: {
        name:
          parsed.name ?? existing.workflow?.name ?? 'unknown-workflow',
        ...(parsed.description !== undefined
          ? { description: parsed.description }
          : {}),
        tasks: parsed.tasks.map((t) => ({
          taskKey: t.taskKey,
          handlerType: t.handlerType,
          handlerVersion: t.handlerVersion,
          configuration: t.configuration,
          inputMapping: t.inputMapping,
          outputMapping: t.outputMapping,
          retryPolicy: t.retryPolicy,
          timeoutMs: t.timeoutMs,
          failurePolicy: t.failurePolicy as FailurePolicy,
          dependencies: t.dependencies,
        })),
      },
    });
  }

  @Post(':workflowVersionId/validate')
  @HttpCode(200)
  async validate(
    @TenantIdParam() tenantId: string,
    @Param('workflowVersionId') workflowVersionId: string,
  ) {
    return this.workflows.getValidationReport(tenantId, workflowVersionId);
  }

  @Post(':workflowVersionId/publish')
  @HttpCode(200)
  async publish(
    @TenantIdParam() tenantId: string,
    @Param('workflowVersionId') workflowVersionId: string,
  ) {
    return this.workflows.publish(tenantId, workflowVersionId);
  }

  @Post(':workflowVersionId/retire')
  @HttpCode(200)
  async retire(
    @TenantIdParam() tenantId: string,
    @Param('workflowVersionId') workflowVersionId: string,
  ) {
    return this.workflows.retire(tenantId, workflowVersionId);
  }
}
