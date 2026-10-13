import {
  Body,
  Controller,
  Get,
  HttpCode,
  Inject,
  Param,
  Post,
  Put,
  Req,
  Res,
} from '@nestjs/common';
import type { Request, Response } from 'express';

import { APP_CONFIG } from '../../config/config.module';
import { EnvConfig } from '../../config/env';
import {
  parseCreateWorkflowBody,
  parseStartRunBody,
  parseTenantId,
  parseVersionBody,
} from './dto';
import { TenantIdParam } from './tenant';
import { FailurePolicy } from '../domain/failure-policy';
import { WorkflowService } from '../application/workflow.service';
import { RunService } from '../application/run.service';

@Controller()
export class WorkflowController {
  constructor(
    private readonly workflows: WorkflowService,
    private readonly runs: RunService,
    @Inject(APP_CONFIG) private readonly config: EnvConfig,
  ) {}

  @Post('workflows')
  async createWorkflow(
    @TenantIdParam() tenantHeader: string | undefined,
    @Body() body: unknown,
  ) {
    const tenantId = parseTenantId(tenantHeader, this.config.defaultTenantId);
    const parsed = parseCreateWorkflowBody(body);

    const workflow = await this.workflows.createWorkflow({
      tenantId,
      name: parsed.name,
      description: parsed.description,
    });

    return workflow;
  }

  @Get('workflows')
  async listWorkflows(@TenantIdParam() tenantHeader: string | undefined) {
    const tenantId = parseTenantId(tenantHeader, this.config.defaultTenantId);
    return this.workflows.listWorkflows(tenantId);
  }

  @Get('workflows/:workflowId')
  async getWorkflow(
    @TenantIdParam() tenantHeader: string | undefined,
    @Param('workflowId') workflowId: string,
  ) {
    const tenantId = parseTenantId(tenantHeader, this.config.defaultTenantId);
    return this.workflows.getWorkflow(tenantId, workflowId);
  }

  @Post('workflows/:workflowId/versions')
  async createVersion(
    @TenantIdParam() tenantHeader: string | undefined,
    @Param('workflowId') workflowId: string,
    @Body() body: unknown,
  ) {
    const tenantId = parseTenantId(tenantHeader, this.config.defaultTenantId);
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
  async listVersions(@Param('workflowId') workflowId: string) {
    return this.workflows.listVersions(workflowId);
  }

  @Post('workflows/:workflowId/runs')
  async startRun(
    @TenantIdParam() tenantHeader: string | undefined,
    @Param('workflowId') workflowId: string,
    @Body() body: unknown,
    @Req() req: Request,
    @Res() res: Response,
  ) {
    const tenantId = parseTenantId(tenantHeader, this.config.defaultTenantId);
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

@Controller('versions')
export class VersionController {
  constructor(private readonly workflows: WorkflowService) {}

  @Get(':workflowVersionId')
  async getVersion(@Param('workflowVersionId') workflowVersionId: string) {
    return this.workflows.getVersion(workflowVersionId);
  }

  @Put(':workflowVersionId/definition')
  async updateDefinition(
    @Param('workflowVersionId') workflowVersionId: string,
    @Body() body: unknown,
  ) {
    const parsed = parseVersionBody(body);
    const existing = await this.workflows.getVersion(workflowVersionId);

    return this.workflows.updateDraftDefinition({
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
  async validate(@Param('workflowVersionId') workflowVersionId: string) {
    return this.workflows.getValidationReport(workflowVersionId);
  }

  @Post(':workflowVersionId/publish')
  @HttpCode(200)
  async publish(@Param('workflowVersionId') workflowVersionId: string) {
    return this.workflows.publish(workflowVersionId);
  }

  @Post(':workflowVersionId/retire')
  @HttpCode(200)
  async retire(@Param('workflowVersionId') workflowVersionId: string) {
    return this.workflows.retire(workflowVersionId);
  }
}
