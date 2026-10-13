import {
  ArgumentsHost,
  Catch,
  ExceptionFilter,
  HttpException,
  HttpStatus,
  Logger,
} from '@nestjs/common';
import { Response } from 'express';

import {
  DomainError,
  RunLaunchRejectedError,
  WorkflowNameConflictError,
  WorkflowNotFoundError,
  WorkflowRunNotFoundError,
  WorkflowVersionNotFoundError,
} from '../../common/errors';
import {
  InvalidWorkflowVersionTransitionError,
  PublishedWorkflowVersionImmutableError,
  WorkflowValidationError,
} from '../domain/workflow-errors';

/**
 * Maps domain errors to stable HTTP responses. Raw database diagnostics
 * are never exposed: unexpected persistence errors become a generic 500.
 */
@Catch()
export class ApiExceptionsFilter implements ExceptionFilter {
  private readonly logger = new Logger(ApiExceptionsFilter.name);

  catch(exception: unknown, host: ArgumentsHost): void {
    const ctx = host.switchToHttp();
    const response = ctx.getResponse<Response>();

    if (exception instanceof HttpException) {
      const status = exception.getStatus();
      const body = exception.getResponse();

      response.status(status).json({
        statusCode: status,
        error: exception.name,
        message:
          typeof body === 'string'
            ? body
            : (body as { message?: string | string[] }).message ??
              exception.message,
      });
      return;
    }

    let status = HttpStatus.INTERNAL_SERVER_ERROR;
    let errorName = 'InternalError';
    let message = 'Internal server error';

    if (exception instanceof WorkflowValidationError) {
      status = HttpStatus.UNPROCESSABLE_ENTITY;
      errorName = 'ValidationError';
      message = exception.message;
    } else if (exception instanceof WorkflowNameConflictError) {
      status = HttpStatus.CONFLICT;
      errorName = 'Conflict';
      message = exception.message;
    } else if (
      exception instanceof InvalidWorkflowVersionTransitionError ||
      exception instanceof PublishedWorkflowVersionImmutableError
    ) {
      status = HttpStatus.CONFLICT;
      errorName = 'Conflict';
      message = exception.message;
    } else if (
      exception instanceof WorkflowNotFoundError ||
      exception instanceof WorkflowVersionNotFoundError ||
      exception instanceof WorkflowRunNotFoundError
    ) {
      status = HttpStatus.NOT_FOUND;
      errorName = 'NotFound';
      message = exception.message;
    } else if (exception instanceof RunLaunchRejectedError) {
      status = HttpStatus.UNPROCESSABLE_ENTITY;
      errorName = 'RunLaunchRejected';
      message = exception.message;
    } else if (exception instanceof DomainError) {
      status = HttpStatus.UNPROCESSABLE_ENTITY;
      errorName = exception.name;
      message = exception.message;
    } else if (exception instanceof Error) {
      // Never leak raw database or driver diagnostics.
      this.logger.error(
        `Unhandled error: ${exception.message}`,
        exception.stack,
      );
    }

    response.status(status).json({ statusCode: status, error: errorName, message });
  }
}
