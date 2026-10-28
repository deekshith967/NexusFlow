import { createParamDecorator, ExecutionContext } from '@nestjs/common';

import type { Request } from 'express';

import { resolveRequestTenantId } from './auth';

/**
 * Resolves the authenticated tenant for the request: the API key's
 * tenant, bound server-side by ApiKeyAuthGuard. The X-Tenant-Id
 * header is never trusted as identity — when present it must match
 * the authenticated tenant (403 otherwise).
 */
export const TenantIdParam = createParamDecorator(
  (_data: unknown, ctx: ExecutionContext): string => {
    const request = ctx.switchToHttp().getRequest<Request>();
    return resolveRequestTenantId(request);
  },
);
