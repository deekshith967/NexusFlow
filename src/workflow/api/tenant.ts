import { createParamDecorator, ExecutionContext } from '@nestjs/common';

import type { Request } from 'express';

/**
 * Extracts the tenant from the X-Tenant-Id header (falls back to the
 * configured default tenant). The value is validated by parseTenantId at
 * the controller boundary.
 */
export const TenantIdParam = createParamDecorator(
  (_data: unknown, ctx: ExecutionContext): string | undefined => {
    const request = ctx.switchToHttp().getRequest<Request>();
    const header = request.headers['x-tenant-id'];
    return Array.isArray(header) ? header[0] : header;
  },
);
