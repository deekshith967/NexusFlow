import { createHash, timingSafeEqual } from 'node:crypto';
import {
  CanActivate,
  ExecutionContext,
  ForbiddenException,
  Inject,
  Injectable,
  UnauthorizedException,
} from '@nestjs/common';
import type { Request } from 'express';

import { APP_CONFIG } from '../../config/config.module';
import { ApiKeyConfig, EnvConfig } from '../../config/env';
import { parseTenantId } from './dto';

export interface AuthenticatedApiKey {
  readonly name: string;
  readonly tenantId: string;
}

export interface AuthenticatedRequest extends Request {
  apiKey?: AuthenticatedApiKey;
}

/**
 * Finds the API key whose secret matches, comparing SHA-256 digests
 * in constant time (secrets have varying lengths, so the raw values
 * cannot go straight into timingSafeEqual). Returns undefined when
 * no key matches.
 */
export function findApiKey(
  keys: readonly ApiKeyConfig[],
  secret: string,
): ApiKeyConfig | undefined {
  const presented = createHash('sha256').update(secret, 'utf8').digest();

  return keys.find((key) => {
    const expected = createHash('sha256').update(key.secret, 'utf8').digest();
    return timingSafeEqual(presented, expected);
  });
}

function bearerToken(
  header: string | string[] | undefined,
): string | undefined {
  const value = Array.isArray(header) ? header[0] : header;

  if (!value) {
    return undefined;
  }

  const match = /^bearer (.+)$/i.exec(value.trim());
  const token = match?.[1]?.trim();

  return token ? token : undefined;
}

/**
 * Authenticates control-plane requests with `Authorization: Bearer
 * <api-key-secret>` and binds the request to the key's tenant. Apply
 * to every controller that serves tenant data; health endpoints stay
 * public so platform health checks (which carry no credentials) work.
 */
@Injectable()
export class ApiKeyAuthGuard implements CanActivate {
  constructor(@Inject(APP_CONFIG) private readonly config: EnvConfig) {}

  canActivate(context: ExecutionContext): boolean {
    const request = context.switchToHttp().getRequest<AuthenticatedRequest>();
    const token = bearerToken(request.headers['authorization']);

    if (!token) {
      throw new UnauthorizedException(
        'Missing or invalid Authorization header',
      );
    }

    const key = findApiKey(this.config.apiKeys, token);

    if (!key) {
      throw new UnauthorizedException('Invalid API key');
    }

    // Only the name and tenant travel with the request — never the secret.
    request.apiKey = { name: key.name, tenantId: key.tenantId };
    return true;
  }
}

/**
 * Resolves the tenant for an authenticated request: always the key's
 * tenant. The X-Tenant-Id header is never trusted as identity — when
 * present it must match the authenticated tenant (fail closed with
 * 403), and a malformed value still fails with 422. Requests that
 * never passed the guard are rejected (fail closed).
 */
export function resolveRequestTenantId(request: Request): string {
  const auth = (request as AuthenticatedRequest).apiKey;

  if (!auth) {
    throw new UnauthorizedException('Authentication required');
  }

  const header = request.headers['x-tenant-id'];
  const resolved = parseTenantId(
    Array.isArray(header) ? header[0] : header,
    auth.tenantId,
  );

  if (resolved !== auth.tenantId) {
    throw new ForbiddenException(
      'X-Tenant-Id does not match the authenticated tenant',
    );
  }

  return resolved;
}
