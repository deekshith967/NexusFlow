/**
 * Unit tests for API-key authentication (no database needed):
 * constant-time key lookup, guard behavior (missing / malformed /
 * valid credentials), and server-side tenant resolution (header
 * match, mismatch 403, malformed 422, missing-auth fail-closed).
 * End-to-end auth behavior over real HTTP is covered in
 * test/integration/http-api.spec.ts.
 */
import {
  ExecutionContext,
  ForbiddenException,
  UnauthorizedException,
} from '@nestjs/common';
import type { Request } from 'express';

import type { ApiKeyConfig, EnvConfig } from '../../src/config/env';
import {
  ApiKeyAuthGuard,
  findApiKey,
  resolveRequestTenantId,
} from '../../src/workflow/api/auth';
import { WorkflowValidationError } from '../../src/workflow/domain/workflow-errors';

const TENANT_A = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa';
const TENANT_B = 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb';
const SECRET_A = 'test-secret-for-tenant-a1';
const SECRET_B = 'test-secret-for-tenant-b1';

const KEYS: readonly ApiKeyConfig[] = [
  { name: 'key-a', secret: SECRET_A, tenantId: TENANT_A },
  { name: 'key-b', secret: SECRET_B, tenantId: TENANT_B },
];

function guardWith(keys: readonly ApiKeyConfig[] = KEYS): {
  guard: ApiKeyAuthGuard;
  contextFor: (headers: Record<string, string | undefined>) => {
    context: ExecutionContext;
    request: Request;
  };
} {
  const guard = new ApiKeyAuthGuard({ apiKeys: keys } as unknown as EnvConfig);

  const contextFor = (headers: Record<string, string | undefined>) => {
    const request = { headers } as unknown as Request;
    const context = {
      switchToHttp: () => ({ getRequest: () => request }),
    } as unknown as ExecutionContext;

    return { context, request };
  };

  return { guard, contextFor };
}

function catchError(fn: () => unknown): unknown {
  try {
    fn();
  } catch (error) {
    return error;
  }

  throw new Error('expected function to throw');
}

describe('findApiKey', () => {
  it('matches the exact secret among several keys', () => {
    expect(findApiKey(KEYS, SECRET_A)).toEqual(KEYS[0]);
    expect(findApiKey(KEYS, SECRET_B)).toEqual(KEYS[1]);
  });

  it('returns undefined for unknown, empty, or prefix-confusable secrets', () => {
    expect(findApiKey(KEYS, 'wrong-secret-value-0000')).toBeUndefined();
    expect(findApiKey(KEYS, '')).toBeUndefined();
    expect(findApiKey(KEYS, `${SECRET_A}-extra`)).toBeUndefined();
    expect(findApiKey(KEYS, SECRET_A.slice(0, -1))).toBeUndefined();
    expect(findApiKey([], SECRET_A)).toBeUndefined();
  });
});

describe('ApiKeyAuthGuard', () => {
  it('allows a valid Bearer secret and binds name + tenant (never the secret)', () => {
    const { guard, contextFor } = guardWith();
    const { context, request } = contextFor({
      authorization: `Bearer ${SECRET_A}`,
    });

    expect(guard.canActivate(context)).toBe(true);
    expect(request as unknown as Record<string, unknown>).toEqual(
      expect.objectContaining({
        apiKey: { name: 'key-a', tenantId: TENANT_A },
      }),
    );
    // The attached identity carries name + tenant only — never the secret.
    const attached = (request as unknown as Record<string, unknown>).apiKey;
    expect(JSON.stringify(attached)).not.toContain(SECRET_A);
  });

  it('accepts the Bearer scheme case-insensitively with surrounding whitespace', () => {
    const { guard, contextFor } = guardWith();
    const { context } = contextFor({
      authorization: `  bEaReR ${SECRET_B}  `,
    });

    expect(guard.canActivate(context)).toBe(true);
  });

  it('rejects missing, malformed, and unknown credentials with 401', () => {
    const { guard, contextFor } = guardWith();

    for (const headers of [
      {},
      { authorization: '' },
      { authorization: 'Bearer ' },
      { authorization: 'Bearer' },
      { authorization: 'Token abc' },
      { authorization: `Bearer ${SECRET_A.slice(0, -1)}x` },
      { authorization: 'Bearer wrong-secret-value-0000' },
    ]) {
      const { context } = contextFor(headers);
      const error = catchError(() => guard.canActivate(context));

      expect(error).toBeInstanceOf(UnauthorizedException);
      expect((error as UnauthorizedException).getStatus()).toBe(401);
    }
  });
});

describe('resolveRequestTenantId', () => {
  function authedRequest(
    headers: Record<string, string | undefined>,
    tenantId: string = TENANT_A,
  ): Request {
    return {
      headers,
      apiKey: { name: 'key-a', tenantId },
    } as unknown as Request;
  }

  it('returns the key tenant when the header is absent', () => {
    expect(resolveRequestTenantId(authedRequest({}))).toBe(TENANT_A);
  });

  it('returns the key tenant when the header matches (any case)', () => {
    expect(
      resolveRequestTenantId(authedRequest({ 'x-tenant-id': TENANT_A })),
    ).toBe(TENANT_A);
    expect(
      resolveRequestTenantId(
        authedRequest({ 'x-tenant-id': TENANT_A.toUpperCase() }),
      ),
    ).toBe(TENANT_A);
  });

  it('rejects a mismatched tenant header with 403 (header never trusted)', () => {
    const error = catchError(() =>
      resolveRequestTenantId(authedRequest({ 'x-tenant-id': TENANT_B })),
    );

    expect(error).toBeInstanceOf(ForbiddenException);
    expect((error as ForbiddenException).getStatus()).toBe(403);
  });

  it('rejects a malformed tenant header with 422', () => {
    const error = catchError(() =>
      resolveRequestTenantId(authedRequest({ 'x-tenant-id': 'nope' })),
    );

    expect(error).toBeInstanceOf(WorkflowValidationError);
  });

  it('fails closed when the request never passed the guard', () => {
    const { contextFor } = guardWith();
    // No apiKey property: simulates a route that never ran the guard.
    const { request } = contextFor({});
    const error = catchError(() => resolveRequestTenantId(request));

    expect(error).toBeInstanceOf(UnauthorizedException);
    expect((error as UnauthorizedException).getStatus()).toBe(401);
  });
});
