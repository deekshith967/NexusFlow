import { HandlerRegistry } from './registry';
import { HandlerContext, HandlerResult } from './types';

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Built-in deterministic handlers.
 *
 * - echo (v1):      output = { echo: input }
 * - sleep (v1):     waits configuration.ms (default 10) and outputs
 *                   { sleptMs }; used to exercise leases and timeouts
 * - fail (v1):      fails while attemptNumber < configuration.failUntilAttempt
 *                   (default 1) and succeeds afterwards; used to exercise
 *                   retries and failure policies deterministically
 * - merge (v1):     output = { ...input, ...configuration.with }
 */
export function createBuiltinRegistry(): HandlerRegistry {
  const registry = new HandlerRegistry();

  registry.register('echo', '1', (ctx: HandlerContext): HandlerResult => ({
    status: 'succeeded',
    output: { echo: ctx.input ?? null },
  }));

  registry.register('sleep', '1', async (ctx: HandlerContext): Promise<HandlerResult> => {
    const rawMs = ctx.configuration.ms;
    const ms =
      typeof rawMs === 'number' && Number.isFinite(rawMs) && rawMs >= 0
        ? rawMs
        : 10;

    await sleep(ms);

    return { status: 'succeeded', output: { sleptMs: ms } };
  });

  registry.register('fail', '1', (ctx: HandlerContext): HandlerResult => {
    const rawUntil = ctx.configuration.failUntilAttempt;
    const failUntilAttempt =
      typeof rawUntil === 'number' && Number.isInteger(rawUntil) && rawUntil >= 1
        ? rawUntil
        : 1;

    const retryable =
      typeof ctx.configuration.retryable === 'boolean'
        ? ctx.configuration.retryable
        : true;

    const failureCode =
      typeof ctx.configuration.failureCode === 'string' &&
      ctx.configuration.failureCode.length > 0
        ? ctx.configuration.failureCode
        : 'HANDLER_FAILED';

    if (ctx.attemptNumber < failUntilAttempt) {
      return {
        status: 'failed',
        failureCode,
        failureMessage: `Simulated failure on attempt ${ctx.attemptNumber} of ${ctx.attemptNumber} (succeeds at attempt ${failUntilAttempt})`,
        retryable,
      };
    }

    return {
      status: 'succeeded',
      output: { recoveredAtAttempt: ctx.attemptNumber, failUntilAttempt },
    };
  });

  registry.register('merge', '1', (ctx: HandlerContext): HandlerResult => {
    const base =
      ctx.input && typeof ctx.input === 'object' && !Array.isArray(ctx.input)
        ? (ctx.input as Record<string, unknown>)
        : {};

    const withObject =
      ctx.configuration.with &&
      typeof ctx.configuration.with === 'object' &&
      !Array.isArray(ctx.configuration.with)
        ? (ctx.configuration.with as Record<string, unknown>)
        : {};

    return { status: 'succeeded', output: { ...base, ...withObject } };
  });

  return registry;
}
