/**
 * Handler contract. Handlers execute deterministic built-in operations
 * (no arbitrary untrusted code). Handlers may have externally visible
 * effects only if they are idempotent (contract: at-least-once
 * execution; exactly-once effects are the handler's responsibility).
 */

export interface HandlerContext {
  /** Resolved task input (run input + upstream outputs). */
  readonly input: unknown;
  /** Task definition configuration (version-pinned, immutable). */
  readonly configuration: Readonly<Record<string, unknown>>;
  /** 1-based attempt number for the current attempt. */
  readonly attemptNumber: number;
}

export type HandlerResult =
  | { readonly status: 'succeeded'; readonly output: unknown }
  | {
      readonly status: 'failed';
      readonly failureCode: string;
      readonly failureMessage: string;
      readonly retryable: boolean;
    };

export type TaskHandler = (
  context: HandlerContext,
) => HandlerResult | Promise<HandlerResult>;
