import { TaskHandler } from './types';

/**
 * Registry of deterministic built-in handlers, keyed by
 * (handler_type, handler_version). Workers resolve handlers here; unknown
 * combinations are permanent failures (no arbitrary code execution).
 */
export class HandlerRegistry {
  private readonly handlers = new Map<string, TaskHandler>();

  register(
    handlerType: string,
    handlerVersion: string,
    handler: TaskHandler,
  ): void {
    const key = `${handlerType}:${handlerVersion}`;

    if (this.handlers.has(key)) {
      throw new Error(`Handler ${key} is already registered`);
    }

    this.handlers.set(key, handler);
  }

  resolve(handlerType: string, handlerVersion: string): TaskHandler | null {
    return this.handlers.get(`${handlerType}:${handlerVersion}`) ?? null;
  }

  list(): readonly string[] {
    return [...this.handlers.keys()];
  }
}
