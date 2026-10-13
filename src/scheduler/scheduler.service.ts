import {
  Inject,
  Injectable,
  Logger,
  OnApplicationShutdown,
  OnModuleInit,
} from '@nestjs/common';

import { APP_CONFIG } from '../config/config.module';
import { EnvConfig } from '../config/env';
import { DatabaseService } from '../database/database.service';
import { claimBatch } from './claim';

/**
 * Scheduler process (contract: durable task claiming).
 *
 * Multiple scheduler instances may run concurrently. Eligibility is
 * enforced by PostgreSQL: FOR UPDATE SKIP LOCKED selection plus
 * conditional updates mean only one scheduler can validly claim a task,
 * and each claim atomically creates the lease, fencing token, attempt,
 * and dispatch intent (D.6).
 *
 * Recovery of claimed-but-undispatched work happens via the outbox
 * relay (the dispatch intent is a durable outbox row) and via lease
 * expiry reclamation inside the claim query.
 */
@Injectable()
export class SchedulerService
  implements OnModuleInit, OnApplicationShutdown
{
  private readonly logger = new Logger(SchedulerService.name);
  private timer: NodeJS.Timeout | null = null;
  private inFlight = false;
  private shutdownRequested = false;
  readonly owner: string;
  readonly schedulerId: string;

  /** Claims since startup (for health/diagnostics). */
  claimedCount = 0;
  lastClaimAt: Date | null = null;

  constructor(
    private readonly database: DatabaseService,
    @Inject(APP_CONFIG) private readonly config: EnvConfig,
  ) {
    this.owner = `sched-${config.serviceId}-${process.pid}`;
    this.schedulerId = this.owner;
  }

  private roleEnabled(): boolean {
    return this.config.role === 'scheduler' || this.config.role === 'all';
  }

  onModuleInit(): void {
    if (!this.roleEnabled()) {
      this.logger.log('Scheduler role not enabled; skipping startup');
      return;
    }

    // NOTE: intentionally NOT unref'd — in scheduler-role processes the
    // poll timer is what keeps the process alive; onApplicationShutdown
    // clears it during graceful shutdown.
    this.timer = setInterval(
      () => void this.scan().catch((error) => {
        this.logger.error(
          `Scheduler scan failed: ${(error as Error).message}`,
        );
      }),
      this.config.scheduler.pollIntervalMs,
    );

    void this.scan().catch(() => undefined);

    this.logger.log(`Scheduler ${this.owner} started`);
  }

  onApplicationShutdown(): void {
    this.shutdownRequested = true;

    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  /** One eligibility scan + claim cycle. Exposed for tests. */
  async scan(): Promise<number> {
    if (this.shutdownRequested || this.inFlight) {
      return 0;
    }

    this.inFlight = true;

    try {
      const claimed = await this.database.transaction((client) =>
        claimBatch(client, {
          batchSize: this.config.scheduler.batchLimit,
          leaseOwner: this.owner,
          schedulerId: this.schedulerId,
          leaseMs: this.config.scheduler.leaseMs,
        }),
      );

      if (claimed.length > 0) {
        this.claimedCount += claimed.length;
        this.lastClaimAt = new Date();
      }

      return claimed.length;
    } finally {
      this.inFlight = false;
    }
  }
}
