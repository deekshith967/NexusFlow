import { Controller, Get, Inject, Res } from '@nestjs/common';
import type { Response } from 'express';

import { APP_CONFIG } from '../../config/config.module';
import { EnvConfig } from '../../config/env';
import { DatabaseService } from '../../database/database.service';

export interface ReadinessBody {
  readonly status: 'ready' | 'not_ready';
  readonly service: 'nexusflow';
  readonly role: string;
  readonly checks: {
    readonly database: 'ok' | 'down';
    readonly transport: string;
    readonly defaultTenantId: string;
  };
}

@Controller('health')
export class HealthController {
  constructor(
    private readonly database: DatabaseService,
    @Inject(APP_CONFIG) private readonly config: EnvConfig,
  ) {}

  @Get()
  liveness() {
    return { status: 'ok', service: 'nexusflow', role: this.config.role };
  }

  /**
   * Readiness: 200 when the database is reachable, 503 otherwise, so
   * orchestrators (container health checks, load balancers, Render)
   * remove unready instances from rotation.
   */
  @Get('ready')
  async readiness(
    @Res({ passthrough: true }) res: Response,
  ): Promise<ReadinessBody> {
    let database: 'ok' | 'down' = 'ok';

    try {
      await this.database.checkHealth();
    } catch {
      database = 'down';
    }

    const ready = database === 'ok';

    res.status(ready ? 200 : 503);

    return {
      status: ready ? 'ready' : 'not_ready',
      service: 'nexusflow',
      role: this.config.role,
      checks: {
        database,
        transport: this.config.transport,
        defaultTenantId: this.config.defaultTenantId,
      },
    };
  }
}
