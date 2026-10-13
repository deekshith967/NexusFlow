import { Controller, Get, Inject } from '@nestjs/common';

import { APP_CONFIG } from '../../config/config.module';
import { EnvConfig } from '../../config/env';
import { DatabaseService } from '../../database/database.service';

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

  @Get('ready')
  async readiness() {
    let database: 'ok' | 'down' = 'ok';

    try {
      await this.database.checkHealth();
    } catch {
      database = 'down';
    }

    const ready = database === 'ok';

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
