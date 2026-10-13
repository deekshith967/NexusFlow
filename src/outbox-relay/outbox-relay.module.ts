import { Module } from '@nestjs/common';

import { ConfigModule } from '../config/config.module';
import { DatabaseModule } from '../database/database.module';
import { TransportModule } from '../transport/transport.module';
import { OutboxRelayService } from './outbox-relay.service';

@Module({
  imports: [ConfigModule, DatabaseModule, TransportModule],
  providers: [OutboxRelayService],
  exports: [OutboxRelayService],
})
export class OutboxRelayModule {}
