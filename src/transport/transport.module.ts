import { Global, Inject, Module, OnApplicationShutdown } from '@nestjs/common';

import { APP_CONFIG } from '../config/config.module';
import { EnvConfig } from '../config/env';
import { KafkaMessageTransport } from './kafka-transport';
import { MemoryMessageTransport } from './memory-transport';
import { MessageTransport } from './types';

export const MESSAGE_TRANSPORT = 'MESSAGE_TRANSPORT';

/**
 * Provides the message transport for this process.
 *
 * - `kafka` (default): production transport.
 * - `memory`: in-process double for tests and single-process local demos
 *   without a broker. Never used for cross-process communication.
 */
export function createMessageTransport(config: EnvConfig): MessageTransport {
  if (config.transport === 'memory') {
    return new MemoryMessageTransport();
  }

  return new KafkaMessageTransport(config.kafka);
}

export class TransportLifecycle implements OnApplicationShutdown {
  constructor(
    @Inject(MESSAGE_TRANSPORT) private readonly transport: MessageTransport,
  ) {}

  async onApplicationShutdown(): Promise<void> {
    await this.transport.close();
  }
}

@Global()
@Module({
  providers: [
    {
      provide: MESSAGE_TRANSPORT,
      inject: [APP_CONFIG],
      useFactory: (config: EnvConfig): MessageTransport =>
        createMessageTransport(config),
    },
    TransportLifecycle,
  ],
  exports: [MESSAGE_TRANSPORT],
})
export class TransportModule {}
