import { Global, Module } from '@nestjs/common';

import { loadEnvConfig, EnvConfig } from './env';

export const APP_CONFIG = 'APP_CONFIG';

export function appConfigProvider() {
  return {
    provide: APP_CONFIG,
    useFactory: (): EnvConfig => loadEnvConfig(process.env),
  };
}

@Global()
@Module({
  providers: [appConfigProvider()],
  exports: [APP_CONFIG],
})
export class ConfigModule {}
