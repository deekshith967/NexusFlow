import { Test } from '@nestjs/testing';
import { AppModule } from '../src/app.module';

describe('AppModule', () => {
  it('compiles the root application module', async () => {
    const module = await Test.createTestingModule({ imports: [AppModule] }).compile();

    expect(module).toBeDefined();
    await module.close();
  });
});
