import { Test } from '@nestjs/testing';
import { Pool } from 'pg';
import { AppModule } from '../src/app.module';

describe('AppModule', () => {
  it('compiles the root application module and shuts down the database pool', async () => {
    const pool = { end: jest.fn().mockResolvedValue(undefined) };
    const module = await Test.createTestingModule({ imports: [AppModule] })
      .overrideProvider(Pool)
      .useValue(pool)
      .compile();

    expect(module).toBeDefined();
    await module.close();

    expect(pool.end).toHaveBeenCalledTimes(1);
  });
});
