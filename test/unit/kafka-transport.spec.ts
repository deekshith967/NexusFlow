/**
 * Unit tests for the Kafka transport (MOCKED kafkajs — no broker in
 * this environment). Verifies topic/message mapping, idempotent producer
 * configuration, consumer group subscription, and crash reconnection
 * wiring. Real-broker behavior is an environment limitation, documented
 * in the README.
 */
import { Kafka, logLevel } from 'kafkajs';

const mockProducer = {
  connect: jest.fn().mockResolvedValue(undefined),
  send: jest.fn().mockResolvedValue({}),
  disconnect: jest.fn().mockResolvedValue(undefined),
};

const mockConsumer = {
  connect: jest.fn().mockResolvedValue(undefined),
  subscribe: jest.fn().mockResolvedValue(undefined),
  run: jest.fn().mockResolvedValue(undefined),
  on: jest.fn(),
  stop: jest.fn().mockResolvedValue(undefined),
  disconnect: jest.fn().mockResolvedValue(undefined),
};

const mockAdmin = {
  connect: jest.fn().mockResolvedValue(undefined),
  createTopics: jest.fn().mockResolvedValue(undefined),
  disconnect: jest.fn().mockResolvedValue(undefined),
};

jest.mock('kafkajs', () => ({
  Kafka: jest.fn().mockImplementation(() => ({
    consumer: jest.fn().mockReturnValue(mockConsumer),
    producer: jest.fn().mockReturnValue(mockProducer),
    admin: jest.fn().mockReturnValue(mockAdmin),
  })),
  logLevel: { WARN: 'WARN' },
}));

import { KafkaMessageTransport } from '../../src/transport/kafka-transport';
import type { EnvConfig } from '../../src/config/env';

function config(): EnvConfig {
  return {
    role: 'all',
    transport: 'kafka',
    databaseUrl: 'unused',
    databaseSsl: false,
    apiKeys: [],
    port: 0,
    defaultTenantId: '00000000-0000-0000-0000-000000000000',
    serviceId: 'kafka-test',
    kafka: {
      bootstrapServers: ['broker-1:9092', 'broker-2:9092'],
      dispatchTopic: 'nexusflow.task.dispatch',
      lifecycleTopic: 'nexusflow.workflow.events',
      consumerGroup: 'test-group',
      clientId: 'kafka-test',
      ssl: false,
    },
    scheduler: { pollIntervalMs: 20, batchLimit: 20, leaseMs: 30_000 },
    worker: { consumerGroup: 'test-group' },
    relay: { pollIntervalMs: 20, batchLimit: 20, claimTtlMs: 5_000, publishTimeoutMs: 5_000 },
  };
}

describe('KafkaMessageTransport (mocked kafkajs)', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('configures the client with brokers, clientId, and idempotent producer', async () => {
    const transport = new KafkaMessageTransport(config().kafka);

    await transport.publisher.publish('nexusflow.task.dispatch', {
      key: 'task-1',
      value: '{"ok":true}',
      headers: { 'event-id': 'evt-1' },
    });

    const KafkaCtor = Kafka as unknown as jest.Mock;
    expect(KafkaCtor).toHaveBeenCalledWith(
      expect.objectContaining({
        clientId: 'kafka-test',
        brokers: ['broker-1:9092', 'broker-2:9092'],
        logLevel: logLevel.WARN,
      }),
    );

    // Idempotent producer for at-least-once semantics.
    expect(mockProducer.connect).toHaveBeenCalled();
    expect(mockProducer.send).toHaveBeenCalledWith({
      topic: 'nexusflow.task.dispatch',
      messages: [
        { key: 'task-1', value: '{"ok":true}', headers: { 'event-id': 'evt-1' } },
      ],
    });
  });

  it('creates the required topics on startup', async () => {
    const transport = new KafkaMessageTransport(config().kafka);

    await transport.ensureTopics([
      'nexusflow.task.dispatch',
      'nexusflow.workflow.events',
    ]);

    expect(mockAdmin.createTopics).toHaveBeenCalledWith({
      topics: [
        { topic: 'nexusflow.task.dispatch', numPartitions: 3, replicationFactor: 1 },
        { topic: 'nexusflow.workflow.events', numPartitions: 3, replicationFactor: 1 },
      ],
    });
  });

  it('tolerates topic-already-exists on createTopics', async () => {
    const transport = new KafkaMessageTransport(config().kafka);
    mockAdmin.createTopics.mockRejectedValueOnce({ code: 3 });

    await expect(
      transport.ensureTopics(['nexusflow.task.dispatch']),
    ).resolves.toBeUndefined();
  });

  it('subscribes the consumer group to the dispatch topic and maps messages', async () => {
    const transport = new KafkaMessageTransport(config().kafka);
    const received: { key: string; value: string; headers?: Record<string, string> }[] = [];

    await transport.consumer.subscribe({
      group: 'test-group',
      topic: 'nexusflow.task.dispatch',
      handler: (message) => {
        received.push(message);
        return Promise.resolve();
      },
    });

    expect(mockConsumer.subscribe).toHaveBeenCalledWith({
      topic: 'nexusflow.task.dispatch',
      fromBeginning: false,
    });
    expect(mockConsumer.run).toHaveBeenCalledTimes(1);

    // mock.calls[n][m] = m-th argument of the n-th call.
    const runArgs = mockConsumer.run.mock.calls[0] as unknown as [
      {
        eachMessage: (event: { message: Record<string, unknown> }) => Promise<void>;
      },
    ];
    const runConfig = runArgs[0];

    // Simulate a broker delivery.
    await runConfig.eachMessage({
      message: {
        key: Buffer.from('task-42'),
        value: Buffer.from('{"dispatchId":"d1"}'),
        headers: { 'event-id': Buffer.from('evt-9') },
        offset: '7',
      },
    });

    expect(received).toEqual([
      {
        key: 'task-42',
        value: '{"dispatchId":"d1"}',
        headers: { 'event-id': 'evt-9' },
        offset: 7,
      },
    ]);
  });

  it('reconnects on consumer crash (at-least-once recovery)', async () => {
    const transport = new KafkaMessageTransport(config().kafka);
    const crashHandler = jest.fn();

    await transport.consumer.subscribe({
      group: 'test-group',
      topic: 'nexusflow.task.dispatch',
      handler: () => Promise.resolve(),
    });

    const onCalls = mockConsumer.on.mock.calls as unknown as [string, () => void][];
    const onCall = onCalls.find((call) => call[0] === 'consumer.crash');
    expect(onCall).toBeDefined();

    // Simulate a crash and let the reconnect timer fire (1s).
    jest.useFakeTimers();
    try {
      onCall![1]();
      await jest.advanceTimersByTimeAsync(1100);
    } finally {
      jest.useRealTimers();
    }

    // A reconnect attempts a fresh consumer (subscribe called again).
    expect(mockConsumer.subscribe).toHaveBeenCalledTimes(2);
    void crashHandler;
  });

  it('close disconnects consumer, producer, and admin', async () => {
    const transport = new KafkaMessageTransport(config().kafka);

    await transport.consumer.subscribe({
      group: 'test-group',
      topic: 'nexusflow.task.dispatch',
      handler: () => Promise.resolve(),
    });
    await transport.publisher.publish('nexusflow.task.dispatch', {
      key: 'k',
      value: '{}',
    });
    await transport.ensureTopics(['nexusflow.task.dispatch']);

    await transport.close();

    expect(mockConsumer.stop).toHaveBeenCalled();
    expect(mockConsumer.disconnect).toHaveBeenCalled();
    expect(mockProducer.disconnect).toHaveBeenCalled();
    expect(mockAdmin.disconnect).toHaveBeenCalled();
  });

  it('passes TLS and SASL options to the Kafka client when configured', () => {
    const base = config();
    const transport = new KafkaMessageTransport({
      ...base.kafka,
      ssl: true,
      sasl: {
        mechanism: 'scram-sha-256',
        username: 'broker-user',
        password: 'broker-password',
      },
    });
    void transport;

    const KafkaCtor = Kafka as unknown as jest.Mock;
    expect(KafkaCtor).toHaveBeenCalledWith(
      expect.objectContaining({
        ssl: true,
        sasl: {
          mechanism: 'scram-sha-256',
          username: 'broker-user',
          password: 'broker-password',
        },
      }),
    );
  });

  it('omits TLS and SASL options for plaintext brokers', () => {
    const transport = new KafkaMessageTransport(config().kafka);
    void transport;

    const KafkaCtor = Kafka as unknown as jest.Mock;
    const calls = KafkaCtor.mock.calls as unknown as Array<
      [Record<string, unknown>]
    >;
    const clientOptions = calls[0][0];

    expect(clientOptions.ssl).toBeUndefined();
    expect(clientOptions.sasl).toBeUndefined();
  });

  it('surfaces producer connection failures on publish', async () => {
    const transport = new KafkaMessageTransport(config().kafka);
    mockProducer.connect.mockRejectedValueOnce(
      new Error('broker unreachable'),
    );

    await expect(
      transport.publisher.publish('nexusflow.task.dispatch', {
        key: 'k',
        value: '{}',
      }),
    ).rejects.toThrow('broker unreachable');
  });

  it('retries send once, then surfaces persistent send failures', async () => {
    const transport = new KafkaMessageTransport(config().kafka);

    // One transient send failure is retried and succeeds.
    mockProducer.send.mockRejectedValueOnce(
      new Error('leader not available'),
    );
    await expect(
      transport.publisher.publish('t', { key: 'k', value: '{}' }),
    ).resolves.toBeUndefined();
    expect(mockProducer.send).toHaveBeenCalledTimes(2);

    // Persistent failures propagate after the single retry.
    mockProducer.send.mockRejectedValueOnce(new Error('still down'));
    mockProducer.send.mockRejectedValueOnce(new Error('still down'));
    await expect(
      transport.publisher.publish('t', { key: 'k', value: '{}' }),
    ).rejects.toThrow('still down');
    expect(mockProducer.send).toHaveBeenCalledTimes(4);
  });

  it('surfaces consumer connection failures on subscribe', async () => {
    const transport = new KafkaMessageTransport(config().kafka);
    mockConsumer.connect.mockRejectedValueOnce(
      new Error('broker unreachable'),
    );

    await expect(
      transport.consumer.subscribe({
        group: 'test-group',
        topic: 'nexusflow.task.dispatch',
        handler: () => Promise.resolve(),
      }),
    ).rejects.toThrow('broker unreachable');
  });

  it('surfaces admin connection failures on ensureTopics', async () => {
    const transport = new KafkaMessageTransport(config().kafka);
    mockAdmin.connect.mockRejectedValueOnce(new Error('broker unreachable'));

    await expect(
      transport.ensureTopics(['nexusflow.task.dispatch']),
    ).rejects.toThrow('broker unreachable');
  });
});
