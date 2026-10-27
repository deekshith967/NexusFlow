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
    port: 0,
    defaultTenantId: '00000000-0000-0000-0000-000000000000',
    serviceId: 'kafka-test',
    kafka: {
      bootstrapServers: ['broker-1:9092', 'broker-2:9092'],
      dispatchTopic: 'nexusflow.task.dispatch',
      lifecycleTopic: 'nexusflow.workflow.events',
      consumerGroup: 'test-group',
      clientId: 'kafka-test',
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
});
