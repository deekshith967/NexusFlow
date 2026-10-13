import { Kafka, logLevel } from 'kafkajs';

import { KafkaConfig } from '../config/env';
import type {
  MessageTransport,
  TransportConsumer,
  TransportMessage,
  TransportPublisher,
} from './types';

/**
 * Kafka transport (production). Publication is at-least-once:
 * - idempotent producer with acks=all
 * - consumer group with server-side offset tracking; offsets are NOT
 *   authoritative task state (contract section E) — PostgreSQL is.
 *
 * Consumers must tolerate duplicate delivery and validate task state,
 * attempt, and fencing token before executing or reporting.
 */
export class KafkaMessageTransport implements MessageTransport {
  readonly kind = 'kafka' as const;
  readonly publisher: TransportPublisher;
  readonly consumer: TransportConsumer;

  private readonly kafka: Kafka;
  private producer: ReturnType<Kafka['producer']> | null = null;
  private admin: ReturnType<Kafka['admin']> | null = null;
  private consumerInstance: ReturnType<Kafka['consumer']> | null = null;
  private producerReady = false;
  private closed = false;
  private reconnectTimer: NodeJS.Timeout | null = null;

  constructor(private readonly config: KafkaConfig) {
    this.kafka = new Kafka({
      clientId: config.clientId,
      brokers: config.bootstrapServers,
      logLevel: logLevel.WARN,
      retry: {
        initialRetryTime: 200,
        retries: 10,
      },
    });

    this.publisher = {
      publish: (topic, message) => this.publishToKafka(topic, message),
      isReady: () => Promise.resolve(this.producerReady),
      close: () => this.closeProducer(),
    };

    this.consumer = {
      subscribe: (options) => this.startConsumer(options.group, options.topic, options.handler),
      close: async () => this.closeConsumer(),
    };
  }

  // -------------------------------------------------------------------------
  // Publisher
  // -------------------------------------------------------------------------

  private async getProducer(): Promise<ReturnType<Kafka['producer']>> {
    if (this.producer && this.producerReady) {
      return this.producer;
    }

    if (!this.producer) {
      this.producer = this.kafka.producer({
        idempotent: true,
        allowAutoTopicCreation: false,
      });
    }

    await this.producer.connect();
    this.producerReady = true;
    return this.producer;
  }

  private async publishToKafka(topic: string, message: TransportMessage): Promise<void> {
    const producer = await this.getProducer();

    try {
      await producer.send({
        topic,
        messages: [
          {
            key: message.key,
            value: message.value,
            headers: message.headers
              ? Object.fromEntries(
                  Object.entries(message.headers).map(([k, v]) => [k, v]),
                )
              : undefined,
          },
        ],
      });
    } catch {
      // Producer may have disconnected; reconnect once and retry.
      this.producerReady = false;
      await this.closeProducer();
      const producer2 = await this.getProducer();
      await producer2.send({
        topic,
        messages: [
          {
            key: message.key,
            value: message.value,
            headers: message.headers
              ? Object.fromEntries(
                  Object.entries(message.headers).map(([k, v]) => [k, v]),
                )
              : undefined,
          },
        ],
      });
    }
  }

  // -------------------------------------------------------------------------
  // Topics / admin
  // -------------------------------------------------------------------------

  private async getAdmin(): Promise<ReturnType<Kafka['admin']>> {
    if (this.admin) {
      return this.admin;
    }

    const admin = this.kafka.admin();
    await admin.connect();
    this.admin = admin;
    return admin;
  }

  async ensureTopics(topics: readonly string[]): Promise<void> {
    const admin = await this.getAdmin();
    const spec = topics.map((topic) => ({
      topic,
      numPartitions: 3,
      replicationFactor: 1,
    }));

    try {
      await admin.createTopics({ topics: spec });
    } catch (error) {
      // TopicAlreadyExists (3) and NotController (-1) are expected/handled.
      const code = (error as { code?: number })?.code;

      if (code !== 3 && code !== -1) {
        throw error;
      }
    }
  }

  // -------------------------------------------------------------------------
  // Consumer
  // -------------------------------------------------------------------------

  private async startConsumer(
    group: string,
    topic: string,
    handler: (message: TransportMessage) => Promise<void>,
  ): Promise<void> {
    const consumer = this.kafka.consumer({
      groupId: group,
      maxBytesPerPartition: 512 * 1024,
    });

    await consumer.connect();
    this.consumerInstance = consumer;

    consumer.on('consumer.crash', () => {
      void this.reconnectConsumer(group, topic, handler);
    });

    await consumer.subscribe({ topic, fromBeginning: false });

    await consumer.run({
      eachMessage: async ({ message }) => {
        await handler({
          key: message.key ? message.key.toString() : '',
          value: message.value ? message.value.toString() : '',
          headers: message.headers
            ? Object.fromEntries(
                Object.entries(message.headers).map(([k, v]) => [
                  k,
                  v === null || v === undefined
                    ? ''
                    : (v as string).toString(),
                ]),
              )
            : undefined,
          offset: message.offset ? Number(message.offset) : undefined,
        });
      },
    });
  }

  /**
   * Rebalance/network failure: reconnect and resume from the last
   * committed offset (at-least-once; duplicates are tolerated).
   */
  private async reconnectConsumer(
    group: string,
    topic: string,
    handler: (message: TransportMessage) => Promise<void>,
  ): Promise<void> {
    if (this.closed || this.reconnectTimer) {
      return;
    }

    await this.closeConsumer();

    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      void this.startConsumer(group, topic, handler).catch(() => undefined);
    }, 1000);
  }

  private async closeConsumer(): Promise<void> {
    const consumer = this.consumerInstance;
    this.consumerInstance = null;

    if (consumer) {
      await consumer.stop().catch(() => undefined);
      await consumer.disconnect().catch(() => undefined);
    }
  }

  private async closeProducer(): Promise<void> {
    const producer = this.producer;
    this.producer = null;
    this.producerReady = false;

    if (producer) {
      await producer.disconnect().catch(() => undefined);
    }
  }

  private async closeAdmin(): Promise<void> {
    const admin = this.admin;
    this.admin = null;

    if (admin) {
      await admin.disconnect().catch(() => undefined);
    }
  }

  async close(): Promise<void> {
    this.closed = true;

    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }

    await this.closeConsumer();
    await this.closeProducer();
    await this.closeAdmin();
  }
}
