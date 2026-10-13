/**
 * Message transport abstraction for dispatch and lifecycle events.
 *
 * Production uses the Kafka implementation (contract: Kafka is the
 * asynchronous dispatch/event transport). The in-memory implementation is
 * a test/local-demo double for environments without a broker; it is never
 * an authoritative state store — PostgreSQL remains authoritative and
 * consumers must tolerate duplicate delivery (at-least-once).
 */

export interface TransportMessage {
  readonly key: string;
  readonly value: string;
  readonly headers?: Record<string, string>;
  /** Broker-assigned position (Kafka offset); informational only. */
  readonly offset?: number;
}

export interface TransportPublisher {
  publish(topic: string, message: TransportMessage): Promise<void>;
  isReady(): Promise<boolean>;
  close(): Promise<void>;
}

export interface TransportConsumer {
  subscribe(options: {
    group: string;
    topic: string;
    handler: (message: TransportMessage) => Promise<void>;
  }): Promise<void>;
  close(): Promise<void>;
}

export interface MessageTransport {
  readonly kind: 'kafka' | 'memory';
  readonly publisher: TransportPublisher;
  readonly consumer: TransportConsumer;
  /** Create topics if the broker allows it (Kafka); no-op otherwise. */
  ensureTopics(topics: readonly string[]): Promise<void>;
  close(): Promise<void>;
}
