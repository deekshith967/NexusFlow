import { EventEmitter } from 'node:events';

import type {
  MessageTransport,
  TransportConsumer,
  TransportMessage,
  TransportPublisher,
} from './types';

interface StoredMessage extends TransportMessage {
  readonly topic: string;
  readonly sequence: number;
}

interface GroupSubscription {
  group: string;
  topic: string;
  handler: (message: TransportMessage) => Promise<void>;
  /** Sequence position per (group, topic); each message delivered once. */
  cursor: number;
  busy: boolean;
}

/**
 * In-process message broker (TEST/LOCAL DEMO DOUBLE — not production).
 *
 * Emulates at-least-once semantics with consumer groups: each message is
 * delivered once per group (like a single-partition topic) and can be
 * redelivered by `requeueLast()` to simulate broker redelivery. It shares
 * no state across processes; multi-process deployments must use Kafka.
 */
export class MemoryMessageTransport implements MessageTransport {
  readonly kind = 'memory' as const;
  readonly publisher: TransportPublisher;
  readonly consumer: TransportConsumer;

  private readonly topics = new Set<string>();
  private readonly bus = new EventEmitter();
  private readonly subscriptions: GroupSubscription[] = [];
  private sequence = 0;
  private closed = false;
  /** Fault injection: number of publishes that must fail. */
  failNextPublishes = 0;
  /** Fault injection: when true, publishes reject until cleared. */
  outage = false;

  /** All messages ever published (for test assertions). */
  get published(): readonly StoredMessage[] {
    return this.messages;
  }

  private readonly messages: StoredMessage[] = [];

  constructor() {
    this.bus.setMaxListeners(0);
    this.publisher = {
      publish: (topic, message) => this.doPublish(topic, message),
      isReady: () => Promise.resolve(!this.closed),
      close: () => Promise.resolve(),
    };

    this.consumer = {
      subscribe: (options) => {
        this.subscriptions.push({
          group: options.group,
          topic: options.topic,
          handler: options.handler,
          cursor: 0,
          busy: false,
        });
        this.bus.emit('subscribe');
        return Promise.resolve();
      },
      close: () => Promise.resolve(),
    };
  }

  private doPublish(topic: string, message: TransportMessage): Promise<void> {
    if (this.closed) {
      throw new Error('transport is closed');
    }

    if (this.outage) {
      throw new Error('memory transport outage (fault injection)');
    }

    if (this.failNextPublishes > 0) {
      this.failNextPublishes -= 1;
      throw new Error('memory transport transient failure (fault injection)');
    }

    const stored: StoredMessage = {
      ...message,
      topic,
      sequence: this.sequence++,
    };

    this.messages.push(stored);

    // Deliver asynchronously (like a broker push); never block the publisher.
    setImmediate(() => this.deliverNext());

    return Promise.resolve();
  }

  private deliverNext(): void {
    // One in-flight delivery per subscription; messages are delivered in
    // sequence order per (group, topic).
    for (const sub of this.subscriptions) {
      if (sub.busy) {
        continue;
      }

      const next = this.messages.find(
        (m) => m.topic === sub.topic && m.sequence >= sub.cursor,
      );

      if (!next) {
        continue;
      }

      sub.busy = true;
      sub.cursor = next.sequence + 1;

      void (async () => {
        try {
          await sub.handler({
            key: next.key,
            value: next.value,
            headers: next.headers,
            offset: next.sequence,
          });
        } catch {
          // Handler rejected: requeue this message for redelivery
          // (at-least-once). Consumers must be idempotent.
          sub.cursor = next.sequence;
        } finally {
          sub.busy = false;
          // Deliver any message queued behind the just-finished one.
          this.deliverNext();
        }
      })();
    }
  }

  /** Fault injection: force redelivery of the last message of a group. */
  requeueLast(group: string, topic: string): void {
    const sub = this.subscriptions.find(
      (s) => s.group === group && s.topic === topic,
    );

    if (sub && sub.cursor > 0) {
      sub.cursor -= 1;
      setImmediate(() => this.deliverNext());
    }
  }

  /** Test helper: wait until no subscription is busy and all caught up. */
  async drain(timeoutMs = 5000): Promise<void> {
    const deadline = Date.now() + timeoutMs;

    for (;;) {
      const busy = this.subscriptions.some((s) => s.busy);
      const pending = this.subscriptions.some((sub) =>
        this.messages.some(
          (m) => m.topic === sub.topic && m.sequence >= sub.cursor,
        ),
      );

      if (!busy && !pending) {
        return;
      }

      if (Date.now() > deadline) {
        throw new Error('memory transport drain timed out');
      }

      await new Promise((resolve) => setImmediate(resolve));
    }
  }

  ensureTopics(topics: readonly string[]): Promise<void> {
    for (const topic of topics) {
      this.topics.add(topic);
    }

    return Promise.resolve();
  }

  close(): Promise<void> {
    this.closed = true;
    this.bus.removeAllListeners();
    return Promise.resolve();
  }
}
