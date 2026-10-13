import { randomUUID } from 'node:crypto';
import type { PoolClient } from 'pg';

import { OUTBOX_SCHEMA_VERSION, OutboxEventInput } from './events';

export interface OutboxEventRow {
  readonly outbox_event_id: string;
  readonly event_id: string;
  readonly event_type: string;
  readonly schema_version: string;
  readonly aggregate_type: string;
  readonly aggregate_id: string;
  readonly tenant_id: string | null;
  readonly workflow_id: string | null;
  readonly workflow_version_id: string | null;
  readonly workflow_run_id: string | null;
  readonly task_execution_id: string | null;
  readonly task_attempt_id: string | null;
  readonly occurred_at: string;
  readonly correlation_id: string;
  readonly causation_id: string | null;
  readonly producer: string;
  readonly payload: Record<string, unknown>;
  readonly headers: Record<string, string> | null;
  readonly created_at: string;
  readonly published_at: string | null;
  readonly publish_attempts: number;
  readonly last_error: string | null;
  readonly claim_owner: string | null;
  readonly claim_expires_at: string | null;
}

const INSERT_SQL = `
  INSERT INTO outbox_events (
    outbox_event_id,
    event_id,
    event_type,
    schema_version,
    aggregate_type,
    aggregate_id,
    tenant_id,
    workflow_id,
    workflow_version_id,
    workflow_run_id,
    task_execution_id,
    task_attempt_id,
    occurred_at,
    correlation_id,
    causation_id,
    producer,
    payload,
    headers,
    created_at
  ) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
    now(), $13, $14, $15, $16, $17, now()
  )
  RETURNING event_id
`;

/**
 * Inserts an outbox event on the provided transactional client. Must be
 * called inside the same transaction as the state transition the event
 * describes (invariant 15). `occurred_at` and `created_at` use database
 * time so event ordering is consistent with lease comparisons.
 */
export async function insertOutboxEvent(
  client: PoolClient,
  input: OutboxEventInput,
): Promise<string> {
  const result = await client.query<{ event_id: string }>(INSERT_SQL, [
    randomUUID(),
    randomUUID(),
    input.eventType,
    OUTBOX_SCHEMA_VERSION,
    input.aggregateType,
    input.aggregateId,
    input.tenantId ?? null,
    input.workflowId ?? null,
    input.workflowVersionId ?? null,
    input.workflowRunId ?? null,
    input.taskExecutionId ?? null,
    input.taskAttemptId ?? null,
    input.correlationId,
    input.causationId ?? null,
    input.producer,
    JSON.stringify(input.payload),
    input.headers ? JSON.stringify(input.headers) : null,
  ]);

  return result.rows[0].event_id;
}
