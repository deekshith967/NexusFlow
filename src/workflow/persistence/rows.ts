export interface WorkflowRow {
  readonly workflow_id: string;
  readonly tenant_id: string;
  readonly name: string;
  readonly description: string | null;
  readonly status: string;
  readonly created_at: string;
  readonly updated_at: string;
  readonly archived_at: string | null;
}

import { WorkflowVersionState } from '../domain/workflow-version-state';

export interface WorkflowVersionRow {
  readonly workflow_version_id: string;
  readonly workflow_id: string;
  readonly tenant_id: string;
  readonly version_number: number;
  readonly state: WorkflowVersionState;
  readonly definition_hash: string | null;
  readonly created_at: string;
  readonly published_at: string | null;
  readonly retired_at: string | null;
}

export interface TaskDefinitionRow {
  readonly task_definition_id: string;
  readonly workflow_version_id: string;
  readonly task_key: string;
  readonly handler_type: string;
  readonly handler_version: string;
  readonly configuration: Record<string, unknown>;
  readonly input_mapping: Record<string, unknown>;
  readonly output_mapping: Record<string, unknown>;
  readonly retry_policy: { maxAttempts: number; retryDelayMs?: number };
  readonly timeout_ms: number;
  readonly failure_policy: string;
  readonly created_at: string;
}

export interface TaskDependencyRow {
  readonly dependency_id: string;
  readonly workflow_version_id: string;
  readonly upstream_task_definition_id: string;
  readonly downstream_task_definition_id: string;
  readonly created_at: string;
}

export interface VersionDefinition {
  readonly version: WorkflowVersionRow;
  readonly workflow: WorkflowRow | null;
  readonly tasks: TaskDefinitionRow[];
  readonly dependencies: TaskDependencyRow[];
}

export interface TaskExecutionRow {
  readonly task_execution_id: string;
  readonly workflow_run_id: string;
  readonly task_definition_id: string;
  readonly task_key: string;
  readonly state: string;
  readonly resolved_input: Record<string, unknown> | null;
  readonly output_payload: unknown;
  readonly failure_code: string | null;
  readonly failure_message: string | null;
  readonly attempt_count: number;
  readonly next_attempt_at: string | null;
  readonly lease_owner: string | null;
  readonly lease_expires_at: string | null;
  readonly fencing_token: number;
  readonly claimed_at: string | null;
  readonly started_at: string | null;
  readonly completed_at: string | null;
  readonly last_attempt_id: string | null;
  readonly created_at: string;
  readonly updated_at: string;
}

export interface TaskAttemptRow {
  readonly task_attempt_id: string;
  readonly task_execution_id: string;
  readonly attempt_number: number;
  readonly state: string;
  readonly worker_id: string | null;
  readonly scheduler_id: string | null;
  readonly fencing_token: number;
  readonly dispatch_id: string;
  readonly started_at: string | null;
  readonly finished_at: string | null;
  readonly timeout_at: string | null;
  readonly result_payload: unknown;
  readonly failure_code: string | null;
  readonly failure_message: string | null;
  readonly created_at: string;
}

export interface WorkflowRunRow {
  readonly workflow_run_id: string;
  readonly tenant_id: string;
  readonly workflow_id: string;
  readonly workflow_version_id: string;
  readonly state: string;
  readonly idempotency_key: string | null;
  readonly input_payload: Record<string, unknown> | null;
  readonly output_payload: unknown;
  readonly correlation_id: string;
  readonly created_at: string;
  readonly started_at: string | null;
  readonly completed_at: string | null;
  readonly failure_code: string | null;
  readonly failure_message: string | null;
  readonly cancellation_requested_at: string | null;
}
