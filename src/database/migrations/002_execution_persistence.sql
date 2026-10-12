-- 002_execution_persistence.sql
-- Execution persistence for Phase 1: workflow_runs, task_executions, task_attempts, outbox_events
-- Follows docs/architecture/final-architecture-contract.md Section C.5-C.8.
--
-- Invariants enforced directly by schema (constraints/indexes) are marked below.
-- Invariants requiring conditional application transactions are documented as deferred;
-- the migration alone cannot enforce them without evaluating current state/ownership.

-- ============================================================================
-- workflow_runs: one execution of one immutable workflow version (C.5)
-- ============================================================================

CREATE TABLE workflow_runs (
    workflow_run_id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    workflow_id UUID NOT NULL,
    workflow_version_id UUID NOT NULL,
    state TEXT NOT NULL,
    idempotency_key TEXT,
    input_payload JSONB,
    output_payload JSONB,
    correlation_id TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    failure_code TEXT,
    failure_message TEXT,
    cancellation_requested_at TIMESTAMPTZ,

    CONSTRAINT workflow_runs_workflow_fk
        FOREIGN KEY (workflow_id)
        REFERENCES workflows (workflow_id),

    CONSTRAINT workflow_runs_workflow_version_fk
        FOREIGN KEY (workflow_version_id)
        REFERENCES workflow_versions (workflow_version_id),

    CONSTRAINT workflow_runs_state_check
        CHECK (state IN (
            'CREATED',
            'RUNNING',
            'SUCCEEDED',
            'FAILED',
            'CANCEL_REQUESTED',
            'CANCELLED'
        )),

    CONSTRAINT workflow_runs_idempotency_key_nonempty
        CHECK (
            idempotency_key IS NULL
            OR length(trim(idempotency_key)) > 0
        ),

    CONSTRAINT workflow_runs_completed_at_check
        CHECK (
            completed_at IS NULL
            OR state IN ('SUCCEEDED', 'FAILED', 'CANCELLED')
        )
);

-- Tenant/workflow/idempotency uniqueness when idempotency_key is present (C.5).
-- Partial unique index enforces the contract without blocking multiple NULL keys.
CREATE UNIQUE INDEX workflow_runs_tenant_workflow_idempotency_unique
    ON workflow_runs (tenant_id, workflow_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE INDEX workflow_runs_tenant_created_idx
    ON workflow_runs (tenant_id, created_at);

CREATE INDEX workflow_runs_workflow_state_idx
    ON workflow_runs (workflow_id, state);

CREATE INDEX workflow_runs_version_idx
    ON workflow_runs (workflow_version_id);

CREATE INDEX workflow_runs_state_created_idx
    ON workflow_runs (state, created_at);

-- Terminal run-state immutability (SUCCEEDED/FAILED/CANCELLED) is enforced by
-- conditional application transactions (D.11/D.12/D.14, invariant 13) that verify
-- the run is RUNNING before transitioning to terminal. A pure CHECK cannot
-- prevent UPDATEs that mutate a terminal row without comparing old/new state,
-- so this migration does not add a trigger-based guard; enforcement is
-- deferred to the transactional completion logic that holds the run row
-- FOR UPDATE and validates state before mutation.

-- ============================================================================
-- task_executions: one logical task instance per workflow run (C.6)
-- ============================================================================

CREATE TABLE task_executions (
    task_execution_id UUID PRIMARY KEY,
    workflow_run_id UUID NOT NULL,
    task_definition_id UUID NOT NULL,
    task_key TEXT NOT NULL,
    state TEXT NOT NULL,
    resolved_input JSONB,
    output_payload JSONB,
    failure_code TEXT,
    failure_message TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ,
    lease_owner TEXT,
    lease_expires_at TIMESTAMPTZ,
    fencing_token BIGINT NOT NULL DEFAULT 0,
    claimed_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    last_attempt_id UUID,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,

    CONSTRAINT task_executions_workflow_run_fk
        FOREIGN KEY (workflow_run_id)
        REFERENCES workflow_runs (workflow_run_id),

    CONSTRAINT task_executions_task_definition_fk
        FOREIGN KEY (task_definition_id)
        REFERENCES task_definitions (task_definition_id),

    CONSTRAINT task_executions_unique_per_run
        UNIQUE (workflow_run_id, task_definition_id),

    CONSTRAINT task_executions_task_key_nonempty
        CHECK (length(trim(task_key)) > 0),

    CONSTRAINT task_executions_state_check
        CHECK (state IN (
            'WAITING',
            'READY',
            'CLAIMED',
            'RUNNING',
            'RETRY_WAIT',
            'SUCCEEDED',
            'FAILED',
            'SKIPPED',
            'BLOCKED',
            'CANCELLED'
        )),

    CONSTRAINT task_executions_attempt_count_nonnegative
        CHECK (attempt_count >= 0),

    CONSTRAINT task_executions_fencing_token_nonnegative
        CHECK (fencing_token >= 0),

    -- Lease fields must be consistent: either no lease (both NULL) or a
    -- current lease (both present). Expired but not yet reclaimed rows
    -- keep both fields populated; recovery reclaims by incrementing the
    -- fencing token and updating lease fields in a conditional transaction.
    CONSTRAINT task_executions_lease_consistency
        CHECK (
            (lease_owner IS NULL AND lease_expires_at IS NULL)
            OR (lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)
        ),

    CONSTRAINT task_executions_next_attempt_consistency
        CHECK (
            next_attempt_at IS NULL
            OR state = 'RETRY_WAIT'
        )
);

CREATE INDEX task_executions_state_next_attempt_idx
    ON task_executions (state, next_attempt_at);

CREATE INDEX task_executions_workflow_run_state_idx
    ON task_executions (workflow_run_id, state);

CREATE INDEX task_executions_lease_expires_idx
    ON task_executions (lease_expires_at);

CREATE INDEX task_executions_lease_owner_state_idx
    ON task_executions (lease_owner, state);

CREATE INDEX task_executions_workflow_run_task_key_idx
    ON task_executions (workflow_run_id, task_key);

-- Invariants deferred to conditional transactions:
-- - fencing_token increases on each ownership change (invariant 9) is enforced
--   by the scheduler claim transaction (D.6) that increments the token
--   atomically with lease assignment; a CHECK can only enforce monotonic
--   non-negativity, not per-row increment.
-- - At-most-one current lease (invariant 8) is the lease_consistency check
--   plus the transactional claim that ensures only one scheduler updates a
--   READY/RETRY_WAIT row via FOR UPDATE SKIP LOCKED.
-- - Terminal states (SUCCEEDED/FAILED/SKIPPED/BLOCKED/CANCELLED) cannot
--   become nonterminal in Phase 1 (invariant 13) is enforced by conditional
--   updates that validate current state before transitioning.
-- - last_attempt_id is a logical reference to task_attempts; no FK is added
--   to avoid a circular dependency. Integrity is maintained by the claim
--   transaction that creates the attempt and updates last_attempt_id atomically.

-- ============================================================================
-- task_attempts: concrete delivery and execution history (C.7)
-- ============================================================================

CREATE TABLE task_attempts (
    task_attempt_id UUID PRIMARY KEY,
    task_execution_id UUID NOT NULL,
    attempt_number INTEGER NOT NULL,
    state TEXT NOT NULL,
    worker_id TEXT,
    scheduler_id TEXT,
    fencing_token BIGINT NOT NULL,
    dispatch_id UUID NOT NULL,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    timeout_at TIMESTAMPTZ,
    result_payload JSONB,
    failure_code TEXT,
    failure_message TEXT,
    created_at TIMESTAMPTZ NOT NULL,

    CONSTRAINT task_attempts_task_execution_fk
        FOREIGN KEY (task_execution_id)
        REFERENCES task_executions (task_execution_id),

    CONSTRAINT task_attempts_unique_attempt_number
        UNIQUE (task_execution_id, attempt_number),

    CONSTRAINT task_attempts_dispatch_unique
        UNIQUE (dispatch_id),

    CONSTRAINT task_attempts_attempt_number_positive
        CHECK (attempt_number > 0),

    CONSTRAINT task_attempts_fencing_token_nonnegative
        CHECK (fencing_token >= 0),

    CONSTRAINT task_attempts_state_check
        CHECK (state IN (
            'CREATED',
            'DISPATCHED',
            'STARTED',
            'SUCCEEDED',
            'FAILED',
            'TIMED_OUT',
            'CANCELLED',
            'ABANDONED'
        ))
);

CREATE INDEX task_attempts_state_created_idx
    ON task_attempts (state, created_at);

CREATE INDEX task_attempts_worker_state_idx
    ON task_attempts (worker_id, state);

-- The UNIQUE constraints above already create btree indexes for
-- (task_execution_id, attempt_number) and (dispatch_id), satisfying
-- the contract's required query indexes for those columns. No
-- additional duplicate indexes are needed.

-- Invariants deferred to conditional transactions:
-- - Attempt terminal-state immutability and fencing-token match
--   (invariants 11-12, D.11/D.12) are enforced by conditional completion
--   transactions that validate task_execution_id + fencing_token + lease
--   before mutating the attempt or task execution. Stale reports affect
--   zero rows.

-- ============================================================================
-- outbox_events: durable publication intent (C.8)
-- ============================================================================

CREATE TABLE outbox_events (
    outbox_event_id UUID PRIMARY KEY,
    event_id UUID NOT NULL,
    event_type TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    aggregate_id UUID NOT NULL,
    tenant_id UUID,
    workflow_id UUID,
    workflow_version_id UUID,
    workflow_run_id UUID,
    task_execution_id UUID,
    task_attempt_id UUID,
    occurred_at TIMESTAMPTZ NOT NULL,
    correlation_id TEXT NOT NULL,
    causation_id TEXT,
    producer TEXT NOT NULL,
    payload JSONB NOT NULL,
    headers JSONB,
    created_at TIMESTAMPTZ NOT NULL,
    published_at TIMESTAMPTZ,
    publish_attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    claim_owner TEXT,
    claim_expires_at TIMESTAMPTZ,

    CONSTRAINT outbox_events_event_id_unique
        UNIQUE (event_id),

    CONSTRAINT outbox_events_event_type_nonempty
        CHECK (length(trim(event_type)) > 0),

    CONSTRAINT outbox_events_aggregate_type_nonempty
        CHECK (length(trim(aggregate_type)) > 0),

    CONSTRAINT outbox_events_producer_nonempty
        CHECK (length(trim(producer)) > 0),

    CONSTRAINT outbox_events_publish_attempts_nonnegative
        CHECK (publish_attempts >= 0),

    CONSTRAINT outbox_events_claim_consistency
        CHECK (
            (claim_owner IS NULL AND claim_expires_at IS NULL)
            OR (claim_owner IS NOT NULL AND claim_expires_at IS NOT NULL)
        ),

    CONSTRAINT outbox_events_published_claim_check
        CHECK (
            published_at IS NULL
            OR (claim_owner IS NULL AND claim_expires_at IS NULL)
        )
);

-- Partial index for unpublished events ordered by creation time (relay claim).
CREATE INDEX outbox_events_unpublished_created_idx
    ON outbox_events (created_at)
    WHERE published_at IS NULL;

CREATE INDEX outbox_events_publish_attempts_created_idx
    ON outbox_events (publish_attempts, created_at);

-- Partial index for reclaimable outbox claims (relay recovery).
CREATE INDEX outbox_events_claim_expires_idx
    ON outbox_events (claim_expires_at)
    WHERE published_at IS NULL;

CREATE INDEX outbox_events_aggregate_created_idx
    ON outbox_events (aggregate_id, created_at);

-- Invariant deferred to application transactions:
-- - Every state transition that requires an event inserts its outbox row
--   in the same PostgreSQL transaction (invariant 15, D.5/D.6/D.11 etc.).
--   The schema cannot enforce cross-table transactional atomicity alone;
--   it is the responsibility of each application transaction to insert
--   the outbox row before commit. The relay's at-least-once publication
--   and event_id deduplication (unique constraint) provide idempotent
--   recovery when the relay crashes after Kafka publish but before
--   marking published_at.
